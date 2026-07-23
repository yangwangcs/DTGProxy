use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use analytics_api::{
    AlgorithmDescriptor, AlgorithmRequest, AnalyticsOutput, AnalyticsProvider, ProjectedGraph,
    ProviderDescriptor, ProviderError, SnapshotGraph, VertexId,
};
use analytics_ledger::{
    AnalyticsJobId, GraphProjectionScope, JobState, ProjectionLimits, decode_algorithm_parameters,
};
use analytics_runtime::BuiltInProvider;
use procedure_runtime::{
    AnalyticsCancelRequest, AnalyticsCancelResponse, AnalyticsResultPage, AnalyticsResultsRequest,
    AnalyticsStatus, AnalyticsStatusRequest, AnalyticsSubmitRequest, AnalyticsSubmitResponse,
    ClusterAnalyticsCoordinator, ClusterAnalyticsError, JobInvocationContext, ProcedureAccess,
    ProcedureInvocation, ProcedureRegistry, ProcedureValue,
};
use temporal_types::{TransactionTime, ValidTime};

#[derive(Default)]
struct CapturingCoordinator {
    submissions: Mutex<Vec<AnalyticsSubmitRequest>>,
    reads: Mutex<Vec<(AnalyticsJobId, [u8; 32])>>,
}

impl ClusterAnalyticsCoordinator for CapturingCoordinator {
    fn submit(
        &self,
        request: AnalyticsSubmitRequest,
    ) -> Result<AnalyticsSubmitResponse, ClusterAnalyticsError> {
        let job_id = AnalyticsJobId::new(request.submission_request_id()).unwrap();
        self.submissions.lock().unwrap().push(request);
        Ok(AnalyticsSubmitResponse::new(job_id))
    }

    fn status(
        &self,
        request: AnalyticsStatusRequest,
    ) -> Result<AnalyticsStatus, ClusterAnalyticsError> {
        self.reads
            .lock()
            .unwrap()
            .push((request.job_id(), request.security_fingerprint()));
        AnalyticsStatus::new(JobState::Queued, 0, None, None)
    }

    fn results(
        &self,
        _request: AnalyticsResultsRequest,
    ) -> Result<AnalyticsResultPage, ClusterAnalyticsError> {
        Err(ClusterAnalyticsError::new(
            "DTG-ANALYTICS-JOB-NOT-READY",
            "analytics job has not completed",
        ))
    }

    fn cancel(
        &self,
        request: AnalyticsCancelRequest,
    ) -> Result<AnalyticsCancelResponse, ClusterAnalyticsError> {
        self.reads
            .lock()
            .unwrap()
            .push((request.job_id(), request.security_fingerprint()));
        Ok(AnalyticsCancelResponse::new(true))
    }
}

fn job_context(outer_request_id: u128) -> JobInvocationContext {
    JobInvocationContext::new(
        outer_request_id,
        9_000,
        7,
        11,
        13,
        17,
        19,
        TransactionTime::new(23_000, 29),
        GraphProjectionScope::Snapshot {
            valid_time: ValidTime::from_micros(31),
        },
        ProjectionLimits::new(100, 200, 300).unwrap(),
    )
    .unwrap()
}

fn graph() -> Arc<ProjectedGraph> {
    Arc::new(ProjectedGraph::Snapshot(
        SnapshotGraph::new(vec![VertexId::new(1)], Vec::new(), true).unwrap(),
    ))
}

async fn invoke(
    registry: &ProcedureRegistry,
    name: &str,
    arguments: BTreeMap<String, ProcedureValue>,
    outer_request_id: u128,
    graph: Option<Arc<ProjectedGraph>>,
) -> Result<procedure_runtime::ProcedureResult, procedure_runtime::ProcedureError> {
    invoke_with_context(
        registry,
        name,
        arguments,
        job_context(outer_request_id),
        graph,
    )
    .await
}

async fn invoke_with_context(
    registry: &ProcedureRegistry,
    name: &str,
    arguments: BTreeMap<String, ProcedureValue>,
    context: JobInvocationContext,
    graph: Option<Arc<ProjectedGraph>>,
) -> Result<procedure_runtime::ProcedureResult, procedure_runtime::ProcedureError> {
    let identity = *registry.catalog().resolve(name).unwrap().identity();
    registry
        .invoke(
            ProcedureInvocation::new(
                identity,
                arguments,
                graph,
                [37; 32],
                &ProcedureAccess::analytics_read(),
            )
            .with_job_context(context),
        )
        .await
}

#[tokio::test]
async fn submit_builds_a_complete_canonical_cluster_job_spec() {
    let coordinator = Arc::new(CapturingCoordinator::default());
    let registry =
        ProcedureRegistry::builtin_analytics(Arc::new(BuiltInProvider::new()), coordinator.clone())
            .unwrap();
    let arguments = BTreeMap::from([
        (
            "algorithm".into(),
            ProcedureValue::String("dtg.graph.pageRank".into()),
        ),
        ("parameters".into(), ProcedureValue::Map(BTreeMap::new())),
    ]);

    let first = invoke(
        &registry,
        "dtg.analytics.submit",
        arguments.clone(),
        41,
        Some(graph()),
    )
    .await
    .unwrap();
    let retry = invoke(
        &registry,
        "dtg.analytics.submit",
        arguments,
        41,
        Some(graph()),
    )
    .await
    .unwrap();
    let ProcedureValue::String(first_id) = &first.rows()[0][0] else {
        panic!("job ID must be a string")
    };
    let ProcedureValue::String(retry_id) = &retry.rows()[0][0] else {
        panic!("job ID must be a string")
    };
    assert_eq!(first_id, retry_id);
    assert_eq!(first_id.len(), 32);
    assert!(
        first_id
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    );

    let submissions = coordinator.submissions.lock().unwrap();
    let first = &submissions[0];
    assert_eq!(
        first.submission_request_id(),
        submissions[1].submission_request_id()
    );
    assert_ne!(first.submission_request_id(), 0);
    assert_eq!(first.deadline_unix_ms(), 9_000);
    assert_eq!(first.graph_id(), 7);
    assert_eq!(first.catalog_revision(), 11);
    assert_eq!(first.topology_epoch(), 13);
    assert_eq!(first.schema_version(), 17);
    assert_eq!(first.backend_generation(), 19);
    assert_eq!(
        first.transaction_snapshot(),
        TransactionTime::new(23_000, 29)
    );
    assert_eq!(first.security_fingerprint(), [37; 32]);
    assert_eq!(first.provider(), "dtg-rust-reference");
    assert_eq!(first.provider_version(), "1.0.0");
    assert_eq!(first.algorithm_version(), "1.0.0");
    assert_eq!(
        first.limits(),
        ProjectionLimits::new(100, 200, 300).unwrap()
    );
    let parameters = decode_algorithm_parameters(first.parameters()).unwrap();
    assert!(parameters.contains_key("damping"));
    assert!(parameters.contains_key("maxIterations"));
    assert!(parameters.contains_key("tolerance"));
}

#[tokio::test]
async fn submission_identity_separates_outer_requests_and_typed_arguments() {
    let coordinator = Arc::new(CapturingCoordinator::default());
    let registry =
        ProcedureRegistry::builtin_analytics(Arc::new(BuiltInProvider::new()), coordinator.clone())
            .unwrap();
    for (outer, algorithm) in [
        (101, "dtg.graph.degree"),
        (102, "dtg.graph.degree"),
        (101, "dtg.graph.pageRank"),
    ] {
        invoke(
            &registry,
            "dtg.analytics.submit",
            BTreeMap::from([
                ("algorithm".into(), ProcedureValue::String(algorithm.into())),
                ("parameters".into(), ProcedureValue::Map(BTreeMap::new())),
            ]),
            outer,
            Some(graph()),
        )
        .await
        .unwrap();
    }
    let submissions = coordinator.submissions.lock().unwrap();
    let ids = submissions
        .iter()
        .map(AnalyticsSubmitRequest::submission_request_id)
        .collect::<Vec<_>>();
    assert_ne!(ids[0], ids[1]);
    assert_ne!(ids[0], ids[2]);
    assert_ne!(ids[1], ids[2]);
}

#[tokio::test]
async fn submission_identity_is_stable_across_execution_fence_reallocation() {
    let coordinator = Arc::new(CapturingCoordinator::default());
    let registry =
        ProcedureRegistry::builtin_analytics(Arc::new(BuiltInProvider::new()), coordinator.clone())
            .unwrap();
    let arguments = BTreeMap::from([
        (
            "algorithm".into(),
            ProcedureValue::String("dtg.graph.degree".into()),
        ),
        ("parameters".into(), ProcedureValue::Map(BTreeMap::new())),
    ]);
    let first_context = job_context(151);
    let reallocated_context = JobInvocationContext::new(
        151,
        9_500,
        7,
        12,
        14,
        18,
        20,
        TransactionTime::new(24_000, 30),
        GraphProjectionScope::Snapshot {
            valid_time: ValidTime::from_micros(31),
        },
        ProjectionLimits::new(101, 201, 301).unwrap(),
    )
    .unwrap();

    invoke_with_context(
        &registry,
        "dtg.analytics.submit",
        arguments.clone(),
        first_context,
        Some(graph()),
    )
    .await
    .unwrap();
    invoke_with_context(
        &registry,
        "dtg.analytics.submit",
        arguments,
        reallocated_context,
        Some(graph()),
    )
    .await
    .unwrap();

    let submissions = coordinator.submissions.lock().unwrap();
    assert_eq!(
        submissions[0].submission_request_id(),
        submissions[1].submission_request_id(),
        "execution fences are fixed by the first durable submission, not part of retry identity"
    );
}

#[tokio::test]
async fn read_and_cancel_accept_only_canonical_ids_and_forward_security() {
    let coordinator = Arc::new(CapturingCoordinator::default());
    let registry =
        ProcedureRegistry::builtin_analytics(Arc::new(BuiltInProvider::new()), coordinator.clone())
            .unwrap();
    let canonical = AnalyticsJobId::new(17).unwrap().to_string();

    let status = invoke(
        &registry,
        "dtg.analytics.status",
        BTreeMap::from([("jobId".into(), ProcedureValue::String(canonical.clone()))]),
        201,
        None,
    )
    .await
    .unwrap();
    assert_eq!(status.rows()[0][0], ProcedureValue::String("QUEUED".into()));
    invoke(
        &registry,
        "dtg.analytics.cancel",
        BTreeMap::from([("jobId".into(), ProcedureValue::String(canonical))]),
        202,
        None,
    )
    .await
    .unwrap();
    assert_eq!(
        coordinator.reads.lock().unwrap().as_slice(),
        &[(AnalyticsJobId::new(17).unwrap(), [37; 32]); 2]
    );

    for invalid in [
        "17",
        "17:42",
        "00000000000000000000000000000000",
        "000000000000000000000000000000AB",
        "0000000000000000000000000000000g",
    ] {
        let error = invoke(
            &registry,
            "dtg.analytics.status",
            BTreeMap::from([("jobId".into(), ProcedureValue::String(invalid.into()))]),
            203,
            None,
        )
        .await
        .unwrap_err();
        assert_eq!(error.code(), "DTG-ANALYTICS-JOB-ID", "{invalid}: {error}");
    }
}

#[test]
fn registry_fails_closed_when_provider_catalog_does_not_match() {
    let error = ProcedureRegistry::builtin_analytics(
        Arc::new(MismatchedProvider),
        Arc::new(CapturingCoordinator::default()),
    )
    .expect_err("provider/catalog mismatch must not register procedures");
    assert_eq!(error.code(), "DTG-PROCEDURE-CATALOG");
}

struct MismatchedProvider;

impl AnalyticsProvider for MismatchedProvider {
    fn descriptor(&self) -> ProviderDescriptor {
        BuiltInProvider::new().descriptor()
    }

    fn algorithms(&self) -> Vec<AlgorithmDescriptor> {
        Vec::new()
    }

    fn execute_into(
        &self,
        _request: AlgorithmRequest,
        _output: &mut dyn AnalyticsOutput,
    ) -> Result<(), ProviderError> {
        unreachable!("mismatched provider must be rejected during registration")
    }
}

#[test]
fn coordinator_errors_truncate_unicode_at_a_character_boundary() {
    let error = ClusterAnalyticsError::new("DTG-TEST-UNICODE", "€".repeat(1_366));

    assert!(error.message().len() <= 4_096);
    assert!(error.message().is_char_boundary(error.message().len()));
}
