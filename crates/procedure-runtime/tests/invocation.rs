use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Condvar, Mutex};
use std::time::Duration;

use analytics_api::{EventEdge, EventGraph, ProjectedGraph, SnapshotGraph};
use analytics_api::{SnapshotEdge, VertexId};
use analytics_runtime::BuiltInProvider;
use procedure_runtime::{
    AnalyticsCancelRequest, AnalyticsCancelResponse, AnalyticsResultPage, AnalyticsResultsRequest,
    AnalyticsStatus, AnalyticsStatusRequest, AnalyticsSubmitRequest, AnalyticsSubmitResponse,
    ClusterAnalyticsCoordinator, ClusterAnalyticsError, ProcedureAccess, ProcedureCatalog,
    ProcedureDefinition, ProcedureEffect, ProcedureError, ProcedureField, ProcedureInvocation,
    ProcedureLimits, ProcedureOutput, ProcedurePermission, ProcedurePlacement, ProcedureProvider,
    ProcedureRegistry, ProcedureResult, ProcedureValue,
};
use temporal_ir::ValueType;
use temporal_types::ValidTime;

struct RejectingCoordinator;

impl ClusterAnalyticsCoordinator for RejectingCoordinator {
    fn submit(
        &self,
        _request: AnalyticsSubmitRequest,
    ) -> Result<AnalyticsSubmitResponse, ClusterAnalyticsError> {
        Err(unexpected_job_call())
    }

    fn status(
        &self,
        _request: AnalyticsStatusRequest,
    ) -> Result<AnalyticsStatus, ClusterAnalyticsError> {
        Err(unexpected_job_call())
    }

    fn results(
        &self,
        _request: AnalyticsResultsRequest,
    ) -> Result<AnalyticsResultPage, ClusterAnalyticsError> {
        Err(unexpected_job_call())
    }

    fn cancel(
        &self,
        _request: AnalyticsCancelRequest,
    ) -> Result<AnalyticsCancelResponse, ClusterAnalyticsError> {
        Err(unexpected_job_call())
    }
}

fn unexpected_job_call() -> ClusterAnalyticsError {
    ClusterAnalyticsError::new(
        "DTG-TEST-UNEXPECTED-JOB-CALL",
        "synchronous graph procedure invoked cluster job control",
    )
}

fn builtin_registry() -> ProcedureRegistry {
    ProcedureRegistry::builtin_analytics(
        Arc::new(BuiltInProvider::new()),
        Arc::new(RejectingCoordinator),
    )
    .unwrap()
}

struct StaticProvider {
    result: ProcedureResult,
}

struct CapturingProvider {
    arguments: Arc<Mutex<Option<BTreeMap<String, ProcedureValue>>>>,
}

impl ProcedureProvider for CapturingProvider {
    fn invoke(
        &self,
        invocation: &ProcedureInvocation,
        output: &mut ProcedureOutput,
    ) -> Result<(), ProcedureError> {
        *self.arguments.lock().unwrap() = Some(invocation.arguments().clone());
        output.declare_columns(vec!["result".into()])?;
        output.push_row(vec![ProcedureValue::Integer(7)])
    }
}

impl ProcedureProvider for StaticProvider {
    fn invoke(
        &self,
        _invocation: &ProcedureInvocation,
        output: &mut ProcedureOutput,
    ) -> Result<(), ProcedureError> {
        output.declare_columns(self.result.columns().to_vec())?;
        for row in self.result.rows() {
            output.push_row(row.clone())?;
        }
        Ok(())
    }
}

fn build_registry(
    result: ProcedureResult,
    limits: ProcedureLimits,
) -> (ProcedureRegistry, temporal_ir::ProcedureIdentity) {
    let catalog = ProcedureCatalog::from_definitions(vec![
        ProcedureDefinition::new(
            "dtg.test.rows",
            Vec::new(),
            vec![ProcedureField::new("value", ValueType::Integer, false)],
            ProcedureEffect::ReadOnly,
            ProcedurePermission::AnalyticsRead,
            ProcedurePlacement::Coordinator,
        )
        .with_limits(limits),
    ])
    .unwrap();
    let identity = *catalog.resolve("dtg.test.rows").unwrap().identity();
    let mut registry = ProcedureRegistry::new(catalog);
    registry
        .register(identity, Arc::new(StaticProvider { result }))
        .unwrap();
    (registry, identity)
}

async fn invoke(
    registry: &ProcedureRegistry,
    identity: temporal_ir::ProcedureIdentity,
) -> Result<ProcedureResult, ProcedureError> {
    let graph = Arc::new(ProjectedGraph::Snapshot(
        SnapshotGraph::new(Vec::new(), Vec::new(), true).unwrap(),
    ));
    registry
        .invoke(ProcedureInvocation::new(
            identity,
            BTreeMap::new(),
            Some(graph),
            [7; 32],
            &ProcedureAccess::analytics_read(),
        ))
        .await
}

#[tokio::test]
async fn built_in_analytics_adapter_returns_typed_degree_rows_without_query_source() {
    let registry = builtin_registry();
    let descriptor = registry.catalog().resolve("dtg.graph.degree").unwrap();
    let graph = SnapshotGraph::new(
        vec![VertexId::new(1), VertexId::new(2)],
        vec![SnapshotEdge::new(VertexId::new(1), VertexId::new(2), 1.0).unwrap()],
        true,
    )
    .unwrap();

    let result = registry
        .invoke(ProcedureInvocation::new(
            *descriptor.identity(),
            BTreeMap::new(),
            Some(Arc::new(ProjectedGraph::Snapshot(graph))),
            [8; 32],
            &ProcedureAccess::analytics_read(),
        ))
        .await
        .unwrap();

    assert_eq!(
        result.columns(),
        &["vertexId", "inDegree", "outDegree", "degree"]
    );
    assert_eq!(result.rows().len(), 2);
    assert!(matches!(&result.rows()[0][0], ProcedureValue::String(value) if value == "1"));
    assert!(matches!(&result.rows()[0][3], ProcedureValue::Integer(1)));
}

#[tokio::test]
async fn latest_ordinary_algorithms_are_registered_as_typed_procedures() {
    let registry = builtin_registry();

    for name in [
        "dtg.graph.dfs",
        "dtg.graph.allPairsShortestPath",
        "dtg.graph.betweenness",
        "dtg.graph.closeness",
        "dtg.graph.louvain",
    ] {
        let descriptor = registry
            .catalog()
            .resolve(name)
            .unwrap_or_else(|| panic!("missing typed procedure {name}"));
        assert!(
            descriptor.algorithm().is_some(),
            "{name} must retain its algorithm contract"
        );
    }

    let descriptor = registry.catalog().resolve("dtg.graph.louvain").unwrap();
    let graph = SnapshotGraph::new(
        vec![VertexId::new(1), VertexId::new(2)],
        vec![SnapshotEdge::new(VertexId::new(1), VertexId::new(2), 1.0).unwrap()],
        false,
    )
    .unwrap();
    let result = registry
        .invoke(ProcedureInvocation::new(
            *descriptor.identity(),
            BTreeMap::new(),
            Some(Arc::new(ProjectedGraph::Snapshot(graph))),
            [10; 32],
            &ProcedureAccess::analytics_read(),
        ))
        .await
        .unwrap();

    assert_eq!(result.columns(), &["vertexId", "communityId"]);
    assert_eq!(result.rows().len(), 2);
}

#[tokio::test]
async fn built_in_analytics_adapter_returns_typed_temporal_page_rank_rows() {
    let registry = builtin_registry();
    let descriptor = registry.catalog().resolve("dtg.temporal.pageRank").unwrap();
    let graph = EventGraph::new(
        vec![VertexId::new(1), VertexId::new(2)],
        vec![
            EventEdge::new(
                VertexId::new(1),
                VertexId::new(2),
                ValidTime::from_micros(10),
                0,
                1.0,
            )
            .unwrap(),
        ],
    )
    .unwrap();
    let result = registry
        .invoke(ProcedureInvocation::new(
            *descriptor.identity(),
            BTreeMap::from([
                (
                    "validFrom".into(),
                    ProcedureValue::TimestampMicros(ValidTime::from_micros(0).as_micros()),
                ),
                (
                    "validTo".into(),
                    ProcedureValue::TimestampMicros(ValidTime::from_micros(20).as_micros()),
                ),
            ]),
            Some(Arc::new(ProjectedGraph::Event(graph))),
            [9; 32],
            &ProcedureAccess::analytics_read(),
        ))
        .await
        .unwrap();

    assert_eq!(result.columns(), &["vertexId", "score"]);
    assert_eq!(result.rows().len(), 2);
    assert!(matches!(
        result.rows()[0].as_slice(),
        [ProcedureValue::String(_), ProcedureValue::FloatBits(_)]
    ));
}

#[tokio::test]
async fn provider_output_wrong_column_or_type_fails_before_returning_rows() {
    let limits = ProcedureLimits::new(10, 10, 10, 1024, 4096).unwrap();
    let (registry, identity) = build_registry(
        ProcedureResult::new(vec!["wrong".into()], vec![vec![ProcedureValue::Integer(1)]]),
        limits,
    );
    assert_eq!(
        invoke(&registry, identity).await,
        Err(ProcedureError::ProviderSchemaMismatch)
    );

    let (registry, identity) = build_registry(
        ProcedureResult::new(
            vec!["value".into()],
            vec![vec![ProcedureValue::String("bad".into())]],
        ),
        limits,
    );
    assert_eq!(
        invoke(&registry, identity).await,
        Err(ProcedureError::ProviderTypeMismatch)
    );
}

#[tokio::test]
async fn provider_output_row_and_byte_bounds_fail_before_returning_rows() {
    let row_limits = ProcedureLimits::new(10, 10, 1, 1024, 4096).unwrap();
    let (registry, identity) = build_registry(
        ProcedureResult::new(
            vec!["value".into()],
            vec![
                vec![ProcedureValue::Integer(1)],
                vec![ProcedureValue::Integer(2)],
            ],
        ),
        row_limits,
    );
    assert_eq!(
        invoke(&registry, identity).await,
        Err(ProcedureError::OutputRowLimit)
    );

    let byte_limits = ProcedureLimits::new(10, 10, 10, 4, 4).unwrap();
    let (registry, identity) = build_registry(
        ProcedureResult::new(vec!["value".into()], vec![vec![ProcedureValue::Integer(1)]]),
        byte_limits,
    );
    assert_eq!(
        invoke(&registry, identity).await,
        Err(ProcedureError::ValueByteLimit)
    );
}

#[tokio::test]
async fn registry_injects_catalog_defaults_before_provider_invocation() {
    let catalog = ProcedureCatalog::from_definitions(vec![ProcedureDefinition::new(
        "dtg.test.defaulted",
        vec![
            ProcedureField::new("value", ValueType::Integer, false)
                .with_default(analytics_api::AlgorithmValue::Integer(7)),
        ],
        vec![ProcedureField::new("result", ValueType::Integer, false)],
        ProcedureEffect::ReadOnly,
        ProcedurePermission::AnalyticsRead,
        ProcedurePlacement::Coordinator,
    )])
    .unwrap();
    let identity = *catalog.resolve("dtg.test.defaulted").unwrap().identity();
    let arguments = Arc::new(Mutex::new(None));
    let mut registry = ProcedureRegistry::new(catalog);
    registry
        .register(
            identity,
            Arc::new(CapturingProvider {
                arguments: Arc::clone(&arguments),
            }),
        )
        .unwrap();

    invoke(&registry, identity)
        .await
        .expect("catalog default should satisfy the invocation");
    assert_eq!(
        arguments.lock().unwrap().as_ref().unwrap().get("value"),
        Some(&ProcedureValue::Integer(7))
    );
}

fn input_registry(
    value_type: ValueType,
    limits: ProcedureLimits,
    arguments: Arc<Mutex<Option<BTreeMap<String, ProcedureValue>>>>,
) -> (ProcedureRegistry, temporal_ir::ProcedureIdentity) {
    let catalog = ProcedureCatalog::from_definitions(vec![
        ProcedureDefinition::new(
            "dtg.test.input",
            vec![ProcedureField::new("value", value_type, false)],
            vec![ProcedureField::new("result", ValueType::Integer, false)],
            ProcedureEffect::ReadOnly,
            ProcedurePermission::AnalyticsRead,
            ProcedurePlacement::Coordinator,
        )
        .with_limits(limits),
    ])
    .unwrap();
    let identity = *catalog.resolve("dtg.test.input").unwrap().identity();
    let mut registry = ProcedureRegistry::new(catalog);
    registry
        .register(identity, Arc::new(CapturingProvider { arguments }))
        .unwrap();
    (registry, identity)
}

#[tokio::test]
async fn input_value_and_recursive_byte_limits_fail_before_provider_invocation() {
    let seen = Arc::new(Mutex::new(None));
    let (registry, identity) = input_registry(
        ValueType::String,
        ProcedureLimits::new(10, 10, 10, 8, 64).unwrap(),
        Arc::clone(&seen),
    );
    let graph = Arc::new(ProjectedGraph::Snapshot(
        SnapshotGraph::new(Vec::new(), Vec::new(), true).unwrap(),
    ));
    let error = registry
        .invoke(ProcedureInvocation::new(
            identity,
            BTreeMap::from([("value".into(), ProcedureValue::String("too-large".into()))]),
            Some(Arc::clone(&graph)),
            [7; 32],
            &ProcedureAccess::analytics_read(),
        ))
        .await
        .expect_err("oversized input must fail before provider invocation");
    assert_eq!(error, ProcedureError::ValueByteLimit);
    assert!(seen.lock().unwrap().is_none());

    let seen = Arc::new(Mutex::new(None));
    let (registry, identity) = input_registry(
        ValueType::List(Box::new(ValueType::Integer)),
        ProcedureLimits::new(10, 10, 10, 23, 23).unwrap(),
        Arc::clone(&seen),
    );
    registry
        .invoke(ProcedureInvocation::new(
            identity,
            BTreeMap::from([(
                "value".into(),
                ProcedureValue::List(vec![ProcedureValue::Integer(1), ProcedureValue::Integer(2)]),
            )]),
            Some(graph),
            [7; 32],
            &ProcedureAccess::analytics_read(),
        ))
        .await
        .expect("a two-integer list is exactly 23 estimated bytes");
    assert!(seen.lock().unwrap().is_some());
}

struct OverflowingProvider {
    attempts: Arc<AtomicUsize>,
}

impl ProcedureProvider for OverflowingProvider {
    fn invoke(
        &self,
        _invocation: &ProcedureInvocation,
        output: &mut ProcedureOutput,
    ) -> Result<(), ProcedureError> {
        output.declare_columns(vec!["value".into()])?;
        self.attempts.fetch_add(1, Ordering::SeqCst);
        output.push_row(vec![ProcedureValue::Integer(1)])?;
        self.attempts.fetch_add(1, Ordering::SeqCst);
        output.push_row(vec![ProcedureValue::Integer(2)])
    }
}

#[tokio::test]
async fn output_limit_rejects_a_row_before_the_registry_can_expose_any_result() {
    let catalog = ProcedureCatalog::from_definitions(vec![
        ProcedureDefinition::new(
            "dtg.test.overflow",
            Vec::new(),
            vec![ProcedureField::new("value", ValueType::Integer, false)],
            ProcedureEffect::ReadOnly,
            ProcedurePermission::AnalyticsRead,
            ProcedurePlacement::Coordinator,
        )
        .with_limits(ProcedureLimits::new(1, 1, 1, 1024, 4096).unwrap()),
    ])
    .unwrap();
    let identity = *catalog.resolve("dtg.test.overflow").unwrap().identity();
    let attempts = Arc::new(AtomicUsize::new(0));
    let mut registry = ProcedureRegistry::new(catalog);
    registry
        .register(
            identity,
            Arc::new(OverflowingProvider {
                attempts: Arc::clone(&attempts),
            }),
        )
        .unwrap();

    assert_eq!(
        invoke(&registry, identity).await,
        Err(ProcedureError::OutputRowLimit)
    );
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
}

struct BlockingProvider {
    next_id: AtomicUsize,
    started: tokio::sync::mpsc::UnboundedSender<usize>,
    released: Arc<(Mutex<Vec<usize>>, Condvar)>,
}

impl ProcedureProvider for BlockingProvider {
    fn invoke(
        &self,
        _invocation: &ProcedureInvocation,
        output: &mut ProcedureOutput,
    ) -> Result<(), ProcedureError> {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        self.started.send(id).unwrap();
        let (released, changed) = &*self.released;
        let mut released = released.lock().unwrap();
        while !released.contains(&id) {
            released = changed.wait(released).unwrap();
        }
        output.declare_columns(vec!["value".into()])?;
        output.push_row(vec![ProcedureValue::Integer(id as i64)])
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_caller_does_not_release_worker_capacity_before_provider_exit() {
    let catalog = ProcedureCatalog::from_definitions(vec![ProcedureDefinition::new(
        "dtg.test.blocking",
        Vec::new(),
        vec![ProcedureField::new("value", ValueType::Integer, false)],
        ProcedureEffect::ReadOnly,
        ProcedurePermission::AnalyticsRead,
        ProcedurePlacement::Coordinator,
    )])
    .unwrap();
    let identity = *catalog.resolve("dtg.test.blocking").unwrap().identity();
    let (started_tx, mut started_rx) = tokio::sync::mpsc::unbounded_channel();
    let released = Arc::new((Mutex::new(Vec::new()), Condvar::new()));
    let mut registry = ProcedureRegistry::new(catalog)
        .with_worker_limit(1)
        .unwrap();
    registry
        .register(
            identity,
            Arc::new(BlockingProvider {
                next_id: AtomicUsize::new(0),
                started: started_tx,
                released: Arc::clone(&released),
            }),
        )
        .unwrap();
    let registry = Arc::new(registry);

    let first = tokio::spawn({
        let registry = Arc::clone(&registry);
        async move { invoke(&registry, identity).await }
    });
    assert_eq!(started_rx.recv().await, Some(0));
    first.abort();
    let _ = first.await;

    let second = tokio::spawn({
        let registry = Arc::clone(&registry);
        async move { invoke(&registry, identity).await }
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(50), started_rx.recv())
            .await
            .is_err(),
        "the second provider entered while the abandoned first provider still owned capacity"
    );

    {
        let (ids, changed) = &*released;
        ids.lock().unwrap().push(0);
        changed.notify_all();
    }
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), started_rx.recv())
            .await
            .unwrap(),
        Some(1)
    );
    {
        let (ids, changed) = &*released;
        ids.lock().unwrap().push(1);
        changed.notify_all();
    }
    assert!(second.await.unwrap().is_ok());
}
