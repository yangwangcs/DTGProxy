use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::{Condvar, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use analytics_api::{ProjectedGraph, SnapshotEdge, SnapshotGraph, VertexId};
use analytics_runtime::BuiltInProvider;
use cypher_compiler::{CompileSession, CypherCompiler};
use distributed_query::{DistributedCoordinator, SnapshotToken};
use procedure_runtime::{
    AnalyticsCancelRequest, AnalyticsCancelResponse, AnalyticsResultPage, AnalyticsResultsRequest,
    AnalyticsStatus, AnalyticsStatusRequest, AnalyticsSubmitRequest, AnalyticsSubmitResponse,
    ClusterAnalyticsCoordinator, ClusterAnalyticsError, ProcedureAccess, ProcedureCatalog,
    ProcedureDefinition, ProcedureEffect, ProcedureError, ProcedureField, ProcedureInvocation,
    ProcedureOutput, ProcedurePermission, ProcedurePlacement, ProcedureProvider, ProcedureRegistry,
    ProcedureValue,
};
use query_executor::ExecutionContext;
use query_optimizer::{DeploymentMode, Optimizer, OptimizerContext};
use temporal_ir::ValueType;
use temporal_types::{TransactionTime, ValidTime};

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

#[tokio::test]
async fn global_procedure_rows_are_canonical_in_primary_replica_and_shared_nothing() {
    let registry = Arc::new(
        ProcedureRegistry::builtin_analytics(
            Arc::new(BuiltInProvider::new()),
            Arc::new(RejectingCoordinator),
        )
        .unwrap(),
    );
    let graph = Arc::new(ProjectedGraph::Snapshot(
        SnapshotGraph::new(
            vec![VertexId::new(1), VertexId::new(2)],
            vec![SnapshotEdge::new(VertexId::new(1), VertexId::new(2), 1.0).unwrap()],
            true,
        )
        .unwrap(),
    ));
    let compiled = CypherCompiler::new()
        .compile_with_procedures(
            "CALL dtg.graph.degree({}) YIELD vertexId, degree RETURN vertexId, degree",
            &CompileSession::new("social", 7, 3, 11).unwrap(),
            registry.catalog(),
            &ProcedureAccess::analytics_read(),
        )
        .unwrap();
    let context = ExecutionContext::new(BTreeMap::new()).with_procedure_runtime(
        Arc::clone(&registry),
        Some(graph),
        [9; 32],
        ProcedureAccess::analytics_read(),
    );
    let snapshot = SnapshotToken::new(7, 3, 11, TransactionTime::new(100, 0), [9; 32]).unwrap();
    let coordinator = DistributedCoordinator::new(64 << 20, 64).unwrap();
    let mut results = Vec::new();

    for (mode, shards) in [
        (DeploymentMode::PrimaryReplica, 1),
        (DeploymentMode::SharedNothing, 2),
    ] {
        let physical = Optimizer::new()
            .optimize(
                compiled.logical_plan(),
                OptimizerContext::new(mode, shards, 64 << 20, 256 << 20).unwrap(),
            )
            .unwrap();
        results.push(
            coordinator
                .execute_plan(
                    physical.plan(),
                    snapshot.clone(),
                    ValidTime::from_micros(1000),
                    u64::MAX,
                    1024,
                    &context,
                )
                .await
                .unwrap(),
        );
    }

    assert_eq!(results[0], results[1]);
    assert_eq!(results[0][0].rows().len(), 2);
}

struct BlockingProvider {
    started: tokio::sync::mpsc::UnboundedSender<()>,
    released: Arc<(Mutex<bool>, Condvar)>,
}

impl ProcedureProvider for BlockingProvider {
    fn invoke(
        &self,
        _invocation: &ProcedureInvocation,
        output: &mut ProcedureOutput,
    ) -> Result<(), ProcedureError> {
        self.started.send(()).unwrap();
        let (released, changed) = &*self.released;
        let mut released = released.lock().unwrap();
        while !*released {
            released = changed.wait(released).unwrap();
        }
        output.declare_columns(vec!["value".into()])?;
        output.push_row(vec![ProcedureValue::Integer(1)])
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn coordinator_deadline_aborts_a_running_procedure_provider() {
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
    let released = Arc::new((Mutex::new(false), Condvar::new()));
    let mut registry = ProcedureRegistry::new(catalog);
    registry
        .register(
            identity,
            Arc::new(BlockingProvider {
                started: started_tx,
                released: Arc::clone(&released),
            }),
        )
        .unwrap();
    let registry = Arc::new(registry);
    let compiled = CypherCompiler::new()
        .compile_with_procedures(
            "CALL dtg.test.blocking({}) YIELD value RETURN value",
            &CompileSession::new("social", 7, 3, 11).unwrap(),
            registry.catalog(),
            &ProcedureAccess::analytics_read(),
        )
        .unwrap();
    let physical = Optimizer::new()
        .optimize(
            compiled.logical_plan(),
            OptimizerContext::new(DeploymentMode::PrimaryReplica, 1, 64 << 20, 256 << 20).unwrap(),
        )
        .unwrap();
    let context = ExecutionContext::new(BTreeMap::new()).with_procedure_runtime(
        registry,
        Some(Arc::new(ProjectedGraph::Snapshot(
            SnapshotGraph::new(Vec::new(), Vec::new(), true).unwrap(),
        ))),
        [9; 32],
        ProcedureAccess::analytics_read(),
    );
    let snapshot = SnapshotToken::new(7, 3, 11, TransactionTime::new(100, 0), [9; 32]).unwrap();
    let deadline = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
        + 50;
    let coordinator = DistributedCoordinator::new(64 << 20, 64).unwrap();
    let plan = physical.plan().clone();
    let mut execution = tokio::spawn(async move {
        coordinator
            .execute_plan(
                &plan,
                snapshot,
                ValidTime::from_micros(1000),
                deadline,
                1024,
                &context,
            )
            .await
    });
    started_rx.recv().await.unwrap();
    let result = tokio::time::timeout(Duration::from_millis(250), &mut execution).await;

    let (flag, changed) = &*released;
    *flag.lock().unwrap() = true;
    changed.notify_all();

    let result = result
        .expect("coordinator deadline must abort the running provider")
        .unwrap()
        .expect_err("expired coordinator execution must fail");
    assert!(result.to_string().contains("DeadlineExceeded"));
}
