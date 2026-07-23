use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};
use std::time::{SystemTime, UNIX_EPOCH};

use adapter_memory::MemoryAdapter;
use cypher_compiler::{CompileSession, CypherCompiler};
use distributed_query::{
    DistributedCoordinator, DistributedQueryError, FragmentRequest, FragmentWorker,
    LocalFragmentWorker, SnapshotToken, TemporalWorkerFuture, WorkerFuture,
};
use physical_plan::{
    MemoryBudget, PhysicalApply, PhysicalOperator, PhysicalPlanBuilder, PhysicalPlanHeader,
    Placement, PlanFragment,
};
use query_executor::{
    CancellationToken, ExecutionContext, RuntimeValue, TemporalBatchExecutor, TemporalProvenance,
};
use query_optimizer::{DeploymentMode, Optimizer, OptimizerContext};
use temporal_ir::{ApplyKind, ChildPlanId, Column, RowSchema, ValueType};
use temporal_storage::{
    CommitContext, ElementId, ElementRef, GraphId, LabelId, PartitionId, TemporalStore,
    VertexMutation,
};
use temporal_types::{CanonicalElement, GraphValue, Interval, TransactionTime, ValidTime};

#[tokio::test]
async fn correlated_subquery_is_snapshot_identical_in_primary_replica_and_shared_nothing() {
    let logical = CypherCompiler::new()
        .compile(
            "UNWIND [10, 20] AS seed \
             CALL (seed) { MATCH (n:Account) RETURN n AS copy } \
             RETURN seed, copy",
            &CompileSession::new("accounts", 1, 3, 11).unwrap(),
        )
        .unwrap()
        .logical_plan()
        .clone();
    let primary = Optimizer::new()
        .optimize(
            &logical,
            OptimizerContext::new(DeploymentMode::PrimaryReplica, 1, 8 << 20, 8 << 20).unwrap(),
        )
        .unwrap();
    let shared = Optimizer::new()
        .optimize(
            &logical,
            OptimizerContext::new(DeploymentMode::SharedNothing, 2, 8 << 20, 8 << 20).unwrap(),
        )
        .unwrap();
    let snapshot = SnapshotToken::new(1, 3, 11, tx(150), [5; 32]).unwrap();

    let mut primary_coordinator = DistributedCoordinator::new(8 << 20, 32).unwrap();
    primary_coordinator
        .register(Arc::new(worker(0, &[1, 2]).await))
        .unwrap();
    let mut shared_coordinator = DistributedCoordinator::new(8 << 20, 32).unwrap();
    shared_coordinator
        .register(Arc::new(worker(0, &[1]).await))
        .unwrap();
    shared_coordinator
        .register(Arc::new(worker(1, &[2]).await))
        .unwrap();

    let primary_rows = primary_coordinator
        .execute_plan(
            primary.plan(),
            snapshot.clone(),
            ValidTime::from_micros(5),
            now_ms() + 10_000,
            8,
            &ExecutionContext::default(),
        )
        .await
        .expect("primary Apply");
    let shared_rows = shared_coordinator
        .execute_plan(
            shared.plan(),
            snapshot,
            ValidTime::from_micros(5),
            now_ms() + 10_000,
            8,
            &ExecutionContext::default(),
        )
        .await
        .expect("shared Apply");

    let canonical = |batches: Vec<query_executor::RecordBatch>| {
        batches
            .iter()
            .flat_map(|batch| batch.rows())
            .map(|row| {
                let RuntimeValue::Integer(seed) = &row[0] else {
                    panic!("seed")
                };
                let RuntimeValue::Node(node) = &row[1] else {
                    panic!("node")
                };
                (*seed, node.element().id().value())
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(
        canonical(primary_rows),
        vec![(10, 1), (10, 2), (20, 1), (20, 2)]
    );
    assert_eq!(
        canonical(shared_rows),
        vec![(10, 1), (10, 2), (20, 1), (20, 2)]
    );
}

#[tokio::test]
async fn primary_replica_graph_parent_apply_gathers_before_child_invocation() {
    let logical = CypherCompiler::new()
        .compile(
            "MATCH (parent:Account) \
             CALL (parent) { MATCH (child:Account) RETURN child AS copy } \
             RETURN parent, copy",
            &CompileSession::new("accounts", 1, 3, 11).unwrap(),
        )
        .unwrap()
        .logical_plan()
        .clone();
    let plan = Optimizer::new()
        .optimize(
            &logical,
            OptimizerContext::new(DeploymentMode::PrimaryReplica, 1, 8 << 20, 8 << 20).unwrap(),
        )
        .unwrap();
    let mut coordinator = DistributedCoordinator::new(8 << 20, 32).unwrap();
    coordinator
        .register(Arc::new(worker(0, &[1, 2]).await))
        .unwrap();

    let rows = coordinator
        .execute_plan(
            plan.plan(),
            SnapshotToken::new(1, 3, 11, tx(150), [5; 32]).unwrap(),
            ValidTime::from_micros(5),
            now_ms() + 10_000,
            8,
            &ExecutionContext::default(),
        )
        .await
        .expect("parent rows must gather before Apply");

    assert_eq!(rows.iter().flat_map(|batch| batch.rows()).count(), 4);
}

#[tokio::test]
async fn shared_nothing_missing_expected_shard_never_returns_partial_apply_success() {
    let logical = CypherCompiler::new()
        .compile(
            "RETURN EXISTS { MATCH (n:Account) RETURN n } AS present",
            &CompileSession::new("accounts", 1, 3, 11).unwrap(),
        )
        .unwrap()
        .logical_plan()
        .clone();
    let plan = Optimizer::new()
        .optimize(
            &logical,
            OptimizerContext::new(DeploymentMode::SharedNothing, 2, 8 << 20, 8 << 20).unwrap(),
        )
        .unwrap();
    let mut coordinator = DistributedCoordinator::new(8 << 20, 32).unwrap();
    coordinator
        .register(Arc::new(worker(0, &[1]).await))
        .unwrap();

    let error = coordinator
        .execute_plan(
            plan.plan(),
            SnapshotToken::new(1, 3, 11, tx(150), [5; 32]).unwrap(),
            ValidTime::from_micros(5),
            now_ms() + 10_000,
            8,
            &ExecutionContext::default(),
        )
        .await
        .expect_err("missing shard 1 must fail before EXISTS short-circuit");

    assert_eq!(error, DistributedQueryError::MissingShards(vec![1]));
}

#[tokio::test]
async fn child_invocation_cancellation_stops_a_hanging_worker() {
    let logical = CypherCompiler::new()
        .compile(
            "RETURN EXISTS { MATCH (n:Account) RETURN n } AS present",
            &CompileSession::new("accounts", 1, 3, 11).unwrap(),
        )
        .unwrap()
        .logical_plan()
        .clone();
    let plan = Optimizer::new()
        .optimize(
            &logical,
            OptimizerContext::new(DeploymentMode::SharedNothing, 2, 8 << 20, 8 << 20).unwrap(),
        )
        .unwrap();
    let mut coordinator = DistributedCoordinator::new(8 << 20, 32).unwrap();
    coordinator
        .register(Arc::new(HangingWorker { shard: 0 }))
        .unwrap();
    coordinator
        .register(Arc::new(HangingWorker { shard: 1 }))
        .unwrap();
    let cancellation = CancellationToken::new();
    let context = ExecutionContext::default().with_cancellation(cancellation.clone());
    let task = tokio::spawn(async move {
        coordinator
            .execute_plan(
                plan.plan(),
                SnapshotToken::new(1, 3, 11, tx(150), [5; 32]).unwrap(),
                ValidTime::from_micros(5),
                now_ms() + 10_000,
                8,
                &context,
            )
            .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    cancellation.cancel();
    assert_eq!(task.await.unwrap(), Err(DistributedQueryError::Cancelled));
}

#[test]
fn child_invocation_cancellation_stops_a_hanging_worker_without_tokio_runtime() {
    let logical = CypherCompiler::new()
        .compile(
            "RETURN EXISTS { MATCH (n:Account) RETURN n } AS present",
            &CompileSession::new("accounts", 1, 3, 11).unwrap(),
        )
        .unwrap()
        .logical_plan()
        .clone();
    let plan = Optimizer::new()
        .optimize(
            &logical,
            OptimizerContext::new(DeploymentMode::SharedNothing, 2, 8 << 20, 8 << 20).unwrap(),
        )
        .unwrap();
    let mut coordinator = DistributedCoordinator::new(8 << 20, 32).unwrap();
    coordinator
        .register(Arc::new(HangingWorker { shard: 0 }))
        .unwrap();
    coordinator
        .register(Arc::new(HangingWorker { shard: 1 }))
        .unwrap();
    let cancellation = CancellationToken::new();
    let context = ExecutionContext::default().with_cancellation(cancellation.clone());
    std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(10));
        cancellation.cancel();
    });

    assert_eq!(
        block_on_without_runtime(coordinator.execute_plan(
            plan.plan(),
            SnapshotToken::new(1, 3, 11, tx(150), [5; 32]).unwrap(),
            ValidTime::from_micros(5),
            now_ms() + 10_000,
            8,
            &context,
        )),
        Err(DistributedQueryError::Cancelled)
    );
}

#[test]
fn child_invocation_deadline_stops_a_hanging_worker_without_tokio_runtime() {
    let logical = CypherCompiler::new()
        .compile(
            "RETURN EXISTS { MATCH (n:Account) RETURN n } AS present",
            &CompileSession::new("accounts", 1, 3, 11).unwrap(),
        )
        .unwrap()
        .logical_plan()
        .clone();
    let plan = Optimizer::new()
        .optimize(
            &logical,
            OptimizerContext::new(DeploymentMode::SharedNothing, 2, 8 << 20, 8 << 20).unwrap(),
        )
        .unwrap();
    let mut coordinator = DistributedCoordinator::new(8 << 20, 32).unwrap();
    coordinator
        .register(Arc::new(HangingWorker { shard: 0 }))
        .unwrap();
    coordinator
        .register(Arc::new(HangingWorker { shard: 1 }))
        .unwrap();

    assert_eq!(
        block_on_without_runtime(coordinator.execute_plan(
            plan.plan(),
            SnapshotToken::new(1, 3, 11, tx(150), [5; 32]).unwrap(),
            ValidTime::from_micros(5),
            now_ms() + 25,
            8,
            &ExecutionContext::default(),
        )),
        Err(DistributedQueryError::DeadlineExceeded)
    );
}

#[tokio::test]
async fn nested_child_apply_consumes_outer_recursive_budget() {
    let empty = RowSchema::empty();
    let bool_schema = |slot| {
        RowSchema::new(vec![Column::new(
            slot,
            "present",
            ValueType::Boolean,
            false,
        )])
        .unwrap()
    };
    let mut grand_builder =
        PhysicalPlanBuilder::new(PhysicalPlanHeader::new(1, 3, 11, [3; 32]).unwrap());
    let grand_root = grand_builder
        .add_fragment(
            Placement::Coordinator,
            vec![
                PhysicalOperator::Argument {
                    output: empty.clone(),
                },
                PhysicalOperator::Finish,
            ],
            empty.clone(),
            MemoryBudget::new(1024, 1024).unwrap(),
        )
        .unwrap();
    let grandchild = grand_builder.finish(grand_root).unwrap();

    let mut inner_builder =
        PhysicalPlanBuilder::new(PhysicalPlanHeader::new(1, 3, 11, [2; 32]).unwrap());
    let inner_output = bool_schema(temporal_ir::SlotId::new(1));
    let inner_root = inner_builder
        .add_fragment(
            Placement::Coordinator,
            vec![
                PhysicalOperator::Argument {
                    output: empty.clone(),
                },
                PhysicalOperator::Apply {
                    apply: PhysicalApply::new(
                        ChildPlanId::new(2),
                        ApplyKind::Exists {
                            output: temporal_ir::SlotId::new(1),
                        },
                        Vec::new(),
                        Vec::new(),
                        empty.clone(),
                        grandchild,
                        100,
                        100,
                        temporal_ir::MAX_APPLY_DEPTH,
                    ),
                    output: inner_output.clone(),
                },
            ],
            inner_output.clone(),
            MemoryBudget::new(1024, 1024).unwrap(),
        )
        .unwrap();
    let inner = inner_builder.finish(inner_root).unwrap();

    let mut parent_builder =
        PhysicalPlanBuilder::new(PhysicalPlanHeader::new(1, 3, 11, [1; 32]).unwrap());
    let parent_output = bool_schema(temporal_ir::SlotId::new(0));
    let parent_root = parent_builder
        .add_fragment(
            Placement::Coordinator,
            vec![
                PhysicalOperator::Argument {
                    output: empty.clone(),
                },
                PhysicalOperator::Apply {
                    apply: PhysicalApply::new(
                        ChildPlanId::new(1),
                        ApplyKind::Exists {
                            output: temporal_ir::SlotId::new(0),
                        },
                        Vec::new(),
                        Vec::new(),
                        empty,
                        inner,
                        2,
                        100,
                        1,
                    ),
                    output: parent_output.clone(),
                },
            ],
            parent_output,
            MemoryBudget::new(1024, 1024).unwrap(),
        )
        .unwrap();
    let parent = parent_builder.finish(parent_root).unwrap();
    let coordinator = DistributedCoordinator::new(1024, 4).unwrap();

    let error = coordinator
        .execute_plan(
            &parent,
            SnapshotToken::new(1, 3, 11, tx(150), [5; 32]).unwrap(),
            ValidTime::from_micros(5),
            now_ms() + 10_000,
            8,
            &ExecutionContext::default(),
        )
        .await
        .expect_err("nested child must consume the outer depth ledger");
    assert_eq!(error, DistributedQueryError::RecursivePlanViolation);

    let interval = coordinator
        .execute_interval_plan(
            &parent,
            SnapshotToken::new(1, 3, 11, tx(150), [5; 32]).unwrap(),
            Interval::new(ValidTime::from_micros(0), Some(ValidTime::from_micros(10))).unwrap(),
            now_ms() + 10_000,
            8,
            &ExecutionContext::default(),
        )
        .await
        .expect_err("nested interval child must consume the outer depth ledger");
    assert_eq!(interval, DistributedQueryError::RecursivePlanViolation);
}

#[tokio::test]
async fn interval_distributed_apply_intersects_regions_without_losing_provenance() {
    let logical = CypherCompiler::new()
        .compile(
            "UNWIND [10, 20] AS seed \
             CALL (seed) { MATCH (n:Account) RETURN n AS copy } \
             RETURN seed, copy",
            &CompileSession::new("accounts", 1, 3, 11).unwrap(),
        )
        .unwrap()
        .logical_plan()
        .clone();
    let plan = Optimizer::new()
        .optimize(
            &logical,
            OptimizerContext::new(DeploymentMode::SharedNothing, 2, 8 << 20, 8 << 20).unwrap(),
        )
        .unwrap();
    let mut coordinator = DistributedCoordinator::new(8 << 20, 32).unwrap();
    coordinator
        .register(Arc::new(worker(0, &[1]).await))
        .unwrap();
    coordinator
        .register(Arc::new(worker(1, &[2]).await))
        .unwrap();
    let window =
        Interval::new(ValidTime::from_micros(0), Some(ValidTime::from_micros(10))).unwrap();

    let rows = coordinator
        .execute_interval_plan(
            plan.plan(),
            SnapshotToken::new(1, 3, 11, tx(150), [5; 32]).unwrap(),
            window,
            now_ms() + 10_000,
            8,
            &ExecutionContext::default(),
        )
        .await
        .expect("distributed interval Apply");

    assert_eq!(rows.len(), 4);
    assert!(rows.iter().all(|row| {
        row.region().valid()
            == Interval::new(ValidTime::from_micros(1), Some(ValidTime::from_micros(10))).unwrap()
    }));
    assert!(rows.iter().all(|row| {
        row.provenance().len() == 2
            && row
                .provenance()
                .iter()
                .any(|item| matches!(item, TemporalProvenance::Element(_)))
    }));
}

#[tokio::test]
async fn distributed_exists_stops_after_first_visible_shard_row() {
    let logical = CypherCompiler::new()
        .compile(
            "RETURN EXISTS { MATCH (n:Account) RETURN n } AS present",
            &CompileSession::new("accounts", 1, 3, 11).unwrap(),
        )
        .unwrap()
        .logical_plan()
        .clone();
    let plan = Optimizer::new()
        .optimize(
            &logical,
            OptimizerContext::new(DeploymentMode::SharedNothing, 2, 8 << 20, 8 << 20).unwrap(),
        )
        .unwrap();
    let first_calls = Arc::new(AtomicUsize::new(0));
    let second_calls = Arc::new(AtomicUsize::new(0));
    let mut coordinator = DistributedCoordinator::new(8 << 20, 32).unwrap();
    coordinator
        .register(Arc::new(CountingWorker::new(
            worker(0, &[1]).await,
            Arc::clone(&first_calls),
        )))
        .unwrap();
    coordinator
        .register(Arc::new(CountingWorker::new(
            worker(1, &[2]).await,
            Arc::clone(&second_calls),
        )))
        .unwrap();

    let batches = coordinator
        .execute_plan(
            plan.plan(),
            SnapshotToken::new(1, 3, 11, tx(150), [5; 32]).unwrap(),
            ValidTime::from_micros(5),
            now_ms() + 10_000,
            8,
            &ExecutionContext::default(),
        )
        .await
        .expect("distributed EXISTS");

    assert_eq!(batches[0].rows(), &[vec![RuntimeValue::Boolean(true)]]);
    assert_eq!(first_calls.load(Ordering::SeqCst), 1);
    assert_eq!(second_calls.load(Ordering::SeqCst), 0);
}

struct CountingWorker {
    inner: LocalFragmentWorker<MemoryAdapter>,
    calls: Arc<AtomicUsize>,
}

struct HangingWorker {
    shard: u32,
}

impl FragmentWorker for HangingWorker {
    fn shard_id(&self) -> u32 {
        self.shard
    }

    fn execute_fragment<'a>(
        &'a self,
        _request: &'a FragmentRequest,
        _fragment: &'a PlanFragment,
        _valid_time: ValidTime,
        _context: &'a ExecutionContext,
    ) -> WorkerFuture<'a> {
        Box::pin(async { std::future::pending::<Result<Vec<_>, _>>().await })
    }

    fn execute_interval_fragment<'a>(
        &'a self,
        _request: &'a FragmentRequest,
        _fragment: &'a PlanFragment,
        _window: Interval<ValidTime>,
        _context: &'a ExecutionContext,
    ) -> TemporalWorkerFuture<'a> {
        Box::pin(async { std::future::pending::<Result<Vec<_>, _>>().await })
    }
}

impl CountingWorker {
    fn new(inner: LocalFragmentWorker<MemoryAdapter>, calls: Arc<AtomicUsize>) -> Self {
        Self { inner, calls }
    }
}

impl FragmentWorker for CountingWorker {
    fn shard_id(&self) -> u32 {
        self.inner.shard_id()
    }

    fn execute_fragment<'a>(
        &'a self,
        request: &'a FragmentRequest,
        fragment: &'a PlanFragment,
        valid_time: ValidTime,
        context: &'a ExecutionContext,
    ) -> WorkerFuture<'a> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.inner
            .execute_fragment(request, fragment, valid_time, context)
    }

    fn execute_interval_fragment<'a>(
        &'a self,
        request: &'a FragmentRequest,
        fragment: &'a PlanFragment,
        window: Interval<ValidTime>,
        context: &'a ExecutionContext,
    ) -> TemporalWorkerFuture<'a> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.inner
            .execute_interval_fragment(request, fragment, window, context)
    }
}

async fn worker(shard: u32, ids: &[u64]) -> LocalFragmentWorker<MemoryAdapter> {
    let store = TemporalStore::new(MemoryAdapter::new());
    for (index, id) in ids.iter().enumerate() {
        let element = ElementRef::vertex(
            GraphId::new(1),
            PartitionId::new(shard),
            ElementId::new(u128::from(*id)),
        );
        store
            .commit_vertex(
                CommitContext::new(
                    shard,
                    u64::try_from(index + 1).unwrap(),
                    u128::from(*id),
                    tx(0),
                    tx(100),
                ),
                VertexMutation::put(
                    element,
                    LabelId::new(stable_id("Account")),
                    Interval::new(ValidTime::from_micros(1), None).unwrap(),
                    CanonicalElement::new(
                        1,
                        BTreeMap::from([(1, GraphValue::Integer(i64::try_from(*id).unwrap()))]),
                    ),
                )
                .unwrap(),
            )
            .await
            .unwrap();
    }
    LocalFragmentWorker::new(shard, 1, 3, 11, [5; 32], TemporalBatchExecutor::new(store))
}

fn stable_id(value: &str) -> u32 {
    let digest = blake3::hash(value.as_bytes());
    u32::from_be_bytes(digest.as_bytes()[..4].try_into().unwrap())
}

fn tx(value: i64) -> TransactionTime {
    TransactionTime::new(value, 0)
}

fn now_ms() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
}

fn block_on_without_runtime<F: Future>(future: F) -> F::Output {
    struct ThreadWake(std::thread::Thread);
    impl Wake for ThreadWake {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
    }
    let waker = Waker::from(Arc::new(ThreadWake(std::thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => std::thread::park_timeout(std::time::Duration::from_millis(5)),
        }
    }
}
