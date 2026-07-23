use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use adapter_memory::MemoryAdapter;
use distributed_query::{
    DistributedQueryError, FragmentRequest, LocalFragmentWorker, SnapshotToken,
};
use physical_plan::{
    MemoryBudget, PhysicalOperator, PhysicalPlanBuilder, PhysicalPlanHeader, Placement,
};
use query_executor::{ExecutionContext, RuntimeValue, TemporalBatchExecutor};
use temporal_ir::{Column, RowSchema, SlotId, ValueType};
use temporal_storage::{
    CommitContext, ElementId, ElementRef, GraphId, LabelId, PartitionId, TemporalStore,
    VertexMutation,
};
use temporal_types::{CanonicalElement, GraphValue, Interval, TransactionTime, ValidTime};

#[test]
fn local_worker_validates_snapshot_identity_before_returning_bounded_batches() {
    let store = TemporalStore::new(MemoryAdapter::new());
    seed(&store);
    let schema = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "n",
        ValueType::Node,
        false,
    )])
    .expect("schema");
    let mut builder =
        PhysicalPlanBuilder::new(PhysicalPlanHeader::new(1, 3, 11, [7; 32]).expect("header"));
    let root = builder
        .add_fragment(
            Placement::Shard(0),
            vec![PhysicalOperator::NodeScan {
                binding: SlotId::new(0),
                labels: vec![11],
                output: schema.clone(),
            }],
            schema,
            MemoryBudget::new(1 << 20, 1 << 20).expect("budget"),
        )
        .expect("fragment");
    let plan = builder.finish(root).expect("plan");
    let snapshot = SnapshotToken::new(1, 3, 11, tx(150), [5; 32]).expect("snapshot");
    let request =
        FragmentRequest::new(root, snapshot, now_ms() + 10_000, 1 << 20, 1).expect("request");
    let worker = LocalFragmentWorker::new(0, 1, 3, 11, [5; 32], TemporalBatchExecutor::new(store));

    let batches = block_on(worker.execute(
        &request,
        &plan.fragments()[0],
        ValidTime::from_micros(5),
        &ExecutionContext::default(),
    ))
    .expect("execute");

    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].sequence(), 0);
    assert!(!batches[0].has_more());
    assert_eq!(batches[0].batch().rows().len(), 1);
    assert!(matches!(
        batches[0].batch().rows()[0][0],
        RuntimeValue::Node(_)
    ));
}

#[test]
fn local_worker_streams_interval_rows_without_discarding_regions() {
    let store = TemporalStore::new(MemoryAdapter::new());
    seed(&store);
    let schema = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "n",
        ValueType::Node,
        false,
    )])
    .expect("schema");
    let mut builder =
        PhysicalPlanBuilder::new(PhysicalPlanHeader::new(1, 3, 11, [9; 32]).expect("header"));
    let root = builder
        .add_fragment(
            Placement::Shard(0),
            vec![PhysicalOperator::NodeScan {
                binding: SlotId::new(0),
                labels: vec![11],
                output: schema.clone(),
            }],
            schema,
            MemoryBudget::new(1 << 20, 1 << 20).expect("budget"),
        )
        .expect("fragment");
    let plan = builder.finish(root).expect("plan");
    let snapshot = SnapshotToken::new(1, 3, 11, tx(150), [5; 32]).expect("snapshot");
    let request =
        FragmentRequest::new(root, snapshot, now_ms() + 10_000, 1 << 20, 1).expect("request");
    let worker = LocalFragmentWorker::new(0, 1, 3, 11, [5; 32], TemporalBatchExecutor::new(store));

    let batches = block_on(worker.execute_interval(
        &request,
        &plan.fragments()[0],
        Interval::new(ValidTime::from_micros(1), Some(ValidTime::from_micros(10))).expect("window"),
        &ExecutionContext::default(),
    ))
    .expect("execute interval");

    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].sequence(), 0);
    assert!(!batches[0].has_more());
    assert_eq!(batches[0].batch().rows().len(), 1);
    assert_eq!(
        batches[0].batch().rows()[0].region().valid(),
        Interval::new(ValidTime::from_micros(1), Some(ValidTime::from_micros(10))).expect("region")
    );
}

#[test]
fn local_worker_rejects_stale_topology_without_exposing_data() {
    let worker = LocalFragmentWorker::new(
        0,
        1,
        3,
        12,
        [5; 32],
        TemporalBatchExecutor::new(TemporalStore::new(MemoryAdapter::new())),
    );
    let snapshot = SnapshotToken::new(1, 3, 11, tx(150), [5; 32]).expect("snapshot");
    let request = FragmentRequest::new(
        physical_plan::FragmentId::new(0),
        snapshot,
        now_ms() + 10_000,
        1024,
        1,
    )
    .expect("request");
    let schema = RowSchema::empty();
    let mut builder =
        PhysicalPlanBuilder::new(PhysicalPlanHeader::new(1, 3, 11, [7; 32]).expect("header"));
    let root = builder
        .add_fragment(
            Placement::Shard(0),
            vec![
                PhysicalOperator::Project {
                    expressions: Vec::new(),
                    output: schema.clone(),
                },
                PhysicalOperator::Finish,
            ],
            schema,
            MemoryBudget::new(1024, 1024).expect("budget"),
        )
        .expect("fragment");
    let plan = builder.finish(root).expect("plan");

    let error = block_on(worker.execute(
        &request,
        &plan.fragments()[0],
        ValidTime::from_micros(5),
        &ExecutionContext::default(),
    ))
    .expect_err("stale topology");
    assert_eq!(error, DistributedQueryError::WorkerIdentityMismatch);
}

#[test]
fn worker_never_widens_an_earlier_parent_deadline() {
    let schema = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "n",
        ValueType::Node,
        false,
    )])
    .unwrap();
    let mut builder = PhysicalPlanBuilder::new(PhysicalPlanHeader::new(1, 3, 11, [7; 32]).unwrap());
    let root = builder
        .add_fragment(
            Placement::Shard(0),
            vec![PhysicalOperator::NodeScan {
                binding: SlotId::new(0),
                labels: Vec::new(),
                output: schema.clone(),
            }],
            schema,
            MemoryBudget::new(1024, 1024).unwrap(),
        )
        .unwrap();
    let plan = builder.finish(root).unwrap();
    let request = FragmentRequest::new(
        root,
        SnapshotToken::new(1, 3, 11, tx(150), [5; 32]).unwrap(),
        now_ms() + 10_000,
        1024,
        1,
    )
    .unwrap();
    let worker = LocalFragmentWorker::new(
        0,
        1,
        3,
        11,
        [5; 32],
        TemporalBatchExecutor::new(TemporalStore::new(MemoryAdapter::new())),
    );
    let expired = Instant::now()
        .checked_sub(Duration::from_millis(1))
        .unwrap();

    let error = block_on(worker.execute(
        &request,
        &plan.fragments()[0],
        ValidTime::from_micros(5),
        &ExecutionContext::default().with_deadline(expired),
    ))
    .expect_err("request deadline must not widen the expired parent deadline");

    assert_eq!(error, DistributedQueryError::DeadlineExceeded);
}

fn seed(store: &TemporalStore<MemoryAdapter>) {
    let element = ElementRef::vertex(GraphId::new(1), PartitionId::new(0), ElementId::new(1));
    block_on(
        store.commit_vertex(
            CommitContext::new(0, 1, 1, tx(0), tx(100)),
            VertexMutation::put(
                element,
                LabelId::new(11),
                Interval::new(ValidTime::from_micros(1), None).expect("interval"),
                CanonicalElement::new(1, BTreeMap::from([(1, GraphValue::String("node".into()))])),
            )
            .expect("mutation"),
        ),
    )
    .expect("commit");
}

fn tx(value: i64) -> TransactionTime {
    TransactionTime::new(value, 0)
}

fn now_ms() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_millis(),
    )
    .expect("time")
}

fn block_on<F: Future>(future: F) -> F::Output {
    struct Noop;
    impl Wake for Noop {
        fn wake(self: Arc<Self>) {}
    }
    let waker = Waker::from(Arc::new(Noop));
    let mut context = Context::from_waker(&waker);
    let mut future = Box::pin(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}
