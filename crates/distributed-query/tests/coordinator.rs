use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::time::{SystemTime, UNIX_EPOCH};

use adapter_memory::MemoryAdapter;
use distributed_query::{
    DistributedCoordinator, FragmentRequest, LocalFragmentWorker, SnapshotTokenV2,
};
use physical_plan::{
    ExchangeKind, MemoryBudget, PhysicalOperator, PhysicalPlanBuilder, PhysicalPlanHeaderV1,
    Placement,
};
use query_executor::v2::{ExecutionContext, RuntimeValue, TemporalBatchExecutor};
use temporal_ir::v2::ScalarExpr;
use temporal_ir::v2::{Column, RowSchema, SlotId, ValueType};
use temporal_storage::{
    CommitContext, ElementId, ElementRef, GraphId, LabelId, PartitionId, TemporalStore,
    VertexMutation,
};
use temporal_types::{CanonicalElement, GraphValue, Interval, TransactionTime, ValidTime};

#[test]
fn coordinator_merges_all_shards_deterministically_under_bounded_credits() {
    let worker0 = worker(0, 1);
    let worker1 = worker(1, 2);
    let mut coordinator = DistributedCoordinator::new(2 << 20, 8).expect("coordinator");
    coordinator.register(Arc::new(worker1)).expect("worker 1");
    coordinator.register(Arc::new(worker0)).expect("worker 0");
    let schema = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "n",
        ValueType::Node,
        false,
    )])
    .expect("schema");
    let mut builder =
        PhysicalPlanBuilder::new(PhysicalPlanHeaderV1::new(1, 3, 11, [7; 32]).expect("header"));
    let root = builder
        .add_fragment(
            Placement::AllShards,
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
    let snapshot = SnapshotTokenV2::new(1, 3, 11, tx(150), [5; 32]).expect("snapshot");
    let request =
        FragmentRequest::new(root, snapshot, now_ms() + 10_000, 1 << 20, 1).expect("request");

    let batches = block_on(coordinator.execute(
        &request,
        &plan.fragments()[0],
        ValidTime::from_micros(5),
        &ExecutionContext::default(),
    ))
    .expect("execute");

    let ids = batches
        .iter()
        .flat_map(|batch| batch.rows())
        .map(|row| match &row[0] {
            RuntimeValue::Node(node) => node.element().id(),
            value => panic!("expected node, got {value:?}"),
        })
        .collect::<Vec<_>>();
    assert_eq!(ids, vec![ElementId::new(1), ElementId::new(2)]);
}

#[test]
fn coordinator_routes_primary_replica_fragment_to_its_single_shard() {
    let mut coordinator = DistributedCoordinator::new(1 << 20, 4).expect("coordinator");
    coordinator
        .register(Arc::new(worker(0, 1)))
        .expect("worker");
    let schema = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "n",
        ValueType::Node,
        false,
    )])
    .expect("schema");
    let mut builder =
        PhysicalPlanBuilder::new(PhysicalPlanHeaderV1::new(1, 3, 11, [6; 32]).expect("header"));
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
    let request = FragmentRequest::new(
        root,
        SnapshotTokenV2::new(1, 3, 11, tx(150), [5; 32]).expect("snapshot"),
        now_ms() + 10_000,
        1 << 20,
        8,
    )
    .expect("request");

    let batches = block_on(coordinator.execute(
        &request,
        &plan.fragments()[0],
        ValidTime::from_micros(5),
        &ExecutionContext::default(),
    ))
    .expect("execute");

    assert_eq!(
        batches
            .iter()
            .map(|batch| batch.rows().len())
            .sum::<usize>(),
        1
    );
}

#[test]
fn coordinator_executes_gather_exchange_and_root_projection() {
    let mut coordinator = DistributedCoordinator::new(2 << 20, 8).expect("coordinator");
    coordinator
        .register(Arc::new(worker(0, 1)))
        .expect("worker 0");
    coordinator
        .register(Arc::new(worker(1, 2)))
        .expect("worker 1");
    let schema = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "n",
        ValueType::Node,
        false,
    )])
    .expect("schema");
    let mut builder =
        PhysicalPlanBuilder::new(PhysicalPlanHeaderV1::new(1, 3, 11, [5; 32]).expect("header"));
    let shard = builder
        .add_fragment(
            Placement::AllShards,
            vec![PhysicalOperator::NodeScan {
                binding: SlotId::new(0),
                labels: vec![11],
                output: schema.clone(),
            }],
            schema.clone(),
            MemoryBudget::new(1 << 20, 1 << 20).expect("budget"),
        )
        .expect("shard fragment");
    let root = builder
        .add_fragment(
            Placement::Coordinator,
            vec![PhysicalOperator::Project {
                expressions: vec![(SlotId::new(0), ScalarExpr::Slot(SlotId::new(0)))],
            }],
            schema.clone(),
            MemoryBudget::new(1 << 20, 1 << 20).expect("budget"),
        )
        .expect("root fragment");
    builder
        .add_exchange(shard, root, ExchangeKind::Gather, schema, 8)
        .expect("exchange");
    let plan = builder.finish(root).expect("plan");
    let snapshot = SnapshotTokenV2::new(1, 3, 11, tx(150), [5; 32]).expect("snapshot");

    let batches = block_on(coordinator.execute_plan(
        &plan,
        snapshot,
        ValidTime::from_micros(5),
        now_ms() + 10_000,
        1,
        &ExecutionContext::default(),
    ))
    .expect("execute plan");

    assert_eq!(
        batches
            .iter()
            .map(|batch| batch.rows().len())
            .sum::<usize>(),
        2
    );
}

fn worker(shard_id: u32, element_id: u128) -> LocalFragmentWorker<MemoryAdapter> {
    let store = TemporalStore::new(MemoryAdapter::new());
    let element = ElementRef::vertex(
        GraphId::new(1),
        PartitionId::new(shard_id),
        ElementId::new(element_id),
    );
    block_on(
        store.commit_vertex(
            CommitContext::new(shard_id, 1, element_id, tx(0), tx(100)),
            VertexMutation::put(
                element,
                LabelId::new(11),
                Interval::new(ValidTime::from_micros(1), None).expect("interval"),
                CanonicalElement::new(
                    1,
                    BTreeMap::from([(1, GraphValue::Integer(element_id as i64))]),
                ),
            )
            .expect("mutation"),
        ),
    )
    .expect("commit");
    LocalFragmentWorker::new(
        shard_id,
        1,
        3,
        11,
        [5; 32],
        TemporalBatchExecutor::new(store),
    )
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
