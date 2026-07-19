use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use adapter_memory::MemoryAdapter;
use physical_plan::{
    MemoryBudget, PhysicalOperator, PhysicalPlanBuilder, PhysicalPlanHeaderV1, Placement,
};
use query_executor::v2::{ExecutionContext, RuntimeValue, TemporalBatchExecutor, TemporalRead};
use temporal_ir::v2::{Column, RowSchema, ScalarExpr, SlotId, ValueType};
use temporal_storage::{
    CommitContext, ElementId, ElementRef, GraphId, LabelId, PartitionId, TemporalStore,
    VertexMutation,
};
use temporal_types::{CanonicalElement, GraphValue, Interval, TransactionTime, ValidTime};

#[test]
fn node_scan_applies_label_and_transaction_time_fences_before_projection() {
    let store = TemporalStore::new(MemoryAdapter::new());
    seed(&store, 1, 11, "account");
    seed(&store, 2, 12, "person");
    let scan_schema = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "n",
        ValueType::Node,
        false,
    )])
    .expect("schema");
    let mut builder =
        PhysicalPlanBuilder::new(PhysicalPlanHeaderV1::new(1, 1, 1, [7; 32]).expect("header"));
    let root = builder
        .add_fragment(
            Placement::Shard(0),
            vec![
                PhysicalOperator::NodeScan {
                    binding: SlotId::new(0),
                    labels: vec![11],
                    output: scan_schema.clone(),
                },
                PhysicalOperator::Project {
                    expressions: vec![(SlotId::new(0), ScalarExpr::Slot(SlotId::new(0)))],
                },
            ],
            scan_schema,
            MemoryBudget::new(1 << 20, 1 << 20).expect("budget"),
        )
        .expect("fragment");
    let plan = builder.finish(root).expect("plan");

    let batches = block_on(TemporalBatchExecutor::new(store).execute_fragment(
        &plan.fragments()[0],
        &ExecutionContext::default(),
        TemporalRead::as_of(GraphId::new(1), ValidTime::from_micros(5), tx(150)),
    ))
    .expect("execute");

    assert_eq!(
        batches
            .iter()
            .map(|batch| batch.rows().len())
            .sum::<usize>(),
        1
    );
    let RuntimeValue::Node(node) = &batches[0].rows()[0][0] else {
        panic!("expected node");
    };
    assert_eq!(node.element().id(), ElementId::new(1));
    assert_eq!(node.label(), Some(LabelId::new(11)));
}

fn seed(store: &TemporalStore<MemoryAdapter>, id: u128, label: u32, name: &str) {
    let element = ElementRef::vertex(GraphId::new(1), PartitionId::new(0), ElementId::new(id));
    let payload = CanonicalElement::new(
        1,
        BTreeMap::from([(1, GraphValue::String(name.to_owned()))]),
    );
    block_on(
        store.commit_vertex(
            CommitContext::new(0, id as u64, id, tx(0), tx(100)),
            VertexMutation::put(
                element,
                LabelId::new(label),
                Interval::new(ValidTime::from_micros(1), None).expect("interval"),
                payload,
            )
            .expect("mutation"),
        ),
    )
    .expect("commit");
}

fn tx(value: i64) -> TransactionTime {
    TransactionTime::new(value, 0)
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
