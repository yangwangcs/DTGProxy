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
    CommitContext, EdgeMutation, EdgeTypeId, ElementId, ElementRef, GraphId, LabelId, PartitionId,
    TemporalStore, VertexMutation,
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

#[test]
fn expand_materializes_typed_relationship_and_destination_at_one_snapshot() {
    let store = TemporalStore::new(MemoryAdapter::new());
    seed(&store, 1, 11, "source");
    seed(&store, 2, 12, "destination");
    seed_edge(&store);
    let scan_schema = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "a",
        ValueType::Node,
        false,
    )])
    .expect("scan schema");
    let expand_schema = RowSchema::new(vec![
        Column::new(SlotId::new(0), "a", ValueType::Node, false),
        Column::new(SlotId::new(1), "r", ValueType::Relationship, false),
        Column::new(SlotId::new(2), "b", ValueType::Node, false),
    ])
    .expect("expand schema");
    let output_schema = RowSchema::new(vec![
        Column::new(SlotId::new(1), "r", ValueType::Relationship, false),
        Column::new(SlotId::new(2), "b", ValueType::Node, false),
    ])
    .expect("output schema");
    let mut builder =
        PhysicalPlanBuilder::new(PhysicalPlanHeaderV1::new(1, 1, 1, [8; 32]).expect("header"));
    let root = builder
        .add_fragment(
            Placement::Shard(0),
            vec![
                PhysicalOperator::NodeScan {
                    binding: SlotId::new(0),
                    labels: vec![11],
                    output: scan_schema,
                },
                PhysicalOperator::Expand {
                    source: SlotId::new(0),
                    relationship: SlotId::new(1),
                    destination: SlotId::new(2),
                    outgoing: true,
                    types: vec![99],
                    output: expand_schema,
                },
                PhysicalOperator::Project {
                    expressions: vec![
                        (SlotId::new(1), ScalarExpr::Slot(SlotId::new(1))),
                        (SlotId::new(2), ScalarExpr::Slot(SlotId::new(2))),
                    ],
                },
            ],
            output_schema,
            MemoryBudget::new(1 << 20, 1 << 20).expect("budget"),
        )
        .expect("fragment");
    let plan = builder.finish(root).expect("plan");

    let batches = block_on(TemporalBatchExecutor::new(store).execute_fragment(
        &plan.fragments()[0],
        &ExecutionContext::default(),
        TemporalRead::as_of(GraphId::new(1), ValidTime::from_micros(5), tx(250)),
    ))
    .expect("execute");

    assert_eq!(batches[0].rows().len(), 1);
    let RuntimeValue::Relationship(relationship) = &batches[0].rows()[0][0] else {
        panic!("expected relationship");
    };
    let RuntimeValue::Node(destination) = &batches[0].rows()[0][1] else {
        panic!("expected destination");
    };
    assert_eq!(relationship.element().id(), ElementId::new(10));
    assert_eq!(relationship.edge_type(), EdgeTypeId::new(99));
    assert_eq!(destination.element().id(), ElementId::new(2));
}

#[test]
fn relationship_scan_filters_types_at_the_fenced_snapshot() {
    let store = TemporalStore::new(MemoryAdapter::new());
    seed(&store, 1, 11, "source");
    seed(&store, 2, 12, "destination");
    seed_edge(&store);
    let schema = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "r",
        ValueType::Relationship,
        false,
    )])
    .expect("schema");
    let mut builder =
        PhysicalPlanBuilder::new(PhysicalPlanHeaderV1::new(1, 1, 1, [9; 32]).expect("header"));
    let root = builder
        .add_fragment(
            Placement::Shard(0),
            vec![PhysicalOperator::RelationshipScan {
                binding: SlotId::new(0),
                types: vec![99],
                output: schema.clone(),
            }],
            schema,
            MemoryBudget::new(1 << 20, 1 << 20).expect("budget"),
        )
        .expect("fragment");
    let plan = builder.finish(root).expect("plan");

    let batches = block_on(TemporalBatchExecutor::new(store).execute_fragment(
        &plan.fragments()[0],
        &ExecutionContext::default(),
        TemporalRead::as_of(GraphId::new(1), ValidTime::from_micros(5), tx(250)),
    ))
    .expect("execute");

    assert_eq!(batches[0].rows().len(), 1);
    assert!(matches!(
        batches[0].rows()[0][0],
        RuntimeValue::Relationship(_)
    ));
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

fn seed_edge(store: &TemporalStore<MemoryAdapter>) {
    let edge = ElementRef::edge(GraphId::new(1), PartitionId::new(0), ElementId::new(10));
    block_on(
        store.commit_edge(
            CommitContext::new(0, 3, 3, tx(100), tx(200)),
            EdgeMutation::put(
                edge,
                EdgeTypeId::new(99),
                ElementId::new(1),
                ElementId::new(2),
                Interval::new(ValidTime::from_micros(1), None).expect("interval"),
                CanonicalElement::new(1, BTreeMap::new()),
            )
            .expect("edge mutation"),
        ),
    )
    .expect("edge commit");
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
