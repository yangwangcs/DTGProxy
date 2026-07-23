use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use adapter_memory::MemoryAdapter;
use physical_plan::{
    MemoryBudget, PhysicalOperator, PhysicalPlanBuilder, PhysicalPlanHeader, Placement,
};
use query_executor::{
    ExecutionContext, RuntimeValue, TemporalBatchExecutor, TemporalRead, TemporalRegion,
};
use temporal_ir::{Column, RowSchema, ScalarExpr, SlotId, ValueType};
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
        PhysicalPlanBuilder::new(PhysicalPlanHeader::new(1, 1, 1, [7; 32]).expect("header"));
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
                    output: scan_schema.clone(),
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
fn interval_node_scan_returns_a_segment_that_exists_only_inside_the_window() {
    let store = TemporalStore::new(MemoryAdapter::new());
    let element = ElementRef::vertex(GraphId::new(1), PartitionId::new(0), ElementId::new(77));
    block_on(
        store.commit_vertex(
            CommitContext::new(0, 1, 1, tx(99), tx(100)),
            VertexMutation::put(
                element,
                LabelId::new(11),
                Interval::new(ValidTime::from_micros(4), Some(ValidTime::from_micros(6)))
                    .expect("valid interval"),
                CanonicalElement::new(1, BTreeMap::new()),
            )
            .expect("vertex mutation"),
        ),
    )
    .expect("commit");

    let rows = block_on(
        TemporalBatchExecutor::new(store).scan_vertex_rows_interval_as_of(
            GraphId::new(1),
            &[11],
            Interval::new(ValidTime::from_micros(1), Some(ValidTime::from_micros(10)))
                .expect("query window"),
            tx(150),
        ),
    )
    .expect("interval scan");

    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].region().valid(),
        Interval::new(ValidTime::from_micros(4), Some(ValidTime::from_micros(6)))
            .expect("expected segment")
    );
    let RuntimeValue::Node(node) = &rows[0].values()[0] else {
        panic!("expected node");
    };
    assert_eq!(node.element(), element);
    assert_eq!(
        rows[0].region().transaction(),
        TemporalRegion::at_transaction(tx(150))
            .expect("transaction region")
            .transaction()
    );
}

#[test]
fn interval_relationship_scan_preserves_the_edge_segment_region() {
    let store = TemporalStore::new(MemoryAdapter::new());
    seed(&store, 1, 11, "source");
    seed(&store, 2, 12, "destination");
    block_on(
        store.commit_edge(
            CommitContext::new(0, 3, 3, tx(100), tx(200)),
            EdgeMutation::put(
                ElementRef::edge(GraphId::new(1), PartitionId::new(0), ElementId::new(55)),
                EdgeTypeId::new(99),
                ElementId::new(1),
                ElementId::new(2),
                Interval::new(ValidTime::from_micros(4), Some(ValidTime::from_micros(6)))
                    .expect("edge interval"),
                CanonicalElement::new(1, BTreeMap::new()),
            )
            .expect("edge mutation"),
        ),
    )
    .expect("commit edge");

    let rows = block_on(
        TemporalBatchExecutor::new(store).scan_edge_rows_interval_as_of(
            GraphId::new(1),
            &[99],
            Interval::new(ValidTime::from_micros(1), Some(ValidTime::from_micros(10)))
                .expect("query window"),
            tx(250),
        ),
    )
    .expect("interval scan");

    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].region().valid(),
        Interval::new(ValidTime::from_micros(4), Some(ValidTime::from_micros(6)))
            .expect("expected segment")
    );
    let RuntimeValue::Relationship(relationship) = &rows[0].values()[0] else {
        panic!("expected relationship");
    };
    assert_eq!(relationship.element().id(), ElementId::new(55));
    assert_eq!(relationship.edge_type(), EdgeTypeId::new(99));
}

#[test]
fn interval_expand_intersects_source_edge_and_destination_regions() {
    let store = TemporalStore::new(MemoryAdapter::new());
    seed(&store, 1, 11, "source");
    seed(&store, 2, 12, "destination");
    block_on(
        store.commit_edge(
            CommitContext::new(0, 3, 3, tx(100), tx(200)),
            EdgeMutation::put(
                ElementRef::edge(GraphId::new(1), PartitionId::new(0), ElementId::new(56)),
                EdgeTypeId::new(99),
                ElementId::new(1),
                ElementId::new(2),
                Interval::new(ValidTime::from_micros(4), Some(ValidTime::from_micros(6)))
                    .expect("edge interval"),
                CanonicalElement::new(1, BTreeMap::new()),
            )
            .expect("edge mutation"),
        ),
    )
    .expect("commit edge");
    let executor = TemporalBatchExecutor::new(store);
    let window = Interval::new(ValidTime::from_micros(1), Some(ValidTime::from_micros(10)))
        .expect("query window");
    let sources =
        block_on(executor.scan_vertex_rows_interval_as_of(GraphId::new(1), &[11], window, tx(250)))
            .expect("source scan");
    let rows = block_on(executor.expand_interval_rows(
        &sources,
        GraphId::new(1),
        true,
        &[99],
        window,
        tx(250),
    ))
    .expect("interval expand");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values().len(), 3);
    assert_eq!(
        rows[0].region().valid(),
        Interval::new(ValidTime::from_micros(4), Some(ValidTime::from_micros(6)))
            .expect("expected intersection")
    );
}

#[test]
fn interval_fragment_executes_scan_and_expand_with_temporal_rows() {
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
    let mut builder =
        PhysicalPlanBuilder::new(PhysicalPlanHeader::new(1, 1, 1, [10; 32]).expect("header"));
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
                    output: expand_schema.clone(),
                },
            ],
            expand_schema,
            MemoryBudget::new(1 << 20, 1 << 20).expect("budget"),
        )
        .expect("fragment");
    let plan = builder.finish(root).expect("plan");
    let rows = block_on(
        TemporalBatchExecutor::new(store).execute_interval_fragment_rows(
            &plan.fragments()[0],
            &ExecutionContext::default(),
            GraphId::new(1),
            Interval::new(ValidTime::from_micros(1), Some(ValidTime::from_micros(10)))
                .expect("window"),
            tx(250),
        ),
    )
    .expect("interval fragment");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values().len(), 3);
    assert_eq!(
        rows[0].region().valid(),
        Interval::new(ValidTime::from_micros(1), Some(ValidTime::from_micros(10)))
            .expect("full valid region")
    );
}

#[test]
fn interval_expand_accepts_a_source_slot_before_other_bindings() {
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
    let unwind_schema = RowSchema::new(vec![
        Column::new(SlotId::new(0), "a", ValueType::Node, false),
        Column::new(SlotId::new(1), "marker", ValueType::Integer, false),
    ])
    .expect("unwind schema");
    let expand_schema = RowSchema::new(vec![
        Column::new(SlotId::new(0), "a", ValueType::Node, false),
        Column::new(SlotId::new(2), "r", ValueType::Relationship, false),
        Column::new(SlotId::new(3), "b", ValueType::Node, false),
        Column::new(SlotId::new(1), "marker", ValueType::Integer, false),
    ])
    .expect("expand schema");
    let mut builder =
        PhysicalPlanBuilder::new(PhysicalPlanHeader::new(1, 1, 1, [12; 32]).expect("header"));
    let root = builder
        .add_fragment(
            Placement::Shard(0),
            vec![
                PhysicalOperator::NodeScan {
                    binding: SlotId::new(0),
                    labels: vec![11],
                    output: scan_schema,
                },
                PhysicalOperator::Unwind {
                    expression: ScalarExpr::Literal(GraphValue::List(vec![GraphValue::Integer(7)])),
                    binding: SlotId::new(1),
                    output: unwind_schema,
                },
                PhysicalOperator::Expand {
                    source: SlotId::new(0),
                    relationship: SlotId::new(2),
                    destination: SlotId::new(3),
                    outgoing: true,
                    types: vec![99],
                    output: expand_schema.clone(),
                },
            ],
            expand_schema,
            MemoryBudget::new(1 << 20, 1 << 20).expect("budget"),
        )
        .expect("fragment");
    let plan = builder.finish(root).expect("plan");

    let rows = block_on(
        TemporalBatchExecutor::new(store).execute_interval_fragment_rows(
            &plan.fragments()[0],
            &ExecutionContext::default(),
            GraphId::new(1),
            Interval::new(ValidTime::from_micros(1), Some(ValidTime::from_micros(10)))
                .expect("window"),
            tx(250),
        ),
    )
    .expect("interval fragment");

    assert_eq!(rows.len(), 1);
    assert!(matches!(rows[0].values()[0], RuntimeValue::Node(_)));
    assert!(matches!(rows[0].values()[1], RuntimeValue::Relationship(_)));
    assert!(matches!(rows[0].values()[2], RuntimeValue::Node(_)));
    assert_eq!(rows[0].values()[3], RuntimeValue::Integer(7));
}

#[test]
fn interval_fragment_preserves_regions_through_filter_and_project() {
    let store = TemporalStore::new(MemoryAdapter::new());
    seed(&store, 1, 11, "source");
    let schema = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "a",
        ValueType::Node,
        false,
    )])
    .expect("schema");
    let mut builder =
        PhysicalPlanBuilder::new(PhysicalPlanHeader::new(1, 1, 1, [11; 32]).expect("header"));
    let root = builder
        .add_fragment(
            Placement::Shard(0),
            vec![
                PhysicalOperator::NodeScan {
                    binding: SlotId::new(0),
                    labels: vec![11],
                    output: schema.clone(),
                },
                PhysicalOperator::Filter(ScalarExpr::Literal(GraphValue::Boolean(true))),
                PhysicalOperator::Project {
                    expressions: vec![(SlotId::new(0), ScalarExpr::Slot(SlotId::new(0)))],
                    output: schema.clone(),
                },
            ],
            schema,
            MemoryBudget::new(1 << 20, 1 << 20).expect("budget"),
        )
        .expect("fragment");
    let plan = builder.finish(root).expect("plan");
    let rows = block_on(
        TemporalBatchExecutor::new(store).execute_interval_fragment_rows(
            &plan.fragments()[0],
            &ExecutionContext::default(),
            GraphId::new(1),
            Interval::new(ValidTime::from_micros(1), Some(ValidTime::from_micros(10)))
                .expect("window"),
            tx(150),
        ),
    )
    .expect("interval fragment");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values().len(), 1);
    assert_eq!(
        rows[0].region().valid(),
        Interval::new(ValidTime::from_micros(1), Some(ValidTime::from_micros(10))).expect("region")
    );
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
        PhysicalPlanBuilder::new(PhysicalPlanHeader::new(1, 1, 1, [8; 32]).expect("header"));
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
                    output: output_schema.clone(),
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
        PhysicalPlanBuilder::new(PhysicalPlanHeader::new(1, 1, 1, [9; 32]).expect("header"));
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
