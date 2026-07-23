use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use adapter_memory::MemoryAdapter;
use physical_plan::{
    MemoryBudget, PhysicalOperator, PhysicalPlanBuilder, PhysicalPlanHeader, Placement,
};
use query_executor::{
    EdgeRecord, ExecutionContext, GraphOverlay, GraphOverlayEntry, RuntimeValue,
    TemporalBatchExecutor, TemporalRead, VertexRecord,
};
use temporal_ir::{Column, RowSchema, ScalarExpr, SlotId, ValueType};
use temporal_storage::{
    CommitContext, EdgeMutation, EdgeTypeId, ElementId, ElementRef, GraphId, LabelId, PartitionId,
    TemporalStore, VertexMutation,
};
use temporal_types::{CanonicalElement, GraphValue, Interval, TransactionTime, ValidTime};

#[test]
fn committed_node_update_and_delete_are_applied_before_filter_and_count() {
    let store = TemporalStore::new(MemoryAdapter::new());
    let node = vertex(1);
    seed_vertex(&store, node, 11, "old");
    let mut overlay = GraphOverlay::new(8).expect("overlay");
    overlay
        .stage([
            GraphOverlayEntry::put(0, valid(1, None), node_value(node, 11, "new"))
                .expect("overlay update"),
        ])
        .expect("stage update");
    let plan = count_named_nodes_plan();
    let executor = TemporalBatchExecutor::new(store);

    let updated = block_on(executor.execute_fragment(
        &plan.fragments()[0],
        &ExecutionContext::default().with_graph_overlay(overlay.clone()),
        TemporalRead::as_of(GraphId::new(1), ValidTime::from_micros(5), tx(150)),
    ))
    .expect("updated count");
    assert_eq!(updated[0].rows(), &[vec![RuntimeValue::Integer(1)]]);

    overlay
        .stage([GraphOverlayEntry::delete(0, node, valid(1, None))])
        .expect("stage delete");
    let deleted = block_on(executor.execute_fragment(
        &plan.fragments()[0],
        &ExecutionContext::default().with_graph_overlay(overlay),
        TemporalRead::as_of(GraphId::new(1), ValidTime::from_micros(5), tx(150)),
    ))
    .expect("deleted count");
    assert_eq!(deleted[0].rows(), &[vec![RuntimeValue::Integer(0)]]);
}

#[test]
fn overlay_value_is_visible_only_where_its_valid_interval_covers_the_point() {
    let store = TemporalStore::new(MemoryAdapter::new());
    let node = vertex(1);
    seed_vertex(&store, node, 11, "old");
    let mut overlay = GraphOverlay::new(8).expect("overlay");
    overlay
        .stage([
            GraphOverlayEntry::put(0, valid(10, Some(20)), node_value(node, 11, "new"))
                .expect("bounded update"),
        ])
        .expect("stage update");
    let plan = scan_plan(11);
    let executor = TemporalBatchExecutor::new(store);

    let before = block_on(executor.execute_fragment(
        &plan.fragments()[0],
        &ExecutionContext::default().with_graph_overlay(overlay.clone()),
        TemporalRead::as_of(GraphId::new(1), ValidTime::from_micros(5), tx(150)),
    ))
    .expect("before interval");
    let inside = block_on(executor.execute_fragment(
        &plan.fragments()[0],
        &ExecutionContext::default().with_graph_overlay(overlay),
        TemporalRead::as_of(GraphId::new(1), ValidTime::from_micros(15), tx(150)),
    ))
    .expect("inside interval");

    assert_eq!(node_name(&before[0].rows()[0][0]), "old");
    assert_eq!(node_name(&inside[0].rows()[0][0]), "new");
}

#[test]
fn mixed_committed_and_staged_expansion_works_in_both_directions() {
    let store = TemporalStore::new(MemoryAdapter::new());
    let first = vertex(1);
    let second = vertex(2);
    let staged_source = vertex(3);
    seed_vertex(&store, first, 11, "first");
    seed_vertex(&store, second, 12, "second");
    seed_edge(&store, edge(10), first, second);
    let mut overlay = GraphOverlay::new(8).expect("overlay");
    overlay
        .stage([
            GraphOverlayEntry::put(0, valid(1, None), node_value(first, 11, "updated"))
                .expect("updated committed source"),
            GraphOverlayEntry::put(0, valid(1, None), node_value(staged_source, 11, "staged"))
                .expect("staged source"),
            GraphOverlayEntry::put(0, valid(1, None), edge_value(edge(11), first, second))
                .expect("staged edge with committed endpoints"),
            GraphOverlayEntry::put(
                0,
                valid(1, None),
                edge_value(edge(12), staged_source, second),
            )
            .expect("staged edge with staged source"),
        ])
        .expect("stage mixed graph");
    let context = ExecutionContext::default().with_graph_overlay(overlay.clone());
    let executor = TemporalBatchExecutor::new(store);

    let outgoing = block_on(executor.execute_fragment(
        &expand_plan(11, true).fragments()[0],
        &context,
        TemporalRead::as_of(GraphId::new(1), ValidTime::from_micros(5), tx(250)),
    ))
    .expect("outgoing expansion");
    let incoming = block_on(executor.execute_fragment(
        &expand_plan(12, false).fragments()[0],
        &context,
        TemporalRead::as_of(GraphId::new(1), ValidTime::from_micros(5), tx(250)),
    ))
    .expect("incoming expansion");

    assert_eq!(row_count(&outgoing), 3);
    assert_eq!(row_count(&incoming), 3);
    assert!(outgoing.iter().flat_map(|batch| batch.rows()).any(|row| {
        matches!(&row[0], RuntimeValue::Node(node) if node.element() == staged_source)
            && matches!(&row[1], RuntimeValue::Relationship(relationship) if relationship.element() == edge(12))
    }));

    overlay
        .stage([GraphOverlayEntry::delete(0, edge(10), valid(1, None))])
        .expect("delete committed edge");
    let without_committed_edge = block_on(executor.execute_fragment(
        &expand_plan(11, true).fragments()[0],
        &ExecutionContext::default().with_graph_overlay(overlay.clone()),
        TemporalRead::as_of(GraphId::new(1), ValidTime::from_micros(5), tx(250)),
    ))
    .expect("expansion after committed-edge delete");
    assert_eq!(row_count(&without_committed_edge), 2);

    overlay
        .stage([GraphOverlayEntry::delete(0, second, valid(1, None))])
        .expect("delete committed destination");
    let without_destination = block_on(executor.execute_fragment(
        &expand_plan(11, true).fragments()[0],
        &ExecutionContext::default().with_graph_overlay(overlay),
        TemporalRead::as_of(GraphId::new(1), ValidTime::from_micros(5), tx(250)),
    ))
    .expect("expansion after endpoint delete");
    assert_eq!(row_count(&without_destination), 0);
}

#[test]
fn cross_partition_staged_edge_reaches_both_endpoint_expansion_shards_without_scan_duplicates() {
    let store = TemporalStore::new(MemoryAdapter::new());
    let source = ElementRef::vertex(GraphId::new(1), PartitionId::new(0), ElementId::new(21));
    let destination = ElementRef::vertex(GraphId::new(1), PartitionId::new(1), ElementId::new(22));
    let relationship = ElementRef::edge(GraphId::new(1), PartitionId::new(0), ElementId::new(23));
    let mut overlay = GraphOverlay::new(8).expect("overlay");
    overlay
        .stage([
            GraphOverlayEntry::put(0, valid(1, None), node_value(source, 11, "source"))
                .expect("source"),
            GraphOverlayEntry::put(
                1,
                valid(1, None),
                node_value(destination, 12, "destination"),
            )
            .expect("destination"),
            GraphOverlayEntry::put_with_adjacency(
                0,
                [0, 1],
                valid(1, None),
                edge_value(relationship, source, destination),
            )
            .expect("relationship"),
        ])
        .expect("stage cross-partition graph");
    let executor = TemporalBatchExecutor::new(store);

    let outgoing = block_on(
        executor.execute_fragment(
            &expand_plan(11, true).fragments()[0],
            &ExecutionContext::default()
                .with_graph_overlay(overlay.clone())
                .for_shard(0),
            TemporalRead::as_of(GraphId::new(1), ValidTime::from_micros(5), tx(250)),
        ),
    )
    .expect("source-owner outgoing expansion");
    let incoming = block_on(
        executor.execute_fragment(
            &expand_plan(12, false).fragments()[0],
            &ExecutionContext::default()
                .with_graph_overlay(overlay.clone())
                .for_shard(1),
            TemporalRead::as_of(GraphId::new(1), ValidTime::from_micros(5), tx(250)),
        ),
    )
    .expect("destination-owner incoming expansion");
    let relationship_scan = block_on(
        executor.execute_fragment(
            &relationship_scan_plan().fragments()[0],
            &ExecutionContext::default()
                .with_graph_overlay(overlay)
                .for_shard(1),
            TemporalRead::as_of(GraphId::new(1), ValidTime::from_micros(5), tx(250)),
        ),
    )
    .expect("destination shard relationship scan");

    assert_eq!(row_count(&outgoing), 1);
    assert_eq!(row_count(&incoming), 1);
    assert_eq!(row_count(&relationship_scan), 0);
}

#[test]
fn graph_overlay_stage_is_atomic_when_the_candidate_exceeds_its_bound() {
    let mut overlay = GraphOverlay::new(1).expect("overlay");
    overlay
        .stage([
            GraphOverlayEntry::put(0, valid(1, None), node_value(vertex(1), 11, "first"))
                .expect("first entry"),
        ])
        .expect("first statement");

    assert!(
        overlay
            .stage([
                GraphOverlayEntry::put(0, valid(1, None), node_value(vertex(2), 11, "second"),)
                    .expect("second entry"),
                GraphOverlayEntry::put(0, valid(1, None), node_value(vertex(3), 11, "third"),)
                    .expect("third entry"),
            ])
            .is_err()
    );
    assert_eq!(overlay.len(), 1);
}

fn count_named_nodes_plan() -> physical_plan::PhysicalPlan {
    let scan = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "n",
        ValueType::Node,
        false,
    )])
    .expect("scan schema");
    let output = RowSchema::new(vec![Column::new(
        SlotId::new(1),
        "count",
        ValueType::Integer,
        false,
    )])
    .expect("count schema");
    let count = u32::from_be_bytes(
        blake3::hash(b"count").as_bytes()[..4]
            .try_into()
            .expect("count id"),
    );
    let mut builder =
        PhysicalPlanBuilder::new(PhysicalPlanHeader::new(1, 1, 1, [1; 32]).expect("header"));
    let root = builder
        .add_fragment(
            Placement::Shard(0),
            vec![
                PhysicalOperator::NodeScan {
                    binding: SlotId::new(0),
                    labels: vec![11],
                    output: scan.clone(),
                },
                PhysicalOperator::Filter(ScalarExpr::Equal(
                    Box::new(ScalarExpr::Property {
                        value: Box::new(ScalarExpr::Slot(SlotId::new(0))),
                        property_id: 1,
                    }),
                    Box::new(ScalarExpr::Literal(GraphValue::String("new".into()))),
                )),
                PhysicalOperator::Aggregate {
                    grouping: Vec::new(),
                    aggregates: vec![(
                        SlotId::new(1),
                        ScalarExpr::Function {
                            function_id: count,
                            arguments: Vec::new(),
                        },
                    )],
                    output: output.clone(),
                },
            ],
            output,
            MemoryBudget::new(1 << 20, 1 << 20).expect("budget"),
        )
        .expect("fragment");
    builder.finish(root).expect("plan")
}

fn scan_plan(label: u32) -> physical_plan::PhysicalPlan {
    let schema = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "n",
        ValueType::Node,
        false,
    )])
    .expect("schema");
    let mut builder =
        PhysicalPlanBuilder::new(PhysicalPlanHeader::new(1, 1, 1, [2; 32]).expect("header"));
    let root = builder
        .add_fragment(
            Placement::Shard(0),
            vec![PhysicalOperator::NodeScan {
                binding: SlotId::new(0),
                labels: vec![label],
                output: schema.clone(),
            }],
            schema,
            MemoryBudget::new(1 << 20, 1 << 20).expect("budget"),
        )
        .expect("fragment");
    builder.finish(root).expect("plan")
}

fn expand_plan(source_label: u32, outgoing: bool) -> physical_plan::PhysicalPlan {
    let scan = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "source",
        ValueType::Node,
        false,
    )])
    .expect("scan schema");
    let output = RowSchema::new(vec![
        Column::new(SlotId::new(0), "source", ValueType::Node, false),
        Column::new(
            SlotId::new(1),
            "relationship",
            ValueType::Relationship,
            false,
        ),
        Column::new(SlotId::new(2), "destination", ValueType::Node, false),
    ])
    .expect("expand schema");
    let mut builder =
        PhysicalPlanBuilder::new(PhysicalPlanHeader::new(1, 1, 1, [3; 32]).expect("header"));
    let root = builder
        .add_fragment(
            Placement::Shard(0),
            vec![
                PhysicalOperator::NodeScan {
                    binding: SlotId::new(0),
                    labels: vec![source_label],
                    output: scan,
                },
                PhysicalOperator::Expand {
                    source: SlotId::new(0),
                    relationship: SlotId::new(1),
                    destination: SlotId::new(2),
                    outgoing,
                    types: vec![99],
                    output: output.clone(),
                },
            ],
            output,
            MemoryBudget::new(1 << 20, 1 << 20).expect("budget"),
        )
        .expect("fragment");
    builder.finish(root).expect("plan")
}

fn relationship_scan_plan() -> physical_plan::PhysicalPlan {
    let schema = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "relationship",
        ValueType::Relationship,
        false,
    )])
    .expect("schema");
    let mut builder =
        PhysicalPlanBuilder::new(PhysicalPlanHeader::new(1, 1, 1, [4; 32]).expect("header"));
    let root = builder
        .add_fragment(
            Placement::Shard(1),
            vec![PhysicalOperator::RelationshipScan {
                binding: SlotId::new(0),
                types: vec![99],
                output: schema.clone(),
            }],
            schema,
            MemoryBudget::new(1 << 20, 1 << 20).expect("budget"),
        )
        .expect("fragment");
    builder.finish(root).expect("plan")
}

fn seed_vertex(store: &TemporalStore<MemoryAdapter>, element: ElementRef, label: u32, name: &str) {
    block_on(
        store.commit_vertex(
            CommitContext::new(
                0,
                element.id().value() as u64,
                element.id().value(),
                tx(0),
                tx(100),
            ),
            VertexMutation::put(element, LabelId::new(label), valid(1, None), payload(name))
                .expect("vertex mutation"),
        ),
    )
    .expect("vertex commit");
}

fn seed_edge(
    store: &TemporalStore<MemoryAdapter>,
    element: ElementRef,
    source: ElementRef,
    destination: ElementRef,
) {
    block_on(
        store.commit_edge(
            CommitContext::new(0, 3, 3, tx(100), tx(200)),
            EdgeMutation::put_between(
                element,
                EdgeTypeId::new(99),
                source,
                destination,
                valid(1, None),
                payload("edge"),
            )
            .expect("edge mutation"),
        ),
    )
    .expect("edge commit");
}

fn node_value(element: ElementRef, label: u32, name: &str) -> RuntimeValue {
    RuntimeValue::Node(VertexRecord::new(
        element,
        Some(LabelId::new(label)),
        payload(name),
    ))
}

fn edge_value(element: ElementRef, source: ElementRef, destination: ElementRef) -> RuntimeValue {
    RuntimeValue::Relationship(EdgeRecord::from_endpoints(
        element,
        EdgeTypeId::new(99),
        source,
        destination,
        payload("edge"),
    ))
}

fn node_name(value: &RuntimeValue) -> &str {
    let RuntimeValue::Node(node) = value else {
        panic!("expected node")
    };
    let Some(GraphValue::String(name)) = node.payload().properties().get(&1) else {
        panic!("expected name")
    };
    name
}

fn payload(name: &str) -> CanonicalElement {
    CanonicalElement::new(
        1,
        BTreeMap::from([(1, GraphValue::String(name.to_owned()))]),
    )
}

fn vertex(id: u128) -> ElementRef {
    ElementRef::vertex(GraphId::new(1), PartitionId::new(0), ElementId::new(id))
}

fn edge(id: u128) -> ElementRef {
    ElementRef::edge(GraphId::new(1), PartitionId::new(0), ElementId::new(id))
}

fn valid(start: i64, end: Option<i64>) -> Interval<ValidTime> {
    Interval::new(
        ValidTime::from_micros(start),
        end.map(ValidTime::from_micros),
    )
    .expect("valid interval")
}

fn tx(value: i64) -> TransactionTime {
    TransactionTime::new(value, 0)
}

fn row_count(batches: &[query_executor::RecordBatch]) -> usize {
    batches.iter().map(|batch| batch.rows().len()).sum()
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
