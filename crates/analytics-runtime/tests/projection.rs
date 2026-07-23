use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use adapter_memory::MemoryAdapter;
use analytics_api::{DeltaKind, EdgeId, VertexId};
use analytics_runtime::{
    ProjectionError, ProjectionLimits, project_delta_bounded, project_event, project_event_bounded,
    project_interval_bounded, project_interval_part_bounded, project_snapshot,
    project_snapshot_bounded, project_valid_time_delta_bounded,
};
use temporal_storage::{
    CommitContext, EdgeMutation, EdgeTypeId, ElementId, ElementRef, GraphId, LabelId, PartitionId,
    TemporalStore, VertexMutation,
};
use temporal_types::{CanonicalElement, GraphValue, Interval, TransactionTime, ValidTime};

#[test]
fn projects_one_fenced_storage_snapshot_with_numeric_weights() {
    let store = TemporalStore::new(MemoryAdapter::new());
    seed(&store);

    let before_edge = block_on(project_snapshot(
        &store,
        GraphId::new(7),
        ValidTime::from_micros(5),
        TransactionTime::new(150, 0),
        true,
        Some(9),
    ))
    .expect("projection");
    assert_eq!(before_edge.vertices().len(), 2);
    assert!(before_edge.edges().is_empty());

    let graph = block_on(project_snapshot(
        &store,
        GraphId::new(7),
        ValidTime::from_micros(5),
        TransactionTime::new(250, 0),
        true,
        Some(9),
    ))
    .expect("projection");
    assert_eq!(graph.outgoing(VertexId::new(1)).len(), 1);
    assert_eq!(graph.outgoing(VertexId::new(1))[0].weight(), 2.5);
}

#[test]
fn projects_event_history_with_explicit_time_and_duration_properties() {
    let store = TemporalStore::new(MemoryAdapter::new());
    seed(&store);
    block_on(
        store.commit_edge(
            CommitContext::new(0, 4, 4, tx(200), tx(300)),
            EdgeMutation::put(
                ElementRef::edge(GraphId::new(7), PartitionId::new(0), ElementId::new(3)),
                EdgeTypeId::new(1),
                ElementId::new(1),
                ElementId::new(2),
                interval(),
                CanonicalElement::new(
                    1,
                    BTreeMap::from([(7, GraphValue::Integer(20)), (8, GraphValue::Integer(3))]),
                ),
            )
            .expect("event edge"),
        ),
    )
    .expect("commit event edge");

    let graph = block_on(project_event(
        &store,
        GraphId::new(7),
        tx(350),
        Some(7),
        Some(8),
        16,
    ))
    .expect("event projection");
    assert_eq!(graph.vertices().len(), 2);
    assert_eq!(graph.events().len(), 2);
    let event = graph
        .events()
        .iter()
        .find(|event| event.event_time() == ValidTime::from_micros(20))
        .expect("explicit event time");
    assert_eq!(event.duration_micros(), 3);

    assert_eq!(
        block_on(project_event_bounded(
            &store,
            GraphId::new(7),
            tx(350),
            Some(7),
            Some(8),
            ProjectionLimits::new(16, 1, 1 << 20).unwrap(),
        )),
        Err(ProjectionError::EventLimit)
    );
}

#[test]
fn snapshot_projection_enforces_entry_and_byte_budgets_during_storage_scan() {
    let store = TemporalStore::new(MemoryAdapter::new());
    seed(&store);

    assert_eq!(
        block_on(project_snapshot_bounded(
            &store,
            GraphId::new(7),
            ValidTime::from_micros(5),
            TransactionTime::new(250, 0),
            true,
            Some(9),
            ProjectionLimits::new(1, 16, 1 << 20).unwrap(),
        )),
        Err(ProjectionError::VertexLimit)
    );
    assert_eq!(
        block_on(project_snapshot_bounded(
            &store,
            GraphId::new(7),
            ValidTime::from_micros(5),
            TransactionTime::new(250, 0),
            true,
            Some(9),
            ProjectionLimits::new(16, 16, 1).unwrap(),
        )),
        Err(ProjectionError::ByteLimit)
    );
}

#[test]
fn projects_interval_segments_and_transaction_delta_as_canonical_graph_models() {
    let store = TemporalStore::new(MemoryAdapter::new());
    seed(&store);
    let limits = ProjectionLimits::new(16, 16, 1 << 20).unwrap();

    let interval_graph = block_on(project_interval_bounded(
        &store,
        GraphId::new(7),
        Interval::new(ValidTime::from_micros(0), Some(ValidTime::from_micros(10))).unwrap(),
        tx(250),
        true,
        Some(9),
        limits,
    ))
    .expect("interval projection");
    assert_eq!(interval_graph.vertices().len(), 2);
    assert_eq!(interval_graph.edges().len(), 1);
    assert_eq!(
        interval_graph.edges()[0].valid(),
        Interval::new(ValidTime::from_micros(1), Some(ValidTime::from_micros(10))).unwrap()
    );
    assert_eq!(
        interval_graph.vertices()[0].payload(),
        &CanonicalElement::new(1, BTreeMap::new())
    );
    assert_eq!(interval_graph.edges()[0].edge_id(), EdgeId::new(3));

    let delta = block_on(project_delta_bounded(
        &store,
        GraphId::new(7),
        ValidTime::from_micros(5),
        tx(150),
        tx(250),
        true,
        Some(9),
        limits,
    ))
    .expect("delta projection");
    assert!(delta.vertices().is_empty());
    assert_eq!(delta.edges().len(), 1);
    assert_eq!(delta.edges()[0].change(), analytics_api::DeltaKind::Added);
    assert_eq!(delta.edges()[0].source(), VertexId::new(1));
    assert_eq!(delta.edges()[0].destination(), VertexId::new(2));
    assert_eq!(delta.edges()[0].before_weight(), None);
    assert_eq!(delta.edges()[0].after_weight(), Some(2.5));
}

#[test]
fn delta_projection_exposes_updated_and_removed_edge_weights() {
    let store = TemporalStore::new(MemoryAdapter::new());
    seed(&store);
    let limits = ProjectionLimits::new(16, 16, 1 << 20).unwrap();
    let edge = ElementRef::edge(GraphId::new(7), PartitionId::new(0), ElementId::new(3));

    block_on(
        store.commit_edge(
            CommitContext::new(0, 4, 4, tx(200), tx(300)),
            EdgeMutation::put(
                edge,
                EdgeTypeId::new(1),
                ElementId::new(1),
                ElementId::new(2),
                interval(),
                CanonicalElement::new(
                    1,
                    BTreeMap::from([(9, GraphValue::FloatBits(4.0_f64.to_bits()))]),
                ),
            )
            .expect("weight update"),
        ),
    )
    .expect("commit weight update");

    let updated = block_on(project_delta_bounded(
        &store,
        GraphId::new(7),
        ValidTime::from_micros(5),
        tx(250),
        tx(350),
        true,
        Some(9),
        limits,
    ))
    .expect("updated delta");
    assert_eq!(updated.edges().len(), 1);
    assert_eq!(
        updated.edges()[0].change(),
        analytics_api::DeltaKind::Updated
    );
    assert_eq!(updated.edges()[0].before_weight(), Some(2.5));
    assert_eq!(updated.edges()[0].after_weight(), Some(4.0));

    block_on(
        store.commit_edge(
            CommitContext::new(0, 5, 5, tx(300), tx(400)),
            EdgeMutation::delete(
                edge,
                EdgeTypeId::new(1),
                ElementId::new(1),
                ElementId::new(2),
                interval(),
            )
            .expect("edge removal"),
        ),
    )
    .expect("commit edge removal");

    let removed = block_on(project_delta_bounded(
        &store,
        GraphId::new(7),
        ValidTime::from_micros(5),
        tx(350),
        tx(450),
        true,
        Some(9),
        limits,
    ))
    .expect("removed delta");
    assert_eq!(removed.edges().len(), 1);
    assert_eq!(
        removed.edges()[0].change(),
        analytics_api::DeltaKind::Removed
    );
    assert_eq!(removed.edges()[0].before_weight(), Some(4.0));
    assert_eq!(removed.edges()[0].after_weight(), None);
}

#[test]
fn delta_projection_rejects_reversed_transaction_order() {
    let store = TemporalStore::new(MemoryAdapter::new());
    seed(&store);

    assert_eq!(
        block_on(project_delta_bounded(
            &store,
            GraphId::new(7),
            ValidTime::from_micros(5),
            tx(250),
            tx(150),
            true,
            Some(9),
            ProjectionLimits::new(16, 16, 1 << 20).unwrap(),
        )),
        Err(ProjectionError::InvalidDeltaOrder)
    );
}

#[test]
fn valid_time_delta_compares_two_business_time_views_at_one_transaction_snapshot() {
    let store = TemporalStore::new(MemoryAdapter::new());
    seed(&store);
    let limits = ProjectionLimits::new(16, 16, 1 << 20).unwrap();

    let delta = block_on(project_valid_time_delta_bounded(
        &store,
        GraphId::new(7),
        ValidTime::from_micros(0),
        ValidTime::from_micros(5),
        tx(250),
        true,
        Some(9),
        limits,
    ))
    .expect("valid-time delta projection");

    assert_eq!(delta.vertices().len(), 2);
    assert!(
        delta
            .vertices()
            .iter()
            .all(|vertex| vertex.change() == DeltaKind::Added)
    );
    assert_eq!(delta.edges().len(), 1);
    assert_eq!(delta.edges()[0].change(), DeltaKind::Added);
    assert_eq!(
        block_on(project_valid_time_delta_bounded(
            &store,
            GraphId::new(7),
            ValidTime::from_micros(5),
            ValidTime::from_micros(0),
            tx(250),
            true,
            Some(9),
            limits,
        )),
        Err(ProjectionError::InvalidDeltaOrder)
    );
}

#[test]
fn interval_projection_limits_the_number_of_projected_segments() {
    let store = TemporalStore::new(MemoryAdapter::new());
    seed(&store);

    assert_eq!(
        block_on(project_interval_bounded(
            &store,
            GraphId::new(7),
            Interval::new(ValidTime::from_micros(0), Some(ValidTime::from_micros(10))).unwrap(),
            tx(250),
            true,
            Some(9),
            ProjectionLimits::new(1, 16, 1 << 20).unwrap(),
        )),
        Err(ProjectionError::VertexLimit)
    );
}

#[test]
fn interval_part_usage_charges_identity_scans_and_materialized_segments() {
    let store = TemporalStore::new(MemoryAdapter::new());
    let vertex = ElementRef::vertex(GraphId::new(7), PartitionId::new(0), ElementId::new(1));
    block_on(
        store.commit_vertex(
            CommitContext::new(0, 1, 1, tx(0), tx(100)),
            VertexMutation::put(
                vertex,
                LabelId::new(1),
                Interval::new(ValidTime::from_micros(1), Some(ValidTime::from_micros(10))).unwrap(),
                CanonicalElement::new(1, BTreeMap::new()),
            )
            .unwrap(),
        ),
    )
    .unwrap();
    block_on(
        store.commit_vertex(
            CommitContext::new(0, 2, 2, tx(100), tx(200)),
            VertexMutation::put(
                vertex,
                LabelId::new(1),
                Interval::new(ValidTime::from_micros(4), Some(ValidTime::from_micros(7))).unwrap(),
                CanonicalElement::new(
                    1,
                    BTreeMap::from([(8, GraphValue::String("corrected".into()))]),
                ),
            )
            .unwrap(),
        ),
    )
    .unwrap();

    let (part, usage) = block_on(project_interval_part_bounded(
        &store,
        GraphId::new(7),
        Interval::new(ValidTime::from_micros(1), Some(ValidTime::from_micros(10))).unwrap(),
        tx(250),
        None,
        ProjectionLimits::new(8, 8, 1 << 20).unwrap(),
    ))
    .expect("interval part");
    let (vertices, edges) = part.into_parts();

    assert_eq!(vertices.len(), 3);
    assert!(edges.is_empty());
    assert_eq!(usage.vertices(), 3);
    assert_eq!(usage.edges(), 0);
    assert!(usage.bytes() > 0);
}

#[test]
fn interval_projection_enforces_byte_limit_during_segment_materialization() {
    let store = TemporalStore::new(MemoryAdapter::new());
    seed(&store);

    assert_eq!(
        block_on(project_interval_bounded(
            &store,
            GraphId::new(7),
            Interval::new(ValidTime::from_micros(0), Some(ValidTime::from_micros(10))).unwrap(),
            tx(250),
            true,
            Some(9),
            ProjectionLimits::new(16, 16, 1).unwrap(),
        )),
        Err(ProjectionError::ByteLimit)
    );
}

#[test]
fn delta_projection_tracks_parallel_edges_by_stable_identity() {
    let store = TemporalStore::new(MemoryAdapter::new());
    seed(&store);
    block_on(
        store.commit_edge(
            CommitContext::new(0, 4, 4, tx(200), tx(300)),
            EdgeMutation::put(
                ElementRef::edge(GraphId::new(7), PartitionId::new(0), ElementId::new(4)),
                EdgeTypeId::new(1),
                ElementId::new(1),
                ElementId::new(2),
                interval(),
                CanonicalElement::new(
                    1,
                    BTreeMap::from([(9, GraphValue::FloatBits(2.5_f64.to_bits()))]),
                ),
            )
            .expect("parallel edge"),
        ),
    )
    .expect("commit parallel edge");
    block_on(
        store.commit_edge(
            CommitContext::new(0, 5, 5, tx(300), tx(400)),
            EdgeMutation::put(
                ElementRef::edge(GraphId::new(7), PartitionId::new(0), ElementId::new(3)),
                EdgeTypeId::new(1),
                ElementId::new(1),
                ElementId::new(2),
                interval(),
                CanonicalElement::new(
                    1,
                    BTreeMap::from([(9, GraphValue::FloatBits(4.0_f64.to_bits()))]),
                ),
            )
            .expect("original edge update"),
        ),
    )
    .expect("commit original edge update");

    let delta = block_on(project_delta_bounded(
        &store,
        GraphId::new(7),
        ValidTime::from_micros(5),
        tx(250),
        tx(450),
        true,
        Some(9),
        ProjectionLimits::new(16, 16, 1 << 20).unwrap(),
    ))
    .expect("parallel edge delta");

    assert_eq!(delta.edges().len(), 2);
    assert_eq!(delta.edges()[0].change(), DeltaKind::Updated);
    assert_eq!(delta.edges()[0].edge_id(), EdgeId::new(3));
    assert_eq!(delta.edges()[1].change(), DeltaKind::Added);
    assert_eq!(delta.edges()[1].edge_id(), EdgeId::new(4));
}

#[test]
fn delta_projection_reports_vertex_and_non_weight_edge_payload_updates() {
    let store = TemporalStore::new(MemoryAdapter::new());
    seed(&store);
    let vertex = ElementRef::vertex(GraphId::new(7), PartitionId::new(0), ElementId::new(1));
    let edge = ElementRef::edge(GraphId::new(7), PartitionId::new(0), ElementId::new(3));
    let vertex_after = CanonicalElement::new(
        1,
        BTreeMap::from([(8, GraphValue::String("new vertex".to_owned()))]),
    );
    let edge_before = CanonicalElement::new(
        1,
        BTreeMap::from([(9, GraphValue::FloatBits(2.5_f64.to_bits()))]),
    );
    let edge_after = CanonicalElement::new(
        1,
        BTreeMap::from([
            (9, GraphValue::FloatBits(2.5_f64.to_bits())),
            (8, GraphValue::String("new edge property".to_owned())),
        ]),
    );
    block_on(
        store.commit_vertex(
            CommitContext::new(0, 4, 4, tx(200), tx(300)),
            VertexMutation::put(vertex, LabelId::new(1), interval(), vertex_after.clone())
                .expect("vertex update"),
        ),
    )
    .expect("commit vertex update");
    block_on(
        store.commit_edge(
            CommitContext::new(0, 5, 5, tx(300), tx(400)),
            EdgeMutation::put(
                edge,
                EdgeTypeId::new(1),
                ElementId::new(1),
                ElementId::new(2),
                interval(),
                edge_after.clone(),
            )
            .expect("edge update"),
        ),
    )
    .expect("commit edge update");

    let delta = block_on(project_delta_bounded(
        &store,
        GraphId::new(7),
        ValidTime::from_micros(5),
        tx(250),
        tx(450),
        true,
        Some(9),
        ProjectionLimits::new(16, 16, 1 << 20).unwrap(),
    ))
    .expect("payload delta");

    assert_eq!(delta.vertices().len(), 1);
    assert_eq!(delta.vertices()[0].change(), DeltaKind::Updated);
    assert_eq!(
        delta.vertices()[0].before_payload(),
        Some(&CanonicalElement::new(1, BTreeMap::new()))
    );
    assert_eq!(delta.vertices()[0].after_payload(), Some(&vertex_after));
    assert_eq!(delta.edges().len(), 1);
    assert_eq!(delta.edges()[0].change(), DeltaKind::Updated);
    assert_eq!(delta.edges()[0].before_payload(), Some(&edge_before));
    assert_eq!(delta.edges()[0].after_payload(), Some(&edge_after));
    assert_eq!(delta.edges()[0].before_weight(), Some(2.5));
    assert_eq!(delta.edges()[0].after_weight(), Some(2.5));
}

#[test]
fn delta_projection_shares_one_budget_across_both_views() {
    let store = TemporalStore::new(MemoryAdapter::new());
    seed(&store);

    assert_eq!(
        block_on(project_delta_bounded(
            &store,
            GraphId::new(7),
            ValidTime::from_micros(5),
            tx(150),
            tx(250),
            true,
            Some(9),
            ProjectionLimits::new(3, 16, 1 << 20).unwrap(),
        )),
        Err(ProjectionError::VertexLimit)
    );
}

#[test]
fn delta_projection_accepts_an_exact_shared_byte_budget_without_edges() {
    let store = TemporalStore::new(MemoryAdapter::new());
    let vertex = ElementRef::vertex(GraphId::new(7), PartitionId::new(0), ElementId::new(1));
    let before_payload = CanonicalElement::new(
        1,
        BTreeMap::from([(8, GraphValue::String("before".to_owned()))]),
    );
    let after_payload = CanonicalElement::new(
        1,
        BTreeMap::from([(8, GraphValue::String("after".to_owned()))]),
    );
    block_on(store.commit_vertex(
        CommitContext::new(0, 1, 1, tx(0), tx(100)),
        VertexMutation::put(vertex, LabelId::new(1), interval(), before_payload.clone()).unwrap(),
    ))
    .unwrap();
    block_on(store.commit_vertex(
        CommitContext::new(0, 2, 2, tx(100), tx(200)),
        VertexMutation::put(vertex, LabelId::new(1), interval(), after_payload.clone()).unwrap(),
    ))
    .unwrap();

    let (_, before_bytes, before_entries) = block_on(store.scan_vertex_views_as_of_bounded(
        GraphId::new(7),
        ValidTime::from_micros(5),
        tx(150),
        8,
        1 << 20,
    ))
    .unwrap();
    let (_, after_bytes, after_entries) = block_on(store.scan_vertex_views_as_of_bounded(
        GraphId::new(7),
        ValidTime::from_micros(5),
        tx(250),
        8,
        1 << 20,
    ))
    .unwrap();
    let exact_bytes = before_bytes.checked_add(after_bytes).unwrap();
    let exact_entries = before_entries.checked_add(after_entries).unwrap();

    let delta = block_on(project_delta_bounded(
        &store,
        GraphId::new(7),
        ValidTime::from_micros(5),
        tx(150),
        tx(250),
        true,
        None,
        ProjectionLimits::new(exact_entries, 1, exact_bytes).unwrap(),
    ))
    .expect("an empty edge view must not consume the exact vertex byte boundary");

    assert_eq!(delta.vertices().len(), 1);
    assert_eq!(delta.vertices()[0].change(), DeltaKind::Updated);
    assert_eq!(delta.vertices()[0].before_payload(), Some(&before_payload));
    assert_eq!(delta.vertices()[0].after_payload(), Some(&after_payload));
    assert!(delta.edges().is_empty());
}

#[test]
fn delta_projection_rejects_an_exact_vertex_budget_when_edges_exist() {
    let store = TemporalStore::new(MemoryAdapter::new());
    seed(&store);
    let (_, before_vertex_bytes, before_vertex_entries) =
        block_on(store.scan_vertex_views_as_of_bounded(
            GraphId::new(7),
            ValidTime::from_micros(5),
            tx(250),
            8,
            1 << 20,
        ))
        .unwrap();
    let (_, before_edge_bytes, before_edge_entries) = block_on(store.scan_edges_as_of_bounded(
        GraphId::new(7),
        ValidTime::from_micros(5),
        tx(250),
        8,
        1 << 20,
    ))
    .unwrap();
    let (_, after_vertex_bytes, after_vertex_entries) =
        block_on(store.scan_vertex_views_as_of_bounded(
            GraphId::new(7),
            ValidTime::from_micros(5),
            tx(250),
            8,
            1 << 20,
        ))
        .unwrap();
    let bytes_without_after_edges = before_vertex_bytes
        .checked_add(before_edge_bytes)
        .and_then(|bytes| bytes.checked_add(after_vertex_bytes))
        .unwrap();

    assert_eq!(
        block_on(project_delta_bounded(
            &store,
            GraphId::new(7),
            ValidTime::from_micros(5),
            tx(250),
            tx(250),
            true,
            Some(9),
            ProjectionLimits::new(
                before_vertex_entries + after_vertex_entries,
                before_edge_entries * 2,
                bytes_without_after_edges,
            )
            .unwrap(),
        )),
        Err(ProjectionError::ByteLimit)
    );
}

fn seed(store: &TemporalStore<MemoryAdapter>) {
    for id in [1_u128, 2] {
        block_on(
            store.commit_vertex(
                CommitContext::new(0, id as u64, id, tx(0), tx(100)),
                VertexMutation::put(
                    ElementRef::vertex(GraphId::new(7), PartitionId::new(0), ElementId::new(id)),
                    LabelId::new(1),
                    interval(),
                    CanonicalElement::new(1, BTreeMap::new()),
                )
                .expect("vertex"),
            ),
        )
        .expect("commit vertex");
    }
    block_on(
        store.commit_edge(
            CommitContext::new(0, 3, 3, tx(100), tx(200)),
            EdgeMutation::put(
                ElementRef::edge(GraphId::new(7), PartitionId::new(0), ElementId::new(3)),
                EdgeTypeId::new(1),
                ElementId::new(1),
                ElementId::new(2),
                interval(),
                CanonicalElement::new(
                    1,
                    BTreeMap::from([(9, GraphValue::FloatBits(2.5_f64.to_bits()))]),
                ),
            )
            .expect("edge"),
        ),
    )
    .expect("commit edge");
}

fn interval() -> Interval<ValidTime> {
    Interval::new(ValidTime::from_micros(1), None).expect("interval")
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
