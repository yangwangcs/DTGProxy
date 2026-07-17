use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use adapter_memory::MemoryAdapter;
use storage_api::StorageAdapter;
use temporal_storage::{
    CommitContext, EdgeMutation, EdgeTypeId, ElementId, ElementRef, GraphId, LabelId, PartitionId,
    TemporalStore, TemporalStoreError, TemporalTransaction, VertexMutation,
};
use temporal_types::{CanonicalElement, GraphValue, Interval, TransactionTime, ValidTime};

fn tx(value: i64) -> TransactionTime {
    TransactionTime::new(value, 0)
}

fn valid(value: i64) -> ValidTime {
    ValidTime::from_micros(value)
}

fn interval(start: i64, end: Option<i64>) -> Interval<ValidTime> {
    Interval::new(valid(start), end.map(valid)).unwrap()
}

fn graph() -> GraphId {
    GraphId::new(1)
}

fn partition() -> PartitionId {
    PartitionId::new(0)
}

fn edge() -> ElementRef {
    ElementRef::edge(graph(), partition(), ElementId::new(70))
}

fn payload(name: &str) -> CanonicalElement {
    CanonicalElement::new(
        1,
        BTreeMap::from([(1, GraphValue::String(name.to_owned()))]),
    )
}

fn context(log_index: u64, read: i64, commit: i64) -> CommitContext {
    CommitContext::new(3, log_index, u128::from(log_index), tx(read), tx(commit))
}

fn put(valid: Interval<ValidTime>, value: &str) -> EdgeMutation {
    EdgeMutation::put(
        edge(),
        EdgeTypeId::new(9),
        ElementId::new(10),
        ElementId::new(20),
        valid,
        payload(value),
    )
    .unwrap()
}

fn seed_endpoints(store: &TemporalStore<MemoryAdapter>) {
    let transaction = TemporalTransaction::new()
        .with_vertex(
            VertexMutation::put(
                ElementRef::vertex(graph(), partition(), ElementId::new(10)),
                LabelId::new(1),
                interval(i64::MIN, None),
                payload("source"),
            )
            .unwrap(),
        )
        .with_vertex(
            VertexMutation::put(
                ElementRef::vertex(graph(), partition(), ElementId::new(20)),
                LabelId::new(1),
                interval(i64::MIN, None),
                payload("destination"),
            )
            .unwrap(),
        );
    block_on(store.commit_transaction(context(1, 0, 100), transaction)).unwrap();
}

#[test]
fn edge_round_trips_through_current_history_and_both_adjacency_directions() {
    let store = TemporalStore::new(MemoryAdapter::new());
    seed_endpoints(&store);
    block_on(store.commit_edge(context(2, 100, 200), put(interval(1, Some(10)), "knows"))).unwrap();

    assert_eq!(
        block_on(store.edge_current(edge(), valid(5))).unwrap(),
        Some(payload("knows"))
    );
    assert_eq!(
        block_on(store.edge_as_of(edge(), valid(5), tx(250))).unwrap(),
        Some(payload("knows"))
    );

    let outgoing =
        block_on(store.expand_out_current(graph(), partition(), ElementId::new(10), valid(5)))
            .unwrap();
    let incoming =
        block_on(store.expand_in_current(graph(), partition(), ElementId::new(20), valid(5)))
            .unwrap();
    assert_eq!(outgoing.len(), 1);
    assert_eq!(incoming.len(), 1);
    assert_eq!(outgoing[0], incoming[0]);
    assert_eq!(outgoing[0].element(), edge());
    assert_eq!(outgoing[0].edge_type(), EdgeTypeId::new(9));
    assert_eq!(outgoing[0].source(), ElementId::new(10));
    assert_eq!(outgoing[0].destination(), ElementId::new(20));
    assert_eq!(outgoing[0].payload(), &payload("knows"));
}

#[test]
fn adjacency_is_exactly_filtered_and_removed_only_when_projection_is_fully_absent() {
    let store = TemporalStore::new(MemoryAdapter::new());
    seed_endpoints(&store);
    block_on(store.commit_edge(context(2, 100, 200), put(interval(1, Some(10)), "edge"))).unwrap();
    block_on(
        store.commit_edge(
            context(3, 200, 300),
            EdgeMutation::delete(
                edge(),
                EdgeTypeId::new(9),
                ElementId::new(10),
                ElementId::new(20),
                interval(4, Some(7)),
            )
            .unwrap(),
        ),
    )
    .unwrap();

    assert_eq!(
        block_on(store.expand_out_current(graph(), partition(), ElementId::new(10), valid(3)))
            .unwrap()
            .len(),
        1
    );
    assert!(
        block_on(store.expand_out_current(graph(), partition(), ElementId::new(10), valid(5)))
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        block_on(store.edge_as_of(edge(), valid(5), tx(250))).unwrap(),
        Some(payload("edge"))
    );
    assert_eq!(
        block_on(store.edge_as_of(edge(), valid(5), tx(350))).unwrap(),
        None
    );

    block_on(
        store.commit_edge(
            context(4, 300, 400),
            EdgeMutation::delete(
                edge(),
                EdgeTypeId::new(9),
                ElementId::new(10),
                ElementId::new(20),
                interval(i64::MIN, None),
            )
            .unwrap(),
        ),
    )
    .unwrap();

    assert!(
        block_on(store.expand_out_current(graph(), partition(), ElementId::new(10), valid(3)))
            .unwrap()
            .is_empty()
    );
    assert!(
        block_on(store.expand_in_current(graph(), partition(), ElementId::new(20), valid(3)))
            .unwrap()
            .is_empty()
    );
}

#[test]
fn edge_identity_is_immutable_and_failure_does_not_advance_the_adapter() {
    let store = TemporalStore::new(MemoryAdapter::new());
    seed_endpoints(&store);
    block_on(store.commit_edge(context(2, 100, 200), put(interval(1, None), "edge"))).unwrap();

    let changed_endpoint = EdgeMutation::put(
        edge(),
        EdgeTypeId::new(9),
        ElementId::new(10),
        ElementId::new(99),
        interval(2, Some(3)),
        payload("bad"),
    )
    .unwrap();
    let error = block_on(store.commit_edge(context(3, 200, 300), changed_endpoint)).unwrap_err();

    assert_eq!(error, TemporalStoreError::IdentityMismatch);
    assert_eq!(store.adapter().applied_log_index().unwrap(), 2);
    assert_eq!(
        block_on(store.edge_current(edge(), valid(2))).unwrap(),
        Some(payload("edge"))
    );
}

#[test]
fn edge_mutation_rejects_a_vertex_identity_before_storage() {
    let vertex = ElementRef::vertex(graph(), partition(), ElementId::new(70));
    assert_eq!(
        EdgeMutation::put(
            vertex,
            EdgeTypeId::new(9),
            ElementId::new(10),
            ElementId::new(20),
            interval(1, None),
            payload("bad"),
        ),
        Err(TemporalStoreError::WrongElementKind)
    );
}

struct NoopWake;

impl Wake for NoopWake {
    fn wake(self: Arc<Self>) {}
}

fn block_on<F>(future: F) -> F::Output
where
    F: Future,
{
    let waker = Waker::from(Arc::new(NoopWake));
    let mut context = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}
