use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll, Wake, Waker};

use adapter_memory::MemoryAdapter;
use temporal_storage::{
    AdapterCallObserver, CommitContext, EdgeMutation, EdgeTypeId, ElementId, ElementRef, GraphId,
    LabelId, ObservedStorageAdapter, PartitionId, TemporalStore, TemporalTransaction,
    VertexMutation,
};
use temporal_types::{CanonicalElement, GraphValue, Interval, TransactionTime, ValidTime};

#[test]
fn historical_edge_lookup_and_expansion_include_edges_absent_from_current_adjacency() {
    let store = TemporalStore::new(MemoryAdapter::new());
    let lifetime = interval(i64::MIN, None);
    let initial = TemporalTransaction::new()
        .with_vertex(
            VertexMutation::put(vertex(1), LabelId::new(1), lifetime, payload("source")).unwrap(),
        )
        .with_vertex(
            VertexMutation::put(vertex(2), LabelId::new(1), lifetime, payload("destination"))
                .unwrap(),
        )
        .with_edge(
            EdgeMutation::put(
                edge(10),
                EdgeTypeId::new(9),
                ElementId::new(1),
                ElementId::new(2),
                lifetime,
                payload("historical-edge"),
            )
            .unwrap(),
        );
    block_on(store.commit_transaction(context(1, 0, 100), initial)).unwrap();
    block_on(
        store.commit_edge(
            context(2, 100, 200),
            EdgeMutation::delete(
                edge(10),
                EdgeTypeId::new(9),
                ElementId::new(1),
                ElementId::new(2),
                lifetime,
            )
            .unwrap(),
        ),
    )
    .unwrap();

    assert!(
        block_on(store.expand_out_current(graph(), partition(), ElementId::new(1), valid(5)))
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        block_on(store.edge_view_current(edge(10), valid(5))).unwrap(),
        None
    );

    let historical = block_on(store.edge_view_as_of(edge(10), valid(5), tx(150)))
        .unwrap()
        .unwrap();
    assert_eq!(historical.element(), edge(10));
    assert_eq!(historical.edge_type(), EdgeTypeId::new(9));
    assert_eq!(historical.source(), ElementId::new(1));
    assert_eq!(historical.destination(), ElementId::new(2));
    assert_eq!(historical.payload(), &payload("historical-edge"));

    let outgoing = block_on(store.expand_out_as_of(
        graph(),
        partition(),
        ElementId::new(1),
        valid(5),
        tx(150),
    ))
    .unwrap();
    let incoming =
        block_on(store.expand_in_as_of(graph(), partition(), ElementId::new(2), valid(5), tx(150)))
            .unwrap();
    assert_eq!(outgoing, vec![historical.clone()]);
    assert_eq!(incoming, vec![historical]);
}

#[derive(Default)]
struct HistoryScanCounter {
    canonical_scans: AtomicU64,
    canonical_batch_scans: AtomicU64,
}

impl AdapterCallObserver for HistoryScanCounter {
    fn record_adapter_call(&self) {}

    fn record_canonical_scan(&self) {
        self.canonical_scans.fetch_add(1, Ordering::Relaxed);
    }

    fn record_canonical_batch_scan(&self) {
        self.canonical_batch_scans.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn historical_expansion_batches_matching_edge_replay() {
    let counter = Arc::new(HistoryScanCounter::default());
    let adapter = ObservedStorageAdapter::new(MemoryAdapter::new(), Arc::clone(&counter));
    let store = TemporalStore::new(adapter);
    let lifetime = interval(i64::MIN, None);
    let mut initial = TemporalTransaction::new()
        .with_vertex(
            VertexMutation::put(vertex(1), LabelId::new(1), lifetime, payload("source")).unwrap(),
        )
        .with_vertex(
            VertexMutation::put(vertex(2), LabelId::new(1), lifetime, payload("destination"))
                .unwrap(),
        );
    let mut deleted = TemporalTransaction::new();
    for edge_id in 10..19 {
        initial = initial.with_edge(
            EdgeMutation::put(
                edge(edge_id),
                EdgeTypeId::new(9),
                ElementId::new(1),
                ElementId::new(2),
                lifetime,
                payload(&format!("historical-edge-{edge_id}")),
            )
            .unwrap(),
        );
        deleted = deleted.with_edge(
            EdgeMutation::delete(
                edge(edge_id),
                EdgeTypeId::new(9),
                ElementId::new(1),
                ElementId::new(2),
                lifetime,
            )
            .unwrap(),
        );
    }
    block_on(store.commit_transaction(context(1, 0, 100), initial)).unwrap();
    block_on(store.commit_transaction(context(2, 100, 200), deleted)).unwrap();

    let edges = block_on(store.expand_out_as_of(
        graph(),
        partition(),
        ElementId::new(1),
        valid(5),
        tx(150),
    ))
    .unwrap();

    assert_eq!(edges.len(), 9);
    assert_eq!(counter.canonical_batch_scans.load(Ordering::Relaxed), 1);
    assert_eq!(counter.canonical_scans.load(Ordering::Relaxed), 0);
}

fn graph() -> GraphId {
    GraphId::new(1)
}

fn partition() -> PartitionId {
    PartitionId::new(0)
}

fn vertex(id: u128) -> ElementRef {
    ElementRef::vertex(graph(), partition(), ElementId::new(id))
}

fn edge(id: u128) -> ElementRef {
    ElementRef::edge(graph(), partition(), ElementId::new(id))
}

fn context(log_index: u64, read: i64, commit: i64) -> CommitContext {
    CommitContext::new(3, log_index, u128::from(log_index), tx(read), tx(commit))
}

fn tx(value: i64) -> TransactionTime {
    TransactionTime::new(value, 0)
}

fn valid(value: i64) -> ValidTime {
    ValidTime::from_micros(value)
}

fn interval(start: i64, end: Option<i64>) -> Interval<ValidTime> {
    Interval::new(valid(start), end.map(valid)).unwrap()
}

fn payload(value: &str) -> CanonicalElement {
    CanonicalElement::new(
        1,
        BTreeMap::from([(1, GraphValue::String(value.to_owned()))]),
    )
}

struct NoopWake;

impl Wake for NoopWake {
    fn wake(self: Arc<Self>) {}
}

fn block_on<F: Future>(future: F) -> F::Output {
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
