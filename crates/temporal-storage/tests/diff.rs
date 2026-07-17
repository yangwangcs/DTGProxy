use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use adapter_memory::MemoryAdapter;
use temporal_storage::{
    CommitContext, EdgeMutation, EdgeTypeId, ElementId, ElementRef, GraphId, LabelId, PartitionId,
    TemporalChange, TemporalChangeKind, TemporalStore, TemporalStoreError, VertexMutation,
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

fn vertex() -> ElementRef {
    ElementRef::vertex(GraphId::new(1), PartitionId::new(0), ElementId::new(7))
}

fn edge() -> ElementRef {
    ElementRef::edge(GraphId::new(1), PartitionId::new(0), ElementId::new(70))
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

#[test]
fn diff_reports_coalesced_added_changed_and_removed_valid_ranges() {
    let store = TemporalStore::new(MemoryAdapter::new());
    let label = LabelId::new(1);
    block_on(store.commit_vertex(
        context(1, 0, 100),
        VertexMutation::put(vertex(), label, interval(1, Some(10)), payload("a")).unwrap(),
    ))
    .unwrap();
    block_on(store.commit_vertex(
        context(2, 100, 200),
        VertexMutation::put(vertex(), label, interval(4, Some(7)), payload("b")).unwrap(),
    ))
    .unwrap();
    block_on(store.commit_vertex(
        context(3, 200, 300),
        VertexMutation::delete(vertex(), label, interval(8, None)).unwrap(),
    ))
    .unwrap();

    assert_eq!(
        block_on(store.diff_vertex(vertex(), tx(50), tx(150))).unwrap(),
        vec![TemporalChange::new(
            interval(1, Some(10)),
            TemporalChangeKind::Added {
                after: payload("a")
            }
        )]
    );
    assert_eq!(
        block_on(store.diff_vertex(vertex(), tx(150), tx(250))).unwrap(),
        vec![TemporalChange::new(
            interval(4, Some(7)),
            TemporalChangeKind::Changed {
                before: payload("a"),
                after: payload("b"),
            }
        )]
    );
    assert_eq!(
        block_on(store.diff_vertex(vertex(), tx(250), tx(350))).unwrap(),
        vec![TemporalChange::new(
            interval(8, Some(10)),
            TemporalChangeKind::Removed {
                before: payload("a")
            }
        )]
    );
}

#[test]
fn diff_rejects_reversed_transaction_snapshots() {
    let store = TemporalStore::new(MemoryAdapter::new());

    assert_eq!(
        block_on(store.diff_vertex(vertex(), tx(20), tx(10))),
        Err(TemporalStoreError::InvalidDiffOrder)
    );
}

#[test]
fn edge_diff_uses_the_same_bitemporal_partitioning() {
    let store = TemporalStore::new(MemoryAdapter::new());
    block_on(
        store.commit_edge(
            context(1, 0, 100),
            EdgeMutation::put(
                edge(),
                EdgeTypeId::new(9),
                ElementId::new(10),
                ElementId::new(20),
                interval(1, None),
                payload("edge"),
            )
            .unwrap(),
        ),
    )
    .unwrap();
    block_on(
        store.commit_edge(
            context(2, 100, 200),
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
        block_on(store.diff_edge(edge(), tx(150), tx(250))).unwrap(),
        vec![TemporalChange::new(
            interval(4, Some(7)),
            TemporalChangeKind::Removed {
                before: payload("edge")
            }
        )]
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
