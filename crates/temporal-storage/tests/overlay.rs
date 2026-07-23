use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use adapter_memory::MemoryAdapter;
use temporal_storage::{
    CommitContext, ElementId, ElementRef, GraphId, LabelId, PartitionId, TemporalStore,
    TransactionOverlay, VertexMutation,
};
use temporal_types::{CanonicalElement, GraphValue, Interval, TransactionTime, ValidTime};

#[test]
fn later_statement_replaces_the_same_element_and_valid_interval() {
    let mut overlay = TransactionOverlay::new(4).expect("overlay");
    overlay
        .stage(vertex_write("before", 1, 10))
        .expect("first statement");
    overlay
        .stage(vertex_write("after", 1, 10))
        .expect("second statement");

    let store = TemporalStore::new(MemoryAdapter::new());
    block_on(store.commit_transaction(context(1, 100), overlay.into_transaction()))
        .expect("commit");
    assert_eq!(
        block_on(store.vertex_current(vertex(), ValidTime::from_micros(5))).unwrap(),
        Some(payload("after"))
    );
}

#[test]
fn rollback_to_savepoint_discards_later_statement_writes() {
    let mut overlay = TransactionOverlay::new(4).expect("overlay");
    overlay
        .stage(vertex_write("before", 1, 10))
        .expect("first statement");
    let savepoint = overlay.savepoint();
    overlay
        .stage(vertex_write("discarded", 1, 10))
        .expect("second statement");
    overlay.rollback_to(savepoint).expect("rollback");

    let store = TemporalStore::new(MemoryAdapter::new());
    block_on(store.commit_transaction(context(1, 100), overlay.into_transaction()))
        .expect("commit");
    assert_eq!(
        block_on(store.vertex_current(vertex(), ValidTime::from_micros(5))).unwrap(),
        Some(payload("before"))
    );
}

fn vertex_write(value: &str, start: i64, end: i64) -> temporal_storage::TemporalTransaction {
    temporal_storage::TemporalTransaction::new().with_vertex(
        VertexMutation::put(
            vertex(),
            LabelId::new(1),
            interval(start, end),
            payload(value),
        )
        .expect("vertex mutation"),
    )
}

fn vertex() -> ElementRef {
    ElementRef::vertex(GraphId::new(7), PartitionId::new(0), ElementId::new(1))
}

fn interval(start: i64, end: i64) -> Interval<ValidTime> {
    Interval::new(
        ValidTime::from_micros(start),
        Some(ValidTime::from_micros(end)),
    )
    .expect("interval")
}

fn payload(value: &str) -> CanonicalElement {
    CanonicalElement::new(
        1,
        BTreeMap::from([(1, GraphValue::String(value.to_owned()))]),
    )
}

fn context(log_index: u64, commit: i64) -> CommitContext {
    CommitContext::new(
        0,
        log_index,
        u128::from(log_index),
        TransactionTime::new(commit - 1, 0),
        TransactionTime::new(commit, 0),
    )
}

fn block_on<F: Future>(future: F) -> F::Output {
    struct Noop;
    impl Wake for Noop {
        fn wake(self: Arc<Self>) {}
    }
    let waker = Waker::from(Arc::new(Noop));
    let mut context = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(value) => return value,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}
