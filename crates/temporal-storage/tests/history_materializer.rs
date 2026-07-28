use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use adapter_memory::MemoryAdapter;
use temporal_storage::{
    CommitContext, ElementId, ElementRef, GraphId, HistoryReadBudget, IntervalHistoryMaterializer,
    LabelId, PartitionId, TemporalStore, VertexMutation,
};
use temporal_types::{CanonicalElement, GraphValue, Interval, TransactionTime, ValidTime};

#[test]
fn fifteen_corrections_materialize_each_payload_once() {
    let store = TemporalStore::new(MemoryAdapter::new());
    block_on(store.commit_vertex(
        context(1, 0, 100),
        VertexMutation::put(vertex(), LabelId::new(1), interval(0, 100), payload(0)).unwrap(),
    ))
    .unwrap();
    for correction in 1..=15 {
        block_on(
            store.commit_vertex(
                context(correction + 1, correction * 100, (correction + 1) * 100),
                VertexMutation::put(
                    vertex(),
                    LabelId::new(1),
                    interval(correction * 2, correction * 2 + 1),
                    payload(correction),
                )
                .unwrap(),
            ),
        )
        .unwrap();
    }

    let snapshot = block_on(store.begin_read_snapshot()).unwrap();
    let outcome = block_on(
        IntervalHistoryMaterializer::new(
            HistoryReadBudget::new(16, 16 * 1024 * 1024, 16 * 1024 * 1024 - 64).unwrap(),
        )
        .projection_at(snapshot.as_ref(), vertex(), tx(2_000)),
    )
    .unwrap();
    let projection = outcome.projection.unwrap();

    assert_eq!(projection.segments().len(), 31);
    for correction in 1..=15 {
        assert_eq!(
            projection.visible_at(valid(correction * 2)),
            Some(&payload(correction))
        );
    }
    assert_eq!(projection.visible_at(valid(99)), Some(&payload(0)));
    assert_eq!(outcome.stats.history_records, 16);
    assert_eq!(outcome.stats.payloads_decoded, 16);
    assert!(
        outcome.stats.payloads_decoded
            <= projection.segments().len() + outcome.stats.history_records - 1
    );
}

fn vertex() -> ElementRef {
    ElementRef::vertex(GraphId::new(1), PartitionId::new(0), ElementId::new(7))
}

fn context(log_index: i64, read: i64, commit: i64) -> CommitContext {
    CommitContext::new(3, log_index as u64, log_index as u128, tx(read), tx(commit))
}

fn tx(value: i64) -> TransactionTime {
    TransactionTime::new(value, 0)
}

fn valid(value: i64) -> ValidTime {
    ValidTime::from_micros(value)
}

fn interval(start: i64, end: i64) -> Interval<ValidTime> {
    Interval::new(valid(start), Some(valid(end))).unwrap()
}

fn payload(value: i64) -> CanonicalElement {
    CanonicalElement::new(1, BTreeMap::from([(1, GraphValue::Integer(value))]))
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
