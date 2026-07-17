use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use adapter_memory::MemoryAdapter;
use storage_api::{CommittedMutationBatch, Mutation, StorageAdapter};
use temporal_model::Timeline;
use temporal_storage::{
    CommitContext, ElementId, ElementRef, GraphId, LabelId, PartitionId, RecordCodecError,
    TemporalStore, TemporalStoreError, VertexMutation, history_anchor_key,
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

fn payload(name: &str) -> CanonicalElement {
    CanonicalElement::new(
        1,
        BTreeMap::from([
            (1, GraphValue::String(name.to_owned())),
            (2, GraphValue::Integer(42)),
            (3, GraphValue::Bytes(vec![0, 255])),
        ]),
    )
}

fn context(log_index: u64, read: i64, commit: i64) -> CommitContext {
    CommitContext::new(3, log_index, u128::from(log_index), tx(read), tx(commit))
}

#[test]
fn retroactive_vertex_correction_round_trips_current_and_transaction_history() {
    let store = TemporalStore::new(MemoryAdapter::new());
    let label = LabelId::new(11);
    let old = payload("old");
    let corrected = payload("corrected");
    let mut oracle = Timeline::new();

    block_on(store.commit_vertex(
        context(1, 0, 100),
        VertexMutation::put(vertex(), label, interval(1, Some(10)), old.clone()).unwrap(),
    ))
    .unwrap();
    oracle
        .put_initial(interval(1, Some(10)), old.clone(), tx(100))
        .unwrap();

    block_on(store.commit_vertex(
        context(2, 100, 200),
        VertexMutation::put(vertex(), label, interval(4, Some(7)), corrected.clone()).unwrap(),
    ))
    .unwrap();
    oracle
        .correct(interval(4, Some(7)), corrected.clone(), tx(100), tx(200))
        .unwrap();

    for point in [2, 5, 8, 11] {
        assert_eq!(
            block_on(store.vertex_current(vertex(), valid(point))).unwrap(),
            oracle.value_at(valid(point), tx(250)).unwrap().cloned()
        );
    }
    assert_eq!(
        block_on(store.vertex_as_of(vertex(), valid(5), tx(150))).unwrap(),
        Some(old)
    );
    assert_eq!(
        block_on(store.vertex_as_of(vertex(), valid(5), tx(250))).unwrap(),
        Some(corrected)
    );
    assert_eq!(store.adapter().applied_log_index().unwrap(), 2);
}

#[test]
fn disjoint_insert_and_partial_delete_preserve_unaffected_valid_ranges() {
    let store = TemporalStore::new(MemoryAdapter::new());
    let label = LabelId::new(11);

    block_on(store.commit_vertex(
        context(1, 0, 100),
        VertexMutation::put(vertex(), label, interval(1, Some(4)), payload("a")).unwrap(),
    ))
    .unwrap();
    block_on(store.commit_vertex(
        context(2, 100, 200),
        VertexMutation::put(vertex(), label, interval(7, None), payload("b")).unwrap(),
    ))
    .unwrap();
    block_on(store.commit_vertex(
        context(3, 200, 300),
        VertexMutation::delete(vertex(), label, interval(8, Some(10))).unwrap(),
    ))
    .unwrap();

    assert_eq!(
        block_on(store.vertex_current(vertex(), valid(2))).unwrap(),
        Some(payload("a"))
    );
    assert_eq!(
        block_on(store.vertex_current(vertex(), valid(5))).unwrap(),
        None
    );
    assert_eq!(
        block_on(store.vertex_current(vertex(), valid(7))).unwrap(),
        Some(payload("b"))
    );
    assert_eq!(
        block_on(store.vertex_current(vertex(), valid(9))).unwrap(),
        None
    );
    assert_eq!(
        block_on(store.vertex_current(vertex(), valid(11))).unwrap(),
        Some(payload("b"))
    );
    assert_eq!(
        block_on(store.vertex_as_of(vertex(), valid(9), tx(250))).unwrap(),
        Some(payload("b"))
    );
}

#[test]
fn identical_commit_replay_is_deterministic() {
    let store = TemporalStore::new(MemoryAdapter::new());
    let mutation = VertexMutation::put(
        vertex(),
        LabelId::new(11),
        interval(1, None),
        payload("same"),
    )
    .unwrap();
    let commit = context(1, 0, 100);

    let first = block_on(store.commit_vertex(commit, mutation.clone())).unwrap();
    let replay = block_on(store.commit_vertex(commit, mutation)).unwrap();

    assert!(!first.duplicate);
    assert!(replay.duplicate);
    assert_eq!(
        block_on(store.vertex_current(vertex(), valid(5))).unwrap(),
        Some(payload("same"))
    );
}

#[test]
fn invalid_time_identity_change_and_stale_overlap_are_rejected_without_partial_writes() {
    let store = TemporalStore::new(MemoryAdapter::new());
    let label = LabelId::new(11);
    block_on(store.commit_vertex(
        context(1, 0, 100),
        VertexMutation::put(vertex(), label, interval(1, Some(5)), payload("a")).unwrap(),
    ))
    .unwrap();

    let bad_time = block_on(store.commit_vertex(
        context(2, 100, 100),
        VertexMutation::put(vertex(), label, interval(6, None), payload("b")).unwrap(),
    ))
    .unwrap_err();
    assert_eq!(bad_time, TemporalStoreError::InvalidCommitOrder);

    let changed_identity = block_on(store.commit_vertex(
        context(2, 100, 200),
        VertexMutation::put(vertex(), LabelId::new(99), interval(6, None), payload("b")).unwrap(),
    ))
    .unwrap_err();
    assert_eq!(changed_identity, TemporalStoreError::IdentityMismatch);

    block_on(store.commit_vertex(
        context(2, 100, 200),
        VertexMutation::put(vertex(), label, interval(2, Some(4)), payload("new")).unwrap(),
    ))
    .unwrap();

    let stale = block_on(store.commit_vertex(
        context(3, 100, 300),
        VertexMutation::put(vertex(), label, interval(3, Some(6)), payload("stale")).unwrap(),
    ))
    .unwrap_err();
    assert_eq!(stale, TemporalStoreError::WriteConflict);
    assert_eq!(store.adapter().applied_log_index().unwrap(), 2);
}

#[test]
fn stale_writer_on_a_disjoint_valid_interval_can_commit() {
    let store = TemporalStore::new(MemoryAdapter::new());
    let label = LabelId::new(11);
    block_on(store.commit_vertex(
        context(1, 0, 100),
        VertexMutation::put(vertex(), label, interval(1, Some(3)), payload("a")).unwrap(),
    ))
    .unwrap();
    block_on(store.commit_vertex(
        context(2, 100, 200),
        VertexMutation::put(vertex(), label, interval(5, Some(7)), payload("b")).unwrap(),
    ))
    .unwrap();

    block_on(store.commit_vertex(
        context(3, 100, 300),
        VertexMutation::put(vertex(), label, interval(8, None), payload("c")).unwrap(),
    ))
    .unwrap();

    assert_eq!(
        block_on(store.vertex_current(vertex(), valid(6))).unwrap(),
        Some(payload("b")),
        "a non-overlapping stale writer must preserve later disjoint state"
    );
    assert_eq!(
        block_on(store.vertex_current(vertex(), valid(9))).unwrap(),
        Some(payload("c"))
    );
}

#[test]
fn as_of_seek_decodes_only_the_first_eligible_anchor() {
    let store = TemporalStore::new(MemoryAdapter::new());
    let label = LabelId::new(11);
    block_on(store.commit_vertex(
        context(1, 0, 100),
        VertexMutation::put(vertex(), label, interval(1, None), payload("old")).unwrap(),
    ))
    .unwrap();
    block_on(store.commit_vertex(
        context(2, 100, 200),
        VertexMutation::put(vertex(), label, interval(4, Some(7)), payload("new")).unwrap(),
    ))
    .unwrap();
    for log_index in 3..=18 {
        let read = i64::try_from((log_index - 1) * 100).unwrap();
        let commit = i64::try_from(log_index * 100).unwrap();
        block_on(
            store.commit_vertex(
                context(log_index, read, commit),
                VertexMutation::put(
                    vertex(),
                    label,
                    interval(20, Some(21)),
                    payload(&format!("filler-{log_index}")),
                )
                .unwrap(),
            ),
        )
        .unwrap();
    }
    block_on(store.adapter().apply_committed(CommittedMutationBatch {
        shard_id: 3,
        log_index: 19,
        txn_id: 999,
        mutations: vec![Mutation::put(
            0,
            history_anchor_key(vertex(), tx(100), 0),
            b"corrupt-old-anchor".to_vec(),
        )],
    }))
    .unwrap();

    assert_eq!(
        block_on(store.vertex_as_of(vertex(), valid(5), tx(1850))).unwrap(),
        Some(payload("new")),
        "replay must stop at the nearest anchor without decoding older records"
    );
    assert_eq!(
        block_on(store.vertex_as_of(vertex(), valid(5), tx(150))),
        Err(TemporalStoreError::Record(RecordCodecError::InvalidMagic))
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
