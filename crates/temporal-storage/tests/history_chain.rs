use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use adapter_memory::MemoryAdapter;
use storage_api::{CommittedMutationBatch, KeySpan, Keyspace, Mutation, StorageAdapter};
use temporal_storage::{
    CommitContext, ElementId, ElementRef, GraphId, HistoryAnchor, HistoryEntry, HistoryReadBudget,
    LabelId, PartitionId, ProjectionRecord, TemporalStore, TemporalStoreError, ValidSegment,
    VertexMutation, history_anchor_key, history_prefix,
};
use temporal_types::{CanonicalElement, GraphValue, Interval, TransactionTime, ValidTime};

fn tx(value: i64) -> TransactionTime {
    TransactionTime::new(value, 0)
}

fn valid(value: i64) -> ValidTime {
    ValidTime::from_micros(value)
}

fn interval() -> Interval<ValidTime> {
    Interval::new(valid(0), Some(valid(100))).unwrap()
}

fn vertex() -> ElementRef {
    ElementRef::vertex(GraphId::new(1), PartitionId::new(0), ElementId::new(1))
}

fn payload(value: u64) -> CanonicalElement {
    CanonicalElement::new(
        1,
        BTreeMap::from([(1, GraphValue::Integer(i64::try_from(value).unwrap()))]),
    )
}

fn context(log_index: u64, read: i64, commit: i64) -> CommitContext {
    CommitContext::new(3, log_index, u128::from(log_index), tx(read), tx(commit))
}

#[test]
fn policy_writes_at_most_fifteen_deltas_before_a_new_anchor() {
    let store = TemporalStore::new(MemoryAdapter::new());
    let label = LabelId::new(1);
    for log_index in 1..=17_u64 {
        let read = i64::try_from((log_index - 1) * 100).unwrap();
        let commit = i64::try_from(log_index * 100).unwrap();
        block_on(store.commit_vertex(
            context(log_index, read, commit),
            VertexMutation::put(vertex(), label, interval(), payload(log_index)).unwrap(),
        ))
        .unwrap();
    }

    let records = block_on(store.adapter().scan(&KeySpan::prefix(
        Keyspace::History,
        history_prefix(vertex()),
    )))
    .unwrap();
    let entries: Vec<_> = records
        .iter()
        .map(|record| HistoryEntry::decode(record.value()).unwrap())
        .collect();

    assert_eq!(entries.len(), 17);
    assert!(matches!(entries[0], HistoryEntry::Anchor(_)));
    assert_eq!(entries[0].commit_ts(), tx(1_700));
    assert!(matches!(entries[1], HistoryEntry::Delta(_)));
    assert!(matches!(entries[16], HistoryEntry::Anchor(_)));
    assert_eq!(
        entries
            .iter()
            .filter(|entry| matches!(entry, HistoryEntry::Delta(_)))
            .count(),
        15
    );
    assert!(records[1].value().len() < records[0].value().len());
    assert_eq!(
        block_on(store.vertex_as_of(vertex(), valid(50), tx(1_650))).unwrap(),
        Some(payload(16))
    );
    assert_eq!(
        block_on(store.vertex_as_of(vertex(), valid(50), tx(1_750))).unwrap(),
        Some(payload(17))
    );
}

#[test]
fn snapshot_bound_point_reads_remain_at_the_snapshot_applied_index() {
    let store = TemporalStore::new(MemoryAdapter::new());
    let label = LabelId::new(1);
    block_on(store.commit_vertex(
        context(1, 0, 100),
        VertexMutation::put(vertex(), label, interval(), payload(1)).unwrap(),
    ))
    .unwrap();
    let snapshot = block_on(store.begin_read_snapshot()).unwrap();
    assert_eq!(snapshot.applied_log_index(), 1);

    block_on(store.commit_vertex(
        context(2, 100, 200),
        VertexMutation::put(vertex(), label, interval(), payload(2)).unwrap(),
    ))
    .unwrap();
    let budget = HistoryReadBudget::new(16, 128 * 1024, 64 * 1024).unwrap();

    for _ in 0..2 {
        assert_eq!(
            block_on(store.vertex_as_of_in_snapshot(
                snapshot.as_ref(),
                vertex(),
                valid(50),
                tx(250),
                budget,
            ))
            .unwrap(),
            Some(payload(1))
        );
    }
    assert_eq!(snapshot.applied_log_index(), 1);
    assert_eq!(
        block_on(store.vertex_as_of(vertex(), valid(50), tx(250))).unwrap(),
        Some(payload(2))
    );
}

#[test]
fn missing_anchor_fails_closed_instead_of_returning_partial_history() {
    let store = TemporalStore::new(MemoryAdapter::new());
    let label = LabelId::new(1);
    block_on(store.commit_vertex(
        context(1, 0, 100),
        VertexMutation::put(vertex(), label, interval(), payload(1)).unwrap(),
    ))
    .unwrap();
    block_on(store.commit_vertex(
        context(2, 100, 200),
        VertexMutation::put(vertex(), label, interval(), payload(2)).unwrap(),
    ))
    .unwrap();
    block_on(store.adapter().apply_committed(CommittedMutationBatch {
        shard_id: 3,
        log_index: 3,
        txn_id: 999,
        mutations: vec![Mutation::delete(
            0,
            history_anchor_key(vertex(), tx(100), 0),
        )],
    }))
    .unwrap();

    assert_eq!(
        block_on(store.vertex_as_of(vertex(), valid(50), tx(250))),
        Err(TemporalStoreError::MissingHistoryAnchor)
    );
}

#[test]
fn phase_one_b_full_anchor_mixes_with_new_deltas() {
    let store = TemporalStore::new(MemoryAdapter::new());
    let label = LabelId::new(1);
    block_on(store.commit_vertex(
        context(1, 0, 100),
        VertexMutation::put(vertex(), label, interval(), payload(1)).unwrap(),
    ))
    .unwrap();
    block_on(store.commit_vertex(
        context(2, 100, 200),
        VertexMutation::put(vertex(), label, interval(), payload(2)).unwrap(),
    ))
    .unwrap();

    let projection =
        ProjectionRecord::new(tx(200), vec![ValidSegment::new(interval(), payload(2))]).unwrap();
    let phase_one_b_anchor = HistoryAnchor::new(tx(200), interval(), projection).unwrap();
    block_on(store.adapter().apply_committed(CommittedMutationBatch {
        shard_id: 3,
        log_index: 3,
        txn_id: 999,
        mutations: vec![Mutation::put(
            0,
            history_anchor_key(vertex(), tx(200), 0),
            phase_one_b_anchor.encode().unwrap(),
        )],
    }))
    .unwrap();

    block_on(store.commit_vertex(
        context(4, 200, 300),
        VertexMutation::put(vertex(), label, interval(), payload(3)).unwrap(),
    ))
    .unwrap();
    assert_eq!(
        block_on(store.vertex_as_of(vertex(), valid(50), tx(250))).unwrap(),
        Some(payload(2))
    );
    assert_eq!(
        block_on(store.vertex_as_of(vertex(), valid(50), tx(350))).unwrap(),
        Some(payload(3))
    );
}

#[test]
fn oversized_delta_forces_an_anchor_before_the_replay_byte_limit() {
    let store = TemporalStore::new(MemoryAdapter::new());
    let label = LabelId::new(1);
    for log_index in 1..=2_u64 {
        let read = i64::try_from((log_index - 1) * 100).unwrap();
        let commit = i64::try_from(log_index * 100).unwrap();
        let large = CanonicalElement::new(
            1,
            BTreeMap::from([(
                1,
                GraphValue::Bytes(vec![u8::try_from(log_index).unwrap(); 70 * 1024]),
            )]),
        );
        block_on(store.commit_vertex(
            context(log_index, read, commit),
            VertexMutation::put(vertex(), label, interval(), large).unwrap(),
        ))
        .unwrap();
    }

    let records = block_on(store.adapter().scan(&KeySpan::prefix(
        Keyspace::History,
        history_prefix(vertex()),
    )))
    .unwrap();
    assert!(matches!(
        HistoryEntry::decode(records[0].value()).unwrap(),
        HistoryEntry::Anchor(_)
    ));
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
