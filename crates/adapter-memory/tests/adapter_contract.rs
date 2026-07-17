use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use adapter_memory::MemoryAdapter;
use storage_api::{
    AdapterError, CommittedMutationBatch, Keyspace, LogicalKey, Mutation, StorageAdapter,
};

fn key(value: &str) -> LogicalKey {
    LogicalKey::new(value.as_bytes().to_vec())
}

fn batch(log_index: u64, txn_id: u128, mutations: Vec<Mutation>) -> CommittedMutationBatch {
    CommittedMutationBatch {
        shard_id: 3,
        log_index,
        txn_id,
        mutations,
    }
}

#[test]
fn committed_batch_is_visible_and_identical_log_replay_is_idempotent() {
    let adapter = MemoryAdapter::new();
    let committed = batch(
        1,
        7,
        vec![Mutation::put(0, key("account:1"), b"A".to_vec())],
    );

    let first = block_on(adapter.apply_committed(committed.clone())).unwrap();
    let duplicate = block_on(adapter.apply_committed(committed)).unwrap();
    let values = block_on(adapter.multi_get(&[key("account:1"), key("missing")])).unwrap();

    assert_eq!(first.applied_log_index, 1);
    assert!(!first.duplicate);
    assert_eq!(duplicate.applied_log_index, 1);
    assert!(duplicate.duplicate);
    assert_eq!(values, vec![Some(b"A".to_vec()), None]);
    assert_eq!(adapter.applied_log_index().unwrap(), 1);
    let capabilities = adapter.capabilities();
    assert!(capabilities.local_atomic_batch);
    assert!(capabilities.idempotent_apply);
}

#[test]
fn mutation_replay_mismatch_rejects_the_entire_batch() {
    let adapter = MemoryAdapter::new();
    block_on(adapter.apply_committed(batch(
        1,
        7,
        vec![Mutation::put(0, key("protected"), b"old".to_vec())],
    )))
    .unwrap();

    let error = block_on(adapter.apply_committed(batch(
        2,
        7,
        vec![
            Mutation::put(1, key("new"), b"must-not-appear".to_vec()),
            Mutation::put(0, key("protected"), b"different".to_vec()),
        ],
    )))
    .unwrap_err();

    assert_eq!(
        error,
        AdapterError::MutationReplayMismatch {
            txn_id: 7,
            sequence: 0,
        }
    );
    assert_eq!(adapter.applied_log_index().unwrap(), 1);
    assert_eq!(
        block_on(adapter.multi_get(&[key("protected"), key("new")])).unwrap(),
        vec![Some(b"old".to_vec()), None]
    );
}

#[test]
fn adapter_rejects_a_non_contiguous_log_index() {
    let adapter = MemoryAdapter::new();

    let error = block_on(adapter.apply_committed(batch(2, 9, Vec::new()))).unwrap_err();

    assert_eq!(
        error,
        AdapterError::NonContiguousLogIndex {
            expected: 1,
            actual: 2,
        }
    );
    assert_eq!(adapter.applied_log_index().unwrap(), 0);
}

#[test]
fn adapter_rejects_different_content_for_an_applied_log_index() {
    let adapter = MemoryAdapter::new();
    block_on(adapter.apply_committed(batch(
        1,
        10,
        vec![Mutation::put(0, key("a"), b"one".to_vec())],
    )))
    .unwrap();

    let error = block_on(adapter.apply_committed(batch(
        1,
        10,
        vec![Mutation::put(0, key("a"), b"two".to_vec())],
    )))
    .unwrap_err();

    assert_eq!(
        error,
        AdapterError::CommittedLogReplayMismatch { log_index: 1 }
    );
}

#[test]
fn delete_mutation_removes_a_committed_key() {
    let adapter = MemoryAdapter::new();
    block_on(adapter.apply_committed(batch(
        1,
        11,
        vec![Mutation::put(0, key("a"), b"one".to_vec())],
    )))
    .unwrap();

    block_on(adapter.apply_committed(batch(2, 12, vec![Mutation::delete(0, key("a"))]))).unwrap();

    assert_eq!(
        block_on(adapter.multi_get(&[key("a")])).unwrap(),
        vec![None]
    );
}

#[test]
fn duplicate_sequence_inside_one_batch_is_rejected_atomically() {
    let adapter = MemoryAdapter::new();

    let error = block_on(adapter.apply_committed(batch(
        1,
        13,
        vec![
            Mutation::put(0, key("a"), b"one".to_vec()),
            Mutation::put(0, key("b"), b"two".to_vec()),
        ],
    )))
    .unwrap_err();

    assert_eq!(
        error,
        AdapterError::DuplicateMutationSequence {
            txn_id: 13,
            sequence: 0,
        }
    );
    assert_eq!(adapter.applied_log_index().unwrap(), 0);
    assert_eq!(
        block_on(adapter.multi_get(&[key("a"), key("b")])).unwrap(),
        vec![None, None]
    );
}

#[test]
fn identical_bytes_in_current_and_history_are_isolated() {
    let adapter = MemoryAdapter::new();
    let current = LogicalKey::in_keyspace(Keyspace::Current, b"same".to_vec());
    let history = LogicalKey::in_keyspace(Keyspace::History, b"same".to_vec());

    block_on(adapter.apply_committed(batch(
        1,
        14,
        vec![
            Mutation::put(0, current.clone(), b"current".to_vec()),
            Mutation::put(1, history.clone(), b"history".to_vec()),
        ],
    )))
    .unwrap();

    assert_eq!(
        block_on(adapter.multi_get(&[current, history])).unwrap(),
        vec![Some(b"current".to_vec()), Some(b"history".to_vec())]
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
