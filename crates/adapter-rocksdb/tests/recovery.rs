use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use adapter_rocksdb::RocksAdapter;
use storage_api::{CommittedMutationBatch, Keyspace, LogicalKey, Mutation, StorageAdapter};

fn batch(log_index: u64, txn_id: u128, mutations: Vec<Mutation>) -> CommittedMutationBatch {
    CommittedMutationBatch {
        shard_id: 3,
        log_index,
        txn_id,
        mutations,
    }
}

#[test]
fn reopening_recovers_values_and_applied_log_index() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("database");
    let current = LogicalKey::in_keyspace(Keyspace::Current, b"same".to_vec());
    let history = LogicalKey::in_keyspace(Keyspace::History, b"same".to_vec());

    {
        let adapter = RocksAdapter::open(&path).unwrap();
        block_on(adapter.apply_committed(batch(
            1,
            20,
            vec![
                Mutation::put(0, current.clone(), b"current".to_vec()),
                Mutation::put(1, history.clone(), b"history".to_vec()),
            ],
        )))
        .unwrap();
    }

    let reopened = RocksAdapter::open(&path).unwrap();
    assert_eq!(reopened.applied_log_index().unwrap(), 1);
    assert_eq!(
        block_on(reopened.multi_get(&[current, history])).unwrap(),
        vec![Some(b"current".to_vec()), Some(b"history".to_vec())]
    );
}

#[test]
fn checkpoint_is_a_stable_snapshot_before_later_commits() {
    let root = tempfile::tempdir().unwrap();
    let source_path = root.path().join("source");
    let checkpoint_path = root.path().join("checkpoint");
    let key = LogicalKey::new(b"account:1".to_vec());
    let source = RocksAdapter::open(&source_path).unwrap();

    block_on(source.apply_committed(batch(
        1,
        21,
        vec![Mutation::put(0, key.clone(), b"v1".to_vec())],
    )))
    .unwrap();
    source.checkpoint(&checkpoint_path).unwrap();
    block_on(source.apply_committed(batch(
        2,
        22,
        vec![Mutation::put(0, key.clone(), b"v2".to_vec())],
    )))
    .unwrap();

    let checkpoint = RocksAdapter::open(&checkpoint_path).unwrap();
    assert_eq!(source.applied_log_index().unwrap(), 2);
    assert_eq!(checkpoint.applied_log_index().unwrap(), 1);
    assert_eq!(
        block_on(source.multi_get(std::slice::from_ref(&key))).unwrap(),
        vec![Some(b"v2".to_vec())]
    );
    assert_eq!(
        block_on(checkpoint.multi_get(&[key])).unwrap(),
        vec![Some(b"v1".to_vec())]
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
