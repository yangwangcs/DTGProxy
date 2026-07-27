use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use adapter_memory::MemoryAdapter;
use storage_api::{KeySpan, Keyspace, LogicalKey, StorageAdapter};
use temporal_storage::{AdapterCallObserver, ObservedStorageAdapter};

#[derive(Default)]
struct QueryCounter(AtomicU64);

impl QueryCounter {
    fn count(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

impl AdapterCallObserver for QueryCounter {
    fn record_adapter_call(&self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn adapter_and_snapshot_reads_share_one_query_scoped_counter() {
    let counter = Arc::new(QueryCounter::default());
    let adapter = ObservedStorageAdapter::new(MemoryAdapter::new(), counter.clone());
    let key = LogicalKey::in_keyspace(Keyspace::Current, b"missing".to_vec());
    let span = KeySpan::prefix(Keyspace::Current, Vec::new());

    block_on(adapter.multi_get(std::slice::from_ref(&key))).unwrap();
    block_on(adapter.scan(&span)).unwrap();
    let snapshot = block_on(adapter.begin_read_snapshot()).unwrap();
    block_on(snapshot.multi_get(std::slice::from_ref(&key))).unwrap();
    block_on(snapshot.scan(&span)).unwrap();

    assert_eq!(counter.count(), 5);
}

#[test]
fn observers_are_isolated_between_queries() {
    let first = Arc::new(QueryCounter::default());
    let second = Arc::new(QueryCounter::default());
    let first_adapter = ObservedStorageAdapter::new(MemoryAdapter::new(), first.clone());
    let second_adapter = ObservedStorageAdapter::new(MemoryAdapter::new(), second.clone());
    let key = LogicalKey::in_keyspace(Keyspace::Current, b"missing".to_vec());

    block_on(first_adapter.multi_get(std::slice::from_ref(&key))).unwrap();
    block_on(first_adapter.multi_get(std::slice::from_ref(&key))).unwrap();
    block_on(second_adapter.multi_get(std::slice::from_ref(&key))).unwrap();

    assert_eq!(first.count(), 2);
    assert_eq!(second.count(), 1);
}

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    let mut future = Box::pin(future);
    let waker = std::task::Waker::noop();
    let mut context = std::task::Context::from_waker(waker);
    loop {
        match future.as_mut().poll(&mut context) {
            std::task::Poll::Ready(output) => return output,
            std::task::Poll::Pending => std::thread::yield_now(),
        }
    }
}
