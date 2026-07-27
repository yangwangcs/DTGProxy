use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use adapter_memory::MemoryAdapter;
use storage_api::{
    CanonicalBatchScanRequest, CanonicalScanRequest, CommittedMutationBatch, KeySpan, Keyspace,
    LogicalKey, Mutation, QueryPageBounds, QueryPrimitiveError, StorageAdapter,
};

#[test]
fn canonical_batch_scan_preserves_order_bounds_snapshot_and_continuations() {
    let adapter = MemoryAdapter::new();
    block_on(adapter.apply_committed(batch(
        1,
        71,
        vec![
            put(0, b"a/1", b"one"),
            put(1, b"a/2", b"two"),
            put(2, b"b/1", b"one"),
            put(3, b"b/2", b"two"),
        ],
    )))
    .unwrap();
    let snapshot = block_on(adapter.begin_read_snapshot()).unwrap();
    block_on(adapter.apply_committed(batch(2, 72, vec![put(0, b"a/3", b"new")]))).unwrap();

    let second = scan(b"b/", 1, 8);
    let first = scan(b"a/", 1, 8);
    let duplicate = CanonicalScanRequest::new(first.span().clone(), bounds(2, 8)).unwrap();
    assert_eq!(
        CanonicalBatchScanRequest::new(vec![first.clone(), duplicate], 16),
        Err(QueryPrimitiveError::DuplicateCanonicalRange)
    );

    let request = CanonicalBatchScanRequest::new(vec![second.clone(), first.clone()], 16).unwrap();
    let page = block_on(snapshot.scan_canonical_batch(&request)).unwrap();

    assert_eq!(page.applied_log_index(), 1);
    assert_eq!(page.pages().len(), 2);
    assert_eq!(page.pages()[0].applied_log_index(), 1);
    assert_eq!(page.pages()[1].applied_log_index(), 1);
    assert_eq!(page.pages()[0].entries()[0].key().as_bytes(), b"b/1");
    assert_eq!(page.pages()[1].entries()[0].key().as_bytes(), b"a/1");
    assert_eq!(
        page.pages()[0].next_start().map(LogicalKey::as_bytes),
        Some(b"b/2".as_slice())
    );
    assert_eq!(
        page.pages()[1].next_start().map(LogicalKey::as_bytes),
        Some(b"a/2".as_slice())
    );

    let retained = page
        .pages()
        .iter()
        .flat_map(|page| page.entries())
        .map(|entry| entry.key().as_bytes().len() + entry.value().len())
        .sum::<usize>();
    assert!(retained <= request.max_total_bytes() as usize);
    for (scan, page) in request.scans().iter().zip(page.pages()) {
        assert!(page.entries().len() <= scan.bounds().max_items());
        let retained = page
            .entries()
            .iter()
            .map(|entry| entry.key().as_bytes().len() + entry.value().len())
            .sum::<usize>();
        assert!(retained <= scan.bounds().max_bytes() as usize);
    }
}

fn scan(prefix: &[u8], max_items: usize, max_bytes: u64) -> CanonicalScanRequest {
    CanonicalScanRequest::new(
        KeySpan::prefix(Keyspace::Current, prefix.to_vec()),
        bounds(max_items, max_bytes),
    )
    .unwrap()
}

fn bounds(max_items: usize, max_bytes: u64) -> QueryPageBounds {
    QueryPageBounds::new(max_items, max_bytes).unwrap()
}

fn put(sequence: u32, key: &[u8], value: &[u8]) -> Mutation {
    Mutation::put(sequence, LogicalKey::new(key.to_vec()), value.to_vec())
}

fn batch(log_index: u64, txn_id: u128, mutations: Vec<Mutation>) -> CommittedMutationBatch {
    CommittedMutationBatch {
        shard_id: 3,
        log_index,
        txn_id,
        mutations,
    }
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
