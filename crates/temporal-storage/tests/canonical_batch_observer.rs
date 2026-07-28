use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use adapter_memory::MemoryAdapter;
use storage_api::{
    CanonicalBatchScanRequest, CanonicalScanRequest, KeySpan, Keyspace, QueryPageBounds,
    StorageAdapter,
};
use temporal_storage::{AdapterCallObserver, ObservedStorageAdapter};

#[derive(Default)]
struct BatchCounter {
    canonical_scans: AtomicU64,
    canonical_batch_scans: AtomicU64,
}

impl AdapterCallObserver for BatchCounter {
    fn record_adapter_call(&self) {}

    fn record_canonical_scan(&self) {
        self.canonical_scans.fetch_add(1, Ordering::Relaxed);
    }

    fn record_canonical_batch_scan(&self) {
        self.canonical_batch_scans.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn nine_ranges_are_observed_as_one_batch_and_zero_single_scans() {
    let counter = Arc::new(BatchCounter::default());
    let adapter = ObservedStorageAdapter::new(MemoryAdapter::new(), Arc::clone(&counter));
    let snapshot = block_on(adapter.begin_read_snapshot()).unwrap();
    let scans = (0_u8..9)
        .map(|ordinal| {
            CanonicalScanRequest::new(
                KeySpan::range(
                    Keyspace::History,
                    vec![ordinal],
                    Some(vec![ordinal.saturating_add(1)]),
                )
                .unwrap(),
                QueryPageBounds::new(1, 16).unwrap(),
            )
            .unwrap()
        })
        .collect();
    let request = CanonicalBatchScanRequest::new(scans, 144).unwrap();

    block_on(snapshot.scan_canonical_batch(&request)).unwrap();

    assert_eq!(counter.canonical_batch_scans.load(Ordering::Relaxed), 1);
    assert_eq!(counter.canonical_scans.load(Ordering::Relaxed), 0);
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
