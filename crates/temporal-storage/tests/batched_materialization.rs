use std::sync::{Arc, Mutex};

use adapter_memory::MemoryAdapter;
use storage_api::{
    AdapterCapabilities, AdapterFuture, ApplyReceipt, BackendFamily, CommittedMutationBatch,
    Durability, KeySpan, LogicalKey, SnapshotCapability, StorageAdapter,
};
use temporal_storage::{
    CommitContext, ElementId, ElementRef, GraphId, LabelId, PartitionId, TemporalStore,
    VertexMutation,
};
use temporal_types::{CanonicalElement, Interval, TransactionTime, ValidTime};

#[derive(Clone)]
struct CountingAdapter {
    inner: Arc<MemoryAdapter>,
    multi_get_sizes: Arc<Mutex<Vec<usize>>>,
}

impl CountingAdapter {
    fn new() -> Self {
        Self {
            inner: Arc::new(MemoryAdapter::new()),
            multi_get_sizes: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn multi_get_call_sizes(&self) -> Vec<usize> {
        self.multi_get_sizes.lock().unwrap().clone()
    }

    fn clear_multi_get_call_sizes(&self) {
        self.multi_get_sizes.lock().unwrap().clear();
    }
}

impl StorageAdapter for CountingAdapter {
    fn capabilities(&self) -> AdapterCapabilities {
        AdapterCapabilities {
            local_atomic_batch: true,
            idempotent_apply: true,
            consistent_multi_get: true,
            ordered_scan: true,
            durable_applied_index: false,
            durability: Durability::Volatile,
            snapshot: SnapshotCapability::None,
            logical_export: false,
            logical_restore: false,
            predicate_pushdown: false,
            adjacency_pushdown: false,
            change_feed: false,
        }
    }

    fn apply_committed<'a>(
        &'a self,
        batch: CommittedMutationBatch,
    ) -> AdapterFuture<'a, ApplyReceipt> {
        self.inner.apply_committed(batch)
    }

    fn multi_get<'a>(&'a self, keys: &'a [LogicalKey]) -> AdapterFuture<'a, Vec<Option<Vec<u8>>>> {
        self.multi_get_sizes.lock().unwrap().push(keys.len());
        self.inner.multi_get(keys)
    }

    fn scan<'a>(&'a self, span: &'a KeySpan) -> AdapterFuture<'a, Vec<storage_api::KeyValue>> {
        self.inner.scan(span)
    }

    fn applied_log_index(&self) -> Result<u64, storage_api::AdapterError> {
        self.inner.applied_log_index()
    }

    fn descriptor(&self) -> storage_api::AdapterDescriptorV1 {
        storage_api::AdapterDescriptorV1::new(
            "counting-memory",
            env!("CARGO_PKG_VERSION"),
            BackendFamily::Test,
            self.capabilities(),
        )
    }
}

#[test]
fn historical_materialization_uses_bounded_multiget_pages() {
    let adapter = CountingAdapter::new();
    let store = TemporalStore::new(adapter.clone());
    for id in 1..=9 {
        let element = ElementRef::vertex(GraphId::new(1), PartitionId::new(0), ElementId::new(id));
        block_on(
            store.commit_vertex(
                CommitContext::new(0, id as u64, id, tx(0), tx(100)),
                VertexMutation::put(
                    element,
                    LabelId::new(1),
                    Interval::new(valid(0), None).unwrap(),
                    CanonicalElement::new(1, Default::default()),
                )
                .unwrap(),
            ),
        )
        .unwrap();
    }
    adapter.clear_multi_get_call_sizes();

    let rows =
        block_on(store.scan_vertex_views_current_batched(GraphId::new(1), valid(1), 3, 4096))
            .unwrap();

    assert_eq!(rows.len(), 9);
    assert_eq!(adapter.multi_get_call_sizes(), vec![3, 3, 3]);
}

fn tx(value: i64) -> TransactionTime {
    TransactionTime::new(value, 0)
}

fn valid(value: i64) -> ValidTime {
    ValidTime::from_micros(value)
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
