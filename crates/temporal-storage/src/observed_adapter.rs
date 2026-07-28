use std::path::Path;
use std::sync::Arc;

use storage_api::{
    AdapterCapabilities, AdapterDescriptorV1, AdapterError, AdapterFuture, AdjacencyExpandPage,
    AdjacencyExpandRequest, ApplyReceipt, CandidateScanPage, CandidateScanRequest,
    CanonicalBatchScanPage, CanonicalBatchScanRequest, CanonicalScanPage, CanonicalScanRequest,
    ChangeScanPage, ChangeScanRequest, CommittedMutationBatch, FencedScan, KeySpan, KeyValue,
    LogicalKey, MappingDescriptorV1, PropertyGatherPage, PropertyGatherRequest,
    QueryCapabilitySnapshot, QueryPrimitiveCapabilities, ReadSnapshot, ReadSnapshotBinding,
    StorageAdapter,
};

pub trait AdapterCallObserver: Send + Sync {
    fn record_adapter_call(&self);

    fn record_canonical_scan(&self) {
        self.record_adapter_call();
    }

    fn record_canonical_batch_scan(&self) {
        self.record_adapter_call();
    }
}

impl<F> AdapterCallObserver for F
where
    F: Fn() + Send + Sync,
{
    fn record_adapter_call(&self) {
        self();
    }
}

pub struct ObservedStorageAdapter<A> {
    inner: A,
    observer: Arc<dyn AdapterCallObserver>,
}

impl<A> ObservedStorageAdapter<A> {
    #[must_use]
    pub fn new<O>(inner: A, observer: Arc<O>) -> Self
    where
        O: AdapterCallObserver + 'static,
    {
        Self { inner, observer }
    }

    #[must_use]
    pub fn from_observer(inner: A, observer: Arc<dyn AdapterCallObserver>) -> Self {
        Self { inner, observer }
    }

    #[must_use]
    pub const fn inner(&self) -> &A {
        &self.inner
    }

    fn observe(&self) {
        self.observer.record_adapter_call();
    }
}

struct ObservedReadSnapshot<'a> {
    inner: Box<dyn ReadSnapshot + 'a>,
    observer: Arc<dyn AdapterCallObserver>,
}

#[must_use]
pub fn observe_read_snapshot<'a>(
    inner: Box<dyn ReadSnapshot + 'a>,
    observer: Arc<dyn AdapterCallObserver>,
) -> Box<dyn ReadSnapshot + 'a> {
    Box::new(ObservedReadSnapshot { inner, observer })
}

impl ReadSnapshot for ObservedReadSnapshot<'_> {
    fn applied_log_index(&self) -> u64 {
        self.inner.applied_log_index()
    }

    fn multi_get<'a>(&'a self, keys: &'a [LogicalKey]) -> AdapterFuture<'a, Vec<Option<Vec<u8>>>> {
        self.observer.record_adapter_call();
        self.inner.multi_get(keys)
    }

    fn scan<'a>(&'a self, span: &'a KeySpan) -> AdapterFuture<'a, Vec<KeyValue>> {
        self.observer.record_adapter_call();
        self.inner.scan(span)
    }

    fn scan_canonical<'a>(
        &'a self,
        request: &'a CanonicalScanRequest,
    ) -> AdapterFuture<'a, CanonicalScanPage> {
        self.observer.record_canonical_scan();
        self.inner.scan_canonical(request)
    }

    fn scan_canonical_batch<'a>(
        &'a self,
        request: &'a CanonicalBatchScanRequest,
    ) -> AdapterFuture<'a, CanonicalBatchScanPage> {
        self.observer.record_canonical_batch_scan();
        self.inner.scan_canonical_batch(request)
    }

    fn scan_candidates<'a>(
        &'a self,
        request: &'a CandidateScanRequest,
    ) -> AdapterFuture<'a, CandidateScanPage> {
        self.observer.record_adapter_call();
        self.inner.scan_candidates(request)
    }

    fn scan_changes<'a>(
        &'a self,
        request: &'a ChangeScanRequest,
    ) -> AdapterFuture<'a, ChangeScanPage> {
        self.observer.record_adapter_call();
        self.inner.scan_changes(request)
    }
}

impl<A> StorageAdapter for ObservedStorageAdapter<A>
where
    A: StorageAdapter,
{
    fn descriptor(&self) -> AdapterDescriptorV1 {
        self.inner.descriptor()
    }

    fn capabilities(&self) -> AdapterCapabilities {
        self.inner.capabilities()
    }

    fn query_primitive_capabilities(&self) -> QueryPrimitiveCapabilities {
        self.inner.query_primitive_capabilities()
    }

    fn query_capability_generation(&self) -> u64 {
        self.inner.query_capability_generation()
    }

    fn query_capability_snapshot(&self) -> QueryCapabilitySnapshot {
        self.inner.query_capability_snapshot()
    }

    fn read_snapshot_binding(&self) -> Result<Option<ReadSnapshotBinding>, AdapterError> {
        self.inner
            .read_snapshot_binding()?
            .map(|binding| {
                let generation = binding.capability_generation();
                let owner: Arc<dyn StorageAdapter> = Arc::new(ObservedStorageAdapter::<
                    Arc<dyn StorageAdapter>,
                >::from_observer(
                    binding.owner_arc(),
                    Arc::clone(&self.observer),
                ));
                ReadSnapshotBinding::new(generation, owner)
            })
            .transpose()
    }

    fn mapping_descriptor(&self) -> Option<MappingDescriptorV1> {
        self.inner.mapping_descriptor()
    }

    fn apply_committed<'a>(
        &'a self,
        batch: CommittedMutationBatch,
    ) -> AdapterFuture<'a, ApplyReceipt> {
        self.inner.apply_committed(batch)
    }

    fn multi_get<'a>(&'a self, keys: &'a [LogicalKey]) -> AdapterFuture<'a, Vec<Option<Vec<u8>>>> {
        self.observe();
        self.inner.multi_get(keys)
    }

    fn scan<'a>(&'a self, span: &'a KeySpan) -> AdapterFuture<'a, Vec<KeyValue>> {
        self.observe();
        self.inner.scan(span)
    }

    fn scan_canonical<'a>(
        &'a self,
        request: &'a CanonicalScanRequest,
    ) -> AdapterFuture<'a, CanonicalScanPage> {
        self.observer.record_canonical_scan();
        self.inner.scan_canonical(request)
    }

    fn scan_candidates<'a>(
        &'a self,
        request: &'a CandidateScanRequest,
    ) -> AdapterFuture<'a, CandidateScanPage> {
        self.observe();
        self.inner.scan_candidates(request)
    }

    fn gather_properties<'a>(
        &'a self,
        request: &'a PropertyGatherRequest,
    ) -> AdapterFuture<'a, PropertyGatherPage> {
        self.observe();
        self.inner.gather_properties(request)
    }

    fn expand_adjacency<'a>(
        &'a self,
        request: &'a AdjacencyExpandRequest,
    ) -> AdapterFuture<'a, AdjacencyExpandPage> {
        self.observe();
        self.inner.expand_adjacency(request)
    }

    fn scan_changes<'a>(
        &'a self,
        request: &'a ChangeScanRequest,
    ) -> AdapterFuture<'a, ChangeScanPage> {
        self.observe();
        self.inner.scan_changes(request)
    }

    fn scan_fenced<'a>(&'a self, span: &'a KeySpan) -> AdapterFuture<'a, FencedScan> {
        self.observe();
        self.inner.scan_fenced(span)
    }

    fn begin_read_snapshot<'a>(&'a self) -> AdapterFuture<'a, Box<dyn ReadSnapshot + 'a>> {
        self.observe();
        Box::pin(async move {
            let inner = self.inner.begin_read_snapshot().await?;
            Ok(observe_read_snapshot(inner, Arc::clone(&self.observer)))
        })
    }

    fn create_physical_checkpoint(&self, destination: &Path) -> Result<(), AdapterError> {
        self.inner.create_physical_checkpoint(destination)
    }

    fn applied_log_index(&self) -> Result<u64, AdapterError> {
        self.inner.applied_log_index()
    }
}
