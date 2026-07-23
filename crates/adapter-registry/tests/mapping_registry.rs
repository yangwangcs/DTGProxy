use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};

use adapter_registry::{
    AdapterFactory, AdapterFactoryFuture, AdapterOpenRequest, AdapterRegistry, RegistryError,
};
use storage_api::{
    AdapterCapabilities, AdapterDescriptorV1, AdapterError, AdapterFuture, AdapterRequirement,
    ApplyReceipt, BackendFamily, CommittedMutationBatch, Durability, KeySpan, KeyValue, LogicalKey,
    MappingCapabilities, MappingDescriptorV1, SnapshotCapability, StorageAdapter,
};

struct DeclaredMappingFactory {
    declared: Option<MappingDescriptorV1>,
    actual: Option<MappingDescriptorV1>,
    opens: Arc<AtomicUsize>,
}

impl AdapterFactory for DeclaredMappingFactory {
    fn provider_name(&self) -> &str {
        "mapping-test"
    }

    fn mapping_descriptor(&self) -> Option<MappingDescriptorV1> {
        self.declared.clone()
    }

    fn open<'a>(&'a self, _request: &'a AdapterOpenRequest) -> AdapterFactoryFuture<'a> {
        Box::pin(async move {
            self.opens.fetch_add(1, Ordering::SeqCst);
            Ok(Arc::new(FixedAdapter {
                mapping: self.actual.clone(),
            }) as Arc<dyn StorageAdapter>)
        })
    }
}

struct FixedAdapter {
    mapping: Option<MappingDescriptorV1>,
}

impl StorageAdapter for FixedAdapter {
    fn descriptor(&self) -> AdapterDescriptorV1 {
        AdapterDescriptorV1::new(
            "mapping-test",
            "1.0.0",
            BackendFamily::KeyValue,
            adapter_capabilities(),
        )
    }

    fn mapping_descriptor(&self) -> Option<MappingDescriptorV1> {
        self.mapping.clone()
    }

    fn capabilities(&self) -> AdapterCapabilities {
        adapter_capabilities()
    }

    fn apply_committed<'a>(
        &'a self,
        _batch: CommittedMutationBatch,
    ) -> AdapterFuture<'a, ApplyReceipt> {
        Box::pin(async { Err(AdapterError::Backend("unused".into())) })
    }

    fn multi_get<'a>(&'a self, _keys: &'a [LogicalKey]) -> AdapterFuture<'a, Vec<Option<Vec<u8>>>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn scan<'a>(&'a self, _span: &'a KeySpan) -> AdapterFuture<'a, Vec<KeyValue>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn applied_log_index(&self) -> Result<u64, AdapterError> {
        Ok(0)
    }
}

fn mapping(name: &str, fingerprint: u8) -> MappingDescriptorV1 {
    MappingDescriptorV1::new(
        name,
        "1.0.0",
        BackendFamily::KeyValue,
        [fingerprint; 32],
        MappingCapabilities {
            atomic_batch_lifecycle: true,
            deterministic_mapping: true,
            idempotent_replay: true,
            canonical_multi_get: true,
            canonical_ordered_scan: true,
            durable_applied_index: true,
            durability: Durability::Synchronous,
            snapshot: SnapshotCapability::PhysicalCheckpoint,
            canonical_export: true,
            canonical_restore: true,
            native_temporal_layout: true,
            predicate_pushdown: false,
            adjacency_pushdown: false,
            change_feed: false,
        },
    )
    .unwrap()
}

fn adapter_capabilities() -> AdapterCapabilities {
    AdapterCapabilities {
        local_atomic_batch: true,
        idempotent_apply: true,
        consistent_multi_get: true,
        ordered_scan: true,
        durable_applied_index: true,
        durability: Durability::Synchronous,
        snapshot: SnapshotCapability::PhysicalCheckpoint,
        logical_export: true,
        logical_restore: true,
        predicate_pushdown: false,
        adjacency_pushdown: false,
        change_feed: false,
    }
}

#[test]
fn registry_accepts_only_an_exact_declared_and_opened_mapping_descriptor() {
    let descriptor = mapping("rocksdb-canonical", 7);
    let opens = Arc::new(AtomicUsize::new(0));
    let mut registry = AdapterRegistry::new();
    registry
        .register(Arc::new(DeclaredMappingFactory {
            declared: Some(descriptor.clone()),
            actual: Some(descriptor.clone()),
            opens: Arc::clone(&opens),
        }))
        .unwrap();

    let opened = block_on(registry.open(
        "mapping-test",
        &AdapterOpenRequest::new("instance"),
        AdapterRequirement::HotPluggableReplica,
    ))
    .unwrap();

    assert_eq!(opens.load(Ordering::SeqCst), 1);
    assert_eq!(opened.mapping_descriptor(), Some(&descriptor));
}

#[test]
fn registry_rejects_mapping_descriptor_drift_after_open() {
    let opens = Arc::new(AtomicUsize::new(0));
    let mut registry = AdapterRegistry::new();
    registry
        .register(Arc::new(DeclaredMappingFactory {
            declared: Some(mapping("rocksdb-canonical", 7)),
            actual: Some(mapping("rocksdb-canonical", 8)),
            opens: Arc::clone(&opens),
        }))
        .unwrap();

    assert!(matches!(
        block_on(registry.open(
            "mapping-test",
            &AdapterOpenRequest::new("instance"),
            AdapterRequirement::ManagedReplica,
        )),
        Err(RegistryError::MappingDescriptorMismatch)
    ));
    assert_eq!(opens.load(Ordering::SeqCst), 1);
}

#[test]
fn registry_rejects_missing_or_unexpected_opened_mapping_declarations() {
    for (declared, actual, expected) in [
        (
            Some(mapping("rocksdb-canonical", 7)),
            None,
            RegistryError::OpenedMappingMissing,
        ),
        (
            None,
            Some(mapping("rocksdb-canonical", 7)),
            RegistryError::OpenedMappingUnexpected,
        ),
    ] {
        let mut registry = AdapterRegistry::new();
        registry
            .register(Arc::new(DeclaredMappingFactory {
                declared,
                actual,
                opens: Arc::new(AtomicUsize::new(0)),
            }))
            .unwrap();
        let result = block_on(registry.open(
            "mapping-test",
            &AdapterOpenRequest::new("instance"),
            AdapterRequirement::ManagedReplica,
        ));
        assert!(
            matches!(result, Err(error) if error == expected),
            "Registry accepted a mismatched Mapping declaration"
        );
    }
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
