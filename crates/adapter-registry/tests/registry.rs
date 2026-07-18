use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Wake, Waker};

use adapter_memory::MemoryAdapter;
use adapter_registry::{
    AdapterFactory, AdapterFactoryFuture, AdapterOpenRequest, AdapterRegistry,
    AdapterRestoreFuture, AdapterRestoreSession, AdapterRestoreSessionFuture, HotSwapAdapter,
    MigrationError, MigrationStatus, RegistryError, SecretString,
};
use storage_api::{
    AdapterCapabilities, AdapterDescriptorV1, AdapterError, AdapterFuture, AdapterRequirement,
    ApplyReceipt, BackendFamily, CommittedMutationBatch, KeySpan, KeyValue, Keyspace, LogicalKey,
    LogicalSnapshotAccumulator, LogicalSnapshotChunkV1, LogicalSnapshotHeaderV1,
    LogicalSnapshotManifestV1, LogicalSnapshotReader, Mutation, StorageAdapter,
};

struct MemoryFactory;

impl AdapterFactory for MemoryFactory {
    fn provider_name(&self) -> &str {
        "memory"
    }

    fn open<'a>(&'a self, _request: &'a AdapterOpenRequest) -> AdapterFactoryFuture<'a> {
        Box::pin(async {
            let adapter: Arc<dyn StorageAdapter> = Arc::new(MemoryAdapter::new());
            Ok(adapter)
        })
    }
}

#[test]
fn registry_selects_a_provider_and_validates_the_opened_instance() {
    let mut registry = AdapterRegistry::new();
    registry.register(Arc::new(MemoryFactory)).unwrap();
    let request = AdapterOpenRequest::new("shard-7");

    let opened =
        block_on(registry.open("memory", &request, AdapterRequirement::Development)).unwrap();
    assert_eq!(opened.provider_name(), "memory");
    assert_eq!(opened.instance_id(), "shard-7");
    assert_eq!(opened.descriptor(), &opened.adapter().descriptor());
}

#[test]
fn production_open_rejects_a_development_only_adapter() {
    let mut registry = AdapterRegistry::new();
    registry.register(Arc::new(MemoryFactory)).unwrap();

    assert!(matches!(
        block_on(registry.open(
            "memory",
            &AdapterOpenRequest::new("production-shard"),
            AdapterRequirement::ManagedReplica,
        )),
        Err(RegistryError::Incompatible(_))
    ));
}

#[test]
fn restore_validates_capabilities_before_publishing_the_target() {
    let finished = Arc::new(AtomicBool::new(false));
    let mut registry = AdapterRegistry::new();
    registry
        .register(Arc::new(DevelopmentRestoreFactory {
            finished: Arc::clone(&finished),
        }))
        .unwrap();
    let header = LogicalSnapshotHeaderV1::new(17, 0);
    let reader: Box<dyn LogicalSnapshotReader> = Box::new(EmptySnapshotReader {
        accumulator: LogicalSnapshotAccumulator::new(header.clone()),
        header,
    });

    assert!(matches!(
        block_on(registry.restore(
            "development-restore",
            &AdapterOpenRequest::new("target"),
            AdapterRequirement::HotPluggableReplica,
            reader,
        )),
        Err(RegistryError::Incompatible(_))
    ));
    assert!(!finished.load(Ordering::Acquire));
}

#[test]
fn duplicate_invalid_and_unknown_providers_fail_closed() {
    let mut registry = AdapterRegistry::new();
    registry.register(Arc::new(MemoryFactory)).unwrap();
    assert!(matches!(
        registry.register(Arc::new(MemoryFactory)),
        Err(RegistryError::DuplicateProvider { provider }) if provider == "memory"
    ));
    assert!(matches!(
        block_on(registry.open(
            "missing",
            &AdapterOpenRequest::new("shard"),
            AdapterRequirement::Development,
        )),
        Err(RegistryError::UnknownProvider { provider }) if provider == "missing"
    ));
    assert!(matches!(
        AdapterRegistry::validate_provider_name("../plugin"),
        Err(RegistryError::InvalidProviderName { .. })
    ));
}

#[test]
fn secrets_are_never_exposed_by_debug_output() {
    let secret = SecretString::new("super-secret-password");
    let request = AdapterOpenRequest::new("postgres-shard")
        .with_parameter("endpoint", "postgresql://database:5432/dtg")
        .with_secret("password", secret);

    let output = format!("{request:?}");
    assert!(output.contains("[REDACTED]"));
    assert!(!output.contains("super-secret-password"));
    assert_eq!(
        request.secret("password").unwrap().expose(),
        "super-secret-password"
    );
    assert_eq!(
        request
            .public_parameters()
            .get("endpoint")
            .map(String::as_str),
        Some("postgresql://database:5432/dtg")
    );
    assert!(!request.public_parameters().contains_key("password"));
}

#[test]
fn restore_rejects_final_descriptor_drift() {
    let error = match restore_with_final_adapter("prospective", "different", 7, 7) {
        Ok(_) => panic!("descriptor drift was accepted"),
        Err(error) => error,
    };
    assert!(matches!(error, RegistryError::FinalDescriptorMismatch));
}

#[test]
fn restore_rejects_final_applied_index_drift() {
    let error = match restore_with_final_adapter("stable", "stable", 7, 6) {
        Ok(_) => panic!("applied-index drift was accepted"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        RegistryError::RestoredIndexMismatch {
            expected: 7,
            actual: 6
        }
    ));
}

#[test]
fn hot_swap_dual_applies_then_cuts_over_without_an_index_gap() {
    let mut registry = AdapterRegistry::new();
    registry.register(Arc::new(MemoryFactory)).unwrap();
    let source = block_on(registry.open(
        "memory",
        &AdapterOpenRequest::new("source"),
        AdapterRequirement::Development,
    ))
    .unwrap();
    let target = block_on(registry.open(
        "memory",
        &AdapterOpenRequest::new("target"),
        AdapterRequirement::Development,
    ))
    .unwrap();

    block_on(source.adapter().apply_committed(batch(1, b"one"))).unwrap();
    block_on(target.adapter().apply_committed(batch(1, b"one"))).unwrap();
    let hot = HotSwapAdapter::new(source);
    hot.start_migration(target, AdapterRequirement::Development)
        .unwrap();
    assert_eq!(
        hot.migration_status(),
        MigrationStatus::DualApplying {
            source_generation: 1,
            target_generation: 2,
            synchronized_index: 1,
        }
    );

    block_on(hot.apply_committed(batch(2, b"two"))).unwrap();
    let retired = hot.cutover().unwrap();
    assert_eq!(hot.generation(), 2);
    assert_eq!(hot.applied_log_index().unwrap(), 2);
    assert_eq!(retired.adapter().applied_log_index().unwrap(), 2);

    block_on(hot.apply_committed(batch(3, b"three"))).unwrap();
    assert_eq!(hot.applied_log_index().unwrap(), 3);
    assert_eq!(retired.adapter().applied_log_index().unwrap(), 2);
}

#[test]
fn hot_swap_refuses_an_unsynchronized_target() {
    let mut registry = AdapterRegistry::new();
    registry.register(Arc::new(MemoryFactory)).unwrap();
    let source = block_on(registry.open(
        "memory",
        &AdapterOpenRequest::new("source"),
        AdapterRequirement::Development,
    ))
    .unwrap();
    let target = block_on(registry.open(
        "memory",
        &AdapterOpenRequest::new("target"),
        AdapterRequirement::Development,
    ))
    .unwrap();
    block_on(source.adapter().apply_committed(batch(1, b"one"))).unwrap();

    let hot = HotSwapAdapter::new(source);
    assert!(matches!(
        hot.start_migration(target, AdapterRequirement::Development),
        Err(MigrationError::TargetIndexMismatch {
            source: 1,
            target: 0
        })
    ));
}

struct DevelopmentRestoreFactory {
    finished: Arc<AtomicBool>,
}

impl AdapterFactory for DevelopmentRestoreFactory {
    fn provider_name(&self) -> &str {
        "development-restore"
    }

    fn open<'a>(&'a self, _request: &'a AdapterOpenRequest) -> AdapterFactoryFuture<'a> {
        Box::pin(async { Ok(Arc::new(MemoryAdapter::new()) as Arc<dyn StorageAdapter>) })
    }

    fn begin_restore<'a>(
        &'a self,
        _request: &'a AdapterOpenRequest,
        _header: LogicalSnapshotHeaderV1,
    ) -> AdapterRestoreSessionFuture<'a> {
        let finished = Arc::clone(&self.finished);
        Box::pin(async move {
            Ok(Box::new(DevelopmentRestoreSession { finished })
                as Box<dyn AdapterRestoreSession + 'a>)
        })
    }
}

struct DevelopmentRestoreSession {
    finished: Arc<AtomicBool>,
}

impl AdapterRestoreSession for DevelopmentRestoreSession {
    fn descriptor(&self) -> AdapterDescriptorV1 {
        MemoryAdapter::new().descriptor()
    }

    fn write_chunk<'a>(
        &'a mut self,
        _chunk: LogicalSnapshotChunkV1,
    ) -> AdapterRestoreFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }

    fn finish<'a>(
        self: Box<Self>,
        _manifest: LogicalSnapshotManifestV1,
    ) -> AdapterRestoreFuture<'a, Arc<dyn StorageAdapter>>
    where
        Self: 'a,
    {
        Box::pin(async move {
            self.finished.store(true, Ordering::Release);
            Ok(Arc::new(MemoryAdapter::new()) as Arc<dyn StorageAdapter>)
        })
    }

    fn abort<'a>(self: Box<Self>) -> AdapterRestoreFuture<'a, ()>
    where
        Self: 'a,
    {
        Box::pin(async move { Ok(()) })
    }
}

struct FinalRestoreFactory {
    prospective: AdapterDescriptorV1,
    final_adapter: Arc<dyn StorageAdapter>,
}

impl AdapterFactory for FinalRestoreFactory {
    fn provider_name(&self) -> &str {
        "final-restore"
    }

    fn open<'a>(&'a self, _request: &'a AdapterOpenRequest) -> AdapterFactoryFuture<'a> {
        let adapter = Arc::clone(&self.final_adapter);
        Box::pin(async move { Ok(adapter) })
    }

    fn begin_restore<'a>(
        &'a self,
        _request: &'a AdapterOpenRequest,
        _header: LogicalSnapshotHeaderV1,
    ) -> AdapterRestoreSessionFuture<'a> {
        let prospective = self.prospective.clone();
        let final_adapter = Arc::clone(&self.final_adapter);
        Box::pin(async move {
            Ok(Box::new(FinalRestoreSession {
                prospective,
                final_adapter,
            }) as Box<dyn AdapterRestoreSession + 'a>)
        })
    }
}

struct FinalRestoreSession {
    prospective: AdapterDescriptorV1,
    final_adapter: Arc<dyn StorageAdapter>,
}

impl AdapterRestoreSession for FinalRestoreSession {
    fn descriptor(&self) -> AdapterDescriptorV1 {
        self.prospective.clone()
    }

    fn write_chunk<'a>(
        &'a mut self,
        _chunk: LogicalSnapshotChunkV1,
    ) -> AdapterRestoreFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }

    fn finish<'a>(
        self: Box<Self>,
        _manifest: LogicalSnapshotManifestV1,
    ) -> AdapterRestoreFuture<'a, Arc<dyn StorageAdapter>>
    where
        Self: 'a,
    {
        Box::pin(async move { Ok(Arc::clone(&self.final_adapter)) })
    }

    fn abort<'a>(self: Box<Self>) -> AdapterRestoreFuture<'a, ()>
    where
        Self: 'a,
    {
        Box::pin(async move { Ok(()) })
    }
}

struct FixedAdapter {
    descriptor: AdapterDescriptorV1,
    applied_index: u64,
}

impl StorageAdapter for FixedAdapter {
    fn descriptor(&self) -> AdapterDescriptorV1 {
        self.descriptor.clone()
    }

    fn capabilities(&self) -> AdapterCapabilities {
        self.descriptor.capabilities()
    }

    fn apply_committed<'a>(
        &'a self,
        batch: CommittedMutationBatch,
    ) -> AdapterFuture<'a, ApplyReceipt> {
        Box::pin(async move {
            Ok(ApplyReceipt {
                applied_log_index: batch.log_index,
                duplicate: false,
            })
        })
    }

    fn multi_get<'a>(&'a self, keys: &'a [LogicalKey]) -> AdapterFuture<'a, Vec<Option<Vec<u8>>>> {
        Box::pin(async move { Ok(vec![None; keys.len()]) })
    }

    fn scan<'a>(&'a self, _span: &'a KeySpan) -> AdapterFuture<'a, Vec<KeyValue>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn applied_log_index(&self) -> Result<u64, AdapterError> {
        Ok(self.applied_index)
    }
}

fn restore_with_final_adapter(
    prospective_version: &str,
    final_version: &str,
    snapshot_index: u64,
    final_index: u64,
) -> Result<adapter_registry::OpenedAdapter, RegistryError> {
    let capabilities = MemoryAdapter::new().capabilities();
    let prospective = AdapterDescriptorV1::new(
        "fixed",
        prospective_version,
        BackendFamily::Test,
        capabilities,
    );
    let final_adapter: Arc<dyn StorageAdapter> = Arc::new(FixedAdapter {
        descriptor: AdapterDescriptorV1::new(
            "fixed",
            final_version,
            BackendFamily::Test,
            capabilities,
        ),
        applied_index: final_index,
    });
    let mut registry = AdapterRegistry::new();
    registry
        .register(Arc::new(FinalRestoreFactory {
            prospective,
            final_adapter,
        }))
        .unwrap();
    let header = LogicalSnapshotHeaderV1::new(91, snapshot_index);
    let reader: Box<dyn LogicalSnapshotReader> = Box::new(EmptySnapshotReader {
        accumulator: LogicalSnapshotAccumulator::new(header.clone()),
        header,
    });
    block_on(registry.restore(
        "final-restore",
        &AdapterOpenRequest::new("target"),
        AdapterRequirement::Development,
        reader,
    ))
}

struct EmptySnapshotReader {
    header: LogicalSnapshotHeaderV1,
    accumulator: LogicalSnapshotAccumulator,
}

impl LogicalSnapshotReader for EmptySnapshotReader {
    fn header(&self) -> &LogicalSnapshotHeaderV1 {
        &self.header
    }

    fn next_chunk<'a>(&'a mut self) -> AdapterFuture<'a, Option<LogicalSnapshotChunkV1>> {
        Box::pin(async { Ok(None) })
    }

    fn finish<'a>(self: Box<Self>) -> AdapterFuture<'a, LogicalSnapshotManifestV1>
    where
        Self: 'a,
    {
        Box::pin(async move { Ok(self.accumulator.complete()) })
    }
}

fn batch(log_index: u64, value: &[u8]) -> CommittedMutationBatch {
    CommittedMutationBatch {
        shard_id: 7,
        log_index,
        txn_id: u128::from(log_index),
        mutations: vec![Mutation::put(
            0,
            LogicalKey::in_keyspace(Keyspace::Current, b"value".to_vec()),
            value.to_vec(),
        )],
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
