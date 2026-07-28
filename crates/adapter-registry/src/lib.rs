#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use storage_api::{
    AdapterCapabilities, AdapterCompatibilityError, AdapterDescriptorV1, AdapterError,
    AdapterFuture, AdapterRequirement, AdjacencyExpandPage, AdjacencyExpandRequest, ApplyReceipt,
    CandidateScanPage, CandidateScanRequest, ChangeScanPage, ChangeScanRequest,
    CommittedMutationBatch, FencedScan, KeySpan, KeyValue, LogicalKey, LogicalSnapshotChunkV1,
    LogicalSnapshotHeaderV1, LogicalSnapshotManifestV1, LogicalSnapshotReader,
    MappingCompatibilityError, MappingDescriptorV1, MappingRequirement, PropertyGatherPage,
    PropertyGatherRequest, QueryCapabilitySnapshot, QueryPrimitiveCapabilities,
    ReadSnapshotBinding, StorageAdapter,
};

pub type AdapterFactoryFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Arc<dyn StorageAdapter>, AdapterFactoryError>> + Send + 'a>>;

pub type AdapterRestoreSessionFuture<'a> = Pin<
    Box<
        dyn Future<Output = Result<Box<dyn AdapterRestoreSession + 'a>, AdapterFactoryError>>
            + Send
            + 'a,
    >,
>;

pub type AdapterRestoreFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, AdapterFactoryError>> + Send + 'a>>;

pub trait AdapterRestoreSession: Send {
    /// Describes the Adapter that `finish` will publish and return.
    ///
    /// Implementations must keep this descriptor stable for the lifetime of the session so the
    /// Registry can reject an incompatible target before any restore data becomes visible.
    fn descriptor(&self) -> AdapterDescriptorV1;

    fn write_chunk<'a>(&'a mut self, chunk: LogicalSnapshotChunkV1)
    -> AdapterRestoreFuture<'a, ()>;

    fn finish<'a>(
        self: Box<Self>,
        manifest: LogicalSnapshotManifestV1,
    ) -> AdapterRestoreFuture<'a, Arc<dyn StorageAdapter>>
    where
        Self: 'a;

    /// Explicitly aborts a restore and reports whether its hidden target was cleaned up.
    ///
    /// `Drop` remains a crash-safety fallback, but callers that decide whether a target can be
    /// retried need an observable result instead of a best-effort destructor.
    fn abort<'a>(self: Box<Self>) -> AdapterRestoreFuture<'a, ()>
    where
        Self: 'a;
}

pub trait AdapterFactory: Send + Sync {
    fn provider_name(&self) -> &str;

    fn mapping_descriptor(&self) -> Option<MappingDescriptorV1> {
        None
    }

    fn open<'a>(&'a self, request: &'a AdapterOpenRequest) -> AdapterFactoryFuture<'a>;

    fn begin_restore<'a>(
        &'a self,
        _request: &'a AdapterOpenRequest,
        _header: LogicalSnapshotHeaderV1,
    ) -> AdapterRestoreSessionFuture<'a> {
        Box::pin(async {
            Err(AdapterFactoryError::new(
                "Adapter factory does not support logical snapshot restore",
            ))
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdapterFactoryError {
    message: String,
}

impl AdapterFactoryError {
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl Display for AdapterFactoryError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for AdapterFactoryError {}

pub struct SecretString(String);

impl SecretString {
    #[must_use]
    pub fn new(secret: impl Into<String>) -> Self {
        Self(secret.into())
    }

    #[must_use]
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl Debug for SecretString {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("[REDACTED]")
    }
}

pub struct AdapterOpenRequest {
    instance_id: String,
    parameters: BTreeMap<String, String>,
    secrets: BTreeMap<String, SecretString>,
}

impl AdapterOpenRequest {
    #[must_use]
    pub fn new(instance_id: impl Into<String>) -> Self {
        Self {
            instance_id: instance_id.into(),
            parameters: BTreeMap::new(),
            secrets: BTreeMap::new(),
        }
    }

    #[must_use]
    pub fn with_parameter(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.parameters.insert(name.into(), value.into());
        self
    }

    #[must_use]
    pub fn with_secret(mut self, name: impl Into<String>, value: SecretString) -> Self {
        self.secrets.insert(name.into(), value);
        self
    }

    #[must_use]
    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    #[must_use]
    pub fn parameter(&self, name: &str) -> Option<&str> {
        self.parameters.get(name).map(String::as_str)
    }

    #[must_use]
    pub const fn public_parameters(&self) -> &BTreeMap<String, String> {
        &self.parameters
    }

    #[must_use]
    pub fn secret(&self, name: &str) -> Option<&SecretString> {
        self.secrets.get(name)
    }
}

impl Debug for AdapterOpenRequest {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdapterOpenRequest")
            .field("instance_id", &self.instance_id)
            .field("parameters", &self.parameters)
            .field("secrets", &self.secrets)
            .finish()
    }
}

#[derive(Clone)]
pub struct OpenedAdapter {
    provider_name: String,
    instance_id: String,
    descriptor: AdapterDescriptorV1,
    mapping_descriptor: Option<MappingDescriptorV1>,
    adapter: Arc<dyn StorageAdapter>,
}

impl OpenedAdapter {
    #[must_use]
    pub fn provider_name(&self) -> &str {
        &self.provider_name
    }

    #[must_use]
    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    #[must_use]
    pub const fn descriptor(&self) -> &AdapterDescriptorV1 {
        &self.descriptor
    }

    #[must_use]
    pub const fn mapping_descriptor(&self) -> Option<&MappingDescriptorV1> {
        self.mapping_descriptor.as_ref()
    }

    #[must_use]
    pub fn adapter(&self) -> &dyn StorageAdapter {
        self.adapter.as_ref()
    }

    #[must_use]
    pub fn into_adapter(self) -> Arc<dyn StorageAdapter> {
        self.adapter
    }

    fn adapter_arc(&self) -> Arc<dyn StorageAdapter> {
        Arc::clone(&self.adapter)
    }
}

#[derive(Default)]
pub struct AdapterRegistry {
    factories: BTreeMap<String, Arc<dyn AdapterFactory>>,
}

impl AdapterRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn validate_provider_name(provider: &str) -> Result<(), RegistryError> {
        let valid_length = (1..=64).contains(&provider.len());
        let valid_first = provider
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_lowercase);
        let valid_characters = provider.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"._-".contains(&byte)
        });
        if valid_length && valid_first && valid_characters {
            Ok(())
        } else {
            Err(RegistryError::InvalidProviderName {
                provider: provider.to_owned(),
            })
        }
    }

    pub fn register(&mut self, factory: Arc<dyn AdapterFactory>) -> Result<(), RegistryError> {
        let provider = factory.provider_name().to_owned();
        Self::validate_provider_name(&provider)?;
        if self.factories.contains_key(&provider) {
            return Err(RegistryError::DuplicateProvider { provider });
        }
        self.factories.insert(provider, factory);
        Ok(())
    }

    #[must_use]
    pub fn providers(&self) -> Vec<&str> {
        self.factories.keys().map(String::as_str).collect()
    }

    pub async fn open(
        &self,
        provider: &str,
        request: &AdapterOpenRequest,
        requirement: AdapterRequirement,
    ) -> Result<OpenedAdapter, RegistryError> {
        Self::validate_provider_name(provider)?;
        let factory =
            self.factories
                .get(provider)
                .ok_or_else(|| RegistryError::UnknownProvider {
                    provider: provider.to_owned(),
                })?;
        let declared_mapping = factory.mapping_descriptor();
        if let Some(mapping) = &declared_mapping {
            mapping.validate(MappingRequirement::from(requirement))?;
        }
        let adapter = factory.open(request).await?;
        let descriptor = adapter.descriptor();
        descriptor.validate(requirement)?;
        let mapping_descriptor = validate_opened_mapping(
            declared_mapping,
            adapter.mapping_descriptor(),
            descriptor.family(),
        )?;
        Ok(OpenedAdapter {
            provider_name: provider.to_owned(),
            instance_id: request.instance_id.clone(),
            descriptor,
            mapping_descriptor,
            adapter,
        })
    }

    pub async fn restore<'a>(
        &self,
        provider: &str,
        request: &AdapterOpenRequest,
        requirement: AdapterRequirement,
        mut reader: Box<dyn LogicalSnapshotReader + 'a>,
    ) -> Result<OpenedAdapter, RegistryError> {
        Self::validate_provider_name(provider)?;
        let factory =
            self.factories
                .get(provider)
                .ok_or_else(|| RegistryError::UnknownProvider {
                    provider: provider.to_owned(),
                })?;
        let declared_mapping = factory.mapping_descriptor();
        if let Some(mapping) = &declared_mapping {
            mapping.validate(MappingRequirement::from(requirement))?;
        }
        let mut restore = factory
            .begin_restore(request, reader.header().clone())
            .await?;
        let descriptor = restore.descriptor();
        descriptor.validate(requirement)?;
        while let Some(chunk) = reader.next_chunk().await? {
            restore.write_chunk(chunk).await?;
        }
        let manifest = reader.finish().await?;
        let expected_applied_index = manifest.header().applied_log_index();
        let adapter = restore.finish(manifest).await?;
        let final_descriptor = adapter.descriptor();
        if final_descriptor != descriptor {
            return Err(RegistryError::FinalDescriptorMismatch);
        }
        final_descriptor.validate(requirement)?;
        let mapping_descriptor = validate_opened_mapping(
            declared_mapping,
            adapter.mapping_descriptor(),
            final_descriptor.family(),
        )?;
        let actual_applied_index = adapter.applied_log_index().map_err(RegistryError::Target)?;
        if actual_applied_index != expected_applied_index {
            return Err(RegistryError::RestoredIndexMismatch {
                expected: expected_applied_index,
                actual: actual_applied_index,
            });
        }
        Ok(OpenedAdapter {
            provider_name: provider.to_owned(),
            instance_id: request.instance_id.clone(),
            descriptor: final_descriptor,
            mapping_descriptor,
            adapter,
        })
    }
}

fn validate_opened_mapping(
    declared: Option<MappingDescriptorV1>,
    actual: Option<MappingDescriptorV1>,
    adapter_family: storage_api::BackendFamily,
) -> Result<Option<MappingDescriptorV1>, RegistryError> {
    match (declared, actual) {
        (None, None) => Ok(None),
        (Some(_), None) => Err(RegistryError::OpenedMappingMissing),
        (None, Some(_)) => Err(RegistryError::OpenedMappingUnexpected),
        (Some(declared), Some(actual)) if declared != actual => {
            Err(RegistryError::MappingDescriptorMismatch)
        }
        (Some(declared), Some(_)) if declared.family() != adapter_family => {
            Err(RegistryError::MappingFamilyMismatch)
        }
        (Some(declared), Some(_)) => Ok(Some(declared)),
    }
}

#[derive(Debug, PartialEq)]
pub enum RegistryError {
    InvalidProviderName { provider: String },
    DuplicateProvider { provider: String },
    UnknownProvider { provider: String },
    Factory(AdapterFactoryError),
    Source(AdapterError),
    Target(AdapterError),
    Incompatible(AdapterCompatibilityError),
    MappingIncompatible(MappingCompatibilityError),
    OpenedMappingMissing,
    OpenedMappingUnexpected,
    MappingDescriptorMismatch,
    MappingFamilyMismatch,
    FinalDescriptorMismatch,
    RestoredIndexMismatch { expected: u64, actual: u64 },
}

impl Display for RegistryError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidProviderName { provider } => {
                write!(formatter, "invalid Adapter provider name {provider:?}")
            }
            Self::DuplicateProvider { provider } => {
                write!(
                    formatter,
                    "Adapter provider {provider} is already registered"
                )
            }
            Self::UnknownProvider { provider } => {
                write!(formatter, "Adapter provider {provider} is not registered")
            }
            Self::Factory(error) => write!(formatter, "Adapter factory failed: {error}"),
            Self::Source(error) => write!(formatter, "snapshot source failed: {error}"),
            Self::Target(error) => write!(formatter, "restored Adapter validation failed: {error}"),
            Self::Incompatible(error) => Display::fmt(error, formatter),
            Self::MappingIncompatible(error) => Display::fmt(error, formatter),
            Self::OpenedMappingMissing => {
                formatter.write_str("opened Adapter omitted its declared Mapping descriptor")
            }
            Self::OpenedMappingUnexpected => {
                formatter.write_str("opened Adapter exposed an undeclared Mapping descriptor")
            }
            Self::MappingDescriptorMismatch => formatter.write_str(
                "opened Adapter Mapping descriptor differs from the Factory declaration",
            ),
            Self::MappingFamilyMismatch => formatter
                .write_str("opened Adapter family differs from its Mapping descriptor family"),
            Self::FinalDescriptorMismatch => formatter.write_str(
                "restored Adapter descriptor differs from the pre-publication descriptor",
            ),
            Self::RestoredIndexMismatch { expected, actual } => write!(
                formatter,
                "restored Adapter applied index {actual} differs from snapshot index {expected}"
            ),
        }
    }
}

impl Error for RegistryError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Factory(error) => Some(error),
            Self::Source(error) => Some(error),
            Self::Target(error) => Some(error),
            Self::Incompatible(error) => Some(error),
            Self::MappingIncompatible(error) => Some(error),
            _ => None,
        }
    }
}

impl From<AdapterFactoryError> for RegistryError {
    fn from(error: AdapterFactoryError) -> Self {
        Self::Factory(error)
    }
}

impl From<AdapterError> for RegistryError {
    fn from(error: AdapterError) -> Self {
        Self::Source(error)
    }
}

impl From<AdapterCompatibilityError> for RegistryError {
    fn from(error: AdapterCompatibilityError) -> Self {
        Self::Incompatible(error)
    }
}

impl From<MappingCompatibilityError> for RegistryError {
    fn from(error: MappingCompatibilityError) -> Self {
        Self::MappingIncompatible(error)
    }
}

struct ShadowAdapter {
    generation: u64,
    synchronized_index: u64,
    opened: OpenedAdapter,
}

struct HotSwapState {
    generation: u64,
    active: OpenedAdapter,
    shadow: Option<ShadowAdapter>,
}

/// A Storage Adapter slot that supports an online, Raft-index-fenced backend migration.
///
/// The target must first be restored/exported to exactly the source Adapter's applied
/// index. While migration is active every committed batch is acknowledged only after
/// both source and target have applied it. Because Adapter apply is idempotent, a target
/// failure after the source succeeds is safely retried. Cutover is allowed only with no
/// apply in flight and equal durable indices.
pub struct HotSwapAdapter {
    state: Mutex<HotSwapState>,
    in_flight_applies: AtomicUsize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HotSwapRecoveryState {
    Active {
        generation: u64,
        provider_name: String,
        instance_id: String,
    },
    DualApplying {
        source_generation: u64,
        source_provider_name: String,
        source_instance_id: String,
        target_generation: u64,
        target_provider_name: String,
        target_instance_id: String,
        synchronized_index: u64,
    },
}

impl HotSwapAdapter {
    #[must_use]
    pub fn new(active: OpenedAdapter) -> Self {
        Self {
            state: Mutex::new(HotSwapState {
                generation: 1,
                active,
                shadow: None,
            }),
            in_flight_applies: AtomicUsize::new(0),
        }
    }

    pub fn recover_active(active: OpenedAdapter, generation: u64) -> Result<Self, MigrationError> {
        if generation == 0 {
            return Err(MigrationError::InvalidGeneration { generation });
        }
        Ok(Self {
            state: Mutex::new(HotSwapState {
                generation,
                active,
                shadow: None,
            }),
            in_flight_applies: AtomicUsize::new(0),
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn recover_dual_applying(
        active: OpenedAdapter,
        source_generation: u64,
        target: OpenedAdapter,
        target_generation: u64,
        synchronized_index: u64,
        requirement: AdapterRequirement,
    ) -> Result<Self, MigrationError> {
        if source_generation == 0 {
            return Err(MigrationError::InvalidGeneration {
                generation: source_generation,
            });
        }
        let expected_target = source_generation
            .checked_add(1)
            .ok_or(MigrationError::GenerationExhausted)?;
        if target_generation != expected_target {
            return Err(MigrationError::NonConsecutiveGeneration {
                source: source_generation,
                target: target_generation,
            });
        }
        target.descriptor.validate(requirement)?;
        let source_index = active.adapter.applied_log_index()?;
        let target_index = target.adapter.applied_log_index()?;
        if source_index != target_index {
            return Err(MigrationError::TargetIndexMismatch {
                source: source_index,
                target: target_index,
            });
        }
        if synchronized_index != source_index {
            return Err(MigrationError::SynchronizedIndexMismatch {
                durable: source_index,
                recorded: synchronized_index,
            });
        }
        Ok(Self {
            state: Mutex::new(HotSwapState {
                generation: source_generation,
                active,
                shadow: Some(ShadowAdapter {
                    generation: target_generation,
                    synchronized_index,
                    opened: target,
                }),
            }),
            in_flight_applies: AtomicUsize::new(0),
        })
    }

    fn lock_state(&self) -> Result<MutexGuard<'_, HotSwapState>, AdapterError> {
        self.state.lock().map_err(|_| AdapterError::LockPoisoned)
    }

    fn inspect_state(&self) -> MutexGuard<'_, HotSwapState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    #[must_use]
    pub fn generation(&self) -> u64 {
        self.inspect_state().generation
    }

    #[must_use]
    pub fn active_adapter(&self) -> Arc<dyn StorageAdapter> {
        self.inspect_state().active.adapter_arc()
    }

    #[must_use]
    pub fn active_provider_name(&self) -> String {
        self.inspect_state().active.provider_name.clone()
    }

    #[must_use]
    pub fn active_instance_id(&self) -> String {
        self.inspect_state().active.instance_id.clone()
    }

    #[must_use]
    pub fn recovery_state(&self) -> HotSwapRecoveryState {
        let state = self.inspect_state();
        state.shadow.as_ref().map_or_else(
            || HotSwapRecoveryState::Active {
                generation: state.generation,
                provider_name: state.active.provider_name.clone(),
                instance_id: state.active.instance_id.clone(),
            },
            |shadow| HotSwapRecoveryState::DualApplying {
                source_generation: state.generation,
                source_provider_name: state.active.provider_name.clone(),
                source_instance_id: state.active.instance_id.clone(),
                target_generation: shadow.generation,
                target_provider_name: shadow.opened.provider_name.clone(),
                target_instance_id: shadow.opened.instance_id.clone(),
                synchronized_index: shadow.synchronized_index,
            },
        )
    }

    #[must_use]
    pub fn migration_status(&self) -> MigrationStatus {
        let state = self.inspect_state();
        state.shadow.as_ref().map_or(
            MigrationStatus::Idle {
                generation: state.generation,
            },
            |shadow| MigrationStatus::DualApplying {
                source_generation: state.generation,
                target_generation: shadow.generation,
                synchronized_index: shadow.synchronized_index,
            },
        )
    }

    pub fn start_migration(
        &self,
        target: OpenedAdapter,
        requirement: AdapterRequirement,
    ) -> Result<(), MigrationError> {
        target.descriptor.validate(requirement)?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| MigrationError::LockPoisoned)?;
        if state.shadow.is_some() {
            return Err(MigrationError::AlreadyMigrating);
        }
        let in_flight = self.in_flight_applies.load(Ordering::Acquire);
        if in_flight != 0 {
            return Err(MigrationError::ApplyInFlight { count: in_flight });
        }
        let source_index = state.active.adapter.applied_log_index()?;
        let target_index = target.adapter.applied_log_index()?;
        if source_index != target_index {
            return Err(MigrationError::TargetIndexMismatch {
                source: source_index,
                target: target_index,
            });
        }
        let generation = state
            .generation
            .checked_add(1)
            .ok_or(MigrationError::GenerationExhausted)?;
        state.shadow = Some(ShadowAdapter {
            generation,
            synchronized_index: source_index,
            opened: target,
        });
        Ok(())
    }

    pub fn cutover(&self) -> Result<OpenedAdapter, MigrationError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| MigrationError::LockPoisoned)?;
        let in_flight = self.in_flight_applies.load(Ordering::Acquire);
        if in_flight != 0 {
            return Err(MigrationError::ApplyInFlight { count: in_flight });
        }
        let shadow = state.shadow.take().ok_or(MigrationError::NotMigrating)?;
        let source_index = state.active.adapter.applied_log_index()?;
        let target_index = shadow.opened.adapter.applied_log_index()?;
        if source_index != target_index || shadow.synchronized_index != target_index {
            state.shadow = Some(shadow);
            return Err(MigrationError::TargetIndexMismatch {
                source: source_index,
                target: target_index,
            });
        }
        let retired = std::mem::replace(&mut state.active, shadow.opened);
        state.generation = shadow.generation;
        Ok(retired)
    }

    pub fn abort_migration(&self) -> Result<OpenedAdapter, MigrationError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| MigrationError::LockPoisoned)?;
        if self.in_flight_applies.load(Ordering::Acquire) != 0 {
            return Err(MigrationError::ApplyInFlight {
                count: self.in_flight_applies.load(Ordering::Relaxed),
            });
        }
        state
            .shadow
            .take()
            .map(|shadow| shadow.opened)
            .ok_or(MigrationError::NotMigrating)
    }
}

impl StorageAdapter for HotSwapAdapter {
    fn descriptor(&self) -> AdapterDescriptorV1 {
        self.inspect_state().active.descriptor.clone()
    }

    fn capabilities(&self) -> AdapterCapabilities {
        self.inspect_state().active.descriptor.capabilities()
    }

    fn query_primitive_capabilities(&self) -> QueryPrimitiveCapabilities {
        self.inspect_state()
            .active
            .adapter
            .query_primitive_capabilities()
    }

    fn query_capability_generation(&self) -> u64 {
        self.generation()
    }

    fn query_capability_snapshot(&self) -> QueryCapabilitySnapshot {
        let state = self.inspect_state();
        QueryCapabilitySnapshot::new(
            state.generation,
            state.active.adapter.query_primitive_capabilities(),
        )
    }

    fn read_snapshot_binding(&self) -> Result<Option<ReadSnapshotBinding>, AdapterError> {
        let state = self.lock_state()?;
        Ok(Some(ReadSnapshotBinding::new(
            state.generation,
            state.active.adapter_arc(),
        )?))
    }

    fn mapping_descriptor(&self) -> Option<MappingDescriptorV1> {
        self.inspect_state().active.mapping_descriptor.clone()
    }

    fn apply_committed<'a>(
        &'a self,
        batch: CommittedMutationBatch,
    ) -> AdapterFuture<'a, ApplyReceipt> {
        Box::pin(async move {
            let (generation, active, shadow) = {
                let state = self.lock_state()?;
                self.in_flight_applies.fetch_add(1, Ordering::AcqRel);
                (
                    state.generation,
                    state.active.adapter_arc(),
                    state
                        .shadow
                        .as_ref()
                        .map(|shadow| (shadow.generation, shadow.opened.adapter_arc())),
                )
            };
            let _in_flight = InFlightApply::new(&self.in_flight_applies);
            let receipt = active.apply_committed(batch.clone()).await?;
            if let Some((shadow_generation, target)) = shadow {
                let target_receipt = target.apply_committed(batch).await?;
                if target_receipt.applied_log_index != receipt.applied_log_index {
                    return Err(AdapterError::Backend(
                        "backend migration target applied a different log index".to_owned(),
                    ));
                }
                let mut state = self.lock_state()?;
                if state.generation == generation
                    && let Some(current) = state.shadow.as_mut()
                    && current.generation == shadow_generation
                {
                    current.synchronized_index = target_receipt.applied_log_index;
                }
            }
            Ok(receipt)
        })
    }

    fn multi_get<'a>(&'a self, keys: &'a [LogicalKey]) -> AdapterFuture<'a, Vec<Option<Vec<u8>>>> {
        let active = match self.lock_state() {
            Ok(state) => state.active.adapter_arc(),
            Err(error) => return Box::pin(async move { Err(error) }),
        };
        Box::pin(async move { active.multi_get(keys).await })
    }

    fn scan<'a>(&'a self, span: &'a KeySpan) -> AdapterFuture<'a, Vec<KeyValue>> {
        let active = match self.lock_state() {
            Ok(state) => state.active.adapter_arc(),
            Err(error) => return Box::pin(async move { Err(error) }),
        };
        Box::pin(async move { active.scan(span).await })
    }

    fn scan_candidates<'a>(
        &'a self,
        request: &'a CandidateScanRequest,
    ) -> AdapterFuture<'a, CandidateScanPage> {
        let active = match self.lock_state() {
            Ok(state) => state.active.adapter_arc(),
            Err(error) => return Box::pin(async move { Err(error) }),
        };
        Box::pin(async move { active.scan_candidates(request).await })
    }

    fn gather_properties<'a>(
        &'a self,
        request: &'a PropertyGatherRequest,
    ) -> AdapterFuture<'a, PropertyGatherPage> {
        let active = match self.lock_state() {
            Ok(state) => state.active.adapter_arc(),
            Err(error) => return Box::pin(async move { Err(error) }),
        };
        Box::pin(async move { active.gather_properties(request).await })
    }

    fn expand_adjacency<'a>(
        &'a self,
        request: &'a AdjacencyExpandRequest,
    ) -> AdapterFuture<'a, AdjacencyExpandPage> {
        let active = match self.lock_state() {
            Ok(state) => state.active.adapter_arc(),
            Err(error) => return Box::pin(async move { Err(error) }),
        };
        Box::pin(async move { active.expand_adjacency(request).await })
    }

    fn scan_changes<'a>(
        &'a self,
        request: &'a ChangeScanRequest,
    ) -> AdapterFuture<'a, ChangeScanPage> {
        let active = match self.lock_state() {
            Ok(state) => state.active.adapter_arc(),
            Err(error) => return Box::pin(async move { Err(error) }),
        };
        Box::pin(async move { active.scan_changes(request).await })
    }

    fn scan_fenced<'a>(&'a self, span: &'a KeySpan) -> AdapterFuture<'a, FencedScan> {
        let active = match self.lock_state() {
            Ok(state) => state.active.adapter_arc(),
            Err(error) => return Box::pin(async move { Err(error) }),
        };
        Box::pin(async move { active.scan_fenced(span).await })
    }

    fn create_physical_checkpoint(
        &self,
        destination: &std::path::Path,
    ) -> Result<(), AdapterError> {
        self.lock_state()?
            .active
            .adapter
            .create_physical_checkpoint(destination)
    }

    fn applied_log_index(&self) -> Result<u64, AdapterError> {
        self.lock_state()?.active.adapter.applied_log_index()
    }
}

struct InFlightApply<'counter> {
    counter: &'counter AtomicUsize,
}

impl<'counter> InFlightApply<'counter> {
    const fn new(counter: &'counter AtomicUsize) -> Self {
        Self { counter }
    }
}

impl Drop for InFlightApply<'_> {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::AcqRel);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MigrationStatus {
    Idle {
        generation: u64,
    },
    DualApplying {
        source_generation: u64,
        target_generation: u64,
        synchronized_index: u64,
    },
}

#[derive(Debug)]
pub enum MigrationError {
    Compatibility(AdapterCompatibilityError),
    Adapter(AdapterError),
    AlreadyMigrating,
    NotMigrating,
    ApplyInFlight { count: usize },
    TargetIndexMismatch { source: u64, target: u64 },
    SynchronizedIndexMismatch { durable: u64, recorded: u64 },
    InvalidGeneration { generation: u64 },
    NonConsecutiveGeneration { source: u64, target: u64 },
    GenerationExhausted,
    LockPoisoned,
}

impl Display for MigrationError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Compatibility(error) => Display::fmt(error, formatter),
            Self::Adapter(error) => Display::fmt(error, formatter),
            Self::AlreadyMigrating => formatter.write_str("an Adapter migration is already active"),
            Self::NotMigrating => formatter.write_str("no Adapter migration is active"),
            Self::ApplyInFlight { count } => {
                write!(
                    formatter,
                    "{count} Adapter apply operation(s) are still in flight"
                )
            }
            Self::TargetIndexMismatch { source, target } => write!(
                formatter,
                "migration source applied index {source} differs from target index {target}"
            ),
            Self::SynchronizedIndexMismatch { durable, recorded } => write!(
                formatter,
                "recorded synchronized index {recorded} differs from durable index {durable}"
            ),
            Self::InvalidGeneration { generation } => {
                write!(formatter, "Adapter generation {generation} is invalid")
            }
            Self::NonConsecutiveGeneration { source, target } => write!(
                formatter,
                "target Adapter generation {target} does not immediately follow source generation {source}"
            ),
            Self::GenerationExhausted => formatter.write_str("Adapter generation is exhausted"),
            Self::LockPoisoned => formatter.write_str("Adapter migration state lock is poisoned"),
        }
    }
}

impl Error for MigrationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Compatibility(error) => Some(error),
            Self::Adapter(error) => Some(error),
            _ => None,
        }
    }
}

impl From<AdapterCompatibilityError> for MigrationError {
    fn from(error: AdapterCompatibilityError) -> Self {
        Self::Compatibility(error)
    }
}

impl From<AdapterError> for MigrationError {
    fn from(error: AdapterError) -> Self {
        Self::Adapter(error)
    }
}
