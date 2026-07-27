use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;

use crate::{
    AdapterCapabilities, AdapterDescriptorV1, AdapterError, AdapterFuture, AdapterRequirement,
    ApplyReceipt, BackendFamily, CommittedMutationBatch, Durability, KeySpan, KeyValue, LogicalKey,
    LogicalSnapshotChunkV1, LogicalSnapshotExportRequest, LogicalSnapshotHeaderV1,
    LogicalSnapshotManifestV1, LogicalSnapshotReader, ReadSnapshot, SnapshotCapability,
    StorageAdapter,
};

pub const MAPPING_SPI_VERSION: u16 = 1;
const MAX_MAPPING_NAME_BYTES: usize = 64;
const MAX_MAPPING_VERSION_BYTES: usize = 64;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MappingCapabilities {
    pub atomic_batch_lifecycle: bool,
    pub deterministic_mapping: bool,
    pub idempotent_replay: bool,
    pub canonical_multi_get: bool,
    pub canonical_ordered_scan: bool,
    pub durable_applied_index: bool,
    pub durability: Durability,
    pub snapshot: SnapshotCapability,
    pub canonical_export: bool,
    pub canonical_restore: bool,
    pub native_temporal_layout: bool,
    pub predicate_pushdown: bool,
    pub adjacency_pushdown: bool,
    pub change_feed: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MappingDescriptorV1 {
    spi_version: u16,
    name: String,
    version: String,
    family: BackendFamily,
    schema_fingerprint: [u8; 32],
    capabilities: MappingCapabilities,
}

impl MappingDescriptorV1 {
    pub fn new(
        name: impl Into<String>,
        version: impl Into<String>,
        family: BackendFamily,
        schema_fingerprint: [u8; 32],
        capabilities: MappingCapabilities,
    ) -> Result<Self, MappingCompatibilityError> {
        Self::with_spi_version(
            MAPPING_SPI_VERSION,
            name,
            version,
            family,
            schema_fingerprint,
            capabilities,
        )
    }

    pub fn with_spi_version(
        spi_version: u16,
        name: impl Into<String>,
        version: impl Into<String>,
        family: BackendFamily,
        schema_fingerprint: [u8; 32],
        capabilities: MappingCapabilities,
    ) -> Result<Self, MappingCompatibilityError> {
        let name = name.into();
        let version = version.into();
        if !valid_mapping_name(&name) {
            return Err(MappingCompatibilityError::InvalidName { name });
        }
        if version.is_empty()
            || version.len() > MAX_MAPPING_VERSION_BYTES
            || version.chars().any(char::is_control)
        {
            return Err(MappingCompatibilityError::InvalidVersion);
        }
        if schema_fingerprint == [0; 32] {
            return Err(MappingCompatibilityError::ZeroSchemaFingerprint);
        }
        Ok(Self {
            spi_version,
            name,
            version,
            family,
            schema_fingerprint,
            capabilities,
        })
    }

    #[must_use]
    pub const fn spi_version(&self) -> u16 {
        self.spi_version
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub fn version(&self) -> &str {
        &self.version
    }

    #[must_use]
    pub const fn family(&self) -> BackendFamily {
        self.family
    }

    #[must_use]
    pub const fn schema_fingerprint(&self) -> [u8; 32] {
        self.schema_fingerprint
    }

    #[must_use]
    pub const fn capabilities(&self) -> MappingCapabilities {
        self.capabilities
    }

    pub fn validate(
        &self,
        requirement: MappingRequirement,
    ) -> Result<(), MappingCompatibilityError> {
        if self.spi_version != MAPPING_SPI_VERSION {
            return Err(MappingCompatibilityError::SpiVersionMismatch {
                expected: MAPPING_SPI_VERSION,
                actual: self.spi_version,
            });
        }
        if requirement == MappingRequirement::Development {
            return Ok(());
        }
        for (available, capability) in [
            (
                self.capabilities.atomic_batch_lifecycle,
                RequiredMappingCapability::AtomicBatchLifecycle,
            ),
            (
                self.capabilities.deterministic_mapping,
                RequiredMappingCapability::DeterministicMapping,
            ),
            (
                self.capabilities.idempotent_replay,
                RequiredMappingCapability::IdempotentReplay,
            ),
            (
                self.capabilities.canonical_multi_get,
                RequiredMappingCapability::CanonicalMultiGet,
            ),
            (
                self.capabilities.canonical_ordered_scan,
                RequiredMappingCapability::CanonicalOrderedScan,
            ),
            (
                self.capabilities.durable_applied_index,
                RequiredMappingCapability::DurableAppliedIndex,
            ),
            (
                self.capabilities.durability == Durability::Synchronous,
                RequiredMappingCapability::SynchronousDurability,
            ),
            (
                self.capabilities.snapshot != SnapshotCapability::None,
                RequiredMappingCapability::SnapshotRecovery,
            ),
        ] {
            if !available {
                return Err(MappingCompatibilityError::MissingCapability {
                    mapping: self.name.clone(),
                    capability,
                });
            }
        }
        if requirement == MappingRequirement::HotPluggableReplica {
            for (available, capability) in [
                (
                    self.capabilities.canonical_export,
                    RequiredMappingCapability::CanonicalExport,
                ),
                (
                    self.capabilities.canonical_restore,
                    RequiredMappingCapability::CanonicalRestore,
                ),
            ] {
                if !available {
                    return Err(MappingCompatibilityError::MissingCapability {
                        mapping: self.name.clone(),
                        capability,
                    });
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MappingRequirement {
    Development,
    ManagedReplica,
    HotPluggableReplica,
}

impl From<AdapterRequirement> for MappingRequirement {
    fn from(requirement: AdapterRequirement) -> Self {
        match requirement {
            AdapterRequirement::Development => Self::Development,
            AdapterRequirement::ManagedReplica => Self::ManagedReplica,
            AdapterRequirement::HotPluggableReplica => Self::HotPluggableReplica,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RequiredMappingCapability {
    AtomicBatchLifecycle,
    DeterministicMapping,
    IdempotentReplay,
    CanonicalMultiGet,
    CanonicalOrderedScan,
    DurableAppliedIndex,
    SynchronousDurability,
    SnapshotRecovery,
    CanonicalExport,
    CanonicalRestore,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MappingCompatibilityError {
    InvalidName {
        name: String,
    },
    InvalidVersion,
    ZeroSchemaFingerprint,
    SpiVersionMismatch {
        expected: u16,
        actual: u16,
    },
    MissingCapability {
        mapping: String,
        capability: RequiredMappingCapability,
    },
}

impl Display for MappingCompatibilityError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidName { name } => write!(formatter, "invalid Mapping name {name:?}"),
            Self::InvalidVersion => formatter.write_str("invalid Mapping version"),
            Self::ZeroSchemaFingerprint => {
                formatter.write_str("Mapping schema fingerprint cannot be zero")
            }
            Self::SpiVersionMismatch { expected, actual } => write!(
                formatter,
                "Mapping SPI version {actual} is incompatible with required version {expected}"
            ),
            Self::MissingCapability {
                mapping,
                capability,
            } => write!(
                formatter,
                "Mapping {mapping} is missing required capability {capability:?}"
            ),
        }
    }
}

impl Error for MappingCompatibilityError {}

fn valid_mapping_name(name: &str) -> bool {
    (1..=MAX_MAPPING_NAME_BYTES).contains(&name.len())
        && name.as_bytes().first().is_some_and(u8::is_ascii_lowercase)
        && name.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"._-".contains(&byte)
        })
}

pub type MappingFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, AdapterError>> + Send + 'a>>;

pub trait PreparedMappingTransaction: Send {
    fn apply<'a>(&'a mut self) -> MappingFuture<'a, ()>;

    fn commit<'a>(&'a mut self) -> MappingFuture<'a, ApplyReceipt>;

    fn abort<'a>(self: Box<Self>) -> MappingFuture<'a, ()>
    where
        Self: 'a;
}

pub trait CanonicalRestoreSession: Send {
    fn write_chunk<'a>(&'a mut self, chunk: LogicalSnapshotChunkV1) -> MappingFuture<'a, ()>;

    fn commit<'a>(&'a mut self, manifest: LogicalSnapshotManifestV1) -> MappingFuture<'a, ()>;

    fn abort<'a>(self: Box<Self>) -> MappingFuture<'a, ()>
    where
        Self: 'a;
}

pub trait TemporalBackendMapping: Send + Sync {
    fn describe_schema(&self) -> MappingDescriptorV1;

    fn capabilities(&self) -> MappingCapabilities {
        self.describe_schema().capabilities()
    }

    fn validate_mapping(&self) -> Result<(), AdapterError>;

    fn prepare<'a>(
        &'a self,
        batch: CommittedMutationBatch,
    ) -> MappingFuture<'a, Box<dyn PreparedMappingTransaction + 'a>>;

    fn get<'a>(&'a self, key: &'a LogicalKey) -> MappingFuture<'a, Option<Vec<u8>>> {
        Box::pin(async move {
            let mut values = self.multi_get(std::slice::from_ref(key)).await?;
            values.pop().ok_or(AdapterError::Backend(
                "Mapping multi_get returned no slot for one requested key".into(),
            ))
        })
    }

    fn multi_get<'a>(&'a self, keys: &'a [LogicalKey]) -> MappingFuture<'a, Vec<Option<Vec<u8>>>>;

    fn scan<'a>(&'a self, span: &'a KeySpan) -> MappingFuture<'a, Vec<KeyValue>>;

    fn begin_read_snapshot<'a>(&'a self) -> MappingFuture<'a, Box<dyn ReadSnapshot + 'a>> {
        Box::pin(async move {
            Err(AdapterError::UnsupportedOperation {
                operation: "Mapping query read snapshot",
            })
        })
    }

    fn export_canonical<'a>(
        &'a self,
        _request: LogicalSnapshotExportRequest,
    ) -> MappingFuture<'a, Box<dyn LogicalSnapshotReader + 'a>> {
        Box::pin(async {
            Err(AdapterError::UnsupportedOperation {
                operation: "canonical Mapping export",
            })
        })
    }

    fn restore_canonical<'a>(
        &'a self,
        _header: LogicalSnapshotHeaderV1,
    ) -> MappingFuture<'a, Box<dyn CanonicalRestoreSession + 'a>> {
        Box::pin(async {
            Err(AdapterError::UnsupportedOperation {
                operation: "canonical Mapping restore",
            })
        })
    }

    fn create_physical_checkpoint(&self, _destination: &Path) -> Result<(), AdapterError> {
        Err(AdapterError::UnsupportedOperation {
            operation: "physical Mapping checkpoint",
        })
    }

    fn applied_log_index(&self) -> Result<u64, AdapterError>;
}

pub struct MappingBackedAdapter {
    mapping: Arc<dyn TemporalBackendMapping>,
    mapping_descriptor: MappingDescriptorV1,
    adapter_descriptor: AdapterDescriptorV1,
}

impl MappingBackedAdapter {
    pub fn new(
        mapping: Arc<dyn TemporalBackendMapping>,
        requirement: MappingRequirement,
    ) -> Result<Self, AdapterError> {
        let descriptor = mapping.describe_schema();
        Self::with_runtime_identity(
            descriptor.name(),
            descriptor.version(),
            mapping,
            requirement,
        )
    }

    pub fn with_runtime_identity(
        implementation: impl Into<String>,
        implementation_version: impl Into<String>,
        mapping: Arc<dyn TemporalBackendMapping>,
        requirement: MappingRequirement,
    ) -> Result<Self, AdapterError> {
        let mapping_descriptor = mapping.describe_schema();
        mapping_descriptor.validate(requirement)?;
        mapping.validate_mapping()?;
        let adapter_descriptor = AdapterDescriptorV1::new(
            implementation,
            implementation_version,
            mapping_descriptor.family(),
            adapter_capabilities(mapping_descriptor.capabilities()),
        );
        Ok(Self {
            mapping,
            mapping_descriptor,
            adapter_descriptor,
        })
    }

    #[must_use]
    pub const fn mapping_descriptor(&self) -> &MappingDescriptorV1 {
        &self.mapping_descriptor
    }
}

impl StorageAdapter for MappingBackedAdapter {
    fn descriptor(&self) -> AdapterDescriptorV1 {
        self.adapter_descriptor.clone()
    }

    fn capabilities(&self) -> AdapterCapabilities {
        self.adapter_descriptor.capabilities()
    }

    fn mapping_descriptor(&self) -> Option<MappingDescriptorV1> {
        Some(self.mapping_descriptor.clone())
    }

    fn apply_committed<'a>(
        &'a self,
        batch: CommittedMutationBatch,
    ) -> AdapterFuture<'a, ApplyReceipt> {
        Box::pin(async move {
            let mut transaction = self.mapping.prepare(batch).await?;
            if let Err(error) = transaction.apply().await {
                return abort_after_error(transaction, error).await;
            }
            match transaction.commit().await {
                Ok(receipt) => Ok(receipt),
                Err(error) => abort_after_error(transaction, error).await,
            }
        })
    }

    fn multi_get<'a>(&'a self, keys: &'a [LogicalKey]) -> AdapterFuture<'a, Vec<Option<Vec<u8>>>> {
        self.mapping.multi_get(keys)
    }

    fn scan<'a>(&'a self, span: &'a KeySpan) -> AdapterFuture<'a, Vec<KeyValue>> {
        self.mapping.scan(span)
    }

    fn begin_read_snapshot<'a>(&'a self) -> AdapterFuture<'a, Box<dyn ReadSnapshot + 'a>> {
        self.mapping.begin_read_snapshot()
    }

    fn begin_logical_export<'a>(
        &'a self,
        request: LogicalSnapshotExportRequest,
    ) -> AdapterFuture<'a, Box<dyn LogicalSnapshotReader + 'a>> {
        self.mapping.export_canonical(request)
    }

    fn create_physical_checkpoint(&self, destination: &Path) -> Result<(), AdapterError> {
        self.mapping.create_physical_checkpoint(destination)
    }

    fn applied_log_index(&self) -> Result<u64, AdapterError> {
        self.mapping.applied_log_index()
    }
}

async fn abort_after_error<T>(
    transaction: Box<dyn PreparedMappingTransaction + '_>,
    primary: AdapterError,
) -> Result<T, AdapterError> {
    match transaction.abort().await {
        Ok(()) => Err(primary),
        Err(abort) => Err(AdapterError::Backend(format!(
            "Mapping operation failed ({primary}); abort also failed ({abort})"
        ))),
    }
}

const fn adapter_capabilities(capabilities: MappingCapabilities) -> AdapterCapabilities {
    AdapterCapabilities {
        local_atomic_batch: capabilities.atomic_batch_lifecycle,
        idempotent_apply: capabilities.idempotent_replay,
        consistent_multi_get: capabilities.canonical_multi_get,
        ordered_scan: capabilities.canonical_ordered_scan,
        durable_applied_index: capabilities.durable_applied_index,
        durability: capabilities.durability,
        snapshot: capabilities.snapshot,
        logical_export: capabilities.canonical_export,
        logical_restore: capabilities.canonical_restore,
        predicate_pushdown: capabilities.predicate_pushdown,
        adjacency_pushdown: capabilities.adjacency_pushdown,
        change_feed: capabilities.change_feed,
    }
}
