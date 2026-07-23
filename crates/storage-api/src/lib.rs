#![forbid(unsafe_code)]

mod mapping;
#[cfg(feature = "mapping-tck")]
mod mapping_tck;

pub use mapping::{
    CanonicalRestoreSession, MAPPING_SPI_VERSION, MappingBackedAdapter, MappingCapabilities,
    MappingCompatibilityError, MappingDescriptorV1, MappingFuture, MappingRequirement,
    PreparedMappingTransaction, RequiredMappingCapability, TemporalBackendMapping,
};
#[cfg(feature = "mapping-tck")]
pub use mapping_tck::{run_mapping_restore_tck, run_mapping_tck};

use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

pub const ADAPTER_SPI_VERSION: u16 = 1;
pub const LOGICAL_SNAPSHOT_FORMAT_VERSION: u16 = 1;
pub const MAX_LOGICAL_SNAPSHOT_CHUNK_ENTRIES: usize = 65_536;
pub const MAX_LOGICAL_SNAPSHOT_CHUNK_BYTES: usize = 16 * 1024 * 1024;
pub const ADAPTER_META_APPLIED_LOG_INDEX_KEY: &[u8] = b"\x00applied_log_index";
const ADAPTER_LOG_FINGERPRINT_PREFIX: u8 = 0x01;
const ADAPTER_MUTATION_FINGERPRINT_PREFIX: u8 = 0x02;
static NEXT_LOGICAL_SNAPSHOT_ID: AtomicU64 = AtomicU64::new(1);

#[must_use]
pub fn new_logical_snapshot_id() -> u128 {
    let time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    let process_and_time = (time as u64) ^ (u64::from(std::process::id()) << 32);
    let sequence = NEXT_LOGICAL_SNAPSHOT_ID.fetch_add(1, Ordering::Relaxed);
    (u128::from(sequence) << 64) | u128::from(process_and_time)
}

#[must_use]
pub fn adapter_log_fingerprint_key(log_index: u64) -> [u8; 9] {
    let mut key = [0; 9];
    key[0] = ADAPTER_LOG_FINGERPRINT_PREFIX;
    key[1..].copy_from_slice(&log_index.to_be_bytes());
    key
}

#[must_use]
pub fn adapter_mutation_fingerprint_key(txn_id: u128, sequence: u32) -> [u8; 21] {
    let mut key = [0; 21];
    key[0] = ADAPTER_MUTATION_FINGERPRINT_PREFIX;
    key[1..17].copy_from_slice(&txn_id.to_be_bytes());
    key[17..].copy_from_slice(&sequence.to_be_bytes());
    key
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackendFamily {
    KeyValue,
    Sql,
    PropertyGraph,
    Test,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Durability {
    /// Process loss can lose acknowledged data. Never valid for a production Replica.
    Volatile,
    /// Durability depends on backend configuration that the Adapter could not verify.
    BackendConfigured,
    /// A successful apply acknowledgement includes a synchronous durable commit.
    Synchronous,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SnapshotCapability {
    None,
    PhysicalCheckpoint,
    LogicalExport,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdapterCapabilities {
    pub local_atomic_batch: bool,
    pub idempotent_apply: bool,
    pub consistent_multi_get: bool,
    pub ordered_scan: bool,
    pub durable_applied_index: bool,
    pub durability: Durability,
    pub snapshot: SnapshotCapability,
    pub logical_export: bool,
    pub logical_restore: bool,
    pub predicate_pushdown: bool,
    pub adjacency_pushdown: bool,
    pub change_feed: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdapterDescriptorV1 {
    spi_version: u16,
    implementation: String,
    implementation_version: String,
    family: BackendFamily,
    capabilities: AdapterCapabilities,
}

impl AdapterDescriptorV1 {
    #[must_use]
    pub fn new(
        implementation: impl Into<String>,
        implementation_version: impl Into<String>,
        family: BackendFamily,
        capabilities: AdapterCapabilities,
    ) -> Self {
        Self::with_spi_version(
            ADAPTER_SPI_VERSION,
            implementation,
            implementation_version,
            family,
            capabilities,
        )
    }

    #[must_use]
    pub fn with_spi_version(
        spi_version: u16,
        implementation: impl Into<String>,
        implementation_version: impl Into<String>,
        family: BackendFamily,
        capabilities: AdapterCapabilities,
    ) -> Self {
        Self {
            spi_version,
            implementation: implementation.into(),
            implementation_version: implementation_version.into(),
            family,
            capabilities,
        }
    }

    #[must_use]
    pub const fn spi_version(&self) -> u16 {
        self.spi_version
    }

    #[must_use]
    pub fn implementation(&self) -> &str {
        &self.implementation
    }

    #[must_use]
    pub fn implementation_version(&self) -> &str {
        &self.implementation_version
    }

    #[must_use]
    pub const fn family(&self) -> BackendFamily {
        self.family
    }

    #[must_use]
    pub const fn capabilities(&self) -> AdapterCapabilities {
        self.capabilities
    }

    pub fn validate(
        &self,
        requirement: AdapterRequirement,
    ) -> Result<(), AdapterCompatibilityError> {
        if self.spi_version != ADAPTER_SPI_VERSION {
            return Err(AdapterCompatibilityError::SpiVersionMismatch {
                expected: ADAPTER_SPI_VERSION,
                actual: self.spi_version,
            });
        }
        if requirement == AdapterRequirement::Development {
            return Ok(());
        }
        for (available, capability) in [
            (
                self.capabilities.local_atomic_batch,
                RequiredCapability::LocalAtomicBatch,
            ),
            (
                self.capabilities.idempotent_apply,
                RequiredCapability::IdempotentApply,
            ),
            (
                self.capabilities.consistent_multi_get,
                RequiredCapability::ConsistentMultiGet,
            ),
            (
                self.capabilities.ordered_scan,
                RequiredCapability::OrderedScan,
            ),
            (
                self.capabilities.durable_applied_index,
                RequiredCapability::DurableAppliedIndex,
            ),
            (
                self.capabilities.durability == Durability::Synchronous,
                RequiredCapability::SynchronousDurability,
            ),
            (
                self.capabilities.snapshot != SnapshotCapability::None,
                RequiredCapability::SnapshotRecovery,
            ),
        ] {
            if !available {
                return Err(AdapterCompatibilityError::MissingCapability {
                    adapter: self.implementation.clone(),
                    capability,
                });
            }
        }
        if requirement == AdapterRequirement::HotPluggableReplica {
            for (available, capability) in [
                (
                    self.capabilities.logical_export,
                    RequiredCapability::LogicalExport,
                ),
                (
                    self.capabilities.logical_restore,
                    RequiredCapability::LogicalRestore,
                ),
            ] {
                if !available {
                    return Err(AdapterCompatibilityError::MissingCapability {
                        adapter: self.implementation.clone(),
                        capability,
                    });
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdapterRequirement {
    Development,
    ManagedReplica,
    HotPluggableReplica,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RequiredCapability {
    LocalAtomicBatch,
    IdempotentApply,
    ConsistentMultiGet,
    OrderedScan,
    DurableAppliedIndex,
    SynchronousDurability,
    SnapshotRecovery,
    LogicalExport,
    LogicalRestore,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdapterCompatibilityError {
    SpiVersionMismatch {
        expected: u16,
        actual: u16,
    },
    MissingCapability {
        adapter: String,
        capability: RequiredCapability,
    },
}

impl Display for AdapterCompatibilityError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::SpiVersionMismatch { expected, actual } => write!(
                formatter,
                "Adapter SPI version {actual} is incompatible with required version {expected}"
            ),
            Self::MissingCapability {
                adapter,
                capability,
            } => write!(
                formatter,
                "Adapter {adapter} is missing required capability {capability:?}"
            ),
        }
    }
}

impl Error for AdapterCompatibilityError {}

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum Keyspace {
    Meta = 0,
    Identity = 1,
    Current = 2,
    AdjOut = 3,
    AdjIn = 4,
    History = 5,
    TemporalIndex = 6,
    Txn = 7,
}

impl Keyspace {
    pub const ALL: [Self; 8] = [
        Self::Meta,
        Self::Identity,
        Self::Current,
        Self::AdjOut,
        Self::AdjIn,
        Self::History,
        Self::TemporalIndex,
        Self::Txn,
    ];

    #[must_use]
    pub const fn tag(self) -> u8 {
        self as u8
    }

    #[must_use]
    pub const fn column_family(self) -> &'static str {
        match self {
            Self::Meta => "meta",
            Self::Identity => "identity",
            Self::Current => "current",
            Self::AdjOut => "adj_out",
            Self::AdjIn => "adj_in",
            Self::History => "history",
            Self::TemporalIndex => "temporal_index",
            Self::Txn => "txn",
        }
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct LogicalKey {
    keyspace: Keyspace,
    bytes: Vec<u8>,
}

impl LogicalKey {
    #[must_use]
    pub const fn new(bytes: Vec<u8>) -> Self {
        Self::in_keyspace(Keyspace::Current, bytes)
    }

    #[must_use]
    pub const fn in_keyspace(keyspace: Keyspace, bytes: Vec<u8>) -> Self {
        Self { keyspace, bytes }
    }

    #[must_use]
    pub const fn keyspace(&self) -> Keyspace {
        self.keyspace
    }

    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KeySpan {
    keyspace: Keyspace,
    start: Vec<u8>,
    end: Option<Vec<u8>>,
    required_prefix: Option<Vec<u8>>,
    limit: Option<usize>,
    max_bytes: Option<u64>,
}

impl KeySpan {
    #[must_use]
    pub fn prefix(keyspace: Keyspace, prefix: Vec<u8>) -> Self {
        let end = prefix_successor(&prefix);
        Self {
            keyspace,
            start: prefix.clone(),
            end,
            required_prefix: Some(prefix),
            limit: None,
            max_bytes: None,
        }
    }

    pub fn prefix_from(
        keyspace: Keyspace,
        prefix: Vec<u8>,
        start: Vec<u8>,
    ) -> Result<Self, KeySpanError> {
        if !start.starts_with(&prefix) {
            return Err(KeySpanError::StartOutsidePrefix);
        }
        Ok(Self {
            keyspace,
            start,
            end: prefix_successor(&prefix),
            required_prefix: Some(prefix),
            limit: None,
            max_bytes: None,
        })
    }

    pub fn range(
        keyspace: Keyspace,
        start: Vec<u8>,
        end: Option<Vec<u8>>,
    ) -> Result<Self, KeySpanError> {
        if end.as_ref().is_some_and(|end| end <= &start) {
            return Err(KeySpanError::EmptyOrReversed);
        }
        Ok(Self {
            keyspace,
            start,
            end,
            required_prefix: None,
            limit: None,
            max_bytes: None,
        })
    }

    pub fn with_limit(mut self, limit: usize) -> Result<Self, KeySpanError> {
        if limit == 0 {
            return Err(KeySpanError::ZeroLimit);
        }
        self.limit = Some(limit);
        Ok(self)
    }

    pub fn with_max_bytes(mut self, max_bytes: u64) -> Result<Self, KeySpanError> {
        if max_bytes == 0 {
            return Err(KeySpanError::ZeroByteLimit);
        }
        self.max_bytes = Some(max_bytes);
        Ok(self)
    }

    #[must_use]
    pub const fn keyspace(&self) -> Keyspace {
        self.keyspace
    }

    #[must_use]
    pub fn start(&self) -> &[u8] {
        &self.start
    }

    #[must_use]
    pub fn end(&self) -> Option<&[u8]> {
        self.end.as_deref()
    }

    #[must_use]
    pub fn contains(&self, key: &[u8]) -> bool {
        key >= self.start.as_slice()
            && self.end.as_deref().is_none_or(|end| key < end)
            && self
                .required_prefix
                .as_deref()
                .is_none_or(|prefix| key.starts_with(prefix))
    }

    #[must_use]
    pub const fn limit(&self) -> Option<usize> {
        self.limit
    }

    #[must_use]
    pub const fn max_bytes(&self) -> Option<u64> {
        self.max_bytes
    }

    #[must_use]
    pub fn required_prefix(&self) -> Option<&[u8]> {
        self.required_prefix.as_deref()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KeySpanError {
    EmptyOrReversed,
    StartOutsidePrefix,
    ZeroLimit,
    ZeroByteLimit,
}

impl Display for KeySpanError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyOrReversed => formatter.write_str("key span end must follow its start"),
            Self::StartOutsidePrefix => {
                formatter.write_str("key span seek start is outside the required prefix")
            }
            Self::ZeroLimit => formatter.write_str("key span limit must be positive"),
            Self::ZeroByteLimit => formatter.write_str("key span byte limit must be positive"),
        }
    }
}

impl Error for KeySpanError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KeyValue {
    key: LogicalKey,
    value: Vec<u8>,
}

impl KeyValue {
    #[must_use]
    pub const fn new(key: LogicalKey, value: Vec<u8>) -> Self {
        Self { key, value }
    }

    #[must_use]
    pub const fn key(&self) -> &LogicalKey {
        &self.key
    }

    #[must_use]
    pub fn value(&self) -> &[u8] {
        &self.value
    }

    #[must_use]
    pub fn into_parts(self) -> (LogicalKey, Vec<u8>) {
        (self.key, self.value)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LogicalSnapshotExportRequest {
    max_entries_per_chunk: usize,
    max_bytes_per_chunk: usize,
}

impl LogicalSnapshotExportRequest {
    pub fn new(
        max_entries_per_chunk: usize,
        max_bytes_per_chunk: usize,
    ) -> Result<Self, LogicalSnapshotError> {
        if !(1..=MAX_LOGICAL_SNAPSHOT_CHUNK_ENTRIES).contains(&max_entries_per_chunk) {
            return Err(LogicalSnapshotError::InvalidChunkEntryLimit {
                max: MAX_LOGICAL_SNAPSHOT_CHUNK_ENTRIES,
                actual: max_entries_per_chunk,
            });
        }
        if !(1..=MAX_LOGICAL_SNAPSHOT_CHUNK_BYTES).contains(&max_bytes_per_chunk) {
            return Err(LogicalSnapshotError::InvalidChunkByteLimit {
                max: MAX_LOGICAL_SNAPSHOT_CHUNK_BYTES,
                actual: max_bytes_per_chunk,
            });
        }
        Ok(Self {
            max_entries_per_chunk,
            max_bytes_per_chunk,
        })
    }

    #[must_use]
    pub const fn max_entries_per_chunk(self) -> usize {
        self.max_entries_per_chunk
    }

    #[must_use]
    pub const fn max_bytes_per_chunk(self) -> usize {
        self.max_bytes_per_chunk
    }
}

impl Default for LogicalSnapshotExportRequest {
    fn default() -> Self {
        Self {
            max_entries_per_chunk: 4_096,
            max_bytes_per_chunk: 4 * 1024 * 1024,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicalSnapshotHeaderV1 {
    format_version: u16,
    snapshot_id: u128,
    applied_log_index: u64,
}

impl LogicalSnapshotHeaderV1 {
    #[must_use]
    pub const fn new(snapshot_id: u128, applied_log_index: u64) -> Self {
        Self {
            format_version: LOGICAL_SNAPSHOT_FORMAT_VERSION,
            snapshot_id,
            applied_log_index,
        }
    }

    #[must_use]
    pub const fn format_version(&self) -> u16 {
        self.format_version
    }

    #[must_use]
    pub const fn snapshot_id(&self) -> u128 {
        self.snapshot_id
    }

    #[must_use]
    pub const fn applied_log_index(&self) -> u64 {
        self.applied_log_index
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicalSnapshotChunkV1 {
    snapshot_id: u128,
    ordinal: u64,
    entries: Vec<KeyValue>,
    digest: [u8; 32],
}

impl LogicalSnapshotChunkV1 {
    pub fn new(
        snapshot_id: u128,
        ordinal: u64,
        entries: Vec<KeyValue>,
    ) -> Result<Self, LogicalSnapshotError> {
        validate_snapshot_entries(&entries)?;
        let digest = hash_snapshot_chunk(snapshot_id, ordinal, &entries);
        Ok(Self {
            snapshot_id,
            ordinal,
            entries,
            digest,
        })
    }

    pub fn from_parts(
        snapshot_id: u128,
        ordinal: u64,
        entries: Vec<KeyValue>,
        digest: [u8; 32],
    ) -> Result<Self, LogicalSnapshotError> {
        let chunk = Self::new(snapshot_id, ordinal, entries)?;
        if chunk.digest != digest {
            return Err(LogicalSnapshotError::ChunkDigestMismatch { ordinal });
        }
        Ok(chunk)
    }

    #[must_use]
    pub const fn snapshot_id(&self) -> u128 {
        self.snapshot_id
    }

    #[must_use]
    pub const fn ordinal(&self) -> u64 {
        self.ordinal
    }

    #[must_use]
    pub fn entries(&self) -> &[KeyValue] {
        &self.entries
    }

    #[must_use]
    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }

    #[must_use]
    pub fn payload_bytes(&self) -> usize {
        self.entries.iter().fold(0_usize, |total, entry| {
            total
                .saturating_add(1)
                .saturating_add(8)
                .saturating_add(entry.key().as_bytes().len())
                .saturating_add(8)
                .saturating_add(entry.value().len())
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicalSnapshotManifestV1 {
    header: LogicalSnapshotHeaderV1,
    total_chunks: u64,
    total_entries: u64,
    content_digest: [u8; 32],
}

impl LogicalSnapshotManifestV1 {
    pub fn from_parts(
        header: LogicalSnapshotHeaderV1,
        total_chunks: u64,
        total_entries: u64,
        content_digest: [u8; 32],
    ) -> Result<Self, LogicalSnapshotError> {
        if header.format_version != LOGICAL_SNAPSHOT_FORMAT_VERSION {
            return Err(LogicalSnapshotError::UnsupportedFormatVersion {
                expected: LOGICAL_SNAPSHOT_FORMAT_VERSION,
                actual: header.format_version,
            });
        }
        Ok(Self {
            header,
            total_chunks,
            total_entries,
            content_digest,
        })
    }

    #[must_use]
    pub const fn header(&self) -> &LogicalSnapshotHeaderV1 {
        &self.header
    }

    #[must_use]
    pub const fn total_chunks(&self) -> u64 {
        self.total_chunks
    }

    #[must_use]
    pub const fn total_entries(&self) -> u64 {
        self.total_entries
    }

    #[must_use]
    pub const fn content_digest(&self) -> [u8; 32] {
        self.content_digest
    }
}

#[derive(Clone)]
pub struct LogicalSnapshotAccumulator {
    header: LogicalSnapshotHeaderV1,
    next_ordinal: u64,
    total_entries: u64,
    previous_key: Option<LogicalKey>,
    hasher: blake3::Hasher,
}

impl LogicalSnapshotAccumulator {
    #[must_use]
    pub fn new(header: LogicalSnapshotHeaderV1) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"DTGProxy/LogicalSnapshot/V1");
        hasher.update(&header.format_version.to_be_bytes());
        hasher.update(&header.snapshot_id.to_be_bytes());
        hasher.update(&header.applied_log_index.to_be_bytes());
        Self {
            header,
            next_ordinal: 0,
            total_entries: 0,
            previous_key: None,
            hasher,
        }
    }

    pub fn observe(&mut self, chunk: &LogicalSnapshotChunkV1) -> Result<(), LogicalSnapshotError> {
        if chunk.snapshot_id != self.header.snapshot_id {
            return Err(LogicalSnapshotError::SnapshotIdMismatch {
                expected: self.header.snapshot_id,
                actual: chunk.snapshot_id,
            });
        }
        if chunk.ordinal != self.next_ordinal {
            return Err(LogicalSnapshotError::ChunkOrdinalMismatch {
                expected: self.next_ordinal,
                actual: chunk.ordinal,
            });
        }
        validate_snapshot_entries(&chunk.entries)?;
        if hash_snapshot_chunk(chunk.snapshot_id, chunk.ordinal, &chunk.entries) != chunk.digest {
            return Err(LogicalSnapshotError::ChunkDigestMismatch {
                ordinal: chunk.ordinal,
            });
        }
        if let (Some(previous), Some(first)) = (&self.previous_key, chunk.entries.first())
            && previous >= first.key()
        {
            return Err(LogicalSnapshotError::EntriesNotStrictlyOrdered);
        }
        for entry in &chunk.entries {
            hash_snapshot_entry(&mut self.hasher, entry);
        }
        self.total_entries = self
            .total_entries
            .checked_add(
                u64::try_from(chunk.entries.len())
                    .map_err(|_| LogicalSnapshotError::CountOverflow)?,
            )
            .ok_or(LogicalSnapshotError::CountOverflow)?;
        self.next_ordinal = self
            .next_ordinal
            .checked_add(1)
            .ok_or(LogicalSnapshotError::CountOverflow)?;
        self.previous_key = chunk.entries.last().map(|entry| entry.key().clone());
        Ok(())
    }

    #[must_use]
    pub fn complete(self) -> LogicalSnapshotManifestV1 {
        LogicalSnapshotManifestV1 {
            header: self.header,
            total_chunks: self.next_ordinal,
            total_entries: self.total_entries,
            content_digest: *self.hasher.finalize().as_bytes(),
        }
    }

    pub fn verify(self, manifest: &LogicalSnapshotManifestV1) -> Result<(), LogicalSnapshotError> {
        let actual = self.complete();
        if &actual == manifest {
            Ok(())
        } else {
            Err(LogicalSnapshotError::ManifestMismatch)
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LogicalSnapshotError {
    InvalidChunkEntryLimit { max: usize, actual: usize },
    InvalidChunkByteLimit { max: usize, actual: usize },
    EmptyChunk,
    TooManyChunkEntries { max: usize, actual: usize },
    ChunkTooLarge { max: usize, actual: usize },
    EntriesNotStrictlyOrdered,
    SnapshotIdMismatch { expected: u128, actual: u128 },
    ChunkOrdinalMismatch { expected: u64, actual: u64 },
    ChunkDigestMismatch { ordinal: u64 },
    UnsupportedFormatVersion { expected: u16, actual: u16 },
    CountOverflow,
    ManifestMismatch,
    ExportNotExhausted,
    EntryTooLarge { max: usize, actual: usize },
}

impl Display for LogicalSnapshotError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidChunkEntryLimit { max, actual } => write!(
                formatter,
                "logical snapshot chunk entry limit {actual} is outside 1..={max}"
            ),
            Self::InvalidChunkByteLimit { max, actual } => write!(
                formatter,
                "logical snapshot chunk byte limit {actual} is outside 1..={max}"
            ),
            Self::EmptyChunk => formatter.write_str("logical snapshot chunk is empty"),
            Self::TooManyChunkEntries { max, actual } => write!(
                formatter,
                "logical snapshot chunk has {actual} entries; maximum is {max}"
            ),
            Self::ChunkTooLarge { max, actual } => write!(
                formatter,
                "logical snapshot chunk has {actual} bytes; maximum is {max}"
            ),
            Self::EntriesNotStrictlyOrdered => {
                formatter.write_str("logical snapshot entries are not in strict key order")
            }
            Self::SnapshotIdMismatch { expected, actual } => write!(
                formatter,
                "logical snapshot ID {actual} differs from expected ID {expected}"
            ),
            Self::ChunkOrdinalMismatch { expected, actual } => write!(
                formatter,
                "logical snapshot chunk ordinal {actual} differs from expected {expected}"
            ),
            Self::ChunkDigestMismatch { ordinal } => {
                write!(
                    formatter,
                    "logical snapshot chunk {ordinal} digest mismatch"
                )
            }
            Self::UnsupportedFormatVersion { expected, actual } => write!(
                formatter,
                "logical snapshot format {actual} differs from supported version {expected}"
            ),
            Self::CountOverflow => formatter.write_str("logical snapshot count overflow"),
            Self::ManifestMismatch => formatter.write_str("logical snapshot manifest mismatch"),
            Self::ExportNotExhausted => {
                formatter.write_str("logical snapshot export was not fully consumed")
            }
            Self::EntryTooLarge { max, actual } => write!(
                formatter,
                "logical snapshot entry has {actual} bytes; chunk maximum is {max}"
            ),
        }
    }
}

impl Error for LogicalSnapshotError {}

fn validate_snapshot_entries(entries: &[KeyValue]) -> Result<(), LogicalSnapshotError> {
    if entries.is_empty() {
        return Err(LogicalSnapshotError::EmptyChunk);
    }
    if entries.len() > MAX_LOGICAL_SNAPSHOT_CHUNK_ENTRIES {
        return Err(LogicalSnapshotError::TooManyChunkEntries {
            max: MAX_LOGICAL_SNAPSHOT_CHUNK_ENTRIES,
            actual: entries.len(),
        });
    }
    if entries
        .windows(2)
        .any(|pair| pair[0].key() >= pair[1].key())
    {
        return Err(LogicalSnapshotError::EntriesNotStrictlyOrdered);
    }
    let bytes = entries.iter().fold(0_usize, |total, entry| {
        total
            .saturating_add(1)
            .saturating_add(8)
            .saturating_add(entry.key().as_bytes().len())
            .saturating_add(8)
            .saturating_add(entry.value().len())
    });
    if bytes > MAX_LOGICAL_SNAPSHOT_CHUNK_BYTES {
        return Err(LogicalSnapshotError::ChunkTooLarge {
            max: MAX_LOGICAL_SNAPSHOT_CHUNK_BYTES,
            actual: bytes,
        });
    }
    Ok(())
}

fn hash_snapshot_chunk(snapshot_id: u128, ordinal: u64, entries: &[KeyValue]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"DTGProxy/LogicalSnapshotChunk/V1");
    hasher.update(&snapshot_id.to_be_bytes());
    hasher.update(&ordinal.to_be_bytes());
    for entry in entries {
        hash_snapshot_entry(&mut hasher, entry);
    }
    *hasher.finalize().as_bytes()
}

fn hash_snapshot_entry(hasher: &mut blake3::Hasher, entry: &KeyValue) {
    hasher.update(&[entry.key().keyspace().tag()]);
    hasher.update(
        &u64::try_from(entry.key().as_bytes().len())
            .unwrap_or(u64::MAX)
            .to_be_bytes(),
    );
    hasher.update(entry.key().as_bytes());
    hasher.update(
        &u64::try_from(entry.value().len())
            .unwrap_or(u64::MAX)
            .to_be_bytes(),
    );
    hasher.update(entry.value());
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MutationOperation {
    Put { key: LogicalKey, value: Vec<u8> },
    Delete { key: LogicalKey },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Mutation {
    pub sequence: u32,
    pub operation: MutationOperation,
}

impl Mutation {
    #[must_use]
    pub const fn put(sequence: u32, key: LogicalKey, value: Vec<u8>) -> Self {
        Self {
            sequence,
            operation: MutationOperation::Put { key, value },
        }
    }

    #[must_use]
    pub const fn delete(sequence: u32, key: LogicalKey) -> Self {
        Self {
            sequence,
            operation: MutationOperation::Delete { key },
        }
    }

    #[must_use]
    pub fn fingerprint(&self) -> u64 {
        let mut fingerprint = Fnv1a::new();
        fingerprint.write(&self.sequence.to_be_bytes());
        match &self.operation {
            MutationOperation::Put { key, value } => {
                fingerprint.write(&[1, key.keyspace().tag()]);
                fingerprint.write_length_delimited(key.as_bytes());
                fingerprint.write_length_delimited(value);
            }
            MutationOperation::Delete { key } => {
                fingerprint.write(&[2, key.keyspace().tag()]);
                fingerprint.write_length_delimited(key.as_bytes());
            }
        }
        fingerprint.finish()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedMutationBatch {
    pub shard_id: u32,
    pub txn_id: u128,
    pub mutations: Vec<Mutation>,
}

impl PreparedMutationBatch {
    #[must_use]
    pub fn fingerprint(&self) -> u64 {
        let mut fingerprint = Fnv1a::new();
        fingerprint.write(&self.shard_id.to_be_bytes());
        fingerprint.write(&self.txn_id.to_be_bytes());
        fingerprint.write_len(self.mutations.len());
        for mutation in &self.mutations {
            fingerprint.write(&mutation.fingerprint().to_be_bytes());
        }
        fingerprint.finish()
    }

    #[must_use]
    pub fn commit_at(self, log_index: u64) -> CommittedMutationBatch {
        CommittedMutationBatch {
            shard_id: self.shard_id,
            log_index,
            txn_id: self.txn_id,
            mutations: self.mutations,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommittedMutationBatch {
    pub shard_id: u32,
    pub log_index: u64,
    pub txn_id: u128,
    pub mutations: Vec<Mutation>,
}

impl CommittedMutationBatch {
    #[must_use]
    pub fn fingerprint(&self) -> u64 {
        let mut fingerprint = Fnv1a::new();
        fingerprint.write(&self.shard_id.to_be_bytes());
        fingerprint.write(&self.log_index.to_be_bytes());
        fingerprint.write(&self.txn_id.to_be_bytes());
        fingerprint.write_len(self.mutations.len());
        for mutation in &self.mutations {
            fingerprint.write(&mutation.fingerprint().to_be_bytes());
        }
        fingerprint.finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ApplyReceipt {
    pub applied_log_index: u64,
    pub duplicate: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdapterError {
    NonContiguousLogIndex { expected: u64, actual: u64 },
    CommittedLogReplayMismatch { log_index: u64 },
    DuplicateMutationSequence { txn_id: u128, sequence: u32 },
    MutationReplayMismatch { txn_id: u128, sequence: u32 },
    UnsupportedOperation { operation: &'static str },
    LogicalSnapshot(LogicalSnapshotError),
    ScanByteLimit { limit: u64, required: u64 },
    ScanResponseByteLimit { limit: u64, required: u64 },
    Mapping(MappingCompatibilityError),
    Unavailable(String),
    Backend(String),
    LockPoisoned,
}

impl Display for AdapterError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::NonContiguousLogIndex { expected, actual } => {
                write!(
                    formatter,
                    "non-contiguous log index: expected {expected}, got {actual}"
                )
            }
            Self::CommittedLogReplayMismatch { log_index } => {
                write!(
                    formatter,
                    "committed log replay differs at index {log_index}"
                )
            }
            Self::DuplicateMutationSequence { txn_id, sequence } => {
                write!(
                    formatter,
                    "transaction {txn_id} repeats mutation sequence {sequence} in one batch"
                )
            }
            Self::MutationReplayMismatch { txn_id, sequence } => {
                write!(
                    formatter,
                    "transaction {txn_id} mutation sequence {sequence} changed during replay"
                )
            }
            Self::UnsupportedOperation { operation } => {
                write!(formatter, "Adapter does not support {operation}")
            }
            Self::LogicalSnapshot(error) => Display::fmt(error, formatter),
            Self::ScanByteLimit { limit, required } => {
                write!(
                    formatter,
                    "scan requires {required} bytes above limit {limit}"
                )
            }
            Self::ScanResponseByteLimit { limit, required } => write!(
                formatter,
                "scan response body requires {required} wire bytes above limit {limit}"
            ),
            Self::Mapping(error) => Display::fmt(error, formatter),
            Self::Unavailable(message) => {
                write!(formatter, "storage backend unavailable: {message}")
            }
            Self::Backend(message) => write!(formatter, "storage backend error: {message}"),
            Self::LockPoisoned => formatter.write_str("adapter state lock is poisoned"),
        }
    }
}

impl Error for AdapterError {}

impl From<LogicalSnapshotError> for AdapterError {
    fn from(error: LogicalSnapshotError) -> Self {
        Self::LogicalSnapshot(error)
    }
}

impl From<MappingCompatibilityError> for AdapterError {
    fn from(error: MappingCompatibilityError) -> Self {
        Self::Mapping(error)
    }
}

pub fn charge_scan_entry(
    span: &KeySpan,
    retained: u64,
    key: &[u8],
    value: &[u8],
) -> Result<u64, AdapterError> {
    let required = retained
        .checked_add(
            u64::try_from(key.len()).map_err(|_| AdapterError::ScanByteLimit {
                limit: span.max_bytes().unwrap_or(u64::MAX),
                required: u64::MAX,
            })?,
        )
        .and_then(|required| required.checked_add(u64::try_from(value.len()).ok()?))
        .ok_or(AdapterError::ScanByteLimit {
            limit: span.max_bytes().unwrap_or(u64::MAX),
            required: u64::MAX,
        })?;
    if span.max_bytes().is_some_and(|limit| required > limit) {
        return Err(AdapterError::ScanByteLimit {
            limit: span.max_bytes().expect("checked byte limit"),
            required,
        });
    }
    Ok(required)
}

pub type AdapterFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, AdapterError>> + Send + 'a>>;

pub trait LogicalSnapshotReader: Send {
    fn header(&self) -> &LogicalSnapshotHeaderV1;

    fn next_chunk<'a>(&'a mut self) -> AdapterFuture<'a, Option<LogicalSnapshotChunkV1>>;

    fn finish<'a>(self: Box<Self>) -> AdapterFuture<'a, LogicalSnapshotManifestV1>
    where
        Self: 'a;
}

pub trait StorageAdapter: Send + Sync {
    fn descriptor(&self) -> AdapterDescriptorV1 {
        AdapterDescriptorV1::new(
            "test-adapter",
            "unversioned",
            BackendFamily::Test,
            self.capabilities(),
        )
    }

    fn capabilities(&self) -> AdapterCapabilities;

    fn mapping_descriptor(&self) -> Option<MappingDescriptorV1> {
        None
    }

    fn apply_committed<'a>(
        &'a self,
        batch: CommittedMutationBatch,
    ) -> AdapterFuture<'a, ApplyReceipt>;

    fn multi_get<'a>(&'a self, keys: &'a [LogicalKey]) -> AdapterFuture<'a, Vec<Option<Vec<u8>>>>;

    fn scan<'a>(&'a self, span: &'a KeySpan) -> AdapterFuture<'a, Vec<KeyValue>>;

    fn begin_logical_export<'a>(
        &'a self,
        _request: LogicalSnapshotExportRequest,
    ) -> AdapterFuture<'a, Box<dyn LogicalSnapshotReader + 'a>> {
        Box::pin(async move {
            Err(AdapterError::UnsupportedOperation {
                operation: "logical snapshot export",
            })
        })
    }

    fn create_physical_checkpoint(&self, _destination: &Path) -> Result<(), AdapterError> {
        Err(AdapterError::UnsupportedOperation {
            operation: "physical checkpoint",
        })
    }

    fn applied_log_index(&self) -> Result<u64, AdapterError>;
}

impl<T> StorageAdapter for &T
where
    T: StorageAdapter + ?Sized,
{
    fn descriptor(&self) -> AdapterDescriptorV1 {
        (**self).descriptor()
    }

    fn capabilities(&self) -> AdapterCapabilities {
        (**self).capabilities()
    }

    fn mapping_descriptor(&self) -> Option<MappingDescriptorV1> {
        (**self).mapping_descriptor()
    }

    fn apply_committed<'a>(
        &'a self,
        batch: CommittedMutationBatch,
    ) -> AdapterFuture<'a, ApplyReceipt> {
        (**self).apply_committed(batch)
    }

    fn multi_get<'a>(&'a self, keys: &'a [LogicalKey]) -> AdapterFuture<'a, Vec<Option<Vec<u8>>>> {
        (**self).multi_get(keys)
    }

    fn scan<'a>(&'a self, span: &'a KeySpan) -> AdapterFuture<'a, Vec<KeyValue>> {
        (**self).scan(span)
    }

    fn create_physical_checkpoint(&self, destination: &Path) -> Result<(), AdapterError> {
        (**self).create_physical_checkpoint(destination)
    }

    fn applied_log_index(&self) -> Result<u64, AdapterError> {
        (**self).applied_log_index()
    }
}

impl<T> StorageAdapter for Arc<T>
where
    T: StorageAdapter + ?Sized,
{
    fn descriptor(&self) -> AdapterDescriptorV1 {
        (**self).descriptor()
    }

    fn capabilities(&self) -> AdapterCapabilities {
        (**self).capabilities()
    }

    fn mapping_descriptor(&self) -> Option<MappingDescriptorV1> {
        (**self).mapping_descriptor()
    }

    fn apply_committed<'a>(
        &'a self,
        batch: CommittedMutationBatch,
    ) -> AdapterFuture<'a, ApplyReceipt> {
        (**self).apply_committed(batch)
    }

    fn multi_get<'a>(&'a self, keys: &'a [LogicalKey]) -> AdapterFuture<'a, Vec<Option<Vec<u8>>>> {
        (**self).multi_get(keys)
    }

    fn scan<'a>(&'a self, span: &'a KeySpan) -> AdapterFuture<'a, Vec<KeyValue>> {
        (**self).scan(span)
    }

    fn begin_logical_export<'a>(
        &'a self,
        request: LogicalSnapshotExportRequest,
    ) -> AdapterFuture<'a, Box<dyn LogicalSnapshotReader + 'a>> {
        (**self).begin_logical_export(request)
    }

    fn create_physical_checkpoint(&self, destination: &Path) -> Result<(), AdapterError> {
        (**self).create_physical_checkpoint(destination)
    }

    fn applied_log_index(&self) -> Result<u64, AdapterError> {
        (**self).applied_log_index()
    }
}

fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    for index in (0..end.len()).rev() {
        if end[index] != u8::MAX {
            end[index] += 1;
            end.truncate(index + 1);
            return Some(end);
        }
    }
    None
}

struct Fnv1a(u64);

impl Fnv1a {
    const OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;

    const fn new() -> Self {
        Self(Self::OFFSET_BASIS)
    }

    fn write(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 ^= u64::from(*byte);
            self.0 = self.0.wrapping_mul(Self::PRIME);
        }
    }

    fn write_len(&mut self, len: usize) {
        let len = u64::try_from(len).expect("usize always fits in u64 on supported targets");
        self.write(&len.to_be_bytes());
    }

    fn write_length_delimited(&mut self, bytes: &[u8]) {
        self.write_len(bytes.len());
        self.write(bytes);
    }

    const fn finish(&self) -> u64 {
        self.0
    }
}
