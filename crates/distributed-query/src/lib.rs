#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{self, Display, Formatter};
#[cfg(any(test, feature = "test-support"))]
use std::ops::Deref;
#[cfg(any(test, feature = "test-support"))]
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(any(test, feature = "test-support"))]
use std::sync::{Arc, Mutex, OnceLock};

use temporal_types::TransactionTime;

mod coordinator;
mod exchange_codec;
mod worker;

pub use coordinator::{ChangePlanRequest, DistributedCapabilitySnapshot, DistributedCoordinator};
pub use exchange_codec::{
    DecodedExchangeBatch, ExchangeCodecLimits, ExchangeDecodeExpectation, ExchangeFrame,
    MAX_EXCHANGE_PAYLOAD_BYTES, schema_fingerprint,
};
#[cfg(any(test, feature = "test-support"))]
pub use test_support::{
    ExchangeTestMetrics, ExchangeTestMetricsGuard, current_exchange_test_metrics,
    install_exchange_test_metrics,
};
pub use worker::{
    FragmentWorker, LocalFragmentWorker, TemporalWorkerBatch, TemporalWorkerFuture, WorkerBatch,
    WorkerFuture, WorkerMorselFuture, WorkerMorselOpenFuture, WorkerMorselSource,
};

pub const DISTRIBUTED_QUERY_PROTOCOL_VERSION: u16 = 1;

#[cfg(any(test, feature = "test-support"))]
mod test_support {
    use super::*;

    #[derive(Debug, Default)]
    pub struct ExchangeTestMetrics {
        encoded_frames: AtomicU64,
        decoded_frames: AtomicU64,
        retained_frame_bytes: AtomicU64,
        peak_retained_frame_bytes: AtomicU64,
        retained_decoded_bytes: AtomicU64,
        peak_retained_decoded_bytes: AtomicU64,
    }

    impl ExchangeTestMetrics {
        #[must_use]
        pub fn encoded_frames(&self) -> u64 {
            self.encoded_frames.load(Ordering::Relaxed)
        }

        #[must_use]
        pub fn decoded_frames(&self) -> u64 {
            self.decoded_frames.load(Ordering::Relaxed)
        }

        #[must_use]
        pub fn retained_frame_bytes(&self) -> u64 {
            self.retained_frame_bytes.load(Ordering::Relaxed)
        }

        #[must_use]
        pub fn peak_retained_frame_bytes(&self) -> u64 {
            self.peak_retained_frame_bytes.load(Ordering::Relaxed)
        }

        #[must_use]
        pub fn retained_decoded_bytes(&self) -> u64 {
            self.retained_decoded_bytes.load(Ordering::Relaxed)
        }

        #[must_use]
        pub fn peak_retained_decoded_bytes(&self) -> u64 {
            self.peak_retained_decoded_bytes.load(Ordering::Relaxed)
        }

        pub fn record_encoded_frame(&self) {
            self.encoded_frames.fetch_add(1, Ordering::Relaxed);
        }

        pub fn record_decoded_frame(&self) {
            self.decoded_frames.fetch_add(1, Ordering::Relaxed);
        }

        pub fn reserve_frame(&self, bytes: u64) {
            let retained = self
                .retained_frame_bytes
                .fetch_add(bytes, Ordering::Relaxed)
                .saturating_add(bytes);
            self.peak_retained_frame_bytes
                .fetch_max(retained, Ordering::Relaxed);
        }

        pub fn release_frame(&self, bytes: u64) {
            self.retained_frame_bytes
                .fetch_sub(bytes, Ordering::Relaxed);
        }

        pub fn reserve_decoded(&self, bytes: u64) {
            let retained = self
                .retained_decoded_bytes
                .fetch_add(bytes, Ordering::Relaxed)
                .saturating_add(bytes);
            self.peak_retained_decoded_bytes
                .fetch_max(retained, Ordering::Relaxed);
        }

        pub fn release_decoded(&self, bytes: u64) {
            self.retained_decoded_bytes
                .fetch_sub(bytes, Ordering::Relaxed);
        }
    }

    #[derive(Debug)]
    pub struct ExchangeTestMetricsGuard {
        metrics: Arc<ExchangeTestMetrics>,
        prior: Option<Arc<ExchangeTestMetrics>>,
    }

    impl ExchangeTestMetricsGuard {
        #[must_use]
        pub fn metrics(&self) -> Arc<ExchangeTestMetrics> {
            Arc::clone(&self.metrics)
        }
    }

    impl Deref for ExchangeTestMetricsGuard {
        type Target = ExchangeTestMetrics;

        fn deref(&self) -> &Self::Target {
            &self.metrics
        }
    }

    impl Drop for ExchangeTestMetricsGuard {
        fn drop(&mut self) {
            let mut slot = metrics_slot()
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            *slot = self.prior.take();
        }
    }

    pub fn install_exchange_test_metrics() -> ExchangeTestMetricsGuard {
        let metrics = Arc::new(ExchangeTestMetrics::default());
        let prior = {
            let mut slot = metrics_slot()
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            slot.replace(Arc::clone(&metrics))
        };
        ExchangeTestMetricsGuard { metrics, prior }
    }

    #[must_use]
    pub fn current_exchange_test_metrics() -> Option<Arc<ExchangeTestMetrics>> {
        metrics_slot()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    fn metrics_slot() -> &'static Mutex<Option<Arc<ExchangeTestMetrics>>> {
        static METRICS: OnceLock<Mutex<Option<Arc<ExchangeTestMetrics>>>> = OnceLock::new();
        METRICS.get_or_init(|| Mutex::new(None))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotToken {
    graph_id: u64,
    schema_version: u64,
    topology_epoch: u64,
    transaction_time: TransactionTime,
    security_fingerprint: [u8; 32],
    fingerprint: [u8; 32],
}

impl SnapshotToken {
    pub fn new(
        graph_id: u64,
        schema_version: u64,
        topology_epoch: u64,
        transaction_time: TransactionTime,
        security_fingerprint: [u8; 32],
    ) -> Result<Self, DistributedQueryError> {
        if graph_id == 0
            || schema_version == 0
            || topology_epoch == 0
            || security_fingerprint == [0; 32]
        {
            return Err(DistributedQueryError::InvalidSnapshot);
        }
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"DTGProxy/DistributedSnapshot/Latest");
        hasher.update(&graph_id.to_be_bytes());
        hasher.update(&schema_version.to_be_bytes());
        hasher.update(&topology_epoch.to_be_bytes());
        hasher.update(&transaction_time.physical_micros().to_be_bytes());
        hasher.update(&transaction_time.logical().to_be_bytes());
        hasher.update(&security_fingerprint);
        let fingerprint = *hasher.finalize().as_bytes();
        Ok(Self {
            graph_id,
            schema_version,
            topology_epoch,
            transaction_time,
            security_fingerprint,
            fingerprint,
        })
    }

    #[must_use]
    pub const fn graph_id(&self) -> u64 {
        self.graph_id
    }

    #[must_use]
    pub const fn schema_version(&self) -> u64 {
        self.schema_version
    }

    #[must_use]
    pub const fn topology_epoch(&self) -> u64 {
        self.topology_epoch
    }

    #[must_use]
    pub const fn transaction_time(&self) -> TransactionTime {
        self.transaction_time
    }

    #[must_use]
    pub const fn security_fingerprint(&self) -> [u8; 32] {
        self.security_fingerprint
    }

    #[must_use]
    pub const fn fingerprint(&self) -> [u8; 32] {
        self.fingerprint
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FragmentRequest {
    protocol_version: u16,
    fragment_id: physical_plan::FragmentId,
    snapshot: SnapshotToken,
    deadline_unix_ms: u64,
    memory_bytes: u64,
    batch_rows: u32,
    expected_shards: Vec<u32>,
    required_applied_indexes: BTreeMap<u32, u64>,
    expected_capability_generations: BTreeMap<u32, u64>,
}

impl FragmentRequest {
    pub fn new(
        fragment_id: physical_plan::FragmentId,
        snapshot: SnapshotToken,
        deadline_unix_ms: u64,
        memory_bytes: u64,
        batch_rows: u32,
    ) -> Result<Self, DistributedQueryError> {
        if deadline_unix_ms == 0 || memory_bytes == 0 || batch_rows == 0 {
            return Err(DistributedQueryError::InvalidRequest);
        }
        Ok(Self {
            protocol_version: DISTRIBUTED_QUERY_PROTOCOL_VERSION,
            fragment_id,
            snapshot,
            deadline_unix_ms,
            memory_bytes,
            batch_rows,
            expected_shards: Vec::new(),
            required_applied_indexes: BTreeMap::new(),
            expected_capability_generations: BTreeMap::new(),
        })
    }

    pub fn with_expected_shards(
        mut self,
        expected_shards: Vec<u32>,
    ) -> Result<Self, DistributedQueryError> {
        let unique = expected_shards.iter().copied().collect::<BTreeSet<_>>();
        let required_shards = self
            .required_applied_indexes
            .keys()
            .copied()
            .collect::<BTreeSet<_>>();
        let capability_shards = self
            .expected_capability_generations
            .keys()
            .copied()
            .collect::<BTreeSet<_>>();
        if expected_shards.is_empty()
            || unique.len() != expected_shards.len()
            || (!required_shards.is_empty() && required_shards != unique)
            || (!capability_shards.is_empty() && capability_shards != unique)
        {
            return Err(DistributedQueryError::InvalidRequest);
        }
        self.expected_shards = expected_shards;
        Ok(self)
    }

    pub fn with_required_applied_indexes(
        mut self,
        required_applied_indexes: BTreeMap<u32, u64>,
    ) -> Result<Self, DistributedQueryError> {
        let required_shards = required_applied_indexes
            .keys()
            .copied()
            .collect::<BTreeSet<_>>();
        let expected_shards = self
            .expected_shards
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        if required_applied_indexes.is_empty()
            || required_applied_indexes.values().any(|index| *index == 0)
            || (!expected_shards.is_empty() && required_shards != expected_shards)
        {
            return Err(DistributedQueryError::InvalidRequest);
        }
        self.required_applied_indexes = required_applied_indexes;
        Ok(self)
    }

    pub fn with_expected_capability_generations(
        mut self,
        expected_capability_generations: BTreeMap<u32, u64>,
    ) -> Result<Self, DistributedQueryError> {
        let capability_shards = expected_capability_generations
            .keys()
            .copied()
            .collect::<BTreeSet<_>>();
        let expected_shards = self
            .expected_shards
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        if expected_capability_generations.is_empty()
            || expected_capability_generations
                .values()
                .any(|generation| *generation == 0)
            || (!expected_shards.is_empty() && capability_shards != expected_shards)
        {
            return Err(DistributedQueryError::InvalidRequest);
        }
        self.expected_capability_generations = expected_capability_generations;
        Ok(self)
    }

    #[must_use]
    pub const fn fragment_id(&self) -> physical_plan::FragmentId {
        self.fragment_id
    }

    #[must_use]
    pub const fn snapshot(&self) -> &SnapshotToken {
        &self.snapshot
    }

    #[must_use]
    pub const fn deadline_unix_ms(&self) -> u64 {
        self.deadline_unix_ms
    }

    #[must_use]
    pub const fn memory_bytes(&self) -> u64 {
        self.memory_bytes
    }

    #[must_use]
    pub const fn batch_rows(&self) -> u32 {
        self.batch_rows
    }

    #[must_use]
    pub fn expected_shards(&self) -> &[u32] {
        &self.expected_shards
    }

    #[must_use]
    pub fn required_applied_indexes(&self) -> &BTreeMap<u32, u64> {
        &self.required_applied_indexes
    }

    #[must_use]
    pub fn required_applied_index(&self, shard_id: u32) -> Option<u64> {
        self.required_applied_indexes.get(&shard_id).copied()
    }

    #[must_use]
    pub fn expected_capability_generations(&self) -> &BTreeMap<u32, u64> {
        &self.expected_capability_generations
    }

    #[must_use]
    pub fn expected_capability_generation(&self, shard_id: u32) -> Option<u64> {
        self.expected_capability_generations.get(&shard_id).copied()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BatchEnvelope {
    shard_id: u32,
    sequence: u64,
    has_more: bool,
    snapshot_fingerprint: [u8; 32],
    payload: Vec<u8>,
}

impl BatchEnvelope {
    #[must_use]
    pub fn new(
        shard_id: u32,
        sequence: u64,
        has_more: bool,
        snapshot: &SnapshotToken,
        payload: Vec<u8>,
    ) -> Self {
        Self {
            shard_id,
            sequence,
            has_more,
            snapshot_fingerprint: snapshot.fingerprint(),
            payload,
        }
    }
}

struct ShardStream {
    next_sequence: u64,
    complete: bool,
    payloads: Vec<Vec<u8>>,
}

pub struct BatchMerger {
    snapshot_fingerprint: [u8; 32],
    max_payload_bytes: usize,
    payload_bytes: usize,
    streams: BTreeMap<u32, ShardStream>,
}

impl BatchMerger {
    pub fn new(
        expected_shards: Vec<u32>,
        snapshot: SnapshotToken,
        max_payload_bytes: usize,
    ) -> Result<Self, DistributedQueryError> {
        if expected_shards.is_empty() || max_payload_bytes == 0 {
            return Err(DistributedQueryError::InvalidMerger);
        }
        let unique = expected_shards.iter().copied().collect::<BTreeSet<_>>();
        if unique.len() != expected_shards.len() {
            return Err(DistributedQueryError::InvalidMerger);
        }
        Ok(Self {
            snapshot_fingerprint: snapshot.fingerprint(),
            max_payload_bytes,
            payload_bytes: 0,
            streams: unique
                .into_iter()
                .map(|shard_id| {
                    (
                        shard_id,
                        ShardStream {
                            next_sequence: 0,
                            complete: false,
                            payloads: Vec::new(),
                        },
                    )
                })
                .collect(),
        })
    }

    pub fn push(&mut self, batch: BatchEnvelope) -> Result<(), DistributedQueryError> {
        if batch.snapshot_fingerprint != self.snapshot_fingerprint {
            return Err(DistributedQueryError::SnapshotMismatch);
        }
        let stream = self
            .streams
            .get_mut(&batch.shard_id)
            .ok_or(DistributedQueryError::UnexpectedShard(batch.shard_id))?;
        if stream.complete || batch.sequence != stream.next_sequence {
            return Err(DistributedQueryError::UnexpectedSequence {
                shard_id: batch.shard_id,
                expected: stream.next_sequence,
                actual: batch.sequence,
            });
        }
        let next_bytes = self
            .payload_bytes
            .checked_add(batch.payload.len())
            .ok_or(DistributedQueryError::PayloadLimit)?;
        if next_bytes > self.max_payload_bytes {
            return Err(DistributedQueryError::PayloadLimit);
        }
        self.payload_bytes = next_bytes;
        stream.next_sequence = stream
            .next_sequence
            .checked_add(1)
            .ok_or(DistributedQueryError::SequenceExhausted)?;
        stream.complete = !batch.has_more;
        stream.payloads.push(batch.payload);
        Ok(())
    }

    pub fn finish(self) -> Result<Vec<Vec<u8>>, DistributedQueryError> {
        let incomplete = self
            .streams
            .iter()
            .filter_map(|(shard_id, stream)| (!stream.complete).then_some(*shard_id))
            .collect::<Vec<_>>();
        if !incomplete.is_empty() {
            return Err(DistributedQueryError::IncompleteShards(incomplete));
        }
        Ok(self
            .streams
            .into_values()
            .flat_map(|stream| stream.payloads)
            .collect())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DistributedQueryError {
    InvalidSnapshot,
    InvalidRequest,
    InvalidMerger,
    SnapshotMismatch,
    CapabilityGenerationMismatch,
    UnexpectedShard(u32),
    UnexpectedSequence {
        shard_id: u32,
        expected: u64,
        actual: u64,
    },
    PayloadLimit,
    SequenceExhausted,
    IncompleteShards(Vec<u32>),
    MissingShards(Vec<u32>),
    SecurityMismatch,
    Cancelled,
    MemoryLimitExceeded {
        limit: u64,
        required: u64,
    },
    ApplyInvocationLimit {
        max: u64,
    },
    ApplyOutputRowLimit {
        max: u64,
    },
    RecursivePlanViolation,
    StorageFailure,
    WorkerIdentityMismatch,
    FragmentMismatch,
    DeadlineExceeded,
    Execution(String),
    InvalidCoordinator,
    DuplicateWorker(u32),
    CreditExhausted,
    UnsupportedExchange,
    UnsupportedChangeScan,
    MissingRequiredAppliedIndex(u32),
    StaleReadIndex {
        shard_id: u32,
        required: u64,
        actual: u64,
    },
    ExchangeVersionMismatch(u16),
    ExchangeChecksumMismatch,
    ExchangeSchemaMismatch,
    ExchangeMetadataMismatch,
    MalformedExchange,
    UnsupportedExchangeValue,
}

impl Display for DistributedQueryError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "distributed query protocol error: {self:?}")
    }
}

impl Error for DistributedQueryError {}
