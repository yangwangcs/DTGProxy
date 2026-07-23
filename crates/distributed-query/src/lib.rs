#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{self, Display, Formatter};

use temporal_types::TransactionTime;

mod coordinator;
mod worker;

pub use coordinator::DistributedCoordinator;
pub use worker::{
    FragmentWorker, LocalFragmentWorker, TemporalWorkerBatch, TemporalWorkerFuture, WorkerBatch,
    WorkerFuture,
};

pub const DISTRIBUTED_QUERY_PROTOCOL_VERSION: u16 = 1;

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
        })
    }

    pub fn with_expected_shards(
        mut self,
        expected_shards: Vec<u32>,
    ) -> Result<Self, DistributedQueryError> {
        let unique = expected_shards.iter().copied().collect::<BTreeSet<_>>();
        if expected_shards.is_empty() || unique.len() != expected_shards.len() {
            return Err(DistributedQueryError::InvalidRequest);
        }
        self.expected_shards = expected_shards;
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
}

impl Display for DistributedQueryError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "distributed query protocol error: {self:?}")
    }
}

impl Error for DistributedQueryError {}
