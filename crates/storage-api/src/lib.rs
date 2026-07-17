#![forbid(unsafe_code)]

use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::future::Future;
use std::pin::Pin;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdapterCapabilities {
    pub local_atomic_batch: bool,
    pub idempotent_apply: bool,
}

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
            Self::Backend(message) => write!(formatter, "storage backend error: {message}"),
            Self::LockPoisoned => formatter.write_str("adapter state lock is poisoned"),
        }
    }
}

impl Error for AdapterError {}

pub type AdapterFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, AdapterError>> + Send + 'a>>;

pub trait StorageAdapter: Send + Sync {
    fn capabilities(&self) -> AdapterCapabilities;

    fn apply_committed<'a>(
        &'a self,
        batch: CommittedMutationBatch,
    ) -> AdapterFuture<'a, ApplyReceipt>;

    fn multi_get<'a>(&'a self, keys: &'a [LogicalKey]) -> AdapterFuture<'a, Vec<Option<Vec<u8>>>>;

    fn applied_log_index(&self) -> Result<u64, AdapterError>;
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
