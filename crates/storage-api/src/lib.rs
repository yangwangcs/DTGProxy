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
pub struct KeySpan {
    keyspace: Keyspace,
    start: Vec<u8>,
    end: Option<Vec<u8>>,
    required_prefix: Option<Vec<u8>>,
    limit: Option<usize>,
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
        })
    }

    pub fn with_limit(mut self, limit: usize) -> Result<Self, KeySpanError> {
        if limit == 0 {
            return Err(KeySpanError::ZeroLimit);
        }
        self.limit = Some(limit);
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
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KeySpanError {
    EmptyOrReversed,
    StartOutsidePrefix,
    ZeroLimit,
}

impl Display for KeySpanError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyOrReversed => formatter.write_str("key span end must follow its start"),
            Self::StartOutsidePrefix => {
                formatter.write_str("key span seek start is outside the required prefix")
            }
            Self::ZeroLimit => formatter.write_str("key span limit must be positive"),
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

    fn scan<'a>(&'a self, span: &'a KeySpan) -> AdapterFuture<'a, Vec<KeyValue>>;

    fn applied_log_index(&self) -> Result<u64, AdapterError>;
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
