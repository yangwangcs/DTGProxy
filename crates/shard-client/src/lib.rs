#![forbid(unsafe_code)]

mod embedded;
mod remote;
mod storage_adapter;

pub use embedded::EmbeddedShardClient;
pub use remote::{RemoteReplica, RemoteShardClient, RemoteTopology, RemoteTopologyError};
pub use storage_adapter::ShardClientStorageAdapter;

use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::future::Future;
use std::pin::Pin;

use storage_api::{KeySpan, KeyValue, LogicalKey};
use temporal_types::TransactionTime;

pub type ShardClientFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, ShardClientError>> + Send + 'a>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShardRequestContext {
    graph_id: u64,
    shard_id: u32,
    placement_epoch: u64,
    request_id: u128,
    deadline_unix_ms: u64,
}

impl ShardRequestContext {
    pub fn new(
        graph_id: u64,
        shard_id: u32,
        placement_epoch: u64,
        request_id: u128,
        deadline_unix_ms: u64,
    ) -> Result<Self, ShardClientError> {
        if graph_id == 0
            || shard_id == 0
            || placement_epoch == 0
            || request_id == 0
            || deadline_unix_ms == 0
        {
            return Err(ShardClientError::InvalidContext);
        }
        Ok(Self {
            graph_id,
            shard_id,
            placement_epoch,
            request_id,
            deadline_unix_ms,
        })
    }

    #[must_use]
    pub const fn graph_id(self) -> u64 {
        self.graph_id
    }

    #[must_use]
    pub const fn shard_id(self) -> u32 {
        self.shard_id
    }

    #[must_use]
    pub const fn placement_epoch(self) -> u64 {
        self.placement_epoch
    }

    #[must_use]
    pub const fn request_id(self) -> u128 {
        self.request_id
    }

    #[must_use]
    pub const fn deadline_unix_ms(self) -> u64 {
        self.deadline_unix_ms
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecuteCommand {
    context: ShardRequestContext,
    command: Vec<u8>,
}

impl ExecuteCommand {
    pub fn new(context: ShardRequestContext, command: Vec<u8>) -> Result<Self, ShardClientError> {
        if command.is_empty() {
            return Err(ShardClientError::EmptyCommand);
        }
        Ok(Self { context, command })
    }

    #[must_use]
    pub const fn context(&self) -> ShardRequestContext {
        self.context
    }

    #[must_use]
    pub fn command(&self) -> &[u8] {
        &self.command
    }

    #[must_use]
    pub fn into_command(self) -> Vec<u8> {
        self.command
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExecuteReceipt {
    raft_index: u64,
    duplicate: bool,
}

impl ExecuteReceipt {
    #[must_use]
    pub const fn new(raft_index: u64, duplicate: bool) -> Self {
        Self {
            raft_index,
            duplicate,
        }
    }

    #[must_use]
    pub const fn raft_index(self) -> u64 {
        self.raft_index
    }

    #[must_use]
    pub const fn duplicate(self) -> bool {
        self.duplicate
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadKeysRequest {
    context: ShardRequestContext,
    keys: Vec<LogicalKey>,
}

impl ReadKeysRequest {
    pub fn new(
        context: ShardRequestContext,
        keys: Vec<LogicalKey>,
    ) -> Result<Self, ShardClientError> {
        if keys.is_empty() {
            return Err(ShardClientError::EmptyRead);
        }
        Ok(Self { context, keys })
    }

    #[must_use]
    pub const fn context(&self) -> ShardRequestContext {
        self.context
    }

    #[must_use]
    pub fn keys(&self) -> &[LogicalKey] {
        &self.keys
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScanRequest {
    context: ShardRequestContext,
    span: KeySpan,
}

impl ScanRequest {
    #[must_use]
    pub const fn new(context: ShardRequestContext, span: KeySpan) -> Self {
        Self { context, span }
    }

    #[must_use]
    pub const fn context(&self) -> ShardRequestContext {
        self.context
    }

    #[must_use]
    pub const fn span(&self) -> &KeySpan {
        &self.span
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShardStatus {
    node_id: u64,
    leader_id: u64,
    term: u64,
    applied_index: u64,
    closed_timestamp: TransactionTime,
}

impl ShardStatus {
    #[must_use]
    pub const fn new(
        node_id: u64,
        leader_id: u64,
        term: u64,
        applied_index: u64,
        closed_timestamp: TransactionTime,
    ) -> Self {
        Self {
            node_id,
            leader_id,
            term,
            applied_index,
            closed_timestamp,
        }
    }

    #[must_use]
    pub const fn node_id(self) -> u64 {
        self.node_id
    }

    #[must_use]
    pub const fn leader_id(self) -> u64 {
        self.leader_id
    }

    #[must_use]
    pub const fn term(self) -> u64 {
        self.term
    }

    #[must_use]
    pub const fn applied_index(self) -> u64 {
        self.applied_index
    }

    #[must_use]
    pub const fn closed_timestamp(self) -> TransactionTime {
        self.closed_timestamp
    }
}

pub trait ShardClient: Send + Sync {
    fn execute<'a>(&'a self, request: ExecuteCommand) -> ShardClientFuture<'a, ExecuteReceipt>;

    fn read_keys<'a>(
        &'a self,
        request: ReadKeysRequest,
    ) -> ShardClientFuture<'a, Vec<Option<Vec<u8>>>>;

    fn scan<'a>(&'a self, request: ScanRequest) -> ShardClientFuture<'a, Vec<KeyValue>>;

    fn status<'a>(&'a self, context: ShardRequestContext) -> ShardClientFuture<'a, ShardStatus>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ShardClientError {
    InvalidContext,
    EmptyCommand,
    EmptyRead,
    WrongGraph { expected: u64, actual: u64 },
    DeadlineExpired,
    NoLeader { shard_id: u32 },
    NotLeader { leader_hint: Option<u64> },
    StaleEpoch { current_epoch: Option<u64> },
    Replication(String),
    ReadBarrier(String),
    Adapter(String),
    RequestMismatch { expected: u128, actual: u128 },
    Internal(String),
}

impl Display for ShardClientError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidContext => formatter.write_str("invalid Shard request context"),
            Self::EmptyCommand => formatter.write_str("Shard command cannot be empty"),
            Self::EmptyRead => formatter.write_str("Shard key read cannot be empty"),
            Self::WrongGraph { expected, actual } => {
                write!(formatter, "expected graph {expected}, got {actual}")
            }
            Self::DeadlineExpired => formatter.write_str("Shard request deadline expired"),
            Self::NoLeader { shard_id } => write!(formatter, "Shard {shard_id} has no Leader"),
            Self::NotLeader { leader_hint } => {
                write!(
                    formatter,
                    "remote Replica is not Leader; hint={leader_hint:?}"
                )
            }
            Self::StaleEpoch { current_epoch } => {
                write!(
                    formatter,
                    "remote Shard placement epoch is stale; current={current_epoch:?}"
                )
            }
            Self::Replication(message) => write!(formatter, "Shard replication failed: {message}"),
            Self::ReadBarrier(message) => write!(formatter, "Shard read barrier failed: {message}"),
            Self::Adapter(message) => write!(formatter, "Shard Adapter failed: {message}"),
            Self::RequestMismatch { expected, actual } => write!(
                formatter,
                "request ID {actual} differs from command request ID {expected}"
            ),
            Self::Internal(message) => write!(formatter, "Shard client internal error: {message}"),
        }
    }
}

impl Error for ShardClientError {}
