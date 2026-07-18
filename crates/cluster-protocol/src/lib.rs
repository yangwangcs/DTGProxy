#![forbid(unsafe_code)]

use std::error::Error;
use std::fmt::{self, Display, Formatter};

pub const CLUSTER_PROTOCOL_VERSION: u32 = 1;
pub const IDENTIFIER_BYTES: usize = 16;
pub const MAX_COMMAND_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_SNAPSHOT_CHUNK_BYTES: usize = 4 * 1024 * 1024;

pub mod proto {
    tonic::include_proto!("dtgproxy.cluster.v1");
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommonRequestContext {
    protocol_version: u32,
    cluster_id: [u8; IDENTIFIER_BYTES],
    request_id: [u8; IDENTIFIER_BYTES],
    deadline_unix_ms: u64,
}

impl CommonRequestContext {
    pub fn new(
        protocol_version: u32,
        cluster_id: [u8; IDENTIFIER_BYTES],
        request_id: [u8; IDENTIFIER_BYTES],
        deadline_unix_ms: u64,
    ) -> Result<Self, ProtocolError> {
        if protocol_version != CLUSTER_PROTOCOL_VERSION {
            return Err(ProtocolError::UnsupportedVersion {
                actual: protocol_version,
            });
        }
        if cluster_id == [0; IDENTIFIER_BYTES] {
            return Err(ProtocolError::ZeroClusterId);
        }
        if request_id == [0; IDENTIFIER_BYTES] {
            return Err(ProtocolError::ZeroRequestId);
        }
        if deadline_unix_ms == 0 {
            return Err(ProtocolError::ZeroDeadline);
        }
        Ok(Self {
            protocol_version,
            cluster_id,
            request_id,
            deadline_unix_ms,
        })
    }

    #[must_use]
    pub const fn protocol_version(&self) -> u32 {
        self.protocol_version
    }

    #[must_use]
    pub const fn cluster_id(&self) -> &[u8; IDENTIFIER_BYTES] {
        &self.cluster_id
    }

    #[must_use]
    pub const fn request_id(&self) -> &[u8; IDENTIFIER_BYTES] {
        &self.request_id
    }

    #[must_use]
    pub const fn deadline_unix_ms(&self) -> u64 {
        self.deadline_unix_ms
    }

    pub fn ensure_active_at(&self, now_unix_ms: u64) -> Result<(), ProtocolError> {
        if now_unix_ms >= self.deadline_unix_ms {
            return Err(ProtocolError::DeadlineExpired {
                deadline_unix_ms: self.deadline_unix_ms,
                now_unix_ms,
            });
        }
        Ok(())
    }
}

impl From<CommonRequestContext> for proto::RequestContext {
    fn from(value: CommonRequestContext) -> Self {
        Self {
            protocol_version: value.protocol_version,
            cluster_id: value.cluster_id.to_vec(),
            request_id: value.request_id.to_vec(),
            deadline_unix_ms: value.deadline_unix_ms,
        }
    }
}

impl TryFrom<proto::RequestContext> for CommonRequestContext {
    type Error = ProtocolError;

    fn try_from(value: proto::RequestContext) -> Result<Self, Self::Error> {
        let cluster_id = fixed_identifier(value.cluster_id, IdentifierKind::Cluster)?;
        let request_id = fixed_identifier(value.request_id, IdentifierKind::Request)?;
        Self::new(
            value.protocol_version,
            cluster_id,
            request_id,
            value.deadline_unix_ms,
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShardRequestContext {
    common: CommonRequestContext,
    graph_id: u64,
    shard_id: u32,
    placement_epoch: u64,
}

impl ShardRequestContext {
    pub fn new(
        common: CommonRequestContext,
        graph_id: u64,
        shard_id: u32,
        placement_epoch: u64,
    ) -> Result<Self, ProtocolError> {
        if graph_id == 0 {
            return Err(ProtocolError::ZeroGraphId);
        }
        if shard_id == 0 {
            return Err(ProtocolError::ZeroShardId);
        }
        if placement_epoch == 0 {
            return Err(ProtocolError::ZeroPlacementEpoch);
        }
        Ok(Self {
            common,
            graph_id,
            shard_id,
            placement_epoch,
        })
    }

    #[must_use]
    pub const fn common(&self) -> &CommonRequestContext {
        &self.common
    }

    #[must_use]
    pub const fn graph_id(&self) -> u64 {
        self.graph_id
    }

    #[must_use]
    pub const fn shard_id(&self) -> u32 {
        self.shard_id
    }

    #[must_use]
    pub const fn placement_epoch(&self) -> u64 {
        self.placement_epoch
    }
}

impl From<ShardRequestContext> for proto::ShardContext {
    fn from(value: ShardRequestContext) -> Self {
        Self {
            request: Some(value.common.into()),
            graph_id: value.graph_id,
            shard_id: value.shard_id,
            placement_epoch: value.placement_epoch,
        }
    }
}

impl TryFrom<proto::ShardContext> for ShardRequestContext {
    type Error = ProtocolError;

    fn try_from(value: proto::ShardContext) -> Result<Self, Self::Error> {
        let common = value
            .request
            .ok_or(ProtocolError::MissingRequestContext)?
            .try_into()?;
        Self::new(
            common,
            value.graph_id,
            value.shard_id,
            value.placement_epoch,
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommandPayload(Vec<u8>);

impl CommandPayload {
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    #[must_use]
    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }
}

impl TryFrom<Vec<u8>> for CommandPayload {
    type Error = ProtocolError;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        if value.is_empty() {
            return Err(ProtocolError::EmptyCommand);
        }
        if value.len() > MAX_COMMAND_BYTES {
            return Err(ProtocolError::CommandTooLarge {
                actual: value.len(),
                maximum: MAX_COMMAND_BYTES,
            });
        }
        Ok(Self(value))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotChunkEnvelope {
    context: ShardRequestContext,
    migration_id: [u8; IDENTIFIER_BYTES],
    ordinal: u64,
    payload: Vec<u8>,
    checksum: u32,
}

impl SnapshotChunkEnvelope {
    pub fn new(
        context: ShardRequestContext,
        migration_id: [u8; IDENTIFIER_BYTES],
        ordinal: u64,
        payload: Vec<u8>,
    ) -> Result<Self, ProtocolError> {
        if migration_id == [0; IDENTIFIER_BYTES] {
            return Err(ProtocolError::ZeroMigrationId);
        }
        if payload.is_empty() {
            return Err(ProtocolError::EmptySnapshotChunk);
        }
        if payload.len() > MAX_SNAPSHOT_CHUNK_BYTES {
            return Err(ProtocolError::SnapshotChunkTooLarge {
                actual: payload.len(),
                maximum: MAX_SNAPSHOT_CHUNK_BYTES,
            });
        }
        let checksum = crc32fast::hash(&payload);
        Ok(Self {
            context,
            migration_id,
            ordinal,
            payload,
            checksum,
        })
    }

    #[must_use]
    pub const fn context(&self) -> &ShardRequestContext {
        &self.context
    }

    #[must_use]
    pub const fn migration_id(&self) -> &[u8; IDENTIFIER_BYTES] {
        &self.migration_id
    }

    #[must_use]
    pub const fn ordinal(&self) -> u64 {
        self.ordinal
    }

    #[must_use]
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    #[must_use]
    pub const fn checksum(&self) -> u32 {
        self.checksum
    }

    #[must_use]
    pub fn verify_checksum(&self) -> bool {
        crc32fast::hash(&self.payload) == self.checksum
    }
}

impl From<SnapshotChunkEnvelope> for proto::SnapshotChunk {
    fn from(value: SnapshotChunkEnvelope) -> Self {
        Self {
            context: Some(value.context.into()),
            migration_id: value.migration_id.to_vec(),
            ordinal: value.ordinal,
            payload: value.payload,
            checksum: value.checksum,
            terminal: false,
            manifest_digest: Vec::new(),
        }
    }
}

impl TryFrom<proto::SnapshotChunk> for SnapshotChunkEnvelope {
    type Error = ProtocolError;

    fn try_from(value: proto::SnapshotChunk) -> Result<Self, Self::Error> {
        if value.terminal || !value.manifest_digest.is_empty() {
            return Err(ProtocolError::TerminalChunkIsNotData);
        }
        let context = value
            .context
            .ok_or(ProtocolError::MissingShardContext)?
            .try_into()?;
        let migration_id = fixed_identifier(value.migration_id, IdentifierKind::Migration)?;
        let expected_checksum = value.checksum;
        let chunk = Self::new(context, migration_id, value.ordinal, value.payload)?;
        if chunk.checksum != expected_checksum {
            return Err(ProtocolError::SnapshotChecksumMismatch);
        }
        Ok(chunk)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum StatusReason {
    NotLeader = 1,
    StaleEpoch = 2,
    RevisionCompacted = 3,
    ResourceExhausted = 4,
}

impl TryFrom<i32> for StatusReason {
    type Error = ProtocolError;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Err(ProtocolError::UnspecifiedStatusReason),
            1 => Ok(Self::NotLeader),
            2 => Ok(Self::StaleEpoch),
            3 => Ok(Self::RevisionCompacted),
            4 => Ok(Self::ResourceExhausted),
            actual => Err(ProtocolError::UnknownStatusReason { actual }),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProtocolError {
    UnsupportedVersion {
        actual: u32,
    },
    InvalidClusterIdLength {
        actual: usize,
    },
    InvalidRequestIdLength {
        actual: usize,
    },
    InvalidMigrationIdLength {
        actual: usize,
    },
    ZeroClusterId,
    ZeroRequestId,
    ZeroMigrationId,
    ZeroDeadline,
    DeadlineExpired {
        deadline_unix_ms: u64,
        now_unix_ms: u64,
    },
    ZeroGraphId,
    ZeroShardId,
    ZeroPlacementEpoch,
    MissingRequestContext,
    MissingShardContext,
    EmptyCommand,
    CommandTooLarge {
        actual: usize,
        maximum: usize,
    },
    EmptySnapshotChunk,
    SnapshotChunkTooLarge {
        actual: usize,
        maximum: usize,
    },
    SnapshotChecksumMismatch,
    TerminalChunkIsNotData,
    UnspecifiedStatusReason,
    UnknownStatusReason {
        actual: i32,
    },
}

impl Display for ProtocolError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedVersion { actual } => write!(
                formatter,
                "unsupported cluster protocol version {actual}; expected {CLUSTER_PROTOCOL_VERSION}"
            ),
            Self::InvalidClusterIdLength { actual } => {
                write!(formatter, "cluster ID has {actual} bytes; expected 16")
            }
            Self::InvalidRequestIdLength { actual } => {
                write!(formatter, "request ID has {actual} bytes; expected 16")
            }
            Self::InvalidMigrationIdLength { actual } => {
                write!(formatter, "migration ID has {actual} bytes; expected 16")
            }
            Self::ZeroClusterId => formatter.write_str("cluster ID cannot be zero"),
            Self::ZeroRequestId => formatter.write_str("request ID cannot be zero"),
            Self::ZeroMigrationId => formatter.write_str("migration ID cannot be zero"),
            Self::ZeroDeadline => formatter.write_str("request deadline cannot be zero"),
            Self::DeadlineExpired {
                deadline_unix_ms,
                now_unix_ms,
            } => write!(
                formatter,
                "request deadline {deadline_unix_ms} is not after current time {now_unix_ms}"
            ),
            Self::ZeroGraphId => formatter.write_str("graph ID cannot be zero"),
            Self::ZeroShardId => formatter.write_str("Shard ID cannot be zero"),
            Self::ZeroPlacementEpoch => formatter.write_str("placement epoch cannot be zero"),
            Self::MissingRequestContext => formatter.write_str("request context is missing"),
            Self::MissingShardContext => formatter.write_str("Shard context is missing"),
            Self::EmptyCommand => formatter.write_str("command payload cannot be empty"),
            Self::CommandTooLarge { actual, maximum } => write!(
                formatter,
                "command payload has {actual} bytes; maximum is {maximum}"
            ),
            Self::EmptySnapshotChunk => formatter.write_str("snapshot chunk cannot be empty"),
            Self::SnapshotChunkTooLarge { actual, maximum } => write!(
                formatter,
                "snapshot chunk has {actual} bytes; maximum is {maximum}"
            ),
            Self::SnapshotChecksumMismatch => formatter.write_str("snapshot checksum mismatch"),
            Self::TerminalChunkIsNotData => {
                formatter.write_str("terminal snapshot chunk is not a data chunk")
            }
            Self::UnspecifiedStatusReason => formatter.write_str("status reason is unspecified"),
            Self::UnknownStatusReason { actual } => {
                write!(formatter, "unknown status reason {actual}")
            }
        }
    }
}

impl Error for ProtocolError {}

enum IdentifierKind {
    Cluster,
    Request,
    Migration,
}

fn fixed_identifier(
    value: Vec<u8>,
    kind: IdentifierKind,
) -> Result<[u8; IDENTIFIER_BYTES], ProtocolError> {
    let actual = value.len();
    value.try_into().map_err(|_: Vec<u8>| match kind {
        IdentifierKind::Cluster => ProtocolError::InvalidClusterIdLength { actual },
        IdentifierKind::Request => ProtocolError::InvalidRequestIdLength { actual },
        IdentifierKind::Migration => ProtocolError::InvalidMigrationIdLength { actual },
    })
}
