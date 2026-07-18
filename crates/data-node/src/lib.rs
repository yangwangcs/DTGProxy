#![forbid(unsafe_code)]

mod config;
mod file_config;
mod host;
mod identity;
mod manifest;
mod migration;
mod raft_network;
mod replica_actor;
mod service;

pub use config::{ConfigError, NodeConfig, TlsFiles, TransportSecurity};
pub use file_config::{DataNodeRuntimeConfig, FileConfigError};
pub use host::{
    DataNodeHost, EnsureReplicaOutcome, HostError, ProposalOutcome, ReplicaKey, ReplicaSpec,
    ReplicaStatus,
};
pub use identity::{NodeIdentity, NodeIdentityStore};
pub use manifest::{ReplicaEntry, ReplicaManifest, ReplicaManifestStore, ReplicaRole};
pub use migration::{
    ChunkAppendOutcome, MigrationChunk, MigrationReceipt, MigrationReceiptStore,
    MigrationStorageError, ReceiptWriteOutcome, SnapshotInbox,
};
pub use raft_network::{RaftDelivery, RaftNetworkError, SharedRaftTransport};
pub use service::{
    DataNodeGrpcService, DataOperation, ReadCodecError, ReplicaProfileError, RequestAuthorizer,
    decode_key_read_result, decode_key_scan_batch, encode_key_read_plan, encode_key_scan_plan,
    encode_rocks_replica_profile,
};

use std::error::Error;
use std::fmt::{self, Display, Formatter};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StorageError {
    Io(String),
    DirectoryLocked,
    ZeroClusterId,
    ZeroNodeId,
    IdentityMismatch { expected: u64, actual: u64 },
    ClusterIdentityMismatch,
    InvalidIdentityMagic,
    UnsupportedIdentityVersion { actual: u16 },
    InvalidIdentityLength,
    IdentityChecksumMismatch,
    InvalidManifestMagic,
    UnsupportedManifestVersion { actual: u16 },
    InvalidManifestLength,
    ManifestTooLarge,
    ManifestChecksumMismatch,
    ManifestTrailingBytes,
    TooManyReplicas,
    InvalidReplicaIdentity,
    InvalidReplicaEpoch,
    InvalidReplicaVoters,
    InvalidReplicaGeneration,
    InvalidReplicaSnapshotIndex,
    InvalidReplicaRole { actual: u8 },
    InvalidReplicaDirectory,
    ReplicaIdentityConflict { graph_id: u64, shard_id: u32 },
    ReplicaDirectoryConflict { directory: String },
    StringTooLong,
    InvalidUtf8,
}

impl Display for StorageError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(message) => write!(formatter, "node storage I/O error: {message}"),
            Self::DirectoryLocked => formatter.write_str("data directory is already in use"),
            Self::ZeroClusterId => formatter.write_str("cluster ID cannot be zero"),
            Self::ZeroNodeId => formatter.write_str("node ID cannot be zero"),
            Self::IdentityMismatch { expected, actual } => write!(
                formatter,
                "data directory belongs to node {actual}; configured node is {expected}"
            ),
            Self::ClusterIdentityMismatch => {
                formatter.write_str("data directory belongs to another cluster")
            }
            Self::InvalidIdentityMagic => formatter.write_str("invalid node identity magic"),
            Self::UnsupportedIdentityVersion { actual } => {
                write!(formatter, "unsupported node identity version {actual}")
            }
            Self::InvalidIdentityLength => formatter.write_str("invalid node identity length"),
            Self::IdentityChecksumMismatch => {
                formatter.write_str("node identity checksum mismatch")
            }
            Self::InvalidManifestMagic => formatter.write_str("invalid Replica manifest magic"),
            Self::UnsupportedManifestVersion { actual } => {
                write!(formatter, "unsupported Replica manifest version {actual}")
            }
            Self::InvalidManifestLength => formatter.write_str("invalid Replica manifest length"),
            Self::ManifestTooLarge => formatter.write_str("Replica manifest exceeds its limit"),
            Self::ManifestChecksumMismatch => {
                formatter.write_str("Replica manifest checksum mismatch")
            }
            Self::ManifestTrailingBytes => {
                formatter.write_str("Replica manifest contains trailing bytes")
            }
            Self::TooManyReplicas => formatter.write_str("Replica manifest has too many entries"),
            Self::InvalidReplicaIdentity => formatter.write_str("invalid Replica identity"),
            Self::InvalidReplicaEpoch => formatter.write_str("invalid Replica placement epoch"),
            Self::InvalidReplicaVoters => formatter.write_str("invalid Replica voter set"),
            Self::InvalidReplicaGeneration => {
                formatter.write_str("invalid Replica schema or backend generation")
            }
            Self::InvalidReplicaSnapshotIndex => {
                formatter.write_str("invalid learner Replica snapshot index")
            }
            Self::InvalidReplicaRole { actual } => {
                write!(formatter, "invalid Replica role {actual}")
            }
            Self::InvalidReplicaDirectory => formatter.write_str("invalid Replica directory"),
            Self::ReplicaIdentityConflict { graph_id, shard_id } => write!(
                formatter,
                "Replica ({graph_id}, {shard_id}) already has different metadata"
            ),
            Self::ReplicaDirectoryConflict { directory } => {
                write!(
                    formatter,
                    "Replica directory {directory:?} is already assigned"
                )
            }
            Self::StringTooLong => formatter.write_str("manifest string exceeds its limit"),
            Self::InvalidUtf8 => formatter.write_str("manifest string is not valid UTF-8"),
        }
    }
}

impl Error for StorageError {}

impl From<std::io::Error> for StorageError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error.to_string())
    }
}
