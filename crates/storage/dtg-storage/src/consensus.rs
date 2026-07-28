use dtg_kernel::{Digest32, ReplicaId};

use crate::{CommandId, ReplicaBinding, StorageError, StoreFuture};

pub const SUPPORTED_CONSENSUS_COMMAND_FORMAT_VERSION: u32 = 1;
pub const SUPPORTED_CONSENSUS_WAL_FORMAT_VERSION: u32 = 1;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConsensusCommandEnvelope {
    format_version: u32,
    payload: Vec<u8>,
    digest: Digest32,
}

impl ConsensusCommandEnvelope {
    pub fn new(format_version: u32, payload: Vec<u8>) -> Result<Self, StorageError> {
        if format_version != SUPPORTED_CONSENSUS_COMMAND_FORMAT_VERSION || payload.is_empty() {
            return Err(StorageError::InvalidConsensus(
                "unsupported or empty consensus command envelope".into(),
            ));
        }
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"dtg-consensus-command-envelope-v1");
        hasher.update(&format_version.to_be_bytes());
        hasher.update(&(payload.len() as u64).to_be_bytes());
        hasher.update(&payload);
        let digest = Digest32::new(*hasher.finalize().as_bytes());
        Ok(Self {
            format_version,
            payload,
            digest,
        })
    }

    pub const fn format_version(&self) -> u32 {
        self.format_version
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    pub const fn digest(&self) -> Digest32 {
        self.digest
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConsensusEntry {
    wal_format_version: u32,
    term: u64,
    index: u64,
    command_id: CommandId,
    command_digest: Digest32,
    command: ConsensusCommandEnvelope,
}

impl ConsensusEntry {
    pub fn new(
        wal_format_version: u32,
        term: u64,
        index: u64,
        command_id: CommandId,
        command: ConsensusCommandEnvelope,
    ) -> Result<Self, StorageError> {
        if wal_format_version != SUPPORTED_CONSENSUS_WAL_FORMAT_VERSION || term == 0 || index == 0 {
            return Err(StorageError::InvalidConsensus(
                "unsupported consensus WAL format or incomplete entry identity".into(),
            ));
        }
        let command_digest = command.digest();
        Ok(Self {
            wal_format_version,
            term,
            index,
            command_id,
            command_digest,
            command,
        })
    }

    pub const fn wal_format_version(&self) -> u32 {
        self.wal_format_version
    }

    pub const fn term(&self) -> u64 {
        self.term
    }

    pub const fn index(&self) -> u64 {
        self.index
    }

    pub const fn command_id(&self) -> CommandId {
        self.command_id
    }

    pub const fn command_digest(&self) -> Digest32 {
        self.command_digest
    }

    pub const fn command(&self) -> &ConsensusCommandEnvelope {
        &self.command
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RaftHardState {
    pub current_term: u64,
    pub voted_for: Option<ReplicaId>,
    pub committed_index: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RaftMembership {
    pub voters: Vec<ReplicaId>,
    pub learners: Vec<ReplicaId>,
    pub configuration_index: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConsensusSnapshotMetadata {
    pub snapshot_id: u128,
    pub last_included_term: u64,
    pub last_included_index: u64,
    pub content_digest: Digest32,
}

pub trait ConsensusStore: Send + Sync {
    fn binding(&self) -> &ReplicaBinding;
    /// Appends one contiguous sequence. Exact overlap is idempotent. The first
    /// divergent existing index is atomically replaced together with the
    /// supplied suffix, and every higher stored entry is truncated.
    fn append(&self, entries: Vec<ConsensusEntry>) -> StoreFuture<'_, ()>;
    fn entries(&self, low: u64, high: u64, max_bytes: u64) -> StoreFuture<'_, Vec<ConsensusEntry>>;
    fn truncate_suffix(&self, from_index: u64) -> StoreFuture<'_, ()>;
    fn hard_state(&self) -> StoreFuture<'_, RaftHardState>;
    fn set_hard_state(&self, state: RaftHardState) -> StoreFuture<'_, ()>;
    fn membership(&self) -> StoreFuture<'_, RaftMembership>;
    fn set_membership(&self, membership: RaftMembership) -> StoreFuture<'_, ()>;
    fn snapshot_metadata(&self) -> StoreFuture<'_, Option<ConsensusSnapshotMetadata>>;
    fn set_snapshot_metadata(&self, metadata: ConsensusSnapshotMetadata) -> StoreFuture<'_, ()>;
}
