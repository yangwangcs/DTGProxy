use dtg_kernel::{Digest32, ReplicaId};

use crate::{CommandId, ReplicaBinding, StorageError, StoreFuture};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConsensusEntry {
    term: u64,
    index: u64,
    command_id: CommandId,
    command_digest: Digest32,
    command: Vec<u8>,
}

impl ConsensusEntry {
    pub fn new(
        term: u64,
        index: u64,
        command_id: CommandId,
        command_digest: Digest32,
        command: Vec<u8>,
    ) -> Result<Self, StorageError> {
        if term == 0 || index == 0 || command_digest.get() == [0; 32] || command.is_empty() {
            return Err(StorageError::InvalidConsensus(
                "consensus entry identity and command must be complete".into(),
            ));
        }
        Ok(Self {
            term,
            index,
            command_id,
            command_digest,
            command,
        })
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

    pub fn command(&self) -> &[u8] {
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
