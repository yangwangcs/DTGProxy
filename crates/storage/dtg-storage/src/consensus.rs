use dtg_kernel::{Digest32, ReplicaId};

use crate::{
    BindingRole, CommandId, LogicalReplicaActivationReceipt, LogicalSnapshotCandidateReceipt,
    ReplicaBinding, StorageError, StoreFuture,
};

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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConsensusSnapshotInstall {
    candidate: LogicalSnapshotCandidateReceipt,
    active_binding: ReplicaBinding,
    hard_state: RaftHardState,
    membership: RaftMembership,
    metadata: ConsensusSnapshotMetadata,
}

impl ConsensusSnapshotInstall {
    pub fn new(
        candidate: LogicalSnapshotCandidateReceipt,
        active_binding: ReplicaBinding,
        hard_state: RaftHardState,
        membership: RaftMembership,
        metadata: ConsensusSnapshotMetadata,
    ) -> Result<Self, StorageError> {
        LogicalReplicaActivationReceipt::new(&candidate, active_binding.clone())?;
        let target_count = membership
            .voters
            .iter()
            .chain(&membership.learners)
            .filter(|replica| **replica == active_binding.replica_id())
            .count();
        if candidate.candidate_binding().role() != BindingRole::Candidate
            || active_binding.role() != BindingRole::Active
            || metadata.snapshot_id != candidate.header().snapshot_id().get()
            || metadata.last_included_index != candidate.header().applied_index()
            || metadata.content_digest != candidate.manifest().content_digest()
            || metadata.last_included_term == 0
            || hard_state.current_term < metadata.last_included_term
            || hard_state.committed_index < metadata.last_included_index
            || target_count != 1
        {
            return Err(StorageError::InvalidConsensus(
                "snapshot install journal is inconsistent".into(),
            ));
        }
        Ok(Self {
            candidate,
            active_binding,
            hard_state,
            membership,
            metadata,
        })
    }

    pub const fn candidate(&self) -> &LogicalSnapshotCandidateReceipt {
        &self.candidate
    }

    pub const fn active_binding(&self) -> &ReplicaBinding {
        &self.active_binding
    }

    pub const fn hard_state(&self) -> RaftHardState {
        self.hard_state
    }

    pub const fn membership(&self) -> &RaftMembership {
        &self.membership
    }

    pub const fn metadata(&self) -> &ConsensusSnapshotMetadata {
        &self.metadata
    }
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
    fn snapshot_install(&self) -> StoreFuture<'_, Option<ConsensusSnapshotInstall>>;
    fn stage_snapshot_install(&self, install: ConsensusSnapshotInstall) -> StoreFuture<'_, ()>;
    fn commit_snapshot_install(&self, install: ConsensusSnapshotInstall) -> StoreFuture<'_, ()>;
}
