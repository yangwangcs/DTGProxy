#![forbid(unsafe_code)]

mod conflict;
mod coordinator;
mod overlay;
mod participant;
mod recovery;
mod snapshot;
mod timestamp;

pub use conflict::{EntityIdentity, IntervalWrite, detect_conflict};
pub use coordinator::{
    CommitResolution, CommitTimeReservation, TemporalTxnCoordinator, TimestampAuthority,
    TransactionContext, TransactionOutcome,
};
pub use dtg_kernel::Value;
pub use dtg_kernel::{
    BackendGeneration, PlacementEpoch, ShardId, TransactionId, TransactionTime, ValidInterval,
    Version,
};
pub use dtg_storage::{
    ChangeCursor, ChangeRecord, EdgeId, EdgeTombstone, EdgeVersion, LogicalMutation, Properties,
    ReplicaMetadata, TransactionRecord, TransactionState, VertexId, VertexTombstone, VertexVersion,
};
pub use overlay::{BaseGraphSnapshot, TransactionOverlay};
pub use participant::{
    DurableParticipantManifest, DurableTransactionManifest, ParticipantService, ParticipantWrite,
    RecoveredParticipantIntent, RecoveredSingleShardCommit, RecoveryLease, ShardCommandExecutor,
    ShardRequest, ShardRequestHeader, SubmissionFailure, SubmissionFuture, SubmissionReceipt,
    TransactionHistory, TxnFuture,
};
pub use snapshot::{ShardSnapshotFence, SnapshotToken};
pub use timestamp::{DurableTimestampAuthority, TimestampCommandLog, TimestampLogFuture};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TxnError {
    WriteConflict,
    IncompleteSnapshot,
    DuplicateShardFence,
    InconsistentSnapshot,
    StalePlacementEpoch,
    StaleBackendGeneration,
    AppliedIndexUnavailable,
    ReadTimeExceedsClosedTime,
    ReferentialIntegrity,
    DuplicateIdentity,
    InvalidMutation,
    ResourceLimit,
    ParticipantsMismatch,
    Shard(String),
    Storage(String),
    CorruptRecovery,
    InjectedCrash,
}

impl TxnError {
    pub const fn code(&self) -> &'static str {
        match self {
            Self::WriteConflict => "DTG-TXN-WRITE-CONFLICT",
            Self::IncompleteSnapshot => "DTG-TXN-SNAPSHOT-INCOMPLETE",
            Self::DuplicateShardFence => "DTG-TXN-SNAPSHOT-DUPLICATE",
            Self::InconsistentSnapshot => "DTG-TXN-SNAPSHOT-INCONSISTENT",
            Self::StalePlacementEpoch => "DTG-TXN-STALE-EPOCH",
            Self::StaleBackendGeneration => "DTG-TXN-STALE-GENERATION",
            Self::AppliedIndexUnavailable => "DTG-TXN-APPLIED-INDEX",
            Self::ReadTimeExceedsClosedTime => "DTG-TXN-CLOSED-TIME",
            Self::ReferentialIntegrity => "DTG-TXN-REFERENTIAL-INTEGRITY",
            Self::DuplicateIdentity => "DTG-TXN-DUPLICATE-IDENTITY",
            Self::InvalidMutation => "DTG-TXN-MUTATION",
            Self::ResourceLimit => "DTG-TXN-RESOURCE-LIMIT",
            Self::ParticipantsMismatch => "DTG-TXN-PARTICIPANTS",
            Self::Shard(_) => "DTG-TXN-SHARD",
            Self::Storage(_) => "DTG-TXN-STORAGE",
            Self::CorruptRecovery => "DTG-TXN-RECOVERY-CORRUPT",
            Self::InjectedCrash => "DTG-TXN-INJECTED-CRASH",
        }
    }
}

impl core::fmt::Display for TxnError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Shard(message) | Self::Storage(message) => formatter.write_str(message),
            _ => formatter.write_str(self.code()),
        }
    }
}

impl std::error::Error for TxnError {}

impl From<dtg_storage::StorageError> for TxnError {
    fn from(error: dtg_storage::StorageError) -> Self {
        Self::Storage(error.to_string())
    }
}

pub const CLEAN_BREAK_ARCHITECTURE_VERSION: u32 = 1;
