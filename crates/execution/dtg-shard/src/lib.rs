#![forbid(unsafe_code)]

mod command;
mod host;
mod raft_store;
mod replica;
mod state_machine;

pub use command::{
    AdvanceClosedTimestamp, CommandHeader, CommitSingleShard, FinalizeParticipant, InstallSnapshot,
    MigrationCommand, MigrationPhase, ParticipantIntent, PrewriteIntent, RecordHomeDecision,
    SUPPORTED_SHARD_COMMAND_FORMAT_VERSION, SUPPORTED_TRANSACTION_INTENT_VERSION, ShardCommand,
    TRANSACTION_INTENT_METADATA_NAME,
};
pub use host::{ReplicaKey, ShardHost};
pub use raft_store::RaftStore;
pub use replica::{
    ProposalReceipt, RaftProgress, RaftReplica, ReplicaLifecycle, ReplicaObservation,
};
pub use state_machine::{ApplyOutcome, ShardError, ShardStateMachine};

pub const CLEAN_BREAK_ARCHITECTURE_VERSION: u32 = 1;
