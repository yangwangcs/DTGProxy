#![forbid(unsafe_code)]

mod command;
mod host;
mod raft_store;
mod replica;
mod state_machine;

pub use command::{
    AdvanceClosedTimestamp, CommandHeader, CommitSingleShard, CommitSingleShardTransaction,
    FinalizeParticipant, InstallSnapshot, MigrationCommand, MigrationPhase, ParticipantIntent,
    PrewriteIntent, RecordHomeDecision, SUPPORTED_SHARD_COMMAND_FORMAT_VERSION,
    SUPPORTED_TRANSACTION_INTENT_VERSION, ShardCommand, TRANSACTION_INTENT_METADATA_NAME,
};
pub use host::{ReplicaKey, ShardHost};
pub use raft_store::RaftStore;
pub use replica::{
    ProposalReceipt, RaftProgress, RaftReplica, ReplicaLifecycle, ReplicaObservation,
};
pub use state_machine::{
    ACTIVE_TRANSACTION_INTENTS_METADATA_NAME, ApplyOutcome, SINGLE_SHARD_TRANSACTION_METADATA_NAME,
    ShardError, ShardStateMachine, SingleShardTransactionReceipt,
    TRANSACTION_STATE_METADATA_PREFIX, decode_single_shard_transaction_metadata,
};

pub const CLEAN_BREAK_ARCHITECTURE_VERSION: u32 = 1;
