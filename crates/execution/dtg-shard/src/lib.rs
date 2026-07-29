#![forbid(unsafe_code)]

mod command;
mod host;
mod migration;
mod raft_store;
mod read;
mod replica;
mod snapshot;
mod state_machine;

pub use command::{
    AdvanceClosedTimestamp, CommandHeader, CommitSingleShard, CommitSingleShardTransaction,
    FinalizeParticipant, InstallSnapshot, ParticipantIntent, PrewriteIntent, RecordHomeDecision,
    SUPPORTED_SHARD_COMMAND_FORMAT_VERSION, SUPPORTED_TRANSACTION_INTENT_VERSION, ShardCommand,
    TRANSACTION_INTENT_METADATA_NAME,
};
pub use host::{ReplicaKey, ShardHost};
pub use migration::{MigrationCommand, MigrationPhase};
pub use raft_store::RaftStore;
pub use read::{
    FollowerReadProof, FollowerReadProofAuthority, ReadError, ReadFailure, ReadMode, ReadPermit,
    SUPPORTED_FOLLOWER_READ_PROOF_VERSION,
};
pub use replica::{
    ProposalReceipt, RaftProgress, RaftReplica, ReplicaLifecycle, ReplicaObservation,
};
pub use snapshot::{
    ReplicaSnapshot, ReplicaSnapshotError, ReplicaSnapshotInstallReceipt,
    ReplicaSnapshotInstallState, ReplicaSnapshotManifest,
    SUPPORTED_REPLICA_SNAPSHOT_FORMAT_VERSION, create_replica_snapshot, install_replica_snapshot,
};
pub use state_machine::{
    ACTIVE_TRANSACTION_INTENTS_METADATA_NAME, ApplyOutcome, SINGLE_SHARD_TRANSACTION_METADATA_NAME,
    ShardError, ShardStateMachine, SingleShardTransactionReceipt,
    TRANSACTION_STATE_METADATA_PREFIX, decode_single_shard_transaction_metadata,
};

pub const CLEAN_BREAK_ARCHITECTURE_VERSION: u32 = 1;
