#![forbid(unsafe_code)]

mod artifact;
mod binding;
mod capability;
mod consensus;
mod error;
mod mutation;
mod read;
mod snapshot;
mod tck;

pub use artifact::{ArtifactChunk, ArtifactKey, ArtifactKind, ArtifactManifest, ArtifactStore};
pub use binding::{
    BackendClass, BindingRole, DurabilityPolicy, NamespaceId, ProviderKind, ReplicaBinding,
    ReplicaBindingBuilder,
};
pub use capability::{
    CapabilityManifest, PushdownExecutor, PushdownOperation, PushdownOutcome, PushdownRequest,
    SUPPORTED_PUSHDOWN_CONTRACT_VERSION,
};
pub use consensus::{
    ConsensusCommandEnvelope, ConsensusEntry, ConsensusSnapshotMetadata, ConsensusStore,
    RaftHardState, RaftMembership, SUPPORTED_CONSENSUS_COMMAND_FORMAT_VERSION,
    SUPPORTED_CONSENSUS_WAL_FORMAT_VERSION,
};
pub use dtg_kernel::{
    BackendGeneration, ClusterId, Digest32, GraphId, PlacementEpoch, ReplicaId, ShardId,
    TransactionId, TransactionTime, ValidInterval, Value, Version,
};
pub use error::StorageError;
pub use mutation::{
    ApplyReceipt, CommandId, CommittedShardBatch, EdgeId, EdgeTombstone, EdgeVersion,
    LogicalMutation, Properties, ReplicaMetadata, TransactionRecord, TransactionState, VertexId,
    VertexTombstone, VertexVersion,
};
pub use read::{
    AdjacencyDirection, AdjacencyRead, ChangePage, ChangeRecord, ChangesRead, EdgeHistoryRead,
    EdgeRead, EdgeScan, ReadFence, ReplicaStateStore, ScanPage, StoreFuture, TemporalReadView,
    VertexHistoryRead, VertexRead, VertexScan,
};
pub use snapshot::{
    LogicalSnapshotReader, LogicalSnapshotSink, LogicalSnapshotSource, LogicalSnapshotWriter,
    SUPPORTED_SNAPSHOT_FORMAT_VERSION, SnapshotChunk, SnapshotHeader, SnapshotId, SnapshotManifest,
    SnapshotRecord, SnapshotRequest, SnapshotRestoreReceipt,
};
pub use tck::{StorageTckFactory, StorageTckStore, run_storage_tck};

pub const CLEAN_BREAK_ARCHITECTURE_VERSION: u32 = 1;
