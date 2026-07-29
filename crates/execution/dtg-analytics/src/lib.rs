#![forbid(unsafe_code)]

mod artifact;
mod catalog;
mod centrality;
mod community;
mod components;
mod csr;
mod job;
mod ledger;
mod projection;
mod scheduler;
mod shortest_path;
mod temporal;
mod traversal;

pub use artifact::{
    AnalyticsArtifact, AnalyticsArtifactGarbageCollector, AnalyticsArtifactIoBudget,
    AnalyticsArtifactManifest, AnalyticsArtifactRepository, StorageArtifactRepository,
};
pub use catalog::{
    AlgorithmBudget, AlgorithmCatalog, AlgorithmDescriptor, AlgorithmRequest, AlgorithmResult,
    AnalyticsError, CsrShape, DeterministicTieBreak, TemporalPathResult, run_builtin,
};
pub use csr::{ReverseCsr, SnapshotCsr, TypedPropertyColumn};
pub use dtg_kernel::{
    BackendGeneration, Digest32, PlacementEpoch, ShardId, TransactionId, TransactionTime, Value,
    Version,
};
pub use dtg_language_ir::{AnalyticsRequestIdentity, BuiltInAlgorithmId};
pub use dtg_storage::{
    ArtifactChunk, ArtifactKey, ArtifactKind, ArtifactManifest, ArtifactStore, EdgeId, VertexId,
};
pub use job::{
    AnalyticsJobError, AnalyticsJobId, AnalyticsJobSpec, AnalyticsJobState, AnalyticsJobStateKind,
    AnalyticsRuntimeFences, JobCas, JobLease, JobTimestamp, WorkerId,
};
pub use ledger::{AnalyticsGcCandidate, AnalyticsJobRecord, AnalyticsLedger};
pub use projection::{
    BudgetUsage, CancellationToken, PartitionProvenance, ProjectedEdge, ProjectedVertex,
    ProjectionBudget, ProjectionError, ProjectionSpec, PropertyColumnSpec, PropertyType,
    ShardProjectionPart, ShardSnapshotProvenance, SnapshotProjectionInput, SnapshotProvenance,
};
pub use scheduler::{
    AnalyticsAlgorithmStep, AnalyticsProjection, AnalyticsProjectionProvider, AnalyticsScheduler,
    AnalyticsSchedulerConfig, AnalyticsSchedulerTick, AnalyticsStepProvider, AnalyticsStepRequest,
    SchedulerFailure,
};

pub const CLEAN_BREAK_ARCHITECTURE_VERSION: u32 = 1;
