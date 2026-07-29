#![forbid(unsafe_code)]

mod catalog;
mod centrality;
mod community;
mod components;
mod csr;
mod projection;
mod shortest_path;
mod temporal;
mod traversal;

pub use catalog::{
    AlgorithmBudget, AlgorithmCatalog, AlgorithmDescriptor, AlgorithmRequest, AlgorithmResult,
    AnalyticsError, CsrShape, DeterministicTieBreak, TemporalPathResult, run_builtin,
};
pub use csr::{ReverseCsr, SnapshotCsr, TypedPropertyColumn};
pub use dtg_kernel::{
    BackendGeneration, Digest32, PlacementEpoch, ShardId, TransactionId, TransactionTime, Value,
    Version,
};
pub use dtg_language_ir::BuiltInAlgorithmId;
pub use dtg_storage::{EdgeId, VertexId};
pub use projection::{
    BudgetUsage, CancellationToken, PartitionProvenance, ProjectedEdge, ProjectedVertex,
    ProjectionBudget, ProjectionError, ProjectionSpec, PropertyColumnSpec, PropertyType,
    ShardProjectionPart, ShardSnapshotProvenance, SnapshotProjectionInput, SnapshotProvenance,
};

pub const CLEAN_BREAK_ARCHITECTURE_VERSION: u32 = 1;
