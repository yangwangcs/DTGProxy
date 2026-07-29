#![forbid(unsafe_code)]

mod batch;
mod budget;
mod exchange;
mod expression;
mod operator;
mod runtime;
mod spill;
mod storage_source;

pub use batch::{ColumnBatch, QueryValue};
pub use budget::{CancellationToken, QueryBudget, QueryContext, QueryError};
pub use exchange::{DeterministicMergeOperator, ExchangeOperator};
pub use expression::Expression;
pub use operator::{
    AggregateOperator, BatchOperator, ExpandOperator, FilterOperator, HashJoinOperator,
    LimitOperator, Operator, OverlayOperator, ProjectOperator, ProjectionExpr, QueryFuture,
    QueryOverlay, SortOperator,
};
pub use runtime::{QueryRuntime, QueryStream};
pub(crate) use spill::SpillMergeOperator;
pub use spill::{SpillConfig, SpillHandle, SpillStore};
pub(crate) use storage_source::StorageSourceOperator;
pub use storage_source::{
    ExecutableAccess, ExecutableAggregate, ExecutableFragment, ExecutableOperator,
    ExecutableOperatorKind, ExecutablePlan, ExecutableProjection, ExecutableSortKey,
    ExecutionFence, LogicalRead, QueryStorage, ReadOperation, ResidualPredicate, SnapshotGuard,
    SnapshotShardFence,
};

pub const CLEAN_BREAK_ARCHITECTURE_VERSION: u32 = 1;
