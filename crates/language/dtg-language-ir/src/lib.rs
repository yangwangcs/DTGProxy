#![forbid(unsafe_code)]

mod analytics;
mod expr;
mod plan;
mod schema;
mod validate;

pub use analytics::{
    AnalyticsExecutionMode, AnalyticsRequestIdentity, AnalyticsRequestIdentityError,
    AnalyticsSubmission, BuiltInAlgorithmId, BuiltInAlgorithmIdError,
};
pub use expr::{BinaryOperator, LogicalExpr, UnaryOperator};
pub use plan::{
    Aggregate, AggregateFunction, AggregateKind, Expand, ExpandDirection, GraphScope, Join,
    JoinKind, Limit, LogicalMutation, LogicalNode, LogicalNodeId, LogicalNodeKind, LogicalPlan,
    LogicalProgram, LogicalStatement, LogicalWrite, NodeScan, Projection, ReadScope,
    RelationshipLookup, RelationshipScan, Sort, SortDirection, SortKey, Subquery, TemporalScope,
    TimeExpr, Unwind, ValidIntervalExpr, ValidTimeExpr, ValidTimePredicate, VertexLookup,
};
pub use schema::{Field, LogicalType, Parameter, RowSchema};
pub use validate::{IrError, IrVersion, validate_program};

pub const CLEAN_BREAK_ARCHITECTURE_VERSION: u32 = 1;
