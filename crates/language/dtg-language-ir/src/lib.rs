#![forbid(unsafe_code)]

mod analytics;
mod expr;
mod plan;
mod schema;
mod validate;

pub use analytics::{AnalyticsSubmission, BuiltInAlgorithmId, BuiltInAlgorithmIdError};
pub use expr::{BinaryOperator, LogicalExpr, UnaryOperator};
pub use plan::{
    Aggregate, AggregateFunction, Expand, Join, JoinKind, Limit, LogicalMutation, LogicalNode,
    LogicalNodeId, LogicalNodeKind, LogicalPlan, LogicalProgram, LogicalStatement, LogicalWrite,
    NodeScan, Projection, RelationshipScan, Sort, SortDirection, SortKey, Subquery, TemporalScope,
    TimeExpr, Unwind,
};
pub use schema::{Field, LogicalType, Parameter, RowSchema};
pub use validate::{IrError, IrVersion, validate_program};

pub const CLEAN_BREAK_ARCHITECTURE_VERSION: u32 = 1;
