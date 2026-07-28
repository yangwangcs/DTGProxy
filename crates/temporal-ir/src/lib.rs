#![forbid(unsafe_code)]

use temporal_storage::{ElementId, ElementKind, ElementRef, GraphId, PartitionId};

mod error;
mod expression;
mod header;
mod logical;
mod procedure;
mod schema;

pub use error::ValidationError;
pub use expression::ScalarExpr;
pub use header::{LanguageProfile, PLAN_VERSION, PlanHeader};
pub use logical::{
    ApplyKind, ApplySlotMapping, ChangeAxis, ChildPlanId, LogicalApply, LogicalBatchSubtransaction,
    LogicalNode, LogicalNodeId, LogicalOperator, LogicalPlan, LogicalPlanBuilder, MAX_APPLY_DEPTH,
    MAX_APPLY_INVOCATIONS, MAX_APPLY_OUTPUT_ROWS, MAX_BATCH_SUBTRANSACTION_ROWS, SortKey,
    TemporalJoinKind, TransactionTimeSpec, ValidTimeSpec,
};
pub use procedure::{
    ProcedureArgument, ProcedureEffect, ProcedureIdentity, ProcedurePlacement,
    ProcedureYieldBinding, ResolvedProcedure,
};
pub use schema::{Column, RowSchema, SlotId, ValueType};

/// Stable graph/partition ownership used by routing and transaction protocols.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GraphScope {
    graph: GraphId,
    partition: PartitionId,
}

impl GraphScope {
    #[must_use]
    pub const fn new(graph: GraphId, partition: PartitionId) -> Self {
        Self { graph, partition }
    }

    #[must_use]
    pub const fn graph(self) -> GraphId {
        self.graph
    }

    #[must_use]
    pub const fn partition(self) -> PartitionId {
        self.partition
    }

    #[must_use]
    pub const fn element(self, kind: ElementKind, id: ElementId) -> ElementRef {
        match kind {
            ElementKind::Vertex => ElementRef::vertex(self.graph, self.partition, id),
            ElementKind::Edge => ElementRef::edge(self.graph, self.partition, id),
        }
    }
}
