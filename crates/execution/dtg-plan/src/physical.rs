use dtg_language_ir::{
    AggregateFunction, JoinKind, LogicalExpr, LogicalNodeId, Projection, RowSchema, SortKey,
};
use dtg_storage::{PushdownRequest, Version};

use crate::{
    Exchange, FragmentId, LogicalReadRequest, PlanFragment, PushdownGuarantee, SemanticRequirements,
};

pub const PHYSICAL_PLAN_VERSION: Version = Version::new(1);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalPlan {
    pub version: Version,
    pub fragments: Vec<PlanFragment>,
    pub exchanges: Vec<Exchange>,
    pub root_operator: LogicalNodeId,
    pub operators: Vec<PhysicalOperator>,
    pub result_schema: RowSchema,
}

impl PhysicalPlan {
    pub fn fragments(&self) -> &[PlanFragment] {
        &self.fragments
    }

    pub fn exchanges(&self) -> &[Exchange] {
        &self.exchanges
    }

    pub const fn root_operator(&self) -> LogicalNodeId {
        self.root_operator
    }

    pub fn operators(&self) -> &[PhysicalOperator] {
        &self.operators
    }

    pub fn operator(&self, id: LogicalNodeId) -> Option<&PhysicalOperator> {
        self.operators.iter().find(|operator| operator.id == id)
    }

    pub const fn result_schema(&self) -> &RowSchema {
        &self.result_schema
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalOperator {
    id: LogicalNodeId,
    kind: PhysicalOperatorKind,
}

impl PhysicalOperator {
    pub(crate) const fn new(id: LogicalNodeId, kind: PhysicalOperatorKind) -> Self {
        Self { id, kind }
    }

    pub const fn id(&self) -> LogicalNodeId {
        self.id
    }

    pub const fn kind(&self) -> &PhysicalOperatorKind {
        &self.kind
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PhysicalOperatorKind {
    Source {
        logical_node: LogicalNodeId,
        fragments: Vec<FragmentId>,
        output: String,
    },
    Filter {
        input: LogicalNodeId,
        predicate: PhysicalExpr,
    },
    Project {
        input: LogicalNodeId,
        projections: Vec<Projection>,
    },
    Join {
        left: LogicalNodeId,
        right: LogicalNodeId,
        kind: JoinKind,
        predicate: Option<PhysicalExpr>,
    },
    Aggregate {
        input: LogicalNodeId,
        groups: Vec<Projection>,
        aggregates: Vec<AggregateFunction>,
    },
    Sort {
        input: LogicalNodeId,
        keys: Vec<SortKey>,
    },
    Limit {
        input: LogicalNodeId,
        skip: u64,
        limit: Option<u64>,
    },
    Unwind {
        input: LogicalNodeId,
        expression: PhysicalExpr,
        alias: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PhysicalExpr {
    Evaluate(LogicalExpr),
    VerifyStorageSemantics {
        node: LogicalNodeId,
        requirements: SemanticRequirements,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StorageAccess {
    Logical(LogicalReadRequest),
    Pushdown {
        node: LogicalNodeId,
        request: Box<PushdownRequest>,
        guarantee: PushdownGuarantee,
        residual: Option<PhysicalExpr>,
    },
}

impl StorageAccess {
    pub const fn node(&self) -> LogicalNodeId {
        match self {
            Self::Logical(request) => request.node(),
            Self::Pushdown { node, .. } => *node,
        }
    }
}
