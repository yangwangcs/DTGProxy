use dtg_language_ir::{LogicalExpr, LogicalNodeId, RowSchema};
use dtg_storage::{PushdownRequest, Version};

use crate::{Exchange, LogicalReadRequest, PlanFragment, PushdownGuarantee, SemanticRequirements};

pub const PHYSICAL_PLAN_VERSION: Version = Version::new(1);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalPlan {
    pub version: Version,
    pub fragments: Vec<PlanFragment>,
    pub exchanges: Vec<Exchange>,
    pub result_schema: RowSchema,
}

impl PhysicalPlan {
    pub fn fragments(&self) -> &[PlanFragment] {
        &self.fragments
    }

    pub fn exchanges(&self) -> &[Exchange] {
        &self.exchanges
    }

    pub const fn result_schema(&self) -> &RowSchema {
        &self.result_schema
    }
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
        request: Box<PushdownRequest>,
        guarantee: PushdownGuarantee,
        residual: Option<PhysicalExpr>,
    },
}
