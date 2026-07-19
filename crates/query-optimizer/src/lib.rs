#![forbid(unsafe_code)]

use std::error::Error;
use std::fmt::{self, Display, Formatter};

use physical_plan::{
    ExchangeKind, JoinKind, MemoryBudget, PhysicalOperator, PhysicalPlan, PhysicalPlanBuilder,
    PhysicalPlanHeaderV1, Placement, WriteOperation,
};
use temporal_ir::v2::{LogicalNode, LogicalOperator, LogicalPlan};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeploymentMode {
    PrimaryReplica,
    SharedNothing,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OptimizerContext {
    mode: DeploymentMode,
    shard_count: u32,
    primary_shard_id: u32,
    memory_bytes: u64,
    spill_bytes: u64,
}

impl OptimizerContext {
    pub fn new(
        mode: DeploymentMode,
        shard_count: u32,
        memory_bytes: u64,
        spill_bytes: u64,
    ) -> Result<Self, OptimizerError> {
        if shard_count == 0 || memory_bytes == 0 || spill_bytes == 0 {
            return Err(OptimizerError::InvalidContext);
        }
        if mode == DeploymentMode::PrimaryReplica && shard_count != 1 {
            return Err(OptimizerError::InvalidContext);
        }
        Ok(Self {
            mode,
            shard_count,
            primary_shard_id: 0,
            memory_bytes,
            spill_bytes,
        })
    }

    #[must_use]
    pub const fn with_primary_shard(mut self, shard_id: u32) -> Self {
        self.primary_shard_id = shard_id;
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TraceEvent {
    rule: &'static str,
    detail: String,
}

impl TraceEvent {
    #[must_use]
    pub fn rule(&self) -> &str {
        self.rule
    }

    #[must_use]
    pub fn detail(&self) -> &str {
        &self.detail
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OptimizedPlan {
    plan: PhysicalPlan,
    trace: Vec<TraceEvent>,
}

impl OptimizedPlan {
    #[must_use]
    pub const fn plan(&self) -> &PhysicalPlan {
        &self.plan
    }

    #[must_use]
    pub fn trace(&self) -> &[TraceEvent] {
        &self.trace
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct Optimizer;

impl Optimizer {
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    pub fn optimize(
        &self,
        logical: &LogicalPlan,
        context: OptimizerContext,
    ) -> Result<OptimizedPlan, OptimizerError> {
        logical
            .validate()
            .map_err(|error| OptimizerError::Logical(error.to_string()))?;
        let header = PhysicalPlanHeaderV1::new(
            logical.header().graph_id(),
            logical.header().schema_version(),
            logical.header().topology_epoch(),
            logical.header().query_fingerprint(),
        )
        .map_err(|error| OptimizerError::Physical(error.to_string()))?;
        let budget = MemoryBudget::new(context.memory_bytes, context.spill_bytes)
            .map_err(|error| OptimizerError::Physical(error.to_string()))?;
        match context.mode {
            DeploymentMode::PrimaryReplica => {
                optimize_primary(logical, header, budget, context.primary_shard_id)
            }
            DeploymentMode::SharedNothing => optimize_shared(logical, header, budget, context),
        }
    }
}

fn optimize_primary(
    logical: &LogicalPlan,
    header: PhysicalPlanHeaderV1,
    budget: MemoryBudget,
    primary_shard_id: u32,
) -> Result<OptimizedPlan, OptimizerError> {
    let operators = logical.nodes().iter().map(physical).collect();
    let mut builder = PhysicalPlanBuilder::new(header);
    let root = builder
        .add_fragment(
            Placement::Shard(primary_shard_id),
            operators,
            logical.output().clone(),
            budget,
        )
        .map_err(|error| OptimizerError::Physical(error.to_string()))?;
    let plan = builder
        .finish(root)
        .map_err(|error| OptimizerError::Physical(error.to_string()))?;
    Ok(OptimizedPlan {
        plan,
        trace: vec![TraceEvent {
            rule: "primary-replica-local-plan",
            detail: "all operators execute on the primary shard".into(),
        }],
    })
}

fn optimize_shared(
    logical: &LogicalPlan,
    header: PhysicalPlanHeaderV1,
    budget: MemoryBudget,
    context: OptimizerContext,
) -> Result<OptimizedPlan, OptimizerError> {
    let split = logical
        .nodes()
        .iter()
        .position(|node| matches!(node.operator(), LogicalOperator::Project { .. }))
        .unwrap_or(logical.nodes().len());
    if split == 0 {
        return optimize_coordinator_only(logical, header, budget);
    }
    let shard_operators = logical.nodes()[..split]
        .iter()
        .map(physical)
        .collect::<Vec<_>>();
    let shard_output = logical.nodes()[split - 1].output().clone();
    let coordinator_operators = if split < logical.nodes().len() {
        logical.nodes()[split..]
            .iter()
            .map(physical)
            .collect::<Vec<_>>()
    } else {
        vec![PhysicalOperator::Finish]
    };
    let mut builder = PhysicalPlanBuilder::new(header);
    let shard = builder
        .add_fragment(
            Placement::AllShards,
            shard_operators,
            shard_output.clone(),
            budget,
        )
        .map_err(|error| OptimizerError::Physical(error.to_string()))?;
    let coordinator = builder
        .add_fragment(
            Placement::Coordinator,
            coordinator_operators,
            logical.output().clone(),
            budget,
        )
        .map_err(|error| OptimizerError::Physical(error.to_string()))?;
    builder
        .add_exchange(shard, coordinator, ExchangeKind::Gather, shard_output, 8)
        .map_err(|error| OptimizerError::Physical(error.to_string()))?;
    let plan = builder
        .finish(coordinator)
        .map_err(|error| OptimizerError::Physical(error.to_string()))?;
    Ok(OptimizedPlan {
        plan,
        trace: vec![TraceEvent {
            rule: "partition-local-graph-operators",
            detail: format!(
                "scan/expand/filter run on {} shards before bounded gather",
                context.shard_count
            ),
        }],
    })
}

fn optimize_coordinator_only(
    logical: &LogicalPlan,
    header: PhysicalPlanHeaderV1,
    budget: MemoryBudget,
) -> Result<OptimizedPlan, OptimizerError> {
    let mut builder = PhysicalPlanBuilder::new(header);
    let root = builder
        .add_fragment(
            Placement::Coordinator,
            logical.nodes().iter().map(physical).collect(),
            logical.output().clone(),
            budget,
        )
        .map_err(|error| OptimizerError::Physical(error.to_string()))?;
    let plan = builder
        .finish(root)
        .map_err(|error| OptimizerError::Physical(error.to_string()))?;
    Ok(OptimizedPlan {
        plan,
        trace: vec![TraceEvent {
            rule: "coordinator-only-plan",
            detail: "plan has no partition-local prefix".into(),
        }],
    })
}

fn physical(node: &LogicalNode) -> PhysicalOperator {
    let operator = node.operator();
    match operator {
        LogicalOperator::Argument => PhysicalOperator::Argument,
        LogicalOperator::NodeScan { binding, labels } => PhysicalOperator::NodeScan {
            binding: *binding,
            labels: labels.clone(),
            output: node.output().clone(),
        },
        LogicalOperator::RelationshipScan { binding, types } => {
            PhysicalOperator::RelationshipScan {
                binding: *binding,
                types: types.clone(),
                output: node.output().clone(),
            }
        }
        LogicalOperator::Expand {
            source,
            relationship,
            destination,
            outgoing,
            types,
        } => PhysicalOperator::Expand {
            source: *source,
            relationship: *relationship,
            destination: *destination,
            outgoing: *outgoing,
            types: types.clone(),
            output: node.output().clone(),
        },
        LogicalOperator::Filter { predicate } => PhysicalOperator::Filter(predicate.clone()),
        LogicalOperator::Project { expressions } => PhysicalOperator::Project {
            expressions: expressions.clone(),
        },
        LogicalOperator::Aggregate {
            grouping,
            aggregates,
        } => PhysicalOperator::Aggregate {
            grouping: grouping.clone(),
            aggregates: aggregates.clone(),
        },
        LogicalOperator::Sort { keys } => PhysicalOperator::Sort { keys: keys.clone() },
        LogicalOperator::Skip { count } => PhysicalOperator::Skip {
            count: count.clone(),
        },
        LogicalOperator::Limit { count } => PhysicalOperator::Limit {
            count: count.clone(),
        },
        LogicalOperator::InnerJoin => PhysicalOperator::HashJoin {
            kind: JoinKind::Inner,
            keys: Vec::new(),
        },
        LogicalOperator::LeftJoin => PhysicalOperator::HashJoin {
            kind: JoinKind::Left,
            keys: Vec::new(),
        },
        LogicalOperator::Union { all } => PhysicalOperator::Union { all: *all },
        LogicalOperator::TemporalSlice {
            valid_time,
            transaction_time,
        } => PhysicalOperator::TemporalSlice {
            valid_time: valid_time.clone(),
            transaction_time: transaction_time.clone(),
        },
        LogicalOperator::Diff => PhysicalOperator::Diff,
        LogicalOperator::ProcedureCall { procedure_id } => PhysicalOperator::Procedure {
            procedure_id: *procedure_id,
        },
        LogicalOperator::Finish => PhysicalOperator::Finish,
        LogicalOperator::Create => PhysicalOperator::Write {
            operation: WriteOperation::Create,
        },
        LogicalOperator::Merge => PhysicalOperator::Write {
            operation: WriteOperation::Merge,
        },
        LogicalOperator::Set => PhysicalOperator::Write {
            operation: WriteOperation::Set,
        },
        LogicalOperator::Remove => PhysicalOperator::Write {
            operation: WriteOperation::Remove,
        },
        LogicalOperator::Delete { detach } => PhysicalOperator::Write {
            operation: WriteOperation::Delete { detach: *detach },
        },
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OptimizerError {
    InvalidContext,
    Logical(String),
    Physical(String),
}

impl Display for OptimizerError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "query optimization failed: {self:?}")
    }
}

impl Error for OptimizerError {}
