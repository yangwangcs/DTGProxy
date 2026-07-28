#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};

pub use physical_plan::AccessGuarantee;
use physical_plan::{
    AggregatePhase, COUNT_AGGREGATE_FUNCTION_ID, ExchangeKind, JoinKind, MemoryBudget,
    PhysicalAccess, PhysicalApply, PhysicalComparisonOperator, PhysicalOperator, PhysicalPlan,
    PhysicalPlanBuilder, PhysicalPlanHeader, PhysicalPropertyConstraint, Placement, PrimitiveKind,
    ResidualPolicy, WriteOperation,
};
use temporal_ir::{
    Column, LogicalNode, LogicalNodeId, LogicalOperator, LogicalPlan, RowSchema, ScalarExpr,
    SlotId, TransactionTimeSpec, ValidTimeSpec, ValueType,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeploymentMode {
    PrimaryReplica,
    SharedNothing,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CapabilitySnapshot {
    generation: u64,
    candidate_scan: AccessGuarantee,
    adjacency_expand: AccessGuarantee,
    change_scan: AccessGuarantee,
}

impl CapabilitySnapshot {
    pub const GENERIC: Self = Self {
        generation: 1,
        candidate_scan: AccessGuarantee::Unsupported,
        adjacency_expand: AccessGuarantee::Unsupported,
        change_scan: AccessGuarantee::Unsupported,
    };

    pub fn new(
        generation: u64,
        candidate_scan: AccessGuarantee,
        adjacency_expand: AccessGuarantee,
        change_scan: AccessGuarantee,
    ) -> Result<Self, OptimizerError> {
        if generation == 0 {
            return Err(OptimizerError::InvalidCapabilityGeneration);
        }
        Ok(Self {
            generation,
            candidate_scan,
            adjacency_expand,
            change_scan,
        })
    }

    #[must_use]
    pub const fn generation(self) -> u64 {
        self.generation
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OptimizerContext {
    mode: DeploymentMode,
    shard_count: u32,
    primary_shard_id: u32,
    memory_bytes: u64,
    spill_bytes: u64,
    shard_ids: Vec<u32>,
    capabilities: CapabilitySnapshot,
    current_projection_candidate_scan: bool,
    current_transaction_scope: bool,
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
            shard_ids: (0..shard_count).collect(),
            capabilities: CapabilitySnapshot::GENERIC,
            current_projection_candidate_scan: false,
            current_transaction_scope: false,
        })
    }

    #[must_use]
    pub const fn with_current_projection_candidate_scan(mut self, enabled: bool) -> Self {
        self.current_projection_candidate_scan = enabled;
        self
    }

    #[must_use]
    pub const fn with_primary_shard(mut self, shard_id: u32) -> Self {
        self.primary_shard_id = shard_id;
        self
    }

    pub fn with_shard_ids(mut self, shard_ids: Vec<u32>) -> Result<Self, OptimizerError> {
        let unique = shard_ids
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        if shard_ids.len() != usize::try_from(self.shard_count).unwrap_or(usize::MAX)
            || unique.len() != shard_ids.len()
        {
            return Err(OptimizerError::InvalidContext);
        }
        self.shard_ids = shard_ids;
        Ok(self)
    }

    #[must_use]
    pub const fn with_capability_snapshot(mut self, capabilities: CapabilitySnapshot) -> Self {
        self.capabilities = capabilities;
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
        mut context: OptimizerContext,
    ) -> Result<OptimizedPlan, OptimizerError> {
        logical
            .validate()
            .map_err(|error| OptimizerError::Logical(error.to_string()))?;
        context.current_transaction_scope = context.current_projection_candidate_scan
            && !logical
                .nodes()
                .iter()
                .any(|node| matches!(node.operator(), LogicalOperator::ChangeScan { .. }))
            && logical
                .nodes()
                .iter()
                .filter_map(|node| match node.operator() {
                    LogicalOperator::TemporalSlice {
                        transaction_time, ..
                    } => Some(transaction_time),
                    _ => None,
                })
                .all(|transaction_time| matches!(transaction_time, TransactionTimeSpec::Current));
        let header = PhysicalPlanHeader::new(
            logical.header().graph_id(),
            logical.header().schema_version(),
            logical.header().topology_epoch(),
            logical.header().query_fingerprint(),
        )
        .map_err(|error| OptimizerError::Physical(error.to_string()))?
        .with_capability_generation(context.capabilities.generation())
        .map_err(|error| OptimizerError::Physical(error.to_string()))?
        .with_expected_shards(context.shard_ids.clone())
        .map_err(|error| OptimizerError::Physical(error.to_string()))?;
        let budget = MemoryBudget::new(context.memory_bytes, context.spill_bytes)
            .map_err(|error| OptimizerError::Physical(error.to_string()))?;
        if matches!(
            logical_root(logical)?.operator(),
            LogicalOperator::Union { .. }
                | LogicalOperator::InnerJoin
                | LogicalOperator::LeftJoin
                | LogicalOperator::TemporalJoin { .. }
        ) || subtree_contains_multi_input(logical, logical.root())?
        {
            return optimize_union(logical, header, budget, &context);
        }
        if !has_graph_source(logical) {
            return optimize_coordinator_only(logical, header, budget, &context);
        }
        if logical.nodes().iter().any(|node| {
            matches!(
                node.operator(),
                LogicalOperator::ProcedureCall { procedure }
                    if procedure.placement() == temporal_ir::ProcedurePlacement::Coordinator
            )
        }) {
            let mut builder = PhysicalPlanBuilder::new(header);
            let (root, _) =
                build_linear_branch(&mut builder, logical, logical.root(), budget, &context)?;
            let plan = builder
                .finish(root)
                .map_err(|error| OptimizerError::Physical(error.to_string()))?;
            return Ok(OptimizedPlan {
                plan,
                trace: vec![TraceEvent {
                    rule: "coordinator-procedure-boundary",
                    detail: "global procedures execute once over gathered coordinator rows".into(),
                }],
            });
        }
        if logical.nodes().iter().any(|node| {
            matches!(
                node.operator(),
                LogicalOperator::Apply { .. } | LogicalOperator::BatchSubtransaction { .. }
            )
        }) {
            let mut builder = PhysicalPlanBuilder::new(header);
            let (root, _) =
                build_linear_branch(&mut builder, logical, logical.root(), budget, &context)?;
            let plan = builder
                .finish(root)
                .map_err(|error| OptimizerError::Physical(error.to_string()))?;
            return Ok(OptimizedPlan {
                plan,
                trace: vec![TraceEvent {
                    rule: "coordinator-apply-boundary",
                    detail: "graph parent rows gather before transport-neutral child invocation"
                        .into(),
                }],
            });
        }
        match context.mode {
            DeploymentMode::PrimaryReplica => optimize_primary(logical, header, budget, &context),
            DeploymentMode::SharedNothing => optimize_shared(logical, header, budget, &context),
        }
    }
}

trait CapabilityAwareBuilder {
    fn add_capability_fragment(
        &mut self,
        placement: Placement,
        operators: Vec<PhysicalOperator>,
        output: RowSchema,
        budget: MemoryBudget,
        context: &OptimizerContext,
    ) -> Result<physical_plan::FragmentId, physical_plan::ValidationError>;
}

impl CapabilityAwareBuilder for PhysicalPlanBuilder {
    fn add_capability_fragment(
        &mut self,
        placement: Placement,
        operators: Vec<PhysicalOperator>,
        output: RowSchema,
        budget: MemoryBudget,
        context: &OptimizerContext,
    ) -> Result<physical_plan::FragmentId, physical_plan::ValidationError> {
        let access = operators
            .iter()
            .enumerate()
            .map(|(index, operator)| {
                access_for(operator, context, candidate_constraints(&operators, index))
            })
            .collect();
        self.add_fragment_with_access(placement, operators, access, output, budget)
    }
}

fn access_for(
    operator: &PhysicalOperator,
    context: &OptimizerContext,
    constraints: Vec<PhysicalPropertyConstraint>,
) -> PhysicalAccess {
    let (primitive, mut guarantee) = match operator {
        PhysicalOperator::NodeScan { .. } if context.current_transaction_scope => (
            PrimitiveKind::CandidateScan,
            context.capabilities.candidate_scan,
        ),
        PhysicalOperator::NodeScan { .. } => return PhysicalAccess::Generic,
        PhysicalOperator::RelationshipScan { .. } => return PhysicalAccess::Generic,
        PhysicalOperator::Expand { .. } => (
            PrimitiveKind::AdjacencyExpand,
            context.capabilities.adjacency_expand,
        ),
        PhysicalOperator::ChangeScan { .. } => {
            (PrimitiveKind::ChangeScan, context.capabilities.change_scan)
        }
        _ => return PhysicalAccess::Generic,
    };
    if primitive == PrimitiveKind::CandidateScan
        && !constraints.is_empty()
        && guarantee == AccessGuarantee::Exact
    {
        guarantee = AccessGuarantee::Candidate;
    }
    match guarantee {
        AccessGuarantee::Unsupported => PhysicalAccess::Generic,
        AccessGuarantee::Candidate | AccessGuarantee::Exact => PhysicalAccess::Primitive {
            primitive,
            guarantee,
            residual: ResidualPolicy::Evaluate,
            constraints,
        },
    }
}

fn candidate_constraints(
    operators: &[PhysicalOperator],
    operator_index: usize,
) -> Vec<PhysicalPropertyConstraint> {
    let Some(PhysicalOperator::NodeScan { binding, .. }) = operators.get(operator_index) else {
        return Vec::new();
    };
    let Some(PhysicalOperator::Filter(predicate)) = operators.get(operator_index + 1) else {
        return Vec::new();
    };
    let mut constraints = Vec::new();
    collect_property_constraints(predicate, *binding, &mut constraints);
    constraints
}

fn collect_property_constraints(
    expression: &ScalarExpr,
    binding: SlotId,
    constraints: &mut Vec<PhysicalPropertyConstraint>,
) {
    if let ScalarExpr::And(left, right) = expression {
        collect_property_constraints(left, binding, constraints);
        collect_property_constraints(right, binding, constraints);
        return;
    }
    let comparison = match expression {
        ScalarExpr::Equal(left, right) => {
            extract_property_constraint(left, right, binding, PhysicalComparisonOperator::Equal)
        }
        ScalarExpr::NotEqual(left, right) => {
            extract_property_constraint(left, right, binding, PhysicalComparisonOperator::NotEqual)
        }
        ScalarExpr::Less(left, right) => {
            extract_property_constraint(left, right, binding, PhysicalComparisonOperator::LessThan)
        }
        ScalarExpr::LessEqual(left, right) => extract_property_constraint(
            left,
            right,
            binding,
            PhysicalComparisonOperator::LessThanOrEqual,
        ),
        ScalarExpr::Greater(left, right) => extract_property_constraint(
            left,
            right,
            binding,
            PhysicalComparisonOperator::GreaterThan,
        ),
        ScalarExpr::GreaterEqual(left, right) => extract_property_constraint(
            left,
            right,
            binding,
            PhysicalComparisonOperator::GreaterThanOrEqual,
        ),
        _ => None,
    };
    if let Some(constraint) = comparison {
        constraints.push(constraint);
    }
}

fn extract_property_constraint(
    left: &ScalarExpr,
    right: &ScalarExpr,
    binding: SlotId,
    operator: PhysicalComparisonOperator,
) -> Option<PhysicalPropertyConstraint> {
    if let (Some(property_id), ScalarExpr::Literal(value)) = (bound_property(left, binding), right)
    {
        return Some(PhysicalPropertyConstraint::new(
            property_id,
            operator,
            value.clone(),
        ));
    }
    if let (ScalarExpr::Literal(value), Some(property_id)) = (left, bound_property(right, binding))
    {
        return Some(PhysicalPropertyConstraint::new(
            property_id,
            reverse_comparison(operator),
            value.clone(),
        ));
    }
    None
}

fn bound_property(expression: &ScalarExpr, binding: SlotId) -> Option<u32> {
    match expression {
        ScalarExpr::Property { value, property_id } if matches!(value.as_ref(), ScalarExpr::Slot(slot) if *slot == binding) => {
            Some(*property_id)
        }
        _ => None,
    }
}

const fn reverse_comparison(operator: PhysicalComparisonOperator) -> PhysicalComparisonOperator {
    match operator {
        PhysicalComparisonOperator::Equal => PhysicalComparisonOperator::Equal,
        PhysicalComparisonOperator::NotEqual => PhysicalComparisonOperator::NotEqual,
        PhysicalComparisonOperator::LessThan => PhysicalComparisonOperator::GreaterThan,
        PhysicalComparisonOperator::LessThanOrEqual => {
            PhysicalComparisonOperator::GreaterThanOrEqual
        }
        PhysicalComparisonOperator::GreaterThan => PhysicalComparisonOperator::LessThan,
        PhysicalComparisonOperator::GreaterThanOrEqual => {
            PhysicalComparisonOperator::LessThanOrEqual
        }
    }
}

fn optimize_union(
    logical: &LogicalPlan,
    header: PhysicalPlanHeader,
    budget: MemoryBudget,
    context: &OptimizerContext,
) -> Result<OptimizedPlan, OptimizerError> {
    let mut builder = PhysicalPlanBuilder::new(header);
    let (root, _) = build_union_dag(
        &mut builder,
        logical,
        logical.root(),
        budget,
        context,
        &mut BTreeMap::new(),
    )?;
    let plan = builder
        .finish(root)
        .map_err(|error| OptimizerError::Physical(error.to_string()))?;
    Ok(OptimizedPlan {
        plan,
        trace: vec![TraceEvent {
            rule: "union-boundary-dag",
            detail:
                "each UNION boundary remains an ordered two-input coordinator fragment and each branch is independently placed"
                    .into(),
        }],
    })
}

fn build_union_dag(
    builder: &mut PhysicalPlanBuilder,
    logical: &LogicalPlan,
    root: LogicalNodeId,
    budget: MemoryBudget,
    context: &OptimizerContext,
    fragments: &mut BTreeMap<LogicalNodeId, (physical_plan::FragmentId, RowSchema)>,
) -> Result<(physical_plan::FragmentId, RowSchema), OptimizerError> {
    if let Some(fragment) = fragments.get(&root) {
        return Ok(fragment.clone());
    }
    let result = build_union_node(builder, logical, root, budget, context, fragments)?;
    fragments.insert(root, result.clone());
    Ok(result)
}

fn build_union_node(
    builder: &mut PhysicalPlanBuilder,
    logical: &LogicalPlan,
    root: LogicalNodeId,
    budget: MemoryBudget,
    context: &OptimizerContext,
    fragments: &mut BTreeMap<LogicalNodeId, (physical_plan::FragmentId, RowSchema)>,
) -> Result<(physical_plan::FragmentId, RowSchema), OptimizerError> {
    let node = logical
        .nodes()
        .get(usize::try_from(root.value()).unwrap_or(usize::MAX))
        .ok_or_else(|| OptimizerError::Logical("UNION subtree root is missing".into()))?;
    if let LogicalOperator::Union { all } = node.operator() {
        let [left, right] = node.inputs() else {
            return Err(OptimizerError::UnsupportedUnionShape);
        };
        let (left_fragment, left_schema) =
            build_union_dag(builder, logical, *left, budget, context, fragments)?;
        let (right_fragment, right_schema) =
            build_union_dag(builder, logical, *right, budget, context, fragments)?;
        if left_schema != right_schema || left_schema != *node.output() {
            return Err(OptimizerError::UnsupportedUnionShape);
        }
        let coordinator = builder
            .add_capability_fragment(
                Placement::Coordinator,
                vec![PhysicalOperator::Union { all: *all }],
                node.output().clone(),
                budget,
                context,
            )
            .map_err(|error| OptimizerError::Physical(error.to_string()))?;
        for (from, schema) in [(left_fragment, left_schema), (right_fragment, right_schema)] {
            builder
                .add_exchange(from, coordinator, ExchangeKind::Gather, schema, 8)
                .map_err(|error| OptimizerError::Physical(error.to_string()))?;
        }
        return Ok((coordinator, node.output().clone()));
    }
    if matches!(
        node.operator(),
        LogicalOperator::InnerJoin
            | LogicalOperator::LeftJoin
            | LogicalOperator::TemporalJoin { .. }
    ) {
        let [left, right] = node.inputs() else {
            return Err(OptimizerError::UnsupportedUnionShape);
        };
        let (left_fragment, left_schema) =
            build_union_dag(builder, logical, *left, budget, context, fragments)?;
        let (right_fragment, right_schema) =
            build_union_dag(builder, logical, *right, budget, context, fragments)?;
        let coordinator = builder
            .add_capability_fragment(
                Placement::Coordinator,
                vec![PhysicalOperator::HashJoin {
                    kind: match node.operator() {
                        LogicalOperator::LeftJoin
                        | LogicalOperator::TemporalJoin {
                            kind: temporal_ir::TemporalJoinKind::Left,
                            ..
                        } => JoinKind::Left,
                        _ => JoinKind::Inner,
                    },
                    keys: match node.operator() {
                        LogicalOperator::TemporalJoin { keys, .. } => keys.clone(),
                        _ => Vec::new(),
                    },
                }],
                node.output().clone(),
                budget,
                context,
            )
            .map_err(|error| OptimizerError::Physical(error.to_string()))?;
        builder
            .add_exchange(
                left_fragment,
                coordinator,
                ExchangeKind::Gather,
                left_schema,
                8,
            )
            .map_err(|error| OptimizerError::Physical(error.to_string()))?;
        builder
            .add_exchange(
                right_fragment,
                coordinator,
                ExchangeKind::Gather,
                right_schema,
                8,
            )
            .map_err(|error| OptimizerError::Physical(error.to_string()))?;
        return Ok((coordinator, node.output().clone()));
    }
    if let [input] = node.inputs()
        && (subtree_contains_multi_input(logical, *input)?
            || subtree_contains_argument(logical, *input)?)
    {
        let (source, schema) =
            build_union_dag(builder, logical, *input, budget, context, fragments)?;
        let coordinator = builder
            .add_capability_fragment(
                Placement::Coordinator,
                vec![physical(node, context)?],
                node.output().clone(),
                budget,
                context,
            )
            .map_err(|error| OptimizerError::Physical(error.to_string()))?;
        builder
            .add_exchange(source, coordinator, ExchangeKind::Gather, schema, 8)
            .map_err(|error| OptimizerError::Physical(error.to_string()))?;
        return Ok((coordinator, node.output().clone()));
    }
    build_linear_branch(builder, logical, root, budget, context)
}

fn build_linear_branch(
    builder: &mut PhysicalPlanBuilder,
    logical: &LogicalPlan,
    root: LogicalNodeId,
    budget: MemoryBudget,
    context: &OptimizerContext,
) -> Result<(physical_plan::FragmentId, RowSchema), OptimizerError> {
    let nodes = linear_branch(logical, root)?;
    let output = nodes
        .last()
        .ok_or(OptimizerError::UnsupportedUnionShape)?
        .output()
        .clone();
    let has_graph_source = nodes.iter().any(|node| {
        matches!(
            node.operator(),
            LogicalOperator::NodeScan { .. }
                | LogicalOperator::RelationshipScan { .. }
                | LogicalOperator::Expand { .. }
        )
    });
    if !has_graph_source {
        let fragment = builder
            .add_capability_fragment(
                Placement::Coordinator,
                nodes
                    .into_iter()
                    .map(|node| physical(node, context))
                    .collect::<Result<Vec<_>, _>>()?,
                output.clone(),
                budget,
                context,
            )
            .map_err(|error| OptimizerError::Physical(error.to_string()))?;
        return Ok((fragment, output));
    }
    let split = nodes
        .iter()
        .position(|node| is_global_pipeline_operator(node.operator()))
        .unwrap_or(nodes.len());
    if context.mode == DeploymentMode::PrimaryReplica && split == nodes.len() {
        let fragment = builder
            .add_capability_fragment(
                Placement::Shard(context.primary_shard_id),
                nodes
                    .into_iter()
                    .map(|node| physical(node, context))
                    .collect::<Result<Vec<_>, _>>()?,
                output.clone(),
                budget,
                context,
            )
            .map_err(|error| OptimizerError::Physical(error.to_string()))?;
        return Ok((fragment, output));
    }
    if split == 0 {
        return Err(OptimizerError::UnsupportedUnionShape);
    }
    let shard_output = nodes[split - 1].output().clone();
    let shard = builder
        .add_capability_fragment(
            Placement::AllShards,
            nodes[..split]
                .iter()
                .map(|node| physical(node, context))
                .collect::<Result<Vec<_>, _>>()?,
            shard_output.clone(),
            budget,
            context,
        )
        .map_err(|error| OptimizerError::Physical(error.to_string()))?;
    if split == nodes.len() {
        return Ok((shard, shard_output));
    }
    let coordinator = builder
        .add_capability_fragment(
            Placement::Coordinator,
            nodes[split..]
                .iter()
                .map(|node| physical(node, context))
                .collect::<Result<Vec<_>, _>>()?,
            output.clone(),
            budget,
            context,
        )
        .map_err(|error| OptimizerError::Physical(error.to_string()))?;
    builder
        .add_exchange(shard, coordinator, ExchangeKind::Gather, shard_output, 8)
        .map_err(|error| OptimizerError::Physical(error.to_string()))?;
    Ok((coordinator, output))
}

fn subtree_contains_multi_input(
    logical: &LogicalPlan,
    root: LogicalNodeId,
) -> Result<bool, OptimizerError> {
    let node = logical
        .nodes()
        .get(usize::try_from(root.value()).unwrap_or(usize::MAX))
        .ok_or_else(|| OptimizerError::Logical("logical subtree node is missing".into()))?;
    if node.inputs().len() > 1 {
        return Ok(true);
    }
    for input in node.inputs() {
        if subtree_contains_multi_input(logical, *input)? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn subtree_contains_argument(
    logical: &LogicalPlan,
    root: LogicalNodeId,
) -> Result<bool, OptimizerError> {
    let node = logical
        .nodes()
        .get(usize::try_from(root.value()).unwrap_or(usize::MAX))
        .ok_or_else(|| OptimizerError::Logical("logical subtree node is missing".into()))?;
    if matches!(node.operator(), LogicalOperator::Argument) {
        return Ok(true);
    }
    for input in node.inputs() {
        if subtree_contains_argument(logical, *input)? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn is_global_pipeline_operator(operator: &LogicalOperator) -> bool {
    matches!(
        operator,
        LogicalOperator::Project { .. }
            | LogicalOperator::Aggregate { .. }
            | LogicalOperator::Sort { .. }
            | LogicalOperator::Skip { .. }
            | LogicalOperator::Limit { .. }
            | LogicalOperator::Apply { .. }
            | LogicalOperator::BatchSubtransaction { .. }
    ) || matches!(
        operator,
        LogicalOperator::ProcedureCall { procedure }
            if procedure.placement() == temporal_ir::ProcedurePlacement::Coordinator
    )
}

fn logical_root(logical: &LogicalPlan) -> Result<&LogicalNode, OptimizerError> {
    logical
        .nodes()
        .get(usize::try_from(logical.root().value()).unwrap_or(usize::MAX))
        .ok_or(OptimizerError::Logical("logical root is missing".into()))
}

fn linear_branch(
    logical: &LogicalPlan,
    root: LogicalNodeId,
) -> Result<Vec<&LogicalNode>, OptimizerError> {
    let mut nodes = Vec::new();
    let mut current = root;
    loop {
        let node = logical
            .nodes()
            .get(usize::try_from(current.value()).unwrap_or(usize::MAX))
            .ok_or_else(|| OptimizerError::Logical("UNION input is missing".into()))?;
        if node.inputs().len() > 1 || matches!(node.operator(), LogicalOperator::Union { .. }) {
            return Err(OptimizerError::UnsupportedUnionShape);
        }
        nodes.push(node);
        let Some(input) = node.inputs().first() else {
            break;
        };
        current = *input;
    }
    nodes.reverse();
    Ok(nodes)
}

fn optimize_primary(
    logical: &LogicalPlan,
    header: PhysicalPlanHeader,
    budget: MemoryBudget,
    context: &OptimizerContext,
) -> Result<OptimizedPlan, OptimizerError> {
    let operators = logical
        .nodes()
        .iter()
        .map(|node| physical(node, context))
        .collect::<Result<Vec<_>, _>>()?;
    let mut builder = PhysicalPlanBuilder::new(header);
    let root = builder
        .add_capability_fragment(
            Placement::Shard(context.primary_shard_id),
            operators,
            logical.output().clone(),
            budget,
            context,
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
    header: PhysicalPlanHeader,
    budget: MemoryBudget,
    context: &OptimizerContext,
) -> Result<OptimizedPlan, OptimizerError> {
    let split = logical
        .nodes()
        .iter()
        .position(|node| {
            matches!(
                node.operator(),
                LogicalOperator::Project { .. }
                    | LogicalOperator::Unwind { .. }
                    | LogicalOperator::Aggregate { .. }
                    | LogicalOperator::Sort { .. }
                    | LogicalOperator::Skip { .. }
                    | LogicalOperator::Limit { .. }
                    | LogicalOperator::Apply { .. }
                    | LogicalOperator::BatchSubtransaction { .. }
            )
        })
        .unwrap_or(logical.nodes().len());
    if split == 0 {
        return optimize_coordinator_only(logical, header, budget, context);
    }
    if is_safe_global_count(logical, split) {
        return optimize_shared_count(logical, header, budget, context, split);
    }
    if is_safe_exact_top_k(logical, split) {
        return optimize_shared_top_k(logical, header, budget, context, split);
    }
    let shard_operators = logical.nodes()[..split]
        .iter()
        .map(|node| physical(node, context))
        .collect::<Result<Vec<_>, _>>()?;
    let shard_output = logical.nodes()[split - 1].output().clone();
    let coordinator_operators = if split < logical.nodes().len() {
        logical.nodes()[split..]
            .iter()
            .map(|node| physical(node, context))
            .collect::<Result<Vec<_>, _>>()?
    } else {
        vec![PhysicalOperator::Finish]
    };
    let mut builder = PhysicalPlanBuilder::new(header);
    let shard = builder
        .add_capability_fragment(
            Placement::AllShards,
            shard_operators,
            shard_output.clone(),
            budget,
            context,
        )
        .map_err(|error| OptimizerError::Physical(error.to_string()))?;
    let coordinator = builder
        .add_capability_fragment(
            Placement::Coordinator,
            coordinator_operators,
            logical.output().clone(),
            budget,
            context,
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

const MAX_EXACT_DISTRIBUTED_TOP_K_ROWS: i64 = 4096;

fn is_safe_exact_top_k(logical: &LogicalPlan, project_index: usize) -> bool {
    let nodes = logical.nodes();
    let suffix = nodes.get(project_index..).unwrap_or_default();
    let (project, sort, limit, final_project) = match suffix {
        [project, sort, limit] => (project, sort, limit, None),
        [project, sort, limit, final_project] => (project, sort, limit, Some(final_project)),
        _ => return false,
    };
    let LogicalOperator::Project { expressions } = project.operator() else {
        return false;
    };
    let LogicalOperator::Sort { keys } = sort.operator() else {
        return false;
    };
    let LogicalOperator::Limit {
        count: ScalarExpr::Literal(temporal_types::GraphValue::Integer(count)),
    } = limit.operator()
    else {
        return false;
    };
    if !(1..=MAX_EXACT_DISTRIBUTED_TOP_K_ROWS).contains(count)
        || keys.is_empty()
        || nodes[..project_index].iter().any(|node| {
            node.inputs().len() > 1
                || matches!(
                    node.operator(),
                    LogicalOperator::ChangeScan { .. } | LogicalOperator::Skip { .. }
                )
        })
    {
        return false;
    }

    let output_slots = project
        .output()
        .columns()
        .iter()
        .map(Column::slot)
        .collect::<Vec<_>>();
    let final_project_is_safe = final_project.is_none_or(|node| {
        let LogicalOperator::Project { expressions } = node.operator() else {
            return false;
        };
        expressions.len() == node.output().columns().len()
            && expressions.iter().all(|(_, expression)| {
                matches!(expression, ScalarExpr::Slot(slot) if output_slots.contains(slot))
            })
    });

    output_slots.len() == expressions.len()
        && output_slots
            .iter()
            .all(|slot| expressions.iter().any(|(projected, _)| projected == slot))
        && output_slots
            .iter()
            .all(|slot| keys.iter().any(|key| key.slot() == *slot))
        && sort.output() == project.output()
        && limit.output() == project.output()
        && final_project_is_safe
}

fn optimize_shared_top_k(
    logical: &LogicalPlan,
    header: PhysicalPlanHeader,
    budget: MemoryBudget,
    context: &OptimizerContext,
    project_index: usize,
) -> Result<OptimizedPlan, OptimizerError> {
    let shard_operators = logical.nodes()[..project_index.saturating_add(3)]
        .iter()
        .map(|node| physical(node, context))
        .collect::<Result<Vec<_>, _>>()?;
    let shard_output = logical.nodes()[project_index + 2].output().clone();
    let coordinator_operators = logical.nodes()[project_index + 1..]
        .iter()
        .map(|node| physical(node, context))
        .collect::<Result<Vec<_>, _>>()?;

    let mut builder = PhysicalPlanBuilder::new(header);
    let shard = builder
        .add_capability_fragment(
            Placement::AllShards,
            shard_operators,
            shard_output.clone(),
            budget,
            context,
        )
        .map_err(|error| OptimizerError::Physical(error.to_string()))?;
    let coordinator = builder
        .add_capability_fragment(
            Placement::Coordinator,
            coordinator_operators,
            logical.output().clone(),
            budget,
            context,
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
            rule: "partition-local-exact-top-k",
            detail: format!(
                "each of {} shards emits at most {MAX_EXACT_DISTRIBUTED_TOP_K_ROWS} exactly ordered rows before the final merge",
                context.shard_count
            ),
        }],
    })
}

fn is_safe_global_count(logical: &LogicalPlan, aggregate_index: usize) -> bool {
    if logical.nodes().iter().any(|node| {
        matches!(node.operator(), LogicalOperator::ChangeScan { .. })
            || matches!(
                node.operator(),
                LogicalOperator::TemporalSlice {
                    valid_time: ValidTimeSpec::Between { .. },
                    ..
                }
            )
    }) {
        return false;
    }
    let Some(node) = logical.nodes().get(aggregate_index) else {
        return false;
    };
    let LogicalOperator::Aggregate {
        grouping,
        aggregates,
    } = node.operator()
    else {
        return false;
    };
    let [input_id] = node.inputs() else {
        return false;
    };
    let Some(input) = logical
        .nodes()
        .get(usize::try_from(input_id.value()).unwrap_or(usize::MAX))
    else {
        return false;
    };
    grouping.is_empty()
        && !aggregates.is_empty()
        && aggregates.iter().all(|(_, expression)| {
            let ScalarExpr::Function {
                function_id,
                arguments,
            } = expression
            else {
                return false;
            };
            if *function_id != COUNT_AGGREGATE_FUNCTION_ID {
                return false;
            }
            match arguments.as_slice() {
                [] => true,
                [ScalarExpr::Slot(slot)] => input
                    .output()
                    .columns()
                    .iter()
                    .find(|column| column.slot() == *slot)
                    .is_some_and(|column| !column.nullable()),
                _ => false,
            }
        })
}

fn optimize_shared_count(
    logical: &LogicalPlan,
    header: PhysicalPlanHeader,
    budget: MemoryBudget,
    context: &OptimizerContext,
    aggregate_index: usize,
) -> Result<OptimizedPlan, OptimizerError> {
    let aggregate_node = &logical.nodes()[aggregate_index];
    let partial_output = count_output_schema(aggregate_node)?;
    let mut shard_operators = logical.nodes()[..=aggregate_index]
        .iter()
        .map(|node| physical(node, context))
        .collect::<Result<Vec<_>, _>>()?;
    let Some(PhysicalOperator::Aggregate { phase, output, .. }) = shard_operators.last_mut() else {
        return Err(OptimizerError::Physical(
            "partial count split did not end at Aggregate".into(),
        ));
    };
    *phase = AggregatePhase::PartialCount;
    *output = partial_output.clone();

    let mut final_aggregate = physical(aggregate_node, context)?;
    let PhysicalOperator::Aggregate { phase, output, .. } = &mut final_aggregate else {
        return Err(OptimizerError::Physical(
            "partial count finalizer was not Aggregate".into(),
        ));
    };
    *phase = AggregatePhase::FinalCount;
    *output = partial_output.clone();
    let mut coordinator_operators = vec![final_aggregate];
    coordinator_operators.extend(
        logical.nodes()[aggregate_index + 1..]
            .iter()
            .map(|node| physical(node, context))
            .collect::<Result<Vec<_>, _>>()?,
    );

    let mut builder = PhysicalPlanBuilder::new(header);
    let shard = builder
        .add_capability_fragment(
            Placement::AllShards,
            shard_operators,
            partial_output.clone(),
            budget,
            context,
        )
        .map_err(|error| OptimizerError::Physical(error.to_string()))?;
    let coordinator = builder
        .add_capability_fragment(
            Placement::Coordinator,
            coordinator_operators,
            logical.output().clone(),
            budget,
            context,
        )
        .map_err(|error| OptimizerError::Physical(error.to_string()))?;
    builder
        .add_exchange(shard, coordinator, ExchangeKind::Gather, partial_output, 8)
        .map_err(|error| OptimizerError::Physical(error.to_string()))?;
    let plan = builder
        .finish(coordinator)
        .map_err(|error| OptimizerError::Physical(error.to_string()))?;
    Ok(OptimizedPlan {
        plan,
        trace: vec![TraceEvent {
            rule: "shard-local-partial-count",
            detail: format!(
                "global count produces one partial row on each of {} shards before final integer sum",
                context.shard_count
            ),
        }],
    })
}

fn count_output_schema(aggregate: &LogicalNode) -> Result<RowSchema, OptimizerError> {
    let LogicalOperator::Aggregate { aggregates, .. } = aggregate.operator() else {
        return Err(OptimizerError::Physical(
            "partial count split did not target an Aggregate".into(),
        ));
    };
    let columns = aggregates
        .iter()
        .map(|(slot, _)| {
            let column = aggregate
                .output()
                .columns()
                .iter()
                .find(|column| column.slot() == *slot)
                .ok_or_else(|| {
                    OptimizerError::Physical("count aggregate output slot is missing".into())
                })?;
            Ok(Column::new(*slot, column.name(), ValueType::Integer, false))
        })
        .collect::<Result<Vec<_>, OptimizerError>>()?;
    RowSchema::new(columns).map_err(|error| OptimizerError::Physical(error.to_string()))
}

fn has_graph_source(logical: &LogicalPlan) -> bool {
    logical.nodes().iter().any(|node| {
        matches!(
            node.operator(),
            LogicalOperator::NodeScan { .. }
                | LogicalOperator::RelationshipScan { .. }
                | LogicalOperator::Expand { .. }
        )
    })
}

fn optimize_coordinator_only(
    logical: &LogicalPlan,
    header: PhysicalPlanHeader,
    budget: MemoryBudget,
    context: &OptimizerContext,
) -> Result<OptimizedPlan, OptimizerError> {
    let mut builder = PhysicalPlanBuilder::new(header);
    let root = builder
        .add_capability_fragment(
            Placement::Coordinator,
            logical
                .nodes()
                .iter()
                .map(|node| physical(node, context))
                .collect::<Result<Vec<_>, _>>()?,
            logical.output().clone(),
            budget,
            context,
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

fn physical(
    node: &LogicalNode,
    context: &OptimizerContext,
) -> Result<PhysicalOperator, OptimizerError> {
    let physical = match node.operator() {
        LogicalOperator::Argument => PhysicalOperator::Argument {
            output: node.output().clone(),
        },
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
            output: node.output().clone(),
        },
        LogicalOperator::Unwind {
            expression,
            binding,
        } => PhysicalOperator::Unwind {
            expression: expression.clone(),
            binding: *binding,
            output: node.output().clone(),
        },
        LogicalOperator::Aggregate {
            grouping,
            aggregates,
        } => PhysicalOperator::Aggregate {
            phase: AggregatePhase::Single,
            grouping: grouping.clone(),
            aggregates: aggregates.clone(),
            output: node.output().clone(),
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
        LogicalOperator::TemporalJoin { kind, keys } => PhysicalOperator::HashJoin {
            kind: match kind {
                temporal_ir::TemporalJoinKind::Inner => JoinKind::Inner,
                temporal_ir::TemporalJoinKind::Left => JoinKind::Left,
            },
            keys: keys.clone(),
        },
        LogicalOperator::Union { all } => PhysicalOperator::Union { all: *all },
        LogicalOperator::TemporalSlice {
            valid_time,
            transaction_time,
        } => PhysicalOperator::TemporalSlice {
            valid_time: valid_time.clone(),
            transaction_time: transaction_time.clone(),
        },
        LogicalOperator::ChangeScan {
            axis,
            start,
            end,
            system_snapshot,
        } => PhysicalOperator::ChangeScan {
            axis: *axis,
            start: start.clone(),
            end: end.clone(),
            system_snapshot: system_snapshot.clone(),
        },
        LogicalOperator::ProcedureCall { procedure } => PhysicalOperator::Procedure {
            procedure: procedure.clone(),
            output: node.output().clone(),
        },
        LogicalOperator::Finish => PhysicalOperator::Finish,
        LogicalOperator::Create => PhysicalOperator::Write {
            operation: WriteOperation::Create,
            output: node.output().clone(),
        },
        LogicalOperator::Merge => PhysicalOperator::Write {
            operation: WriteOperation::Merge,
            output: node.output().clone(),
        },
        LogicalOperator::Set => PhysicalOperator::Write {
            operation: WriteOperation::Set,
            output: node.output().clone(),
        },
        LogicalOperator::Remove => PhysicalOperator::Write {
            operation: WriteOperation::Remove,
            output: node.output().clone(),
        },
        LogicalOperator::Delete { detach } => PhysicalOperator::Write {
            operation: WriteOperation::Delete { detach: *detach },
            output: node.output().clone(),
        },
        LogicalOperator::Apply { apply } => {
            let optimized = Optimizer::new().optimize(apply.child_plan(), context.clone())?;
            let child_input = apply
                .child_plan()
                .nodes()
                .first()
                .filter(|node| matches!(node.operator(), LogicalOperator::Argument))
                .ok_or_else(|| OptimizerError::Logical("Apply child Argument is missing".into()))?
                .output()
                .clone();
            PhysicalOperator::Apply {
                apply: PhysicalApply::new(
                    apply.identity(),
                    apply.kind(),
                    apply.imports().to_vec(),
                    apply.exports().to_vec(),
                    child_input,
                    optimized.plan,
                    apply.max_invocations(),
                    apply.max_output_rows(),
                    apply.max_depth(),
                ),
                output: node.output().clone(),
            }
        }
        LogicalOperator::BatchSubtransaction { batch } => {
            let apply = batch.apply();
            let optimized = Optimizer::new().optimize(apply.child_plan(), context.clone())?;
            let child_input = apply
                .child_plan()
                .nodes()
                .first()
                .filter(|node| matches!(node.operator(), LogicalOperator::Argument))
                .ok_or_else(|| {
                    OptimizerError::Logical("batch subtransaction child Argument is missing".into())
                })?
                .output()
                .clone();
            PhysicalOperator::BatchSubtransaction {
                apply: PhysicalApply::new(
                    apply.identity(),
                    apply.kind(),
                    apply.imports().to_vec(),
                    apply.exports().to_vec(),
                    child_input,
                    optimized.plan,
                    apply.max_invocations(),
                    apply.max_output_rows(),
                    apply.max_depth(),
                ),
                batch_rows: batch.batch_rows(),
                output: node.output().clone(),
            }
        }
    };
    Ok(physical)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OptimizerError {
    InvalidContext,
    InvalidCapabilityGeneration,
    Logical(String),
    Physical(String),
    UnsupportedUnionShape,
}

impl Display for OptimizerError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "query optimization failed: {self:?}")
    }
}

impl Error for OptimizerError {}
