#![forbid(unsafe_code)]

mod capability;
mod catalog;
mod fragment;
mod logical;
mod physical;
mod validate;

pub use capability::{
    CAP_DUPLICATE_EXACT, CAP_NULL_EXACT, CAP_ORDER_EXACT, CAP_SNAPSHOT_EXACT, CAP_TEMPORAL_EXACT,
    CAP_VERTEX_POINT, CAP_VERTEX_SCAN, EXACT_VERTEX_POINT_CAPABILITIES,
    EXACT_VERTEX_SCAN_CAPABILITIES, PushdownDecision, PushdownGuarantee, PushdownKind,
    SemanticRequirements, decide_pushdown,
};
pub use catalog::{CatalogShard, CatalogSnapshot, PlanningContext, SnapshotRequirements};
pub use fragment::{Exchange, ExchangeKind, FragmentId, PlanFence, PlanFragment};
pub use logical::{LogicalReadOperation, LogicalReadRequest};
pub use physical::{
    PHYSICAL_PLAN_VERSION, PhysicalExpr, PhysicalOperator, PhysicalOperatorKind, PhysicalPlan,
    StorageAccess,
};
pub use validate::PlanError;

use dtg_language_ir::{LogicalProgram, LogicalStatement};
use dtg_storage::{
    CapabilityManifest, PushdownOperation, PushdownRequest, SUPPORTED_PUSHDOWN_CONTRACT_VERSION,
    VertexRead, VertexScan,
};

pub const CLEAN_BREAK_ARCHITECTURE_VERSION: u32 = 1;

pub struct Planner;

impl Planner {
    pub fn plan(
        &self,
        program: &LogicalProgram,
        context: &PlanningContext,
    ) -> Result<PhysicalPlan, PlanError> {
        plan(program, context)
    }
}

pub fn plan(
    program: &LogicalProgram,
    context: &PlanningContext,
) -> Result<PhysicalPlan, PlanError> {
    validate::validate_inputs(program, context)?;
    let LogicalStatement::Query(logical_plan) = &program.statement else {
        return Err(PlanError::UnsupportedStatement);
    };
    let reads = logical::collect_reads(logical_plan, context)?;
    let mut fragments = Vec::with_capacity(context.catalog().shards().len());
    let mut exchanges = Vec::with_capacity(context.catalog().shards().len());
    for (ordinal, shard) in context.catalog().shards().iter().enumerate() {
        let id = FragmentId::new(u32::try_from(ordinal + 1).map_err(|_| {
            PlanError::InvalidCatalog(
                "fragment count exceeds the supported identifier range".into(),
            )
        })?);
        let fence = PlanFence::new(
            shard,
            context.catalog().version(),
            context.catalog().schema_version(),
            context.snapshot_requirements().clone(),
        );
        let storage_accesses = reads
            .iter()
            .map(|request| storage_access(request, &fence, context))
            .collect::<Result<Vec<_>, _>>()?;
        fragments.push(PlanFragment::new(id, fence, storage_accesses));
        exchanges.push(Exchange::gather(id));
    }
    let operators = lower_operators(logical_plan, &fragments)?;
    Ok(PhysicalPlan {
        version: PHYSICAL_PLAN_VERSION,
        fragments,
        exchanges,
        root_operator: logical_plan.root,
        operators,
        result_schema: program.result_schema.clone(),
    })
}

fn lower_operators(
    logical_plan: &dtg_language_ir::LogicalPlan,
    fragments: &[PlanFragment],
) -> Result<Vec<PhysicalOperator>, PlanError> {
    use dtg_language_ir::LogicalNodeKind;

    let fragment_ids: Vec<FragmentId> = fragments.iter().map(PlanFragment::id).collect();
    logical_plan
        .nodes
        .iter()
        .map(|node| {
            let kind = match &node.kind {
                LogicalNodeKind::NodeScan(scan) => PhysicalOperatorKind::Source {
                    logical_node: node.id,
                    fragments: fragment_ids.clone(),
                    output: scan.variable.clone(),
                },
                LogicalNodeKind::RelationshipScan(scan) => PhysicalOperatorKind::Source {
                    logical_node: node.id,
                    fragments: fragment_ids.clone(),
                    output: scan.variable.clone(),
                },
                LogicalNodeKind::VertexLookup(lookup) => PhysicalOperatorKind::Source {
                    logical_node: node.id,
                    fragments: fragment_ids.clone(),
                    output: lookup.variable.clone(),
                },
                LogicalNodeKind::RelationshipLookup(lookup) => PhysicalOperatorKind::Source {
                    logical_node: node.id,
                    fragments: fragment_ids.clone(),
                    output: lookup.variable.clone(),
                },
                LogicalNodeKind::Filter { input, predicate } => PhysicalOperatorKind::Filter {
                    input: *input,
                    predicate: PhysicalExpr::Evaluate(predicate.clone()),
                },
                LogicalNodeKind::Project { input, projections } => PhysicalOperatorKind::Project {
                    input: *input,
                    projections: projections.clone(),
                },
                LogicalNodeKind::Join(join) => PhysicalOperatorKind::Join {
                    left: join.left,
                    right: join.right,
                    kind: join.kind,
                    predicate: join.predicate.clone().map(PhysicalExpr::Evaluate),
                },
                LogicalNodeKind::Aggregate(aggregate) => PhysicalOperatorKind::Aggregate {
                    input: aggregate.input,
                    groups: aggregate.groups.clone(),
                    aggregates: aggregate.aggregates.clone(),
                },
                LogicalNodeKind::Sort(sort) => PhysicalOperatorKind::Sort {
                    input: sort.input,
                    keys: sort.keys.clone(),
                },
                LogicalNodeKind::Limit(limit) => PhysicalOperatorKind::Limit {
                    input: limit.input,
                    skip: physical_row_count(node.id, limit.skip.as_ref())?.unwrap_or(0),
                    limit: physical_row_count(node.id, limit.limit.as_ref())?,
                },
                LogicalNodeKind::Unwind(unwind) => PhysicalOperatorKind::Unwind {
                    input: unwind.input,
                    expression: PhysicalExpr::Evaluate(unwind.expression.clone()),
                    alias: unwind.alias.clone(),
                },
                LogicalNodeKind::Expand(_) | LogicalNodeKind::Subquery(_) => {
                    return Err(PlanError::UnsupportedNode {
                        node: node.id,
                        reason: "logical operator has no bounded physical implementation".into(),
                    });
                }
            };
            Ok(PhysicalOperator::new(node.id, kind))
        })
        .collect()
}

fn physical_row_count(
    node: dtg_language_ir::LogicalNodeId,
    expression: Option<&dtg_language_ir::LogicalExpr>,
) -> Result<Option<u64>, PlanError> {
    let Some(expression) = expression else {
        return Ok(None);
    };
    let dtg_language_ir::LogicalExpr::Literal(dtg_language_ir::Value::Integer(value)) = expression
    else {
        return Err(PlanError::UnsupportedNode {
            node,
            reason: "LIMIT and SKIP must be resolved to non-negative literals before planning"
                .into(),
        });
    };
    u64::try_from(*value)
        .map(Some)
        .map_err(|_| PlanError::UnsupportedNode {
            node,
            reason: "LIMIT and SKIP must be non-negative".into(),
        })
}

fn storage_access(
    logical: &LogicalReadRequest,
    fence: &PlanFence,
    context: &PlanningContext,
) -> Result<StorageAccess, PlanError> {
    let Some(kind) = logical.pushdown_kind() else {
        return Ok(StorageAccess::Logical(logical.clone()));
    };
    let decision = decide_pushdown(context.capabilities(), kind);
    if decision == PushdownDecision::Unsupported {
        return Ok(StorageAccess::Logical(logical.clone()));
    }
    let Some((transaction_time, valid_at)) = logical::resolve_read_time(logical, context) else {
        return Ok(StorageAccess::Logical(logical.clone()));
    };
    let operation = match logical.operation() {
        LogicalReadOperation::VertexPoint(id) => {
            PushdownOperation::Vertex(VertexRead::new(*id, valid_at, transaction_time))
        }
        LogicalReadOperation::VertexScan => PushdownOperation::VertexScan(VertexScan::new(
            valid_at,
            transaction_time,
            None,
            logical.row_bound(),
        )?),
        LogicalReadOperation::EdgePoint(_)
        | LogicalReadOperation::EdgeScan
        | LogicalReadOperation::Adjacency { .. } => {
            return Ok(StorageAccess::Logical(logical.clone()));
        }
    };
    let required_capabilities = CapabilityManifest::from_names([kind.capability()])?;
    let request = PushdownRequest::new(
        SUPPORTED_PUSHDOWN_CONTRACT_VERSION,
        fence.read_fence().clone(),
        required_capabilities,
        operation,
    )?;
    let (guarantee, residual) = match decision {
        PushdownDecision::Exact(guarantee) => (guarantee, None),
        PushdownDecision::ResidualRequired(guarantee) => (
            guarantee,
            Some(PhysicalExpr::VerifyStorageSemantics {
                node: logical.node(),
                requirements: SemanticRequirements::exact(),
            }),
        ),
        PushdownDecision::Unsupported => unreachable!("unsupported handled before request build"),
    };
    Ok(StorageAccess::Pushdown {
        node: logical.node(),
        request: Box::new(request),
        guarantee,
        residual,
    })
}
