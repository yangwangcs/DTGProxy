use dtg_language_ir::{
    ExpandDirection, LogicalExpr, LogicalNodeId, LogicalNodeKind, LogicalPlan, ReadScope,
    RelationshipLookup, TimeExpr, ValidTimeExpr, ValidTimePredicate, Value, VertexLookup,
};
use dtg_storage::{EdgeId, TransactionTime, VertexId};

use crate::{PlanError, PlanningContext, PushdownKind};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LogicalReadOperation {
    VertexPoint(VertexId),
    VertexScan,
    EdgePoint(EdgeId),
    EdgeScan,
    Adjacency { direction: ExpandDirection },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicalReadRequest {
    node: LogicalNodeId,
    operation: LogicalReadOperation,
    read_scope: ReadScope,
    row_bound: u32,
}

impl LogicalReadRequest {
    pub(crate) const fn new(
        node: LogicalNodeId,
        operation: LogicalReadOperation,
        read_scope: ReadScope,
        row_bound: u32,
    ) -> Self {
        Self {
            node,
            operation,
            read_scope,
            row_bound,
        }
    }

    pub const fn node(&self) -> LogicalNodeId {
        self.node
    }

    pub const fn operation(&self) -> &LogicalReadOperation {
        &self.operation
    }

    pub const fn read_scope(&self) -> &ReadScope {
        &self.read_scope
    }

    pub const fn row_bound(&self) -> u32 {
        self.row_bound
    }

    pub const fn pushdown_kind(&self) -> Option<PushdownKind> {
        match self.operation {
            LogicalReadOperation::VertexPoint(_) => Some(PushdownKind::VertexPoint),
            LogicalReadOperation::VertexScan => Some(PushdownKind::VertexScan),
            LogicalReadOperation::EdgePoint(_)
            | LogicalReadOperation::EdgeScan
            | LogicalReadOperation::Adjacency { .. } => None,
        }
    }
}

pub(crate) fn collect_reads(
    plan: &LogicalPlan,
    context: &PlanningContext,
) -> Result<Vec<LogicalReadRequest>, PlanError> {
    let mut reads = Vec::new();
    collect_plan_reads(plan, context, &mut reads)?;
    Ok(reads)
}

fn collect_plan_reads(
    plan: &LogicalPlan,
    context: &PlanningContext,
    reads: &mut Vec<LogicalReadRequest>,
) -> Result<(), PlanError> {
    for node in &plan.nodes {
        let request = match &node.kind {
            LogicalNodeKind::VertexLookup(lookup) => Some(LogicalReadRequest::new(
                node.id,
                LogicalReadOperation::VertexPoint(vertex_id(node.id, lookup)?),
                lookup.read_scope.clone(),
                1,
            )),
            LogicalNodeKind::NodeScan(scan) => Some(LogicalReadRequest::new(
                node.id,
                LogicalReadOperation::VertexScan,
                scan.read_scope.clone(),
                scan_bound(plan, node.id, context)?,
            )),
            LogicalNodeKind::RelationshipLookup(lookup) => Some(LogicalReadRequest::new(
                node.id,
                LogicalReadOperation::EdgePoint(edge_id(node.id, lookup)?),
                lookup.read_scope.clone(),
                1,
            )),
            LogicalNodeKind::RelationshipScan(scan) => Some(LogicalReadRequest::new(
                node.id,
                LogicalReadOperation::EdgeScan,
                scan.read_scope.clone(),
                scan_bound(plan, node.id, context)?,
            )),
            LogicalNodeKind::Expand(expand) => Some(LogicalReadRequest::new(
                node.id,
                LogicalReadOperation::Adjacency {
                    direction: expand.direction,
                },
                expand.read_scope.clone(),
                scan_bound(plan, node.id, context)?,
            )),
            LogicalNodeKind::Subquery(subquery) => {
                collect_plan_reads(&subquery.plan, context, reads)?;
                None
            }
            LogicalNodeKind::Filter { .. }
            | LogicalNodeKind::Project { .. }
            | LogicalNodeKind::Join(_)
            | LogicalNodeKind::Aggregate(_)
            | LogicalNodeKind::Sort(_)
            | LogicalNodeKind::Limit(_)
            | LogicalNodeKind::Unwind(_) => None,
        };
        if let Some(request) = request {
            reads.push(request);
        }
    }
    Ok(())
}

fn vertex_id(node: LogicalNodeId, lookup: &VertexLookup) -> Result<VertexId, PlanError> {
    literal_identifier(&lookup.id)
        .and_then(|value| VertexId::new(value).ok())
        .ok_or(PlanError::UnboundPointIdentity { node })
}

fn edge_id(node: LogicalNodeId, lookup: &RelationshipLookup) -> Result<EdgeId, PlanError> {
    literal_identifier(&lookup.id)
        .and_then(|value| EdgeId::new(value).ok())
        .ok_or(PlanError::UnboundPointIdentity { node })
}

fn literal_identifier(expression: &LogicalExpr) -> Option<u128> {
    match expression {
        LogicalExpr::Literal(Value::Integer(value)) => u128::try_from(*value).ok(),
        _ => None,
    }
}

fn scan_bound(
    plan: &LogicalPlan,
    input: LogicalNodeId,
    context: &PlanningContext,
) -> Result<u32, PlanError> {
    plan.nodes
        .iter()
        .filter_map(|node| match &node.kind {
            LogicalNodeKind::Limit(limit) if limit.input == input => {
                limit.limit.as_ref().and_then(literal_row_bound)
            }
            _ => None,
        })
        .chain(context.logical_scan_bound())
        .min()
        .ok_or(PlanError::NoBoundedAccess { node: input })
}

fn literal_row_bound(expression: &LogicalExpr) -> Option<u32> {
    match expression {
        LogicalExpr::Literal(Value::Integer(value)) => {
            u32::try_from(*value).ok().filter(|v| *v > 0)
        }
        _ => None,
    }
}

pub(crate) fn resolve_read_time(
    request: &LogicalReadRequest,
    context: &PlanningContext,
) -> Option<(TransactionTime, i64)> {
    let transaction_time = match &request.read_scope.transaction_time {
        dtg_language_ir::TemporalScope::Current => {
            context.snapshot_requirements().transaction_time()
        }
        dtg_language_ir::TemporalScope::AsOf(TimeExpr::Literal(value)) => *value,
        dtg_language_ir::TemporalScope::AsOf(TimeExpr::Parameter(_))
        | dtg_language_ir::TemporalScope::Changes { .. } => return None,
    };
    let valid_at = match &request.read_scope.valid_time {
        None => context.snapshot_requirements().valid_at(),
        Some(ValidTimePredicate::At(ValidTimeExpr::Literal(value))) => *value,
        Some(ValidTimePredicate::At(ValidTimeExpr::Parameter(_)))
        | Some(ValidTimePredicate::Overlaps(_))
        | Some(ValidTimePredicate::Changes { .. }) => return None,
    };
    Some((transaction_time, valid_at))
}
