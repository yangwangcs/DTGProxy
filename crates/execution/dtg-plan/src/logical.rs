use dtg_language_ir::{
    ExpandDirection, Limit, LogicalExpr, LogicalNodeId, LogicalNodeKind, LogicalPlan, ReadScope,
    RelationshipLookup, TimeExpr, ValidTimeExpr, ValidTimePredicate, Value, VertexLookup,
};
use dtg_storage::{EdgeId, TransactionTime, VertexId};
use std::collections::BTreeSet;

use crate::{PlanError, PlanningContext, PushdownKind};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LogicalReadOperation {
    VertexPoint(VertexId),
    VertexScan,
    EdgePoint(EdgeId),
    EdgeScan,
    Adjacency {
        vertex_id: VertexId,
        direction: ExpandDirection,
    },
    Traversal {
        vertex_id: VertexId,
        directions: Vec<ExpandDirection>,
    },
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
            | LogicalReadOperation::Adjacency { .. }
            | LogicalReadOperation::Traversal { .. } => None,
        }
    }
}

pub(crate) fn collect_reads(
    plan: &LogicalPlan,
    context: &PlanningContext,
) -> Result<Vec<LogicalReadRequest>, PlanError> {
    let expanded_inputs = point_anchored_expand_inputs(plan)?;
    let mut reads = Vec::new();
    collect_plan_reads(plan, context, &expanded_inputs, &mut reads)?;
    Ok(reads)
}

fn point_anchored_expand_inputs(plan: &LogicalPlan) -> Result<BTreeSet<LogicalNodeId>, PlanError> {
    let mut inputs = BTreeSet::new();
    for node in &plan.nodes {
        let LogicalNodeKind::Expand(expand) = &node.kind else {
            continue;
        };
        point_anchored_expand_chain(plan, node.id, expand)?;
        if plan.nodes.iter().any(|candidate| {
            logical_node_uses_column(&candidate.kind, &expand.source)
                || logical_node_uses_column(&candidate.kind, &expand.destination)
        }) {
            return Err(PlanError::UnsupportedNode {
                node: node.id,
                reason:
                    "point-anchored Expand does not materialize source or destination variables"
                        .into(),
            });
        }
        inputs.insert(expand.input);
    }
    for node in &plan.nodes {
        let LogicalNodeKind::Expand(expand) = &node.kind else {
            continue;
        };
        let children = plan
            .nodes
            .iter()
            .filter(|candidate| {
                matches!(
                    &candidate.kind,
                    LogicalNodeKind::Expand(child) if child.input == node.id
                )
            })
            .count();
        if children > 1 {
            return Err(PlanError::UnsupportedNode {
                node: node.id,
                reason: "point-anchored traversal does not support branching intermediate expands"
                    .into(),
            });
        }
        if inputs.contains(&node.id)
            && plan
                .nodes
                .iter()
                .any(|candidate| logical_node_uses_column(&candidate.kind, &expand.relationship))
        {
            return Err(PlanError::UnsupportedNode {
                node: node.id,
                reason: "point-anchored traversal does not materialize intermediate relationship variables"
                    .into(),
            });
        }
    }
    Ok(inputs)
}

fn point_anchored_expand_chain(
    plan: &LogicalPlan,
    terminal: LogicalNodeId,
    terminal_expand: &dtg_language_ir::Expand,
) -> Result<(VertexId, Vec<ExpandDirection>), PlanError> {
    let mut current_id = terminal;
    let mut current = terminal_expand;
    let mut directions = Vec::new();
    loop {
        if !current.destination_labels.is_empty() || !current.relationship_types.is_empty() {
            return Err(PlanError::UnsupportedNode {
                node: current_id,
                reason: "point-anchored Expand does not yet support label or relationship-type predicates"
                    .into(),
            });
        }
        if current.read_scope != terminal_expand.read_scope {
            return Err(PlanError::UnsupportedNode {
                node: current_id,
                reason: "point-anchored traversal hops must have one read scope".into(),
            });
        }
        directions.push(current.direction);
        let input = plan
            .nodes
            .iter()
            .find(|candidate| candidate.id == current.input)
            .ok_or(PlanError::UnsupportedNode {
                node: current_id,
                reason: "Expand input is absent from the logical plan".into(),
            })?;
        match &input.kind {
            LogicalNodeKind::VertexLookup(lookup) => {
                if lookup.variable != current.source {
                    return Err(PlanError::UnsupportedNode {
                        node: current_id,
                        reason: "Expand source does not match its vertex lookup input".into(),
                    });
                }
                if !lookup.labels.is_empty() {
                    return Err(PlanError::UnsupportedNode {
                        node: input.id,
                        reason: "point-anchored Expand does not yet support label predicates"
                            .into(),
                    });
                }
                directions.reverse();
                return Ok((vertex_id(input.id, lookup)?, directions));
            }
            LogicalNodeKind::Expand(previous) => {
                if previous.destination != current.source {
                    return Err(PlanError::UnsupportedNode {
                        node: current_id,
                        reason: "Expand source does not match its preceding destination".into(),
                    });
                }
                current_id = input.id;
                current = previous;
            }
            _ => {
                return Err(PlanError::UnsupportedNode {
                    node: current_id,
                    reason: "Expand requires a point-anchored traversal input".into(),
                });
            }
        }
    }
}

fn collect_plan_reads(
    plan: &LogicalPlan,
    context: &PlanningContext,
    expanded_inputs: &BTreeSet<LogicalNodeId>,
    reads: &mut Vec<LogicalReadRequest>,
) -> Result<(), PlanError> {
    for node in &plan.nodes {
        let request = match &node.kind {
            LogicalNodeKind::VertexLookup(lookup) if !expanded_inputs.contains(&node.id) => {
                Some(LogicalReadRequest::new(
                    node.id,
                    LogicalReadOperation::VertexPoint(vertex_id(node.id, lookup)?),
                    lookup.read_scope.clone(),
                    1,
                ))
            }
            LogicalNodeKind::VertexLookup(_) => None,
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
            LogicalNodeKind::Expand(expand) if !expanded_inputs.contains(&node.id) => {
                let (vertex_id, directions) = point_anchored_expand_chain(plan, node.id, expand)?;
                let operation = if directions.len() == 1 {
                    LogicalReadOperation::Adjacency {
                        vertex_id,
                        direction: directions[0],
                    }
                } else {
                    LogicalReadOperation::Traversal {
                        vertex_id,
                        directions,
                    }
                };
                Some(LogicalReadRequest::new(
                    node.id,
                    operation,
                    expand.read_scope.clone(),
                    scan_bound(plan, node.id, context)?,
                ))
            }
            LogicalNodeKind::Expand(_) => None,
            LogicalNodeKind::Subquery(_)
            | LogicalNodeKind::Filter { .. }
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

fn logical_node_uses_column(kind: &LogicalNodeKind, name: &str) -> bool {
    match kind {
        LogicalNodeKind::Filter { predicate, .. } => logical_expr_uses_column(predicate, name),
        LogicalNodeKind::Project { projections, .. } => projections
            .iter()
            .any(|projection| logical_expr_uses_column(&projection.expression, name)),
        LogicalNodeKind::Join(join) => join
            .predicate
            .as_ref()
            .is_some_and(|predicate| logical_expr_uses_column(predicate, name)),
        LogicalNodeKind::Aggregate(aggregate) => {
            aggregate
                .groups
                .iter()
                .any(|group| logical_expr_uses_column(&group.expression, name))
                || aggregate.aggregates.iter().any(|aggregate| {
                    aggregate
                        .argument
                        .as_ref()
                        .is_some_and(|argument| logical_expr_uses_column(argument, name))
                })
        }
        LogicalNodeKind::Sort(sort) => sort
            .keys
            .iter()
            .any(|key| logical_expr_uses_column(&key.expression, name)),
        LogicalNodeKind::Unwind(unwind) => logical_expr_uses_column(&unwind.expression, name),
        LogicalNodeKind::NodeScan(_)
        | LogicalNodeKind::RelationshipScan(_)
        | LogicalNodeKind::VertexLookup(_)
        | LogicalNodeKind::RelationshipLookup(_)
        | LogicalNodeKind::Expand(_)
        | LogicalNodeKind::Limit(_)
        | LogicalNodeKind::Subquery(_) => false,
    }
}

fn logical_expr_uses_column(expression: &LogicalExpr, name: &str) -> bool {
    match expression {
        LogicalExpr::Column(column) => column == name,
        LogicalExpr::Property { input, .. } | LogicalExpr::Unary { input, .. } => {
            logical_expr_uses_column(input, name)
        }
        LogicalExpr::Binary { left, right, .. } => {
            logical_expr_uses_column(left, name) || logical_expr_uses_column(right, name)
        }
        LogicalExpr::List(values) => values
            .iter()
            .any(|value| logical_expr_uses_column(value, name)),
        LogicalExpr::Map(values) => values
            .iter()
            .any(|(_, value)| logical_expr_uses_column(value, name)),
        LogicalExpr::Literal(_) | LogicalExpr::Parameter(_) => false,
    }
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
            LogicalNodeKind::Limit(limit) if limit.input == input => literal_limit_bound(limit),
            _ => None,
        })
        .chain(context.logical_scan_bound())
        .min()
        .ok_or(PlanError::NoBoundedAccess { node: input })
}

fn literal_limit_bound(limit: &Limit) -> Option<u32> {
    let count = literal_nonnegative_row_count(limit.limit.as_ref()?)?;
    let skip = match limit.skip.as_ref() {
        Some(skip) => literal_nonnegative_row_count(skip)?,
        None => 0,
    };
    u32::try_from(skip.checked_add(count)?)
        .ok()
        .filter(|value| *value > 0)
}

fn literal_nonnegative_row_count(expression: &LogicalExpr) -> Option<u64> {
    match expression {
        LogicalExpr::Literal(Value::Integer(value)) => u64::try_from(*value).ok(),
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
