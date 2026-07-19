use super::{PlanHeaderV2, RowSchema, ScalarExpr, SlotId, V2_PLAN_VERSION, ValidationError};

pub const MAX_LOGICAL_NODES: usize = 250_000;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct LogicalNodeId(u32);

impl LogicalNodeId {
    #[must_use]
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn value(self) -> u32 {
        self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LogicalOperator {
    Argument,
    NodeScan {
        binding: SlotId,
        labels: Vec<u32>,
    },
    RelationshipScan {
        binding: SlotId,
        types: Vec<u32>,
    },
    Expand {
        source: SlotId,
        relationship: SlotId,
        destination: SlotId,
        outgoing: bool,
    },
    Filter {
        predicate: ScalarExpr,
    },
    Project {
        expressions: Vec<(SlotId, ScalarExpr)>,
    },
    Aggregate {
        grouping: Vec<SlotId>,
        aggregates: Vec<(SlotId, ScalarExpr)>,
    },
    Sort {
        keys: Vec<SlotId>,
    },
    Skip {
        count: ScalarExpr,
    },
    Limit {
        count: ScalarExpr,
    },
    InnerJoin,
    LeftJoin,
    Union {
        all: bool,
    },
    TemporalSlice,
    Diff,
    Create,
    Merge,
    Set,
    Remove,
    Delete {
        detach: bool,
    },
    ProcedureCall {
        procedure_id: u32,
    },
    Finish,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicalNode {
    operator: LogicalOperator,
    inputs: Vec<LogicalNodeId>,
    output: RowSchema,
}

impl LogicalNode {
    #[must_use]
    pub const fn new(
        operator: LogicalOperator,
        inputs: Vec<LogicalNodeId>,
        output: RowSchema,
    ) -> Self {
        Self {
            operator,
            inputs,
            output,
        }
    }

    #[must_use]
    pub const fn operator(&self) -> &LogicalOperator {
        &self.operator
    }

    #[must_use]
    pub fn inputs(&self) -> &[LogicalNodeId] {
        &self.inputs
    }

    #[must_use]
    pub const fn output(&self) -> &RowSchema {
        &self.output
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicalPlan {
    header: PlanHeaderV2,
    nodes: Vec<LogicalNode>,
    root: LogicalNodeId,
    output: RowSchema,
}

impl LogicalPlan {
    #[must_use]
    pub const fn from_parts(
        header: PlanHeaderV2,
        nodes: Vec<LogicalNode>,
        root: LogicalNodeId,
        output: RowSchema,
    ) -> Self {
        Self {
            header,
            nodes,
            root,
            output,
        }
    }

    pub fn validate(&self) -> Result<(), ValidationError> {
        self.header.validate()?;
        if self.nodes.len() > MAX_LOGICAL_NODES {
            return Err(ValidationError::TooManyNodes {
                max: MAX_LOGICAL_NODES,
                actual: self.nodes.len(),
            });
        }
        let root_index = usize::try_from(self.root.value()).unwrap_or(usize::MAX);
        let root = self
            .nodes
            .get(root_index)
            .ok_or(ValidationError::InvalidRoot(self.root))?;
        for (index, node) in self.nodes.iter().enumerate() {
            let node_id = LogicalNodeId::new(u32::try_from(index).unwrap_or(u32::MAX));
            for input in &node.inputs {
                if usize::try_from(input.value()).unwrap_or(usize::MAX) >= index {
                    return Err(ValidationError::InvalidInput {
                        node: node_id,
                        input: *input,
                    });
                }
            }
            validate_input_count(node_id, node)?;
            validate_slots(node_id, node, &self.nodes)?;
        }
        if root.output != self.output {
            return Err(ValidationError::OutputSchemaMismatch);
        }
        Ok(())
    }

    #[must_use]
    pub const fn header(&self) -> &PlanHeaderV2 {
        &self.header
    }

    #[must_use]
    pub fn nodes(&self) -> &[LogicalNode] {
        &self.nodes
    }

    #[must_use]
    pub const fn root(&self) -> LogicalNodeId {
        self.root
    }

    #[must_use]
    pub const fn output(&self) -> &RowSchema {
        &self.output
    }
}

pub struct LogicalPlanBuilder {
    header: PlanHeaderV2,
    nodes: Vec<LogicalNode>,
}

impl LogicalPlanBuilder {
    #[must_use]
    pub const fn new(header: PlanHeaderV2) -> Self {
        Self {
            header,
            nodes: Vec::new(),
        }
    }

    pub fn add(
        &mut self,
        operator: LogicalOperator,
        inputs: Vec<LogicalNodeId>,
        output: RowSchema,
    ) -> Result<LogicalNodeId, ValidationError> {
        let node = LogicalNodeId::new(u32::try_from(self.nodes.len()).map_err(|_| {
            ValidationError::TooManyNodes {
                max: MAX_LOGICAL_NODES,
                actual: self.nodes.len().saturating_add(1),
            }
        })?);
        if self.nodes.len() >= MAX_LOGICAL_NODES {
            return Err(ValidationError::TooManyNodes {
                max: MAX_LOGICAL_NODES,
                actual: self.nodes.len().saturating_add(1),
            });
        }
        for input in &inputs {
            if usize::try_from(input.value()).unwrap_or(usize::MAX) >= self.nodes.len() {
                return Err(ValidationError::InvalidInput {
                    node,
                    input: *input,
                });
            }
        }
        let logical = LogicalNode::new(operator, inputs, output);
        validate_input_count(node, &logical)?;
        validate_slots(node, &logical, &self.nodes)?;
        self.nodes.push(logical);
        Ok(node)
    }

    pub fn finish(self, root: LogicalNodeId) -> Result<LogicalPlan, ValidationError> {
        let output = self
            .nodes
            .get(usize::try_from(root.value()).unwrap_or(usize::MAX))
            .ok_or(ValidationError::InvalidRoot(root))?
            .output
            .clone();
        let plan = LogicalPlan::from_parts(self.header, self.nodes, root, output);
        plan.validate()?;
        Ok(plan)
    }
}

fn validate_input_count(node_id: LogicalNodeId, node: &LogicalNode) -> Result<(), ValidationError> {
    let expected = match node.operator {
        LogicalOperator::Argument
        | LogicalOperator::NodeScan { .. }
        | LogicalOperator::RelationshipScan { .. }
        | LogicalOperator::Diff => 0,
        LogicalOperator::InnerJoin | LogicalOperator::LeftJoin | LogicalOperator::Union { .. } => 2,
        _ => 1,
    };
    if node.inputs.len() != expected {
        return Err(ValidationError::InvalidInputCount {
            node: node_id,
            expected,
            actual: node.inputs.len(),
        });
    }
    Ok(())
}

fn validate_slots(
    node_id: LogicalNodeId,
    node: &LogicalNode,
    preceding: &[LogicalNode],
) -> Result<(), ValidationError> {
    let input_has = |slot| {
        node.inputs.iter().any(|input| {
            preceding
                .get(usize::try_from(input.value()).unwrap_or(usize::MAX))
                .is_some_and(|input| input.output.contains(slot))
        })
    };
    let mut referenced = Vec::new();
    let mut required_outputs = Vec::new();
    match &node.operator {
        LogicalOperator::NodeScan { binding, .. }
        | LogicalOperator::RelationshipScan { binding, .. } => {
            required_outputs.push(*binding);
        }
        LogicalOperator::Expand {
            source,
            relationship,
            destination,
            ..
        } => {
            referenced.push(*source);
            required_outputs.push(*relationship);
            required_outputs.push(*destination);
        }
        LogicalOperator::Filter { predicate } => {
            predicate.visit_slots(&mut |slot| referenced.push(slot));
        }
        LogicalOperator::Project { expressions } => {
            for (output, expression) in expressions {
                required_outputs.push(*output);
                expression.visit_slots(&mut |slot| referenced.push(slot));
            }
        }
        LogicalOperator::Aggregate {
            grouping,
            aggregates,
        } => {
            for slot in grouping {
                referenced.push(*slot);
            }
            for (output, expression) in aggregates {
                required_outputs.push(*output);
                expression.visit_slots(&mut |slot| referenced.push(slot));
            }
        }
        LogicalOperator::Sort { keys } => {
            for slot in keys {
                referenced.push(*slot);
            }
        }
        LogicalOperator::Skip { count } | LogicalOperator::Limit { count } => {
            count.visit_slots(&mut |slot| referenced.push(slot));
        }
        _ => {}
    }
    if let Some(slot) = required_outputs
        .into_iter()
        .find(|slot| !node.output.contains(*slot))
        .or_else(|| referenced.into_iter().find(|slot| !input_has(*slot)))
    {
        return Err(ValidationError::UnknownSlot {
            node: node_id,
            slot,
        });
    }
    Ok(())
}

const _: u16 = V2_PLAN_VERSION;
