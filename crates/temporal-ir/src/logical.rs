use super::{
    Column, PLAN_VERSION, PlanHeader, ResolvedProcedure, RowSchema, ScalarExpr, SlotId,
    ValidationError,
};

pub const MAX_LOGICAL_NODES: usize = 250_000;
pub const MAX_APPLY_DEPTH: u16 = 32;
pub const MAX_APPLY_INVOCATIONS: u64 = 1_000_000;
pub const MAX_APPLY_OUTPUT_ROWS: u64 = 1_000_000;
pub const MAX_BATCH_SUBTRANSACTION_ROWS: u32 = 1_000_000;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ChildPlanId(u64);

impl ChildPlanId {
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn value(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ApplySlotMapping {
    parent_slot: SlotId,
    child_slot: SlotId,
}

impl ApplySlotMapping {
    #[must_use]
    pub const fn new(parent_slot: SlotId, child_slot: SlotId) -> Self {
        Self {
            parent_slot,
            child_slot,
        }
    }

    #[must_use]
    pub const fn parent_slot(self) -> SlotId {
        self.parent_slot
    }

    #[must_use]
    pub const fn child_slot(self) -> SlotId {
        self.child_slot
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApplyKind {
    Inner,
    Exists { output: SlotId },
    Count { output: SlotId },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicalApply {
    identity: ChildPlanId,
    kind: ApplyKind,
    imports: Vec<ApplySlotMapping>,
    exports: Vec<ApplySlotMapping>,
    child_plan: Box<LogicalPlan>,
    max_invocations: u64,
    max_output_rows: u64,
    max_depth: u16,
}

impl LogicalApply {
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new(
        identity: ChildPlanId,
        kind: ApplyKind,
        imports: Vec<ApplySlotMapping>,
        exports: Vec<ApplySlotMapping>,
        child_plan: LogicalPlan,
        max_invocations: u64,
        max_output_rows: u64,
        max_depth: u16,
    ) -> Self {
        Self {
            identity,
            kind,
            imports,
            exports,
            child_plan: Box::new(child_plan),
            max_invocations,
            max_output_rows,
            max_depth,
        }
    }

    #[must_use]
    pub const fn identity(&self) -> ChildPlanId {
        self.identity
    }

    #[must_use]
    pub const fn kind(&self) -> ApplyKind {
        self.kind
    }

    #[must_use]
    pub fn imports(&self) -> &[ApplySlotMapping] {
        &self.imports
    }

    #[must_use]
    pub fn exports(&self) -> &[ApplySlotMapping] {
        &self.exports
    }

    #[must_use]
    pub fn child_plan(&self) -> &LogicalPlan {
        &self.child_plan
    }

    #[must_use]
    pub const fn max_invocations(&self) -> u64 {
        self.max_invocations
    }

    #[must_use]
    pub const fn max_output_rows(&self) -> u64 {
        self.max_output_rows
    }

    #[must_use]
    pub const fn max_depth(&self) -> u16 {
        self.max_depth
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicalBatchSubtransaction {
    apply: LogicalApply,
    batch_rows: u32,
}

impl LogicalBatchSubtransaction {
    #[must_use]
    pub const fn new(apply: LogicalApply, batch_rows: u32) -> Self {
        Self { apply, batch_rows }
    }

    #[must_use]
    pub const fn apply(&self) -> &LogicalApply {
        &self.apply
    }

    #[must_use]
    pub const fn batch_rows(&self) -> u32 {
        self.batch_rows
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SortKey {
    slot: SlotId,
    ascending: bool,
}

impl SortKey {
    #[must_use]
    pub const fn new(slot: SlotId, ascending: bool) -> Self {
        Self { slot, ascending }
    }

    #[must_use]
    pub const fn slot(self) -> SlotId {
        self.slot
    }

    #[must_use]
    pub const fn ascending(self) -> bool {
        self.ascending
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ValidTimeSpec {
    Current,
    AsOf(ScalarExpr),
    Between { start: ScalarExpr, end: ScalarExpr },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TransactionTimeSpec {
    Current,
    AsOf(ScalarExpr),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TemporalJoinKind {
    Inner,
    Left,
}

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
        types: Vec<u32>,
    },
    Filter {
        predicate: ScalarExpr,
    },
    Project {
        expressions: Vec<(SlotId, ScalarExpr)>,
    },
    Unwind {
        expression: ScalarExpr,
        binding: SlotId,
    },
    Aggregate {
        grouping: Vec<SlotId>,
        aggregates: Vec<(SlotId, ScalarExpr)>,
    },
    Sort {
        keys: Vec<SortKey>,
    },
    Skip {
        count: ScalarExpr,
    },
    Limit {
        count: ScalarExpr,
    },
    InnerJoin,
    LeftJoin,
    TemporalJoin {
        kind: TemporalJoinKind,
        keys: Vec<SlotId>,
    },
    Union {
        all: bool,
    },
    TemporalSlice {
        valid_time: ValidTimeSpec,
        transaction_time: TransactionTimeSpec,
    },
    Diff,
    Create,
    Merge,
    Set,
    Remove,
    Delete {
        detach: bool,
    },
    ProcedureCall {
        procedure: ResolvedProcedure,
    },
    Apply {
        apply: LogicalApply,
    },
    BatchSubtransaction {
        batch: LogicalBatchSubtransaction,
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
    header: PlanHeader,
    nodes: Vec<LogicalNode>,
    root: LogicalNodeId,
    output: RowSchema,
}

impl LogicalPlan {
    #[must_use]
    pub const fn from_parts(
        header: PlanHeader,
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
        let mut identities = std::collections::BTreeSet::new();
        let mut total_nodes = 0_usize;
        validate_apply_tree(self, 0, &mut identities, &mut total_nodes)?;
        Ok(())
    }

    #[must_use]
    pub const fn header(&self) -> &PlanHeader {
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
    header: PlanHeader,
    nodes: Vec<LogicalNode>,
}

impl LogicalPlanBuilder {
    #[must_use]
    pub const fn new(header: PlanHeader) -> Self {
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
        LogicalOperator::InnerJoin
        | LogicalOperator::LeftJoin
        | LogicalOperator::TemporalJoin { .. }
        | LogicalOperator::Union { .. } => 2,
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
        LogicalOperator::Unwind {
            expression,
            binding,
        } => {
            required_outputs.push(*binding);
            expression.visit_slots(&mut |slot| referenced.push(slot));
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
            for key in keys {
                referenced.push(key.slot());
            }
        }
        LogicalOperator::Skip { count } | LogicalOperator::Limit { count } => {
            count.visit_slots(&mut |slot| referenced.push(slot));
        }
        LogicalOperator::TemporalSlice {
            valid_time,
            transaction_time,
        } => {
            match valid_time {
                ValidTimeSpec::Current => {}
                ValidTimeSpec::AsOf(expression) => {
                    expression.visit_slots(&mut |slot| referenced.push(slot));
                }
                ValidTimeSpec::Between { start, end } => {
                    start.visit_slots(&mut |slot| referenced.push(slot));
                    end.visit_slots(&mut |slot| referenced.push(slot));
                }
            }
            if let TransactionTimeSpec::AsOf(expression) = transaction_time {
                expression.visit_slots(&mut |slot| referenced.push(slot));
            }
        }
        LogicalOperator::TemporalJoin { kind, keys } => {
            validate_temporal_join(node, preceding, *kind, keys)?;
        }
        LogicalOperator::ProcedureCall { procedure } => {
            if procedure.identity().authority_key() == [0; 32]
                || procedure.identity().catalog_revision() == 0
                || procedure.identity().procedure_revision() == 0
                || procedure.name().is_empty()
                || procedure.max_invocations() == 0
                || procedure.max_input_rows() == 0
                || procedure.max_output_rows() == 0
                || procedure.max_value_bytes() == 0
                || procedure.max_result_bytes() == 0
            {
                return Err(ValidationError::InvalidProcedureIdentity);
            }
            let mut names = std::collections::BTreeSet::new();
            for argument in procedure.arguments() {
                if argument.name().is_empty() || !names.insert(argument.name()) {
                    return Err(ValidationError::InvalidProcedureArguments);
                }
                argument
                    .expression()
                    .visit_slots(&mut |slot| referenced.push(slot));
            }
            let input = node
                .inputs
                .first()
                .and_then(|input| {
                    preceding.get(usize::try_from(input.value()).unwrap_or(usize::MAX))
                })
                .ok_or(ValidationError::ProcedureSchemaMismatch)?;
            if node.output.columns().len()
                != input
                    .output
                    .columns()
                    .len()
                    .saturating_add(procedure.yields().len())
                || input.output.columns().iter().any(|column| {
                    !node
                        .output
                        .columns()
                        .iter()
                        .any(|candidate| candidate == column)
                })
            {
                return Err(ValidationError::ProcedureSchemaMismatch);
            }
            for binding in procedure.yields() {
                let source = procedure
                    .provider_output()
                    .columns()
                    .get(usize::try_from(binding.source_index()).unwrap_or(usize::MAX))
                    .ok_or(ValidationError::ProcedureSchemaMismatch)?;
                let output = node
                    .output
                    .columns()
                    .iter()
                    .find(|column| column.slot() == binding.output_slot())
                    .ok_or(ValidationError::ProcedureSchemaMismatch)?;
                if source.value_type() != output.value_type()
                    || source.nullable() != output.nullable()
                {
                    return Err(ValidationError::ProcedureSchemaMismatch);
                }
            }
        }
        LogicalOperator::Apply { apply } => {
            validate_apply(node_id, node, preceding, apply)?;
        }
        LogicalOperator::BatchSubtransaction { batch } => {
            if batch.batch_rows == 0
                || batch.batch_rows > MAX_BATCH_SUBTRANSACTION_ROWS
                || batch.apply.kind != ApplyKind::Inner
            {
                return Err(ValidationError::InvalidBatchSubtransaction);
            }
            validate_apply(node_id, node, preceding, &batch.apply)?;
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

fn validate_temporal_join(
    node: &LogicalNode,
    preceding: &[LogicalNode],
    kind: TemporalJoinKind,
    keys: &[SlotId],
) -> Result<(), ValidationError> {
    let [left_id, right_id] = node.inputs.as_slice() else {
        return Err(ValidationError::TemporalJoinSchemaMismatch);
    };
    let left = preceding
        .get(usize::try_from(left_id.value()).unwrap_or(usize::MAX))
        .ok_or(ValidationError::TemporalJoinSchemaMismatch)?;
    let right = preceding
        .get(usize::try_from(right_id.value()).unwrap_or(usize::MAX))
        .ok_or(ValidationError::TemporalJoinSchemaMismatch)?;
    let key_slots = keys
        .iter()
        .copied()
        .collect::<std::collections::BTreeSet<_>>();
    if key_slots.len() != keys.len() {
        return Err(ValidationError::TemporalJoinSchemaMismatch);
    }
    for key in keys {
        let left_column = left
            .output
            .columns()
            .iter()
            .find(|column| column.slot() == *key)
            .ok_or(ValidationError::TemporalJoinSchemaMismatch)?;
        let right_column = right
            .output
            .columns()
            .iter()
            .find(|column| column.slot() == *key)
            .ok_or(ValidationError::TemporalJoinSchemaMismatch)?;
        if left_column.value_type() != right_column.value_type() {
            return Err(ValidationError::TemporalJoinSchemaMismatch);
        }
    }
    let left_slots = left
        .output
        .columns()
        .iter()
        .map(Column::slot)
        .collect::<std::collections::BTreeSet<_>>();
    let mut columns = left.output.columns().to_vec();
    for column in right.output.columns() {
        if left_slots.contains(&column.slot()) {
            if !key_slots.contains(&column.slot()) {
                return Err(ValidationError::TemporalJoinSchemaMismatch);
            }
            continue;
        }
        columns.push(Column::new(
            column.slot(),
            column.name(),
            column.value_type().clone(),
            column.nullable() || kind == TemporalJoinKind::Left,
        ));
    }
    let expected =
        RowSchema::new(columns).map_err(|_| ValidationError::TemporalJoinSchemaMismatch)?;
    if node.output != expected {
        return Err(ValidationError::TemporalJoinSchemaMismatch);
    }
    Ok(())
}

fn validate_apply(
    node_id: LogicalNodeId,
    node: &LogicalNode,
    preceding: &[LogicalNode],
    apply: &LogicalApply,
) -> Result<(), ValidationError> {
    if apply.identity.value() == 0 {
        return Err(ValidationError::InvalidApplyIdentity);
    }
    if apply.max_invocations == 0
        || apply.max_invocations > MAX_APPLY_INVOCATIONS
        || apply.max_output_rows == 0
        || apply.max_output_rows > MAX_APPLY_OUTPUT_ROWS
        || apply.max_depth == 0
        || apply.max_depth > MAX_APPLY_DEPTH
    {
        return Err(ValidationError::InvalidApplyBudget);
    }
    apply.child_plan.validate()?;
    let parent = node
        .inputs
        .first()
        .and_then(|input| preceding.get(usize::try_from(input.value()).unwrap_or(usize::MAX)))
        .ok_or(ValidationError::ApplySchemaMismatch)?;
    if node.output.columns().get(..parent.output.columns().len()) != Some(parent.output.columns()) {
        return Err(ValidationError::ApplySchemaMismatch);
    }
    let child_argument = apply
        .child_plan
        .nodes()
        .first()
        .filter(|node| matches!(node.operator(), LogicalOperator::Argument))
        .ok_or(ValidationError::ApplySchemaMismatch)?;
    let mut parent_imports = std::collections::BTreeSet::new();
    let mut child_imports = std::collections::BTreeSet::new();
    for mapping in &apply.imports {
        if !parent_imports.insert(mapping.parent_slot) || !child_imports.insert(mapping.child_slot)
        {
            return Err(ValidationError::ApplySchemaMismatch);
        }
        let parent_column = parent
            .output
            .columns()
            .iter()
            .find(|column| column.slot() == mapping.parent_slot)
            .ok_or(ValidationError::UnknownSlot {
                node: node_id,
                slot: mapping.parent_slot,
            })?;
        let child_column = child_argument
            .output
            .columns()
            .iter()
            .find(|column| column.slot() == mapping.child_slot)
            .ok_or(ValidationError::ApplySchemaMismatch)?;
        if parent_column.name() != child_column.name()
            || parent_column.value_type() != child_column.value_type()
        {
            return Err(ValidationError::ApplySchemaMismatch);
        }
    }
    if child_argument.output.columns().len() != apply.imports.len() {
        return Err(ValidationError::ApplySchemaMismatch);
    }
    match apply.kind {
        ApplyKind::Inner => {
            if node.output.columns().len()
                != parent
                    .output
                    .columns()
                    .len()
                    .saturating_add(apply.exports.len())
            {
                return Err(ValidationError::ApplySchemaMismatch);
            }
            let mut parent_exports = std::collections::BTreeSet::new();
            let mut child_exports = std::collections::BTreeSet::new();
            for mapping in &apply.exports {
                if !parent_exports.insert(mapping.parent_slot)
                    || !child_exports.insert(mapping.child_slot)
                    || parent.output.contains(mapping.parent_slot)
                {
                    return Err(ValidationError::ApplySchemaMismatch);
                }
                let parent_column = node
                    .output
                    .columns()
                    .iter()
                    .find(|column| column.slot() == mapping.parent_slot)
                    .ok_or(ValidationError::ApplySchemaMismatch)?;
                let child_column = apply
                    .child_plan
                    .output()
                    .columns()
                    .iter()
                    .find(|column| column.slot() == mapping.child_slot)
                    .ok_or(ValidationError::ApplySchemaMismatch)?;
                if parent_column.name() != child_column.name()
                    || parent_column.value_type() != child_column.value_type()
                    || parent_column.nullable() != child_column.nullable()
                {
                    return Err(ValidationError::ApplySchemaMismatch);
                }
            }
        }
        ApplyKind::Exists { output } | ApplyKind::Count { output } => {
            if !apply.exports.is_empty()
                || parent.output.contains(output)
                || node.output.columns().len() != parent.output.columns().len() + 1
            {
                return Err(ValidationError::ApplySchemaMismatch);
            }
            let result = node
                .output
                .columns()
                .last()
                .filter(|column| column.slot() == output)
                .ok_or(ValidationError::ApplySchemaMismatch)?;
            let expected = if matches!(apply.kind, ApplyKind::Exists { .. }) {
                super::ValueType::Boolean
            } else {
                super::ValueType::Integer
            };
            if result.value_type() != &expected || result.nullable() {
                return Err(ValidationError::ApplySchemaMismatch);
            }
        }
    }
    Ok(())
}

fn validate_apply_tree(
    plan: &LogicalPlan,
    depth: u16,
    identities: &mut std::collections::BTreeSet<ChildPlanId>,
    total_nodes: &mut usize,
) -> Result<(), ValidationError> {
    *total_nodes = total_nodes.saturating_add(plan.nodes.len());
    if *total_nodes > MAX_LOGICAL_NODES {
        return Err(ValidationError::TooManyNodes {
            max: MAX_LOGICAL_NODES,
            actual: *total_nodes,
        });
    }
    for node in &plan.nodes {
        let apply = match node.operator() {
            LogicalOperator::Apply { apply } => Some(apply),
            LogicalOperator::BatchSubtransaction { batch } => Some(batch.apply()),
            _ => None,
        };
        if let Some(apply) = apply {
            if depth >= MAX_APPLY_DEPTH || depth >= apply.max_depth {
                return Err(ValidationError::ApplyDepthExceeded);
            }
            if !identities.insert(apply.identity) {
                return Err(ValidationError::DuplicateChildPlanIdentity(apply.identity));
            }
            if apply.child_plan.header.graph_id() != plan.header.graph_id()
                || apply.child_plan.header.schema_version() != plan.header.schema_version()
                || apply.child_plan.header.topology_epoch() != plan.header.topology_epoch()
                || apply.child_plan.header.language_profile() != plan.header.language_profile()
                || apply.child_plan.header.semantic_baseline() != plan.header.semantic_baseline()
                || apply.child_plan.header.query_fingerprint() == plan.header.query_fingerprint()
            {
                return Err(ValidationError::ApplyHeaderMismatch);
            }
            validate_apply_tree(&apply.child_plan, depth + 1, identities, total_nodes)?;
        }
    }
    Ok(())
}

const _: u16 = PLAN_VERSION;
