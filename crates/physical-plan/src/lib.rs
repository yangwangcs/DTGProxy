#![forbid(unsafe_code)]

use std::collections::BTreeSet;
use std::error::Error;
use std::fmt::{self, Display, Formatter};

use temporal_ir::{
    ApplyKind, ApplySlotMapping, ChildPlanId, Column, MAX_APPLY_DEPTH, MAX_APPLY_INVOCATIONS,
    MAX_APPLY_OUTPUT_ROWS, MAX_BATCH_SUBTRANSACTION_ROWS, ProcedurePlacement, ResolvedProcedure,
    RowSchema, ScalarExpr, SlotId, SortKey, TransactionTimeSpec, ValidTimeSpec,
};

pub const PHYSICAL_PLAN_VERSION: u16 = 1;
pub const MAX_FRAGMENTS: usize = 65_536;
pub const MAX_EXCHANGES: usize = 131_072;
pub const MAX_RECURSIVE_PLAN_NODES: usize = 4_096;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalPlanHeader {
    version: u16,
    graph_id: u64,
    schema_version: u64,
    topology_epoch: u64,
    query_fingerprint: [u8; 32],
    expected_shards: Vec<u32>,
}

impl PhysicalPlanHeader {
    pub fn new(
        graph_id: u64,
        schema_version: u64,
        topology_epoch: u64,
        query_fingerprint: [u8; 32],
    ) -> Result<Self, ValidationError> {
        let header = Self {
            version: PHYSICAL_PLAN_VERSION,
            graph_id,
            schema_version,
            topology_epoch,
            query_fingerprint,
            expected_shards: vec![0],
        };
        header.validate()?;
        Ok(header)
    }

    pub fn with_expected_shards(
        mut self,
        expected_shards: Vec<u32>,
    ) -> Result<Self, ValidationError> {
        let unique = expected_shards.iter().copied().collect::<BTreeSet<_>>();
        if expected_shards.is_empty() || unique.len() != expected_shards.len() {
            return Err(ValidationError::InvalidExpectedShards);
        }
        self.expected_shards = expected_shards;
        Ok(self)
    }

    fn validate(&self) -> Result<(), ValidationError> {
        if self.version != PHYSICAL_PLAN_VERSION {
            return Err(ValidationError::UnsupportedVersion {
                expected: PHYSICAL_PLAN_VERSION,
                actual: self.version,
            });
        }
        if self.graph_id == 0
            || self.schema_version == 0
            || self.topology_epoch == 0
            || self.query_fingerprint == [0; 32]
        {
            return Err(ValidationError::InvalidHeader);
        }
        let unique = self
            .expected_shards
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        if self.expected_shards.is_empty() || unique.len() != self.expected_shards.len() {
            return Err(ValidationError::InvalidExpectedShards);
        }
        Ok(())
    }

    #[must_use]
    pub const fn graph_id(&self) -> u64 {
        self.graph_id
    }

    #[must_use]
    pub const fn schema_version(&self) -> u64 {
        self.schema_version
    }

    #[must_use]
    pub const fn topology_epoch(&self) -> u64 {
        self.topology_epoch
    }

    #[must_use]
    pub const fn query_fingerprint(&self) -> [u8; 32] {
        self.query_fingerprint
    }

    #[must_use]
    pub fn expected_shards(&self) -> &[u32] {
        &self.expected_shards
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FragmentId(u32);

impl FragmentId {
    #[must_use]
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn value(self) -> u32 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ExchangeId(u32);

impl ExchangeId {
    #[must_use]
    pub const fn new(value: u32) -> Self {
        Self(value)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Placement {
    Coordinator,
    Shard(u32),
    AllShards,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MemoryBudget {
    memory_bytes: u64,
    spill_bytes: u64,
}

impl MemoryBudget {
    pub fn new(memory_bytes: u64, spill_bytes: u64) -> Result<Self, ValidationError> {
        if memory_bytes == 0 || spill_bytes == 0 {
            return Err(ValidationError::InvalidMemoryBudget);
        }
        Ok(Self {
            memory_bytes,
            spill_bytes,
        })
    }

    #[must_use]
    pub const fn memory_bytes(self) -> u64 {
        self.memory_bytes
    }

    #[must_use]
    pub const fn spill_bytes(self) -> u64 {
        self.spill_bytes
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum JoinKind {
    Inner,
    Left,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WriteOperation {
    Create,
    Merge,
    Set,
    Remove,
    Delete { detach: bool },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PhysicalOperator {
    Argument {
        output: RowSchema,
    },
    NodeScan {
        binding: SlotId,
        labels: Vec<u32>,
        output: RowSchema,
    },
    RelationshipScan {
        binding: SlotId,
        types: Vec<u32>,
        output: RowSchema,
    },
    Expand {
        source: SlotId,
        relationship: SlotId,
        destination: SlotId,
        outgoing: bool,
        types: Vec<u32>,
        output: RowSchema,
    },
    Filter(ScalarExpr),
    Project {
        expressions: Vec<(SlotId, ScalarExpr)>,
        output: RowSchema,
    },
    Unwind {
        expression: ScalarExpr,
        binding: SlotId,
        output: RowSchema,
    },
    HashJoin {
        kind: JoinKind,
        keys: Vec<SlotId>,
    },
    Aggregate {
        grouping: Vec<SlotId>,
        aggregates: Vec<(SlotId, ScalarExpr)>,
        output: RowSchema,
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
    Union {
        all: bool,
    },
    TemporalSlice {
        valid_time: ValidTimeSpec,
        transaction_time: TransactionTimeSpec,
    },
    Diff,
    Write {
        operation: WriteOperation,
        output: RowSchema,
    },
    Procedure {
        procedure: ResolvedProcedure,
        output: RowSchema,
    },
    Apply {
        apply: PhysicalApply,
        output: RowSchema,
    },
    BatchSubtransaction {
        apply: PhysicalApply,
        batch_rows: u32,
        output: RowSchema,
    },
    Finish,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalApply {
    identity: ChildPlanId,
    kind: ApplyKind,
    imports: Vec<ApplySlotMapping>,
    exports: Vec<ApplySlotMapping>,
    child_input: RowSchema,
    child_plan: Box<PhysicalPlan>,
    max_invocations: u64,
    max_output_rows: u64,
    max_depth: u16,
}

impl PhysicalApply {
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new(
        identity: ChildPlanId,
        kind: ApplyKind,
        imports: Vec<ApplySlotMapping>,
        exports: Vec<ApplySlotMapping>,
        child_input: RowSchema,
        child_plan: PhysicalPlan,
        max_invocations: u64,
        max_output_rows: u64,
        max_depth: u16,
    ) -> Self {
        Self {
            identity,
            kind,
            imports,
            exports,
            child_input,
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
    pub const fn child_input(&self) -> &RowSchema {
        &self.child_input
    }

    #[must_use]
    pub fn child_plan(&self) -> &PhysicalPlan {
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
pub struct PlanFragment {
    id: FragmentId,
    placement: Placement,
    operators: Vec<PhysicalOperator>,
    output: RowSchema,
    budget: MemoryBudget,
}

impl PlanFragment {
    #[must_use]
    pub const fn id(&self) -> FragmentId {
        self.id
    }

    #[must_use]
    pub const fn placement(&self) -> Placement {
        self.placement
    }

    #[must_use]
    pub fn operators(&self) -> &[PhysicalOperator] {
        &self.operators
    }

    #[must_use]
    pub const fn output(&self) -> &RowSchema {
        &self.output
    }

    #[must_use]
    pub const fn budget(&self) -> MemoryBudget {
        self.budget
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExchangeKind {
    Gather,
    Broadcast,
    HashPartition(Vec<SlotId>),
    RangePartition(Vec<SlotId>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Exchange {
    id: ExchangeId,
    from: FragmentId,
    to: FragmentId,
    kind: ExchangeKind,
    schema: RowSchema,
    max_inflight_batches: u32,
}

impl Exchange {
    #[must_use]
    pub const fn id(&self) -> ExchangeId {
        self.id
    }

    #[must_use]
    pub const fn from(&self) -> FragmentId {
        self.from
    }

    #[must_use]
    pub const fn to(&self) -> FragmentId {
        self.to
    }

    #[must_use]
    pub const fn kind(&self) -> &ExchangeKind {
        &self.kind
    }

    #[must_use]
    pub const fn schema(&self) -> &RowSchema {
        &self.schema
    }

    #[must_use]
    pub const fn max_inflight_batches(&self) -> u32 {
        self.max_inflight_batches
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalPlan {
    header: PhysicalPlanHeader,
    fragments: Vec<PlanFragment>,
    exchanges: Vec<Exchange>,
    root: FragmentId,
}

impl PhysicalPlan {
    pub fn validate(&self) -> Result<(), ValidationError> {
        self.header.validate()?;
        if self.fragments.is_empty() || self.fragments.len() > MAX_FRAGMENTS {
            return Err(ValidationError::InvalidFragmentCount);
        }
        if self.exchanges.len() > MAX_EXCHANGES {
            return Err(ValidationError::TooManyExchanges);
        }
        if self
            .fragments
            .get(usize::try_from(self.root.value()).unwrap_or(usize::MAX))
            .is_none()
        {
            return Err(ValidationError::InvalidRoot(self.root));
        }
        for (index, fragment) in self.fragments.iter().enumerate() {
            if fragment.id.value() != u32::try_from(index).unwrap_or(u32::MAX)
                || fragment.operators.is_empty()
            {
                return Err(ValidationError::InvalidFragment(fragment.id));
            }
            let incoming = self
                .exchanges
                .iter()
                .filter(|exchange| exchange.to == fragment.id)
                .collect::<Vec<_>>();
            let mut current_schema = match incoming.as_slice() {
                [] => RowSchema::empty(),
                [exchange] => exchange.schema.clone(),
                exchanges => match fragment.operators.first() {
                    Some(PhysicalOperator::Union { .. }) => {
                        let schema = exchanges[0].schema.clone();
                        for exchange in &exchanges[1..] {
                            if exchange.schema != schema {
                                return Err(ValidationError::ExchangeSchemaMismatch(exchange.id));
                            }
                        }
                        schema
                    }
                    Some(PhysicalOperator::HashJoin { kind, keys }) if exchanges.len() == 2 => {
                        derive_hash_join_schema(
                            fragment.id,
                            kind,
                            keys,
                            &exchanges[0].schema,
                            &exchanges[1].schema,
                        )?
                    }
                    _ => return Err(ValidationError::InvalidFragment(fragment.id)),
                },
            };
            for (operator_index, operator) in fragment.operators.iter().enumerate() {
                match operator {
                    PhysicalOperator::Argument { output } => current_schema = output.clone(),
                    PhysicalOperator::NodeScan { output, .. }
                    | PhysicalOperator::RelationshipScan { output, .. }
                    | PhysicalOperator::Expand { output, .. }
                    | PhysicalOperator::Project { output, .. }
                    | PhysicalOperator::Unwind { output, .. }
                    | PhysicalOperator::Aggregate { output, .. }
                    | PhysicalOperator::Write { output, .. } => {
                        current_schema = output.clone();
                    }
                    PhysicalOperator::Procedure { procedure, output } => {
                        validate_procedure(fragment.placement, procedure, &current_schema, output)?;
                        current_schema = output.clone();
                    }
                    PhysicalOperator::Apply { apply, output } => {
                        validate_apply(&self.header, &current_schema, apply, output)?;
                        current_schema = output.clone();
                    }
                    PhysicalOperator::BatchSubtransaction {
                        apply,
                        batch_rows,
                        output,
                    } => {
                        if *batch_rows == 0
                            || *batch_rows > MAX_BATCH_SUBTRANSACTION_ROWS
                            || apply.kind() != ApplyKind::Inner
                        {
                            return Err(ValidationError::InvalidBatchSubtransaction);
                        }
                        validate_apply(&self.header, &current_schema, apply, output)?;
                        current_schema = output.clone();
                    }
                    PhysicalOperator::HashJoin { .. }
                        if operator_index != 0 || incoming.len() != 2 =>
                    {
                        return Err(ValidationError::HashJoinSchemaMismatch(fragment.id));
                    }
                    _ => {}
                }
            }
            if current_schema != fragment.output {
                return Err(ValidationError::FragmentOutputMismatch(fragment.id));
            }
        }
        for exchange in &self.exchanges {
            validate_exchange(&self.fragments, exchange)?;
        }
        let mut identities = BTreeSet::new();
        let mut node_count = 0_usize;
        validate_apply_tree(self, 0, &mut identities, &mut node_count)?;
        Ok(())
    }

    #[must_use]
    pub const fn header(&self) -> &PhysicalPlanHeader {
        &self.header
    }

    #[must_use]
    pub fn fragments(&self) -> &[PlanFragment] {
        &self.fragments
    }

    #[must_use]
    pub fn exchanges(&self) -> &[Exchange] {
        &self.exchanges
    }

    #[must_use]
    pub const fn root(&self) -> FragmentId {
        self.root
    }

    #[must_use]
    pub fn output(&self) -> Option<&RowSchema> {
        self.fragments
            .get(usize::try_from(self.root.value()).unwrap_or(usize::MAX))
            .map(PlanFragment::output)
    }
}

fn validate_apply(
    parent_header: &PhysicalPlanHeader,
    parent_input: &RowSchema,
    apply: &PhysicalApply,
    output: &RowSchema,
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
    validate_argument_source(apply.child_plan(), apply.child_input())?;
    let child_header = apply.child_plan.header();
    if child_header.graph_id() != parent_header.graph_id()
        || child_header.schema_version() != parent_header.schema_version()
        || child_header.topology_epoch() != parent_header.topology_epoch()
        || child_header.expected_shards() != parent_header.expected_shards()
        || child_header.query_fingerprint() == parent_header.query_fingerprint()
    {
        return Err(ValidationError::ApplyHeaderMismatch);
    }
    if output.columns().get(..parent_input.columns().len()) != Some(parent_input.columns())
        || apply.child_input.columns().len() != apply.imports.len()
    {
        return Err(ValidationError::ApplySchemaMismatch);
    }
    let mut parent_imports = BTreeSet::new();
    let mut child_imports = BTreeSet::new();
    for mapping in &apply.imports {
        if !parent_imports.insert(mapping.parent_slot())
            || !child_imports.insert(mapping.child_slot())
        {
            return Err(ValidationError::ApplySchemaMismatch);
        }
        let parent = parent_input
            .columns()
            .iter()
            .find(|column| column.slot() == mapping.parent_slot())
            .ok_or(ValidationError::ApplySchemaMismatch)?;
        let child = apply
            .child_input
            .columns()
            .iter()
            .find(|column| column.slot() == mapping.child_slot())
            .ok_or(ValidationError::ApplySchemaMismatch)?;
        if parent.name() != child.name()
            || parent.value_type() != child.value_type()
            || parent.nullable() != child.nullable()
        {
            return Err(ValidationError::ApplySchemaMismatch);
        }
    }
    match apply.kind {
        ApplyKind::Inner => {
            if output.columns().len()
                != parent_input
                    .columns()
                    .len()
                    .saturating_add(apply.exports.len())
            {
                return Err(ValidationError::ApplySchemaMismatch);
            }
            let mut parent_exports = BTreeSet::new();
            let mut child_exports = BTreeSet::new();
            for mapping in &apply.exports {
                if !parent_exports.insert(mapping.parent_slot())
                    || !child_exports.insert(mapping.child_slot())
                {
                    return Err(ValidationError::ApplySchemaMismatch);
                }
                let parent = output
                    .columns()
                    .iter()
                    .find(|column| column.slot() == mapping.parent_slot())
                    .ok_or(ValidationError::ApplySchemaMismatch)?;
                let child = apply
                    .child_plan
                    .output()
                    .ok_or(ValidationError::ApplySchemaMismatch)?
                    .columns()
                    .iter()
                    .find(|column| column.slot() == mapping.child_slot())
                    .ok_or(ValidationError::ApplySchemaMismatch)?;
                if parent.name() != child.name()
                    || parent.value_type() != child.value_type()
                    || parent.nullable() != child.nullable()
                {
                    return Err(ValidationError::ApplySchemaMismatch);
                }
            }
        }
        ApplyKind::Exists { output: result } | ApplyKind::Count { output: result } => {
            if !apply.exports.is_empty()
                || output.columns().len() != parent_input.columns().len() + 1
            {
                return Err(ValidationError::ApplySchemaMismatch);
            }
            let result_column = output
                .columns()
                .last()
                .filter(|column| column.slot() == result)
                .ok_or(ValidationError::ApplySchemaMismatch)?;
            let expected = if matches!(apply.kind, ApplyKind::Exists { .. }) {
                temporal_ir::ValueType::Boolean
            } else {
                temporal_ir::ValueType::Integer
            };
            if result_column.value_type() != &expected || result_column.nullable() {
                return Err(ValidationError::ApplySchemaMismatch);
            }
        }
    }
    Ok(())
}

fn validate_apply_tree(
    plan: &PhysicalPlan,
    depth: u16,
    identities: &mut BTreeSet<ChildPlanId>,
    node_count: &mut usize,
) -> Result<(), ValidationError> {
    for fragment in plan.fragments() {
        *node_count = node_count
            .checked_add(fragment.operators().len())
            .ok_or(ValidationError::RecursivePlanNodeLimit)?;
        if *node_count > MAX_RECURSIVE_PLAN_NODES {
            return Err(ValidationError::RecursivePlanNodeLimit);
        }
        for operator in fragment.operators() {
            let apply = match operator {
                PhysicalOperator::Apply { apply, .. }
                | PhysicalOperator::BatchSubtransaction { apply, .. } => Some(apply),
                _ => None,
            };
            if let Some(apply) = apply {
                if depth >= MAX_APPLY_DEPTH || depth >= apply.max_depth {
                    return Err(ValidationError::ApplyDepthExceeded);
                }
                if !identities.insert(apply.identity) {
                    return Err(ValidationError::DuplicateChildPlanIdentity(apply.identity));
                }
                validate_apply_tree(&apply.child_plan, depth + 1, identities, node_count)?;
            }
        }
    }
    Ok(())
}

fn validate_argument_source(
    plan: &PhysicalPlan,
    child_input: &RowSchema,
) -> Result<(), ValidationError> {
    let arguments = plan
        .fragments()
        .iter()
        .flat_map(|fragment| {
            fragment
                .operators()
                .iter()
                .enumerate()
                .filter_map(move |(index, operator)| match operator {
                    PhysicalOperator::Argument { output } => Some((fragment, index, output)),
                    _ => None,
                })
        })
        .collect::<Vec<_>>();
    let [(fragment, 0, output)] = arguments.as_slice() else {
        return Err(ValidationError::InvalidApplyArgumentSource);
    };
    if fragment.placement() != Placement::Coordinator
        || plan
            .exchanges()
            .iter()
            .any(|exchange| exchange.to() == fragment.id())
        || *output != child_input
    {
        return Err(ValidationError::InvalidApplyArgumentSource);
    }
    Ok(())
}

fn derive_hash_join_schema(
    fragment: FragmentId,
    kind: &JoinKind,
    keys: &[SlotId],
    left: &RowSchema,
    right: &RowSchema,
) -> Result<RowSchema, ValidationError> {
    let key_slots = keys.iter().copied().collect::<BTreeSet<_>>();
    if key_slots.len() != keys.len() {
        return Err(ValidationError::HashJoinSchemaMismatch(fragment));
    }
    for key in keys {
        let left_column = left
            .columns()
            .iter()
            .find(|column| column.slot() == *key)
            .ok_or(ValidationError::HashJoinSchemaMismatch(fragment))?;
        let right_column = right
            .columns()
            .iter()
            .find(|column| column.slot() == *key)
            .ok_or(ValidationError::HashJoinSchemaMismatch(fragment))?;
        if left_column.value_type() != right_column.value_type() {
            return Err(ValidationError::HashJoinSchemaMismatch(fragment));
        }
    }

    let left_slots = left
        .columns()
        .iter()
        .map(Column::slot)
        .collect::<BTreeSet<_>>();
    let mut columns = left.columns().to_vec();
    for column in right.columns() {
        if left_slots.contains(&column.slot()) {
            if !key_slots.contains(&column.slot()) {
                return Err(ValidationError::HashJoinSchemaMismatch(fragment));
            }
            continue;
        }
        columns.push(Column::new(
            column.slot(),
            column.name(),
            column.value_type().clone(),
            column.nullable() || matches!(kind, JoinKind::Left),
        ));
    }
    RowSchema::new(columns).map_err(|_| ValidationError::HashJoinSchemaMismatch(fragment))
}

fn validate_procedure(
    placement: Placement,
    procedure: &ResolvedProcedure,
    input: &RowSchema,
    output: &RowSchema,
) -> Result<(), ValidationError> {
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
        return Err(ValidationError::InvalidProcedure);
    }
    let placement_matches = matches!(
        (procedure.placement(), placement),
        (ProcedurePlacement::Coordinator, Placement::Coordinator)
            | (
                ProcedurePlacement::ShardLocal,
                Placement::Shard(_) | Placement::AllShards
            )
    );
    if !placement_matches {
        return Err(ValidationError::ProcedurePlacementMismatch);
    }
    let mut argument_names = BTreeSet::new();
    for argument in procedure.arguments() {
        if argument.name().is_empty() || !argument_names.insert(argument.name()) {
            return Err(ValidationError::InvalidProcedure);
        }
        let mut unknown_slot = false;
        argument.expression().visit_slots(&mut |slot| {
            unknown_slot |= !input.contains(slot);
        });
        if unknown_slot {
            return Err(ValidationError::ProcedureSchemaMismatch);
        }
    }
    if output.columns().len() != input.columns().len() + procedure.yields().len()
        || output.columns().get(..input.columns().len()) != Some(input.columns())
    {
        return Err(ValidationError::ProcedureSchemaMismatch);
    }
    for (offset, binding) in procedure.yields().iter().enumerate() {
        let source = procedure
            .provider_output()
            .columns()
            .get(usize::try_from(binding.source_index()).unwrap_or(usize::MAX))
            .ok_or(ValidationError::ProcedureSchemaMismatch)?;
        let target = output
            .columns()
            .get(input.columns().len() + offset)
            .ok_or(ValidationError::ProcedureSchemaMismatch)?;
        if target.slot() != binding.output_slot()
            || source.value_type() != target.value_type()
            || source.nullable() != target.nullable()
        {
            return Err(ValidationError::ProcedureSchemaMismatch);
        }
    }
    Ok(())
}

pub struct PhysicalPlanBuilder {
    header: PhysicalPlanHeader,
    fragments: Vec<PlanFragment>,
    exchanges: Vec<Exchange>,
}

impl PhysicalPlanBuilder {
    #[must_use]
    pub const fn new(header: PhysicalPlanHeader) -> Self {
        Self {
            header,
            fragments: Vec::new(),
            exchanges: Vec::new(),
        }
    }

    pub fn add_fragment(
        &mut self,
        placement: Placement,
        operators: Vec<PhysicalOperator>,
        output: RowSchema,
        budget: MemoryBudget,
    ) -> Result<FragmentId, ValidationError> {
        if operators.is_empty() || self.fragments.len() >= MAX_FRAGMENTS {
            return Err(ValidationError::InvalidFragmentCount);
        }
        let id = FragmentId::new(
            u32::try_from(self.fragments.len())
                .map_err(|_| ValidationError::InvalidFragmentCount)?,
        );
        self.fragments.push(PlanFragment {
            id,
            placement,
            operators,
            output,
            budget,
        });
        Ok(id)
    }

    pub fn add_exchange(
        &mut self,
        from: FragmentId,
        to: FragmentId,
        kind: ExchangeKind,
        schema: RowSchema,
        max_inflight_batches: u32,
    ) -> Result<ExchangeId, ValidationError> {
        if self.exchanges.len() >= MAX_EXCHANGES {
            return Err(ValidationError::TooManyExchanges);
        }
        let id = ExchangeId::new(
            u32::try_from(self.exchanges.len()).map_err(|_| ValidationError::TooManyExchanges)?,
        );
        let exchange = Exchange {
            id,
            from,
            to,
            kind,
            schema,
            max_inflight_batches,
        };
        validate_exchange(&self.fragments, &exchange)?;
        self.exchanges.push(exchange);
        Ok(id)
    }

    pub fn finish(self, root: FragmentId) -> Result<PhysicalPlan, ValidationError> {
        let plan = PhysicalPlan {
            header: self.header,
            fragments: self.fragments,
            exchanges: self.exchanges,
            root,
        };
        plan.validate()?;
        Ok(plan)
    }
}

fn validate_exchange(
    fragments: &[PlanFragment],
    exchange: &Exchange,
) -> Result<(), ValidationError> {
    if exchange.from >= exchange.to {
        return Err(ValidationError::InvalidExchangeDirection {
            from: exchange.from,
            to: exchange.to,
        });
    }
    let source = fragments
        .get(usize::try_from(exchange.from.value()).unwrap_or(usize::MAX))
        .ok_or(ValidationError::InvalidFragment(exchange.from))?;
    if fragments
        .get(usize::try_from(exchange.to.value()).unwrap_or(usize::MAX))
        .is_none()
    {
        return Err(ValidationError::InvalidFragment(exchange.to));
    }
    if exchange.max_inflight_batches == 0 {
        return Err(ValidationError::InvalidExchangeCredit);
    }
    if source.output != exchange.schema {
        return Err(ValidationError::ExchangeSchemaMismatch(exchange.id));
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ValidationError {
    UnsupportedVersion { expected: u16, actual: u16 },
    InvalidHeader,
    InvalidExpectedShards,
    InvalidMemoryBudget,
    InvalidFragmentCount,
    TooManyExchanges,
    InvalidRoot(FragmentId),
    InvalidFragment(FragmentId),
    InvalidExchangeDirection { from: FragmentId, to: FragmentId },
    InvalidExchangeCredit,
    ExchangeSchemaMismatch(ExchangeId),
    HashJoinSchemaMismatch(FragmentId),
    FragmentOutputMismatch(FragmentId),
    InvalidProcedure,
    ProcedurePlacementMismatch,
    ProcedureSchemaMismatch,
    InvalidApplyIdentity,
    InvalidApplyBudget,
    ApplyHeaderMismatch,
    ApplySchemaMismatch,
    ApplyDepthExceeded,
    DuplicateChildPlanIdentity(ChildPlanId),
    InvalidApplyArgumentSource,
    InvalidBatchSubtransaction,
    RecursivePlanNodeLimit,
}

impl Display for ValidationError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "physical plan validation failed: {self:?}")
    }
}

impl Error for ValidationError {}
