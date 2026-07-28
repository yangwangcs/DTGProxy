#![forbid(unsafe_code)]

use std::collections::BTreeSet;
use std::error::Error;
use std::fmt::{self, Display, Formatter};

use temporal_ir::{
    ApplyKind, ApplySlotMapping, ChangeAxis, ChildPlanId, Column, MAX_APPLY_DEPTH,
    MAX_APPLY_INVOCATIONS, MAX_APPLY_OUTPUT_ROWS, MAX_BATCH_SUBTRANSACTION_ROWS,
    ProcedurePlacement, ResolvedProcedure, RowSchema, ScalarExpr, SlotId, SortKey,
    TransactionTimeSpec, ValidTimeSpec, ValueType,
};
use temporal_types::GraphValue;

pub const PHYSICAL_PLAN_VERSION: u16 = 1;
pub const MAX_FRAGMENTS: usize = 65_536;
pub const MAX_EXCHANGES: usize = 131_072;
pub const MAX_RECURSIVE_PLAN_NODES: usize = 4_096;
pub const DEFAULT_RAW_SCAN_ENTRY_LIMIT: u64 = 16_384;
pub const DEFAULT_RAW_SCAN_BYTE_LIMIT: u64 = 16 * 1024 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalPlanHeader {
    version: u16,
    graph_id: u64,
    schema_version: u64,
    topology_epoch: u64,
    capability_generation: u64,
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
            capability_generation: 1,
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

    pub fn with_capability_generation(
        mut self,
        capability_generation: u64,
    ) -> Result<Self, ValidationError> {
        if capability_generation == 0 {
            return Err(ValidationError::InvalidCapabilityGeneration);
        }
        self.capability_generation = capability_generation;
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
            || self.capability_generation == 0
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
    pub const fn capability_generation(&self) -> u64 {
        self.capability_generation
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RawScanBudget {
    entry_limit: u64,
    byte_limit: u64,
}

impl RawScanBudget {
    pub fn new(entry_limit: u64, byte_limit: u64) -> Result<Self, ValidationError> {
        if entry_limit == 0 || byte_limit == 0 {
            return Err(ValidationError::InvalidRawScanBudget);
        }
        Ok(Self {
            entry_limit,
            byte_limit,
        })
    }

    #[must_use]
    pub const fn entry_limit(self) -> u64 {
        self.entry_limit
    }

    #[must_use]
    pub const fn byte_limit(self) -> u64 {
        self.byte_limit
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FragmentExecutionBudget {
    memory: MemoryBudget,
    raw_scan: RawScanBudget,
}

impl FragmentExecutionBudget {
    #[must_use]
    pub const fn new(memory: MemoryBudget, raw_scan: RawScanBudget) -> Self {
        Self { memory, raw_scan }
    }

    #[must_use]
    pub const fn resident_memory_bytes(self) -> u64 {
        self.memory.memory_bytes()
    }

    #[must_use]
    pub const fn spill_bytes(self) -> u64 {
        self.memory.spill_bytes()
    }

    #[must_use]
    pub const fn memory(self) -> MemoryBudget {
        self.memory
    }

    #[must_use]
    pub const fn raw_scan(self) -> RawScanBudget {
        self.raw_scan
    }
}

impl From<MemoryBudget> for FragmentExecutionBudget {
    fn from(memory: MemoryBudget) -> Self {
        Self {
            memory,
            raw_scan: RawScanBudget {
                entry_limit: DEFAULT_RAW_SCAN_ENTRY_LIMIT,
                byte_limit: DEFAULT_RAW_SCAN_BYTE_LIMIT,
            },
        }
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PrimitiveKind {
    CandidateScan,
    AdjacencyExpand,
    ChangeScan,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AccessGuarantee {
    Unsupported,
    Candidate,
    Exact,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResidualPolicy {
    Evaluate,
    Omit,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PhysicalComparisonOperator {
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalPropertyConstraint {
    property_id: u32,
    operator: PhysicalComparisonOperator,
    value: GraphValue,
}

impl PhysicalPropertyConstraint {
    #[must_use]
    pub const fn new(
        property_id: u32,
        operator: PhysicalComparisonOperator,
        value: GraphValue,
    ) -> Self {
        Self {
            property_id,
            operator,
            value,
        }
    }

    #[must_use]
    pub const fn property_id(&self) -> u32 {
        self.property_id
    }

    #[must_use]
    pub const fn operator(&self) -> PhysicalComparisonOperator {
        self.operator
    }

    #[must_use]
    pub const fn value(&self) -> &GraphValue {
        &self.value
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PhysicalAccess {
    Generic,
    Primitive {
        primitive: PrimitiveKind,
        guarantee: AccessGuarantee,
        residual: ResidualPolicy,
        constraints: Vec<PhysicalPropertyConstraint>,
    },
}

impl PhysicalAccess {
    fn validate(&self) -> Result<(), ValidationError> {
        match self {
            Self::Generic => Ok(()),
            Self::Primitive {
                guarantee: AccessGuarantee::Unsupported,
                ..
            }
            | Self::Primitive {
                residual: ResidualPolicy::Omit,
                ..
            } => Err(ValidationError::InvalidPhysicalAccess),
            Self::Primitive {
                primitive,
                guarantee,
                constraints,
                ..
            } if constraints.is_empty()
                || (*primitive == PrimitiveKind::CandidateScan
                    && *guarantee == AccessGuarantee::Candidate) =>
            {
                Ok(())
            }
            Self::Primitive { .. } => Err(ValidationError::InvalidPhysicalAccess),
        }
    }
}

pub const COUNT_AGGREGATE_FUNCTION_ID: u32 = 0xd190_b1fd;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AggregatePhase {
    Single,
    PartialCount,
    FinalCount,
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
        phase: AggregatePhase,
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
    ChangeScan {
        axis: ChangeAxis,
        start: ScalarExpr,
        end: ScalarExpr,
        system_snapshot: TransactionTimeSpec,
    },
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
    access: Vec<PhysicalAccess>,
    output: RowSchema,
    execution_budget: FragmentExecutionBudget,
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
    pub fn access(&self) -> &[PhysicalAccess] {
        &self.access
    }

    #[must_use]
    pub const fn output(&self) -> &RowSchema {
        &self.output
    }

    #[must_use]
    pub const fn budget(&self) -> MemoryBudget {
        self.execution_budget.memory()
    }

    #[must_use]
    pub const fn execution_budget(&self) -> FragmentExecutionBudget {
        self.execution_budget
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
        let mut change_scan_fragment = None;
        for (index, fragment) in self.fragments.iter().enumerate() {
            if fragment.id.value() != u32::try_from(index).unwrap_or(u32::MAX)
                || fragment.operators.is_empty()
            {
                return Err(ValidationError::InvalidFragment(fragment.id));
            }
            if fragment.access.len() != fragment.operators.len() {
                return Err(ValidationError::AccessMetadataMismatch(fragment.id));
            }
            for access in &fragment.access {
                access.validate()?;
            }
            if validate_change_scan_structure(fragment)?
                && change_scan_fragment.replace(fragment.id).is_some()
            {
                return Err(ValidationError::InvalidChangeScanStructure(fragment.id));
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
                    | PhysicalOperator::Write { output, .. } => {
                        current_schema = output.clone();
                    }
                    PhysicalOperator::Aggregate {
                        phase,
                        grouping,
                        aggregates,
                        output,
                    } => {
                        validate_aggregate_phase(
                            fragment.id,
                            fragment.placement,
                            *phase,
                            grouping,
                            aggregates,
                            &current_schema,
                            output,
                        )?;
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

fn validate_change_scan_structure(fragment: &PlanFragment) -> Result<bool, ValidationError> {
    let mut change_scans = fragment
        .operators
        .iter()
        .enumerate()
        .filter(|(_, operator)| matches!(operator, PhysicalOperator::ChangeScan { .. }));
    let Some((change_index, _)) = change_scans.next() else {
        return Ok(false);
    };
    if change_index != 1
        || change_scans.next().is_some()
        || !matches!(
            fragment.operators.first(),
            Some(PhysicalOperator::NodeScan { .. } | PhysicalOperator::RelationshipScan { .. })
        )
    {
        return Err(ValidationError::InvalidChangeScanStructure(fragment.id));
    }
    Ok(true)
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
        || child_header.capability_generation() != parent_header.capability_generation()
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
        let access = vec![PhysicalAccess::Generic; operators.len()];
        self.add_fragment_with_access_and_execution_budget(
            placement,
            operators,
            access,
            output,
            budget.into(),
        )
    }

    pub fn add_fragment_with_access(
        &mut self,
        placement: Placement,
        operators: Vec<PhysicalOperator>,
        access: Vec<PhysicalAccess>,
        output: RowSchema,
        budget: MemoryBudget,
    ) -> Result<FragmentId, ValidationError> {
        self.add_fragment_with_access_and_execution_budget(
            placement,
            operators,
            access,
            output,
            budget.into(),
        )
    }

    pub fn add_fragment_with_execution_budget(
        &mut self,
        placement: Placement,
        operators: Vec<PhysicalOperator>,
        output: RowSchema,
        execution_budget: FragmentExecutionBudget,
    ) -> Result<FragmentId, ValidationError> {
        let access = vec![PhysicalAccess::Generic; operators.len()];
        self.add_fragment_with_access_and_execution_budget(
            placement,
            operators,
            access,
            output,
            execution_budget,
        )
    }

    fn add_fragment_with_access_and_execution_budget(
        &mut self,
        placement: Placement,
        operators: Vec<PhysicalOperator>,
        access: Vec<PhysicalAccess>,
        output: RowSchema,
        execution_budget: FragmentExecutionBudget,
    ) -> Result<FragmentId, ValidationError> {
        if operators.is_empty() || self.fragments.len() >= MAX_FRAGMENTS {
            return Err(ValidationError::InvalidFragmentCount);
        }
        let id = FragmentId::new(
            u32::try_from(self.fragments.len())
                .map_err(|_| ValidationError::InvalidFragmentCount)?,
        );
        if access.len() != operators.len() {
            return Err(ValidationError::AccessMetadataMismatch(id));
        }
        for (index, (operator, item)) in operators.iter().zip(&access).enumerate() {
            item.validate()?;
            let aligned = match item {
                PhysicalAccess::Generic => true,
                PhysicalAccess::Primitive {
                    primitive: PrimitiveKind::CandidateScan,
                    ..
                } => matches!(operator, PhysicalOperator::NodeScan { .. }),
                PhysicalAccess::Primitive {
                    primitive: PrimitiveKind::AdjacencyExpand,
                    ..
                } => matches!(operator, PhysicalOperator::Expand { .. }),
                PhysicalAccess::Primitive {
                    primitive: PrimitiveKind::ChangeScan,
                    ..
                } => matches!(operator, PhysicalOperator::ChangeScan { .. }),
            };
            if !aligned {
                return Err(ValidationError::InvalidPhysicalAccess);
            }
            if let PhysicalAccess::Primitive {
                primitive: PrimitiveKind::CandidateScan,
                constraints,
                ..
            } = item
                && !constraints.is_empty()
            {
                let Some(PhysicalOperator::NodeScan { binding, .. }) = operators.get(index) else {
                    return Err(ValidationError::InvalidPhysicalAccess);
                };
                let Some(PhysicalOperator::Filter(predicate)) = operators.get(index + 1) else {
                    return Err(ValidationError::InvalidPhysicalAccess);
                };
                if access.get(index + 1) != Some(&PhysicalAccess::Generic) {
                    return Err(ValidationError::InvalidPhysicalAccess);
                }
                let mut residual_constraints = Vec::new();
                collect_physical_constraints(predicate, *binding, &mut residual_constraints);
                if constraints
                    .iter()
                    .any(|constraint| !residual_constraints.contains(constraint))
                {
                    return Err(ValidationError::InvalidPhysicalAccess);
                }
            }
        }
        self.fragments.push(PlanFragment {
            id,
            placement,
            operators,
            access,
            output,
            execution_budget,
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

fn collect_physical_constraints(
    expression: &ScalarExpr,
    binding: SlotId,
    constraints: &mut Vec<PhysicalPropertyConstraint>,
) {
    if let ScalarExpr::And(left, right) = expression {
        collect_physical_constraints(left, binding, constraints);
        collect_physical_constraints(right, binding, constraints);
        return;
    }
    let extracted = match expression {
        ScalarExpr::Equal(left, right) => {
            physical_constraint(left, right, binding, PhysicalComparisonOperator::Equal)
        }
        ScalarExpr::NotEqual(left, right) => {
            physical_constraint(left, right, binding, PhysicalComparisonOperator::NotEqual)
        }
        ScalarExpr::Less(left, right) => {
            physical_constraint(left, right, binding, PhysicalComparisonOperator::LessThan)
        }
        ScalarExpr::LessEqual(left, right) => physical_constraint(
            left,
            right,
            binding,
            PhysicalComparisonOperator::LessThanOrEqual,
        ),
        ScalarExpr::Greater(left, right) => physical_constraint(
            left,
            right,
            binding,
            PhysicalComparisonOperator::GreaterThan,
        ),
        ScalarExpr::GreaterEqual(left, right) => physical_constraint(
            left,
            right,
            binding,
            PhysicalComparisonOperator::GreaterThanOrEqual,
        ),
        _ => None,
    };
    if let Some(constraint) = extracted {
        constraints.push(constraint);
    }
}

fn physical_constraint(
    left: &ScalarExpr,
    right: &ScalarExpr,
    binding: SlotId,
    operator: PhysicalComparisonOperator,
) -> Option<PhysicalPropertyConstraint> {
    if let (Some(property_id), ScalarExpr::Literal(value)) =
        (physical_bound_property(left, binding), right)
    {
        return Some(PhysicalPropertyConstraint::new(
            property_id,
            operator,
            value.clone(),
        ));
    }
    if let (ScalarExpr::Literal(value), Some(property_id)) =
        (left, physical_bound_property(right, binding))
    {
        return Some(PhysicalPropertyConstraint::new(
            property_id,
            reverse_physical_comparison(operator),
            value.clone(),
        ));
    }
    None
}

fn physical_bound_property(expression: &ScalarExpr, binding: SlotId) -> Option<u32> {
    match expression {
        ScalarExpr::Property { value, property_id } if matches!(value.as_ref(), ScalarExpr::Slot(slot) if *slot == binding) => {
            Some(*property_id)
        }
        _ => None,
    }
}

const fn reverse_physical_comparison(
    operator: PhysicalComparisonOperator,
) -> PhysicalComparisonOperator {
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

fn validate_aggregate_phase(
    fragment: FragmentId,
    placement: Placement,
    phase: AggregatePhase,
    grouping: &[SlotId],
    aggregates: &[(SlotId, ScalarExpr)],
    input: &RowSchema,
    output: &RowSchema,
) -> Result<(), ValidationError> {
    if phase == AggregatePhase::Single {
        return Ok(());
    }
    if !grouping.is_empty()
        || aggregates.is_empty()
        || aggregates.iter().any(|(_, expression)| {
            !matches!(
                expression,
                ScalarExpr::Function { function_id, .. }
                    if *function_id == COUNT_AGGREGATE_FUNCTION_ID
            )
        })
    {
        return Err(ValidationError::InvalidAggregatePhase(fragment));
    }
    if !count_output_schema_matches(aggregates, output) {
        return Err(ValidationError::InvalidAggregatePhase(fragment));
    }
    match phase {
        AggregatePhase::Single => Ok(()),
        AggregatePhase::PartialCount => {
            if placement != Placement::AllShards
                || !aggregates.iter().all(|(_, expression)| {
                    let ScalarExpr::Function { arguments, .. } = expression else {
                        return false;
                    };
                    match arguments.as_slice() {
                        [] => true,
                        [ScalarExpr::Slot(slot)] => input
                            .columns()
                            .iter()
                            .find(|column| column.slot() == *slot)
                            .is_some_and(|column| !column.nullable()),
                        _ => false,
                    }
                })
            {
                return Err(ValidationError::InvalidAggregatePhase(fragment));
            }
            Ok(())
        }
        AggregatePhase::FinalCount => {
            if placement != Placement::Coordinator
                || !count_output_schema_matches(aggregates, input)
            {
                return Err(ValidationError::InvalidAggregatePhase(fragment));
            }
            Ok(())
        }
    }
}

fn count_output_schema_matches(aggregates: &[(SlotId, ScalarExpr)], schema: &RowSchema) -> bool {
    let aggregate_slots = aggregates
        .iter()
        .map(|(slot, _)| *slot)
        .collect::<BTreeSet<_>>();
    aggregate_slots.len() == aggregates.len()
        && aggregate_slots.len() == schema.columns().len()
        && aggregate_slots.iter().all(|slot| {
            schema.columns().iter().any(|column| {
                column.slot() == *slot
                    && column.value_type() == &ValueType::Integer
                    && !column.nullable()
            })
        })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ValidationError {
    UnsupportedVersion { expected: u16, actual: u16 },
    InvalidHeader,
    InvalidCapabilityGeneration,
    InvalidExpectedShards,
    InvalidMemoryBudget,
    InvalidRawScanBudget,
    InvalidFragmentCount,
    TooManyExchanges,
    InvalidRoot(FragmentId),
    InvalidFragment(FragmentId),
    AccessMetadataMismatch(FragmentId),
    InvalidPhysicalAccess,
    InvalidExchangeDirection { from: FragmentId, to: FragmentId },
    InvalidExchangeCredit,
    ExchangeSchemaMismatch(ExchangeId),
    HashJoinSchemaMismatch(FragmentId),
    InvalidAggregatePhase(FragmentId),
    InvalidChangeScanStructure(FragmentId),
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
