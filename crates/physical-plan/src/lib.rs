#![forbid(unsafe_code)]

use std::error::Error;
use std::fmt::{self, Display, Formatter};

use temporal_ir::v2::{RowSchema, ScalarExpr, SlotId};

pub const PHYSICAL_PLAN_VERSION: u16 = 1;
pub const MAX_FRAGMENTS: usize = 65_536;
pub const MAX_EXCHANGES: usize = 131_072;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalPlanHeaderV1 {
    version: u16,
    graph_id: u64,
    schema_version: u64,
    topology_epoch: u64,
    query_fingerprint: [u8; 32],
}

impl PhysicalPlanHeaderV1 {
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
        };
        header.validate()?;
        Ok(header)
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
    Argument,
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
        output: RowSchema,
    },
    Filter(ScalarExpr),
    Project {
        expressions: Vec<(SlotId, ScalarExpr)>,
    },
    HashJoin {
        kind: JoinKind,
        keys: Vec<SlotId>,
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
    Union {
        all: bool,
    },
    TemporalSlice,
    Diff,
    Write {
        operation: WriteOperation,
    },
    Procedure {
        procedure_id: u32,
    },
    Finish,
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
    header: PhysicalPlanHeaderV1,
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
        }
        for exchange in &self.exchanges {
            validate_exchange(&self.fragments, exchange)?;
        }
        Ok(())
    }

    #[must_use]
    pub const fn header(&self) -> &PhysicalPlanHeaderV1 {
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
}

pub struct PhysicalPlanBuilder {
    header: PhysicalPlanHeaderV1,
    fragments: Vec<PlanFragment>,
    exchanges: Vec<Exchange>,
}

impl PhysicalPlanBuilder {
    #[must_use]
    pub const fn new(header: PhysicalPlanHeaderV1) -> Self {
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
    InvalidMemoryBudget,
    InvalidFragmentCount,
    TooManyExchanges,
    InvalidRoot(FragmentId),
    InvalidFragment(FragmentId),
    InvalidExchangeDirection { from: FragmentId, to: FragmentId },
    InvalidExchangeCredit,
    ExchangeSchemaMismatch(ExchangeId),
}

impl Display for ValidationError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "physical plan validation failed: {self:?}")
    }
}

impl Error for ValidationError {}
