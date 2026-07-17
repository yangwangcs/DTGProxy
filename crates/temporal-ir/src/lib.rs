#![forbid(unsafe_code)]

use std::error::Error;
use std::fmt::{self, Display, Formatter};

use temporal_storage::{ElementId, ElementKind, ElementRef, GraphId, PartitionId};
use temporal_types::{TransactionTime, ValidTime};

pub const PLAN_VERSION: u16 = 1;
pub const MAX_RESULT_LIMIT: u32 = 10_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GraphScope {
    graph: GraphId,
    partition: PartitionId,
}

impl GraphScope {
    #[must_use]
    pub const fn new(graph: GraphId, partition: PartitionId) -> Self {
        Self { graph, partition }
    }

    #[must_use]
    pub const fn graph(self) -> GraphId {
        self.graph
    }

    #[must_use]
    pub const fn partition(self) -> PartitionId {
        self.partition
    }

    #[must_use]
    pub const fn element(self, kind: ElementKind, id: ElementId) -> ElementRef {
        match kind {
            ElementKind::Vertex => ElementRef::vertex(self.graph, self.partition, id),
            ElementKind::Edge => ElementRef::edge(self.graph, self.partition, id),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExpandDirection {
    Out,
    In,
    Both,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PointOperator {
    VertexById(ElementId),
    EdgeById(ElementId),
    Expand {
        origin: ElementId,
        direction: ExpandDirection,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TemporalSelector {
    Current,
    AsOf(TransactionTime),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DiffOperator {
    Element { kind: ElementKind, id: ElementId },
}

impl DiffOperator {
    #[must_use]
    pub const fn element(self, scope: GraphScope) -> ElementRef {
        match self {
            Self::Element { kind, id } => scope.element(kind, id),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PlanBody {
    Point {
        operator: PointOperator,
        valid_time: ValidTime,
        transaction: TemporalSelector,
        limit: u32,
    },
    Diff {
        operator: DiffOperator,
        from_transaction: TransactionTime,
        to_transaction: TransactionTime,
        limit: u32,
    },
}

impl PlanBody {
    #[must_use]
    pub const fn limit(&self) -> u32 {
        match self {
            Self::Point { limit, .. } | Self::Diff { limit, .. } => *limit,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TemporalPlan {
    version: u16,
    scope: GraphScope,
    body: PlanBody,
}

impl TemporalPlan {
    #[must_use]
    pub const fn point(
        scope: GraphScope,
        operator: PointOperator,
        valid_time: ValidTime,
        transaction: TemporalSelector,
        limit: u32,
    ) -> Self {
        Self::with_version(
            PLAN_VERSION,
            scope,
            PlanBody::Point {
                operator,
                valid_time,
                transaction,
                limit,
            },
        )
    }

    #[must_use]
    pub const fn diff(
        scope: GraphScope,
        operator: DiffOperator,
        from_transaction: TransactionTime,
        to_transaction: TransactionTime,
        limit: u32,
    ) -> Self {
        Self::with_version(
            PLAN_VERSION,
            scope,
            PlanBody::Diff {
                operator,
                from_transaction,
                to_transaction,
                limit,
            },
        )
    }

    #[must_use]
    pub const fn with_version(version: u16, scope: GraphScope, body: PlanBody) -> Self {
        Self {
            version,
            scope,
            body,
        }
    }

    #[must_use]
    pub const fn version(&self) -> u16 {
        self.version
    }

    #[must_use]
    pub const fn scope(&self) -> GraphScope {
        self.scope
    }

    #[must_use]
    pub const fn body(&self) -> &PlanBody {
        &self.body
    }

    pub fn validate(&self) -> Result<(), PlanError> {
        if self.version != PLAN_VERSION {
            return Err(PlanError::UnsupportedVersion {
                expected: PLAN_VERSION,
                actual: self.version,
            });
        }
        let limit = self.body.limit();
        if !(1..=MAX_RESULT_LIMIT).contains(&limit) {
            return Err(PlanError::InvalidLimit {
                max: MAX_RESULT_LIMIT,
                actual: limit,
            });
        }
        if let PlanBody::Diff {
            from_transaction,
            to_transaction,
            ..
        } = &self.body
            && from_transaction > to_transaction
        {
            return Err(PlanError::InvalidDiffOrder);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PlanError {
    UnsupportedVersion { expected: u16, actual: u16 },
    InvalidLimit { max: u32, actual: u32 },
    InvalidDiffOrder,
}

impl Display for PlanError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedVersion { expected, actual } => {
                write!(
                    formatter,
                    "unsupported temporal IR version {actual}; expected {expected}"
                )
            }
            Self::InvalidLimit { max, actual } => {
                write!(
                    formatter,
                    "result limit must be between 1 and {max}; got {actual}"
                )
            }
            Self::InvalidDiffOrder => {
                formatter.write_str("DIFF start transaction must not follow its end")
            }
        }
    }
}

impl Error for PlanError {}
