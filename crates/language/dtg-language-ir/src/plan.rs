use std::collections::BTreeMap;

use dtg_kernel::{TransactionTime, ValidInterval};

use crate::{AnalyticsSubmission, LogicalExpr, Parameter, RowSchema};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicalProgram {
    pub version: crate::IrVersion,
    pub parameters: Vec<Parameter>,
    pub statement: LogicalStatement,
    pub result_schema: RowSchema,
}

impl LogicalProgram {
    pub const fn new(statement: LogicalStatement) -> Self {
        Self {
            version: crate::IrVersion::CURRENT,
            parameters: Vec::new(),
            statement,
            result_schema: RowSchema::empty(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LogicalStatement {
    Query(LogicalPlan),
    Write(LogicalWrite),
    SubmitAnalytics(AnalyticsSubmission),
    BeginTransaction,
    CommitTransaction,
    RollbackTransaction,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicalWrite {
    pub mutations: Vec<LogicalMutation>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LogicalMutation {
    CreateVertex {
        variable: String,
        labels: Vec<String>,
        properties: BTreeMap<String, LogicalExpr>,
        valid_interval: ValidInterval,
    },
    CreateRelationship {
        variable: String,
        relationship_type: String,
        source: String,
        destination: String,
        properties: BTreeMap<String, LogicalExpr>,
        valid_interval: ValidInterval,
    },
    SetProperties {
        variable: String,
        properties: BTreeMap<String, LogicalExpr>,
        valid_interval: ValidInterval,
    },
    Delete {
        variable: String,
        valid_interval: ValidInterval,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicalPlan {
    pub root: LogicalNodeId,
    pub nodes: Vec<LogicalNode>,
}

impl LogicalPlan {
    pub const fn empty() -> Self {
        Self {
            root: LogicalNodeId(0),
            nodes: Vec::new(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub struct LogicalNodeId(u32);

impl LogicalNodeId {
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u32 {
        self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicalNode {
    pub id: LogicalNodeId,
    pub kind: LogicalNodeKind,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LogicalNodeKind {
    NodeScan(NodeScan),
    RelationshipScan(RelationshipScan),
    Expand(Expand),
    Filter {
        input: LogicalNodeId,
        predicate: LogicalExpr,
    },
    Project {
        input: LogicalNodeId,
        projections: Vec<Projection>,
    },
    Join(Join),
    Aggregate(Aggregate),
    Sort(Sort),
    Limit(Limit),
    Unwind(Unwind),
    Subquery(Subquery),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeScan {
    pub variable: String,
    pub labels: Vec<String>,
    pub temporal_scope: TemporalScope,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RelationshipScan {
    pub variable: String,
    pub relationship_types: Vec<String>,
    pub temporal_scope: TemporalScope,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Expand {
    pub input: LogicalNodeId,
    pub source: String,
    pub relationship: String,
    pub destination: String,
    pub direction: ExpandDirection,
    pub relationship_types: Vec<String>,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub enum ExpandDirection {
    Outgoing,
    Incoming,
    Either,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Projection {
    pub expression: LogicalExpr,
    pub alias: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Join {
    pub left: LogicalNodeId,
    pub right: LogicalNodeId,
    pub kind: JoinKind,
    pub predicate: Option<LogicalExpr>,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub enum JoinKind {
    Inner,
    Left,
    Semi,
    Anti,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Aggregate {
    pub input: LogicalNodeId,
    pub groups: Vec<Projection>,
    pub aggregates: Vec<AggregateFunction>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AggregateFunction {
    pub function: AggregateKind,
    pub argument: Option<LogicalExpr>,
    pub alias: String,
    pub distinct: bool,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub enum AggregateKind {
    Count,
    Sum,
    Average,
    Minimum,
    Maximum,
    Collect,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Sort {
    pub input: LogicalNodeId,
    pub keys: Vec<SortKey>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SortKey {
    pub expression: LogicalExpr,
    pub direction: SortDirection,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub enum SortDirection {
    Ascending,
    Descending,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Limit {
    pub input: LogicalNodeId,
    pub skip: Option<LogicalExpr>,
    pub limit: Option<LogicalExpr>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Unwind {
    pub input: LogicalNodeId,
    pub expression: LogicalExpr,
    pub alias: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Subquery {
    pub input: Option<LogicalNodeId>,
    pub plan: Box<LogicalPlan>,
    pub correlated_variables: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TemporalScope {
    Current,
    AsOf(TimeExpr),
    Changes { from: TimeExpr, to: TimeExpr },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TimeExpr {
    Literal(TransactionTime),
    Parameter(String),
}
