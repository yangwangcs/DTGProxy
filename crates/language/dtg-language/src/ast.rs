use std::collections::BTreeMap;

#[derive(Clone, Debug)]
pub(crate) struct Program {
    pub(crate) graph: Option<String>,
    pub(crate) statement: Statement,
}
#[derive(Clone, Debug)]
pub(crate) enum Statement {
    Query(Query),
    Write(Write),
    Boundary(Boundary),
    SubmitAnalytics { algorithm: String },
    Procedure { name: String },
}
#[derive(Clone, Copy, Debug)]
pub(crate) enum Boundary {
    Begin,
    Commit,
    Rollback,
}
#[derive(Clone, Debug)]
pub(crate) struct Query {
    pub(crate) scopes: Vec<Scope>,
    pub(crate) matches: Vec<Match>,
    pub(crate) where_clause: Option<(Expr, Expr)>,
    pub(crate) returns: Vec<Expr>,
    pub(crate) order_by: Vec<OrderKey>,
}
#[derive(Clone, Debug)]
pub(crate) struct OrderKey {
    pub(crate) expression: Expr,
    pub(crate) direction: OrderDirection,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OrderDirection {
    Ascending,
    Descending,
}
#[derive(Clone, Debug)]
pub(crate) struct Match {
    pub(crate) pattern: Pattern,
    pub(crate) scopes: Vec<Scope>,
}
#[derive(Clone, Debug)]
pub(crate) struct Pattern {
    pub(crate) nodes: Vec<NodePattern>,
    pub(crate) relationships: Vec<RelationshipPattern>,
}
#[derive(Clone, Debug)]
pub(crate) struct NodePattern {
    pub(crate) variable: String,
    pub(crate) labels: Vec<String>,
    pub(crate) properties: BTreeMap<String, Expr>,
}
#[derive(Clone, Debug)]
pub(crate) struct RelationshipPattern {
    pub(crate) variable: String,
    pub(crate) types: Vec<String>,
    pub(crate) properties: BTreeMap<String, Expr>,
    pub(crate) direction: RelationshipDirection,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RelationshipDirection {
    Outgoing,
    Incoming,
    Either,
}
#[derive(Clone, Debug)]
pub(crate) enum Write {
    Create {
        node: NodePattern,
        valid_from: Expr,
    },
    CreateRelationship {
        matches: Vec<Match>,
        pattern: Pattern,
        valid_from: Expr,
    },
    Set {
        matches: Vec<Match>,
        variable: String,
        properties: BTreeMap<String, Expr>,
        valid_from: Expr,
    },
    Delete {
        matches: Vec<Match>,
        variable: String,
        valid_from: Expr,
    },
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Scope {
    pub(crate) axis: Axis,
    pub(crate) mode: Mode,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Axis {
    Valid,
    System,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Mode {
    AsOf(Expr),
    Between(Expr, Expr),
    Changes(Expr, Expr),
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Expr {
    Parameter(String),
    Integer(i64),
    String(String),
    Boolean(bool),
    Null,
    Column(String),
    Property { input: String, name: String },
}
