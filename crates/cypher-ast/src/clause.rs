use crate::{Expression, Identifier, QueryStatement};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TextSpan {
    start: usize,
    end: usize,
}

impl TextSpan {
    #[must_use]
    pub const fn new(start: usize, end: usize) -> Self {
        Self { start, end }
    }

    #[must_use]
    pub const fn start(self) -> usize {
        self.start
    }

    #[must_use]
    pub const fn end(self) -> usize {
        self.end
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClauseKind {
    Match,
    OptionalMatch,
    Where,
    Filter,
    With,
    Let,
    Return,
    Finish,
    Unwind,
    For,
    Union { all: bool },
    Next,
    When,
    OrderBy,
    Skip,
    Offset,
    Limit,
    Create,
    Merge,
    Set,
    Remove,
    Delete { detach: bool },
    Foreach,
    Call,
    Yield,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct YieldItem {
    name: String,
    alias: Option<String>,
}

impl YieldItem {
    #[must_use]
    pub fn new(name: impl Into<String>, alias: Option<String>) -> Self {
        Self {
            name: name.into(),
            alias,
        }
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub fn alias(&self) -> Option<&str> {
        self.alias.as_deref()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProcedureCall {
    name: String,
    arguments: Vec<Expression>,
    yield_selection: ProcedureYield,
}

impl ProcedureCall {
    #[must_use]
    pub fn new(
        name: impl Into<String>,
        arguments: Vec<Expression>,
        yield_selection: ProcedureYield,
    ) -> Self {
        Self {
            name: name.into(),
            arguments,
            yield_selection,
        }
    }
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
    #[must_use]
    pub fn arguments(&self) -> &[Expression] {
        &self.arguments
    }
    #[must_use]
    pub const fn yield_selection(&self) -> &ProcedureYield {
        &self.yield_selection
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProcedureYield {
    None,
    All,
    Items(Vec<YieldItem>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SubqueryErrorPolicy {
    Fail,
}

pub const DEFAULT_SUBQUERY_BATCH_ROWS: u32 = 1_000;
pub const MAX_SUBQUERY_BATCH_ROWS: u32 = 1_000_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InTransactions {
    batch_rows: u32,
    error_policy: SubqueryErrorPolicy,
}

impl InTransactions {
    #[must_use]
    pub const fn try_new(batch_rows: u32, error_policy: SubqueryErrorPolicy) -> Option<Self> {
        if batch_rows == 0 || batch_rows > MAX_SUBQUERY_BATCH_ROWS {
            return None;
        }
        Some(Self {
            batch_rows,
            error_policy,
        })
    }

    #[must_use]
    pub const fn batch_rows(self) -> u32 {
        self.batch_rows
    }

    #[must_use]
    pub const fn error_policy(self) -> SubqueryErrorPolicy {
        self.error_policy
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CallSubquery {
    imports: Vec<Identifier>,
    query: Box<QueryStatement>,
    exports: Vec<Identifier>,
    in_transactions: Option<InTransactions>,
}

impl CallSubquery {
    #[must_use]
    pub fn new(
        imports: Vec<Identifier>,
        query: QueryStatement,
        exports: Vec<Identifier>,
        in_transactions: Option<InTransactions>,
    ) -> Self {
        Self {
            imports,
            query: Box::new(query),
            exports,
            in_transactions,
        }
    }

    #[must_use]
    pub fn imports(&self) -> &[Identifier] {
        &self.imports
    }

    #[must_use]
    pub fn query(&self) -> &QueryStatement {
        &self.query
    }

    #[must_use]
    pub fn exports(&self) -> &[Identifier] {
        &self.exports
    }

    #[must_use]
    pub const fn in_transactions(&self) -> Option<InTransactions> {
        self.in_transactions
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Clause {
    kind: ClauseKind,
    span: TextSpan,
    text: String,
    procedure: Option<ProcedureCall>,
    call_subquery: Option<CallSubquery>,
}

impl Clause {
    #[must_use]
    pub fn new(kind: ClauseKind, span: TextSpan, text: impl Into<String>) -> Self {
        Self {
            kind,
            span,
            text: text.into(),
            procedure: None,
            call_subquery: None,
        }
    }

    #[must_use]
    pub const fn kind(&self) -> ClauseKind {
        self.kind
    }

    #[must_use]
    pub const fn span(&self) -> TextSpan {
        self.span
    }

    #[must_use]
    pub fn text(&self) -> &str {
        &self.text
    }

    #[must_use]
    pub fn procedure(&self) -> Option<&ProcedureCall> {
        self.procedure.as_ref()
    }

    #[must_use]
    pub const fn call_subquery(&self) -> Option<&CallSubquery> {
        self.call_subquery.as_ref()
    }

    #[must_use]
    pub fn with_procedure(mut self, procedure: ProcedureCall) -> Self {
        self.procedure = Some(procedure);
        self
    }

    #[must_use]
    pub fn with_call_subquery(mut self, subquery: CallSubquery) -> Self {
        self.call_subquery = Some(subquery);
        self
    }
}
