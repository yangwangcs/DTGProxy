use crate::{Clause, Expression, Identifier, TemporalContext, TransactionTimeScope};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryStatement {
    graph: Option<Identifier>,
    temporal: TemporalContext,
    clauses: Vec<Clause>,
}

impl QueryStatement {
    #[must_use]
    pub const fn new(
        graph: Option<Identifier>,
        temporal: TemporalContext,
        clauses: Vec<Clause>,
    ) -> Self {
        Self {
            graph,
            temporal,
            clauses,
        }
    }

    #[must_use]
    pub const fn graph(&self) -> Option<&Identifier> {
        self.graph.as_ref()
    }

    #[must_use]
    pub const fn temporal(&self) -> &TemporalContext {
        &self.temporal
    }

    #[must_use]
    pub fn clauses(&self) -> &[Clause] {
        &self.clauses
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DiffStatement {
    graph: Identifier,
    from_valid_time: Expression,
    to_valid_time: Expression,
    transaction_time: TransactionTimeScope,
    yield_items: Vec<String>,
}

impl DiffStatement {
    #[must_use]
    pub const fn new(
        graph: Identifier,
        from_valid_time: Expression,
        to_valid_time: Expression,
        transaction_time: TransactionTimeScope,
        yield_items: Vec<String>,
    ) -> Self {
        Self {
            graph,
            from_valid_time,
            to_valid_time,
            transaction_time,
            yield_items,
        }
    }

    #[must_use]
    pub const fn graph(&self) -> &Identifier {
        &self.graph
    }

    #[must_use]
    pub const fn from_valid_time(&self) -> &Expression {
        &self.from_valid_time
    }

    #[must_use]
    pub const fn to_valid_time(&self) -> &Expression {
        &self.to_valid_time
    }

    #[must_use]
    pub const fn transaction_time(&self) -> &TransactionTimeScope {
        &self.transaction_time
    }

    #[must_use]
    pub fn yield_items(&self) -> &[String] {
        &self.yield_items
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Statement {
    Query(QueryStatement),
    Diff(DiffStatement),
}
