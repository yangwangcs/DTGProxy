use crate::{Clause, Identifier, TemporalContext};

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
pub enum Statement {
    Query(QueryStatement),
}
