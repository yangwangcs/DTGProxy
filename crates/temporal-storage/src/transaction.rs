use crate::{EdgeMutation, ElementRef, VertexMutation};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TemporalTransaction {
    operations: Vec<TemporalOperation>,
}

impl TemporalTransaction {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            operations: Vec::new(),
        }
    }

    #[must_use]
    pub fn with_vertex(mut self, mutation: VertexMutation) -> Self {
        self.operations.push(TemporalOperation::Vertex(mutation));
        self
    }

    #[must_use]
    pub fn with_edge(mut self, mutation: EdgeMutation) -> Self {
        self.operations.push(TemporalOperation::Edge(mutation));
        self
    }

    pub(crate) fn into_operations(self) -> Vec<TemporalOperation> {
        self.operations
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum TemporalOperation {
    Vertex(VertexMutation),
    Edge(EdgeMutation),
}

impl TemporalOperation {
    pub(crate) const fn element(&self) -> ElementRef {
        match self {
            Self::Vertex(mutation) => mutation.element,
            Self::Edge(mutation) => mutation.element,
        }
    }
}
