use crate::{EdgeMutation, ElementRef, GraphId, PartitionId, VertexMutation};
use temporal_types::{Interval, ValidTime};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EndpointGuard {
    vertex: ElementRef,
    valid: Interval<ValidTime>,
}

impl EndpointGuard {
    #[must_use]
    pub const fn vertex(self) -> ElementRef {
        self.vertex
    }

    #[must_use]
    pub const fn valid(self) -> Interval<ValidTime> {
        self.valid
    }
}

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

    #[must_use]
    pub fn is_scoped_to(&self, graph: GraphId, partition: PartitionId) -> bool {
        self.operations.iter().all(|operation| {
            let element = operation.element();
            element.graph() == graph && element.partition() == partition
        })
    }

    pub fn extend(&mut self, other: Self) {
        self.operations.extend(other.operations);
    }

    #[must_use]
    pub fn remote_endpoint_guards(&self) -> Vec<EndpointGuard> {
        self.operations
            .iter()
            .filter_map(|operation| match operation {
                TemporalOperation::Edge(mutation)
                    if mutation.destination.partition() != mutation.element.partition() =>
                {
                    Some(EndpointGuard {
                        vertex: mutation.destination,
                        valid: mutation.valid,
                    })
                }
                TemporalOperation::Vertex(_) | TemporalOperation::Edge(_) => None,
            })
            .collect()
    }

    #[must_use]
    pub fn created_vertex_intervals(&self) -> Vec<(ElementRef, Interval<ValidTime>)> {
        self.operations
            .iter()
            .filter_map(|operation| match operation {
                TemporalOperation::Vertex(mutation) if mutation.replacement.is_some() => {
                    Some((mutation.element, mutation.valid))
                }
                TemporalOperation::Vertex(_) | TemporalOperation::Edge(_) => None,
            })
            .collect()
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
