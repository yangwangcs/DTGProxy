use dtg_kernel::{TransactionTime, ValidInterval};
use dtg_storage::{EdgeId, LogicalMutation, VertexId};

use crate::TxnError;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub enum EntityIdentity {
    Vertex(VertexId),
    Edge(EdgeId),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WriteExtent {
    Interval(ValidInterval),
    CompleteIdentity,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IntervalWrite {
    identity: EntityIdentity,
    extent: WriteExtent,
    transaction_time: TransactionTime,
}

impl IntervalWrite {
    pub const fn new(
        identity: EntityIdentity,
        valid_time: ValidInterval,
        transaction_time: TransactionTime,
    ) -> Self {
        Self {
            identity,
            extent: WriteExtent::Interval(valid_time),
            transaction_time,
        }
    }

    pub const fn tombstone(identity: EntityIdentity, transaction_time: TransactionTime) -> Self {
        Self {
            identity,
            extent: WriteExtent::CompleteIdentity,
            transaction_time,
        }
    }

    pub const fn identity(self) -> EntityIdentity {
        self.identity
    }

    pub const fn transaction_time(self) -> TransactionTime {
        self.transaction_time
    }
}

pub fn detect_conflict(
    staged: &IntervalWrite,
    committed: &[IntervalWrite],
) -> Result<(), TxnError> {
    if committed.iter().any(|candidate| {
        candidate.identity == staged.identity
            && candidate.transaction_time > staged.transaction_time
            && extents_overlap(candidate.extent, staged.extent)
    }) {
        Err(TxnError::WriteConflict)
    } else {
        Ok(())
    }
}

pub(crate) fn detect_mutation_conflicts(
    staged: &[LogicalMutation],
    committed: &[LogicalMutation],
    start_time: TransactionTime,
) -> Result<(), TxnError> {
    let committed: Vec<_> = committed
        .iter()
        .filter_map(|mutation| write_from_mutation(mutation, None))
        .collect();
    for mutation in staged {
        if let Some(staged) = write_from_mutation(mutation, Some(start_time)) {
            detect_conflict(&staged, &committed)?;
        }
    }
    Ok(())
}

fn write_from_mutation(
    mutation: &LogicalMutation,
    transaction_time: Option<TransactionTime>,
) -> Option<IntervalWrite> {
    match mutation {
        LogicalMutation::PutVertex(vertex) => Some(IntervalWrite::new(
            EntityIdentity::Vertex(vertex.id()),
            vertex.valid_time(),
            transaction_time.unwrap_or_else(|| vertex.transaction_time()),
        )),
        LogicalMutation::DeleteVertex(vertex) => Some(IntervalWrite::tombstone(
            EntityIdentity::Vertex(vertex.id()),
            transaction_time.unwrap_or_else(|| vertex.transaction_time()),
        )),
        LogicalMutation::PutEdge(edge) => Some(IntervalWrite::new(
            EntityIdentity::Edge(edge.id()),
            edge.valid_time(),
            transaction_time.unwrap_or_else(|| edge.transaction_time()),
        )),
        LogicalMutation::DeleteEdge(edge) => Some(IntervalWrite::tombstone(
            EntityIdentity::Edge(edge.id()),
            transaction_time.unwrap_or_else(|| edge.transaction_time()),
        )),
        LogicalMutation::PutTransaction(_) | LogicalMutation::PutReplicaMetadata(_) => None,
    }
}

const fn extents_overlap(left: WriteExtent, right: WriteExtent) -> bool {
    match (left, right) {
        (WriteExtent::Interval(left), WriteExtent::Interval(right)) => left.overlaps(right),
        (WriteExtent::CompleteIdentity, _) | (_, WriteExtent::CompleteIdentity) => true,
    }
}
