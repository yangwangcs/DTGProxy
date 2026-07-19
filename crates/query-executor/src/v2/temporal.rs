use std::error::Error;
use std::fmt::{self, Display, Formatter};

use physical_plan::{PhysicalOperator, PlanFragment};
use storage_api::StorageAdapter;
use temporal_storage::{GraphId, TemporalStore, TemporalStoreError};
use temporal_types::{TransactionTime, ValidTime};

use crate::VertexRecord;

use super::{
    BatchExecutor, ExecutionContext, MAX_BATCH_ROWS, RecordBatch, RuntimeError, RuntimeValue,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransactionRead {
    Current,
    AsOf(TransactionTime),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TemporalRead {
    graph: GraphId,
    valid_time: ValidTime,
    transaction: TransactionRead,
}

impl TemporalRead {
    #[must_use]
    pub const fn current(graph: GraphId, valid_time: ValidTime) -> Self {
        Self {
            graph,
            valid_time,
            transaction: TransactionRead::Current,
        }
    }

    #[must_use]
    pub const fn as_of(
        graph: GraphId,
        valid_time: ValidTime,
        transaction_time: TransactionTime,
    ) -> Self {
        Self {
            graph,
            valid_time,
            transaction: TransactionRead::AsOf(transaction_time),
        }
    }
}

pub struct TemporalBatchExecutor<A> {
    store: TemporalStore<A>,
    scalar: BatchExecutor,
}

impl<A> TemporalBatchExecutor<A>
where
    A: StorageAdapter,
{
    #[must_use]
    pub const fn new(store: TemporalStore<A>) -> Self {
        Self {
            store,
            scalar: BatchExecutor::new(),
        }
    }

    #[must_use]
    pub const fn store(&self) -> &TemporalStore<A> {
        &self.store
    }

    pub async fn execute_fragment(
        &self,
        fragment: &PlanFragment,
        context: &ExecutionContext,
        read: TemporalRead,
    ) -> Result<Vec<RecordBatch>, TemporalExecutionError> {
        let Some((first, remaining)) = fragment.operators().split_first() else {
            return Err(RuntimeError::UnsupportedOperator("empty fragment").into());
        };
        let batches = match first {
            PhysicalOperator::NodeScan { labels, output, .. } => {
                self.node_scan(labels, output, read).await?
            }
            _ => Vec::new(),
        };
        let operators = if matches!(first, PhysicalOperator::NodeScan { .. }) {
            remaining
        } else {
            fragment.operators()
        };
        self.scalar
            .execute_operators(
                operators,
                fragment.output(),
                fragment.budget().memory_bytes(),
                context,
                batches,
            )
            .map_err(Into::into)
    }

    async fn node_scan(
        &self,
        labels: &[u32],
        output: &temporal_ir::v2::RowSchema,
        read: TemporalRead,
    ) -> Result<Vec<RecordBatch>, TemporalExecutionError> {
        let vertices = match read.transaction {
            TransactionRead::Current => {
                self.store
                    .scan_vertex_views_current(read.graph, read.valid_time)
                    .await?
            }
            TransactionRead::AsOf(transaction_time) => {
                self.store
                    .scan_vertex_views_as_of(read.graph, read.valid_time, transaction_time)
                    .await?
            }
        };
        let rows = vertices
            .into_iter()
            .filter(|vertex| labels.is_empty() || labels.contains(&vertex.label().value()))
            .map(|vertex| vec![RuntimeValue::Node(VertexRecord::from(vertex))])
            .collect::<Vec<_>>();
        if rows.is_empty() {
            return Ok(vec![RecordBatch::try_new(output.clone(), Vec::new())?]);
        }
        rows.chunks(MAX_BATCH_ROWS)
            .map(|chunk| RecordBatch::try_new(output.clone(), chunk.to_vec()).map_err(Into::into))
            .collect()
    }
}

#[derive(Debug)]
pub enum TemporalExecutionError {
    Runtime(RuntimeError),
    Storage(TemporalStoreError),
}

impl Display for TemporalExecutionError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Runtime(error) => Display::fmt(error, formatter),
            Self::Storage(error) => Display::fmt(error, formatter),
        }
    }
}

impl Error for TemporalExecutionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Runtime(error) => Some(error),
            Self::Storage(error) => Some(error),
        }
    }
}

impl From<RuntimeError> for TemporalExecutionError {
    fn from(value: RuntimeError) -> Self {
        Self::Runtime(value)
    }
}

impl From<TemporalStoreError> for TemporalExecutionError {
    fn from(value: TemporalStoreError) -> Self {
        Self::Storage(value)
    }
}
