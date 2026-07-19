use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};

use physical_plan::{PhysicalOperator, PlanFragment};
use storage_api::StorageAdapter;
use temporal_ir::v2::{RowSchema, ScalarExpr, TransactionTimeSpec, ValidTimeSpec};
use temporal_storage::{EdgeView, GraphId, TemporalStore, TemporalStoreError};
use temporal_types::{TransactionTime, ValidTime};

use crate::{EdgeRecord, VertexRecord};

use super::{
    BatchExecutor, ExecutionContext, MAX_BATCH_ROWS, RecordBatch, RuntimeError, RuntimeValue,
};

use super::expression::evaluate;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResolvedValidTime {
    Point(ValidTime),
    Interval { start: ValidTime, end: ValidTime },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResolvedTemporalScope {
    graph: GraphId,
    valid_time: ResolvedValidTime,
    transaction_time: TransactionTime,
}

impl ResolvedTemporalScope {
    #[must_use]
    pub const fn graph(&self) -> GraphId {
        self.graph
    }

    #[must_use]
    pub const fn valid_time(&self) -> ResolvedValidTime {
        self.valid_time
    }

    #[must_use]
    pub const fn transaction_time(&self) -> TransactionTime {
        self.transaction_time
    }

    pub fn point_read(self) -> Result<TemporalRead, RuntimeError> {
        match self.valid_time {
            ResolvedValidTime::Point(valid_time) => Ok(TemporalRead::as_of(
                self.graph,
                valid_time,
                self.transaction_time,
            )),
            ResolvedValidTime::Interval { .. } => Err(RuntimeError::UnsupportedOperator(
                "valid-time interval execution",
            )),
        }
    }
}

pub fn resolve_temporal_scope(
    graph: GraphId,
    valid_time: &ValidTimeSpec,
    transaction_time: &TransactionTimeSpec,
    current_valid_time: ValidTime,
    current_transaction_time: TransactionTime,
    context: &ExecutionContext,
) -> Result<ResolvedTemporalScope, RuntimeError> {
    context.check_fences()?;
    let valid_time = match valid_time {
        ValidTimeSpec::Current => ResolvedValidTime::Point(current_valid_time),
        ValidTimeSpec::AsOf(expression) => ResolvedValidTime::Point(ValidTime::from_micros(
            resolve_timestamp(expression, context)?,
        )),
        ValidTimeSpec::Between { start, end } => {
            let start = ValidTime::from_micros(resolve_timestamp(start, context)?);
            let end = ValidTime::from_micros(resolve_timestamp(end, context)?);
            if start >= end {
                return Err(RuntimeError::InvalidTemporalInterval);
            }
            ResolvedValidTime::Interval { start, end }
        }
    };
    let transaction_time = match transaction_time {
        TransactionTimeSpec::Current => current_transaction_time,
        TransactionTimeSpec::AsOf(expression) => {
            TransactionTime::new(resolve_timestamp(expression, context)?, u32::MAX)
        }
    };
    Ok(ResolvedTemporalScope {
        graph,
        valid_time,
        transaction_time,
    })
}

fn resolve_timestamp(
    expression: &ScalarExpr,
    context: &ExecutionContext,
) -> Result<i64, RuntimeError> {
    match evaluate(expression, &RowSchema::empty(), &[], context)? {
        RuntimeValue::TimestampMicros(value) => Ok(value),
        value => Err(RuntimeError::InvalidTemporalValue(value.kind())),
    }
}

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
        if fragment.operators().is_empty() {
            return Err(RuntimeError::UnsupportedOperator("empty fragment").into());
        }
        let mut batches = Vec::new();
        for operator in fragment.operators() {
            context.check_fences()?;
            match operator {
                PhysicalOperator::NodeScan { labels, output, .. } => {
                    batches = self.node_scan(labels, output, read).await?;
                }
                PhysicalOperator::RelationshipScan { types, output, .. } => {
                    batches = self.relationship_scan(types, output, read).await?;
                }
                PhysicalOperator::Expand {
                    source,
                    relationship,
                    destination,
                    outgoing,
                    types,
                    output,
                } => {
                    batches = self
                        .expand(
                            batches,
                            *source,
                            *relationship,
                            *destination,
                            *outgoing,
                            types,
                            output,
                            read,
                            context,
                        )
                        .await?;
                }
                _ => {
                    let expected = if matches!(operator, PhysicalOperator::Project { .. }) {
                        fragment.output().clone()
                    } else {
                        batches.first().map_or_else(
                            || fragment.output().clone(),
                            |batch| batch.schema().clone(),
                        )
                    };
                    batches = self.scalar.execute_operators(
                        std::slice::from_ref(operator),
                        &expected,
                        fragment.budget().memory_bytes(),
                        context,
                        batches,
                    )?;
                }
            }
        }
        if batches
            .iter()
            .any(|batch| batch.schema() != fragment.output())
        {
            return Err(RuntimeError::OutputSchemaMismatch.into());
        }
        Ok(batches)
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

    async fn relationship_scan(
        &self,
        types: &[u32],
        output: &temporal_ir::v2::RowSchema,
        read: TemporalRead,
    ) -> Result<Vec<RecordBatch>, TemporalExecutionError> {
        let edges = match read.transaction {
            TransactionRead::Current => {
                self.store
                    .scan_edges_current(read.graph, read.valid_time)
                    .await?
            }
            TransactionRead::AsOf(transaction_time) => {
                self.store
                    .scan_edges_as_of(read.graph, read.valid_time, transaction_time)
                    .await?
            }
        };
        let rows = edges
            .into_iter()
            .filter(|edge| types.is_empty() || types.contains(&edge.edge_type().value()))
            .map(|edge| vec![RuntimeValue::Relationship(EdgeRecord::from(edge))])
            .collect();
        batches_from_rows(output, rows).map_err(Into::into)
    }

    #[allow(clippy::too_many_arguments)]
    async fn expand(
        &self,
        batches: Vec<RecordBatch>,
        source: temporal_ir::v2::SlotId,
        relationship: temporal_ir::v2::SlotId,
        destination: temporal_ir::v2::SlotId,
        outgoing: bool,
        types: &[u32],
        output: &temporal_ir::v2::RowSchema,
        read: TemporalRead,
        context: &ExecutionContext,
    ) -> Result<Vec<RecordBatch>, TemporalExecutionError> {
        let mut output_rows = Vec::new();
        for batch in batches {
            let schema = batch.schema().clone();
            let source_index = schema
                .columns()
                .iter()
                .position(|column| column.slot() == source)
                .ok_or(RuntimeError::MissingSlot(source))?;
            for row in batch.into_rows() {
                context.check_fences()?;
                let source_value = row
                    .get(source_index)
                    .ok_or(RuntimeError::MissingSlot(source))?;
                let RuntimeValue::Node(source_node) = source_value else {
                    return Err(RuntimeError::TypeMismatch {
                        expected: temporal_ir::v2::ValueType::Node,
                        actual: source_value.kind(),
                    }
                    .into());
                };
                for edge in self
                    .expand_edges(source_node.element(), outgoing, read)
                    .await?
                    .into_iter()
                    .filter(|edge| types.is_empty() || types.contains(&edge.edge_type().value()))
                {
                    let destination_ref = if outgoing {
                        edge.destination_ref()
                    } else {
                        edge.source_ref()
                    };
                    let Some(destination_view) = self.vertex(destination_ref, read).await? else {
                        continue;
                    };
                    let mut values = schema
                        .columns()
                        .iter()
                        .zip(&row)
                        .map(|(column, value)| (column.slot(), value.clone()))
                        .collect::<BTreeMap<_, _>>();
                    values.insert(
                        relationship,
                        RuntimeValue::Relationship(EdgeRecord::from(edge)),
                    );
                    values.insert(
                        destination,
                        RuntimeValue::Node(VertexRecord::from(destination_view)),
                    );
                    output_rows.push(
                        output
                            .columns()
                            .iter()
                            .map(|column| {
                                values
                                    .get(&column.slot())
                                    .cloned()
                                    .ok_or(RuntimeError::MissingSlot(column.slot()))
                            })
                            .collect::<Result<Vec<_>, _>>()?,
                    );
                }
            }
        }
        batches_from_rows(output, output_rows).map_err(Into::into)
    }

    async fn expand_edges(
        &self,
        source: temporal_storage::ElementRef,
        outgoing: bool,
        read: TemporalRead,
    ) -> Result<Vec<EdgeView>, TemporalStoreError> {
        match (outgoing, read.transaction) {
            (true, TransactionRead::Current) => {
                self.store
                    .expand_out_current(
                        read.graph,
                        source.partition(),
                        source.id(),
                        read.valid_time,
                    )
                    .await
            }
            (false, TransactionRead::Current) => {
                self.store
                    .expand_in_current(read.graph, source.partition(), source.id(), read.valid_time)
                    .await
            }
            (true, TransactionRead::AsOf(transaction_time)) => {
                self.store
                    .expand_out_as_of(
                        read.graph,
                        source.partition(),
                        source.id(),
                        read.valid_time,
                        transaction_time,
                    )
                    .await
            }
            (false, TransactionRead::AsOf(transaction_time)) => {
                self.store
                    .expand_in_as_of(
                        read.graph,
                        source.partition(),
                        source.id(),
                        read.valid_time,
                        transaction_time,
                    )
                    .await
            }
        }
    }

    async fn vertex(
        &self,
        element: temporal_storage::ElementRef,
        read: TemporalRead,
    ) -> Result<Option<temporal_storage::VertexView>, TemporalStoreError> {
        match read.transaction {
            TransactionRead::Current => {
                self.store
                    .vertex_view_current(element, read.valid_time)
                    .await
            }
            TransactionRead::AsOf(transaction_time) => {
                self.store
                    .vertex_view_as_of(element, read.valid_time, transaction_time)
                    .await
            }
        }
    }
}

fn batches_from_rows(
    schema: &temporal_ir::v2::RowSchema,
    rows: Vec<Vec<RuntimeValue>>,
) -> Result<Vec<RecordBatch>, RuntimeError> {
    if rows.is_empty() {
        return Ok(vec![RecordBatch::try_new(schema.clone(), Vec::new())?]);
    }
    rows.chunks(MAX_BATCH_ROWS)
        .map(|chunk| RecordBatch::try_new(schema.clone(), chunk.to_vec()))
        .collect()
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
