use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{self, Display, Formatter};

use physical_plan::{PhysicalApply, PhysicalOperator, PhysicalPlan, Placement, PlanFragment};
use storage_api::StorageAdapter;
use temporal_ir::{
    ApplyKind, ResolvedProcedure, RowSchema, ScalarExpr, TransactionTimeSpec, ValidTimeSpec,
};
use temporal_storage::{ElementKind, GraphId, TemporalStore, TemporalStoreError};
use temporal_types::{Interval, TransactionTime, ValidTime};

use crate::{EdgeRecord, VertexRecord};

use super::executor::{
    aggregate_expression, compare_values, estimate_composed_procedure_row, invoke_procedure_row,
    provider_schema_error, row_count, runtime_value,
};
use super::temporal_row::{region_cells, region_contains};
use super::{
    BatchExecutor, ChildOutputDemand, ChildPlanInvoker, ExecutionContext, MAX_BATCH_ROWS,
    RecordBatch, RuntimeError, RuntimeValue,
};
use super::{
    TemporalProvenance, TemporalRegion, TemporalRow, coalesce_temporal_rows,
    distinct_temporal_rows, ensure_temporal_rows_memory, temporal_join,
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
        RuntimeValue::TimestampMicros(value) | RuntimeValue::Integer(value) => Ok(value),
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

    pub async fn scan_vertex_rows_interval_as_of(
        &self,
        graph: GraphId,
        labels: &[u32],
        window: Interval<ValidTime>,
        transaction_time: TransactionTime,
    ) -> Result<Vec<TemporalRow>, TemporalExecutionError> {
        let transaction = TemporalRegion::at_transaction(transaction_time)
            .ok_or(RuntimeError::InvalidTemporalInterval)?
            .transaction();
        let segments = self
            .store
            .scan_vertex_segments_as_of(graph, window, transaction_time)
            .await?;
        Ok(segments
            .into_iter()
            .filter(|segment| labels.is_empty() || labels.contains(&segment.label().value()))
            .map(|segment| {
                TemporalRow::with_provenance(
                    vec![RuntimeValue::Node(VertexRecord::new(
                        segment.element(),
                        Some(segment.label()),
                        segment.payload().clone(),
                    ))],
                    TemporalRegion::new(segment.valid(), transaction),
                    vec![TemporalProvenance::Element(segment.element())],
                )
            })
            .collect())
    }

    pub async fn scan_edge_rows_interval_as_of(
        &self,
        graph: GraphId,
        types: &[u32],
        window: Interval<ValidTime>,
        transaction_time: TransactionTime,
    ) -> Result<Vec<TemporalRow>, TemporalExecutionError> {
        let transaction = TemporalRegion::at_transaction(transaction_time)
            .ok_or(RuntimeError::InvalidTemporalInterval)?
            .transaction();
        let segments = self
            .store
            .scan_edge_segments_as_of(graph, window, transaction_time)
            .await?;
        Ok(segments
            .into_iter()
            .filter(|segment| types.is_empty() || types.contains(&segment.edge_type().value()))
            .map(|segment| {
                TemporalRow::with_provenance(
                    vec![RuntimeValue::Relationship(EdgeRecord::from_endpoints(
                        segment.element(),
                        segment.edge_type(),
                        segment.source_ref(),
                        segment.destination_ref(),
                        segment.payload().clone(),
                    ))],
                    TemporalRegion::new(segment.valid(), transaction),
                    vec![TemporalProvenance::Element(segment.element())],
                )
            })
            .collect())
    }

    async fn vertex_rows_interval_as_of(
        &self,
        element: temporal_storage::ElementRef,
        window: Interval<ValidTime>,
        transaction_time: TransactionTime,
    ) -> Result<Vec<TemporalRow>, TemporalExecutionError> {
        let transaction = TemporalRegion::at_transaction(transaction_time)
            .ok_or(RuntimeError::InvalidTemporalInterval)?
            .transaction();
        Ok(self
            .store
            .vertex_segments_as_of(element, window, transaction_time)
            .await?
            .into_iter()
            .map(|segment| {
                TemporalRow::with_provenance(
                    vec![RuntimeValue::Node(VertexRecord::new(
                        segment.element(),
                        Some(segment.label()),
                        segment.payload().clone(),
                    ))],
                    TemporalRegion::new(segment.valid(), transaction),
                    vec![TemporalProvenance::Element(segment.element())],
                )
            })
            .collect())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn expand_interval_rows(
        &self,
        source_rows: &[TemporalRow],
        graph: GraphId,
        outgoing: bool,
        types: &[u32],
        window: Interval<ValidTime>,
        transaction_time: TransactionTime,
    ) -> Result<Vec<TemporalRow>, TemporalExecutionError> {
        let Some(source_position) = source_rows
            .first()
            .and_then(|row| row.values().len().checked_sub(1))
        else {
            return Ok(Vec::new());
        };
        self.expand_interval_joined_rows(
            source_rows,
            source_position,
            graph,
            outgoing,
            types,
            window,
            transaction_time,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn expand_interval_rows_with_slots(
        &self,
        source_rows: &[TemporalRow],
        source_position: usize,
        input: &RowSchema,
        relationship: temporal_ir::SlotId,
        destination: temporal_ir::SlotId,
        output: &RowSchema,
        graph: GraphId,
        outgoing: bool,
        types: &[u32],
        window: Interval<ValidTime>,
        transaction_time: TransactionTime,
    ) -> Result<Vec<TemporalRow>, TemporalExecutionError> {
        let rows = self
            .expand_interval_joined_rows(
                source_rows,
                source_position,
                graph,
                outgoing,
                types,
                window,
                transaction_time,
            )
            .await?;
        let input_slots = input
            .columns()
            .iter()
            .enumerate()
            .map(|(index, column)| (column.slot(), index))
            .collect::<BTreeMap<_, _>>();
        let relationship_index = input.columns().len();
        let destination_index = relationship_index
            .checked_add(1)
            .ok_or(RuntimeError::SizeOverflow)?;
        rows.into_iter()
            .map(|row| {
                let values = output
                    .columns()
                    .iter()
                    .map(|column| {
                        let index = if column.slot() == relationship {
                            relationship_index
                        } else if column.slot() == destination {
                            destination_index
                        } else {
                            *input_slots
                                .get(&column.slot())
                                .ok_or(RuntimeError::MissingSlot(column.slot()))?
                        };
                        row.values()
                            .get(index)
                            .cloned()
                            .ok_or(RuntimeError::MissingSlot(column.slot()))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(row.with_values(values))
            })
            .collect::<Result<Vec<_>, RuntimeError>>()
            .map_err(Into::into)
    }

    #[allow(clippy::too_many_arguments)]
    async fn expand_interval_joined_rows(
        &self,
        source_rows: &[TemporalRow],
        source_position: usize,
        graph: GraphId,
        outgoing: bool,
        types: &[u32],
        window: Interval<ValidTime>,
        transaction_time: TransactionTime,
    ) -> Result<Vec<TemporalRow>, TemporalExecutionError> {
        let edges = self
            .scan_edge_rows_interval_as_of(graph, types, window, transaction_time)
            .await?;
        let source_edges = temporal_join(source_rows, &edges, |source, edge| {
            let Some(RuntimeValue::Node(source)) = source.values().get(source_position) else {
                return false;
            };
            let Some(RuntimeValue::Relationship(edge)) = edge.values().first() else {
                return false;
            };
            if outgoing {
                edge.source_ref() == source.element()
            } else {
                edge.destination_ref() == source.element()
            }
        });
        if source_edges.is_empty() {
            return Ok(Vec::new());
        }
        let endpoints = source_edges
            .iter()
            .filter_map(|row| {
                let RuntimeValue::Relationship(edge) = row.values().last()? else {
                    return None;
                };
                Some(if outgoing {
                    edge.destination_ref()
                } else {
                    edge.source_ref()
                })
            })
            .collect::<BTreeSet<_>>();
        let mut vertices = Vec::new();
        for endpoint in endpoints {
            vertices.extend(
                self.vertex_rows_interval_as_of(endpoint, window, transaction_time)
                    .await?,
            );
        }
        Ok(temporal_join(
            &source_edges,
            &vertices,
            |source_edge, destination| {
                let Some(RuntimeValue::Relationship(edge)) = source_edge.values().last() else {
                    return false;
                };
                let Some(RuntimeValue::Node(destination)) = destination.values().first() else {
                    return false;
                };
                if outgoing {
                    edge.destination_ref() == destination.element()
                } else {
                    edge.source_ref() == destination.element()
                }
            },
        ))
    }

    pub async fn execute_interval_fragment_rows(
        &self,
        fragment: &PlanFragment,
        context: &ExecutionContext,
        graph: GraphId,
        window: Interval<ValidTime>,
        transaction_time: TransactionTime,
    ) -> Result<Vec<TemporalRow>, TemporalExecutionError> {
        if fragment.operators().is_empty() {
            return Err(RuntimeError::UnsupportedOperator("empty fragment").into());
        }
        let mut rows = Vec::new();
        let mut schema = RowSchema::empty();
        for operator in fragment.operators() {
            context.check_fences()?;
            match operator {
                PhysicalOperator::NodeScan { labels, output, .. } => {
                    rows = self
                        .scan_vertex_rows_interval_as_of(graph, labels, window, transaction_time)
                        .await?;
                    schema = output.clone();
                }
                PhysicalOperator::RelationshipScan { types, output, .. } => {
                    rows = self
                        .scan_edge_rows_interval_as_of(graph, types, window, transaction_time)
                        .await?;
                    schema = output.clone();
                }
                PhysicalOperator::Expand {
                    source,
                    relationship,
                    destination,
                    outgoing,
                    types,
                    output,
                } => {
                    let source_position = schema
                        .columns()
                        .iter()
                        .position(|column| column.slot() == *source)
                        .ok_or(RuntimeError::MissingSlot(*source))?;
                    rows = self
                        .expand_interval_rows_with_slots(
                            &rows,
                            source_position,
                            &schema,
                            *relationship,
                            *destination,
                            output,
                            graph,
                            *outgoing,
                            types,
                            window,
                            transaction_time,
                        )
                        .await?;
                    if output.columns().len() != schema.columns().len() + 2
                        || !output
                            .columns()
                            .iter()
                            .any(|column| column.slot() == *relationship)
                        || !output
                            .columns()
                            .iter()
                            .any(|column| column.slot() == *destination)
                    {
                        return Err(RuntimeError::OutputSchemaMismatch.into());
                    }
                    schema = output.clone();
                }
                PhysicalOperator::Filter(predicate) => {
                    let mut filtered = Vec::with_capacity(rows.len());
                    for row in rows {
                        match evaluate(predicate, &schema, row.values(), context)? {
                            RuntimeValue::Boolean(true) => filtered.push(row),
                            RuntimeValue::Boolean(false) | RuntimeValue::Null => {}
                            value => {
                                return Err(RuntimeError::TypeMismatch {
                                    expected: temporal_ir::ValueType::Boolean,
                                    actual: value.kind(),
                                }
                                .into());
                            }
                        }
                    }
                    rows = filtered;
                }
                PhysicalOperator::Project {
                    expressions,
                    output,
                } => {
                    let mut projected = Vec::with_capacity(rows.len());
                    for row in rows {
                        let values = expressions
                            .iter()
                            .map(|(slot, expression)| {
                                Ok((*slot, evaluate(expression, &schema, row.values(), context)?))
                            })
                            .collect::<Result<BTreeMap<_, _>, RuntimeError>>()?;
                        let values = output
                            .columns()
                            .iter()
                            .map(|column| {
                                values
                                    .get(&column.slot())
                                    .cloned()
                                    .ok_or(RuntimeError::MissingSlot(column.slot()))
                            })
                            .collect::<Result<Vec<_>, _>>()?;
                        projected.push(row.with_values(values));
                    }
                    rows = projected;
                    schema = output.clone();
                }
                PhysicalOperator::Unwind {
                    expression,
                    binding,
                    output,
                } => {
                    rows = unwind_interval_rows(
                        rows,
                        &schema,
                        expression,
                        *binding,
                        output,
                        fragment.budget().memory_bytes(),
                        context,
                    )?;
                    schema = output.clone();
                }
                PhysicalOperator::Skip { count } => {
                    let count = row_count(count, context)?;
                    rows = rows.into_iter().skip(count).collect();
                }
                PhysicalOperator::Limit { count } => {
                    let count = row_count(count, context)?;
                    rows = rows.into_iter().take(count).collect();
                }
                PhysicalOperator::Sort { keys } => {
                    sort_temporal_rows(&mut rows, &schema, keys)?;
                }
                PhysicalOperator::Aggregate {
                    grouping,
                    aggregates,
                    output,
                } => {
                    rows = aggregate_interval_rows(
                        rows, &schema, grouping, aggregates, output, context,
                    )?;
                    schema = output.clone();
                }
                PhysicalOperator::TemporalSlice { .. } | PhysicalOperator::Finish => {}
                _ => {
                    return Err(
                        RuntimeError::UnsupportedOperator("interval fragment operator").into(),
                    );
                }
            }
            ensure_temporal_rows_memory(&schema, &rows, fragment.budget().memory_bytes())?;
        }
        if schema != *fragment.output() {
            return Err(RuntimeError::OutputSchemaMismatch.into());
        }
        Ok(rows)
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
                    batches = self.node_scan(labels, output, read, context).await?;
                }
                PhysicalOperator::RelationshipScan { types, output, .. } => {
                    batches = self.relationship_scan(types, output, read, context).await?;
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
                    let expected = if let PhysicalOperator::Argument { output } = operator {
                        output.clone()
                    } else if let PhysicalOperator::Unwind { output, .. } = operator {
                        output.clone()
                    } else if let PhysicalOperator::Project { output, .. }
                    | PhysicalOperator::Aggregate { output, .. } = operator
                    {
                        output.clone()
                    } else {
                        batches.first().map_or_else(
                            || fragment.output().clone(),
                            |batch| batch.schema().clone(),
                        )
                    };
                    batches = self
                        .scalar
                        .execute_operators(
                            std::slice::from_ref(operator),
                            &expected,
                            fragment.budget().memory_bytes(),
                            context,
                            batches,
                            None,
                            ChildOutputDemand::AllRows,
                            &super::ApplyBudgetLedger::default(),
                        )
                        .await?;
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
        output: &temporal_ir::RowSchema,
        read: TemporalRead,
        context: &ExecutionContext,
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
        let mut combined = vertices
            .into_iter()
            .map(|vertex| {
                let value = RuntimeValue::Node(VertexRecord::from(vertex));
                let RuntimeValue::Node(vertex) = &value else {
                    unreachable!("constructed a node value")
                };
                (vertex.element(), value)
            })
            .collect::<BTreeMap<_, _>>();
        for (element, replacement) in context
            .graph_overlay()
            .visible_scan(read.valid_time, ElementKind::Vertex)
        {
            if let Some(replacement) = replacement {
                combined.insert(element, replacement);
            } else {
                combined.remove(&element);
            }
        }
        let rows = combined
            .into_values()
            .filter(|value| {
                let RuntimeValue::Node(vertex) = value else {
                    return false;
                };
                labels.is_empty()
                    || vertex
                        .label()
                        .is_some_and(|label| labels.contains(&label.value()))
            })
            .map(|value| vec![value])
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
        output: &temporal_ir::RowSchema,
        read: TemporalRead,
        context: &ExecutionContext,
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
        let mut combined = edges
            .into_iter()
            .map(|edge| {
                let value = RuntimeValue::Relationship(EdgeRecord::from(edge));
                let RuntimeValue::Relationship(edge) = &value else {
                    unreachable!("constructed a relationship value")
                };
                (edge.element(), value)
            })
            .collect::<BTreeMap<_, _>>();
        for (element, replacement) in context
            .graph_overlay()
            .visible_scan(read.valid_time, ElementKind::Edge)
        {
            if let Some(replacement) = replacement {
                combined.insert(element, replacement);
            } else {
                combined.remove(&element);
            }
        }
        let rows = combined
            .into_values()
            .filter(|value| {
                let RuntimeValue::Relationship(edge) = value else {
                    return false;
                };
                types.is_empty() || types.contains(&edge.edge_type().value())
            })
            .map(|value| vec![value])
            .collect();
        batches_from_rows(output, rows).map_err(Into::into)
    }

    #[allow(clippy::too_many_arguments)]
    async fn expand(
        &self,
        batches: Vec<RecordBatch>,
        source: temporal_ir::SlotId,
        relationship: temporal_ir::SlotId,
        destination: temporal_ir::SlotId,
        outgoing: bool,
        types: &[u32],
        output: &temporal_ir::RowSchema,
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
                        expected: temporal_ir::ValueType::Node,
                        actual: source_value.kind(),
                    }
                    .into());
                };
                for edge in self
                    .combined_expand_edges(source_node.element(), outgoing, read, context)
                    .await?
                    .into_iter()
                    .filter(|edge| types.is_empty() || types.contains(&edge.edge_type().value()))
                {
                    let destination_ref = if outgoing {
                        edge.destination_ref()
                    } else {
                        edge.source_ref()
                    };
                    let Some(destination_value) =
                        self.combined_vertex(destination_ref, read, context).await?
                    else {
                        continue;
                    };
                    let mut values = schema
                        .columns()
                        .iter()
                        .zip(&row)
                        .map(|(column, value)| (column.slot(), value.clone()))
                        .collect::<BTreeMap<_, _>>();
                    values.insert(relationship, RuntimeValue::Relationship(edge));
                    values.insert(destination, destination_value);
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

    async fn combined_expand_edges(
        &self,
        source: temporal_storage::ElementRef,
        outgoing: bool,
        read: TemporalRead,
        context: &ExecutionContext,
    ) -> Result<Vec<EdgeRecord>, TemporalStoreError> {
        let committed = match (outgoing, read.transaction) {
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
        }?;
        let mut combined = committed
            .into_iter()
            .map(|edge| {
                let edge = EdgeRecord::from(edge);
                (edge.element(), edge)
            })
            .collect::<BTreeMap<_, _>>();
        for (element, replacement) in context
            .graph_overlay()
            .visible_expand(read.valid_time, ElementKind::Edge)
        {
            match replacement {
                Some(RuntimeValue::Relationship(edge)) => {
                    combined.insert(element, edge);
                }
                Some(_) | None => {
                    combined.remove(&element);
                }
            }
        }
        Ok(combined
            .into_values()
            .filter(|edge| {
                if outgoing {
                    edge.source_ref() == source
                } else {
                    edge.destination_ref() == source
                }
            })
            .collect())
    }

    async fn combined_vertex(
        &self,
        element: temporal_storage::ElementRef,
        read: TemporalRead,
        context: &ExecutionContext,
    ) -> Result<Option<RuntimeValue>, TemporalStoreError> {
        let committed = match read.transaction {
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
        }?
        .map(VertexRecord::from)
        .map(RuntimeValue::Node);
        Ok(context
            .graph_overlay()
            .visible_expand(read.valid_time, ElementKind::Vertex)
            .remove(&element)
            .unwrap_or(committed))
    }
}

pub async fn execute_interval_coordinator_operators(
    fragment: &PlanFragment,
    schema: RowSchema,
    rows: Vec<TemporalRow>,
    context: &ExecutionContext,
) -> Result<Vec<TemporalRow>, RuntimeError> {
    execute_interval_coordinator_operators_with_invoker(fragment, schema, rows, context, None).await
}

pub async fn execute_interval_coordinator_operators_with_invoker(
    fragment: &PlanFragment,
    schema: RowSchema,
    rows: Vec<TemporalRow>,
    context: &ExecutionContext,
    child_invoker: Option<&dyn ChildPlanInvoker>,
) -> Result<Vec<TemporalRow>, RuntimeError> {
    execute_interval_coordinator_operators_with_invoker_and_ledger(
        fragment,
        schema,
        rows,
        context,
        child_invoker,
        super::ApplyBudgetLedger::default(),
    )
    .await
}

pub async fn execute_interval_coordinator_operators_with_invoker_and_ledger(
    fragment: &PlanFragment,
    mut schema: RowSchema,
    mut rows: Vec<TemporalRow>,
    context: &ExecutionContext,
    child_invoker: Option<&dyn ChildPlanInvoker>,
    ledger: super::ApplyBudgetLedger,
) -> Result<Vec<TemporalRow>, RuntimeError> {
    for operator in fragment.operators() {
        context.check_fences()?;
        match operator {
            PhysicalOperator::Argument { output } => schema = output.clone(),
            PhysicalOperator::Filter(predicate) => {
                let mut filtered = Vec::with_capacity(rows.len());
                for row in rows {
                    match evaluate(predicate, &schema, row.values(), context)? {
                        RuntimeValue::Boolean(true) => filtered.push(row),
                        RuntimeValue::Boolean(false) | RuntimeValue::Null => {}
                        value => {
                            return Err(RuntimeError::TypeMismatch {
                                expected: temporal_ir::ValueType::Boolean,
                                actual: value.kind(),
                            });
                        }
                    }
                }
                rows = filtered;
            }
            PhysicalOperator::Project {
                expressions,
                output,
            } => {
                let mut projected = Vec::with_capacity(rows.len());
                for row in rows {
                    let values = expressions
                        .iter()
                        .map(|(slot, expression)| {
                            Ok((*slot, evaluate(expression, &schema, row.values(), context)?))
                        })
                        .collect::<Result<BTreeMap<_, _>, RuntimeError>>()?;
                    let values = output
                        .columns()
                        .iter()
                        .map(|column| {
                            values
                                .get(&column.slot())
                                .cloned()
                                .ok_or(RuntimeError::MissingSlot(column.slot()))
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    projected.push(row.with_values(values));
                }
                rows = projected;
                schema = output.clone();
            }
            PhysicalOperator::Unwind {
                expression,
                binding,
                output,
            } => {
                rows = unwind_interval_rows(
                    rows,
                    &schema,
                    expression,
                    *binding,
                    output,
                    fragment.budget().memory_bytes(),
                    context,
                )?;
                schema = output.clone();
            }
            PhysicalOperator::Skip { count } => {
                let count = row_count(count, context)?;
                rows = rows.into_iter().skip(count).collect();
            }
            PhysicalOperator::Limit { count } => {
                let count = row_count(count, context)?;
                rows = rows.into_iter().take(count).collect();
            }
            PhysicalOperator::Sort { keys } => {
                sort_temporal_rows(&mut rows, &schema, keys)?;
            }
            PhysicalOperator::Aggregate {
                grouping,
                aggregates,
                output,
            } => {
                rows =
                    aggregate_interval_rows(rows, &schema, grouping, aggregates, output, context)?;
                schema = output.clone();
            }
            PhysicalOperator::Apply { apply, output } => {
                rows = execute_interval_apply(
                    rows,
                    &schema,
                    apply,
                    output,
                    fragment.budget().memory_bytes(),
                    context,
                    child_invoker,
                    &ledger,
                )
                .await?;
                schema = output.clone();
            }
            PhysicalOperator::Procedure { procedure, output } => {
                rows = execute_interval_procedure(
                    rows,
                    &schema,
                    procedure,
                    output,
                    fragment.budget().memory_bytes(),
                    context,
                )
                .await?;
                schema = output.clone();
            }
            PhysicalOperator::Union { all } => {
                if !all {
                    rows = distinct_temporal_rows(rows);
                }
            }
            PhysicalOperator::TemporalSlice { .. } | PhysicalOperator::Finish => {}
            _ => {
                return Err(RuntimeError::UnsupportedOperator(
                    "interval coordinator operator",
                ));
            }
        }
        ensure_temporal_rows_memory(&schema, &rows, fragment.budget().memory_bytes())?;
    }
    if schema != *fragment.output() {
        return Err(RuntimeError::OutputSchemaMismatch);
    }
    Ok(rows)
}

async fn execute_interval_procedure(
    rows: Vec<TemporalRow>,
    input_schema: &RowSchema,
    procedure: &ResolvedProcedure,
    output: &RowSchema,
    memory_limit: u64,
    context: &ExecutionContext,
) -> Result<Vec<TemporalRow>, RuntimeError> {
    let input_rows = rows.len();
    if u64::try_from(input_rows).map_err(|_| RuntimeError::SizeOverflow)?
        > procedure.max_input_rows()
    {
        return Err(RuntimeError::ProcedureInputRowLimit {
            max: procedure.max_input_rows(),
        });
    }
    if input_rows > usize::try_from(procedure.max_invocations()).unwrap_or(usize::MAX) {
        return Err(RuntimeError::ProcedureInvocationLimit {
            max: procedure.max_invocations(),
        });
    }
    let registry = context.procedure_registry()?;
    let mut output_rows = Vec::new();
    let mut retained_bytes = 0_u64;
    for input_row in rows {
        let result = invoke_procedure_row(
            registry,
            procedure,
            input_schema,
            input_row.values(),
            context,
        )
        .await?;
        for provider_row in result.rows() {
            let next_count = output_rows
                .len()
                .checked_add(1)
                .ok_or(RuntimeError::SizeOverflow)?;
            if u64::try_from(next_count).map_err(|_| RuntimeError::SizeOverflow)?
                > procedure.max_output_rows()
            {
                return Err(RuntimeError::ProcedureOutputRowLimit {
                    max: procedure.max_output_rows(),
                });
            }
            let row_bytes = estimate_composed_procedure_row(
                input_row.values(),
                provider_row,
                procedure.yields(),
            )?;
            let required = retained_bytes
                .checked_add(row_bytes)
                .ok_or(RuntimeError::SizeOverflow)?;
            if required > memory_limit {
                return Err(RuntimeError::MemoryLimitExceeded {
                    limit: memory_limit,
                    required,
                });
            }
            let mut values = input_row.values().to_vec();
            for binding in procedure.yields() {
                let value = provider_row
                    .get(
                        usize::try_from(binding.source_index())
                            .map_err(|_| RuntimeError::SizeOverflow)?,
                    )
                    .ok_or_else(provider_schema_error)?;
                values.push(runtime_value(value.clone()));
            }
            retained_bytes = required;
            output_rows.push(input_row.with_values(values));
        }
    }
    ensure_temporal_rows_memory(output, &output_rows, memory_limit)?;
    Ok(output_rows)
}

#[allow(clippy::too_many_arguments)]
async fn execute_interval_apply(
    rows: Vec<TemporalRow>,
    parent_schema: &RowSchema,
    apply: &PhysicalApply,
    output: &RowSchema,
    memory_limit: u64,
    context: &ExecutionContext,
    child_invoker: Option<&dyn ChildPlanInvoker>,
    ledger: &super::ApplyBudgetLedger,
) -> Result<Vec<TemporalRow>, RuntimeError> {
    let mut applied = Vec::new();
    let mut retained_bytes = 0_u64;
    let limits = super::ChildInvocationLimits {
        max_invocations: apply.max_invocations(),
        max_output_rows: apply.max_output_rows(),
        max_depth: apply.max_depth(),
    };
    for parent in rows {
        context.check_fences()?;
        ledger.charge_invocation(limits)?;
        let _depth = ledger.enter(limits)?;
        let child_values = apply
            .child_input()
            .columns()
            .iter()
            .map(|child_column| {
                let mapping = apply
                    .imports()
                    .iter()
                    .find(|mapping| mapping.child_slot() == child_column.slot())
                    .ok_or(RuntimeError::MissingSlot(child_column.slot()))?;
                let index = parent_schema
                    .columns()
                    .iter()
                    .position(|column| column.slot() == mapping.parent_slot())
                    .ok_or(RuntimeError::MissingSlot(mapping.parent_slot()))?;
                parent
                    .values()
                    .get(index)
                    .cloned()
                    .ok_or(RuntimeError::MissingSlot(mapping.parent_slot()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let demand = ChildOutputDemand::AllRows;
        let child_input = vec![TemporalRow::new(child_values, parent.region())];
        let child_rows = if let Some(invoker) = child_invoker {
            invoker
                .invoke_interval(
                    apply.child_plan(),
                    apply.child_input().clone(),
                    child_input,
                    context,
                    demand,
                    limits,
                    ledger.clone(),
                )
                .await?
        } else {
            Box::pin(execute_interval_child_plan(
                apply.child_plan(),
                apply.child_input().clone(),
                child_input,
                context,
                child_invoker,
                demand,
                ledger,
            ))
            .await?
        };
        match apply.kind() {
            ApplyKind::Inner => {
                for child in child_rows {
                    let Some(region) = parent.region().intersection(child.region()) else {
                        continue;
                    };
                    let child_schema = apply
                        .child_plan()
                        .output()
                        .ok_or(RuntimeError::InvalidPhysicalPlan)?;
                    let mut values = parent.values().to_vec();
                    for column in &output.columns()[parent_schema.columns().len()..] {
                        let mapping = apply
                            .exports()
                            .iter()
                            .find(|mapping| mapping.parent_slot() == column.slot())
                            .ok_or(RuntimeError::MissingSlot(column.slot()))?;
                        let index = child_schema
                            .columns()
                            .iter()
                            .position(|column| column.slot() == mapping.child_slot())
                            .ok_or(RuntimeError::MissingSlot(mapping.child_slot()))?;
                        values.push(
                            child
                                .values()
                                .get(index)
                                .cloned()
                                .ok_or(RuntimeError::MissingSlot(mapping.child_slot()))?,
                        );
                    }
                    let provenance = parent
                        .provenance()
                        .iter()
                        .chain(child.provenance())
                        .cloned()
                        .collect();
                    retain_temporal_apply_row(
                        &mut applied,
                        &mut retained_bytes,
                        TemporalRow::with_provenance(values, region, provenance),
                        output,
                        apply.max_output_rows(),
                        memory_limit,
                        ledger,
                        limits,
                    )?;
                }
            }
            ApplyKind::Exists { .. } => {
                for row in scalar_apply_temporal_cells(&parent, &child_rows, apply.kind())? {
                    retain_temporal_apply_row(
                        &mut applied,
                        &mut retained_bytes,
                        row,
                        output,
                        apply.max_output_rows(),
                        memory_limit,
                        ledger,
                        limits,
                    )?;
                }
            }
            ApplyKind::Count { .. } => {
                for row in scalar_apply_temporal_cells(&parent, &child_rows, apply.kind())? {
                    retain_temporal_apply_row(
                        &mut applied,
                        &mut retained_bytes,
                        row,
                        output,
                        apply.max_output_rows(),
                        memory_limit,
                        ledger,
                        limits,
                    )?;
                }
            }
        }
    }
    let applied = coalesce_temporal_rows(applied);
    ensure_temporal_rows_memory(output, &applied, memory_limit)?;
    Ok(applied)
}

#[allow(clippy::too_many_arguments)]
fn retain_temporal_apply_row(
    applied: &mut Vec<TemporalRow>,
    retained_bytes: &mut u64,
    row: TemporalRow,
    output: &RowSchema,
    max_output_rows: u64,
    memory_limit: u64,
    ledger: &super::ApplyBudgetLedger,
    limits: super::ChildInvocationLimits,
) -> Result<(), RuntimeError> {
    let next_rows = u64::try_from(applied.len())
        .map_err(|_| RuntimeError::SizeOverflow)?
        .checked_add(1)
        .ok_or(RuntimeError::SizeOverflow)?;
    if next_rows > max_output_rows {
        return Err(RuntimeError::ApplyOutputRowLimit {
            max: max_output_rows,
        });
    }
    ledger.charge_output(1, limits)?;
    let bytes =
        super::TemporalRecordBatch::try_new(output.clone(), vec![row.clone()])?.estimated_bytes();
    let next_bytes = retained_bytes
        .checked_add(bytes)
        .ok_or(RuntimeError::SizeOverflow)?;
    if next_bytes > memory_limit {
        return Err(RuntimeError::MemoryLimitExceeded {
            limit: memory_limit,
            required: next_bytes,
        });
    }
    *retained_bytes = next_bytes;
    applied.push(row);
    Ok(())
}

fn scalar_apply_temporal_cells(
    parent: &TemporalRow,
    child_rows: &[TemporalRow],
    kind: ApplyKind,
) -> Result<Vec<TemporalRow>, RuntimeError> {
    let intersections = child_rows
        .iter()
        .filter_map(|child| {
            parent
                .region()
                .intersection(child.region())
                .map(|region| (child, region))
        })
        .collect::<Vec<_>>();
    let valid_cells = region_cells(
        parent.region().valid(),
        intersections.iter().map(|(_, region)| region.valid()),
    );
    let transaction_cells = region_cells(
        parent.region().transaction(),
        intersections.iter().map(|(_, region)| region.transaction()),
    );
    let mut rows = Vec::new();
    for valid in valid_cells {
        for transaction in &transaction_cells {
            let region = TemporalRegion::new(valid, *transaction);
            let active = intersections
                .iter()
                .filter(|(_, visible)| region_contains(*visible, region))
                .map(|(child, _)| *child)
                .collect::<Vec<_>>();
            let scalar = match kind {
                ApplyKind::Exists { .. } => RuntimeValue::Boolean(!active.is_empty()),
                ApplyKind::Count { .. } => RuntimeValue::Integer(
                    i64::try_from(active.len()).map_err(|_| RuntimeError::SizeOverflow)?,
                ),
                ApplyKind::Inner => return Err(RuntimeError::InvalidPhysicalPlan),
            };
            let mut values = parent.values().to_vec();
            values.push(scalar);
            let mut provenance = parent.provenance().to_vec();
            for child in active {
                provenance.extend(child.provenance().iter().cloned());
            }
            provenance.sort();
            provenance.dedup();
            rows.push(TemporalRow::with_provenance(values, region, provenance));
        }
    }
    Ok(coalesce_temporal_rows(rows))
}

async fn execute_interval_child_plan(
    plan: &PhysicalPlan,
    input_schema: RowSchema,
    input_rows: Vec<TemporalRow>,
    context: &ExecutionContext,
    child_invoker: Option<&dyn ChildPlanInvoker>,
    demand: ChildOutputDemand,
    ledger: &super::ApplyBudgetLedger,
) -> Result<Vec<TemporalRow>, RuntimeError> {
    plan.validate()
        .map_err(|_| RuntimeError::InvalidPhysicalPlan)?;
    let mut results: BTreeMap<physical_plan::FragmentId, Vec<TemporalRow>> = BTreeMap::new();
    for fragment in plan.fragments() {
        if fragment.placement() != Placement::Coordinator {
            return Err(RuntimeError::UnsupportedOperator(
                "distributed interval Apply child plan",
            ));
        }
        let incoming = plan
            .exchanges()
            .iter()
            .filter(|exchange| exchange.to() == fragment.id())
            .collect::<Vec<_>>();
        let (schema, rows) = if incoming.is_empty() {
            (input_schema.clone(), input_rows.clone())
        } else {
            let schema = incoming[0].schema().clone();
            let mut rows = Vec::new();
            for exchange in incoming {
                if exchange.schema() != &schema {
                    return Err(RuntimeError::InvalidPhysicalPlan);
                }
                rows.extend(
                    results
                        .get(&exchange.from())
                        .ok_or(RuntimeError::InvalidPhysicalPlan)?
                        .iter()
                        .cloned(),
                );
            }
            (schema, rows)
        };
        let rows = execute_interval_coordinator_operators_with_invoker_and_ledger(
            fragment,
            schema,
            rows,
            context,
            child_invoker,
            ledger.clone(),
        )
        .await?;
        results.insert(fragment.id(), rows);
    }
    let mut rows = results
        .remove(&plan.root())
        .ok_or(RuntimeError::InvalidPhysicalPlan)?;
    if demand == ChildOutputDemand::FirstVisibleRow {
        rows.truncate(1);
    }
    Ok(rows)
}

fn unwind_interval_rows(
    rows: Vec<TemporalRow>,
    schema: &RowSchema,
    expression: &ScalarExpr,
    binding: temporal_ir::SlotId,
    output: &RowSchema,
    memory_limit: u64,
    context: &ExecutionContext,
) -> Result<Vec<TemporalRow>, RuntimeError> {
    let mut expanded = Vec::new();
    let mut required = 0_u64;
    for row in rows {
        context.check_fences()?;
        let value = evaluate(expression, schema, row.values(), context)?;
        let values = match value {
            RuntimeValue::List(values) => values,
            RuntimeValue::Null => Vec::new(),
            value => {
                return Err(RuntimeError::TypeMismatch {
                    expected: temporal_ir::ValueType::List(Box::new(temporal_ir::ValueType::Any)),
                    actual: value.kind(),
                });
            }
        };
        if values.len() > MAX_BATCH_ROWS {
            return Err(RuntimeError::BatchTooLarge {
                max: MAX_BATCH_ROWS,
                actual: values.len(),
            });
        }
        let existing = schema
            .columns()
            .iter()
            .zip(row.values())
            .map(|(column, value)| (column.slot(), value.clone()))
            .collect::<BTreeMap<_, _>>();
        for (index, value) in values.into_iter().enumerate() {
            context.check_fences()?;
            let values = output
                .columns()
                .iter()
                .map(|column| {
                    if column.slot() == binding {
                        Ok(value.clone())
                    } else {
                        existing
                            .get(&column.slot())
                            .cloned()
                            .ok_or(RuntimeError::MissingSlot(column.slot()))
                    }
                })
                .collect::<Result<Vec<_>, _>>()?;
            let row_bytes = values.iter().try_fold(0_u64, |total, value| {
                total
                    .checked_add(value.estimated_bytes()?)
                    .ok_or(RuntimeError::SizeOverflow)
            })?;
            required = required
                .checked_add(row_bytes)
                .ok_or(RuntimeError::SizeOverflow)?;
            if required > memory_limit {
                return Err(RuntimeError::MemoryLimitExceeded {
                    limit: memory_limit,
                    required,
                });
            }
            let index = u32::try_from(index).map_err(|_| RuntimeError::SizeOverflow)?;
            expanded.push(
                row.with_values(values)
                    .with_appended_provenance(TemporalProvenance::Unwind(index)),
            );
        }
    }
    Ok(expanded)
}

fn sort_temporal_rows(
    rows: &mut [TemporalRow],
    schema: &RowSchema,
    keys: &[temporal_ir::SortKey],
) -> Result<(), RuntimeError> {
    let key_indices = keys
        .iter()
        .map(|key| {
            schema
                .columns()
                .iter()
                .position(|column| column.slot() == key.slot())
                .map(|index| (index, key.ascending()))
                .ok_or(RuntimeError::MissingSlot(key.slot()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    rows.sort_by(|left, right| {
        key_indices
            .iter()
            .map(|(index, ascending)| {
                let ordering = match (&left.values()[*index], &right.values()[*index]) {
                    (RuntimeValue::Null, RuntimeValue::Null) => std::cmp::Ordering::Equal,
                    (RuntimeValue::Null, _) => std::cmp::Ordering::Greater,
                    (_, RuntimeValue::Null) => std::cmp::Ordering::Less,
                    (left, right) => compare_values(left, right),
                };
                if *ascending {
                    ordering
                } else {
                    ordering.reverse()
                }
            })
            .find(|ordering| !ordering.is_eq())
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    Ok(())
}

fn aggregate_interval_rows(
    rows: Vec<TemporalRow>,
    input: &RowSchema,
    grouping: &[temporal_ir::SlotId],
    aggregates: &[(temporal_ir::SlotId, ScalarExpr)],
    output: &RowSchema,
    context: &ExecutionContext,
) -> Result<Vec<TemporalRow>, RuntimeError> {
    let grouping_indices = grouping
        .iter()
        .map(|slot| {
            input
                .columns()
                .iter()
                .position(|column| column.slot() == *slot)
                .ok_or(RuntimeError::MissingSlot(*slot))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let valid_cells = temporal_cells(rows.iter().map(|row| row.region().valid()));
    let transaction_cells = temporal_cells(rows.iter().map(|row| row.region().transaction()));
    let mut result = Vec::new();
    for valid in valid_cells {
        for transaction in &transaction_cells {
            let active = rows
                .iter()
                .filter(|row| {
                    interval_contains(row.region().valid(), valid.start())
                        && interval_contains(row.region().transaction(), transaction.start())
                })
                .collect::<Vec<_>>();
            if active.is_empty() {
                continue;
            }
            let mut groups: Vec<(Vec<RuntimeValue>, Vec<&TemporalRow>)> = Vec::new();
            for row in active {
                let key = grouping_indices
                    .iter()
                    .map(|index| row.values()[*index].clone())
                    .collect::<Vec<_>>();
                if let Some((_, members)) =
                    groups.iter_mut().find(|(candidate, _)| *candidate == key)
                {
                    members.push(row);
                } else {
                    groups.push((key, vec![row]));
                }
            }
            for (group, members) in groups {
                let member_values = members
                    .iter()
                    .map(|row| row.values().to_vec())
                    .collect::<Vec<_>>();
                let mut values = grouping
                    .iter()
                    .copied()
                    .zip(group)
                    .collect::<BTreeMap<_, _>>();
                for (slot, expression) in aggregates {
                    values.insert(
                        *slot,
                        aggregate_expression(expression, input, &member_values, context)?,
                    );
                }
                let values = output
                    .columns()
                    .iter()
                    .map(|column| {
                        values
                            .get(&column.slot())
                            .cloned()
                            .ok_or(RuntimeError::MissingSlot(column.slot()))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let provenance = members
                    .iter()
                    .flat_map(|row| row.provenance().iter().cloned())
                    .collect();
                result.push(TemporalRow::with_provenance(
                    values,
                    TemporalRegion::new(valid, *transaction),
                    provenance,
                ));
            }
        }
    }
    Ok(result)
}

fn temporal_cells<T>(
    regions: impl Iterator<Item = temporal_types::Interval<T>>,
) -> Vec<temporal_types::Interval<T>>
where
    T: Copy + Ord,
{
    let mut points = BTreeSet::new();
    let mut open_ended = false;
    for region in regions {
        points.insert(region.start());
        if let Some(end) = region.end() {
            points.insert(end);
        } else {
            open_ended = true;
        }
    }
    let points = points.into_iter().collect::<Vec<_>>();
    points
        .iter()
        .enumerate()
        .filter_map(|(index, start)| {
            let end = points.get(index + 1).copied();
            if end.is_none() && !open_ended {
                return None;
            }
            temporal_types::Interval::new(*start, end).ok()
        })
        .collect()
}

fn interval_contains<T>(interval: temporal_types::Interval<T>, point: T) -> bool
where
    T: Copy + Ord,
{
    interval.start() <= point && interval.end().is_none_or(|end| point < end)
}

pub(super) fn batches_from_rows(
    schema: &temporal_ir::RowSchema,
    rows: Vec<Vec<RuntimeValue>>,
) -> Result<Vec<RecordBatch>, RuntimeError> {
    if rows.is_empty() {
        return Ok(vec![RecordBatch::try_new(schema.clone(), Vec::new())?]);
    }
    let batch_count = rows.len().saturating_add(MAX_BATCH_ROWS - 1) / MAX_BATCH_ROWS;
    let mut rows = rows.into_iter();
    let mut batches = Vec::with_capacity(batch_count);
    loop {
        let chunk = rows.by_ref().take(MAX_BATCH_ROWS).collect::<Vec<_>>();
        if chunk.is_empty() {
            break;
        }
        batches.push(RecordBatch::try_new(schema.clone(), chunk)?);
    }
    Ok(batches)
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

#[cfg(test)]
mod apply_cell_tests {
    use super::*;

    fn valid(start: i64, end: i64) -> Interval<ValidTime> {
        Interval::new(
            ValidTime::from_micros(start),
            Some(ValidTime::from_micros(end)),
        )
        .unwrap()
    }

    fn transaction(start: i64, end: i64) -> Interval<TransactionTime> {
        Interval::new(
            TransactionTime::new(start, 0),
            Some(TransactionTime::new(end, 0)),
        )
        .unwrap()
    }

    #[test]
    fn interval_exists_and_count_split_parent_region_at_child_visibility_cells() {
        let parent = TemporalRow::with_provenance(
            vec![RuntimeValue::Integer(7)],
            TemporalRegion::new(valid(0, 10), transaction(10, 20)),
            vec![TemporalProvenance::Unwind(0)],
        );
        let children = vec![
            TemporalRow::with_provenance(
                Vec::new(),
                TemporalRegion::new(valid(2, 8), transaction(10, 20)),
                vec![TemporalProvenance::Unwind(1)],
            ),
            TemporalRow::with_provenance(
                Vec::new(),
                TemporalRegion::new(valid(5, 9), transaction(10, 20)),
                vec![TemporalProvenance::Unwind(2)],
            ),
        ];

        let exists = scalar_apply_temporal_cells(
            &parent,
            &children,
            ApplyKind::Exists {
                output: temporal_ir::SlotId::new(1),
            },
        )
        .unwrap();
        assert_eq!(
            exists
                .iter()
                .map(|row| (row.region().valid(), row.values()[1].clone()))
                .collect::<Vec<_>>(),
            vec![
                (valid(0, 2), RuntimeValue::Boolean(false)),
                (valid(2, 5), RuntimeValue::Boolean(true)),
                (valid(5, 8), RuntimeValue::Boolean(true)),
                (valid(8, 9), RuntimeValue::Boolean(true)),
                (valid(9, 10), RuntimeValue::Boolean(false)),
            ]
        );
        let count = scalar_apply_temporal_cells(
            &parent,
            &children,
            ApplyKind::Count {
                output: temporal_ir::SlotId::new(1),
            },
        )
        .unwrap();
        assert_eq!(
            count
                .iter()
                .map(|row| (row.region().valid(), row.values()[1].clone()))
                .collect::<Vec<_>>(),
            vec![
                (valid(0, 2), RuntimeValue::Integer(0)),
                (valid(2, 5), RuntimeValue::Integer(1)),
                (valid(5, 8), RuntimeValue::Integer(2)),
                (valid(8, 9), RuntimeValue::Integer(1)),
                (valid(9, 10), RuntimeValue::Integer(0)),
            ]
        );
    }
}
