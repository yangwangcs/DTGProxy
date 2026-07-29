use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use dtg_language_ir::{Field, LogicalType, RowSchema, SortDirection};
use dtg_storage::{
    AdjacencyDirection, AdjacencyRead, LogicalMutation, TemporalReadView, TransactionTime, VertexId,
};

use crate::{ColumnBatch, Expression, QueryContext, QueryError, QueryValue};

pub type QueryFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, QueryError>> + Send + 'a>>;

pub trait Operator: Send {
    fn schema(&self) -> &RowSchema;
    fn next_batch<'a>(
        &'a mut self,
        context: &'a mut QueryContext,
    ) -> QueryFuture<'a, Option<ColumnBatch>>;
}

pub struct BatchOperator {
    schema: RowSchema,
    batches: VecDeque<ColumnBatch>,
}

impl BatchOperator {
    pub fn new(batches: Vec<ColumnBatch>) -> Self {
        let schema = batches
            .first()
            .map_or_else(RowSchema::empty, |batch| batch.schema().clone());
        Self {
            schema,
            batches: batches.into(),
        }
    }
}

impl Operator for BatchOperator {
    fn schema(&self) -> &RowSchema {
        &self.schema
    }

    fn next_batch<'a>(
        &'a mut self,
        context: &'a mut QueryContext,
    ) -> QueryFuture<'a, Option<ColumnBatch>> {
        Box::pin(async move {
            context.checkpoint()?;
            Ok(self.batches.pop_front())
        })
    }
}

pub struct FilterOperator {
    input: Box<dyn Operator>,
    predicate: Expression,
    schema: RowSchema,
}

impl FilterOperator {
    pub fn new(input: Box<dyn Operator>, predicate: Expression) -> Self {
        let schema = input.schema().clone();
        Self {
            input,
            predicate,
            schema,
        }
    }
}

impl Operator for FilterOperator {
    fn schema(&self) -> &RowSchema {
        &self.schema
    }

    fn next_batch<'a>(
        &'a mut self,
        context: &'a mut QueryContext,
    ) -> QueryFuture<'a, Option<ColumnBatch>> {
        Box::pin(async move {
            loop {
                context.checkpoint()?;
                let Some(batch) = self.input.next_batch(context).await? else {
                    return Ok(None);
                };
                let mut rows = Vec::new();
                for row in batch.rows() {
                    context.checkpoint()?;
                    if self.predicate.evaluate(&self.schema, &row)? == QueryValue::Boolean(true) {
                        rows.push(row);
                    }
                }
                if !rows.is_empty() {
                    return ColumnBatch::from_rows(self.schema.clone(), rows).map(Some);
                }
            }
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectionExpr {
    field: Field,
    expression: Expression,
}

impl ProjectionExpr {
    pub fn new(
        name: impl Into<String>,
        data_type: LogicalType,
        nullable: bool,
        expression: Expression,
    ) -> Self {
        Self {
            field: Field {
                name: name.into(),
                data_type,
                nullable,
            },
            expression,
        }
    }
}

pub struct ProjectOperator {
    input: Box<dyn Operator>,
    projections: Vec<ProjectionExpr>,
    input_schema: RowSchema,
    schema: RowSchema,
}

impl ProjectOperator {
    pub fn new(input: Box<dyn Operator>, projections: Vec<ProjectionExpr>) -> Self {
        let input_schema = input.schema().clone();
        let schema = RowSchema {
            fields: projections
                .iter()
                .map(|projection| projection.field.clone())
                .collect(),
        };
        Self {
            input,
            projections,
            input_schema,
            schema,
        }
    }
}

impl Operator for ProjectOperator {
    fn schema(&self) -> &RowSchema {
        &self.schema
    }

    fn next_batch<'a>(
        &'a mut self,
        context: &'a mut QueryContext,
    ) -> QueryFuture<'a, Option<ColumnBatch>> {
        Box::pin(async move {
            context.checkpoint()?;
            let Some(batch) = self.input.next_batch(context).await? else {
                return Ok(None);
            };
            let mut rows = Vec::with_capacity(batch.row_count());
            for row in batch.rows() {
                context.checkpoint()?;
                rows.push(
                    self.projections
                        .iter()
                        .map(|projection| projection.expression.evaluate(&self.input_schema, &row))
                        .collect::<Result<Vec<_>, _>>()?,
                );
            }
            ColumnBatch::from_rows(self.schema.clone(), rows).map(Some)
        })
    }
}

pub struct HashJoinOperator {
    left: Box<dyn Operator>,
    right: Box<dyn Operator>,
    left_key: usize,
    right_key: usize,
    schema: RowSchema,
    emitted: bool,
}

impl HashJoinOperator {
    pub fn new(
        left: Box<dyn Operator>,
        right: Box<dyn Operator>,
        left_key: usize,
        right_key: usize,
    ) -> Self {
        let mut fields = left.schema().fields.clone();
        fields.extend(right.schema().fields.clone());
        Self {
            left,
            right,
            left_key,
            right_key,
            schema: RowSchema { fields },
            emitted: false,
        }
    }
}

impl Operator for HashJoinOperator {
    fn schema(&self) -> &RowSchema {
        &self.schema
    }

    fn next_batch<'a>(
        &'a mut self,
        context: &'a mut QueryContext,
    ) -> QueryFuture<'a, Option<ColumnBatch>> {
        Box::pin(async move {
            context.checkpoint()?;
            if self.emitted {
                return Ok(None);
            }
            self.emitted = true;
            let (_, left_rows) = collect_rows(&mut self.left, context).await?;
            let (_, right_rows) = collect_rows(&mut self.right, context).await?;
            let mut table: BTreeMap<ScalarKey, Vec<Vec<QueryValue>>> = BTreeMap::new();
            for row in right_rows {
                context.checkpoint()?;
                let key = scalar_key(row.get(self.right_key))?;
                table.entry(key).or_default().push(row);
            }
            context.charge_memory(estimate_rows(table.values().flatten()))?;
            let mut output = Vec::new();
            for left in left_rows {
                context.checkpoint()?;
                let key = scalar_key(left.get(self.left_key))?;
                if let Some(matches) = table.get(&key) {
                    for right in matches {
                        context.checkpoint()?;
                        let mut row = left.clone();
                        row.extend(right.clone());
                        output.push(row);
                    }
                }
            }
            ColumnBatch::from_rows(self.schema.clone(), output).map(Some)
        })
    }
}

pub struct AggregateOperator {
    input: Box<dyn Operator>,
    group_column: usize,
    schema: RowSchema,
    emitted: bool,
}

impl AggregateOperator {
    pub fn count_by(
        input: Box<dyn Operator>,
        group_column: usize,
        count_name: impl Into<String>,
    ) -> Self {
        let group_field = input
            .schema()
            .fields
            .get(group_column)
            .cloned()
            .unwrap_or(Field {
                name: "group".into(),
                data_type: LogicalType::Any,
                nullable: true,
            });
        Self {
            input,
            group_column,
            schema: RowSchema {
                fields: vec![
                    group_field,
                    Field {
                        name: count_name.into(),
                        data_type: LogicalType::Integer,
                        nullable: false,
                    },
                ],
            },
            emitted: false,
        }
    }
}

impl Operator for AggregateOperator {
    fn schema(&self) -> &RowSchema {
        &self.schema
    }

    fn next_batch<'a>(
        &'a mut self,
        context: &'a mut QueryContext,
    ) -> QueryFuture<'a, Option<ColumnBatch>> {
        Box::pin(async move {
            context.checkpoint()?;
            if self.emitted {
                return Ok(None);
            }
            self.emitted = true;
            let (_, rows) = collect_rows(&mut self.input, context).await?;
            let mut groups: BTreeMap<ScalarKey, (QueryValue, u64)> = BTreeMap::new();
            for row in rows {
                context.checkpoint()?;
                let value = row.get(self.group_column).cloned().ok_or_else(|| {
                    QueryError::InvalidPlan("aggregate group column is absent".into())
                })?;
                let key = scalar_key(Some(&value))?;
                let entry = groups.entry(key).or_insert((value, 0));
                entry.1 = entry.1.checked_add(1).ok_or(QueryError::RowBudget)?;
            }
            context.charge_memory((groups.len() as u64).saturating_mul(32))?;
            let rows = groups
                .into_values()
                .map(|(group, count)| {
                    i64::try_from(count)
                        .map(|count| vec![group, QueryValue::Integer(count)])
                        .map_err(|_| QueryError::RowBudget)
                })
                .collect::<Result<Vec<_>, _>>()?;
            ColumnBatch::from_rows(self.schema.clone(), rows).map(Some)
        })
    }
}

pub struct SortOperator {
    input: Box<dyn Operator>,
    column: usize,
    direction: SortDirection,
    schema: RowSchema,
    emitted: bool,
}

impl SortOperator {
    pub fn new(input: Box<dyn Operator>, column: usize, direction: SortDirection) -> Self {
        let schema = input.schema().clone();
        Self {
            input,
            column,
            direction,
            schema,
            emitted: false,
        }
    }
}

impl Operator for SortOperator {
    fn schema(&self) -> &RowSchema {
        &self.schema
    }

    fn next_batch<'a>(
        &'a mut self,
        context: &'a mut QueryContext,
    ) -> QueryFuture<'a, Option<ColumnBatch>> {
        Box::pin(async move {
            context.checkpoint()?;
            if self.emitted {
                return Ok(None);
            }
            self.emitted = true;
            let (_, mut rows) = collect_rows(&mut self.input, context).await?;
            context.charge_memory(estimate_rows(rows.iter()))?;
            rows.sort_by(|left, right| {
                let ordering = compare_row_column(left, right, self.column);
                match self.direction {
                    SortDirection::Ascending => ordering,
                    SortDirection::Descending => ordering.reverse(),
                }
                .then_with(|| compare_rows(left, right))
            });
            ColumnBatch::from_rows(self.schema.clone(), rows).map(Some)
        })
    }
}

pub struct LimitOperator {
    input: Box<dyn Operator>,
    skip: usize,
    limit: usize,
    schema: RowSchema,
    emitted: usize,
    skipped: usize,
    done: bool,
}

impl LimitOperator {
    pub fn new(input: Box<dyn Operator>, skip: usize, limit: usize) -> Self {
        let schema = input.schema().clone();
        Self {
            input,
            skip,
            limit,
            schema,
            emitted: 0,
            skipped: 0,
            done: false,
        }
    }
}

impl Operator for LimitOperator {
    fn schema(&self) -> &RowSchema {
        &self.schema
    }

    fn next_batch<'a>(
        &'a mut self,
        context: &'a mut QueryContext,
    ) -> QueryFuture<'a, Option<ColumnBatch>> {
        Box::pin(async move {
            while !self.done {
                context.checkpoint()?;
                if self.emitted == self.limit {
                    self.done = true;
                    return Ok(None);
                }
                let Some(batch) = self.input.next_batch(context).await? else {
                    self.done = true;
                    return Ok(None);
                };
                let mut output = Vec::new();
                for row in batch.rows() {
                    context.checkpoint()?;
                    if self.skipped < self.skip {
                        self.skipped += 1;
                        continue;
                    }
                    if self.emitted == self.limit {
                        self.done = true;
                        break;
                    }
                    self.emitted += 1;
                    output.push(row);
                }
                if !output.is_empty() {
                    return ColumnBatch::from_rows(self.schema.clone(), output).map(Some);
                }
            }
            Ok(None)
        })
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct QueryOverlay {
    mutations: Vec<LogicalMutation>,
}

impl QueryOverlay {
    pub fn from_mutations(mutations: Vec<LogicalMutation>) -> Result<Self, QueryError> {
        if mutations.iter().any(|mutation| {
            matches!(
                mutation,
                LogicalMutation::PutTransaction(_) | LogicalMutation::PutReplicaMetadata(_)
            )
        }) {
            return Err(QueryError::InvalidPlan(
                "query overlay accepts only graph mutations".into(),
            ));
        }
        Ok(Self { mutations })
    }

    pub fn mutations(&self) -> &[LogicalMutation] {
        &self.mutations
    }
}

pub struct OverlayOperator {
    input: Box<dyn Operator>,
    overlay: QueryOverlay,
    valid_at: i64,
    schema: RowSchema,
    emitted: bool,
}

impl OverlayOperator {
    pub fn new(input: Box<dyn Operator>, overlay: QueryOverlay, valid_at: i64) -> Self {
        let schema = input.schema().clone();
        Self {
            input,
            overlay,
            valid_at,
            schema,
            emitted: false,
        }
    }
}

impl Operator for OverlayOperator {
    fn schema(&self) -> &RowSchema {
        &self.schema
    }

    fn next_batch<'a>(
        &'a mut self,
        context: &'a mut QueryContext,
    ) -> QueryFuture<'a, Option<ColumnBatch>> {
        Box::pin(async move {
            context.checkpoint()?;
            if self.emitted {
                return Ok(None);
            }
            self.emitted = true;
            let (_, rows) = collect_rows(&mut self.input, context).await?;
            if self.schema.fields.len() != 1
                || self.schema.fields[0].data_type != LogicalType::Vertex
            {
                return Err(QueryError::Unsupported(
                    "overlay currently requires a single vertex column".into(),
                ));
            }
            let mut vertices = BTreeMap::new();
            for row in rows {
                context.checkpoint()?;
                if let Some(QueryValue::Vertex(vertex)) = row.first() {
                    vertices.insert(vertex.id(), vertex.clone());
                }
            }
            for mutation in self.overlay.mutations() {
                context.checkpoint()?;
                match mutation {
                    LogicalMutation::PutVertex(vertex)
                        if vertex.valid_time().start() <= self.valid_at
                            && self.valid_at < vertex.valid_time().end() =>
                    {
                        vertices.insert(vertex.id(), vertex.clone());
                    }
                    LogicalMutation::DeleteVertex(tombstone) => {
                        vertices.remove(&tombstone.id());
                    }
                    LogicalMutation::PutVertex(_)
                    | LogicalMutation::PutEdge(_)
                    | LogicalMutation::DeleteEdge(_)
                    | LogicalMutation::PutTransaction(_)
                    | LogicalMutation::PutReplicaMetadata(_) => {}
                }
            }
            let rows = vertices
                .into_values()
                .map(|vertex| vec![QueryValue::Vertex(vertex)])
                .collect();
            ColumnBatch::from_rows(self.schema.clone(), rows).map(Some)
        })
    }
}

pub struct ExpandOperator {
    input: Box<dyn Operator>,
    vertex_column: usize,
    view: Arc<dyn TemporalReadView>,
    direction: AdjacencyDirection,
    valid_at: i64,
    transaction_time: TransactionTime,
    limit: u32,
    schema: RowSchema,
    emitted: bool,
}

impl ExpandOperator {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        input: Box<dyn Operator>,
        vertex_column: usize,
        view: Arc<dyn TemporalReadView>,
        direction: AdjacencyDirection,
        valid_at: i64,
        transaction_time: TransactionTime,
        limit: u32,
    ) -> Result<Self, QueryError> {
        if limit == 0 || vertex_column >= input.schema().fields.len() {
            return Err(QueryError::InvalidPlan(
                "expand requires a valid vertex column and nonzero bound".into(),
            ));
        }
        Ok(Self {
            input,
            vertex_column,
            view,
            direction,
            valid_at,
            transaction_time,
            limit,
            schema: RowSchema {
                fields: vec![Field {
                    name: "relationship".into(),
                    data_type: LogicalType::Relationship,
                    nullable: false,
                }],
            },
            emitted: false,
        })
    }
}

impl Operator for ExpandOperator {
    fn schema(&self) -> &RowSchema {
        &self.schema
    }

    fn next_batch<'a>(
        &'a mut self,
        context: &'a mut QueryContext,
    ) -> QueryFuture<'a, Option<ColumnBatch>> {
        Box::pin(async move {
            context.checkpoint()?;
            if self.emitted {
                return Ok(None);
            }
            self.emitted = true;
            let (_, rows) = collect_rows(&mut self.input, context).await?;
            let mut output = Vec::new();
            let mut remaining = self.limit;
            for row in rows {
                context.checkpoint()?;
                if remaining == 0 {
                    break;
                }
                let Some(QueryValue::Vertex(vertex)) = row.get(self.vertex_column) else {
                    return Err(QueryError::InvalidPlan(
                        "expand input column is not a vertex".into(),
                    ));
                };
                let edges = self
                    .view
                    .expand(AdjacencyRead::new(
                        vertex.id(),
                        self.direction,
                        self.valid_at,
                        self.transaction_time,
                        remaining,
                    )?)
                    .await?;
                if edges.len() > remaining as usize {
                    return Err(QueryError::ProviderViolation(
                        "adjacency result exceeded the requested bound".into(),
                    ));
                }
                for edge in edges {
                    context.checkpoint()?;
                    context.charge_scan_bytes(
                        QueryValue::Relationship(edge.clone()).estimated_bytes(),
                    )?;
                    output.push(vec![QueryValue::Relationship(edge)]);
                    remaining -= 1;
                }
            }
            ColumnBatch::from_rows(self.schema.clone(), output).map(Some)
        })
    }
}

pub(crate) async fn collect_rows(
    input: &mut Box<dyn Operator>,
    context: &mut QueryContext,
) -> Result<(RowSchema, Vec<Vec<QueryValue>>), QueryError> {
    let schema = input.schema().clone();
    let mut rows = Vec::new();
    loop {
        context.checkpoint()?;
        let Some(batch) = input.next_batch(context).await? else {
            break;
        };
        rows.extend(batch.rows());
    }
    Ok((schema, rows))
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum ScalarKey {
    Null,
    Boolean(bool),
    Integer(i64),
    FloatBits(u64),
    Bytes(Vec<u8>),
    String(String),
    Vertex(VertexId),
}

fn scalar_key(value: Option<&QueryValue>) -> Result<ScalarKey, QueryError> {
    match value {
        Some(QueryValue::Null) => Ok(ScalarKey::Null),
        Some(QueryValue::Boolean(value)) => Ok(ScalarKey::Boolean(*value)),
        Some(QueryValue::Integer(value)) => Ok(ScalarKey::Integer(*value)),
        Some(QueryValue::FloatBits(value)) => Ok(ScalarKey::FloatBits(*value)),
        Some(QueryValue::Bytes(value)) => Ok(ScalarKey::Bytes(value.clone())),
        Some(QueryValue::String(value)) => Ok(ScalarKey::String(value.clone())),
        Some(QueryValue::Vertex(vertex)) => Ok(ScalarKey::Vertex(vertex.id())),
        Some(QueryValue::List(_) | QueryValue::Map(_) | QueryValue::Relationship(_)) => Err(
            QueryError::Unsupported("join/aggregate key is not scalar".into()),
        ),
        None => Err(QueryError::InvalidPlan("key column is absent".into())),
    }
}

fn compare_row_column(
    left: &[QueryValue],
    right: &[QueryValue],
    column: usize,
) -> std::cmp::Ordering {
    match (left.get(column), right.get(column)) {
        (Some(left), Some(right)) => left.total_cmp(right),
        (Some(_), None) => std::cmp::Ordering::Greater,
        (None, Some(_)) => std::cmp::Ordering::Less,
        (None, None) => std::cmp::Ordering::Equal,
    }
}

pub(crate) fn compare_rows(left: &[QueryValue], right: &[QueryValue]) -> std::cmp::Ordering {
    left.iter()
        .zip(right)
        .find_map(|(left, right)| {
            let ordering = left.total_cmp(right);
            (ordering != std::cmp::Ordering::Equal).then_some(ordering)
        })
        .unwrap_or_else(|| left.len().cmp(&right.len()))
}

fn estimate_rows<'a>(rows: impl IntoIterator<Item = &'a Vec<QueryValue>>) -> u64 {
    rows.into_iter()
        .flatten()
        .map(QueryValue::estimated_bytes)
        .sum()
}
