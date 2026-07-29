use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use dtg_language_ir::{ExpandDirection, Field, LogicalType, RowSchema};
use dtg_storage::{
    AdjacencyDirection, AdjacencyRead, BackendGeneration, Digest32, EdgeRead, EdgeScan,
    PlacementEpoch, PushdownExecutor, PushdownOperation, PushdownOutcome, PushdownRequest,
    ReadFence, ShardId, SnapshotRecord, TemporalReadView, TransactionTime, Version, VertexId,
    VertexRead, VertexScan,
};

use crate::{ColumnBatch, Expression, Operator, QueryContext, QueryError, QueryFuture, QueryValue};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionFence {
    read_fence: ReadFence,
    catalog_version: Version,
    schema_version: Version,
    transaction_time: TransactionTime,
    valid_at: i64,
    immutable: bool,
}

impl ExecutionFence {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        read_fence: ReadFence,
        catalog_version: Version,
        schema_version: Version,
        transaction_time: TransactionTime,
        valid_at: i64,
        immutable: bool,
    ) -> Result<Self, QueryError> {
        if catalog_version.get() == 0
            || schema_version.get() == 0
            || !immutable
            || read_fence.capability_digest() != read_fence.binding().capability_digest()
        {
            return Err(QueryError::InvalidPlan(
                "execution fence is incomplete, mutable, or capability-drifted".into(),
            ));
        }
        Ok(Self {
            read_fence,
            catalog_version,
            schema_version,
            transaction_time,
            valid_at,
            immutable,
        })
    }

    pub const fn read_fence(&self) -> &ReadFence {
        &self.read_fence
    }

    pub const fn shard_id(&self) -> ShardId {
        self.read_fence.binding().shard_id()
    }

    pub const fn placement_epoch(&self) -> PlacementEpoch {
        self.read_fence.binding().placement_epoch()
    }

    pub const fn backend_generation(&self) -> BackendGeneration {
        self.read_fence.binding().backend_generation()
    }

    pub const fn capability_digest(&self) -> Digest32 {
        self.read_fence.capability_digest()
    }

    pub const fn applied_index(&self) -> u64 {
        self.read_fence.applied_index()
    }

    pub const fn catalog_version(&self) -> Version {
        self.catalog_version
    }

    pub const fn schema_version(&self) -> Version {
        self.schema_version
    }

    pub const fn transaction_time(&self) -> TransactionTime {
        self.transaction_time
    }

    pub const fn valid_at(&self) -> i64 {
        self.valid_at
    }

    pub const fn immutable(&self) -> bool {
        self.immutable
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadOperation {
    VertexPoint(VertexId),
    VertexScan,
    EdgePoint(dtg_storage::EdgeId),
    EdgeScan,
    Adjacency {
        vertex_id: VertexId,
        direction: ExpandDirection,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicalRead {
    operation: ReadOperation,
    row_bound: u32,
    transaction_time: TransactionTime,
    valid_at: i64,
}

impl LogicalRead {
    pub fn new(
        operation: ReadOperation,
        row_bound: u32,
        transaction_time: TransactionTime,
        valid_at: i64,
    ) -> Result<Self, QueryError> {
        if row_bound == 0 {
            return Err(QueryError::InvalidPlan(
                "logical storage access must have a nonzero row bound".into(),
            ));
        }
        Ok(Self {
            operation,
            row_bound,
            transaction_time,
            valid_at,
        })
    }

    pub const fn operation(&self) -> ReadOperation {
        self.operation
    }

    pub const fn row_bound(&self) -> u32 {
        self.row_bound
    }

    pub const fn transaction_time(&self) -> TransactionTime {
        self.transaction_time
    }

    pub const fn valid_at(&self) -> i64 {
        self.valid_at
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ResidualPredicate {
    StorageSemantics,
    Expression(Expression),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExecutableAccess {
    Logical(LogicalRead),
    Pushdown {
        request: Box<PushdownRequest>,
        residual: Option<ResidualPredicate>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutableFragment {
    id: u32,
    fence: ExecutionFence,
    accesses: Vec<ExecutableAccess>,
}

impl ExecutableFragment {
    pub fn new(
        id: u32,
        fence: ExecutionFence,
        accesses: Vec<ExecutableAccess>,
    ) -> Result<Self, QueryError> {
        if id == 0 || accesses.is_empty() {
            return Err(QueryError::InvalidPlan(
                "executable fragment identity and access set must be nonempty".into(),
            ));
        }
        Ok(Self {
            id,
            fence,
            accesses,
        })
    }

    pub const fn id(&self) -> u32 {
        self.id
    }

    pub const fn fence(&self) -> &ExecutionFence {
        &self.fence
    }

    pub fn accesses(&self) -> &[ExecutableAccess] {
        &self.accesses
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutablePlan {
    version: Version,
    fragments: Vec<ExecutableFragment>,
    result_schema: RowSchema,
}

impl ExecutablePlan {
    pub fn new(
        version: Version,
        mut fragments: Vec<ExecutableFragment>,
        result_schema: RowSchema,
    ) -> Result<Self, QueryError> {
        if version.get() == 0 || fragments.is_empty() {
            return Err(QueryError::InvalidPlan(
                "executable plan version and fragment set must be nonempty".into(),
            ));
        }
        let mut ids = BTreeSet::new();
        let mut shards = BTreeSet::new();
        for fragment in &fragments {
            if !ids.insert(fragment.id()) || !shards.insert(fragment.fence().shard_id()) {
                return Err(QueryError::InvalidPlan(
                    "executable fragment or Shard identity is duplicated".into(),
                ));
            }
        }
        fragments.sort_unstable_by_key(|fragment| fragment.fence().shard_id());
        Ok(Self {
            version,
            fragments,
            result_schema,
        })
    }

    pub const fn version(&self) -> Version {
        self.version
    }

    pub fn fragments(&self) -> &[ExecutableFragment] {
        &self.fragments
    }

    pub const fn result_schema(&self) -> &RowSchema {
        &self.result_schema
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SnapshotShardFence {
    pub placement_epoch: PlacementEpoch,
    pub backend_generation: BackendGeneration,
    pub applied_index: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotGuard {
    transaction_time: TransactionTime,
    catalog_version: Version,
    shards: BTreeMap<ShardId, SnapshotShardFence>,
}

impl SnapshotGuard {
    pub fn new(
        transaction_time: TransactionTime,
        catalog_version: Version,
        shard_fences: Vec<(ShardId, SnapshotShardFence)>,
    ) -> Result<Self, QueryError> {
        if catalog_version.get() == 0 || shard_fences.is_empty() {
            return Err(QueryError::SnapshotDrift);
        }
        let mut shards = BTreeMap::new();
        for (shard_id, fence) in shard_fences {
            if shards.insert(shard_id, fence).is_some() {
                return Err(QueryError::SnapshotDrift);
            }
        }
        Ok(Self {
            transaction_time,
            catalog_version,
            shards,
        })
    }

    pub fn validate(&self, fence: &ExecutionFence) -> Result<(), QueryError> {
        let snapshot = self
            .shards
            .get(&fence.shard_id())
            .ok_or(QueryError::SnapshotDrift)?;
        if self.transaction_time != fence.transaction_time()
            || self.catalog_version != fence.catalog_version()
            || snapshot.placement_epoch != fence.placement_epoch()
            || snapshot.backend_generation != fence.backend_generation()
            || snapshot.applied_index != fence.applied_index()
        {
            return Err(QueryError::SnapshotDrift);
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct QueryStorage {
    view: Arc<dyn TemporalReadView>,
    pushdown: Option<Arc<dyn PushdownExecutor>>,
}

impl QueryStorage {
    pub const fn new(view: Arc<dyn TemporalReadView>) -> Self {
        Self {
            view,
            pushdown: None,
        }
    }

    pub fn with_pushdown(mut self, pushdown: Arc<dyn PushdownExecutor>) -> Self {
        self.pushdown = Some(pushdown);
        self
    }

    pub const fn view(&self) -> &Arc<dyn TemporalReadView> {
        &self.view
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Cursor {
    Start,
    Vertex(VertexId),
    Edge(dtg_storage::EdgeId),
    Done,
}

pub(crate) struct StorageSourceOperator {
    access: ExecutableAccess,
    fence: ExecutionFence,
    storage: QueryStorage,
    batch_size: u32,
    schema: RowSchema,
    cursor: Cursor,
    remaining: u32,
}

impl StorageSourceOperator {
    pub fn new(
        access: ExecutableAccess,
        fence: ExecutionFence,
        storage: QueryStorage,
        batch_size: u32,
    ) -> Result<Self, QueryError> {
        if batch_size == 0 {
            return Err(QueryError::InvalidPlan(
                "query batch size must be nonzero".into(),
            ));
        }
        validate_view(&fence, storage.view().fence())?;
        let (schema, remaining) = access_shape(&access)?;
        Ok(Self {
            access,
            fence,
            storage,
            batch_size,
            schema,
            cursor: Cursor::Start,
            remaining,
        })
    }

    async fn execute_logical(
        &mut self,
        logical: &LogicalRead,
        context: &mut QueryContext,
    ) -> Result<Option<ColumnBatch>, QueryError> {
        let limit = self.next_limit(context)?;
        let rows: Vec<QueryValue> = match logical.operation() {
            ReadOperation::VertexPoint(id) => {
                self.cursor = Cursor::Done;
                self.storage
                    .view
                    .get_vertex(VertexRead::new(
                        id,
                        logical.valid_at(),
                        logical.transaction_time(),
                    ))
                    .await?
                    .into_iter()
                    .map(QueryValue::Vertex)
                    .collect()
            }
            ReadOperation::VertexScan => {
                let after = match self.cursor {
                    Cursor::Start => None,
                    Cursor::Vertex(after) => Some(after),
                    Cursor::Done => return Ok(None),
                    Cursor::Edge(_) => {
                        return Err(QueryError::InvalidPlan("invalid scan cursor".into()));
                    }
                };
                let page = self
                    .storage
                    .view
                    .scan_vertices(VertexScan::new(
                        logical.valid_at(),
                        logical.transaction_time(),
                        after,
                        limit,
                    )?)
                    .await?;
                if page.rows().len() > limit as usize {
                    return Err(QueryError::ProviderViolation(
                        "vertex scan exceeded requested page bound".into(),
                    ));
                }
                self.cursor = page.next_after().map_or(Cursor::Done, Cursor::Vertex);
                page.rows()
                    .iter()
                    .cloned()
                    .map(QueryValue::Vertex)
                    .collect()
            }
            ReadOperation::EdgePoint(id) => {
                self.cursor = Cursor::Done;
                self.storage
                    .view
                    .get_edge(EdgeRead::new(
                        id,
                        logical.valid_at(),
                        logical.transaction_time(),
                    ))
                    .await?
                    .into_iter()
                    .map(QueryValue::Relationship)
                    .collect()
            }
            ReadOperation::EdgeScan => {
                let after = match self.cursor {
                    Cursor::Start => None,
                    Cursor::Edge(after) => Some(after),
                    Cursor::Done => return Ok(None),
                    Cursor::Vertex(_) => {
                        return Err(QueryError::InvalidPlan("invalid scan cursor".into()));
                    }
                };
                let page = self
                    .storage
                    .view
                    .scan_edges(EdgeScan::new(
                        logical.valid_at(),
                        logical.transaction_time(),
                        after,
                        limit,
                    )?)
                    .await?;
                if page.rows().len() > limit as usize {
                    return Err(QueryError::ProviderViolation(
                        "edge scan exceeded requested page bound".into(),
                    ));
                }
                self.cursor = page.next_after().map_or(Cursor::Done, Cursor::Edge);
                page.rows()
                    .iter()
                    .cloned()
                    .map(QueryValue::Relationship)
                    .collect()
            }
            ReadOperation::Adjacency {
                vertex_id,
                direction,
            } => {
                self.cursor = Cursor::Done;
                let direction = match direction {
                    ExpandDirection::Outgoing => AdjacencyDirection::Outgoing,
                    ExpandDirection::Incoming => AdjacencyDirection::Incoming,
                    ExpandDirection::Either => AdjacencyDirection::Both,
                };
                self.storage
                    .view
                    .expand(AdjacencyRead::new(
                        vertex_id,
                        direction,
                        logical.valid_at(),
                        logical.transaction_time(),
                        limit,
                    )?)
                    .await?
                    .into_iter()
                    .map(QueryValue::Relationship)
                    .collect()
            }
        };
        for value in &rows {
            context.checkpoint()?;
            context.charge_scan_bytes(value.estimated_bytes())?;
        }
        self.finish_rows(rows)
    }

    async fn execute_pushdown(
        &mut self,
        request: &PushdownRequest,
        residual: Option<ResidualPredicate>,
        context: &mut QueryContext,
    ) -> Result<Option<ColumnBatch>, QueryError> {
        let limit = self.next_limit(context)?;
        let bounded = bounded_pushdown(request, self.cursor, limit)?;
        let Some(executor) = &self.storage.pushdown else {
            return self.execute_pushdown_fallback(&bounded, context).await;
        };
        if executor.binding().identity_digest()
            != self.fence.read_fence().binding().identity_digest()
            || executor.capabilities().digest() != self.fence.capability_digest()
        {
            return Err(QueryError::CapabilityDrift);
        }
        let outcome = executor.execute_pushdown(bounded.clone()).await?;
        let (records, provider_requires_residual) = match outcome {
            PushdownOutcome::Exact(rows) => (rows, false),
            PushdownOutcome::ResidualRequired { rows, .. } => (rows, true),
            PushdownOutcome::Unsupported => {
                return self.execute_pushdown_fallback(&bounded, context).await;
            }
        };
        let requires_residual = residual.is_some() || provider_requires_residual;
        if !requires_residual && records.len() > limit as usize {
            return Err(QueryError::ProviderViolation(
                "pushdown candidate set exceeded requested bound".into(),
            ));
        }
        let raw_len = records.len();
        let previous = self.cursor;
        let storage_residual = provider_requires_residual
            || matches!(residual, Some(ResidualPredicate::StorageSemantics));
        let expression_residual = match residual {
            Some(ResidualPredicate::Expression(expression)) => Some(expression),
            Some(ResidualPredicate::StorageSemantics) | None => None,
        };
        let mut values = if storage_residual {
            apply_storage_residual(&bounded, records, context)?
        } else {
            for record in &records {
                context.checkpoint()?;
                context.charge_scan_bytes(snapshot_record_bytes(record))?;
            }
            exact_records(records)?
        };
        if let Some(expression) = expression_residual {
            values = apply_expression_residual(values, &self.schema, &expression, context)?;
        }
        advance_pushdown_cursor(
            &bounded,
            &values,
            raw_len,
            limit,
            previous,
            &mut self.cursor,
        )?;
        values.truncate(self.remaining as usize);
        self.finish_rows(values)
    }

    async fn execute_pushdown_fallback(
        &mut self,
        request: &PushdownRequest,
        context: &mut QueryContext,
    ) -> Result<Option<ColumnBatch>, QueryError> {
        let logical = logical_from_pushdown(request)?;
        self.execute_logical(&logical, context).await
    }

    fn next_limit(&self, context: &QueryContext) -> Result<u32, QueryError> {
        if self.cursor == Cursor::Done || self.remaining == 0 {
            return Ok(0);
        }
        let budget = context.remaining_rows().min(u64::from(u32::MAX)) as u32;
        let limit = self.batch_size.min(self.remaining).min(budget);
        if limit == 0 {
            Err(QueryError::RowBudget)
        } else {
            Ok(limit)
        }
    }

    fn finish_rows(&mut self, rows: Vec<QueryValue>) -> Result<Option<ColumnBatch>, QueryError> {
        if rows.len() > self.remaining as usize {
            return Err(QueryError::ProviderViolation(
                "storage result exceeded executable row bound".into(),
            ));
        }
        self.remaining -= rows.len() as u32;
        if self.remaining == 0 {
            self.cursor = Cursor::Done;
        }
        if rows.is_empty() {
            if self.cursor == Cursor::Done {
                Ok(None)
            } else {
                Err(QueryError::ProviderViolation(
                    "storage cursor advanced without a candidate row".into(),
                ))
            }
        } else {
            ColumnBatch::from_rows(
                self.schema.clone(),
                rows.into_iter().map(|value| vec![value]).collect(),
            )
            .map(Some)
        }
    }
}

impl Operator for StorageSourceOperator {
    fn schema(&self) -> &RowSchema {
        &self.schema
    }

    fn next_batch<'a>(
        &'a mut self,
        context: &'a mut QueryContext,
    ) -> QueryFuture<'a, Option<ColumnBatch>> {
        Box::pin(async move {
            context.checkpoint()?;
            if self.cursor == Cursor::Done || self.remaining == 0 {
                return Ok(None);
            }
            match self.access.clone() {
                ExecutableAccess::Logical(logical) => self.execute_logical(&logical, context).await,
                ExecutableAccess::Pushdown { request, residual } => {
                    self.execute_pushdown(&request, residual, context).await
                }
            }
        })
    }
}

fn validate_view(fence: &ExecutionFence, actual: &ReadFence) -> Result<(), QueryError> {
    if actual.binding().shard_id() != fence.shard_id()
        || actual.binding().placement_epoch() != fence.placement_epoch()
        || actual.binding().backend_generation() != fence.backend_generation()
        || actual.applied_index() != fence.applied_index()
    {
        return Err(QueryError::SnapshotDrift);
    }
    if actual.capability_digest() != fence.capability_digest() {
        return Err(QueryError::CapabilityDrift);
    }
    Ok(())
}

fn access_shape(access: &ExecutableAccess) -> Result<(RowSchema, u32), QueryError> {
    let (vertex, bound) = match access {
        ExecutableAccess::Logical(read) => (
            matches!(
                read.operation(),
                ReadOperation::VertexPoint(_) | ReadOperation::VertexScan
            ),
            read.row_bound(),
        ),
        ExecutableAccess::Pushdown { request, .. } => match request.operation() {
            PushdownOperation::Vertex(_) => (true, 1),
            PushdownOperation::VertexScan(scan) => (true, scan.limit()),
        },
    };
    let data_type = if vertex {
        LogicalType::Vertex
    } else {
        LogicalType::Relationship
    };
    Ok((
        RowSchema {
            fields: vec![Field {
                name: if vertex { "vertex" } else { "relationship" }.into(),
                data_type,
                nullable: false,
            }],
        },
        bound,
    ))
}

fn bounded_pushdown(
    request: &PushdownRequest,
    cursor: Cursor,
    limit: u32,
) -> Result<PushdownRequest, QueryError> {
    let operation = match request.operation() {
        PushdownOperation::Vertex(read) => PushdownOperation::Vertex(read.clone()),
        PushdownOperation::VertexScan(scan) => {
            let after = match cursor {
                Cursor::Start => scan.after(),
                Cursor::Vertex(after) => Some(after),
                Cursor::Done => scan.after(),
                Cursor::Edge(_) => {
                    return Err(QueryError::InvalidPlan("invalid pushdown cursor".into()));
                }
            };
            PushdownOperation::VertexScan(VertexScan::new(
                scan.valid_at(),
                scan.transaction_at(),
                after,
                limit,
            )?)
        }
    };
    Ok(PushdownRequest::new(
        request.contract_version(),
        request.fence().clone(),
        request.required_capabilities().clone(),
        operation,
    )?)
}

fn logical_from_pushdown(request: &PushdownRequest) -> Result<LogicalRead, QueryError> {
    match request.operation() {
        PushdownOperation::Vertex(read) => LogicalRead::new(
            ReadOperation::VertexPoint(read.id()),
            1,
            read.transaction_at(),
            read.valid_at(),
        ),
        PushdownOperation::VertexScan(scan) => LogicalRead::new(
            ReadOperation::VertexScan,
            scan.limit(),
            scan.transaction_at(),
            scan.valid_at(),
        ),
    }
}

fn exact_records(records: Vec<SnapshotRecord>) -> Result<Vec<QueryValue>, QueryError> {
    records
        .into_iter()
        .map(|record| match record {
            SnapshotRecord::Vertex(vertex) => Ok(QueryValue::Vertex(vertex)),
            SnapshotRecord::Edge(edge) => Ok(QueryValue::Relationship(edge)),
            _ => Err(QueryError::ProviderViolation(
                "exact pushdown returned a non-query record".into(),
            )),
        })
        .collect()
}

fn apply_storage_residual(
    request: &PushdownRequest,
    records: Vec<SnapshotRecord>,
    context: &mut QueryContext,
) -> Result<Vec<QueryValue>, QueryError> {
    match request.operation() {
        PushdownOperation::Vertex(read) => {
            let mut best = None;
            for record in records {
                context.checkpoint()?;
                context.charge_scan_bytes(snapshot_record_bytes(&record))?;
                let SnapshotRecord::Vertex(vertex) = record else {
                    continue;
                };
                if vertex.id() != read.id()
                    || vertex.valid_time().start() > read.valid_at()
                    || read.valid_at() >= vertex.valid_time().end()
                    || vertex.transaction_time() > read.transaction_at()
                {
                    continue;
                }
                let replace = best
                    .as_ref()
                    .is_none_or(|current: &dtg_storage::VertexVersion| {
                        (vertex.transaction_time(), vertex.version())
                            > (current.transaction_time(), current.version())
                    });
                if replace {
                    best = Some(vertex);
                }
            }
            Ok(best.into_iter().map(QueryValue::Vertex).collect())
        }
        PushdownOperation::VertexScan(scan) => {
            let mut visible = BTreeMap::new();
            for record in records {
                context.checkpoint()?;
                context.charge_scan_bytes(snapshot_record_bytes(&record))?;
                let SnapshotRecord::Vertex(vertex) = record else {
                    continue;
                };
                if scan.after().is_none_or(|after| vertex.id() > after)
                    && vertex.valid_time().start() <= scan.valid_at()
                    && scan.valid_at() < vertex.valid_time().end()
                    && vertex.transaction_time() <= scan.transaction_at()
                {
                    let replace = visible.get(&vertex.id()).is_none_or(
                        |current: &dtg_storage::VertexVersion| {
                            (vertex.transaction_time(), vertex.version())
                                > (current.transaction_time(), current.version())
                        },
                    );
                    if replace {
                        visible.insert(vertex.id(), vertex);
                    }
                }
            }
            Ok(visible
                .into_values()
                .take(scan.limit() as usize)
                .map(QueryValue::Vertex)
                .collect())
        }
    }
}

fn apply_expression_residual(
    values: Vec<QueryValue>,
    schema: &RowSchema,
    expression: &Expression,
    context: &mut QueryContext,
) -> Result<Vec<QueryValue>, QueryError> {
    let mut filtered = Vec::new();
    for value in values {
        context.checkpoint()?;
        if expression.evaluate(schema, std::slice::from_ref(&value))? == QueryValue::Boolean(true) {
            filtered.push(value);
        }
    }
    Ok(filtered)
}

fn snapshot_record_bytes(record: &SnapshotRecord) -> u64 {
    match record {
        SnapshotRecord::Vertex(vertex) => QueryValue::Vertex(vertex.clone()).estimated_bytes(),
        SnapshotRecord::Edge(edge) => QueryValue::Relationship(edge.clone()).estimated_bytes(),
        SnapshotRecord::VertexTombstone(_)
        | SnapshotRecord::EdgeTombstone(_)
        | SnapshotRecord::Transaction(_)
        | SnapshotRecord::ReplicaMetadata(_)
        | SnapshotRecord::Replay(_)
        | SnapshotRecord::Change(_) => 64,
    }
}

fn advance_pushdown_cursor(
    request: &PushdownRequest,
    values: &[QueryValue],
    raw_len: usize,
    limit: u32,
    previous: Cursor,
    cursor: &mut Cursor,
) -> Result<(), QueryError> {
    match request.operation() {
        PushdownOperation::Vertex(_) => *cursor = Cursor::Done,
        PushdownOperation::VertexScan(_) => {
            if raw_len < limit as usize {
                *cursor = Cursor::Done;
            } else {
                let next = values
                    .iter()
                    .filter_map(|value| match value {
                        QueryValue::Vertex(vertex) => Some(vertex.id()),
                        _ => None,
                    })
                    .max()
                    .ok_or_else(|| {
                        QueryError::ProviderViolation(
                            "pushdown page cannot advance its logical cursor".into(),
                        )
                    })?;
                if matches!(previous, Cursor::Vertex(value) if next <= value) {
                    return Err(QueryError::ProviderViolation(
                        "pushdown cursor did not advance".into(),
                    ));
                }
                *cursor = Cursor::Vertex(next);
            }
        }
    }
    Ok(())
}
