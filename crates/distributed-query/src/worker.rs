use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use physical_plan::{PhysicalAccess, Placement, PlanFragment};
use query_executor::{
    ChangeScanScope, ColumnBatch, ExecutionContext, MAX_BATCH_ROWS, QueryExecutionMetrics,
    RecordBatch, RecordMorselSource, RuntimeError, TemporalBatchExecutor, TemporalExecutionError,
    TemporalRead, TemporalRecordBatch,
};
use storage_api::{QueryCapabilitySnapshot, QueryPrimitiveCapabilities, StorageAdapter};
use temporal_ir::RowSchema;
use temporal_storage::GraphId;
use temporal_types::{Interval, ValidTime};

use crate::{
    DecodedExchangeBatch, DistributedQueryError, ExchangeCodecLimits, ExchangeDecodeExpectation,
    ExchangeFrame, FragmentRequest, MAX_EXCHANGE_PAYLOAD_BYTES, SnapshotToken,
};

pub type WorkerFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Vec<WorkerBatch>, DistributedQueryError>> + Send + 'a>>;
pub type TemporalWorkerFuture<'a> = Pin<
    Box<dyn Future<Output = Result<Vec<TemporalWorkerBatch>, DistributedQueryError>> + Send + 'a>,
>;
pub type WorkerMorselFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Option<WorkerBatch>, DistributedQueryError>> + Send + 'a>>;
pub type WorkerMorselOpenFuture<'a> = Pin<
    Box<
        dyn Future<Output = Result<Box<dyn WorkerMorselSource + Send + 'a>, DistributedQueryError>>
            + Send
            + 'a,
    >,
>;

pub trait WorkerMorselSource: Send {
    fn next<'a>(&'a mut self, max_frame_bytes: u64) -> WorkerMorselFuture<'a>;
}

struct VecWorkerMorselSource {
    batches: std::vec::IntoIter<WorkerBatch>,
}

struct LocalWorkerMorselSource<'a> {
    shard_id: u32,
    request: FragmentRequest,
    source: Box<dyn RecordMorselSource + Send + 'a>,
    max_rows: usize,
    pending: std::collections::VecDeque<RecordBatch>,
    upstream_has_more: bool,
    next_sequence: u64,
    metrics: Option<Arc<QueryExecutionMetrics>>,
}

impl WorkerMorselSource for LocalWorkerMorselSource<'_> {
    fn next<'a>(&'a mut self, max_frame_bytes: u64) -> WorkerMorselFuture<'a> {
        Box::pin(async move {
            while self.pending.is_empty() {
                let Some(morsel) = self.source.next().await.map_err(map_temporal_error)? else {
                    return Ok(None);
                };
                self.upstream_has_more = morsel.has_more();
                self.pending.extend(
                    morsel
                        .into_batch()
                        .rechunk(self.max_rows)
                        .map_err(|error| DistributedQueryError::Execution(error.to_string()))?,
                );
            }
            let batch = self.pending.pop_front().expect("checked non-empty queue");
            let copied_value_bytes = batch.variable_width_bytes();
            let batch = ColumnBatch::from_record_batch(&batch)
                .map_err(|error| DistributedQueryError::Execution(error.to_string()))?;
            if let Some(metrics) = &self.metrics {
                metrics.record_value_copy_bytes(copied_value_bytes);
            }
            let sequence = self.next_sequence;
            self.next_sequence = self
                .next_sequence
                .checked_add(1)
                .ok_or(DistributedQueryError::SequenceExhausted)?;
            let encoded = WorkerBatch::encode(
                self.shard_id,
                sequence,
                !self.pending.is_empty() || self.upstream_has_more,
                &self.request,
                &batch,
                max_frame_bytes,
            )?;
            if let Some(metrics) = &self.metrics {
                let frame_bytes = u64::try_from(encoded.frame_bytes().len())
                    .map_err(|_| DistributedQueryError::PayloadLimit)?;
                metrics.record_wire_encoded_bytes(frame_bytes);
                metrics.observe_retained_memory_bytes(frame_bytes);
            }
            Ok(Some(encoded))
        })
    }
}

impl VecWorkerMorselSource {
    fn new(batches: Vec<WorkerBatch>) -> Self {
        Self {
            batches: batches.into_iter(),
        }
    }
}

impl WorkerMorselSource for VecWorkerMorselSource {
    fn next<'a>(&'a mut self, max_frame_bytes: u64) -> WorkerMorselFuture<'a> {
        Box::pin(async move {
            let batch = self.batches.next();
            if batch.as_ref().is_some_and(|batch| {
                u64::try_from(batch.frame_bytes().len())
                    .map_or(true, |bytes| bytes > max_frame_bytes)
            }) {
                return Err(DistributedQueryError::CreditExhausted);
            }
            Ok(batch)
        })
    }
}

pub trait FragmentWorker: Send + Sync {
    fn shard_id(&self) -> u32;

    fn query_capability_generation(&self) -> u64 {
        1
    }

    fn query_primitive_capabilities(&self) -> QueryPrimitiveCapabilities {
        QueryPrimitiveCapabilities::NONE
    }

    fn query_capability_snapshot(&self) -> QueryCapabilitySnapshot {
        QueryCapabilitySnapshot::new(
            self.query_capability_generation(),
            self.query_primitive_capabilities(),
        )
    }

    fn execute_fragment<'a>(
        &'a self,
        request: &'a FragmentRequest,
        fragment: &'a PlanFragment,
        valid_time: ValidTime,
        context: &'a ExecutionContext,
    ) -> WorkerFuture<'a>;

    fn open_fragment_morsels<'a>(
        &'a self,
        request: &'a FragmentRequest,
        fragment: &'a PlanFragment,
        valid_time: ValidTime,
        context: &'a ExecutionContext,
    ) -> WorkerMorselOpenFuture<'a> {
        Box::pin(async move {
            let batches = self
                .execute_fragment(request, fragment, valid_time, context)
                .await?;
            Ok(Box::new(VecWorkerMorselSource::new(batches))
                as Box<dyn WorkerMorselSource + Send + 'a>)
        })
    }

    fn execute_interval_fragment<'a>(
        &'a self,
        request: &'a FragmentRequest,
        fragment: &'a PlanFragment,
        window: Interval<ValidTime>,
        context: &'a ExecutionContext,
    ) -> TemporalWorkerFuture<'a>;

    fn execute_change_fragment<'a>(
        &'a self,
        _request: &'a FragmentRequest,
        _fragment: &'a PlanFragment,
        _scope: ChangeScanScope,
        _context: &'a ExecutionContext,
    ) -> WorkerFuture<'a> {
        Box::pin(async { Err(DistributedQueryError::UnsupportedChangeScan) })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkerBatch {
    frame: ExchangeFrame,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TemporalWorkerBatch {
    shard_id: u32,
    sequence: u64,
    has_more: bool,
    snapshot_fingerprint: [u8; 32],
    batch: TemporalRecordBatch,
}

impl TemporalWorkerBatch {
    #[must_use]
    pub const fn shard_id(&self) -> u32 {
        self.shard_id
    }

    #[must_use]
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }

    #[must_use]
    pub const fn has_more(&self) -> bool {
        self.has_more
    }

    #[must_use]
    pub const fn batch(&self) -> &TemporalRecordBatch {
        &self.batch
    }

    pub const fn snapshot_fingerprint(&self) -> [u8; 32] {
        self.snapshot_fingerprint
    }

    pub fn into_batch(self) -> TemporalRecordBatch {
        self.batch
    }
}

impl WorkerBatch {
    pub fn from_frame_bytes(
        bytes: Vec<u8>,
        max_payload_bytes: usize,
    ) -> Result<Self, DistributedQueryError> {
        Ok(Self {
            frame: ExchangeFrame::from_bytes(
                bytes,
                ExchangeCodecLimits::new(MAX_BATCH_ROWS, max_payload_bytes)?,
            )?,
        })
    }

    #[must_use]
    pub const fn shard_id(&self) -> u32 {
        self.frame.shard_id()
    }

    #[must_use]
    pub const fn sequence(&self) -> u64 {
        self.frame.sequence()
    }

    #[must_use]
    pub const fn has_more(&self) -> bool {
        self.frame.has_more()
    }

    #[must_use]
    pub const fn row_count(&self) -> usize {
        self.frame.row_count()
    }

    #[must_use]
    pub fn frame_bytes(&self) -> &[u8] {
        self.frame.as_bytes()
    }

    pub(crate) const fn snapshot_fingerprint(&self) -> [u8; 32] {
        self.frame.snapshot_fingerprint()
    }

    pub(crate) fn decode(
        self,
        expected_sequence: u64,
        snapshot: SnapshotToken,
        schema: RowSchema,
        limits: ExchangeCodecLimits,
    ) -> Result<DecodedExchangeBatch, DistributedQueryError> {
        let shard_id = self.frame.shard_id();
        self.frame.into_decoded(
            ExchangeDecodeExpectation::new(shard_id, expected_sequence, snapshot, schema),
            limits,
        )
    }

    pub(crate) fn decoded_bytes_upper_bound(
        &self,
        expected_sequence: u64,
        snapshot: SnapshotToken,
        schema: RowSchema,
        limits: ExchangeCodecLimits,
    ) -> Result<u64, DistributedQueryError> {
        self.frame.decoded_bytes_upper_bound(
            &ExchangeDecodeExpectation::new(
                self.frame.shard_id(),
                expected_sequence,
                snapshot,
                schema,
            ),
            limits,
        )
    }

    pub fn decoded_batch(
        &self,
        snapshot: SnapshotToken,
        schema: RowSchema,
        max_payload_bytes: usize,
    ) -> Result<ColumnBatch, DistributedQueryError> {
        self.frame
            .decode(
                ExchangeDecodeExpectation::new(
                    self.frame.shard_id(),
                    self.frame.sequence(),
                    snapshot,
                    schema,
                ),
                ExchangeCodecLimits::new(MAX_BATCH_ROWS, max_payload_bytes)?,
            )
            .map(DecodedExchangeBatch::into_batch)
    }

    fn encode(
        shard_id: u32,
        sequence: u64,
        has_more: bool,
        request: &FragmentRequest,
        batch: &ColumnBatch,
        max_frame_bytes: u64,
    ) -> Result<Self, DistributedQueryError> {
        let max_payload_bytes = usize::try_from(request.memory_bytes())
            .unwrap_or(usize::MAX)
            .min(usize::try_from(max_frame_bytes).unwrap_or(usize::MAX))
            .min(MAX_EXCHANGE_PAYLOAD_BYTES);
        Ok(Self {
            frame: ExchangeFrame::encode(
                shard_id,
                sequence,
                has_more,
                request.snapshot(),
                batch,
                ExchangeCodecLimits::new(MAX_BATCH_ROWS, max_payload_bytes)?,
            )?,
        })
    }
}

pub struct LocalFragmentWorker<A> {
    shard_id: u32,
    graph_id: u64,
    schema_version: u64,
    topology_epoch: u64,
    security_fingerprint: [u8; 32],
    executor: TemporalBatchExecutor<A>,
}

impl<A> LocalFragmentWorker<A>
where
    A: StorageAdapter,
{
    #[must_use]
    pub const fn new(
        shard_id: u32,
        graph_id: u64,
        schema_version: u64,
        topology_epoch: u64,
        security_fingerprint: [u8; 32],
        executor: TemporalBatchExecutor<A>,
    ) -> Self {
        Self {
            shard_id,
            graph_id,
            schema_version,
            topology_epoch,
            security_fingerprint,
            executor,
        }
    }

    pub async fn execute(
        &self,
        request: &FragmentRequest,
        fragment: &PlanFragment,
        valid_time: ValidTime,
        context: &ExecutionContext,
    ) -> Result<Vec<WorkerBatch>, DistributedQueryError> {
        let batches = self
            .execute_record_batches(request, fragment, valid_time, context)
            .await?;
        encode_worker_batches(self.shard_id, request, batches, context.query_metrics())
    }

    async fn execute_record_batches(
        &self,
        request: &FragmentRequest,
        fragment: &PlanFragment,
        valid_time: ValidTime,
        context: &ExecutionContext,
    ) -> Result<Vec<RecordBatch>, DistributedQueryError> {
        self.validate(request, fragment)?;
        let context = request_context(context, request.deadline_unix_ms(), self.shard_id)?;
        let read = TemporalRead::as_of(
            GraphId::new(request.snapshot().graph_id()),
            valid_time,
            request.snapshot().transaction_time(),
        );
        let expected_generation = primitive_expected_generation(request, fragment, self.shard_id)?;
        let batches = self
            .executor
            .execute_fragment_with_expected_capability_generation(
                fragment,
                &context,
                read,
                expected_generation,
            )
            .await
            .map_err(map_temporal_error)?;
        let max_rows = usize::try_from(request.batch_rows())
            .map_err(|_| DistributedQueryError::InvalidRequest)?;
        let batches = batches
            .into_iter()
            .map(|batch| batch.rechunk(max_rows))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| DistributedQueryError::Execution(error.to_string()))?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        Ok(batches)
    }

    pub async fn execute_interval(
        &self,
        request: &FragmentRequest,
        fragment: &PlanFragment,
        window: Interval<ValidTime>,
        context: &ExecutionContext,
    ) -> Result<Vec<TemporalWorkerBatch>, DistributedQueryError> {
        self.validate(request, fragment)?;
        let context = request_context(context, request.deadline_unix_ms(), self.shard_id)?;
        let rows = self
            .executor
            .execute_interval_fragment_rows(
                fragment,
                &context,
                GraphId::new(request.snapshot().graph_id()),
                window,
                request.snapshot().transaction_time(),
            )
            .await
            .map_err(map_temporal_error)?;
        let max_rows = usize::try_from(request.batch_rows())
            .map_err(|_| DistributedQueryError::InvalidRequest)?;
        let chunks = if rows.is_empty() {
            vec![TemporalRecordBatch::try_new(
                fragment.output().clone(),
                Vec::new(),
            )]
        } else {
            rows.chunks(max_rows)
                .map(|rows| TemporalRecordBatch::try_new(fragment.output().clone(), rows.to_vec()))
                .collect()
        }
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| DistributedQueryError::Execution(error.to_string()))?;
        let count = chunks.len();
        chunks
            .into_iter()
            .enumerate()
            .map(|(index, batch)| {
                Ok(TemporalWorkerBatch {
                    shard_id: self.shard_id,
                    sequence: u64::try_from(index)
                        .map_err(|_| DistributedQueryError::SequenceExhausted)?,
                    has_more: index + 1 < count,
                    snapshot_fingerprint: request.snapshot().fingerprint(),
                    batch,
                })
            })
            .collect()
    }

    pub async fn execute_change(
        &self,
        request: &FragmentRequest,
        fragment: &PlanFragment,
        scope: ChangeScanScope,
        context: &ExecutionContext,
    ) -> Result<Vec<WorkerBatch>, DistributedQueryError> {
        self.validate(request, fragment)?;
        if scope.graph().value() != request.snapshot().graph_id()
            || scope.snapshot() > request.snapshot().transaction_time()
        {
            return Err(DistributedQueryError::SnapshotMismatch);
        }
        let context = request_context(context, request.deadline_unix_ms(), self.shard_id)?;
        let output_batch_rows = usize::try_from(request.batch_rows())
            .map_err(|_| DistributedQueryError::InvalidRequest)?;
        let required_applied_index = request.required_applied_index(self.shard_id).ok_or(
            DistributedQueryError::MissingRequiredAppliedIndex(self.shard_id),
        )?;
        let scan_entry_budget =
            usize::try_from(fragment.execution_budget().raw_scan().entry_limit())
                .unwrap_or(usize::MAX)
                .min(usize::MAX - 1);
        let expected_generation = primitive_expected_generation(request, fragment, self.shard_id)?;
        let binding = self
            .executor
            .store()
            .read_snapshot_binding()
            .map_err(TemporalExecutionError::from)
            .map_err(map_temporal_error)?;
        let read = match binding.as_ref() {
            Some(binding) => {
                if expected_generation
                    .is_some_and(|expected| expected != binding.capability_generation())
                {
                    return Err(DistributedQueryError::CapabilityGenerationMismatch);
                }
                binding
                    .owner()
                    .begin_read_snapshot()
                    .await
                    .map_err(temporal_storage::TemporalStoreError::from)
                    .map_err(TemporalExecutionError::from)
                    .map_err(map_temporal_error)?
            }
            None => self
                .executor
                .store()
                .begin_read_snapshot()
                .await
                .map_err(TemporalExecutionError::from)
                .map_err(map_temporal_error)?,
        };
        let result = self
            .executor
            .execute_change_fragment_in_snapshot(
                read.as_ref(),
                fragment,
                &scope,
                &context,
                scan_entry_budget,
            )
            .await
            .map_err(map_temporal_error)?;
        let actual_applied_index = result.applied_log_index();
        if actual_applied_index < required_applied_index {
            return Err(DistributedQueryError::StaleReadIndex {
                shard_id: self.shard_id,
                required: required_applied_index,
                actual: actual_applied_index,
            });
        }
        let batches = result
            .into_batches()
            .into_iter()
            .map(|batch| batch.rechunk(output_batch_rows))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| DistributedQueryError::Execution(error.to_string()))?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        encode_worker_batches(self.shard_id, request, batches, context.query_metrics())
    }

    fn validate(
        &self,
        request: &FragmentRequest,
        fragment: &PlanFragment,
    ) -> Result<(), DistributedQueryError> {
        let snapshot = request.snapshot();
        if snapshot.graph_id() != self.graph_id
            || snapshot.schema_version() != self.schema_version
            || snapshot.topology_epoch() != self.topology_epoch
        {
            return Err(DistributedQueryError::WorkerIdentityMismatch);
        }
        if snapshot.security_fingerprint() != self.security_fingerprint {
            return Err(DistributedQueryError::SecurityMismatch);
        }
        if request.fragment_id() != fragment.id() {
            return Err(DistributedQueryError::FragmentMismatch);
        }
        if let Some(expected) = primitive_expected_generation(request, fragment, self.shard_id)?
            && self
                .executor
                .store()
                .adapter()
                .query_capability_generation()
                != expected
        {
            return Err(DistributedQueryError::CapabilityGenerationMismatch);
        }
        let placement_ok = match fragment.placement() {
            Placement::AllShards => true,
            Placement::Shard(shard) => shard == self.shard_id,
            Placement::Coordinator => false,
        };
        if !placement_ok
            || request.memory_bytes() != fragment.budget().memory_bytes()
            || request.batch_rows() == 0
            || usize::try_from(request.batch_rows()).unwrap_or(usize::MAX) > MAX_BATCH_ROWS
        {
            return Err(DistributedQueryError::InvalidRequest);
        }
        Ok(())
    }
}

fn primitive_expected_generation(
    request: &FragmentRequest,
    fragment: &PlanFragment,
    shard_id: u32,
) -> Result<Option<u64>, DistributedQueryError> {
    if fragment
        .access()
        .iter()
        .any(|access| matches!(access, PhysicalAccess::Primitive { .. }))
    {
        return request
            .expected_capability_generation(shard_id)
            .map(Some)
            .ok_or(DistributedQueryError::InvalidRequest);
    }
    Ok(None)
}

fn encode_worker_batches(
    shard_id: u32,
    request: &FragmentRequest,
    batches: Vec<RecordBatch>,
    metrics: Option<Arc<QueryExecutionMetrics>>,
) -> Result<Vec<WorkerBatch>, DistributedQueryError> {
    let count = batches.len();
    let mut encoded = Vec::with_capacity(count);
    let mut encoded_bytes = 0_u64;
    for (index, batch) in batches.into_iter().enumerate() {
        let sequence =
            u64::try_from(index).map_err(|_| DistributedQueryError::SequenceExhausted)?;
        let copied_value_bytes = batch.variable_width_bytes();
        let batch = ColumnBatch::from_record_batch(&batch)
            .map_err(|error| DistributedQueryError::Execution(error.to_string()))?;
        if let Some(metrics) = &metrics {
            metrics.record_value_copy_bytes(copied_value_bytes);
        }
        let worker_batch = WorkerBatch::encode(
            shard_id,
            sequence,
            index + 1 < count,
            request,
            &batch,
            request.memory_bytes(),
        )?;
        encoded_bytes = encoded_bytes
            .checked_add(
                u64::try_from(worker_batch.frame_bytes().len())
                    .map_err(|_| DistributedQueryError::PayloadLimit)?,
            )
            .ok_or(DistributedQueryError::PayloadLimit)?;
        if let Some(metrics) = &metrics {
            let frame_bytes = u64::try_from(worker_batch.frame_bytes().len())
                .map_err(|_| DistributedQueryError::PayloadLimit)?;
            metrics.record_wire_encoded_bytes(frame_bytes);
            metrics.observe_retained_memory_bytes(frame_bytes);
        }
        if encoded_bytes > request.memory_bytes() {
            return Err(DistributedQueryError::MemoryLimitExceeded {
                limit: request.memory_bytes(),
                required: encoded_bytes,
            });
        }
        encoded.push(worker_batch);
    }
    Ok(encoded)
}

impl<A> FragmentWorker for LocalFragmentWorker<A>
where
    A: StorageAdapter + 'static,
{
    fn shard_id(&self) -> u32 {
        self.shard_id
    }

    fn query_capability_generation(&self) -> u64 {
        self.executor
            .store()
            .adapter()
            .query_capability_generation()
    }

    fn query_primitive_capabilities(&self) -> QueryPrimitiveCapabilities {
        self.executor
            .store()
            .adapter()
            .query_primitive_capabilities()
    }

    fn query_capability_snapshot(&self) -> QueryCapabilitySnapshot {
        self.executor.store().adapter().query_capability_snapshot()
    }

    fn execute_fragment<'a>(
        &'a self,
        request: &'a FragmentRequest,
        fragment: &'a PlanFragment,
        valid_time: ValidTime,
        context: &'a ExecutionContext,
    ) -> WorkerFuture<'a> {
        Box::pin(self.execute(request, fragment, valid_time, context))
    }

    fn open_fragment_morsels<'a>(
        &'a self,
        request: &'a FragmentRequest,
        fragment: &'a PlanFragment,
        valid_time: ValidTime,
        context: &'a ExecutionContext,
    ) -> WorkerMorselOpenFuture<'a> {
        Box::pin(async move {
            self.validate(request, fragment)?;
            let context = request_context(context, request.deadline_unix_ms(), self.shard_id)?;
            let read = TemporalRead::as_of(
                GraphId::new(request.snapshot().graph_id()),
                valid_time,
                request.snapshot().transaction_time(),
            );
            let expected_generation =
                primitive_expected_generation(request, fragment, self.shard_id)?;
            let max_rows = usize::try_from(request.batch_rows())
                .map_err(|_| DistributedQueryError::InvalidRequest)?;
            let source = self.executor.open_fragment_morsels(
                fragment,
                &context,
                read,
                expected_generation,
                max_rows,
            );
            Ok(Box::new(LocalWorkerMorselSource {
                shard_id: self.shard_id,
                request: request.clone(),
                source,
                max_rows,
                pending: std::collections::VecDeque::new(),
                upstream_has_more: false,
                next_sequence: 0,
                metrics: context.query_metrics(),
            }) as Box<dyn WorkerMorselSource + Send + 'a>)
        })
    }

    fn execute_interval_fragment<'a>(
        &'a self,
        request: &'a FragmentRequest,
        fragment: &'a PlanFragment,
        window: Interval<ValidTime>,
        context: &'a ExecutionContext,
    ) -> TemporalWorkerFuture<'a> {
        Box::pin(self.execute_interval(request, fragment, window, context))
    }

    fn execute_change_fragment<'a>(
        &'a self,
        request: &'a FragmentRequest,
        fragment: &'a PlanFragment,
        scope: ChangeScanScope,
        context: &'a ExecutionContext,
    ) -> WorkerFuture<'a> {
        Box::pin(self.execute_change(request, fragment, scope, context))
    }
}

fn unix_ms() -> Result<u64, DistributedQueryError> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| DistributedQueryError::DeadlineExceeded)?
        .as_millis();
    u64::try_from(millis).map_err(|_| DistributedQueryError::DeadlineExceeded)
}

fn map_temporal_error(error: TemporalExecutionError) -> DistributedQueryError {
    match error {
        TemporalExecutionError::Runtime(RuntimeError::Cancelled) => {
            DistributedQueryError::Cancelled
        }
        TemporalExecutionError::Runtime(RuntimeError::DeadlineExceeded) => {
            DistributedQueryError::DeadlineExceeded
        }
        TemporalExecutionError::Runtime(RuntimeError::MemoryLimitExceeded { limit, required }) => {
            DistributedQueryError::MemoryLimitExceeded { limit, required }
        }
        TemporalExecutionError::Runtime(RuntimeError::ApplyInvocationLimit { max }) => {
            DistributedQueryError::ApplyInvocationLimit { max }
        }
        TemporalExecutionError::Runtime(RuntimeError::ApplyOutputRowLimit { max }) => {
            DistributedQueryError::ApplyOutputRowLimit { max }
        }
        TemporalExecutionError::Storage(_) => DistributedQueryError::StorageFailure,
        TemporalExecutionError::Runtime(RuntimeError::InvalidPhysicalPlan) => {
            DistributedQueryError::FragmentMismatch
        }
        TemporalExecutionError::Runtime(RuntimeError::CapabilityGenerationMismatch) => {
            DistributedQueryError::CapabilityGenerationMismatch
        }
        other => DistributedQueryError::Execution(other.to_string()),
    }
}

fn request_context(
    context: &ExecutionContext,
    deadline_unix_ms: u64,
    shard_id: u32,
) -> Result<ExecutionContext, DistributedQueryError> {
    let context = context.clone().for_shard(shard_id);
    if deadline_unix_ms == u64::MAX {
        return Ok(context);
    }
    let remaining = deadline_unix_ms
        .checked_sub(unix_ms()?)
        .ok_or(DistributedQueryError::DeadlineExceeded)?;
    let deadline = Instant::now()
        .checked_add(Duration::from_millis(remaining))
        .ok_or(DistributedQueryError::DeadlineExceeded)?;
    if context.deadline().is_some_and(|parent| parent <= deadline) {
        Ok(context)
    } else {
        Ok(context.with_deadline(deadline))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use physical_plan::FragmentId;
    use query_executor::{ExecutionMorsel, RuntimeValue};
    use temporal_ir::{Column, SlotId, ValueType};
    use temporal_types::TransactionTime;

    struct TestRecordMorselSource {
        batches: std::iter::Peekable<std::vec::IntoIter<RecordBatch>>,
    }

    impl RecordMorselSource for TestRecordMorselSource {
        fn next<'a>(&'a mut self) -> query_executor::RecordMorselFuture<'a> {
            Box::pin(async move {
                let batch = self.batches.next();
                Ok(batch.map(|batch| ExecutionMorsel::new(batch, self.batches.peek().is_some())))
            })
        }
    }

    #[tokio::test]
    async fn local_morsel_source_encodes_only_when_next_is_requested() {
        let metrics = Arc::new(QueryExecutionMetrics::default());
        let schema = RowSchema::new(vec![Column::new(
            SlotId::new(0),
            "value",
            ValueType::Integer,
            false,
        )])
        .unwrap();
        let batch =
            || RecordBatch::try_new(schema.clone(), vec![vec![RuntimeValue::Integer(1)]]).unwrap();
        let request = FragmentRequest::new(
            FragmentId::new(0),
            SnapshotToken::new(1, 1, 1, TransactionTime::new(1, 0), [1; 32]).unwrap(),
            u64::MAX,
            1 << 20,
            1,
        )
        .unwrap();
        let mut source = LocalWorkerMorselSource {
            shard_id: 0,
            request,
            source: Box::new(TestRecordMorselSource {
                batches: vec![batch(), batch()].into_iter().peekable(),
            }),
            max_rows: 1,
            pending: std::collections::VecDeque::new(),
            upstream_has_more: false,
            next_sequence: 0,
            metrics: Some(Arc::clone(&metrics)),
        };

        assert_eq!(metrics.snapshot().wire_encoded_bytes(), 0);
        assert_eq!(source.next(1 << 20).await.unwrap().unwrap().sequence(), 0);
        let after_first = metrics.snapshot().wire_encoded_bytes();
        assert!(after_first > 0);
        assert_eq!(source.next(1 << 20).await.unwrap().unwrap().sequence(), 1);
        assert!(metrics.snapshot().wire_encoded_bytes() > after_first);
    }
}
