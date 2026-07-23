use std::future::Future;
use std::pin::Pin;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use physical_plan::{Placement, PlanFragment};
use query_executor::{
    ExecutionContext, MAX_BATCH_ROWS, RecordBatch, RuntimeError, TemporalBatchExecutor,
    TemporalExecutionError, TemporalRead, TemporalRecordBatch,
};
use storage_api::StorageAdapter;
use temporal_storage::GraphId;
use temporal_types::{Interval, ValidTime};

use crate::{DistributedQueryError, FragmentRequest};

pub type WorkerFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Vec<WorkerBatch>, DistributedQueryError>> + Send + 'a>>;
pub type TemporalWorkerFuture<'a> = Pin<
    Box<dyn Future<Output = Result<Vec<TemporalWorkerBatch>, DistributedQueryError>> + Send + 'a>,
>;

pub trait FragmentWorker: Send + Sync {
    fn shard_id(&self) -> u32;

    fn execute_fragment<'a>(
        &'a self,
        request: &'a FragmentRequest,
        fragment: &'a PlanFragment,
        valid_time: ValidTime,
        context: &'a ExecutionContext,
    ) -> WorkerFuture<'a>;

    fn execute_interval_fragment<'a>(
        &'a self,
        request: &'a FragmentRequest,
        fragment: &'a PlanFragment,
        window: Interval<ValidTime>,
        context: &'a ExecutionContext,
    ) -> TemporalWorkerFuture<'a>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkerBatch {
    shard_id: u32,
    sequence: u64,
    has_more: bool,
    snapshot_fingerprint: [u8; 32],
    batch: RecordBatch,
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
    pub const fn batch(&self) -> &RecordBatch {
        &self.batch
    }

    pub(crate) const fn snapshot_fingerprint(&self) -> [u8; 32] {
        self.snapshot_fingerprint
    }

    pub(crate) fn into_batch(self) -> RecordBatch {
        self.batch
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
        self.validate(request, fragment)?;
        let context = request_context(context, request.deadline_unix_ms(), self.shard_id)?;
        let read = TemporalRead::as_of(
            GraphId::new(request.snapshot().graph_id()),
            valid_time,
            request.snapshot().transaction_time(),
        );
        let batches = self
            .executor
            .execute_fragment(fragment, &context, read)
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
        let count = batches.len();
        batches
            .into_iter()
            .enumerate()
            .map(|(index, batch)| {
                Ok(WorkerBatch {
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

impl<A> FragmentWorker for LocalFragmentWorker<A>
where
    A: StorageAdapter + 'static,
{
    fn shard_id(&self) -> u32 {
        self.shard_id
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

    fn execute_interval_fragment<'a>(
        &'a self,
        request: &'a FragmentRequest,
        fragment: &'a PlanFragment,
        window: Interval<ValidTime>,
        context: &'a ExecutionContext,
    ) -> TemporalWorkerFuture<'a> {
        Box::pin(self.execute_interval(request, fragment, window, context))
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
