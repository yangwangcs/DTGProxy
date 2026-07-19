use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use physical_plan::{Placement, PlanFragment};
use query_executor::v2::{
    ExecutionContext, MAX_BATCH_ROWS, RecordBatch, TemporalBatchExecutor, TemporalRead,
};
use storage_api::StorageAdapter;
use temporal_storage::GraphId;
use temporal_types::ValidTime;

use crate::{DistributedQueryError, FragmentRequest};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkerBatch {
    shard_id: u32,
    sequence: u64,
    has_more: bool,
    snapshot_fingerprint: [u8; 32],
    batch: RecordBatch,
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
        let remaining = request
            .deadline_unix_ms()
            .checked_sub(unix_ms()?)
            .ok_or(DistributedQueryError::DeadlineExceeded)?;
        let context = context
            .clone()
            .with_deadline(Instant::now() + Duration::from_millis(remaining));
        let read = TemporalRead::as_of(
            GraphId::new(request.snapshot().graph_id()),
            valid_time,
            request.snapshot().transaction_time(),
        );
        let batches = self
            .executor
            .execute_fragment(fragment, &context, read)
            .await
            .map_err(|error| DistributedQueryError::Execution(error.to_string()))?;
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

    fn validate(
        &self,
        request: &FragmentRequest,
        fragment: &PlanFragment,
    ) -> Result<(), DistributedQueryError> {
        let snapshot = request.snapshot();
        if snapshot.graph_id() != self.graph_id
            || snapshot.schema_version() != self.schema_version
            || snapshot.topology_epoch() != self.topology_epoch
            || snapshot.security_fingerprint() != self.security_fingerprint
        {
            return Err(DistributedQueryError::WorkerIdentityMismatch);
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

fn unix_ms() -> Result<u64, DistributedQueryError> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| DistributedQueryError::DeadlineExceeded)?
        .as_millis();
    u64::try_from(millis).map_err(|_| DistributedQueryError::DeadlineExceeded)
}
