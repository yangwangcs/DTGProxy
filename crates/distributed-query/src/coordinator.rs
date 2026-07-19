use std::collections::BTreeMap;
use std::sync::Arc;

use physical_plan::{ExchangeKind, PhysicalPlan, Placement, PlanFragment};
use query_executor::v2::{BatchExecutor, ExecutionContext, RecordBatch};
use temporal_types::ValidTime;

use crate::{DistributedQueryError, FragmentRequest, FragmentWorker, SnapshotTokenV2, WorkerBatch};

pub struct DistributedCoordinator {
    workers: BTreeMap<u32, Arc<dyn FragmentWorker>>,
    max_buffered_bytes: u64,
    max_inflight_batches: usize,
}

impl DistributedCoordinator {
    pub fn new(
        max_buffered_bytes: u64,
        max_inflight_batches: usize,
    ) -> Result<Self, DistributedQueryError> {
        if max_buffered_bytes == 0 || max_inflight_batches == 0 {
            return Err(DistributedQueryError::InvalidCoordinator);
        }
        Ok(Self {
            workers: BTreeMap::new(),
            max_buffered_bytes,
            max_inflight_batches,
        })
    }

    pub fn register<W>(&mut self, worker: Arc<W>) -> Result<(), DistributedQueryError>
    where
        W: FragmentWorker + 'static,
    {
        let shard_id = worker.shard_id();
        if self.workers.insert(shard_id, worker).is_some() {
            return Err(DistributedQueryError::DuplicateWorker(shard_id));
        }
        Ok(())
    }

    pub async fn execute(
        &self,
        request: &FragmentRequest,
        fragment: &PlanFragment,
        valid_time: ValidTime,
        context: &ExecutionContext,
    ) -> Result<Vec<RecordBatch>, DistributedQueryError> {
        if self.workers.is_empty() {
            return Err(DistributedQueryError::InvalidCoordinator);
        }
        let worker_ids = match fragment.placement() {
            Placement::AllShards => self.workers.keys().copied().collect::<Vec<_>>(),
            Placement::Shard(shard) if self.workers.contains_key(&shard) => vec![shard],
            Placement::Shard(_) | Placement::Coordinator => {
                return Err(DistributedQueryError::InvalidCoordinator);
            }
        };
        let mut merger = TypedBatchMerger::new(
            worker_ids.clone(),
            request.snapshot().fingerprint(),
            self.max_buffered_bytes,
            self.max_inflight_batches,
        )?;
        for worker_id in worker_ids {
            let worker = self
                .workers
                .get(&worker_id)
                .ok_or(DistributedQueryError::InvalidCoordinator)?;
            let batches = worker
                .execute_fragment(request, fragment, valid_time, context)
                .await?;
            for batch in batches {
                merger.push(batch)?;
            }
        }
        merger.finish()
    }

    pub async fn execute_plan(
        &self,
        plan: &PhysicalPlan,
        snapshot: SnapshotTokenV2,
        valid_time: ValidTime,
        deadline_unix_ms: u64,
        batch_rows: u32,
        context: &ExecutionContext,
    ) -> Result<Vec<RecordBatch>, DistributedQueryError> {
        plan.validate()
            .map_err(|error| DistributedQueryError::Execution(error.to_string()))?;
        if plan.header().graph_id() != snapshot.graph_id()
            || plan.header().schema_version() != snapshot.schema_version()
            || plan.header().topology_epoch() != snapshot.topology_epoch()
        {
            return Err(DistributedQueryError::SnapshotMismatch);
        }
        let root = fragment(plan, plan.root())?;
        let incoming = plan
            .exchanges()
            .iter()
            .filter(|exchange| exchange.to() == plan.root())
            .collect::<Vec<_>>();
        if incoming.is_empty() {
            let request = FragmentRequest::new(
                root.id(),
                snapshot,
                deadline_unix_ms,
                root.budget().memory_bytes(),
                batch_rows,
            )?;
            return self.execute(&request, root, valid_time, context).await;
        }
        if root.placement() != Placement::Coordinator {
            return Err(DistributedQueryError::UnsupportedExchange);
        }
        let mut inputs = Vec::new();
        for exchange in incoming {
            if !matches!(exchange.kind(), ExchangeKind::Gather) {
                return Err(DistributedQueryError::UnsupportedExchange);
            }
            let source = fragment(plan, exchange.from())?;
            if plan
                .exchanges()
                .iter()
                .any(|candidate| candidate.to() == source.id())
            {
                return Err(DistributedQueryError::UnsupportedExchange);
            }
            let request = FragmentRequest::new(
                source.id(),
                snapshot.clone(),
                deadline_unix_ms,
                source.budget().memory_bytes(),
                batch_rows,
            )?;
            inputs.extend(self.execute(&request, source, valid_time, context).await?);
        }
        BatchExecutor::new()
            .execute_fragment(root, context, inputs)
            .map_err(|error| DistributedQueryError::Execution(error.to_string()))
    }
}

fn fragment(
    plan: &PhysicalPlan,
    id: physical_plan::FragmentId,
) -> Result<&PlanFragment, DistributedQueryError> {
    plan.fragments()
        .get(usize::try_from(id.value()).unwrap_or(usize::MAX))
        .ok_or(DistributedQueryError::FragmentMismatch)
}

struct TypedStream {
    next_sequence: u64,
    complete: bool,
    batches: Vec<RecordBatch>,
}

struct TypedBatchMerger {
    fingerprint: [u8; 32],
    max_buffered_bytes: u64,
    max_inflight_batches: usize,
    buffered_bytes: u64,
    buffered_batches: usize,
    streams: BTreeMap<u32, TypedStream>,
}

impl TypedBatchMerger {
    fn new(
        shards: Vec<u32>,
        fingerprint: [u8; 32],
        max_buffered_bytes: u64,
        max_inflight_batches: usize,
    ) -> Result<Self, DistributedQueryError> {
        if shards.is_empty() || max_buffered_bytes == 0 || max_inflight_batches == 0 {
            return Err(DistributedQueryError::InvalidCoordinator);
        }
        let streams = shards
            .into_iter()
            .map(|shard| {
                (
                    shard,
                    TypedStream {
                        next_sequence: 0,
                        complete: false,
                        batches: Vec::new(),
                    },
                )
            })
            .collect();
        Ok(Self {
            fingerprint,
            max_buffered_bytes,
            max_inflight_batches,
            buffered_bytes: 0,
            buffered_batches: 0,
            streams,
        })
    }

    fn push(&mut self, batch: WorkerBatch) -> Result<(), DistributedQueryError> {
        if batch.snapshot_fingerprint() != self.fingerprint {
            return Err(DistributedQueryError::SnapshotMismatch);
        }
        let stream = self
            .streams
            .get_mut(&batch.shard_id())
            .ok_or(DistributedQueryError::UnexpectedShard(batch.shard_id()))?;
        if stream.complete || batch.sequence() != stream.next_sequence {
            return Err(DistributedQueryError::UnexpectedSequence {
                shard_id: batch.shard_id(),
                expected: stream.next_sequence,
                actual: batch.sequence(),
            });
        }
        let next_batches = self
            .buffered_batches
            .checked_add(1)
            .ok_or(DistributedQueryError::CreditExhausted)?;
        let next_bytes = self
            .buffered_bytes
            .checked_add(batch.batch().estimated_bytes())
            .ok_or(DistributedQueryError::PayloadLimit)?;
        if next_batches > self.max_inflight_batches || next_bytes > self.max_buffered_bytes {
            return Err(DistributedQueryError::CreditExhausted);
        }
        self.buffered_batches = next_batches;
        self.buffered_bytes = next_bytes;
        stream.next_sequence = stream
            .next_sequence
            .checked_add(1)
            .ok_or(DistributedQueryError::SequenceExhausted)?;
        stream.complete = !batch.has_more();
        stream.batches.push(batch.into_batch());
        Ok(())
    }

    fn finish(self) -> Result<Vec<RecordBatch>, DistributedQueryError> {
        let incomplete = self
            .streams
            .iter()
            .filter_map(|(shard, stream)| (!stream.complete).then_some(*shard))
            .collect::<Vec<_>>();
        if !incomplete.is_empty() {
            return Err(DistributedQueryError::IncompleteShards(incomplete));
        }
        Ok(self
            .streams
            .into_values()
            .flat_map(|stream| stream.batches)
            .collect())
    }
}
