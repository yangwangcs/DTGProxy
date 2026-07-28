use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Poll, Waker};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures_util::stream::{FuturesUnordered, StreamExt};
use physical_plan::{ExchangeKind, PhysicalOperator, PhysicalPlan, Placement, PlanFragment};
use query_executor::{
    ApplyBudgetLedger, BatchExecutor, ChangeScanScope, ChildInvocationFuture,
    ChildInvocationLimits, ChildOutputDemand, ChildPlanInvoker, ColumnBatch, ExecutionContext,
    IntervalChildInvocationFuture, MAX_BATCH_ROWS, QueryExecutionMetrics, RecordBatch,
    RuntimeError, RuntimeValue, TemporalRegion, TemporalRow, distinct_temporal_rows,
    ensure_temporal_rows_memory, execute_interval_coordinator_operators_with_invoker_and_ledger,
    temporal_hash_join_bounded, temporal_left_hash_join_bounded,
};
use storage_api::{PushdownGuarantee, QueryPrimitiveCapabilities};
use temporal_ir::RowSchema;
use temporal_types::{Interval, ValidTime};

use crate::{
    DistributedQueryError, ExchangeCodecLimits, FragmentRequest, FragmentWorker, SnapshotToken,
    WorkerBatch,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChangePlanRequest {
    snapshot: SnapshotToken,
    scope: ChangeScanScope,
    required_applied_indexes: BTreeMap<u32, u64>,
    deadline_unix_ms: u64,
    batch_rows: u32,
}

impl ChangePlanRequest {
    #[must_use]
    pub fn new(
        snapshot: SnapshotToken,
        scope: ChangeScanScope,
        required_applied_indexes: BTreeMap<u32, u64>,
        deadline_unix_ms: u64,
        batch_rows: u32,
    ) -> Self {
        Self {
            snapshot,
            scope,
            required_applied_indexes,
            deadline_unix_ms,
            batch_rows,
        }
    }
}

pub struct DistributedCoordinator {
    workers: BTreeMap<u32, Arc<dyn FragmentWorker>>,
    max_buffered_bytes: u64,
    max_inflight_batches: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DistributedCapabilitySnapshot {
    generation: u64,
    capabilities: QueryPrimitiveCapabilities,
    shard_generations: BTreeMap<u32, u64>,
}

impl DistributedCapabilitySnapshot {
    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    #[must_use]
    pub const fn capabilities(&self) -> QueryPrimitiveCapabilities {
        self.capabilities
    }

    #[must_use]
    pub const fn shard_generations(&self) -> &BTreeMap<u32, u64> {
        &self.shard_generations
    }
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

    pub fn capability_snapshot(
        &self,
        shard_ids: &[u32],
    ) -> Result<DistributedCapabilitySnapshot, DistributedQueryError> {
        if shard_ids.is_empty() {
            return Err(DistributedQueryError::InvalidCoordinator);
        }
        let mut ordered = shard_ids.to_vec();
        ordered.sort_unstable();
        if ordered.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(DistributedQueryError::InvalidCoordinator);
        }
        let missing = ordered
            .iter()
            .copied()
            .filter(|shard_id| !self.workers.contains_key(shard_id))
            .collect::<Vec<_>>();
        if !missing.is_empty() {
            return Err(DistributedQueryError::MissingShards(missing));
        }

        let mut candidate_scan = PushdownGuarantee::Exact;
        let mut property_gather = PushdownGuarantee::Exact;
        let mut adjacency_expand = PushdownGuarantee::Exact;
        let mut change_scan = PushdownGuarantee::Exact;
        let mut hasher = blake3::Hasher::new();
        let mut shard_generations = BTreeMap::new();
        hasher.update(b"DTGProxy/DistributedCapabilitySnapshot/V1");
        for shard_id in ordered {
            let worker = self
                .workers
                .get(&shard_id)
                .ok_or(DistributedQueryError::InvalidCoordinator)?;
            let snapshot = worker.query_capability_snapshot();
            let generation = snapshot.generation();
            if generation == 0 {
                return Err(DistributedQueryError::InvalidCoordinator);
            }
            shard_generations.insert(shard_id, generation);
            let capabilities = snapshot.capabilities();
            candidate_scan = meet_guarantee(candidate_scan, capabilities.candidate_scan());
            property_gather = meet_guarantee(property_gather, capabilities.property_gather());
            adjacency_expand = meet_guarantee(adjacency_expand, capabilities.adjacency_expand());
            change_scan = meet_guarantee(change_scan, capabilities.change_scan());
            hasher.update(&shard_id.to_be_bytes());
            hasher.update(&generation.to_be_bytes());
            for guarantee in [
                capabilities.candidate_scan(),
                capabilities.property_gather(),
                capabilities.adjacency_expand(),
                capabilities.change_scan(),
            ] {
                hasher.update(&[guarantee_code(guarantee)]);
            }
        }
        let mut generation_bytes = [0_u8; 8];
        generation_bytes.copy_from_slice(&hasher.finalize().as_bytes()[..8]);
        let generation = u64::from_be_bytes(generation_bytes).max(1);
        Ok(DistributedCapabilitySnapshot {
            generation,
            capabilities: QueryPrimitiveCapabilities::new(
                candidate_scan,
                property_gather,
                adjacency_expand,
                change_scan,
            ),
            shard_generations,
        })
    }

    fn validate_plan_capabilities(
        &self,
        plan: &PhysicalPlan,
    ) -> Result<Option<DistributedCapabilitySnapshot>, DistributedQueryError> {
        let uses_primitive = plan.fragments().iter().any(|fragment| {
            fragment
                .access()
                .iter()
                .any(|access| matches!(access, physical_plan::PhysicalAccess::Primitive { .. }))
        });
        if !uses_primitive {
            return Ok(None);
        }
        let current = self.capability_snapshot(plan.header().expected_shards())?;
        if current.generation() != plan.header().capability_generation() {
            return Err(DistributedQueryError::CapabilityGenerationMismatch);
        }
        Ok(Some(current))
    }

    pub async fn execute(
        &self,
        request: &FragmentRequest,
        fragment: &PlanFragment,
        valid_time: ValidTime,
        context: &ExecutionContext,
    ) -> Result<Vec<RecordBatch>, DistributedQueryError> {
        let mut output_batches = Vec::new();
        self.execute_each_batch(request, fragment, valid_time, context, |batch| {
            output_batches.push(batch);
            std::future::ready(Ok(()))
        })
        .await?;
        Ok(output_batches)
    }

    pub async fn execute_each_batch<F, Fut>(
        &self,
        request: &FragmentRequest,
        fragment: &PlanFragment,
        valid_time: ValidTime,
        context: &ExecutionContext,
        mut consume: F,
    ) -> Result<(), DistributedQueryError>
    where
        F: FnMut(RecordBatch) -> Fut,
        Fut: Future<Output = Result<(), DistributedQueryError>>,
    {
        if self.workers.is_empty() {
            return Err(DistributedQueryError::InvalidCoordinator);
        }
        let worker_ids = match fragment.placement() {
            Placement::AllShards => {
                let missing = request
                    .expected_shards()
                    .iter()
                    .copied()
                    .filter(|shard| !self.workers.contains_key(shard))
                    .collect::<Vec<_>>();
                if !missing.is_empty() {
                    return Err(DistributedQueryError::MissingShards(missing));
                }
                request.expected_shards().to_vec()
            }
            Placement::Shard(shard) if self.workers.contains_key(&shard) => vec![shard],
            Placement::Shard(_) | Placement::Coordinator => {
                return Err(DistributedQueryError::InvalidCoordinator);
            }
        };
        let mut merger = TypedBatchMerger::new(
            worker_ids.clone(),
            request.snapshot().clone(),
            fragment.output().clone(),
            self.max_buffered_bytes,
            self.max_inflight_batches,
        )?
        .with_query_metrics(context.query_metrics());
        let sources = if context.benchmark_ablations().parallel_shard_fanout {
            let open_futures = worker_ids
                .iter()
                .map(|worker_id| {
                    self.workers
                        .get(worker_id)
                        .ok_or(DistributedQueryError::InvalidCoordinator)
                        .map(|worker| {
                            worker.open_fragment_morsels(request, fragment, valid_time, context)
                        })
                })
                .collect::<Result<Vec<_>, _>>()?;
            await_with_fences(join_ordered(open_futures), context).await?
        } else {
            let mut sources = Vec::with_capacity(worker_ids.len());
            for worker_id in &worker_ids {
                let worker = self
                    .workers
                    .get(worker_id)
                    .ok_or(DistributedQueryError::InvalidCoordinator)?;
                let source = await_with_fences(
                    worker.open_fragment_morsels(request, fragment, valid_time, context),
                    context,
                )
                .await?;
                context.record_serial_shard_open();
                sources.push(source);
            }
            sources
        };
        let mut fan_in = BoundedMorselFanIn::new(
            worker_ids.into_iter().zip(sources).collect(),
            self.max_inflight_batches,
            self.max_buffered_bytes,
        )?
        .with_query_metrics(context.query_metrics());
        while let Some((_worker_id, batch)) = await_with_fences(fan_in.next(), context).await? {
            let batch = materialize_column_output(merger.push(batch)?, context)?;
            consume(batch).await?;
        }
        merger.finish()
    }

    async fn execute_with_demand(
        &self,
        request: &FragmentRequest,
        fragment: &PlanFragment,
        valid_time: ValidTime,
        context: &ExecutionContext,
        demand: ChildOutputDemand,
    ) -> Result<Vec<RecordBatch>, DistributedQueryError> {
        if demand == ChildOutputDemand::AllRows {
            return self.execute(request, fragment, valid_time, context).await;
        }
        if self.workers.is_empty() {
            return Err(DistributedQueryError::InvalidCoordinator);
        }
        let worker_ids = match fragment.placement() {
            Placement::AllShards => {
                let missing = request
                    .expected_shards()
                    .iter()
                    .copied()
                    .filter(|shard| !self.workers.contains_key(shard))
                    .collect::<Vec<_>>();
                if !missing.is_empty() {
                    return Err(DistributedQueryError::MissingShards(missing));
                }
                request.expected_shards().to_vec()
            }
            Placement::Shard(shard) if self.workers.contains_key(&shard) => vec![shard],
            Placement::Shard(_) | Placement::Coordinator => {
                return Err(DistributedQueryError::InvalidCoordinator);
            }
        };
        let mut merger = TypedBatchMerger::new(
            worker_ids.clone(),
            request.snapshot().clone(),
            fragment.output().clone(),
            self.max_buffered_bytes,
            self.max_inflight_batches,
        )?
        .with_query_metrics(context.query_metrics());
        for worker_id in worker_ids {
            let worker = self
                .workers
                .get(&worker_id)
                .ok_or(DistributedQueryError::InvalidCoordinator)?;
            let mut source = await_with_fences(
                worker.open_fragment_morsels(request, fragment, valid_time, context),
                context,
            )
            .await?;
            while let Some(batch) =
                await_with_fences(source.next(self.max_buffered_bytes / 2), context).await?
            {
                if let Some(metrics) = context.query_metrics() {
                    metrics.record_sent_frame();
                }
                let batch = merger.push(batch)?;
                if let Some(row) = batch.row(0) {
                    return Ok(vec![
                        RecordBatch::try_new(fragment.output().clone(), vec![row])
                            .map_err(|error| DistributedQueryError::Execution(error.to_string()))?,
                    ]);
                }
            }
        }
        merger.finish()?;
        Ok(Vec::new())
    }

    pub async fn execute_interval(
        &self,
        request: &FragmentRequest,
        fragment: &PlanFragment,
        window: Interval<ValidTime>,
        context: &ExecutionContext,
    ) -> Result<Vec<TemporalRow>, DistributedQueryError> {
        self.execute_interval_with_demand(
            request,
            fragment,
            window,
            context,
            ChildOutputDemand::AllRows,
        )
        .await
    }

    pub async fn execute_change(
        &self,
        request: &FragmentRequest,
        fragment: &PlanFragment,
        scope: ChangeScanScope,
        context: &ExecutionContext,
    ) -> Result<Vec<RecordBatch>, DistributedQueryError> {
        if self.workers.is_empty() {
            return Err(DistributedQueryError::InvalidCoordinator);
        }
        let worker_ids = match fragment.placement() {
            Placement::AllShards => request.expected_shards().to_vec(),
            Placement::Shard(shard) if self.workers.contains_key(&shard) => vec![shard],
            Placement::Shard(_) | Placement::Coordinator => {
                return Err(DistributedQueryError::InvalidCoordinator);
            }
        };
        if worker_ids.is_empty()
            || worker_ids
                .iter()
                .any(|shard| !self.workers.contains_key(shard))
        {
            return Err(DistributedQueryError::MissingShards(worker_ids));
        }
        let mut merger = TypedBatchMerger::new(
            worker_ids.clone(),
            request.snapshot().clone(),
            fragment.output().clone(),
            self.max_buffered_bytes,
            self.max_inflight_batches,
        )?
        .with_query_metrics(context.query_metrics());
        let mut output_batches = Vec::new();
        for worker_id in worker_ids {
            let worker = self
                .workers
                .get(&worker_id)
                .ok_or(DistributedQueryError::InvalidCoordinator)?;
            for batch in await_with_fences(
                worker.execute_change_fragment(request, fragment, scope, context),
                context,
            )
            .await?
            {
                if let Some(metrics) = context.query_metrics() {
                    metrics.record_sent_frame();
                }
                output_batches.push(materialize_column_output(merger.push(batch)?, context)?);
            }
        }
        merger.finish()?;
        Ok(output_batches)
    }

    pub async fn execute_change_plan(
        &self,
        plan: &PhysicalPlan,
        request: ChangePlanRequest,
        context: &ExecutionContext,
    ) -> Result<Vec<RecordBatch>, DistributedQueryError> {
        let ChangePlanRequest {
            snapshot,
            scope,
            required_applied_indexes,
            deadline_unix_ms,
            batch_rows,
        } = request;
        let context = context_with_deadline(context, deadline_unix_ms)?;
        plan.validate().map_err(distributed_plan_error)?;
        let capability_snapshot = self.validate_plan_capabilities(plan)?;
        if plan.header().graph_id() != snapshot.graph_id()
            || plan.header().schema_version() != snapshot.schema_version()
            || plan.header().topology_epoch() != snapshot.topology_epoch()
        {
            return Err(DistributedQueryError::SnapshotMismatch);
        }
        let source = plan
            .fragments()
            .iter()
            .find(|fragment| {
                fragment
                    .operators()
                    .iter()
                    .any(|operator| matches!(operator, PhysicalOperator::ChangeScan { .. }))
            })
            .ok_or(DistributedQueryError::FragmentMismatch)?;
        let request = FragmentRequest::new(
            source.id(),
            snapshot,
            deadline_unix_ms,
            source.budget().memory_bytes(),
            batch_rows,
        )?;
        let required_applied_indexes = match source.placement() {
            Placement::AllShards => required_applied_indexes,
            Placement::Shard(shard) => required_applied_indexes
                .get(&shard)
                .copied()
                .map(|index| BTreeMap::from([(shard, index)]))
                .unwrap_or_default(),
            Placement::Coordinator => return Err(DistributedQueryError::FragmentMismatch),
        };
        let request = request.with_required_applied_indexes(required_applied_indexes)?;
        let request = if matches!(source.placement(), Placement::AllShards) {
            request.with_expected_shards(plan.header().expected_shards().to_vec())?
        } else {
            request
        };
        let request = bind_fragment_capabilities(request, source, capability_snapshot.as_ref())?;
        let source_batches = self
            .execute_change(&request, source, scope, &context)
            .await?;
        if source.id() == plan.root() {
            return Ok(source_batches);
        }
        let root = plan
            .fragments()
            .iter()
            .find(|fragment| fragment.id() == plan.root())
            .ok_or(DistributedQueryError::FragmentMismatch)?;
        if root.placement() != Placement::Coordinator {
            return Err(DistributedQueryError::FragmentMismatch);
        }
        BatchExecutor::new()
            .execute_fragment(root, &context, source_batches)
            .await
            .map_err(distributed_runtime_error)
    }

    async fn execute_interval_with_demand(
        &self,
        request: &FragmentRequest,
        fragment: &PlanFragment,
        window: Interval<ValidTime>,
        context: &ExecutionContext,
        demand: ChildOutputDemand,
    ) -> Result<Vec<TemporalRow>, DistributedQueryError> {
        if self.workers.is_empty() {
            return Err(DistributedQueryError::InvalidCoordinator);
        }
        let worker_ids = match fragment.placement() {
            Placement::AllShards => {
                let missing = request
                    .expected_shards()
                    .iter()
                    .copied()
                    .filter(|shard| !self.workers.contains_key(shard))
                    .collect::<Vec<_>>();
                if !missing.is_empty() {
                    return Err(DistributedQueryError::MissingShards(missing));
                }
                request.expected_shards().to_vec()
            }
            Placement::Shard(shard) if self.workers.contains_key(&shard) => vec![shard],
            Placement::Shard(_) | Placement::Coordinator => {
                return Err(DistributedQueryError::InvalidCoordinator);
            }
        };
        let mut buffered_bytes = 0_u64;
        let mut buffered_batches = 0_usize;
        let mut rows = Vec::new();
        for worker_id in worker_ids {
            let worker = self
                .workers
                .get(&worker_id)
                .ok_or(DistributedQueryError::InvalidCoordinator)?;
            let batches = await_with_fences(
                worker.execute_interval_fragment(request, fragment, window, context),
                context,
            )
            .await?;
            let mut next_sequence = 0_u64;
            let mut complete = false;
            for batch in batches {
                if batch.snapshot_fingerprint() != request.snapshot().fingerprint() {
                    return Err(DistributedQueryError::SnapshotMismatch);
                }
                if batch.shard_id() != worker_id || batch.sequence() != next_sequence || complete {
                    return Err(DistributedQueryError::UnexpectedSequence {
                        shard_id: worker_id,
                        expected: next_sequence,
                        actual: batch.sequence(),
                    });
                }
                buffered_batches = buffered_batches
                    .checked_add(1)
                    .ok_or(DistributedQueryError::CreditExhausted)?;
                buffered_bytes = buffered_bytes
                    .checked_add(batch.batch().estimated_bytes())
                    .ok_or(DistributedQueryError::PayloadLimit)?;
                if buffered_batches > self.max_inflight_batches
                    || buffered_bytes > self.max_buffered_bytes
                {
                    return Err(DistributedQueryError::CreditExhausted);
                }
                next_sequence = next_sequence
                    .checked_add(1)
                    .ok_or(DistributedQueryError::SequenceExhausted)?;
                complete = !batch.has_more();
                let batch_rows = batch.into_batch().into_rows();
                if demand == ChildOutputDemand::FirstVisibleRow
                    && let Some(row) = batch_rows.first()
                {
                    return Ok(vec![row.clone()]);
                }
                rows.extend(batch_rows);
            }
            if !complete {
                return Err(DistributedQueryError::IncompleteShards(vec![worker_id]));
            }
        }
        Ok(rows)
    }

    pub async fn execute_plan(
        &self,
        plan: &PhysicalPlan,
        snapshot: SnapshotToken,
        valid_time: ValidTime,
        deadline_unix_ms: u64,
        batch_rows: u32,
        context: &ExecutionContext,
    ) -> Result<Vec<RecordBatch>, DistributedQueryError> {
        self.execute_plan_with_input(
            plan,
            snapshot,
            valid_time,
            deadline_unix_ms,
            batch_rows,
            context,
            None,
            ChildOutputDemand::AllRows,
            ApplyBudgetLedger::default(),
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn execute_child_plan(
        &self,
        plan: &PhysicalPlan,
        snapshot: SnapshotToken,
        valid_time: ValidTime,
        deadline_unix_ms: u64,
        batch_rows: u32,
        context: &ExecutionContext,
        argument_input: RecordBatch,
    ) -> Result<Vec<RecordBatch>, DistributedQueryError> {
        self.execute_plan_with_input(
            plan,
            snapshot,
            valid_time,
            deadline_unix_ms,
            batch_rows,
            context,
            Some(argument_input),
            ChildOutputDemand::AllRows,
            ApplyBudgetLedger::default(),
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn execute_plan_with_input(
        &self,
        plan: &PhysicalPlan,
        snapshot: SnapshotToken,
        valid_time: ValidTime,
        deadline_unix_ms: u64,
        batch_rows: u32,
        context: &ExecutionContext,
        argument_input: Option<RecordBatch>,
        demand: ChildOutputDemand,
        ledger: ApplyBudgetLedger,
    ) -> Result<Vec<RecordBatch>, DistributedQueryError> {
        let context = context_with_deadline(context, deadline_unix_ms)?;
        plan.validate().map_err(distributed_plan_error)?;
        let capability_snapshot = self.validate_plan_capabilities(plan)?;
        if plan.header().graph_id() != snapshot.graph_id()
            || plan.header().schema_version() != snapshot.schema_version()
            || plan.header().topology_epoch() != snapshot.topology_epoch()
        {
            return Err(DistributedQueryError::SnapshotMismatch);
        }
        if context.security_fingerprint() != [0; 32]
            && context.security_fingerprint() != snapshot.security_fingerprint()
        {
            return Err(DistributedQueryError::SecurityMismatch);
        }
        let invoker = DistributedChildInvoker {
            coordinator: self,
            snapshot: snapshot.clone(),
            scope: ChildTemporalScope::Point(valid_time),
            deadline_unix_ms,
            batch_rows,
        };
        let can_short_circuit =
            demand == ChildOutputDemand::FirstVisibleRow && first_row_short_circuit_safe(plan);
        let mut outputs: BTreeMap<_, Vec<RecordBatch>> = BTreeMap::new();
        for current in plan.fragments() {
            if current.id() > plan.root() {
                break;
            }
            let incoming = plan
                .exchanges()
                .iter()
                .filter(|exchange| exchange.to() == current.id())
                .collect::<Vec<_>>();
            let batches = if incoming.is_empty() {
                match current.placement() {
                    Placement::Coordinator => BatchExecutor::new()
                        .execute_fragment_with_invoker_and_demand_and_ledger(
                            current,
                            &context,
                            argument_input_for_fragment(current, argument_input.as_ref())?,
                            Some(&invoker),
                            if can_short_circuit {
                                ChildOutputDemand::FirstVisibleRow
                            } else {
                                ChildOutputDemand::AllRows
                            },
                            ledger.clone(),
                        )
                        .await
                        .map_err(distributed_runtime_error)?,
                    Placement::AllShards | Placement::Shard(_) => {
                        let request = FragmentRequest::new(
                            current.id(),
                            snapshot.clone(),
                            deadline_unix_ms,
                            current.budget().memory_bytes(),
                            batch_rows,
                        )?
                        .with_expected_shards(fragment_expected_shards(current, plan)?)?;
                        let request = bind_fragment_capabilities(
                            request,
                            current,
                            capability_snapshot.as_ref(),
                        )?;
                        self.execute_with_demand(
                            &request,
                            current,
                            valid_time,
                            &context,
                            if can_short_circuit {
                                ChildOutputDemand::FirstVisibleRow
                            } else {
                                ChildOutputDemand::AllRows
                            },
                        )
                        .await?
                    }
                }
            } else {
                if current.placement() != Placement::Coordinator {
                    return Err(DistributedQueryError::UnsupportedExchange);
                }
                if let [PhysicalOperator::HashJoin { kind, keys }] = current.operators() {
                    let [left_exchange, right_exchange] = incoming.as_slice() else {
                        return Err(DistributedQueryError::UnsupportedExchange);
                    };
                    if !matches!(left_exchange.kind(), ExchangeKind::Gather)
                        || !matches!(right_exchange.kind(), ExchangeKind::Gather)
                    {
                        return Err(DistributedQueryError::UnsupportedExchange);
                    }
                    let left = outputs
                        .get(&left_exchange.from())
                        .cloned()
                        .ok_or(DistributedQueryError::FragmentMismatch)?;
                    let right = outputs
                        .get(&right_exchange.from())
                        .cloned()
                        .ok_or(DistributedQueryError::FragmentMismatch)?;
                    let joined = bounded_row_join(
                        &left,
                        left_exchange.schema(),
                        &right,
                        right_exchange.schema(),
                        kind,
                        keys,
                        current.output(),
                        current.budget().memory_bytes(),
                    )?;
                    outputs.insert(current.id(), joined);
                    continue;
                }
                let mut inputs = Vec::new();
                let mut input_bytes = 0_u64;
                for exchange in incoming {
                    if !matches!(exchange.kind(), ExchangeKind::Gather) {
                        return Err(DistributedQueryError::UnsupportedExchange);
                    }
                    let exchange_batches = outputs
                        .get(&exchange.from())
                        .cloned()
                        .ok_or(DistributedQueryError::FragmentMismatch)?;
                    for batch in exchange_batches {
                        input_bytes = input_bytes
                            .checked_add(batch.estimated_bytes())
                            .ok_or(DistributedQueryError::PayloadLimit)?;
                        if input_bytes > current.budget().memory_bytes() {
                            return Err(memory_limit_error(
                                current.budget().memory_bytes(),
                                input_bytes,
                            ));
                        }
                        inputs.push(batch);
                    }
                }
                BatchExecutor::new()
                    .execute_fragment_with_invoker_and_demand_and_ledger(
                        current,
                        &context,
                        inputs,
                        Some(&invoker),
                        if can_short_circuit {
                            ChildOutputDemand::FirstVisibleRow
                        } else {
                            ChildOutputDemand::AllRows
                        },
                        ledger.clone(),
                    )
                    .await
                    .map_err(distributed_runtime_error)?
            };
            outputs.insert(current.id(), batches);
        }
        let mut batches = outputs
            .remove(&plan.root())
            .ok_or(DistributedQueryError::FragmentMismatch)?;
        if demand == ChildOutputDemand::FirstVisibleRow {
            retain_first_batch_row(&mut batches)?;
        }
        Ok(batches)
    }

    pub async fn execute_interval_plan(
        &self,
        plan: &PhysicalPlan,
        snapshot: SnapshotToken,
        window: Interval<ValidTime>,
        deadline_unix_ms: u64,
        batch_rows: u32,
        context: &ExecutionContext,
    ) -> Result<Vec<TemporalRow>, DistributedQueryError> {
        self.execute_interval_plan_with_input(
            plan,
            snapshot,
            window,
            deadline_unix_ms,
            batch_rows,
            context,
            None,
            ChildOutputDemand::AllRows,
            ApplyBudgetLedger::default(),
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn execute_interval_plan_with_input(
        &self,
        plan: &PhysicalPlan,
        snapshot: SnapshotToken,
        window: Interval<ValidTime>,
        deadline_unix_ms: u64,
        batch_rows: u32,
        context: &ExecutionContext,
        argument_input: Option<(RowSchema, Vec<TemporalRow>)>,
        demand: ChildOutputDemand,
        ledger: ApplyBudgetLedger,
    ) -> Result<Vec<TemporalRow>, DistributedQueryError> {
        let context = context_with_deadline(context, deadline_unix_ms)?;
        plan.validate().map_err(distributed_plan_error)?;
        let capability_snapshot = self.validate_plan_capabilities(plan)?;
        if plan.header().graph_id() != snapshot.graph_id()
            || plan.header().schema_version() != snapshot.schema_version()
            || plan.header().topology_epoch() != snapshot.topology_epoch()
        {
            return Err(DistributedQueryError::SnapshotMismatch);
        }
        if context.security_fingerprint() != [0; 32]
            && context.security_fingerprint() != snapshot.security_fingerprint()
        {
            return Err(DistributedQueryError::SecurityMismatch);
        }
        let point_region = TemporalRegion::at_transaction(snapshot.transaction_time())
            .ok_or(DistributedQueryError::FragmentMismatch)?;
        let source_free_region = TemporalRegion::new(window, point_region.transaction());
        let invoker = DistributedChildInvoker {
            coordinator: self,
            snapshot: snapshot.clone(),
            scope: ChildTemporalScope::Interval(window),
            deadline_unix_ms,
            batch_rows,
        };
        let can_short_circuit =
            demand == ChildOutputDemand::FirstVisibleRow && first_row_short_circuit_safe(plan);
        let mut outputs: BTreeMap<_, (RowSchema, Vec<TemporalRow>)> = BTreeMap::new();
        for current in plan.fragments() {
            if current.id() > plan.root() {
                break;
            }
            let incoming = plan
                .exchanges()
                .iter()
                .filter(|exchange| exchange.to() == current.id())
                .collect::<Vec<_>>();
            let rows = if incoming.is_empty() {
                match current.placement() {
                    Placement::Coordinator => {
                        let (schema, rows) = argument_interval_input_for_fragment(
                            current,
                            argument_input.as_ref(),
                            source_free_region,
                        )?;
                        execute_interval_coordinator_operators_with_invoker_and_ledger(
                            current,
                            schema,
                            rows,
                            &context,
                            Some(&invoker),
                            ledger.clone(),
                        )
                        .await
                        .map_err(distributed_runtime_error)?
                    }
                    Placement::AllShards | Placement::Shard(_) => {
                        let request = FragmentRequest::new(
                            current.id(),
                            snapshot.clone(),
                            deadline_unix_ms,
                            current.budget().memory_bytes(),
                            batch_rows,
                        )?
                        .with_expected_shards(fragment_expected_shards(current, plan)?)?;
                        let request = bind_fragment_capabilities(
                            request,
                            current,
                            capability_snapshot.as_ref(),
                        )?;
                        self.execute_interval_with_demand(
                            &request,
                            current,
                            window,
                            &context,
                            if can_short_circuit {
                                ChildOutputDemand::FirstVisibleRow
                            } else {
                                ChildOutputDemand::AllRows
                            },
                        )
                        .await?
                    }
                }
            } else {
                if current.placement() != Placement::Coordinator {
                    return Err(DistributedQueryError::UnsupportedExchange);
                }
                let mut inputs = Vec::with_capacity(incoming.len());
                for exchange in incoming {
                    if !matches!(exchange.kind(), ExchangeKind::Gather) {
                        return Err(DistributedQueryError::UnsupportedExchange);
                    }
                    let (schema, rows) = outputs
                        .get(&exchange.from())
                        .cloned()
                        .ok_or(DistributedQueryError::FragmentMismatch)?;
                    if schema != *exchange.schema() {
                        return Err(DistributedQueryError::FragmentMismatch);
                    }
                    inputs.push((schema, rows));
                }
                match current.operators() {
                    [PhysicalOperator::Union { all }] => {
                        if inputs.iter().any(|(schema, _)| schema != current.output()) {
                            return Err(DistributedQueryError::FragmentMismatch);
                        }
                        bounded_temporal_union(
                            inputs,
                            current.output(),
                            *all,
                            current.budget().memory_bytes(),
                        )?
                    }
                    [PhysicalOperator::HashJoin { kind, keys }] => {
                        let [(left_schema, left), (right_schema, right)] = inputs.as_mut_slice()
                        else {
                            return Err(DistributedQueryError::UnsupportedExchange);
                        };
                        match kind {
                            physical_plan::JoinKind::Inner => temporal_hash_join_bounded(
                                left,
                                left_schema,
                                right,
                                right_schema,
                                keys,
                                current.output(),
                                current.budget().memory_bytes(),
                            ),
                            physical_plan::JoinKind::Left => temporal_left_hash_join_bounded(
                                left,
                                left_schema,
                                right,
                                right_schema,
                                keys,
                                current.output(),
                                current.budget().memory_bytes(),
                            ),
                        }
                        .map_err(distributed_runtime_error)?
                    }
                    _ => {
                        let [(schema, rows)] = inputs.as_mut_slice() else {
                            return Err(DistributedQueryError::UnsupportedExchange);
                        };
                        execute_interval_coordinator_operators_with_invoker_and_ledger(
                            current,
                            schema.clone(),
                            std::mem::take(rows),
                            &context,
                            Some(&invoker),
                            ledger.clone(),
                        )
                        .await
                        .map_err(distributed_runtime_error)?
                    }
                }
            };
            ensure_temporal_rows_memory(current.output(), &rows, current.budget().memory_bytes())
                .map_err(distributed_runtime_error)?;
            outputs.insert(current.id(), (current.output().clone(), rows));
        }
        let mut rows = outputs
            .remove(&plan.root())
            .map(|(_, rows)| rows)
            .ok_or(DistributedQueryError::FragmentMismatch)?;
        if demand == ChildOutputDemand::FirstVisibleRow {
            rows.truncate(1);
        }
        Ok(rows)
    }
}

const fn meet_guarantee(left: PushdownGuarantee, right: PushdownGuarantee) -> PushdownGuarantee {
    match (left, right) {
        (PushdownGuarantee::Unsupported, _) | (_, PushdownGuarantee::Unsupported) => {
            PushdownGuarantee::Unsupported
        }
        (PushdownGuarantee::Candidate, _) | (_, PushdownGuarantee::Candidate) => {
            PushdownGuarantee::Candidate
        }
        (PushdownGuarantee::Exact, PushdownGuarantee::Exact) => PushdownGuarantee::Exact,
    }
}

const fn guarantee_code(guarantee: PushdownGuarantee) -> u8 {
    match guarantee {
        PushdownGuarantee::Unsupported => 0,
        PushdownGuarantee::Candidate => 1,
        PushdownGuarantee::Exact => 2,
    }
}

fn materialize_column_output(
    batch: ColumnBatch,
    context: &ExecutionContext,
) -> Result<RecordBatch, DistributedQueryError> {
    let row_batch = batch
        .to_record_batch()
        .map_err(|error| DistributedQueryError::Execution(error.to_string()))?;
    if context.benchmark_ablations().column_batches {
        return Ok(row_batch);
    }
    let column_batch = ColumnBatch::from_record_batch(&row_batch)
        .map_err(|error| DistributedQueryError::Execution(error.to_string()))?;
    let row_batch = column_batch
        .to_record_batch()
        .map_err(|error| DistributedQueryError::Execution(error.to_string()))?;
    context.record_row_column_conversion_boundary();
    Ok(row_batch)
}

type OrderedFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, DistributedQueryError>> + Send + 'a>>;

async fn join_ordered<T>(
    mut futures: Vec<OrderedFuture<'_, T>>,
) -> Result<Vec<T>, DistributedQueryError> {
    let mut results = std::iter::repeat_with(|| None)
        .take(futures.len())
        .collect::<Vec<Option<Result<T, DistributedQueryError>>>>();
    std::future::poll_fn(move |cx| {
        for (future, result) in futures.iter_mut().zip(&mut results) {
            if result.is_none()
                && let Poll::Ready(output) = future.as_mut().poll(cx)
            {
                *result = Some(output);
            }
        }

        for result in &mut results {
            match result.as_ref() {
                Some(Ok(_)) => {}
                Some(Err(_)) => {
                    let Some(Err(error)) = result.take() else {
                        unreachable!("ordered join error slot changed while polled")
                    };
                    return Poll::Ready(Err(error));
                }
                None => return Poll::Pending,
            }
        }

        Poll::Ready(Ok(results
            .iter_mut()
            .map(|result| match result.take() {
                Some(Ok(value)) => value,
                _ => unreachable!("ordered join completed without every result"),
            })
            .collect()))
    })
    .await
}

#[derive(Clone, Copy)]
enum ChildTemporalScope {
    Point(ValidTime),
    Interval(Interval<ValidTime>),
}

struct DistributedChildInvoker<'a> {
    coordinator: &'a DistributedCoordinator,
    snapshot: SnapshotToken,
    scope: ChildTemporalScope,
    deadline_unix_ms: u64,
    batch_rows: u32,
}

impl ChildPlanInvoker for DistributedChildInvoker<'_> {
    fn invoke<'a>(
        &'a self,
        plan: &'a PhysicalPlan,
        input: RecordBatch,
        context: &'a ExecutionContext,
        demand: ChildOutputDemand,
        _limits: ChildInvocationLimits,
        ledger: ApplyBudgetLedger,
    ) -> ChildInvocationFuture<'a> {
        Box::pin(async move {
            let ChildTemporalScope::Point(valid_time) = self.scope else {
                return Err(RuntimeError::ChildInvocationFailed);
            };
            self.coordinator
                .execute_plan_with_input(
                    plan,
                    self.snapshot.clone(),
                    valid_time,
                    self.deadline_unix_ms,
                    self.batch_rows,
                    context,
                    Some(input),
                    demand,
                    ledger.clone(),
                )
                .await
                .map_err(distributed_child_error)
        })
    }

    fn invoke_interval<'a>(
        &'a self,
        plan: &'a PhysicalPlan,
        input_schema: RowSchema,
        input_rows: Vec<TemporalRow>,
        context: &'a ExecutionContext,
        demand: ChildOutputDemand,
        _limits: ChildInvocationLimits,
        ledger: ApplyBudgetLedger,
    ) -> IntervalChildInvocationFuture<'a> {
        Box::pin(async move {
            let ChildTemporalScope::Interval(window) = self.scope else {
                return Err(RuntimeError::ChildInvocationFailed);
            };
            self.coordinator
                .execute_interval_plan_with_input(
                    plan,
                    self.snapshot.clone(),
                    window,
                    self.deadline_unix_ms,
                    self.batch_rows,
                    context,
                    Some((input_schema, input_rows)),
                    demand,
                    ledger.clone(),
                )
                .await
                .map_err(distributed_child_error)
        })
    }
}

fn bind_fragment_capabilities(
    request: FragmentRequest,
    fragment: &PlanFragment,
    snapshot: Option<&DistributedCapabilitySnapshot>,
) -> Result<FragmentRequest, DistributedQueryError> {
    let uses_primitive = fragment
        .access()
        .iter()
        .any(|access| matches!(access, physical_plan::PhysicalAccess::Primitive { .. }));
    if !uses_primitive {
        return Ok(request);
    }
    let snapshot = snapshot.ok_or(DistributedQueryError::CapabilityGenerationMismatch)?;
    let shard_ids = if request.expected_shards().is_empty() {
        match fragment.placement() {
            Placement::Shard(shard_id) => vec![shard_id],
            Placement::AllShards => snapshot.shard_generations().keys().copied().collect(),
            Placement::Coordinator => return Err(DistributedQueryError::FragmentMismatch),
        }
    } else {
        request.expected_shards().to_vec()
    };
    let generations = shard_ids
        .into_iter()
        .map(|shard_id| {
            snapshot
                .shard_generations()
                .get(&shard_id)
                .copied()
                .map(|generation| (shard_id, generation))
                .ok_or_else(|| DistributedQueryError::MissingShards(vec![shard_id]))
        })
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    request.with_expected_capability_generations(generations)
}

fn fragment_expected_shards(
    fragment: &PlanFragment,
    plan: &PhysicalPlan,
) -> Result<Vec<u32>, DistributedQueryError> {
    match fragment.placement() {
        Placement::AllShards => Ok(plan.header().expected_shards().to_vec()),
        Placement::Shard(shard_id) => Ok(vec![shard_id]),
        Placement::Coordinator => Err(DistributedQueryError::FragmentMismatch),
    }
}

fn distributed_child_error(error: DistributedQueryError) -> RuntimeError {
    match error {
        DistributedQueryError::DeadlineExceeded => RuntimeError::DeadlineExceeded,
        DistributedQueryError::MissingShards(shards) => RuntimeError::MissingShards(shards),
        DistributedQueryError::SnapshotMismatch => RuntimeError::ChildSnapshotMismatch,
        DistributedQueryError::CapabilityGenerationMismatch => {
            RuntimeError::ChildCapabilityGenerationMismatch
        }
        DistributedQueryError::SecurityMismatch => RuntimeError::ChildSecurityMismatch,
        DistributedQueryError::Cancelled => RuntimeError::Cancelled,
        DistributedQueryError::MemoryLimitExceeded { limit, required } => {
            RuntimeError::MemoryLimitExceeded { limit, required }
        }
        DistributedQueryError::ApplyInvocationLimit { max } => {
            RuntimeError::ApplyInvocationLimit { max }
        }
        DistributedQueryError::ApplyOutputRowLimit { max } => {
            RuntimeError::ApplyOutputRowLimit { max }
        }
        DistributedQueryError::IncompleteShards(shards) => {
            RuntimeError::ChildIncompleteShards(shards)
        }
        DistributedQueryError::WorkerIdentityMismatch => RuntimeError::ChildWorkerIdentityMismatch,
        DistributedQueryError::FragmentMismatch => RuntimeError::ChildFragmentMismatch,
        DistributedQueryError::StorageFailure => RuntimeError::ChildStorageFailure,
        DistributedQueryError::RecursivePlanViolation => RuntimeError::RecursivePlanViolation,
        DistributedQueryError::PayloadLimit => {
            RuntimeError::ChildTransportBudgetExceeded("payload")
        }
        DistributedQueryError::CreditExhausted => {
            RuntimeError::ChildTransportBudgetExceeded("credit")
        }
        DistributedQueryError::SequenceExhausted => {
            RuntimeError::ChildTransportBudgetExceeded("sequence")
        }
        DistributedQueryError::UnexpectedShard(_) => {
            RuntimeError::ChildTransportProtocolViolation("unexpected shard")
        }
        DistributedQueryError::UnexpectedSequence { .. } => {
            RuntimeError::ChildTransportProtocolViolation("unexpected sequence")
        }
        DistributedQueryError::UnsupportedExchange => {
            RuntimeError::ChildTransportProtocolViolation("unsupported exchange")
        }
        _ => RuntimeError::ChildInvocationFailed,
    }
}

fn distributed_runtime_error(error: RuntimeError) -> DistributedQueryError {
    match error {
        RuntimeError::DeadlineExceeded => DistributedQueryError::DeadlineExceeded,
        RuntimeError::MissingShards(shards) => DistributedQueryError::MissingShards(shards),
        RuntimeError::ChildSnapshotMismatch => DistributedQueryError::SnapshotMismatch,
        RuntimeError::ChildCapabilityGenerationMismatch => {
            DistributedQueryError::CapabilityGenerationMismatch
        }
        RuntimeError::ChildSecurityMismatch => DistributedQueryError::SecurityMismatch,
        RuntimeError::Cancelled => DistributedQueryError::Cancelled,
        RuntimeError::ChildIncompleteShards(shards) => {
            DistributedQueryError::IncompleteShards(shards)
        }
        RuntimeError::ChildWorkerIdentityMismatch => DistributedQueryError::WorkerIdentityMismatch,
        RuntimeError::ChildFragmentMismatch => DistributedQueryError::FragmentMismatch,
        RuntimeError::ChildStorageFailure => DistributedQueryError::StorageFailure,
        RuntimeError::MemoryLimitExceeded { limit, required } => {
            DistributedQueryError::MemoryLimitExceeded { limit, required }
        }
        RuntimeError::ApplyInvocationLimit { max } => {
            DistributedQueryError::ApplyInvocationLimit { max }
        }
        RuntimeError::ApplyOutputRowLimit { max } => {
            DistributedQueryError::ApplyOutputRowLimit { max }
        }
        RuntimeError::RecursivePlanViolation => DistributedQueryError::RecursivePlanViolation,
        RuntimeError::ChildTransportBudgetExceeded("payload") => {
            DistributedQueryError::PayloadLimit
        }
        RuntimeError::ChildTransportBudgetExceeded("credit") => {
            DistributedQueryError::CreditExhausted
        }
        RuntimeError::ChildTransportBudgetExceeded("sequence") => {
            DistributedQueryError::SequenceExhausted
        }
        RuntimeError::ChildTransportProtocolViolation(_) => {
            DistributedQueryError::UnsupportedExchange
        }
        error => DistributedQueryError::Execution(error.to_string()),
    }
}

fn distributed_plan_error(error: physical_plan::ValidationError) -> DistributedQueryError {
    match error {
        physical_plan::ValidationError::ApplyDepthExceeded
        | physical_plan::ValidationError::DuplicateChildPlanIdentity(_)
        | physical_plan::ValidationError::InvalidApplyArgumentSource
        | physical_plan::ValidationError::RecursivePlanNodeLimit => {
            DistributedQueryError::RecursivePlanViolation
        }
        other => DistributedQueryError::Execution(other.to_string()),
    }
}

fn argument_input_for_fragment(
    fragment: &PlanFragment,
    input: Option<&RecordBatch>,
) -> Result<Vec<RecordBatch>, DistributedQueryError> {
    let Some(input) = input else {
        return Ok(Vec::new());
    };
    match fragment.operators().first() {
        Some(PhysicalOperator::Argument { output }) if output == input.schema() => {
            Ok(vec![input.clone()])
        }
        Some(PhysicalOperator::Argument { .. }) => Err(DistributedQueryError::FragmentMismatch),
        _ => Ok(Vec::new()),
    }
}

fn argument_interval_input_for_fragment(
    fragment: &PlanFragment,
    input: Option<&(RowSchema, Vec<TemporalRow>)>,
    source_free_region: TemporalRegion,
) -> Result<(RowSchema, Vec<TemporalRow>), DistributedQueryError> {
    let Some((input_schema, input_rows)) = input else {
        return Ok((
            RowSchema::empty(),
            vec![TemporalRow::new(Vec::new(), source_free_region)],
        ));
    };
    match fragment.operators().first() {
        Some(PhysicalOperator::Argument { output }) if output == input_schema => {
            Ok((input_schema.clone(), input_rows.clone()))
        }
        Some(PhysicalOperator::Argument { .. }) => Err(DistributedQueryError::FragmentMismatch),
        _ => Ok((RowSchema::empty(), Vec::new())),
    }
}

fn first_row_short_circuit_safe(plan: &PhysicalPlan) -> bool {
    plan.fragments().iter().all(|fragment| {
        fragment.operators().iter().all(|operator| match operator {
            PhysicalOperator::Argument { .. }
            | PhysicalOperator::NodeScan { .. }
            | PhysicalOperator::RelationshipScan { .. }
            | PhysicalOperator::Project { .. }
            | PhysicalOperator::TemporalSlice { .. }
            | PhysicalOperator::Finish => true,
            PhysicalOperator::HashJoin {
                kind: physical_plan::JoinKind::Inner,
                keys,
            } => keys.is_empty(),
            _ => false,
        })
    })
}

fn retain_first_batch_row(batches: &mut Vec<RecordBatch>) -> Result<(), DistributedQueryError> {
    let Some((schema, row)) = batches.iter().find_map(|batch| {
        batch
            .rows()
            .first()
            .map(|row| (batch.schema().clone(), row.clone()))
    }) else {
        batches.clear();
        return Ok(());
    };
    *batches = vec![
        RecordBatch::try_new(schema, vec![row])
            .map_err(|error| DistributedQueryError::Execution(error.to_string()))?,
    ];
    Ok(())
}

fn context_with_deadline(
    context: &ExecutionContext,
    deadline_unix_ms: u64,
) -> Result<ExecutionContext, DistributedQueryError> {
    if deadline_unix_ms == u64::MAX {
        return Ok(context.clone());
    }
    let now_unix_ms = u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| DistributedQueryError::DeadlineExceeded)?
            .as_millis(),
    )
    .map_err(|_| DistributedQueryError::DeadlineExceeded)?;
    let remaining = deadline_unix_ms
        .checked_sub(now_unix_ms)
        .ok_or(DistributedQueryError::DeadlineExceeded)?;
    let deadline = Instant::now()
        .checked_add(Duration::from_millis(remaining))
        .ok_or(DistributedQueryError::DeadlineExceeded)?;
    if context.deadline().is_some_and(|parent| parent <= deadline) {
        Ok(context.clone())
    } else {
        Ok(context.clone().with_deadline(deadline))
    }
}

async fn await_with_fences<T, F>(
    future: F,
    context: &ExecutionContext,
) -> Result<T, DistributedQueryError>
where
    F: Future<Output = Result<T, DistributedQueryError>>,
{
    if context.is_cancelled() {
        return Err(DistributedQueryError::Cancelled);
    }
    if context
        .deadline()
        .is_some_and(|deadline| deadline <= Instant::now())
    {
        return Err(DistributedQueryError::DeadlineExceeded);
    }
    if tokio::runtime::Handle::try_current().is_err() {
        let mut worker = std::pin::pin!(future);
        let mut cancelled = std::pin::pin!(context.cancelled());
        let mut deadline_wake: Option<Arc<DeadlineWake>> = None;
        return std::future::poll_fn(move |cx| {
            if context.is_cancelled() || cancelled.as_mut().poll(cx).is_ready() {
                return Poll::Ready(Err(DistributedQueryError::Cancelled));
            }
            if context
                .deadline()
                .is_some_and(|deadline| deadline <= Instant::now())
            {
                return Poll::Ready(Err(DistributedQueryError::DeadlineExceeded));
            }
            if let Poll::Ready(result) = worker.as_mut().poll(cx) {
                return Poll::Ready(result);
            }
            if let Some(deadline) = context.deadline() {
                let wake = deadline_wake.get_or_insert_with(|| DeadlineWake::start(deadline));
                wake.install(cx.waker());
                if wake.reached.load(Ordering::Acquire) {
                    return Poll::Ready(Err(DistributedQueryError::DeadlineExceeded));
                }
            }
            Poll::Pending
        })
        .await;
    }
    if let Some(deadline) = context.deadline() {
        tokio::select! {
            result = future => result,
            () = context.cancelled() => Err(DistributedQueryError::Cancelled),
            () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                Err(DistributedQueryError::DeadlineExceeded)
            }
        }
    } else {
        tokio::select! {
            result = future => result,
            () = context.cancelled() => Err(DistributedQueryError::Cancelled),
        }
    }
}

struct DeadlineWake {
    reached: AtomicBool,
    waker: Mutex<Option<Waker>>,
}

impl DeadlineWake {
    fn start(deadline: Instant) -> Arc<Self> {
        let wake = Arc::new(Self {
            reached: AtomicBool::new(false),
            waker: Mutex::new(None),
        });
        let background = Arc::clone(&wake);
        std::thread::spawn(move || {
            if let Some(remaining) = deadline.checked_duration_since(Instant::now()) {
                std::thread::sleep(remaining);
            }
            background.reached.store(true, Ordering::Release);
            if let Ok(mut waker) = background.waker.lock()
                && let Some(waker) = waker.take()
            {
                waker.wake();
            }
        });
        wake
    }

    fn install(&self, waker: &Waker) {
        if let Ok(mut current) = self.waker.lock()
            && current
                .as_ref()
                .is_none_or(|registered| !registered.will_wake(waker))
        {
            *current = Some(waker.clone());
        }
    }
}

fn bounded_temporal_union(
    inputs: Vec<(RowSchema, Vec<TemporalRow>)>,
    schema: &RowSchema,
    all: bool,
    memory_limit: u64,
) -> Result<Vec<TemporalRow>, DistributedQueryError> {
    let mut rows = Vec::new();
    let mut required = 0_u64;
    for (_, input_rows) in inputs {
        for row in input_rows {
            let row_bytes = RecordBatch::try_new(schema.clone(), vec![row.values().to_vec()])
                .map_err(|error| DistributedQueryError::Execution(error.to_string()))?
                .estimated_bytes();
            required = required
                .checked_add(row_bytes)
                .ok_or(DistributedQueryError::PayloadLimit)?;
            if required > memory_limit {
                return Err(memory_limit_error(memory_limit, required));
            }
            rows.push(row);
        }
    }
    Ok(if all {
        rows
    } else {
        distinct_temporal_rows(rows)
    })
}

#[allow(clippy::too_many_arguments)]
fn bounded_row_join(
    left: &[RecordBatch],
    left_schema: &RowSchema,
    right: &[RecordBatch],
    right_schema: &RowSchema,
    kind: &physical_plan::JoinKind,
    keys: &[temporal_ir::SlotId],
    output: &RowSchema,
    memory_limit: u64,
) -> Result<Vec<RecordBatch>, DistributedQueryError> {
    let left_positions = keys
        .iter()
        .map(|slot| {
            left_schema
                .columns()
                .iter()
                .position(|column| column.slot() == *slot)
                .ok_or(DistributedQueryError::FragmentMismatch)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let right_positions = keys
        .iter()
        .map(|slot| {
            right_schema
                .columns()
                .iter()
                .position(|column| column.slot() == *slot)
                .ok_or(DistributedQueryError::FragmentMismatch)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let left_slots = left_schema
        .columns()
        .iter()
        .enumerate()
        .map(|(index, column)| (column.slot(), index))
        .collect::<BTreeMap<_, _>>();
    let right_slots = right_schema
        .columns()
        .iter()
        .enumerate()
        .map(|(index, column)| (column.slot(), index))
        .collect::<BTreeMap<_, _>>();
    let right_rows = right
        .iter()
        .flat_map(|batch| batch.rows())
        .collect::<Vec<_>>();
    let mut rows = Vec::new();
    let mut required = 0_u64;
    for left_row in left.iter().flat_map(|batch| batch.rows()) {
        let mut matched = false;
        for right_row in &right_rows {
            if left_positions
                .iter()
                .zip(&right_positions)
                .any(|(left, right)| left_row[*left] != right_row[*right])
            {
                continue;
            }
            matched = true;
            let row = join_values(left_row, Some(right_row), &left_slots, &right_slots, output)?;
            retain_join_row(&mut rows, &mut required, row, output, memory_limit)?;
        }
        if !matched && matches!(kind, physical_plan::JoinKind::Left) {
            let row = join_values(left_row, None, &left_slots, &right_slots, output)?;
            retain_join_row(&mut rows, &mut required, row, output, memory_limit)?;
        }
    }
    let mut batches = if rows.is_empty() {
        vec![
            RecordBatch::try_new(output.clone(), Vec::new())
                .map_err(|error| DistributedQueryError::Execution(error.to_string()))?,
        ]
    } else {
        rows.chunks(MAX_BATCH_ROWS)
            .map(|rows| {
                RecordBatch::try_new(output.clone(), rows.to_vec())
                    .map_err(|error| DistributedQueryError::Execution(error.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?
    };
    if batches.is_empty() {
        batches.push(
            RecordBatch::try_new(output.clone(), Vec::new())
                .map_err(|error| DistributedQueryError::Execution(error.to_string()))?,
        );
    }
    Ok(batches)
}

fn retain_join_row(
    rows: &mut Vec<Vec<RuntimeValue>>,
    required: &mut u64,
    row: Vec<RuntimeValue>,
    output: &RowSchema,
    memory_limit: u64,
) -> Result<(), DistributedQueryError> {
    let row_bytes = RecordBatch::try_new(output.clone(), vec![row.clone()])
        .map_err(distributed_runtime_error)?
        .estimated_bytes();
    let next = required
        .checked_add(row_bytes)
        .ok_or(DistributedQueryError::PayloadLimit)?;
    if next > memory_limit {
        return Err(memory_limit_error(memory_limit, next));
    }
    *required = next;
    rows.push(row);
    Ok(())
}

fn join_values(
    left: &[RuntimeValue],
    right: Option<&&Vec<RuntimeValue>>,
    left_slots: &BTreeMap<temporal_ir::SlotId, usize>,
    right_slots: &BTreeMap<temporal_ir::SlotId, usize>,
    output: &RowSchema,
) -> Result<Vec<RuntimeValue>, DistributedQueryError> {
    output
        .columns()
        .iter()
        .map(|column| {
            if let Some(index) = left_slots.get(&column.slot()) {
                return left
                    .get(*index)
                    .cloned()
                    .ok_or(DistributedQueryError::FragmentMismatch);
            }
            let index = right_slots
                .get(&column.slot())
                .ok_or(DistributedQueryError::FragmentMismatch)?;
            Ok(right
                .and_then(|row| row.get(*index))
                .cloned()
                .unwrap_or(RuntimeValue::Null))
        })
        .collect()
}

fn memory_limit_error(limit: u64, required: u64) -> DistributedQueryError {
    DistributedQueryError::MemoryLimitExceeded { limit, required }
}

async fn poll_one_morsel<'a>(
    worker_id: u32,
    mut source: Box<dyn crate::WorkerMorselSource + Send + 'a>,
    max_frame_bytes: u64,
    metrics: Option<Arc<QueryExecutionMetrics>>,
) -> (
    u32,
    Box<dyn crate::WorkerMorselSource + Send + 'a>,
    Result<Option<WorkerBatch>, DistributedQueryError>,
) {
    let result = source.next(max_frame_bytes).await.and_then(|batch| {
        if batch.as_ref().is_some_and(|batch| {
            u64::try_from(batch.frame_bytes().len()).map_or(true, |bytes| bytes > max_frame_bytes)
        }) {
            Err(DistributedQueryError::CreditExhausted)
        } else {
            if batch.is_some()
                && let Some(metrics) = &metrics
            {
                metrics.record_sent_frame();
            }
            Ok(batch)
        }
    });
    (worker_id, source, result)
}

type PendingMorselFuture<'a> = Pin<
    Box<
        dyn Future<
                Output = (
                    u32,
                    Box<dyn crate::WorkerMorselSource + Send + 'a>,
                    Result<Option<WorkerBatch>, DistributedQueryError>,
                ),
            > + Send
            + 'a,
    >,
>;

struct BoundedMorselFanIn<'a> {
    ready: VecDeque<(u32, Box<dyn crate::WorkerMorselSource + Send + 'a>)>,
    pending: FuturesUnordered<PendingMorselFuture<'a>>,
    max_inflight: usize,
    max_frame_bytes: u64,
    metrics: Option<Arc<QueryExecutionMetrics>>,
}

impl<'a> BoundedMorselFanIn<'a> {
    fn new(
        sources: Vec<(u32, Box<dyn crate::WorkerMorselSource + Send + 'a>)>,
        max_inflight: usize,
        max_buffered_bytes: u64,
    ) -> Result<Self, DistributedQueryError> {
        let frame_pool_bytes = max_buffered_bytes / 2;
        let max_frame_bytes = frame_pool_bytes / u64::try_from(max_inflight).unwrap_or(u64::MAX);
        if sources.is_empty() || max_inflight == 0 || max_frame_bytes == 0 {
            return Err(DistributedQueryError::InvalidCoordinator);
        }
        Ok(Self {
            ready: sources.into(),
            pending: FuturesUnordered::new(),
            max_inflight,
            max_frame_bytes,
            metrics: None,
        })
    }

    fn with_query_metrics(mut self, metrics: Option<Arc<QueryExecutionMetrics>>) -> Self {
        self.metrics = metrics;
        self
    }

    async fn next(&mut self) -> Result<Option<(u32, WorkerBatch)>, DistributedQueryError> {
        loop {
            while self.pending.len() < self.max_inflight {
                let Some((worker_id, source)) = self.ready.pop_front() else {
                    break;
                };
                self.pending.push(Box::pin(poll_one_morsel(
                    worker_id,
                    source,
                    self.max_frame_bytes,
                    self.metrics.clone(),
                )));
            }
            let Some((worker_id, source, result)) = self.pending.next().await else {
                return Ok(None);
            };
            if let Some(batch) = result? {
                if batch.has_more() {
                    self.ready.push_back((worker_id, source));
                }
                return Ok(Some((worker_id, batch)));
            }
        }
    }
}

struct TypedStream {
    next_sequence: u64,
    complete: bool,
}

struct TypedBatchMerger {
    snapshot: SnapshotToken,
    schema: RowSchema,
    codec_limits: ExchangeCodecLimits,
    max_buffered_bytes: u64,
    max_inflight_batches: usize,
    streams: BTreeMap<u32, TypedStream>,
    query_metrics: Option<Arc<QueryExecutionMetrics>>,
    #[cfg(any(test, feature = "test-support"))]
    metrics: Option<Arc<crate::ExchangeTestMetrics>>,
}

impl TypedBatchMerger {
    fn new(
        shards: Vec<u32>,
        snapshot: SnapshotToken,
        schema: RowSchema,
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
                    },
                )
            })
            .collect();
        Ok(Self {
            snapshot,
            schema,
            codec_limits: ExchangeCodecLimits::new(
                MAX_BATCH_ROWS,
                usize::try_from(max_buffered_bytes).unwrap_or(usize::MAX),
            )?,
            max_buffered_bytes,
            max_inflight_batches,
            streams,
            query_metrics: None,
            #[cfg(any(test, feature = "test-support"))]
            metrics: crate::current_exchange_test_metrics(),
        })
    }

    fn with_query_metrics(mut self, metrics: Option<Arc<QueryExecutionMetrics>>) -> Self {
        self.query_metrics = metrics;
        self
    }

    fn push(&mut self, batch: WorkerBatch) -> Result<ColumnBatch, DistributedQueryError> {
        if batch.snapshot_fingerprint() != self.snapshot.fingerprint() {
            return Err(DistributedQueryError::SnapshotMismatch);
        }
        let shard_id = batch.shard_id();
        let sequence = batch.sequence();
        let stream = self
            .streams
            .get_mut(&shard_id)
            .ok_or(DistributedQueryError::UnexpectedShard(shard_id))?;
        if stream.complete || sequence != stream.next_sequence {
            return Err(DistributedQueryError::UnexpectedSequence {
                shard_id,
                expected: stream.next_sequence,
                actual: sequence,
            });
        }
        let frame_bytes = u64::try_from(batch.frame_bytes().len())
            .map_err(|_| DistributedQueryError::PayloadLimit)?;
        let frame_reservation = frame_bytes;
        if self.max_inflight_batches == 0 || frame_reservation > self.max_buffered_bytes {
            return Err(DistributedQueryError::CreditExhausted);
        }
        let decoded_upper_bound = batch.decoded_bytes_upper_bound(
            stream.next_sequence,
            self.snapshot.clone(),
            self.schema.clone(),
            self.codec_limits,
        )?;
        let conversion_upper_bound = decoded_upper_bound
            .checked_mul(2)
            .ok_or(DistributedQueryError::PayloadLimit)?;
        let decode_pool_bytes = self
            .max_buffered_bytes
            .checked_sub(self.max_buffered_bytes / 2)
            .ok_or(DistributedQueryError::PayloadLimit)?;
        if conversion_upper_bound > decode_pool_bytes {
            return Err(DistributedQueryError::CreditExhausted);
        }
        let transient_bytes = frame_reservation
            .checked_add(decoded_upper_bound)
            .ok_or(DistributedQueryError::PayloadLimit)?;
        if transient_bytes > self.max_buffered_bytes {
            return Err(DistributedQueryError::CreditExhausted);
        }
        if let Some(metrics) = &self.query_metrics {
            metrics.record_wire_decoded_bytes(frame_bytes);
            metrics.observe_retained_memory_bytes(transient_bytes);
        }
        let decoded = batch.decode(
            stream.next_sequence,
            self.snapshot.clone(),
            self.schema.clone(),
            self.codec_limits,
        )?;
        #[cfg(any(test, feature = "test-support"))]
        if let Some(metrics) = &self.metrics {
            metrics.reserve_decoded(decoded_upper_bound);
        }
        stream.next_sequence = stream
            .next_sequence
            .checked_add(1)
            .ok_or(DistributedQueryError::SequenceExhausted)?;
        stream.complete = !decoded.has_more();
        let batch = decoded.into_batch();
        if let Some(metrics) = &self.query_metrics {
            metrics.record_value_copy_bytes(batch.variable_width_bytes());
        }
        #[cfg(any(test, feature = "test-support"))]
        if let Some(metrics) = &self.metrics {
            metrics.release_decoded(decoded_upper_bound);
        }
        Ok(batch)
    }

    fn finish(self) -> Result<(), DistributedQueryError> {
        let incomplete = self
            .streams
            .iter()
            .filter_map(|(shard, stream)| (!stream.complete).then_some(*shard))
            .collect::<Vec<_>>();
        if !incomplete.is_empty() {
            return Err(DistributedQueryError::IncompleteShards(incomplete));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use physical_plan::{
        AccessGuarantee, MemoryBudget, PhysicalAccess, PhysicalPlanBuilder, PhysicalPlanHeader,
        PrimitiveKind, ResidualPolicy,
    };
    use std::collections::VecDeque;
    use std::sync::atomic::AtomicUsize;
    use std::task::{Context, Wake};
    use storage_api::{PushdownGuarantee, QueryPrimitiveCapabilities};
    use temporal_ir::{Column, SlotId, ValueType};

    struct CapabilityWorker {
        shard_id: u32,
        generation: u64,
        capabilities: QueryPrimitiveCapabilities,
    }

    impl FragmentWorker for CapabilityWorker {
        fn shard_id(&self) -> u32 {
            self.shard_id
        }

        fn query_capability_generation(&self) -> u64 {
            self.generation
        }

        fn query_primitive_capabilities(&self) -> QueryPrimitiveCapabilities {
            self.capabilities
        }

        fn execute_fragment<'a>(
            &'a self,
            _request: &'a FragmentRequest,
            _fragment: &'a PlanFragment,
            _valid_time: ValidTime,
            _context: &'a ExecutionContext,
        ) -> crate::WorkerFuture<'a> {
            Box::pin(async { Err(DistributedQueryError::StorageFailure) })
        }

        fn execute_interval_fragment<'a>(
            &'a self,
            _request: &'a FragmentRequest,
            _fragment: &'a PlanFragment,
            _window: Interval<ValidTime>,
            _context: &'a ExecutionContext,
        ) -> crate::TemporalWorkerFuture<'a> {
            Box::pin(async { Err(DistributedQueryError::StorageFailure) })
        }
    }

    #[test]
    fn coordinator_combines_worker_capabilities_conservatively_and_generationally() {
        let mut coordinator = DistributedCoordinator::new(1024, 2).unwrap();
        coordinator
            .register(Arc::new(CapabilityWorker {
                shard_id: 3,
                generation: 7,
                capabilities: QueryPrimitiveCapabilities::new(
                    PushdownGuarantee::Exact,
                    PushdownGuarantee::Unsupported,
                    PushdownGuarantee::Exact,
                    PushdownGuarantee::Candidate,
                ),
            }))
            .unwrap();
        coordinator
            .register(Arc::new(CapabilityWorker {
                shard_id: 9,
                generation: 11,
                capabilities: QueryPrimitiveCapabilities::new(
                    PushdownGuarantee::Candidate,
                    PushdownGuarantee::Unsupported,
                    PushdownGuarantee::Unsupported,
                    PushdownGuarantee::Exact,
                ),
            }))
            .unwrap();

        let snapshot = coordinator.capability_snapshot(&[3, 9]).unwrap();

        assert_ne!(snapshot.generation(), 0);
        assert_eq!(
            snapshot.capabilities().candidate_scan(),
            PushdownGuarantee::Candidate
        );
        assert_eq!(
            snapshot.capabilities().adjacency_expand(),
            PushdownGuarantee::Unsupported
        );
        assert_eq!(
            snapshot.capabilities().change_scan(),
            PushdownGuarantee::Candidate
        );
        let first_generation = snapshot.generation();

        coordinator.workers.insert(
            9,
            Arc::new(CapabilityWorker {
                shard_id: 9,
                generation: 12,
                capabilities: QueryPrimitiveCapabilities::new(
                    PushdownGuarantee::Candidate,
                    PushdownGuarantee::Unsupported,
                    PushdownGuarantee::Unsupported,
                    PushdownGuarantee::Exact,
                ),
            }),
        );
        assert_ne!(
            coordinator
                .capability_snapshot(&[3, 9])
                .unwrap()
                .generation(),
            first_generation
        );
    }

    #[test]
    fn primitive_plan_rejects_capability_generation_drift() {
        let capabilities = QueryPrimitiveCapabilities::new(
            PushdownGuarantee::Candidate,
            PushdownGuarantee::Unsupported,
            PushdownGuarantee::Unsupported,
            PushdownGuarantee::Unsupported,
        );
        let mut coordinator = DistributedCoordinator::new(1024, 1).unwrap();
        coordinator
            .register(Arc::new(CapabilityWorker {
                shard_id: 3,
                generation: 7,
                capabilities,
            }))
            .unwrap();
        let current = coordinator.capability_snapshot(&[3]).unwrap().generation();
        let header = PhysicalPlanHeader::new(1, 1, 1, [1; 32])
            .unwrap()
            .with_capability_generation(current.wrapping_add(1).max(1))
            .unwrap()
            .with_expected_shards(vec![3])
            .unwrap();
        let schema = RowSchema::new(vec![Column::new(
            SlotId::new(0),
            "n",
            ValueType::Node,
            false,
        )])
        .unwrap();
        let mut builder = PhysicalPlanBuilder::new(header);
        let root = builder
            .add_fragment_with_access(
                Placement::AllShards,
                vec![PhysicalOperator::NodeScan {
                    binding: SlotId::new(0),
                    labels: Vec::new(),
                    output: schema.clone(),
                }],
                vec![PhysicalAccess::Primitive {
                    primitive: PrimitiveKind::CandidateScan,
                    guarantee: AccessGuarantee::Candidate,
                    residual: ResidualPolicy::Evaluate,
                    constraints: Vec::new(),
                }],
                schema,
                MemoryBudget::new(1024, 1024).unwrap(),
            )
            .unwrap();
        let plan = builder.finish(root).unwrap();

        assert_eq!(
            coordinator.validate_plan_capabilities(&plan),
            Err(DistributedQueryError::CapabilityGenerationMismatch)
        );
    }

    #[test]
    fn primitive_request_reuses_the_generation_map_from_plan_validation() {
        let schema = RowSchema::new(vec![Column::new(
            SlotId::new(0),
            "n",
            ValueType::Node,
            false,
        )])
        .unwrap();
        let mut builder =
            PhysicalPlanBuilder::new(PhysicalPlanHeader::new(1, 1, 1, [1; 32]).unwrap());
        let root = builder
            .add_fragment_with_access(
                Placement::AllShards,
                vec![PhysicalOperator::NodeScan {
                    binding: SlotId::new(0),
                    labels: Vec::new(),
                    output: schema.clone(),
                }],
                vec![PhysicalAccess::Primitive {
                    primitive: PrimitiveKind::CandidateScan,
                    guarantee: AccessGuarantee::Candidate,
                    residual: ResidualPolicy::Evaluate,
                    constraints: Vec::new(),
                }],
                schema,
                MemoryBudget::new(1024, 1024).unwrap(),
            )
            .unwrap();
        let plan = builder.finish(root).unwrap();
        let request = FragmentRequest::new(
            root,
            SnapshotToken::new(1, 1, 1, temporal_types::TransactionTime::new(1, 0), [1; 32])
                .unwrap(),
            1,
            1024,
            1,
        )
        .unwrap()
        .with_expected_shards(vec![3])
        .unwrap();
        let validated = DistributedCapabilitySnapshot {
            generation: 99,
            capabilities: QueryPrimitiveCapabilities::NONE,
            shard_generations: BTreeMap::from([(3, 7)]),
        };

        let request =
            bind_fragment_capabilities(request, &plan.fragments()[0], Some(&validated)).unwrap();

        assert_eq!(request.expected_capability_generation(3), Some(7));
    }

    #[test]
    fn generic_request_does_not_allocate_a_capability_generation_map() {
        let schema = RowSchema::new(vec![Column::new(
            SlotId::new(0),
            "n",
            ValueType::Node,
            false,
        )])
        .unwrap();
        let mut builder =
            PhysicalPlanBuilder::new(PhysicalPlanHeader::new(1, 1, 1, [1; 32]).unwrap());
        let root = builder
            .add_fragment(
                Placement::AllShards,
                vec![PhysicalOperator::NodeScan {
                    binding: SlotId::new(0),
                    labels: Vec::new(),
                    output: schema.clone(),
                }],
                schema,
                MemoryBudget::new(1024, 1024).unwrap(),
            )
            .unwrap();
        let plan = builder.finish(root).unwrap();
        let request = FragmentRequest::new(
            root,
            SnapshotToken::new(1, 1, 1, temporal_types::TransactionTime::new(1, 0), [1; 32])
                .unwrap(),
            1,
            1024,
            1,
        )
        .unwrap()
        .with_expected_shards(vec![3])
        .unwrap();

        let request = bind_fragment_capabilities(request, &plan.fragments()[0], None).unwrap();

        assert!(request.expected_capability_generations().is_empty());
    }

    #[test]
    fn ordered_join_polls_every_source_before_waiting_for_the_first() {
        let starts = Arc::new(AtomicUsize::new(0));
        let first_starts = Arc::clone(&starts);
        let second_starts = Arc::clone(&starts);
        let futures: Vec<OrderedFuture<'_, u32>> = vec![
            Box::pin(std::future::poll_fn(move |cx| {
                first_starts.fetch_add(1, Ordering::SeqCst);
                if first_starts.load(Ordering::SeqCst) >= 2 {
                    Poll::Ready(Ok(1))
                } else {
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            })),
            Box::pin(async move {
                second_starts.fetch_add(1, Ordering::SeqCst);
                Ok(2)
            }),
        ];

        let results = block_on(join_ordered(futures)).unwrap();

        assert_eq!(results, vec![1, 2]);
        assert!(starts.load(Ordering::SeqCst) >= 2);
    }

    struct ScriptedMorselSource {
        batches: VecDeque<WorkerBatch>,
    }

    struct ConcurrencyObservedMorselSource {
        batch: Option<WorkerBatch>,
        active: Arc<AtomicUsize>,
        peak: Arc<AtomicUsize>,
        frame_credits: Arc<Mutex<Vec<u64>>>,
    }

    impl crate::WorkerMorselSource for ConcurrencyObservedMorselSource {
        fn next<'a>(&'a mut self, max_frame_bytes: u64) -> crate::WorkerMorselFuture<'a> {
            self.frame_credits.lock().unwrap().push(max_frame_bytes);
            Box::pin(async move {
                let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
                self.peak.fetch_max(active, Ordering::SeqCst);
                tokio::task::yield_now().await;
                self.active.fetch_sub(1, Ordering::SeqCst);
                Ok(self.batch.take())
            })
        }
    }

    impl crate::WorkerMorselSource for ScriptedMorselSource {
        fn next<'a>(&'a mut self, _max_frame_bytes: u64) -> crate::WorkerMorselFuture<'a> {
            Box::pin(async move {
                let batch = self.batches.pop_front();
                if batch.is_some() {
                    tokio::task::yield_now().await;
                }
                Ok(batch)
            })
        }
    }

    fn scripted_batch(
        shard_id: u32,
        sequence: u64,
        has_more: bool,
        snapshot: &SnapshotToken,
        schema: &RowSchema,
    ) -> WorkerBatch {
        let record = RecordBatch::try_new(
            schema.clone(),
            vec![vec![RuntimeValue::Integer(i64::from(shard_id))]],
        )
        .unwrap();
        let column = ColumnBatch::from_record_batch(&record).unwrap();
        let limits = ExchangeCodecLimits::new(MAX_BATCH_ROWS, 8 * 1024).unwrap();
        let frame =
            crate::ExchangeFrame::encode(shard_id, sequence, has_more, snapshot, &column, limits)
                .unwrap();
        WorkerBatch::from_frame_bytes(frame.as_bytes().to_vec(), 8 * 1024).unwrap()
    }

    #[tokio::test]
    async fn morsel_fan_in_requeues_each_source_after_one_batch() {
        let schema = RowSchema::new(vec![Column::new(
            SlotId::new(0),
            "value",
            ValueType::Integer,
            false,
        )])
        .unwrap();
        let snapshot =
            SnapshotToken::new(1, 1, 1, temporal_types::TransactionTime::new(1, 0), [1; 32])
                .unwrap();
        let mut pending = FuturesUnordered::new();
        pending.push(poll_one_morsel(
            1,
            Box::new(ScriptedMorselSource {
                batches: VecDeque::from([
                    scripted_batch(1, 0, true, &snapshot, &schema),
                    scripted_batch(1, 1, false, &snapshot, &schema),
                ]),
            }),
            8 * 1024,
            None,
        ));
        pending.push(poll_one_morsel(
            2,
            Box::new(ScriptedMorselSource {
                batches: VecDeque::from([
                    scripted_batch(2, 0, true, &snapshot, &schema),
                    scripted_batch(2, 1, false, &snapshot, &schema),
                ]),
            }),
            8 * 1024,
            None,
        ));

        let mut order = Vec::new();
        while let Some((shard, source, result)) = pending.next().await {
            if result.unwrap().is_some() {
                order.push(shard);
                pending.push(poll_one_morsel(shard, source, 8 * 1024, None));
            }
        }

        assert_eq!(order.len(), 4);
        assert_ne!(order[0], order[1]);
        assert_eq!(order.iter().filter(|shard| **shard == 1).count(), 2);
        assert_eq!(order.iter().filter(|shard| **shard == 2).count(), 2);
    }

    #[tokio::test]
    async fn morsel_fan_in_never_exceeds_its_inflight_credit() {
        let schema = RowSchema::new(vec![Column::new(
            SlotId::new(0),
            "value",
            ValueType::Integer,
            false,
        )])
        .unwrap();
        let snapshot =
            SnapshotToken::new(1, 1, 1, temporal_types::TransactionTime::new(1, 0), [1; 32])
                .unwrap();
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let frame_credits = Arc::new(Mutex::new(Vec::new()));
        let sources = (0..4)
            .map(|shard_id| {
                (
                    shard_id,
                    Box::new(ConcurrencyObservedMorselSource {
                        batch: Some(scripted_batch(shard_id, 0, false, &snapshot, &schema)),
                        active: Arc::clone(&active),
                        peak: Arc::clone(&peak),
                        frame_credits: Arc::clone(&frame_credits),
                    }) as Box<dyn crate::WorkerMorselSource + Send>,
                )
            })
            .collect::<Vec<_>>();
        let mut fan_in = BoundedMorselFanIn::new(sources, 2, 32 * 1024).unwrap();
        let mut shards = Vec::new();

        while let Some((shard_id, _batch)) = fan_in.next().await.unwrap() {
            shards.push(shard_id);
        }

        shards.sort_unstable();
        assert_eq!(shards, vec![0, 1, 2, 3]);
        assert_eq!(peak.load(Ordering::SeqCst), 2);
        assert_eq!(active.load(Ordering::SeqCst), 0);
        assert_eq!(*frame_credits.lock().unwrap(), vec![8 * 1024; 4]);
    }

    #[tokio::test]
    async fn morsel_fan_in_rejects_a_frame_larger_than_its_credit() {
        let schema = RowSchema::new(vec![Column::new(
            SlotId::new(0),
            "value",
            ValueType::Integer,
            false,
        )])
        .unwrap();
        let snapshot =
            SnapshotToken::new(1, 1, 1, temporal_types::TransactionTime::new(1, 0), [1; 32])
                .unwrap();
        let source = ScriptedMorselSource {
            batches: VecDeque::from([scripted_batch(1, 0, false, &snapshot, &schema)]),
        };
        let mut fan_in = BoundedMorselFanIn::new(
            vec![(
                1,
                Box::new(source) as Box<dyn crate::WorkerMorselSource + Send>,
            )],
            1,
            2,
        )
        .unwrap();

        assert_eq!(
            fan_in.next().await,
            Err(DistributedQueryError::CreditExhausted)
        );
    }

    #[test]
    fn typed_merger_releases_decoded_credit_after_each_consumed_batch() {
        let metrics = crate::install_exchange_test_metrics();
        let schema = RowSchema::new(vec![Column::new(
            SlotId::new(0),
            "value",
            ValueType::Integer,
            false,
        )])
        .unwrap();
        let snapshot =
            SnapshotToken::new(1, 1, 1, temporal_types::TransactionTime::new(1, 0), [1; 32])
                .unwrap();
        let mut merger =
            TypedBatchMerger::new(vec![7], snapshot.clone(), schema.clone(), 8 * 1024, 1).unwrap();

        let first = merger
            .push(scripted_batch(7, 0, true, &snapshot, &schema))
            .unwrap();
        assert_eq!(first.row(0), Some(vec![RuntimeValue::Integer(7)]));
        assert_eq!(metrics.retained_decoded_bytes(), 0);
        let second = merger
            .push(scripted_batch(7, 1, false, &snapshot, &schema))
            .unwrap();
        assert_eq!(second.row(0), Some(vec![RuntimeValue::Integer(7)]));
        assert_eq!(metrics.retained_decoded_bytes(), 0);
        merger.finish().unwrap();
    }

    fn block_on<F: Future>(future: F) -> F::Output {
        struct Noop;

        impl Wake for Noop {
            fn wake(self: Arc<Self>) {}
        }

        let waker = Waker::from(Arc::new(Noop));
        let mut context = Context::from_waker(&waker);
        let mut future = Box::pin(future);
        loop {
            match future.as_mut().poll(&mut context) {
                Poll::Ready(output) => return output,
                Poll::Pending => std::thread::yield_now(),
            }
        }
    }

    #[test]
    fn keyed_inner_join_is_not_first_row_short_circuit_safe() {
        let schema = RowSchema::new(vec![Column::new(
            SlotId::new(0),
            "key",
            ValueType::Integer,
            false,
        )])
        .unwrap();
        let mut builder =
            PhysicalPlanBuilder::new(PhysicalPlanHeader::new(1, 1, 1, [1; 32]).unwrap());
        let left = builder
            .add_fragment(
                Placement::AllShards,
                vec![PhysicalOperator::NodeScan {
                    binding: SlotId::new(0),
                    labels: Vec::new(),
                    output: schema.clone(),
                }],
                schema.clone(),
                MemoryBudget::new(1024, 1024).unwrap(),
            )
            .unwrap();
        let right = builder
            .add_fragment(
                Placement::AllShards,
                vec![PhysicalOperator::NodeScan {
                    binding: SlotId::new(0),
                    labels: Vec::new(),
                    output: schema.clone(),
                }],
                schema.clone(),
                MemoryBudget::new(1024, 1024).unwrap(),
            )
            .unwrap();
        let join = builder
            .add_fragment(
                Placement::Coordinator,
                vec![PhysicalOperator::HashJoin {
                    kind: physical_plan::JoinKind::Inner,
                    keys: vec![SlotId::new(0)],
                }],
                schema.clone(),
                MemoryBudget::new(1024, 1024).unwrap(),
            )
            .unwrap();
        builder
            .add_exchange(left, join, ExchangeKind::Gather, schema.clone(), 1)
            .unwrap();
        builder
            .add_exchange(right, join, ExchangeKind::Gather, schema, 1)
            .unwrap();
        let plan = builder.finish(join).unwrap();

        assert!(!first_row_short_circuit_safe(&plan));
    }

    #[test]
    fn join_rejects_the_first_exceeding_row_before_retention() {
        let schema = RowSchema::new(vec![Column::new(
            SlotId::new(0),
            "value",
            ValueType::Integer,
            false,
        )])
        .unwrap();
        let mut rows = Vec::new();
        let mut required = 0;

        let error = retain_join_row(
            &mut rows,
            &mut required,
            vec![RuntimeValue::Integer(1)],
            &schema,
            0,
        )
        .expect_err("the first row must be rejected before ownership transfer");

        assert!(matches!(
            error,
            DistributedQueryError::MemoryLimitExceeded {
                limit: 0,
                required: _
            }
        ));
        assert!(rows.is_empty());
        assert_eq!(required, 0);
    }

    #[test]
    fn child_errors_preserve_stable_typed_categories() {
        assert_eq!(
            distributed_child_error(DistributedQueryError::PayloadLimit),
            RuntimeError::ChildTransportBudgetExceeded("payload")
        );
        assert_eq!(
            distributed_child_error(DistributedQueryError::CreditExhausted),
            RuntimeError::ChildTransportBudgetExceeded("credit")
        );
        assert_eq!(
            distributed_child_error(DistributedQueryError::SnapshotMismatch),
            RuntimeError::ChildSnapshotMismatch
        );
        assert_eq!(
            distributed_child_error(DistributedQueryError::SecurityMismatch),
            RuntimeError::ChildSecurityMismatch
        );
        assert_eq!(
            distributed_child_error(DistributedQueryError::Cancelled),
            RuntimeError::Cancelled
        );
        assert_eq!(
            distributed_child_error(DistributedQueryError::MemoryLimitExceeded {
                limit: 10,
                required: 11,
            }),
            RuntimeError::MemoryLimitExceeded {
                limit: 10,
                required: 11,
            }
        );
        assert_eq!(
            distributed_child_error(DistributedQueryError::ApplyInvocationLimit { max: 3 }),
            RuntimeError::ApplyInvocationLimit { max: 3 }
        );
        assert_eq!(
            distributed_child_error(DistributedQueryError::ApplyOutputRowLimit { max: 4 }),
            RuntimeError::ApplyOutputRowLimit { max: 4 }
        );
        assert_eq!(
            distributed_child_error(DistributedQueryError::IncompleteShards(vec![2])),
            RuntimeError::ChildIncompleteShards(vec![2])
        );
        assert_eq!(
            distributed_child_error(DistributedQueryError::WorkerIdentityMismatch),
            RuntimeError::ChildWorkerIdentityMismatch
        );
        assert_eq!(
            distributed_child_error(DistributedQueryError::FragmentMismatch),
            RuntimeError::ChildFragmentMismatch
        );
        assert_eq!(
            distributed_child_error(DistributedQueryError::StorageFailure),
            RuntimeError::ChildStorageFailure
        );
        assert_eq!(
            distributed_runtime_error(RuntimeError::MemoryLimitExceeded {
                limit: 10,
                required: 11,
            }),
            DistributedQueryError::MemoryLimitExceeded {
                limit: 10,
                required: 11,
            }
        );
        assert_eq!(
            distributed_runtime_error(RuntimeError::ApplyInvocationLimit { max: 3 }),
            DistributedQueryError::ApplyInvocationLimit { max: 3 }
        );
        assert_eq!(
            distributed_runtime_error(RuntimeError::ApplyOutputRowLimit { max: 4 }),
            DistributedQueryError::ApplyOutputRowLimit { max: 4 }
        );
    }

    #[test]
    fn typed_merger_releases_transport_credit_after_consuming_each_frame() {
        let metrics = crate::install_exchange_test_metrics();
        let schema = RowSchema::new(vec![Column::new(
            SlotId::new(0),
            "value",
            ValueType::Integer,
            false,
        )])
        .unwrap();
        let snapshot =
            SnapshotToken::new(1, 1, 1, temporal_types::TransactionTime::new(1, 0), [1; 32])
                .unwrap();
        let batch =
            RecordBatch::try_new(schema.clone(), vec![vec![RuntimeValue::Integer(7)]]).unwrap();
        let column_batch = ColumnBatch::from_record_batch(&batch).unwrap();
        let limits = ExchangeCodecLimits::new(MAX_BATCH_ROWS, 8 * 1024).unwrap();
        let first_frame =
            crate::ExchangeFrame::encode(1, 0, true, &snapshot, &column_batch, limits).unwrap();
        let first =
            WorkerBatch::from_frame_bytes(first_frame.as_bytes().to_vec(), 8 * 1024).unwrap();
        let second_frame =
            crate::ExchangeFrame::encode(1, 1, false, &snapshot, &column_batch, limits).unwrap();
        let second =
            WorkerBatch::from_frame_bytes(second_frame.as_bytes().to_vec(), 8 * 1024).unwrap();
        let mut merger = TypedBatchMerger::new(vec![1], snapshot, schema, 8 * 1024, 1).unwrap();

        assert_eq!(merger.push(first).unwrap().row_count(), 1);
        assert_eq!(metrics.retained_decoded_bytes(), 0);
        assert_eq!(merger.push(second).unwrap().row_count(), 1);

        merger.finish().unwrap();
        assert_eq!(metrics.retained_decoded_bytes(), 0);
        assert!(metrics.peak_retained_decoded_bytes() > 0);
    }
}
