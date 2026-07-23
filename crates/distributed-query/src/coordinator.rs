use std::collections::BTreeMap;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Poll, Waker};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use physical_plan::{ExchangeKind, PhysicalOperator, PhysicalPlan, Placement, PlanFragment};
use query_executor::{
    ApplyBudgetLedger, BatchExecutor, ChildInvocationFuture, ChildInvocationLimits,
    ChildOutputDemand, ChildPlanInvoker, ExecutionContext, IntervalChildInvocationFuture,
    MAX_BATCH_ROWS, RecordBatch, RuntimeError, RuntimeValue, TemporalRegion, TemporalRow,
    distinct_temporal_rows, ensure_temporal_rows_memory,
    execute_interval_coordinator_operators_with_invoker_and_ledger, temporal_hash_join_bounded,
    temporal_left_hash_join_bounded,
};
use temporal_ir::RowSchema;
use temporal_types::{Interval, ValidTime};

use crate::{DistributedQueryError, FragmentRequest, FragmentWorker, SnapshotToken, WorkerBatch};

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
        self.execute_with_demand(
            request,
            fragment,
            valid_time,
            context,
            ChildOutputDemand::AllRows,
        )
        .await
    }

    async fn execute_with_demand(
        &self,
        request: &FragmentRequest,
        fragment: &PlanFragment,
        valid_time: ValidTime,
        context: &ExecutionContext,
        demand: ChildOutputDemand,
    ) -> Result<Vec<RecordBatch>, DistributedQueryError> {
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
            request.snapshot().fingerprint(),
            self.max_buffered_bytes,
            self.max_inflight_batches,
        )?;
        for worker_id in worker_ids {
            let worker = self
                .workers
                .get(&worker_id)
                .ok_or(DistributedQueryError::InvalidCoordinator)?;
            let batches = await_with_fences(
                worker.execute_fragment(request, fragment, valid_time, context),
                context,
            )
            .await?;
            if demand == ChildOutputDemand::FirstVisibleRow
                && let Some((schema, row)) = batches.iter().find_map(|batch| {
                    batch
                        .batch()
                        .rows()
                        .first()
                        .map(|row| (batch.batch().schema().clone(), row.clone()))
                })
            {
                return Ok(vec![RecordBatch::try_new(schema, vec![row]).map_err(
                    |error| DistributedQueryError::Execution(error.to_string()),
                )?]);
            }
            for batch in batches {
                merger.push(batch)?;
            }
        }
        merger.finish()
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
                        .with_expected_shards(plan.header().expected_shards().to_vec())?;
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
                        .with_expected_shards(plan.header().expected_shards().to_vec())?;
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

fn distributed_child_error(error: DistributedQueryError) -> RuntimeError {
    match error {
        DistributedQueryError::DeadlineExceeded => RuntimeError::DeadlineExceeded,
        DistributedQueryError::MissingShards(shards) => RuntimeError::MissingShards(shards),
        DistributedQueryError::SnapshotMismatch => RuntimeError::ChildSnapshotMismatch,
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

#[cfg(test)]
mod tests {
    use super::*;
    use physical_plan::{MemoryBudget, PhysicalPlanBuilder, PhysicalPlanHeader};
    use temporal_ir::{Column, SlotId, ValueType};

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
}
