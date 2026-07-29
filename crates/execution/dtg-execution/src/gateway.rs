use std::collections::BTreeMap;

use dtg_analytics::{
    AnalyticsArtifactRepository, AnalyticsJobError, AnalyticsLedger, AnalyticsProjectionProvider,
    AnalyticsScheduler, AnalyticsSchedulerTick, AnalyticsStepProvider, CancellationToken,
    JobTimestamp, ProjectionError, ShardSnapshotProvenance, SnapshotProvenance,
};
use dtg_language::{Language, LanguageError, LogicalProgram};
use dtg_language_ir::{TemporalScope, TimeExpr, ValidTimeExpr, ValidTimePredicate};
use dtg_plan::{
    ExchangeKind, LogicalReadOperation, LogicalReadRequest, PhysicalExpr, PhysicalPlan, Planner,
    PlanningContext, SemanticRequirements, StorageAccess,
};
use dtg_query::{
    CancellationToken as QueryCancellationToken, ExecutableAccess, ExecutableFragment,
    ExecutablePlan, ExecutionFence, LogicalRead, QueryBudget, QueryError, QueryOverlay,
    QueryRuntime, QueryStorage, QueryStream, ReadOperation, ResidualPredicate, SnapshotGuard,
    SnapshotShardFence,
};
use dtg_storage::{PushdownOperation, ShardId, TransactionId, Version};
use dtg_transaction::{
    ParticipantWrite, ShardSnapshotFence, SnapshotToken, TemporalTxnCoordinator,
    TransactionContext, TransactionOutcome, TxnFuture,
};

use crate::ExecutionBuildError;

trait AnalyticsRuntime {
    fn tick(
        &mut self,
        ledger: &mut AnalyticsLedger,
        now: JobTimestamp,
        cancellation: &CancellationToken,
    ) -> Result<AnalyticsSchedulerTick, AnalyticsJobError>;
}

impl<P, A, R> AnalyticsRuntime for AnalyticsScheduler<P, A, R>
where
    P: AnalyticsProjectionProvider + 'static,
    A: AnalyticsStepProvider + 'static,
    R: AnalyticsArtifactRepository + 'static,
{
    fn tick(
        &mut self,
        ledger: &mut AnalyticsLedger,
        now: JobTimestamp,
        cancellation: &CancellationToken,
    ) -> Result<AnalyticsSchedulerTick, AnalyticsJobError> {
        AnalyticsScheduler::tick(self, ledger, now, cancellation)
    }
}

pub struct GatewayExecution {
    language: Language,
    planner: Planner,
    query: QueryRuntime,
    transactions: TemporalTxnCoordinator,
    analytics: Box<dyn AnalyticsRuntime>,
}

impl GatewayExecution {
    pub fn builder() -> GatewayExecutionBuilder {
        GatewayExecutionBuilder::default()
    }

    pub fn compile(&self, source: &str) -> Result<LogicalProgram, LanguageError> {
        self.language.compile(source)
    }

    pub fn plan(
        &self,
        program: &LogicalProgram,
        context: &PlanningContext,
    ) -> Result<PhysicalPlan, dtg_plan::PlanError> {
        self.planner.plan(program, context)
    }

    pub fn lower_plan(&self, plan: &PhysicalPlan) -> Result<ExecutablePlan, QueryError> {
        validate_exchanges(plan)?;
        let fragments = plan
            .fragments()
            .iter()
            .map(lower_fragment)
            .collect::<Result<Vec<_>, _>>()?;
        ExecutablePlan::new(plan.version, fragments, plan.result_schema().clone())
    }

    pub fn tick_analytics(
        &mut self,
        ledger: &mut AnalyticsLedger,
        now: JobTimestamp,
        cancellation: &CancellationToken,
    ) -> Result<AnalyticsSchedulerTick, AnalyticsJobError> {
        self.analytics.tick(ledger, now, cancellation)
    }

    pub fn query_snapshot(&self, token: &SnapshotToken) -> Result<SnapshotGuard, QueryError> {
        SnapshotGuard::new(
            token.start_time,
            token.catalog_version,
            token
                .shards
                .iter()
                .map(|(shard_id, fence)| {
                    (
                        *shard_id,
                        SnapshotShardFence {
                            placement_epoch: fence.placement_epoch,
                            backend_generation: fence.backend_generation,
                            applied_index: fence.applied_index,
                        },
                    )
                })
                .collect(),
        )
    }

    pub fn analytics_provenance(
        &self,
        token: &SnapshotToken,
    ) -> Result<SnapshotProvenance, ProjectionError> {
        SnapshotProvenance::new(
            token.transaction_id,
            token.start_time,
            token.catalog_version,
            token
                .shards
                .iter()
                .map(|(shard_id, fence)| {
                    (
                        *shard_id,
                        ShardSnapshotProvenance {
                            placement_epoch: fence.placement_epoch,
                            backend_generation: fence.backend_generation,
                            applied_index: fence.applied_index,
                            closed_time: fence.closed_time,
                        },
                    )
                })
                .collect(),
        )
    }

    pub async fn execute_query(
        &self,
        plan: &ExecutablePlan,
        storage: BTreeMap<ShardId, QueryStorage>,
        snapshot: &SnapshotGuard,
        budget: QueryBudget,
        cancellation: QueryCancellationToken,
        overlay: Option<QueryOverlay>,
    ) -> Result<QueryStream, QueryError> {
        self.query
            .execute(plan, storage, snapshot, budget, cancellation, overlay)
            .await
    }

    pub fn begin_transaction(
        &self,
        transaction_id: TransactionId,
        catalog_version: Version,
        shard_fences: Vec<(ShardId, ShardSnapshotFence)>,
    ) -> TxnFuture<'_, TransactionContext> {
        self.transactions
            .begin(transaction_id, catalog_version, shard_fences)
    }

    pub fn commit_transaction<'a>(
        &'a self,
        context: &'a TransactionContext,
        participants: Vec<ParticipantWrite>,
    ) -> TxnFuture<'a, TransactionOutcome> {
        self.transactions.commit(context, participants)
    }

    pub fn abort_transaction<'a>(
        &'a self,
        context: &'a TransactionContext,
        participants: Vec<ParticipantWrite>,
    ) -> TxnFuture<'a, TransactionOutcome> {
        self.transactions.abort(context, participants)
    }
}

#[derive(Default)]
pub struct GatewayExecutionBuilder {
    language: Option<Language>,
    planner: Option<Planner>,
    query: Option<QueryRuntime>,
    transactions: Option<TemporalTxnCoordinator>,
    analytics: Option<Box<dyn AnalyticsRuntime>>,
}

impl GatewayExecutionBuilder {
    pub fn with_language(mut self, language: Language) -> Self {
        self.language = Some(language);
        self
    }

    pub fn with_planner(mut self, planner: Planner) -> Self {
        self.planner = Some(planner);
        self
    }

    pub fn with_query(mut self, query: QueryRuntime) -> Self {
        self.query = Some(query);
        self
    }

    pub fn with_transactions(mut self, transactions: TemporalTxnCoordinator) -> Self {
        self.transactions = Some(transactions);
        self
    }

    pub fn with_analytics_scheduler<P, A, R>(
        mut self,
        analytics: AnalyticsScheduler<P, A, R>,
    ) -> Self
    where
        P: AnalyticsProjectionProvider + 'static,
        A: AnalyticsStepProvider + 'static,
        R: AnalyticsArtifactRepository + 'static,
    {
        self.analytics = Some(Box::new(analytics));
        self
    }

    pub fn build(self) -> Result<GatewayExecution, ExecutionBuildError> {
        Ok(GatewayExecution {
            language: self
                .language
                .ok_or(ExecutionBuildError::MissingComponent("language"))?,
            planner: self
                .planner
                .ok_or(ExecutionBuildError::MissingComponent("planner"))?,
            query: self
                .query
                .ok_or(ExecutionBuildError::MissingComponent("query runtime"))?,
            transactions: self
                .transactions
                .ok_or(ExecutionBuildError::MissingComponent(
                    "transaction coordinator",
                ))?,
            analytics: self
                .analytics
                .ok_or(ExecutionBuildError::MissingComponent("analytics scheduler"))?,
        })
    }
}

fn validate_exchanges(plan: &PhysicalPlan) -> Result<(), QueryError> {
    if plan.exchanges().len() != plan.fragments().len() {
        return Err(QueryError::InvalidPlan(
            "physical exchanges do not cover every fragment".into(),
        ));
    }
    for fragment in plan.fragments() {
        let matching = plan.exchanges().iter().filter(|exchange| {
            exchange.source() == fragment.id() && exchange.kind() == ExchangeKind::Gather
        });
        if matching.count() != 1 {
            return Err(QueryError::InvalidPlan(
                "physical exchange identity drifted from its fragment".into(),
            ));
        }
    }
    Ok(())
}

fn lower_fragment(fragment: &dtg_plan::PlanFragment) -> Result<ExecutableFragment, QueryError> {
    let planned_fence = fragment.fence();
    let snapshot = planned_fence.snapshot_requirements();
    let fence = ExecutionFence::new(
        planned_fence.read_fence().clone(),
        planned_fence.catalog_version(),
        planned_fence.schema_version(),
        snapshot.transaction_time(),
        snapshot.valid_at(),
        snapshot.immutable(),
    )?;
    let accesses = fragment
        .storage_accesses()
        .iter()
        .map(|access| lower_access(access, planned_fence))
        .collect::<Result<Vec<_>, _>>()?;
    ExecutableFragment::new(fragment.id().get(), fence, accesses)
}

fn lower_access(
    access: &StorageAccess,
    fence: &dtg_plan::PlanFence,
) -> Result<ExecutableAccess, QueryError> {
    match access {
        StorageAccess::Logical(request) => lower_logical_read(request, fence),
        StorageAccess::Pushdown {
            request,
            guarantee,
            residual,
        } => lower_pushdown(request, *guarantee, residual.as_ref(), fence),
    }
}

fn lower_logical_read(
    request: &LogicalReadRequest,
    fence: &dtg_plan::PlanFence,
) -> Result<ExecutableAccess, QueryError> {
    let snapshot = fence.snapshot_requirements();
    let (transaction_time, valid_at) = resolve_read_scope(request, snapshot)?;
    if transaction_time != snapshot.transaction_time() || valid_at != snapshot.valid_at() {
        return Err(QueryError::SnapshotDrift);
    }
    let operation = match request.operation() {
        LogicalReadOperation::VertexPoint(id) => ReadOperation::VertexPoint(*id),
        LogicalReadOperation::VertexScan => ReadOperation::VertexScan,
        LogicalReadOperation::EdgePoint(id) => ReadOperation::EdgePoint(*id),
        LogicalReadOperation::EdgeScan => ReadOperation::EdgeScan,
        LogicalReadOperation::Adjacency { .. } => {
            return Err(QueryError::Unsupported(
                "logical adjacency identity is not executable".into(),
            ));
        }
    };
    Ok(ExecutableAccess::Logical(LogicalRead::new(
        operation,
        request.row_bound(),
        transaction_time,
        valid_at,
    )?))
}

fn resolve_read_scope(
    request: &LogicalReadRequest,
    snapshot: &dtg_plan::SnapshotRequirements,
) -> Result<(dtg_storage::TransactionTime, i64), QueryError> {
    let transaction_time = match &request.read_scope().transaction_time {
        TemporalScope::Current => snapshot.transaction_time(),
        TemporalScope::AsOf(TimeExpr::Literal(value)) => *value,
        TemporalScope::AsOf(TimeExpr::Parameter(_)) | TemporalScope::Changes { .. } => {
            return Err(QueryError::Unsupported(
                "logical read scope must be resolved before execution".into(),
            ));
        }
    };
    let valid_at = match &request.read_scope().valid_time {
        None => snapshot.valid_at(),
        Some(ValidTimePredicate::At(ValidTimeExpr::Literal(value))) => *value,
        Some(ValidTimePredicate::At(ValidTimeExpr::Parameter(_)))
        | Some(ValidTimePredicate::Overlaps(_))
        | Some(ValidTimePredicate::Changes { .. }) => {
            return Err(QueryError::Unsupported(
                "logical read scope must be resolved before execution".into(),
            ));
        }
    };
    Ok((transaction_time, valid_at))
}

fn lower_pushdown(
    request: &dtg_storage::PushdownRequest,
    guarantee: dtg_plan::PushdownGuarantee,
    residual: Option<&PhysicalExpr>,
    fence: &dtg_plan::PlanFence,
) -> Result<ExecutableAccess, QueryError> {
    request.validate()?;
    if request.fence() != fence.read_fence() {
        return Err(QueryError::SnapshotDrift);
    }
    let snapshot = fence.snapshot_requirements();
    let (transaction_time, valid_at) = match request.operation() {
        PushdownOperation::Vertex(read) => (read.transaction_at(), read.valid_at()),
        PushdownOperation::VertexScan(read) => (read.transaction_at(), read.valid_at()),
    };
    if transaction_time != snapshot.transaction_time() || valid_at != snapshot.valid_at() {
        return Err(QueryError::SnapshotDrift);
    }
    let residual = match (guarantee.is_exact(), residual) {
        (true, None) => None,
        (false, Some(PhysicalExpr::VerifyStorageSemantics { requirements, .. }))
            if *requirements == SemanticRequirements::exact() =>
        {
            Some(ResidualPredicate::StorageSemantics)
        }
        _ => {
            return Err(QueryError::InvalidPlan(
                "pushdown guarantees and execution residual drifted".into(),
            ));
        }
    };
    Ok(ExecutableAccess::Pushdown {
        request: Box::new(request.clone()),
        residual,
    })
}
