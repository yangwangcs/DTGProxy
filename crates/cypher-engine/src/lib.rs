#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use analytics_api::ProjectedGraph;
use analytics_ledger::GraphProjectionScope;
use cypher_compiler::{CompileSession, CompiledQuery, CypherCompiler};
use distributed_query::{DistributedCoordinator, SnapshotToken};
use procedure_runtime::{JobInvocationContext, ProcedureAccess, ProcedureRegistry};
use query_executor::{
    CancellationToken, ExecutionContext, GraphOverlay, MAX_BATCH_ROWS, RecordBatch,
    ResolvedTemporalScope, ResolvedValidTime, RuntimeValue, TemporalRow, resolve_temporal_scope,
};
pub use query_optimizer::DeploymentMode;
use query_optimizer::{Optimizer, OptimizerContext};
use temporal_ir::{LogicalOperator, LogicalPlan, RowSchema, TransactionTimeSpec, ValidTimeSpec};
use temporal_storage::GraphId;
use temporal_types::{Interval, TransactionTime, ValidTime};

mod bolt;
mod bolt_service;
mod write;

pub use bolt::{BoltValueError, bolt_parameter_to_runtime, runtime_value_to_bolt};
pub use bolt_service::{
    BackendFuture, BackendQueryResult, BoltQueryBackend, BoltQueryRequest, CypherBoltService,
};
pub use write::{
    MaterializedElement, MaterializedElementKind, MaterializedWriteSet, MergeConstraint,
    ScopedWrite, WriteContext, WriteMaterializationError, WriteSubqueryInput, WriteSubqueryRow,
    materialize_write, materialize_write_with_subquery_inputs, probe_merge_constraints,
    probe_merge_constraints_with_subquery_inputs, schema_id,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResourceLimits {
    memory_bytes: u64,
    spill_bytes: u64,
    batch_rows: u32,
}

impl ResourceLimits {
    pub fn new(memory_bytes: u64, spill_bytes: u64, batch_rows: u32) -> Result<Self, EngineError> {
        if memory_bytes == 0
            || spill_bytes == 0
            || batch_rows == 0
            || usize::try_from(batch_rows).unwrap_or(usize::MAX) > MAX_BATCH_ROWS
        {
            return Err(EngineError::InvalidConfiguration);
        }
        Ok(Self {
            memory_bytes,
            spill_bytes,
            batch_rows,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EngineConfig {
    graph_name: String,
    graph_id: u64,
    schema_version: u64,
    topology_epoch: u64,
    deployment: DeploymentMode,
    shard_ids: Vec<u32>,
    limits: ResourceLimits,
}

impl EngineConfig {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        graph_name: impl Into<String>,
        graph_id: u64,
        schema_version: u64,
        topology_epoch: u64,
        deployment: DeploymentMode,
        shard_ids: Vec<u32>,
        limits: ResourceLimits,
    ) -> Result<Self, EngineError> {
        let graph_name = graph_name.into();
        let unique = shard_ids.iter().copied().collect::<BTreeSet<_>>();
        let deployment_is_valid = match deployment {
            DeploymentMode::PrimaryReplica => shard_ids.len() == 1,
            DeploymentMode::SharedNothing => shard_ids.len() >= 2,
        };
        if graph_name.is_empty()
            || graph_id == 0
            || schema_version == 0
            || topology_epoch == 0
            || unique.len() != shard_ids.len()
            || !deployment_is_valid
        {
            return Err(EngineError::InvalidConfiguration);
        }
        Ok(Self {
            graph_name,
            graph_id,
            schema_version,
            topology_epoch,
            deployment,
            shard_ids,
            limits,
        })
    }
}

#[derive(Clone, Debug)]
pub struct CypherQueryRequest {
    text: String,
    parameters: BTreeMap<String, RuntimeValue>,
    current_valid_time: ValidTime,
    current_transaction_time: TransactionTime,
    security_fingerprint: [u8; 32],
    deadline_unix_ms: u64,
    cancellation: CancellationToken,
    graph_overlay: GraphOverlay,
    fixed_transaction_snapshot: Option<TransactionTime>,
    procedure_registry: Option<Arc<ProcedureRegistry>>,
    procedure_graph: Option<Arc<ProjectedGraph>>,
    procedure_access: ProcedureAccess,
    job_invocation_context: Option<JobInvocationContext>,
}

impl CypherQueryRequest {
    pub fn new(
        text: impl Into<String>,
        parameters: BTreeMap<String, RuntimeValue>,
        current_valid_time: ValidTime,
        current_transaction_time: TransactionTime,
        security_fingerprint: [u8; 32],
        deadline_unix_ms: u64,
    ) -> Self {
        Self {
            text: text.into(),
            parameters,
            current_valid_time,
            current_transaction_time,
            security_fingerprint,
            deadline_unix_ms,
            cancellation: CancellationToken::new(),
            graph_overlay: GraphOverlay::default(),
            fixed_transaction_snapshot: None,
            procedure_registry: None,
            procedure_graph: None,
            procedure_access: ProcedureAccess::denied(),
            job_invocation_context: None,
        }
    }

    #[must_use]
    pub fn with_graph_overlay(mut self, graph_overlay: GraphOverlay) -> Self {
        self.graph_overlay = graph_overlay;
        self
    }

    #[must_use]
    pub fn with_fixed_transaction_snapshot(mut self, snapshot: TransactionTime) -> Self {
        self.fixed_transaction_snapshot = Some(snapshot);
        self
    }

    #[must_use]
    pub fn with_cancellation(mut self, cancellation: CancellationToken) -> Self {
        self.cancellation = cancellation;
        self
    }

    #[must_use]
    pub fn with_procedure_runtime(
        mut self,
        registry: Arc<ProcedureRegistry>,
        graph: Arc<ProjectedGraph>,
        access: ProcedureAccess,
    ) -> Self {
        self.procedure_registry = Some(registry);
        self.procedure_graph = Some(graph);
        self.procedure_access = access;
        self
    }

    #[must_use]
    pub fn with_procedure_runtime_optional_graph(
        mut self,
        registry: Arc<ProcedureRegistry>,
        graph: Option<Arc<ProjectedGraph>>,
        access: ProcedureAccess,
    ) -> Self {
        self.procedure_registry = Some(registry);
        self.procedure_graph = graph;
        self.procedure_access = access;
        self
    }

    #[must_use]
    pub fn with_job_invocation_context(mut self, context: JobInvocationContext) -> Self {
        self.job_invocation_context = Some(context);
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CypherQueryResponse {
    fingerprint: [u8; 32],
    schema: RowSchema,
    batches: Vec<RecordBatch>,
    temporal_rows: Option<Vec<TemporalRow>>,
    optimizer_trace: Vec<String>,
}

impl CypherQueryResponse {
    #[must_use]
    pub const fn fingerprint(&self) -> [u8; 32] {
        self.fingerprint
    }

    #[must_use]
    pub const fn schema(&self) -> &RowSchema {
        &self.schema
    }

    #[must_use]
    pub fn batches(&self) -> &[RecordBatch] {
        &self.batches
    }

    #[must_use]
    pub fn temporal_rows(&self) -> Option<&[TemporalRow]> {
        self.temporal_rows.as_deref()
    }

    #[must_use]
    pub fn row_count(&self) -> usize {
        self.batches.iter().map(|batch| batch.rows().len()).sum()
    }

    #[must_use]
    pub fn optimizer_trace(&self) -> &[String] {
        &self.optimizer_trace
    }
}

#[derive(Clone, Debug)]
pub struct CypherQueryEngine {
    config: EngineConfig,
}
impl CypherQueryEngine {
    #[must_use]
    pub const fn new(config: EngineConfig) -> Self {
        Self { config }
    }

    pub async fn execute(
        &self,
        coordinator: &DistributedCoordinator,
        request: CypherQueryRequest,
    ) -> Result<CypherQueryResponse, EngineError> {
        if request.text.is_empty()
            || request.security_fingerprint == [0; 32]
            || request.deadline_unix_ms == 0
        {
            return Err(EngineError::InvalidRequest);
        }
        let session = CompileSession::new(
            self.config.graph_name.clone(),
            self.config.graph_id,
            self.config.schema_version,
            self.config.topology_epoch,
        )
        .map_err(|error| EngineError::Compile(error.to_string()))?;
        let compiled = if let Some(registry) = request.procedure_registry.as_ref() {
            CypherCompiler::new().compile_with_procedures(
                &request.text,
                &session,
                registry.catalog(),
                &request.procedure_access,
            )
        } else {
            CypherCompiler::new().compile(&request.text, &session)
        }
        .map_err(|error| EngineError::Compile(error.to_string()))?;
        if compiled.uses_procedures() && request.procedure_registry.is_none() {
            return Err(EngineError::ProcedureRuntimeMissing);
        }
        if !compiled.is_read_only() {
            return Err(EngineError::WriteQueryUnsupported);
        }
        self.execute_logical_plan(
            coordinator,
            compiled.logical_plan(),
            compiled.fingerprint(),
            request,
            None,
        )
        .await
    }

    pub async fn execute_read_prefix(
        &self,
        coordinator: &DistributedCoordinator,
        compiled: &CompiledQuery,
        request: CypherQueryRequest,
    ) -> Result<CypherQueryResponse, EngineError> {
        if request.text.is_empty()
            || request.security_fingerprint == [0; 32]
            || request.deadline_unix_ms == 0
        {
            return Err(EngineError::InvalidRequest);
        }
        let prefix = compiled
            .read_prefix_plan()
            .ok_or(EngineError::WritePrefixMissing)?;
        self.execute_logical_plan(coordinator, &prefix, compiled.fingerprint(), request, None)
            .await
    }

    pub async fn execute_child_read_prefix(
        &self,
        coordinator: &DistributedCoordinator,
        logical_plan: &LogicalPlan,
        bindings: &BTreeMap<String, RuntimeValue>,
        fingerprint: [u8; 32],
        request: CypherQueryRequest,
    ) -> Result<CypherQueryResponse, EngineError> {
        if request.text.is_empty()
            || request.security_fingerprint == [0; 32]
            || request.deadline_unix_ms == 0
        {
            return Err(EngineError::InvalidRequest);
        }
        let argument_schema = logical_plan
            .nodes()
            .iter()
            .find(|node| {
                node.inputs().is_empty() && matches!(node.operator(), LogicalOperator::Argument)
            })
            .map(|node| node.output().clone())
            .ok_or(EngineError::ChildArgumentMissing)?;
        let values = argument_schema
            .columns()
            .iter()
            .map(|column| {
                bindings
                    .get(column.name())
                    .cloned()
                    .ok_or_else(|| EngineError::ChildImportMissing(column.name().to_owned()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        if bindings.len() != argument_schema.columns().len() {
            return Err(EngineError::ChildImportSchemaMismatch);
        }
        let input = RecordBatch::try_new(argument_schema, vec![values])
            .map_err(|error| EngineError::Distributed(error.to_string()))?;
        self.execute_logical_plan(coordinator, logical_plan, fingerprint, request, Some(input))
            .await
    }

    async fn execute_logical_plan(
        &self,
        coordinator: &DistributedCoordinator,
        logical_plan: &LogicalPlan,
        fingerprint: [u8; 32],
        request: CypherQueryRequest,
        argument_input: Option<RecordBatch>,
    ) -> Result<CypherQueryResponse, EngineError> {
        let deadline = request_deadline(request.deadline_unix_ms)?;
        let (valid_time, transaction_time) = temporal_spec(logical_plan)?;
        let has_graph_overlay = !request.graph_overlay.is_empty();
        let mut context = ExecutionContext::new(request.parameters)
            .with_cancellation(request.cancellation)
            .with_graph_overlay(request.graph_overlay);
        if let Some(deadline) = deadline {
            context = context.with_deadline(deadline);
        }
        if let Some(registry) = request.procedure_registry {
            context = context.with_procedure_runtime(
                registry,
                request.procedure_graph,
                request.security_fingerprint,
                request.procedure_access,
            );
        }
        let resolved = resolve_temporal_scope(
            GraphId::new(self.config.graph_id),
            &valid_time,
            &transaction_time,
            request.current_valid_time,
            request.current_transaction_time,
            &context,
        )
        .map_err(|error| EngineError::Temporal(error.to_string()))?;
        if request
            .fixed_transaction_snapshot
            .is_some_and(|snapshot| resolved.transaction_time() != snapshot)
        {
            return Err(EngineError::FixedSnapshotOverride);
        }
        let requires_job_context = logical_plan.nodes().iter().any(|node| {
            let LogicalOperator::ProcedureCall { procedure } = node.operator() else {
                return false;
            };
            matches!(
                procedure.name(),
                "dtg.analytics.submit"
                    | "dtg.analytics.status"
                    | "dtg.analytics.results"
                    | "dtg.analytics.cancel"
            )
        });
        validate_job_invocation_context(
            requires_job_context,
            request.job_invocation_context.as_ref(),
            &self.config,
            resolved.valid_time(),
            resolved.transaction_time(),
        )?;
        if let Some(job_context) = request.job_invocation_context {
            context = context.with_job_invocation_context(job_context);
        }
        let optimizer_context = OptimizerContext::new(
            self.config.deployment,
            u32::try_from(self.config.shard_ids.len())
                .map_err(|_| EngineError::InvalidConfiguration)?,
            self.config.limits.memory_bytes,
            self.config.limits.spill_bytes,
        )
        .map_err(|error| EngineError::Optimize(error.to_string()))?
        .with_shard_ids(self.config.shard_ids.clone())
        .map_err(|error| EngineError::Optimize(error.to_string()))?
        .with_primary_shard(self.config.shard_ids[0]);
        let optimized = Optimizer::new()
            .optimize(logical_plan, optimizer_context)
            .map_err(|error| EngineError::Optimize(error.to_string()))?;
        let snapshot = SnapshotToken::new(
            self.config.graph_id,
            self.config.schema_version,
            self.config.topology_epoch,
            resolved.transaction_time(),
            request.security_fingerprint,
        )
        .map_err(|error| EngineError::Distributed(error.to_string()))?;
        let (schema, batches, temporal_rows) = match resolved.valid_time() {
            ResolvedValidTime::Point(valid_time) => {
                let batches = if let Some(input) = argument_input {
                    coordinator
                        .execute_child_plan(
                            optimized.plan(),
                            snapshot,
                            valid_time,
                            request.deadline_unix_ms,
                            self.config.limits.batch_rows,
                            &context,
                            input,
                        )
                        .await
                } else {
                    coordinator
                        .execute_plan(
                            optimized.plan(),
                            snapshot,
                            valid_time,
                            request.deadline_unix_ms,
                            self.config.limits.batch_rows,
                            &context,
                        )
                        .await
                }
                .map_err(|error| EngineError::Distributed(error.to_string()))?;
                let schema = batches.first().map_or_else(
                    || logical_plan.output().clone(),
                    |batch| batch.schema().clone(),
                );
                (schema, batches, None)
            }
            ResolvedValidTime::Interval { start, end } => {
                if argument_input.is_some() {
                    return Err(EngineError::ChildIntervalPrefixUnsupported);
                }
                if has_graph_overlay {
                    return Err(EngineError::IntervalOverlayUnsupported);
                }
                let window = Interval::new(start, Some(end))
                    .map_err(|error| EngineError::Temporal(error.to_string()))?;
                let rows = coordinator
                    .execute_interval_plan(
                        optimized.plan(),
                        snapshot,
                        window,
                        request.deadline_unix_ms,
                        self.config.limits.batch_rows,
                        &context,
                    )
                    .await
                    .map_err(|error| EngineError::Distributed(error.to_string()))?;
                let schema = logical_plan.output().clone();
                let batches = if rows.is_empty() {
                    vec![
                        RecordBatch::try_new(schema.clone(), Vec::new())
                            .map_err(|error| EngineError::Distributed(error.to_string()))?,
                    ]
                } else {
                    rows.chunks(MAX_BATCH_ROWS)
                        .map(|chunk| {
                            RecordBatch::try_new(
                                schema.clone(),
                                chunk.iter().map(|row| row.values().to_vec()).collect(),
                            )
                            .map_err(|error| EngineError::Distributed(error.to_string()))
                        })
                        .collect::<Result<Vec<_>, _>>()?
                };
                (schema, batches, Some(rows))
            }
        };
        Ok(CypherQueryResponse {
            fingerprint,
            schema,
            batches,
            temporal_rows,
            optimizer_trace: optimized
                .trace()
                .iter()
                .map(|event| format!("{}: {}", event.rule(), event.detail()))
                .collect(),
        })
    }
}

fn request_deadline(deadline_unix_ms: u64) -> Result<Option<Instant>, EngineError> {
    if deadline_unix_ms == u64::MAX {
        return Ok(None);
    }
    let now_unix_ms = u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| EngineError::DeadlineExceeded)?
            .as_millis(),
    )
    .map_err(|_| EngineError::DeadlineExceeded)?;
    let remaining = deadline_unix_ms
        .checked_sub(now_unix_ms)
        .ok_or(EngineError::DeadlineExceeded)?;
    Instant::now()
        .checked_add(Duration::from_millis(remaining))
        .map(Some)
        .ok_or(EngineError::DeadlineExceeded)
}

pub fn resolve_compiled_temporal_scope(
    compiled: &CompiledQuery,
    graph_id: u64,
    current_valid_time: ValidTime,
    current_transaction_time: TransactionTime,
    parameters: BTreeMap<String, RuntimeValue>,
) -> Result<ResolvedTemporalScope, EngineError> {
    let (valid_time, transaction_time) = temporal_spec(compiled.logical_plan())?;
    resolve_temporal_scope(
        GraphId::new(graph_id),
        &valid_time,
        &transaction_time,
        current_valid_time,
        current_transaction_time,
        &ExecutionContext::new(parameters),
    )
    .map_err(|error| EngineError::Temporal(error.to_string()))
}

fn temporal_spec(
    plan: &temporal_ir::LogicalPlan,
) -> Result<(ValidTimeSpec, TransactionTimeSpec), EngineError> {
    let scopes = plan
        .nodes()
        .iter()
        .filter_map(|node| match node.operator() {
            LogicalOperator::TemporalSlice {
                valid_time,
                transaction_time,
            } => Some((valid_time.clone(), transaction_time.clone())),
            _ => None,
        })
        .collect::<Vec<_>>();
    let Some(first) = scopes.first() else {
        return Ok((ValidTimeSpec::Current, TransactionTimeSpec::Current));
    };
    if scopes.iter().all(|scope| scope == first) {
        Ok(first.clone())
    } else {
        Err(EngineError::AmbiguousTemporalScope)
    }
}

fn validate_job_invocation_context(
    required: bool,
    context: Option<&JobInvocationContext>,
    config: &EngineConfig,
    valid_time: ResolvedValidTime,
    transaction_time: TransactionTime,
) -> Result<(), EngineError> {
    let Some(context) = context else {
        return if required {
            Err(EngineError::JobContextMissing)
        } else {
            Ok(())
        };
    };
    let projection_matches = match (context.projection(), valid_time) {
        (
            GraphProjectionScope::Snapshot {
                valid_time: expected,
            },
            ResolvedValidTime::Point(actual),
        ) => *expected == actual,
        (GraphProjectionScope::Event, _) => true,
        (
            GraphProjectionScope::Interval {
                valid_from,
                valid_to,
            },
            ResolvedValidTime::Interval { start, end },
        ) => *valid_from == start && *valid_to == end,
        (
            GraphProjectionScope::Delta { before, after },
            ResolvedValidTime::Interval { start, end },
        ) => *before == start && *after == end,
        (
            GraphProjectionScope::Snapshot { .. }
            | GraphProjectionScope::Interval { .. }
            | GraphProjectionScope::Delta { .. },
            _,
        ) => false,
    };
    if context.graph_id() != config.graph_id
        || context.topology_epoch() != config.topology_epoch
        || context.schema_version() != config.schema_version
        || context.transaction_snapshot() != transaction_time
        || !projection_matches
    {
        return Err(EngineError::JobContextMismatch);
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EngineError {
    InvalidConfiguration,
    InvalidRequest,
    DeadlineExceeded,
    Compile(String),
    WriteQueryUnsupported,
    ProcedureRuntimeMissing,
    AmbiguousTemporalScope,
    Temporal(String),
    IntervalOverlayUnsupported,
    FixedSnapshotOverride,
    JobContextMissing,
    JobContextMismatch,
    WritePrefixMissing,
    ChildArgumentMissing,
    ChildImportMissing(String),
    ChildImportSchemaMismatch,
    ChildIntervalPrefixUnsupported,
    Optimize(String),
    Distributed(String),
}

impl Display for EngineError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "Cypher query engine failed: {self:?}")
    }
}

impl Error for EngineError {}

#[cfg(test)]
mod tests {
    use analytics_ledger::{GraphProjectionScope, ProjectionLimits};
    use procedure_runtime::JobInvocationContext;
    use query_executor::ResolvedValidTime;
    use temporal_types::{TransactionTime, ValidTime};

    use super::{
        CypherQueryRequest, DeploymentMode, EngineConfig, EngineError, ResourceLimits,
        validate_job_invocation_context,
    };

    fn job_context(
        graph_id: u64,
        topology_epoch: u64,
        schema_version: u64,
        transaction_snapshot: TransactionTime,
        projection: GraphProjectionScope,
    ) -> JobInvocationContext {
        JobInvocationContext::new(
            9,
            10,
            graph_id,
            12,
            topology_epoch,
            schema_version,
            15,
            transaction_snapshot,
            projection,
            ProjectionLimits::new(18, 19, 20).unwrap(),
        )
        .unwrap()
    }

    fn engine_config() -> EngineConfig {
        EngineConfig::new(
            "graph",
            11,
            14,
            13,
            DeploymentMode::PrimaryReplica,
            vec![1],
            ResourceLimits::new(1, 1, 1).unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn query_request_retains_the_resolved_job_invocation_context() {
        let context = job_context(
            11,
            13,
            14,
            TransactionTime::new(16, 0),
            GraphProjectionScope::Snapshot {
                valid_time: ValidTime::from_micros(17),
            },
        );
        let request = CypherQueryRequest::new(
            "RETURN 1",
            std::collections::BTreeMap::new(),
            ValidTime::from_micros(21),
            TransactionTime::new(22, 0),
            [1; 32],
            23,
        )
        .with_job_invocation_context(context.clone());

        assert_eq!(request.job_invocation_context, Some(context));
    }

    #[test]
    fn async_job_procedures_fail_closed_without_a_context() {
        assert_eq!(
            validate_job_invocation_context(
                true,
                None,
                &engine_config(),
                ResolvedValidTime::Point(ValidTime::from_micros(17)),
                TransactionTime::new(16, 0),
            ),
            Err(EngineError::JobContextMissing),
        );
    }

    #[test]
    fn job_context_must_match_engine_and_resolved_temporal_fences() {
        let config = engine_config();
        let transaction = TransactionTime::new(16, 0);
        let valid = ValidTime::from_micros(17);
        let matching = job_context(
            11,
            13,
            14,
            transaction,
            GraphProjectionScope::Snapshot { valid_time: valid },
        );
        assert_eq!(
            validate_job_invocation_context(
                true,
                Some(&matching),
                &config,
                ResolvedValidTime::Point(valid),
                transaction,
            ),
            Ok(()),
        );
        for mismatch in [
            job_context(
                99,
                13,
                14,
                transaction,
                GraphProjectionScope::Snapshot { valid_time: valid },
            ),
            job_context(
                11,
                99,
                14,
                transaction,
                GraphProjectionScope::Snapshot { valid_time: valid },
            ),
            job_context(
                11,
                13,
                99,
                transaction,
                GraphProjectionScope::Snapshot { valid_time: valid },
            ),
            job_context(
                11,
                13,
                14,
                TransactionTime::new(99, 0),
                GraphProjectionScope::Snapshot { valid_time: valid },
            ),
            job_context(
                11,
                13,
                14,
                transaction,
                GraphProjectionScope::Snapshot {
                    valid_time: ValidTime::from_micros(99),
                },
            ),
        ] {
            assert_eq!(
                validate_job_invocation_context(
                    true,
                    Some(&mismatch),
                    &config,
                    ResolvedValidTime::Point(valid),
                    transaction,
                ),
                Err(EngineError::JobContextMismatch),
            );
        }
    }
}
