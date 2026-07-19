#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{self, Display, Formatter};

use cypher_compiler::{CompileSession, CompiledQuery, CypherCompiler};
use distributed_query::{DistributedCoordinator, SnapshotTokenV2};
use query_executor::v2::{
    ExecutionContext, MAX_BATCH_ROWS, RecordBatch, ResolvedTemporalScope, ResolvedValidTime,
    RuntimeValue, resolve_temporal_scope,
};
pub use query_optimizer::DeploymentMode;
use query_optimizer::{Optimizer, OptimizerContext};
use temporal_ir::v2::{LogicalOperator, RowSchema, TransactionTimeSpec, ValidTimeSpec};
use temporal_storage::GraphId;
use temporal_types::{TransactionTime, ValidTime};

mod bolt;
mod bolt_service;
mod write;

pub use bolt::{BoltValueError, bolt_parameter_to_runtime, runtime_value_to_bolt};
pub use bolt_service::{
    BackendFuture, BackendQueryResult, BoltQueryBackend, BoltQueryRequest, CypherBoltService,
};
pub use write::{
    MaterializedElement, MaterializedElementKind, MaterializedWriteSet, ScopedWrite, WriteContext,
    WriteMaterializationError, materialize_write, schema_id,
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CypherQueryRequest {
    text: String,
    parameters: BTreeMap<String, RuntimeValue>,
    current_valid_time: ValidTime,
    current_transaction_time: TransactionTime,
    security_fingerprint: [u8; 32],
    deadline_unix_ms: u64,
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
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CypherQueryResponse {
    fingerprint: [u8; 32],
    schema: RowSchema,
    batches: Vec<RecordBatch>,
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
        let compiled = CypherCompiler::new()
            .compile(&request.text, &session)
            .map_err(|error| EngineError::Compile(error.to_string()))?;
        if !compiled.is_read_only() {
            return Err(EngineError::WriteQueryUnsupported);
        }
        let (valid_time, transaction_time) = temporal_spec(compiled.logical_plan())?;
        let context = ExecutionContext::new(request.parameters);
        let resolved = resolve_temporal_scope(
            GraphId::new(self.config.graph_id),
            &valid_time,
            &transaction_time,
            request.current_valid_time,
            request.current_transaction_time,
            &context,
        )
        .map_err(|error| EngineError::Temporal(error.to_string()))?;
        let valid_time = match resolved.valid_time() {
            ResolvedValidTime::Point(value) => value,
            ResolvedValidTime::Interval { .. } => {
                return Err(EngineError::IntervalExecutionUnsupported);
            }
        };
        let optimizer_context = OptimizerContext::new(
            self.config.deployment,
            u32::try_from(self.config.shard_ids.len())
                .map_err(|_| EngineError::InvalidConfiguration)?,
            self.config.limits.memory_bytes,
            self.config.limits.spill_bytes,
        )
        .map_err(|error| EngineError::Optimize(error.to_string()))?
        .with_primary_shard(self.config.shard_ids[0]);
        let optimized = Optimizer::new()
            .optimize(compiled.logical_plan(), optimizer_context)
            .map_err(|error| EngineError::Optimize(error.to_string()))?;
        let snapshot = SnapshotTokenV2::new(
            self.config.graph_id,
            self.config.schema_version,
            self.config.topology_epoch,
            resolved.transaction_time(),
            request.security_fingerprint,
        )
        .map_err(|error| EngineError::Distributed(error.to_string()))?;
        let batches = coordinator
            .execute_plan(
                optimized.plan(),
                snapshot,
                valid_time,
                request.deadline_unix_ms,
                self.config.limits.batch_rows,
                &context,
            )
            .await
            .map_err(|error| EngineError::Distributed(error.to_string()))?;
        Ok(CypherQueryResponse {
            fingerprint: compiled.fingerprint(),
            schema: compiled.result_schema().clone(),
            batches,
            optimizer_trace: optimized
                .trace()
                .iter()
                .map(|event| format!("{}: {}", event.rule(), event.detail()))
                .collect(),
        })
    }
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
    plan: &temporal_ir::v2::LogicalPlan,
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
    match scopes.as_slice() {
        [] => Ok((ValidTimeSpec::Current, TransactionTimeSpec::Current)),
        [scope] => Ok(scope.clone()),
        _ => Err(EngineError::AmbiguousTemporalScope),
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EngineError {
    InvalidConfiguration,
    InvalidRequest,
    Compile(String),
    WriteQueryUnsupported,
    AmbiguousTemporalScope,
    Temporal(String),
    IntervalExecutionUnsupported,
    Optimize(String),
    Distributed(String),
}

impl Display for EngineError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "Cypher query engine failed: {self:?}")
    }
}

impl Error for EngineError {}
