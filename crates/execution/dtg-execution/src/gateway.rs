use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use dtg_analytics::{
    AnalyticsArtifactRepository, AnalyticsJobError, AnalyticsLedger, AnalyticsProjectionProvider,
    AnalyticsScheduler, AnalyticsSchedulerTick, AnalyticsStepProvider, CancellationToken,
    JobTimestamp, ProjectionError, ShardSnapshotProvenance, SnapshotProvenance,
};
use dtg_cluster_v2::{
    PROTOCOL_MAJOR, SUPPORTED_MINOR_MAX, checksum_bytes, proto, validate_column_batch,
    validate_typed_status,
};
use dtg_language::{EmptySchemaCatalog, Language, LanguageError, LogicalProgram};
use dtg_language_ir::{
    LogicalNodeKind, LogicalPlan, LogicalStatement, TemporalScope, TimeExpr, ValidTimeExpr,
    ValidTimePredicate,
};
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
use tonic::transport::Channel;

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

pub type GatewayFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum GatewayValue {
    Null,
    Boolean(bool),
    Integer(i64),
    FloatBits(u64),
    Bytes(Vec<u8>),
    String(String),
    List(Vec<Self>),
    Map(BTreeMap<String, Self>),
}

impl From<bool> for GatewayValue {
    fn from(value: bool) -> Self {
        Self::Boolean(value)
    }
}

impl From<i64> for GatewayValue {
    fn from(value: i64) -> Self {
        Self::Integer(value)
    }
}

impl From<String> for GatewayValue {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<&str> for GatewayValue {
    fn from(value: &str) -> Self {
        Self::String(value.to_owned())
    }
}

impl From<Vec<u8>> for GatewayValue {
    fn from(value: Vec<u8>) -> Self {
        Self::Bytes(value)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GatewayRetry {
    Never,
    Safe,
    Replay,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GatewayExecutionError {
    code: String,
    message: String,
    retry: GatewayRetry,
}

impl GatewayExecutionError {
    pub fn new(code: impl Into<String>, message: impl Into<String>, retry: GatewayRetry) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            retry,
        }
    }

    pub fn code(&self) -> &str {
        &self.code
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub const fn retry(&self) -> GatewayRetry {
        self.retry
    }
}

impl std::fmt::Display for GatewayExecutionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for GatewayExecutionError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GatewayRequestContext {
    cluster_id: u64,
    request_id: u128,
    deadline_unix_ms: u64,
    trace_context: Vec<u8>,
}

impl GatewayRequestContext {
    pub fn new(
        cluster_id: u64,
        request_id: u128,
        deadline_unix_ms: u64,
        trace_context: Vec<u8>,
    ) -> Result<Self, GatewayExecutionError> {
        if cluster_id == 0 || request_id == 0 || deadline_unix_ms == 0 {
            return Err(GatewayExecutionError::new(
                "DTG-EXECUTION-CONTEXT",
                "cluster, request, and deadline identifiers must be non-zero",
                GatewayRetry::Never,
            ));
        }
        if trace_context.len() > 4 * 1024 {
            return Err(GatewayExecutionError::new(
                "DTG-EXECUTION-TRACE-LIMIT",
                "trace context exceeds 4096 bytes",
                GatewayRetry::Never,
            ));
        }
        Ok(Self {
            cluster_id,
            request_id,
            deadline_unix_ms,
            trace_context,
        })
    }

    pub const fn cluster_id(&self) -> u64 {
        self.cluster_id
    }

    pub const fn request_id(&self) -> u128 {
        self.request_id
    }

    pub const fn deadline_unix_ms(&self) -> u64 {
        self.deadline_unix_ms
    }

    pub fn trace_context(&self) -> &[u8] {
        &self.trace_context
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GatewayOperation {
    Query,
    Write,
    BeginTransaction,
    CommitTransaction,
    RollbackTransaction,
    SubmitAnalytics { algorithm: String },
    AnalyticsStatus { job_id: u128 },
    AnalyticsResult { job_id: u128 },
    CancelAnalytics { job_id: u128 },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GatewayTime {
    Literal(i64),
    Parameter(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GatewayTemporalMode {
    Current,
    AsOf(GatewayTime),
    Changes { from: GatewayTime, to: GatewayTime },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GatewayClusterRequest {
    context: GatewayRequestContext,
    operation: GatewayOperation,
    statement: Option<String>,
    parameters: BTreeMap<String, GatewayValue>,
    transaction_id: Option<u128>,
    result_fields: Vec<String>,
    temporal_mode: GatewayTemporalMode,
}

impl GatewayClusterRequest {
    pub const fn context(&self) -> &GatewayRequestContext {
        &self.context
    }

    pub const fn operation(&self) -> &GatewayOperation {
        &self.operation
    }

    pub fn statement(&self) -> Option<&str> {
        self.statement.as_deref()
    }

    pub const fn parameters(&self) -> &BTreeMap<String, GatewayValue> {
        &self.parameters
    }

    pub const fn transaction_id(&self) -> Option<u128> {
        self.transaction_id
    }

    pub fn result_fields(&self) -> &[String] {
        &self.result_fields
    }

    pub const fn temporal_mode(&self) -> &GatewayTemporalMode {
        &self.temporal_mode
    }

    pub const fn is_query(&self) -> bool {
        matches!(self.operation, GatewayOperation::Query)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GatewayRows {
    fields: Vec<String>,
    rows: Vec<Vec<GatewayValue>>,
}

impl GatewayRows {
    pub fn new(
        fields: Vec<String>,
        rows: Vec<Vec<GatewayValue>>,
    ) -> Result<Self, GatewayExecutionError> {
        if rows.iter().any(|row| row.len() != fields.len()) {
            return Err(GatewayExecutionError::new(
                "DTG-EXECUTION-ROW-WIDTH",
                "row width does not match result fields",
                GatewayRetry::Never,
            ));
        }
        Ok(Self { fields, rows })
    }

    pub fn fields(&self) -> &[String] {
        &self.fields
    }

    pub fn rows(&self) -> &[Vec<GatewayValue>] {
        &self.rows
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GatewayAnalyticsState {
    Queued,
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GatewayResponse {
    Rows(GatewayRows),
    Acknowledged,
    Transaction {
        transaction_id: u128,
    },
    AnalyticsSubmitted {
        job_id: u128,
    },
    AnalyticsStatus {
        job_id: u128,
        state: GatewayAnalyticsState,
    },
    AnalyticsResult {
        job_id: u128,
        rows: GatewayRows,
    },
    AnalyticsCancelled {
        job_id: u128,
    },
}

#[derive(Clone, Default)]
pub struct GatewayCancellationToken {
    cancelled: Arc<AtomicBool>,
}

impl GatewayCancellationToken {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
}

pub trait GatewayExecutionTransport: Send + Sync {
    fn execute(
        &self,
        request: GatewayClusterRequest,
    ) -> GatewayFuture<'_, Result<GatewayResponse, GatewayExecutionError>>;
}

pub trait GatewayProtocolV2Client: Send + Sync {
    fn execute(
        &self,
        request: proto::GatewayRequest,
    ) -> GatewayFuture<'_, Result<Vec<proto::GatewayResponse>, GatewayExecutionError>>;
}

pub trait GatewayExecutionTransportFactory: Send + Sync {
    fn connect<'a>(
        &'a self,
        endpoint: &'a str,
    ) -> GatewayFuture<'a, Result<Arc<dyn GatewayExecutionTransport>, GatewayExecutionError>>;
}

pub struct GatewayProtocolV2Transport {
    client: Arc<dyn GatewayProtocolV2Client>,
}

impl GatewayProtocolV2Transport {
    pub fn new(client: Arc<dyn GatewayProtocolV2Client>) -> Self {
        Self { client }
    }
}

pub struct TonicGatewayProtocolV2TransportFactory;

impl GatewayExecutionTransportFactory for TonicGatewayProtocolV2TransportFactory {
    fn connect<'a>(
        &'a self,
        endpoint: &'a str,
    ) -> GatewayFuture<'a, Result<Arc<dyn GatewayExecutionTransport>, GatewayExecutionError>> {
        Box::pin(async move {
            let client =
                proto::gateway_service_client::GatewayServiceClient::connect(endpoint.to_owned())
                    .await
                    .map_err(|error| {
                        GatewayExecutionError::new(
                            "DTG-CLUSTER-CONNECT",
                            error.to_string(),
                            GatewayRetry::Safe,
                        )
                    })?;
            let client: Arc<dyn GatewayProtocolV2Client> =
                Arc::new(TonicGatewayProtocolV2Client { client });
            Ok(Arc::new(GatewayProtocolV2Transport::new(client))
                as Arc<dyn GatewayExecutionTransport>)
        })
    }
}

#[derive(Clone)]
struct TonicGatewayProtocolV2Client {
    client: proto::gateway_service_client::GatewayServiceClient<Channel>,
}

impl GatewayProtocolV2Client for TonicGatewayProtocolV2Client {
    fn execute(
        &self,
        request: proto::GatewayRequest,
    ) -> GatewayFuture<'_, Result<Vec<proto::GatewayResponse>, GatewayExecutionError>> {
        let mut client = self.client.clone();
        Box::pin(async move {
            let mut stream = client
                .execute(request)
                .await
                .map_err(|error| {
                    GatewayExecutionError::new(
                        "DTG-CLUSTER-RPC",
                        error.to_string(),
                        GatewayRetry::Safe,
                    )
                })?
                .into_inner();
            let mut responses = Vec::new();
            while let Some(response) = stream.message().await.map_err(|error| {
                GatewayExecutionError::new(
                    "DTG-CLUSTER-STREAM",
                    error.to_string(),
                    GatewayRetry::Safe,
                )
            })? {
                if responses.len() >= 65_536 {
                    return Err(GatewayExecutionError::new(
                        "DTG-CLUSTER-STREAM-LIMIT",
                        "protocol v2 response stream exceeds 65536 messages",
                        GatewayRetry::Never,
                    ));
                }
                responses.push(response);
            }
            Ok(responses)
        })
    }
}

impl GatewayExecutionTransport for GatewayProtocolV2Transport {
    fn execute(
        &self,
        request: GatewayClusterRequest,
    ) -> GatewayFuture<'_, Result<GatewayResponse, GatewayExecutionError>> {
        let operation = request.operation.clone();
        let wire_request = encode_protocol_v2_request(&request);
        Box::pin(async move {
            let responses = self.client.execute(wire_request).await?;
            decode_protocol_v2_responses(responses, &operation)
        })
    }
}

enum GatewayExecutionMode {
    Composed {
        planner: Planner,
        query: QueryRuntime,
        transactions: TemporalTxnCoordinator,
        analytics: Box<dyn AnalyticsRuntime>,
    },
    Process {
        transport: Arc<dyn GatewayExecutionTransport>,
    },
}

pub struct GatewayExecution {
    language: Language,
    mode: GatewayExecutionMode,
}

impl GatewayExecution {
    pub fn builder() -> GatewayExecutionBuilder {
        GatewayExecutionBuilder::default()
    }

    pub fn for_process(transport: Arc<dyn GatewayExecutionTransport>) -> Self {
        Self {
            language: Language::new(Arc::new(EmptySchemaCatalog)),
            mode: GatewayExecutionMode::Process { transport },
        }
    }

    pub fn compile(&self, source: &str) -> Result<LogicalProgram, LanguageError> {
        self.language.compile(source)
    }

    pub fn plan(
        &self,
        program: &LogicalProgram,
        context: &PlanningContext,
    ) -> Result<PhysicalPlan, dtg_plan::PlanError> {
        let GatewayExecutionMode::Composed { planner, .. } = &self.mode else {
            panic!("process GatewayExecution does not expose low-level planning")
        };
        planner.plan(program, context)
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
        let GatewayExecutionMode::Composed { analytics, .. } = &mut self.mode else {
            panic!("process GatewayExecution does not expose scheduler internals")
        };
        analytics.tick(ledger, now, cancellation)
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
        let GatewayExecutionMode::Composed { query, .. } = &self.mode else {
            panic!("process GatewayExecution does not expose query runtime internals")
        };
        query
            .execute(plan, storage, snapshot, budget, cancellation, overlay)
            .await
    }

    pub fn begin_transaction(
        &self,
        transaction_id: TransactionId,
        catalog_version: Version,
        shard_fences: Vec<(ShardId, ShardSnapshotFence)>,
    ) -> TxnFuture<'_, TransactionContext> {
        let GatewayExecutionMode::Composed { transactions, .. } = &self.mode else {
            panic!("process GatewayExecution does not expose transaction internals")
        };
        transactions.begin(transaction_id, catalog_version, shard_fences)
    }

    pub fn commit_transaction<'a>(
        &'a self,
        context: &'a TransactionContext,
        participants: Vec<ParticipantWrite>,
    ) -> TxnFuture<'a, TransactionOutcome> {
        let GatewayExecutionMode::Composed { transactions, .. } = &self.mode else {
            panic!("process GatewayExecution does not expose transaction internals")
        };
        transactions.commit(context, participants)
    }

    pub fn abort_transaction<'a>(
        &'a self,
        context: &'a TransactionContext,
        participants: Vec<ParticipantWrite>,
    ) -> TxnFuture<'a, TransactionOutcome> {
        let GatewayExecutionMode::Composed { transactions, .. } = &self.mode else {
            panic!("process GatewayExecution does not expose transaction internals")
        };
        transactions.abort(context, participants)
    }

    pub fn execute_statement<'a>(
        &'a self,
        context: GatewayRequestContext,
        statement: String,
        parameters: BTreeMap<String, GatewayValue>,
        transaction_id: Option<u128>,
        cancellation: &'a GatewayCancellationToken,
    ) -> GatewayFuture<'a, Result<GatewayResponse, GatewayExecutionError>> {
        Box::pin(async move {
            validate_process_request(&context, cancellation)?;
            let program = self.compile(&statement).map_err(|error| {
                GatewayExecutionError::new(error.code(), error.to_string(), GatewayRetry::Never)
            })?;
            for parameter in &program.parameters {
                if parameter.required && !parameters.contains_key(&parameter.name) {
                    return Err(GatewayExecutionError::new(
                        "DTG-EXECUTION-MISSING-PARAMETER",
                        format!("missing required parameter: {}", parameter.name),
                        GatewayRetry::Never,
                    ));
                }
            }
            let operation = match &program.statement {
                LogicalStatement::Query(_) => GatewayOperation::Query,
                LogicalStatement::Write(_) => GatewayOperation::Write,
                LogicalStatement::BeginTransaction => GatewayOperation::BeginTransaction,
                LogicalStatement::CommitTransaction => GatewayOperation::CommitTransaction,
                LogicalStatement::RollbackTransaction => GatewayOperation::RollbackTransaction,
                LogicalStatement::SubmitAnalytics(submission) => {
                    GatewayOperation::SubmitAnalytics {
                        algorithm: submission.algorithm.as_str().to_owned(),
                    }
                }
            };
            let temporal_mode = normalized_temporal_mode(&program.statement)?;
            let result_fields = program
                .result_schema
                .fields
                .iter()
                .map(|field| field.name.clone())
                .collect();
            let request = GatewayClusterRequest {
                context,
                operation,
                statement: Some(statement),
                parameters,
                transaction_id,
                result_fields,
                temporal_mode,
            };
            let GatewayExecutionMode::Process { transport } = &self.mode else {
                return Err(GatewayExecutionError::new(
                    "DTG-EXECUTION-PROCESS-TRANSPORT",
                    "GatewayExecution was not constructed for process execution",
                    GatewayRetry::Never,
                ));
            };
            let response = transport.execute(request).await?;
            validate_process_request_end(cancellation)?;
            Ok(response)
        })
    }

    pub fn execute_operation<'a>(
        &'a self,
        context: GatewayRequestContext,
        operation: GatewayOperation,
        transaction_id: Option<u128>,
        cancellation: &'a GatewayCancellationToken,
    ) -> GatewayFuture<'a, Result<GatewayResponse, GatewayExecutionError>> {
        Box::pin(async move {
            validate_process_request(&context, cancellation)?;
            let request = GatewayClusterRequest {
                context,
                operation,
                statement: None,
                parameters: BTreeMap::new(),
                transaction_id,
                result_fields: Vec::new(),
                temporal_mode: GatewayTemporalMode::Current,
            };
            let GatewayExecutionMode::Process { transport } = &self.mode else {
                return Err(GatewayExecutionError::new(
                    "DTG-EXECUTION-PROCESS-TRANSPORT",
                    "GatewayExecution was not constructed for process execution",
                    GatewayRetry::Never,
                ));
            };
            let response = transport.execute(request).await?;
            validate_process_request_end(cancellation)?;
            Ok(response)
        })
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
            mode: GatewayExecutionMode::Composed {
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
            },
        })
    }
}

fn validate_process_request(
    context: &GatewayRequestContext,
    cancellation: &GatewayCancellationToken,
) -> Result<(), GatewayExecutionError> {
    if cancellation.is_cancelled() {
        return Err(GatewayExecutionError::new(
            "DTG-EXECUTION-CANCELLED",
            "request was cancelled",
            GatewayRetry::Never,
        ));
    }
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| {
            GatewayExecutionError::new(
                "DTG-EXECUTION-CLOCK",
                "system clock precedes the Unix epoch",
                GatewayRetry::Safe,
            )
        })?
        .as_millis();
    if now >= u128::from(context.deadline_unix_ms()) {
        return Err(GatewayExecutionError::new(
            "DTG-EXECUTION-DEADLINE",
            "request deadline has elapsed",
            GatewayRetry::Safe,
        ));
    }
    Ok(())
}

fn validate_process_request_end(
    cancellation: &GatewayCancellationToken,
) -> Result<(), GatewayExecutionError> {
    if cancellation.is_cancelled() {
        return Err(GatewayExecutionError::new(
            "DTG-EXECUTION-CANCELLED",
            "request was cancelled",
            GatewayRetry::Never,
        ));
    }
    Ok(())
}

fn normalized_temporal_mode(
    statement: &LogicalStatement,
) -> Result<GatewayTemporalMode, GatewayExecutionError> {
    match statement {
        LogicalStatement::Query(plan) => temporal_mode_from_plan(plan),
        LogicalStatement::Write(write) => write
            .input
            .as_ref()
            .map_or(Ok(GatewayTemporalMode::Current), temporal_mode_from_plan),
        LogicalStatement::SubmitAnalytics(submission) => {
            temporal_mode_from_scope(&submission.read_scope.transaction_time)
        }
        LogicalStatement::BeginTransaction
        | LogicalStatement::CommitTransaction
        | LogicalStatement::RollbackTransaction => Ok(GatewayTemporalMode::Current),
    }
}

fn temporal_mode_from_plan(
    plan: &LogicalPlan,
) -> Result<GatewayTemporalMode, GatewayExecutionError> {
    let mut normalized = None;
    for node in &plan.nodes {
        let scope = match &node.kind {
            LogicalNodeKind::NodeScan(scan) => Some(&scan.read_scope.transaction_time),
            LogicalNodeKind::RelationshipScan(scan) => Some(&scan.read_scope.transaction_time),
            LogicalNodeKind::VertexLookup(lookup) => Some(&lookup.read_scope.transaction_time),
            LogicalNodeKind::RelationshipLookup(lookup) => {
                Some(&lookup.read_scope.transaction_time)
            }
            LogicalNodeKind::Expand(expand) => Some(&expand.read_scope.transaction_time),
            LogicalNodeKind::Subquery(subquery) => {
                let nested = temporal_mode_from_plan(&subquery.plan)?;
                merge_temporal_mode(&mut normalized, nested)?;
                None
            }
            LogicalNodeKind::Filter { .. }
            | LogicalNodeKind::Project { .. }
            | LogicalNodeKind::Join(_)
            | LogicalNodeKind::Aggregate(_)
            | LogicalNodeKind::Sort(_)
            | LogicalNodeKind::Limit(_)
            | LogicalNodeKind::Unwind(_) => None,
        };
        if let Some(scope) = scope {
            merge_temporal_mode(&mut normalized, temporal_mode_from_scope(scope)?)?;
        }
    }
    Ok(normalized.unwrap_or(GatewayTemporalMode::Current))
}

fn merge_temporal_mode(
    current: &mut Option<GatewayTemporalMode>,
    next: GatewayTemporalMode,
) -> Result<(), GatewayExecutionError> {
    match current {
        Some(existing) if existing != &next => Err(GatewayExecutionError::new(
            "DTG-EXECUTION-TEMPORAL-SCOPE-DRIFT",
            "normalized query contains incompatible system-time scopes",
            GatewayRetry::Never,
        )),
        Some(_) => Ok(()),
        None => {
            *current = Some(next);
            Ok(())
        }
    }
}

fn temporal_mode_from_scope(
    scope: &TemporalScope,
) -> Result<GatewayTemporalMode, GatewayExecutionError> {
    Ok(match scope {
        TemporalScope::Current => GatewayTemporalMode::Current,
        TemporalScope::AsOf(time) => GatewayTemporalMode::AsOf(gateway_time(time)),
        TemporalScope::Changes { from, to } => GatewayTemporalMode::Changes {
            from: gateway_time(from),
            to: gateway_time(to),
        },
    })
}

fn gateway_time(time: &TimeExpr) -> GatewayTime {
    match time {
        TimeExpr::Literal(value) => GatewayTime::Literal(value.get()),
        TimeExpr::Parameter(name) => GatewayTime::Parameter(name.clone()),
    }
}

fn encode_protocol_v2_request(request: &GatewayClusterRequest) -> proto::GatewayRequest {
    let context = proto::RequestContext {
        protocol_major: PROTOCOL_MAJOR,
        protocol_minor: SUPPORTED_MINOR_MAX,
        cluster_id: request.context.cluster_id().to_be_bytes().to_vec(),
        request_id: request.context.request_id().to_be_bytes().to_vec(),
        deadline_unix_ms: request.context.deadline_unix_ms(),
        trace_context: request.context.trace_context().to_vec(),
    };
    let body = encode_cluster_request_body(request);
    let item_count = u32::try_from(request.parameters.len())
        .unwrap_or(u32::MAX)
        .saturating_add(1);
    proto::GatewayRequest {
        request: Some(context),
        execution_request: Some(proto::BoundedPayload {
            format_version: 1,
            declared_len: body.len() as u64,
            item_count,
            checksum: checksum_bytes(&body).to_vec(),
            body,
        }),
    }
}

fn encode_cluster_request_body(request: &GatewayClusterRequest) -> Vec<u8> {
    let mut body = Vec::new();
    body.push(operation_tag(&request.operation));
    match request.transaction_id {
        Some(transaction_id) => {
            body.push(1);
            body.extend_from_slice(&transaction_id.to_be_bytes());
        }
        None => body.push(0),
    }
    encode_temporal_mode(&request.temporal_mode, &mut body);
    encode_optional_string(request.statement.as_deref(), &mut body);
    encode_u32(request.parameters.len(), &mut body);
    for (name, value) in &request.parameters {
        encode_string(name, &mut body);
        encode_gateway_value(value, &mut body);
    }
    encode_u32(request.result_fields.len(), &mut body);
    for field in &request.result_fields {
        encode_string(field, &mut body);
    }
    body
}

fn operation_tag(operation: &GatewayOperation) -> u8 {
    match operation {
        GatewayOperation::Query => 1,
        GatewayOperation::Write => 2,
        GatewayOperation::BeginTransaction => 3,
        GatewayOperation::CommitTransaction => 4,
        GatewayOperation::RollbackTransaction => 5,
        GatewayOperation::SubmitAnalytics { .. } => 6,
        GatewayOperation::AnalyticsStatus { .. } => 7,
        GatewayOperation::AnalyticsResult { .. } => 8,
        GatewayOperation::CancelAnalytics { .. } => 9,
    }
}

fn encode_temporal_mode(mode: &GatewayTemporalMode, output: &mut Vec<u8>) {
    match mode {
        GatewayTemporalMode::Current => output.push(0),
        GatewayTemporalMode::AsOf(time) => {
            output.push(1);
            encode_gateway_time(time, output);
        }
        GatewayTemporalMode::Changes { from, to } => {
            output.push(2);
            encode_gateway_time(from, output);
            encode_gateway_time(to, output);
        }
    }
}

fn encode_gateway_time(time: &GatewayTime, output: &mut Vec<u8>) {
    match time {
        GatewayTime::Literal(value) => {
            output.push(0);
            output.extend_from_slice(&value.to_be_bytes());
        }
        GatewayTime::Parameter(name) => {
            output.push(1);
            encode_string(name, output);
        }
    }
}

fn encode_optional_string(value: Option<&str>, output: &mut Vec<u8>) {
    match value {
        Some(value) => {
            output.push(1);
            encode_string(value, output);
        }
        None => output.push(0),
    }
}

fn encode_string(value: &str, output: &mut Vec<u8>) {
    encode_u32(value.len(), output);
    output.extend_from_slice(value.as_bytes());
}

fn encode_u32(value: usize, output: &mut Vec<u8>) {
    output.extend_from_slice(&u32::try_from(value).unwrap_or(u32::MAX).to_be_bytes());
}

fn encode_gateway_value(value: &GatewayValue, output: &mut Vec<u8>) {
    match value {
        GatewayValue::Null => output.push(0),
        GatewayValue::Boolean(value) => {
            output.push(1);
            output.push(u8::from(*value));
        }
        GatewayValue::Integer(value) => {
            output.push(2);
            output.extend_from_slice(&value.to_be_bytes());
        }
        GatewayValue::FloatBits(value) => {
            output.push(3);
            output.extend_from_slice(&value.to_be_bytes());
        }
        GatewayValue::Bytes(value) => {
            output.push(4);
            encode_u32(value.len(), output);
            output.extend_from_slice(value);
        }
        GatewayValue::String(value) => {
            output.push(5);
            encode_string(value, output);
        }
        GatewayValue::List(values) => {
            output.push(6);
            encode_u32(values.len(), output);
            for value in values {
                encode_gateway_value(value, output);
            }
        }
        GatewayValue::Map(values) => {
            output.push(7);
            encode_u32(values.len(), output);
            for (name, value) in values {
                encode_string(name, output);
                encode_gateway_value(value, output);
            }
        }
    }
}

fn decode_protocol_v2_responses(
    responses: Vec<proto::GatewayResponse>,
    operation: &GatewayOperation,
) -> Result<GatewayResponse, GatewayExecutionError> {
    if responses.is_empty() {
        return Err(GatewayExecutionError::new(
            "DTG-PROTOCOL-EMPTY-RESPONSE",
            "protocol v2 client returned no response",
            GatewayRetry::Safe,
        ));
    }
    let mut fields = None;
    let mut rows = Vec::new();
    let mut control_response = None;
    for response in responses {
        let status = response.status.ok_or_else(|| {
            GatewayExecutionError::new(
                "DTG-PROTOCOL-MISSING-STATUS",
                "protocol v2 response omitted typed status",
                GatewayRetry::Safe,
            )
        })?;
        let validated = validate_typed_status(status.clone()).map_err(|error| {
            GatewayExecutionError::new(error.code(), error.to_string(), GatewayRetry::Never)
        })?;
        if status.code != 1 {
            return Err(protocol_status_error(status));
        }
        if let Some(details) = validated.details() {
            let decoded = decode_control_response(details.body())?;
            if control_response.replace(decoded).is_some() {
                return Err(wire_codec_error(
                    "protocol v2 response repeated control details",
                ));
            }
        }
        if let Some(batch) = response.batch {
            let row_count = batch.row_count;
            let payload = validate_column_batch(batch).map_err(|error| {
                GatewayExecutionError::new(error.code(), error.to_string(), GatewayRetry::Never)
            })?;
            let decoded = decode_gateway_rows(payload.body(), row_count)?;
            match &fields {
                Some(existing) if existing != decoded.fields() => {
                    return Err(wire_codec_error(
                        "protocol v2 batches changed result fields",
                    ));
                }
                Some(_) => {}
                None => fields = Some(decoded.fields().to_vec()),
            }
            rows.extend_from_slice(decoded.rows());
        }
    }
    if control_response.is_some() && fields.is_some() {
        return Err(wire_codec_error(
            "protocol v2 response mixed control details and typed rows",
        ));
    }
    if let Some(response) = control_response {
        return Ok(response);
    }
    match fields {
        Some(fields) => {
            let rows = GatewayRows::new(fields, rows)?;
            match operation {
                GatewayOperation::AnalyticsResult { job_id } => {
                    Ok(GatewayResponse::AnalyticsResult {
                        job_id: *job_id,
                        rows,
                    })
                }
                _ => Ok(GatewayResponse::Rows(rows)),
            }
        }
        None => match operation {
            GatewayOperation::BeginTransaction
            | GatewayOperation::SubmitAnalytics { .. }
            | GatewayOperation::AnalyticsStatus { .. }
            | GatewayOperation::CancelAnalytics { .. } => Err(wire_codec_error(
                "protocol v2 control response omitted typed details",
            )),
            _ => Ok(GatewayResponse::Acknowledged),
        },
    }
}

fn protocol_status_error(status: proto::TypedStatus) -> GatewayExecutionError {
    let code = match status.code {
        2 => "DTG-CLUSTER-INVALID-REQUEST",
        3 => "DTG-CLUSTER-STALE-FENCE",
        4 => "DTG-CLUSTER-CONFLICT",
        5 => "DTG-CLUSTER-UNAVAILABLE",
        _ => "DTG-CLUSTER-INTERNAL",
    };
    let retry = match status.retry {
        2 => GatewayRetry::Safe,
        3 => GatewayRetry::Replay,
        _ => GatewayRetry::Never,
    };
    GatewayExecutionError::new(code, status.message, retry)
}

fn decode_gateway_rows(
    body: &[u8],
    wire_row_count: u32,
) -> Result<GatewayRows, GatewayExecutionError> {
    let mut cursor = WireCursor::new(body);
    let field_count = cursor.read_len()?;
    let mut fields = Vec::with_capacity(field_count);
    for _ in 0..field_count {
        fields.push(cursor.read_string()?);
    }
    let row_count = cursor.read_len()?;
    if row_count != wire_row_count as usize {
        return Err(wire_codec_error(
            "protocol v2 row count differs from the typed batch header",
        ));
    }
    let mut rows = Vec::with_capacity(row_count);
    for _ in 0..row_count {
        let mut row = Vec::with_capacity(field_count);
        for _ in 0..field_count {
            row.push(cursor.read_value(0)?);
        }
        rows.push(row);
    }
    if !cursor.is_empty() {
        return Err(wire_codec_error(
            "protocol v2 typed row payload has trailing bytes",
        ));
    }
    GatewayRows::new(fields, rows)
}

fn decode_control_response(body: &[u8]) -> Result<GatewayResponse, GatewayExecutionError> {
    let mut cursor = WireCursor::new(body);
    let response = match cursor.read_u8()? {
        1 => GatewayResponse::Transaction {
            transaction_id: cursor.read_u128()?,
        },
        2 => GatewayResponse::AnalyticsSubmitted {
            job_id: cursor.read_u128()?,
        },
        3 => {
            let job_id = cursor.read_u128()?;
            let state = match cursor.read_u8()? {
                0 => GatewayAnalyticsState::Queued,
                1 => GatewayAnalyticsState::Running,
                2 => GatewayAnalyticsState::Succeeded,
                3 => GatewayAnalyticsState::Failed,
                4 => GatewayAnalyticsState::Cancelled,
                _ => return Err(wire_codec_error("protocol v2 analytics state is invalid")),
            };
            GatewayResponse::AnalyticsStatus { job_id, state }
        }
        4 => GatewayResponse::AnalyticsCancelled {
            job_id: cursor.read_u128()?,
        },
        _ => {
            return Err(wire_codec_error(
                "protocol v2 control response has an unknown tag",
            ));
        }
    };
    if !cursor.is_empty() {
        return Err(wire_codec_error(
            "protocol v2 control response has trailing bytes",
        ));
    }
    Ok(response)
}

struct WireCursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> WireCursor<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn is_empty(&self) -> bool {
        self.offset == self.bytes.len()
    }

    fn read_exact(&mut self, len: usize) -> Result<&'a [u8], GatewayExecutionError> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(|| wire_codec_error("protocol v2 row offset overflow"))?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| wire_codec_error("protocol v2 typed row payload is truncated"))?;
        self.offset = end;
        Ok(value)
    }

    fn read_u8(&mut self) -> Result<u8, GatewayExecutionError> {
        Ok(self.read_exact(1)?[0])
    }

    fn read_u32(&mut self) -> Result<u32, GatewayExecutionError> {
        let bytes: [u8; 4] = self
            .read_exact(4)?
            .try_into()
            .map_err(|_| wire_codec_error("protocol v2 u32 is malformed"))?;
        Ok(u32::from_be_bytes(bytes))
    }

    fn read_u64(&mut self) -> Result<u64, GatewayExecutionError> {
        let bytes: [u8; 8] = self
            .read_exact(8)?
            .try_into()
            .map_err(|_| wire_codec_error("protocol v2 u64 is malformed"))?;
        Ok(u64::from_be_bytes(bytes))
    }

    fn read_u128(&mut self) -> Result<u128, GatewayExecutionError> {
        let bytes: [u8; 16] = self
            .read_exact(16)?
            .try_into()
            .map_err(|_| wire_codec_error("protocol v2 u128 is malformed"))?;
        Ok(u128::from_be_bytes(bytes))
    }

    fn read_i64(&mut self) -> Result<i64, GatewayExecutionError> {
        let bytes: [u8; 8] = self
            .read_exact(8)?
            .try_into()
            .map_err(|_| wire_codec_error("protocol v2 i64 is malformed"))?;
        Ok(i64::from_be_bytes(bytes))
    }

    fn read_len(&mut self) -> Result<usize, GatewayExecutionError> {
        usize::try_from(self.read_u32()?)
            .map_err(|_| wire_codec_error("protocol v2 length exceeds usize"))
    }

    fn read_string(&mut self) -> Result<String, GatewayExecutionError> {
        let len = self.read_len()?;
        let bytes = self.read_exact(len)?;
        String::from_utf8(bytes.to_vec())
            .map_err(|_| wire_codec_error("protocol v2 string is not UTF-8"))
    }

    fn read_value(&mut self, depth: usize) -> Result<GatewayValue, GatewayExecutionError> {
        if depth > 64 {
            return Err(wire_codec_error(
                "protocol v2 value nesting exceeds 64 levels",
            ));
        }
        match self.read_u8()? {
            0 => Ok(GatewayValue::Null),
            1 => match self.read_u8()? {
                0 => Ok(GatewayValue::Boolean(false)),
                1 => Ok(GatewayValue::Boolean(true)),
                _ => Err(wire_codec_error("protocol v2 boolean is malformed")),
            },
            2 => self.read_i64().map(GatewayValue::Integer),
            3 => self.read_u64().map(GatewayValue::FloatBits),
            4 => {
                let len = self.read_len()?;
                self.read_exact(len)
                    .map(|bytes| GatewayValue::Bytes(bytes.to_vec()))
            }
            5 => self.read_string().map(GatewayValue::String),
            6 => {
                let len = self.read_len()?;
                let mut values = Vec::with_capacity(len);
                for _ in 0..len {
                    values.push(self.read_value(depth + 1)?);
                }
                Ok(GatewayValue::List(values))
            }
            7 => {
                let len = self.read_len()?;
                let mut values = BTreeMap::new();
                for _ in 0..len {
                    let name = self.read_string()?;
                    let value = self.read_value(depth + 1)?;
                    if values.insert(name, value).is_some() {
                        return Err(wire_codec_error("protocol v2 map contains a duplicate key"));
                    }
                }
                Ok(GatewayValue::Map(values))
            }
            _ => Err(wire_codec_error(
                "protocol v2 typed row contains an unknown value tag",
            )),
        }
    }
}

fn wire_codec_error(message: impl Into<String>) -> GatewayExecutionError {
    GatewayExecutionError::new("DTG-PROTOCOL-ROW-CODEC", message, GatewayRetry::Never)
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
