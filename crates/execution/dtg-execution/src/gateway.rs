use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use dtg_analytics::{
    AnalyticsArtifactRepository, AnalyticsJobError, AnalyticsLedger, AnalyticsProjectionProvider,
    AnalyticsScheduler, AnalyticsSchedulerTick, AnalyticsStepProvider, CancellationToken,
    JobTimestamp, ProjectionError, ShardSnapshotProvenance, SnapshotProvenance,
};
use dtg_cluster_v2::{
    PROTOCOL_MAJOR, SUPPORTED_MINOR_MAX, ValidatedPayload, checksum_bytes, proto,
    validate_column_batch, validate_typed_status,
};
use dtg_language::{EmptySchemaCatalog, Language, LanguageError, LogicalProgram};
use dtg_language_ir::{
    AggregateKind, Field, LogicalExpr, LogicalNodeKind, LogicalPlan, LogicalStatement, LogicalType,
    RowSchema, TemporalScope, TimeExpr, ValidTimeExpr, ValidTimePredicate,
};
use dtg_plan::{
    CatalogShard, CatalogSnapshot, ExchangeKind, LogicalReadOperation, LogicalReadRequest,
    PhysicalExpr, PhysicalPlan, Planner, PlanningContext, SemanticRequirements,
    SnapshotRequirements, StorageAccess,
};
use dtg_query::{
    CancellationToken as QueryCancellationToken, ColumnBatch as QueryColumnBatch, ExecutableAccess,
    ExecutableAggregate, ExecutableFragment, ExecutableOperator, ExecutableOperatorKind,
    ExecutablePlan, ExecutableProjection, ExecutableSortKey, ExecutionFence, Expression,
    LogicalRead, QueryBudget, QueryError, QueryOverlay, QueryRuntime, QueryStorage, QueryStream,
    QueryValue, ReadOperation, ResidualPredicate, SnapshotGuard, SnapshotShardFence,
};
use dtg_shard::{CommitSingleShardTransaction, ShardCommand};
use dtg_storage::{
    CommandId, LogicalMutation as StorageMutation, Properties, ValidInterval, VertexId,
    VertexVersion,
};
use dtg_storage::{PushdownOperation, ShardId, TransactionId, Value, Version};
use dtg_transaction::{
    ParticipantWrite, ShardSnapshotFence, SnapshotToken, TemporalTxnCoordinator,
    TransactionContext, TransactionOutcome, TxnFuture,
};
use sha2::{Digest, Sha256};
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::{Notify, mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Channel, Endpoint};

use crate::{ExecutionBuildError, RequestDetail, RequestStage, RequestStageMetrics, StageOutcome};

const PARTIAL_VERTEX_COUNT_FIELD: &str = "__dtg_partial_vertex_count";

trait AnalyticsRuntime: Send + Sync {
    fn tick(
        &mut self,
        ledger: &mut AnalyticsLedger,
        now: JobTimestamp,
        cancellation: &CancellationToken,
    ) -> Result<AnalyticsSchedulerTick, AnalyticsJobError>;
}

impl<P, A, R> AnalyticsRuntime for AnalyticsScheduler<P, A, R>
where
    P: AnalyticsProjectionProvider + Send + Sync + 'static,
    A: AnalyticsStepProvider + Send + Sync + 'static,
    R: AnalyticsArtifactRepository + Send + Sync + 'static,
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

pub type GatewayFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

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
    physical_plan: Option<PhysicalPlan>,
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

    pub const fn physical_plan(&self) -> Option<&PhysicalPlan> {
        self.physical_plan.as_ref()
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

    pub fn into_parts(self) -> (Vec<String>, Vec<Vec<GatewayValue>>) {
        (self.fields, self.rows)
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

#[derive(Clone)]
pub struct GatewayCancellationToken {
    cancelled: Arc<AtomicBool>,
    notified: Arc<Notify>,
}

impl Default for GatewayCancellationToken {
    fn default() -> Self {
        Self {
            cancelled: Arc::new(AtomicBool::new(false)),
            notified: Arc::new(Notify::new()),
        }
    }
}

impl GatewayCancellationToken {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        self.notified.notify_waiters();
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    pub async fn cancelled(&self) {
        loop {
            if self.is_cancelled() {
                return;
            }
            let notified = self.notified.notified();
            if self.is_cancelled() {
                return;
            }
            notified.await;
        }
    }
}

pub trait GatewayExecutionTransport: Send + Sync {
    fn execute(
        &self,
        request: GatewayClusterRequest,
    ) -> GatewayFuture<'_, Result<GatewayResponse, GatewayExecutionError>>;

    fn execute_query(
        &self,
        request: GatewayClusterRequest,
    ) -> GatewayFuture<'_, Result<GatewayQueryResponse, GatewayExecutionError>> {
        Box::pin(async move { self.execute(request).await.map(GatewayQueryResponse::Final) })
    }

    fn execute_query_with_metrics(
        &self,
        request: GatewayClusterRequest,
        _metrics: Arc<RequestStageMetrics>,
    ) -> GatewayFuture<'_, Result<GatewayQueryResponse, GatewayExecutionError>> {
        self.execute_query(request)
    }

    fn execute_query_with_metrics_and_cancellation(
        &self,
        request: GatewayClusterRequest,
        metrics: Arc<RequestStageMetrics>,
        _cancellation: &GatewayCancellationToken,
    ) -> GatewayFuture<'_, Result<GatewayQueryResponse, GatewayExecutionError>> {
        self.execute_query_with_metrics(request, metrics)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GatewayWriteRoute {
    context: GatewayRequestContext,
    binding: dtg_storage::ReplicaBinding,
    catalog_version: Version,
    snapshot_applied_index: u64,
}

impl GatewayWriteRoute {
    fn new(
        context: GatewayRequestContext,
        binding: dtg_storage::ReplicaBinding,
        catalog_version: Version,
        snapshot_applied_index: u64,
    ) -> Self {
        Self {
            context,
            binding,
            catalog_version,
            snapshot_applied_index,
        }
    }

    pub const fn context(&self) -> &GatewayRequestContext {
        &self.context
    }

    pub const fn binding(&self) -> &dtg_storage::ReplicaBinding {
        &self.binding
    }

    pub const fn catalog_version(&self) -> Version {
        self.catalog_version
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GatewayWriteRequest {
    route: GatewayWriteRoute,
    transaction_id: TransactionId,
    start_time: dtg_storage::TransactionTime,
    commit_time: dtg_storage::TransactionTime,
    command: ShardCommand,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GatewayWriteReceipt {
    applied_index: u64,
    replayed: bool,
}

impl GatewayWriteReceipt {
    pub const fn new(applied_index: u64, replayed: bool) -> Self {
        Self {
            applied_index,
            replayed,
        }
    }

    pub const fn applied_index(self) -> u64 {
        self.applied_index
    }

    pub const fn replayed(self) -> bool {
        self.replayed
    }
}

impl GatewayWriteRequest {
    fn new(
        route: GatewayWriteRoute,
        transaction_id: TransactionId,
        start_time: dtg_storage::TransactionTime,
        commit_time: dtg_storage::TransactionTime,
        command: ShardCommand,
    ) -> Self {
        Self {
            route,
            transaction_id,
            start_time,
            commit_time,
            command,
        }
    }

    pub const fn transaction_id(&self) -> TransactionId {
        self.transaction_id
    }

    pub const fn start_time(&self) -> dtg_storage::TransactionTime {
        self.start_time
    }

    pub const fn commit_time(&self) -> dtg_storage::TransactionTime {
        self.commit_time
    }

    pub const fn snapshot_applied_index(&self) -> u64 {
        self.route.snapshot_applied_index
    }

    pub fn mutations(&self) -> &[StorageMutation] {
        let ShardCommand::CommitSingleShardTransaction(command) = &self.command else {
            unreachable!("process write request contains a single-shard transaction command")
        };
        command.mutations()
    }

    pub const fn command(&self) -> &ShardCommand {
        &self.command
    }
}

pub trait GatewayWriteTransport: Send + Sync {
    fn allocate_start_time(
        &self,
        route: &GatewayWriteRoute,
        transaction_id: TransactionId,
    ) -> GatewayFuture<'_, Result<dtg_storage::TransactionTime, GatewayExecutionError>>;

    fn reserve_commit_time(
        &self,
        route: &GatewayWriteRoute,
        transaction_id: TransactionId,
    ) -> GatewayFuture<'_, Result<dtg_storage::TransactionTime, GatewayExecutionError>>;

    fn prepare_write_times(
        &self,
        route: &GatewayWriteRoute,
        transaction_id: TransactionId,
    ) -> GatewayFuture<
        '_,
        Result<(dtg_storage::TransactionTime, dtg_storage::TransactionTime), GatewayExecutionError>,
    > {
        let route = route.clone();
        Box::pin(async move {
            tokio::try_join!(
                self.allocate_start_time(&route, transaction_id),
                self.reserve_commit_time(&route, transaction_id),
            )
        })
    }

    fn apply_single_shard(
        &self,
        request: GatewayWriteRequest,
    ) -> GatewayFuture<'_, Result<GatewayWriteReceipt, GatewayExecutionError>>;

    fn resolve_committed(
        &self,
        route: &GatewayWriteRoute,
        transaction_id: TransactionId,
    ) -> GatewayFuture<'_, Result<(), GatewayExecutionError>>;

    fn abort(
        &self,
        route: &GatewayWriteRoute,
        transaction_id: TransactionId,
    ) -> GatewayFuture<'_, Result<(), GatewayExecutionError>>;
}

#[derive(Clone)]
pub struct TonicGatewayWriteTransport {
    meta: proto::meta_service_client::MetaServiceClient<Channel>,
    data: proto::data_service_client::DataServiceClient<Channel>,
}

impl TonicGatewayWriteTransport {
    pub async fn connect(
        meta_endpoint: impl Into<String>,
        data_endpoint: impl Into<String>,
    ) -> Result<Arc<dyn GatewayWriteTransport>, GatewayExecutionError> {
        let meta = proto::meta_service_client::MetaServiceClient::connect(meta_endpoint.into())
            .await
            .map_err(cluster_connect_error)?;
        let data = proto::data_service_client::DataServiceClient::connect(data_endpoint.into())
            .await
            .map_err(cluster_connect_error)?;
        Ok(Arc::new(Self { meta, data }))
    }

    async fn timestamp(
        &self,
        route: &GatewayWriteRoute,
        transaction_id: TransactionId,
        operation: i32,
    ) -> Result<dtg_storage::TransactionTime, GatewayExecutionError> {
        let request = transaction_rpc_request(route, transaction_id, operation, None)?;
        let mut client = self.meta.clone();
        let status = client
            .submit_transaction(request)
            .await
            .map_err(cluster_rpc_error)?
            .into_inner();
        let details = status_details(status)?.ok_or_else(|| {
            GatewayExecutionError::new(
                "DTG-EXECUTION-TIMESTAMP",
                "Meta timestamp response is missing details",
                GatewayRetry::Safe,
            )
        })?;
        let bytes: [u8; 8] = details.body().try_into().map_err(|_| {
            GatewayExecutionError::new(
                "DTG-EXECUTION-TIMESTAMP",
                "Meta timestamp response is not an i64",
                GatewayRetry::Safe,
            )
        })?;
        dtg_storage::TransactionTime::new(i64::from_be_bytes(bytes)).map_err(|error| {
            GatewayExecutionError::new(
                "DTG-EXECUTION-TIMESTAMP",
                error.to_string(),
                GatewayRetry::Safe,
            )
        })
    }

    async fn prepare_write_times_rpc(
        &self,
        route: &GatewayWriteRoute,
        transaction_id: TransactionId,
    ) -> Result<(dtg_storage::TransactionTime, dtg_storage::TransactionTime), GatewayExecutionError>
    {
        let request = transaction_rpc_request(route, transaction_id, 6, None)?;
        let mut client = self.meta.clone();
        let status = client
            .submit_transaction(request)
            .await
            .map_err(cluster_rpc_error)?
            .into_inner();
        let details = status_details(status)?.ok_or_else(|| {
            GatewayExecutionError::new(
                "DTG-EXECUTION-TIMESTAMP",
                "Meta write preparation response is missing details",
                GatewayRetry::Safe,
            )
        })?;
        let bytes: [u8; 16] = details.body().try_into().map_err(|_| {
            GatewayExecutionError::new(
                "DTG-EXECUTION-TIMESTAMP",
                "Meta write preparation response is not two i64 values",
                GatewayRetry::Safe,
            )
        })?;
        let start =
            dtg_storage::TransactionTime::new(i64::from_be_bytes(bytes[..8].try_into().unwrap()))
                .map_err(|error| {
                GatewayExecutionError::new(
                    "DTG-EXECUTION-TIMESTAMP",
                    error.to_string(),
                    GatewayRetry::Safe,
                )
            })?;
        let commit =
            dtg_storage::TransactionTime::new(i64::from_be_bytes(bytes[8..].try_into().unwrap()))
                .map_err(|error| {
                GatewayExecutionError::new(
                    "DTG-EXECUTION-TIMESTAMP",
                    error.to_string(),
                    GatewayRetry::Safe,
                )
            })?;
        if commit <= start {
            return Err(GatewayExecutionError::new(
                "DTG-EXECUTION-TIMESTAMP",
                "Meta write preparation returned a non-increasing timestamp pair",
                GatewayRetry::Safe,
            ));
        }
        Ok((start, commit))
    }
}

impl GatewayWriteTransport for TonicGatewayWriteTransport {
    fn allocate_start_time(
        &self,
        route: &GatewayWriteRoute,
        transaction_id: TransactionId,
    ) -> GatewayFuture<'_, Result<dtg_storage::TransactionTime, GatewayExecutionError>> {
        let route = route.clone();
        Box::pin(async move { self.timestamp(&route, transaction_id, 1).await })
    }

    fn reserve_commit_time(
        &self,
        route: &GatewayWriteRoute,
        transaction_id: TransactionId,
    ) -> GatewayFuture<'_, Result<dtg_storage::TransactionTime, GatewayExecutionError>> {
        let route = route.clone();
        Box::pin(async move { self.timestamp(&route, transaction_id, 2).await })
    }

    fn prepare_write_times(
        &self,
        route: &GatewayWriteRoute,
        transaction_id: TransactionId,
    ) -> GatewayFuture<
        '_,
        Result<(dtg_storage::TransactionTime, dtg_storage::TransactionTime), GatewayExecutionError>,
    > {
        let route = route.clone();
        Box::pin(async move { self.prepare_write_times_rpc(&route, transaction_id).await })
    }

    fn apply_single_shard(
        &self,
        request: GatewayWriteRequest,
    ) -> GatewayFuture<'_, Result<GatewayWriteReceipt, GatewayExecutionError>> {
        Box::pin(async move {
            let command = request.command().encode_current().map_err(|error| {
                GatewayExecutionError::new(
                    "DTG-EXECUTION-WRITE-COMMAND",
                    error.to_string(),
                    GatewayRetry::Never,
                )
            })?;
            let wire = transaction_rpc_request(
                &request.route,
                request.transaction_id(),
                2,
                Some((request.command().header().command_id().get(), command)),
            )?;
            let mut client = self.data.clone();
            let status = client
                .apply_transaction(wire)
                .await
                .map_err(cluster_rpc_error)?
                .into_inner();
            decode_write_receipt(status_details(status)?)
        })
    }

    fn resolve_committed(
        &self,
        route: &GatewayWriteRoute,
        transaction_id: TransactionId,
    ) -> GatewayFuture<'_, Result<(), GatewayExecutionError>> {
        let route = route.clone();
        Box::pin(async move { self.timestamp(&route, transaction_id, 5).await.map(|_| ()) })
    }

    fn abort(
        &self,
        route: &GatewayWriteRoute,
        transaction_id: TransactionId,
    ) -> GatewayFuture<'_, Result<(), GatewayExecutionError>> {
        let route = route.clone();
        Box::pin(async move { self.timestamp(&route, transaction_id, 3).await.map(|_| ()) })
    }
}

fn transaction_rpc_request(
    route: &GatewayWriteRoute,
    transaction_id: TransactionId,
    operation: i32,
    command: Option<(u128, Vec<u8>)>,
) -> Result<tonic::Request<proto::TransactionRequest>, GatewayExecutionError> {
    let request = proto::RequestContext {
        protocol_major: PROTOCOL_MAJOR,
        protocol_minor: SUPPORTED_MINOR_MAX,
        cluster_id: route.context.cluster_id().to_be_bytes().to_vec(),
        request_id: route.context.request_id().to_be_bytes().to_vec(),
        deadline_unix_ms: route.context.deadline_unix_ms(),
        trace_context: route.context.trace_context().to_vec(),
    };
    let (idempotency_key, body) = command.unwrap_or((transaction_id.get(), vec![operation as u8]));
    let payload = proto::BoundedPayload {
        format_version: 1,
        declared_len: body.len() as u64,
        item_count: 1,
        checksum: checksum_bytes(&body).to_vec(),
        body,
    };
    Ok(tonic::Request::new(proto::TransactionRequest {
        context: Some(proto::ShardContext {
            request: Some(request),
            graph_id: route.binding.graph_id().get(),
            shard_id: u32::try_from(route.binding.shard_id().get()).map_err(|_| {
                GatewayExecutionError::new(
                    "DTG-EXECUTION-WRITE-ROUTING",
                    "Shard identifier exceeds wire range",
                    GatewayRetry::Never,
                )
            })?,
            placement_epoch: route.binding.placement_epoch().get(),
            backend_generation: route.binding.backend_generation().get(),
            catalog_version: route.catalog_version.get(),
        }),
        transaction_id: transaction_id.get().to_be_bytes().to_vec(),
        operation,
        idempotency_key: idempotency_key.to_be_bytes().to_vec(),
        payload: Some(payload),
    }))
}

fn status_details(
    status: proto::TypedStatus,
) -> Result<Option<ValidatedPayload>, GatewayExecutionError> {
    let validated = validate_typed_status(status.clone()).map_err(|error| {
        GatewayExecutionError::new(error.code(), error.to_string(), GatewayRetry::Never)
    })?;
    if status.code == proto::StatusCode::Ok as i32 {
        return Ok(validated.details().cloned());
    }
    let retry = match proto::RetryDisposition::try_from(status.retry).ok() {
        Some(proto::RetryDisposition::Safe) => GatewayRetry::Safe,
        _ => GatewayRetry::Never,
    };
    Err(GatewayExecutionError::new(
        "DTG-CLUSTER-STATUS",
        status.message,
        retry,
    ))
}

fn decode_write_receipt(
    details: Option<ValidatedPayload>,
) -> Result<GatewayWriteReceipt, GatewayExecutionError> {
    let details = details.ok_or_else(|| {
        GatewayExecutionError::new(
            "DTG-EXECUTION-WRITE-RECEIPT",
            "Data write response is missing an applied receipt",
            GatewayRetry::Safe,
        )
    })?;
    if details.format_version() != 1 || details.item_count() != 1 || details.len() != 9 {
        return Err(GatewayExecutionError::new(
            "DTG-EXECUTION-WRITE-RECEIPT",
            "Data write response is not a single applied receipt",
            GatewayRetry::Safe,
        ));
    }
    let bytes: [u8; 9] = details.body().try_into().map_err(|_| {
        GatewayExecutionError::new(
            "DTG-EXECUTION-WRITE-RECEIPT",
            "Data write response is not an applied receipt",
            GatewayRetry::Safe,
        )
    })?;
    let applied_index = u64::from_be_bytes(bytes[..8].try_into().expect("length checked"));
    if applied_index == 0 {
        return Err(GatewayExecutionError::new(
            "DTG-EXECUTION-WRITE-RECEIPT",
            "Data write response contains a zero applied index",
            GatewayRetry::Safe,
        ));
    }
    let replayed = match bytes[8] {
        0 => false,
        1 => true,
        _ => {
            return Err(GatewayExecutionError::new(
                "DTG-EXECUTION-WRITE-RECEIPT",
                "Data write response contains an invalid replay flag",
                GatewayRetry::Safe,
            ));
        }
    };
    Ok(GatewayWriteReceipt::new(applied_index, replayed))
}

fn cluster_connect_error(error: tonic::transport::Error) -> GatewayExecutionError {
    GatewayExecutionError::new("DTG-CLUSTER-CONNECT", error.to_string(), GatewayRetry::Safe)
}

fn cluster_rpc_error(error: tonic::Status) -> GatewayExecutionError {
    GatewayExecutionError::new("DTG-CLUSTER-RPC", error.to_string(), GatewayRetry::Safe)
}

pub enum GatewayQueryResponse {
    Materialized(BTreeMap<u32, Vec<QueryColumnBatch>>),
    Final(GatewayResponse),
}

type GatewaySessionResponses = Result<Vec<proto::GatewayResponse>, GatewayExecutionError>;
type GatewaySessionPending = Arc<Mutex<BTreeMap<u128, oneshot::Sender<GatewaySessionResponses>>>>;

const PIPELINE_MAX_PENDING: usize = 256;
const PIPELINE_MAX_BATCH_REQUESTS: usize = 32;
const PIPELINE_MAX_BATCH_BYTES: usize = 65_536;

#[derive(Default)]
struct GatewayPipelineBatch {
    requests: Vec<proto::GatewayRequest>,
    estimated_bytes: usize,
}

impl GatewayPipelineBatch {
    fn push(&mut self, request: proto::GatewayRequest) -> Result<(), ()> {
        if self.requests.len() >= PIPELINE_MAX_BATCH_REQUESTS {
            return Err(());
        }
        let request_bytes = gateway_pipeline_request_size(&request);
        let total_bytes = self.estimated_bytes.saturating_add(request_bytes);
        if total_bytes > PIPELINE_MAX_BATCH_BYTES {
            return Err(());
        }
        self.estimated_bytes = total_bytes;
        self.requests.push(request);
        Ok(())
    }

    fn is_empty(&self) -> bool {
        self.requests.is_empty()
    }

    fn take_prefix(&mut self, count: usize) -> Vec<proto::GatewayRequest> {
        self.requests.drain(..count).collect()
    }
}

fn gateway_pipeline_request_size(request: &proto::GatewayRequest) -> usize {
    let mut bytes = 32_usize;
    if let Some(context) = request.request.as_ref() {
        bytes = bytes
            .saturating_add(context.cluster_id.len())
            .saturating_add(context.request_id.len())
            .saturating_add(context.trace_context.len())
            .saturating_add(32);
    }
    if let Some(payload) = request.execution_request.as_ref() {
        bytes = bytes
            .saturating_add(payload.body.len())
            .saturating_add(payload.checksum.len())
            .saturating_add(32);
    }
    for fragment in &request.fragments {
        bytes = bytes
            .saturating_add(fragment.fragment_id.len())
            .saturating_add(fragment.capability_digest.len())
            .saturating_add(64);
        if let Some(payload) = fragment.payload.as_ref() {
            bytes = bytes
                .saturating_add(payload.body.len())
                .saturating_add(payload.checksum.len())
                .saturating_add(32);
        }
        if let Some(context) = fragment
            .context
            .as_ref()
            .and_then(|context| context.request.as_ref())
        {
            bytes = bytes
                .saturating_add(context.cluster_id.len())
                .saturating_add(context.request_id.len())
                .saturating_add(context.trace_context.len())
                .saturating_add(32);
        }
    }
    bytes
}

type GatewayPipelinePending = Arc<Mutex<BTreeMap<u128, oneshot::Sender<GatewaySessionResponses>>>>;

struct GatewayPipelineSubmission {
    request_id: u128,
    request: proto::GatewayRequest,
}

struct TonicGatewayPipelineClient {
    submissions: mpsc::Sender<GatewayPipelineSubmission>,
    frames: mpsc::Sender<proto::GatewayPipelineClientFrame>,
    pending: GatewayPipelinePending,
    active: Arc<AtomicBool>,
    credit_notify: Arc<Notify>,
}

pub trait GatewayProtocolV2Client: Send + Sync {
    fn execute(
        &self,
        request: proto::GatewayRequest,
    ) -> GatewayFuture<'_, Result<Vec<proto::GatewayResponse>, GatewayExecutionError>>;

    fn execute_query(
        &self,
        request: proto::GatewayRequest,
    ) -> GatewayFuture<'_, Result<BTreeMap<u32, Vec<QueryColumnBatch>>, GatewayExecutionError>>
    {
        Box::pin(async move { decode_protocol_v2_query_responses(self.execute(request).await?) })
    }

    fn execute_session(
        &self,
        _request: proto::GatewayRequest,
    ) -> GatewayFuture<'_, Result<Option<Vec<proto::GatewayResponse>>, GatewayExecutionError>> {
        Box::pin(async { Ok(None) })
    }

    fn execute_session_with_metrics(
        &self,
        request: proto::GatewayRequest,
        _metrics: Arc<RequestStageMetrics>,
    ) -> GatewayFuture<'_, Result<Option<Vec<proto::GatewayResponse>>, GatewayExecutionError>> {
        self.execute_session(request)
    }

    fn execute_pipeline_with_metrics_and_cancellation(
        &self,
        _request: proto::GatewayRequest,
        _metrics: Arc<RequestStageMetrics>,
        _cancellation: &GatewayCancellationToken,
    ) -> GatewayFuture<'_, Result<Option<Vec<proto::GatewayResponse>>, GatewayExecutionError>> {
        Box::pin(async { Ok(None) })
    }
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

pub struct ShardRoutedGatewayTransport {
    default: Arc<dyn GatewayExecutionTransport>,
    routes: BTreeMap<u64, Arc<dyn GatewayExecutionTransport>>,
}

impl ShardRoutedGatewayTransport {
    pub fn new(
        default: Arc<dyn GatewayExecutionTransport>,
        routes: BTreeMap<u64, Arc<dyn GatewayExecutionTransport>>,
    ) -> Self {
        Self { default, routes }
    }
}

impl GatewayExecutionTransport for ShardRoutedGatewayTransport {
    fn execute(
        &self,
        request: GatewayClusterRequest,
    ) -> GatewayFuture<'_, Result<GatewayResponse, GatewayExecutionError>> {
        self.default.execute(request)
    }

    fn execute_query(
        &self,
        request: GatewayClusterRequest,
    ) -> GatewayFuture<'_, Result<GatewayQueryResponse, GatewayExecutionError>> {
        let Some(plan) = request.physical_plan.as_ref() else {
            return self.default.execute_query(request);
        };
        let mut grouped = BTreeMap::<u64, Vec<dtg_plan::PlanFragment>>::new();
        for fragment in plan.fragments() {
            grouped
                .entry(fragment.fence().shard_id().get())
                .or_default()
                .push(fragment.clone());
        }
        let mut requests = Vec::with_capacity(grouped.len());
        for (shard_id, fragments) in grouped {
            let transport = self.routes.get(&shard_id).unwrap_or(&self.default).clone();
            let mut routed = request.clone();
            let mut routed_plan = plan.clone();
            routed_plan.fragments = fragments;
            routed.physical_plan = Some(routed_plan);
            requests.push((transport, routed));
        }
        Box::pin(async move {
            let mut batches = BTreeMap::new();
            for (transport, request) in requests {
                let GatewayQueryResponse::Materialized(response) =
                    transport.execute_query(request).await?
                else {
                    return Err(GatewayExecutionError::new(
                        "DTG-CLUSTER-ROUTING",
                        "shard-routed query transport returned a non-materialized response",
                        GatewayRetry::Safe,
                    ));
                };
                for (fragment_id, fragment_batches) in response {
                    if batches.insert(fragment_id, fragment_batches).is_some() {
                        return Err(GatewayExecutionError::new(
                            "DTG-CLUSTER-ROUTING",
                            "two shard routes returned the same fragment",
                            GatewayRetry::Never,
                        ));
                    }
                }
            }
            Ok(GatewayQueryResponse::Materialized(batches))
        })
    }

    fn execute_query_with_metrics(
        &self,
        request: GatewayClusterRequest,
        metrics: Arc<RequestStageMetrics>,
    ) -> GatewayFuture<'_, Result<GatewayQueryResponse, GatewayExecutionError>> {
        let Some(plan) = request.physical_plan.as_ref() else {
            return self.default.execute_query_with_metrics(request, metrics);
        };
        let mut grouped = BTreeMap::<u64, Vec<dtg_plan::PlanFragment>>::new();
        for fragment in plan.fragments() {
            grouped
                .entry(fragment.fence().shard_id().get())
                .or_default()
                .push(fragment.clone());
        }
        let mut requests = Vec::with_capacity(grouped.len());
        for (shard_id, fragments) in grouped {
            let transport = self.routes.get(&shard_id).unwrap_or(&self.default).clone();
            let mut routed = request.clone();
            let mut routed_plan = plan.clone();
            routed_plan.fragments = fragments;
            routed.physical_plan = Some(routed_plan);
            requests.push((transport, routed));
        }
        Box::pin(async move {
            let mut batches = BTreeMap::new();
            for (transport, request) in requests {
                let GatewayQueryResponse::Materialized(response) = transport
                    .execute_query_with_metrics(request, Arc::clone(&metrics))
                    .await?
                else {
                    return Err(GatewayExecutionError::new(
                        "DTG-CLUSTER-ROUTING",
                        "shard-routed query transport returned a non-materialized response",
                        GatewayRetry::Safe,
                    ));
                };
                for (fragment_id, fragment_batches) in response {
                    if batches.insert(fragment_id, fragment_batches).is_some() {
                        return Err(GatewayExecutionError::new(
                            "DTG-CLUSTER-ROUTING",
                            "two shard routes returned the same fragment",
                            GatewayRetry::Never,
                        ));
                    }
                }
            }
            Ok(GatewayQueryResponse::Materialized(batches))
        })
    }

    fn execute_query_with_metrics_and_cancellation(
        &self,
        request: GatewayClusterRequest,
        metrics: Arc<RequestStageMetrics>,
        cancellation: &GatewayCancellationToken,
    ) -> GatewayFuture<'_, Result<GatewayQueryResponse, GatewayExecutionError>> {
        let Some(plan) = request.physical_plan.as_ref() else {
            return self.default.execute_query_with_metrics_and_cancellation(
                request,
                metrics,
                cancellation,
            );
        };
        let mut grouped = BTreeMap::<u64, Vec<dtg_plan::PlanFragment>>::new();
        for fragment in plan.fragments() {
            grouped
                .entry(fragment.fence().shard_id().get())
                .or_default()
                .push(fragment.clone());
        }
        let mut requests = Vec::with_capacity(grouped.len());
        for (shard_id, fragments) in grouped {
            let transport = self.routes.get(&shard_id).unwrap_or(&self.default).clone();
            let mut routed = request.clone();
            let mut routed_plan = plan.clone();
            routed_plan.fragments = fragments;
            routed.physical_plan = Some(routed_plan);
            requests.push((transport, routed));
        }
        let cancellation = cancellation.clone();
        Box::pin(async move {
            let mut batches = BTreeMap::new();
            for (transport, request) in requests {
                let GatewayQueryResponse::Materialized(response) = transport
                    .execute_query_with_metrics_and_cancellation(
                        request,
                        Arc::clone(&metrics),
                        &cancellation,
                    )
                    .await?
                else {
                    return Err(GatewayExecutionError::new(
                        "DTG-CLUSTER-ROUTING",
                        "shard-routed query transport returned a non-materialized response",
                        GatewayRetry::Safe,
                    ));
                };
                for (fragment_id, fragment_batches) in response {
                    if batches.insert(fragment_id, fragment_batches).is_some() {
                        return Err(GatewayExecutionError::new(
                            "DTG-CLUSTER-ROUTING",
                            "two shard routes returned the same fragment",
                            GatewayRetry::Never,
                        ));
                    }
                }
            }
            Ok(GatewayQueryResponse::Materialized(batches))
        })
    }
}

pub struct TonicGatewayProtocolV2TransportFactory {
    query_sessions_enabled: bool,
    query_pipelines_enabled: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum GatewayDataEndpoint {
    Tcp(String),
    Unix(PathBuf),
}

fn parse_gateway_data_endpoint(endpoint: &str) -> Result<GatewayDataEndpoint, &'static str> {
    if let Some(path) = endpoint.strip_prefix("unix://") {
        if path.is_empty() || !path.starts_with('/') {
            return Err("unix endpoint must use an absolute socket path");
        }
        return Ok(GatewayDataEndpoint::Unix(PathBuf::from(path)));
    }
    if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        return Ok(GatewayDataEndpoint::Tcp(endpoint.to_owned()));
    }
    Err("gateway data endpoint must use http(s):// or unix://")
}

async fn connect_gateway_data_channel(endpoint: &str) -> Result<Channel, GatewayExecutionError> {
    parse_gateway_data_endpoint(endpoint).map_err(|error| {
        GatewayExecutionError::new("DTG-CLUSTER-CONNECT", error, GatewayRetry::Safe)
    })?;
    Endpoint::from_shared(endpoint.to_owned())
        .map_err(|error| {
            GatewayExecutionError::new("DTG-CLUSTER-CONNECT", error.to_string(), GatewayRetry::Safe)
        })?
        .connect()
        .await
        .map_err(cluster_connect_error)
}

impl TonicGatewayProtocolV2TransportFactory {
    pub const fn new(query_sessions_enabled: bool) -> Self {
        Self {
            query_sessions_enabled,
            query_pipelines_enabled: false,
        }
    }

    pub const fn with_query_pipelines(mut self, query_pipelines_enabled: bool) -> Self {
        self.query_pipelines_enabled = query_pipelines_enabled;
        self
    }
}

impl Default for TonicGatewayProtocolV2TransportFactory {
    fn default() -> Self {
        Self::new(true)
    }
}

impl GatewayExecutionTransportFactory for TonicGatewayProtocolV2TransportFactory {
    fn connect<'a>(
        &'a self,
        endpoint: &'a str,
    ) -> GatewayFuture<'a, Result<Arc<dyn GatewayExecutionTransport>, GatewayExecutionError>> {
        Box::pin(async move {
            let channel = connect_gateway_data_channel(endpoint).await?;
            let client = proto::gateway_service_client::GatewayServiceClient::new(channel);
            let session = if self.query_sessions_enabled {
                TonicGatewaySessionClient::connect(client.clone())
                    .await
                    .ok()
                    .map(Arc::new)
            } else {
                None
            };
            let pipeline = if self.query_pipelines_enabled {
                match TonicGatewayPipelineClient::connect(client.clone()).await {
                    Ok(pipeline) => Some(Arc::new(pipeline)),
                    Err(_) => None,
                }
            } else {
                None
            };
            let client: Arc<dyn GatewayProtocolV2Client> = Arc::new(TonicGatewayProtocolV2Client {
                client,
                session,
                pipeline,
            });
            Ok(Arc::new(GatewayProtocolV2Transport::new(client))
                as Arc<dyn GatewayExecutionTransport>)
        })
    }
}

#[derive(Clone)]
struct TonicGatewayProtocolV2Client {
    client: proto::gateway_service_client::GatewayServiceClient<Channel>,
    session: Option<Arc<TonicGatewaySessionClient>>,
    pipeline: Option<Arc<TonicGatewayPipelineClient>>,
}

struct TonicGatewaySessionClient {
    sender: mpsc::Sender<proto::GatewayRequest>,
    pending: GatewaySessionPending,
    active: Arc<AtomicBool>,
}

impl TonicGatewayPipelineClient {
    async fn connect(
        mut client: proto::gateway_service_client::GatewayServiceClient<Channel>,
    ) -> Result<Self, GatewayExecutionError> {
        let (frames, frame_receiver) = mpsc::channel(PIPELINE_MAX_BATCH_REQUESTS);
        let responses = client
            .execute_pipelined(ReceiverStream::new(frame_receiver))
            .await
            .map_err(cluster_rpc_error)?
            .into_inner();
        let (submissions, submission_receiver) = mpsc::channel(PIPELINE_MAX_PENDING);
        let pending = Arc::new(Mutex::new(BTreeMap::new()));
        let active = Arc::new(AtomicBool::new(true));
        let credits = Arc::new(AtomicUsize::new(0));
        let credit_notify = Arc::new(Notify::new());
        tokio::spawn(read_gateway_pipeline_responses(
            responses,
            Arc::clone(&pending),
            Arc::clone(&active),
            Arc::clone(&credits),
            Arc::clone(&credit_notify),
        ));
        tokio::spawn(write_gateway_pipeline_requests(
            submission_receiver,
            frames.clone(),
            Arc::clone(&pending),
            Arc::clone(&active),
            Arc::clone(&credits),
            Arc::clone(&credit_notify),
        ));
        Ok(Self {
            submissions,
            frames,
            pending,
            active,
            credit_notify,
        })
    }

    async fn execute_with_metrics_and_cancellation(
        &self,
        request: proto::GatewayRequest,
        metrics: Arc<RequestStageMetrics>,
        cancellation: &GatewayCancellationToken,
    ) -> Result<Vec<proto::GatewayResponse>, GatewayExecutionError> {
        let cancellation = cancellation.clone();
        let submit_started = Instant::now();
        let request_id = gateway_session_request_id(request.request.as_ref())?;
        let (sender, receiver) = oneshot::channel();
        {
            let mut pending = self.pending.lock().map_err(|_| {
                GatewayExecutionError::new(
                    "DTG-CLUSTER-PIPELINE",
                    "Gateway pipeline pending-request lock is poisoned",
                    GatewayRetry::Safe,
                )
            })?;
            if !self.active.load(Ordering::Acquire) {
                record_session_metric(
                    &Some(Arc::clone(&metrics)),
                    RequestDetail::GatewayQueryPipelineSubmit,
                    StageOutcome::Error,
                    submit_started.elapsed(),
                );
                return Err(gateway_pipeline_unavailable("Gateway pipeline is closed"));
            }
            if pending.len() >= PIPELINE_MAX_PENDING {
                return Err(GatewayExecutionError::new(
                    "DTG-CLUSTER-PIPELINE-BACKPRESSURE",
                    "Gateway pipeline pending-request limit is exhausted",
                    GatewayRetry::Safe,
                ));
            }
            if pending.insert(request_id, sender).is_some() {
                return Err(GatewayExecutionError::new(
                    "DTG-CLUSTER-PIPELINE-ID",
                    "Gateway pipeline received a duplicate in-flight request ID",
                    GatewayRetry::Safe,
                ));
            }
        }
        match self.submissions.try_send(GatewayPipelineSubmission {
            request_id,
            request: request.clone(),
        }) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                remove_gateway_pipeline_pending(&self.pending, request_id)?;
                return Err(GatewayExecutionError::new(
                    "DTG-CLUSTER-PIPELINE-BACKPRESSURE",
                    "Gateway pipeline ingress queue is full",
                    GatewayRetry::Safe,
                ));
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                remove_gateway_pipeline_pending(&self.pending, request_id)?;
                return Err(gateway_pipeline_unavailable(
                    "Gateway pipeline request writer is closed",
                ));
            }
        }
        record_session_metric(
            &Some(Arc::clone(&metrics)),
            RequestDetail::GatewayQueryPipelineSubmit,
            StageOutcome::Success,
            submit_started.elapsed(),
        );
        let wait_started = Instant::now();
        let response = tokio::select! {
            response = receiver => response.map_err(|_| gateway_pipeline_unavailable("Gateway pipeline response dispatcher stopped"))?,
            _ = cancellation.cancelled() => {
                self.cancel(request_id, request.request).await;
                Err(gateway_execution_cancelled())
            }
        };
        record_session_metric(
            &Some(metrics),
            RequestDetail::GatewayQueryPipelineResponseWait,
            if response.is_ok() {
                StageOutcome::Success
            } else if cancellation.is_cancelled() {
                StageOutcome::Cancelled
            } else {
                StageOutcome::Error
            },
            wait_started.elapsed(),
        );
        response
    }

    async fn cancel(&self, request_id: u128, request: Option<proto::RequestContext>) {
        let present = match self.pending.lock() {
            Ok(pending) => pending.contains_key(&request_id),
            Err(_) => false,
        };
        if !present {
            return;
        }
        let frame = proto::GatewayPipelineClientFrame {
            payload: Some(proto::gateway_pipeline_client_frame::Payload::Cancel(
                proto::GatewayPipelineCancel { request },
            )),
        };
        if self.frames.send(frame).await.is_err() {
            fail_gateway_pipeline(
                &self.pending,
                &self.active,
                &self.credit_notify,
                gateway_pipeline_unavailable("Gateway pipeline request stream is closed"),
            );
        }
    }
}

async fn write_gateway_pipeline_requests(
    mut submissions: mpsc::Receiver<GatewayPipelineSubmission>,
    frames: mpsc::Sender<proto::GatewayPipelineClientFrame>,
    pending: GatewayPipelinePending,
    active: Arc<AtomicBool>,
    credits: Arc<AtomicUsize>,
    credit_notify: Arc<Notify>,
) {
    let mut deferred = None;
    loop {
        let first = match deferred.take() {
            Some(submission) => submission,
            None => match submissions.recv().await {
                Some(submission) => submission,
                None => return,
            },
        };
        let mut batch = GatewayPipelineBatch::default();
        if let Err(submission) = push_active_pipeline_submission(&pending, &mut batch, first) {
            let submission = *submission;
            if batch.is_empty() {
                fail_gateway_pipeline_request(
                    &pending,
                    submission.request_id,
                    GatewayExecutionError::new(
                        "DTG-CLUSTER-PIPELINE-BACKPRESSURE",
                        "Gateway pipeline request exceeds the maximum batch size",
                        GatewayRetry::Never,
                    ),
                );
                continue;
            }
            deferred = Some(submission);
        }
        loop {
            if deferred.is_some() {
                break;
            }
            match submissions.try_recv() {
                Ok(submission) => {
                    if let Err(submission) =
                        push_active_pipeline_submission(&pending, &mut batch, submission)
                    {
                        deferred = Some(*submission);
                        break;
                    }
                    if batch.requests.len() == PIPELINE_MAX_BATCH_REQUESTS {
                        break;
                    }
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => break,
            }
        }
        if batch.is_empty() {
            continue;
        }
        while !batch.is_empty() {
            let request_count = match reserve_gateway_pipeline_credits(
                batch.requests.len(),
                &active,
                &credits,
                &credit_notify,
            )
            .await
            {
                Ok(request_count) => request_count,
                Err(()) => {
                    fail_gateway_pipeline(
                        &pending,
                        &active,
                        &credit_notify,
                        gateway_pipeline_unavailable(
                            "Gateway pipeline became inactive while awaiting credits",
                        ),
                    );
                    return;
                }
            };
            let frame = proto::GatewayPipelineClientFrame {
                payload: Some(proto::gateway_pipeline_client_frame::Payload::Batch(
                    proto::GatewayPipelineRequestBatch {
                        requests: batch.take_prefix(request_count),
                    },
                )),
            };
            if frames.send(frame).await.is_err() {
                fail_gateway_pipeline(
                    &pending,
                    &active,
                    &credit_notify,
                    gateway_pipeline_unavailable("Gateway pipeline request stream is closed"),
                );
                return;
            }
        }
    }
}

fn push_active_pipeline_submission(
    pending: &GatewayPipelinePending,
    batch: &mut GatewayPipelineBatch,
    submission: GatewayPipelineSubmission,
) -> Result<(), Box<GatewayPipelineSubmission>> {
    let active = pending
        .lock()
        .map(|pending| pending.contains_key(&submission.request_id))
        .unwrap_or(false);
    if !active {
        return Ok(());
    }
    let request_bytes = gateway_pipeline_request_size(&submission.request);
    if batch.requests.len() >= PIPELINE_MAX_BATCH_REQUESTS
        || batch.estimated_bytes.saturating_add(request_bytes) > PIPELINE_MAX_BATCH_BYTES
    {
        return Err(Box::new(submission));
    }
    batch
        .push(submission.request)
        .expect("pipeline batch capacity was checked before push");
    Ok(())
}

async fn reserve_gateway_pipeline_credits(
    requested: usize,
    active: &Arc<AtomicBool>,
    credits: &Arc<AtomicUsize>,
    credit_notify: &Arc<Notify>,
) -> Result<usize, ()> {
    loop {
        if !active.load(Ordering::Acquire) {
            return Err(());
        }
        let available = credits.load(Ordering::Acquire);
        let granted = available.min(requested);
        if granted != 0
            && credits
                .compare_exchange(
                    available,
                    available - granted,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
        {
            return Ok(granted);
        }
        let notified = credit_notify.notified();
        if !active.load(Ordering::Acquire) {
            return Err(());
        }
        // `notify_waiters` does not retain a permit. Subscribe before the
        // second credit check so a completion between the first zero read and
        // the wait cannot strand the writer with credits already available.
        if credits.load(Ordering::Acquire) != 0 {
            continue;
        }
        notified.await;
    }
}

async fn read_gateway_pipeline_responses(
    mut responses: tonic::Streaming<proto::GatewayPipelineServerFrame>,
    pending: GatewayPipelinePending,
    active: Arc<AtomicBool>,
    credits: Arc<AtomicUsize>,
    credit_notify: Arc<Notify>,
) {
    loop {
        let frame = match responses.message().await {
            Ok(Some(frame)) => frame,
            Ok(None) => {
                fail_gateway_pipeline(
                    &pending,
                    &active,
                    &credit_notify,
                    gateway_pipeline_unavailable("Gateway pipeline response stream closed"),
                );
                return;
            }
            Err(error) => {
                fail_gateway_pipeline(
                    &pending,
                    &active,
                    &credit_notify,
                    GatewayExecutionError::new(
                        "DTG-CLUSTER-STREAM",
                        error.to_string(),
                        GatewayRetry::Safe,
                    ),
                );
                return;
            }
        };
        if frame.returned_credits != 0 {
            credits.fetch_add(frame.returned_credits as usize, Ordering::AcqRel);
            credit_notify.notify_waiters();
        }
        match frame.payload {
            Some(proto::gateway_pipeline_server_frame::Payload::Credit(credit)) => {
                credits.fetch_add(credit.available_requests as usize, Ordering::AcqRel);
                credit_notify.notify_waiters();
            }
            Some(proto::gateway_pipeline_server_frame::Payload::Response(response)) => {
                let request_id = match gateway_session_request_id(response.request.as_ref()) {
                    Ok(request_id) => request_id,
                    Err(error) => {
                        fail_gateway_pipeline(&pending, &active, &credit_notify, error);
                        return;
                    }
                };
                let sender = match pending.lock() {
                    Ok(mut pending) => pending.remove(&request_id),
                    Err(_) => {
                        fail_gateway_pipeline(
                            &pending,
                            &active,
                            &credit_notify,
                            GatewayExecutionError::new(
                                "DTG-CLUSTER-PIPELINE",
                                "Gateway pipeline pending-request lock is poisoned",
                                GatewayRetry::Safe,
                            ),
                        );
                        return;
                    }
                };
                if let Some(sender) = sender {
                    let _ = sender.send(Ok(response.responses));
                }
            }
            None => {
                fail_gateway_pipeline(
                    &pending,
                    &active,
                    &credit_notify,
                    GatewayExecutionError::new(
                        "DTG-CLUSTER-PIPELINE",
                        "Gateway pipeline response frame omitted a payload",
                        GatewayRetry::Safe,
                    ),
                );
                return;
            }
        }
    }
}

fn remove_gateway_pipeline_pending(
    pending: &GatewayPipelinePending,
    request_id: u128,
) -> Result<(), GatewayExecutionError> {
    pending
        .lock()
        .map(|mut pending| {
            pending.remove(&request_id);
        })
        .map_err(|_| {
            GatewayExecutionError::new(
                "DTG-CLUSTER-PIPELINE",
                "Gateway pipeline pending-request lock is poisoned",
                GatewayRetry::Safe,
            )
        })
}

fn fail_gateway_pipeline_request(
    pending: &GatewayPipelinePending,
    request_id: u128,
    error: GatewayExecutionError,
) {
    let sender = pending
        .lock()
        .ok()
        .and_then(|mut pending| pending.remove(&request_id));
    if let Some(sender) = sender {
        let _ = sender.send(Err(error));
    }
}

fn gateway_pipeline_unavailable(message: impl Into<String>) -> GatewayExecutionError {
    GatewayExecutionError::new(
        "DTG-CLUSTER-PIPELINE-UNAVAILABLE",
        message,
        GatewayRetry::Safe,
    )
}

fn gateway_execution_cancelled() -> GatewayExecutionError {
    GatewayExecutionError::new(
        "DTG-EXECUTION-CANCELLED",
        "Gateway request was cancelled",
        GatewayRetry::Never,
    )
}

fn fail_gateway_pipeline(
    pending: &GatewayPipelinePending,
    active: &Arc<AtomicBool>,
    credit_notify: &Arc<Notify>,
    error: GatewayExecutionError,
) {
    active.store(false, Ordering::Release);
    credit_notify.notify_waiters();
    let entries = match pending.lock() {
        Ok(mut pending) => std::mem::take(&mut *pending),
        Err(_) => return,
    };
    for (_, sender) in entries {
        let _ = sender.send(Err(error.clone()));
    }
}

impl TonicGatewaySessionClient {
    async fn connect(
        mut client: proto::gateway_service_client::GatewayServiceClient<Channel>,
    ) -> Result<Self, GatewayExecutionError> {
        let (sender, receiver) = mpsc::channel(32);
        let responses = client
            .execute_session(ReceiverStream::new(receiver))
            .await
            .map_err(cluster_rpc_error)?
            .into_inner();
        let pending = Arc::new(Mutex::new(BTreeMap::new()));
        let active = Arc::new(AtomicBool::new(true));
        tokio::spawn(read_gateway_session_responses(
            responses,
            Arc::clone(&pending),
            Arc::clone(&active),
        ));
        Ok(Self {
            sender,
            pending,
            active,
        })
    }

    async fn execute(
        &self,
        request: proto::GatewayRequest,
    ) -> Result<Vec<proto::GatewayResponse>, GatewayExecutionError> {
        self.execute_inner(request, None).await
    }

    async fn execute_with_metrics(
        &self,
        request: proto::GatewayRequest,
        metrics: Arc<RequestStageMetrics>,
    ) -> Result<Vec<proto::GatewayResponse>, GatewayExecutionError> {
        self.execute_inner(request, Some(metrics)).await
    }

    async fn execute_inner(
        &self,
        request: proto::GatewayRequest,
        metrics: Option<Arc<RequestStageMetrics>>,
    ) -> Result<Vec<proto::GatewayResponse>, GatewayExecutionError> {
        let submit_started = Instant::now();
        let request_id = gateway_session_request_id(request.request.as_ref())?;
        let (sender, receiver) = oneshot::channel();
        {
            let mut pending = self.pending.lock().map_err(|_| {
                GatewayExecutionError::new(
                    "DTG-CLUSTER-SESSION",
                    "Gateway session pending-request lock is poisoned",
                    GatewayRetry::Safe,
                )
            })?;
            if !self.active.load(Ordering::Acquire) {
                record_session_metric(
                    &metrics,
                    RequestDetail::GatewayQuerySessionSubmit,
                    StageOutcome::Error,
                    submit_started.elapsed(),
                );
                return Err(gateway_session_unavailable("Gateway session is closed"));
            }
            if pending.insert(request_id, sender).is_some() {
                return Err(GatewayExecutionError::new(
                    "DTG-CLUSTER-SESSION-ID",
                    "Gateway session received a duplicate in-flight request ID",
                    GatewayRetry::Safe,
                ));
            }
        }
        if self.sender.send(request).await.is_err() {
            let mut pending = self.pending.lock().map_err(|_| {
                GatewayExecutionError::new(
                    "DTG-CLUSTER-SESSION",
                    "Gateway session pending-request lock is poisoned",
                    GatewayRetry::Safe,
                )
            })?;
            pending.remove(&request_id);
            record_session_metric(
                &metrics,
                RequestDetail::GatewayQuerySessionSubmit,
                StageOutcome::Error,
                submit_started.elapsed(),
            );
            return Err(gateway_session_unavailable(
                "Gateway session request stream is closed",
            ));
        }
        record_session_metric(
            &metrics,
            RequestDetail::GatewayQuerySessionSubmit,
            StageOutcome::Success,
            submit_started.elapsed(),
        );
        let wait_started = Instant::now();
        let response = receiver.await.map_err(|_| {
            gateway_session_unavailable("Gateway session response dispatcher stopped")
        })?;
        record_session_metric(
            &metrics,
            RequestDetail::GatewayQuerySessionResponseWait,
            if response.is_ok() {
                StageOutcome::Success
            } else {
                StageOutcome::Error
            },
            wait_started.elapsed(),
        );
        response
    }
}

fn record_session_metric(
    metrics: &Option<Arc<RequestStageMetrics>>,
    detail: RequestDetail,
    outcome: StageOutcome,
    elapsed: Duration,
) {
    if let Some(metrics) = metrics {
        metrics.record_detail(
            detail,
            outcome,
            u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX),
        );
    }
}

async fn read_gateway_session_responses(
    mut responses: tonic::Streaming<proto::GatewaySessionResponse>,
    pending: GatewaySessionPending,
    active: Arc<AtomicBool>,
) {
    loop {
        let frame = match responses.message().await {
            Ok(Some(frame)) => frame,
            Ok(None) => {
                fail_gateway_session(
                    &pending,
                    &active,
                    gateway_session_unavailable("Gateway session response stream closed"),
                );
                return;
            }
            Err(error) => {
                fail_gateway_session(
                    &pending,
                    &active,
                    GatewayExecutionError::new(
                        "DTG-CLUSTER-STREAM",
                        error.to_string(),
                        GatewayRetry::Safe,
                    ),
                );
                return;
            }
        };
        let request_id = match gateway_session_request_id(frame.request.as_ref()) {
            Ok(request_id) => request_id,
            Err(error) => {
                fail_gateway_session(&pending, &active, error);
                return;
            }
        };
        let sender = match pending.lock() {
            Ok(mut pending) => pending.remove(&request_id),
            Err(_) => {
                fail_gateway_session(
                    &pending,
                    &active,
                    GatewayExecutionError::new(
                        "DTG-CLUSTER-SESSION",
                        "Gateway session pending-request lock is poisoned",
                        GatewayRetry::Safe,
                    ),
                );
                return;
            }
        };
        let Some(sender) = sender else {
            fail_gateway_session(
                &pending,
                &active,
                GatewayExecutionError::new(
                    "DTG-CLUSTER-SESSION-ID",
                    "Gateway session response did not match an in-flight request",
                    GatewayRetry::Safe,
                ),
            );
            return;
        };
        let _ = sender.send(Ok(frame.responses));
    }
}

fn gateway_session_request_id(
    context: Option<&proto::RequestContext>,
) -> Result<u128, GatewayExecutionError> {
    let context = context.ok_or_else(|| {
        GatewayExecutionError::new(
            "DTG-CLUSTER-SESSION-ID",
            "Gateway session frame omitted request identity",
            GatewayRetry::Safe,
        )
    })?;
    let bytes: [u8; 16] = context.request_id.as_slice().try_into().map_err(|_| {
        GatewayExecutionError::new(
            "DTG-CLUSTER-SESSION-ID",
            "Gateway session request identity must contain 16 bytes",
            GatewayRetry::Safe,
        )
    })?;
    let request_id = u128::from_be_bytes(bytes);
    if request_id == 0 {
        return Err(GatewayExecutionError::new(
            "DTG-CLUSTER-SESSION-ID",
            "Gateway session request identity must be non-zero",
            GatewayRetry::Safe,
        ));
    }
    Ok(request_id)
}

fn gateway_session_unavailable(message: impl Into<String>) -> GatewayExecutionError {
    GatewayExecutionError::new("DTG-CLUSTER-SESSION", message, GatewayRetry::Safe)
}

fn fail_gateway_session(
    pending: &GatewaySessionPending,
    active: &Arc<AtomicBool>,
    error: GatewayExecutionError,
) {
    active.store(false, Ordering::Release);
    let entries = match pending.lock() {
        Ok(mut pending) => std::mem::take(&mut *pending),
        Err(_) => return,
    };
    for (_, sender) in entries {
        let _ = sender.send(Err(error.clone()));
    }
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

    fn execute_session(
        &self,
        request: proto::GatewayRequest,
    ) -> GatewayFuture<'_, Result<Option<Vec<proto::GatewayResponse>>, GatewayExecutionError>> {
        let session = self.session.clone();
        Box::pin(async move {
            let Some(session) = session else {
                return Ok(None);
            };
            session.execute(request).await.map(Some)
        })
    }

    fn execute_session_with_metrics(
        &self,
        request: proto::GatewayRequest,
        metrics: Arc<RequestStageMetrics>,
    ) -> GatewayFuture<'_, Result<Option<Vec<proto::GatewayResponse>>, GatewayExecutionError>> {
        let session = self.session.clone();
        Box::pin(async move {
            let Some(session) = session else {
                return Ok(None);
            };
            session
                .execute_with_metrics(request, metrics)
                .await
                .map(Some)
        })
    }

    fn execute_pipeline_with_metrics_and_cancellation(
        &self,
        request: proto::GatewayRequest,
        metrics: Arc<RequestStageMetrics>,
        cancellation: &GatewayCancellationToken,
    ) -> GatewayFuture<'_, Result<Option<Vec<proto::GatewayResponse>>, GatewayExecutionError>> {
        let pipeline = self.pipeline.clone();
        let cancellation = cancellation.clone();
        Box::pin(async move {
            let Some(pipeline) = pipeline else {
                return Ok(None);
            };
            pipeline
                .execute_with_metrics_and_cancellation(request, metrics, &cancellation)
                .await
                .map(Some)
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

    fn execute_query(
        &self,
        request: GatewayClusterRequest,
    ) -> GatewayFuture<'_, Result<GatewayQueryResponse, GatewayExecutionError>> {
        let wire_request = encode_protocol_v2_request(&request);
        Box::pin(async move {
            if wire_request.fragments.len() == 1
                && let Ok(Some(responses)) = self.client.execute_session(wire_request.clone()).await
            {
                return decode_protocol_v2_query_responses(responses)
                    .map(GatewayQueryResponse::Materialized);
            }
            self.client
                .execute_query(wire_request)
                .await
                .map(GatewayQueryResponse::Materialized)
        })
    }

    fn execute_query_with_metrics(
        &self,
        request: GatewayClusterRequest,
        metrics: Arc<RequestStageMetrics>,
    ) -> GatewayFuture<'_, Result<GatewayQueryResponse, GatewayExecutionError>> {
        let encode = metrics.start_detail(RequestDetail::GatewayQueryRequestEncode);
        let wire_request = encode_protocol_v2_request(&request);
        encode.finish(StageOutcome::Success);
        Box::pin(async move {
            let collect = metrics.start_detail(RequestDetail::GatewayQueryResponseCollect);
            let responses = async {
                if wire_request.fragments.len() == 1
                    && let Ok(Some(responses)) = self
                        .client
                        .execute_session_with_metrics(wire_request.clone(), Arc::clone(&metrics))
                        .await
                {
                    return Ok(responses);
                }
                self.client.execute(wire_request).await
            }
            .await;
            let responses = collect.finish_result(responses)?;
            let decode = metrics.start_detail(RequestDetail::GatewayQueryResponseDecode);
            decode
                .finish_result(decode_protocol_v2_query_responses(responses))
                .map(GatewayQueryResponse::Materialized)
        })
    }

    fn execute_query_with_metrics_and_cancellation(
        &self,
        request: GatewayClusterRequest,
        metrics: Arc<RequestStageMetrics>,
        cancellation: &GatewayCancellationToken,
    ) -> GatewayFuture<'_, Result<GatewayQueryResponse, GatewayExecutionError>> {
        let encode = metrics.start_detail(RequestDetail::GatewayQueryRequestEncode);
        let wire_request = encode_protocol_v2_request(&request);
        encode.finish(StageOutcome::Success);
        let cancellation = cancellation.clone();
        Box::pin(async move {
            let collect = metrics.start_detail(RequestDetail::GatewayQueryResponseCollect);
            let responses = async {
                if wire_request.fragments.len() == 1 {
                    match self
                        .client
                        .execute_pipeline_with_metrics_and_cancellation(
                            wire_request.clone(),
                            Arc::clone(&metrics),
                            &cancellation,
                        )
                        .await
                    {
                        Ok(Some(responses)) => return Ok(responses),
                        Ok(None) => {}
                        Err(error) if error.code() == "DTG-CLUSTER-PIPELINE-UNAVAILABLE" => {}
                        Err(error) => return Err(error),
                    }
                }
                if wire_request.fragments.len() == 1
                    && let Ok(Some(responses)) = self
                        .client
                        .execute_session_with_metrics(wire_request.clone(), Arc::clone(&metrics))
                        .await
                {
                    return Ok(responses);
                }
                self.client.execute(wire_request).await
            }
            .await;
            let responses = collect.finish_result(responses)?;
            let decode = metrics.start_detail(RequestDetail::GatewayQueryResponseDecode);
            decode
                .finish_result(decode_protocol_v2_query_responses(responses))
                .map(GatewayQueryResponse::Materialized)
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
        planner: Planner,
        query: QueryRuntime,
        planning_context: Arc<RwLock<PlanningContext>>,
        transport: Arc<dyn GatewayExecutionTransport>,
        write_transport: Option<Arc<dyn GatewayWriteTransport>>,
        write_accounting: Arc<ProcessWriteAccounting>,
    },
}

pub struct GatewayExecution {
    language: Language,
    mode: GatewayExecutionMode,
    request_metrics: Arc<RequestStageMetrics>,
}

impl GatewayExecution {
    pub fn builder() -> GatewayExecutionBuilder {
        GatewayExecutionBuilder::default()
    }

    pub fn for_process(
        transport: Arc<dyn GatewayExecutionTransport>,
        planning_context: PlanningContext,
    ) -> Self {
        Self {
            language: Language::new(Arc::new(EmptySchemaCatalog)),
            mode: GatewayExecutionMode::Process {
                planner: Planner,
                query: QueryRuntime::new(1024),
                planning_context: Arc::new(RwLock::new(planning_context)),
                transport,
                write_transport: None,
                write_accounting: Arc::new(ProcessWriteAccounting::default()),
            },
            request_metrics: Arc::new(RequestStageMetrics::default()),
        }
    }

    pub fn for_process_with_writes(
        transport: Arc<dyn GatewayExecutionTransport>,
        write_transport: Arc<dyn GatewayWriteTransport>,
        planning_context: PlanningContext,
    ) -> Self {
        let mut execution = Self::for_process(transport, planning_context);
        let GatewayExecutionMode::Process {
            write_transport: slot,
            ..
        } = &mut execution.mode
        else {
            unreachable!("process constructor creates process execution")
        };
        *slot = Some(write_transport);
        execution
    }

    pub fn install_planning_context(
        &self,
        next: PlanningContext,
    ) -> Result<bool, GatewayExecutionError> {
        let GatewayExecutionMode::Process {
            planning_context, ..
        } = &self.mode
        else {
            return Err(GatewayExecutionError::new(
                "DTG-EXECUTION-CATALOG-MODE",
                "catalog installation requires process Gateway execution",
                GatewayRetry::Never,
            ));
        };
        let mut current = planning_context.write().map_err(|_| {
            GatewayExecutionError::new(
                "DTG-EXECUTION-CATALOG-LOCK",
                "Gateway planning catalog lock is poisoned",
                GatewayRetry::Safe,
            )
        })?;
        validate_planning_context_update(&current, &next)?;
        if *current == next {
            return Ok(false);
        }
        *current = next;
        Ok(true)
    }

    pub fn compile(&self, source: &str) -> Result<LogicalProgram, LanguageError> {
        let timer = self.request_metrics.start(RequestStage::GatewayCompile);
        timer.finish_result(self.language.compile(source))
    }

    pub fn request_metrics(&self) -> Arc<RequestStageMetrics> {
        Arc::clone(&self.request_metrics)
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
        let operators = plan
            .operators()
            .iter()
            .map(lower_operator)
            .collect::<Result<Vec<_>, _>>()?;
        ExecutablePlan::with_operators(
            plan.version,
            fragments,
            plan.root_operator().get(),
            operators,
            materialized_result_schema(plan.result_schema()),
        )
    }

    pub fn lower_plan_with_parameters(
        &self,
        plan: &PhysicalPlan,
        parameters: &BTreeMap<String, GatewayValue>,
    ) -> Result<ExecutablePlan, GatewayExecutionError> {
        validate_exchanges(plan).map_err(gateway_lowering_error)?;
        let fragments = plan
            .fragments()
            .iter()
            .map(lower_fragment)
            .collect::<Result<Vec<_>, _>>()
            .map_err(gateway_lowering_error)?;
        let operators = plan
            .operators()
            .iter()
            .map(|operator| lower_operator_with_parameters(operator, parameters))
            .collect::<Result<Vec<_>, _>>()?;
        ExecutablePlan::with_operators(
            plan.version,
            fragments,
            plan.root_operator().get(),
            operators,
            materialized_result_schema(plan.result_schema()),
        )
        .map_err(gateway_lowering_error)
    }

    pub fn bind_logical_expr(
        expression: &LogicalExpr,
        parameters: &BTreeMap<String, GatewayValue>,
    ) -> Result<LogicalExpr, GatewayExecutionError> {
        bind_logical_expr(expression, parameters)
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
            if let LogicalStatement::Write(write) = &program.statement {
                if transaction_id.is_some() {
                    return Err(GatewayExecutionError::new(
                        "DTG-EXECUTION-WRITE-TRANSACTION",
                        "explicit process transactions do not support writes",
                        GatewayRetry::Never,
                    ));
                }
                let GatewayExecutionMode::Process {
                    planning_context,
                    write_transport,
                    write_accounting,
                    ..
                } = &self.mode
                else {
                    return Err(GatewayExecutionError::new(
                        "DTG-EXECUTION-PROCESS-TRANSPORT",
                        "GatewayExecution was not constructed for process execution",
                        GatewayRetry::Never,
                    ));
                };
                let transport = write_transport.as_ref().ok_or_else(|| {
                    GatewayExecutionError::new(
                        "DTG-EXECUTION-WRITE-TRANSPORT",
                        "process write transport is not configured",
                        GatewayRetry::Never,
                    )
                })?;
                let planning_context = planning_context
                    .read()
                    .map_err(|_| {
                        GatewayExecutionError::new(
                            "DTG-EXECUTION-CATALOG-LOCK",
                            "Gateway planning catalog lock is poisoned",
                            GatewayRetry::Safe,
                        )
                    })?
                    .clone();
                let outcome = execute_process_create(
                    transport.as_ref(),
                    context,
                    write,
                    &parameters,
                    &planning_context,
                    write_accounting,
                    &self.request_metrics,
                )
                .await?;
                account_process_write(
                    write_accounting,
                    match &self.mode {
                        GatewayExecutionMode::Process {
                            planning_context, ..
                        } => planning_context,
                        GatewayExecutionMode::Composed { .. } => unreachable!(),
                    },
                    &outcome,
                )?;
                validate_process_request_end(cancellation)?;
                return Ok(GatewayResponse::Acknowledged);
            }
            let result_fields = program
                .result_schema
                .fields
                .iter()
                .map(|field| field.name.clone())
                .collect();
            let planned = match &program.statement {
                LogicalStatement::Query(_) => {
                    let GatewayExecutionMode::Process {
                        planner,
                        planning_context,
                        ..
                    } = &self.mode
                    else {
                        return Err(GatewayExecutionError::new(
                            "DTG-EXECUTION-PROCESS-TRANSPORT",
                            "GatewayExecution was not constructed for process execution",
                            GatewayRetry::Never,
                        ));
                    };
                    let timer = self.request_metrics.start(RequestStage::GatewayPlan);
                    let result = (|| {
                        let planning_context = planning_context.read().map_err(|_| {
                            GatewayExecutionError::new(
                                "DTG-EXECUTION-CATALOG-LOCK",
                                "Gateway planning catalog lock is poisoned",
                                GatewayRetry::Safe,
                            )
                        })?;
                        let bound_program = bind_process_query_parameters(&program, &parameters)?;
                        let physical_plan = planner
                            .plan(&bound_program, &planning_context)
                            .map_err(|error| {
                                GatewayExecutionError::new(
                                    "DTG-EXECUTION-PLAN",
                                    error.to_string(),
                                    GatewayRetry::Never,
                                )
                            })?;
                        let executable_plan =
                            self.lower_plan_with_parameters(&physical_plan, &parameters)?;
                        Ok((Some(physical_plan), Some(executable_plan)))
                    })();
                    timer.finish_result(result)?
                }
                _ => (None, None),
            };
            let (physical_plan, executable_plan) = planned;
            let partial_vertex_count_alias = physical_plan
                .as_ref()
                .and_then(partial_vertex_count_alias)
                .map(str::to_owned);
            let partial_vertex_count_fragments = physical_plan
                .as_ref()
                .filter(|_| partial_vertex_count_alias.is_some())
                .map(|plan| {
                    plan.fragments()
                        .iter()
                        .map(|fragment| fragment.id().get())
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            let request = GatewayClusterRequest {
                context,
                operation,
                physical_plan,
                parameters,
                transaction_id,
                result_fields,
                temporal_mode,
            };
            let query_deadline = request.context.deadline_unix_ms();
            let expected_result_fields = request.result_fields.clone();
            let GatewayExecutionMode::Process {
                query, transport, ..
            } = &self.mode
            else {
                return Err(GatewayExecutionError::new(
                    "DTG-EXECUTION-PROCESS-TRANSPORT",
                    "GatewayExecution was not constructed for process execution",
                    GatewayRetry::Never,
                ));
            };
            let response = if let Some(executable_plan) = executable_plan {
                let timer = self.request_metrics.start(RequestStage::GatewayInternalRpc);
                let remote = transport
                    .execute_query_with_metrics_and_cancellation(
                        request,
                        Arc::clone(&self.request_metrics),
                        cancellation,
                    )
                    .await;
                let remote = timer.finish_result(remote)?;
                match remote {
                    GatewayQueryResponse::Final(response) => response,
                    GatewayQueryResponse::Materialized(fragment_batches) => {
                        if let Some(alias) = partial_vertex_count_alias.as_deref() {
                            partial_vertex_count_response(
                                fragment_batches,
                                &partial_vertex_count_fragments,
                                alias,
                            )?
                        } else {
                            let timer = self
                                .request_metrics
                                .start(RequestStage::GatewayLocalExecution);
                            let detail_timer = self
                                .request_metrics
                                .start_detail(RequestDetail::GatewayQueryLocalMaterialize);
                            let local = async {
                            let query_cancellation = QueryCancellationToken::new();
                            if cancellation.is_cancelled() {
                                query_cancellation.cancel();
                            }
                            let budget = process_query_budget(query_deadline)?;
                            let mut stream = query
                                .execute_materialized(
                                    &executable_plan,
                                    fragment_batches,
                                    budget,
                                    query_cancellation.clone(),
                                )
                                .await
                                .map_err(gateway_query_error)?;
                            let mut result_rows = Vec::new();
                            while let Some(batch) = {
                                if cancellation.is_cancelled() {
                                    query_cancellation.cancel();
                                }
                                stream.next_batch().await.map_err(gateway_query_error)?
                            } {
                                if batch.schema() != executable_plan.result_schema() {
                                    return Err(GatewayExecutionError::new(
                                        "DTG-EXECUTION-RESULT-SCHEMA",
                                        "query runtime batch schema differs from the executable plan",
                                        GatewayRetry::Never,
                                    ));
                                }
                                result_rows.extend(batch.into_rows());
                            }
                            let fields = executable_plan
                                .result_schema()
                                .fields
                                .iter()
                                .map(|field| field.name.clone())
                                .collect::<Vec<_>>();
                            if fields != expected_result_fields {
                                return Err(GatewayExecutionError::new(
                                    "DTG-EXECUTION-RESULT-SCHEMA",
                                    "query runtime result schema differs from planned result fields",
                                    GatewayRetry::Never,
                                ));
                            }
                            let rows = result_rows
                                .into_iter()
                                .map(|row| {
                                    row.into_iter()
                                        .map(query_value_to_gateway)
                                        .collect::<Result<Vec<_>, _>>()
                                })
                                .collect::<Result<Vec<_>, _>>()?;
                            Ok(GatewayResponse::Rows(GatewayRows::new(fields, rows)?))
                        }
                        .await;
                            let local = detail_timer.finish_result(local)?;
                            timer.finish_result(Ok::<_, GatewayExecutionError>(local))?
                        }
                    }
                }
            } else {
                let timer = self.request_metrics.start(RequestStage::GatewayInternalRpc);
                timer.finish_result(transport.execute(request).await)?
            };
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
                physical_plan: None,
                parameters: BTreeMap::new(),
                transaction_id,
                result_fields: Vec::new(),
                temporal_mode: GatewayTemporalMode::Current,
            };
            let GatewayExecutionMode::Process { transport, .. } = &self.mode else {
                return Err(GatewayExecutionError::new(
                    "DTG-EXECUTION-PROCESS-TRANSPORT",
                    "GatewayExecution was not constructed for process execution",
                    GatewayRetry::Never,
                ));
            };
            let timer = self.request_metrics.start(RequestStage::GatewayInternalRpc);
            let response = timer.finish_result(transport.execute(request).await)?;
            validate_process_request_end(cancellation)?;
            Ok(response)
        })
    }
}

fn validate_planning_context_update(
    current: &PlanningContext,
    next: &PlanningContext,
) -> Result<(), GatewayExecutionError> {
    let current_catalog = current.catalog();
    let next_catalog = next.catalog();
    if next_catalog.version() < current_catalog.version() {
        return Err(GatewayExecutionError::new(
            "DTG-EXECUTION-CATALOG-REVISION-REGRESSION",
            "catalog revision regressed",
            GatewayRetry::Never,
        ));
    }
    if next_catalog.version() == current_catalog.version() {
        return if current == next {
            Ok(())
        } else {
            Err(GatewayExecutionError::new(
                "DTG-EXECUTION-CATALOG-REVISION-CONFLICT",
                "equal catalog revisions contain different planning state",
                GatewayRetry::Never,
            ))
        };
    }
    if next_catalog.graph_id() != current_catalog.graph_id() {
        return Err(GatewayExecutionError::new(
            "DTG-EXECUTION-CATALOG-GRAPH-CONFLICT",
            "catalog update changed the configured graph",
            GatewayRetry::Never,
        ));
    }

    for current_shard in current_catalog.shards() {
        let Some(next_shard) = next_catalog
            .shards()
            .iter()
            .find(|next| next.binding().shard_id() == current_shard.binding().shard_id())
        else {
            return Err(GatewayExecutionError::new(
                "DTG-EXECUTION-CATALOG-SHARD-REMOVED",
                "catalog update removed a previously routed Shard",
                GatewayRetry::Never,
            ));
        };
        let current_binding = current_shard.binding();
        let next_binding = next_shard.binding();
        if next_binding.placement_epoch() < current_binding.placement_epoch() {
            return Err(GatewayExecutionError::new(
                "DTG-EXECUTION-CATALOG-EPOCH-REGRESSION",
                "catalog placement epoch regressed",
                GatewayRetry::Never,
            ));
        }
        if next_binding.backend_generation() < current_binding.backend_generation() {
            return Err(GatewayExecutionError::new(
                "DTG-EXECUTION-CATALOG-GENERATION-REGRESSION",
                "catalog backend generation regressed",
                GatewayRetry::Never,
            ));
        }
        if next_binding.placement_epoch() == current_binding.placement_epoch()
            && next_binding.backend_generation() != current_binding.backend_generation()
        {
            return Err(GatewayExecutionError::new(
                "DTG-EXECUTION-CATALOG-GENERATION-CONFLICT",
                "backend generation changed without a new placement epoch",
                GatewayRetry::Never,
            ));
        }
    }
    Ok(())
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
        P: AnalyticsProjectionProvider + Send + Sync + 'static,
        A: AnalyticsStepProvider + Send + Sync + 'static,
        R: AnalyticsArtifactRepository + Send + Sync + 'static,
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
            request_metrics: Arc::new(RequestStageMetrics::default()),
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
    let fragments = request
        .physical_plan()
        .map(|plan| {
            plan.fragments()
                .iter()
                .map(|fragment| encode_physical_fragment(&context, plan, fragment))
                .collect()
        })
        .unwrap_or_default();
    proto::GatewayRequest {
        request: Some(context),
        execution_request: Some(proto::BoundedPayload {
            format_version: 1,
            declared_len: body.len() as u64,
            item_count,
            checksum: checksum_bytes(&body).to_vec(),
            body,
        }),
        fragments,
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

fn encode_physical_fragment(
    request: &proto::RequestContext,
    plan: &PhysicalPlan,
    fragment: &dtg_plan::PlanFragment,
) -> proto::ExecutionFragment {
    let fence = fragment.fence();
    let snapshot = fence.snapshot_requirements();
    let body = encode_physical_fragment_body(plan, fragment);
    let format_version = u32::try_from(plan.version.get()).unwrap_or(u32::MAX);
    let item_count = u32::try_from(
        plan.operators()
            .len()
            .saturating_add(fragment.storage_accesses().len()),
    )
    .unwrap_or(u32::MAX);
    proto::ExecutionFragment {
        context: Some(proto::ShardContext {
            request: Some(request.clone()),
            graph_id: fence.read_fence().binding().graph_id().get(),
            shard_id: u32::try_from(fence.shard_id().get()).unwrap_or(u32::MAX),
            placement_epoch: fence.placement_epoch().get(),
            backend_generation: fence.backend_generation().get(),
            catalog_version: fence.catalog_version().get(),
        }),
        fragment_id: u128::from(fragment.id().get()).to_be_bytes().to_vec(),
        payload: Some(proto::BoundedPayload {
            format_version,
            declared_len: body.len() as u64,
            item_count,
            checksum: checksum_bytes(&body).to_vec(),
            body,
        }),
        schema_version: fence.schema_version().get(),
        capability_digest: fence.capability_digest().get().to_vec(),
        applied_index: fence.applied_index(),
        transaction_time: snapshot.transaction_time().get(),
        valid_at: snapshot.valid_at(),
        snapshot_immutable: snapshot.immutable(),
    }
}

pub fn encode_physical_fragment_body(
    plan: &PhysicalPlan,
    fragment: &dtg_plan::PlanFragment,
) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&plan.version.get().to_be_bytes());
    body.extend_from_slice(&plan.root_operator().get().to_be_bytes());
    encode_row_schema(plan.result_schema(), &mut body);
    encode_u32(fragment.storage_accesses().len(), &mut body);
    let partial_vertex_count = partial_vertex_count_alias(plan).is_some();
    for access in fragment.storage_accesses() {
        encode_storage_access(access, partial_vertex_count, &mut body);
    }
    encode_u32(plan.operators().len(), &mut body);
    for operator in plan.operators() {
        encode_physical_operator(operator, &mut body);
    }
    body
}

fn encode_storage_access(access: &StorageAccess, partial_vertex_count: bool, output: &mut Vec<u8>) {
    output.extend_from_slice(&access.node().get().to_be_bytes());
    match access {
        StorageAccess::Logical(request) => {
            output.push(0);
            encode_logical_read_operation(request.operation(), partial_vertex_count, output);
            encode_read_scope(request.read_scope(), output);
            output.extend_from_slice(&request.row_bound().to_be_bytes());
        }
        StorageAccess::Pushdown {
            request,
            guarantee,
            residual,
            ..
        } => {
            output.push(1);
            output.extend_from_slice(&request.contract_version().to_be_bytes());
            let capabilities = request.required_capabilities().names().collect::<Vec<_>>();
            encode_u32(capabilities.len(), output);
            for capability in capabilities {
                encode_string(capability, output);
            }
            match request.operation() {
                PushdownOperation::Vertex(read) => {
                    output.push(0);
                    output.extend_from_slice(&read.id().get().to_be_bytes());
                    output.extend_from_slice(&read.valid_at().to_be_bytes());
                    output.extend_from_slice(&read.transaction_at().get().to_be_bytes());
                }
                PushdownOperation::VertexScan(scan) => {
                    output.push(if partial_vertex_count { 2 } else { 1 });
                    output.extend_from_slice(&scan.valid_at().to_be_bytes());
                    output.extend_from_slice(&scan.transaction_at().get().to_be_bytes());
                    match scan.after() {
                        Some(after) => {
                            output.push(1);
                            output.extend_from_slice(&after.get().to_be_bytes());
                        }
                        None => output.push(0),
                    }
                    output.extend_from_slice(&scan.limit().to_be_bytes());
                }
            }
            output.push(semantic_flags(*guarantee));
            match residual {
                Some(residual) => {
                    output.push(1);
                    encode_physical_expr(residual, output);
                }
                None => output.push(0),
            }
        }
    }
}

fn semantic_flags(guarantee: dtg_plan::PushdownGuarantee) -> u8 {
    u8::from(guarantee.temporal())
        | (u8::from(guarantee.nulls()) << 1)
        | (u8::from(guarantee.duplicates()) << 2)
        | (u8::from(guarantee.order()) << 3)
        | (u8::from(guarantee.snapshot()) << 4)
}

fn encode_logical_read_operation(
    operation: &LogicalReadOperation,
    partial_vertex_count: bool,
    output: &mut Vec<u8>,
) {
    match operation {
        LogicalReadOperation::VertexPoint(id) => {
            output.push(0);
            output.extend_from_slice(&id.get().to_be_bytes());
        }
        LogicalReadOperation::VertexScan => output.push(if partial_vertex_count { 2 } else { 1 }),
        LogicalReadOperation::EdgePoint(id) => {
            output.push(2);
            output.extend_from_slice(&id.get().to_be_bytes());
        }
        LogicalReadOperation::EdgeScan => output.push(3),
        LogicalReadOperation::Adjacency {
            vertex_id,
            direction,
        } => {
            output.push(4);
            output.extend_from_slice(&vertex_id.get().to_be_bytes());
            output.push(encode_adjacency_direction(*direction));
        }
        LogicalReadOperation::Traversal {
            vertex_id,
            directions,
        } => {
            output.push(5);
            output.extend_from_slice(&vertex_id.get().to_be_bytes());
            encode_u32(directions.len(), output);
            for direction in directions {
                output.push(encode_adjacency_direction(*direction));
            }
        }
    }
}

fn encode_adjacency_direction(direction: dtg_language_ir::ExpandDirection) -> u8 {
    match direction {
        dtg_language_ir::ExpandDirection::Outgoing => 0,
        dtg_language_ir::ExpandDirection::Incoming => 1,
        dtg_language_ir::ExpandDirection::Either => 2,
    }
}

fn partial_vertex_count_alias(plan: &PhysicalPlan) -> Option<&str> {
    let aggregate = plan.operator(plan.root_operator())?;
    let dtg_plan::PhysicalOperatorKind::Aggregate {
        input,
        groups,
        aggregates,
    } = aggregate.kind()
    else {
        return None;
    };
    let [aggregate] = aggregates.as_slice() else {
        return None;
    };
    if aggregate.function != AggregateKind::Count
        || aggregate.argument.is_some()
        || aggregate.distinct
        || !groups.is_empty()
    {
        return None;
    }
    let source = plan.operator(*input)?;
    let dtg_plan::PhysicalOperatorKind::Source { logical_node, .. } = source.kind() else {
        return None;
    };
    let [field] = plan.result_schema().fields.as_slice() else {
        return None;
    };
    if field.name != aggregate.alias || plan.fragments().is_empty() {
        return None;
    }
    plan.fragments()
        .iter()
        .all(|fragment| partial_vertex_count_access(fragment.storage_accesses(), *logical_node))
        .then_some(aggregate.alias.as_str())
}

fn partial_vertex_count_access(
    accesses: &[StorageAccess],
    logical_node: dtg_language_ir::LogicalNodeId,
) -> bool {
    match accesses {
        [StorageAccess::Logical(request)] => {
            request.node() == logical_node
                && matches!(request.operation(), LogicalReadOperation::VertexScan)
        }
        [
            StorageAccess::Pushdown {
                node,
                request,
                residual: None,
                ..
            },
        ] => {
            *node == logical_node && matches!(request.operation(), PushdownOperation::VertexScan(_))
        }
        _ => false,
    }
}

fn partial_vertex_count_response(
    mut fragments: BTreeMap<u32, Vec<QueryColumnBatch>>,
    expected_fragment_ids: &[u32],
    alias: &str,
) -> Result<GatewayResponse, GatewayExecutionError> {
    let mut total = 0_i64;
    for fragment_id in expected_fragment_ids {
        let batches = fragments.remove(fragment_id).ok_or_else(|| {
            GatewayExecutionError::new(
                "DTG-EXECUTION-PARTIAL-COUNT",
                "partial vertex count omitted an expected fragment",
                GatewayRetry::Safe,
            )
        })?;
        let [batch] = batches.as_slice() else {
            return Err(GatewayExecutionError::new(
                "DTG-EXECUTION-PARTIAL-COUNT",
                "partial vertex count fragment must contain exactly one batch",
                GatewayRetry::Safe,
            ));
        };
        let [field] = batch.schema().fields.as_slice() else {
            return Err(GatewayExecutionError::new(
                "DTG-EXECUTION-PARTIAL-COUNT",
                "partial vertex count batch must contain exactly one field",
                GatewayRetry::Safe,
            ));
        };
        if field.name != PARTIAL_VERTEX_COUNT_FIELD {
            return Err(GatewayExecutionError::new(
                "DTG-EXECUTION-PARTIAL-COUNT",
                "partial vertex count batch has an unexpected field",
                GatewayRetry::Safe,
            ));
        }
        let rows = batch.rows();
        let [row] = rows.as_slice() else {
            return Err(GatewayExecutionError::new(
                "DTG-EXECUTION-PARTIAL-COUNT",
                "partial vertex count batch must contain exactly one row",
                GatewayRetry::Safe,
            ));
        };
        let [QueryValue::Integer(value)] = row.as_slice() else {
            return Err(GatewayExecutionError::new(
                "DTG-EXECUTION-PARTIAL-COUNT",
                "partial vertex count row must contain one non-negative integer",
                GatewayRetry::Safe,
            ));
        };
        if *value < 0 {
            return Err(GatewayExecutionError::new(
                "DTG-EXECUTION-PARTIAL-COUNT",
                "partial vertex count row must contain one non-negative integer",
                GatewayRetry::Safe,
            ));
        }
        total = total.checked_add(*value).ok_or_else(|| {
            GatewayExecutionError::new(
                "DTG-EXECUTION-PARTIAL-COUNT",
                "partial vertex count exceeds the supported integer range",
                GatewayRetry::Never,
            )
        })?;
    }
    if !fragments.is_empty() {
        return Err(GatewayExecutionError::new(
            "DTG-EXECUTION-PARTIAL-COUNT",
            "partial vertex count included an unknown fragment",
            GatewayRetry::Safe,
        ));
    }
    Ok(GatewayResponse::Rows(GatewayRows::new(
        vec![alias.into()],
        vec![vec![GatewayValue::Integer(total)]],
    )?))
}

fn encode_read_scope(scope: &dtg_language_ir::ReadScope, output: &mut Vec<u8>) {
    encode_temporal_scope(&scope.transaction_time, output);
    match &scope.valid_time {
        None => output.push(0),
        Some(dtg_language_ir::ValidTimePredicate::At(value)) => {
            output.push(1);
            encode_valid_time_expr(value, output);
        }
        Some(dtg_language_ir::ValidTimePredicate::Overlaps(interval)) => {
            output.push(2);
            match interval {
                dtg_language_ir::ValidIntervalExpr::Literal(value) => {
                    output.push(0);
                    output.extend_from_slice(&value.start().to_be_bytes());
                    output.extend_from_slice(&value.end().to_be_bytes());
                }
                dtg_language_ir::ValidIntervalExpr::Parameter(name) => {
                    output.push(1);
                    encode_string(name, output);
                }
                dtg_language_ir::ValidIntervalExpr::Bounds { start, end } => {
                    output.push(2);
                    encode_valid_time_expr(start, output);
                    encode_valid_time_expr(end, output);
                }
            }
        }
        Some(dtg_language_ir::ValidTimePredicate::Changes { from, to }) => {
            output.push(3);
            encode_valid_time_expr(from, output);
            encode_valid_time_expr(to, output);
        }
    }
}

fn encode_temporal_scope(scope: &TemporalScope, output: &mut Vec<u8>) {
    match scope {
        TemporalScope::Current => output.push(0),
        TemporalScope::AsOf(time) => {
            output.push(1);
            encode_time_expr(time, output);
        }
        TemporalScope::Changes { from, to } => {
            output.push(2);
            encode_time_expr(from, output);
            encode_time_expr(to, output);
        }
    }
}

fn encode_time_expr(time: &TimeExpr, output: &mut Vec<u8>) {
    match time {
        TimeExpr::Literal(value) => {
            output.push(0);
            output.extend_from_slice(&value.get().to_be_bytes());
        }
        TimeExpr::Parameter(name) => {
            output.push(1);
            encode_string(name, output);
        }
    }
}

fn encode_valid_time_expr(time: &ValidTimeExpr, output: &mut Vec<u8>) {
    match time {
        ValidTimeExpr::Literal(value) => {
            output.push(0);
            output.extend_from_slice(&value.to_be_bytes());
        }
        ValidTimeExpr::Parameter(name) => {
            output.push(1);
            encode_string(name, output);
        }
    }
}

fn encode_physical_operator(operator: &dtg_plan::PhysicalOperator, output: &mut Vec<u8>) {
    use dtg_plan::PhysicalOperatorKind;

    output.extend_from_slice(&operator.id().get().to_be_bytes());
    match operator.kind() {
        PhysicalOperatorKind::Source {
            logical_node,
            fragments,
            output: source_output,
        } => {
            output.push(0);
            output.extend_from_slice(&logical_node.get().to_be_bytes());
            encode_u32(fragments.len(), output);
            for fragment in fragments {
                output.extend_from_slice(&fragment.get().to_be_bytes());
            }
            encode_string(source_output, output);
        }
        PhysicalOperatorKind::Filter { input, predicate } => {
            output.push(1);
            output.extend_from_slice(&input.get().to_be_bytes());
            encode_physical_expr(predicate, output);
        }
        PhysicalOperatorKind::Project { input, projections } => {
            output.push(2);
            output.extend_from_slice(&input.get().to_be_bytes());
            encode_projections(projections, output);
        }
        PhysicalOperatorKind::Join {
            left,
            right,
            kind,
            predicate,
        } => {
            output.push(3);
            output.extend_from_slice(&left.get().to_be_bytes());
            output.extend_from_slice(&right.get().to_be_bytes());
            output.push(match kind {
                dtg_language_ir::JoinKind::Inner => 0,
                dtg_language_ir::JoinKind::Left => 1,
                dtg_language_ir::JoinKind::Semi => 2,
                dtg_language_ir::JoinKind::Anti => 3,
            });
            match predicate {
                Some(predicate) => {
                    output.push(1);
                    encode_physical_expr(predicate, output);
                }
                None => output.push(0),
            }
        }
        PhysicalOperatorKind::Aggregate {
            input,
            groups,
            aggregates,
        } => {
            output.push(4);
            output.extend_from_slice(&input.get().to_be_bytes());
            encode_projections(groups, output);
            encode_u32(aggregates.len(), output);
            for aggregate in aggregates {
                output.push(match aggregate.function {
                    dtg_language_ir::AggregateKind::Count => 0,
                    dtg_language_ir::AggregateKind::Sum => 1,
                    dtg_language_ir::AggregateKind::Average => 2,
                    dtg_language_ir::AggregateKind::Minimum => 3,
                    dtg_language_ir::AggregateKind::Maximum => 4,
                    dtg_language_ir::AggregateKind::Collect => 5,
                });
                match &aggregate.argument {
                    Some(argument) => {
                        output.push(1);
                        encode_logical_expr(argument, output);
                    }
                    None => output.push(0),
                }
                encode_string(&aggregate.alias, output);
                output.push(u8::from(aggregate.distinct));
            }
        }
        PhysicalOperatorKind::Sort { input, keys } => {
            output.push(5);
            output.extend_from_slice(&input.get().to_be_bytes());
            encode_u32(keys.len(), output);
            for key in keys {
                encode_logical_expr(&key.expression, output);
                output.push(match key.direction {
                    dtg_language_ir::SortDirection::Ascending => 0,
                    dtg_language_ir::SortDirection::Descending => 1,
                });
            }
        }
        PhysicalOperatorKind::Limit { input, skip, limit } => {
            output.push(6);
            output.extend_from_slice(&input.get().to_be_bytes());
            output.extend_from_slice(&skip.to_be_bytes());
            match limit {
                Some(limit) => {
                    output.push(1);
                    output.extend_from_slice(&limit.to_be_bytes());
                }
                None => output.push(0),
            }
        }
        PhysicalOperatorKind::Unwind {
            input,
            expression,
            alias,
        } => {
            output.push(7);
            output.extend_from_slice(&input.get().to_be_bytes());
            encode_physical_expr(expression, output);
            encode_string(alias, output);
        }
    }
}

fn encode_projections(projections: &[dtg_language_ir::Projection], output: &mut Vec<u8>) {
    encode_u32(projections.len(), output);
    for projection in projections {
        encode_logical_expr(&projection.expression, output);
        encode_string(&projection.alias, output);
    }
}

fn encode_physical_expr(expression: &PhysicalExpr, output: &mut Vec<u8>) {
    match expression {
        PhysicalExpr::Evaluate(expression) => {
            output.push(0);
            encode_logical_expr(expression, output);
        }
        PhysicalExpr::VerifyStorageSemantics { node, requirements } => {
            output.push(1);
            output.extend_from_slice(&node.get().to_be_bytes());
            output.push(
                u8::from(requirements.temporal())
                    | (u8::from(requirements.nulls()) << 1)
                    | (u8::from(requirements.duplicates()) << 2)
                    | (u8::from(requirements.order()) << 3)
                    | (u8::from(requirements.snapshot()) << 4),
            );
        }
    }
}

fn encode_logical_expr(expression: &dtg_language_ir::LogicalExpr, output: &mut Vec<u8>) {
    use dtg_language_ir::LogicalExpr;

    match expression {
        LogicalExpr::Literal(value) => {
            output.push(0);
            encode_kernel_value(value, output);
        }
        LogicalExpr::Parameter(name) => {
            output.push(1);
            encode_string(name, output);
        }
        LogicalExpr::Column(name) => {
            output.push(2);
            encode_string(name, output);
        }
        LogicalExpr::Property { input, name } => {
            output.push(3);
            encode_logical_expr(input, output);
            encode_string(name, output);
        }
        LogicalExpr::Unary { operator, input } => {
            output.push(4);
            output.push(match operator {
                dtg_language_ir::UnaryOperator::Not => 0,
                dtg_language_ir::UnaryOperator::Negate => 1,
                dtg_language_ir::UnaryOperator::IsNull => 2,
            });
            encode_logical_expr(input, output);
        }
        LogicalExpr::Binary {
            left,
            operator,
            right,
        } => {
            output.push(5);
            output.push(match operator {
                dtg_language_ir::BinaryOperator::Add => 0,
                dtg_language_ir::BinaryOperator::Subtract => 1,
                dtg_language_ir::BinaryOperator::Multiply => 2,
                dtg_language_ir::BinaryOperator::Divide => 3,
                dtg_language_ir::BinaryOperator::Equal => 4,
                dtg_language_ir::BinaryOperator::NotEqual => 5,
                dtg_language_ir::BinaryOperator::LessThan => 6,
                dtg_language_ir::BinaryOperator::LessThanOrEqual => 7,
                dtg_language_ir::BinaryOperator::GreaterThan => 8,
                dtg_language_ir::BinaryOperator::GreaterThanOrEqual => 9,
                dtg_language_ir::BinaryOperator::And => 10,
                dtg_language_ir::BinaryOperator::Or => 11,
                dtg_language_ir::BinaryOperator::Contains => 12,
            });
            encode_logical_expr(left, output);
            encode_logical_expr(right, output);
        }
        LogicalExpr::List(values) => {
            output.push(6);
            encode_u32(values.len(), output);
            for value in values {
                encode_logical_expr(value, output);
            }
        }
        LogicalExpr::Map(values) => {
            output.push(7);
            encode_u32(values.len(), output);
            for (name, value) in values {
                encode_string(name, output);
                encode_logical_expr(value, output);
            }
        }
    }
}

fn encode_kernel_value(value: &dtg_storage::Value, output: &mut Vec<u8>) {
    match value {
        dtg_storage::Value::Null => output.push(0),
        dtg_storage::Value::Boolean(value) => {
            output.push(1);
            output.push(u8::from(*value));
        }
        dtg_storage::Value::Integer(value) => {
            output.push(2);
            output.extend_from_slice(&value.to_be_bytes());
        }
        dtg_storage::Value::FloatBits(value) => {
            output.push(3);
            output.extend_from_slice(&value.to_be_bytes());
        }
        dtg_storage::Value::Bytes(value) => {
            output.push(4);
            encode_u32(value.len(), output);
            output.extend_from_slice(value);
        }
        dtg_storage::Value::String(value) => {
            output.push(5);
            encode_string(value, output);
        }
        dtg_storage::Value::List(values) => {
            output.push(6);
            encode_u32(values.len(), output);
            for value in values {
                encode_kernel_value(value, output);
            }
        }
        dtg_storage::Value::Map(values) => {
            output.push(7);
            encode_u32(values.len(), output);
            for (name, value) in values {
                encode_string(name, output);
                encode_kernel_value(value, output);
            }
        }
    }
}

fn encode_row_schema(schema: &dtg_language_ir::RowSchema, output: &mut Vec<u8>) {
    encode_u32(schema.fields.len(), output);
    for field in &schema.fields {
        encode_string(&field.name, output);
        encode_logical_type(&field.data_type, output);
        output.push(u8::from(field.nullable));
    }
}

fn encode_logical_type(data_type: &dtg_language_ir::LogicalType, output: &mut Vec<u8>) {
    use dtg_language_ir::LogicalType;

    match data_type {
        LogicalType::Null => output.push(0),
        LogicalType::Boolean => output.push(1),
        LogicalType::Integer => output.push(2),
        LogicalType::Float => output.push(3),
        LogicalType::Bytes => output.push(4),
        LogicalType::String => output.push(5),
        LogicalType::List(element) => {
            output.push(6);
            encode_logical_type(element, output);
        }
        LogicalType::Map => output.push(7),
        LogicalType::Vertex => output.push(8),
        LogicalType::Relationship => output.push(9),
        LogicalType::Any => output.push(10),
    }
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

fn decode_protocol_v2_query_responses(
    responses: Vec<proto::GatewayResponse>,
) -> Result<BTreeMap<u32, Vec<QueryColumnBatch>>, GatewayExecutionError> {
    let mut decoder = ProtocolV2QueryResponseDecoder::default();
    for response in responses {
        decoder.push(response)?;
    }
    decoder.finish()
}

#[derive(Default)]
struct ProtocolV2QueryResponseDecoder {
    response_count: usize,
    batches: BTreeMap<u32, Vec<(u64, QueryColumnBatch)>>,
}

impl ProtocolV2QueryResponseDecoder {
    fn push(&mut self, response: proto::GatewayResponse) -> Result<(), GatewayExecutionError> {
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
        if validated.details().is_some() {
            return Err(wire_codec_error(
                "protocol v2 query response contained control details",
            ));
        }
        let batch = response
            .batch
            .ok_or_else(|| wire_codec_error("protocol v2 query response omitted a batch"))?;
        self.push_batch(batch)
    }

    fn push_batch(&mut self, batch: proto::ColumnBatch) -> Result<(), GatewayExecutionError> {
        if self.response_count >= 65_536 {
            return Err(GatewayExecutionError::new(
                "DTG-CLUSTER-STREAM-LIMIT",
                "protocol v2 response stream exceeds 65536 messages",
                GatewayRetry::Never,
            ));
        }
        self.response_count = self.response_count.saturating_add(1);
        let fragment_bytes: [u8; 16] = batch
            .fragment_id
            .as_slice()
            .try_into()
            .map_err(|_| wire_codec_error("protocol v2 fragment ID must contain 16 bytes"))?;
        let fragment_id = u32::try_from(u128::from_be_bytes(fragment_bytes))
            .map_err(|_| wire_codec_error("protocol v2 fragment ID exceeds u32"))?;
        let sequence = batch.sequence;
        let row_count = batch.row_count;
        let payload = validate_column_batch(batch).map_err(|error| {
            GatewayExecutionError::new(error.code(), error.to_string(), GatewayRetry::Never)
        })?;
        let query_batch = decode_query_column_batch(payload.body(), row_count)?;
        let fragment = self.batches.entry(fragment_id).or_default();
        if fragment.iter().any(|(existing, _)| *existing == sequence) {
            return Err(wire_codec_error(
                "protocol v2 fragment repeated a batch sequence",
            ));
        }
        fragment.push((sequence, query_batch));
        Ok(())
    }

    fn finish(self) -> Result<BTreeMap<u32, Vec<QueryColumnBatch>>, GatewayExecutionError> {
        if self.response_count == 0 {
            return Err(GatewayExecutionError::new(
                "DTG-PROTOCOL-EMPTY-RESPONSE",
                "protocol v2 client returned no response",
                GatewayRetry::Safe,
            ));
        }
        self.batches
            .into_iter()
            .map(|(fragment_id, mut fragment)| {
                fragment.sort_by_key(|(sequence, _)| *sequence);
                for (index, (sequence, _)) in fragment.iter().enumerate() {
                    let expected = u64::try_from(index).unwrap_or(u64::MAX).saturating_add(1);
                    if *sequence != expected {
                        return Err(wire_codec_error(
                            "protocol v2 fragment batch sequence is not contiguous",
                        ));
                    }
                }
                Ok((
                    fragment_id,
                    fragment.into_iter().map(|(_, batch)| batch).collect(),
                ))
            })
            .collect()
    }
}

fn decode_query_column_batch(
    body: &[u8],
    wire_row_count: u32,
) -> Result<QueryColumnBatch, GatewayExecutionError> {
    let mut cursor = WireCursor::new(body);
    let field_count = cursor.read_len()?;
    let fields = (0..field_count)
        .map(|_| cursor.read_string())
        .collect::<Result<Vec<_>, _>>()?;
    let row_count = cursor.read_len()?;
    if row_count != wire_row_count as usize {
        return Err(wire_codec_error(
            "protocol v2 row count differs from the typed batch header",
        ));
    }
    let mut columns = (0..field_count)
        .map(|_| Vec::with_capacity(row_count))
        .collect::<Vec<Vec<QueryValue>>>();
    for _ in 0..row_count {
        for column in &mut columns {
            column.push(gateway_value_to_query(cursor.read_value(0)?));
        }
    }
    if !cursor.is_empty() {
        return Err(wire_codec_error(
            "protocol v2 typed row payload has trailing bytes",
        ));
    }
    let schema = RowSchema {
        fields: fields
            .into_iter()
            .map(|name| Field {
                name,
                data_type: LogicalType::Any,
                nullable: true,
            })
            .collect(),
    };
    QueryColumnBatch::try_new(schema, columns).map_err(gateway_query_error)
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

fn process_query_budget(deadline_unix_ms: u64) -> Result<QueryBudget, GatewayExecutionError> {
    let now_unix_ms = u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| {
                GatewayExecutionError::new(
                    "DTG-EXECUTION-CLOCK",
                    "system clock precedes Unix epoch",
                    GatewayRetry::Safe,
                )
            })?
            .as_millis(),
    )
    .map_err(|_| {
        GatewayExecutionError::new(
            "DTG-EXECUTION-CLOCK",
            "system clock exceeds u64 milliseconds",
            GatewayRetry::Safe,
        )
    })?;
    let remaining = deadline_unix_ms.checked_sub(now_unix_ms).ok_or_else(|| {
        GatewayExecutionError::new(
            "DTG-EXECUTION-DEADLINE",
            "Gateway request deadline elapsed before query execution",
            GatewayRetry::Safe,
        )
    })?;
    Ok(QueryBudget {
        max_rows: 1_000_000,
        max_scan_bytes: 256 * 1024 * 1024,
        max_memory_bytes: 256 * 1024 * 1024,
        max_network_bytes: 256 * 1024 * 1024,
        max_spill_bytes: 1024 * 1024 * 1024,
        deadline: Instant::now() + Duration::from_millis(remaining),
    })
}

fn gateway_query_error(error: QueryError) -> GatewayExecutionError {
    GatewayExecutionError::new(
        "DTG-EXECUTION-QUERY",
        error.to_string(),
        GatewayRetry::Never,
    )
}

fn gateway_value_to_query(value: GatewayValue) -> QueryValue {
    match value {
        GatewayValue::Null => QueryValue::Null,
        GatewayValue::Boolean(value) => QueryValue::Boolean(value),
        GatewayValue::Integer(value) => QueryValue::Integer(value),
        GatewayValue::FloatBits(value) => QueryValue::FloatBits(value),
        GatewayValue::Bytes(value) => QueryValue::Bytes(value),
        GatewayValue::String(value) => QueryValue::String(value),
        GatewayValue::List(values) => {
            QueryValue::List(values.into_iter().map(gateway_value_to_query).collect())
        }
        GatewayValue::Map(values) => QueryValue::Map(
            values
                .into_iter()
                .map(|(name, value)| (name, gateway_value_to_query(value)))
                .collect(),
        ),
    }
}

fn query_value_to_gateway(value: QueryValue) -> Result<GatewayValue, GatewayExecutionError> {
    match value {
        QueryValue::Null => Ok(GatewayValue::Null),
        QueryValue::Boolean(value) => Ok(GatewayValue::Boolean(value)),
        QueryValue::Integer(value) => Ok(GatewayValue::Integer(value)),
        QueryValue::FloatBits(value) => Ok(GatewayValue::FloatBits(value)),
        QueryValue::Bytes(value) => Ok(GatewayValue::Bytes(value)),
        QueryValue::String(value) => Ok(GatewayValue::String(value)),
        QueryValue::List(values) => values
            .into_iter()
            .map(query_value_to_gateway)
            .collect::<Result<Vec<_>, _>>()
            .map(GatewayValue::List),
        QueryValue::Map(values) => values
            .into_iter()
            .map(|(name, value)| Ok((name, query_value_to_gateway(value)?)))
            .collect::<Result<BTreeMap<_, _>, _>>()
            .map(GatewayValue::Map),
        QueryValue::Vertex(_) | QueryValue::Relationship(_) => Err(GatewayExecutionError::new(
            "DTG-EXECUTION-QUERY-VALUE",
            "query runtime produced an unsupported graph value",
            GatewayRetry::Never,
        )),
    }
}

fn materialized_result_schema(schema: &RowSchema) -> RowSchema {
    RowSchema {
        fields: schema
            .fields
            .iter()
            .map(|field| match field.data_type {
                LogicalType::Vertex | LogicalType::Relationship => Field {
                    name: field.name.clone(),
                    data_type: LogicalType::Any,
                    nullable: true,
                },
                _ => field.clone(),
            })
            .collect(),
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
    let access_nodes = fragment
        .storage_accesses()
        .iter()
        .map(|access| access.node().get())
        .collect();
    ExecutableFragment::with_access_nodes(fragment.id().get(), fence, accesses, access_nodes)
}

fn lower_operator(operator: &dtg_plan::PhysicalOperator) -> Result<ExecutableOperator, QueryError> {
    lower_operator_with(
        operator,
        &|expression| Ok(Expression::new(expression.clone())),
        &lower_expression,
        &|error| error,
    )
}

fn lower_operator_with_parameters(
    operator: &dtg_plan::PhysicalOperator,
    parameters: &BTreeMap<String, GatewayValue>,
) -> Result<ExecutableOperator, GatewayExecutionError> {
    lower_operator_with(
        operator,
        &|expression| bind_logical_expr(expression, parameters).map(Expression::new),
        &|expression| lower_expression_with_parameters(expression, parameters),
        &gateway_lowering_error,
    )
}

fn lower_operator_with<E>(
    operator: &dtg_plan::PhysicalOperator,
    lower_logical: &impl Fn(&LogicalExpr) -> Result<Expression, E>,
    lower_physical: &impl Fn(&PhysicalExpr) -> Result<Expression, E>,
    map_query_error: &impl Fn(QueryError) -> E,
) -> Result<ExecutableOperator, E> {
    use dtg_plan::PhysicalOperatorKind;

    let kind = match operator.kind() {
        PhysicalOperatorKind::Source {
            logical_node,
            fragments,
            output,
        } => ExecutableOperatorKind::Source {
            logical_node: logical_node.get(),
            fragments: fragments.iter().map(|fragment| fragment.get()).collect(),
            output: output.clone(),
        },
        PhysicalOperatorKind::Filter { input, predicate } => ExecutableOperatorKind::Filter {
            input: input.get(),
            predicate: lower_physical(predicate)?,
        },
        PhysicalOperatorKind::Project { input, projections } => ExecutableOperatorKind::Project {
            input: input.get(),
            projections: projections
                .iter()
                .map(|projection| {
                    Ok(ExecutableProjection::new(
                        projection.alias.clone(),
                        lower_logical(&projection.expression)?,
                    ))
                })
                .collect::<Result<Vec<_>, E>>()?,
        },
        PhysicalOperatorKind::Join {
            left,
            right,
            kind,
            predicate,
        } => ExecutableOperatorKind::Join {
            left: left.get(),
            right: right.get(),
            kind: *kind,
            predicate: predicate.as_ref().map(lower_physical).transpose()?,
        },
        PhysicalOperatorKind::Aggregate {
            input,
            groups,
            aggregates,
        } => ExecutableOperatorKind::Aggregate {
            input: input.get(),
            groups: groups
                .iter()
                .map(|projection| {
                    Ok(ExecutableProjection::new(
                        projection.alias.clone(),
                        lower_logical(&projection.expression)?,
                    ))
                })
                .collect::<Result<Vec<_>, E>>()?,
            aggregates: aggregates
                .iter()
                .map(|aggregate| {
                    Ok(ExecutableAggregate {
                        function: aggregate.function,
                        argument: aggregate.argument.as_ref().map(lower_logical).transpose()?,
                        alias: aggregate.alias.clone(),
                        distinct: aggregate.distinct,
                    })
                })
                .collect::<Result<Vec<_>, E>>()?,
        },
        PhysicalOperatorKind::Sort { input, keys } => ExecutableOperatorKind::Sort {
            input: input.get(),
            keys: keys
                .iter()
                .map(|key| {
                    Ok(ExecutableSortKey {
                        expression: lower_logical(&key.expression)?,
                        direction: key.direction,
                    })
                })
                .collect::<Result<Vec<_>, E>>()?,
        },
        PhysicalOperatorKind::Limit { input, skip, limit } => ExecutableOperatorKind::Limit {
            input: input.get(),
            skip: *skip,
            limit: *limit,
        },
        PhysicalOperatorKind::Unwind {
            input,
            expression,
            alias,
        } => ExecutableOperatorKind::Unwind {
            input: input.get(),
            expression: lower_physical(expression)?,
            alias: alias.clone(),
        },
    };
    ExecutableOperator::new(operator.id().get(), kind).map_err(map_query_error)
}

fn lower_expression(expression: &PhysicalExpr) -> Result<Expression, QueryError> {
    match expression {
        PhysicalExpr::Evaluate(expression) => Ok(Expression::new(expression.clone())),
        PhysicalExpr::VerifyStorageSemantics { .. } => Err(QueryError::InvalidPlan(
            "storage verification expression cannot be used as a physical operator expression"
                .into(),
        )),
    }
}

fn lower_expression_with_parameters(
    expression: &PhysicalExpr,
    parameters: &BTreeMap<String, GatewayValue>,
) -> Result<Expression, GatewayExecutionError> {
    match expression {
        PhysicalExpr::Evaluate(expression) => {
            bind_logical_expr(expression, parameters).map(Expression::new)
        }
        PhysicalExpr::VerifyStorageSemantics { .. } => {
            Err(gateway_lowering_error(QueryError::InvalidPlan(
                "storage verification expression cannot be used as a physical operator expression"
                    .into(),
            )))
        }
    }
}

pub fn bind_logical_expr(
    expression: &LogicalExpr,
    parameters: &BTreeMap<String, GatewayValue>,
) -> Result<LogicalExpr, GatewayExecutionError> {
    match expression {
        LogicalExpr::Literal(value) => Ok(LogicalExpr::Literal(value.clone())),
        LogicalExpr::Parameter(name) => parameters
            .get(name)
            .ok_or_else(|| {
                GatewayExecutionError::new(
                    "DTG-EXECUTION-MISSING-PARAMETER",
                    format!("missing required parameter: {name}"),
                    GatewayRetry::Never,
                )
            })
            .and_then(gateway_value_to_storage)
            .map(LogicalExpr::Literal),
        LogicalExpr::Column(name) => Ok(LogicalExpr::Column(name.clone())),
        LogicalExpr::Property { input, name } => Ok(LogicalExpr::Property {
            input: Box::new(bind_logical_expr(input, parameters)?),
            name: name.clone(),
        }),
        LogicalExpr::Unary { operator, input } => Ok(LogicalExpr::Unary {
            operator: *operator,
            input: Box::new(bind_logical_expr(input, parameters)?),
        }),
        LogicalExpr::Binary {
            left,
            operator,
            right,
        } => Ok(LogicalExpr::Binary {
            left: Box::new(bind_logical_expr(left, parameters)?),
            operator: *operator,
            right: Box::new(bind_logical_expr(right, parameters)?),
        }),
        LogicalExpr::List(values) => values
            .iter()
            .map(|value| bind_logical_expr(value, parameters))
            .collect::<Result<Vec<_>, _>>()
            .map(LogicalExpr::List),
        LogicalExpr::Map(values) => values
            .iter()
            .map(|(name, value)| Ok((name.clone(), bind_logical_expr(value, parameters)?)))
            .collect::<Result<Vec<_>, _>>()
            .map(LogicalExpr::Map),
    }
}

fn bind_process_query_parameters(
    program: &LogicalProgram,
    parameters: &BTreeMap<String, GatewayValue>,
) -> Result<LogicalProgram, GatewayExecutionError> {
    let mut program = program.clone();
    let LogicalStatement::Query(plan) = &mut program.statement else {
        return Ok(program);
    };
    bind_process_query_plan_parameters(plan, parameters)?;
    Ok(program)
}

fn bind_process_query_plan_parameters(
    plan: &mut LogicalPlan,
    parameters: &BTreeMap<String, GatewayValue>,
) -> Result<(), GatewayExecutionError> {
    for node in &mut plan.nodes {
        match &mut node.kind {
            LogicalNodeKind::NodeScan(scan) => {
                bind_read_scope_parameters(&mut scan.read_scope, parameters)?;
            }
            LogicalNodeKind::RelationshipScan(scan) => {
                bind_read_scope_parameters(&mut scan.read_scope, parameters)?;
            }
            LogicalNodeKind::VertexLookup(lookup) => {
                lookup.id = bind_logical_expr(&lookup.id, parameters)?;
                bind_read_scope_parameters(&mut lookup.read_scope, parameters)?;
            }
            LogicalNodeKind::RelationshipLookup(lookup) => {
                lookup.id = bind_logical_expr(&lookup.id, parameters)?;
                bind_read_scope_parameters(&mut lookup.read_scope, parameters)?;
            }
            LogicalNodeKind::Expand(expand) => {
                bind_read_scope_parameters(&mut expand.read_scope, parameters)?;
            }
            LogicalNodeKind::Subquery(subquery) => {
                bind_process_query_plan_parameters(&mut subquery.plan, parameters)?;
            }
            LogicalNodeKind::Filter { .. }
            | LogicalNodeKind::Project { .. }
            | LogicalNodeKind::Join(_)
            | LogicalNodeKind::Aggregate(_)
            | LogicalNodeKind::Sort(_)
            | LogicalNodeKind::Limit(_)
            | LogicalNodeKind::Unwind(_) => {}
        }
    }
    Ok(())
}

fn bind_read_scope_parameters(
    scope: &mut dtg_language_ir::ReadScope,
    parameters: &BTreeMap<String, GatewayValue>,
) -> Result<(), GatewayExecutionError> {
    let TemporalScope::AsOf(time) = &mut scope.transaction_time else {
        return Ok(());
    };
    let TimeExpr::Parameter(name) = time else {
        return Ok(());
    };
    let value = parameters.get(name).ok_or_else(|| {
        GatewayExecutionError::new(
            "DTG-EXECUTION-MISSING-PARAMETER",
            format!("missing required parameter: {name}"),
            GatewayRetry::Never,
        )
    })?;
    let GatewayValue::Integer(value) = value else {
        return Err(GatewayExecutionError::new(
            "DTG-EXECUTION-TEMPORAL-PARAMETER",
            format!("temporal parameter {name} must be an integer"),
            GatewayRetry::Never,
        ));
    };
    *time = TimeExpr::Literal(dtg_storage::TransactionTime::new(*value).map_err(|error| {
        GatewayExecutionError::new(
            "DTG-EXECUTION-TEMPORAL-PARAMETER",
            error.to_string(),
            GatewayRetry::Never,
        )
    })?);
    Ok(())
}

async fn execute_process_create(
    transport: &dyn GatewayWriteTransport,
    context: GatewayRequestContext,
    write: &dtg_language_ir::LogicalWrite,
    parameters: &BTreeMap<String, GatewayValue>,
    planning_context: &PlanningContext,
    write_accounting: &ProcessWriteAccounting,
    request_metrics: &Arc<RequestStageMetrics>,
) -> Result<ProcessWriteOutcome, GatewayExecutionError> {
    let [catalog_shard] = planning_context.catalog().shards() else {
        return Err(GatewayExecutionError::new(
            "DTG-EXECUTION-WRITE-ROUTING",
            "process CREATE requires exactly one catalog Shard",
            GatewayRetry::Never,
        ));
    };
    if catalog_shard.binding().role() != dtg_storage::BindingRole::Active {
        return Err(GatewayExecutionError::new(
            "DTG-EXECUTION-WRITE-ROUTING",
            "process CREATE requires an active catalog Shard",
            GatewayRetry::Never,
        ));
    }
    if write.input.is_some() || write.mutations.len() != 1 {
        return Err(GatewayExecutionError::new(
            "DTG-EXECUTION-WRITE-SHAPE",
            "process writes support only one input-free vertex CREATE",
            GatewayRetry::Never,
        ));
    }
    let dtg_language_ir::LogicalMutation::CreateVertex {
        labels,
        properties,
        valid_from,
        ..
    } = &write.mutations[0]
    else {
        return Err(GatewayExecutionError::new(
            "DTG-EXECUTION-WRITE-SHAPE",
            "process writes support only one input-free vertex CREATE",
            GatewayRetry::Never,
        ));
    };
    let route = GatewayWriteRoute::new(
        context.clone(),
        catalog_shard.binding().clone(),
        planning_context.catalog().version(),
        catalog_shard.applied_index(),
    );
    let transaction_id = TransactionId::new(process_write_identity(
        b"dtg-gateway-create-transaction-v1",
        context.request_id(),
        0,
    ))
    .map_err(|error| process_write_error(error.to_string()))?;
    let (start_time, commit_time) = request_metrics
        .start_detail(RequestDetail::GatewayMetaPrepareWrite)
        .finish_result(transport.prepare_write_times(&route, transaction_id).await)?;
    let command_id = CommandId::new(process_write_identity(
        b"dtg-gateway-create-command-v1",
        context.request_id(),
        0,
    ))
    .map_err(|error| process_write_error(error.to_string()))?;
    let vertex_id = VertexId::new(process_write_identity(
        b"dtg-gateway-create-vertex-v1",
        context.request_id(),
        0,
    ))
    .map_err(|error| process_write_error(error.to_string()))?;
    let vertex = lower_process_create_vertex(
        vertex_id,
        labels,
        properties,
        valid_from,
        parameters,
        commit_time,
    )?;
    let request_digest = dtg_storage::Digest32::new(process_write_digest(
        b"dtg-gateway-create-command-digest-v1",
        context.request_id(),
        0,
    ));
    let command = ShardCommand::CommitSingleShardTransaction(
        CommitSingleShardTransaction::new(
            command_id,
            route.binding().placement_epoch().get(),
            route.binding().backend_generation().get(),
            transaction_id,
            start_time,
            route.snapshot_applied_index,
            request_digest,
            vec![StorageMutation::PutVertex(vertex)],
        )
        .map_err(|error| process_write_error(error.to_string()))?,
    );
    let request = GatewayWriteRequest::new(
        route.clone(),
        transaction_id,
        start_time,
        commit_time,
        command,
    );
    reserve_process_write_capacity(write_accounting, transaction_id)?;
    let receipt = match request_metrics
        .start_detail(RequestDetail::GatewayDataApplyRpc)
        .finish_result(transport.apply_single_shard(request).await)
    {
        Ok(receipt) => receipt,
        Err(error) => {
            complete_process_write_attempt(
                write_accounting,
                transaction_id,
                error.retry() == GatewayRetry::Safe,
            )?;
            if error.retry() == GatewayRetry::Never {
                transport.abort(&route, transaction_id).await?;
            }
            return Err(error);
        }
    };
    complete_process_write_attempt(write_accounting, transaction_id, !receipt.replayed())?;
    request_metrics
        .start_detail(RequestDetail::GatewayMetaResolveCommit)
        .finish_result(transport.resolve_committed(&route, transaction_id).await)?;
    Ok(ProcessWriteOutcome {
        transaction_id,
        binding: route.binding,
        applied_index: receipt.applied_index(),
        commit_time,
    })
}

struct ProcessWriteOutcome {
    transaction_id: TransactionId,
    binding: dtg_storage::ReplicaBinding,
    applied_index: u64,
    commit_time: dtg_storage::TransactionTime,
}

// This is bounded process-local diagnostic retention, not restart recovery. Completed receipts
// are evicted fail-closed for bound growth; pending receipts are admitted before Data I/O and
// never evicted while ambiguous.
const PROCESS_WRITE_ACCOUNTING_LIMIT: usize = 4_096;

#[derive(Clone, Copy)]
enum ProcessWriteAccountingState {
    Reserved { in_flight: usize },
    Pending,
    Accounting,
    Accounted,
}

#[derive(Default)]
struct ProcessWriteAccounting {
    state: Mutex<ProcessWriteAccountingStateMachine>,
}

#[derive(Default)]
struct ProcessWriteAccountingStateMachine {
    states: BTreeMap<TransactionId, ProcessWriteAccountingState>,
    accounted_order: VecDeque<TransactionId>,
    pending_count: usize,
    capacity_reservations: usize,
}

impl ProcessWriteAccountingStateMachine {
    fn reserve_capacity(
        &mut self,
        transaction_id: TransactionId,
    ) -> Result<bool, GatewayExecutionError> {
        if let Some(state) = self.states.get_mut(&transaction_id) {
            if let ProcessWriteAccountingState::Reserved { in_flight } = state {
                *in_flight = in_flight.saturating_add(1);
            }
            return Ok(false);
        }
        if self.pending_count + self.capacity_reservations >= PROCESS_WRITE_ACCOUNTING_LIMIT {
            return Err(process_write_error(
                "process write-accounting capacity is exhausted",
            ));
        }
        self.capacity_reservations += 1;
        self.states.insert(
            transaction_id,
            ProcessWriteAccountingState::Reserved { in_flight: 1 },
        );
        Ok(true)
    }

    fn complete_attempt(&mut self, transaction_id: TransactionId, retain_pending: bool) {
        let Some(ProcessWriteAccountingState::Reserved { in_flight }) =
            self.states.get(&transaction_id).copied()
        else {
            return;
        };
        if retain_pending {
            self.states
                .insert(transaction_id, ProcessWriteAccountingState::Pending);
            self.capacity_reservations = self
                .capacity_reservations
                .checked_sub(1)
                .expect("capacity reservation exists");
            self.pending_count += 1;
        } else if in_flight == 1 {
            self.states.remove(&transaction_id);
            self.capacity_reservations = self
                .capacity_reservations
                .checked_sub(1)
                .expect("capacity reservation exists");
        } else {
            self.states.insert(
                transaction_id,
                ProcessWriteAccountingState::Reserved {
                    in_flight: in_flight - 1,
                },
            );
        }
    }

    fn mark_accounted(&mut self, transaction_id: TransactionId) {
        self.states
            .insert(transaction_id, ProcessWriteAccountingState::Accounted);
        self.pending_count = self
            .pending_count
            .checked_sub(1)
            .expect("accounted write was pending");
        self.accounted_order.push_back(transaction_id);
        while self.accounted_order.len() > PROCESS_WRITE_ACCOUNTING_LIMIT {
            let expired = self.accounted_order.pop_front().expect("length checked");
            self.states.remove(&expired);
        }
    }
}

fn reserve_process_write_capacity(
    write_accounting: &ProcessWriteAccounting,
    transaction_id: TransactionId,
) -> Result<bool, GatewayExecutionError> {
    write_accounting
        .state
        .lock()
        .map_err(|_| process_write_error("process write-accounting lock is poisoned"))?
        .reserve_capacity(transaction_id)
}

fn complete_process_write_attempt(
    write_accounting: &ProcessWriteAccounting,
    transaction_id: TransactionId,
    retain_pending: bool,
) -> Result<(), GatewayExecutionError> {
    write_accounting
        .state
        .lock()
        .map_err(|_| process_write_error("process write-accounting lock is poisoned"))?
        .complete_attempt(transaction_id, retain_pending);
    Ok(())
}

// Lock ordering is write accounting, then planning context. Neither lock spans Meta/Data I/O;
// install_planning_context acquires only the planning-context lock, so it has no inverse ordering.
fn account_process_write(
    write_accounting: &ProcessWriteAccounting,
    planning_context: &RwLock<PlanningContext>,
    outcome: &ProcessWriteOutcome,
) -> Result<(), GatewayExecutionError> {
    let mut accounting = write_accounting
        .state
        .lock()
        .map_err(|_| process_write_error("process write-accounting lock is poisoned"))?;
    let increment_bound = match accounting.states.get(&outcome.transaction_id).copied() {
        Some(ProcessWriteAccountingState::Pending) => {
            accounting.states.insert(
                outcome.transaction_id,
                ProcessWriteAccountingState::Accounting,
            );
            true
        }
        Some(ProcessWriteAccountingState::Accounted) | None => false,
        Some(ProcessWriteAccountingState::Reserved { .. }) => {
            return Err(process_write_error(
                "process write accounting receipt was not classified",
            ));
        }
        Some(ProcessWriteAccountingState::Accounting) => {
            return Err(process_write_error(
                "process write accounting state was observed while owned",
            ));
        }
    };
    match advance_process_snapshot(planning_context, outcome, increment_bound) {
        Ok(()) => {
            if increment_bound {
                accounting.mark_accounted(outcome.transaction_id);
            }
            Ok(())
        }
        Err(error) => {
            if increment_bound {
                accounting
                    .states
                    .insert(outcome.transaction_id, ProcessWriteAccountingState::Pending);
            }
            Err(error)
        }
    }
}

fn advance_process_snapshot(
    planning_context: &RwLock<PlanningContext>,
    outcome: &ProcessWriteOutcome,
    increment_bound: bool,
) -> Result<(), GatewayExecutionError> {
    let mut current = planning_context.write().map_err(|_| {
        GatewayExecutionError::new(
            "DTG-EXECUTION-CATALOG-LOCK",
            "Gateway planning catalog lock is poisoned",
            GatewayRetry::Safe,
        )
    })?;
    let [shard] = current.catalog().shards() else {
        return Err(process_write_error(
            "process CREATE completed outside a singleton catalog",
        ));
    };
    if shard.binding() != &outcome.binding {
        return Err(GatewayExecutionError::new(
            "DTG-EXECUTION-WRITE-CATALOG-RACE",
            "process CREATE committed against a replaced catalog binding",
            GatewayRetry::Safe,
        ));
    }
    let applied_index = shard.applied_index().max(outcome.applied_index);
    let transaction_time = current
        .snapshot_requirements()
        .transaction_time()
        .max(outcome.commit_time);
    let logical_scan_bound = current.logical_scan_bound().map(|bound| {
        if increment_bound {
            bound.saturating_add(1)
        } else {
            bound
        }
    });
    if applied_index == shard.applied_index()
        && transaction_time == current.snapshot_requirements().transaction_time()
        && logical_scan_bound == current.logical_scan_bound()
    {
        return Ok(());
    }
    let catalog = CatalogSnapshot::new(
        current.catalog().version(),
        current.catalog().schema_version(),
        vec![CatalogShard::new(outcome.binding.clone(), applied_index)],
    )
    .map_err(|error| process_write_error(error.to_string()))?;
    *current = PlanningContext::new(
        catalog,
        current.capabilities().clone(),
        SnapshotRequirements::new(
            transaction_time,
            current.snapshot_requirements().valid_at(),
            current.snapshot_requirements().immutable(),
        ),
        logical_scan_bound,
    )
    .map_err(|error| process_write_error(error.to_string()))?;
    Ok(())
}

fn lower_process_create_vertex(
    vertex_id: VertexId,
    labels: &[String],
    properties: &BTreeMap<String, LogicalExpr>,
    valid_from: &ValidTimeExpr,
    parameters: &BTreeMap<String, GatewayValue>,
    commit_time: dtg_storage::TransactionTime,
) -> Result<VertexVersion, GatewayExecutionError> {
    const LABELS_PROPERTY: &str = "\0dtg.labels";
    if properties.contains_key(LABELS_PROPERTY) {
        return Err(GatewayExecutionError::new(
            "DTG-EXECUTION-WRITE-LABELS",
            "process CREATE properties may not overwrite the reserved label property",
            GatewayRetry::Never,
        ));
    }
    let valid_from = match valid_from {
        ValidTimeExpr::Literal(value) => *value,
        ValidTimeExpr::Parameter(_) => {
            return Err(GatewayExecutionError::new(
                "DTG-EXECUTION-WRITE-VALID-TIME",
                "process CREATE requires a literal VALID FROM value",
                GatewayRetry::Never,
            ));
        }
    };
    let mut persisted = Properties::new();
    for (name, expression) in properties {
        let LogicalExpr::Literal(value) = bind_logical_expr(expression, parameters)? else {
            return Err(GatewayExecutionError::new(
                "DTG-EXECUTION-WRITE-PROPERTY",
                "process CREATE properties must bind to literal values",
                GatewayRetry::Never,
            ));
        };
        persisted.insert(name.clone(), value);
    }
    persisted.insert(
        LABELS_PROPERTY.into(),
        Value::List(labels.iter().cloned().map(Value::String).collect()),
    );
    VertexVersion::new(
        vertex_id,
        Version::new(1),
        ValidInterval::new(valid_from, i64::MAX)
            .map_err(|error| process_write_error(error.to_string()))?,
        commit_time,
        persisted,
    )
    .map_err(|error| process_write_error(error.to_string()))
}

fn process_write_identity(domain: &[u8], request_id: u128, ordinal: u64) -> u128 {
    let digest = process_write_digest(domain, request_id, ordinal);
    u128::from_be_bytes(
        digest[..16]
            .try_into()
            .expect("SHA-256 prefix has 16 bytes"),
    )
    .max(1)
}

fn process_write_digest(domain: &[u8], request_id: u128, ordinal: u64) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    hasher.update(request_id.to_be_bytes());
    hasher.update(ordinal.to_be_bytes());
    hasher.finalize().into()
}

fn process_write_error(message: impl Into<String>) -> GatewayExecutionError {
    GatewayExecutionError::new("DTG-EXECUTION-WRITE", message, GatewayRetry::Never)
}

fn gateway_value_to_storage(value: &GatewayValue) -> Result<Value, GatewayExecutionError> {
    match value {
        GatewayValue::Null => Ok(Value::Null),
        GatewayValue::Boolean(value) => Ok(Value::Boolean(*value)),
        GatewayValue::Integer(value) => Ok(Value::Integer(*value)),
        GatewayValue::FloatBits(value) => Ok(Value::FloatBits(*value)),
        GatewayValue::Bytes(value) => Ok(Value::Bytes(value.clone())),
        GatewayValue::String(value) => Ok(Value::String(value.clone())),
        GatewayValue::List(values) => values
            .iter()
            .map(gateway_value_to_storage)
            .collect::<Result<Vec<_>, _>>()
            .map(Value::List),
        GatewayValue::Map(values) => values
            .iter()
            .map(|(name, value)| Ok((name.clone(), gateway_value_to_storage(value)?)))
            .collect::<Result<BTreeMap<_, _>, _>>()
            .map(Value::Map),
    }
}

fn gateway_lowering_error(error: QueryError) -> GatewayExecutionError {
    GatewayExecutionError::new(
        "DTG-EXECUTION-LOWER",
        error.to_string(),
        GatewayRetry::Never,
    )
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
            ..
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
        LogicalReadOperation::Adjacency {
            vertex_id,
            direction,
        } => ReadOperation::Adjacency {
            vertex_id: *vertex_id,
            direction: *direction,
        },
        LogicalReadOperation::Traversal {
            vertex_id,
            directions,
        } => ReadOperation::Traversal {
            vertex_id: *vertex_id,
            directions: directions.clone(),
        },
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

#[cfg(test)]
mod write_receipt_tests {
    use super::*;

    #[test]
    fn pipeline_batch_keeps_request_identity_and_stops_at_its_request_limit() {
        let mut batch = GatewayPipelineBatch::default();
        for request_id in 1..=PIPELINE_MAX_BATCH_REQUESTS {
            assert!(batch.push(pipeline_request(request_id as u128)).is_ok());
        }
        assert!(
            batch
                .push(pipeline_request(PIPELINE_MAX_BATCH_REQUESTS as u128 + 1))
                .is_err()
        );

        let request_ids = batch
            .take_prefix(PIPELINE_MAX_BATCH_REQUESTS)
            .into_iter()
            .map(|request| gateway_session_request_id(request.request.as_ref()).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            request_ids,
            (1..=PIPELINE_MAX_BATCH_REQUESTS)
                .map(|request_id| request_id as u128)
                .collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn pipeline_writer_flushes_a_second_batch_after_returned_credits() {
        let (submission_sender, submission_receiver) = mpsc::channel(PIPELINE_MAX_PENDING);
        let (frame_sender, mut frame_receiver) = mpsc::channel(4);
        let pending = Arc::new(Mutex::new(BTreeMap::new()));
        for request_id in 1..=64_u128 {
            let (completion, _receiver) = oneshot::channel();
            pending.lock().unwrap().insert(request_id, completion);
        }
        let active = Arc::new(AtomicBool::new(true));
        let credits = Arc::new(AtomicUsize::new(PIPELINE_MAX_BATCH_REQUESTS));
        let credit_notify = Arc::new(Notify::new());
        tokio::spawn(write_gateway_pipeline_requests(
            submission_receiver,
            frame_sender,
            Arc::clone(&pending),
            Arc::clone(&active),
            Arc::clone(&credits),
            Arc::clone(&credit_notify),
        ));
        for request_id in 1..=64_u128 {
            submission_sender
                .send(GatewayPipelineSubmission {
                    request_id,
                    request: pipeline_request(request_id),
                })
                .await
                .unwrap();
        }

        let first = tokio::time::timeout(Duration::from_secs(1), frame_receiver.recv())
            .await
            .unwrap()
            .unwrap();
        let first_count = match first.payload.unwrap() {
            proto::gateway_pipeline_client_frame::Payload::Batch(batch) => batch.requests.len(),
            _ => panic!("writer emitted a non-batch frame"),
        };
        assert_eq!(first_count, PIPELINE_MAX_BATCH_REQUESTS);

        credits.fetch_add(PIPELINE_MAX_BATCH_REQUESTS, Ordering::AcqRel);
        credit_notify.notify_waiters();
        let second = tokio::time::timeout(Duration::from_secs(1), frame_receiver.recv())
            .await
            .unwrap()
            .unwrap();
        let second_count = match second.payload.unwrap() {
            proto::gateway_pipeline_client_frame::Payload::Batch(batch) => batch.requests.len(),
            _ => panic!("writer emitted a non-batch frame"),
        };
        assert_eq!(second_count, PIPELINE_MAX_BATCH_REQUESTS);
    }

    #[tokio::test]
    async fn pipeline_writer_splits_a_ready_batch_to_the_available_credits() {
        let (submission_sender, submission_receiver) = mpsc::channel(PIPELINE_MAX_PENDING);
        let (frame_sender, mut frame_receiver) = mpsc::channel(4);
        let pending = Arc::new(Mutex::new(BTreeMap::new()));
        for request_id in 1..=PIPELINE_MAX_BATCH_REQUESTS as u128 {
            let (completion, _receiver) = oneshot::channel();
            pending.lock().unwrap().insert(request_id, completion);
        }
        let active = Arc::new(AtomicBool::new(true));
        let credits = Arc::new(AtomicUsize::new(20));
        let credit_notify = Arc::new(Notify::new());
        tokio::spawn(write_gateway_pipeline_requests(
            submission_receiver,
            frame_sender,
            Arc::clone(&pending),
            active,
            credits,
            credit_notify,
        ));
        for request_id in 1..=PIPELINE_MAX_BATCH_REQUESTS as u128 {
            submission_sender
                .send(GatewayPipelineSubmission {
                    request_id,
                    request: pipeline_request(request_id),
                })
                .await
                .unwrap();
        }

        let frame = tokio::time::timeout(Duration::from_secs(1), frame_receiver.recv())
            .await
            .expect("writer must flush the credited prefix rather than wait for a full batch")
            .unwrap();
        let count = match frame.payload.unwrap() {
            proto::gateway_pipeline_client_frame::Payload::Batch(batch) => batch.requests.len(),
            _ => panic!("writer emitted a non-batch frame"),
        };
        assert_eq!(count, 20);
    }

    #[test]
    fn gateway_data_endpoint_parses_tcp_and_absolute_unix_forms() {
        assert_eq!(
            parse_gateway_data_endpoint("http://127.0.0.1:7690").unwrap(),
            GatewayDataEndpoint::Tcp("http://127.0.0.1:7690".into())
        );
        assert_eq!(
            parse_gateway_data_endpoint("unix:///tmp/dtg-data.sock").unwrap(),
            GatewayDataEndpoint::Unix("/tmp/dtg-data.sock".into())
        );
    }

    #[test]
    fn gateway_data_endpoint_rejects_relative_unix_and_unknown_forms() {
        for endpoint in ["unix://relative.sock", "quic://127.0.0.1:7690", "unix://"] {
            assert!(parse_gateway_data_endpoint(endpoint).is_err(), "{endpoint}");
        }
    }

    #[tokio::test]
    async fn gateway_data_channel_connects_to_a_unix_socket() {
        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("data.sock");
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        let accepted = tokio::spawn(async move { listener.accept().await.unwrap() });

        let channel = connect_gateway_data_channel(&format!("unix://{}", socket.display()))
            .await
            .unwrap();
        drop(channel);
        accepted.await.unwrap();
    }

    fn request_context() -> proto::RequestContext {
        proto::RequestContext {
            protocol_major: PROTOCOL_MAJOR,
            protocol_minor: SUPPORTED_MINOR_MAX,
            cluster_id: 7_u64.to_be_bytes().to_vec(),
            request_id: 11_u128.to_be_bytes().to_vec(),
            deadline_unix_ms: 1_900_000_000_000,
            trace_context: b"traceparent".to_vec(),
        }
    }

    fn pipeline_request(request_id: u128) -> proto::GatewayRequest {
        proto::GatewayRequest {
            request: Some(proto::RequestContext {
                request_id: request_id.to_be_bytes().to_vec(),
                ..request_context()
            }),
            execution_request: None,
            fragments: Vec::new(),
        }
    }

    fn status(body: Vec<u8>) -> proto::TypedStatus {
        proto::TypedStatus {
            request: Some(request_context()),
            code: proto::StatusCode::Ok as i32,
            retry: proto::RetryDisposition::Never as i32,
            message: "ok".into(),
            idempotency_key: Vec::new(),
            details: Some(proto::BoundedPayload {
                format_version: 1,
                declared_len: body.len() as u64,
                item_count: 1,
                checksum: checksum_bytes(&body).to_vec(),
                body,
            }),
        }
    }

    #[test]
    fn status_details_rejects_corrupt_bounded_payload_metadata() {
        let mut corrupt_checksum = status(vec![0; 9]);
        corrupt_checksum.details.as_mut().unwrap().checksum[0] ^= 1;
        assert_eq!(
            status_details(corrupt_checksum).unwrap_err().code(),
            "DTG-PROTOCOL-CHECKSUM"
        );

        let mut wrong_length = status(vec![0; 9]);
        wrong_length.details.as_mut().unwrap().declared_len += 1;
        assert_eq!(
            status_details(wrong_length).unwrap_err().code(),
            "DTG-PROTOCOL-LENGTH"
        );

        let mut zero_items = status(vec![0; 9]);
        zero_items.details.as_mut().unwrap().item_count = 0;
        assert_eq!(
            status_details(zero_items).unwrap_err().code(),
            "DTG-PROTOCOL-ITEM-LIMIT"
        );
    }

    #[test]
    fn write_receipt_requires_exactly_nine_bytes() {
        let error = decode_write_receipt(status_details(status(vec![0; 8])).unwrap()).unwrap_err();
        assert_eq!(error.code(), "DTG-EXECUTION-WRITE-RECEIPT");
    }

    #[test]
    fn write_receipt_requires_one_bounded_item_and_present_details() {
        let mut multiple_items = status(vec![0, 0, 0, 0, 0, 0, 0, 1, 0]);
        multiple_items.details.as_mut().unwrap().item_count = 2;
        assert_eq!(
            decode_write_receipt(status_details(multiple_items).unwrap())
                .unwrap_err()
                .code(),
            "DTG-EXECUTION-WRITE-RECEIPT"
        );

        let mut missing_details = status(vec![0; 9]);
        missing_details.details = None;
        assert_eq!(
            decode_write_receipt(status_details(missing_details).unwrap())
                .unwrap_err()
                .code(),
            "DTG-EXECUTION-WRITE-RECEIPT"
        );
    }

    #[test]
    fn query_response_decoder_accepts_batches_as_they_arrive() {
        let body = [
            0, 0, 0, 1, // one field
            0, 0, 0, 1, b'n', // named "n"
            0, 0, 0, 0, // zero rows
        ];
        let batch = proto::ColumnBatch {
            request: Some(request_context()),
            fragment_id: 1_u128.to_be_bytes().to_vec(),
            sequence: 1,
            row_count: 0,
            payload: Some(proto::BoundedPayload {
                format_version: 1,
                declared_len: body.len() as u64,
                item_count: 0,
                checksum: checksum_bytes(&body).to_vec(),
                body: body.to_vec(),
            }),
        };
        let mut decoder = ProtocolV2QueryResponseDecoder::default();
        decoder
            .push(proto::GatewayResponse {
                status: Some(proto::TypedStatus {
                    request: Some(request_context()),
                    code: proto::StatusCode::Ok as i32,
                    retry: proto::RetryDisposition::Never as i32,
                    message: "ok".into(),
                    idempotency_key: Vec::new(),
                    details: None,
                }),
                batch: Some(batch),
            })
            .unwrap();

        let batches = decoder.finish().unwrap();
        assert_eq!(batches[&1].len(), 1);
        assert_eq!(batches[&1][0].row_count(), 0);
    }

    #[test]
    fn query_batch_decoder_builds_columns_from_a_multi_row_payload() {
        let body = [
            0, 0, 0, 2, // two fields
            0, 0, 0, 2, b'i', b'd', 0, 0, 0, 5, b'l', b'a', b'b', b'e', b'l', 0, 0, 0,
            2, // two rows
            2, 0, 0, 0, 0, 0, 0, 0, 7, // id = 7
            5, 0, 0, 0, 5, b'f', b'i', b'r', b's', b't', 2, 0, 0, 0, 0, 0, 0, 0, 8, // id = 8
            5, 0, 0, 0, 6, b's', b'e', b'c', b'o', b'n', b'd',
        ];

        let batch = decode_query_column_batch(&body, 2).unwrap();

        assert_eq!(batch.schema().fields[0].name, "id");
        assert_eq!(batch.schema().fields[1].name, "label");
        assert_eq!(
            batch.rows(),
            vec![
                vec![QueryValue::Integer(7), QueryValue::String("first".into())],
                vec![QueryValue::Integer(8), QueryValue::String("second".into())],
            ]
        );
    }

    #[test]
    fn pending_write_accounting_rejects_new_attempts_without_evicting_ambiguity() {
        let accounting = ProcessWriteAccounting::default();
        let first = TransactionId::new(1).unwrap();
        for id in 1..=u128::try_from(PROCESS_WRITE_ACCOUNTING_LIMIT).unwrap() {
            let transaction_id = TransactionId::new(id).unwrap();
            assert!(reserve_process_write_capacity(&accounting, transaction_id).unwrap());
            complete_process_write_attempt(&accounting, transaction_id, true).unwrap();
        }
        assert!(matches!(
            accounting.state.lock().unwrap().states.get(&first),
            Some(ProcessWriteAccountingState::Pending)
        ));
        assert!(
            reserve_process_write_capacity(&accounting, TransactionId::new(5_000).unwrap())
                .is_err()
        );
        assert!(matches!(
            accounting.state.lock().unwrap().states.get(&first),
            Some(ProcessWriteAccountingState::Pending)
        ));
    }
}
