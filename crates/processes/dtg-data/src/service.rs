use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::{AtomicI64, AtomicU8, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use dtg_execution::cluster_protocol::proto::data_service_server::DataService;
use dtg_execution::cluster_protocol::proto::gateway_service_server::GatewayService;
use dtg_execution::cluster_protocol::proto::{
    ColumnBatch, ExecutionFragment, GatewayPipelineClientFrame, GatewayPipelineCredit,
    GatewayPipelineServerFrame, GatewayRequest, GatewayResponse as GatewayWireResponse,
    GatewaySessionResponse, LogicalReplicaSnapshot, RaftEnvelope, RaftMessageKind, RequestContext,
    RetryDisposition, SnapshotIngestBatch, SnapshotIngestReceipt, SnapshotIngestReceiptRequest,
    SnapshotIngestReceiptResponse, SnapshotIngestState, StatusCode, TransactionRequest,
    TypedStatus,
};
use dtg_execution::cluster_protocol::{
    PROTOCOL_MAJOR, ProtocolError, ShardRequestContext, checksum_bytes,
    validate_execution_fragment, validate_gateway_request, validate_raft_envelope,
    validate_replica_snapshot, validate_snapshot_ingest_batch,
    validate_snapshot_ingest_receipt_request, validate_transaction_request,
};
use dtg_execution::shard::{
    CommitSingleShardTransaction, ProposalReceipt, ReplicaKey,
    SINGLE_SHARD_TRANSACTION_METADATA_NAME, ShardCommand, decode_single_shard_transaction_metadata,
};
use dtg_execution::storage::{
    BackendClass, BindingRole, CapabilityManifest, ConsensusStore, LogicalMutation, StorageError,
    TransactionTime, VertexVersion,
};
use dtg_execution::{
    DataExecution, DataExecutionBuilder, GatewayRows, GatewayValue, ProviderKind, ProviderResolver,
    ReplicaBinding, RequestStage, RequestStageMetrics,
};
use dtg_storage_fjall::FjallConsensusStore;
#[cfg(test)]
use futures_util::TryStreamExt as _;
use futures_util::{StreamExt as _, stream};
use prost_011::Message as _;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio::task::{AbortHandle, JoinSet};
use tokio::time::{Instant, timeout_at};
use tokio_stream::{Stream, wrappers::ReceiverStream};
use tonic::{Request, Response, Status, Streaming};

use crate::{DataProcessConfig, FjallResolver, KuzuResolver, PostgresResolver};

const APPLY_BATCH_WINDOW: Duration = Duration::from_micros(250);
const APPLY_BATCH_MAX_COMMANDS: usize = 64;
const SNAPSHOT_INGEST_RECEIPT_LIMIT: usize = 4_096;
const SNAPSHOT_INGEST_CHANNEL_CAPACITY: usize = 4_096;
const MAX_GATEWAY_FRAGMENT_CONCURRENCY: usize = 32;
const MAX_GATEWAY_SESSION_IN_FLIGHT: usize = 32;
const MAX_GATEWAY_PIPELINE_IN_FLIGHT: usize = 32;
const MAX_CONFIGURED_GATEWAY_PIPELINE_EXECUTION: usize = 128;
const MAX_GATEWAY_PIPELINE_BATCH_REQUESTS: usize = 32;
const MAX_GATEWAY_PIPELINE_BATCH_BYTES: usize = 65_536;
const MAX_GATEWAY_PIPELINE_RESPONSE_BATCH_RESPONSES: usize = 32;
const MAX_GATEWAY_PIPELINE_RESPONSE_BATCH_BYTES: usize = 65_536;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaFailure {
    binding: ReplicaBinding,
    message: String,
}

impl ReplicaFailure {
    fn new(binding: ReplicaBinding, message: impl Into<String>) -> Self {
        Self {
            binding,
            message: message.into(),
        }
    }

    pub const fn binding(&self) -> &ReplicaBinding {
        &self.binding
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DataNodeError {
    Build(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AssignmentUpdate {
    Assign(ReplicaBinding),
    Remove(ReplicaBinding),
}

pub type RaftTransportFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(), DataNodeError>> + Send + 'a>>;

pub trait RaftTransport: Send + Sync {
    fn send<'a>(
        &'a self,
        binding: &'a ReplicaBinding,
        message: raft::eraftpb::Message,
    ) -> RaftTransportFuture<'a>;
}

struct RejectingRaftTransport;

impl RaftTransport for RejectingRaftTransport {
    fn send<'a>(
        &'a self,
        _binding: &'a ReplicaBinding,
        _message: raft::eraftpb::Message,
    ) -> RaftTransportFuture<'a> {
        Box::pin(async {
            Err(DataNodeError::Build(
                "outbound Raft transport is not configured".into(),
            ))
        })
    }
}

impl fmt::Display for DataNodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Build(message) => {
                write!(formatter, "failed to compose Data execution: {message}")
            }
        }
    }
}

impl std::error::Error for DataNodeError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LifecycleState {
    Starting,
    Ready,
    Draining,
    Stopped,
}

impl LifecycleState {
    const fn encode(self) -> u8 {
        match self {
            Self::Starting => 0,
            Self::Ready => 1,
            Self::Draining => 2,
            Self::Stopped => 3,
        }
    }

    const fn decode(value: u8) -> Self {
        match value {
            1 => Self::Ready,
            2 => Self::Draining,
            3 => Self::Stopped,
            _ => Self::Starting,
        }
    }
}

#[derive(Default)]
struct MetricCounters {
    hosted_replicas: AtomicU64,
    failed_replicas: AtomicU64,
    rpc_requests: AtomicU64,
    rpc_failures: AtomicU64,
    raft_ticks: AtomicU64,
}

#[derive(Clone, Default)]
pub struct DataMetrics {
    counters: Arc<MetricCounters>,
}

impl DataMetrics {
    pub fn hosted_replicas(&self) -> u64 {
        self.counters.hosted_replicas.load(Ordering::Relaxed)
    }

    pub fn failed_replicas(&self) -> u64 {
        self.counters.failed_replicas.load(Ordering::Relaxed)
    }

    pub fn rpc_requests(&self) -> u64 {
        self.counters.rpc_requests.load(Ordering::Relaxed)
    }

    pub fn rpc_failures(&self) -> u64 {
        self.counters.rpc_failures.load(Ordering::Relaxed)
    }

    pub fn raft_ticks(&self) -> u64 {
        self.counters.raft_ticks.load(Ordering::Relaxed)
    }

    fn record_hosted(&self) {
        self.counters
            .hosted_replicas
            .fetch_add(1, Ordering::Relaxed);
    }

    fn record_removed(&self) {
        self.counters
            .hosted_replicas
            .fetch_sub(1, Ordering::Relaxed);
    }

    fn record_replica_failure(&self) {
        self.counters
            .failed_replicas
            .fetch_add(1, Ordering::Relaxed);
    }

    fn record_rpc_request(&self) {
        self.counters.rpc_requests.fetch_add(1, Ordering::Relaxed);
    }

    fn record_rpc_failure(&self) {
        self.counters.rpc_failures.fetch_add(1, Ordering::Relaxed);
    }

    fn record_raft_tick(&self) {
        self.counters.raft_ticks.fetch_add(1, Ordering::Relaxed);
    }
}

struct ProcessState {
    lifecycle: AtomicU8,
    metrics: DataMetrics,
}

impl ProcessState {
    fn new() -> Self {
        Self {
            lifecycle: AtomicU8::new(LifecycleState::Starting.encode()),
            metrics: DataMetrics::default(),
        }
    }

    fn lifecycle(&self) -> LifecycleState {
        LifecycleState::decode(self.lifecycle.load(Ordering::Acquire))
    }

    fn set_lifecycle(&self, lifecycle: LifecycleState) {
        self.lifecycle.store(lifecycle.encode(), Ordering::Release);
    }
}

pub struct DataNodeBuilder {
    consensus_root: PathBuf,
    execution: DataExecutionBuilder,
    assignments: Vec<ReplicaBinding>,
    raft_transport: Arc<dyn RaftTransport>,
    configured_provider_kind: Option<ProviderKind>,
    provider_kinds: Vec<ProviderKind>,
}

impl DataNodeBuilder {
    pub fn new(consensus_root: impl AsRef<Path>) -> Self {
        Self {
            consensus_root: consensus_root.as_ref().to_path_buf(),
            execution: DataExecution::builder(),
            assignments: Vec::new(),
            raft_transport: Arc::new(RejectingRaftTransport),
            configured_provider_kind: None,
            provider_kinds: Vec::new(),
        }
    }

    pub fn from_config(config: DataProcessConfig) -> Self {
        let configured_provider_kind = config.backend_kind().clone();
        let mut builder = Self::new(config.consensus_root());
        builder.configured_provider_kind = Some(configured_provider_kind);
        builder = match config.backend_kind() {
            ProviderKind::Fjall => builder.with_provider(
                ProviderKind::Fjall,
                Arc::new(FjallResolver::new(config.fjall_root())),
            ),
            ProviderKind::PostgreSql => builder.with_provider(
                ProviderKind::PostgreSql,
                Arc::new(PostgresResolver::from_config(&config)),
            ),
            ProviderKind::Kuzu => builder.with_provider(
                ProviderKind::Kuzu,
                Arc::new(KuzuResolver::new(config.kuzu_root())),
            ),
            ProviderKind::Remote(_) => {
                unreachable!("environment configuration rejects remote backends")
            }
        };
        for binding in config.assignments() {
            builder = builder.assign(binding.clone());
        }
        builder
    }

    #[must_use]
    pub fn with_provider(
        mut self,
        kind: ProviderKind,
        resolver: Arc<dyn ProviderResolver>,
    ) -> Self {
        self.execution = self.execution.with_provider(kind.clone(), resolver);
        self.provider_kinds.push(kind);
        self
    }

    pub fn provider_kinds(&self) -> Vec<ProviderKind> {
        self.provider_kinds.clone()
    }

    #[must_use]
    pub fn assign(mut self, binding: ReplicaBinding) -> Self {
        self.assignments.push(binding);
        self
    }

    #[must_use]
    pub fn with_raft_transport(mut self, transport: Arc<dyn RaftTransport>) -> Self {
        self.raft_transport = transport;
        self
    }

    pub async fn start(self) -> Result<DataNode, DataNodeError> {
        if let Some(configured) = self.configured_provider_kind.as_ref() {
            if let Some(binding) = self
                .assignments
                .iter()
                .find(|binding| binding.provider_kind() != configured)
            {
                return Err(DataNodeError::Build(format!(
                    "assignment provider {:?} does not match configured backend {:?}",
                    binding.provider_kind(),
                    configured
                )));
            }
        }
        std::fs::create_dir_all(&self.consensus_root).map_err(|error| {
            DataNodeError::Build(format!("cannot create Fjall consensus root: {error}"))
        })?;
        let request_metrics = Arc::new(RequestStageMetrics::default());
        let pipeline_execution_permits =
            Arc::new(Semaphore::new(configured_gateway_pipeline_execution_limit()));
        let execution = self
            .execution
            .with_request_metrics(Arc::clone(&request_metrics))
            .build()
            .map_err(|error| DataNodeError::Build(error.to_string()))?;
        let state = Arc::new(ProcessState::new());
        let execution = Arc::new(execution);
        let observed = Arc::new(Mutex::new(Vec::new()));
        let failures = Arc::new(Mutex::new(Vec::new()));
        let apply_batchers = Arc::new(Mutex::new(BTreeMap::new()));
        let snapshot_ingest_receipts = Arc::new(Mutex::new(SnapshotIngestReceipts::default()));
        let last_snapshot_commit_time = Arc::new(AtomicI64::new(0));
        let (snapshot_ingest_sender, mut snapshot_ingest_receiver) =
            mpsc::channel(SNAPSHOT_INGEST_CHANNEL_CAPACITY);
        for binding in self.assignments {
            let result = add_assignment(&execution, &self.consensus_root, binding.clone()).await;
            match result {
                Ok(()) => {
                    state.metrics.record_hosted();
                    observed
                        .lock()
                        .expect("observed mutex is poisoned")
                        .push(binding);
                }
                Err(error) => {
                    state.metrics.record_replica_failure();
                    failures
                        .lock()
                        .expect("failures mutex is poisoned")
                        .push(ReplicaFailure::new(binding, error.to_string()));
                }
            }
        }
        state.set_lifecycle(LifecycleState::Ready);

        let driver_stop = Arc::new(AtomicBool::new(false));
        spawn_raft_driver(
            Arc::clone(&execution),
            Arc::clone(&state),
            Arc::clone(&driver_stop),
            self.raft_transport,
        );
        let ingest_service = DataRpcService {
            state: Arc::clone(&state),
            execution: Arc::clone(&execution),
            request_metrics: Arc::clone(&request_metrics),
            apply_batchers: Arc::clone(&apply_batchers),
            snapshot_ingest_sender: snapshot_ingest_sender.clone(),
            snapshot_ingest_receipts: Arc::clone(&snapshot_ingest_receipts),
            last_snapshot_commit_time: Arc::clone(&last_snapshot_commit_time),
            pipeline_execution_permits: Arc::clone(&pipeline_execution_permits),
        };
        tokio::spawn(async move {
            while let Some(pending) = snapshot_ingest_receiver.recv().await {
                let state = match ingest_service
                    .apply_snapshot_ingest(pending.transaction)
                    .await
                {
                    Ok((applied_index, commit_time)) => SnapshotIngestReceiptState::Committed {
                        applied_index,
                        commit_time,
                    },
                    Err(error) => SnapshotIngestReceiptState::Rejected {
                        message: error.message().to_owned(),
                    },
                };
                if let Ok(mut receipts) = ingest_service.snapshot_ingest_receipts.lock() {
                    receipts.complete(pending.receipt_id, state);
                }
            }
        });

        Ok(DataNode {
            execution,
            consensus_root: self.consensus_root,
            observed,
            failures,
            state,
            request_metrics,
            apply_batchers,
            snapshot_ingest_sender,
            snapshot_ingest_receipts,
            last_snapshot_commit_time,
            pipeline_execution_permits,
            driver_stop,
        })
    }
}

async fn add_assignment(
    execution: &DataExecution,
    consensus_root: &Path,
    binding: ReplicaBinding,
) -> Result<(), DataNodeError> {
    let runtime_store = execution
        .open_runtime_store(binding.clone())
        .await
        .map_err(|error| DataNodeError::Build(error.to_string()))?;
    let consensus_binding =
        consensus_binding(&binding).map_err(|error| DataNodeError::Build(error.to_string()))?;
    let consensus_path = consensus_root.join(consensus_binding.namespace_id().as_str());
    let consensus_store = Arc::new(
        FjallConsensusStore::open(consensus_path, consensus_binding)
            .map_err(|error| DataNodeError::Build(error.to_string()))?,
    );
    let mut membership = match consensus_store.membership().await {
        Ok(membership) => membership,
        Err(StorageError::NotFound) => dtg_execution::storage::RaftMembership {
            voters: Vec::new(),
            learners: Vec::new(),
            configuration_index: 0,
        },
        Err(error) => {
            return Err(DataNodeError::Build(format!(
                "cannot read replica membership: {error}"
            )));
        }
    };
    if membership.voters.is_empty() && membership.learners.is_empty() {
        membership.voters.push(binding.replica_id());
        consensus_store
            .set_membership(membership.clone())
            .await
            .map_err(|error| {
                DataNodeError::Build(format!("cannot bootstrap replica membership: {error}"))
            })?;
    }
    if let Some(activation) = runtime_store.activation() {
        dtg_execution::shard::recover_replica_snapshot_install(
            activation.as_ref(),
            consensus_store.as_ref(),
        )
        .map_err(|error| DataNodeError::Build(error.to_string()))?;
    }
    let campaign = membership.voters == [binding.replica_id()] && membership.learners.is_empty();
    let key = execution
        .add_replica_runtime(consensus_store, runtime_store)
        .map_err(|error| DataNodeError::Build(error.to_string()))?;
    execution
        .start_replica(key)
        .map_err(|error| DataNodeError::Build(error.to_string()))?;
    if campaign {
        execution
            .campaign_replica(key)
            .map_err(|error| DataNodeError::Build(error.to_string()))?;
    }
    execution
        .drive_replica(key)
        .map(drop)
        .map_err(|error| DataNodeError::Build(error.to_string()))
}

fn consensus_binding(binding: &ReplicaBinding) -> Result<ReplicaBinding, StorageError> {
    let capabilities = CapabilityManifest::from_names([
        "adjacency",
        "immutable-read-view",
        "logical-snapshot",
        "point",
    ])?;
    let backend = BackendClass::new(
        ProviderKind::Fjall,
        1,
        1,
        capabilities.names().map(str::to_owned),
    )?;
    ReplicaBinding::builder()
        .cluster_id(binding.cluster_id().get())
        .graph_id(binding.graph_id().get())
        .shard_id(binding.shard_id().get())
        .placement_epoch(binding.placement_epoch().get())
        .replica_id(binding.replica_id().get())
        .backend_generation(binding.backend_generation().get())
        .backend_class_digest(backend.digest())
        .provider_kind(ProviderKind::Fjall)
        .contract_version(1)
        .layout_version(1)
        .capability_digest(capabilities.digest())
        .namespace_id(format!(
            "raft-{}",
            hex_digest(binding.identity_digest().get())
        ))
        .endpoint_profile_ref("local-fjall-consensus")
        .credential_ref("local-fjall-consensus")
        .role(BindingRole::Active)
        .build()
}

fn configured_gateway_pipeline_execution_limit() -> usize {
    gateway_pipeline_execution_limit(
        std::env::var("DTG_DATA_GATEWAY_PIPELINE_EXECUTION_LIMIT")
            .ok()
            .as_deref(),
    )
}

fn gateway_pipeline_execution_limit(configured: Option<&str>) -> usize {
    configured
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|limit| (1..=MAX_CONFIGURED_GATEWAY_PIPELINE_EXECUTION).contains(limit))
        .unwrap_or(MAX_GATEWAY_PIPELINE_IN_FLIGHT)
}

fn hex_digest(bytes: [u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn spawn_raft_driver(
    execution: Arc<DataExecution>,
    state: Arc<ProcessState>,
    stop: Arc<AtomicBool>,
    transport: Arc<dyn RaftTransport>,
) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(50));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            interval.tick().await;
            if stop.load(Ordering::Acquire) {
                return;
            }
            if state.lifecycle() != LifecycleState::Ready {
                continue;
            }
            for key in execution.replica_keys() {
                state.metrics.record_raft_tick();
                if execution.tick_replica(key).is_err() {
                    state.metrics.record_replica_failure();
                    continue;
                }
                let binding = match execution.replica_observation(key) {
                    Ok(observation) => observation.binding().clone(),
                    Err(_) => {
                        state.metrics.record_replica_failure();
                        continue;
                    }
                };
                let mut progress = match execution.drive_replica(key) {
                    Ok(progress) => progress,
                    Err(_) => {
                        state.metrics.record_replica_failure();
                        continue;
                    }
                };
                for message in progress.take_messages() {
                    if transport.send(&binding, message).await.is_err() {
                        state.metrics.record_replica_failure();
                    }
                }
            }
        }
    });
}

struct PendingApply {
    command_id: u128,
    command: ShardCommand,
    queued_at: Instant,
    completion: oneshot::Sender<Result<BatchedApplyReceipt, String>>,
}

#[derive(Clone)]
struct BatchedApplyReceipt {
    receipt: ProposalReceipt,
}

#[derive(Clone)]
struct PendingSnapshotIngest {
    receipt_id: u128,
    transaction: TransactionRequest,
}

#[derive(Clone)]
enum SnapshotIngestReceiptState {
    Pending,
    Committed {
        applied_index: u64,
        commit_time: TransactionTime,
    },
    Rejected {
        message: String,
    },
}

#[derive(Clone)]
struct SnapshotIngestReceiptEntry {
    payload_digest: [u8; 32],
    state: SnapshotIngestReceiptState,
}

#[derive(Default)]
struct SnapshotIngestReceipts {
    entries: BTreeMap<u128, SnapshotIngestReceiptEntry>,
    completed_order: std::collections::VecDeque<u128>,
}

enum SnapshotIngestAdmissionError {
    Capacity,
    ReceiptPayloadMismatch,
}

impl SnapshotIngestReceipts {
    fn admit(
        &mut self,
        receipt_id: u128,
        payload_digest: [u8; 32],
    ) -> Result<bool, SnapshotIngestAdmissionError> {
        if let Some(existing) = self.entries.get(&receipt_id) {
            if existing.payload_digest != payload_digest {
                return Err(SnapshotIngestAdmissionError::ReceiptPayloadMismatch);
            }
            return Ok(false);
        }
        if self.entries.len() >= SNAPSHOT_INGEST_RECEIPT_LIMIT {
            self.evict_completed();
        }
        if self.entries.len() >= SNAPSHOT_INGEST_RECEIPT_LIMIT {
            return Err(SnapshotIngestAdmissionError::Capacity);
        }
        self.entries.insert(
            receipt_id,
            SnapshotIngestReceiptEntry {
                payload_digest,
                state: SnapshotIngestReceiptState::Pending,
            },
        );
        Ok(true)
    }

    fn complete(&mut self, receipt_id: u128, state: SnapshotIngestReceiptState) {
        if matches!(
            self.entries.get(&receipt_id).map(|entry| &entry.state),
            Some(SnapshotIngestReceiptState::Pending)
        ) {
            self.entries
                .get_mut(&receipt_id)
                .expect("receipt exists")
                .state = state;
            self.completed_order.push_back(receipt_id);
        }
        self.evict_completed();
    }

    fn get(&self, receipt_id: u128) -> Option<SnapshotIngestReceiptState> {
        self.entries
            .get(&receipt_id)
            .map(|entry| entry.state.clone())
    }

    fn remove(&mut self, receipt_id: u128) {
        self.entries.remove(&receipt_id);
    }

    fn evict_completed(&mut self) {
        while self.entries.len() >= SNAPSHOT_INGEST_RECEIPT_LIMIT {
            let Some(receipt_id) = self.completed_order.pop_front() else {
                return;
            };
            if !matches!(
                self.entries.get(&receipt_id).map(|entry| &entry.state),
                Some(SnapshotIngestReceiptState::Pending)
            ) {
                self.entries.remove(&receipt_id);
            }
        }
    }
}

fn snapshot_ingest_receipt(
    receipt_id: u128,
    state: SnapshotIngestReceiptState,
) -> SnapshotIngestReceipt {
    let (state, applied_index, message, commit_time) = match state {
        SnapshotIngestReceiptState::Pending => (
            SnapshotIngestState::Pending,
            0,
            "accepted by Data ingress".into(),
            0,
        ),
        SnapshotIngestReceiptState::Committed {
            applied_index,
            commit_time,
        } => (
            SnapshotIngestState::Committed,
            applied_index,
            "committed into the stable snapshot".into(),
            commit_time.get(),
        ),
        SnapshotIngestReceiptState::Rejected { message } => {
            (SnapshotIngestState::Rejected, 0, message, 0)
        }
    };
    SnapshotIngestReceipt {
        receipt_id: receipt_id.to_be_bytes().to_vec(),
        state: state.into(),
        applied_index,
        message,
        commit_time,
    }
}

fn snapshot_ingest_payload_digest(transaction: &TransactionRequest) -> [u8; 32] {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&transaction.transaction_id);
    bytes.extend_from_slice(&transaction.operation.to_be_bytes());
    bytes.extend_from_slice(&transaction.idempotency_key);
    if let Some(context) = &transaction.context {
        bytes.extend_from_slice(&context.graph_id.to_be_bytes());
        bytes.extend_from_slice(&context.shard_id.to_be_bytes());
        bytes.extend_from_slice(&context.placement_epoch.to_be_bytes());
        bytes.extend_from_slice(&context.backend_generation.to_be_bytes());
        bytes.extend_from_slice(&context.catalog_version.to_be_bytes());
    }
    if let Some(payload) = &transaction.payload {
        bytes.extend_from_slice(&payload.format_version.to_be_bytes());
        bytes.extend_from_slice(&payload.declared_len.to_be_bytes());
        bytes.extend_from_slice(&payload.item_count.to_be_bytes());
        bytes.extend_from_slice(&payload.checksum);
        bytes.extend_from_slice(&payload.body);
    }
    checksum_bytes(&bytes)
}

async fn run_apply_batcher(
    execution: Arc<DataExecution>,
    request_metrics: Arc<RequestStageMetrics>,
    key: ReplicaKey,
    mut receiver: mpsc::Receiver<PendingApply>,
) {
    while let Some(first) = receiver.recv().await {
        let mut batch = vec![first];
        let deadline = Instant::now() + APPLY_BATCH_WINDOW;
        while batch.len() < APPLY_BATCH_MAX_COMMANDS {
            match timeout_at(deadline, receiver.recv()).await {
                Ok(Some(request)) => batch.push(request),
                Ok(None) | Err(_) => break,
            }
        }

        let command_ids = batch
            .iter()
            .map(|request| request.command_id)
            .collect::<Vec<_>>();
        let commands = batch
            .iter()
            .map(|request| request.command.clone())
            .collect::<Vec<_>>();
        for request in &batch {
            request_metrics.record_detail(
                dtg_execution::RequestDetail::DataRaftBatchQueue,
                dtg_execution::StageOutcome::Success,
                elapsed_nanoseconds(request.queued_at),
            );
        }
        let execution = Arc::clone(&execution);
        let raft_apply_timers = (0..batch.len())
            .map(|_| request_metrics.start_detail(dtg_execution::RequestDetail::DataRaftApply))
            .collect::<Vec<_>>();
        let provider_apply_timers = (0..batch.len())
            .map(|_| request_metrics.start_detail(dtg_execution::RequestDetail::DataProviderApply))
            .collect::<Vec<_>>();
        let dispatch_started = Instant::now();
        let result = tokio::task::spawn_blocking(move || {
            let dispatch_nanoseconds = elapsed_nanoseconds(dispatch_started);
            execution
                .apply_transaction_commands_timed(key, commands)
                .map(|timing| (dispatch_nanoseconds, timing))
        })
        .await;
        let completions = match result {
            Ok(Ok((dispatch_nanoseconds, timing))) => {
                for timer in raft_apply_timers {
                    timer.finish(dtg_execution::StageOutcome::Success);
                }
                for timer in provider_apply_timers {
                    timer.finish(dtg_execution::StageOutcome::Success);
                }
                for _ in &batch {
                    request_metrics.record_detail(
                        dtg_execution::RequestDetail::DataRaftBlockingDispatch,
                        dtg_execution::StageOutcome::Success,
                        dispatch_nanoseconds,
                    );
                }
                request_metrics.record_detail(
                    dtg_execution::RequestDetail::DataRaftLockWait,
                    dtg_execution::StageOutcome::Success,
                    timing.lock_wait_nanoseconds(),
                );
                request_metrics.record_detail(
                    dtg_execution::RequestDetail::DataRaftPropose,
                    dtg_execution::StageOutcome::Success,
                    timing.propose_nanoseconds(),
                );
                request_metrics.record_detail(
                    dtg_execution::RequestDetail::DataRaftDriveReady,
                    dtg_execution::StageOutcome::Success,
                    timing.drive_ready_nanoseconds(),
                );
                let receipts = timing.into_progress().receipts().to_vec();
                if receipts.len() != batch.len()
                    || receipts
                        .iter()
                        .zip(&command_ids)
                        .any(|(receipt, command_id)| receipt.command_id() != *command_id)
                {
                    vec![
                        Err("Raft batch receipts do not match proposed commands".into());
                        batch.len()
                    ]
                } else {
                    receipts
                        .into_iter()
                        .map(|receipt| Ok(BatchedApplyReceipt { receipt }))
                        .collect()
                }
            }
            Ok(Err(error)) => {
                for timer in raft_apply_timers {
                    timer.finish(dtg_execution::StageOutcome::Error);
                }
                for timer in provider_apply_timers {
                    timer.finish(dtg_execution::StageOutcome::Error);
                }
                vec![Err(error.to_string()); batch.len()]
            }
            Err(error) => {
                for timer in raft_apply_timers {
                    timer.finish(dtg_execution::StageOutcome::Error);
                }
                for timer in provider_apply_timers {
                    timer.finish(dtg_execution::StageOutcome::Error);
                }
                vec![Err(format!("Raft batch worker failed: {error}")); batch.len()]
            }
        };
        for (request, completion) in batch.into_iter().zip(completions) {
            let _ = request.completion.send(completion);
        }
    }
}

pub struct DataNode {
    execution: Arc<DataExecution>,
    consensus_root: PathBuf,
    observed: Arc<Mutex<Vec<ReplicaBinding>>>,
    failures: Arc<Mutex<Vec<ReplicaFailure>>>,
    state: Arc<ProcessState>,
    request_metrics: Arc<RequestStageMetrics>,
    apply_batchers: Arc<Mutex<BTreeMap<ReplicaKey, mpsc::Sender<PendingApply>>>>,
    snapshot_ingest_sender: mpsc::Sender<PendingSnapshotIngest>,
    snapshot_ingest_receipts: Arc<Mutex<SnapshotIngestReceipts>>,
    last_snapshot_commit_time: Arc<AtomicI64>,
    pipeline_execution_permits: Arc<Semaphore>,
    driver_stop: Arc<AtomicBool>,
}

impl DataNode {
    pub async fn observed_replicas(&self) -> Vec<ReplicaBinding> {
        self.observed
            .lock()
            .expect("observed mutex is poisoned")
            .clone()
    }

    pub async fn replica_failures(&self) -> Vec<ReplicaFailure> {
        self.failures
            .lock()
            .expect("failures mutex is poisoned")
            .clone()
    }

    pub fn provider_kinds(&self) -> Vec<ProviderKind> {
        self.execution.provider_kinds()
    }

    pub async fn replica_observations(&self) -> Vec<dtg_execution::shard::ReplicaObservation> {
        self.execution.replica_observations()
    }

    pub async fn apply_assignment(&self, update: AssignmentUpdate) -> Result<(), DataNodeError> {
        match update {
            AssignmentUpdate::Assign(binding) => {
                if self
                    .observed
                    .lock()
                    .map_err(|_| {
                        DataNodeError::Build("observed replica registry is poisoned".into())
                    })?
                    .contains(&binding)
                {
                    return Ok(());
                }
                add_assignment(&self.execution, &self.consensus_root, binding.clone()).await?;
                let mut observed = self.observed.lock().map_err(|_| {
                    DataNodeError::Build("observed replica registry is poisoned".into())
                })?;
                if !observed.contains(&binding) {
                    observed.push(binding);
                    observed.sort_unstable_by_key(|binding| {
                        (binding.graph_id(), binding.shard_id(), binding.replica_id())
                    });
                    self.state.metrics.record_hosted();
                }
                Ok(())
            }
            AssignmentUpdate::Remove(binding) => {
                if !self
                    .observed
                    .lock()
                    .map_err(|_| {
                        DataNodeError::Build("observed replica registry is poisoned".into())
                    })?
                    .contains(&binding)
                {
                    return Ok(());
                }
                let key = self
                    .execution
                    .locate_replica(
                        binding.cluster_id(),
                        binding.graph_id(),
                        binding.shard_id(),
                        binding.placement_epoch(),
                        binding.backend_generation(),
                        Some(binding.replica_id()),
                    )
                    .map_err(|error| DataNodeError::Build(error.to_string()))?;
                self.execution
                    .remove_replica(key)
                    .map_err(|error| DataNodeError::Build(error.to_string()))?;
                let mut observed = self.observed.lock().map_err(|_| {
                    DataNodeError::Build("observed replica registry is poisoned".into())
                })?;
                let previous = observed.len();
                observed.retain(|existing| existing != &binding);
                if observed.len() != previous {
                    self.state.metrics.record_removed();
                }
                Ok(())
            }
        }
    }

    pub async fn watch_assignments(
        self: Arc<Self>,
        mut updates: tokio::sync::mpsc::Receiver<AssignmentUpdate>,
    ) -> Result<(), DataNodeError> {
        while let Some(update) = updates.recv().await {
            self.apply_assignment(update).await?;
        }
        Ok(())
    }

    pub fn lifecycle(&self) -> LifecycleState {
        self.state.lifecycle()
    }

    pub fn metrics(&self) -> DataMetrics {
        self.state.metrics.clone()
    }

    pub fn request_metrics(&self) -> Arc<RequestStageMetrics> {
        Arc::clone(&self.request_metrics)
    }

    pub fn rpc_service(&self) -> DataRpcService {
        DataRpcService {
            state: self.state.clone(),
            execution: self.execution.clone(),
            request_metrics: Arc::clone(&self.request_metrics),
            apply_batchers: Arc::clone(&self.apply_batchers),
            snapshot_ingest_sender: self.snapshot_ingest_sender.clone(),
            snapshot_ingest_receipts: Arc::clone(&self.snapshot_ingest_receipts),
            last_snapshot_commit_time: Arc::clone(&self.last_snapshot_commit_time),
            pipeline_execution_permits: Arc::clone(&self.pipeline_execution_permits),
        }
    }

    pub fn begin_draining(&self) {
        self.state.set_lifecycle(LifecycleState::Draining);
    }

    pub fn stop(&self) {
        self.driver_stop.store(true, Ordering::Release);
        self.state.set_lifecycle(LifecycleState::Stopped);
    }
}

impl Drop for DataNode {
    fn drop(&mut self) {
        self.driver_stop.store(true, Ordering::Release);
    }
}

#[derive(Clone)]
pub struct DataRpcService {
    state: Arc<ProcessState>,
    execution: Arc<DataExecution>,
    request_metrics: Arc<RequestStageMetrics>,
    apply_batchers: Arc<Mutex<BTreeMap<ReplicaKey, mpsc::Sender<PendingApply>>>>,
    snapshot_ingest_sender: mpsc::Sender<PendingSnapshotIngest>,
    snapshot_ingest_receipts: Arc<Mutex<SnapshotIngestReceipts>>,
    last_snapshot_commit_time: Arc<AtomicI64>,
    pipeline_execution_permits: Arc<Semaphore>,
}

impl DataRpcService {
    pub const fn protocol_major(&self) -> u32 {
        PROTOCOL_MAJOR
    }

    pub fn lifecycle(&self) -> LifecycleState {
        self.state.lifecycle()
    }

    pub fn metrics(&self) -> DataMetrics {
        self.state.metrics.clone()
    }

    pub fn request_metrics(&self) -> Arc<RequestStageMetrics> {
        Arc::clone(&self.request_metrics)
    }

    pub fn begin_draining(&self) {
        self.state.set_lifecycle(LifecycleState::Draining);
    }

    fn apply_batch_sender(&self, key: ReplicaKey) -> Result<mpsc::Sender<PendingApply>, Status> {
        let mut batchers = self
            .apply_batchers
            .lock()
            .map_err(|_| self.execution_failure("Raft apply batch registry is poisoned"))?;
        if let Some(sender) = batchers.get(&key) {
            return Ok(sender.clone());
        }
        let (sender, receiver) = mpsc::channel(APPLY_BATCH_MAX_COMMANDS);
        tokio::spawn(run_apply_batcher(
            Arc::clone(&self.execution),
            Arc::clone(&self.request_metrics),
            key,
            receiver,
        ));
        batchers.insert(key, sender.clone());
        Ok(sender)
    }

    async fn apply_transaction_batched(
        &self,
        key: ReplicaKey,
        command: ShardCommand,
    ) -> Result<BatchedApplyReceipt, Status> {
        let command_id = command.header().command_id().get();
        let sender = self.apply_batch_sender(key)?;
        let (completion, response) = oneshot::channel();
        let queue_timer = self
            .request_metrics
            .start_detail(dtg_execution::RequestDetail::DataRaftQueue);
        let admission_started = Instant::now();
        let permit = queue_timer.finish_result(
            sender
                .reserve()
                .await
                .map_err(|_| self.execution_failure("Raft apply batch worker stopped")),
        )?;
        self.request_metrics.record_detail(
            dtg_execution::RequestDetail::DataRaftBatchAdmission,
            dtg_execution::StageOutcome::Success,
            elapsed_nanoseconds(admission_started),
        );
        permit.send(PendingApply {
            command_id,
            command,
            queued_at: Instant::now(),
            completion,
        });
        response
            .await
            .map_err(|_| self.execution_failure("Raft apply batch worker dropped its response"))?
            .map_err(|error| self.execution_failure(error))
    }

    async fn apply_snapshot_ingest(
        &self,
        transaction: TransactionRequest,
    ) -> Result<(u64, TransactionTime), Status> {
        let shard_context: ShardRequestContext = transaction
            .context
            .clone()
            .ok_or_else(|| self.invalid(ProtocolError::MissingContext))?
            .try_into()
            .map_err(|error| self.invalid(error))?;
        let payload = validate_transaction_request(transaction.clone())
            .map_err(|error| self.invalid(error))?;
        if transaction.operation
            != dtg_execution::cluster_protocol::proto::TransactionOperation::CommitSnapshot as i32
        {
            return Err(self.invalid(ProtocolError::UnknownEnum));
        }
        let command =
            ShardCommand::decode(payload.body()).map_err(|error| self.execution_failure(error))?;
        if command.header().placement_epoch() != shard_context.placement_epoch()
            || command.header().backend_generation() != shard_context.backend_generation()
        {
            return Err(self.execution_failure("transaction command and request fence differ"));
        }
        let lookup = self
            .execution
            .locate_replica_timed(
                shard_context.request().cluster_id(),
                shard_context.graph_id(),
                shard_context.shard_id(),
                shard_context.placement_epoch(),
                shard_context.backend_generation(),
                None,
            )
            .map_err(|error| self.execution_failure(error))?;
        let (command, commit_time) = if let Some(commit_time) = self
            .existing_snapshot_commit_time(lookup.key(), &command)
            .await?
        {
            self.stamp_snapshot_commit_at(command, commit_time)?
        } else {
            self.stamp_snapshot_commit(command)?
        };
        let receipt = self
            .apply_transaction_batched(lookup.key(), command)
            .await?;
        if receipt.receipt.rejection().is_some() {
            return Err(self.execution_failure("snapshot ingest transaction was rejected"));
        }
        Ok((receipt.receipt.index(), commit_time))
    }

    fn snapshot_ingest_state(
        &self,
        receipt_id: u128,
    ) -> Result<Option<SnapshotIngestReceiptState>, Status> {
        self.snapshot_ingest_receipts
            .lock()
            .map_err(|_| self.execution_failure("snapshot ingest receipt registry is poisoned"))
            .map(|receipts| receipts.get(receipt_id))
    }

    fn next_snapshot_commit_time(
        &self,
        start_time: TransactionTime,
    ) -> Result<TransactionTime, Status> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| self.execution_failure("system clock precedes the Unix epoch"))?
            .as_micros();
        let now = i64::try_from(now).unwrap_or(i64::MAX);
        let mut current = self.last_snapshot_commit_time.load(Ordering::Acquire);
        loop {
            let next = now
                .max(start_time.get().saturating_add(1))
                .max(current.saturating_add(1));
            match self.last_snapshot_commit_time.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return TransactionTime::new(next)
                        .map_err(|error| self.execution_failure(error));
                }
                Err(observed) => current = observed,
            }
        }
    }

    fn stamp_snapshot_commit(
        &self,
        command: ShardCommand,
    ) -> Result<(ShardCommand, TransactionTime), Status> {
        let ShardCommand::CommitSingleShardTransaction(transaction) = command else {
            return Err(
                self.execution_failure("snapshot commit requires a single-Shard transaction")
            );
        };
        let commit_time = self.next_snapshot_commit_time(transaction.start_time())?;
        self.stamp_snapshot_transaction(transaction, commit_time)
    }

    fn stamp_snapshot_commit_at(
        &self,
        command: ShardCommand,
        commit_time: TransactionTime,
    ) -> Result<(ShardCommand, TransactionTime), Status> {
        let ShardCommand::CommitSingleShardTransaction(transaction) = command else {
            return Err(
                self.execution_failure("snapshot commit requires a single-Shard transaction")
            );
        };
        self.stamp_snapshot_transaction(transaction, commit_time)
    }

    fn stamp_snapshot_transaction(
        &self,
        transaction: CommitSingleShardTransaction,
        commit_time: TransactionTime,
    ) -> Result<(ShardCommand, TransactionTime), Status> {
        let mutations = transaction
            .mutations()
            .iter()
            .map(|mutation| match mutation {
                LogicalMutation::PutVertex(vertex) => VertexVersion::new(
                    vertex.id(),
                    vertex.version(),
                    vertex.valid_time(),
                    commit_time,
                    vertex.properties().clone(),
                )
                .map(LogicalMutation::PutVertex),
                _ => Err(StorageError::InvalidMutation(
                    "snapshot commit currently supports vertex mutations only".into(),
                )),
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| self.execution_failure(error))?;
        let command = CommitSingleShardTransaction::new(
            transaction.header().command_id(),
            transaction.header().placement_epoch().get(),
            transaction.header().backend_generation().get(),
            transaction.transaction_id(),
            transaction.start_time(),
            transaction.snapshot_applied_index(),
            transaction.request_digest(),
            mutations,
        )
        .map(ShardCommand::CommitSingleShardTransaction)
        .map_err(|error| self.execution_failure(error))?;
        Ok((command, commit_time))
    }

    async fn existing_snapshot_commit_time(
        &self,
        key: ReplicaKey,
        command: &ShardCommand,
    ) -> Result<Option<TransactionTime>, Status> {
        let ShardCommand::CommitSingleShardTransaction(transaction) = command else {
            return Err(
                self.execution_failure("snapshot commit requires a single-Shard transaction")
            );
        };
        let name = format!(
            "{SINGLE_SHARD_TRANSACTION_METADATA_NAME}{:032x}",
            transaction.transaction_id().get()
        );
        let store = self
            .execution
            .replica_state_store(key)
            .map_err(|error| self.execution_failure(error))?;
        let Some(metadata) = store
            .replica_metadata(&name)
            .await
            .map_err(|error| self.execution_failure(error))?
        else {
            return Ok(None);
        };
        let receipt = decode_single_shard_transaction_metadata(&metadata)
            .map_err(|error| self.execution_failure(error))?;
        if receipt.command_id() != transaction.header().command_id()
            || receipt.start_time() != transaction.start_time()
            || receipt.snapshot_applied_index() != transaction.snapshot_applied_index()
            || receipt.request_digest() != transaction.request_digest()
        {
            return Err(self.execution_failure(
                "snapshot transaction ID is already bound to a different command",
            ));
        }
        Ok(Some(receipt.commit_time()))
    }

    fn begin_request(&self) -> Result<(), Status> {
        self.state.metrics.record_rpc_request();
        if self.lifecycle() != LifecycleState::Ready {
            self.state.metrics.record_rpc_failure();
            return Err(Status::unavailable("Data process is not ready"));
        }
        Ok(())
    }

    fn invalid(&self, error: ProtocolError) -> Status {
        self.state.metrics.record_rpc_failure();
        Status::invalid_argument(error.code())
    }

    fn execution_failure(&self, error: impl fmt::Display) -> Status {
        self.state.metrics.record_rpc_failure();
        Status::failed_precondition(error.to_string())
    }

    async fn execute_fragment_wire(
        &self,
        wire: ExecutionFragment,
    ) -> Result<Vec<ColumnBatch>, Status> {
        let response_context = wire
            .context
            .as_ref()
            .and_then(|context| context.request.clone());
        let timer = self.request_metrics.start(RequestStage::DataValidation);
        let validation_detail = self
            .request_metrics
            .start_detail(dtg_execution::RequestDetail::DataValidation);
        let validation: Result<_, Status> = (|| {
            let shard_context: ShardRequestContext = wire
                .context
                .clone()
                .ok_or_else(|| self.invalid(ProtocolError::MissingContext))?
                .try_into()
                .map_err(|error| self.invalid(error))?;
            let payload =
                validate_execution_fragment(wire.clone()).map_err(|error| self.invalid(error))?;
            Ok((shard_context, payload))
        })();
        let (shard_context, payload) =
            validation_detail.finish_result(timer.finish_result(validation))?;
        let timer = self.request_metrics.start(RequestStage::DataRouting);
        let lookup = timer.finish_result(
            self.execution
                .locate_replica_timed(
                    shard_context.request().cluster_id(),
                    shard_context.graph_id(),
                    shard_context.shard_id(),
                    shard_context.placement_epoch(),
                    shard_context.backend_generation(),
                    None,
                )
                .map_err(|error| self.execution_failure(error)),
        )?;
        self.request_metrics.record_detail(
            dtg_execution::RequestDetail::DataRouteLockWait,
            dtg_execution::StageOutcome::Success,
            lookup.lock_wait_nanoseconds(),
        );
        self.request_metrics.record_detail(
            dtg_execution::RequestDetail::DataRouteLookup,
            dtg_execution::StageOutcome::Success,
            lookup.lookup_nanoseconds(),
        );
        let key = lookup.key();
        let observation = self
            .execution
            .replica_observation(key)
            .map_err(|error| self.execution_failure(error))?;
        if observation.binding().capability_digest().get().as_slice()
            != wire.capability_digest.as_slice()
        {
            return Err(self.execution_failure("fragment capability digest drifted"));
        }
        let transaction_time = dtg_execution::storage::TransactionTime::new(wire.transaction_time)
            .map_err(|error| self.execution_failure(error))?;
        let timer = self
            .request_metrics
            .start(RequestStage::DataProviderExecution);
        let execution_detail = self
            .request_metrics
            .start_detail(dtg_execution::RequestDetail::DataExecution);
        let provider_apply = self
            .request_metrics
            .start_detail(dtg_execution::RequestDetail::DataProviderApply);
        let rows = provider_apply.finish_result(
            execution_detail.finish_result(
                timer.finish_result(
                    self.execution
                        .execute_fragment(
                            key,
                            wire.applied_index,
                            transaction_time,
                            wire.valid_at,
                            payload.body(),
                        )
                        .await
                        .map_err(|error| self.execution_failure(error)),
                ),
            ),
        )?;
        encode_fragment_batches(response_context, wire.fragment_id, rows)
            .map_err(|error| self.execution_failure(error))
    }
}

fn elapsed_nanoseconds(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

fn encode_fragment_batches(
    request: Option<dtg_execution::cluster_protocol::proto::RequestContext>,
    fragment_id: Vec<u8>,
    rows: GatewayRows,
) -> Result<Vec<ColumnBatch>, &'static str> {
    let mut batches = Vec::new();
    let chunks = if rows.rows().is_empty() {
        vec![&[][..]]
    } else {
        rows.rows().chunks(1024).collect()
    };
    for (index, chunk) in chunks.into_iter().enumerate() {
        let mut body = Vec::new();
        encode_wire_len(rows.fields().len(), &mut body)?;
        for field in rows.fields() {
            encode_wire_string(field, &mut body)?;
        }
        encode_wire_len(chunk.len(), &mut body)?;
        for row in chunk {
            for value in row {
                encode_wire_value(value, &mut body, 0)?;
            }
        }
        let row_count = u32::try_from(chunk.len()).map_err(|_| "row count exceeds u32")?;
        batches.push(ColumnBatch {
            request: request.clone(),
            fragment_id: fragment_id.clone(),
            sequence: u64::try_from(index)
                .map_err(|_| "batch sequence exceeds u64")?
                .saturating_add(1),
            row_count,
            payload: Some(dtg_execution::cluster_protocol::proto::BoundedPayload {
                format_version: 1,
                declared_len: body.len() as u64,
                item_count: row_count,
                checksum: checksum_bytes(&body).to_vec(),
                body,
            }),
        });
    }
    Ok(batches)
}

#[cfg(test)]
async fn collect_fragment_results_bounded<T, R, E, F, Fut>(
    fragments: Vec<T>,
    concurrency: usize,
    execute: F,
) -> Result<Vec<(usize, R)>, E>
where
    F: Fn(T) -> Fut,
    Fut: Future<Output = Result<R, E>>,
{
    debug_assert!(concurrency > 0);
    let mut results = stream::iter(fragments.into_iter().enumerate())
        .map(move |(ordinal, fragment)| {
            let execution = execute(fragment);
            async move { execution.await.map(|result| (ordinal, result)) }
        })
        .buffer_unordered(concurrency)
        .try_collect::<Vec<_>>()
        .await?;
    results.sort_by_key(|(ordinal, _)| *ordinal);
    Ok(results)
}

fn stream_fragment_results_bounded<T, R, E, F, Fut>(
    fragments: Vec<T>,
    concurrency: usize,
    execute: F,
) -> ReceiverStream<Result<(usize, R), E>>
where
    T: Send + 'static,
    R: Send + 'static,
    E: Send + 'static,
    F: Fn(T) -> Fut + Send + 'static,
    Fut: Future<Output = Result<R, E>> + Send + 'static,
{
    debug_assert!(concurrency > 0);
    let (sender, receiver) = mpsc::channel(concurrency);
    tokio::spawn(async move {
        let mut results = stream::iter(fragments.into_iter().enumerate())
            .map(move |(ordinal, fragment)| {
                let execution = execute(fragment);
                async move { execution.await.map(|result| (ordinal, result)) }
            })
            .buffer_unordered(concurrency);

        while let Some(result) = results.next().await {
            if sender.send(result).await.is_err() {
                return;
            }
        }
    });
    ReceiverStream::new(receiver)
}

#[cfg(test)]
fn stream_gateway_pipeline_results_bounded<T, R, E, F, Fut>(
    requests: Vec<T>,
    concurrency: usize,
    execute: F,
) -> ReceiverStream<Result<(usize, R), E>>
where
    T: Send + 'static,
    R: Send + 'static,
    E: Send + 'static,
    F: Fn(T) -> Fut + Send + 'static,
    Fut: Future<Output = Result<R, E>> + Send + 'static,
{
    stream_fragment_results_bounded(requests, concurrency, execute)
}

fn gateway_query_responses(
    response_context: Option<RequestContext>,
    batches: Vec<ColumnBatch>,
) -> Vec<GatewayWireResponse> {
    if batches.is_empty() {
        return vec![GatewayWireResponse {
            status: Some(TypedStatus {
                request: response_context,
                code: StatusCode::Ok.into(),
                retry: RetryDisposition::Never.into(),
                message: "query completed with zero rows".into(),
                idempotency_key: Vec::new(),
                details: None,
            }),
            batch: None,
        }];
    }
    batches
        .into_iter()
        .map(|batch| GatewayWireResponse {
            status: Some(TypedStatus {
                request: response_context.clone(),
                code: StatusCode::Ok.into(),
                retry: RetryDisposition::Never.into(),
                message: "query fragment executed".into(),
                idempotency_key: Vec::new(),
                details: None,
            }),
            batch: Some(batch),
        })
        .collect()
}

fn gateway_session_frame(
    request: RequestContext,
    responses: Vec<GatewayWireResponse>,
) -> GatewaySessionResponse {
    GatewaySessionResponse {
        request: Some(request),
        responses,
    }
}

fn gateway_session_error(request: RequestContext, error: Status) -> GatewaySessionResponse {
    let (code, retry) = match error.code() {
        tonic::Code::InvalidArgument | tonic::Code::FailedPrecondition => {
            (StatusCode::InvalidRequest, RetryDisposition::Never)
        }
        tonic::Code::Unavailable | tonic::Code::DeadlineExceeded => {
            (StatusCode::Unavailable, RetryDisposition::Safe)
        }
        _ => (StatusCode::Internal, RetryDisposition::Safe),
    };
    gateway_session_frame(
        request.clone(),
        vec![GatewayWireResponse {
            status: Some(TypedStatus {
                request: Some(request),
                code: code.into(),
                retry: retry.into(),
                message: error.message().to_owned(),
                idempotency_key: Vec::new(),
                details: None,
            }),
            batch: None,
        }],
    )
}

fn encode_wire_len(value: usize, output: &mut Vec<u8>) -> Result<(), &'static str> {
    output.extend_from_slice(
        &u32::try_from(value)
            .map_err(|_| "wire collection exceeds u32")?
            .to_be_bytes(),
    );
    Ok(())
}

fn encode_wire_string(value: &str, output: &mut Vec<u8>) -> Result<(), &'static str> {
    encode_wire_len(value.len(), output)?;
    output.extend_from_slice(value.as_bytes());
    Ok(())
}

fn encode_wire_value(
    value: &GatewayValue,
    output: &mut Vec<u8>,
    depth: usize,
) -> Result<(), &'static str> {
    if depth > 64 {
        return Err("wire value nesting exceeds 64");
    }
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
            encode_wire_len(value.len(), output)?;
            output.extend_from_slice(value);
        }
        GatewayValue::String(value) => {
            output.push(5);
            encode_wire_string(value, output)?;
        }
        GatewayValue::List(values) => {
            output.push(6);
            encode_wire_len(values.len(), output)?;
            for value in values {
                encode_wire_value(value, output, depth + 1)?;
            }
        }
        GatewayValue::Map(values) => {
            output.push(7);
            encode_wire_len(values.len(), output)?;
            for (name, value) in values {
                encode_wire_string(name, output)?;
                encode_wire_value(value, output, depth + 1)?;
            }
        }
    }
    Ok(())
}

#[tonic::async_trait]
impl DataService for DataRpcService {
    type ExecuteFragmentStream =
        Pin<Box<dyn Stream<Item = Result<ColumnBatch, Status>> + Send + 'static>>;

    async fn execute_fragment(
        &self,
        request: Request<ExecutionFragment>,
    ) -> Result<Response<Self::ExecuteFragmentStream>, Status> {
        self.begin_request()?;
        let wire = request.into_inner();
        let batches = self.execute_fragment_wire(wire).await?;
        Ok(Response::new(Box::pin(tokio_stream::iter(
            batches.into_iter().map(Ok),
        ))))
    }

    async fn apply_transaction(
        &self,
        request: Request<TransactionRequest>,
    ) -> Result<Response<TypedStatus>, Status> {
        self.begin_request()?;
        let wire = request.into_inner();
        let response_context = wire
            .context
            .as_ref()
            .and_then(|context| context.request.clone());
        let timer = self.request_metrics.start(RequestStage::DataValidation);
        let validation_detail = self
            .request_metrics
            .start_detail(dtg_execution::RequestDetail::DataValidation);
        let validation: Result<_, Status> = (|| {
            let shard_context: ShardRequestContext = wire
                .context
                .clone()
                .ok_or_else(|| self.invalid(ProtocolError::MissingContext))?
                .try_into()
                .map_err(|error| self.invalid(error))?;
            let payload =
                validate_transaction_request(wire.clone()).map_err(|error| self.invalid(error))?;
            let command = dtg_execution::shard::ShardCommand::decode(payload.body())
                .map_err(|error| self.execution_failure(error))?;
            if command.header().placement_epoch() != shard_context.placement_epoch()
                || command.header().backend_generation() != shard_context.backend_generation()
            {
                return Err(self.execution_failure("transaction command and request fence differ"));
            }
            Ok((shard_context, command))
        })();
        let (shard_context, command) =
            validation_detail.finish_result(timer.finish_result(validation))?;
        let snapshot_commit = wire.operation
            == dtg_execution::cluster_protocol::proto::TransactionOperation::CommitSnapshot as i32;
        let timer = self.request_metrics.start(RequestStage::DataRouting);
        let lookup = timer.finish_result(
            self.execution
                .locate_replica_timed(
                    shard_context.request().cluster_id(),
                    shard_context.graph_id(),
                    shard_context.shard_id(),
                    shard_context.placement_epoch(),
                    shard_context.backend_generation(),
                    None,
                )
                .map_err(|error| self.execution_failure(error)),
        )?;
        self.request_metrics.record_detail(
            dtg_execution::RequestDetail::DataRouteLockWait,
            dtg_execution::StageOutcome::Success,
            lookup.lock_wait_nanoseconds(),
        );
        self.request_metrics.record_detail(
            dtg_execution::RequestDetail::DataRouteLookup,
            dtg_execution::StageOutcome::Success,
            lookup.lookup_nanoseconds(),
        );
        let key = lookup.key();
        let (command, commit_time) = if snapshot_commit {
            if let Some(commit_time) = self.existing_snapshot_commit_time(key, &command).await? {
                self.stamp_snapshot_commit_at(command, commit_time)?
            } else {
                self.stamp_snapshot_commit(command)?
            }
        } else {
            (
                command,
                TransactionTime::new(0).expect("zero transaction time is valid"),
            )
        };
        let command_id = command.header().command_id().get();
        let timer = self.request_metrics.start(RequestStage::DataRaftApply);
        let apply = timer.finish_result(self.apply_transaction_batched(key, command).await)?;
        let receipt = apply.receipt;
        if receipt.command_id() != command_id {
            return Err(self.execution_failure("transaction command receipt identifier differs"));
        }
        if receipt.rejection().is_some() {
            return Err(self.execution_failure("transaction command was rejected"));
        }
        let mut body = receipt.index().to_be_bytes().to_vec();
        body.push(u8::from(receipt.replayed()));
        if snapshot_commit {
            body.extend_from_slice(&commit_time.get().to_be_bytes());
        }
        Ok(Response::new(TypedStatus {
            request: response_context,
            code: StatusCode::Ok.into(),
            retry: RetryDisposition::Never.into(),
            message: if snapshot_commit {
                "snapshot-isolated transaction committed by Shard Raft".into()
            } else {
                "transaction command accepted by Shard Raft".into()
            },
            idempotency_key: wire.idempotency_key,
            details: Some(dtg_execution::cluster_protocol::proto::BoundedPayload {
                format_version: 1,
                declared_len: body.len() as u64,
                item_count: 1,
                checksum: checksum_bytes(&body).to_vec(),
                body,
            }),
        }))
    }

    type AcceptSnapshotIngestStream =
        Pin<Box<dyn Stream<Item = Result<SnapshotIngestReceipt, Status>> + Send + 'static>>;

    async fn accept_snapshot_ingest(
        &self,
        request: Request<SnapshotIngestBatch>,
    ) -> Result<Response<Self::AcceptSnapshotIngestStream>, Status> {
        self.begin_request()?;
        let batch = request.into_inner();
        validate_snapshot_ingest_batch(&batch).map_err(|error| self.invalid(error))?;
        let mut replies = Vec::with_capacity(batch.items.len());
        for item in batch.items {
            let admission_started = Instant::now();
            let receipt_id = u128::from_be_bytes(
                item.receipt_id
                    .as_slice()
                    .try_into()
                    .map_err(|_| self.invalid(ProtocolError::IdentifierLength))?,
            );
            let transaction = item
                .transaction
                .ok_or_else(|| self.invalid(ProtocolError::MissingPayload))?;
            let payload_digest = snapshot_ingest_payload_digest(&transaction);
            let newly_admitted = self
                .snapshot_ingest_receipts
                .lock()
                .map_err(|_| {
                    self.execution_failure("snapshot ingest receipt registry is poisoned")
                })?
                .admit(receipt_id, payload_digest)
                .map_err(|error| match error {
                    SnapshotIngestAdmissionError::Capacity => {
                        Status::resource_exhausted("snapshot ingest receipt capacity is exhausted")
                    }
                    SnapshotIngestAdmissionError::ReceiptPayloadMismatch => {
                        Status::invalid_argument(
                            "snapshot ingest receipt ID is already bound to a different payload",
                        )
                    }
                })?;
            if newly_admitted {
                match self.snapshot_ingest_sender.try_send(PendingSnapshotIngest {
                    receipt_id,
                    transaction,
                }) {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        self.snapshot_ingest_receipts
                            .lock()
                            .map_err(|_| {
                                self.execution_failure(
                                    "snapshot ingest receipt registry is poisoned",
                                )
                            })?
                            .remove(receipt_id);
                        return Err(Status::resource_exhausted(
                            "snapshot ingest queue is exhausted; retry safely",
                        ));
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        return Err(self.execution_failure("snapshot ingest worker stopped"));
                    }
                }
            }
            let state = self
                .snapshot_ingest_state(receipt_id)?
                .expect("receipt is admitted before it is returned");
            self.request_metrics.record_detail(
                dtg_execution::RequestDetail::DataSnapshotIngestAdmission,
                dtg_execution::StageOutcome::Success,
                elapsed_nanoseconds(admission_started),
            );
            replies.push(Ok(snapshot_ingest_receipt(receipt_id, state)));
        }
        Ok(Response::new(Box::pin(tokio_stream::iter(replies))))
    }

    async fn get_snapshot_ingest_receipt(
        &self,
        request: Request<SnapshotIngestReceiptRequest>,
    ) -> Result<Response<SnapshotIngestReceiptResponse>, Status> {
        self.begin_request()?;
        let lookup_started = Instant::now();
        let request = request.into_inner();
        validate_snapshot_ingest_receipt_request(&request).map_err(|error| self.invalid(error))?;
        let receipt_id = u128::from_be_bytes(
            request
                .receipt_id
                .as_slice()
                .try_into()
                .map_err(|_| self.invalid(ProtocolError::IdentifierLength))?,
        );
        let Some(state) = self.snapshot_ingest_state(receipt_id)? else {
            self.request_metrics.record_detail(
                dtg_execution::RequestDetail::DataSnapshotIngestReceiptLookup,
                dtg_execution::StageOutcome::Success,
                elapsed_nanoseconds(lookup_started),
            );
            return Ok(Response::new(SnapshotIngestReceiptResponse {
                status: Some(TypedStatus {
                    request: request.request,
                    code: StatusCode::Unavailable.into(),
                    retry: RetryDisposition::Safe.into(),
                    message: "snapshot ingest receipt is unknown; retry with the same receipt ID"
                        .into(),
                    idempotency_key: receipt_id.to_be_bytes().to_vec(),
                    details: None,
                }),
                receipt: None,
            }));
        };
        self.request_metrics.record_detail(
            dtg_execution::RequestDetail::DataSnapshotIngestReceiptLookup,
            dtg_execution::StageOutcome::Success,
            elapsed_nanoseconds(lookup_started),
        );
        Ok(Response::new(SnapshotIngestReceiptResponse {
            status: Some(TypedStatus {
                request: request.request,
                code: StatusCode::Ok.into(),
                retry: RetryDisposition::Never.into(),
                message: "snapshot ingest receipt found".into(),
                idempotency_key: receipt_id.to_be_bytes().to_vec(),
                details: None,
            }),
            receipt: Some(snapshot_ingest_receipt(receipt_id, state)),
        }))
    }

    async fn send_raft(
        &self,
        request: Request<RaftEnvelope>,
    ) -> Result<Response<TypedStatus>, Status> {
        self.begin_request()?;
        let wire = request.into_inner();
        let response_context = wire
            .context
            .as_ref()
            .and_then(|context| context.request.clone());
        let timer = self.request_metrics.start(RequestStage::DataValidation);
        let validation: Result<_, Status> = (|| {
            let shard_context: ShardRequestContext = wire
                .context
                .clone()
                .ok_or_else(|| self.invalid(ProtocolError::MissingContext))?
                .try_into()
                .map_err(|error| self.invalid(error))?;
            let payload =
                validate_raft_envelope(wire.clone()).map_err(|error| self.invalid(error))?;
            let message = raft::eraftpb::Message::decode(payload.body())
                .map_err(|_| Status::invalid_argument("DTG-PROTOCOL-MALFORMED-RAFT"))?;
            let encoded_kind = RaftMessageKind::try_from(wire.kind)
                .map_err(|_| Status::invalid_argument("DTG-PROTOCOL-ENUM"))?;
            let payload_kind = crate::raft_transport::raft_message_kind(message.get_msg_type())
                .map_err(|_| Status::invalid_argument("DTG-PROTOCOL-RAFT-KIND"))?;
            if encoded_kind != payload_kind {
                return Err(Status::invalid_argument("DTG-PROTOCOL-RAFT-KIND"));
            }
            let target = dtg_execution::storage::ReplicaId::new(wire.to_replica_id)
                .map_err(|error| self.execution_failure(error))?;
            Ok((shard_context, payload, target))
        })();
        let (shard_context, payload, target) = timer.finish_result(validation)?;
        let timer = self.request_metrics.start(RequestStage::DataRouting);
        let key = timer.finish_result(
            self.execution
                .locate_replica(
                    shard_context.request().cluster_id(),
                    shard_context.graph_id(),
                    shard_context.shard_id(),
                    shard_context.placement_epoch(),
                    shard_context.backend_generation(),
                    Some(target),
                )
                .map_err(|error| self.execution_failure(error)),
        )?;
        self.execution
            .receive_raft_message(
                key,
                payload.body(),
                dtg_execution::storage::ReplicaId::new(wire.from_replica_id)
                    .map_err(|error| self.execution_failure(error))?,
                target,
                wire.term,
            )
            .map_err(|error| self.execution_failure(error))?;
        Ok(Response::new(TypedStatus {
            request: response_context,
            code: StatusCode::Ok.into(),
            retry: RetryDisposition::Never.into(),
            message: "Raft message accepted".into(),
            idempotency_key: Vec::new(),
            details: None,
        }))
    }

    async fn install_replica_snapshot(
        &self,
        request: Request<LogicalReplicaSnapshot>,
    ) -> Result<Response<TypedStatus>, Status> {
        self.begin_request()?;
        let wire = request.into_inner();
        let response_context = wire
            .context
            .as_ref()
            .and_then(|context| context.request.clone());
        let shard_context: ShardRequestContext = wire
            .context
            .clone()
            .ok_or_else(|| self.invalid(ProtocolError::MissingContext))?
            .try_into()
            .map_err(|error| self.invalid(error))?;
        validate_replica_snapshot(wire.clone()).map_err(|error| self.invalid(error))?;
        self.execution
            .locate_replica(
                shard_context.request().cluster_id(),
                shard_context.graph_id(),
                shard_context.shard_id(),
                shard_context.placement_epoch(),
                shard_context.backend_generation(),
                None,
            )
            .map_err(|error| self.execution_failure(error))?;
        self.state.metrics.record_rpc_failure();
        Ok(Response::new(TypedStatus {
            request: response_context,
            code: StatusCode::Unavailable.into(),
            retry: RetryDisposition::Safe.into(),
            message: "logical snapshot chunk receiver is not available for the active assignment"
                .into(),
            idempotency_key: wire.snapshot_id,
            details: None,
        }))
    }
}

#[tonic::async_trait]
impl GatewayService for DataRpcService {
    type ExecuteStream =
        Pin<Box<dyn Stream<Item = Result<GatewayWireResponse, Status>> + Send + 'static>>;
    type ExecuteSessionStream =
        Pin<Box<dyn Stream<Item = Result<GatewaySessionResponse, Status>> + Send + 'static>>;
    type ExecutePipelinedStream =
        Pin<Box<dyn Stream<Item = Result<GatewayPipelineServerFrame, Status>> + Send + 'static>>;

    async fn execute(
        &self,
        request: Request<GatewayRequest>,
    ) -> Result<Response<Self::ExecuteStream>, Status> {
        self.begin_request()?;
        let wire = request.into_inner();
        let response_context = wire.request.clone();
        let validated =
            validate_gateway_request(wire.clone()).map_err(|error| self.invalid(error))?;
        if validated.execution().body().first() != Some(&1) || wire.fragments.is_empty() {
            return Err(Status::failed_precondition(
                "Data GatewayService currently accepts only planned query fragments",
            ));
        }
        if wire.fragments.len() == 1 {
            let fragment = wire
                .fragments
                .into_iter()
                .next()
                .expect("fragment count was checked");
            let batches = self.execute_fragment_wire(fragment).await?;
            let responses = gateway_query_responses(response_context, batches);
            return Ok(Response::new(Box::pin(tokio_stream::iter(
                responses.into_iter().map(Ok),
            ))));
        }
        let service = self.clone();
        let mut fragment_results = stream_fragment_results_bounded(
            wire.fragments,
            MAX_GATEWAY_FRAGMENT_CONCURRENCY,
            move |fragment| {
                let service = service.clone();
                async move { service.execute_fragment_wire(fragment).await }
            },
        );
        let (sender, receiver) = mpsc::channel(MAX_GATEWAY_FRAGMENT_CONCURRENCY);
        tokio::spawn(async move {
            let mut emitted_batch = false;
            while let Some(result) = fragment_results.next().await {
                match result {
                    Ok((_, batches)) => {
                        for batch in batches {
                            emitted_batch = true;
                            let response = GatewayWireResponse {
                                status: Some(TypedStatus {
                                    request: response_context.clone(),
                                    code: StatusCode::Ok.into(),
                                    retry: RetryDisposition::Never.into(),
                                    message: "query fragment executed".into(),
                                    idempotency_key: Vec::new(),
                                    details: None,
                                }),
                                batch: Some(batch),
                            };
                            if sender.send(Ok(response)).await.is_err() {
                                return;
                            }
                        }
                    }
                    Err(status) => {
                        let _ = sender.send(Err(status)).await;
                        return;
                    }
                }
            }
            if !emitted_batch {
                let _ = sender
                    .send(Ok(GatewayWireResponse {
                        status: Some(TypedStatus {
                            request: response_context,
                            code: StatusCode::Ok.into(),
                            retry: RetryDisposition::Never.into(),
                            message: "query completed with zero rows".into(),
                            idempotency_key: Vec::new(),
                            details: None,
                        }),
                        batch: None,
                    }))
                    .await;
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(receiver))))
    }

    async fn execute_session(
        &self,
        request: Request<Streaming<GatewayRequest>>,
    ) -> Result<Response<Self::ExecuteSessionStream>, Status> {
        let mut requests = request.into_inner();
        let service = self.clone();
        let (sender, receiver) = mpsc::channel(MAX_GATEWAY_SESSION_IN_FLIGHT);
        tokio::spawn(async move {
            let permits = Arc::new(Semaphore::new(MAX_GATEWAY_SESSION_IN_FLIGHT));
            let mut tasks = tokio::task::JoinSet::new();
            let mut input_open = true;
            loop {
                if input_open {
                    tokio::select! {
                        request = requests.message() => match request {
                            Ok(Some(request)) => {
                                let permit = match Arc::clone(&permits).acquire_owned().await {
                                    Ok(permit) => permit,
                                    Err(_) => return,
                                };
                                let service = service.clone();
                                tasks.spawn(async move {
                                    let _permit = permit;
                                    execute_gateway_session_request(service, request).await
                                });
                            }
                            Ok(None) => input_open = false,
                            Err(error) => {
                                let _ = sender.send(Err(Status::invalid_argument(error.to_string()))).await;
                                return;
                            }
                        },
                        completed = tasks.join_next(), if !tasks.is_empty() => {
                            if let Some(result) = completed {
                                match result {
                                    Ok(frame) => {
                                        if sender.send(Ok(frame)).await.is_err() {
                                            return;
                                        }
                                    }
                                    Err(error) => {
                                        let _ = sender.send(Err(Status::internal(error.to_string()))).await;
                                        return;
                                    }
                                }
                            }
                        }
                    }
                } else {
                    let Some(result) = tasks.join_next().await else {
                        return;
                    };
                    match result {
                        Ok(frame) => {
                            if sender.send(Ok(frame)).await.is_err() {
                                return;
                            }
                        }
                        Err(error) => {
                            let _ = sender.send(Err(Status::internal(error.to_string()))).await;
                            return;
                        }
                    }
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(receiver))))
    }

    async fn execute_pipelined(
        &self,
        request: Request<Streaming<GatewayPipelineClientFrame>>,
    ) -> Result<Response<Self::ExecutePipelinedStream>, Status> {
        let mut requests = request.into_inner();
        let service = self.clone();
        let (sender, receiver) = mpsc::channel(MAX_GATEWAY_PIPELINE_IN_FLIGHT);
        tokio::spawn(async move {
            if sender
                .send(Ok(GatewayPipelineServerFrame {
                    payload: Some(
                        dtg_execution::cluster_protocol::proto::gateway_pipeline_server_frame::Payload::Credit(
                            GatewayPipelineCredit {
                                available_requests: MAX_GATEWAY_PIPELINE_IN_FLIGHT as u32,
                            },
                        ),
                    ),
                    returned_credits: 0,
                    emitted_unix_ns: 0,
                }))
                .await
                .is_err()
            {
                return;
            }
            let permits = Arc::new(Semaphore::new(MAX_GATEWAY_PIPELINE_IN_FLIGHT));
            let execution_permits = Arc::clone(&service.pipeline_execution_permits);
            let mut tasks: JoinSet<(u128, GatewaySessionResponse)> = JoinSet::new();
            let mut abort_handles = BTreeMap::<u128, AbortHandle>::new();
            let mut cancelled_before_start = BTreeMap::<u128, RequestContext>::new();
            let mut response_batches_enabled = false;
            let mut input_open = true;
            loop {
                if input_open {
                    tokio::select! {
                        frame = requests.message() => match frame {
                            Ok(Some(frame)) => {
                                let received_at = Instant::now();
                                if process_gateway_pipeline_frame(
                                    &service,
                                    frame,
                                    received_at,
                                    &mut response_batches_enabled,
                                    &permits,
                                    &execution_permits,
                                    &mut tasks,
                                    &mut abort_handles,
                                    &mut cancelled_before_start,
                                    &sender,
                                ).await.is_err() {
                                    return;
                                }
                            }
                            Ok(None) => input_open = false,
                            Err(error) => {
                                let _ = sender.send(Err(Status::invalid_argument(error.to_string()))).await;
                                return;
                            }
                        },
                        completed = tasks.join_next(), if !tasks.is_empty() => {
                            if !emit_gateway_pipeline_completion(
                                &sender,
                                &service.request_metrics,
                                &mut tasks,
                                &mut abort_handles,
                                completed,
                                response_batches_enabled,
                            ).await {
                                return;
                            }
                        }
                    }
                } else {
                    let Some(completed) = tasks.join_next().await else {
                        return;
                    };
                    if !emit_gateway_pipeline_completion(
                        &sender,
                        &service.request_metrics,
                        &mut tasks,
                        &mut abort_handles,
                        Some(completed),
                        response_batches_enabled,
                    )
                    .await
                    {
                        return;
                    }
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(receiver))))
    }
}

async fn process_gateway_pipeline_frame(
    service: &DataRpcService,
    frame: GatewayPipelineClientFrame,
    received_at: Instant,
    response_batches_enabled: &mut bool,
    permits: &Arc<Semaphore>,
    execution_permits: &Arc<Semaphore>,
    tasks: &mut JoinSet<(u128, GatewaySessionResponse)>,
    abort_handles: &mut BTreeMap<u128, AbortHandle>,
    cancelled_before_start: &mut BTreeMap<u128, RequestContext>,
    sender: &mpsc::Sender<Result<GatewayPipelineServerFrame, Status>>,
) -> Result<(), ()> {
    let request_sent_unix_ns = frame.sent_unix_ns;
    match frame.payload {
        Some(
            dtg_execution::cluster_protocol::proto::gateway_pipeline_client_frame::Payload::Batch(
                batch,
            ),
        ) => {
            *response_batches_enabled |= batch.accepts_response_batches;
            let batch_too_large = batch.requests.len() > MAX_GATEWAY_PIPELINE_BATCH_REQUESTS
                || batch
                    .requests
                    .iter()
                    .map(gateway_pipeline_wire_size)
                    .sum::<usize>()
                    > MAX_GATEWAY_PIPELINE_BATCH_BYTES;
            for wire in batch.requests {
                let Some(request) = wire.request.clone() else {
                    return Err(());
                };
                let request_id = match pipeline_request_id(Some(&request)) {
                    Ok(request_id) => request_id,
                    Err(error) => {
                        if !send_gateway_pipeline_terminal(
                            sender,
                            gateway_pipeline_error_frame(
                                request,
                                StatusCode::InvalidRequest,
                                RetryDisposition::Never,
                                error,
                            ),
                        )
                        .await
                        {
                            return Err(());
                        }
                        continue;
                    }
                };
                if cancelled_before_start.remove(&request_id).is_some() {
                    if !send_gateway_pipeline_terminal(
                        sender,
                        gateway_pipeline_error_frame(
                            request,
                            StatusCode::Cancelled,
                            RetryDisposition::Never,
                            "Gateway pipeline request was cancelled",
                        ),
                    )
                    .await
                    {
                        return Err(());
                    }
                    continue;
                }
                if batch_too_large {
                    if !send_gateway_pipeline_terminal(
                        sender,
                        gateway_pipeline_error_frame(
                            request,
                            StatusCode::ResourceExhausted,
                            RetryDisposition::Safe,
                            "Gateway pipeline batch exceeds its request or byte limit",
                        ),
                    )
                    .await
                    {
                        return Err(());
                    }
                    continue;
                }
                if abort_handles.contains_key(&request_id) {
                    if !send_gateway_pipeline_terminal(
                        sender,
                        gateway_pipeline_error_frame(
                            request,
                            StatusCode::InvalidRequest,
                            RetryDisposition::Never,
                            "Gateway pipeline request ID is already in flight",
                        ),
                    )
                    .await
                    {
                        return Err(());
                    }
                    continue;
                }
                let permit = match Arc::clone(permits).try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(_) => {
                        if !send_gateway_pipeline_terminal(
                            sender,
                            gateway_pipeline_error_frame(
                                request,
                                StatusCode::ResourceExhausted,
                                RetryDisposition::Safe,
                                "Data pipeline execution credits are exhausted",
                            ),
                        )
                        .await
                        {
                            return Err(());
                        }
                        continue;
                    }
                };
                if let Some(transport_wait) = elapsed_since_unix_timestamp(request_sent_unix_ns) {
                    service.request_metrics.record_detail(
                        dtg_execution::RequestDetail::DataGatewayPipelineRequestTransportWait,
                        dtg_execution::StageOutcome::Success,
                        transport_wait,
                    );
                }
                let service = service.clone();
                let execution_permits = Arc::clone(execution_permits);
                let handle = tasks.spawn(async move {
                    let _permit = permit;
                    let _execution_permit =
                        acquire_gateway_pipeline_execution_permit(execution_permits).await;
                    service.request_metrics.record_detail(
                        dtg_execution::RequestDetail::DataGatewayPipelineDispatchWait,
                        dtg_execution::StageOutcome::Success,
                        elapsed_nanoseconds(received_at),
                    );
                    (
                        request_id,
                        execute_gateway_session_request(service, wire).await,
                    )
                });
                abort_handles.insert(request_id, handle);
            }
        }
        Some(
            dtg_execution::cluster_protocol::proto::gateway_pipeline_client_frame::Payload::Cancel(
                cancel,
            ),
        ) => {
            let Some(request) = cancel.request else {
                return Err(());
            };
            let request_id = pipeline_request_id(Some(&request)).map_err(|_| ())?;
            if let Some(handle) = abort_handles.remove(&request_id) {
                handle.abort();
                if !send_gateway_pipeline_terminal(
                    sender,
                    gateway_pipeline_error_frame(
                        request,
                        StatusCode::Cancelled,
                        RetryDisposition::Never,
                        "Gateway pipeline request was cancelled",
                    ),
                )
                .await
                {
                    return Err(());
                }
            } else if cancelled_before_start.len() < MAX_GATEWAY_PIPELINE_IN_FLIGHT {
                cancelled_before_start.insert(request_id, request);
            } else {
                return Err(());
            }
        }
        None => return Err(()),
    }
    Ok(())
}

async fn acquire_gateway_pipeline_execution_permit(
    permits: Arc<Semaphore>,
) -> OwnedSemaphorePermit {
    permits
        .acquire_owned()
        .await
        .expect("process-wide pipeline execution semaphore is never closed")
}

async fn emit_gateway_pipeline_completion(
    sender: &mpsc::Sender<Result<GatewayPipelineServerFrame, Status>>,
    request_metrics: &Arc<RequestStageMetrics>,
    tasks: &mut JoinSet<(u128, GatewaySessionResponse)>,
    abort_handles: &mut BTreeMap<u128, AbortHandle>,
    completed: Option<Result<(u128, GatewaySessionResponse), tokio::task::JoinError>>,
    response_batches_enabled: bool,
) -> bool {
    let Some(completed) = completed else {
        return true;
    };
    let mut responses = Vec::with_capacity(MAX_GATEWAY_PIPELINE_RESPONSE_BATCH_RESPONSES);
    match take_gateway_pipeline_completion(abort_handles, completed) {
        Ok(Some(response)) => responses.push(response),
        Ok(None) => return true,
        Err(error) => {
            let _ = sender.send(Err(Status::internal(error.to_string()))).await;
            return false;
        }
    }
    if response_batches_enabled {
        while responses.len() < MAX_GATEWAY_PIPELINE_RESPONSE_BATCH_RESPONSES {
            let Some(completed) = tasks.try_join_next() else {
                break;
            };
            match take_gateway_pipeline_completion(abort_handles, completed) {
                Ok(Some(response)) => responses.push(response),
                Ok(None) => {}
                Err(error) => {
                    let _ = sender.send(Err(Status::internal(error.to_string()))).await;
                    return false;
                }
            }
        }
    }
    send_gateway_pipeline_completions(sender, request_metrics, responses).await
}

fn take_gateway_pipeline_completion(
    abort_handles: &mut BTreeMap<u128, AbortHandle>,
    completed: Result<(u128, GatewaySessionResponse), tokio::task::JoinError>,
) -> Result<Option<GatewaySessionResponse>, tokio::task::JoinError> {
    match completed {
        Ok((request_id, response)) => {
            // Cancellation emits the only terminal response and removes the
            // abort handle. A task that won the completion race must therefore
            // be discarded rather than returning a second response/credit.
            Ok(abort_handles.remove(&request_id).map(|_| response))
        }
        Err(error) if error.is_cancelled() => Ok(None),
        Err(error) => Err(error),
    }
}

async fn send_gateway_pipeline_completions(
    sender: &mpsc::Sender<Result<GatewayPipelineServerFrame, Status>>,
    request_metrics: &Arc<RequestStageMetrics>,
    responses: Vec<GatewaySessionResponse>,
) -> bool {
    let response_count = responses.len();
    debug_assert_ne!(response_count, 0);
    request_metrics.record_detail(
        dtg_execution::RequestDetail::DataGatewayPipelineCompletionFrame,
        dtg_execution::StageOutcome::Success,
        0,
    );
    let batch_bytes = responses
        .iter()
        .map(prost_014::Message::encoded_len)
        .sum::<usize>();
    if response_count > 1 && batch_bytes <= MAX_GATEWAY_PIPELINE_RESPONSE_BATCH_BYTES {
        let sent = send_gateway_pipeline_completion_frame(
            sender,
            request_metrics,
            GatewayPipelineServerFrame {
                payload: Some(
                    dtg_execution::cluster_protocol::proto::gateway_pipeline_server_frame::Payload::ResponseBatch(
                        dtg_execution::cluster_protocol::proto::GatewayPipelineResponseBatch {
                            responses,
                        },
                    ),
                ),
                returned_credits: response_count as u32,
                emitted_unix_ns: 0,
            },
            response_count,
        )
        .await;
        return sent;
    }
    for response in responses {
        if !send_gateway_pipeline_completion_frame(
            sender,
            request_metrics,
            GatewayPipelineServerFrame {
                payload: Some(
                    dtg_execution::cluster_protocol::proto::gateway_pipeline_server_frame::Payload::Response(
                        response,
                    ),
                ),
                returned_credits: 1,
                emitted_unix_ns: 0,
            },
            1,
        )
        .await
        {
            return false;
        }
    }
    true
}

async fn send_gateway_pipeline_completion_frame(
    sender: &mpsc::Sender<Result<GatewayPipelineServerFrame, Status>>,
    request_metrics: &Arc<RequestStageMetrics>,
    mut frame: GatewayPipelineServerFrame,
    response_count: usize,
) -> bool {
    frame.emitted_unix_ns = unix_timestamp_nanoseconds();
    let send_started = Instant::now();
    let sent = send_gateway_pipeline_terminal(sender, frame).await;
    let outcome = if sent {
        dtg_execution::StageOutcome::Success
    } else {
        dtg_execution::StageOutcome::Error
    };
    let elapsed = elapsed_nanoseconds(send_started);
    for _ in 0..response_count {
        request_metrics.record_detail(
            dtg_execution::RequestDetail::DataGatewayPipelineCompletionSendWait,
            outcome,
            elapsed,
        );
    }
    sent
}

fn unix_timestamp_nanoseconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| u64::try_from(duration.as_nanos()).ok())
        .unwrap_or(0)
}

fn elapsed_since_unix_timestamp(sent_unix_ns: u64) -> Option<u64> {
    if sent_unix_ns == 0 {
        return None;
    }
    unix_timestamp_nanoseconds().checked_sub(sent_unix_ns)
}

async fn send_gateway_pipeline_terminal(
    sender: &mpsc::Sender<Result<GatewayPipelineServerFrame, Status>>,
    response: GatewayPipelineServerFrame,
) -> bool {
    sender.send(Ok(response)).await.is_ok()
}

fn gateway_pipeline_error_frame(
    request: RequestContext,
    code: StatusCode,
    retry: RetryDisposition,
    message: impl Into<String>,
) -> GatewayPipelineServerFrame {
    GatewayPipelineServerFrame {
        payload: Some(
            dtg_execution::cluster_protocol::proto::gateway_pipeline_server_frame::Payload::Response(
                GatewaySessionResponse {
                    request: Some(request.clone()),
                    responses: vec![GatewayWireResponse {
                        status: Some(TypedStatus {
                            request: Some(request),
                            code: code.into(),
                            retry: retry.into(),
                            message: message.into(),
                            idempotency_key: Vec::new(),
                            details: None,
                        }),
                        batch: None,
                    }],
                },
            ),
        ),
        returned_credits: 1,
        emitted_unix_ns: 0,
    }
}

fn pipeline_request_id(request: Option<&RequestContext>) -> Result<u128, &'static str> {
    let request = request.ok_or("Gateway pipeline request omitted request identity")?;
    let bytes: [u8; 16] = request
        .request_id
        .as_slice()
        .try_into()
        .map_err(|_| "Gateway pipeline request identity must contain 16 bytes")?;
    let request_id = u128::from_be_bytes(bytes);
    if request_id == 0 {
        return Err("Gateway pipeline request identity must be non-zero");
    }
    Ok(request_id)
}

fn gateway_pipeline_wire_size(request: &GatewayRequest) -> usize {
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

async fn execute_gateway_session_request(
    service: DataRpcService,
    wire: GatewayRequest,
) -> GatewaySessionResponse {
    let Some(request) = wire.request.clone() else {
        return GatewaySessionResponse {
            request: None,
            responses: Vec::new(),
        };
    };
    if let Err(error) = service.begin_request() {
        return gateway_session_error(request, error);
    }
    let timer = service
        .request_metrics
        .start_detail(dtg_execution::RequestDetail::DataGatewaySessionExecution);
    let result = async {
        let validated =
            validate_gateway_request(wire.clone()).map_err(|error| service.invalid(error))?;
        if validated.execution().body().first() != Some(&1) || wire.fragments.len() != 1 {
            return Err(Status::failed_precondition(
                "Data Gateway session accepts only one planned query fragment per request",
            ));
        }
        let fragment = wire
            .fragments
            .into_iter()
            .next()
            .expect("fragment count was checked");
        let batches = service.execute_fragment_wire(fragment).await?;
        Ok(gateway_session_frame(
            request.clone(),
            gateway_query_responses(Some(request.clone()), batches),
        ))
    }
    .await;
    match result {
        Ok(frame) => {
            timer.finish(dtg_execution::StageOutcome::Success);
            frame
        }
        Err(error) => {
            timer.finish(dtg_execution::StageOutcome::Error);
            gateway_session_error(request, error)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::time::Duration;

    use dtg_execution::RequestStageMetrics;
    use futures_util::StreamExt as _;
    use tokio::sync::{Barrier, Semaphore, mpsc};
    use tokio::task::JoinSet;

    use super::{
        ColumnBatch, GatewaySessionResponse, PROTOCOL_MAJOR, RequestContext, StatusCode,
        acquire_gateway_pipeline_execution_permit, collect_fragment_results_bounded,
        emit_gateway_pipeline_completion, gateway_pipeline_execution_limit,
        gateway_query_responses, gateway_session_frame, stream_fragment_results_bounded,
        stream_gateway_pipeline_results_bounded,
    };

    #[test]
    fn single_fragment_responses_preserve_batches_and_emit_an_empty_completion() {
        let batch = ColumnBatch {
            request: None,
            fragment_id: 7_u128.to_be_bytes().to_vec(),
            sequence: 1,
            row_count: 0,
            payload: None,
        };

        let responses = gateway_query_responses(None, vec![batch]);
        assert_eq!(responses.len(), 1);
        assert_eq!(
            responses[0].status.as_ref().unwrap().code,
            StatusCode::Ok as i32
        );
        assert_eq!(
            responses[0].batch.as_ref().unwrap().fragment_id,
            7_u128.to_be_bytes()
        );

        let empty = gateway_query_responses(None, Vec::new());
        assert_eq!(empty.len(), 1);
        assert_eq!(
            empty[0].status.as_ref().unwrap().code,
            StatusCode::Ok as i32
        );
        assert!(empty[0].batch.is_none());
    }

    #[test]
    fn gateway_session_frame_preserves_the_request_identity_and_batches() {
        let request = RequestContext {
            protocol_major: PROTOCOL_MAJOR,
            protocol_minor: 1,
            cluster_id: 7_u64.to_be_bytes().to_vec(),
            request_id: 11_u128.to_be_bytes().to_vec(),
            deadline_unix_ms: 1,
            trace_context: Vec::new(),
        };
        let batch = ColumnBatch {
            request: Some(request.clone()),
            fragment_id: 7_u128.to_be_bytes().to_vec(),
            sequence: 1,
            row_count: 0,
            payload: None,
        };
        let responses = gateway_query_responses(Some(request.clone()), vec![batch]);

        let frame = gateway_session_frame(request.clone(), responses);
        assert_eq!(frame.request, Some(request));
        assert_eq!(frame.responses.len(), 1);
        assert!(frame.responses[0].batch.is_some());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn bounded_fragment_collection_starts_ready_work_concurrently() {
        let barrier = Arc::new(Barrier::new(2));
        let results = tokio::time::timeout(
            Duration::from_millis(100),
            collect_fragment_results_bounded(vec![1_u8, 2], 2, {
                let barrier = Arc::clone(&barrier);
                move |value| {
                    let barrier = Arc::clone(&barrier);
                    async move {
                        barrier.wait().await;
                        Ok::<_, ()>(value)
                    }
                }
            }),
        )
        .await
        .expect("both fragment tasks must start before either completes")
        .unwrap();

        assert_eq!(results, vec![(0, 1), (1, 2)]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn bounded_fragment_stream_emits_ready_work_without_waiting_for_slow_work() {
        let mut results = stream_fragment_results_bounded(vec![1_u8, 2], 2, |value| async move {
            if value == 1 {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Ok::<_, ()>(value)
        });

        let first = tokio::time::timeout(Duration::from_millis(50), results.next())
            .await
            .expect("ready fragment was delayed behind slow work")
            .expect("stream ended before emitting the ready fragment")
            .unwrap();
        assert_eq!(first, (1, 2));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pipeline_stream_emits_a_fast_request_before_a_slow_batch_peer() {
        let mut results =
            stream_gateway_pipeline_results_bounded(vec![1_u8, 2], 2, |request_id| async move {
                if request_id == 1 {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Ok::<_, ()>(request_id)
            });

        let first = tokio::time::timeout(Duration::from_millis(50), results.next())
            .await
            .expect("fast pipeline request was delayed behind slow peer")
            .expect("pipeline stream ended before emitting the fast request")
            .unwrap();
        assert_eq!(first, (1, 2));
    }

    #[tokio::test]
    async fn shared_pipeline_execution_limit_applies_across_streams() {
        let permits = Arc::new(Semaphore::new(1));
        let first = acquire_gateway_pipeline_execution_permit(Arc::clone(&permits)).await;
        let second = tokio::spawn({
            let permits = Arc::clone(&permits);
            async move { acquire_gateway_pipeline_execution_permit(permits).await }
        });

        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut std::pin::pin!(second))
                .await
                .is_err(),
            "a second pipeline stream must share the process execution budget"
        );
        drop(first);
    }

    #[test]
    fn configured_pipeline_execution_limit_is_bounded_and_defaults_safely() {
        assert_eq!(gateway_pipeline_execution_limit(None), 32);
        assert_eq!(gateway_pipeline_execution_limit(Some("64")), 64);
        assert_eq!(gateway_pipeline_execution_limit(Some("0")), 32);
        assert_eq!(gateway_pipeline_execution_limit(Some("129")), 32);
        assert_eq!(gateway_pipeline_execution_limit(Some("not-a-number")), 32);
    }

    #[tokio::test]
    async fn cancelled_pipeline_request_does_not_emit_a_second_terminal_response() {
        let (sender, mut receiver) = mpsc::channel(1);
        let metrics = Arc::new(RequestStageMetrics::default());
        let mut tasks = JoinSet::new();
        tasks.spawn(async { (7_u128, GatewaySessionResponse::default()) });
        let completed = tasks.join_next().await;

        assert!(
            emit_gateway_pipeline_completion(
                &sender,
                &metrics,
                &mut tasks,
                &mut BTreeMap::new(),
                completed,
                false,
            )
            .await
        );
        assert!(receiver.try_recv().is_err());
    }

    #[tokio::test]
    async fn pipeline_completion_records_the_output_queue_wait() {
        let (sender, mut receiver) = mpsc::channel(1);
        let metrics = Arc::new(RequestStageMetrics::default());
        let mut tasks = JoinSet::new();
        let handle = tasks.spawn(async { (7_u128, GatewaySessionResponse::default()) });
        let mut abort_handles = BTreeMap::new();
        abort_handles.insert(7, handle);
        let completed = tasks.join_next().await;

        assert!(
            emit_gateway_pipeline_completion(
                &sender,
                &metrics,
                &mut tasks,
                &mut abort_handles,
                completed,
                false,
            )
            .await
        );
        assert!(receiver.try_recv().is_ok());
        assert_eq!(
            metrics.snapshot().details
                [dtg_execution::RequestDetail::DataGatewayPipelineCompletionSendWait as usize]
                .success,
            1
        );
    }

    #[tokio::test]
    async fn pipeline_completion_batches_ready_siblings_when_the_client_opted_in() {
        let (sender, mut receiver) = mpsc::channel(1);
        let metrics = Arc::new(RequestStageMetrics::default());
        let mut tasks = JoinSet::new();
        let first = tasks.spawn(async { (7_u128, GatewaySessionResponse::default()) });
        let second = tasks.spawn(async { (8_u128, GatewaySessionResponse::default()) });
        let mut abort_handles = BTreeMap::new();
        abort_handles.insert(7, first);
        abort_handles.insert(8, second);
        tokio::task::yield_now().await;
        let completed = tasks.join_next().await;

        assert!(
            emit_gateway_pipeline_completion(
                &sender,
                &metrics,
                &mut tasks,
                &mut abort_handles,
                completed,
                true,
            )
            .await
        );
        let frame = receiver.try_recv().expect("batched completion frame");
        let frame = frame.expect("completion stream status");
        assert_eq!(frame.returned_credits, 2);
        let Some(
            dtg_execution::cluster_protocol::proto::gateway_pipeline_server_frame::Payload::ResponseBatch(batch),
        ) = frame.payload
        else {
            panic!("ready completions must be emitted in a batch");
        };
        assert_eq!(batch.responses.len(), 2);
    }
}
