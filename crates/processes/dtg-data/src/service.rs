use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::time::Duration;

use dtg_execution::cluster_protocol::proto::data_service_server::DataService;
use dtg_execution::cluster_protocol::proto::gateway_service_server::GatewayService;
use dtg_execution::cluster_protocol::proto::{
    ColumnBatch, ExecutionFragment, GatewayRequest, GatewayResponse as GatewayWireResponse,
    GatewaySessionResponse, LogicalReplicaSnapshot, RaftEnvelope, RaftMessageKind, RequestContext,
    RetryDisposition, StatusCode, TransactionRequest, TypedStatus,
};
use dtg_execution::cluster_protocol::{
    PROTOCOL_MAJOR, ProtocolError, ShardRequestContext, checksum_bytes,
    validate_execution_fragment, validate_gateway_request, validate_raft_envelope,
    validate_replica_snapshot, validate_transaction_request,
};
use dtg_execution::shard::{ProposalReceipt, ReplicaKey, ShardCommand};
use dtg_execution::storage::{
    BackendClass, BindingRole, CapabilityManifest, ConsensusStore, StorageError,
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
use tokio::sync::{Semaphore, mpsc, oneshot};
use tokio::time::{Instant, timeout_at};
use tokio_stream::{Stream, wrappers::ReceiverStream};
use tonic::{Request, Response, Status, Streaming};

use crate::{DataProcessConfig, FjallResolver, KuzuResolver, PostgresResolver, RemoteResolver};

const APPLY_BATCH_WINDOW: Duration = Duration::from_micros(250);
const APPLY_BATCH_MAX_COMMANDS: usize = 64;
const MAX_GATEWAY_FRAGMENT_CONCURRENCY: usize = 32;
const MAX_GATEWAY_SESSION_IN_FLIGHT: usize = 32;

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
}

impl DataNodeBuilder {
    pub fn new(consensus_root: impl AsRef<Path>) -> Self {
        Self {
            consensus_root: consensus_root.as_ref().to_path_buf(),
            execution: DataExecution::builder(),
            assignments: Vec::new(),
            raft_transport: Arc::new(RejectingRaftTransport),
        }
    }

    pub fn from_config(config: DataProcessConfig) -> Self {
        let mut builder = Self::new(config.consensus_root())
            .with_provider(
                ProviderKind::Fjall,
                Arc::new(FjallResolver::new(config.fjall_root())),
            )
            .with_provider(
                ProviderKind::PostgreSql,
                Arc::new(PostgresResolver::from_config(&config)),
            )
            .with_provider(
                ProviderKind::Kuzu,
                Arc::new(KuzuResolver::new(config.kuzu_root())),
            );
        for name in config.remote_providers() {
            builder = builder.with_provider(
                ProviderKind::Remote(name.clone()),
                Arc::new(RemoteResolver::from_config(name, &config)),
            );
        }
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
        self.execution = self.execution.with_provider(kind, resolver);
        self
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
        std::fs::create_dir_all(&self.consensus_root).map_err(|error| {
            DataNodeError::Build(format!("cannot create Fjall consensus root: {error}"))
        })?;
        let request_metrics = Arc::new(RequestStageMetrics::default());
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
        for binding in self.assignments {
            match add_assignment(&execution, &self.consensus_root, binding.clone()).await {
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

        Ok(DataNode {
            execution,
            consensus_root: self.consensus_root,
            observed,
            failures,
            state,
            request_metrics,
            apply_batchers,
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
            Ok(Err(error)) => vec![Err(error.to_string()); batch.len()],
            Err(error) => vec![Err(format!("Raft batch worker failed: {error}")); batch.len()],
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
        let admission_started = Instant::now();
        let permit = sender
            .reserve()
            .await
            .map_err(|_| self.execution_failure("Raft apply batch worker stopped"))?;
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
        let (shard_context, payload) = timer.finish_result(validation)?;
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
        let rows = timer.finish_result(
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
        let (shard_context, command) = timer.finish_result(validation)?;
        let command_id = command.header().command_id().get();
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
        Ok(Response::new(TypedStatus {
            request: response_context,
            code: StatusCode::Ok.into(),
            retry: RetryDisposition::Never.into(),
            message: "transaction command accepted by Shard Raft".into(),
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
        Ok(frame) => frame,
        Err(error) => gateway_session_error(request, error),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use futures_util::StreamExt as _;
    use tokio::sync::Barrier;

    use super::{
        ColumnBatch, PROTOCOL_MAJOR, RequestContext, StatusCode, collect_fragment_results_bounded,
        gateway_query_responses, gateway_session_frame, stream_fragment_results_bounded,
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
}
