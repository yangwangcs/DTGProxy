use std::fmt;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};

use dtg_execution::cluster_protocol::proto::data_service_server::DataService;
use dtg_execution::cluster_protocol::proto::{
    ColumnBatch, ExecutionFragment, LogicalReplicaSnapshot, RaftEnvelope, TransactionRequest,
    TypedStatus,
};
use dtg_execution::cluster_protocol::{
    PROTOCOL_MAJOR, ProtocolError, validate_execution_fragment, validate_raft_envelope,
    validate_replica_snapshot, validate_transaction_request,
};
use dtg_execution::storage::{BackendClass, BindingRole, CapabilityManifest, StorageError};
use dtg_execution::{
    DataExecution, DataExecutionBuilder, ProviderKind, ProviderResolver, ReplicaBinding,
};
use dtg_storage_fjall::FjallConsensusStore;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use crate::{DataProcessConfig, FjallResolver, Neo4jResolver, PostgresResolver, RemoteResolver};

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

    fn record_hosted(&self) {
        self.counters
            .hosted_replicas
            .fetch_add(1, Ordering::Relaxed);
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
}

impl DataNodeBuilder {
    pub fn new(consensus_root: impl AsRef<Path>) -> Self {
        Self {
            consensus_root: consensus_root.as_ref().to_path_buf(),
            execution: DataExecution::builder(),
            assignments: Vec::new(),
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
                ProviderKind::Neo4j,
                Arc::new(Neo4jResolver::from_config(&config)),
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

    pub async fn start(self) -> Result<DataNode, DataNodeError> {
        std::fs::create_dir_all(&self.consensus_root).map_err(|error| {
            DataNodeError::Build(format!("cannot create Fjall consensus root: {error}"))
        })?;
        let mut execution = self
            .execution
            .build()
            .map_err(|error| DataNodeError::Build(error.to_string()))?;
        let state = Arc::new(ProcessState::new());
        let mut observed = Vec::new();
        let mut failures = Vec::new();

        for binding in self.assignments {
            let state_store = match execution.open_store(binding.clone()).await {
                Ok(store) => store,
                Err(error) => {
                    state.metrics.record_replica_failure();
                    failures.push(ReplicaFailure::new(binding, error.to_string()));
                    continue;
                }
            };
            let consensus_binding = match consensus_binding(&binding) {
                Ok(binding) => binding,
                Err(error) => {
                    state.metrics.record_replica_failure();
                    failures.push(ReplicaFailure::new(binding, error.to_string()));
                    continue;
                }
            };
            let consensus_path = self
                .consensus_root
                .join(consensus_binding.namespace_id().as_str());
            let consensus_store = match FjallConsensusStore::open(consensus_path, consensus_binding)
            {
                Ok(store) => Arc::new(store),
                Err(error) => {
                    state.metrics.record_replica_failure();
                    failures.push(ReplicaFailure::new(binding, error.to_string()));
                    continue;
                }
            };
            match execution.add_replica(consensus_store, state_store) {
                Ok(_) => {
                    state.metrics.record_hosted();
                    observed.push(binding);
                }
                Err(error) => {
                    state.metrics.record_replica_failure();
                    failures.push(ReplicaFailure::new(binding, error.to_string()));
                }
            }
        }
        state.set_lifecycle(LifecycleState::Ready);

        Ok(DataNode {
            execution,
            observed,
            failures,
            state,
        })
    }
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

pub struct DataNode {
    execution: DataExecution,
    observed: Vec<ReplicaBinding>,
    failures: Vec<ReplicaFailure>,
    state: Arc<ProcessState>,
}

impl DataNode {
    pub async fn observed_replicas(&self) -> Vec<ReplicaBinding> {
        self.observed.clone()
    }

    pub async fn replica_failures(&self) -> Vec<ReplicaFailure> {
        self.failures.clone()
    }

    pub fn provider_kinds(&self) -> Vec<ProviderKind> {
        self.execution.provider_kinds()
    }

    pub fn lifecycle(&self) -> LifecycleState {
        self.state.lifecycle()
    }

    pub fn metrics(&self) -> DataMetrics {
        self.state.metrics.clone()
    }

    pub fn rpc_service(&self) -> DataRpcService {
        DataRpcService {
            state: self.state.clone(),
        }
    }

    pub fn begin_draining(&self) {
        self.state.set_lifecycle(LifecycleState::Draining);
    }

    pub fn stop(&self) {
        self.state.set_lifecycle(LifecycleState::Stopped);
    }
}

#[derive(Clone)]
pub struct DataRpcService {
    state: Arc<ProcessState>,
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

    pub fn begin_draining(&self) {
        self.state.set_lifecycle(LifecycleState::Draining);
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

    fn unsupported(&self, operation: &'static str) -> Status {
        self.state.metrics.record_rpc_failure();
        Status::unimplemented(format!(
            "{operation} is not exposed by the thin DataExecution facade"
        ))
    }
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
        validate_execution_fragment(request.into_inner()).map_err(|error| self.invalid(error))?;
        Err(self.unsupported("ExecuteFragment"))
    }

    async fn apply_transaction(
        &self,
        request: Request<TransactionRequest>,
    ) -> Result<Response<TypedStatus>, Status> {
        self.begin_request()?;
        validate_transaction_request(request.into_inner()).map_err(|error| self.invalid(error))?;
        Err(self.unsupported("ApplyTransaction"))
    }

    async fn send_raft(
        &self,
        request: Request<RaftEnvelope>,
    ) -> Result<Response<TypedStatus>, Status> {
        self.begin_request()?;
        validate_raft_envelope(request.into_inner()).map_err(|error| self.invalid(error))?;
        Err(self.unsupported("SendRaft"))
    }

    async fn install_replica_snapshot(
        &self,
        request: Request<LogicalReplicaSnapshot>,
    ) -> Result<Response<TypedStatus>, Status> {
        self.begin_request()?;
        validate_replica_snapshot(request.into_inner()).map_err(|error| self.invalid(error))?;
        Err(self.unsupported("InstallReplicaSnapshot"))
    }
}
