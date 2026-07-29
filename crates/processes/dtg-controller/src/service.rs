use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::sync::Arc;

use dtg_execution::cluster_protocol::proto::{
    self, CatalogSnapshot, CatalogWatchRequest, RequestContext, RetryDisposition, StatusCode,
    TypedStatus,
    controller_service_server::{ControllerService, ControllerServiceServer},
    meta_service_client::MetaServiceClient,
};
use dtg_execution::cluster_protocol::{
    PROTOCOL_MAJOR, checksum_bytes, validate_control_observation,
};
use dtg_execution::control::{
    BackendClass, BackendGeneration, BindingRole, CatalogCommand, CatalogState, GraphId,
    ObservedNodeState, ObservedReplicaLifecycle, ObservedReplicaState, PlacementEpoch,
    ProviderKind, ReconcileAction, ReplicaBinding, ReplicaBindingRecord, ReplicaId, RetentionPin,
    ShardId, ShardPlacement, TransactionTime, Version,
};
use dtg_execution::storage::{DurabilityPolicy, StorageError};
use dtg_execution::{ControllerExecution, ExecutionBuildError};
use serde::Deserialize;
use tokio::sync::Mutex;
use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};
use tonic::{Request, Response, Status};

use crate::{ControllerConfig, TransportSecurity};

pub struct ControllerProcess {
    config: ControllerConfig,
    core: Arc<ControllerCore>,
}

struct ControllerCore {
    execution: Mutex<ControllerExecution>,
}

impl ControllerProcess {
    pub async fn connect(config: ControllerConfig) -> Result<Self, ControllerProcessError> {
        let (catalog, endpoint) = initial_catalog(&config).await?;
        let process = Self::compose(config, catalog).await?;
        spawn_catalog_watch(
            process.core.clone(),
            process.config.cluster_id(),
            process.config.meta_endpoints().to_vec(),
            endpoint,
        );
        Ok(process)
    }

    #[doc(hidden)]
    pub async fn open_for_test(
        config: ControllerConfig,
        catalog: CatalogState,
    ) -> Result<Self, ControllerProcessError> {
        Self::compose(config, catalog).await
    }

    async fn compose(
        config: ControllerConfig,
        catalog: CatalogState,
    ) -> Result<Self, ControllerProcessError> {
        std::fs::create_dir_all(config.data_directory())?;
        let execution = ControllerExecution::builder()
            .with_catalog(catalog)
            .build()?;
        Ok(Self {
            config,
            core: Arc::new(ControllerCore {
                execution: Mutex::new(execution),
            }),
        })
    }

    pub async fn record_observation(
        &self,
        observation: ObservedNodeState,
    ) -> Result<(), ControllerProcessError> {
        self.core
            .execution
            .lock()
            .await
            .record_observation(observation)?;
        Ok(())
    }

    pub async fn reconcile(&self) -> Result<Vec<ReconcileAction>, ControllerProcessError> {
        Ok(self.core.execution.lock().await.reconcile()?)
    }

    pub fn rpc_service(&self) -> ControllerRpcService {
        ControllerRpcService {
            core: self.core.clone(),
            cluster_id: self.config.cluster_id(),
        }
    }

    pub async fn serve(self) -> Result<(), ControllerProcessError> {
        let mut server = Server::builder();
        if let TransportSecurity::MutualTls(files) = self.config.security() {
            let certificate = tokio::fs::read(files.certificate()).await?;
            let private_key = tokio::fs::read(files.private_key()).await?;
            let client_ca = tokio::fs::read(files.client_ca()).await?;
            server = server.tls_config(
                ServerTlsConfig::new()
                    .identity(Identity::from_pem(certificate, private_key))
                    .client_ca_root(Certificate::from_pem(client_ca)),
            )?;
        }
        let address = self.config.listen_addr();
        tracing::info!(%address, node_id = self.config.node_id(), "Controller process serving cluster protocol v2");
        server
            .add_service(ControllerServiceServer::new(self.rpc_service()))
            .serve_with_shutdown(address, shutdown_signal())
            .await?;
        Ok(())
    }
}

async fn initial_catalog(
    config: &ControllerConfig,
) -> Result<(CatalogState, String), ControllerProcessError> {
    let mut last_error = None;
    for endpoint in config.meta_endpoints() {
        match open_catalog_stream(endpoint, config.cluster_id(), 0).await {
            Ok(mut stream) => match stream.message().await {
                Ok(Some(snapshot)) => {
                    return Ok((decode_catalog_snapshot(snapshot)?, endpoint.clone()));
                }
                Ok(None) => last_error = Some("Meta catalog stream ended before bootstrap".into()),
                Err(error) => last_error = Some(error.to_string()),
            },
            Err(error) => last_error = Some(error.to_string()),
        }
    }
    Err(ControllerProcessError::CatalogSource(
        last_error.unwrap_or_else(|| "no Meta endpoints configured".into()),
    ))
}

fn spawn_catalog_watch(
    core: Arc<ControllerCore>,
    cluster_id: u64,
    endpoints: Vec<String>,
    preferred: String,
) {
    tokio::spawn(async move {
        let mut ordered = vec![preferred.clone()];
        ordered.extend(
            endpoints
                .into_iter()
                .filter(|endpoint| endpoint != &preferred),
        );
        loop {
            let after_revision = core.execution.lock().await.catalog_version().get();
            let mut connected = false;
            for endpoint in &ordered {
                let Ok(mut stream) =
                    open_catalog_stream(endpoint, cluster_id, after_revision).await
                else {
                    continue;
                };
                connected = true;
                while let Ok(Some(snapshot)) = stream.message().await {
                    let Ok(catalog) = decode_catalog_snapshot(snapshot) else {
                        break;
                    };
                    if core
                        .execution
                        .lock()
                        .await
                        .install_catalog(catalog)
                        .is_err()
                    {
                        break;
                    }
                }
            }
            let delay = if connected { 100 } else { 500 };
            tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
        }
    });
}

async fn open_catalog_stream(
    endpoint: &str,
    cluster_id: u64,
    after_revision: u64,
) -> Result<tonic::Streaming<CatalogSnapshot>, ControllerProcessError> {
    let mut client = MetaServiceClient::connect(endpoint.to_owned())
        .await
        .map_err(|error| ControllerProcessError::CatalogSource(error.to_string()))?;
    Ok(client
        .watch_catalog(CatalogWatchRequest {
            request: Some(RequestContext {
                protocol_major: PROTOCOL_MAJOR,
                protocol_minor: 0,
                cluster_id: cluster_id.to_be_bytes().to_vec(),
                request_id: catalog_watch_request_id().to_be_bytes().to_vec(),
                deadline_unix_ms: u64::MAX,
                trace_context: Vec::new(),
            }),
            after_revision,
        })
        .await
        .map_err(|error| ControllerProcessError::CatalogSource(error.to_string()))?
        .into_inner())
}

fn catalog_watch_request_id() -> u128 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .max(1)
}

#[derive(Clone)]
pub struct ControllerRpcService {
    core: Arc<ControllerCore>,
    cluster_id: u64,
}

impl ControllerRpcService {
    pub const fn protocol_major(&self) -> u32 {
        PROTOCOL_MAJOR
    }
}

#[tonic::async_trait]
impl ControllerService for ControllerRpcService {
    async fn observe(
        &self,
        request: Request<proto::ControlObservation>,
    ) -> Result<Response<TypedStatus>, Status> {
        let wire = request.into_inner();
        let context = response_context(wire.request.as_ref(), self.cluster_id);
        let payload = match validate_control_observation(wire.clone()) {
            Ok(payload) => payload,
            Err(error) => {
                return Ok(Response::new(status(
                    context,
                    StatusCode::InvalidRequest,
                    RetryDisposition::Never,
                    error.code(),
                )));
            }
        };
        let node_id = match String::from_utf8(wire.node_id) {
            Ok(node_id) if !node_id.trim().is_empty() => node_id,
            _ => {
                return Ok(Response::new(status(
                    context,
                    StatusCode::InvalidRequest,
                    RetryDisposition::Never,
                    "invalid UTF-8 node identity",
                )));
            }
        };
        let observation: ObservationWire = match serde_json::from_slice(payload.body()) {
            Ok(observation) => observation,
            Err(_) => {
                return Ok(Response::new(status(
                    context,
                    StatusCode::InvalidRequest,
                    RetryDisposition::Never,
                    "invalid observation payload",
                )));
            }
        };
        let observation = match observation.into_state(node_id) {
            Ok(observation) => observation,
            Err(error) => {
                return Ok(Response::new(status(
                    context,
                    StatusCode::InvalidRequest,
                    RetryDisposition::Never,
                    &error.to_string(),
                )));
            }
        };
        let result = self
            .core
            .execution
            .lock()
            .await
            .record_observation(observation);
        Ok(Response::new(match result {
            Ok(()) => status(context, StatusCode::Ok, RetryDisposition::Never, "ok"),
            Err(error) => status(
                context,
                StatusCode::Conflict,
                RetryDisposition::Safe,
                error.code(),
            ),
        }))
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ObservationWire {
    catalog_version: u64,
    replicas: Vec<ReplicaObservationWire>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReplicaObservationWire {
    binding: ReplicaBindingWire,
    backend_class: BackendClassWire,
    lifecycle: LifecycleWire,
    applied_index: u64,
    leader_id: Option<u64>,
    closed_timestamp: Option<i64>,
    caught_up: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReplicaBindingWire {
    cluster_id: u64,
    graph_id: u64,
    shard_id: u64,
    placement_epoch: u64,
    replica_id: u64,
    backend_generation: u64,
    provider: ProviderWire,
    contract_version: u32,
    layout_version: u32,
    namespace_id: String,
    endpoint_profile_ref: String,
    credential_ref: String,
    role: BindingRoleWire,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BackendClassWire {
    provider: ProviderWire,
    contract_version: u32,
    layout_version: u32,
    durability: DurabilityWire,
    capabilities: Vec<String>,
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ProviderWire {
    Fjall,
    PostgreSql,
    Neo4j,
    Remote(String),
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum DurabilityWire {
    DurableCommit,
    DurableCommitWithReplicaSync,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum BindingRoleWire {
    Candidate,
    Active,
    Retiring,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum LifecycleWire {
    Allocated,
    Learner,
    Voter,
    Sealed,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum CatalogCommandWire {
    PutPlacement {
        expected_version: u64,
        placement: PlacementWire,
    },
    PinRetention {
        expected_version: u64,
        graph_id: u64,
        shard_id: u64,
        generation: u64,
        pin: String,
    },
    UnpinRetention {
        expected_version: u64,
        graph_id: u64,
        shard_id: u64,
        generation: u64,
        pin: String,
    },
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PlacementWire {
    graph_id: u64,
    shard_id: u64,
    placement_epoch: u64,
    active_generation: u64,
    backend_class: BackendClassWire,
    replicas: Vec<ReplicaRecordWire>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReplicaRecordWire {
    binding: ReplicaBindingWire,
    backend_class: BackendClassWire,
}

fn decode_catalog_snapshot(
    snapshot: CatalogSnapshot,
) -> Result<CatalogState, ControllerProcessError> {
    let payload = snapshot.payload.ok_or_else(|| {
        ControllerProcessError::CatalogSource("catalog snapshot has no payload".into())
    })?;
    if payload.format_version != 1
        || payload.declared_len != payload.body.len() as u64
        || payload.checksum.as_slice() != checksum_bytes(&payload.body)
    {
        return Err(ControllerProcessError::CatalogSource(
            "catalog snapshot payload fence failed".into(),
        ));
    }
    let commands: Vec<CatalogCommandWire> = serde_json::from_slice(&payload.body)
        .map_err(|error| ControllerProcessError::CatalogSource(error.to_string()))?;
    if payload.item_count as usize != commands.len() {
        return Err(ControllerProcessError::CatalogSource(
            "catalog snapshot command count mismatch".into(),
        ));
    }
    let mut catalog = CatalogState::new();
    for command in commands {
        catalog = catalog.apply(command.into_command()?)?;
    }
    if catalog.version().get() != snapshot.revision {
        return Err(ControllerProcessError::CatalogSource(
            "catalog snapshot revision mismatch".into(),
        ));
    }
    Ok(catalog)
}

impl CatalogCommandWire {
    fn into_command(self) -> Result<CatalogCommand, ControllerProcessError> {
        match self {
            Self::PutPlacement {
                expected_version,
                placement,
            } => Ok(CatalogCommand::put_placement(
                Version::new(expected_version),
                placement.into_placement()?,
            )),
            Self::PinRetention {
                expected_version,
                graph_id,
                shard_id,
                generation,
                pin,
            } => Ok(CatalogCommand::pin_retention(
                Version::new(expected_version),
                GraphId::new(graph_id).map_err(control_identity_error)?,
                ShardId::new(shard_id).map_err(control_identity_error)?,
                BackendGeneration::new(generation).map_err(control_identity_error)?,
                RetentionPin::new(pin)?,
            )),
            Self::UnpinRetention {
                expected_version,
                graph_id,
                shard_id,
                generation,
                pin,
            } => Ok(CatalogCommand::unpin_retention(
                Version::new(expected_version),
                GraphId::new(graph_id).map_err(control_identity_error)?,
                ShardId::new(shard_id).map_err(control_identity_error)?,
                BackendGeneration::new(generation).map_err(control_identity_error)?,
                RetentionPin::new(pin)?,
            )),
        }
    }
}

impl PlacementWire {
    fn into_placement(self) -> Result<ShardPlacement, ControllerProcessError> {
        let backend_class = self.backend_class.into_backend_class()?;
        Ok(ShardPlacement {
            graph_id: GraphId::new(self.graph_id).map_err(control_identity_error)?,
            shard_id: ShardId::new(self.shard_id).map_err(control_identity_error)?,
            placement_epoch: PlacementEpoch::new(self.placement_epoch)
                .map_err(control_identity_error)?,
            active_generation: BackendGeneration::new(self.active_generation)
                .map_err(control_identity_error)?,
            backend_class,
            replicas: self
                .replicas
                .into_iter()
                .map(ReplicaRecordWire::into_record)
                .collect::<Result<Vec<_>, _>>()?,
        })
    }
}

impl ReplicaRecordWire {
    fn into_record(self) -> Result<ReplicaBindingRecord, ControllerProcessError> {
        let backend_class = self.backend_class.into_backend_class()?;
        let binding = self.binding.into_binding(&backend_class)?;
        Ok(ReplicaBindingRecord::new(binding, backend_class)?)
    }
}

fn control_identity_error(error: impl Display) -> ControllerProcessError {
    ControllerProcessError::Control(error.to_string())
}

impl ObservationWire {
    fn into_state(self, node_id: String) -> Result<ObservedNodeState, ControllerProcessError> {
        let catalog_version = Version::new(self.catalog_version);
        let replicas = self
            .replicas
            .into_iter()
            .map(|replica| replica.into_state(&node_id, catalog_version))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(ObservedNodeState::new(node_id, catalog_version, replicas)?)
    }
}

impl ReplicaObservationWire {
    fn into_state(
        self,
        node_id: &str,
        catalog_version: Version,
    ) -> Result<ObservedReplicaState, ControllerProcessError> {
        let backend_class = self.backend_class.into_backend_class()?;
        let binding = self.binding.into_binding(&backend_class)?;
        let leader_id = self
            .leader_id
            .map(ReplicaId::new)
            .transpose()
            .map_err(|error| ControllerProcessError::Control(error.to_string()))?;
        let closed_timestamp = self
            .closed_timestamp
            .map(TransactionTime::new)
            .transpose()
            .map_err(|error| ControllerProcessError::Control(error.to_string()))?;
        Ok(ObservedReplicaState::new(
            node_id.to_owned(),
            catalog_version,
            binding,
            backend_class.digest(),
            self.lifecycle.into(),
        )?
        .with_applied_index(self.applied_index)
        .with_leader_id(leader_id)
        .with_closed_timestamp(closed_timestamp)
        .with_caught_up(self.caught_up))
    }
}

impl ReplicaBindingWire {
    fn into_binding(
        self,
        backend_class: &BackendClass,
    ) -> Result<ReplicaBinding, ControllerProcessError> {
        Ok(ReplicaBinding::builder()
            .cluster_id(self.cluster_id)
            .graph_id(self.graph_id)
            .shard_id(self.shard_id)
            .placement_epoch(self.placement_epoch)
            .replica_id(self.replica_id)
            .backend_generation(self.backend_generation)
            .backend_class_digest(backend_class.digest())
            .provider_kind(self.provider.into())
            .contract_version(self.contract_version)
            .layout_version(self.layout_version)
            .capability_digest(backend_class.required_capabilities().digest())
            .namespace_id(self.namespace_id)
            .endpoint_profile_ref(self.endpoint_profile_ref)
            .credential_ref(self.credential_ref)
            .role(self.role.into())
            .build()?)
    }
}

impl BackendClassWire {
    fn into_backend_class(self) -> Result<BackendClass, ControllerProcessError> {
        Ok(BackendClass::with_durability(
            self.provider.into(),
            self.contract_version,
            self.layout_version,
            self.durability.into(),
            self.capabilities,
        )?)
    }
}

impl From<ProviderWire> for ProviderKind {
    fn from(value: ProviderWire) -> Self {
        match value {
            ProviderWire::Fjall => Self::Fjall,
            ProviderWire::PostgreSql => Self::PostgreSql,
            ProviderWire::Neo4j => Self::Neo4j,
            ProviderWire::Remote(name) => Self::Remote(name),
        }
    }
}

impl From<DurabilityWire> for DurabilityPolicy {
    fn from(value: DurabilityWire) -> Self {
        match value {
            DurabilityWire::DurableCommit => Self::DurableCommit,
            DurabilityWire::DurableCommitWithReplicaSync => Self::DurableCommitWithReplicaSync,
        }
    }
}

impl From<BindingRoleWire> for BindingRole {
    fn from(value: BindingRoleWire) -> Self {
        match value {
            BindingRoleWire::Candidate => Self::Candidate,
            BindingRoleWire::Active => Self::Active,
            BindingRoleWire::Retiring => Self::Retiring,
        }
    }
}

impl From<LifecycleWire> for ObservedReplicaLifecycle {
    fn from(value: LifecycleWire) -> Self {
        match value {
            LifecycleWire::Allocated => Self::Allocated,
            LifecycleWire::Learner => Self::Learner,
            LifecycleWire::Voter => Self::Voter,
            LifecycleWire::Sealed => Self::Sealed,
        }
    }
}

fn status(
    request: proto::RequestContext,
    code: StatusCode,
    retry: RetryDisposition,
    message: &str,
) -> TypedStatus {
    TypedStatus {
        request: Some(request),
        code: code as i32,
        retry: retry as i32,
        message: message.to_owned(),
        idempotency_key: Vec::new(),
        details: None,
    }
}

fn response_context(
    context: Option<&proto::RequestContext>,
    cluster_id: u64,
) -> proto::RequestContext {
    context.cloned().unwrap_or_else(|| proto::RequestContext {
        protocol_major: PROTOCOL_MAJOR,
        protocol_minor: 0,
        cluster_id: cluster_id.to_be_bytes().to_vec(),
        request_id: 1_u128.to_be_bytes().to_vec(),
        deadline_unix_ms: 1,
        trace_context: Vec::new(),
    })
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

#[derive(Debug)]
pub enum ControllerProcessError {
    Io(String),
    Storage(String),
    Control(String),
    Execution(String),
    Transport(String),
    CatalogSource(String),
}

impl Display for ControllerProcessError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(message)
            | Self::Storage(message)
            | Self::Control(message)
            | Self::Execution(message)
            | Self::Transport(message)
            | Self::CatalogSource(message) => formatter.write_str(message),
        }
    }
}

impl Error for ControllerProcessError {}

impl From<std::io::Error> for ControllerProcessError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error.to_string())
    }
}

impl From<StorageError> for ControllerProcessError {
    fn from(error: StorageError) -> Self {
        Self::Storage(error.to_string())
    }
}

impl From<dtg_execution::control::ControlError> for ControllerProcessError {
    fn from(error: dtg_execution::control::ControlError) -> Self {
        Self::Control(error.to_string())
    }
}

impl From<ExecutionBuildError> for ControllerProcessError {
    fn from(error: ExecutionBuildError) -> Self {
        Self::Execution(error.to_string())
    }
}

impl From<tonic::transport::Error> for ControllerProcessError {
    fn from(error: tonic::transport::Error) -> Self {
        Self::Transport(error.to_string())
    }
}
