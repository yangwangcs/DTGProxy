use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::sync::Arc;

use dtg_execution::cluster_protocol::proto::{
    self, RetryDisposition, StatusCode, TypedStatus,
    controller_service_server::{ControllerService, ControllerServiceServer},
};
use dtg_execution::cluster_protocol::{PROTOCOL_MAJOR, validate_control_observation};
use dtg_execution::control::{
    BackendClass, BindingRole, CatalogState, ObservedNodeState, ProviderKind, ReconcileAction,
    ReplicaBinding, Version,
};
use dtg_execution::storage::StorageError;
use dtg_execution::{ControllerExecution, ExecutionBuildError};
use dtg_storage_fjall::FjallConsensusStore;
use serde::Deserialize;
use tokio::sync::Mutex;
use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};
use tonic::{Request, Response, Status};

use crate::{ControllerConfig, TransportSecurity};

const CONTROLLER_GRAPH_ID: u64 = u64::MAX - 3;

pub struct ControllerProcess {
    config: ControllerConfig,
    core: Arc<ControllerCore>,
    _consensus: Arc<FjallConsensusStore>,
}

struct ControllerCore {
    execution: Mutex<ControllerExecution>,
}

impl ControllerProcess {
    pub async fn open(
        config: ControllerConfig,
        catalog: CatalogState,
    ) -> Result<Self, ControllerProcessError> {
        std::fs::create_dir_all(config.data_directory())?;
        let consensus = Arc::new(FjallConsensusStore::open(
            config.consensus_path(),
            consensus_binding(&config)?,
        )?);
        let execution = ControllerExecution::builder()
            .with_catalog(catalog)
            .build()?;
        Ok(Self {
            config,
            core: Arc::new(ControllerCore {
                execution: Mutex::new(execution),
            }),
            _consensus: consensus,
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
        let observation = match ObservedNodeState::new(
            node_id,
            Version::new(observation.catalog_version),
            Vec::new(),
        ) {
            Ok(observation) => observation,
            Err(error) => {
                return Ok(Response::new(status(
                    context,
                    StatusCode::InvalidRequest,
                    RetryDisposition::Never,
                    error.code(),
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
}

fn consensus_binding(config: &ControllerConfig) -> Result<ReplicaBinding, StorageError> {
    let class = BackendClass::new(
        ProviderKind::Fjall,
        1,
        1,
        [
            "adjacency",
            "immutable-read-view",
            "logical-snapshot",
            "point",
        ],
    )?;
    ReplicaBinding::builder()
        .cluster_id(config.cluster_id())
        .graph_id(CONTROLLER_GRAPH_ID)
        .shard_id(1)
        .placement_epoch(1)
        .replica_id(config.node_id())
        .backend_generation(1)
        .backend_class_digest(class.digest())
        .provider_kind(ProviderKind::Fjall)
        .contract_version(1)
        .layout_version(1)
        .capability_digest(class.required_capabilities().digest())
        .namespace_id("controller")
        .endpoint_profile_ref("process://local-fjall")
        .credential_ref("process://local-fjall")
        .role(BindingRole::Active)
        .build()
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
}

impl Display for ControllerProcessError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(message)
            | Self::Storage(message)
            | Self::Control(message)
            | Self::Execution(message)
            | Self::Transport(message) => formatter.write_str(message),
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
