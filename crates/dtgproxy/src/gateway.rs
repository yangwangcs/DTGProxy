use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::future::Future;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use adapter_postgres::PostgresAdapterFactory;
use adapter_registry::{AdapterOpenRequest, AdapterRegistry, RegistryError, SecretString};
use adapter_rocksdb::RocksAdapterFactory;
use control_plane::{BackendProfile, Catalog, CatalogCommand, CatalogError, GraphDefinition};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use storage_api::{BackendFamily, StorageAdapter};
use temporal_ir::GraphScope;
use temporal_storage::{
    EdgeMutation, EdgeTypeId, ElementId, ElementRef, GraphId, LabelId, PartitionId,
    TemporalTransaction, VertexMutation,
};
use temporal_types::{CanonicalElement, Interval, ValidTime};
use timestamp_oracle::{TimestampOracle, TimestampOracleError};
use txn_protocol::IsolationLevel;

use crate::config::{ConfigError, NodeConfig};
use crate::{
    DeploymentConfig, DeploymentError, DeploymentMode, InProcessDeploymentRuntime,
    RoutedRuntimeError, ScopedTemporalTransaction, TransactionCoordinator,
    TransactionCoordinatorError,
};

pub const GATEWAY_API_VERSION: u16 = 1;
pub const MAX_GATEWAY_FRAME_BYTES: usize = 4 * 1024 * 1024;

pub fn initialize_node(
    config_path: impl AsRef<Path>,
    config: &NodeConfig,
    graph: GraphDefinition,
) -> Result<(), GatewayError> {
    if graph.graph_id() != config.graph_id() {
        return Err(GatewayError::GraphMismatch {
            expected: config.graph_id(),
            actual: graph.graph_id(),
        });
    }
    std::fs::create_dir_all(config.root()).map_err(io_error)?;
    let mut catalog = Catalog::open(config.root().join("control-plane"))?;
    if catalog.state().revision() != 0 {
        return Err(GatewayError::AlreadyInitialized);
    }
    catalog.execute(CatalogCommand::create_graph(1, 0, graph))?;
    catalog.checkpoint()?;
    config.store(config_path)?;
    Ok(())
}

pub struct GatewayService {
    config: NodeConfig,
    catalog: Catalog,
    oracle: TimestampOracle,
    runtime: InProcessDeploymentRuntime,
    backends: Vec<BackendReplicaStatus>,
}

type ReplicaAdapters = std::collections::BTreeMap<(u32, u64), Arc<dyn StorageAdapter>>;

async fn open_backend_replicas(
    config: &NodeConfig,
    graph: &GraphDefinition,
    deployment: &DeploymentConfig,
) -> Result<(ReplicaAdapters, Vec<BackendReplicaStatus>), GatewayError> {
    let mut registry = AdapterRegistry::new();
    registry.register(Arc::new(RocksAdapterFactory))?;
    registry.register(Arc::new(PostgresAdapterFactory))?;

    let mut adapters = ReplicaAdapters::new();
    let mut statuses = Vec::new();
    for shard in deployment.all_shards() {
        for node_id in shard.voters() {
            let instance_id = format!(
                "graph-{}-shard-{}-replica-{}-generation-{}",
                graph.graph_id(),
                shard.shard_id(),
                node_id,
                graph.backend().generation()
            );
            let request = backend_open_request(
                config.root(),
                graph.backend(),
                shard.shard_id(),
                *node_id,
                instance_id,
            )?;
            let opened = registry
                .open(
                    graph.backend().provider(),
                    &request,
                    graph.backend().requirement(),
                )
                .await?;
            let adapter = opened.into_adapter();
            statuses.push(BackendReplicaStatus::from_adapter(
                shard.shard_id(),
                *node_id,
                graph.backend().provider(),
                adapter.as_ref(),
            )?);
            adapters.insert((shard.shard_id(), *node_id), adapter);
        }
    }
    Ok((adapters, statuses))
}

fn backend_open_request(
    root: &Path,
    profile: &BackendProfile,
    shard_id: u32,
    node_id: u64,
    instance_id: String,
) -> Result<AdapterOpenRequest, GatewayError> {
    let mut request = AdapterOpenRequest::new(instance_id);
    for (name, value) in profile.public_parameters() {
        let value = if profile.provider() == "rocksdb" && name == "path" {
            let base = resolve_from_root(root, value);
            let path = base
                .join(format!("shard-{shard_id}"))
                .join(format!("replica-{node_id}"));
            std::fs::create_dir_all(path.parent().expect("replica path has a parent"))
                .map_err(io_error)?;
            path.to_string_lossy().into_owned()
        } else {
            value.clone()
        };
        request = request.with_parameter(name, value);
    }
    for (name, reference) in profile.secret_references() {
        request = request.with_secret(name, SecretString::new(resolve_secret(root, reference)?));
    }
    Ok(request)
}

fn resolve_from_root(root: &Path, value: &str) -> PathBuf {
    let path = PathBuf::from(value);
    if path.is_absolute() {
        path
    } else {
        root.join(path)
    }
}

fn resolve_secret(root: &Path, reference: &str) -> Result<String, GatewayError> {
    let secret = if let Some(variable) = reference.strip_prefix("env:") {
        std::env::var(variable).map_err(|_| GatewayError::SecretReference {
            reference: reference.to_owned(),
        })?
    } else if let Some(path) = reference.strip_prefix("file:") {
        std::fs::read_to_string(resolve_from_root(root, path)).map_err(|_| {
            GatewayError::SecretReference {
                reference: reference.to_owned(),
            }
        })?
    } else {
        std::env::var(reference).map_err(|_| GatewayError::SecretReference {
            reference: reference.to_owned(),
        })?
    };
    let secret = secret.trim().to_owned();
    if secret.is_empty() {
        return Err(GatewayError::SecretReference {
            reference: reference.to_owned(),
        });
    }
    Ok(secret)
}

impl GatewayService {
    pub async fn open(config: NodeConfig) -> Result<Self, GatewayError> {
        let catalog = Catalog::open(config.root().join("control-plane"))?;
        let graph = catalog.state().graph(config.graph_id()).cloned().ok_or(
            GatewayError::UnknownGraph {
                graph_id: config.graph_id(),
            },
        )?;
        let deployment = DeploymentConfig::from_catalog(&graph)?;
        let (adapters, backends) = open_backend_replicas(&config, &graph, &deployment).await?;
        let mut runtime =
            InProcessDeploymentRuntime::new_with_adapters(deployment, adapters).await?;
        let elections = runtime
            .config()
            .all_shards()
            .iter()
            .map(|placement| (placement.shard_id(), placement.voters()[0]))
            .collect::<Vec<_>>();
        for (shard_id, node_id) in elections {
            runtime.elect(shard_id, node_id).await?;
        }
        let oracle = TimestampOracle::production(
            config.root().join("timestamp-oracle.state"),
            config.timestamp_reservation(),
        )?;
        Ok(Self {
            config,
            catalog,
            oracle,
            runtime,
            backends,
        })
    }

    #[must_use]
    pub const fn config(&self) -> &NodeConfig {
        &self.config
    }

    #[must_use]
    pub const fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    #[must_use]
    pub const fn runtime(&self) -> &InProcessDeploymentRuntime {
        &self.runtime
    }

    #[must_use]
    pub fn backend_replicas(&self) -> &[BackendReplicaStatus] {
        &self.backends
    }

    pub fn status(&self) -> Result<GatewayStatus, GatewayError> {
        let graph = self.catalog.state().graph(self.config.graph_id()).ok_or(
            GatewayError::UnknownGraph {
                graph_id: self.config.graph_id(),
            },
        )?;
        Ok(GatewayStatus {
            version: GATEWAY_API_VERSION,
            graph_id: graph.graph_id().to_string(),
            graph_name: graph.name().to_owned(),
            mode: match self.runtime.config().mode() {
                DeploymentMode::PrimaryReplica => "primary_replica",
                DeploymentMode::SharedNothing => "shared_nothing",
            }
            .to_owned(),
            catalog_revision: self.catalog.state().revision(),
            schema_version: graph.schema_version(),
            topology_epoch: graph.topology().epoch(),
            backend_provider: graph.backend().provider().to_owned(),
            backend_generation: graph.backend().generation(),
            shards: graph
                .topology()
                .placements()
                .iter()
                .map(|placement| GatewayShardStatus {
                    shard_id: placement.shard_id(),
                    placement_epoch: placement.epoch(),
                    leader_id: self
                        .runtime
                        .raft()
                        .group(placement.shard_id())
                        .and_then(|group| group.leader_id()),
                })
                .collect(),
        })
    }

    pub async fn execute_request(&mut self, request: GatewayRequest) -> GatewayResponse {
        let request_id = request.request_id.clone();
        match self.execute_operation(request).await {
            Ok(result) => GatewayResponse::success(request_id, result),
            Err(error) => GatewayResponse::failure(request_id, error.to_string()),
        }
    }

    pub async fn execute_json(&mut self, bytes: &[u8]) -> Vec<u8> {
        let parsed = serde_json::from_slice::<GatewayRequest>(bytes);
        let response = match parsed {
            Ok(request) => self.execute_request(request).await,
            Err(error) => {
                GatewayResponse::failure("unknown".into(), format!("invalid request JSON: {error}"))
            }
        };
        serde_json::to_vec(&response).expect("GatewayResponse is JSON serializable")
    }

    async fn execute_operation(&mut self, request: GatewayRequest) -> Result<Value, GatewayError> {
        if request.version != GATEWAY_API_VERSION {
            return Err(GatewayError::UnsupportedApiVersion {
                version: request.version,
            });
        }
        if request.request_id.is_empty() || request.request_id.len() > 128 {
            return Err(GatewayError::InvalidRequestId);
        }
        match request.operation {
            GatewayOperation::Status => {
                Ok(serde_json::to_value(self.status()?)
                    .expect("GatewayStatus is JSON serializable"))
            }
            GatewayOperation::Query { text } => {
                let plan = temporal_query::parse(&text)
                    .map_err(|error| GatewayError::Query(error.to_string()))?;
                if plan.scope().graph().value() != self.config.graph_id() {
                    return Err(GatewayError::GraphMismatch {
                        expected: self.config.graph_id(),
                        actual: plan.scope().graph().value(),
                    });
                }
                let result = self
                    .runtime
                    .execute_leader(&plan, self.config.max_raft_ticks())
                    .await?;
                let canonical = result
                    .to_canonical_json()
                    .map_err(|error| GatewayError::Query(error.to_string()))?;
                serde_json::from_str(&canonical)
                    .map_err(|error| GatewayError::Query(error.to_string()))
            }
            GatewayOperation::Transaction {
                schema_version,
                ttl_micros,
                mutations,
            } => {
                let scoped = mutations
                    .into_iter()
                    .map(|mutation| mutation.into_scoped(self.config.graph_id()))
                    .collect::<Result<Vec<_>, _>>()?;
                let coordinator =
                    TransactionCoordinator::new(&self.oracle, self.config.max_raft_ticks());
                let receipt = coordinator
                    .commit_temporal(
                        &mut self.runtime,
                        schema_version,
                        IsolationLevel::TemporalSnapshot,
                        ttl_micros,
                        scoped,
                    )
                    .await?;
                Ok(json!({
                    "transaction_id": receipt.transaction_id().value().to_string(),
                    "start_ts": timestamp_json(receipt.start_ts()),
                    "commit_ts": timestamp_json(receipt.commit_ts()),
                    "home_shard": receipt.home().shard_id(),
                    "home_epoch": receipt.home().placement_epoch(),
                    "participants": receipt.participants().iter().map(|participant| json!({
                        "shard_id": participant.shard_id(),
                        "placement_epoch": participant.placement_epoch(),
                    })).collect::<Vec<_>>(),
                    "single_shard_fast_path": receipt.single_shard_fast_path(),
                }))
            }
        }
    }
}

pub fn serve(
    mut gateway: GatewayService,
    listener: TcpListener,
    max_requests: Option<usize>,
) -> Result<(), GatewayError> {
    let mut served = 0_usize;
    loop {
        if max_requests.is_some_and(|maximum| served >= maximum) {
            return Ok(());
        }
        let (mut stream, _) = listener.accept().map_err(io_error)?;
        let request = read_frame(&mut stream)?;
        let response = block_on(gateway.execute_json(&request));
        write_frame(&mut stream, &response)?;
        served = served.saturating_add(1);
    }
}

pub fn read_frame(stream: &mut TcpStream) -> Result<Vec<u8>, GatewayError> {
    let mut length = [0_u8; 4];
    stream.read_exact(&mut length).map_err(io_error)?;
    let length =
        usize::try_from(u32::from_be_bytes(length)).map_err(|_| GatewayError::FrameTooLarge)?;
    if length == 0 || length > MAX_GATEWAY_FRAME_BYTES {
        return Err(GatewayError::FrameTooLarge);
    }
    let mut bytes = vec![0; length];
    stream.read_exact(&mut bytes).map_err(io_error)?;
    Ok(bytes)
}

pub fn write_frame(stream: &mut TcpStream, bytes: &[u8]) -> Result<(), GatewayError> {
    if bytes.is_empty() || bytes.len() > MAX_GATEWAY_FRAME_BYTES {
        return Err(GatewayError::FrameTooLarge);
    }
    let length = u32::try_from(bytes.len()).map_err(|_| GatewayError::FrameTooLarge)?;
    stream
        .write_all(&length.to_be_bytes())
        .and_then(|()| stream.write_all(bytes))
        .and_then(|()| stream.flush())
        .map_err(io_error)
}

pub fn send_request(
    address: std::net::SocketAddr,
    request: &[u8],
) -> Result<Vec<u8>, GatewayError> {
    let mut stream = TcpStream::connect(address).map_err(io_error)?;
    write_frame(&mut stream, request)?;
    read_frame(&mut stream)
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct GatewayStatus {
    version: u16,
    graph_id: String,
    graph_name: String,
    mode: String,
    catalog_revision: u64,
    schema_version: u64,
    topology_epoch: u64,
    backend_provider: String,
    backend_generation: u64,
    shards: Vec<GatewayShardStatus>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct BackendReplicaStatus {
    shard_id: u32,
    node_id: u64,
    provider: String,
    spi_version: u16,
    implementation: String,
    implementation_version: String,
    family: String,
    applied_log_index: u64,
}

impl BackendReplicaStatus {
    fn from_adapter(
        shard_id: u32,
        node_id: u64,
        provider: &str,
        adapter: &dyn StorageAdapter,
    ) -> Result<Self, GatewayError> {
        let descriptor = adapter.descriptor();
        Ok(Self {
            shard_id,
            node_id,
            provider: provider.to_owned(),
            spi_version: descriptor.spi_version(),
            implementation: descriptor.implementation().to_owned(),
            implementation_version: descriptor.implementation_version().to_owned(),
            family: backend_family_name(descriptor.family()).to_owned(),
            applied_log_index: adapter
                .applied_log_index()
                .map_err(|error| GatewayError::Backend(error.to_string()))?,
        })
    }
}

const fn backend_family_name(family: BackendFamily) -> &'static str {
    match family {
        BackendFamily::KeyValue => "key_value",
        BackendFamily::Sql => "sql",
        BackendFamily::PropertyGraph => "property_graph",
        BackendFamily::Test => "test",
    }
}

impl GatewayStatus {
    #[must_use]
    pub fn mode(&self) -> &str {
        &self.mode
    }

    #[must_use]
    pub fn backend_provider(&self) -> &str {
        &self.backend_provider
    }

    #[must_use]
    pub fn shards(&self) -> &[GatewayShardStatus] {
        &self.shards
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct GatewayShardStatus {
    shard_id: u32,
    placement_epoch: u64,
    leader_id: Option<u64>,
}

impl GatewayShardStatus {
    #[must_use]
    pub const fn shard_id(self) -> u32 {
        self.shard_id
    }

    #[must_use]
    pub const fn leader_id(self) -> Option<u64> {
        self.leader_id
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct GatewayRequest {
    pub version: u16,
    pub request_id: String,
    #[serde(flatten)]
    pub operation: GatewayOperation,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
pub enum GatewayOperation {
    Status,
    Query {
        text: String,
    },
    Transaction {
        schema_version: u64,
        ttl_micros: u64,
        mutations: Vec<ApiMutation>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ApiMutation {
    PutVertex {
        partition: u32,
        vertex_id: String,
        label_id: u32,
        valid_from_micros: i64,
        valid_to_micros: Option<i64>,
        payload_dtp1: String,
    },
    DeleteVertex {
        partition: u32,
        vertex_id: String,
        label_id: u32,
        valid_from_micros: i64,
        valid_to_micros: Option<i64>,
    },
    PutEdge {
        edge_partition: u32,
        edge_id: String,
        edge_type_id: u32,
        source_partition: u32,
        source_id: String,
        destination_partition: u32,
        destination_id: String,
        valid_from_micros: i64,
        valid_to_micros: Option<i64>,
        payload_dtp1: String,
    },
    DeleteEdge {
        edge_partition: u32,
        edge_id: String,
        edge_type_id: u32,
        source_partition: u32,
        source_id: String,
        destination_partition: u32,
        destination_id: String,
        valid_from_micros: i64,
        valid_to_micros: Option<i64>,
    },
}

impl ApiMutation {
    fn into_scoped(self, graph_id: u64) -> Result<ScopedTemporalTransaction, GatewayError> {
        let graph = GraphId::new(graph_id);
        match self {
            Self::PutVertex {
                partition,
                vertex_id,
                label_id,
                valid_from_micros,
                valid_to_micros,
                payload_dtp1,
            } => {
                let scope = scope(graph, partition);
                Ok(ScopedTemporalTransaction::new(
                    scope,
                    TemporalTransaction::new().with_vertex(VertexMutation::put(
                        scope.element(temporal_storage::ElementKind::Vertex, parse_id(&vertex_id)?),
                        LabelId::new(label_id),
                        interval(valid_from_micros, valid_to_micros)?,
                        decode_payload(&payload_dtp1)?,
                    )?),
                ))
            }
            Self::DeleteVertex {
                partition,
                vertex_id,
                label_id,
                valid_from_micros,
                valid_to_micros,
            } => {
                let scope = scope(graph, partition);
                Ok(ScopedTemporalTransaction::new(
                    scope,
                    TemporalTransaction::new().with_vertex(VertexMutation::delete(
                        scope.element(temporal_storage::ElementKind::Vertex, parse_id(&vertex_id)?),
                        LabelId::new(label_id),
                        interval(valid_from_micros, valid_to_micros)?,
                    )?),
                ))
            }
            Self::PutEdge {
                edge_partition,
                edge_id,
                edge_type_id,
                source_partition,
                source_id,
                destination_partition,
                destination_id,
                valid_from_micros,
                valid_to_micros,
                payload_dtp1,
            } => edge_scoped(
                graph,
                edge_partition,
                edge_id,
                edge_type_id,
                source_partition,
                source_id,
                destination_partition,
                destination_id,
                valid_from_micros,
                valid_to_micros,
                Some(payload_dtp1),
            ),
            Self::DeleteEdge {
                edge_partition,
                edge_id,
                edge_type_id,
                source_partition,
                source_id,
                destination_partition,
                destination_id,
                valid_from_micros,
                valid_to_micros,
            } => edge_scoped(
                graph,
                edge_partition,
                edge_id,
                edge_type_id,
                source_partition,
                source_id,
                destination_partition,
                destination_id,
                valid_from_micros,
                valid_to_micros,
                None,
            ),
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn edge_scoped(
    graph: GraphId,
    edge_partition: u32,
    edge_id: String,
    edge_type_id: u32,
    source_partition: u32,
    source_id: String,
    destination_partition: u32,
    destination_id: String,
    valid_from_micros: i64,
    valid_to_micros: Option<i64>,
    payload: Option<String>,
) -> Result<ScopedTemporalTransaction, GatewayError> {
    if edge_partition != source_partition {
        return Err(GatewayError::InvalidMutation(
            "edge_partition must equal source_partition".into(),
        ));
    }
    let scope = scope(graph, edge_partition);
    let element = ElementRef::edge(graph, scope.partition(), parse_id(&edge_id)?);
    let source = ElementRef::vertex(
        graph,
        PartitionId::new(source_partition),
        parse_id(&source_id)?,
    );
    let destination = ElementRef::vertex(
        graph,
        PartitionId::new(destination_partition),
        parse_id(&destination_id)?,
    );
    let valid = interval(valid_from_micros, valid_to_micros)?;
    let edge = match payload {
        Some(payload) => EdgeMutation::put_between(
            element,
            EdgeTypeId::new(edge_type_id),
            source,
            destination,
            valid,
            decode_payload(&payload)?,
        )?,
        None => EdgeMutation::delete_between(
            element,
            EdgeTypeId::new(edge_type_id),
            source,
            destination,
            valid,
        )?,
    };
    Ok(ScopedTemporalTransaction::new(
        scope,
        TemporalTransaction::new().with_edge(edge),
    ))
}

fn scope(graph: GraphId, partition: u32) -> GraphScope {
    GraphScope::new(graph, PartitionId::new(partition))
}

fn parse_id(value: &str) -> Result<ElementId, GatewayError> {
    value
        .parse::<u128>()
        .map(ElementId::new)
        .map_err(|_| GatewayError::InvalidMutation("element ID must be an unsigned integer".into()))
}

fn interval(start: i64, end: Option<i64>) -> Result<Interval<ValidTime>, GatewayError> {
    Interval::new(
        ValidTime::from_micros(start),
        end.map(ValidTime::from_micros),
    )
    .map_err(|error| GatewayError::InvalidMutation(error.to_string()))
}

fn decode_payload(value: &str) -> Result<CanonicalElement, GatewayError> {
    if !value.len().is_multiple_of(2) {
        return Err(GatewayError::InvalidMutation(
            "payload_dtp1 must contain even-length hexadecimal".into(),
        ));
    }
    let mut bytes = Vec::with_capacity(value.len() / 2);
    for pair in value.as_bytes().chunks_exact(2) {
        let pair = std::str::from_utf8(pair)
            .map_err(|_| GatewayError::InvalidMutation("payload_dtp1 is not hexadecimal".into()))?;
        bytes.push(u8::from_str_radix(pair, 16).map_err(|_| {
            GatewayError::InvalidMutation("payload_dtp1 is not hexadecimal".into())
        })?);
    }
    CanonicalElement::decode(&bytes)
        .map_err(|error| GatewayError::InvalidMutation(error.to_string()))
}

fn timestamp_json(timestamp: temporal_types::TransactionTime) -> Value {
    json!({
        "physical_micros": timestamp.physical_micros(),
        "logical": timestamp.logical(),
    })
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct GatewayResponse {
    version: u16,
    request_id: String,
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

impl GatewayResponse {
    fn success(request_id: String, result: Value) -> Self {
        Self {
            version: GATEWAY_API_VERSION,
            request_id,
            ok: true,
            result: Some(result),
            error: None,
        }
    }

    fn failure(request_id: String, error: String) -> Self {
        Self {
            version: GATEWAY_API_VERSION,
            request_id,
            ok: false,
            result: None,
            error: Some(error),
        }
    }
}

fn io_error(error: std::io::Error) -> GatewayError {
    GatewayError::Io(error.to_string())
}

struct ThreadWake(std::thread::Thread);

impl Wake for ThreadWake {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::from(Arc::new(ThreadWake(std::thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => std::thread::park(),
        }
    }
}

#[derive(Debug)]
pub enum GatewayError {
    Config(ConfigError),
    Catalog(CatalogError),
    Deployment(DeploymentError),
    Runtime(shard_runtime::ReplicationError),
    Routed(RoutedRuntimeError),
    Oracle(TimestampOracleError),
    Transaction(Box<TransactionCoordinatorError>),
    Temporal(temporal_storage::TemporalStoreError),
    Registry(RegistryError),
    UnsupportedApiVersion { version: u16 },
    InvalidRequestId,
    UnknownGraph { graph_id: u64 },
    GraphMismatch { expected: u64, actual: u64 },
    AlreadyInitialized,
    InvalidMutation(String),
    Query(String),
    SecretReference { reference: String },
    Backend(String),
    FrameTooLarge,
    Io(String),
}

impl Display for GatewayError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(error) => Display::fmt(error, formatter),
            Self::Catalog(error) => Display::fmt(error, formatter),
            Self::Deployment(error) => Display::fmt(error, formatter),
            Self::Runtime(error) => Display::fmt(error, formatter),
            Self::Routed(error) => Display::fmt(error, formatter),
            Self::Oracle(error) => Display::fmt(error, formatter),
            Self::Transaction(error) => Display::fmt(error, formatter),
            Self::Temporal(error) => Display::fmt(error, formatter),
            Self::Registry(error) => Display::fmt(error, formatter),
            Self::UnsupportedApiVersion { version } => {
                write!(formatter, "unsupported Gateway API version {version}")
            }
            Self::InvalidRequestId => formatter.write_str("invalid Gateway request ID"),
            Self::UnknownGraph { graph_id } => {
                write!(formatter, "graph {graph_id} is not in the catalog")
            }
            Self::GraphMismatch { expected, actual } => write!(
                formatter,
                "request graph {actual} differs from configured graph {expected}"
            ),
            Self::AlreadyInitialized => formatter.write_str("DTGProxy root is already initialized"),
            Self::InvalidMutation(message) => {
                write!(formatter, "invalid temporal mutation: {message}")
            }
            Self::Query(message) => write!(formatter, "query failed: {message}"),
            Self::SecretReference { reference } => {
                write!(
                    formatter,
                    "backend secret reference {reference:?} could not be resolved"
                )
            }
            Self::Backend(message) => write!(formatter, "backend validation failed: {message}"),
            Self::FrameTooLarge => {
                formatter.write_str("Gateway frame is empty or exceeds its size limit")
            }
            Self::Io(message) => write!(formatter, "Gateway I/O failed: {message}"),
        }
    }
}

impl Error for GatewayError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Config(error) => Some(error),
            Self::Catalog(error) => Some(error),
            Self::Deployment(error) => Some(error),
            Self::Runtime(error) => Some(error),
            Self::Routed(error) => Some(error),
            Self::Oracle(error) => Some(error),
            Self::Transaction(error) => Some(error),
            Self::Temporal(error) => Some(error),
            Self::Registry(error) => Some(error),
            Self::UnsupportedApiVersion { .. }
            | Self::InvalidRequestId
            | Self::UnknownGraph { .. }
            | Self::GraphMismatch { .. }
            | Self::AlreadyInitialized
            | Self::InvalidMutation(_)
            | Self::Query(_)
            | Self::SecretReference { .. }
            | Self::Backend(_)
            | Self::FrameTooLarge
            | Self::Io(_) => None,
        }
    }
}

impl From<ConfigError> for GatewayError {
    fn from(value: ConfigError) -> Self {
        Self::Config(value)
    }
}

impl From<CatalogError> for GatewayError {
    fn from(value: CatalogError) -> Self {
        Self::Catalog(value)
    }
}

impl From<DeploymentError> for GatewayError {
    fn from(value: DeploymentError) -> Self {
        Self::Deployment(value)
    }
}

impl From<shard_runtime::ReplicationError> for GatewayError {
    fn from(value: shard_runtime::ReplicationError) -> Self {
        Self::Runtime(value)
    }
}

impl From<RoutedRuntimeError> for GatewayError {
    fn from(value: RoutedRuntimeError) -> Self {
        Self::Routed(value)
    }
}

impl From<TimestampOracleError> for GatewayError {
    fn from(value: TimestampOracleError) -> Self {
        Self::Oracle(value)
    }
}

impl From<TransactionCoordinatorError> for GatewayError {
    fn from(value: TransactionCoordinatorError) -> Self {
        Self::Transaction(Box::new(value))
    }
}

impl From<temporal_storage::TemporalStoreError> for GatewayError {
    fn from(value: temporal_storage::TemporalStoreError) -> Self {
        Self::Temporal(value)
    }
}

impl From<RegistryError> for GatewayError {
    fn from(value: RegistryError) -> Self {
        Self::Registry(value)
    }
}
