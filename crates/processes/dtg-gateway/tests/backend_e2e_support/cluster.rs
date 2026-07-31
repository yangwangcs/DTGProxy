use std::env;
use std::fs::File;
use std::io;
use std::net::{SocketAddr, TcpListener as StdTcpListener};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use dtg_execution::cluster_protocol::proto::controller_service_client::ControllerServiceClient;
use dtg_execution::cluster_protocol::proto::data_service_client::DataServiceClient;
use dtg_execution::cluster_protocol::proto::meta_service_client::MetaServiceClient;
use dtg_execution::cluster_protocol::proto::{
    BoundedPayload, CatalogWatchRequest, ControlObservation, ExecutionFragment, RequestContext,
    ShardContext, StatusCode, TransactionOperation, TransactionRequest,
};
use dtg_execution::cluster_protocol::{PROTOCOL_MAJOR, SUPPORTED_MINOR_MAX, checksum_bytes};
use dtg_execution::shard::{CommitSingleShard, ShardCommand};
use dtg_execution::storage::{
    BackendClass, BindingRole, CapabilityManifest, CommandId, LogicalMutation, Properties,
    ProviderKind, ReplicaBinding, TransactionTime, ValidInterval, Value, Version, VertexId,
    VertexVersion,
};
use serde_json::json;
use tempfile::TempDir;
use tokio::net::TcpStream;

use super::{Backend, CellSpec, Workload};

const CAPABILITIES: &str = "adjacency,immutable-read-view,logical-snapshot,point";
const STARTUP_TIMEOUT: Duration = Duration::from_secs(15);
static NEXT_CELL_ID: AtomicU64 = AtomicU64::new(1);

pub struct DiagnosticRuntime {
    bin_dir: PathBuf,
    pub postgres_endpoint: String,
    pub postgres_credential: String,
    pub neo4j_endpoint: String,
    pub neo4j_username: String,
    pub neo4j_password: String,
}

impl DiagnosticRuntime {
    pub fn from_env() -> io::Result<Self> {
        let bin_dir = env::var_os("DTG_BACKEND_E2E_BIN_DIR")
            .map(PathBuf::from)
            .ok_or_else(|| invalid_input("DTG_BACKEND_E2E_BIN_DIR is required"))?;
        if !bin_dir.is_absolute() {
            return Err(invalid_input("DTG_BACKEND_E2E_BIN_DIR must be absolute"));
        }
        for binary in [
            "dtgproxy-meta",
            "dtgproxy-controller",
            "dtgproxy-data",
            "dtgproxy-gateway",
        ] {
            let path = bin_dir.join(binary);
            if !path.is_file() {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("required release binary is absent: {}", path.display()),
                ));
            }
        }
        Ok(Self {
            bin_dir,
            postgres_endpoint: environment_string("DTG_BACKEND_E2E_POSTGRES_ENDPOINT"),
            postgres_credential: environment_string("DTG_BACKEND_E2E_POSTGRES_CREDENTIAL"),
            neo4j_endpoint: environment_string("DTG_BACKEND_E2E_NEO4J_ENDPOINT"),
            neo4j_username: environment_string("DTG_BACKEND_E2E_NEO4J_USERNAME"),
            neo4j_password: environment_string("DTG_BACKEND_E2E_NEO4J_PASSWORD"),
        })
    }

    fn binary(&self, name: &str) -> PathBuf {
        self.bin_dir.join(name)
    }
}

pub struct DiagnosticCluster {
    root: TempDir,
    log_dir: PathBuf,
    gateway_binary: PathBuf,
    spec: CellSpec,
    meta_address: SocketAddr,
    data_address: SocketAddr,
    gateway_address: SocketAddr,
    binding: ReplicaBinding,
    children: Vec<ManagedChild>,
}

impl DiagnosticCluster {
    pub async fn start(runtime: &DiagnosticRuntime, spec: CellSpec) -> io::Result<Self> {
        let identity = CellIdentity::new(spec);
        let root = tempfile::Builder::new()
            .prefix(&format!("dtg-backend-e2e-{}-", identity.unique))
            .tempdir()?;
        let log_dir = root.path().join("logs");
        std::fs::create_dir_all(&log_dir)?;

        let meta_address = free_address()?;
        let controller_address = free_address()?;
        let data_address = free_address()?;
        let gateway_address = free_address()?;
        let meta_config = root.path().join("meta.json");
        let controller_config = root.path().join("controller.json");
        write_json(
            &meta_config,
            json!({
                "version": 1,
                "cluster_id": identity.cluster_id,
                "node_id": identity.meta_node_id,
                "listen_addr": meta_address,
                "data_directory": root.path().join("meta"),
                "analytics_lease_duration": 30,
                "consensus_namespace": format!("{}-meta", identity.namespace),
                "peers": [{
                    "node_id": identity.meta_node_id,
                    "raft_addr": meta_address,
                    "rpc_addr": meta_address
                }],
                "security": {"mode": "loopback_plaintext"}
            }),
        )?;
        write_json(
            &controller_config,
            json!({
                "version": 1,
                "cluster_id": identity.cluster_id,
                "node_id": identity.controller_node_id,
                "listen_addr": controller_address,
                "data_directory": root.path().join("controller"),
                "meta_endpoints": [format!("http://{meta_address}")],
                "security": {"mode": "loopback_plaintext"}
            }),
        )?;

        let binding = identity.binding(spec.backend)?;
        let mut cluster = Self {
            root,
            log_dir,
            gateway_binary: runtime.binary("dtgproxy-gateway"),
            spec,
            meta_address,
            data_address,
            gateway_address,
            binding,
            children: Vec::with_capacity(4),
        };
        cluster.spawn(
            "meta",
            runtime.binary("dtgproxy-meta"),
            &["--config".into(), meta_config.into_os_string()],
            &[],
        )?;
        cluster.wait_for_port(meta_address, "meta").await?;
        probe_meta(meta_address, identity.request_id(1)).await?;

        cluster.spawn(
            "controller",
            runtime.binary("dtgproxy-controller"),
            &["--config".into(), controller_config.into_os_string()],
            &[],
        )?;
        cluster
            .wait_for_port(controller_address, "controller")
            .await?;
        probe_controller(controller_address, identity.request_id(2)).await?;

        let assignment = assignment_spec(&cluster.binding);
        let mut data_environment = vec![
            ("DTG_DATA_RPC_ADDR", data_address.to_string()),
            (
                "DTG_DATA_FJALL_ROOT",
                cluster
                    .root
                    .path()
                    .join("data/business")
                    .display()
                    .to_string(),
            ),
            (
                "DTG_DATA_CONSENSUS_ROOT",
                cluster.root.path().join("data/raft").display().to_string(),
            ),
            ("DTG_DATA_CAPABILITIES", CAPABILITIES.into()),
            ("DTG_DATA_ASSIGNMENTS", assignment),
        ];
        match spec.backend {
            Backend::Fjall => {}
            Backend::PostgreSql => {
                data_environment.push((
                    "DTG_DATA_POSTGRES_ENDPOINT",
                    runtime.postgres_endpoint.clone(),
                ));
                data_environment.push((
                    "DTG_DATA_POSTGRES_CREDENTIAL",
                    runtime.postgres_credential.clone(),
                ));
            }
            Backend::Neo4j => {
                data_environment.push(("DTG_DATA_NEO4J_ENDPOINT", runtime.neo4j_endpoint.clone()));
                data_environment.push(("DTG_DATA_NEO4J_DATABASE", "neo4j".into()));
                data_environment.push(("DTG_DATA_NEO4J_USERNAME", runtime.neo4j_username.clone()));
                data_environment.push(("DTG_DATA_NEO4J_PASSWORD", runtime.neo4j_password.clone()));
            }
        }
        cluster.spawn(
            "data",
            runtime.binary("dtgproxy-data"),
            &[],
            &data_environment,
        )?;
        cluster.wait_for_port(data_address, "data").await?;

        if spec.workload.is_write() {
            let applied_index = cluster
                .observe_applied_index(identity.request_id(3))
                .await?;
            cluster.start_gateway(applied_index)?;
            cluster.wait_for_port(gateway_address, "gateway").await?;
        }
        Ok(cluster)
    }

    pub async fn seed_read_dataset(&mut self, vertices: u64) -> io::Result<()> {
        if self.spec.workload.is_write() {
            return Err(invalid_input("read data cannot be seeded for a write cell"));
        }
        if self.children.iter().any(|child| child.name == "gateway") {
            return Err(invalid_input("read data was already seeded for this cell"));
        }
        let mutations = (1..=vertices)
            .map(|id| {
                let mut properties = Properties::new();
                properties.insert(
                    "id".into(),
                    Value::Integer(i64::try_from(id).map_err(|_| {
                        invalid_input("read dataset vertex id exceeds signed integer range")
                    })?),
                );
                let vertex = VertexVersion::new(
                    VertexId::new(u128::from(id)).map_err(invalid_data)?,
                    Version::new(1),
                    ValidInterval::new(1, 10_000).map_err(invalid_data)?,
                    TransactionTime::new(41).map_err(invalid_data)?,
                    properties,
                )
                .map_err(invalid_data)?;
                Ok(LogicalMutation::PutVertex(vertex))
            })
            .collect::<io::Result<Vec<_>>>()?;
        let command_id = request_seed(self.binding.namespace_id().as_str());
        let command = ShardCommand::CommitSingleShard(
            CommitSingleShard::new(
                CommandId::new(command_id).map_err(invalid_data)?,
                self.binding.placement_epoch().get(),
                self.binding.backend_generation().get(),
                mutations,
            )
            .map_err(invalid_data)?,
        );
        let body = command.encode_current().map_err(invalid_data)?;
        let mut client = DataServiceClient::connect(format!("http://{}", self.data_address))
            .await
            .map_err(io_other)?;
        let status = client
            .apply_transaction(TransactionRequest {
                context: Some(shard_context(&self.binding, command_id)),
                transaction_id: command_id.to_be_bytes().to_vec(),
                operation: TransactionOperation::Commit.into(),
                idempotency_key: command_id.to_be_bytes().to_vec(),
                payload: Some(payload(
                    body,
                    u32::try_from(vertices).map_err(|_| {
                        invalid_input("read dataset size exceeds protocol item count")
                    })?,
                )),
            })
            .await
            .map_err(io_other)?
            .into_inner();
        if status.code != StatusCode::Ok as i32 {
            return Err(invalid_data(format!(
                "Data seed failed with status {}: {}",
                status.code, status.message
            )));
        }
        let applied_index = self
            .observe_applied_index(command_id.saturating_add(1))
            .await?;
        self.start_gateway(applied_index)?;
        self.wait_for_port(self.gateway_address, "gateway").await
    }

    pub const fn bolt_address(&self) -> SocketAddr {
        self.gateway_address
    }

    pub async fn shutdown(&mut self) -> io::Result<()> {
        retire_children(&mut self.children)
    }

    async fn observe_applied_index(&self, request_id: u128) -> io::Result<u64> {
        let mut candidate = 1_u64;
        for _ in 0..4 {
            let result = probe_read_fence(
                self.data_address,
                &self.binding,
                candidate,
                request_id.saturating_add(u128::from(candidate)),
            )
            .await;
            match result {
                Ok(()) => return Ok(candidate),
                Err(error) => {
                    let Some(observed) = parse_applied_index(&error.to_string()) else {
                        return Err(error);
                    };
                    if observed == 0 || observed == candidate {
                        return Err(error);
                    }
                    candidate = observed;
                }
            }
        }
        Err(invalid_data("Data applied index did not stabilize"))
    }

    fn start_gateway(&mut self, applied_index: u64) -> io::Result<()> {
        let shard = gateway_shard_spec(&self.binding, applied_index);
        let logical_scan_bound = match self.spec.workload {
            Workload::PointLookup => "1",
            Workload::CreateVertex | Workload::CountVertices => "4096",
        };
        let environment = vec![
            ("DTG_GATEWAY_BIND", self.gateway_address.to_string()),
            (
                "DTG_GATEWAY_CLUSTER_ID",
                self.binding.cluster_id().get().to_string(),
            ),
            ("DTG_GATEWAY_REQUEST_TIMEOUT_MS", "10000".into()),
            (
                "DTG_GATEWAY_CLUSTER_ENDPOINT",
                format!("http://{}", self.data_address),
            ),
            (
                "DTG_GATEWAY_META_ENDPOINT",
                format!("http://{}", self.meta_address),
            ),
            (
                "DTG_GATEWAY_GRAPH_ID",
                self.binding.graph_id().get().to_string(),
            ),
            ("DTG_GATEWAY_CATALOG_VERSION", "31".into()),
            ("DTG_GATEWAY_SCHEMA_VERSION", "31".into()),
            ("DTG_GATEWAY_TRANSACTION_TIME", "41".into()),
            ("DTG_GATEWAY_VALID_AT", "10".into()),
            ("DTG_GATEWAY_LOGICAL_SCAN_BOUND", logical_scan_bound.into()),
            ("DTG_GATEWAY_CAPABILITIES", CAPABILITIES.into()),
            ("DTG_GATEWAY_SHARDS", shard),
        ];
        self.spawn("gateway", self.gateway_binary.clone(), &[], &environment)
    }

    fn spawn(
        &mut self,
        name: &'static str,
        binary: PathBuf,
        arguments: &[std::ffi::OsString],
        environment: &[(&str, String)],
    ) -> io::Result<()> {
        let log_path = self.log_dir.join(format!("{name}.log"));
        let log = File::create(&log_path)?;
        let child = Command::new(&binary)
            .args(arguments)
            .envs(environment.iter().map(|(name, value)| (*name, value)))
            .stdout(Stdio::from(log.try_clone()?))
            .stderr(Stdio::from(log))
            .spawn()
            .map_err(|error| {
                io::Error::new(
                    error.kind(),
                    format!("failed to start {}: {error}", binary.display()),
                )
            })?;
        println!("DTG_BACKEND_E2E_CHILD_STARTED={name}:{}", child.id());
        self.children.push(ManagedChild {
            name,
            log_path,
            child,
        });
        Ok(())
    }

    async fn wait_for_port(&mut self, address: SocketAddr, name: &str) -> io::Result<()> {
        let deadline = tokio::time::Instant::now() + STARTUP_TIMEOUT;
        loop {
            self.ensure_running(name)?;
            if TcpStream::connect(address).await.is_ok() {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!("{name} did not open {address}"),
                ));
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    fn ensure_running(&mut self, name: &str) -> io::Result<()> {
        let child = self
            .children
            .iter_mut()
            .find(|child| child.name == name)
            .ok_or_else(|| invalid_data(format!("managed child {name} is absent")))?;
        if let Some(status) = child.child.try_wait()? {
            let log = std::fs::read_to_string(&child.log_path).unwrap_or_default();
            return Err(invalid_data(format!(
                "{name} exited with {status}; log follows:\n{log}"
            )));
        }
        Ok(())
    }
}

impl Drop for DiagnosticCluster {
    fn drop(&mut self) {
        let _ = retire_children(&mut self.children);
    }
}

impl CellSpec {
    pub const fn one(
        backend: Backend,
        workload: Workload,
        concurrency: usize,
        repetition: u8,
    ) -> Self {
        Self {
            backend,
            workload,
            concurrency,
            repetition,
        }
    }
}

struct ManagedChild {
    name: &'static str,
    log_path: PathBuf,
    child: Child,
}

struct CellIdentity {
    unique: u64,
    cluster_id: u64,
    graph_id: u64,
    shard_id: u64,
    placement_epoch: u64,
    replica_id: u64,
    backend_generation: u64,
    meta_node_id: u64,
    controller_node_id: u64,
    namespace: String,
}

impl CellIdentity {
    fn new(spec: CellSpec) -> Self {
        let clock = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        let unique = clock
            ^ u64::from(std::process::id()).rotate_left(17)
            ^ NEXT_CELL_ID.fetch_add(1, Ordering::Relaxed).rotate_left(31)
            ^ u64::from(spec.repetition).rotate_left(47);
        let base = 1_000_000 + unique % 1_000_000_000;
        Self {
            unique,
            cluster_id: base,
            graph_id: base.saturating_add(1),
            shard_id: 1 + base % 1_000_000,
            placement_epoch: base.saturating_add(3),
            replica_id: base.saturating_add(4),
            backend_generation: base.saturating_add(5),
            meta_node_id: base.saturating_add(6),
            controller_node_id: base.saturating_add(7),
            namespace: format!(
                "backend-e2e-{}-r{}-{unique}",
                backend_name(spec.backend),
                spec.repetition
            ),
        }
    }

    fn binding(&self, backend: Backend) -> io::Result<ReplicaBinding> {
        let capabilities =
            CapabilityManifest::from_names(CAPABILITIES.split(',')).map_err(invalid_data)?;
        let provider = provider_kind(backend);
        let class = BackendClass::new(
            provider.clone(),
            1,
            1,
            capabilities.names().map(str::to_owned),
        )
        .map_err(invalid_data)?;
        ReplicaBinding::builder()
            .cluster_id(self.cluster_id)
            .graph_id(self.graph_id)
            .shard_id(self.shard_id)
            .placement_epoch(self.placement_epoch)
            .replica_id(self.replica_id)
            .backend_generation(self.backend_generation)
            .backend_class_digest(class.digest())
            .provider_kind(provider)
            .contract_version(1)
            .layout_version(1)
            .capability_digest(capabilities.digest())
            .namespace_id(&self.namespace)
            .endpoint_profile_ref("environment-bootstrap")
            .credential_ref("environment-bootstrap")
            .role(BindingRole::Active)
            .build()
            .map_err(invalid_data)
    }

    fn request_id(&self, suffix: u64) -> u128 {
        (u128::from(self.unique) << 64) | u128::from(suffix)
    }
}

async fn probe_meta(address: SocketAddr, request_id: u128) -> io::Result<()> {
    let mut client = MetaServiceClient::connect(format!("http://{address}"))
        .await
        .map_err(io_other)?;
    let mut stream = client
        .watch_catalog(CatalogWatchRequest {
            request: Some(request_context(1, request_id)),
            after_revision: 0,
        })
        .await
        .map_err(io_other)?
        .into_inner();
    let snapshot = tokio::time::timeout(Duration::from_secs(5), stream.message())
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "Meta catalog probe timed out"))?
        .map_err(io_other)?;
    if snapshot.is_none() {
        return Err(invalid_data("Meta catalog probe ended without a snapshot"));
    }
    Ok(())
}

async fn probe_controller(address: SocketAddr, request_id: u128) -> io::Result<()> {
    let body =
        serde_json::to_vec(&json!({"catalog_version": 0, "replicas": []})).map_err(io_other)?;
    let node_id = (request_id as u64) ^ ((request_id >> 64) as u64);
    let mut client = ControllerServiceClient::connect(format!("http://{address}"))
        .await
        .map_err(io_other)?;
    let status = client
        .observe(ControlObservation {
            request: Some(request_context(1, request_id)),
            node_id: format!("{node_id:016x}").into_bytes(),
            observation_version: 1,
            observed_at_unix_ms: unix_time_millis(),
            payload: Some(payload(body, 1)),
        })
        .await
        .map_err(io_other)?
        .into_inner();
    if status.code != StatusCode::Ok as i32 {
        return Err(invalid_data(format!(
            "Controller probe failed with status {}: {}",
            status.code, status.message
        )));
    }
    Ok(())
}

async fn probe_read_fence(
    address: SocketAddr,
    binding: &ReplicaBinding,
    applied_index: u64,
    request_id: u128,
) -> io::Result<()> {
    let body = point_fragment_body(1);
    let mut client = DataServiceClient::connect(format!("http://{address}"))
        .await
        .map_err(io_other)?;
    let mut stream = client
        .execute_fragment(ExecutionFragment {
            context: Some(shard_context(binding, request_id)),
            fragment_id: request_id.to_be_bytes().to_vec(),
            payload: Some(payload(body, 1)),
            schema_version: 31,
            capability_digest: binding.capability_digest().get().to_vec(),
            applied_index,
            transaction_time: 41,
            valid_at: 10,
            snapshot_immutable: true,
        })
        .await
        .map_err(io_other)?
        .into_inner();
    while stream.message().await.map_err(io_other)?.is_some() {}
    Ok(())
}

fn point_fragment_body(vertex_id: u128) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&1_u64.to_be_bytes());
    body.extend_from_slice(&1_u32.to_be_bytes());
    body.extend_from_slice(&0_u32.to_be_bytes());
    body.extend_from_slice(&1_u32.to_be_bytes());
    body.extend_from_slice(&1_u32.to_be_bytes());
    body.push(0);
    body.push(0);
    body.extend_from_slice(&vertex_id.to_be_bytes());
    body
}

fn request_context(cluster_id: u64, request_id: u128) -> RequestContext {
    RequestContext {
        protocol_major: PROTOCOL_MAJOR,
        protocol_minor: SUPPORTED_MINOR_MAX,
        cluster_id: cluster_id.to_be_bytes().to_vec(),
        request_id: request_id.to_be_bytes().to_vec(),
        deadline_unix_ms: unix_time_millis().saturating_add(10_000),
        trace_context: Vec::new(),
    }
}

fn shard_context(binding: &ReplicaBinding, request_id: u128) -> ShardContext {
    ShardContext {
        request: Some(request_context(binding.cluster_id().get(), request_id)),
        graph_id: binding.graph_id().get(),
        shard_id: u32::try_from(binding.shard_id().get()).unwrap_or(u32::MAX),
        placement_epoch: binding.placement_epoch().get(),
        backend_generation: binding.backend_generation().get(),
        catalog_version: 31,
    }
}

fn payload(body: Vec<u8>, item_count: u32) -> BoundedPayload {
    BoundedPayload {
        format_version: 1,
        declared_len: body.len() as u64,
        item_count,
        checksum: checksum_bytes(&body).to_vec(),
        body,
    }
}

fn assignment_spec(binding: &ReplicaBinding) -> String {
    format!(
        "{}:{}:{}:{}:{}:{}:{}:1:1:{}",
        binding.cluster_id().get(),
        binding.graph_id().get(),
        binding.shard_id().get(),
        binding.placement_epoch().get(),
        binding.replica_id().get(),
        binding.backend_generation().get(),
        backend_name_from_provider(binding.provider_kind()),
        binding.namespace_id().as_str()
    )
}

fn gateway_shard_spec(binding: &ReplicaBinding, applied_index: u64) -> String {
    format!(
        "{}:{}:{}:{}:{}:{}:1:1:{}",
        binding.shard_id().get(),
        binding.placement_epoch().get(),
        binding.replica_id().get(),
        binding.backend_generation().get(),
        applied_index,
        backend_name_from_provider(binding.provider_kind()),
        binding.namespace_id().as_str()
    )
}

fn provider_kind(backend: Backend) -> ProviderKind {
    match backend {
        Backend::Fjall => ProviderKind::Fjall,
        Backend::PostgreSql => ProviderKind::PostgreSql,
        Backend::Neo4j => ProviderKind::Neo4j,
    }
}

const fn backend_name(backend: Backend) -> &'static str {
    match backend {
        Backend::Fjall => "fjall",
        Backend::PostgreSql => "postgresql",
        Backend::Neo4j => "neo4j",
    }
}

fn backend_name_from_provider(provider: &ProviderKind) -> &'static str {
    match provider {
        ProviderKind::Fjall => "fjall",
        ProviderKind::PostgreSql => "postgresql",
        ProviderKind::Neo4j => "neo4j",
        ProviderKind::Remote(_) => "remote",
    }
}

fn retire_children(children: &mut Vec<ManagedChild>) -> io::Result<()> {
    let mut first_error = None;
    for managed in children.iter_mut().rev() {
        match managed.child.try_wait() {
            Ok(Some(_)) => {}
            Ok(None) => {
                if let Err(error) = managed.child.kill()
                    && first_error.is_none()
                {
                    first_error = Some(error);
                }
            }
            Err(error) if first_error.is_none() => first_error = Some(error),
            Err(_) => {}
        }
    }
    for mut managed in children.drain(..).rev() {
        let pid = managed.child.id();
        match managed.child.wait() {
            Ok(_) => println!("DTG_BACKEND_E2E_CHILD_RETIRED={}:{}", managed.name, pid),
            Err(error) if first_error.is_none() => first_error = Some(error),
            Err(_) => {}
        }
    }
    first_error.map_or(Ok(()), Err)
}

fn parse_applied_index(message: &str) -> Option<u64> {
    message
        .split("applied index ")
        .nth(1)?
        .split(|character: char| !character.is_ascii_digit())
        .next()?
        .parse()
        .ok()
}

fn request_seed(namespace: &str) -> u128 {
    let mut value = 0xcbf2_9ce4_8422_2325_u128;
    for byte in namespace.bytes() {
        value ^= u128::from(byte);
        value = value.wrapping_mul(0x100_0000_01b3);
    }
    value.max(1)
}

fn free_address() -> io::Result<SocketAddr> {
    let listener = StdTcpListener::bind("127.0.0.1:0")?;
    listener.local_addr()
}

fn write_json(path: &Path, value: serde_json::Value) -> io::Result<()> {
    let bytes = serde_json::to_vec_pretty(&value).map_err(io_other)?;
    std::fs::write(path, bytes)
}

fn environment_string(name: &str) -> String {
    env::var(name).unwrap_or_default()
}

fn unix_time_millis() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(u64::MAX)
}

fn invalid_input(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

fn invalid_data(error: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error.to_string())
}

fn io_other(error: impl std::fmt::Display) -> io::Error {
    io::Error::other(error.to_string())
}
