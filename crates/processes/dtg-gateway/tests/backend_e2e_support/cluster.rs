use std::collections::BTreeMap;
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
    RetryDisposition, ShardContext, StatusCode, TransactionOperation, TransactionRequest,
    TypedStatus,
};
use dtg_execution::cluster_protocol::{
    PROTOCOL_MAJOR, SUPPORTED_MINOR_MAX, checksum_bytes, validate_typed_status,
};
use dtg_execution::shard::{CommitSingleShard, ShardCommand};
use dtg_execution::storage::{
    BackendClass, BindingRole, CapabilityManifest, CommandId, EdgeId, EdgeVersion, LogicalMutation,
    Properties, ProviderKind, ReplicaBinding, TransactionTime, ValidInterval, Value, Version,
    VertexId, VertexVersion,
};
use serde_json::json;
use tempfile::TempDir;
use tokio::net::TcpStream;

use super::{
    Backend, BoltSession, BoltValue, CellSpec, ProcessMetricsSnapshot, RawObservation,
    StageMetricsWindow, Workload, stage_metrics_window_from_log,
};

const CAPABILITIES: &str = "adjacency,immutable-read-view,logical-snapshot,point";
const STARTUP_TIMEOUT: Duration = Duration::from_secs(15);
const POST_MEASUREMENT_METRICS_WAIT: Duration = Duration::from_millis(1_250);
static NEXT_CELL_ID: AtomicU64 = AtomicU64::new(1);

pub struct DiagnosticRuntime {
    bin_dir: PathBuf,
    pub postgres_endpoint: String,
    pub postgres_credential: String,
    gateway_data_uds: bool,
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
            gateway_data_uds: env::var("DTG_BACKEND_E2E_GATEWAY_DATA_UDS")
                .is_ok_and(|value| value != "0"),
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
    gateway_data_endpoint: String,
    gateway_address: SocketAddr,
    binding: ReplicaBinding,
    children: Vec<ManagedChild>,
    process_logs: BTreeMap<&'static str, PathBuf>,
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
        let gateway_unix_socket = runtime.gateway_data_uds.then(|| {
            std::env::temp_dir().join(format!("dtgproxy-{}-gateway.sock", identity.unique))
        });
        if let Some(path) = gateway_unix_socket.as_deref()
            && let Some(parent) = path.parent()
        {
            std::fs::create_dir_all(parent)?;
        }
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
            gateway_data_endpoint: gateway_data_endpoint(
                data_address,
                gateway_unix_socket.as_deref(),
            ),
            gateway_address,
            binding,
            children: Vec::with_capacity(4),
            process_logs: BTreeMap::new(),
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
        let mut data_environment = diagnostic_data_environment(
            cluster.root.path(),
            &data_address.to_string(),
            backend_name(spec.backend),
            assignment,
        );
        if let Some(path) = gateway_unix_socket.as_deref() {
            data_environment.push(("DTG_DATA_GATEWAY_UNIX_SOCKET", path.display().to_string()));
        }
        if let Some(limit) = env::var_os("DTG_BACKEND_E2E_DATA_PIPELINE_EXECUTION_LIMIT") {
            data_environment.push((
                "DTG_DATA_GATEWAY_PIPELINE_EXECUTION_LIMIT",
                limit.to_string_lossy().into_owned(),
            ));
        }
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
            Backend::Kuzu => {}
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
        if matches!(
            self.spec.workload,
            Workload::OneHopExpand | Workload::TwoHopExpand
        ) && vertices < 4_096
        {
            return Err(invalid_input(
                "expand dataset requires at least 4096 vertices",
            ));
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
        let edge_mutations = match self.spec.workload {
            Workload::OneHopExpand => vec![bench_edge(1, 2048, 4096)?],
            Workload::TwoHopExpand => vec![bench_edge(1, 2048, 3072)?, bench_edge(2, 3072, 4096)?],
            Workload::CreateVertex | Workload::PointLookup | Workload::CountVertices => Vec::new(),
        };
        let command_id = request_seed(self.binding.namespace_id().as_str());
        let mut client = DataServiceClient::connect(format!("http://{}", self.data_address))
            .await
            .map_err(io_other)?;
        let applied_index = self
            .apply_seed_batch(&mut client, command_id, mutations)
            .await?;
        let applied_index = if edge_mutations.is_empty() {
            applied_index
        } else {
            let edge_command_id = command_id.saturating_add(1);
            self.apply_seed_batch(&mut client, edge_command_id, edge_mutations)
                .await?
        };
        self.start_gateway(applied_index)?;
        self.wait_for_port(self.gateway_address, "gateway").await
    }

    pub const fn bolt_address(&self) -> SocketAddr {
        self.gateway_address
    }

    async fn apply_seed_batch(
        &self,
        client: &mut DataServiceClient<tonic::transport::Channel>,
        command_id: u128,
        mutations: Vec<LogicalMutation>,
    ) -> io::Result<u64> {
        let item_count = u32::try_from(mutations.len())
            .map_err(|_| invalid_input("read dataset size exceeds protocol item count"))?;
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
        let status = client
            .apply_transaction(TransactionRequest {
                context: Some(shard_context(&self.binding, command_id)),
                transaction_id: command_id.to_be_bytes().to_vec(),
                operation: TransactionOperation::Commit.into(),
                idempotency_key: command_id.to_be_bytes().to_vec(),
                payload: Some(payload(body, item_count)),
            })
            .await
            .map_err(io_other)?
            .into_inner();
        seed_applied_index(status)
    }

    pub async fn measure_cell(&self, spec: CellSpec) -> io::Result<RawObservation> {
        let mut observation = super::measure_cell(self.bolt_address(), spec).await?;
        let (gateway_stage_metrics, data_stage_metrics) = tokio::try_join!(
            self.stage_metrics_window("gateway", &observation),
            self.stage_metrics_window("data", &observation),
        )?;
        observation.gateway_stage_metrics = Some(gateway_stage_metrics);
        observation.data_stage_metrics = Some(data_stage_metrics);
        if spec.workload.is_write() {
            observation.persisted_operations = self.persisted_vertex_count().await?;
            let accepted_operations = observation
                .operations
                .saturating_add(observation.warmup_operations);
            if observation.persisted_operations != accepted_operations {
                return Err(invalid_data(format!(
                    "write persistence verification failed: accepted {accepted_operations} CREATE operations but COUNT(*) returned {}",
                    observation.persisted_operations,
                )));
            }
        }
        observation.finished_at_unix_ns = unix_time_nanos();
        Ok(observation)
    }

    async fn persisted_vertex_count(&self) -> io::Result<u64> {
        let mut session = BoltSession::connect(self.bolt_address()).await?;
        let result = session
            .run("MATCH (n) RETURN COUNT(*)", BTreeMap::new())
            .await?;
        match result.rows.as_slice() {
            [row] => match row.as_slice() {
                [BoltValue::Integer(count)] if *count >= 0 => u64::try_from(*count)
                    .map_err(|_| invalid_data("persisted vertex count exceeds u64")),
                _ => Err(invalid_data(
                    "write persistence verification did not return a single non-negative COUNT(*)",
                )),
            },
            _ => Err(invalid_data(
                "write persistence verification did not return a single non-negative COUNT(*)",
            )),
        }
    }

    pub async fn measure_pipeline_cell(
        &self,
        spec: CellSpec,
        depth: usize,
    ) -> io::Result<RawObservation> {
        let mut observation = super::measure_pipeline_cell_with_durations(
            self.bolt_address(),
            spec,
            depth,
            Duration::from_secs(1),
            Duration::from_secs(5),
        )
        .await?;
        let (gateway_stage_metrics, data_stage_metrics) = tokio::try_join!(
            self.stage_metrics_window("gateway", &observation),
            self.stage_metrics_window("data", &observation),
        )?;
        observation.gateway_stage_metrics = Some(gateway_stage_metrics);
        observation.data_stage_metrics = Some(data_stage_metrics);
        observation.finished_at_unix_ns = unix_time_nanos();
        Ok(observation)
    }

    pub fn last_request_metrics_line(&self, process: &str) -> io::Result<String> {
        const PREFIX: &str = "DTG_REQUEST_STAGE_METRICS=";
        std::fs::read_to_string(self.process_log_path(process)?)?
            .lines()
            .rev()
            .find_map(|line| line.strip_prefix(PREFIX).map(str::to_owned))
            .ok_or_else(|| invalid_data(format!("{process} did not export request metrics")))
    }

    pub async fn next_request_metrics_snapshot(
        &self,
        process: &str,
        after_sequence: Option<u64>,
    ) -> io::Result<ProcessMetricsSnapshot> {
        let deadline = tokio::time::Instant::now() + POST_MEASUREMENT_METRICS_WAIT;
        loop {
            match self.last_request_metrics_line(process) {
                Ok(line) => {
                    let snapshot =
                        serde_json::from_str::<ProcessMetricsSnapshot>(&line).map_err(|error| {
                            invalid_data(format!(
                                "{process} exported invalid request metrics: {error}"
                            ))
                        })?;
                    if after_sequence.is_none_or(|sequence| snapshot.sequence > sequence) {
                        return Ok(snapshot);
                    }
                }
                Err(error)
                    if error.kind() == io::ErrorKind::InvalidData
                        && error.to_string().contains("did not export request metrics") => {}
                Err(error) => return Err(error),
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(invalid_data(format!(
                    "{process} did not export a newer request metrics snapshot"
                )));
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
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
            Workload::PointLookup
            | Workload::OneHopExpand
            | Workload::TwoHopExpand
            | Workload::CountVertices => "4096",
            Workload::CreateVertex => "1",
        };
        let mut environment = vec![
            ("DTG_GATEWAY_BIND", self.gateway_address.to_string()),
            (
                "DTG_GATEWAY_CLUSTER_ID",
                self.binding.cluster_id().get().to_string(),
            ),
            ("DTG_GATEWAY_REQUEST_TIMEOUT_MS", "10000".into()),
            (
                "DTG_GATEWAY_CLUSTER_ENDPOINT",
                self.gateway_data_endpoint.clone(),
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
        if env::var_os("DTG_BACKEND_E2E_DISABLE_QUERY_SESSIONS").is_some() {
            environment.push(("DTG_GATEWAY_QUERY_SESSIONS", "0".into()));
        }
        if env::var_os("DTG_BACKEND_E2E_DISABLE_QUERY_PIPELINE").is_some() {
            environment.push(("DTG_GATEWAY_QUERY_PIPELINE", "0".into()));
        }
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
            log_path: log_path.clone(),
            child,
        });
        self.process_logs.insert(name, log_path);
        Ok(())
    }

    async fn stage_metrics_window(
        &self,
        process: &str,
        observation: &RawObservation,
    ) -> io::Result<StageMetricsWindow> {
        let log_path = self.process_log_path(process)?.to_owned();
        let deadline = tokio::time::Instant::now() + POST_MEASUREMENT_METRICS_WAIT;
        loop {
            let log = std::fs::read_to_string(&log_path)?;
            match stage_metrics_window_from_log(
                &log,
                process,
                observation.measurement_started_at_unix_ns,
                observation.measurement_finished_at_unix_ns,
            ) {
                // The exporter is periodic and may be delayed while the process is
                // saturated. The parser already guarantees that the selected pair
                // brackets the measurement window; a late cumulative snapshot is
                // still valid because no requests are issued after measurement ends.
                Ok(window) => return Ok(window),
                Err(error)
                    if error.kind() == io::ErrorKind::InvalidData
                        && error
                            .to_string()
                            .contains("lack a post-measurement snapshot")
                        && tokio::time::Instant::now() < deadline =>
                {
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
                Err(error) => return Err(error),
            }
        }
    }

    fn process_log_path(&self, process: &str) -> io::Result<&Path> {
        self.process_logs
            .get(process)
            .map(PathBuf::as_path)
            .ok_or_else(|| invalid_input(format!("managed child {process} is absent")))
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

fn bench_edge(id: u128, source: u128, target: u128) -> io::Result<LogicalMutation> {
    Ok(LogicalMutation::PutEdge(
        EdgeVersion::new(
            EdgeId::new(id).map_err(invalid_data)?,
            VertexId::new(source).map_err(invalid_data)?,
            VertexId::new(target).map_err(invalid_data)?,
            "BENCH",
            Version::new(1),
            ValidInterval::new(1, 10_000).map_err(invalid_data)?,
            TransactionTime::new(41).map_err(invalid_data)?,
            Properties::new(),
        )
        .map_err(invalid_data)?,
    ))
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
    body.push(0);
    body.push(0);
    body.extend_from_slice(&1_u32.to_be_bytes());
    body.extend_from_slice(&0_u32.to_be_bytes());
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
        Backend::Kuzu => ProviderKind::Kuzu,
    }
}

const fn backend_name(backend: Backend) -> &'static str {
    match backend {
        Backend::Fjall => "fjall",
        Backend::PostgreSql => "postgresql",
        Backend::Kuzu => "kuzu",
    }
}

fn backend_name_from_provider(provider: &ProviderKind) -> &'static str {
    match provider {
        ProviderKind::Fjall => "fjall",
        ProviderKind::PostgreSql => "postgresql",
        ProviderKind::Kuzu => "kuzu",
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

fn seed_applied_index(status: TypedStatus) -> io::Result<u64> {
    if status.code != StatusCode::Ok as i32 {
        return Err(invalid_data(format!(
            "Data seed failed with status {}: {}",
            status.code, status.message
        )));
    }
    let status = validate_typed_status(status).map_err(invalid_data)?;
    let details = status
        .details()
        .ok_or_else(|| invalid_data("Data seed status is missing the Raft receipt"))?;
    if details.item_count() != 1 || details.body().len() != 9 {
        return Err(invalid_data("Data seed Raft receipt has an invalid shape"));
    }
    let applied_index = u64::from_be_bytes(
        details.body()[..8]
            .try_into()
            .map_err(|_| invalid_data("Data seed Raft receipt has an invalid index"))?,
    );
    if applied_index == 0 || !matches!(details.body()[8], 0 | 1) {
        return Err(invalid_data("Data seed Raft receipt is invalid"));
    }
    Ok(applied_index)
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

fn gateway_data_endpoint(address: SocketAddr, socket: Option<&Path>) -> String {
    socket.map_or_else(
        || format!("http://{address}"),
        |path| format!("unix://{}", path.display()),
    )
}

fn diagnostic_data_environment(
    root: &Path,
    rpc_address: &str,
    backend_kind: &str,
    assignment: String,
) -> Vec<(&'static str, String)> {
    vec![
        ("DTG_DATA_RPC_ADDR", rpc_address.into()),
        ("DTG_DATA_BACKEND_KIND", backend_kind.into()),
        (
            "DTG_DATA_FJALL_ROOT",
            root.join("data/business").display().to_string(),
        ),
        (
            "DTG_DATA_KUZU_ROOT",
            root.join("data/kuzu").display().to_string(),
        ),
        (
            "DTG_DATA_CONSENSUS_ROOT",
            root.join("data/raft").display().to_string(),
        ),
        ("DTG_DATA_CAPABILITIES", CAPABILITIES.into()),
        ("DTG_DATA_ASSIGNMENTS", assignment),
    ]
}

#[test]
fn gateway_data_endpoint_prefers_configured_unix_socket() {
    let address: SocketAddr = "127.0.0.1:7690".parse().unwrap();
    assert_eq!(
        gateway_data_endpoint(address, None),
        "http://127.0.0.1:7690"
    );
    assert_eq!(
        gateway_data_endpoint(address, Some(Path::new("/tmp/dtg-data.sock"))),
        "unix:///tmp/dtg-data.sock"
    );
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

fn unix_time_nanos() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos(),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seed_receipt_uses_the_validated_applied_index_from_data() {
        let mut body = 37_u64.to_be_bytes().to_vec();
        body.push(0);
        let status = TypedStatus {
            request: Some(request_context(17, 19)),
            code: StatusCode::Ok.into(),
            retry: RetryDisposition::Never.into(),
            message: "transaction command accepted by Shard Raft".into(),
            idempotency_key: 19_u128.to_be_bytes().to_vec(),
            details: Some(payload(body, 1)),
        };

        assert_eq!(seed_applied_index(status).unwrap(), 37);
    }

    #[test]
    fn diagnostic_data_environment_keeps_kuzu_state_inside_the_cell_root() {
        let root = Path::new("/tmp/dtg-backend-e2e-cell");
        let environment =
            diagnostic_data_environment(root, "127.0.0.1:50052", "kuzu", "seed".into());

        assert!(environment.contains(&(
            "DTG_DATA_KUZU_ROOT",
            "/tmp/dtg-backend-e2e-cell/data/kuzu".into(),
        )));
        assert!(
            !environment
                .iter()
                .any(|(_, value)| value == "./dtg-data/kuzu")
        );
    }
}
