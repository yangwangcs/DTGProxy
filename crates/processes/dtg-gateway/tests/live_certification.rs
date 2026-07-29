use std::collections::BTreeMap;
use std::net::{SocketAddr, TcpListener as StdTcpListener};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use dtg_execution::cluster_protocol::proto::controller_service_client::ControllerServiceClient;
use dtg_execution::cluster_protocol::proto::data_service_client::DataServiceClient;
use dtg_execution::cluster_protocol::proto::meta_service_client::MetaServiceClient;
use dtg_execution::cluster_protocol::proto::{
    BoundedPayload, CatalogWatchRequest, ControlObservation, RequestContext, ShardContext,
    StatusCode, TransactionOperation, TransactionRequest,
};
use dtg_execution::cluster_protocol::{PROTOCOL_MAJOR, SUPPORTED_MINOR_MAX, checksum_bytes};
use dtg_execution::shard::{CommitSingleShard, ShardCommand};
use dtg_execution::storage::{
    BackendClass, BindingRole, CapabilityManifest, CommandId, LogicalMutation, Properties,
    ProviderKind, ReplicaBinding, TransactionTime, ValidInterval, Version, VertexId, VertexVersion,
};
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const CAPABILITIES: &str = "adjacency,immutable-read-view,logical-snapshot,point";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn four_process_functional_probes_and_real_bolt_query() {
    let root = tempfile::tempdir().unwrap();
    let target = target_debug_dir();
    let meta_addr = free_address();
    let controller_addr = free_address();
    let data_addr = free_address();
    let gateway_addr = free_address();
    let meta_config = root.path().join("meta.json");
    let controller_config = root.path().join("controller.json");
    write_json(
        &meta_config,
        json!({
            "version": 1,
            "cluster_id": 9001,
            "node_id": 1,
            "listen_addr": meta_addr,
            "data_directory": root.path().join("meta"),
            "analytics_lease_duration": 30,
            "consensus_namespace": "certification-meta",
            "peers": [{"node_id": 1, "raft_addr": meta_addr, "rpc_addr": meta_addr}],
            "security": {"mode": "loopback_plaintext"}
        }),
    );
    write_json(
        &controller_config,
        json!({
            "version": 1,
            "cluster_id": 9001,
            "node_id": 2,
            "listen_addr": controller_addr,
            "data_directory": root.path().join("controller"),
            "meta_endpoints": [format!("http://{meta_addr}")],
            "security": {"mode": "loopback_plaintext"}
        }),
    );

    let mut processes = ProcessGroup::default();
    processes.spawn(
        target.join("dtgproxy-meta"),
        &["--config", meta_config.to_str().unwrap()],
        &[],
        root.path().join("meta.log"),
    );
    wait_for_port(meta_addr).await;
    let meta_revision = probe_meta(meta_addr).await;

    processes.spawn(
        target.join("dtgproxy-controller"),
        &["--config", controller_config.to_str().unwrap()],
        &[],
        root.path().join("controller.log"),
    );
    wait_for_port(controller_addr).await;
    probe_controller(controller_addr).await;

    let binding = fjall_binding("certification-live-bolt");
    let assignment = format!(
        "{}:{}:{}:{}:{}:{}:fjall:1:1:{}",
        binding.cluster_id().get(),
        binding.graph_id().get(),
        binding.shard_id().get(),
        binding.placement_epoch().get(),
        binding.replica_id().get(),
        binding.backend_generation().get(),
        binding.namespace_id().as_str()
    );
    let data_rpc = data_addr.to_string();
    let data_fjall = root.path().join("data/business");
    let data_raft = root.path().join("data/raft");
    processes.spawn(
        target.join("dtgproxy-data"),
        &[],
        &[
            ("DTG_DATA_RPC_ADDR", data_rpc.as_str()),
            ("DTG_DATA_FJALL_ROOT", data_fjall.to_str().unwrap()),
            ("DTG_DATA_CONSENSUS_ROOT", data_raft.to_str().unwrap()),
            ("DTG_DATA_CAPABILITIES", CAPABILITIES),
            ("DTG_DATA_ASSIGNMENTS", assignment.as_str()),
        ],
        root.path().join("data.log"),
    );
    wait_for_port(data_addr).await;
    let applied_index = seed_data(data_addr, &binding).await;

    let gateway_bind = gateway_addr.to_string();
    let data_endpoint = format!("http://{data_addr}");
    let shard_spec = format!(
        "{}:{}:{}:{}:{}:fjall:1:1:{}",
        binding.shard_id().get(),
        binding.placement_epoch().get(),
        binding.replica_id().get(),
        binding.backend_generation().get(),
        applied_index,
        binding.namespace_id().as_str()
    );
    processes.spawn(
        PathBuf::from(env!("CARGO_BIN_EXE_dtgproxy-gateway")),
        &[],
        &[
            ("DTG_GATEWAY_BIND", gateway_bind.as_str()),
            ("DTG_GATEWAY_CLUSTER_ID", "9001"),
            ("DTG_GATEWAY_REQUEST_TIMEOUT_MS", "10000"),
            ("DTG_GATEWAY_CLUSTER_ENDPOINT", data_endpoint.as_str()),
            ("DTG_GATEWAY_GRAPH_ID", "11"),
            ("DTG_GATEWAY_CATALOG_VERSION", "31"),
            ("DTG_GATEWAY_SCHEMA_VERSION", "31"),
            ("DTG_GATEWAY_TRANSACTION_TIME", "41"),
            ("DTG_GATEWAY_VALID_AT", "10"),
            ("DTG_GATEWAY_LOGICAL_SCAN_BOUND", "10"),
            ("DTG_GATEWAY_CAPABILITIES", CAPABILITIES),
            ("DTG_GATEWAY_SHARDS", shard_spec.as_str()),
        ],
        root.path().join("gateway.log"),
    );
    wait_for_port(gateway_addr).await;
    let bolt = bolt_query(gateway_addr).await;

    assert_eq!(bolt.fields, vec!["value"]);
    assert_eq!(bolt.vertex_id, 37);
    assert!(!bolt.summary_has_more);
    assert_eq!(processes.len(), 4);
    processes.assert_running(root.path());

    let evidence = json!({
        "schema_version": 1,
        "four_process_functional_probes": {
            "meta_catalog_revision": meta_revision,
            "controller_observation_status": "ok",
            "data_applied_index": applied_index,
            "gateway_bolt": {
                "transport": "bolt-v5.4-tcp",
                "normalized_tcypher": "MATCH (n) FOR SYSTEM_TIME AS OF 41 RETURN n ORDER BY n",
                "fields": bolt.fields,
                "typed_row": {"kind": "map", "vertex_id": bolt.vertex_id},
                "summary": {"has_more": bolt.summary_has_more}
            }
        }
    });
    if let Some(path) = std::env::var_os("DTG_CLEAN_BREAK_FUNCTIONAL_EVIDENCE") {
        std::fs::write(path, serde_json::to_vec_pretty(&evidence).unwrap()).unwrap();
    }
    println!("DTG_CERTIFICATION_EVIDENCE={evidence}");
}

async fn probe_meta(address: SocketAddr) -> u64 {
    let mut client = MetaServiceClient::connect(format!("http://{address}"))
        .await
        .unwrap();
    let mut stream = client
        .watch_catalog(CatalogWatchRequest {
            request: Some(request_context(101)),
            after_revision: 0,
        })
        .await
        .unwrap()
        .into_inner();
    tokio::time::timeout(Duration::from_secs(5), stream.message())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .revision
}

async fn probe_controller(address: SocketAddr) {
    let body = serde_json::to_vec(&json!({"catalog_version": 0, "replicas": []})).unwrap();
    let mut client = ControllerServiceClient::connect(format!("http://{address}"))
        .await
        .unwrap();
    let status = client
        .observe(ControlObservation {
            request: Some(request_context(102)),
            node_id: b"cert-data-node01".to_vec(),
            observation_version: 1,
            observed_at_unix_ms: unix_time_millis(),
            payload: Some(payload(body, 1)),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(status.code, StatusCode::Ok as i32, "{}", status.message);
}

async fn seed_data(address: SocketAddr, binding: &ReplicaBinding) -> u64 {
    let mut properties = Properties::new();
    properties.insert(
        "name".into(),
        dtg_execution::storage::Value::String("certified".into()),
    );
    let vertex = VertexVersion::new(
        VertexId::new(37).unwrap(),
        Version::new(1),
        ValidInterval::new(1, 100).unwrap(),
        TransactionTime::new(41).unwrap(),
        properties,
    )
    .unwrap();
    let command = ShardCommand::CommitSingleShard(
        CommitSingleShard::new(
            CommandId::new(103).unwrap(),
            binding.placement_epoch().get(),
            binding.backend_generation().get(),
            vec![LogicalMutation::PutVertex(vertex)],
        )
        .unwrap(),
    );
    let body = command.encode_current().unwrap();
    let mut client = DataServiceClient::connect(format!("http://{address}"))
        .await
        .unwrap();
    let status = client
        .apply_transaction(TransactionRequest {
            context: Some(shard_context(binding, 103)),
            transaction_id: 103_u128.to_be_bytes().to_vec(),
            operation: TransactionOperation::Commit.into(),
            idempotency_key: 103_u128.to_be_bytes().to_vec(),
            payload: Some(payload(body, 1)),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(status.code, StatusCode::Ok as i32);
    2
}

struct BoltEvidence {
    fields: Vec<String>,
    vertex_id: i64,
    summary_has_more: bool,
}

async fn bolt_query(address: SocketAddr) -> BoltEvidence {
    let mut socket = TcpStream::connect(address).await.unwrap();
    socket
        .write_all(&[
            0x60, 0x60, 0xb0, 0x17, 0x00, 0x00, 0x04, 0x05, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ])
        .await
        .unwrap();
    let mut selected = [0_u8; 4];
    socket.read_exact(&mut selected).await.unwrap();
    assert_eq!(selected, [0, 0, 4, 5]);

    write_bolt_message(&mut socket, &[0xb1, 0x01, 0xa0]).await;
    assert_success(&read_bolt_message(&mut socket).await);

    let statement = "MATCH (n) FOR SYSTEM_TIME AS OF 41 RETURN n ORDER BY n";
    let mut run = vec![0xb3, 0x10];
    encode_string(statement, &mut run);
    run.push(0xa0);
    run.push(0xa0);
    write_bolt_message(&mut socket, &run).await;
    let run_success = read_bolt_message(&mut socket).await;
    let run_metadata = decode_success(&run_success);
    let fields = match run_metadata.get("fields").unwrap() {
        PackValue::List(values) => values
            .iter()
            .map(|value| match value {
                PackValue::String(value) => value.clone(),
                _ => panic!("Bolt fields were not strings"),
            })
            .collect(),
        _ => panic!("Bolt RUN response omitted typed fields"),
    };

    write_bolt_message(&mut socket, &[0xb1, 0x3f, 0xa0]).await;
    let record = decode_record(&read_bolt_message(&mut socket).await);
    let summary = decode_success(&read_bolt_message(&mut socket).await);
    let vertex_id = match record.as_slice() {
        [PackValue::Map(vertex)] => match vertex.get("id") {
            Some(PackValue::Integer(value)) => *value,
            _ => panic!("Bolt vertex id was not a typed integer"),
        },
        _ => panic!("Bolt row was not one typed map value"),
    };
    let summary_has_more = match summary.get("has_more") {
        Some(PackValue::Boolean(value)) => *value,
        _ => panic!("Bolt PULL response omitted typed summary"),
    };
    BoltEvidence {
        fields,
        vertex_id,
        summary_has_more,
    }
}

#[derive(Clone, Debug, PartialEq)]
enum PackValue {
    Null,
    Boolean(bool),
    Integer(i64),
    String(String),
    List(Vec<PackValue>),
    Map(BTreeMap<String, PackValue>),
}

fn decode_success(message: &[u8]) -> BTreeMap<String, PackValue> {
    let mut decoder = PackDecoder::new(message);
    assert_eq!(decoder.byte(), 0xb1);
    let signature = decoder.byte();
    if signature == 0x7f {
        panic!("Bolt FAILURE: {:?}", decoder.map());
    }
    assert_eq!(signature, 0x70);
    let metadata = decoder.map();
    decoder.finish();
    metadata
}

fn assert_success(message: &[u8]) {
    let _ = decode_success(message);
}

fn decode_record(message: &[u8]) -> Vec<PackValue> {
    let mut decoder = PackDecoder::new(message);
    assert_eq!(decoder.byte(), 0xb1);
    assert_eq!(decoder.byte(), 0x71);
    let values = decoder.list();
    decoder.finish();
    values
}

struct PackDecoder<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> PackDecoder<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn byte(&mut self) -> u8 {
        let value = self.bytes[self.offset];
        self.offset += 1;
        value
    }

    fn value(&mut self) -> PackValue {
        match self.byte() {
            marker @ 0x00..=0x7f => PackValue::Integer(i64::from(marker)),
            marker @ 0xf0..=0xff => PackValue::Integer(i64::from(marker as i8)),
            0xc0 => PackValue::Null,
            0xc2 => PackValue::Boolean(false),
            0xc3 => PackValue::Boolean(true),
            0xcb => PackValue::Integer(i64::from_be_bytes(self.take(8).try_into().unwrap())),
            marker @ 0x80..=0x8f => PackValue::String(self.string_with_len((marker & 0x0f).into())),
            0xd0 => {
                let len = usize::from(self.byte());
                PackValue::String(self.string_with_len(len))
            }
            marker @ 0x90..=0x9f => PackValue::List(self.list_with_len((marker & 0x0f).into())),
            marker @ 0xa0..=0xaf => PackValue::Map(self.map_with_len((marker & 0x0f).into())),
            marker => panic!("unsupported certification PackStream marker {marker:#x}"),
        }
    }

    fn list(&mut self) -> Vec<PackValue> {
        let marker = self.byte();
        assert!((0x90..=0x9f).contains(&marker));
        self.list_with_len((marker & 0x0f).into())
    }

    fn list_with_len(&mut self, len: usize) -> Vec<PackValue> {
        (0..len).map(|_| self.value()).collect()
    }

    fn map(&mut self) -> BTreeMap<String, PackValue> {
        let marker = self.byte();
        assert!((0xa0..=0xaf).contains(&marker));
        self.map_with_len((marker & 0x0f).into())
    }

    fn map_with_len(&mut self, len: usize) -> BTreeMap<String, PackValue> {
        (0..len)
            .map(|_| {
                let key = match self.value() {
                    PackValue::String(value) => value,
                    _ => panic!("PackStream map key was not a string"),
                };
                (key, self.value())
            })
            .collect()
    }

    fn string_with_len(&mut self, len: usize) -> String {
        String::from_utf8(self.take(len).to_vec()).unwrap()
    }

    fn take(&mut self, len: usize) -> &'a [u8] {
        let start = self.offset;
        self.offset += len;
        &self.bytes[start..self.offset]
    }

    fn finish(&self) {
        assert_eq!(self.offset, self.bytes.len());
    }
}

async fn write_bolt_message(socket: &mut TcpStream, message: &[u8]) {
    socket
        .write_all(&(message.len() as u16).to_be_bytes())
        .await
        .unwrap();
    socket.write_all(message).await.unwrap();
    socket.write_all(&[0, 0]).await.unwrap();
}

async fn read_bolt_message(socket: &mut TcpStream) -> Vec<u8> {
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut message = Vec::new();
        loop {
            let mut size = [0_u8; 2];
            socket.read_exact(&mut size).await.unwrap();
            let size = usize::from(u16::from_be_bytes(size));
            if size == 0 {
                return message;
            }
            let start = message.len();
            message.resize(start + size, 0);
            socket.read_exact(&mut message[start..]).await.unwrap();
        }
    })
    .await
    .unwrap()
}

fn encode_string(value: &str, output: &mut Vec<u8>) {
    if value.len() <= 15 {
        output.push(0x80 | value.len() as u8);
    } else {
        output.extend_from_slice(&[0xd0, value.len() as u8]);
    }
    output.extend_from_slice(value.as_bytes());
}

fn request_context(request_id: u128) -> RequestContext {
    RequestContext {
        protocol_major: PROTOCOL_MAJOR,
        protocol_minor: SUPPORTED_MINOR_MAX,
        cluster_id: 9001_u64.to_be_bytes().to_vec(),
        request_id: request_id.to_be_bytes().to_vec(),
        deadline_unix_ms: unix_time_millis() + 10_000,
        trace_context: Vec::new(),
    }
}

fn shard_context(binding: &ReplicaBinding, request_id: u128) -> ShardContext {
    ShardContext {
        request: Some(request_context(request_id)),
        graph_id: binding.graph_id().get(),
        shard_id: u32::try_from(binding.shard_id().get()).unwrap(),
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

fn fjall_binding(namespace: &str) -> ReplicaBinding {
    let capabilities = CapabilityManifest::from_names(CAPABILITIES.split(',')).unwrap();
    let class = BackendClass::new(
        ProviderKind::Fjall,
        1,
        1,
        capabilities.names().map(str::to_owned),
    )
    .unwrap();
    ReplicaBinding::builder()
        .cluster_id(9001)
        .graph_id(11)
        .shard_id(13)
        .placement_epoch(17)
        .replica_id(19)
        .backend_generation(23)
        .backend_class_digest(class.digest())
        .provider_kind(ProviderKind::Fjall)
        .contract_version(1)
        .layout_version(1)
        .capability_digest(capabilities.digest())
        .namespace_id(namespace)
        .endpoint_profile_ref("certification")
        .credential_ref("certification")
        .role(BindingRole::Active)
        .build()
        .unwrap()
}

fn free_address() -> SocketAddr {
    let listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap()
}

async fn wait_for_port(address: SocketAddr) {
    for _ in 0..100 {
        if TcpStream::connect(address).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("process did not open {address}");
}

fn write_json(path: &Path, value: serde_json::Value) {
    std::fs::write(path, serde_json::to_vec_pretty(&value).unwrap()).unwrap();
}

fn target_debug_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_dtgproxy-gateway"))
        .parent()
        .unwrap()
        .to_path_buf()
}

fn unix_time_millis() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap()
}

#[derive(Default)]
struct ProcessGroup {
    children: Vec<(PathBuf, Child)>,
}

impl ProcessGroup {
    fn spawn(
        &mut self,
        binary: PathBuf,
        arguments: &[&str],
        environment: &[(&str, &str)],
        log_path: PathBuf,
    ) {
        let log = std::fs::File::create(&log_path).unwrap();
        let child = Command::new(&binary)
            .args(arguments)
            .envs(environment.iter().copied())
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log))
            .spawn()
            .unwrap_or_else(|error| panic!("failed to start {}: {error}", binary.display()));
        self.children.push((log_path, child));
    }

    fn len(&self) -> usize {
        self.children.len()
    }

    fn assert_running(&mut self, _root: &Path) {
        for (log_path, child) in &mut self.children {
            if let Some(status) = child.try_wait().unwrap() {
                let log = std::fs::read_to_string(log_path).unwrap_or_default();
                panic!("process exited with {status}: {log}");
            }
        }
    }
}

impl Drop for ProcessGroup {
    fn drop(&mut self) {
        for (_, child) in &mut self.children {
            let _ = child.kill();
        }
        for (_, child) in &mut self.children {
            let _ = child.wait();
        }
    }
}
