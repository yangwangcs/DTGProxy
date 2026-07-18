use std::collections::BTreeMap;
use std::future::Future;
use std::net::{Ipv4Addr, SocketAddr, TcpListener, TcpStream};
use std::process::Command;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use dtgproxy::config::NodeConfig;
use dtgproxy::control_plane::{
    BackendProfile, DeploymentMode, GraphDefinition, Placement, TopologyDefinition,
};
use dtgproxy::gateway::{
    ApiMutation, GATEWAY_API_VERSION, GatewayOperation, GatewayRequest, GatewayService,
    initialize_node, read_frame, serve, write_frame,
};
use storage_api::AdapterRequirement;
use temporal_types::{CanonicalElement, GraphValue};

#[test]
fn cli_init_and_status_open_the_persisted_primary_replica_service() {
    let directory = tempfile::tempdir().unwrap();
    let config = directory.path().join("node.json");
    let root = directory.path().join("data");
    let initialized = Command::new(env!("CARGO_BIN_EXE_dtgproxy"))
        .args([
            "init",
            "--config",
            config.to_str().unwrap(),
            "--root",
            root.to_str().unwrap(),
            "--graph-id",
            "7",
            "--mode",
            "primary-replica",
            "--shards",
            "10:1:10",
            "--backend",
            "rocksdb",
        ])
        .output()
        .unwrap();
    assert!(initialized.status.success());
    assert!(config.exists());

    let status = Command::new(env!("CARGO_BIN_EXE_dtgproxy"))
        .args(["status", "--config", config.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(
        status.status.success(),
        "{}",
        String::from_utf8_lossy(&status.stderr)
    );
    let status: serde_json::Value = serde_json::from_slice(&status.stdout).unwrap();
    assert_eq!(status["mode"], "primary_replica");
    assert_eq!(status["backend_provider"], "rocksdb");
    assert_eq!(status["shards"][0]["leader_id"], 10);
}

#[test]
fn versioned_node_config_and_catalog_start_both_deployment_modes() {
    for mode in [
        DeploymentMode::PrimaryReplica,
        DeploymentMode::SharedNothing,
    ] {
        let directory = tempfile::tempdir().unwrap();
        let config_path = directory.path().join("node.json");
        let config = NodeConfig::new(
            directory.path().join("data"),
            7,
            SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
            20,
            16,
        )
        .unwrap();
        initialize_node(&config_path, &config, graph(mode)).unwrap();

        let loaded = NodeConfig::load(&config_path).unwrap();
        let service = block_on(GatewayService::open(loaded)).unwrap();
        let status = service.status().unwrap();
        assert_eq!(
            status.mode(),
            match mode {
                DeploymentMode::PrimaryReplica => "primary_replica",
                DeploymentMode::SharedNothing => "shared_nothing",
            }
        );
        assert_eq!(status.backend_provider(), "rocksdb");
        assert!(
            status
                .shards()
                .iter()
                .all(|shard| shard.leader_id().is_some())
        );
    }
}

#[test]
fn gateway_transaction_accepts_temporal_input_and_query_reads_it_back() {
    let directory = tempfile::tempdir().unwrap();
    let config_path = directory.path().join("node.json");
    let config = NodeConfig::new(
        directory.path().join("data"),
        7,
        SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        20,
        16,
    )
    .unwrap();
    initialize_node(&config_path, &config, graph(DeploymentMode::PrimaryReplica)).unwrap();
    let mut gateway = block_on(GatewayService::open(config)).unwrap();
    let payload = CanonicalElement::new(
        1,
        BTreeMap::from([(1, GraphValue::String("gateway".into()))]),
    );
    let transaction = GatewayRequest {
        version: GATEWAY_API_VERSION,
        request_id: "txn-1".into(),
        operation: GatewayOperation::Transaction {
            schema_version: 1,
            ttl_micros: 10_000,
            mutations: vec![ApiMutation::PutVertex {
                partition: 0,
                vertex_id: "9".into(),
                label_id: 1,
                valid_from_micros: 0,
                valid_to_micros: None,
                payload_dtp1: hex(&payload.encode().unwrap()),
            }],
        },
    };
    let committed = block_on(gateway.execute_request(transaction));
    let committed = serde_json::to_value(committed).unwrap();
    assert_eq!(committed["ok"], true);
    assert_eq!(committed["result"]["single_shard_fast_path"], true);

    let query = GatewayRequest {
        version: GATEWAY_API_VERSION,
        request_id: "query-1".into(),
        operation: GatewayOperation::Query {
            text: "VERTEX 9 GRAPH 7 PARTITION 0 FOR VALID TIME 1 CURRENT LIMIT 1".into(),
        },
    };
    let queried = block_on(gateway.execute_request(query));
    let queried = serde_json::to_value(queried).unwrap();
    assert_eq!(queried["ok"], true);
    assert_eq!(queried["result"]["records"][0]["element_id"], "9");
}

#[test]
fn bounded_tcp_gateway_serves_a_canonical_status_frame() {
    let directory = tempfile::tempdir().unwrap();
    let config_path = directory.path().join("node.json");
    let config = NodeConfig::new(
        directory.path().join("data"),
        7,
        SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        20,
        16,
    )
    .unwrap();
    initialize_node(&config_path, &config, graph(DeploymentMode::PrimaryReplica)).unwrap();
    let gateway = block_on(GatewayService::open(config)).unwrap();
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || serve(gateway, listener, Some(1)).unwrap());

    let mut stream = TcpStream::connect(address).unwrap();
    write_frame(
        &mut stream,
        br#"{"version":1,"request_id":"status-1","operation":"status"}"#,
    )
    .unwrap();
    let response: serde_json::Value =
        serde_json::from_slice(&read_frame(&mut stream).unwrap()).unwrap();
    assert_eq!(response["ok"], true);
    assert_eq!(response["result"]["graph_name"], "graph-7");
    server.join().unwrap();
}

fn graph(mode: DeploymentMode) -> GraphDefinition {
    let placements = match mode {
        DeploymentMode::PrimaryReplica => vec![Placement::new(10, 1, vec![10]).unwrap()],
        DeploymentMode::SharedNothing => vec![
            Placement::new(10, 1, vec![10]).unwrap(),
            Placement::new(20, 1, vec![20]).unwrap(),
        ],
    };
    GraphDefinition::new(
        7,
        "graph-7",
        1,
        TopologyDefinition::new(mode, 99, 128, 1, placements).unwrap(),
        BackendProfile::new(
            "rocksdb",
            BTreeMap::from([("path".into(), "backends/graph-7".into())]),
            BTreeMap::new(),
            AdapterRequirement::HotPluggableReplica,
            1,
        )
        .unwrap(),
    )
    .unwrap()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
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
