use std::collections::{BTreeMap, BTreeSet};
use std::io::{BufRead, BufReader};
use std::net::SocketAddr;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use cluster_protocol::CLUSTER_PROTOCOL_VERSION;
use cluster_protocol::proto::gateway_service_client::GatewayServiceClient;
use cluster_protocol::proto::meta_service_server::{MetaService, MetaServiceServer};
use cluster_protocol::proto::{GatewaySubmitRequest, ProposeRequest, RequestContext};
use control_plane::{
    BackendProfile, CatalogCommand, DeploymentMode, GraphDefinition, Placement, TopologyDefinition,
};
use meta_node::{MetaNodeService, MetaRaftReplica, ReplicatedTso};
use storage_api::AdapterRequirement;
use timestamp_oracle::ManualClock;
use tokio::sync::Mutex;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::Request;
use tonic::transport::Server;

const CLUSTER_ID: [u8; 16] = [0x74; 16];

fn now_ms() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
}

fn context(request_id: u128) -> RequestContext {
    RequestContext {
        protocol_version: CLUSTER_PROTOCOL_VERSION,
        cluster_id: CLUSTER_ID.to_vec(),
        request_id: request_id.to_be_bytes().to_vec(),
        deadline_unix_ms: now_ms() + 60_000,
    }
}

fn graph() -> GraphDefinition {
    GraphDefinition::new(
        7,
        "process-graph",
        1,
        TopologyDefinition::new(
            DeploymentMode::PrimaryReplica,
            99,
            128,
            1,
            vec![Placement::new(10, 1, vec![10]).unwrap()],
        )
        .unwrap(),
        BackendProfile::new(
            "rocksdb",
            BTreeMap::new(),
            BTreeMap::new(),
            AdapterRequirement::HotPluggableReplica,
            1,
        )
        .unwrap(),
    )
    .unwrap()
}

fn elected_meta(root: &std::path::Path) -> MetaNodeService {
    let mut replica =
        MetaRaftReplica::open(1, &[1], root.join("raft"), root.join("state")).unwrap();
    replica.campaign().unwrap();
    for _ in 0..32 {
        assert!(replica.drain_ready().unwrap().is_empty());
        if replica.is_leader() {
            break;
        }
        replica.tick();
    }
    MetaNodeService::new(
        CLUSTER_ID,
        Arc::new(Mutex::new(replica)),
        Arc::new(ReplicatedTso::new(Arc::new(ManualClock::new(1_000_000)), 8, 1_000_000).unwrap()),
    )
}

fn reserve_address() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gateway_binary_bootstraps_from_meta_without_creating_replica_storage() {
    let temporary = tempfile::tempdir().unwrap();
    let meta = elected_meta(&temporary.path().join("meta"));
    meta.propose(Request::new(ProposeRequest {
        context: Some(context(101)),
        command: CatalogCommand::create_graph(101, 0, graph())
            .encode()
            .unwrap(),
    }))
    .await
    .unwrap();
    let meta_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let meta_address = meta_listener.local_addr().unwrap();
    let (meta_shutdown, meta_shutdown_rx) = tokio::sync::oneshot::channel();
    let meta_server = tokio::spawn(
        Server::builder()
            .add_service(MetaServiceServer::new(meta))
            .serve_with_incoming_shutdown(TcpListenerStream::new(meta_listener), async {
                let _ = meta_shutdown_rx.await;
            }),
    );

    let gateway_address = reserve_address();
    let config_path = temporary.path().join("gateway.json");
    std::fs::write(
        &config_path,
        serde_json::to_vec_pretty(&serde_json::json!({
            "version": 1,
            "cluster_id": "74747474747474747474747474747474",
            "node_id": 31,
            "graph_id": 7,
            "listen_address": gateway_address,
            "advertise_address": gateway_address,
            "meta_seeds": [meta_address],
            "data_nodes": {"10": "127.0.0.1:7110"},
            "security": {"mode": "loopback_plaintext"},
            "maximum_inflight": 32,
            "max_raft_ticks": 64,
            "catalog_watch_timeout_ms": 5000,
            "shutdown_grace_ms": 1000
        }))
        .unwrap(),
    )
    .unwrap();
    let entries_before = std::fs::read_dir(temporary.path())
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect::<BTreeSet<_>>();

    let mut child = Command::new(env!("CARGO_BIN_EXE_dtgproxy-gateway"))
        .args(["--config", config_path.to_str().unwrap()])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let ready = tokio::task::spawn_blocking(move || {
        let mut reader = BufReader::new(stdout);
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        line
    })
    .await
    .unwrap();
    assert!(ready.starts_with("DTGPROXY_GATEWAY_READY"), "{ready}");

    let mut client = GatewayServiceClient::connect(format!("http://{gateway_address}"))
        .await
        .unwrap();
    let response = client
        .submit(Request::new(GatewaySubmitRequest {
            context: Some(context(201)),
            request_json: br#"{"version":1,"request_id":"process-status","operation":"status"}"#
                .to_vec(),
        }))
        .await
        .unwrap()
        .into_inner();
    let response: serde_json::Value = serde_json::from_slice(&response.response_json).unwrap();
    assert_eq!(response["ok"], true, "{response}");
    assert_eq!(response["result"]["replica_storage"], "none");

    child.kill().unwrap();
    child.wait().unwrap();
    let entries_after = std::fs::read_dir(temporary.path())
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect::<BTreeSet<_>>();
    assert_eq!(entries_after, entries_before);
    meta_shutdown.send(()).unwrap();
    meta_server.await.unwrap().unwrap();
}
