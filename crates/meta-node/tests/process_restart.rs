use std::net::TcpListener;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cluster_protocol::CLUSTER_PROTOCOL_VERSION;
use cluster_protocol::proto::meta_service_client::MetaServiceClient;
use cluster_protocol::proto::{AllocateTimestampRequest, RequestContext};
use tempfile::tempdir;

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

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
        cluster_id: vec![0x93; 16],
        request_id: request_id.to_be_bytes().to_vec(),
        deadline_unix_ms: now_ms() + 60_000,
    }
}

fn write_config(path: &Path, data_directory: &Path, service_port: u16, raft_port: u16) {
    let json = serde_json::json!({
        "version": 1,
        "cluster_id": "93939393939393939393939393939393",
        "node_id": 7,
        "voters": [7],
        "listen_address": format!("127.0.0.1:{service_port}"),
        "advertise_address": format!("127.0.0.1:{service_port}"),
        "raft_listen_address": format!("127.0.0.1:{raft_port}"),
        "peer_addresses": {},
        "data_directory": data_directory,
        "security": { "mode": "loopback_plaintext" },
        "timestamp_reservation_size": 16,
        "maximum_future_drift_ms": 5000,
        "shutdown_grace_ms": 5000
    });
    std::fs::write(path, serde_json::to_vec_pretty(&json).unwrap()).unwrap();
}

fn spawn_meta(config: &Path) -> Child {
    Command::new(env!("CARGO_BIN_EXE_dtgproxy-meta"))
        .args(["--config", config.to_str().unwrap()])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap()
}

async fn wait_for_meta(endpoint: &str) -> MetaServiceClient<tonic::transport::Channel> {
    for _ in 0..200 {
        if let Ok(client) = MetaServiceClient::connect(endpoint.to_owned()).await {
            return client;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("Meta process did not become ready at {endpoint}");
}

fn terminate(child: &mut Child) {
    assert!(
        Command::new("kill")
            .args(["-TERM", &child.id().to_string()])
            .status()
            .unwrap()
            .success()
    );
    assert!(child.wait().unwrap().success());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn meta_process_restart_never_reuses_a_committed_timestamp_lease() {
    let temporary = tempdir().unwrap();
    let config = temporary.path().join("meta.json");
    let service_port = free_port();
    let raft_port = free_port();
    write_config(
        &config,
        &temporary.path().join("data"),
        service_port,
        raft_port,
    );
    let endpoint = format!("http://127.0.0.1:{service_port}");

    let mut first_process = spawn_meta(&config);
    let mut client = wait_for_meta(&endpoint).await;
    let first = client
        .allocate_timestamp(AllocateTimestampRequest {
            context: Some(context(101)),
            count: 3,
            observed_physical_ms: 0,
        })
        .await
        .unwrap()
        .into_inner();
    drop(client);
    terminate(&mut first_process);

    let mut second_process = spawn_meta(&config);
    let mut client = wait_for_meta(&endpoint).await;
    let second = client
        .allocate_timestamp(AllocateTimestampRequest {
            context: Some(context(102)),
            count: 3,
            observed_physical_ms: 0,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(
        (second.first_physical_ms, second.first_logical)
            > (
                first.lease_high_water_physical_ms,
                first.lease_high_water_logical
            )
    );
    terminate(&mut second_process);
}
