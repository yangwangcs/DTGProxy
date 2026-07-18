use std::collections::BTreeMap;
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
        cluster_id: vec![0x94; 16],
        request_id: request_id.to_be_bytes().to_vec(),
        deadline_unix_ms: now_ms() + 15_000,
    }
}

fn write_config(
    path: &Path,
    data_directory: &Path,
    node_id: u64,
    service_port: u16,
    raft_port: u16,
    raft_ports: &BTreeMap<u64, u16>,
) {
    let peers: BTreeMap<_, _> = raft_ports
        .iter()
        .filter(|(peer, _)| **peer != node_id)
        .map(|(peer, port)| (peer.to_string(), format!("127.0.0.1:{port}")))
        .collect();
    let json = serde_json::json!({
        "version": 1,
        "cluster_id": "94949494949494949494949494949494",
        "node_id": node_id,
        "voters": [1, 2, 3],
        "listen_address": format!("127.0.0.1:{service_port}"),
        "advertise_address": format!("127.0.0.1:{service_port}"),
        "raft_listen_address": format!("127.0.0.1:{raft_port}"),
        "peer_addresses": peers,
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

fn stop(child: &mut Child) {
    if child.try_wait().unwrap().is_none() {
        let _ = Command::new("kill")
            .args(["-TERM", &child.id().to_string()])
            .status();
        let _ = child.wait();
    }
}

async fn allocate_from_leader(
    endpoints: &[(u64, String)],
    request_id: u128,
) -> Option<(u64, cluster_protocol::proto::AllocateTimestampResponse)> {
    for _ in 0..200 {
        for (node_id, endpoint) in endpoints {
            let Ok(mut client) = MetaServiceClient::connect(endpoint.clone()).await else {
                continue;
            };
            let response = client
                .allocate_timestamp(AllocateTimestampRequest {
                    context: Some(context(request_id)),
                    count: 3,
                    observed_physical_ms: 0,
                })
                .await;
            if let Ok(response) = response {
                return Some((*node_id, response.into_inner()));
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    None
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_process_meta_quorum_fails_over_without_reusing_timestamps() {
    let temporary = tempdir().unwrap();
    let service_ports = BTreeMap::from([(1, free_port()), (2, free_port()), (3, free_port())]);
    let raft_ports = BTreeMap::from([(1, free_port()), (2, free_port()), (3, free_port())]);
    let mut children = Vec::new();
    let mut endpoints = Vec::new();
    for node_id in 1..=3 {
        let config = temporary.path().join(format!("meta-{node_id}.json"));
        write_config(
            &config,
            &temporary.path().join(format!("data-{node_id}")),
            node_id,
            service_ports[&node_id],
            raft_ports[&node_id],
            &raft_ports,
        );
        children.push((node_id, spawn_meta(&config)));
        endpoints.push((
            node_id,
            format!("http://127.0.0.1:{}", service_ports[&node_id]),
        ));
    }

    let (leader, first) = allocate_from_leader(&endpoints, 501)
        .await
        .expect("three-node Meta group did not elect a Leader");
    let leader_child = children
        .iter_mut()
        .find(|(node_id, _)| *node_id == leader)
        .unwrap();
    stop(&mut leader_child.1);
    let survivors: Vec<_> = endpoints
        .iter()
        .filter(|(node_id, _)| *node_id != leader)
        .cloned()
        .collect();
    let (_, second) = allocate_from_leader(&survivors, 502)
        .await
        .expect("surviving quorum did not elect a replacement Leader");
    assert!(
        (second.first_physical_ms, second.first_logical)
            > (
                first.lease_high_water_physical_ms,
                first.lease_high_water_logical
            )
    );
    for (_, child) in &mut children {
        stop(child);
    }
}
