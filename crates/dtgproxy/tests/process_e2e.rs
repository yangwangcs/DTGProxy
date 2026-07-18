use std::collections::BTreeMap;
use std::net::{Ipv4Addr, TcpListener};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::Duration;

use dtgproxy::gateway::{ApiMutation, GATEWAY_API_VERSION, GatewayOperation, GatewayRequest};
use temporal_types::CanonicalElement;

#[test]
fn cli_process_write_query_and_restart_recovery() {
    let temporary = tempfile::tempdir().unwrap();
    let config = temporary.path().join("node.json");
    let root = temporary.path().join("data");
    let request_file = temporary.path().join("transaction.json");
    let address = available_address();

    let initialized = Command::new(binary())
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
            "--listen",
            &address.to_string(),
        ])
        .output()
        .unwrap();
    assert_command_succeeded("init", &initialized);

    let payload = CanonicalElement::new(1, BTreeMap::new());
    let request = GatewayRequest {
        version: GATEWAY_API_VERSION,
        request_id: "process-e2e-write".into(),
        operation: GatewayOperation::Transaction {
            schema_version: 1,
            ttl_micros: 10_000,
            mutations: vec![ApiMutation::PutVertex {
                partition: 0,
                vertex_id: "42".into(),
                label_id: 1,
                valid_from_micros: 0,
                valid_to_micros: None,
                payload_dtp1: hex(&payload.encode().unwrap()),
            }],
        },
    };
    std::fs::write(&request_file, serde_json::to_vec_pretty(&request).unwrap()).unwrap();

    let mut server = Server::start(&config);
    server.assert_running();
    let committed = Command::new(binary())
        .args([
            "transaction",
            "--config",
            config.to_str().unwrap(),
            "--file",
            request_file.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert_command_succeeded("transaction", &committed);
    let committed: serde_json::Value = serde_json::from_slice(&committed.stdout).unwrap();
    assert_eq!(committed["ok"], true, "{committed}");

    assert_vertex_query(&config);
    server.stop();

    let mut restarted = Server::start(&config);
    restarted.assert_running();
    assert_vertex_query(&config);
    restarted.stop();
}

fn available_address() -> std::net::SocketAddr {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    listener.local_addr().unwrap()
}

fn assert_vertex_query(config: &std::path::Path) {
    let queried = Command::new(binary())
        .args([
            "query",
            "--config",
            config.to_str().unwrap(),
            "--text",
            "VERTEX 42 GRAPH 7 PARTITION 0 FOR VALID TIME 1 CURRENT LIMIT 1",
        ])
        .output()
        .unwrap();
    assert_command_succeeded("query", &queried);
    let queried: serde_json::Value = serde_json::from_slice(&queried.stdout).unwrap();
    assert_eq!(queried["ok"], true, "{queried}");
    assert_eq!(queried["result"]["records"][0]["element_id"], "42");
}

fn assert_command_succeeded(name: &str, output: &std::process::Output) {
    assert!(
        output.status.success(),
        "{name} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn binary() -> &'static str {
    env!("CARGO_BIN_EXE_dtgproxy")
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

struct Server(Option<Child>);

impl Server {
    fn start(config: &std::path::Path) -> Self {
        let child = Command::new(binary())
            .args(["serve", "--config", config.to_str().unwrap()])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        thread::sleep(Duration::from_millis(250));
        Self(Some(child))
    }

    fn assert_running(&mut self) {
        assert!(self.0.as_mut().unwrap().try_wait().unwrap().is_none());
    }

    fn stop(mut self) {
        stop_child(self.0.take().unwrap());
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        if let Some(child) = self.0.take() {
            stop_child(child);
        }
    }
}

fn stop_child(mut child: Child) {
    if child.try_wait().unwrap().is_none() {
        child.kill().unwrap();
    }
    child.wait().unwrap();
}
