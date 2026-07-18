use std::path::Path;

use meta_node::{MetaNodeRuntimeConfig, MetaTransportSecurity};
use tempfile::tempdir;

fn write_config(path: &Path, data_directory: &Path, voters: &[u64]) {
    let json = serde_json::json!({
        "version": 1,
        "cluster_id": "92929292929292929292929292929292",
        "node_id": 2,
        "voters": voters,
        "listen_address": "127.0.0.1:7202",
        "advertise_address": "127.0.0.1:7202",
        "raft_listen_address": "127.0.0.1:7302",
        "peer_addresses": {
            "1": "127.0.0.1:7301",
            "3": "127.0.0.1:7303"
        },
        "data_directory": data_directory,
        "security": { "mode": "loopback_plaintext" },
        "timestamp_reservation_size": 4096,
        "maximum_future_drift_ms": 5000,
        "shutdown_grace_ms": 5000
    });
    std::fs::write(path, serde_json::to_vec_pretty(&json).unwrap()).unwrap();
}

#[test]
fn runtime_config_loads_a_bounded_three_voter_meta_group() {
    let temporary = tempdir().unwrap();
    let path = temporary.path().join("meta.json");
    let data_directory = temporary.path().join("data");
    write_config(&path, &data_directory, &[1, 2, 3]);

    let config = MetaNodeRuntimeConfig::load(path).unwrap();
    assert_eq!(config.cluster_id(), &[0x92; 16]);
    assert_eq!(config.node_id(), 2);
    assert_eq!(config.voters(), &[1, 2, 3]);
    assert_eq!(config.peer_addresses().len(), 2);
    assert_eq!(config.data_directory(), data_directory);
    assert_eq!(config.timestamp_reservation_size(), 4096);
    assert_eq!(config.maximum_future_drift_micros(), 5_000_000);
    assert!(matches!(
        config.transport_security(),
        MetaTransportSecurity::LoopbackPlaintext
    ));
}

#[test]
fn runtime_config_rejects_missing_peers_and_insecure_non_loopback() {
    let temporary = tempdir().unwrap();
    let path = temporary.path().join("meta.json");
    write_config(&path, &temporary.path().join("data"), &[1, 2, 3, 4, 5]);
    let error = MetaNodeRuntimeConfig::load(&path).unwrap_err();
    assert!(error.to_string().contains("peer_addresses"));

    let json = serde_json::json!({
        "version": 1,
        "cluster_id": "92929292929292929292929292929292",
        "node_id": 2,
        "voters": [2],
        "listen_address": "0.0.0.0:7202",
        "advertise_address": "10.0.0.2:7202",
        "raft_listen_address": "0.0.0.0:7302",
        "peer_addresses": {},
        "data_directory": temporary.path().join("data"),
        "security": { "mode": "loopback_plaintext" },
        "timestamp_reservation_size": 4096,
        "maximum_future_drift_ms": 5000,
        "shutdown_grace_ms": 5000
    });
    std::fs::write(&path, serde_json::to_vec(&json).unwrap()).unwrap();
    let error = MetaNodeRuntimeConfig::load(path).unwrap_err();
    assert!(error.to_string().contains("loopback"));
}
