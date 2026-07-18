use std::fs;

use gateway_node::{GatewayConfigError, GatewayNodeRuntimeConfig};

#[test]
fn loads_a_bounded_loopback_gateway_configuration_without_a_data_directory() {
    let temporary = tempfile::tempdir().unwrap();
    let path = temporary.path().join("gateway.json");
    fs::write(
        &path,
        r#"{
          "version": 1,
          "cluster_id": "72727272727272727272727272727272",
          "node_id": 31,
          "graph_id": 7,
          "listen_address": "127.0.0.1:7301",
          "advertise_address": "127.0.0.1:7301",
          "meta_seeds": ["127.0.0.1:7001"],
          "data_nodes": {"10": "127.0.0.1:7100", "20": "127.0.0.1:7200"},
          "security": {"mode": "loopback_plaintext"},
          "maximum_inflight": 256,
          "max_raft_ticks": 128,
          "catalog_watch_timeout_ms": 30000,
          "shutdown_grace_ms": 5000
        }"#,
    )
    .unwrap();

    let config = GatewayNodeRuntimeConfig::load(path).unwrap();
    assert_eq!(config.node_id(), 31);
    assert_eq!(config.graph_id(), 7);
    assert_eq!(config.data_nodes().len(), 2);
    assert_eq!(config.maximum_inflight(), 256);
    assert_eq!(config.max_raft_ticks(), 128);
    assert_eq!(config.catalog_watch_timeout().as_secs(), 30);
    assert_eq!(config.shutdown_grace().as_secs(), 5);
}

#[test]
fn rejects_unknown_fields_and_non_loopback_plaintext() {
    let temporary = tempfile::tempdir().unwrap();
    let unknown = temporary.path().join("unknown.json");
    fs::write(
        &unknown,
        r#"{
          "version": 1,
          "cluster_id": "72727272727272727272727272727272",
          "node_id": 31,
          "graph_id": 7,
          "listen_address": "127.0.0.1:7301",
          "advertise_address": "127.0.0.1:7301",
          "meta_seeds": ["127.0.0.1:7001"],
          "data_nodes": {"10": "127.0.0.1:7100"},
          "security": {"mode": "loopback_plaintext"},
          "maximum_inflight": 256,
          "max_raft_ticks": 128,
          "catalog_watch_timeout_ms": 30000,
          "shutdown_grace_ms": 5000,
          "data_directory": "/must/not/exist"
        }"#,
    )
    .unwrap();
    assert!(matches!(
        GatewayNodeRuntimeConfig::load(unknown),
        Err(GatewayConfigError::Json(_))
    ));

    let insecure = temporary.path().join("insecure.json");
    fs::write(
        &insecure,
        r#"{
          "version": 1,
          "cluster_id": "72727272727272727272727272727272",
          "node_id": 31,
          "graph_id": 7,
          "listen_address": "0.0.0.0:7301",
          "advertise_address": "192.0.2.31:7301",
          "meta_seeds": ["192.0.2.1:7001"],
          "data_nodes": {"10": "192.0.2.10:7100"},
          "security": {"mode": "loopback_plaintext"},
          "maximum_inflight": 256,
          "max_raft_ticks": 128,
          "catalog_watch_timeout_ms": 30000,
          "shutdown_grace_ms": 5000
        }"#,
    )
    .unwrap();
    assert_eq!(
        GatewayNodeRuntimeConfig::load(insecure).unwrap_err(),
        GatewayConfigError::InsecureNonLoopback
    );
}

#[test]
fn rejects_zero_ids_empty_routes_and_unbounded_runtime_values() {
    let temporary = tempfile::tempdir().unwrap();
    let path = temporary.path().join("invalid.json");
    fs::write(
        &path,
        r#"{
          "version": 1,
          "cluster_id": "00000000000000000000000000000000",
          "node_id": 0,
          "graph_id": 0,
          "listen_address": "127.0.0.1:7301",
          "advertise_address": "127.0.0.1:7301",
          "meta_seeds": [],
          "data_nodes": {},
          "security": {"mode": "loopback_plaintext"},
          "maximum_inflight": 0,
          "max_raft_ticks": 0,
          "catalog_watch_timeout_ms": 0,
          "shutdown_grace_ms": 0
        }"#,
    )
    .unwrap();
    assert!(GatewayNodeRuntimeConfig::load(path).is_err());
}
