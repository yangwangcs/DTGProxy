use std::fs;

use controller::{ControllerConfigError, ControllerRuntimeConfig};

#[test]
fn controller_config_loads_bounded_loopback_meta_and_data_endpoints() {
    let temporary = tempfile::tempdir().unwrap();
    let path = temporary.path().join("controller.json");
    fs::write(
        &path,
        br#"{
          "version": 1,
          "cluster_id": "11111111111111111111111111111111",
          "controller_id": 9,
          "meta_seeds": ["127.0.0.1:7001"],
          "data_nodes": {"1": "127.0.0.1:7101", "2": "127.0.0.1:7102"},
          "reconcile_interval_ms": 50,
          "request_timeout_ms": 5000,
          "security": "loopback_plaintext"
        }"#,
    )
    .unwrap();
    let config = ControllerRuntimeConfig::load(path).unwrap();
    assert_eq!(config.controller_id(), 9);
    assert_eq!(config.meta_seeds().len(), 1);
    assert_eq!(config.data_nodes().len(), 2);
}

#[test]
fn controller_config_rejects_plaintext_non_loopback_and_unknown_fields() {
    let temporary = tempfile::tempdir().unwrap();
    let path = temporary.path().join("controller.json");
    fs::write(
        &path,
        br#"{
          "version": 1,
          "cluster_id": "11111111111111111111111111111111",
          "controller_id": 9,
          "meta_seeds": ["127.0.0.1:7001"],
          "data_nodes": {"1": "192.0.2.1:7101"},
          "reconcile_interval_ms": 50,
          "request_timeout_ms": 5000,
          "security": "loopback_plaintext"
        }"#,
    )
    .unwrap();
    assert_eq!(
        ControllerRuntimeConfig::load(&path).unwrap_err(),
        ControllerConfigError::InsecureNonLoopback
    );
    fs::write(
        &path,
        br#"{
          "version": 1,
          "cluster_id": "11111111111111111111111111111111",
          "controller_id": 9,
          "meta_seeds": ["127.0.0.1:7001"],
          "data_nodes": {"1": "127.0.0.1:7101"},
          "reconcile_interval_ms": 50,
          "request_timeout_ms": 5000,
          "security": "loopback_plaintext",
          "surprise": true
        }"#,
    )
    .unwrap();
    assert!(matches!(
        ControllerRuntimeConfig::load(path).unwrap_err(),
        ControllerConfigError::Json(_)
    ));
}
