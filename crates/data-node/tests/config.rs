use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;

use data_node::{
    ConfigError, DataNodeRuntimeConfig, FileConfigError, NodeConfig, NodeIdentity, TlsFiles,
    TransportSecurity,
};
use tempfile::tempdir;

fn address(octets: [u8; 4], port: u16) -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::from(octets)), port)
}

fn identity() -> NodeIdentity {
    NodeIdentity::new([0x41; 16], 7).unwrap()
}

#[test]
fn loopback_plaintext_config_is_explicit_and_validated() {
    let temporary = tempdir().unwrap();
    let config = NodeConfig::new(
        identity(),
        address([127, 0, 0, 1], 7101),
        address([127, 0, 0, 1], 7101),
        temporary.path(),
        vec![address([127, 0, 0, 1], 7001)],
        TransportSecurity::LoopbackPlaintext,
        BTreeMap::from([("zone".to_owned(), "test-a".to_owned())]),
    )
    .unwrap();

    assert_eq!(config.identity(), &identity());
    assert_eq!(config.data_directory(), temporary.path());
    assert_eq!(config.meta_seeds().len(), 1);
    assert_eq!(
        NodeConfig::new(
            identity(),
            address([0, 0, 0, 0], 7101),
            address([10, 0, 0, 7], 7101),
            temporary.path(),
            vec![address([10, 0, 0, 1], 7001)],
            TransportSecurity::LoopbackPlaintext,
            BTreeMap::new(),
        ),
        Err(ConfigError::InsecureNonLoopback)
    );
}

#[test]
fn config_rejects_missing_duplicate_or_unroutable_addresses() {
    let temporary = tempdir().unwrap();
    let base = || {
        NodeConfig::new(
            identity(),
            address([127, 0, 0, 1], 7101),
            address([127, 0, 0, 1], 7101),
            temporary.path(),
            Vec::new(),
            TransportSecurity::LoopbackPlaintext,
            BTreeMap::new(),
        )
    };
    assert_eq!(base(), Err(ConfigError::MissingMetaSeeds));
    assert_eq!(
        NodeConfig::new(
            identity(),
            address([127, 0, 0, 1], 7101),
            address([127, 0, 0, 1], 7101),
            temporary.path(),
            vec![address([127, 0, 0, 1], 7001), address([127, 0, 0, 1], 7001),],
            TransportSecurity::LoopbackPlaintext,
            BTreeMap::new(),
        ),
        Err(ConfigError::DuplicateMetaSeed {
            address: address([127, 0, 0, 1], 7001),
        })
    );
    assert_eq!(
        NodeConfig::new(
            identity(),
            address([127, 0, 0, 1], 0),
            address([127, 0, 0, 1], 7101),
            temporary.path(),
            vec![address([127, 0, 0, 1], 7001)],
            TransportSecurity::LoopbackPlaintext,
            BTreeMap::new(),
        ),
        Err(ConfigError::ZeroPort)
    );
}

#[test]
fn config_requires_absolute_storage_and_tls_secret_paths() {
    assert_eq!(
        NodeConfig::new(
            identity(),
            address([127, 0, 0, 1], 7101),
            address([127, 0, 0, 1], 7101),
            PathBuf::from("relative-data"),
            vec![address([127, 0, 0, 1], 7001)],
            TransportSecurity::LoopbackPlaintext,
            BTreeMap::new(),
        ),
        Err(ConfigError::DataDirectoryNotAbsolute)
    );
    let temporary = tempdir().unwrap();
    assert_eq!(
        TlsFiles::new(
            temporary.path().join("ca.pem"),
            PathBuf::from("relative-cert.pem"),
            temporary.path().join("key.pem"),
        ),
        Err(ConfigError::SecretPathNotAbsolute)
    );
}

#[test]
fn capacity_labels_are_bounded_and_canonical() {
    let temporary = tempdir().unwrap();
    assert_eq!(
        NodeConfig::new(
            identity(),
            address([127, 0, 0, 1], 7101),
            address([127, 0, 0, 1], 7101),
            temporary.path(),
            vec![address([127, 0, 0, 1], 7001)],
            TransportSecurity::LoopbackPlaintext,
            BTreeMap::from([("bad label".to_owned(), "x".to_owned())]),
        ),
        Err(ConfigError::InvalidCapacityLabel {
            name: "bad label".to_owned(),
        })
    );
}

fn base_config(data_directory: &std::path::Path) -> serde_json::Value {
    serde_json::json!({
        "version": 1,
        "cluster_id": "93939393939393939393939393939393",
        "node_id": 1,
        "listen_address": "127.0.0.1:7101",
        "advertise_address": "127.0.0.1:7101",
        "data_directory": data_directory,
        "meta_seeds": ["127.0.0.1:7001"],
        "security": { "mode": "loopback_plaintext" },
        "actor_queue_capacity": 64,
        "shutdown_grace_ms": 5000
    })
}

#[test]
fn raft_runtime_addresses_are_explicit_and_backward_compatible() {
    let temporary = tempdir().unwrap();
    let path = temporary.path().join("data.json");
    let legacy = base_config(&temporary.path().join("data"));
    std::fs::write(&path, serde_json::to_vec(&legacy).unwrap()).unwrap();
    let loaded = DataNodeRuntimeConfig::load(&path).unwrap();
    assert_eq!(loaded.raft_listen_address(), None);
    assert!(loaded.raft_peers().is_empty());

    let mut networked = legacy;
    networked["raft_listen_address"] = serde_json::json!("127.0.0.1:7201");
    networked["raft_peers"] = serde_json::json!({ "2": "127.0.0.1:7202" });
    std::fs::write(&path, serde_json::to_vec(&networked).unwrap()).unwrap();
    let loaded = DataNodeRuntimeConfig::load(&path).unwrap();
    assert_eq!(
        loaded.raft_listen_address().unwrap().to_string(),
        "127.0.0.1:7201"
    );
    assert_eq!(loaded.raft_peers()[&2].to_string(), "127.0.0.1:7202");
}

#[test]
fn peers_without_a_private_raft_listener_fail_closed() {
    let temporary = tempdir().unwrap();
    let path = temporary.path().join("data.json");
    let mut config = base_config(&temporary.path().join("data"));
    config["raft_peers"] = serde_json::json!({ "2": "127.0.0.1:7202" });
    std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
    assert_eq!(
        DataNodeRuntimeConfig::load(&path).unwrap_err(),
        FileConfigError::MissingRaftListenAddress
    );
}
