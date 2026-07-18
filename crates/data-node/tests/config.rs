use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;

use data_node::{ConfigError, NodeConfig, NodeIdentity, TlsFiles, TransportSecurity};
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
