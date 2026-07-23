use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::Write;

use data_node::{
    BackendProfile, BackendSlotState, NodeIdentity, NodeIdentityStore, ReplicaEntry,
    ReplicaManifest, ReplicaManifestStore, ReplicaRole, StorageError,
};
use tempfile::tempdir;

fn identity(node_id: u64) -> NodeIdentity {
    NodeIdentity::new([0x41; 16], node_id).expect("valid identity")
}

fn replica(graph_id: u64, shard_id: u32, directory: &str) -> ReplicaEntry {
    ReplicaEntry::new(
        graph_id,
        shard_id,
        3,
        vec![7],
        ReplicaRole::Voter,
        5,
        7,
        directory,
    )
    .expect("valid replica")
}

#[test]
fn first_boot_persists_identity_and_restart_rejects_identity_drift() {
    let temporary = tempdir().unwrap();
    {
        let store = NodeIdentityStore::open_or_create(temporary.path(), identity(7)).unwrap();
        assert_eq!(store.identity(), &identity(7));
        assert!(temporary.path().join("node.identity").is_file());
    }

    let reopened = NodeIdentityStore::open_or_create(temporary.path(), identity(7)).unwrap();
    assert_eq!(reopened.identity(), &identity(7));
    drop(reopened);

    assert!(matches!(
        NodeIdentityStore::open_or_create(temporary.path(), identity(8)),
        Err(StorageError::IdentityMismatch {
            expected: 8,
            actual: 7
        })
    ));
}

#[test]
fn live_process_exclusively_owns_the_data_directory() {
    let temporary = tempdir().unwrap();
    let owner = NodeIdentityStore::open_or_create(temporary.path(), identity(7)).unwrap();
    assert!(matches!(
        NodeIdentityStore::open_or_create(temporary.path(), identity(7)),
        Err(StorageError::DirectoryLocked)
    ));
    drop(owner);
    assert!(NodeIdentityStore::open_or_create(temporary.path(), identity(7)).is_ok());
}

#[test]
fn manifest_round_trips_sorted_replicas_and_rejects_directory_aliasing() {
    let temporary = tempdir().unwrap();
    let mut manifest = ReplicaManifest::new();
    manifest.insert(replica(2, 9, "graph-2-shard-9")).unwrap();
    manifest.insert(replica(1, 4, "graph-1-shard-4")).unwrap();

    assert_eq!(
        manifest.insert(replica(3, 12, "graph-1-shard-4")),
        Err(StorageError::ReplicaDirectoryConflict {
            directory: "graph-1-shard-4".to_owned(),
        })
    );

    {
        let mut store = ReplicaManifestStore::open(temporary.path()).unwrap();
        store.persist(&manifest).unwrap();
    }
    let reopened = ReplicaManifestStore::open(temporary.path()).unwrap();
    assert_eq!(reopened.manifest(), &manifest);
    assert_eq!(
        reopened
            .manifest()
            .replicas()
            .map(|entry| (entry.graph_id(), entry.shard_id()))
            .collect::<Vec<_>>(),
        vec![(1, 4), (2, 9)]
    );
}

#[test]
fn incomplete_trailing_manifest_record_is_discarded_on_reopen() {
    let temporary = tempdir().unwrap();
    let mut manifest = ReplicaManifest::new();
    manifest.insert(replica(1, 4, "graph-1-shard-4")).unwrap();
    let log_path;
    {
        let mut store = ReplicaManifestStore::open(temporary.path()).unwrap();
        store.persist(&manifest).unwrap();
        log_path = store.path().to_path_buf();
    }
    let stable_length = fs::metadata(&log_path).unwrap().len();
    let mut file = OpenOptions::new().append(true).open(&log_path).unwrap();
    file.write_all(b"DTRP\0\x02\0\0\0\x20partial").unwrap();
    file.sync_all().unwrap();
    drop(file);

    let reopened = ReplicaManifestStore::open(temporary.path()).unwrap();
    assert_eq!(reopened.manifest(), &manifest);
    assert_eq!(fs::metadata(log_path).unwrap().len(), stable_length);
}

#[test]
fn identity_and_manifest_unknown_versions_fail_closed() {
    let identity_directory = tempdir().unwrap();
    {
        let store =
            NodeIdentityStore::open_or_create(identity_directory.path(), identity(7)).unwrap();
        drop(store);
    }
    let identity_path = identity_directory.path().join("node.identity");
    let mut bytes = fs::read(&identity_path).unwrap();
    bytes[4..6].copy_from_slice(&99_u16.to_be_bytes());
    fs::write(identity_path, bytes).unwrap();
    assert!(matches!(
        NodeIdentityStore::open_or_create(identity_directory.path(), identity(7)),
        Err(StorageError::UnsupportedIdentityVersion { actual: 99 })
    ));

    let manifest_directory = tempdir().unwrap();
    let mut manifest = ReplicaManifest::new();
    manifest.insert(replica(1, 4, "graph-1-shard-4")).unwrap();
    let path;
    {
        let mut store = ReplicaManifestStore::open(manifest_directory.path()).unwrap();
        store.persist(&manifest).unwrap();
        path = store.path().to_path_buf();
    }
    let mut bytes = fs::read(&path).unwrap();
    bytes[4..6].copy_from_slice(&99_u16.to_be_bytes());
    fs::write(path, bytes).unwrap();
    assert!(matches!(
        ReplicaManifestStore::open(manifest_directory.path()),
        Err(StorageError::UnsupportedManifestVersion { actual: 99 })
    ));
}

#[test]
fn predecessor_manifest_versions_are_rejected() {
    for version in [2_u16, 3] {
        let temporary = tempdir().unwrap();
        let path = temporary.path().join("replicas.manifest.log");
        let mut record = Vec::new();
        record.extend_from_slice(b"DTRP");
        record.extend_from_slice(&version.to_be_bytes());
        record.extend_from_slice(&4_u32.to_be_bytes());
        record.extend_from_slice(&0_u32.to_be_bytes());
        record.extend_from_slice(&crc32fast::hash(&record).to_be_bytes());
        fs::write(path, record).unwrap();

        assert!(matches!(
            ReplicaManifestStore::open(temporary.path()),
            Err(StorageError::UnsupportedManifestVersion { actual }) if actual == version
        ));
    }
}

#[test]
fn manifest_round_trips_active_and_dual_applying_backend_slots() {
    let temporary = tempdir().unwrap();
    let source = BackendProfile::new(
        "rocksdb",
        "graph-1-shard-4-generation-7",
        BTreeMap::from([("path".into(), "adapter".into())]),
        BTreeMap::new(),
    )
    .unwrap();
    let target = BackendProfile::new(
        "sidecar",
        "graph-1-shard-4-generation-8",
        BTreeMap::from([
            ("endpoint".into(), "127.0.0.1:19091".into()),
            ("target_provider".into(), "postgresql".into()),
        ]),
        BTreeMap::from([("password".into(), "postgres-main".into())]),
    )
    .unwrap();
    let slot = BackendSlotState::dual_applying(7, source, 8, target, 91, 91).unwrap();
    let entry = ReplicaEntry::new_with_backend(
        1,
        4,
        3,
        vec![7],
        ReplicaRole::Voter,
        5,
        slot.clone(),
        "graph-1-shard-4",
    )
    .unwrap();
    let mut manifest = ReplicaManifest::new();
    manifest.insert(entry).unwrap();

    {
        let mut store = ReplicaManifestStore::open(temporary.path()).unwrap();
        store.persist(&manifest).unwrap();
    }
    let reopened = ReplicaManifestStore::open(temporary.path()).unwrap();
    let recovered = reopened.manifest().replicas().next().unwrap();
    assert_eq!(recovered.backend_generation(), 7);
    assert_eq!(recovered.backend_slot(), &slot);
}

#[test]
fn backend_profiles_are_canonical_and_do_not_embed_secrets() {
    let parameters = BTreeMap::from([
        ("endpoint".into(), "127.0.0.1:19091".into()),
        ("target_provider".into(), "neo4j".into()),
    ]);
    let first = BackendProfile::new(
        "sidecar",
        "neo4j-generation-2",
        parameters.clone(),
        BTreeMap::from([("password".into(), "neo4j-main".into())]),
    )
    .unwrap();
    let second = BackendProfile::new(
        "sidecar",
        "neo4j-generation-2",
        parameters,
        BTreeMap::from([("password".into(), "neo4j-main".into())]),
    )
    .unwrap();
    assert_eq!(first.digest(), second.digest());
    assert!(matches!(
        BackendProfile::new(
            "sidecar",
            "bad-secret",
            BTreeMap::from([("password".into(), "literal-secret".into())]),
            BTreeMap::new(),
        ),
        Err(StorageError::EmbeddedBackendSecret { .. })
    ));
    assert!(matches!(
        BackendProfile::new(
            "../plugin",
            "bad-provider",
            BTreeMap::new(),
            BTreeMap::new()
        ),
        Err(StorageError::InvalidBackendProfile)
    ));
}

#[test]
fn backend_slot_rejects_zero_or_non_consecutive_generations_and_invalid_indices() {
    let profile = || {
        BackendProfile::new(
            "rocksdb",
            "backend",
            BTreeMap::from([("path".into(), "adapter".into())]),
            BTreeMap::new(),
        )
        .unwrap()
    };
    assert!(matches!(
        BackendSlotState::active(0, profile()),
        Err(StorageError::InvalidReplicaGeneration)
    ));
    assert!(matches!(
        BackendSlotState::dual_applying(7, profile(), 9, profile(), 11, 11),
        Err(StorageError::InvalidBackendTransition)
    ));
    assert!(matches!(
        BackendSlotState::dual_applying(7, profile(), 8, profile(), 11, 12),
        Err(StorageError::InvalidBackendTransition)
    ));
}
