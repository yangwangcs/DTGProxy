use std::fs::{self, OpenOptions};
use std::io::Write;

use data_node::{
    NodeIdentity, NodeIdentityStore, ReplicaEntry, ReplicaManifest, ReplicaManifestStore,
    ReplicaRole, StorageError,
};
use tempfile::tempdir;

fn identity(node_id: u64) -> NodeIdentity {
    NodeIdentity::new([0x41; 16], node_id).expect("valid identity")
}

fn replica(graph_id: u64, shard_id: u32, directory: &str) -> ReplicaEntry {
    ReplicaEntry::new(graph_id, shard_id, 3, ReplicaRole::Voter, 5, 7, directory)
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
    file.write_all(b"DTRP\0\x01\0\0\0\x20partial").unwrap();
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
