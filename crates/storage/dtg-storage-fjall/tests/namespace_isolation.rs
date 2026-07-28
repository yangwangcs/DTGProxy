use std::{
    fs,
    future::Future,
    pin::pin,
    task::{Context, Poll, Waker},
};

use dtg_storage::{
    ArtifactChunk, ArtifactKey, ArtifactKind, ArtifactManifest, ArtifactStore, BackendClass,
    BindingRole, CapabilityManifest, Digest32, ProviderKind, ReplicaBinding, StorageError,
};
use dtg_storage_fjall::{FjallArtifactStore, FjallConsensusStore, FjallReplicaStore};
use fjall::{Database, KeyspaceCreateOptions, PersistMode};

fn block_on<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    match future.as_mut().poll(&mut context) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("Fjall storage future unexpectedly yielded"),
    }
}

fn fixture_binding(replica: u64, generation: u64) -> ReplicaBinding {
    let capabilities = CapabilityManifest::from_names([
        "adjacency",
        "immutable-read-view",
        "logical-snapshot",
        "point",
    ])
    .unwrap();
    let class = BackendClass::new(
        ProviderKind::Fjall,
        1,
        1,
        capabilities.names().map(str::to_owned),
    )
    .unwrap();
    ReplicaBinding::builder()
        .cluster_id(1)
        .graph_id(2)
        .shard_id(3)
        .placement_epoch(4)
        .replica_id(replica)
        .backend_generation(generation)
        .backend_class_digest(class.digest())
        .provider_kind(ProviderKind::Fjall)
        .contract_version(1)
        .layout_version(1)
        .capability_digest(capabilities.digest())
        .namespace_id("namespace-isolation")
        .endpoint_profile_ref("local")
        .credential_ref("local")
        .role(BindingRole::Active)
        .build()
        .unwrap()
}

#[test]
fn mismatched_binding_cannot_reopen_namespace() {
    let dir = tempfile::tempdir().unwrap();
    let binding = fixture_binding(1, 1);
    let _replica = FjallReplicaStore::open(dir.path(), binding.clone()).unwrap();
    let _consensus = FjallConsensusStore::open(dir.path(), binding.clone()).unwrap();
    let _artifact = FjallArtifactStore::open(dir.path(), binding).unwrap();
    let error = FjallReplicaStore::open(dir.path(), fixture_binding(2, 1)).unwrap_err();
    assert_eq!(error.code(), "DTG-STORAGE-NAMESPACE-OWNER");
}

#[test]
fn artifact_chunks_and_manifest_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let binding = fixture_binding(1, 1);
    let key = ArtifactKey::new(10, 2, ArtifactKind::Checkpoint).unwrap();
    let chunks = vec![
        ArtifactChunk::new(key, 0, vec![1, 2]).unwrap(),
        ArtifactChunk::new(key, 1, vec![3, 4]).unwrap(),
    ];
    let manifest = ArtifactManifest::new(key, &chunks).unwrap();
    let store = FjallArtifactStore::open(dir.path(), binding.clone()).unwrap();
    for chunk in &chunks {
        block_on(store.put_chunk(binding.clone(), chunk.clone())).unwrap();
    }
    block_on(store.commit_manifest(binding.clone(), manifest.clone())).unwrap();
    drop(store);

    let reopened = FjallArtifactStore::open(dir.path(), binding.clone()).unwrap();
    assert_eq!(
        block_on(reopened.get_chunk(binding.clone(), key, 1)).unwrap(),
        Some(chunks[1].clone())
    );
    assert_eq!(
        block_on(reopened.manifest(binding, key)).unwrap(),
        Some(manifest)
    );
}

#[test]
fn committed_artifact_chunks_are_immutable_but_identical_replay_is_allowed() {
    let dir = tempfile::tempdir().unwrap();
    let binding = fixture_binding(1, 1);
    let key = ArtifactKey::new(20, 2, ArtifactKind::Result).unwrap();
    let chunks = vec![ArtifactChunk::new(key, 0, vec![1, 2, 3]).unwrap()];
    let manifest = ArtifactManifest::new(key, &chunks).unwrap();
    let store = FjallArtifactStore::open(dir.path(), binding.clone()).unwrap();
    block_on(store.put_chunk(binding.clone(), chunks[0].clone())).unwrap();
    block_on(store.commit_manifest(binding.clone(), manifest.clone())).unwrap();

    block_on(store.put_chunk(binding.clone(), chunks[0].clone())).unwrap();
    block_on(store.commit_manifest(binding.clone(), manifest)).unwrap();
    let divergent = ArtifactChunk::new(key, 0, vec![9, 9, 9]).unwrap();
    assert!(matches!(
        block_on(store.put_chunk(binding, divergent)),
        Err(StorageError::InvalidArtifact(_))
    ));
}

#[test]
fn artifact_conflict_delete_corruption_reopen_and_binding_fence_are_enforced() {
    let dir = tempfile::tempdir().unwrap();
    let binding = fixture_binding(1, 1);
    let wrong = fixture_binding(2, 1);
    let key = ArtifactKey::new(30, 3, ArtifactKind::Checkpoint).unwrap();
    let chunk = ArtifactChunk::new(key, 0, vec![4, 5, 6]).unwrap();
    let manifest = ArtifactManifest::new(key, std::slice::from_ref(&chunk)).unwrap();
    let store = FjallArtifactStore::open(dir.path(), binding.clone()).unwrap();
    assert!(matches!(
        block_on(store.put_chunk(wrong.clone(), chunk.clone())),
        Err(StorageError::StaleBinding { .. })
    ));
    block_on(store.put_chunk(binding.clone(), chunk.clone())).unwrap();
    let conflicting = ArtifactManifest::new(
        key,
        &[chunk.clone(), ArtifactChunk::new(key, 1, vec![7]).unwrap()],
    )
    .unwrap();
    assert!(matches!(
        block_on(store.commit_manifest(binding.clone(), conflicting)),
        Err(StorageError::InvalidArtifact(_))
    ));
    block_on(store.commit_manifest(binding.clone(), manifest)).unwrap();
    assert!(matches!(
        block_on(store.manifest(wrong, key)),
        Err(StorageError::StaleBinding { .. })
    ));
    drop(store);

    let db = Database::builder(dir.path()).open().unwrap();
    let artifact = db
        .keyspace("artifact", KeyspaceCreateOptions::default)
        .unwrap();
    let raw_key = artifact_chunk_key(key, 0);
    let mut bytes = artifact.get(&raw_key).unwrap().unwrap().to_vec();
    *bytes.last_mut().unwrap() ^= 0xff;
    artifact.insert(raw_key, bytes).unwrap();
    db.persist(PersistMode::SyncAll).unwrap();
    drop(artifact);
    drop(db);

    let reopened = FjallArtifactStore::open(dir.path(), binding.clone()).unwrap();
    assert!(block_on(reopened.get_chunk(binding.clone(), key, 0)).is_err());
    assert!(block_on(reopened.manifest(binding.clone(), key)).is_err());
    block_on(reopened.delete(binding.clone(), key)).unwrap();
    drop(reopened);
    let empty = FjallArtifactStore::open(dir.path(), binding.clone()).unwrap();
    assert_eq!(
        block_on(empty.get_chunk(binding.clone(), key, 0)).unwrap(),
        None
    );
    assert_eq!(block_on(empty.manifest(binding, key)).unwrap(), None);
}

#[cfg(unix)]
#[test]
fn symlink_dotdot_and_direct_paths_share_one_physical_namespace() {
    use std::os::unix::fs::symlink;

    let dir = tempfile::tempdir().unwrap();
    let physical_parent = dir.path().join("physical");
    let alias_parent = dir.path().join("alias");
    let detour = physical_parent.join("detour");
    fs::create_dir(&physical_parent).unwrap();
    fs::create_dir(&detour).unwrap();
    symlink(&physical_parent, &alias_parent).unwrap();
    let binding = fixture_binding(1, 1);
    let direct = physical_parent.join("db");
    let symlinked = alias_parent.join("db");
    let dotdot = detour.join("..").join("db");

    let _replica = FjallReplicaStore::open(&symlinked, binding.clone()).unwrap();
    let _consensus = FjallConsensusStore::open(&dotdot, binding.clone()).unwrap();
    let _artifact = FjallArtifactStore::open(&direct, binding).unwrap();
    let error = FjallReplicaStore::open(&direct, fixture_binding(2, 1)).unwrap_err();
    assert_eq!(error.code(), "DTG-STORAGE-NAMESPACE-OWNER");
}

#[test]
fn every_fjall_open_rejects_incompatible_bindings_before_creating_a_namespace() {
    let dir = tempfile::tempdir().unwrap();
    let valid = fixture_binding(1, 1);
    let non_fjall = valid
        .to_builder()
        .provider_kind(ProviderKind::PostgreSql)
        .build()
        .unwrap();
    let wrong_contract = valid.to_builder().contract_version(2).build().unwrap();
    let wrong_layout = valid.to_builder().layout_version(2).build().unwrap();
    let wrong_class = valid
        .to_builder()
        .backend_class_digest(Digest32::new([9; 32]))
        .build()
        .unwrap();
    let wrong_capabilities = valid
        .to_builder()
        .capability_digest(Digest32::new([8; 32]))
        .build()
        .unwrap();

    for (ordinal, binding) in [
        non_fjall,
        wrong_contract,
        wrong_layout,
        wrong_class,
        wrong_capabilities,
    ]
    .into_iter()
    .enumerate()
    {
        let replica_path = dir.path().join(format!("replica-{ordinal}"));
        let consensus_path = dir.path().join(format!("consensus-{ordinal}"));
        let artifact_path = dir.path().join(format!("artifact-{ordinal}"));
        assert!(matches!(
            FjallReplicaStore::open(&replica_path, binding.clone()),
            Err(StorageError::InvalidBinding(_))
        ));
        assert!(matches!(
            FjallConsensusStore::open(&consensus_path, binding.clone()),
            Err(StorageError::InvalidBinding(_))
        ));
        assert!(matches!(
            FjallArtifactStore::open(&artifact_path, binding),
            Err(StorageError::InvalidBinding(_))
        ));
        assert!(!replica_path.exists());
        assert!(!consensus_path.exists());
        assert!(!artifact_path.exists());
    }
}

fn artifact_chunk_key(key: ArtifactKey, ordinal: u64) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(34);
    encoded.push(b'c');
    encoded.extend_from_slice(&key.job_id().to_be_bytes());
    encoded.extend_from_slice(&key.generation().to_be_bytes());
    encoded.push(match key.kind() {
        ArtifactKind::Checkpoint => 1,
        ArtifactKind::Result => 2,
    });
    encoded.extend_from_slice(&ordinal.to_be_bytes());
    encoded
}
