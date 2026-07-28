use std::{
    future::Future,
    pin::pin,
    task::{Context, Poll, Waker},
};

use dtg_storage::{
    ArtifactChunk, ArtifactKey, ArtifactKind, ArtifactManifest, ArtifactStore, BackendClass,
    BindingRole, CapabilityManifest, ProviderKind, ReplicaBinding,
};
use dtg_storage_fjall::{FjallArtifactStore, FjallConsensusStore, FjallReplicaStore};

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
