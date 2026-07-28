use std::{
    collections::BTreeMap,
    future::Future,
    pin::pin,
    task::{Context, Poll, Waker},
};

use dtg_storage::{
    ArtifactChunk, ArtifactKey, ArtifactKind, ArtifactManifest, ArtifactStore, BackendClass,
    BindingRole, CapabilityManifest, CommandId, CommittedShardBatch, ConsensusCommandEnvelope,
    ConsensusEntry, ConsensusStore, EdgeId, EdgeTombstone, EdgeVersion, LogicalMutation,
    LogicalSnapshotSink, LogicalSnapshotSource, ProviderKind, ReadFence, ReplicaBinding,
    ReplicaStateStore, SnapshotChunk, SnapshotManifest, SnapshotRecord, SnapshotRequest,
    TransactionTime, ValidInterval, Value, Version, VertexId, VertexRead, VertexVersion,
};
use dtg_storage_fjall::{FjallArtifactStore, FjallConsensusStore, FjallReplicaStore};
use fjall::{Database, KeyspaceCreateOptions};

fn block_on<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    match future.as_mut().poll(&mut context) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("Fjall storage future unexpectedly yielded"),
    }
}

fn binding(namespace: &str, generation: u64) -> ReplicaBinding {
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
        .replica_id(5)
        .backend_generation(generation)
        .backend_class_digest(class.digest())
        .provider_kind(ProviderKind::Fjall)
        .contract_version(1)
        .layout_version(1)
        .capability_digest(capabilities.digest())
        .namespace_id(namespace)
        .endpoint_profile_ref("local")
        .credential_ref("local")
        .role(BindingRole::Active)
        .build()
        .unwrap()
}

fn vertex(id: u128, version: u64) -> VertexVersion {
    VertexVersion::new(
        VertexId::new(id).unwrap(),
        Version::new(version),
        ValidInterval::new(0, 100).unwrap(),
        TransactionTime::new(version as i64).unwrap(),
        BTreeMap::from([("name".to_owned(), Value::String(format!("v{id}")))]),
    )
    .unwrap()
}

fn edge(id: u128, source: u128, target: u128, version: u64) -> EdgeVersion {
    EdgeVersion::new(
        EdgeId::new(id).unwrap(),
        VertexId::new(source).unwrap(),
        VertexId::new(target).unwrap(),
        "knows",
        Version::new(version),
        ValidInterval::new(0, 100).unwrap(),
        TransactionTime::new(version as i64).unwrap(),
        BTreeMap::new(),
    )
    .unwrap()
}

fn export(
    store: &FjallReplicaStore,
    binding: &ReplicaBinding,
    applied_index: u64,
    snapshot_id: u128,
) -> (
    dtg_storage::SnapshotHeader,
    Vec<SnapshotChunk>,
    SnapshotManifest,
    Vec<SnapshotRecord>,
) {
    let mut reader = block_on(store.begin_snapshot(
        ReadFence::new(binding.clone(), applied_index),
        SnapshotRequest::new(snapshot_id, 2).unwrap(),
    ))
    .unwrap();
    let header = reader.header().clone();
    let mut chunks = Vec::new();
    while let Some(chunk) = block_on(reader.next_chunk()).unwrap() {
        chunks.push(chunk);
    }
    let manifest = block_on(reader.finish()).unwrap();
    let records = chunks
        .iter()
        .flat_map(|chunk| chunk.records().iter().cloned())
        .collect();
    (header, chunks, manifest, records)
}

#[test]
fn snapshot_restore_replaces_dirty_state_preserves_deletes_and_survives_restart() {
    let source_dir = tempfile::tempdir().unwrap();
    let target_dir = tempfile::tempdir().unwrap();
    let source_binding = binding("snapshot-source", 1);
    let source = FjallReplicaStore::open(source_dir.path(), source_binding.clone()).unwrap();
    let deleted_vertex = vertex(10, 1);
    let deleted_edge = edge(20, 101, 202, 1);
    let first = CommittedShardBatch::new(
        source_binding.clone(),
        1,
        1,
        CommandId::new(1).unwrap(),
        vec![
            LogicalMutation::PutVertex(deleted_vertex.clone()),
            LogicalMutation::PutEdge(deleted_edge.clone()),
        ],
    )
    .unwrap();
    block_on(source.apply(first)).unwrap();
    let delete = CommittedShardBatch::new(
        source_binding.clone(),
        1,
        2,
        CommandId::new(2).unwrap(),
        vec![
            LogicalMutation::DeleteVertex(dtg_storage::VertexTombstone::new(
                deleted_vertex.id(),
                Version::new(2),
                TransactionTime::new(2).unwrap(),
            )),
            LogicalMutation::DeleteEdge(EdgeTombstone::new(
                deleted_edge.id(),
                Version::new(2),
                TransactionTime::new(2).unwrap(),
            )),
        ],
    )
    .unwrap();
    block_on(source.apply(delete.clone())).unwrap();
    let (header, chunks, manifest, source_records) = export(&source, &source_binding, 2, 50);

    let target_binding = binding("snapshot-target", 2);
    let target = FjallReplicaStore::open(target_dir.path(), target_binding.clone()).unwrap();
    let consensus = FjallConsensusStore::open(target_dir.path(), target_binding.clone()).unwrap();
    let consensus_entry = ConsensusEntry::new(
        1,
        7,
        8,
        CommandId::new(808).unwrap(),
        ConsensusCommandEnvelope::new(1, vec![8]).unwrap(),
    )
    .unwrap();
    block_on(consensus.append(vec![consensus_entry.clone()])).unwrap();
    let artifact = FjallArtifactStore::open(target_dir.path(), target_binding.clone()).unwrap();
    let artifact_key = ArtifactKey::new(88, 1, ArtifactKind::Checkpoint).unwrap();
    let artifact_chunk = ArtifactChunk::new(artifact_key, 0, vec![8, 8]).unwrap();
    let artifact_manifest =
        ArtifactManifest::new(artifact_key, std::slice::from_ref(&artifact_chunk)).unwrap();
    block_on(artifact.put_chunk(target_binding.clone(), artifact_chunk)).unwrap();
    block_on(artifact.commit_manifest(target_binding.clone(), artifact_manifest.clone())).unwrap();
    let dirty = vertex(99, 1);
    block_on(
        target.apply(
            CommittedShardBatch::new(
                target_binding.clone(),
                9,
                1,
                CommandId::new(99).unwrap(),
                vec![LogicalMutation::PutVertex(dirty.clone())],
            )
            .unwrap(),
        ),
    )
    .unwrap();
    let mut writer = block_on(target.begin_restore(target_binding.clone(), header)).unwrap();
    for chunk in chunks {
        block_on(writer.write_chunk(chunk)).unwrap();
    }
    block_on(writer.commit(manifest)).unwrap();
    let (_, _, _, restored_records) = export(&target, &target_binding, 2, 51);
    assert_eq!(restored_records, source_records);
    assert_eq!(
        block_on(consensus.entries(8, 9, 4096)).unwrap(),
        vec![consensus_entry]
    );
    assert_eq!(
        block_on(artifact.manifest(target_binding.clone(), artifact_key)).unwrap(),
        Some(artifact_manifest)
    );
    drop(target);
    drop(consensus);
    drop(artifact);

    let reopened = FjallReplicaStore::open(target_dir.path(), target_binding.clone()).unwrap();
    let view =
        block_on(reopened.begin_read_view(ReadFence::new(target_binding.clone(), 2))).unwrap();
    for id in [dirty.id(), deleted_vertex.id()] {
        assert!(
            block_on(view.get_vertex(VertexRead::new(id, 10, TransactionTime::new(10).unwrap())))
                .unwrap()
                .is_none()
        );
    }
    let replay = CommittedShardBatch::new(
        target_binding.clone(),
        delete.raft_term(),
        delete.raft_index(),
        delete.command_id(),
        delete.mutations().to_vec(),
    )
    .unwrap();
    assert!(block_on(reopened.apply(replay)).unwrap().replayed());
    let continued = vertex(100, 1);
    block_on(
        reopened.apply(
            CommittedShardBatch::new(
                target_binding.clone(),
                2,
                3,
                CommandId::new(3).unwrap(),
                vec![LogicalMutation::PutVertex(continued.clone())],
            )
            .unwrap(),
        ),
    )
    .unwrap();
    drop(reopened);
    let restarted = FjallReplicaStore::open(target_dir.path(), target_binding.clone()).unwrap();
    let restarted_view =
        block_on(restarted.begin_read_view(ReadFence::new(target_binding, 3))).unwrap();
    assert_eq!(
        block_on(restarted_view.get_vertex(VertexRead::new(
            continued.id(),
            10,
            TransactionTime::new(10).unwrap()
        )))
        .unwrap(),
        Some(continued)
    );
}

#[test]
fn native_adjacency_partitions_drop_stale_rows_for_batch_updates_and_deletes() {
    let dir = tempfile::tempdir().unwrap();
    let store_binding = binding("adjacency-native", 1);
    let store = FjallReplicaStore::open(dir.path(), store_binding.clone()).unwrap();
    let old = edge(30, 1, 2, 1);
    let moved = edge(30, 3, 4, 2);
    block_on(
        store.apply(
            CommittedShardBatch::new(
                store_binding.clone(),
                1,
                1,
                CommandId::new(30).unwrap(),
                vec![
                    LogicalMutation::PutEdge(old),
                    LogicalMutation::PutEdge(moved.clone()),
                ],
            )
            .unwrap(),
        ),
    )
    .unwrap();
    drop(store);
    assert_adjacency_counts(dir.path(), 1, 1);

    let reopened = FjallReplicaStore::open(dir.path(), store_binding.clone()).unwrap();
    block_on(
        reopened.apply(
            CommittedShardBatch::new(
                store_binding,
                1,
                2,
                CommandId::new(31).unwrap(),
                vec![
                    LogicalMutation::PutEdge(edge(30, 5, 6, 3)),
                    LogicalMutation::DeleteEdge(EdgeTombstone::new(
                        moved.id(),
                        Version::new(4),
                        TransactionTime::new(4).unwrap(),
                    )),
                ],
            )
            .unwrap(),
        ),
    )
    .unwrap();
    drop(reopened);
    assert_adjacency_counts(dir.path(), 0, 0);
}

fn assert_adjacency_counts(path: &std::path::Path, outgoing: usize, incoming: usize) {
    let db = Database::builder(path).open().unwrap();
    let adjacency_out = db
        .keyspace("adjacency_out", KeyspaceCreateOptions::default)
        .unwrap();
    let adjacency_in = db
        .keyspace("adjacency_in", KeyspaceCreateOptions::default)
        .unwrap();
    assert_eq!(adjacency_out.iter().count(), outgoing);
    assert_eq!(adjacency_in.iter().count(), incoming);
}
