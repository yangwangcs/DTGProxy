use std::{
    collections::BTreeMap,
    future::Future,
    pin::pin,
    task::{Context, Poll, Waker},
};

#[cfg(feature = "tck")]
use std::thread;

#[cfg(feature = "tck")]
use dtg_storage::SnapshotReplayRecord;

use dtg_storage::{
    ArtifactChunk, ArtifactKey, ArtifactKind, ArtifactManifest, ArtifactStore, BackendClass,
    BindingRole, CapabilityManifest, CommandId, CommittedShardBatch, ConsensusCommandEnvelope,
    ConsensusEntry, ConsensusStore, EdgeId, EdgeTombstone, EdgeVersion, LogicalMutation,
    LogicalSnapshotSink, LogicalSnapshotSource, ProviderKind, ReadFence, ReplicaBinding,
    ReplicaStateStore, SnapshotChunk, SnapshotManifest, SnapshotRecord, SnapshotRequest,
    StorageError, TransactionTime, ValidInterval, Value, Version, VertexId, VertexRead,
    VertexVersion,
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
fn snapshot_history_authentication_preserves_duplicate_graph_mutations() {
    let source_dir = tempfile::tempdir().unwrap();
    let target_dir = tempfile::tempdir().unwrap();
    let source_binding = binding("snapshot-duplicate-source", 1);
    let source = FjallReplicaStore::open(source_dir.path(), source_binding.clone()).unwrap();
    let duplicate = vertex(30, 1);
    block_on(
        source.apply(
            CommittedShardBatch::new(
                source_binding.clone(),
                1,
                1,
                CommandId::new(30).unwrap(),
                vec![
                    LogicalMutation::PutVertex(duplicate.clone()),
                    LogicalMutation::PutVertex(duplicate.clone()),
                ],
            )
            .unwrap(),
        ),
    )
    .unwrap();
    let (header, chunks, manifest, source_records) = export(&source, &source_binding, 1, 30);
    assert_eq!(
        source_records
            .iter()
            .filter(
                |record| matches!(record, SnapshotRecord::Vertex(vertex) if vertex == &duplicate)
            )
            .count(),
        2
    );

    let target_binding = binding("snapshot-duplicate-target", 2);
    let target = FjallReplicaStore::open(target_dir.path(), target_binding.clone()).unwrap();
    let mut writer = block_on(target.begin_restore(target_binding.clone(), header)).unwrap();
    for chunk in chunks {
        block_on(writer.write_chunk(chunk)).unwrap();
    }
    block_on(writer.commit(manifest)).unwrap();
    let (_, _, _, restored_records) = export(&target, &target_binding, 1, 31);
    assert_eq!(restored_records, source_records);
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

#[test]
fn stale_snapshot_fence_is_rejected_instead_of_labeling_newer_records() {
    let dir = tempfile::tempdir().unwrap();
    let store_binding = binding("stale-snapshot-fence", 1);
    let store = FjallReplicaStore::open(dir.path(), store_binding.clone()).unwrap();
    block_on(
        store.apply(
            CommittedShardBatch::new(
                store_binding.clone(),
                1,
                1,
                CommandId::new(1).unwrap(),
                vec![LogicalMutation::PutVertex(vertex(1, 1))],
            )
            .unwrap(),
        ),
    )
    .unwrap();

    let error = match block_on(store.begin_snapshot(
        ReadFence::new(store_binding, 0),
        SnapshotRequest::new(100, 16).unwrap(),
    )) {
        Ok(_) => panic!("stale snapshot fence was accepted"),
        Err(error) => error,
    };
    assert_eq!(
        error,
        StorageError::ReadFenceUnavailable {
            requested: 0,
            applied: 1,
        }
    );
}

#[cfg(feature = "tck")]
#[test]
fn snapshot_capture_blocks_apply_at_the_post_fence_materialization_boundary() {
    let source_dir = tempfile::tempdir().unwrap();
    let target_dir = tempfile::tempdir().unwrap();
    let source_binding = binding("export-race-source", 1);
    let source = FjallReplicaStore::open(source_dir.path(), source_binding.clone()).unwrap();
    block_on(
        source.apply(
            CommittedShardBatch::new(
                source_binding.clone(),
                1,
                1,
                CommandId::new(11).unwrap(),
                vec![LogicalMutation::PutVertex(vertex(11, 1))],
            )
            .unwrap(),
        ),
    )
    .unwrap();
    let apply_handle = FjallReplicaStore::open(source_dir.path(), source_binding.clone()).unwrap();
    let export_handle = FjallReplicaStore::open(source_dir.path(), source_binding.clone()).unwrap();
    let capture_pause = export_handle.arm_tck_snapshot_after_fence_pause().unwrap();
    let export_binding = source_binding.clone();
    let export_thread = thread::spawn(move || {
        let mut reader = block_on(export_handle.begin_snapshot(
            ReadFence::new(export_binding, 1),
            SnapshotRequest::new(111, 32).unwrap(),
        ))
        .unwrap();
        let header = reader.header().clone();
        let mut chunks = Vec::new();
        while let Some(chunk) = block_on(reader.next_chunk()).unwrap() {
            chunks.push(chunk);
        }
        let manifest = block_on(reader.finish()).unwrap();
        (header, chunks, manifest)
    });

    capture_pause.wait_until_reached().unwrap();
    let apply_binding = source_binding.clone();
    let apply_thread = thread::spawn(move || {
        block_on(
            apply_handle.apply(
                CommittedShardBatch::new(
                    apply_binding,
                    1,
                    2,
                    CommandId::new(12).unwrap(),
                    vec![LogicalMutation::PutVertex(vertex(12, 2))],
                )
                .unwrap(),
            ),
        )
    });
    source.wait_for_tck_graph_waiter().unwrap();
    assert_eq!(block_on(source.applied_index()).unwrap(), 1);

    capture_pause.release().unwrap();
    let (header, chunks, manifest) = export_thread.join().unwrap();
    assert_eq!(header.applied_index(), 1);
    assert!(apply_thread.join().unwrap().is_ok());
    assert_eq!(block_on(source.applied_index()).unwrap(), 2);

    let target_binding = binding("export-race-target", 2);
    let target = FjallReplicaStore::open(target_dir.path(), target_binding.clone()).unwrap();
    let mut writer = block_on(target.begin_restore(target_binding.clone(), header)).unwrap();
    for chunk in chunks {
        block_on(writer.write_chunk(chunk)).unwrap();
    }
    block_on(writer.commit(manifest)).unwrap();
    assert_eq!(block_on(target.applied_index()).unwrap(), 1);
    let restored =
        block_on(target.begin_read_view(ReadFence::new(target_binding.clone(), 1))).unwrap();
    assert_eq!(
        block_on(restored.get_vertex(VertexRead::new(
            VertexId::new(11).unwrap(),
            10,
            TransactionTime::new(10).unwrap(),
        )))
        .unwrap(),
        Some(vertex(11, 1))
    );
    assert!(
        block_on(restored.get_vertex(VertexRead::new(
            VertexId::new(12).unwrap(),
            10,
            TransactionTime::new(10).unwrap(),
        )))
        .unwrap()
        .is_none()
    );
}

#[cfg(feature = "tck")]
#[test]
fn concurrent_handles_publish_exactly_one_divergent_next_batch() {
    let dir = tempfile::tempdir().unwrap();
    let store_binding = binding("concurrent-apply", 1);
    let first = FjallReplicaStore::open(dir.path(), store_binding.clone()).unwrap();
    let second = FjallReplicaStore::open(dir.path(), store_binding.clone()).unwrap();
    let first_vertices = [vertex(1, 1), vertex(2, 1)];
    let second_vertices = [vertex(101, 1), vertex(102, 1)];
    let first_mutations = first_vertices
        .iter()
        .cloned()
        .map(LogicalMutation::PutVertex)
        .collect::<Vec<_>>();
    let second_mutations = second_vertices
        .iter()
        .cloned()
        .map(LogicalMutation::PutVertex)
        .collect::<Vec<_>>();
    let first_batch = CommittedShardBatch::new(
        store_binding.clone(),
        7,
        1,
        CommandId::new(101).unwrap(),
        first_mutations.clone(),
    )
    .unwrap();
    let second_batch = CommittedShardBatch::new(
        store_binding.clone(),
        8,
        1,
        CommandId::new(202).unwrap(),
        second_mutations.clone(),
    )
    .unwrap();
    let expected_replay = SnapshotReplayRecord::new(
        first_batch.raft_index(),
        first_batch.raft_term(),
        first_batch.command_id(),
        first_batch.mutation_digest(),
    )
    .unwrap();
    let commit_pause = first.arm_tck_apply_before_commit_pause().unwrap();
    let first_thread = thread::spawn(move || block_on(first.apply(first_batch)));
    commit_pause.wait_until_reached().unwrap();
    let second_thread = thread::spawn(move || block_on(second.apply(second_batch)));
    let observer = FjallReplicaStore::open(dir.path(), store_binding.clone()).unwrap();
    observer.wait_for_tck_graph_waiter().unwrap();
    assert_eq!(block_on(observer.applied_index()).unwrap(), 0);

    commit_pause.release().unwrap();
    let first_result = first_thread.join().unwrap();
    let second_result = second_thread.join().unwrap();
    assert!(first_result.is_ok());
    assert_eq!(
        second_result.unwrap_err(),
        StorageError::ReplayMismatch { raft_index: 1 }
    );

    let reopened = FjallReplicaStore::open(dir.path(), store_binding.clone()).unwrap();
    assert_eq!(block_on(reopened.applied_index()).unwrap(), 1);
    let view =
        block_on(reopened.begin_read_view(ReadFence::new(store_binding.clone(), 1))).unwrap();
    for winner in &first_vertices {
        assert_eq!(
            block_on(view.get_vertex(VertexRead::new(
                winner.id(),
                10,
                TransactionTime::new(10).unwrap(),
            )))
            .unwrap(),
            Some(winner.clone())
        );
    }
    for loser in &second_vertices {
        assert!(
            block_on(view.get_vertex(VertexRead::new(
                loser.id(),
                10,
                TransactionTime::new(10).unwrap(),
            )))
            .unwrap()
            .is_none()
        );
    }

    let (_, _, _, records) = export(&reopened, &store_binding, 1, 101);
    let history = records
        .iter()
        .filter_map(|record| match record {
            SnapshotRecord::Vertex(vertex) => Some(vertex.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(history, first_vertices);
    let changes = records
        .iter()
        .filter_map(|record| match record {
            SnapshotRecord::Change(change) => Some(change.mutation().clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(changes, first_mutations);
    let replay = records
        .iter()
        .filter_map(|record| match record {
            SnapshotRecord::Replay(replay) => Some(replay.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(replay, vec![expected_replay]);
    assert!(records.iter().all(|record| match record {
        SnapshotRecord::Vertex(vertex) => !second_vertices.contains(vertex),
        SnapshotRecord::Change(change) => !second_mutations.contains(change.mutation()),
        _ => true,
    }));
}

#[cfg(feature = "tck")]
#[test]
fn duplicate_apply_pause_arm_preserves_first_token_without_orphaning_future_applies() {
    let dir = tempfile::tempdir().unwrap();
    let store_binding = binding("duplicate-apply-pause", 1);
    let store = FjallReplicaStore::open(dir.path(), store_binding.clone()).unwrap();
    let first_pause = store.arm_tck_apply_before_commit_pause().unwrap();
    match store.arm_tck_apply_before_commit_pause() {
        Ok(_) => panic!("duplicate apply pause arm unexpectedly succeeded"),
        Err(StorageError::Internal(message)) => {
            assert_eq!(message, "Fjall graph pause point is already armed");
        }
        Err(error) => panic!("duplicate apply pause arm returned {error:?}"),
    }

    let paused_store = store.clone();
    let paused_binding = store_binding.clone();
    let paused_apply = thread::spawn(move || {
        block_on(
            paused_store.apply(
                CommittedShardBatch::new(
                    paused_binding,
                    1,
                    1,
                    CommandId::new(601).unwrap(),
                    vec![LogicalMutation::PutVertex(vertex(601, 1))],
                )
                .unwrap(),
            ),
        )
    });
    first_pause.wait_until_reached().unwrap();
    first_pause.release().unwrap();
    assert!(paused_apply.join().unwrap().is_ok());

    assert!(
        block_on(
            store.apply(
                CommittedShardBatch::new(
                    store_binding.clone(),
                    2,
                    2,
                    CommandId::new(602).unwrap(),
                    vec![LogicalMutation::PutVertex(vertex(602, 2))],
                )
                .unwrap(),
            )
        )
        .is_ok()
    );
    assert_eq!(block_on(store.applied_index()).unwrap(), 2);
}

#[cfg(feature = "tck")]
#[test]
fn apply_racing_restore_finishes_as_one_complete_serial_outcome() {
    let source_dir = tempfile::tempdir().unwrap();
    let target_dir = tempfile::tempdir().unwrap();
    let source_binding = binding("restore-race-source", 1);
    let source = FjallReplicaStore::open(source_dir.path(), source_binding.clone()).unwrap();
    block_on(
        source.apply(
            CommittedShardBatch::new(
                source_binding.clone(),
                1,
                1,
                CommandId::new(301).unwrap(),
                vec![LogicalMutation::PutVertex(vertex(1, 1))],
            )
            .unwrap(),
        ),
    )
    .unwrap();
    block_on(
        source.apply(
            CommittedShardBatch::new(
                source_binding.clone(),
                2,
                2,
                CommandId::new(302).unwrap(),
                vec![LogicalMutation::PutVertex(vertex(2, 2))],
            )
            .unwrap(),
        ),
    )
    .unwrap();
    let (header, chunks, manifest, source_records) = export(&source, &source_binding, 2, 301);

    let target_binding = binding("restore-race-target", 2);
    let restore_handle =
        FjallReplicaStore::open(target_dir.path(), target_binding.clone()).unwrap();
    let apply_handle = FjallReplicaStore::open(target_dir.path(), target_binding.clone()).unwrap();
    block_on(
        apply_handle.apply(
            CommittedShardBatch::new(
                target_binding.clone(),
                1,
                1,
                CommandId::new(401).unwrap(),
                vec![LogicalMutation::PutVertex(vertex(20_001, 1))],
            )
            .unwrap(),
        ),
    )
    .unwrap();
    block_on(
        apply_handle.apply(
            CommittedShardBatch::new(
                target_binding.clone(),
                2,
                2,
                CommandId::new(402).unwrap(),
                vec![LogicalMutation::PutVertex(vertex(20_002, 2))],
            )
            .unwrap(),
        ),
    )
    .unwrap();
    let raced_vertex = vertex(99_999, 2);
    let raced_batch = CommittedShardBatch::new(
        target_binding.clone(),
        3,
        3,
        CommandId::new(403).unwrap(),
        vec![LogicalMutation::PutVertex(raced_vertex.clone())],
    )
    .unwrap();
    let mut writer =
        block_on(restore_handle.begin_restore(target_binding.clone(), header)).unwrap();
    for chunk in chunks {
        block_on(writer.write_chunk(chunk)).unwrap();
    }
    let commit_pause = restore_handle
        .arm_tck_restore_before_commit_pause()
        .unwrap();
    let restore_thread = thread::spawn(move || block_on(writer.commit(manifest)));
    commit_pause.wait_until_reached().unwrap();
    let apply_thread = thread::spawn(move || block_on(apply_handle.apply(raced_batch)));
    restore_handle.wait_for_tck_graph_waiter().unwrap();
    assert_eq!(block_on(restore_handle.applied_index()).unwrap(), 2);

    commit_pause.release().unwrap();
    let restore_result = restore_thread.join().unwrap();
    assert!(restore_result.is_ok());
    assert!(apply_thread.join().unwrap().is_ok());

    let final_store = FjallReplicaStore::open(target_dir.path(), target_binding.clone()).unwrap();
    assert_eq!(block_on(final_store.applied_index()).unwrap(), 3);
    let (_, _, _, final_records) = export(&final_store, &target_binding, 3, 302);
    let base_records = final_records
        .iter()
        .filter(|record| match record {
            SnapshotRecord::Vertex(vertex) => vertex.id() != raced_vertex.id(),
            SnapshotRecord::Replay(replay) => replay.raft_index() != 3,
            SnapshotRecord::Change(change) => change.raft_index() != 3,
            _ => true,
        })
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(base_records, source_records);
    assert_eq!(
        final_records
            .iter()
            .filter(
                |record| matches!(record, SnapshotRecord::Vertex(vertex) if vertex == &raced_vertex)
            )
            .count(),
        1
    );
    assert_eq!(
        final_records
            .iter()
            .filter(|record| matches!(record, SnapshotRecord::Replay(replay) if replay.raft_index() == 3))
            .count(),
        1
    );
    assert_eq!(
        final_records
            .iter()
            .filter(|record| matches!(record, SnapshotRecord::Change(change) if change.raft_index() == 3 && change.mutation() == &LogicalMutation::PutVertex(raced_vertex.clone())))
            .count(),
        1
    );
}

#[test]
fn restore_rejects_missing_graph_history_without_publication() {
    assert_graph_history_corruption_rejected("missing-history", |records| {
        let position = records
            .iter()
            .position(|record| {
                matches!(record, SnapshotRecord::Vertex(vertex) if vertex.id() == VertexId::new(1).unwrap())
            })
            .unwrap();
        records.remove(position);
    });
}

#[test]
fn restore_rejects_altered_graph_history_without_publication() {
    assert_graph_history_corruption_rejected("altered-history", |records| {
        let position = records
            .iter()
            .position(|record| {
                matches!(record, SnapshotRecord::Vertex(vertex) if vertex.id() == VertexId::new(2).unwrap())
            })
            .unwrap();
        records[position] = SnapshotRecord::Vertex(vertex(20, 1));
    });
}

fn assert_graph_history_corruption_rejected(
    name: &str,
    corrupt: impl FnOnce(&mut Vec<SnapshotRecord>),
) {
    let source_dir = tempfile::tempdir().unwrap();
    let source_binding = binding("history-digest-source", 1);
    let source = FjallReplicaStore::open(source_dir.path(), source_binding.clone()).unwrap();
    block_on(
        source.apply(
            CommittedShardBatch::new(
                source_binding.clone(),
                1,
                1,
                CommandId::new(450).unwrap(),
                vec![
                    LogicalMutation::PutVertex(vertex(1, 1)),
                    LogicalMutation::PutVertex(vertex(2, 1)),
                ],
            )
            .unwrap(),
        ),
    )
    .unwrap();
    let (header, _, _, mut corrupted_records) = export(&source, &source_binding, 1, 450);
    corrupt(&mut corrupted_records);

    let target_dir = tempfile::tempdir().unwrap();
    let target_binding = binding(&format!("history-digest-target-{name}"), 2);
    let target = FjallReplicaStore::open(target_dir.path(), target_binding.clone()).unwrap();
    let dirty = vertex(80, 1);
    block_on(
        target.apply(
            CommittedShardBatch::new(
                target_binding.clone(),
                9,
                1,
                CommandId::new(800).unwrap(),
                vec![LogicalMutation::PutVertex(dirty.clone())],
            )
            .unwrap(),
        ),
    )
    .unwrap();
    let chunks = corrupted_records
        .chunks(3)
        .enumerate()
        .map(|(chunk_ordinal, records)| {
            SnapshotChunk::new(header.snapshot_id(), chunk_ordinal as u64, records.to_vec())
        })
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let manifest = SnapshotManifest::new(&header, &chunks).unwrap();
    let mut writer = block_on(target.begin_restore(target_binding.clone(), header)).unwrap();
    for chunk in chunks {
        block_on(writer.write_chunk(chunk)).unwrap();
    }
    let error = block_on(writer.commit(manifest)).unwrap_err();
    assert_eq!(error.code(), "DTG-STORAGE-SNAPSHOT-CORRUPT", "{name}");
    assert_eq!(block_on(target.applied_index()).unwrap(), 1, "{name}");
    let view = block_on(target.begin_read_view(ReadFence::new(target_binding, 1))).unwrap();
    assert_eq!(
        block_on(view.get_vertex(VertexRead::new(
            dirty.id(),
            10,
            TransactionTime::new(10).unwrap(),
        )))
        .unwrap(),
        Some(dirty),
        "{name}"
    );
}

#[test]
fn restore_rejects_semantically_corrupt_change_sequences_without_publication() {
    let source_dir = tempfile::tempdir().unwrap();
    let source_binding = binding("digest-source", 1);
    let source = FjallReplicaStore::open(source_dir.path(), source_binding.clone()).unwrap();
    block_on(
        source.apply(
            CommittedShardBatch::new(
                source_binding.clone(),
                1,
                1,
                CommandId::new(501).unwrap(),
                vec![
                    LogicalMutation::PutVertex(vertex(1, 1)),
                    LogicalMutation::PutVertex(vertex(2, 1)),
                ],
            )
            .unwrap(),
        ),
    )
    .unwrap();
    block_on(
        source.apply(
            CommittedShardBatch::new(
                source_binding.clone(),
                2,
                2,
                CommandId::new(502).unwrap(),
                vec![
                    LogicalMutation::PutVertex(vertex(3, 2)),
                    LogicalMutation::PutVertex(vertex(4, 2)),
                ],
            )
            .unwrap(),
        ),
    )
    .unwrap();
    let (header, _, _, records) = export(&source, &source_binding, 2, 501);
    let change_positions = records
        .iter()
        .enumerate()
        .filter_map(|(position, record)| match record {
            SnapshotRecord::Change(change) => Some((change.cursor(), position)),
            _ => None,
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(change_positions.len(), 4);
    let position = |raft_index, mutation_ordinal| {
        change_positions[&dtg_storage::ChangeCursor::new(raft_index, mutation_ordinal)]
    };

    let mut corruptions = Vec::new();
    let mut missing = records.clone();
    missing.remove(position(2, 1));
    corruptions.push(("missing", missing));

    let mut extra = records.clone();
    extra.push(SnapshotRecord::Change(dtg_storage::ChangeRecord::new(
        dtg_storage::ChangeCursor::new(2, 2),
        LogicalMutation::PutVertex(vertex(5, 2)),
    )));
    corruptions.push(("extra", extra));

    let mut reordered = records.clone();
    let first_change = match &records[position(1, 0)] {
        SnapshotRecord::Change(change) => change.clone(),
        _ => unreachable!(),
    };
    let second_change = match &records[position(1, 1)] {
        SnapshotRecord::Change(change) => change.clone(),
        _ => unreachable!(),
    };
    reordered[position(1, 0)] = SnapshotRecord::Change(dtg_storage::ChangeRecord::new(
        first_change.cursor(),
        second_change.mutation().clone(),
    ));
    reordered[position(1, 1)] = SnapshotRecord::Change(dtg_storage::ChangeRecord::new(
        second_change.cursor(),
        first_change.mutation().clone(),
    ));
    corruptions.push(("reordered", reordered));

    let mut cursor_swapped = records.clone();
    cursor_swapped[position(1, 0)] = SnapshotRecord::Change(dtg_storage::ChangeRecord::new(
        second_change.cursor(),
        first_change.mutation().clone(),
    ));
    cursor_swapped[position(1, 1)] = SnapshotRecord::Change(dtg_storage::ChangeRecord::new(
        first_change.cursor(),
        second_change.mutation().clone(),
    ));
    corruptions.push(("cursor-swapped", cursor_swapped));

    let mut altered = records.clone();
    let altered_change = match &records[position(2, 0)] {
        SnapshotRecord::Change(change) => change.clone(),
        _ => unreachable!(),
    };
    altered[position(2, 0)] = SnapshotRecord::Change(dtg_storage::ChangeRecord::new(
        altered_change.cursor(),
        LogicalMutation::PutVertex(vertex(6, 2)),
    ));
    corruptions.push(("altered", altered));

    let mut cross_index_reassociated = records;
    let first_index_change = match &cross_index_reassociated[position(1, 1)] {
        SnapshotRecord::Change(change) => change.clone(),
        _ => unreachable!(),
    };
    let second_index_change = match &cross_index_reassociated[position(2, 0)] {
        SnapshotRecord::Change(change) => change.clone(),
        _ => unreachable!(),
    };
    cross_index_reassociated[position(1, 1)] =
        SnapshotRecord::Change(dtg_storage::ChangeRecord::new(
            first_index_change.cursor(),
            second_index_change.mutation().clone(),
        ));
    cross_index_reassociated[position(2, 0)] =
        SnapshotRecord::Change(dtg_storage::ChangeRecord::new(
            second_index_change.cursor(),
            first_index_change.mutation().clone(),
        ));
    corruptions.push(("cross-index-reassociated", cross_index_reassociated));

    for (ordinal, (name, corrupted_records)) in corruptions.into_iter().enumerate() {
        let target_dir = tempfile::tempdir().unwrap();
        let target_binding = binding(&format!("digest-target-{name}"), 2);
        let target = FjallReplicaStore::open(target_dir.path(), target_binding.clone()).unwrap();
        let dirty = vertex(90 + ordinal as u128, 1);
        block_on(
            target.apply(
                CommittedShardBatch::new(
                    target_binding.clone(),
                    9,
                    1,
                    CommandId::new(900 + ordinal as u128).unwrap(),
                    vec![LogicalMutation::PutVertex(dirty.clone())],
                )
                .unwrap(),
            ),
        )
        .unwrap();
        let chunks = corrupted_records
            .chunks(3)
            .enumerate()
            .map(|(chunk_ordinal, records)| {
                SnapshotChunk::new(header.snapshot_id(), chunk_ordinal as u64, records.to_vec())
            })
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let manifest = SnapshotManifest::new(&header, &chunks).unwrap();
        let mut writer =
            block_on(target.begin_restore(target_binding.clone(), header.clone())).unwrap();
        for chunk in chunks {
            block_on(writer.write_chunk(chunk)).unwrap();
        }
        let error = block_on(writer.commit(manifest)).unwrap_err();
        assert_eq!(error.code(), "DTG-STORAGE-SNAPSHOT-CORRUPT", "{name}");
        assert_eq!(block_on(target.applied_index()).unwrap(), 1, "{name}");
        let view = block_on(target.begin_read_view(ReadFence::new(target_binding, 1))).unwrap();
        assert_eq!(
            block_on(view.get_vertex(VertexRead::new(
                dirty.id(),
                10,
                TransactionTime::new(10).unwrap(),
            )))
            .unwrap(),
            Some(dirty),
            "{name}"
        );
    }
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
