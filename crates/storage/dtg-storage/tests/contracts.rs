use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::future::Future;
use std::pin::pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use dtg_storage::{
    AdjacencyRead, ApplyReceipt, ArtifactStore, BackendClass, BindingRole, CapabilityManifest,
    ChangeCursor, ChangePage, ChangeRecord, ChangesRead, CommandId, CommittedShardBatch,
    ConsensusCommandEnvelope, ConsensusEntry, ConsensusStore, Digest32, DurabilityPolicy,
    EdgeHistoryRead, EdgeId, EdgeRead, EdgeScan, EdgeTombstone, EdgeVersion, LogicalMutation,
    LogicalSnapshotReader, LogicalSnapshotSink, LogicalSnapshotSource, LogicalSnapshotWriter,
    ProviderKind, PushdownExecutor, PushdownOperation, PushdownOutcome, PushdownRequest, ReadFence,
    ReplicaBinding, ReplicaMetadata, ReplicaStateStore, SUPPORTED_CONSENSUS_COMMAND_FORMAT_VERSION,
    SUPPORTED_CONSENSUS_WAL_FORMAT_VERSION, SUPPORTED_PUSHDOWN_CONTRACT_VERSION,
    SUPPORTED_SNAPSHOT_FORMAT_VERSION, ScanPage, SnapshotChunk, SnapshotHeader, SnapshotManifest,
    SnapshotRecord, SnapshotReplayRecord, SnapshotRequest, SnapshotRestoreReceipt, StorageError,
    StorageTckFactory, StorageTckStore, StoreFuture, TemporalReadView, TransactionId,
    TransactionRecord, TransactionState, TransactionTime, ValidInterval, Value, Version,
    VertexHistoryRead, VertexId, VertexRead, VertexScan, VertexTombstone, VertexVersion,
    run_storage_tck,
};

fn block_on<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    match future.as_mut().poll(&mut context) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("deterministic storage future unexpectedly yielded"),
    }
}

fn capabilities() -> CapabilityManifest {
    CapabilityManifest::from_names([
        "adjacency",
        "immutable-read-view",
        "logical-snapshot",
        "point",
    ])
    .unwrap()
}

fn binding(namespace: &str, generation: u64) -> ReplicaBinding {
    let manifest = capabilities();
    let class = BackendClass::with_durability(
        ProviderKind::Fjall,
        1,
        3,
        DurabilityPolicy::DurableCommit,
        manifest.names().map(str::to_owned),
    )
    .unwrap();
    ReplicaBinding::builder()
        .cluster_id(1)
        .graph_id(7)
        .shard_id(11)
        .placement_epoch(13)
        .replica_id(17)
        .backend_generation(generation)
        .backend_class_digest(class.digest())
        .provider_kind(ProviderKind::Fjall)
        .contract_version(1)
        .layout_version(3)
        .capability_digest(manifest.digest())
        .namespace_id(namespace)
        .endpoint_profile_ref("local-test-endpoint")
        .credential_ref("local-test-credential")
        .role(BindingRole::Active)
        .build()
        .unwrap()
}

#[test]
fn binding_rejects_zero_epoch_and_generation() {
    assert!(matches!(
        ReplicaBinding::builder().placement_epoch(0).build(),
        Err(StorageError::InvalidBinding(_))
    ));
    let invalid_generation = ReplicaBinding::builder()
        .cluster_id(1)
        .graph_id(1)
        .shard_id(1)
        .placement_epoch(1)
        .replica_id(1)
        .backend_generation(0)
        .backend_class_digest(Digest32::new([1; 32]))
        .provider_kind(ProviderKind::Fjall)
        .contract_version(1)
        .layout_version(1)
        .capability_digest(Digest32::new([2; 32]))
        .namespace_id("namespace")
        .endpoint_profile_ref("endpoint")
        .credential_ref("credential")
        .role(BindingRole::Candidate)
        .build();
    assert!(matches!(
        invalid_generation,
        Err(StorageError::InvalidBinding(_))
    ));
}

#[test]
fn capability_manifest_digest_is_order_independent() {
    let a = CapabilityManifest::from_names(["point", "adjacency"]).unwrap();
    let b = CapabilityManifest::from_names(["adjacency", "point"]).unwrap();
    assert_eq!(a.names().collect::<Vec<_>>(), vec!["adjacency", "point"]);
    assert_eq!(a.digest(), b.digest());
}

#[test]
fn capability_names_are_canonical_and_fail_closed() {
    assert!(CapabilityManifest::from_names(["Point"]).is_err());
    assert!(CapabilityManifest::from_names(["point read"]).is_err());
    assert!(CapabilityManifest::from_names([""]).is_err());
    assert_ne!(
        CapabilityManifest::from_names(["point"]).unwrap().digest(),
        CapabilityManifest::from_names(["point", "adjacency"])
            .unwrap()
            .digest()
    );
}

#[test]
fn backend_class_excludes_connection_material_but_includes_policy_and_layout() {
    let class = BackendClass::new(ProviderKind::Fjall, 1, 3, ["point"]).unwrap();
    assert_ne!(
        class.digest(),
        BackendClass::new(ProviderKind::PostgreSql, 1, 3, ["point"])
            .unwrap()
            .digest()
    );
    assert_ne!(
        class.digest(),
        BackendClass::new(ProviderKind::Fjall, 2, 3, ["point"])
            .unwrap()
            .digest()
    );
    assert_ne!(
        class.digest(),
        BackendClass::new(ProviderKind::Fjall, 1, 4, ["point"])
            .unwrap()
            .digest()
    );
    assert_ne!(
        class.digest(),
        BackendClass::with_durability(
            ProviderKind::Fjall,
            1,
            3,
            DurabilityPolicy::DurableCommitWithReplicaSync,
            ["point"],
        )
        .unwrap()
        .digest()
    );
    assert_ne!(
        class.digest(),
        BackendClass::new(ProviderKind::Fjall, 1, 3, ["point", "adjacency"])
            .unwrap()
            .digest()
    );

    let first = binding("class-test", 1);
    let second = first
        .to_builder()
        .endpoint_profile_ref("different-endpoint")
        .credential_ref("different-credential")
        .build()
        .unwrap();
    assert_eq!(first.backend_class_digest(), second.backend_class_digest());
    assert_ne!(first, second);
}

#[test]
fn snapshot_manifest_digest_includes_the_complete_source_binding() {
    let first_binding = binding("snapshot-source", 1);
    let second_binding = first_binding
        .to_builder()
        .endpoint_profile_ref("another-endpoint")
        .build()
        .unwrap();
    let snapshot_id = SnapshotRequest::new(44, 2).unwrap().snapshot_id();
    let first_header = SnapshotHeader::new(snapshot_id, first_binding, 8, 1).unwrap();
    let second_header = SnapshotHeader::new(snapshot_id, second_binding, 8, 1).unwrap();
    let chunks = vec![
        SnapshotChunk::new(
            snapshot_id,
            0,
            vec![SnapshotRecord::Vertex(sample_vertex(1, 1))],
        )
        .unwrap(),
    ];
    assert_ne!(
        SnapshotManifest::new(&first_header, &chunks)
            .unwrap()
            .content_digest(),
        SnapshotManifest::new(&second_header, &chunks)
            .unwrap()
            .content_digest()
    );
}

#[test]
fn capability_drift_rejects_read_views_and_snapshots() {
    let factory = TestFactory::new();
    let store_binding = binding("capability-drift", 1);
    let store = block_on(factory.open(store_binding.clone())).unwrap();
    let drifted = ReadFence::with_capability_digest(store_binding, 0, Digest32::new([9; 32]));
    assert!(matches!(
        block_on(store.begin_read_view(drifted.clone())),
        Err(StorageError::CapabilityDrift)
    ));
    assert!(matches!(
        block_on(store.begin_snapshot(drifted, SnapshotRequest::new(45, 2).unwrap())),
        Err(StorageError::CapabilityDrift)
    ));
}

#[test]
fn pushdown_contract_owns_binding_and_reports_partial_guarantees() {
    let factory = TestFactory::new();
    let store_binding = binding("pushdown-partial", 1);
    let store = block_on(factory.open(store_binding.clone())).unwrap();
    assert_eq!(PushdownExecutor::binding(&*store), &store_binding);
    let request = PushdownRequest::new(
        1,
        ReadFence::new(store_binding, 0),
        CapabilityManifest::from_names(["point", "typed-property-predicate"]).unwrap(),
        PushdownOperation::Vertex(VertexRead::new(
            VertexId::new(1).unwrap(),
            10,
            TransactionTime::new(10).unwrap(),
        )),
    )
    .unwrap();
    match block_on(store.execute_pushdown(request)).unwrap() {
        PushdownOutcome::ResidualRequired { guarantees, .. } => {
            assert!(guarantees.supports("point"));
            assert!(!guarantees.supports("typed-property-predicate"));
        }
        outcome => panic!("expected partial pushdown guarantees, got {outcome:?}"),
    }
}

#[test]
fn unknown_snapshot_and_pushdown_versions_fail_closed() {
    let store_binding = binding("unknown-formats", 1);
    let snapshot_id = SnapshotRequest::new(46, 2).unwrap().snapshot_id();
    assert!(matches!(
        SnapshotHeader::new(snapshot_id, store_binding.clone(), 0, 2),
        Err(StorageError::CorruptSnapshot(_))
    ));
    assert!(matches!(
        PushdownRequest::new(
            2,
            ReadFence::new(store_binding, 0),
            CapabilityManifest::from_names(["point"]).unwrap(),
            PushdownOperation::Vertex(VertexRead::new(
                VertexId::new(1).unwrap(),
                10,
                TransactionTime::new(10).unwrap(),
            )),
        ),
        Err(StorageError::InvalidCapability(_))
    ));
}

#[test]
fn supported_wire_versions_are_explicit_and_consensus_is_enveloped() {
    assert_eq!(SUPPORTED_SNAPSHOT_FORMAT_VERSION, 1);
    assert_eq!(SUPPORTED_PUSHDOWN_CONTRACT_VERSION, 1);
    assert_eq!(SUPPORTED_CONSENSUS_COMMAND_FORMAT_VERSION, 1);
    assert_eq!(SUPPORTED_CONSENSUS_WAL_FORMAT_VERSION, 1);

    assert!(matches!(
        ConsensusCommandEnvelope::new(2, vec![1, 2, 3]),
        Err(StorageError::InvalidConsensus(_))
    ));
    let command =
        ConsensusCommandEnvelope::new(SUPPORTED_CONSENSUS_COMMAND_FORMAT_VERSION, vec![1, 2, 3])
            .unwrap();
    assert_eq!(
        command.format_version(),
        SUPPORTED_CONSENSUS_COMMAND_FORMAT_VERSION
    );
    assert_eq!(command.payload(), &[1, 2, 3]);
    assert!(matches!(
        ConsensusEntry::new(2, 3, 4, CommandId::new(5).unwrap(), command.clone(),),
        Err(StorageError::InvalidConsensus(_))
    ));
    let entry = ConsensusEntry::new(
        SUPPORTED_CONSENSUS_WAL_FORMAT_VERSION,
        3,
        4,
        CommandId::new(5).unwrap(),
        command.clone(),
    )
    .unwrap();
    assert_eq!(
        entry.wal_format_version(),
        SUPPORTED_CONSENSUS_WAL_FORMAT_VERSION
    );
    assert_eq!(entry.command(), &command);
    assert_eq!(entry.command_digest(), command.digest());
}

#[test]
fn tombstones_expose_complete_temporal_identity() {
    let version = Version::new(7);
    let transaction_time = TransactionTime::new(9).unwrap();
    let vertex = VertexTombstone::new(VertexId::new(10).unwrap(), version, transaction_time);
    assert_eq!(vertex.id(), VertexId::new(10).unwrap());
    assert_eq!(vertex.version(), version);
    assert_eq!(vertex.transaction_time(), transaction_time);

    let edge = EdgeTombstone::new(EdgeId::new(11).unwrap(), version, transaction_time);
    assert_eq!(edge.id(), EdgeId::new(11).unwrap());
    assert_eq!(edge.version(), version);
    assert_eq!(edge.transaction_time(), transaction_time);
}

#[test]
fn change_pages_resume_after_the_complete_ordering_key() {
    let cursor = ChangeCursor::new(7, 3);
    let mutation = LogicalMutation::PutVertex(sample_vertex(10, 1));
    let record = ChangeRecord::new(cursor, mutation);
    let page = ChangePage::new(vec![record.clone()], Some(cursor));

    assert_eq!(record.cursor(), cursor);
    assert_eq!(record.raft_index(), 7);
    assert_eq!(record.mutation_ordinal(), 3);
    assert_eq!(page.next_after(), Some(cursor));

    let next = ChangesRead::new(page.next_after(), 7, 1).unwrap();
    assert!(!next.includes(cursor));
    assert!(next.includes(ChangeCursor::new(7, 4)));
}

#[test]
fn snapshot_records_cover_tombstones_replay_and_ordered_changes() {
    let vertex_tombstone = VertexTombstone::new(
        VertexId::new(10).unwrap(),
        Version::new(2),
        TransactionTime::new(2).unwrap(),
    );
    let edge_tombstone = EdgeTombstone::new(
        EdgeId::new(11).unwrap(),
        Version::new(3),
        TransactionTime::new(3).unwrap(),
    );
    let replay =
        SnapshotReplayRecord::new(7, 4, CommandId::new(12).unwrap(), Digest32::new([5; 32]))
            .unwrap();
    let change = ChangeRecord::new(
        ChangeCursor::new(7, 0),
        LogicalMutation::DeleteVertex(vertex_tombstone.clone()),
    );
    let snapshot_id = SnapshotRequest::new(47, 8).unwrap().snapshot_id();
    let records = vec![
        SnapshotRecord::VertexTombstone(vertex_tombstone),
        SnapshotRecord::EdgeTombstone(edge_tombstone),
        SnapshotRecord::Replay(replay),
        SnapshotRecord::Change(change),
    ];
    let chunk = SnapshotChunk::new(snapshot_id, 0, records.clone()).unwrap();

    assert_eq!(chunk.records(), records);
    chunk.validate().unwrap();
}

#[test]
fn scan_pages_preserve_typed_128_bit_cursors_for_next_requests() {
    let vertex_cursor = VertexId::new(u128::from(u64::MAX) + 101).unwrap();
    let vertex_page = ScanPage::<VertexVersion, VertexId>::new(Vec::new(), Some(vertex_cursor));
    let next_vertex_request = VertexScan::new(
        10,
        TransactionTime::new(10).unwrap(),
        vertex_page.next_after(),
        1,
    )
    .unwrap();
    assert_eq!(next_vertex_request.after(), Some(vertex_cursor));

    let edge_cursor = EdgeId::new(u128::from(u64::MAX) + 202).unwrap();
    let edge_page = ScanPage::<EdgeVersion, EdgeId>::new(Vec::new(), Some(edge_cursor));
    let next_edge_request = EdgeScan::new(
        10,
        TransactionTime::new(10).unwrap(),
        edge_page.next_after(),
        1,
    )
    .unwrap();
    assert_eq!(next_edge_request.after(), Some(edge_cursor));
}

#[test]
fn committed_edges_do_not_require_local_endpoint_records() {
    let factory = TestFactory::new();
    let store_binding = binding("cross-shard-edge", 1);
    let store = block_on(factory.open(store_binding.clone())).unwrap();
    let edge = sample_edge(9, VertexId::new(101).unwrap(), VertexId::new(202).unwrap());

    block_on(
        store.apply(
            CommittedShardBatch::new(
                store_binding.clone(),
                1,
                1,
                CommandId::new(1).unwrap(),
                vec![LogicalMutation::PutEdge(edge.clone())],
            )
            .unwrap(),
        ),
    )
    .unwrap();

    let view = block_on(store.begin_read_view(ReadFence::new(store_binding, 1))).unwrap();
    assert_eq!(
        block_on(view.get_edge(EdgeRead::new(
            edge.id(),
            10,
            TransactionTime::new(10).unwrap(),
        )))
        .unwrap(),
        Some(edge)
    );
}

#[test]
fn execution_stage_failure_is_atomic() {
    let factory = TestFactory::new();
    let store_binding = binding("execution-failure", 1);
    let store = block_on(factory.open(store_binding.clone())).unwrap();
    let first = sample_vertex(1, 1);
    let second = sample_vertex(2, 1);
    block_on(
        store.apply(
            CommittedShardBatch::new(
                store_binding.clone(),
                1,
                1,
                CommandId::new(1).unwrap(),
                vec![
                    LogicalMutation::PutVertex(first.clone()),
                    LogicalMutation::PutVertex(second.clone()),
                ],
            )
            .unwrap(),
        ),
    )
    .unwrap();

    let first_staged = sample_vertex(3, 1);
    let second_staged = sample_vertex(4, 1);
    store.arm_apply_failure_after(1).unwrap();
    let failing_batch = CommittedShardBatch::new(
        store_binding.clone(),
        1,
        2,
        CommandId::new(2).unwrap(),
        vec![
            LogicalMutation::PutVertex(first_staged.clone()),
            LogicalMutation::PutVertex(second_staged.clone()),
        ],
    )
    .unwrap();
    assert!(matches!(
        block_on(store.apply(failing_batch.clone())),
        Err(StorageError::InjectedApplyFailure {
            staged_mutations: 1
        })
    ));
    assert_eq!(block_on(store.applied_index()).unwrap(), 1);

    let view = block_on(store.begin_read_view(ReadFence::new(store_binding.clone(), 1))).unwrap();
    assert_eq!(
        block_on(view.get_vertex(VertexRead::new(
            first.id(),
            10,
            TransactionTime::new(10).unwrap(),
        )))
        .unwrap(),
        Some(first.clone())
    );
    assert!(
        block_on(view.get_vertex(VertexRead::new(
            first_staged.id(),
            10,
            TransactionTime::new(10).unwrap(),
        )))
        .unwrap()
        .is_none()
    );
    assert!(
        block_on(view.get_vertex(VertexRead::new(
            second_staged.id(),
            10,
            TransactionTime::new(10).unwrap(),
        )))
        .unwrap()
        .is_none()
    );
    let scan =
        block_on(view.scan_vertices(
            VertexScan::new(10, TransactionTime::new(10).unwrap(), None, 16).unwrap(),
        ))
        .unwrap();
    assert_eq!(scan.rows(), &[first, second]);
    let changes = block_on(view.changes(ChangesRead::new(None, 2, 16).unwrap())).unwrap();
    assert_eq!(changes.rows().len(), 2);
    assert!(changes.rows().iter().all(|change| change.raft_index() == 1));

    let retry = block_on(store.apply(failing_batch.clone())).unwrap();
    assert!(!retry.replayed());
    assert_eq!(block_on(store.applied_index()).unwrap(), 2);
    let committed = block_on(store.begin_read_view(ReadFence::new(store_binding, 2))).unwrap();
    for vertex in [&first_staged, &second_staged] {
        assert_eq!(
            block_on(committed.get_vertex(VertexRead::new(
                vertex.id(),
                10,
                TransactionTime::new(10).unwrap(),
            )))
            .unwrap(),
            Some(vertex.clone())
        );
    }
    let committed_changes = block_on(
        committed.changes(ChangesRead::new(Some(ChangeCursor::new(1, u64::MAX)), 2, 16).unwrap()),
    )
    .unwrap();
    assert_eq!(committed_changes.rows().len(), 2);
    assert!(
        committed_changes
            .rows()
            .iter()
            .all(|change| change.raft_index() == 2)
    );

    let replay = block_on(store.apply(failing_batch)).unwrap();
    assert!(replay.replayed());
    assert_eq!(block_on(store.applied_index()).unwrap(), 2);
    let after_replay = block_on(store.begin_read_view(ReadFence::new(
        ReplicaStateStore::binding(&*store).clone(),
        2,
    )))
    .unwrap();
    let after_replay_changes = block_on(
        after_replay
            .changes(ChangesRead::new(Some(ChangeCursor::new(1, u64::MAX)), 2, 16).unwrap()),
    )
    .unwrap();
    assert_eq!(after_replay_changes, committed_changes);
}

#[test]
fn edge_scan_pages_return_one_latest_visible_version_per_edge_id() {
    let factory = TestFactory::new();
    let store_binding = binding("edge-scan-versions", 1);
    let store = block_on(factory.open(store_binding.clone())).unwrap();
    let source = VertexId::new(101).unwrap();
    let target = VertexId::new(202).unwrap();
    let old = sample_edge_version(11, source, target, 1);
    let latest = sample_edge_version(11, source, target, 2);
    let invisible_newest = sample_edge_temporal_version(11, source, target, 3, 20);
    let other = sample_edge_version(12, source, target, 1);
    block_on(
        store.apply(
            CommittedShardBatch::new(
                store_binding.clone(),
                1,
                1,
                CommandId::new(1).unwrap(),
                vec![
                    LogicalMutation::PutEdge(old),
                    LogicalMutation::PutEdge(latest.clone()),
                    LogicalMutation::PutEdge(invisible_newest),
                    LogicalMutation::PutEdge(other.clone()),
                ],
            )
            .unwrap(),
        ),
    )
    .unwrap();
    let view = block_on(store.begin_read_view(ReadFence::new(store_binding, 1))).unwrap();
    let large = block_on(
        view.scan_edges(EdgeScan::new(10, TransactionTime::new(10).unwrap(), None, 10).unwrap()),
    )
    .unwrap();

    let mut paged = Vec::new();
    let mut after = None;
    loop {
        let page =
            block_on(view.scan_edges(
                EdgeScan::new(10, TransactionTime::new(10).unwrap(), after, 1).unwrap(),
            ))
            .unwrap();
        paged.extend_from_slice(page.rows());
        match page.next_after() {
            Some(cursor) => after = Some(cursor),
            None => break,
        }
    }

    assert_eq!(large.rows(), &[latest, other]);
    assert_eq!(paged, large.rows());
}

#[test]
fn snapshot_round_trip_preserves_all_typed_records() {
    let factory = TestFactory::new();
    let source_binding = binding("snapshot-all-types", 1);
    let source = block_on(factory.open(source_binding.clone())).unwrap();
    let first = sample_vertex(11, 1);
    let second = sample_vertex(12, 1);
    let edge = sample_edge(13, first.id(), second.id());
    let transaction = sample_transaction(14);
    let metadata = ReplicaMetadata::new("lease-owner", Value::String("replica-17".into())).unwrap();
    let mutations = vec![
        LogicalMutation::PutVertex(first.clone()),
        LogicalMutation::PutVertex(second.clone()),
        LogicalMutation::PutEdge(edge.clone()),
        LogicalMutation::PutTransaction(transaction.clone()),
        LogicalMutation::PutReplicaMetadata(metadata.clone()),
    ];
    let batch = CommittedShardBatch::new(
        source_binding.clone(),
        1,
        1,
        CommandId::new(14).unwrap(),
        mutations.clone(),
    )
    .unwrap();
    let mut expected = vec![
        SnapshotRecord::Vertex(first.clone()),
        SnapshotRecord::Vertex(second.clone()),
        SnapshotRecord::Edge(edge.clone()),
        SnapshotRecord::Transaction(transaction.clone()),
        SnapshotRecord::ReplicaMetadata(metadata.clone()),
    ];
    expected.push(SnapshotRecord::Replay(
        SnapshotReplayRecord::new(
            batch.raft_index(),
            batch.raft_term(),
            batch.command_id(),
            batch.mutation_digest(),
        )
        .unwrap(),
    ));
    expected.extend(
        mutations
            .into_iter()
            .enumerate()
            .map(|(ordinal, mutation)| {
                SnapshotRecord::Change(ChangeRecord::new(
                    ChangeCursor::new(1, ordinal as u64),
                    mutation,
                ))
            }),
    );
    block_on(source.apply(batch)).unwrap();

    let (header, chunks, manifest, records) = export_snapshot(&*source, &source_binding, 1, 47, 2);
    assert_eq!(records, expected);

    let target_binding = binding("snapshot-all-types-restored", 2);
    let target = block_on(factory.open(target_binding.clone())).unwrap();
    let mut writer = block_on(target.begin_restore(target_binding.clone(), header)).unwrap();
    for chunk in chunks {
        block_on(writer.write_chunk(chunk)).unwrap();
    }
    block_on(writer.commit(manifest)).unwrap();
    let (_, _, _, restored_records) = export_snapshot(&*target, &target_binding, 1, 48, 3);
    assert_eq!(restored_records, expected);
}

#[test]
fn all_async_contracts_are_object_safe() {
    fn state_store(_: &dyn ReplicaStateStore) {}
    fn read_view(_: &dyn TemporalReadView) {}
    fn snapshot_source(_: &dyn LogicalSnapshotSource) {}
    fn snapshot_sink(_: &dyn LogicalSnapshotSink) {}
    fn snapshot_reader(_: &dyn LogicalSnapshotReader) {}
    fn snapshot_writer(_: &dyn LogicalSnapshotWriter) {}
    fn pushdown(_: &dyn PushdownExecutor) {}
    fn consensus(_: &dyn ConsensusStore) {}
    fn artifacts(_: &dyn ArtifactStore) {}

    let _ = state_store;
    let _ = read_view;
    let _ = snapshot_source;
    let _ = snapshot_sink;
    let _ = snapshot_reader;
    let _ = snapshot_writer;
    let _ = pushdown;
    let _ = consensus;
    let _ = artifacts;
}

#[test]
fn deterministic_store_passes_the_public_tck() {
    block_on(run_storage_tck(&TestFactory::new())).unwrap();
}

#[test]
fn public_tck_rejects_an_injected_failure_that_is_not_one_shot() {
    let factory = TestFactory::with_persistent_apply_failure();
    assert!(matches!(
        block_on(run_storage_tck(&factory)),
        Err(StorageError::InjectedApplyFailure {
            staged_mutations: 1
        })
    ));
}

#[test]
fn public_tck_rejects_replay_that_duplicates_change_records() {
    let factory = TestFactory::with_duplicate_changes_on_replay();
    assert!(matches!(
        block_on(run_storage_tck(&factory)),
        Err(StorageError::TckViolation(_))
    ));
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ReplayRecord {
    term: u64,
    command_id: CommandId,
    digest: Digest32,
}

#[derive(Clone, Default)]
struct TestState {
    applied_index: u64,
    vertices: BTreeMap<VertexId, Vec<VertexVersion>>,
    edges: Vec<EdgeVersion>,
    transactions: Vec<TransactionRecord>,
    metadata: Vec<ReplicaMetadata>,
    history: Vec<LogicalMutation>,
    replay: BTreeMap<u64, ReplayRecord>,
    changes: Vec<(ChangeCursor, LogicalMutation)>,
}

#[derive(Clone)]
struct TestStore {
    binding: ReplicaBinding,
    capabilities: CapabilityManifest,
    state: Arc<Mutex<TestState>>,
    apply_failure_after: Arc<Mutex<Option<usize>>>,
    persistent_apply_failure: bool,
    duplicate_changes_on_replay: bool,
}

#[derive(Default)]
struct FactoryState {
    owners: BTreeMap<String, ReplicaBinding>,
    stores: BTreeMap<String, Arc<Mutex<TestState>>>,
}

struct TestFactory {
    state: Mutex<FactoryState>,
    capabilities: CapabilityManifest,
    persistent_apply_failure: bool,
    duplicate_changes_on_replay: bool,
}

impl TestFactory {
    fn new() -> Self {
        Self {
            state: Mutex::new(FactoryState::default()),
            capabilities: capabilities(),
            persistent_apply_failure: false,
            duplicate_changes_on_replay: false,
        }
    }

    fn with_persistent_apply_failure() -> Self {
        Self {
            persistent_apply_failure: true,
            ..Self::new()
        }
    }

    fn with_duplicate_changes_on_replay() -> Self {
        Self {
            duplicate_changes_on_replay: true,
            ..Self::new()
        }
    }
}

impl StorageTckFactory for TestFactory {
    fn capabilities(&self) -> CapabilityManifest {
        self.capabilities.clone()
    }

    fn binding(
        &self,
        namespace: &str,
        backend_generation: u64,
    ) -> Result<ReplicaBinding, StorageError> {
        Ok(binding(namespace, backend_generation))
    }

    fn open(&self, requested: ReplicaBinding) -> StoreFuture<'_, Box<dyn StorageTckStore>> {
        Box::pin(async move {
            let namespace = requested.namespace_id().as_str().to_owned();
            let mut factory = self.state.lock().unwrap();
            if let Some(owner) = factory.owners.get(&namespace) {
                if owner != &requested {
                    return Err(StorageError::NamespaceOwnerMismatch {
                        expected: Box::new(owner.clone()),
                        actual: Box::new(requested),
                    });
                }
            } else {
                factory.owners.insert(namespace.clone(), requested.clone());
            }
            let state = factory
                .stores
                .entry(namespace)
                .or_insert_with(|| Arc::new(Mutex::new(TestState::default())))
                .clone();
            Ok(Box::new(TestStore {
                binding: requested,
                capabilities: self.capabilities.clone(),
                state,
                apply_failure_after: Arc::new(Mutex::new(None)),
                persistent_apply_failure: self.persistent_apply_failure,
                duplicate_changes_on_replay: self.duplicate_changes_on_replay,
            }) as Box<dyn StorageTckStore>)
        })
    }
}

impl StorageTckStore for TestStore {
    fn arm_apply_failure_after(&self, staged_mutations: usize) -> Result<(), StorageError> {
        if staged_mutations == 0 {
            return Err(StorageError::TckViolation(
                "apply failure must occur after at least one staged mutation".into(),
            ));
        }
        *self.apply_failure_after.lock().unwrap() = Some(staged_mutations);
        Ok(())
    }
}

impl ReplicaStateStore for TestStore {
    fn binding(&self) -> &ReplicaBinding {
        &self.binding
    }

    fn applied_index(&self) -> StoreFuture<'_, u64> {
        Box::pin(async move { Ok(self.state.lock().unwrap().applied_index) })
    }

    fn replica_metadata<'a>(&'a self, name: &'a str) -> StoreFuture<'a, Option<ReplicaMetadata>> {
        Box::pin(async move {
            Ok(self
                .state
                .lock()
                .unwrap()
                .metadata
                .iter()
                .find(|metadata| metadata.name() == name)
                .cloned())
        })
    }

    fn apply(&self, batch: CommittedShardBatch) -> StoreFuture<'_, ApplyReceipt> {
        Box::pin(async move {
            batch.validate()?;
            if batch.binding() != &self.binding {
                return Err(StorageError::StaleBinding {
                    expected: Box::new(self.binding.clone()),
                    actual: Box::new(batch.binding().clone()),
                });
            }

            let mut state = self.state.lock().unwrap();
            if batch.raft_index() <= state.applied_index {
                let replay =
                    state
                        .replay
                        .get(&batch.raft_index())
                        .ok_or(StorageError::ReplayMismatch {
                            raft_index: batch.raft_index(),
                        })?;
                if replay.term != batch.raft_term()
                    || replay.command_id != batch.command_id()
                    || replay.digest != batch.mutation_digest()
                {
                    return Err(StorageError::ReplayMismatch {
                        raft_index: batch.raft_index(),
                    });
                }
                if self.duplicate_changes_on_replay {
                    state
                        .changes
                        .extend(batch.mutations().iter().cloned().enumerate().map(
                            |(ordinal, mutation)| {
                                (
                                    ChangeCursor::new(batch.raft_index(), ordinal as u64),
                                    mutation,
                                )
                            },
                        ));
                }
                return Ok(ApplyReceipt::new(&batch, true));
            }
            if batch.raft_index() != state.applied_index + 1 {
                return Err(StorageError::NonMonotonicIndex {
                    applied: state.applied_index,
                    proposed: batch.raft_index(),
                });
            }

            let mut next = state.clone();
            for (position, mutation) in batch.mutations().iter().enumerate() {
                match mutation {
                    LogicalMutation::PutVertex(vertex) => {
                        next.vertices
                            .entry(vertex.id())
                            .or_default()
                            .push(vertex.clone());
                    }
                    LogicalMutation::DeleteVertex(_) => {}
                    LogicalMutation::PutEdge(edge) => {
                        next.edges.push(edge.clone());
                    }
                    LogicalMutation::DeleteEdge(_) => {}
                    LogicalMutation::PutTransaction(transaction) => {
                        next.transactions.push(transaction.clone());
                    }
                    LogicalMutation::PutReplicaMetadata(metadata) => {
                        next.metadata
                            .retain(|existing| existing.name() != metadata.name());
                        next.metadata.push(metadata.clone());
                    }
                }
                next.history.push(mutation.clone());
                next.changes.push((
                    ChangeCursor::new(batch.raft_index(), position as u64),
                    mutation.clone(),
                ));
                let mut armed_failure = self.apply_failure_after.lock().unwrap();
                if *armed_failure == Some(position + 1) {
                    if !self.persistent_apply_failure {
                        *armed_failure = None;
                    }
                    return Err(StorageError::InjectedApplyFailure {
                        staged_mutations: position + 1,
                    });
                }
            }
            next.applied_index = batch.raft_index();
            next.replay.insert(
                batch.raft_index(),
                ReplayRecord {
                    term: batch.raft_term(),
                    command_id: batch.command_id(),
                    digest: batch.mutation_digest(),
                },
            );
            *state = next;
            Ok(ApplyReceipt::new(&batch, false))
        })
    }

    fn begin_read_view(&self, fence: ReadFence) -> StoreFuture<'_, Box<dyn TemporalReadView>> {
        Box::pin(async move {
            if fence.binding() != &self.binding {
                return Err(StorageError::StaleBinding {
                    expected: Box::new(self.binding.clone()),
                    actual: Box::new(fence.binding().clone()),
                });
            }
            if fence.capability_digest() != self.capabilities.digest() {
                return Err(StorageError::CapabilityDrift);
            }
            let state = self.state.lock().unwrap().clone();
            if fence.applied_index() != state.applied_index {
                return Err(StorageError::ReadFenceUnavailable {
                    requested: fence.applied_index(),
                    applied: state.applied_index,
                });
            }
            Ok(Box::new(TestReadView { fence, state }) as Box<dyn TemporalReadView>)
        })
    }
}

struct TestReadView {
    fence: ReadFence,
    state: TestState,
}

fn visible_vertex(history: &[LogicalMutation], request: &VertexRead) -> Option<VertexVersion> {
    let mut candidate: Option<(TransactionTime, Version, Option<VertexVersion>)> = None;
    for mutation in history {
        let event = match mutation {
            LogicalMutation::PutVertex(vertex)
                if vertex.id() == request.id()
                    && vertex.transaction_time() <= request.transaction_at()
                    && vertex.valid_time().start() <= request.valid_at()
                    && request.valid_at() < vertex.valid_time().end() =>
            {
                Some((
                    vertex.transaction_time(),
                    vertex.version(),
                    Some(vertex.clone()),
                ))
            }
            LogicalMutation::DeleteVertex(tombstone)
                if tombstone.id() == request.id()
                    && tombstone.transaction_time() <= request.transaction_at() =>
            {
                Some((tombstone.transaction_time(), tombstone.version(), None))
            }
            _ => None,
        };
        if let Some(event) = event
            && candidate
                .as_ref()
                .is_none_or(|current| (event.0, event.1) > (current.0, current.1))
        {
            candidate = Some(event);
        }
    }
    candidate.and_then(|(_, _, vertex)| vertex)
}

fn visible_edge(history: &[LogicalMutation], request: &EdgeRead) -> Option<EdgeVersion> {
    let mut candidate: Option<(TransactionTime, Version, Option<EdgeVersion>)> = None;
    for mutation in history {
        let event = match mutation {
            LogicalMutation::PutEdge(edge)
                if edge.id() == request.id()
                    && edge.transaction_time() <= request.transaction_at()
                    && edge.valid_time().start() <= request.valid_at()
                    && request.valid_at() < edge.valid_time().end() =>
            {
                Some((edge.transaction_time(), edge.version(), Some(edge.clone())))
            }
            LogicalMutation::DeleteEdge(tombstone)
                if tombstone.id() == request.id()
                    && tombstone.transaction_time() <= request.transaction_at() =>
            {
                Some((tombstone.transaction_time(), tombstone.version(), None))
            }
            _ => None,
        };
        if let Some(event) = event
            && candidate
                .as_ref()
                .is_none_or(|current| (event.0, event.1) > (current.0, current.1))
        {
            candidate = Some(event);
        }
    }
    candidate.and_then(|(_, _, edge)| edge)
}

impl TemporalReadView for TestReadView {
    fn fence(&self) -> &ReadFence {
        &self.fence
    }

    fn get_vertex(&self, request: VertexRead) -> StoreFuture<'_, Option<VertexVersion>> {
        Box::pin(async move { Ok(visible_vertex(&self.state.history, &request)) })
    }

    fn get_edge(&self, request: EdgeRead) -> StoreFuture<'_, Option<EdgeVersion>> {
        Box::pin(async move { Ok(visible_edge(&self.state.history, &request)) })
    }

    fn vertex_history(&self, request: VertexHistoryRead) -> StoreFuture<'_, Vec<VertexVersion>> {
        Box::pin(async move {
            let mut versions = self
                .state
                .history
                .iter()
                .filter_map(|mutation| match mutation {
                    LogicalMutation::PutVertex(vertex)
                        if vertex.id() == request.id() && request.includes(vertex) =>
                    {
                        Some(vertex.clone())
                    }
                    _ => None,
                })
                .collect::<Vec<_>>();
            versions.sort_by_key(|version| (version.transaction_time(), version.version()));
            versions.truncate(request.limit() as usize);
            Ok(versions)
        })
    }

    fn edge_history(&self, request: EdgeHistoryRead) -> StoreFuture<'_, Vec<EdgeVersion>> {
        Box::pin(async move {
            let mut versions = self
                .state
                .history
                .iter()
                .filter_map(|mutation| match mutation {
                    LogicalMutation::PutEdge(edge)
                        if edge.id() == request.id() && request.includes(edge) =>
                    {
                        Some(edge.clone())
                    }
                    _ => None,
                })
                .collect::<Vec<_>>();
            versions.sort_by_key(|edge| (edge.transaction_time(), edge.version()));
            versions.truncate(request.limit() as usize);
            Ok(versions)
        })
    }

    fn expand(&self, request: AdjacencyRead) -> StoreFuture<'_, Vec<EdgeVersion>> {
        Box::pin(async move {
            let ids = self
                .state
                .history
                .iter()
                .filter_map(|mutation| match mutation {
                    LogicalMutation::PutEdge(edge) => Some(edge.id()),
                    LogicalMutation::DeleteEdge(tombstone) => Some(tombstone.id()),
                    _ => None,
                })
                .collect::<BTreeSet<_>>();
            let mut rows = Vec::new();
            for id in ids {
                if let Some(edge) = visible_edge(
                    &self.state.history,
                    &EdgeRead::new(id, request.valid_at(), request.transaction_at()),
                ) && request.matches(&edge)
                {
                    rows.push(edge);
                }
            }
            rows.truncate(request.limit() as usize);
            Ok(rows)
        })
    }

    fn changes(&self, request: ChangesRead) -> StoreFuture<'_, ChangePage> {
        Box::pin(async move {
            let changes = self
                .state
                .changes
                .iter()
                .filter(|(cursor, _)| request.includes(*cursor))
                .take(request.limit() as usize)
                .map(|(cursor, mutation)| ChangeRecord::new(*cursor, mutation.clone()))
                .collect::<Vec<_>>();
            let has_more = self
                .state
                .changes
                .iter()
                .filter(|(cursor, _)| request.includes(*cursor))
                .count()
                > changes.len();
            let next_after = has_more.then(|| changes.last().unwrap().cursor());
            Ok(ChangePage::new(changes, next_after))
        })
    }

    fn scan_vertices(
        &self,
        request: VertexScan,
    ) -> StoreFuture<'_, ScanPage<VertexVersion, VertexId>> {
        Box::pin(async move {
            let ids = self
                .state
                .history
                .iter()
                .filter_map(|mutation| match mutation {
                    LogicalMutation::PutVertex(vertex) => Some(vertex.id()),
                    LogicalMutation::DeleteVertex(tombstone) => Some(tombstone.id()),
                    _ => None,
                })
                .filter(|id| request.after().is_none_or(|after| *id > after))
                .collect::<BTreeSet<_>>();
            let mut rows = ids
                .into_iter()
                .filter_map(|id| {
                    visible_vertex(
                        &self.state.history,
                        &VertexRead::new(id, request.valid_at(), request.transaction_at()),
                    )
                })
                .take(request.limit() as usize + 1)
                .collect::<Vec<_>>();
            let next_after = (rows.len() > request.limit() as usize)
                .then(|| rows[request.limit() as usize - 1].id());
            rows.truncate(request.limit() as usize);
            Ok(ScanPage::new(rows, next_after))
        })
    }

    fn scan_edges(&self, request: EdgeScan) -> StoreFuture<'_, ScanPage<EdgeVersion, EdgeId>> {
        Box::pin(async move {
            let ids = self
                .state
                .history
                .iter()
                .filter_map(|mutation| match mutation {
                    LogicalMutation::PutEdge(edge) => Some(edge.id()),
                    LogicalMutation::DeleteEdge(tombstone) => Some(tombstone.id()),
                    _ => None,
                })
                .filter(|id| request.after().is_none_or(|after| *id > after))
                .collect::<BTreeSet<_>>();
            let mut rows = ids
                .into_iter()
                .filter_map(|id| {
                    visible_edge(
                        &self.state.history,
                        &EdgeRead::new(id, request.valid_at(), request.transaction_at()),
                    )
                })
                .take(request.limit() as usize + 1)
                .collect::<Vec<_>>();
            let next_after = (rows.len() > request.limit() as usize)
                .then(|| rows[request.limit() as usize - 1].id());
            rows.truncate(request.limit() as usize);
            Ok(ScanPage::new(rows, next_after))
        })
    }
}

impl PushdownExecutor for TestStore {
    fn binding(&self) -> &ReplicaBinding {
        &self.binding
    }

    fn capabilities(&self) -> &CapabilityManifest {
        &self.capabilities
    }

    fn execute_pushdown(&self, request: PushdownRequest) -> StoreFuture<'_, PushdownOutcome> {
        Box::pin(async move {
            request.validate()?;
            if request.fence().binding() != &self.binding {
                return Err(StorageError::StaleBinding {
                    expected: Box::new(self.binding.clone()),
                    actual: Box::new(request.fence().binding().clone()),
                });
            }
            if request.fence().capability_digest() != self.capabilities.digest() {
                return Err(StorageError::CapabilityDrift);
            }
            let guarantees = self
                .capabilities
                .intersection(request.required_capabilities());
            let state = self.state.lock().unwrap().clone();
            let rows = match request.operation() {
                PushdownOperation::Vertex(read) => visible_vertex(&state.history, read)
                    .map(SnapshotRecord::Vertex)
                    .into_iter()
                    .collect(),
                PushdownOperation::VertexScan(scan) => state
                    .history
                    .iter()
                    .filter_map(|mutation| match mutation {
                        LogicalMutation::PutVertex(vertex) => Some(vertex.id()),
                        LogicalMutation::DeleteVertex(tombstone) => Some(tombstone.id()),
                        _ => None,
                    })
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .filter_map(|id| {
                        visible_vertex(
                            &state.history,
                            &VertexRead::new(id, scan.valid_at(), scan.transaction_at()),
                        )
                    })
                    .take(scan.limit() as usize)
                    .map(SnapshotRecord::Vertex)
                    .collect(),
            };
            if self
                .capabilities
                .contains_all(request.required_capabilities())
            {
                Ok(PushdownOutcome::Exact(rows))
            } else if guarantees.is_empty() {
                Ok(PushdownOutcome::Unsupported)
            } else {
                Ok(PushdownOutcome::ResidualRequired { rows, guarantees })
            }
        })
    }
}

impl LogicalSnapshotSource for TestStore {
    fn begin_snapshot(
        &self,
        fence: ReadFence,
        request: SnapshotRequest,
    ) -> StoreFuture<'_, Box<dyn LogicalSnapshotReader>> {
        Box::pin(async move {
            request.validate()?;
            if fence.binding() != &self.binding {
                return Err(StorageError::StaleBinding {
                    expected: Box::new(self.binding.clone()),
                    actual: Box::new(fence.binding().clone()),
                });
            }
            if fence.capability_digest() != self.capabilities.digest() {
                return Err(StorageError::CapabilityDrift);
            }
            let state = self.state.lock().unwrap().clone();
            if fence.applied_index() != state.applied_index {
                return Err(StorageError::ReadFenceUnavailable {
                    requested: fence.applied_index(),
                    applied: state.applied_index,
                });
            }
            let header = SnapshotHeader::new(
                request.snapshot_id(),
                self.binding.clone(),
                state.applied_index,
                1,
            )?;
            let mut records = state
                .history
                .iter()
                .filter_map(|mutation| match mutation {
                    LogicalMutation::PutVertex(vertex) => {
                        Some(SnapshotRecord::Vertex(vertex.clone()))
                    }
                    LogicalMutation::DeleteVertex(tombstone) => {
                        Some(SnapshotRecord::VertexTombstone(tombstone.clone()))
                    }
                    LogicalMutation::PutEdge(edge) => Some(SnapshotRecord::Edge(edge.clone())),
                    LogicalMutation::DeleteEdge(tombstone) => {
                        Some(SnapshotRecord::EdgeTombstone(tombstone.clone()))
                    }
                    LogicalMutation::PutTransaction(_) | LogicalMutation::PutReplicaMetadata(_) => {
                        None
                    }
                })
                .collect::<Vec<_>>();
            records.extend(
                state
                    .transactions
                    .into_iter()
                    .map(SnapshotRecord::Transaction),
            );
            records.extend(
                state
                    .metadata
                    .into_iter()
                    .map(SnapshotRecord::ReplicaMetadata),
            );
            records.extend(state.replay.into_iter().map(|(raft_index, replay)| {
                SnapshotRecord::Replay(
                    SnapshotReplayRecord::new(
                        raft_index,
                        replay.term,
                        replay.command_id,
                        replay.digest,
                    )
                    .expect("stored replay identity was validated on apply"),
                )
            }));
            records.extend(state.changes.into_iter().map(|(cursor, mutation)| {
                SnapshotRecord::Change(ChangeRecord::new(cursor, mutation))
            }));
            let chunks = records
                .chunks(request.max_records_per_chunk() as usize)
                .enumerate()
                .map(|(ordinal, records)| {
                    SnapshotChunk::new(request.snapshot_id(), ordinal as u64, records.to_vec())
                })
                .collect::<Result<Vec<_>, _>>()?;
            let manifest = SnapshotManifest::new(&header, &chunks)?;
            Ok(Box::new(TestSnapshotReader {
                header,
                chunks: chunks.into(),
                manifest,
            }) as Box<dyn LogicalSnapshotReader>)
        })
    }
}

struct TestSnapshotReader {
    header: SnapshotHeader,
    chunks: VecDeque<SnapshotChunk>,
    manifest: SnapshotManifest,
}

impl LogicalSnapshotReader for TestSnapshotReader {
    fn header(&self) -> &SnapshotHeader {
        &self.header
    }

    fn next_chunk(&mut self) -> StoreFuture<'_, Option<SnapshotChunk>> {
        Box::pin(async move { Ok(self.chunks.pop_front()) })
    }

    fn finish(self: Box<Self>) -> StoreFuture<'static, SnapshotManifest> {
        Box::pin(async move {
            if !self.chunks.is_empty() {
                return Err(StorageError::SnapshotNotExhausted);
            }
            Ok(self.manifest)
        })
    }
}

impl LogicalSnapshotSink for TestStore {
    fn begin_restore(
        &self,
        binding: ReplicaBinding,
        header: SnapshotHeader,
    ) -> StoreFuture<'_, Box<dyn LogicalSnapshotWriter>> {
        Box::pin(async move {
            if binding != self.binding {
                return Err(StorageError::StaleBinding {
                    expected: Box::new(self.binding.clone()),
                    actual: Box::new(binding),
                });
            }
            if header.source_binding().cluster_id() != self.binding.cluster_id()
                || header.source_binding().graph_id() != self.binding.graph_id()
                || header.source_binding().shard_id() != self.binding.shard_id()
            {
                return Err(StorageError::SnapshotIdentityMismatch);
            }
            Ok(Box::new(TestSnapshotWriter {
                target_binding: self.binding.clone(),
                header,
                chunks: Vec::new(),
                state: Arc::clone(&self.state),
            }) as Box<dyn LogicalSnapshotWriter>)
        })
    }
}

struct TestSnapshotWriter {
    target_binding: ReplicaBinding,
    header: SnapshotHeader,
    chunks: Vec<SnapshotChunk>,
    state: Arc<Mutex<TestState>>,
}

impl LogicalSnapshotWriter for TestSnapshotWriter {
    fn target_binding(&self) -> &ReplicaBinding {
        &self.target_binding
    }

    fn header(&self) -> &SnapshotHeader {
        &self.header
    }

    fn write_chunk(&mut self, chunk: SnapshotChunk) -> StoreFuture<'_, ()> {
        Box::pin(async move {
            chunk.validate()?;
            if chunk.snapshot_id() != self.header.snapshot_id()
                || chunk.ordinal() != self.chunks.len() as u64
            {
                return Err(StorageError::CorruptSnapshot(
                    "snapshot chunk identity or order mismatch".into(),
                ));
            }
            self.chunks.push(chunk);
            Ok(())
        })
    }

    fn commit(
        self: Box<Self>,
        manifest: SnapshotManifest,
    ) -> StoreFuture<'static, SnapshotRestoreReceipt> {
        Box::pin(async move {
            manifest.validate(&self.header, &self.chunks)?;
            let mut next = TestState {
                applied_index: self.header.applied_index(),
                ..TestState::default()
            };
            for record in self
                .chunks
                .into_iter()
                .flat_map(SnapshotChunk::into_records)
            {
                match record {
                    SnapshotRecord::Vertex(vertex) => {
                        next.history
                            .push(LogicalMutation::PutVertex(vertex.clone()));
                        next.vertices.entry(vertex.id()).or_default().push(vertex);
                    }
                    SnapshotRecord::VertexTombstone(tombstone) => {
                        next.history.push(LogicalMutation::DeleteVertex(tombstone));
                    }
                    SnapshotRecord::Edge(edge) => {
                        next.history.push(LogicalMutation::PutEdge(edge.clone()));
                        next.edges.push(edge);
                    }
                    SnapshotRecord::EdgeTombstone(tombstone) => {
                        next.history.push(LogicalMutation::DeleteEdge(tombstone));
                    }
                    SnapshotRecord::Transaction(transaction) => {
                        next.transactions.push(transaction);
                    }
                    SnapshotRecord::ReplicaMetadata(metadata) => {
                        next.metadata.push(metadata);
                    }
                    SnapshotRecord::Replay(replay) => {
                        next.replay.insert(
                            replay.raft_index(),
                            ReplayRecord {
                                term: replay.raft_term(),
                                command_id: replay.command_id(),
                                digest: replay.mutation_digest(),
                            },
                        );
                    }
                    SnapshotRecord::Change(change) => {
                        next.changes
                            .push((change.cursor(), change.mutation().clone()));
                    }
                }
            }
            *self.state.lock().unwrap() = next;
            Ok(SnapshotRestoreReceipt::new(self.target_binding, manifest))
        })
    }

    fn abort(self: Box<Self>) -> StoreFuture<'static, ()> {
        Box::pin(async { Ok(()) })
    }
}

fn sample_vertex(id: u128, version: u64) -> VertexVersion {
    VertexVersion::new(
        VertexId::new(id).unwrap(),
        Version::new(version),
        ValidInterval::new(0, 100).unwrap(),
        TransactionTime::new(version as i64).unwrap(),
        BTreeMap::from([("name".to_owned(), Value::String(format!("v{id}")))]),
    )
    .unwrap()
}

fn sample_edge(id: u128, source: VertexId, target: VertexId) -> EdgeVersion {
    sample_edge_version(id, source, target, 1)
}

fn sample_edge_version(id: u128, source: VertexId, target: VertexId, version: u64) -> EdgeVersion {
    sample_edge_temporal_version(id, source, target, version, version as i64)
}

fn sample_edge_temporal_version(
    id: u128,
    source: VertexId,
    target: VertexId,
    version: u64,
    transaction_time: i64,
) -> EdgeVersion {
    EdgeVersion::new(
        EdgeId::new(id).unwrap(),
        source,
        target,
        "knows",
        Version::new(version),
        ValidInterval::new(0, 100).unwrap(),
        TransactionTime::new(transaction_time).unwrap(),
        BTreeMap::from([("weight".to_owned(), Value::Integer(7))]),
    )
    .unwrap()
}

fn sample_transaction(id: u128) -> TransactionRecord {
    TransactionRecord::new(
        TransactionId::new(id).unwrap(),
        TransactionState::Committed,
        TransactionTime::new(1).unwrap(),
        Digest32::new([7; 32]),
    )
    .unwrap()
}

fn export_snapshot(
    store: &dyn StorageTckStore,
    binding: &ReplicaBinding,
    applied_index: u64,
    snapshot_id: u128,
    max_records_per_chunk: u32,
) -> (
    SnapshotHeader,
    Vec<SnapshotChunk>,
    SnapshotManifest,
    Vec<SnapshotRecord>,
) {
    let mut reader = block_on(store.begin_snapshot(
        ReadFence::new(binding.clone(), applied_index),
        SnapshotRequest::new(snapshot_id, max_records_per_chunk).unwrap(),
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
fn bounded_read_requests_expose_provider_neutral_query_parameters() {
    let vertex_id = VertexId::new(41).unwrap();
    let edge_id = EdgeId::new(43).unwrap();
    let transaction_from = TransactionTime::new(5).unwrap();
    let transaction_through = TransactionTime::new(17).unwrap();

    let vertex_history =
        VertexHistoryRead::new(vertex_id, transaction_from, transaction_through, 19).unwrap();
    assert_eq!(vertex_history.transaction_from(), transaction_from);
    assert_eq!(vertex_history.transaction_through(), transaction_through);

    let edge_history =
        EdgeHistoryRead::new(edge_id, transaction_from, transaction_through, 23).unwrap();
    assert_eq!(edge_history.transaction_from(), transaction_from);
    assert_eq!(edge_history.transaction_through(), transaction_through);

    let adjacency = AdjacencyRead::new(
        vertex_id,
        dtg_storage::AdjacencyDirection::Incoming,
        29,
        transaction_through,
        31,
    )
    .unwrap();
    assert_eq!(adjacency.vertex_id(), vertex_id);
    assert_eq!(
        adjacency.direction(),
        dtg_storage::AdjacencyDirection::Incoming
    );

    let cursor = ChangeCursor::new(37, 41);
    let changes = ChangesRead::new(Some(cursor), 43, 47).unwrap();
    assert_eq!(changes.after(), Some(cursor));
    assert_eq!(changes.through_index(), 43);
}
