use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::future::Future;
use std::pin::pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use dtg_storage::{
    AdjacencyRead, ApplyReceipt, ArtifactStore, BackendClass, BindingRole, CapabilityManifest,
    ChangePage, ChangesRead, CommandId, CommittedShardBatch, ConsensusCommandEnvelope,
    ConsensusEntry, ConsensusStore, Digest32, DurabilityPolicy, EdgeHistoryRead, EdgeId, EdgeRead,
    EdgeScan, EdgeTombstone, EdgeVersion, LogicalMutation, LogicalSnapshotReader,
    LogicalSnapshotSink, LogicalSnapshotSource, LogicalSnapshotWriter, ProviderKind,
    PushdownExecutor, PushdownOperation, PushdownOutcome, PushdownRequest, ReadFence,
    ReplicaBinding, ReplicaMetadata, ReplicaStateStore, SUPPORTED_CONSENSUS_COMMAND_FORMAT_VERSION,
    SUPPORTED_CONSENSUS_WAL_FORMAT_VERSION, SUPPORTED_PUSHDOWN_CONTRACT_VERSION,
    SUPPORTED_SNAPSHOT_FORMAT_VERSION, ScanPage, SnapshotChunk, SnapshotHeader, SnapshotManifest,
    SnapshotRecord, SnapshotRequest, SnapshotRestoreReceipt, StorageError, StorageTckFactory,
    StorageTckStore, StoreFuture, TemporalReadView, TransactionId, TransactionRecord,
    TransactionState, TransactionTime, ValidInterval, Value, Version, VertexHistoryRead, VertexId,
    VertexRead, VertexScan, VertexTombstone, VertexVersion, run_storage_tck,
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
                    LogicalMutation::PutVertex(second),
                ],
            )
            .unwrap(),
        ),
    )
    .unwrap();

    let staged = sample_vertex(3, 1);
    let invalid_edge = sample_edge(9, staged.id(), VertexId::new(4).unwrap());
    let invalid_batch = CommittedShardBatch::new(
        store_binding.clone(),
        1,
        2,
        CommandId::new(2).unwrap(),
        vec![
            LogicalMutation::PutVertex(staged.clone()),
            LogicalMutation::PutEdge(invalid_edge.clone()),
        ],
    )
    .unwrap();
    invalid_batch.validate().unwrap();
    assert!(block_on(store.apply(invalid_batch)).is_err());
    assert_eq!(block_on(store.applied_index()).unwrap(), 1);

    let view = block_on(store.begin_read_view(ReadFence::new(store_binding, 1))).unwrap();
    assert_eq!(
        block_on(view.get_vertex(VertexRead::new(
            first.id(),
            10,
            TransactionTime::new(10).unwrap(),
        )))
        .unwrap(),
        Some(first)
    );
    assert!(
        block_on(view.get_vertex(VertexRead::new(
            staged.id(),
            10,
            TransactionTime::new(10).unwrap(),
        )))
        .unwrap()
        .is_none()
    );
    assert!(
        block_on(view.get_edge(EdgeRead::new(
            invalid_edge.id(),
            10,
            TransactionTime::new(10).unwrap(),
        )))
        .unwrap()
        .is_none()
    );
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
    let expected = vec![
        SnapshotRecord::Vertex(first.clone()),
        SnapshotRecord::Vertex(second.clone()),
        SnapshotRecord::Edge(edge.clone()),
        SnapshotRecord::Transaction(transaction.clone()),
        SnapshotRecord::ReplicaMetadata(metadata.clone()),
    ];
    block_on(
        source.apply(
            CommittedShardBatch::new(
                source_binding.clone(),
                1,
                1,
                CommandId::new(14).unwrap(),
                vec![
                    LogicalMutation::PutVertex(first),
                    LogicalMutation::PutVertex(second),
                    LogicalMutation::PutEdge(edge),
                    LogicalMutation::PutTransaction(transaction),
                    LogicalMutation::PutReplicaMetadata(metadata),
                ],
            )
            .unwrap(),
        ),
    )
    .unwrap();

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
fn public_tck_certifies_execution_failure_and_complete_snapshot() {
    let factory = TestFactory::new();
    block_on(run_storage_tck(&factory)).unwrap();
    let audit = factory.audit.lock().unwrap().clone();
    assert!(audit.execution_stage_failures > 0);
    assert_eq!(
        audit.restored_record_kinds,
        BTreeSet::from(["edge", "metadata", "transaction", "vertex"])
    );
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
    replay: BTreeMap<u64, ReplayRecord>,
    changes: Vec<(u64, LogicalMutation)>,
}

#[derive(Clone)]
struct TestStore {
    binding: ReplicaBinding,
    capabilities: CapabilityManifest,
    state: Arc<Mutex<TestState>>,
    audit: Arc<Mutex<TestAudit>>,
}

#[derive(Clone, Default)]
struct TestAudit {
    execution_stage_failures: usize,
    restored_record_kinds: BTreeSet<&'static str>,
}

#[derive(Default)]
struct FactoryState {
    owners: BTreeMap<String, ReplicaBinding>,
    stores: BTreeMap<String, Arc<Mutex<TestState>>>,
}

struct TestFactory {
    state: Mutex<FactoryState>,
    capabilities: CapabilityManifest,
    audit: Arc<Mutex<TestAudit>>,
}

impl TestFactory {
    fn new() -> Self {
        Self {
            state: Mutex::new(FactoryState::default()),
            capabilities: capabilities(),
            audit: Arc::new(Mutex::new(TestAudit::default())),
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
                audit: Arc::clone(&self.audit),
            }) as Box<dyn StorageTckStore>)
        })
    }
}

impl ReplicaStateStore for TestStore {
    fn binding(&self) -> &ReplicaBinding {
        &self.binding
    }

    fn applied_index(&self) -> StoreFuture<'_, u64> {
        Box::pin(async move { Ok(self.state.lock().unwrap().applied_index) })
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
                return Ok(ApplyReceipt::new(&batch, true));
            }
            if batch.raft_index() != state.applied_index + 1 {
                return Err(StorageError::NonMonotonicIndex {
                    applied: state.applied_index,
                    proposed: batch.raft_index(),
                });
            }

            let mut next = state.clone();
            for mutation in batch.mutations() {
                match mutation {
                    LogicalMutation::PutVertex(vertex) => {
                        next.vertices
                            .entry(vertex.id())
                            .or_default()
                            .push(vertex.clone());
                    }
                    LogicalMutation::DeleteVertex(tombstone) => {
                        next.vertices.remove(&tombstone.id());
                    }
                    LogicalMutation::PutEdge(edge) => {
                        if !next.vertices.contains_key(&edge.source())
                            || !next.vertices.contains_key(&edge.target())
                        {
                            self.audit.lock().unwrap().execution_stage_failures += 1;
                            return Err(StorageError::ConstraintViolation(
                                "edge endpoints must exist during atomic application".into(),
                            ));
                        }
                        next.edges.push(edge.clone());
                    }
                    LogicalMutation::DeleteEdge(tombstone) => {
                        next.edges.retain(|edge| edge.id() != tombstone.id());
                    }
                    LogicalMutation::PutTransaction(transaction) => {
                        next.transactions.push(transaction.clone());
                    }
                    LogicalMutation::PutReplicaMetadata(metadata) => {
                        next.metadata
                            .retain(|existing| existing.name() != metadata.name());
                        next.metadata.push(metadata.clone());
                    }
                }
                next.changes.push((batch.raft_index(), mutation.clone()));
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

fn visible_vertex(versions: &[VertexVersion], request: &VertexRead) -> Option<VertexVersion> {
    versions
        .iter()
        .filter(|version| {
            version.valid_time().start() <= request.valid_at()
                && request.valid_at() < version.valid_time().end()
                && version.transaction_time() <= request.transaction_at()
        })
        .max_by_key(|version| (version.transaction_time(), version.version()))
        .cloned()
}

impl TemporalReadView for TestReadView {
    fn fence(&self) -> &ReadFence {
        &self.fence
    }

    fn get_vertex(&self, request: VertexRead) -> StoreFuture<'_, Option<VertexVersion>> {
        Box::pin(async move {
            Ok(self
                .state
                .vertices
                .get(&request.id())
                .and_then(|versions| visible_vertex(versions, &request)))
        })
    }

    fn get_edge(&self, request: EdgeRead) -> StoreFuture<'_, Option<EdgeVersion>> {
        Box::pin(async move {
            Ok(self
                .state
                .edges
                .iter()
                .filter(|edge| {
                    edge.id() == request.id()
                        && edge.valid_time().start() <= request.valid_at()
                        && request.valid_at() < edge.valid_time().end()
                        && edge.transaction_time() <= request.transaction_at()
                })
                .max_by_key(|edge| (edge.transaction_time(), edge.version()))
                .cloned())
        })
    }

    fn vertex_history(&self, request: VertexHistoryRead) -> StoreFuture<'_, Vec<VertexVersion>> {
        Box::pin(async move {
            let mut versions = self
                .state
                .vertices
                .get(&request.id())
                .cloned()
                .unwrap_or_default();
            versions.retain(|version| request.includes(version));
            versions.sort_by_key(|version| (version.transaction_time(), version.version()));
            versions.truncate(request.limit() as usize);
            Ok(versions)
        })
    }

    fn edge_history(&self, request: EdgeHistoryRead) -> StoreFuture<'_, Vec<EdgeVersion>> {
        Box::pin(async move {
            let mut versions = self
                .state
                .edges
                .iter()
                .filter(|edge| edge.id() == request.id() && request.includes(edge))
                .cloned()
                .collect::<Vec<_>>();
            versions.sort_by_key(|edge| (edge.transaction_time(), edge.version()));
            versions.truncate(request.limit() as usize);
            Ok(versions)
        })
    }

    fn expand(&self, request: AdjacencyRead) -> StoreFuture<'_, Vec<EdgeVersion>> {
        Box::pin(async move {
            let mut edges = self
                .state
                .edges
                .iter()
                .filter(|edge| request.matches(edge))
                .cloned()
                .collect::<Vec<_>>();
            edges.sort_by_key(EdgeVersion::id);
            edges.truncate(request.limit() as usize);
            Ok(edges)
        })
    }

    fn changes(&self, request: ChangesRead) -> StoreFuture<'_, ChangePage> {
        Box::pin(async move {
            let changes = self
                .state
                .changes
                .iter()
                .filter(|(index, _)| request.includes(*index))
                .take(request.limit() as usize)
                .map(|(index, mutation)| dtg_storage::ChangeRecord::new(*index, mutation.clone()))
                .collect();
            Ok(ChangePage::new(changes, None))
        })
    }

    fn scan_vertices(
        &self,
        request: VertexScan,
    ) -> StoreFuture<'_, ScanPage<VertexVersion, VertexId>> {
        Box::pin(async move {
            let mut rows = self
                .state
                .vertices
                .iter()
                .filter(|(id, _)| request.after().is_none_or(|after| **id > after))
                .filter_map(|(_, versions)| {
                    visible_vertex(
                        versions,
                        &VertexRead::new(
                            versions.first().unwrap().id(),
                            request.valid_at(),
                            request.transaction_at(),
                        ),
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
            let mut rows = self
                .state
                .edges
                .iter()
                .filter(|edge| request.includes(edge))
                .cloned()
                .collect::<Vec<_>>();
            rows.sort_by_key(EdgeVersion::id);
            rows.truncate(request.limit() as usize + 1);
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
                PushdownOperation::Vertex(read) => state
                    .vertices
                    .get(&read.id())
                    .and_then(|versions| visible_vertex(versions, read))
                    .map(SnapshotRecord::Vertex)
                    .into_iter()
                    .collect(),
                PushdownOperation::VertexScan(scan) => state
                    .vertices
                    .values()
                    .filter_map(|versions| {
                        visible_vertex(
                            versions,
                            &VertexRead::new(
                                versions.first().unwrap().id(),
                                scan.valid_at(),
                                scan.transaction_at(),
                            ),
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
            let mut records = Vec::new();
            for versions in state.vertices.values() {
                records.extend(versions.iter().cloned().map(SnapshotRecord::Vertex));
            }
            records.extend(state.edges.into_iter().map(SnapshotRecord::Edge));
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
                audit: Arc::clone(&self.audit),
            }) as Box<dyn LogicalSnapshotWriter>)
        })
    }
}

struct TestSnapshotWriter {
    target_binding: ReplicaBinding,
    header: SnapshotHeader,
    chunks: Vec<SnapshotChunk>,
    state: Arc<Mutex<TestState>>,
    audit: Arc<Mutex<TestAudit>>,
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
                        self.audit
                            .lock()
                            .unwrap()
                            .restored_record_kinds
                            .insert("vertex");
                        next.vertices.entry(vertex.id()).or_default().push(vertex);
                    }
                    SnapshotRecord::Edge(edge) => {
                        self.audit
                            .lock()
                            .unwrap()
                            .restored_record_kinds
                            .insert("edge");
                        next.edges.push(edge);
                    }
                    SnapshotRecord::Transaction(transaction) => {
                        self.audit
                            .lock()
                            .unwrap()
                            .restored_record_kinds
                            .insert("transaction");
                        next.transactions.push(transaction);
                    }
                    SnapshotRecord::ReplicaMetadata(metadata) => {
                        self.audit
                            .lock()
                            .unwrap()
                            .restored_record_kinds
                            .insert("metadata");
                        next.metadata.push(metadata);
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

#[allow(dead_code)]
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
    EdgeVersion::new(
        EdgeId::new(id).unwrap(),
        source,
        target,
        "knows",
        Version::new(1),
        ValidInterval::new(0, 100).unwrap(),
        TransactionTime::new(1).unwrap(),
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
