use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::pin::pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use dtg_storage::{
    AdjacencyRead, ApplyReceipt, ArtifactStore, BackendClass, BindingRole, CapabilityManifest,
    ChangePage, ChangesRead, CommandId, CommittedShardBatch, ConsensusStore, Digest32,
    DurabilityPolicy, EdgeHistoryRead, EdgeRead, EdgeVersion, LogicalMutation,
    LogicalSnapshotReader, LogicalSnapshotSink, LogicalSnapshotSource, LogicalSnapshotWriter,
    ProviderKind, PushdownExecutor, PushdownOperation, PushdownOutcome, PushdownRequest, ReadFence,
    ReplicaBinding, ReplicaStateStore, ScanPage, SnapshotChunk, SnapshotHeader, SnapshotManifest,
    SnapshotRecord, SnapshotRequest, SnapshotRestoreReceipt, StorageError, StorageTckFactory,
    StorageTckStore, StoreFuture, TemporalReadView, TransactionRecord, TransactionTime,
    ValidInterval, Value, Version, VertexHistoryRead, VertexId, VertexRead, VertexScan,
    VertexVersion, run_storage_tck,
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
    replay: BTreeMap<u64, ReplayRecord>,
    changes: Vec<(u64, LogicalMutation)>,
}

#[derive(Clone)]
struct TestStore {
    binding: ReplicaBinding,
    capabilities: CapabilityManifest,
    state: Arc<Mutex<TestState>>,
}

#[derive(Default)]
struct FactoryState {
    owners: BTreeMap<String, ReplicaBinding>,
    stores: BTreeMap<String, Arc<Mutex<TestState>>>,
}

struct TestFactory {
    state: Mutex<FactoryState>,
    capabilities: CapabilityManifest,
}

impl TestFactory {
    fn new() -> Self {
        Self {
            state: Mutex::new(FactoryState::default()),
            capabilities: capabilities(),
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
                    LogicalMutation::PutEdge(edge) => next.edges.push(edge.clone()),
                    LogicalMutation::DeleteEdge(tombstone) => {
                        next.edges.retain(|edge| edge.id() != tombstone.id());
                    }
                    LogicalMutation::PutTransaction(transaction) => {
                        next.transactions.push(transaction.clone());
                    }
                    LogicalMutation::PutReplicaMetadata(_) => {}
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

    fn scan_vertices(&self, request: VertexScan) -> StoreFuture<'_, ScanPage<VertexVersion>> {
        Box::pin(async move {
            let rows = self
                .state
                .vertices
                .values()
                .filter_map(|versions| {
                    visible_vertex(
                        versions,
                        &VertexRead::new(
                            versions.first().unwrap().id(),
                            request.valid_at(),
                            request.transaction_at(),
                        ),
                    )
                })
                .take(request.limit() as usize)
                .collect();
            Ok(ScanPage::new(rows, None))
        })
    }

    fn scan_edges(&self, request: dtg_storage::EdgeScan) -> StoreFuture<'_, ScanPage<EdgeVersion>> {
        Box::pin(async move {
            let rows = self
                .state
                .edges
                .iter()
                .filter(|edge| request.includes(edge))
                .take(request.limit() as usize)
                .cloned()
                .collect();
            Ok(ScanPage::new(rows, None))
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
                        next.vertices.entry(vertex.id()).or_default().push(vertex);
                    }
                    SnapshotRecord::Edge(edge) => next.edges.push(edge),
                    SnapshotRecord::Transaction(transaction) => {
                        next.transactions.push(transaction);
                    }
                    SnapshotRecord::ReplicaMetadata(_) => {}
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
