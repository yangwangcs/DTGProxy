#![forbid(unsafe_code)]

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    fmt,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use bincode::{Decode, Encode};
use dtg_storage::{
    AdjacencyRead, ApplyReceipt, BackendClass, BindingRole, CapabilityManifest, ChangeCursor,
    ChangePage, ChangeRecord, ChangesRead, CommittedShardBatch, Digest32, EdgeHistoryRead, EdgeId,
    EdgeRead, EdgeScan, EdgeTombstone, EdgeVersion, LogicalMutation, LogicalReplicaActivation,
    LogicalReplicaActivationReceipt, LogicalSnapshotCandidateReceipt, LogicalSnapshotReader,
    LogicalSnapshotSink, LogicalSnapshotSource, LogicalSnapshotWriter, Properties, ProviderKind,
    PushdownExecutor, PushdownOperation, PushdownOutcome, PushdownRequest, ReadFence,
    ReplicaBinding, ReplicaMetadata, ReplicaStateStore, ScanPage, SnapshotChunk, SnapshotHeader,
    SnapshotManifest, SnapshotReplayRecord, SnapshotRequest, SnapshotRestoreReceipt, StorageError,
    StorageTckFactory, StorageTckStore, StoreFuture, TemporalReadView, TransactionId,
    TransactionRecord, TransactionState, TransactionTime, ValidInterval, Value, Version,
    VertexHistoryRead, VertexId, VertexRead, VertexScan, VertexTombstone, VertexVersion,
    run_storage_tck,
};
use kuzu::{Connection, Database, SystemConfig, Value as KuzuValue};

const KUZU_CONTRACT_VERSION: u32 = 1;
const KUZU_LAYOUT_VERSION: u32 = 1;

#[derive(Clone)]
struct State {
    applied_index: u64,
    history: Vec<LogicalMutation>,
    replay: BTreeMap<u64, Replay>,
    changes: Vec<(ChangeCursor, LogicalMutation)>,
}

#[derive(Clone)]
struct Replay {
    term: u64,
    command_id: dtg_storage::CommandId,
    digest: Digest32,
}

impl State {
    fn empty() -> Self {
        Self {
            applied_index: 0,
            history: Vec::new(),
            replay: BTreeMap::new(),
            changes: Vec::new(),
        }
    }
}

struct Inner {
    database: Database,
    binding: ReplicaBinding,
    capabilities: CapabilityManifest,
    state: Mutex<State>,
    apply_guard: Mutex<()>,
}

#[derive(Clone)]
pub struct KuzuReplicaStore {
    inner: Arc<Inner>,
}

impl fmt::Debug for KuzuReplicaStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KuzuReplicaStore")
            .field("binding", &self.inner.binding)
            .finish_non_exhaustive()
    }
}

impl KuzuReplicaStore {
    pub fn open(path: impl AsRef<Path>, binding: ReplicaBinding) -> Result<Self, StorageError> {
        validate_binding(&binding)?;
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|error| {
                StorageError::Internal(format!(
                    "cannot create Kuzu database parent directory: {error}"
                ))
            })?;
        }
        let database = Database::new(path, SystemConfig::default()).map_err(kuzu_error)?;
        initialize_namespace(&database, &binding)?;
        let state = load_state(&database)?;
        Ok(Self {
            inner: Arc::new(Inner {
                database,
                binding,
                capabilities: capabilities()?,
                state: Mutex::new(state),
                apply_guard: Mutex::new(()),
            }),
        })
    }

    pub fn capabilities(&self) -> &CapabilityManifest {
        &self.inner.capabilities
    }

    fn verify_fence(&self, fence: &ReadFence) -> Result<State, StorageError> {
        if fence.binding() != &self.inner.binding {
            return Err(StorageError::StaleBinding {
                expected: Box::new(self.inner.binding.clone()),
                actual: Box::new(fence.binding().clone()),
            });
        }
        if fence.capability_digest() != self.inner.capabilities.digest() {
            return Err(StorageError::CapabilityDrift);
        }
        let state = lock(&self.inner.state, "Kuzu state")?.clone();
        if state.applied_index != fence.applied_index() {
            return Err(StorageError::ReadFenceUnavailable {
                requested: fence.applied_index(),
                applied: state.applied_index,
            });
        }
        Ok(state)
    }

    fn restore_state(&self, state: State) -> Result<(), StorageError> {
        persist_state(&self.inner.database, &state)?;
        *lock(&self.inner.state, "Kuzu state")? = state;
        Ok(())
    }
}

impl ReplicaStateStore for KuzuReplicaStore {
    fn binding(&self) -> &ReplicaBinding {
        &self.inner.binding
    }

    fn applied_index(&self) -> StoreFuture<'_, u64> {
        Box::pin(async move { Ok(lock(&self.inner.state, "Kuzu state")?.applied_index) })
    }

    fn replica_metadata<'a>(&'a self, name: &'a str) -> StoreFuture<'a, Option<ReplicaMetadata>> {
        Box::pin(async move {
            Ok(lock(&self.inner.state, "Kuzu state")?
                .history
                .iter()
                .rev()
                .find_map(|mutation| match mutation {
                    LogicalMutation::PutReplicaMetadata(metadata) if metadata.name() == name => {
                        Some(metadata.clone())
                    }
                    _ => None,
                }))
        })
    }

    fn apply(&self, batch: CommittedShardBatch) -> StoreFuture<'_, ApplyReceipt> {
        Box::pin(async move {
            batch.validate()?;
            if batch.binding() != &self.inner.binding {
                return Err(StorageError::StaleBinding {
                    expected: Box::new(self.inner.binding.clone()),
                    actual: Box::new(batch.binding().clone()),
                });
            }
            let _apply_guard = lock(&self.inner.apply_guard, "Kuzu apply")?;
            let current = lock(&self.inner.state, "Kuzu state")?.clone();
            if batch.raft_index() <= current.applied_index {
                let replay = current.replay.get(&batch.raft_index()).ok_or(
                    StorageError::ReplayMismatch {
                        raft_index: batch.raft_index(),
                    },
                )?;
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
            if batch.raft_index() != current.applied_index.saturating_add(1) {
                return Err(StorageError::NonMonotonicIndex {
                    applied: current.applied_index,
                    proposed: batch.raft_index(),
                });
            }
            let mut next = current;
            for (ordinal, mutation) in batch.mutations().iter().enumerate() {
                next.history.push(mutation.clone());
                next.changes.push((
                    ChangeCursor::new(batch.raft_index(), ordinal as u64),
                    mutation.clone(),
                ));
            }
            next.applied_index = batch.raft_index();
            next.replay.insert(
                batch.raft_index(),
                Replay {
                    term: batch.raft_term(),
                    command_id: batch.command_id(),
                    digest: batch.mutation_digest(),
                },
            );
            persist_state(&self.inner.database, &next)?;
            *lock(&self.inner.state, "Kuzu state")? = next;
            Ok(ApplyReceipt::new(&batch, false))
        })
    }

    fn begin_read_view(&self, fence: ReadFence) -> StoreFuture<'_, Box<dyn TemporalReadView>> {
        Box::pin(async move {
            let state = self.verify_fence(&fence)?;
            Ok(Box::new(KuzuReadView { fence, state }) as Box<dyn TemporalReadView>)
        })
    }
}

struct KuzuReadView {
    fence: ReadFence,
    state: State,
}

impl TemporalReadView for KuzuReadView {
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
            let mut rows = self
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
            rows.sort_by_key(|vertex| (vertex.transaction_time(), vertex.version()));
            rows.truncate(request.limit() as usize);
            Ok(rows)
        })
    }

    fn edge_history(&self, request: EdgeHistoryRead) -> StoreFuture<'_, Vec<EdgeVersion>> {
        Box::pin(async move {
            let mut rows = self
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
            rows.sort_by_key(|edge| (edge.transaction_time(), edge.version()));
            rows.truncate(request.limit() as usize);
            Ok(rows)
        })
    }

    fn expand(&self, request: AdjacencyRead) -> StoreFuture<'_, Vec<EdgeVersion>> {
        Box::pin(async move {
            let mut rows = edge_ids(&self.state.history)
                .into_iter()
                .filter_map(|id| {
                    visible_edge(
                        &self.state.history,
                        &EdgeRead::new(id, request.valid_at(), request.transaction_at()),
                    )
                })
                .filter(|edge| request.matches(edge))
                .collect::<Vec<_>>();
            rows.sort_by_key(EdgeVersion::id);
            rows.truncate(request.limit() as usize);
            Ok(rows)
        })
    }

    fn changes(&self, request: ChangesRead) -> StoreFuture<'_, ChangePage> {
        Box::pin(async move {
            let mut rows = self
                .state
                .changes
                .iter()
                .filter(|(cursor, _)| request.includes(*cursor))
                .map(|(cursor, mutation)| ChangeRecord::new(*cursor, mutation.clone()))
                .collect::<Vec<_>>();
            rows.sort_by_key(ChangeRecord::cursor);
            let has_more = rows.len() > request.limit() as usize;
            rows.truncate(request.limit() as usize);
            Ok(ChangePage::new(
                rows.clone(),
                has_more.then(|| {
                    rows.last()
                        .expect("bounded Kuzu changes are nonempty")
                        .cursor()
                }),
            ))
        })
    }

    fn scan_vertices(
        &self,
        request: VertexScan,
    ) -> StoreFuture<'_, ScanPage<VertexVersion, VertexId>> {
        Box::pin(async move {
            let mut rows = vertex_ids(&self.state.history)
                .into_iter()
                .filter(|id| request.after().is_none_or(|after| *id > after))
                .filter_map(|id| {
                    visible_vertex(
                        &self.state.history,
                        &VertexRead::new(id, request.valid_at(), request.transaction_at()),
                    )
                })
                .collect::<Vec<_>>();
            rows.sort_by_key(VertexVersion::id);
            page(rows, request.limit(), VertexVersion::id)
        })
    }

    fn scan_edges(&self, request: EdgeScan) -> StoreFuture<'_, ScanPage<EdgeVersion, EdgeId>> {
        Box::pin(async move {
            let mut rows = edge_ids(&self.state.history)
                .into_iter()
                .filter(|id| request.after().is_none_or(|after| *id > after))
                .filter_map(|id| {
                    visible_edge(
                        &self.state.history,
                        &EdgeRead::new(id, request.valid_at(), request.transaction_at()),
                    )
                })
                .collect::<Vec<_>>();
            rows.sort_by_key(EdgeVersion::id);
            page(rows, request.limit(), EdgeVersion::id)
        })
    }
}

fn page<T, C: Copy>(
    mut rows: Vec<T>,
    limit: u32,
    cursor: impl Fn(&T) -> C,
) -> Result<ScanPage<T, C>, StorageError> {
    let has_more = rows.len() > limit as usize;
    rows.truncate(limit as usize);
    let next = has_more.then(|| cursor(rows.last().expect("bounded Kuzu scan is nonempty")));
    Ok(ScanPage::new(rows, next))
}

impl PushdownExecutor for KuzuReplicaStore {
    fn binding(&self) -> &ReplicaBinding {
        &self.inner.binding
    }

    fn capabilities(&self) -> &CapabilityManifest {
        &self.inner.capabilities
    }

    fn execute_pushdown(&self, request: PushdownRequest) -> StoreFuture<'_, PushdownOutcome> {
        Box::pin(async move {
            request.validate()?;
            let view = self.begin_read_view(request.fence().clone()).await?;
            let rows = match request.operation() {
                PushdownOperation::Vertex(read) => view
                    .get_vertex(read.clone())
                    .await?
                    .into_iter()
                    .map(dtg_storage::SnapshotRecord::Vertex)
                    .collect(),
                PushdownOperation::VertexScan(scan) => view
                    .scan_vertices(scan.clone())
                    .await?
                    .rows()
                    .iter()
                    .cloned()
                    .map(dtg_storage::SnapshotRecord::Vertex)
                    .collect(),
            };
            if self
                .capabilities()
                .contains_all(request.required_capabilities())
            {
                Ok(PushdownOutcome::Exact(rows))
            } else {
                let guarantees = self
                    .capabilities()
                    .intersection(request.required_capabilities());
                if guarantees.is_empty() {
                    Ok(PushdownOutcome::Unsupported)
                } else {
                    Ok(PushdownOutcome::ResidualRequired { rows, guarantees })
                }
            }
        })
    }
}

impl LogicalSnapshotSource for KuzuReplicaStore {
    fn begin_snapshot(
        &self,
        fence: ReadFence,
        request: SnapshotRequest,
    ) -> StoreFuture<'_, Box<dyn LogicalSnapshotReader>> {
        Box::pin(async move {
            request.validate()?;
            let state = self.verify_fence(&fence)?;
            let header = SnapshotHeader::new(
                request.snapshot_id(),
                self.inner.binding.clone(),
                state.applied_index,
                dtg_storage::SUPPORTED_SNAPSHOT_FORMAT_VERSION,
            )?;
            let chunks = snapshot_chunks(&header, &state, request.max_records_per_chunk())?;
            let manifest = SnapshotManifest::new(&header, &chunks)?;
            Ok(Box::new(KuzuSnapshotReader {
                header,
                chunks: chunks.into(),
                manifest,
            }) as Box<dyn LogicalSnapshotReader>)
        })
    }
}

struct KuzuSnapshotReader {
    header: SnapshotHeader,
    chunks: VecDeque<SnapshotChunk>,
    manifest: SnapshotManifest,
}

impl LogicalSnapshotReader for KuzuSnapshotReader {
    fn header(&self) -> &SnapshotHeader {
        &self.header
    }

    fn next_chunk(&mut self) -> StoreFuture<'_, Option<SnapshotChunk>> {
        Box::pin(async move { Ok(self.chunks.pop_front()) })
    }

    fn finish(self: Box<Self>) -> StoreFuture<'static, SnapshotManifest> {
        Box::pin(async move {
            if self.chunks.is_empty() {
                Ok(self.manifest)
            } else {
                Err(StorageError::SnapshotNotExhausted)
            }
        })
    }
}

impl LogicalSnapshotSink for KuzuReplicaStore {
    fn begin_restore(
        &self,
        binding: ReplicaBinding,
        header: SnapshotHeader,
    ) -> StoreFuture<'_, Box<dyn LogicalSnapshotWriter>> {
        Box::pin(async move {
            if binding != self.inner.binding {
                return Err(StorageError::StaleBinding {
                    expected: Box::new(self.inner.binding.clone()),
                    actual: Box::new(binding),
                });
            }
            if !same_logical_identity(header.source_binding(), &self.inner.binding) {
                return Err(StorageError::SnapshotIdentityMismatch);
            }
            Ok(Box::new(KuzuSnapshotWriter {
                store: self.clone(),
                target_binding: self.inner.binding.clone(),
                header,
                chunks: Vec::new(),
            }) as Box<dyn LogicalSnapshotWriter>)
        })
    }
}

impl LogicalReplicaActivation for KuzuReplicaStore {
    fn activate_candidate(
        &self,
        candidate: LogicalSnapshotCandidateReceipt,
        active_binding: ReplicaBinding,
    ) -> StoreFuture<'_, LogicalReplicaActivationReceipt> {
        Box::pin(async move {
            let receipt = LogicalReplicaActivationReceipt::new(&candidate, active_binding.clone())?;
            if self.inner.binding != *candidate.candidate_binding() {
                return Err(StorageError::SnapshotIdentityMismatch);
            }
            let connection = Connection::new(&self.inner.database).map_err(kuzu_error)?;
            let mut owners = connection
                .query("MATCH (owner:DtgOwner {id: 'owner'}) RETURN owner.payload;")
                .map_err(kuzu_error)?;
            let Some(row) = owners.next() else {
                return Err(StorageError::Internal(
                    "missing Kuzu namespace owner".into(),
                ));
            };
            let KuzuValue::Blob(bytes) = &row[0] else {
                return Err(StorageError::Internal(
                    "Kuzu owner payload is not a BLOB".into(),
                ));
            };
            let owner = decode_binding(bytes)?;
            if owner != *candidate.candidate_binding() && owner != active_binding {
                return Err(StorageError::SnapshotIdentityMismatch);
            }
            if owner == active_binding {
                return Ok(receipt);
            }
            let mut statement = connection
                .prepare("MATCH (owner:DtgOwner {id: 'owner'}) SET owner.payload = $payload;")
                .map_err(kuzu_error)?;
            connection
                .execute(
                    &mut statement,
                    vec![("payload", KuzuValue::Blob(encode_binding(&active_binding)?))],
                )
                .map_err(kuzu_error)?;
            Ok(receipt)
        })
    }
}

struct KuzuSnapshotWriter {
    store: KuzuReplicaStore,
    target_binding: ReplicaBinding,
    header: SnapshotHeader,
    chunks: Vec<SnapshotChunk>,
}

impl LogicalSnapshotWriter for KuzuSnapshotWriter {
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
                    "Kuzu snapshot chunk identity or order mismatch".into(),
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
            let state = state_from_snapshot(&self.chunks, self.header.applied_index())?;
            self.store.restore_state(state)?;
            Ok(SnapshotRestoreReceipt::new(self.target_binding, manifest))
        })
    }

    fn abort(self: Box<Self>) -> StoreFuture<'static, ()> {
        Box::pin(async { Ok(()) })
    }
}

pub struct KuzuStorageTckFactory {
    root: PathBuf,
    capabilities: CapabilityManifest,
}

impl KuzuStorageTckFactory {
    pub fn new(root: impl AsRef<Path>) -> Self {
        Self {
            root: root.as_ref().to_path_buf(),
            capabilities: capabilities().expect("static Kuzu capabilities are valid"),
        }
    }
}

impl StorageTckFactory for KuzuStorageTckFactory {
    fn capabilities(&self) -> CapabilityManifest {
        self.capabilities.clone()
    }

    fn binding(
        &self,
        namespace: &str,
        backend_generation: u64,
    ) -> Result<ReplicaBinding, StorageError> {
        let class = BackendClass::new(
            ProviderKind::Kuzu,
            KUZU_CONTRACT_VERSION,
            KUZU_LAYOUT_VERSION,
            self.capabilities.names().map(str::to_owned),
        )?;
        ReplicaBinding::builder()
            .cluster_id(1)
            .graph_id(7)
            .shard_id(11)
            .placement_epoch(13)
            .replica_id(17)
            .backend_generation(backend_generation)
            .backend_class_digest(class.digest())
            .provider_kind(ProviderKind::Kuzu)
            .contract_version(KUZU_CONTRACT_VERSION)
            .layout_version(KUZU_LAYOUT_VERSION)
            .capability_digest(self.capabilities.digest())
            .namespace_id(namespace)
            .endpoint_profile_ref("local-kuzu")
            .credential_ref("local-kuzu")
            .role(BindingRole::Active)
            .build()
    }

    fn open(&self, binding: ReplicaBinding) -> StoreFuture<'_, Box<dyn StorageTckStore>> {
        Box::pin(async move {
            Ok(Box::new(KuzuTckStore {
                store: KuzuReplicaStore::open(
                    self.root.join(binding.namespace_id().as_str()),
                    binding,
                )?,
                failure_after: Mutex::new(None),
            }) as Box<dyn StorageTckStore>)
        })
    }
}

struct KuzuTckStore {
    store: KuzuReplicaStore,
    failure_after: Mutex<Option<usize>>,
}

impl StorageTckStore for KuzuTckStore {
    fn arm_apply_failure_after(&self, staged_mutations: usize) -> Result<(), StorageError> {
        *lock(&self.failure_after, "Kuzu TCK fault")? = Some(staged_mutations);
        Ok(())
    }
}

impl ReplicaStateStore for KuzuTckStore {
    fn binding(&self) -> &ReplicaBinding {
        ReplicaStateStore::binding(&self.store)
    }
    fn applied_index(&self) -> StoreFuture<'_, u64> {
        self.store.applied_index()
    }
    fn replica_metadata<'a>(&'a self, name: &'a str) -> StoreFuture<'a, Option<ReplicaMetadata>> {
        self.store.replica_metadata(name)
    }
    fn apply(&self, batch: CommittedShardBatch) -> StoreFuture<'_, ApplyReceipt> {
        Box::pin(async move {
            if let Some(after) = lock(&self.failure_after, "Kuzu TCK fault")?.take()
                && after > 0
                && after <= batch.mutations().len()
            {
                return Err(StorageError::InjectedApplyFailure {
                    staged_mutations: after,
                });
            }
            self.store.apply(batch).await
        })
    }
    fn begin_read_view(&self, fence: ReadFence) -> StoreFuture<'_, Box<dyn TemporalReadView>> {
        self.store.begin_read_view(fence)
    }
}

impl LogicalSnapshotSource for KuzuTckStore {
    fn begin_snapshot(
        &self,
        fence: ReadFence,
        request: SnapshotRequest,
    ) -> StoreFuture<'_, Box<dyn LogicalSnapshotReader>> {
        self.store.begin_snapshot(fence, request)
    }
}
impl LogicalSnapshotSink for KuzuTckStore {
    fn begin_restore(
        &self,
        binding: ReplicaBinding,
        header: SnapshotHeader,
    ) -> StoreFuture<'_, Box<dyn LogicalSnapshotWriter>> {
        self.store.begin_restore(binding, header)
    }
}
impl PushdownExecutor for KuzuTckStore {
    fn binding(&self) -> &ReplicaBinding {
        PushdownExecutor::binding(&self.store)
    }
    fn capabilities(&self) -> &CapabilityManifest {
        self.store.capabilities()
    }
    fn execute_pushdown(&self, request: PushdownRequest) -> StoreFuture<'_, PushdownOutcome> {
        self.store.execute_pushdown(request)
    }
}

fn capabilities() -> Result<CapabilityManifest, StorageError> {
    CapabilityManifest::from_names([
        "adjacency",
        "immutable-read-view",
        "logical-snapshot",
        "point",
    ])
}

fn validate_binding(binding: &ReplicaBinding) -> Result<(), StorageError> {
    let capabilities = capabilities()?;
    let class = BackendClass::new(
        ProviderKind::Kuzu,
        KUZU_CONTRACT_VERSION,
        KUZU_LAYOUT_VERSION,
        capabilities.names().map(str::to_owned),
    )?;
    if binding.provider_kind() != &ProviderKind::Kuzu
        || binding.contract_version() != KUZU_CONTRACT_VERSION
        || binding.layout_version() != KUZU_LAYOUT_VERSION
        || binding.capability_digest() != capabilities.digest()
        || binding.backend_class_digest() != class.digest()
    {
        return Err(StorageError::InvalidBinding(
            "binding does not match the native Kuzu backend class".into(),
        ));
    }
    Ok(())
}

fn initialize_namespace(database: &Database, binding: &ReplicaBinding) -> Result<(), StorageError> {
    let connection = Connection::new(database).map_err(kuzu_error)?;
    connection
        .query(
            "CREATE NODE TABLE IF NOT EXISTS DtgOwner(id STRING, payload BLOB, PRIMARY KEY(id));",
        )
        .map_err(kuzu_error)?;
    connection
        .query(
            "CREATE NODE TABLE IF NOT EXISTS DtgState(id STRING, payload BLOB, PRIMARY KEY(id));",
        )
        .map_err(kuzu_error)?;
    let mut owners = connection
        .query("MATCH (owner:DtgOwner {id: 'owner'}) RETURN owner.payload;")
        .map_err(kuzu_error)?;
    if let Some(row) = owners.next() {
        let KuzuValue::Blob(bytes) = &row[0] else {
            return Err(StorageError::Internal(
                "Kuzu owner payload is not a BLOB".into(),
            ));
        };
        let actual = decode_binding(bytes)?;
        if &actual != binding {
            return Err(StorageError::NamespaceOwnerMismatch {
                expected: Box::new(actual),
                actual: Box::new(binding.clone()),
            });
        }
    } else {
        let mut statement = connection
            .prepare("CREATE (:DtgOwner {id: 'owner', payload: $payload});")
            .map_err(kuzu_error)?;
        connection
            .execute(
                &mut statement,
                vec![("payload", KuzuValue::Blob(encode_binding(binding)?))],
            )
            .map_err(kuzu_error)?;
    }
    Ok(())
}

fn load_state(database: &Database) -> Result<State, StorageError> {
    let connection = Connection::new(database).map_err(kuzu_error)?;
    let mut rows = connection
        .query("MATCH (state:DtgState {id: 'state'}) RETURN state.payload;")
        .map_err(kuzu_error)?;
    rows.next().map_or_else(
        || Ok(State::empty()),
        |row| match &row[0] {
            KuzuValue::Blob(bytes) => decode_state(bytes),
            _ => Err(StorageError::Internal(
                "Kuzu state payload is not a BLOB".into(),
            )),
        },
    )
}

fn persist_state(database: &Database, state: &State) -> Result<(), StorageError> {
    let connection = Connection::new(database).map_err(kuzu_error)?;
    connection.query("BEGIN TRANSACTION;").map_err(kuzu_error)?;
    let result = (|| {
        let mut statement = connection
            .prepare("MERGE (state:DtgState {id: 'state'}) SET state.payload = $payload;")
            .map_err(kuzu_error)?;
        connection
            .execute(
                &mut statement,
                vec![("payload", KuzuValue::Blob(encode_state(state)?))],
            )
            .map_err(kuzu_error)?;
        connection.query("COMMIT;").map_err(kuzu_error)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = connection.query("ROLLBACK;");
    }
    result
}

fn lock<'a, T>(
    mutex: &'a Mutex<T>,
    name: &str,
) -> Result<std::sync::MutexGuard<'a, T>, StorageError> {
    mutex
        .lock()
        .map_err(|_| StorageError::Internal(format!("{name} lock is poisoned")))
}

fn kuzu_error(error: kuzu::Error) -> StorageError {
    StorageError::Internal(format!("Kuzu error: {error}"))
}

fn visible_vertex(history: &[LogicalMutation], request: &VertexRead) -> Option<VertexVersion> {
    let mut candidate = None;
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
            && candidate.as_ref().is_none_or(
                |current: &(TransactionTime, Version, Option<VertexVersion>)| {
                    (event.0, event.1) > (current.0, current.1)
                },
            )
        {
            candidate = Some(event);
        }
    }
    candidate.and_then(|(_, _, vertex)| vertex)
}

fn visible_edge(history: &[LogicalMutation], request: &EdgeRead) -> Option<EdgeVersion> {
    let mut candidate = None;
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
            && candidate.as_ref().is_none_or(
                |current: &(TransactionTime, Version, Option<EdgeVersion>)| {
                    (event.0, event.1) > (current.0, current.1)
                },
            )
        {
            candidate = Some(event);
        }
    }
    candidate.and_then(|(_, _, edge)| edge)
}

fn vertex_ids(history: &[LogicalMutation]) -> BTreeSet<VertexId> {
    history
        .iter()
        .filter_map(|mutation| match mutation {
            LogicalMutation::PutVertex(vertex) => Some(vertex.id()),
            LogicalMutation::DeleteVertex(tombstone) => Some(tombstone.id()),
            _ => None,
        })
        .collect()
}
fn edge_ids(history: &[LogicalMutation]) -> BTreeSet<EdgeId> {
    history
        .iter()
        .filter_map(|mutation| match mutation {
            LogicalMutation::PutEdge(edge) => Some(edge.id()),
            LogicalMutation::DeleteEdge(tombstone) => Some(tombstone.id()),
            _ => None,
        })
        .collect()
}

fn snapshot_chunks(
    header: &SnapshotHeader,
    state: &State,
    max_records: u32,
) -> Result<Vec<SnapshotChunk>, StorageError> {
    let mut records = state
        .history
        .iter()
        .cloned()
        .map(record_from_mutation)
        .collect::<Vec<_>>();
    records.extend(
        state
            .replay
            .iter()
            .map(|(index, replay)| {
                SnapshotReplayRecord::new(*index, replay.term, replay.command_id, replay.digest)
                    .map(dtg_storage::SnapshotRecord::Replay)
            })
            .collect::<Result<Vec<_>, _>>()?,
    );
    records.extend(state.changes.iter().map(|(cursor, mutation)| {
        dtg_storage::SnapshotRecord::Change(ChangeRecord::new(*cursor, mutation.clone()))
    }));
    records
        .chunks(max_records as usize)
        .enumerate()
        .map(|(ordinal, chunk)| {
            SnapshotChunk::new(header.snapshot_id(), ordinal as u64, chunk.to_vec())
        })
        .collect()
}

fn record_from_mutation(mutation: LogicalMutation) -> dtg_storage::SnapshotRecord {
    match mutation {
        LogicalMutation::PutVertex(vertex) => dtg_storage::SnapshotRecord::Vertex(vertex),
        LogicalMutation::DeleteVertex(tombstone) => {
            dtg_storage::SnapshotRecord::VertexTombstone(tombstone)
        }
        LogicalMutation::PutEdge(edge) => dtg_storage::SnapshotRecord::Edge(edge),
        LogicalMutation::DeleteEdge(tombstone) => {
            dtg_storage::SnapshotRecord::EdgeTombstone(tombstone)
        }
        LogicalMutation::PutTransaction(transaction) => {
            dtg_storage::SnapshotRecord::Transaction(transaction)
        }
        LogicalMutation::PutReplicaMetadata(metadata) => {
            dtg_storage::SnapshotRecord::ReplicaMetadata(metadata)
        }
    }
}

fn state_from_snapshot(
    chunks: &[SnapshotChunk],
    applied_index: u64,
) -> Result<State, StorageError> {
    let mut state = State {
        applied_index,
        ..State::empty()
    };
    for record in chunks.iter().flat_map(|chunk| chunk.records().iter()) {
        match record {
            dtg_storage::SnapshotRecord::Vertex(vertex) => state
                .history
                .push(LogicalMutation::PutVertex(vertex.clone())),
            dtg_storage::SnapshotRecord::VertexTombstone(tombstone) => state
                .history
                .push(LogicalMutation::DeleteVertex(tombstone.clone())),
            dtg_storage::SnapshotRecord::Edge(edge) => {
                state.history.push(LogicalMutation::PutEdge(edge.clone()))
            }
            dtg_storage::SnapshotRecord::EdgeTombstone(tombstone) => state
                .history
                .push(LogicalMutation::DeleteEdge(tombstone.clone())),
            dtg_storage::SnapshotRecord::Transaction(transaction) => state
                .history
                .push(LogicalMutation::PutTransaction(transaction.clone())),
            dtg_storage::SnapshotRecord::ReplicaMetadata(metadata) => state
                .history
                .push(LogicalMutation::PutReplicaMetadata(metadata.clone())),
            dtg_storage::SnapshotRecord::Replay(replay) => {
                state.replay.insert(
                    replay.raft_index(),
                    Replay {
                        term: replay.raft_term(),
                        command_id: replay.command_id(),
                        digest: replay.mutation_digest(),
                    },
                );
            }
            dtg_storage::SnapshotRecord::Change(change) => state
                .changes
                .push((change.cursor(), change.mutation().clone())),
        }
    }
    Ok(state)
}

fn same_logical_identity(left: &ReplicaBinding, right: &ReplicaBinding) -> bool {
    left.cluster_id() == right.cluster_id()
        && left.graph_id() == right.graph_id()
        && left.shard_id() == right.shard_id()
}

#[derive(Encode, Decode)]
struct WireBinding {
    cluster: u64,
    graph: u64,
    shard: u64,
    epoch: u64,
    replica: u64,
    generation: u64,
    class: [u8; 32],
    contract: u32,
    layout: u32,
    capability: [u8; 32],
    namespace: String,
    endpoint: String,
    credential: String,
    role: u8,
}
#[derive(Encode, Decode)]
struct WireState {
    applied_index: u64,
    history: Vec<WireMutation>,
    replay: Vec<(u64, u64, u128, [u8; 32])>,
    changes: Vec<(u64, u64, WireMutation)>,
}
#[derive(Encode, Decode)]
enum WireMutation {
    Vertex(WireVertex),
    VertexTombstone(u128, u64, i64),
    Edge(WireEdge),
    EdgeTombstone(u128, u64, i64),
    Transaction(u128, u8, i64, [u8; 32]),
    Metadata(String, WireValue),
}
#[derive(Encode, Decode)]
struct WireVertex {
    id: u128,
    version: u64,
    valid_from: i64,
    valid_to: i64,
    transaction_time: i64,
    properties: BTreeMap<String, WireValue>,
}
#[derive(Encode, Decode)]
struct WireEdge {
    id: u128,
    source: u128,
    target: u128,
    edge_type: String,
    version: u64,
    valid_from: i64,
    valid_to: i64,
    transaction_time: i64,
    properties: BTreeMap<String, WireValue>,
}
#[derive(Encode, Decode)]
enum WireValue {
    Null,
    Boolean(bool),
    Integer(i64),
    FloatBits(u64),
    Bytes(Vec<u8>),
    String(String),
    List(Vec<WireValue>),
    Map(BTreeMap<String, WireValue>),
}

fn encode_binding(binding: &ReplicaBinding) -> Result<Vec<u8>, StorageError> {
    bincode::encode_to_vec(
        WireBinding {
            cluster: binding.cluster_id().get(),
            graph: binding.graph_id().get(),
            shard: binding.shard_id().get(),
            epoch: binding.placement_epoch().get(),
            replica: binding.replica_id().get(),
            generation: binding.backend_generation().get(),
            class: binding.backend_class_digest().get(),
            contract: binding.contract_version(),
            layout: binding.layout_version(),
            capability: binding.capability_digest().get(),
            namespace: binding.namespace_id().as_str().into(),
            endpoint: binding.endpoint_profile_ref().into(),
            credential: binding.credential_ref().into(),
            role: match binding.role() {
                BindingRole::Candidate => 1,
                BindingRole::Active => 2,
                BindingRole::Retiring => 3,
            },
        },
        bincode::config::standard(),
    )
    .map_err(codec_error)
}
fn decode_binding(bytes: &[u8]) -> Result<ReplicaBinding, StorageError> {
    let (wire, used): (WireBinding, usize) =
        bincode::decode_from_slice(bytes, bincode::config::standard()).map_err(codec_error)?;
    if used != bytes.len() {
        return Err(StorageError::Internal("trailing Kuzu owner bytes".into()));
    }
    ReplicaBinding::builder()
        .cluster_id(wire.cluster)
        .graph_id(wire.graph)
        .shard_id(wire.shard)
        .placement_epoch(wire.epoch)
        .replica_id(wire.replica)
        .backend_generation(wire.generation)
        .backend_class_digest(Digest32::new(wire.class))
        .provider_kind(ProviderKind::Kuzu)
        .contract_version(wire.contract)
        .layout_version(wire.layout)
        .capability_digest(Digest32::new(wire.capability))
        .namespace_id(wire.namespace)
        .endpoint_profile_ref(wire.endpoint)
        .credential_ref(wire.credential)
        .role(match wire.role {
            1 => BindingRole::Candidate,
            2 => BindingRole::Active,
            3 => BindingRole::Retiring,
            _ => return Err(StorageError::Internal("invalid Kuzu owner role".into())),
        })
        .build()
}
fn encode_state(state: &State) -> Result<Vec<u8>, StorageError> {
    bincode::encode_to_vec(
        WireState {
            applied_index: state.applied_index,
            history: state
                .history
                .iter()
                .map(wire_mutation)
                .collect::<Result<_, _>>()?,
            replay: state
                .replay
                .iter()
                .map(|(index, replay)| {
                    (
                        *index,
                        replay.term,
                        replay.command_id.get(),
                        replay.digest.get(),
                    )
                })
                .collect(),
            changes: state
                .changes
                .iter()
                .map(|(cursor, mutation)| {
                    Ok((
                        cursor.raft_index(),
                        cursor.mutation_ordinal(),
                        wire_mutation(mutation)?,
                    ))
                })
                .collect::<Result<_, StorageError>>()?,
        },
        bincode::config::standard(),
    )
    .map_err(codec_error)
}
fn decode_state(bytes: &[u8]) -> Result<State, StorageError> {
    let (wire, used): (WireState, usize) =
        bincode::decode_from_slice(bytes, bincode::config::standard()).map_err(codec_error)?;
    if used != bytes.len() {
        return Err(StorageError::Internal("trailing Kuzu state bytes".into()));
    }
    Ok(State {
        applied_index: wire.applied_index,
        history: wire
            .history
            .iter()
            .map(read_mutation)
            .collect::<Result<_, _>>()?,
        replay: wire
            .replay
            .into_iter()
            .map(|(index, term, command, digest)| {
                Ok((
                    index,
                    Replay {
                        term,
                        command_id: dtg_storage::CommandId::new(command)?,
                        digest: Digest32::new(digest),
                    },
                ))
            })
            .collect::<Result<_, StorageError>>()?,
        changes: wire
            .changes
            .iter()
            .map(|(index, ordinal, mutation)| {
                Ok((
                    ChangeCursor::new(*index, *ordinal),
                    read_mutation(mutation)?,
                ))
            })
            .collect::<Result<_, StorageError>>()?,
    })
}
fn wire_mutation(mutation: &LogicalMutation) -> Result<WireMutation, StorageError> {
    Ok(match mutation {
        LogicalMutation::PutVertex(vertex) => WireMutation::Vertex(WireVertex {
            id: vertex.id().get(),
            version: vertex.version().get(),
            valid_from: vertex.valid_time().start(),
            valid_to: vertex.valid_time().end(),
            transaction_time: vertex.transaction_time().get(),
            properties: wire_properties(vertex.properties())?,
        }),
        LogicalMutation::DeleteVertex(tombstone) => WireMutation::VertexTombstone(
            tombstone.id().get(),
            tombstone.version().get(),
            tombstone.transaction_time().get(),
        ),
        LogicalMutation::PutEdge(edge) => WireMutation::Edge(WireEdge {
            id: edge.id().get(),
            source: edge.source().get(),
            target: edge.target().get(),
            edge_type: edge.edge_type().into(),
            version: edge.version().get(),
            valid_from: edge.valid_time().start(),
            valid_to: edge.valid_time().end(),
            transaction_time: edge.transaction_time().get(),
            properties: wire_properties(edge.properties())?,
        }),
        LogicalMutation::DeleteEdge(tombstone) => WireMutation::EdgeTombstone(
            tombstone.id().get(),
            tombstone.version().get(),
            tombstone.transaction_time().get(),
        ),
        LogicalMutation::PutTransaction(transaction) => WireMutation::Transaction(
            transaction.id().get(),
            match transaction.state() {
                TransactionState::Prepared => 1,
                TransactionState::Committed => 2,
                TransactionState::Aborted => 3,
            },
            transaction.transaction_time().get(),
            transaction.record_digest().get(),
        ),
        LogicalMutation::PutReplicaMetadata(metadata) => {
            WireMutation::Metadata(metadata.name().into(), wire_value(metadata.value())?)
        }
    })
}
fn read_mutation(wire: &WireMutation) -> Result<LogicalMutation, StorageError> {
    Ok(match wire {
        WireMutation::Vertex(vertex) => LogicalMutation::PutVertex(VertexVersion::new(
            VertexId::new(vertex.id)?,
            Version::new(vertex.version),
            ValidInterval::new(vertex.valid_from, vertex.valid_to).map_err(kernel_error)?,
            TransactionTime::new(vertex.transaction_time).map_err(kernel_error)?,
            read_properties(&vertex.properties)?,
        )?),
        WireMutation::VertexTombstone(id, version, time) => {
            LogicalMutation::DeleteVertex(VertexTombstone::new(
                VertexId::new(*id)?,
                Version::new(*version),
                TransactionTime::new(*time).map_err(kernel_error)?,
            ))
        }
        WireMutation::Edge(edge) => LogicalMutation::PutEdge(EdgeVersion::new(
            EdgeId::new(edge.id)?,
            VertexId::new(edge.source)?,
            VertexId::new(edge.target)?,
            &edge.edge_type,
            Version::new(edge.version),
            ValidInterval::new(edge.valid_from, edge.valid_to).map_err(kernel_error)?,
            TransactionTime::new(edge.transaction_time).map_err(kernel_error)?,
            read_properties(&edge.properties)?,
        )?),
        WireMutation::EdgeTombstone(id, version, time) => {
            LogicalMutation::DeleteEdge(EdgeTombstone::new(
                EdgeId::new(*id)?,
                Version::new(*version),
                TransactionTime::new(*time).map_err(kernel_error)?,
            ))
        }
        WireMutation::Transaction(id, state, time, digest) => {
            LogicalMutation::PutTransaction(TransactionRecord::new(
                TransactionId::new(*id).map_err(kernel_error)?,
                match state {
                    1 => TransactionState::Prepared,
                    2 => TransactionState::Committed,
                    3 => TransactionState::Aborted,
                    _ => {
                        return Err(StorageError::Internal(
                            "invalid Kuzu transaction state".into(),
                        ));
                    }
                },
                TransactionTime::new(*time).map_err(kernel_error)?,
                Digest32::new(*digest),
            )?)
        }
        WireMutation::Metadata(name, value) => {
            LogicalMutation::PutReplicaMetadata(ReplicaMetadata::new(name, read_value(value)?)?)
        }
    })
}
fn wire_properties(properties: &Properties) -> Result<BTreeMap<String, WireValue>, StorageError> {
    properties
        .iter()
        .map(|(name, value)| Ok((name.clone(), wire_value(value)?)))
        .collect()
}
fn read_properties(properties: &BTreeMap<String, WireValue>) -> Result<Properties, StorageError> {
    properties
        .iter()
        .map(|(name, value)| Ok((name.clone(), read_value(value)?)))
        .collect()
}
fn wire_value(value: &Value) -> Result<WireValue, StorageError> {
    Ok(match value {
        Value::Null => WireValue::Null,
        Value::Boolean(value) => WireValue::Boolean(*value),
        Value::Integer(value) => WireValue::Integer(*value),
        Value::FloatBits(value) => WireValue::FloatBits(*value),
        Value::Bytes(value) => WireValue::Bytes(value.clone()),
        Value::String(value) => WireValue::String(value.clone()),
        Value::List(values) => {
            WireValue::List(values.iter().map(wire_value).collect::<Result<_, _>>()?)
        }
        Value::Map(values) => WireValue::Map(
            values
                .iter()
                .map(|(name, value)| Ok((name.clone(), wire_value(value)?)))
                .collect::<Result<_, StorageError>>()?,
        ),
    })
}
fn read_value(value: &WireValue) -> Result<Value, StorageError> {
    Ok(match value {
        WireValue::Null => Value::Null,
        WireValue::Boolean(value) => Value::Boolean(*value),
        WireValue::Integer(value) => Value::Integer(*value),
        WireValue::FloatBits(value) => Value::FloatBits(*value),
        WireValue::Bytes(value) => Value::Bytes(value.clone()),
        WireValue::String(value) => Value::String(value.clone()),
        WireValue::List(values) => {
            Value::List(values.iter().map(read_value).collect::<Result<_, _>>()?)
        }
        WireValue::Map(values) => Value::Map(
            values
                .iter()
                .map(|(name, value)| Ok((name.clone(), read_value(value)?)))
                .collect::<Result<_, StorageError>>()?,
        ),
    })
}
fn codec_error(error: impl fmt::Display) -> StorageError {
    StorageError::Internal(format!("Kuzu codec error: {error}"))
}
fn kernel_error(error: dtg_kernel::KernelError) -> StorageError {
    StorageError::Internal(format!("invalid Kuzu typed value: {error}"))
}

#[allow(dead_code)]
fn _tck_signature(factory: &dyn StorageTckFactory) -> StoreFuture<'_, ()> {
    run_storage_tck(factory)
}
