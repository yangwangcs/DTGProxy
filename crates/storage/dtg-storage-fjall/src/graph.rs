use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    path::Path,
    sync::{Arc, MutexGuard},
};

#[cfg(feature = "tck")]
use std::sync::{Condvar, Mutex};

use dtg_storage::{
    ApplyReceipt, CapabilityManifest, ChangeRecord, CommittedShardBatch, EdgeId, EdgeTombstone,
    EdgeVersion, LogicalMutation, ReadFence, ReplicaBinding, ReplicaMetadata, ReplicaStateStore,
    SnapshotRecord, SnapshotReplayRecord, StorageError, StoreFuture, TemporalReadView,
    TransactionId, TransactionRecord, VertexTombstone, VertexVersion,
};
use fjall::{Keyspace, OwnedWriteBatch, PersistMode};

use crate::{
    codec::{decode_mutation, decode_replay_identity, encode_mutation, encode_replay_identity},
    namespace::{NamespaceDb, fjall_capabilities, fjall_error},
    read_view::FjallReadView,
};

pub(crate) const APPLIED_INDEX_KEY: &[u8] = b"system/applied_index";

struct ReplicaInner {
    namespace: NamespaceDb,
    binding: ReplicaBinding,
    capabilities: CapabilityManifest,
    #[cfg(feature = "tck")]
    injected_failure_after: Mutex<Option<usize>>,
    #[cfg(feature = "tck")]
    graph_pauses: Mutex<BTreeMap<GraphPausePoint, Arc<GraphPauseState>>>,
}

#[cfg(feature = "tck")]
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum GraphPausePoint {
    ApplyBeforeCommit,
    RestoreBeforeCommit,
    SnapshotAfterFence,
}

#[cfg(feature = "tck")]
#[derive(Default)]
struct GraphPauseStatus {
    reached: bool,
    released: bool,
}

#[cfg(feature = "tck")]
struct GraphPauseState {
    status: Mutex<GraphPauseStatus>,
    changed: Condvar,
}

#[cfg(feature = "tck")]
impl GraphPauseState {
    fn new() -> Self {
        Self {
            status: Mutex::new(GraphPauseStatus::default()),
            changed: Condvar::new(),
        }
    }

    fn pause(&self) -> Result<(), StorageError> {
        let mut status = lock(&self.status)?;
        status.reached = true;
        self.changed.notify_all();
        while !status.released {
            status = self
                .changed
                .wait(status)
                .map_err(|_| StorageError::Internal("Fjall graph pause is poisoned".into()))?;
        }
        Ok(())
    }
}

#[cfg(feature = "tck")]
pub struct FjallGraphPause {
    state: Arc<GraphPauseState>,
}

#[cfg(feature = "tck")]
impl FjallGraphPause {
    pub fn wait_until_reached(&self) -> Result<(), StorageError> {
        let mut status = lock(&self.state.status)?;
        while !status.reached {
            status = self
                .state
                .changed
                .wait(status)
                .map_err(|_| StorageError::Internal("Fjall graph pause is poisoned".into()))?;
        }
        Ok(())
    }

    pub fn release(&self) -> Result<(), StorageError> {
        let mut status = lock(&self.state.status)?;
        status.released = true;
        self.state.changed.notify_all();
        Ok(())
    }
}

#[cfg(feature = "tck")]
impl Drop for FjallGraphPause {
    fn drop(&mut self) {
        if let Ok(mut status) = self.state.status.lock() {
            status.released = true;
            self.state.changed.notify_all();
        }
    }
}

#[derive(Clone)]
pub struct FjallReplicaStore {
    inner: Arc<ReplicaInner>,
}

impl fmt::Debug for FjallReplicaStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FjallReplicaStore")
            .field("binding", &self.inner.binding)
            .finish_non_exhaustive()
    }
}

impl FjallReplicaStore {
    pub fn open(path: impl AsRef<Path>, binding: ReplicaBinding) -> Result<Self, StorageError> {
        let capabilities = fjall_capabilities()?;
        let namespace = NamespaceDb::open(path.as_ref(), &binding)?;
        Ok(Self {
            inner: Arc::new(ReplicaInner {
                namespace,
                binding,
                capabilities,
                #[cfg(feature = "tck")]
                injected_failure_after: Mutex::new(None),
                #[cfg(feature = "tck")]
                graph_pauses: Mutex::new(BTreeMap::new()),
            }),
        })
    }

    pub(crate) fn capabilities(&self) -> &CapabilityManifest {
        &self.inner.capabilities
    }

    pub(crate) fn namespace(&self) -> &NamespaceDb {
        &self.inner.namespace
    }

    pub(crate) fn verify_binding(&self, binding: &ReplicaBinding) -> Result<(), StorageError> {
        if binding == &self.inner.binding {
            Ok(())
        } else {
            Err(StorageError::StaleBinding {
                expected: Box::new(self.inner.binding.clone()),
                actual: Box::new(binding.clone()),
            })
        }
    }

    pub(crate) fn verify_fence(&self, fence: &ReadFence) -> Result<(), StorageError> {
        self.verify_binding(fence.binding())?;
        if fence.capability_digest() != self.inner.binding.capability_digest() {
            return Err(StorageError::CapabilityDrift);
        }
        let applied = self.applied_index_sync()?;
        if fence.applied_index() != applied {
            return Err(StorageError::ReadFenceUnavailable {
                requested: fence.applied_index(),
                applied,
            });
        }
        Ok(())
    }

    #[cfg(feature = "tck")]
    pub(crate) fn arm_apply_failure_after(
        &self,
        staged_mutations: usize,
    ) -> Result<(), StorageError> {
        *lock(&self.inner.injected_failure_after)? = Some(staged_mutations);
        Ok(())
    }

    pub(crate) fn applied_index_sync(&self) -> Result<u64, StorageError> {
        self.inner
            .namespace
            .replica_meta
            .get(APPLIED_INDEX_KEY)
            .map_err(fjall_error)?
            .map(|bytes| decode_u64(&bytes))
            .transpose()
            .map(Option::unwrap_or_default)
    }

    pub(crate) fn lock_graph(&self) -> Result<MutexGuard<'_, ()>, StorageError> {
        self.inner.namespace.graph_guard.lock()
    }

    #[cfg(feature = "tck")]
    pub fn arm_tck_apply_before_commit_pause(&self) -> Result<FjallGraphPause, StorageError> {
        self.arm_graph_pause(GraphPausePoint::ApplyBeforeCommit)
    }

    #[cfg(feature = "tck")]
    pub fn arm_tck_restore_before_commit_pause(&self) -> Result<FjallGraphPause, StorageError> {
        self.arm_graph_pause(GraphPausePoint::RestoreBeforeCommit)
    }

    #[cfg(feature = "tck")]
    pub fn arm_tck_snapshot_after_fence_pause(&self) -> Result<FjallGraphPause, StorageError> {
        self.arm_graph_pause(GraphPausePoint::SnapshotAfterFence)
    }

    #[cfg(feature = "tck")]
    pub fn wait_for_tck_graph_waiter(&self) -> Result<(), StorageError> {
        self.inner.namespace.graph_guard.wait_for_waiter()
    }

    #[cfg(feature = "tck")]
    fn arm_graph_pause(&self, point: GraphPausePoint) -> Result<FjallGraphPause, StorageError> {
        let state = Arc::new(GraphPauseState::new());
        let mut pauses = lock(&self.inner.graph_pauses)?;
        match pauses.entry(point) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(Arc::clone(&state));
            }
            std::collections::btree_map::Entry::Occupied(_) => {
                return Err(StorageError::Internal(
                    "Fjall graph pause point is already armed".into(),
                ));
            }
        }
        Ok(FjallGraphPause { state })
    }

    #[cfg(feature = "tck")]
    pub(crate) fn pause_tck_snapshot_after_fence(&self) -> Result<(), StorageError> {
        self.pause_graph_at(GraphPausePoint::SnapshotAfterFence)
    }

    #[cfg(feature = "tck")]
    fn pause_graph_at(&self, point: GraphPausePoint) -> Result<(), StorageError> {
        let pause = lock(&self.inner.graph_pauses)?.remove(&point);
        match pause {
            Some(pause) => pause.pause(),
            None => Ok(()),
        }
    }

    fn apply_sync(&self, batch: CommittedShardBatch) -> Result<ApplyReceipt, StorageError> {
        batch.validate()?;
        self.verify_binding(batch.binding())?;
        let _guard = self.lock_graph()?;
        let applied = self.applied_index_sync()?;
        if batch.raft_index() <= applied {
            let replay = self
                .inner
                .namespace
                .identity
                .get(batch.raft_index().to_be_bytes())
                .map_err(fjall_error)?
                .ok_or(StorageError::ReplayMismatch {
                    raft_index: batch.raft_index(),
                })?;
            let replay = decode_replay_identity(&replay)?;
            if replay.term == batch.raft_term()
                && replay.command_id == batch.command_id()
                && replay.mutation_digest == batch.mutation_digest()
            {
                return Ok(ApplyReceipt::new(&batch, true));
            }
            return Err(StorageError::ReplayMismatch {
                raft_index: batch.raft_index(),
            });
        }
        if batch.raft_index() != applied.saturating_add(1) {
            return Err(StorageError::NonMonotonicIndex {
                applied,
                proposed: batch.raft_index(),
            });
        }

        #[cfg(feature = "tck")]
        let failure_after = lock(&self.inner.injected_failure_after)?.take();
        let mut write = self
            .inner
            .namespace
            .db
            .batch()
            .durability(Some(PersistMode::SyncAll));
        let mut adjacency = EdgeAdjacencyState::default();
        for (ordinal, mutation) in batch.mutations().iter().enumerate() {
            stage_mutation(
                &self.inner.namespace,
                &mut write,
                batch.raft_index(),
                ordinal as u64,
                mutation,
                &mut adjacency,
            )?;
            #[cfg(feature = "tck")]
            if failure_after == Some(ordinal + 1) {
                return Err(StorageError::InjectedApplyFailure {
                    staged_mutations: ordinal + 1,
                });
            }
        }
        write.insert(
            &self.inner.namespace.identity,
            batch.raft_index().to_be_bytes(),
            encode_replay_identity(
                batch.raft_term(),
                batch.command_id(),
                batch.mutation_digest(),
            )?,
        );
        write.insert(
            &self.inner.namespace.replica_meta,
            APPLIED_INDEX_KEY,
            batch.raft_index().to_be_bytes(),
        );
        #[cfg(feature = "tck")]
        self.pause_graph_at(GraphPausePoint::ApplyBeforeCommit)?;
        write.commit().map_err(fjall_error)?;
        Ok(ApplyReceipt::new(&batch, false))
    }

    pub(crate) fn restore_records(
        &self,
        records: &[SnapshotRecord],
        applied_index: u64,
        stage_keys: &[Vec<u8>],
    ) -> Result<(), StorageError> {
        let _guard = self.lock_graph()?;
        let mut history = Vec::new();
        let mut transactions = Vec::new();
        let mut metadata = Vec::new();
        let mut replay = Vec::new();
        let mut changes = Vec::new();
        for record in records {
            match record {
                SnapshotRecord::Vertex(vertex) => {
                    history.push(LogicalMutation::PutVertex(vertex.clone()));
                }
                SnapshotRecord::VertexTombstone(tombstone) => {
                    history.push(LogicalMutation::DeleteVertex(tombstone.clone()));
                }
                SnapshotRecord::Edge(edge) => {
                    history.push(LogicalMutation::PutEdge(edge.clone()));
                }
                SnapshotRecord::EdgeTombstone(tombstone) => {
                    history.push(LogicalMutation::DeleteEdge(tombstone.clone()));
                }
                SnapshotRecord::Transaction(transaction) => {
                    transactions.push(transaction.clone());
                }
                SnapshotRecord::ReplicaMetadata(record) => metadata.push(record.clone()),
                SnapshotRecord::Replay(record) => replay.push(record.clone()),
                SnapshotRecord::Change(record) => changes.push(record.clone()),
            }
        }
        changes.sort_by_key(ChangeRecord::cursor);
        let authenticated_current = validate_restored_state(
            &self.inner.binding,
            applied_index,
            &history,
            &transactions,
            &metadata,
            &replay,
            &changes,
        )?;

        let mut write = self
            .inner
            .namespace
            .db
            .batch()
            .durability(Some(PersistMode::SyncAll));
        clear_logical_state(&self.inner.namespace, &mut write)?;
        for change in &changes {
            if is_graph_mutation(change.mutation()) {
                stage_history_mutation(
                    &self.inner.namespace,
                    &mut write,
                    change.raft_index(),
                    change.mutation_ordinal(),
                    change.mutation(),
                )?;
            }
        }
        for transaction in authenticated_current.transactions.values() {
            let mutation = LogicalMutation::PutTransaction(transaction.clone());
            stage_current_mutation(
                &self.inner.namespace,
                &mut write,
                &mutation,
                &mut EdgeAdjacencyState::default(),
            )?;
        }
        for record in authenticated_current.metadata.values() {
            let mutation = LogicalMutation::PutReplicaMetadata(record.clone());
            stage_current_mutation(
                &self.inner.namespace,
                &mut write,
                &mutation,
                &mut EdgeAdjacencyState::default(),
            )?;
        }
        for record in &replay {
            write.insert(
                &self.inner.namespace.identity,
                record.raft_index().to_be_bytes(),
                encode_replay_identity(
                    record.raft_term(),
                    record.command_id(),
                    record.mutation_digest(),
                )?,
            );
        }
        let mut adjacency = EdgeAdjacencyState::default();
        for change in &changes {
            write.insert(
                &self.inner.namespace.temporal_index,
                change_key(change.raft_index(), change.mutation_ordinal()),
                encode_mutation(change.mutation())?,
            );
            if is_graph_mutation(change.mutation()) {
                stage_current_mutation(
                    &self.inner.namespace,
                    &mut write,
                    change.mutation(),
                    &mut adjacency,
                )?;
            }
        }
        for key in stage_keys {
            write.remove(&self.inner.namespace.snapshot_stage, key);
        }
        write.insert(
            &self.inner.namespace.replica_meta,
            APPLIED_INDEX_KEY,
            applied_index.to_be_bytes(),
        );
        #[cfg(feature = "tck")]
        self.pause_graph_at(GraphPausePoint::RestoreBeforeCommit)?;
        write.commit().map_err(fjall_error)
    }
}

impl ReplicaStateStore for FjallReplicaStore {
    fn binding(&self) -> &ReplicaBinding {
        &self.inner.binding
    }

    fn applied_index(&self) -> StoreFuture<'_, u64> {
        Box::pin(async move { self.applied_index_sync() })
    }

    fn replica_metadata<'a>(&'a self, name: &'a str) -> StoreFuture<'a, Option<ReplicaMetadata>> {
        Box::pin(async move {
            let _guard = self.lock_graph()?;
            let mut key = b"user/".to_vec();
            key.extend_from_slice(name.as_bytes());
            let Some(bytes) = self
                .inner
                .namespace
                .replica_meta
                .get(key)
                .map_err(fjall_error)?
            else {
                return Ok(None);
            };
            match decode_mutation(&bytes)? {
                LogicalMutation::PutReplicaMetadata(metadata) if metadata.name() == name => {
                    Ok(Some(metadata))
                }
                _ => Err(StorageError::Internal(
                    "stored replica metadata point value is malformed".into(),
                )),
            }
        })
    }

    fn apply(&self, batch: CommittedShardBatch) -> StoreFuture<'_, ApplyReceipt> {
        Box::pin(async move { self.apply_sync(batch) })
    }

    fn begin_read_view(&self, fence: ReadFence) -> StoreFuture<'_, Box<dyn TemporalReadView>> {
        Box::pin(async move {
            let _guard = self.lock_graph()?;
            self.verify_fence(&fence)?;
            Ok(Box::new(FjallReadView::load(self, fence)?) as Box<dyn TemporalReadView>)
        })
    }
}

pub(crate) fn stage_mutation(
    namespace: &NamespaceDb,
    write: &mut OwnedWriteBatch,
    raft_index: u64,
    ordinal: u64,
    mutation: &LogicalMutation,
    adjacency: &mut EdgeAdjacencyState,
) -> Result<(), StorageError> {
    let value = encode_mutation(mutation)?;
    stage_history_mutation(namespace, write, raft_index, ordinal, mutation)?;
    stage_current_mutation(namespace, write, mutation, adjacency)?;
    write.insert(
        &namespace.temporal_index,
        change_key(raft_index, ordinal),
        value,
    );
    Ok(())
}

fn stage_history_mutation(
    namespace: &NamespaceDb,
    write: &mut OwnedWriteBatch,
    raft_index: u64,
    ordinal: u64,
    mutation: &LogicalMutation,
) -> Result<(), StorageError> {
    let value = encode_mutation(mutation)?;
    match mutation {
        LogicalMutation::PutVertex(vertex) => {
            write.insert(
                &namespace.history,
                history_occurrence_key(vertex_history_key(vertex, false), raft_index, ordinal),
                value.clone(),
            );
        }
        LogicalMutation::DeleteVertex(tombstone) => {
            write.insert(
                &namespace.history,
                history_occurrence_key(vertex_tombstone_key(tombstone), raft_index, ordinal),
                value.clone(),
            );
        }
        LogicalMutation::PutEdge(edge) => {
            write.insert(
                &namespace.history,
                history_occurrence_key(edge_history_key(edge, false), raft_index, ordinal),
                value.clone(),
            );
        }
        LogicalMutation::DeleteEdge(tombstone) => {
            write.insert(
                &namespace.history,
                history_occurrence_key(edge_tombstone_key(tombstone), raft_index, ordinal),
                value.clone(),
            );
        }
        LogicalMutation::PutTransaction(_) | LogicalMutation::PutReplicaMetadata(_) => {}
    }
    Ok(())
}

#[derive(Default)]
pub(crate) struct EdgeAdjacencyState {
    current: BTreeMap<EdgeId, EdgeVersion>,
    cleaned: BTreeSet<EdgeId>,
}

fn stage_current_mutation(
    namespace: &NamespaceDb,
    write: &mut OwnedWriteBatch,
    mutation: &LogicalMutation,
    adjacency: &mut EdgeAdjacencyState,
) -> Result<(), StorageError> {
    let value = encode_mutation(mutation)?;
    match mutation {
        LogicalMutation::PutVertex(vertex) => write.insert(
            &namespace.current_vertex,
            vertex.id().get().to_be_bytes(),
            value,
        ),
        LogicalMutation::DeleteVertex(tombstone) => {
            write.remove(
                &namespace.current_vertex,
                tombstone.id().get().to_be_bytes(),
            );
        }
        LogicalMutation::PutEdge(edge) => {
            clean_adjacency(namespace, write, edge.id(), adjacency)?;
            if let Some(previous) = adjacency.current.remove(&edge.id()) {
                remove_adjacency(write, namespace, &previous);
            }
            write.insert(
                &namespace.current_edge,
                edge.id().get().to_be_bytes(),
                value,
            );
            write.insert(
                &namespace.adjacency_out,
                adjacency_key(edge.source().get(), edge),
                [],
            );
            write.insert(
                &namespace.adjacency_in,
                adjacency_key(edge.target().get(), edge),
                [],
            );
            adjacency.current.insert(edge.id(), edge.clone());
        }
        LogicalMutation::DeleteEdge(tombstone) => {
            clean_adjacency(namespace, write, tombstone.id(), adjacency)?;
            if let Some(previous) = adjacency.current.remove(&tombstone.id()) {
                remove_adjacency(write, namespace, &previous);
            }
            write.remove(&namespace.current_edge, tombstone.id().get().to_be_bytes());
        }
        LogicalMutation::PutTransaction(transaction) => write.insert(
            &namespace.transaction,
            transaction.id().get().to_be_bytes(),
            value,
        ),
        LogicalMutation::PutReplicaMetadata(metadata) => {
            let mut key = b"user/".to_vec();
            key.extend_from_slice(metadata.name().as_bytes());
            write.insert(&namespace.replica_meta, key, value);
        }
    }
    Ok(())
}

struct AuthenticatedCurrentState {
    transactions: BTreeMap<TransactionId, TransactionRecord>,
    metadata: BTreeMap<String, ReplicaMetadata>,
}

fn validate_restored_state(
    binding: &ReplicaBinding,
    applied_index: u64,
    history: &[LogicalMutation],
    transactions: &[TransactionRecord],
    metadata: &[ReplicaMetadata],
    replay: &[SnapshotReplayRecord],
    changes: &[ChangeRecord],
) -> Result<AuthenticatedCurrentState, StorageError> {
    let mut replay_indices = replay
        .iter()
        .map(SnapshotReplayRecord::raft_index)
        .collect::<Vec<_>>();
    replay_indices.sort_unstable();
    replay_indices.dedup();
    if replay_indices.len() != replay.len() || replay_indices.iter().copied().ne(1..=applied_index)
    {
        return Err(StorageError::CorruptSnapshot(
            "snapshot replay identities are not contiguous through the applied index".into(),
        ));
    }
    let replay_indices = replay_indices.into_iter().collect::<BTreeSet<_>>();
    let mut cursors = changes.iter().map(ChangeRecord::cursor).collect::<Vec<_>>();
    cursors.sort_unstable();
    if cursors.windows(2).any(|pair| pair[0] == pair[1])
        || cursors
            .iter()
            .any(|cursor| !replay_indices.contains(&cursor.raft_index()))
    {
        return Err(StorageError::CorruptSnapshot(
            "snapshot change ordering is duplicated or lacks replay identity".into(),
        ));
    }
    let mut expected = BTreeMap::<u64, u64>::new();
    for cursor in cursors {
        let ordinal = expected.entry(cursor.raft_index()).or_default();
        if cursor.mutation_ordinal() != *ordinal {
            return Err(StorageError::CorruptSnapshot(
                "snapshot change ordinals are not contiguous".into(),
            ));
        }
        *ordinal += 1;
    }
    let replay_by_index = replay
        .iter()
        .map(|record| (record.raft_index(), record))
        .collect::<BTreeMap<_, _>>();
    let mut mutations_by_index = BTreeMap::<u64, Vec<LogicalMutation>>::new();
    for change in changes {
        mutations_by_index
            .entry(change.raft_index())
            .or_default()
            .push(change.mutation().clone());
    }
    for index in 1..=applied_index {
        let replay = replay_by_index.get(&index).ok_or_else(|| {
            StorageError::CorruptSnapshot("snapshot replay identity is missing".into())
        })?;
        let mutations = mutations_by_index.get(&index).ok_or_else(|| {
            StorageError::CorruptSnapshot("snapshot committed batch has no changes".into())
        })?;
        let batch = CommittedShardBatch::new(
            binding.clone(),
            replay.raft_term(),
            index,
            replay.command_id(),
            mutations.clone(),
        )
        .map_err(|error| {
            StorageError::CorruptSnapshot(format!(
                "snapshot committed batch cannot be reconstructed: {error}"
            ))
        })?;
        if batch.mutation_digest() != replay.mutation_digest() {
            return Err(StorageError::CorruptSnapshot(format!(
                "snapshot change digest does not match replay identity at Raft index {index}"
            )));
        }
    }
    let supplied_history = graph_history_multiset(history.iter())?;
    let authenticated_history = graph_history_multiset(changes.iter().map(ChangeRecord::mutation))?;
    if supplied_history != authenticated_history {
        return Err(StorageError::CorruptSnapshot(
            "snapshot graph history does not match authenticated changes".into(),
        ));
    }
    let supplied_transactions = unique_transaction_map(transactions)?;
    let supplied_metadata = unique_metadata_map(metadata)?;
    let mut authenticated_transactions = BTreeMap::new();
    let mut authenticated_metadata = BTreeMap::new();
    for change in changes {
        match change.mutation() {
            LogicalMutation::PutTransaction(transaction) => {
                authenticated_transactions.insert(transaction.id(), transaction.clone());
            }
            LogicalMutation::PutReplicaMetadata(metadata) => {
                authenticated_metadata.insert(metadata.name().to_owned(), metadata.clone());
            }
            LogicalMutation::PutVertex(_)
            | LogicalMutation::DeleteVertex(_)
            | LogicalMutation::PutEdge(_)
            | LogicalMutation::DeleteEdge(_) => {}
        }
    }
    if supplied_transactions != authenticated_transactions {
        return Err(StorageError::CorruptSnapshot(
            "snapshot transaction state does not match authenticated changes".into(),
        ));
    }
    if supplied_metadata != authenticated_metadata {
        return Err(StorageError::CorruptSnapshot(
            "snapshot replica metadata does not match authenticated changes".into(),
        ));
    }
    Ok(AuthenticatedCurrentState {
        transactions: authenticated_transactions,
        metadata: authenticated_metadata,
    })
}

fn unique_transaction_map(
    transactions: &[TransactionRecord],
) -> Result<BTreeMap<TransactionId, TransactionRecord>, StorageError> {
    let mut records = BTreeMap::new();
    for transaction in transactions {
        if records
            .insert(transaction.id(), transaction.clone())
            .is_some()
        {
            return Err(StorageError::CorruptSnapshot(
                "snapshot transaction state contains duplicate identifiers".into(),
            ));
        }
    }
    Ok(records)
}

fn unique_metadata_map(
    metadata: &[ReplicaMetadata],
) -> Result<BTreeMap<String, ReplicaMetadata>, StorageError> {
    let mut records = BTreeMap::new();
    for record in metadata {
        if records
            .insert(record.name().to_owned(), record.clone())
            .is_some()
        {
            return Err(StorageError::CorruptSnapshot(
                "snapshot replica metadata contains duplicate names".into(),
            ));
        }
    }
    Ok(records)
}

fn graph_history_multiset<'a>(
    mutations: impl Iterator<Item = &'a LogicalMutation>,
) -> Result<BTreeMap<Vec<u8>, usize>, StorageError> {
    let mut multiset = BTreeMap::new();
    for mutation in mutations.filter(|mutation| is_graph_mutation(mutation)) {
        *multiset.entry(encode_mutation(mutation)?).or_default() += 1;
    }
    Ok(multiset)
}

fn is_graph_mutation(mutation: &LogicalMutation) -> bool {
    matches!(
        mutation,
        LogicalMutation::PutVertex(_)
            | LogicalMutation::DeleteVertex(_)
            | LogicalMutation::PutEdge(_)
            | LogicalMutation::DeleteEdge(_)
    )
}

fn clear_logical_state(
    namespace: &NamespaceDb,
    write: &mut OwnedWriteBatch,
) -> Result<(), StorageError> {
    for keyspace in [
        &namespace.identity,
        &namespace.current_vertex,
        &namespace.current_edge,
        &namespace.history,
        &namespace.adjacency_out,
        &namespace.adjacency_in,
        &namespace.temporal_index,
        &namespace.transaction,
        &namespace.replica_meta,
    ] {
        for key in keyspace_keys(keyspace)? {
            write.remove(keyspace, key);
        }
    }
    Ok(())
}

fn keyspace_keys(keyspace: &Keyspace) -> Result<Vec<Vec<u8>>, StorageError> {
    keyspace
        .iter()
        .map(|item| item.key().map(|key| key.to_vec()).map_err(fjall_error))
        .collect()
}

fn clean_adjacency(
    namespace: &NamespaceDb,
    write: &mut OwnedWriteBatch,
    edge_id: EdgeId,
    state: &mut EdgeAdjacencyState,
) -> Result<(), StorageError> {
    if !state.cleaned.insert(edge_id) {
        return Ok(());
    }
    for keyspace in [&namespace.adjacency_out, &namespace.adjacency_in] {
        for key in keyspace_keys(keyspace)? {
            if key.len() != 48 {
                return Err(StorageError::Internal(
                    "invalid stored adjacency key".into(),
                ));
            }
            if key[16..32] == edge_id.get().to_be_bytes() {
                write.remove(keyspace, key);
            }
        }
    }
    Ok(())
}

fn remove_adjacency(write: &mut OwnedWriteBatch, namespace: &NamespaceDb, edge: &EdgeVersion) {
    write.remove(
        &namespace.adjacency_out,
        adjacency_key(edge.source().get(), edge),
    );
    write.remove(
        &namespace.adjacency_in,
        adjacency_key(edge.target().get(), edge),
    );
}

fn change_key(raft_index: u64, ordinal: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(17);
    key.push(b'c');
    key.extend_from_slice(&raft_index.to_be_bytes());
    key.extend_from_slice(&ordinal.to_be_bytes());
    key
}

fn vertex_history_key(vertex: &VertexVersion, tombstone: bool) -> Vec<u8> {
    history_key(
        b'v',
        vertex.id().get(),
        vertex.transaction_time().get(),
        vertex.version().get(),
        tombstone,
    )
}

fn vertex_tombstone_key(tombstone: &VertexTombstone) -> Vec<u8> {
    history_key(
        b'v',
        tombstone.id().get(),
        tombstone.transaction_time().get(),
        tombstone.version().get(),
        true,
    )
}

fn edge_history_key(edge: &EdgeVersion, tombstone: bool) -> Vec<u8> {
    history_key(
        b'e',
        edge.id().get(),
        edge.transaction_time().get(),
        edge.version().get(),
        tombstone,
    )
}

fn edge_tombstone_key(tombstone: &EdgeTombstone) -> Vec<u8> {
    history_key(
        b'e',
        tombstone.id().get(),
        tombstone.transaction_time().get(),
        tombstone.version().get(),
        true,
    )
}

fn history_key(
    prefix: u8,
    id: u128,
    transaction_time: i64,
    version: u64,
    tombstone: bool,
) -> Vec<u8> {
    let mut key = Vec::with_capacity(34);
    key.push(prefix);
    key.extend_from_slice(&id.to_be_bytes());
    key.extend_from_slice(&transaction_time.to_be_bytes());
    key.extend_from_slice(&version.to_be_bytes());
    key.push(u8::from(tombstone));
    key
}

fn history_occurrence_key(mut key: Vec<u8>, raft_index: u64, ordinal: u64) -> Vec<u8> {
    key.extend_from_slice(&raft_index.to_be_bytes());
    key.extend_from_slice(&ordinal.to_be_bytes());
    key
}

fn adjacency_key(vertex_id: u128, edge: &EdgeVersion) -> Vec<u8> {
    let mut key = Vec::with_capacity(48);
    key.extend_from_slice(&vertex_id.to_be_bytes());
    key.extend_from_slice(&edge.id().get().to_be_bytes());
    key.extend_from_slice(&edge.transaction_time().get().to_be_bytes());
    key.extend_from_slice(&edge.version().get().to_be_bytes());
    key
}

fn decode_u64(bytes: &[u8]) -> Result<u64, StorageError> {
    let bytes: [u8; 8] = bytes
        .try_into()
        .map_err(|_| StorageError::Internal("invalid stored u64".into()))?;
    Ok(u64::from_be_bytes(bytes))
}

#[cfg(feature = "tck")]
fn lock<T>(mutex: &Mutex<T>) -> Result<MutexGuard<'_, T>, StorageError> {
    mutex
        .lock()
        .map_err(|_| StorageError::Internal("Fjall store lock is poisoned".into()))
}
