use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    path::Path,
    sync::{Arc, MutexGuard},
};

#[cfg(feature = "tck")]
use std::sync::{
    Condvar, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use dtg_storage::{
    ApplyReceipt, CapabilityManifest, CommittedShardBatch, EdgeId, EdgeTombstone, EdgeVersion,
    LogicalMutation, ReadFence, ReplicaBinding, ReplicaMetadata, ReplicaStateStore, StorageError,
    StoreFuture, TemporalReadView, VertexTombstone, VertexVersion,
};
use fjall::{OwnedWriteBatch, PersistMode};

use crate::{
    codec::{decode_mutation, decode_replay_identity, encode_mutation, encode_replay_identity},
    namespace::{NamespaceDb, SNAPSHOT_RESTORE_IN_PROGRESS_KEY, fjall_capabilities, fjall_error},
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
    #[cfg(feature = "tck")]
    snapshot_buffer_high_watermark: AtomicUsize,
    #[cfg(feature = "tck")]
    snapshot_writer_buffer_high_watermark: AtomicUsize,
    #[cfg(feature = "tck")]
    snapshot_commit_buffer_high_watermark: AtomicUsize,
    #[cfg(feature = "tck")]
    snapshot_restore_failure_after_batches: Mutex<Option<usize>>,
}

#[cfg(feature = "tck")]
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum GraphPausePoint {
    ApplyBeforeCommit,
    RestoreAfterOwnerCheckBeforeLock,
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
                #[cfg(feature = "tck")]
                snapshot_buffer_high_watermark: AtomicUsize::new(0),
                #[cfg(feature = "tck")]
                snapshot_writer_buffer_high_watermark: AtomicUsize::new(0),
                #[cfg(feature = "tck")]
                snapshot_commit_buffer_high_watermark: AtomicUsize::new(0),
                #[cfg(feature = "tck")]
                snapshot_restore_failure_after_batches: Mutex::new(None),
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
        self.inner.namespace.ensure_binding(&self.inner.binding)?;
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
        self.ensure_no_restore_in_progress()?;
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
        self.inner.namespace.ensure_binding(&self.inner.binding)?;
        self.inner
            .namespace
            .replica_meta
            .get(APPLIED_INDEX_KEY)
            .map_err(fjall_error)?
            .map(|bytes| decode_u64(&bytes))
            .transpose()
            .map(Option::unwrap_or_default)
    }

    fn ensure_no_restore_in_progress(&self) -> Result<(), StorageError> {
        if self
            .inner
            .namespace
            .owner
            .get(SNAPSHOT_RESTORE_IN_PROGRESS_KEY)
            .map_err(fjall_error)?
            .is_some()
        {
            Err(StorageError::CorruptSnapshot(
                "Fjall snapshot restore is incomplete".into(),
            ))
        } else {
            Ok(())
        }
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
    pub fn arm_tck_restore_after_owner_check_pause(&self) -> Result<FjallGraphPause, StorageError> {
        self.arm_graph_pause(GraphPausePoint::RestoreAfterOwnerCheckBeforeLock)
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
    pub fn tck_snapshot_buffer_high_watermark(&self) -> usize {
        self.inner
            .snapshot_buffer_high_watermark
            .load(Ordering::Relaxed)
    }

    #[cfg(feature = "tck")]
    pub(crate) fn observe_tck_snapshot_buffer(&self, records: usize) {
        self.inner
            .snapshot_buffer_high_watermark
            .fetch_max(records, Ordering::Relaxed);
    }

    #[cfg(feature = "tck")]
    pub fn tck_snapshot_writer_buffer_high_watermark(&self) -> usize {
        self.inner
            .snapshot_writer_buffer_high_watermark
            .load(Ordering::Relaxed)
    }

    #[cfg(feature = "tck")]
    pub(crate) fn observe_tck_snapshot_writer_buffer(&self, records: usize) {
        self.inner
            .snapshot_writer_buffer_high_watermark
            .fetch_max(records, Ordering::Relaxed);
    }

    #[cfg(feature = "tck")]
    pub fn tck_snapshot_commit_buffer_high_watermark(&self) -> usize {
        self.inner
            .snapshot_commit_buffer_high_watermark
            .load(Ordering::Relaxed)
    }

    #[cfg(feature = "tck")]
    pub(crate) fn observe_tck_snapshot_commit_buffer(&self, records: usize) {
        self.inner
            .snapshot_commit_buffer_high_watermark
            .fetch_max(records, Ordering::Relaxed);
    }

    #[cfg(feature = "tck")]
    pub fn arm_tck_snapshot_restore_failure_after_batches(
        &self,
        restored_batches: usize,
    ) -> Result<(), StorageError> {
        if restored_batches == 0 {
            return Err(StorageError::Internal(
                "snapshot restore failure point must be nonzero".into(),
            ));
        }
        *lock(&self.inner.snapshot_restore_failure_after_batches)? = Some(restored_batches);
        Ok(())
    }

    #[cfg(feature = "tck")]
    pub(crate) fn take_tck_snapshot_restore_failure_after_batches(
        &self,
    ) -> Result<Option<usize>, StorageError> {
        Ok(lock(&self.inner.snapshot_restore_failure_after_batches)?.take())
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
    pub(crate) fn pause_tck_restore_after_owner_check_before_lock(
        &self,
    ) -> Result<(), StorageError> {
        self.pause_graph_at(GraphPausePoint::RestoreAfterOwnerCheckBeforeLock)
    }

    #[cfg(feature = "tck")]
    pub(crate) fn pause_tck_restore_before_commit(&self) -> Result<(), StorageError> {
        self.pause_graph_at(GraphPausePoint::RestoreBeforeCommit)
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
        self.ensure_no_restore_in_progress()?;
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
            self.inner.namespace.ensure_binding(&self.inner.binding)?;
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
        for item in keyspace.iter() {
            let (key, _) = item.into_inner().map_err(fjall_error)?;
            if key.len() != 48 {
                return Err(StorageError::Internal(
                    "invalid stored adjacency key".into(),
                ));
            }
            if key[16..32] == edge_id.get().to_be_bytes() {
                write.remove(keyspace, key.as_ref());
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
