use std::{
    fmt,
    path::Path,
    sync::{Arc, Mutex, MutexGuard},
};

use dtg_storage::{
    ApplyReceipt, CapabilityManifest, CommittedShardBatch, EdgeTombstone, EdgeVersion,
    LogicalMutation, ReadFence, ReplicaBinding, ReplicaStateStore, StorageError, StoreFuture,
    TemporalReadView, VertexTombstone, VertexVersion,
};
use fjall::{OwnedWriteBatch, PersistMode};

use crate::{
    codec::{decode_replay_identity, encode_mutation, encode_replay_identity},
    namespace::{NamespaceDb, fjall_error},
    read_view::FjallReadView,
};

pub(crate) const APPLIED_INDEX_KEY: &[u8] = b"system/applied_index";

struct ReplicaInner {
    namespace: NamespaceDb,
    binding: ReplicaBinding,
    capabilities: CapabilityManifest,
    apply_guard: Mutex<()>,
    injected_failure_after: Mutex<Option<usize>>,
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
        let capabilities = CapabilityManifest::from_names([
            "adjacency",
            "immutable-read-view",
            "logical-snapshot",
            "point",
        ])?;
        if binding.capability_digest() != capabilities.digest() {
            return Err(StorageError::InvalidBinding(
                "Fjall replica binding capability digest does not match provider capabilities"
                    .into(),
            ));
        }
        let namespace = NamespaceDb::open(path.as_ref(), &binding)?;
        Ok(Self {
            inner: Arc::new(ReplicaInner {
                namespace,
                binding,
                capabilities,
                apply_guard: Mutex::new(()),
                injected_failure_after: Mutex::new(None),
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
        if fence.applied_index() > applied {
            return Err(StorageError::ReadFenceUnavailable {
                requested: fence.applied_index(),
                applied,
            });
        }
        Ok(())
    }

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

    fn apply_sync(&self, batch: CommittedShardBatch) -> Result<ApplyReceipt, StorageError> {
        batch.validate()?;
        self.verify_binding(batch.binding())?;
        let _guard = lock(&self.inner.apply_guard)?;
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

        let failure_after = lock(&self.inner.injected_failure_after)?.take();
        let mut write = self
            .inner
            .namespace
            .db
            .batch()
            .durability(Some(PersistMode::SyncAll));
        for (ordinal, mutation) in batch.mutations().iter().enumerate() {
            stage_mutation(
                &self.inner.namespace,
                &mut write,
                batch.raft_index(),
                ordinal as u64,
                mutation,
            )?;
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
        write.commit().map_err(fjall_error)?;
        Ok(ApplyReceipt::new(&batch, false))
    }

    pub(crate) fn restore_records(
        &self,
        records: &[dtg_storage::SnapshotRecord],
        applied_index: u64,
        stage_keys: &[Vec<u8>],
    ) -> Result<(), StorageError> {
        let _guard = lock(&self.inner.apply_guard)?;
        let mut write = self
            .inner
            .namespace
            .db
            .batch()
            .durability(Some(PersistMode::SyncAll));
        for (ordinal, record) in records.iter().enumerate() {
            let mutation = match record {
                dtg_storage::SnapshotRecord::Vertex(vertex) => {
                    LogicalMutation::PutVertex(vertex.clone())
                }
                dtg_storage::SnapshotRecord::Edge(edge) => LogicalMutation::PutEdge(edge.clone()),
                dtg_storage::SnapshotRecord::Transaction(transaction) => {
                    LogicalMutation::PutTransaction(transaction.clone())
                }
                dtg_storage::SnapshotRecord::ReplicaMetadata(metadata) => {
                    LogicalMutation::PutReplicaMetadata(metadata.clone())
                }
            };
            stage_mutation(
                &self.inner.namespace,
                &mut write,
                applied_index,
                ordinal as u64,
                &mutation,
            )?;
        }
        for key in stage_keys {
            write.remove(&self.inner.namespace.snapshot_stage, key);
        }
        write.insert(
            &self.inner.namespace.replica_meta,
            APPLIED_INDEX_KEY,
            applied_index.to_be_bytes(),
        );
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

    fn apply(&self, batch: CommittedShardBatch) -> StoreFuture<'_, ApplyReceipt> {
        Box::pin(async move { self.apply_sync(batch) })
    }

    fn begin_read_view(&self, fence: ReadFence) -> StoreFuture<'_, Box<dyn TemporalReadView>> {
        Box::pin(async move {
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
) -> Result<(), StorageError> {
    let value = encode_mutation(mutation)?;
    match mutation {
        LogicalMutation::PutVertex(vertex) => {
            write.insert(
                &namespace.current_vertex,
                vertex.id().get().to_be_bytes(),
                value.clone(),
            );
            write.insert(
                &namespace.history,
                vertex_history_key(vertex, false),
                value.clone(),
            );
        }
        LogicalMutation::DeleteVertex(tombstone) => {
            write.remove(
                &namespace.current_vertex,
                tombstone.id().get().to_be_bytes(),
            );
            write.insert(
                &namespace.history,
                vertex_tombstone_key(tombstone),
                value.clone(),
            );
        }
        LogicalMutation::PutEdge(edge) => {
            write.insert(
                &namespace.current_edge,
                edge.id().get().to_be_bytes(),
                value.clone(),
            );
            write.insert(
                &namespace.history,
                edge_history_key(edge, false),
                value.clone(),
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
        }
        LogicalMutation::DeleteEdge(tombstone) => {
            write.remove(&namespace.current_edge, tombstone.id().get().to_be_bytes());
            write.insert(
                &namespace.history,
                edge_tombstone_key(tombstone),
                value.clone(),
            );
        }
        LogicalMutation::PutTransaction(transaction) => {
            write.insert(
                &namespace.transaction,
                transaction.id().get().to_be_bytes(),
                value.clone(),
            );
        }
        LogicalMutation::PutReplicaMetadata(metadata) => {
            let mut key = b"user/".to_vec();
            key.extend_from_slice(metadata.name().as_bytes());
            write.insert(&namespace.replica_meta, key, value.clone());
        }
    }
    let mut change_key = Vec::with_capacity(17);
    change_key.push(b'c');
    change_key.extend_from_slice(&raft_index.to_be_bytes());
    change_key.extend_from_slice(&ordinal.to_be_bytes());
    write.insert(&namespace.temporal_index, change_key, value);
    Ok(())
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

fn lock<T>(mutex: &Mutex<T>) -> Result<MutexGuard<'_, T>, StorageError> {
    mutex
        .lock()
        .map_err(|_| StorageError::Internal("Fjall store lock is poisoned".into()))
}
