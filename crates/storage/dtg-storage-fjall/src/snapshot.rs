use dtg_storage::{
    BindingRole, LogicalReplicaActivation, LogicalReplicaActivationReceipt,
    LogicalSnapshotCandidateReceipt, LogicalSnapshotReader, LogicalSnapshotSink,
    LogicalSnapshotSource, LogicalSnapshotWriter, ReadFence, ReplicaBinding, ReplicaStateStore,
    SUPPORTED_SNAPSHOT_FORMAT_VERSION, SnapshotChunk, SnapshotHeader, SnapshotManifest,
    SnapshotRequest, SnapshotRestoreReceipt, StorageError, StoreFuture,
};
use fjall::PersistMode;

use crate::{
    codec::{decode_binding, encode_binding},
    graph::FjallReplicaStore,
    namespace::{OWNER_KEY, SNAPSHOT_ACTIVATION_KEY, SNAPSHOT_INSTALL_KEY, fjall_error},
    read_view::FjallReadView,
};

impl LogicalSnapshotSource for FjallReplicaStore {
    fn begin_snapshot(
        &self,
        fence: ReadFence,
        request: SnapshotRequest,
    ) -> StoreFuture<'_, Box<dyn LogicalSnapshotReader>> {
        Box::pin(async move {
            let _guard = self.lock_graph()?;
            self.verify_fence(&fence)?;
            request.validate()?;
            let header = SnapshotHeader::new(
                request.snapshot_id(),
                self.binding().clone(),
                fence.applied_index(),
                SUPPORTED_SNAPSHOT_FORMAT_VERSION,
            )?;
            #[cfg(feature = "tck")]
            self.pause_tck_snapshot_after_fence()?;
            let records = FjallReadView::load(self, fence)?.snapshot_records();
            let chunks = records
                .chunks(request.max_records_per_chunk() as usize)
                .enumerate()
                .map(|(ordinal, records)| {
                    SnapshotChunk::new(request.snapshot_id(), ordinal as u64, records.to_vec())
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Box::new(FjallSnapshotReader {
                header,
                chunks,
                next: 0,
            }) as Box<dyn LogicalSnapshotReader>)
        })
    }
}

struct FjallSnapshotReader {
    header: SnapshotHeader,
    chunks: Vec<SnapshotChunk>,
    next: usize,
}

impl LogicalSnapshotReader for FjallSnapshotReader {
    fn header(&self) -> &SnapshotHeader {
        &self.header
    }

    fn next_chunk(&mut self) -> StoreFuture<'_, Option<SnapshotChunk>> {
        Box::pin(async move {
            let chunk = self.chunks.get(self.next).cloned();
            if chunk.is_some() {
                self.next += 1;
            }
            Ok(chunk)
        })
    }

    fn finish(self: Box<Self>) -> StoreFuture<'static, SnapshotManifest> {
        Box::pin(async move {
            if self.next != self.chunks.len() {
                return Err(StorageError::SnapshotNotExhausted);
            }
            SnapshotManifest::new(&self.header, &self.chunks)
        })
    }
}

impl LogicalSnapshotSink for FjallReplicaStore {
    fn begin_restore(
        &self,
        binding: ReplicaBinding,
        header: SnapshotHeader,
    ) -> StoreFuture<'_, Box<dyn LogicalSnapshotWriter>> {
        Box::pin(async move {
            self.verify_binding(&binding)?;
            if !same_logical_identity(&binding, header.source_binding()) {
                return Err(StorageError::SnapshotIdentityMismatch);
            }
            Ok(Box::new(FjallSnapshotWriter {
                store: self.clone(),
                target_binding: binding,
                header,
                chunks: Vec::new(),
                stage_keys: Vec::new(),
            }) as Box<dyn LogicalSnapshotWriter>)
        })
    }
}

struct FjallSnapshotWriter {
    store: FjallReplicaStore,
    target_binding: ReplicaBinding,
    header: SnapshotHeader,
    chunks: Vec<SnapshotChunk>,
    stage_keys: Vec<Vec<u8>>,
}

impl LogicalSnapshotWriter for FjallSnapshotWriter {
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
            let _guard = self.store.lock_graph()?;
            self.store.verify_binding(&self.target_binding)?;
            let key = stage_key(chunk.snapshot_id().get(), chunk.ordinal());
            self.store
                .namespace()
                .snapshot_stage
                .insert(&key, chunk.digest.get())
                .map_err(fjall_error)?;
            self.store
                .namespace()
                .db
                .persist(PersistMode::SyncAll)
                .map_err(fjall_error)?;
            self.stage_keys.push(key);
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
            let install_marker = (self.target_binding.role() == BindingRole::Candidate)
                .then(|| encode_install_marker(&self.header, &manifest));
            let records = self
                .chunks
                .iter()
                .flat_map(|chunk| chunk.records().iter().cloned())
                .collect::<Vec<_>>();
            self.store.restore_records(
                &records,
                self.header.applied_index(),
                &self.stage_keys,
                install_marker.as_deref(),
            )?;
            Ok(SnapshotRestoreReceipt::new(
                self.target_binding.clone(),
                manifest,
            ))
        })
    }

    fn abort(self: Box<Self>) -> StoreFuture<'static, ()> {
        Box::pin(async move {
            let _guard = self.store.lock_graph()?;
            self.store.verify_binding(&self.target_binding)?;
            for key in &self.stage_keys {
                self.store
                    .namespace()
                    .snapshot_stage
                    .remove(key)
                    .map_err(fjall_error)?;
            }
            self.store
                .namespace()
                .db
                .persist(PersistMode::SyncAll)
                .map_err(fjall_error)
        })
    }
}

impl LogicalReplicaActivation for FjallReplicaStore {
    fn activate_candidate(
        &self,
        candidate: LogicalSnapshotCandidateReceipt,
        active_binding: ReplicaBinding,
    ) -> StoreFuture<'_, LogicalReplicaActivationReceipt> {
        Box::pin(async move {
            let receipt = LogicalReplicaActivationReceipt::new(&candidate, active_binding.clone())?;
            if self.binding() != candidate.candidate_binding() && self.binding() != &active_binding
            {
                return Err(StorageError::SnapshotIdentityMismatch);
            }
            let _guard = self.lock_graph()?;
            let owner_bytes = self
                .namespace()
                .owner
                .get(OWNER_KEY)
                .map_err(fjall_error)?
                .ok_or_else(|| StorageError::Internal("missing namespace owner".into()))?;
            let owner = decode_binding(&owner_bytes)?;
            let expected_marker = encode_install_marker(candidate.header(), candidate.manifest());
            let expected_activation = encode_activation(&candidate, &receipt);
            if owner == active_binding {
                let stored = self
                    .namespace()
                    .owner
                    .get(SNAPSHOT_ACTIVATION_KEY)
                    .map_err(fjall_error)?
                    .ok_or_else(|| {
                        StorageError::CorruptSnapshot(
                            "activated namespace is missing its activation receipt".into(),
                        )
                    })?;
                if stored.as_ref() == expected_activation.as_slice() {
                    return Ok(receipt);
                }
                return Err(StorageError::SnapshotIdentityMismatch);
            }
            if owner != *candidate.candidate_binding() {
                return Err(StorageError::SnapshotIdentityMismatch);
            }
            let marker = self
                .namespace()
                .owner
                .get(SNAPSHOT_INSTALL_KEY)
                .map_err(fjall_error)?
                .ok_or_else(|| {
                    StorageError::CorruptSnapshot(
                        "candidate namespace is missing its snapshot install marker".into(),
                    )
                })?;
            if marker.as_ref() != expected_marker.as_slice() {
                return Err(StorageError::SnapshotIdentityMismatch);
            }
            let mut write = self
                .namespace()
                .db
                .batch()
                .durability(Some(PersistMode::SyncAll));
            write.insert(
                &self.namespace().owner,
                OWNER_KEY,
                encode_binding(&active_binding)?,
            );
            write.remove(&self.namespace().owner, SNAPSHOT_INSTALL_KEY);
            write.insert(
                &self.namespace().owner,
                SNAPSHOT_ACTIVATION_KEY,
                expected_activation,
            );
            write.commit().map_err(fjall_error)?;
            self.namespace()
                .rebind(candidate.candidate_binding(), &active_binding)?;
            Ok(receipt)
        })
    }
}

fn stage_key(snapshot_id: u128, ordinal: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(24);
    key.extend_from_slice(&snapshot_id.to_be_bytes());
    key.extend_from_slice(&ordinal.to_be_bytes());
    key
}

fn same_logical_identity(left: &ReplicaBinding, right: &ReplicaBinding) -> bool {
    left.cluster_id() == right.cluster_id()
        && left.graph_id() == right.graph_id()
        && left.shard_id() == right.shard_id()
}

fn encode_install_marker(header: &SnapshotHeader, manifest: &SnapshotManifest) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(64);
    bytes.extend_from_slice(&1_u32.to_be_bytes());
    bytes.extend_from_slice(&header.snapshot_id().get().to_be_bytes());
    bytes.extend_from_slice(&header.applied_index().to_be_bytes());
    bytes.extend_from_slice(&manifest.content_digest().get());
    bytes.extend_from_slice(&header.format_version().to_be_bytes());
    bytes
}

fn encode_activation(
    candidate: &LogicalSnapshotCandidateReceipt,
    receipt: &LogicalReplicaActivationReceipt,
) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(128);
    bytes.extend_from_slice(b"dtg-logical-replica-activation-v1");
    bytes.extend_from_slice(&candidate.candidate_binding().identity_digest().get());
    bytes.extend_from_slice(&receipt.active_binding().identity_digest().get());
    bytes.extend_from_slice(&receipt.snapshot_id().get().to_be_bytes());
    bytes.extend_from_slice(&receipt.applied_index().to_be_bytes());
    bytes.extend_from_slice(&receipt.content_digest().get());
    bytes.extend_from_slice(&receipt.format_version().to_be_bytes());
    bytes
}
