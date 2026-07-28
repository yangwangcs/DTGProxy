use dtg_storage::{
    LogicalSnapshotReader, LogicalSnapshotSink, LogicalSnapshotSource, LogicalSnapshotWriter,
    ReadFence, ReplicaBinding, ReplicaStateStore, SUPPORTED_SNAPSHOT_FORMAT_VERSION, SnapshotChunk,
    SnapshotHeader, SnapshotManifest, SnapshotRequest, SnapshotRestoreReceipt, StorageError,
    StoreFuture,
};
use fjall::PersistMode;

use crate::{graph::FjallReplicaStore, namespace::fjall_error, read_view::FjallReadView};

impl LogicalSnapshotSource for FjallReplicaStore {
    fn begin_snapshot(
        &self,
        fence: ReadFence,
        request: SnapshotRequest,
    ) -> StoreFuture<'_, Box<dyn LogicalSnapshotReader>> {
        Box::pin(async move {
            self.verify_fence(&fence)?;
            request.validate()?;
            let header = SnapshotHeader::new(
                request.snapshot_id(),
                self.binding().clone(),
                fence.applied_index(),
                SUPPORTED_SNAPSHOT_FORMAT_VERSION,
            )?;
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
            let records = self
                .chunks
                .iter()
                .flat_map(|chunk| chunk.records().iter().cloned())
                .collect::<Vec<_>>();
            self.store
                .restore_records(&records, self.header.applied_index(), &self.stage_keys)?;
            Ok(SnapshotRestoreReceipt::new(
                self.target_binding.clone(),
                manifest,
            ))
        })
    }

    fn abort(self: Box<Self>) -> StoreFuture<'static, ()> {
        Box::pin(async move {
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
