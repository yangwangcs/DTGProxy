use dtg_storage::{
    BindingRole, CommittedShardBatch, LogicalMutation, LogicalReplicaActivation,
    LogicalReplicaActivationReceipt, LogicalSnapshotCandidateReceipt, LogicalSnapshotReader,
    LogicalSnapshotSink, LogicalSnapshotSource, LogicalSnapshotWriter, ReadFence, ReplicaBinding,
    ReplicaStateStore, SUPPORTED_SNAPSHOT_FORMAT_VERSION, SnapshotChunk, SnapshotHeader,
    SnapshotManifest, SnapshotManifestBuilder, SnapshotRecord, SnapshotReplayRecord,
    SnapshotRequest, SnapshotRestoreReceipt, StorageError, StoreFuture,
};
use fjall::{Keyspace, PersistMode, Readable};

use crate::{
    codec::{
        decode_binding, decode_mutation, decode_replay_identity, decode_snapshot_record,
        encode_binding, encode_mutation, encode_replay_identity, encode_snapshot_record,
    },
    graph::{APPLIED_INDEX_KEY, EdgeAdjacencyState, FjallReplicaStore, stage_mutation},
    namespace::{
        OWNER_KEY, SNAPSHOT_ACTIVATION_KEY, SNAPSHOT_INSTALL_KEY, SNAPSHOT_RESTORE_IN_PROGRESS_KEY,
        fjall_error,
    },
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
            Ok(Box::new(FjallSnapshotReader {
                manifest: SnapshotManifestBuilder::new(header.clone()),
                header,
                snapshot: self.namespace().db.snapshot(),
                namespace: self.namespace().clone(),
                max_records_per_chunk: request.max_records_per_chunk() as usize,
                phase: SnapshotReadPhase::History,
                cursor: None,
                exhausted: false,
                #[cfg(feature = "tck")]
                store: self.clone(),
            }) as Box<dyn LogicalSnapshotReader>)
        })
    }
}

struct FjallSnapshotReader {
    header: SnapshotHeader,
    manifest: SnapshotManifestBuilder,
    snapshot: fjall::Snapshot,
    namespace: crate::namespace::NamespaceDb,
    max_records_per_chunk: usize,
    phase: SnapshotReadPhase,
    cursor: Option<Vec<u8>>,
    exhausted: bool,
    #[cfg(feature = "tck")]
    store: FjallReplicaStore,
}

#[derive(Clone, Copy)]
enum SnapshotReadPhase {
    History,
    Transactions,
    Metadata,
    Replay,
    Changes,
    Done,
}

impl FjallSnapshotReader {
    fn next_record(&mut self) -> Result<Option<SnapshotRecord>, StorageError> {
        loop {
            let keyspace = match self.phase {
                SnapshotReadPhase::History => &self.namespace.history,
                SnapshotReadPhase::Transactions => &self.namespace.transaction,
                SnapshotReadPhase::Metadata => &self.namespace.replica_meta,
                SnapshotReadPhase::Replay => &self.namespace.identity,
                SnapshotReadPhase::Changes => &self.namespace.temporal_index,
                SnapshotReadPhase::Done => return Ok(None),
            };
            let Some((key, value)) =
                next_snapshot_item(&self.snapshot, keyspace, self.cursor.as_deref())?
            else {
                self.phase = match self.phase {
                    SnapshotReadPhase::History => SnapshotReadPhase::Transactions,
                    SnapshotReadPhase::Transactions => SnapshotReadPhase::Metadata,
                    SnapshotReadPhase::Metadata => SnapshotReadPhase::Replay,
                    SnapshotReadPhase::Replay => SnapshotReadPhase::Changes,
                    SnapshotReadPhase::Changes | SnapshotReadPhase::Done => SnapshotReadPhase::Done,
                };
                self.cursor = None;
                continue;
            };
            self.cursor = Some(key.clone());
            let record = match self.phase {
                SnapshotReadPhase::History => match decode_mutation(&value)? {
                    dtg_storage::LogicalMutation::PutVertex(value) => SnapshotRecord::Vertex(value),
                    dtg_storage::LogicalMutation::DeleteVertex(value) => {
                        SnapshotRecord::VertexTombstone(value)
                    }
                    dtg_storage::LogicalMutation::PutEdge(value) => SnapshotRecord::Edge(value),
                    dtg_storage::LogicalMutation::DeleteEdge(value) => {
                        SnapshotRecord::EdgeTombstone(value)
                    }
                    _ => {
                        return Err(StorageError::Internal(
                            "non-graph mutation in Fjall history".into(),
                        ));
                    }
                },
                SnapshotReadPhase::Transactions => match decode_mutation(&value)? {
                    dtg_storage::LogicalMutation::PutTransaction(value) => {
                        SnapshotRecord::Transaction(value)
                    }
                    _ => {
                        return Err(StorageError::Internal(
                            "non-transaction in Fjall transaction state".into(),
                        ));
                    }
                },
                SnapshotReadPhase::Metadata => {
                    if !key.starts_with(b"user/") {
                        continue;
                    }
                    match decode_mutation(&value)? {
                        dtg_storage::LogicalMutation::PutReplicaMetadata(value) => {
                            SnapshotRecord::ReplicaMetadata(value)
                        }
                        _ => {
                            return Err(StorageError::Internal(
                                "non-metadata in Fjall replica metadata".into(),
                            ));
                        }
                    }
                }
                SnapshotReadPhase::Replay => {
                    let raft_index =
                        u64::from_be_bytes(key.as_slice().try_into().map_err(|_| {
                            StorageError::Internal("invalid Fjall replay key".into())
                        })?);
                    let replay = decode_replay_identity(&value)?;
                    SnapshotRecord::Replay(SnapshotReplayRecord::new(
                        raft_index,
                        replay.term,
                        replay.command_id,
                        replay.mutation_digest,
                    )?)
                }
                SnapshotReadPhase::Changes => {
                    if key.first() != Some(&b'c') {
                        continue;
                    }
                    if key.len() != 17 {
                        return Err(StorageError::Internal("invalid Fjall change key".into()));
                    }
                    let raft_index = u64::from_be_bytes(key[1..9].try_into().map_err(|_| {
                        StorageError::Internal("invalid Fjall change index".into())
                    })?);
                    let ordinal = u64::from_be_bytes(key[9..17].try_into().map_err(|_| {
                        StorageError::Internal("invalid Fjall change ordinal".into())
                    })?);
                    SnapshotRecord::Change(dtg_storage::ChangeRecord::new(
                        dtg_storage::ChangeCursor::new(raft_index, ordinal),
                        decode_mutation(&value)?,
                    ))
                }
                SnapshotReadPhase::Done => return Ok(None),
            };
            return Ok(Some(record));
        }
    }
}

impl LogicalSnapshotReader for FjallSnapshotReader {
    fn header(&self) -> &SnapshotHeader {
        &self.header
    }

    fn next_chunk(&mut self) -> StoreFuture<'_, Option<SnapshotChunk>> {
        Box::pin(async move {
            if self.exhausted {
                return Ok(None);
            }
            let mut records = Vec::with_capacity(self.max_records_per_chunk);
            while records.len() < self.max_records_per_chunk {
                match self.next_record()? {
                    Some(record) => records.push(record),
                    None => {
                        self.exhausted = true;
                        break;
                    }
                }
            }
            if records.is_empty() {
                return Ok(None);
            }
            #[cfg(feature = "tck")]
            self.store.observe_tck_snapshot_buffer(records.len());
            let chunk = SnapshotChunk::new(
                self.header.snapshot_id(),
                self.manifest.next_ordinal(),
                records,
            )?;
            self.manifest.push(&chunk)?;
            Ok(Some(chunk))
        })
    }

    fn finish(self: Box<Self>) -> StoreFuture<'static, SnapshotManifest> {
        Box::pin(async move {
            if !self.exhausted {
                return Err(StorageError::SnapshotNotExhausted);
            }
            Ok(self.manifest.finish())
        })
    }
}

type SnapshotItem = (Vec<u8>, Vec<u8>);

fn next_snapshot_item(
    snapshot: &fjall::Snapshot,
    keyspace: &Keyspace,
    after: Option<&[u8]>,
) -> Result<Option<SnapshotItem>, StorageError> {
    use std::ops::Bound::{Excluded, Unbounded};
    let item = match after {
        Some(after) => snapshot
            .range(keyspace, (Excluded(after.to_vec()), Unbounded::<Vec<u8>>))
            .next(),
        None => snapshot.iter(keyspace).next(),
    };
    item.map(|item| {
        let (key, value) = item.into_inner().map_err(fjall_error)?;
        Ok((key.to_vec(), value.to_vec()))
    })
    .transpose()
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
            let _guard = self.lock_graph()?;
            clear_staged_snapshot(self.namespace(), header.snapshot_id().get())?;
            Ok(Box::new(FjallSnapshotWriter {
                store: self.clone(),
                target_binding: binding,
                manifest: SnapshotManifestBuilder::new(header.clone()),
                header,
                last_change_cursor: None,
            }) as Box<dyn LogicalSnapshotWriter>)
        })
    }
}

struct FjallSnapshotWriter {
    store: FjallReplicaStore,
    target_binding: ReplicaBinding,
    header: SnapshotHeader,
    manifest: SnapshotManifestBuilder,
    last_change_cursor: Option<dtg_storage::ChangeCursor>,
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
                || chunk.ordinal() != self.manifest.next_ordinal()
            {
                return Err(StorageError::CorruptSnapshot(
                    "snapshot chunk identity or order mismatch".into(),
                ));
            }
            let _guard = self.store.lock_graph()?;
            self.store.verify_binding(&self.target_binding)?;
            #[cfg(feature = "tck")]
            self.store
                .observe_tck_snapshot_writer_buffer(chunk.records().len());
            let namespace = self.store.namespace();
            for (record_ordinal, record) in chunk.records().iter().enumerate() {
                namespace
                    .snapshot_stage
                    .insert(
                        stage_record_key(
                            chunk.snapshot_id().get(),
                            chunk.ordinal(),
                            record_ordinal as u64,
                        ),
                        encode_snapshot_record(record)?,
                    )
                    .map_err(fjall_error)?;
                stage_record_indexes(
                    namespace,
                    chunk.snapshot_id().get(),
                    record,
                    &mut self.last_change_cursor,
                )?;
            }
            namespace
                .snapshot_stage
                .insert(
                    stage_chunk_key(chunk.snapshot_id().get(), chunk.ordinal()),
                    stage_chunk_value(chunk.digest, chunk.records().len() as u64),
                )
                .map_err(fjall_error)?;
            namespace
                .db
                .persist(PersistMode::SyncAll)
                .map_err(fjall_error)?;
            self.manifest.push(&chunk)?;
            Ok(())
        })
    }

    fn commit(
        self: Box<Self>,
        manifest: SnapshotManifest,
    ) -> StoreFuture<'static, SnapshotRestoreReceipt> {
        Box::pin(async move {
            if self.manifest.finish() != manifest {
                return Err(StorageError::CorruptSnapshot(
                    "Fjall staged snapshot manifest mismatch".into(),
                ));
            }
            let install_marker = (self.target_binding.role() == BindingRole::Candidate)
                .then(|| encode_install_marker(&self.header, &manifest));
            #[cfg(feature = "tck")]
            self.store
                .pause_tck_restore_after_owner_check_before_lock()?;
            let _guard = self.store.lock_graph()?;
            self.store.verify_binding(&self.target_binding)?;
            restore_staged_snapshot(
                &self.store,
                &self.header,
                &manifest,
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
            clear_staged_snapshot(self.store.namespace(), self.header.snapshot_id().get())
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

fn stage_prefix(snapshot_id: u128) -> Vec<u8> {
    let mut key = Vec::with_capacity(17);
    key.push(b's');
    key.extend_from_slice(&snapshot_id.to_be_bytes());
    key
}

fn stage_chunk_key(snapshot_id: u128, ordinal: u64) -> Vec<u8> {
    let mut key = stage_prefix(snapshot_id);
    key.push(b'c');
    key.extend_from_slice(&ordinal.to_be_bytes());
    key
}

fn stage_record_key(snapshot_id: u128, ordinal: u64, record_ordinal: u64) -> Vec<u8> {
    let mut key = stage_prefix(snapshot_id);
    key.push(b'r');
    key.extend_from_slice(&ordinal.to_be_bytes());
    key.extend_from_slice(&record_ordinal.to_be_bytes());
    key
}

fn stage_chunk_value(digest: dtg_storage::Digest32, records: u64) -> Vec<u8> {
    let mut value = Vec::with_capacity(40);
    value.extend_from_slice(&digest.get());
    value.extend_from_slice(&records.to_be_bytes());
    value
}

fn stage_kind_prefix(snapshot_id: u128, kind: u8) -> Vec<u8> {
    let mut key = stage_prefix(snapshot_id);
    key.push(kind);
    key
}

fn stage_index_key(snapshot_id: u128, kind: u8, suffix: &[u8]) -> Vec<u8> {
    let mut key = stage_kind_prefix(snapshot_id, kind);
    key.extend_from_slice(suffix);
    key
}

fn stage_record_indexes(
    namespace: &crate::namespace::NamespaceDb,
    snapshot_id: u128,
    record: &SnapshotRecord,
    last_change_cursor: &mut Option<dtg_storage::ChangeCursor>,
) -> Result<(), StorageError> {
    match record {
        SnapshotRecord::Vertex(vertex) => stage_multiset_record(
            namespace,
            snapshot_id,
            b'h',
            &LogicalMutation::PutVertex(vertex.clone()),
        ),
        SnapshotRecord::VertexTombstone(tombstone) => stage_multiset_record(
            namespace,
            snapshot_id,
            b'h',
            &LogicalMutation::DeleteVertex(tombstone.clone()),
        ),
        SnapshotRecord::Edge(edge) => stage_multiset_record(
            namespace,
            snapshot_id,
            b'h',
            &LogicalMutation::PutEdge(edge.clone()),
        ),
        SnapshotRecord::EdgeTombstone(tombstone) => stage_multiset_record(
            namespace,
            snapshot_id,
            b'h',
            &LogicalMutation::DeleteEdge(tombstone.clone()),
        ),
        SnapshotRecord::Transaction(transaction) => insert_unique_stage_value(
            namespace,
            snapshot_id,
            stage_index_key(snapshot_id, b't', &transaction.id().get().to_be_bytes()),
            encode_mutation(&LogicalMutation::PutTransaction(transaction.clone()))?,
            "transaction identifier",
        ),
        SnapshotRecord::ReplicaMetadata(metadata) => insert_unique_stage_value(
            namespace,
            snapshot_id,
            stage_index_key(snapshot_id, b'm', metadata.name().as_bytes()),
            encode_mutation(&LogicalMutation::PutReplicaMetadata(metadata.clone()))?,
            "metadata name",
        ),
        SnapshotRecord::Replay(replay) => insert_unique_stage_value(
            namespace,
            snapshot_id,
            stage_index_key(snapshot_id, b'i', &replay.raft_index().to_be_bytes()),
            encode_snapshot_record(record)?,
            "replay index",
        ),
        SnapshotRecord::Change(change) => {
            if last_change_cursor.is_some_and(|cursor| change.cursor() <= cursor) {
                mark_stage_invalid(
                    namespace,
                    snapshot_id,
                    "snapshot changes are not in stable cursor order",
                )?;
            }
            let mut suffix = Vec::with_capacity(16);
            suffix.extend_from_slice(&change.raft_index().to_be_bytes());
            suffix.extend_from_slice(&change.mutation_ordinal().to_be_bytes());
            let encoded = encode_mutation(change.mutation())?;
            insert_unique_stage_value(
                namespace,
                snapshot_id,
                stage_index_key(snapshot_id, b'g', &suffix),
                encoded.clone(),
                "change cursor",
            )?;
            match change.mutation() {
                LogicalMutation::PutVertex(_)
                | LogicalMutation::DeleteVertex(_)
                | LogicalMutation::PutEdge(_)
                | LogicalMutation::DeleteEdge(_) => {
                    stage_multiset_encoded(namespace, snapshot_id, b'a', &encoded)?
                }
                LogicalMutation::PutTransaction(transaction) => namespace
                    .snapshot_stage
                    .insert(
                        stage_index_key(snapshot_id, b'T', &transaction.id().get().to_be_bytes()),
                        encoded,
                    )
                    .map_err(fjall_error)?,
                LogicalMutation::PutReplicaMetadata(metadata) => namespace
                    .snapshot_stage
                    .insert(
                        stage_index_key(snapshot_id, b'M', metadata.name().as_bytes()),
                        encoded,
                    )
                    .map_err(fjall_error)?,
            }
            *last_change_cursor = Some(change.cursor());
            Ok(())
        }
    }
}

fn insert_unique_stage_value(
    namespace: &crate::namespace::NamespaceDb,
    snapshot_id: u128,
    key: Vec<u8>,
    value: Vec<u8>,
    identity: &str,
) -> Result<(), StorageError> {
    if namespace
        .snapshot_stage
        .get(&key)
        .map_err(fjall_error)?
        .is_some()
    {
        mark_stage_invalid(
            namespace,
            snapshot_id,
            &format!("snapshot contains duplicate {identity}"),
        )?;
        return Ok(());
    }
    namespace
        .snapshot_stage
        .insert(key, value)
        .map_err(fjall_error)
}

fn mark_stage_invalid(
    namespace: &crate::namespace::NamespaceDb,
    snapshot_id: u128,
    reason: &str,
) -> Result<(), StorageError> {
    namespace
        .snapshot_stage
        .insert(stage_kind_prefix(snapshot_id, b'!'), reason.as_bytes())
        .map_err(fjall_error)
}

fn stage_multiset_record(
    namespace: &crate::namespace::NamespaceDb,
    snapshot_id: u128,
    kind: u8,
    mutation: &LogicalMutation,
) -> Result<(), StorageError> {
    stage_multiset_encoded(namespace, snapshot_id, kind, &encode_mutation(mutation)?)
}

fn stage_multiset_encoded(
    namespace: &crate::namespace::NamespaceDb,
    snapshot_id: u128,
    kind: u8,
    encoded: &[u8],
) -> Result<(), StorageError> {
    let digest = blake3::hash(encoded);
    let key = stage_index_key(snapshot_id, kind, digest.as_bytes());
    let count = match namespace.snapshot_stage.get(&key).map_err(fjall_error)? {
        Some(value) => {
            let (count, stored) = decode_multiset_value(&value)?;
            if stored != encoded {
                return Err(StorageError::CorruptSnapshot(
                    "snapshot graph mutation digest collision".into(),
                ));
            }
            count
        }
        None => 0,
    };
    let count = count
        .checked_add(1)
        .ok_or_else(|| StorageError::CorruptSnapshot("snapshot multiset overflow".into()))?;
    let mut value = Vec::with_capacity(8 + encoded.len());
    value.extend_from_slice(&count.to_be_bytes());
    value.extend_from_slice(encoded);
    namespace
        .snapshot_stage
        .insert(key, value)
        .map_err(fjall_error)
}

fn decode_multiset_value(value: &[u8]) -> Result<(u64, &[u8]), StorageError> {
    let count = value
        .get(..8)
        .ok_or_else(|| StorageError::CorruptSnapshot("invalid staged multiset value".into()))?;
    Ok((
        u64::from_be_bytes(
            count.try_into().map_err(|_| {
                StorageError::CorruptSnapshot("invalid staged multiset count".into())
            })?,
        ),
        &value[8..],
    ))
}

fn restore_staged_snapshot(
    store: &FjallReplicaStore,
    header: &SnapshotHeader,
    manifest: &SnapshotManifest,
    install_marker: Option<&[u8]>,
) -> Result<(), StorageError> {
    let namespace = store.namespace();
    validate_staged_manifest(namespace, header, manifest)?;
    validate_staged_state(store, header)?;

    let mut begin = namespace.db.batch().durability(Some(PersistMode::SyncAll));
    begin.insert(
        &namespace.owner,
        SNAPSHOT_RESTORE_IN_PROGRESS_KEY,
        header.snapshot_id().get().to_be_bytes(),
    );
    begin.commit().map_err(fjall_error)?;

    for keyspace in [
        &namespace.identity,
        &namespace.current_vertex,
        &namespace.current_edge,
        &namespace.history,
        &namespace.adjacency_out,
        &namespace.adjacency_in,
        &namespace.temporal_index,
        &namespace.transaction,
    ] {
        clear_keyspace(namespace, keyspace)?;
    }
    clear_keyspace_prefix(namespace, &namespace.replica_meta, b"user/".to_vec())?;

    #[cfg(feature = "tck")]
    let failure_after = store.take_tck_snapshot_restore_failure_after_batches()?;
    #[cfg(feature = "tck")]
    let mut restored_batches = 0_usize;
    for raft_index in 1..=header.applied_index() {
        let (replay, mutations) =
            load_staged_batch(namespace, header.snapshot_id().get(), raft_index)?;
        #[cfg(feature = "tck")]
        store.observe_tck_snapshot_commit_buffer(mutations.len());
        let mut write = namespace.db.batch().durability(Some(PersistMode::SyncAll));
        let mut adjacency = EdgeAdjacencyState::default();
        for (ordinal, mutation) in mutations.iter().enumerate() {
            stage_mutation(
                namespace,
                &mut write,
                raft_index,
                ordinal as u64,
                mutation,
                &mut adjacency,
            )?;
        }
        write.insert(
            &namespace.identity,
            raft_index.to_be_bytes(),
            encode_replay_identity(
                replay.raft_term(),
                replay.command_id(),
                replay.mutation_digest(),
            )?,
        );
        write.commit().map_err(fjall_error)?;
        #[cfg(feature = "tck")]
        {
            restored_batches += 1;
            if failure_after == Some(restored_batches) {
                return Err(StorageError::Internal(
                    "injected Fjall snapshot restore failure".into(),
                ));
            }
        }
    }

    let mut publish = namespace.db.batch().durability(Some(PersistMode::SyncAll));
    publish.insert(
        &namespace.replica_meta,
        APPLIED_INDEX_KEY,
        header.applied_index().to_be_bytes(),
    );
    if let Some(marker) = install_marker {
        publish.insert(&namespace.owner, SNAPSHOT_INSTALL_KEY, marker);
    }
    publish.remove(&namespace.owner, SNAPSHOT_RESTORE_IN_PROGRESS_KEY);
    #[cfg(feature = "tck")]
    store.pause_tck_restore_before_commit()?;
    publish.commit().map_err(fjall_error)?;
    clear_staged_snapshot(namespace, header.snapshot_id().get())
}

fn validate_staged_manifest(
    namespace: &crate::namespace::NamespaceDb,
    header: &SnapshotHeader,
    expected: &SnapshotManifest,
) -> Result<(), StorageError> {
    let snapshot_id = header.snapshot_id().get();
    let chunk_prefix = stage_kind_prefix(snapshot_id, b'c');
    let mut builder = SnapshotManifestBuilder::new(header.clone());
    for item in namespace.snapshot_stage.prefix(&chunk_prefix) {
        let (key, value) = item.into_inner().map_err(fjall_error)?;
        let ordinal = decode_stage_u64_suffix(&key, &chunk_prefix, "chunk ordinal")?;
        if ordinal != builder.next_ordinal() || value.len() != 40 {
            return Err(StorageError::CorruptSnapshot(
                "Fjall staged snapshot chunks are not contiguous".into(),
            ));
        }
        let digest =
            dtg_storage::Digest32::new(value[..32].try_into().map_err(|_| {
                StorageError::CorruptSnapshot("invalid staged chunk digest".into())
            })?);
        let record_count = u64::from_be_bytes(value[32..].try_into().map_err(|_| {
            StorageError::CorruptSnapshot("invalid staged chunk record count".into())
        })?);
        let mut record_prefix = stage_kind_prefix(snapshot_id, b'r');
        record_prefix.extend_from_slice(&ordinal.to_be_bytes());
        let mut records = Vec::with_capacity(record_count.try_into().map_err(|_| {
            StorageError::CorruptSnapshot("staged chunk record count is too large".into())
        })?);
        for record_item in namespace.snapshot_stage.prefix(&record_prefix) {
            let (record_key, record_value) = record_item.into_inner().map_err(fjall_error)?;
            let record_ordinal =
                decode_stage_u64_suffix(&record_key, &record_prefix, "record ordinal")?;
            if record_ordinal != records.len() as u64 {
                return Err(StorageError::CorruptSnapshot(
                    "Fjall staged snapshot records are not contiguous".into(),
                ));
            }
            records.push(decode_snapshot_record(&record_value)?);
        }
        if records.len() as u64 != record_count {
            return Err(StorageError::CorruptSnapshot(
                "Fjall staged snapshot record count mismatch".into(),
            ));
        }
        let chunk = SnapshotChunk::new(header.snapshot_id(), ordinal, records)?;
        if chunk.digest != digest {
            return Err(StorageError::CorruptSnapshot(
                "Fjall staged snapshot chunk digest mismatch".into(),
            ));
        }
        builder.push(&chunk)?;
    }
    if builder.finish() == *expected {
        Ok(())
    } else {
        Err(StorageError::CorruptSnapshot(
            "Fjall staged snapshot manifest mismatch".into(),
        ))
    }
}

fn validate_staged_state(
    store: &FjallReplicaStore,
    header: &SnapshotHeader,
) -> Result<(), StorageError> {
    let namespace = store.namespace();
    let snapshot_id = header.snapshot_id().get();
    if let Some(reason) = namespace
        .snapshot_stage
        .get(stage_kind_prefix(snapshot_id, b'!'))
        .map_err(fjall_error)?
    {
        return Err(StorageError::CorruptSnapshot(
            String::from_utf8_lossy(&reason).into_owned(),
        ));
    }
    validate_replay_sequence(namespace, snapshot_id, header.applied_index())?;
    let mut staged_change_count = 0_u64;
    for raft_index in 1..=header.applied_index() {
        let (replay, mutations) = load_staged_batch(namespace, snapshot_id, raft_index)?;
        #[cfg(feature = "tck")]
        store.observe_tck_snapshot_commit_buffer(mutations.len());
        staged_change_count = staged_change_count
            .checked_add(mutations.len() as u64)
            .ok_or_else(|| {
                StorageError::CorruptSnapshot("snapshot change count overflow".into())
            })?;
        let batch = CommittedShardBatch::new(
            store.binding().clone(),
            replay.raft_term(),
            raft_index,
            replay.command_id(),
            mutations,
        )
        .map_err(|error| {
            StorageError::CorruptSnapshot(format!(
                "snapshot committed batch cannot be reconstructed: {error}"
            ))
        })?;
        if batch.mutation_digest() != replay.mutation_digest() {
            return Err(StorageError::CorruptSnapshot(format!(
                "snapshot change digest does not match replay identity at Raft index {raft_index}"
            )));
        }
    }
    if count_stage_entries(namespace, stage_kind_prefix(snapshot_id, b'g'))? != staged_change_count
    {
        return Err(StorageError::CorruptSnapshot(
            "snapshot contains changes outside the applied Raft range".into(),
        ));
    }
    for (supplied, authenticated, description) in [
        (b'h', b'a', "graph history"),
        (b't', b'T', "transaction state"),
        (b'm', b'M', "replica metadata"),
    ] {
        compare_stage_sets(namespace, snapshot_id, supplied, authenticated, description)?;
    }
    Ok(())
}

fn validate_replay_sequence(
    namespace: &crate::namespace::NamespaceDb,
    snapshot_id: u128,
    applied_index: u64,
) -> Result<(), StorageError> {
    let prefix = stage_kind_prefix(snapshot_id, b'i');
    let mut expected = 1_u64;
    for item in namespace.snapshot_stage.prefix(&prefix) {
        let (key, value) = item.into_inner().map_err(fjall_error)?;
        let raft_index = decode_stage_u64_suffix(&key, &prefix, "replay index")?;
        let replay = match decode_snapshot_record(&value)? {
            SnapshotRecord::Replay(replay) => replay,
            _ => {
                return Err(StorageError::CorruptSnapshot(
                    "invalid staged replay record".into(),
                ));
            }
        };
        if raft_index != expected || replay.raft_index() != raft_index {
            return Err(StorageError::CorruptSnapshot(
                "snapshot replay identities are not contiguous through the applied index".into(),
            ));
        }
        expected = expected
            .checked_add(1)
            .ok_or_else(|| StorageError::CorruptSnapshot("replay index overflow".into()))?;
    }
    if expected == applied_index.saturating_add(1) {
        Ok(())
    } else {
        Err(StorageError::CorruptSnapshot(
            "snapshot replay identities are not contiguous through the applied index".into(),
        ))
    }
}

fn load_staged_batch(
    namespace: &crate::namespace::NamespaceDb,
    snapshot_id: u128,
    raft_index: u64,
) -> Result<(SnapshotReplayRecord, Vec<LogicalMutation>), StorageError> {
    let replay_value = namespace
        .snapshot_stage
        .get(stage_index_key(
            snapshot_id,
            b'i',
            &raft_index.to_be_bytes(),
        ))
        .map_err(fjall_error)?
        .ok_or_else(|| {
            StorageError::CorruptSnapshot("snapshot replay identity is missing".into())
        })?;
    let replay = match decode_snapshot_record(&replay_value)? {
        SnapshotRecord::Replay(replay) => replay,
        _ => {
            return Err(StorageError::CorruptSnapshot(
                "invalid staged replay record".into(),
            ));
        }
    };
    let mut prefix = stage_kind_prefix(snapshot_id, b'g');
    prefix.extend_from_slice(&raft_index.to_be_bytes());
    let mut mutations = Vec::new();
    for item in namespace.snapshot_stage.prefix(&prefix) {
        let (key, value) = item.into_inner().map_err(fjall_error)?;
        let ordinal = decode_stage_u64_suffix(&key, &prefix, "change ordinal")?;
        if ordinal != mutations.len() as u64 {
            return Err(StorageError::CorruptSnapshot(
                "snapshot change ordinals are not contiguous".into(),
            ));
        }
        mutations.push(decode_mutation(&value)?);
    }
    if mutations.is_empty() {
        return Err(StorageError::CorruptSnapshot(
            "snapshot committed batch has no changes".into(),
        ));
    }
    Ok((replay, mutations))
}

fn compare_stage_sets(
    namespace: &crate::namespace::NamespaceDb,
    snapshot_id: u128,
    supplied_kind: u8,
    authenticated_kind: u8,
    description: &str,
) -> Result<(), StorageError> {
    let supplied_prefix = stage_kind_prefix(snapshot_id, supplied_kind);
    let authenticated_prefix = stage_kind_prefix(snapshot_id, authenticated_kind);
    let mut supplied = namespace.snapshot_stage.prefix(&supplied_prefix);
    let mut authenticated = namespace.snapshot_stage.prefix(&authenticated_prefix);
    loop {
        let left = supplied
            .next()
            .map(|item| item.into_inner().map_err(fjall_error))
            .transpose()?;
        let right = authenticated
            .next()
            .map(|item| item.into_inner().map_err(fjall_error))
            .transpose()?;
        match (left, right) {
            (None, None) => return Ok(()),
            (Some((left_key, left_value)), Some((right_key, right_value)))
                if left_key[supplied_prefix.len()..] == right_key[authenticated_prefix.len()..]
                    && left_value == right_value => {}
            _ => {
                return Err(StorageError::CorruptSnapshot(format!(
                    "snapshot {description} does not match authenticated changes"
                )));
            }
        }
    }
}

fn count_stage_entries(
    namespace: &crate::namespace::NamespaceDb,
    prefix: Vec<u8>,
) -> Result<u64, StorageError> {
    namespace
        .snapshot_stage
        .prefix(prefix)
        .try_fold(0_u64, |count, item| {
            item.into_inner().map_err(fjall_error)?;
            count.checked_add(1).ok_or_else(|| {
                StorageError::CorruptSnapshot("snapshot stage count overflow".into())
            })
        })
}

fn decode_stage_u64_suffix(
    key: &[u8],
    prefix: &[u8],
    description: &str,
) -> Result<u64, StorageError> {
    let suffix = key
        .get(prefix.len()..)
        .ok_or_else(|| StorageError::CorruptSnapshot(format!("invalid staged {description}")))?;
    Ok(u64::from_be_bytes(suffix.try_into().map_err(|_| {
        StorageError::CorruptSnapshot(format!("invalid staged {description}"))
    })?))
}

fn clear_staged_snapshot(
    namespace: &crate::namespace::NamespaceDb,
    snapshot_id: u128,
) -> Result<(), StorageError> {
    clear_keyspace_prefix(
        namespace,
        &namespace.snapshot_stage,
        stage_prefix(snapshot_id),
    )
}

fn clear_keyspace(
    namespace: &crate::namespace::NamespaceDb,
    keyspace: &Keyspace,
) -> Result<(), StorageError> {
    const DELETE_BATCH_SIZE: usize = 256;
    loop {
        let keys = keyspace
            .iter()
            .take(DELETE_BATCH_SIZE)
            .map(|item| {
                let (key, _) = item.into_inner().map_err(fjall_error)?;
                Ok(key.to_vec())
            })
            .collect::<Result<Vec<_>, StorageError>>()?;
        if keys.is_empty() {
            return Ok(());
        }
        let mut write = namespace.db.batch().durability(Some(PersistMode::SyncAll));
        for key in keys {
            write.remove(keyspace, key);
        }
        write.commit().map_err(fjall_error)?;
    }
}

fn clear_keyspace_prefix(
    namespace: &crate::namespace::NamespaceDb,
    keyspace: &Keyspace,
    prefix: Vec<u8>,
) -> Result<(), StorageError> {
    const DELETE_BATCH_SIZE: usize = 256;
    loop {
        let keys = keyspace
            .prefix(prefix.clone())
            .take(DELETE_BATCH_SIZE)
            .map(|item| {
                let (key, _) = item.into_inner().map_err(fjall_error)?;
                Ok(key.to_vec())
            })
            .collect::<Result<Vec<_>, StorageError>>()?;
        if keys.is_empty() {
            return Ok(());
        }
        let mut write = namespace.db.batch().durability(Some(PersistMode::SyncAll));
        for key in keys {
            write.remove(keyspace, key);
        }
        write.commit().map_err(fjall_error)?;
    }
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
