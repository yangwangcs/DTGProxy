use std::collections::{BTreeMap, BTreeSet};

use dtg_storage::{
    BindingRole, ChangeRecord, CommittedShardBatch, Digest32, LogicalMutation,
    LogicalReplicaActivationReceipt, LogicalSnapshotCandidateReceipt, LogicalSnapshotReader,
    LogicalSnapshotWriter, ReadFence, ReplicaBinding, ReplicaMetadata, SnapshotChunk,
    SnapshotHeader, SnapshotManifest, SnapshotRecord, SnapshotReplayRecord, SnapshotRequest,
    SnapshotRestoreReceipt, StorageError, StoreFuture, TransactionId, TransactionRecord,
};
use tokio_postgres::{Client, Row};

use crate::{
    PostgresReplicaStore,
    apply::{insert_replay, stage_mutation},
    codec::encode_mutation,
    config::postgres_error,
    schema::{
        decode_digest, decode_u64, finish_transaction, load_owner, read_applied_index, role_tag,
        u64_bytes, u128_bytes, verify_owner,
    },
};

pub(crate) async fn snapshot_reader(
    store: &PostgresReplicaStore,
    fence: ReadFence,
    request: SnapshotRequest,
) -> Result<Box<dyn LogicalSnapshotReader>, StorageError> {
    request.validate()?;
    let view = store.open_read_view(fence.clone()).await?;
    let header = SnapshotHeader::new(
        request.snapshot_id(),
        store.binding_ref().clone(),
        fence.applied_index(),
        dtg_storage::SUPPORTED_SNAPSHOT_FORMAT_VERSION,
    )?;
    let records = view.snapshot_records().await?;
    let chunks = records
        .chunks(request.max_records_per_chunk() as usize)
        .enumerate()
        .map(|(ordinal, records)| {
            SnapshotChunk::new(request.snapshot_id(), ordinal as u64, records.to_vec())
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Box::new(PostgresSnapshotReader {
        header,
        chunks,
        next: 0,
    }))
}

struct PostgresSnapshotReader {
    header: SnapshotHeader,
    chunks: Vec<SnapshotChunk>,
    next: usize,
}

impl LogicalSnapshotReader for PostgresSnapshotReader {
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

pub(crate) async fn snapshot_writer(
    store: &PostgresReplicaStore,
    binding: ReplicaBinding,
    header: SnapshotHeader,
) -> Result<Box<dyn LogicalSnapshotWriter>, StorageError> {
    if &binding != store.binding_ref() {
        return Err(StorageError::StaleBinding {
            expected: Box::new(store.binding_ref().clone()),
            actual: Box::new(binding),
        });
    }
    if !same_logical_identity(store.binding_ref(), header.source_binding()) {
        return Err(StorageError::SnapshotIdentityMismatch);
    }
    let client = store.connect().await?;
    verify_owner(&client, store.binding_ref(), false).await?;
    Ok(Box::new(PostgresSnapshotWriter {
        store: store.clone(),
        target_binding: store.binding_ref().clone(),
        header,
        chunks: Vec::new(),
    }))
}

struct PostgresSnapshotWriter {
    store: PostgresReplicaStore,
    target_binding: ReplicaBinding,
    header: SnapshotHeader,
    chunks: Vec<SnapshotChunk>,
}

impl LogicalSnapshotWriter for PostgresSnapshotWriter {
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
            let client = self.store.connect().await?;
            client
                .batch_execute(
                    "BEGIN ISOLATION LEVEL SERIALIZABLE;
                     SET LOCAL synchronous_commit = on",
                )
                .await
                .map_err(postgres_error)?;
            let result = async {
                verify_owner(&client, self.store.binding_ref(), true).await?;
                client
                    .execute(
                        "INSERT INTO snapshot_stage (
                            snapshot_id, chunk_ordinal, chunk_digest, record_count
                         ) VALUES ($1, $2, $3, $4)
                         ON CONFLICT (snapshot_id, chunk_ordinal) DO UPDATE SET
                            chunk_digest = EXCLUDED.chunk_digest,
                            record_count = EXCLUDED.record_count",
                        &[
                            &u128_bytes(chunk.snapshot_id().get()),
                            &u64_bytes(chunk.ordinal()),
                            &chunk.digest.get().to_vec(),
                            &i64::try_from(chunk.records().len()).map_err(|_| {
                                StorageError::CorruptSnapshot(
                                    "snapshot chunk record count exceeds PostgreSQL BIGINT".into(),
                                )
                            })?,
                        ],
                    )
                    .await
                    .map_err(postgres_error)?;
                Ok(())
            }
            .await;
            finish_transaction(&client, result).await?;
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
            let batches = validate_restored_records(
                &self.target_binding,
                self.header.applied_index(),
                &records,
            )?;
            let client = self.store.connect().await?;
            client
                .batch_execute(
                    "BEGIN ISOLATION LEVEL SERIALIZABLE;
                     SET LOCAL synchronous_commit = on",
                )
                .await
                .map_err(postgres_error)?;
            let result = async {
                verify_owner(&client, self.store.binding_ref(), true).await?;
                verify_staged_chunks(&client, &self.header, &self.chunks).await?;
                clear_logical_state(&client).await?;
                for batch in &batches {
                    for (ordinal, mutation) in batch.mutations().iter().enumerate() {
                        stage_mutation(&client, batch.raft_index(), ordinal as u64, mutation)
                            .await?;
                    }
                    insert_replay(&client, batch).await?;
                }
                client
                    .execute(
                        "UPDATE replica_meta SET applied_index = $1 WHERE singleton = TRUE",
                        &[&u64_bytes(self.header.applied_index())],
                    )
                    .await
                    .map_err(postgres_error)?;
                if self.target_binding.role() == BindingRole::Candidate {
                    persist_install_marker(
                        &client,
                        &SnapshotInstallMarker::new(&self.target_binding, &self.header, &manifest)?,
                    )
                    .await?;
                } else {
                    client
                        .execute("DELETE FROM snapshot_install WHERE singleton = TRUE", &[])
                        .await
                        .map_err(postgres_error)?;
                }
                client
                    .execute(
                        "DELETE FROM snapshot_stage WHERE snapshot_id = $1",
                        &[&u128_bytes(self.header.snapshot_id().get())],
                    )
                    .await
                    .map_err(postgres_error)?;
                Ok(())
            }
            .await;
            finish_transaction(&client, result).await?;
            Ok(SnapshotRestoreReceipt::new(
                self.target_binding.clone(),
                manifest,
            ))
        })
    }

    fn abort(self: Box<Self>) -> StoreFuture<'static, ()> {
        Box::pin(async move {
            let client = self.store.connect().await?;
            client
                .batch_execute(
                    "BEGIN ISOLATION LEVEL SERIALIZABLE;
                     SET LOCAL synchronous_commit = on",
                )
                .await
                .map_err(postgres_error)?;
            let result = async {
                verify_owner(&client, self.store.binding_ref(), true).await?;
                client
                    .execute(
                        "DELETE FROM snapshot_stage WHERE snapshot_id = $1",
                        &[&u128_bytes(self.header.snapshot_id().get())],
                    )
                    .await
                    .map_err(postgres_error)?;
                Ok(())
            }
            .await;
            finish_transaction(&client, result).await
        })
    }
}

pub(crate) async fn activate_candidate(
    store: &PostgresReplicaStore,
    candidate: LogicalSnapshotCandidateReceipt,
    active_binding: ReplicaBinding,
) -> Result<LogicalReplicaActivationReceipt, StorageError> {
    let receipt = LogicalReplicaActivationReceipt::new(&candidate, active_binding.clone())?;
    if store.binding_ref() != candidate.candidate_binding()
        && store.binding_ref() != &active_binding
    {
        return Err(StorageError::SnapshotIdentityMismatch);
    }
    let expected_install = SnapshotInstallMarker::new(
        candidate.candidate_binding(),
        candidate.header(),
        candidate.manifest(),
    )?;
    let expected_activation = SnapshotActivationMarker::new(&candidate, &receipt)?;
    let client = store.connect().await?;
    client
        .batch_execute(
            "BEGIN ISOLATION LEVEL SERIALIZABLE;
             SET LOCAL synchronous_commit = on",
        )
        .await
        .map_err(postgres_error)?;
    let result = async {
        let owner = load_owner(&client, true).await?;
        if owner == active_binding {
            let stored = load_activation_marker(&client, true)
                .await?
                .ok_or_else(|| {
                    StorageError::CorruptSnapshot(
                        "activated PostgreSQL namespace has no activation receipt".into(),
                    )
                })?;
            if stored == expected_activation {
                return Ok(receipt);
            }
            return Err(StorageError::SnapshotIdentityMismatch);
        }
        if owner != *candidate.candidate_binding() {
            return Err(StorageError::SnapshotIdentityMismatch);
        }
        if load_activation_marker(&client, true).await?.is_some() {
            return Err(StorageError::CorruptSnapshot(
                "candidate PostgreSQL namespace already has an activation receipt".into(),
            ));
        }
        let installed = load_install_marker(&client, true).await?.ok_or_else(|| {
            StorageError::CorruptSnapshot(
                "candidate PostgreSQL namespace has no complete install marker".into(),
            )
        })?;
        if installed != expected_install {
            return Err(StorageError::SnapshotIdentityMismatch);
        }
        if read_applied_index(&client).await? != candidate.header().applied_index() {
            return Err(StorageError::CorruptSnapshot(
                "candidate PostgreSQL applied index differs from its install marker".into(),
            ));
        }
        let updated = client
            .execute(
                "UPDATE replica_owner
                 SET binding_role = $1, binding_digest = $2
                 WHERE singleton = TRUE AND binding_digest = $3",
                &[
                    &role_tag(BindingRole::Active),
                    &active_binding.identity_digest().get().to_vec(),
                    &candidate
                        .candidate_binding()
                        .identity_digest()
                        .get()
                        .to_vec(),
                ],
            )
            .await
            .map_err(postgres_error)?;
        if updated != 1 {
            return Err(StorageError::SnapshotIdentityMismatch);
        }
        let removed = client
            .execute("DELETE FROM snapshot_install WHERE singleton = TRUE", &[])
            .await
            .map_err(postgres_error)?;
        if removed != 1 {
            return Err(StorageError::CorruptSnapshot(
                "candidate PostgreSQL install marker disappeared during activation".into(),
            ));
        }
        persist_activation_marker(&client, &expected_activation).await?;
        Ok(receipt)
    }
    .await;
    finish_transaction(&client, result).await
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SnapshotInstallMarker {
    candidate_binding_digest: Digest32,
    snapshot_id: u128,
    applied_index: u64,
    format_version: u32,
    chunk_count: u64,
    record_count: u64,
    content_digest: Digest32,
}

impl SnapshotInstallMarker {
    fn new(
        candidate_binding: &ReplicaBinding,
        header: &SnapshotHeader,
        manifest: &SnapshotManifest,
    ) -> Result<Self, StorageError> {
        if candidate_binding.role() != BindingRole::Candidate
            || header.snapshot_id() != manifest.snapshot_id()
            || header.format_version() != dtg_storage::SUPPORTED_SNAPSHOT_FORMAT_VERSION
        {
            return Err(StorageError::SnapshotIdentityMismatch);
        }
        Ok(Self {
            candidate_binding_digest: candidate_binding.identity_digest(),
            snapshot_id: header.snapshot_id().get(),
            applied_index: header.applied_index(),
            format_version: header.format_version(),
            chunk_count: manifest.chunk_count(),
            record_count: manifest.record_count(),
            content_digest: manifest.content_digest(),
        })
    }

    fn decode(row: &Row) -> Result<Self, StorageError> {
        Ok(Self {
            candidate_binding_digest: marker_digest(row.get::<_, Vec<u8>>(0).as_slice())?,
            snapshot_id: marker_u128(row.get::<_, Vec<u8>>(1).as_slice())?,
            applied_index: marker_u64(row.get::<_, Vec<u8>>(2).as_slice())?,
            format_version: marker_format(row.get(3))?,
            chunk_count: marker_u64(row.get::<_, Vec<u8>>(4).as_slice())?,
            record_count: marker_u64(row.get::<_, Vec<u8>>(5).as_slice())?,
            content_digest: marker_digest(row.get::<_, Vec<u8>>(6).as_slice())?,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SnapshotActivationMarker {
    install: SnapshotInstallMarker,
    active_binding_digest: Digest32,
}

impl SnapshotActivationMarker {
    fn new(
        candidate: &LogicalSnapshotCandidateReceipt,
        receipt: &LogicalReplicaActivationReceipt,
    ) -> Result<Self, StorageError> {
        Ok(Self {
            install: SnapshotInstallMarker::new(
                candidate.candidate_binding(),
                candidate.header(),
                candidate.manifest(),
            )?,
            active_binding_digest: receipt.active_binding().identity_digest(),
        })
    }

    fn decode(row: &Row) -> Result<Self, StorageError> {
        Ok(Self {
            install: SnapshotInstallMarker {
                candidate_binding_digest: marker_digest(row.get::<_, Vec<u8>>(0).as_slice())?,
                snapshot_id: marker_u128(row.get::<_, Vec<u8>>(2).as_slice())?,
                applied_index: marker_u64(row.get::<_, Vec<u8>>(3).as_slice())?,
                format_version: marker_format(row.get(4))?,
                chunk_count: marker_u64(row.get::<_, Vec<u8>>(5).as_slice())?,
                record_count: marker_u64(row.get::<_, Vec<u8>>(6).as_slice())?,
                content_digest: marker_digest(row.get::<_, Vec<u8>>(7).as_slice())?,
            },
            active_binding_digest: marker_digest(row.get::<_, Vec<u8>>(1).as_slice())?,
        })
    }
}

async fn verify_staged_chunks(
    client: &Client,
    header: &SnapshotHeader,
    chunks: &[SnapshotChunk],
) -> Result<(), StorageError> {
    let rows = client
        .query(
            "SELECT chunk_ordinal, chunk_digest, record_count
             FROM snapshot_stage WHERE snapshot_id = $1
             ORDER BY chunk_ordinal FOR UPDATE",
            &[&u128_bytes(header.snapshot_id().get())],
        )
        .await
        .map_err(postgres_error)?;
    if rows.len() != chunks.len() {
        return Err(StorageError::CorruptSnapshot(
            "PostgreSQL snapshot staging is incomplete".into(),
        ));
    }
    for (row, chunk) in rows.iter().zip(chunks) {
        let ordinal = marker_u64(row.get::<_, Vec<u8>>(0).as_slice())?;
        let digest = marker_digest(row.get::<_, Vec<u8>>(1).as_slice())?;
        let record_count: i64 = row.get(2);
        if ordinal != chunk.ordinal()
            || digest != chunk.digest
            || u64::try_from(record_count).ok() != Some(chunk.records().len() as u64)
        {
            return Err(StorageError::CorruptSnapshot(
                "PostgreSQL snapshot staging does not match submitted chunks".into(),
            ));
        }
    }
    Ok(())
}

async fn persist_install_marker(
    client: &Client,
    marker: &SnapshotInstallMarker,
) -> Result<(), StorageError> {
    client
        .execute(
            "INSERT INTO snapshot_install (
                singleton, candidate_binding_digest, snapshot_id, applied_index,
                format_version, chunk_count, record_count, content_digest
             ) VALUES (TRUE, $1, $2, $3, $4, $5, $6, $7)
             ON CONFLICT (singleton) DO UPDATE SET
                candidate_binding_digest = EXCLUDED.candidate_binding_digest,
                snapshot_id = EXCLUDED.snapshot_id,
                applied_index = EXCLUDED.applied_index,
                format_version = EXCLUDED.format_version,
                chunk_count = EXCLUDED.chunk_count,
                record_count = EXCLUDED.record_count,
                content_digest = EXCLUDED.content_digest",
            &[
                &marker.candidate_binding_digest.get().to_vec(),
                &u128_bytes(marker.snapshot_id),
                &u64_bytes(marker.applied_index),
                &i32::try_from(marker.format_version).map_err(|_| {
                    StorageError::CorruptSnapshot(
                        "snapshot format exceeds PostgreSQL INTEGER".into(),
                    )
                })?,
                &u64_bytes(marker.chunk_count),
                &u64_bytes(marker.record_count),
                &marker.content_digest.get().to_vec(),
            ],
        )
        .await
        .map_err(postgres_error)?;
    Ok(())
}

async fn load_install_marker(
    client: &Client,
    lock: bool,
) -> Result<Option<SnapshotInstallMarker>, StorageError> {
    let suffix = if lock { " FOR UPDATE" } else { "" };
    client
        .query_opt(
            &format!(
                "SELECT candidate_binding_digest, snapshot_id, applied_index, format_version,
                        chunk_count, record_count, content_digest
                 FROM snapshot_install WHERE singleton = TRUE{suffix}"
            ),
            &[],
        )
        .await
        .map_err(postgres_error)?
        .map(|row| SnapshotInstallMarker::decode(&row))
        .transpose()
}

async fn persist_activation_marker(
    client: &Client,
    marker: &SnapshotActivationMarker,
) -> Result<(), StorageError> {
    client
        .execute(
            "INSERT INTO snapshot_activation (
                singleton, candidate_binding_digest, active_binding_digest, snapshot_id,
                applied_index, format_version, chunk_count, record_count, content_digest
             ) VALUES (TRUE, $1, $2, $3, $4, $5, $6, $7, $8)",
            &[
                &marker.install.candidate_binding_digest.get().to_vec(),
                &marker.active_binding_digest.get().to_vec(),
                &u128_bytes(marker.install.snapshot_id),
                &u64_bytes(marker.install.applied_index),
                &i32::try_from(marker.install.format_version).map_err(|_| {
                    StorageError::CorruptSnapshot(
                        "snapshot format exceeds PostgreSQL INTEGER".into(),
                    )
                })?,
                &u64_bytes(marker.install.chunk_count),
                &u64_bytes(marker.install.record_count),
                &marker.install.content_digest.get().to_vec(),
            ],
        )
        .await
        .map_err(postgres_error)?;
    Ok(())
}

async fn load_activation_marker(
    client: &Client,
    lock: bool,
) -> Result<Option<SnapshotActivationMarker>, StorageError> {
    let suffix = if lock { " FOR UPDATE" } else { "" };
    client
        .query_opt(
            &format!(
                "SELECT candidate_binding_digest, active_binding_digest, snapshot_id,
                        applied_index, format_version, chunk_count, record_count, content_digest
                 FROM snapshot_activation WHERE singleton = TRUE{suffix}"
            ),
            &[],
        )
        .await
        .map_err(postgres_error)?
        .map(|row| SnapshotActivationMarker::decode(&row))
        .transpose()
}

fn marker_u64(bytes: &[u8]) -> Result<u64, StorageError> {
    decode_u64(bytes)
        .map_err(|_| StorageError::CorruptSnapshot("invalid PostgreSQL marker u64".into()))
}

fn marker_u128(bytes: &[u8]) -> Result<u128, StorageError> {
    Ok(u128::from_be_bytes(bytes.try_into().map_err(|_| {
        StorageError::CorruptSnapshot("invalid PostgreSQL marker u128".into())
    })?))
}

fn marker_digest(bytes: &[u8]) -> Result<Digest32, StorageError> {
    decode_digest(bytes)
        .map_err(|_| StorageError::CorruptSnapshot("invalid PostgreSQL marker digest".into()))
}

fn marker_format(value: i32) -> Result<u32, StorageError> {
    u32::try_from(value)
        .ok()
        .filter(|value| *value == dtg_storage::SUPPORTED_SNAPSHOT_FORMAT_VERSION)
        .ok_or_else(|| {
            StorageError::CorruptSnapshot("invalid PostgreSQL marker snapshot format".into())
        })
}

async fn clear_logical_state(client: &tokio_postgres::Client) -> Result<(), StorageError> {
    client
        .batch_execute(
            "DELETE FROM adjacency;
             DELETE FROM current_edge;
             DELETE FROM current_vertex;
             DELETE FROM edge_history;
             DELETE FROM vertex_history;
             DELETE FROM transaction_state;
             DELETE FROM replica_metadata;
             DELETE FROM replay_identity;
             DELETE FROM change_record;",
        )
        .await
        .map_err(postgres_error)
}

fn validate_restored_records(
    binding: &ReplicaBinding,
    applied_index: u64,
    records: &[SnapshotRecord],
) -> Result<Vec<CommittedShardBatch>, StorageError> {
    let mut supplied_graph = Vec::new();
    let mut supplied_transactions = BTreeMap::<TransactionId, TransactionRecord>::new();
    let mut supplied_metadata = BTreeMap::<String, ReplicaMetadata>::new();
    let mut replay = BTreeMap::<u64, SnapshotReplayRecord>::new();
    let mut changes = BTreeMap::<u64, Vec<ChangeRecord>>::new();
    for record in records {
        match record {
            SnapshotRecord::Vertex(vertex) => {
                supplied_graph.push(LogicalMutation::PutVertex(vertex.clone()));
            }
            SnapshotRecord::VertexTombstone(tombstone) => {
                supplied_graph.push(LogicalMutation::DeleteVertex(tombstone.clone()));
            }
            SnapshotRecord::Edge(edge) => {
                supplied_graph.push(LogicalMutation::PutEdge(edge.clone()));
            }
            SnapshotRecord::EdgeTombstone(tombstone) => {
                supplied_graph.push(LogicalMutation::DeleteEdge(tombstone.clone()));
            }
            SnapshotRecord::Transaction(transaction) => {
                if supplied_transactions
                    .insert(transaction.id(), transaction.clone())
                    .is_some()
                {
                    return Err(StorageError::CorruptSnapshot(
                        "snapshot transaction state contains duplicate identifiers".into(),
                    ));
                }
            }
            SnapshotRecord::ReplicaMetadata(metadata) => {
                if supplied_metadata
                    .insert(metadata.name().to_owned(), metadata.clone())
                    .is_some()
                {
                    return Err(StorageError::CorruptSnapshot(
                        "snapshot replica metadata contains duplicate names".into(),
                    ));
                }
            }
            SnapshotRecord::Replay(record) => {
                if replay.insert(record.raft_index(), record.clone()).is_some() {
                    return Err(StorageError::CorruptSnapshot(
                        "snapshot replay identities are duplicated".into(),
                    ));
                }
            }
            SnapshotRecord::Change(record) => {
                changes
                    .entry(record.raft_index())
                    .or_default()
                    .push(record.clone());
            }
        }
    }
    if replay.keys().copied().ne(1..=applied_index) {
        return Err(StorageError::CorruptSnapshot(
            "snapshot replay identities are not contiguous through the applied index".into(),
        ));
    }

    let mut authenticated_graph = Vec::new();
    let mut authenticated_transactions = BTreeMap::new();
    let mut authenticated_metadata = BTreeMap::new();
    let mut batches = Vec::new();
    for index in 1..=applied_index {
        let replay_record = replay
            .get(&index)
            .ok_or_else(|| StorageError::CorruptSnapshot("snapshot replay is missing".into()))?;
        let changes_at_index = changes.get_mut(&index).ok_or_else(|| {
            StorageError::CorruptSnapshot("snapshot committed batch has no changes".into())
        })?;
        changes_at_index.sort_by_key(ChangeRecord::mutation_ordinal);
        if changes_at_index
            .iter()
            .enumerate()
            .any(|(ordinal, change)| change.mutation_ordinal() != ordinal as u64)
        {
            return Err(StorageError::CorruptSnapshot(
                "snapshot change ordinals are not contiguous".into(),
            ));
        }
        let mutations = changes_at_index
            .iter()
            .map(|change| change.mutation().clone())
            .collect::<Vec<_>>();
        let batch = CommittedShardBatch::new(
            binding.clone(),
            replay_record.raft_term(),
            index,
            replay_record.command_id(),
            mutations,
        )
        .map_err(|error| {
            StorageError::CorruptSnapshot(format!(
                "snapshot committed batch cannot be reconstructed: {error}"
            ))
        })?;
        if batch.mutation_digest() != replay_record.mutation_digest() {
            return Err(StorageError::CorruptSnapshot(format!(
                "snapshot change digest does not match replay identity at Raft index {index}"
            )));
        }
        for mutation in batch.mutations() {
            match mutation {
                LogicalMutation::PutVertex(_)
                | LogicalMutation::DeleteVertex(_)
                | LogicalMutation::PutEdge(_)
                | LogicalMutation::DeleteEdge(_) => authenticated_graph.push(mutation.clone()),
                LogicalMutation::PutTransaction(transaction) => {
                    authenticated_transactions.insert(transaction.id(), transaction.clone());
                }
                LogicalMutation::PutReplicaMetadata(metadata) => {
                    authenticated_metadata.insert(metadata.name().to_owned(), metadata.clone());
                }
            }
        }
        batches.push(batch);
    }
    if changes.keys().copied().collect::<BTreeSet<_>>()
        != (1..=applied_index).collect::<BTreeSet<_>>()
    {
        return Err(StorageError::CorruptSnapshot(
            "snapshot changes contain an unknown replay identity".into(),
        ));
    }
    if mutation_multiset(&supplied_graph)? != mutation_multiset(&authenticated_graph)? {
        return Err(StorageError::CorruptSnapshot(
            "snapshot graph history does not match authenticated changes".into(),
        ));
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
    Ok(batches)
}

fn mutation_multiset(
    mutations: &[LogicalMutation],
) -> Result<BTreeMap<Vec<u8>, usize>, StorageError> {
    let mut multiset = BTreeMap::new();
    for mutation in mutations {
        *multiset.entry(encode_mutation(mutation)?).or_default() += 1;
    }
    Ok(multiset)
}

fn same_logical_identity(left: &ReplicaBinding, right: &ReplicaBinding) -> bool {
    left.cluster_id() == right.cluster_id()
        && left.graph_id() == right.graph_id()
        && left.shard_id() == right.shard_id()
}
