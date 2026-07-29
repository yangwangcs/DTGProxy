use dtg_storage::{
    BindingRole, CommittedShardBatch, Digest32, LogicalMutation, LogicalReplicaActivationReceipt,
    LogicalSnapshotCandidateReceipt, LogicalSnapshotReader, LogicalSnapshotWriter, ReadFence,
    ReplicaBinding, SnapshotChunk, SnapshotHeader, SnapshotManifest, SnapshotManifestBuilder,
    SnapshotRecord, SnapshotRequest, SnapshotRestoreReceipt, StorageError, StoreFuture,
};
use tokio_postgres::{Client, Row};

use crate::{
    PostgresReplicaStore,
    apply::{insert_replay, stage_mutation},
    codec::{decode_mutation, encode_mutation},
    config::postgres_error,
    read_view::{PostgresReadView, SnapshotPageKind},
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
    Ok(Box::new(PostgresSnapshotReader {
        manifest: SnapshotManifestBuilder::new(header.clone()),
        header,
        view,
        max_records_per_chunk: request.max_records_per_chunk() as usize,
        kind: SnapshotPageKind::Vertices,
        offset: 0,
        exhausted: false,
    }))
}

struct PostgresSnapshotReader {
    header: SnapshotHeader,
    manifest: SnapshotManifestBuilder,
    view: PostgresReadView,
    max_records_per_chunk: usize,
    kind: SnapshotPageKind,
    offset: i64,
    exhausted: bool,
}

impl LogicalSnapshotReader for PostgresSnapshotReader {
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
                let remaining = self.max_records_per_chunk - records.len();
                let page = self
                    .view
                    .snapshot_page(
                        self.kind,
                        self.offset,
                        i64::try_from(remaining).map_err(|_| {
                            StorageError::CorruptSnapshot(
                                "PostgreSQL snapshot chunk bound exceeds BIGINT".into(),
                            )
                        })?,
                    )
                    .await?;
                if page.is_empty() {
                    self.kind = self.kind.next();
                    self.offset = 0;
                    if matches!(self.kind, SnapshotPageKind::Done) {
                        self.exhausted = true;
                        break;
                    }
                    continue;
                }
                self.offset += i64::try_from(page.len()).map_err(|_| {
                    StorageError::CorruptSnapshot("PostgreSQL snapshot offset overflow".into())
                })?;
                records.extend(page);
            }
            if records.is_empty() {
                return Ok(None);
            }
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
        manifest: SnapshotManifestBuilder::new(header.clone()),
        header,
    }))
}

struct PostgresSnapshotWriter {
    store: PostgresReplicaStore,
    target_binding: ReplicaBinding,
    header: SnapshotHeader,
    manifest: SnapshotManifestBuilder,
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
                || chunk.ordinal() != self.manifest.next_ordinal()
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
                for (record_ordinal, record) in chunk.records().iter().enumerate() {
                    let staged = staged_record(record)?;
                    client
                        .execute(
                            "INSERT INTO snapshot_stage_record (
                           snapshot_id, chunk_ordinal, record_ordinal, record_kind,
                           mutation_payload, logical_key, raft_index, raft_term_or_ordinal,
                           command_id, digest
                         ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
                         ON CONFLICT (snapshot_id, chunk_ordinal, record_ordinal) DO UPDATE SET
                           record_kind=EXCLUDED.record_kind,
                           mutation_payload=EXCLUDED.mutation_payload,
                           logical_key=EXCLUDED.logical_key,
                           raft_index=EXCLUDED.raft_index,
                           raft_term_or_ordinal=EXCLUDED.raft_term_or_ordinal,
                           command_id=EXCLUDED.command_id,
                           digest=EXCLUDED.digest",
                            &[
                                &u128_bytes(chunk.snapshot_id().get()),
                                &u64_bytes(chunk.ordinal()),
                                &u64_bytes(record_ordinal as u64),
                                &staged.kind,
                                &staged.mutation,
                                &staged.logical_key,
                                &staged.raft_index,
                                &staged.raft_term_or_ordinal,
                                &staged.command_id,
                                &staged.digest,
                            ],
                        )
                        .await
                        .map_err(postgres_error)?;
                }
                Ok(())
            }
            .await;
            finish_transaction(&client, result).await?;
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
                    "PostgreSQL staged snapshot manifest mismatch".into(),
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
                verify_staged_chunks(&client, &self.header, &manifest).await?;
                validate_staged_state_sets(&client, self.header.snapshot_id().get(), &manifest)
                    .await?;
                clear_logical_state(&client).await?;
                let mut change_count = 0_u64;
                for raft_index in 1..=self.header.applied_index() {
                    let batch = load_staged_batch(
                        &client,
                        &self.target_binding,
                        self.header.snapshot_id().get(),
                        raft_index,
                    )
                    .await?;
                    change_count = change_count
                        .checked_add(batch.mutations().len() as u64)
                        .ok_or_else(|| {
                            StorageError::CorruptSnapshot(
                                "PostgreSQL staged change count overflow".into(),
                            )
                        })?;
                    for (ordinal, mutation) in batch.mutations().iter().enumerate() {
                        stage_mutation(&client, batch.raft_index(), ordinal as u64, mutation)
                            .await?;
                    }
                    insert_replay(&client, &batch).await?;
                }
                verify_staged_change_count(&client, self.header.snapshot_id().get(), change_count)
                    .await?;
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
                        "DELETE FROM snapshot_stage_record WHERE snapshot_id = $1",
                        &[&u128_bytes(self.header.snapshot_id().get())],
                    )
                    .await
                    .map_err(postgres_error)?;
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
                        "DELETE FROM snapshot_stage_record WHERE snapshot_id = $1",
                        &[&u128_bytes(self.header.snapshot_id().get())],
                    )
                    .await
                    .map_err(postgres_error)?;
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

struct StagedRecord {
    kind: i16,
    mutation: Option<Vec<u8>>,
    logical_key: Option<Vec<u8>>,
    raft_index: Option<Vec<u8>>,
    raft_term_or_ordinal: Option<Vec<u8>>,
    command_id: Option<Vec<u8>>,
    digest: Option<Vec<u8>>,
}

fn staged_record(record: &SnapshotRecord) -> Result<StagedRecord, StorageError> {
    let mutation = match record {
        SnapshotRecord::Vertex(value) => Some(LogicalMutation::PutVertex(value.clone())),
        SnapshotRecord::VertexTombstone(value) => {
            Some(LogicalMutation::DeleteVertex(value.clone()))
        }
        SnapshotRecord::Edge(value) => Some(LogicalMutation::PutEdge(value.clone())),
        SnapshotRecord::EdgeTombstone(value) => Some(LogicalMutation::DeleteEdge(value.clone())),
        SnapshotRecord::Transaction(value) => Some(LogicalMutation::PutTransaction(value.clone())),
        SnapshotRecord::ReplicaMetadata(value) => {
            Some(LogicalMutation::PutReplicaMetadata(value.clone()))
        }
        SnapshotRecord::Replay(_) | SnapshotRecord::Change(_) => None,
    };
    let (kind, raft_index, raft_term_or_ordinal, command_id, digest) = match record {
        SnapshotRecord::Vertex(_) => (1, None, None, None, None),
        SnapshotRecord::VertexTombstone(_) => (2, None, None, None, None),
        SnapshotRecord::Edge(_) => (3, None, None, None, None),
        SnapshotRecord::EdgeTombstone(_) => (4, None, None, None, None),
        SnapshotRecord::Transaction(_) => (5, None, None, None, None),
        SnapshotRecord::ReplicaMetadata(_) => (6, None, None, None, None),
        SnapshotRecord::Replay(value) => (
            7,
            Some(u64_bytes(value.raft_index())),
            Some(u64_bytes(value.raft_term())),
            Some(u128_bytes(value.command_id().get())),
            Some(value.mutation_digest().get().to_vec()),
        ),
        SnapshotRecord::Change(value) => (
            8,
            Some(u64_bytes(value.raft_index())),
            Some(u64_bytes(value.mutation_ordinal())),
            None,
            None,
        ),
    };
    Ok(StagedRecord {
        kind,
        mutation: mutation
            .as_ref()
            .or_else(|| match record {
                SnapshotRecord::Change(value) => Some(value.mutation()),
                _ => None,
            })
            .map(encode_mutation)
            .transpose()?,
        logical_key: mutation
            .as_ref()
            .or_else(|| match record {
                SnapshotRecord::Change(value) => Some(value.mutation()),
                _ => None,
            })
            .and_then(mutation_logical_key),
        raft_index,
        raft_term_or_ordinal,
        command_id,
        digest,
    })
}

fn mutation_logical_key(mutation: &LogicalMutation) -> Option<Vec<u8>> {
    match mutation {
        LogicalMutation::PutTransaction(value) => Some(value.id().get().to_be_bytes().to_vec()),
        LogicalMutation::PutReplicaMetadata(value) => Some(value.name().as_bytes().to_vec()),
        _ => None,
    }
}

async fn validate_staged_state_sets(
    client: &Client,
    snapshot_id: u128,
    manifest: &SnapshotManifest,
) -> Result<(), StorageError> {
    let row = client
        .query_one(
            "WITH staged AS (
               SELECT * FROM snapshot_stage_record WHERE snapshot_id = $1
             ), supplied_graph AS (
               SELECT mutation_payload, count(*) AS copies FROM staged
               WHERE record_kind BETWEEN 1 AND 4 GROUP BY mutation_payload
             ), authenticated_graph AS (
               SELECT mutation_payload, count(*) AS copies FROM staged
               WHERE record_kind = 8 AND get_byte(mutation_payload, 0) BETWEEN 1 AND 4
               GROUP BY mutation_payload
             ), supplied_transactions AS (
               SELECT logical_key, mutation_payload FROM staged WHERE record_kind = 5
             ), authenticated_transactions AS (
               SELECT DISTINCT ON (logical_key) logical_key, mutation_payload FROM staged
               WHERE record_kind = 8 AND get_byte(mutation_payload, 0) = 5
               ORDER BY logical_key, chunk_ordinal DESC, record_ordinal DESC
             ), supplied_metadata AS (
               SELECT logical_key, mutation_payload FROM staged WHERE record_kind = 6
             ), authenticated_metadata AS (
               SELECT DISTINCT ON (logical_key) logical_key, mutation_payload FROM staged
               WHERE record_kind = 8 AND get_byte(mutation_payload, 0) = 6
               ORDER BY logical_key, chunk_ordinal DESC, record_ordinal DESC
             )
             SELECT
               (SELECT count(*) FROM staged),
               NOT EXISTS ((SELECT * FROM supplied_graph EXCEPT SELECT * FROM authenticated_graph)
                 UNION ALL (SELECT * FROM authenticated_graph EXCEPT SELECT * FROM supplied_graph)),
               NOT EXISTS ((SELECT * FROM supplied_transactions EXCEPT SELECT * FROM authenticated_transactions)
                 UNION ALL (SELECT * FROM authenticated_transactions EXCEPT SELECT * FROM supplied_transactions)),
               NOT EXISTS ((SELECT * FROM supplied_metadata EXCEPT SELECT * FROM authenticated_metadata)
                 UNION ALL (SELECT * FROM authenticated_metadata EXCEPT SELECT * FROM supplied_metadata)),
               (SELECT count(*) = count(DISTINCT logical_key) FROM supplied_transactions),
               (SELECT count(*) = count(DISTINCT logical_key) FROM supplied_metadata)",
            &[&u128_bytes(snapshot_id)],
        )
        .await
        .map_err(postgres_error)?;
    let record_count: i64 = row.get(0);
    let valid = row.get::<_, bool>(1)
        && row.get::<_, bool>(2)
        && row.get::<_, bool>(3)
        && row.get::<_, bool>(4)
        && row.get::<_, bool>(5);
    if u64::try_from(record_count).ok() != Some(manifest.record_count()) || !valid {
        return Err(StorageError::CorruptSnapshot(
            "PostgreSQL staged snapshot state does not match authenticated changes".into(),
        ));
    }
    Ok(())
}

async fn load_staged_batch(
    client: &Client,
    binding: &ReplicaBinding,
    snapshot_id: u128,
    raft_index: u64,
) -> Result<CommittedShardBatch, StorageError> {
    let replay_rows = client
        .query(
            "SELECT raft_term_or_ordinal, command_id, digest
             FROM snapshot_stage_record
             WHERE snapshot_id = $1 AND record_kind = 7 AND raft_index = $2",
            &[&u128_bytes(snapshot_id), &u64_bytes(raft_index)],
        )
        .await
        .map_err(postgres_error)?;
    let [replay] = replay_rows.as_slice() else {
        return Err(StorageError::CorruptSnapshot(
            "PostgreSQL staged replay identity is missing or duplicated".into(),
        ));
    };
    let term = decode_u64(&required_stage_bytes(replay, 0, "replay term")?)?;
    let command_id = dtg_storage::CommandId::new(u128::from_be_bytes(
        required_stage_bytes(replay, 1, "replay command")?
            .as_slice()
            .try_into()
            .map_err(|_| StorageError::CorruptSnapshot("invalid replay command".into()))?,
    ))?;
    let expected_digest = decode_digest(&required_stage_bytes(replay, 2, "replay digest")?)?;
    let rows = client
        .query(
            "SELECT raft_term_or_ordinal, mutation_payload
             FROM snapshot_stage_record
             WHERE snapshot_id = $1 AND record_kind = 8 AND raft_index = $2
             ORDER BY raft_term_or_ordinal",
            &[&u128_bytes(snapshot_id), &u64_bytes(raft_index)],
        )
        .await
        .map_err(postgres_error)?;
    let mut mutations = Vec::with_capacity(rows.len());
    for (expected_ordinal, row) in rows.iter().enumerate() {
        let ordinal = decode_u64(&required_stage_bytes(row, 0, "change ordinal")?)?;
        if ordinal != expected_ordinal as u64 {
            return Err(StorageError::CorruptSnapshot(
                "PostgreSQL staged change ordinals are not contiguous".into(),
            ));
        }
        mutations.push(decode_mutation(&required_stage_bytes(
            row,
            1,
            "change payload",
        )?)?);
    }
    let batch = CommittedShardBatch::new(binding.clone(), term, raft_index, command_id, mutations)
        .map_err(|error| {
            StorageError::CorruptSnapshot(format!(
                "PostgreSQL staged batch cannot be reconstructed: {error}"
            ))
        })?;
    if batch.mutation_digest() != expected_digest {
        return Err(StorageError::CorruptSnapshot(
            "PostgreSQL staged batch digest mismatch".into(),
        ));
    }
    Ok(batch)
}

async fn verify_staged_change_count(
    client: &Client,
    snapshot_id: u128,
    expected: u64,
) -> Result<(), StorageError> {
    let row = client
        .query_one(
            "SELECT count(*) FROM snapshot_stage_record
             WHERE snapshot_id = $1 AND record_kind = 8",
            &[&u128_bytes(snapshot_id)],
        )
        .await
        .map_err(postgres_error)?;
    let actual: i64 = row.get(0);
    if u64::try_from(actual).ok() != Some(expected) {
        return Err(StorageError::CorruptSnapshot(
            "PostgreSQL staged snapshot contains changes outside the applied prefix".into(),
        ));
    }
    Ok(())
}

fn required_stage_bytes(row: &Row, column: usize, name: &str) -> Result<Vec<u8>, StorageError> {
    row.get::<_, Option<Vec<u8>>>(column)
        .ok_or_else(|| StorageError::CorruptSnapshot(format!("missing PostgreSQL {name}")))
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
    manifest: &SnapshotManifest,
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
    if rows.len() as u64 != manifest.chunk_count() {
        return Err(StorageError::CorruptSnapshot(
            "PostgreSQL snapshot staging is incomplete".into(),
        ));
    }
    for (expected_ordinal, row) in rows.iter().enumerate() {
        let ordinal = marker_u64(row.get::<_, Vec<u8>>(0).as_slice())?;
        let digest = marker_digest(row.get::<_, Vec<u8>>(1).as_slice())?;
        let record_count: i64 = row.get(2);
        if ordinal != expected_ordinal as u64 || digest.get() == [0; 32] || record_count <= 0 {
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

fn same_logical_identity(left: &ReplicaBinding, right: &ReplicaBinding) -> bool {
    left.cluster_id() == right.cluster_id()
        && left.graph_id() == right.graph_id()
        && left.shard_id() == right.shard_id()
}
