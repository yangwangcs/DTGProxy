use base64::{Engine as _, engine::general_purpose::STANDARD};
use dtg_storage::{
    BindingRole, CommittedShardBatch, LogicalMutation, LogicalReplicaActivationReceipt,
    LogicalSnapshotCandidateReceipt, LogicalSnapshotReader, LogicalSnapshotWriter, ReadFence,
    ReplicaBinding, SnapshotChunk, SnapshotHeader, SnapshotManifest, SnapshotManifestBuilder,
    SnapshotRecord, SnapshotRequest, SnapshotRestoreReceipt, StorageError, StoreFuture,
};
use serde_json::Value;

use crate::{
    Neo4jReplicaStore,
    apply::{decode_payload, insert_replay, stage_mutation},
    codec::{encode_mutation, mutation_kind},
    config::QueryApiTransaction,
    read_view::{Neo4jReadView, SnapshotPageKind},
    schema::{
        begin_fenced_transaction, decode_digest, decode_u64_hex, decode_u128_hex, digest_text,
        fenced_parameters, owner_parameters, read_applied_index, text, u64_hex, u128_hex,
    },
};

const CANDIDATE_SNAPSHOT_PUBLISH_QUERY: &str = "MATCH (owner:DtgOwner {
       namespace_id: $namespace_id,
       backend_generation: $backend_generation,
       binding_digest: $binding_digest
     })
     SET owner.applied_index = $snapshot_applied_index
     WITH owner
     OPTIONAL MATCH (stage:DtgSnapshotStage {
       namespace_id: $namespace_id,
       backend_generation: $backend_generation,
       restore_id: $snapshot_id
     })
     OPTIONAL MATCH (record:DtgSnapshotRecord {
       namespace_id: $namespace_id,
       backend_generation: $backend_generation,
       restore_id: $snapshot_id
     })
     DETACH DELETE stage, record
     WITH DISTINCT owner
     MERGE (install:DtgSnapshotInstall {
       namespace_id: $namespace_id,
       backend_generation: $backend_generation
     })
     SET install.candidate_binding_digest = $candidate_binding_digest,
       install.snapshot_format_version = $snapshot_format_version,
       install.snapshot_id = $snapshot_id,
       install.snapshot_applied_index = $snapshot_applied_index,
       install.snapshot_chunk_count = $snapshot_chunk_count,
       install.snapshot_record_count = $snapshot_record_count,
       install.snapshot_content_digest = $snapshot_content_digest
     RETURN owner.applied_index";

const ACTIVE_SNAPSHOT_PUBLISH_QUERY: &str = "MATCH (owner:DtgOwner {
       namespace_id: $namespace_id,
       backend_generation: $backend_generation,
       binding_digest: $binding_digest
     })
     SET owner.applied_index = $snapshot_applied_index
     WITH owner
     OPTIONAL MATCH (stage:DtgSnapshotStage {
       namespace_id: $namespace_id,
       backend_generation: $backend_generation,
       restore_id: $snapshot_id
     })
     OPTIONAL MATCH (record:DtgSnapshotRecord {
       namespace_id: $namespace_id,
       backend_generation: $backend_generation,
       restore_id: $snapshot_id
     })
     DETACH DELETE stage, record
     WITH DISTINCT owner
     OPTIONAL MATCH (install:DtgSnapshotInstall {
       namespace_id: $namespace_id,
       backend_generation: $backend_generation
     })
     DETACH DELETE install
     WITH DISTINCT owner
     RETURN owner.applied_index";

const ACTIVATE_CANDIDATE_QUERY: &str = "MATCH (owner:DtgOwner {namespace_id: $namespace_id})
     CALL {
       WITH owner
       WHERE owner.backend_generation = $backend_generation
         AND owner.cluster_id = $cluster_id
         AND owner.graph_id = $graph_id
         AND owner.shard_id = $shard_id
         AND owner.placement_epoch = $placement_epoch
         AND owner.replica_id = $replica_id
         AND owner.backend_class_digest = $backend_class_digest
         AND owner.provider_kind = $provider_kind
         AND owner.contract_version = $contract_version
         AND owner.layout_version = $layout_version
         AND owner.capability_digest = $capability_digest
         AND owner.endpoint_profile_ref = $endpoint_profile_ref
         AND owner.credential_ref = $credential_ref
         AND owner.binding_role = $binding_role
         AND owner.binding_digest = $binding_digest
       MATCH (install:DtgSnapshotInstall {
         namespace_id: $namespace_id,
         backend_generation: $backend_generation,
         candidate_binding_digest: $candidate_binding_digest,
         snapshot_format_version: $snapshot_format_version,
         snapshot_id: $snapshot_id,
         snapshot_applied_index: $snapshot_applied_index,
         snapshot_chunk_count: $snapshot_chunk_count,
         snapshot_record_count: $snapshot_record_count,
         snapshot_content_digest: $snapshot_content_digest
       })
       OPTIONAL MATCH (existing:DtgSnapshotActivation {
         namespace_id: $namespace_id,
         backend_generation: $backend_generation
       })
       WITH owner, install, existing
       WHERE existing IS NULL OR (
         existing.candidate_binding_digest = $candidate_binding_digest
         AND existing.active_binding_digest = $active_binding_digest
         AND existing.snapshot_format_version = $snapshot_format_version
         AND existing.snapshot_id = $snapshot_id
         AND existing.snapshot_applied_index = $snapshot_applied_index
         AND existing.snapshot_content_digest = $snapshot_content_digest
       )
       SET owner.binding_role = $active_binding_role,
         owner.binding_digest = $active_binding_digest
       DELETE install
       MERGE (activation:DtgSnapshotActivation {
         namespace_id: $namespace_id,
         backend_generation: $backend_generation
       })
       ON CREATE SET
         activation.candidate_binding_digest = $candidate_binding_digest,
         activation.active_binding_digest = $active_binding_digest,
         activation.snapshot_format_version = $snapshot_format_version,
         activation.snapshot_id = $snapshot_id,
         activation.snapshot_applied_index = $snapshot_applied_index,
         activation.snapshot_content_digest = $snapshot_content_digest
       RETURN activation.active_binding_digest AS active_binding_digest,
         activation.snapshot_id AS snapshot_id,
         activation.snapshot_applied_index AS snapshot_applied_index,
         activation.snapshot_content_digest AS snapshot_content_digest,
         activation.snapshot_format_version AS snapshot_format_version
       UNION
       WITH owner
       WHERE owner.backend_generation = $backend_generation
         AND owner.cluster_id = $cluster_id
         AND owner.graph_id = $graph_id
         AND owner.shard_id = $shard_id
         AND owner.placement_epoch = $placement_epoch
         AND owner.replica_id = $replica_id
         AND owner.backend_class_digest = $backend_class_digest
         AND owner.provider_kind = $provider_kind
         AND owner.contract_version = $contract_version
         AND owner.layout_version = $layout_version
         AND owner.capability_digest = $capability_digest
         AND owner.endpoint_profile_ref = $endpoint_profile_ref
         AND owner.credential_ref = $credential_ref
         AND owner.binding_role = $active_binding_role
         AND owner.binding_digest = $active_binding_digest
       MATCH (activation:DtgSnapshotActivation {
         namespace_id: $namespace_id,
         backend_generation: $backend_generation,
         candidate_binding_digest: $candidate_binding_digest,
         active_binding_digest: $active_binding_digest,
         snapshot_format_version: $snapshot_format_version,
         snapshot_id: $snapshot_id,
         snapshot_applied_index: $snapshot_applied_index,
         snapshot_content_digest: $snapshot_content_digest
       })
       RETURN activation.active_binding_digest AS active_binding_digest,
         activation.snapshot_id AS snapshot_id,
         activation.snapshot_applied_index AS snapshot_applied_index,
         activation.snapshot_content_digest AS snapshot_content_digest,
         activation.snapshot_format_version AS snapshot_format_version
     }
     RETURN active_binding_digest, snapshot_id, snapshot_applied_index,
       snapshot_content_digest, snapshot_format_version";

pub(crate) async fn snapshot_reader(
    store: &Neo4jReplicaStore,
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
    Ok(Box::new(Neo4jSnapshotReader {
        manifest: SnapshotManifestBuilder::new(header.clone()),
        header,
        view,
        max_records_per_chunk: request.max_records_per_chunk() as usize,
        kind: SnapshotPageKind::History,
        offset: 0,
        exhausted: false,
    }))
}

struct Neo4jSnapshotReader {
    header: SnapshotHeader,
    manifest: SnapshotManifestBuilder,
    view: Neo4jReadView,
    max_records_per_chunk: usize,
    kind: SnapshotPageKind,
    offset: usize,
    exhausted: bool,
}

impl LogicalSnapshotReader for Neo4jSnapshotReader {
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
                    .snapshot_page(self.kind, self.offset, remaining)
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
                self.offset = self.offset.checked_add(page.len()).ok_or_else(|| {
                    StorageError::CorruptSnapshot("Neo4j snapshot offset overflow".into())
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
    store: &Neo4jReplicaStore,
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
    let client = store.client()?;
    read_applied_index(&client, store.binding_ref()).await?;
    Ok(Box::new(Neo4jSnapshotWriter {
        store: store.clone(),
        target_binding: store.binding_ref().clone(),
        manifest: SnapshotManifestBuilder::new(header.clone()),
        header,
    }))
}

struct Neo4jSnapshotWriter {
    store: Neo4jReplicaStore,
    target_binding: ReplicaBinding,
    header: SnapshotHeader,
    manifest: SnapshotManifestBuilder,
}

impl LogicalSnapshotWriter for Neo4jSnapshotWriter {
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
            let client = self.store.client()?;
            let mut parameters = fenced_parameters(self.store.binding_ref());
            parameters.insert(
                "restore_id".into(),
                Value::String(u128_hex(chunk.snapshot_id().get())),
            );
            parameters.insert("ordinal".into(), Value::String(u64_hex(chunk.ordinal())));
            parameters.insert(
                "chunk_digest".into(),
                Value::String(crate::schema::digest_text(chunk.digest)),
            );
            parameters.insert(
                "record_count".into(),
                Value::from(u64::try_from(chunk.records().len()).map_err(|_| {
                    StorageError::CorruptSnapshot(
                        "snapshot chunk record count exceeds Neo4j INTEGER".into(),
                    )
                })?),
            );
            let rows = client
                .execute(
                    "MATCH (owner:DtgOwner {
                      namespace_id: $namespace_id,
                      backend_generation: $backend_generation,
                      binding_digest: $binding_digest
                    })
                    MERGE (stage:DtgSnapshotStage {
                      namespace_id: $namespace_id,
                      backend_generation: $backend_generation,
                      restore_id: $restore_id, ordinal: $ordinal
                    })
                    SET stage.chunk_digest = $chunk_digest,
                      stage.record_count = $record_count
                    RETURN stage.ordinal",
                    Value::Object(parameters),
                )
                .await?;
            if rows.len() != 1 {
                return Err(StorageError::Internal(
                    "Neo4j snapshot stage lost its owner fence".into(),
                ));
            }
            for (record_ordinal, record) in chunk.records().iter().enumerate() {
                let staged = staged_record(record)?;
                let mut parameters = fenced_parameters(self.store.binding_ref());
                parameters.insert(
                    "restore_id".into(),
                    Value::String(u128_hex(chunk.snapshot_id().get())),
                );
                parameters.insert(
                    "chunk_ordinal".into(),
                    Value::String(u64_hex(chunk.ordinal())),
                );
                parameters.insert(
                    "record_ordinal".into(),
                    Value::String(u64_hex(record_ordinal as u64)),
                );
                parameters.insert("record_kind".into(), Value::from(staged.kind));
                parameters.insert(
                    "mutation_kind".into(),
                    staged.mutation_kind.map(Value::from).unwrap_or(Value::Null),
                );
                parameters.insert(
                    "logical_key".into(),
                    staged.logical_key.map(Value::String).unwrap_or(Value::Null),
                );
                parameters.insert(
                    "payload".into(),
                    staged.payload.map(Value::String).unwrap_or(Value::Null),
                );
                parameters.insert(
                    "raft_index".into(),
                    staged
                        .raft_index
                        .map(|v| Value::String(u64_hex(v)))
                        .unwrap_or(Value::Null),
                );
                parameters.insert(
                    "raft_term_or_ordinal".into(),
                    staged
                        .raft_term_or_ordinal
                        .map(|v| Value::String(u64_hex(v)))
                        .unwrap_or(Value::Null),
                );
                parameters.insert(
                    "command_id".into(),
                    staged
                        .command_id
                        .map(|v| Value::String(u128_hex(v)))
                        .unwrap_or(Value::Null),
                );
                parameters.insert(
                    "digest".into(),
                    staged
                        .digest
                        .map(|v| Value::String(crate::schema::digest_text(v)))
                        .unwrap_or(Value::Null),
                );
                let rows = client.execute(
                    "MATCH (owner:DtgOwner {namespace_id:$namespace_id, backend_generation:$backend_generation, binding_digest:$binding_digest})
                     MERGE (record:DtgSnapshotRecord {namespace_id:$namespace_id, backend_generation:$backend_generation, restore_id:$restore_id, chunk_ordinal:$chunk_ordinal, record_ordinal:$record_ordinal})
                     SET record.record_kind=$record_kind, record.mutation_kind=$mutation_kind,
                         record.logical_key=$logical_key, record.payload=$payload,
                         record.raft_index=$raft_index, record.raft_term_or_ordinal=$raft_term_or_ordinal,
                         record.command_id=$command_id, record.digest=$digest
                     RETURN record.record_ordinal",
                    Value::Object(parameters),
                ).await?;
                if rows.len() != 1 {
                    return Err(StorageError::Internal(
                        "Neo4j snapshot record stage lost its owner fence".into(),
                    ));
                }
            }
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
                    "Neo4j staged snapshot manifest mismatch".into(),
                ));
            }
            let _guard = self.store.inner.apply_guard.lock().await;
            let client = self.store.client()?;
            let (transaction, _) =
                begin_fenced_transaction(&client, self.store.binding_ref(), true).await?;
            let result = async {
                validate_staged_state_sets(
                    &transaction,
                    self.store.binding_ref(),
                    self.header.snapshot_id().get(),
                    &manifest,
                )
                .await?;
                clear_logical_state(&transaction, self.store.binding_ref()).await?;
                let mut change_count = 0_u64;
                for raft_index in 1..=self.header.applied_index() {
                    let batch = load_staged_batch(
                        &transaction,
                        self.store.binding_ref(),
                        self.header.snapshot_id().get(),
                        raft_index,
                    )
                    .await?;
                    change_count = change_count
                        .checked_add(batch.mutations().len() as u64)
                        .ok_or_else(|| {
                            StorageError::CorruptSnapshot(
                                "Neo4j staged change count overflow".into(),
                            )
                        })?;
                    for (ordinal, mutation) in batch.mutations().iter().enumerate() {
                        stage_mutation(
                            &transaction,
                            self.store.binding_ref(),
                            batch.raft_index(),
                            ordinal as u64,
                            mutation,
                        )
                        .await?;
                    }
                    insert_replay(&transaction, &batch).await?;
                }
                verify_staged_change_count(
                    &transaction,
                    self.store.binding_ref(),
                    self.header.snapshot_id().get(),
                    change_count,
                )
                .await?;
                let parameters =
                    snapshot_publish_parameters(self.store.binding_ref(), &self.header, &manifest);
                let publish_query = match self.target_binding.role() {
                    BindingRole::Candidate => CANDIDATE_SNAPSHOT_PUBLISH_QUERY,
                    BindingRole::Active => ACTIVE_SNAPSHOT_PUBLISH_QUERY,
                    BindingRole::Retiring => {
                        return Err(StorageError::SnapshotIdentityMismatch);
                    }
                };
                let rows = transaction
                    .execute(publish_query, Value::Object(parameters))
                    .await?;
                if rows.len() != 1 {
                    return Err(StorageError::Internal(
                        "Neo4j snapshot publish lost its owner fence".into(),
                    ));
                }
                Ok(())
            }
            .await;
            match result {
                Ok(()) => transaction.commit().await?,
                Err(error) => {
                    let _ = transaction.rollback().await;
                    return Err(error);
                }
            }
            Ok(SnapshotRestoreReceipt::new(
                self.target_binding.clone(),
                manifest,
            ))
        })
    }

    fn abort(self: Box<Self>) -> StoreFuture<'static, ()> {
        Box::pin(async move {
            let client = self.store.client()?;
            let mut parameters = fenced_parameters(self.store.binding_ref());
            parameters.insert(
                "restore_id".into(),
                Value::String(u128_hex(self.header.snapshot_id().get())),
            );
            let rows = client
                .execute(
                    "MATCH (owner:DtgOwner {
                      namespace_id: $namespace_id,
                      backend_generation: $backend_generation,
                      binding_digest: $binding_digest
                    })
                    OPTIONAL MATCH (stage:DtgSnapshotStage {
                      namespace_id: $namespace_id,
                      backend_generation: $backend_generation,
                      restore_id: $restore_id
                    })
                    OPTIONAL MATCH (record:DtgSnapshotRecord {
                      namespace_id: $namespace_id,
                      backend_generation: $backend_generation,
                      restore_id: $restore_id
                    })
                    DETACH DELETE stage, record
                    WITH DISTINCT owner
                    RETURN owner.namespace_id",
                    Value::Object(parameters),
                )
                .await?;
            if rows.len() != 1 {
                return Err(StorageError::Internal(
                    "Neo4j snapshot abort lost its owner fence".into(),
                ));
            }
            Ok(())
        })
    }
}

struct StagedRecord {
    kind: i64,
    mutation_kind: Option<i64>,
    logical_key: Option<String>,
    payload: Option<String>,
    raft_index: Option<u64>,
    raft_term_or_ordinal: Option<u64>,
    command_id: Option<u128>,
    digest: Option<dtg_storage::Digest32>,
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
        SnapshotRecord::Replay(_) => None,
        SnapshotRecord::Change(value) => Some(value.mutation().clone()),
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
            Some(value.raft_index()),
            Some(value.raft_term()),
            Some(value.command_id().get()),
            Some(value.mutation_digest()),
        ),
        SnapshotRecord::Change(value) => (
            8,
            Some(value.raft_index()),
            Some(value.mutation_ordinal()),
            None,
            None,
        ),
    };
    Ok(StagedRecord {
        kind,
        mutation_kind: mutation
            .as_ref()
            .map(|value| i64::from(mutation_kind(value))),
        logical_key: mutation.as_ref().and_then(|value| match value {
            LogicalMutation::PutTransaction(record) => Some(u128_hex(record.id().get())),
            LogicalMutation::PutReplicaMetadata(record) => Some(record.name().to_owned()),
            _ => None,
        }),
        payload: mutation
            .as_ref()
            .map(encode_mutation)
            .transpose()?
            .map(|bytes| STANDARD.encode(bytes)),
        raft_index,
        raft_term_or_ordinal,
        command_id,
        digest,
    })
}

async fn validate_staged_state_sets(
    transaction: &QueryApiTransaction,
    binding: &ReplicaBinding,
    snapshot_id: u128,
    manifest: &SnapshotManifest,
) -> Result<(), StorageError> {
    let mut parameters = fenced_parameters(binding);
    parameters.insert("restore_id".into(), Value::String(u128_hex(snapshot_id)));
    let rows = transaction.execute(
        "MATCH (owner:DtgOwner {namespace_id:$namespace_id, backend_generation:$backend_generation, binding_digest:$binding_digest})
         MATCH (record:DtgSnapshotRecord {namespace_id:$namespace_id, backend_generation:$backend_generation, restore_id:$restore_id})
         RETURN count(record)",
        Value::Object(parameters.clone()),
    ).await?;
    let count = rows
        .first()
        .and_then(|row| row.first())
        .and_then(Value::as_u64)
        .ok_or_else(|| {
            StorageError::CorruptSnapshot("Neo4j staged snapshot count is missing".into())
        })?;
    if count != manifest.record_count() {
        return Err(StorageError::CorruptSnapshot(
            "Neo4j staged snapshot record count mismatch".into(),
        ));
    }
    for query in [
        "MATCH (owner:DtgOwner {namespace_id:$namespace_id, backend_generation:$backend_generation, binding_digest:$binding_digest}) MATCH (supplied:DtgSnapshotRecord {namespace_id:$namespace_id, backend_generation:$backend_generation, restore_id:$restore_id}) WHERE supplied.record_kind IN [1,2,3,4] WITH owner, supplied.payload AS payload, count(supplied) AS copies OPTIONAL MATCH (authenticated:DtgSnapshotRecord {namespace_id:$namespace_id, backend_generation:$backend_generation, restore_id:$restore_id, record_kind:8, payload:payload}) WHERE authenticated.mutation_kind IN [1,2,3,4] WITH copies, count(authenticated) AS actual WHERE copies <> actual RETURN count(*)",
        "MATCH (owner:DtgOwner {namespace_id:$namespace_id, backend_generation:$backend_generation, binding_digest:$binding_digest}) MATCH (authenticated:DtgSnapshotRecord {namespace_id:$namespace_id, backend_generation:$backend_generation, restore_id:$restore_id, record_kind:8}) WHERE authenticated.mutation_kind IN [1,2,3,4] WITH owner, authenticated.payload AS payload, count(authenticated) AS copies OPTIONAL MATCH (supplied:DtgSnapshotRecord {namespace_id:$namespace_id, backend_generation:$backend_generation, restore_id:$restore_id, payload:payload}) WHERE supplied.record_kind IN [1,2,3,4] WITH copies, count(supplied) AS actual WHERE copies <> actual RETURN count(*)",
    ] {
        require_zero_count(transaction, query, parameters.clone(), "graph history").await?;
    }
    for (record_kind, mutation_kind_value, name) in [
        (5_i64, 5_i64, "transaction state"),
        (6, 6, "metadata state"),
    ] {
        let mut typed = parameters.clone();
        typed.insert("record_kind".into(), Value::from(record_kind));
        typed.insert("mutation_kind".into(), Value::from(mutation_kind_value));
        require_zero_count(
            transaction,
            "MATCH (owner:DtgOwner {namespace_id:$namespace_id, backend_generation:$backend_generation, binding_digest:$binding_digest}) MATCH (supplied:DtgSnapshotRecord {namespace_id:$namespace_id, backend_generation:$backend_generation, restore_id:$restore_id, record_kind:$record_kind}) WITH owner, supplied.logical_key AS logical_key, count(supplied) AS copies WHERE copies <> 1 RETURN count(*)",
            typed.clone(),
            name,
        ).await?;
        require_zero_count(
            transaction,
            "MATCH (owner:DtgOwner {namespace_id:$namespace_id, backend_generation:$backend_generation, binding_digest:$binding_digest}) MATCH (supplied:DtgSnapshotRecord {namespace_id:$namespace_id, backend_generation:$backend_generation, restore_id:$restore_id, record_kind:$record_kind}) OPTIONAL MATCH (authenticated:DtgSnapshotRecord {namespace_id:$namespace_id, backend_generation:$backend_generation, restore_id:$restore_id, record_kind:8, mutation_kind:$mutation_kind, logical_key:supplied.logical_key}) WITH supplied, authenticated ORDER BY authenticated.chunk_ordinal DESC, authenticated.record_ordinal DESC WITH supplied, head(collect(authenticated)) AS latest WHERE latest IS NULL OR latest.payload <> supplied.payload RETURN count(*)",
            typed.clone(),
            name,
        ).await?;
        require_zero_count(
            transaction,
            "MATCH (owner:DtgOwner {namespace_id:$namespace_id, backend_generation:$backend_generation, binding_digest:$binding_digest}) MATCH (authenticated:DtgSnapshotRecord {namespace_id:$namespace_id, backend_generation:$backend_generation, restore_id:$restore_id, record_kind:8, mutation_kind:$mutation_kind}) WITH owner, authenticated.logical_key AS logical_key OPTIONAL MATCH (supplied:DtgSnapshotRecord {namespace_id:$namespace_id, backend_generation:$backend_generation, restore_id:$restore_id, record_kind:$record_kind, logical_key:logical_key}) WITH logical_key, count(supplied) AS copies WHERE copies = 0 RETURN count(*)",
            typed,
            name,
        ).await?;
    }
    Ok(())
}

async fn require_zero_count(
    transaction: &QueryApiTransaction,
    query: &str,
    parameters: serde_json::Map<String, Value>,
    name: &str,
) -> Result<(), StorageError> {
    let rows = transaction
        .execute(query, Value::Object(parameters))
        .await?;
    let count = rows
        .first()
        .and_then(|row| row.first())
        .and_then(Value::as_u64)
        .ok_or_else(|| {
            StorageError::CorruptSnapshot(format!("Neo4j staged {name} validation is missing"))
        })?;
    if count != 0 {
        return Err(StorageError::CorruptSnapshot(format!(
            "Neo4j staged {name} does not match authenticated changes"
        )));
    }
    Ok(())
}

async fn load_staged_batch(
    transaction: &QueryApiTransaction,
    binding: &ReplicaBinding,
    snapshot_id: u128,
    raft_index: u64,
) -> Result<CommittedShardBatch, StorageError> {
    let mut parameters = fenced_parameters(binding);
    parameters.insert("restore_id".into(), Value::String(u128_hex(snapshot_id)));
    parameters.insert("raft_index".into(), Value::String(u64_hex(raft_index)));
    let replay_rows = transaction.execute(
        "MATCH (owner:DtgOwner {namespace_id:$namespace_id, backend_generation:$backend_generation, binding_digest:$binding_digest}) MATCH (record:DtgSnapshotRecord {namespace_id:$namespace_id, backend_generation:$backend_generation, restore_id:$restore_id, record_kind:7, raft_index:$raft_index}) RETURN record.raft_term_or_ordinal, record.command_id, record.digest",
        Value::Object(parameters.clone()),
    ).await?;
    let [replay] = replay_rows.as_slice() else {
        return Err(StorageError::CorruptSnapshot(
            "Neo4j staged replay identity is missing or duplicated".into(),
        ));
    };
    let term = decode_u64_hex(text(&replay[0], "staged replay term")?)?;
    let command_id =
        dtg_storage::CommandId::new(decode_u128_hex(text(&replay[1], "staged replay command")?)?)?;
    let expected_digest = decode_digest(text(&replay[2], "staged replay digest")?)?;
    let rows = transaction.execute(
        "MATCH (owner:DtgOwner {namespace_id:$namespace_id, backend_generation:$backend_generation, binding_digest:$binding_digest}) MATCH (record:DtgSnapshotRecord {namespace_id:$namespace_id, backend_generation:$backend_generation, restore_id:$restore_id, record_kind:8, raft_index:$raft_index}) RETURN record.raft_term_or_ordinal, record.payload ORDER BY record.raft_term_or_ordinal",
        Value::Object(parameters),
    ).await?;
    let mut mutations = Vec::with_capacity(rows.len());
    for (expected_ordinal, row) in rows.iter().enumerate() {
        let ordinal = decode_u64_hex(text(&row[0], "staged change ordinal")?)?;
        if ordinal != expected_ordinal as u64 {
            return Err(StorageError::CorruptSnapshot(
                "Neo4j staged change ordinals are not contiguous".into(),
            ));
        }
        mutations.push(decode_payload(&row[1])?);
    }
    let batch = CommittedShardBatch::new(binding.clone(), term, raft_index, command_id, mutations)
        .map_err(|error| {
            StorageError::CorruptSnapshot(format!(
                "Neo4j staged batch cannot be reconstructed: {error}"
            ))
        })?;
    if batch.mutation_digest() != expected_digest {
        return Err(StorageError::CorruptSnapshot(
            "Neo4j staged batch digest mismatch".into(),
        ));
    }
    Ok(batch)
}

async fn verify_staged_change_count(
    transaction: &QueryApiTransaction,
    binding: &ReplicaBinding,
    snapshot_id: u128,
    expected: u64,
) -> Result<(), StorageError> {
    let mut parameters = fenced_parameters(binding);
    parameters.insert("restore_id".into(), Value::String(u128_hex(snapshot_id)));
    let rows = transaction.execute(
        "MATCH (owner:DtgOwner {namespace_id:$namespace_id, backend_generation:$backend_generation, binding_digest:$binding_digest}) MATCH (record:DtgSnapshotRecord {namespace_id:$namespace_id, backend_generation:$backend_generation, restore_id:$restore_id, record_kind:8}) RETURN count(record)",
        Value::Object(parameters),
    ).await?;
    let actual = rows
        .first()
        .and_then(|row| row.first())
        .and_then(Value::as_u64)
        .ok_or_else(|| {
            StorageError::CorruptSnapshot("Neo4j staged change count is missing".into())
        })?;
    if actual != expected {
        return Err(StorageError::CorruptSnapshot(
            "Neo4j staged snapshot contains changes outside the applied prefix".into(),
        ));
    }
    Ok(())
}

pub(crate) async fn activate_candidate(
    store: &Neo4jReplicaStore,
    candidate: LogicalSnapshotCandidateReceipt,
    active_binding: ReplicaBinding,
) -> Result<LogicalReplicaActivationReceipt, StorageError> {
    let receipt = LogicalReplicaActivationReceipt::new(&candidate, active_binding.clone())?;
    if store.binding_ref() != candidate.candidate_binding()
        && store.binding_ref() != &active_binding
    {
        return Err(StorageError::SnapshotIdentityMismatch);
    }
    let _guard = store.inner.apply_guard.lock().await;
    let client = store.client()?;
    let parameters = activation_parameters(&candidate, &active_binding);
    let expected = vec![
        Value::String(digest_text(active_binding.identity_digest())),
        Value::String(u128_hex(candidate.header().snapshot_id().get())),
        Value::String(u64_hex(candidate.header().applied_index())),
        Value::String(digest_text(candidate.manifest().content_digest())),
        Value::from(candidate.header().format_version()),
    ];
    let rows = client
        .execute(ACTIVATE_CANDIDATE_QUERY, Value::Object(parameters))
        .await?;
    if rows.as_slice() != [expected] {
        return Err(StorageError::SnapshotIdentityMismatch);
    }
    Ok(receipt)
}

fn snapshot_publish_parameters(
    binding: &ReplicaBinding,
    header: &SnapshotHeader,
    manifest: &SnapshotManifest,
) -> serde_json::Map<String, Value> {
    let mut parameters = fenced_parameters(binding);
    parameters.insert(
        "candidate_binding_digest".into(),
        Value::String(digest_text(binding.identity_digest())),
    );
    parameters.insert(
        "snapshot_format_version".into(),
        Value::from(header.format_version()),
    );
    parameters.insert(
        "snapshot_id".into(),
        Value::String(u128_hex(header.snapshot_id().get())),
    );
    parameters.insert(
        "snapshot_applied_index".into(),
        Value::String(u64_hex(header.applied_index())),
    );
    parameters.insert(
        "snapshot_chunk_count".into(),
        Value::String(u64_hex(manifest.chunk_count())),
    );
    parameters.insert(
        "snapshot_record_count".into(),
        Value::String(u64_hex(manifest.record_count())),
    );
    parameters.insert(
        "snapshot_content_digest".into(),
        Value::String(digest_text(manifest.content_digest())),
    );
    parameters
}

fn activation_parameters(
    candidate: &LogicalSnapshotCandidateReceipt,
    active_binding: &ReplicaBinding,
) -> serde_json::Map<String, Value> {
    let mut parameters = owner_parameters(candidate.candidate_binding())
        .as_object()
        .cloned()
        .expect("owner parameters are always a JSON object");
    parameters.insert(
        "candidate_binding_digest".into(),
        Value::String(digest_text(candidate.candidate_binding().identity_digest())),
    );
    parameters.insert("active_binding_role".into(), Value::String("active".into()));
    parameters.insert(
        "active_binding_digest".into(),
        Value::String(digest_text(active_binding.identity_digest())),
    );
    parameters.insert(
        "snapshot_format_version".into(),
        Value::from(candidate.header().format_version()),
    );
    parameters.insert(
        "snapshot_id".into(),
        Value::String(u128_hex(candidate.header().snapshot_id().get())),
    );
    parameters.insert(
        "snapshot_applied_index".into(),
        Value::String(u64_hex(candidate.header().applied_index())),
    );
    parameters.insert(
        "snapshot_chunk_count".into(),
        Value::String(u64_hex(candidate.manifest().chunk_count())),
    );
    parameters.insert(
        "snapshot_record_count".into(),
        Value::String(u64_hex(candidate.manifest().record_count())),
    );
    parameters.insert(
        "snapshot_content_digest".into(),
        Value::String(digest_text(candidate.manifest().content_digest())),
    );
    parameters
}

async fn clear_logical_state(
    transaction: &crate::config::QueryApiTransaction,
    binding: &ReplicaBinding,
) -> Result<(), StorageError> {
    transaction
        .execute(
            "MATCH (owner:DtgOwner {
              namespace_id: $namespace_id,
              backend_generation: $backend_generation,
              binding_digest: $binding_digest
            })
            MATCH (node)
            WHERE node.namespace_id = $namespace_id
              AND node.backend_generation = $backend_generation
              AND NOT node:DtgOwner AND NOT node:DtgSnapshotStage
              AND NOT node:DtgSnapshotRecord
            DETACH DELETE node RETURN count(node)",
            Value::Object(fenced_parameters(binding)),
        )
        .await?;
    Ok(())
}

fn same_logical_identity(left: &ReplicaBinding, right: &ReplicaBinding) -> bool {
    left.cluster_id() == right.cluster_id()
        && left.graph_id() == right.graph_id()
        && left.shard_id() == right.shard_id()
}
