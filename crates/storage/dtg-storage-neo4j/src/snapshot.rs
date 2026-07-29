use std::collections::{BTreeMap, BTreeSet};

use dtg_storage::{
    BindingRole, ChangeRecord, CommittedShardBatch, LogicalMutation,
    LogicalReplicaActivationReceipt, LogicalSnapshotCandidateReceipt, LogicalSnapshotReader,
    LogicalSnapshotWriter, ReadFence, ReplicaBinding, ReplicaMetadata, SnapshotChunk,
    SnapshotHeader, SnapshotManifest, SnapshotRecord, SnapshotReplayRecord, SnapshotRequest,
    SnapshotRestoreReceipt, StorageError, StoreFuture, TransactionId, TransactionRecord,
};
use serde_json::Value;

use crate::{
    Neo4jReplicaStore,
    apply::{insert_replay, stage_mutation},
    codec::encode_mutation,
    schema::{
        begin_fenced_transaction, digest_text, fenced_parameters, owner_parameters,
        read_applied_index, u64_hex, u128_hex,
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
     DETACH DELETE stage
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
     DETACH DELETE stage
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
    let records = view.snapshot_records().await?;
    let chunks = records
        .chunks(request.max_records_per_chunk() as usize)
        .enumerate()
        .map(|(ordinal, records)| {
            SnapshotChunk::new(request.snapshot_id(), ordinal as u64, records.to_vec())
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Box::new(Neo4jSnapshotReader {
        header,
        chunks,
        next: 0,
    }))
}

struct Neo4jSnapshotReader {
    header: SnapshotHeader,
    chunks: Vec<SnapshotChunk>,
    next: usize,
}

impl LogicalSnapshotReader for Neo4jSnapshotReader {
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
        header,
        chunks: Vec::new(),
    }))
}

struct Neo4jSnapshotWriter {
    store: Neo4jReplicaStore,
    target_binding: ReplicaBinding,
    header: SnapshotHeader,
    chunks: Vec<SnapshotChunk>,
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
                || chunk.ordinal() != self.chunks.len() as u64
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
            let _guard = self.store.inner.apply_guard.lock().await;
            let client = self.store.client()?;
            let (transaction, _) =
                begin_fenced_transaction(&client, self.store.binding_ref(), true).await?;
            let result = async {
                clear_logical_state(&transaction, self.store.binding_ref()).await?;
                for batch in &batches {
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
                    insert_replay(&transaction, batch).await?;
                }
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
                    DETACH DELETE stage
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
            DETACH DELETE node RETURN count(node)",
            Value::Object(fenced_parameters(binding)),
        )
        .await?;
    Ok(())
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
