use base64::{Engine as _, engine::general_purpose::STANDARD};
use dtg_storage::{
    ApplyReceipt, CommittedShardBatch, LogicalMutation, StorageError, TransactionState,
};
use serde_json::{Map, Value};

use crate::{
    Neo4jReplicaStore,
    codec::{encode_mutation, encode_value, mutation_kind},
    config::QueryApiTransaction,
    schema::{
        begin_fenced_transaction, decode_digest, decode_u64_hex, digest_text, fenced_parameters,
        text, u64_hex, u128_hex,
    },
};

pub(crate) async fn apply_batch(
    store: &Neo4jReplicaStore,
    batch: CommittedShardBatch,
    failure_after: Option<usize>,
) -> Result<ApplyReceipt, StorageError> {
    batch.validate()?;
    if batch.binding() != store.binding_ref() {
        return Err(StorageError::StaleBinding {
            expected: Box::new(store.binding_ref().clone()),
            actual: Box::new(batch.binding().clone()),
        });
    }
    let _guard = store.inner.apply_guard.lock().await;
    let client = store.client()?;
    let (transaction, applied) =
        begin_fenced_transaction(&client, store.binding_ref(), true).await?;
    let result = apply_in_transaction(&transaction, &batch, applied, failure_after).await;
    match result {
        Ok(receipt) => {
            transaction.commit().await?;
            Ok(receipt)
        }
        Err(error) => {
            let _ = transaction.rollback().await;
            Err(error)
        }
    }
}

async fn apply_in_transaction(
    transaction: &QueryApiTransaction,
    batch: &CommittedShardBatch,
    applied: u64,
    failure_after: Option<usize>,
) -> Result<ApplyReceipt, StorageError> {
    if batch.raft_index() <= applied {
        return verify_replay(transaction, batch).await;
    }
    if batch.raft_index() != applied.saturating_add(1) {
        return Err(StorageError::NonMonotonicIndex {
            applied,
            proposed: batch.raft_index(),
        });
    }
    for (ordinal, mutation) in batch.mutations().iter().enumerate() {
        stage_mutation(
            transaction,
            batch.binding(),
            batch.raft_index(),
            ordinal as u64,
            mutation,
        )
        .await?;
        if failure_after == Some(ordinal + 1) {
            return Err(StorageError::InjectedApplyFailure {
                staged_mutations: ordinal + 1,
            });
        }
    }
    insert_replay(transaction, batch).await?;
    let mut parameters = owner_fence(batch);
    parameters.insert("expected_index".into(), Value::String(u64_hex(applied)));
    parameters.insert(
        "applied_index".into(),
        Value::String(u64_hex(batch.raft_index())),
    );
    let rows = transaction
        .execute(
            "MATCH (owner:DtgOwner {
               namespace_id: $namespace_id,
               backend_generation: $backend_generation,
               binding_digest: $binding_digest,
               applied_index: $expected_index
             })
             SET owner.applied_index = $applied_index
             RETURN owner.applied_index",
            Value::Object(parameters),
        )
        .await?;
    if rows.len() != 1 {
        return Err(StorageError::NonMonotonicIndex {
            applied,
            proposed: batch.raft_index(),
        });
    }
    Ok(ApplyReceipt::new(batch, false))
}

async fn verify_replay(
    transaction: &QueryApiTransaction,
    batch: &CommittedShardBatch,
) -> Result<ApplyReceipt, StorageError> {
    let mut parameters = owner_fence(batch);
    parameters.insert(
        "raft_index".into(),
        Value::String(u64_hex(batch.raft_index())),
    );
    let rows = transaction
        .execute(
            "MATCH (owner:DtgOwner {
               namespace_id: $namespace_id,
               backend_generation: $backend_generation,
               binding_digest: $binding_digest
             })
             MATCH (replay:DtgReplay {
               namespace_id: $namespace_id,
               backend_generation: $backend_generation,
               raft_index: $raft_index
             })
             RETURN replay.raft_term, replay.command_id, replay.mutation_digest
             LIMIT 1",
            Value::Object(parameters),
        )
        .await?;
    let matches = rows.first().is_some_and(|row| {
        row.first()
            .and_then(Value::as_str)
            .and_then(|value| decode_u64_hex(value).ok())
            == Some(batch.raft_term())
            && row.get(1).and_then(Value::as_str)
                == Some(u128_hex(batch.command_id().get()).as_str())
            && row
                .get(2)
                .and_then(Value::as_str)
                .and_then(|value| decode_digest(value).ok())
                == Some(batch.mutation_digest())
    });
    if matches {
        Ok(ApplyReceipt::new(batch, true))
    } else {
        Err(StorageError::ReplayMismatch {
            raft_index: batch.raft_index(),
        })
    }
}

pub(crate) async fn stage_mutation(
    transaction: &QueryApiTransaction,
    binding: &dtg_storage::ReplicaBinding,
    raft_index: u64,
    ordinal: u64,
    mutation: &LogicalMutation,
) -> Result<(), StorageError> {
    let mut parameters = fenced_parameters(binding);
    parameters.insert("raft_index".into(), Value::String(u64_hex(raft_index)));
    parameters.insert("ordinal".into(), Value::String(u64_hex(ordinal)));
    parameters.insert(
        "payload".into(),
        Value::String(STANDARD.encode(encode_mutation(mutation)?)),
    );
    parameters.insert(
        "mutation_kind".into(),
        Value::from(i64::from(mutation_kind(mutation))),
    );
    match mutation {
        LogicalMutation::PutVertex(vertex) => {
            parameters.insert("entity_kind".into(), Value::String("vertex".into()));
            parameters.insert(
                "entity_id".into(),
                Value::String(u128_hex(vertex.id().get())),
            );
            parameters.insert(
                "version".into(),
                Value::String(u64_hex(vertex.version().get())),
            );
            parameters.insert(
                "valid_from".into(),
                Value::from(vertex.valid_time().start()),
            );
            parameters.insert("valid_to".into(), Value::from(vertex.valid_time().end()));
            parameters.insert(
                "transaction_time".into(),
                Value::from(vertex.transaction_time().get()),
            );
            transaction
                .execute(
                    "MATCH (owner:DtgOwner {
                       namespace_id: $namespace_id,
                       backend_generation: $backend_generation,
                       binding_digest: $binding_digest
                     })
                     CREATE (history:DtgVersion {
                       namespace_id: $namespace_id,
                       backend_generation: $backend_generation,
                       entity_kind: $entity_kind, entity_id: $entity_id,
                       version: $version, valid_from: $valid_from, valid_to: $valid_to,
                       transaction_time: $transaction_time, tombstone: false,
                       payload: $payload, raft_index: $raft_index, ordinal: $ordinal
                     })
                     MERGE (vertex:DtgVertex {
                       namespace_id: $namespace_id,
                       backend_generation: $backend_generation,
                       vertex_id: $entity_id
                     })
                     SET vertex.version = $version, vertex.valid_from = $valid_from,
                       vertex.valid_to = $valid_to,
                       vertex.transaction_time = $transaction_time,
                       vertex.payload = $payload, vertex.placeholder = false
                     CREATE (vertex)-[:HAS_VERSION {
                       namespace_id: $namespace_id,
                       backend_generation: $backend_generation
                     }]->(history)
                     RETURN history.entity_id",
                    Value::Object(parameters.clone()),
                )
                .await?;
        }
        LogicalMutation::DeleteVertex(tombstone) => {
            parameters.insert("entity_kind".into(), Value::String("vertex".into()));
            parameters.insert(
                "entity_id".into(),
                Value::String(u128_hex(tombstone.id().get())),
            );
            parameters.insert(
                "version".into(),
                Value::String(u64_hex(tombstone.version().get())),
            );
            parameters.insert(
                "transaction_time".into(),
                Value::from(tombstone.transaction_time().get()),
            );
            transaction
                .execute(
                    "MATCH (owner:DtgOwner {
                       namespace_id: $namespace_id,
                       backend_generation: $backend_generation,
                       binding_digest: $binding_digest
                     })
                     CREATE (history:DtgVersion {
                       namespace_id: $namespace_id,
                       backend_generation: $backend_generation,
                       entity_kind: $entity_kind, entity_id: $entity_id,
                       version: $version, transaction_time: $transaction_time,
                       tombstone: true, payload: $payload,
                       raft_index: $raft_index, ordinal: $ordinal
                     })
                     WITH history
                     OPTIONAL MATCH (vertex:DtgVertex {
                       namespace_id: $namespace_id,
                       backend_generation: $backend_generation,
                       vertex_id: $entity_id
                     })
                     DETACH DELETE vertex
                     RETURN history.entity_id",
                    Value::Object(parameters.clone()),
                )
                .await?;
        }
        LogicalMutation::PutEdge(edge) => {
            parameters.insert("entity_kind".into(), Value::String("edge".into()));
            parameters.insert("entity_id".into(), Value::String(u128_hex(edge.id().get())));
            parameters.insert(
                "source_id".into(),
                Value::String(u128_hex(edge.source().get())),
            );
            parameters.insert(
                "target_id".into(),
                Value::String(u128_hex(edge.target().get())),
            );
            parameters.insert(
                "edge_type".into(),
                Value::String(edge.edge_type().to_owned()),
            );
            parameters.insert(
                "version".into(),
                Value::String(u64_hex(edge.version().get())),
            );
            parameters.insert("valid_from".into(), Value::from(edge.valid_time().start()));
            parameters.insert("valid_to".into(), Value::from(edge.valid_time().end()));
            parameters.insert(
                "transaction_time".into(),
                Value::from(edge.transaction_time().get()),
            );
            transaction
                .execute(
                    "MATCH (owner:DtgOwner {
                       namespace_id: $namespace_id,
                       backend_generation: $backend_generation,
                       binding_digest: $binding_digest
                     })
                     OPTIONAL MATCH ()-[old:DTG_EDGE {
                       namespace_id: $namespace_id,
                       backend_generation: $backend_generation,
                       edge_id: $entity_id
                     }]->()
                     DELETE old
                     WITH owner
                     CREATE (history:DtgVersion {
                       namespace_id: $namespace_id,
                       backend_generation: $backend_generation,
                       entity_kind: $entity_kind, entity_id: $entity_id,
                       source_id: $source_id, target_id: $target_id, edge_type: $edge_type,
                       version: $version, valid_from: $valid_from, valid_to: $valid_to,
                       transaction_time: $transaction_time, tombstone: false,
                       payload: $payload, raft_index: $raft_index, ordinal: $ordinal
                     })
                     MERGE (source:DtgVertex {
                       namespace_id: $namespace_id,
                       backend_generation: $backend_generation,
                       vertex_id: $source_id
                     })
                     ON CREATE SET source.placeholder = true
                     MERGE (target:DtgVertex {
                       namespace_id: $namespace_id,
                       backend_generation: $backend_generation,
                       vertex_id: $target_id
                     })
                     ON CREATE SET target.placeholder = true
                     CREATE (source)-[:DTG_EDGE {
                       namespace_id: $namespace_id,
                       backend_generation: $backend_generation,
                       edge_id: $entity_id, edge_type: $edge_type,
                       version: $version, valid_from: $valid_from, valid_to: $valid_to,
                       transaction_time: $transaction_time, payload: $payload
                     }]->(target)
                     RETURN history.entity_id",
                    Value::Object(parameters.clone()),
                )
                .await?;
        }
        LogicalMutation::DeleteEdge(tombstone) => {
            parameters.insert("entity_kind".into(), Value::String("edge".into()));
            parameters.insert(
                "entity_id".into(),
                Value::String(u128_hex(tombstone.id().get())),
            );
            parameters.insert(
                "version".into(),
                Value::String(u64_hex(tombstone.version().get())),
            );
            parameters.insert(
                "transaction_time".into(),
                Value::from(tombstone.transaction_time().get()),
            );
            transaction
                .execute(
                    "MATCH (owner:DtgOwner {
                       namespace_id: $namespace_id,
                       backend_generation: $backend_generation,
                       binding_digest: $binding_digest
                     })
                     CREATE (history:DtgVersion {
                       namespace_id: $namespace_id,
                       backend_generation: $backend_generation,
                       entity_kind: $entity_kind, entity_id: $entity_id,
                       version: $version, transaction_time: $transaction_time,
                       tombstone: true, payload: $payload,
                       raft_index: $raft_index, ordinal: $ordinal
                     })
                     WITH history
                     OPTIONAL MATCH ()-[edge:DTG_EDGE {
                       namespace_id: $namespace_id,
                       backend_generation: $backend_generation,
                       edge_id: $entity_id
                     }]->()
                     DELETE edge
                     RETURN history.entity_id",
                    Value::Object(parameters.clone()),
                )
                .await?;
        }
        LogicalMutation::PutTransaction(record) => {
            parameters.insert(
                "transaction_id".into(),
                Value::String(u128_hex(record.id().get())),
            );
            parameters.insert(
                "state".into(),
                Value::String(
                    match record.state() {
                        TransactionState::Prepared => "prepared",
                        TransactionState::Committed => "committed",
                        TransactionState::Aborted => "aborted",
                    }
                    .into(),
                ),
            );
            parameters.insert(
                "transaction_time".into(),
                Value::from(record.transaction_time().get()),
            );
            parameters.insert(
                "record_digest".into(),
                Value::String(digest_text(record.record_digest())),
            );
            transaction
                .execute(
                    "MATCH (owner:DtgOwner {
                       namespace_id: $namespace_id,
                       backend_generation: $backend_generation,
                       binding_digest: $binding_digest
                     })
                     MERGE (record:DtgTransaction {
                       namespace_id: $namespace_id,
                       backend_generation: $backend_generation,
                       transaction_id: $transaction_id
                     })
                     SET record.state = $state,
                       record.transaction_time = $transaction_time,
                       record.record_digest = $record_digest,
                       record.payload = $payload
                     RETURN record.transaction_id",
                    Value::Object(parameters.clone()),
                )
                .await?;
        }
        LogicalMutation::PutReplicaMetadata(metadata) => {
            parameters.insert("key".into(), Value::String(metadata.name().to_owned()));
            parameters.insert(
                "value".into(),
                Value::String(STANDARD.encode(encode_value(metadata.value())?)),
            );
            transaction
                .execute(
                    "MATCH (owner:DtgOwner {
                       namespace_id: $namespace_id,
                       backend_generation: $backend_generation,
                       binding_digest: $binding_digest
                     })
                     MERGE (metadata:DtgMetadata {
                       namespace_id: $namespace_id,
                       backend_generation: $backend_generation,
                       key: $key
                     })
                     SET metadata.value = $value,
                       metadata.payload = $payload
                     RETURN metadata.key",
                    Value::Object(parameters.clone()),
                )
                .await?;
        }
    }
    transaction
        .execute(
            "MATCH (owner:DtgOwner {
               namespace_id: $namespace_id,
               backend_generation: $backend_generation,
               binding_digest: $binding_digest
             })
             CREATE (change:DtgChange {
               namespace_id: $namespace_id,
               backend_generation: $backend_generation,
               raft_index: $raft_index, ordinal: $ordinal,
               mutation_kind: $mutation_kind, payload: $payload
             })
             RETURN change.ordinal",
            Value::Object(parameters),
        )
        .await?;
    Ok(())
}

pub(crate) async fn insert_replay(
    transaction: &QueryApiTransaction,
    batch: &CommittedShardBatch,
) -> Result<(), StorageError> {
    let mut parameters = owner_fence(batch);
    parameters.insert(
        "raft_index".into(),
        Value::String(u64_hex(batch.raft_index())),
    );
    parameters.insert(
        "raft_term".into(),
        Value::String(u64_hex(batch.raft_term())),
    );
    parameters.insert(
        "command_id".into(),
        Value::String(u128_hex(batch.command_id().get())),
    );
    parameters.insert(
        "mutation_digest".into(),
        Value::String(digest_text(batch.mutation_digest())),
    );
    transaction
        .execute(
            "MATCH (owner:DtgOwner {
               namespace_id: $namespace_id,
               backend_generation: $backend_generation,
               binding_digest: $binding_digest
             })
             CREATE (replay:DtgReplay {
               namespace_id: $namespace_id,
               backend_generation: $backend_generation,
               raft_index: $raft_index, raft_term: $raft_term,
               command_id: $command_id, mutation_digest: $mutation_digest
             })
             RETURN replay.raft_index",
            Value::Object(parameters),
        )
        .await?;
    Ok(())
}

fn owner_fence(batch: &CommittedShardBatch) -> Map<String, Value> {
    fenced_parameters(batch.binding())
}

pub(crate) fn decode_payload(value: &Value) -> Result<LogicalMutation, StorageError> {
    let bytes = STANDARD
        .decode(text(value, "typed mutation payload")?)
        .map_err(|_| StorageError::Internal("invalid Neo4j mutation payload encoding".into()))?;
    crate::codec::decode_mutation(&bytes)
}
