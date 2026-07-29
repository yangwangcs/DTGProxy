use dtg_storage::{
    ApplyReceipt, CommittedShardBatch, LogicalMutation, StorageError, TransactionState,
};
use tokio_postgres::Client;

use crate::{
    PostgresReplicaStore,
    codec::{encode_mutation, encode_properties, encode_value, mutation_kind},
    config::postgres_error,
    schema::{
        decode_digest, decode_u64, decode_u128, ensure_serving_binding, finish_transaction,
        read_applied_index, u64_bytes, u128_bytes, verify_owner,
    },
};

pub(crate) async fn apply_batch(
    store: &PostgresReplicaStore,
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
    let client = store.connect().await?;
    client
        .batch_execute(
            "BEGIN ISOLATION LEVEL SERIALIZABLE;
             SET LOCAL synchronous_commit = on",
        )
        .await
        .map_err(postgres_error)?;
    let result = apply_in_transaction(&client, store, &batch, failure_after).await;
    finish_transaction(&client, result).await
}

async fn apply_in_transaction(
    client: &Client,
    store: &PostgresReplicaStore,
    batch: &CommittedShardBatch,
    failure_after: Option<usize>,
) -> Result<ApplyReceipt, StorageError> {
    verify_owner(client, store.binding_ref(), true).await?;
    ensure_serving_binding(store.binding_ref())?;
    let applied = read_applied_index(client).await?;
    if batch.raft_index() <= applied {
        return verify_replay(client, batch).await;
    }
    if batch.raft_index() != applied.saturating_add(1) {
        return Err(StorageError::NonMonotonicIndex {
            applied,
            proposed: batch.raft_index(),
        });
    }

    for (ordinal, mutation) in batch.mutations().iter().enumerate() {
        stage_mutation(client, batch.raft_index(), ordinal as u64, mutation).await?;
        if failure_after == Some(ordinal + 1) {
            return Err(StorageError::InjectedApplyFailure {
                staged_mutations: ordinal + 1,
            });
        }
    }
    insert_replay(client, batch).await?;
    let updated = client
        .execute(
            "UPDATE replica_meta SET applied_index = $1 WHERE singleton = TRUE AND applied_index = $2",
            &[&u64_bytes(batch.raft_index()), &u64_bytes(applied)],
        )
        .await
        .map_err(postgres_error)?;
    if updated != 1 {
        return Err(StorageError::NonMonotonicIndex {
            applied: read_applied_index(client).await?,
            proposed: batch.raft_index(),
        });
    }
    Ok(ApplyReceipt::new(batch, false))
}

async fn verify_replay(
    client: &Client,
    batch: &CommittedShardBatch,
) -> Result<ApplyReceipt, StorageError> {
    let row = client
        .query_opt(
            "SELECT raft_term, command_id, mutation_digest FROM replay_identity WHERE raft_index = $1",
            &[&u64_bytes(batch.raft_index())],
        )
        .await
        .map_err(postgres_error)?
        .ok_or(StorageError::ReplayMismatch {
            raft_index: batch.raft_index(),
        })?;
    let term = decode_u64(row.get::<_, Vec<u8>>(0).as_slice())?;
    let command = decode_u128(row.get::<_, Vec<u8>>(1).as_slice())?;
    let digest = decode_digest(row.get::<_, Vec<u8>>(2).as_slice())?;
    if term == batch.raft_term()
        && command == batch.command_id().get()
        && digest == batch.mutation_digest()
    {
        Ok(ApplyReceipt::new(batch, true))
    } else {
        Err(StorageError::ReplayMismatch {
            raft_index: batch.raft_index(),
        })
    }
}

pub(crate) async fn stage_mutation(
    client: &Client,
    raft_index: u64,
    ordinal: u64,
    mutation: &LogicalMutation,
) -> Result<(), StorageError> {
    match mutation {
        LogicalMutation::PutVertex(vertex) => {
            let id = u128_bytes(vertex.id().get());
            let version = u64_bytes(vertex.version().get());
            let properties = encode_properties(vertex.properties())?;
            client
                .execute(
                    "INSERT INTO vertex_history (
                        vertex_id, version, valid_from, valid_to, transaction_time, properties,
                        tombstone, raft_index, mutation_ordinal
                     ) VALUES ($1, $2, $3, $4, $5, $6, FALSE, $7, $8)",
                    &[
                        &id,
                        &version,
                        &vertex.valid_time().start(),
                        &vertex.valid_time().end(),
                        &vertex.transaction_time().get(),
                        &properties,
                        &u64_bytes(raft_index),
                        &u64_bytes(ordinal),
                    ],
                )
                .await
                .map_err(postgres_error)?;
            client
                .execute(
                    "INSERT INTO current_vertex (
                        vertex_id, version, valid_from, valid_to, transaction_time, properties
                     ) VALUES ($1, $2, $3, $4, $5, $6)
                     ON CONFLICT (vertex_id) DO UPDATE SET
                        version = EXCLUDED.version, valid_from = EXCLUDED.valid_from,
                        valid_to = EXCLUDED.valid_to, transaction_time = EXCLUDED.transaction_time,
                        properties = EXCLUDED.properties",
                    &[
                        &id,
                        &version,
                        &vertex.valid_time().start(),
                        &vertex.valid_time().end(),
                        &vertex.transaction_time().get(),
                        &properties,
                    ],
                )
                .await
                .map_err(postgres_error)?;
        }
        LogicalMutation::DeleteVertex(tombstone) => {
            let id = u128_bytes(tombstone.id().get());
            client
                .execute(
                    "INSERT INTO vertex_history (
                        vertex_id, version, valid_from, valid_to, transaction_time, properties,
                        tombstone, raft_index, mutation_ordinal
                     ) VALUES ($1, $2, NULL, NULL, $3, NULL, TRUE, $4, $5)",
                    &[
                        &id,
                        &u64_bytes(tombstone.version().get()),
                        &tombstone.transaction_time().get(),
                        &u64_bytes(raft_index),
                        &u64_bytes(ordinal),
                    ],
                )
                .await
                .map_err(postgres_error)?;
            client
                .execute("DELETE FROM current_vertex WHERE vertex_id = $1", &[&id])
                .await
                .map_err(postgres_error)?;
        }
        LogicalMutation::PutEdge(edge) => {
            let id = u128_bytes(edge.id().get());
            let source = u128_bytes(edge.source().get());
            let target = u128_bytes(edge.target().get());
            let version = u64_bytes(edge.version().get());
            let properties = encode_properties(edge.properties())?;
            client
                .execute(
                    "INSERT INTO edge_history (
                        edge_id, source_vertex_id, target_vertex_id, edge_type, version,
                        valid_from, valid_to, transaction_time, properties, tombstone,
                        raft_index, mutation_ordinal
                     ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, FALSE, $10, $11)",
                    &[
                        &id,
                        &source,
                        &target,
                        &edge.edge_type(),
                        &version,
                        &edge.valid_time().start(),
                        &edge.valid_time().end(),
                        &edge.transaction_time().get(),
                        &properties,
                        &u64_bytes(raft_index),
                        &u64_bytes(ordinal),
                    ],
                )
                .await
                .map_err(postgres_error)?;
            client
                .execute("DELETE FROM adjacency WHERE edge_id = $1", &[&id])
                .await
                .map_err(postgres_error)?;
            client
                .execute(
                    "INSERT INTO current_edge (
                        edge_id, source_vertex_id, target_vertex_id, edge_type, version,
                        valid_from, valid_to, transaction_time, properties
                     ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                     ON CONFLICT (edge_id) DO UPDATE SET
                        source_vertex_id = EXCLUDED.source_vertex_id,
                        target_vertex_id = EXCLUDED.target_vertex_id,
                        edge_type = EXCLUDED.edge_type, version = EXCLUDED.version,
                        valid_from = EXCLUDED.valid_from, valid_to = EXCLUDED.valid_to,
                        transaction_time = EXCLUDED.transaction_time,
                        properties = EXCLUDED.properties",
                    &[
                        &id,
                        &source,
                        &target,
                        &edge.edge_type(),
                        &version,
                        &edge.valid_time().start(),
                        &edge.valid_time().end(),
                        &edge.transaction_time().get(),
                        &properties,
                    ],
                )
                .await
                .map_err(postgres_error)?;
            for (vertex, peer, direction) in [(&source, &target, 1_i16), (&target, &source, 2_i16)]
            {
                client
                    .execute(
                        "INSERT INTO adjacency (
                            vertex_id, edge_id, peer_vertex_id, direction, edge_type,
                            valid_from, valid_to, transaction_time, version
                         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
                        &[
                            vertex,
                            &id,
                            peer,
                            &direction,
                            &edge.edge_type(),
                            &edge.valid_time().start(),
                            &edge.valid_time().end(),
                            &edge.transaction_time().get(),
                            &version,
                        ],
                    )
                    .await
                    .map_err(postgres_error)?;
            }
        }
        LogicalMutation::DeleteEdge(tombstone) => {
            let id = u128_bytes(tombstone.id().get());
            client
                .execute(
                    "INSERT INTO edge_history (
                        edge_id, source_vertex_id, target_vertex_id, edge_type, version,
                        valid_from, valid_to, transaction_time, properties, tombstone,
                        raft_index, mutation_ordinal
                     ) VALUES ($1, NULL, NULL, NULL, $2, NULL, NULL, $3, NULL, TRUE, $4, $5)",
                    &[
                        &id,
                        &u64_bytes(tombstone.version().get()),
                        &tombstone.transaction_time().get(),
                        &u64_bytes(raft_index),
                        &u64_bytes(ordinal),
                    ],
                )
                .await
                .map_err(postgres_error)?;
            client
                .execute("DELETE FROM adjacency WHERE edge_id = $1", &[&id])
                .await
                .map_err(postgres_error)?;
            client
                .execute("DELETE FROM current_edge WHERE edge_id = $1", &[&id])
                .await
                .map_err(postgres_error)?;
        }
        LogicalMutation::PutTransaction(transaction) => {
            let state = match transaction.state() {
                TransactionState::Prepared => 1_i16,
                TransactionState::Committed => 2_i16,
                TransactionState::Aborted => 3_i16,
            };
            client
                .execute(
                    "INSERT INTO transaction_state (
                        transaction_id, state, transaction_time, record_digest
                     ) VALUES ($1, $2, $3, $4)
                     ON CONFLICT (transaction_id) DO UPDATE SET
                        state = EXCLUDED.state, transaction_time = EXCLUDED.transaction_time,
                        record_digest = EXCLUDED.record_digest",
                    &[
                        &u128_bytes(transaction.id().get()),
                        &state,
                        &transaction.transaction_time().get(),
                        &transaction.record_digest().get().to_vec(),
                    ],
                )
                .await
                .map_err(postgres_error)?;
        }
        LogicalMutation::PutReplicaMetadata(metadata) => {
            client
                .execute(
                    "INSERT INTO replica_metadata (name, value) VALUES ($1, $2)
                     ON CONFLICT (name) DO UPDATE SET value = EXCLUDED.value",
                    &[&metadata.name(), &encode_value(metadata.value())?],
                )
                .await
                .map_err(postgres_error)?;
        }
    }

    client
        .execute(
            "INSERT INTO change_record (
                raft_index, mutation_ordinal, mutation_kind, mutation_payload
             ) VALUES ($1, $2, $3, $4)",
            &[
                &u64_bytes(raft_index),
                &u64_bytes(ordinal),
                &mutation_kind(mutation),
                &encode_mutation(mutation)?,
            ],
        )
        .await
        .map_err(postgres_error)?;
    Ok(())
}

pub(crate) async fn insert_replay(
    client: &Client,
    batch: &CommittedShardBatch,
) -> Result<(), StorageError> {
    client
        .execute(
            "INSERT INTO replay_identity (
                raft_index, raft_term, command_id, mutation_digest
             ) VALUES ($1, $2, $3, $4)",
            &[
                &u64_bytes(batch.raft_index()),
                &u64_bytes(batch.raft_term()),
                &u128_bytes(batch.command_id().get()),
                &batch.mutation_digest().get().to_vec(),
            ],
        )
        .await
        .map_err(postgres_error)?;
    Ok(())
}
