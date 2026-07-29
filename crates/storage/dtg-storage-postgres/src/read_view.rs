use dtg_storage::{
    AdjacencyDirection, AdjacencyRead, ChangeCursor, ChangePage, ChangeRecord, ChangesRead,
    EdgeHistoryRead, EdgeId, EdgeRead, EdgeScan, EdgeTombstone, EdgeVersion, LogicalMutation,
    ReadFence, ScanPage, SnapshotRecord, SnapshotReplayRecord, StorageError, StoreFuture,
    TemporalReadView, TransactionId, TransactionRecord, TransactionState, TransactionTime,
    ValidInterval, Version, VertexHistoryRead, VertexId, VertexRead, VertexScan, VertexTombstone,
    VertexVersion,
};
use tokio_postgres::{Client, Row};

use crate::{
    codec::{decode_mutation, decode_properties, decode_value},
    config::postgres_error,
    schema::{decode_digest, decode_u64, decode_u128, u64_bytes, u128_bytes},
};

pub(crate) struct PostgresReadView {
    client: Client,
    fence: ReadFence,
}

impl PostgresReadView {
    pub(crate) const fn new(client: Client, fence: ReadFence) -> Self {
        Self { client, fence }
    }

    pub(crate) async fn snapshot_records(&self) -> Result<Vec<SnapshotRecord>, StorageError> {
        let mut records = Vec::new();
        for row in self
            .client
            .query(
                "SELECT vertex_id, version, valid_from, valid_to, transaction_time,
                        properties, tombstone
                 FROM vertex_history ORDER BY raft_index, mutation_ordinal",
                &[],
            )
            .await
            .map_err(postgres_error)?
        {
            records.push(match decode_vertex_event(&row)? {
                LogicalMutation::PutVertex(vertex) => SnapshotRecord::Vertex(vertex),
                LogicalMutation::DeleteVertex(tombstone) => {
                    SnapshotRecord::VertexTombstone(tombstone)
                }
                _ => unreachable!("vertex event decoder returned a non-vertex mutation"),
            });
        }
        for row in self
            .client
            .query(
                "SELECT edge_id, source_vertex_id, target_vertex_id, edge_type, version,
                        valid_from, valid_to, transaction_time, properties, tombstone
                 FROM edge_history ORDER BY raft_index, mutation_ordinal",
                &[],
            )
            .await
            .map_err(postgres_error)?
        {
            records.push(match decode_edge_event(&row)? {
                LogicalMutation::PutEdge(edge) => SnapshotRecord::Edge(edge),
                LogicalMutation::DeleteEdge(tombstone) => SnapshotRecord::EdgeTombstone(tombstone),
                _ => unreachable!("edge event decoder returned a non-edge mutation"),
            });
        }
        for row in self
            .client
            .query(
                "SELECT transaction_id, state, transaction_time, record_digest
                 FROM transaction_state ORDER BY transaction_id",
                &[],
            )
            .await
            .map_err(postgres_error)?
        {
            records.push(SnapshotRecord::Transaction(decode_transaction(&row)?));
        }
        for row in self
            .client
            .query(
                "SELECT name, value FROM replica_metadata ORDER BY name",
                &[],
            )
            .await
            .map_err(postgres_error)?
        {
            records.push(SnapshotRecord::ReplicaMetadata(
                dtg_storage::ReplicaMetadata::new(
                    row.get::<_, String>(0),
                    decode_value(row.get::<_, Vec<u8>>(1).as_slice())?,
                )?,
            ));
        }
        for row in self
            .client
            .query(
                "SELECT raft_index, raft_term, command_id, mutation_digest
                 FROM replay_identity ORDER BY raft_index",
                &[],
            )
            .await
            .map_err(postgres_error)?
        {
            records.push(SnapshotRecord::Replay(SnapshotReplayRecord::new(
                decode_u64(row.get::<_, Vec<u8>>(0).as_slice())?,
                decode_u64(row.get::<_, Vec<u8>>(1).as_slice())?,
                dtg_storage::CommandId::new(decode_u128(row.get::<_, Vec<u8>>(2).as_slice())?)?,
                decode_digest(row.get::<_, Vec<u8>>(3).as_slice())?,
            )?));
        }
        for row in self
            .client
            .query(
                "SELECT raft_index, mutation_ordinal, mutation_payload
                 FROM change_record ORDER BY raft_index, mutation_ordinal",
                &[],
            )
            .await
            .map_err(postgres_error)?
        {
            records.push(SnapshotRecord::Change(ChangeRecord::new(
                ChangeCursor::new(
                    decode_u64(row.get::<_, Vec<u8>>(0).as_slice())?,
                    decode_u64(row.get::<_, Vec<u8>>(1).as_slice())?,
                ),
                decode_mutation(row.get::<_, Vec<u8>>(2).as_slice())?,
            )));
        }
        Ok(records)
    }

    async fn vertex(&self, request: &VertexRead) -> Result<Option<VertexVersion>, StorageError> {
        let row = self
            .client
            .query_opt(
                "SELECT vertex_id, version, valid_from, valid_to, transaction_time,
                        properties, tombstone
                 FROM vertex_history
                 WHERE vertex_id = $1 AND transaction_time <= $2
                   AND (tombstone OR (valid_from <= $3 AND $3 < valid_to))
                 ORDER BY transaction_time DESC, version DESC
                 LIMIT 1",
                &[
                    &u128_bytes(request.id().get()),
                    &request.transaction_at().get(),
                    &request.valid_at(),
                ],
            )
            .await
            .map_err(postgres_error)?;
        row.map(|row| match decode_vertex_event(&row)? {
            LogicalMutation::PutVertex(vertex) => Ok(Some(vertex)),
            LogicalMutation::DeleteVertex(_) => Ok(None),
            _ => unreachable!("vertex event decoder returned a non-vertex mutation"),
        })
        .transpose()
        .map(Option::flatten)
    }

    async fn edge(&self, request: &EdgeRead) -> Result<Option<EdgeVersion>, StorageError> {
        let row = self
            .client
            .query_opt(
                "SELECT edge_id, source_vertex_id, target_vertex_id, edge_type, version,
                        valid_from, valid_to, transaction_time, properties, tombstone
                 FROM edge_history
                 WHERE edge_id = $1 AND transaction_time <= $2
                   AND (tombstone OR (valid_from <= $3 AND $3 < valid_to))
                 ORDER BY transaction_time DESC, version DESC
                 LIMIT 1",
                &[
                    &u128_bytes(request.id().get()),
                    &request.transaction_at().get(),
                    &request.valid_at(),
                ],
            )
            .await
            .map_err(postgres_error)?;
        row.map(|row| match decode_edge_event(&row)? {
            LogicalMutation::PutEdge(edge) => Ok(Some(edge)),
            LogicalMutation::DeleteEdge(_) => Ok(None),
            _ => unreachable!("edge event decoder returned a non-edge mutation"),
        })
        .transpose()
        .map(Option::flatten)
    }
}

impl TemporalReadView for PostgresReadView {
    fn fence(&self) -> &ReadFence {
        &self.fence
    }

    fn get_vertex(&self, request: VertexRead) -> StoreFuture<'_, Option<VertexVersion>> {
        Box::pin(async move { self.vertex(&request).await })
    }

    fn get_edge(&self, request: EdgeRead) -> StoreFuture<'_, Option<EdgeVersion>> {
        Box::pin(async move { self.edge(&request).await })
    }

    fn vertex_history(&self, request: VertexHistoryRead) -> StoreFuture<'_, Vec<VertexVersion>> {
        Box::pin(async move {
            self.client
                .query(
                    "SELECT vertex_id, version, valid_from, valid_to, transaction_time,
                            properties, tombstone
                     FROM vertex_history
                     WHERE vertex_id = $1 AND tombstone = FALSE
                       AND transaction_time BETWEEN $2 AND $3
                     ORDER BY transaction_time, version
                     LIMIT $4",
                    &[
                        &u128_bytes(request.id().get()),
                        &request.transaction_from().get(),
                        &request.transaction_through().get(),
                        &i64::from(request.limit()),
                    ],
                )
                .await
                .map_err(postgres_error)?
                .iter()
                .map(decode_vertex_version)
                .collect()
        })
    }

    fn edge_history(&self, request: EdgeHistoryRead) -> StoreFuture<'_, Vec<EdgeVersion>> {
        Box::pin(async move {
            self.client
                .query(
                    "SELECT edge_id, source_vertex_id, target_vertex_id, edge_type, version,
                            valid_from, valid_to, transaction_time, properties, tombstone
                     FROM edge_history
                     WHERE edge_id = $1 AND tombstone = FALSE
                       AND transaction_time BETWEEN $2 AND $3
                     ORDER BY transaction_time, version
                     LIMIT $4",
                    &[
                        &u128_bytes(request.id().get()),
                        &request.transaction_from().get(),
                        &request.transaction_through().get(),
                        &i64::from(request.limit()),
                    ],
                )
                .await
                .map_err(postgres_error)?
                .iter()
                .map(decode_edge_version)
                .collect()
        })
    }

    fn expand(&self, request: AdjacencyRead) -> StoreFuture<'_, Vec<EdgeVersion>> {
        Box::pin(async move {
            let endpoint = match request.direction() {
                AdjacencyDirection::Outgoing => "source_vertex_id = $1",
                AdjacencyDirection::Incoming => "target_vertex_id = $1",
                AdjacencyDirection::Both => "(source_vertex_id = $1 OR target_vertex_id = $1)",
            };
            let sql = format!(
                "WITH candidate_ids AS (
                    SELECT DISTINCT edge_id FROM edge_history WHERE {endpoint}
                 ), ranked AS (
                    SELECT history.*,
                           ROW_NUMBER() OVER (
                               PARTITION BY history.edge_id
                               ORDER BY history.transaction_time DESC, history.version DESC
                           ) AS rank
                    FROM edge_history AS history
                    JOIN candidate_ids USING (edge_id)
                    WHERE history.transaction_time <= $2
                      AND (history.tombstone OR (history.valid_from <= $3 AND $3 < history.valid_to))
                 )
                 SELECT edge_id, source_vertex_id, target_vertex_id, edge_type, version,
                        valid_from, valid_to, transaction_time, properties, tombstone
                 FROM ranked
                 WHERE rank = 1 AND tombstone = FALSE AND {endpoint}
                 ORDER BY edge_id
                 LIMIT $4"
            );
            self.client
                .query(
                    &sql,
                    &[
                        &u128_bytes(request.vertex_id().get()),
                        &request.transaction_at().get(),
                        &request.valid_at(),
                        &i64::from(request.limit()),
                    ],
                )
                .await
                .map_err(postgres_error)?
                .iter()
                .map(decode_edge_version)
                .collect()
        })
    }

    fn changes(&self, request: ChangesRead) -> StoreFuture<'_, ChangePage> {
        Box::pin(async move {
            let (after_index, after_ordinal) = request
                .after()
                .map(|cursor| (cursor.raft_index(), cursor.mutation_ordinal()))
                .unwrap_or((0, 0));
            let has_after = request.after().is_some();
            let mut rows = self
                .client
                .query(
                    "SELECT raft_index, mutation_ordinal, mutation_payload
                     FROM change_record
                     WHERE raft_index <= $1
                       AND (NOT $2 OR raft_index > $3 OR (raft_index = $3 AND mutation_ordinal > $4))
                     ORDER BY raft_index, mutation_ordinal
                     LIMIT $5",
                    &[
                        &u64_bytes(request.through_index()),
                        &has_after,
                        &u64_bytes(after_index),
                        &u64_bytes(after_ordinal),
                        &(i64::from(request.limit()) + 1),
                    ],
                )
                .await
                .map_err(postgres_error)?
                .iter()
                .map(|row| {
                    Ok(ChangeRecord::new(
                        ChangeCursor::new(
                            decode_u64(row.get::<_, Vec<u8>>(0).as_slice())?,
                            decode_u64(row.get::<_, Vec<u8>>(1).as_slice())?,
                        ),
                        decode_mutation(row.get::<_, Vec<u8>>(2).as_slice())?,
                    ))
                })
                .collect::<Result<Vec<_>, StorageError>>()?;
            let has_more = rows.len() > request.limit() as usize;
            rows.truncate(request.limit() as usize);
            let next = has_more.then(|| {
                rows.last()
                    .expect("bounded change page is nonempty")
                    .cursor()
            });
            Ok(ChangePage::new(rows, next))
        })
    }

    fn scan_vertices(
        &self,
        request: VertexScan,
    ) -> StoreFuture<'_, ScanPage<VertexVersion, VertexId>> {
        Box::pin(async move {
            let after = request.after().map(VertexId::get).unwrap_or(0);
            let mut rows = self
                .client
                .query(
                    "WITH candidate_ids AS (
                        SELECT DISTINCT vertex_id FROM vertex_history
                        WHERE NOT $1 OR vertex_id > $2
                     ), ranked AS (
                        SELECT history.*,
                               ROW_NUMBER() OVER (
                                   PARTITION BY history.vertex_id
                                   ORDER BY history.transaction_time DESC, history.version DESC
                               ) AS rank
                        FROM vertex_history AS history
                        JOIN candidate_ids USING (vertex_id)
                        WHERE history.transaction_time <= $3
                          AND (history.tombstone OR (history.valid_from <= $4 AND $4 < history.valid_to))
                     )
                     SELECT vertex_id, version, valid_from, valid_to, transaction_time,
                            properties, tombstone
                     FROM ranked
                     WHERE rank = 1 AND tombstone = FALSE
                     ORDER BY vertex_id
                     LIMIT $5",
                    &[
                        &request.after().is_some(),
                        &u128_bytes(after),
                        &request.transaction_at().get(),
                        &request.valid_at(),
                        &(i64::from(request.limit()) + 1),
                    ],
                )
                .await
                .map_err(postgres_error)?
                .iter()
                .map(decode_vertex_version)
                .collect::<Result<Vec<_>, StorageError>>()?;
            let has_more = rows.len() > request.limit() as usize;
            rows.truncate(request.limit() as usize);
            let next = has_more.then(|| rows.last().expect("bounded vertex page is nonempty").id());
            Ok(ScanPage::new(rows, next))
        })
    }

    fn scan_edges(&self, request: EdgeScan) -> StoreFuture<'_, ScanPage<EdgeVersion, EdgeId>> {
        Box::pin(async move {
            let after = request.after().map(EdgeId::get).unwrap_or(0);
            let mut rows = self
                .client
                .query(
                    "WITH candidate_ids AS (
                        SELECT DISTINCT edge_id FROM edge_history
                        WHERE NOT $1 OR edge_id > $2
                     ), ranked AS (
                        SELECT history.*,
                               ROW_NUMBER() OVER (
                                   PARTITION BY history.edge_id
                                   ORDER BY history.transaction_time DESC, history.version DESC
                               ) AS rank
                        FROM edge_history AS history
                        JOIN candidate_ids USING (edge_id)
                        WHERE history.transaction_time <= $3
                          AND (history.tombstone OR (history.valid_from <= $4 AND $4 < history.valid_to))
                     )
                     SELECT edge_id, source_vertex_id, target_vertex_id, edge_type, version,
                            valid_from, valid_to, transaction_time, properties, tombstone
                     FROM ranked
                     WHERE rank = 1 AND tombstone = FALSE
                     ORDER BY edge_id
                     LIMIT $5",
                    &[
                        &request.after().is_some(),
                        &u128_bytes(after),
                        &request.transaction_at().get(),
                        &request.valid_at(),
                        &(i64::from(request.limit()) + 1),
                    ],
                )
                .await
                .map_err(postgres_error)?
                .iter()
                .map(decode_edge_version)
                .collect::<Result<Vec<_>, StorageError>>()?;
            let has_more = rows.len() > request.limit() as usize;
            rows.truncate(request.limit() as usize);
            let next = has_more.then(|| rows.last().expect("bounded edge page is nonempty").id());
            Ok(ScanPage::new(rows, next))
        })
    }
}

fn decode_vertex_event(row: &Row) -> Result<LogicalMutation, StorageError> {
    let id = VertexId::new(decode_u128(row.get::<_, Vec<u8>>(0).as_slice())?)?;
    let version = Version::new(decode_u64(row.get::<_, Vec<u8>>(1).as_slice())?);
    let transaction_time = TransactionTime::new(row.get(4)).map_err(kernel_error)?;
    if row.get::<_, bool>(6) {
        Ok(LogicalMutation::DeleteVertex(VertexTombstone::new(
            id,
            version,
            transaction_time,
        )))
    } else {
        Ok(LogicalMutation::PutVertex(VertexVersion::new(
            id,
            version,
            ValidInterval::new(row.get(2), row.get(3)).map_err(kernel_error)?,
            transaction_time,
            decode_properties(row.get::<_, Vec<u8>>(5).as_slice())?,
        )?))
    }
}

fn decode_edge_event(row: &Row) -> Result<LogicalMutation, StorageError> {
    let id = EdgeId::new(decode_u128(row.get::<_, Vec<u8>>(0).as_slice())?)?;
    let version = Version::new(decode_u64(row.get::<_, Vec<u8>>(4).as_slice())?);
    let transaction_time = TransactionTime::new(row.get(7)).map_err(kernel_error)?;
    if row.get::<_, bool>(9) {
        Ok(LogicalMutation::DeleteEdge(EdgeTombstone::new(
            id,
            version,
            transaction_time,
        )))
    } else {
        Ok(LogicalMutation::PutEdge(EdgeVersion::new(
            id,
            VertexId::new(decode_u128(row.get::<_, Vec<u8>>(1).as_slice())?)?,
            VertexId::new(decode_u128(row.get::<_, Vec<u8>>(2).as_slice())?)?,
            row.get::<_, String>(3),
            version,
            ValidInterval::new(row.get(5), row.get(6)).map_err(kernel_error)?,
            transaction_time,
            decode_properties(row.get::<_, Vec<u8>>(8).as_slice())?,
        )?))
    }
}

fn decode_vertex_version(row: &Row) -> Result<VertexVersion, StorageError> {
    match decode_vertex_event(row)? {
        LogicalMutation::PutVertex(vertex) => Ok(vertex),
        _ => Err(StorageError::Internal(
            "PostgreSQL vertex query returned a tombstone".into(),
        )),
    }
}

fn decode_edge_version(row: &Row) -> Result<EdgeVersion, StorageError> {
    match decode_edge_event(row)? {
        LogicalMutation::PutEdge(edge) => Ok(edge),
        _ => Err(StorageError::Internal(
            "PostgreSQL edge query returned a tombstone".into(),
        )),
    }
}

fn decode_transaction(row: &Row) -> Result<TransactionRecord, StorageError> {
    let state = match row.get::<_, i16>(1) {
        1 => TransactionState::Prepared,
        2 => TransactionState::Committed,
        3 => TransactionState::Aborted,
        _ => {
            return Err(StorageError::Internal(
                "PostgreSQL transaction row has an invalid state".into(),
            ));
        }
    };
    TransactionRecord::new(
        TransactionId::new(decode_u128(row.get::<_, Vec<u8>>(0).as_slice())?)
            .map_err(kernel_error)?,
        state,
        TransactionTime::new(row.get(2)).map_err(kernel_error)?,
        decode_digest(row.get::<_, Vec<u8>>(3).as_slice())?,
    )
}

fn kernel_error(error: dtg_kernel::KernelError) -> StorageError {
    StorageError::Internal(format!("invalid typed PostgreSQL row: {error}"))
}
