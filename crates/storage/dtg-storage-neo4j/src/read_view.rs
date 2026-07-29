use dtg_storage::{
    AdjacencyDirection, AdjacencyRead, ChangeCursor, ChangePage, ChangeRecord, ChangesRead,
    EdgeHistoryRead, EdgeId, EdgeRead, EdgeScan, EdgeVersion, LogicalMutation, ReadFence, ScanPage,
    SnapshotRecord, SnapshotReplayRecord, StorageError, StoreFuture, TemporalReadView,
    VertexHistoryRead, VertexId, VertexRead, VertexScan, VertexVersion,
};
use serde_json::{Map, Value};

use crate::{
    apply::decode_payload,
    config::QueryApiTransaction,
    schema::{
        decode_digest, decode_u64_hex, decode_u128_hex, fenced_parameters, text, u64_hex, u128_hex,
    },
};

const CANDIDATE_LIMIT: i64 = 1_000_000;

#[derive(Clone, Copy)]
pub(crate) enum SnapshotPageKind {
    History,
    Transactions,
    Metadata,
    Replay,
    Changes,
    Done,
}

impl SnapshotPageKind {
    pub(crate) const fn next(self) -> Self {
        match self {
            Self::History => Self::Transactions,
            Self::Transactions => Self::Metadata,
            Self::Metadata => Self::Replay,
            Self::Replay => Self::Changes,
            Self::Changes | Self::Done => Self::Done,
        }
    }
}

pub(crate) struct Neo4jReadView {
    transaction: QueryApiTransaction,
    fence: ReadFence,
}

impl Neo4jReadView {
    pub(crate) const fn new(transaction: QueryApiTransaction, fence: ReadFence) -> Self {
        Self { transaction, fence }
    }

    fn parameters(&self) -> Map<String, Value> {
        let mut parameters = fenced_parameters(self.fence.binding());
        parameters.insert(
            "fence_index".into(),
            Value::String(u64_hex(self.fence.applied_index())),
        );
        parameters
    }

    pub(crate) async fn snapshot_page(
        &self,
        kind: SnapshotPageKind,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<SnapshotRecord>, StorageError> {
        if limit == 0 {
            return Err(StorageError::CorruptSnapshot(
                "Neo4j snapshot page bound must be nonzero".into(),
            ));
        }
        let mut parameters = self.parameters();
        parameters.insert("offset".into(), Value::from(offset));
        parameters.insert("limit".into(), Value::from(limit));
        let query = match kind {
            SnapshotPageKind::History => {
                "MATCH (owner:DtgOwner {namespace_id: $namespace_id, backend_generation: $backend_generation, binding_digest: $binding_digest}) MATCH (record:DtgVersion {namespace_id: $namespace_id, backend_generation: $backend_generation}) WHERE record.raft_index <= $fence_index RETURN record.payload ORDER BY record.raft_index, record.ordinal SKIP $offset LIMIT $limit"
            }
            SnapshotPageKind::Transactions => {
                "MATCH (owner:DtgOwner {namespace_id: $namespace_id, backend_generation: $backend_generation, binding_digest: $binding_digest}) MATCH (record:DtgTransaction {namespace_id: $namespace_id, backend_generation: $backend_generation}) RETURN record.payload ORDER BY record.transaction_id SKIP $offset LIMIT $limit"
            }
            SnapshotPageKind::Metadata => {
                "MATCH (owner:DtgOwner {namespace_id: $namespace_id, backend_generation: $backend_generation, binding_digest: $binding_digest}) MATCH (record:DtgMetadata {namespace_id: $namespace_id, backend_generation: $backend_generation}) RETURN record.payload ORDER BY record.key SKIP $offset LIMIT $limit"
            }
            SnapshotPageKind::Replay => {
                "MATCH (owner:DtgOwner {namespace_id: $namespace_id, backend_generation: $backend_generation, binding_digest: $binding_digest}) MATCH (record:DtgReplay {namespace_id: $namespace_id, backend_generation: $backend_generation}) WHERE record.raft_index <= $fence_index RETURN record.raft_index, record.raft_term, record.command_id, record.mutation_digest ORDER BY record.raft_index SKIP $offset LIMIT $limit"
            }
            SnapshotPageKind::Changes => {
                "MATCH (owner:DtgOwner {namespace_id: $namespace_id, backend_generation: $backend_generation, binding_digest: $binding_digest}) MATCH (record:DtgChange {namespace_id: $namespace_id, backend_generation: $backend_generation}) WHERE record.raft_index <= $fence_index RETURN record.raft_index, record.ordinal, record.payload ORDER BY record.raft_index, record.ordinal SKIP $offset LIMIT $limit"
            }
            SnapshotPageKind::Done => return Ok(Vec::new()),
        };
        self.transaction
            .execute(query, Value::Object(parameters))
            .await?
            .into_iter()
            .map(|row| match kind {
                SnapshotPageKind::History => Ok(
                    match decode_payload(row.first().ok_or_else(|| {
                        StorageError::Internal("Neo4j history page omitted payload".into())
                    })?)? {
                        LogicalMutation::PutVertex(value) => SnapshotRecord::Vertex(value),
                        LogicalMutation::DeleteVertex(value) => {
                            SnapshotRecord::VertexTombstone(value)
                        }
                        LogicalMutation::PutEdge(value) => SnapshotRecord::Edge(value),
                        LogicalMutation::DeleteEdge(value) => SnapshotRecord::EdgeTombstone(value),
                        _ => {
                            return Err(StorageError::Internal(
                                "Neo4j history page contained non-graph mutation".into(),
                            ));
                        }
                    },
                ),
                SnapshotPageKind::Transactions => {
                    match decode_payload(row.first().ok_or_else(|| {
                        StorageError::Internal("Neo4j transaction page omitted payload".into())
                    })?)? {
                        LogicalMutation::PutTransaction(value) => {
                            Ok(SnapshotRecord::Transaction(value))
                        }
                        _ => Err(StorageError::Internal(
                            "Neo4j transaction page contained invalid payload".into(),
                        )),
                    }
                }
                SnapshotPageKind::Metadata => {
                    match decode_payload(row.first().ok_or_else(|| {
                        StorageError::Internal("Neo4j metadata page omitted payload".into())
                    })?)? {
                        LogicalMutation::PutReplicaMetadata(value) => {
                            Ok(SnapshotRecord::ReplicaMetadata(value))
                        }
                        _ => Err(StorageError::Internal(
                            "Neo4j metadata page contained invalid payload".into(),
                        )),
                    }
                }
                SnapshotPageKind::Replay => Ok(SnapshotRecord::Replay(SnapshotReplayRecord::new(
                    decode_u64_hex(text(&row[0], "replay index")?)?,
                    decode_u64_hex(text(&row[1], "replay term")?)?,
                    dtg_storage::CommandId::new(decode_u128_hex(text(
                        &row[2],
                        "replay command",
                    )?)?)?,
                    decode_digest(text(&row[3], "replay digest")?)?,
                )?)),
                SnapshotPageKind::Changes => Ok(SnapshotRecord::Change(ChangeRecord::new(
                    ChangeCursor::new(
                        decode_u64_hex(text(&row[0], "change index")?)?,
                        decode_u64_hex(text(&row[1], "change ordinal")?)?,
                    ),
                    decode_payload(&row[2])?,
                ))),
                SnapshotPageKind::Done => unreachable!(),
            })
            .collect()
    }

    async fn vertex(&self, request: &VertexRead) -> Result<Option<VertexVersion>, StorageError> {
        let mut parameters = self.parameters();
        parameters.insert(
            "entity_id".into(),
            Value::String(u128_hex(request.id().get())),
        );
        parameters.insert(
            "transaction_at".into(),
            Value::from(request.transaction_at().get()),
        );
        parameters.insert("valid_at".into(), Value::from(request.valid_at()));
        parameters.insert("limit".into(), Value::from(1));
        let rows = self
            .transaction
            .execute(
                "MATCH (owner:DtgOwner {
                   namespace_id: $namespace_id,
                   backend_generation: $backend_generation,
                   binding_digest: $binding_digest
                 })
                 MATCH (history:DtgVersion {
                   namespace_id: $namespace_id,
                   backend_generation: $backend_generation,
                   entity_kind: 'vertex', entity_id: $entity_id
                 })
                 WHERE history.raft_index <= $fence_index
                   AND history.transaction_time <= $transaction_at
                   AND (history.tombstone OR
                     (history.valid_from <= $valid_at AND $valid_at < history.valid_to))
                 RETURN history.payload
                 ORDER BY history.transaction_time DESC, history.version DESC
                 LIMIT $limit",
                Value::Object(parameters),
            )
            .await?;
        decode_optional_vertex(rows.first().and_then(|row| row.first()))
    }

    async fn edge(&self, request: &EdgeRead) -> Result<Option<EdgeVersion>, StorageError> {
        let mut parameters = self.parameters();
        parameters.insert(
            "entity_id".into(),
            Value::String(u128_hex(request.id().get())),
        );
        parameters.insert(
            "transaction_at".into(),
            Value::from(request.transaction_at().get()),
        );
        parameters.insert("valid_at".into(), Value::from(request.valid_at()));
        parameters.insert("limit".into(), Value::from(1));
        let rows = self
            .transaction
            .execute(
                "MATCH (owner:DtgOwner {
                   namespace_id: $namespace_id,
                   backend_generation: $backend_generation,
                   binding_digest: $binding_digest
                 })
                 MATCH (history:DtgVersion {
                   namespace_id: $namespace_id,
                   backend_generation: $backend_generation,
                   entity_kind: 'edge', entity_id: $entity_id
                 })
                 WHERE history.raft_index <= $fence_index
                   AND history.transaction_time <= $transaction_at
                   AND (history.tombstone OR
                     (history.valid_from <= $valid_at AND $valid_at < history.valid_to))
                 RETURN history.payload
                 ORDER BY history.transaction_time DESC, history.version DESC
                 LIMIT $limit",
                Value::Object(parameters),
            )
            .await?;
        decode_optional_edge(rows.first().and_then(|row| row.first()))
    }
}

impl TemporalReadView for Neo4jReadView {
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
            let mut parameters = self.parameters();
            parameters.insert(
                "entity_id".into(),
                Value::String(u128_hex(request.id().get())),
            );
            parameters.insert(
                "transaction_from".into(),
                Value::from(request.transaction_from().get()),
            );
            parameters.insert(
                "transaction_through".into(),
                Value::from(request.transaction_through().get()),
            );
            parameters.insert("limit".into(), Value::from(request.limit()));
            self.transaction
                .execute(
                    "MATCH (owner:DtgOwner {
                       namespace_id: $namespace_id,
                       backend_generation: $backend_generation,
                       binding_digest: $binding_digest
                     })
                     MATCH (history:DtgVersion {
                       namespace_id: $namespace_id,
                       backend_generation: $backend_generation,
                       entity_kind: 'vertex', entity_id: $entity_id,
                       tombstone: false
                     })
                     WHERE history.raft_index <= $fence_index
                       AND $transaction_from <= history.transaction_time
                       AND history.transaction_time <= $transaction_through
                     RETURN history.payload
                     ORDER BY history.transaction_time, history.version LIMIT $limit",
                    Value::Object(parameters),
                )
                .await?
                .iter()
                .map(|row| decode_required_vertex(row.first()))
                .collect()
        })
    }

    fn edge_history(&self, request: EdgeHistoryRead) -> StoreFuture<'_, Vec<EdgeVersion>> {
        Box::pin(async move {
            let mut parameters = self.parameters();
            parameters.insert(
                "entity_id".into(),
                Value::String(u128_hex(request.id().get())),
            );
            parameters.insert(
                "transaction_from".into(),
                Value::from(request.transaction_from().get()),
            );
            parameters.insert(
                "transaction_through".into(),
                Value::from(request.transaction_through().get()),
            );
            parameters.insert("limit".into(), Value::from(request.limit()));
            self.transaction
                .execute(
                    "MATCH (owner:DtgOwner {
                       namespace_id: $namespace_id,
                       backend_generation: $backend_generation,
                       binding_digest: $binding_digest
                     })
                     MATCH (history:DtgVersion {
                       namespace_id: $namespace_id,
                       backend_generation: $backend_generation,
                       entity_kind: 'edge', entity_id: $entity_id,
                       tombstone: false
                     })
                     WHERE history.raft_index <= $fence_index
                       AND $transaction_from <= history.transaction_time
                       AND history.transaction_time <= $transaction_through
                     RETURN history.payload
                     ORDER BY history.transaction_time, history.version LIMIT $limit",
                    Value::Object(parameters),
                )
                .await?
                .iter()
                .map(|row| decode_required_edge(row.first()))
                .collect()
        })
    }

    fn expand(&self, request: AdjacencyRead) -> StoreFuture<'_, Vec<EdgeVersion>> {
        Box::pin(async move {
            let mut parameters = self.parameters();
            parameters.insert(
                "vertex_id".into(),
                Value::String(u128_hex(request.vertex_id().get())),
            );
            parameters.insert(
                "transaction_at".into(),
                Value::from(request.transaction_at().get()),
            );
            parameters.insert("valid_at".into(), Value::from(request.valid_at()));
            parameters.insert("candidate_limit".into(), Value::from(CANDIDATE_LIMIT));
            parameters.insert("limit".into(), Value::from(request.limit()));
            let direction = match request.direction() {
                AdjacencyDirection::Outgoing => "candidate.source_id = $vertex_id",
                AdjacencyDirection::Incoming => "candidate.target_id = $vertex_id",
                AdjacencyDirection::Both => {
                    "(candidate.source_id = $vertex_id OR candidate.target_id = $vertex_id)"
                }
            };
            let query = format!(
                "MATCH (owner:DtgOwner {{
                   namespace_id: $namespace_id,
                   backend_generation: $backend_generation,
                   binding_digest: $binding_digest
                 }})
                 MATCH (candidate:DtgVersion {{
                   namespace_id: $namespace_id,
                   backend_generation: $backend_generation,
                   entity_kind: 'edge', tombstone: false
                 }})
                 WHERE candidate.raft_index <= $fence_index AND {direction}
                 WITH DISTINCT candidate.entity_id AS entity_id
                 ORDER BY entity_id LIMIT $candidate_limit
                 MATCH (history:DtgVersion {{
                   namespace_id: $namespace_id,
                   backend_generation: $backend_generation,
                   entity_kind: 'edge'
                 }})
                 WHERE history.entity_id = entity_id
                   AND history.raft_index <= $fence_index
                   AND history.transaction_time <= $transaction_at
                   AND (history.tombstone OR
                     (history.valid_from <= $valid_at AND $valid_at < history.valid_to))
                 WITH entity_id, history
                 ORDER BY entity_id, history.transaction_time DESC, history.version DESC
                 WITH entity_id, head(collect(history)) AS latest
                 WHERE latest.tombstone = false AND {latest_direction}
                 RETURN latest.payload ORDER BY entity_id LIMIT $limit",
                latest_direction = direction.replace("candidate.", "latest."),
            );
            self.transaction
                .execute(query, Value::Object(parameters))
                .await?
                .iter()
                .map(|row| decode_required_edge(row.first()))
                .collect()
        })
    }

    fn changes(&self, request: ChangesRead) -> StoreFuture<'_, ChangePage> {
        Box::pin(async move {
            let mut parameters = self.parameters();
            let after = request.after().unwrap_or(ChangeCursor::new(0, 0));
            parameters.insert("has_after".into(), Value::Bool(request.after().is_some()));
            parameters.insert(
                "after_index".into(),
                Value::String(u64_hex(after.raft_index())),
            );
            parameters.insert(
                "after_ordinal".into(),
                Value::String(u64_hex(after.mutation_ordinal())),
            );
            parameters.insert(
                "through_index".into(),
                Value::String(u64_hex(request.through_index())),
            );
            parameters.insert("limit".into(), Value::from(u64::from(request.limit()) + 1));
            let mut rows = self
                .transaction
                .execute(
                    "MATCH (owner:DtgOwner {
                       namespace_id: $namespace_id,
                       backend_generation: $backend_generation,
                       binding_digest: $binding_digest
                     })
                     MATCH (change:DtgChange {
                       namespace_id: $namespace_id,
                       backend_generation: $backend_generation
                     })
                     WHERE change.raft_index <= $fence_index
                       AND change.raft_index <= $through_index
                       AND (NOT $has_after OR change.raft_index > $after_index OR
                         (change.raft_index = $after_index AND change.ordinal > $after_ordinal))
                     RETURN change.raft_index, change.ordinal, change.payload
                     ORDER BY change.raft_index, change.ordinal LIMIT $limit",
                    Value::Object(parameters),
                )
                .await?
                .iter()
                .map(|row| {
                    Ok(ChangeRecord::new(
                        ChangeCursor::new(
                            decode_u64_hex(text(&row[0], "change index")?)?,
                            decode_u64_hex(text(&row[1], "change ordinal")?)?,
                        ),
                        decode_payload(&row[2])?,
                    ))
                })
                .collect::<Result<Vec<_>, StorageError>>()?;
            let has_more = rows.len() > request.limit() as usize;
            rows.truncate(request.limit() as usize);
            let next = has_more.then(|| {
                rows.last()
                    .expect("bounded Neo4j change page is nonempty")
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
            let mut parameters = self.parameters();
            parameters.insert("has_after".into(), Value::Bool(request.after().is_some()));
            parameters.insert(
                "after".into(),
                Value::String(u128_hex(request.after().map(VertexId::get).unwrap_or(0))),
            );
            parameters.insert(
                "transaction_at".into(),
                Value::from(request.transaction_at().get()),
            );
            parameters.insert("valid_at".into(), Value::from(request.valid_at()));
            parameters.insert("candidate_limit".into(), Value::from(CANDIDATE_LIMIT));
            parameters.insert("limit".into(), Value::from(u64::from(request.limit()) + 1));
            let mut rows = self
                .transaction
                .execute(
                    "MATCH (owner:DtgOwner {
                       namespace_id: $namespace_id,
                       backend_generation: $backend_generation,
                       binding_digest: $binding_digest
                     })
                     MATCH (candidate:DtgVersion {
                       namespace_id: $namespace_id,
                       backend_generation: $backend_generation,
                       entity_kind: 'vertex'
                     })
                     WHERE candidate.raft_index <= $fence_index
                       AND (NOT $has_after OR candidate.entity_id > $after)
                     WITH DISTINCT candidate.entity_id AS entity_id
                     ORDER BY entity_id LIMIT $candidate_limit
                     MATCH (history:DtgVersion {
                       namespace_id: $namespace_id,
                       backend_generation: $backend_generation,
                       entity_kind: 'vertex'
                     })
                     WHERE history.entity_id = entity_id
                       AND history.raft_index <= $fence_index
                       AND history.transaction_time <= $transaction_at
                       AND (history.tombstone OR
                         (history.valid_from <= $valid_at AND $valid_at < history.valid_to))
                     WITH entity_id, history
                     ORDER BY entity_id, history.transaction_time DESC, history.version DESC
                     WITH entity_id, head(collect(history)) AS latest
                     WHERE latest.tombstone = false
                     RETURN latest.payload ORDER BY entity_id LIMIT $limit",
                    Value::Object(parameters),
                )
                .await?
                .iter()
                .map(|row| decode_required_vertex(row.first()))
                .collect::<Result<Vec<_>, StorageError>>()?;
            let has_more = rows.len() > request.limit() as usize;
            rows.truncate(request.limit() as usize);
            let next = has_more.then(|| {
                rows.last()
                    .expect("bounded Neo4j vertex page is nonempty")
                    .id()
            });
            Ok(ScanPage::new(rows, next))
        })
    }

    fn scan_edges(&self, request: EdgeScan) -> StoreFuture<'_, ScanPage<EdgeVersion, EdgeId>> {
        Box::pin(async move {
            let mut parameters = self.parameters();
            parameters.insert("has_after".into(), Value::Bool(request.after().is_some()));
            parameters.insert(
                "after".into(),
                Value::String(u128_hex(request.after().map(EdgeId::get).unwrap_or(0))),
            );
            parameters.insert(
                "transaction_at".into(),
                Value::from(request.transaction_at().get()),
            );
            parameters.insert("valid_at".into(), Value::from(request.valid_at()));
            parameters.insert("candidate_limit".into(), Value::from(CANDIDATE_LIMIT));
            parameters.insert("limit".into(), Value::from(u64::from(request.limit()) + 1));
            let mut rows = self
                .transaction
                .execute(
                    "MATCH (owner:DtgOwner {
                       namespace_id: $namespace_id,
                       backend_generation: $backend_generation,
                       binding_digest: $binding_digest
                     })
                     MATCH (candidate:DtgVersion {
                       namespace_id: $namespace_id,
                       backend_generation: $backend_generation,
                       entity_kind: 'edge'
                     })
                     WHERE candidate.raft_index <= $fence_index
                       AND (NOT $has_after OR candidate.entity_id > $after)
                     WITH DISTINCT candidate.entity_id AS entity_id
                     ORDER BY entity_id LIMIT $candidate_limit
                     MATCH (history:DtgVersion {
                       namespace_id: $namespace_id,
                       backend_generation: $backend_generation,
                       entity_kind: 'edge'
                     })
                     WHERE history.entity_id = entity_id
                       AND history.raft_index <= $fence_index
                       AND history.transaction_time <= $transaction_at
                       AND (history.tombstone OR
                         (history.valid_from <= $valid_at AND $valid_at < history.valid_to))
                     WITH entity_id, history
                     ORDER BY entity_id, history.transaction_time DESC, history.version DESC
                     WITH entity_id, head(collect(history)) AS latest
                     WHERE latest.tombstone = false
                     RETURN latest.payload ORDER BY entity_id LIMIT $limit",
                    Value::Object(parameters),
                )
                .await?
                .iter()
                .map(|row| decode_required_edge(row.first()))
                .collect::<Result<Vec<_>, StorageError>>()?;
            let has_more = rows.len() > request.limit() as usize;
            rows.truncate(request.limit() as usize);
            let next = has_more.then(|| {
                rows.last()
                    .expect("bounded Neo4j edge page is nonempty")
                    .id()
            });
            Ok(ScanPage::new(rows, next))
        })
    }
}

fn decode_optional_vertex(value: Option<&Value>) -> Result<Option<VertexVersion>, StorageError> {
    match value.map(decode_payload).transpose()? {
        Some(LogicalMutation::PutVertex(vertex)) => Ok(Some(vertex)),
        Some(LogicalMutation::DeleteVertex(_)) | None => Ok(None),
        _ => Err(StorageError::Internal(
            "Neo4j vertex query returned a non-vertex mutation".into(),
        )),
    }
}

fn decode_optional_edge(value: Option<&Value>) -> Result<Option<EdgeVersion>, StorageError> {
    match value.map(decode_payload).transpose()? {
        Some(LogicalMutation::PutEdge(edge)) => Ok(Some(edge)),
        Some(LogicalMutation::DeleteEdge(_)) | None => Ok(None),
        _ => Err(StorageError::Internal(
            "Neo4j edge query returned a non-edge mutation".into(),
        )),
    }
}

fn decode_required_vertex(value: Option<&Value>) -> Result<VertexVersion, StorageError> {
    decode_optional_vertex(value)?
        .ok_or_else(|| StorageError::Internal("Neo4j vertex query returned a tombstone".into()))
}

fn decode_required_edge(value: Option<&Value>) -> Result<EdgeVersion, StorageError> {
    decode_optional_edge(value)?
        .ok_or_else(|| StorageError::Internal("Neo4j edge query returned a tombstone".into()))
}
