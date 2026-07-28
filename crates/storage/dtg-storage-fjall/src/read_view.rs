use std::collections::BTreeMap;

use dtg_storage::{
    AdjacencyRead, ChangePage, ChangeRecord, ChangesRead, EdgeHistoryRead, EdgeId, EdgeRead,
    EdgeScan, EdgeVersion, LogicalMutation, ReadFence, ScanPage, StorageError, StoreFuture,
    TemporalReadView, TransactionRecord, VertexHistoryRead, VertexId, VertexRead, VertexScan,
    VertexVersion,
};
use fjall::Readable;

use crate::{codec::decode_mutation, graph::FjallReplicaStore, namespace::fjall_error};

pub(crate) struct FjallReadView {
    fence: ReadFence,
    history: Vec<LogicalMutation>,
    changes: Vec<ChangeRecord>,
    transactions: Vec<TransactionRecord>,
    metadata: Vec<dtg_storage::ReplicaMetadata>,
}

impl FjallReadView {
    pub(crate) fn load(store: &FjallReplicaStore, fence: ReadFence) -> Result<Self, StorageError> {
        let snapshot = store.namespace().db.snapshot();
        let history = snapshot
            .iter(&store.namespace().history)
            .map(|item| {
                let (_, value) = item.into_inner().map_err(fjall_error)?;
                decode_mutation(&value)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let changes = snapshot
            .iter(&store.namespace().temporal_index)
            .filter_map(|item| match item.into_inner() {
                Ok((key, value)) if key.first() == Some(&b'c') => Some(Ok((key, value))),
                Ok(_) => None,
                Err(error) => Some(Err(fjall_error(error))),
            })
            .map(|item: Result<fjall::KvPair, StorageError>| {
                let (key, value) = item?;
                if key.len() != 17 {
                    return Err(StorageError::Internal("invalid change index key".into()));
                }
                let index = u64::from_be_bytes(
                    key[1..9]
                        .try_into()
                        .map_err(|_| StorageError::Internal("invalid change index".into()))?,
                );
                Ok(ChangeRecord::new(index, decode_mutation(&value)?))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let transactions = snapshot
            .iter(&store.namespace().transaction)
            .map(|item| {
                let (_, value) = item.into_inner().map_err(fjall_error)?;
                match decode_mutation(&value)? {
                    LogicalMutation::PutTransaction(record) => Ok(record),
                    _ => Err(StorageError::Internal(
                        "non-transaction value in transaction partition".into(),
                    )),
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        let metadata = snapshot
            .prefix(&store.namespace().replica_meta, b"user/")
            .map(|item| {
                let (_, value) = item.into_inner().map_err(fjall_error)?;
                match decode_mutation(&value)? {
                    LogicalMutation::PutReplicaMetadata(record) => Ok(record),
                    _ => Err(StorageError::Internal(
                        "non-metadata value in replica metadata partition".into(),
                    )),
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            fence,
            history,
            changes,
            transactions,
            metadata,
        })
    }

    pub(crate) fn snapshot_records(&self) -> Vec<dtg_storage::SnapshotRecord> {
        let mut records = Vec::new();
        for mutation in &self.history {
            match mutation {
                LogicalMutation::PutVertex(vertex) => {
                    records.push(dtg_storage::SnapshotRecord::Vertex(vertex.clone()));
                }
                LogicalMutation::PutEdge(edge) => {
                    records.push(dtg_storage::SnapshotRecord::Edge(edge.clone()));
                }
                LogicalMutation::DeleteVertex(_)
                | LogicalMutation::DeleteEdge(_)
                | LogicalMutation::PutTransaction(_)
                | LogicalMutation::PutReplicaMetadata(_) => {}
            }
        }
        records.extend(
            self.transactions
                .iter()
                .cloned()
                .map(dtg_storage::SnapshotRecord::Transaction),
        );
        records.extend(
            self.metadata
                .iter()
                .cloned()
                .map(dtg_storage::SnapshotRecord::ReplicaMetadata),
        );
        records
    }

    fn visible_vertex(&self, request: &VertexRead) -> Option<VertexVersion> {
        let mut candidate: Option<(i64, u64, Option<VertexVersion>)> = None;
        for mutation in &self.history {
            let event = match mutation {
                LogicalMutation::PutVertex(vertex)
                    if vertex.id() == request.id()
                        && vertex.transaction_time() <= request.transaction_at()
                        && vertex.valid_time().start() <= request.valid_at()
                        && request.valid_at() < vertex.valid_time().end() =>
                {
                    Some((
                        vertex.transaction_time().get(),
                        vertex.version().get(),
                        Some(vertex.clone()),
                    ))
                }
                LogicalMutation::DeleteVertex(tombstone)
                    if tombstone.id() == request.id()
                        && tombstone.transaction_time() <= request.transaction_at() =>
                {
                    Some((
                        tombstone.transaction_time().get(),
                        tombstone.version().get(),
                        None,
                    ))
                }
                _ => None,
            };
            if let Some(event) = event
                && candidate
                    .as_ref()
                    .is_none_or(|current| (event.0, event.1) > (current.0, current.1))
            {
                candidate = Some(event);
            }
        }
        candidate.and_then(|(_, _, vertex)| vertex)
    }

    fn visible_edge(&self, request: &EdgeRead) -> Option<EdgeVersion> {
        let mut candidate: Option<(i64, u64, Option<EdgeVersion>)> = None;
        for mutation in &self.history {
            let event = match mutation {
                LogicalMutation::PutEdge(edge)
                    if edge.id() == request.id()
                        && edge.transaction_time() <= request.transaction_at()
                        && edge.valid_time().start() <= request.valid_at()
                        && request.valid_at() < edge.valid_time().end() =>
                {
                    Some((
                        edge.transaction_time().get(),
                        edge.version().get(),
                        Some(edge.clone()),
                    ))
                }
                LogicalMutation::DeleteEdge(tombstone)
                    if tombstone.id() == request.id()
                        && tombstone.transaction_time() <= request.transaction_at() =>
                {
                    Some((
                        tombstone.transaction_time().get(),
                        tombstone.version().get(),
                        None,
                    ))
                }
                _ => None,
            };
            if let Some(event) = event
                && candidate
                    .as_ref()
                    .is_none_or(|current| (event.0, event.1) > (current.0, current.1))
            {
                candidate = Some(event);
            }
        }
        candidate.and_then(|(_, _, edge)| edge)
    }
}

impl TemporalReadView for FjallReadView {
    fn fence(&self) -> &ReadFence {
        &self.fence
    }

    fn get_vertex(&self, request: VertexRead) -> StoreFuture<'_, Option<VertexVersion>> {
        Box::pin(async move { Ok(self.visible_vertex(&request)) })
    }

    fn get_edge(&self, request: EdgeRead) -> StoreFuture<'_, Option<EdgeVersion>> {
        Box::pin(async move { Ok(self.visible_edge(&request)) })
    }

    fn vertex_history(&self, request: VertexHistoryRead) -> StoreFuture<'_, Vec<VertexVersion>> {
        Box::pin(async move {
            let mut rows: Vec<_> = self
                .history
                .iter()
                .filter_map(|mutation| match mutation {
                    LogicalMutation::PutVertex(vertex)
                        if vertex.id() == request.id() && request.includes(vertex) =>
                    {
                        Some(vertex.clone())
                    }
                    _ => None,
                })
                .collect();
            rows.sort_by_key(|vertex| (vertex.transaction_time(), vertex.version()));
            rows.truncate(request.limit() as usize);
            Ok(rows)
        })
    }

    fn edge_history(&self, request: EdgeHistoryRead) -> StoreFuture<'_, Vec<EdgeVersion>> {
        Box::pin(async move {
            let mut rows: Vec<_> = self
                .history
                .iter()
                .filter_map(|mutation| match mutation {
                    LogicalMutation::PutEdge(edge)
                        if edge.id() == request.id() && request.includes(edge) =>
                    {
                        Some(edge.clone())
                    }
                    _ => None,
                })
                .collect();
            rows.sort_by_key(|edge| (edge.transaction_time(), edge.version()));
            rows.truncate(request.limit() as usize);
            Ok(rows)
        })
    }

    fn expand(&self, request: AdjacencyRead) -> StoreFuture<'_, Vec<EdgeVersion>> {
        Box::pin(async move {
            let mut visible = BTreeMap::<EdgeId, EdgeVersion>::new();
            for mutation in &self.history {
                if let LogicalMutation::PutEdge(edge) = mutation
                    && request.matches(edge)
                    && visible.get(&edge.id()).is_none_or(|current| {
                        (edge.transaction_time(), edge.version())
                            > (current.transaction_time(), current.version())
                    })
                {
                    visible.insert(edge.id(), edge.clone());
                }
            }
            Ok(visible
                .into_values()
                .take(request.limit() as usize)
                .collect())
        })
    }

    fn changes(&self, request: ChangesRead) -> StoreFuture<'_, ChangePage> {
        Box::pin(async move {
            let matching: Vec<_> = self
                .changes
                .iter()
                .filter(|change| request.includes(change.raft_index()))
                .cloned()
                .collect();
            let limit = request.limit() as usize;
            let has_more = matching.len() > limit;
            let rows: Vec<_> = matching.into_iter().take(limit).collect();
            let next = has_more
                .then(|| rows.last().map(ChangeRecord::raft_index))
                .flatten();
            Ok(ChangePage::new(rows, next))
        })
    }

    fn scan_vertices(
        &self,
        request: VertexScan,
    ) -> StoreFuture<'_, ScanPage<VertexVersion, VertexId>> {
        Box::pin(async move {
            let ids: std::collections::BTreeSet<_> = self
                .history
                .iter()
                .filter_map(|mutation| match mutation {
                    LogicalMutation::PutVertex(vertex) => Some(vertex.id()),
                    LogicalMutation::DeleteVertex(tombstone) => Some(tombstone.id()),
                    _ => None,
                })
                .filter(|id| request.after().is_none_or(|after| *id > after))
                .collect();
            let mut all = Vec::new();
            for id in ids {
                if let Some(vertex) = self.visible_vertex(&VertexRead::new(
                    id,
                    request.valid_at(),
                    request.transaction_at(),
                )) {
                    all.push(vertex);
                }
            }
            let limit = request.limit() as usize;
            let has_more = all.len() > limit;
            all.truncate(limit);
            let next = has_more
                .then(|| all.last().map(VertexVersion::id))
                .flatten();
            Ok(ScanPage::new(all, next))
        })
    }

    fn scan_edges(&self, request: EdgeScan) -> StoreFuture<'_, ScanPage<EdgeVersion, EdgeId>> {
        Box::pin(async move {
            let ids: std::collections::BTreeSet<_> = self
                .history
                .iter()
                .filter_map(|mutation| match mutation {
                    LogicalMutation::PutEdge(edge) => Some(edge.id()),
                    LogicalMutation::DeleteEdge(tombstone) => Some(tombstone.id()),
                    _ => None,
                })
                .filter(|id| request.after().is_none_or(|after| *id > after))
                .collect();
            let mut all = Vec::new();
            for id in ids {
                if let Some(edge) = self.visible_edge(&EdgeRead::new(
                    id,
                    request.valid_at(),
                    request.transaction_at(),
                )) {
                    all.push(edge);
                }
            }
            let limit = request.limit() as usize;
            let has_more = all.len() > limit;
            all.truncate(limit);
            let next = has_more.then(|| all.last().map(EdgeVersion::id)).flatten();
            Ok(ScanPage::new(all, next))
        })
    }
}
