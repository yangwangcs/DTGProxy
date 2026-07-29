use dtg_storage::{
    AdjacencyRead, ChangePage, ChangesRead, EdgeHistoryRead, EdgeId, EdgeRead, EdgeScan,
    EdgeVersion, LogicalMutation, PushdownOutcome, PushdownRequest, ReadFence, ReplicaMetadata,
    ScanPage, StorageError, StoreFuture, TemporalReadView, TransactionTime, VertexHistoryRead,
    VertexId, VertexRead, VertexScan, VertexVersion,
};
use dtg_storage_remote_protocol::{bounded_payload, proto, validate_payload};

use crate::client::{StorageRemoteClient, protocol_storage_error, require_ok};
use crate::codec::{
    decode_change_page, decode_edge_page, decode_mutations, decode_pushdown_outcome,
    decode_vertex_page, encode_adjacency_request, encode_changes_request, encode_history_request,
    encode_point_request, encode_pushdown_request, encode_scan_request, encode_text_request,
};

pub(crate) struct RemoteReadView {
    client: StorageRemoteClient,
    fence: ReadFence,
    session_id: Vec<u8>,
}

impl RemoteReadView {
    pub(crate) async fn open(
        client: StorageRemoteClient,
        fence: ReadFence,
    ) -> Result<Self, StorageError> {
        if fence.binding() != client.binding_ref() {
            return Err(StorageError::StaleBinding {
                expected: Box::new(client.binding_ref().clone()),
                actual: Box::new(fence.binding().clone()),
            });
        }
        if fence.capability_digest() != client.capabilities().digest() {
            return Err(StorageError::CapabilityDrift);
        }
        let response = client
            .rpc()
            .begin_read(proto::BeginReadRequest {
                context: Some(client.context()),
                applied_index: fence.applied_index(),
                capability_digest: fence.capability_digest().get().to_vec(),
            })
            .await
            .map_err(|error| {
                StorageError::Internal(format!("remote begin-read RPC failed: {error}"))
            })?
            .into_inner();
        require_ok(response.status)?;
        if response.session_id.len() != 16 || response.applied_index != fence.applied_index() {
            return Err(StorageError::Internal(
                "remote begin-read response lost session identity".into(),
            ));
        }
        Ok(Self {
            client,
            fence,
            session_id: response.session_id,
        })
    }

    async fn request(
        &self,
        operation: proto::ReadOperation,
        body: Vec<u8>,
    ) -> Result<proto::BoundedPayload, StorageError> {
        let request = bounded_payload(body, 1).map_err(protocol_storage_error)?;
        let response = self
            .client
            .rpc()
            .read(proto::ReadRequest {
                context: Some(self.client.context()),
                session_id: self.session_id.clone(),
                operation: operation as i32,
                request: Some(request),
            })
            .await
            .map_err(|error| StorageError::Internal(format!("remote read RPC failed: {error}")))?
            .into_inner();
        require_ok(response.status)?;
        let payload = response
            .records
            .ok_or_else(|| StorageError::Internal("remote read omitted records".into()))?;
        validate_payload(&payload).map_err(protocol_storage_error)?;
        Ok(payload)
    }

    pub(crate) async fn replica_metadata(
        &self,
        name: &str,
    ) -> Result<Option<ReplicaMetadata>, StorageError> {
        let payload = self
            .request(
                proto::ReadOperation::ReplicaMetadata,
                encode_text_request(name)?,
            )
            .await?;
        let mut mutations = decode_mutations(&payload.body, payload.item_count as usize)?;
        if mutations.len() > 1 {
            return Err(StorageError::Internal(
                "remote metadata read returned multiple records".into(),
            ));
        }
        match mutations.pop() {
            None => Ok(None),
            Some(LogicalMutation::PutReplicaMetadata(metadata)) => Ok(Some(metadata)),
            Some(_) => Err(StorageError::Internal(
                "remote metadata read returned a non-metadata record".into(),
            )),
        }
    }

    pub(crate) async fn execute_pushdown(
        &self,
        request: &PushdownRequest,
    ) -> Result<PushdownOutcome, StorageError> {
        let payload = self
            .request(
                proto::ReadOperation::Pushdown,
                encode_pushdown_request(request)?,
            )
            .await?;
        decode_pushdown_outcome(&payload.body, payload.item_count as usize)
    }

    async fn vertex_records(
        &self,
        operation: proto::ReadOperation,
        body: Vec<u8>,
    ) -> Result<Vec<VertexVersion>, StorageError> {
        let payload = self.request(operation, body).await?;
        decode_mutations(&payload.body, payload.item_count as usize)?
            .into_iter()
            .map(|mutation| match mutation {
                LogicalMutation::PutVertex(vertex) => Ok(vertex),
                _ => Err(StorageError::Internal(
                    "remote vertex read returned another record category".into(),
                )),
            })
            .collect()
    }

    async fn edge_records(
        &self,
        operation: proto::ReadOperation,
        body: Vec<u8>,
    ) -> Result<Vec<EdgeVersion>, StorageError> {
        let payload = self.request(operation, body).await?;
        decode_mutations(&payload.body, payload.item_count as usize)?
            .into_iter()
            .map(|mutation| match mutation {
                LogicalMutation::PutEdge(edge) => Ok(edge),
                _ => Err(StorageError::Internal(
                    "remote edge read returned another record category".into(),
                )),
            })
            .collect()
    }
}

impl TemporalReadView for RemoteReadView {
    fn fence(&self) -> &ReadFence {
        &self.fence
    }

    fn get_vertex(&self, request: VertexRead) -> StoreFuture<'_, Option<VertexVersion>> {
        Box::pin(async move {
            let mut rows = self
                .vertex_records(
                    proto::ReadOperation::GetVertex,
                    encode_point_request(
                        request.id().get(),
                        request.valid_at(),
                        request.transaction_at().get(),
                    ),
                )
                .await?;
            if rows.len() > 1 {
                return Err(StorageError::Internal(
                    "remote point read returned multiple vertices".into(),
                ));
            }
            Ok(rows.pop())
        })
    }

    fn get_edge(&self, request: EdgeRead) -> StoreFuture<'_, Option<EdgeVersion>> {
        Box::pin(async move {
            let mut rows = self
                .edge_records(
                    proto::ReadOperation::GetEdge,
                    encode_point_request(
                        request.id().get(),
                        request.valid_at(),
                        request.transaction_at().get(),
                    ),
                )
                .await?;
            if rows.len() > 1 {
                return Err(StorageError::Internal(
                    "remote point read returned multiple edges".into(),
                ));
            }
            Ok(rows.pop())
        })
    }

    fn vertex_history(&self, request: VertexHistoryRead) -> StoreFuture<'_, Vec<VertexVersion>> {
        Box::pin(async move {
            self.vertex_records(
                proto::ReadOperation::VertexHistory,
                encode_history_request(
                    request.id().get(),
                    request.transaction_from().get(),
                    request.transaction_through().get(),
                    request.limit(),
                ),
            )
            .await
        })
    }

    fn edge_history(&self, request: EdgeHistoryRead) -> StoreFuture<'_, Vec<EdgeVersion>> {
        Box::pin(async move {
            self.edge_records(
                proto::ReadOperation::EdgeHistory,
                encode_history_request(
                    request.id().get(),
                    request.transaction_from().get(),
                    request.transaction_through().get(),
                    request.limit(),
                ),
            )
            .await
        })
    }

    fn expand(&self, request: AdjacencyRead) -> StoreFuture<'_, Vec<EdgeVersion>> {
        Box::pin(async move {
            self.edge_records(
                proto::ReadOperation::Expand,
                encode_adjacency_request(
                    request.vertex_id().get(),
                    request.direction(),
                    request.valid_at(),
                    request.transaction_at().get(),
                    request.limit(),
                ),
            )
            .await
        })
    }

    fn changes(&self, request: ChangesRead) -> StoreFuture<'_, ChangePage> {
        Box::pin(async move {
            let payload = self
                .request(
                    proto::ReadOperation::Changes,
                    encode_changes_request(
                        request.after(),
                        request.through_index(),
                        request.limit(),
                    ),
                )
                .await?;
            decode_change_page(&payload.body, payload.item_count as usize)
        })
    }

    fn scan_vertices(
        &self,
        request: VertexScan,
    ) -> StoreFuture<'_, ScanPage<VertexVersion, VertexId>> {
        Box::pin(async move {
            let payload = self
                .request(
                    proto::ReadOperation::ScanVertices,
                    encode_scan_request(
                        request.valid_at(),
                        request.transaction_at().get(),
                        request.after().map(VertexId::get),
                        request.limit(),
                    ),
                )
                .await?;
            decode_vertex_page(&payload.body, payload.item_count as usize)
        })
    }

    fn scan_edges(&self, request: EdgeScan) -> StoreFuture<'_, ScanPage<EdgeVersion, EdgeId>> {
        Box::pin(async move {
            let payload = self
                .request(
                    proto::ReadOperation::ScanEdges,
                    encode_scan_request(
                        request.valid_at(),
                        request.transaction_at().get(),
                        request.after().map(EdgeId::get),
                        request.limit(),
                    ),
                )
                .await?;
            decode_edge_page(&payload.body, payload.item_count as usize)
        })
    }
}

impl Drop for RemoteReadView {
    fn drop(&mut self) {
        let client = self.client.clone();
        let session_id = self.session_id.clone();
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                let _ = client
                    .rpc()
                    .end_read(proto::EndReadRequest {
                        context: Some(client.context()),
                        session_id,
                    })
                    .await;
            });
        }
    }
}

pub(crate) fn transaction_time(value: i64) -> Result<TransactionTime, StorageError> {
    TransactionTime::new(value).map_err(|error| {
        StorageError::InvalidMutation(format!("invalid transaction time: {error}"))
    })
}
