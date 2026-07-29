use dtg_storage::{
    LogicalSnapshotReader, LogicalSnapshotWriter, ReadFence, ReplicaBinding, SnapshotChunk,
    SnapshotHeader, SnapshotId, SnapshotManifest, SnapshotRequest, SnapshotRestoreReceipt,
    StorageError, StoreFuture,
};
use dtg_storage_remote_protocol::{bounded_payload, proto, validate_payload};
use prost::Message;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Streaming;

use crate::client::{StorageRemoteClient, protocol_storage_error, require_ok};
use crate::codec::{decode_snapshot_records, encode_snapshot_records};
use crate::wire::{decode_binding, digest, encode_binding};

pub(crate) const SNAPSHOT_HEADER_FRAME: u32 = 1;
pub(crate) const SNAPSHOT_CHUNK_FRAME: u32 = 2;
pub(crate) const SNAPSHOT_COMMIT_FRAME: u32 = 3;
pub(crate) const SNAPSHOT_ABORT_FRAME: u32 = 4;
const SNAPSHOT_STREAM_CAPACITY: usize = 4;
const MAX_EXPORT_RETRIES: usize = 3;

pub(crate) fn encode_header(header: &SnapshotHeader) -> proto::SnapshotHeaderPayload {
    proto::SnapshotHeaderPayload {
        snapshot_id: header.snapshot_id().get().to_be_bytes().to_vec(),
        source_binding: Some(encode_binding(header.source_binding())),
        applied_index: header.applied_index(),
        format_version: header.format_version(),
    }
}

pub(crate) fn decode_header(
    payload: &proto::SnapshotHeaderPayload,
) -> Result<SnapshotHeader, StorageError> {
    SnapshotHeader::new(
        decode_snapshot_id(&payload.snapshot_id)?,
        decode_binding(
            payload
                .source_binding
                .as_ref()
                .ok_or_else(|| corrupt("snapshot header omitted source binding"))?,
        )?,
        payload.applied_index,
        payload.format_version,
    )
}

pub(crate) fn encode_chunk(
    chunk: &SnapshotChunk,
) -> Result<proto::SnapshotChunkPayload, StorageError> {
    Ok(proto::SnapshotChunkPayload {
        snapshot_id: chunk.snapshot_id().get().to_be_bytes().to_vec(),
        ordinal: chunk.ordinal(),
        digest: chunk.digest.get().to_vec(),
        records: Some(
            bounded_payload(
                encode_snapshot_records(chunk.records())?,
                chunk.records().len(),
            )
            .map_err(protocol_storage_error)?,
        ),
    })
}

pub(crate) fn decode_chunk(
    payload: &proto::SnapshotChunkPayload,
) -> Result<SnapshotChunk, StorageError> {
    let records = payload
        .records
        .as_ref()
        .ok_or_else(|| corrupt("snapshot chunk omitted records"))?;
    validate_payload(records).map_err(protocol_storage_error)?;
    let decoded = decode_snapshot_records(&records.body)?;
    if decoded.len() != records.item_count as usize {
        return Err(corrupt("snapshot chunk record count mismatch"));
    }
    let chunk = SnapshotChunk::new(
        decode_snapshot_id(&payload.snapshot_id)?,
        payload.ordinal,
        decoded,
    )?;
    if chunk.digest != digest(&payload.digest)? {
        return Err(corrupt("snapshot chunk digest mismatch"));
    }
    Ok(chunk)
}

pub(crate) fn encode_manifest(manifest: &SnapshotManifest) -> proto::SnapshotManifestPayload {
    proto::SnapshotManifestPayload {
        snapshot_id: manifest.snapshot_id().get().to_be_bytes().to_vec(),
        chunk_count: manifest.chunk_count(),
        record_count: manifest.record_count(),
        content_digest: manifest.content_digest().get().to_vec(),
    }
}

pub(crate) fn decode_manifest(
    payload: &proto::SnapshotManifestPayload,
) -> Result<SnapshotManifest, StorageError> {
    Ok(SnapshotManifest {
        snapshot_id: decode_snapshot_id(&payload.snapshot_id)?,
        chunk_count: payload.chunk_count,
        record_count: payload.record_count,
        content_digest: digest(&payload.content_digest)?,
    })
}

pub(crate) fn encode_restore_receipt(
    receipt: &SnapshotRestoreReceipt,
) -> proto::SnapshotRestoreReceiptPayload {
    proto::SnapshotRestoreReceiptPayload {
        binding: Some(encode_binding(receipt.binding())),
        manifest: Some(encode_manifest(receipt.manifest())),
    }
}

pub(crate) fn decode_restore_receipt(
    payload: &proto::SnapshotRestoreReceiptPayload,
) -> Result<SnapshotRestoreReceipt, StorageError> {
    Ok(SnapshotRestoreReceipt::new(
        decode_binding(
            payload
                .binding
                .as_ref()
                .ok_or_else(|| corrupt("restore receipt omitted binding"))?,
        )?,
        decode_manifest(
            payload
                .manifest
                .as_ref()
                .ok_or_else(|| corrupt("restore receipt omitted manifest"))?,
        )?,
    ))
}

pub(crate) fn encode_message_payload(
    message: &impl Message,
) -> Result<proto::BoundedPayload, StorageError> {
    bounded_payload(message.encode_to_vec(), 1).map_err(protocol_storage_error)
}

pub(crate) fn decode_message_payload<M>(payload: &proto::BoundedPayload) -> Result<M, StorageError>
where
    M: Message + Default,
{
    validate_payload(payload).map_err(protocol_storage_error)?;
    if payload.item_count != 1 {
        return Err(corrupt("snapshot frame item count must be one"));
    }
    M::decode(payload.body.as_slice())
        .map_err(|error| corrupt(&format!("invalid snapshot frame: {error}")))
}

pub(crate) struct RemoteSnapshotReader {
    client: StorageRemoteClient,
    fence: ReadFence,
    request: SnapshotRequest,
    header: SnapshotHeader,
    stream: Streaming<proto::SnapshotExportFrame>,
    chunks: Vec<SnapshotChunk>,
    manifest: Option<SnapshotManifest>,
    retries: usize,
}

impl RemoteSnapshotReader {
    pub(crate) async fn begin(
        client: StorageRemoteClient,
        fence: ReadFence,
        request: SnapshotRequest,
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
        request.validate()?;
        let (header, stream) = open_export(&client, &fence, &request, None).await?;
        Ok(Self {
            client,
            fence,
            request,
            header,
            stream,
            chunks: Vec::new(),
            manifest: None,
            retries: 0,
        })
    }

    async fn resume(&mut self) -> Result<(), StorageError> {
        if self.retries >= MAX_EXPORT_RETRIES {
            return Err(StorageError::Internal(
                "remote snapshot export retry budget exhausted".into(),
            ));
        }
        self.retries += 1;
        let resume_after = self.chunks.last().map(SnapshotChunk::ordinal);
        let (header, stream) =
            open_export(&self.client, &self.fence, &self.request, resume_after).await?;
        if header != self.header {
            return Err(StorageError::SnapshotIdentityMismatch);
        }
        self.stream = stream;
        Ok(())
    }
}

impl LogicalSnapshotReader for RemoteSnapshotReader {
    fn header(&self) -> &SnapshotHeader {
        &self.header
    }

    fn next_chunk(&mut self) -> StoreFuture<'_, Option<SnapshotChunk>> {
        Box::pin(async move {
            loop {
                let frame = match self.stream.message().await {
                    Ok(Some(frame)) => frame,
                    Ok(None) => {
                        return Err(StorageError::Internal(
                            "remote snapshot stream ended before its manifest".into(),
                        ));
                    }
                    Err(_) => {
                        self.resume().await?;
                        continue;
                    }
                };
                require_ok(frame.status)?;
                let payload = frame
                    .payload
                    .as_ref()
                    .ok_or_else(|| corrupt("snapshot export frame omitted payload"))?;
                match frame.kind {
                    SNAPSHOT_CHUNK_FRAME => {
                        let wire: proto::SnapshotChunkPayload = decode_message_payload(payload)?;
                        let chunk = decode_chunk(&wire)?;
                        if chunk.snapshot_id() != self.header.snapshot_id()
                            || chunk.ordinal() != self.chunks.len() as u64
                            || frame.sequence != chunk.ordinal() + 1
                        {
                            return Err(corrupt("snapshot export chunk order mismatch"));
                        }
                        self.chunks.push(chunk.clone());
                        return Ok(Some(chunk));
                    }
                    SNAPSHOT_COMMIT_FRAME => {
                        let wire: proto::SnapshotManifestPayload = decode_message_payload(payload)?;
                        let manifest = decode_manifest(&wire)?;
                        if frame.sequence != manifest.chunk_count() + 1 {
                            return Err(corrupt("snapshot manifest sequence mismatch"));
                        }
                        manifest.validate(&self.header, &self.chunks)?;
                        self.manifest = Some(manifest);
                        return Ok(None);
                    }
                    _ => return Err(corrupt("unexpected snapshot export frame kind")),
                }
            }
        })
    }

    fn finish(self: Box<Self>) -> StoreFuture<'static, SnapshotManifest> {
        Box::pin(async move { self.manifest.ok_or(StorageError::SnapshotNotExhausted) })
    }
}

async fn open_export(
    client: &StorageRemoteClient,
    fence: &ReadFence,
    request: &SnapshotRequest,
    resume_after: Option<u64>,
) -> Result<(SnapshotHeader, Streaming<proto::SnapshotExportFrame>), StorageError> {
    let mut stream = client
        .rpc()
        .export_snapshot(proto::ExportSnapshotRequest {
            context: Some(client.context()),
            applied_index: fence.applied_index(),
            snapshot_id: request.snapshot_id().get().to_be_bytes().to_vec(),
            max_records_per_chunk: request.max_records_per_chunk(),
            has_resume_after: resume_after.is_some(),
            resume_after_ordinal: resume_after.unwrap_or_default(),
        })
        .await
        .map_err(|error| StorageError::Internal(format!("remote snapshot export failed: {error}")))?
        .into_inner();
    let frame = stream
        .message()
        .await
        .map_err(|error| StorageError::Internal(format!("remote snapshot header failed: {error}")))?
        .ok_or_else(|| corrupt("snapshot export omitted header"))?;
    require_ok(frame.status)?;
    if frame.kind != SNAPSHOT_HEADER_FRAME {
        return Err(corrupt("snapshot export did not begin with a header"));
    }
    let wire: proto::SnapshotHeaderPayload = decode_message_payload(
        frame
            .payload
            .as_ref()
            .ok_or_else(|| corrupt("snapshot header frame omitted payload"))?,
    )?;
    let header = decode_header(&wire)?;
    if header.snapshot_id() != request.snapshot_id()
        || header.source_binding() != client.binding_ref()
        || header.applied_index() != fence.applied_index()
    {
        return Err(StorageError::SnapshotIdentityMismatch);
    }
    Ok((header, stream))
}

pub(crate) struct RemoteSnapshotWriter {
    client: StorageRemoteClient,
    target_binding: ReplicaBinding,
    header: SnapshotHeader,
    sender: Option<mpsc::Sender<proto::SnapshotImportFrame>>,
    stream_id: Vec<u8>,
    next_sequence: u64,
    chunks: Vec<SnapshotChunk>,
    response: tokio::task::JoinHandle<Result<proto::ImportSnapshotResponse, StorageError>>,
}

impl RemoteSnapshotWriter {
    pub(crate) async fn begin(
        client: StorageRemoteClient,
        target_binding: ReplicaBinding,
        header: SnapshotHeader,
    ) -> Result<Self, StorageError> {
        if &target_binding != client.binding_ref() {
            return Err(StorageError::StaleBinding {
                expected: Box::new(client.binding_ref().clone()),
                actual: Box::new(target_binding),
            });
        }
        let (sender, receiver) = mpsc::channel(SNAPSHOT_STREAM_CAPACITY);
        let rpc_client = client.clone();
        let response = tokio::spawn(async move {
            rpc_client
                .rpc()
                .import_snapshot(ReceiverStream::new(receiver))
                .await
                .map(|response| response.into_inner())
                .map_err(|error| {
                    StorageError::Internal(format!("remote snapshot import failed: {error}"))
                })
        });
        let stream_id = client.context().request_id;
        let mut writer = Self {
            client,
            target_binding,
            header,
            sender: Some(sender),
            stream_id,
            next_sequence: 0,
            chunks: Vec::new(),
            response,
        };
        let wire = encode_header(&writer.header);
        writer
            .send_frame(SNAPSHOT_HEADER_FRAME, encode_message_payload(&wire)?)
            .await?;
        Ok(writer)
    }

    async fn send_frame(
        &mut self,
        kind: u32,
        payload: proto::BoundedPayload,
    ) -> Result<(), StorageError> {
        let frame = proto::SnapshotImportFrame {
            context: Some(self.client.context()),
            stream_id: self.stream_id.clone(),
            kind,
            sequence: self.next_sequence,
            payload: Some(payload),
        };
        self.sender
            .as_ref()
            .ok_or_else(|| StorageError::Internal("snapshot import stream is closed".into()))?
            .send(frame)
            .await
            .map_err(|_| StorageError::Internal("snapshot import stream closed early".into()))?;
        self.next_sequence += 1;
        Ok(())
    }
}

impl LogicalSnapshotWriter for RemoteSnapshotWriter {
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
                return Err(corrupt("snapshot import chunk order mismatch"));
            }
            let wire = encode_chunk(&chunk)?;
            self.send_frame(SNAPSHOT_CHUNK_FRAME, encode_message_payload(&wire)?)
                .await?;
            self.chunks.push(chunk);
            Ok(())
        })
    }

    fn commit(
        mut self: Box<Self>,
        manifest: SnapshotManifest,
    ) -> StoreFuture<'static, SnapshotRestoreReceipt> {
        Box::pin(async move {
            manifest.validate(&self.header, &self.chunks)?;
            let wire = encode_manifest(&manifest);
            self.send_frame(SNAPSHOT_COMMIT_FRAME, encode_message_payload(&wire)?)
                .await?;
            self.sender.take();
            let response = self.response.await.map_err(|error| {
                StorageError::Internal(format!("snapshot import task failed: {error}"))
            })??;
            require_ok(response.status)?;
            let wire: proto::SnapshotRestoreReceiptPayload = decode_message_payload(
                response
                    .receipt
                    .as_ref()
                    .ok_or_else(|| corrupt("snapshot import omitted receipt"))?,
            )?;
            let receipt = decode_restore_receipt(&wire)?;
            if receipt.binding() != &self.target_binding || receipt.manifest() != &manifest {
                return Err(StorageError::SnapshotIdentityMismatch);
            }
            Ok(receipt)
        })
    }

    fn abort(mut self: Box<Self>) -> StoreFuture<'static, ()> {
        Box::pin(async move {
            let payload = bounded_payload(Vec::new(), 0).map_err(protocol_storage_error)?;
            self.send_frame(SNAPSHOT_ABORT_FRAME, payload).await?;
            self.sender.take();
            let response = self.response.await.map_err(|error| {
                StorageError::Internal(format!("snapshot abort task failed: {error}"))
            })??;
            require_ok(response.status)?;
            Ok(())
        })
    }
}

fn decode_snapshot_id(bytes: &[u8]) -> Result<SnapshotId, StorageError> {
    SnapshotId::new(u128::from_be_bytes(
        bytes
            .try_into()
            .map_err(|_| corrupt("snapshot identifier must contain 16 bytes"))?,
    ))
}

fn corrupt(message: &str) -> StorageError {
    StorageError::CorruptSnapshot(message.into())
}
