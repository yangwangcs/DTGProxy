use std::collections::HashMap;
use std::fmt;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use dtg_storage::{
    AdjacencyRead, BackendClass, BindingRole, CapabilityManifest, ChangesRead, CommandId,
    CommittedShardBatch, Digest32, EdgeHistoryRead, EdgeId, EdgeRead, EdgeScan, LogicalMutation,
    LogicalReplicaActivationReceipt, LogicalSnapshotCandidateReceipt, ProviderKind, ReadFence,
    ReplicaBinding, ReplicaStateStore, SnapshotHeader, SnapshotId, SnapshotManifest,
    SnapshotRequest, SnapshotRestoreReceipt, StorageError, StorageTckFactory, StorageTckStore,
    StoreFuture, TemporalReadView, VertexHistoryRead, VertexId, VertexRead, VertexScan,
};
use dtg_storage_remote_protocol::{
    CONTRACT_MAJOR, CONTRACT_MINOR, MAX_MESSAGE_BYTES, MAX_MESSAGE_ITEMS, PROTOCOL_MAJOR,
    PROTOCOL_MINOR, bounded_payload,
    proto::{
        self,
        storage_server::{Storage, StorageServer},
    },
    validate_context, validate_payload,
};
use tokio::net::TcpListener;
use tokio::sync::{Mutex, oneshot};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status};

use crate::client::{RemoteError, StorageRemoteClient, now_ms};
use crate::codec::{
    decode_adjacency_request, decode_changes_request, decode_history_request, decode_mutations,
    decode_point_request, decode_pushdown_request, decode_scan_request, decode_text_request,
    encode_change_page, encode_edge_page, encode_mutations, encode_pushdown_outcome,
    encode_vertex_page,
};
use crate::read_view::transaction_time;
use crate::snapshot::{
    SNAPSHOT_ABORT_FRAME, SNAPSHOT_CHUNK_FRAME, SNAPSHOT_COMMIT_FRAME, SNAPSHOT_HEADER_FRAME,
    decode_chunk, decode_header, decode_manifest, decode_message_payload, encode_chunk,
    encode_header, encode_manifest, encode_message_payload, encode_restore_receipt,
};
use crate::wire::{decode_binding, encode_binding};

const NO_SNAPSHOT_EXPORT_FAILURE: u64 = u64::MAX;

#[derive(Clone, Debug)]
pub struct ReferenceServerConfig {
    protocol_major: u32,
    protocol_minor: u32,
    contract_major: u32,
    contract_minor: u32,
    capabilities: Option<CapabilityManifest>,
}

impl Default for ReferenceServerConfig {
    fn default() -> Self {
        Self {
            protocol_major: PROTOCOL_MAJOR,
            protocol_minor: PROTOCOL_MINOR,
            contract_major: CONTRACT_MAJOR,
            contract_minor: CONTRACT_MINOR,
            capabilities: None,
        }
    }
}

impl ReferenceServerConfig {
    #[must_use]
    pub const fn with_protocol_major(mut self, protocol_major: u32) -> Self {
        self.protocol_major = protocol_major;
        self
    }

    #[must_use]
    pub const fn with_contract_major(mut self, contract_major: u32) -> Self {
        self.contract_major = contract_major;
        self
    }

    #[must_use]
    pub fn with_capabilities(mut self, capabilities: CapabilityManifest) -> Self {
        self.capabilities = Some(capabilities);
        self
    }
}

pub struct ReferenceStorageServer {
    uri: String,
    state: Arc<ReferenceState>,
    shutdown: Option<oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
}

impl fmt::Debug for ReferenceStorageServer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReferenceStorageServer")
            .field("uri", &self.uri)
            .finish_non_exhaustive()
    }
}

impl ReferenceStorageServer {
    pub async fn spawn(
        factory: Arc<dyn StorageTckFactory>,
        config: ReferenceServerConfig,
    ) -> Result<Self, std::io::Error> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let (shutdown, receiver) = oneshot::channel();
        let state = Arc::new(ReferenceState {
            factory,
            config,
            replicas: RwLock::new(HashMap::new()),
            sessions: Mutex::new(HashMap::new()),
            session_sequence: AtomicU64::new(1),
            lose_next_activation_response: AtomicBool::new(false),
            snapshot_export_failure_after: AtomicU64::new(NO_SNAPSHOT_EXPORT_FAILURE),
        });
        let service = ReferenceService {
            state: state.clone(),
        };
        let task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(
                    StorageServer::new(service)
                        .max_decoding_message_size(MAX_MESSAGE_BYTES)
                        .max_encoding_message_size(MAX_MESSAGE_BYTES),
                )
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    let _ = receiver.await;
                })
                .await
        });
        Ok(Self {
            uri: uri(address),
            state,
            shutdown: Some(shutdown),
            task,
        })
    }

    pub fn uri(&self) -> String {
        self.uri.clone()
    }

    pub fn tck_factory(
        &self,
        provider_name: impl Into<String>,
    ) -> Result<ReferenceStorageTckFactory, StorageError> {
        let provider_name = provider_name.into();
        let capabilities = self.state.capabilities();
        BackendClass::new(
            ProviderKind::Remote(provider_name.clone()),
            1,
            1,
            capabilities.names().map(str::to_owned),
        )?;
        Ok(ReferenceStorageTckFactory {
            uri: self.uri(),
            provider_name,
            capabilities,
            state: self.state.clone(),
        })
    }

    pub fn arm_activation_response_loss(&self) {
        self.state
            .lose_next_activation_response
            .store(true, Ordering::Release);
    }

    pub fn arm_snapshot_export_failure_after(&self, ordinal: u64) {
        self.state
            .snapshot_export_failure_after
            .store(ordinal, Ordering::Release);
    }

    pub async fn active_read_session_count(&self) -> usize {
        self.state.sessions.lock().await.len()
    }
}

impl Drop for ReferenceStorageServer {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        self.task.abort();
    }
}

fn uri(address: SocketAddr) -> String {
    format!("http://{address}")
}

#[derive(Clone)]
struct ReferenceService {
    state: Arc<ReferenceState>,
}

struct ReferenceState {
    factory: Arc<dyn StorageTckFactory>,
    config: ReferenceServerConfig,
    replicas: RwLock<HashMap<String, Arc<ReferenceReplica>>>,
    sessions: Mutex<HashMap<Vec<u8>, Arc<ReferenceSession>>>,
    session_sequence: AtomicU64,
    lose_next_activation_response: AtomicBool,
    snapshot_export_failure_after: AtomicU64,
}

struct ReferenceReplica {
    external_binding: ReplicaBinding,
    #[allow(dead_code)]
    internal_binding: ReplicaBinding,
    #[allow(dead_code)]
    store: Arc<dyn StorageTckStore>,
    candidate_marker: Mutex<Option<CandidateMarker>>,
    activation_store: Option<Arc<dyn StorageTckStore>>,
}

#[derive(Clone)]
struct CandidateMarker {
    external_candidate_binding: ReplicaBinding,
    internal_candidate_binding: ReplicaBinding,
    external_header: SnapshotHeader,
    external_manifest: SnapshotManifest,
    internal_header: SnapshotHeader,
    internal_manifest: SnapshotManifest,
}

struct ReferenceSession {
    external_binding: ReplicaBinding,
    replica: Arc<ReferenceReplica>,
    view: Arc<dyn TemporalReadView>,
}

impl ReferenceState {
    fn capabilities(&self) -> CapabilityManifest {
        self.config
            .capabilities
            .clone()
            .unwrap_or_else(|| self.factory.capabilities())
    }

    async fn ensure_replica(
        &self,
        external_binding: ReplicaBinding,
    ) -> Result<Arc<ReferenceReplica>, StorageError> {
        if external_binding.capability_digest() != self.capabilities().digest() {
            return Err(StorageError::CapabilityDrift);
        }
        let namespace = external_binding.namespace_id().as_str().to_owned();
        if let Some(replica) = self
            .replicas
            .read()
            .map_err(|_| StorageError::Internal("reference replica lock is poisoned".into()))?
            .get(&namespace)
            .cloned()
        {
            if replica.external_binding == external_binding {
                return Ok(replica);
            }
            if same_binding_except_role(&replica.external_binding, &external_binding) {
                return Err(StorageError::StaleBinding {
                    expected: Box::new(replica.external_binding.clone()),
                    actual: Box::new(external_binding),
                });
            }
            return Err(StorageError::NamespaceOwnerMismatch {
                expected: Box::new(replica.external_binding.clone()),
                actual: Box::new(external_binding),
            });
        }
        let base = self.factory.binding(
            external_binding.namespace_id().as_str(),
            external_binding.backend_generation().get(),
        )?;
        let internal_binding = base
            .to_builder()
            .cluster_id(external_binding.cluster_id().get())
            .graph_id(external_binding.graph_id().get())
            .shard_id(external_binding.shard_id().get())
            .placement_epoch(external_binding.placement_epoch().get())
            .replica_id(external_binding.replica_id().get())
            .backend_generation(external_binding.backend_generation().get())
            .namespace_id(external_binding.namespace_id().as_str())
            .role(external_binding.role())
            .build()?;
        let store: Arc<dyn StorageTckStore> =
            Arc::from(self.factory.open(internal_binding.clone()).await?);
        let replica = Arc::new(ReferenceReplica {
            external_binding,
            internal_binding,
            store,
            candidate_marker: Mutex::new(None),
            activation_store: None,
        });
        let mut replicas = self
            .replicas
            .write()
            .map_err(|_| StorageError::Internal("reference replica lock is poisoned".into()))?;
        if let Some(existing) = replicas.get(&namespace) {
            if existing.external_binding == replica.external_binding {
                return Ok(existing.clone());
            }
            return Err(StorageError::NamespaceOwnerMismatch {
                expected: Box::new(existing.external_binding.clone()),
                actual: Box::new(replica.external_binding.clone()),
            });
        }
        replicas.insert(namespace, replica.clone());
        Ok(replica)
    }

    fn next_session_id(&self, replica_id: u64) -> Vec<u8> {
        let sequence = self.session_sequence.fetch_add(1, Ordering::Relaxed);
        let mut session_id = [0_u8; 16];
        session_id[..8].copy_from_slice(&sequence.to_be_bytes());
        session_id[8..].copy_from_slice(&replica_id.to_be_bytes());
        session_id.to_vec()
    }
}

#[tonic::async_trait]
impl Storage for ReferenceService {
    async fn handshake(
        &self,
        request: Request<proto::HandshakeRequest>,
    ) -> Result<Response<proto::HandshakeResponse>, Status> {
        let request = request.into_inner();
        let context = validate_context(request.context.as_ref(), now_ms())
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let binding = context
            .binding
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing binding"))?;
        let capabilities = self.state.capabilities();
        let binding = match decode_binding(binding) {
            Ok(binding) => binding,
            Err(error) => {
                return Ok(Response::new(handshake_response(
                    &self.state.config,
                    &capabilities,
                    error_status(error),
                    0,
                )));
            }
        };
        let replica = match self.state.ensure_replica(binding).await {
            Ok(replica) => replica,
            Err(error) => {
                return Ok(Response::new(handshake_response(
                    &self.state.config,
                    &capabilities,
                    error_status(error),
                    0,
                )));
            }
        };
        let applied = replica
            .store
            .applied_index()
            .await
            .map_err(storage_status)?;
        Ok(Response::new(handshake_response(
            &self.state.config,
            &capabilities,
            ok_status(applied),
            applied,
        )))
    }

    async fn apply(
        &self,
        request: Request<proto::ApplyRequest>,
    ) -> Result<Response<proto::ApplyResponse>, Status> {
        let request = request.into_inner();
        let context = validate_context(request.context.as_ref(), now_ms())
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let external_binding = decode_binding(
            context
                .binding
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("missing binding"))?,
        )
        .map_err(storage_status)?;
        let replica = match self.state.ensure_replica(external_binding).await {
            Ok(replica) => replica,
            Err(error) => {
                return Ok(Response::new(proto::ApplyResponse {
                    status: Some(error_status(error)),
                    replayed: false,
                }));
            }
        };
        if request.command_id.len() != 16 || request.idempotency_key != request.command_id {
            return Err(Status::invalid_argument(
                "apply identity or idempotency key is invalid",
            ));
        }
        let payload = request
            .mutations
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing mutation payload"))?;
        validate_payload(payload).map_err(|error| Status::invalid_argument(error.to_string()))?;
        let mutations = decode_mutations(&payload.body).map_err(storage_status)?;
        if payload.item_count as usize != mutations.len() {
            return Err(Status::invalid_argument(
                "mutation payload item count differs from its body",
            ));
        }
        let command_id = CommandId::new(u128::from_be_bytes(
            request
                .command_id
                .as_slice()
                .try_into()
                .map_err(|_| Status::invalid_argument("invalid command identifier"))?,
        ))
        .map_err(storage_status)?;
        let batch = CommittedShardBatch::new(
            replica.internal_binding.clone(),
            request.raft_term,
            request.raft_index,
            command_id,
            mutations,
        )
        .map_err(storage_status)?;
        let transmitted_digest = Digest32::new(
            request
                .mutation_digest
                .as_slice()
                .try_into()
                .map_err(|_| Status::invalid_argument("invalid mutation digest"))?,
        );
        if transmitted_digest != batch.mutation_digest() {
            return Err(Status::invalid_argument("mutation digest mismatch"));
        }
        match replica.store.apply(batch).await {
            Ok(receipt) => Ok(Response::new(proto::ApplyResponse {
                status: Some(ok_status(receipt.raft_index())),
                replayed: receipt.replayed(),
            })),
            Err(error) => Ok(Response::new(proto::ApplyResponse {
                status: Some(error_status(error)),
                replayed: false,
            })),
        }
    }

    async fn begin_read(
        &self,
        request: Request<proto::BeginReadRequest>,
    ) -> Result<Response<proto::BeginReadResponse>, Status> {
        let request = request.into_inner();
        let context = validate_context(request.context.as_ref(), now_ms())
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let external_binding = decode_binding(
            context
                .binding
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("missing binding"))?,
        )
        .map_err(storage_status)?;
        let replica = match self.state.ensure_replica(external_binding.clone()).await {
            Ok(replica) => replica,
            Err(error) => {
                return Ok(Response::new(proto::BeginReadResponse {
                    status: Some(error_status(error)),
                    session_id: Vec::new(),
                    applied_index: 0,
                }));
            }
        };
        if request.capability_digest.as_slice()
            != external_binding.capability_digest().get().as_slice()
        {
            return Ok(Response::new(proto::BeginReadResponse {
                status: Some(error_status(StorageError::CapabilityDrift)),
                session_id: Vec::new(),
                applied_index: 0,
            }));
        }
        let fence = ReadFence::new(replica.internal_binding.clone(), request.applied_index);
        let view = match replica.store.begin_read_view(fence).await {
            Ok(view) => Arc::<dyn TemporalReadView>::from(view),
            Err(error) => {
                return Ok(Response::new(proto::BeginReadResponse {
                    status: Some(error_status(error)),
                    session_id: Vec::new(),
                    applied_index: 0,
                }));
            }
        };
        let session_id = self
            .state
            .next_session_id(external_binding.replica_id().get());
        let session = Arc::new(ReferenceSession {
            external_binding,
            replica,
            view,
        });
        let mut sessions = self.state.sessions.lock().await;
        if sessions.len() >= MAX_MESSAGE_ITEMS {
            return Err(Status::resource_exhausted("too many remote read sessions"));
        }
        sessions.insert(session_id.clone(), session);
        Ok(Response::new(proto::BeginReadResponse {
            status: Some(ok_status(request.applied_index)),
            session_id,
            applied_index: request.applied_index,
        }))
    }

    async fn read(
        &self,
        request: Request<proto::ReadRequest>,
    ) -> Result<Response<proto::ReadResponse>, Status> {
        let request = request.into_inner();
        let context = validate_context(request.context.as_ref(), now_ms())
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let external_binding = decode_binding(
            context
                .binding
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("missing binding"))?,
        )
        .map_err(storage_status)?;
        let payload = request
            .request
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing read payload"))?;
        validate_payload(payload).map_err(|error| Status::invalid_argument(error.to_string()))?;
        let session = self
            .state
            .sessions
            .lock()
            .await
            .get(&request.session_id)
            .cloned();
        let Some(session) = session else {
            return Ok(Response::new(proto::ReadResponse {
                status: Some(error_status(StorageError::NotFound)),
                records: None,
            }));
        };
        if session.external_binding != external_binding {
            return Ok(Response::new(proto::ReadResponse {
                status: Some(error_status(StorageError::StaleBinding {
                    expected: Box::new(session.external_binding.clone()),
                    actual: Box::new(external_binding),
                })),
                records: None,
            }));
        }
        let current_binding = self
            .state
            .replicas
            .read()
            .map_err(|_| Status::internal("reference replica lock is poisoned"))?
            .get(session.external_binding.namespace_id().as_str())
            .map(|replica| replica.external_binding.clone())
            .ok_or_else(|| storage_status(StorageError::NotFound))?;
        if current_binding != session.external_binding {
            return Ok(Response::new(proto::ReadResponse {
                status: Some(error_status(StorageError::StaleBinding {
                    expected: Box::new(current_binding),
                    actual: Box::new(session.external_binding.clone()),
                })),
                records: None,
            }));
        }
        let operation = proto::ReadOperation::try_from(request.operation)
            .map_err(|_| Status::invalid_argument("unknown read operation"))?;
        let result = dispatch_read(&session, operation, &payload.body).await;
        match result {
            Ok((body, item_count)) => {
                if body.len() as u64 > context.max_response_bytes
                    || item_count as u32 > context.max_response_items
                {
                    return Err(Status::resource_exhausted(
                        "read result exceeds the negotiated response budget",
                    ));
                }
                Ok(Response::new(proto::ReadResponse {
                    status: Some(ok_status(session.view.fence().applied_index())),
                    records: Some(
                        bounded_payload(body, item_count)
                            .map_err(|error| Status::resource_exhausted(error.to_string()))?,
                    ),
                }))
            }
            Err(error) => Ok(Response::new(proto::ReadResponse {
                status: Some(error_status(error)),
                records: None,
            })),
        }
    }

    async fn end_read(
        &self,
        request: Request<proto::EndReadRequest>,
    ) -> Result<Response<proto::EndReadResponse>, Status> {
        let request = request.into_inner();
        let context = validate_context(request.context.as_ref(), now_ms())
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let external_binding = decode_binding(
            context
                .binding
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("missing binding"))?,
        )
        .map_err(storage_status)?;
        let mut sessions = self.state.sessions.lock().await;
        match sessions.get(&request.session_id) {
            Some(session) if session.external_binding != external_binding => {
                return Ok(Response::new(proto::EndReadResponse {
                    status: Some(error_status(StorageError::StaleBinding {
                        expected: Box::new(session.external_binding.clone()),
                        actual: Box::new(external_binding),
                    })),
                }));
            }
            Some(_) => {
                sessions.remove(&request.session_id);
            }
            None => {
                return Ok(Response::new(proto::EndReadResponse {
                    status: Some(error_status(StorageError::NotFound)),
                }));
            }
        }
        Ok(Response::new(proto::EndReadResponse {
            status: Some(ok_status(0)),
        }))
    }

    type ExportSnapshotStream =
        tokio_stream::wrappers::ReceiverStream<Result<proto::SnapshotExportFrame, Status>>;

    async fn export_snapshot(
        &self,
        request: Request<proto::ExportSnapshotRequest>,
    ) -> Result<Response<Self::ExportSnapshotStream>, Status> {
        let request = request.into_inner();
        let context = validate_context(request.context.as_ref(), now_ms())
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let external_binding = decode_binding(
            context
                .binding
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("missing binding"))?,
        )
        .map_err(storage_status)?;
        let replica = self
            .state
            .ensure_replica(external_binding.clone())
            .await
            .map_err(storage_status)?;
        let snapshot_id = SnapshotId::new(u128::from_be_bytes(
            request
                .snapshot_id
                .as_slice()
                .try_into()
                .map_err(|_| Status::invalid_argument("invalid snapshot identifier"))?,
        ))
        .map_err(storage_status)?;
        let snapshot_request =
            SnapshotRequest::new(snapshot_id.get(), request.max_records_per_chunk)
                .map_err(storage_status)?;
        let fence = ReadFence::new(replica.internal_binding.clone(), request.applied_index);
        let mut reader = replica
            .store
            .begin_snapshot(fence, snapshot_request)
            .await
            .map_err(storage_status)?;
        let header = SnapshotHeader::new(
            snapshot_id,
            external_binding,
            request.applied_index,
            reader.header().format_version(),
        )
        .map_err(storage_status)?;
        let resume_after = request
            .has_resume_after
            .then_some(request.resume_after_ordinal);
        let fail_after = self
            .state
            .snapshot_export_failure_after
            .swap(NO_SNAPSHOT_EXPORT_FAILURE, Ordering::AcqRel);
        let (sender, receiver) = tokio::sync::mpsc::channel(4);
        tokio::spawn(async move {
            let result = async {
                let header_wire = encode_header(&header);
                sender
                    .send(Ok(proto::SnapshotExportFrame {
                        status: Some(ok_status(header.applied_index())),
                        kind: SNAPSHOT_HEADER_FRAME,
                        sequence: 0,
                        payload: Some(
                            encode_message_payload(&header_wire).map_err(storage_status)?,
                        ),
                    }))
                    .await
                    .map_err(|_| Status::cancelled("snapshot export receiver closed"))?;
                let mut chunks = Vec::new();
                while let Some(chunk) = reader.next_chunk().await.map_err(storage_status)? {
                    if fail_after != NO_SNAPSHOT_EXPORT_FAILURE && chunk.ordinal() > fail_after {
                        return Err(Status::unavailable(
                            "injected snapshot export stream failure",
                        ));
                    }
                    let should_send = resume_after.is_none_or(|ordinal| chunk.ordinal() > ordinal);
                    let wire = encode_chunk(&chunk).map_err(storage_status)?;
                    if should_send {
                        sender
                            .send(Ok(proto::SnapshotExportFrame {
                                status: Some(ok_status(header.applied_index())),
                                kind: SNAPSHOT_CHUNK_FRAME,
                                sequence: chunk.ordinal() + 1,
                                payload: Some(
                                    encode_message_payload(&wire).map_err(storage_status)?,
                                ),
                            }))
                            .await
                            .map_err(|_| Status::cancelled("snapshot export receiver closed"))?;
                    }
                    chunks.push(chunk);
                }
                let _ = reader.finish().await.map_err(storage_status)?;
                let manifest = SnapshotManifest::new(&header, &chunks).map_err(storage_status)?;
                let wire = encode_manifest(&manifest);
                sender
                    .send(Ok(proto::SnapshotExportFrame {
                        status: Some(ok_status(header.applied_index())),
                        kind: SNAPSHOT_COMMIT_FRAME,
                        sequence: manifest.chunk_count() + 1,
                        payload: Some(encode_message_payload(&wire).map_err(storage_status)?),
                    }))
                    .await
                    .map_err(|_| Status::cancelled("snapshot export receiver closed"))?;
                Ok::<(), Status>(())
            }
            .await;
            if let Err(error) = result {
                let _ = sender.send(Err(error)).await;
            }
        });
        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(
            receiver,
        )))
    }

    async fn import_snapshot(
        &self,
        request: Request<tonic::Streaming<proto::SnapshotImportFrame>>,
    ) -> Result<Response<proto::ImportSnapshotResponse>, Status> {
        import_snapshot_stream(&self.state, request.into_inner()).await
    }

    async fn activate(
        &self,
        request: Request<proto::ActivateRequest>,
    ) -> Result<Response<proto::ActivateResponse>, Status> {
        let request = request.into_inner();
        let context = validate_context(request.context.as_ref(), now_ms())
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let context_binding = decode_binding(
            context
                .binding
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("missing binding"))?,
        )
        .map_err(storage_status)?;
        let candidate_wire = request
            .candidate
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("activation omitted candidate"))?;
        let candidate_binding = decode_binding(
            candidate_wire
                .candidate_binding
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("candidate omitted binding"))?,
        )
        .map_err(storage_status)?;
        let candidate_header = decode_header(
            candidate_wire
                .header
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("candidate omitted header"))?,
        )
        .map_err(storage_status)?;
        let candidate_manifest = decode_manifest(
            candidate_wire
                .manifest
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("candidate omitted manifest"))?,
        )
        .map_err(storage_status)?;
        let active_binding = decode_binding(
            request
                .active_binding
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("activation omitted active binding"))?,
        )
        .map_err(storage_status)?;
        let candidate = match LogicalSnapshotCandidateReceipt::new(
            candidate_binding.clone(),
            candidate_header,
            candidate_manifest,
        ) {
            Ok(candidate) => candidate,
            Err(error) => return Ok(Response::new(activation_error_response(error))),
        };
        let expected =
            match LogicalReplicaActivationReceipt::new(&candidate, active_binding.clone()) {
                Ok(receipt) => receipt,
                Err(error) => return Ok(Response::new(activation_error_response(error))),
            };
        if context_binding != candidate_binding {
            return Ok(Response::new(activation_error_response(
                StorageError::SnapshotIdentityMismatch,
            )));
        }
        let namespace = candidate_binding.namespace_id().as_str().to_owned();
        let replica = self
            .state
            .replicas
            .read()
            .map_err(|_| Status::internal("reference replica lock is poisoned"))?
            .get(&namespace)
            .cloned()
            .ok_or_else(|| storage_status(StorageError::NotFound))?;
        let marker = replica
            .candidate_marker
            .lock()
            .await
            .clone()
            .ok_or_else(|| storage_status(StorageError::SnapshotIdentityMismatch))?;
        if marker.external_candidate_binding != candidate_binding
            || marker.external_header != *candidate.header()
            || marker.external_manifest != *candidate.manifest()
            || (replica.external_binding != candidate_binding
                && replica.external_binding != active_binding)
        {
            return Ok(Response::new(activation_error_response(
                StorageError::SnapshotIdentityMismatch,
            )));
        }
        let internal_candidate = LogicalSnapshotCandidateReceipt::new(
            marker.internal_candidate_binding.clone(),
            marker.internal_header.clone(),
            marker.internal_manifest.clone(),
        )
        .map_err(storage_status)?;
        let internal_active = marker
            .internal_candidate_binding
            .to_builder()
            .role(BindingRole::Active)
            .build()
            .map_err(storage_status)?;
        let activation_store = replica
            .activation_store
            .clone()
            .unwrap_or_else(|| replica.store.clone());
        if let Err(error) = activation_store
            .activate_candidate(internal_candidate, internal_active.clone())
            .await
        {
            return Ok(Response::new(activation_error_response(error)));
        }
        if replica.external_binding == candidate_binding {
            let active_store: Arc<dyn StorageTckStore> = Arc::from(
                self.state
                    .factory
                    .open(internal_active.clone())
                    .await
                    .map_err(storage_status)?,
            );
            let active_replica = Arc::new(ReferenceReplica {
                external_binding: active_binding.clone(),
                internal_binding: internal_active,
                store: active_store,
                candidate_marker: Mutex::new(Some(marker)),
                activation_store: Some(activation_store),
            });
            let mut replicas = self
                .state
                .replicas
                .write()
                .map_err(|_| Status::internal("reference replica lock is poisoned"))?;
            let current = replicas
                .get(&namespace)
                .ok_or_else(|| storage_status(StorageError::NotFound))?;
            if current.external_binding == candidate_binding {
                replicas.insert(namespace, active_replica);
            } else if current.external_binding != active_binding {
                return Ok(Response::new(activation_error_response(
                    StorageError::SnapshotIdentityMismatch,
                )));
            }
        }
        if self
            .state
            .lose_next_activation_response
            .swap(false, Ordering::AcqRel)
        {
            return Err(Status::unavailable("injected activation response loss"));
        }
        Ok(Response::new(activation_success_response(&expected)))
    }

    async fn health(
        &self,
        request: Request<proto::HealthRequest>,
    ) -> Result<Response<proto::HealthResponse>, Status> {
        let request = request.into_inner();
        let context = validate_context(request.context.as_ref(), now_ms())
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let binding = decode_binding(
            context
                .binding
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("missing binding"))?,
        )
        .map_err(storage_status)?;
        let replica = match self.state.ensure_replica(binding).await {
            Ok(replica) => replica,
            Err(error) => {
                return Ok(Response::new(proto::HealthResponse {
                    status: Some(error_status(error)),
                    ready: false,
                    draining: false,
                    observed_applied_index: 0,
                }));
            }
        };
        let applied = replica
            .store
            .applied_index()
            .await
            .map_err(storage_status)?;
        Ok(Response::new(proto::HealthResponse {
            status: Some(ok_status(applied)),
            ready: true,
            draining: false,
            observed_applied_index: applied,
        }))
    }
}

async fn dispatch_read(
    session: &ReferenceSession,
    operation: proto::ReadOperation,
    body: &[u8],
) -> Result<(Vec<u8>, usize), StorageError> {
    match operation {
        proto::ReadOperation::GetVertex => {
            let (id, valid_at, transaction_at) = decode_point_request(body)?;
            let rows = session
                .view
                .get_vertex(VertexRead::new(
                    VertexId::new(id)?,
                    valid_at,
                    transaction_time(transaction_at)?,
                ))
                .await?
                .into_iter()
                .map(LogicalMutation::PutVertex)
                .collect::<Vec<_>>();
            Ok((encode_mutations(&rows)?, rows.len()))
        }
        proto::ReadOperation::GetEdge => {
            let (id, valid_at, transaction_at) = decode_point_request(body)?;
            let rows = session
                .view
                .get_edge(EdgeRead::new(
                    EdgeId::new(id)?,
                    valid_at,
                    transaction_time(transaction_at)?,
                ))
                .await?
                .into_iter()
                .map(LogicalMutation::PutEdge)
                .collect::<Vec<_>>();
            Ok((encode_mutations(&rows)?, rows.len()))
        }
        proto::ReadOperation::VertexHistory => {
            let (id, from, through, limit) = decode_history_request(body)?;
            let rows = session
                .view
                .vertex_history(VertexHistoryRead::new(
                    VertexId::new(id)?,
                    transaction_time(from)?,
                    transaction_time(through)?,
                    limit,
                )?)
                .await?
                .into_iter()
                .map(LogicalMutation::PutVertex)
                .collect::<Vec<_>>();
            Ok((encode_mutations(&rows)?, rows.len()))
        }
        proto::ReadOperation::EdgeHistory => {
            let (id, from, through, limit) = decode_history_request(body)?;
            let rows = session
                .view
                .edge_history(EdgeHistoryRead::new(
                    EdgeId::new(id)?,
                    transaction_time(from)?,
                    transaction_time(through)?,
                    limit,
                )?)
                .await?
                .into_iter()
                .map(LogicalMutation::PutEdge)
                .collect::<Vec<_>>();
            Ok((encode_mutations(&rows)?, rows.len()))
        }
        proto::ReadOperation::Expand => {
            let (vertex, direction, valid_at, transaction_at, limit) =
                decode_adjacency_request(body)?;
            let rows = session
                .view
                .expand(AdjacencyRead::new(
                    VertexId::new(vertex)?,
                    direction,
                    valid_at,
                    transaction_time(transaction_at)?,
                    limit,
                )?)
                .await?
                .into_iter()
                .map(LogicalMutation::PutEdge)
                .collect::<Vec<_>>();
            Ok((encode_mutations(&rows)?, rows.len()))
        }
        proto::ReadOperation::Changes => {
            let (after, through, limit) = decode_changes_request(body)?;
            let page = session
                .view
                .changes(ChangesRead::new(after, through, limit)?)
                .await?;
            let count = page.rows().len();
            Ok((encode_change_page(&page)?, count))
        }
        proto::ReadOperation::ScanVertices => {
            let (valid_at, transaction_at, after, limit) = decode_scan_request(body)?;
            let page = session
                .view
                .scan_vertices(VertexScan::new(
                    valid_at,
                    transaction_time(transaction_at)?,
                    after.map(VertexId::new).transpose()?,
                    limit,
                )?)
                .await?;
            let count = page.rows().len();
            Ok((encode_vertex_page(&page)?, count))
        }
        proto::ReadOperation::ScanEdges => {
            let (valid_at, transaction_at, after, limit) = decode_scan_request(body)?;
            let page = session
                .view
                .scan_edges(EdgeScan::new(
                    valid_at,
                    transaction_time(transaction_at)?,
                    after.map(EdgeId::new).transpose()?,
                    limit,
                )?)
                .await?;
            let count = page.rows().len();
            Ok((encode_edge_page(&page)?, count))
        }
        proto::ReadOperation::ReplicaMetadata => {
            let name = decode_text_request(body)?;
            let rows = session
                .replica
                .store
                .replica_metadata(&name)
                .await?
                .into_iter()
                .map(LogicalMutation::PutReplicaMetadata)
                .collect::<Vec<_>>();
            Ok((encode_mutations(&rows)?, rows.len()))
        }
        proto::ReadOperation::Pushdown => {
            let request = decode_pushdown_request(body, session.view.fence().clone())?;
            let outcome = session.replica.store.execute_pushdown(request).await?;
            let count = match &outcome {
                dtg_storage::PushdownOutcome::Exact(rows)
                | dtg_storage::PushdownOutcome::ResidualRequired { rows, .. } => rows.len(),
                dtg_storage::PushdownOutcome::Unsupported => 0,
            };
            Ok((encode_pushdown_outcome(&outcome)?, count))
        }
        proto::ReadOperation::Unspecified => Err(StorageError::Unsupported),
    }
}

async fn import_snapshot_stream(
    state: &Arc<ReferenceState>,
    mut stream: tonic::Streaming<proto::SnapshotImportFrame>,
) -> Result<Response<proto::ImportSnapshotResponse>, Status> {
    let first = stream
        .message()
        .await?
        .ok_or_else(|| Status::invalid_argument("snapshot import omitted header"))?;
    if first.kind != SNAPSHOT_HEADER_FRAME || first.sequence != 0 || first.stream_id.len() != 16 {
        return Err(Status::invalid_argument(
            "snapshot import did not begin with a valid header frame",
        ));
    }
    let context = validate_context(first.context.as_ref(), now_ms())
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    let external_binding = decode_binding(
        context
            .binding
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing binding"))?,
    )
    .map_err(storage_status)?;
    let replica = state
        .ensure_replica(external_binding.clone())
        .await
        .map_err(storage_status)?;
    let header_wire: proto::SnapshotHeaderPayload = decode_message_payload(
        first
            .payload
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("snapshot header omitted payload"))?,
    )
    .map_err(storage_status)?;
    let external_header = decode_header(&header_wire).map_err(storage_status)?;
    if !same_logical_identity(&external_binding, external_header.source_binding()) {
        return Err(storage_status(StorageError::SnapshotIdentityMismatch));
    }
    let internal_header = SnapshotHeader::new(
        external_header.snapshot_id(),
        replica.internal_binding.clone(),
        external_header.applied_index(),
        external_header.format_version(),
    )
    .map_err(storage_status)?;
    let mut writer = replica
        .store
        .begin_restore(replica.internal_binding.clone(), internal_header.clone())
        .await
        .map_err(storage_status)?;
    let stream_id = first.stream_id;
    let mut expected_sequence = 1_u64;
    let mut chunks = Vec::new();
    loop {
        let Some(frame) = stream.message().await? else {
            writer.abort().await.map_err(storage_status)?;
            return Err(Status::invalid_argument(
                "snapshot import ended without commit or abort",
            ));
        };
        if frame.stream_id != stream_id || frame.sequence != expected_sequence {
            writer.abort().await.map_err(storage_status)?;
            return Err(Status::invalid_argument(
                "snapshot import stream identity or sequence changed",
            ));
        }
        let frame_context = validate_context(frame.context.as_ref(), now_ms())
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let frame_binding = decode_binding(
            frame_context
                .binding
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("missing binding"))?,
        )
        .map_err(storage_status)?;
        if frame_binding != external_binding {
            writer.abort().await.map_err(storage_status)?;
            return Err(storage_status(StorageError::StaleBinding {
                expected: Box::new(external_binding.clone()),
                actual: Box::new(frame_binding),
            }));
        }
        expected_sequence += 1;
        match frame.kind {
            SNAPSHOT_CHUNK_FRAME => {
                let wire: proto::SnapshotChunkPayload =
                    decode_message_payload(frame.payload.as_ref().ok_or_else(|| {
                        Status::invalid_argument("snapshot chunk omitted payload")
                    })?)
                    .map_err(storage_status)?;
                let chunk = decode_chunk(&wire).map_err(storage_status)?;
                writer
                    .write_chunk(chunk.clone())
                    .await
                    .map_err(storage_status)?;
                chunks.push(chunk);
            }
            SNAPSHOT_COMMIT_FRAME => {
                let wire: proto::SnapshotManifestPayload =
                    decode_message_payload(frame.payload.as_ref().ok_or_else(|| {
                        Status::invalid_argument("snapshot commit omitted manifest")
                    })?)
                    .map_err(storage_status)?;
                let external_manifest = decode_manifest(&wire).map_err(storage_status)?;
                external_manifest
                    .validate(&external_header, &chunks)
                    .map_err(storage_status)?;
                let internal_manifest =
                    SnapshotManifest::new(&internal_header, &chunks).map_err(storage_status)?;
                writer
                    .commit(internal_manifest)
                    .await
                    .map_err(storage_status)?;
                if external_binding.role() == BindingRole::Candidate {
                    *replica.candidate_marker.lock().await = Some(CandidateMarker {
                        external_candidate_binding: external_binding.clone(),
                        internal_candidate_binding: replica.internal_binding.clone(),
                        external_header: external_header.clone(),
                        external_manifest: external_manifest.clone(),
                        internal_header: internal_header.clone(),
                        internal_manifest: SnapshotManifest::new(&internal_header, &chunks)
                            .map_err(storage_status)?,
                    });
                }
                let receipt =
                    SnapshotRestoreReceipt::new(external_binding.clone(), external_manifest);
                let wire = encode_restore_receipt(&receipt);
                return Ok(Response::new(proto::ImportSnapshotResponse {
                    status: Some(ok_status(receipt.manifest().chunk_count())),
                    receipt: Some(encode_message_payload(&wire).map_err(storage_status)?),
                }));
            }
            SNAPSHOT_ABORT_FRAME => {
                writer.abort().await.map_err(storage_status)?;
                return Ok(Response::new(proto::ImportSnapshotResponse {
                    status: Some(ok_status(0)),
                    receipt: None,
                }));
            }
            _ => {
                writer.abort().await.map_err(storage_status)?;
                return Err(Status::invalid_argument(
                    "unexpected snapshot import frame kind",
                ));
            }
        }
    }
}

fn same_logical_identity(left: &ReplicaBinding, right: &ReplicaBinding) -> bool {
    left.cluster_id() == right.cluster_id()
        && left.graph_id() == right.graph_id()
        && left.shard_id() == right.shard_id()
}

fn same_binding_except_role(left: &ReplicaBinding, right: &ReplicaBinding) -> bool {
    left.to_builder()
        .role(right.role())
        .build()
        .is_ok_and(|normalized| normalized == *right)
}

pub struct ReferenceStorageTckFactory {
    uri: String,
    provider_name: String,
    capabilities: CapabilityManifest,
    state: Arc<ReferenceState>,
}

impl StorageTckFactory for ReferenceStorageTckFactory {
    fn capabilities(&self) -> CapabilityManifest {
        self.capabilities.clone()
    }

    fn binding(
        &self,
        namespace: &str,
        backend_generation: u64,
    ) -> Result<ReplicaBinding, StorageError> {
        let provider = ProviderKind::Remote(self.provider_name.clone());
        let class = BackendClass::new(
            provider.clone(),
            1,
            1,
            self.capabilities.names().map(str::to_owned),
        )?;
        ReplicaBinding::builder()
            .cluster_id(1)
            .graph_id(7)
            .shard_id(11)
            .placement_epoch(13)
            .replica_id(17)
            .backend_generation(backend_generation)
            .backend_class_digest(class.digest())
            .provider_kind(provider)
            .contract_version(1)
            .layout_version(1)
            .capability_digest(self.capabilities.digest())
            .namespace_id(namespace)
            .endpoint_profile_ref("remote-reference")
            .credential_ref("remote-reference")
            .role(BindingRole::Active)
            .build()
    }

    fn open(&self, binding: ReplicaBinding) -> StoreFuture<'_, Box<dyn StorageTckStore>> {
        Box::pin(async move {
            let client = StorageRemoteClient::connect(self.uri.clone(), binding)
                .await
                .map_err(remote_storage_error)?;
            Ok(Box::new(ReferenceStorageTckStore {
                client,
                state: self.state.clone(),
            }) as Box<dyn StorageTckStore>)
        })
    }
}

struct ReferenceStorageTckStore {
    client: StorageRemoteClient,
    state: Arc<ReferenceState>,
}

impl ReplicaStateStore for ReferenceStorageTckStore {
    fn binding(&self) -> &ReplicaBinding {
        ReplicaStateStore::binding(&self.client)
    }

    fn applied_index(&self) -> StoreFuture<'_, u64> {
        self.client.applied_index()
    }

    fn replica_metadata<'a>(
        &'a self,
        name: &'a str,
    ) -> StoreFuture<'a, Option<dtg_storage::ReplicaMetadata>> {
        self.client.replica_metadata(name)
    }

    fn apply(
        &self,
        batch: dtg_storage::CommittedShardBatch,
    ) -> StoreFuture<'_, dtg_storage::ApplyReceipt> {
        self.client.apply(batch)
    }

    fn begin_read_view(
        &self,
        fence: dtg_storage::ReadFence,
    ) -> StoreFuture<'_, Box<dyn dtg_storage::TemporalReadView>> {
        self.client.begin_read_view(fence)
    }
}

impl dtg_storage::LogicalSnapshotSource for ReferenceStorageTckStore {
    fn begin_snapshot(
        &self,
        fence: dtg_storage::ReadFence,
        request: dtg_storage::SnapshotRequest,
    ) -> StoreFuture<'_, Box<dyn dtg_storage::LogicalSnapshotReader>> {
        self.client.begin_snapshot(fence, request)
    }
}

impl dtg_storage::LogicalSnapshotSink for ReferenceStorageTckStore {
    fn begin_restore(
        &self,
        binding: ReplicaBinding,
        header: dtg_storage::SnapshotHeader,
    ) -> StoreFuture<'_, Box<dyn dtg_storage::LogicalSnapshotWriter>> {
        self.client.begin_restore(binding, header)
    }
}

impl dtg_storage::PushdownExecutor for ReferenceStorageTckStore {
    fn binding(&self) -> &ReplicaBinding {
        dtg_storage::PushdownExecutor::binding(&self.client)
    }

    fn capabilities(&self) -> &CapabilityManifest {
        self.client.capabilities()
    }

    fn execute_pushdown(
        &self,
        request: dtg_storage::PushdownRequest,
    ) -> StoreFuture<'_, dtg_storage::PushdownOutcome> {
        self.client.execute_pushdown(request)
    }
}

impl StorageTckStore for ReferenceStorageTckStore {
    fn arm_apply_failure_after(&self, staged_mutations: usize) -> Result<(), StorageError> {
        let replicas = self
            .state
            .replicas
            .read()
            .map_err(|_| StorageError::Internal("reference replica lock is poisoned".into()))?;
        let replica = replicas
            .get(self.client.binding_ref().namespace_id().as_str())
            .ok_or(StorageError::NotFound)?;
        if replica.external_binding != *self.client.binding_ref() {
            return Err(StorageError::StaleBinding {
                expected: Box::new(replica.external_binding.clone()),
                actual: Box::new(self.client.binding_ref().clone()),
            });
        }
        replica.store.arm_apply_failure_after(staged_mutations)
    }

    fn activate_candidate(
        &self,
        candidate: LogicalSnapshotCandidateReceipt,
        active_binding: ReplicaBinding,
    ) -> StoreFuture<'_, LogicalReplicaActivationReceipt> {
        dtg_storage::LogicalReplicaActivation::activate_candidate(
            &self.client,
            candidate,
            active_binding,
        )
    }
}

fn remote_storage_error(error: RemoteError) -> StorageError {
    match error {
        RemoteError::Storage(error) => error,
        error => StorageError::Internal(error.to_string()),
    }
}

fn storage_status(error: StorageError) -> Status {
    Status::failed_precondition(error.to_string())
}

fn handshake_response(
    config: &ReferenceServerConfig,
    capabilities: &CapabilityManifest,
    status: proto::TypedStatus,
    applied: u64,
) -> proto::HandshakeResponse {
    proto::HandshakeResponse {
        status: Some(status),
        protocol_major: config.protocol_major,
        protocol_minor: config.protocol_minor,
        contract_major: config.contract_major,
        contract_minor: config.contract_minor,
        capabilities: Some(proto::CapabilityManifest {
            names: capabilities.names().map(str::to_owned).collect(),
            digest: capabilities.digest().get().to_vec(),
        }),
        observed_applied_index: applied,
        max_message_bytes: MAX_MESSAGE_BYTES as u64,
        max_message_items: MAX_MESSAGE_ITEMS as u32,
    }
}

fn activation_success_response(
    receipt: &LogicalReplicaActivationReceipt,
) -> proto::ActivateResponse {
    proto::ActivateResponse {
        status: Some(ok_status(receipt.applied_index())),
        active_binding: Some(encode_binding(receipt.active_binding())),
        snapshot_id: receipt.snapshot_id().get().to_be_bytes().to_vec(),
        applied_index: receipt.applied_index(),
        content_digest: receipt.content_digest().get().to_vec(),
        format_version: receipt.format_version(),
    }
}

fn activation_error_response(error: StorageError) -> proto::ActivateResponse {
    proto::ActivateResponse {
        status: Some(error_status(error)),
        active_binding: None,
        snapshot_id: Vec::new(),
        applied_index: 0,
        content_digest: Vec::new(),
        format_version: 0,
    }
}

fn ok_status(applied: u64) -> proto::TypedStatus {
    proto::TypedStatus {
        code: "OK".into(),
        message: String::new(),
        retryable: false,
        idempotency_key: Vec::new(),
        observed_applied_index: applied,
        detail_u64_a: 0,
        detail_u64_b: 0,
        expected_binding: None,
        actual_binding: None,
    }
}

fn error_status(error: StorageError) -> proto::TypedStatus {
    let mut status = proto::TypedStatus {
        code: error.code().into(),
        message: error.to_string(),
        retryable: matches!(error, StorageError::Internal(_)),
        idempotency_key: Vec::new(),
        observed_applied_index: 0,
        detail_u64_a: 0,
        detail_u64_b: 0,
        expected_binding: None,
        actual_binding: None,
    };
    match error {
        StorageError::InjectedApplyFailure { staged_mutations } => {
            status.detail_u64_a = staged_mutations as u64;
        }
        StorageError::StaleBinding { expected, actual }
        | StorageError::NamespaceOwnerMismatch { expected, actual } => {
            status.expected_binding = Some(encode_binding(&expected));
            status.actual_binding = Some(encode_binding(&actual));
        }
        StorageError::NonMonotonicIndex { applied, proposed } => {
            status.detail_u64_a = applied;
            status.detail_u64_b = proposed;
            status.observed_applied_index = applied;
        }
        StorageError::ReplayMismatch { raft_index } => {
            status.detail_u64_a = raft_index;
        }
        StorageError::ReadFenceUnavailable { requested, applied } => {
            status.detail_u64_a = requested;
            status.detail_u64_b = applied;
            status.observed_applied_index = applied;
        }
        _ => {}
    }
    status
}
