use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use dtg_storage::{
    ApplyReceipt, CapabilityManifest, CommittedShardBatch, LogicalReplicaActivation,
    LogicalReplicaActivationReceipt, LogicalSnapshotCandidateReceipt, LogicalSnapshotReader,
    LogicalSnapshotSink, LogicalSnapshotSource, LogicalSnapshotWriter, ProviderKind,
    PushdownExecutor, PushdownOutcome, PushdownRequest, ReadFence, ReplicaBinding, ReplicaMetadata,
    ReplicaStateStore, SnapshotHeader, SnapshotRequest, StorageError, StoreFuture,
    TemporalReadView,
};
use dtg_storage_remote_protocol::{
    CONTRACT_MAJOR, CONTRACT_MINOR, MAX_MESSAGE_BYTES, MAX_MESSAGE_ITEMS, PROTOCOL_MAJOR,
    PROTOCOL_MINOR, bounded_payload,
    proto::{self, storage_client::StorageClient},
};
use tonic::transport::Channel;

use crate::auth::{RemoteAuthToken, sign_context};
use crate::codec::encode_mutations;
use crate::read_view::RemoteReadView;
use crate::snapshot::{RemoteSnapshotReader, RemoteSnapshotWriter, encode_header, encode_manifest};
use crate::wire::{decode_binding, digest, encode_binding};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RemoteError {
    Protocol { code: &'static str, message: String },
    Transport(String),
    Storage(StorageError),
}

impl RemoteError {
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Protocol { code, .. } => code,
            Self::Transport(_) => "DTG-REMOTE-TRANSPORT",
            Self::Storage(error) => error.code(),
        }
    }
}

impl fmt::Display for RemoteError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Protocol { code, message } => write!(formatter, "{code}: {message}"),
            Self::Transport(message) => write!(formatter, "DTG-REMOTE-TRANSPORT: {message}"),
            Self::Storage(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for RemoteError {}

impl From<StorageError> for RemoteError {
    fn from(value: StorageError) -> Self {
        Self::Storage(value)
    }
}

struct ClientInner {
    rpc: StorageClient<Channel>,
    binding: ReplicaBinding,
    capabilities: CapabilityManifest,
    request_sequence: AtomicU64,
    deadline: Duration,
    auth_token: RemoteAuthToken,
}

#[derive(Clone)]
pub struct StorageRemoteClient {
    inner: Arc<ClientInner>,
}

impl fmt::Debug for StorageRemoteClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageRemoteClient")
            .field("binding", &self.inner.binding)
            .finish_non_exhaustive()
    }
}

impl StorageRemoteClient {
    pub async fn connect(
        endpoint: impl Into<String>,
        binding: ReplicaBinding,
        auth_token: RemoteAuthToken,
    ) -> Result<Self, RemoteError> {
        if !matches!(binding.provider_kind(), ProviderKind::Remote(_)) {
            return Err(StorageError::InvalidBinding(
                "remote client requires a remote provider binding".into(),
            )
            .into());
        }
        let rpc = StorageClient::connect(endpoint.into())
            .await
            .map_err(|error| RemoteError::Transport(error.to_string()))?
            .max_decoding_message_size(MAX_MESSAGE_BYTES)
            .max_encoding_message_size(MAX_MESSAGE_BYTES);
        let provisional = Self {
            inner: Arc::new(ClientInner {
                rpc,
                binding,
                capabilities: CapabilityManifest::from_names(std::iter::empty::<String>())?,
                request_sequence: AtomicU64::new(1),
                deadline: Duration::from_secs(30),
                auth_token: auth_token.clone(),
            }),
        };
        let response = provisional
            .rpc()
            .handshake(proto::HandshakeRequest {
                context: Some(provisional.context()),
            })
            .await
            .map_err(|error| RemoteError::Transport(error.to_string()))?
            .into_inner();
        if response.protocol_major != PROTOCOL_MAJOR {
            return Err(RemoteError::Protocol {
                code: "DTG-REMOTE-PROTOCOL-MAJOR",
                message: format!("server protocol major is {}", response.protocol_major),
            });
        }
        if response.contract_major != CONTRACT_MAJOR {
            return Err(RemoteError::Protocol {
                code: "DTG-REMOTE-CONTRACT-MAJOR",
                message: format!("server contract major is {}", response.contract_major),
            });
        }
        if response
            .protocol_minor
            .checked_sub(PROTOCOL_MINOR)
            .is_none()
            || response
                .contract_minor
                .checked_sub(CONTRACT_MINOR)
                .is_none()
        {
            return Err(RemoteError::Protocol {
                code: "DTG-REMOTE-PROTOCOL-MINOR",
                message: "server minor version is below the required floor".into(),
            });
        }
        let manifest = response.capabilities.ok_or_else(|| RemoteError::Protocol {
            code: "DTG-REMOTE-CAPABILITIES",
            message: "handshake omitted capabilities".into(),
        })?;
        let capabilities = CapabilityManifest::from_names(manifest.names)?;
        if capabilities.digest().get().as_slice() != manifest.digest
            || capabilities.digest() != provisional.inner.binding.capability_digest()
        {
            return Err(RemoteError::Protocol {
                code: "DTG-REMOTE-CAPABILITIES",
                message: "capability manifest or binding digest differs".into(),
            });
        }
        require_ok(response.status).map_err(RemoteError::Storage)?;
        Ok(Self {
            inner: Arc::new(ClientInner {
                rpc: provisional.inner.rpc.clone(),
                binding: provisional.inner.binding.clone(),
                capabilities,
                request_sequence: AtomicU64::new(2),
                deadline: provisional.inner.deadline,
                auth_token,
            }),
        })
    }

    pub(crate) fn rpc(&self) -> StorageClient<Channel> {
        self.inner.rpc.clone()
    }

    pub(crate) fn context(&self) -> proto::RequestContext {
        let sequence = self.inner.request_sequence.fetch_add(1, Ordering::Relaxed);
        let mut request_id = [0_u8; 16];
        request_id[..8].copy_from_slice(&sequence.to_be_bytes());
        request_id[8..].copy_from_slice(&self.inner.binding.replica_id().get().to_be_bytes());
        let mut context = proto::RequestContext {
            protocol_major: PROTOCOL_MAJOR,
            protocol_minor: PROTOCOL_MINOR,
            contract_major: CONTRACT_MAJOR,
            contract_minor: CONTRACT_MINOR,
            request_id: request_id.to_vec(),
            deadline_unix_ms: now_ms().saturating_add(self.inner.deadline.as_millis() as u64),
            binding: Some(encode_binding(&self.inner.binding)),
            auth_context: Vec::new(),
            max_response_bytes: MAX_MESSAGE_BYTES as u64,
            max_response_items: MAX_MESSAGE_ITEMS as u32,
        };
        context.auth_context = sign_context(&context, &self.inner.auth_token);
        context
    }

    pub fn capabilities(&self) -> &CapabilityManifest {
        &self.inner.capabilities
    }

    pub(crate) fn binding_ref(&self) -> &ReplicaBinding {
        &self.inner.binding
    }

    async fn health_applied_index(&self) -> Result<u64, StorageError> {
        let response = self
            .rpc()
            .health(proto::HealthRequest {
                context: Some(self.context()),
            })
            .await
            .map_err(|error| StorageError::Internal(format!("remote health RPC failed: {error}")))?
            .into_inner();
        require_ok(response.status)?;
        if !response.ready || response.draining {
            return Err(StorageError::Internal(
                "remote storage is not ready for requests".into(),
            ));
        }
        Ok(response.observed_applied_index)
    }
}

impl ReplicaStateStore for StorageRemoteClient {
    fn binding(&self) -> &ReplicaBinding {
        self.binding_ref()
    }

    fn applied_index(&self) -> StoreFuture<'_, u64> {
        Box::pin(async move { self.health_applied_index().await })
    }

    fn replica_metadata<'a>(&'a self, name: &'a str) -> StoreFuture<'a, Option<ReplicaMetadata>> {
        Box::pin(async move {
            let applied = self.health_applied_index().await?;
            let view = RemoteReadView::open(
                self.clone(),
                ReadFence::new(self.binding_ref().clone(), applied),
            )
            .await?;
            view.replica_metadata(name).await
        })
    }

    fn apply(&self, batch: CommittedShardBatch) -> StoreFuture<'_, ApplyReceipt> {
        Box::pin(async move {
            batch.validate()?;
            if batch.binding() != self.binding_ref() {
                return Err(StorageError::StaleBinding {
                    expected: Box::new(self.binding_ref().clone()),
                    actual: Box::new(batch.binding().clone()),
                });
            }
            let command_id = batch.command_id().get().to_be_bytes().to_vec();
            let payload = bounded_payload(
                encode_mutations(batch.mutations())?,
                batch.mutations().len(),
            )
            .map_err(protocol_storage_error)?;
            let response = self
                .rpc()
                .apply(proto::ApplyRequest {
                    context: Some(self.context()),
                    idempotency_key: command_id.clone(),
                    raft_term: batch.raft_term(),
                    raft_index: batch.raft_index(),
                    command_id,
                    mutation_digest: batch.mutation_digest().get().to_vec(),
                    mutations: Some(payload),
                })
                .await
                .map_err(|error| {
                    StorageError::Internal(format!("remote apply RPC failed: {error}"))
                })?
                .into_inner();
            require_ok(response.status)?;
            Ok(ApplyReceipt::new(&batch, response.replayed))
        })
    }

    fn begin_read_view(&self, fence: ReadFence) -> StoreFuture<'_, Box<dyn TemporalReadView>> {
        Box::pin(async move {
            Ok(Box::new(RemoteReadView::open(self.clone(), fence).await?)
                as Box<dyn TemporalReadView>)
        })
    }
}

impl LogicalSnapshotSource for StorageRemoteClient {
    fn begin_snapshot(
        &self,
        fence: ReadFence,
        request: SnapshotRequest,
    ) -> StoreFuture<'_, Box<dyn LogicalSnapshotReader>> {
        Box::pin(async move {
            Ok(
                Box::new(RemoteSnapshotReader::begin(self.clone(), fence, request).await?)
                    as Box<dyn LogicalSnapshotReader>,
            )
        })
    }
}

impl LogicalSnapshotSink for StorageRemoteClient {
    fn begin_restore(
        &self,
        binding: ReplicaBinding,
        header: SnapshotHeader,
    ) -> StoreFuture<'_, Box<dyn LogicalSnapshotWriter>> {
        Box::pin(async move {
            Ok(
                Box::new(RemoteSnapshotWriter::begin(self.clone(), binding, header).await?)
                    as Box<dyn LogicalSnapshotWriter>,
            )
        })
    }
}

impl PushdownExecutor for StorageRemoteClient {
    fn binding(&self) -> &ReplicaBinding {
        self.binding_ref()
    }

    fn capabilities(&self) -> &CapabilityManifest {
        self.capabilities()
    }

    fn execute_pushdown(&self, request: PushdownRequest) -> StoreFuture<'_, PushdownOutcome> {
        Box::pin(async move {
            request.validate()?;
            let view = RemoteReadView::open(self.clone(), request.fence().clone()).await?;
            view.execute_pushdown(&request).await
        })
    }
}

impl LogicalReplicaActivation for StorageRemoteClient {
    fn activate_candidate(
        &self,
        candidate: LogicalSnapshotCandidateReceipt,
        active_binding: ReplicaBinding,
    ) -> StoreFuture<'_, LogicalReplicaActivationReceipt> {
        Box::pin(async move {
            if candidate.candidate_binding() != self.binding_ref() {
                return Err(StorageError::SnapshotIdentityMismatch);
            }
            let expected =
                LogicalReplicaActivationReceipt::new(&candidate, active_binding.clone())?;
            let request = proto::ActivateRequest {
                context: Some(self.context()),
                candidate: Some(proto::SnapshotCandidatePayload {
                    candidate_binding: Some(encode_binding(candidate.candidate_binding())),
                    header: Some(encode_header(candidate.header())),
                    manifest: Some(encode_manifest(candidate.manifest())),
                }),
                active_binding: Some(encode_binding(&active_binding)),
            };
            let mut retries = 0;
            let response = loop {
                match self.rpc().activate(request.clone()).await {
                    Ok(response) => break response.into_inner(),
                    Err(error)
                        if retries < 3
                            && matches!(
                                error.code(),
                                tonic::Code::Unavailable | tonic::Code::Unknown
                            ) =>
                    {
                        retries += 1;
                    }
                    Err(error) => {
                        return Err(StorageError::Internal(format!(
                            "remote activation RPC failed: {error}"
                        )));
                    }
                }
            };
            require_ok(response.status)?;
            let response_binding = decode_binding(
                response
                    .active_binding
                    .as_ref()
                    .ok_or_else(|| StorageError::Internal("activation omitted binding".into()))?,
            )?;
            let snapshot_id = u128::from_be_bytes(
                response
                    .snapshot_id
                    .as_slice()
                    .try_into()
                    .map_err(|_| StorageError::SnapshotIdentityMismatch)?,
            );
            if response_binding != *expected.active_binding()
                || snapshot_id != expected.snapshot_id().get()
                || response.applied_index != expected.applied_index()
                || digest(&response.content_digest)? != expected.content_digest()
                || response.format_version != expected.format_version()
            {
                return Err(StorageError::SnapshotIdentityMismatch);
            }
            Ok(expected)
        })
    }
}

pub(crate) fn require_ok(
    status: Option<proto::TypedStatus>,
) -> Result<proto::TypedStatus, StorageError> {
    let status =
        status.ok_or_else(|| StorageError::Internal("remote response omitted status".into()))?;
    if status.code == "OK" {
        Ok(status)
    } else {
        Err(storage_error_from_status(status))
    }
}

fn storage_error_from_status(status: proto::TypedStatus) -> StorageError {
    let message = status.message;
    match status.code.as_str() {
        "DTG-STORAGE-BINDING" => StorageError::InvalidBinding(message),
        "DTG-STORAGE-CAPABILITY" => StorageError::InvalidCapability(message),
        "DTG-STORAGE-MUTATION" => StorageError::InvalidMutation(message),
        "DTG-STORAGE-BATCH" => StorageError::InvalidBatch(message),
        "DTG-STORAGE-INJECTED-APPLY" => StorageError::InjectedApplyFailure {
            staged_mutations: status.detail_u64_a as usize,
        },
        "DTG-STORAGE-STALE-BINDING" => binding_pair_error(
            status.expected_binding,
            status.actual_binding,
            true,
            message,
        ),
        "DTG-STORAGE-NAMESPACE-OWNER" => binding_pair_error(
            status.expected_binding,
            status.actual_binding,
            false,
            message,
        ),
        "DTG-STORAGE-RAFT-INDEX" => StorageError::NonMonotonicIndex {
            applied: status.detail_u64_a,
            proposed: status.detail_u64_b,
        },
        "DTG-STORAGE-REPLAY" => StorageError::ReplayMismatch {
            raft_index: status.detail_u64_a,
        },
        "DTG-STORAGE-READ-FENCE" => StorageError::ReadFenceUnavailable {
            requested: status.detail_u64_a,
            applied: status.detail_u64_b,
        },
        "DTG-STORAGE-CAPABILITY-DRIFT" => StorageError::CapabilityDrift,
        "DTG-STORAGE-UNSUPPORTED" => StorageError::Unsupported,
        "DTG-STORAGE-SNAPSHOT-CORRUPT" => StorageError::CorruptSnapshot(message),
        "DTG-STORAGE-SNAPSHOT-IDENTITY" => StorageError::SnapshotIdentityMismatch,
        "DTG-STORAGE-SNAPSHOT-NOT-EXHAUSTED" => StorageError::SnapshotNotExhausted,
        "DTG-STORAGE-NOT-FOUND" => StorageError::NotFound,
        "DTG-STORAGE-TCK" => StorageError::TckViolation(message),
        _ => StorageError::Internal(format!(
            "remote storage returned {}: {message}",
            status.code
        )),
    }
}

fn binding_pair_error(
    expected: Option<proto::Binding>,
    actual: Option<proto::Binding>,
    stale: bool,
    message: String,
) -> StorageError {
    match (
        expected.as_ref().map(decode_binding).transpose(),
        actual.as_ref().map(decode_binding).transpose(),
    ) {
        (Ok(Some(expected)), Ok(Some(actual))) if stale => StorageError::StaleBinding {
            expected: Box::new(expected),
            actual: Box::new(actual),
        },
        (Ok(Some(expected)), Ok(Some(actual))) => StorageError::NamespaceOwnerMismatch {
            expected: Box::new(expected),
            actual: Box::new(actual),
        },
        _ => StorageError::Internal(format!("invalid remote binding error: {message}")),
    }
}

pub(crate) fn protocol_storage_error(error: impl fmt::Display) -> StorageError {
    StorageError::Internal(format!("remote protocol error: {error}"))
}

pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
