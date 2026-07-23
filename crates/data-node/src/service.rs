use std::collections::VecDeque;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::fs::File;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use adapter_registry::MigrationStatus;
use cluster_protocol::proto::node_admin_service_server::NodeAdminService;
use cluster_protocol::proto::shard_service_server::ShardService;
use cluster_protocol::proto::{
    ActivateReplicaRequest, ActivateReplicaResponse, AdvanceAnalyticsArtifactFenceRequest,
    AdvanceAnalyticsArtifactFenceResponse, AnalyticsArtifactChunk, AnalyticsArtifactGeneration,
    AnalyticsArtifactKind as WireArtifactKind, BackendLifecyclePhase, BackendProfileSpec,
    BackendTransitionResponse, BeginBackendDualApplyRequest, ChangeMembershipRequest,
    ChangeMembershipResponse, DeleteAnalyticsArtifactGenerationRequest,
    DeleteAnalyticsArtifactGenerationResponse, DeleteReplicaRequest, DeleteReplicaResponse,
    EnsureReplicaRequest, EnsureReplicaResponse, ExecuteRequest, ExecuteResponse,
    ExportSnapshotRequest, FinishBackendMigrationRequest, GetAnalyticsArtifactGenerationRequest,
    GetBackendStatusRequest, GetBackendStatusResponse, GetMigrationReceiptRequest,
    GetMigrationReceiptResponse, InstallSnapshotResponse,
    ListAnalyticsArtifactGenerationHeadsRequest, ListAnalyticsArtifactGenerationHeadsResponse,
    ListAnalyticsArtifactGenerationsRequest, ListAnalyticsArtifactGenerationsResponse,
    PinAnalyticsArtifactGenerationRequest, PinAnalyticsArtifactGenerationResponse,
    PrepareBackendTargetRequest, PrepareBackendTargetResponse, PutAnalyticsArtifactChunkRequest,
    PutAnalyticsArtifactChunkResponse, ReadRequest, ReadResponse, ReplicaBootstrapProfile,
    ReplicaRole as WireReplicaRole, ReplicaStatusRequest, ReplicaStatusResponse, ScanBatch,
    ScanRequest, SnapshotChunk,
};
use cluster_protocol::{CommandPayload, CommonRequestContext, ProtocolError, ShardRequestContext};
use prost::Message as ProstMessage;
use storage_api::{KeySpan, KeyValue, Keyspace, LogicalKey};
use tokio::io::AsyncReadExt;
use tokio::sync::{Mutex, mpsc};
use tokio_stream::{Stream, StreamExt, wrappers::ReceiverStream};
use tonic::metadata::MetadataValue;
use tonic::{Code, Request, Response, Status};

use crate::{
    BackendProfile, BackendRuntimeStatus, BackendSlotState, ChunkAppendOutcome, DataNodeHost,
    EnsureReplicaOutcome, HostError, MigrationChunk, ProposalOutcome, ReplicaKey, ReplicaRole,
    ReplicaSpec, ReplicaStatus,
};

const READ_PLAN_MAGIC: [u8; 4] = *b"DTRK";
const READ_RESULT_MAGIC: [u8; 4] = *b"DTRV";
const SCAN_PLAN_MAGIC: [u8; 4] = *b"DTSK";
const SCAN_BATCH_MAGIC: [u8; 4] = *b"DTSB";
const READ_CODEC_VERSION: u16 = 1;
const MAX_READ_KEYS: usize = 4_096;
const MAX_READ_KEY_BYTES: usize = 1024 * 1024;
const MAX_READ_RESULT_BYTES: usize = 16 * 1024 * 1024;
const MAX_SCAN_PLAN_BYTES: usize = 2 * 1024 * 1024;
const MIN_SCAN_BATCH_BYTES: usize = 1024;
const MAX_SCAN_BATCH_BYTES: usize = 4 * 1024 * 1024;
const MAX_SCAN_ROWS: usize = 65_536;
const REPLICA_PROFILE_MAGIC: [u8; 4] = *b"DTRF";
const MAX_PROFILE_VOTERS: usize = 64;
const MAX_REPLICA_DIRECTORY_BYTES: usize = 255;
const MAX_REPLICA_PROFILE_BYTES: usize = 1024 * 1024;
const SNAPSHOT_INSTALL_STEP: u32 = 2;
const SNAPSHOT_EXPORT_STEP: u32 = 1;
const SNAPSHOT_STREAM_CHUNK_BYTES: usize = 1024 * 1024;
const SNAPSHOT_OUTCOME_MAGIC: [u8; 4] = *b"DTSO";
const SNAPSHOT_OUTCOME_VERSION: u16 = 1;
const SNAPSHOT_OUTCOME_BYTES: usize = 50;
const PROPOSAL_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(2);
const ARTIFACT_READ_BATCH_CHUNKS: usize = 8;

#[derive(Clone, Copy, Debug)]
struct StableArtifactHeadObservation {
    identity: shard_runtime::AnalyticsArtifactGenerationHeadIdentity,
    head: shard_runtime::AnalyticsArtifactGenerationHead,
    pin: Option<shard_runtime::AnalyticsArtifactGenerationPin>,
}

type DataNodeArtifactBatchFuture =
    Pin<Box<dyn Future<Output = Result<(usize, Vec<Option<Vec<u8>>>), Status>> + Send + 'static>>;

#[doc(hidden)]
pub struct DataNodeArtifactStream {
    host: Arc<DataNodeHost>,
    key: ReplicaKey,
    job_id: u128,
    kind: raft_command::AnalyticsArtifactKindV1,
    generation: u64,
    count: usize,
    expected_total_bytes: u64,
    expected_content_digest: [u8; 32],
    expected_tail_digest: [u8; 32],
    applied_index: u64,
    deadline_unix_ms: u64,
    next_start: usize,
    pending: Option<DataNodeArtifactBatchFuture>,
    buffered: VecDeque<AnalyticsArtifactChunk>,
    deferred_error: Option<Status>,
    previous_digest: [u8; 32],
    total_bytes: u64,
    content_hasher: blake3::Hasher,
    terminal: bool,
}

impl fmt::Debug for DataNodeArtifactStream {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DataNodeArtifactStream")
            .field("key", &self.key)
            .field("count", &self.count)
            .field("next_start", &self.next_start)
            .field("buffered", &self.buffered.len())
            .field("terminal", &self.terminal)
            .finish_non_exhaustive()
    }
}

impl DataNodeArtifactStream {
    fn start_next_batch(&mut self) -> Result<(), Status> {
        if unix_time_ms()? >= self.deadline_unix_ms {
            return Err(Status::deadline_exceeded(
                "analytics artifact read deadline expired",
            ));
        }
        let start = self.next_start;
        let end = (start + ARTIFACT_READ_BATCH_CHUNKS).min(self.count);
        let keys = (start..end)
            .map(|ordinal| {
                shard_runtime::analytics_artifact_chunk_key(
                    self.job_id,
                    self.kind,
                    self.generation,
                    u64::try_from(ordinal).expect("bounded artifact ordinal"),
                )
            })
            .collect::<Vec<_>>();
        let host = Arc::clone(&self.host);
        let key = self.key;
        self.next_start = end;
        self.pending = Some(Box::pin(async move {
            let values = host
                .multi_get(key, keys.clone())
                .await
                .map_err(artifact_host_status)?;
            if values.len() != keys.len() {
                return Err(Status::data_loss(
                    "analytics artifact read returned an incomplete key batch",
                ));
            }
            Ok((start, values))
        }));
        Ok(())
    }

    fn retain_batch(&mut self, start: usize, values: Vec<Option<Vec<u8>>>) -> Result<(), Status> {
        let mut decoded = VecDeque::with_capacity(values.len());
        for (offset, value) in values.into_iter().enumerate() {
            let value = value.ok_or_else(|| {
                Status::data_loss("analytics artifact generation has a missing chunk")
            })?;
            let chunk = shard_runtime::decode_analytics_artifact_chunk(&value).map_err(|_| {
                Status::data_loss("analytics artifact chunk failed integrity validation")
            })?;
            if chunk.previous_digest() != self.previous_digest {
                return Err(Status::failed_precondition(
                    "analytics artifact digest chain is discontinuous",
                ));
            }
            let payload_bytes = u64::try_from(chunk.payload().len())
                .map_err(|_| Status::data_loss("analytics artifact payload length overflow"))?;
            self.total_bytes = self
                .total_bytes
                .checked_add(payload_bytes)
                .filter(|total| *total <= self.expected_total_bytes)
                .ok_or_else(|| {
                    Status::failed_precondition(
                        "analytics artifact exceeds the requested total byte count",
                    )
                })?;
            self.content_hasher.update(chunk.payload());
            self.previous_digest = chunk.payload_digest();
            decoded.push_back(AnalyticsArtifactChunk {
                ordinal: u64::try_from(start + offset).expect("bounded artifact ordinal"),
                applied_index: self.applied_index,
                previous_digest: chunk.previous_digest().to_vec(),
                payload_digest: chunk.payload_digest().to_vec(),
                payload: chunk.payload().to_vec(),
            });
        }
        if self.next_start == self.count {
            let final_error = if self.previous_digest != self.expected_tail_digest {
                Some(Status::failed_precondition(
                    "analytics artifact generation tail differs from its head",
                ))
            } else if self.total_bytes != self.expected_total_bytes {
                Some(Status::failed_precondition(
                    "analytics artifact total byte count differs from the request",
                ))
            } else if *self.content_hasher.finalize().as_bytes() != self.expected_content_digest {
                Some(Status::failed_precondition(
                    "analytics artifact content digest differs from the request",
                ))
            } else {
                None
            };
            if let Some(status) = final_error {
                decoded.pop_back();
                self.deferred_error = Some(status);
            }
        }
        self.buffered = decoded;
        Ok(())
    }
}

impl Stream for DataNodeArtifactStream {
    type Item = Result<AnalyticsArtifactChunk, Status>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if self.terminal {
                return Poll::Ready(None);
            }
            if let Some(chunk) = self.buffered.pop_front() {
                return Poll::Ready(Some(Ok(chunk)));
            }
            if let Some(status) = self.deferred_error.take() {
                self.terminal = true;
                return Poll::Ready(Some(Err(status)));
            }
            if let Some(pending) = self.pending.as_mut() {
                match pending.as_mut().poll(context) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok((start, values))) => {
                        self.pending = None;
                        if let Err(status) = self.retain_batch(start, values) {
                            self.terminal = true;
                            return Poll::Ready(Some(Err(status)));
                        }
                        continue;
                    }
                    Poll::Ready(Err(status)) => {
                        self.pending = None;
                        self.terminal = true;
                        return Poll::Ready(Some(Err(status)));
                    }
                }
            }
            if self.next_start < self.count {
                if let Err(status) = self.start_next_batch() {
                    self.terminal = true;
                    return Poll::Ready(Some(Err(status)));
                }
                continue;
            }
            self.terminal = true;
            return Poll::Ready(None);
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DataOperation {
    Execute,
    Read,
    Scan,
    PutAnalyticsArtifact,
    PinAnalyticsArtifact,
    GetAnalyticsArtifact,
    ListAnalyticsArtifactGenerations,
    ListAnalyticsArtifactGenerationHeads,
    DeleteAnalyticsArtifact,
    AdvanceAnalyticsArtifactFence,
    ExportSnapshot,
    InstallSnapshot,
    ReplicaStatus,
    EnsureReplica,
    ChangeMembership,
    ActivateReplica,
    DeleteReplica,
    MigrationReceipt,
    BackendManagement,
}

pub trait RequestAuthorizer: Send + Sync + 'static {
    fn authorize(
        &self,
        context: &CommonRequestContext,
        operation: DataOperation,
    ) -> Result<(), Status>;
}

struct AllowAllAuthorizer;

impl RequestAuthorizer for AllowAllAuthorizer {
    fn authorize(
        &self,
        _context: &CommonRequestContext,
        _operation: DataOperation,
    ) -> Result<(), Status> {
        Ok(())
    }
}

#[derive(Clone)]
pub struct DataNodeGrpcService {
    host: Arc<DataNodeHost>,
    authorizer: Arc<dyn RequestAuthorizer>,
    migration_gate: Arc<Mutex<()>>,
}

impl DataNodeGrpcService {
    #[must_use]
    pub fn new(host: Arc<DataNodeHost>) -> Self {
        Self {
            host,
            authorizer: Arc::new(AllowAllAuthorizer),
            migration_gate: Arc::new(Mutex::new(())),
        }
    }

    #[must_use]
    pub fn with_authorizer(
        host: Arc<DataNodeHost>,
        authorizer: Arc<dyn RequestAuthorizer>,
    ) -> Self {
        Self {
            host,
            authorizer,
            migration_gate: Arc::new(Mutex::new(())),
        }
    }

    fn validate_common(
        &self,
        context: Option<cluster_protocol::proto::RequestContext>,
        operation: DataOperation,
    ) -> Result<CommonRequestContext, Status> {
        let context: CommonRequestContext = context
            .ok_or_else(|| Status::invalid_argument("missing request context"))?
            .try_into()
            .map_err(protocol_status)?;
        if context.cluster_id() != self.host.identity().cluster_id() {
            return Err(Status::permission_denied("cluster identity mismatch"));
        }
        context
            .ensure_active_at(unix_time_ms()?)
            .map_err(protocol_status)?;
        self.authorizer.authorize(&context, operation)?;
        Ok(context)
    }

    fn validate(
        &self,
        context: Option<cluster_protocol::proto::ShardContext>,
        operation: DataOperation,
    ) -> Result<(ShardRequestContext, ReplicaKey, u128), Status> {
        let context: ShardRequestContext = context
            .ok_or_else(|| Status::invalid_argument("missing Shard context"))?
            .try_into()
            .map_err(protocol_status)?;
        if context.common().cluster_id() != self.host.identity().cluster_id() {
            return Err(Status::permission_denied("cluster identity mismatch"));
        }
        context
            .common()
            .ensure_active_at(unix_time_ms()?)
            .map_err(protocol_status)?;
        self.authorizer.authorize(context.common(), operation)?;
        let key = ReplicaKey::new(context.graph_id(), context.shard_id())
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let request_id = u128::from_be_bytes(*context.common().request_id());
        Ok((context, key, request_id))
    }

    async fn require_leader(&self, key: ReplicaKey) -> Result<ReplicaStatus, Status> {
        let status = self.host.status(key).await.map_err(host_status)?;
        if !status.is_leader() {
            return Err(not_leader_status(status.leader_id()));
        }
        Ok(status)
    }

    async fn propose_until_applied(
        &self,
        key: ReplicaKey,
        placement_epoch: u64,
        request_id: u128,
        deadline_unix_ms: u64,
        command: Vec<u8>,
    ) -> Result<ProposalOutcome, Status> {
        let mut outcome = self
            .host
            .propose_with_outcome(key, placement_epoch, request_id, command.clone())
            .await;
        while matches!(outcome, Err(HostError::ProposalPending { .. })) {
            if unix_time_ms()? >= deadline_unix_ms {
                return Err(Status::deadline_exceeded(
                    "Raft proposal did not apply before request deadline",
                ));
            }
            tokio::time::sleep(PROPOSAL_POLL_INTERVAL).await;
            outcome = self
                .host
                .proposal_status(key, request_id, command.clone())
                .await;
        }
        outcome.map_err(artifact_host_status)
    }

    async fn finish_backend_migration(
        &self,
        request: FinishBackendMigrationRequest,
        cutover: bool,
    ) -> Result<Response<BackendTransitionResponse>, Status> {
        let (context, key, request_id) =
            self.validate(request.context, DataOperation::BackendManagement)?;
        validate_operation_id(&request.operation_id, request_id)?;
        self.require_leader(key).await?;
        let digest = profile_digest(&request.target_profile_digest)?;
        let body = if cutover {
            raft_command::CommandBodyV1::CutoverBackend(raft_command::CutoverBackendV1 {
                source_generation: request.source_generation,
                target_generation: request.target_generation,
                target_profile_digest: digest,
            })
        } else {
            raft_command::CommandBodyV1::AbortBackendMigration(
                raft_command::AbortBackendMigrationV1 {
                    source_generation: request.source_generation,
                    target_generation: request.target_generation,
                    target_profile_digest: digest,
                },
            )
        };
        let command = raft_command::CommandEnvelopeV1::new(
            key.shard_id(),
            context.placement_epoch(),
            request_id,
            body,
        )
        .encode()
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
        backend_transition_response(
            self.host
                .propose_backend_transition(key, context.placement_epoch(), request_id, command)
                .await
                .map_err(host_status)?,
        )
    }
}

#[tonic::async_trait]
impl ShardService for DataNodeGrpcService {
    async fn execute(
        &self,
        request: Request<ExecuteRequest>,
    ) -> Result<Response<ExecuteResponse>, Status> {
        let request = request.into_inner();
        let (context, key, request_id) = self.validate(request.context, DataOperation::Execute)?;
        let command = CommandPayload::try_from(request.command).map_err(protocol_status)?;
        let envelope = raft_command::CommandEnvelopeV1::decode(command.as_bytes())
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        if matches!(
            envelope.body,
            raft_command::CommandBodyV1::PutAnalyticsArtifactChunk(_)
                | raft_command::CommandBodyV1::PinAnalyticsArtifactGeneration(_)
                | raft_command::CommandBodyV1::DeleteAnalyticsArtifactGeneration(_)
                | raft_command::CommandBodyV1::AdvanceAnalyticsArtifactFence(_)
        ) {
            return Err(Status::permission_denied(
                "analytics artifact commands require the explicit artifact RPC",
            ));
        }
        self.require_leader(key).await?;
        let command = command.into_bytes();
        let mut outcome = self
            .host
            .propose_with_outcome(key, context.placement_epoch(), request_id, command.clone())
            .await;
        while matches!(outcome, Err(HostError::ProposalPending { .. })) {
            if unix_time_ms()? >= context.common().deadline_unix_ms() {
                return Err(Status::deadline_exceeded(
                    "Raft proposal did not apply before request deadline",
                ));
            }
            tokio::time::sleep(PROPOSAL_POLL_INTERVAL).await;
            outcome = self
                .host
                .proposal_status(key, request_id, command.clone())
                .await;
        }
        let outcome = outcome.map_err(host_status)?;
        Ok(Response::new(ExecuteResponse {
            raft_index: outcome.status().applied_index(),
            result: Vec::new(),
            duplicate: outcome.duplicate(),
        }))
    }

    async fn read(&self, request: Request<ReadRequest>) -> Result<Response<ReadResponse>, Status> {
        let request = request.into_inner();
        let (context, key, _) = self.validate(request.context, DataOperation::Read)?;
        if !request.read_proof.is_empty() {
            return Err(Status::invalid_argument(
                "follower read proofs are not enabled on the leader-only P0 path",
            ));
        }
        let status = self.require_leader(key).await?;
        if status.placement_epoch() != context.placement_epoch() {
            return Err(stale_epoch_status(status.placement_epoch()));
        }
        let keys = decode_key_read_plan(&request.plan)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        if keys.iter().any(|key| key.keyspace() == Keyspace::Meta) {
            return Err(Status::permission_denied(
                "generic reads cannot access reserved metadata keys",
            ));
        }
        let values = self.host.multi_get(key, keys).await.map_err(host_status)?;
        let result = encode_key_read_result(&values)
            .map_err(|error| Status::resource_exhausted(error.to_string()))?;
        Ok(Response::new(ReadResponse {
            applied_index: status.applied_index(),
            result,
        }))
    }

    async fn put_analytics_artifact_chunk(
        &self,
        request: Request<PutAnalyticsArtifactChunkRequest>,
    ) -> Result<Response<PutAnalyticsArtifactChunkResponse>, Status> {
        let request = request.into_inner();
        let (context, key, request_id) =
            self.validate(request.context, DataOperation::PutAnalyticsArtifact)?;
        let job_id = artifact_job_id(&request.job_id)?;
        let kind = artifact_kind(request.kind)?;
        if request.generation == 0 {
            return Err(Status::invalid_argument(
                "analytics artifact generation must be non-zero",
            ));
        }
        if request.created_at_unix_ms == 0 {
            return Err(Status::invalid_argument(
                "analytics artifact creation time must be non-zero",
            ));
        }
        let previous_digest = artifact_digest(&request.previous_digest, "previous digest")?;
        let command = raft_command::PutAnalyticsArtifactChunkV1::new(
            job_id,
            kind,
            request.generation,
            request.created_at_unix_ms,
            request.ordinal,
            previous_digest,
            request.payload,
        )
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
        self.require_leader(key).await?;
        let command = raft_command::CommandEnvelopeV1::new(
            key.shard_id(),
            context.placement_epoch(),
            request_id,
            raft_command::CommandBodyV1::PutAnalyticsArtifactChunk(command),
        )
        .encode()
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let outcome = self
            .propose_until_applied(
                key,
                context.placement_epoch(),
                request_id,
                context.common().deadline_unix_ms(),
                command,
            )
            .await?;
        Ok(Response::new(PutAnalyticsArtifactChunkResponse {
            raft_index: outcome.status().applied_index(),
            duplicate: outcome.duplicate(),
        }))
    }

    async fn pin_analytics_artifact_generation(
        &self,
        request: Request<PinAnalyticsArtifactGenerationRequest>,
    ) -> Result<Response<PinAnalyticsArtifactGenerationResponse>, Status> {
        let request = request.into_inner();
        let (context, key, request_id) =
            self.validate(request.context, DataOperation::PinAnalyticsArtifact)?;
        let expected_count = artifact_chunk_count(request.expected_chunk_count)?;
        let expected_content_digest = artifact_manifest(
            request.generation,
            expected_count,
            request.expected_total_bytes,
            &request.expected_content_digest,
        )?;
        let command = raft_command::PinAnalyticsArtifactGenerationV1::new(
            artifact_job_id(&request.job_id)?,
            artifact_kind(request.kind)?,
            request.generation,
            u16::try_from(expected_count).map_err(|_| {
                Status::invalid_argument("analytics artifact chunk count is out of range")
            })?,
            request.expected_total_bytes,
            expected_content_digest,
        )
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
        self.require_leader(key).await?;
        let command = raft_command::CommandEnvelopeV1::new(
            key.shard_id(),
            context.placement_epoch(),
            request_id,
            raft_command::CommandBodyV1::PinAnalyticsArtifactGeneration(command),
        )
        .encode()
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let outcome = self
            .propose_until_applied(
                key,
                context.placement_epoch(),
                request_id,
                context.common().deadline_unix_ms(),
                command,
            )
            .await?;
        Ok(Response::new(PinAnalyticsArtifactGenerationResponse {
            raft_index: outcome.status().applied_index(),
            duplicate: outcome.duplicate(),
        }))
    }

    type GetAnalyticsArtifactGenerationStream = DataNodeArtifactStream;

    async fn get_analytics_artifact_generation(
        &self,
        request: Request<GetAnalyticsArtifactGenerationRequest>,
    ) -> Result<Response<Self::GetAnalyticsArtifactGenerationStream>, Status> {
        let request = request.into_inner();
        let (context, key, request_id) =
            self.validate(request.context, DataOperation::GetAnalyticsArtifact)?;
        let job_id = artifact_job_id(&request.job_id)?;
        let kind = artifact_kind(request.kind)?;
        let expected_count = artifact_chunk_count(request.expected_chunk_count)?;
        let expected_content_digest = artifact_manifest(
            request.generation,
            expected_count,
            request.expected_total_bytes,
            &request.expected_content_digest,
        )?;
        let deadline = context.common().deadline_unix_ms();
        let read_deadline = monotonic_deadline(deadline)?;
        let applied_index = self
            .host
            .leader_read_permit(key, context.placement_epoch(), request_id, read_deadline)
            .await
            .map_err(artifact_host_status)?;
        let pin_key =
            shard_runtime::analytics_artifact_generation_pin_key(job_id, kind, request.generation);
        let head_key =
            shard_runtime::analytics_artifact_generation_head_key(job_id, kind, request.generation);
        let mut records = self
            .host
            .multi_get(key, vec![pin_key, head_key])
            .await
            .map_err(artifact_host_status)?;
        if records.len() != 2 {
            return Err(Status::data_loss(
                "analytics artifact pin/head read returned an invalid result count",
            ));
        }
        let pin = records.remove(0).ok_or_else(|| {
            Status::failed_precondition("analytics artifact generation is not pinned")
        })?;
        let pin = shard_runtime::decode_analytics_artifact_generation_pin(&pin).map_err(|_| {
            Status::data_loss("analytics artifact generation pin failed integrity validation")
        })?;
        if usize::from(pin.expected_chunk_count()) != expected_count
            || pin.expected_total_bytes() != request.expected_total_bytes
            || pin.expected_content_digest() != expected_content_digest
        {
            return Err(Status::failed_precondition(
                "analytics artifact generation pin differs from the requested manifest",
            ));
        }
        let head = records
            .pop()
            .flatten()
            .ok_or_else(|| Status::data_loss("analytics artifact generation has no head"))?;
        let head =
            shard_runtime::decode_analytics_artifact_generation_head(&head).map_err(|_| {
                Status::data_loss("analytics artifact generation head failed integrity validation")
            })?;
        if usize::from(head.count()) != expected_count {
            return Err(Status::failed_precondition(
                "analytics artifact generation count differs from the requested count",
            ));
        }
        let expected_tail_digest = head.last_digest();
        Ok(Response::new(DataNodeArtifactStream {
            host: Arc::clone(&self.host),
            key,
            job_id,
            kind,
            generation: request.generation,
            count: expected_count,
            expected_total_bytes: request.expected_total_bytes,
            expected_content_digest,
            expected_tail_digest,
            applied_index,
            deadline_unix_ms: deadline,
            next_start: 0,
            pending: None,
            buffered: VecDeque::with_capacity(ARTIFACT_READ_BATCH_CHUNKS),
            deferred_error: None,
            previous_digest: [0; 32],
            total_bytes: 0,
            content_hasher: blake3::Hasher::new(),
            terminal: false,
        }))
    }

    async fn delete_analytics_artifact_generation(
        &self,
        request: Request<DeleteAnalyticsArtifactGenerationRequest>,
    ) -> Result<Response<DeleteAnalyticsArtifactGenerationResponse>, Status> {
        let request = request.into_inner();
        let (context, key, request_id) =
            self.validate(request.context, DataOperation::DeleteAnalyticsArtifact)?;
        let command = raft_command::DeleteAnalyticsArtifactGenerationV1::new_with_gc_epoch(
            artifact_job_id(&request.job_id)?,
            artifact_kind(request.kind)?,
            request.generation,
            request.gc_epoch,
        )
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
        self.require_leader(key).await?;
        let command = raft_command::CommandEnvelopeV1::new(
            key.shard_id(),
            context.placement_epoch(),
            request_id,
            raft_command::CommandBodyV1::DeleteAnalyticsArtifactGeneration(command),
        )
        .encode()
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let outcome = self
            .propose_until_applied(
                key,
                context.placement_epoch(),
                request_id,
                context.common().deadline_unix_ms(),
                command,
            )
            .await?;
        Ok(Response::new(DeleteAnalyticsArtifactGenerationResponse {
            raft_index: outcome.status().applied_index(),
            duplicate: outcome.duplicate(),
        }))
    }

    async fn advance_analytics_artifact_fence(
        &self,
        request: Request<AdvanceAnalyticsArtifactFenceRequest>,
    ) -> Result<Response<AdvanceAnalyticsArtifactFenceResponse>, Status> {
        let request = request.into_inner();
        let (context, key, request_id) = self.validate(
            request.context,
            DataOperation::AdvanceAnalyticsArtifactFence,
        )?;
        let command = raft_command::AdvanceAnalyticsArtifactFenceV1::new(
            artifact_job_id(&request.job_id)?,
            artifact_kind(request.kind)?,
            request.generation,
            request.gc_epoch,
        )
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
        self.require_leader(key).await?;
        let command = raft_command::CommandEnvelopeV1::new(
            key.shard_id(),
            context.placement_epoch(),
            request_id,
            raft_command::CommandBodyV1::AdvanceAnalyticsArtifactFence(command),
        )
        .encode()
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let outcome = self
            .propose_until_applied(
                key,
                context.placement_epoch(),
                request_id,
                context.common().deadline_unix_ms(),
                command,
            )
            .await?;
        Ok(Response::new(AdvanceAnalyticsArtifactFenceResponse {
            raft_index: outcome.status().applied_index(),
            duplicate: outcome.duplicate(),
        }))
    }

    async fn list_analytics_artifact_generations(
        &self,
        request: Request<ListAnalyticsArtifactGenerationsRequest>,
    ) -> Result<Response<ListAnalyticsArtifactGenerationsResponse>, Status> {
        let request = request.into_inner();
        let (context, key, request_id) = self.validate(
            request.context,
            DataOperation::ListAnalyticsArtifactGenerations,
        )?;
        let job_id = artifact_job_id(&request.job_id)?;
        let kind = artifact_kind(request.kind)?;
        let limit = usize::try_from(request.limit)
            .map_err(|_| Status::invalid_argument("artifact generation limit is invalid"))?;
        if !(1..=4096).contains(&limit) {
            return Err(Status::invalid_argument(
                "artifact generation limit must be between 1 and 4096",
            ));
        }
        let deadline = monotonic_deadline(context.common().deadline_unix_ms())?;
        let read_index = self
            .host
            .leader_read_permit(key, context.placement_epoch(), request_id, deadline)
            .await
            .map_err(artifact_host_status)?;
        let span = KeySpan::prefix(
            Keyspace::Meta,
            shard_runtime::analytics_artifact_generation_head_prefix(job_id, kind),
        )
        .with_limit(limit)
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let (applied_index, observations) =
            stable_artifact_head_scan(&self.host, key, read_index, context.placement_epoch(), span)
                .await?;
        let generations = observations
            .into_iter()
            .map(|observation| {
                if observation.identity.job_id() != job_id || observation.identity.kind() != kind {
                    return Err(Status::data_loss(
                        "filtered analytics artifact scan crossed its requested prefix",
                    ));
                }
                Ok(AnalyticsArtifactGeneration {
                    job_id: request.job_id.clone(),
                    kind: request.kind,
                    generation: observation.identity.generation(),
                    created_at_unix_ms: observation.head.created_at_unix_ms(),
                    expected_chunk_count: observation
                        .pin
                        .map_or(u32::from(observation.head.count()), |value| {
                            u32::from(value.expected_chunk_count())
                        }),
                    expected_total_bytes: observation
                        .pin
                        .map_or(0, |value| value.expected_total_bytes()),
                    expected_content_digest: observation
                        .pin
                        .map_or_else(Vec::new, |value| value.expected_content_digest().to_vec()),
                    pinned: observation.pin.is_some(),
                    applied_index,
                })
            })
            .collect::<Result<Vec<_>, Status>>()?;
        Ok(Response::new(ListAnalyticsArtifactGenerationsResponse {
            applied_index,
            generations,
        }))
    }

    async fn list_analytics_artifact_generation_heads(
        &self,
        request: Request<ListAnalyticsArtifactGenerationHeadsRequest>,
    ) -> Result<Response<ListAnalyticsArtifactGenerationHeadsResponse>, Status> {
        let request = request.into_inner();
        let (context, key, request_id) = self.validate(
            request.context,
            DataOperation::ListAnalyticsArtifactGenerationHeads,
        )?;
        let after = request
            .after
            .map(|cursor| {
                let job_id = artifact_job_id(&cursor.job_id)?;
                let kind = artifact_kind(cursor.kind)?;
                if cursor.generation == 0 {
                    return Err(Status::invalid_argument(
                        "analytics artifact generation cursor must be non-zero",
                    ));
                }
                Ok((job_id, kind, cursor.generation))
            })
            .transpose()?;
        let limit = usize::try_from(request.limit)
            .map_err(|_| Status::invalid_argument("artifact generation limit is invalid"))?;
        if !(1..=4096).contains(&limit) {
            return Err(Status::invalid_argument(
                "artifact generation limit must be between 1 and 4096",
            ));
        }
        let deadline = monotonic_deadline(context.common().deadline_unix_ms())?;
        let read_index = self
            .host
            .leader_read_permit(key, context.placement_epoch(), request_id, deadline)
            .await
            .map_err(artifact_host_status)?;
        let prefix = shard_runtime::analytics_artifact_generation_heads_prefix().to_vec();
        let span = if let Some((job_id, kind, generation)) = after {
            let mut start =
                shard_runtime::analytics_artifact_generation_head_key(job_id, kind, generation)
                    .as_bytes()
                    .to_vec();
            start.push(0);
            KeySpan::prefix_from(Keyspace::Meta, prefix, start)
        } else {
            Ok(KeySpan::prefix(Keyspace::Meta, prefix))
        }
        .and_then(|span| span.with_limit(limit))
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let (applied_index, observations) =
            stable_artifact_head_scan(&self.host, key, read_index, context.placement_epoch(), span)
                .await?;
        let full_page = observations.len() == limit;
        let generations = observations
            .into_iter()
            .map(|observation| {
                Ok(AnalyticsArtifactGeneration {
                    job_id: observation.identity.job_id().to_be_bytes().to_vec(),
                    kind: wire_artifact_kind(observation.identity.kind()).into(),
                    generation: observation.identity.generation(),
                    created_at_unix_ms: observation.head.created_at_unix_ms(),
                    expected_chunk_count: observation
                        .pin
                        .map_or(u32::from(observation.head.count()), |value| {
                            u32::from(value.expected_chunk_count())
                        }),
                    expected_total_bytes: observation
                        .pin
                        .map_or(0, |value| value.expected_total_bytes()),
                    expected_content_digest: observation
                        .pin
                        .map_or_else(Vec::new, |value| value.expected_content_digest().to_vec()),
                    pinned: observation.pin.is_some(),
                    applied_index,
                })
            })
            .collect::<Result<Vec<_>, Status>>()?;
        let next = if full_page {
            generations.last().map(|generation| {
                cluster_protocol::proto::AnalyticsArtifactGenerationCursor {
                    job_id: generation.job_id.clone(),
                    kind: generation.kind,
                    generation: generation.generation,
                }
            })
        } else {
            None
        };
        Ok(Response::new(
            ListAnalyticsArtifactGenerationHeadsResponse {
                applied_index,
                generations,
                next,
            },
        ))
    }

    type ScanStream = ReceiverStream<Result<ScanBatch, Status>>;

    async fn scan(
        &self,
        request: Request<ScanRequest>,
    ) -> Result<Response<Self::ScanStream>, Status> {
        let request = request.into_inner();
        let (context, key, _) = self.validate(request.context, DataOperation::Scan)?;
        if !request.read_proof.is_empty() {
            return Err(Status::invalid_argument(
                "follower scan proofs are not enabled on the leader-only P0 path",
            ));
        }
        let maximum_batch_bytes = usize::try_from(request.maximum_batch_bytes)
            .map_err(|_| Status::invalid_argument("scan batch bound is out of range"))?;
        if !(MIN_SCAN_BATCH_BYTES..=MAX_SCAN_BATCH_BYTES).contains(&maximum_batch_bytes) {
            return Err(Status::invalid_argument("invalid scan batch byte bound"));
        }
        let status = self.require_leader(key).await?;
        if status.placement_epoch() != context.placement_epoch() {
            return Err(stale_epoch_status(status.placement_epoch()));
        }
        let span = decode_key_scan_plan(&request.plan)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        if span.keyspace() == Keyspace::Meta {
            return Err(Status::permission_denied(
                "generic scans cannot access reserved metadata keys",
            ));
        }
        let rows = self.host.scan(key, span).await.map_err(host_status)?;
        let batches = partition_scan_rows(rows, maximum_batch_bytes)
            .map_err(|error| Status::resource_exhausted(error.to_string()))?;
        let applied_index = status.applied_index();
        let (sender, receiver) = mpsc::channel(8);
        tokio::spawn(async move {
            let terminal_sequence = batches.len().saturating_sub(1);
            for (sequence, batch) in batches.into_iter().enumerate() {
                let encoded = match encode_key_scan_batch(&batch) {
                    Ok(encoded) => encoded,
                    Err(error) => {
                        let _ = sender
                            .send(Err(Status::resource_exhausted(error.to_string())))
                            .await;
                        return;
                    }
                };
                let Ok(sequence) = u64::try_from(sequence) else {
                    let _ = sender
                        .send(Err(Status::internal("scan sequence overflow")))
                        .await;
                    return;
                };
                if sender
                    .send(Ok(ScanBatch {
                        sequence,
                        applied_index,
                        arrow_record_batch: encoded,
                        terminal: usize::try_from(sequence).ok() == Some(terminal_sequence),
                    }))
                    .await
                    .is_err()
                {
                    return;
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(receiver)))
    }

    type ExportSnapshotStream = ReceiverStream<Result<SnapshotChunk, Status>>;

    async fn export_snapshot(
        &self,
        request: Request<ExportSnapshotRequest>,
    ) -> Result<Response<Self::ExportSnapshotStream>, Status> {
        let _gate = self.migration_gate.lock().await;
        let request = request.into_inner();
        let wire_context = request
            .context
            .clone()
            .ok_or_else(|| Status::invalid_argument("missing Shard context"))?;
        let (context, key, _) = self.validate(request.context, DataOperation::ExportSnapshot)?;
        validate_identifier(&request.migration_id, "migration ID")?;
        self.require_leader(key).await?;
        let migration_id: [u8; 16] = request
            .migration_id
            .as_slice()
            .try_into()
            .expect("validated migration ID length");
        let root = self
            .host
            .data_directory()
            .join("migration")
            .join("exports")
            .join(hex_identifier(migration_id));
        let bundle = root.join("bundle");
        let archive = root.join("snapshot.archive");
        std::fs::create_dir_all(&root).map_err(|error| Status::internal(error.to_string()))?;
        let manifest = if bundle.exists() {
            replica_snapshot::open_snapshot_bundle(&bundle)
                .map_err(|error| Status::failed_precondition(error.to_string()))?
        } else {
            self.host
                .create_snapshot(key, context.placement_epoch(), bundle.clone())
                .await
                .map_err(host_status)?
        };
        let content_digest = if archive.exists() {
            hash_file(&archive).map_err(|error| Status::internal(error.to_string()))?
        } else {
            let mut output =
                File::create(&archive).map_err(|error| Status::internal(error.to_string()))?;
            let digest = replica_snapshot::write_snapshot_archive(&bundle, &mut output)
                .map_err(|error| Status::internal(error.to_string()))?;
            output
                .sync_all()
                .map_err(|error| Status::internal(error.to_string()))?;
            digest
        };
        let outcome = encode_snapshot_outcome(manifest.applied_index, manifest.checkpoint_digest);
        self.host
            .record_migration_receipt(migration_id, SNAPSHOT_EXPORT_STEP, content_digest, outcome)
            .map_err(host_status)?;
        let file_length = std::fs::metadata(&archive)
            .map_err(|error| Status::internal(error.to_string()))?
            .len();
        if file_length == 0 {
            return Err(Status::internal("snapshot archive is empty"));
        }
        let (sender, receiver) = mpsc::channel(4);
        tokio::spawn(async move {
            let mut file = match tokio::fs::File::open(archive).await {
                Ok(file) => file,
                Err(error) => {
                    let _ = sender.send(Err(Status::internal(error.to_string()))).await;
                    return;
                }
            };
            let mut ordinal = 0_u64;
            let mut consumed = 0_u64;
            loop {
                let mut payload = vec![0_u8; SNAPSHOT_STREAM_CHUNK_BYTES];
                let read = match file.read(&mut payload).await {
                    Ok(read) => read,
                    Err(error) => {
                        let _ = sender.send(Err(Status::internal(error.to_string()))).await;
                        return;
                    }
                };
                if read == 0 {
                    return;
                }
                payload.truncate(read);
                consumed = consumed.saturating_add(read as u64);
                let terminal = consumed == file_length;
                let chunk = SnapshotChunk {
                    context: Some(wire_context.clone()),
                    migration_id: migration_id.to_vec(),
                    ordinal,
                    checksum: crc32fast::hash(&payload),
                    payload,
                    terminal,
                    manifest_digest: if terminal {
                        content_digest.to_vec()
                    } else {
                        Vec::new()
                    },
                };
                if sender.send(Ok(chunk)).await.is_err() || terminal {
                    return;
                }
                ordinal = ordinal.saturating_add(1);
            }
        });
        Ok(Response::new(ReceiverStream::new(receiver)))
    }

    async fn install_snapshot(
        &self,
        request: Request<tonic::Streaming<SnapshotChunk>>,
    ) -> Result<Response<InstallSnapshotResponse>, Status> {
        let _gate = self.migration_gate.lock().await;
        let mut stream = request.into_inner();
        let mut identity = None;
        let mut completion = None;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            if completion.is_some() {
                return Err(Status::invalid_argument(
                    "snapshot stream contains chunks after terminal chunk",
                ));
            }
            let (context, key, _) = self.validate(chunk.context, DataOperation::InstallSnapshot)?;
            validate_identifier(&chunk.migration_id, "migration ID")?;
            let migration_id: [u8; 16] = chunk
                .migration_id
                .as_slice()
                .try_into()
                .expect("validated migration ID length");
            let current = (key, context.placement_epoch(), migration_id);
            if identity.is_some_and(|expected| expected != current) {
                return Err(Status::invalid_argument(
                    "snapshot stream changed Shard, epoch, or migration identity",
                ));
            }
            identity = Some(current);
            let content_digest = if chunk.terminal {
                Some(chunk.manifest_digest.as_slice().try_into().map_err(|_| {
                    Status::invalid_argument("terminal snapshot digest must contain 32 bytes")
                })?)
            } else {
                if !chunk.manifest_digest.is_empty() {
                    return Err(Status::invalid_argument(
                        "non-terminal snapshot chunk carries a digest",
                    ));
                }
                None
            };
            let chunk = MigrationChunk::new_with_checksum(
                migration_id,
                chunk.ordinal,
                chunk.payload,
                chunk.checksum,
                chunk.terminal,
                content_digest,
            )
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
            if let outcome @ ChunkAppendOutcome::Completed { .. } = self
                .host
                .append_migration_chunk(chunk)
                .map_err(host_status)?
            {
                completion = Some(outcome);
            }
        }
        let (key, placement_epoch, migration_id) =
            identity.ok_or_else(|| Status::invalid_argument("snapshot stream is empty"))?;
        let ChunkAppendOutcome::Completed {
            archive_path,
            content_digest,
            duplicate: chunk_duplicate,
        } = completion.ok_or_else(|| {
            Status::invalid_argument("snapshot stream ended before a terminal chunk")
        })?
        else {
            unreachable!("completion only stores terminal outcomes")
        };
        if let Some(receipt) = self
            .host
            .migration_receipt(migration_id, SNAPSHOT_INSTALL_STEP)
            .map_err(host_status)?
        {
            if receipt.input_digest() != &content_digest {
                return Err(Status::already_exists(
                    "snapshot install receipt has another input digest",
                ));
            }
            let installed_index = decode_snapshot_outcome(receipt.outcome())?;
            return Ok(Response::new(InstallSnapshotResponse {
                migration_id: migration_id.to_vec(),
                installed_index,
                content_digest: receipt.input_digest().to_vec(),
                duplicate: true,
            }));
        }
        let migration_root = self
            .host
            .data_directory()
            .join("migration")
            .join("snapshots")
            .join(hex_identifier(migration_id));
        let bundle_path = migration_root.join("bundle");
        let installed_path = self
            .host
            .learner_replica_directory(key, placement_epoch)
            .map_err(host_status)?;
        std::fs::create_dir_all(&migration_root)
            .map_err(|error| Status::internal(error.to_string()))?;
        let manifest = if bundle_path.exists() {
            replica_snapshot::open_snapshot_bundle(&bundle_path)
        } else {
            replica_snapshot::extract_snapshot_archive(
                File::open(&archive_path).map_err(|error| Status::internal(error.to_string()))?,
                &bundle_path,
            )
        }
        .map_err(|error| Status::failed_precondition(error.to_string()))?;
        if manifest.shard_id != key.shard_id() || manifest.placement_epoch != placement_epoch {
            return Err(Status::failed_precondition(
                "snapshot manifest differs from requested Shard or placement epoch",
            ));
        }
        let installed = if installed_path.exists() {
            replica_snapshot::open_installed_snapshot(&installed_path).await
        } else {
            replica_snapshot::install_snapshot_bundle(&bundle_path, &installed_path).await
        }
        .map_err(|error| Status::failed_precondition(error.to_string()))?;
        let outcome = encode_snapshot_outcome(
            installed.manifest.applied_index,
            installed.manifest.checkpoint_digest,
        );
        self.host
            .mark_learner_snapshot(key, placement_epoch, installed.manifest.applied_index)
            .await
            .map_err(host_status)?;
        let receipt_outcome = self
            .host
            .record_migration_receipt(migration_id, SNAPSHOT_INSTALL_STEP, content_digest, outcome)
            .map_err(host_status)?;
        Ok(Response::new(InstallSnapshotResponse {
            migration_id: migration_id.to_vec(),
            installed_index: installed.manifest.applied_index,
            content_digest: content_digest.to_vec(),
            duplicate: chunk_duplicate || receipt_outcome == crate::ReceiptWriteOutcome::Duplicate,
        }))
    }

    async fn replica_status(
        &self,
        request: Request<ReplicaStatusRequest>,
    ) -> Result<Response<ReplicaStatusResponse>, Status> {
        let (context, key, _) =
            self.validate(request.into_inner().context, DataOperation::ReplicaStatus)?;
        let status = self.host.status(key).await.map_err(host_status)?;
        if status.placement_epoch() != context.placement_epoch() {
            return Err(stale_epoch_status(status.placement_epoch()));
        }
        Ok(Response::new(status_response(status)))
    }
}

#[tonic::async_trait]
impl NodeAdminService for DataNodeGrpcService {
    async fn ensure_replica(
        &self,
        request: Request<EnsureReplicaRequest>,
    ) -> Result<Response<EnsureReplicaResponse>, Status> {
        let request = request.into_inner();
        let (context, key, _) = self.validate(request.context, DataOperation::EnsureReplica)?;
        validate_identifier(&request.operation_id, "operation ID")?;
        if request.local_node_id != self.host.identity().node_id() {
            return Err(Status::failed_precondition(
                "EnsureReplica targets another Data node",
            ));
        }
        let initial_role = WireReplicaRole::try_from(request.initial_role)
            .map_err(|_| Status::invalid_argument("unknown initial Replica role"))?;
        let role = match initial_role {
            WireReplicaRole::Learner => ReplicaRole::Learner,
            WireReplicaRole::Follower | WireReplicaRole::Leader => ReplicaRole::Voter,
            WireReplicaRole::Unspecified => {
                return Err(Status::invalid_argument(
                    "initial Replica role is unspecified",
                ));
            }
        };
        let profile = decode_replica_profile(&request.backend_profile, request.backend_generation)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let spec = if let Some(backend_slot) = profile.backend_slot {
            ReplicaSpec::new_with_backend(
                key.graph_id(),
                key.shard_id(),
                context.placement_epoch(),
                profile.voters,
                role,
                request.schema_version,
                backend_slot,
                profile.relative_directory,
            )
        } else {
            ReplicaSpec::new(
                key.graph_id(),
                key.shard_id(),
                context.placement_epoch(),
                profile.voters,
                role,
                request.schema_version,
                request.backend_generation,
                profile.relative_directory,
            )
        }
        .map_err(host_status)?;
        let outcome = self.host.ensure_replica(spec).await.map_err(host_status)?;
        let status = if initial_role == WireReplicaRole::Leader {
            self.host.campaign(key).await.map_err(host_status)?
        } else {
            self.host.status(key).await.map_err(host_status)?
        };
        Ok(Response::new(EnsureReplicaResponse {
            created: outcome == EnsureReplicaOutcome::Created,
            status: Some(status_response(status)),
        }))
    }

    async fn change_membership(
        &self,
        request: Request<ChangeMembershipRequest>,
    ) -> Result<Response<ChangeMembershipResponse>, Status> {
        let request = request.into_inner();
        let (context, key, request_id) =
            self.validate(request.context, DataOperation::ChangeMembership)?;
        validate_identifier(&request.operation_id, "operation ID")?;
        if request.operation_id.as_slice() != request_id.to_be_bytes() {
            return Err(Status::invalid_argument(
                "operation ID differs from request ID",
            ));
        }
        validate_membership(&request.old_voters, &request.new_voters, &request.learners)?;
        self.require_leader(key).await?;
        let (status, duplicate) = self
            .host
            .change_membership(
                key,
                context.placement_epoch(),
                request_id,
                request.old_voters,
                request.new_voters,
                request.learners,
            )
            .await
            .map_err(host_status)?;
        Ok(Response::new(ChangeMembershipResponse {
            applied_index: status.applied_index(),
            duplicate,
        }))
    }

    async fn delete_replica(
        &self,
        request: Request<DeleteReplicaRequest>,
    ) -> Result<Response<DeleteReplicaResponse>, Status> {
        let request = request.into_inner();
        let (context, key, request_id) =
            self.validate(request.context, DataOperation::DeleteReplica)?;
        validate_identifier(&request.operation_id, "operation ID")?;
        if request.operation_id.as_slice() != request_id.to_be_bytes() {
            return Err(Status::invalid_argument(
                "operation ID differs from request ID",
            ));
        }
        let deleted = self
            .host
            .delete_replica(
                key,
                context.placement_epoch(),
                request_id,
                request.minimum_safe_index,
            )
            .await
            .map_err(host_status)?;
        Ok(Response::new(DeleteReplicaResponse { deleted }))
    }

    async fn activate_replica(
        &self,
        request: Request<ActivateReplicaRequest>,
    ) -> Result<Response<ActivateReplicaResponse>, Status> {
        let request = request.into_inner();
        let (context, key, request_id) =
            self.validate(request.context, DataOperation::ActivateReplica)?;
        validate_identifier(&request.operation_id, "operation ID")?;
        if request.operation_id.as_slice() != request_id.to_be_bytes() {
            return Err(Status::invalid_argument(
                "operation ID differs from request ID",
            ));
        }
        if request.target_placement_epoch != context.placement_epoch().checked_add(1).unwrap_or(0) {
            return Err(Status::invalid_argument(
                "target placement epoch must immediately follow source epoch",
            ));
        }
        validate_membership(&request.voters, &request.voters, &[])?;
        let (status, duplicate) = self
            .host
            .activate_replica(
                key,
                context.placement_epoch(),
                request.target_placement_epoch,
                request.voters,
            )
            .await
            .map_err(host_status)?;
        Ok(Response::new(ActivateReplicaResponse {
            status: Some(status_response(status)),
            duplicate,
        }))
    }

    async fn get_migration_receipt(
        &self,
        request: Request<GetMigrationReceiptRequest>,
    ) -> Result<Response<GetMigrationReceiptResponse>, Status> {
        let request = request.into_inner();
        self.validate_common(request.context, DataOperation::MigrationReceipt)?;
        validate_identifier(&request.migration_id, "migration ID")?;
        if request.step == 0 {
            return Err(Status::invalid_argument("migration step must be non-zero"));
        }
        let migration_id = request
            .migration_id
            .as_slice()
            .try_into()
            .expect("validated migration ID length");
        let receipt = self
            .host
            .migration_receipt(migration_id, request.step)
            .map_err(host_status)?;
        Ok(Response::new(match receipt {
            Some(receipt) => GetMigrationReceiptResponse {
                present: true,
                input_digest: receipt.input_digest().to_vec(),
                outcome: receipt.outcome().to_vec(),
            },
            None => GetMigrationReceiptResponse {
                present: false,
                input_digest: Vec::new(),
                outcome: Vec::new(),
            },
        }))
    }

    async fn prepare_backend_target(
        &self,
        request: Request<PrepareBackendTargetRequest>,
    ) -> Result<Response<PrepareBackendTargetResponse>, Status> {
        let request = request.into_inner();
        let (context, key, _) = self.validate(request.context, DataOperation::BackendManagement)?;
        validate_identifier(&request.operation_id, "operation ID")?;
        let target = decode_backend_profile_spec(
            request
                .target_profile
                .ok_or_else(|| Status::invalid_argument("missing target backend profile"))?,
        )?;
        let status = self
            .host
            .prepare_backend_target(
                key,
                context.placement_epoch(),
                request.target_generation,
                target.clone(),
            )
            .await
            .map_err(host_status)?;
        let runtime = self
            .host
            .backend_runtime_status(key)
            .await
            .map_err(host_status)?;
        let fence_index = backend_fence(&runtime)?;
        Ok(Response::new(PrepareBackendTargetResponse {
            status: Some(status_response(status)),
            fence_index,
            target_profile_digest: target.digest().to_vec(),
        }))
    }

    async fn begin_backend_dual_apply(
        &self,
        request: Request<BeginBackendDualApplyRequest>,
    ) -> Result<Response<BackendTransitionResponse>, Status> {
        let request = request.into_inner();
        let (context, key, request_id) =
            self.validate(request.context, DataOperation::BackendManagement)?;
        validate_operation_id(&request.operation_id, request_id)?;
        self.require_leader(key).await?;
        let digest = profile_digest(&request.target_profile_digest)?;
        let command = raft_command::CommandEnvelopeV1::new(
            key.shard_id(),
            context.placement_epoch(),
            request_id,
            raft_command::CommandBodyV1::BeginBackendDualApply(
                raft_command::BeginBackendDualApplyV1 {
                    source_generation: request.source_generation,
                    target_generation: request.target_generation,
                    target_profile_digest: digest,
                    fence_index: request.fence_index,
                },
            ),
        )
        .encode()
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
        backend_transition_response(
            self.host
                .propose_backend_transition(key, context.placement_epoch(), request_id, command)
                .await
                .map_err(host_status)?,
        )
    }

    async fn cutover_backend(
        &self,
        request: Request<FinishBackendMigrationRequest>,
    ) -> Result<Response<BackendTransitionResponse>, Status> {
        self.finish_backend_migration(request.into_inner(), true)
            .await
    }

    async fn abort_backend_migration(
        &self,
        request: Request<FinishBackendMigrationRequest>,
    ) -> Result<Response<BackendTransitionResponse>, Status> {
        self.finish_backend_migration(request.into_inner(), false)
            .await
    }

    async fn get_backend_status(
        &self,
        request: Request<GetBackendStatusRequest>,
    ) -> Result<Response<GetBackendStatusResponse>, Status> {
        let (context, key, _) = self.validate(
            request.into_inner().context,
            DataOperation::BackendManagement,
        )?;
        let runtime = self
            .host
            .backend_runtime_status(key)
            .await
            .map_err(host_status)?;
        if runtime.replica().placement_epoch() != context.placement_epoch() {
            return Err(stale_epoch_status(runtime.replica().placement_epoch()));
        }
        Ok(Response::new(backend_status_response(runtime)))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DecodedReplicaProfile {
    voters: Vec<u64>,
    relative_directory: String,
    backend_slot: Option<BackendSlotState>,
}

pub fn encode_backend_replica_profile(
    voters: &[u64],
    relative_directory: &str,
    backend: &BackendProfile,
) -> Result<Vec<u8>, ReplicaProfileError> {
    let mut voters = voters.to_vec();
    voters.sort_unstable();
    voters.dedup();
    validate_replica_profile(&voters, relative_directory)?;
    let profile = ReplicaBootstrapProfile {
        format_version: 1,
        voters,
        relative_directory: relative_directory.to_owned(),
        backend: Some(BackendProfileSpec {
            provider: backend.provider().to_owned(),
            instance_id: backend.instance_id().to_owned(),
            public_parameters: backend
                .public_parameters()
                .iter()
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect(),
            credential_refs: backend
                .credential_refs()
                .iter()
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect(),
        }),
    };
    let encoded = profile.encode_to_vec();
    if encoded.len() > MAX_REPLICA_PROFILE_BYTES {
        return Err(ReplicaProfileError::TooLarge);
    }
    Ok(encoded)
}

pub fn encode_rocks_replica_profile(
    voters: &[u64],
    relative_directory: &str,
) -> Result<Vec<u8>, ReplicaProfileError> {
    let mut voters = voters.to_vec();
    voters.sort_unstable();
    voters.dedup();
    validate_replica_profile(&voters, relative_directory)?;
    let mut encoded = Vec::new();
    encoded.extend_from_slice(&REPLICA_PROFILE_MAGIC);
    encoded.extend_from_slice(&READ_CODEC_VERSION.to_be_bytes());
    encoded.extend_from_slice(&(voters.len() as u16).to_be_bytes());
    encoded.extend_from_slice(&(relative_directory.len() as u16).to_be_bytes());
    for voter in voters {
        encoded.extend_from_slice(&voter.to_be_bytes());
    }
    encoded.extend_from_slice(relative_directory.as_bytes());
    append_checksum(&mut encoded);
    Ok(encoded)
}

fn decode_replica_profile(
    encoded: &[u8],
    backend_generation: u64,
) -> Result<DecodedReplicaProfile, ReplicaProfileError> {
    if !encoded.starts_with(&REPLICA_PROFILE_MAGIC) {
        if encoded.is_empty() || encoded.len() > MAX_REPLICA_PROFILE_BYTES {
            return Err(ReplicaProfileError::TooLarge);
        }
        let decoded = ReplicaBootstrapProfile::decode(encoded)
            .map_err(|_| ReplicaProfileError::InvalidProtobuf)?;
        if decoded.format_version != 1 {
            return Err(ReplicaProfileError::UnsupportedVersion {
                actual: u16::try_from(decoded.format_version).unwrap_or(u16::MAX),
            });
        }
        validate_replica_profile(&decoded.voters, &decoded.relative_directory)?;
        let backend = decoded.backend.ok_or(ReplicaProfileError::MissingBackend)?;
        let profile = BackendProfile::new(
            backend.provider,
            backend.instance_id,
            backend.public_parameters.into_iter().collect(),
            backend.credential_refs.into_iter().collect(),
        )
        .map_err(|error| ReplicaProfileError::InvalidBackend(error.to_string()))?;
        let backend_slot = BackendSlotState::active(backend_generation, profile)
            .map_err(|error| ReplicaProfileError::InvalidBackend(error.to_string()))?;
        return Ok(DecodedReplicaProfile {
            voters: decoded.voters,
            relative_directory: decoded.relative_directory,
            backend_slot: Some(backend_slot),
        });
    }
    if encoded.len() < 14 {
        return Err(ReplicaProfileError::Truncated);
    }
    if encoded[..4] != REPLICA_PROFILE_MAGIC {
        return Err(ReplicaProfileError::InvalidMagic);
    }
    let version = u16::from_be_bytes(encoded[4..6].try_into().expect("fixed profile version"));
    if version != READ_CODEC_VERSION {
        return Err(ReplicaProfileError::UnsupportedVersion { actual: version });
    }
    let checksum_offset = encoded.len() - 4;
    let stored = u32::from_be_bytes(
        encoded[checksum_offset..]
            .try_into()
            .expect("fixed profile checksum"),
    );
    if crc32fast::hash(&encoded[..checksum_offset]) != stored {
        return Err(ReplicaProfileError::ChecksumMismatch);
    }
    let voter_count = usize::from(u16::from_be_bytes(
        encoded[6..8].try_into().expect("fixed voter count"),
    ));
    let directory_length = usize::from(u16::from_be_bytes(
        encoded[8..10].try_into().expect("fixed directory length"),
    ));
    let voter_bytes = voter_count
        .checked_mul(8)
        .ok_or(ReplicaProfileError::Truncated)?;
    if 10 + voter_bytes + directory_length != checksum_offset {
        return Err(ReplicaProfileError::Truncated);
    }
    let mut voters = Vec::with_capacity(voter_count);
    let mut offset = 10;
    for _ in 0..voter_count {
        voters.push(u64::from_be_bytes(
            encoded[offset..offset + 8]
                .try_into()
                .expect("bounded voter ID"),
        ));
        offset += 8;
    }
    let relative_directory = std::str::from_utf8(&encoded[offset..checksum_offset])
        .map_err(|_| ReplicaProfileError::InvalidDirectory)?
        .to_owned();
    validate_replica_profile(&voters, &relative_directory)?;
    Ok(DecodedReplicaProfile {
        voters,
        relative_directory,
        backend_slot: None,
    })
}

fn validate_replica_profile(
    voters: &[u64],
    relative_directory: &str,
) -> Result<(), ReplicaProfileError> {
    if voters.is_empty()
        || voters.len() > MAX_PROFILE_VOTERS
        || voters.contains(&0)
        || voters.windows(2).any(|pair| pair[0] >= pair[1])
    {
        return Err(ReplicaProfileError::InvalidVoters);
    }
    if relative_directory.is_empty()
        || relative_directory.len() > MAX_REPLICA_DIRECTORY_BYTES
        || relative_directory.starts_with('/')
        || relative_directory
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..")
    {
        return Err(ReplicaProfileError::InvalidDirectory);
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReplicaProfileError {
    InvalidVoters,
    InvalidDirectory,
    InvalidMagic,
    UnsupportedVersion { actual: u16 },
    ChecksumMismatch,
    Truncated,
    TooLarge,
    InvalidProtobuf,
    MissingBackend,
    InvalidBackend(String),
}

impl Display for ReplicaProfileError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidVoters => formatter.write_str("invalid RocksDB Replica voter set"),
            Self::InvalidDirectory => formatter.write_str("invalid RocksDB Replica directory"),
            Self::InvalidMagic => formatter.write_str("invalid RocksDB Replica profile magic"),
            Self::UnsupportedVersion { actual } => {
                write!(
                    formatter,
                    "unsupported RocksDB Replica profile version {actual}"
                )
            }
            Self::ChecksumMismatch => {
                formatter.write_str("RocksDB Replica profile checksum mismatch")
            }
            Self::Truncated => formatter.write_str("RocksDB Replica profile is truncated"),
            Self::TooLarge => formatter.write_str("Replica profile exceeds its size limit"),
            Self::InvalidProtobuf => formatter.write_str("Replica profile protobuf is invalid"),
            Self::MissingBackend => formatter.write_str("Replica profile backend is missing"),
            Self::InvalidBackend(message) => {
                write!(formatter, "invalid backend profile: {message}")
            }
        }
    }
}

impl Error for ReplicaProfileError {}

fn decode_backend_profile_spec(spec: BackendProfileSpec) -> Result<BackendProfile, Status> {
    BackendProfile::new(
        spec.provider,
        spec.instance_id,
        spec.public_parameters.into_iter().collect(),
        spec.credential_refs.into_iter().collect(),
    )
    .map_err(|error| Status::invalid_argument(error.to_string()))
}

fn validate_operation_id(encoded: &[u8], request_id: u128) -> Result<(), Status> {
    validate_identifier(encoded, "operation ID")?;
    if encoded != request_id.to_be_bytes() {
        return Err(Status::invalid_argument(
            "operation ID differs from request ID",
        ));
    }
    Ok(())
}

fn profile_digest(encoded: &[u8]) -> Result<[u8; 32], Status> {
    encoded
        .try_into()
        .map_err(|_| Status::invalid_argument("target profile digest must contain 32 bytes"))
}

fn backend_fence(runtime: &BackendRuntimeStatus) -> Result<u64, Status> {
    match runtime.slot() {
        BackendSlotState::DualApplying { fence_index, .. } => Ok(*fence_index),
        BackendSlotState::Active { .. } => Err(Status::failed_precondition(
            "backend target has not entered local dual-apply preparation",
        )),
    }
}

fn backend_transition_response(
    outcome: crate::ProposalOutcome,
) -> Result<Response<BackendTransitionResponse>, Status> {
    Ok(Response::new(BackendTransitionResponse {
        status: Some(status_response(outcome.status())),
        duplicate: outcome.duplicate(),
    }))
}

fn backend_status_response(runtime: BackendRuntimeStatus) -> GetBackendStatusResponse {
    let (phase, source_generation, target_generation, digest, fence_index) = match runtime.slot() {
        BackendSlotState::Active { generation, .. } => {
            (BackendLifecyclePhase::Active, *generation, 0, Vec::new(), 0)
        }
        BackendSlotState::DualApplying {
            source_generation,
            target_generation,
            target,
            fence_index,
            ..
        } => (
            BackendLifecyclePhase::DualApplying,
            *source_generation,
            *target_generation,
            target.digest().to_vec(),
            *fence_index,
        ),
    };
    let synchronized_index = match runtime.local() {
        MigrationStatus::Idle { .. } => runtime.replica().applied_index(),
        MigrationStatus::DualApplying {
            synchronized_index, ..
        } => synchronized_index,
    };
    GetBackendStatusResponse {
        status: Some(status_response(runtime.replica())),
        phase: phase.into(),
        source_generation,
        target_generation,
        target_profile_digest: digest,
        fence_index,
        synchronized_index,
    }
}

pub fn encode_key_read_plan(keys: &[LogicalKey]) -> Result<Vec<u8>, ReadCodecError> {
    if keys.is_empty() || keys.len() > MAX_READ_KEYS {
        return Err(ReadCodecError::InvalidKeyCount { actual: keys.len() });
    }
    let mut encoded = Vec::new();
    encoded.extend_from_slice(&READ_PLAN_MAGIC);
    encoded.extend_from_slice(&READ_CODEC_VERSION.to_be_bytes());
    encoded.extend_from_slice(&(keys.len() as u16).to_be_bytes());
    for key in keys {
        if key.as_bytes().is_empty() || key.as_bytes().len() > MAX_READ_KEY_BYTES {
            return Err(ReadCodecError::InvalidKeyLength {
                actual: key.as_bytes().len(),
            });
        }
        encoded.push(key.keyspace().tag());
        let length =
            u32::try_from(key.as_bytes().len()).map_err(|_| ReadCodecError::InvalidKeyLength {
                actual: key.as_bytes().len(),
            })?;
        encoded.extend_from_slice(&length.to_be_bytes());
        encoded.extend_from_slice(key.as_bytes());
    }
    append_checksum(&mut encoded);
    Ok(encoded)
}

fn decode_key_read_plan(encoded: &[u8]) -> Result<Vec<LogicalKey>, ReadCodecError> {
    validate_header(encoded, READ_PLAN_MAGIC)?;
    let count = usize::from(u16::from_be_bytes(
        encoded[6..8].try_into().expect("fixed read count"),
    ));
    if count == 0 || count > MAX_READ_KEYS {
        return Err(ReadCodecError::InvalidKeyCount { actual: count });
    }
    let checksum_offset = encoded.len() - 4;
    let mut offset = 8;
    let mut keys = Vec::with_capacity(count);
    for _ in 0..count {
        if offset + 5 > checksum_offset {
            return Err(ReadCodecError::Truncated);
        }
        let tag = encoded[offset];
        offset += 1;
        let length = u32::from_be_bytes(
            encoded[offset..offset + 4]
                .try_into()
                .expect("bounded key length"),
        ) as usize;
        offset += 4;
        if length == 0 || length > MAX_READ_KEY_BYTES || offset + length > checksum_offset {
            return Err(ReadCodecError::InvalidKeyLength { actual: length });
        }
        let keyspace = Keyspace::ALL
            .into_iter()
            .find(|keyspace| keyspace.tag() == tag)
            .ok_or(ReadCodecError::UnknownKeyspace { tag })?;
        keys.push(LogicalKey::in_keyspace(
            keyspace,
            encoded[offset..offset + length].to_vec(),
        ));
        offset += length;
    }
    if offset != checksum_offset {
        return Err(ReadCodecError::TrailingBytes);
    }
    Ok(keys)
}

fn encode_key_read_result(values: &[Option<Vec<u8>>]) -> Result<Vec<u8>, ReadCodecError> {
    if values.len() > MAX_READ_KEYS {
        return Err(ReadCodecError::InvalidKeyCount {
            actual: values.len(),
        });
    }
    let mut encoded = Vec::new();
    encoded.extend_from_slice(&READ_RESULT_MAGIC);
    encoded.extend_from_slice(&READ_CODEC_VERSION.to_be_bytes());
    encoded.extend_from_slice(&(values.len() as u16).to_be_bytes());
    for value in values {
        match value {
            Some(value) => {
                encoded.push(1);
                let length =
                    u32::try_from(value.len()).map_err(|_| ReadCodecError::ResultTooLarge)?;
                encoded.extend_from_slice(&length.to_be_bytes());
                encoded.extend_from_slice(value);
            }
            None => encoded.push(0),
        }
        if encoded.len() > MAX_READ_RESULT_BYTES {
            return Err(ReadCodecError::ResultTooLarge);
        }
    }
    append_checksum(&mut encoded);
    Ok(encoded)
}

pub fn decode_key_read_result(encoded: &[u8]) -> Result<Vec<Option<Vec<u8>>>, ReadCodecError> {
    validate_header(encoded, READ_RESULT_MAGIC)?;
    if encoded.len() > MAX_READ_RESULT_BYTES {
        return Err(ReadCodecError::ResultTooLarge);
    }
    let count = usize::from(u16::from_be_bytes(
        encoded[6..8].try_into().expect("fixed result count"),
    ));
    if count > MAX_READ_KEYS {
        return Err(ReadCodecError::InvalidKeyCount { actual: count });
    }
    let checksum_offset = encoded.len() - 4;
    let mut offset = 8;
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        if offset >= checksum_offset {
            return Err(ReadCodecError::Truncated);
        }
        let present = encoded[offset];
        offset += 1;
        if present == 0 {
            values.push(None);
            continue;
        }
        if present != 1 || offset + 4 > checksum_offset {
            return Err(ReadCodecError::Truncated);
        }
        let length = u32::from_be_bytes(
            encoded[offset..offset + 4]
                .try_into()
                .expect("bounded value length"),
        ) as usize;
        offset += 4;
        if offset + length > checksum_offset {
            return Err(ReadCodecError::Truncated);
        }
        values.push(Some(encoded[offset..offset + length].to_vec()));
        offset += length;
    }
    if offset != checksum_offset {
        return Err(ReadCodecError::TrailingBytes);
    }
    Ok(values)
}

pub fn encode_key_scan_plan(span: &KeySpan) -> Result<Vec<u8>, ReadCodecError> {
    let start_length =
        u32::try_from(span.start().len()).map_err(|_| ReadCodecError::InvalidKeyLength {
            actual: span.start().len(),
        })?;
    let end_length = optional_length(span.end())?;
    let prefix_length = optional_length(span.required_prefix())?;
    let limit = span
        .limit()
        .map(u64::try_from)
        .transpose()
        .map_err(|_| ReadCodecError::ResultTooLarge)?
        .unwrap_or(0);
    let max_bytes = span.max_bytes().unwrap_or(0);
    let mut encoded = Vec::new();
    encoded.extend_from_slice(&SCAN_PLAN_MAGIC);
    encoded.extend_from_slice(&READ_CODEC_VERSION.to_be_bytes());
    encoded.push(span.keyspace().tag());
    encoded.extend_from_slice(&start_length.to_be_bytes());
    encoded.extend_from_slice(&end_length.to_be_bytes());
    encoded.extend_from_slice(&prefix_length.to_be_bytes());
    encoded.extend_from_slice(&limit.to_be_bytes());
    encoded.extend_from_slice(&max_bytes.to_be_bytes());
    encoded.extend_from_slice(span.start());
    if let Some(end) = span.end() {
        encoded.extend_from_slice(end);
    }
    if let Some(prefix) = span.required_prefix() {
        encoded.extend_from_slice(prefix);
    }
    if encoded.len() + 4 > MAX_SCAN_PLAN_BYTES {
        return Err(ReadCodecError::ResultTooLarge);
    }
    append_checksum(&mut encoded);
    Ok(encoded)
}

fn decode_key_scan_plan(encoded: &[u8]) -> Result<KeySpan, ReadCodecError> {
    validate_header(encoded, SCAN_PLAN_MAGIC)?;
    if encoded.len() < 39 || encoded.len() > MAX_SCAN_PLAN_BYTES {
        return Err(ReadCodecError::Truncated);
    }
    let keyspace = decode_keyspace(encoded[6])?;
    let start_length = usize::try_from(u32::from_be_bytes(
        encoded[7..11].try_into().expect("fixed scan start length"),
    ))
    .map_err(|_| ReadCodecError::Truncated)?;
    let end_length = u32::from_be_bytes(encoded[11..15].try_into().expect("fixed scan end length"));
    let prefix_length = u32::from_be_bytes(
        encoded[15..19]
            .try_into()
            .expect("fixed scan prefix length"),
    );
    let limit = u64::from_be_bytes(encoded[19..27].try_into().expect("fixed scan limit"));
    let max_bytes = u64::from_be_bytes(encoded[27..35].try_into().expect("fixed scan byte limit"));
    let checksum_offset = encoded.len() - 4;
    let mut offset = 35;
    let start = take_scan_bytes(encoded, &mut offset, start_length, checksum_offset)?;
    let end = take_optional_scan_bytes(encoded, &mut offset, end_length, checksum_offset)?;
    let prefix = take_optional_scan_bytes(encoded, &mut offset, prefix_length, checksum_offset)?;
    if offset != checksum_offset {
        return Err(ReadCodecError::TrailingBytes);
    }
    let mut span = match prefix {
        Some(prefix) => {
            let span = KeySpan::prefix_from(keyspace, prefix, start)
                .map_err(|_| ReadCodecError::InvalidSpan)?;
            if span.end() != end.as_deref() {
                return Err(ReadCodecError::InvalidSpan);
            }
            span
        }
        None => KeySpan::range(keyspace, start, end).map_err(|_| ReadCodecError::InvalidSpan)?,
    };
    if limit != 0 {
        span = span
            .with_limit(usize::try_from(limit).map_err(|_| ReadCodecError::InvalidSpan)?)
            .map_err(|_| ReadCodecError::InvalidSpan)?;
    }
    if max_bytes != 0 {
        span = span
            .with_max_bytes(max_bytes)
            .map_err(|_| ReadCodecError::InvalidSpan)?;
    }
    Ok(span)
}

fn optional_length(bytes: Option<&[u8]>) -> Result<u32, ReadCodecError> {
    bytes.map_or(Ok(u32::MAX), |bytes| {
        u32::try_from(bytes.len()).map_err(|_| ReadCodecError::InvalidKeyLength {
            actual: bytes.len(),
        })
    })
}

fn take_optional_scan_bytes(
    encoded: &[u8],
    offset: &mut usize,
    length: u32,
    end: usize,
) -> Result<Option<Vec<u8>>, ReadCodecError> {
    if length == u32::MAX {
        return Ok(None);
    }
    let length = usize::try_from(length).map_err(|_| ReadCodecError::Truncated)?;
    take_scan_bytes(encoded, offset, length, end).map(Some)
}

fn take_scan_bytes(
    encoded: &[u8],
    offset: &mut usize,
    length: usize,
    end: usize,
) -> Result<Vec<u8>, ReadCodecError> {
    let next = offset
        .checked_add(length)
        .filter(|next| *next <= end)
        .ok_or(ReadCodecError::Truncated)?;
    let bytes = encoded[*offset..next].to_vec();
    *offset = next;
    Ok(bytes)
}

fn partition_scan_rows(
    rows: Vec<KeyValue>,
    maximum_batch_bytes: usize,
) -> Result<Vec<Vec<KeyValue>>, ReadCodecError> {
    if rows.len() > MAX_SCAN_ROWS {
        return Err(ReadCodecError::TooManyRows { actual: rows.len() });
    }
    let mut batches = Vec::new();
    let mut current = Vec::new();
    let mut current_bytes = 14_usize;
    for row in rows {
        let row_bytes = 9_usize
            .checked_add(row.key().as_bytes().len())
            .and_then(|size| size.checked_add(row.value().len()))
            .ok_or(ReadCodecError::ResultTooLarge)?;
        if 14 + row_bytes > maximum_batch_bytes {
            return Err(ReadCodecError::ResultTooLarge);
        }
        if !current.is_empty() && current_bytes + row_bytes > maximum_batch_bytes {
            batches.push(std::mem::take(&mut current));
            current_bytes = 14;
        }
        current_bytes += row_bytes;
        current.push(row);
    }
    if !current.is_empty() || batches.is_empty() {
        batches.push(current);
    }
    Ok(batches)
}

fn encode_key_scan_batch(rows: &[KeyValue]) -> Result<Vec<u8>, ReadCodecError> {
    if rows.len() > MAX_SCAN_ROWS {
        return Err(ReadCodecError::TooManyRows { actual: rows.len() });
    }
    let mut encoded = Vec::new();
    encoded.extend_from_slice(&SCAN_BATCH_MAGIC);
    encoded.extend_from_slice(&READ_CODEC_VERSION.to_be_bytes());
    encoded.extend_from_slice(
        &u32::try_from(rows.len())
            .map_err(|_| ReadCodecError::TooManyRows { actual: rows.len() })?
            .to_be_bytes(),
    );
    for row in rows {
        encoded.push(row.key().keyspace().tag());
        encoded.extend_from_slice(
            &u32::try_from(row.key().as_bytes().len())
                .map_err(|_| ReadCodecError::InvalidKeyLength {
                    actual: row.key().as_bytes().len(),
                })?
                .to_be_bytes(),
        );
        encoded.extend_from_slice(
            &u32::try_from(row.value().len())
                .map_err(|_| ReadCodecError::ResultTooLarge)?
                .to_be_bytes(),
        );
        encoded.extend_from_slice(row.key().as_bytes());
        encoded.extend_from_slice(row.value());
        if encoded.len() + 4 > MAX_SCAN_BATCH_BYTES {
            return Err(ReadCodecError::ResultTooLarge);
        }
    }
    append_checksum(&mut encoded);
    Ok(encoded)
}

pub fn decode_key_scan_batch(encoded: &[u8]) -> Result<Vec<KeyValue>, ReadCodecError> {
    decode_key_scan_batch_bounded(encoded, u64::MAX).map(|(rows, _)| rows)
}

pub fn decode_key_scan_batch_bounded(
    encoded: &[u8],
    max_bytes: u64,
) -> Result<(Vec<KeyValue>, u64), ReadCodecError> {
    validate_header(encoded, SCAN_BATCH_MAGIC)?;
    if encoded.len() > MAX_SCAN_BATCH_BYTES || encoded.len() < 14 {
        return Err(ReadCodecError::ResultTooLarge);
    }
    let count = usize::try_from(u32::from_be_bytes(
        encoded[6..10].try_into().expect("fixed scan row count"),
    ))
    .map_err(|_| ReadCodecError::Truncated)?;
    if count > MAX_SCAN_ROWS {
        return Err(ReadCodecError::TooManyRows { actual: count });
    }
    let checksum_offset = encoded.len() - 4;
    let mut offset = 10;
    let mut rows = Vec::with_capacity(count);
    let mut retained = 0_u64;
    for _ in 0..count {
        if offset + 9 > checksum_offset {
            return Err(ReadCodecError::Truncated);
        }
        let keyspace = decode_keyspace(encoded[offset])?;
        let key_length = u32::from_be_bytes(
            encoded[offset + 1..offset + 5]
                .try_into()
                .expect("bounded scan key length"),
        ) as usize;
        let value_length = u32::from_be_bytes(
            encoded[offset + 5..offset + 9]
                .try_into()
                .expect("bounded scan value length"),
        ) as usize;
        let entry_bytes = u64::try_from(key_length)
            .map_err(|_| ReadCodecError::ResultTooLarge)?
            .checked_add(u64::try_from(value_length).map_err(|_| ReadCodecError::ResultTooLarge)?)
            .ok_or(ReadCodecError::ResultTooLarge)?;
        let required = retained
            .checked_add(entry_bytes)
            .ok_or(ReadCodecError::ResultTooLarge)?;
        if required > max_bytes {
            return Err(ReadCodecError::ScanByteLimit {
                limit: max_bytes,
                required,
            });
        }
        retained = required;
        offset += 9;
        let key = take_scan_bytes(encoded, &mut offset, key_length, checksum_offset)?;
        let value = take_scan_bytes(encoded, &mut offset, value_length, checksum_offset)?;
        rows.push(KeyValue::new(LogicalKey::in_keyspace(keyspace, key), value));
    }
    if offset != checksum_offset {
        return Err(ReadCodecError::TrailingBytes);
    }
    Ok((rows, retained))
}

fn decode_keyspace(tag: u8) -> Result<Keyspace, ReadCodecError> {
    Keyspace::ALL
        .into_iter()
        .find(|keyspace| keyspace.tag() == tag)
        .ok_or(ReadCodecError::UnknownKeyspace { tag })
}

fn validate_header(encoded: &[u8], magic: [u8; 4]) -> Result<(), ReadCodecError> {
    if encoded.len() < 12 {
        return Err(ReadCodecError::Truncated);
    }
    if encoded[..4] != magic {
        return Err(ReadCodecError::InvalidMagic);
    }
    let version = u16::from_be_bytes(encoded[4..6].try_into().expect("fixed read version"));
    if version != READ_CODEC_VERSION {
        return Err(ReadCodecError::UnsupportedVersion { actual: version });
    }
    let checksum_offset = encoded.len() - 4;
    let stored = u32::from_be_bytes(
        encoded[checksum_offset..]
            .try_into()
            .expect("fixed read checksum"),
    );
    if crc32fast::hash(&encoded[..checksum_offset]) != stored {
        return Err(ReadCodecError::ChecksumMismatch);
    }
    Ok(())
}

fn append_checksum(encoded: &mut Vec<u8>) {
    encoded.extend_from_slice(&crc32fast::hash(encoded).to_be_bytes());
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReadCodecError {
    InvalidKeyCount { actual: usize },
    InvalidKeyLength { actual: usize },
    UnknownKeyspace { tag: u8 },
    InvalidMagic,
    UnsupportedVersion { actual: u16 },
    ChecksumMismatch,
    Truncated,
    TrailingBytes,
    ResultTooLarge,
    ScanByteLimit { limit: u64, required: u64 },
    InvalidSpan,
    TooManyRows { actual: usize },
}

impl Display for ReadCodecError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidKeyCount { actual } => {
                write!(formatter, "invalid read key count {actual}")
            }
            Self::InvalidKeyLength { actual } => {
                write!(formatter, "invalid read key length {actual}")
            }
            Self::UnknownKeyspace { tag } => write!(formatter, "unknown keyspace tag {tag}"),
            Self::InvalidMagic => formatter.write_str("invalid read codec magic"),
            Self::UnsupportedVersion { actual } => {
                write!(formatter, "unsupported read codec version {actual}")
            }
            Self::ChecksumMismatch => formatter.write_str("read codec checksum mismatch"),
            Self::Truncated => formatter.write_str("read codec payload is truncated"),
            Self::TrailingBytes => {
                formatter.write_str("read codec payload contains trailing bytes")
            }
            Self::ResultTooLarge => formatter.write_str("read result exceeds its size limit"),
            Self::ScanByteLimit { limit, required } => write!(
                formatter,
                "scan requires {required} bytes, exceeding byte limit {limit}"
            ),
            Self::InvalidSpan => formatter.write_str("invalid scan key span"),
            Self::TooManyRows { actual } => write!(formatter, "scan has too many rows: {actual}"),
        }
    }
}

impl Error for ReadCodecError {}

fn unix_time_ms() -> Result<u64, Status> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| Status::internal("system clock is before the Unix epoch"))?
        .as_millis();
    u64::try_from(millis).map_err(|_| Status::internal("system clock overflow"))
}

fn monotonic_deadline(deadline_unix_ms: u64) -> Result<tokio::time::Instant, Status> {
    let remaining = deadline_unix_ms
        .checked_sub(unix_time_ms()?)
        .ok_or_else(|| Status::deadline_exceeded("request deadline has expired"))?;
    Ok(tokio::time::Instant::now() + Duration::from_millis(remaining))
}

fn validate_identifier(identifier: &[u8], name: &str) -> Result<(), Status> {
    if identifier.len() != 16 || identifier.iter().all(|byte| *byte == 0) {
        return Err(Status::invalid_argument(format!(
            "{name} must be a non-zero 16-byte value"
        )));
    }
    Ok(())
}

fn validate_membership(old: &[u64], new: &[u64], learners: &[u64]) -> Result<(), Status> {
    let canonical =
        |nodes: &[u64]| !nodes.contains(&0) && nodes.windows(2).all(|pair| pair[0] < pair[1]);
    if old.is_empty()
        || new.is_empty()
        || !canonical(old)
        || !canonical(new)
        || !canonical(learners)
        || new.iter().any(|node| learners.binary_search(node).is_ok())
    {
        return Err(Status::invalid_argument(
            "Raft voters and learners must be canonical and disjoint",
        ));
    }
    Ok(())
}

fn encode_snapshot_outcome(installed_index: u64, checkpoint_digest: [u8; 32]) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(SNAPSHOT_OUTCOME_BYTES);
    encoded.extend_from_slice(&SNAPSHOT_OUTCOME_MAGIC);
    encoded.extend_from_slice(&SNAPSHOT_OUTCOME_VERSION.to_be_bytes());
    encoded.extend_from_slice(&installed_index.to_be_bytes());
    encoded.extend_from_slice(&checkpoint_digest);
    encoded.extend_from_slice(&crc32fast::hash(&encoded).to_be_bytes());
    encoded
}

fn decode_snapshot_outcome(encoded: &[u8]) -> Result<u64, Status> {
    if encoded.len() != SNAPSHOT_OUTCOME_BYTES
        || encoded[..4] != SNAPSHOT_OUTCOME_MAGIC
        || u16::from_be_bytes(
            encoded[4..6]
                .try_into()
                .expect("fixed snapshot outcome version"),
        ) != SNAPSHOT_OUTCOME_VERSION
        || crc32fast::hash(&encoded[..46])
            != u32::from_be_bytes(
                encoded[46..]
                    .try_into()
                    .expect("fixed snapshot outcome checksum"),
            )
    {
        return Err(Status::internal("durable snapshot receipt is corrupt"));
    }
    Ok(u64::from_be_bytes(
        encoded[6..14]
            .try_into()
            .expect("fixed installed snapshot index"),
    ))
}

fn hex_identifier(identifier: [u8; 16]) -> String {
    identifier
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn hash_file(path: &std::path::Path) -> Result<[u8; 32], std::io::Error> {
    let mut file = File::open(path)?;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = vec![0_u8; 64 * 1024];
    loop {
        let read = std::io::Read::read(&mut file, &mut buffer)?;
        if read == 0 {
            return Ok(*hasher.finalize().as_bytes());
        }
        hasher.update(&buffer[..read]);
    }
}

fn status_response(status: ReplicaStatus) -> ReplicaStatusResponse {
    let role = if status.is_leader() {
        WireReplicaRole::Leader
    } else {
        match status.role() {
            ReplicaRole::Learner => WireReplicaRole::Learner,
            ReplicaRole::Voter => WireReplicaRole::Follower,
        }
    };
    ReplicaStatusResponse {
        node_id: status.node_id(),
        role: role.into(),
        term: status.term(),
        commit_index: status.commit_index(),
        applied_index: status.applied_index(),
        snapshot_index: status.snapshot_index(),
        schema_version: status.schema_version(),
        backend_generation: status.backend_generation(),
        ready: status.ready(),
    }
}

fn protocol_status(error: ProtocolError) -> Status {
    match error {
        ProtocolError::DeadlineExpired { .. } => Status::deadline_exceeded(error.to_string()),
        ProtocolError::CommandTooLarge { .. } | ProtocolError::SnapshotChunkTooLarge { .. } => {
            resource_exhausted_status(error.to_string())
        }
        _ => Status::invalid_argument(error.to_string()),
    }
}

fn artifact_job_id(value: &[u8]) -> Result<u128, Status> {
    let bytes: [u8; 16] = value
        .try_into()
        .map_err(|_| Status::invalid_argument("analytics artifact job ID must contain 16 bytes"))?;
    let job_id = u128::from_be_bytes(bytes);
    if job_id == 0 {
        return Err(Status::invalid_argument(
            "analytics artifact job ID must be non-zero",
        ));
    }
    Ok(job_id)
}

fn artifact_kind(value: i32) -> Result<raft_command::AnalyticsArtifactKindV1, Status> {
    match WireArtifactKind::try_from(value) {
        Ok(WireArtifactKind::Checkpoint) => Ok(raft_command::AnalyticsArtifactKindV1::Checkpoint),
        Ok(WireArtifactKind::Result) => Ok(raft_command::AnalyticsArtifactKindV1::Result),
        _ => Err(Status::invalid_argument(
            "analytics artifact kind is invalid",
        )),
    }
}

fn wire_artifact_kind(kind: raft_command::AnalyticsArtifactKindV1) -> WireArtifactKind {
    match kind {
        raft_command::AnalyticsArtifactKindV1::Checkpoint => WireArtifactKind::Checkpoint,
        raft_command::AnalyticsArtifactKindV1::Result => WireArtifactKind::Result,
    }
}

fn artifact_digest(value: &[u8], name: &'static str) -> Result<[u8; 32], Status> {
    value.try_into().map_err(|_| {
        Status::invalid_argument(format!("analytics artifact {name} must contain 32 bytes"))
    })
}

fn artifact_chunk_count(value: u32) -> Result<usize, Status> {
    let count = usize::try_from(value)
        .map_err(|_| Status::invalid_argument("analytics artifact chunk count is out of range"))?;
    if count == 0 || count > usize::from(shard_runtime::MAX_ANALYTICS_ARTIFACT_CHUNKS) {
        return Err(Status::invalid_argument(
            "analytics artifact chunk count must be between 1 and 4096",
        ));
    }
    Ok(count)
}

async fn stable_artifact_head_scan(
    host: &DataNodeHost,
    key: ReplicaKey,
    read_index: u64,
    placement_epoch: u64,
    span: KeySpan,
) -> Result<(u64, Vec<StableArtifactHeadObservation>), Status> {
    let baseline = host
        .status(key)
        .await
        .map_err(artifact_snapshot_status_turn)?;
    let rows = host.scan(key, span).await.map_err(artifact_host_status)?;
    let mut heads = Vec::with_capacity(rows.len());
    let mut pin_keys = Vec::with_capacity(rows.len());
    for row in rows {
        let identity =
            shard_runtime::decode_analytics_artifact_generation_head_identity(row.key().as_bytes())
                .map_err(|_| Status::data_loss("analytics artifact head key failed validation"))?;
        let head = shard_runtime::decode_analytics_artifact_generation_head(row.value())
            .map_err(|_| Status::data_loss("analytics artifact head failed validation"))?;
        pin_keys.push(shard_runtime::analytics_artifact_generation_pin_key(
            identity.job_id(),
            identity.kind(),
            identity.generation(),
        ));
        heads.push((identity, head));
    }
    let pins = host
        .multi_get(key, pin_keys)
        .await
        .map_err(artifact_host_status)?;
    let final_status = host
        .status(key)
        .await
        .map_err(artifact_snapshot_status_turn)?;
    let applied_index =
        stable_artifact_snapshot_index(read_index, placement_epoch, baseline, final_status)?;
    if pins.len() != heads.len() {
        return Err(Status::data_loss(
            "analytics artifact generation pin scan returned an invalid result count",
        ));
    }
    let observations = heads
        .into_iter()
        .zip(pins)
        .map(|((identity, head), pin)| {
            let pin = pin
                .map(|bytes| {
                    shard_runtime::decode_analytics_artifact_generation_pin(&bytes).map_err(|_| {
                        Status::data_loss("analytics artifact generation pin failed validation")
                    })
                })
                .transpose()?;
            if pin.is_some_and(|pin| pin.expected_chunk_count() != head.count()) {
                return Err(Status::data_loss(
                    "analytics artifact generation pin disagrees with its head",
                ));
            }
            Ok(StableArtifactHeadObservation {
                identity,
                head,
                pin,
            })
        })
        .collect::<Result<Vec<_>, Status>>()?;
    Ok((applied_index, observations))
}

fn stable_artifact_snapshot_index(
    read_index: u64,
    placement_epoch: u64,
    baseline: ReplicaStatus,
    final_status: ReplicaStatus,
) -> Result<u64, Status> {
    let baseline_is_current_leader = baseline.is_leader()
        && baseline.leader_id() == Some(baseline.node_id())
        && baseline.placement_epoch() == placement_epoch
        && baseline.applied_index() >= read_index
        && read_index != 0;
    let stable = baseline.graph_id() == final_status.graph_id()
        && baseline.shard_id() == final_status.shard_id()
        && baseline.node_id() == final_status.node_id()
        && baseline.is_leader() == final_status.is_leader()
        && baseline.leader_id() == final_status.leader_id()
        && baseline.term() == final_status.term()
        && baseline.placement_epoch() == final_status.placement_epoch()
        && baseline.applied_index() == final_status.applied_index();
    if !baseline_is_current_leader || !stable {
        return Err(Status::aborted(
            "analytics artifact snapshot changed during discovery; retry the request",
        ));
    }
    Ok(baseline.applied_index())
}

fn artifact_snapshot_status_turn(error: HostError) -> Status {
    let status = artifact_host_status(error);
    match status.code() {
        Code::DeadlineExceeded | Code::Unavailable | Code::ResourceExhausted => status,
        _ => Status::aborted("analytics artifact snapshot status turn could not be completed"),
    }
}

fn artifact_manifest(
    generation: u64,
    expected_count: usize,
    expected_total_bytes: u64,
    expected_content_digest: &[u8],
) -> Result<[u8; 32], Status> {
    let expected_content_digest = artifact_digest(expected_content_digest, "content digest")?;
    let count = u64::try_from(expected_count)
        .map_err(|_| Status::invalid_argument("analytics artifact chunk count is out of range"))?;
    let maximum_total_bytes = count
        .checked_mul(
            u64::try_from(raft_command::MAX_ANALYTICS_ARTIFACT_CHUNK_BYTES)
                .expect("artifact chunk limit fits in u64"),
        )
        .ok_or_else(|| Status::invalid_argument("analytics artifact byte limit overflow"))?;
    if generation == 0
        || expected_total_bytes < count
        || expected_total_bytes > maximum_total_bytes
        || expected_content_digest == [0; 32]
    {
        return Err(Status::invalid_argument(
            "analytics artifact manifest bounds or digest are invalid",
        ));
    }
    Ok(expected_content_digest)
}

fn artifact_host_status(error: HostError) -> Status {
    match &error {
        HostError::DurableReplica(message) if message.contains("corrupt analytics artifact") => {
            Status::data_loss("analytics artifact failed integrity validation")
        }
        HostError::DurableReplica(message)
            if message.contains("analytics artifact generation")
                || message.contains("analytics artifact chunk conflicts") =>
        {
            Status::failed_precondition(message.clone())
        }
        _ => host_status(error),
    }
}

fn host_status(error: HostError) -> Status {
    match error {
        HostError::UnknownReplica { .. } => Status::not_found(error.to_string()),
        HostError::StaleEpoch { expected, .. } => stale_epoch_status(expected),
        HostError::Overloaded { .. } | HostError::OutboundOverloaded => {
            resource_exhausted_status(error.to_string())
        }
        HostError::ScanByteLimit { limit, required } => scan_byte_limit_status(limit, required),
        HostError::RequestEnvelopeMismatch { .. } => Status::invalid_argument(error.to_string()),
        HostError::RequestMismatch { .. } => Status::already_exists(error.to_string()),
        HostError::InvalidReadContext => Status::invalid_argument(error.to_string()),
        HostError::DuplicateReadContext { .. } => Status::already_exists(error.to_string()),
        HostError::ReadBarrierDeadline { .. } => Status::deadline_exceeded(error.to_string()),
        HostError::ReadBarrierLimit => resource_exhausted_status(error.to_string()),
        HostError::ReadBarrierUnavailable { .. } | HostError::ReadBarrierContextExhausted => {
            Status::unavailable(error.to_string())
        }
        HostError::AdapterUnavailable(_) => Status::unavailable(error.to_string()),
        HostError::ReplicaNotReady { .. } => Status::failed_precondition(error.to_string()),
        HostError::NotLeader { leader_id } => not_leader_status(leader_id),
        HostError::MembershipConflict => Status::failed_precondition(error.to_string()),
        HostError::MembershipPending => Status::unavailable(error.to_string()),
        HostError::ProposalPending { .. } => Status::unavailable(error.to_string()),
        HostError::UnsafeReplicaDelete { .. } => Status::failed_precondition(error.to_string()),
        HostError::ActivationFenceMismatch => Status::failed_precondition(error.to_string()),
        HostError::LearnerNotYetSupported => Status::failed_precondition(error.to_string()),
        HostError::ActorStopped => Status::unavailable(error.to_string()),
        _ => Status::internal(error.to_string()),
    }
}

fn not_leader_status(leader_id: Option<u64>) -> Status {
    let mut status = Status::failed_precondition("Replica is not the Shard leader");
    status
        .metadata_mut()
        .insert("dtgproxy-reason", MetadataValue::from_static("not_leader"));
    if let Some(leader_id) = leader_id
        && let Ok(value) = MetadataValue::try_from(leader_id.to_string())
    {
        status.metadata_mut().insert("dtgproxy-leader-node", value);
    }
    status
}

fn stale_epoch_status(current_epoch: u64) -> Status {
    let mut status = Status::failed_precondition("stale Shard placement epoch");
    status
        .metadata_mut()
        .insert("dtgproxy-reason", MetadataValue::from_static("stale_epoch"));
    if let Ok(value) = MetadataValue::try_from(current_epoch.to_string()) {
        status
            .metadata_mut()
            .insert("dtgproxy-current-epoch", value);
    }
    status
}

fn resource_exhausted_status(message: String) -> Status {
    let mut status = Status::resource_exhausted(message);
    status.metadata_mut().insert(
        "dtgproxy-reason",
        MetadataValue::from_static("resource_exhausted"),
    );
    status
}

fn scan_byte_limit_status(limit: u64, required: u64) -> Status {
    let mut status = Status::resource_exhausted(format!(
        "scan requires {required} bytes, exceeding byte limit {limit}"
    ));
    status.metadata_mut().insert(
        "dtgproxy-reason",
        MetadataValue::from_static("scan_byte_limit"),
    );
    if let Ok(value) = MetadataValue::try_from(limit.to_string()) {
        status.metadata_mut().insert("dtgproxy-scan-limit", value);
    }
    if let Ok(value) = MetadataValue::try_from(required.to_string()) {
        status
            .metadata_mut()
            .insert("dtgproxy-scan-required", value);
    }
    status
}

#[cfg(test)]
mod tests {
    use super::*;

    fn replica_status(
        placement_epoch: u64,
        leader: bool,
        leader_id: Option<u64>,
        term: u64,
        applied_index: u64,
    ) -> ReplicaStatus {
        ReplicaStatus::new(
            1,
            11,
            placement_epoch,
            7,
            leader,
            leader_id,
            term,
            applied_index,
            applied_index,
            ReplicaRole::Voter,
            1,
            1,
            0,
            true,
        )
    }

    #[test]
    fn durable_read_index_capacity_maps_to_resource_exhausted() {
        let host_error = HostError::from_durable(
            shard_runtime::DurableReplicaError::TooManyPendingReadIndexRequests,
        );
        assert_eq!(host_error, HostError::ReadBarrierLimit);

        let status = host_status(host_error);
        assert_eq!(status.code(), tonic::Code::ResourceExhausted);
        assert_eq!(
            status.metadata().get("dtgproxy-reason").unwrap(),
            "resource_exhausted"
        );
    }

    #[test]
    fn adapter_unavailable_maps_to_retryable_transport_status() {
        let host_error = HostError::from_adapter(storage_api::AdapterError::Unavailable(
            "backend restart".into(),
        ));

        let status = host_status(host_error);
        assert_eq!(status.code(), tonic::Code::Unavailable);
    }

    #[test]
    fn artifact_snapshot_status_fence_accepts_only_one_stable_leader_state() {
        let stable = replica_status(3, true, Some(7), 5, 12);
        assert_eq!(
            stable_artifact_snapshot_index(10, 3, stable, stable).unwrap(),
            12
        );

        for final_status in [
            replica_status(3, true, Some(7), 6, 12),
            replica_status(3, false, Some(8), 5, 12),
            replica_status(4, true, Some(7), 5, 12),
            replica_status(3, true, Some(7), 5, 13),
        ] {
            assert_eq!(
                stable_artifact_snapshot_index(10, 3, stable, final_status)
                    .unwrap_err()
                    .code(),
                tonic::Code::Aborted
            );
        }
        assert_eq!(
            stable_artifact_snapshot_index(13, 3, stable, stable)
                .unwrap_err()
                .code(),
            tonic::Code::Aborted
        );
        let wrong_epoch = replica_status(4, true, Some(7), 5, 12);
        assert_eq!(
            stable_artifact_snapshot_index(10, 3, wrong_epoch, wrong_epoch)
                .unwrap_err()
                .code(),
            tonic::Code::Aborted
        );
        assert_eq!(
            artifact_snapshot_status_turn(HostError::ActorStopped).code(),
            tonic::Code::Unavailable
        );
    }
}
