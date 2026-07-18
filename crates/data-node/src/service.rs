use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::fs::File;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use cluster_protocol::proto::node_admin_service_server::NodeAdminService;
use cluster_protocol::proto::shard_service_server::ShardService;
use cluster_protocol::proto::{
    ActivateReplicaRequest, ActivateReplicaResponse, ChangeMembershipRequest,
    ChangeMembershipResponse, DeleteReplicaRequest, DeleteReplicaResponse, EnsureReplicaRequest,
    EnsureReplicaResponse, ExecuteRequest, ExecuteResponse, ExportSnapshotRequest,
    GetMigrationReceiptRequest, GetMigrationReceiptResponse, InstallSnapshotResponse, ReadRequest,
    ReadResponse, ReplicaRole as WireReplicaRole, ReplicaStatusRequest, ReplicaStatusResponse,
    ScanBatch, ScanRequest, SnapshotChunk,
};
use cluster_protocol::{CommandPayload, CommonRequestContext, ProtocolError, ShardRequestContext};
use storage_api::{KeySpan, KeyValue, Keyspace, LogicalKey};
use tokio::io::AsyncReadExt;
use tokio::sync::{Mutex, mpsc};
use tokio_stream::{StreamExt, wrappers::ReceiverStream};
use tonic::metadata::MetadataValue;
use tonic::{Request, Response, Status};

use crate::{
    ChunkAppendOutcome, DataNodeHost, EnsureReplicaOutcome, HostError, MigrationChunk, ReplicaKey,
    ReplicaRole, ReplicaSpec, ReplicaStatus,
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
const SNAPSHOT_INSTALL_STEP: u32 = 2;
const SNAPSHOT_EXPORT_STEP: u32 = 1;
const SNAPSHOT_STREAM_CHUNK_BYTES: usize = 1024 * 1024;
const SNAPSHOT_OUTCOME_MAGIC: [u8; 4] = *b"DTSO";
const SNAPSHOT_OUTCOME_VERSION: u16 = 1;
const SNAPSHOT_OUTCOME_BYTES: usize = 50;
const PROPOSAL_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(2);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DataOperation {
    Execute,
    Read,
    Scan,
    ExportSnapshot,
    InstallSnapshot,
    ReplicaStatus,
    EnsureReplica,
    ChangeMembership,
    ActivateReplica,
    DeleteReplica,
    MigrationReceipt,
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
        let values = self.host.multi_get(key, keys).await.map_err(host_status)?;
        let result = encode_key_read_result(&values)
            .map_err(|error| Status::resource_exhausted(error.to_string()))?;
        Ok(Response::new(ReadResponse {
            applied_index: status.applied_index(),
            result,
        }))
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
        let profile = decode_rocks_replica_profile(&request.backend_profile)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let spec = ReplicaSpec::new(
            key.graph_id(),
            key.shard_id(),
            context.placement_epoch(),
            profile.voters,
            role,
            request.schema_version,
            request.backend_generation,
            profile.relative_directory,
        )
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
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RocksReplicaProfile {
    voters: Vec<u64>,
    relative_directory: String,
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

fn decode_rocks_replica_profile(
    encoded: &[u8],
) -> Result<RocksReplicaProfile, ReplicaProfileError> {
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
    Ok(RocksReplicaProfile {
        voters,
        relative_directory,
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
        }
    }
}

impl Error for ReplicaProfileError {}

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
    let mut encoded = Vec::new();
    encoded.extend_from_slice(&SCAN_PLAN_MAGIC);
    encoded.extend_from_slice(&READ_CODEC_VERSION.to_be_bytes());
    encoded.push(span.keyspace().tag());
    encoded.extend_from_slice(&start_length.to_be_bytes());
    encoded.extend_from_slice(&end_length.to_be_bytes());
    encoded.extend_from_slice(&prefix_length.to_be_bytes());
    encoded.extend_from_slice(&limit.to_be_bytes());
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
    if encoded.len() < 31 || encoded.len() > MAX_SCAN_PLAN_BYTES {
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
    let checksum_offset = encoded.len() - 4;
    let mut offset = 27;
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
        offset += 9;
        let key = take_scan_bytes(encoded, &mut offset, key_length, checksum_offset)?;
        let value = take_scan_bytes(encoded, &mut offset, value_length, checksum_offset)?;
        rows.push(KeyValue::new(LogicalKey::in_keyspace(keyspace, key), value));
    }
    if offset != checksum_offset {
        return Err(ReadCodecError::TrailingBytes);
    }
    Ok(rows)
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

fn host_status(error: HostError) -> Status {
    match error {
        HostError::UnknownReplica { .. } => Status::not_found(error.to_string()),
        HostError::StaleEpoch { expected, .. } => stale_epoch_status(expected),
        HostError::Overloaded { .. } | HostError::OutboundOverloaded => {
            resource_exhausted_status(error.to_string())
        }
        HostError::RequestEnvelopeMismatch { .. } => Status::invalid_argument(error.to_string()),
        HostError::RequestMismatch { .. } => Status::already_exists(error.to_string()),
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
