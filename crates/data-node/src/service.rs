use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use cluster_protocol::proto::node_admin_service_server::NodeAdminService;
use cluster_protocol::proto::shard_service_server::ShardService;
use cluster_protocol::proto::{
    ChangeMembershipRequest, ChangeMembershipResponse, DeleteReplicaRequest, DeleteReplicaResponse,
    EnsureReplicaRequest, EnsureReplicaResponse, ExecuteRequest, ExecuteResponse,
    GetMigrationReceiptRequest, GetMigrationReceiptResponse, InstallSnapshotResponse, ReadRequest,
    ReadResponse, ReplicaRole as WireReplicaRole, ReplicaStatusRequest, ReplicaStatusResponse,
    ScanBatch, ScanRequest, SnapshotChunk,
};
use cluster_protocol::{CommandPayload, CommonRequestContext, ProtocolError, ShardRequestContext};
use storage_api::{Keyspace, LogicalKey};
use tokio_stream::wrappers::ReceiverStream;
use tonic::metadata::MetadataValue;
use tonic::{Request, Response, Status};

use crate::{
    DataNodeHost, EnsureReplicaOutcome, HostError, ReplicaKey, ReplicaRole, ReplicaSpec,
    ReplicaStatus,
};

const READ_PLAN_MAGIC: [u8; 4] = *b"DTRK";
const READ_RESULT_MAGIC: [u8; 4] = *b"DTRV";
const READ_CODEC_VERSION: u16 = 1;
const MAX_READ_KEYS: usize = 4_096;
const MAX_READ_KEY_BYTES: usize = 1024 * 1024;
const MAX_READ_RESULT_BYTES: usize = 16 * 1024 * 1024;
const REPLICA_PROFILE_MAGIC: [u8; 4] = *b"DTRF";
const MAX_PROFILE_VOTERS: usize = 64;
const MAX_REPLICA_DIRECTORY_BYTES: usize = 255;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DataOperation {
    Execute,
    Read,
    Scan,
    InstallSnapshot,
    ReplicaStatus,
    EnsureReplica,
    ChangeMembership,
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
}

impl DataNodeGrpcService {
    #[must_use]
    pub fn new(host: Arc<DataNodeHost>) -> Self {
        Self {
            host,
            authorizer: Arc::new(AllowAllAuthorizer),
        }
    }

    #[must_use]
    pub fn with_authorizer(
        host: Arc<DataNodeHost>,
        authorizer: Arc<dyn RequestAuthorizer>,
    ) -> Self {
        Self { host, authorizer }
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
        let outcome = self
            .host
            .propose_with_outcome(
                key,
                context.placement_epoch(),
                request_id,
                command.into_bytes(),
            )
            .await
            .map_err(host_status)?;
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
        _request: Request<ScanRequest>,
    ) -> Result<Response<Self::ScanStream>, Status> {
        Err(Status::unimplemented(
            "Arrow scan execution is connected in the distributed query milestone",
        ))
    }

    async fn install_snapshot(
        &self,
        _request: Request<tonic::Streaming<SnapshotChunk>>,
    ) -> Result<Response<InstallSnapshotResponse>, Status> {
        Err(Status::unimplemented(
            "snapshot stream activation is connected in the migration milestone",
        ))
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
        _request: Request<ChangeMembershipRequest>,
    ) -> Result<Response<ChangeMembershipResponse>, Status> {
        Err(Status::unimplemented(
            "joint-consensus membership is connected in the migration milestone",
        ))
    }

    async fn delete_replica(
        &self,
        _request: Request<DeleteReplicaRequest>,
    ) -> Result<Response<DeleteReplicaResponse>, Status> {
        Err(Status::unimplemented(
            "receipt-gated Replica deletion is connected in the migration milestone",
        ))
    }

    async fn get_migration_receipt(
        &self,
        request: Request<GetMigrationReceiptRequest>,
    ) -> Result<Response<GetMigrationReceiptResponse>, Status> {
        let request = request.into_inner();
        self.validate_common(request.context, DataOperation::MigrationReceipt)?;
        validate_identifier(&request.migration_id, "migration ID")?;
        Err(Status::unimplemented(
            "durable migration receipts are connected in the migration milestone",
        ))
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
        snapshot_index: 0,
        schema_version: status.schema_version(),
        backend_generation: status.backend_generation(),
        ready: true,
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
