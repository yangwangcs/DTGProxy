use core::fmt;

use dtg_kernel::ReplicaId;

use crate::{RequestContext, ShardRequestContext, context::exact_u128, proto};

pub const MAX_TRACE_CONTEXT_BYTES: usize = 4 * 1024;
pub const MAX_FRAGMENT_BYTES: usize = 1024 * 1024;
pub const MAX_FRAGMENT_ITEMS: u32 = 4_096;
pub const MAX_BATCH_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_BATCH_ROWS: u32 = 65_536;
pub const MAX_TRANSACTION_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_TRANSACTION_ITEMS: u32 = 4_096;
pub const MAX_RAFT_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_SNAPSHOT_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_SNAPSHOT_CHUNKS: u32 = 4_096;
pub const MAX_OBSERVATION_BYTES: usize = 1024 * 1024;
pub const MAX_OBSERVATION_ITEMS: u32 = 4_096;
pub const MAX_STATUS_MESSAGE_BYTES: usize = 1024;
pub const MAX_STATUS_DETAILS_BYTES: usize = 64 * 1024;
const CURRENT_FORMAT_VERSION: u32 = 1;
const MAX_REQUEST_CONTEXT_WIRE_BYTES: usize = 8 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProtocolError {
    MajorVersion,
    MinorVersion,
    IdentifierLength,
    ZeroCluster,
    ZeroRequest,
    ZeroDeadline,
    TraceLimit,
    MissingContext,
    ZeroGraph,
    ZeroShard,
    ZeroEpoch,
    ZeroGeneration,
    ZeroCatalog,
    MissingPayload,
    FormatVersion,
    PayloadLimit,
    ItemLimit,
    LengthMismatch,
    Checksum,
    UnknownEnum,
    ZeroRaftTerm,
    ChunkRange,
    StatusLimit,
    Malformed,
}

impl ProtocolError {
    pub const fn code(&self) -> &'static str {
        match self {
            Self::MajorVersion => "DTG-PROTOCOL-MAJOR",
            Self::MinorVersion => "DTG-PROTOCOL-MINOR",
            Self::IdentifierLength => "DTG-PROTOCOL-ID-LENGTH",
            Self::ZeroCluster => "DTG-PROTOCOL-ZERO-CLUSTER",
            Self::ZeroRequest => "DTG-PROTOCOL-ZERO-REQUEST",
            Self::ZeroDeadline => "DTG-PROTOCOL-ZERO-DEADLINE",
            Self::TraceLimit => "DTG-PROTOCOL-TRACE-LIMIT",
            Self::MissingContext => "DTG-PROTOCOL-MISSING-CONTEXT",
            Self::ZeroGraph => "DTG-PROTOCOL-ZERO-GRAPH",
            Self::ZeroShard => "DTG-PROTOCOL-ZERO-SHARD",
            Self::ZeroEpoch => "DTG-PROTOCOL-ZERO-EPOCH",
            Self::ZeroGeneration => "DTG-PROTOCOL-ZERO-GENERATION",
            Self::ZeroCatalog => "DTG-PROTOCOL-ZERO-CATALOG",
            Self::MissingPayload => "DTG-PROTOCOL-MISSING-PAYLOAD",
            Self::FormatVersion => "DTG-PROTOCOL-FORMAT-VERSION",
            Self::PayloadLimit => "DTG-PROTOCOL-PAYLOAD-LIMIT",
            Self::ItemLimit => "DTG-PROTOCOL-ITEM-LIMIT",
            Self::LengthMismatch => "DTG-PROTOCOL-LENGTH",
            Self::Checksum => "DTG-PROTOCOL-CHECKSUM",
            Self::UnknownEnum => "DTG-PROTOCOL-ENUM",
            Self::ZeroRaftTerm => "DTG-PROTOCOL-ZERO-RAFT-TERM",
            Self::ChunkRange => "DTG-PROTOCOL-CHUNK-RANGE",
            Self::StatusLimit => "DTG-PROTOCOL-STATUS-LIMIT",
            Self::Malformed => "DTG-PROTOCOL-MALFORMED",
        }
    }
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code())
    }
}

impl std::error::Error for ProtocolError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidatedPayload {
    format_version: u32,
    item_count: u32,
    body: Vec<u8>,
}

impl ValidatedPayload {
    pub const fn format_version(&self) -> u32 {
        self.format_version
    }

    pub const fn item_count(&self) -> u32 {
        self.item_count
    }

    pub fn len(&self) -> usize {
        self.body.len()
    }

    pub fn is_empty(&self) -> bool {
        self.body.is_empty()
    }

    pub fn body(&self) -> &[u8] {
        &self.body
    }

    pub fn into_body(self) -> Vec<u8> {
        self.body
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidatedStatus {
    details: Option<ValidatedPayload>,
}

impl ValidatedStatus {
    pub const fn details(&self) -> Option<&ValidatedPayload> {
        self.details.as_ref()
    }
}

pub fn checksum_bytes(body: &[u8]) -> [u8; 32] {
    *blake3::hash(body).as_bytes()
}

pub fn decode_request_context(bytes: &[u8]) -> Result<RequestContext, ProtocolError> {
    use prost::Message;

    if bytes.len() > MAX_REQUEST_CONTEXT_WIRE_BYTES {
        return Err(ProtocolError::PayloadLimit);
    }
    let wire = proto::RequestContext::decode(bytes).map_err(|_| ProtocolError::Malformed)?;
    wire.try_into()
}

pub fn validate_execution_fragment(
    wire: proto::ExecutionFragment,
) -> Result<ValidatedPayload, ProtocolError> {
    let _context: ShardRequestContext = wire
        .context
        .ok_or(ProtocolError::MissingContext)?
        .try_into()?;
    require_nonzero_id(&wire.fragment_id)?;
    validate_payload(wire.payload, MAX_FRAGMENT_BYTES, MAX_FRAGMENT_ITEMS)
}

pub fn validate_column_batch(wire: proto::ColumnBatch) -> Result<ValidatedPayload, ProtocolError> {
    let _context: RequestContext = wire
        .request
        .ok_or(ProtocolError::MissingContext)?
        .try_into()?;
    require_nonzero_id(&wire.fragment_id)?;
    if wire.sequence == 0 || wire.row_count == 0 || wire.row_count > MAX_BATCH_ROWS {
        return Err(ProtocolError::ItemLimit);
    }
    let payload = validate_payload(wire.payload, MAX_BATCH_BYTES, MAX_BATCH_ROWS)?;
    if payload.item_count != wire.row_count {
        return Err(ProtocolError::LengthMismatch);
    }
    Ok(payload)
}

pub fn validate_transaction_request(
    wire: proto::TransactionRequest,
) -> Result<ValidatedPayload, ProtocolError> {
    let _context: ShardRequestContext = wire
        .context
        .ok_or(ProtocolError::MissingContext)?
        .try_into()?;
    require_nonzero_id(&wire.transaction_id)?;
    require_nonzero_id(&wire.idempotency_key)?;
    if !(1..=4).contains(&wire.operation) {
        return Err(ProtocolError::UnknownEnum);
    }
    validate_payload(wire.payload, MAX_TRANSACTION_BYTES, MAX_TRANSACTION_ITEMS)
}

pub fn validate_raft_envelope(
    wire: proto::RaftEnvelope,
) -> Result<ValidatedPayload, ProtocolError> {
    let _context: ShardRequestContext = wire
        .context
        .ok_or(ProtocolError::MissingContext)?
        .try_into()?;
    ReplicaId::new(wire.from_replica_id).map_err(|_| ProtocolError::IdentifierLength)?;
    ReplicaId::new(wire.to_replica_id).map_err(|_| ProtocolError::IdentifierLength)?;
    if wire.term == 0 {
        return Err(ProtocolError::ZeroRaftTerm);
    }
    if !(1..=4).contains(&wire.kind) {
        return Err(ProtocolError::UnknownEnum);
    }
    validate_payload(wire.payload, MAX_RAFT_BYTES, 1)
}

pub fn validate_replica_snapshot(
    wire: proto::LogicalReplicaSnapshot,
) -> Result<ValidatedPayload, ProtocolError> {
    let _context: ShardRequestContext = wire
        .context
        .ok_or(ProtocolError::MissingContext)?
        .try_into()?;
    require_nonzero_id(&wire.snapshot_id)?;
    if wire.snapshot_version != CURRENT_FORMAT_VERSION {
        return Err(ProtocolError::FormatVersion);
    }
    if wire.last_included_term == 0
        || wire.chunk_count == 0
        || wire.chunk_count > MAX_SNAPSHOT_CHUNKS
        || wire.chunk_index >= wire.chunk_count
    {
        return Err(ProtocolError::ChunkRange);
    }
    validate_payload(wire.payload, MAX_SNAPSHOT_BYTES, MAX_TRANSACTION_ITEMS)
}

pub fn validate_control_observation(
    wire: proto::ControlObservation,
) -> Result<ValidatedPayload, ProtocolError> {
    let _context: RequestContext = wire
        .request
        .ok_or(ProtocolError::MissingContext)?
        .try_into()?;
    require_nonzero_id(&wire.node_id)?;
    if wire.observation_version != CURRENT_FORMAT_VERSION {
        return Err(ProtocolError::FormatVersion);
    }
    if wire.observed_at_unix_ms == 0 {
        return Err(ProtocolError::ZeroDeadline);
    }
    validate_payload(wire.payload, MAX_OBSERVATION_BYTES, MAX_OBSERVATION_ITEMS)
}

pub fn validate_typed_status(wire: proto::TypedStatus) -> Result<ValidatedStatus, ProtocolError> {
    let _context: RequestContext = wire
        .request
        .ok_or(ProtocolError::MissingContext)?
        .try_into()?;
    if !(1..=6).contains(&wire.code) || !(1..=3).contains(&wire.retry) {
        return Err(ProtocolError::UnknownEnum);
    }
    if wire.message.len() > MAX_STATUS_MESSAGE_BYTES {
        return Err(ProtocolError::StatusLimit);
    }
    if wire.retry == 3 || !wire.idempotency_key.is_empty() {
        require_nonzero_id(&wire.idempotency_key)?;
    }
    let details = wire
        .details
        .map(|payload| validate_payload(Some(payload), MAX_STATUS_DETAILS_BYTES, 256))
        .transpose()?;
    Ok(ValidatedStatus { details })
}

fn validate_payload(
    payload: Option<proto::BoundedPayload>,
    max_bytes: usize,
    max_items: u32,
) -> Result<ValidatedPayload, ProtocolError> {
    let payload = payload.ok_or(ProtocolError::MissingPayload)?;
    if payload.format_version != CURRENT_FORMAT_VERSION {
        return Err(ProtocolError::FormatVersion);
    }
    if payload.item_count == 0 || payload.item_count > max_items {
        return Err(ProtocolError::ItemLimit);
    }
    let declared_len =
        usize::try_from(payload.declared_len).map_err(|_| ProtocolError::PayloadLimit)?;
    if declared_len > max_bytes {
        return Err(ProtocolError::PayloadLimit);
    }
    if payload.body.len() != declared_len {
        return Err(ProtocolError::LengthMismatch);
    }
    let expected: [u8; 32] = payload
        .checksum
        .as_slice()
        .try_into()
        .map_err(|_| ProtocolError::Checksum)?;
    if checksum_bytes(&payload.body) != expected {
        return Err(ProtocolError::Checksum);
    }
    Ok(ValidatedPayload {
        format_version: payload.format_version,
        item_count: payload.item_count,
        body: payload.body,
    })
}

fn require_nonzero_id(bytes: &[u8]) -> Result<u128, ProtocolError> {
    let value = exact_u128(bytes)?;
    if value == 0 {
        return Err(ProtocolError::ZeroRequest);
    }
    Ok(value)
}
