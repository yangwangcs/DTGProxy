#![forbid(unsafe_code)]

mod context;
mod validate;

pub mod proto {
    tonic::include_proto!("dtgproxy.cluster.v2");
}

pub use context::{RequestContext, SUPPORTED_MINOR_MAX, SUPPORTED_MINOR_MIN, ShardRequestContext};
pub use validate::{
    MAX_BATCH_BYTES, MAX_BATCH_ROWS, MAX_FRAGMENT_BYTES, MAX_FRAGMENT_ITEMS, MAX_GATEWAY_FRAGMENTS,
    MAX_GATEWAY_REQUEST_BYTES, MAX_OBSERVATION_BYTES, MAX_OBSERVATION_ITEMS, MAX_RAFT_BYTES,
    MAX_SNAPSHOT_BYTES, MAX_SNAPSHOT_CHUNKS, MAX_STATUS_DETAILS_BYTES, MAX_STATUS_MESSAGE_BYTES,
    MAX_TRACE_CONTEXT_BYTES, MAX_TRANSACTION_BYTES, MAX_TRANSACTION_ITEMS, ProtocolError,
    ValidatedGatewayRequest, ValidatedPayload, ValidatedStatus, checksum_bytes,
    decode_request_context, validate_column_batch, validate_control_observation,
    validate_execution_fragment, validate_gateway_request, validate_raft_envelope,
    validate_replica_snapshot, validate_snapshot_ingest_batch,
    validate_snapshot_ingest_receipt_request, validate_transaction_request, validate_typed_status,
};

pub const PROTOCOL_MAJOR: u32 = 2;
pub const CLEAN_BREAK_ARCHITECTURE_VERSION: u32 = 1;
