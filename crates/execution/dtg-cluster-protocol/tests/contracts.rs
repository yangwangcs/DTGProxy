use dtg_cluster_v2::{
    MAX_BATCH_BYTES, MAX_BATCH_ROWS, MAX_FRAGMENT_BYTES, MAX_STATUS_MESSAGE_BYTES, PROTOCOL_MAJOR,
    SUPPORTED_MINOR_MAX, SUPPORTED_MINOR_MIN, ShardRequestContext, checksum_bytes,
    decode_request_context, proto, validate_column_batch, validate_control_observation,
    validate_execution_fragment, validate_gateway_request, validate_raft_envelope,
    validate_replica_snapshot, validate_transaction_request, validate_typed_status,
};
use prost::Message;

#[test]
fn shard_context_requires_backend_generation() {
    let wire = proto::ShardContext {
        backend_generation: 0,
        ..valid_shard_context()
    };
    assert_eq!(
        ShardRequestContext::try_from(wire).unwrap_err().code(),
        "DTG-PROTOCOL-ZERO-GENERATION"
    );
}

#[test]
fn only_current_major_and_documented_minor_window_are_accepted() {
    for minor in SUPPORTED_MINOR_MIN..=SUPPORTED_MINOR_MAX {
        let wire = proto::RequestContext {
            protocol_minor: minor,
            ..valid_request_context()
        };
        assert!(dtg_cluster_v2::RequestContext::try_from(wire).is_ok());
    }

    let old_major = proto::RequestContext {
        protocol_major: PROTOCOL_MAJOR - 1,
        ..valid_request_context()
    };
    assert_eq!(
        dtg_cluster_v2::RequestContext::try_from(old_major)
            .unwrap_err()
            .code(),
        "DTG-PROTOCOL-MAJOR"
    );

    let future_minor = proto::RequestContext {
        protocol_minor: SUPPORTED_MINOR_MAX + 1,
        ..valid_request_context()
    };
    assert_eq!(
        dtg_cluster_v2::RequestContext::try_from(future_minor)
            .unwrap_err()
            .code(),
        "DTG-PROTOCOL-MINOR"
    );
}

#[test]
fn facade_rejects_old_major_before_returning_a_context() {
    let old = proto::RequestContext {
        protocol_major: 1,
        ..valid_request_context()
    };
    let encoded = old.encode_to_vec();
    assert_eq!(
        decode_request_context(&encoded).unwrap_err().code(),
        "DTG-PROTOCOL-MAJOR"
    );
}

#[test]
fn common_and_shard_identifiers_are_exact_and_nonzero() {
    let short_cluster = proto::RequestContext {
        cluster_id: vec![1; 7],
        ..valid_request_context()
    };
    assert_eq!(
        dtg_cluster_v2::RequestContext::try_from(short_cluster)
            .unwrap_err()
            .code(),
        "DTG-PROTOCOL-ID-LENGTH"
    );

    let zero_graph = proto::ShardContext {
        graph_id: 0,
        ..valid_shard_context()
    };
    assert_eq!(
        ShardRequestContext::try_from(zero_graph)
            .unwrap_err()
            .code(),
        "DTG-PROTOCOL-ZERO-GRAPH"
    );
}

#[test]
fn declared_limits_fail_before_checksum_work_or_large_allocation() {
    let mut fragment = valid_fragment();
    fragment.payload = Some(proto::BoundedPayload {
        format_version: 1,
        declared_len: MAX_FRAGMENT_BYTES as u64 + 1,
        item_count: 1,
        checksum: vec![0],
        body: Vec::new(),
    });
    assert_eq!(
        validate_execution_fragment(fragment).unwrap_err().code(),
        "DTG-PROTOCOL-PAYLOAD-LIMIT"
    );
}

#[test]
fn execution_fragment_rejects_a_mutable_snapshot_fence() {
    let mut fragment = valid_fragment();
    fragment.snapshot_immutable = false;

    assert_eq!(
        validate_execution_fragment(fragment).unwrap_err().code(),
        "DTG-PROTOCOL-MUTABLE-SNAPSHOT"
    );
}

#[test]
fn declared_length_and_checksum_must_match_exactly() {
    let mut fragment = valid_fragment();
    fragment.payload.as_mut().unwrap().declared_len += 1;
    assert_eq!(
        validate_execution_fragment(fragment).unwrap_err().code(),
        "DTG-PROTOCOL-LENGTH"
    );

    let mut fragment = valid_fragment();
    fragment.payload.as_mut().unwrap().checksum[0] ^= 1;
    assert_eq!(
        validate_execution_fragment(fragment).unwrap_err().code(),
        "DTG-PROTOCOL-CHECKSUM"
    );
}

#[test]
fn all_bounded_v2_message_families_validate() {
    let gateway = validate_gateway_request(valid_gateway_request()).unwrap();
    assert_eq!(gateway.execution().len(), 3);
    assert_eq!(gateway.fragments().len(), 1);
    assert_eq!(
        validate_execution_fragment(valid_fragment()).unwrap().len(),
        3
    );
    assert_eq!(validate_column_batch(valid_batch()).unwrap().len(), 3);
    assert_eq!(
        validate_transaction_request(valid_transaction())
            .unwrap()
            .len(),
        3
    );
    assert_eq!(validate_raft_envelope(valid_raft()).unwrap().len(), 3);
    assert_eq!(
        validate_replica_snapshot(valid_snapshot()).unwrap().len(),
        3
    );
    assert_eq!(
        validate_control_observation(valid_observation())
            .unwrap()
            .len(),
        3
    );
    assert_eq!(
        validate_typed_status(valid_status())
            .unwrap()
            .details()
            .unwrap()
            .len(),
        3
    );
}

#[test]
fn explicit_empty_column_batch_is_valid_but_malformed_batches_still_fail_closed() {
    let mut empty = valid_batch();
    empty.row_count = 0;
    empty.payload = Some(payload(b"schema-and-zero-rows", 0));
    assert_eq!(
        validate_column_batch(empty.clone()).unwrap().item_count(),
        0
    );

    let mut count_mismatch = empty.clone();
    count_mismatch.payload.as_mut().unwrap().item_count = 1;
    assert_eq!(
        validate_column_batch(count_mismatch).unwrap_err().code(),
        "DTG-PROTOCOL-LENGTH"
    );

    let mut corrupt = empty;
    corrupt.payload.as_mut().unwrap().checksum[0] ^= 1;
    assert_eq!(
        validate_column_batch(corrupt).unwrap_err().code(),
        "DTG-PROTOCOL-CHECKSUM"
    );

    let mut malformed_nonempty = valid_batch();
    malformed_nonempty.payload.as_mut().unwrap().item_count = 0;
    assert_eq!(
        validate_column_batch(malformed_nonempty)
            .unwrap_err()
            .code(),
        "DTG-PROTOCOL-LENGTH"
    );
}

#[test]
fn gateway_request_fails_closed_when_its_execution_envelope_is_tampered() {
    let mut request = valid_gateway_request();
    request.execution_request.as_mut().unwrap().checksum[0] ^= 1;

    assert_eq!(
        validate_gateway_request(request).unwrap_err().code(),
        "DTG-PROTOCOL-CHECKSUM"
    );
}

#[test]
fn raft_control_messages_are_part_of_the_current_protocol() {
    let mut raft = valid_raft();
    raft.kind = proto::RaftMessageKind::Control.into();

    assert_eq!(validate_raft_envelope(raft).unwrap().len(), 3);
}

#[test]
fn family_specific_versions_ids_counts_and_enums_fail_closed() {
    let mut fragment = valid_fragment();
    fragment.payload.as_mut().unwrap().format_version = 2;
    assert_eq!(
        validate_execution_fragment(fragment).unwrap_err().code(),
        "DTG-PROTOCOL-FORMAT-VERSION"
    );

    let mut batch = valid_batch();
    batch.row_count = MAX_BATCH_ROWS + 1;
    batch.payload.as_mut().unwrap().declared_len = MAX_BATCH_BYTES as u64 + 1;
    assert_eq!(
        validate_column_batch(batch).unwrap_err().code(),
        "DTG-PROTOCOL-ITEM-LIMIT"
    );

    let mut transaction = valid_transaction();
    transaction.operation = 99;
    assert_eq!(
        validate_transaction_request(transaction)
            .unwrap_err()
            .code(),
        "DTG-PROTOCOL-ENUM"
    );

    let mut raft = valid_raft();
    raft.term = 0;
    assert_eq!(
        validate_raft_envelope(raft).unwrap_err().code(),
        "DTG-PROTOCOL-ZERO-RAFT-TERM"
    );

    let mut snapshot = valid_snapshot();
    snapshot.chunk_count = 0;
    assert_eq!(
        validate_replica_snapshot(snapshot).unwrap_err().code(),
        "DTG-PROTOCOL-CHUNK-RANGE"
    );

    let mut observation = valid_observation();
    observation.observation_version = 2;
    assert_eq!(
        validate_control_observation(observation)
            .unwrap_err()
            .code(),
        "DTG-PROTOCOL-FORMAT-VERSION"
    );

    let mut status = valid_status();
    status.message = "x".repeat(MAX_STATUS_MESSAGE_BYTES + 1);
    assert_eq!(
        validate_typed_status(status).unwrap_err().code(),
        "DTG-PROTOCOL-STATUS-LIMIT"
    );
}

#[test]
fn generated_facade_contains_only_v2_thin_services() {
    let source = include_str!("../proto/dtg_cluster_v2.proto");
    assert!(source.contains("package dtgproxy.cluster.v2;"));
    for service in [
        "service GatewayService",
        "service DataService",
        "service MetaService",
        "service ControllerService",
    ] {
        assert!(source.contains(service), "missing {service}");
    }
    let old_package = ["dtgproxy.cluster.", "v", "1"].concat();
    let old_internal_name = ["cluster", "_", "protocol"].concat();
    let provider_native_field = ["provider", "_", "payload"].concat();
    let process_logic_token = ["state", "_", "machine"].concat();
    assert!(!source.contains(&old_package));
    assert!(!source.contains(&old_internal_name));
    assert!(!source.contains(&provider_native_field));
    assert!(!source.contains(&process_logic_token));
}

fn valid_request_context() -> proto::RequestContext {
    proto::RequestContext {
        protocol_major: PROTOCOL_MAJOR,
        protocol_minor: SUPPORTED_MINOR_MAX,
        cluster_id: 7_u64.to_be_bytes().to_vec(),
        request_id: id128(11),
        deadline_unix_ms: 1_900_000_000_000,
        trace_context: b"traceparent".to_vec(),
    }
}

fn valid_shard_context() -> proto::ShardContext {
    proto::ShardContext {
        request: Some(valid_request_context()),
        graph_id: 7,
        shard_id: 9,
        placement_epoch: 11,
        backend_generation: 13,
        catalog_version: 17,
    }
}

fn payload(body: &[u8], item_count: u32) -> proto::BoundedPayload {
    proto::BoundedPayload {
        format_version: 1,
        declared_len: body.len() as u64,
        item_count,
        checksum: checksum_bytes(body).to_vec(),
        body: body.to_vec(),
    }
}

fn valid_fragment() -> proto::ExecutionFragment {
    proto::ExecutionFragment {
        context: Some(valid_shard_context()),
        fragment_id: id128(21),
        payload: Some(payload(b"abc", 1)),
        schema_version: 19,
        capability_digest: vec![7; 32],
        applied_index: 23,
        transaction_time: 29,
        valid_at: 31,
        snapshot_immutable: true,
    }
}

fn valid_gateway_request() -> proto::GatewayRequest {
    proto::GatewayRequest {
        request: Some(valid_request_context()),
        execution_request: Some(payload(b"abc", 1)),
        fragments: vec![valid_fragment()],
    }
}

fn valid_batch() -> proto::ColumnBatch {
    proto::ColumnBatch {
        request: Some(valid_request_context()),
        fragment_id: id128(21),
        sequence: 1,
        row_count: 1,
        payload: Some(payload(b"abc", 1)),
    }
}

fn valid_transaction() -> proto::TransactionRequest {
    proto::TransactionRequest {
        context: Some(valid_shard_context()),
        transaction_id: id128(31),
        operation: 1,
        idempotency_key: id128(32),
        payload: Some(payload(b"abc", 1)),
    }
}

fn valid_raft() -> proto::RaftEnvelope {
    proto::RaftEnvelope {
        context: Some(valid_shard_context()),
        from_replica_id: 1,
        to_replica_id: 2,
        term: 3,
        committed_index: 4,
        kind: 1,
        payload: Some(payload(b"abc", 1)),
    }
}

fn valid_snapshot() -> proto::LogicalReplicaSnapshot {
    proto::LogicalReplicaSnapshot {
        context: Some(valid_shard_context()),
        snapshot_id: id128(41),
        snapshot_version: 1,
        last_included_term: 3,
        last_included_index: 4,
        chunk_index: 0,
        chunk_count: 1,
        payload: Some(payload(b"abc", 1)),
    }
}

fn valid_observation() -> proto::ControlObservation {
    proto::ControlObservation {
        request: Some(valid_request_context()),
        node_id: id128(51),
        observation_version: 1,
        observed_at_unix_ms: 1_900_000_000_000,
        payload: Some(payload(b"abc", 1)),
    }
}

fn valid_status() -> proto::TypedStatus {
    proto::TypedStatus {
        request: Some(valid_request_context()),
        code: 1,
        retry: 1,
        message: "ok".into(),
        idempotency_key: id128(61),
        details: Some(payload(b"abc", 1)),
    }
}

fn id128(value: u128) -> Vec<u8> {
    value.to_be_bytes().to_vec()
}
