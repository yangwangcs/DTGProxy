#![forbid(unsafe_code)]

use dtg_storage_remote_protocol::{
    CONTRACT_MAJOR, CONTRACT_MINOR, MAX_AUTH_CONTEXT_BYTES, MAX_MESSAGE_BYTES, MAX_MESSAGE_ITEMS,
    PROTOCOL_MAJOR, PROTOCOL_MINOR, ProtocolError, bounded_payload, proto, validate_context,
    validate_payload,
};

fn context() -> proto::RequestContext {
    proto::RequestContext {
        protocol_major: PROTOCOL_MAJOR,
        protocol_minor: PROTOCOL_MINOR,
        contract_major: CONTRACT_MAJOR,
        contract_minor: CONTRACT_MINOR,
        request_id: vec![1; 16],
        deadline_unix_ms: 2,
        binding: Some(proto::Binding::default()),
        auth_context: Vec::new(),
        max_response_bytes: MAX_MESSAGE_BYTES as u64,
        max_response_items: MAX_MESSAGE_ITEMS as u32,
    }
}

#[test]
fn payload_checksum_length_and_item_count_are_fail_closed() {
    let mut payload = bounded_payload(vec![1, 2, 3], 1).unwrap();
    payload.body[0] ^= 1;
    assert!(matches!(
        validate_payload(&payload),
        Err(ProtocolError::InvalidPayload(_))
    ));

    let mut payload = bounded_payload(vec![1, 2, 3], 1).unwrap();
    payload.declared_len += 1;
    assert!(matches!(
        validate_payload(&payload),
        Err(ProtocolError::InvalidPayload(_))
    ));
}

#[test]
fn payload_and_authentication_bounds_are_enforced() {
    assert!(bounded_payload(vec![0; MAX_MESSAGE_BYTES + 1], 1).is_err());
    assert!(bounded_payload(Vec::new(), MAX_MESSAGE_ITEMS + 1).is_err());

    let mut request = context();
    request.auth_context = vec![0; MAX_AUTH_CONTEXT_BYTES + 1];
    assert_eq!(
        validate_context(Some(&request), 1),
        Err(ProtocolError::AuthContextTooLarge)
    );
}

#[test]
fn request_identity_versions_deadlines_and_response_budgets_are_validated() {
    let mut request = context();
    request.protocol_major += 1;
    assert!(matches!(
        validate_context(Some(&request), 1),
        Err(ProtocolError::ProtocolMajor { .. })
    ));

    let mut request = context();
    request.request_id.fill(0);
    assert_eq!(
        validate_context(Some(&request), 1),
        Err(ProtocolError::InvalidRequestId)
    );

    let mut request = context();
    request.deadline_unix_ms = 1;
    assert_eq!(
        validate_context(Some(&request), 1),
        Err(ProtocolError::InvalidDeadline)
    );

    let mut request = context();
    request.max_response_items = 0;
    assert_eq!(
        validate_context(Some(&request), 1),
        Err(ProtocolError::InvalidResponseBudget)
    );
}
