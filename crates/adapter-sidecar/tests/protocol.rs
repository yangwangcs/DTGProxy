use std::io::Cursor;

use adapter_sidecar::{
    FrameKind, HealthStatus, ProtocolError, RemoteError, Request, Response, decode_frame,
    encode_frame, read_frame, write_frame,
};
use storage_api::{
    ADAPTER_SPI_VERSION, AdapterCapabilities, AdapterDescriptorV1, ApplyReceipt, BackendFamily,
    CommittedMutationBatch, Durability, KeySpan, KeyValue, Keyspace, LogicalKey, Mutation,
    SnapshotCapability,
};

fn key(keyspace: Keyspace, value: &[u8]) -> LogicalKey {
    LogicalKey::in_keyspace(keyspace, value.to_vec())
}

fn descriptor() -> AdapterDescriptorV1 {
    AdapterDescriptorV1::new(
        "postgresql",
        "18.4",
        BackendFamily::Sql,
        AdapterCapabilities {
            local_atomic_batch: true,
            idempotent_apply: true,
            consistent_multi_get: true,
            ordered_scan: true,
            durable_applied_index: true,
            durability: Durability::Synchronous,
            snapshot: SnapshotCapability::LogicalExport,
            logical_export: true,
            logical_restore: true,
            predicate_pushdown: true,
            adjacency_pushdown: false,
            change_feed: true,
        },
    )
}

#[test]
fn every_request_round_trips_through_a_versioned_checksummed_frame() {
    let requests = vec![
        Request::Describe,
        Request::Apply(CommittedMutationBatch {
            shard_id: 7,
            log_index: 9,
            txn_id: u128::MAX - 1,
            mutations: vec![
                Mutation::put(0, key(Keyspace::Current, b"k1"), b"value".to_vec()),
                Mutation::delete(1, key(Keyspace::History, b"k2")),
            ],
        }),
        Request::MultiGet(vec![
            key(Keyspace::Current, b"a"),
            key(Keyspace::History, b"b"),
        ]),
        Request::Scan(
            KeySpan::prefix_from(Keyspace::AdjOut, b"vertex/".to_vec(), b"vertex/7".to_vec())
                .unwrap()
                .with_limit(99)
                .unwrap(),
        ),
        Request::AppliedLogIndex,
        Request::Health,
    ];
    for (ordinal, request) in requests.into_iter().enumerate() {
        let request_id = u128::try_from(ordinal).unwrap().saturating_add(1);
        let bytes = encode_frame(request_id, &request).unwrap();
        let decoded = decode_frame::<Request>(&bytes).unwrap();
        assert_eq!(decoded.kind(), FrameKind::Request);
        assert_eq!(decoded.request_id(), request_id);
        assert_eq!(decoded.message(), &request);
    }
}

#[test]
fn every_response_round_trips_without_losing_empty_and_missing_values() {
    let responses = vec![
        Response::Descriptor(descriptor()),
        Response::Apply(ApplyReceipt {
            applied_log_index: 8,
            duplicate: true,
        }),
        Response::MultiGet(vec![Some(Vec::new()), None, Some(b"v".to_vec())]),
        Response::Scan(vec![KeyValue::new(
            key(Keyspace::TemporalIndex, b"index"),
            b"row".to_vec(),
        )]),
        Response::AppliedLogIndex(44),
        Response::Health(HealthStatus {
            ready: true,
            detail: "ready".to_owned(),
        }),
        Response::Error(RemoteError {
            code: 503,
            message: "retry later".to_owned(),
            retryable: true,
        }),
    ];
    for (ordinal, response) in responses.into_iter().enumerate() {
        let request_id = u128::try_from(ordinal).unwrap().saturating_add(100);
        let bytes = encode_frame(request_id, &response).unwrap();
        let decoded = decode_frame::<Response>(&bytes).unwrap();
        assert_eq!(decoded.kind(), FrameKind::Response);
        assert_eq!(decoded.request_id(), request_id);
        assert_eq!(decoded.message(), &response);
    }
}

#[test]
fn stream_helpers_preserve_frame_boundaries() {
    let request = Request::MultiGet(vec![key(Keyspace::Identity, b"id")]);
    let mut stream = Vec::new();
    write_frame(&mut stream, 77, &request).unwrap();
    let decoded = read_frame::<_, Request>(&mut Cursor::new(stream)).unwrap();
    assert_eq!(decoded.request_id(), 77);
    assert_eq!(decoded.message(), &request);
}

#[test]
fn corruption_wrong_version_trailing_bytes_and_oversized_lengths_fail_closed() {
    let request = Request::Describe;
    let bytes = encode_frame(1, &request).unwrap();

    let mut corrupted = bytes.clone();
    corrupted[30] ^= 0x80;
    assert!(matches!(
        decode_frame::<Request>(&corrupted),
        Err(ProtocolError::ChecksumMismatch)
    ));

    let mut wrong_version = bytes.clone();
    wrong_version[4..6].copy_from_slice(&(ADAPTER_SPI_VERSION + 1).to_be_bytes());
    assert!(matches!(
        decode_frame::<Request>(&wrong_version),
        Err(ProtocolError::UnsupportedWireVersion { .. })
    ));

    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(matches!(
        decode_frame::<Request>(&trailing),
        Err(ProtocolError::FrameLengthMismatch { .. })
    ));

    let mut oversized = bytes;
    oversized[24..28].copy_from_slice(&u32::MAX.to_be_bytes());
    assert!(matches!(
        decode_frame::<Request>(&oversized),
        Err(ProtocolError::PayloadTooLarge { .. })
    ));

    let mut noncanonical = encode_frame(2, &request).unwrap();
    let checksum_offset = noncanonical.len() - 4;
    noncanonical.splice(checksum_offset..checksum_offset, [0xf8, 0x07, 0x01]);
    let payload_length = u32::try_from(noncanonical.len() - 28 - 4).unwrap();
    noncanonical[24..28].copy_from_slice(&payload_length.to_be_bytes());
    let checksum_offset = noncanonical.len() - 4;
    let checksum = crc32fast::hash(&noncanonical[..checksum_offset]);
    noncanonical[checksum_offset..].copy_from_slice(&checksum.to_be_bytes());
    assert!(matches!(
        decode_frame::<Request>(&noncanonical),
        Err(ProtocolError::NonCanonicalPayload)
    ));
}
