use std::io::{self, Read};

use adapter_sidecar::{
    BeginExportRequest, BeginRestoreRequest, ExportStarted, FeatureSet, HelloRequest,
    HelloResponse, MAX_FRAME_PAYLOAD_BYTES, PublicAdapterOpenRequest, RemoteErrorCode, Request,
    Response, RestoreComplete, RestoreStarted, decode_frame, encode_frame, read_frame,
};
use prost::Message;
use storage_api::{
    AdapterCapabilities, AdapterDescriptorV1, BackendFamily, Durability, KeyValue, Keyspace,
    LogicalKey, LogicalSnapshotAccumulator, LogicalSnapshotChunkV1, LogicalSnapshotExportRequest,
    LogicalSnapshotHeaderV1, SnapshotCapability,
};

fn sample_chunk(snapshot_id: u128, ordinal: u64) -> LogicalSnapshotChunkV1 {
    LogicalSnapshotChunkV1::new(
        snapshot_id,
        ordinal,
        vec![KeyValue::new(
            LogicalKey::in_keyspace(Keyspace::Current, format!("key-{ordinal}").into_bytes()),
            format!("value-{ordinal}").into_bytes(),
        )],
    )
    .unwrap()
}

fn sample_manifest(
    snapshot_id: u128,
    applied_log_index: u64,
) -> storage_api::LogicalSnapshotManifestV1 {
    let header = LogicalSnapshotHeaderV1::new(snapshot_id, applied_log_index);
    let chunk = sample_chunk(snapshot_id, 0);
    let mut accumulator = LogicalSnapshotAccumulator::new(header);
    accumulator.observe(&chunk).unwrap();
    accumulator.complete()
}

fn descriptor() -> AdapterDescriptorV1 {
    AdapterDescriptorV1::new(
        "snapshot-test",
        "1.0.0",
        BackendFamily::Test,
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
            predicate_pushdown: false,
            adjacency_pushdown: false,
            change_feed: false,
        },
    )
}

#[test]
fn every_snapshot_request_round_trips_canonically() {
    let requests = vec![
        Request::Hello(HelloRequest::adapter_client()),
        Request::BeginExport(BeginExportRequest {
            limits: LogicalSnapshotExportRequest::new(4_096, 4 * 1024 * 1024).unwrap(),
            expected_applied_log_index: Some(9),
        }),
        Request::ExportNext {
            session_id: 11,
            expected_ordinal: 0,
        },
        Request::BeginRestore(BeginRestoreRequest {
            header: LogicalSnapshotHeaderV1::new(22, 9),
            target: PublicAdapterOpenRequest::new("target").with_parameter("pool_size", "8"),
        }),
        Request::RestoreChunk {
            session_id: 12,
            chunk: sample_chunk(22, 0),
        },
        Request::FinishRestore {
            session_id: 12,
            manifest: sample_manifest(22, 9),
        },
        Request::AbortSession { session_id: 12 },
    ];
    for (ordinal, request) in requests.into_iter().enumerate() {
        let encoded = encode_frame(100 + ordinal as u128, &request).unwrap();
        assert_eq!(
            decode_frame::<Request>(&encoded).unwrap().into_message(),
            request
        );
    }
}

#[test]
fn every_snapshot_response_round_trips_canonically() {
    let limits = LogicalSnapshotExportRequest::new(4_096, 4 * 1024 * 1024).unwrap();
    let header = LogicalSnapshotHeaderV1::new(22, 9);
    let responses = vec![
        Response::Hello(HelloResponse {
            wire_version: 1,
            negotiated_features: FeatureSet::ALL,
            max_payload_bytes: MAX_FRAME_PAYLOAD_BYTES as u32,
            max_chunk_bytes: 16 * 1024 * 1024,
            max_chunk_entries: 65_536,
            snapshot_format_version: 1,
        }),
        Response::ExportStarted(ExportStarted {
            session_id: 11,
            header: header.clone(),
            limits,
        }),
        Response::ExportChunk {
            session_id: 11,
            chunk: sample_chunk(22, 0),
        },
        Response::ExportComplete {
            session_id: 11,
            manifest: sample_manifest(22, 9),
        },
        Response::RestoreStarted(RestoreStarted {
            session_id: 12,
            prospective_descriptor: descriptor(),
            max_chunk_bytes: 16 * 1024 * 1024,
            max_chunk_entries: 65_536,
        }),
        Response::RestoreChunkAccepted {
            session_id: 12,
            ordinal: 0,
            digest: sample_chunk(22, 0).digest(),
        },
        Response::RestoreComplete(RestoreComplete {
            session_id: 12,
            final_descriptor: descriptor(),
            applied_log_index: 9,
        }),
        Response::SessionAborted { session_id: 12 },
    ];
    for (ordinal, response) in responses.into_iter().enumerate() {
        let encoded = encode_frame(200 + ordinal as u128, &response).unwrap();
        assert_eq!(
            decode_frame::<Response>(&encoded).unwrap().into_message(),
            response
        );
    }
}

#[test]
fn feature_bits_and_remote_error_codes_are_stable() {
    assert_eq!(FeatureSet::BASE_ADAPTER_V1.bits(), 1 << 0);
    assert_eq!(FeatureSet::LOGICAL_EXPORT_SESSION_V1.bits(), 1 << 1);
    assert_eq!(FeatureSet::LOGICAL_RESTORE_SESSION_V1.bits(), 1 << 2);
    assert_eq!(FeatureSet::RESUMABLE_ORDINAL_REPLAY_V1.bits(), 1 << 3);
    assert!(FeatureSet::from_bits(1 << 63).is_err());

    assert_eq!(
        [
            RemoteErrorCode::FeatureUnsupported as u32,
            RemoteErrorCode::NotActive as u32,
            RemoteErrorCode::ResourceExhausted as u32,
            RemoteErrorCode::SessionUnknown as u32,
            RemoteErrorCode::SessionExpired as u32,
            RemoteErrorCode::SessionKindMismatch as u32,
            RemoteErrorCode::SessionBusy as u32,
            RemoteErrorCode::OrdinalGap as u32,
            RemoteErrorCode::OrdinalRegression as u32,
            RemoteErrorCode::ChunkDigestMismatch as u32,
            RemoteErrorCode::RequestReplayMismatch as u32,
            RemoteErrorCode::TerminalReplayMismatch as u32,
            RemoteErrorCode::RestoreAlreadyInProgress as u32,
            RemoteErrorCode::ServiceFaulted as u32,
            RemoteErrorCode::TargetRequestMismatch as u32,
        ],
        [
            100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114
        ]
    );
}

struct DeclaredOversizedFrame {
    header: [u8; 28],
    position: usize,
    reads_after_header: usize,
}

impl DeclaredOversizedFrame {
    fn new() -> Self {
        let mut header = [0_u8; 28];
        header[..4].copy_from_slice(b"DTAS");
        header[4..6].copy_from_slice(&1_u16.to_be_bytes());
        header[6] = 1;
        header[24..28].copy_from_slice(
            &u32::try_from(MAX_FRAME_PAYLOAD_BYTES + 1)
                .unwrap()
                .to_be_bytes(),
        );
        Self {
            header,
            position: 0,
            reads_after_header: 0,
        }
    }
}

impl Read for DeclaredOversizedFrame {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if self.position == self.header.len() {
            self.reads_after_header += 1;
            return Err(io::Error::other("decoder read after oversized header"));
        }
        let length = buffer.len().min(self.header.len() - self.position);
        buffer[..length].copy_from_slice(&self.header[self.position..self.position + length]);
        self.position += length;
        Ok(length)
    }
}

#[test]
fn payload_limit_is_twenty_mebibytes_and_checked_before_allocation() {
    assert_eq!(MAX_FRAME_PAYLOAD_BYTES, 20 * 1024 * 1024);
    let mut input = DeclaredOversizedFrame::new();
    assert!(matches!(
        read_frame::<_, Request>(&mut input),
        Err(adapter_sidecar::ProtocolError::PayloadTooLarge { .. })
    ));
    assert_eq!(input.reads_after_header, 0);
}

fn raw_request_frame(body: raw::request_envelope::Body) -> Vec<u8> {
    let payload = raw::RequestEnvelope {
        spi_version: 1,
        body: Some(body),
    }
    .encode_to_vec();
    let mut frame = Vec::with_capacity(28 + payload.len() + 4);
    frame.extend_from_slice(b"DTAS");
    frame.extend_from_slice(&1_u16.to_be_bytes());
    frame.push(1);
    frame.push(0);
    frame.extend_from_slice(&99_u128.to_be_bytes());
    frame.extend_from_slice(&u32::try_from(payload.len()).unwrap().to_be_bytes());
    frame.extend_from_slice(&payload);
    frame.extend_from_slice(&crc32fast::hash(&frame).to_be_bytes());
    frame
}

fn raw_header(format_version: u32) -> raw::SnapshotHeader {
    raw::SnapshotHeader {
        format_version,
        snapshot_id: 22_u128.to_be_bytes().to_vec(),
        applied_log_index: 9,
    }
}

#[test]
fn fixed_width_session_ids_and_digests_fail_closed() {
    for actual in [15, 17] {
        let frame = raw_request_frame(raw::request_envelope::Body::AbortSession(
            raw::AbortSessionRequest {
                session_id: vec![0; actual],
            },
        ));
        assert!(matches!(
            decode_frame::<Request>(&frame),
            Err(adapter_sidecar::ProtocolError::InvalidSessionIdLength { actual: seen })
                if seen == actual
        ));
    }

    for actual in [15, 17] {
        let mut header = raw_header(1);
        header.snapshot_id = vec![0; actual];
        let frame = raw_request_frame(raw::request_envelope::Body::BeginRestore(
            raw::BeginRestoreRequest {
                header: Some(header),
                target: Some(raw::PublicAdapterOpenRequest {
                    instance_id: "target".to_owned(),
                    parameters: Vec::new(),
                }),
            },
        ));
        assert!(matches!(
            decode_frame::<Request>(&frame),
            Err(adapter_sidecar::ProtocolError::InvalidSnapshotIdLength { actual: seen })
                if seen == actual
        ));
    }

    for actual in [31, 33] {
        let frame = raw_request_frame(raw::request_envelope::Body::RestoreChunk(
            raw::RestoreChunkRequest {
                session_id: 12_u128.to_be_bytes().to_vec(),
                chunk: Some(raw::SnapshotChunk {
                    snapshot_id: 22_u128.to_be_bytes().to_vec(),
                    ordinal: 0,
                    entries: vec![raw::KeyValue {
                        key: Some(raw::LogicalKey {
                            keyspace: u32::from(Keyspace::Current.tag()),
                            key: b"key".to_vec(),
                        }),
                        value: b"value".to_vec(),
                    }],
                    digest: vec![0; actual],
                }),
            },
        ));
        assert!(matches!(
            decode_frame::<Request>(&frame),
            Err(adapter_sidecar::ProtocolError::InvalidDigestLength { actual: seen })
                if seen == actual
        ));
    }
}

#[test]
fn unknown_features_formats_and_chunk_limits_fail_closed() {
    let unknown_features =
        raw_request_frame(raw::request_envelope::Body::Hello(raw::HelloRequest {
            required_features: 1 << 63,
            optional_features: 0,
            max_payload_bytes: MAX_FRAME_PAYLOAD_BYTES as u32,
        }));
    assert!(matches!(
        decode_frame::<Request>(&unknown_features),
        Err(adapter_sidecar::ProtocolError::UnknownFeatureBits { bits }) if bits == 1 << 63
    ));

    let invalid_limits = raw_request_frame(raw::request_envelope::Body::BeginExport(
        raw::BeginExportRequest {
            limits: Some(raw::SnapshotLimits {
                max_entries: 0,
                max_bytes: 1,
            }),
        },
    ));
    assert!(matches!(
        decode_frame::<Request>(&invalid_limits),
        Err(adapter_sidecar::ProtocolError::InvalidChunkLimits(_))
    ));

    let invalid_format = raw_request_frame(raw::request_envelope::Body::BeginRestore(
        raw::BeginRestoreRequest {
            header: Some(raw_header(2)),
            target: Some(raw::PublicAdapterOpenRequest {
                instance_id: "target".to_owned(),
                parameters: Vec::new(),
            }),
        },
    ));
    assert!(matches!(
        decode_frame::<Request>(&invalid_format),
        Err(adapter_sidecar::ProtocolError::UnsupportedSnapshotFormat {
            expected: 1,
            actual: 2
        })
    ));
}

#[test]
fn unordered_and_duplicate_public_parameters_are_rejected_not_normalized() {
    for names in [["b", "a"], ["a", "a"]] {
        let frame = raw_request_frame(raw::request_envelope::Body::BeginRestore(
            raw::BeginRestoreRequest {
                header: Some(raw_header(1)),
                target: Some(raw::PublicAdapterOpenRequest {
                    instance_id: "target".to_owned(),
                    parameters: names
                        .into_iter()
                        .map(|name| raw::PublicParameter {
                            name: name.to_owned(),
                            value: "value".to_owned(),
                        })
                        .collect(),
                }),
            },
        ));
        assert!(matches!(
            decode_frame::<Request>(&frame),
            Err(adapter_sidecar::ProtocolError::NonCanonicalPublicParameters)
        ));
    }
}

mod raw {
    use prost::{Message, Oneof};

    #[derive(Clone, PartialEq, Message)]
    pub struct LogicalKey {
        #[prost(uint32, tag = "1")]
        pub keyspace: u32,
        #[prost(bytes = "vec", tag = "2")]
        pub key: Vec<u8>,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct KeyValue {
        #[prost(message, optional, tag = "1")]
        pub key: Option<LogicalKey>,
        #[prost(bytes = "vec", tag = "2")]
        pub value: Vec<u8>,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct HelloRequest {
        #[prost(uint64, tag = "1")]
        pub required_features: u64,
        #[prost(uint64, tag = "2")]
        pub optional_features: u64,
        #[prost(uint32, tag = "3")]
        pub max_payload_bytes: u32,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct SnapshotLimits {
        #[prost(uint32, tag = "1")]
        pub max_entries: u32,
        #[prost(uint32, tag = "2")]
        pub max_bytes: u32,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct SnapshotHeader {
        #[prost(uint32, tag = "1")]
        pub format_version: u32,
        #[prost(bytes = "vec", tag = "2")]
        pub snapshot_id: Vec<u8>,
        #[prost(uint64, tag = "3")]
        pub applied_log_index: u64,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct SnapshotChunk {
        #[prost(bytes = "vec", tag = "1")]
        pub snapshot_id: Vec<u8>,
        #[prost(uint64, tag = "2")]
        pub ordinal: u64,
        #[prost(message, repeated, tag = "3")]
        pub entries: Vec<KeyValue>,
        #[prost(bytes = "vec", tag = "4")]
        pub digest: Vec<u8>,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct PublicParameter {
        #[prost(string, tag = "1")]
        pub name: String,
        #[prost(string, tag = "2")]
        pub value: String,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct PublicAdapterOpenRequest {
        #[prost(string, tag = "1")]
        pub instance_id: String,
        #[prost(message, repeated, tag = "2")]
        pub parameters: Vec<PublicParameter>,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct BeginExportRequest {
        #[prost(message, optional, tag = "1")]
        pub limits: Option<SnapshotLimits>,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct BeginRestoreRequest {
        #[prost(message, optional, tag = "1")]
        pub header: Option<SnapshotHeader>,
        #[prost(message, optional, tag = "2")]
        pub target: Option<PublicAdapterOpenRequest>,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct RestoreChunkRequest {
        #[prost(bytes = "vec", tag = "1")]
        pub session_id: Vec<u8>,
        #[prost(message, optional, tag = "2")]
        pub chunk: Option<SnapshotChunk>,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct AbortSessionRequest {
        #[prost(bytes = "vec", tag = "1")]
        pub session_id: Vec<u8>,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct RequestEnvelope {
        #[prost(uint32, tag = "1")]
        pub spi_version: u32,
        #[prost(oneof = "request_envelope::Body", tags = "8, 9, 11, 12, 14")]
        pub body: Option<request_envelope::Body>,
    }

    pub mod request_envelope {
        use super::Oneof;
        use super::{
            AbortSessionRequest, BeginExportRequest, BeginRestoreRequest, HelloRequest,
            RestoreChunkRequest,
        };

        #[derive(Clone, PartialEq, Oneof)]
        pub enum Body {
            #[prost(message, tag = "8")]
            Hello(HelloRequest),
            #[prost(message, tag = "9")]
            BeginExport(BeginExportRequest),
            #[prost(message, tag = "11")]
            BeginRestore(BeginRestoreRequest),
            #[prost(message, tag = "12")]
            RestoreChunk(RestoreChunkRequest),
            #[prost(message, tag = "14")]
            AbortSession(AbortSessionRequest),
        }
    }
}
