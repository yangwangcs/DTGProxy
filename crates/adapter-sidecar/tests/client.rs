use std::collections::VecDeque;
use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::{Context, Poll, Wake, Waker};

use adapter_memory::MemoryAdapter;
use adapter_sidecar::{
    FeatureSet, HealthStatus, HelloResponse, LoopbackTransport, MAX_FRAME_PAYLOAD_BYTES,
    RemoteError, Response, SidecarAdapter, SidecarClientError, SidecarService, SidecarTransport,
    SidecarTransportFuture,
};
use storage_api::{
    AdapterError, ApplyReceipt, CandidateScanRequest, CanonicalBatchScanRequest,
    CanonicalScanRequest, CommittedMutationBatch, ComparisonOperator, KeySpan, KeyValue, Keyspace,
    LogicalKey, Mutation, PropertyConstraint, PropertyId, PushdownGuarantee, QueryPageBounds,
    StorageAdapter,
};
use temporal_types::{GraphValue, ValidTime};

#[test]
fn client_read_snapshot_is_reusable_and_ends_its_remote_session_on_drop() {
    let backend = Arc::new(MemoryAdapter::new());
    block_on(backend.apply_committed(CommittedMutationBatch {
        shard_id: 1,
        log_index: 1,
        txn_id: 1,
        mutations: vec![Mutation::put(0, key(b"vertex/1"), b"old".to_vec())],
    }))
    .unwrap();
    let service = Arc::new(SidecarService::new(backend, None));
    let requests = Arc::new(Mutex::new(Vec::new()));
    let adapter = block_on(SidecarAdapter::connect(ServiceTransport {
        service,
        requests: Arc::clone(&requests),
    }))
    .unwrap();
    let snapshot = block_on(adapter.begin_read_snapshot()).unwrap();
    assert_eq!(snapshot.applied_log_index(), 1);

    block_on(adapter.apply_committed(CommittedMutationBatch {
        shard_id: 1,
        log_index: 2,
        txn_id: 2,
        mutations: vec![Mutation::put(0, key(b"vertex/1"), b"new".to_vec())],
    }))
    .unwrap();
    for _ in 0..2 {
        assert_eq!(
            block_on(snapshot.multi_get(&[key(b"vertex/1")])).unwrap(),
            vec![Some(b"old".to_vec())]
        );
        assert_eq!(
            block_on(snapshot.scan(&KeySpan::prefix(Keyspace::Current, b"vertex/".to_vec())))
                .unwrap()[0]
                .value(),
            b"old"
        );
    }
    drop(snapshot);

    assert!(matches!(
        requests.lock().unwrap().last(),
        Some(adapter_sidecar::Request::EndReadView { .. })
    ));
}

#[test]
fn stateless_sidecar_rejects_query_read_snapshots() {
    let backend: Arc<dyn StorageAdapter> = Arc::new(MemoryAdapter::new());
    let adapter = block_on(SidecarAdapter::connect(LoopbackTransport::new(backend))).unwrap();
    let error = match block_on(adapter.begin_read_snapshot()) {
        Ok(_) => panic!("stateless Sidecar claimed a reusable read view"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        AdapterError::UnsupportedOperation {
            operation: "query read snapshot"
        }
    ));
}

#[test]
fn client_snapshot_preserves_canonical_page_index_bounds_and_continuation() {
    let backend = Arc::new(MemoryAdapter::new());
    block_on(backend.apply_committed(CommittedMutationBatch {
        shard_id: 1,
        log_index: 1,
        txn_id: 1,
        mutations: vec![
            Mutation::put(0, key(b"vertex/1"), b"one".to_vec()),
            Mutation::put(1, key(b"vertex/2"), b"two".to_vec()),
        ],
    }))
    .unwrap();
    let adapter = block_on(SidecarAdapter::connect(ServiceTransport {
        service: Arc::new(SidecarService::new(backend, None)),
        requests: Arc::new(Mutex::new(Vec::new())),
    }))
    .unwrap();
    let snapshot = block_on(adapter.begin_read_snapshot()).unwrap();
    let request = canonical_request(1, 64);

    block_on(adapter.apply_committed(CommittedMutationBatch {
        shard_id: 1,
        log_index: 2,
        txn_id: 2,
        mutations: vec![Mutation::put(0, key(b"vertex/1"), b"new".to_vec())],
    }))
    .unwrap();

    let page = block_on(snapshot.scan_canonical(&request)).unwrap();
    assert_eq!(page.applied_log_index(), 1);
    assert_eq!(page.entries().len(), 1);
    assert_eq!(page.entries()[0].key(), &key(b"vertex/1"));
    assert_eq!(page.entries()[0].value(), b"one");
    assert_eq!(page.next_start(), Some(&key(b"vertex/2")));

    let continuation = CanonicalScanRequest::new(
        KeySpan::prefix_from(
            Keyspace::Current,
            b"vertex/".to_vec(),
            page.next_start().unwrap().as_bytes().to_vec(),
        )
        .unwrap(),
        QueryPageBounds::new(1, 64).unwrap(),
    )
    .unwrap();
    let next = block_on(snapshot.scan_canonical(&continuation)).unwrap();
    assert_eq!(next.applied_log_index(), 1);
    assert_eq!(next.entries()[0].key(), &key(b"vertex/2"));
    assert_eq!(next.next_start(), None);
}

#[test]
fn client_snapshot_sends_one_ordered_batch_exchange() {
    let backend = Arc::new(MemoryAdapter::new());
    block_on(backend.apply_committed(CommittedMutationBatch {
        shard_id: 1,
        log_index: 1,
        txn_id: 1,
        mutations: vec![
            Mutation::put(0, key(b"a/1"), b"one".to_vec()),
            Mutation::put(1, key(b"b/1"), b"two".to_vec()),
        ],
    }))
    .unwrap();
    let requests = Arc::new(Mutex::new(Vec::new()));
    let adapter = block_on(SidecarAdapter::connect(ServiceTransport {
        service: Arc::new(SidecarService::new(backend, None)),
        requests: Arc::clone(&requests),
    }))
    .unwrap();
    let snapshot = block_on(adapter.begin_read_snapshot()).unwrap();
    let request = CanonicalBatchScanRequest::new(
        [b"b/".as_slice(), b"a/".as_slice()]
            .into_iter()
            .map(|prefix| {
                CanonicalScanRequest::new(
                    KeySpan::prefix(Keyspace::Current, prefix.to_vec()),
                    QueryPageBounds::new(1, 64).unwrap(),
                )
                .unwrap()
            })
            .collect(),
        128,
    )
    .unwrap();
    requests.lock().unwrap().clear();

    let page = block_on(snapshot.scan_canonical_batch(&request)).unwrap();

    assert_eq!(page.pages()[0].entries()[0].key(), &key(b"b/1"));
    assert_eq!(page.pages()[1].entries()[0].key(), &key(b"a/1"));
    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert!(matches!(
        &requests[0],
        adapter_sidecar::Request::ReadViewCanonicalBatchScan { request: sent, .. }
            if sent == &request
    ));
}

#[test]
fn client_snapshot_rejects_batch_without_negotiated_feature() {
    let descriptor = MemoryAdapter::new().descriptor();
    let transport = ScriptedTransport::new(vec![
        hello_response(
            FeatureSet::BASE_ADAPTER_V1
                .union(FeatureSet::READ_VIEW_SESSION_V1)
                .union(FeatureSet::CANONICAL_SCAN_READ_VIEW_V1),
        ),
        Response::Descriptor(descriptor),
        Response::Health(HealthStatus {
            ready: true,
            detail: "ready".to_owned(),
        }),
        Response::AppliedLogIndex(4),
        Response::ReadViewStarted {
            session_id: 7,
            applied_log_index: 4,
        },
        Response::ReadViewEnded { session_id: 7 },
    ]);
    let adapter = block_on(SidecarAdapter::connect(transport)).unwrap();
    let snapshot = block_on(adapter.begin_read_snapshot()).unwrap();
    let request = CanonicalBatchScanRequest::new(vec![canonical_request(1, 64)], 64).unwrap();

    assert!(matches!(
        block_on(snapshot.scan_canonical_batch(&request)),
        Err(AdapterError::UnsupportedOperation {
            operation: "snapshot canonical batch scan"
        })
    ));
}

#[test]
fn client_snapshot_rejects_canonical_page_index_drift() {
    let descriptor = MemoryAdapter::new().descriptor();
    let transport = ScriptedTransport::new(vec![
        hello_response(
            FeatureSet::BASE_ADAPTER_V1
                .union(FeatureSet::READ_VIEW_SESSION_V1)
                .union(FeatureSet::CANONICAL_SCAN_READ_VIEW_V1),
        ),
        Response::Descriptor(descriptor),
        Response::Health(HealthStatus {
            ready: true,
            detail: "ready".to_owned(),
        }),
        Response::AppliedLogIndex(4),
        Response::ReadViewStarted {
            session_id: 7,
            applied_log_index: 4,
        },
        Response::ReadViewCanonicalScan {
            session_id: 7,
            applied_log_index: 5,
            entries: Vec::new(),
            next_start: None,
        },
        Response::ReadViewEnded { session_id: 7 },
    ]);
    let adapter = block_on(SidecarAdapter::connect(transport)).unwrap();
    let snapshot = block_on(adapter.begin_read_snapshot()).unwrap();

    let error = block_on(snapshot.scan_canonical(&canonical_request(1, 64))).unwrap_err();
    assert!(error.to_string().contains("differs from fixed index 4"));
}

#[test]
fn client_snapshot_fails_closed_without_canonical_read_view_feature() {
    let descriptor = MemoryAdapter::new().descriptor();
    let transport = ScriptedTransport::new(vec![
        hello_response(FeatureSet::BASE_ADAPTER_V1.union(FeatureSet::READ_VIEW_SESSION_V1)),
        Response::Descriptor(descriptor),
        Response::Health(HealthStatus {
            ready: true,
            detail: "ready".to_owned(),
        }),
        Response::AppliedLogIndex(4),
        Response::ReadViewStarted {
            session_id: 7,
            applied_log_index: 4,
        },
        Response::ReadViewEnded { session_id: 7 },
    ]);
    let adapter = block_on(SidecarAdapter::connect(transport)).unwrap();
    let snapshot = block_on(adapter.begin_read_snapshot()).unwrap();

    assert!(matches!(
        block_on(snapshot.scan_canonical(&canonical_request(1, 64))),
        Err(AdapterError::UnsupportedOperation {
            operation: "snapshot canonical scan"
        })
    ));
}

#[test]
fn client_candidate_scan_uses_fixed_snapshot_and_never_advertises_exact() {
    let backend = Arc::new(MemoryAdapter::new());
    block_on(backend.apply_committed(CommittedMutationBatch {
        shard_id: 1,
        log_index: 1,
        txn_id: 1,
        mutations: vec![Mutation::put(0, key(b"vertex/1"), b"old".to_vec())],
    }))
    .unwrap();
    let adapter = block_on(SidecarAdapter::connect(ServiceTransport {
        service: Arc::new(SidecarService::new(backend, None)),
        requests: Arc::new(Mutex::new(Vec::new())),
    }))
    .unwrap();
    assert_eq!(
        adapter.query_primitive_capabilities().candidate_scan(),
        PushdownGuarantee::Candidate
    );
    let snapshot = block_on(adapter.begin_read_snapshot()).unwrap();

    block_on(adapter.apply_committed(CommittedMutationBatch {
        shard_id: 1,
        log_index: 2,
        txn_id: 2,
        mutations: vec![Mutation::put(0, key(b"vertex/1"), b"new".to_vec())],
    }))
    .unwrap();

    let page = block_on(snapshot.scan_candidates(&candidate_request())).unwrap();
    assert_eq!(page.applied_log_index(), 1);
    assert_eq!(page.guarantee(), PushdownGuarantee::Candidate);
    assert_eq!(page.entries()[0].value(), b"old");
}

#[test]
fn client_candidate_scan_fails_closed_without_feature_or_descriptor_capability() {
    for (features, mut descriptor) in [
        (
            FeatureSet::READ_VIEW_SESSION_V1,
            MemoryAdapter::new().descriptor(),
        ),
        (
            FeatureSet::READ_VIEW_SESSION_V1.union(FeatureSet::CANDIDATE_SCAN_READ_VIEW_V1),
            MemoryAdapter::new().descriptor(),
        ),
    ] {
        if features.contains(FeatureSet::CANDIDATE_SCAN_READ_VIEW_V1) {
            let capabilities = descriptor.capabilities();
            descriptor = storage_api::AdapterDescriptorV1::new(
                descriptor.implementation(),
                descriptor.implementation_version(),
                descriptor.family(),
                storage_api::AdapterCapabilities {
                    predicate_pushdown: false,
                    ..capabilities
                },
            );
        }
        let transport = ScriptedTransport::new(vec![
            hello_response(FeatureSet::BASE_ADAPTER_V1.union(features)),
            Response::Descriptor(descriptor),
            Response::Health(HealthStatus {
                ready: true,
                detail: "ready".to_owned(),
            }),
            Response::AppliedLogIndex(4),
            Response::ReadViewStarted {
                session_id: 7,
                applied_log_index: 4,
            },
            Response::ReadViewEnded { session_id: 7 },
        ]);
        let adapter = block_on(SidecarAdapter::connect(transport)).unwrap();
        assert_eq!(
            adapter.query_primitive_capabilities().candidate_scan(),
            PushdownGuarantee::Unsupported
        );
        let snapshot = block_on(adapter.begin_read_snapshot()).unwrap();
        assert!(matches!(
            block_on(snapshot.scan_candidates(&candidate_request())),
            Err(AdapterError::UnsupportedOperation {
                operation: "snapshot candidate scan"
            })
        ));
    }
}

#[test]
fn client_candidate_scan_downgrades_exact_wire_pages() {
    let mut descriptor = MemoryAdapter::new().descriptor();
    let capabilities = descriptor.capabilities();
    descriptor = storage_api::AdapterDescriptorV1::new(
        descriptor.implementation(),
        descriptor.implementation_version(),
        descriptor.family(),
        storage_api::AdapterCapabilities {
            predicate_pushdown: true,
            ..capabilities
        },
    );
    let transport = ScriptedTransport::new(vec![
        hello_response(
            FeatureSet::BASE_ADAPTER_V1
                .union(FeatureSet::READ_VIEW_SESSION_V1)
                .union(FeatureSet::CANDIDATE_SCAN_READ_VIEW_V1),
        ),
        Response::Descriptor(descriptor),
        Response::Health(HealthStatus {
            ready: true,
            detail: "ready".to_owned(),
        }),
        Response::AppliedLogIndex(4),
        Response::ReadViewStarted {
            session_id: 7,
            applied_log_index: 4,
        },
        Response::ReadViewCandidateScan {
            session_id: 7,
            applied_log_index: 4,
            guarantee: PushdownGuarantee::Exact,
            entries: Vec::new(),
            next_start: None,
        },
        Response::ReadViewEnded { session_id: 7 },
    ]);
    let adapter = block_on(SidecarAdapter::connect(transport)).unwrap();
    let snapshot = block_on(adapter.begin_read_snapshot()).unwrap();
    let page = block_on(snapshot.scan_candidates(&candidate_request())).unwrap();

    assert_eq!(page.guarantee(), PushdownGuarantee::Candidate);
}

#[test]
fn sidecar_client_preserves_the_storage_adapter_contract_and_index_cache() {
    let backend: Arc<dyn StorageAdapter> = Arc::new(MemoryAdapter::new());
    let transport = LoopbackTransport::new(backend);
    let adapter = block_on(SidecarAdapter::connect(transport)).unwrap();
    assert_eq!(adapter.descriptor().implementation(), "memory");
    assert_eq!(adapter.applied_log_index().unwrap(), 0);

    let batch = CommittedMutationBatch {
        shard_id: 7,
        log_index: 1,
        txn_id: 101,
        mutations: vec![Mutation::put(0, key(b"vertex/1"), b"payload".to_vec())],
    };
    let first = block_on(adapter.apply_committed(batch.clone())).unwrap();
    let replay = block_on(adapter.apply_committed(batch)).unwrap();
    assert!(!first.duplicate);
    assert!(replay.duplicate);
    assert_eq!(adapter.applied_log_index().unwrap(), 1);
    assert_eq!(
        block_on(adapter.multi_get(&[key(b"vertex/1"), key(b"missing")])).unwrap(),
        vec![Some(b"payload".to_vec()), None]
    );
    let rows =
        block_on(adapter.scan(&KeySpan::prefix(Keyspace::Current, b"vertex/".to_vec()))).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].key(), &key(b"vertex/1"));
}

#[test]
fn client_rejects_invalid_counts_indices_and_scan_rows() {
    let descriptor = MemoryAdapter::new().descriptor();
    let transport = ScriptedTransport::new(vec![
        hello_response(FeatureSet::BASE_ADAPTER_V1),
        Response::Descriptor(descriptor),
        Response::Health(HealthStatus {
            ready: true,
            detail: "ready".to_owned(),
        }),
        Response::AppliedLogIndex(4),
        Response::MultiGet(vec![None]),
        Response::Apply(ApplyReceipt {
            applied_log_index: 4,
            duplicate: false,
        }),
        Response::Scan(vec![KeyValue::new(key(b"outside/1"), b"value".to_vec())]),
    ]);
    let adapter = block_on(SidecarAdapter::connect(transport)).unwrap();

    let count_error = block_on(adapter.multi_get(&[key(b"a"), key(b"b")])).unwrap_err();
    assert!(count_error.to_string().contains("1 values for 2 keys"));
    let index_error = block_on(adapter.apply_committed(CommittedMutationBatch {
        shard_id: 1,
        log_index: 5,
        txn_id: 1,
        mutations: vec![],
    }))
    .unwrap_err();
    assert!(index_error.to_string().contains("below required index 5"));
    let scan_error =
        block_on(adapter.scan(&KeySpan::prefix(Keyspace::Current, b"inside/".to_vec())))
            .unwrap_err();
    assert!(
        scan_error
            .to_string()
            .contains("outside the requested key span")
    );
}

#[test]
fn client_preserves_exact_remote_scan_byte_limit() {
    let descriptor = MemoryAdapter::new().descriptor();
    let transport = ScriptedTransport::new(vec![
        hello_response(FeatureSet::BASE_ADAPTER_V1),
        Response::Descriptor(descriptor),
        Response::Health(HealthStatus {
            ready: true,
            detail: "ready".to_owned(),
        }),
        Response::AppliedLogIndex(0),
        Response::Error(RemoteError {
            code: 9,
            message: "scan requires 13 bytes".into(),
            retryable: false,
            scan_limit: Some(8),
            scan_required: Some(13),
            scan_response_limit: None,
            scan_response_required: None,
        }),
    ]);
    let adapter = block_on(SidecarAdapter::connect(transport)).unwrap();

    assert_eq!(
        block_on(
            adapter.scan(
                &KeySpan::prefix(Keyspace::Current, Vec::new())
                    .with_max_bytes(8)
                    .unwrap()
            )
        ),
        Err(AdapterError::ScanByteLimit {
            limit: 8,
            required: 13,
        })
    );
}

#[test]
fn client_preserves_exact_remote_scan_response_body_limit() {
    let descriptor = MemoryAdapter::new().descriptor();
    let transport = ScriptedTransport::new(vec![
        hello_response(FeatureSet::BASE_ADAPTER_V1),
        Response::Descriptor(descriptor),
        Response::Health(HealthStatus {
            ready: true,
            detail: "ready".to_owned(),
        }),
        Response::AppliedLogIndex(0),
        Response::Error(RemoteError {
            code: 10,
            message: "scan response body requires 65 wire bytes".into(),
            retryable: false,
            scan_limit: None,
            scan_required: None,
            scan_response_limit: Some(64),
            scan_response_required: Some(65),
        }),
    ]);
    let adapter = block_on(SidecarAdapter::connect(transport)).unwrap();

    assert_eq!(
        block_on(adapter.scan(&KeySpan::prefix(Keyspace::Current, Vec::new()))),
        Err(AdapterError::ScanResponseByteLimit {
            limit: 64,
            required: 65,
        })
    );
}

struct ScriptedTransport {
    responses: Mutex<VecDeque<Response>>,
}

impl ScriptedTransport {
    fn new(responses: Vec<Response>) -> Self {
        Self {
            responses: Mutex::new(responses.into()),
        }
    }
}

impl SidecarTransport for ScriptedTransport {
    fn call<'a>(&'a self, _request: adapter_sidecar::Request) -> SidecarTransportFuture<'a> {
        Box::pin(async move {
            self.responses
                .lock()
                .unwrap()
                .pop_front()
                .ok_or_else(|| SidecarClientError::Transport("script exhausted".to_owned()))
        })
    }
}

struct ServiceTransport {
    service: Arc<SidecarService>,
    requests: Arc<Mutex<Vec<adapter_sidecar::Request>>>,
}

impl SidecarTransport for ServiceTransport {
    fn call<'a>(&'a self, request: adapter_sidecar::Request) -> SidecarTransportFuture<'a> {
        Box::pin(async move {
            self.requests.lock().unwrap().push(request.clone());
            Ok(self.service.dispatch(request).await)
        })
    }
}

fn key(value: &[u8]) -> LogicalKey {
    LogicalKey::in_keyspace(Keyspace::Current, value.to_vec())
}

fn canonical_request(max_items: usize, max_bytes: u64) -> CanonicalScanRequest {
    CanonicalScanRequest::new(
        KeySpan::prefix(Keyspace::Current, b"vertex/".to_vec()),
        QueryPageBounds::new(max_items, max_bytes).unwrap(),
    )
    .unwrap()
}

fn candidate_request() -> CandidateScanRequest {
    CandidateScanRequest::new(
        KeySpan::prefix(Keyspace::Current, b"vertex/".to_vec()),
        ValidTime::from_micros(77),
        vec![PropertyConstraint::new(
            PropertyId::new(7),
            ComparisonOperator::Equal,
            GraphValue::Boolean(true),
        )],
        QueryPageBounds::new(1, 64).unwrap(),
    )
    .unwrap()
}

fn hello_response(features: FeatureSet) -> Response {
    Response::Hello(HelloResponse {
        wire_version: 1,
        negotiated_features: features,
        max_payload_bytes: MAX_FRAME_PAYLOAD_BYTES as u32,
        max_chunk_bytes: storage_api::MAX_LOGICAL_SNAPSHOT_CHUNK_BYTES as u32,
        max_chunk_entries: storage_api::MAX_LOGICAL_SNAPSHOT_CHUNK_ENTRIES as u32,
        snapshot_format_version: storage_api::LOGICAL_SNAPSHOT_FORMAT_VERSION,
    })
}

struct NoopWake;

impl Wake for NoopWake {
    fn wake(self: Arc<Self>) {}
}

fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::from(Arc::new(NoopWake));
    let mut context = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}
