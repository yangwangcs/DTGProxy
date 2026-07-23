use std::collections::VecDeque;
use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::{Context, Poll, Wake, Waker};

use adapter_memory::MemoryAdapter;
use adapter_sidecar::{
    HealthStatus, LoopbackTransport, RemoteError, Response, SidecarAdapter, SidecarClientError,
    SidecarTransport, SidecarTransportFuture,
};
use storage_api::{
    AdapterError, ApplyReceipt, CommittedMutationBatch, KeySpan, KeyValue, Keyspace, LogicalKey,
    Mutation, StorageAdapter,
};

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

fn key(value: &[u8]) -> LogicalKey {
    LogicalKey::in_keyspace(Keyspace::Current, value.to_vec())
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
