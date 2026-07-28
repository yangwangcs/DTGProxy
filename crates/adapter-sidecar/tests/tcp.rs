use std::future::Future;
use std::net::{Ipv4Addr, SocketAddr, TcpListener};
use std::sync::{Arc, mpsc};
use std::task::{Context, Poll, Wake, Waker};
use std::thread;
use std::time::Duration;

use adapter_memory::MemoryAdapter;
use adapter_sidecar::{
    Request, SidecarAdapter, SidecarService, SidecarTransport, TcpSidecarConfig,
    TcpSidecarServerConfig, TcpSidecarTransport, dispatch_request, read_frame, serve_connection,
    spawn_stateful_tcp_sidecar_server, spawn_tcp_sidecar_server, write_frame,
};
use storage_api::{
    CandidateScanRequest, CommittedMutationBatch, KeySpan, Keyspace, LogicalKey, Mutation,
    PushdownGuarantee, QueryPageBounds, StorageAdapter,
};
use temporal_types::ValidTime;

#[test]
fn tcp_read_view_reuses_one_remote_snapshot_across_multiple_reads() {
    let backend = Arc::new(MemoryAdapter::new());
    block_on(backend.apply_committed(CommittedMutationBatch {
        shard_id: 1,
        log_index: 1,
        txn_id: 1,
        mutations: vec![Mutation::put(0, key(b"v/1"), b"old".to_vec())],
    }))
    .unwrap();
    let service = Arc::new(SidecarService::new(backend, None));
    let server = spawn_stateful_tcp_sidecar_server(
        TcpSidecarServerConfig::new(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))),
        service,
    )
    .unwrap();
    let transport = TcpSidecarTransport::connect(
        TcpSidecarConfig::new(server.local_addr())
            .with_pool_size(2)
            .unwrap(),
    )
    .unwrap();
    let adapter = block_on(SidecarAdapter::connect(transport)).unwrap();
    let snapshot = block_on(adapter.begin_read_snapshot()).unwrap();

    block_on(adapter.apply_committed(CommittedMutationBatch {
        shard_id: 1,
        log_index: 2,
        txn_id: 2,
        mutations: vec![Mutation::put(0, key(b"v/1"), b"new".to_vec())],
    }))
    .unwrap();
    for _ in 0..2 {
        assert_eq!(
            block_on(snapshot.multi_get(&[key(b"v/1")])).unwrap(),
            vec![Some(b"old".to_vec())]
        );
        assert_eq!(
            block_on(snapshot.scan(&KeySpan::prefix(Keyspace::Current, b"v/".to_vec()))).unwrap()
                [0]
            .value(),
            b"old"
        );
    }

    drop(snapshot);
    drop(adapter);
    server.shutdown().unwrap();
}

#[test]
fn tcp_sidecar_forwards_typed_candidate_scan_on_the_backend_snapshot() {
    let backend = Arc::new(MemoryAdapter::new());
    block_on(backend.apply_committed(CommittedMutationBatch {
        shard_id: 1,
        log_index: 1,
        txn_id: 1,
        mutations: vec![
            Mutation::put(0, key(b"v/1"), b"old-one".to_vec()),
            Mutation::put(1, key(b"v/2"), b"old-two".to_vec()),
        ],
    }))
    .unwrap();
    let service = Arc::new(SidecarService::new(backend, None));
    let server = spawn_stateful_tcp_sidecar_server(
        TcpSidecarServerConfig::new(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))),
        service,
    )
    .unwrap();
    let transport = TcpSidecarTransport::connect(
        TcpSidecarConfig::new(server.local_addr())
            .with_pool_size(1)
            .unwrap(),
    )
    .unwrap();
    let adapter = block_on(SidecarAdapter::connect(transport)).unwrap();
    assert_eq!(
        adapter.query_primitive_capabilities().candidate_scan(),
        PushdownGuarantee::Candidate
    );
    let snapshot = block_on(adapter.begin_read_snapshot()).unwrap();
    let request = CandidateScanRequest::new(
        KeySpan::prefix(Keyspace::Current, b"v/".to_vec()),
        ValidTime::from_micros(55),
        Vec::new(),
        QueryPageBounds::new(1, 64).unwrap(),
    )
    .unwrap();

    block_on(adapter.apply_committed(CommittedMutationBatch {
        shard_id: 1,
        log_index: 2,
        txn_id: 2,
        mutations: vec![Mutation::put(0, key(b"v/1"), b"new".to_vec())],
    }))
    .unwrap();

    let page = block_on(snapshot.scan_candidates(&request)).unwrap();
    assert_eq!(page.applied_log_index(), 1);
    assert_eq!(page.guarantee(), PushdownGuarantee::Candidate);
    assert_eq!(page.entries().len(), 1);
    assert_eq!(page.entries()[0].value(), b"old-one");
    assert_eq!(page.next_start(), Some(&key(b"v/2")));

    drop(snapshot);
    drop(adapter);
    server.shutdown().unwrap();
}

#[test]
fn persistent_tcp_transport_executes_and_replays_committed_batches() {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let address = listener.local_addr().unwrap();
    let backend: Arc<dyn StorageAdapter> = Arc::new(MemoryAdapter::new());
    let server_backend = Arc::clone(&backend);
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        serve_connection(stream, server_backend.as_ref())
    });

    let config = TcpSidecarConfig::new(address)
        .with_pool_size(1)
        .unwrap()
        .with_timeouts(
            Duration::from_secs(1),
            Duration::from_secs(1),
            Duration::from_secs(1),
        )
        .unwrap();
    let transport = TcpSidecarTransport::connect(config).unwrap();
    let adapter = block_on(SidecarAdapter::connect(transport)).unwrap();
    let batch = CommittedMutationBatch {
        shard_id: 3,
        log_index: 1,
        txn_id: 901,
        mutations: vec![Mutation::put(0, key(b"v/1"), b"value".to_vec())],
    };
    assert!(
        !block_on(adapter.apply_committed(batch.clone()))
            .unwrap()
            .duplicate
    );
    assert!(block_on(adapter.apply_committed(batch)).unwrap().duplicate);
    assert_eq!(
        block_on(adapter.multi_get(&[key(b"v/1")])).unwrap(),
        vec![Some(b"value".to_vec())]
    );

    drop(adapter);
    server.join().unwrap().unwrap();
}

#[test]
fn tcp_configuration_rejects_empty_pools_and_zero_timeouts() {
    let address = SocketAddr::from((Ipv4Addr::LOCALHOST, 1));
    assert!(TcpSidecarConfig::new(address).with_pool_size(0).is_err());
    assert!(
        TcpSidecarConfig::new(address)
            .with_timeouts(
                Duration::ZERO,
                Duration::from_secs(1),
                Duration::from_secs(1)
            )
            .is_err()
    );
}

#[test]
fn bounded_tcp_server_handles_a_full_client_pool_and_shuts_down() {
    let backend: Arc<dyn StorageAdapter> = Arc::new(MemoryAdapter::new());
    let config = TcpSidecarServerConfig::new(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .with_capacity(2, 2)
        .unwrap();
    let server = spawn_tcp_sidecar_server(config, backend).unwrap();
    let client_config = TcpSidecarConfig::new(server.local_addr())
        .with_pool_size(2)
        .unwrap();
    let adapter = block_on(SidecarAdapter::connect(
        TcpSidecarTransport::connect(client_config).unwrap(),
    ))
    .unwrap();

    let batch = CommittedMutationBatch {
        shard_id: 9,
        log_index: 1,
        txn_id: 903,
        mutations: vec![Mutation::put(0, key(b"v/2"), b"bounded".to_vec())],
    };
    block_on(adapter.apply_committed(batch)).unwrap();
    assert_eq!(adapter.applied_log_index().unwrap(), 1);

    drop(adapter);
    server.shutdown().unwrap();
}

#[test]
fn reconnect_retry_reuses_the_original_request_identifier() {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let address = listener.local_addr().unwrap();
    let backend: Arc<dyn StorageAdapter> = Arc::new(MemoryAdapter::new());
    let server_backend = Arc::clone(&backend);
    let server = thread::spawn(move || {
        let (mut first_stream, _) = listener.accept().unwrap();
        let first = read_frame::<_, Request>(&mut first_stream).unwrap();
        drop(first_stream);

        let (mut replacement, _) = listener.accept().unwrap();
        let replay = read_frame::<_, Request>(&mut replacement).unwrap();
        assert_eq!(first.request_id(), replay.request_id());
        assert_eq!(first.message(), replay.message());
        let response = block_on(dispatch_request(
            server_backend.as_ref(),
            replay.into_message(),
        ));
        write_frame(&mut replacement, first.request_id(), &response).unwrap();
        serve_connection(replacement, server_backend.as_ref()).unwrap();
    });

    let config = TcpSidecarConfig::new(address)
        .with_pool_size(1)
        .unwrap()
        .with_timeouts(
            Duration::from_secs(1),
            Duration::from_secs(1),
            Duration::from_secs(1),
        )
        .unwrap();
    let adapter = block_on(SidecarAdapter::connect(
        TcpSidecarTransport::connect(config).unwrap(),
    ))
    .unwrap();
    assert_eq!(adapter.descriptor().implementation(), "memory");
    drop(adapter);
    server.join().unwrap();
}

#[test]
fn tcp_transport_does_not_retry_non_idempotent_read_view_creation() {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let address = listener.local_addr().unwrap();
    let (observed, observation) = mpsc::sync_channel(1);
    let server = thread::spawn(move || {
        let (mut first_stream, _) = listener.accept().unwrap();
        let first = read_frame::<_, Request>(&mut first_stream).unwrap();
        assert_eq!(first.message(), &Request::BeginReadView);
        drop(first_stream);

        listener.set_nonblocking(true).unwrap();
        let deadline = std::time::Instant::now() + Duration::from_millis(400);
        let mut reconnected = false;
        while std::time::Instant::now() < deadline {
            match listener.accept() {
                Ok((_stream, _)) => {
                    reconnected = true;
                    break;
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(5));
                }
                Err(error) => panic!("failed to observe reconnect: {error}"),
            }
        }
        observed.send(reconnected).unwrap();
    });
    let transport = TcpSidecarTransport::connect(
        TcpSidecarConfig::new(address)
            .with_pool_size(1)
            .unwrap()
            .with_timeouts(
                Duration::from_secs(1),
                Duration::from_millis(100),
                Duration::from_secs(1),
            )
            .unwrap(),
    )
    .unwrap();

    assert!(block_on(transport.call(Request::BeginReadView)).is_err());
    assert!(!observation.recv().unwrap(), "BeginReadView was retried");
    server.join().unwrap();
}

fn key(value: &[u8]) -> LogicalKey {
    LogicalKey::new(value.to_vec())
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
            Poll::Pending => thread::yield_now(),
        }
    }
}
