use std::future::Future;
use std::net::{Ipv4Addr, SocketAddr, TcpListener};
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::thread;
use std::time::Duration;

use adapter_memory::MemoryAdapter;
use adapter_sidecar::{
    Request, SidecarAdapter, TcpSidecarConfig, TcpSidecarServerConfig, TcpSidecarTransport,
    dispatch_request, read_frame, serve_connection, spawn_tcp_sidecar_server, write_frame,
};
use storage_api::{CommittedMutationBatch, LogicalKey, Mutation, StorageAdapter};

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
