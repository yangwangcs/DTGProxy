use std::future::Future;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use adapter_memory::MemoryAdapter;
use adapter_registry::{AdapterOpenRequest, AdapterRegistry};
use adapter_rocksdb::{RocksAdapter, RocksAdapterFactory};
use adapter_sidecar::{
    BeginExportRequest, MAX_ACTIVE_SNAPSHOT_SESSIONS, RemoteErrorCode, Request, Response,
    SidecarRestoreBackend, SidecarService, TcpSidecarAdapterFactory, TcpSidecarServerConfig,
    spawn_stateful_tcp_sidecar_server,
};
use storage_api::{
    AdapterRequirement, CommittedMutationBatch, Keyspace, LogicalKey, LogicalSnapshotExportRequest,
    Mutation, StorageAdapter,
};

#[test]
fn snapshot_sessions_are_capacity_bounded_and_release_slots_on_abort() {
    let temporary = tempfile::tempdir().unwrap();
    let backend = Arc::new(RocksAdapter::open(temporary.path().join("bounded-source")).unwrap());
    let service = SidecarService::new(backend, None);
    let mut sessions = Vec::new();
    for _ in 0..MAX_ACTIVE_SNAPSHOT_SESSIONS {
        match block_on(service.dispatch(Request::BeginExport(BeginExportRequest {
            limits: LogicalSnapshotExportRequest::default(),
            expected_applied_log_index: Some(0),
        }))) {
            Response::ExportStarted(started) => sessions.push(started.session_id),
            response => panic!("expected export session, got {response:?}"),
        }
    }
    match block_on(service.dispatch(Request::BeginExport(BeginExportRequest {
        limits: LogicalSnapshotExportRequest::default(),
        expected_applied_log_index: Some(0),
    }))) {
        Response::Error(error) => assert_eq!(error.code, RemoteErrorCode::ResourceExhausted as u32),
        response => panic!("capacity overflow was accepted: {response:?}"),
    }
    assert!(matches!(
        block_on(service.dispatch(Request::AbortSession {
            session_id: sessions[0]
        })),
        Response::SessionAborted { .. }
    ));
    assert!(matches!(
        block_on(service.dispatch(Request::BeginExport(BeginExportRequest {
            limits: LogicalSnapshotExportRequest::default(),
            expected_applied_log_index: Some(0),
        }))),
        Response::ExportStarted(_)
    ));
}

#[test]
fn sidecar_factory_rejects_zero_transport_timeouts() {
    let service = Arc::new(SidecarService::new(Arc::new(MemoryAdapter::new()), None));
    let server = spawn_stateful_tcp_sidecar_server(
        TcpSidecarServerConfig::new(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))),
        service,
    )
    .unwrap();
    let mut registry = AdapterRegistry::new();
    registry
        .register(Arc::new(TcpSidecarAdapterFactory))
        .unwrap();
    let request = AdapterOpenRequest::new("invalid-timeout")
        .with_parameter("endpoint", server.local_addr().to_string())
        .with_parameter("connect_timeout_ms", "0");
    let error = match block_on(registry.open("sidecar", &request, AdapterRequirement::Development))
    {
        Ok(_) => panic!("zero timeout was accepted"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("timeout"));
    server.shutdown().unwrap();
}

#[test]
fn stateful_server_reuses_connection_capacity_after_clients_disconnect() {
    let service = Arc::new(SidecarService::new(Arc::new(MemoryAdapter::new()), None));
    let server = spawn_stateful_tcp_sidecar_server(
        TcpSidecarServerConfig::new(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
            .with_capacity(1, 1)
            .unwrap(),
        service,
    )
    .unwrap();
    let mut registry = AdapterRegistry::new();
    registry
        .register(Arc::new(TcpSidecarAdapterFactory))
        .unwrap();

    for attempt in 0..3 {
        let request = AdapterOpenRequest::new(format!("client-{attempt}"))
            .with_parameter("endpoint", server.local_addr().to_string())
            .with_parameter("pool_size", "1");
        let opened =
            block_on(registry.open("sidecar", &request, AdapterRequirement::Development)).unwrap();
        assert_eq!(opened.adapter().applied_log_index().unwrap(), 0);
        drop(opened);
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    server.shutdown().unwrap();
}

#[test]
fn stateful_sidecars_export_restore_and_publish_a_remote_adapter() {
    let temporary = tempfile::tempdir().unwrap();
    let source_backend = Arc::new(RocksAdapter::open(temporary.path().join("source")).unwrap());
    block_on(source_backend.apply_committed(batch(1, b"vertex/1", b"payload"))).unwrap();
    let source_service = Arc::new(SidecarService::new(source_backend, None));
    let source_server = spawn_stateful_tcp_sidecar_server(
        TcpSidecarServerConfig::new(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))),
        source_service,
    )
    .unwrap();

    let probe = RocksAdapter::open(temporary.path().join("descriptor-probe")).unwrap();
    let descriptor = probe.descriptor();
    drop(probe);
    let mut target_backends = AdapterRegistry::new();
    target_backends
        .register(Arc::new(RocksAdapterFactory))
        .unwrap();
    let target_service = Arc::new(SidecarService::new(
        Arc::new(MemoryAdapter::new()),
        Some(SidecarRestoreBackend::new(
            Arc::new(target_backends),
            "rocksdb",
            AdapterRequirement::HotPluggableReplica,
            descriptor,
        )),
    ));
    let target_server = spawn_stateful_tcp_sidecar_server(
        TcpSidecarServerConfig::new(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))),
        target_service,
    )
    .unwrap();

    let mut registry = AdapterRegistry::new();
    registry
        .register(Arc::new(TcpSidecarAdapterFactory))
        .unwrap();
    let source_request = AdapterOpenRequest::new("remote-source")
        .with_parameter("endpoint", source_server.local_addr().to_string());
    let source = block_on(registry.open(
        "sidecar",
        &source_request,
        AdapterRequirement::HotPluggableReplica,
    ))
    .unwrap();
    let reader = block_on(
        source
            .adapter()
            .begin_logical_export(LogicalSnapshotExportRequest::new(2, 4_096).unwrap()),
    )
    .unwrap();
    let target_path = temporary.path().join("target");
    let target_request = AdapterOpenRequest::new("remote-target")
        .with_parameter("endpoint", target_server.local_addr().to_string())
        .with_parameter("path", target_path.to_string_lossy());
    let target = block_on(registry.restore(
        "sidecar",
        &target_request,
        AdapterRequirement::HotPluggableReplica,
        reader,
    ))
    .unwrap();
    assert_eq!(target.adapter().applied_log_index().unwrap(), 1);
    assert_eq!(
        block_on(target.adapter().multi_get(&[key(b"vertex/1")])).unwrap(),
        vec![Some(b"payload".to_vec())]
    );
    block_on(
        target
            .adapter()
            .apply_committed(batch(2, b"vertex/2", b"second")),
    )
    .unwrap();
    assert_eq!(target.adapter().applied_log_index().unwrap(), 2);

    drop(target);
    drop(source);
    target_server.shutdown().unwrap();
    source_server.shutdown().unwrap();
}

fn batch(index: u64, value_key: &[u8], value: &[u8]) -> CommittedMutationBatch {
    CommittedMutationBatch {
        shard_id: 1,
        log_index: index,
        txn_id: u128::from(index),
        mutations: vec![Mutation::put(0, key(value_key), value.to_vec())],
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
