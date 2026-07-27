use std::future::Future;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::{Arc, Condvar, Mutex, mpsc};
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
    AdapterCapabilities, AdapterFuture, AdapterRequirement, ApplyReceipt, CanonicalScanRequest,
    CommittedMutationBatch, KeySpan, KeyValue, Keyspace, LogicalKey, LogicalSnapshotExportRequest,
    Mutation, QueryPageBounds, ReadSnapshot, ReadSnapshotBinding, StorageAdapter,
};

#[test]
fn stateful_sidecar_rejects_backends_without_real_read_snapshots() {
    let service = SidecarService::new(Arc::new(NoReadSnapshotAdapter(MemoryAdapter::new())), None);
    match block_on(service.dispatch(Request::BeginReadView)) {
        Response::Error(error) => {
            assert_eq!(error.code, RemoteErrorCode::FeatureUnsupported as u32)
        }
        response => panic!("backend without snapshots opened a read view: {response:?}"),
    }
}

#[test]
fn read_view_session_reuses_one_backend_snapshot_until_end() {
    let backend = Arc::new(MemoryAdapter::new());
    block_on(backend.apply_committed(batch(1, b"vertex/1", b"old"))).unwrap();
    let service = SidecarService::new(backend, None);

    let (session_id, applied_log_index) = match block_on(service.dispatch(Request::BeginReadView)) {
        Response::ReadViewStarted {
            session_id,
            applied_log_index,
        } => (session_id, applied_log_index),
        response => panic!("expected read-view session, got {response:?}"),
    };
    assert_eq!(applied_log_index, 1);

    block_on(service.dispatch(Request::Apply(batch(2, b"vertex/1", b"new"))));
    for _ in 0..2 {
        assert_eq!(
            block_on(service.dispatch(Request::ReadViewMultiGet {
                session_id,
                keys: vec![key(b"vertex/1")],
            })),
            Response::ReadViewMultiGet {
                session_id,
                values: vec![Some(b"old".to_vec())],
            }
        );
        assert_eq!(
            block_on(service.dispatch(Request::ReadViewScan {
                session_id,
                span: KeySpan::prefix(Keyspace::Current, b"vertex/".to_vec()),
            })),
            Response::ReadViewScan {
                session_id,
                values: vec![storage_api::KeyValue::new(
                    key(b"vertex/1"),
                    b"old".to_vec(),
                )],
            }
        );
    }
    let canonical_request = CanonicalScanRequest::new(
        KeySpan::prefix(Keyspace::Current, b"vertex/".to_vec()),
        QueryPageBounds::new(1, 64).unwrap(),
    )
    .unwrap();
    match block_on(service.dispatch(Request::ReadViewCanonicalScan {
        session_id,
        request: canonical_request,
    })) {
        Response::ReadViewCanonicalScan {
            session_id: actual_session,
            applied_log_index,
            entries,
            next_start,
        } => {
            assert_eq!(actual_session, session_id);
            assert_eq!(applied_log_index, 1);
            assert_eq!(entries[0].value(), b"old");
            assert_eq!(next_start, None);
        }
        response => panic!("expected canonical read-view page, got {response:?}"),
    }

    assert_eq!(
        block_on(service.dispatch(Request::EndReadView { session_id })),
        Response::ReadViewEnded { session_id }
    );
    match block_on(service.dispatch(Request::ReadViewMultiGet {
        session_id,
        keys: vec![key(b"vertex/1")],
    })) {
        Response::Error(error) => assert_eq!(error.code, RemoteErrorCode::SessionUnknown as u32),
        response => panic!("ended read view remained usable: {response:?}"),
    }
}

#[test]
fn read_view_opens_from_a_bound_snapshot_owner() {
    let owner = Arc::new(MemoryAdapter::new());
    block_on(owner.apply_committed(batch(1, b"vertex/1", b"bound"))).unwrap();
    let service = SidecarService::new(
        Arc::new(BindingOnlyReadAdapter {
            owner,
            generation: 7,
        }),
        None,
    );

    let session_id = match block_on(service.dispatch(Request::BeginReadView)) {
        Response::ReadViewStarted {
            session_id,
            applied_log_index: 1,
        } => session_id,
        response => panic!("expected bound read-view session, got {response:?}"),
    };
    assert_eq!(
        block_on(service.dispatch(Request::ReadViewMultiGet {
            session_id,
            keys: vec![key(b"vertex/1")],
        })),
        Response::ReadViewMultiGet {
            session_id,
            values: vec![Some(b"bound".to_vec())],
        }
    );
}

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
fn end_read_view_does_not_destroy_a_different_session_kind() {
    let temporary = tempfile::tempdir().unwrap();
    let backend = Arc::new(RocksAdapter::open(temporary.path().join("kind-source")).unwrap());
    let service = SidecarService::new(backend, None);
    let session_id = match block_on(service.dispatch(Request::BeginExport(BeginExportRequest {
        limits: LogicalSnapshotExportRequest::default(),
        expected_applied_log_index: Some(0),
    }))) {
        Response::ExportStarted(started) => started.session_id,
        response => panic!("expected export session, got {response:?}"),
    };

    match block_on(service.dispatch(Request::EndReadView { session_id })) {
        Response::Error(error) => {
            assert_eq!(error.code, RemoteErrorCode::SessionKindMismatch as u32)
        }
        response => panic!("wrong-kind end was accepted: {response:?}"),
    }
    assert_eq!(
        block_on(service.dispatch(Request::AbortSession { session_id })),
        Response::SessionAborted { session_id }
    );
}

#[test]
fn ending_a_blocked_read_view_does_not_block_other_session_operations() {
    let (entered, read_started) = mpsc::sync_channel(1);
    let release = Arc::new((Mutex::new(false), Condvar::new()));
    let service = Arc::new(SidecarService::new(
        Arc::new(BlockingReadAdapter {
            inner: MemoryAdapter::new(),
            entered,
            release: Arc::clone(&release),
        }),
        None,
    ));
    let session_id = match block_on(service.dispatch(Request::BeginReadView)) {
        Response::ReadViewStarted { session_id, .. } => session_id,
        response => panic!("expected read-view session, got {response:?}"),
    };

    let reader_service = Arc::clone(&service);
    let reader = std::thread::spawn(move || {
        block_on(reader_service.dispatch(Request::ReadViewScan {
            session_id,
            span: KeySpan::prefix(Keyspace::Current, Vec::new()),
        }))
    });
    read_started.recv().unwrap();

    let end_service = Arc::clone(&service);
    let end = std::thread::spawn(move || {
        block_on(end_service.dispatch(Request::EndReadView { session_id }))
    });
    let begin_service = Arc::clone(&service);
    let (begin_result, begin_response) = mpsc::sync_channel(1);
    let begin = std::thread::spawn(move || {
        let response = block_on(begin_service.dispatch(Request::BeginReadView));
        let _ = begin_result.send(response);
    });

    let response = begin_response.recv_timeout(std::time::Duration::from_millis(200));
    {
        let (released, ready) = &*release;
        *released.lock().unwrap() = true;
        ready.notify_all();
    }
    let response = response.expect("another session operation was blocked by read-view shutdown");
    assert!(matches!(response, Response::ReadViewStarted { .. }));
    assert!(matches!(
        reader.join().unwrap(),
        Response::ReadViewScan { .. }
    ));
    assert_eq!(end.join().unwrap(), Response::ReadViewEnded { session_id });
    begin.join().unwrap();
}

#[test]
fn blocked_read_view_rejects_excess_queued_commands() {
    let (entered, read_started) = mpsc::sync_channel(32);
    let release = Arc::new((Mutex::new(false), Condvar::new()));
    let service = Arc::new(SidecarService::new(
        Arc::new(BlockingReadAdapter {
            inner: MemoryAdapter::new(),
            entered,
            release: Arc::clone(&release),
        }),
        None,
    ));
    let session_id = match block_on(service.dispatch(Request::BeginReadView)) {
        Response::ReadViewStarted { session_id, .. } => session_id,
        response => panic!("expected read-view session, got {response:?}"),
    };

    let active_service = Arc::clone(&service);
    let active = std::thread::spawn(move || {
        block_on(active_service.dispatch(Request::ReadViewScan {
            session_id,
            span: KeySpan::prefix(Keyspace::Current, Vec::new()),
        }))
    });
    read_started.recv().unwrap();

    let (responses, queued_responses) = mpsc::channel();
    let queued = (0..8)
        .map(|_| {
            let service = Arc::clone(&service);
            let responses = responses.clone();
            std::thread::spawn(move || {
                let response = block_on(service.dispatch(Request::ReadViewScan {
                    session_id,
                    span: KeySpan::prefix(Keyspace::Current, Vec::new()),
                }));
                let _ = responses.send(response);
            })
        })
        .collect::<Vec<_>>();
    drop(responses);

    let overloaded = queued_responses.recv_timeout(std::time::Duration::from_secs(1));

    {
        let (released, ready) = &*release;
        *released.lock().unwrap() = true;
        ready.notify_all();
    }
    assert!(matches!(
        active.join().unwrap(),
        Response::ReadViewScan { .. }
    ));
    for request in queued {
        request.join().unwrap();
    }
    let overloaded =
        overloaded.expect("an unbounded read-view command queue accepted every blocked request");
    assert!(matches!(
        overloaded,
        Response::Error(error) if error.code == RemoteErrorCode::SessionBusy as u32
    ));
}

#[test]
fn slow_read_view_begin_cannot_resurrect_an_aborted_reservation() {
    let (entered, begin_started) = mpsc::sync_channel(1);
    let release = Arc::new((Mutex::new(false), Condvar::new()));
    let service = Arc::new(SidecarService::new(
        Arc::new(BlockingBeginAdapter {
            inner: MemoryAdapter::new(),
            entered,
            release: Arc::clone(&release),
        }),
        None,
    ));

    let begin_service = Arc::clone(&service);
    let begin =
        std::thread::spawn(move || block_on(begin_service.dispatch(Request::BeginReadView)));
    begin_started.recv().unwrap();

    assert_eq!(
        block_on(service.dispatch(Request::AbortSession { session_id: 1 })),
        Response::SessionAborted { session_id: 1 }
    );
    {
        let (released, ready) = &*release;
        *released.lock().unwrap() = true;
        ready.notify_all();
    }

    assert!(matches!(
        begin.join().unwrap(),
        Response::Error(error) if error.code == RemoteErrorCode::SessionExpired as u32
    ));
    let next_session = match block_on(service.dispatch(Request::BeginReadView)) {
        Response::ReadViewStarted { session_id, .. } => session_id,
        response => {
            panic!("fresh reservation failed after the stale Begin was rejected: {response:?}")
        }
    };
    assert_eq!(next_session, 2);
    assert_eq!(
        block_on(service.dispatch(Request::EndReadView {
            session_id: next_session,
        })),
        Response::ReadViewEnded {
            session_id: next_session,
        }
    );
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

struct NoReadSnapshotAdapter(MemoryAdapter);

struct BindingOnlyReadAdapter {
    owner: Arc<MemoryAdapter>,
    generation: u64,
}

impl StorageAdapter for BindingOnlyReadAdapter {
    fn capabilities(&self) -> AdapterCapabilities {
        self.owner.capabilities()
    }

    fn query_capability_generation(&self) -> u64 {
        self.generation
    }

    fn read_snapshot_binding(
        &self,
    ) -> Result<Option<ReadSnapshotBinding>, storage_api::AdapterError> {
        let owner: Arc<dyn StorageAdapter> = self.owner.clone();
        ReadSnapshotBinding::new(self.generation, owner).map(Some)
    }

    fn apply_committed<'a>(
        &'a self,
        batch: CommittedMutationBatch,
    ) -> AdapterFuture<'a, ApplyReceipt> {
        self.owner.apply_committed(batch)
    }

    fn multi_get<'a>(&'a self, keys: &'a [LogicalKey]) -> AdapterFuture<'a, Vec<Option<Vec<u8>>>> {
        self.owner.multi_get(keys)
    }

    fn scan<'a>(&'a self, span: &'a KeySpan) -> AdapterFuture<'a, Vec<KeyValue>> {
        self.owner.scan(span)
    }

    fn applied_log_index(&self) -> Result<u64, storage_api::AdapterError> {
        self.owner.applied_log_index()
    }
}

impl StorageAdapter for NoReadSnapshotAdapter {
    fn capabilities(&self) -> AdapterCapabilities {
        self.0.capabilities()
    }

    fn apply_committed<'a>(
        &'a self,
        batch: CommittedMutationBatch,
    ) -> AdapterFuture<'a, ApplyReceipt> {
        self.0.apply_committed(batch)
    }

    fn multi_get<'a>(&'a self, keys: &'a [LogicalKey]) -> AdapterFuture<'a, Vec<Option<Vec<u8>>>> {
        self.0.multi_get(keys)
    }

    fn scan<'a>(&'a self, span: &'a KeySpan) -> AdapterFuture<'a, Vec<KeyValue>> {
        self.0.scan(span)
    }

    fn applied_log_index(&self) -> Result<u64, storage_api::AdapterError> {
        self.0.applied_log_index()
    }
}

struct BlockingReadAdapter {
    inner: MemoryAdapter,
    entered: mpsc::SyncSender<()>,
    release: Arc<(Mutex<bool>, Condvar)>,
}

struct BlockingBeginAdapter {
    inner: MemoryAdapter,
    entered: mpsc::SyncSender<()>,
    release: Arc<(Mutex<bool>, Condvar)>,
}

impl StorageAdapter for BlockingBeginAdapter {
    fn capabilities(&self) -> AdapterCapabilities {
        self.inner.capabilities()
    }

    fn apply_committed<'a>(
        &'a self,
        batch: CommittedMutationBatch,
    ) -> AdapterFuture<'a, ApplyReceipt> {
        self.inner.apply_committed(batch)
    }

    fn multi_get<'a>(&'a self, keys: &'a [LogicalKey]) -> AdapterFuture<'a, Vec<Option<Vec<u8>>>> {
        self.inner.multi_get(keys)
    }

    fn scan<'a>(&'a self, span: &'a KeySpan) -> AdapterFuture<'a, Vec<KeyValue>> {
        self.inner.scan(span)
    }

    fn begin_read_snapshot<'a>(&'a self) -> AdapterFuture<'a, Box<dyn ReadSnapshot + 'a>> {
        Box::pin(async move {
            let _ = self.entered.send(());
            {
                let (released, ready) = &*self.release;
                let mut released = released.lock().unwrap();
                while !*released {
                    released = ready.wait(released).unwrap();
                }
            }
            self.inner.begin_read_snapshot().await
        })
    }

    fn applied_log_index(&self) -> Result<u64, storage_api::AdapterError> {
        self.inner.applied_log_index()
    }
}

impl StorageAdapter for BlockingReadAdapter {
    fn capabilities(&self) -> AdapterCapabilities {
        self.inner.capabilities()
    }

    fn apply_committed<'a>(
        &'a self,
        batch: CommittedMutationBatch,
    ) -> AdapterFuture<'a, ApplyReceipt> {
        self.inner.apply_committed(batch)
    }

    fn multi_get<'a>(&'a self, keys: &'a [LogicalKey]) -> AdapterFuture<'a, Vec<Option<Vec<u8>>>> {
        self.inner.multi_get(keys)
    }

    fn scan<'a>(&'a self, span: &'a KeySpan) -> AdapterFuture<'a, Vec<KeyValue>> {
        self.inner.scan(span)
    }

    fn begin_read_snapshot<'a>(&'a self) -> AdapterFuture<'a, Box<dyn ReadSnapshot + 'a>> {
        let entered = self.entered.clone();
        let release = Arc::clone(&self.release);
        Box::pin(async move {
            Ok(Box::new(BlockingReadSnapshot { entered, release }) as Box<dyn ReadSnapshot>)
        })
    }

    fn applied_log_index(&self) -> Result<u64, storage_api::AdapterError> {
        self.inner.applied_log_index()
    }
}

struct BlockingReadSnapshot {
    entered: mpsc::SyncSender<()>,
    release: Arc<(Mutex<bool>, Condvar)>,
}

impl ReadSnapshot for BlockingReadSnapshot {
    fn applied_log_index(&self) -> u64 {
        0
    }

    fn multi_get<'a>(&'a self, keys: &'a [LogicalKey]) -> AdapterFuture<'a, Vec<Option<Vec<u8>>>> {
        Box::pin(async move { Ok(vec![None; keys.len()]) })
    }

    fn scan<'a>(&'a self, _span: &'a KeySpan) -> AdapterFuture<'a, Vec<KeyValue>> {
        Box::pin(async move {
            let _ = self.entered.send(());
            let (released, ready) = &*self.release;
            let mut released = released.lock().unwrap();
            while !*released {
                released = ready.wait(released).unwrap();
            }
            Ok(Vec::new())
        })
    }
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
