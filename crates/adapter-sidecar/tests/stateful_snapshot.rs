use std::future::Future;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use adapter_memory::MemoryAdapter;
use adapter_registry::{AdapterOpenRequest, AdapterRegistry};
use adapter_rocksdb::{RocksAdapter, RocksAdapterFactory};
use adapter_sidecar::{
    SidecarRestoreBackend, SidecarService, TcpSidecarAdapterFactory, TcpSidecarServerConfig,
    spawn_stateful_tcp_sidecar_server,
};
use storage_api::{
    AdapterRequirement, CommittedMutationBatch, Keyspace, LogicalKey, LogicalSnapshotExportRequest,
    Mutation, StorageAdapter,
};

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
