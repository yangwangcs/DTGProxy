use std::collections::BTreeMap;
use std::future::Future;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use adapter_rocksdb::RocksAdapter;
use adapter_sidecar::{SidecarService, TcpSidecarServerConfig, spawn_stateful_tcp_sidecar_server};
use data_node::{BackendError, BackendManager, BackendProfile, BackendSlotState, StartupBackend};
use storage_api::{BackendFamily, StorageAdapter};

#[test]
fn rocksdb_startup_accepts_only_rocksdb_active_profiles() {
    let manager = BackendManager::production(StartupBackend::Rocksdb).unwrap();

    assert!(manager.validate_active_profile(&rocks_profile()).is_ok());
    assert!(
        manager
            .validate_active_profile(&sidecar_profile("postgresql"))
            .is_err()
    );
}

#[test]
fn logical_sidecar_backends_remain_distinct() {
    let postgres = BackendManager::production(StartupBackend::Postgresql).unwrap();

    assert!(
        postgres
            .validate_active_profile(&sidecar_profile("postgresql"))
            .is_ok()
    );
    assert!(
        postgres
            .validate_active_profile(&sidecar_profile("neo4j"))
            .is_err()
    );
}

#[test]
fn migration_target_is_validated_without_preloading_it() {
    let manager = BackendManager::production(StartupBackend::Rocksdb).unwrap();

    assert!(
        manager
            .validate_migration_target(&sidecar_profile("neo4j"))
            .is_ok()
    );
}

fn rocks_profile() -> BackendProfile {
    BackendProfile::new(
        "rocksdb",
        "local-generation-1",
        BTreeMap::from([("path".into(), "adapter-generation-1".into())]),
        BTreeMap::new(),
    )
    .unwrap()
}

fn sidecar_profile(target_provider: &str) -> BackendProfile {
    BackendProfile::new(
        "sidecar",
        format!("{target_provider}-generation-1"),
        BTreeMap::from([
            ("endpoint".into(), "127.0.0.1:19091".into()),
            ("target_provider".into(), target_provider.into()),
        ]),
        BTreeMap::new(),
    )
    .unwrap()
}

#[test]
fn production_backend_manager_opens_local_rocks_and_preserves_generation() {
    let temporary = tempfile::tempdir().unwrap();
    let profile = BackendProfile::new(
        "rocksdb",
        "local-generation-3",
        BTreeMap::from([("path".into(), "adapter-generation-3".into())]),
        BTreeMap::new(),
    )
    .unwrap();
    let state = BackendSlotState::active(3, profile).unwrap();
    let manager = BackendManager::production(StartupBackend::Rocksdb).unwrap();

    let slot = block_on(manager.open_slot(temporary.path(), &state)).unwrap();

    assert_eq!(slot.generation(), 3);
    assert_eq!(slot.active_provider_name(), "rocksdb");
    assert_eq!(slot.descriptor().family(), BackendFamily::KeyValue);
    assert!(temporary.path().join("adapter-generation-3").is_dir());
}

#[test]
fn production_backend_manager_opens_a_loopback_sidecar_profile() {
    let temporary = tempfile::tempdir().unwrap();
    let backend = Arc::new(RocksAdapter::open(temporary.path().join("remote-rocks")).unwrap());
    let server = spawn_stateful_tcp_sidecar_server(
        TcpSidecarServerConfig::new(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))),
        Arc::new(SidecarService::new(backend, None)),
    )
    .unwrap();
    let profile = BackendProfile::new(
        "sidecar",
        "remote-generation-8",
        BTreeMap::from([
            ("endpoint".into(), server.local_addr().to_string()),
            ("pool_size".into(), "1".into()),
            ("target_provider".into(), "postgresql".into()),
        ]),
        BTreeMap::from([("password".into(), "postgres-main".into())]),
    )
    .unwrap();
    let state = BackendSlotState::active(8, profile).unwrap();
    let manager = BackendManager::production(StartupBackend::Postgresql).unwrap();

    let slot = block_on(manager.open_slot(temporary.path(), &state)).unwrap();

    assert_eq!(slot.generation(), 8);
    assert_eq!(slot.active_provider_name(), "sidecar");
    assert_eq!(slot.descriptor().family(), BackendFamily::KeyValue);
    server.shutdown().unwrap();
}

#[test]
fn production_backend_manager_rejects_non_loopback_plaintext_sidecars() {
    let temporary = tempfile::tempdir().unwrap();
    let profile = BackendProfile::new(
        "sidecar",
        "unsafe-sidecar",
        BTreeMap::from([
            ("endpoint".into(), "192.0.2.1:19091".into()),
            ("target_provider".into(), "postgresql".into()),
        ]),
        BTreeMap::new(),
    )
    .unwrap();
    let state = BackendSlotState::active(1, profile).unwrap();
    let manager = BackendManager::production(StartupBackend::Postgresql).unwrap();

    assert!(matches!(
        block_on(manager.open_slot(temporary.path(), &state)),
        Err(BackendError::NonLoopbackSidecarEndpoint { .. })
    ));
}

fn block_on<F: Future>(future: F) -> F::Output {
    struct ThreadWake(std::thread::Thread);

    impl Wake for ThreadWake {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
    }

    let waker = Waker::from(Arc::new(ThreadWake(std::thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = Box::pin(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(value) => return value,
            Poll::Pending => std::thread::park(),
        }
    }
}
