use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use adapter_registry::{AdapterOpenRequest, AdapterRegistry};
use adapter_rocksdb::{RocksAdapter, RocksAdapterFactory};
use storage_api::{AdapterError, AdapterRequirement};

#[test]
fn open_creates_all_required_column_families() {
    let directory = tempfile::tempdir().unwrap();
    let adapter = RocksAdapter::open(directory.path()).unwrap();

    assert_eq!(adapter.path(), directory.path());
    assert!(adapter.capabilities().local_atomic_batch);
    assert!(adapter.capabilities().idempotent_apply);
    assert_eq!(
        adapter.column_family_names().unwrap(),
        vec![
            "adj_in",
            "adj_out",
            "current",
            "default",
            "history",
            "identity",
            "meta",
            "temporal_index",
            "txn",
        ]
    );
}

#[test]
fn open_maps_native_failures_to_a_typed_backend_error() {
    let file = tempfile::NamedTempFile::new().unwrap();

    let error = match RocksAdapter::open(file.path()) {
        Ok(_) => panic!("opening a regular file as RocksDB must fail"),
        Err(error) => error,
    };

    assert!(matches!(error, AdapterError::Backend(_)));
}

#[test]
fn production_registry_hot_plugs_the_rocksdb_factory() {
    let directory = tempfile::tempdir().unwrap();
    let mut registry = AdapterRegistry::new();
    registry.register(Arc::new(RocksAdapterFactory)).unwrap();
    let request = AdapterOpenRequest::new("shard-7").with_parameter(
        "path",
        directory.path().to_str().expect("temporary path is UTF-8"),
    );

    let opened =
        block_on(registry.open("rocksdb", &request, AdapterRequirement::ManagedReplica)).unwrap();
    assert_eq!(opened.provider_name(), "rocksdb");
    assert_eq!(opened.descriptor().implementation(), "rocksdb");
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
