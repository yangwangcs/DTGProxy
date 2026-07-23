use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use adapter_registry::{AdapterOpenRequest, AdapterRegistry};
use adapter_rocksdb::{RocksAdapter, RocksAdapterFactory};
use storage_api::{
    AdapterRequirement, CommittedMutationBatch, LogicalKey, MappingRequirement, Mutation,
    TemporalBackendMapping, run_mapping_restore_tck, run_mapping_tck,
};
use temporal_storage::run_temporal_graph_mapping_tck;

fn batch(log_index: u64) -> CommittedMutationBatch {
    CommittedMutationBatch {
        shard_id: 3,
        log_index,
        txn_id: u128::from(log_index),
        mutations: vec![Mutation::put(
            0,
            LogicalKey::new(b"vertex/1".to_vec()),
            b"payload".to_vec(),
        )],
    }
}

#[test]
fn rocksdb_mapping_lifecycle_has_no_visibility_before_commit() {
    let directory = tempfile::tempdir().unwrap();
    let mapping = RocksAdapter::open(directory.path()).unwrap();
    mapping
        .describe_schema()
        .validate(MappingRequirement::HotPluggableReplica)
        .unwrap();
    mapping.validate_mapping().unwrap();

    let mut aborted = block_on(mapping.prepare(batch(1))).unwrap();
    block_on(aborted.apply()).unwrap();
    assert_eq!(
        TemporalBackendMapping::applied_log_index(&mapping).unwrap(),
        0
    );
    assert_eq!(
        block_on(TemporalBackendMapping::multi_get(
            &mapping,
            &[LogicalKey::new(b"vertex/1".to_vec())],
        ))
        .unwrap(),
        vec![None]
    );
    block_on(aborted.abort()).unwrap();
    assert_eq!(
        TemporalBackendMapping::applied_log_index(&mapping).unwrap(),
        0
    );

    let mut committed = block_on(mapping.prepare(batch(1))).unwrap();
    block_on(committed.apply()).unwrap();
    let receipt = block_on(committed.commit()).unwrap();
    assert_eq!(receipt.applied_log_index, 1);
    assert!(!receipt.duplicate);
    assert_eq!(
        block_on(TemporalBackendMapping::multi_get(
            &mapping,
            &[LogicalKey::new(b"vertex/1".to_vec())],
        ))
        .unwrap(),
        vec![Some(b"payload".to_vec())]
    );
}

#[test]
fn rocksdb_factory_publishes_the_exact_mapping_descriptor() {
    let directory = tempfile::tempdir().unwrap();
    let mut registry = AdapterRegistry::new();
    registry.register(Arc::new(RocksAdapterFactory)).unwrap();
    let request = AdapterOpenRequest::new("rocks-mapping")
        .with_parameter("path", directory.path().to_string_lossy());

    let opened =
        block_on(registry.open("rocksdb", &request, AdapterRequirement::HotPluggableReplica))
            .unwrap();

    let mapping = opened
        .mapping_descriptor()
        .expect("RocksDB must be Mapping-certified");
    assert_eq!(mapping.name(), "rocksdb-canonical");
    assert_eq!(mapping.version(), "1.0.0");
    assert_eq!(
        Some(mapping),
        opened.adapter().mapping_descriptor().as_ref()
    );
}

#[test]
fn rocksdb_passes_the_shared_mapping_tck_and_canonical_restore() {
    let source_directory = tempfile::tempdir().unwrap();
    let destination_directory = tempfile::tempdir().unwrap();
    let source = RocksAdapter::open(source_directory.path()).unwrap();
    let destination = RocksAdapter::open(destination_directory.path()).unwrap();

    run_mapping_tck(&source);
    run_mapping_restore_tck(&source, &destination);
}

#[test]
fn rocksdb_passes_the_temporal_graph_mapping_tck() {
    let source_directory = tempfile::tempdir().unwrap();
    let destination_directory = tempfile::tempdir().unwrap();
    let source: Arc<dyn TemporalBackendMapping> =
        Arc::new(RocksAdapter::open(source_directory.path()).unwrap());
    let destination: Arc<dyn TemporalBackendMapping> =
        Arc::new(RocksAdapter::open(destination_directory.path()).unwrap());

    run_temporal_graph_mapping_tck(source, destination);
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
