use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use adapter_registry::{AdapterFactory, AdapterOpenRequest, AdapterRegistry};
use adapter_rocksdb::RocksAdapter;
use adapter_rocksdb::RocksAdapterFactory;
use storage_api::{
    AdapterRequirement, CommittedMutationBatch, Keyspace, LogicalKey, LogicalSnapshotExportRequest,
    LogicalSnapshotManifestV1, Mutation, StorageAdapter,
};

#[test]
fn logical_export_is_chunked_globally_ordered_and_frozen_at_one_applied_index() {
    let root = tempfile::tempdir().unwrap();
    let adapter = RocksAdapter::open(root.path().join("source")).unwrap();
    block_on(adapter.apply_committed(batch(1, b"a", b"first"))).unwrap();
    let mut reader =
        block_on(adapter.begin_logical_export(LogicalSnapshotExportRequest::new(2, 1024).unwrap()))
            .unwrap();
    assert_eq!(reader.header().applied_log_index(), 1);

    block_on(adapter.apply_committed(batch(2, b"b", b"later"))).unwrap();
    let mut entries = Vec::new();
    let mut chunks = 0_u64;
    while let Some(chunk) = block_on(reader.next_chunk()).unwrap() {
        assert_eq!(chunk.ordinal(), chunks);
        assert!(chunk.entries().len() <= 2);
        entries.extend_from_slice(chunk.entries());
        chunks += 1;
    }
    let manifest = block_on(reader.finish()).unwrap();
    assert_eq!(manifest.header().applied_log_index(), 1);
    assert_eq!(manifest.total_chunks(), chunks);
    assert_eq!(manifest.total_entries(), entries.len() as u64);
    assert!(entries.windows(2).all(|pair| pair[0].key() < pair[1].key()));
    assert!(entries.iter().any(|entry| {
        entry.key() == &LogicalKey::in_keyspace(Keyspace::Current, b"a".to_vec())
            && entry.value() == b"first"
    }));
    assert!(!entries.iter().any(|entry| {
        entry.key() == &LogicalKey::in_keyspace(Keyspace::Current, b"b".to_vec())
    }));
    assert!(
        entries
            .iter()
            .any(|entry| entry.key().keyspace() == Keyspace::Meta)
    );
    assert!(
        entries
            .iter()
            .any(|entry| entry.key().keyspace() == Keyspace::Txn)
    );
    assert_eq!(adapter.applied_log_index().unwrap(), 2);
}

#[test]
fn registry_restores_a_hidden_generation_then_replays_from_its_snapshot_index() {
    let root = tempfile::tempdir().unwrap();
    let source = RocksAdapter::open(root.path().join("source")).unwrap();
    let first = batch(1, b"a", b"first");
    block_on(source.apply_committed(first.clone())).unwrap();
    let reader =
        block_on(source.begin_logical_export(LogicalSnapshotExportRequest::new(1, 1024).unwrap()))
            .unwrap();

    let target_path = root.path().join("published-target");
    let request =
        AdapterOpenRequest::new("target-1").with_parameter("path", target_path.to_str().unwrap());
    let mut registry = AdapterRegistry::new();
    registry.register(Arc::new(RocksAdapterFactory)).unwrap();
    let restored = block_on(registry.restore(
        "rocksdb",
        &request,
        AdapterRequirement::HotPluggableReplica,
        reader,
    ))
    .unwrap();

    assert!(target_path.is_dir());
    assert_eq!(restored.adapter().applied_log_index().unwrap(), 1);
    assert_eq!(
        block_on(
            restored
                .adapter()
                .multi_get(&[LogicalKey::in_keyspace(Keyspace::Current, b"a".to_vec())])
        )
        .unwrap(),
        vec![Some(b"first".to_vec())]
    );
    assert!(
        block_on(restored.adapter().apply_committed(first))
            .unwrap()
            .duplicate
    );
    block_on(
        restored
            .adapter()
            .apply_committed(batch(2, b"b", b"second")),
    )
    .unwrap();
    assert_eq!(restored.adapter().applied_log_index().unwrap(), 2);
}

#[test]
fn restore_does_not_publish_a_generation_with_a_bad_final_manifest() {
    let root = tempfile::tempdir().unwrap();
    let source = RocksAdapter::open(root.path().join("source")).unwrap();
    block_on(source.apply_committed(batch(1, b"a", b"first"))).unwrap();
    let mut reader =
        block_on(source.begin_logical_export(LogicalSnapshotExportRequest::new(2, 1024).unwrap()))
            .unwrap();
    let header = reader.header().clone();
    let mut chunks = Vec::new();
    while let Some(chunk) = block_on(reader.next_chunk()).unwrap() {
        chunks.push(chunk);
    }
    let manifest = block_on(reader.finish()).unwrap();
    let bad_manifest = LogicalSnapshotManifestV1::from_parts(
        manifest.header().clone(),
        manifest.total_chunks(),
        manifest.total_entries(),
        [0; 32],
    )
    .unwrap();

    let target_path = root.path().join("must-not-publish");
    let request =
        AdapterOpenRequest::new("bad-target").with_parameter("path", target_path.to_str().unwrap());
    let factory = RocksAdapterFactory;
    let mut restore = block_on(factory.begin_restore(&request, header)).unwrap();
    for chunk in chunks {
        block_on(restore.write_chunk(chunk.clone())).unwrap();
        block_on(restore.write_chunk(chunk)).unwrap();
        assert!(!target_path.exists());
    }
    assert!(block_on(restore.finish(bad_manifest)).is_err());
    assert!(!target_path.exists());
    assert!(std::fs::read_dir(root.path()).unwrap().all(|entry| {
        !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .contains("must-not-publish.dtg-restore")
    }));
}

#[test]
fn explicit_restore_abort_removes_the_hidden_generation() {
    let root = tempfile::tempdir().unwrap();
    let target_path = root.path().join("aborted-target");
    let request = AdapterOpenRequest::new("abort-target")
        .with_parameter("path", target_path.to_str().unwrap());
    let factory = RocksAdapterFactory;
    let restore =
        block_on(factory.begin_restore(&request, storage_api::LogicalSnapshotHeaderV1::new(77, 0)))
            .unwrap();

    assert!(root.path().read_dir().unwrap().any(|entry| {
        entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .contains("aborted-target.dtg-restore")
    }));
    block_on(restore.abort()).unwrap();
    assert!(!target_path.exists());
    assert!(root.path().read_dir().unwrap().all(|entry| {
        !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .contains("aborted-target.dtg-restore")
    }));
}

fn batch(index: u64, key: &[u8], value: &[u8]) -> CommittedMutationBatch {
    CommittedMutationBatch {
        shard_id: 1,
        log_index: index,
        txn_id: u128::from(index),
        mutations: vec![Mutation::put(
            0,
            LogicalKey::in_keyspace(Keyspace::Current, key.to_vec()),
            value.to_vec(),
        )],
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
