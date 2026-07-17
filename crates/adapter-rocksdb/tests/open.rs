use adapter_rocksdb::RocksAdapter;
use storage_api::AdapterError;

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
