use storage_api::{
    ADAPTER_META_APPLIED_LOG_INDEX_KEY, Keyspace, LogicalKey, adapter_log_fingerprint_key,
    adapter_mutation_fingerprint_key,
};

#[test]
fn keyspace_tags_and_column_families_are_stable() {
    assert_eq!(Keyspace::Meta.tag(), 0);
    assert_eq!(Keyspace::Identity.tag(), 1);
    assert_eq!(Keyspace::Current.column_family(), "current");
    assert_eq!(Keyspace::AdjOut.column_family(), "adj_out");
    assert_eq!(Keyspace::AdjIn.column_family(), "adj_in");
    assert_eq!(Keyspace::History.column_family(), "history");
    assert_eq!(Keyspace::TemporalIndex.column_family(), "temporal_index");
    assert_eq!(Keyspace::Txn.column_family(), "txn");
}

#[test]
fn logical_key_defaults_to_current_and_can_select_history() {
    let current = LogicalKey::new(b"same".to_vec());
    let history = LogicalKey::in_keyspace(Keyspace::History, b"same".to_vec());

    assert_eq!(current.keyspace(), Keyspace::Current);
    assert_eq!(history.keyspace(), Keyspace::History);
    assert_ne!(current, history);
}

#[test]
fn adapter_replay_metadata_keys_are_stable_across_backend_families() {
    assert_eq!(ADAPTER_META_APPLIED_LOG_INDEX_KEY, b"\x00applied_log_index");
    assert_eq!(
        adapter_log_fingerprint_key(0x0102_0304_0506_0708),
        [1, 1, 2, 3, 4, 5, 6, 7, 8]
    );
    let key =
        adapter_mutation_fingerprint_key(0x0102_0304_0506_0708_1112_1314_1516_1718, 0x2122_2324);
    assert_eq!(
        key,
        [
            2, 1, 2, 3, 4, 5, 6, 7, 8, 17, 18, 19, 20, 21, 22, 23, 24, 33, 34, 35, 36
        ]
    );
}
