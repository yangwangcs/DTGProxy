use storage_api::{Keyspace, LogicalKey};

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

