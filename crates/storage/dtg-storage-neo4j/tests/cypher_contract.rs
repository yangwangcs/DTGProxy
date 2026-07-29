#![forbid(unsafe_code)]

#[test]
fn every_read_query_is_owner_fenced_and_bounded() {
    let source = include_str!("../src/read_view.rs");
    let query_count = source.matches(".execute(").count();
    assert_eq!(query_count, 13, "unexpected read query surface change");
    assert_eq!(source.matches("MATCH (owner:DtgOwner").count(), query_count);
    assert_eq!(source.matches("LIMIT $limit").count(), query_count);
    assert!(!source.contains("CanonicalKv"));
    assert!(!source.contains("CANONICAL_ENTRY"));
}

#[test]
fn apply_uses_one_explicit_transaction_and_never_a_mirror() {
    let source = include_str!("../src/apply.rs");
    assert!(source.contains("begin_fenced_transaction"));
    assert!(source.contains("transaction.commit().await"));
    assert!(source.contains("transaction.rollback().await"));
    assert!(source.contains("DtgVertex"));
    assert!(source.contains("DtgVersion"));
    assert!(source.contains("DTG_EDGE"));
    assert!(source.contains("DtgChange"));
    assert!(!source.contains("CanonicalKv"));
    assert!(!source.contains("Sidecar"));
    assert!(!source.contains("adapter_neo4j"));
}

#[test]
fn candidate_restore_and_activation_are_owner_fenced_atomic_and_exact() {
    let source = include_str!("../src/snapshot.rs");

    for marker_field in [
        "candidate_binding_digest",
        "snapshot_format_version",
        "snapshot_id",
        "snapshot_applied_index",
        "snapshot_chunk_count",
        "snapshot_record_count",
        "snapshot_content_digest",
    ] {
        assert!(
            source.contains(marker_field),
            "missing durable candidate marker field {marker_field}"
        );
    }
    assert!(source.contains("BindingRole::Candidate"));
    assert!(source.contains("CANDIDATE_SNAPSHOT_PUBLISH_QUERY"));
    assert!(source.contains("ACTIVE_SNAPSHOT_PUBLISH_QUERY"));
    assert!(source.contains("MATCH (install:DtgSnapshotInstall"));
    assert!(source.contains("DELETE install"));
    assert!(source.contains("SET owner.binding_role = $active_binding_role"));
    assert!(source.contains("owner.binding_digest = $active_binding_digest"));
    assert!(source.contains("DtgSnapshotActivation"));
    assert!(source.contains("LogicalReplicaActivationReceipt::new"));
    assert!(source.contains("read_applied_index(&client, store.binding_ref()).await?"));
    assert!(source.contains("Neo4j snapshot abort lost its owner fence"));
    assert!(!source.contains("CanonicalKv"));
    assert!(!source.contains("Sidecar"));
}
