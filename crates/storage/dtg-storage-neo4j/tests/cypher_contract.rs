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
