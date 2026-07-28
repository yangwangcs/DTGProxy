use adapter_neo4j::CANONICAL_BATCH_SCAN_CYPHER;

#[test]
fn canonical_batch_scan_is_one_ordinal_preserving_unwind_query() {
    let query = CANONICAL_BATCH_SCAN_CYPHER;

    assert_eq!(query.matches("UNWIND $ranges AS range").count(), 1);
    assert!(query.contains("range.ordinal"));
    assert!(query.contains("range.keyspace"));
    assert!(query.contains("range.start_hex"));
    assert!(query.contains("range.end_hex"));
    assert!(query.contains("range.required_prefix_hex"));
    assert!(query.contains("range.max_items"));
    assert!(!query.contains("range.max_bytes"));
    assert!(query.contains("ORDER BY ordinal, logical_key_hex"));
    assert!(!query.contains("$keyspace"));
}
