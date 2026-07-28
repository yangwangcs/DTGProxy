use adapter_postgres::POSTGRES_CANONICAL_BATCH_SCAN_SQL;

#[test]
fn canonical_batch_scan_is_one_parameterized_ordinal_preserving_statement() {
    let statement = POSTGRES_CANONICAL_BATCH_SCAN_SQL;

    assert!(statement.contains("unnest("));
    assert!(statement.contains("WITH ORDINALITY"));
    assert!(statement.contains("JOIN LATERAL"));
    assert!(statement.contains("ORDER BY input_ordinal, logical_key"));
    for parameter in [
        "$2::bigint[]",
        "$3::smallint[]",
        "$4::bytea[]",
        "$5::bytea[]",
        "$6::bytea[]",
        "$7::bigint[]",
        "$8::bigint[]",
    ] {
        assert!(
            statement.contains(parameter),
            "missing parameter {parameter}"
        );
    }
    assert!(statement.contains("required_prefix"));
    assert!(statement.contains("max_items"));
    assert!(statement.contains("max_bytes"));
    assert!(statement.contains("retained_bytes <= max_bytes"));
    assert_eq!(statement.matches("WITH canonical_keys").count(), 1);
}
