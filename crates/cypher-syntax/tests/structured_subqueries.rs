use cypher_ast::{ClauseKind, Expression, Statement, SubqueryErrorPolicy};
use cypher_syntax::{parse, parse_expression};

#[test]
fn parses_nested_call_subquery_with_imports_and_batch_spec() {
    let parsed = parse(
        "UNWIND [1] AS outer_value \
         CALL (outer_value) { \
           CALL () { RETURN {nested: {value: 1}} AS inner_value } \
           RETURN outer_value AS exported_value \
         } IN TRANSACTIONS OF 32 ROWS \
         RETURN exported_value",
    )
    .expect("scoped nested CALL subquery should parse");
    let Statement::Query(query) = parsed.statement() else {
        panic!("expected query statement");
    };
    let call = query.clauses()[1]
        .call_subquery()
        .expect("structured CALL subquery");

    assert_eq!(call.imports()[0].value(), "outer_value");
    assert_eq!(call.exports()[0].value(), "exported_value");
    assert_eq!(call.query().clauses()[0].kind(), ClauseKind::Call);
    assert!(call.query().clauses()[0].call_subquery().is_some());
    let batch = call.in_transactions().expect("batch specification");
    assert_eq!(batch.batch_rows(), 32);
    assert_eq!(batch.error_policy(), SubqueryErrorPolicy::Fail);
}

#[test]
fn parses_exists_and_count_subquery_expressions() {
    let exists = parse_expression("EXISTS { MATCH (n) WHERE n.active = true RETURN n }")
        .expect("EXISTS subquery expression should parse");
    let count = parse_expression("COUNT { UNWIND [1, 2] AS value RETURN value }")
        .expect("COUNT subquery expression should parse");

    assert!(matches!(
        exists,
        Expression::ExistsSubquery(query)
            if query.clauses().iter().any(|clause| clause.kind() == ClauseKind::Match)
    ));
    assert!(matches!(
        count,
        Expression::CountSubquery(query)
            if query.clauses().iter().any(|clause| clause.kind() == ClauseKind::Unwind)
    ));
}

#[test]
fn rejects_unbalanced_subquery_suffix_invalid_batch_size_and_legacy_call_form() {
    for (query, expected_code) in [
        (
            "CALL () { RETURN 1 AS value",
            "DTG-CYPHER-UNBALANCED-DELIMITER",
        ),
        (
            "CALL () { RETURN 1 AS value } trailing",
            "DTG-CYPHER-INVALID-SUBQUERY-SUFFIX",
        ),
        (
            "CALL () { RETURN 1 AS value } IN TRANSACTIONS OF 0 ROWS",
            "DTG-CYPHER-INVALID-SUBQUERY-BATCH",
        ),
        (
            "CALL () { RETURN 1 AS value } IN TRANSACTIONS OF $rows ROWS",
            "DTG-CYPHER-INVALID-SUBQUERY-BATCH",
        ),
        (
            "CALL () { RETURN 1 AS value } IN TRANSACTIONS OF 4294967296 ROWS",
            "DTG-CYPHER-INVALID-SUBQUERY-BATCH",
        ),
        (
            "CALL { RETURN 1 AS value }",
            "DTG-CYPHER-SCOPED-SUBQUERY-REQUIRED",
        ),
    ] {
        let error = parse(query).expect_err("invalid subquery form must be rejected");
        assert_eq!(error.code(), expected_code, "{query}");
    }
}
