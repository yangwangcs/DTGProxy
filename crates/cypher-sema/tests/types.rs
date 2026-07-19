use cypher_sema::{CypherType, SemanticAnalyzer};
use cypher_syntax::parse;

#[test]
fn infers_numeric_expression_type() {
    let query = parse("RETURN 1 + 2 * 3 AS total").expect("query should parse");
    let analyzed = SemanticAnalyzer::new()
        .analyze(&query)
        .expect("query should type-check");

    assert_eq!(analyzed.output()[0].name(), "total");
    assert_eq!(analyzed.output()[0].cypher_type(), &CypherType::Integer);
}

#[test]
fn rejects_arithmetic_over_boolean_values() {
    let query = parse("RETURN true + 1 AS invalid").expect("query should parse");
    let error = SemanticAnalyzer::new()
        .analyze(&query)
        .expect_err("boolean arithmetic must fail");

    assert_eq!(error.code(), "DTG-CYPHER-TYPE-MISMATCH");
}

#[test]
fn a_where_predicate_must_be_boolean() {
    let query = parse("MATCH (n) WHERE 42 RETURN n").expect("query should parse");
    let error = SemanticAnalyzer::new()
        .analyze(&query)
        .expect_err("WHERE must be boolean");

    assert_eq!(error.code(), "DTG-CYPHER-NON-BOOLEAN-PREDICATE");
}
