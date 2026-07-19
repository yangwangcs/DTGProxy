use cypher_sema::{CypherType, QueryEffect, SemanticAnalyzer};
use cypher_syntax::parse;

#[test]
fn let_and_unwind_bind_typed_variables() {
    let query = parse("LET values = [1, 2, 3] UNWIND values AS item RETURN item")
        .expect("query should parse");
    let analyzed = SemanticAnalyzer::new()
        .analyze(&query)
        .expect("query should analyze");

    assert_eq!(analyzed.output()[0].name(), "item");
    assert_eq!(analyzed.output()[0].cypher_type(), &CypherType::Integer);
}

#[test]
fn write_clauses_mark_the_query_as_writing() {
    let query = parse("MATCH (n) SET n.active = true RETURN n").expect("query should parse");
    let analyzed = SemanticAnalyzer::new()
        .analyze(&query)
        .expect("query should analyze");

    assert_eq!(analyzed.effect(), QueryEffect::Write);
}

#[test]
fn historical_transaction_time_is_read_only() {
    let query = parse(
        "AT TRANSACTION_TIME AS OF $historical \
         MATCH (n) SET n.active = true RETURN n",
    )
    .expect("query should parse");
    let error = SemanticAnalyzer::new()
        .analyze(&query)
        .expect_err("historical snapshots cannot be written");

    assert_eq!(error.code(), "DTG-TEMPORAL-HISTORICAL-WRITE");
}

#[test]
fn duplicate_projection_aliases_are_rejected() {
    let query = parse("RETURN 1 AS value, 2 AS value").expect("query should parse");
    let error = SemanticAnalyzer::new()
        .analyze(&query)
        .expect_err("duplicate output must fail");

    assert_eq!(error.code(), "DTG-CYPHER-DUPLICATE-OUTPUT");
}
