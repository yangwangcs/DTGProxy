use cypher_sema::{CypherType, QueryEffect, SemanticAnalyzer};
use cypher_syntax::parse;

#[test]
fn diff_has_a_stable_four_column_schema() {
    let query = parse(
        "DIFF GRAPH accounts AT VALID_TIME AS OF $a AND AS OF $b \
         AT TRANSACTION_TIME AS OF $tx \
         YIELD element, changeType, before, after",
    )
    .expect("DIFF should parse");
    let analyzed = SemanticAnalyzer::new()
        .analyze(&query)
        .expect("DIFF should analyze");

    assert_eq!(analyzed.effect(), QueryEffect::ReadOnly);
    assert_eq!(
        analyzed
            .output()
            .iter()
            .map(|field| (field.name(), field.cypher_type()))
            .collect::<Vec<_>>(),
        vec![
            ("element", &CypherType::Any),
            ("changeType", &CypherType::Any),
            ("before", &CypherType::Any),
            ("after", &CypherType::Any),
        ]
    );
}

#[test]
fn backdated_valid_time_update_is_allowed() {
    let query = parse(
        "AT VALID_TIME AS OF $business_time \
         MATCH (n) SET n.status = 'corrected' RETURN n",
    )
    .expect("query should parse");
    let analyzed = SemanticAnalyzer::new()
        .analyze(&query)
        .expect("valid-time correction should analyze");

    assert_eq!(analyzed.effect(), QueryEffect::Write);
}
