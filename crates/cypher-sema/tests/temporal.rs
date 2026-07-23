use cypher_sema::{CypherType, QueryEffect, SemanticAnalyzer};
use cypher_syntax::parse;

#[test]
fn changes_is_a_read_only_query_with_the_declared_projection() {
    let query = parse(
        "USE accounts CHANGES FOR VALID_TIME BETWEEN $a AND $b \
         FOR SYSTEM_TIME AS OF $tx \
         MATCH (n) RETURN n",
    )
    .expect("CHANGES should parse");
    let analyzed = SemanticAnalyzer::new()
        .analyze(&query)
        .expect("CHANGES should analyze");

    assert_eq!(analyzed.effect(), QueryEffect::ReadOnly);
    assert_eq!(
        analyzed
            .output()
            .iter()
            .map(|field| (field.name(), field.cypher_type()))
            .collect::<Vec<_>>(),
        vec![("n", &CypherType::Node)]
    );
}

#[test]
fn backdated_valid_time_update_is_allowed() {
    let query = parse(
        "FOR VALID_TIME AS OF $business_time \
         MATCH (n) SET n.status = 'corrected' RETURN n",
    )
    .expect("query should parse");
    let analyzed = SemanticAnalyzer::new()
        .analyze(&query)
        .expect("valid-time correction should analyze");

    assert_eq!(analyzed.effect(), QueryEffect::Write);
}
