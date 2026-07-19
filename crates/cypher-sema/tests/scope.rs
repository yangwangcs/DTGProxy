use cypher_sema::{CypherType, SemanticAnalyzer};
use cypher_syntax::parse;

#[test]
fn with_replaces_the_visible_scope_and_preserves_aliases() {
    let query =
        parse("MATCH (n:Account) WITH n AS account RETURN account").expect("query should parse");
    let analyzed = SemanticAnalyzer::new()
        .analyze(&query)
        .expect("query should bind");

    assert_eq!(analyzed.output().len(), 1);
    assert_eq!(analyzed.output()[0].name(), "account");
    assert_eq!(analyzed.output()[0].cypher_type(), &CypherType::Node);
}

#[test]
fn with_hides_variables_that_are_not_projected() {
    let query = parse("MATCH (n) WITH n AS account RETURN n").expect("query should parse");
    let error = SemanticAnalyzer::new()
        .analyze(&query)
        .expect_err("n is out of scope");

    assert_eq!(error.code(), "DTG-CYPHER-UNBOUND-VARIABLE");
    assert_eq!(error.symbol(), Some("n"));
}

#[test]
fn relationship_and_node_variables_have_distinct_types() {
    let query = parse("MATCH (a)-[r:TRANSFER]->(b) RETURN a, r, b").expect("query should parse");
    let analyzed = SemanticAnalyzer::new()
        .analyze(&query)
        .expect("query should bind");

    assert_eq!(
        analyzed
            .output()
            .iter()
            .map(|field| field.cypher_type())
            .collect::<Vec<_>>(),
        vec![
            &CypherType::Node,
            &CypherType::Relationship,
            &CypherType::Node
        ]
    );
}
