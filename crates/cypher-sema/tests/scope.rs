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

#[test]
fn with_star_preserves_the_current_scope_and_aliases_can_shadow_names() {
    let query = parse(
        "UNWIND [1] AS value WITH *, value + 1 AS incremented \
         WITH incremented AS value RETURN value",
    )
    .expect("query should parse");
    let analyzed = SemanticAnalyzer::new()
        .analyze(&query)
        .expect("WITH * and alias shadowing should analyze");

    assert_eq!(analyzed.output().len(), 1);
    assert_eq!(analyzed.output()[0].name(), "value");
    assert_eq!(analyzed.output()[0].cypher_type(), &CypherType::Integer);
}

#[test]
fn union_requires_matching_column_names() {
    let query = parse("RETURN 1 AS left UNION RETURN 2 AS right").expect("query should parse");
    let error = SemanticAnalyzer::new()
        .analyze(&query)
        .expect_err("UNION column names must match");

    assert_eq!(error.code(), "DTG-CYPHER-UNION-SCHEMA-NAME-MISMATCH");
}

#[test]
fn union_requires_compatible_column_types() {
    let query = parse("RETURN 1 AS value UNION RETURN 'one' AS value").expect("query should parse");
    let error = SemanticAnalyzer::new()
        .analyze(&query)
        .expect_err("UNION column types must match");

    assert_eq!(error.code(), "DTG-CYPHER-UNION-SCHEMA-TYPE-MISMATCH");
}

#[test]
fn each_union_branch_starts_from_an_isolated_scope() {
    let query = parse(
        "UNWIND [1] AS branch_only RETURN branch_only AS value \
         UNION RETURN branch_only AS value",
    )
    .expect("query should parse");
    let error = SemanticAnalyzer::new()
        .analyze(&query)
        .expect_err("a UNION branch cannot see bindings from its sibling");

    assert_eq!(error.code(), "DTG-CYPHER-UNBOUND-VARIABLE");
    assert_eq!(error.symbol(), Some("branch_only"));
}
