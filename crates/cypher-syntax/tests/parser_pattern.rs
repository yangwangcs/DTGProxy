use cypher_ast::{Expression, RelationshipDirection};
use cypher_syntax::parse_pattern;

#[test]
fn parses_labeled_property_path_and_variable_length_relationship() {
    let pattern =
        parse_pattern("(a:Account {id: $account})-[r:TRANSFER|PAYMENT*1..3]->(b:Account)")
            .expect("pattern should parse");
    let path = &pattern.paths()[0];

    assert_eq!(path.start().variable().expect("variable").value(), "a");
    assert_eq!(path.start().labels()[0].value(), "Account");
    assert!(matches!(
        path.start().properties(),
        Some(Expression::Map(_))
    ));
    let chain = &path.chains()[0];
    assert_eq!(
        chain.relationship().direction(),
        RelationshipDirection::Outgoing
    );
    assert_eq!(
        chain.relationship().variable().expect("variable").value(),
        "r"
    );
    assert_eq!(
        chain
            .relationship()
            .types()
            .iter()
            .map(|identifier| identifier.value())
            .collect::<Vec<_>>(),
        vec!["TRANSFER", "PAYMENT"]
    );
    let length = chain.relationship().length().expect("length");
    assert_eq!(length.minimum(), Some(1));
    assert_eq!(length.maximum(), Some(3));
    assert_eq!(chain.node().variable().expect("variable").value(), "b");
}

#[test]
fn parses_incoming_and_undirected_relationships() {
    let pattern = parse_pattern("(a)<-[:PARENT]-(b)--(c)").expect("pattern should parse");
    let chains = pattern.paths()[0].chains();

    assert_eq!(
        chains[0].relationship().direction(),
        RelationshipDirection::Incoming
    );
    assert_eq!(
        chains[1].relationship().direction(),
        RelationshipDirection::Undirected
    );
}

#[test]
fn rejects_a_path_without_a_terminal_node() {
    let error = parse_pattern("(a)-[:EDGE]->").expect_err("terminal node is required");

    assert_eq!(error.code(), "DTG-CYPHER-EXPECTED-NODE-PATTERN");
}
