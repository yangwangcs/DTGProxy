use cypher_sema::{CypherType, SemanticAnalyzer};
use cypher_syntax::parse;

#[test]
fn variable_relationships_bind_as_ordered_relationship_lists() {
    let query = parse("MATCH (a)-[p:KNOWS*1..3]->(b) RETURN p").expect("bounded path should parse");
    let analyzed = SemanticAnalyzer::new()
        .analyze(&query)
        .expect("bounded path should analyze");

    assert_eq!(
        analyzed.output()[0].cypher_type(),
        &CypherType::List(Box::new(CypherType::Relationship))
    );
}

#[test]
fn variable_relationship_properties_are_rejected() {
    let query =
        parse("MATCH (a)-[p:KNOWS*1..3]->(b) RETURN p.weight").expect("bounded path should parse");
    let error = SemanticAnalyzer::new()
        .analyze(&query)
        .expect_err("a relationship list has no scalar property");

    assert_eq!(error.code(), "DTG-CYPHER-TYPE-MISMATCH");
}

#[test]
fn temporal_metadata_functions_have_stable_types() {
    let query = parse(
        "FOR VALID_TIME BETWEEN $from AND $to MATCH (n) \
         RETURN valid_from(n) AS vf, valid_to(n) AS vt, \
                system_time(n) AS st, commit_seq(n) AS seq, operation(n) AS op",
    )
    .expect("metadata query should parse");
    let analyzed = SemanticAnalyzer::new()
        .analyze(&query)
        .expect("metadata query should analyze");

    assert_eq!(
        analyzed
            .output()
            .iter()
            .map(|field| field.cypher_type().clone())
            .collect::<Vec<_>>(),
        vec![
            CypherType::Temporal,
            CypherType::Temporal,
            CypherType::Temporal,
            CypherType::Integer,
            CypherType::String,
        ]
    );
}

#[test]
fn temporal_metadata_requires_one_graph_value() {
    let query = parse("MATCH (n) RETURN valid_from()").expect("function syntax should parse");
    let error = SemanticAnalyzer::new()
        .analyze(&query)
        .expect_err("metadata requires provenance");

    assert_eq!(error.code(), "DTG-TEMPORAL-METADATA-ARGUMENT");
}

#[test]
fn mixed_path_changes_are_rejected_deterministically() {
    let query = parse(
        "CHANGES FOR VALID_TIME BETWEEN $from AND $to \
         MATCH (a)-[p:KNOWS*1..3]->(b)-[r:WORKS_WITH]->(c) RETURN p, r",
    )
    .expect("mixed change syntax should parse");
    let error = SemanticAnalyzer::new()
        .analyze(&query)
        .expect_err("mixed paths have no change-event ownership in this release");

    assert_eq!(error.code(), "DTG-TEMPORAL-MIXED-PATH-CHANGES");
}

#[test]
fn only_one_change_axis_is_allowed() {
    let query = parse(
        "CHANGES FOR VALID_TIME BETWEEN $from AND $to \
         CHANGES FOR SYSTEM_TIME BETWEEN $s1 AND $s2 MATCH (n) RETURN n",
    )
    .expect("orthogonal scopes should parse");
    let error = SemanticAnalyzer::new()
        .analyze(&query)
        .expect_err("one statement has one change axis");

    assert_eq!(error.code(), "DTG-TEMPORAL-MULTIPLE-CHANGE-AXES");
}
