use cypher_ast::{ClauseKind, CypherVersion, Statement};
use cypher_syntax::parse;

#[test]
fn cypher_25_accepts_let_clause() {
    let parsed = parse("CYPHER 25 LET x = 1 RETURN x").expect("LET is a Cypher 25 clause");
    let Statement::Query(query) = parsed.statement() else {
        panic!("expected query statement");
    };

    assert_eq!(parsed.profile().version(), CypherVersion::V25);
    assert_eq!(query.clauses()[0].kind(), ClauseKind::Let);
}

#[test]
fn cypher_5_rejects_let_clause() {
    let error = parse("CYPHER 5 LET x = 1 RETURN x").expect_err("LET is unavailable in Cypher 5");

    assert_eq!(error.code(), "DTG-CYPHER-FEATURE-NOT-IN-PROFILE");
}

#[test]
fn recognizes_stable_standard_clause_families() {
    let parsed = parse(
        "MATCH (n) WHERE n.active = true \
         WITH n UNWIND n.items AS item \
         CREATE (m:Seen) SET m.value = item REMOVE m.old \
         MERGE (x:Index {value: item}) \
         DETACH DELETE m CALL dtg.algo.list() YIELD name RETURN name",
    )
    .expect("standard clause families should parse");
    let Statement::Query(query) = parsed.statement() else {
        panic!("expected query statement");
    };

    assert_eq!(
        query
            .clauses()
            .iter()
            .map(|clause| clause.kind())
            .collect::<Vec<_>>(),
        vec![
            ClauseKind::Match,
            ClauseKind::Where,
            ClauseKind::With,
            ClauseKind::Unwind,
            ClauseKind::Create,
            ClauseKind::Set,
            ClauseKind::Remove,
            ClauseKind::Merge,
            ClauseKind::Delete { detach: true },
            ClauseKind::Call,
            ClauseKind::Yield,
            ClauseKind::Return,
        ]
    );
}
