use cypher_ast::{ClauseKind, CypherVersion, Expression, ProcedureYield, Statement};
use cypher_syntax::parse;

#[test]
fn cypher_25_accepts_let_clause() {
    let parsed = parse("CYPHER 25 LET x = 1 RETURN x").expect("LET is a Cypher 25 clause");
    let Statement::Query(query) = parsed.statement();

    assert_eq!(parsed.profile().version(), CypherVersion::V25);
    assert_eq!(query.clauses()[0].kind(), ClauseKind::Let);
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
    let Statement::Query(query) = parsed.statement();

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

#[test]
fn parses_call_yield_into_a_structured_procedure_descriptor() {
    let parsed = parse(
        "CALL dtg.graph.degree({weighted: true}) YIELD vertexId, degree AS score RETURN score",
    )
    .expect("CALL YIELD should parse");
    let Statement::Query(query) = parsed.statement();
    let call = query.clauses()[0]
        .procedure()
        .expect("procedure descriptor");
    assert_eq!(call.name(), "dtg.graph.degree");
    assert!(matches!(call.arguments(), [Expression::Map(_)]));
    let ProcedureYield::Items(items) = call.yield_selection() else {
        panic!("expected explicit YIELD items");
    };
    assert_eq!(items.len(), 2);
    assert_eq!(items[0].name(), "vertexId");
    assert_eq!(items[1].alias(), Some("score"));
}

#[test]
fn parses_qualified_call_typed_arguments_yield_star_and_aliases() {
    let parsed =
        parse("CALL dtg.graph.degree({weighted: true, nested: [1, $limit]}, $fallback) YIELD *")
            .expect("structured CALL should parse");
    let Statement::Query(query) = parsed.statement();
    let procedure = query.clauses()[0].procedure().expect("procedure call");

    assert_eq!(procedure.name(), "dtg.graph.degree");
    assert_eq!(procedure.arguments().len(), 2);
    assert!(matches!(
        &procedure.arguments()[0],
        Expression::Map(items)
            if matches!(&items[1].1, Expression::List(values)
                if matches!(&values[1], Expression::Parameter(name) if name == "limit"))
    ));
    assert!(matches!(procedure.yield_selection(), ProcedureYield::All));

    let parsed =
        parse("CALL dtg.graph.degree({}) YIELD vertexId, degree AS score RETURN vertexId, score")
            .expect("explicit YIELD aliases should parse");
    let Statement::Query(query) = parsed.statement();
    let procedure = query.clauses()[0].procedure().expect("procedure call");
    assert!(matches!(
        procedure.yield_selection(),
        ProcedureYield::Items(items)
            if items.len() == 2
                && items[0].name() == "vertexId"
                && items[1].name() == "degree"
                && items[1].alias() == Some("score")
    ));
}

#[test]
fn rejects_call_yield_after_nested_argument_and_duplicate_alias() {
    let error = parse(
        "CALL dtg.graph.degree({nested: [1, {enabled: true}]}) \
         YIELD vertexId AS result, degree AS result RETURN result",
    )
    .expect_err("duplicate YIELD aliases must be rejected by the parser");

    assert_eq!(error.code(), "DTG-CYPHER-DUPLICATE-YIELD");
}

#[test]
fn parses_mixed_case_call_keyword() {
    let parsed = parse("cAlL dtg.graph.degree() YIELD degree RETURN degree")
        .expect("Cypher keywords are case-insensitive");
    let Statement::Query(query) = parsed.statement();
    assert_eq!(
        query.clauses()[0].procedure().unwrap().name(),
        "dtg.graph.degree"
    );
}

#[test]
fn parses_a_read_subquery_as_a_structured_call_clause() {
    let parsed = parse("CALL () { UNWIND [1, 2] AS value RETURN value } RETURN value")
        .expect("CALL subquery should parse");
    let Statement::Query(query) = parsed.statement();

    assert_eq!(query.clauses()[0].kind(), ClauseKind::Call);
    let subquery = query.clauses()[0]
        .call_subquery()
        .expect("structured subquery");
    assert!(subquery.imports().is_empty());
    assert_eq!(subquery.exports()[0].value(), "value");
}

#[test]
fn rejects_an_incomplete_yield_alias() {
    let error = parse("CALL dtg.graph.degree() YIELD degree AS RETURN degree")
        .expect_err("malformed YIELD items must not be silently ignored");
    assert_eq!(error.code(), "DTG-CYPHER-INVALID-YIELD");
}

#[test]
fn parses_escaped_yield_identifiers_and_aliases() {
    let parsed = parse("CALL dtg.graph.degree() YIELD degree AS `my score` RETURN `my score`")
        .expect("escaped Cypher identifiers are valid in YIELD");
    let Statement::Query(query) = parsed.statement();
    let procedure = query.clauses()[0].procedure().expect("procedure call");
    let cypher_ast::ProcedureYield::Items(items) = procedure.yield_selection() else {
        panic!("explicit YIELD items");
    };

    assert_eq!(items[0].name(), "degree");
    assert_eq!(items[0].alias(), Some("my score"));
}
