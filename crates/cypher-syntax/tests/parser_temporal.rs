use cypher_ast::{ClauseKind, Expression, Statement, TemporalAxis, TemporalMode, TemporalScope};
use cypher_syntax::parse;

#[test]
fn parses_cedar_bitemporal_as_of_scopes() {
    let parsed = parse(
        "USE accounts\n\
         FOR SYSTEM_TIME AS OF $system_time\n\
         FOR VALID_TIME AS OF $valid_time\n\
         MATCH (a:Account)-[e:TRANSFER]->(b:Account)\n\
         RETURN a, e, b",
    )
    .expect("Cedar temporal query should parse");
    let Statement::Query(query) = parsed.statement();

    assert_eq!(query.graph().expect("graph").value(), "accounts");
    assert_eq!(
        query.temporal().scopes(),
        [
            TemporalScope::as_of(
                TemporalAxis::SystemTime,
                Expression::Parameter("system_time".into()),
            ),
            TemporalScope::as_of(
                TemporalAxis::ValidTime,
                Expression::Parameter("valid_time".into()),
            ),
        ]
    );
    assert_eq!(
        query
            .clauses()
            .iter()
            .map(|clause| clause.kind())
            .collect::<Vec<_>>(),
        vec![ClauseKind::Match, ClauseKind::Return]
    );
}

#[test]
fn parses_cedar_valid_time_state_range() {
    let parsed = parse(
        "USE accounts FOR VALID_TIME BETWEEN $from AND $to \
         FOR SYSTEM_TIME AS OF $tx MATCH (n) RETURN n",
    )
    .expect("interval query should parse");
    let Statement::Query(query) = parsed.statement();

    assert_eq!(
        query.temporal().scopes(),
        [
            TemporalScope::between(
                TemporalAxis::ValidTime,
                TemporalMode::StateBetween,
                Expression::Parameter("from".into()),
                Expression::Parameter("to".into()),
            ),
            TemporalScope::as_of(TemporalAxis::SystemTime, Expression::Parameter("tx".into()),),
        ]
    );
}

#[test]
fn parses_changes_as_the_same_query_statement() {
    let parsed = parse(
        "USE accounts CHANGES FOR VALID_TIME BETWEEN $from AND $to \
         FOR SYSTEM_TIME AS OF $tx MATCH (n:Person) \
         RETURN n, operation(n), commit_seq(n)",
    )
    .expect("change query should parse");
    let Statement::Query(query) = parsed.statement();

    assert_eq!(
        query.temporal().scopes(),
        [
            TemporalScope::between(
                TemporalAxis::ValidTime,
                TemporalMode::ChangesBetween,
                Expression::Parameter("from".into()),
                Expression::Parameter("to".into()),
            ),
            TemporalScope::as_of(TemporalAxis::SystemTime, Expression::Parameter("tx".into()),),
        ]
    );
}

#[test]
fn rejects_duplicate_temporal_axis() {
    let error = parse("FOR VALID_TIME AS OF $a FOR VALID_TIME AS OF $b MATCH (n) RETURN n")
        .expect_err("duplicate valid-time scopes must fail");
    assert_eq!(error.code(), "DTG-CYPHER-DUPLICATE-TEMPORAL-SCOPE");
}

#[test]
fn rejects_old_at_temporal_syntax() {
    let error = parse("AT VALID_TIME AS OF $valid MATCH (n) RETURN n")
        .expect_err("old AT syntax must not remain executable");
    assert_eq!(error.code(), "DTG-CYPHER-REMOVED-TEMPORAL-SYNTAX");
}

#[test]
fn rejects_old_diff_graph_syntax() {
    let error = parse(
        "DIFF GRAPH accounts AT VALID_TIME AS OF $a AND AS OF $b \
         AT TRANSACTION_TIME AS OF $tx YIELD element",
    )
    .expect_err("old DIFF syntax must not remain executable");
    assert_eq!(error.code(), "DTG-CYPHER-REMOVED-TEMPORAL-SYNTAX");
}
