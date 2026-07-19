use cypher_ast::{ClauseKind, Expression, Statement, TransactionTimeScope, ValidTimeScope};
use cypher_syntax::parse;

#[test]
fn parses_bitemporal_as_of_scope() {
    let parsed = parse(
        "USE accounts\n\
         AT VALID_TIME AS OF $valid_time\n\
         AT TRANSACTION_TIME AS OF $transaction_time\n\
         MATCH (a:Account)-[e:TRANSFER]->(b:Account)\n\
         RETURN a, e, b",
    )
    .expect("temporal query should parse");
    let Statement::Query(query) = parsed.statement() else {
        panic!("expected query statement");
    };

    assert_eq!(query.graph().expect("graph").value(), "accounts");
    assert_eq!(
        query.temporal().valid_time(),
        Some(&ValidTimeScope::AsOf(Expression::Parameter(
            "valid_time".into()
        )))
    );
    assert_eq!(
        query.temporal().transaction_time(),
        &TransactionTimeScope::AsOf(Expression::Parameter("transaction_time".into()))
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
fn parses_valid_time_interval() {
    let parsed = parse(
        "USE accounts AT VALID_TIME FROM $from TO $to \
         AT TRANSACTION_TIME AS OF $tx MATCH (n) RETURN n",
    )
    .expect("interval query should parse");
    let Statement::Query(query) = parsed.statement() else {
        panic!("expected query statement");
    };

    assert_eq!(
        query.temporal().valid_time(),
        Some(&ValidTimeScope::Between {
            start: Expression::Parameter("from".into()),
            end: Expression::Parameter("to".into()),
        })
    );
}

#[test]
fn parses_diff_as_a_first_class_statement() {
    let parsed = parse(
        "DIFF GRAPH accounts \
         AT VALID_TIME AS OF $t1 AND AS OF $t2 \
         AT TRANSACTION_TIME AS OF $tx \
         YIELD element, changeType, before, after",
    )
    .expect("diff should parse");
    let Statement::Diff(diff) = parsed.statement() else {
        panic!("expected diff statement");
    };

    assert_eq!(diff.graph().value(), "accounts");
    assert_eq!(diff.from_valid_time(), &Expression::Parameter("t1".into()));
    assert_eq!(diff.to_valid_time(), &Expression::Parameter("t2".into()));
    assert_eq!(
        diff.transaction_time(),
        &TransactionTimeScope::AsOf(Expression::Parameter("tx".into()))
    );
    assert_eq!(
        diff.yield_items(),
        ["element", "changeType", "before", "after"]
    );
}
