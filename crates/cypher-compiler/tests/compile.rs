use cypher_compiler::{CompileSession, CypherCompiler};
use cypher_sema::QueryEffect;
use temporal_ir::v2::{
    LanguageProfile, LogicalOperator, ScalarExpr, TransactionTimeSpec, ValidTimeSpec,
};

fn session() -> CompileSession {
    CompileSession::new("accounts", 7, 3, 11).expect("session should be valid")
}

#[test]
fn compiles_temporal_match_into_valid_ir_v2() {
    let compiled = CypherCompiler::new()
        .compile(
            "CYPHER 25 USE accounts \
             AT VALID_TIME AS OF $valid \
             AT TRANSACTION_TIME AS OF $tx \
             MATCH (a:Account)-[r:TRANSFER]->(b:Account) \
             WHERE a.active = true RETURN a, r, b",
            &session(),
        )
        .expect("query should compile");

    compiled
        .logical_plan()
        .validate()
        .expect("compiled plan should validate");
    assert_eq!(
        compiled.logical_plan().header().language_profile(),
        LanguageProfile::Cypher25
    );
    assert_eq!(compiled.logical_plan().header().graph_id(), 7);
    assert_eq!(compiled.effect(), QueryEffect::ReadOnly);
    let operators = compiled
        .logical_plan()
        .nodes()
        .iter()
        .map(|node| node.operator())
        .collect::<Vec<_>>();
    assert!(operators.iter().any(|operator| matches!(
        operator,
        LogicalOperator::TemporalSlice { valid_time, transaction_time }
            if valid_time == &ValidTimeSpec::AsOf(ScalarExpr::Parameter("valid".into()))
                && transaction_time
                    == &TransactionTimeSpec::AsOf(ScalarExpr::Parameter("tx".into()))
    )));
    assert!(
        operators
            .iter()
            .any(|operator| matches!(operator, LogicalOperator::NodeScan { .. }))
    );
    assert!(
        operators
            .iter()
            .any(|operator| matches!(operator, LogicalOperator::Expand { .. }))
    );
    assert!(
        operators
            .iter()
            .any(|operator| matches!(operator, LogicalOperator::Filter { .. }))
    );
    assert_eq!(compiled.result_schema().columns().len(), 3);
}

#[test]
fn preserves_interval_and_current_transaction_time_in_ir() {
    let compiled = CypherCompiler::new()
        .compile(
            "USE accounts AT VALID_TIME FROM $from TO $to MATCH (n) RETURN n",
            &session(),
        )
        .expect("interval query should compile");

    assert!(compiled.logical_plan().nodes().iter().any(|node| matches!(
        node.operator(),
        LogicalOperator::TemporalSlice { valid_time, transaction_time }
            if valid_time == &ValidTimeSpec::Between {
                start: ScalarExpr::Parameter("from".into()),
                end: ScalarExpr::Parameter("to".into()),
            } && transaction_time == &TransactionTimeSpec::Current
    )));
}

#[test]
fn fingerprints_are_profile_specific() {
    let compiler = CypherCompiler::new();
    let cypher_5 = compiler
        .compile("CYPHER 5 RETURN 1 AS value", &session())
        .expect("Cypher 5 should compile");
    let cypher_25 = compiler
        .compile("CYPHER 25 RETURN 1 AS value", &session())
        .expect("Cypher 25 should compile");

    assert_ne!(cypher_5.fingerprint(), cypher_25.fingerprint());
    assert_ne!(
        cypher_5.logical_plan().header().language_profile(),
        cypher_25.logical_plan().header().language_profile()
    );
}

#[test]
fn rejects_a_query_for_another_graph() {
    let error = CypherCompiler::new()
        .compile("USE inventory MATCH (n) RETURN n", &session())
        .expect_err("graph mismatch must fail");

    assert_eq!(error.code(), "DTG-CYPHER-GRAPH-MISMATCH");
}
