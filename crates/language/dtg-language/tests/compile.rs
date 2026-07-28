use dtg_language::{EmptySchemaCatalog, SchemaCatalog, compile};
use dtg_language_ir::{
    GraphId, GraphScope, LogicalNodeKind, LogicalStatement, TemporalScope, TimeExpr,
    ValidTimePredicate,
};

struct Catalog;
impl SchemaCatalog for Catalog {
    fn graph_id(&self, name: &str) -> Option<GraphId> {
        (name == "accounts").then(|| GraphId::new(7).unwrap())
    }
}

#[test]
fn as_of_query_normalizes_to_explicit_scope() {
    let program = compile(
        "MATCH (n) FOR SYSTEM_TIME AS OF $t RETURN n",
        &EmptySchemaCatalog,
    )
    .unwrap();
    let LogicalStatement::Query(plan) = program.statement else {
        panic!("expected query");
    };
    assert!(plan.contains_scope(TemporalScope::AsOf(TimeExpr::Parameter("t".into()))));
}

#[test]
fn arbitrary_call_is_rejected_at_compile_time() {
    let error = compile("CALL user.code()", &EmptySchemaCatalog).unwrap_err();
    assert_eq!(error.code(), "DTG-LANG-UNKNOWN-BUILTIN");
}

#[test]
fn valid_time_between_normalizes_to_overlap_predicate() {
    let program = compile(
        "FOR VALID_TIME BETWEEN 10 AND 20 MATCH (n:Person) RETURN n",
        &EmptySchemaCatalog,
    )
    .unwrap();
    let LogicalStatement::Query(plan) = program.statement else {
        panic!("expected query");
    };
    let LogicalNodeKind::NodeScan(scan) = &plan.nodes[0].kind else {
        panic!("expected a node scan");
    };
    assert!(matches!(
        scan.read_scope.valid_time,
        Some(ValidTimePredicate::Overlaps(_))
    ));
}

#[test]
fn two_parameter_valid_time_range_reports_the_ir_gap() {
    let error = compile(
        "FOR VALID_TIME BETWEEN $from AND $to MATCH (n) RETURN n",
        &EmptySchemaCatalog,
    )
    .unwrap_err();
    assert_eq!(error.code(), "DTG-LANG-IR-MISMATCH");
}

#[test]
fn use_scope_and_match_override_are_normalized() {
    let program = compile(
        "USE accounts FOR SYSTEM_TIME AS OF 5 MATCH (a) MATCH (b) FOR SYSTEM_TIME AS OF 7 RETURN b",
        &Catalog,
    )
    .unwrap();
    assert_eq!(
        program.graph_scope,
        GraphScope::Explicit(GraphId::new(7).unwrap())
    );
    let LogicalStatement::Query(plan) = program.statement else {
        panic!("expected query");
    };
    assert!(plan.contains_scope(TemporalScope::AsOf(TimeExpr::Literal(
        dtg_language_ir::TransactionTime::new(7).unwrap()
    ))));
}

#[test]
fn system_time_changes_normalizes_to_change_scope() {
    let program = compile(
        "CHANGES FOR SYSTEM_TIME BETWEEN $from AND $to MATCH (n) RETURN n",
        &EmptySchemaCatalog,
    )
    .unwrap();
    let LogicalStatement::Query(plan) = program.statement else {
        panic!("expected query");
    };
    assert!(plan.contains_scope(TemporalScope::Changes {
        from: TimeExpr::Parameter("from".into()),
        to: TimeExpr::Parameter("to".into())
    }));
}

#[test]
fn valid_from_writes_are_parsed_but_report_the_open_interval_ir_gap() {
    for source in [
        "CREATE (n:Person {name: 'Li'}) VALID FROM $t",
        "MATCH (n:Person) SET n.name = 'Wang' VALID FROM $t",
        "MATCH (n:Person) DELETE n VALID FROM $t",
    ] {
        let error = compile(source, &EmptySchemaCatalog).unwrap_err();
        assert_eq!(error.code(), "DTG-LANG-IR-MISMATCH");
    }
}

#[test]
fn transaction_boundaries_compile_to_boundary_statements() {
    for (source, expected) in [
        ("BEGIN", "BeginTransaction"),
        ("COMMIT", "CommitTransaction"),
        ("ROLLBACK", "RollbackTransaction"),
    ] {
        let statement = compile(source, &EmptySchemaCatalog).unwrap().statement;
        assert_eq!(
            format!("{statement:?}").split('(').next().unwrap(),
            expected
        );
    }
}

#[test]
fn removed_temporal_syntax_is_rejected() {
    for source in [
        "MATCH (n) AT VALID_TIME $t RETURN n",
        "DIFF GRAPH accounts",
        "CREATE (n) VALID TO $t",
    ] {
        let error = compile(source, &EmptySchemaCatalog).unwrap_err();
        assert_eq!(error.code(), "DTG-LANG-REMOVED-SYNTAX");
    }
}
