use dtg_language::{EmptySchemaCatalog, SchemaCatalog, compile};
use dtg_language_ir::{
    GraphId, GraphScope, LogicalMutation, LogicalNodeKind, LogicalStatement, TemporalScope, TimeExpr,
    ValidInterval, ValidIntervalExpr, ValidTimeExpr, ValidTimePredicate,
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
    assert_eq!(
        scan.read_scope.valid_time,
        Some(ValidTimePredicate::Overlaps(ValidIntervalExpr::Literal(
            ValidInterval::new(10, 20).unwrap()
        )))
    );
}

#[test]
fn two_parameter_valid_time_range_preserves_both_endpoints() {
    let program = compile(
        "FOR VALID_TIME BETWEEN $from AND $to MATCH (n) RETURN n",
        &EmptySchemaCatalog,
    )
    .unwrap();
    let LogicalStatement::Query(plan) = program.statement else {
        panic!("expected query");
    };
    let LogicalNodeKind::NodeScan(scan) = &plan.nodes[0].kind else {
        panic!("expected a node scan");
    };
    assert_eq!(
        scan.read_scope.valid_time,
        Some(ValidTimePredicate::Overlaps(ValidIntervalExpr::Bounds {
            start: ValidTimeExpr::Parameter("from".into()),
            end: ValidTimeExpr::Parameter("to".into()),
        }))
    );
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
fn valid_time_changes_preserves_the_valid_axis_and_both_endpoints() {
    let program = compile(
        "CHANGES FOR VALID_TIME BETWEEN $from AND $to MATCH (n) RETURN n",
        &EmptySchemaCatalog,
    )
    .unwrap();
    let LogicalStatement::Query(plan) = program.statement else {
        panic!("expected query");
    };
    let LogicalNodeKind::NodeScan(scan) = &plan.nodes[0].kind else {
        panic!("expected a node scan");
    };
    assert_eq!(
        scan.read_scope.valid_time,
        Some(ValidTimePredicate::Changes {
            from: ValidTimeExpr::Parameter("from".into()),
            to: ValidTimeExpr::Parameter("to".into()),
        })
    );
    assert_eq!(scan.read_scope.transaction_time, TemporalScope::Current);
}

#[test]
fn valid_from_parameter_is_preserved_by_every_write_form() {
    for source in [
        "CREATE (n:Person {name: 'Li'}) VALID FROM $t",
        "MATCH (n:Person) SET n.name = 'Wang' VALID FROM $t",
        "MATCH (n:Person) DELETE n VALID FROM $t",
    ] {
        let program = compile(source, &EmptySchemaCatalog).unwrap();
        assert_eq!(
            program
                .parameters
                .iter()
                .map(|parameter| parameter.name.as_str())
                .collect::<Vec<_>>(),
            vec!["t"]
        );
        let LogicalStatement::Write(write) = program.statement else {
            panic!("expected write");
        };
        let valid_from = match &write.mutations[..] {
            [LogicalMutation::CreateVertex { valid_from, .. }]
            | [LogicalMutation::SetProperties { valid_from, .. }]
            | [LogicalMutation::Delete { valid_from, .. }] => valid_from,
            mutations => panic!("expected one normalized mutation, got {mutations:?}"),
        };
        assert_eq!(valid_from, &ValidTimeExpr::Parameter("t".into()));
    }
}

#[test]
fn valid_from_literals_compile_and_invalid_expressions_fail_semantically() {
    for source in [
        "CREATE (n:Person {name: 'Li'}) VALID FROM 17",
        "MATCH (n:Person) SET n.name = 'Wang' VALID FROM 17",
        "MATCH (n:Person) DELETE n VALID FROM 17",
    ] {
        let program = compile(source, &EmptySchemaCatalog).unwrap();
        let LogicalStatement::Write(write) = program.statement else {
            panic!("expected write");
        };
        let valid_from = match &write.mutations[..] {
            [LogicalMutation::CreateVertex { valid_from, .. }]
            | [LogicalMutation::SetProperties { valid_from, .. }]
            | [LogicalMutation::Delete { valid_from, .. }] => valid_from,
            mutations => panic!("expected one normalized mutation, got {mutations:?}"),
        };
        assert_eq!(valid_from, &ValidTimeExpr::Literal(17));
    }

    for source in [
        "CREATE (n) VALID FROM 'tomorrow'",
        "MATCH (n) SET n.name = 'Wang' VALID FROM false",
        "MATCH (n) DELETE n VALID FROM null",
    ] {
        let error = compile(source, &EmptySchemaCatalog).unwrap_err();
        assert_eq!(error.code(), "DTG-LANG-TYPE");
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
