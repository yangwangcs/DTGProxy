use dtg_language::{EmptySchemaCatalog, SchemaCatalog, compile};
use dtg_language_ir::{
    BinaryOperator, ExpandDirection, Field, GraphId, GraphScope, LogicalExpr, LogicalMutation,
    LogicalNodeKind, LogicalStatement, LogicalType, TemporalScope, TimeExpr, ValidInterval,
    ValidIntervalExpr, ValidTimeExpr, ValidTimePredicate, Value,
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
        assert_eq!(write.input.is_some(), source.starts_with("MATCH"));
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
fn match_constrained_write_preserves_relationship_expansion_and_properties() {
    let program = compile(
        "MATCH (a:Person {id: $id})-[r:KNOWS {since: $since}]->(b) \
         SET a.name = 'Wang' VALID FROM $t",
        &EmptySchemaCatalog,
    )
    .unwrap();
    assert_eq!(
        program
            .parameters
            .iter()
            .map(|parameter| parameter.name.as_str())
            .collect::<Vec<_>>(),
        vec!["id", "since", "t"]
    );
    let LogicalStatement::Write(write) = program.statement else {
        panic!("expected write");
    };
    let input = write.input.expect("MATCH write must carry its selection");
    assert!(input.nodes.iter().any(|node| matches!(
        &node.kind,
        LogicalNodeKind::Expand(expand)
            if expand.source == "a"
                && expand.relationship == "r"
                && expand.destination == "b"
                && expand.relationship_types == ["KNOWS"]
    )));
    assert!(input.nodes.iter().any(|node| matches!(
        &node.kind,
        LogicalNodeKind::Filter { predicate, .. }
            if predicate.eq(&LogicalExpr::Binary {
                left: Box::new(LogicalExpr::Property {
                    input: Box::new(LogicalExpr::Column("r".into())),
                    name: "since".into(),
                }),
                operator: BinaryOperator::Equal,
                right: Box::new(LogicalExpr::Parameter("since".into())),
            })
    )));
}

#[test]
fn expanded_destination_labels_are_preserved_for_queries_and_writes() {
    let query = compile(
        "MATCH (a)-[r:KNOWS]->(b:Person:Employee) RETURN b",
        &EmptySchemaCatalog,
    )
    .unwrap();
    let LogicalStatement::Query(plan) = query.statement else {
        panic!("expected query");
    };
    assert!(plan.nodes.iter().any(|node| matches!(
        &node.kind,
        LogicalNodeKind::Expand(expand)
            if expand.destination == "b"
                && expand.destination_labels == ["Person", "Employee"]
    )));

    for source in [
        "MATCH (a)-[r:KNOWS]->(b:Person:Employee) SET b.name = 'Wang' VALID FROM 1",
        "MATCH (a)-[r:KNOWS]->(b:Person:Employee) DELETE b VALID FROM 1",
    ] {
        let program = compile(source, &EmptySchemaCatalog).unwrap();
        let LogicalStatement::Write(write) = program.statement else {
            panic!("expected write");
        };
        let input = write.input.expect("MATCH write must carry its selection");
        assert!(
            input.nodes.iter().any(|node| matches!(
                &node.kind,
                LogicalNodeKind::Expand(expand)
                    if expand.destination == "b"
                        && expand.destination_labels == ["Person", "Employee"]
            )),
            "{source}"
        );
    }
}

#[test]
fn match_constrained_set_and_delete_preserve_their_selection_plan() {
    for source in [
        "MATCH (n:Person {id: $id}) SET n.name = 'Wang' VALID FROM $t",
        "MATCH (n:Person {id: $id}) DELETE n VALID FROM $t",
    ] {
        let program = compile(source, &EmptySchemaCatalog).unwrap();
        assert_eq!(
            program
                .parameters
                .iter()
                .map(|parameter| parameter.name.as_str())
                .collect::<Vec<_>>(),
            vec!["id", "t"]
        );
        let LogicalStatement::Write(write) = program.statement else {
            panic!("expected write");
        };
        let input = write.input.expect("MATCH write must carry its selection");
        assert!(input.nodes.iter().any(|node| matches!(
            &node.kind,
            LogicalNodeKind::NodeScan(scan)
                if scan.variable == "n" && scan.labels == ["Person"]
        )));
        assert!(input.nodes.iter().any(|node| matches!(
            &node.kind,
            LogicalNodeKind::Filter { predicate, .. }
                if predicate.eq(&LogicalExpr::Binary {
                    left: Box::new(LogicalExpr::Property {
                        input: Box::new(LogicalExpr::Column("n".into())),
                        name: "id".into(),
                    }),
                    operator: BinaryOperator::Equal,
                    right: Box::new(LogicalExpr::Parameter("id".into())),
                })
        )));
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
fn multiple_matches_preserve_the_exact_where_predicate_and_result_schema() {
    let program = compile(
        "MATCH (a) MATCH (b) WHERE a.id = b.id RETURN a",
        &EmptySchemaCatalog,
    )
    .unwrap();
    assert_eq!(
        program.result_schema.fields,
        vec![Field {
            name: "a".into(),
            data_type: LogicalType::Vertex,
            nullable: false,
        }]
    );
    let LogicalStatement::Query(plan) = program.statement else {
        panic!("expected query");
    };
    assert_eq!(
        plan.nodes
            .iter()
            .filter(|node| matches!(node.kind, LogicalNodeKind::NodeScan(_)))
            .count(),
        2
    );
    assert_eq!(
        plan.nodes
            .iter()
            .filter(|node| matches!(node.kind, LogicalNodeKind::Join(_)))
            .count(),
        1
    );
    let predicate = plan.nodes.iter().find_map(|node| match &node.kind {
        LogicalNodeKind::Filter { predicate, .. } => Some(predicate),
        _ => None,
    });
    assert_eq!(
        predicate,
        Some(&LogicalExpr::Binary {
            left: Box::new(LogicalExpr::Property {
                input: Box::new(LogicalExpr::Column("a".into())),
                name: "id".into(),
            }),
            operator: BinaryOperator::Equal,
            right: Box::new(LogicalExpr::Property {
                input: Box::new(LogicalExpr::Column("b".into())),
                name: "id".into(),
            }),
        })
    );
}

#[test]
fn repeated_match_variables_reuse_the_binding_while_independent_matches_join() {
    let repeated = compile("MATCH (a) MATCH (a) RETURN a", &EmptySchemaCatalog).unwrap();
    let LogicalStatement::Query(repeated) = repeated.statement else {
        panic!("expected query");
    };
    assert_eq!(
        repeated
            .nodes
            .iter()
            .filter(|node| matches!(node.kind, LogicalNodeKind::NodeScan(_)))
            .count(),
        1
    );
    assert!(
        !repeated
            .nodes
            .iter()
            .any(|node| matches!(node.kind, LogicalNodeKind::Join(_)))
    );

    let independent = compile("MATCH (a) MATCH (b) RETURN a", &EmptySchemaCatalog).unwrap();
    let LogicalStatement::Query(independent) = independent.statement else {
        panic!("expected query");
    };
    assert!(
        independent
            .nodes
            .iter()
            .any(|node| matches!(node.kind, LogicalNodeKind::Join(_)))
    );
}

#[test]
fn unsupported_relationship_and_multi_anchor_correlations_fail_closed() {
    for source in [
        "MATCH (a)-[r:KNOWS]->(b) MATCH (c)-[r:KNOWS]->(d) RETURN r",
        "MATCH (a) MATCH (b) MATCH (a)-[r:KNOWS]->(b) RETURN r",
    ] {
        let error = compile(source, &EmptySchemaCatalog).unwrap_err();
        assert_eq!(error.code(), "DTG-LANG-UNSUPPORTED-CORRELATION", "{source}");
    }
}

#[test]
fn reused_bindings_require_compatible_effective_read_scopes() {
    for source in [
        "MATCH (a) FOR SYSTEM_TIME AS OF 1 MATCH (a) FOR SYSTEM_TIME AS OF 2 RETURN a",
        "MATCH (a) FOR VALID_TIME AS OF 1 MATCH (a)-[r]->(b) FOR VALID_TIME AS OF 2 RETURN b",
    ] {
        let error = compile(source, &EmptySchemaCatalog).unwrap_err();
        assert_eq!(error.code(), "DTG-LANG-INCOMPATIBLE-SCOPE", "{source}");
    }
}

#[test]
fn valid_and_system_time_changes_cannot_be_combined() {
    let error = compile(
        "CHANGES FOR VALID_TIME BETWEEN 1 AND 2 \
         CHANGES FOR SYSTEM_TIME BETWEEN 3 AND 4 MATCH (n) RETURN n",
        &EmptySchemaCatalog,
    )
    .unwrap_err();
    assert_eq!(error.code(), "DTG-LANG-UNSUPPORTED-CHANGES");
}

#[test]
fn statement_wide_changes_axes_cannot_be_hidden_by_match_overrides() {
    let error = compile(
        "CHANGES FOR VALID_TIME BETWEEN 1 AND 2 \
         CHANGES FOR SYSTEM_TIME BETWEEN 3 AND 4 \
         MATCH (a) FOR VALID_TIME AS OF 1 \
         MATCH (b) FOR SYSTEM_TIME AS OF 3 RETURN a",
        &EmptySchemaCatalog,
    )
    .unwrap_err();
    assert_eq!(error.code(), "DTG-LANG-UNSUPPORTED-CHANGES");
}

#[test]
fn standalone_match_requires_a_return_clause() {
    let error = compile("MATCH (n)", &EmptySchemaCatalog).unwrap_err();
    assert_eq!(error.code(), "DTG-LANG-PARSE");
}

#[test]
fn repeated_bindings_inside_one_pattern_fail_closed() {
    for source in [
        "MATCH (a)-[r]->(b)-[s]->(a) RETURN a",
        "MATCH (a)-[r]->(b)-[r]->(c) RETURN r",
    ] {
        let error = compile(source, &EmptySchemaCatalog).unwrap_err();
        assert_eq!(error.code(), "DTG-LANG-UNSUPPORTED-CORRELATION", "{source}");
    }
}

#[test]
fn historical_system_time_is_rejected_for_write_selection_matches() {
    for source in [
        "MATCH (n) FOR SYSTEM_TIME AS OF 1 SET n.name = 'Wang' VALID FROM 2",
        "MATCH (n) FOR SYSTEM_TIME AS OF 1 DELETE n VALID FROM 2",
    ] {
        let error = compile(source, &EmptySchemaCatalog).unwrap_err();
        assert_eq!(
            error.code(),
            "DTG-LANG-HISTORICAL-WRITE-SELECTION",
            "{source}"
        );
    }
}

#[test]
fn anonymous_node_bindings_are_hygienic_and_not_returnable() {
    let program = compile(
        "MATCH (_node_6) MATCH () RETURN _node_6",
        &EmptySchemaCatalog,
    )
    .unwrap();
    let LogicalStatement::Query(plan) = program.statement else {
        panic!("expected query");
    };
    assert_eq!(
        plan.nodes
            .iter()
            .filter(|node| matches!(node.kind, LogicalNodeKind::NodeScan(_)))
            .count(),
        2
    );
    assert!(
        plan.nodes
            .iter()
            .any(|node| matches!(node.kind, LogicalNodeKind::Join(_)))
    );

    let error = compile("MATCH () RETURN _node_2", &EmptySchemaCatalog).unwrap_err();
    assert_eq!(error.code(), "DTG-LANG-UNBOUND-VARIABLE");
}

#[test]
fn anonymous_relationship_bindings_are_hygienic_and_not_returnable() {
    let program = compile(
        "MATCH (a)-[_rel_18]->(b) MATCH (c)-[]->(d) RETURN _rel_18",
        &EmptySchemaCatalog,
    )
    .unwrap();
    let LogicalStatement::Query(plan) = program.statement else {
        panic!("expected query");
    };
    let relationship_variables = plan
        .nodes
        .iter()
        .filter_map(|node| match &node.kind {
            LogicalNodeKind::Expand(expand) => Some(expand.relationship.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(relationship_variables.len(), 2);
    assert!(relationship_variables.contains(&"_rel_18"));
    assert_eq!(
        relationship_variables
            .iter()
            .filter(|variable| variable.starts_with("@anonymous_relationship:"))
            .count(),
        1
    );

    let error = compile("MATCH (a)-[]->(b) RETURN _rel_6", &EmptySchemaCatalog).unwrap_err();
    assert_eq!(error.code(), "DTG-LANG-UNBOUND-VARIABLE");
}

#[test]
fn semantic_analysis_rejects_unbound_and_conflicting_variables() {
    for source in [
        "MATCH (n) RETURN missing",
        "MATCH (n) WHERE missing.id = n.id RETURN n",
        "MATCH (n) RETURN missing.name",
        "MATCH (n) SET missing.name = 'Wang' VALID FROM 1",
        "MATCH (n) DELETE missing VALID FROM 1",
    ] {
        let error = compile(source, &EmptySchemaCatalog).unwrap_err();
        assert_eq!(error.code(), "DTG-LANG-UNBOUND-VARIABLE", "{source}");
    }

    let error = compile("MATCH (n)-[n:KNOWS]->(other) RETURN n", &EmptySchemaCatalog).unwrap_err();
    assert_eq!(error.code(), "DTG-LANG-CONFLICTING-BINDING");
}

#[test]
fn projection_aliases_and_types_are_observable_and_stable() {
    let program = compile(
        "MATCH (n)-[r:KNOWS]->(m) RETURN n, r, n.name, '中文🌍', $p",
        &EmptySchemaCatalog,
    )
    .unwrap();
    assert_eq!(
        program.result_schema.fields,
        vec![
            Field {
                name: "n".into(),
                data_type: LogicalType::Vertex,
                nullable: false,
            },
            Field {
                name: "r".into(),
                data_type: LogicalType::Relationship,
                nullable: false,
            },
            Field {
                name: "n.name".into(),
                data_type: LogicalType::Any,
                nullable: true,
            },
            Field {
                name: "expression_3".into(),
                data_type: LogicalType::String,
                nullable: false,
            },
            Field {
                name: "$p".into(),
                data_type: LogicalType::Any,
                nullable: true,
            },
        ]
    );
    let LogicalStatement::Query(plan) = program.statement else {
        panic!("expected query");
    };
    let projections = plan.nodes.iter().find_map(|node| match &node.kind {
        LogicalNodeKind::Project { projections, .. } => Some(projections),
        _ => None,
    });
    assert_eq!(
        projections
            .unwrap()
            .iter()
            .map(|projection| projection.alias.as_str())
            .collect::<Vec<_>>(),
        vec!["n", "r", "n.name", "expression_3", "$p"]
    );
}

#[test]
fn explicit_relationship_create_normalizes_all_logical_fields() {
    let program = compile(
        "MATCH (a:Person {id: $from}) MATCH (b:Person {id: $to}) \
         CREATE (a)-[r:KNOWS {since: $since}]->(b) VALID FROM $t",
        &EmptySchemaCatalog,
    )
    .unwrap();
    assert_eq!(
        program
            .parameters
            .iter()
            .map(|parameter| parameter.name.as_str())
            .collect::<Vec<_>>(),
        vec!["from", "since", "t", "to"]
    );
    let LogicalStatement::Write(write) = program.statement else {
        panic!("expected write");
    };
    assert!(write.input.is_some());
    assert_eq!(
        write.mutations,
        vec![LogicalMutation::CreateRelationship {
            variable: "r".into(),
            relationship_type: "KNOWS".into(),
            source: "a".into(),
            destination: "b".into(),
            properties: std::collections::BTreeMap::from([(
                "since".into(),
                LogicalExpr::Parameter("since".into()),
            )]),
            valid_from: ValidTimeExpr::Parameter("t".into()),
        }]
    );

    for source in [
        "MATCH (a) MATCH (b) CREATE (a)-[r]->(b) VALID FROM 1",
        "MATCH (a) MATCH (b) CREATE (a)-[r:A:B]->(b) VALID FROM 1",
    ] {
        let error = compile(source, &EmptySchemaCatalog).unwrap_err();
        assert_eq!(error.code(), "DTG-LANG-RELATIONSHIP-TYPE", "{source}");
    }
}

#[test]
fn utf8_strings_and_escapes_round_trip_without_splitting_bytes() {
    let program = compile("MATCH (n) RETURN '中文🌍', '转\\义'", &EmptySchemaCatalog).unwrap();
    let LogicalStatement::Query(plan) = program.statement else {
        panic!("expected query");
    };
    let projections = plan.nodes.iter().find_map(|node| match &node.kind {
        LogicalNodeKind::Project { projections, .. } => Some(projections),
        _ => None,
    });
    assert_eq!(
        projections
            .unwrap()
            .iter()
            .map(|projection| &projection.expression)
            .collect::<Vec<_>>(),
        vec![
            &LogicalExpr::Literal(Value::String("中文🌍".into())),
            &LogicalExpr::Literal(Value::String("转义".into())),
        ]
    );
}

#[test]
fn set_and_delete_identifiers_do_not_reclassify_read_queries() {
    for source in [
        "MATCH (set:delete {set: 'delete'}) RETURN set",
        "MATCH (delete:set) RETURN delete",
    ] {
        let program = compile(source, &EmptySchemaCatalog).unwrap();
        assert!(
            matches!(program.statement, LogicalStatement::Query(_)),
            "{source}"
        );
    }
}

#[test]
fn undirected_relationships_normalize_to_either_expansion() {
    let program = compile("MATCH (a)-[r:KNOWS]-(b) RETURN a", &EmptySchemaCatalog).unwrap();
    let LogicalStatement::Query(plan) = program.statement else {
        panic!("expected query");
    };
    assert!(plan.nodes.iter().any(|node| matches!(
        node.kind,
        LogicalNodeKind::Expand(ref expand) if expand.direction == ExpandDirection::Either
    )));
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

#[test]
fn removed_syntax_after_graph_scope_preserves_its_stable_error_code() {
    let error = compile("USE accounts DIFF GRAPH accounts", &EmptySchemaCatalog).unwrap_err();
    assert_eq!(error.code(), "DTG-LANG-REMOVED-SYNTAX");
}
