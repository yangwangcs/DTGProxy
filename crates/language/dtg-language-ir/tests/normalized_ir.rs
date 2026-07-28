use std::collections::BTreeMap;

use dtg_language_ir::{
    Aggregate, AggregateFunction, AggregateKind, AnalyticsExecutionMode, AnalyticsRequestIdentity,
    AnalyticsSubmission, BinaryOperator, BuiltInAlgorithmId, Expand, ExpandDirection, Field,
    GraphId, GraphScope, IrError, IrVersion, Join, JoinKind, Limit, LogicalExpr, LogicalNode,
    LogicalNodeId, LogicalNodeKind, LogicalPlan, LogicalProgram, LogicalStatement, LogicalType,
    LogicalWrite, NodeScan, Parameter, Projection, ReadScope, RelationshipLookup, RelationshipScan,
    RowSchema, Sort, SortDirection, SortKey, Subquery, TemporalScope, TimeExpr, TransactionTime,
    UnaryOperator, Unwind, ValidInterval, ValidIntervalExpr, ValidTimeExpr, ValidTimePredicate,
    Value, VertexLookup, validate_program,
};

fn parameter(name: &str) -> Parameter {
    Parameter {
        name: name.into(),
        data_type: LogicalType::Any,
        required: true,
    }
}

fn program(statement: LogicalStatement) -> LogicalProgram {
    LogicalProgram {
        version: IrVersion::CURRENT,
        graph_scope: GraphScope::Explicit(GraphId::new(7).unwrap()),
        parameters: vec![parameter("known")],
        statement,
        result_schema: RowSchema::empty(),
    }
}

fn read_scope() -> ReadScope {
    ReadScope {
        transaction_time: TemporalScope::AsOf(TimeExpr::Parameter("known".into())),
        valid_time: Some(ValidTimePredicate::Overlaps(ValidIntervalExpr::Parameter(
            "known".into(),
        ))),
    }
}

fn unknown_read_scope() -> ReadScope {
    ReadScope {
        transaction_time: TemporalScope::Current,
        valid_time: Some(ValidTimePredicate::At(ValidTimeExpr::Parameter(
            "unknown".into(),
        ))),
    }
}

fn plan(kind: LogicalNodeKind) -> LogicalPlan {
    LogicalPlan {
        root: LogicalNodeId::new(1),
        nodes: vec![LogicalNode {
            id: LogicalNodeId::new(1),
            kind,
        }],
    }
}

fn expression_with_unknown_parameter() -> LogicalExpr {
    LogicalExpr::Map(vec![(
        "nested".into(),
        LogicalExpr::Property {
            input: Box::new(LogicalExpr::List(vec![LogicalExpr::Unary {
                operator: UnaryOperator::Not,
                input: Box::new(LogicalExpr::Binary {
                    left: Box::new(LogicalExpr::Parameter("unknown".into())),
                    operator: BinaryOperator::Equal,
                    right: Box::new(LogicalExpr::Literal(Value::Boolean(true))),
                }),
            }])),
            name: "active".into(),
        },
    )])
}

fn assert_unknown_parameter(statement: LogicalStatement) {
    assert_eq!(
        validate_program(&program(statement)),
        Err(IrError::UnknownParameter("unknown".into()))
    );
}

#[test]
fn current_ir_has_one_fresh_major_version() {
    assert_eq!(IrVersion::CURRENT.major(), 1);
}

#[test]
fn built_in_algorithm_catalog_is_closed_and_has_stable_names() {
    let catalog = [
        ("bfs", BuiltInAlgorithmId::BreadthFirstSearch),
        ("dfs", BuiltInAlgorithmId::DepthFirstSearch),
        (
            "bounded_sssp",
            BuiltInAlgorithmId::BoundedSingleSourceShortestPath,
        ),
        (
            "bounded_apsp",
            BuiltInAlgorithmId::BoundedAllPairsShortestPaths,
        ),
        (
            "strongly_connected_components",
            BuiltInAlgorithmId::StronglyConnectedComponents,
        ),
        (
            "weakly_connected_components",
            BuiltInAlgorithmId::WeaklyConnectedComponents,
        ),
        ("page_rank", BuiltInAlgorithmId::PageRank),
        ("degree_centrality", BuiltInAlgorithmId::DegreeCentrality),
        (
            "closeness_centrality",
            BuiltInAlgorithmId::ClosenessCentrality,
        ),
        (
            "betweenness_centrality",
            BuiltInAlgorithmId::BetweennessCentrality,
        ),
        ("triangle_count", BuiltInAlgorithmId::TriangleCount),
        (
            "clustering_coefficient",
            BuiltInAlgorithmId::ClusteringCoefficient,
        ),
        ("k_core", BuiltInAlgorithmId::KCore),
        ("label_propagation", BuiltInAlgorithmId::LabelPropagation),
        ("louvain", BuiltInAlgorithmId::Louvain),
        ("earliest_arrival", BuiltInAlgorithmId::EarliestArrival),
        ("latest_departure", BuiltInAlgorithmId::LatestDeparture),
        (
            "temporal_reachability",
            BuiltInAlgorithmId::TemporalReachability,
        ),
        ("temporal_motif", BuiltInAlgorithmId::TemporalMotif),
        ("change_point", BuiltInAlgorithmId::ChangePoint),
    ];

    for (name, algorithm) in catalog {
        assert_eq!(BuiltInAlgorithmId::try_from(name), Ok(algorithm));
        assert_eq!(algorithm.as_str(), name);
    }
    assert!(BuiltInAlgorithmId::try_from("user.uploaded_code").is_err());
    assert!(BuiltInAlgorithmId::try_from("shortest_paths").is_err());
}

#[test]
fn programs_have_an_inspectable_default_or_explicit_graph_scope() {
    let default_program = LogicalProgram::new(LogicalStatement::Query(LogicalPlan::empty()));
    assert_eq!(default_program.graph_scope, GraphScope::SessionDefault);

    let graph = GraphId::new(9).unwrap();
    let scoped = LogicalProgram::for_graph(graph, LogicalStatement::Query(LogicalPlan::empty()));
    assert_eq!(scoped.graph_scope, GraphScope::Explicit(graph));
}

#[test]
fn logical_plans_find_transaction_scopes_in_reads_and_subqueries() {
    let scan_scope = TemporalScope::AsOf(TimeExpr::Parameter("scan".into()));
    let lookup_scope = TemporalScope::AsOf(TimeExpr::Parameter("lookup".into()));
    let expand_scope = TemporalScope::Changes {
        from: TimeExpr::Parameter("from".into()),
        to: TimeExpr::Parameter("to".into()),
    };
    let nested_scope = TemporalScope::AsOf(TimeExpr::Parameter("nested".into()));
    let plan = LogicalPlan {
        root: LogicalNodeId::new(1),
        nodes: vec![
            LogicalNode {
                id: LogicalNodeId::new(1),
                kind: LogicalNodeKind::NodeScan(NodeScan {
                    variable: "v".into(),
                    labels: Vec::new(),
                    read_scope: ReadScope {
                        transaction_time: scan_scope.clone(),
                        valid_time: None,
                    },
                }),
            },
            LogicalNode {
                id: LogicalNodeId::new(2),
                kind: LogicalNodeKind::RelationshipScan(RelationshipScan {
                    variable: "r".into(),
                    relationship_types: Vec::new(),
                    read_scope: ReadScope {
                        transaction_time: scan_scope.clone(),
                        valid_time: None,
                    },
                }),
            },
            LogicalNode {
                id: LogicalNodeId::new(3),
                kind: LogicalNodeKind::VertexLookup(VertexLookup {
                    variable: "lookup_v".into(),
                    id: LogicalExpr::Parameter("id".into()),
                    labels: Vec::new(),
                    read_scope: ReadScope {
                        transaction_time: lookup_scope.clone(),
                        valid_time: None,
                    },
                }),
            },
            LogicalNode {
                id: LogicalNodeId::new(4),
                kind: LogicalNodeKind::RelationshipLookup(RelationshipLookup {
                    variable: "lookup_r".into(),
                    id: LogicalExpr::Parameter("id".into()),
                    relationship_types: Vec::new(),
                    read_scope: ReadScope {
                        transaction_time: lookup_scope.clone(),
                        valid_time: None,
                    },
                }),
            },
            LogicalNode {
                id: LogicalNodeId::new(5),
                kind: LogicalNodeKind::Expand(Expand {
                    input: LogicalNodeId::new(1),
                    source: "v".into(),
                    relationship: "expanded_r".into(),
                    destination: "other".into(),
                    direction: ExpandDirection::Outgoing,
                    relationship_types: Vec::new(),
                    read_scope: ReadScope {
                        transaction_time: expand_scope.clone(),
                        valid_time: None,
                    },
                }),
            },
            LogicalNode {
                id: LogicalNodeId::new(6),
                kind: LogicalNodeKind::Subquery(Subquery {
                    input: None,
                    plan: Box::new(plan(LogicalNodeKind::Subquery(Subquery {
                        input: None,
                        plan: Box::new(plan(LogicalNodeKind::NodeScan(NodeScan {
                            variable: "nested_v".into(),
                            labels: Vec::new(),
                            read_scope: ReadScope {
                                transaction_time: nested_scope.clone(),
                                valid_time: None,
                            },
                        }))),
                        correlated_variables: Vec::new(),
                    }))),
                    correlated_variables: Vec::new(),
                }),
            },
        ],
    };

    assert!(plan.contains_scope(scan_scope));
    assert!(plan.contains_scope(lookup_scope));
    assert!(plan.contains_scope(expand_scope));
    assert!(plan.contains_scope(nested_scope));
    assert!(!plan.contains_scope(TemporalScope::AsOf(TimeExpr::Parameter("missing".into(),))));
}

#[test]
fn point_lookups_and_valid_time_read_scopes_are_public_and_validate_parameters() {
    let valid_program = program(LogicalStatement::Query(plan(
        LogicalNodeKind::VertexLookup(VertexLookup {
            variable: "v".into(),
            id: LogicalExpr::Parameter("known".into()),
            labels: vec!["Person".into()],
            read_scope: ReadScope {
                transaction_time: TemporalScope::AsOf(TimeExpr::Literal(
                    TransactionTime::new(10).unwrap(),
                )),
                valid_time: Some(ValidTimePredicate::At(ValidTimeExpr::Parameter(
                    "known".into(),
                ))),
            },
        }),
    )));
    assert_eq!(validate_program(&valid_program), Ok(()));

    let interval = ValidInterval::new(10, 20).unwrap();
    let relationship_lookup = RelationshipLookup {
        variable: "r".into(),
        id: LogicalExpr::Literal(Value::Integer(42)),
        relationship_types: vec!["KNOWS".into()],
        read_scope: ReadScope {
            transaction_time: TemporalScope::Current,
            valid_time: Some(ValidTimePredicate::Overlaps(ValidIntervalExpr::Literal(
                interval,
            ))),
        },
    };
    assert_eq!(
        validate_program(&program(LogicalStatement::Query(plan(
            LogicalNodeKind::RelationshipLookup(relationship_lookup),
        )))),
        Ok(())
    );

    let unknown_time = program(LogicalStatement::Query(plan(LogicalNodeKind::NodeScan(
        NodeScan {
            variable: "v".into(),
            labels: Vec::new(),
            read_scope: ReadScope {
                transaction_time: TemporalScope::Current,
                valid_time: Some(ValidTimePredicate::At(ValidTimeExpr::Parameter(
                    "unknown".into(),
                ))),
            },
        },
    ))));
    assert_eq!(
        validate_program(&unknown_time),
        Err(IrError::UnknownParameter("unknown".into()))
    );
}

#[test]
fn analytics_submission_is_asynchronous_and_carries_schema_and_stable_identity() {
    let submission = AnalyticsSubmission {
        algorithm: BuiltInAlgorithmId::PageRank,
        execution_mode: AnalyticsExecutionMode::Asynchronous,
        read_scope: read_scope(),
        arguments: BTreeMap::from([("damping".into(), LogicalExpr::Parameter("known".into()))]),
        result_schema: RowSchema {
            fields: vec![Field {
                name: "score".into(),
                data_type: LogicalType::Float,
                nullable: false,
            }],
        },
        request_identity: Some(AnalyticsRequestIdentity::new("request-17".into()).unwrap()),
    };
    assert_eq!(
        validate_program(&program(LogicalStatement::SubmitAnalytics(submission))),
        Ok(())
    );
}

#[test]
fn validation_recursively_rejects_unknown_expression_parameters_everywhere() {
    assert_unknown_parameter(LogicalStatement::Query(plan(LogicalNodeKind::NodeScan(
        NodeScan {
            variable: "v".into(),
            labels: Vec::new(),
            read_scope: unknown_read_scope(),
        },
    ))));
    assert_unknown_parameter(LogicalStatement::Query(plan(
        LogicalNodeKind::RelationshipScan(RelationshipScan {
            variable: "r".into(),
            relationship_types: Vec::new(),
            read_scope: unknown_read_scope(),
        }),
    )));
    assert_unknown_parameter(LogicalStatement::Query(plan(
        LogicalNodeKind::VertexLookup(VertexLookup {
            variable: "v".into(),
            id: expression_with_unknown_parameter(),
            labels: Vec::new(),
            read_scope: ReadScope::current(),
        }),
    )));
    assert_unknown_parameter(LogicalStatement::Query(plan(
        LogicalNodeKind::RelationshipLookup(RelationshipLookup {
            variable: "r".into(),
            id: expression_with_unknown_parameter(),
            relationship_types: Vec::new(),
            read_scope: ReadScope::current(),
        }),
    )));
    assert_unknown_parameter(LogicalStatement::Query(plan(LogicalNodeKind::Expand(
        Expand {
            input: LogicalNodeId::new(1),
            source: "v".into(),
            relationship: "r".into(),
            destination: "other".into(),
            direction: ExpandDirection::Outgoing,
            relationship_types: Vec::new(),
            read_scope: ReadScope {
                transaction_time: TemporalScope::Current,
                valid_time: Some(ValidTimePredicate::At(ValidTimeExpr::Parameter(
                    "unknown".into(),
                ))),
            },
        },
    ))));
    assert_unknown_parameter(LogicalStatement::Query(plan(LogicalNodeKind::Filter {
        input: LogicalNodeId::new(1),
        predicate: expression_with_unknown_parameter(),
    })));
    assert_unknown_parameter(LogicalStatement::Query(plan(LogicalNodeKind::Project {
        input: LogicalNodeId::new(1),
        projections: vec![Projection {
            expression: expression_with_unknown_parameter(),
            alias: "value".into(),
        }],
    })));
    assert_unknown_parameter(LogicalStatement::Query(plan(LogicalNodeKind::Join(Join {
        left: LogicalNodeId::new(1),
        right: LogicalNodeId::new(1),
        kind: JoinKind::Inner,
        predicate: Some(expression_with_unknown_parameter()),
    }))));
    assert_unknown_parameter(LogicalStatement::Query(plan(LogicalNodeKind::Aggregate(
        Aggregate {
            input: LogicalNodeId::new(1),
            groups: vec![Projection {
                expression: expression_with_unknown_parameter(),
                alias: "group".into(),
            }],
            aggregates: vec![AggregateFunction {
                function: AggregateKind::Count,
                argument: Some(expression_with_unknown_parameter()),
                alias: "count".into(),
                distinct: false,
            }],
        },
    ))));
    assert_unknown_parameter(LogicalStatement::Query(plan(LogicalNodeKind::Sort(Sort {
        input: LogicalNodeId::new(1),
        keys: vec![SortKey {
            expression: expression_with_unknown_parameter(),
            direction: SortDirection::Ascending,
        }],
    }))));
    assert_unknown_parameter(LogicalStatement::Query(plan(LogicalNodeKind::Limit(
        Limit {
            input: LogicalNodeId::new(1),
            skip: Some(expression_with_unknown_parameter()),
            limit: Some(expression_with_unknown_parameter()),
        },
    ))));
    assert_unknown_parameter(LogicalStatement::Query(plan(LogicalNodeKind::Unwind(
        Unwind {
            input: LogicalNodeId::new(1),
            expression: expression_with_unknown_parameter(),
            alias: "item".into(),
        },
    ))));
    assert_unknown_parameter(LogicalStatement::Query(plan(LogicalNodeKind::Subquery(
        Subquery {
            input: None,
            plan: Box::new(plan(LogicalNodeKind::Filter {
                input: LogicalNodeId::new(1),
                predicate: expression_with_unknown_parameter(),
            })),
            correlated_variables: Vec::new(),
        },
    ))));
    assert_unknown_parameter(LogicalStatement::Write(LogicalWrite {
        mutations: vec![dtg_language_ir::LogicalMutation::SetProperties {
            variable: "v".into(),
            properties: BTreeMap::from([("property".into(), expression_with_unknown_parameter())]),
            valid_interval: ValidInterval::new(1, 2).unwrap(),
        }],
    }));
    assert_unknown_parameter(LogicalStatement::SubmitAnalytics(AnalyticsSubmission {
        algorithm: BuiltInAlgorithmId::PageRank,
        execution_mode: AnalyticsExecutionMode::Asynchronous,
        read_scope: ReadScope::current(),
        arguments: BTreeMap::from([("input".into(), expression_with_unknown_parameter())]),
        result_schema: RowSchema::empty(),
        request_identity: None,
    }));
}

#[test]
fn empty_query_is_rejected() {
    let program = LogicalProgram::new(LogicalStatement::Query(LogicalPlan::empty()));
    assert_eq!(validate_program(&program), Err(IrError::EmptyQuery));
}
