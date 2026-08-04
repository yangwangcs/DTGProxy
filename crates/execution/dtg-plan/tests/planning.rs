use dtg_language_ir::{
    Aggregate, AggregateFunction, AggregateKind, Expand, ExpandDirection, GraphScope, Join,
    JoinKind, Limit, LogicalExpr, LogicalNode, LogicalNodeId, LogicalNodeKind, LogicalPlan,
    LogicalProgram, LogicalStatement, Projection, ReadScope, RowSchema, Sort, SortDirection,
    SortKey, Subquery, Unwind, Value, VertexLookup,
};
use dtg_plan::{
    CatalogShard, CatalogSnapshot, LogicalReadOperation, PhysicalOperatorKind, PlanError,
    PlanningContext, SnapshotRequirements, StorageAccess, plan,
};
use dtg_storage::{
    BackendClass, BindingRole, CapabilityManifest, ProviderKind, ReplicaBinding, TransactionTime,
    Version,
};

fn exact_capabilities() -> CapabilityManifest {
    CapabilityManifest::from_names(dtg_plan::EXACT_VERTEX_POINT_CAPABILITIES).unwrap()
}

fn binding(
    shard_id: u64,
    placement_epoch: u64,
    backend_generation: u64,
    capabilities: &CapabilityManifest,
) -> ReplicaBinding {
    binding_for_provider(
        ProviderKind::Fjall,
        shard_id,
        placement_epoch,
        backend_generation,
        capabilities,
    )
}

fn binding_for_provider(
    provider_kind: ProviderKind,
    shard_id: u64,
    placement_epoch: u64,
    backend_generation: u64,
    capabilities: &CapabilityManifest,
) -> ReplicaBinding {
    let class = BackendClass::new(
        provider_kind.clone(),
        1,
        1,
        capabilities.names().map(str::to_owned),
    )
    .unwrap();
    ReplicaBinding::builder()
        .cluster_id(1)
        .graph_id(9)
        .shard_id(shard_id)
        .placement_epoch(placement_epoch)
        .replica_id(shard_id)
        .backend_generation(backend_generation)
        .backend_class_digest(class.digest())
        .provider_kind(provider_kind)
        .contract_version(1)
        .layout_version(1)
        .capability_digest(capabilities.digest())
        .namespace_id(format!("graph-9-shard-{shard_id}"))
        .endpoint_profile_ref("fixture-endpoint")
        .credential_ref("fixture-credential")
        .role(BindingRole::Active)
        .build()
        .unwrap()
}

fn point_query() -> LogicalProgram {
    LogicalProgram {
        version: dtg_language_ir::IrVersion::CURRENT,
        graph_scope: GraphScope::Explicit(dtg_storage::GraphId::new(9).unwrap()),
        parameters: Vec::new(),
        statement: LogicalStatement::Query(LogicalPlan {
            root: LogicalNodeId::new(1),
            nodes: vec![LogicalNode {
                id: LogicalNodeId::new(1),
                kind: LogicalNodeKind::VertexLookup(VertexLookup {
                    variable: "vertex".into(),
                    id: LogicalExpr::Literal(Value::Integer(41)),
                    labels: vec!["Person".into()],
                    read_scope: ReadScope::current(),
                }),
            }],
        }),
        result_schema: RowSchema::empty(),
    }
}

fn point_expand_query() -> LogicalProgram {
    LogicalProgram {
        version: dtg_language_ir::IrVersion::CURRENT,
        graph_scope: GraphScope::Explicit(dtg_storage::GraphId::new(9).unwrap()),
        parameters: Vec::new(),
        statement: LogicalStatement::Query(LogicalPlan {
            root: LogicalNodeId::new(2),
            nodes: vec![
                LogicalNode {
                    id: LogicalNodeId::new(1),
                    kind: LogicalNodeKind::VertexLookup(VertexLookup {
                        variable: "source".into(),
                        id: LogicalExpr::Literal(Value::Integer(41)),
                        labels: Vec::new(),
                        read_scope: ReadScope::current(),
                    }),
                },
                LogicalNode {
                    id: LogicalNodeId::new(2),
                    kind: LogicalNodeKind::Expand(Expand {
                        input: LogicalNodeId::new(1),
                        source: "source".into(),
                        relationship: "relationship".into(),
                        destination: "destination".into(),
                        destination_labels: Vec::new(),
                        direction: ExpandDirection::Outgoing,
                        relationship_types: Vec::new(),
                        read_scope: ReadScope::current(),
                    }),
                },
            ],
        }),
        result_schema: RowSchema::empty(),
    }
}

fn point_two_hop_expand_query() -> LogicalProgram {
    LogicalProgram {
        version: dtg_language_ir::IrVersion::CURRENT,
        graph_scope: GraphScope::Explicit(dtg_storage::GraphId::new(9).unwrap()),
        parameters: Vec::new(),
        statement: LogicalStatement::Query(LogicalPlan {
            root: LogicalNodeId::new(3),
            nodes: vec![
                LogicalNode {
                    id: LogicalNodeId::new(1),
                    kind: LogicalNodeKind::VertexLookup(VertexLookup {
                        variable: "source".into(),
                        id: LogicalExpr::Literal(Value::Integer(41)),
                        labels: Vec::new(),
                        read_scope: ReadScope::current(),
                    }),
                },
                LogicalNode {
                    id: LogicalNodeId::new(2),
                    kind: LogicalNodeKind::Expand(Expand {
                        input: LogicalNodeId::new(1),
                        source: "source".into(),
                        relationship: "first".into(),
                        destination: "middle".into(),
                        destination_labels: Vec::new(),
                        direction: ExpandDirection::Outgoing,
                        relationship_types: Vec::new(),
                        read_scope: ReadScope::current(),
                    }),
                },
                LogicalNode {
                    id: LogicalNodeId::new(3),
                    kind: LogicalNodeKind::Expand(Expand {
                        input: LogicalNodeId::new(2),
                        source: "middle".into(),
                        relationship: "second".into(),
                        destination: "destination".into(),
                        destination_labels: Vec::new(),
                        direction: ExpandDirection::Outgoing,
                        relationship_types: Vec::new(),
                        read_scope: ReadScope::current(),
                    }),
                },
            ],
        }),
        result_schema: RowSchema::empty(),
    }
}

fn operator_query() -> LogicalProgram {
    let column = |name: &str| LogicalExpr::Column(name.into());
    LogicalProgram {
        version: dtg_language_ir::IrVersion::CURRENT,
        graph_scope: GraphScope::Explicit(dtg_storage::GraphId::new(9).unwrap()),
        parameters: Vec::new(),
        statement: LogicalStatement::Query(LogicalPlan {
            root: LogicalNodeId::new(9),
            nodes: vec![
                LogicalNode {
                    id: LogicalNodeId::new(1),
                    kind: LogicalNodeKind::VertexLookup(VertexLookup {
                        variable: "left".into(),
                        id: LogicalExpr::Literal(Value::Integer(41)),
                        labels: Vec::new(),
                        read_scope: ReadScope::current(),
                    }),
                },
                LogicalNode {
                    id: LogicalNodeId::new(2),
                    kind: LogicalNodeKind::Filter {
                        input: LogicalNodeId::new(1),
                        predicate: LogicalExpr::Literal(Value::Boolean(true)),
                    },
                },
                LogicalNode {
                    id: LogicalNodeId::new(3),
                    kind: LogicalNodeKind::Project {
                        input: LogicalNodeId::new(2),
                        projections: vec![Projection {
                            expression: column("left"),
                            alias: "left_id".into(),
                        }],
                    },
                },
                LogicalNode {
                    id: LogicalNodeId::new(4),
                    kind: LogicalNodeKind::VertexLookup(VertexLookup {
                        variable: "right".into(),
                        id: LogicalExpr::Literal(Value::Integer(42)),
                        labels: Vec::new(),
                        read_scope: ReadScope::current(),
                    }),
                },
                LogicalNode {
                    id: LogicalNodeId::new(5),
                    kind: LogicalNodeKind::Join(Join {
                        left: LogicalNodeId::new(3),
                        right: LogicalNodeId::new(4),
                        kind: JoinKind::Inner,
                        predicate: Some(LogicalExpr::Literal(Value::Boolean(true))),
                    }),
                },
                LogicalNode {
                    id: LogicalNodeId::new(6),
                    kind: LogicalNodeKind::Aggregate(Aggregate {
                        input: LogicalNodeId::new(5),
                        groups: vec![Projection {
                            expression: column("left_id"),
                            alias: "group".into(),
                        }],
                        aggregates: vec![AggregateFunction {
                            function: AggregateKind::Count,
                            argument: None,
                            alias: "count".into(),
                            distinct: false,
                        }],
                    }),
                },
                LogicalNode {
                    id: LogicalNodeId::new(7),
                    kind: LogicalNodeKind::Sort(Sort {
                        input: LogicalNodeId::new(6),
                        keys: vec![SortKey {
                            expression: column("count"),
                            direction: SortDirection::Descending,
                        }],
                    }),
                },
                LogicalNode {
                    id: LogicalNodeId::new(8),
                    kind: LogicalNodeKind::Limit(Limit {
                        input: LogicalNodeId::new(7),
                        skip: Some(LogicalExpr::Literal(Value::Integer(1))),
                        limit: Some(LogicalExpr::Literal(Value::Integer(2))),
                    }),
                },
                LogicalNode {
                    id: LogicalNodeId::new(9),
                    kind: LogicalNodeKind::Unwind(Unwind {
                        input: LogicalNodeId::new(8),
                        expression: LogicalExpr::List(vec![
                            LogicalExpr::Literal(Value::Integer(1)),
                            LogicalExpr::Literal(Value::Integer(2)),
                        ]),
                        alias: "item".into(),
                    }),
                },
            ],
        }),
        result_schema: RowSchema::empty(),
    }
}

#[test]
fn planner_lowers_a_point_anchored_expand_to_one_adjacency_source() {
    let plan = plan(&point_expand_query(), &fixture_context()).unwrap();

    for fragment in plan.fragments() {
        assert_eq!(fragment.storage_accesses().len(), 1);
        assert_eq!(fragment.storage_accesses()[0].node(), LogicalNodeId::new(2));
        assert!(matches!(
            &fragment.storage_accesses()[0],
            StorageAccess::Logical(request)
                if matches!(
                    request.operation(),
                    LogicalReadOperation::Adjacency {
                        vertex_id,
                        direction: ExpandDirection::Outgoing,
                    } if vertex_id.get() == 41
                )
        ));
    }
    assert!(matches!(
        plan.operators(),
        [operator]
            if operator.id() == LogicalNodeId::new(2)
                && matches!(
                    operator.kind(),
                    PhysicalOperatorKind::Source { output, .. } if output == "relationship"
                )
    ));
}

#[test]
fn planner_lowers_a_point_anchored_two_hop_expand_to_one_traversal_source() {
    let plan = plan(&point_two_hop_expand_query(), &fixture_context()).unwrap();

    for fragment in plan.fragments() {
        assert_eq!(fragment.storage_accesses().len(), 1);
        assert_eq!(fragment.storage_accesses()[0].node(), LogicalNodeId::new(3));
        assert!(matches!(
            &fragment.storage_accesses()[0],
            StorageAccess::Logical(request)
                if matches!(
                    request.operation(),
                    LogicalReadOperation::Traversal {
                        vertex_id,
                        directions,
                    } if vertex_id.get() == 41
                        && directions == &[ExpandDirection::Outgoing, ExpandDirection::Outgoing]
                )
        ));
    }
    assert!(matches!(
        plan.operators(),
        [operator]
            if operator.id() == LogicalNodeId::new(3)
                && matches!(
                    operator.kind(),
                    PhysicalOperatorKind::Source { output, .. } if output == "second"
                )
    ));
}

#[test]
fn point_anchored_traversal_rejects_an_intermediate_relationship_projection() {
    let mut program = point_two_hop_expand_query();
    let LogicalStatement::Query(logical_plan) = &mut program.statement else {
        panic!()
    };
    logical_plan.nodes.push(LogicalNode {
        id: LogicalNodeId::new(4),
        kind: LogicalNodeKind::Project {
            input: LogicalNodeId::new(2),
            projections: vec![Projection {
                expression: LogicalExpr::Column("first".into()),
                alias: "first".into(),
            }],
        },
    });
    logical_plan.root = LogicalNodeId::new(4);

    assert!(matches!(
        plan(&program, &fixture_context()),
        Err(PlanError::UnsupportedNode { node, reason })
            if node == LogicalNodeId::new(2)
                && reason.contains("intermediate relationship")
    ));
}

#[test]
fn point_anchored_traversal_rejects_a_branching_intermediate_expand() {
    let mut program = point_two_hop_expand_query();
    let LogicalStatement::Query(logical_plan) = &mut program.statement else {
        panic!()
    };
    logical_plan.nodes.push(LogicalNode {
        id: LogicalNodeId::new(4),
        kind: LogicalNodeKind::Expand(Expand {
            input: LogicalNodeId::new(2),
            source: "middle".into(),
            relationship: "alternate".into(),
            destination: "other_destination".into(),
            destination_labels: Vec::new(),
            direction: ExpandDirection::Outgoing,
            relationship_types: Vec::new(),
            read_scope: ReadScope::current(),
        }),
    });

    assert!(matches!(
        plan(&program, &fixture_context()),
        Err(PlanError::UnsupportedNode { node, reason })
            if node == LogicalNodeId::new(2)
                && reason.contains("branching")
    ));
}

#[test]
fn point_anchored_expand_rejects_a_projection_of_unmaterialized_vertices() {
    let mut program = point_expand_query();
    let LogicalStatement::Query(logical_plan) = &mut program.statement else {
        panic!()
    };
    logical_plan.nodes.push(LogicalNode {
        id: LogicalNodeId::new(3),
        kind: LogicalNodeKind::Project {
            input: LogicalNodeId::new(2),
            projections: vec![Projection {
                expression: LogicalExpr::Column("destination".into()),
                alias: "destination".into(),
            }],
        },
    });
    logical_plan.root = LogicalNodeId::new(3);

    assert!(matches!(
        plan(&program, &fixture_context()),
        Err(PlanError::UnsupportedNode { node, reason })
            if node == LogicalNodeId::new(2)
                && reason.contains("does not materialize source or destination")
    ));
}

fn fixture_context() -> PlanningContext {
    let capabilities = exact_capabilities();
    let catalog = CatalogSnapshot::new(
        Version::new(11),
        Version::new(5),
        vec![CatalogShard::new(binding(13, 7, 3, &capabilities), 29)],
    )
    .unwrap();
    PlanningContext::new(
        catalog,
        capabilities,
        SnapshotRequirements::fixed(TransactionTime::new(23).unwrap(), 17),
        Some(128),
    )
    .unwrap()
}

#[test]
fn plan_pins_epoch_generation_and_capability_digest() {
    let plan = plan(&point_query(), &fixture_context()).unwrap();
    let fence = plan.fragments()[0].fence();
    assert_eq!(fence.placement_epoch().get(), 7);
    assert_eq!(fence.backend_generation().get(), 3);
    assert_eq!(fence.capability_digest(), exact_capabilities().digest());
}

#[test]
fn plan_pins_catalog_schema_and_snapshot_requirements() {
    let plan = plan(&point_query(), &fixture_context()).unwrap();
    let fence = plan.fragments()[0].fence();
    assert_eq!(fence.catalog_version(), Version::new(11));
    assert_eq!(fence.schema_version(), Version::new(5));
    assert_eq!(
        fence.snapshot_requirements(),
        &SnapshotRequirements::fixed(TransactionTime::new(23).unwrap(), 17)
    );
    assert_eq!(fence.applied_index(), 29);
}

#[test]
fn point_lookup_creates_one_fenced_owner_fragment_and_exchange() {
    let capabilities = exact_capabilities();
    let catalog = CatalogSnapshot::new(
        Version::new(2),
        Version::new(3),
        vec![
            CatalogShard::new(binding(17, 8, 4, &capabilities), 31),
            CatalogShard::new(binding(13, 7, 3, &capabilities), 29),
        ],
    )
    .unwrap();
    let context = PlanningContext::new(
        catalog,
        capabilities,
        SnapshotRequirements::fixed(TransactionTime::new(23).unwrap(), 17),
        Some(128),
    )
    .unwrap();

    let plan = plan(&point_query(), &context).unwrap();

    assert_eq!(plan.fragments().len(), 1);
    assert_eq!(plan.exchanges().len(), 1);
    assert_eq!(plan.fragments()[0].fence().shard_id().get(), 17);
}

#[test]
fn static_owner_routes_point_and_adjacency_reads_to_one_shard() {
    let capabilities = exact_capabilities();
    let context = PlanningContext::new(
        CatalogSnapshot::new(
            Version::new(2),
            Version::new(3),
            vec![
                CatalogShard::new(binding(17, 8, 4, &capabilities), 31),
                CatalogShard::new(binding(13, 7, 3, &capabilities), 29),
                CatalogShard::new(binding(19, 9, 5, &capabilities), 37),
            ],
        )
        .unwrap(),
        capabilities,
        SnapshotRequirements::fixed(TransactionTime::new(23).unwrap(), 17),
        Some(128),
    )
    .unwrap();

    for program in [point_query(), point_expand_query()] {
        let plan = plan(&program, &context).unwrap();
        assert_eq!(plan.fragments().len(), 1);
        assert_eq!(plan.exchanges().len(), 1);
        assert_eq!(plan.fragments()[0].fence().shard_id().get(), 19);
    }
}

#[test]
fn catalog_rejects_mixed_active_backend_providers() {
    let capabilities = exact_capabilities();

    assert!(matches!(
        CatalogSnapshot::new(
            Version::new(2),
            Version::new(3),
            vec![
                CatalogShard::new(binding(13, 7, 3, &capabilities), 29),
                CatalogShard::new(
                    binding_for_provider(ProviderKind::Kuzu, 17, 8, 4, &capabilities),
                    31,
                ),
            ],
        ),
        Err(PlanError::InvalidCatalog(message)) if message.contains("one backend provider")
    ));
}

#[test]
fn planner_preserves_every_normalized_query_operator_in_the_physical_dag() {
    let plan = plan(&operator_query(), &fixture_context()).unwrap();

    assert_eq!(plan.root_operator().get(), 9);
    assert!(matches!(
        plan.operator(LogicalNodeId::new(2)).unwrap().kind(),
        PhysicalOperatorKind::Filter { .. }
    ));
    assert!(matches!(
        plan.operator(LogicalNodeId::new(3)).unwrap().kind(),
        PhysicalOperatorKind::Project { .. }
    ));
    assert!(matches!(
        plan.operator(LogicalNodeId::new(5)).unwrap().kind(),
        PhysicalOperatorKind::Join { .. }
    ));
    assert!(matches!(
        plan.operator(LogicalNodeId::new(6)).unwrap().kind(),
        PhysicalOperatorKind::Aggregate { .. }
    ));
    assert!(matches!(
        plan.operator(LogicalNodeId::new(7)).unwrap().kind(),
        PhysicalOperatorKind::Sort { .. }
    ));
    assert!(matches!(
        plan.operator(LogicalNodeId::new(8)).unwrap().kind(),
        PhysicalOperatorKind::Limit {
            skip: 1,
            limit: Some(2),
            ..
        }
    ));
    assert!(matches!(
        plan.operator(LogicalNodeId::new(9)).unwrap().kind(),
        PhysicalOperatorKind::Unwind { .. }
    ));
}

#[test]
fn multi_shard_limit_is_a_single_global_operator_above_all_fragment_sources() {
    let capabilities = exact_capabilities();
    let catalog = CatalogSnapshot::new(
        Version::new(2),
        Version::new(3),
        vec![
            CatalogShard::new(binding(17, 8, 4, &capabilities), 31),
            CatalogShard::new(binding(13, 7, 3, &capabilities), 29),
        ],
    )
    .unwrap();
    let context = PlanningContext::new(
        catalog,
        capabilities,
        SnapshotRequirements::fixed(TransactionTime::new(23).unwrap(), 17),
        Some(128),
    )
    .unwrap();
    let mut query = point_query();
    let LogicalStatement::Query(logical) = &mut query.statement else {
        unreachable!()
    };
    logical.nodes.push(LogicalNode {
        id: LogicalNodeId::new(2),
        kind: LogicalNodeKind::Limit(Limit {
            input: LogicalNodeId::new(1),
            skip: None,
            limit: Some(LogicalExpr::Literal(Value::Integer(1))),
        }),
    });
    logical.root = LogicalNodeId::new(2);

    let plan = plan(&query, &context).unwrap();
    let sources = plan
        .operators()
        .iter()
        .filter_map(|operator| match operator.kind() {
            PhysicalOperatorKind::Source { fragments, .. } => Some(fragments),
            _ => None,
        })
        .collect::<Vec<_>>();

    assert_eq!(
        plan.operators()
            .iter()
            .filter(|operator| matches!(operator.kind(), PhysicalOperatorKind::Limit { .. }))
            .count(),
        1
    );
    assert_eq!(sources.len(), 1);
    assert_eq!(sources[0].len(), 1);
}

#[test]
fn unsupported_logical_node_fails_closed_instead_of_disappearing() {
    let mut query = point_query();
    let LogicalStatement::Query(logical) = &mut query.statement else {
        unreachable!()
    };
    logical.nodes.push(LogicalNode {
        id: LogicalNodeId::new(2),
        kind: LogicalNodeKind::Subquery(Subquery {
            input: Some(LogicalNodeId::new(1)),
            plan: Box::new(LogicalPlan {
                root: LogicalNodeId::new(1),
                nodes: vec![LogicalNode {
                    id: LogicalNodeId::new(1),
                    kind: LogicalNodeKind::VertexLookup(VertexLookup {
                        variable: "nested".into(),
                        id: LogicalExpr::Literal(Value::Integer(43)),
                        labels: Vec::new(),
                        read_scope: ReadScope::current(),
                    }),
                }],
            }),
            correlated_variables: Vec::new(),
        }),
    });
    logical.root = LogicalNodeId::new(2);

    assert!(matches!(
        plan(&query, &fixture_context()),
        Err(PlanError::UnsupportedNode { node, .. }) if node == LogicalNodeId::new(2)
    ));
}

#[test]
fn catalog_rejects_a_non_active_replica_binding() {
    let capabilities = exact_capabilities();
    let candidate = binding(13, 7, 3, &capabilities)
        .to_builder()
        .role(BindingRole::Candidate)
        .build()
        .unwrap();

    assert!(matches!(
        CatalogSnapshot::new(
            Version::new(2),
            Version::new(3),
            vec![CatalogShard::new(candidate, 29)]
        ),
        Err(PlanError::InvalidCatalog(_))
    ));
}

#[test]
fn capability_digest_drift_fails_closed_before_planning() {
    let catalog_capabilities = exact_capabilities();
    let planning_capabilities =
        CapabilityManifest::from_names([dtg_plan::CAP_VERTEX_POINT]).unwrap();
    let catalog = CatalogSnapshot::new(
        Version::new(2),
        Version::new(3),
        vec![CatalogShard::new(
            binding(13, 7, 3, &catalog_capabilities),
            29,
        )],
    )
    .unwrap();

    assert!(matches!(
        PlanningContext::new(
            catalog,
            planning_capabilities,
            SnapshotRequirements::fixed(TransactionTime::new(23).unwrap(), 17),
            Some(128),
        ),
        Err(PlanError::CapabilityDrift { .. })
    ));
}
