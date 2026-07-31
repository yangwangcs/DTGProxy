mod support;

use std::collections::BTreeMap;

use dtg_language_ir::{
    AggregateKind, BinaryOperator, Field, JoinKind, LogicalExpr, LogicalType, RowSchema,
    SortDirection, Value,
};
use dtg_query::{
    AggregateOperator, BatchOperator, CancellationToken, ColumnBatch, DeterministicMergeOperator,
    ExecutableAccess, ExecutableAggregate, ExecutableFragment, ExecutableOperator,
    ExecutableOperatorKind, ExecutablePlan, ExecutableProjection, ExecutableSortKey,
    ExpandOperator, Expression, FilterOperator, HashJoinOperator, LimitOperator, LogicalRead,
    OverlayOperator, ProjectOperator, ProjectionExpr, QueryBudget, QueryOverlay, QueryRuntime,
    QueryStream, QueryValue, ReadOperation, SnapshotGuard, SnapshotShardFence, SortOperator,
};
use dtg_storage::{
    AdjacencyDirection, CapabilityManifest, LogicalMutation, ShardId, SnapshotRecord,
    TransactionTime, Version,
};

use support::{
    FixtureStore, block_on, edge, execution_fence_for_shard, point_plan, snapshot, storage_map,
    vertex,
};

fn int_schema(name: &str) -> RowSchema {
    RowSchema {
        fields: vec![Field {
            name: name.into(),
            data_type: LogicalType::Integer,
            nullable: false,
        }],
    }
}

fn collect(operator: Box<dyn dtg_query::Operator>) -> ColumnBatch {
    block_on(
        QueryStream::from_operator(operator, QueryBudget::unlimited(), CancellationToken::new())
            .collect(),
    )
    .unwrap()
}

#[test]
fn column_batches_enforce_types_and_preserve_temporal_entities() {
    let schema = RowSchema {
        fields: vec![Field {
            name: "vertex".into(),
            data_type: LogicalType::Vertex,
            nullable: false,
        }],
    };
    let row = vertex(2, 20);
    let batch = ColumnBatch::from_rows(schema.clone(), vec![vec![QueryValue::Vertex(row.clone())]])
        .unwrap();

    assert_eq!(batch.vertex_ids(), vec![row.id()]);
    assert_eq!(batch.row_count(), 1);
    assert!(ColumnBatch::from_rows(schema, vec![vec![QueryValue::Integer(2)]],).is_err());
}

#[test]
fn filter_project_sort_and_limit_are_columnar_and_deterministic() {
    let batch = ColumnBatch::from_rows(
        int_schema("score"),
        vec![
            vec![QueryValue::Integer(1)],
            vec![QueryValue::Integer(3)],
            vec![QueryValue::Integer(2)],
        ],
    )
    .unwrap();
    let filtered = FilterOperator::new(
        Box::new(BatchOperator::new(vec![batch])),
        Expression::new(LogicalExpr::Binary {
            left: Box::new(LogicalExpr::Column("score".into())),
            operator: BinaryOperator::GreaterThan,
            right: Box::new(LogicalExpr::Literal(Value::Integer(1))),
        }),
    );
    let projected = ProjectOperator::new(
        Box::new(filtered),
        vec![ProjectionExpr::new(
            "result",
            LogicalType::Integer,
            false,
            Expression::new(LogicalExpr::Column("score".into())),
        )],
    );
    let sorted = SortOperator::new(Box::new(projected), 0, SortDirection::Descending);
    let limited = LimitOperator::new(Box::new(sorted), 0, 1);
    let output = collect(Box::new(limited));

    assert_eq!(output.rows(), vec![vec![QueryValue::Integer(3)]]);
}

#[test]
fn hash_join_aggregate_and_merge_have_stable_duplicate_rules() {
    let left = ColumnBatch::from_rows(
        int_schema("left_id"),
        vec![vec![QueryValue::Integer(2)], vec![QueryValue::Integer(1)]],
    )
    .unwrap();
    let right = ColumnBatch::from_rows(
        int_schema("right_id"),
        vec![vec![QueryValue::Integer(1)], vec![QueryValue::Integer(2)]],
    )
    .unwrap();
    let joined = HashJoinOperator::new(
        Box::new(BatchOperator::new(vec![left])),
        Box::new(BatchOperator::new(vec![right])),
        0,
        0,
    );
    let joined = collect(Box::new(joined));
    assert_eq!(joined.row_count(), 2);

    let counted =
        AggregateOperator::count_by(Box::new(BatchOperator::new(vec![joined])), 0, "count");
    let counted = collect(Box::new(counted));
    assert_eq!(counted.row_count(), 2);

    let one = ColumnBatch::from_rows(
        int_schema("id"),
        vec![vec![QueryValue::Integer(2)], vec![QueryValue::Integer(1)]],
    )
    .unwrap();
    let two = ColumnBatch::from_rows(
        int_schema("id"),
        vec![vec![QueryValue::Integer(1)], vec![QueryValue::Integer(3)]],
    )
    .unwrap();
    let merged = DeterministicMergeOperator::new(
        vec![
            Box::new(BatchOperator::new(vec![one])),
            Box::new(BatchOperator::new(vec![two])),
        ],
        0,
        true,
    );
    assert_eq!(
        collect(Box::new(merged)).rows(),
        vec![
            vec![QueryValue::Integer(1)],
            vec![QueryValue::Integer(2)],
            vec![QueryValue::Integer(3)],
        ]
    );
}

#[test]
fn global_count_all_counts_input_rows() {
    let batch = ColumnBatch::from_rows(
        int_schema("value"),
        vec![
            vec![QueryValue::Integer(1)],
            vec![QueryValue::Integer(2)],
            vec![QueryValue::Integer(3)],
        ],
    )
    .unwrap();

    let output = collect(Box::new(AggregateOperator::count_all(
        Box::new(BatchOperator::new(vec![batch])),
        "COUNT(*)",
    )));

    assert_eq!(output.rows(), vec![vec![QueryValue::Integer(3)]]);
}

#[test]
fn global_count_all_returns_zero_for_an_empty_batch() {
    let empty = ColumnBatch::from_rows(int_schema("value"), Vec::new()).unwrap();

    let output = collect(Box::new(AggregateOperator::count_all(
        Box::new(BatchOperator::new(vec![empty])),
        "COUNT(*)",
    )));

    assert_eq!(output.rows(), vec![vec![QueryValue::Integer(0)]]);
}

#[test]
fn global_count_runtime_lowers_zero_groups() {
    let capabilities = CapabilityManifest::from_names([] as [&str; 0]).unwrap();
    let fragment = ExecutableFragment::with_access_nodes(
        1,
        execution_fence_for_shard(&capabilities, 13),
        vec![ExecutableAccess::Logical(
            LogicalRead::new(
                ReadOperation::VertexScan,
                8,
                TransactionTime::new(23).unwrap(),
                17,
            )
            .unwrap(),
        )],
        vec![1],
    )
    .unwrap();
    let plan = ExecutablePlan::with_operators(
        Version::new(1),
        vec![fragment],
        2,
        vec![
            ExecutableOperator::new(
                1,
                ExecutableOperatorKind::Source {
                    logical_node: 1,
                    fragments: vec![1],
                    output: "vertex".into(),
                },
            )
            .unwrap(),
            ExecutableOperator::new(
                2,
                ExecutableOperatorKind::Aggregate {
                    input: 1,
                    groups: Vec::new(),
                    aggregates: vec![ExecutableAggregate {
                        function: AggregateKind::Count,
                        argument: None,
                        alias: "COUNT(*)".into(),
                        distinct: false,
                    }],
                },
            )
            .unwrap(),
        ],
        int_schema("COUNT(*)"),
    )
    .unwrap();
    let store = FixtureStore::new(
        capabilities,
        vec![vertex(1, 1), vertex(2, 2), vertex(3, 3)],
        Vec::new(),
    );

    let mut stream = block_on(QueryRuntime::new(16).execute(
        &plan,
        storage_map(store.storage(false)),
        &snapshot(),
        QueryBudget::unlimited(),
        CancellationToken::new(),
        None,
    ))
    .unwrap();
    let output = block_on(stream.collect()).unwrap();

    assert_eq!(output.rows(), vec![vec![QueryValue::Integer(3)]]);
}

#[test]
fn materialized_source_filters_and_projects() {
    let capabilities = CapabilityManifest::from_names([] as [&str; 0]).unwrap();
    let fragment = ExecutableFragment::with_access_nodes(
        1,
        execution_fence_for_shard(&capabilities, 13),
        vec![ExecutableAccess::Logical(
            LogicalRead::new(
                ReadOperation::VertexScan,
                8,
                TransactionTime::new(23).unwrap(),
                17,
            )
            .unwrap(),
        )],
        vec![1],
    )
    .unwrap();
    let plan = ExecutablePlan::with_operators(
        Version::new(1),
        vec![fragment],
        3,
        vec![
            ExecutableOperator::new(
                1,
                ExecutableOperatorKind::Source {
                    logical_node: 1,
                    fragments: vec![1],
                    output: "n".into(),
                },
            )
            .unwrap(),
            ExecutableOperator::new(
                2,
                ExecutableOperatorKind::Filter {
                    input: 1,
                    predicate: Expression::new(LogicalExpr::Binary {
                        left: Box::new(LogicalExpr::Property {
                            input: Box::new(LogicalExpr::Column("n".into())),
                            name: "id".into(),
                        }),
                        operator: BinaryOperator::Equal,
                        right: Box::new(LogicalExpr::Literal(Value::Integer(2048))),
                    }),
                },
            )
            .unwrap(),
            ExecutableOperator::new(
                3,
                ExecutableOperatorKind::Project {
                    input: 2,
                    projections: vec![ExecutableProjection::new(
                        "n.id",
                        Expression::new(LogicalExpr::Property {
                            input: Box::new(LogicalExpr::Column("n".into())),
                            name: "id".into(),
                        }),
                    )],
                },
            )
            .unwrap(),
        ],
        RowSchema {
            fields: vec![Field {
                name: "n.id".into(),
                data_type: LogicalType::Any,
                nullable: true,
            }],
        },
    )
    .unwrap();
    let batch = |start, end| {
        ColumnBatch::from_rows(
            RowSchema {
                fields: vec![Field {
                    name: "vertex".into(),
                    data_type: LogicalType::Any,
                    nullable: false,
                }],
            },
            (start..=end)
                .map(|id| {
                    vec![QueryValue::Map(BTreeMap::from([(
                        "id".into(),
                        QueryValue::Integer(id),
                    )]))]
                })
                .collect(),
        )
        .unwrap()
    };
    let materialized = BTreeMap::from([(1, vec![batch(1, 2048), batch(2049, 4096)])]);

    let mut stream = block_on(QueryRuntime::new(256).execute_materialized(
        &plan,
        materialized,
        QueryBudget::unlimited(),
        CancellationToken::new(),
    ))
    .unwrap();
    let output = block_on(stream.collect()).unwrap();

    assert_eq!(output.schema(), plan.result_schema());
    assert_eq!(output.rows(), vec![vec![QueryValue::Integer(2048)]]);
}

#[test]
fn overlay_applies_read_your_own_writes_before_results_escape() {
    let base = vertex(1, 10);
    let staged = vertex(2, 20);
    let batch = ColumnBatch::from_rows(
        RowSchema {
            fields: vec![Field {
                name: "vertex".into(),
                data_type: LogicalType::Vertex,
                nullable: false,
            }],
        },
        vec![vec![QueryValue::Vertex(base.clone())]],
    )
    .unwrap();
    let overlay = QueryOverlay::from_mutations(vec![
        LogicalMutation::DeleteVertex(dtg_storage::VertexTombstone::new(
            base.id(),
            dtg_storage::Version::new(2),
            dtg_storage::TransactionTime::new(23).unwrap(),
        )),
        LogicalMutation::PutVertex(staged.clone()),
    ])
    .unwrap();
    let output = collect(Box::new(OverlayOperator::new(
        Box::new(BatchOperator::new(vec![batch])),
        overlay,
        17,
    )));

    assert_eq!(output.vertex_ids(), vec![staged.id()]);
}

#[test]
fn expand_reads_bounded_adjacency_from_temporal_view() {
    let capabilities = CapabilityManifest::from_names([] as [&str; 0]).unwrap();
    let source = vertex(1, 10);
    let relationship = edge(7, 1, 2);
    let store = FixtureStore::new(
        capabilities,
        vec![source.clone(), vertex(2, 20)],
        vec![relationship.clone()],
    );
    let input = ColumnBatch::from_rows(
        RowSchema {
            fields: vec![Field {
                name: "vertex".into(),
                data_type: LogicalType::Vertex,
                nullable: false,
            }],
        },
        vec![vec![QueryValue::Vertex(source)]],
    )
    .unwrap();
    let expanded = ExpandOperator::new(
        Box::new(BatchOperator::new(vec![input])),
        0,
        store,
        AdjacencyDirection::Outgoing,
        17,
        TransactionTime::new(23).unwrap(),
        16,
    )
    .unwrap();

    assert_eq!(
        collect(Box::new(expanded)).relationship_ids(),
        vec![relationship.id()]
    );
}

#[test]
fn runtime_consumes_executable_plan_temporal_view_and_snapshot_guard() {
    let capabilities =
        CapabilityManifest::from_names(support::EXACT_VERTEX_POINT_CAPABILITIES).unwrap();
    let wanted = vertex(2, 20);
    let store = FixtureStore::new(capabilities.clone(), vec![wanted.clone()], Vec::new());
    store.with_pushdown_outcome(dtg_storage::PushdownOutcome::Exact(vec![
        SnapshotRecord::Vertex(wanted.clone()),
    ]));
    let mut stream = block_on(QueryRuntime::new(16).execute(
        &point_plan(capabilities, 2),
        storage_map(store.storage(true)),
        &snapshot(),
        QueryBudget::unlimited(),
        CancellationToken::new(),
        None,
    ))
    .unwrap();
    let rows = block_on(stream.collect()).unwrap();

    assert_eq!(rows.vertex_ids(), vec![wanted.id()]);
}

#[test]
fn runtime_applies_limit_once_after_merging_all_shard_sources() {
    let capabilities = CapabilityManifest::from_names([] as [&str; 0]).unwrap();
    let fragments = [13_u64, 17_u64]
        .into_iter()
        .enumerate()
        .map(|(index, shard_id)| {
            ExecutableFragment::with_access_nodes(
                u32::try_from(index + 1).unwrap(),
                execution_fence_for_shard(&capabilities, shard_id),
                vec![ExecutableAccess::Logical(
                    LogicalRead::new(
                        ReadOperation::VertexScan,
                        8,
                        TransactionTime::new(23).unwrap(),
                        17,
                    )
                    .unwrap(),
                )],
                vec![1],
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let plan = ExecutablePlan::with_operators(
        Version::new(1),
        fragments,
        2,
        vec![
            ExecutableOperator::new(
                1,
                ExecutableOperatorKind::Source {
                    logical_node: 1,
                    fragments: vec![1, 2],
                    output: "vertex".into(),
                },
            )
            .unwrap(),
            ExecutableOperator::new(
                2,
                ExecutableOperatorKind::Limit {
                    input: 1,
                    skip: 0,
                    limit: Some(1),
                },
            )
            .unwrap(),
        ],
        RowSchema::empty(),
    )
    .unwrap();
    let left =
        FixtureStore::new_for_shard(capabilities.clone(), 13, vec![vertex(1, 10)], Vec::new());
    let right = FixtureStore::new_for_shard(capabilities, 17, vec![vertex(2, 20)], Vec::new());
    let storage = std::collections::BTreeMap::from([
        (ShardId::new(13).unwrap(), left.storage(false)),
        (ShardId::new(17).unwrap(), right.storage(false)),
    ]);
    let snapshot = SnapshotGuard::new(
        TransactionTime::new(23).unwrap(),
        Version::new(11),
        vec![
            (
                ShardId::new(13).unwrap(),
                SnapshotShardFence {
                    placement_epoch: dtg_storage::PlacementEpoch::new(7).unwrap(),
                    backend_generation: dtg_storage::BackendGeneration::new(3).unwrap(),
                    applied_index: 29,
                },
            ),
            (
                ShardId::new(17).unwrap(),
                SnapshotShardFence {
                    placement_epoch: dtg_storage::PlacementEpoch::new(7).unwrap(),
                    backend_generation: dtg_storage::BackendGeneration::new(3).unwrap(),
                    applied_index: 29,
                },
            ),
        ],
    )
    .unwrap();

    let mut stream = block_on(QueryRuntime::new(16).execute(
        &plan,
        storage,
        &snapshot,
        QueryBudget::unlimited(),
        CancellationToken::new(),
        None,
    ))
    .unwrap();
    let rows = block_on(stream.collect()).unwrap();

    assert_eq!(rows.row_count(), 1);
}

#[test]
fn runtime_builds_every_operator_kind_from_an_unordered_physical_dag() {
    let capabilities = CapabilityManifest::from_names([] as [&str; 0]).unwrap();
    let fragment = ExecutableFragment::with_access_nodes(
        1,
        execution_fence_for_shard(&capabilities, 13),
        vec![
            ExecutableAccess::Logical(
                LogicalRead::new(
                    ReadOperation::VertexScan,
                    8,
                    TransactionTime::new(23).unwrap(),
                    17,
                )
                .unwrap(),
            ),
            ExecutableAccess::Logical(
                LogicalRead::new(
                    ReadOperation::VertexScan,
                    8,
                    TransactionTime::new(23).unwrap(),
                    17,
                )
                .unwrap(),
            ),
        ],
        vec![1, 2],
    )
    .unwrap();
    let operators = vec![
        ExecutableOperator::new(
            10,
            ExecutableOperatorKind::Limit {
                input: 9,
                skip: 0,
                limit: Some(1),
            },
        )
        .unwrap(),
        ExecutableOperator::new(
            9,
            ExecutableOperatorKind::Sort {
                input: 8,
                keys: vec![ExecutableSortKey {
                    expression: Expression::new(LogicalExpr::Column("item".into())),
                    direction: SortDirection::Descending,
                }],
            },
        )
        .unwrap(),
        ExecutableOperator::new(
            8,
            ExecutableOperatorKind::Aggregate {
                input: 7,
                groups: vec![ExecutableProjection::new(
                    "item",
                    Expression::new(LogicalExpr::Column("item".into())),
                )],
                aggregates: vec![ExecutableAggregate {
                    function: AggregateKind::Count,
                    argument: None,
                    alias: "matches".into(),
                    distinct: false,
                }],
            },
        )
        .unwrap(),
        ExecutableOperator::new(
            7,
            ExecutableOperatorKind::Join {
                left: 4,
                right: 6,
                kind: JoinKind::Inner,
                predicate: Some(Expression::new(LogicalExpr::Binary {
                    left: Box::new(LogicalExpr::Column("left_id".into())),
                    operator: BinaryOperator::Equal,
                    right: Box::new(LogicalExpr::Column("right_id".into())),
                })),
            },
        )
        .unwrap(),
        ExecutableOperator::new(
            6,
            ExecutableOperatorKind::Project {
                input: 5,
                projections: vec![ExecutableProjection::new(
                    "right_id",
                    Expression::new(LogicalExpr::Property {
                        input: Box::new(LogicalExpr::Column("right_vertex".into())),
                        name: "score".into(),
                    }),
                )],
            },
        )
        .unwrap(),
        ExecutableOperator::new(
            5,
            ExecutableOperatorKind::Source {
                logical_node: 2,
                fragments: vec![1],
                output: "right_vertex".into(),
            },
        )
        .unwrap(),
        ExecutableOperator::new(
            4,
            ExecutableOperatorKind::Unwind {
                input: 3,
                expression: Expression::new(LogicalExpr::Column("items".into())),
                alias: "item".into(),
            },
        )
        .unwrap(),
        ExecutableOperator::new(
            3,
            ExecutableOperatorKind::Project {
                input: 2,
                projections: vec![
                    ExecutableProjection::new(
                        "left_id",
                        Expression::new(LogicalExpr::Property {
                            input: Box::new(LogicalExpr::Column("left_vertex".into())),
                            name: "score".into(),
                        }),
                    ),
                    ExecutableProjection::new(
                        "items",
                        Expression::new(LogicalExpr::List(vec![
                            LogicalExpr::Literal(Value::Integer(1)),
                            LogicalExpr::Literal(Value::Integer(2)),
                        ])),
                    ),
                ],
            },
        )
        .unwrap(),
        ExecutableOperator::new(
            2,
            ExecutableOperatorKind::Filter {
                input: 1,
                predicate: Expression::new(LogicalExpr::Binary {
                    left: Box::new(LogicalExpr::Property {
                        input: Box::new(LogicalExpr::Column("left_vertex".into())),
                        name: "score".into(),
                    }),
                    operator: BinaryOperator::GreaterThan,
                    right: Box::new(LogicalExpr::Literal(Value::Integer(0))),
                }),
            },
        )
        .unwrap(),
        ExecutableOperator::new(
            1,
            ExecutableOperatorKind::Source {
                logical_node: 1,
                fragments: vec![1],
                output: "left_vertex".into(),
            },
        )
        .unwrap(),
    ];
    let result_schema = RowSchema {
        fields: vec![
            Field {
                name: "item".into(),
                data_type: LogicalType::Any,
                nullable: true,
            },
            Field {
                name: "matches".into(),
                data_type: LogicalType::Integer,
                nullable: false,
            },
        ],
    };
    let plan = ExecutablePlan::with_operators(
        Version::new(1),
        vec![fragment],
        10,
        operators,
        result_schema,
    )
    .unwrap();
    let store = FixtureStore::new(capabilities, vec![vertex(1, 1), vertex(2, 2)], Vec::new());

    let mut stream = block_on(QueryRuntime::new(1).execute(
        &plan,
        storage_map(store.storage(false)),
        &snapshot(),
        QueryBudget::unlimited(),
        CancellationToken::new(),
        None,
    ))
    .unwrap();
    let rows = block_on(stream.collect()).unwrap();

    assert_eq!(
        rows.rows(),
        vec![vec![QueryValue::Integer(2), QueryValue::Integer(2)]]
    );
}
