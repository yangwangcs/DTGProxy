use cypher_compiler::{CompileSession, CypherCompiler};
use physical_plan::{
    AggregatePhase, MemoryBudget, PhysicalOperator, PhysicalPlanBuilder, PhysicalPlanHeader,
    Placement,
};
use query_executor::{
    ExecutionContext, RuntimeValue, TemporalRecordBatch, TemporalRegion, TemporalRow,
    coalesce_temporal_rows, distinct_temporal_rows, execute_interval_coordinator_operators,
    temporal_hash_join, temporal_hash_join_bounded, temporal_join, temporal_left_hash_join,
};
use query_optimizer::{DeploymentMode, Optimizer, OptimizerContext};
use temporal_ir::{Column, RowSchema, ScalarExpr, SlotId, SortKey, ValueType};
use temporal_types::{GraphValue, Interval, TransactionTime, ValidTime};

fn valid(start: i64, end: i64) -> Interval<ValidTime> {
    Interval::new(
        ValidTime::from_micros(start),
        Some(ValidTime::from_micros(end)),
    )
    .unwrap()
}

fn transaction(start: i64, end: i64) -> Interval<TransactionTime> {
    Interval::new(
        TransactionTime::new(start, 0),
        Some(TransactionTime::new(end, 0)),
    )
    .unwrap()
}

fn identity_project(schema: &RowSchema) -> PhysicalOperator {
    PhysicalOperator::Project {
        expressions: schema
            .columns()
            .iter()
            .map(|column| (column.slot(), ScalarExpr::Slot(column.slot())))
            .collect(),
        output: schema.clone(),
    }
}

#[test]
fn temporal_join_intersects_both_valid_and_transaction_regions() {
    let left = TemporalRow::new(
        vec![RuntimeValue::Integer(1)],
        TemporalRegion::new(valid(1, 10), transaction(10, 30)),
    );
    let right = TemporalRow::new(
        vec![RuntimeValue::Integer(2)],
        TemporalRegion::new(valid(5, 15), transaction(20, 40)),
    );

    let joined = temporal_join(&[left], &[right], |_, _| true);
    assert_eq!(joined.len(), 1);
    assert_eq!(
        joined[0].values(),
        &[RuntimeValue::Integer(1), RuntimeValue::Integer(2)]
    );
    assert_eq!(joined[0].region().valid(), valid(5, 10));
    assert_eq!(joined[0].region().transaction(), transaction(20, 30));
}

#[test]
fn temporal_join_canonicalizes_equivalent_provenance_before_coalescing() {
    use query_executor::TemporalProvenance;

    let left = vec![
        TemporalRow::with_provenance(
            vec![RuntimeValue::Integer(1)],
            TemporalRegion::new(valid(1, 5), transaction(10, 20)),
            vec![TemporalProvenance::Unwind(2), TemporalProvenance::Unwind(1)],
        ),
        TemporalRow::with_provenance(
            vec![RuntimeValue::Integer(1)],
            TemporalRegion::new(valid(5, 10), transaction(10, 20)),
            vec![TemporalProvenance::Unwind(1), TemporalProvenance::Unwind(2)],
        ),
    ];
    let right = TemporalRow::with_provenance(
        vec![RuntimeValue::Integer(2)],
        TemporalRegion::new(valid(1, 10), transaction(10, 20)),
        vec![TemporalProvenance::Unwind(3)],
    );

    let joined = temporal_join(&left, &[right], |_, _| true);

    assert_eq!(joined.len(), 1);
    assert_eq!(joined[0].region().valid(), valid(1, 10));
    assert_eq!(
        joined[0].provenance(),
        &[
            TemporalProvenance::Unwind(1),
            TemporalProvenance::Unwind(2),
            TemporalProvenance::Unwind(3)
        ]
    );
}

#[test]
fn temporal_hash_join_never_matches_null_keys() {
    let schema = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "key",
        ValueType::Any,
        true,
    )])
    .unwrap();
    let region = TemporalRegion::new(valid(1, 10), transaction(10, 20));

    let rows = temporal_hash_join(
        &[TemporalRow::new(vec![RuntimeValue::Null], region)],
        &schema,
        &[TemporalRow::new(vec![RuntimeValue::Null], region)],
        &schema,
        &[SlotId::new(0)],
        &schema,
    )
    .expect("temporal inner join");

    assert!(rows.is_empty());
}

#[test]
fn bounded_temporal_hash_join_rejects_the_first_row_beyond_memory_before_retention() {
    let left_schema = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "left",
        ValueType::Integer,
        false,
    )])
    .unwrap();
    let right_schema = RowSchema::new(vec![Column::new(
        SlotId::new(1),
        "right",
        ValueType::Integer,
        false,
    )])
    .unwrap();
    let output = RowSchema::new(vec![
        left_schema.columns()[0].clone(),
        right_schema.columns()[0].clone(),
    ])
    .unwrap();
    let region = TemporalRegion::new(valid(1, 5), transaction(10, 20));
    let left = vec![TemporalRow::new(vec![RuntimeValue::Integer(1)], region)];
    let right = vec![
        TemporalRow::new(vec![RuntimeValue::Integer(2)], region),
        TemporalRow::new(vec![RuntimeValue::Integer(3)], region),
    ];
    let one_row_bytes = TemporalRecordBatch::try_new(
        output.clone(),
        vec![TemporalRow::new(
            vec![RuntimeValue::Integer(1), RuntimeValue::Integer(2)],
            region,
        )],
    )
    .unwrap()
    .estimated_bytes();

    assert_eq!(
        temporal_hash_join_bounded(
            &left,
            &left_schema,
            &right,
            &right_schema,
            &[],
            &output,
            one_row_bytes,
        ),
        Err(query_executor::RuntimeError::MemoryLimitExceeded {
            limit: one_row_bytes,
            required: one_row_bytes * 2,
        })
    );
}

#[tokio::test]
async fn interval_subquery_apply_intersects_regions_without_losing_provenance() {
    let logical = CypherCompiler::new()
        .compile(
            "UNWIND [1, 2] AS value \
             CALL (value) { UNWIND [value, value + 10] AS child RETURN child AS copy } \
             RETURN value, copy",
            &CompileSession::new("accounts", 7, 3, 11).unwrap(),
        )
        .unwrap()
        .logical_plan()
        .clone();
    let physical = Optimizer::new()
        .optimize(
            &logical,
            OptimizerContext::new(DeploymentMode::PrimaryReplica, 1, 8 << 20, 8 << 20).unwrap(),
        )
        .unwrap();
    let region = TemporalRegion::new(valid(5, 15), transaction(20, 40));

    let rows = execute_interval_coordinator_operators(
        &physical.plan().fragments()[0],
        RowSchema::empty(),
        vec![TemporalRow::new(Vec::new(), region)],
        &ExecutionContext::default(),
    )
    .await
    .expect("interval Apply");

    assert_eq!(rows.len(), 4);
    assert!(rows.iter().all(|row| row.region() == region));
    assert!(rows.iter().all(|row| row.provenance().len() == 2));
    assert_eq!(
        rows.iter().map(|row| row.values()).collect::<Vec<_>>(),
        vec![
            &[RuntimeValue::Integer(1), RuntimeValue::Integer(1)][..],
            &[RuntimeValue::Integer(1), RuntimeValue::Integer(11)][..],
            &[RuntimeValue::Integer(2), RuntimeValue::Integer(2)][..],
            &[RuntimeValue::Integer(2), RuntimeValue::Integer(12)][..],
        ]
    );
}

#[test]
fn temporal_rows_with_equal_values_coalesce_only_when_both_regions_are_adjacent() {
    let rows = vec![
        TemporalRow::new(
            vec![RuntimeValue::Integer(7)],
            TemporalRegion::new(valid(1, 5), transaction(10, 20)),
        ),
        TemporalRow::new(
            vec![RuntimeValue::Integer(7)],
            TemporalRegion::new(valid(5, 9), transaction(10, 20)),
        ),
        TemporalRow::new(
            vec![RuntimeValue::Integer(7)],
            TemporalRegion::new(valid(9, 12), transaction(21, 30)),
        ),
    ];

    let coalesced = coalesce_temporal_rows(rows);
    assert_eq!(coalesced.len(), 2);
    assert_eq!(coalesced[0].region().valid(), valid(1, 9));
    assert_eq!(coalesced[1].region().valid(), valid(9, 12));
}

#[test]
fn temporal_distinct_removes_only_exact_rows_and_never_merges_unequal_regions() {
    let first_region = TemporalRegion::new(valid(1, 5), transaction(10, 20));
    let second_region = TemporalRegion::new(valid(5, 9), transaction(10, 20));
    let first = TemporalRow::new(vec![RuntimeValue::Integer(7)], first_region);
    let second = TemporalRow::new(vec![RuntimeValue::Integer(7)], second_region);

    let distinct = distinct_temporal_rows(vec![first.clone(), first, second]);

    assert_eq!(distinct.len(), 2);
    assert_eq!(distinct[0].region(), first_region);
    assert_eq!(distinct[1].region(), second_region);
}

#[test]
fn temporal_distinct_ignores_lineage_but_merges_it_deterministically() {
    use query_executor::TemporalProvenance;
    use temporal_storage::{ElementId, ElementRef, GraphId, PartitionId};

    let region = TemporalRegion::new(valid(1, 5), transaction(10, 20));
    let element = ElementRef::vertex(GraphId::new(7), PartitionId::new(2), ElementId::new(9));
    let first = TemporalRow::with_provenance(
        vec![RuntimeValue::Integer(7)],
        region,
        vec![
            TemporalProvenance::Unwind(1),
            TemporalProvenance::Element(element),
        ],
    );
    let second = TemporalRow::with_provenance(
        vec![RuntimeValue::Integer(7)],
        region,
        vec![
            TemporalProvenance::Unwind(0),
            TemporalProvenance::Element(element),
        ],
    );

    let distinct = distinct_temporal_rows(vec![first, second]);

    assert_eq!(distinct.len(), 1);
    assert_eq!(
        distinct[0].provenance(),
        &[
            TemporalProvenance::Element(element),
            TemporalProvenance::Unwind(0),
            TemporalProvenance::Unwind(1),
        ]
    );
}

#[tokio::test]
async fn interval_unwind_rejects_one_upstream_list_beyond_the_item_limit() {
    let output = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "value",
        ValueType::Any,
        true,
    )])
    .expect("output schema");
    let mut builder =
        PhysicalPlanBuilder::new(PhysicalPlanHeader::new(7, 3, 11, [4; 32]).expect("header"));
    let root = builder
        .add_fragment(
            Placement::Coordinator,
            vec![PhysicalOperator::Unwind {
                expression: ScalarExpr::Literal(GraphValue::List(vec![
                    GraphValue::Integer(1);
                    query_executor::MAX_BATCH_ROWS
                        + 1
                ])),
                binding: SlotId::new(0),
                output: output.clone(),
            }],
            output,
            MemoryBudget::new(64 << 20, 64 << 20).expect("budget"),
        )
        .expect("fragment");
    let plan = builder.finish(root).expect("plan");

    let error = execute_interval_coordinator_operators(
        &plan.fragments()[0],
        RowSchema::empty(),
        vec![TemporalRow::new(
            Vec::new(),
            TemporalRegion::new(valid(1, 5), transaction(10, 20)),
        )],
        &ExecutionContext::default(),
    )
    .await
    .expect_err("one UNWIND list above the item bound must fail");

    assert!(matches!(
        error,
        query_executor::RuntimeError::BatchTooLarge { .. }
    ));
}

#[test]
fn temporal_record_batch_validates_values_without_discarding_regions() {
    let schema = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "value",
        ValueType::Integer,
        false,
    )])
    .expect("schema");
    let region = TemporalRegion::new(valid(1, 5), transaction(10, 20));
    let batch = TemporalRecordBatch::try_new(
        schema,
        vec![TemporalRow::new(vec![RuntimeValue::Integer(9)], region)],
    )
    .expect("temporal batch");

    assert_eq!(batch.rows()[0].region(), region);
    assert_eq!(batch.rows()[0].values(), &[RuntimeValue::Integer(9)]);
    assert!(batch.estimated_bytes() > 0);
}

#[tokio::test]
async fn interval_skip_and_limit_preserve_the_selected_row_regions() {
    let schema = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "value",
        ValueType::Integer,
        false,
    )])
    .expect("schema");
    let mut builder =
        PhysicalPlanBuilder::new(PhysicalPlanHeader::new(7, 3, 11, [9; 32]).expect("header"));
    let root = builder
        .add_fragment(
            Placement::Coordinator,
            vec![
                identity_project(&schema),
                PhysicalOperator::Skip {
                    count: ScalarExpr::Literal(GraphValue::Integer(1)),
                },
                PhysicalOperator::Limit {
                    count: ScalarExpr::Literal(GraphValue::Integer(1)),
                },
            ],
            schema.clone(),
            MemoryBudget::new(1 << 20, 1 << 20).expect("budget"),
        )
        .expect("fragment");
    let plan = builder.finish(root).expect("plan");
    let expected_region = TemporalRegion::new(valid(5, 10), transaction(20, 30));

    let rows = execute_interval_coordinator_operators(
        &plan.fragments()[0],
        schema,
        vec![
            TemporalRow::new(
                vec![RuntimeValue::Integer(1)],
                TemporalRegion::new(valid(1, 5), transaction(10, 20)),
            ),
            TemporalRow::new(vec![RuntimeValue::Integer(2)], expected_region),
            TemporalRow::new(
                vec![RuntimeValue::Integer(3)],
                TemporalRegion::new(valid(10, 20), transaction(30, 40)),
            ),
        ],
        &ExecutionContext::default(),
    )
    .await
    .expect("interval skip/limit");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values(), &[RuntimeValue::Integer(2)]);
    assert_eq!(rows[0].region(), expected_region);
}

#[tokio::test]
async fn interval_row_pipeline_enforces_its_fragment_memory_budget() {
    let schema = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "value",
        ValueType::Integer,
        false,
    )])
    .expect("schema");
    let mut builder =
        PhysicalPlanBuilder::new(PhysicalPlanHeader::new(7, 3, 11, [5; 32]).expect("header"));
    let root = builder
        .add_fragment(
            Placement::Coordinator,
            vec![
                identity_project(&schema),
                PhysicalOperator::Sort {
                    keys: vec![SortKey::new(SlotId::new(0), true)],
                },
            ],
            schema.clone(),
            MemoryBudget::new(1, 1).expect("budget"),
        )
        .expect("fragment");
    let plan = builder.finish(root).expect("plan");

    let error = execute_interval_coordinator_operators(
        &plan.fragments()[0],
        schema,
        vec![TemporalRow::new(
            vec![RuntimeValue::Integer(1)],
            TemporalRegion::new(valid(1, 5), transaction(10, 20)),
        )],
        &ExecutionContext::default(),
    )
    .await
    .expect_err("the interval row must exceed the one-byte budget");

    assert!(matches!(
        error,
        query_executor::RuntimeError::MemoryLimitExceeded { limit: 1, .. }
    ));
}

#[tokio::test]
async fn interval_unwind_copies_the_input_temporal_region_to_each_element() {
    let input = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "values",
        ValueType::List(Box::new(ValueType::Integer)),
        false,
    )])
    .expect("input schema");
    let output = RowSchema::new(vec![
        Column::new(
            SlotId::new(0),
            "values",
            ValueType::List(Box::new(ValueType::Integer)),
            false,
        ),
        Column::new(SlotId::new(1), "value", ValueType::Integer, false),
    ])
    .expect("output schema");
    let mut builder =
        PhysicalPlanBuilder::new(PhysicalPlanHeader::new(7, 3, 11, [8; 32]).expect("header"));
    let root = builder
        .add_fragment(
            Placement::Coordinator,
            vec![
                identity_project(&input),
                PhysicalOperator::Unwind {
                    expression: ScalarExpr::Slot(SlotId::new(0)),
                    binding: SlotId::new(1),
                    output: output.clone(),
                },
            ],
            output,
            MemoryBudget::new(1 << 20, 1 << 20).expect("budget"),
        )
        .expect("fragment");
    let plan = builder.finish(root).expect("plan");
    let region = TemporalRegion::new(valid(1, 10), transaction(20, 30));

    let rows = execute_interval_coordinator_operators(
        &plan.fragments()[0],
        input,
        vec![TemporalRow::new(
            vec![RuntimeValue::List(vec![
                RuntimeValue::Integer(8),
                RuntimeValue::Integer(8),
            ])],
            region,
        )],
        &ExecutionContext::default(),
    )
    .await
    .expect("interval unwind");

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].values()[1], RuntimeValue::Integer(8));
    assert_eq!(rows[1].values()[1], RuntimeValue::Integer(8));
    assert!(rows.iter().all(|row| row.region() == region));
}

#[tokio::test]
async fn interval_sort_reorders_visible_values_without_changing_regions() {
    let schema = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "value",
        ValueType::Integer,
        false,
    )])
    .expect("schema");
    let mut builder =
        PhysicalPlanBuilder::new(PhysicalPlanHeader::new(7, 3, 11, [7; 32]).expect("header"));
    let root = builder
        .add_fragment(
            Placement::Coordinator,
            vec![
                identity_project(&schema),
                PhysicalOperator::Sort {
                    keys: vec![SortKey::new(SlotId::new(0), true)],
                },
            ],
            schema.clone(),
            MemoryBudget::new(1 << 20, 1 << 20).expect("budget"),
        )
        .expect("fragment");
    let plan = builder.finish(root).expect("plan");
    let first_region = TemporalRegion::new(valid(1, 5), transaction(10, 20));
    let second_region = TemporalRegion::new(valid(5, 9), transaction(20, 30));

    let rows = execute_interval_coordinator_operators(
        &plan.fragments()[0],
        schema,
        vec![
            TemporalRow::new(vec![RuntimeValue::Integer(2)], first_region),
            TemporalRow::new(vec![RuntimeValue::Integer(1)], second_region),
        ],
        &ExecutionContext::default(),
    )
    .await
    .expect("interval sort");

    assert_eq!(rows[0].values(), &[RuntimeValue::Integer(1)]);
    assert_eq!(rows[0].region(), second_region);
    assert_eq!(rows[1].values(), &[RuntimeValue::Integer(2)]);
    assert_eq!(rows[1].region(), first_region);
}

#[tokio::test]
async fn interval_aggregate_splits_count_over_valid_time_change_points() {
    let input = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "value",
        ValueType::Integer,
        false,
    )])
    .expect("input schema");
    let output = RowSchema::new(vec![Column::new(
        SlotId::new(1),
        "count",
        ValueType::Integer,
        false,
    )])
    .expect("output schema");
    let count = u32::from_be_bytes(
        blake3::hash(b"count").as_bytes()[..4]
            .try_into()
            .expect("function identifier"),
    );
    let mut builder =
        PhysicalPlanBuilder::new(PhysicalPlanHeader::new(7, 3, 11, [6; 32]).expect("header"));
    let root = builder
        .add_fragment(
            Placement::Coordinator,
            vec![
                identity_project(&input),
                PhysicalOperator::Aggregate {
                    phase: AggregatePhase::Single,
                    grouping: Vec::new(),
                    aggregates: vec![{
                        (
                            SlotId::new(1),
                            ScalarExpr::Function {
                                function_id: count,
                                arguments: Vec::new(),
                            },
                        )
                    }],
                    output: output.clone(),
                },
            ],
            output.clone(),
            MemoryBudget::new(1 << 20, 1 << 20).expect("budget"),
        )
        .expect("fragment");
    let plan = builder.finish(root).expect("plan");

    let rows = execute_interval_coordinator_operators(
        &plan.fragments()[0],
        input,
        vec![
            TemporalRow::new(
                vec![RuntimeValue::Integer(1)],
                TemporalRegion::new(valid(1, 5), transaction(10, 20)),
            ),
            TemporalRow::new(
                vec![RuntimeValue::Integer(2)],
                TemporalRegion::new(valid(3, 7), transaction(10, 20)),
            ),
        ],
        &ExecutionContext::default(),
    )
    .await
    .expect("interval aggregate");

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].values(), &[RuntimeValue::Integer(1)]);
    assert_eq!(rows[0].region().valid(), valid(1, 3));
    assert_eq!(rows[1].values(), &[RuntimeValue::Integer(2)]);
    assert_eq!(rows[1].region().valid(), valid(3, 5));
    assert_eq!(rows[2].values(), &[RuntimeValue::Integer(1)]);
    assert_eq!(rows[2].region().valid(), valid(5, 7));
    assert!(
        rows.iter()
            .all(|row| row.region().transaction() == transaction(10, 20))
    );
}

#[test]
fn temporal_hash_join_intersects_matching_rows_and_remaps_shared_slots() {
    let left_schema = RowSchema::new(vec![
        Column::new(SlotId::new(0), "key", ValueType::Integer, false),
        Column::new(SlotId::new(1), "left", ValueType::String, false),
    ])
    .expect("left schema");
    let right_schema = RowSchema::new(vec![
        Column::new(SlotId::new(0), "key", ValueType::Integer, false),
        Column::new(SlotId::new(2), "right", ValueType::Integer, false),
    ])
    .expect("right schema");
    let output = RowSchema::new(vec![
        Column::new(SlotId::new(0), "key", ValueType::Integer, false),
        Column::new(SlotId::new(1), "left", ValueType::String, false),
        Column::new(SlotId::new(2), "right", ValueType::Integer, false),
    ])
    .expect("output schema");

    let rows = temporal_hash_join(
        &[TemporalRow::new(
            vec![
                RuntimeValue::Integer(7),
                RuntimeValue::String("left".into()),
            ],
            TemporalRegion::new(valid(1, 8), transaction(10, 30)),
        )],
        &left_schema,
        &[TemporalRow::new(
            vec![RuntimeValue::Integer(7), RuntimeValue::Integer(9)],
            TemporalRegion::new(valid(3, 10), transaction(20, 40)),
        )],
        &right_schema,
        &[SlotId::new(0)],
        &output,
    )
    .expect("temporal hash join");

    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].values(),
        &[
            RuntimeValue::Integer(7),
            RuntimeValue::String("left".into()),
            RuntimeValue::Integer(9)
        ]
    );
    assert_eq!(rows[0].region().valid(), valid(3, 8));
    assert_eq!(rows[0].region().transaction(), transaction(20, 30));
}

#[test]
fn temporal_left_hash_join_emits_nulls_for_the_unmatched_temporal_remainder() {
    let left_schema = RowSchema::new(vec![
        Column::new(SlotId::new(0), "key", ValueType::Integer, false),
        Column::new(SlotId::new(1), "left", ValueType::String, false),
    ])
    .expect("left schema");
    let right_schema = RowSchema::new(vec![
        Column::new(SlotId::new(0), "key", ValueType::Integer, false),
        Column::new(SlotId::new(2), "right", ValueType::Integer, false),
    ])
    .expect("right schema");
    let output = RowSchema::new(vec![
        Column::new(SlotId::new(0), "key", ValueType::Integer, false),
        Column::new(SlotId::new(1), "left", ValueType::String, false),
        Column::new(SlotId::new(2), "right", ValueType::Integer, true),
    ])
    .expect("output schema");
    let rows = temporal_left_hash_join(
        &[TemporalRow::new(
            vec![
                RuntimeValue::Integer(7),
                RuntimeValue::String("left".into()),
            ],
            TemporalRegion::new(valid(1, 10), transaction(10, 20)),
        )],
        &left_schema,
        &[TemporalRow::new(
            vec![RuntimeValue::Integer(7), RuntimeValue::Integer(9)],
            TemporalRegion::new(valid(3, 5), transaction(10, 20)),
        )],
        &right_schema,
        &[SlotId::new(0)],
        &output,
    )
    .expect("temporal left join");

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].region().valid(), valid(1, 3));
    assert_eq!(rows[0].values()[2], RuntimeValue::Null);
    assert_eq!(rows[1].region().valid(), valid(3, 5));
    assert_eq!(rows[1].values()[2], RuntimeValue::Integer(9));
    assert_eq!(rows[2].region().valid(), valid(5, 10));
    assert_eq!(rows[2].values()[2], RuntimeValue::Null);
}

#[test]
fn temporal_left_hash_join_splits_the_unmatched_bitemporal_area() {
    let left_schema = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "left",
        ValueType::Integer,
        false,
    )])
    .expect("left schema");
    let right_schema = RowSchema::new(vec![Column::new(
        SlotId::new(1),
        "right",
        ValueType::Integer,
        false,
    )])
    .expect("right schema");
    let output = RowSchema::new(vec![
        Column::new(SlotId::new(0), "left", ValueType::Integer, false),
        Column::new(SlotId::new(1), "right", ValueType::Integer, true),
    ])
    .expect("output schema");
    let rows = temporal_left_hash_join(
        &[TemporalRow::new(
            vec![RuntimeValue::Integer(1)],
            TemporalRegion::new(valid(1, 10), transaction(10, 20)),
        )],
        &left_schema,
        &[TemporalRow::new(
            vec![RuntimeValue::Integer(2)],
            TemporalRegion::new(valid(3, 5), transaction(12, 18)),
        )],
        &right_schema,
        &[],
        &output,
    )
    .expect("temporal left join");

    assert_eq!(rows.len(), 5);
    assert_eq!(
        rows.iter()
            .filter(|row| row.values()[1] == RuntimeValue::Integer(2))
            .count(),
        1
    );
    assert_eq!(
        rows.iter()
            .filter(|row| row.values()[1] == RuntimeValue::Null)
            .count(),
        4
    );
    let matched = rows
        .iter()
        .find(|row| row.values()[1] == RuntimeValue::Integer(2))
        .expect("match");
    assert_eq!(matched.region().valid(), valid(3, 5));
    assert_eq!(matched.region().transaction(), transaction(12, 18));
}
