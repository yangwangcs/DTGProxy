use physical_plan::{
    AccessGuarantee, AggregatePhase, COUNT_AGGREGATE_FUNCTION_ID, ExchangeKind,
    FragmentExecutionBudget, FragmentId, JoinKind, MAX_RECURSIVE_PLAN_NODES, MemoryBudget,
    PhysicalAccess, PhysicalApply, PhysicalOperator, PhysicalPlanBuilder, PhysicalPlanHeader,
    Placement, PrimitiveKind, RawScanBudget, ResidualPolicy, ValidationError,
};
use temporal_ir::{
    ApplyKind, ChangeAxis, ChildPlanId, Column, ProcedureArgument, ProcedureEffect,
    ProcedureIdentity, ProcedurePlacement, ProcedureYieldBinding, ResolvedProcedure, RowSchema,
    ScalarExpr, SlotId, TransactionTimeSpec, ValueType,
};

fn schema() -> RowSchema {
    RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "n",
        ValueType::Node,
        false,
    )])
    .expect("schema")
}

fn header() -> PhysicalPlanHeader {
    PhysicalPlanHeader::new(7, 3, 11, [8; 32]).expect("header")
}

fn count_expression(arguments: Vec<ScalarExpr>) -> ScalarExpr {
    ScalarExpr::Function {
        function_id: COUNT_AGGREGATE_FUNCTION_ID,
        arguments,
    }
}

fn count_plan(
    phase: AggregatePhase,
    placement: Placement,
    input: RowSchema,
    aggregates: Vec<(SlotId, ScalarExpr)>,
    output: RowSchema,
) -> Result<physical_plan::PhysicalPlan, ValidationError> {
    let mut builder = PhysicalPlanBuilder::new(header());
    let root = builder.add_fragment(
        placement,
        vec![
            PhysicalOperator::Argument { output: input },
            PhysicalOperator::Aggregate {
                phase,
                grouping: Vec::new(),
                aggregates,
                output: output.clone(),
            },
        ],
        output,
        MemoryBudget::new(1024, 1024).expect("budget"),
    )?;
    builder.finish(root)
}

fn integer_count_schema(slot: u32) -> RowSchema {
    RowSchema::new(vec![Column::new(
        SlotId::new(slot),
        "count",
        ValueType::Integer,
        false,
    )])
    .expect("count schema")
}

#[test]
fn partial_count_rejects_missing_extra_or_non_integer_aggregate_output_slots() {
    let aggregate = vec![(SlotId::new(1), count_expression(Vec::new()))];
    let extra = RowSchema::new(vec![
        Column::new(SlotId::new(1), "count", ValueType::Integer, false),
        Column::new(SlotId::new(2), "extra", ValueType::Integer, false),
    ])
    .expect("extra output");
    let wrong_type = RowSchema::new(vec![Column::new(
        SlotId::new(1),
        "count",
        ValueType::String,
        false,
    )])
    .expect("wrong type output");
    let nullable_output = RowSchema::new(vec![Column::new(
        SlotId::new(1),
        "count",
        ValueType::Integer,
        true,
    )])
    .expect("nullable output");

    for output in [RowSchema::empty(), extra, wrong_type, nullable_output] {
        assert_eq!(
            count_plan(
                AggregatePhase::PartialCount,
                Placement::AllShards,
                schema(),
                aggregate.clone(),
                output,
            ),
            Err(ValidationError::InvalidAggregatePhase(FragmentId::new(0)))
        );
    }
}

#[test]
fn partial_count_rejects_duplicate_aggregate_output_slots() {
    let output = RowSchema::new(vec![
        Column::new(SlotId::new(1), "count", ValueType::Integer, false),
        Column::new(SlotId::new(2), "extra", ValueType::Integer, false),
    ])
    .expect("output");

    assert_eq!(
        count_plan(
            AggregatePhase::PartialCount,
            Placement::AllShards,
            schema(),
            vec![
                (SlotId::new(1), count_expression(Vec::new())),
                (SlotId::new(1), count_expression(Vec::new())),
            ],
            output,
        ),
        Err(ValidationError::InvalidAggregatePhase(FragmentId::new(0)))
    );
}

#[test]
fn final_count_rejects_non_integer_or_non_matching_input_and_output_slots() {
    let aggregate = vec![(SlotId::new(1), count_expression(Vec::new()))];
    let extra_input = RowSchema::new(vec![
        Column::new(SlotId::new(1), "count", ValueType::Integer, false),
        Column::new(SlotId::new(2), "extra", ValueType::Integer, false),
    ])
    .expect("extra input");
    let wrong_type_input = RowSchema::new(vec![Column::new(
        SlotId::new(1),
        "count",
        ValueType::String,
        false,
    )])
    .expect("wrong type input");
    let nullable_input = RowSchema::new(vec![Column::new(
        SlotId::new(1),
        "count",
        ValueType::Integer,
        true,
    )])
    .expect("nullable input");
    let wrong_slot_output = integer_count_schema(2);

    for (input, output) in [
        (extra_input, integer_count_schema(1)),
        (wrong_type_input, integer_count_schema(1)),
        (nullable_input, integer_count_schema(1)),
        (integer_count_schema(1), wrong_slot_output),
    ] {
        assert_eq!(
            count_plan(
                AggregatePhase::FinalCount,
                Placement::Coordinator,
                input,
                aggregate.clone(),
                output,
            ),
            Err(ValidationError::InvalidAggregatePhase(FragmentId::new(0)))
        );
    }
}

#[test]
fn physical_header_tracks_a_non_zero_capability_generation() {
    let default_header = header();
    assert_eq!(default_header.capability_generation(), 1);

    let selected = default_header
        .with_capability_generation(9)
        .expect("non-zero generation");
    assert_eq!(selected.capability_generation(), 9);
    assert_eq!(
        header().with_capability_generation(0),
        Err(ValidationError::InvalidCapabilityGeneration)
    );
}

#[test]
fn fragment_access_metadata_is_aligned_and_backend_neutral() {
    let access = PhysicalAccess::Primitive {
        primitive: PrimitiveKind::CandidateScan,
        guarantee: AccessGuarantee::Candidate,
        residual: ResidualPolicy::Evaluate,
        constraints: Vec::new(),
    };
    let mut builder = PhysicalPlanBuilder::new(header());
    let root = builder
        .add_fragment_with_access(
            Placement::AllShards,
            vec![PhysicalOperator::NodeScan {
                binding: SlotId::new(0),
                labels: vec![42],
                output: schema(),
            }],
            vec![access.clone()],
            schema(),
            MemoryBudget::new(1024, 1024).expect("budget"),
        )
        .expect("fragment");
    let plan = builder.finish(root).expect("plan");

    assert_eq!(plan.fragments()[0].access(), &[access]);
}

#[test]
fn fragment_rejects_access_metadata_with_the_wrong_arity() {
    let mut builder = PhysicalPlanBuilder::new(header());
    assert_eq!(
        builder.add_fragment_with_access(
            Placement::AllShards,
            vec![PhysicalOperator::NodeScan {
                binding: SlotId::new(0),
                labels: Vec::new(),
                output: schema(),
            }],
            Vec::new(),
            schema(),
            MemoryBudget::new(1024, 1024).expect("budget"),
        ),
        Err(ValidationError::AccessMetadataMismatch(FragmentId::new(0)))
    );
}

#[test]
fn fragment_rejects_exact_primitive_without_a_residual() {
    let mut builder = PhysicalPlanBuilder::new(header());
    assert_eq!(
        builder.add_fragment_with_access(
            Placement::AllShards,
            vec![PhysicalOperator::NodeScan {
                binding: SlotId::new(0),
                labels: Vec::new(),
                output: schema(),
            }],
            vec![PhysicalAccess::Primitive {
                primitive: PrimitiveKind::CandidateScan,
                guarantee: AccessGuarantee::Exact,
                residual: ResidualPolicy::Omit,
                constraints: Vec::new(),
            }],
            schema(),
            MemoryBudget::new(1024, 1024).expect("budget"),
        ),
        Err(ValidationError::InvalidPhysicalAccess)
    );
}

#[test]
fn fragment_rejects_a_primitive_attached_to_the_wrong_operator() {
    let mut builder = PhysicalPlanBuilder::new(header());
    assert_eq!(
        builder.add_fragment_with_access(
            Placement::AllShards,
            vec![PhysicalOperator::Filter(ScalarExpr::Parameter(
                "predicate".into(),
            ))],
            vec![PhysicalAccess::Primitive {
                primitive: PrimitiveKind::CandidateScan,
                guarantee: AccessGuarantee::Candidate,
                residual: ResidualPolicy::Evaluate,
                constraints: Vec::new(),
            }],
            schema(),
            MemoryBudget::new(1024, 1024).expect("budget"),
        ),
        Err(ValidationError::InvalidPhysicalAccess)
    );
}

fn change_scan() -> PhysicalOperator {
    PhysicalOperator::ChangeScan {
        axis: ChangeAxis::ValidTime,
        start: ScalarExpr::Parameter("start".into()),
        end: ScalarExpr::Parameter("end".into()),
        system_snapshot: TransactionTimeSpec::Current,
    }
}

fn change_plan(
    operators: Vec<PhysicalOperator>,
    output: RowSchema,
) -> Result<physical_plan::PhysicalPlan, ValidationError> {
    let mut builder = PhysicalPlanBuilder::new(header());
    let root = builder.add_fragment(
        Placement::AllShards,
        operators,
        output,
        MemoryBudget::new(1024, 1024).expect("budget"),
    )?;
    builder.finish(root)
}

#[test]
fn validates_change_scan_after_node_or_relationship_source() {
    change_plan(
        vec![
            PhysicalOperator::NodeScan {
                binding: SlotId::new(0),
                labels: Vec::new(),
                output: schema(),
            },
            change_scan(),
        ],
        schema(),
    )
    .expect("node ChangeScan source");

    let relationship_schema = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "r",
        ValueType::Relationship,
        false,
    )])
    .expect("relationship schema");
    change_plan(
        vec![
            PhysicalOperator::RelationshipScan {
                binding: SlotId::new(0),
                types: Vec::new(),
                output: relationship_schema.clone(),
            },
            change_scan(),
        ],
        relationship_schema,
    )
    .expect("relationship ChangeScan source");
}

#[test]
fn rejects_change_scan_at_fragment_start() {
    assert_eq!(
        change_plan(
            vec![change_scan(), PhysicalOperator::Finish],
            RowSchema::empty()
        ),
        Err(ValidationError::InvalidChangeScanStructure(
            FragmentId::new(0)
        ))
    );
}

#[test]
fn rejects_change_scan_after_operator_index_one() {
    assert_eq!(
        change_plan(
            vec![
                PhysicalOperator::NodeScan {
                    binding: SlotId::new(0),
                    labels: Vec::new(),
                    output: schema(),
                },
                PhysicalOperator::Filter(ScalarExpr::Parameter("predicate".into())),
                change_scan(),
            ],
            schema(),
        ),
        Err(ValidationError::InvalidChangeScanStructure(
            FragmentId::new(0)
        ))
    );
}

#[test]
fn rejects_change_scan_without_graph_scan_source() {
    assert_eq!(
        change_plan(
            vec![
                PhysicalOperator::Argument {
                    output: RowSchema::empty(),
                },
                change_scan(),
            ],
            RowSchema::empty(),
        ),
        Err(ValidationError::InvalidChangeScanStructure(
            FragmentId::new(0)
        ))
    );
}

#[test]
fn rejects_duplicate_change_scan_in_fragment() {
    assert_eq!(
        change_plan(
            vec![
                PhysicalOperator::NodeScan {
                    binding: SlotId::new(0),
                    labels: Vec::new(),
                    output: schema(),
                },
                change_scan(),
                change_scan(),
            ],
            schema(),
        ),
        Err(ValidationError::InvalidChangeScanStructure(
            FragmentId::new(0)
        ))
    );
}

#[test]
fn rejects_change_scan_in_multiple_fragments() {
    let mut builder = PhysicalPlanBuilder::new(header());
    let mut sources = Vec::new();
    for _ in 0..2 {
        sources.push(
            builder
                .add_fragment(
                    Placement::AllShards,
                    vec![
                        PhysicalOperator::NodeScan {
                            binding: SlotId::new(0),
                            labels: Vec::new(),
                            output: schema(),
                        },
                        change_scan(),
                    ],
                    schema(),
                    MemoryBudget::new(1024, 1024).expect("budget"),
                )
                .expect("source fragment"),
        );
    }
    let root = builder
        .add_fragment(
            Placement::Coordinator,
            vec![PhysicalOperator::Union { all: true }],
            schema(),
            MemoryBudget::new(1024, 1024).expect("budget"),
        )
        .expect("root fragment");
    for source in sources {
        builder
            .add_exchange(source, root, ExchangeKind::Gather, schema(), 1)
            .expect("exchange");
    }

    assert_eq!(
        builder.finish(root),
        Err(ValidationError::InvalidChangeScanStructure(
            FragmentId::new(1)
        ))
    );
}

#[test]
fn validates_a_fragment_dag_with_bounded_exchange() {
    let mut builder = PhysicalPlanBuilder::new(header());
    let shard = builder
        .add_fragment(
            Placement::AllShards,
            vec![PhysicalOperator::NodeScan {
                binding: SlotId::new(0),
                labels: vec![42],
                output: schema(),
            }],
            schema(),
            MemoryBudget::new(64 * 1024 * 1024, 256 * 1024 * 1024).expect("budget"),
        )
        .expect("fragment");
    let coordinator = builder
        .add_fragment(
            Placement::Coordinator,
            vec![PhysicalOperator::Project {
                expressions: vec![(
                    SlotId::new(0),
                    temporal_ir::ScalarExpr::Slot(SlotId::new(0)),
                )],
                output: schema(),
            }],
            schema(),
            MemoryBudget::new(32 * 1024 * 1024, 64 * 1024 * 1024).expect("budget"),
        )
        .expect("fragment");
    builder
        .add_exchange(shard, coordinator, ExchangeKind::Gather, schema(), 8)
        .expect("exchange");
    let plan = builder.finish(coordinator).expect("plan");

    plan.validate().expect("valid physical plan");
    assert_eq!(plan.root(), coordinator);
}

#[test]
fn rejects_backward_exchange_edges() {
    let mut builder = PhysicalPlanBuilder::new(header());
    let first = builder
        .add_fragment(
            Placement::Coordinator,
            vec![PhysicalOperator::Project {
                expressions: vec![(
                    SlotId::new(0),
                    temporal_ir::ScalarExpr::Slot(SlotId::new(0)),
                )],
                output: schema(),
            }],
            schema(),
            MemoryBudget::new(1, 1).expect("budget"),
        )
        .expect("fragment");
    let second = builder
        .add_fragment(
            Placement::Coordinator,
            vec![PhysicalOperator::Project {
                expressions: vec![(
                    SlotId::new(0),
                    temporal_ir::ScalarExpr::Slot(SlotId::new(0)),
                )],
                output: schema(),
            }],
            schema(),
            MemoryBudget::new(1, 1).expect("budget"),
        )
        .expect("fragment");

    let error = builder
        .add_exchange(second, first, ExchangeKind::Gather, schema(), 1)
        .expect_err("backward edge must fail");
    assert_eq!(
        error,
        ValidationError::InvalidExchangeDirection {
            from: FragmentId::new(1),
            to: FragmentId::new(0),
        }
    );
}

#[test]
fn rejects_zero_memory_or_exchange_credit() {
    assert_eq!(
        MemoryBudget::new(0, 1),
        Err(ValidationError::InvalidMemoryBudget)
    );
}

#[test]
fn fragment_execution_budget_keeps_scan_units_separate_from_resident_memory() {
    let memory = MemoryBudget::new(64, 128).expect("memory budget");
    let scan = RawScanBudget::new(7, 4_096).expect("scan budget");
    let budget = FragmentExecutionBudget::new(memory, scan);

    assert_eq!(budget.resident_memory_bytes(), 64);
    assert_eq!(budget.spill_bytes(), 128);
    assert_eq!(budget.raw_scan().entry_limit(), 7);
    assert_eq!(budget.raw_scan().byte_limit(), 4_096);
}

#[test]
fn legacy_memory_budget_uses_scan_defaults_independent_of_memory_size() {
    let small =
        FragmentExecutionBudget::from(MemoryBudget::new(1, 1).expect("small memory budget"));
    let large = FragmentExecutionBudget::from(
        MemoryBudget::new(64 * 1024 * 1024, 64 * 1024 * 1024).expect("large memory budget"),
    );

    assert_eq!(small.raw_scan(), large.raw_scan());
    assert_ne!(small.resident_memory_bytes(), large.resident_memory_bytes());
    assert_eq!(
        RawScanBudget::new(0, 1),
        Err(ValidationError::InvalidRawScanBudget)
    );
    assert_eq!(
        RawScanBudget::new(1, 0),
        Err(ValidationError::InvalidRawScanBudget)
    );
}

fn procedure(arguments: Vec<ProcedureArgument>, max_input_rows: u64) -> ResolvedProcedure {
    ResolvedProcedure::new(
        ProcedureIdentity::new([7; 32], 3, 5),
        "dtg.test.procedure",
        arguments,
        Vec::new(),
        RowSchema::new(vec![Column::new(
            SlotId::new(0),
            "value",
            ValueType::Integer,
            false,
        )])
        .unwrap(),
        ProcedureEffect::ReadOnly,
        ProcedurePlacement::Coordinator,
        10,
        max_input_rows,
        10,
        1024,
        4096,
        false,
    )
}

fn procedure_plan(
    procedure: ResolvedProcedure,
    output: RowSchema,
) -> Result<physical_plan::PhysicalPlan, ValidationError> {
    let mut builder = PhysicalPlanBuilder::new(header());
    let root = builder.add_fragment(
        Placement::Coordinator,
        vec![
            PhysicalOperator::Argument {
                output: RowSchema::empty(),
            },
            PhysicalOperator::Procedure {
                procedure,
                output: output.clone(),
            },
        ],
        output,
        MemoryBudget::new(1024, 1024).unwrap(),
    )?;
    builder.finish(root)
}

fn single_column_schema(slot: u32, name: &str, nullable: bool) -> RowSchema {
    RowSchema::new(vec![Column::new(
        SlotId::new(slot),
        name,
        ValueType::Integer,
        nullable,
    )])
    .unwrap()
}

fn join_plan(
    left_schema: RowSchema,
    right_schema: RowSchema,
    operators: Vec<PhysicalOperator>,
    output: RowSchema,
) -> Result<physical_plan::PhysicalPlan, ValidationError> {
    let mut builder = PhysicalPlanBuilder::new(header());
    let left = builder.add_fragment(
        Placement::AllShards,
        vec![PhysicalOperator::NodeScan {
            binding: left_schema.columns()[0].slot(),
            labels: Vec::new(),
            output: left_schema.clone(),
        }],
        left_schema.clone(),
        MemoryBudget::new(1024, 1024).unwrap(),
    )?;
    let right = builder.add_fragment(
        Placement::AllShards,
        vec![PhysicalOperator::NodeScan {
            binding: right_schema.columns()[0].slot(),
            labels: Vec::new(),
            output: right_schema.clone(),
        }],
        right_schema.clone(),
        MemoryBudget::new(1024, 1024).unwrap(),
    )?;
    let root = builder.add_fragment(
        Placement::Coordinator,
        operators,
        output,
        MemoryBudget::new(1024, 1024).unwrap(),
    )?;
    builder.add_exchange(left, root, ExchangeKind::Gather, left_schema, 1)?;
    builder.add_exchange(right, root, ExchangeKind::Gather, right_schema, 1)?;
    builder.finish(root)
}

#[test]
fn rejects_hash_join_keys_missing_from_either_input() {
    let left = single_column_schema(0, "left_key", false);
    let right = single_column_schema(1, "right_value", false);
    let output =
        RowSchema::new(vec![left.columns()[0].clone(), right.columns()[0].clone()]).unwrap();

    assert_eq!(
        join_plan(
            left,
            right,
            vec![PhysicalOperator::HashJoin {
                kind: JoinKind::Inner,
                keys: vec![SlotId::new(0)],
            }],
            output,
        ),
        Err(ValidationError::HashJoinSchemaMismatch(FragmentId::new(2)))
    );
}

#[test]
fn rejects_hash_join_output_that_does_not_match_derived_schema() {
    let left = single_column_schema(0, "left", false);
    let right = single_column_schema(1, "right", false);

    assert_eq!(
        join_plan(
            left.clone(),
            right,
            vec![PhysicalOperator::HashJoin {
                kind: JoinKind::Inner,
                keys: Vec::new(),
            }],
            left,
        ),
        Err(ValidationError::FragmentOutputMismatch(FragmentId::new(2)))
    );
}

#[test]
fn left_hash_join_requires_right_side_output_columns_to_be_nullable() {
    let left = single_column_schema(0, "left", false);
    let right = single_column_schema(1, "right", false);
    let invalid_output =
        RowSchema::new(vec![left.columns()[0].clone(), right.columns()[0].clone()]).unwrap();
    assert_eq!(
        join_plan(
            left.clone(),
            right.clone(),
            vec![PhysicalOperator::HashJoin {
                kind: JoinKind::Left,
                keys: Vec::new(),
            }],
            invalid_output,
        ),
        Err(ValidationError::FragmentOutputMismatch(FragmentId::new(2)))
    );

    let valid_output = RowSchema::new(vec![
        left.columns()[0].clone(),
        Column::new(SlotId::new(1), "right", ValueType::Integer, true),
    ])
    .unwrap();
    join_plan(
        left,
        right,
        vec![PhysicalOperator::HashJoin {
            kind: JoinKind::Left,
            keys: Vec::new(),
        }],
        valid_output,
    )
    .unwrap();
}

#[test]
fn rejects_fragment_output_that_differs_from_final_operator_schema() {
    let actual = single_column_schema(0, "actual", false);
    let declared = single_column_schema(1, "declared", false);
    let mut builder = PhysicalPlanBuilder::new(header());
    let root = builder
        .add_fragment(
            Placement::AllShards,
            vec![PhysicalOperator::NodeScan {
                binding: SlotId::new(0),
                labels: Vec::new(),
                output: actual,
            }],
            declared,
            MemoryBudget::new(1024, 1024).unwrap(),
        )
        .unwrap();

    assert_eq!(
        builder.finish(root),
        Err(ValidationError::FragmentOutputMismatch(root))
    );
}

#[test]
fn hash_join_followed_by_procedure_uses_the_derived_join_schema() {
    let left = single_column_schema(0, "left", false);
    let right = single_column_schema(1, "right", false);
    let joined =
        RowSchema::new(vec![left.columns()[0].clone(), right.columns()[0].clone()]).unwrap();
    let procedure_output = RowSchema::new(vec![
        left.columns()[0].clone(),
        right.columns()[0].clone(),
        Column::new(SlotId::new(2), "score", ValueType::Integer, false),
    ])
    .unwrap();
    let procedure = ResolvedProcedure::new(
        ProcedureIdentity::new([7; 32], 3, 5),
        "dtg.test.joined",
        vec![ProcedureArgument::new(
            "right",
            ScalarExpr::Slot(SlotId::new(1)),
        )],
        vec![ProcedureYieldBinding::new(0, SlotId::new(2))],
        single_column_schema(9, "score", false),
        ProcedureEffect::ReadOnly,
        ProcedurePlacement::Coordinator,
        10,
        10,
        10,
        1024,
        4096,
        false,
    );

    join_plan(
        left,
        right,
        vec![
            PhysicalOperator::HashJoin {
                kind: JoinKind::Inner,
                keys: Vec::new(),
            },
            PhysicalOperator::Procedure {
                procedure,
                output: procedure_output.clone(),
            },
        ],
        procedure_output,
    )
    .unwrap();
    assert_eq!(joined.columns().len(), 2);
}

#[test]
fn rejects_zero_procedure_input_bound_and_duplicate_arguments() {
    assert_eq!(
        procedure_plan(procedure(Vec::new(), 0), RowSchema::empty()),
        Err(ValidationError::InvalidProcedure)
    );
    assert_eq!(
        procedure_plan(
            procedure(
                vec![
                    ProcedureArgument::new("value", ScalarExpr::Parameter("first".into())),
                    ProcedureArgument::new("value", ScalarExpr::Parameter("second".into())),
                ],
                10,
            ),
            RowSchema::empty(),
        ),
        Err(ValidationError::InvalidProcedure)
    );
}

#[test]
fn rejects_extra_procedure_output_columns_without_yield_bindings() {
    assert_eq!(
        procedure_plan(procedure(Vec::new(), 10), schema()),
        Err(ValidationError::ProcedureSchemaMismatch)
    );
}

#[test]
fn rejects_apply_child_without_one_exact_argument_source() {
    let mut child_builder =
        PhysicalPlanBuilder::new(PhysicalPlanHeader::new(7, 3, 11, [9; 32]).expect("child header"));
    let child_root = child_builder
        .add_fragment(
            Placement::Coordinator,
            vec![PhysicalOperator::Finish],
            RowSchema::empty(),
            MemoryBudget::new(1024, 1024).unwrap(),
        )
        .unwrap();
    let child = child_builder.finish(child_root).unwrap();
    let output = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "present",
        ValueType::Boolean,
        false,
    )])
    .unwrap();
    let mut parent_builder = PhysicalPlanBuilder::new(header());
    let root = parent_builder
        .add_fragment(
            Placement::Coordinator,
            vec![
                PhysicalOperator::Argument {
                    output: RowSchema::empty(),
                },
                PhysicalOperator::Apply {
                    apply: PhysicalApply::new(
                        ChildPlanId::new(1),
                        ApplyKind::Exists {
                            output: SlotId::new(0),
                        },
                        Vec::new(),
                        Vec::new(),
                        RowSchema::empty(),
                        child,
                        1,
                        1,
                        1,
                    ),
                    output: output.clone(),
                },
            ],
            output,
            MemoryBudget::new(1024, 1024).unwrap(),
        )
        .unwrap();

    assert_eq!(
        parent_builder.finish(root),
        Err(ValidationError::InvalidApplyArgumentSource)
    );
}

#[test]
fn rejects_apply_tree_whose_cumulative_nodes_exceed_the_global_bound() {
    let mut child_builder =
        PhysicalPlanBuilder::new(PhysicalPlanHeader::new(7, 3, 11, [9; 32]).expect("child header"));
    let mut operators = vec![PhysicalOperator::Argument {
        output: RowSchema::empty(),
    }];
    operators.extend(std::iter::repeat_n(
        PhysicalOperator::Finish,
        MAX_RECURSIVE_PLAN_NODES,
    ));
    let child_root = child_builder
        .add_fragment(
            Placement::Coordinator,
            operators,
            RowSchema::empty(),
            MemoryBudget::new(1024, 1024).unwrap(),
        )
        .unwrap();
    assert_eq!(
        child_builder.finish(child_root),
        Err(ValidationError::RecursivePlanNodeLimit)
    );
}

#[test]
fn rejects_procedure_arguments_that_reference_unknown_input_slots() {
    assert_eq!(
        procedure_plan(
            procedure(
                vec![ProcedureArgument::new(
                    "value",
                    ScalarExpr::Slot(SlotId::new(99)),
                )],
                10,
            ),
            RowSchema::empty(),
        ),
        Err(ValidationError::ProcedureSchemaMismatch)
    );
}
