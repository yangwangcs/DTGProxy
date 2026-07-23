use physical_plan::{
    ExchangeKind, FragmentId, JoinKind, MAX_RECURSIVE_PLAN_NODES, MemoryBudget, PhysicalApply,
    PhysicalOperator, PhysicalPlanBuilder, PhysicalPlanHeader, Placement, ValidationError,
};
use temporal_ir::{
    ApplyKind, ChildPlanId, Column, ProcedureArgument, ProcedureEffect, ProcedureIdentity,
    ProcedurePlacement, ProcedureYieldBinding, ResolvedProcedure, RowSchema, ScalarExpr, SlotId,
    ValueType,
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
