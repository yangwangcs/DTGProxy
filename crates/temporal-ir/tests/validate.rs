use temporal_ir::{
    ApplyKind, ApplySlotMapping, ChildPlanId, Column, LanguageProfile, LogicalApply, LogicalNode,
    LogicalNodeId, LogicalOperator, LogicalPlan, LogicalPlanBuilder, MAX_APPLY_DEPTH,
    MAX_APPLY_INVOCATIONS, MAX_APPLY_OUTPUT_ROWS, PLAN_VERSION, PlanHeader, ProcedureArgument,
    ProcedureEffect, ProcedureIdentity, ProcedurePlacement, ProcedureYieldBinding,
    ResolvedProcedure, RowSchema, ScalarExpr, SlotId, TemporalJoinKind, ValidationError, ValueType,
};
use temporal_types::GraphValue;

fn header() -> PlanHeader {
    header_with_fingerprint(9)
}

fn header_with_fingerprint(byte: u8) -> PlanHeader {
    PlanHeader::new(
        7,
        3,
        11,
        LanguageProfile::Cypher25,
        "Cypher 25 / 2026.07",
        [byte; 32],
    )
    .expect("header should be valid")
}

fn argument_plan(header: PlanHeader, schema: RowSchema) -> LogicalPlan {
    let mut builder = LogicalPlanBuilder::new(header);
    let root = builder
        .add(LogicalOperator::Argument, vec![], schema)
        .expect("argument");
    builder.finish(root).expect("argument plan")
}

fn node_schema() -> RowSchema {
    RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "n",
        ValueType::Node,
        false,
    )])
    .expect("schema should be valid")
}

#[test]
fn builds_and_validates_a_typed_acyclic_plan() {
    let mut builder = LogicalPlanBuilder::new(header());
    let scan = builder
        .add(
            LogicalOperator::NodeScan {
                binding: SlotId::new(0),
                labels: vec![42],
            },
            vec![],
            node_schema(),
        )
        .expect("scan should build");
    let filter = builder
        .add(
            LogicalOperator::Filter {
                predicate: ScalarExpr::Equal(
                    Box::new(ScalarExpr::Slot(SlotId::new(0))),
                    Box::new(ScalarExpr::Literal(GraphValue::Integer(7))),
                ),
            },
            vec![scan],
            node_schema(),
        )
        .expect("filter should build");
    let plan = builder.finish(filter).expect("plan should finish");

    plan.validate().expect("plan should validate");
    assert_eq!(plan.root(), filter);
    assert_eq!(plan.output(), &node_schema());
}

#[test]
fn rejects_recursive_apply_cycle_unknown_import_slot_and_unbounded_batch_spec() {
    let parent_schema = node_schema();
    let child_schema = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "n",
        ValueType::Node,
        false,
    )])
    .unwrap();
    let child = argument_plan(header_with_fingerprint(8), child_schema);

    let mut unknown_builder = LogicalPlanBuilder::new(header());
    let parent = unknown_builder
        .add(LogicalOperator::Argument, vec![], parent_schema.clone())
        .unwrap();
    let unknown = unknown_builder
        .add(
            LogicalOperator::Apply {
                apply: LogicalApply::new(
                    ChildPlanId::new(1),
                    ApplyKind::Inner,
                    vec![ApplySlotMapping::new(SlotId::new(99), SlotId::new(0))],
                    Vec::new(),
                    child.clone(),
                    MAX_APPLY_INVOCATIONS,
                    MAX_APPLY_OUTPUT_ROWS,
                    MAX_APPLY_DEPTH,
                ),
            },
            vec![parent],
            parent_schema.clone(),
        )
        .expect_err("unknown parent import must fail");
    assert_eq!(
        unknown,
        ValidationError::UnknownSlot {
            node: LogicalNodeId::new(1),
            slot: SlotId::new(99),
        }
    );

    let mut budget_builder = LogicalPlanBuilder::new(header());
    let parent = budget_builder
        .add(LogicalOperator::Argument, vec![], parent_schema.clone())
        .unwrap();
    let budget = budget_builder
        .add(
            LogicalOperator::Apply {
                apply: LogicalApply::new(
                    ChildPlanId::new(2),
                    ApplyKind::Inner,
                    vec![ApplySlotMapping::new(SlotId::new(0), SlotId::new(0))],
                    Vec::new(),
                    child,
                    0,
                    MAX_APPLY_OUTPUT_ROWS,
                    MAX_APPLY_DEPTH,
                ),
            },
            vec![parent],
            parent_schema.clone(),
        )
        .expect_err("zero Apply invocation budget must fail");
    assert_eq!(budget, ValidationError::InvalidApplyBudget);

    let grandchild = argument_plan(header_with_fingerprint(6), RowSchema::empty());
    let mut child_builder = LogicalPlanBuilder::new(header_with_fingerprint(7));
    let child_argument = child_builder
        .add(LogicalOperator::Argument, vec![], RowSchema::empty())
        .unwrap();
    let child_apply = child_builder
        .add(
            LogicalOperator::Apply {
                apply: LogicalApply::new(
                    ChildPlanId::new(7),
                    ApplyKind::Inner,
                    Vec::new(),
                    Vec::new(),
                    grandchild,
                    MAX_APPLY_INVOCATIONS,
                    MAX_APPLY_OUTPUT_ROWS,
                    MAX_APPLY_DEPTH,
                ),
            },
            vec![child_argument],
            RowSchema::empty(),
        )
        .unwrap();
    let child = child_builder.finish(child_apply).unwrap();
    let mut parent_builder = LogicalPlanBuilder::new(header());
    let parent = parent_builder
        .add(LogicalOperator::Argument, vec![], RowSchema::empty())
        .unwrap();
    let repeated_root = parent_builder
        .add(
            LogicalOperator::Apply {
                apply: LogicalApply::new(
                    ChildPlanId::new(7),
                    ApplyKind::Inner,
                    Vec::new(),
                    Vec::new(),
                    child,
                    MAX_APPLY_INVOCATIONS,
                    MAX_APPLY_OUTPUT_ROWS,
                    MAX_APPLY_DEPTH,
                ),
            },
            vec![parent],
            RowSchema::empty(),
        )
        .expect("each child is valid in isolation");
    let repeated = parent_builder
        .finish(repeated_root)
        .expect_err("repeated nested child identity must fail");
    assert_eq!(
        repeated,
        ValidationError::DuplicateChildPlanIdentity(ChildPlanId::new(7))
    );
}

#[test]
fn rejects_an_input_that_does_not_precede_its_consumer() {
    let mut builder = LogicalPlanBuilder::new(header());
    let error = builder
        .add(
            LogicalOperator::Finish,
            vec![LogicalNodeId::new(0)],
            RowSchema::empty(),
        )
        .expect_err("forward reference must fail");

    assert_eq!(
        error,
        ValidationError::InvalidInput {
            node: LogicalNodeId::new(0),
            input: LogicalNodeId::new(0),
        }
    );
}

#[test]
fn rejects_unknown_plan_versions() {
    let plan = LogicalPlan::from_parts(
        PlanHeader::with_version(
            PLAN_VERSION + 1,
            7,
            3,
            11,
            LanguageProfile::Cypher25,
            "Cypher 25 / 2026.07",
            [9; 32],
        )
        .expect("header shape should build"),
        vec![LogicalNode::new(
            LogicalOperator::Argument,
            vec![],
            RowSchema::empty(),
        )],
        LogicalNodeId::new(0),
        RowSchema::empty(),
    );

    assert_eq!(
        plan.validate(),
        Err(ValidationError::UnsupportedVersion {
            expected: PLAN_VERSION,
            actual: PLAN_VERSION + 1,
        })
    );
}

#[test]
fn rejects_duplicate_schema_slots() {
    let error = RowSchema::new(vec![
        Column::new(SlotId::new(0), "left", ValueType::Integer, false),
        Column::new(SlotId::new(0), "right", ValueType::Integer, false),
    ])
    .expect_err("slot must be unique");

    assert_eq!(error, ValidationError::DuplicateSlot(SlotId::new(0)));
}

#[test]
fn temporal_join_validates_keys_and_left_output_nullability() {
    let left = RowSchema::new(vec![
        Column::new(SlotId::new(0), "key", ValueType::Integer, false),
        Column::new(SlotId::new(1), "left", ValueType::String, false),
    ])
    .unwrap();
    let right = RowSchema::new(vec![
        Column::new(SlotId::new(0), "key", ValueType::Integer, false),
        Column::new(SlotId::new(2), "right", ValueType::Integer, false),
    ])
    .unwrap();
    let output = RowSchema::new(vec![
        Column::new(SlotId::new(0), "key", ValueType::Integer, false),
        Column::new(SlotId::new(1), "left", ValueType::String, false),
        Column::new(SlotId::new(2), "right", ValueType::Integer, true),
    ])
    .unwrap();
    let mut builder = LogicalPlanBuilder::new(header());
    let left_node = builder
        .add(LogicalOperator::Argument, vec![], left)
        .unwrap();
    let right_node = builder
        .add(LogicalOperator::Argument, vec![], right)
        .unwrap();
    let join = builder
        .add(
            LogicalOperator::TemporalJoin {
                kind: TemporalJoinKind::Left,
                keys: vec![SlotId::new(0)],
            },
            vec![left_node, right_node],
            output,
        )
        .expect("valid TemporalJoin");
    builder.finish(join).expect("valid TemporalJoin plan");

    let left = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "key",
        ValueType::Integer,
        false,
    )])
    .unwrap();
    let right = RowSchema::new(vec![
        Column::new(SlotId::new(0), "key", ValueType::Integer, false),
        Column::new(SlotId::new(1), "right", ValueType::Integer, false),
    ])
    .unwrap();
    let invalid_output = RowSchema::new(vec![
        Column::new(SlotId::new(0), "key", ValueType::Integer, false),
        Column::new(SlotId::new(1), "right", ValueType::Integer, false),
    ])
    .unwrap();
    let mut builder = LogicalPlanBuilder::new(header());
    let left_node = builder
        .add(LogicalOperator::Argument, vec![], left)
        .unwrap();
    let right_node = builder
        .add(LogicalOperator::Argument, vec![], right)
        .unwrap();
    assert_eq!(
        builder.add(
            LogicalOperator::TemporalJoin {
                kind: TemporalJoinKind::Left,
                keys: vec![SlotId::new(0)],
            },
            vec![left_node, right_node],
            invalid_output,
        ),
        Err(ValidationError::TemporalJoinSchemaMismatch)
    );
}

fn resolved_procedure(
    identity: ProcedureIdentity,
    argument: ScalarExpr,
    provider_output: RowSchema,
    output_slot: SlotId,
) -> ResolvedProcedure {
    ResolvedProcedure::new(
        identity,
        "dtg.test.rows",
        vec![ProcedureArgument::new("input", argument)],
        vec![ProcedureYieldBinding::new(0, output_slot)],
        provider_output,
        ProcedureEffect::ReadOnly,
        ProcedurePlacement::Coordinator,
        10,
        10,
        100,
        1024,
        4096,
        true,
    )
}

#[test]
fn rejects_procedure_plan_with_unknown_argument_slot_or_schema_mismatch() {
    let input = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "input",
        ValueType::Integer,
        false,
    )])
    .unwrap();
    let provider = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "value",
        ValueType::Integer,
        false,
    )])
    .unwrap();
    let output = RowSchema::new(vec![
        Column::new(SlotId::new(0), "input", ValueType::Integer, false),
        Column::new(SlotId::new(1), "value", ValueType::Integer, false),
    ])
    .unwrap();
    let identity = ProcedureIdentity::new([7; 32], 11, 13);

    let mut builder = LogicalPlanBuilder::new(header());
    let argument = builder
        .add(LogicalOperator::Argument, vec![], input.clone())
        .unwrap();
    let unknown_slot = builder
        .add(
            LogicalOperator::ProcedureCall {
                procedure: resolved_procedure(
                    identity,
                    ScalarExpr::Slot(SlotId::new(99)),
                    provider.clone(),
                    SlotId::new(1),
                ),
            },
            vec![argument],
            output.clone(),
        )
        .expect_err("unknown correlated input slot must fail");
    assert!(
        matches!(unknown_slot, ValidationError::UnknownSlot { slot, .. } if slot == SlotId::new(99))
    );

    let invalid_identity = resolved_procedure(
        ProcedureIdentity::new([0; 32], 0, 0),
        ScalarExpr::Slot(SlotId::new(0)),
        provider.clone(),
        SlotId::new(1),
    );
    let error = builder
        .add(
            LogicalOperator::ProcedureCall {
                procedure: invalid_identity,
            },
            vec![argument],
            output.clone(),
        )
        .expect_err("descriptor revision and authority are mandatory");
    assert_eq!(error, ValidationError::InvalidProcedureIdentity);

    let wrong_output = RowSchema::new(vec![
        Column::new(SlotId::new(0), "input", ValueType::Integer, false),
        Column::new(SlotId::new(1), "value", ValueType::String, false),
    ])
    .unwrap();
    let error = builder
        .add(
            LogicalOperator::ProcedureCall {
                procedure: resolved_procedure(
                    identity,
                    ScalarExpr::Slot(SlotId::new(0)),
                    provider,
                    SlotId::new(1),
                ),
            },
            vec![argument],
            wrong_output,
        )
        .expect_err("provider/yield output schema must match exactly");
    assert_eq!(error, ValidationError::ProcedureSchemaMismatch);
}
