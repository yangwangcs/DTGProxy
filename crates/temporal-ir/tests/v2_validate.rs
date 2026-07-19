use temporal_ir::v2::{
    Column, LanguageProfile, LogicalNode, LogicalNodeId, LogicalOperator, LogicalPlan,
    LogicalPlanBuilder, PlanHeaderV2, RowSchema, ScalarExpr, SlotId, V2_PLAN_VERSION,
    ValidationError, ValueType,
};
use temporal_types::GraphValue;

fn header() -> PlanHeaderV2 {
    PlanHeaderV2::new(
        7,
        3,
        11,
        LanguageProfile::Cypher25,
        "Cypher 25 / 2026.07",
        [9; 32],
    )
    .expect("header should be valid")
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
        PlanHeaderV2::with_version(
            V2_PLAN_VERSION + 1,
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
            expected: V2_PLAN_VERSION,
            actual: V2_PLAN_VERSION + 1,
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
