use std::collections::BTreeMap;

use physical_plan::{
    MemoryBudget, PhysicalOperator, PhysicalPlanBuilder, PhysicalPlanHeaderV1, Placement,
};
use query_executor::v2::{BatchExecutor, ExecutionContext, RecordBatch, RuntimeValue};
use temporal_ir::v2::{Column, RowSchema, ScalarExpr, SlotId, ValueType};
use temporal_types::GraphValue;

fn schema(slot: u32, name: &str, nullable: bool) -> RowSchema {
    RowSchema::new(vec![Column::new(
        SlotId::new(slot),
        name,
        ValueType::Integer,
        nullable,
    )])
    .expect("schema")
}

#[test]
fn filter_and_project_execute_with_cypher_null_semantics() {
    let input_schema = schema(0, "n", true);
    let output_schema = schema(1, "result", true);
    let operators = vec![
        PhysicalOperator::Filter(ScalarExpr::Greater(
            Box::new(ScalarExpr::Slot(SlotId::new(0))),
            Box::new(ScalarExpr::Parameter("minimum".into())),
        )),
        PhysicalOperator::Project {
            expressions: vec![(
                SlotId::new(1),
                ScalarExpr::Add(
                    Box::new(ScalarExpr::Slot(SlotId::new(0))),
                    Box::new(ScalarExpr::Literal(GraphValue::Integer(10))),
                ),
            )],
        },
    ];
    let mut builder =
        PhysicalPlanBuilder::new(PhysicalPlanHeaderV1::new(7, 3, 11, [9; 32]).expect("header"));
    let root = builder
        .add_fragment(
            Placement::Coordinator,
            operators,
            output_schema.clone(),
            MemoryBudget::new(1 << 20, 1 << 20).expect("budget"),
        )
        .expect("fragment");
    let plan = builder.finish(root).expect("plan");
    let input = RecordBatch::try_new(
        input_schema,
        vec![
            vec![RuntimeValue::Integer(0)],
            vec![RuntimeValue::Integer(2)],
            vec![RuntimeValue::Null],
        ],
    )
    .expect("batch");
    let mut parameters = BTreeMap::new();
    parameters.insert("minimum".into(), RuntimeValue::Integer(1));

    let batches = BatchExecutor::new()
        .execute_fragment(
            &plan.fragments()[0],
            &ExecutionContext::new(parameters),
            vec![input],
        )
        .expect("execute");

    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].schema(), &output_schema);
    assert_eq!(batches[0].rows(), &[vec![RuntimeValue::Integer(12)]]);
}

#[test]
fn boolean_and_obeys_three_valued_logic() {
    let expression = ScalarExpr::And(
        Box::new(ScalarExpr::Literal(GraphValue::Boolean(false))),
        Box::new(ScalarExpr::Literal(GraphValue::Null)),
    );
    let value = BatchExecutor::new()
        .evaluate(
            &expression,
            &RowSchema::empty(),
            &[],
            &ExecutionContext::default(),
        )
        .expect("evaluate");

    assert_eq!(value, RuntimeValue::Boolean(false));
}

#[test]
fn equality_compares_collection_values_without_requiring_total_order() {
    let expression = ScalarExpr::Equal(
        Box::new(ScalarExpr::Literal(GraphValue::List(vec![
            GraphValue::Integer(1),
        ]))),
        Box::new(ScalarExpr::Literal(GraphValue::List(vec![
            GraphValue::Integer(2),
        ]))),
    );

    assert_eq!(
        BatchExecutor::new()
            .evaluate(
                &expression,
                &RowSchema::empty(),
                &[],
                &ExecutionContext::default(),
            )
            .expect("evaluate"),
        RuntimeValue::Boolean(false)
    );
}
