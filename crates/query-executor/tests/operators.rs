use std::collections::BTreeMap;

use physical_plan::{
    MemoryBudget, PhysicalOperator, PhysicalPlanBuilder, PhysicalPlanHeader, Placement,
};
use query_executor::{BatchExecutor, ExecutionContext, RecordBatch, RuntimeValue};
use temporal_ir::{Column, RowSchema, ScalarExpr, SlotId, ValueType};
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

#[tokio::test]
async fn filter_and_project_execute_with_cypher_null_semantics() {
    let input_schema = schema(0, "n", true);
    let output_schema = schema(1, "result", true);
    let operators = vec![
        PhysicalOperator::Project {
            expressions: vec![(SlotId::new(0), ScalarExpr::Slot(SlotId::new(0)))],
            output: input_schema.clone(),
        },
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
            output: output_schema.clone(),
        },
    ];
    let mut builder =
        PhysicalPlanBuilder::new(PhysicalPlanHeader::new(7, 3, 11, [9; 32]).expect("header"));
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
        .await
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

#[tokio::test]
async fn point_unwind_rejects_one_upstream_list_beyond_the_item_limit() {
    let output = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "value",
        ValueType::Any,
        true,
    )])
    .expect("output schema");
    let mut builder =
        PhysicalPlanBuilder::new(PhysicalPlanHeader::new(7, 3, 11, [3; 32]).expect("header"));
    let root = builder
        .add_fragment(
            Placement::Coordinator,
            vec![
                PhysicalOperator::Argument {
                    output: RowSchema::empty(),
                },
                PhysicalOperator::Unwind {
                    expression: ScalarExpr::Literal(GraphValue::List(vec![
                        GraphValue::Integer(1);
                        query_executor::MAX_BATCH_ROWS
                            + 1
                    ])),
                    binding: SlotId::new(0),
                    output: output.clone(),
                },
            ],
            output,
            MemoryBudget::new(64 << 20, 64 << 20).expect("budget"),
        )
        .expect("fragment");
    let plan = builder.finish(root).expect("plan");

    let error = BatchExecutor::new()
        .execute_fragment(
            &plan.fragments()[0],
            &ExecutionContext::default(),
            Vec::new(),
        )
        .await
        .expect_err("one UNWIND list above the item bound must fail");

    assert!(matches!(
        error,
        query_executor::RuntimeError::BatchTooLarge { .. }
    ));
}
