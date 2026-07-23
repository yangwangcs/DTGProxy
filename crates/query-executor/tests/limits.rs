use std::time::{Duration, Instant};

use physical_plan::{
    MemoryBudget, PhysicalOperator, PhysicalPlanBuilder, PhysicalPlanHeader, Placement,
};
use query_executor::{
    BatchExecutor, CancellationToken, ExecutionContext, MAX_BATCH_ROWS, RecordBatch, RuntimeError,
    RuntimeValue,
};
use temporal_ir::{Column, RowSchema, SlotId, ValueType};

fn integer_schema(nullable: bool) -> RowSchema {
    RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "n",
        ValueType::Integer,
        nullable,
    )])
    .expect("schema")
}

#[test]
fn record_batches_reject_invalid_shapes_and_unbounded_rows() {
    assert!(matches!(
        RecordBatch::try_new(integer_schema(false), vec![vec![]]),
        Err(RuntimeError::RowWidth {
            expected: 1,
            actual: 0
        })
    ));
    assert!(matches!(
        RecordBatch::try_new(
            integer_schema(false),
            vec![vec![RuntimeValue::Integer(1)]; MAX_BATCH_ROWS + 1],
        ),
        Err(RuntimeError::BatchTooLarge { .. })
    ));
    assert!(matches!(
        RecordBatch::try_new(integer_schema(false), vec![vec![RuntimeValue::Null]]),
        Err(RuntimeError::NullInNonNullableColumn { .. })
    ));
}

#[tokio::test]
async fn execution_honors_memory_cancellation_and_deadline_fences() {
    let output = integer_schema(false);
    let mut builder =
        PhysicalPlanBuilder::new(PhysicalPlanHeader::new(7, 3, 11, [9; 32]).expect("header"));
    let root = builder
        .add_fragment(
            Placement::Coordinator,
            vec![
                PhysicalOperator::Project {
                    expressions: vec![(
                        SlotId::new(0),
                        temporal_ir::ScalarExpr::Slot(SlotId::new(0)),
                    )],
                    output: output.clone(),
                },
                PhysicalOperator::Finish,
            ],
            output.clone(),
            MemoryBudget::new(1, 1).expect("budget"),
        )
        .expect("fragment");
    let plan = builder.finish(root).expect("plan");
    let input = RecordBatch::try_new(output, vec![vec![RuntimeValue::Integer(1)]]).expect("batch");
    let executor = BatchExecutor::new();

    assert!(matches!(
        executor
            .execute_fragment(
                &plan.fragments()[0],
                &ExecutionContext::default(),
                vec![input.clone()]
            )
            .await,
        Err(RuntimeError::MemoryLimitExceeded { .. })
    ));

    let cancellation = CancellationToken::new();
    cancellation.cancel();
    assert_eq!(
        executor
            .execute_fragment(
                &plan.fragments()[0],
                &ExecutionContext::default().with_cancellation(cancellation),
                vec![input.clone()]
            )
            .await,
        Err(RuntimeError::Cancelled)
    );
    assert_eq!(
        executor
            .execute_fragment(
                &plan.fragments()[0],
                &ExecutionContext::default()
                    .with_deadline(Instant::now() - Duration::from_millis(1)),
                vec![input]
            )
            .await,
        Err(RuntimeError::DeadlineExceeded)
    );
}
