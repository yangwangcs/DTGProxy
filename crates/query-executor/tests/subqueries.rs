use cypher_compiler::{CompileSession, CypherCompiler};
use physical_plan::{
    MemoryBudget, PhysicalApply, PhysicalOperator, PhysicalPlanBuilder, PhysicalPlanHeader,
    Placement,
};
use query_executor::{BatchExecutor, ExecutionContext, RuntimeValue};
use query_optimizer::{DeploymentMode, Optimizer, OptimizerContext};
use temporal_ir::{ApplyKind, ChildPlanId, Column, RowSchema, ScalarExpr, SlotId, ValueType};
use temporal_types::GraphValue;

fn plan(query: &str) -> physical_plan::PhysicalPlan {
    let logical = CypherCompiler::new()
        .compile(query, &CompileSession::new("accounts", 7, 3, 11).unwrap())
        .expect("compile")
        .logical_plan()
        .clone();
    Optimizer::new()
        .optimize(
            &logical,
            OptimizerContext::new(DeploymentMode::PrimaryReplica, 1, 8 << 20, 8 << 20).unwrap(),
        )
        .expect("optimize")
        .plan()
        .clone()
}

#[tokio::test]
async fn correlated_read_subquery_runs_per_outer_row_and_empty_child_has_inner_apply_semantics() {
    let physical = plan(
        "UNWIND [1, 2] AS value \
         CALL (value) { UNWIND [value, value + 10] AS child RETURN child AS copy } \
         RETURN value, copy",
    );
    let rows = BatchExecutor::new()
        .execute_fragment(
            &physical.fragments()[0],
            &ExecutionContext::default(),
            Vec::new(),
        )
        .await
        .expect("correlated Apply");
    assert_eq!(
        rows[0].rows(),
        &[
            vec![RuntimeValue::Integer(1), RuntimeValue::Integer(1)],
            vec![RuntimeValue::Integer(1), RuntimeValue::Integer(11)],
            vec![RuntimeValue::Integer(2), RuntimeValue::Integer(2)],
            vec![RuntimeValue::Integer(2), RuntimeValue::Integer(12)],
        ]
    );

    let empty = plan(
        "UNWIND [1, 2] AS value \
         CALL (value) { UNWIND [] AS child RETURN child AS copy } \
         RETURN value, copy",
    );
    let rows = BatchExecutor::new()
        .execute_fragment(
            &empty.fragments()[0],
            &ExecutionContext::default(),
            Vec::new(),
        )
        .await
        .expect("empty child is valid");
    assert!(rows.iter().all(|batch| batch.rows().is_empty()));
}

#[tokio::test]
async fn exists_short_circuits_and_count_handles_zero_null_and_multiple_rows() {
    let plan = plan(
        "UNWIND [null, [], [1, 2]] AS values \
         RETURN EXISTS { UNWIND values AS item RETURN item } AS present, \
                COUNT { UNWIND values AS item RETURN item } AS total",
    );
    let rows = BatchExecutor::new()
        .execute_fragment(
            &plan.fragments()[0],
            &ExecutionContext::default(),
            Vec::new(),
        )
        .await
        .expect("scalar Apply");

    assert_eq!(
        rows[0].rows(),
        &[
            vec![RuntimeValue::Boolean(false), RuntimeValue::Integer(0)],
            vec![RuntimeValue::Boolean(false), RuntimeValue::Integer(0)],
            vec![RuntimeValue::Boolean(true), RuntimeValue::Integer(2)],
        ]
    );
}

#[tokio::test]
async fn local_exists_stops_before_retaining_the_second_visible_child_row() {
    let child_output = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "item",
        ValueType::Integer,
        false,
    )])
    .unwrap();
    let mut child_builder =
        PhysicalPlanBuilder::new(PhysicalPlanHeader::new(7, 3, 11, [2; 32]).unwrap());
    let child_root = child_builder
        .add_fragment(
            Placement::Coordinator,
            vec![
                PhysicalOperator::Argument {
                    output: RowSchema::empty(),
                },
                PhysicalOperator::Unwind {
                    expression: ScalarExpr::Literal(GraphValue::List(vec![
                        GraphValue::Integer(1),
                        GraphValue::Integer(2),
                    ])),
                    binding: SlotId::new(0),
                    output: child_output.clone(),
                },
                PhysicalOperator::Project {
                    expressions: vec![(SlotId::new(0), ScalarExpr::Slot(SlotId::new(0)))],
                    output: child_output.clone(),
                },
            ],
            child_output,
            MemoryBudget::new(9, 9).unwrap(),
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
    let mut parent_builder =
        PhysicalPlanBuilder::new(PhysicalPlanHeader::new(7, 3, 11, [1; 32]).unwrap());
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
    let parent = parent_builder.finish(root).unwrap();

    let rows = BatchExecutor::new()
        .execute_fragment(
            &parent.fragments()[0],
            &ExecutionContext::default(),
            Vec::new(),
        )
        .await
        .expect("EXISTS must retain only the first child row under the nine-byte child budget");

    assert_eq!(rows[0].rows(), &[vec![RuntimeValue::Boolean(true)]]);
}

#[tokio::test]
async fn local_nested_apply_consumes_the_outer_recursive_budget() {
    let empty = RowSchema::empty();
    let boolean = |slot| {
        RowSchema::new(vec![Column::new(
            slot,
            "present",
            ValueType::Boolean,
            false,
        )])
        .unwrap()
    };
    let mut leaf_builder =
        PhysicalPlanBuilder::new(PhysicalPlanHeader::new(7, 3, 11, [3; 32]).unwrap());
    let leaf_root = leaf_builder
        .add_fragment(
            Placement::Coordinator,
            vec![
                PhysicalOperator::Argument {
                    output: empty.clone(),
                },
                PhysicalOperator::Finish,
            ],
            empty.clone(),
            MemoryBudget::new(1024, 1024).unwrap(),
        )
        .unwrap();
    let leaf = leaf_builder.finish(leaf_root).unwrap();

    let inner_output = boolean(SlotId::new(1));
    let mut inner_builder =
        PhysicalPlanBuilder::new(PhysicalPlanHeader::new(7, 3, 11, [2; 32]).unwrap());
    let inner_root = inner_builder
        .add_fragment(
            Placement::Coordinator,
            vec![
                PhysicalOperator::Argument {
                    output: empty.clone(),
                },
                PhysicalOperator::Apply {
                    apply: PhysicalApply::new(
                        ChildPlanId::new(2),
                        ApplyKind::Exists {
                            output: SlotId::new(1),
                        },
                        Vec::new(),
                        Vec::new(),
                        empty.clone(),
                        leaf,
                        100,
                        100,
                        temporal_ir::MAX_APPLY_DEPTH,
                    ),
                    output: inner_output.clone(),
                },
            ],
            inner_output.clone(),
            MemoryBudget::new(1024, 1024).unwrap(),
        )
        .unwrap();
    let inner = inner_builder.finish(inner_root).unwrap();

    let parent_output = boolean(SlotId::new(0));
    let mut parent_builder =
        PhysicalPlanBuilder::new(PhysicalPlanHeader::new(7, 3, 11, [1; 32]).unwrap());
    let parent_root = parent_builder
        .add_fragment(
            Placement::Coordinator,
            vec![
                PhysicalOperator::Argument {
                    output: empty.clone(),
                },
                PhysicalOperator::Apply {
                    apply: PhysicalApply::new(
                        ChildPlanId::new(1),
                        ApplyKind::Exists {
                            output: SlotId::new(0),
                        },
                        Vec::new(),
                        Vec::new(),
                        empty,
                        inner,
                        2,
                        100,
                        1,
                    ),
                    output: parent_output.clone(),
                },
            ],
            parent_output,
            MemoryBudget::new(1024, 1024).unwrap(),
        )
        .unwrap();
    let parent = parent_builder.finish(parent_root).unwrap();

    assert_eq!(
        BatchExecutor::new()
            .execute_fragment(
                &parent.fragments()[0],
                &ExecutionContext::default(),
                Vec::new(),
            )
            .await,
        Err(query_executor::RuntimeError::RecursivePlanViolation)
    );
}
