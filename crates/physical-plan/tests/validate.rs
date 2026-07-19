use physical_plan::{
    ExchangeKind, FragmentId, MemoryBudget, PhysicalOperator, PhysicalPlanBuilder,
    PhysicalPlanHeaderV1, Placement, ValidationError,
};
use temporal_ir::v2::{Column, RowSchema, SlotId, ValueType};

fn schema() -> RowSchema {
    RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "n",
        ValueType::Node,
        false,
    )])
    .expect("schema")
}

fn header() -> PhysicalPlanHeaderV1 {
    PhysicalPlanHeaderV1::new(7, 3, 11, [8; 32]).expect("header")
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
                    temporal_ir::v2::ScalarExpr::Slot(SlotId::new(0)),
                )],
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
                    temporal_ir::v2::ScalarExpr::Slot(SlotId::new(0)),
                )],
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
                    temporal_ir::v2::ScalarExpr::Slot(SlotId::new(0)),
                )],
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
