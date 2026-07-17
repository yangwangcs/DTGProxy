use temporal_ir::{
    DiffOperator, ExpandDirection, GraphScope, MAX_RESULT_LIMIT, PLAN_VERSION, PlanBody, PlanError,
    PointOperator, TemporalPlan, TemporalSelector,
};
use temporal_storage::{ElementId, ElementKind, GraphId, PartitionId};
use temporal_types::{TransactionTime, ValidTime};

fn scope() -> GraphScope {
    GraphScope::new(GraphId::new(7), PartitionId::new(3))
}

fn tx(value: i64) -> TransactionTime {
    TransactionTime::new(value, 0)
}

#[test]
fn typed_point_plans_require_scope_valid_time_transaction_selector_and_limit() {
    let operators = [
        PointOperator::VertexById(ElementId::new(1)),
        PointOperator::EdgeById(ElementId::new(2)),
        PointOperator::Expand {
            origin: ElementId::new(3),
            direction: ExpandDirection::Out,
        },
        PointOperator::Expand {
            origin: ElementId::new(3),
            direction: ExpandDirection::In,
        },
        PointOperator::Expand {
            origin: ElementId::new(3),
            direction: ExpandDirection::Both,
        },
    ];

    for operator in operators {
        let current = TemporalPlan::point(
            scope(),
            operator,
            ValidTime::from_micros(50),
            TemporalSelector::Current,
            32,
        );
        current.validate().unwrap();
        assert_eq!(current.version(), PLAN_VERSION);
        assert_eq!(current.scope(), scope());

        let as_of = TemporalPlan::point(
            scope(),
            operator,
            ValidTime::from_micros(50),
            TemporalSelector::AsOf(tx(100)),
            32,
        );
        as_of.validate().unwrap();
    }
}

#[test]
fn diff_plan_is_typed_by_element_kind_and_ordered_transaction_bounds() {
    for kind in [ElementKind::Vertex, ElementKind::Edge] {
        let plan = TemporalPlan::diff(
            scope(),
            DiffOperator::Element {
                kind,
                id: ElementId::new(9),
            },
            tx(100),
            tx(200),
            8,
        );
        plan.validate().unwrap();
        assert!(matches!(plan.body(), PlanBody::Diff { .. }));
    }

    let reversed = TemporalPlan::diff(
        scope(),
        DiffOperator::Element {
            kind: ElementKind::Vertex,
            id: ElementId::new(9),
        },
        tx(200),
        tx(100),
        8,
    );
    assert_eq!(reversed.validate(), Err(PlanError::InvalidDiffOrder));
}

#[test]
fn validation_rejects_unknown_versions_and_unbounded_result_requests() {
    let body = PlanBody::Point {
        operator: PointOperator::VertexById(ElementId::new(1)),
        valid_time: ValidTime::from_micros(50),
        transaction: TemporalSelector::Current,
        limit: 1,
    };
    let unknown = TemporalPlan::with_version(PLAN_VERSION + 1, scope(), body.clone());
    assert_eq!(
        unknown.validate(),
        Err(PlanError::UnsupportedVersion {
            expected: PLAN_VERSION,
            actual: PLAN_VERSION + 1,
        })
    );

    for limit in [0, MAX_RESULT_LIMIT + 1] {
        let invalid = TemporalPlan::with_version(
            PLAN_VERSION,
            scope(),
            PlanBody::Point {
                operator: PointOperator::VertexById(ElementId::new(1)),
                valid_time: ValidTime::from_micros(50),
                transaction: TemporalSelector::Current,
                limit,
            },
        );
        assert_eq!(
            invalid.validate(),
            Err(PlanError::InvalidLimit {
                max: MAX_RESULT_LIMIT,
                actual: limit,
            })
        );
    }
}
