use temporal_ir::{
    DiffOperator, ExpandDirection, GraphScope, PlanBody, PlanError, PointOperator, TemporalPlan,
    TemporalSelector,
};
use temporal_query::{ParseError, parse};
use temporal_storage::{ElementId, ElementKind, GraphId, PartitionId};
use temporal_types::{TransactionTime, ValidTime};

fn scope() -> GraphScope {
    GraphScope::new(GraphId::new(2), PartitionId::new(3))
}

fn tx(physical: i64, logical: u32) -> TransactionTime {
    TransactionTime::new(physical, logical)
}

#[test]
fn point_lookup_and_expansion_syntax_compile_to_typed_ir() {
    assert_eq!(
        parse("VERTEX 7 GRAPH 2 PARTITION 3 FOR VALID TIME -5 CURRENT LIMIT 1").unwrap(),
        TemporalPlan::point(
            scope(),
            PointOperator::VertexById(ElementId::new(7)),
            ValidTime::from_micros(-5),
            TemporalSelector::Current,
            1,
        )
    );
    assert_eq!(
        parse(
            "  edge 9 graph 2 partition 3 for valid time 50 as of transaction time 100:4 limit 1  "
        )
        .unwrap(),
        TemporalPlan::point(
            scope(),
            PointOperator::EdgeById(ElementId::new(9)),
            ValidTime::from_micros(50),
            TemporalSelector::AsOf(tx(100, 4)),
            1,
        )
    );
    assert_eq!(
        parse(
            "EXPAND BOTH FROM 7 GRAPH 2 PARTITION 3 FOR VALID TIME 50 AS OF TRANSACTION TIME 100:4 LIMIT 32"
        )
        .unwrap(),
        TemporalPlan::point(
            scope(),
            PointOperator::Expand {
                origin: ElementId::new(7),
                direction: ExpandDirection::Both,
            },
            ValidTime::from_micros(50),
            TemporalSelector::AsOf(tx(100, 4)),
            32,
        )
    );
}

#[test]
fn element_diff_syntax_compiles_to_ordered_transaction_bounds() {
    assert_eq!(
        parse("VERTEX 7 GRAPH 2 PARTITION 3 DIFF TRANSACTION TIME 100:0 TO 200:3 LIMIT 8").unwrap(),
        TemporalPlan::diff(
            scope(),
            DiffOperator::Element {
                kind: ElementKind::Vertex,
                id: ElementId::new(7),
            },
            tx(100, 0),
            tx(200, 3),
            8,
        )
    );
}

#[test]
fn parser_reports_structured_syntax_integer_and_plan_errors() {
    assert_eq!(parse(""), Err(ParseError::Empty));
    assert_eq!(
        parse("MATCH (n) RETURN n"),
        Err(ParseError::UnsupportedStatement {
            token: "MATCH".to_owned(),
        })
    );
    assert_eq!(
        parse("VERTEX nope GRAPH 2 PARTITION 3 FOR VALID TIME 5 CURRENT LIMIT 1"),
        Err(ParseError::InvalidInteger {
            position: 1,
            field: "element id",
            value: "nope".to_owned(),
        })
    );
    assert_eq!(
        parse("VERTEX 7 GRAPH 2 PARTITION 3 FOR VALID TIME 5 CURRENT LIMIT 0"),
        Err(ParseError::InvalidPlan(PlanError::InvalidLimit {
            max: temporal_ir::MAX_RESULT_LIMIT,
            actual: 0,
        }))
    );
    assert_eq!(
        parse("VERTEX 7 GRAPH 2 PARTITION 3 FOR VALID 5 CURRENT LIMIT 1"),
        Err(ParseError::UnexpectedToken {
            position: 8,
            expected: "TIME",
            actual: Some("5".to_owned()),
        })
    );
    assert_eq!(
        parse("VERTEX 7 GRAPH 2 PARTITION 3 FOR VALID TIME 5 CURRENT LIMIT 1 EXTRA"),
        Err(ParseError::TrailingToken {
            position: 13,
            token: "EXTRA".to_owned(),
        })
    );
}

#[test]
fn parser_bounds_input_tokens_and_all_integer_widths() {
    let long = "x".repeat(4_097);
    assert_eq!(
        parse(&long),
        Err(ParseError::QueryTooLong {
            max: 4_096,
            actual: 4_097,
        })
    );
    let tokens = std::iter::repeat_n("x", 65).collect::<Vec<_>>().join(" ");
    assert_eq!(
        parse(&tokens),
        Err(ParseError::TooManyTokens {
            max: 64,
            actual: 65,
        })
    );
    assert!(matches!(
        parse(
            "VERTEX 340282366920938463463374607431768211456 GRAPH 2 PARTITION 3 FOR VALID TIME 5 CURRENT LIMIT 1"
        ),
        Err(ParseError::InvalidInteger {
            field: "element id",
            ..
        })
    ));
    assert!(matches!(
        parse("VERTEX 7 GRAPH 18446744073709551616 PARTITION 3 FOR VALID TIME 5 CURRENT LIMIT 1"),
        Err(ParseError::InvalidInteger {
            field: "graph id",
            ..
        })
    ));
    assert!(matches!(
        parse("VERTEX 7 GRAPH 2 PARTITION 4294967296 FOR VALID TIME 5 CURRENT LIMIT 1"),
        Err(ParseError::InvalidInteger {
            field: "partition id",
            ..
        })
    ));
    assert!(matches!(
        parse("VERTEX 7 GRAPH 2 PARTITION 3 FOR VALID TIME 9223372036854775808 CURRENT LIMIT 1"),
        Err(ParseError::InvalidInteger {
            field: "valid time",
            ..
        })
    ));
    assert!(matches!(
        parse(
            "VERTEX 7 GRAPH 2 PARTITION 3 FOR VALID TIME 5 AS OF TRANSACTION TIME 100:4294967296 LIMIT 1"
        ),
        Err(ParseError::InvalidInteger {
            field: "transaction time",
            ..
        })
    ));
}

#[test]
fn reversed_diff_and_missing_transaction_components_fail_closed() {
    assert_eq!(
        parse("EDGE 9 GRAPH 2 PARTITION 3 DIFF TRANSACTION TIME 200:0 TO 100:0 LIMIT 8"),
        Err(ParseError::InvalidPlan(PlanError::InvalidDiffOrder))
    );
    assert!(matches!(
        parse("EDGE 9 GRAPH 2 PARTITION 3 DIFF TRANSACTION TIME 200 TO 300:0 LIMIT 8"),
        Err(ParseError::InvalidInteger {
            field: "transaction time",
            ..
        })
    ));
}

#[test]
fn plan_bodies_from_the_parser_carry_no_backend_specific_state() {
    let plan = parse("EDGE 9 GRAPH 2 PARTITION 3 FOR VALID TIME 50 CURRENT LIMIT 1").unwrap();
    assert!(matches!(
        plan.body(),
        PlanBody::Point {
            operator: PointOperator::EdgeById(_),
            ..
        }
    ));
}
