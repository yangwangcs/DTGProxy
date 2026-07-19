use cypher_ast::{BinaryOperator, Expression, Identifier, UnaryOperator};
use cypher_syntax::parse_expression;

#[test]
fn respects_boolean_comparison_and_arithmetic_precedence() {
    let expression = parse_expression("n.score + 2 * $weight >= 10 AND NOT n.active = false")
        .expect("expression should parse");

    assert_eq!(
        expression,
        Expression::binary(
            BinaryOperator::And,
            Expression::binary(
                BinaryOperator::GreaterEqual,
                Expression::binary(
                    BinaryOperator::Add,
                    Expression::property(Expression::identifier("n"), "score"),
                    Expression::binary(
                        BinaryOperator::Multiply,
                        Expression::Integer("2".into()),
                        Expression::Parameter("weight".into()),
                    ),
                ),
                Expression::Integer("10".into()),
            ),
            Expression::unary(
                UnaryOperator::Not,
                Expression::binary(
                    BinaryOperator::Equal,
                    Expression::property(Expression::identifier("n"), "active"),
                    Expression::Boolean(false),
                ),
            ),
        )
    );
}

#[test]
fn parses_qualified_calls_lists_maps_and_indexing() {
    let expression =
        parse_expression("coalesce(n.values[0], {fallback: [1, 2, $default]}.fallback)")
            .expect("expression should parse");

    let Expression::FunctionCall { name, arguments } = expression else {
        panic!("expected function call");
    };
    assert_eq!(name, vec![Identifier::new("coalesce", false)]);
    assert_eq!(arguments.len(), 2);
    assert!(matches!(arguments[0], Expression::Index { .. }));
    assert!(matches!(arguments[1], Expression::Property { .. }));
}

#[test]
fn rejects_trailing_tokens_in_expression() {
    let error = parse_expression("n n").expect_err("trailing token must fail");

    assert_eq!(error.code(), "DTG-CYPHER-UNEXPECTED-TOKEN");
}
