use dtg_language_ir::{BinaryOperator, LogicalExpr, UnaryOperator};

use crate::{QueryError, QueryValue};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Expression {
    logical: LogicalExpr,
}

impl Expression {
    pub const fn new(logical: LogicalExpr) -> Self {
        Self { logical }
    }

    pub const fn logical(&self) -> &LogicalExpr {
        &self.logical
    }

    pub fn evaluate(
        &self,
        schema: &dtg_language_ir::RowSchema,
        row: &[QueryValue],
    ) -> Result<QueryValue, QueryError> {
        evaluate(&self.logical, schema, row)
    }
}

fn evaluate(
    expression: &LogicalExpr,
    schema: &dtg_language_ir::RowSchema,
    row: &[QueryValue],
) -> Result<QueryValue, QueryError> {
    match expression {
        LogicalExpr::Literal(value) => Ok(QueryValue::from_kernel(value.clone())),
        LogicalExpr::Parameter(name) => Err(QueryError::Unsupported(format!(
            "unbound parameter in executable expression: {name}"
        ))),
        LogicalExpr::Column(name) => schema
            .fields
            .iter()
            .position(|field| field.name == *name)
            .and_then(|index| row.get(index))
            .cloned()
            .ok_or_else(|| QueryError::InvalidPlan(format!("unknown execution column: {name}"))),
        LogicalExpr::Property { input, name } => {
            let input = evaluate(input, schema, row)?;
            let property = match input {
                QueryValue::Vertex(vertex) => vertex.properties().get(name).cloned(),
                QueryValue::Relationship(edge) => edge.properties().get(name).cloned(),
                QueryValue::Map(values) => {
                    return Ok(values.get(name).cloned().unwrap_or(QueryValue::Null));
                }
                QueryValue::Null => return Ok(QueryValue::Null),
                _ => None,
            };
            Ok(property.map_or(QueryValue::Null, QueryValue::from_kernel))
        }
        LogicalExpr::Unary { operator, input } => {
            let input = evaluate(input, schema, row)?;
            evaluate_unary(*operator, input)
        }
        LogicalExpr::Binary {
            left,
            operator,
            right,
        } => {
            let left = evaluate(left, schema, row)?;
            let right = evaluate(right, schema, row)?;
            evaluate_binary(left, *operator, right)
        }
        LogicalExpr::List(values) => values
            .iter()
            .map(|value| evaluate(value, schema, row))
            .collect::<Result<Vec<_>, _>>()
            .map(QueryValue::List),
        LogicalExpr::Map(values) => values
            .iter()
            .map(|(name, value)| Ok((name.clone(), evaluate(value, schema, row)?)))
            .collect::<Result<_, QueryError>>()
            .map(QueryValue::Map),
    }
}

fn evaluate_unary(operator: UnaryOperator, input: QueryValue) -> Result<QueryValue, QueryError> {
    match (operator, input) {
        (UnaryOperator::IsNull, QueryValue::Null) => Ok(QueryValue::Boolean(true)),
        (UnaryOperator::IsNull, _) => Ok(QueryValue::Boolean(false)),
        (_, QueryValue::Null) => Ok(QueryValue::Null),
        (UnaryOperator::Not, QueryValue::Boolean(value)) => Ok(QueryValue::Boolean(!value)),
        (UnaryOperator::Negate, QueryValue::Integer(value)) => value
            .checked_neg()
            .map(QueryValue::Integer)
            .ok_or_else(|| QueryError::InvalidPlan("integer negation overflow".into())),
        _ => Err(QueryError::InvalidPlan(
            "unary expression received an incompatible value".into(),
        )),
    }
}

fn evaluate_binary(
    left: QueryValue,
    operator: BinaryOperator,
    right: QueryValue,
) -> Result<QueryValue, QueryError> {
    if matches!(left, QueryValue::Null) || matches!(right, QueryValue::Null) {
        return match operator {
            BinaryOperator::And => and_null(left, right),
            BinaryOperator::Or => or_null(left, right),
            _ => Ok(QueryValue::Null),
        };
    }
    match operator {
        BinaryOperator::Add
        | BinaryOperator::Subtract
        | BinaryOperator::Multiply
        | BinaryOperator::Divide => arithmetic(left, operator, right),
        BinaryOperator::Equal => Ok(QueryValue::Boolean(left == right)),
        BinaryOperator::NotEqual => Ok(QueryValue::Boolean(left != right)),
        BinaryOperator::LessThan => Ok(QueryValue::Boolean(left.total_cmp(&right).is_lt())),
        BinaryOperator::LessThanOrEqual => Ok(QueryValue::Boolean(!left.total_cmp(&right).is_gt())),
        BinaryOperator::GreaterThan => Ok(QueryValue::Boolean(left.total_cmp(&right).is_gt())),
        BinaryOperator::GreaterThanOrEqual => {
            Ok(QueryValue::Boolean(!left.total_cmp(&right).is_lt()))
        }
        BinaryOperator::And => boolean(left, right, |left, right| left && right),
        BinaryOperator::Or => boolean(left, right, |left, right| left || right),
        BinaryOperator::Contains => contains(left, right),
    }
}

fn arithmetic(
    left: QueryValue,
    operator: BinaryOperator,
    right: QueryValue,
) -> Result<QueryValue, QueryError> {
    let (QueryValue::Integer(left), QueryValue::Integer(right)) = (left, right) else {
        return Err(QueryError::InvalidPlan(
            "arithmetic requires integer operands".into(),
        ));
    };
    let value = match operator {
        BinaryOperator::Add => left.checked_add(right),
        BinaryOperator::Subtract => left.checked_sub(right),
        BinaryOperator::Multiply => left.checked_mul(right),
        BinaryOperator::Divide => left.checked_div(right),
        _ => None,
    };
    value
        .map(QueryValue::Integer)
        .ok_or_else(|| QueryError::InvalidPlan("integer arithmetic failed".into()))
}

fn boolean(
    left: QueryValue,
    right: QueryValue,
    operation: impl FnOnce(bool, bool) -> bool,
) -> Result<QueryValue, QueryError> {
    let (QueryValue::Boolean(left), QueryValue::Boolean(right)) = (left, right) else {
        return Err(QueryError::InvalidPlan(
            "boolean expression requires boolean operands".into(),
        ));
    };
    Ok(QueryValue::Boolean(operation(left, right)))
}

fn and_null(left: QueryValue, right: QueryValue) -> Result<QueryValue, QueryError> {
    match (left, right) {
        (QueryValue::Boolean(false), _) | (_, QueryValue::Boolean(false)) => {
            Ok(QueryValue::Boolean(false))
        }
        (QueryValue::Null, QueryValue::Null | QueryValue::Boolean(true))
        | (QueryValue::Boolean(true), QueryValue::Null) => Ok(QueryValue::Null),
        _ => Err(QueryError::InvalidPlan(
            "AND received an incompatible value".into(),
        )),
    }
}

fn or_null(left: QueryValue, right: QueryValue) -> Result<QueryValue, QueryError> {
    match (left, right) {
        (QueryValue::Boolean(true), _) | (_, QueryValue::Boolean(true)) => {
            Ok(QueryValue::Boolean(true))
        }
        (QueryValue::Null, QueryValue::Null | QueryValue::Boolean(false))
        | (QueryValue::Boolean(false), QueryValue::Null) => Ok(QueryValue::Null),
        _ => Err(QueryError::InvalidPlan(
            "OR received an incompatible value".into(),
        )),
    }
}

fn contains(left: QueryValue, right: QueryValue) -> Result<QueryValue, QueryError> {
    match (left, right) {
        (QueryValue::String(left), QueryValue::String(right)) => {
            Ok(QueryValue::Boolean(left.contains(&right)))
        }
        (QueryValue::List(left), right) => Ok(QueryValue::Boolean(left.contains(&right))),
        _ => Err(QueryError::InvalidPlan(
            "CONTAINS received incompatible values".into(),
        )),
    }
}
