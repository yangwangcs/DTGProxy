use std::cmp::Ordering;

use temporal_ir::v2::{RowSchema, ScalarExpr};

use super::{ExecutionContext, RuntimeError, RuntimeValue};

pub(crate) fn evaluate(
    expression: &ScalarExpr,
    schema: &RowSchema,
    row: &[RuntimeValue],
    context: &ExecutionContext,
) -> Result<RuntimeValue, RuntimeError> {
    match expression {
        ScalarExpr::Slot(slot) => schema
            .columns()
            .iter()
            .position(|column| column.slot() == *slot)
            .and_then(|index| row.get(index))
            .cloned()
            .ok_or(RuntimeError::MissingSlot(*slot)),
        ScalarExpr::Parameter(name) => context.parameter(name).cloned(),
        ScalarExpr::Literal(value) => Ok(value.clone().into()),
        ScalarExpr::Property { value, property_id } => {
            let value = evaluate(value, schema, row, context)?;
            property(&value, *property_id)
        }
        ScalarExpr::Not(value) => match evaluate(value, schema, row, context)? {
            RuntimeValue::Boolean(value) => Ok(RuntimeValue::Boolean(!value)),
            RuntimeValue::Null => Ok(RuntimeValue::Null),
            value => Err(type_mismatch("BOOLEAN", value.kind())),
        },
        ScalarExpr::Negate(value) => negate(evaluate(value, schema, row, context)?),
        ScalarExpr::Equal(left, right) => equality(left, right, schema, row, context, false),
        ScalarExpr::NotEqual(left, right) => equality(left, right, schema, row, context, true),
        ScalarExpr::Less(left, right) => comparison(left, right, schema, row, context, |value| {
            value == Ordering::Less
        }),
        ScalarExpr::LessEqual(left, right) => {
            comparison(left, right, schema, row, context, |value| {
                value != Ordering::Greater
            })
        }
        ScalarExpr::Greater(left, right) => {
            comparison(left, right, schema, row, context, |value| {
                value == Ordering::Greater
            })
        }
        ScalarExpr::GreaterEqual(left, right) => {
            comparison(left, right, schema, row, context, |value| {
                value != Ordering::Less
            })
        }
        ScalarExpr::And(left, right) => boolean_binary(left, right, schema, row, context, true),
        ScalarExpr::Or(left, right) => boolean_binary(left, right, schema, row, context, false),
        ScalarExpr::Add(left, right) => {
            arithmetic(left, right, schema, row, context, Arithmetic::Add)
        }
        ScalarExpr::Subtract(left, right) => {
            arithmetic(left, right, schema, row, context, Arithmetic::Subtract)
        }
        ScalarExpr::Multiply(left, right) => {
            arithmetic(left, right, schema, row, context, Arithmetic::Multiply)
        }
        ScalarExpr::Divide(left, right) => {
            arithmetic(left, right, schema, row, context, Arithmetic::Divide)
        }
        ScalarExpr::Function { function_id, .. } => {
            Err(RuntimeError::FunctionUnsupported(*function_id))
        }
    }
}

fn property(value: &RuntimeValue, property_id: u32) -> Result<RuntimeValue, RuntimeError> {
    let property = match value {
        RuntimeValue::Null => return Ok(RuntimeValue::Null),
        RuntimeValue::Node(node) => node.payload().property(property_id),
        RuntimeValue::Relationship(relationship) => relationship.payload().property(property_id),
        RuntimeValue::Map(map) => {
            return Ok(map.get(&property_id).cloned().unwrap_or(RuntimeValue::Null));
        }
        value => return Err(type_mismatch("NODE, RELATIONSHIP, or MAP", value.kind())),
    };
    Ok(property
        .cloned()
        .map(Into::into)
        .unwrap_or(RuntimeValue::Null))
}

fn negate(value: RuntimeValue) -> Result<RuntimeValue, RuntimeError> {
    match value {
        RuntimeValue::Null => Ok(RuntimeValue::Null),
        RuntimeValue::Integer(value) => value
            .checked_neg()
            .map(RuntimeValue::Integer)
            .ok_or(RuntimeError::ArithmeticOverflow),
        RuntimeValue::FloatBits(value) => {
            Ok(RuntimeValue::FloatBits((-f64::from_bits(value)).to_bits()))
        }
        value => Err(type_mismatch("NUMBER", value.kind())),
    }
}

fn comparison(
    left: &ScalarExpr,
    right: &ScalarExpr,
    schema: &RowSchema,
    row: &[RuntimeValue],
    context: &ExecutionContext,
    predicate: impl FnOnce(Ordering) -> bool,
) -> Result<RuntimeValue, RuntimeError> {
    let left = evaluate(left, schema, row, context)?;
    let right = evaluate(right, schema, row, context)?;
    if matches!(left, RuntimeValue::Null) || matches!(right, RuntimeValue::Null) {
        return Ok(RuntimeValue::Null);
    }
    let ordering = compare(&left, &right)?;
    Ok(RuntimeValue::Boolean(predicate(ordering)))
}

fn equality(
    left: &ScalarExpr,
    right: &ScalarExpr,
    schema: &RowSchema,
    row: &[RuntimeValue],
    context: &ExecutionContext,
    negate: bool,
) -> Result<RuntimeValue, RuntimeError> {
    let left = evaluate(left, schema, row, context)?;
    let right = evaluate(right, schema, row, context)?;
    if matches!(left, RuntimeValue::Null) || matches!(right, RuntimeValue::Null) {
        return Ok(RuntimeValue::Null);
    }
    let equal = match (&left, &right) {
        (RuntimeValue::Integer(left), RuntimeValue::FloatBits(right)) => {
            (*left as f64) == f64::from_bits(*right)
        }
        (RuntimeValue::FloatBits(left), RuntimeValue::Integer(right)) => {
            f64::from_bits(*left) == (*right as f64)
        }
        (RuntimeValue::FloatBits(left), RuntimeValue::FloatBits(right)) => {
            f64::from_bits(*left) == f64::from_bits(*right)
        }
        _ => left == right,
    };
    Ok(RuntimeValue::Boolean(equal != negate))
}

fn compare(left: &RuntimeValue, right: &RuntimeValue) -> Result<Ordering, RuntimeError> {
    match (left, right) {
        (RuntimeValue::Integer(left), RuntimeValue::Integer(right)) => Ok(left.cmp(right)),
        (RuntimeValue::FloatBits(left), RuntimeValue::FloatBits(right)) => f64::from_bits(*left)
            .partial_cmp(&f64::from_bits(*right))
            .ok_or_else(|| type_mismatch("ORDERED NUMBER", "NaN")),
        (RuntimeValue::Integer(left), RuntimeValue::FloatBits(right)) => (*left as f64)
            .partial_cmp(&f64::from_bits(*right))
            .ok_or_else(|| type_mismatch("ORDERED NUMBER", "NaN")),
        (RuntimeValue::FloatBits(left), RuntimeValue::Integer(right)) => f64::from_bits(*left)
            .partial_cmp(&(*right as f64))
            .ok_or_else(|| type_mismatch("ORDERED NUMBER", "NaN")),
        (RuntimeValue::String(left), RuntimeValue::String(right)) => Ok(left.cmp(right)),
        (RuntimeValue::Boolean(left), RuntimeValue::Boolean(right)) => Ok(left.cmp(right)),
        (RuntimeValue::TimestampMicros(left), RuntimeValue::TimestampMicros(right)) => {
            Ok(left.cmp(right))
        }
        (left, right) if left.kind() == right.kind() => Ok(if left == right {
            Ordering::Equal
        } else {
            return Err(type_mismatch("COMPARABLE VALUE", left.kind()));
        }),
        (left, right) => Err(type_mismatch(left.kind(), right.kind())),
    }
}

fn boolean_binary(
    left: &ScalarExpr,
    right: &ScalarExpr,
    schema: &RowSchema,
    row: &[RuntimeValue],
    context: &ExecutionContext,
    and: bool,
) -> Result<RuntimeValue, RuntimeError> {
    let left = truth(evaluate(left, schema, row, context)?)?;
    let right = truth(evaluate(right, schema, row, context)?)?;
    let result = if and {
        match (left, right) {
            (Some(false), _) | (_, Some(false)) => Some(false),
            (Some(true), Some(true)) => Some(true),
            _ => None,
        }
    } else {
        match (left, right) {
            (Some(true), _) | (_, Some(true)) => Some(true),
            (Some(false), Some(false)) => Some(false),
            _ => None,
        }
    };
    Ok(result
        .map(RuntimeValue::Boolean)
        .unwrap_or(RuntimeValue::Null))
}

fn truth(value: RuntimeValue) -> Result<Option<bool>, RuntimeError> {
    match value {
        RuntimeValue::Boolean(value) => Ok(Some(value)),
        RuntimeValue::Null => Ok(None),
        value => Err(type_mismatch("BOOLEAN", value.kind())),
    }
}

#[derive(Clone, Copy)]
enum Arithmetic {
    Add,
    Subtract,
    Multiply,
    Divide,
}

fn arithmetic(
    left: &ScalarExpr,
    right: &ScalarExpr,
    schema: &RowSchema,
    row: &[RuntimeValue],
    context: &ExecutionContext,
    operation: Arithmetic,
) -> Result<RuntimeValue, RuntimeError> {
    let left = evaluate(left, schema, row, context)?;
    let right = evaluate(right, schema, row, context)?;
    if matches!(left, RuntimeValue::Null) || matches!(right, RuntimeValue::Null) {
        return Ok(RuntimeValue::Null);
    }
    match (left, right) {
        (RuntimeValue::Integer(left), RuntimeValue::Integer(right)) => {
            let value = match operation {
                Arithmetic::Add => left.checked_add(right),
                Arithmetic::Subtract => left.checked_sub(right),
                Arithmetic::Multiply => left.checked_mul(right),
                Arithmetic::Divide if right == 0 => return Err(RuntimeError::DivisionByZero),
                Arithmetic::Divide => left.checked_div(right),
            };
            value
                .map(RuntimeValue::Integer)
                .ok_or(RuntimeError::ArithmeticOverflow)
        }
        (left, right) => {
            let left = number(left)?;
            let right = number(right)?;
            if matches!(operation, Arithmetic::Divide) && right == 0.0 {
                return Err(RuntimeError::DivisionByZero);
            }
            let value = match operation {
                Arithmetic::Add => left + right,
                Arithmetic::Subtract => left - right,
                Arithmetic::Multiply => left * right,
                Arithmetic::Divide => left / right,
            };
            Ok(RuntimeValue::FloatBits(value.to_bits()))
        }
    }
}

fn number(value: RuntimeValue) -> Result<f64, RuntimeError> {
    match value {
        RuntimeValue::Integer(value) => Ok(value as f64),
        RuntimeValue::FloatBits(value) => Ok(f64::from_bits(value)),
        value => Err(type_mismatch("NUMBER", value.kind())),
    }
}

fn type_mismatch(expected: &'static str, actual: &'static str) -> RuntimeError {
    use temporal_ir::v2::ValueType;

    let expected = match expected {
        "BOOLEAN" => ValueType::Boolean,
        "NUMBER" | "ORDERED NUMBER" => ValueType::Any,
        _ => ValueType::Any,
    };
    RuntimeError::TypeMismatch { expected, actual }
}
