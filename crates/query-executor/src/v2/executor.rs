use std::cmp::Ordering;

use physical_plan::{PhysicalOperator, PlanFragment};
use temporal_ir::v2::{RowSchema, ScalarExpr};

use super::expression;
use super::{ExecutionContext, RecordBatch, RuntimeError, RuntimeValue};

#[derive(Clone, Copy, Debug, Default)]
pub struct BatchExecutor;

impl BatchExecutor {
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    pub fn evaluate(
        &self,
        expression: &ScalarExpr,
        schema: &RowSchema,
        row: &[RuntimeValue],
        context: &ExecutionContext,
    ) -> Result<RuntimeValue, RuntimeError> {
        context.check_fences()?;
        expression::evaluate(expression, schema, row, context)
    }

    pub fn execute_fragment(
        &self,
        fragment: &PlanFragment,
        context: &ExecutionContext,
        batches: Vec<RecordBatch>,
    ) -> Result<Vec<RecordBatch>, RuntimeError> {
        self.execute_operators(
            fragment.operators(),
            fragment.output(),
            fragment.budget().memory_bytes(),
            context,
            batches,
        )
    }

    pub(crate) fn execute_operators(
        &self,
        operators: &[PhysicalOperator],
        output: &RowSchema,
        memory_limit: u64,
        context: &ExecutionContext,
        mut batches: Vec<RecordBatch>,
    ) -> Result<Vec<RecordBatch>, RuntimeError> {
        context.check_fences()?;
        ensure_memory(&batches, memory_limit)?;
        for operator in operators {
            context.check_fences()?;
            batches = match operator {
                PhysicalOperator::Argument => argument(batches)?,
                PhysicalOperator::Filter(predicate) => filter(batches, predicate, context)?,
                PhysicalOperator::Project { expressions } => {
                    project(batches, expressions, output, context)?
                }
                PhysicalOperator::Skip { count } => skip(batches, row_count(count, context)?)?,
                PhysicalOperator::Limit { count } => limit(batches, row_count(count, context)?)?,
                PhysicalOperator::Sort { keys } => sort(batches, keys)?,
                PhysicalOperator::Aggregate {
                    grouping,
                    aggregates,
                } => aggregate(batches, grouping, aggregates, output, context)?,
                PhysicalOperator::TemporalSlice { .. } | PhysicalOperator::Finish => batches,
                PhysicalOperator::NodeScan { .. } => {
                    return Err(RuntimeError::UnsupportedOperator("NodeScan"));
                }
                PhysicalOperator::RelationshipScan { .. } => {
                    return Err(RuntimeError::UnsupportedOperator("RelationshipScan"));
                }
                PhysicalOperator::Expand { .. } => {
                    return Err(RuntimeError::UnsupportedOperator("Expand"));
                }
                PhysicalOperator::HashJoin { .. } => {
                    return Err(RuntimeError::UnsupportedOperator("HashJoin"));
                }
                PhysicalOperator::Union { .. } => {
                    return Err(RuntimeError::UnsupportedOperator("Union"));
                }
                PhysicalOperator::Diff => {
                    return Err(RuntimeError::UnsupportedOperator("Diff"));
                }
                PhysicalOperator::Write { .. } => {
                    return Err(RuntimeError::UnsupportedOperator("Write"));
                }
                PhysicalOperator::Procedure { .. } => {
                    return Err(RuntimeError::UnsupportedOperator("Procedure"));
                }
            };
            ensure_memory(&batches, memory_limit)?;
        }
        if batches.iter().any(|batch| batch.schema() != output) {
            return Err(RuntimeError::OutputSchemaMismatch);
        }
        Ok(batches)
    }
}

fn sort(
    batches: Vec<RecordBatch>,
    keys: &[temporal_ir::v2::SlotId],
) -> Result<Vec<RecordBatch>, RuntimeError> {
    if batches.is_empty() {
        return Ok(batches);
    }
    let schema = batches[0].schema().clone();
    let key_indices = keys
        .iter()
        .map(|slot| {
            schema
                .columns()
                .iter()
                .position(|column| column.slot() == *slot)
                .ok_or(RuntimeError::MissingSlot(*slot))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut rows = batches
        .into_iter()
        .flat_map(RecordBatch::into_rows)
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        key_indices
            .iter()
            .map(|index| compare_values(&left[*index], &right[*index]))
            .find(|ordering| *ordering != Ordering::Equal)
            .unwrap_or(Ordering::Equal)
    });
    Ok(vec![RecordBatch::try_new(schema, rows)?])
}

fn aggregate(
    batches: Vec<RecordBatch>,
    grouping: &[temporal_ir::v2::SlotId],
    aggregates: &[(temporal_ir::v2::SlotId, ScalarExpr)],
    output: &RowSchema,
    context: &ExecutionContext,
) -> Result<Vec<RecordBatch>, RuntimeError> {
    if !grouping.is_empty() {
        return Err(RuntimeError::UnsupportedOperator("grouped Aggregate"));
    }
    let schema = batches
        .first()
        .map_or_else(RowSchema::empty, |batch| batch.schema().clone());
    let rows = batches
        .into_iter()
        .flat_map(RecordBatch::into_rows)
        .collect::<Vec<_>>();
    let mut values = Vec::with_capacity(aggregates.len());
    for (_, expression) in aggregates {
        values.push(aggregate_expression(expression, &schema, &rows, context)?);
    }
    let output_values = output
        .columns()
        .iter()
        .map(|column| {
            aggregates
                .iter()
                .position(|(slot, _)| *slot == column.slot())
                .map(|index| values[index].clone())
                .ok_or(RuntimeError::MissingSlot(column.slot()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(vec![RecordBatch::try_new(
        output.clone(),
        vec![output_values],
    )?])
}

fn aggregate_expression(
    expression: &ScalarExpr,
    schema: &RowSchema,
    rows: &[Vec<RuntimeValue>],
    context: &ExecutionContext,
) -> Result<RuntimeValue, RuntimeError> {
    let ScalarExpr::Function {
        function_id,
        arguments,
    } = expression
    else {
        return Err(RuntimeError::UnsupportedOperator("non-function Aggregate"));
    };
    if *function_id == function_id_for("count") {
        let mut count = 0_i64;
        for row in rows {
            if arguments.is_empty()
                || !matches!(
                    expression::evaluate(&arguments[0], schema, row, context)?,
                    RuntimeValue::Null
                )
            {
                count = count
                    .checked_add(1)
                    .ok_or(RuntimeError::ArithmeticOverflow)?;
            }
        }
        return Ok(RuntimeValue::Integer(count));
    }
    let mut values = rows
        .iter()
        .map(|row| {
            arguments
                .first()
                .ok_or(RuntimeError::FunctionUnsupported(*function_id))
                .and_then(|argument| expression::evaluate(argument, schema, row, context))
        })
        .filter_map(|value| match value {
            Ok(RuntimeValue::Null) => None,
            Ok(value) => Some(Ok(value)),
            Err(error) => Some(Err(error)),
        })
        .collect::<Result<Vec<_>, _>>()?;
    if values.is_empty() {
        return Ok(RuntimeValue::Null);
    }
    if *function_id == function_id_for("sum") || *function_id == function_id_for("avg") {
        let mut total = 0.0_f64;
        for value in &values {
            total += numeric_value(value)?;
        }
        if *function_id == function_id_for("avg") {
            total /= values.len() as f64;
        }
        return Ok(RuntimeValue::FloatBits(total.to_bits()));
    }
    if *function_id == function_id_for("min") || *function_id == function_id_for("max") {
        values.sort_by(compare_values);
        return Ok(if *function_id == function_id_for("min") {
            values.remove(0)
        } else {
            values.pop().expect("non-empty aggregate values")
        });
    }
    Err(RuntimeError::FunctionUnsupported(*function_id))
}

fn numeric_value(value: &RuntimeValue) -> Result<f64, RuntimeError> {
    match value {
        RuntimeValue::Integer(value) => Ok(*value as f64),
        RuntimeValue::FloatBits(value) => Ok(f64::from_bits(*value)),
        value => Err(RuntimeError::TypeMismatch {
            expected: temporal_ir::v2::ValueType::Float,
            actual: value.kind(),
        }),
    }
}

fn function_id_for(name: &str) -> u32 {
    let digest = blake3::hash(name.as_bytes());
    u32::from_be_bytes(
        digest.as_bytes()[..4]
            .try_into()
            .expect("digest has four bytes"),
    )
}

fn compare_values(left: &RuntimeValue, right: &RuntimeValue) -> Ordering {
    use RuntimeValue as Value;
    match (left, right) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Less,
        (_, Value::Null) => Ordering::Greater,
        (Value::Boolean(left), Value::Boolean(right)) => left.cmp(right),
        (Value::Integer(left), Value::Integer(right)) => left.cmp(right),
        (Value::FloatBits(left), Value::FloatBits(right)) => {
            f64::from_bits(*left).total_cmp(&f64::from_bits(*right))
        }
        (Value::String(left), Value::String(right)) => left.cmp(right),
        (Value::TimestampMicros(left), Value::TimestampMicros(right)) => left.cmp(right),
        (Value::Node(left), Value::Node(right)) => left.element().cmp(&right.element()),
        (Value::Relationship(left), Value::Relationship(right)) => {
            left.element().cmp(&right.element())
        }
        (left, right) => left.kind().cmp(right.kind()),
    }
}

fn argument(batches: Vec<RecordBatch>) -> Result<Vec<RecordBatch>, RuntimeError> {
    if batches.is_empty() {
        return Ok(vec![RecordBatch::try_new(
            RowSchema::empty(),
            vec![Vec::new()],
        )?]);
    }
    Ok(batches)
}

fn filter(
    batches: Vec<RecordBatch>,
    predicate: &ScalarExpr,
    context: &ExecutionContext,
) -> Result<Vec<RecordBatch>, RuntimeError> {
    batches
        .into_iter()
        .map(|batch| {
            let schema = batch.schema().clone();
            let mut rows = Vec::new();
            for row in batch.into_rows() {
                context.check_fences()?;
                match expression::evaluate(predicate, &schema, &row, context)? {
                    RuntimeValue::Boolean(true) => rows.push(row),
                    RuntimeValue::Boolean(false) | RuntimeValue::Null => {}
                    value => return Err(RuntimeError::InvalidPredicate(value.kind())),
                }
            }
            RecordBatch::try_new(schema, rows)
        })
        .collect()
}

fn project(
    batches: Vec<RecordBatch>,
    expressions: &[(temporal_ir::v2::SlotId, ScalarExpr)],
    output: &RowSchema,
    context: &ExecutionContext,
) -> Result<Vec<RecordBatch>, RuntimeError> {
    let ordered = output
        .columns()
        .iter()
        .map(|column| {
            expressions
                .iter()
                .find(|(slot, _)| *slot == column.slot())
                .map(|(_, expression)| expression)
                .ok_or(RuntimeError::MissingSlot(column.slot()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    batches
        .into_iter()
        .map(|batch| {
            let schema = batch.schema().clone();
            let rows = batch
                .into_rows()
                .into_iter()
                .map(|row| {
                    context.check_fences()?;
                    ordered
                        .iter()
                        .map(|expression| expression::evaluate(expression, &schema, &row, context))
                        .collect::<Result<Vec<_>, _>>()
                })
                .collect::<Result<Vec<_>, _>>()?;
            RecordBatch::try_new(output.clone(), rows)
        })
        .collect()
}

fn row_count(expression: &ScalarExpr, context: &ExecutionContext) -> Result<usize, RuntimeError> {
    match expression::evaluate(expression, &RowSchema::empty(), &[], context)? {
        RuntimeValue::Integer(value) if value >= 0 => {
            usize::try_from(value).map_err(|_| RuntimeError::InvalidRowCount)
        }
        _ => Err(RuntimeError::InvalidRowCount),
    }
}

fn skip(batches: Vec<RecordBatch>, mut count: usize) -> Result<Vec<RecordBatch>, RuntimeError> {
    let mut output = Vec::with_capacity(batches.len());
    for batch in batches {
        let schema = batch.schema().clone();
        let rows = batch.into_rows();
        let skipped = count.min(rows.len());
        count -= skipped;
        let rows = rows.into_iter().skip(skipped).collect();
        output.push(RecordBatch::try_new(schema, rows)?);
    }
    Ok(output)
}

fn limit(batches: Vec<RecordBatch>, mut count: usize) -> Result<Vec<RecordBatch>, RuntimeError> {
    let mut output = Vec::new();
    for batch in batches {
        if count == 0 {
            break;
        }
        let schema = batch.schema().clone();
        let rows = batch.into_rows();
        let taken = count.min(rows.len());
        count -= taken;
        output.push(RecordBatch::try_new(
            schema,
            rows.into_iter().take(taken).collect(),
        )?);
    }
    Ok(output)
}

fn ensure_memory(batches: &[RecordBatch], limit: u64) -> Result<(), RuntimeError> {
    let required = batches.iter().try_fold(0_u64, |total, batch| {
        total
            .checked_add(batch.estimated_bytes())
            .ok_or(RuntimeError::SizeOverflow)
    })?;
    if required > limit {
        return Err(RuntimeError::MemoryLimitExceeded { limit, required });
    }
    Ok(())
}
