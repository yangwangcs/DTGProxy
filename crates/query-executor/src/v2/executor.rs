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
                PhysicalOperator::TemporalSlice | PhysicalOperator::Finish => batches,
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
                PhysicalOperator::Aggregate { .. } => {
                    return Err(RuntimeError::UnsupportedOperator("Aggregate"));
                }
                PhysicalOperator::Sort { .. } => {
                    return Err(RuntimeError::UnsupportedOperator("Sort"));
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
