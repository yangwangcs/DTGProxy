use dtg_language_ir::RowSchema;

use crate::operator::{collect_rows, compare_rows};
use crate::{ColumnBatch, Operator, QueryContext, QueryFuture, QueryValue};

pub struct ExchangeOperator {
    input: Box<dyn Operator>,
    schema: RowSchema,
}

impl ExchangeOperator {
    pub fn new(input: Box<dyn Operator>) -> Self {
        let schema = input.schema().clone();
        Self { input, schema }
    }
}

impl Operator for ExchangeOperator {
    fn schema(&self) -> &RowSchema {
        &self.schema
    }

    fn next_batch<'a>(
        &'a mut self,
        context: &'a mut QueryContext,
    ) -> QueryFuture<'a, Option<ColumnBatch>> {
        Box::pin(async move {
            context.checkpoint()?;
            let batch = self.input.next_batch(context).await?;
            if let Some(batch) = &batch {
                context.charge_network(batch.estimated_bytes())?;
            }
            Ok(batch)
        })
    }
}

pub struct DeterministicMergeOperator {
    inputs: Vec<Box<dyn Operator>>,
    key_column: usize,
    deduplicate: bool,
    schema: RowSchema,
    emitted: bool,
}

impl DeterministicMergeOperator {
    pub fn new(inputs: Vec<Box<dyn Operator>>, key_column: usize, deduplicate: bool) -> Self {
        let schema = inputs
            .first()
            .map_or_else(RowSchema::empty, |input| input.schema().clone());
        Self {
            inputs,
            key_column,
            deduplicate,
            schema,
            emitted: false,
        }
    }
}

impl Operator for DeterministicMergeOperator {
    fn schema(&self) -> &RowSchema {
        &self.schema
    }

    fn next_batch<'a>(
        &'a mut self,
        context: &'a mut QueryContext,
    ) -> QueryFuture<'a, Option<ColumnBatch>> {
        Box::pin(async move {
            context.checkpoint()?;
            if self.emitted {
                return Ok(None);
            }
            self.emitted = true;
            let mut rows = Vec::new();
            for input in &mut self.inputs {
                context.checkpoint()?;
                if input.schema() != &self.schema {
                    return Err(crate::QueryError::InvalidPlan(
                        "merge inputs have different schemas".into(),
                    ));
                }
                let (_, input_rows) = collect_rows(input, context).await?;
                rows.extend(input_rows);
            }
            context.charge_memory(rows.iter().flatten().map(QueryValue::estimated_bytes).sum())?;
            rows.sort_by(|left, right| {
                match (left.get(self.key_column), right.get(self.key_column)) {
                    (Some(left), Some(right)) => left.total_cmp(right),
                    (Some(_), None) => std::cmp::Ordering::Greater,
                    (None, Some(_)) => std::cmp::Ordering::Less,
                    (None, None) => std::cmp::Ordering::Equal,
                }
                .then_with(|| compare_rows(left, right))
            });
            if self.deduplicate {
                rows.dedup();
            }
            ColumnBatch::from_rows(self.schema.clone(), rows).map(Some)
        })
    }
}
