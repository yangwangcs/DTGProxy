use std::collections::BTreeMap;

use dtg_language_ir::RowSchema;
use dtg_storage::ShardId;

use crate::{
    CancellationToken, ColumnBatch, DeterministicMergeOperator, ExecutablePlan, Operator,
    OverlayOperator, QueryBudget, QueryContext, QueryError, QueryFuture, QueryOverlay,
    QueryStorage, StorageSourceOperator,
};

pub struct QueryRuntime {
    batch_size: u32,
}

impl QueryRuntime {
    pub fn new(batch_size: u32) -> Self {
        Self { batch_size }
    }

    #[allow(clippy::unused_async)]
    pub async fn execute(
        &self,
        plan: &ExecutablePlan,
        mut storage: BTreeMap<ShardId, QueryStorage>,
        snapshot: &crate::SnapshotGuard,
        budget: QueryBudget,
        cancellation: CancellationToken,
        overlay: Option<QueryOverlay>,
    ) -> Result<QueryStream, QueryError> {
        if self.batch_size == 0 {
            return Err(QueryError::InvalidPlan(
                "query runtime batch size must be nonzero".into(),
            ));
        }
        let mut fragment_operators: Vec<Box<dyn Operator>> = Vec::new();
        for fragment in plan.fragments() {
            snapshot.validate(fragment.fence())?;
            let shard_id = fragment.fence().shard_id();
            let shard_storage = storage
                .remove(&shard_id)
                .ok_or(QueryError::MissingStorage(shard_id))?;
            let mut access_operators: Vec<Box<dyn Operator>> = fragment
                .accesses()
                .iter()
                .cloned()
                .map(|access| {
                    StorageSourceOperator::new(
                        access,
                        fragment.fence().clone(),
                        shard_storage.clone(),
                        self.batch_size,
                    )
                    .map(|operator| Box::new(operator) as Box<dyn Operator>)
                })
                .collect::<Result<_, _>>()?;
            let fragment_operator = if access_operators.len() == 1 {
                access_operators.pop().expect("length checked")
            } else {
                Box::new(DeterministicMergeOperator::new(access_operators, 0, false))
                    as Box<dyn Operator>
            };
            fragment_operators.push(fragment_operator);
        }
        let mut root: Box<dyn Operator> = if fragment_operators.len() == 1 {
            fragment_operators.pop().expect("length checked")
        } else {
            Box::new(DeterministicMergeOperator::new(fragment_operators, 0, true))
        };
        if let Some(overlay) = overlay {
            root = Box::new(OverlayOperator::new(
                root,
                overlay,
                plan.fragments()[0].fence().valid_at(),
            ));
        }
        Ok(QueryStream::from_operator(root, budget, cancellation))
    }
}

pub struct QueryStream {
    operator: Box<dyn Operator>,
    context: QueryContext,
    schema: RowSchema,
}

impl QueryStream {
    pub fn from_operator(
        operator: Box<dyn Operator>,
        budget: QueryBudget,
        cancellation: CancellationToken,
    ) -> Self {
        let schema = operator.schema().clone();
        Self {
            operator,
            context: QueryContext::new(budget, cancellation),
            schema,
        }
    }

    pub fn next_batch(&mut self) -> QueryFuture<'_, Option<ColumnBatch>> {
        Box::pin(async move {
            self.context.checkpoint()?;
            let batch = self.operator.next_batch(&mut self.context).await?;
            if let Some(batch) = &batch {
                self.context.charge_rows(batch.row_count() as u64)?;
                self.context.charge_memory(batch.estimated_bytes())?;
            }
            Ok(batch)
        })
    }

    pub fn collect(&mut self) -> QueryFuture<'_, ColumnBatch> {
        Box::pin(async move {
            let mut rows = Vec::new();
            while let Some(batch) = self.next_batch().await? {
                self.context.checkpoint()?;
                if batch.schema() != &self.schema {
                    return Err(QueryError::InvalidBatch(
                        "query stream schema changed between batches".into(),
                    ));
                }
                rows.extend(batch.rows());
            }
            ColumnBatch::from_rows(self.schema.clone(), rows)
        })
    }
}
