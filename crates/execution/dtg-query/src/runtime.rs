use std::collections::BTreeMap;

use dtg_language_ir::RowSchema;
use dtg_storage::ShardId;

use crate::{
    CancellationToken, ColumnBatch, DeterministicMergeOperator, ExecutableOperatorKind,
    ExecutablePlan, Expression, LimitOperator, Operator, OverlayOperator, ProjectOperator,
    ProjectionExpr, QueryBudget, QueryContext, QueryError, QueryFuture, QueryOverlay, QueryStorage,
    SpillConfig, SpillMergeOperator, StorageSourceOperator,
};

pub struct QueryRuntime {
    batch_size: u32,
    spill: Option<SpillConfig>,
}

impl QueryRuntime {
    pub fn new(batch_size: u32) -> Self {
        Self {
            batch_size,
            spill: None,
        }
    }

    pub fn with_spill(mut self, spill: SpillConfig) -> Self {
        self.spill = Some(spill);
        self
    }

    #[allow(clippy::unused_async)]
    pub async fn execute(
        &self,
        plan: &ExecutablePlan,
        storage: BTreeMap<ShardId, QueryStorage>,
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
        let mut fragment_storage = BTreeMap::new();
        for fragment in plan.fragments() {
            snapshot.validate(fragment.fence())?;
            let shard_id = fragment.fence().shard_id();
            let shard_storage = storage
                .get(&shard_id)
                .ok_or(QueryError::MissingStorage(shard_id))?;
            fragment_storage.insert(fragment.id(), (fragment, shard_storage.clone()));
        }
        let mut operators = BTreeMap::<u32, Box<dyn Operator>>::new();
        for operator in plan.operators() {
            let executable: Box<dyn Operator> = match operator.kind() {
                ExecutableOperatorKind::Source {
                    logical_node,
                    fragments,
                    output,
                } => {
                    let mut sources = Vec::with_capacity(fragments.len());
                    for fragment_id in fragments {
                        let (fragment, shard_storage) =
                            fragment_storage.get(fragment_id).ok_or_else(|| {
                                QueryError::InvalidPlan(format!(
                                    "physical source references missing fragment {fragment_id}"
                                ))
                            })?;
                        let access = fragment.access(*logical_node).cloned().ok_or_else(|| {
                            QueryError::InvalidPlan(format!(
                                "fragment {fragment_id} is missing logical source {logical_node}"
                            ))
                        })?;
                        let source = StorageSourceOperator::new(
                            access,
                            fragment.fence().clone(),
                            shard_storage.clone(),
                            self.batch_size,
                        )?;
                        let storage_field = source
                            .schema()
                            .fields
                            .first()
                            .ok_or_else(|| {
                                QueryError::InvalidPlan(
                                    "storage source schema is unexpectedly empty".into(),
                                )
                            })?
                            .name
                            .clone();
                        sources.push(Box::new(ProjectOperator::new(
                            Box::new(source),
                            vec![ProjectionExpr::new(
                                output.clone(),
                                dtg_language_ir::LogicalType::Any,
                                false,
                                Expression::new(dtg_language_ir::LogicalExpr::Column(
                                    storage_field,
                                )),
                            )],
                        )) as Box<dyn Operator>);
                    }
                    if sources.len() == 1 {
                        sources.pop().expect("length checked")
                    } else if let Some(spill) = &self.spill {
                        Box::new(SpillMergeOperator::new(
                            sources,
                            0,
                            true,
                            self.batch_size as usize,
                            spill.clone(),
                        )?)
                    } else {
                        Box::new(DeterministicMergeOperator::new(sources, 0, true))
                    }
                }
                ExecutableOperatorKind::Limit { input, skip, limit } => {
                    let input = take_operator(&mut operators, *input)?;
                    let skip = usize::try_from(*skip)
                        .map_err(|_| QueryError::InvalidPlan("LIMIT skip exceeds usize".into()))?;
                    let limit = match limit {
                        Some(limit) => usize::try_from(*limit).map_err(|_| {
                            QueryError::InvalidPlan("LIMIT count exceeds usize".into())
                        })?,
                        None => usize::MAX,
                    };
                    Box::new(LimitOperator::new(input, skip, limit))
                }
                _ => {
                    return Err(QueryError::Unsupported(
                        "physical operator is not executable by this runtime".into(),
                    ));
                }
            };
            if operators.insert(operator.id(), executable).is_some() {
                return Err(QueryError::InvalidPlan(
                    "physical operator identity is duplicated".into(),
                ));
            }
        }
        let mut root = take_operator(&mut operators, plan.root_operator())?;
        if !operators.is_empty() {
            return Err(QueryError::InvalidPlan(
                "physical operator DAG contains unreachable operators".into(),
            ));
        }
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

fn take_operator(
    operators: &mut BTreeMap<u32, Box<dyn Operator>>,
    id: u32,
) -> Result<Box<dyn Operator>, QueryError> {
    operators
        .remove(&id)
        .ok_or_else(|| QueryError::InvalidPlan(format!("physical operator input {id} is absent")))
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
