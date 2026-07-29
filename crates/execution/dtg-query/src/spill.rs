use std::sync::Arc;

use dtg_language_ir::RowSchema;

use crate::operator::compare_rows;
use crate::{ColumnBatch, Operator, QueryContext, QueryError, QueryFuture, QueryValue};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub struct SpillHandle(u64);

impl SpillHandle {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

pub trait SpillStore: Send + Sync {
    fn write_run(
        &self,
        schema: &RowSchema,
        rows: Vec<Vec<QueryValue>>,
    ) -> Result<SpillHandle, QueryError>;
    fn row_count(&self, handle: SpillHandle) -> Result<usize, QueryError>;
    fn row_estimated_bytes(&self, handle: SpillHandle, index: usize) -> Result<u64, QueryError>;
    fn read_row(&self, handle: SpillHandle, index: usize) -> Result<Vec<QueryValue>, QueryError>;
    fn remove_run(&self, handle: SpillHandle) -> Result<(), QueryError>;
}

#[derive(Clone)]
pub struct SpillConfig {
    store: Arc<dyn SpillStore>,
    threshold_bytes: u64,
}

impl SpillConfig {
    pub fn new(store: Arc<dyn SpillStore>, threshold_bytes: u64) -> Result<Self, QueryError> {
        if threshold_bytes == 0 {
            return Err(QueryError::InvalidPlan(
                "spill threshold must be nonzero".into(),
            ));
        }
        Ok(Self {
            store,
            threshold_bytes,
        })
    }

    pub(crate) fn store(&self) -> &Arc<dyn SpillStore> {
        &self.store
    }

    pub(crate) const fn threshold_bytes(&self) -> u64 {
        self.threshold_bytes
    }
}

enum MergeRun {
    Memory {
        rows: Vec<Vec<QueryValue>>,
        index: usize,
    },
    Spilled {
        handle: SpillHandle,
        row_count: usize,
        index: usize,
        cached: Option<Vec<QueryValue>>,
    },
}

impl MergeRun {
    fn ensure_head(
        &mut self,
        context: &mut QueryContext,
        store: &dyn SpillStore,
    ) -> Result<(), QueryError> {
        let Self::Spilled {
            handle,
            row_count,
            index,
            cached,
        } = self
        else {
            return Ok(());
        };
        if cached.is_some() || *index >= *row_count {
            return Ok(());
        }
        let declared = store.row_estimated_bytes(*handle, *index)?;
        context.charge_memory(declared)?;
        let row = store.read_row(*handle, *index)?;
        let actual = estimate_row(&row);
        if actual > declared {
            context.charge_memory(actual - declared)?;
        } else {
            context.release_memory(declared - actual);
        }
        *cached = Some(row);
        Ok(())
    }

    fn head(&self) -> Option<&Vec<QueryValue>> {
        match self {
            Self::Memory { rows, index } => rows.get(*index),
            Self::Spilled {
                row_count,
                index,
                cached,
                ..
            } => (*index < *row_count).then_some(cached.as_ref()).flatten(),
        }
    }

    fn take_head(&mut self, store: &dyn SpillStore) -> Result<Option<Vec<QueryValue>>, QueryError> {
        match self {
            Self::Memory { rows, index } => {
                let Some(row) = rows.get_mut(*index) else {
                    return Ok(None);
                };
                *index += 1;
                Ok(Some(std::mem::take(row)))
            }
            Self::Spilled {
                handle,
                row_count,
                index,
                cached,
            } => {
                if *index >= *row_count {
                    return Ok(None);
                }
                let row = cached.take().ok_or_else(|| {
                    QueryError::Storage("spill store did not provide the requested row".into())
                })?;
                *index += 1;
                if *index == *row_count {
                    store.remove_run(*handle)?;
                }
                Ok(Some(row))
            }
        }
    }

    fn spilled_handle(&self) -> Option<SpillHandle> {
        match self {
            Self::Spilled { handle, .. } => Some(*handle),
            Self::Memory { .. } => None,
        }
    }
}

pub(crate) struct SpillMergeOperator {
    inputs: Vec<Box<dyn Operator>>,
    key_column: usize,
    deduplicate: bool,
    batch_size: usize,
    config: SpillConfig,
    schema: RowSchema,
    runs: Vec<MergeRun>,
    last_row: Option<Vec<QueryValue>>,
    initialized: bool,
    done: bool,
}

impl SpillMergeOperator {
    pub(crate) fn new(
        inputs: Vec<Box<dyn Operator>>,
        key_column: usize,
        deduplicate: bool,
        batch_size: usize,
        config: SpillConfig,
    ) -> Result<Self, QueryError> {
        if batch_size == 0 || inputs.is_empty() {
            return Err(QueryError::InvalidPlan(
                "spill merge requires inputs and a nonzero output batch size".into(),
            ));
        }
        let schema = inputs[0].schema().clone();
        Ok(Self {
            inputs,
            key_column,
            deduplicate,
            batch_size,
            config,
            schema,
            runs: Vec::new(),
            last_row: None,
            initialized: false,
            done: false,
        })
    }

    async fn initialize(&mut self, context: &mut QueryContext) -> Result<(), QueryError> {
        for input in &mut self.inputs {
            if input.schema() != &self.schema {
                return Err(QueryError::InvalidPlan(
                    "spill merge inputs have different schemas".into(),
                ));
            }
            while let Some(batch) = input.next_batch(context).await? {
                context.checkpoint()?;
                let estimated = batch.estimated_bytes();
                let spill = if estimated >= self.config.threshold_bytes() {
                    true
                } else {
                    match context.charge_memory(estimated) {
                        Ok(()) => false,
                        Err(QueryError::MemoryBudget) => true,
                        Err(error) => return Err(error),
                    }
                };
                if spill {
                    context.charge_spill(estimated)?;
                }
                let mut rows = batch.rows();
                rows.sort_by(|left, right| compare_for_merge(left, right, self.key_column));
                if spill {
                    let handle = self.config.store().write_run(&self.schema, rows)?;
                    let row_count = self.config.store().row_count(handle)?;
                    self.runs.push(MergeRun::Spilled {
                        handle,
                        row_count,
                        index: 0,
                        cached: None,
                    });
                } else {
                    self.runs.push(MergeRun::Memory { rows, index: 0 });
                }
            }
        }
        self.initialized = true;
        Ok(())
    }
}

impl Operator for SpillMergeOperator {
    fn schema(&self) -> &RowSchema {
        &self.schema
    }

    fn next_batch<'a>(
        &'a mut self,
        context: &'a mut QueryContext,
    ) -> QueryFuture<'a, Option<ColumnBatch>> {
        Box::pin(async move {
            context.checkpoint()?;
            if self.done {
                return Ok(None);
            }
            if !self.initialized {
                self.initialize(context).await?;
            }
            let mut output = Vec::new();
            while output.len() < self.batch_size {
                for run in &mut self.runs {
                    run.ensure_head(context, self.config.store().as_ref())?;
                }
                let selected = self
                    .runs
                    .iter()
                    .enumerate()
                    .filter_map(|(index, run)| run.head().map(|row| (index, row)))
                    .min_by(|(_, left), (_, right)| compare_for_merge(left, right, self.key_column))
                    .map(|(index, _)| index);
                let Some(selected) = selected else {
                    self.done = true;
                    break;
                };
                let row = self.runs[selected]
                    .take_head(self.config.store().as_ref())?
                    .expect("selected run has a head");
                if self.deduplicate && self.last_row.as_ref() == Some(&row) {
                    context.release_memory(estimate_row(&row));
                    continue;
                }
                if self.deduplicate {
                    let bytes = estimate_row(&row);
                    context.charge_memory(bytes)?;
                    if let Some(previous) = self.last_row.replace(row.clone()) {
                        context.release_memory(estimate_row(&previous));
                    }
                }
                output.push(row);
            }
            if output.is_empty() {
                Ok(None)
            } else {
                ColumnBatch::from_rows(self.schema.clone(), output).map(Some)
            }
        })
    }
}

impl Drop for SpillMergeOperator {
    fn drop(&mut self) {
        for handle in self.runs.iter().filter_map(MergeRun::spilled_handle) {
            let _ = self.config.store().remove_run(handle);
        }
    }
}

fn compare_for_merge(
    left: &[QueryValue],
    right: &[QueryValue],
    key_column: usize,
) -> std::cmp::Ordering {
    match (left.get(key_column), right.get(key_column)) {
        (Some(left), Some(right)) => left.total_cmp(right),
        (Some(_), None) => std::cmp::Ordering::Greater,
        (None, Some(_)) => std::cmp::Ordering::Less,
        (None, None) => std::cmp::Ordering::Equal,
    }
    .then_with(|| compare_rows(left, right))
}

fn estimate_row(row: &[QueryValue]) -> u64 {
    row.iter().map(QueryValue::estimated_bytes).sum()
}
