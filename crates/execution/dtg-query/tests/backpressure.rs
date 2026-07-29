mod support;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use dtg_language_ir::{Field, LogicalType, RowSchema, SortDirection};
use dtg_query::{
    BatchOperator, CancellationToken, ColumnBatch, ExchangeOperator, ExecutableAccess,
    ExecutableFragment, ExecutableOperator, ExecutableOperatorKind, ExecutablePlan,
    HashJoinOperator, LogicalRead, Operator, QueryBudget, QueryContext, QueryError, QueryFuture,
    QueryRuntime, QueryStream, QueryValue, ReadOperation, SnapshotGuard, SnapshotShardFence,
    SortOperator, SpillConfig, SpillHandle, SpillStore,
};
use dtg_storage::{CapabilityManifest, ShardId, TransactionTime, Version};

use support::{
    FixtureStore, block_on, execution_fence_for_shard, scan_plan, snapshot, storage_map, vertex,
};

struct CountingOperator {
    schema: RowSchema,
    pulls: Arc<AtomicUsize>,
    remaining: usize,
}

#[derive(Default)]
struct RecordingSpillStore {
    next: AtomicUsize,
    writes: AtomicUsize,
    runs: Mutex<std::collections::BTreeMap<u64, Vec<Vec<QueryValue>>>>,
}

impl SpillStore for RecordingSpillStore {
    fn write_run(
        &self,
        _schema: &RowSchema,
        rows: Vec<Vec<QueryValue>>,
    ) -> Result<SpillHandle, QueryError> {
        let id = u64::try_from(self.next.fetch_add(1, Ordering::SeqCst) + 1).unwrap();
        self.writes.fetch_add(1, Ordering::SeqCst);
        self.runs.lock().unwrap().insert(id, rows);
        Ok(SpillHandle::new(id))
    }

    fn row_count(&self, handle: SpillHandle) -> Result<usize, QueryError> {
        self.runs
            .lock()
            .unwrap()
            .get(&handle.get())
            .map(Vec::len)
            .ok_or_else(|| QueryError::Storage("missing spill run".into()))
    }

    fn row_estimated_bytes(&self, handle: SpillHandle, index: usize) -> Result<u64, QueryError> {
        self.runs
            .lock()
            .unwrap()
            .get(&handle.get())
            .and_then(|rows| rows.get(index))
            .map(|row| row.iter().map(QueryValue::estimated_bytes).sum())
            .ok_or_else(|| QueryError::Storage("missing spill row".into()))
    }

    fn read_row(&self, handle: SpillHandle, index: usize) -> Result<Vec<QueryValue>, QueryError> {
        self.runs
            .lock()
            .unwrap()
            .get(&handle.get())
            .and_then(|rows| rows.get(index))
            .cloned()
            .ok_or_else(|| QueryError::Storage("missing spill row".into()))
    }

    fn remove_run(&self, handle: SpillHandle) -> Result<(), QueryError> {
        self.runs.lock().unwrap().remove(&handle.get());
        Ok(())
    }
}

impl Operator for CountingOperator {
    fn schema(&self) -> &RowSchema {
        &self.schema
    }

    fn next_batch<'a>(
        &'a mut self,
        context: &'a mut QueryContext,
    ) -> QueryFuture<'a, Option<ColumnBatch>> {
        Box::pin(async move {
            context.checkpoint()?;
            self.pulls.fetch_add(1, Ordering::SeqCst);
            if self.remaining == 0 {
                return Ok(None);
            }
            self.remaining -= 1;
            ColumnBatch::from_rows(
                self.schema.clone(),
                vec![vec![QueryValue::Integer(self.remaining as i64)]],
            )
            .map(Some)
        })
    }
}

fn int_batch(values: &[i64]) -> ColumnBatch {
    ColumnBatch::from_rows(
        RowSchema {
            fields: vec![Field {
                name: "value".into(),
                data_type: LogicalType::Integer,
                nullable: false,
            }],
        },
        values
            .iter()
            .map(|value| vec![QueryValue::Integer(*value)])
            .collect(),
    )
    .unwrap()
}

fn string_batch(values: &[String]) -> ColumnBatch {
    ColumnBatch::from_rows(
        RowSchema {
            fields: vec![Field {
                name: "value".into(),
                data_type: LogicalType::String,
                nullable: false,
            }],
        },
        values
            .iter()
            .cloned()
            .map(|value| vec![QueryValue::String(value)])
            .collect(),
    )
    .unwrap()
}

#[test]
fn scan_budget_fails_before_unbounded_materialization() {
    let capabilities = CapabilityManifest::from_names([] as [&str; 0]).unwrap();
    let vertices = (1..=100).map(|id| vertex(id, id as i64)).collect();
    let store = FixtureStore::new(capabilities.clone(), vertices, Vec::new());
    let mut stream = block_on(QueryRuntime::new(64).execute(
        &scan_plan(capabilities, 100),
        storage_map(store.storage(false)),
        &snapshot(),
        QueryBudget::rows(10),
        CancellationToken::new(),
        None,
    ))
    .unwrap();
    let error = block_on(stream.collect()).unwrap_err();

    assert_eq!(error.code(), "DTG-QUERY-ROW-BUDGET");
    assert_eq!(store.scan_calls(), 1);
    assert!(store.max_scan_limit() <= 10);
}

#[test]
fn query_stream_pulls_only_on_consumer_demand() {
    let pulls = Arc::new(AtomicUsize::new(0));
    let operator = CountingOperator {
        schema: int_batch(&[1]).schema().clone(),
        pulls: pulls.clone(),
        remaining: 2,
    };
    let mut stream = QueryStream::from_operator(
        Box::new(operator),
        QueryBudget::unlimited(),
        CancellationToken::new(),
    );
    assert_eq!(pulls.load(Ordering::SeqCst), 0);
    assert!(block_on(stream.next_batch()).unwrap().is_some());
    assert_eq!(pulls.load(Ordering::SeqCst), 1);
    assert!(block_on(stream.next_batch()).unwrap().is_some());
    assert_eq!(pulls.load(Ordering::SeqCst), 2);
}

#[test]
fn cancellation_and_deadline_are_checked_before_operator_work() {
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let mut cancelled = QueryStream::from_operator(
        Box::new(BatchOperator::new(vec![int_batch(&[1])])),
        QueryBudget::unlimited(),
        cancellation,
    );
    assert_eq!(
        block_on(cancelled.next_batch()).unwrap_err().code(),
        "DTG-QUERY-CANCELLED"
    );

    let mut deadline = QueryBudget::unlimited();
    deadline.deadline = Instant::now() - Duration::from_millis(1);
    let mut expired = QueryStream::from_operator(
        Box::new(BatchOperator::new(vec![int_batch(&[1])])),
        deadline,
        CancellationToken::new(),
    );
    assert_eq!(
        block_on(expired.next_batch()).unwrap_err().code(),
        "DTG-QUERY-DEADLINE"
    );
}

#[test]
fn memory_and_network_budgets_fail_closed() {
    let batch = int_batch(&[1, 2, 3]);
    let mut memory = QueryBudget::unlimited();
    memory.max_memory_bytes = 1;
    let mut memory_stream = QueryStream::from_operator(
        Box::new(BatchOperator::new(vec![batch.clone()])),
        memory,
        CancellationToken::new(),
    );
    assert_eq!(
        block_on(memory_stream.collect()).unwrap_err().code(),
        "DTG-QUERY-MEMORY-BUDGET"
    );

    let mut network = QueryBudget::unlimited();
    network.max_network_bytes = 1;
    let exchanged = ExchangeOperator::new(Box::new(BatchOperator::new(vec![batch])));
    let mut network_stream =
        QueryStream::from_operator(Box::new(exchanged), network, CancellationToken::new());
    assert_eq!(
        block_on(network_stream.collect()).unwrap_err().code(),
        "DTG-QUERY-NETWORK-BUDGET"
    );
}

#[test]
fn sort_reserves_each_input_batch_before_growing_its_materialization() {
    let pulls = Arc::new(AtomicUsize::new(0));
    let input = CountingOperator {
        schema: int_batch(&[1]).schema().clone(),
        pulls: pulls.clone(),
        remaining: 3,
    };
    let mut budget = QueryBudget::unlimited();
    budget.max_memory_bytes = 8;
    let mut stream = QueryStream::from_operator(
        Box::new(SortOperator::new(
            Box::new(input),
            0,
            SortDirection::Ascending,
        )),
        budget,
        CancellationToken::new(),
    );

    assert_eq!(
        block_on(stream.collect()).unwrap_err().code(),
        "DTG-QUERY-MEMORY-BUDGET"
    );
    assert_eq!(pulls.load(Ordering::SeqCst), 2);
}

#[test]
fn hash_join_charges_output_before_growth_and_emits_bounded_batches() {
    let left = int_batch(&[1]);
    let right = int_batch(&[1, 1, 1, 1, 1]);
    let mut join = HashJoinOperator::with_batch_size(
        Box::new(BatchOperator::new(vec![left])),
        Box::new(BatchOperator::new(vec![right])),
        0,
        0,
        2,
    )
    .unwrap();
    let mut context = QueryContext::new(QueryBudget::unlimited(), CancellationToken::new());

    let first = block_on(join.next_batch(&mut context)).unwrap().unwrap();
    assert_eq!(first.row_count(), 2);
    assert!(context.memory_bytes() >= first.estimated_bytes());
    let second = block_on(join.next_batch(&mut context)).unwrap().unwrap();
    assert_eq!(second.row_count(), 2);
    let third = block_on(join.next_batch(&mut context)).unwrap().unwrap();
    assert_eq!(third.row_count(), 1);
    assert!(block_on(join.next_batch(&mut context)).unwrap().is_none());
}

#[test]
fn hash_join_accounts_for_owned_string_keys_before_building_the_table() {
    let key = "k".repeat(128);
    let left = string_batch(std::slice::from_ref(&key));
    let right = string_batch(std::slice::from_ref(&key));
    let retained_input_bytes = left.estimated_bytes() + right.estimated_bytes();
    let output_bytes = retained_input_bytes;
    let expected_key_and_container_bytes = key.len() as u64 + 32 + 8;
    let mut join = HashJoinOperator::with_batch_size(
        Box::new(BatchOperator::new(vec![left])),
        Box::new(BatchOperator::new(vec![right])),
        0,
        0,
        1,
    )
    .unwrap();
    let mut context = QueryContext::new(QueryBudget::unlimited(), CancellationToken::new());

    let batch = block_on(join.next_batch(&mut context)).unwrap().unwrap();

    assert_eq!(batch.row_count(), 1);
    assert_eq!(
        context.memory_bytes(),
        retained_input_bytes + output_bytes + expected_key_and_container_bytes
    );
}

#[test]
fn configured_query_runtime_selects_spill_for_production_multi_shard_merge() {
    let capabilities = CapabilityManifest::from_names([] as [&str; 0]).unwrap();
    let fragments = [13_u64, 17_u64]
        .into_iter()
        .enumerate()
        .map(|(index, shard_id)| {
            ExecutableFragment::with_access_nodes(
                u32::try_from(index + 1).unwrap(),
                execution_fence_for_shard(&capabilities, shard_id),
                vec![ExecutableAccess::Logical(
                    LogicalRead::new(
                        ReadOperation::VertexScan,
                        8,
                        TransactionTime::new(23).unwrap(),
                        17,
                    )
                    .unwrap(),
                )],
                vec![1],
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let plan = ExecutablePlan::with_operators(
        Version::new(1),
        fragments,
        1,
        vec![
            ExecutableOperator::new(
                1,
                ExecutableOperatorKind::Source {
                    logical_node: 1,
                    fragments: vec![1, 2],
                    output: "vertex".into(),
                },
            )
            .unwrap(),
        ],
        RowSchema::empty(),
    )
    .unwrap();
    let left =
        FixtureStore::new_for_shard(capabilities.clone(), 13, vec![vertex(1, 10)], Vec::new());
    let right = FixtureStore::new_for_shard(capabilities, 17, vec![vertex(2, 20)], Vec::new());
    let storage = std::collections::BTreeMap::from([
        (ShardId::new(13).unwrap(), left.storage(false)),
        (ShardId::new(17).unwrap(), right.storage(false)),
    ]);
    let snapshot = SnapshotGuard::new(
        TransactionTime::new(23).unwrap(),
        Version::new(11),
        vec![
            (
                ShardId::new(13).unwrap(),
                SnapshotShardFence {
                    placement_epoch: dtg_storage::PlacementEpoch::new(7).unwrap(),
                    backend_generation: dtg_storage::BackendGeneration::new(3).unwrap(),
                    applied_index: 29,
                },
            ),
            (
                ShardId::new(17).unwrap(),
                SnapshotShardFence {
                    placement_epoch: dtg_storage::PlacementEpoch::new(7).unwrap(),
                    backend_generation: dtg_storage::BackendGeneration::new(3).unwrap(),
                    applied_index: 29,
                },
            ),
        ],
    )
    .unwrap();
    let spill = Arc::new(RecordingSpillStore::default());
    let runtime = QueryRuntime::new(16).with_spill(SpillConfig::new(spill.clone(), 1).unwrap());

    let mut stream = block_on(runtime.execute(
        &plan,
        storage,
        &snapshot,
        QueryBudget::unlimited(),
        CancellationToken::new(),
        None,
    ))
    .unwrap();
    assert_eq!(block_on(stream.collect()).unwrap().row_count(), 2);
    assert!(spill.writes.load(Ordering::SeqCst) > 0);
}
