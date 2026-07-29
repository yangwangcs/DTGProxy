mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use dtg_language_ir::{Field, LogicalType, RowSchema};
use dtg_query::{
    BatchOperator, CancellationToken, ColumnBatch, ExchangeOperator, Operator, QueryBudget,
    QueryContext, QueryFuture, QueryRuntime, QueryStream, QueryValue,
};
use dtg_storage::CapabilityManifest;

use support::{FixtureStore, block_on, scan_plan, snapshot, storage_map, vertex};

struct CountingOperator {
    schema: RowSchema,
    pulls: Arc<AtomicUsize>,
    remaining: usize,
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
