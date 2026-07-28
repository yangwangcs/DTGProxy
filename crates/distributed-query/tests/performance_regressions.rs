#![cfg(feature = "test-support")]

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{SystemTime, UNIX_EPOCH};

use cypher_compiler::{CompileSession, CypherCompiler};
use distributed_query::{
    DistributedCoordinator, DistributedQueryError, ExchangeCodecLimits, ExchangeDecodeExpectation,
    ExchangeFrame, FragmentRequest, FragmentWorker, SnapshotToken, TemporalWorkerFuture,
    WorkerBatch, WorkerFuture, WorkerMorselFuture, WorkerMorselOpenFuture, WorkerMorselSource,
    current_exchange_test_metrics, install_exchange_test_metrics,
};
use physical_plan::{
    MemoryBudget, PhysicalOperator, PhysicalPlan, PhysicalPlanBuilder, PhysicalPlanHeader,
    Placement, PlanFragment,
};
use query_executor::{
    CancellationToken, ColumnBatch, ExecutionContext, QueryExecutionMetrics, RecordBatch,
    RuntimeValue, VertexRecord,
};
use query_optimizer::{DeploymentMode, Optimizer, OptimizerContext};
use temporal_ir::{Column, RowSchema, SlotId, ValueType};
use temporal_storage::{ElementId, ElementRef, GraphId, LabelId, PartitionId};
use temporal_types::{CanonicalElement, Interval, TransactionTime, ValidTime};

#[tokio::test]
async fn metrics_guard_restores_the_prior_test_sink_on_drop() {
    let outer = install_exchange_test_metrics();
    outer.reserve_frame(8);
    assert_eq!(outer.retained_frame_bytes(), 8);
    assert!(Arc::ptr_eq(
        &outer.metrics(),
        &current_exchange_test_metrics().expect("outer metrics installed")
    ));

    {
        let inner = install_exchange_test_metrics();
        inner.reserve_frame(3);
        assert_eq!(inner.retained_frame_bytes(), 3);
        assert!(Arc::ptr_eq(
            &inner.metrics(),
            &current_exchange_test_metrics().expect("inner metrics installed")
        ));
    }

    assert!(Arc::ptr_eq(
        &outer.metrics(),
        &current_exchange_test_metrics().expect("outer metrics restored")
    ));
}

#[test]
fn successful_exchange_roundtrip_records_encode_and_decode_once() {
    let metrics = install_exchange_test_metrics();
    let schema = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "value",
        ValueType::Integer,
        false,
    )])
    .unwrap();
    let snapshot = SnapshotToken::new(1, 1, 1, TransactionTime::new(1, 0), [1; 32]).unwrap();
    let batch = ColumnBatch::from_record_batch(
        &RecordBatch::try_new(schema.clone(), vec![vec![RuntimeValue::Integer(1)]]).unwrap(),
    )
    .unwrap();
    let limits = ExchangeCodecLimits::new(1024, 4096).unwrap();
    let frame = ExchangeFrame::encode(0, 0, false, &snapshot, &batch, limits).unwrap();
    let frame_bytes = u64::try_from(frame.as_bytes().len()).unwrap();

    assert_eq!(metrics.encoded_frames(), 1);
    frame
        .decode(
            ExchangeDecodeExpectation::new(0, 0, snapshot, schema),
            limits,
        )
        .unwrap();
    assert_eq!(metrics.decoded_frames(), 1);
    assert_eq!(metrics.retained_frame_bytes(), 0);
    assert_eq!(metrics.peak_retained_frame_bytes(), frame_bytes);
}

#[test]
fn rejected_exchange_decode_releases_frame_reservation_without_success_count() {
    let metrics = install_exchange_test_metrics();
    let schema = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "value",
        ValueType::Integer,
        false,
    )])
    .unwrap();
    let snapshot = SnapshotToken::new(1, 1, 1, TransactionTime::new(1, 0), [1; 32]).unwrap();
    let batch = ColumnBatch::from_record_batch(
        &RecordBatch::try_new(schema.clone(), vec![vec![RuntimeValue::Integer(1)]]).unwrap(),
    )
    .unwrap();
    let limits = ExchangeCodecLimits::new(1024, 4096).unwrap();
    let frame = ExchangeFrame::encode(0, 0, false, &snapshot, &batch, limits).unwrap();
    let frame_bytes = u64::try_from(frame.as_bytes().len()).unwrap();

    assert!(
        frame
            .decode(
                ExchangeDecodeExpectation::new(0, 1, snapshot, schema),
                limits,
            )
            .is_err()
    );
    assert_eq!(metrics.decoded_frames(), 0);
    assert_eq!(metrics.retained_frame_bytes(), 0);
    assert_eq!(metrics.peak_retained_frame_bytes(), frame_bytes);
}

#[tokio::test]
async fn first_visible_batch_does_not_requeue_its_source() {
    let snapshot = snapshot();
    let schema = integer_schema();
    let plan = exists_plan(schema);
    let child_schema = plan
        .fragments()
        .iter()
        .flat_map(|fragment| fragment.operators())
        .find_map(|operator| match operator {
            PhysicalOperator::Apply { apply, .. } => apply
                .child_plan()
                .fragments()
                .iter()
                .find(|fragment| matches!(fragment.placement(), Placement::AllShards))
                .map(|fragment| fragment.output().clone()),
            _ => None,
        })
        .expect("EXISTS child scan schema");
    let node = || {
        RuntimeValue::Node(VertexRecord::new(
            ElementRef::vertex(GraphId::new(1), PartitionId::new(0), ElementId::new(1)),
            Some(LabelId::new(1)),
            CanonicalElement::new(1, Default::default()),
        ))
    };
    let next_calls = Arc::new(AtomicUsize::new(0));
    let source = ScriptedSource {
        next_calls: Arc::clone(&next_calls),
        frame_credits: None,
        batches: VecDeque::from([
            Ok(Some(worker_batch_value(
                0,
                0,
                true,
                &snapshot,
                &child_schema,
                node(),
            ))),
            Ok(Some(worker_batch_value(
                0,
                1,
                false,
                &snapshot,
                &child_schema,
                node(),
            ))),
        ]),
    };
    let mut coordinator = DistributedCoordinator::new(1 << 20, 1).unwrap();
    coordinator
        .register(Arc::new(SourceWorker::new(0, source)))
        .unwrap();

    let batches = coordinator
        .execute_plan(
            &plan,
            snapshot,
            ValidTime::from_micros(1),
            deadline_ms(),
            1,
            &ExecutionContext::default(),
        )
        .await
        .unwrap();

    assert_eq!(batches[0].rows(), &[vec![RuntimeValue::Boolean(true)]]);
    assert_eq!(next_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn source_error_releases_all_retained_decode_credit() {
    let metrics = install_exchange_test_metrics();
    let snapshot = snapshot();
    let schema = integer_schema();
    let source = ScriptedSource {
        next_calls: Arc::new(AtomicUsize::new(0)),
        frame_credits: None,
        batches: VecDeque::from([
            Ok(Some(worker_batch(0, 0, true, &snapshot, &schema, 1))),
            Ok(Some(worker_batch(0, 2, false, &snapshot, &schema, 2))),
        ]),
    };
    let mut coordinator = DistributedCoordinator::new(1 << 20, 1).unwrap();
    coordinator
        .register(Arc::new(SourceWorker::new(0, source)))
        .unwrap();
    let plan = shard_plan(schema, vec![0]);
    let request = FragmentRequest::new(plan.root(), snapshot, deadline_ms(), 1 << 20, 1)
        .unwrap()
        .with_expected_shards(vec![0])
        .unwrap();

    let error = coordinator
        .execute(
            &request,
            &plan.fragments()[0],
            ValidTime::from_micros(1),
            &ExecutionContext::default(),
        )
        .await
        .unwrap_err();

    assert_eq!(
        error,
        DistributedQueryError::UnexpectedSequence {
            shard_id: 0,
            expected: 1,
            actual: 2,
        }
    );
    assert_eq!(metrics.decoded_frames(), 1);
    assert!(metrics.peak_retained_decoded_bytes() > 0);
    assert_eq!(metrics.retained_decoded_bytes(), 0);
}

#[tokio::test]
async fn execute_each_batch_delivers_batches_without_query_wide_collection() {
    let snapshot = snapshot();
    let schema = integer_schema();
    let next_calls = Arc::new(AtomicUsize::new(0));
    let source = ScriptedSource {
        next_calls: Arc::clone(&next_calls),
        frame_credits: None,
        batches: VecDeque::from([
            Ok(Some(worker_batch(0, 0, true, &snapshot, &schema, 1))),
            Ok(Some(worker_batch(0, 1, false, &snapshot, &schema, 2))),
        ]),
    };
    let mut coordinator = DistributedCoordinator::new(1 << 20, 1).unwrap();
    coordinator
        .register(Arc::new(SourceWorker::new(0, source)))
        .unwrap();
    let plan = shard_plan(schema, vec![0]);
    let request = FragmentRequest::new(plan.root(), snapshot, deadline_ms(), 1 << 20, 1)
        .unwrap()
        .with_expected_shards(vec![0])
        .unwrap();
    let delivered = Arc::new(Mutex::new(Vec::new()));

    coordinator
        .execute_each_batch(
            &request,
            &plan.fragments()[0],
            ValidTime::from_micros(1),
            &ExecutionContext::default(),
            {
                let delivered = Arc::clone(&delivered);
                move |batch| {
                    delivered.lock().unwrap().push(batch.rows()[0][0].clone());
                    std::future::ready(Ok(()))
                }
            },
        )
        .await
        .unwrap();

    assert_eq!(
        *delivered.lock().unwrap(),
        vec![RuntimeValue::Integer(1), RuntimeValue::Integer(2)]
    );
    assert_eq!(next_calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn execute_each_batch_stops_pulling_when_the_consumer_rejects_a_batch() {
    let snapshot = snapshot();
    let schema = integer_schema();
    let next_calls = Arc::new(AtomicUsize::new(0));
    let source = ScriptedSource {
        next_calls: Arc::clone(&next_calls),
        frame_credits: None,
        batches: VecDeque::from([
            Ok(Some(worker_batch(0, 0, true, &snapshot, &schema, 1))),
            Ok(Some(worker_batch(0, 1, false, &snapshot, &schema, 2))),
        ]),
    };
    let mut coordinator = DistributedCoordinator::new(1 << 20, 1).unwrap();
    coordinator
        .register(Arc::new(SourceWorker::new(0, source)))
        .unwrap();
    let plan = shard_plan(schema, vec![0]);
    let request = FragmentRequest::new(plan.root(), snapshot, deadline_ms(), 1 << 20, 1)
        .unwrap()
        .with_expected_shards(vec![0])
        .unwrap();

    let error = coordinator
        .execute_each_batch(
            &request,
            &plan.fragments()[0],
            ValidTime::from_micros(1),
            &ExecutionContext::default(),
            |_batch| {
                std::future::ready(Err(DistributedQueryError::Execution(
                    "consumer stopped".into(),
                )))
            },
        )
        .await
        .unwrap_err();

    assert_eq!(
        error,
        DistributedQueryError::Execution("consumer stopped".into())
    );
    assert_eq!(next_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cancellation_keeps_pending_sources_within_credit_and_drops_them() {
    const CREDIT: usize = 2;
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let (started_tx, mut started_rx) = tokio::sync::mpsc::unbounded_channel();
    let mut coordinator = DistributedCoordinator::new(1 << 20, CREDIT).unwrap();
    for shard_id in 0..4 {
        coordinator
            .register(Arc::new(SourceWorker::new(
                shard_id,
                PendingSource {
                    shard_id,
                    active: Arc::clone(&active),
                    peak: Arc::clone(&peak),
                    started_tx: started_tx.clone(),
                },
            )))
            .unwrap();
    }
    drop(started_tx);
    let snapshot = snapshot();
    let plan = shard_plan(integer_schema(), vec![0, 1, 2, 3]);
    let request = FragmentRequest::new(plan.root(), snapshot, deadline_ms(), 1 << 20, 1)
        .unwrap()
        .with_expected_shards(vec![0, 1, 2, 3])
        .unwrap();
    let cancellation = CancellationToken::new();
    let context = ExecutionContext::default().with_cancellation(cancellation.clone());
    let task = tokio::spawn(async move {
        coordinator
            .execute(
                &request,
                &plan.fragments()[0],
                ValidTime::from_micros(1),
                &context,
            )
            .await
    });

    let mut started = vec![
        started_rx.recv().await.unwrap(),
        started_rx.recv().await.unwrap(),
    ];
    started.sort_unstable();
    assert_eq!(started, vec![0, 1]);
    assert_eq!(active.load(Ordering::SeqCst), CREDIT);
    assert_eq!(peak.load(Ordering::SeqCst), CREDIT);
    assert!(started_rx.try_recv().is_err());

    cancellation.cancel();
    assert_eq!(task.await.unwrap(), Err(DistributedQueryError::Cancelled));
    assert_eq!(active.load(Ordering::SeqCst), 0);
    assert!(started_rx.try_recv().is_err());
}

#[tokio::test]
async fn cancellation_acknowledgement_stops_encoded_and_sent_frame_counts() {
    let query_metrics = Arc::new(QueryExecutionMetrics::default());
    let (encoded_tx, encoded_rx) = tokio::sync::oneshot::channel();
    let mut coordinator = DistributedCoordinator::new(1 << 20, 1).unwrap();
    coordinator
        .register(Arc::new(SourceWorker::new(
            0,
            EncodeThenPendingSource {
                snapshot: snapshot(),
                metrics: Arc::clone(&query_metrics),
                encoded_tx: Some(encoded_tx),
            },
        )))
        .unwrap();
    let plan = shard_plan(integer_schema(), vec![0]);
    let request = FragmentRequest::new(plan.root(), snapshot(), deadline_ms(), 1 << 20, 1)
        .unwrap()
        .with_expected_shards(vec![0])
        .unwrap();
    let cancellation = CancellationToken::new();
    let context = ExecutionContext::default()
        .with_cancellation(cancellation.clone())
        .with_query_metrics(Arc::clone(&query_metrics));
    let task = tokio::spawn(async move {
        coordinator
            .execute(
                &request,
                &plan.fragments()[0],
                ValidTime::from_micros(1),
                &context,
            )
            .await
    });

    encoded_rx.await.unwrap();
    assert!(query_metrics.snapshot().wire_encoded_bytes() > 0);
    assert_eq!(query_metrics.snapshot().sent_frame_count(), 0);

    cancellation.cancel();
    assert_eq!(task.await.unwrap(), Err(DistributedQueryError::Cancelled));
    let acknowledged = query_metrics.snapshot();
    for _ in 0..3 {
        tokio::task::yield_now().await;
    }
    let settled = query_metrics.snapshot();
    assert_eq!(
        settled.wire_encoded_bytes(),
        acknowledged.wire_encoded_bytes()
    );
    assert_eq!(
        settled.adapter_rpc_count(),
        acknowledged.adapter_rpc_count()
    );
    assert_eq!(settled.sent_frame_count(), acknowledged.sent_frame_count());
}

#[tokio::test]
async fn successful_morsel_delivery_counts_encoded_and_sent_frames_separately() {
    let query_metrics = Arc::new(QueryExecutionMetrics::default());
    let mut coordinator = DistributedCoordinator::new(1 << 20, 1).unwrap();
    coordinator
        .register(Arc::new(SourceWorker::new(
            0,
            EncodeOneFrameSource {
                snapshot: snapshot(),
                metrics: Arc::clone(&query_metrics),
                emitted: false,
            },
        )))
        .unwrap();
    let plan = shard_plan(integer_schema(), vec![0]);
    let request = FragmentRequest::new(plan.root(), snapshot(), deadline_ms(), 1 << 20, 1)
        .unwrap()
        .with_expected_shards(vec![0])
        .unwrap();
    let context = ExecutionContext::default().with_query_metrics(Arc::clone(&query_metrics));

    let output = coordinator
        .execute(
            &request,
            &plan.fragments()[0],
            ValidTime::from_micros(1),
            &context,
        )
        .await
        .unwrap();

    assert_eq!(output.len(), 1);
    assert!(query_metrics.snapshot().wire_encoded_bytes() > 0);
    assert_eq!(query_metrics.snapshot().sent_frame_count(), 1);
}

fn snapshot() -> SnapshotToken {
    SnapshotToken::new(1, 1, 1, TransactionTime::new(1, 0), [1; 32]).unwrap()
}

fn deadline_ms() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
        + 60_000
}

fn integer_schema() -> RowSchema {
    RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "value",
        ValueType::Integer,
        false,
    )])
    .unwrap()
}

fn worker_batch(
    shard_id: u32,
    sequence: u64,
    has_more: bool,
    snapshot: &SnapshotToken,
    schema: &RowSchema,
    value: i64,
) -> WorkerBatch {
    worker_batch_value(
        shard_id,
        sequence,
        has_more,
        snapshot,
        schema,
        RuntimeValue::Integer(value),
    )
}

fn worker_batch_value(
    shard_id: u32,
    sequence: u64,
    has_more: bool,
    snapshot: &SnapshotToken,
    schema: &RowSchema,
    value: RuntimeValue,
) -> WorkerBatch {
    let batch = ColumnBatch::from_record_batch(
        &RecordBatch::try_new(schema.clone(), vec![vec![value]]).unwrap(),
    )
    .unwrap();
    let limits = ExchangeCodecLimits::new(1024, 4096).unwrap();
    let frame =
        ExchangeFrame::encode(shard_id, sequence, has_more, snapshot, &batch, limits).unwrap();
    WorkerBatch::from_frame_bytes(frame.as_bytes().to_vec(), 4096).unwrap()
}

fn shard_plan(schema: RowSchema, expected_shards: Vec<u32>) -> PhysicalPlan {
    let header = PhysicalPlanHeader::new(1, 1, 1, [2; 32])
        .unwrap()
        .with_expected_shards(expected_shards)
        .unwrap();
    let mut builder = PhysicalPlanBuilder::new(header);
    let root = builder
        .add_fragment(
            Placement::AllShards,
            vec![PhysicalOperator::NodeScan {
                binding: SlotId::new(0),
                labels: vec![1],
                output: schema.clone(),
            }],
            schema,
            MemoryBudget::new(1 << 20, 1 << 20).unwrap(),
        )
        .unwrap();
    builder.finish(root).unwrap()
}

fn exists_plan(_schema: RowSchema) -> PhysicalPlan {
    let logical = CypherCompiler::new()
        .compile(
            "RETURN EXISTS { MATCH (n:Account) RETURN n } AS present",
            &CompileSession::new("accounts", 1, 1, 1).unwrap(),
        )
        .unwrap()
        .logical_plan()
        .clone();
    Optimizer::new()
        .optimize(
            &logical,
            OptimizerContext::new(DeploymentMode::SharedNothing, 1, 1 << 20, 1 << 20).unwrap(),
        )
        .unwrap()
        .plan()
        .clone()
}

struct SourceWorker {
    shard_id: u32,
    source: Mutex<Option<Box<dyn WorkerMorselSource + Send>>>,
}

impl SourceWorker {
    fn new<S>(shard_id: u32, source: S) -> Self
    where
        S: WorkerMorselSource + Send + 'static,
    {
        Self {
            shard_id,
            source: Mutex::new(Some(Box::new(source))),
        }
    }
}

impl FragmentWorker for SourceWorker {
    fn shard_id(&self) -> u32 {
        self.shard_id
    }

    fn execute_fragment<'a>(
        &'a self,
        _request: &'a FragmentRequest,
        _fragment: &'a PlanFragment,
        _valid_time: ValidTime,
        _context: &'a ExecutionContext,
    ) -> WorkerFuture<'a> {
        Box::pin(async {
            Err(DistributedQueryError::Execution(
                "morsel API required".into(),
            ))
        })
    }

    fn open_fragment_morsels<'a>(
        &'a self,
        _request: &'a FragmentRequest,
        _fragment: &'a PlanFragment,
        _valid_time: ValidTime,
        _context: &'a ExecutionContext,
    ) -> WorkerMorselOpenFuture<'a> {
        Box::pin(async move {
            self.source
                .lock()
                .unwrap()
                .take()
                .ok_or_else(|| DistributedQueryError::Execution("source reopened".into()))
        })
    }

    fn execute_interval_fragment<'a>(
        &'a self,
        _request: &'a FragmentRequest,
        _fragment: &'a PlanFragment,
        _window: Interval<ValidTime>,
        _context: &'a ExecutionContext,
    ) -> TemporalWorkerFuture<'a> {
        Box::pin(async { Err(DistributedQueryError::Execution("point test worker".into())) })
    }
}

struct ScriptedSource {
    next_calls: Arc<AtomicUsize>,
    frame_credits: Option<Arc<Mutex<Vec<u64>>>>,
    batches: VecDeque<Result<Option<WorkerBatch>, DistributedQueryError>>,
}

impl WorkerMorselSource for ScriptedSource {
    fn next<'a>(&'a mut self, max_frame_bytes: u64) -> WorkerMorselFuture<'a> {
        self.next_calls.fetch_add(1, Ordering::SeqCst);
        if let Some(frame_credits) = &self.frame_credits {
            frame_credits.lock().unwrap().push(max_frame_bytes);
        }
        let result = self.batches.pop_front().unwrap_or(Ok(None));
        Box::pin(async move { result })
    }
}

struct PendingSource {
    shard_id: u32,
    active: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
    started_tx: tokio::sync::mpsc::UnboundedSender<u32>,
}

struct EncodeThenPendingSource {
    snapshot: SnapshotToken,
    metrics: Arc<QueryExecutionMetrics>,
    encoded_tx: Option<tokio::sync::oneshot::Sender<()>>,
}

struct EncodeOneFrameSource {
    snapshot: SnapshotToken,
    metrics: Arc<QueryExecutionMetrics>,
    emitted: bool,
}

impl WorkerMorselSource for EncodeOneFrameSource {
    fn next<'a>(&'a mut self, max_frame_bytes: u64) -> WorkerMorselFuture<'a> {
        Box::pin(async move {
            if self.emitted {
                return Ok(None);
            }
            self.emitted = true;
            let batch = ColumnBatch::from_record_batch(
                &RecordBatch::try_new(integer_schema(), vec![vec![RuntimeValue::Integer(1)]])
                    .unwrap(),
            )
            .unwrap();
            let limits =
                ExchangeCodecLimits::new(1, usize::try_from(max_frame_bytes).unwrap_or(usize::MAX))
                    .unwrap();
            let frame = ExchangeFrame::encode(0, 0, false, &self.snapshot, &batch, limits).unwrap();
            self.metrics
                .record_wire_encoded_bytes(u64::try_from(frame.as_bytes().len()).unwrap());
            WorkerBatch::from_frame_bytes(
                frame.as_bytes().to_vec(),
                usize::try_from(max_frame_bytes).unwrap_or(usize::MAX),
            )
            .map(Some)
        })
    }
}

impl WorkerMorselSource for EncodeThenPendingSource {
    fn next<'a>(&'a mut self, max_frame_bytes: u64) -> WorkerMorselFuture<'a> {
        Box::pin(std::future::poll_fn(move |_context| {
            if let Some(encoded_tx) = self.encoded_tx.take() {
                let batch = ColumnBatch::from_record_batch(
                    &RecordBatch::try_new(integer_schema(), vec![vec![RuntimeValue::Integer(1)]])
                        .unwrap(),
                )
                .unwrap();
                let limits = ExchangeCodecLimits::new(
                    1,
                    usize::try_from(max_frame_bytes).unwrap_or(usize::MAX),
                )
                .unwrap();
                let frame =
                    ExchangeFrame::encode(0, 0, false, &self.snapshot, &batch, limits).unwrap();
                self.metrics
                    .record_wire_encoded_bytes(u64::try_from(frame.as_bytes().len()).unwrap());
                encoded_tx.send(()).unwrap();
            }
            Poll::Pending
        }))
    }
}

impl WorkerMorselSource for PendingSource {
    fn next<'a>(&'a mut self, _max_frame_bytes: u64) -> WorkerMorselFuture<'a> {
        Box::pin(ObservedPending {
            shard_id: self.shard_id,
            active: Arc::clone(&self.active),
            peak: Arc::clone(&self.peak),
            started_tx: self.started_tx.clone(),
            started: false,
        })
    }
}

struct ObservedPending {
    shard_id: u32,
    active: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
    started_tx: tokio::sync::mpsc::UnboundedSender<u32>,
    started: bool,
}

impl Future for ObservedPending {
    type Output = Result<Option<WorkerBatch>, DistributedQueryError>;

    fn poll(mut self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.started {
            self.started = true;
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(active, Ordering::SeqCst);
            self.started_tx.send(self.shard_id).unwrap();
        }
        Poll::Pending
    }
}

impl Drop for ObservedPending {
    fn drop(&mut self) {
        if self.started {
            self.active.fetch_sub(1, Ordering::SeqCst);
        }
    }
}
