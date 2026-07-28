use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use adapter_memory::MemoryAdapter;
use distributed_query::{
    DistributedQueryError, FragmentRequest, LocalFragmentWorker, SnapshotToken, WorkerBatch,
};
use physical_plan::{
    AccessGuarantee, FragmentExecutionBudget, MemoryBudget, PhysicalAccess, PhysicalOperator,
    PhysicalPlanBuilder, PhysicalPlanHeader, Placement, PrimitiveKind, RawScanBudget,
    ResidualPolicy,
};
use query_executor::{ChangeScanScope, ExecutionContext, RuntimeValue, TemporalBatchExecutor};
use storage_api::{AdapterFuture, ReadSnapshotBinding, StorageAdapter};
use temporal_ir::{
    ChangeAxis, Column, RowSchema, ScalarExpr, SlotId, TransactionTimeSpec, ValueType,
};
use temporal_storage::{
    CommitContext, ElementId, ElementRef, GraphId, LabelId, PartitionId, TemporalStore,
    VertexMutation,
};
use temporal_types::{CanonicalElement, GraphValue, Interval, TransactionTime, ValidTime};

#[test]
fn local_worker_validates_snapshot_identity_before_returning_bounded_batches() {
    let store = TemporalStore::new(MemoryAdapter::new());
    seed(&store);
    let schema = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "n",
        ValueType::Node,
        false,
    )])
    .expect("schema");
    let mut builder =
        PhysicalPlanBuilder::new(PhysicalPlanHeader::new(1, 3, 11, [7; 32]).expect("header"));
    let root = builder
        .add_fragment(
            Placement::Shard(0),
            vec![PhysicalOperator::NodeScan {
                binding: SlotId::new(0),
                labels: vec![11],
                output: schema.clone(),
            }],
            schema,
            MemoryBudget::new(1 << 20, 1 << 20).expect("budget"),
        )
        .expect("fragment");
    let plan = builder.finish(root).expect("plan");
    let snapshot = SnapshotToken::new(1, 3, 11, tx(150), [5; 32]).expect("snapshot");
    let request =
        FragmentRequest::new(root, snapshot, now_ms() + 10_000, 1 << 20, 1).expect("request");
    let worker = LocalFragmentWorker::new(0, 1, 3, 11, [5; 32], TemporalBatchExecutor::new(store));

    let batches = block_on(worker.execute(
        &request,
        &plan.fragments()[0],
        ValidTime::from_micros(5),
        &ExecutionContext::default(),
    ))
    .expect("execute");

    assert_eq!(batches.len(), 1);
    assert_eq!(&batches[0].frame_bytes()[..4], b"DTXE");
    assert_eq!(batches[0].sequence(), 0);
    assert!(!batches[0].has_more());
    assert_eq!(batches[0].row_count(), 1);
    let transported = WorkerBatch::from_frame_bytes(batches[0].frame_bytes().to_vec(), 1 << 20)
        .expect("transport frame");
    let decoded = transported
        .decoded_batch(
            request.snapshot().clone(),
            plan.fragments()[0].output().clone(),
            1 << 20,
        )
        .expect("decode exchange frame");
    assert!(matches!(
        decoded.row(0).expect("columnar row boundary")[0],
        RuntimeValue::Node(_)
    ));
}

#[test]
fn local_worker_rejects_capability_cutover_while_opening_a_change_snapshot() {
    let owner = Arc::new(MemoryAdapter::new());
    let store = TemporalStore::new(GenerationDriftAdapter {
        owner,
        reported_generation: 1,
        bound_generation: 2,
    });
    seed(&store);
    let schema = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "n",
        ValueType::Node,
        false,
    )])
    .unwrap();
    let mut builder = PhysicalPlanBuilder::new(PhysicalPlanHeader::new(1, 3, 11, [7; 32]).unwrap());
    let root = builder
        .add_fragment_with_access(
            Placement::Shard(0),
            vec![
                PhysicalOperator::NodeScan {
                    binding: SlotId::new(0),
                    labels: vec![11],
                    output: schema.clone(),
                },
                PhysicalOperator::ChangeScan {
                    axis: ChangeAxis::ValidTime,
                    start: ScalarExpr::Literal(GraphValue::TimestampMicros(1)),
                    end: ScalarExpr::Literal(GraphValue::TimestampMicros(2)),
                    system_snapshot: TransactionTimeSpec::Current,
                },
            ],
            vec![
                PhysicalAccess::Generic,
                PhysicalAccess::Primitive {
                    primitive: PrimitiveKind::ChangeScan,
                    guarantee: AccessGuarantee::Candidate,
                    residual: ResidualPolicy::Evaluate,
                    constraints: Vec::new(),
                },
            ],
            schema,
            MemoryBudget::new(1 << 20, 1 << 20).unwrap(),
        )
        .unwrap();
    let plan = builder.finish(root).unwrap();
    let request = FragmentRequest::new(
        root,
        SnapshotToken::new(1, 3, 11, tx(150), [5; 32]).unwrap(),
        now_ms() + 10_000,
        1 << 20,
        1,
    )
    .unwrap()
    .with_required_applied_indexes(BTreeMap::from([(0, 1)]))
    .unwrap()
    .with_expected_capability_generations(BTreeMap::from([(0, 1)]))
    .unwrap();
    let worker = LocalFragmentWorker::new(0, 1, 3, 11, [5; 32], TemporalBatchExecutor::new(store));
    let scope = ChangeScanScope::valid(
        GraphId::new(1),
        ValidTime::from_micros(1),
        ValidTime::from_micros(2),
        tx(150),
    )
    .unwrap();

    assert_eq!(
        block_on(worker.execute_change(
            &request,
            &plan.fragments()[0],
            scope,
            &ExecutionContext::default(),
        )),
        Err(DistributedQueryError::CapabilityGenerationMismatch)
    );
}

#[test]
fn local_worker_bounds_the_complete_encoded_exchange_response() {
    let store = TemporalStore::new(MemoryAdapter::new());
    let schema = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "n",
        ValueType::Node,
        false,
    )])
    .expect("schema");
    let mut builder =
        PhysicalPlanBuilder::new(PhysicalPlanHeader::new(1, 3, 11, [7; 32]).expect("header"));
    let root = builder
        .add_fragment(
            Placement::Shard(0),
            vec![PhysicalOperator::NodeScan {
                binding: SlotId::new(0),
                labels: vec![11],
                output: schema.clone(),
            }],
            schema,
            MemoryBudget::new(100, 100).expect("budget"),
        )
        .expect("fragment");
    let plan = builder.finish(root).expect("plan");
    let request = FragmentRequest::new(
        root,
        SnapshotToken::new(1, 3, 11, tx(150), [5; 32]).expect("snapshot"),
        now_ms() + 10_000,
        100,
        1,
    )
    .expect("request")
    .with_expected_capability_generations(BTreeMap::from([(0, 1)]))
    .expect("capability generations");
    let worker = LocalFragmentWorker::new(0, 1, 3, 11, [5; 32], TemporalBatchExecutor::new(store));

    assert!(matches!(
        block_on(worker.execute(
            &request,
            &plan.fragments()[0],
            ValidTime::from_micros(5),
            &ExecutionContext::default(),
        )),
        Err(DistributedQueryError::MemoryLimitExceeded { limit: 100, .. })
    ));
}

#[test]
fn local_worker_streams_interval_rows_without_discarding_regions() {
    let store = TemporalStore::new(MemoryAdapter::new());
    seed(&store);
    let schema = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "n",
        ValueType::Node,
        false,
    )])
    .expect("schema");
    let mut builder =
        PhysicalPlanBuilder::new(PhysicalPlanHeader::new(1, 3, 11, [9; 32]).expect("header"));
    let root = builder
        .add_fragment(
            Placement::Shard(0),
            vec![PhysicalOperator::NodeScan {
                binding: SlotId::new(0),
                labels: vec![11],
                output: schema.clone(),
            }],
            schema,
            MemoryBudget::new(1 << 20, 1 << 20).expect("budget"),
        )
        .expect("fragment");
    let plan = builder.finish(root).expect("plan");
    let snapshot = SnapshotToken::new(1, 3, 11, tx(150), [5; 32]).expect("snapshot");
    let request =
        FragmentRequest::new(root, snapshot, now_ms() + 10_000, 1 << 20, 1).expect("request");
    let worker = LocalFragmentWorker::new(0, 1, 3, 11, [5; 32], TemporalBatchExecutor::new(store));

    let batches = block_on(worker.execute_interval(
        &request,
        &plan.fragments()[0],
        Interval::new(ValidTime::from_micros(1), Some(ValidTime::from_micros(10))).expect("window"),
        &ExecutionContext::default(),
    ))
    .expect("execute interval");

    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].sequence(), 0);
    assert!(!batches[0].has_more());
    assert_eq!(batches[0].batch().rows().len(), 1);
    assert_eq!(
        batches[0].batch().rows()[0].region().valid(),
        Interval::new(ValidTime::from_micros(1), Some(ValidTime::from_micros(10))).expect("region")
    );
}

#[test]
fn local_worker_returns_change_events_and_rejects_a_mismatched_scope_snapshot() {
    let store = TemporalStore::new(MemoryAdapter::new());
    seed(&store);
    let element = ElementRef::vertex(GraphId::new(1), PartitionId::new(0), ElementId::new(1));
    block_on(
        store.commit_vertex(
            CommitContext::new(0, 2, 2, tx(100), tx(150)),
            VertexMutation::delete(
                element,
                LabelId::new(11),
                Interval::new(ValidTime::from_micros(1), None).unwrap(),
            )
            .unwrap(),
        ),
    )
    .unwrap();
    let schema = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "n",
        ValueType::Node,
        false,
    )])
    .unwrap();
    let mut builder = PhysicalPlanBuilder::new(PhysicalPlanHeader::new(1, 3, 11, [7; 32]).unwrap());
    let root = builder
        .add_fragment(
            Placement::Shard(0),
            vec![
                PhysicalOperator::NodeScan {
                    binding: SlotId::new(0),
                    labels: vec![11],
                    output: schema.clone(),
                },
                PhysicalOperator::ChangeScan {
                    axis: ChangeAxis::ValidTime,
                    start: ScalarExpr::Parameter("from".into()),
                    end: ScalarExpr::Parameter("to".into()),
                    system_snapshot: TransactionTimeSpec::Current,
                },
            ],
            schema,
            MemoryBudget::new(1 << 20, 1 << 20).unwrap(),
        )
        .unwrap();
    let plan = builder.finish(root).unwrap();
    let request = FragmentRequest::new(
        root,
        SnapshotToken::new(1, 3, 11, tx(150), [5; 32]).unwrap(),
        now_ms() + 10_000,
        1 << 20,
        1,
    )
    .unwrap()
    .with_required_applied_indexes(BTreeMap::from([(0, 2)]))
    .unwrap();
    let worker = LocalFragmentWorker::new(0, 1, 3, 11, [5; 32], TemporalBatchExecutor::new(store));
    let scope = ChangeScanScope::valid(
        GraphId::new(1),
        ValidTime::from_micros(1),
        ValidTime::from_micros(2),
        tx(150),
    )
    .unwrap();
    let batches = block_on(worker.execute_change(
        &request,
        &plan.fragments()[0],
        scope,
        &ExecutionContext::default(),
    ))
    .unwrap();
    assert_eq!(batches.len(), 2);
    assert!(batches.iter().all(|batch| batch.row_count() == 1));
    let historical_scope = ChangeScanScope::valid(
        GraphId::new(1),
        ValidTime::from_micros(1),
        ValidTime::from_micros(2),
        tx(149),
    )
    .unwrap();
    assert!(
        block_on(worker.execute_change(
            &request,
            &plan.fragments()[0],
            historical_scope,
            &ExecutionContext::default(),
        ))
        .is_ok()
    );
    let wrong_snapshot = ChangeScanScope::valid(
        GraphId::new(1),
        ValidTime::from_micros(1),
        ValidTime::from_micros(2),
        tx(151),
    )
    .unwrap();
    assert_eq!(
        block_on(worker.execute_change(
            &request,
            &plan.fragments()[0],
            wrong_snapshot,
            &ExecutionContext::default()
        )),
        Err(DistributedQueryError::SnapshotMismatch)
    );
}

#[test]
fn local_worker_rejects_change_scan_without_its_shard_read_index() {
    let error = execute_two_change_events_with_required_index(None)
        .expect_err("a change scan must carry this shard's ReadIndex requirement");

    assert_eq!(error, DistributedQueryError::MissingRequiredAppliedIndex(0));
}

#[test]
fn local_worker_rejects_a_fenced_change_scan_behind_the_required_read_index() {
    let error = execute_two_change_events_with_required_index(Some(3))
        .expect_err("the backend fence only covers applied index 2");

    assert_eq!(
        error,
        DistributedQueryError::StaleReadIndex {
            shard_id: 0,
            required: 3,
            actual: 2,
        }
    );
}

#[test]
fn local_worker_accepts_a_fenced_change_scan_covering_the_required_read_index() {
    let batches = execute_two_change_events_with_required_index(Some(2))
        .expect("the exact fenced index covers the shard ReadIndex");

    assert_eq!(batches.len(), 2);
}

#[test]
fn local_worker_enforces_raw_scan_entry_limit_independently_from_resident_memory() {
    let error = execute_two_change_events(1, 1 << 20, 1 << 20, 16)
        .expect_err("two events must exceed the one-entry scan budget");

    assert_eq!(error, DistributedQueryError::StorageFailure);
}

#[test]
fn local_worker_enforces_raw_scan_byte_limit_independently_from_resident_memory() {
    let error = execute_two_change_events(16, 1, 1 << 20, 16)
        .expect_err("event payload must exceed the one-byte scan budget");

    assert_eq!(error, DistributedQueryError::StorageFailure);
}

#[test]
fn local_worker_uses_request_batch_rows_only_for_output_rechunking() {
    let batches =
        execute_two_change_events(2, 1 << 20, 1 << 20, 1).expect("scan budget admits both events");

    assert_eq!(batches.len(), 2);
    assert!(batches.iter().all(|batch| batch.row_count() == 1));
}

#[test]
fn local_worker_rejects_stale_topology_without_exposing_data() {
    let worker = LocalFragmentWorker::new(
        0,
        1,
        3,
        12,
        [5; 32],
        TemporalBatchExecutor::new(TemporalStore::new(MemoryAdapter::new())),
    );
    let snapshot = SnapshotToken::new(1, 3, 11, tx(150), [5; 32]).expect("snapshot");
    let request = FragmentRequest::new(
        physical_plan::FragmentId::new(0),
        snapshot,
        now_ms() + 10_000,
        1024,
        1,
    )
    .expect("request");
    let schema = RowSchema::empty();
    let mut builder =
        PhysicalPlanBuilder::new(PhysicalPlanHeader::new(1, 3, 11, [7; 32]).expect("header"));
    let root = builder
        .add_fragment(
            Placement::Shard(0),
            vec![
                PhysicalOperator::Project {
                    expressions: Vec::new(),
                    output: schema.clone(),
                },
                PhysicalOperator::Finish,
            ],
            schema,
            MemoryBudget::new(1024, 1024).expect("budget"),
        )
        .expect("fragment");
    let plan = builder.finish(root).expect("plan");

    let error = block_on(worker.execute(
        &request,
        &plan.fragments()[0],
        ValidTime::from_micros(5),
        &ExecutionContext::default(),
    ))
    .expect_err("stale topology");
    assert_eq!(error, DistributedQueryError::WorkerIdentityMismatch);
}

#[test]
fn worker_never_widens_an_earlier_parent_deadline() {
    let schema = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "n",
        ValueType::Node,
        false,
    )])
    .unwrap();
    let mut builder = PhysicalPlanBuilder::new(PhysicalPlanHeader::new(1, 3, 11, [7; 32]).unwrap());
    let root = builder
        .add_fragment(
            Placement::Shard(0),
            vec![PhysicalOperator::NodeScan {
                binding: SlotId::new(0),
                labels: Vec::new(),
                output: schema.clone(),
            }],
            schema,
            MemoryBudget::new(1024, 1024).unwrap(),
        )
        .unwrap();
    let plan = builder.finish(root).unwrap();
    let request = FragmentRequest::new(
        root,
        SnapshotToken::new(1, 3, 11, tx(150), [5; 32]).unwrap(),
        now_ms() + 10_000,
        1024,
        1,
    )
    .unwrap();
    let worker = LocalFragmentWorker::new(
        0,
        1,
        3,
        11,
        [5; 32],
        TemporalBatchExecutor::new(TemporalStore::new(MemoryAdapter::new())),
    );
    let expired = Instant::now()
        .checked_sub(Duration::from_millis(1))
        .unwrap();

    let error = block_on(worker.execute(
        &request,
        &plan.fragments()[0],
        ValidTime::from_micros(5),
        &ExecutionContext::default().with_deadline(expired),
    ))
    .expect_err("request deadline must not widen the expired parent deadline");

    assert_eq!(error, DistributedQueryError::DeadlineExceeded);
}

fn seed<A: StorageAdapter>(store: &TemporalStore<A>) {
    let element = ElementRef::vertex(GraphId::new(1), PartitionId::new(0), ElementId::new(1));
    block_on(
        store.commit_vertex(
            CommitContext::new(0, 1, 1, tx(0), tx(100)),
            VertexMutation::put(
                element,
                LabelId::new(11),
                Interval::new(ValidTime::from_micros(1), None).expect("interval"),
                CanonicalElement::new(1, BTreeMap::from([(1, GraphValue::String("node".into()))])),
            )
            .expect("mutation"),
        ),
    )
    .expect("commit");
}

fn execute_two_change_events(
    raw_scan_entry_limit: u64,
    raw_scan_byte_limit: u64,
    resident_memory_bytes: u64,
    output_batch_rows: u32,
) -> Result<Vec<distributed_query::WorkerBatch>, DistributedQueryError> {
    execute_two_change_events_with_options(
        raw_scan_entry_limit,
        raw_scan_byte_limit,
        resident_memory_bytes,
        output_batch_rows,
        Some(2),
    )
}

fn execute_two_change_events_with_required_index(
    required_applied_index: Option<u64>,
) -> Result<Vec<distributed_query::WorkerBatch>, DistributedQueryError> {
    execute_two_change_events_with_options(16, 1 << 20, 1 << 20, 1, required_applied_index)
}

fn execute_two_change_events_with_options(
    raw_scan_entry_limit: u64,
    raw_scan_byte_limit: u64,
    resident_memory_bytes: u64,
    output_batch_rows: u32,
    required_applied_index: Option<u64>,
) -> Result<Vec<distributed_query::WorkerBatch>, DistributedQueryError> {
    let store = TemporalStore::new(MemoryAdapter::new());
    seed(&store);
    let element = ElementRef::vertex(GraphId::new(1), PartitionId::new(0), ElementId::new(1));
    block_on(
        store.commit_vertex(
            CommitContext::new(0, 2, 2, tx(100), tx(150)),
            VertexMutation::delete(
                element,
                LabelId::new(11),
                Interval::new(ValidTime::from_micros(1), None).expect("delete interval"),
            )
            .expect("delete mutation"),
        ),
    )
    .expect("delete commit");
    let schema = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "n",
        ValueType::Node,
        false,
    )])
    .expect("schema");
    let execution_budget = FragmentExecutionBudget::new(
        MemoryBudget::new(resident_memory_bytes, resident_memory_bytes).expect("memory budget"),
        RawScanBudget::new(raw_scan_entry_limit, raw_scan_byte_limit).expect("scan budget"),
    );
    let mut builder =
        PhysicalPlanBuilder::new(PhysicalPlanHeader::new(1, 3, 11, [7; 32]).expect("header"));
    let root = builder
        .add_fragment_with_execution_budget(
            Placement::Shard(0),
            vec![
                PhysicalOperator::NodeScan {
                    binding: SlotId::new(0),
                    labels: vec![11],
                    output: schema.clone(),
                },
                PhysicalOperator::ChangeScan {
                    axis: ChangeAxis::ValidTime,
                    start: ScalarExpr::Parameter("from".into()),
                    end: ScalarExpr::Parameter("to".into()),
                    system_snapshot: TransactionTimeSpec::Current,
                },
            ],
            schema,
            execution_budget,
        )
        .expect("fragment");
    let plan = builder.finish(root).expect("plan");
    let request = FragmentRequest::new(
        root,
        SnapshotToken::new(1, 3, 11, tx(150), [5; 32]).expect("snapshot"),
        now_ms() + 10_000,
        resident_memory_bytes,
        output_batch_rows,
    )
    .expect("request")
    .with_expected_capability_generations(BTreeMap::from([(0, 1)]))
    .expect("capability generations");
    let request = match required_applied_index {
        Some(required) => request
            .with_required_applied_indexes(BTreeMap::from([(0, required)]))
            .expect("required indexes"),
        None => request,
    };
    let worker = LocalFragmentWorker::new(0, 1, 3, 11, [5; 32], TemporalBatchExecutor::new(store));
    let scope = ChangeScanScope::valid(
        GraphId::new(1),
        ValidTime::from_micros(1),
        ValidTime::from_micros(2),
        tx(150),
    )
    .expect("change scope");

    block_on(worker.execute_change(
        &request,
        &plan.fragments()[0],
        scope,
        &ExecutionContext::default(),
    ))
}

fn tx(value: i64) -> TransactionTime {
    TransactionTime::new(value, 0)
}

struct GenerationDriftAdapter {
    owner: Arc<MemoryAdapter>,
    reported_generation: u64,
    bound_generation: u64,
}

impl StorageAdapter for GenerationDriftAdapter {
    fn capabilities(&self) -> storage_api::AdapterCapabilities {
        self.owner.capabilities()
    }

    fn query_primitive_capabilities(&self) -> storage_api::QueryPrimitiveCapabilities {
        self.owner.query_primitive_capabilities()
    }

    fn query_capability_generation(&self) -> u64 {
        self.reported_generation
    }

    fn read_snapshot_binding(
        &self,
    ) -> Result<Option<ReadSnapshotBinding>, storage_api::AdapterError> {
        let owner: Arc<dyn StorageAdapter> = self.owner.clone();
        ReadSnapshotBinding::new(self.bound_generation, owner).map(Some)
    }

    fn apply_committed<'a>(
        &'a self,
        batch: storage_api::CommittedMutationBatch,
    ) -> AdapterFuture<'a, storage_api::ApplyReceipt> {
        self.owner.apply_committed(batch)
    }

    fn multi_get<'a>(
        &'a self,
        keys: &'a [storage_api::LogicalKey],
    ) -> AdapterFuture<'a, Vec<Option<Vec<u8>>>> {
        self.owner.multi_get(keys)
    }

    fn scan<'a>(
        &'a self,
        span: &'a storage_api::KeySpan,
    ) -> AdapterFuture<'a, Vec<storage_api::KeyValue>> {
        self.owner.scan(span)
    }

    fn applied_log_index(&self) -> Result<u64, storage_api::AdapterError> {
        self.owner.applied_log_index()
    }
}

fn now_ms() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_millis(),
    )
    .expect("time")
}

fn block_on<F: Future>(future: F) -> F::Output {
    struct Noop;
    impl Wake for Noop {
        fn wake(self: Arc<Self>) {}
    }
    let waker = Waker::from(Arc::new(Noop));
    let mut context = Context::from_waker(&waker);
    let mut future = Box::pin(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}
