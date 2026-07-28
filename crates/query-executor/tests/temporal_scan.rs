use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use adapter_memory::MemoryAdapter;
use physical_plan::{
    AccessGuarantee, MemoryBudget, PhysicalAccess, PhysicalComparisonOperator, PhysicalOperator,
    PhysicalPlanBuilder, PhysicalPlanHeader, PhysicalPropertyConstraint, Placement, PrimitiveKind,
    ResidualPolicy,
};
use query_executor::{
    BatchExecutor, CancellationToken, ChangeEventBatch, ChangeScanScope, ExecutionContext,
    RuntimeError, RuntimeValue, TemporalBatchExecutor, TemporalExecutionError, TemporalRead,
    TemporalRegion,
};
use storage_api::{ReadSnapshot, StorageAdapter};
use temporal_ir::{
    ChangeAxis, Column, RowSchema, ScalarExpr, SlotId, TransactionTimeSpec, ValueType,
};
use temporal_storage::{
    CommitContext, EdgeMutation, EdgeTypeId, ElementId, ElementRef, GraphId, LabelId, PartitionId,
    TemporalStore, VertexMutation,
};
use temporal_types::{CanonicalElement, GraphValue, Interval, TransactionTime, ValidTime};

#[test]
fn node_scan_applies_label_and_transaction_time_fences_before_projection() {
    let store = TemporalStore::new(MemoryAdapter::new());
    seed(&store, 1, 11, "account");
    seed(&store, 2, 12, "person");
    let scan_schema = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "n",
        ValueType::Node,
        false,
    )])
    .expect("schema");
    let mut builder =
        PhysicalPlanBuilder::new(PhysicalPlanHeader::new(1, 1, 1, [7; 32]).expect("header"));
    let root = builder
        .add_fragment(
            Placement::Shard(0),
            vec![
                PhysicalOperator::NodeScan {
                    binding: SlotId::new(0),
                    labels: vec![11],
                    output: scan_schema.clone(),
                },
                PhysicalOperator::Project {
                    expressions: vec![(SlotId::new(0), ScalarExpr::Slot(SlotId::new(0)))],
                    output: scan_schema.clone(),
                },
            ],
            scan_schema,
            MemoryBudget::new(1 << 20, 1 << 20).expect("budget"),
        )
        .expect("fragment");
    let plan = builder.finish(root).expect("plan");

    let batches = block_on(TemporalBatchExecutor::new(store).execute_fragment(
        &plan.fragments()[0],
        &ExecutionContext::default(),
        TemporalRead::as_of(GraphId::new(1), ValidTime::from_micros(5), tx(150)),
    ))
    .expect("execute");

    assert_eq!(
        batches
            .iter()
            .map(|batch| batch.rows().len())
            .sum::<usize>(),
        1
    );
    let RuntimeValue::Node(node) = &batches[0].rows()[0][0] else {
        panic!("expected node");
    };
    assert_eq!(node.element().id(), ElementId::new(1));
    assert_eq!(node.label(), Some(LabelId::new(11)));
}

#[test]
fn opening_record_morsels_defers_execution_until_first_pull() {
    let store = TemporalStore::new(MemoryAdapter::new());
    let schema = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "n",
        ValueType::Node,
        false,
    )])
    .expect("schema");
    let mut builder =
        PhysicalPlanBuilder::new(PhysicalPlanHeader::new(1, 1, 1, [7; 32]).expect("header"));
    let root = builder
        .add_fragment(
            Placement::Shard(0),
            vec![PhysicalOperator::NodeScan {
                binding: SlotId::new(0),
                labels: Vec::new(),
                output: schema.clone(),
            }],
            schema,
            MemoryBudget::new(1 << 20, 1 << 20).expect("budget"),
        )
        .expect("fragment");
    let plan = builder.finish(root).expect("plan");
    let executor = TemporalBatchExecutor::new(store);
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let context = ExecutionContext::default().with_cancellation(cancellation);

    let mut source = executor.open_fragment_morsels(
        &plan.fragments()[0],
        &context,
        TemporalRead::current(GraphId::new(1), ValidTime::from_micros(5)),
        None,
        16,
    );

    assert!(matches!(
        block_on(source.next()),
        Err(TemporalExecutionError::Runtime(RuntimeError::Cancelled))
    ));
}

#[test]
fn interval_node_scan_returns_a_segment_that_exists_only_inside_the_window() {
    let store = TemporalStore::new(MemoryAdapter::new());
    let element = ElementRef::vertex(GraphId::new(1), PartitionId::new(0), ElementId::new(77));
    block_on(
        store.commit_vertex(
            CommitContext::new(0, 1, 1, tx(99), tx(100)),
            VertexMutation::put(
                element,
                LabelId::new(11),
                Interval::new(ValidTime::from_micros(4), Some(ValidTime::from_micros(6)))
                    .expect("valid interval"),
                CanonicalElement::new(1, BTreeMap::new()),
            )
            .expect("vertex mutation"),
        ),
    )
    .expect("commit");

    let rows = block_on(
        TemporalBatchExecutor::new(store).scan_vertex_rows_interval_as_of(
            GraphId::new(1),
            &[11],
            Interval::new(ValidTime::from_micros(1), Some(ValidTime::from_micros(10)))
                .expect("query window"),
            tx(150),
        ),
    )
    .expect("interval scan");

    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].region().valid(),
        Interval::new(ValidTime::from_micros(4), Some(ValidTime::from_micros(6)))
            .expect("expected segment")
    );
    let RuntimeValue::Node(node) = &rows[0].values()[0] else {
        panic!("expected node");
    };
    assert_eq!(node.element(), element);
    assert_eq!(
        rows[0].region().transaction(),
        TemporalRegion::at_transaction(tx(150))
            .expect("transaction region")
            .transaction()
    );
}

#[test]
fn change_node_scan_reads_delete_events_without_reconstructing_current_state() {
    let store = TemporalStore::new(MemoryAdapter::new());
    let element = ElementRef::vertex(GraphId::new(1), PartitionId::new(0), ElementId::new(88));
    let label = LabelId::new(11);
    block_on(
        store.commit_vertex(
            CommitContext::new(0, 1, 1, tx(0), tx(10)),
            VertexMutation::put(
                element,
                label,
                Interval::new(ValidTime::from_micros(1), Some(ValidTime::from_micros(5))).unwrap(),
                CanonicalElement::new(
                    1,
                    BTreeMap::from([(1, GraphValue::String("before".into()))]),
                ),
            )
            .unwrap(),
        ),
    )
    .unwrap();
    block_on(
        store.commit_vertex(
            CommitContext::new(0, 2, 2, tx(10), tx(20)),
            VertexMutation::delete(
                element,
                label,
                Interval::new(ValidTime::from_micros(1), Some(ValidTime::from_micros(5))).unwrap(),
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
    let scope = ChangeScanScope::valid(
        GraphId::new(1),
        ValidTime::from_micros(1),
        ValidTime::from_micros(2),
        tx(20),
    )
    .unwrap();

    let ChangeEventBatch {
        columns,
        events,
        applied_log_index,
    } = block_on(TemporalBatchExecutor::new(store).scan_change_nodes(
        &scope,
        &[11],
        &schema,
        16,
        1 << 20,
    ))
    .unwrap();
    assert_eq!(applied_log_index, 2);
    assert_eq!(columns.row_count(), 2);
    assert_eq!(events.len(), 2);
    assert_eq!(
        events[1].operation(),
        temporal_storage::TemporalEventOperation::Delete
    );
    assert!(matches!(columns.row(1), Some(row) if matches!(row[0], RuntimeValue::Node(_))));
    let batch = columns.to_record_batch().unwrap();
    let operation = BatchExecutor::new()
        .evaluate(
            &ScalarExpr::Function {
                function_id: function_id("operation"),
                arguments: vec![ScalarExpr::Slot(SlotId::new(0))],
            },
            batch.schema(),
            &batch.rows()[1],
            &ExecutionContext::default(),
        )
        .unwrap();
    assert_eq!(operation, RuntimeValue::String("DELETE".into()));
    assert_eq!(
        evaluate_metadata_function("valid_from", batch.schema(), &batch.rows()[1]),
        RuntimeValue::TimestampMicros(1)
    );
    assert_eq!(
        evaluate_metadata_function("valid_to", batch.schema(), &batch.rows()[1]),
        RuntimeValue::TimestampMicros(5)
    );
    assert_eq!(
        evaluate_metadata_function("system_time", batch.schema(), &batch.rows()[1]),
        RuntimeValue::TimestampMicros(20)
    );
    assert_eq!(
        evaluate_metadata_function("commit_seq", batch.schema(), &batch.rows()[1]),
        RuntimeValue::Integer(0)
    );
}

#[test]
fn planned_change_primitive_uses_the_snapshot_typed_page() {
    let adapter = CountingAdapter::new();
    let store = TemporalStore::new(adapter.clone());
    let element = ElementRef::vertex(GraphId::new(1), PartitionId::new(0), ElementId::new(89));
    block_on(
        store.commit_vertex(
            CommitContext::new(0, 1, 1, tx(0), tx(10)),
            VertexMutation::put(
                element,
                LabelId::new(11),
                Interval::new(ValidTime::from_micros(1), Some(ValidTime::from_micros(5))).unwrap(),
                CanonicalElement::new(1, BTreeMap::new()),
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
    let mut builder =
        PhysicalPlanBuilder::new(PhysicalPlanHeader::new(1, 1, 1, [19; 32]).expect("header"));
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
                    end: ScalarExpr::Literal(GraphValue::TimestampMicros(5)),
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
    let scope = ChangeScanScope::valid(
        GraphId::new(1),
        ValidTime::from_micros(1),
        ValidTime::from_micros(5),
        tx(10),
    )
    .unwrap();
    let executor = TemporalBatchExecutor::new(TemporalStore::new(adapter.clone()));
    let read = block_on(store.begin_read_snapshot()).unwrap();

    let result = block_on(executor.execute_change_fragment_in_snapshot(
        read.as_ref(),
        &plan.fragments()[0],
        &scope,
        &ExecutionContext::default(),
        16,
    ))
    .unwrap();

    assert_eq!(result.applied_log_index(), 1);
    assert_eq!(adapter.change_scan_calls(), 1);
}

#[test]
fn planned_current_node_candidate_scan_uses_the_snapshot_typed_page() {
    let adapter = CountingAdapter::new();
    let store = TemporalStore::new(adapter.clone());
    seed(&store, 1, 11, "candidate");
    seed(&store, 2, 12, "residual-false-positive");
    let schema = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "n",
        ValueType::Node,
        false,
    )])
    .unwrap();
    let mut builder = PhysicalPlanBuilder::new(PhysicalPlanHeader::new(1, 1, 1, [23; 32]).unwrap());
    let root = builder
        .add_fragment_with_access(
            Placement::Shard(0),
            vec![PhysicalOperator::NodeScan {
                binding: SlotId::new(0),
                labels: vec![11],
                output: schema.clone(),
            }],
            vec![PhysicalAccess::Primitive {
                primitive: PrimitiveKind::CandidateScan,
                guarantee: AccessGuarantee::Candidate,
                residual: ResidualPolicy::Evaluate,
                constraints: Vec::new(),
            }],
            schema,
            MemoryBudget::new(1 << 20, 1 << 20).unwrap(),
        )
        .unwrap();
    let plan = builder.finish(root).unwrap();

    let batches = block_on(TemporalBatchExecutor::new(store).execute_fragment(
        &plan.fragments()[0],
        &ExecutionContext::default(),
        TemporalRead::current(GraphId::new(1), ValidTime::from_micros(5)),
    ))
    .unwrap();

    assert_eq!(adapter.candidate_scan_calls(), 1);
    assert_eq!(
        batches
            .iter()
            .map(|batch| batch.rows().len())
            .sum::<usize>(),
        1
    );
    let RuntimeValue::Node(node) = &batches[0].rows()[0][0] else {
        panic!("expected node");
    };
    assert_eq!(node.element().id(), ElementId::new(1));
}

#[test]
fn planned_candidate_scan_falls_back_to_canonical_history_for_as_of_reads() {
    let adapter = CountingAdapter::new();
    let store = TemporalStore::new(adapter.clone());
    seed(&store, 1, 11, "historical");
    let schema = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "n",
        ValueType::Node,
        false,
    )])
    .unwrap();
    let mut builder = PhysicalPlanBuilder::new(PhysicalPlanHeader::new(1, 1, 1, [40; 32]).unwrap());
    let root = builder
        .add_fragment_with_access(
            Placement::Shard(0),
            vec![PhysicalOperator::NodeScan {
                binding: SlotId::new(0),
                labels: vec![11],
                output: schema.clone(),
            }],
            vec![PhysicalAccess::Primitive {
                primitive: PrimitiveKind::CandidateScan,
                guarantee: AccessGuarantee::Candidate,
                residual: ResidualPolicy::Evaluate,
                constraints: Vec::new(),
            }],
            schema,
            MemoryBudget::new(1 << 20, 1 << 20).unwrap(),
        )
        .unwrap();
    let plan = builder.finish(root).unwrap();

    let batches = block_on(TemporalBatchExecutor::new(store).execute_fragment(
        &plan.fragments()[0],
        &ExecutionContext::default(),
        TemporalRead::as_of(GraphId::new(1), ValidTime::from_micros(5), tx(150)),
    ))
    .unwrap();

    assert_eq!(adapter.candidate_scan_calls(), 0);
    assert_eq!(
        batches
            .iter()
            .map(|batch| batch.rows().len())
            .sum::<usize>(),
        1
    );
}

#[test]
fn candidate_morsels_pull_one_canonical_page_at_a_time() {
    let adapter = CountingAdapter::new();
    let store = TemporalStore::new(adapter.clone());
    for id in 1..=3 {
        seed(&store, id, 11, "candidate");
    }
    let schema = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "n",
        ValueType::Node,
        false,
    )])
    .unwrap();
    let mut builder = PhysicalPlanBuilder::new(PhysicalPlanHeader::new(1, 1, 1, [41; 32]).unwrap());
    let root = builder
        .add_fragment_with_access(
            Placement::Shard(0),
            vec![PhysicalOperator::NodeScan {
                binding: SlotId::new(0),
                labels: vec![11],
                output: schema.clone(),
            }],
            vec![PhysicalAccess::Primitive {
                primitive: PrimitiveKind::CandidateScan,
                guarantee: AccessGuarantee::Candidate,
                residual: ResidualPolicy::Evaluate,
                constraints: Vec::new(),
            }],
            schema,
            MemoryBudget::new(1 << 20, 1 << 20).unwrap(),
        )
        .unwrap();
    let plan = builder.finish(root).unwrap();
    let executor = TemporalBatchExecutor::new(store);
    let mut source = executor.open_fragment_morsels(
        &plan.fragments()[0],
        &ExecutionContext::default(),
        TemporalRead::current(GraphId::new(1), ValidTime::from_micros(5)),
        None,
        1,
    );

    assert_eq!(adapter.candidate_scan_calls(), 0);
    let first = block_on(source.next()).unwrap().unwrap();
    assert_eq!(first.batch().rows().len(), 1);
    assert!(first.has_more());
    assert_eq!(adapter.candidate_scan_calls(), 1);
    let second = block_on(source.next()).unwrap().unwrap();
    assert_eq!(second.batch().rows().len(), 1);
    assert!(second.has_more());
    assert_eq!(adapter.candidate_scan_calls(), 2);
}

#[test]
fn candidate_morsel_limit_stops_without_an_extra_page_pull() {
    let adapter = CountingAdapter::new();
    let store = TemporalStore::new(adapter.clone());
    for id in 1..=3 {
        seed(&store, id, 11, "candidate");
    }
    let schema = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "n",
        ValueType::Node,
        false,
    )])
    .unwrap();
    let mut builder = PhysicalPlanBuilder::new(PhysicalPlanHeader::new(1, 1, 1, [43; 32]).unwrap());
    let root = builder
        .add_fragment_with_access(
            Placement::Shard(0),
            vec![
                PhysicalOperator::NodeScan {
                    binding: SlotId::new(0),
                    labels: vec![11],
                    output: schema.clone(),
                },
                PhysicalOperator::Limit {
                    count: ScalarExpr::Literal(GraphValue::Integer(1)),
                },
            ],
            vec![
                PhysicalAccess::Primitive {
                    primitive: PrimitiveKind::CandidateScan,
                    guarantee: AccessGuarantee::Candidate,
                    residual: ResidualPolicy::Evaluate,
                    constraints: Vec::new(),
                },
                PhysicalAccess::Generic,
            ],
            schema,
            MemoryBudget::new(1 << 20, 1 << 20).unwrap(),
        )
        .unwrap();
    let plan = builder.finish(root).unwrap();
    let executor = TemporalBatchExecutor::new(store);
    let mut source = executor.open_fragment_morsels(
        &plan.fragments()[0],
        &ExecutionContext::default(),
        TemporalRead::current(GraphId::new(1), ValidTime::from_micros(5)),
        None,
        1,
    );

    let only = block_on(source.next()).unwrap().unwrap();
    assert_eq!(only.batch().rows().len(), 1);
    assert!(!only.has_more());
    assert_eq!(adapter.candidate_scan_calls(), 1);
    assert!(block_on(source.next()).unwrap().is_none());
    assert_eq!(adapter.candidate_scan_calls(), 1);
}

#[test]
fn planned_candidate_scan_passes_constraints_and_valid_time_to_the_snapshot() {
    let adapter = CountingAdapter::new();
    let store = TemporalStore::new(adapter.clone());
    seed(&store, 1, 11, "candidate");
    seed(&store, 2, 11, "other");
    let schema = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "n",
        ValueType::Node,
        false,
    )])
    .unwrap();
    let predicate = ScalarExpr::Equal(
        Box::new(ScalarExpr::Property {
            value: Box::new(ScalarExpr::Slot(SlotId::new(0))),
            property_id: 1,
        }),
        Box::new(ScalarExpr::Literal(GraphValue::String("candidate".into()))),
    );
    let constraint = PhysicalPropertyConstraint::new(
        1,
        PhysicalComparisonOperator::Equal,
        GraphValue::String("candidate".into()),
    );
    let mut builder = PhysicalPlanBuilder::new(PhysicalPlanHeader::new(1, 1, 1, [29; 32]).unwrap());
    let root = builder
        .add_fragment_with_access(
            Placement::Shard(0),
            vec![
                PhysicalOperator::NodeScan {
                    binding: SlotId::new(0),
                    labels: Vec::new(),
                    output: schema.clone(),
                },
                PhysicalOperator::Filter(predicate),
            ],
            vec![
                PhysicalAccess::Primitive {
                    primitive: PrimitiveKind::CandidateScan,
                    guarantee: AccessGuarantee::Candidate,
                    residual: ResidualPolicy::Evaluate,
                    constraints: vec![constraint],
                },
                PhysicalAccess::Generic,
            ],
            schema,
            MemoryBudget::new(1 << 20, 1 << 20).unwrap(),
        )
        .unwrap();
    let plan = builder.finish(root).unwrap();
    let valid_time = ValidTime::from_micros(5);

    let batches = block_on(TemporalBatchExecutor::new(store).execute_fragment(
        &plan.fragments()[0],
        &ExecutionContext::default(),
        TemporalRead::current(GraphId::new(1), valid_time),
    ))
    .unwrap();

    assert_eq!(adapter.candidate_valid_times(), vec![valid_time]);
    assert_eq!(adapter.candidate_constraints().len(), 1);
    assert_eq!(adapter.candidate_constraints()[0].len(), 1);
    assert_eq!(adapter.candidate_constraints()[0][0].property().value(), 1);
    assert_eq!(
        adapter.candidate_constraints()[0][0].operator(),
        storage_api::ComparisonOperator::Equal
    );
    assert_eq!(
        adapter.candidate_constraints()[0][0].value(),
        &GraphValue::String("candidate".into())
    );
    assert_eq!(
        batches
            .iter()
            .map(|batch| batch.rows().len())
            .sum::<usize>(),
        1
    );
}

#[test]
fn planned_candidate_scan_opens_the_snapshot_from_its_bound_owner() {
    let owner = Arc::new(CountingAdapter::new());
    let adapter = BindingOnlyAdapter {
        owner: Arc::clone(&owner),
        generation: 7,
    };
    let store = TemporalStore::new(adapter);
    seed(&store, 1, 11, "candidate");
    let schema = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "n",
        ValueType::Node,
        false,
    )])
    .unwrap();
    let mut builder = PhysicalPlanBuilder::new(PhysicalPlanHeader::new(1, 1, 1, [31; 32]).unwrap());
    let root = builder
        .add_fragment_with_access(
            Placement::Shard(0),
            vec![PhysicalOperator::NodeScan {
                binding: SlotId::new(0),
                labels: vec![11],
                output: schema.clone(),
            }],
            vec![PhysicalAccess::Primitive {
                primitive: PrimitiveKind::CandidateScan,
                guarantee: AccessGuarantee::Candidate,
                residual: ResidualPolicy::Evaluate,
                constraints: Vec::new(),
            }],
            schema,
            MemoryBudget::new(1 << 20, 1 << 20).unwrap(),
        )
        .unwrap();
    let plan = builder.finish(root).unwrap();

    let batches = block_on(TemporalBatchExecutor::new(store).execute_fragment(
        &plan.fragments()[0],
        &ExecutionContext::default(),
        TemporalRead::current(GraphId::new(1), ValidTime::from_micros(5)),
    ))
    .unwrap();

    assert_eq!(owner.candidate_scan_calls(), 1);
    assert_eq!(
        batches
            .iter()
            .map(|batch| batch.rows().len())
            .sum::<usize>(),
        1
    );
}

#[test]
fn planned_candidate_scan_rejects_a_generation_drift_at_snapshot_binding() {
    let owner = Arc::new(CountingAdapter::new());
    let store = TemporalStore::new(BindingOnlyAdapter {
        owner,
        generation: 7,
    });
    seed(&store, 1, 11, "candidate");
    let schema = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "n",
        ValueType::Node,
        false,
    )])
    .unwrap();
    let mut builder = PhysicalPlanBuilder::new(PhysicalPlanHeader::new(1, 1, 1, [37; 32]).unwrap());
    let root = builder
        .add_fragment_with_access(
            Placement::Shard(0),
            vec![PhysicalOperator::NodeScan {
                binding: SlotId::new(0),
                labels: vec![11],
                output: schema.clone(),
            }],
            vec![PhysicalAccess::Primitive {
                primitive: PrimitiveKind::CandidateScan,
                guarantee: AccessGuarantee::Candidate,
                residual: ResidualPolicy::Evaluate,
                constraints: Vec::new(),
            }],
            schema,
            MemoryBudget::new(1 << 20, 1 << 20).unwrap(),
        )
        .unwrap();
    let plan = builder.finish(root).unwrap();

    let error = block_on(
        TemporalBatchExecutor::new(store).execute_fragment_with_expected_capability_generation(
            &plan.fragments()[0],
            &ExecutionContext::default(),
            TemporalRead::current(GraphId::new(1), ValidTime::from_micros(5)),
            Some(6),
        ),
    )
    .unwrap_err();

    assert!(matches!(
        error,
        TemporalExecutionError::Runtime(RuntimeError::CapabilityGenerationMismatch)
    ));
}

#[test]
fn planned_candidate_scan_rejects_an_as_of_transaction_read() {
    let schema = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "n",
        ValueType::Node,
        false,
    )])
    .unwrap();
    let mut builder = PhysicalPlanBuilder::new(PhysicalPlanHeader::new(1, 1, 1, [29; 32]).unwrap());
    let root = builder
        .add_fragment_with_access(
            Placement::Shard(0),
            vec![PhysicalOperator::NodeScan {
                binding: SlotId::new(0),
                labels: Vec::new(),
                output: schema.clone(),
            }],
            vec![PhysicalAccess::Primitive {
                primitive: PrimitiveKind::CandidateScan,
                guarantee: AccessGuarantee::Candidate,
                residual: ResidualPolicy::Evaluate,
                constraints: Vec::new(),
            }],
            schema,
            MemoryBudget::new(1 << 20, 1 << 20).unwrap(),
        )
        .unwrap();
    let plan = builder.finish(root).unwrap();

    let error = block_on(
        TemporalBatchExecutor::new(TemporalStore::new(MemoryAdapter::new())).execute_fragment(
            &plan.fragments()[0],
            &ExecutionContext::default(),
            TemporalRead::as_of(GraphId::new(1), ValidTime::from_micros(5), tx(10)),
        ),
    )
    .unwrap_err();

    assert!(matches!(
        error,
        query_executor::TemporalExecutionError::Runtime(
            query_executor::RuntimeError::InvalidPhysicalPlan
        )
    ));
}

#[test]
fn current_candidate_scan_keeps_property_filter_as_a_residual() {
    let adapter = CountingAdapter::new();
    let store = TemporalStore::new(adapter.clone());
    seed(&store, 1, 11, "keep");
    seed(&store, 2, 11, "drop");
    let schema = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "n",
        ValueType::Node,
        false,
    )])
    .unwrap();
    let mut builder = PhysicalPlanBuilder::new(PhysicalPlanHeader::new(1, 1, 1, [31; 32]).unwrap());
    let root = builder
        .add_fragment_with_access(
            Placement::Shard(0),
            vec![
                PhysicalOperator::NodeScan {
                    binding: SlotId::new(0),
                    labels: vec![11],
                    output: schema.clone(),
                },
                PhysicalOperator::Filter(ScalarExpr::Equal(
                    Box::new(ScalarExpr::Property {
                        value: Box::new(ScalarExpr::Slot(SlotId::new(0))),
                        property_id: 1,
                    }),
                    Box::new(ScalarExpr::Literal(GraphValue::String("keep".into()))),
                )),
            ],
            vec![
                PhysicalAccess::Primitive {
                    primitive: PrimitiveKind::CandidateScan,
                    guarantee: AccessGuarantee::Candidate,
                    residual: ResidualPolicy::Evaluate,
                    constraints: Vec::new(),
                },
                PhysicalAccess::Generic,
            ],
            schema,
            MemoryBudget::new(1 << 20, 1 << 20).unwrap(),
        )
        .unwrap();
    let plan = builder.finish(root).unwrap();

    let batches = block_on(TemporalBatchExecutor::new(store).execute_fragment(
        &plan.fragments()[0],
        &ExecutionContext::default(),
        TemporalRead::current(GraphId::new(1), ValidTime::from_micros(5)),
    ))
    .unwrap();

    assert_eq!(adapter.candidate_scan_calls(), 1);
    assert_eq!(
        batches
            .iter()
            .map(|batch| batch.rows().len())
            .sum::<usize>(),
        1
    );
}

#[test]
fn change_source_fragment_uses_event_scan_instead_of_state_scan() {
    let store = TemporalStore::new(MemoryAdapter::new());
    let element = ElementRef::vertex(GraphId::new(1), PartitionId::new(0), ElementId::new(91));
    block_on(
        store.commit_vertex(
            CommitContext::new(0, 1, 1, tx(0), tx(10)),
            VertexMutation::put(
                element,
                LabelId::new(11),
                Interval::new(ValidTime::from_micros(1), Some(ValidTime::from_micros(2))).unwrap(),
                CanonicalElement::new(1, BTreeMap::new()),
            )
            .unwrap(),
        ),
    )
    .unwrap();
    block_on(
        store.commit_vertex(
            CommitContext::new(0, 2, 2, tx(10), tx(20)),
            VertexMutation::delete(
                element,
                LabelId::new(11),
                Interval::new(ValidTime::from_micros(1), Some(ValidTime::from_micros(2))).unwrap(),
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
    let mut builder = PhysicalPlanBuilder::new(PhysicalPlanHeader::new(1, 1, 1, [4; 32]).unwrap());
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
            schema.clone(),
            MemoryBudget::new(1 << 20, 1 << 20).unwrap(),
        )
        .unwrap();
    let plan = builder.finish(root).unwrap();
    let batch = block_on(
        TemporalBatchExecutor::new(store).execute_change_source_fragment(
            &plan.fragments()[0],
            &ChangeScanScope::valid(
                GraphId::new(1),
                ValidTime::from_micros(1),
                ValidTime::from_micros(2),
                tx(20),
            )
            .unwrap(),
            16,
        ),
    )
    .unwrap();
    assert_eq!(batch.events.len(), 2);
    assert_eq!(
        batch.events[1].operation(),
        temporal_storage::TemporalEventOperation::Delete
    );

    assert!(
        block_on(
            TemporalBatchExecutor::new(TemporalStore::new(MemoryAdapter::new()))
                .execute_change_source_fragment(
                    &plan.fragments()[0],
                    &ChangeScanScope::system(GraphId::new(1), tx(1), tx(2), tx(20)).unwrap(),
                    16,
                ),
        )
        .is_err()
    );
}

#[test]
fn change_relationship_scan_preserves_immutable_endpoint_metadata() {
    let store = TemporalStore::new(MemoryAdapter::new());
    seed(&store, 1, 11, "source");
    seed(&store, 2, 12, "destination");
    seed_edge(&store);
    let schema = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "r",
        ValueType::Relationship,
        false,
    )])
    .unwrap();
    let scope = ChangeScanScope::system(GraphId::new(1), tx(150), tx(250), tx(250)).unwrap();

    let ChangeEventBatch {
        columns,
        events,
        applied_log_index,
    } = block_on(TemporalBatchExecutor::new(store).scan_change_relationships(
        &scope,
        &[99],
        &schema,
        16,
        1 << 20,
    ))
    .unwrap();
    assert_eq!(applied_log_index, 3);
    assert_eq!(events.len(), 1);
    let Some(row) = columns.row(0) else {
        panic!("event row")
    };
    let RuntimeValue::Relationship(edge) = &row[0] else {
        panic!("relationship")
    };
    assert_eq!(edge.source_ref().id(), ElementId::new(1));
    assert_eq!(edge.destination_ref().id(), ElementId::new(2));
    assert_eq!(edge.edge_type(), EdgeTypeId::new(99));
}

#[test]
fn interval_relationship_scan_preserves_the_edge_segment_region() {
    let store = TemporalStore::new(MemoryAdapter::new());
    seed(&store, 1, 11, "source");
    seed(&store, 2, 12, "destination");
    block_on(
        store.commit_edge(
            CommitContext::new(0, 3, 3, tx(100), tx(200)),
            EdgeMutation::put(
                ElementRef::edge(GraphId::new(1), PartitionId::new(0), ElementId::new(55)),
                EdgeTypeId::new(99),
                ElementId::new(1),
                ElementId::new(2),
                Interval::new(ValidTime::from_micros(4), Some(ValidTime::from_micros(6)))
                    .expect("edge interval"),
                CanonicalElement::new(1, BTreeMap::new()),
            )
            .expect("edge mutation"),
        ),
    )
    .expect("commit edge");

    let rows = block_on(
        TemporalBatchExecutor::new(store).scan_edge_rows_interval_as_of(
            GraphId::new(1),
            &[99],
            Interval::new(ValidTime::from_micros(1), Some(ValidTime::from_micros(10)))
                .expect("query window"),
            tx(250),
        ),
    )
    .expect("interval scan");

    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].region().valid(),
        Interval::new(ValidTime::from_micros(4), Some(ValidTime::from_micros(6)))
            .expect("expected segment")
    );
    let RuntimeValue::Relationship(relationship) = &rows[0].values()[0] else {
        panic!("expected relationship");
    };
    assert_eq!(relationship.element().id(), ElementId::new(55));
    assert_eq!(relationship.edge_type(), EdgeTypeId::new(99));
}

#[test]
fn interval_expand_intersects_source_edge_and_destination_regions() {
    let store = TemporalStore::new(MemoryAdapter::new());
    seed(&store, 1, 11, "source");
    seed(&store, 2, 12, "destination");
    block_on(
        store.commit_edge(
            CommitContext::new(0, 3, 3, tx(100), tx(200)),
            EdgeMutation::put(
                ElementRef::edge(GraphId::new(1), PartitionId::new(0), ElementId::new(56)),
                EdgeTypeId::new(99),
                ElementId::new(1),
                ElementId::new(2),
                Interval::new(ValidTime::from_micros(4), Some(ValidTime::from_micros(6)))
                    .expect("edge interval"),
                CanonicalElement::new(1, BTreeMap::new()),
            )
            .expect("edge mutation"),
        ),
    )
    .expect("commit edge");
    let executor = TemporalBatchExecutor::new(store);
    let window = Interval::new(ValidTime::from_micros(1), Some(ValidTime::from_micros(10)))
        .expect("query window");
    let sources =
        block_on(executor.scan_vertex_rows_interval_as_of(GraphId::new(1), &[11], window, tx(250)))
            .expect("source scan");
    let rows = block_on(executor.expand_interval_rows(
        &sources,
        GraphId::new(1),
        true,
        &[99],
        window,
        tx(250),
    ))
    .expect("interval expand");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values().len(), 3);
    assert_eq!(
        rows[0].region().valid(),
        Interval::new(ValidTime::from_micros(4), Some(ValidTime::from_micros(6)))
            .expect("expected intersection")
    );
}

#[test]
fn interval_fragment_executes_scan_and_expand_with_temporal_rows() {
    let store = TemporalStore::new(MemoryAdapter::new());
    seed(&store, 1, 11, "source");
    seed(&store, 2, 12, "destination");
    seed_edge(&store);
    let scan_schema = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "a",
        ValueType::Node,
        false,
    )])
    .expect("scan schema");
    let expand_schema = RowSchema::new(vec![
        Column::new(SlotId::new(0), "a", ValueType::Node, false),
        Column::new(SlotId::new(1), "r", ValueType::Relationship, false),
        Column::new(SlotId::new(2), "b", ValueType::Node, false),
    ])
    .expect("expand schema");
    let mut builder =
        PhysicalPlanBuilder::new(PhysicalPlanHeader::new(1, 1, 1, [10; 32]).expect("header"));
    let root = builder
        .add_fragment(
            Placement::Shard(0),
            vec![
                PhysicalOperator::NodeScan {
                    binding: SlotId::new(0),
                    labels: vec![11],
                    output: scan_schema,
                },
                PhysicalOperator::Expand {
                    source: SlotId::new(0),
                    relationship: SlotId::new(1),
                    destination: SlotId::new(2),
                    outgoing: true,
                    types: vec![99],
                    output: expand_schema.clone(),
                },
            ],
            expand_schema,
            MemoryBudget::new(1 << 20, 1 << 20).expect("budget"),
        )
        .expect("fragment");
    let plan = builder.finish(root).expect("plan");
    let rows = block_on(
        TemporalBatchExecutor::new(store).execute_interval_fragment_rows(
            &plan.fragments()[0],
            &ExecutionContext::default(),
            GraphId::new(1),
            Interval::new(ValidTime::from_micros(1), Some(ValidTime::from_micros(10)))
                .expect("window"),
            tx(250),
        ),
    )
    .expect("interval fragment");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values().len(), 3);
    assert_eq!(
        rows[0].region().valid(),
        Interval::new(ValidTime::from_micros(1), Some(ValidTime::from_micros(10)))
            .expect("full valid region")
    );
}

#[test]
fn interval_expand_accepts_a_source_slot_before_other_bindings() {
    let store = TemporalStore::new(MemoryAdapter::new());
    seed(&store, 1, 11, "source");
    seed(&store, 2, 12, "destination");
    seed_edge(&store);
    let scan_schema = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "a",
        ValueType::Node,
        false,
    )])
    .expect("scan schema");
    let unwind_schema = RowSchema::new(vec![
        Column::new(SlotId::new(0), "a", ValueType::Node, false),
        Column::new(SlotId::new(1), "marker", ValueType::Integer, false),
    ])
    .expect("unwind schema");
    let expand_schema = RowSchema::new(vec![
        Column::new(SlotId::new(0), "a", ValueType::Node, false),
        Column::new(SlotId::new(2), "r", ValueType::Relationship, false),
        Column::new(SlotId::new(3), "b", ValueType::Node, false),
        Column::new(SlotId::new(1), "marker", ValueType::Integer, false),
    ])
    .expect("expand schema");
    let mut builder =
        PhysicalPlanBuilder::new(PhysicalPlanHeader::new(1, 1, 1, [12; 32]).expect("header"));
    let root = builder
        .add_fragment(
            Placement::Shard(0),
            vec![
                PhysicalOperator::NodeScan {
                    binding: SlotId::new(0),
                    labels: vec![11],
                    output: scan_schema,
                },
                PhysicalOperator::Unwind {
                    expression: ScalarExpr::Literal(GraphValue::List(vec![GraphValue::Integer(7)])),
                    binding: SlotId::new(1),
                    output: unwind_schema,
                },
                PhysicalOperator::Expand {
                    source: SlotId::new(0),
                    relationship: SlotId::new(2),
                    destination: SlotId::new(3),
                    outgoing: true,
                    types: vec![99],
                    output: expand_schema.clone(),
                },
            ],
            expand_schema,
            MemoryBudget::new(1 << 20, 1 << 20).expect("budget"),
        )
        .expect("fragment");
    let plan = builder.finish(root).expect("plan");

    let rows = block_on(
        TemporalBatchExecutor::new(store).execute_interval_fragment_rows(
            &plan.fragments()[0],
            &ExecutionContext::default(),
            GraphId::new(1),
            Interval::new(ValidTime::from_micros(1), Some(ValidTime::from_micros(10)))
                .expect("window"),
            tx(250),
        ),
    )
    .expect("interval fragment");

    assert_eq!(rows.len(), 1);
    assert!(matches!(rows[0].values()[0], RuntimeValue::Node(_)));
    assert!(matches!(rows[0].values()[1], RuntimeValue::Relationship(_)));
    assert!(matches!(rows[0].values()[2], RuntimeValue::Node(_)));
    assert_eq!(rows[0].values()[3], RuntimeValue::Integer(7));
}

#[test]
fn interval_fragment_preserves_regions_through_filter_and_project() {
    let store = TemporalStore::new(MemoryAdapter::new());
    seed(&store, 1, 11, "source");
    let schema = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "a",
        ValueType::Node,
        false,
    )])
    .expect("schema");
    let mut builder =
        PhysicalPlanBuilder::new(PhysicalPlanHeader::new(1, 1, 1, [11; 32]).expect("header"));
    let root = builder
        .add_fragment(
            Placement::Shard(0),
            vec![
                PhysicalOperator::NodeScan {
                    binding: SlotId::new(0),
                    labels: vec![11],
                    output: schema.clone(),
                },
                PhysicalOperator::Filter(ScalarExpr::Literal(GraphValue::Boolean(true))),
                PhysicalOperator::Project {
                    expressions: vec![(SlotId::new(0), ScalarExpr::Slot(SlotId::new(0)))],
                    output: schema.clone(),
                },
            ],
            schema,
            MemoryBudget::new(1 << 20, 1 << 20).expect("budget"),
        )
        .expect("fragment");
    let plan = builder.finish(root).expect("plan");
    let rows = block_on(
        TemporalBatchExecutor::new(store).execute_interval_fragment_rows(
            &plan.fragments()[0],
            &ExecutionContext::default(),
            GraphId::new(1),
            Interval::new(ValidTime::from_micros(1), Some(ValidTime::from_micros(10)))
                .expect("window"),
            tx(150),
        ),
    )
    .expect("interval fragment");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values().len(), 1);
    assert_eq!(
        rows[0].region().valid(),
        Interval::new(ValidTime::from_micros(1), Some(ValidTime::from_micros(10))).expect("region")
    );
}

#[test]
fn expand_materializes_typed_relationship_and_destination_at_one_snapshot() {
    let store = TemporalStore::new(MemoryAdapter::new());
    seed(&store, 1, 11, "source");
    seed(&store, 2, 12, "destination");
    seed_edge(&store);
    let scan_schema = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "a",
        ValueType::Node,
        false,
    )])
    .expect("scan schema");
    let expand_schema = RowSchema::new(vec![
        Column::new(SlotId::new(0), "a", ValueType::Node, false),
        Column::new(SlotId::new(1), "r", ValueType::Relationship, false),
        Column::new(SlotId::new(2), "b", ValueType::Node, false),
    ])
    .expect("expand schema");
    let output_schema = RowSchema::new(vec![
        Column::new(SlotId::new(1), "r", ValueType::Relationship, false),
        Column::new(SlotId::new(2), "b", ValueType::Node, false),
    ])
    .expect("output schema");
    let mut builder =
        PhysicalPlanBuilder::new(PhysicalPlanHeader::new(1, 1, 1, [8; 32]).expect("header"));
    let root = builder
        .add_fragment(
            Placement::Shard(0),
            vec![
                PhysicalOperator::NodeScan {
                    binding: SlotId::new(0),
                    labels: vec![11],
                    output: scan_schema,
                },
                PhysicalOperator::Expand {
                    source: SlotId::new(0),
                    relationship: SlotId::new(1),
                    destination: SlotId::new(2),
                    outgoing: true,
                    types: vec![99],
                    output: expand_schema,
                },
                PhysicalOperator::Project {
                    expressions: vec![
                        (SlotId::new(1), ScalarExpr::Slot(SlotId::new(1))),
                        (SlotId::new(2), ScalarExpr::Slot(SlotId::new(2))),
                    ],
                    output: output_schema.clone(),
                },
            ],
            output_schema,
            MemoryBudget::new(1 << 20, 1 << 20).expect("budget"),
        )
        .expect("fragment");
    let plan = builder.finish(root).expect("plan");

    let batches = block_on(TemporalBatchExecutor::new(store).execute_fragment(
        &plan.fragments()[0],
        &ExecutionContext::default(),
        TemporalRead::as_of(GraphId::new(1), ValidTime::from_micros(5), tx(250)),
    ))
    .expect("execute");

    assert_eq!(batches[0].rows().len(), 1);
    let RuntimeValue::Relationship(relationship) = &batches[0].rows()[0][0] else {
        panic!("expected relationship");
    };
    let RuntimeValue::Node(destination) = &batches[0].rows()[0][1] else {
        panic!("expected destination");
    };
    assert_eq!(relationship.element().id(), ElementId::new(10));
    assert_eq!(relationship.edge_type(), EdgeTypeId::new(99));
    assert_eq!(destination.element().id(), ElementId::new(2));
}

#[test]
fn expand_batches_target_reads_without_changing_input_order() {
    let adapter = CountingAdapter::new();
    let store = TemporalStore::new(adapter.clone());
    for (log_index, (id, label)) in [(1, (1_u128, 11)), (2, (2, 12)), (3, (7, 12)), (4, (9, 12))] {
        seed_with_log(&store, id, label, "vertex", log_index);
    }
    for (log_index, (edge_id, destination)) in [(5, (1_u128, 9_u128)), (6, (2, 2)), (7, (3, 7))] {
        seed_edge_to(&store, edge_id, destination, log_index);
    }
    adapter.multi_get_sizes.lock().unwrap().clear();

    let scan_schema = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "a",
        ValueType::Node,
        false,
    )])
    .unwrap();
    let output_schema = RowSchema::new(vec![
        Column::new(SlotId::new(1), "r", ValueType::Relationship, false),
        Column::new(SlotId::new(2), "b", ValueType::Node, false),
    ])
    .unwrap();
    let expand_schema = RowSchema::new(vec![
        scan_schema.columns()[0].clone(),
        output_schema.columns()[0].clone(),
        output_schema.columns()[1].clone(),
    ])
    .unwrap();
    let mut builder = PhysicalPlanBuilder::new(PhysicalPlanHeader::new(1, 1, 1, [12; 32]).unwrap());
    let root = builder
        .add_fragment(
            Placement::Shard(0),
            vec![
                PhysicalOperator::NodeScan {
                    binding: SlotId::new(0),
                    labels: vec![11],
                    output: scan_schema,
                },
                PhysicalOperator::Expand {
                    source: SlotId::new(0),
                    relationship: SlotId::new(1),
                    destination: SlotId::new(2),
                    outgoing: true,
                    types: vec![99],
                    output: expand_schema,
                },
                PhysicalOperator::Project {
                    expressions: vec![
                        (SlotId::new(1), ScalarExpr::Slot(SlotId::new(1))),
                        (SlotId::new(2), ScalarExpr::Slot(SlotId::new(2))),
                    ],
                    output: output_schema,
                },
            ],
            RowSchema::new(vec![
                Column::new(SlotId::new(1), "r", ValueType::Relationship, false),
                Column::new(SlotId::new(2), "b", ValueType::Node, false),
            ])
            .unwrap(),
            MemoryBudget::new(1 << 20, 1 << 20).unwrap(),
        )
        .unwrap();
    let plan = builder.finish(root).unwrap();

    let batches = block_on(TemporalBatchExecutor::new(store).execute_fragment(
        &plan.fragments()[0],
        &ExecutionContext::default(),
        TemporalRead::current(GraphId::new(1), ValidTime::from_micros(5)),
    ))
    .unwrap();

    let ids = batches[0]
        .rows()
        .iter()
        .map(|row| match &row[1] {
            RuntimeValue::Node(node) => node.element().id().value(),
            value => panic!("expected node, got {value:?}"),
        })
        .collect::<Vec<_>>();
    assert_eq!(ids, vec![9, 2, 7]);
    assert_eq!(adapter.multi_get_call_sizes(), vec![4, 3, 3]);
}

#[test]
fn relationship_scan_filters_types_at_the_fenced_snapshot() {
    let store = TemporalStore::new(MemoryAdapter::new());
    seed(&store, 1, 11, "source");
    seed(&store, 2, 12, "destination");
    seed_edge(&store);
    let schema = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "r",
        ValueType::Relationship,
        false,
    )])
    .expect("schema");
    let mut builder =
        PhysicalPlanBuilder::new(PhysicalPlanHeader::new(1, 1, 1, [9; 32]).expect("header"));
    let root = builder
        .add_fragment(
            Placement::Shard(0),
            vec![PhysicalOperator::RelationshipScan {
                binding: SlotId::new(0),
                types: vec![99],
                output: schema.clone(),
            }],
            schema,
            MemoryBudget::new(1 << 20, 1 << 20).expect("budget"),
        )
        .expect("fragment");
    let plan = builder.finish(root).expect("plan");

    let batches = block_on(TemporalBatchExecutor::new(store).execute_fragment(
        &plan.fragments()[0],
        &ExecutionContext::default(),
        TemporalRead::as_of(GraphId::new(1), ValidTime::from_micros(5), tx(250)),
    ))
    .expect("execute");

    assert_eq!(batches[0].rows().len(), 1);
    assert!(matches!(
        batches[0].rows()[0][0],
        RuntimeValue::Relationship(_)
    ));
}

fn seed<A: StorageAdapter>(store: &TemporalStore<A>, id: u128, label: u32, name: &str) {
    seed_with_log(store, id, label, name, id as u64);
}

fn seed_with_log<A: StorageAdapter>(
    store: &TemporalStore<A>,
    id: u128,
    label: u32,
    name: &str,
    log_index: u64,
) {
    let element = ElementRef::vertex(GraphId::new(1), PartitionId::new(0), ElementId::new(id));
    let payload = CanonicalElement::new(
        1,
        BTreeMap::from([(1, GraphValue::String(name.to_owned()))]),
    );
    block_on(
        store.commit_vertex(
            CommitContext::new(0, log_index, u128::from(log_index), tx(0), tx(100)),
            VertexMutation::put(
                element,
                LabelId::new(label),
                Interval::new(ValidTime::from_micros(1), None).expect("interval"),
                payload,
            )
            .expect("mutation"),
        ),
    )
    .expect("commit");
}

fn seed_edge(store: &TemporalStore<MemoryAdapter>) {
    let edge = ElementRef::edge(GraphId::new(1), PartitionId::new(0), ElementId::new(10));
    block_on(
        store.commit_edge(
            CommitContext::new(0, 3, 3, tx(100), tx(200)),
            EdgeMutation::put(
                edge,
                EdgeTypeId::new(99),
                ElementId::new(1),
                ElementId::new(2),
                Interval::new(ValidTime::from_micros(1), None).expect("interval"),
                CanonicalElement::new(1, BTreeMap::new()),
            )
            .expect("edge mutation"),
        ),
    )
    .expect("edge commit");
}

fn seed_edge_to(
    store: &TemporalStore<CountingAdapter>,
    edge_id: u128,
    destination: u128,
    log_index: u64,
) {
    block_on(
        store.commit_edge(
            CommitContext::new(0, log_index, u128::from(log_index), tx(100), tx(200)),
            EdgeMutation::put(
                ElementRef::edge(
                    GraphId::new(1),
                    PartitionId::new(0),
                    ElementId::new(edge_id),
                ),
                EdgeTypeId::new(99),
                ElementId::new(1),
                ElementId::new(destination),
                Interval::new(ValidTime::from_micros(1), None).unwrap(),
                CanonicalElement::new(1, BTreeMap::new()),
            )
            .unwrap(),
        ),
    )
    .unwrap();
}

#[derive(Clone)]
struct CountingAdapter {
    inner: Arc<MemoryAdapter>,
    multi_get_sizes: Arc<std::sync::Mutex<Vec<usize>>>,
    change_scan_calls: Arc<std::sync::atomic::AtomicUsize>,
    candidate_scan_calls: Arc<std::sync::atomic::AtomicUsize>,
    candidate_constraints: Arc<std::sync::Mutex<Vec<Vec<storage_api::PropertyConstraint>>>>,
    candidate_valid_times: Arc<std::sync::Mutex<Vec<ValidTime>>>,
}

impl CountingAdapter {
    fn new() -> Self {
        Self {
            inner: Arc::new(MemoryAdapter::new()),
            multi_get_sizes: Arc::new(std::sync::Mutex::new(Vec::new())),
            change_scan_calls: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            candidate_scan_calls: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            candidate_constraints: Arc::new(std::sync::Mutex::new(Vec::new())),
            candidate_valid_times: Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }

    fn multi_get_call_sizes(&self) -> Vec<usize> {
        self.multi_get_sizes.lock().unwrap().clone()
    }

    fn change_scan_calls(&self) -> usize {
        self.change_scan_calls
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    fn candidate_scan_calls(&self) -> usize {
        self.candidate_scan_calls
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    fn candidate_constraints(&self) -> Vec<Vec<storage_api::PropertyConstraint>> {
        self.candidate_constraints.lock().unwrap().clone()
    }

    fn candidate_valid_times(&self) -> Vec<ValidTime> {
        self.candidate_valid_times.lock().unwrap().clone()
    }
}

struct CountingReadSnapshot<'a> {
    inner: Box<dyn ReadSnapshot + 'a>,
    change_scan_calls: Arc<std::sync::atomic::AtomicUsize>,
    candidate_scan_calls: Arc<std::sync::atomic::AtomicUsize>,
    candidate_constraints: Arc<std::sync::Mutex<Vec<Vec<storage_api::PropertyConstraint>>>>,
    candidate_valid_times: Arc<std::sync::Mutex<Vec<ValidTime>>>,
}

impl ReadSnapshot for CountingReadSnapshot<'_> {
    fn applied_log_index(&self) -> u64 {
        self.inner.applied_log_index()
    }

    fn multi_get<'a>(
        &'a self,
        keys: &'a [storage_api::LogicalKey],
    ) -> storage_api::AdapterFuture<'a, Vec<Option<Vec<u8>>>> {
        self.inner.multi_get(keys)
    }

    fn scan<'a>(
        &'a self,
        span: &'a storage_api::KeySpan,
    ) -> storage_api::AdapterFuture<'a, Vec<storage_api::KeyValue>> {
        self.inner.scan(span)
    }

    fn scan_changes<'a>(
        &'a self,
        request: &'a storage_api::ChangeScanRequest,
    ) -> storage_api::AdapterFuture<'a, storage_api::ChangeScanPage> {
        self.change_scan_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.scan_changes(request)
    }

    fn scan_candidates<'a>(
        &'a self,
        request: &'a storage_api::CandidateScanRequest,
    ) -> storage_api::AdapterFuture<'a, storage_api::CandidateScanPage> {
        self.candidate_scan_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.candidate_constraints
            .lock()
            .unwrap()
            .push(request.constraints().to_vec());
        self.candidate_valid_times
            .lock()
            .unwrap()
            .push(request.valid_time());
        self.inner.scan_candidates(request)
    }
}

impl StorageAdapter for CountingAdapter {
    fn capabilities(&self) -> storage_api::AdapterCapabilities {
        self.inner.capabilities()
    }

    fn query_primitive_capabilities(&self) -> storage_api::QueryPrimitiveCapabilities {
        self.inner.query_primitive_capabilities()
    }

    fn apply_committed<'a>(
        &'a self,
        batch: storage_api::CommittedMutationBatch,
    ) -> storage_api::AdapterFuture<'a, storage_api::ApplyReceipt> {
        self.inner.apply_committed(batch)
    }

    fn multi_get<'a>(
        &'a self,
        keys: &'a [storage_api::LogicalKey],
    ) -> storage_api::AdapterFuture<'a, Vec<Option<Vec<u8>>>> {
        self.multi_get_sizes.lock().unwrap().push(keys.len());
        self.inner.multi_get(keys)
    }

    fn scan<'a>(
        &'a self,
        span: &'a storage_api::KeySpan,
    ) -> storage_api::AdapterFuture<'a, Vec<storage_api::KeyValue>> {
        self.inner.scan(span)
    }

    fn begin_read_snapshot<'a>(
        &'a self,
    ) -> storage_api::AdapterFuture<'a, Box<dyn ReadSnapshot + 'a>> {
        Box::pin(async move {
            let inner = self.inner.begin_read_snapshot().await?;
            Ok(Box::new(CountingReadSnapshot {
                inner,
                change_scan_calls: Arc::clone(&self.change_scan_calls),
                candidate_scan_calls: Arc::clone(&self.candidate_scan_calls),
                candidate_constraints: Arc::clone(&self.candidate_constraints),
                candidate_valid_times: Arc::clone(&self.candidate_valid_times),
            }) as Box<dyn ReadSnapshot + 'a>)
        })
    }

    fn applied_log_index(&self) -> Result<u64, storage_api::AdapterError> {
        self.inner.applied_log_index()
    }
}

struct BindingOnlyAdapter {
    owner: Arc<CountingAdapter>,
    generation: u64,
}

impl StorageAdapter for BindingOnlyAdapter {
    fn capabilities(&self) -> storage_api::AdapterCapabilities {
        self.owner.capabilities()
    }

    fn query_primitive_capabilities(&self) -> storage_api::QueryPrimitiveCapabilities {
        self.owner.query_primitive_capabilities()
    }

    fn query_capability_generation(&self) -> u64 {
        self.generation
    }

    fn read_snapshot_binding(
        &self,
    ) -> Result<Option<storage_api::ReadSnapshotBinding>, storage_api::AdapterError> {
        let owner: Arc<dyn StorageAdapter> = self.owner.clone();
        storage_api::ReadSnapshotBinding::new(self.generation, owner).map(Some)
    }

    fn apply_committed<'a>(
        &'a self,
        batch: storage_api::CommittedMutationBatch,
    ) -> storage_api::AdapterFuture<'a, storage_api::ApplyReceipt> {
        self.owner.apply_committed(batch)
    }

    fn multi_get<'a>(
        &'a self,
        keys: &'a [storage_api::LogicalKey],
    ) -> storage_api::AdapterFuture<'a, Vec<Option<Vec<u8>>>> {
        self.owner.multi_get(keys)
    }

    fn scan<'a>(
        &'a self,
        span: &'a storage_api::KeySpan,
    ) -> storage_api::AdapterFuture<'a, Vec<storage_api::KeyValue>> {
        self.owner.scan(span)
    }

    fn applied_log_index(&self) -> Result<u64, storage_api::AdapterError> {
        self.owner.applied_log_index()
    }
}

fn tx(value: i64) -> TransactionTime {
    TransactionTime::new(value, 0)
}

fn function_id(name: &str) -> u32 {
    u32::from_be_bytes(
        blake3::hash(name.as_bytes()).as_bytes()[..4]
            .try_into()
            .unwrap(),
    )
}

fn evaluate_metadata_function(
    name: &str,
    schema: &RowSchema,
    row: &[RuntimeValue],
) -> RuntimeValue {
    BatchExecutor::new()
        .evaluate(
            &ScalarExpr::Function {
                function_id: function_id(name),
                arguments: vec![ScalarExpr::Slot(SlotId::new(0))],
            },
            schema,
            row,
            &ExecutionContext::default(),
        )
        .unwrap()
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
