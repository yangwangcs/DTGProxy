use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::time::{SystemTime, UNIX_EPOCH};

use adapter_memory::MemoryAdapter;
use distributed_query::{
    DistributedCoordinator, FragmentRequest, LocalFragmentWorker, SnapshotToken,
};
use physical_plan::{
    ExchangeKind, JoinKind, MemoryBudget, PhysicalOperator, PhysicalPlanBuilder,
    PhysicalPlanHeader, Placement,
};
use query_executor::{ExecutionContext, RuntimeValue, TemporalBatchExecutor};
use query_optimizer::{DeploymentMode, Optimizer, OptimizerContext};
use temporal_ir::ScalarExpr;
use temporal_ir::{
    Column, LanguageProfile, LogicalOperator, LogicalPlanBuilder, PlanHeader, RowSchema, SlotId,
    TemporalJoinKind, ValueType,
};
use temporal_storage::{
    CommitContext, ElementId, ElementRef, GraphId, LabelId, PartitionId, TemporalStore,
    VertexMutation,
};
use temporal_types::{CanonicalElement, GraphValue, Interval, TransactionTime, ValidTime};

fn shared_header(fingerprint: [u8; 32]) -> PhysicalPlanHeader {
    PhysicalPlanHeader::new(1, 3, 11, fingerprint)
        .unwrap()
        .with_expected_shards(vec![0, 1])
        .unwrap()
}

#[test]
fn coordinator_merges_all_shards_under_bounded_credits() {
    let worker0 = worker(0, 1);
    let worker1 = worker(1, 2);
    let mut coordinator = DistributedCoordinator::new(2 << 20, 8).expect("coordinator");
    coordinator.register(Arc::new(worker1)).expect("worker 1");
    coordinator.register(Arc::new(worker0)).expect("worker 0");
    let schema = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "n",
        ValueType::Node,
        false,
    )])
    .expect("schema");
    let mut builder = PhysicalPlanBuilder::new(shared_header([7; 32]));
    let root = builder
        .add_fragment(
            Placement::AllShards,
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
    let request = FragmentRequest::new(root, snapshot, now_ms() + 10_000, 1 << 20, 1)
        .expect("request")
        .with_expected_shards(vec![0, 1])
        .expect("expected shards");

    let batches = block_on(coordinator.execute(
        &request,
        &plan.fragments()[0],
        ValidTime::from_micros(5),
        &ExecutionContext::default(),
    ))
    .expect("execute");

    let mut ids = batches
        .iter()
        .flat_map(|batch| batch.rows())
        .map(|row| match &row[0] {
            RuntimeValue::Node(node) => node.element().id(),
            value => panic!("expected node, got {value:?}"),
        })
        .collect::<Vec<_>>();
    ids.sort_unstable();
    assert_eq!(ids, vec![ElementId::new(1), ElementId::new(2)]);
}

#[test]
fn coordinator_merges_interval_rows_without_discarding_regions() {
    let mut coordinator = DistributedCoordinator::new(2 << 20, 8).expect("coordinator");
    coordinator
        .register(Arc::new(worker(0, 1)))
        .expect("worker 0");
    coordinator
        .register(Arc::new(worker(1, 2)))
        .expect("worker 1");
    let schema = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "n",
        ValueType::Node,
        false,
    )])
    .expect("schema");
    let mut builder = PhysicalPlanBuilder::new(shared_header([12; 32]));
    let root = builder
        .add_fragment(
            Placement::AllShards,
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
    let request = FragmentRequest::new(
        root,
        SnapshotToken::new(1, 3, 11, tx(150), [5; 32]).expect("snapshot"),
        now_ms() + 10_000,
        1 << 20,
        8,
    )
    .expect("request")
    .with_expected_shards(vec![0, 1])
    .expect("expected shards");

    let rows = block_on(coordinator.execute_interval(
        &request,
        &plan.fragments()[0],
        Interval::new(ValidTime::from_micros(1), Some(ValidTime::from_micros(10))).expect("window"),
        &ExecutionContext::default(),
    ))
    .expect("interval execution");

    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|row| {
        row.region().valid()
            == Interval::new(ValidTime::from_micros(1), Some(ValidTime::from_micros(10)))
                .expect("region")
    }));
}

#[test]
fn coordinator_routes_primary_replica_fragment_to_its_single_shard() {
    let mut coordinator = DistributedCoordinator::new(1 << 20, 4).expect("coordinator");
    coordinator
        .register(Arc::new(worker(0, 1)))
        .expect("worker");
    let schema = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "n",
        ValueType::Node,
        false,
    )])
    .expect("schema");
    let mut builder =
        PhysicalPlanBuilder::new(PhysicalPlanHeader::new(1, 3, 11, [6; 32]).expect("header"));
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
    let request = FragmentRequest::new(
        root,
        SnapshotToken::new(1, 3, 11, tx(150), [5; 32]).expect("snapshot"),
        now_ms() + 10_000,
        1 << 20,
        8,
    )
    .expect("request");

    let batches = block_on(coordinator.execute(
        &request,
        &plan.fragments()[0],
        ValidTime::from_micros(5),
        &ExecutionContext::default(),
    ))
    .expect("execute");

    assert_eq!(
        batches
            .iter()
            .map(|batch| batch.rows().len())
            .sum::<usize>(),
        1
    );
}

#[test]
fn coordinator_executes_gather_exchange_and_root_projection() {
    let mut coordinator = DistributedCoordinator::new(2 << 20, 8).expect("coordinator");
    coordinator
        .register(Arc::new(worker(0, 1)))
        .expect("worker 0");
    coordinator
        .register(Arc::new(worker(1, 2)))
        .expect("worker 1");
    let schema = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "n",
        ValueType::Node,
        false,
    )])
    .expect("schema");
    let mut builder = PhysicalPlanBuilder::new(shared_header([5; 32]));
    let shard = builder
        .add_fragment(
            Placement::AllShards,
            vec![PhysicalOperator::NodeScan {
                binding: SlotId::new(0),
                labels: vec![11],
                output: schema.clone(),
            }],
            schema.clone(),
            MemoryBudget::new(1 << 20, 1 << 20).expect("budget"),
        )
        .expect("shard fragment");
    let root = builder
        .add_fragment(
            Placement::Coordinator,
            vec![PhysicalOperator::Project {
                expressions: vec![(SlotId::new(0), ScalarExpr::Slot(SlotId::new(0)))],
                output: schema.clone(),
            }],
            schema.clone(),
            MemoryBudget::new(1 << 20, 1 << 20).expect("budget"),
        )
        .expect("root fragment");
    builder
        .add_exchange(shard, root, ExchangeKind::Gather, schema, 8)
        .expect("exchange");
    let plan = builder.finish(root).expect("plan");
    let snapshot = SnapshotToken::new(1, 3, 11, tx(150), [5; 32]).expect("snapshot");

    let batches = block_on(coordinator.execute_plan(
        &plan,
        snapshot,
        ValidTime::from_micros(5),
        now_ms() + 10_000,
        1,
        &ExecutionContext::default(),
    ))
    .expect("execute plan");

    assert_eq!(
        batches
            .iter()
            .map(|batch| batch.rows().len())
            .sum::<usize>(),
        2
    );
}

#[test]
fn coordinator_executes_a_source_free_root_fragment_once() {
    let mut coordinator = DistributedCoordinator::new(2 << 20, 8).expect("coordinator");
    coordinator
        .register(Arc::new(worker(0, 1)))
        .expect("worker 0");
    coordinator
        .register(Arc::new(worker(1, 2)))
        .expect("worker 1");
    let schema = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "item",
        ValueType::Any,
        false,
    )])
    .expect("schema");
    let mut builder =
        PhysicalPlanBuilder::new(PhysicalPlanHeader::new(1, 3, 11, [15; 32]).expect("header"));
    let root = builder
        .add_fragment(
            Placement::Coordinator,
            vec![
                PhysicalOperator::Argument {
                    output: RowSchema::empty(),
                },
                PhysicalOperator::Unwind {
                    expression: ScalarExpr::Literal(GraphValue::List(vec![
                        GraphValue::Integer(1),
                        GraphValue::Integer(2),
                        GraphValue::Integer(3),
                    ])),
                    binding: SlotId::new(0),
                    output: schema.clone(),
                },
            ],
            schema,
            MemoryBudget::new(1 << 20, 1 << 20).expect("budget"),
        )
        .expect("root fragment");
    let plan = builder.finish(root).expect("plan");

    let batches = block_on(coordinator.execute_plan(
        &plan,
        SnapshotToken::new(1, 3, 11, tx(150), [5; 32]).expect("snapshot"),
        ValidTime::from_micros(5),
        now_ms() + 10_000,
        8,
        &ExecutionContext::default(),
    ))
    .expect("execute source-free plan");

    assert_eq!(
        batches
            .iter()
            .flat_map(|batch| batch.rows())
            .map(|row| row[0].clone())
            .collect::<Vec<_>>(),
        vec![
            RuntimeValue::Integer(1),
            RuntimeValue::Integer(2),
            RuntimeValue::Integer(3),
        ]
    );
}

#[test]
fn coordinator_executes_two_input_union_with_global_distinct() {
    let mut coordinator = DistributedCoordinator::new(2 << 20, 8).expect("coordinator");
    coordinator
        .register(Arc::new(worker(0, 1)))
        .expect("worker 0");
    coordinator
        .register(Arc::new(worker(1, 2)))
        .expect("worker 1");
    let schema = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "n",
        ValueType::Node,
        false,
    )])
    .expect("schema");
    let mut builder = PhysicalPlanBuilder::new(shared_header([4; 32]));
    let left = builder
        .add_fragment(
            Placement::AllShards,
            vec![PhysicalOperator::NodeScan {
                binding: SlotId::new(0),
                labels: vec![11],
                output: schema.clone(),
            }],
            schema.clone(),
            MemoryBudget::new(1 << 20, 1 << 20).unwrap(),
        )
        .unwrap();
    let right = builder
        .add_fragment(
            Placement::AllShards,
            vec![PhysicalOperator::NodeScan {
                binding: SlotId::new(0),
                labels: vec![11],
                output: schema.clone(),
            }],
            schema.clone(),
            MemoryBudget::new(1 << 20, 1 << 20).unwrap(),
        )
        .unwrap();
    let root = builder
        .add_fragment(
            Placement::Coordinator,
            vec![PhysicalOperator::Union { all: false }],
            schema.clone(),
            MemoryBudget::new(1 << 20, 1 << 20).unwrap(),
        )
        .unwrap();
    builder
        .add_exchange(left, root, ExchangeKind::Gather, schema.clone(), 8)
        .unwrap();
    builder
        .add_exchange(right, root, ExchangeKind::Gather, schema, 8)
        .unwrap();
    let plan = builder.finish(root).unwrap();
    let batches = block_on(coordinator.execute_plan(
        &plan,
        SnapshotToken::new(1, 3, 11, tx(150), [5; 32]).unwrap(),
        ValidTime::from_micros(5),
        now_ms() + 10_000,
        8,
        &ExecutionContext::default(),
    ))
    .unwrap();
    assert_eq!(
        batches
            .iter()
            .map(|batch| batch.rows().len())
            .sum::<usize>(),
        2
    );
}

#[test]
fn coordinator_executes_two_input_interval_hash_join() {
    let mut coordinator = DistributedCoordinator::new(2 << 20, 8).expect("coordinator");
    coordinator
        .register(Arc::new(worker(0, 1)))
        .expect("worker 0");
    coordinator
        .register(Arc::new(worker(1, 2)))
        .expect("worker 1");
    let left_schema = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "left",
        ValueType::Node,
        false,
    )])
    .expect("left schema");
    let right_schema = RowSchema::new(vec![Column::new(
        SlotId::new(1),
        "right",
        ValueType::Node,
        false,
    )])
    .expect("right schema");
    let output = RowSchema::new(vec![
        Column::new(SlotId::new(0), "left", ValueType::Node, false),
        Column::new(SlotId::new(1), "right", ValueType::Node, false),
    ])
    .expect("output schema");
    let mut builder = PhysicalPlanBuilder::new(shared_header([3; 32]));
    let left = builder
        .add_fragment(
            Placement::AllShards,
            vec![PhysicalOperator::NodeScan {
                binding: SlotId::new(0),
                labels: vec![11],
                output: left_schema.clone(),
            }],
            left_schema.clone(),
            MemoryBudget::new(1 << 20, 1 << 20).expect("budget"),
        )
        .expect("left");
    let right = builder
        .add_fragment(
            Placement::AllShards,
            vec![PhysicalOperator::NodeScan {
                binding: SlotId::new(1),
                labels: vec![11],
                output: right_schema.clone(),
            }],
            right_schema.clone(),
            MemoryBudget::new(1 << 20, 1 << 20).expect("budget"),
        )
        .expect("right");
    let root = builder
        .add_fragment(
            Placement::Coordinator,
            vec![PhysicalOperator::HashJoin {
                kind: JoinKind::Inner,
                keys: Vec::new(),
            }],
            output,
            MemoryBudget::new(1 << 20, 1 << 20).expect("budget"),
        )
        .expect("root");
    builder
        .add_exchange(left, root, ExchangeKind::Gather, left_schema, 8)
        .expect("left exchange");
    builder
        .add_exchange(right, root, ExchangeKind::Gather, right_schema, 8)
        .expect("right exchange");
    let plan = builder.finish(root).expect("plan");

    let rows = block_on(coordinator.execute_interval_plan(
        &plan,
        SnapshotToken::new(1, 3, 11, tx(150), [5; 32]).expect("snapshot"),
        Interval::new(ValidTime::from_micros(1), Some(ValidTime::from_micros(10))).expect("window"),
        now_ms() + 10_000,
        8,
        &ExecutionContext::default(),
    ))
    .expect("interval join");

    assert_eq!(rows.len(), 4);
    assert!(rows.iter().all(|row| row.values().len() == 2));
}

#[test]
fn coordinator_executes_interval_left_join_with_empty_right_input() {
    let mut coordinator = DistributedCoordinator::new(2 << 20, 8).expect("coordinator");
    coordinator
        .register(Arc::new(worker(0, 1)))
        .expect("worker 0");
    coordinator
        .register(Arc::new(worker(1, 2)))
        .expect("worker 1");
    let left_schema = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "left",
        ValueType::Node,
        false,
    )])
    .expect("left schema");
    let right_schema = RowSchema::new(vec![Column::new(
        SlotId::new(1),
        "right",
        ValueType::Node,
        false,
    )])
    .expect("right schema");
    let output = RowSchema::new(vec![
        Column::new(SlotId::new(0), "left", ValueType::Node, false),
        Column::new(SlotId::new(1), "right", ValueType::Node, true),
    ])
    .expect("output schema");
    let mut builder = PhysicalPlanBuilder::new(shared_header([2; 32]));
    let left = builder
        .add_fragment(
            Placement::AllShards,
            vec![PhysicalOperator::NodeScan {
                binding: SlotId::new(0),
                labels: vec![11],
                output: left_schema.clone(),
            }],
            left_schema.clone(),
            MemoryBudget::new(1 << 20, 1 << 20).expect("budget"),
        )
        .expect("left");
    let right = builder
        .add_fragment(
            Placement::AllShards,
            vec![PhysicalOperator::NodeScan {
                binding: SlotId::new(1),
                labels: vec![12],
                output: right_schema.clone(),
            }],
            right_schema.clone(),
            MemoryBudget::new(1 << 20, 1 << 20).expect("budget"),
        )
        .expect("right");
    let root = builder
        .add_fragment(
            Placement::Coordinator,
            vec![PhysicalOperator::HashJoin {
                kind: JoinKind::Left,
                keys: Vec::new(),
            }],
            output,
            MemoryBudget::new(1 << 20, 1 << 20).expect("budget"),
        )
        .expect("root");
    builder
        .add_exchange(left, root, ExchangeKind::Gather, left_schema, 8)
        .expect("left exchange");
    builder
        .add_exchange(right, root, ExchangeKind::Gather, right_schema, 8)
        .expect("right exchange");
    let plan = builder.finish(root).expect("plan");

    let rows = block_on(coordinator.execute_interval_plan(
        &plan,
        SnapshotToken::new(1, 3, 11, tx(150), [5; 32]).expect("snapshot"),
        Interval::new(ValidTime::from_micros(1), Some(ValidTime::from_micros(10))).expect("window"),
        now_ms() + 10_000,
        8,
        &ExecutionContext::default(),
    ))
    .expect("interval left join");

    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|row| row.values()[1] == RuntimeValue::Null));
}

#[test]
fn temporal_join_is_equivalent_in_primary_replica_and_shared_nothing() {
    let primary_rows =
        execute_temporal_join(DeploymentMode::PrimaryReplica, TemporalJoinKind::Left);
    let shared_rows = execute_temporal_join(DeploymentMode::SharedNothing, TemporalJoinKind::Left);

    assert_eq!(primary_rows, shared_rows);
    assert_eq!(primary_rows.len(), 3);
    assert_eq!(primary_rows[0].region().valid(), valid_interval(1, 3));
    assert_eq!(primary_rows[0].values()[1], RuntimeValue::Null);
    assert_eq!(primary_rows[0].provenance().len(), 1);
    assert_eq!(primary_rows[1].region().valid(), valid_interval(3, 7));
    assert_ne!(primary_rows[1].values()[1], RuntimeValue::Null);
    assert_eq!(primary_rows[1].provenance().len(), 2);
    assert_eq!(primary_rows[2].region().valid(), valid_interval(7, 10));
    assert_eq!(primary_rows[2].values()[1], RuntimeValue::Null);
    assert_eq!(primary_rows[2].provenance().len(), 1);
}

#[test]
fn temporal_inner_join_is_equivalent_in_primary_replica_and_shared_nothing() {
    let primary_rows =
        execute_temporal_join(DeploymentMode::PrimaryReplica, TemporalJoinKind::Inner);
    let shared_rows = execute_temporal_join(DeploymentMode::SharedNothing, TemporalJoinKind::Inner);

    assert_eq!(primary_rows, shared_rows);
    assert_eq!(primary_rows.len(), 1);
    assert_eq!(primary_rows[0].region().valid(), valid_interval(3, 7));
    assert_ne!(primary_rows[0].values()[1], RuntimeValue::Null);
    assert_eq!(primary_rows[0].provenance().len(), 2);
}

fn execute_temporal_join(
    mode: DeploymentMode,
    kind: TemporalJoinKind,
) -> Vec<query_executor::TemporalRow> {
    let plan = temporal_join_plan(mode, kind);
    let mut coordinator = DistributedCoordinator::new(2 << 20, 8).unwrap();
    match mode {
        DeploymentMode::PrimaryReplica => coordinator
            .register(Arc::new(worker_with_vertices(
                0,
                &[(1, 0, 11, 1, 10), (2, 1, 12, 3, 7)],
            )))
            .unwrap(),
        DeploymentMode::SharedNothing => {
            coordinator
                .register(Arc::new(worker_with_vertices(0, &[(1, 0, 11, 1, 10)])))
                .unwrap();
            coordinator
                .register(Arc::new(worker_with_vertices(1, &[(2, 1, 12, 3, 7)])))
                .unwrap();
        }
    }
    block_on(coordinator.execute_interval_plan(
        &plan,
        SnapshotToken::new(1, 3, 11, tx(150), [5; 32]).unwrap(),
        valid_interval(1, 10),
        now_ms() + 10_000,
        8,
        &ExecutionContext::default(),
    ))
    .unwrap()
}

fn temporal_join_plan(mode: DeploymentMode, kind: TemporalJoinKind) -> physical_plan::PhysicalPlan {
    let left = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "left",
        ValueType::Node,
        false,
    )])
    .unwrap();
    let right = RowSchema::new(vec![Column::new(
        SlotId::new(1),
        "right",
        ValueType::Node,
        false,
    )])
    .unwrap();
    let output = RowSchema::new(vec![
        left.columns()[0].clone(),
        Column::new(
            SlotId::new(1),
            "right",
            ValueType::Node,
            kind == TemporalJoinKind::Left,
        ),
    ])
    .unwrap();
    let mut builder = LogicalPlanBuilder::new(
        PlanHeader::new(
            1,
            3,
            11,
            LanguageProfile::Cypher25,
            "Cypher 25 / 2026.07",
            [13; 32],
        )
        .unwrap(),
    );
    let left_node = builder
        .add(
            LogicalOperator::NodeScan {
                binding: SlotId::new(0),
                labels: vec![11],
            },
            vec![],
            left,
        )
        .unwrap();
    let right_node = builder
        .add(
            LogicalOperator::NodeScan {
                binding: SlotId::new(1),
                labels: vec![12],
            },
            vec![],
            right,
        )
        .unwrap();
    let root = builder
        .add(
            LogicalOperator::TemporalJoin {
                kind,
                keys: Vec::new(),
            },
            vec![left_node, right_node],
            output,
        )
        .unwrap();
    let logical = builder.finish(root).unwrap();
    let shards = if mode == DeploymentMode::PrimaryReplica {
        1
    } else {
        2
    };
    Optimizer::new()
        .optimize(
            &logical,
            OptimizerContext::new(mode, shards, 1 << 20, 1 << 20).unwrap(),
        )
        .unwrap()
        .plan()
        .clone()
}

fn valid_interval(start: i64, end: i64) -> Interval<ValidTime> {
    Interval::new(
        ValidTime::from_micros(start),
        Some(ValidTime::from_micros(end)),
    )
    .unwrap()
}

fn worker_with_vertices(
    shard_id: u32,
    vertices: &[(u128, u32, u32, i64, i64)],
) -> LocalFragmentWorker<MemoryAdapter> {
    let store = TemporalStore::new(MemoryAdapter::new());
    for (index, &(element_id, partition_id, label_id, valid_from, valid_to)) in
        vertices.iter().enumerate()
    {
        let element = ElementRef::vertex(
            GraphId::new(1),
            PartitionId::new(partition_id),
            ElementId::new(element_id),
        );
        block_on(
            store.commit_vertex(
                CommitContext::new(
                    shard_id,
                    u64::try_from(index).unwrap() + 1,
                    element_id,
                    tx(0),
                    tx(100),
                ),
                VertexMutation::put(
                    element,
                    LabelId::new(label_id),
                    valid_interval(valid_from, valid_to),
                    CanonicalElement::new(
                        1,
                        BTreeMap::from([(1, GraphValue::Integer(element_id as i64))]),
                    ),
                )
                .unwrap(),
            ),
        )
        .unwrap();
    }
    LocalFragmentWorker::new(
        shard_id,
        1,
        3,
        11,
        [5; 32],
        TemporalBatchExecutor::new(store),
    )
}

fn worker(shard_id: u32, element_id: u128) -> LocalFragmentWorker<MemoryAdapter> {
    let store = TemporalStore::new(MemoryAdapter::new());
    let element = ElementRef::vertex(
        GraphId::new(1),
        PartitionId::new(shard_id),
        ElementId::new(element_id),
    );
    block_on(
        store.commit_vertex(
            CommitContext::new(shard_id, 1, element_id, tx(0), tx(100)),
            VertexMutation::put(
                element,
                LabelId::new(11),
                Interval::new(ValidTime::from_micros(1), None).expect("interval"),
                CanonicalElement::new(
                    1,
                    BTreeMap::from([(1, GraphValue::Integer(element_id as i64))]),
                ),
            )
            .expect("mutation"),
        ),
    )
    .expect("commit");
    LocalFragmentWorker::new(
        shard_id,
        1,
        3,
        11,
        [5; 32],
        TemporalBatchExecutor::new(store),
    )
}

fn tx(value: i64) -> TransactionTime {
    TransactionTime::new(value, 0)
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
