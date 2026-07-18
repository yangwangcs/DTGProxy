use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use adapter_memory::MemoryAdapter;
use dtgproxy::{
    DeploymentConfig, DeploymentError, DeploymentMode, InProcessDeploymentRuntime,
    QueryRoutePolicy, RoutedRuntimeError, ShardPlacement,
};
use query_executor::QueryRecord;
use raft_command::{ApplyPreparedV1, CommandBodyV1, CommandEnvelopeV1};
use storage_api::{AdapterRequirement, StorageAdapter};
use temporal_ir::{DiffOperator, GraphScope, PointOperator, TemporalPlan, TemporalSelector};
use temporal_storage::{
    ElementId, ElementKind, ElementRef, GraphId, LabelId, PartitionId, PrepareContext,
    TemporalStore, TemporalTransaction, VertexMutation,
};
use temporal_types::{CanonicalElement, GraphValue, Interval, TransactionTime, ValidTime};

fn placement(shard_id: u32, voters: &[u64]) -> ShardPlacement {
    ShardPlacement::new(shard_id, 7, voters.to_vec()).unwrap()
}

fn scope(graph: u64, partition: u32) -> GraphScope {
    GraphScope::new(GraphId::new(graph), PartitionId::new(partition))
}

#[test]
fn primary_replica_routes_every_scope_to_its_only_shard() {
    let config = DeploymentConfig::primary_replica(placement(11, &[1, 2, 3]));

    assert_eq!(config.mode(), DeploymentMode::PrimaryReplica);
    assert_eq!(config.route_scope(scope(1, 0)).shard_id(), 11);
    assert_eq!(config.route_scope(scope(99, 42)).shard_id(), 11);
    assert_eq!(config.all_shards(), &[placement(11, &[1, 2, 3])]);
}

#[test]
fn durable_catalog_graph_materializes_the_same_runtime_topology() {
    use dtgproxy::control_plane::{
        BackendProfile, DeploymentMode as CatalogMode, GraphDefinition,
        Placement as CatalogPlacement, TopologyDefinition,
    };

    let graph = GraphDefinition::new(
        9,
        "catalog-graph",
        1,
        TopologyDefinition::new(
            CatalogMode::SharedNothing,
            77,
            4_096,
            3,
            vec![
                CatalogPlacement::new(10, 5, vec![1, 2, 3]).unwrap(),
                CatalogPlacement::new(20, 6, vec![4, 5, 6]).unwrap(),
            ],
        )
        .unwrap(),
        BackendProfile::new(
            "rocksdb",
            BTreeMap::from([("path".into(), "data/catalog-graph".into())]),
            BTreeMap::new(),
            AdapterRequirement::HotPluggableReplica,
            1,
        )
        .unwrap(),
    )
    .unwrap();

    let config = DeploymentConfig::from_catalog(&graph).unwrap();
    assert_eq!(config.mode(), DeploymentMode::SharedNothing);
    assert_eq!(config.route_seed(), 77);
    assert_eq!(config.virtual_partitions(), 4_096);
    assert_eq!(config.all_shards()[0].placement_epoch(), 5);
    assert_eq!(config.all_shards()[1].voters(), &[4, 5, 6]);
}

#[test]
fn shared_nothing_route_is_deterministic_and_minimizes_movement_when_adding_a_shard() {
    let original = DeploymentConfig::shared_nothing(
        0x4454_4750,
        vec![placement(1, &[1, 2, 3]), placement(2, &[4, 5, 6])],
    )
    .unwrap();
    let expanded = DeploymentConfig::shared_nothing(
        0x4454_4750,
        vec![
            placement(1, &[1, 2, 3]),
            placement(2, &[4, 5, 6]),
            placement(3, &[7, 8, 9]),
        ],
    )
    .unwrap();

    assert_eq!(original.mode(), DeploymentMode::SharedNothing);
    let mut saw_shard_1 = false;
    let mut saw_shard_2 = false;
    let mut saw_new_shard = false;
    for partition in 0..1_000 {
        let graph_scope = scope(9, partition);
        let before = original.route_scope(graph_scope).shard_id();
        let repeated = original.route_scope(graph_scope).shard_id();
        let after = expanded.route_scope(graph_scope).shard_id();
        assert_eq!(before, repeated);
        assert!(after == before || after == 3);
        saw_shard_1 |= before == 1;
        saw_shard_2 |= before == 2;
        saw_new_shard |= after == 3;
    }
    assert!(saw_shard_1 && saw_shard_2 && saw_new_shard);
}

#[test]
fn deployment_validation_fails_closed() {
    assert!(matches!(
        ShardPlacement::new(1, 0, vec![1]),
        Err(DeploymentError::ZeroPlacementEpoch { shard_id: 1 })
    ));
    assert!(matches!(
        ShardPlacement::new(1, 1, vec![1, 1]),
        Err(DeploymentError::DuplicateVoter {
            shard_id: 1,
            node_id: 1
        })
    ));
    assert!(matches!(
        DeploymentConfig::shared_nothing(1, vec![placement(1, &[1]), placement(1, &[2])]),
        Err(DeploymentError::DuplicateShard { shard_id: 1 })
    ));
    assert!(matches!(
        DeploymentConfig::shared_nothing(1, vec![placement(1, &[1])]),
        Err(DeploymentError::SharedNothingNeedsMultipleShards { actual: 1 })
    ));
}

#[test]
fn temporal_ir_preserves_current_and_snapshot_read_policy() {
    let config = DeploymentConfig::shared_nothing(
        5,
        vec![placement(10, &[1, 2, 3]), placement(20, &[4, 5, 6])],
    )
    .unwrap();
    let graph_scope = scope(7, 8);
    let current = TemporalPlan::point(
        graph_scope,
        PointOperator::VertexById(ElementId::new(42)),
        ValidTime::from_micros(10),
        TemporalSelector::Current,
        1,
    );
    let historical = TemporalPlan::diff(
        graph_scope,
        DiffOperator::Element {
            kind: ElementKind::Vertex,
            id: ElementId::new(42),
        },
        TransactionTime::new(100, 0),
        TransactionTime::new(200, 0),
        10,
    );

    let current_route = config.route_plan(&current).unwrap();
    let historical_route = config.route_plan(&historical).unwrap();
    assert_eq!(current_route.shard(), historical_route.shard());
    assert_eq!(current_route.policy(), QueryRoutePolicy::LeaderRequired);
    assert_eq!(
        historical_route.policy(),
        QueryRoutePolicy::FollowerEligible {
            read_ts: TransactionTime::new(200, 0)
        }
    );
}

#[test]
fn both_modes_materialize_the_same_independent_multi_raft_runtime() {
    let primary = DeploymentConfig::primary_replica(placement(1, &[1, 2, 3]));
    let mut primary_runtime = block_on(primary.build_in_process_runtime()).unwrap();
    assert!(primary_runtime.owns(1, 1));
    assert!(!primary_runtime.owns(1, 2));
    block_on(primary_runtime.group_mut(1).unwrap().elect(1)).unwrap();

    let shared = DeploymentConfig::shared_nothing(
        3,
        vec![placement(1, &[1, 2, 3]), placement(2, &[1, 2, 3])],
    )
    .unwrap();
    let mut shared_runtime = block_on(shared.build_in_process_runtime()).unwrap();
    assert!(shared_runtime.owns(1, 1));
    assert!(shared_runtime.owns(1, 2));
    block_on(shared_runtime.group_mut(1).unwrap().elect(1)).unwrap();
    block_on(shared_runtime.group_mut(2).unwrap().elect(2)).unwrap();
    assert_eq!(shared_runtime.group(1).unwrap().leader_id(), Some(1));
    assert_eq!(shared_runtime.group(2).unwrap().leader_id(), Some(2));
}

#[test]
fn shared_nothing_executes_a_logical_partition_on_its_routed_physical_shard() {
    let config = DeploymentConfig::shared_nothing(
        5,
        vec![placement(10, &[1, 2, 3]), placement(20, &[1, 2, 3])],
    )
    .unwrap();
    let graph_scope = scope(7, 8);
    let routed = config.route_scope(graph_scope).clone();
    assert_ne!(routed.shard_id(), graph_scope.partition().value());

    let mut runtime = block_on(InProcessDeploymentRuntime::new(config)).unwrap();
    block_on(runtime.elect(10, 1)).unwrap();
    block_on(runtime.elect(20, 1)).unwrap();

    let planner = TemporalStore::new(MemoryAdapter::new());
    let vertex = ElementRef::vertex(
        graph_scope.graph(),
        graph_scope.partition(),
        ElementId::new(42),
    );
    let value = payload("routed");
    let prepared = block_on(
        planner.prepare_transaction(
            PrepareContext::new(routed.shard_id(), 9_001, ts(0), ts(100)),
            TemporalTransaction::new().with_vertex(
                VertexMutation::put(
                    vertex,
                    LabelId::new(1),
                    Interval::new(ValidTime::from_micros(0), None).unwrap(),
                    value.clone(),
                )
                .unwrap(),
            ),
        ),
    )
    .unwrap();
    block_on(
        planner
            .adapter()
            .apply_committed(prepared.clone().commit_at(1)),
    )
    .unwrap();
    let command = CommandEnvelopeV1::new(
        routed.shard_id(),
        routed.placement_epoch(),
        101,
        CommandBodyV1::ApplyPrepared(ApplyPreparedV1 {
            commit_ts: ts(100),
            batch: prepared,
        }),
    )
    .encode()
    .unwrap();
    block_on(runtime.propose_scoped(graph_scope, command, 20)).unwrap();
    block_on(runtime.advance_closed_timestamp(graph_scope, ts(100), 20)).unwrap();

    let current = TemporalPlan::point(
        graph_scope,
        PointOperator::VertexById(ElementId::new(42)),
        ValidTime::from_micros(1),
        TemporalSelector::Current,
        1,
    );
    let current_result = block_on(runtime.execute_leader(&current, 5)).unwrap();
    assert_eq!(result_payload(&current_result), value);

    let historical = TemporalPlan::point(
        graph_scope,
        PointOperator::VertexById(ElementId::new(42)),
        ValidTime::from_micros(1),
        TemporalSelector::AsOf(ts(100)),
        1,
    );
    let follower_result = block_on(runtime.execute_follower(&historical, 2, 5)).unwrap();
    assert_eq!(result_payload(&follower_result), payload("routed"));
    assert!(matches!(
        block_on(runtime.execute_follower(&current, 2, 5)),
        Err(RoutedRuntimeError::CurrentRequiresLeader)
    ));
}

fn ts(value: i64) -> TransactionTime {
    TransactionTime::new(value, 0)
}

fn payload(value: &str) -> CanonicalElement {
    CanonicalElement::new(
        1,
        BTreeMap::from([(1, GraphValue::String(value.to_owned()))]),
    )
}

fn result_payload(result: &query_executor::QueryResult) -> CanonicalElement {
    let [QueryRecord::Vertex(record)] = result.records() else {
        panic!("expected one vertex record");
    };
    record.payload().clone()
}

struct NoopWake;

impl Wake for NoopWake {
    fn wake(self: Arc<Self>) {}
}

fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::from(Arc::new(NoopWake));
    let mut context = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}
