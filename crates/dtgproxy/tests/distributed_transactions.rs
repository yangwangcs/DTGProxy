use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use dtgproxy::{
    DeploymentConfig, InProcessDeploymentRuntime, PreparedShardTransaction,
    ScopedTemporalTransaction, ShardPlacement, TransactionCoordinator, TransactionCoordinatorError,
    TransactionStatus,
};
use storage_api::{Keyspace, LogicalKey, Mutation, PreparedMutationBatch, StorageAdapter};
use temporal_ir::GraphScope;
use temporal_storage::{
    ElementId, ElementRef, GraphId, LabelId, PartitionId, TemporalStore, TemporalTransaction,
    VertexMutation,
};
use temporal_types::{CanonicalElement, GraphValue, Interval, ValidTime};
use timestamp_oracle::{ManualClock, MemoryTimestampStore, TimestampOracle};
use txn_protocol::TransactionState;
use txn_protocol::{HomeDecisionEngine, HomeTransactionRecord, IsolationLevel};

fn placement(shard_id: u32) -> ShardPlacement {
    ShardPlacement::new(shard_id, 7, vec![u64::from(shard_id)]).unwrap()
}

fn batch(
    shard_id: u32,
    transaction_id: u128,
    key: &[u8],
    value: &[u8],
) -> PreparedShardTransaction {
    PreparedShardTransaction::new(
        shard_id,
        7,
        PreparedMutationBatch {
            shard_id,
            txn_id: transaction_id,
            mutations: vec![Mutation::put(
                0,
                LogicalKey::in_keyspace(Keyspace::Current, key.to_vec()),
                value.to_vec(),
            )],
        },
    )
    .unwrap()
}

#[test]
fn cross_shard_commit_records_home_decision_and_applies_every_participant() {
    let config = DeploymentConfig::shared_nothing(9, vec![placement(10), placement(20)]).unwrap();
    let mut runtime = block_on(InProcessDeploymentRuntime::new(config)).unwrap();
    block_on(runtime.elect(10, 10)).unwrap();
    block_on(runtime.elect(20, 20)).unwrap();
    let oracle = TimestampOracle::open(
        Arc::new(MemoryTimestampStore::new()),
        Arc::new(ManualClock::new(1_000)),
        16,
    )
    .unwrap();
    let coordinator = TransactionCoordinator::new(&oracle, 20);
    let context = coordinator
        .begin(3, IsolationLevel::TemporalSnapshot, 10_000)
        .unwrap();

    let receipt = block_on(coordinator.commit(
        &mut runtime,
        context,
        vec![
            batch(20, context.transaction_id().value(), b"v/20", b"twenty"),
            batch(10, context.transaction_id().value(), b"v/10", b"ten"),
        ],
    ))
    .unwrap();
    assert!(!receipt.single_shard_fast_path());
    assert_eq!(receipt.participants().len(), 2);
    assert_eq!(read(&runtime, 10, b"v/10"), Some(b"ten".to_vec()));
    assert_eq!(read(&runtime, 20, b"v/20"), Some(b"twenty".to_vec()));

    let home = receipt.home();
    let home_key = HomeDecisionEngine::inspection_key(home, receipt.transaction_id()).unwrap();
    let decision = read_key(&runtime, home.shard_id(), &home_key).unwrap();
    let decision = HomeTransactionRecord::decode(&decision).unwrap();
    assert_eq!(decision.commit_ts(), Some(receipt.commit_ts()));
    assert_eq!(
        block_on(coordinator.status(&mut runtime, home, receipt.transaction_id())).unwrap(),
        TransactionStatus::Committed {
            commit_ts: receipt.commit_ts()
        }
    );
}

#[test]
fn single_shard_transaction_uses_one_raft_entry() {
    let config = DeploymentConfig::primary_replica(placement(10));
    let mut runtime = block_on(InProcessDeploymentRuntime::new(config)).unwrap();
    block_on(runtime.elect(10, 10)).unwrap();
    let applied_before = {
        let group = runtime.raft().group(10).unwrap();
        let leader = group.leader_id().unwrap();
        group.replica_metadata(leader).unwrap().applied_index
    };
    let oracle = TimestampOracle::open(
        Arc::new(MemoryTimestampStore::new()),
        Arc::new(ManualClock::new(2_000)),
        16,
    )
    .unwrap();
    let coordinator = TransactionCoordinator::new(&oracle, 20);
    let context = coordinator
        .begin(3, IsolationLevel::TemporalSnapshot, 10_000)
        .unwrap();
    let receipt = block_on(coordinator.commit(
        &mut runtime,
        context,
        vec![batch(
            10,
            context.transaction_id().value(),
            b"v/10",
            b"fast",
        )],
    ))
    .unwrap();

    assert!(receipt.single_shard_fast_path());
    let group = runtime.raft().group(10).unwrap();
    let leader = group.leader_id().unwrap();
    assert_eq!(
        group.replica_metadata(leader).unwrap().applied_index,
        applied_before + 1
    );
    assert_eq!(read(&runtime, 10, b"v/10"), Some(b"fast".to_vec()));
    assert_eq!(
        block_on(coordinator.status(&mut runtime, receipt.home(), receipt.transaction_id()))
            .unwrap(),
        TransactionStatus::Committed {
            commit_ts: receipt.commit_ts()
        }
    );
}

#[test]
fn temporal_graph_input_is_rewritten_distributed_committed_and_read_back_temporally() {
    let config = DeploymentConfig::shared_nothing(13, vec![placement(10), placement(20)]).unwrap();
    let scopes = (0..1_000)
        .map(|partition| {
            let scope = GraphScope::new(GraphId::new(9), PartitionId::new(partition));
            (config.route_scope(scope).shard_id(), scope)
        })
        .fold(BTreeMap::new(), |mut scopes, (shard, scope)| {
            scopes.entry(shard).or_insert(scope);
            scopes
        });
    let scope_10 = scopes[&10];
    let scope_20 = scopes[&20];
    let vertex_10 = ElementRef::vertex(scope_10.graph(), scope_10.partition(), ElementId::new(10));
    let vertex_20 = ElementRef::vertex(scope_20.graph(), scope_20.partition(), ElementId::new(20));
    let payload_10 = payload("ten");
    let payload_20 = payload("twenty");
    let valid = Interval::new(ValidTime::from_micros(0), None).unwrap();

    let mut runtime = block_on(InProcessDeploymentRuntime::new(config)).unwrap();
    block_on(runtime.elect(10, 10)).unwrap();
    block_on(runtime.elect(20, 20)).unwrap();
    let oracle = TimestampOracle::open(
        Arc::new(MemoryTimestampStore::new()),
        Arc::new(ManualClock::new(3_000)),
        16,
    )
    .unwrap();
    let coordinator = TransactionCoordinator::new(&oracle, 20);
    let receipt = block_on(coordinator.commit_temporal(
        &mut runtime,
        1,
        IsolationLevel::TemporalSnapshot,
        10_000,
        vec![
            ScopedTemporalTransaction::new(
                scope_10,
                TemporalTransaction::new().with_vertex(
                    VertexMutation::put(vertex_10, LabelId::new(1), valid, payload_10.clone())
                        .unwrap(),
                ),
            ),
            ScopedTemporalTransaction::new(
                scope_20,
                TemporalTransaction::new().with_vertex(
                    VertexMutation::put(vertex_20, LabelId::new(1), valid, payload_20.clone())
                        .unwrap(),
                ),
            ),
        ],
    ))
    .unwrap();
    assert!(!receipt.single_shard_fast_path());
    assert_eq!(read_vertex(&runtime, 10, vertex_10), Some(payload_10));
    assert_eq!(read_vertex(&runtime, 20, vertex_20), Some(payload_20));
}

#[test]
fn failed_prewrite_records_abort_and_cleans_every_known_prepared_participant() {
    let config = DeploymentConfig::shared_nothing(21, vec![placement(10), placement(20)]).unwrap();
    let mut runtime = block_on(InProcessDeploymentRuntime::new(config)).unwrap();
    block_on(runtime.elect(10, 10)).unwrap();
    block_on(runtime.elect(20, 20)).unwrap();
    runtime
        .raft_mut()
        .group_mut(20)
        .unwrap()
        .stop_node(20)
        .unwrap();
    let oracle = TimestampOracle::open(
        Arc::new(MemoryTimestampStore::new()),
        Arc::new(ManualClock::new(4_000)),
        16,
    )
    .unwrap();
    let coordinator = TransactionCoordinator::new(&oracle, 2);
    let context = coordinator
        .begin(3, IsolationLevel::TemporalSnapshot, 10_000)
        .unwrap();
    let error = block_on(coordinator.commit(
        &mut runtime,
        context,
        vec![
            batch(10, context.transaction_id().value(), b"v/10", b"ten"),
            batch(20, context.transaction_id().value(), b"v/20", b"twenty"),
        ],
    ))
    .unwrap_err();
    let TransactionCoordinatorError::PrewriteFailed {
        abort_decision_durable,
        cleanup_pending,
        ..
    } = error
    else {
        panic!("expected Prewrite failure");
    };
    assert!(abort_decision_durable);
    assert_eq!(cleanup_pending.len(), 1);
    assert_eq!(cleanup_pending[0].shard_id(), 20);
    assert_eq!(read(&runtime, 10, b"v/10"), None);

    let home = txn_protocol::ShardEpoch::new(10, 7).unwrap();
    let home_key = HomeDecisionEngine::inspection_key(home, context.transaction_id()).unwrap();
    let decision =
        HomeTransactionRecord::decode(&read_key(&runtime, 10, &home_key).unwrap()).unwrap();
    assert_eq!(decision.state(), TransactionState::Aborted);
    assert_eq!(
        block_on(coordinator.status(&mut runtime, home, context.transaction_id())).unwrap(),
        TransactionStatus::Aborted
    );
}

fn read(runtime: &InProcessDeploymentRuntime, shard_id: u32, key: &[u8]) -> Option<Vec<u8>> {
    read_key(
        runtime,
        shard_id,
        &LogicalKey::in_keyspace(Keyspace::Current, key.to_vec()),
    )
}

fn read_key(
    runtime: &InProcessDeploymentRuntime,
    shard_id: u32,
    key: &LogicalKey,
) -> Option<Vec<u8>> {
    let group = runtime.raft().group(shard_id).unwrap();
    let leader = group.leader_id().unwrap();
    block_on(
        group
            .replica_adapter(leader)
            .unwrap()
            .multi_get(std::slice::from_ref(key)),
    )
    .unwrap()
    .pop()
    .flatten()
}

fn read_vertex(
    runtime: &InProcessDeploymentRuntime,
    shard_id: u32,
    vertex: ElementRef,
) -> Option<CanonicalElement> {
    let group = runtime.raft().group(shard_id).unwrap();
    let leader = group.leader_id().unwrap();
    block_on(
        TemporalStore::new(group.replica_adapter(leader).unwrap())
            .vertex_current(vertex, ValidTime::from_micros(1)),
    )
    .unwrap()
}

fn payload(value: &str) -> CanonicalElement {
    CanonicalElement::new(
        1,
        BTreeMap::from([(1, GraphValue::String(value.to_owned()))]),
    )
}

fn block_on<F: Future>(future: F) -> F::Output {
    struct ThreadWaker(std::thread::Thread);

    impl Wake for ThreadWaker {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.unpark();
        }
    }

    let waker = Waker::from(Arc::new(ThreadWaker(std::thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = Box::pin(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => std::thread::park(),
        }
    }
}
use std::collections::BTreeMap;
