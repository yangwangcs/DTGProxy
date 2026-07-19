use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::time::{SystemTime, UNIX_EPOCH};

use adapter_memory::MemoryAdapter;
use cypher_engine::{
    CypherQueryEngine, CypherQueryRequest, DeploymentMode, EngineConfig, ResourceLimits,
};
use distributed_query::{DistributedCoordinator, LocalFragmentWorker};
use query_executor::v2::{RuntimeValue, TemporalBatchExecutor};
use temporal_storage::{
    CommitContext, ElementId, ElementRef, GraphId, LabelId, PartitionId, TemporalStore,
    VertexMutation,
};
use temporal_types::{CanonicalElement, GraphValue, Interval, TransactionTime, ValidTime};

#[test]
fn executes_parameterized_temporal_cypher_on_nonzero_primary_shard() {
    let security = [9; 32];
    let store = TemporalStore::new(MemoryAdapter::new());
    seed(&store);
    let worker =
        LocalFragmentWorker::new(42, 7, 3, 11, security, TemporalBatchExecutor::new(store));
    let mut coordinator = DistributedCoordinator::new(4 << 20, 16).expect("coordinator");
    coordinator.register(Arc::new(worker)).expect("worker");
    let engine = CypherQueryEngine::new(
        EngineConfig::new(
            "accounts",
            7,
            3,
            11,
            DeploymentMode::PrimaryReplica,
            vec![42],
            ResourceLimits::new(1 << 20, 4 << 20, 256).expect("limits"),
        )
        .expect("engine config"),
    );

    let visible = request(150, security);
    let response = block_on(engine.execute(&coordinator, visible)).expect("visible query");
    assert_eq!(response.row_count(), 1);
    let RuntimeValue::Node(node) = &response.batches()[0].rows()[0][0] else {
        panic!("expected node");
    };
    assert_eq!(node.element().id(), ElementId::new(1));

    let historical = request(50, security);
    let response = block_on(engine.execute(&coordinator, historical)).expect("historical query");
    assert_eq!(response.row_count(), 0);
}

#[test]
fn executes_count_aggregate_after_temporal_scan() {
    let security = [8; 32];
    let store = TemporalStore::new(MemoryAdapter::new());
    seed(&store);
    let worker =
        LocalFragmentWorker::new(42, 7, 3, 11, security, TemporalBatchExecutor::new(store));
    let mut coordinator = DistributedCoordinator::new(4 << 20, 16).expect("coordinator");
    coordinator.register(Arc::new(worker)).expect("worker");
    let engine = CypherQueryEngine::new(
        EngineConfig::new(
            "accounts",
            7,
            3,
            11,
            DeploymentMode::PrimaryReplica,
            vec![42],
            ResourceLimits::new(1 << 20, 4 << 20, 256).expect("limits"),
        )
        .expect("engine config"),
    );
    let response = block_on(engine.execute(
        &coordinator,
        CypherQueryRequest::new(
            "USE accounts AT VALID_TIME AS OF 5 MATCH (n) RETURN count(n)",
            BTreeMap::new(),
            ValidTime::from_micros(5),
            TransactionTime::new(150, 0),
            security,
            now_ms() + 10_000,
        ),
    ))
    .expect("count query");
    assert_eq!(response.row_count(), 1);
    assert_eq!(response.batches()[0].rows()[0][0], RuntimeValue::Integer(1));
}

#[test]
fn gathers_shared_nothing_shards_under_one_temporal_snapshot() {
    let security = [7; 32];
    let mut coordinator = DistributedCoordinator::new(4 << 20, 16).expect("coordinator");
    coordinator
        .register(Arc::new(worker(8, 2, security)))
        .expect("worker 8");
    coordinator
        .register(Arc::new(worker(3, 1, security)))
        .expect("worker 3");
    let engine = CypherQueryEngine::new(
        EngineConfig::new(
            "accounts",
            7,
            3,
            11,
            DeploymentMode::SharedNothing,
            vec![8, 3],
            ResourceLimits::new(1 << 20, 4 << 20, 1).expect("limits"),
        )
        .expect("engine config"),
    );

    let response =
        block_on(engine.execute(&coordinator, request(150, security))).expect("distributed query");
    let ids = response
        .batches()
        .iter()
        .flat_map(|batch| batch.rows())
        .map(|row| match &row[0] {
            RuntimeValue::Node(node) => node.element().id(),
            value => panic!("expected node, got {value:?}"),
        })
        .collect::<Vec<_>>();
    assert_eq!(ids, vec![ElementId::new(1), ElementId::new(2)]);
}

#[test]
fn shared_nothing_aggregates_after_global_gather() {
    let security = [6; 32];
    let mut coordinator = DistributedCoordinator::new(4 << 20, 16).expect("coordinator");
    coordinator
        .register(Arc::new(worker(8, 2, security)))
        .expect("worker 8");
    coordinator
        .register(Arc::new(worker(3, 1, security)))
        .expect("worker 3");
    let engine = CypherQueryEngine::new(
        EngineConfig::new(
            "accounts",
            7,
            3,
            11,
            DeploymentMode::SharedNothing,
            vec![8, 3],
            ResourceLimits::new(1 << 20, 4 << 20, 256).expect("limits"),
        )
        .expect("engine config"),
    );
    let response = block_on(engine.execute(
        &coordinator,
        CypherQueryRequest::new(
            "MATCH (n) RETURN count(n)",
            BTreeMap::new(),
            ValidTime::from_micros(5),
            TransactionTime::new(150, 0),
            security,
            now_ms() + 10_000,
        ),
    ))
    .expect("distributed count query");
    assert_eq!(response.row_count(), 1);
    assert_eq!(response.batches()[0].rows()[0][0], RuntimeValue::Integer(2));
}

fn request(transaction_micros: i64, security: [u8; 32]) -> CypherQueryRequest {
    CypherQueryRequest::new(
        "USE accounts AT VALID_TIME AS OF $valid \
         AT TRANSACTION_TIME AS OF $tx MATCH (n) RETURN n",
        BTreeMap::from([
            ("valid".into(), RuntimeValue::TimestampMicros(5)),
            (
                "tx".into(),
                RuntimeValue::TimestampMicros(transaction_micros),
            ),
        ]),
        ValidTime::from_micros(999),
        TransactionTime::new(999, 0),
        security,
        now_ms() + 10_000,
    )
}

fn seed(store: &TemporalStore<MemoryAdapter>) {
    let element = ElementRef::vertex(GraphId::new(7), PartitionId::new(0), ElementId::new(1));
    block_on(
        store.commit_vertex(
            CommitContext::new(
                42,
                1,
                1,
                TransactionTime::new(0, 0),
                TransactionTime::new(100, 0),
            ),
            VertexMutation::put(
                element,
                LabelId::new(1),
                Interval::new(ValidTime::from_micros(1), None).expect("interval"),
                CanonicalElement::new(3, BTreeMap::from([(1, GraphValue::String("alice".into()))])),
            )
            .expect("mutation"),
        ),
    )
    .expect("commit");
}

fn worker(
    shard_id: u32,
    element_id: u128,
    security: [u8; 32],
) -> LocalFragmentWorker<MemoryAdapter> {
    let store = TemporalStore::new(MemoryAdapter::new());
    let element = ElementRef::vertex(
        GraphId::new(7),
        PartitionId::new(shard_id),
        ElementId::new(element_id),
    );
    block_on(
        store.commit_vertex(
            CommitContext::new(
                shard_id,
                1,
                element_id,
                TransactionTime::new(0, 0),
                TransactionTime::new(100, 0),
            ),
            VertexMutation::put(
                element,
                LabelId::new(1),
                Interval::new(ValidTime::from_micros(1), None).expect("interval"),
                CanonicalElement::new(
                    3,
                    BTreeMap::from([(1, GraphValue::Integer(element_id as i64))]),
                ),
            )
            .expect("mutation"),
        ),
    )
    .expect("commit");
    LocalFragmentWorker::new(
        shard_id,
        7,
        3,
        11,
        security,
        TemporalBatchExecutor::new(store),
    )
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
