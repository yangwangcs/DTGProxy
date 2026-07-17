use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use adapter_memory::MemoryAdapter;
use query_executor::{QueryRecord, ShardQueryError, ShardQueryExecutor};
use raft_command::{ApplyPreparedV1, CommandBodyV1, CommandEnvelopeV1};
use shard_runtime::{InProcessShardGroup, ReadBarrierError};
use storage_api::StorageAdapter;
use temporal_ir::{DiffOperator, GraphScope, PointOperator, TemporalPlan, TemporalSelector};
use temporal_storage::{
    ElementId, ElementKind, ElementRef, GraphId, LabelId, PartitionId, PrepareContext,
    TemporalStore, TemporalTransaction, VertexMutation,
};
use temporal_types::{CanonicalElement, GraphValue, Interval, TransactionTime, ValidTime};

const VOTERS: &[u64] = &[1, 2, 3];

#[test]
fn temporal_ir_executes_only_after_leader_or_follower_read_authorization() {
    let mut group = block_on(InProcessShardGroup::new(7, 9, VOTERS)).unwrap();
    block_on(group.elect(1)).unwrap();
    let planner = TemporalStore::new(MemoryAdapter::new());
    block_on(propose_vertex(
        &planner,
        &mut group,
        1,
        101,
        0,
        100,
        payload("before"),
    ));
    block_on(propose_vertex(
        &planner,
        &mut group,
        2,
        102,
        100,
        200,
        payload("after"),
    ));
    block_on(group.advance_closed_timestamp(ts(200), 20)).unwrap();

    let current = point_plan(TemporalSelector::Current);
    let leader_result = block_on(ShardQueryExecutor::execute_leader(
        &mut group, 1, 9, &current, 5,
    ))
    .unwrap();
    assert_eq!(vertex_payload(&leader_result), payload("after"));

    let historical = point_plan(TemporalSelector::AsOf(ts(150)));
    let proof = block_on(group.issue_follower_read_proof(9, 5)).unwrap();
    let follower_result = block_on(ShardQueryExecutor::execute_follower(
        &group,
        2,
        9,
        &proof,
        &historical,
    ))
    .unwrap();
    assert_eq!(vertex_payload(&follower_result), payload("before"));

    let diff = TemporalPlan::diff(
        GraphScope::new(GraphId::new(1), PartitionId::new(7)),
        DiffOperator::Element {
            kind: ElementKind::Vertex,
            id: ElementId::new(1),
        },
        ts(150),
        ts(200),
        10,
    );
    let diff_result = block_on(ShardQueryExecutor::execute_follower(
        &group, 2, 9, &proof, &diff,
    ))
    .unwrap();
    assert!(matches!(diff_result.records(), [QueryRecord::Change(_)]));

    assert!(matches!(
        block_on(ShardQueryExecutor::execute_follower(
            &group, 2, 9, &proof, &current,
        )),
        Err(ShardQueryError::FollowerCurrentUnsupported)
    ));
    let future = point_plan(TemporalSelector::AsOf(ts(201)));
    assert!(matches!(
        block_on(ShardQueryExecutor::execute_follower(
            &group, 2, 9, &proof, &future,
        )),
        Err(ShardQueryError::Barrier(ReadBarrierError::NotReady { .. }))
    ));
}

#[test]
fn plan_partition_is_fenced_before_read_index_or_adapter_access() {
    let mut group = block_on(InProcessShardGroup::new(7, 9, VOTERS)).unwrap();
    block_on(group.elect(1)).unwrap();
    let wrong_partition = TemporalPlan::point(
        GraphScope::new(GraphId::new(1), PartitionId::new(8)),
        PointOperator::VertexById(ElementId::new(1)),
        valid(5),
        TemporalSelector::Current,
        1,
    );
    assert!(matches!(
        block_on(ShardQueryExecutor::execute_leader(
            &mut group,
            1,
            9,
            &wrong_partition,
            5,
        )),
        Err(ShardQueryError::ShardMismatch {
            expected: 7,
            actual: 8
        })
    ));
}

async fn propose_vertex(
    planner: &TemporalStore<MemoryAdapter>,
    group: &mut InProcessShardGroup,
    planner_index: u64,
    request_id: u128,
    read: i64,
    commit: i64,
    value: CanonicalElement,
) {
    let prepared = planner
        .prepare_transaction(
            PrepareContext::new(7, request_id + 1_000, ts(read), ts(commit)),
            TemporalTransaction::new().with_vertex(
                VertexMutation::put(vertex(), LabelId::new(1), interval(1, Some(10)), value)
                    .unwrap(),
            ),
        )
        .await
        .unwrap();
    planner
        .adapter()
        .apply_committed(prepared.clone().commit_at(planner_index))
        .await
        .unwrap();
    let command = CommandEnvelopeV1::new(
        7,
        9,
        request_id,
        CommandBodyV1::ApplyPrepared(ApplyPreparedV1 {
            commit_ts: ts(commit),
            batch: prepared,
        }),
    )
    .encode()
    .unwrap();
    group.propose_and_wait(command, 20).await.unwrap();
}

fn point_plan(selector: TemporalSelector) -> TemporalPlan {
    TemporalPlan::point(
        GraphScope::new(GraphId::new(1), PartitionId::new(7)),
        PointOperator::VertexById(ElementId::new(1)),
        valid(5),
        selector,
        1,
    )
}

fn vertex() -> ElementRef {
    ElementRef::vertex(GraphId::new(1), PartitionId::new(7), ElementId::new(1))
}

fn vertex_payload(result: &query_executor::QueryResult) -> CanonicalElement {
    let QueryRecord::Vertex(record) = &result.records()[0] else {
        panic!("expected one vertex record");
    };
    record.payload().clone()
}

fn payload(value: &str) -> CanonicalElement {
    CanonicalElement::new(
        1,
        BTreeMap::from([(1, GraphValue::String(value.to_owned()))]),
    )
}

fn ts(value: i64) -> TransactionTime {
    TransactionTime::new(value, 0)
}

fn valid(value: i64) -> ValidTime {
    ValidTime::from_micros(value)
}

fn interval(start: i64, end: Option<i64>) -> Interval<ValidTime> {
    Interval::new(valid(start), end.map(valid)).unwrap()
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
