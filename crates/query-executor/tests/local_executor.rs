use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use adapter_memory::MemoryAdapter;
use adapter_rocksdb::RocksAdapter;
use query_executor::{ExecutorError, LocalExecutor, QueryRecord};
use temporal_ir::{
    DiffOperator, ExpandDirection, GraphScope, PLAN_VERSION, PlanBody, PlanError, PointOperator,
    TemporalPlan, TemporalSelector,
};
use temporal_storage::{
    CommitContext, EdgeMutation, EdgeTypeId, ElementId, ElementKind, ElementRef, GraphId, LabelId,
    PartitionId, TemporalStore, TemporalTransaction, VertexMutation,
};
use temporal_types::{CanonicalElement, GraphValue, Interval, TransactionTime, ValidTime};

#[test]
fn local_executor_matches_direct_current_as_of_diff_and_expansion_apis() {
    let store = TemporalStore::new(MemoryAdapter::new());
    seed(&store);
    let expected_diff = block_on(store.diff_vertex(vertex(1), tx(150), tx(250))).unwrap();
    let executor = LocalExecutor::new(store);

    let current = block_on(executor.execute(&TemporalPlan::point(
        scope(),
        PointOperator::VertexById(ElementId::new(1)),
        valid(5),
        TemporalSelector::Current,
        1,
    )))
    .unwrap();
    assert_eq!(current.records().len(), 1);
    let QueryRecord::Vertex(vertex_record) = &current.records()[0] else {
        panic!("expected vertex record");
    };
    assert_eq!(vertex_record.element(), vertex(1));
    assert_eq!(vertex_record.payload(), &payload("vertex-new"));

    let historical = block_on(executor.execute(&TemporalPlan::point(
        scope(),
        PointOperator::VertexById(ElementId::new(1)),
        valid(5),
        TemporalSelector::AsOf(tx(150)),
        1,
    )))
    .unwrap();
    let QueryRecord::Vertex(vertex_record) = &historical.records()[0] else {
        panic!("expected historical vertex record");
    };
    assert_eq!(vertex_record.payload(), &payload("vertex-old"));

    let historical_edge = block_on(executor.execute(&TemporalPlan::point(
        scope(),
        PointOperator::EdgeById(ElementId::new(10)),
        valid(5),
        TemporalSelector::AsOf(tx(250)),
        1,
    )))
    .unwrap();
    let QueryRecord::Edge(edge_record) = &historical_edge.records()[0] else {
        panic!("expected edge record");
    };
    assert_eq!(edge_record.element(), edge(10));
    assert_eq!(edge_record.edge_type(), EdgeTypeId::new(9));
    assert_eq!(edge_record.source(), ElementId::new(1));
    assert_eq!(edge_record.destination(), ElementId::new(2));
    assert_eq!(edge_record.payload(), &payload("edge"));

    let current_expand = block_on(executor.execute(&TemporalPlan::point(
        scope(),
        PointOperator::Expand {
            origin: ElementId::new(1),
            direction: ExpandDirection::Out,
        },
        valid(5),
        TemporalSelector::Current,
        10,
    )))
    .unwrap();
    assert!(current_expand.records().is_empty());

    let historical_both = block_on(executor.execute(&TemporalPlan::point(
        scope(),
        PointOperator::Expand {
            origin: ElementId::new(1),
            direction: ExpandDirection::Both,
        },
        valid(5),
        TemporalSelector::AsOf(tx(250)),
        10,
    )))
    .unwrap();
    assert_eq!(historical_both.records().len(), 1);
    assert!(matches!(historical_both.records()[0], QueryRecord::Edge(_)));

    let diff = block_on(executor.execute(&TemporalPlan::diff(
        scope(),
        DiffOperator::Element {
            kind: ElementKind::Vertex,
            id: ElementId::new(1),
        },
        tx(150),
        tx(250),
        10,
    )))
    .unwrap();
    assert_eq!(diff.records().len(), expected_diff.len());
    for (record, expected) in diff.records().iter().zip(&expected_diff) {
        let QueryRecord::Change(change) = record else {
            panic!("expected change record");
        };
        assert_eq!(change.element(), vertex(1));
        assert_eq!(change.change(), expected);
    }
}

#[test]
fn executor_validates_the_complete_plan_before_touching_storage() {
    let executor = LocalExecutor::new(TemporalStore::new(MemoryAdapter::new()));
    let invalid = TemporalPlan::with_version(
        PLAN_VERSION + 1,
        scope(),
        PlanBody::Point {
            operator: PointOperator::VertexById(ElementId::new(1)),
            valid_time: valid(5),
            transaction: TemporalSelector::Current,
            limit: 1,
        },
    );

    assert_eq!(
        block_on(executor.execute(&invalid)),
        Err(ExecutorError::Plan(PlanError::UnsupportedVersion {
            expected: PLAN_VERSION,
            actual: PLAN_VERSION + 1,
        }))
    );
}

#[test]
fn the_same_typed_plan_executes_against_a_rocksdb_checkpoint() {
    let source = tempfile::tempdir().unwrap();
    let checkpoint_root = tempfile::tempdir().unwrap();
    let checkpoint = checkpoint_root.path().join("query-snapshot");
    {
        let store = TemporalStore::new(RocksAdapter::open(source.path()).unwrap());
        seed(&store);
        store.adapter().checkpoint(&checkpoint).unwrap();
    }

    let executor = LocalExecutor::new(TemporalStore::new(RocksAdapter::open(&checkpoint).unwrap()));
    let plan = TemporalPlan::point(
        scope(),
        PointOperator::EdgeById(ElementId::new(10)),
        valid(5),
        TemporalSelector::AsOf(tx(250)),
        1,
    );
    let result = block_on(executor.execute(&plan)).unwrap();
    let QueryRecord::Edge(record) = &result.records()[0] else {
        panic!("expected checkpoint edge record");
    };
    assert_eq!(record.payload(), &payload("edge"));
}

#[test]
fn expansion_applies_residual_valid_time_deduplicates_both_and_limits_after_element_ordering() {
    let store = TemporalStore::new(MemoryAdapter::new());
    let lifetime = interval(1, Some(10));
    let transaction = TemporalTransaction::new()
        .with_vertex(
            VertexMutation::put(vertex(1), LabelId::new(1), lifetime, payload("v1")).unwrap(),
        )
        .with_vertex(
            VertexMutation::put(vertex(2), LabelId::new(1), lifetime, payload("v2")).unwrap(),
        )
        .with_vertex(
            VertexMutation::put(vertex(3), LabelId::new(1), lifetime, payload("v3")).unwrap(),
        )
        .with_edge(
            EdgeMutation::put(
                edge(20),
                EdgeTypeId::new(9),
                ElementId::new(1),
                ElementId::new(2),
                lifetime,
                payload("e20"),
            )
            .unwrap(),
        )
        .with_edge(
            EdgeMutation::put(
                edge(10),
                EdgeTypeId::new(9),
                ElementId::new(1),
                ElementId::new(3),
                lifetime,
                payload("e10"),
            )
            .unwrap(),
        )
        .with_edge(
            EdgeMutation::put(
                edge(5),
                EdgeTypeId::new(9),
                ElementId::new(1),
                ElementId::new(1),
                lifetime,
                payload("self"),
            )
            .unwrap(),
        );
    block_on(store.commit_transaction(context(1, 0, 100), transaction)).unwrap();
    let executor = LocalExecutor::new(store);

    let plan = |valid_time, limit| {
        TemporalPlan::point(
            scope(),
            PointOperator::Expand {
                origin: ElementId::new(1),
                direction: ExpandDirection::Both,
            },
            valid_time,
            TemporalSelector::Current,
            limit,
        )
    };
    let bounded = block_on(executor.execute(&plan(valid(5), 2))).unwrap();
    let ids = bounded
        .records()
        .iter()
        .map(|record| match record {
            QueryRecord::Edge(edge) => edge.element().id(),
            _ => panic!("expected only edge records"),
        })
        .collect::<Vec<_>>();
    assert_eq!(ids, vec![ElementId::new(5), ElementId::new(10)]);

    let outside_valid_time = block_on(executor.execute(&plan(valid(15), 10))).unwrap();
    assert!(outside_valid_time.records().is_empty());
}

fn seed<A: storage_api::StorageAdapter>(store: &TemporalStore<A>) {
    let initial = TemporalTransaction::new()
        .with_vertex(
            VertexMutation::put(
                vertex(1),
                LabelId::new(1),
                interval(1, Some(10)),
                payload("vertex-old"),
            )
            .unwrap(),
        )
        .with_vertex(
            VertexMutation::put(
                vertex(2),
                LabelId::new(1),
                interval(1, Some(10)),
                payload("destination"),
            )
            .unwrap(),
        )
        .with_edge(
            EdgeMutation::put(
                edge(10),
                EdgeTypeId::new(9),
                ElementId::new(1),
                ElementId::new(2),
                interval(2, Some(9)),
                payload("edge"),
            )
            .unwrap(),
        );
    block_on(store.commit_transaction(context(1, 0, 100), initial)).unwrap();
    block_on(
        store.commit_vertex(
            context(2, 100, 200),
            VertexMutation::put(
                vertex(1),
                LabelId::new(1),
                interval(4, Some(7)),
                payload("vertex-new"),
            )
            .unwrap(),
        ),
    )
    .unwrap();
    block_on(
        store.commit_edge(
            context(3, 200, 300),
            EdgeMutation::delete(
                edge(10),
                EdgeTypeId::new(9),
                ElementId::new(1),
                ElementId::new(2),
                interval(i64::MIN, None),
            )
            .unwrap(),
        ),
    )
    .unwrap();
}

fn scope() -> GraphScope {
    GraphScope::new(graph(), partition())
}

fn graph() -> GraphId {
    GraphId::new(1)
}

fn partition() -> PartitionId {
    PartitionId::new(0)
}

fn vertex(id: u128) -> ElementRef {
    ElementRef::vertex(graph(), partition(), ElementId::new(id))
}

fn edge(id: u128) -> ElementRef {
    ElementRef::edge(graph(), partition(), ElementId::new(id))
}

fn context(log_index: u64, read: i64, commit: i64) -> CommitContext {
    CommitContext::new(3, log_index, u128::from(log_index), tx(read), tx(commit))
}

fn tx(value: i64) -> TransactionTime {
    TransactionTime::new(value, 0)
}

fn valid(value: i64) -> ValidTime {
    ValidTime::from_micros(value)
}

fn interval(start: i64, end: Option<i64>) -> Interval<ValidTime> {
    Interval::new(valid(start), end.map(valid)).unwrap()
}

fn payload(value: &str) -> CanonicalElement {
    CanonicalElement::new(
        1,
        BTreeMap::from([(1, GraphValue::String(value.to_owned()))]),
    )
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
