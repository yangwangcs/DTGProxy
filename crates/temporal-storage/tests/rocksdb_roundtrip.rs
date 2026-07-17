use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use adapter_rocksdb::RocksAdapter;
use temporal_storage::{
    CommitContext, EdgeMutation, EdgeTypeId, ElementId, ElementRef, GraphId, LabelId, PartitionId,
    TemporalChangeKind, TemporalStore, TemporalTransaction, VertexMutation,
};
use temporal_types::{CanonicalElement, GraphValue, Interval, TransactionTime, ValidTime};

fn tx(value: i64) -> TransactionTime {
    TransactionTime::new(value, 0)
}

fn valid(value: i64) -> ValidTime {
    ValidTime::from_micros(value)
}

fn interval(start: i64, end: Option<i64>) -> Interval<ValidTime> {
    Interval::new(valid(start), end.map(valid)).unwrap()
}

fn graph() -> GraphId {
    GraphId::new(1)
}

fn partition() -> PartitionId {
    PartitionId::new(0)
}

fn vertex() -> ElementRef {
    ElementRef::vertex(graph(), partition(), ElementId::new(7))
}

fn edge() -> ElementRef {
    ElementRef::edge(graph(), partition(), ElementId::new(70))
}

fn payload(name: &str) -> CanonicalElement {
    CanonicalElement::new(
        5,
        BTreeMap::from([
            (1, GraphValue::String(name.to_owned())),
            (2, GraphValue::Bytes(vec![0, 255, 7])),
            (3, GraphValue::TimestampMicros(123_456)),
            (4, GraphValue::FloatBits(f64::NAN.to_bits())),
        ]),
    )
}

fn context(log_index: u64, read: i64, commit: i64) -> CommitContext {
    CommitContext::new(3, log_index, u128::from(log_index), tx(read), tx(commit))
}

#[test]
fn typed_temporal_graph_survives_rocksdb_restart_and_checkpoint() {
    let source_directory = tempfile::tempdir().unwrap();
    let checkpoint_root = tempfile::tempdir().unwrap();
    let checkpoint_path = checkpoint_root.path().join("snapshot");
    let label = LabelId::new(1);

    {
        let store = TemporalStore::new(RocksAdapter::open(source_directory.path()).unwrap());
        block_on(
            store.commit_vertex(
                context(1, 0, 100),
                VertexMutation::put(vertex(), label, interval(1, Some(10)), payload("vertex-a"))
                    .unwrap(),
            ),
        )
        .unwrap();
        block_on(
            store.commit_vertex(
                context(2, 100, 200),
                VertexMutation::put(vertex(), label, interval(4, Some(7)), payload("vertex-b"))
                    .unwrap(),
            ),
        )
        .unwrap();
        block_on(
            store.commit_vertex(
                context(3, 0, 250),
                VertexMutation::put(
                    ElementRef::vertex(graph(), partition(), ElementId::new(8)),
                    label,
                    interval(1, Some(10)),
                    payload("vertex-destination"),
                )
                .unwrap(),
            ),
        )
        .unwrap();
        block_on(
            store.commit_edge(
                context(4, 250, 300),
                EdgeMutation::put(
                    edge(),
                    EdgeTypeId::new(9),
                    ElementId::new(7),
                    ElementId::new(8),
                    interval(2, Some(9)),
                    payload("edge"),
                )
                .unwrap(),
            ),
        )
        .unwrap();
        block_on(
            store.commit_edge(
                context(5, 300, 350),
                EdgeMutation::delete(
                    edge(),
                    EdgeTypeId::new(9),
                    ElementId::new(7),
                    ElementId::new(8),
                    interval(4, Some(6)),
                )
                .unwrap(),
            ),
        )
        .unwrap();
    }

    let store = TemporalStore::new(RocksAdapter::open(source_directory.path()).unwrap());
    assert_eq!(
        block_on(store.vertex_current(vertex(), valid(5))).unwrap(),
        Some(payload("vertex-b"))
    );
    assert_eq!(
        block_on(store.vertex_as_of(vertex(), valid(5), tx(150))).unwrap(),
        Some(payload("vertex-a"))
    );
    assert_eq!(
        block_on(store.edge_current(edge(), valid(5))).unwrap(),
        None
    );
    assert_eq!(
        block_on(store.edge_as_of(edge(), valid(5), tx(325))).unwrap(),
        Some(payload("edge"))
    );
    assert_eq!(
        block_on(store.expand_out_current(graph(), partition(), ElementId::new(7), valid(7)))
            .unwrap()
            .len(),
        1
    );
    let diff = block_on(store.diff_vertex(vertex(), tx(150), tx(250))).unwrap();
    assert_eq!(diff.len(), 1);
    assert!(matches!(diff[0].kind(), TemporalChangeKind::Changed { .. }));
    let edge_diff = block_on(store.diff_edge(edge(), tx(325), tx(375))).unwrap();
    assert_eq!(edge_diff.len(), 1);
    assert!(matches!(
        edge_diff[0].kind(),
        TemporalChangeKind::Removed { .. }
    ));

    store.adapter().checkpoint(&checkpoint_path).unwrap();
    let coordinated_delete = TemporalTransaction::new()
        .with_vertex(VertexMutation::delete(vertex(), label, interval(4, Some(7))).unwrap())
        .with_edge(
            EdgeMutation::delete(
                edge(),
                EdgeTypeId::new(9),
                ElementId::new(7),
                ElementId::new(8),
                interval(4, Some(7)),
            )
            .unwrap(),
        );
    block_on(store.commit_transaction(context(6, 350, 400), coordinated_delete)).unwrap();
    assert_eq!(
        block_on(store.vertex_current(vertex(), valid(5))).unwrap(),
        None
    );

    let checkpoint = TemporalStore::new(RocksAdapter::open(&checkpoint_path).unwrap());
    assert_eq!(
        block_on(checkpoint.vertex_current(vertex(), valid(5))).unwrap(),
        Some(payload("vertex-b"))
    );
    assert_eq!(
        block_on(checkpoint.edge_current(edge(), valid(7))).unwrap(),
        Some(payload("edge"))
    );
}

struct NoopWake;

impl Wake for NoopWake {
    fn wake(self: Arc<Self>) {}
}

fn block_on<F>(future: F) -> F::Output
where
    F: Future,
{
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
