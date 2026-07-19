use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use adapter_memory::MemoryAdapter;
use analytics_api::VertexId;
use analytics_runtime::project_snapshot;
use temporal_storage::{
    CommitContext, EdgeMutation, EdgeTypeId, ElementId, ElementRef, GraphId, LabelId, PartitionId,
    TemporalStore, VertexMutation,
};
use temporal_types::{CanonicalElement, GraphValue, Interval, TransactionTime, ValidTime};

#[test]
fn projects_one_fenced_storage_snapshot_with_numeric_weights() {
    let store = TemporalStore::new(MemoryAdapter::new());
    seed(&store);

    let before_edge = block_on(project_snapshot(
        &store,
        GraphId::new(7),
        ValidTime::from_micros(5),
        TransactionTime::new(150, 0),
        true,
        Some(9),
    ))
    .expect("projection");
    assert_eq!(before_edge.vertices().len(), 2);
    assert!(before_edge.edges().is_empty());

    let graph = block_on(project_snapshot(
        &store,
        GraphId::new(7),
        ValidTime::from_micros(5),
        TransactionTime::new(250, 0),
        true,
        Some(9),
    ))
    .expect("projection");
    assert_eq!(graph.outgoing(VertexId::new(1)).len(), 1);
    assert_eq!(graph.outgoing(VertexId::new(1))[0].weight(), 2.5);
}

fn seed(store: &TemporalStore<MemoryAdapter>) {
    for id in [1_u128, 2] {
        block_on(
            store.commit_vertex(
                CommitContext::new(0, id as u64, id, tx(0), tx(100)),
                VertexMutation::put(
                    ElementRef::vertex(GraphId::new(7), PartitionId::new(0), ElementId::new(id)),
                    LabelId::new(1),
                    interval(),
                    CanonicalElement::new(1, BTreeMap::new()),
                )
                .expect("vertex"),
            ),
        )
        .expect("commit vertex");
    }
    block_on(
        store.commit_edge(
            CommitContext::new(0, 3, 3, tx(100), tx(200)),
            EdgeMutation::put(
                ElementRef::edge(GraphId::new(7), PartitionId::new(0), ElementId::new(3)),
                EdgeTypeId::new(1),
                ElementId::new(1),
                ElementId::new(2),
                interval(),
                CanonicalElement::new(
                    1,
                    BTreeMap::from([(9, GraphValue::FloatBits(2.5_f64.to_bits()))]),
                ),
            )
            .expect("edge"),
        ),
    )
    .expect("commit edge");
}

fn interval() -> Interval<ValidTime> {
    Interval::new(ValidTime::from_micros(1), None).expect("interval")
}

fn tx(value: i64) -> TransactionTime {
    TransactionTime::new(value, 0)
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
