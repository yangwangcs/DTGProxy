use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use adapter_memory::MemoryAdapter;
use adapter_rocksdb::RocksAdapter;
use storage_api::StorageAdapter;
use temporal_storage::{
    CommitContext, EdgeMutation, EdgeTypeId, ElementId, ElementRef, GraphId, LabelId, PartitionId,
    TemporalStore, TemporalStoreError, TemporalTransaction, VertexMutation,
};
use temporal_types::{CanonicalElement, GraphValue, Interval, TransactionTime, ValidTime};

#[test]
fn memory_adapter_satisfies_the_temporal_transaction_contract() {
    run_contract(MemoryAdapter::new());
}

#[test]
fn rocksdb_adapter_satisfies_the_temporal_transaction_contract() {
    let directory = tempfile::tempdir().unwrap();
    run_contract(RocksAdapter::open(directory.path()).unwrap());
}

fn run_contract<A: StorageAdapter>(adapter: A) {
    let store = TemporalStore::new(adapter);
    let transaction = TemporalTransaction::new()
        .with_vertex(
            VertexMutation::put(
                vertex(1),
                LabelId::new(1),
                interval(1, Some(10)),
                payload("source"),
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

    let first = block_on(store.commit_transaction(context(1, 0, 100), transaction.clone()))
        .expect("atomic multi-element commit");
    let replay = block_on(store.commit_transaction(context(1, 0, 100), transaction))
        .expect("deterministic retry");
    assert!(!first.duplicate);
    assert!(replay.duplicate);
    assert_eq!(
        block_on(store.edge_current(edge(10), valid(5))).unwrap(),
        Some(payload("edge"))
    );

    let invalid = TemporalTransaction::new().with_vertex(
        VertexMutation::delete(vertex(1), LabelId::new(1), interval(4, Some(7))).unwrap(),
    );
    assert_eq!(
        block_on(store.commit_transaction(context(2, 100, 200), invalid)),
        Err(TemporalStoreError::EndpointStillReferenced {
            vertex: vertex(1),
            edge: edge(10),
        })
    );
    assert_eq!(store.adapter().applied_log_index().unwrap(), 1);
    assert_eq!(
        block_on(store.vertex_current(vertex(1), valid(5))).unwrap(),
        Some(payload("source"))
    );
    assert_eq!(
        block_on(store.edge_current(edge(10), valid(5))).unwrap(),
        Some(payload("edge"))
    );
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
    CommitContext::new(
        3,
        log_index,
        u128::from(log_index),
        TransactionTime::new(read, 0),
        TransactionTime::new(commit, 0),
    )
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
