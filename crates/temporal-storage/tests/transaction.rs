use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use adapter_memory::MemoryAdapter;
use storage_api::{AdapterError, StorageAdapter};
use temporal_storage::{
    CommitContext, EdgeMutation, EdgeTypeId, ElementId, ElementRef, GraphId, LabelId, PartitionId,
    TemporalStore, TemporalStoreError, TemporalTransaction, VertexMutation,
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

fn vertex(id: u128) -> ElementRef {
    ElementRef::vertex(graph(), partition(), ElementId::new(id))
}

fn edge(id: u128) -> ElementRef {
    ElementRef::edge(graph(), partition(), ElementId::new(id))
}

fn payload(value: &str) -> CanonicalElement {
    CanonicalElement::new(
        1,
        BTreeMap::from([(1, GraphValue::String(value.to_owned()))]),
    )
}

fn context(log_index: u64, read: i64, commit: i64) -> CommitContext {
    CommitContext::new(3, log_index, u128::from(log_index), tx(read), tx(commit))
}

#[test]
fn one_batch_atomically_commits_two_vertices_an_edge_and_both_adjacencies() {
    let store = TemporalStore::new(MemoryAdapter::new());
    let transaction = TemporalTransaction::new()
        .with_vertex(
            VertexMutation::put(
                vertex(1),
                LabelId::new(1),
                interval(1, Some(10)),
                payload("v1"),
            )
            .unwrap(),
        )
        .with_vertex(
            VertexMutation::put(
                vertex(2),
                LabelId::new(1),
                interval(1, Some(10)),
                payload("v2"),
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
                payload("e"),
            )
            .unwrap(),
        );

    let receipt = block_on(store.commit_transaction(context(1, 0, 100), transaction)).unwrap();

    assert_eq!(receipt.applied_log_index, 1);
    assert_eq!(store.adapter().applied_log_index().unwrap(), 1);
    assert_eq!(
        block_on(store.vertex_current(vertex(1), valid(5))).unwrap(),
        Some(payload("v1"))
    );
    assert_eq!(
        block_on(store.vertex_current(vertex(2), valid(5))).unwrap(),
        Some(payload("v2"))
    );
    assert_eq!(
        block_on(store.edge_current(edge(10), valid(5))).unwrap(),
        Some(payload("e"))
    );
    assert_eq!(
        block_on(store.expand_out_current(graph(), partition(), ElementId::new(1), valid(5)))
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        block_on(store.expand_in_current(graph(), partition(), ElementId::new(2), valid(5)))
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn endpoint_validation_failure_rolls_back_every_staged_element() {
    let store = TemporalStore::new(MemoryAdapter::new());
    let transaction = TemporalTransaction::new()
        .with_vertex(
            VertexMutation::put(
                vertex(1),
                LabelId::new(1),
                interval(1, Some(10)),
                payload("v1"),
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
                payload("e"),
            )
            .unwrap(),
        );

    assert_eq!(
        block_on(store.commit_transaction(context(1, 0, 100), transaction)),
        Err(TemporalStoreError::EndpointNotPresent { vertex: vertex(2) })
    );
    assert_eq!(store.adapter().applied_log_index().unwrap(), 0);
    assert_eq!(
        block_on(store.vertex_current(vertex(1), valid(5))).unwrap(),
        None
    );
    assert_eq!(
        block_on(store.edge_current(edge(10), valid(5))).unwrap(),
        None
    );
}

#[test]
fn endpoint_projection_must_cover_the_complete_edge_valid_interval() {
    let store = TemporalStore::new(MemoryAdapter::new());
    let vertices = TemporalTransaction::new()
        .with_vertex(
            VertexMutation::put(
                vertex(1),
                LabelId::new(1),
                interval(1, Some(5)),
                payload("v1"),
            )
            .unwrap(),
        )
        .with_vertex(
            VertexMutation::put(
                vertex(2),
                LabelId::new(1),
                interval(1, Some(5)),
                payload("v2"),
            )
            .unwrap(),
        );
    block_on(store.commit_transaction(context(1, 0, 100), vertices)).unwrap();

    let edge = TemporalTransaction::new().with_edge(
        EdgeMutation::put(
            edge(10),
            EdgeTypeId::new(9),
            ElementId::new(1),
            ElementId::new(2),
            interval(2, Some(9)),
            payload("e"),
        )
        .unwrap(),
    );
    assert_eq!(
        block_on(store.commit_transaction(context(2, 100, 200), edge)),
        Err(TemporalStoreError::EndpointNotPresent { vertex: vertex(1) })
    );
    assert_eq!(store.adapter().applied_log_index().unwrap(), 1);
}

#[test]
fn transaction_retry_is_byte_deterministic_and_duplicate_elements_are_rejected() {
    let store = TemporalStore::new(MemoryAdapter::new());
    let transaction = TemporalTransaction::new().with_vertex(
        VertexMutation::put(vertex(1), LabelId::new(1), interval(1, None), payload("v1")).unwrap(),
    );

    let first =
        block_on(store.commit_transaction(context(1, 0, 100), transaction.clone())).unwrap();
    let replay = block_on(store.commit_transaction(context(1, 0, 100), transaction)).unwrap();
    assert!(!first.duplicate);
    assert!(replay.duplicate);

    let duplicate = TemporalTransaction::new()
        .with_vertex(
            VertexMutation::put(
                vertex(1),
                LabelId::new(1),
                interval(2, Some(3)),
                payload("a"),
            )
            .unwrap(),
        )
        .with_vertex(
            VertexMutation::put(
                vertex(1),
                LabelId::new(1),
                interval(4, Some(5)),
                payload("b"),
            )
            .unwrap(),
        );
    assert_eq!(
        block_on(store.commit_transaction(context(2, 100, 200), duplicate)),
        Err(TemporalStoreError::DuplicateElementOperation { element: vertex(1) })
    );
    assert_eq!(store.adapter().applied_log_index().unwrap(), 1);
}

#[test]
fn transaction_snapshot_rejects_overlap_but_preserves_disjoint_later_writes() {
    let store = TemporalStore::new(MemoryAdapter::new());
    let label = LabelId::new(1);
    block_on(
        store.commit_transaction(
            context(1, 0, 100),
            TemporalTransaction::new().with_vertex(
                VertexMutation::put(vertex(1), label, interval(1, Some(10)), payload("initial"))
                    .unwrap(),
            ),
        ),
    )
    .unwrap();
    block_on(store.commit_transaction(
        context(2, 100, 200),
        TemporalTransaction::new().with_vertex(
            VertexMutation::put(vertex(1), label, interval(2, Some(4)), payload("later")).unwrap(),
        ),
    ))
    .unwrap();

    let overlapping = TemporalTransaction::new().with_vertex(
        VertexMutation::put(
            vertex(1),
            label,
            interval(3, Some(5)),
            payload("stale-overlap"),
        )
        .unwrap(),
    );
    assert_eq!(
        block_on(store.commit_transaction(context(3, 100, 300), overlapping)),
        Err(TemporalStoreError::WriteConflict)
    );

    let disjoint = TemporalTransaction::new().with_vertex(
        VertexMutation::put(
            vertex(1),
            label,
            interval(20, Some(30)),
            payload("stale-disjoint"),
        )
        .unwrap(),
    );
    block_on(store.commit_transaction(context(3, 100, 300), disjoint)).unwrap();
    assert_eq!(
        block_on(store.vertex_current(vertex(1), valid(3))).unwrap(),
        Some(payload("later"))
    );
    assert_eq!(
        block_on(store.vertex_current(vertex(1), valid(25))).unwrap(),
        Some(payload("stale-disjoint"))
    );
}

#[test]
fn transaction_preparation_rejects_log_gaps_before_reading_or_staging_the_write_set() {
    let store = TemporalStore::new(MemoryAdapter::new());
    let edge_without_endpoints = TemporalTransaction::new().with_edge(
        EdgeMutation::put(
            edge(10),
            EdgeTypeId::new(9),
            ElementId::new(1),
            ElementId::new(2),
            interval(1, Some(10)),
            payload("edge"),
        )
        .unwrap(),
    );

    assert_eq!(
        block_on(store.commit_transaction(context(2, 0, 100), edge_without_endpoints)),
        Err(TemporalStoreError::Adapter(
            AdapterError::NonContiguousLogIndex {
                expected: 1,
                actual: 2,
            }
        ))
    );
    assert_eq!(store.adapter().applied_log_index().unwrap(), 0);
    assert_eq!(
        block_on(store.edge_current(edge(10), valid(5))).unwrap(),
        None
    );
}

#[test]
fn endpoint_deletion_requires_incident_edges_to_be_rewritten_in_the_same_transaction() {
    let store = TemporalStore::new(MemoryAdapter::new());
    let label = LabelId::new(1);
    let initial = TemporalTransaction::new()
        .with_vertex(
            VertexMutation::put(vertex(1), label, interval(1, Some(10)), payload("v1")).unwrap(),
        )
        .with_vertex(
            VertexMutation::put(vertex(2), label, interval(1, Some(10)), payload("v2")).unwrap(),
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

    let dangling = TemporalTransaction::new()
        .with_vertex(VertexMutation::delete(vertex(1), label, interval(4, Some(7))).unwrap());
    assert_eq!(
        block_on(store.commit_transaction(context(2, 100, 200), dangling)),
        Err(TemporalStoreError::EndpointStillReferenced {
            vertex: vertex(1),
            edge: edge(10),
        })
    );
    assert_eq!(store.adapter().applied_log_index().unwrap(), 1);
    assert_eq!(
        block_on(store.vertex_current(vertex(1), valid(5))).unwrap(),
        Some(payload("v1"))
    );

    let coordinated = TemporalTransaction::new()
        .with_vertex(VertexMutation::delete(vertex(1), label, interval(4, Some(7))).unwrap())
        .with_edge(
            EdgeMutation::delete(
                edge(10),
                EdgeTypeId::new(9),
                ElementId::new(1),
                ElementId::new(2),
                interval(4, Some(7)),
            )
            .unwrap(),
        );
    block_on(store.commit_transaction(context(2, 100, 200), coordinated)).unwrap();
    assert_eq!(
        block_on(store.vertex_current(vertex(1), valid(5))).unwrap(),
        None
    );
    assert_eq!(
        block_on(store.edge_current(edge(10), valid(5))).unwrap(),
        None
    );
    assert_eq!(
        block_on(store.edge_current(edge(10), valid(3))).unwrap(),
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
