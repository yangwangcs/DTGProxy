use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use adapter_memory::MemoryAdapter;
use temporal_storage::{
    CommitContext, EdgeMutation, EdgeTypeId, ElementId, ElementRef, GraphId, LabelId, PartitionId,
    TemporalStore, TemporalStoreError, VertexMutation,
};
use temporal_types::{CanonicalElement, GraphValue, Interval, TransactionTime, ValidTime};

#[test]
fn scans_vertex_segments_at_a_fenced_transaction_snapshot_and_clips_to_the_window() {
    let store = TemporalStore::new(MemoryAdapter::new());
    let vertex = ElementRef::vertex(GraphId::new(7), PartitionId::new(0), ElementId::new(1));
    let label = LabelId::new(3);
    block_on(store.commit_vertex(
        context(1, 100),
        VertexMutation::put(vertex, label, valid(1, 10), payload("before")).unwrap(),
    ))
    .unwrap();
    block_on(store.commit_vertex(
        context(2, 200),
        VertexMutation::put(vertex, label, valid(4, 7), payload("corrected")).unwrap(),
    ))
    .unwrap();

    let segments = block_on(store.scan_vertex_segments_as_of(
        GraphId::new(7),
        valid(3, 8),
        TransactionTime::new(250, 0),
    ))
    .unwrap();
    assert_eq!(segments.len(), 3);
    assert_eq!(segments[0].element(), vertex);
    assert_eq!(segments[0].label(), label);
    assert_eq!(segments[0].valid(), valid(3, 4));
    assert_eq!(segments[0].payload(), &payload("before"));
    assert_eq!(segments[1].valid(), valid(4, 7));
    assert_eq!(segments[1].payload(), &payload("corrected"));
    assert_eq!(segments[2].valid(), valid(7, 8));
    assert_eq!(segments[2].payload(), &payload("before"));
}

#[test]
fn scans_edge_segments_at_a_fenced_transaction_snapshot_and_clips_to_the_window() {
    let store = TemporalStore::new(MemoryAdapter::new());
    let edge = ElementRef::edge(GraphId::new(7), PartitionId::new(0), ElementId::new(9));
    for vertex_id in [1, 2] {
        block_on(
            store.commit_vertex(
                context(vertex_id, 50),
                VertexMutation::put(
                    ElementRef::vertex(
                        GraphId::new(7),
                        PartitionId::new(0),
                        ElementId::new(u128::from(vertex_id)),
                    ),
                    LabelId::new(3),
                    Interval::forever_from(ValidTime::from_micros(1)),
                    payload("endpoint"),
                )
                .unwrap(),
            ),
        )
        .unwrap();
    }
    block_on(
        store.commit_edge(
            context(3, 100),
            EdgeMutation::put(
                edge,
                EdgeTypeId::new(4),
                ElementId::new(1),
                ElementId::new(2),
                valid(1, 10),
                payload("before"),
            )
            .unwrap(),
        ),
    )
    .unwrap();
    block_on(
        store.commit_edge(
            context(4, 200),
            EdgeMutation::put(
                edge,
                EdgeTypeId::new(4),
                ElementId::new(1),
                ElementId::new(2),
                valid(4, 7),
                payload("corrected"),
            )
            .unwrap(),
        ),
    )
    .unwrap();

    let segments = block_on(store.scan_edge_segments_as_of(
        GraphId::new(7),
        valid(3, 8),
        TransactionTime::new(250, 0),
    ))
    .unwrap();

    assert_eq!(segments.len(), 3);
    assert_eq!(segments[0].element(), edge);
    assert_eq!(segments[0].edge_type(), EdgeTypeId::new(4));
    assert_eq!(segments[0].source_ref().id(), ElementId::new(1));
    assert_eq!(segments[0].destination_ref().id(), ElementId::new(2));
    assert_eq!(segments[0].valid(), valid(3, 4));
    assert_eq!(segments[0].payload(), &payload("before"));
    assert_eq!(segments[1].valid(), valid(4, 7));
    assert_eq!(segments[1].payload(), &payload("corrected"));
    assert_eq!(segments[2].valid(), valid(7, 8));
    assert_eq!(segments[2].payload(), &payload("before"));
}

#[test]
fn bounded_segment_scans_enforce_segment_and_payload_byte_limits() {
    let store = TemporalStore::new(MemoryAdapter::new());
    let vertex = ElementRef::vertex(GraphId::new(7), PartitionId::new(0), ElementId::new(1));
    block_on(store.commit_vertex(
        context(1, 100),
        VertexMutation::put(vertex, LabelId::new(3), valid(1, 10), payload("before")).unwrap(),
    ))
    .unwrap();
    block_on(store.commit_vertex(
        context(2, 200),
        VertexMutation::put(vertex, LabelId::new(3), valid(4, 7), payload("corrected")).unwrap(),
    ))
    .unwrap();

    assert_eq!(
        block_on(store.scan_vertex_segments_as_of_bounded(
            GraphId::new(7),
            valid(3, 8),
            TransactionTime::new(250, 0),
            2,
            1 << 20,
        )),
        Err(TemporalStoreError::ScanEntryLimit)
    );
    assert_eq!(
        block_on(store.scan_vertex_segments_as_of_bounded(
            GraphId::new(7),
            valid(3, 8),
            TransactionTime::new(250, 0),
            16,
            1,
        )),
        Err(TemporalStoreError::ScanByteLimit)
    );
}

#[test]
fn bounded_vertex_segment_scan_limits_non_intersecting_identity_reads() {
    let store = TemporalStore::new(MemoryAdapter::new());
    for id in 1_u64..=3 {
        let vertex = ElementRef::vertex(
            GraphId::new(7),
            PartitionId::new(0),
            ElementId::new(u128::from(id)),
        );
        block_on(store.commit_vertex(
            context(id, 100 + i64::try_from(id).unwrap()),
            VertexMutation::put(vertex, LabelId::new(3), valid(1, 2), payload("outside")).unwrap(),
        ))
        .unwrap();
    }

    assert_eq!(
        block_on(store.scan_vertex_segments_as_of_bounded(
            GraphId::new(7),
            valid(10, 20),
            TransactionTime::new(200, 0),
            2,
            1 << 20,
        )),
        Err(TemporalStoreError::ScanEntryLimit)
    );
    assert_eq!(
        block_on(store.scan_vertex_segments_as_of_bounded(
            GraphId::new(7),
            valid(10, 20),
            TransactionTime::new(200, 0),
            8,
            1,
        )),
        Err(TemporalStoreError::ScanByteLimit)
    );
}

#[test]
fn bounded_edge_segment_scan_limits_non_intersecting_identity_reads() {
    let store = TemporalStore::new(MemoryAdapter::new());
    for id in 1_u64..=2 {
        block_on(
            store.commit_vertex(
                context(id, 100 + i64::try_from(id).unwrap()),
                VertexMutation::put(
                    ElementRef::vertex(
                        GraphId::new(7),
                        PartitionId::new(0),
                        ElementId::new(u128::from(id)),
                    ),
                    LabelId::new(3),
                    valid(1, 30),
                    payload("endpoint"),
                )
                .unwrap(),
            ),
        )
        .unwrap();
    }
    for edge_id in 3_u64..=4 {
        block_on(
            store.commit_edge(
                context(edge_id, 100 + i64::try_from(edge_id).unwrap()),
                EdgeMutation::put(
                    ElementRef::edge(
                        GraphId::new(7),
                        PartitionId::new(0),
                        ElementId::new(u128::from(edge_id)),
                    ),
                    EdgeTypeId::new(4),
                    ElementId::new(1),
                    ElementId::new(2),
                    valid(1, 2),
                    payload("outside"),
                )
                .unwrap(),
            ),
        )
        .unwrap();
    }

    assert_eq!(
        block_on(store.scan_edge_segments_as_of_bounded(
            GraphId::new(7),
            valid(10, 20),
            TransactionTime::new(200, 0),
            1,
            1 << 20,
        )),
        Err(TemporalStoreError::ScanEntryLimit)
    );
    assert_eq!(
        block_on(store.scan_edge_segments_as_of_bounded(
            GraphId::new(7),
            valid(10, 20),
            TransactionTime::new(200, 0),
            8,
            1,
        )),
        Err(TemporalStoreError::ScanByteLimit)
    );
}

fn context(log_index: u64, commit: i64) -> CommitContext {
    CommitContext::new(
        0,
        log_index,
        u128::from(log_index),
        TransactionTime::new(commit - 1, 0),
        TransactionTime::new(commit, 0),
    )
}

fn valid(start: i64, end: i64) -> Interval<ValidTime> {
    Interval::new(
        ValidTime::from_micros(start),
        Some(ValidTime::from_micros(end)),
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
    struct Noop;
    impl Wake for Noop {
        fn wake(self: Arc<Self>) {}
    }
    let waker = Waker::from(Arc::new(Noop));
    let mut context = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(value) => return value,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}
