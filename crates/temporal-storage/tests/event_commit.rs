use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};

use adapter_memory::MemoryAdapter;
use storage_api::{
    AdapterFuture, CandidateScanPage, CandidateScanRequest, ChangeScanPage, ChangeScanRequest,
    KeySpan, Keyspace, LogicalKey, MAX_QUERY_PAGE_BYTES, PushdownGuarantee, QueryPageBounds,
    ReadSnapshot, StorageAdapter,
};
use temporal_storage::{
    CanonicalTemporalEvent, CommitContext, EdgeMutation, EdgeTypeId, ElementId, ElementRef,
    GraphId, LabelId, PartitionId, TemporalEventOperation, TemporalScanBudget, TemporalStore,
    TemporalStoreError, TemporalTransaction, VertexIdentity, VertexMutation,
    current_vertex_graph_prefix, temporal_event_graph_prefix,
};
use temporal_types::{CanonicalElement, GraphValue, Interval, TransactionTime, ValidTime};

fn tx(value: i64) -> TransactionTime {
    TransactionTime::new(value, 0)
}

fn interval(start: i64, end: Option<i64>) -> Interval<ValidTime> {
    Interval::new(
        ValidTime::from_micros(start),
        end.map(ValidTime::from_micros),
    )
    .unwrap()
}

fn vertex(id: u128) -> ElementRef {
    ElementRef::vertex(GraphId::new(1), PartitionId::new(0), ElementId::new(id))
}

fn edge(id: u128) -> ElementRef {
    ElementRef::edge(GraphId::new(1), PartitionId::new(0), ElementId::new(id))
}

fn payload(name: &str) -> CanonicalElement {
    CanonicalElement::new(1, BTreeMap::from([(1, GraphValue::String(name.into()))]))
}

fn context(log_index: u64, read: i64, commit: i64) -> CommitContext {
    CommitContext::new(3, log_index, u128::from(log_index), tx(read), tx(commit))
}

#[test]
fn committed_mutations_write_canonical_events_idempotently() {
    let store = TemporalStore::new(MemoryAdapter::new());
    let label = LabelId::new(1);
    let endpoints = TemporalTransaction::new()
        .with_vertex(
            VertexMutation::put(vertex(1), label, interval(0, None), payload("source")).unwrap(),
        )
        .with_vertex(
            VertexMutation::put(vertex(2), label, interval(0, None), payload("destination"))
                .unwrap(),
        );
    block_on(store.commit_transaction(context(1, 0, 10), endpoints)).unwrap();
    block_on(
        store.commit_edge(
            context(2, 10, 20),
            EdgeMutation::put(
                edge(3),
                EdgeTypeId::new(2),
                vertex(1).id(),
                vertex(2).id(),
                interval(5, Some(9)),
                payload("knows"),
            )
            .unwrap(),
        ),
    )
    .unwrap();
    let deletion = VertexMutation::delete(vertex(1), label, interval(9, Some(10))).unwrap();
    block_on(store.commit_vertex(context(3, 20, 30), deletion.clone())).unwrap();

    let committed_events = events(&store);
    assert_eq!(committed_events.len(), 4);
    assert!(
        committed_events
            .iter()
            .any(|event| event.element() == edge(3)
                && event.operation() == TemporalEventOperation::Put
                && event.valid() == interval(5, Some(9))
                && event.commit_ts() == tx(20)
                && event.payload() == Some(&payload("knows"))
                && matches!(event.metadata(), Some(temporal_storage::TemporalEventMetadata::Edge { edge_type, source, destination }) if *edge_type == EdgeTypeId::new(2) && *source == vertex(1) && *destination == vertex(2)))
    );
    assert!(
        committed_events
            .iter()
            .any(|event| event.element() == vertex(1)
                && event.operation() == TemporalEventOperation::Delete
                && event.valid() == interval(9, Some(10))
                && event.commit_ts() == tx(30)
                && event.payload().is_none())
    );

    block_on(store.commit_vertex(context(3, 20, 30), deletion)).unwrap();
    assert_eq!(events(&store), committed_events);
}

#[test]
fn event_scans_apply_axis_windows_snapshot_and_budgets() {
    let store = TemporalStore::new(MemoryAdapter::new());
    let label = LabelId::new(1);
    block_on(store.commit_vertex(
        context(1, 0, 10),
        VertexMutation::put(vertex(1), label, interval(1, Some(3)), payload("first")).unwrap(),
    ))
    .unwrap();
    block_on(store.commit_vertex(
        context(2, 10, 20),
        VertexMutation::delete(vertex(1), label, interval(4, Some(5))).unwrap(),
    ))
    .unwrap();

    let (valid_events, _) = block_on(store.scan_events_by_valid_from(
        GraphId::new(1),
        ValidTime::from_micros(1),
        ValidTime::from_micros(4),
        tx(20),
        2,
        4096,
    ))
    .unwrap();
    assert_eq!(valid_events.len(), 1);
    assert_eq!(valid_events[0].operation(), TemporalEventOperation::Put);

    let (commit_events, _) =
        block_on(store.scan_events_by_commit(GraphId::new(1), tx(10), tx(20), tx(20), 2, 4096))
            .unwrap();
    assert_eq!(commit_events.len(), 1);
    assert_eq!(commit_events[0].operation(), TemporalEventOperation::Put);

    assert!(
        block_on(store.scan_events_by_valid_from(
            GraphId::new(1),
            ValidTime::from_micros(1),
            ValidTime::from_micros(6),
            tx(20),
            1,
            4096,
        ))
        .is_err()
    );
}

#[test]
fn event_scan_from_read_snapshot_excludes_later_commits() {
    let store = TemporalStore::new(MemoryAdapter::new());
    let label = LabelId::new(1);
    block_on(store.commit_vertex(
        context(1, 0, 10),
        VertexMutation::put(vertex(1), label, interval(1, Some(2)), payload("first")).unwrap(),
    ))
    .unwrap();

    let snapshot = block_on(store.adapter().begin_read_snapshot()).unwrap();
    block_on(store.commit_vertex(
        context(2, 10, 20),
        VertexMutation::put(vertex(2), label, interval(2, Some(3)), payload("later")).unwrap(),
    ))
    .unwrap();

    let (events, _) = block_on(store.scan_events_by_valid_from_in_snapshot(
        snapshot.as_ref(),
        GraphId::new(1),
        ValidTime::from_micros(1),
        ValidTime::from_micros(3),
        tx(20),
        TemporalScanBudget::new(2, 4096),
    ))
    .unwrap();

    assert_eq!(events.len(), 1);
    assert_eq!(events[0].element(), vertex(1));

    let (bounded_events, _) = block_on(store.scan_events_by_valid_from_in_snapshot(
        snapshot.as_ref(),
        GraphId::new(1),
        ValidTime::from_micros(1),
        ValidTime::from_micros(3),
        tx(20),
        TemporalScanBudget::new(1, 4096),
    ))
    .unwrap();
    assert_eq!(bounded_events.len(), 1);

    let fresh = block_on(store.begin_read_snapshot()).unwrap();
    assert!(
        block_on(store.scan_events_by_valid_from_in_snapshot(
            fresh.as_ref(),
            GraphId::new(1),
            ValidTime::from_micros(1),
            ValidTime::from_micros(3),
            tx(20),
            TemporalScanBudget::new(1, 4096),
        ))
        .is_err()
    );
}

#[test]
fn event_scan_budget_probes_use_the_open_read_snapshot() {
    let store = TemporalStore::new(MemoryAdapter::new());
    let empty = block_on(store.begin_read_snapshot()).unwrap();
    block_on(
        store.commit_vertex(
            context(1, 0, 10),
            VertexMutation::put(
                vertex(1),
                LabelId::new(1),
                interval(1, Some(2)),
                payload("later"),
            )
            .unwrap(),
        ),
    )
    .unwrap();

    for (max_rows, max_bytes) in [(0, 4096), (1, 0)] {
        let (events, _) = block_on(store.scan_events_by_valid_from_in_snapshot(
            empty.as_ref(),
            GraphId::new(1),
            ValidTime::from_micros(1),
            ValidTime::from_micros(2),
            tx(10),
            TemporalScanBudget::new(max_rows, max_bytes),
        ))
        .unwrap();
        assert!(events.is_empty());
    }

    let fresh = block_on(store.begin_read_snapshot()).unwrap();
    assert!(
        block_on(store.scan_events_by_valid_from_in_snapshot(
            fresh.as_ref(),
            GraphId::new(1),
            ValidTime::from_micros(1),
            ValidTime::from_micros(2),
            tx(10),
            TemporalScanBudget::new(0, 4096),
        ))
        .is_err()
    );
    assert!(
        block_on(store.scan_events_by_valid_from_in_snapshot(
            fresh.as_ref(),
            GraphId::new(1),
            ValidTime::from_micros(1),
            ValidTime::from_micros(2),
            tx(10),
            TemporalScanBudget::new(1, 0),
        ))
        .is_err()
    );
}

#[test]
fn valid_event_scan_budget_applies_only_inside_the_requested_window() {
    let store = TemporalStore::new(MemoryAdapter::new());
    let label = LabelId::new(1);
    block_on(store.commit_vertex(
        context(1, 0, 10),
        VertexMutation::put(vertex(1), label, interval(-100, Some(-99)), payload("old")).unwrap(),
    ))
    .unwrap();
    block_on(store.commit_vertex(
        context(2, 10, 20),
        VertexMutation::put(vertex(2), label, interval(10, Some(11)), payload("target")).unwrap(),
    ))
    .unwrap();

    let (events, _) = block_on(store.scan_events_by_valid_from(
        GraphId::new(1),
        ValidTime::from_micros(10),
        ValidTime::from_micros(11),
        tx(20),
        1,
        4096,
    ))
    .unwrap();

    assert_eq!(events.len(), 1);
    assert_eq!(events[0].element(), vertex(2));
}

#[test]
fn primitive_change_scan_rejects_a_page_from_another_applied_index() {
    let store = TemporalStore::new(MemoryAdapter::new());
    let snapshot = block_on(store.begin_read_snapshot()).unwrap();
    let mismatched = RewritingSnapshot {
        inner: snapshot,
        rewrite: PageRewrite::AppliedIndex(1),
    };

    let error = block_on(store.scan_events_by_valid_from_primitive_in_snapshot(
        &mismatched,
        GraphId::new(1),
        ValidTime::from_micros(1),
        ValidTime::from_micros(2),
        tx(10),
        TemporalScanBudget::new(1, 4096),
        PushdownGuarantee::Candidate,
    ))
    .unwrap_err();

    assert_eq!(
        error,
        TemporalStoreError::ChangeScanAppliedIndexMismatch {
            expected: 0,
            actual: 1,
        }
    );
}

#[test]
fn primitive_change_scan_rejects_candidate_pages_for_an_exact_plan() {
    let store = TemporalStore::new(MemoryAdapter::new());
    let snapshot = block_on(store.begin_read_snapshot()).unwrap();

    let error = block_on(store.scan_events_by_valid_from_primitive_in_snapshot(
        snapshot.as_ref(),
        GraphId::new(1),
        ValidTime::from_micros(1),
        ValidTime::from_micros(2),
        tx(10),
        TemporalScanBudget::new(1, 4096),
        PushdownGuarantee::Exact,
    ))
    .unwrap_err();

    assert_eq!(
        error,
        TemporalStoreError::ChangeScanGuaranteeMismatch {
            required: PushdownGuarantee::Exact,
            actual: PushdownGuarantee::Candidate,
        }
    );
}

#[test]
fn primitive_change_scan_rejects_a_non_advancing_empty_page() {
    let store = TemporalStore::new(MemoryAdapter::new());
    let snapshot = block_on(store.begin_read_snapshot()).unwrap();
    let stuck = RewritingSnapshot {
        inner: snapshot,
        rewrite: PageRewrite::StuckContinuation,
    };

    let error = block_on(store.scan_events_by_valid_from_primitive_in_snapshot(
        &stuck,
        GraphId::new(1),
        ValidTime::from_micros(1),
        ValidTime::from_micros(2),
        tx(10),
        TemporalScanBudget::new(1, 4096),
        PushdownGuarantee::Candidate,
    ))
    .unwrap_err();

    assert_eq!(
        error,
        TemporalStoreError::ChangeScanContinuationNotAdvancing
    );
}

#[test]
fn primitive_change_scan_enforces_the_entry_budget_across_pages() {
    let store = TemporalStore::new(MemoryAdapter::new());
    for (log_index, id, valid_start) in [(1, 1, 1), (2, 2, 2)] {
        block_on(
            store.commit_vertex(
                context(
                    log_index,
                    i64::from(valid_start - 1),
                    i64::from(valid_start + 10),
                ),
                VertexMutation::put(
                    vertex(id),
                    LabelId::new(1),
                    interval(i64::from(valid_start), Some(i64::from(valid_start + 1))),
                    payload("candidate"),
                )
                .unwrap(),
            ),
        )
        .unwrap();
    }
    let snapshot = block_on(store.begin_read_snapshot()).unwrap();
    let one_item_pages = RewritingSnapshot {
        inner: snapshot,
        rewrite: PageRewrite::CapOne,
    };

    let error = block_on(store.scan_events_by_valid_from_primitive_in_snapshot(
        &one_item_pages,
        GraphId::new(1),
        ValidTime::from_micros(1),
        ValidTime::from_micros(4),
        tx(20),
        TemporalScanBudget::new(1, 4096),
        PushdownGuarantee::Candidate,
    ))
    .unwrap_err();

    assert_eq!(error, TemporalStoreError::ScanEntryLimit);
}

#[test]
fn primitive_change_scan_removes_candidate_events_from_another_graph() {
    let store = TemporalStore::new(MemoryAdapter::new());
    block_on(
        store.commit_vertex(
            context(1, 0, 10),
            VertexMutation::put(
                vertex(1),
                LabelId::new(1),
                interval(1, Some(2)),
                payload("candidate"),
            )
            .unwrap(),
        ),
    )
    .unwrap();
    let snapshot = block_on(store.begin_read_snapshot()).unwrap();
    let false_positive = RewritingSnapshot {
        inner: snapshot,
        rewrite: PageRewrite::WrongPayloadGraph,
    };

    let (events, _) = block_on(store.scan_events_by_valid_from_primitive_in_snapshot(
        &false_positive,
        GraphId::new(1),
        ValidTime::from_micros(1),
        ValidTime::from_micros(2),
        tx(10),
        TemporalScanBudget::new(1, 4096),
        PushdownGuarantee::Candidate,
    ))
    .unwrap();

    assert!(events.is_empty());
}

#[test]
fn primitive_candidate_scan_rejects_a_page_from_another_applied_index() {
    let store = TemporalStore::new(MemoryAdapter::new());
    let snapshot = block_on(store.begin_read_snapshot()).unwrap();
    let mismatched = RewritingSnapshot {
        inner: snapshot,
        rewrite: PageRewrite::AppliedIndex(1),
    };

    let error = block_on(store.scan_vertex_views_current_candidate_in_snapshot(
        &mismatched,
        GraphId::new(1),
        ValidTime::from_micros(1),
        TemporalScanBudget::new(1, 4096),
        PushdownGuarantee::Candidate,
        &[],
    ))
    .unwrap_err();

    assert_eq!(
        error,
        TemporalStoreError::CandidateScanAppliedIndexMismatch {
            expected: 0,
            actual: 1,
        }
    );
}

#[test]
fn primitive_candidate_page_scans_exactly_one_page_and_returns_continuation() {
    let store = TemporalStore::new(MemoryAdapter::new());
    for (log_index, id) in [(1, 1), (2, 2)] {
        block_on(
            store.commit_vertex(
                context(log_index, 0, 10),
                VertexMutation::put(
                    vertex(id),
                    LabelId::new(1),
                    interval(1, None),
                    payload("candidate"),
                )
                .unwrap(),
            ),
        )
        .unwrap();
    }
    let calls = Arc::new(AtomicUsize::new(0));
    let snapshot = CountingCandidateSnapshot {
        inner: block_on(store.begin_read_snapshot()).unwrap(),
        calls: Arc::clone(&calls),
    };
    let span = KeySpan::prefix(
        Keyspace::Current,
        current_vertex_graph_prefix(GraphId::new(1)),
    );

    let page = block_on(store.scan_vertex_views_current_candidate_page_in_snapshot(
        &snapshot,
        GraphId::new(1),
        ValidTime::from_micros(1),
        span,
        QueryPageBounds::new(1, MAX_QUERY_PAGE_BYTES).unwrap(),
        TemporalScanBudget::new(2, MAX_QUERY_PAGE_BYTES),
        PushdownGuarantee::Candidate,
        &[],
    ))
    .unwrap();

    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(page.views().len(), 1);
    assert_eq!(page.scanned_rows(), 1);
    assert!(page.scanned_bytes() > 0);
    assert!(page.next_start().is_some());
}

#[test]
fn primitive_candidate_page_rejects_a_page_from_another_applied_index() {
    let store = TemporalStore::new(MemoryAdapter::new());
    let snapshot = block_on(store.begin_read_snapshot()).unwrap();
    let mismatched = RewritingSnapshot {
        inner: snapshot,
        rewrite: PageRewrite::AppliedIndex(1),
    };

    let error = block_on(store.scan_vertex_views_current_candidate_page_in_snapshot(
        &mismatched,
        GraphId::new(1),
        ValidTime::from_micros(1),
        KeySpan::prefix(
            Keyspace::Current,
            current_vertex_graph_prefix(GraphId::new(1)),
        ),
        QueryPageBounds::new(1, MAX_QUERY_PAGE_BYTES).unwrap(),
        TemporalScanBudget::new(1, MAX_QUERY_PAGE_BYTES),
        PushdownGuarantee::Candidate,
        &[],
    ))
    .unwrap_err();

    assert_eq!(
        error,
        TemporalStoreError::CandidateScanAppliedIndexMismatch {
            expected: 0,
            actual: 1,
        }
    );
}

#[test]
fn primitive_candidate_page_enforces_the_remaining_row_budget() {
    let store = TemporalStore::new(MemoryAdapter::new());
    block_on(
        store.commit_vertex(
            context(1, 0, 10),
            VertexMutation::put(
                vertex(1),
                LabelId::new(1),
                interval(1, None),
                payload("candidate"),
            )
            .unwrap(),
        ),
    )
    .unwrap();
    let snapshot = block_on(store.begin_read_snapshot()).unwrap();

    let error = block_on(store.scan_vertex_views_current_candidate_page_in_snapshot(
        snapshot.as_ref(),
        GraphId::new(1),
        ValidTime::from_micros(1),
        KeySpan::prefix(
            Keyspace::Current,
            current_vertex_graph_prefix(GraphId::new(1)),
        ),
        QueryPageBounds::new(1, MAX_QUERY_PAGE_BYTES).unwrap(),
        TemporalScanBudget::new(0, MAX_QUERY_PAGE_BYTES),
        PushdownGuarantee::Candidate,
        &[],
    ))
    .unwrap_err();

    assert_eq!(error, TemporalStoreError::ScanEntryLimit);
}

#[test]
fn primitive_candidate_page_enforces_the_remaining_byte_budget() {
    let store = TemporalStore::new(MemoryAdapter::new());
    block_on(
        store.commit_vertex(
            context(1, 0, 10),
            VertexMutation::put(
                vertex(1),
                LabelId::new(1),
                interval(1, None),
                payload("candidate"),
            )
            .unwrap(),
        ),
    )
    .unwrap();
    let snapshot = block_on(store.begin_read_snapshot()).unwrap();

    let error = block_on(store.scan_vertex_views_current_candidate_page_in_snapshot(
        snapshot.as_ref(),
        GraphId::new(1),
        ValidTime::from_micros(1),
        KeySpan::prefix(
            Keyspace::Current,
            current_vertex_graph_prefix(GraphId::new(1)),
        ),
        QueryPageBounds::new(1, MAX_QUERY_PAGE_BYTES).unwrap(),
        TemporalScanBudget::new(1, 1),
        PushdownGuarantee::Candidate,
        &[],
    ))
    .unwrap_err();

    assert_eq!(error, TemporalStoreError::ScanByteLimit);
}

#[test]
fn primitive_candidate_scan_rejects_candidate_pages_for_an_exact_plan() {
    let store = TemporalStore::new(MemoryAdapter::new());
    let snapshot = block_on(store.begin_read_snapshot()).unwrap();

    let error = block_on(store.scan_vertex_views_current_candidate_in_snapshot(
        snapshot.as_ref(),
        GraphId::new(1),
        ValidTime::from_micros(1),
        TemporalScanBudget::new(1, 4096),
        PushdownGuarantee::Exact,
        &[],
    ))
    .unwrap_err();

    assert_eq!(
        error,
        TemporalStoreError::CandidateScanGuaranteeMismatch {
            required: PushdownGuarantee::Exact,
            actual: PushdownGuarantee::Candidate,
        }
    );
}

#[test]
fn primitive_candidate_scan_rejects_a_non_advancing_empty_page() {
    let store = TemporalStore::new(MemoryAdapter::new());
    let snapshot = block_on(store.begin_read_snapshot()).unwrap();
    let stuck = RewritingSnapshot {
        inner: snapshot,
        rewrite: PageRewrite::StuckContinuation,
    };

    let error = block_on(store.scan_vertex_views_current_candidate_in_snapshot(
        &stuck,
        GraphId::new(1),
        ValidTime::from_micros(1),
        TemporalScanBudget::new(1, 4096),
        PushdownGuarantee::Candidate,
        &[],
    ))
    .unwrap_err();

    assert_eq!(
        error,
        TemporalStoreError::CandidateScanContinuationNotAdvancing
    );
}

#[test]
fn primitive_candidate_scan_enforces_the_byte_budget_across_pages() {
    let store = TemporalStore::new(MemoryAdapter::new());
    for (log_index, id) in [(1, 1), (2, 2)] {
        block_on(
            store.commit_vertex(
                context(log_index, 0, 10),
                VertexMutation::put(
                    vertex(id),
                    LabelId::new(1),
                    interval(1, None),
                    payload("candidate"),
                )
                .unwrap(),
            ),
        )
        .unwrap();
    }
    let snapshot = block_on(store.begin_read_snapshot()).unwrap();
    let entries = block_on(snapshot.scan(&KeySpan::prefix(
        Keyspace::Current,
        current_vertex_graph_prefix(GraphId::new(1)),
    )))
    .unwrap();
    let entry_bytes = entries
        .iter()
        .map(|entry| entry.key().as_bytes().len() + entry.value().len())
        .collect::<Vec<_>>();
    assert_eq!(entry_bytes.len(), 2);
    let max_bytes = u64::try_from(entry_bytes[0] + entry_bytes[1] - 1).unwrap();
    let one_item_pages = RewritingSnapshot {
        inner: snapshot,
        rewrite: PageRewrite::CapOne,
    };

    let error = block_on(store.scan_vertex_views_current_candidate_in_snapshot(
        &one_item_pages,
        GraphId::new(1),
        ValidTime::from_micros(1),
        TemporalScanBudget::new(2, max_bytes),
        PushdownGuarantee::Candidate,
        &[],
    ))
    .unwrap_err();

    assert_eq!(error, TemporalStoreError::ScanByteLimit);
}

#[test]
fn primitive_candidate_scan_rejects_a_missing_identity() {
    let store = TemporalStore::new(MemoryAdapter::new());
    block_on(
        store.commit_vertex(
            context(1, 0, 10),
            VertexMutation::put(
                vertex(1),
                LabelId::new(1),
                interval(1, None),
                payload("candidate"),
            )
            .unwrap(),
        ),
    )
    .unwrap();
    let snapshot = block_on(store.begin_read_snapshot()).unwrap();
    let missing_identity = RewritingSnapshot {
        inner: snapshot,
        rewrite: PageRewrite::MissingIdentity,
    };

    let error = block_on(store.scan_vertex_views_current_candidate_in_snapshot(
        &missing_identity,
        GraphId::new(1),
        ValidTime::from_micros(1),
        TemporalScanBudget::new(1, 4096),
        PushdownGuarantee::Candidate,
        &[],
    ))
    .unwrap_err();

    assert_eq!(error, TemporalStoreError::IdentityMismatch);
}

#[test]
fn primitive_candidate_scan_rejects_a_mismatched_identity() {
    let store = TemporalStore::new(MemoryAdapter::new());
    block_on(
        store.commit_vertex(
            context(1, 0, 10),
            VertexMutation::put(
                vertex(1),
                LabelId::new(1),
                interval(1, None),
                payload("candidate"),
            )
            .unwrap(),
        ),
    )
    .unwrap();
    let snapshot = block_on(store.begin_read_snapshot()).unwrap();
    let mismatched_identity = RewritingSnapshot {
        inner: snapshot,
        rewrite: PageRewrite::MismatchedIdentity,
    };

    let error = block_on(store.scan_vertex_views_current_candidate_in_snapshot(
        &mismatched_identity,
        GraphId::new(1),
        ValidTime::from_micros(1),
        TemporalScanBudget::new(1, 4096),
        PushdownGuarantee::Candidate,
        &[],
    ))
    .unwrap_err();

    assert_eq!(error, TemporalStoreError::IdentityMismatch);
}

enum PageRewrite {
    AppliedIndex(u64),
    StuckContinuation,
    CapOne,
    WrongPayloadGraph,
    MissingIdentity,
    MismatchedIdentity,
}

struct RewritingSnapshot<'a> {
    inner: Box<dyn ReadSnapshot + 'a>,
    rewrite: PageRewrite,
}

struct CountingCandidateSnapshot<'a> {
    inner: Box<dyn ReadSnapshot + 'a>,
    calls: Arc<AtomicUsize>,
}

impl ReadSnapshot for CountingCandidateSnapshot<'_> {
    fn applied_log_index(&self) -> u64 {
        self.inner.applied_log_index()
    }

    fn multi_get<'a>(
        &'a self,
        keys: &'a [storage_api::LogicalKey],
    ) -> AdapterFuture<'a, Vec<Option<Vec<u8>>>> {
        self.inner.multi_get(keys)
    }

    fn scan<'a>(&'a self, span: &'a KeySpan) -> AdapterFuture<'a, Vec<storage_api::KeyValue>> {
        self.inner.scan(span)
    }

    fn scan_candidates<'a>(
        &'a self,
        request: &'a CandidateScanRequest,
    ) -> AdapterFuture<'a, CandidateScanPage> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move { self.inner.scan_candidates(request).await })
    }
}

impl ReadSnapshot for RewritingSnapshot<'_> {
    fn applied_log_index(&self) -> u64 {
        self.inner.applied_log_index()
    }

    fn multi_get<'a>(
        &'a self,
        keys: &'a [storage_api::LogicalKey],
    ) -> AdapterFuture<'a, Vec<Option<Vec<u8>>>> {
        match self.rewrite {
            PageRewrite::MissingIdentity => Box::pin(async move { Ok(vec![None; keys.len()]) }),
            PageRewrite::MismatchedIdentity => Box::pin(async move {
                let encoded = VertexIdentity::new(vertex(999), LabelId::new(1))
                    .map_err(|error| storage_api::AdapterError::Backend(error.to_string()))?
                    .encode();
                Ok(vec![Some(encoded); keys.len()])
            }),
            _ => self.inner.multi_get(keys),
        }
    }

    fn scan<'a>(&'a self, span: &'a KeySpan) -> AdapterFuture<'a, Vec<storage_api::KeyValue>> {
        self.inner.scan(span)
    }

    fn scan_changes<'a>(
        &'a self,
        request: &'a ChangeScanRequest,
    ) -> AdapterFuture<'a, ChangeScanPage> {
        Box::pin(async move {
            match self.rewrite {
                PageRewrite::AppliedIndex(applied_log_index) => {
                    let page = self.inner.scan_changes(request).await?;
                    let guarantee = page.guarantee();
                    let next_start = page.next_start().cloned();
                    ChangeScanPage::new(
                        request,
                        applied_log_index,
                        guarantee,
                        page.into_entries(),
                        next_start,
                    )
                    .map_err(|error| storage_api::AdapterError::Backend(error.to_string()))
                }
                PageRewrite::StuckContinuation => ChangeScanPage::new(
                    request,
                    self.applied_log_index(),
                    PushdownGuarantee::Candidate,
                    Vec::new(),
                    Some(LogicalKey::in_keyspace(
                        request.span().keyspace(),
                        request.span().start().to_vec(),
                    )),
                )
                .map_err(|error| storage_api::AdapterError::Backend(error.to_string())),
                PageRewrite::CapOne => {
                    let narrow = ChangeScanRequest::new(
                        request.span().clone(),
                        QueryPageBounds::new(1, MAX_QUERY_PAGE_BYTES).map_err(|error| {
                            storage_api::AdapterError::Backend(error.to_string())
                        })?,
                    )
                    .map_err(|error| storage_api::AdapterError::Backend(error.to_string()))?;
                    let page = self.inner.scan_changes(&narrow).await?;
                    let applied_log_index = page.applied_log_index();
                    let guarantee = page.guarantee();
                    let next_start = page.next_start().cloned();
                    ChangeScanPage::new(
                        request,
                        applied_log_index,
                        guarantee,
                        page.into_entries(),
                        next_start,
                    )
                    .map_err(|error| storage_api::AdapterError::Backend(error.to_string()))
                }
                PageRewrite::WrongPayloadGraph => {
                    let page = self.inner.scan_changes(request).await?;
                    let applied_log_index = page.applied_log_index();
                    let guarantee = page.guarantee();
                    let next_start = page.next_start().cloned();
                    let entries = page
                        .into_entries()
                        .into_iter()
                        .map(|entry| {
                            let event = CanonicalTemporalEvent::put(
                                ElementRef::vertex(
                                    GraphId::new(2),
                                    PartitionId::new(0),
                                    ElementId::new(999),
                                ),
                                interval(1, Some(2)),
                                tx(10),
                                0,
                                payload("false-positive"),
                            )
                            .unwrap();
                            storage_api::KeyValue::new(entry.key().clone(), event.encode().unwrap())
                        })
                        .collect();
                    ChangeScanPage::new(request, applied_log_index, guarantee, entries, next_start)
                        .map_err(|error| storage_api::AdapterError::Backend(error.to_string()))
                }
                PageRewrite::MissingIdentity | PageRewrite::MismatchedIdentity => {
                    self.inner.scan_changes(request).await
                }
            }
        })
    }

    fn scan_candidates<'a>(
        &'a self,
        request: &'a CandidateScanRequest,
    ) -> AdapterFuture<'a, CandidateScanPage> {
        Box::pin(async move {
            match self.rewrite {
                PageRewrite::AppliedIndex(applied_log_index) => {
                    let page = self.inner.scan_candidates(request).await?;
                    let guarantee = page.guarantee();
                    let next_start = page.next_start().cloned();
                    CandidateScanPage::new(
                        request,
                        applied_log_index,
                        guarantee,
                        page.into_entries(),
                        next_start,
                    )
                    .map_err(|error| storage_api::AdapterError::Backend(error.to_string()))
                }
                PageRewrite::StuckContinuation => CandidateScanPage::new(
                    request,
                    self.applied_log_index(),
                    PushdownGuarantee::Candidate,
                    Vec::new(),
                    Some(LogicalKey::in_keyspace(
                        request.span().keyspace(),
                        request.span().start().to_vec(),
                    )),
                )
                .map_err(|error| storage_api::AdapterError::Backend(error.to_string())),
                PageRewrite::CapOne => {
                    let narrow = CandidateScanRequest::new(
                        request.span().clone(),
                        request.valid_time(),
                        request.constraints().to_vec(),
                        QueryPageBounds::new(1, MAX_QUERY_PAGE_BYTES).map_err(|error| {
                            storage_api::AdapterError::Backend(error.to_string())
                        })?,
                    )
                    .map_err(|error| storage_api::AdapterError::Backend(error.to_string()))?;
                    let page = self.inner.scan_candidates(&narrow).await?;
                    let applied_log_index = page.applied_log_index();
                    let guarantee = page.guarantee();
                    let next_start = page.next_start().cloned();
                    CandidateScanPage::new(
                        request,
                        applied_log_index,
                        guarantee,
                        page.into_entries(),
                        next_start,
                    )
                    .map_err(|error| storage_api::AdapterError::Backend(error.to_string()))
                }
                PageRewrite::WrongPayloadGraph
                | PageRewrite::MissingIdentity
                | PageRewrite::MismatchedIdentity => self.inner.scan_candidates(request).await,
            }
        })
    }
}

fn events(store: &TemporalStore<MemoryAdapter>) -> Vec<CanonicalTemporalEvent> {
    block_on(store.adapter().scan(&KeySpan::prefix(
        Keyspace::TemporalIndex,
        temporal_event_graph_prefix(GraphId::new(1)),
    )))
    .unwrap()
    .iter()
    .map(|entry| CanonicalTemporalEvent::decode(entry.value()).unwrap())
    .collect()
}

fn block_on<T>(future: impl Future<Output = T>) -> T {
    struct Noop;
    impl Wake for Noop {
        fn wake(self: Arc<Self>) {}
    }

    let waker = Waker::from(Arc::new(Noop));
    let mut context = Context::from_waker(&waker);
    let mut future = Box::pin(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(value) => return value,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}
