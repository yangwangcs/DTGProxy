use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use storage_api::{
    AdapterCapabilities, AdapterError, AdapterFuture, AdjacencyEntry, AdjacencyExpandPage,
    AdjacencyExpandRequest, ApplyReceipt, CandidateScanPage, CandidateScanRequest,
    CanonicalBatchScanPage, CanonicalBatchScanRequest, CanonicalScanPage, CanonicalScanRequest,
    ChangeScanPage, ChangeScanRequest, CommittedMutationBatch, ComparisonOperator, KeySpan,
    KeyValue, Keyspace, LogicalKey, MAX_CANONICAL_BATCH_RANGES, MAX_QUERY_PAGE_BYTES,
    MAX_QUERY_PAGE_ITEMS, PropertyConstraint, PropertyGatherPage, PropertyGatherRequest,
    PropertyRow, PushdownGuarantee, QueryPageBounds, QueryPrimitiveCapabilities,
    QueryPrimitiveError, ReadSnapshot, StorageAdapter,
};
use temporal_types::GraphValue;

#[test]
fn pushdown_guarantees_make_the_residual_rule_explicit() {
    assert_eq!(PushdownGuarantee::Unsupported.residual_required(), None);
    assert_eq!(PushdownGuarantee::Candidate.residual_required(), Some(true));
    assert_eq!(PushdownGuarantee::Exact.residual_required(), Some(false));

    let capabilities = QueryPrimitiveCapabilities::new(
        PushdownGuarantee::Candidate,
        PushdownGuarantee::Exact,
        PushdownGuarantee::Unsupported,
        PushdownGuarantee::Candidate,
    );
    assert_eq!(capabilities.candidate_scan(), PushdownGuarantee::Candidate);
    assert_eq!(capabilities.property_gather(), PushdownGuarantee::Exact);
    assert_eq!(
        capabilities.adjacency_expand(),
        PushdownGuarantee::Unsupported
    );
    assert_eq!(capabilities.change_scan(), PushdownGuarantee::Candidate);
}

#[test]
fn page_bounds_reject_zero_and_platform_exceeding_limits() {
    assert_eq!(
        QueryPageBounds::new(0, 1),
        Err(QueryPrimitiveError::ZeroItemLimit)
    );
    assert_eq!(
        QueryPageBounds::new(1, 0),
        Err(QueryPrimitiveError::ZeroByteLimit)
    );
    assert!(matches!(
        QueryPageBounds::new(storage_api::MAX_QUERY_PAGE_ITEMS + 1, 1),
        Err(QueryPrimitiveError::ItemLimitTooLarge { .. })
    ));
    assert!(matches!(
        QueryPageBounds::new(1, storage_api::MAX_QUERY_PAGE_BYTES + 1),
        Err(QueryPrimitiveError::ByteLimitTooLarge { .. })
    ));
}

#[test]
fn canonical_scan_page_enforces_bounds_order_and_continuation() {
    let request = CanonicalScanRequest::new(
        KeySpan::prefix(Keyspace::Current, b"vertex/".to_vec()),
        bounds(2, 64),
    )
    .unwrap();
    let page = CanonicalScanPage::new(
        &request,
        7,
        vec![entry(Keyspace::Current, b"vertex/1", b"one")],
        Some(key(Keyspace::Current, b"vertex/2")),
    )
    .unwrap();
    assert_eq!(page.applied_log_index(), 7);
    assert_eq!(page.entries().len(), 1);
    assert_eq!(
        page.next_start().map(LogicalKey::as_bytes),
        Some(b"vertex/2".as_slice())
    );

    assert_eq!(
        CanonicalScanPage::new(
            &request,
            7,
            vec![entry(Keyspace::Current, b"vertex/1", b"one")],
            Some(key(Keyspace::Current, b"vertex/1")),
        ),
        Err(QueryPrimitiveError::InvalidContinuation)
    );
    assert!(matches!(
        CanonicalScanPage::new(
            &request,
            7,
            vec![entry(Keyspace::Current, b"vertex/1", &[0; 64])],
            None,
        ),
        Err(QueryPrimitiveError::PageByteLimitExceeded { .. })
    ));
}

#[test]
fn canonical_batch_request_enforces_range_and_aggregate_limits() {
    let first = CanonicalScanRequest::new(
        KeySpan::prefix(Keyspace::Current, b"a/".to_vec()),
        bounds(2, 32),
    )
    .unwrap();
    let second = CanonicalScanRequest::new(
        KeySpan::prefix(Keyspace::Current, b"b/".to_vec()),
        bounds(3, 48),
    )
    .unwrap();

    assert_eq!(
        CanonicalBatchScanRequest::new(Vec::new(), 1),
        Err(QueryPrimitiveError::EmptyInput)
    );
    let duplicate = CanonicalScanRequest::new(first.span().clone(), bounds(1, 16)).unwrap();
    assert!(matches!(
        CanonicalBatchScanRequest::new(vec![first.clone(), duplicate], 64),
        Err(QueryPrimitiveError::DuplicateCanonicalRange)
    ));
    assert!(matches!(
        CanonicalBatchScanRequest::new(vec![first.clone()], MAX_QUERY_PAGE_BYTES + 1),
        Err(QueryPrimitiveError::ByteLimitTooLarge { .. })
    ));
    assert!(matches!(
        CanonicalBatchScanRequest::new(vec![first.clone(), second.clone()], 79),
        Err(QueryPrimitiveError::RequestByteLimitExceeded { .. })
    ));

    let oversized_items = CanonicalScanRequest::new(
        KeySpan::prefix(Keyspace::Current, b"c/".to_vec()),
        bounds(MAX_QUERY_PAGE_ITEMS, 8),
    )
    .unwrap();
    assert!(matches!(
        CanonicalBatchScanRequest::new(vec![first.clone(), oversized_items], 40),
        Err(QueryPrimitiveError::InputLimitExceeded { .. })
    ));

    let too_many = (0..=MAX_CANONICAL_BATCH_RANGES)
        .map(|ordinal| {
            CanonicalScanRequest::new(
                KeySpan::prefix(Keyspace::Current, format!("range/{ordinal}/").into_bytes()),
                bounds(1, 32),
            )
            .unwrap()
        })
        .collect();
    assert!(matches!(
        CanonicalBatchScanRequest::new(too_many, MAX_QUERY_PAGE_BYTES),
        Err(QueryPrimitiveError::InputLimitExceeded { .. })
    ));

    let request = CanonicalBatchScanRequest::new(vec![second.clone(), first.clone()], 80).unwrap();
    assert_eq!(request.scans(), &[second, first]);
    assert_eq!(request.max_total_bytes(), 80);
}

#[test]
fn canonical_batch_page_enforces_order_index_and_aggregate_bytes() {
    let first = CanonicalScanRequest::new(
        KeySpan::prefix(Keyspace::Current, b"a/".to_vec()),
        bounds(1, 8),
    )
    .unwrap();
    let second = CanonicalScanRequest::new(
        KeySpan::prefix(Keyspace::Current, b"b/".to_vec()),
        bounds(1, 8),
    )
    .unwrap();
    let request = CanonicalBatchScanRequest::new(vec![first.clone(), second.clone()], 16).unwrap();
    let first_page = CanonicalScanPage::new(
        &first,
        42,
        vec![entry(Keyspace::Current, b"a/1", b"one")],
        None,
    )
    .unwrap();
    let second_page = CanonicalScanPage::new(
        &second,
        42,
        vec![entry(Keyspace::Current, b"b/1", b"two")],
        None,
    )
    .unwrap();

    let page =
        CanonicalBatchScanPage::new(&request, 42, vec![first_page.clone(), second_page.clone()])
            .unwrap();
    assert_eq!(page.applied_log_index(), 42);
    assert_eq!(page.pages(), &[first_page.clone(), second_page.clone()]);
    assert!(
        page.pages()
            .iter()
            .flat_map(CanonicalScanPage::entries)
            .map(|entry| entry.key().as_bytes().len() + entry.value().len())
            .sum::<usize>()
            <= 16
    );
    assert_eq!(
        page.into_pages(),
        vec![first_page.clone(), second_page.clone()]
    );

    assert!(matches!(
        CanonicalBatchScanPage::new(&request, 42, vec![first_page.clone()]),
        Err(QueryPrimitiveError::CardinalityMismatch { .. })
    ));
    assert_eq!(
        CanonicalBatchScanPage::new(&request, 42, vec![second_page.clone(), first_page.clone()]),
        Err(QueryPrimitiveError::CanonicalRequestMismatch { ordinal: 0 })
    );

    let mismatched_index = CanonicalScanPage::new(&second, 41, Vec::new(), None).unwrap();
    assert_eq!(
        CanonicalBatchScanPage::new(&request, 42, vec![first_page.clone(), mismatched_index]),
        Err(QueryPrimitiveError::AppliedIndexMismatch {
            expected: 42,
            actual: 41,
        })
    );

    let empty_first = CanonicalScanPage::new(&first, 42, Vec::new(), None).unwrap();
    let empty_second = CanonicalScanPage::new(&second, 42, Vec::new(), None).unwrap();
    assert_eq!(
        CanonicalBatchScanPage::new(&request, 42, vec![empty_second, empty_first]),
        Err(QueryPrimitiveError::CanonicalRequestMismatch { ordinal: 0 })
    );
}

#[test]
fn typed_requests_reject_wrong_keyspaces_and_unbounded_inputs() {
    let bounds = bounds(4, 256);
    let constraint = PropertyConstraint::new(
        storage_api::PropertyId::new(1),
        ComparisonOperator::Equal,
        GraphValue::String("active".to_owned()),
    );
    assert!(matches!(
        CandidateScanRequest::new(
            KeySpan::prefix(Keyspace::Meta, Vec::new()),
            temporal_types::ValidTime::from_micros(7),
            vec![constraint],
            bounds,
        ),
        Err(QueryPrimitiveError::InvalidKeyspace { .. })
    ));
    assert!(matches!(
        CandidateScanRequest::new(
            KeySpan::prefix(Keyspace::Current, Vec::new())
                .with_limit(2)
                .unwrap(),
            temporal_types::ValidTime::from_micros(7),
            Vec::new(),
            bounds,
        ),
        Err(QueryPrimitiveError::SpanAlreadyBounded)
    ));

    assert!(matches!(
        PropertyGatherRequest::new(
            vec![key(Keyspace::AdjOut, b"edge/1")],
            vec![storage_api::PropertyId::new(1)],
            bounds,
        ),
        Err(QueryPrimitiveError::InvalidKeyspace { .. })
    ));
    assert!(matches!(
        AdjacencyExpandRequest::new(
            vec![KeySpan::prefix(Keyspace::Current, b"vertex/1".to_vec())],
            bounds,
        ),
        Err(QueryPrimitiveError::InvalidKeyspace { .. })
    ));
    assert!(matches!(
        ChangeScanRequest::new(KeySpan::prefix(Keyspace::History, Vec::new()), bounds),
        Err(QueryPrimitiveError::InvalidKeyspace { .. })
    ));
}

#[test]
fn typed_requests_reject_inputs_above_their_byte_budget() {
    let constraint = PropertyConstraint::new(
        storage_api::PropertyId::new(1),
        ComparisonOperator::Equal,
        GraphValue::Bytes(vec![0; 32]),
    );
    assert!(matches!(
        CandidateScanRequest::new(
            KeySpan::prefix(Keyspace::Current, b"vertex/".to_vec()),
            temporal_types::ValidTime::from_micros(7),
            vec![constraint],
            bounds(2, 16),
        ),
        Err(QueryPrimitiveError::RequestByteLimitExceeded { .. })
    ));

    assert!(matches!(
        PropertyGatherRequest::new(
            vec![key(Keyspace::Current, b"a-very-long-canonical-key")],
            vec![storage_api::PropertyId::new(1)],
            bounds(2, 8),
        ),
        Err(QueryPrimitiveError::RequestByteLimitExceeded { .. })
    ));
}

#[test]
fn candidate_constraints_have_an_independent_limit_and_retain_valid_time() {
    let valid_time = temporal_types::ValidTime::from_micros(123);
    let constraints = vec![
        PropertyConstraint::new(
            storage_api::PropertyId::new(1),
            ComparisonOperator::Equal,
            GraphValue::Boolean(true),
        ),
        PropertyConstraint::new(
            storage_api::PropertyId::new(2),
            ComparisonOperator::GreaterThan,
            GraphValue::Integer(7),
        ),
    ];
    let request = CandidateScanRequest::new(
        KeySpan::prefix(Keyspace::Current, b"vertex/".to_vec()),
        valid_time,
        constraints,
        bounds(1, 256),
    )
    .expect("output page size must not cap predicate constraints");

    assert_eq!(request.valid_time(), valid_time);
    assert_eq!(request.constraints().len(), 2);
}

#[test]
fn candidate_and_change_pages_reject_oversize_or_out_of_domain_entries() {
    let candidate_request = CandidateScanRequest::new(
        KeySpan::prefix(Keyspace::Current, b"vertex/".to_vec()),
        temporal_types::ValidTime::from_micros(7),
        Vec::new(),
        bounds(1, 64),
    )
    .unwrap();
    assert!(matches!(
        CandidateScanPage::new(
            &candidate_request,
            7,
            PushdownGuarantee::Candidate,
            vec![
                entry(Keyspace::Current, b"vertex/1", b"one"),
                entry(Keyspace::Current, b"vertex/2", b"two"),
            ],
            None,
        ),
        Err(QueryPrimitiveError::PageItemLimitExceeded { .. })
    ));
    assert!(matches!(
        CandidateScanPage::new(
            &candidate_request,
            7,
            PushdownGuarantee::Exact,
            vec![entry(Keyspace::History, b"vertex/1", b"one")],
            None,
        ),
        Err(QueryPrimitiveError::InvalidKeyspace { .. })
    ));

    let change_request = ChangeScanRequest::new(
        KeySpan::prefix(Keyspace::TemporalIndex, Vec::new()),
        bounds(2, 8),
    )
    .unwrap();
    assert!(matches!(
        ChangeScanPage::new(
            &change_request,
            9,
            PushdownGuarantee::Exact,
            vec![entry(Keyspace::TemporalIndex, b"event/1", b"payload")],
            None,
        ),
        Err(QueryPrimitiveError::PageByteLimitExceeded { .. })
    ));
}

#[test]
fn property_page_requires_one_typed_row_per_requested_key() {
    let request = PropertyGatherRequest::new(
        vec![
            key(Keyspace::Current, b"vertex/1"),
            key(Keyspace::Current, b"vertex/2"),
        ],
        vec![
            storage_api::PropertyId::new(1),
            storage_api::PropertyId::new(2),
        ],
        bounds(4, 256),
    )
    .unwrap();

    assert!(matches!(
        PropertyGatherPage::new(
            &request,
            11,
            PushdownGuarantee::Exact,
            vec![PropertyRow::new(
                key(Keyspace::Current, b"vertex/1"),
                vec![Some(GraphValue::String("Ada".to_owned()))],
            )],
        ),
        Err(QueryPrimitiveError::CardinalityMismatch { .. })
    ));

    assert!(matches!(
        PropertyGatherPage::new(
            &request,
            11,
            PushdownGuarantee::Exact,
            vec![
                PropertyRow::new(
                    key(Keyspace::Current, b"vertex/1"),
                    vec![Some(GraphValue::String("Ada".to_owned()))],
                ),
                PropertyRow::new(
                    key(Keyspace::Current, b"vertex/2"),
                    vec![None, Some(GraphValue::Integer(37))],
                ),
            ],
        ),
        Err(QueryPrimitiveError::CardinalityMismatch { .. })
    ));
}

#[test]
fn adjacency_page_validates_source_ordinal_and_keyspace() {
    let request = AdjacencyExpandRequest::new(
        vec![
            KeySpan::prefix(Keyspace::AdjOut, b"vertex/1/".to_vec()),
            KeySpan::prefix(Keyspace::AdjOut, b"vertex/2/".to_vec()),
        ],
        bounds(4, 256),
    )
    .unwrap();

    assert!(matches!(
        AdjacencyExpandPage::new(
            &request,
            13,
            PushdownGuarantee::Candidate,
            vec![AdjacencyEntry::new(
                2,
                entry(Keyspace::AdjOut, b"vertex/3/edge/1", b"edge"),
            )],
            None,
        ),
        Err(QueryPrimitiveError::UnknownInputOrdinal { .. })
    ));
    assert!(matches!(
        AdjacencyExpandPage::new(
            &request,
            13,
            PushdownGuarantee::Candidate,
            vec![AdjacencyEntry::new(
                0,
                entry(Keyspace::AdjIn, b"vertex/1/edge/1", b"edge"),
            )],
            None,
        ),
        Err(QueryPrimitiveError::InvalidKeyspace { .. })
    ));
}

#[test]
fn optional_adapter_primitives_are_unsupported_by_default() {
    let adapter = MinimalAdapter;
    assert_eq!(
        adapter.query_primitive_capabilities(),
        QueryPrimitiveCapabilities::NONE
    );

    let candidate = CandidateScanRequest::new(
        KeySpan::prefix(Keyspace::Current, Vec::new()),
        temporal_types::ValidTime::from_micros(7),
        Vec::new(),
        bounds(1, 64),
    )
    .unwrap();
    let canonical = CanonicalScanRequest::new(
        KeySpan::prefix(Keyspace::Current, Vec::new()),
        bounds(1, 64),
    )
    .unwrap();
    let properties = PropertyGatherRequest::new(
        vec![key(Keyspace::Current, b"vertex/1")],
        vec![storage_api::PropertyId::new(1)],
        bounds(1, 64),
    )
    .unwrap();
    let adjacency = AdjacencyExpandRequest::new(
        vec![KeySpan::prefix(Keyspace::AdjOut, Vec::new())],
        bounds(1, 64),
    )
    .unwrap();
    let changes = ChangeScanRequest::new(
        KeySpan::prefix(Keyspace::TemporalIndex, Vec::new()),
        bounds(1, 64),
    )
    .unwrap();

    for result in [
        block_on(adapter.scan_canonical(&canonical)).map(|_| ()),
        block_on(adapter.scan_candidates(&candidate)).map(|_| ()),
        block_on(adapter.gather_properties(&properties)).map(|_| ()),
        block_on(adapter.expand_adjacency(&adjacency)).map(|_| ()),
        block_on(adapter.scan_changes(&changes)).map(|_| ()),
    ] {
        assert!(matches!(
            result,
            Err(AdapterError::UnsupportedOperation { .. })
        ));
    }
}

#[test]
fn snapshot_canonical_batch_scan_is_unsupported_by_default() {
    let scan = CanonicalScanRequest::new(
        KeySpan::prefix(Keyspace::Current, Vec::new()),
        bounds(1, 64),
    )
    .unwrap();
    let request = CanonicalBatchScanRequest::new(vec![scan], 64).unwrap();

    assert_eq!(
        block_on(MinimalSnapshot.scan_canonical_batch(&request)),
        Err(AdapterError::UnsupportedOperation {
            operation: "snapshot canonical batch scan",
        })
    );
}

fn bounds(max_items: usize, max_bytes: u64) -> QueryPageBounds {
    QueryPageBounds::new(max_items, max_bytes).unwrap()
}

fn key(keyspace: Keyspace, bytes: &[u8]) -> LogicalKey {
    LogicalKey::in_keyspace(keyspace, bytes.to_vec())
}

fn entry(keyspace: Keyspace, key_bytes: &[u8], value: &[u8]) -> KeyValue {
    KeyValue::new(key(keyspace, key_bytes), value.to_vec())
}

struct MinimalAdapter;

struct MinimalSnapshot;

impl ReadSnapshot for MinimalSnapshot {
    fn applied_log_index(&self) -> u64 {
        0
    }

    fn multi_get<'a>(&'a self, _keys: &'a [LogicalKey]) -> AdapterFuture<'a, Vec<Option<Vec<u8>>>> {
        Box::pin(async { panic!("not used") })
    }

    fn scan<'a>(&'a self, _span: &'a KeySpan) -> AdapterFuture<'a, Vec<KeyValue>> {
        Box::pin(async { panic!("not used") })
    }
}

impl StorageAdapter for MinimalAdapter {
    fn capabilities(&self) -> AdapterCapabilities {
        panic!("not used")
    }

    fn apply_committed<'a>(
        &'a self,
        _batch: CommittedMutationBatch,
    ) -> AdapterFuture<'a, ApplyReceipt> {
        Box::pin(async { panic!("not used") })
    }

    fn multi_get<'a>(&'a self, _keys: &'a [LogicalKey]) -> AdapterFuture<'a, Vec<Option<Vec<u8>>>> {
        Box::pin(async { panic!("not used") })
    }

    fn scan<'a>(&'a self, _span: &'a KeySpan) -> AdapterFuture<'a, Vec<KeyValue>> {
        Box::pin(async { panic!("not used") })
    }

    fn applied_log_index(&self) -> Result<u64, AdapterError> {
        Ok(0)
    }
}

fn block_on<F: Future>(future: F) -> F::Output {
    struct NoopWake;

    impl Wake for NoopWake {
        fn wake(self: Arc<Self>) {}
    }

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
