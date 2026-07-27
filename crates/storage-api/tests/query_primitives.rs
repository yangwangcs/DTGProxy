use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use storage_api::{
    AdapterCapabilities, AdapterError, AdapterFuture, AdjacencyEntry, AdjacencyExpandPage,
    AdjacencyExpandRequest, ApplyReceipt, CandidateScanPage, CandidateScanRequest,
    CanonicalScanPage, CanonicalScanRequest, ChangeScanPage, ChangeScanRequest,
    CommittedMutationBatch, ComparisonOperator, KeySpan, KeyValue, Keyspace, LogicalKey,
    PropertyConstraint, PropertyGatherPage, PropertyGatherRequest, PropertyRow, PushdownGuarantee,
    QueryPageBounds, QueryPrimitiveCapabilities, QueryPrimitiveError, StorageAdapter,
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
