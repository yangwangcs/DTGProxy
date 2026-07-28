use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use adapter_memory::MemoryAdapter;
use adapter_sidecar::{SidecarAdapter, SidecarService, SidecarTransport, SidecarTransportFuture};
use storage_api::{
    AdapterError, AdapterFuture, CanonicalBatchScanPage, CanonicalBatchScanRequest,
    CanonicalScanPage, CanonicalScanRequest, CommittedMutationBatch, KeyValue, LogicalKey,
    Mutation, ReadSnapshot, StorageAdapter,
};
use temporal_storage::{
    ElementId, ElementRef, GraphId, HistoryAnchor, HistoryDelta, HistoryEntry, HistoryReadBudget,
    PartitionId, PointHistoryReader, PointHistoryRequest, ProjectionRecord, PropertyDemand,
    TemporalStoreError, ValidSegment, history_anchor_key,
};
use temporal_types::{CanonicalElement, GraphValue, Interval, TransactionTime, ValidTime};

fn tx(value: i64) -> TransactionTime {
    TransactionTime::new(value, 0)
}

fn valid(value: i64) -> ValidTime {
    ValidTime::from_micros(value)
}

fn interval(start: i64, end: i64) -> Interval<ValidTime> {
    Interval::new(valid(start), Some(valid(end))).unwrap()
}

fn vertex(id: u128) -> ElementRef {
    ElementRef::vertex(GraphId::new(1), PartitionId::new(0), ElementId::new(id))
}

fn payload(value: &str) -> CanonicalElement {
    CanonicalElement::new(
        1,
        BTreeMap::from([
            (1, GraphValue::String(value.to_owned())),
            (2, GraphValue::Integer(42)),
        ]),
    )
}

fn anchor(
    element: ElementRef,
    commit: i64,
    segments: Vec<(Interval<ValidTime>, CanonicalElement)>,
) -> KeyValue {
    let segments = segments
        .into_iter()
        .map(|(valid, payload)| ValidSegment::new(valid, payload))
        .collect();
    let projection = ProjectionRecord::new(tx(commit), segments).unwrap();
    let entry =
        HistoryEntry::Anchor(HistoryAnchor::new(tx(commit), interval(0, 100), projection).unwrap());
    history_entry(element, commit, entry)
}

fn put(
    element: ElementRef,
    commit: i64,
    changed_valid: Interval<ValidTime>,
    payload: CanonicalElement,
) -> KeyValue {
    history_entry(
        element,
        commit,
        HistoryEntry::Delta(HistoryDelta::put(tx(commit), changed_valid, payload)),
    )
}

fn delete(element: ElementRef, commit: i64, changed_valid: Interval<ValidTime>) -> KeyValue {
    history_entry(
        element,
        commit,
        HistoryEntry::Delta(HistoryDelta::delete(tx(commit), changed_valid)),
    )
}

fn history_entry(element: ElementRef, commit: i64, entry: HistoryEntry) -> KeyValue {
    KeyValue::new(
        history_anchor_key(element, tx(commit), 0),
        entry.encode().unwrap(),
    )
}

#[derive(Clone, Copy)]
enum SnapshotBehavior {
    Normal,
    AppliedIndex(u64),
    NonAdvancingContinuation,
}

struct ScriptedSnapshot {
    applied_log_index: u64,
    entries: Vec<KeyValue>,
    page_items: usize,
    behavior: SnapshotBehavior,
    scan_calls: AtomicUsize,
    batch_calls: AtomicUsize,
}

impl ScriptedSnapshot {
    fn new(mut entries: Vec<KeyValue>) -> Self {
        entries.sort_by(|left, right| left.key().cmp(right.key()));
        Self {
            applied_log_index: 7,
            entries,
            page_items: usize::MAX,
            behavior: SnapshotBehavior::Normal,
            scan_calls: AtomicUsize::new(0),
            batch_calls: AtomicUsize::new(0),
        }
    }

    fn with_page_items(mut self, page_items: usize) -> Self {
        self.page_items = page_items;
        self
    }

    fn with_behavior(mut self, behavior: SnapshotBehavior) -> Self {
        self.behavior = behavior;
        self
    }

    fn batch_calls(&self) -> usize {
        self.batch_calls.load(Ordering::Relaxed)
    }

    fn scan_calls(&self) -> usize {
        self.scan_calls.load(Ordering::Relaxed)
    }

    fn page_index(&self) -> u64 {
        match self.behavior {
            SnapshotBehavior::AppliedIndex(index) => index,
            SnapshotBehavior::Normal | SnapshotBehavior::NonAdvancingContinuation => {
                self.applied_log_index
            }
        }
    }

    fn canonical_page(
        &self,
        scan: &CanonicalScanRequest,
    ) -> Result<CanonicalScanPage, AdapterError> {
        let page_index = self.page_index();
        if matches!(self.behavior, SnapshotBehavior::NonAdvancingContinuation) {
            let next_start =
                LogicalKey::in_keyspace(scan.span().keyspace(), scan.span().start().to_vec());
            return CanonicalScanPage::new(scan, page_index, Vec::new(), Some(next_start))
                .map_err(|error| AdapterError::Backend(error.to_string()));
        }

        let matching: Vec<_> = self
            .entries
            .iter()
            .filter(|entry| {
                entry.key().keyspace() == scan.span().keyspace()
                    && scan.span().contains(entry.key().as_bytes())
            })
            .cloned()
            .collect();
        let item_limit = self.page_items.min(scan.bounds().max_items());
        let mut entries = Vec::new();
        let mut retained = 0_u64;
        let mut next_start = None;
        for entry in matching {
            if entries.len() == item_limit {
                next_start = Some(entry.key().clone());
                break;
            }
            let entry_bytes = u64::try_from(entry.key().as_bytes().len())
                .unwrap()
                .checked_add(u64::try_from(entry.value().len()).unwrap())
                .unwrap();
            let required = retained.checked_add(entry_bytes).unwrap();
            if required > scan.bounds().max_bytes() {
                if entries.is_empty() {
                    return Err(AdapterError::ScanByteLimit {
                        limit: scan.bounds().max_bytes(),
                        required,
                    });
                }
                next_start = Some(entry.key().clone());
                break;
            }
            retained = required;
            entries.push(entry);
        }
        CanonicalScanPage::new(scan, page_index, entries, next_start)
            .map_err(|error| AdapterError::Backend(error.to_string()))
    }
}

impl ReadSnapshot for ScriptedSnapshot {
    fn applied_log_index(&self) -> u64 {
        self.applied_log_index
    }

    fn multi_get<'a>(&'a self, _keys: &'a [LogicalKey]) -> AdapterFuture<'a, Vec<Option<Vec<u8>>>> {
        Box::pin(async {
            Err(AdapterError::UnsupportedOperation {
                operation: "test multi-get",
            })
        })
    }

    fn scan<'a>(&'a self, _span: &'a storage_api::KeySpan) -> AdapterFuture<'a, Vec<KeyValue>> {
        Box::pin(async {
            Err(AdapterError::UnsupportedOperation {
                operation: "test scan",
            })
        })
    }

    fn scan_canonical<'a>(
        &'a self,
        request: &'a CanonicalScanRequest,
    ) -> AdapterFuture<'a, CanonicalScanPage> {
        Box::pin(async move {
            self.scan_calls.fetch_add(1, Ordering::Relaxed);
            self.canonical_page(request)
        })
    }

    fn scan_canonical_batch<'a>(
        &'a self,
        request: &'a CanonicalBatchScanRequest,
    ) -> AdapterFuture<'a, CanonicalBatchScanPage> {
        Box::pin(async move {
            self.batch_calls.fetch_add(1, Ordering::Relaxed);
            let mut pages = Vec::with_capacity(request.scans().len());
            for scan in request.scans() {
                pages.push(self.canonical_page(scan)?);
            }
            CanonicalBatchScanPage::new(request, self.page_index(), pages)
                .map_err(|error| AdapterError::Backend(error.to_string()))
        })
    }
}

#[test]
fn budget_rejects_zero_excessive_and_inverted_limits() {
    assert_eq!(
        HistoryReadBudget::new(0, 1, 1),
        Err(TemporalStoreError::InvalidHistoryReadBudget)
    );
    assert_eq!(
        HistoryReadBudget::new(17, 1, 1),
        Err(TemporalStoreError::InvalidHistoryReadBudget)
    );
    assert_eq!(
        HistoryReadBudget::new(16, 0, 0),
        Err(TemporalStoreError::InvalidHistoryReadBudget)
    );
    assert_eq!(
        HistoryReadBudget::new(16, 512, 1024),
        Err(TemporalStoreError::InvalidHistoryReadBudget)
    );
}

#[test]
fn point_reader_replays_only_deltas_covering_the_requested_valid_time() {
    let element = vertex(1);
    let snapshot = ScriptedSnapshot::new(vec![
        anchor(element, 100, vec![(interval(0, 10), payload("anchor"))]),
        put(element, 200, interval(0, 10), payload("visible")),
        put(
            element,
            300,
            interval(20, 30),
            payload(&"x".repeat(8 * 1024)),
        ),
        delete(element, 400, interval(40, 50)),
    ]);

    let outcome =
        block_on(
            PointHistoryReader::new(HistoryReadBudget::new(16, 128 * 1024, 64 * 1024).unwrap())
                .read(&snapshot, element, tx(400), valid(5), PropertyDemand::All),
        )
        .unwrap();

    assert_eq!(outcome.value, Some(payload("visible")));
    assert_eq!(outcome.stats.history_records, 4);
    assert_eq!(outcome.stats.payloads_decoded, 1);
}

#[test]
fn single_point_reader_uses_one_canonical_scan_without_batch_scaffolding() {
    let element = vertex(11);
    let snapshot = ScriptedSnapshot::new(vec![anchor(
        element,
        100,
        vec![(interval(0, 10), payload("single"))],
    )]);

    let outcome = block_on(
        PointHistoryReader::new(HistoryReadBudget::new(16, 4096, 4096).unwrap()).read(
            &snapshot,
            element,
            tx(100),
            valid(5),
            PropertyDemand::All,
        ),
    )
    .unwrap();

    assert_eq!(outcome.value, Some(payload("single")));
    assert_eq!(snapshot.scan_calls(), 1);
    assert_eq!(snapshot.batch_calls(), 0);
}

#[test]
fn point_reader_rejects_an_anchor_larger_than_the_record_budget() {
    let element = vertex(2);
    let snapshot = ScriptedSnapshot::new(vec![anchor(
        element,
        100,
        vec![(interval(0, 10), payload(&"x".repeat(600)))],
    )]);

    assert_eq!(
        block_on(
            PointHistoryReader::new(HistoryReadBudget::new(16, 1024, 512).unwrap()).read(
                &snapshot,
                element,
                tx(200),
                valid(1),
                PropertyDemand::All,
            )
        ),
        Err(TemporalStoreError::HistoryRecordByteLimit)
    );
}

#[test]
fn depths_zero_one_eight_and_fifteen_are_equivalent_across_pages() {
    for depth in [0_usize, 1, 8, 15] {
        let element = vertex(10 + depth as u128);
        let mut entries = vec![anchor(
            element,
            100,
            vec![(interval(0, 10), payload("same"))],
        )];
        for ordinal in 0..depth {
            entries.push(put(
                element,
                101 + ordinal as i64,
                interval(20, 30),
                payload("ignored"),
            ));
        }
        let snapshot = ScriptedSnapshot::new(entries).with_page_items(2);
        let outcome = block_on(
            PointHistoryReader::new(HistoryReadBudget::new(16, 128 * 1024, 64 * 1024).unwrap())
                .read(&snapshot, element, tx(200), valid(5), PropertyDemand::All),
        )
        .unwrap();

        assert_eq!(outcome.value, Some(payload("same")), "depth {depth}");
        assert_eq!(outcome.stats.history_records, depth + 1, "depth {depth}");
        assert_eq!(outcome.stats.payloads_decoded, 1, "depth {depth}");
    }
}

#[test]
fn selected_property_demand_is_applied_to_the_owned_result() {
    let element = vertex(30);
    let snapshot = ScriptedSnapshot::new(vec![anchor(
        element,
        100,
        vec![(interval(0, 10), payload("selected"))],
    )]);
    let outcome = block_on(
        PointHistoryReader::new(HistoryReadBudget::new(16, 4096, 4096).unwrap()).read(
            &snapshot,
            element,
            tx(100),
            valid(5),
            PropertyDemand::Selected(&[2]),
        ),
    )
    .unwrap();

    assert_eq!(
        outcome.value,
        Some(CanonicalElement::new(
            1,
            BTreeMap::from([(2, GraphValue::Integer(42))])
        ))
    );
}

#[test]
fn reader_enforces_total_bytes_and_chain_depth() {
    let total_element = vertex(31);
    let total_entries = vec![
        anchor(
            total_element,
            100,
            vec![(interval(0, 10), payload(&"a".repeat(220)))],
        ),
        put(
            total_element,
            200,
            interval(0, 10),
            payload(&"b".repeat(220)),
        ),
    ];
    let total_bytes: u64 = total_entries
        .iter()
        .map(|entry| u64::try_from(entry.value().len()).unwrap())
        .sum();
    let max_record = total_entries
        .iter()
        .map(|entry| u64::try_from(entry.value().len()).unwrap())
        .max()
        .unwrap();
    let total_snapshot = ScriptedSnapshot::new(total_entries);
    assert_eq!(
        block_on(
            PointHistoryReader::new(
                HistoryReadBudget::new(16, total_bytes - 1, max_record).unwrap()
            )
            .read(
                &total_snapshot,
                total_element,
                tx(200),
                valid(5),
                PropertyDemand::All,
            )
        ),
        Err(TemporalStoreError::HistoryTotalByteLimit)
    );

    let deep_element = vertex(32);
    let deep_entries = (0..16)
        .map(|ordinal| {
            put(
                deep_element,
                100 + ordinal,
                interval(20, 30),
                payload("delta"),
            )
        })
        .collect();
    let deep_snapshot = ScriptedSnapshot::new(deep_entries);
    assert_eq!(
        block_on(
            PointHistoryReader::new(HistoryReadBudget::new(16, 128 * 1024, 64 * 1024).unwrap())
                .read(
                    &deep_snapshot,
                    deep_element,
                    tx(200),
                    valid(5),
                    PropertyDemand::All,
                )
        ),
        Err(TemporalStoreError::HistoryChainTooDeep)
    );
}

#[test]
fn missing_anchor_is_rejected() {
    let element = vertex(33);
    let snapshot =
        ScriptedSnapshot::new(vec![put(element, 100, interval(0, 10), payload("orphan"))]);

    assert_eq!(
        block_on(
            PointHistoryReader::new(HistoryReadBudget::new(16, 4096, 4096).unwrap()).read(
                &snapshot,
                element,
                tx(100),
                valid(5),
                PropertyDemand::All,
            )
        ),
        Err(TemporalStoreError::MissingHistoryAnchor)
    );
}

#[test]
fn applied_index_and_nonadvancing_continuation_are_rejected() {
    let element = vertex(34);
    let mismatched = ScriptedSnapshot::new(vec![anchor(
        element,
        100,
        vec![(interval(0, 10), payload("value"))],
    )])
    .with_behavior(SnapshotBehavior::AppliedIndex(8));
    let reader = PointHistoryReader::new(HistoryReadBudget::new(16, 4096, 4096).unwrap());
    assert_eq!(
        block_on(reader.read(&mismatched, element, tx(100), valid(5), PropertyDemand::All,)),
        Err(TemporalStoreError::HistoryAppliedIndexMismatch {
            expected: 7,
            actual: 8,
        })
    );

    let nonadvancing =
        ScriptedSnapshot::new(Vec::new()).with_behavior(SnapshotBehavior::NonAdvancingContinuation);
    assert_eq!(
        block_on(reader.read(
            &nonadvancing,
            element,
            tx(100),
            valid(5),
            PropertyDemand::All,
        )),
        Err(TemporalStoreError::HistoryContinuationNotAdvancing)
    );
}

#[test]
fn malformed_history_key_and_key_record_timestamp_mismatch_are_rejected() {
    let malformed_element = vertex(35);
    let encoded_anchor = anchor(
        malformed_element,
        100,
        vec![(interval(0, 10), payload("value"))],
    );
    let mut malformed_bytes = encoded_anchor.key().as_bytes().to_vec();
    malformed_bytes.push(0);
    let malformed = KeyValue::new(
        LogicalKey::in_keyspace(encoded_anchor.key().keyspace(), malformed_bytes),
        encoded_anchor.value().to_vec(),
    );
    let malformed_snapshot = ScriptedSnapshot::new(vec![malformed]);
    let reader = PointHistoryReader::new(HistoryReadBudget::new(16, 4096, 4096).unwrap());
    assert_eq!(
        block_on(reader.read(
            &malformed_snapshot,
            malformed_element,
            tx(100),
            valid(5),
            PropertyDemand::All,
        )),
        Err(TemporalStoreError::UnexpectedHistoryKey)
    );

    let mismatch_element = vertex(36);
    let mismatched = KeyValue::new(
        history_anchor_key(mismatch_element, tx(100), 0),
        HistoryEntry::Delta(HistoryDelta::put(tx(99), interval(0, 10), payload("value")))
            .encode()
            .unwrap(),
    );
    let mismatch_snapshot = ScriptedSnapshot::new(vec![mismatched]);
    assert_eq!(
        block_on(reader.read(
            &mismatch_snapshot,
            mismatch_element,
            tx(100),
            valid(5),
            PropertyDemand::All,
        )),
        Err(TemporalStoreError::HistoryKeyTimestampMismatch)
    );
}

#[test]
fn nine_distinct_one_page_requests_use_one_batch_call() {
    let entries = (0_u128..9)
        .map(|ordinal| {
            anchor(
                vertex(100 + ordinal),
                100,
                vec![(interval(0, 10), payload(&format!("value-{ordinal}")))],
            )
        })
        .collect();
    let snapshot = ScriptedSnapshot::new(entries);
    let requests: Vec<_> = (0_u128..9)
        .map(|ordinal| PointHistoryRequest {
            element: vertex(100 + ordinal),
            transaction_time: tx(100),
            valid_time: valid(5),
        })
        .collect();

    let key_bytes =
        u64::try_from(history_anchor_key(vertex(100), tx(100), 0).as_bytes().len()).unwrap();
    let outcomes = block_on(
        PointHistoryReader::new(
            HistoryReadBudget::new(
                16,
                storage_api::MAX_QUERY_PAGE_BYTES,
                storage_api::MAX_QUERY_PAGE_BYTES - key_bytes,
            )
            .unwrap(),
        )
        .read_batch(&snapshot, &requests, PropertyDemand::All),
    )
    .unwrap();

    assert_eq!(outcomes.len(), 9);
    assert_eq!(snapshot.batch_calls(), 1);
    for (ordinal, outcome) in outcomes.into_iter().enumerate() {
        assert_eq!(outcome.value, Some(payload(&format!("value-{ordinal}"))));
    }
}

#[test]
fn oversized_record_after_continuation_is_a_history_record_limit() {
    let element = vertex(151);
    let mut entries = vec![anchor(
        element,
        100,
        vec![(interval(0, 10), payload(&"a".repeat(900)))],
    )];
    entries.extend(
        (0..15).map(|ordinal| put(element, 101 + ordinal, interval(20, 30), payload("small"))),
    );
    let snapshot = ScriptedSnapshot::new(entries).with_page_items(15);

    assert_eq!(
        block_on(
            PointHistoryReader::new(HistoryReadBudget::new(16, 4096, 512).unwrap()).read(
                &snapshot,
                element,
                tx(115),
                valid(5),
                PropertyDemand::All,
            )
        ),
        Err(TemporalStoreError::HistoryRecordByteLimit)
    );
}

#[test]
fn stateful_sidecar_batch_scan_preserves_record_limit_after_continuation() {
    let element = vertex(153);
    let mut entries = vec![anchor(
        element,
        100,
        vec![(interval(0, 10), payload(&"a".repeat(900)))],
    )];
    entries.extend(
        (0..15).map(|ordinal| put(element, 101 + ordinal, interval(20, 30), payload("small"))),
    );
    let backend = Arc::new(MemoryAdapter::new());
    block_on(
        backend.apply_committed(CommittedMutationBatch {
            shard_id: 1,
            log_index: 1,
            txn_id: 1,
            mutations: entries
                .into_iter()
                .enumerate()
                .map(|(sequence, entry)| {
                    Mutation::put(
                        u32::try_from(sequence).unwrap(),
                        entry.key().clone(),
                        entry.value().to_vec(),
                    )
                })
                .collect(),
        }),
    )
    .unwrap();
    let adapter = block_on(SidecarAdapter::connect(StatefulServiceTransport {
        service: Arc::new(SidecarService::new(backend, None)),
    }))
    .unwrap();
    let snapshot = block_on(adapter.begin_read_snapshot()).unwrap();

    assert_eq!(
        block_on(
            PointHistoryReader::new(HistoryReadBudget::new(16, 4096, 512).unwrap()).read(
                snapshot.as_ref(),
                element,
                tx(115),
                valid(5),
                PropertyDemand::All,
            )
        ),
        Err(TemporalStoreError::HistoryRecordByteLimit)
    );
}

#[test]
fn cumulative_bytes_after_continuation_are_a_history_total_limit() {
    let element = vertex(152);
    let mut entries = vec![anchor(
        element,
        100,
        vec![(interval(0, 10), payload(&"anchor".repeat(80)))],
    )];
    entries.extend(
        (0..15).map(|ordinal| put(element, 101 + ordinal, interval(20, 30), payload("small"))),
    );
    let total = entries
        .iter()
        .map(|entry| u64::try_from(entry.value().len()).unwrap())
        .sum::<u64>();
    let max_record = entries
        .iter()
        .map(|entry| u64::try_from(entry.value().len()).unwrap())
        .max()
        .unwrap();
    let snapshot = ScriptedSnapshot::new(entries).with_page_items(15);

    assert_eq!(
        block_on(
            PointHistoryReader::new(HistoryReadBudget::new(16, total - 1, max_record).unwrap(),)
                .read(&snapshot, element, tx(115), valid(5), PropertyDemand::All)
        ),
        Err(TemporalStoreError::HistoryTotalByteLimit)
    );
}

#[test]
fn overwritten_payloads_are_not_decoded_or_copied_into_the_final_result() {
    let element = vertex(150);
    let final_payload = payload("final");
    let snapshot = ScriptedSnapshot::new(vec![
        anchor(element, 100, vec![(interval(0, 10), payload("anchor"))]),
        put(element, 200, interval(0, 10), payload("overwritten")),
        put(element, 300, interval(0, 10), final_payload.clone()),
    ]);

    let outcome = block_on(
        PointHistoryReader::new(HistoryReadBudget::new(16, 4096, 4096).unwrap()).read(
            &snapshot,
            element,
            tx(300),
            valid(5),
            PropertyDemand::Selected(&[1]),
        ),
    )
    .unwrap();

    assert_eq!(
        outcome.value,
        Some(CanonicalElement::new(
            1,
            BTreeMap::from([(1, GraphValue::String("final".to_owned()))]),
        ))
    );
    assert_eq!(outcome.stats.payloads_decoded, 1);
    assert_eq!(
        outcome.stats.payload_bytes_copied,
        u64::try_from(final_payload.encode().unwrap().len()).unwrap() * 2
    );
}

#[test]
fn duplicate_range_with_different_valid_times_is_scanned_once() {
    let element = vertex(200);
    let snapshot = ScriptedSnapshot::new(vec![
        anchor(
            element,
            100,
            vec![
                (interval(0, 10), payload("early")),
                (interval(10, 20), payload("late")),
            ],
        ),
        put(element, 200, interval(0, 10), payload("early-update")),
    ]);
    let requests = [
        PointHistoryRequest {
            element,
            transaction_time: tx(200),
            valid_time: valid(5),
        },
        PointHistoryRequest {
            element,
            transaction_time: tx(200),
            valid_time: valid(15),
        },
    ];

    let outcomes = block_on(
        PointHistoryReader::new(HistoryReadBudget::new(16, 4096, 4096).unwrap()).read_batch(
            &snapshot,
            &requests,
            PropertyDemand::All,
        ),
    )
    .unwrap();

    assert_eq!(snapshot.batch_calls(), 1);
    assert_eq!(outcomes[0].value, Some(payload("early-update")));
    assert_eq!(outcomes[1].value, Some(payload("late")));
    assert_eq!(
        outcomes[1].stats.payload_bytes_copied,
        u64::try_from(payload("late").encode().unwrap().len()).unwrap()
    );
}

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    let mut future = Box::pin(future);
    let waker = std::task::Waker::noop();
    let mut context = std::task::Context::from_waker(waker);
    loop {
        match future.as_mut().poll(&mut context) {
            std::task::Poll::Ready(output) => return output,
            std::task::Poll::Pending => std::thread::yield_now(),
        }
    }
}

struct StatefulServiceTransport {
    service: Arc<SidecarService>,
}

impl SidecarTransport for StatefulServiceTransport {
    fn call<'a>(&'a self, request: adapter_sidecar::Request) -> SidecarTransportFuture<'a> {
        Box::pin(async move { Ok(self.service.dispatch(request).await) })
    }
}
