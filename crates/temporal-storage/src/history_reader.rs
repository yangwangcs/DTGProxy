use std::collections::BTreeMap;

use storage_api::{
    AdapterError, CanonicalBatchScanRequest, CanonicalScanRequest, KeySpan, Keyspace, LogicalKey,
    MAX_CANONICAL_BATCH_RANGES, MAX_QUERY_PAGE_BYTES, QueryPageBounds, ReadSnapshot,
};
use temporal_types::{CanonicalElement, CanonicalElementRef, TransactionTime, ValidTime};

use crate::history::MAX_CHAIN_ENTRIES;
use crate::{
    ElementRef, GraphKey, HistoryEntryRef, HistoryOperationRef, TemporalStoreError,
    decode_graph_key, history_anchor_key, history_prefix,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HistoryReadBudget {
    max_records: usize,
    max_total_bytes: u64,
    max_record_bytes: u64,
}

impl HistoryReadBudget {
    pub fn new(
        max_records: usize,
        max_total_bytes: u64,
        max_record_bytes: u64,
    ) -> Result<Self, TemporalStoreError> {
        if !(1..=MAX_CHAIN_ENTRIES).contains(&max_records)
            || max_total_bytes == 0
            || max_record_bytes == 0
            || max_record_bytes > max_total_bytes
        {
            return Err(TemporalStoreError::InvalidHistoryReadBudget);
        }
        Ok(Self {
            max_records,
            max_total_bytes,
            max_record_bytes,
        })
    }

    #[must_use]
    pub const fn max_records(self) -> usize {
        self.max_records
    }

    #[must_use]
    pub const fn max_total_bytes(self) -> u64 {
        self.max_total_bytes
    }

    #[must_use]
    pub const fn max_record_bytes(self) -> u64 {
        self.max_record_bytes
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct HistoryReadStats {
    pub history_records: usize,
    pub history_bytes: u64,
    pub payloads_decoded: usize,
    pub payload_bytes_copied: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PropertyDemand<'a> {
    All,
    Selected(&'a [u32]),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PointHistoryRequest {
    pub element: ElementRef,
    pub transaction_time: TransactionTime,
    pub valid_time: ValidTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PointHistoryOutcome {
    pub value: Option<CanonicalElement>,
    pub stats: HistoryReadStats,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PointHistoryReader {
    budget: HistoryReadBudget,
}

impl PointHistoryReader {
    #[must_use]
    pub const fn new(budget: HistoryReadBudget) -> Self {
        Self { budget }
    }

    pub async fn read(
        &self,
        read: &dyn ReadSnapshot,
        element: ElementRef,
        transaction_time: TransactionTime,
        valid_time: ValidTime,
        demand: PropertyDemand<'_>,
    ) -> Result<PointHistoryOutcome, TemporalStoreError> {
        let request = PointHistoryRequest {
            element,
            transaction_time,
            valid_time,
        };
        let mut outcomes = self.read_batch(read, &[request], demand).await?;
        Ok(outcomes
            .pop()
            .expect("single point history request has one outcome"))
    }

    pub async fn read_batch(
        &self,
        read: &dyn ReadSnapshot,
        requests: &[PointHistoryRequest],
        demand: PropertyDemand<'_>,
    ) -> Result<Vec<PointHistoryOutcome>, TemporalStoreError> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }

        let mut range_positions = BTreeMap::new();
        let mut ranges = Vec::new();
        for (ordinal, request) in requests.iter().copied().enumerate() {
            let range_key = (request.element, request.transaction_time);
            let range_ordinal = if let Some(&range_ordinal) = range_positions.get(&range_key) {
                range_ordinal
            } else {
                let range_ordinal = ranges.len();
                range_positions.insert(range_key, range_ordinal);
                ranges.push(RangeReplay::new(request.element, request.transaction_time));
                range_ordinal
            };
            ranges[range_ordinal].points.push(PointReplay {
                ordinal,
                valid_time: request.valid_time,
                replacement: None,
                payload_bytes_copied: 0,
            });
        }

        let expected_applied_log_index = read.applied_log_index();
        let mut outcomes = (0..requests.len()).map(|_| None).collect::<Vec<_>>();
        while ranges.iter().any(|range| !range.complete) {
            let mut max_ranges = MAX_CANONICAL_BATCH_RANGES;
            let (range_ordinals, page) = loop {
                let (range_ordinals, scans, max_total_bytes) =
                    self.build_batch(&ranges, max_ranges)?;
                let request = CanonicalBatchScanRequest::new(scans, max_total_bytes)
                    .map_err(query_primitive_error)?;
                match read.scan_canonical_batch(&request).await {
                    Ok(page) => break (range_ordinals, page),
                    Err(AdapterError::ScanByteLimit { .. }) if range_ordinals.len() > 1 => {
                        max_ranges = range_ordinals.len().div_ceil(2);
                    }
                    Err(AdapterError::ScanByteLimit { .. }) => {
                        return Err(TemporalStoreError::HistoryRecordByteLimit);
                    }
                    Err(error) => return Err(TemporalStoreError::Adapter(error)),
                }
            };
            if page.applied_log_index() != expected_applied_log_index {
                return Err(TemporalStoreError::HistoryAppliedIndexMismatch {
                    expected: expected_applied_log_index,
                    actual: page.applied_log_index(),
                });
            }

            for (range_ordinal, page) in range_ordinals.into_iter().zip(page.into_pages()) {
                self.consume_page(&mut ranges[range_ordinal], page, demand, &mut outcomes)?;
            }
        }

        outcomes
            .into_iter()
            .map(|outcome| outcome.ok_or(TemporalStoreError::MissingHistoryAnchor))
            .collect()
    }

    fn build_batch(
        &self,
        ranges: &[RangeReplay],
        max_ranges: usize,
    ) -> Result<(Vec<usize>, Vec<CanonicalScanRequest>, u64), TemporalStoreError> {
        let range_ordinals = ranges
            .iter()
            .enumerate()
            .filter_map(|(ordinal, range)| (!range.complete).then_some(ordinal))
            .take(max_ranges.min(MAX_CANONICAL_BATCH_RANGES))
            .collect::<Vec<_>>();
        let per_range_bytes = MAX_QUERY_PAGE_BYTES
            .checked_div(
                u64::try_from(range_ordinals.len())
                    .map_err(|_| TemporalStoreError::HistoryTotalByteLimit)?,
            )
            .ok_or(TemporalStoreError::HistoryTotalByteLimit)?;
        let mut scans = Vec::with_capacity(range_ordinals.len());
        let mut aggregate_bytes = 0_u64;

        for &range_ordinal in &range_ordinals {
            let range = &ranges[range_ordinal];
            let remaining_records = self
                .budget
                .max_records
                .checked_sub(range.stats.history_records)
                .ok_or(TemporalStoreError::HistoryChainTooDeep)?;
            if remaining_records == 0 {
                return Err(TemporalStoreError::HistoryChainTooDeep);
            }
            let key_bytes = u64::try_from(
                history_anchor_key(range.element, range.transaction_time, 0)
                    .as_bytes()
                    .len(),
            )
            .map_err(|_| TemporalStoreError::HistoryTotalByteLimit)?;
            let requested_bytes = self
                .budget
                .max_record_bytes
                .checked_add(key_bytes)
                .ok_or(TemporalStoreError::HistoryTotalByteLimit)?
                .min(per_range_bytes)
                .min(MAX_QUERY_PAGE_BYTES);
            let next_aggregate = aggregate_bytes
                .checked_add(requested_bytes)
                .ok_or(TemporalStoreError::HistoryTotalByteLimit)?;

            let span =
                KeySpan::prefix_from(Keyspace::History, range.prefix.clone(), range.start.clone())
                    .map_err(|error| {
                        TemporalStoreError::Adapter(AdapterError::Backend(error.to_string()))
                    })?;
            let bounds = QueryPageBounds::new(remaining_records, requested_bytes)
                .map_err(query_primitive_error)?;
            scans.push(CanonicalScanRequest::new(span, bounds).map_err(query_primitive_error)?);
            aggregate_bytes = next_aggregate;
        }

        Ok((range_ordinals, scans, aggregate_bytes))
    }

    fn consume_page(
        &self,
        range: &mut RangeReplay,
        page: storage_api::CanonicalScanPage,
        demand: PropertyDemand<'_>,
        outcomes: &mut [Option<PointHistoryOutcome>],
    ) -> Result<(), TemporalStoreError> {
        let next_start = page.next_start().cloned();
        for entry in page.entries() {
            let key_transaction_time = validate_history_key(entry.key(), range.element)?;
            charge_record(self.budget, &mut range.stats, entry.value().len())?;
            let parsed = HistoryEntryRef::parse(entry.value())?;
            if parsed.commit_ts() != key_transaction_time {
                return Err(TemporalStoreError::HistoryKeyTimestampMismatch);
            }

            match parsed {
                HistoryEntryRef::Delta(delta) => {
                    range.saw_delta = true;
                    for point in &mut range.points {
                        if point.replacement.is_some()
                            || !delta.changed_valid().contains(point.valid_time)
                        {
                            continue;
                        }
                        point.replacement = Some(match delta.operation() {
                            HistoryOperationRef::Put(payload) => {
                                point.payload_bytes_copied = payload_len(payload)?;
                                OwnedPointDelta::Put(payload.encoded().to_vec())
                            }
                            HistoryOperationRef::Delete => OwnedPointDelta::Delete,
                        });
                    }
                    if range.stats.history_records == self.budget.max_records {
                        return Err(TemporalStoreError::HistoryChainTooDeep);
                    }
                }
                HistoryEntryRef::Anchor(anchor) => {
                    for point in &range.points {
                        let mut stats = range.stats;
                        stats.payload_bytes_copied = point.payload_bytes_copied;
                        let value = match &point.replacement {
                            Some(OwnedPointDelta::Put(encoded)) => {
                                let payload = CanonicalElementRef::parse(encoded)
                                    .map_err(crate::RecordCodecError::Canonical)?;
                                Some(decode_payload(payload, demand, &mut stats)?)
                            }
                            Some(OwnedPointDelta::Delete) => None,
                            None => match anchor.projection().visible_at(point.valid_time)? {
                                Some(payload) => Some(decode_payload(payload, demand, &mut stats)?),
                                None => None,
                            },
                        };
                        outcomes[point.ordinal] = Some(PointHistoryOutcome { value, stats });
                    }
                    range.complete = true;
                    return Ok(());
                }
            }
        }

        let Some(next_start) = next_start else {
            if range.saw_delta {
                return Err(TemporalStoreError::MissingHistoryAnchor);
            }
            for point in &range.points {
                outcomes[point.ordinal] = Some(PointHistoryOutcome {
                    value: None,
                    stats: range.stats,
                });
            }
            range.complete = true;
            return Ok(());
        };
        if next_start.keyspace() != Keyspace::History
            || next_start.as_bytes() <= range.start.as_slice()
        {
            return Err(TemporalStoreError::HistoryContinuationNotAdvancing);
        }
        range.start = next_start.as_bytes().to_vec();
        Ok(())
    }
}

struct RangeReplay {
    element: ElementRef,
    transaction_time: TransactionTime,
    prefix: Vec<u8>,
    start: Vec<u8>,
    points: Vec<PointReplay>,
    stats: HistoryReadStats,
    saw_delta: bool,
    complete: bool,
}

impl RangeReplay {
    fn new(element: ElementRef, transaction_time: TransactionTime) -> Self {
        Self {
            element,
            transaction_time,
            prefix: history_prefix(element),
            start: history_anchor_key(element, transaction_time, 0)
                .as_bytes()
                .to_vec(),
            points: Vec::new(),
            stats: HistoryReadStats::default(),
            saw_delta: false,
            complete: false,
        }
    }
}

struct PointReplay {
    ordinal: usize,
    valid_time: ValidTime,
    replacement: Option<OwnedPointDelta>,
    payload_bytes_copied: u64,
}

enum OwnedPointDelta {
    Put(Vec<u8>),
    Delete,
}

fn validate_history_key(
    key: &LogicalKey,
    expected_element: ElementRef,
) -> Result<TransactionTime, TemporalStoreError> {
    let Ok(GraphKey::HistoryAnchor {
        element,
        transaction_time,
        ..
    }) = decode_graph_key(key)
    else {
        return Err(TemporalStoreError::UnexpectedHistoryKey);
    };
    if element != expected_element {
        return Err(TemporalStoreError::UnexpectedHistoryKey);
    }
    Ok(transaction_time)
}

fn charge_record(
    budget: HistoryReadBudget,
    stats: &mut HistoryReadStats,
    record_bytes: usize,
) -> Result<(), TemporalStoreError> {
    let record_bytes =
        u64::try_from(record_bytes).map_err(|_| TemporalStoreError::HistoryRecordByteLimit)?;
    if record_bytes > budget.max_record_bytes {
        return Err(TemporalStoreError::HistoryRecordByteLimit);
    }
    let history_bytes = stats
        .history_bytes
        .checked_add(record_bytes)
        .ok_or(TemporalStoreError::HistoryTotalByteLimit)?;
    if history_bytes > budget.max_total_bytes {
        return Err(TemporalStoreError::HistoryTotalByteLimit);
    }
    stats.history_records = stats
        .history_records
        .checked_add(1)
        .ok_or(TemporalStoreError::HistoryChainTooDeep)?;
    if stats.history_records > budget.max_records {
        return Err(TemporalStoreError::HistoryChainTooDeep);
    }
    stats.history_bytes = history_bytes;
    Ok(())
}

fn decode_payload(
    payload: CanonicalElementRef<'_>,
    demand: PropertyDemand<'_>,
    stats: &mut HistoryReadStats,
) -> Result<CanonicalElement, TemporalStoreError> {
    let copied = payload_len(payload)?;
    let decoded = match demand {
        PropertyDemand::All => CanonicalElement::decode(payload.encoded())
            .map_err(crate::RecordCodecError::Canonical)?,
        PropertyDemand::Selected(properties) => payload
            .project(properties)
            .map_err(crate::RecordCodecError::Canonical)?,
    };
    stats.payloads_decoded = stats
        .payloads_decoded
        .checked_add(1)
        .ok_or(TemporalStoreError::HistoryTotalByteLimit)?;
    stats.payload_bytes_copied = stats
        .payload_bytes_copied
        .checked_add(copied)
        .ok_or(TemporalStoreError::HistoryTotalByteLimit)?;
    Ok(decoded)
}

fn payload_len(payload: CanonicalElementRef<'_>) -> Result<u64, TemporalStoreError> {
    u64::try_from(payload.encoded().len()).map_err(|_| TemporalStoreError::HistoryTotalByteLimit)
}

fn query_primitive_error(error: storage_api::QueryPrimitiveError) -> TemporalStoreError {
    TemporalStoreError::Adapter(AdapterError::Backend(error.to_string()))
}
