use std::sync::Arc;

use storage_api::{
    AdapterError, CanonicalScanRequest, KeySpan, Keyspace, MAX_QUERY_PAGE_BYTES, QueryPageBounds,
    ReadSnapshot,
};
use temporal_types::{CanonicalElement, Interval, TransactionTime, ValidTime};

use crate::{
    ElementRef, GraphKey, HistoryAnchor, HistoryEntryRef, HistoryOperationRef, HistoryReadBudget,
    HistoryReadStats, ProjectionRecord, RecordCodecError, TemporalStoreError, ValidSegment,
    decode_graph_key, history_anchor_key, history_prefix,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IntervalHistoryOutcome {
    pub projection: Option<ProjectionRecord>,
    pub stats: HistoryReadStats,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IntervalHistoryMaterializer {
    budget: HistoryReadBudget,
}

impl IntervalHistoryMaterializer {
    #[must_use]
    pub const fn new(budget: HistoryReadBudget) -> Self {
        Self { budget }
    }

    pub async fn projection_at(
        &self,
        read: &dyn ReadSnapshot,
        element: ElementRef,
        transaction_time: TransactionTime,
    ) -> Result<IntervalHistoryOutcome, TemporalStoreError> {
        let prefix = history_prefix(element);
        let mut start = history_anchor_key(element, transaction_time, 0)
            .as_bytes()
            .to_vec();
        let expected_index = read.applied_log_index();
        let mut stats = HistoryReadStats::default();
        let mut deltas = Vec::new();

        loop {
            let remaining = self
                .budget
                .max_records()
                .checked_sub(stats.history_records)
                .ok_or(TemporalStoreError::HistoryChainTooDeep)?;
            if remaining == 0 {
                return Err(TemporalStoreError::HistoryChainTooDeep);
            }
            let key_bytes = u64::try_from(start.len())
                .map_err(|_| TemporalStoreError::HistoryTotalByteLimit)?;
            let page_bytes = self
                .budget
                .max_record_bytes()
                .checked_add(key_bytes)
                .ok_or(TemporalStoreError::HistoryTotalByteLimit)?
                .min(MAX_QUERY_PAGE_BYTES);
            let span = KeySpan::prefix_from(Keyspace::History, prefix.clone(), start.clone())
                .map_err(|error| {
                    TemporalStoreError::Adapter(AdapterError::Backend(error.to_string()))
                })?;
            let request = CanonicalScanRequest::new(
                span,
                QueryPageBounds::new(remaining, page_bytes).map_err(|error| {
                    TemporalStoreError::Adapter(AdapterError::Backend(error.to_string()))
                })?,
            )
            .map_err(|error| {
                TemporalStoreError::Adapter(AdapterError::Backend(error.to_string()))
            })?;
            let page = match read.scan_canonical(&request).await {
                Ok(page) => page,
                Err(AdapterError::ScanByteLimit { .. }) => {
                    return Err(TemporalStoreError::HistoryRecordByteLimit);
                }
                Err(error) => return Err(error.into()),
            };
            if page.applied_log_index() != expected_index {
                return Err(TemporalStoreError::HistoryAppliedIndexMismatch {
                    expected: expected_index,
                    actual: page.applied_log_index(),
                });
            }
            for entry in page.entries() {
                let key_ts = validate_history_key(entry.key(), element)?;
                charge_record(self.budget, &mut stats, entry.value().len())?;
                let parsed = HistoryEntryRef::parse(entry.value())?;
                if parsed.commit_ts() != key_ts {
                    return Err(TemporalStoreError::HistoryKeyTimestampMismatch);
                }
                match parsed {
                    HistoryEntryRef::Delta(delta) => {
                        let replacement = match delta.operation() {
                            HistoryOperationRef::Put(payload) => {
                                stats.payloads_decoded = stats
                                    .payloads_decoded
                                    .checked_add(1)
                                    .ok_or(TemporalStoreError::HistoryTotalByteLimit)?;
                                stats.payload_bytes_copied = stats
                                    .payload_bytes_copied
                                    .checked_add(
                                        u64::try_from(payload.encoded().len()).map_err(|_| {
                                            TemporalStoreError::HistoryTotalByteLimit
                                        })?,
                                    )
                                    .ok_or(TemporalStoreError::HistoryTotalByteLimit)?;
                                Some(
                                    CanonicalElement::decode(payload.encoded())
                                        .map_err(RecordCodecError::Canonical)?,
                                )
                            }
                            HistoryOperationRef::Delete => None,
                        };
                        deltas.push((delta.commit_ts(), delta.changed_valid(), replacement));
                        if stats.history_records == self.budget.max_records() {
                            return Err(TemporalStoreError::HistoryChainTooDeep);
                        }
                    }
                    HistoryEntryRef::Anchor(_) => {
                        let anchor = HistoryAnchor::decode(entry.value())?;
                        stats.payloads_decoded = stats
                            .payloads_decoded
                            .checked_add(anchor.projection().segments().len())
                            .ok_or(TemporalStoreError::HistoryTotalByteLimit)?;
                        for segment in anchor.projection().segments() {
                            stats.payload_bytes_copied = stats
                                .payload_bytes_copied
                                .checked_add(
                                    u64::try_from(
                                        segment
                                            .payload()
                                            .encode()
                                            .map_err(RecordCodecError::Canonical)?
                                            .len(),
                                    )
                                    .map_err(|_| TemporalStoreError::HistoryTotalByteLimit)?,
                                )
                                .ok_or(TemporalStoreError::HistoryTotalByteLimit)?;
                        }
                        let mut editor = ProjectionEditor::from_projection(anchor.projection());
                        for (commit_ts, changed_valid, replacement) in deltas.into_iter().rev() {
                            editor.apply(commit_ts, changed_valid, replacement)?;
                        }
                        return Ok(IntervalHistoryOutcome {
                            projection: Some(editor.finish()?),
                            stats,
                        });
                    }
                }
            }
            let Some(next) = page.next_start() else {
                if deltas.is_empty() {
                    return Ok(IntervalHistoryOutcome {
                        projection: None,
                        stats,
                    });
                }
                return Err(TemporalStoreError::MissingHistoryAnchor);
            };
            if next.as_bytes() <= start.as_slice() {
                return Err(TemporalStoreError::HistoryContinuationNotAdvancing);
            }
            start = next.as_bytes().to_vec();
        }
    }
}

#[derive(Clone)]
struct SharedSegment {
    valid: Interval<ValidTime>,
    payload: Arc<CanonicalElement>,
}

pub(crate) struct ProjectionEditor {
    commit_ts: TransactionTime,
    segments: Vec<SharedSegment>,
}

impl ProjectionEditor {
    pub(crate) fn from_projection(projection: &ProjectionRecord) -> Self {
        Self {
            commit_ts: projection.commit_ts(),
            segments: projection
                .segments()
                .iter()
                .map(|segment| SharedSegment {
                    valid: segment.valid(),
                    payload: Arc::new(segment.payload().clone()),
                })
                .collect(),
        }
    }

    pub(crate) fn apply(
        &mut self,
        commit_ts: TransactionTime,
        changed_valid: Interval<ValidTime>,
        replacement: Option<CanonicalElement>,
    ) -> Result<(), RecordCodecError> {
        let replacement = replacement.map(Arc::new);
        let mut inserted = false;
        let mut next = Vec::with_capacity(self.segments.len().saturating_add(2));
        for segment in self.segments.drain(..) {
            if !segment.valid.overlaps(&changed_valid) {
                if !inserted && segment.valid.start() >= changed_valid.start() {
                    if let Some(payload) = &replacement {
                        next.push(SharedSegment {
                            valid: changed_valid,
                            payload: Arc::clone(payload),
                        });
                    }
                    inserted = true;
                }
                next.push(segment);
                continue;
            }
            for residual in segment.valid.subtract(&changed_valid) {
                if residual.start() < changed_valid.start() {
                    next.push(SharedSegment {
                        valid: residual,
                        payload: Arc::clone(&segment.payload),
                    });
                } else {
                    if !inserted {
                        if let Some(payload) = &replacement {
                            next.push(SharedSegment {
                                valid: changed_valid,
                                payload: Arc::clone(payload),
                            });
                        }
                        inserted = true;
                    }
                    next.push(SharedSegment {
                        valid: residual,
                        payload: Arc::clone(&segment.payload),
                    });
                }
            }
            if !inserted {
                if let Some(payload) = &replacement {
                    next.push(SharedSegment {
                        valid: changed_valid,
                        payload: Arc::clone(payload),
                    });
                }
                inserted = true;
            }
        }
        if !inserted && let Some(payload) = replacement {
            next.push(SharedSegment {
                valid: changed_valid,
                payload,
            });
        }
        self.commit_ts = commit_ts;
        self.segments = next;
        Ok(())
    }

    pub(crate) fn finish(self) -> Result<ProjectionRecord, RecordCodecError> {
        let mut coalesced: Vec<SharedSegment> = Vec::with_capacity(self.segments.len());
        for segment in self.segments {
            if coalesced.last().is_some_and(|previous| {
                previous.payload == segment.payload
                    && previous.valid.end() == Some(segment.valid.start())
            }) {
                let previous = coalesced.pop().expect("last segment exists");
                coalesced.push(SharedSegment {
                    valid: Interval::new(previous.valid.start(), segment.valid.end())
                        .expect("adjacent intervals form one interval"),
                    payload: previous.payload,
                });
            } else {
                coalesced.push(segment);
            }
        }
        ProjectionRecord::new(
            self.commit_ts,
            coalesced
                .into_iter()
                .map(|segment| {
                    ValidSegment::new(segment.valid, Arc::unwrap_or_clone(segment.payload))
                })
                .collect(),
        )
    }
}

fn validate_history_key(
    key: &storage_api::LogicalKey,
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
    let bytes =
        u64::try_from(record_bytes).map_err(|_| TemporalStoreError::HistoryRecordByteLimit)?;
    if bytes > budget.max_record_bytes() {
        return Err(TemporalStoreError::HistoryRecordByteLimit);
    }
    stats.history_bytes = stats
        .history_bytes
        .checked_add(bytes)
        .ok_or(TemporalStoreError::HistoryTotalByteLimit)?;
    if stats.history_bytes > budget.max_total_bytes() {
        return Err(TemporalStoreError::HistoryTotalByteLimit);
    }
    stats.history_records = stats
        .history_records
        .checked_add(1)
        .ok_or(TemporalStoreError::HistoryChainTooDeep)?;
    if stats.history_records > budget.max_records() {
        return Err(TemporalStoreError::HistoryChainTooDeep);
    }
    Ok(())
}
