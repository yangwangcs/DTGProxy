use temporal_types::{CanonicalElement, Interval, TransactionTime, ValidTime};

use crate::{ProjectionRecord, RecordCodecError, ValidSegment};

pub(crate) fn rewrite_projection(
    base: &ProjectionRecord,
    commit_ts: TransactionTime,
    changed_valid: Interval<ValidTime>,
    replacement: Option<CanonicalElement>,
) -> Result<ProjectionRecord, RecordCodecError> {
    let mut segments = Vec::with_capacity(base.segments().len().saturating_add(2));
    for segment in base.segments() {
        if segment.valid().overlaps(&changed_valid) {
            for residual in segment.valid().subtract(&changed_valid) {
                segments.push(ValidSegment::new(residual, segment.payload().clone()));
            }
        } else {
            segments.push(segment.clone());
        }
    }
    if let Some(payload) = replacement {
        segments.push(ValidSegment::new(changed_valid, payload));
    }
    segments.sort_by_key(|segment| segment.valid().start());
    ProjectionRecord::new(commit_ts, coalesce(segments))
}

fn coalesce(segments: Vec<ValidSegment>) -> Vec<ValidSegment> {
    let mut coalesced: Vec<ValidSegment> = Vec::with_capacity(segments.len());
    for segment in segments {
        let can_merge = coalesced.last().is_some_and(|previous| {
            previous.payload() == segment.payload()
                && previous.valid().end() == Some(segment.valid().start())
        });
        if can_merge {
            let previous = coalesced.pop().expect("last element was just inspected");
            let merged = Interval::new(previous.valid().start(), segment.valid().end())
                .expect("adjacent valid intervals form a non-empty interval");
            coalesced.push(ValidSegment::new(merged, segment.payload().clone()));
        } else {
            coalesced.push(segment);
        }
    }
    coalesced
}
