use temporal_types::{CanonicalElement, Interval, TransactionTime, ValidTime};

use crate::rewrite::rewrite_projection;
use crate::{
    HistoryAnchor, HistoryDelta, HistoryEntry, ProjectionRecord, RecordCodecError,
    TemporalStoreError,
};

pub(crate) const MAX_DELTAS_PER_ANCHOR: usize = 15;
pub(crate) const MAX_CHAIN_ENTRIES: usize = MAX_DELTAS_PER_ANCHOR + 1;
const MAX_DELTA_BYTES_PER_ANCHOR: usize = 64 * 1024;

pub(crate) fn entry_for_commit(
    recent_entries: &[HistoryEntry],
    commit_ts: TransactionTime,
    changed_valid: Interval<ValidTime>,
    replacement: Option<CanonicalElement>,
    projection: ProjectionRecord,
) -> Result<HistoryEntry, RecordCodecError> {
    let existing_at_commit = recent_entries
        .first()
        .filter(|entry| entry.commit_ts() == commit_ts);
    let delta = match replacement {
        Some(replacement) => HistoryDelta::put(commit_ts, changed_valid, replacement),
        None => HistoryDelta::delete(commit_ts, changed_valid),
    };
    let new_delta_bytes = delta.encode()?.len();
    let prior_delta_bytes = recent_entries
        .iter()
        .take_while(|entry| !entry.is_anchor())
        .try_fold(0_usize, |total, entry| {
            total
                .checked_add(entry.encode()?.len())
                .ok_or(RecordCodecError::LengthOverflow)
        })?;
    let write_anchor = existing_at_commit.map_or_else(
        || {
            recent_entries.is_empty()
                || recent_entries
                    .iter()
                    .take_while(|entry| !entry.is_anchor())
                    .count()
                    >= MAX_DELTAS_PER_ANCHOR
                || prior_delta_bytes.saturating_add(new_delta_bytes) > MAX_DELTA_BYTES_PER_ANCHOR
        },
        HistoryEntry::is_anchor,
    );

    if write_anchor {
        Ok(HistoryEntry::Anchor(HistoryAnchor::new(
            commit_ts,
            changed_valid,
            projection,
        )?))
    } else {
        Ok(HistoryEntry::Delta(delta))
    }
}

pub(crate) fn reconstruct(
    entries: &[HistoryEntry],
) -> Result<Option<ProjectionRecord>, TemporalStoreError> {
    if entries.is_empty() {
        return Ok(None);
    }
    if entries.len() > MAX_CHAIN_ENTRIES {
        return Err(TemporalStoreError::HistoryChainTooDeep);
    }
    let anchor_index = entries
        .iter()
        .position(HistoryEntry::is_anchor)
        .ok_or(TemporalStoreError::MissingHistoryAnchor)?;
    if anchor_index > MAX_DELTAS_PER_ANCHOR {
        return Err(TemporalStoreError::HistoryChainTooDeep);
    }
    let HistoryEntry::Anchor(anchor) = &entries[anchor_index] else {
        unreachable!("anchor position was just identified")
    };
    let mut projection = anchor.projection().clone();
    for entry in entries[..anchor_index].iter().rev() {
        let HistoryEntry::Delta(delta) = entry else {
            return Err(TemporalStoreError::UnexpectedHistoryAnchor);
        };
        projection = rewrite_projection(
            &projection,
            delta.commit_ts(),
            delta.changed_valid(),
            delta.replacement().cloned(),
        )?;
    }
    Ok(Some(projection))
}
