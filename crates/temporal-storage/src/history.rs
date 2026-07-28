use temporal_types::{CanonicalElement, Interval, TransactionTime, ValidTime};

use crate::{HistoryAnchor, HistoryDelta, HistoryEntry, ProjectionRecord, RecordCodecError};

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
