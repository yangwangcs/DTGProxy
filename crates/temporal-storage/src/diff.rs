use std::collections::BTreeSet;

use temporal_types::{CanonicalElement, Interval, ValidTime};

use crate::ProjectionRecord;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TemporalChange {
    valid: Interval<ValidTime>,
    kind: TemporalChangeKind,
}

impl TemporalChange {
    #[must_use]
    pub const fn new(valid: Interval<ValidTime>, kind: TemporalChangeKind) -> Self {
        Self { valid, kind }
    }

    #[must_use]
    pub const fn valid(&self) -> Interval<ValidTime> {
        self.valid
    }

    #[must_use]
    pub const fn kind(&self) -> &TemporalChangeKind {
        &self.kind
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TemporalChangeKind {
    Added {
        after: CanonicalElement,
    },
    Removed {
        before: CanonicalElement,
    },
    Changed {
        before: CanonicalElement,
        after: CanonicalElement,
    },
}

pub(crate) fn diff_projections(
    before: Option<&ProjectionRecord>,
    after: Option<&ProjectionRecord>,
) -> Vec<TemporalChange> {
    let mut boundaries = BTreeSet::new();
    for projection in [before, after].into_iter().flatten() {
        for segment in projection.segments() {
            boundaries.insert(segment.valid().start());
            if let Some(end) = segment.valid().end() {
                boundaries.insert(end);
            }
        }
    }
    let boundaries: Vec<_> = boundaries.into_iter().collect();
    let mut changes = Vec::new();
    for (index, start) in boundaries.iter().copied().enumerate() {
        let end = boundaries.get(index + 1).copied();
        let before_value = before.and_then(|projection| projection.visible_at(start));
        let after_value = after.and_then(|projection| projection.visible_at(start));
        let kind = match (before_value, after_value) {
            (None, None) => continue,
            (Some(before), Some(after)) if before == after => continue,
            (None, Some(after)) => TemporalChangeKind::Added {
                after: after.clone(),
            },
            (Some(before), None) => TemporalChangeKind::Removed {
                before: before.clone(),
            },
            (Some(before), Some(after)) => TemporalChangeKind::Changed {
                before: before.clone(),
                after: after.clone(),
            },
        };
        let valid = Interval::new(start, end).expect("ordered boundaries form a valid interval");
        push_coalesced(&mut changes, TemporalChange::new(valid, kind));
    }
    changes
}

fn push_coalesced(changes: &mut Vec<TemporalChange>, next: TemporalChange) {
    let can_merge = changes.last().is_some_and(|previous| {
        previous.kind == next.kind && previous.valid.end() == Some(next.valid.start())
    });
    if can_merge {
        let previous = changes.pop().expect("last change was just inspected");
        let valid = Interval::new(previous.valid.start(), next.valid.end())
            .expect("adjacent change ranges form a valid interval");
        changes.push(TemporalChange::new(valid, next.kind));
    } else {
        changes.push(next);
    }
}
