#![forbid(unsafe_code)]

use std::error::Error;
use std::fmt::{self, Display, Formatter};

use temporal_types::{Interval, TransactionTime, ValidTime};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TemporalOperation {
    Put,
    Delete,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChangeEvent<T> {
    valid_from: ValidTime,
    commit: TransactionTime,
    operation: TemporalOperation,
    value: Option<T>,
}

impl<T> ChangeEvent<T> {
    #[must_use]
    pub const fn put(valid_from: ValidTime, commit: TransactionTime, value: T) -> Self {
        Self {
            valid_from,
            commit,
            operation: TemporalOperation::Put,
            value: Some(value),
        }
    }

    #[must_use]
    pub const fn delete(valid_from: ValidTime, commit: TransactionTime) -> Self {
        Self {
            valid_from,
            commit,
            operation: TemporalOperation::Delete,
            value: None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DerivedInterval<T> {
    valid: Interval<ValidTime>,
    commit: TransactionTime,
    operation: TemporalOperation,
    value: Option<T>,
}

impl<T> DerivedInterval<T> {
    #[must_use]
    pub const fn valid(&self) -> Interval<ValidTime> {
        self.valid
    }

    #[must_use]
    pub const fn commit(&self) -> TransactionTime {
        self.commit
    }

    #[must_use]
    pub const fn operation(&self) -> TemporalOperation {
        self.operation
    }

    #[must_use]
    pub const fn value(&self) -> Option<&T> {
        self.value.as_ref()
    }
}

pub fn derive_visible_intervals<T>(
    mut events: Vec<ChangeEvent<T>>,
    snapshot: TransactionTime,
    max_events: usize,
) -> Result<Vec<DerivedInterval<T>>, TemporalSemanticError> {
    enforce_limit(events.len(), max_events)?;
    events.retain(|event| event.commit <= snapshot);
    events.sort_by(|left, right| {
        left.valid_from
            .cmp(&right.valid_from)
            .then_with(|| right.commit.cmp(&left.commit))
    });

    let mut visible = Vec::with_capacity(events.len());
    for event in events {
        if visible
            .last()
            .is_some_and(|previous: &ChangeEvent<T>| previous.valid_from == event.valid_from)
        {
            continue;
        }
        visible.push(event);
    }

    let mut visible = visible.into_iter().peekable();
    let mut derived = Vec::new();
    while let Some(event) = visible.next() {
        let end = visible.peek().map(|next| next.valid_from);
        derived.push(DerivedInterval {
            valid: Interval::new(event.valid_from, end)
                .expect("visible events are strictly ordered by valid_from"),
            commit: event.commit,
            operation: event.operation,
            value: event.value,
        });
    }
    Ok(derived)
}

pub fn intersect_interval_sets(
    interval_sets: &[Vec<Interval<ValidTime>>],
    max_cells: usize,
) -> Result<Vec<Interval<ValidTime>>, TemporalSemanticError> {
    if interval_sets.is_empty() {
        return Ok(Vec::new());
    }
    let mut boundaries = interval_sets
        .iter()
        .flatten()
        .flat_map(|interval| [Some(interval.start()), interval.end()])
        .flatten()
        .collect::<Vec<_>>();
    boundaries.sort_unstable();
    boundaries.dedup();

    let mut cells = Vec::new();
    for (index, start) in boundaries.iter().copied().enumerate() {
        let end = boundaries.get(index + 1).copied();
        if !interval_sets
            .iter()
            .all(|intervals| intervals.iter().any(|interval| interval.contains(start)))
        {
            continue;
        }
        enforce_limit(cells.len() + 1, max_cells)?;
        cells.push(
            Interval::new(start, end).expect("ordered interval boundaries form a valid cell"),
        );
    }
    Ok(cells)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IntervalCell<T> {
    valid: Interval<ValidTime>,
    value: T,
    commit: TransactionTime,
    operation: TemporalOperation,
}

impl<T> IntervalCell<T> {
    #[must_use]
    pub const fn new(
        valid: Interval<ValidTime>,
        value: T,
        commit: TransactionTime,
        operation: TemporalOperation,
    ) -> Self {
        Self {
            valid,
            value,
            commit,
            operation,
        }
    }

    #[must_use]
    pub const fn valid(&self) -> Interval<ValidTime> {
        self.valid
    }
}

pub fn coalesce_interval_cells<T>(
    cells: Vec<IntervalCell<T>>,
    preserve_provenance: bool,
    max_cells: usize,
) -> Result<Vec<IntervalCell<T>>, TemporalSemanticError>
where
    T: Eq,
{
    enforce_limit(cells.len(), max_cells)?;
    let mut output: Vec<IntervalCell<T>> = Vec::with_capacity(cells.len());
    for cell in cells {
        let Some(previous) = output.last_mut() else {
            output.push(cell);
            continue;
        };
        let same_values = previous.value == cell.value;
        let same_provenance =
            previous.commit == cell.commit && previous.operation == cell.operation;
        if same_values
            && (!preserve_provenance || same_provenance)
            && previous.valid.end() == Some(cell.valid.start())
        {
            previous.valid = Interval::new(previous.valid.start(), cell.valid.end())
                .expect("adjacent cells form a valid interval");
        } else {
            output.push(cell);
        }
    }
    Ok(output)
}

fn enforce_limit(actual: usize, limit: usize) -> Result<(), TemporalSemanticError> {
    if actual > limit {
        Err(TemporalSemanticError::LimitExceeded { limit, actual })
    } else {
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TemporalSemanticError {
    LimitExceeded { limit: usize, actual: usize },
}

impl Display for TemporalSemanticError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::LimitExceeded { limit, actual } => {
                write!(
                    formatter,
                    "temporal semantic cell limit {limit} exceeded by {actual}"
                )
            }
        }
    }
}

impl Error for TemporalSemanticError {}
