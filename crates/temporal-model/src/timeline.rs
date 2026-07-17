use std::error::Error;
use std::fmt::{self, Display, Formatter};

use temporal_types::{BitemporalVersion, Interval, TransactionTime, ValidTime};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CorrectionSummary {
    pub closed_versions: usize,
    pub opened_versions: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TimelineError {
    InvalidCommitOrder,
    WriteConflict,
    OverlappingVersion,
    InvariantViolation,
}

impl Display for TimelineError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidCommitOrder => {
                formatter.write_str("commit timestamp must follow transaction snapshot")
            }
            Self::WriteConflict => {
                formatter.write_str("write conflict after transaction snapshot")
            }
            Self::OverlappingVersion => {
                formatter.write_str("a visible version already overlaps the valid interval")
            }
            Self::InvariantViolation => {
                formatter.write_str("more than one version is visible at the requested time")
            }
        }
    }
}

impl Error for TimelineError {}

#[derive(Clone, Debug, Eq, PartialEq)]
struct StoredVersion<T> {
    version: BitemporalVersion<T>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Timeline<T> {
    versions: Vec<StoredVersion<T>>,
}

impl<T> Default for Timeline<T> {
    fn default() -> Self {
        Self {
            versions: Vec::new(),
        }
    }
}

impl<T> Timeline<T>
where
    T: Clone,
{
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn put_initial(
        &mut self,
        valid: Interval<ValidTime>,
        value: T,
        commit_ts: TransactionTime,
    ) -> Result<(), TimelineError> {
        let overlaps = self.versions.iter().any(|stored| {
            stored.version.transaction().contains(commit_ts)
                && stored.version.valid().overlaps(&valid)
        });
        if overlaps {
            return Err(TimelineError::OverlappingVersion);
        }

        self.push_version(BitemporalVersion::new(
            value,
            valid,
            Interval::forever_from(commit_ts),
        ));
        Ok(())
    }

    pub fn correct(
        &mut self,
        valid: Interval<ValidTime>,
        value: T,
        read_ts: TransactionTime,
        commit_ts: TransactionTime,
    ) -> Result<CorrectionSummary, TimelineError> {
        if commit_ts <= read_ts {
            return Err(TimelineError::InvalidCommitOrder);
        }

        let stale_overlap = self.versions.iter().any(|stored| {
            let transaction_start = stored.version.transaction().start();
            transaction_start > read_ts
                && transaction_start < commit_ts
                && stored.version.valid().overlaps(&valid)
        });
        if stale_overlap {
            return Err(TimelineError::WriteConflict);
        }

        let affected: Vec<_> = self
            .versions
            .iter()
            .enumerate()
            .filter(|(_, stored)| {
                stored.version.transaction().contains(read_ts)
                    && stored.version.valid().overlaps(&valid)
            })
            .map(|(index, stored)| {
                (
                    index,
                    stored.version.valid(),
                    stored.version.value().clone(),
                )
            })
            .collect();

        let mut residuals = Vec::new();
        for (index, old_valid, old_value) in &affected {
            self.versions[*index]
                .version
                .close_transaction_at(commit_ts)
                .map_err(|_| TimelineError::InvalidCommitOrder)?;
            for residual in old_valid.subtract(&valid) {
                residuals.push((residual, old_value.clone()));
            }
        }

        let residual_count = residuals.len();
        for (residual_valid, residual_value) in residuals {
            self.push_version(BitemporalVersion::new(
                residual_value,
                residual_valid,
                Interval::forever_from(commit_ts),
            ));
        }
        self.push_version(BitemporalVersion::new(
            value,
            valid,
            Interval::forever_from(commit_ts),
        ));

        Ok(CorrectionSummary {
            closed_versions: affected.len(),
            opened_versions: residual_count + 1,
        })
    }

    pub fn value_at(
        &self,
        valid_time: ValidTime,
        transaction_time: TransactionTime,
    ) -> Result<Option<&T>, TimelineError> {
        let mut visible = self
            .versions
            .iter()
            .filter(|stored| {
                stored
                    .version
                    .is_visible_at(valid_time, transaction_time)
            })
            .map(|stored| stored.version.value());

        let value = visible.next();
        if visible.next().is_some() {
            return Err(TimelineError::InvariantViolation);
        }
        Ok(value)
    }

    fn push_version(&mut self, version: BitemporalVersion<T>) {
        self.versions.push(StoredVersion { version });
    }
}
