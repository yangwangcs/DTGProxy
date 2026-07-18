use std::error::Error;
use std::fmt::{self, Display, Formatter};

const MAX_VOTERS: usize = 1_024;
const MAX_ERROR_BYTES: usize = 4_096;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MigrationState {
    Preparing,
    Copying,
    CatchingUp,
    Ready,
    Committing,
    Committed,
    Cleaning,
    Cleaned,
    Aborting,
    Aborted,
}

impl MigrationState {
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Cleaned | Self::Aborted)
    }

    #[must_use]
    pub const fn can_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::Preparing, Self::Copying)
                | (Self::Copying, Self::CatchingUp)
                | (Self::CatchingUp, Self::Ready)
                | (Self::Ready, Self::Committing)
                | (Self::Committing, Self::Committed)
                | (Self::Committed, Self::Cleaning)
                | (Self::Cleaning, Self::Cleaned)
                | (Self::Preparing, Self::Aborting)
                | (Self::Copying, Self::Aborting)
                | (Self::CatchingUp, Self::Aborting)
                | (Self::Ready, Self::Aborting)
                | (Self::Aborting, Self::Aborted)
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MigrationProgress {
    pub(super) snapshot_index: Option<u64>,
    pub(super) snapshot_checksum: Option<[u8; 32]>,
    pub(super) catchup_index: u64,
    pub(super) cutover_index: u64,
    pub(super) owner_term: u64,
    pub(super) updated_at_unix_ms: u64,
}

impl MigrationProgress {
    pub fn new(owner_term: u64, updated_at_unix_ms: u64) -> Result<Self, MigrationError> {
        if owner_term == 0 || updated_at_unix_ms == 0 {
            return Err(MigrationError::InvalidProgress);
        }
        Ok(Self {
            snapshot_index: None,
            snapshot_checksum: None,
            catchup_index: 0,
            cutover_index: 0,
            owner_term,
            updated_at_unix_ms,
        })
    }

    pub fn with_snapshot(
        mut self,
        snapshot_index: u64,
        snapshot_checksum: [u8; 32],
    ) -> Result<Self, MigrationError> {
        if snapshot_index == 0 || snapshot_checksum == [0; 32] {
            return Err(MigrationError::InvalidProgress);
        }
        self.snapshot_index = Some(snapshot_index);
        self.snapshot_checksum = Some(snapshot_checksum);
        Ok(self)
    }

    pub fn with_catchup_index(mut self, catchup_index: u64) -> Result<Self, MigrationError> {
        if catchup_index == 0 {
            return Err(MigrationError::InvalidProgress);
        }
        self.catchup_index = catchup_index;
        Ok(self)
    }

    pub fn with_cutover_index(mut self, cutover_index: u64) -> Result<Self, MigrationError> {
        if cutover_index == 0 {
            return Err(MigrationError::InvalidProgress);
        }
        self.cutover_index = cutover_index;
        Ok(self)
    }

    #[must_use]
    pub const fn snapshot_index(&self) -> Option<u64> {
        self.snapshot_index
    }

    #[must_use]
    pub const fn snapshot_checksum(&self) -> Option<[u8; 32]> {
        self.snapshot_checksum
    }

    #[must_use]
    pub const fn catchup_index(&self) -> u64 {
        self.catchup_index
    }

    #[must_use]
    pub const fn cutover_index(&self) -> u64 {
        self.cutover_index
    }

    #[must_use]
    pub const fn owner_term(&self) -> u64 {
        self.owner_term
    }

    #[must_use]
    pub const fn updated_at_unix_ms(&self) -> u64 {
        self.updated_at_unix_ms
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MigrationRecord {
    pub(super) migration_id: u128,
    pub(super) graph_id: u64,
    pub(super) shard_id: u32,
    pub(super) source_epoch: u64,
    pub(super) target_epoch: u64,
    pub(super) source_voters: Vec<u64>,
    pub(super) target_voters: Vec<u64>,
    pub(super) state: MigrationState,
    pub(super) state_revision: u64,
    pub(super) snapshot_index: Option<u64>,
    pub(super) snapshot_checksum: Option<[u8; 32]>,
    pub(super) catchup_index: u64,
    pub(super) cutover_index: u64,
    pub(super) owner_term: u64,
    pub(super) retry_count: u32,
    pub(super) last_error: Option<String>,
    pub(super) created_at_unix_ms: u64,
    pub(super) updated_at_unix_ms: u64,
}

impl MigrationRecord {
    #[allow(clippy::too_many_arguments)]
    pub fn new_shard(
        migration_id: u128,
        graph_id: u64,
        shard_id: u32,
        source_epoch: u64,
        target_epoch: u64,
        source_voters: Vec<u64>,
        target_voters: Vec<u64>,
        owner_term: u64,
        created_at_unix_ms: u64,
    ) -> Result<Self, MigrationError> {
        validate_identity(
            migration_id,
            graph_id,
            shard_id,
            source_epoch,
            target_epoch,
            &source_voters,
            &target_voters,
            owner_term,
            created_at_unix_ms,
        )?;
        Ok(Self {
            migration_id,
            graph_id,
            shard_id,
            source_epoch,
            target_epoch,
            source_voters,
            target_voters,
            state: MigrationState::Preparing,
            state_revision: 1,
            snapshot_index: None,
            snapshot_checksum: None,
            catchup_index: 0,
            cutover_index: 0,
            owner_term,
            retry_count: 0,
            last_error: None,
            created_at_unix_ms,
            updated_at_unix_ms: created_at_unix_ms,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn restore(
        migration_id: u128,
        graph_id: u64,
        shard_id: u32,
        source_epoch: u64,
        target_epoch: u64,
        source_voters: Vec<u64>,
        target_voters: Vec<u64>,
        state: MigrationState,
        state_revision: u64,
        snapshot_index: Option<u64>,
        snapshot_checksum: Option<[u8; 32]>,
        catchup_index: u64,
        cutover_index: u64,
        owner_term: u64,
        retry_count: u32,
        last_error: Option<String>,
        created_at_unix_ms: u64,
        updated_at_unix_ms: u64,
    ) -> Result<Self, MigrationError> {
        validate_identity(
            migration_id,
            graph_id,
            shard_id,
            source_epoch,
            target_epoch,
            &source_voters,
            &target_voters,
            owner_term,
            created_at_unix_ms,
        )?;
        validate_error(last_error.as_deref())?;
        if state_revision == 0
            || updated_at_unix_ms < created_at_unix_ms
            || snapshot_index.is_some() != snapshot_checksum.is_some()
            || snapshot_index == Some(0)
            || snapshot_checksum == Some([0; 32])
            || (catchup_index > 0 && snapshot_index.is_none())
            || snapshot_index.is_some_and(|index| catchup_index > 0 && catchup_index < index)
            || (cutover_index > 0 && (catchup_index == 0 || cutover_index < catchup_index))
        {
            return Err(MigrationError::InvalidRecord);
        }
        match state {
            MigrationState::CatchingUp if snapshot_index.is_none() => {
                return Err(MigrationError::MissingSnapshotFence);
            }
            MigrationState::Ready | MigrationState::Committing
                if snapshot_index.is_none()
                    || catchup_index < snapshot_index.expect("checked snapshot fence") =>
            {
                return Err(MigrationError::MissingCatchupFence);
            }
            MigrationState::Committed | MigrationState::Cleaning | MigrationState::Cleaned
                if snapshot_index.is_none()
                    || catchup_index < snapshot_index.expect("checked snapshot fence")
                    || cutover_index < catchup_index =>
            {
                return Err(MigrationError::MissingCutoverFence);
            }
            _ => {}
        }
        Ok(Self {
            migration_id,
            graph_id,
            shard_id,
            source_epoch,
            target_epoch,
            source_voters,
            target_voters,
            state,
            state_revision,
            snapshot_index,
            snapshot_checksum,
            catchup_index,
            cutover_index,
            owner_term,
            retry_count,
            last_error,
            created_at_unix_ms,
            updated_at_unix_ms,
        })
    }

    pub(super) fn advance(
        &mut self,
        expected_state_revision: u64,
        next: MigrationState,
        progress: MigrationProgress,
    ) -> Result<(), MigrationError> {
        self.ensure_revision(expected_state_revision)?;
        if !self.state.can_transition_to(next) {
            return Err(MigrationError::IllegalTransition {
                from: self.state,
                to: next,
            });
        }
        self.validate_progress(&progress)?;
        match next {
            MigrationState::CatchingUp if progress.snapshot_index.is_none() => {
                return Err(MigrationError::MissingSnapshotFence);
            }
            MigrationState::Ready
                if progress.snapshot_index.is_none()
                    || progress.catchup_index
                        < progress.snapshot_index.expect("checked snapshot fence") =>
            {
                return Err(MigrationError::MissingCatchupFence);
            }
            MigrationState::Committed
                if progress.catchup_index == 0
                    || progress.cutover_index < progress.catchup_index =>
            {
                return Err(MigrationError::MissingCutoverFence);
            }
            _ => {}
        }
        self.install_progress(progress);
        self.state = next;
        self.state_revision = self
            .state_revision
            .checked_add(1)
            .ok_or(MigrationError::RevisionExhausted)?;
        self.last_error = None;
        Ok(())
    }

    pub(super) fn fail(
        &mut self,
        expected_state_revision: u64,
        owner_term: u64,
        updated_at_unix_ms: u64,
        error: String,
    ) -> Result<(), MigrationError> {
        self.ensure_revision(expected_state_revision)?;
        if self.state.is_terminal() {
            return Err(MigrationError::TerminalWorkflow);
        }
        validate_error(Some(&error))?;
        if owner_term < self.owner_term || updated_at_unix_ms < self.updated_at_unix_ms {
            return Err(MigrationError::ProgressRegression);
        }
        self.owner_term = owner_term;
        self.updated_at_unix_ms = updated_at_unix_ms;
        self.retry_count = self
            .retry_count
            .checked_add(1)
            .ok_or(MigrationError::RetryCountExhausted)?;
        self.state_revision = self
            .state_revision
            .checked_add(1)
            .ok_or(MigrationError::RevisionExhausted)?;
        self.last_error = Some(error);
        Ok(())
    }

    fn ensure_revision(&self, actual: u64) -> Result<(), MigrationError> {
        if actual == self.state_revision {
            Ok(())
        } else {
            Err(MigrationError::StaleStateRevision {
                expected: self.state_revision,
                actual,
            })
        }
    }

    fn validate_progress(&self, progress: &MigrationProgress) -> Result<(), MigrationError> {
        if progress.owner_term < self.owner_term
            || progress.updated_at_unix_ms < self.updated_at_unix_ms
            || progress.snapshot_index.is_some() != progress.snapshot_checksum.is_some()
            || progress.snapshot_index == Some(0)
            || progress.snapshot_checksum == Some([0; 32])
            || (self.snapshot_index.is_some()
                && (progress.snapshot_index != self.snapshot_index
                    || progress.snapshot_checksum != self.snapshot_checksum))
            || progress.catchup_index < self.catchup_index
            || progress.cutover_index < self.cutover_index
            || (progress.catchup_index > 0 && progress.snapshot_index.is_none())
            || progress
                .snapshot_index
                .is_some_and(|index| progress.catchup_index > 0 && progress.catchup_index < index)
            || (progress.cutover_index > 0
                && (progress.catchup_index == 0 || progress.cutover_index < progress.catchup_index))
        {
            return Err(MigrationError::ProgressRegression);
        }
        Ok(())
    }

    fn install_progress(&mut self, progress: MigrationProgress) {
        self.snapshot_index = progress.snapshot_index;
        self.snapshot_checksum = progress.snapshot_checksum;
        self.catchup_index = progress.catchup_index;
        self.cutover_index = progress.cutover_index;
        self.owner_term = progress.owner_term;
        self.updated_at_unix_ms = progress.updated_at_unix_ms;
    }

    #[must_use]
    pub const fn migration_id(&self) -> u128 {
        self.migration_id
    }

    #[must_use]
    pub const fn graph_id(&self) -> u64 {
        self.graph_id
    }

    #[must_use]
    pub const fn shard_id(&self) -> u32 {
        self.shard_id
    }

    #[must_use]
    pub const fn source_epoch(&self) -> u64 {
        self.source_epoch
    }

    #[must_use]
    pub const fn target_epoch(&self) -> u64 {
        self.target_epoch
    }

    #[must_use]
    pub fn source_voters(&self) -> &[u64] {
        &self.source_voters
    }

    #[must_use]
    pub fn target_voters(&self) -> &[u64] {
        &self.target_voters
    }

    #[must_use]
    pub const fn state(&self) -> MigrationState {
        self.state
    }

    #[must_use]
    pub const fn state_revision(&self) -> u64 {
        self.state_revision
    }

    #[must_use]
    pub const fn snapshot_index(&self) -> Option<u64> {
        self.snapshot_index
    }

    #[must_use]
    pub const fn snapshot_checksum(&self) -> Option<[u8; 32]> {
        self.snapshot_checksum
    }

    #[must_use]
    pub const fn catchup_index(&self) -> u64 {
        self.catchup_index
    }

    #[must_use]
    pub const fn cutover_index(&self) -> u64 {
        self.cutover_index
    }

    #[must_use]
    pub const fn owner_term(&self) -> u64 {
        self.owner_term
    }

    #[must_use]
    pub const fn retry_count(&self) -> u32 {
        self.retry_count
    }

    #[must_use]
    pub fn last_error(&self) -> Option<&str> {
        self.last_error.as_deref()
    }

    #[must_use]
    pub const fn created_at_unix_ms(&self) -> u64 {
        self.created_at_unix_ms
    }

    #[must_use]
    pub const fn updated_at_unix_ms(&self) -> u64 {
        self.updated_at_unix_ms
    }
}

#[allow(clippy::too_many_arguments)]
fn validate_identity(
    migration_id: u128,
    graph_id: u64,
    shard_id: u32,
    source_epoch: u64,
    target_epoch: u64,
    source_voters: &[u64],
    target_voters: &[u64],
    owner_term: u64,
    created_at_unix_ms: u64,
) -> Result<(), MigrationError> {
    if migration_id == 0
        || graph_id == 0
        || shard_id == 0
        || source_epoch == 0
        || target_epoch != source_epoch.checked_add(1).unwrap_or(0)
        || owner_term == 0
        || created_at_unix_ms == 0
        || source_voters == target_voters
        || !valid_voters(source_voters)
        || !valid_voters(target_voters)
    {
        return Err(MigrationError::InvalidRecord);
    }
    Ok(())
}

fn valid_voters(voters: &[u64]) -> bool {
    !voters.is_empty()
        && voters.len() <= MAX_VOTERS
        && voters[0] != 0
        && voters.windows(2).all(|pair| pair[0] < pair[1])
}

fn validate_error(error: Option<&str>) -> Result<(), MigrationError> {
    if error.is_some_and(|message| {
        message.is_empty()
            || message.len() > MAX_ERROR_BYTES
            || message.chars().any(char::is_control)
    }) {
        Err(MigrationError::InvalidErrorMessage)
    } else {
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MigrationError {
    InvalidRecord,
    InvalidProgress,
    InvalidErrorMessage,
    UnknownMigration {
        migration_id: u128,
    },
    ActiveWorkflowConflict {
        graph_id: u64,
        shard_id: u32,
    },
    SourcePlacementMismatch {
        graph_id: u64,
        shard_id: u32,
    },
    StaleStateRevision {
        expected: u64,
        actual: u64,
    },
    IllegalTransition {
        from: MigrationState,
        to: MigrationState,
    },
    MissingSnapshotFence,
    MissingCatchupFence,
    MissingCutoverFence,
    ProgressRegression,
    TerminalWorkflow,
    RevisionExhausted,
    RetryCountExhausted,
}

impl Display for MigrationError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRecord => formatter.write_str("invalid migration record"),
            Self::InvalidProgress => formatter.write_str("invalid migration progress"),
            Self::InvalidErrorMessage => formatter.write_str("invalid migration error message"),
            Self::UnknownMigration { migration_id } => {
                write!(formatter, "migration {migration_id} does not exist")
            }
            Self::ActiveWorkflowConflict { graph_id, shard_id } => {
                write!(
                    formatter,
                    "graph {graph_id} Shard {shard_id} already has an active migration"
                )
            }
            Self::SourcePlacementMismatch { graph_id, shard_id } => {
                write!(
                    formatter,
                    "graph {graph_id} Shard {shard_id} source placement does not match Catalog"
                )
            }
            Self::StaleStateRevision { expected, actual } => {
                write!(
                    formatter,
                    "migration state revision {actual} is stale; expected {expected}"
                )
            }
            Self::IllegalTransition { from, to } => {
                write!(formatter, "illegal migration transition {from:?} -> {to:?}")
            }
            Self::MissingSnapshotFence => {
                formatter.write_str("migration snapshot fence is missing")
            }
            Self::MissingCatchupFence => formatter.write_str("migration catch-up fence is missing"),
            Self::MissingCutoverFence => formatter.write_str("migration cutover fence is missing"),
            Self::ProgressRegression => formatter.write_str("migration progress regressed"),
            Self::TerminalWorkflow => formatter.write_str("migration workflow is terminal"),
            Self::RevisionExhausted => formatter.write_str("migration state revision exhausted"),
            Self::RetryCountExhausted => formatter.write_str("migration retry count exhausted"),
        }
    }
}

impl Error for MigrationError {}
