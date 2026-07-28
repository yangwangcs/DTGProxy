use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::sync::{Arc, Mutex};

use query_executor::{
    AblationAxis, BenchmarkAblationConfig, BenchmarkAblationCounters,
    BenchmarkAblationCountersSnapshot,
};

#[derive(Clone, Debug)]
pub struct BenchmarkAblationRuntime {
    state: Arc<Mutex<RuntimeState>>,
}

impl Default for BenchmarkAblationRuntime {
    fn default() -> Self {
        Self::new()
    }
}

impl BenchmarkAblationRuntime {
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(RuntimeState::default())),
        }
    }

    pub fn begin_cell(
        &self,
        cell_id: &str,
        configuration_digest: &str,
        config: BenchmarkAblationConfig,
    ) -> Result<BenchmarkCellStarted, BenchmarkControlError> {
        if cell_id.trim().is_empty()
            || cell_id.len() > 256
            || configuration_digest.len() != 64
            || !configuration_digest
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
            || disabled_axes(config).len() > 1
        {
            return Err(BenchmarkControlError::InvalidConfiguration);
        }
        let mut state = self.lock()?;
        if let Some(active) = state.active.as_ref() {
            if active.cell_id == cell_id
                && active
                    .configuration_digest
                    .eq_ignore_ascii_case(configuration_digest)
                && active.config == config
                && active.queries_started == 0
            {
                return Ok(BenchmarkCellStarted {
                    session_token: active.token.clone(),
                    accepted_config: active.config,
                });
            }
            return Err(BenchmarkControlError::SessionAlreadyActive);
        }
        let mut token = [0_u8; 32];
        getrandom::fill(&mut token).map_err(|_| BenchmarkControlError::RandomSourceFailed)?;
        let token = hex(&token);
        state.last_finished = None;
        state.active = Some(ActiveSession {
            token: token.clone(),
            cell_id: cell_id.to_owned(),
            configuration_digest: configuration_digest.to_ascii_lowercase(),
            config,
            counters: Arc::new(BenchmarkAblationCounters::default()),
            queries_started: 0,
            queries_completed: 0,
            queries_failed: 0,
            queries_in_flight: 0,
        });
        Ok(BenchmarkCellStarted {
            session_token: token,
            accepted_config: config,
        })
    }

    pub fn has_active_session(&self) -> Result<bool, BenchmarkControlError> {
        Ok(self.lock()?.active.is_some())
    }

    pub fn acquire(&self, token: &str) -> Result<BenchmarkQueryLease, BenchmarkControlError> {
        let mut state = self.lock()?;
        let session = matching_session_mut(&mut state.active, token)?;
        session.queries_started = session
            .queries_started
            .checked_add(1)
            .ok_or(BenchmarkControlError::CounterOverflow)?;
        session.queries_in_flight = session
            .queries_in_flight
            .checked_add(1)
            .ok_or(BenchmarkControlError::CounterOverflow)?;
        Ok(BenchmarkQueryLease {
            runtime: self.clone(),
            token: token.to_owned(),
            config: session.config,
            counters: Arc::clone(&session.counters),
            outcome: None,
        })
    }

    pub fn finish_cell(&self, token: &str) -> Result<BenchmarkCellFinished, BenchmarkControlError> {
        let mut state = self.lock()?;
        if state.active.is_none() {
            return state
                .last_finished
                .as_ref()
                .filter(|finished| finished.token == token)
                .map(|finished| finished.evidence.clone())
                .ok_or(BenchmarkControlError::NoActiveSession);
        }
        let session = matching_session(&state.active, token)?;
        if session.queries_in_flight != 0 {
            return Err(BenchmarkControlError::QueriesInFlight {
                count: session.queries_in_flight,
            });
        }
        let terminal_error = if session.queries_failed != 0 {
            Some(BenchmarkControlError::QueriesFailed {
                count: session.queries_failed,
            })
        } else if session.queries_started == 0
            || session.queries_started
                != session
                    .queries_completed
                    .checked_add(session.queries_failed)
                    .and_then(|value| value.checked_add(session.queries_in_flight))
                    .ok_or(BenchmarkControlError::CounterOverflow)?
        {
            Some(BenchmarkControlError::NoCompletedQueries)
        } else {
            let counters = session.counters.snapshot();
            validate_counters(session.config, counters).err()
        };
        if let Some(error) = terminal_error {
            state.active = None;
            return Err(error);
        }
        let counters = session.counters.snapshot();
        let finished = BenchmarkCellFinished {
            cell_id: session.cell_id.clone(),
            configuration_digest: session.configuration_digest.clone(),
            config: session.config,
            queries_started: session.queries_started,
            queries_completed: session.queries_completed,
            queries_failed: session.queries_failed,
            queries_in_flight: session.queries_in_flight,
            counters,
        };
        state.active = None;
        state.last_finished = Some(FinishedSession {
            token: token.to_owned(),
            evidence: finished.clone(),
        });
        Ok(finished)
    }

    pub fn abort_cell(&self, token: &str) -> Result<(), BenchmarkControlError> {
        let mut state = self.lock()?;
        matching_session(&state.active, token)?;
        state.active = None;
        Ok(())
    }

    fn record_outcome(
        &self,
        token: &str,
        outcome: QueryOutcome,
    ) -> Result<(), BenchmarkControlError> {
        let mut state = self.lock()?;
        let session = matching_session_mut(&mut state.active, token)?;
        session.queries_in_flight = session
            .queries_in_flight
            .checked_sub(1)
            .ok_or(BenchmarkControlError::CounterUnderflow)?;
        let counter = match outcome {
            QueryOutcome::Completed => &mut session.queries_completed,
            QueryOutcome::Failed => &mut session.queries_failed,
        };
        *counter = counter
            .checked_add(1)
            .ok_or(BenchmarkControlError::CounterOverflow)?;
        Ok(())
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, RuntimeState>, BenchmarkControlError> {
        self.state
            .lock()
            .map_err(|_| BenchmarkControlError::StatePoisoned)
    }
}

#[derive(Debug, Default)]
struct RuntimeState {
    active: Option<ActiveSession>,
    last_finished: Option<FinishedSession>,
}

#[derive(Debug)]
struct FinishedSession {
    token: String,
    evidence: BenchmarkCellFinished,
}

#[derive(Debug)]
struct ActiveSession {
    token: String,
    cell_id: String,
    configuration_digest: String,
    config: BenchmarkAblationConfig,
    counters: Arc<BenchmarkAblationCounters>,
    queries_started: u64,
    queries_completed: u64,
    queries_failed: u64,
    queries_in_flight: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BenchmarkCellStarted {
    session_token: String,
    accepted_config: BenchmarkAblationConfig,
}

impl BenchmarkCellStarted {
    #[must_use]
    pub fn session_token(&self) -> &str {
        &self.session_token
    }

    #[must_use]
    pub const fn accepted_config(&self) -> BenchmarkAblationConfig {
        self.accepted_config
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BenchmarkCellFinished {
    cell_id: String,
    configuration_digest: String,
    config: BenchmarkAblationConfig,
    queries_started: u64,
    queries_completed: u64,
    queries_failed: u64,
    queries_in_flight: u64,
    counters: BenchmarkAblationCountersSnapshot,
}

impl BenchmarkCellFinished {
    #[must_use]
    pub fn cell_id(&self) -> &str {
        &self.cell_id
    }

    #[must_use]
    pub fn configuration_digest(&self) -> &str {
        &self.configuration_digest
    }

    #[must_use]
    pub const fn config(&self) -> BenchmarkAblationConfig {
        self.config
    }

    #[must_use]
    pub const fn queries_started(&self) -> u64 {
        self.queries_started
    }

    #[must_use]
    pub const fn queries_completed(&self) -> u64 {
        self.queries_completed
    }

    #[must_use]
    pub const fn queries_failed(&self) -> u64 {
        self.queries_failed
    }

    #[must_use]
    pub const fn queries_in_flight(&self) -> u64 {
        self.queries_in_flight
    }

    #[must_use]
    pub const fn counters(&self) -> BenchmarkAblationCountersSnapshot {
        self.counters
    }
}

#[derive(Debug)]
pub struct BenchmarkQueryLease {
    runtime: BenchmarkAblationRuntime,
    token: String,
    config: BenchmarkAblationConfig,
    counters: Arc<BenchmarkAblationCounters>,
    outcome: Option<QueryOutcome>,
}

impl BenchmarkQueryLease {
    #[must_use]
    pub const fn config(&self) -> BenchmarkAblationConfig {
        self.config
    }

    #[must_use]
    pub fn counters(&self) -> Arc<BenchmarkAblationCounters> {
        Arc::clone(&self.counters)
    }

    pub fn complete(mut self) -> Result<(), BenchmarkControlError> {
        self.outcome = Some(QueryOutcome::Completed);
        self.runtime
            .record_outcome(&self.token, QueryOutcome::Completed)
    }

    pub fn fail(mut self) -> Result<(), BenchmarkControlError> {
        self.outcome = Some(QueryOutcome::Failed);
        self.runtime
            .record_outcome(&self.token, QueryOutcome::Failed)
    }
}

impl Drop for BenchmarkQueryLease {
    fn drop(&mut self) {
        if self.outcome.is_none() {
            self.outcome = Some(QueryOutcome::Failed);
            let _ = self
                .runtime
                .record_outcome(&self.token, QueryOutcome::Failed);
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum QueryOutcome {
    Completed,
    Failed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BenchmarkControlError {
    InvalidConfiguration,
    SessionAlreadyActive,
    NoActiveSession,
    InvalidSessionToken,
    RandomSourceFailed,
    StatePoisoned,
    CounterOverflow,
    CounterUnderflow,
    QueriesInFlight { count: u64 },
    QueriesFailed { count: u64 },
    NoCompletedQueries,
    AblationNotExercised { axis: AblationAxis },
    UnexpectedAblationCounter { axis: AblationAxis },
}

impl Display for BenchmarkControlError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "benchmark ablation control failed: {self:?}")
    }
}

impl Error for BenchmarkControlError {}

fn matching_session<'a>(
    state: &'a Option<ActiveSession>,
    token: &str,
) -> Result<&'a ActiveSession, BenchmarkControlError> {
    let session = state
        .as_ref()
        .ok_or(BenchmarkControlError::NoActiveSession)?;
    if session.token != token {
        return Err(BenchmarkControlError::InvalidSessionToken);
    }
    Ok(session)
}

fn matching_session_mut<'a>(
    state: &'a mut Option<ActiveSession>,
    token: &str,
) -> Result<&'a mut ActiveSession, BenchmarkControlError> {
    let session = state
        .as_mut()
        .ok_or(BenchmarkControlError::NoActiveSession)?;
    if session.token != token {
        return Err(BenchmarkControlError::InvalidSessionToken);
    }
    Ok(session)
}

fn validate_counters(
    config: BenchmarkAblationConfig,
    counters: BenchmarkAblationCountersSnapshot,
) -> Result<(), BenchmarkControlError> {
    let counts = counter_counts(counters);
    let disabled = disabled_axes(config);
    for (axis, count) in counts {
        if disabled.as_slice() == [axis] {
            if count == 0 {
                return Err(BenchmarkControlError::AblationNotExercised { axis });
            }
        } else if count != 0 {
            return Err(BenchmarkControlError::UnexpectedAblationCounter { axis });
        }
    }
    Ok(())
}

fn disabled_axes(config: BenchmarkAblationConfig) -> Vec<AblationAxis> {
    [
        (!config.native_pushdown, AblationAxis::NativePushdown),
        (!config.column_batches, AblationAxis::ColumnBatches),
        (!config.bounded_lazy_pages, AblationAxis::BoundedLazyPages),
        (
            !config.parallel_shard_fanout,
            AblationAxis::ParallelShardFanout,
        ),
        (
            !config.batched_property_gather,
            AblationAxis::BatchedPropertyGather,
        ),
    ]
    .into_iter()
    .filter_map(|(disabled, axis)| disabled.then_some(axis))
    .collect()
}

fn counter_counts(counters: BenchmarkAblationCountersSnapshot) -> [(AblationAxis, u64); 5] {
    [
        (
            AblationAxis::NativePushdown,
            counters.canonical_residual_scans(),
        ),
        (
            AblationAxis::ColumnBatches,
            counters.row_column_conversion_boundaries(),
        ),
        (
            AblationAxis::BoundedLazyPages,
            counters.eager_page_collections(),
        ),
        (
            AblationAxis::ParallelShardFanout,
            counters.serial_shard_opens(),
        ),
        (
            AblationAxis::BatchedPropertyGather,
            counters.singleton_property_gather_reads(),
        ),
    ]
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(DIGITS[usize::from(byte >> 4)]));
        encoded.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    encoded
}
