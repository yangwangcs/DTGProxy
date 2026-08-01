use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use tokio::sync::Mutex;

use crate::{
    CommitResolution, CommitTimeReservation, TimestampAuthority, TransactionId, TransactionTime,
    TxnError, TxnFuture,
};

const COMMAND_MAGIC: &[u8; 4] = b"DTGT";
const COMMAND_VERSION: u16 = 1;
const COMMAND_PREFIX_BYTES: usize = 32;
const COMMAND_BYTES: usize = COMMAND_PREFIX_BYTES + 32;

pub type TimestampLogFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, TxnError>> + Send + 'a>>;

pub trait TimestampCommandLog: Send + Sync {
    fn replay(&self) -> TimestampLogFuture<'_, Vec<Vec<u8>>>;
    /// Atomically appends the complete ordered command batch, or none of it.
    fn append_batch(&self, commands: Vec<Vec<u8>>) -> TimestampLogFuture<'_, ()>;
}

pub struct DurableTimestampAuthority {
    log: Arc<dyn TimestampCommandLog>,
    state: Mutex<TimestampState>,
}

impl DurableTimestampAuthority {
    pub async fn open(log: Arc<dyn TimestampCommandLog>) -> Result<Self, TxnError> {
        let mut state = TimestampState::default();
        for bytes in log.replay().await? {
            state.apply(decode_command(&bytes)?)?;
        }
        Ok(Self {
            log,
            state: Mutex::new(state),
        })
    }

    /// Applies operations in arrival order and persists all newly-created commands as one atomic
    /// log append. Invalid operations fail independently; an append failure leaves the in-memory
    /// state untouched and fails every operation that depended on the append.
    pub async fn apply_batch(
        &self,
        operations: Vec<TimestampOperation>,
    ) -> Vec<Result<TransactionTime, TxnError>> {
        let mut state = self.state.lock().await;
        let mut staged = state.clone();
        let mut commands = Vec::new();
        let mut results = Vec::with_capacity(operations.len());
        let mut persisted = Vec::with_capacity(operations.len());

        for operation in operations {
            let mut candidate = staged.clone();
            match stage_operation(&mut candidate, operation) {
                Ok((timestamp, command)) => {
                    staged = candidate;
                    if let Some(command) = command {
                        commands.push(encode_command(command));
                        persisted.push(results.len());
                    }
                    results.push(Ok(timestamp));
                }
                Err(error) => results.push(Err(error)),
            }
        }

        if !commands.is_empty() {
            if let Err(error) = self.log.append_batch(commands).await {
                for index in persisted {
                    results[index] = Err(error.clone());
                }
                return results;
            }
            *state = staged;
        }
        results
    }
}

impl TimestampAuthority for DurableTimestampAuthority {
    fn allocate_start_time(&self, transaction_id: TransactionId) -> TxnFuture<'_, TransactionTime> {
        Box::pin(async move {
            self.apply_batch(vec![TimestampOperation::AllocateStart { transaction_id }])
                .await
                .into_iter()
                .next()
                .expect("one timestamp operation produces one result")
        })
    }

    fn reserve_commit_time(&self, transaction_id: TransactionId) -> TxnFuture<'_, TransactionTime> {
        Box::pin(async move {
            self.apply_batch(vec![TimestampOperation::ReserveCommit { transaction_id }])
                .await
                .into_iter()
                .next()
                .expect("one timestamp operation produces one result")
        })
    }

    fn commit_time_reservation(
        &self,
        transaction_id: TransactionId,
    ) -> TxnFuture<'_, Option<CommitTimeReservation>> {
        Box::pin(async move {
            Ok(self
                .state
                .lock()
                .await
                .reservations
                .get(&transaction_id)
                .copied())
        })
    }

    fn resolve_commit_time(
        &self,
        transaction_id: TransactionId,
        commit_time: TransactionTime,
        resolution: CommitResolution,
    ) -> TxnFuture<'_, ()> {
        Box::pin(async move {
            self.apply_batch(vec![TimestampOperation::ResolveCommit {
                transaction_id,
                commit_time,
                resolution,
            }])
            .await
            .into_iter()
            .next()
            .expect("one timestamp operation produces one result")
            .map(|_| ())
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TimestampOperation {
    AllocateStart {
        transaction_id: TransactionId,
    },
    ReserveCommit {
        transaction_id: TransactionId,
    },
    ResolveCommit {
        transaction_id: TransactionId,
        commit_time: TransactionTime,
        resolution: CommitResolution,
    },
}

fn stage_operation(
    state: &mut TimestampState,
    operation: TimestampOperation,
) -> Result<(TransactionTime, Option<TimestampCommand>), TxnError> {
    match operation {
        TimestampOperation::AllocateStart { transaction_id } => {
            if let Some(timestamp) = state.start_times.get(&transaction_id) {
                return Ok((*timestamp, None));
            }
            let timestamp = state.next_start_time()?;
            let command = TimestampCommand::AllocateStart {
                transaction_id,
                timestamp,
            };
            state.apply(command)?;
            Ok((timestamp, Some(command)))
        }
        TimestampOperation::ReserveCommit { transaction_id } => {
            if let Some(reservation) = state.reservations.get(&transaction_id) {
                return Ok((reservation.commit_time(), None));
            }
            let timestamp = state.next_timestamp()?;
            let command = TimestampCommand::ReserveCommit {
                transaction_id,
                timestamp,
            };
            state.apply(command)?;
            Ok((timestamp, Some(command)))
        }
        TimestampOperation::ResolveCommit {
            transaction_id,
            commit_time,
            resolution,
        } => {
            let reservation = state
                .reservations
                .get(&transaction_id)
                .copied()
                .ok_or(TxnError::CorruptRecovery)?;
            if reservation.commit_time() != commit_time {
                return Err(TxnError::CorruptRecovery);
            }
            match reservation.resolution() {
                Some(existing) if existing == resolution => return Ok((commit_time, None)),
                Some(_) => return Err(TxnError::CorruptRecovery),
                None => {}
            }
            let command = TimestampCommand::ResolveCommit {
                transaction_id,
                timestamp: commit_time,
                resolution,
            };
            state.apply(command)?;
            Ok((commit_time, Some(command)))
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TimestampCommand {
    AllocateStart {
        transaction_id: TransactionId,
        timestamp: TransactionTime,
    },
    ReserveCommit {
        transaction_id: TransactionId,
        timestamp: TransactionTime,
    },
    ResolveCommit {
        transaction_id: TransactionId,
        timestamp: TransactionTime,
        resolution: CommitResolution,
    },
}

impl TimestampCommand {
    const fn transaction_id(self) -> TransactionId {
        match self {
            Self::AllocateStart { transaction_id, .. }
            | Self::ReserveCommit { transaction_id, .. }
            | Self::ResolveCommit { transaction_id, .. } => transaction_id,
        }
    }

    const fn timestamp(self) -> TransactionTime {
        match self {
            Self::AllocateStart { timestamp, .. }
            | Self::ReserveCommit { timestamp, .. }
            | Self::ResolveCommit { timestamp, .. } => timestamp,
        }
    }
}

#[derive(Clone, Default)]
struct TimestampState {
    last_issued: i64,
    published_frontier: i64,
    start_times: BTreeMap<TransactionId, TransactionTime>,
    reservations: BTreeMap<TransactionId, CommitTimeReservation>,
}

impl TimestampState {
    fn next_start_time(&self) -> Result<TransactionTime, TxnError> {
        if self
            .reservations
            .values()
            .any(|reservation| reservation.resolution().is_none())
        {
            return TransactionTime::new(self.published_frontier)
                .map_err(|_| TxnError::CorruptRecovery);
        }
        self.next_timestamp()
    }

    fn next_timestamp(&self) -> Result<TransactionTime, TxnError> {
        self.last_issued
            .checked_add(1)
            .ok_or(TxnError::ResourceLimit)
            .and_then(|value| TransactionTime::new(value).map_err(|_| TxnError::CorruptRecovery))
    }

    fn apply(&mut self, command: TimestampCommand) -> Result<(), TxnError> {
        match command {
            TimestampCommand::AllocateStart {
                transaction_id,
                timestamp,
            } => {
                if let Some(existing) = self.start_times.get(&transaction_id) {
                    return (*existing == timestamp)
                        .then_some(())
                        .ok_or(TxnError::CorruptRecovery);
                }
                if timestamp.get()
                    == self
                        .last_issued
                        .checked_add(1)
                        .ok_or(TxnError::CorruptRecovery)?
                {
                    if self.published_frontier != self.last_issued {
                        return Err(TxnError::CorruptRecovery);
                    }
                    self.apply_new_timestamp(timestamp)?;
                    self.published_frontier = timestamp.get();
                } else if timestamp.get() != self.published_frontier {
                    return Err(TxnError::CorruptRecovery);
                }
                self.start_times.insert(transaction_id, timestamp);
            }
            TimestampCommand::ReserveCommit {
                transaction_id,
                timestamp,
            } => {
                if let Some(existing) = self.reservations.get(&transaction_id) {
                    return (existing.commit_time() == timestamp)
                        .then_some(())
                        .ok_or(TxnError::CorruptRecovery);
                }
                self.apply_new_timestamp(timestamp)?;
                self.reservations
                    .insert(transaction_id, CommitTimeReservation::new(timestamp, None));
            }
            TimestampCommand::ResolveCommit {
                transaction_id,
                timestamp,
                resolution,
            } => {
                let reservation = self
                    .reservations
                    .get_mut(&transaction_id)
                    .ok_or(TxnError::CorruptRecovery)?;
                if reservation.commit_time() != timestamp {
                    return Err(TxnError::CorruptRecovery);
                }
                match reservation.resolution() {
                    Some(existing) if existing == resolution => {}
                    Some(_) => return Err(TxnError::CorruptRecovery),
                    None => {
                        *reservation = CommitTimeReservation::new(timestamp, Some(resolution));
                        self.advance_published_frontier()?;
                    }
                }
            }
        }
        Ok(())
    }

    fn apply_new_timestamp(&mut self, timestamp: TransactionTime) -> Result<(), TxnError> {
        if timestamp.get()
            != self
                .last_issued
                .checked_add(1)
                .ok_or(TxnError::CorruptRecovery)?
        {
            return Err(TxnError::CorruptRecovery);
        }
        self.last_issued = timestamp.get();
        Ok(())
    }

    fn advance_published_frontier(&mut self) -> Result<(), TxnError> {
        while self.published_frontier < self.last_issued {
            let next = self
                .published_frontier
                .checked_add(1)
                .ok_or(TxnError::CorruptRecovery)?;
            let resolved = self.reservations.values().any(|reservation| {
                reservation.commit_time().get() == next && reservation.resolution().is_some()
            });
            if !resolved {
                break;
            }
            self.published_frontier = next;
        }
        Ok(())
    }
}

fn encode_command(command: TimestampCommand) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(COMMAND_BYTES);
    bytes.extend_from_slice(COMMAND_MAGIC);
    bytes.extend_from_slice(&COMMAND_VERSION.to_be_bytes());
    let (tag, resolution) = match command {
        TimestampCommand::AllocateStart { .. } => (1, 0),
        TimestampCommand::ReserveCommit { .. } => (2, 0),
        TimestampCommand::ResolveCommit { resolution, .. } => (
            3,
            match resolution {
                CommitResolution::Committed => 1,
                CommitResolution::Aborted => 2,
            },
        ),
    };
    bytes.push(tag);
    bytes.extend_from_slice(&command.transaction_id().get().to_be_bytes());
    bytes.extend_from_slice(&command.timestamp().get().to_be_bytes());
    bytes.push(resolution);
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"dtg-timestamp-command-v1");
    hasher.update(&bytes);
    bytes.extend_from_slice(hasher.finalize().as_bytes());
    bytes
}

fn decode_command(bytes: &[u8]) -> Result<TimestampCommand, TxnError> {
    if bytes.len() != COMMAND_BYTES
        || &bytes[..4] != COMMAND_MAGIC
        || u16::from_be_bytes(
            bytes[4..6]
                .try_into()
                .map_err(|_| TxnError::CorruptRecovery)?,
        ) != COMMAND_VERSION
    {
        return Err(TxnError::CorruptRecovery);
    }
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"dtg-timestamp-command-v1");
    hasher.update(&bytes[..COMMAND_PREFIX_BYTES]);
    if hasher.finalize().as_bytes() != &bytes[COMMAND_PREFIX_BYTES..] {
        return Err(TxnError::CorruptRecovery);
    }
    let transaction_id = TransactionId::new(u128::from_be_bytes(
        bytes[7..23]
            .try_into()
            .map_err(|_| TxnError::CorruptRecovery)?,
    ))
    .map_err(|_| TxnError::CorruptRecovery)?;
    let timestamp = TransactionTime::new(i64::from_be_bytes(
        bytes[23..31]
            .try_into()
            .map_err(|_| TxnError::CorruptRecovery)?,
    ))
    .map_err(|_| TxnError::CorruptRecovery)?;
    match (bytes[6], bytes[31]) {
        (1, 0) => Ok(TimestampCommand::AllocateStart {
            transaction_id,
            timestamp,
        }),
        (2, 0) => Ok(TimestampCommand::ReserveCommit {
            transaction_id,
            timestamp,
        }),
        (3, 1) => Ok(TimestampCommand::ResolveCommit {
            transaction_id,
            timestamp,
            resolution: CommitResolution::Committed,
        }),
        (3, 2) => Ok(TimestampCommand::ResolveCommit {
            transaction_id,
            timestamp,
            resolution: CommitResolution::Aborted,
        }),
        _ => Err(TxnError::CorruptRecovery),
    }
}
