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
    fn append(&self, command: Vec<u8>) -> TimestampLogFuture<'_, ()>;
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

    async fn append_and_apply(
        &self,
        state: &mut TimestampState,
        command: TimestampCommand,
    ) -> Result<(), TxnError> {
        self.log.append(encode_command(command)).await?;
        state.apply(command)
    }
}

impl TimestampAuthority for DurableTimestampAuthority {
    fn allocate_start_time(&self, transaction_id: TransactionId) -> TxnFuture<'_, TransactionTime> {
        Box::pin(async move {
            let mut state = self.state.lock().await;
            if let Some(timestamp) = state.start_times.get(&transaction_id) {
                return Ok(*timestamp);
            }
            let timestamp = state.next_timestamp()?;
            self.append_and_apply(
                &mut state,
                TimestampCommand::AllocateStart {
                    transaction_id,
                    timestamp,
                },
            )
            .await?;
            Ok(timestamp)
        })
    }

    fn reserve_commit_time(&self, transaction_id: TransactionId) -> TxnFuture<'_, TransactionTime> {
        Box::pin(async move {
            let mut state = self.state.lock().await;
            if let Some(reservation) = state.reservations.get(&transaction_id) {
                return Ok(reservation.commit_time());
            }
            let timestamp = state.next_timestamp()?;
            self.append_and_apply(
                &mut state,
                TimestampCommand::ReserveCommit {
                    transaction_id,
                    timestamp,
                },
            )
            .await?;
            Ok(timestamp)
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
            let mut state = self.state.lock().await;
            let reservation = state
                .reservations
                .get(&transaction_id)
                .copied()
                .ok_or(TxnError::CorruptRecovery)?;
            if reservation.commit_time() != commit_time {
                return Err(TxnError::CorruptRecovery);
            }
            match reservation.resolution() {
                Some(existing) if existing == resolution => return Ok(()),
                Some(_) => return Err(TxnError::CorruptRecovery),
                None => {}
            }
            self.append_and_apply(
                &mut state,
                TimestampCommand::ResolveCommit {
                    transaction_id,
                    timestamp: commit_time,
                    resolution,
                },
            )
            .await
        })
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

#[derive(Default)]
struct TimestampState {
    last_issued: i64,
    start_times: BTreeMap<TransactionId, TransactionTime>,
    reservations: BTreeMap<TransactionId, CommitTimeReservation>,
}

impl TimestampState {
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
                self.apply_new_timestamp(timestamp)?;
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
