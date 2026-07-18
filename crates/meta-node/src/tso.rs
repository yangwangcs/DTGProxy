use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::sync::{Arc, Mutex};

use temporal_types::TransactionTime;
use timestamp_oracle::{PhysicalClock, TimestampOracleError, advance_timestamp};

const COMMAND_MAGIC: [u8; 4] = *b"DTRS";
const COMMAND_VERSION: u16 = 1;
const COMMAND_BYTES: usize = 62;
const MAX_RESERVATION_SIZE: u32 = 1_048_576;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReserveTimestampCommand {
    command_id: u128,
    expected_high_water: TransactionTime,
    first: TransactionTime,
    new_high_water: TransactionTime,
}

impl ReserveTimestampCommand {
    pub fn new(
        command_id: u128,
        expected_high_water: TransactionTime,
        first: TransactionTime,
        new_high_water: TransactionTime,
    ) -> Result<Self, TsoError> {
        if command_id == 0 || first <= expected_high_water || new_high_water < first {
            return Err(TsoError::InvalidReservation);
        }
        Ok(Self {
            command_id,
            expected_high_water,
            first,
            new_high_water,
        })
    }

    #[must_use]
    pub const fn command_id(&self) -> u128 {
        self.command_id
    }

    #[must_use]
    pub const fn expected_high_water(&self) -> TransactionTime {
        self.expected_high_water
    }

    #[must_use]
    pub const fn new_high_water(&self) -> TransactionTime {
        self.new_high_water
    }

    #[must_use]
    pub const fn first(&self) -> TransactionTime {
        self.first
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut encoded = Vec::with_capacity(COMMAND_BYTES);
        encoded.extend_from_slice(&COMMAND_MAGIC);
        encoded.extend_from_slice(&COMMAND_VERSION.to_be_bytes());
        encoded.extend_from_slice(&self.command_id.to_be_bytes());
        encode_timestamp(&mut encoded, self.expected_high_water);
        encode_timestamp(&mut encoded, self.first);
        encode_timestamp(&mut encoded, self.new_high_water);
        encoded.extend_from_slice(&crc32fast::hash(&encoded).to_be_bytes());
        encoded
    }

    pub fn decode(encoded: &[u8]) -> Result<Self, TsoError> {
        if encoded.len() != COMMAND_BYTES {
            return Err(TsoError::InvalidCommandLength);
        }
        if encoded[..4] != COMMAND_MAGIC {
            return Err(TsoError::InvalidCommandMagic);
        }
        let version = u16::from_be_bytes(encoded[4..6].try_into().expect("fixed TSO version"));
        if version != COMMAND_VERSION {
            return Err(TsoError::UnsupportedCommandVersion { actual: version });
        }
        let stored = u32::from_be_bytes(encoded[58..62].try_into().expect("fixed TSO checksum"));
        if crc32fast::hash(&encoded[..58]) != stored {
            return Err(TsoError::CommandChecksumMismatch);
        }
        Self::new(
            u128::from_be_bytes(encoded[6..22].try_into().expect("fixed command ID")),
            decode_timestamp(&encoded[22..34]),
            decode_timestamp(&encoded[34..46]),
            decode_timestamp(&encoded[46..58]),
        )
    }

    #[must_use]
    pub(crate) fn has_magic(encoded: &[u8]) -> bool {
        encoded.starts_with(&COMMAND_MAGIC)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TimestampBatch {
    first: TransactionTime,
    count: u32,
}

impl TimestampBatch {
    #[must_use]
    pub const fn first(self) -> TransactionTime {
        self.first
    }

    #[must_use]
    pub const fn count(self) -> u32 {
        self.count
    }

    pub fn last(self) -> Result<TransactionTime, TsoError> {
        advance_timestamp(self.first, self.count.saturating_sub(1)).map_err(TsoError::from)
    }
}

struct ActiveLease {
    next: TransactionTime,
    high_water: TransactionTime,
}

pub struct ReplicatedTso {
    clock: Arc<dyn PhysicalClock>,
    reservation_size: u32,
    maximum_future_drift_micros: i64,
    lease: Mutex<Option<ActiveLease>>,
}

impl ReplicatedTso {
    pub fn new(
        clock: Arc<dyn PhysicalClock>,
        reservation_size: u32,
        maximum_future_drift_micros: i64,
    ) -> Result<Self, TsoError> {
        if !(1..=MAX_RESERVATION_SIZE).contains(&reservation_size)
            || maximum_future_drift_micros <= 0
        {
            return Err(TsoError::InvalidConfiguration);
        }
        Ok(Self {
            clock,
            reservation_size,
            maximum_future_drift_micros,
            lease: Mutex::new(None),
        })
    }

    pub fn plan_reservation(
        &self,
        committed_high_water: TransactionTime,
        command_id: u128,
        minimum_count: u32,
    ) -> Result<ReserveTimestampCommand, TsoError> {
        self.plan_reservation_after(committed_high_water, command_id, minimum_count, i64::MIN)
    }

    pub fn plan_reservation_after(
        &self,
        committed_high_water: TransactionTime,
        command_id: u128,
        minimum_count: u32,
        observed_physical_micros: i64,
    ) -> Result<ReserveTimestampCommand, TsoError> {
        if minimum_count == 0 || minimum_count > MAX_RESERVATION_SIZE {
            return Err(TsoError::InvalidReservation);
        }
        let clock = self.clock.now_micros()?.max(observed_physical_micros);
        let first = if clock > committed_high_water.physical_micros() {
            TransactionTime::new(clock, 0)
        } else {
            advance_timestamp(committed_high_water, 1)?
        };
        let size = self.reservation_size.max(minimum_count);
        let high_water = advance_timestamp(first, size - 1)?;
        let future_drift = high_water.physical_micros().saturating_sub(clock);
        if future_drift > self.maximum_future_drift_micros {
            return Err(TsoError::FutureDriftExceeded {
                clock,
                proposed: high_water.physical_micros(),
                maximum: self.maximum_future_drift_micros,
            });
        }
        ReserveTimestampCommand::new(command_id, committed_high_water, first, high_water)
    }

    pub fn activate_committed(
        &self,
        command: &ReserveTimestampCommand,
        committed_high_water: TransactionTime,
    ) -> Result<(), TsoError> {
        if committed_high_water != command.new_high_water {
            return Err(TsoError::ReservationNotCommitted {
                expected: command.new_high_water,
                actual: committed_high_water,
            });
        }
        let next = command.first;
        *self.lease.lock().map_err(|_| TsoError::LockPoisoned)? = Some(ActiveLease {
            next,
            high_water: command.new_high_water,
        });
        Ok(())
    }

    pub fn allocate(&self, count: u32) -> Result<TimestampBatch, TsoError> {
        if count == 0 || count > MAX_RESERVATION_SIZE {
            return Err(TsoError::InvalidReservation);
        }
        let mut lease = self.lease.lock().map_err(|_| TsoError::LockPoisoned)?;
        let lease = lease.as_mut().ok_or(TsoError::LeaseExhausted)?;
        let last = advance_timestamp(lease.next, count - 1)?;
        if last > lease.high_water {
            return Err(TsoError::LeaseExhausted);
        }
        let batch = TimestampBatch {
            first: lease.next,
            count,
        };
        lease.next = advance_timestamp(last, 1)?;
        Ok(batch)
    }

    pub fn fence(&self) -> Result<(), TsoError> {
        *self.lease.lock().map_err(|_| TsoError::LockPoisoned)? = None;
        Ok(())
    }
}

fn encode_timestamp(output: &mut Vec<u8>, timestamp: TransactionTime) {
    output.extend_from_slice(&timestamp.physical_micros().to_be_bytes());
    output.extend_from_slice(&timestamp.logical().to_be_bytes());
}

fn decode_timestamp(encoded: &[u8]) -> TransactionTime {
    TransactionTime::new(
        i64::from_be_bytes(encoded[..8].try_into().expect("fixed TSO physical")),
        u32::from_be_bytes(encoded[8..12].try_into().expect("fixed TSO logical")),
    )
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TsoError {
    InvalidConfiguration,
    InvalidReservation,
    InvalidCommandLength,
    InvalidCommandMagic,
    UnsupportedCommandVersion {
        actual: u16,
    },
    CommandChecksumMismatch,
    StaleHighWater {
        expected: TransactionTime,
        actual: TransactionTime,
    },
    ReservationReplayMismatch {
        command_id: u128,
    },
    ReservationHistoryFull,
    ReservationNotCommitted {
        expected: TransactionTime,
        actual: TransactionTime,
    },
    LeaseExhausted,
    FutureDriftExceeded {
        clock: i64,
        proposed: i64,
        maximum: i64,
    },
    TimestampOracle(String),
    LockPoisoned,
}

impl Display for TsoError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfiguration => {
                formatter.write_str("invalid replicated TSO configuration")
            }
            Self::InvalidReservation => formatter.write_str("invalid timestamp reservation"),
            Self::InvalidCommandLength => formatter.write_str("invalid TSO command length"),
            Self::InvalidCommandMagic => formatter.write_str("invalid TSO command magic"),
            Self::UnsupportedCommandVersion { actual } => {
                write!(formatter, "unsupported TSO command version {actual}")
            }
            Self::CommandChecksumMismatch => formatter.write_str("TSO command checksum mismatch"),
            Self::StaleHighWater { expected, actual } => write!(
                formatter,
                "expected TSO high-water {expected:?}, got {actual:?}"
            ),
            Self::ReservationReplayMismatch { command_id } => write!(
                formatter,
                "TSO reservation {command_id} was replayed with different bounds"
            ),
            Self::ReservationHistoryFull => formatter.write_str("TSO reservation history is full"),
            Self::ReservationNotCommitted { expected, actual } => write!(
                formatter,
                "committed TSO high-water {actual:?} differs from planned {expected:?}"
            ),
            Self::LeaseExhausted => formatter.write_str("local timestamp lease is exhausted"),
            Self::FutureDriftExceeded {
                clock,
                proposed,
                maximum,
            } => write!(
                formatter,
                "TSO high-water {proposed} exceeds clock {clock} by more than {maximum} microseconds"
            ),
            Self::TimestampOracle(message) => {
                write!(formatter, "timestamp arithmetic error: {message}")
            }
            Self::LockPoisoned => formatter.write_str("replicated TSO lock is poisoned"),
        }
    }
}

impl Error for TsoError {}

impl From<TimestampOracleError> for TsoError {
    fn from(error: TimestampOracleError) -> Self {
        Self::TimestampOracle(error.to_string())
    }
}
