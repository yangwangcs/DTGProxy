#![forbid(unsafe_code)]

use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use temporal_types::TransactionTime;

const STATE_MAGIC: [u8; 4] = *b"DTSO";
const STATE_VERSION: u16 = 1;
const STATE_BYTES: usize = 22;
const MAX_RESERVATION_SIZE: u32 = 1_048_576;
pub const MIN_TRANSACTION_TIME: TransactionTime = TransactionTime::new(i64::MIN, 0);

pub trait TimestampStore: Send + Sync {
    fn load_reserved_through(&self) -> Result<Option<TransactionTime>, TimestampOracleError>;
    fn persist_reserved_through(
        &self,
        timestamp: TransactionTime,
    ) -> Result<(), TimestampOracleError>;
}

pub trait PhysicalClock: Send + Sync {
    fn now_micros(&self) -> Result<i64, TimestampOracleError>;
}

#[derive(Debug, Default)]
pub struct SystemClock;

impl PhysicalClock for SystemClock {
    fn now_micros(&self) -> Result<i64, TimestampOracleError> {
        match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(duration) => i64::try_from(duration.as_micros())
                .map_err(|_| TimestampOracleError::ClockOutOfRange),
            Err(error) => {
                let magnitude = i64::try_from(error.duration().as_micros())
                    .map_err(|_| TimestampOracleError::ClockOutOfRange)?;
                magnitude
                    .checked_neg()
                    .ok_or(TimestampOracleError::ClockOutOfRange)
            }
        }
    }
}

#[derive(Debug)]
pub struct ManualClock {
    micros: AtomicI64,
}

impl ManualClock {
    #[must_use]
    pub const fn new(micros: i64) -> Self {
        Self {
            micros: AtomicI64::new(micros),
        }
    }

    pub fn set(&self, micros: i64) {
        self.micros.store(micros, Ordering::Release);
    }
}

impl PhysicalClock for ManualClock {
    fn now_micros(&self) -> Result<i64, TimestampOracleError> {
        Ok(self.micros.load(Ordering::Acquire))
    }
}

#[derive(Debug, Default)]
pub struct MemoryTimestampStore {
    reserved_through: Mutex<Option<TransactionTime>>,
}

impl MemoryTimestampStore {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            reserved_through: Mutex::new(None),
        }
    }

    pub fn reserved_through(&self) -> Result<Option<TransactionTime>, TimestampOracleError> {
        self.load_reserved_through()
    }
}

impl TimestampStore for MemoryTimestampStore {
    fn load_reserved_through(&self) -> Result<Option<TransactionTime>, TimestampOracleError> {
        self.reserved_through
            .lock()
            .map(|value| *value)
            .map_err(|_| TimestampOracleError::LockPoisoned)
    }

    fn persist_reserved_through(
        &self,
        timestamp: TransactionTime,
    ) -> Result<(), TimestampOracleError> {
        let mut reserved = self
            .reserved_through
            .lock()
            .map_err(|_| TimestampOracleError::LockPoisoned)?;
        if reserved.is_some_and(|current| timestamp <= current) {
            return Err(TimestampOracleError::ReservationRegressed {
                current: reserved.expect("reservation checked as present"),
                proposed: timestamp,
            });
        }
        *reserved = Some(timestamp);
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct FileTimestampStore {
    path: PathBuf,
}

impl FileTimestampStore {
    #[must_use]
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
        }
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl TimestampStore for FileTimestampStore {
    fn load_reserved_through(&self) -> Result<Option<TransactionTime>, TimestampOracleError> {
        let bytes = match fs::read(&self.path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(TimestampOracleError::Io(error.to_string())),
        };
        decode_state(&bytes).map(Some)
    }

    fn persist_reserved_through(
        &self,
        timestamp: TransactionTime,
    ) -> Result<(), TimestampOracleError> {
        if let Some(current) = self.load_reserved_through()?
            && timestamp <= current
        {
            return Err(TimestampOracleError::ReservationRegressed {
                current,
                proposed: timestamp,
            });
        }
        let parent = nonempty_parent(&self.path);
        fs::create_dir_all(parent).map_err(|error| TimestampOracleError::Io(error.to_string()))?;
        let temporary = temporary_path(&self.path);
        let bytes = encode_state(timestamp);
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temporary)
            .map_err(|error| TimestampOracleError::Io(error.to_string()))?;
        file.write_all(&bytes)
            .and_then(|()| file.sync_all())
            .map_err(|error| TimestampOracleError::Io(error.to_string()))?;
        fs::rename(&temporary, &self.path)
            .map_err(|error| TimestampOracleError::Io(error.to_string()))?;
        File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| TimestampOracleError::Io(error.to_string()))?;
        Ok(())
    }
}

struct OracleState {
    last_issued: TransactionTime,
    reserved_through: TransactionTime,
}

pub struct TimestampOracle {
    store: Arc<dyn TimestampStore>,
    clock: Arc<dyn PhysicalClock>,
    reservation_size: u32,
    state: Mutex<OracleState>,
}

impl TimestampOracle {
    pub fn open(
        store: Arc<dyn TimestampStore>,
        clock: Arc<dyn PhysicalClock>,
        reservation_size: u32,
    ) -> Result<Self, TimestampOracleError> {
        if !(1..=MAX_RESERVATION_SIZE).contains(&reservation_size) {
            return Err(TimestampOracleError::InvalidReservationSize {
                max: MAX_RESERVATION_SIZE,
                actual: reservation_size,
            });
        }
        let reserved_through = store
            .load_reserved_through()?
            .unwrap_or(MIN_TRANSACTION_TIME);
        Ok(Self {
            store,
            clock,
            reservation_size,
            state: Mutex::new(OracleState {
                last_issued: reserved_through,
                reserved_through,
            }),
        })
    }

    pub fn production(
        path: impl AsRef<Path>,
        reservation_size: u32,
    ) -> Result<Self, TimestampOracleError> {
        Self::open(
            Arc::new(FileTimestampStore::new(path)),
            Arc::new(SystemClock),
            reservation_size,
        )
    }

    pub fn next(&self) -> Result<TransactionTime, TimestampOracleError> {
        self.allocate_after(None)
    }

    pub fn next_after(
        &self,
        fence: TransactionTime,
    ) -> Result<TransactionTime, TimestampOracleError> {
        self.allocate_after(Some(fence))
    }

    pub fn snapshot(&self) -> Result<SnapshotToken, TimestampOracleError> {
        self.next().map(SnapshotToken::new)
    }

    pub fn closed_timestamp(&self) -> Result<TransactionTime, TimestampOracleError> {
        self.state
            .lock()
            .map(|state| state.last_issued)
            .map_err(|_| TimestampOracleError::LockPoisoned)
    }

    fn allocate_after(
        &self,
        fence: Option<TransactionTime>,
    ) -> Result<TransactionTime, TimestampOracleError> {
        let observed_physical = self.clock.now_micros()?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| TimestampOracleError::LockPoisoned)?;
        let base = fence.map_or(state.last_issued, |fence| state.last_issued.max(fence));
        let candidate = if observed_physical > base.physical_micros() {
            TransactionTime::new(observed_physical, 0)
        } else {
            advance_timestamp(base, 1)?
        };
        if candidate > state.reserved_through {
            let reserved_through = advance_timestamp(candidate, self.reservation_size - 1)?;
            self.store.persist_reserved_through(reserved_through)?;
            state.reserved_through = reserved_through;
        }
        state.last_issued = candidate;
        Ok(candidate)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SnapshotToken {
    transaction_time: TransactionTime,
}

impl SnapshotToken {
    #[must_use]
    pub const fn new(transaction_time: TransactionTime) -> Self {
        Self { transaction_time }
    }

    #[must_use]
    pub const fn transaction_time(self) -> TransactionTime {
        self.transaction_time
    }
}

fn advance_timestamp(
    timestamp: TransactionTime,
    steps: u32,
) -> Result<TransactionTime, TimestampOracleError> {
    let physical_offset = u128::from(
        u64::try_from(i128::from(timestamp.physical_micros()) - i128::from(i64::MIN))
            .expect("i64 timestamp offset fits u64"),
    );
    let ordinal = (physical_offset << 32) | u128::from(timestamp.logical());
    let advanced = ordinal
        .checked_add(u128::from(steps))
        .filter(|value| *value < (1_u128 << 96))
        .ok_or(TimestampOracleError::TimestampExhausted)?;
    let physical_offset =
        u64::try_from(advanced >> 32).map_err(|_| TimestampOracleError::TimestampExhausted)?;
    let physical = i128::from(i64::MIN) + i128::from(physical_offset);
    let physical = i64::try_from(physical).map_err(|_| TimestampOracleError::TimestampExhausted)?;
    let logical = u32::try_from(advanced & u128::from(u32::MAX))
        .expect("timestamp logical component is masked to u32");
    Ok(TransactionTime::new(physical, logical))
}

fn encode_state(timestamp: TransactionTime) -> [u8; STATE_BYTES] {
    let mut bytes = [0_u8; STATE_BYTES];
    bytes[..4].copy_from_slice(&STATE_MAGIC);
    bytes[4..6].copy_from_slice(&STATE_VERSION.to_be_bytes());
    bytes[6..14].copy_from_slice(&timestamp.physical_micros().to_be_bytes());
    bytes[14..18].copy_from_slice(&timestamp.logical().to_be_bytes());
    let checksum = crc32fast::hash(&bytes[..18]);
    bytes[18..].copy_from_slice(&checksum.to_be_bytes());
    bytes
}

fn decode_state(bytes: &[u8]) -> Result<TransactionTime, TimestampOracleError> {
    if bytes.len() != STATE_BYTES || bytes[..4] != STATE_MAGIC {
        return Err(TimestampOracleError::CorruptState);
    }
    let version = u16::from_be_bytes(
        bytes[4..6]
            .try_into()
            .expect("fixed timestamp state version slice"),
    );
    if version != STATE_VERSION {
        return Err(TimestampOracleError::UnsupportedStateVersion { version });
    }
    let expected = u32::from_be_bytes(
        bytes[18..22]
            .try_into()
            .expect("fixed timestamp state checksum slice"),
    );
    if crc32fast::hash(&bytes[..18]) != expected {
        return Err(TimestampOracleError::CorruptState);
    }
    Ok(TransactionTime::new(
        i64::from_be_bytes(
            bytes[6..14]
                .try_into()
                .expect("fixed timestamp physical slice"),
        ),
        u32::from_be_bytes(
            bytes[14..18]
                .try_into()
                .expect("fixed timestamp logical slice"),
        ),
    ))
}

fn nonempty_parent(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

fn temporary_path(path: &Path) -> PathBuf {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("timestamp-oracle");
    path.with_file_name(format!(".{file_name}.dtg-tso.tmp"))
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TimestampOracleError {
    Io(String),
    CorruptState,
    UnsupportedStateVersion {
        version: u16,
    },
    LockPoisoned,
    InvalidReservationSize {
        max: u32,
        actual: u32,
    },
    ReservationRegressed {
        current: TransactionTime,
        proposed: TransactionTime,
    },
    ClockOutOfRange,
    TimestampExhausted,
}

impl Display for TimestampOracleError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(message) => write!(formatter, "timestamp store I/O error: {message}"),
            Self::CorruptState => formatter.write_str("timestamp store state is corrupt"),
            Self::UnsupportedStateVersion { version } => {
                write!(formatter, "unsupported timestamp store version {version}")
            }
            Self::LockPoisoned => formatter.write_str("timestamp Oracle lock is poisoned"),
            Self::InvalidReservationSize { max, actual } => write!(
                formatter,
                "timestamp reservation size {actual} is outside 1..={max}"
            ),
            Self::ReservationRegressed { current, proposed } => write!(
                formatter,
                "timestamp reservation regressed from {current:?} to {proposed:?}"
            ),
            Self::ClockOutOfRange => formatter.write_str("physical clock is outside i64 micros"),
            Self::TimestampExhausted => {
                formatter.write_str("transaction timestamp space exhausted")
            }
        }
    }
}

impl Error for TimestampOracleError {}
