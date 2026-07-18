use std::collections::{BTreeMap, VecDeque};
use std::error::Error;
use std::fmt::{self, Display, Formatter};

use control_plane::{CatalogCommand, CatalogError, CatalogState};
use temporal_types::TransactionTime;

use crate::{ReserveTimestampCommand, TsoError};

const SNAPSHOT_MAGIC: [u8; 4] = *b"DTMS";
const SNAPSHOT_VERSION: u16 = 2;
const SNAPSHOT_V1_HEADER_BYTES: usize = 18;
const SNAPSHOT_HEADER_BYTES: usize = 34;
const RESERVATION_RECORD_BYTES: usize = 52;
const CHECKSUM_BYTES: usize = 4;
const MAX_META_SNAPSHOT_BYTES: usize = 64 * 1024 * 1024;
const MAX_WATCH_EVENTS: usize = 65_536;
const MAX_RESERVATION_HISTORY: usize = 65_536;
const MIN_TIMESTAMP: TransactionTime = TransactionTime::new(i64::MIN, 0);

#[derive(Clone, Debug, Eq, PartialEq)]
struct ReservationRecord {
    expected: TransactionTime,
    first: TransactionTime,
    reserved_through: TransactionTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogEvent {
    revision: u64,
    command: Vec<u8>,
    checksum: u32,
}

impl CatalogEvent {
    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    #[must_use]
    pub fn command(&self) -> &[u8] {
        &self.command
    }

    #[must_use]
    pub const fn checksum(&self) -> u32 {
        self.checksum
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WatchBatch {
    current_revision: u64,
    events: Vec<CatalogEvent>,
}

impl WatchBatch {
    #[must_use]
    pub const fn current_revision(&self) -> u64 {
        self.current_revision
    }

    #[must_use]
    pub fn events(&self) -> &[CatalogEvent] {
        &self.events
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MetaApplyReceipt {
    applied_index: u64,
    catalog_revision: u64,
    duplicate: bool,
}

impl MetaApplyReceipt {
    #[must_use]
    pub const fn applied_index(self) -> u64 {
        self.applied_index
    }

    #[must_use]
    pub const fn catalog_revision(self) -> u64 {
        self.catalog_revision
    }

    #[must_use]
    pub const fn duplicate(self) -> bool {
        self.duplicate
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetaStateMachine {
    applied_index: u64,
    catalog: CatalogState,
    events: VecDeque<CatalogEvent>,
    event_capacity: usize,
    compacted_through: u64,
    timestamp_high_water: TransactionTime,
    reservations: BTreeMap<u128, ReservationRecord>,
}

impl MetaStateMachine {
    pub fn new(event_capacity: usize) -> Result<Self, MetaStateError> {
        if event_capacity == 0 || event_capacity > MAX_WATCH_EVENTS {
            return Err(MetaStateError::InvalidEventCapacity {
                actual: event_capacity,
            });
        }
        Ok(Self {
            applied_index: 0,
            catalog: CatalogState::new(),
            events: VecDeque::with_capacity(event_capacity),
            event_capacity,
            compacted_through: 0,
            timestamp_high_water: MIN_TIMESTAMP,
            reservations: BTreeMap::new(),
        })
    }

    #[must_use]
    pub const fn applied_index(&self) -> u64 {
        self.applied_index
    }

    #[must_use]
    pub const fn catalog(&self) -> &CatalogState {
        &self.catalog
    }

    #[must_use]
    pub const fn compacted_through(&self) -> u64 {
        self.compacted_through
    }

    #[must_use]
    pub const fn timestamp_high_water(&self) -> TransactionTime {
        self.timestamp_high_water
    }

    pub fn apply_committed(
        &mut self,
        term: u64,
        index: u64,
        command: &[u8],
    ) -> Result<MetaApplyReceipt, MetaStateError> {
        if term == 0 || index == 0 {
            return Err(MetaStateError::InvalidLogPosition { term, index });
        }
        let expected = self
            .applied_index
            .checked_add(1)
            .ok_or(MetaStateError::AppliedIndexExhausted)?;
        if index != expected {
            return Err(MetaStateError::NonContiguousIndex {
                expected,
                actual: index,
            });
        }
        if ReserveTimestampCommand::has_magic(command) {
            let command = ReserveTimestampCommand::decode(command)?;
            let duplicate = self.apply_timestamp_reservation(&command)?;
            self.applied_index = index;
            return Ok(MetaApplyReceipt {
                applied_index: index,
                catalog_revision: self.catalog.revision(),
                duplicate,
            });
        }
        let decoded = CatalogCommand::decode(command)?;
        let mut next = self.catalog.clone();
        let receipt = next.apply(decoded)?;
        self.catalog = next;
        self.applied_index = index;
        if !receipt.duplicate() {
            if self.events.len() == self.event_capacity
                && let Some(compacted) = self.events.pop_front()
            {
                self.compacted_through = compacted.revision;
            }
            self.events.push_back(CatalogEvent {
                revision: receipt.revision(),
                command: command.to_vec(),
                checksum: crc32fast::hash(command),
            });
        }
        Ok(MetaApplyReceipt {
            applied_index: index,
            catalog_revision: receipt.revision(),
            duplicate: receipt.duplicate(),
        })
    }

    fn apply_timestamp_reservation(
        &mut self,
        command: &ReserveTimestampCommand,
    ) -> Result<bool, MetaStateError> {
        if let Some(existing) = self.reservations.get(&command.command_id()) {
            if existing.expected != command.expected_high_water()
                || existing.first != command.first()
                || existing.reserved_through != command.new_high_water()
            {
                return Err(TsoError::ReservationReplayMismatch {
                    command_id: command.command_id(),
                }
                .into());
            }
            return Ok(true);
        }
        if self.reservations.len() == MAX_RESERVATION_HISTORY {
            return Err(TsoError::ReservationHistoryFull.into());
        }
        if command.expected_high_water() != self.timestamp_high_water {
            return Err(TsoError::StaleHighWater {
                expected: self.timestamp_high_water,
                actual: command.expected_high_water(),
            }
            .into());
        }
        self.timestamp_high_water = command.new_high_water();
        self.reservations.insert(
            command.command_id(),
            ReservationRecord {
                expected: command.expected_high_water(),
                first: command.first(),
                reserved_through: command.new_high_water(),
            },
        );
        Ok(false)
    }

    pub fn apply_noop(&mut self, term: u64, index: u64) -> Result<(), MetaStateError> {
        if term == 0 || index == 0 {
            return Err(MetaStateError::InvalidLogPosition { term, index });
        }
        let expected = self
            .applied_index
            .checked_add(1)
            .ok_or(MetaStateError::AppliedIndexExhausted)?;
        if index != expected {
            return Err(MetaStateError::NonContiguousIndex {
                expected,
                actual: index,
            });
        }
        self.applied_index = index;
        Ok(())
    }

    pub fn watch_after(&self, revision: u64) -> Result<WatchBatch, MetaStateError> {
        if revision < self.compacted_through {
            return Err(MetaStateError::RevisionCompacted {
                compacted_through: self.compacted_through,
            });
        }
        if revision > self.catalog.revision() {
            return Err(MetaStateError::FutureRevision {
                current: self.catalog.revision(),
                requested: revision,
            });
        }
        Ok(WatchBatch {
            current_revision: self.catalog.revision(),
            events: self
                .events
                .iter()
                .filter(|event| event.revision > revision)
                .cloned()
                .collect(),
        })
    }

    pub fn encode_snapshot(&self) -> Result<Vec<u8>, MetaStateError> {
        let catalog = self.catalog.encode_snapshot()?;
        let catalog_length =
            u32::try_from(catalog.len()).map_err(|_| MetaStateError::SnapshotTooLarge)?;
        let reservation_bytes = self
            .reservations
            .len()
            .checked_mul(RESERVATION_RECORD_BYTES)
            .ok_or(MetaStateError::SnapshotTooLarge)?;
        let mut encoded = Vec::with_capacity(
            SNAPSHOT_HEADER_BYTES + reservation_bytes + catalog.len() + CHECKSUM_BYTES,
        );
        encoded.extend_from_slice(&SNAPSHOT_MAGIC);
        encoded.extend_from_slice(&SNAPSHOT_VERSION.to_be_bytes());
        encoded.extend_from_slice(&self.applied_index.to_be_bytes());
        encode_timestamp(&mut encoded, self.timestamp_high_water);
        encoded.extend_from_slice(&(self.reservations.len() as u32).to_be_bytes());
        encoded.extend_from_slice(&catalog_length.to_be_bytes());
        for (command_id, reservation) in &self.reservations {
            encoded.extend_from_slice(&command_id.to_be_bytes());
            encode_timestamp(&mut encoded, reservation.expected);
            encode_timestamp(&mut encoded, reservation.first);
            encode_timestamp(&mut encoded, reservation.reserved_through);
        }
        encoded.extend_from_slice(&catalog);
        let checksum = crc32fast::hash(&encoded);
        encoded.extend_from_slice(&checksum.to_be_bytes());
        if encoded.len() > MAX_META_SNAPSHOT_BYTES {
            return Err(MetaStateError::SnapshotTooLarge);
        }
        Ok(encoded)
    }

    pub fn decode_snapshot(encoded: &[u8], event_capacity: usize) -> Result<Self, MetaStateError> {
        let mut state = Self::new(event_capacity)?;
        if encoded.len() < SNAPSHOT_V1_HEADER_BYTES + CHECKSUM_BYTES
            || encoded.len() > MAX_META_SNAPSHOT_BYTES
        {
            return Err(MetaStateError::InvalidSnapshotLength);
        }
        if encoded[..4] != SNAPSHOT_MAGIC {
            return Err(MetaStateError::InvalidSnapshotMagic);
        }
        let version = u16::from_be_bytes(encoded[4..6].try_into().expect("fixed version"));
        if !matches!(version, 1 | SNAPSHOT_VERSION) {
            return Err(MetaStateError::UnsupportedSnapshotVersion { actual: version });
        }
        let checksum_offset = encoded.len() - CHECKSUM_BYTES;
        let stored = u32::from_be_bytes(
            encoded[checksum_offset..]
                .try_into()
                .expect("fixed checksum"),
        );
        if crc32fast::hash(&encoded[..checksum_offset]) != stored {
            return Err(MetaStateError::SnapshotChecksumMismatch);
        }
        state.applied_index =
            u64::from_be_bytes(encoded[6..14].try_into().expect("fixed applied index"));
        let catalog_offset = if version == 1 {
            let catalog_length =
                u32::from_be_bytes(encoded[14..18].try_into().expect("fixed v1 catalog length"))
                    as usize;
            if SNAPSHOT_V1_HEADER_BYTES + catalog_length != checksum_offset {
                return Err(MetaStateError::InvalidSnapshotLength);
            }
            SNAPSHOT_V1_HEADER_BYTES
        } else {
            if encoded.len() < SNAPSHOT_HEADER_BYTES + CHECKSUM_BYTES {
                return Err(MetaStateError::InvalidSnapshotLength);
            }
            state.timestamp_high_water = decode_timestamp(&encoded[14..26]);
            let reservation_count =
                u32::from_be_bytes(encoded[26..30].try_into().expect("fixed reservation count"))
                    as usize;
            if reservation_count > MAX_RESERVATION_HISTORY {
                return Err(MetaStateError::SnapshotTooLarge);
            }
            let catalog_length =
                u32::from_be_bytes(encoded[30..34].try_into().expect("fixed catalog length"))
                    as usize;
            let reservation_bytes = reservation_count
                .checked_mul(RESERVATION_RECORD_BYTES)
                .ok_or(MetaStateError::SnapshotTooLarge)?;
            let catalog_offset = SNAPSHOT_HEADER_BYTES
                .checked_add(reservation_bytes)
                .ok_or(MetaStateError::SnapshotTooLarge)?;
            if catalog_offset + catalog_length != checksum_offset {
                return Err(MetaStateError::InvalidSnapshotLength);
            }
            let mut offset = SNAPSHOT_HEADER_BYTES;
            for _ in 0..reservation_count {
                let command_id = u128::from_be_bytes(
                    encoded[offset..offset + 16]
                        .try_into()
                        .expect("bounded reservation ID"),
                );
                let expected = decode_timestamp(&encoded[offset + 16..offset + 28]);
                let first = decode_timestamp(&encoded[offset + 28..offset + 40]);
                let reserved_through = decode_timestamp(&encoded[offset + 40..offset + 52]);
                if command_id == 0
                    || first <= expected
                    || reserved_through < first
                    || state
                        .reservations
                        .insert(
                            command_id,
                            ReservationRecord {
                                expected,
                                first,
                                reserved_through,
                            },
                        )
                        .is_some()
                {
                    return Err(MetaStateError::CorruptReservationHistory);
                }
                offset += RESERVATION_RECORD_BYTES;
            }
            catalog_offset
        };
        state.catalog = CatalogState::decode_snapshot(&encoded[catalog_offset..checksum_offset])?;
        state.compacted_through = state.catalog.revision();
        Ok(state)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MetaStateError {
    Catalog(CatalogError),
    Tso(TsoError),
    InvalidEventCapacity { actual: usize },
    InvalidLogPosition { term: u64, index: u64 },
    NonContiguousIndex { expected: u64, actual: u64 },
    AppliedIndexExhausted,
    RevisionCompacted { compacted_through: u64 },
    FutureRevision { current: u64, requested: u64 },
    SnapshotTooLarge,
    InvalidSnapshotLength,
    InvalidSnapshotMagic,
    UnsupportedSnapshotVersion { actual: u16 },
    SnapshotChecksumMismatch,
    CorruptReservationHistory,
}

impl Display for MetaStateError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Catalog(error) => write!(formatter, "Catalog state error: {error}"),
            Self::Tso(error) => write!(formatter, "TSO state error: {error}"),
            Self::InvalidEventCapacity { actual } => {
                write!(formatter, "invalid Meta watch event capacity {actual}")
            }
            Self::InvalidLogPosition { term, index } => {
                write!(
                    formatter,
                    "invalid Meta log position term={term}, index={index}"
                )
            }
            Self::NonContiguousIndex { expected, actual } => {
                write!(
                    formatter,
                    "expected Meta log index {expected}, got {actual}"
                )
            }
            Self::AppliedIndexExhausted => formatter.write_str("Meta applied index is exhausted"),
            Self::RevisionCompacted { compacted_through } => {
                write!(
                    formatter,
                    "Catalog revisions through {compacted_through} are compacted"
                )
            }
            Self::FutureRevision { current, requested } => write!(
                formatter,
                "requested Catalog revision {requested} is ahead of current revision {current}"
            ),
            Self::SnapshotTooLarge => formatter.write_str("Meta snapshot exceeds its size limit"),
            Self::InvalidSnapshotLength => formatter.write_str("invalid Meta snapshot length"),
            Self::InvalidSnapshotMagic => formatter.write_str("invalid Meta snapshot magic"),
            Self::UnsupportedSnapshotVersion { actual } => {
                write!(formatter, "unsupported Meta snapshot version {actual}")
            }
            Self::SnapshotChecksumMismatch => {
                formatter.write_str("Meta snapshot checksum mismatch")
            }
            Self::CorruptReservationHistory => {
                formatter.write_str("Meta snapshot has corrupt TSO reservation history")
            }
        }
    }
}

impl Error for MetaStateError {}

impl From<CatalogError> for MetaStateError {
    fn from(error: CatalogError) -> Self {
        Self::Catalog(error)
    }
}

impl From<TsoError> for MetaStateError {
    fn from(error: TsoError) -> Self {
        Self::Tso(error)
    }
}

fn encode_timestamp(output: &mut Vec<u8>, timestamp: TransactionTime) {
    output.extend_from_slice(&timestamp.physical_micros().to_be_bytes());
    output.extend_from_slice(&timestamp.logical().to_be_bytes());
}

fn decode_timestamp(encoded: &[u8]) -> TransactionTime {
    TransactionTime::new(
        i64::from_be_bytes(encoded[..8].try_into().expect("fixed snapshot physical")),
        u32::from_be_bytes(encoded[8..12].try_into().expect("fixed snapshot logical")),
    )
}
