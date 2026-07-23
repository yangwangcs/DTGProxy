use std::collections::{BTreeMap, VecDeque};
use std::error::Error;
use std::fmt::{self, Display, Formatter};

use analytics_ledger::{JobCommand, JobError, LedgerState};
use control_plane::{CatalogCommand, CatalogError, CatalogState};
use temporal_types::TransactionTime;

use crate::{ReserveTimestampCommand, TsoError};

const SNAPSHOT_MAGIC: [u8; 4] = *b"DTMS";
const SNAPSHOT_VERSION: u16 = 4;
const SNAPSHOT_HEADER_BYTES: usize = 94;
const RESERVATION_RECORD_BYTES: usize = 52;
const CHECKSUM_BYTES: usize = 4;
const MAX_META_SNAPSHOT_BYTES: usize = 128 * 1024 * 1024;
const MAX_WATCH_EVENTS: usize = 65_536;
const MAX_RESERVATION_HISTORY: usize = 65_536;
const MIN_TIMESTAMP: TransactionTime = TransactionTime::new(i64::MIN, 0);
const GC_LEASE_MAGIC: [u8; 4] = *b"DTGC";
const GC_LEASE_VERSION: u16 = 1;
const GC_LEASE_COMMAND_BYTES: usize = 74;

#[derive(Clone, Debug, Eq, PartialEq)]
struct ReservationRecord {
    expected: TransactionTime,
    first: TransactionTime,
    reserved_through: TransactionTime,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AnalyticsGcLeaseRecord {
    gateway_id: u64,
    owner_term: u64,
    gc_epoch: u64,
    expires_unix_ms: u64,
}

impl AnalyticsGcLeaseRecord {
    #[must_use]
    pub const fn gateway_id(self) -> u64 {
        self.gateway_id
    }

    #[must_use]
    pub const fn owner_term(self) -> u64 {
        self.owner_term
    }

    #[must_use]
    pub const fn gc_epoch(self) -> u64 {
        self.gc_epoch
    }

    #[must_use]
    pub const fn expires_unix_ms(self) -> u64 {
        self.expires_unix_ms
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AnalyticsGcLeaseCommand {
    command_id: u128,
    expected_gc_epoch: u64,
    gateway_id: u64,
    owner_term: u64,
    gc_epoch: u64,
    observed_now_unix_ms: u64,
    expires_unix_ms: u64,
}

impl AnalyticsGcLeaseCommand {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        command_id: u128,
        expected_gc_epoch: u64,
        gateway_id: u64,
        owner_term: u64,
        gc_epoch: u64,
        observed_now_unix_ms: u64,
        expires_unix_ms: u64,
    ) -> Result<Self, MetaStateError> {
        if command_id == 0
            || gateway_id == 0
            || owner_term == 0
            || gc_epoch == 0
            || expires_unix_ms <= observed_now_unix_ms
        {
            return Err(MetaStateError::InvalidGcLeaseCommand);
        }
        Ok(Self {
            command_id,
            expected_gc_epoch,
            gateway_id,
            owner_term,
            gc_epoch,
            observed_now_unix_ms,
            expires_unix_ms,
        })
    }

    #[must_use]
    pub const fn command_id(self) -> u128 {
        self.command_id
    }

    #[must_use]
    pub const fn expected_gc_epoch(self) -> u64 {
        self.expected_gc_epoch
    }

    #[must_use]
    pub const fn gateway_id(self) -> u64 {
        self.gateway_id
    }

    #[must_use]
    pub const fn owner_term(self) -> u64 {
        self.owner_term
    }

    #[must_use]
    pub const fn gc_epoch(self) -> u64 {
        self.gc_epoch
    }

    #[must_use]
    pub const fn observed_now_unix_ms(self) -> u64 {
        self.observed_now_unix_ms
    }

    #[must_use]
    pub const fn expires_unix_ms(self) -> u64 {
        self.expires_unix_ms
    }

    pub fn encode(self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(GC_LEASE_COMMAND_BYTES);
        bytes.extend_from_slice(&GC_LEASE_MAGIC);
        bytes.extend_from_slice(&GC_LEASE_VERSION.to_be_bytes());
        bytes.extend_from_slice(&self.command_id.to_be_bytes());
        bytes.extend_from_slice(&self.expected_gc_epoch.to_be_bytes());
        bytes.extend_from_slice(&self.gateway_id.to_be_bytes());
        bytes.extend_from_slice(&self.owner_term.to_be_bytes());
        bytes.extend_from_slice(&self.gc_epoch.to_be_bytes());
        bytes.extend_from_slice(&self.observed_now_unix_ms.to_be_bytes());
        bytes.extend_from_slice(&self.expires_unix_ms.to_be_bytes());
        bytes.extend_from_slice(&crc32fast::hash(&bytes).to_be_bytes());
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, MetaStateError> {
        if bytes.len() != GC_LEASE_COMMAND_BYTES
            || bytes[..4] != GC_LEASE_MAGIC
            || u16::from_be_bytes(bytes[4..6].try_into().expect("fixed GC lease version"))
                != GC_LEASE_VERSION
        {
            return Err(MetaStateError::InvalidGcLeaseCommand);
        }
        let checksum = u32::from_be_bytes(bytes[70..74].try_into().expect("fixed GC checksum"));
        if crc32fast::hash(&bytes[..70]) != checksum {
            return Err(MetaStateError::GcLeaseChecksumMismatch);
        }
        Self::new(
            u128::from_be_bytes(bytes[6..22].try_into().expect("fixed GC command ID")),
            u64::from_be_bytes(bytes[22..30].try_into().expect("fixed GC expected epoch")),
            u64::from_be_bytes(bytes[30..38].try_into().expect("fixed GC gateway ID")),
            u64::from_be_bytes(bytes[38..46].try_into().expect("fixed GC owner term")),
            u64::from_be_bytes(bytes[46..54].try_into().expect("fixed GC epoch")),
            u64::from_be_bytes(bytes[54..62].try_into().expect("fixed GC observed time")),
            u64::from_be_bytes(bytes[62..70].try_into().expect("fixed GC expiry")),
        )
    }

    #[must_use]
    pub(crate) fn has_magic(bytes: &[u8]) -> bool {
        bytes.starts_with(&GC_LEASE_MAGIC)
    }
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
    analytics_revision: u64,
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
    pub const fn analytics_revision(self) -> u64 {
        self.analytics_revision
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
    analytics: LedgerState,
    events: VecDeque<CatalogEvent>,
    event_capacity: usize,
    compacted_through: u64,
    timestamp_high_water: TransactionTime,
    reservations: BTreeMap<u128, ReservationRecord>,
    analytics_gc_lease: Option<AnalyticsGcLeaseRecord>,
    analytics_gc_epoch: u64,
    analytics_gc_command_id: u128,
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
            analytics: LedgerState::new(),
            events: VecDeque::with_capacity(event_capacity),
            event_capacity,
            compacted_through: 0,
            timestamp_high_water: MIN_TIMESTAMP,
            reservations: BTreeMap::new(),
            analytics_gc_lease: None,
            analytics_gc_epoch: 0,
            analytics_gc_command_id: 0,
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
    pub const fn analytics(&self) -> &LedgerState {
        &self.analytics
    }

    #[must_use]
    pub const fn compacted_through(&self) -> u64 {
        self.compacted_through
    }

    #[must_use]
    pub const fn timestamp_high_water(&self) -> TransactionTime {
        self.timestamp_high_water
    }

    #[must_use]
    pub const fn analytics_gc_lease(&self) -> Option<AnalyticsGcLeaseRecord> {
        self.analytics_gc_lease
    }

    pub fn validate_analytics_gc_lease(
        &self,
        command: AnalyticsGcLeaseCommand,
    ) -> Result<bool, MetaStateError> {
        let mut next = self.clone();
        next.apply_analytics_gc_lease(command)
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
                analytics_revision: self.analytics.revision(),
                duplicate,
            });
        }
        if AnalyticsGcLeaseCommand::has_magic(command) {
            let command = AnalyticsGcLeaseCommand::decode(command)?;
            let duplicate = self.apply_analytics_gc_lease(command)?;
            self.applied_index = index;
            return Ok(MetaApplyReceipt {
                applied_index: index,
                catalog_revision: self.catalog.revision(),
                analytics_revision: self.analytics.revision(),
                duplicate,
            });
        }
        if JobCommand::has_magic(command) {
            let command = JobCommand::decode(command)?;
            let mut next = self.analytics.clone();
            let receipt = next.apply(command)?;
            self.analytics = next;
            self.applied_index = index;
            return Ok(MetaApplyReceipt {
                applied_index: index,
                catalog_revision: self.catalog.revision(),
                analytics_revision: receipt.ledger_revision(),
                duplicate: receipt.duplicate(),
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
            analytics_revision: self.analytics.revision(),
            duplicate: receipt.duplicate(),
        })
    }

    fn apply_analytics_gc_lease(
        &mut self,
        command: AnalyticsGcLeaseCommand,
    ) -> Result<bool, MetaStateError> {
        if command.command_id() == self.analytics_gc_command_id {
            let current = self
                .analytics_gc_lease
                .ok_or(MetaStateError::GcLeaseReplayMismatch)?;
            if current.gateway_id() == command.gateway_id()
                && current.owner_term() == command.owner_term()
                && current.gc_epoch() == command.gc_epoch()
                && current.expires_unix_ms() == command.expires_unix_ms()
            {
                return Ok(true);
            }
            return Err(MetaStateError::GcLeaseReplayMismatch);
        }
        if command.expected_gc_epoch() != self.analytics_gc_epoch
            || command.gc_epoch() < self.analytics_gc_epoch
        {
            return Err(MetaStateError::GcLeaseStaleEpoch);
        }
        if let Some(current) = self.analytics_gc_lease {
            if command.gc_epoch() == current.gc_epoch() {
                if current.gateway_id() != command.gateway_id()
                    || current.owner_term() != command.owner_term()
                    || command.expires_unix_ms() <= current.expires_unix_ms()
                {
                    return Err(MetaStateError::GcLeaseConflict);
                }
            } else if command.owner_term() < current.owner_term()
                || (command.owner_term() == current.owner_term()
                    && command.observed_now_unix_ms() < current.expires_unix_ms())
            {
                return Err(MetaStateError::GcLeaseConflict);
            }
        }
        self.analytics_gc_epoch = command.gc_epoch();
        self.analytics_gc_command_id = command.command_id();
        self.analytics_gc_lease = Some(AnalyticsGcLeaseRecord {
            gateway_id: command.gateway_id(),
            owner_term: command.owner_term(),
            gc_epoch: command.gc_epoch(),
            expires_unix_ms: command.expires_unix_ms(),
        });
        Ok(false)
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
        let analytics = self.analytics.encode_snapshot()?;
        let catalog_length =
            u32::try_from(catalog.len()).map_err(|_| MetaStateError::SnapshotTooLarge)?;
        let analytics_length =
            u32::try_from(analytics.len()).map_err(|_| MetaStateError::SnapshotTooLarge)?;
        let reservation_bytes = self
            .reservations
            .len()
            .checked_mul(RESERVATION_RECORD_BYTES)
            .ok_or(MetaStateError::SnapshotTooLarge)?;
        let mut encoded = Vec::with_capacity(
            SNAPSHOT_HEADER_BYTES
                + reservation_bytes
                + catalog.len()
                + analytics.len()
                + CHECKSUM_BYTES,
        );
        encoded.extend_from_slice(&SNAPSHOT_MAGIC);
        encoded.extend_from_slice(&SNAPSHOT_VERSION.to_be_bytes());
        encoded.extend_from_slice(&self.applied_index.to_be_bytes());
        encode_timestamp(&mut encoded, self.timestamp_high_water);
        encoded.extend_from_slice(&(self.reservations.len() as u32).to_be_bytes());
        encoded.extend_from_slice(&catalog_length.to_be_bytes());
        encoded.extend_from_slice(&analytics_length.to_be_bytes());
        encoded.extend_from_slice(&self.analytics_gc_epoch.to_be_bytes());
        if let Some(lease) = self.analytics_gc_lease {
            encoded.extend_from_slice(&lease.gateway_id().to_be_bytes());
            encoded.extend_from_slice(&lease.owner_term().to_be_bytes());
            encoded.extend_from_slice(&lease.gc_epoch().to_be_bytes());
            encoded.extend_from_slice(&lease.expires_unix_ms().to_be_bytes());
            encoded.extend_from_slice(&self.analytics_gc_command_id.to_be_bytes());
        } else {
            encoded.extend_from_slice(&[0; 48]);
        }
        for (command_id, reservation) in &self.reservations {
            encoded.extend_from_slice(&command_id.to_be_bytes());
            encode_timestamp(&mut encoded, reservation.expected);
            encode_timestamp(&mut encoded, reservation.first);
            encode_timestamp(&mut encoded, reservation.reserved_through);
        }
        encoded.extend_from_slice(&catalog);
        encoded.extend_from_slice(&analytics);
        let checksum = crc32fast::hash(&encoded);
        encoded.extend_from_slice(&checksum.to_be_bytes());
        if encoded.len() > MAX_META_SNAPSHOT_BYTES {
            return Err(MetaStateError::SnapshotTooLarge);
        }
        Ok(encoded)
    }

    pub fn decode_snapshot(encoded: &[u8], event_capacity: usize) -> Result<Self, MetaStateError> {
        let mut state = Self::new(event_capacity)?;
        if encoded.len() < SNAPSHOT_HEADER_BYTES + CHECKSUM_BYTES
            || encoded.len() > MAX_META_SNAPSHOT_BYTES
        {
            return Err(MetaStateError::InvalidSnapshotLength);
        }
        if encoded[..4] != SNAPSHOT_MAGIC {
            return Err(MetaStateError::InvalidSnapshotMagic);
        }
        let version = u16::from_be_bytes(encoded[4..6].try_into().expect("fixed version"));
        if version != SNAPSHOT_VERSION {
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
        state.timestamp_high_water = decode_timestamp(&encoded[14..26]);
        let reservation_count =
            u32::from_be_bytes(encoded[26..30].try_into().expect("fixed reservation count"))
                as usize;
        if reservation_count > MAX_RESERVATION_HISTORY {
            return Err(MetaStateError::SnapshotTooLarge);
        }
        let catalog_length =
            u32::from_be_bytes(encoded[30..34].try_into().expect("fixed catalog length")) as usize;
        let analytics_length =
            u32::from_be_bytes(encoded[34..38].try_into().expect("fixed analytics length"))
                as usize;
        state.analytics_gc_epoch =
            u64::from_be_bytes(encoded[38..46].try_into().expect("fixed GC epoch"));
        let gateway_id = u64::from_be_bytes(encoded[46..54].try_into().expect("fixed GC gateway"));
        let owner_term = u64::from_be_bytes(encoded[54..62].try_into().expect("fixed GC term"));
        let lease_epoch =
            u64::from_be_bytes(encoded[62..70].try_into().expect("fixed GC lease epoch"));
        let expires = u64::from_be_bytes(encoded[70..78].try_into().expect("fixed GC expiry"));
        state.analytics_gc_command_id =
            u128::from_be_bytes(encoded[78..94].try_into().expect("fixed GC command"));
        if gateway_id == 0 {
            if owner_term != 0
                || lease_epoch != 0
                || expires != 0
                || state.analytics_gc_command_id != 0
                || state.analytics_gc_epoch != 0
            {
                return Err(MetaStateError::CorruptGcLeaseState);
            }
        } else {
            if owner_term == 0
                || lease_epoch == 0
                || expires == 0
                || state.analytics_gc_command_id == 0
                || lease_epoch != state.analytics_gc_epoch
            {
                return Err(MetaStateError::CorruptGcLeaseState);
            }
            state.analytics_gc_lease = Some(AnalyticsGcLeaseRecord {
                gateway_id,
                owner_term,
                gc_epoch: lease_epoch,
                expires_unix_ms: expires,
            });
        }
        let reservation_bytes = reservation_count
            .checked_mul(RESERVATION_RECORD_BYTES)
            .ok_or(MetaStateError::SnapshotTooLarge)?;
        let catalog_offset = SNAPSHOT_HEADER_BYTES
            .checked_add(reservation_bytes)
            .ok_or(MetaStateError::SnapshotTooLarge)?;
        let analytics_offset = catalog_offset
            .checked_add(catalog_length)
            .ok_or(MetaStateError::SnapshotTooLarge)?;
        let payload_end = analytics_offset
            .checked_add(analytics_length)
            .ok_or(MetaStateError::SnapshotTooLarge)?;
        if payload_end != checksum_offset {
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
        state.catalog = CatalogState::decode_snapshot(&encoded[catalog_offset..analytics_offset])?;
        state.analytics = LedgerState::decode_snapshot(&encoded[analytics_offset..payload_end])?;
        state.compacted_through = state.catalog.revision();
        Ok(state)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MetaStateError {
    Catalog(CatalogError),
    Analytics(JobError),
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
    InvalidGcLeaseCommand,
    GcLeaseChecksumMismatch,
    GcLeaseReplayMismatch,
    GcLeaseStaleEpoch,
    GcLeaseConflict,
    CorruptGcLeaseState,
}

impl Display for MetaStateError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Catalog(error) => write!(formatter, "Catalog state error: {error}"),
            Self::Analytics(error) => write!(formatter, "analytics ledger state error: {error}"),
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
            Self::InvalidGcLeaseCommand => {
                formatter.write_str("invalid analytics GC lease command")
            }
            Self::GcLeaseChecksumMismatch => {
                formatter.write_str("analytics GC lease checksum mismatch")
            }
            Self::GcLeaseReplayMismatch => {
                formatter.write_str("analytics GC lease replay mismatch")
            }
            Self::GcLeaseStaleEpoch => formatter.write_str("analytics GC lease epoch is stale"),
            Self::GcLeaseConflict => formatter.write_str("analytics GC lease conflict"),
            Self::CorruptGcLeaseState => {
                formatter.write_str("Meta snapshot has corrupt analytics GC lease state")
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

impl From<JobError> for MetaStateError {
    fn from(error: JobError) -> Self {
        Self::Analytics(error)
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
