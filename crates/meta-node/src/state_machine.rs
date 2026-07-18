use std::collections::VecDeque;
use std::error::Error;
use std::fmt::{self, Display, Formatter};

use control_plane::{CatalogCommand, CatalogError, CatalogState};

const SNAPSHOT_MAGIC: [u8; 4] = *b"DTMS";
const SNAPSHOT_VERSION: u16 = 1;
const SNAPSHOT_HEADER_BYTES: usize = 18;
const CHECKSUM_BYTES: usize = 4;
const MAX_META_SNAPSHOT_BYTES: usize = 64 * 1024 * 1024;
const MAX_WATCH_EVENTS: usize = 65_536;

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
        let mut encoded =
            Vec::with_capacity(SNAPSHOT_HEADER_BYTES + catalog.len() + CHECKSUM_BYTES);
        encoded.extend_from_slice(&SNAPSHOT_MAGIC);
        encoded.extend_from_slice(&SNAPSHOT_VERSION.to_be_bytes());
        encoded.extend_from_slice(&self.applied_index.to_be_bytes());
        encoded.extend_from_slice(&catalog_length.to_be_bytes());
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
        let catalog_length =
            u32::from_be_bytes(encoded[14..18].try_into().expect("fixed catalog length")) as usize;
        if SNAPSHOT_HEADER_BYTES + catalog_length != checksum_offset {
            return Err(MetaStateError::InvalidSnapshotLength);
        }
        state.applied_index =
            u64::from_be_bytes(encoded[6..14].try_into().expect("fixed applied index"));
        state.catalog =
            CatalogState::decode_snapshot(&encoded[SNAPSHOT_HEADER_BYTES..checksum_offset])?;
        state.compacted_through = state.catalog.revision();
        Ok(state)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MetaStateError {
    Catalog(CatalogError),
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
}

impl Display for MetaStateError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Catalog(error) => write!(formatter, "Catalog state error: {error}"),
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
        }
    }
}

impl Error for MetaStateError {}

impl From<CatalogError> for MetaStateError {
    fn from(error: CatalogError) -> Self {
        Self::Catalog(error)
    }
}
