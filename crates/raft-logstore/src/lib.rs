#![forbid(unsafe_code)]

use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};

use prost::Message as ProstMessage;
use raft::eraftpb::{ConfState, Entry, HardState, Snapshot};
use raft::storage::MemStorage;
use raft::{GetEntriesContext, RaftState, Storage};
use rocksdb::{DB, Direction, IteratorMode, Options, WriteBatch, WriteOptions};

const HARD_STATE_KEY: &[u8] = b"M/hard-state";
const CONF_STATE_KEY: &[u8] = b"M/conf-state";
const SNAPSHOT_KEY: &[u8] = b"M/snapshot";
const ENTRY_PREFIX: &[u8] = b"E/";
const HARD_STATE_MAGIC: [u8; 4] = *b"DRHS";
const CONF_STATE_MAGIC: [u8; 4] = *b"DRCS";
const SNAPSHOT_MAGIC: [u8; 4] = *b"DRSN";
const ENTRY_MAGIC: [u8; 4] = *b"DREN";
const RECORD_VERSION: u16 = 1;
const RECORD_OVERHEAD: usize = 14;
const MAX_RECORD_BYTES: usize = 32 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PersistFailpoint {
    BeforeWrite,
    AfterWriteBeforeCache,
}

#[derive(Clone)]
pub struct RocksRaftStorage {
    db: Arc<DB>,
    cache: MemStorage,
    latest_snapshot: Arc<RwLock<Snapshot>>,
    persist_guard: Arc<Mutex<()>>,
    fail_once: Arc<Mutex<Option<PersistFailpoint>>>,
}

impl RocksRaftStorage {
    pub fn open(path: impl AsRef<Path>, voters: &[u64]) -> Result<Self, RaftLogStoreError> {
        validate_voters(voters)?;
        let mut options = Options::default();
        options.create_if_missing(true);
        let db = Arc::new(DB::open(&options, path)?);
        if db.get(CONF_STATE_KEY)?.is_none() {
            let conf_state = ConfState {
                voters: voters.to_vec(),
                ..Default::default()
            };
            let mut batch = WriteBatch::default();
            batch.put(
                CONF_STATE_KEY,
                encode_message(CONF_STATE_MAGIC, &conf_state)?,
            );
            write_sync(&db, batch)?;
        }
        let (cache, latest_snapshot) = load_cache(&db)?;
        let stored_voters = cache.initial_state()?.conf_state.voters;
        if stored_voters != voters {
            return Err(RaftLogStoreError::MembershipMismatch {
                expected: stored_voters,
                actual: voters.to_vec(),
            });
        }
        Ok(Self {
            db,
            cache,
            latest_snapshot: Arc::new(RwLock::new(latest_snapshot)),
            persist_guard: Arc::new(Mutex::new(())),
            fail_once: Arc::new(Mutex::new(None)),
        })
    }

    pub fn inject_failure_once(&self, failpoint: PersistFailpoint) {
        if let Ok(mut configured) = self.fail_once.lock() {
            *configured = Some(failpoint);
        }
    }

    pub fn persist_ready(
        &self,
        snapshot: Option<&Snapshot>,
        entries: &[Entry],
        hard_state: Option<&HardState>,
    ) -> Result<(), RaftLogStoreError> {
        let _guard = self
            .persist_guard
            .lock()
            .map_err(|_| RaftLogStoreError::LockPoisoned)?;
        validate_ready(&self.cache, snapshot, entries)?;
        let derived_hard_state = if hard_state.is_none() {
            snapshot
                .and_then(|snapshot| snapshot.metadata.as_ref())
                .map(|metadata| {
                    let mut state = self.cache.initial_state()?.hard_state;
                    state.term = state.term.max(metadata.term);
                    state.commit = metadata.index;
                    Ok::<_, RaftLogStoreError>(state)
                })
                .transpose()?
        } else {
            None
        };
        let hard_state = hard_state.or(derived_hard_state.as_ref());
        if self.take_failpoint(PersistFailpoint::BeforeWrite)? {
            return Err(RaftLogStoreError::InjectedFailure(
                PersistFailpoint::BeforeWrite,
            ));
        }

        let mut batch = WriteBatch::default();
        if let Some(snapshot) = snapshot {
            for key in entry_keys_from(&self.db, 0)? {
                batch.delete(key);
            }
            batch.put(SNAPSHOT_KEY, encode_message(SNAPSHOT_MAGIC, snapshot)?);
            let conf_state = snapshot
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.conf_state.as_ref())
                .ok_or(RaftLogStoreError::SnapshotMissingMetadata)?;
            batch.put(
                CONF_STATE_KEY,
                encode_message(CONF_STATE_MAGIC, conf_state)?,
            );
        } else if let Some(first) = entries.first() {
            for key in entry_keys_from(&self.db, first.index)? {
                batch.delete(key);
            }
        }
        for entry in entries {
            batch.put(entry_key(entry.index), encode_message(ENTRY_MAGIC, entry)?);
        }
        if let Some(hard_state) = hard_state {
            batch.put(
                HARD_STATE_KEY,
                encode_message(HARD_STATE_MAGIC, hard_state)?,
            );
        }
        write_sync(&self.db, batch)?;
        if self.take_failpoint(PersistFailpoint::AfterWriteBeforeCache)? {
            return Err(RaftLogStoreError::InjectedFailure(
                PersistFailpoint::AfterWriteBeforeCache,
            ));
        }

        if let Some(snapshot) = snapshot {
            self.cache.wl().apply_snapshot(snapshot.clone())?;
            *self
                .latest_snapshot
                .write()
                .map_err(|_| RaftLogStoreError::LockPoisoned)? = snapshot.clone();
        }
        self.cache.wl().append(entries)?;
        if let Some(hard_state) = hard_state {
            self.cache.wl().set_hardstate(hard_state.clone());
        }
        Ok(())
    }

    pub fn persist_snapshot(&self, snapshot: &Snapshot) -> Result<(), RaftLogStoreError> {
        self.persist_ready(Some(snapshot), &[], None)
    }

    pub fn persist_light_commit(&self, commit_index: u64) -> Result<(), RaftLogStoreError> {
        let mut hard_state = self.cache.initial_state()?.hard_state;
        hard_state.commit = commit_index;
        self.persist_ready(None, &[], Some(&hard_state))
    }

    pub fn set_conf_state(&self, conf_state: &ConfState) -> Result<(), RaftLogStoreError> {
        validate_voters(&conf_state.voters)?;
        let _guard = self
            .persist_guard
            .lock()
            .map_err(|_| RaftLogStoreError::LockPoisoned)?;
        let mut batch = WriteBatch::default();
        batch.put(
            CONF_STATE_KEY,
            encode_message(CONF_STATE_MAGIC, conf_state)?,
        );
        write_sync(&self.db, batch)?;
        self.cache.wl().set_conf_state(conf_state.clone());
        Ok(())
    }

    pub fn compact(&self, compact_index: u64) -> Result<(), RaftLogStoreError> {
        let _guard = self
            .persist_guard
            .lock()
            .map_err(|_| RaftLogStoreError::LockPoisoned)?;
        let snapshot_index = self
            .latest_snapshot
            .read()
            .map_err(|_| RaftLogStoreError::LockPoisoned)?
            .metadata
            .as_ref()
            .map_or(0, |metadata| metadata.index);
        if snapshot_index.saturating_add(1) < compact_index {
            return Err(RaftLogStoreError::CompactionBeforeSnapshot {
                snapshot_index,
                compact_index,
            });
        }
        let mut batch = WriteBatch::default();
        for key in entry_keys_before(&self.db, compact_index)? {
            batch.delete(key);
        }
        write_sync(&self.db, batch)?;
        self.cache.wl().compact(compact_index)?;
        Ok(())
    }

    fn take_failpoint(&self, target: PersistFailpoint) -> Result<bool, RaftLogStoreError> {
        let mut failpoint = self
            .fail_once
            .lock()
            .map_err(|_| RaftLogStoreError::LockPoisoned)?;
        if *failpoint == Some(target) {
            *failpoint = None;
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

impl Storage for RocksRaftStorage {
    fn initial_state(&self) -> raft::Result<RaftState> {
        self.cache.initial_state()
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        context: GetEntriesContext,
    ) -> raft::Result<Vec<Entry>> {
        self.cache.entries(low, high, max_size, context)
    }

    fn term(&self, index: u64) -> raft::Result<u64> {
        self.cache.term(index)
    }

    fn first_index(&self) -> raft::Result<u64> {
        self.cache.first_index()
    }

    fn last_index(&self) -> raft::Result<u64> {
        self.cache.last_index()
    }

    fn snapshot(&self, request_index: u64, to: u64) -> raft::Result<Snapshot> {
        if let Ok(snapshot) = self.latest_snapshot.read()
            && snapshot
                .metadata
                .as_ref()
                .is_some_and(|metadata| metadata.index >= request_index)
        {
            return Ok(snapshot.clone());
        }
        self.cache.snapshot(request_index, to)
    }
}

fn load_cache(db: &DB) -> Result<(MemStorage, Snapshot), RaftLogStoreError> {
    let conf_state = decode_required::<ConfState>(db, CONF_STATE_KEY, CONF_STATE_MAGIC)?;
    let hard_state =
        decode_optional::<HardState>(db, HARD_STATE_KEY, HARD_STATE_MAGIC)?.unwrap_or_default();
    let snapshot =
        decode_optional::<Snapshot>(db, SNAPSHOT_KEY, SNAPSHOT_MAGIC)?.unwrap_or_default();
    let cache = MemStorage::new();
    if snapshot
        .metadata
        .as_ref()
        .is_some_and(|metadata| metadata.index > 0)
    {
        cache.wl().apply_snapshot(snapshot.clone())?;
    } else {
        cache.initialize_with_conf_state(conf_state.clone());
    }
    let mut entries = Vec::new();
    for item in db.iterator(IteratorMode::From(ENTRY_PREFIX, Direction::Forward)) {
        let (key, value) = item?;
        if !key.starts_with(ENTRY_PREFIX) {
            break;
        }
        entries.push(decode_message(ENTRY_MAGIC, &value)?);
    }
    entries.sort_by_key(|entry: &Entry| entry.index);
    cache.wl().append(&entries)?;
    cache.wl().set_conf_state(conf_state);
    cache.wl().set_hardstate(hard_state);
    Ok((cache, snapshot))
}

fn validate_ready(
    cache: &MemStorage,
    snapshot: Option<&Snapshot>,
    entries: &[Entry],
) -> Result<(), RaftLogStoreError> {
    for pair in entries.windows(2) {
        if pair[0].index.saturating_add(1) != pair[1].index {
            return Err(RaftLogStoreError::NonContiguousEntries);
        }
    }
    if let Some(snapshot) = snapshot {
        let metadata = snapshot
            .metadata
            .as_ref()
            .ok_or(RaftLogStoreError::SnapshotMissingMetadata)?;
        if metadata.conf_state.is_none() {
            return Err(RaftLogStoreError::SnapshotMissingMetadata);
        }
        if let Some(first) = entries.first()
            && first.index != metadata.index.saturating_add(1)
        {
            return Err(RaftLogStoreError::NonContiguousEntries);
        }
    } else if let Some(first) = entries.first() {
        let first_index = cache.first_index()?;
        let last_index = cache.last_index()?;
        if first.index < first_index || first.index > last_index.saturating_add(1) {
            return Err(RaftLogStoreError::NonContiguousEntries);
        }
    }
    Ok(())
}

fn validate_voters(voters: &[u64]) -> Result<(), RaftLogStoreError> {
    if voters.is_empty() || voters[0] == 0 || voters.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(RaftLogStoreError::NonCanonicalVoters);
    }
    Ok(())
}

fn entry_key(index: u64) -> [u8; 10] {
    let mut key = [0_u8; 10];
    key[..2].copy_from_slice(ENTRY_PREFIX);
    key[2..].copy_from_slice(&index.to_be_bytes());
    key
}

fn entry_keys_from(db: &DB, index: u64) -> Result<Vec<Box<[u8]>>, RaftLogStoreError> {
    let mut keys = Vec::new();
    for item in db.iterator(IteratorMode::From(&entry_key(index), Direction::Forward)) {
        let (key, _) = item?;
        if !key.starts_with(ENTRY_PREFIX) {
            break;
        }
        keys.push(key);
    }
    Ok(keys)
}

fn entry_keys_before(db: &DB, index: u64) -> Result<Vec<Box<[u8]>>, RaftLogStoreError> {
    let mut keys = Vec::new();
    for item in db.iterator(IteratorMode::From(ENTRY_PREFIX, Direction::Forward)) {
        let (key, _) = item?;
        if !key.starts_with(ENTRY_PREFIX) || key.as_ref() >= entry_key(index).as_slice() {
            break;
        }
        keys.push(key);
    }
    Ok(keys)
}

fn encode_message<M: ProstMessage>(
    magic: [u8; 4],
    message: &M,
) -> Result<Vec<u8>, RaftLogStoreError> {
    let payload = message.encode_to_vec();
    let total = RECORD_OVERHEAD
        .checked_add(payload.len())
        .ok_or(RaftLogStoreError::RecordTooLarge)?;
    if total > MAX_RECORD_BYTES {
        return Err(RaftLogStoreError::RecordTooLarge);
    }
    let payload_length =
        u32::try_from(payload.len()).map_err(|_| RaftLogStoreError::RecordTooLarge)?;
    let mut bytes = Vec::with_capacity(total);
    bytes.extend_from_slice(&magic);
    bytes.extend_from_slice(&RECORD_VERSION.to_be_bytes());
    bytes.extend_from_slice(&payload_length.to_be_bytes());
    bytes.extend_from_slice(&payload);
    let checksum = crc32fast::hash(&bytes);
    bytes.extend_from_slice(&checksum.to_be_bytes());
    Ok(bytes)
}

fn decode_message<M: ProstMessage + Default>(
    magic: [u8; 4],
    bytes: &[u8],
) -> Result<M, RaftLogStoreError> {
    if bytes.len() < RECORD_OVERHEAD || bytes.len() > MAX_RECORD_BYTES || bytes[..4] != magic {
        return Err(RaftLogStoreError::CorruptRecord);
    }
    if u16::from_be_bytes(bytes[4..6].try_into().expect("fixed record version slice"))
        != RECORD_VERSION
    {
        return Err(RaftLogStoreError::UnsupportedRecordVersion);
    }
    let payload_length = usize::try_from(u32::from_be_bytes(
        bytes[6..10].try_into().expect("fixed record length slice"),
    ))
    .map_err(|_| RaftLogStoreError::RecordTooLarge)?;
    if bytes.len() != RECORD_OVERHEAD + payload_length {
        return Err(RaftLogStoreError::CorruptRecord);
    }
    let checksum_offset = bytes.len() - 4;
    let stored = u32::from_be_bytes(
        bytes[checksum_offset..]
            .try_into()
            .expect("fixed checksum slice"),
    );
    if crc32fast::hash(&bytes[..checksum_offset]) != stored {
        return Err(RaftLogStoreError::CorruptRecord);
    }
    M::decode(&bytes[10..checksum_offset]).map_err(|_| RaftLogStoreError::CorruptRecord)
}

fn decode_required<M: ProstMessage + Default>(
    db: &DB,
    key: &[u8],
    magic: [u8; 4],
) -> Result<M, RaftLogStoreError> {
    let bytes = db.get(key)?.ok_or(RaftLogStoreError::MissingMetadata)?;
    decode_message(magic, &bytes)
}

fn decode_optional<M: ProstMessage + Default>(
    db: &DB,
    key: &[u8],
    magic: [u8; 4],
) -> Result<Option<M>, RaftLogStoreError> {
    db.get(key)?
        .map(|bytes| decode_message(magic, &bytes))
        .transpose()
}

fn write_sync(db: &DB, batch: WriteBatch) -> Result<(), RaftLogStoreError> {
    let mut options = WriteOptions::default();
    options.set_sync(true);
    options.disable_wal(false);
    db.write_opt(batch, &options)?;
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RaftLogStoreError {
    Backend(String),
    Raft(String),
    LockPoisoned,
    MissingMetadata,
    CorruptRecord,
    UnsupportedRecordVersion,
    RecordTooLarge,
    NonCanonicalVoters,
    MembershipMismatch {
        expected: Vec<u64>,
        actual: Vec<u64>,
    },
    SnapshotMissingMetadata,
    NonContiguousEntries,
    CompactionBeforeSnapshot {
        snapshot_index: u64,
        compact_index: u64,
    },
    InjectedFailure(PersistFailpoint),
}

impl Display for RaftLogStoreError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Backend(message) => write!(formatter, "Raft WAL backend error: {message}"),
            Self::Raft(message) => write!(formatter, "Raft storage error: {message}"),
            Self::LockPoisoned => formatter.write_str("Raft WAL lock is poisoned"),
            Self::MissingMetadata => formatter.write_str("Raft WAL metadata is missing"),
            Self::CorruptRecord => formatter.write_str("Raft WAL record is corrupt"),
            Self::UnsupportedRecordVersion => {
                formatter.write_str("Raft WAL record version is unsupported")
            }
            Self::RecordTooLarge => formatter.write_str("Raft WAL record is too large"),
            Self::NonCanonicalVoters => {
                formatter.write_str("Raft voters must be nonzero, sorted, and unique")
            }
            Self::MembershipMismatch { expected, actual } => write!(
                formatter,
                "Raft WAL membership {expected:?} differs from requested {actual:?}"
            ),
            Self::SnapshotMissingMetadata => {
                formatter.write_str("Raft snapshot metadata or membership is missing")
            }
            Self::NonContiguousEntries => {
                formatter.write_str("Raft entries are non-contiguous or overwrite compacted data")
            }
            Self::CompactionBeforeSnapshot {
                snapshot_index,
                compact_index,
            } => write!(
                formatter,
                "cannot compact to {compact_index} before durable snapshot {snapshot_index}"
            ),
            Self::InjectedFailure(failpoint) => {
                write!(formatter, "injected Raft WAL failure at {failpoint:?}")
            }
        }
    }
}

impl Error for RaftLogStoreError {}

impl From<rocksdb::Error> for RaftLogStoreError {
    fn from(error: rocksdb::Error) -> Self {
        Self::Backend(error.to_string())
    }
}

impl From<raft::Error> for RaftLogStoreError {
    fn from(error: raft::Error) -> Self {
        Self::Raft(error.to_string())
    }
}
