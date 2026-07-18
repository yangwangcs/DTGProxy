use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use control_plane::CatalogCommand;
use raft::eraftpb::{Entry, EntryType, Message, Snapshot, SnapshotMetadata};
use raft::{Config, RawNode, StateRole, Storage};
use raft_logstore::{RaftLogStoreError, RocksRaftStorage};
use slog::{Logger, o};

use crate::{MetaStateError, MetaStateMachine, ReserveTimestampCommand};

const JOURNAL_MAGIC: [u8; 4] = *b"DTMJ";
const JOURNAL_VERSION: u16 = 1;
const JOURNAL_HEADER_BYTES: usize = 26;
const CHECKSUM_BYTES: usize = 4;
const MAX_COMMAND_BYTES: usize = 16 * 1024 * 1024;
const JOURNAL_FILE: &str = "meta.applied.log";
const SNAPSHOT_FILE: &str = "meta.applied.snapshot";
const TEMP_SNAPSHOT_FILE: &str = ".meta.applied.snapshot.tmp";
const MAX_READY_ROUNDS: usize = 256;
const DEFAULT_EVENT_CAPACITY: usize = 8_192;

pub struct MetaRaftReplica {
    node_id: u64,
    storage: RocksRaftStorage,
    raw_node: RawNode<RocksRaftStorage>,
    state_store: MetaStateStore,
}

impl MetaRaftReplica {
    pub fn open(
        node_id: u64,
        voters: &[u64],
        raft_path: impl AsRef<Path>,
        state_path: impl AsRef<Path>,
    ) -> Result<Self, MetaRaftError> {
        let storage = RocksRaftStorage::open(raft_path, voters)?;
        let state_store = MetaStateStore::open(state_path, DEFAULT_EVENT_CAPACITY)?;
        let applied = state_store.state.applied_index();
        let raft_state = storage.initial_state()?;
        if raft_state.hard_state.commit < applied {
            return Err(MetaRaftError::CommitBehindApply {
                commit: raft_state.hard_state.commit,
                applied,
            });
        }
        let config = Config {
            id: node_id,
            election_tick: 10,
            heartbeat_tick: 2,
            check_quorum: true,
            pre_vote: true,
            applied,
            ..Default::default()
        };
        config
            .validate()
            .map_err(|error| MetaRaftError::Raft(error.to_string()))?;
        let raw_node = RawNode::new(&config, storage.clone(), &discard_logger())
            .map_err(|error| MetaRaftError::Raft(error.to_string()))?;
        Ok(Self {
            node_id,
            storage,
            raw_node,
            state_store,
        })
    }

    #[must_use]
    pub const fn node_id(&self) -> u64 {
        self.node_id
    }

    #[must_use]
    pub fn is_leader(&self) -> bool {
        self.raw_node.raft.state == StateRole::Leader
    }

    #[must_use]
    pub fn leader_id(&self) -> Option<u64> {
        (self.raw_node.raft.leader_id != 0).then_some(self.raw_node.raft.leader_id)
    }

    #[must_use]
    pub fn current_term(&self) -> u64 {
        self.raw_node.raft.term
    }

    #[must_use]
    pub fn commit_index(&self) -> u64 {
        self.raw_node.raft.raft_log.committed
    }

    #[must_use]
    pub const fn state(&self) -> &MetaStateMachine {
        &self.state_store.state
    }

    #[must_use]
    pub fn has_ready(&self) -> bool {
        self.raw_node.has_ready()
    }

    pub fn campaign(&mut self) -> Result<(), MetaRaftError> {
        self.raw_node
            .campaign()
            .map_err(|error| MetaRaftError::Raft(error.to_string()))
    }

    pub fn propose(&mut self, command: Vec<u8>) -> Result<(), MetaRaftError> {
        if !self.is_leader() {
            return Err(MetaRaftError::NotLeader {
                leader_id: self.leader_id(),
            });
        }
        let decoded = CatalogCommand::decode(&command)?;
        let mut validation = self.state_store.state.catalog().clone();
        validation.apply(decoded.clone())?;
        self.raw_node
            .propose(decoded.command_id().to_be_bytes().to_vec(), command)
            .map_err(|error| MetaRaftError::Raft(error.to_string()))
    }

    pub fn propose_timestamp(
        &mut self,
        command: ReserveTimestampCommand,
    ) -> Result<(), MetaRaftError> {
        if !self.is_leader() {
            return Err(MetaRaftError::NotLeader {
                leader_id: self.leader_id(),
            });
        }
        if command.expected_high_water() != self.state().timestamp_high_water() {
            return Err(crate::TsoError::StaleHighWater {
                expected: self.state().timestamp_high_water(),
                actual: command.expected_high_water(),
            }
            .into());
        }
        self.raw_node
            .propose(
                command.command_id().to_be_bytes().to_vec(),
                command.encode(),
            )
            .map_err(|error| MetaRaftError::Raft(error.to_string()))
    }

    pub fn step(&mut self, message: Message) -> Result<(), MetaRaftError> {
        self.raw_node
            .step(message)
            .map_err(|error| MetaRaftError::Raft(error.to_string()))
    }

    pub fn tick(&mut self) {
        self.raw_node.tick();
    }

    pub fn process_ready(&mut self) -> Result<Vec<Message>, MetaRaftError> {
        if !self.raw_node.has_ready() {
            return Ok(Vec::new());
        }
        let mut ready = self.raw_node.ready();
        let snapshot = (!ready.snapshot().is_empty()).then(|| ready.snapshot().clone());
        let mut messages = ready.take_messages();
        self.storage
            .persist_ready(snapshot.as_ref(), ready.entries(), ready.hs())?;
        messages.extend(ready.take_persisted_messages());
        if let Some(snapshot) = snapshot {
            let metadata = snapshot
                .metadata
                .as_ref()
                .ok_or(MetaRaftError::SnapshotMissingMetadata)?;
            let state =
                MetaStateMachine::decode_snapshot(&snapshot.data, self.state_store.event_capacity)?;
            if state.applied_index() != metadata.index {
                return Err(MetaRaftError::SnapshotIndexMismatch {
                    metadata: metadata.index,
                    state: state.applied_index(),
                });
            }
            self.state_store.install_snapshot(state)?;
        }
        self.apply_entries(ready.take_committed_entries())?;
        let mut light_ready = self.raw_node.advance(ready);
        if let Some(commit_index) = light_ready.commit_index() {
            self.storage.persist_light_commit(commit_index)?;
        }
        messages.extend(light_ready.take_messages());
        self.apply_entries(light_ready.take_committed_entries())?;
        self.raw_node.advance_apply();
        Ok(messages)
    }

    pub fn drain_ready(&mut self) -> Result<Vec<Message>, MetaRaftError> {
        let mut messages = Vec::new();
        for _ in 0..MAX_READY_ROUNDS {
            if !self.has_ready() {
                return Ok(messages);
            }
            messages.extend(self.process_ready()?);
        }
        Err(MetaRaftError::ReadyLoopLimit)
    }

    pub fn create_snapshot(&mut self) -> Result<Snapshot, MetaRaftError> {
        let index = self.state().applied_index();
        if index == 0 {
            return Err(MetaRaftError::EmptySnapshot);
        }
        let term = self.storage.term(index)?;
        let conf_state = self.storage.initial_state()?.conf_state;
        let snapshot = Snapshot {
            data: self.state().encode_snapshot()?,
            metadata: Some(SnapshotMetadata {
                conf_state: Some(conf_state),
                index,
                term,
            }),
        };
        self.state_store.checkpoint()?;
        self.storage
            .persist_local_snapshot_preserving_suffix(&snapshot)?;
        Ok(snapshot)
    }

    fn apply_entries(&mut self, entries: Vec<Entry>) -> Result<(), MetaRaftError> {
        for entry in entries {
            match entry.get_entry_type() {
                EntryType::EntryNormal if entry.data.is_empty() => {
                    self.state_store.append_noop(entry.term, entry.index)?;
                }
                EntryType::EntryNormal => {
                    self.state_store
                        .append_command(entry.term, entry.index, &entry.data)?;
                }
                EntryType::EntryConfChange | EntryType::EntryConfChangeV2 => {
                    return Err(MetaRaftError::UnsupportedEntryType);
                }
            }
        }
        Ok(())
    }
}

struct MetaStateStore {
    directory: PathBuf,
    journal: File,
    state: MetaStateMachine,
    event_capacity: usize,
}

impl MetaStateStore {
    fn open(directory: impl AsRef<Path>, event_capacity: usize) -> Result<Self, MetaRaftError> {
        let directory = directory.as_ref().to_path_buf();
        fs::create_dir_all(&directory)?;
        let snapshot_path = directory.join(SNAPSHOT_FILE);
        let mut state = match fs::read(&snapshot_path) {
            Ok(bytes) => MetaStateMachine::decode_snapshot(&bytes, event_capacity)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                MetaStateMachine::new(event_capacity)?
            }
            Err(error) => return Err(error.into()),
        };
        let journal_path = directory.join(JOURNAL_FILE);
        let mut journal = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(journal_path)?;
        replay_journal(&mut journal, &mut state)?;
        journal.seek(SeekFrom::End(0))?;
        Ok(Self {
            directory,
            journal,
            state,
            event_capacity,
        })
    }

    fn append_noop(&mut self, term: u64, index: u64) -> Result<(), MetaRaftError> {
        let mut next = self.state.clone();
        next.apply_noop(term, index)?;
        self.append_record(term, index, &[])?;
        self.state = next;
        Ok(())
    }

    fn append_command(
        &mut self,
        term: u64,
        index: u64,
        command: &[u8],
    ) -> Result<(), MetaRaftError> {
        let mut next = self.state.clone();
        next.apply_committed(term, index, command)?;
        self.append_record(term, index, command)?;
        self.state = next;
        Ok(())
    }

    fn append_record(
        &mut self,
        term: u64,
        index: u64,
        command: &[u8],
    ) -> Result<(), MetaRaftError> {
        let frame = encode_journal_record(term, index, command)?;
        self.journal.write_all(&frame)?;
        self.journal.sync_data()?;
        Ok(())
    }

    fn checkpoint(&mut self) -> Result<(), MetaRaftError> {
        write_snapshot_atomically(&self.directory, &self.state.encode_snapshot()?)?;
        self.journal.set_len(0)?;
        self.journal.seek(SeekFrom::Start(0))?;
        self.journal.sync_all()?;
        File::open(&self.directory)?.sync_all()?;
        Ok(())
    }

    fn install_snapshot(&mut self, state: MetaStateMachine) -> Result<(), MetaRaftError> {
        write_snapshot_atomically(&self.directory, &state.encode_snapshot()?)?;
        self.journal.set_len(0)?;
        self.journal.seek(SeekFrom::Start(0))?;
        self.journal.sync_all()?;
        self.state = state;
        Ok(())
    }
}

fn encode_journal_record(term: u64, index: u64, command: &[u8]) -> Result<Vec<u8>, MetaRaftError> {
    if command.len() > MAX_COMMAND_BYTES {
        return Err(MetaRaftError::CommandTooLarge);
    }
    let length = u32::try_from(command.len()).map_err(|_| MetaRaftError::CommandTooLarge)?;
    let mut encoded = Vec::with_capacity(JOURNAL_HEADER_BYTES + command.len() + CHECKSUM_BYTES);
    encoded.extend_from_slice(&JOURNAL_MAGIC);
    encoded.extend_from_slice(&JOURNAL_VERSION.to_be_bytes());
    encoded.extend_from_slice(&term.to_be_bytes());
    encoded.extend_from_slice(&index.to_be_bytes());
    encoded.extend_from_slice(&length.to_be_bytes());
    encoded.extend_from_slice(command);
    encoded.extend_from_slice(&crc32fast::hash(&encoded).to_be_bytes());
    Ok(encoded)
}

fn replay_journal(journal: &mut File, state: &mut MetaStateMachine) -> Result<(), MetaRaftError> {
    journal.seek(SeekFrom::Start(0))?;
    let mut encoded = Vec::new();
    journal.read_to_end(&mut encoded)?;
    let mut offset = 0;
    while offset < encoded.len() {
        if encoded.len() - offset < JOURNAL_HEADER_BYTES {
            journal.set_len(offset as u64)?;
            break;
        }
        if encoded[offset..offset + 4] != JOURNAL_MAGIC {
            return Err(MetaRaftError::InvalidJournalMagic);
        }
        let version = u16::from_be_bytes(
            encoded[offset + 4..offset + 6]
                .try_into()
                .expect("fixed journal version"),
        );
        if version != JOURNAL_VERSION {
            return Err(MetaRaftError::UnsupportedJournalVersion { actual: version });
        }
        let length = u32::from_be_bytes(
            encoded[offset + 22..offset + 26]
                .try_into()
                .expect("fixed journal length"),
        ) as usize;
        if length > MAX_COMMAND_BYTES {
            return Err(MetaRaftError::CommandTooLarge);
        }
        let record_length = JOURNAL_HEADER_BYTES + length + CHECKSUM_BYTES;
        if encoded.len() - offset < record_length {
            journal.set_len(offset as u64)?;
            break;
        }
        let checksum_offset = offset + record_length - CHECKSUM_BYTES;
        let stored = u32::from_be_bytes(
            encoded[checksum_offset..checksum_offset + CHECKSUM_BYTES]
                .try_into()
                .expect("fixed journal checksum"),
        );
        if crc32fast::hash(&encoded[offset..checksum_offset]) != stored {
            return Err(MetaRaftError::JournalChecksumMismatch);
        }
        let term = u64::from_be_bytes(
            encoded[offset + 6..offset + 14]
                .try_into()
                .expect("fixed journal term"),
        );
        let index = u64::from_be_bytes(
            encoded[offset + 14..offset + 22]
                .try_into()
                .expect("fixed journal index"),
        );
        let command = &encoded[offset + JOURNAL_HEADER_BYTES..checksum_offset];
        if command.is_empty() {
            state.apply_noop(term, index)?;
        } else {
            state.apply_committed(term, index, command)?;
        }
        offset += record_length;
    }
    Ok(())
}

fn write_snapshot_atomically(directory: &Path, bytes: &[u8]) -> Result<(), MetaRaftError> {
    let temporary = directory.join(TEMP_SNAPSHOT_FILE);
    let destination = directory.join(SNAPSHOT_FILE);
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temporary)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    drop(file);
    fs::rename(temporary, destination)?;
    File::open(directory)?.sync_all()?;
    Ok(())
}

fn discard_logger() -> Logger {
    Logger::root(slog::Discard, o!())
}

#[derive(Debug)]
pub enum MetaRaftError {
    Io(String),
    Catalog(control_plane::CatalogError),
    State(MetaStateError),
    Tso(crate::TsoError),
    LogStore(RaftLogStoreError),
    Raft(String),
    NotLeader { leader_id: Option<u64> },
    CommitBehindApply { commit: u64, applied: u64 },
    SnapshotMissingMetadata,
    SnapshotIndexMismatch { metadata: u64, state: u64 },
    EmptySnapshot,
    CommandTooLarge,
    InvalidJournalMagic,
    UnsupportedJournalVersion { actual: u16 },
    JournalChecksumMismatch,
    UnsupportedEntryType,
    ReadyLoopLimit,
}

impl Display for MetaRaftError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(message) => write!(formatter, "Meta I/O error: {message}"),
            Self::Catalog(error) => write!(formatter, "Meta Catalog error: {error}"),
            Self::State(error) => write!(formatter, "Meta state error: {error}"),
            Self::Tso(error) => write!(formatter, "Meta TSO error: {error}"),
            Self::LogStore(error) => write!(formatter, "Meta Raft WAL error: {error}"),
            Self::Raft(message) => write!(formatter, "Meta Raft error: {message}"),
            Self::NotLeader { leader_id } => {
                write!(formatter, "Meta node is not Leader; Leader={leader_id:?}")
            }
            Self::CommitBehindApply { commit, applied } => write!(
                formatter,
                "Meta commit {commit} is behind applied {applied}"
            ),
            Self::SnapshotMissingMetadata => {
                formatter.write_str("Meta snapshot is missing metadata")
            }
            Self::SnapshotIndexMismatch { metadata, state } => write!(
                formatter,
                "Meta snapshot index {metadata} differs from state index {state}"
            ),
            Self::EmptySnapshot => formatter.write_str("cannot create an empty Meta snapshot"),
            Self::CommandTooLarge => formatter.write_str("Meta command exceeds its size limit"),
            Self::InvalidJournalMagic => formatter.write_str("invalid Meta journal magic"),
            Self::UnsupportedJournalVersion { actual } => {
                write!(formatter, "unsupported Meta journal version {actual}")
            }
            Self::JournalChecksumMismatch => formatter.write_str("Meta journal checksum mismatch"),
            Self::UnsupportedEntryType => {
                formatter.write_str("Meta dynamic membership is not connected")
            }
            Self::ReadyLoopLimit => formatter.write_str("Meta Ready loop did not quiesce"),
        }
    }
}

impl Error for MetaRaftError {}

impl From<std::io::Error> for MetaRaftError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error.to_string())
    }
}

impl From<control_plane::CatalogError> for MetaRaftError {
    fn from(error: control_plane::CatalogError) -> Self {
        Self::Catalog(error)
    }
}

impl From<MetaStateError> for MetaRaftError {
    fn from(error: MetaStateError) -> Self {
        Self::State(error)
    }
}

impl From<crate::TsoError> for MetaRaftError {
    fn from(error: crate::TsoError) -> Self {
        Self::Tso(error)
    }
}

impl From<RaftLogStoreError> for MetaRaftError {
    fn from(error: RaftLogStoreError) -> Self {
        Self::LogStore(error)
    }
}

impl From<raft::Error> for MetaRaftError {
    fn from(error: raft::Error) -> Self {
        Self::Raft(error.to_string())
    }
}
