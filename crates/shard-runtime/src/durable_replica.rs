use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::path::Path;

use adapter_rocksdb::RocksAdapter;
use raft::eraftpb::{Entry, EntryType, Message};
use raft::{Config, RawNode, StateRole, Storage};
use raft_command::CommandEnvelopeV1;
use raft_logstore::{RaftLogStoreError, RocksRaftStorage};
use slog::{Logger, o};

use crate::{ReplicaMetadata, ShardRuntimeError, ShardStateMachine};

pub struct DurableRaftReplica {
    node_id: u64,
    storage: RocksRaftStorage,
    raw_node: RawNode<RocksRaftStorage>,
    state_machine: ShardStateMachine<RocksAdapter>,
    crash_before_apply_once: bool,
}

impl DurableRaftReplica {
    pub async fn open(
        node_id: u64,
        voters: &[u64],
        shard_id: u32,
        placement_epoch: u64,
        raft_wal_path: impl AsRef<Path>,
        adapter_path: impl AsRef<Path>,
    ) -> Result<Self, DurableReplicaError> {
        let storage = RocksRaftStorage::open(raft_wal_path, voters)?;
        let state_machine =
            ShardStateMachine::open(RocksAdapter::open(adapter_path)?, shard_id, placement_epoch)
                .await?;
        let applied = state_machine.metadata().applied_index;
        let raft_state = storage.initial_state()?;
        if raft_state.hard_state.commit < applied {
            return Err(DurableReplicaError::CommitBehindApply {
                commit: raft_state.hard_state.commit,
                applied,
            });
        }
        let first_index = storage.first_index()?;
        if applied.saturating_add(1) < first_index {
            return Err(DurableReplicaError::SnapshotInstallRequired {
                applied,
                first_index,
            });
        }
        let config = raft_config(node_id, applied)?;
        let raw_node = RawNode::new(&config, storage.clone(), &discard_logger())
            .map_err(|error| DurableReplicaError::Raft(error.to_string()))?;
        Ok(Self {
            node_id,
            storage,
            raw_node,
            state_machine,
            crash_before_apply_once: false,
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
    pub fn has_ready(&self) -> bool {
        self.raw_node.has_ready()
    }

    #[must_use]
    pub const fn metadata(&self) -> ReplicaMetadata {
        self.state_machine.metadata()
    }

    #[must_use]
    pub const fn adapter(&self) -> &RocksAdapter {
        self.state_machine.adapter()
    }

    #[must_use]
    pub const fn state_machine(&self) -> &ShardStateMachine<RocksAdapter> {
        &self.state_machine
    }

    #[must_use]
    pub const fn raft_storage(&self) -> &RocksRaftStorage {
        &self.storage
    }

    pub fn campaign(&mut self) -> Result<(), DurableReplicaError> {
        self.raw_node
            .campaign()
            .map_err(|error| DurableReplicaError::Raft(error.to_string()))
    }

    pub fn propose(
        &mut self,
        request_id: u128,
        command: Vec<u8>,
    ) -> Result<(), DurableReplicaError> {
        self.raw_node
            .propose(request_id.to_be_bytes().to_vec(), command)
            .map_err(|error| DurableReplicaError::Raft(error.to_string()))
    }

    pub fn step(&mut self, message: Message) -> Result<(), DurableReplicaError> {
        self.raw_node
            .step(message)
            .map_err(|error| DurableReplicaError::Raft(error.to_string()))
    }

    pub fn tick(&mut self) {
        self.raw_node.tick();
    }

    /// Injects a one-shot crash boundary after committed Raft state is durable
    /// but before the corresponding state-machine entries are applied.
    ///
    /// This models the recovery case that the Adapter applied index is designed
    /// to handle. The Replica must be dropped and reopened after the error.
    pub fn inject_crash_after_wal_before_apply_once(&mut self) {
        self.crash_before_apply_once = true;
    }

    pub async fn process_ready(&mut self) -> Result<Vec<Message>, DurableReplicaError> {
        if !self.raw_node.has_ready() {
            return Ok(Vec::new());
        }
        let mut ready = self.raw_node.ready();
        if !ready.snapshot().is_empty() {
            return Err(DurableReplicaError::SnapshotInstallNotConnected);
        }
        let mut messages = ready.take_messages();
        self.storage
            .persist_ready(None, ready.entries(), ready.hs())?;
        messages.extend(ready.take_persisted_messages());
        let committed_entries = ready.take_committed_entries();
        self.fail_before_apply_if_requested(&committed_entries)?;
        apply_entries(&mut self.state_machine, committed_entries).await?;

        let mut light_ready = self.raw_node.advance(ready);
        if let Some(commit_index) = light_ready.commit_index() {
            self.storage.persist_light_commit(commit_index)?;
        }
        messages.extend(light_ready.take_messages());
        let committed_entries = light_ready.take_committed_entries();
        self.fail_before_apply_if_requested(&committed_entries)?;
        apply_entries(&mut self.state_machine, committed_entries).await?;
        self.raw_node.advance_apply();
        Ok(messages)
    }

    fn fail_before_apply_if_requested(
        &mut self,
        committed_entries: &[Entry],
    ) -> Result<(), DurableReplicaError> {
        if self.crash_before_apply_once && !committed_entries.is_empty() {
            self.crash_before_apply_once = false;
            return Err(DurableReplicaError::InjectedCrashAfterWalBeforeApply);
        }
        Ok(())
    }
}

async fn apply_entries(
    state_machine: &mut ShardStateMachine<RocksAdapter>,
    entries: Vec<Entry>,
) -> Result<(), DurableReplicaError> {
    for entry in entries {
        match entry.get_entry_type() {
            EntryType::EntryNormal if entry.data.is_empty() => {
                state_machine
                    .apply_noop_entry(entry.term, entry.index)
                    .await?;
            }
            EntryType::EntryNormal => {
                CommandEnvelopeV1::decode(&entry.data)?;
                state_machine
                    .apply_entry(entry.term, entry.index, &entry.data)
                    .await?;
            }
            EntryType::EntryConfChange | EntryType::EntryConfChangeV2 => {
                return Err(DurableReplicaError::UnsupportedEntryType);
            }
        }
    }
    Ok(())
}

fn raft_config(node_id: u64, applied: u64) -> Result<Config, DurableReplicaError> {
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
        .map_err(|error| DurableReplicaError::Raft(error.to_string()))?;
    Ok(config)
}

fn discard_logger() -> Logger {
    Logger::root(slog::Discard, o!())
}

#[derive(Debug)]
pub enum DurableReplicaError {
    Adapter(storage_api::AdapterError),
    StateMachine(ShardRuntimeError),
    Command(raft_command::CommandCodecError),
    LogStore(RaftLogStoreError),
    Raft(String),
    CommitBehindApply { commit: u64, applied: u64 },
    SnapshotInstallRequired { applied: u64, first_index: u64 },
    SnapshotInstallNotConnected,
    InjectedCrashAfterWalBeforeApply,
    UnsupportedEntryType,
}

impl Display for DurableReplicaError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Adapter(error) => write!(formatter, "Adapter error: {error}"),
            Self::StateMachine(error) => write!(formatter, "state-machine error: {error}"),
            Self::Command(error) => write!(formatter, "command error: {error}"),
            Self::LogStore(error) => write!(formatter, "Raft WAL error: {error}"),
            Self::Raft(message) => write!(formatter, "Raft error: {message}"),
            Self::CommitBehindApply { commit, applied } => write!(
                formatter,
                "Raft commit index {commit} is behind Adapter applied index {applied}"
            ),
            Self::SnapshotInstallRequired {
                applied,
                first_index,
            } => write!(
                formatter,
                "Adapter index {applied} requires a snapshot before WAL first index {first_index}"
            ),
            Self::SnapshotInstallNotConnected => {
                formatter.write_str("Raft snapshot install is not connected to Adapter restore")
            }
            Self::InjectedCrashAfterWalBeforeApply => formatter.write_str(
                "injected crash after committed Raft state was persisted and before Adapter apply",
            ),
            Self::UnsupportedEntryType => {
                formatter.write_str("dynamic membership is not supported by this Replica")
            }
        }
    }
}

impl Error for DurableReplicaError {}

impl From<storage_api::AdapterError> for DurableReplicaError {
    fn from(error: storage_api::AdapterError) -> Self {
        Self::Adapter(error)
    }
}

impl From<ShardRuntimeError> for DurableReplicaError {
    fn from(error: ShardRuntimeError) -> Self {
        Self::StateMachine(error)
    }
}

impl From<raft_command::CommandCodecError> for DurableReplicaError {
    fn from(error: raft_command::CommandCodecError) -> Self {
        Self::Command(error)
    }
}

impl From<RaftLogStoreError> for DurableReplicaError {
    fn from(error: RaftLogStoreError) -> Self {
        Self::LogStore(error)
    }
}

impl From<raft::Error> for DurableReplicaError {
    fn from(error: raft::Error) -> Self {
        Self::Raft(error.to_string())
    }
}
