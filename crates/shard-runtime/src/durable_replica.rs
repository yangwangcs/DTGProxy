use std::collections::{BTreeMap, VecDeque};
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::path::Path;
use std::sync::Arc;

use adapter_registry::{
    AdapterOpenRequest, AdapterRegistry, HotSwapAdapter, MigrationError, MigrationStatus,
    RegistryError,
};
use adapter_rocksdb::RocksAdapterFactory;
use prost::Message as ProstMessage;
use raft::eraftpb::{
    ConfChangeSingle, ConfChangeType, ConfChangeV2, ConfState, Entry, EntryType, Message,
};
use raft::{Config, RawNode, StateRole, Storage};
use raft_command::CommandEnvelopeV1;
use raft_logstore::{RaftLogStoreError, RocksRaftStorage};
use slog::{Logger, o};

use crate::{
    BackendLifecycle, CommittedEntryOutcome, ReplicaMetadata, ShardRuntimeError, ShardStateMachine,
};

const MAX_PENDING_READ_INDEX_REQUESTS: usize = 1_024;

#[derive(Clone, Copy)]
struct PendingReadIndex {
    leader_id: u64,
    term: u64,
    active: bool,
}

pub struct DurableRaftReplica {
    node_id: u64,
    storage: RocksRaftStorage,
    raw_node: RawNode<RocksRaftStorage>,
    state_machine: ShardStateMachine<Arc<HotSwapAdapter>>,
    crash_before_apply_once: bool,
    pending_auto_leave: bool,
    pending_leader_transfer: Option<u64>,
    pending_read_indexes: BTreeMap<Vec<u8>, PendingReadIndex>,
    completed_read_states: VecDeque<(Vec<u8>, u64, u64, u64)>,
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
        let adapter_path = adapter_path.as_ref();
        let mut registry = AdapterRegistry::new();
        registry.register(Arc::new(RocksAdapterFactory))?;
        let opened = registry
            .open(
                "rocksdb",
                &AdapterOpenRequest::new(format!("shard-{shard_id}-rocksdb"))
                    .with_parameter("path", adapter_path.to_string_lossy()),
                storage_api::AdapterRequirement::ManagedReplica,
            )
            .await?;
        Self::open_with_adapter_slot(
            node_id,
            voters,
            shard_id,
            placement_epoch,
            raft_wal_path,
            Arc::new(HotSwapAdapter::new(opened)),
        )
        .await
    }

    pub async fn open_with_adapter_slot(
        node_id: u64,
        voters: &[u64],
        shard_id: u32,
        placement_epoch: u64,
        raft_wal_path: impl AsRef<Path>,
        backend_slot: Arc<HotSwapAdapter>,
    ) -> Result<Self, DurableReplicaError> {
        let storage = RocksRaftStorage::open(raft_wal_path, voters)?;
        let backend_generation = backend_slot.generation();
        let state_machine = ShardStateMachine::open_with_backend_generation(
            backend_slot,
            shard_id,
            placement_epoch,
            backend_generation,
        )
        .await?;
        reconcile_backend_slot(state_machine.adapter(), state_machine.metadata())?;
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
            pending_auto_leave: false,
            pending_leader_transfer: None,
            pending_read_indexes: BTreeMap::new(),
            completed_read_states: VecDeque::new(),
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
    pub const fn adapter(&self) -> &Arc<HotSwapAdapter> {
        self.state_machine.adapter()
    }

    #[must_use]
    pub const fn backend_slot(&self) -> &Arc<HotSwapAdapter> {
        self.state_machine.adapter()
    }

    #[must_use]
    pub const fn state_machine(&self) -> &ShardStateMachine<Arc<HotSwapAdapter>> {
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
        let envelope = CommandEnvelopeV1::decode(&command)?;
        if envelope.request_id != request_id {
            return Err(DurableReplicaError::RequestEnvelopeMismatch {
                expected: request_id,
                actual: envelope.request_id,
            });
        }
        self.raw_node
            .propose(request_id.to_be_bytes().to_vec(), command)
            .map_err(|error| DurableReplicaError::Raft(error.to_string()))
    }

    pub fn request_read_index(&mut self, context: Vec<u8>) -> Result<(), DurableReplicaError> {
        if context.is_empty() {
            return Err(DurableReplicaError::InvalidReadIndexContext);
        }
        let leader_id = self.leader_id().ok_or(DurableReplicaError::NotLeader)?;
        if !self.is_leader() || leader_id != self.node_id {
            return Err(DurableReplicaError::NotLeader);
        }
        if !self.raw_node.raft.commit_to_current_term() {
            return Err(DurableReplicaError::ReadIndexLeaderNotReady);
        }
        self.prune_read_indexes_from_prior_leadership();
        if self.pending_read_indexes.contains_key(&context)
            || self
                .completed_read_states
                .iter()
                .any(|(completed, ..)| completed == &context)
        {
            return Err(DurableReplicaError::DuplicateReadIndexContext);
        }
        if self.pending_read_indexes.len() + self.completed_read_states.len()
            >= MAX_PENDING_READ_INDEX_REQUESTS
        {
            return Err(DurableReplicaError::TooManyPendingReadIndexRequests);
        }
        let term = self.current_term();
        self.raw_node.read_index(context.clone());
        self.pending_read_indexes.insert(
            context,
            PendingReadIndex {
                leader_id,
                term,
                active: true,
            },
        );
        Ok(())
    }

    pub fn cancel_read_index(&mut self, context: &[u8]) {
        if let Some(pending) = self.pending_read_indexes.get_mut(context) {
            pending.active = false;
        }
        self.completed_read_states
            .retain(|(completed, ..)| completed.as_slice() != context);
    }

    pub fn take_completed_read_state(&mut self) -> Option<(Vec<u8>, u64, u64, u64)> {
        self.completed_read_states.pop_front()
    }

    pub fn propose_membership(
        &mut self,
        operation_id: u128,
        voters: &[u64],
        learners: &[u64],
    ) -> Result<bool, DurableReplicaError> {
        let current = self.membership()?;
        if current.voters == voters && current.learners == learners {
            return Ok(true);
        }
        if !current.voters_outgoing.is_empty() {
            return Err(DurableReplicaError::JointConfigurationInProgress);
        }
        let changes = membership_changes(&current, voters, learners)?;
        if changes.is_empty() {
            return Ok(true);
        }
        self.raw_node
            .propose_conf_change(
                operation_id.to_be_bytes().to_vec(),
                ConfChangeV2 {
                    transition: 0,
                    changes,
                    context: operation_id.to_be_bytes().to_vec(),
                },
            )
            .map_err(|error| DurableReplicaError::Raft(error.to_string()))?;
        Ok(false)
    }

    pub fn leave_joint_membership(
        &mut self,
        operation_id: u128,
    ) -> Result<(), DurableReplicaError> {
        let current = self.membership()?;
        if current.voters_outgoing.is_empty() {
            return Ok(());
        }
        if !self.is_leader() {
            return Err(DurableReplicaError::NotLeader);
        }
        self.raw_node
            .propose_conf_change(
                operation_id.to_be_bytes().to_vec(),
                ConfChangeV2 {
                    context: operation_id.to_be_bytes().to_vec(),
                    ..Default::default()
                },
            )
            .map_err(|error| DurableReplicaError::Raft(error.to_string()))
    }

    pub fn membership(&self) -> Result<ConfState, DurableReplicaError> {
        Ok(self.storage.initial_state()?.conf_state)
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
        let read_states = ready.take_read_states();
        self.storage
            .persist_ready(None, ready.entries(), ready.hs())?;
        messages.extend(ready.take_persisted_messages());
        let committed_entries = ready.take_committed_entries();
        self.fail_before_apply_if_requested(&committed_entries)?;
        self.apply_entries("ready", committed_entries).await?;

        // The state machine, not RawNode's transient commit cursor, owns the
        // durable apply frontier. `advance()` would advance that cursor before
        // LightReady entries have been persisted through the state machine.
        let mut light_ready = self.raw_node.advance_append(ready);
        if let Some(commit_index) = light_ready.commit_index() {
            self.storage.persist_light_commit(commit_index)?;
        }
        messages.extend(light_ready.take_messages());
        let committed_entries = light_ready.take_committed_entries();
        self.fail_before_apply_if_requested(&committed_entries)?;
        self.apply_entries("light-ready", committed_entries).await?;
        self.raw_node
            .advance_apply_to(self.state_machine.metadata().applied_index);
        for state in read_states {
            let Some(pending) = self.pending_read_indexes.remove(&state.request_ctx) else {
                continue;
            };
            if pending.active {
                self.completed_read_states.push_back((
                    state.request_ctx,
                    state.index,
                    pending.leader_id,
                    pending.term,
                ));
            }
        }
        self.run_deferred_membership_action()?;
        Ok(messages)
    }

    fn prune_read_indexes_from_prior_leadership(&mut self) {
        let leader_id = self.leader_id();
        let term = self.current_term();
        self.pending_read_indexes
            .retain(|_, pending| leader_id == Some(pending.leader_id) && term == pending.term);
    }

    async fn apply_entries(
        &mut self,
        phase: &'static str,
        entries: Vec<Entry>,
    ) -> Result<(), DurableReplicaError> {
        for entry in entries {
            match entry.get_entry_type() {
                EntryType::EntryNormal if entry.data.is_empty() => {
                    let result = self
                        .state_machine
                        .apply_noop_entry(entry.term, entry.index)
                        .await;
                    if let Err(error) = result {
                        return Err(self.state_machine_apply_error(phase, entry.index, error));
                    }
                }
                EntryType::EntryNormal => {
                    let command = CommandEnvelopeV1::decode(&entry.data)?;
                    let result = self
                        .state_machine
                        .apply_committed_entry(entry.term, entry.index, &entry.data)
                        .await;
                    match result {
                        Ok(CommittedEntryOutcome::Applied(_)) => {
                            self.validate_backend_transition(&command.body)?;
                            self.finish_backend_transition(&command.body)?;
                        }
                        Ok(CommittedEntryOutcome::Rejected { .. }) => {}
                        Err(error) => {
                            return Err(self.state_machine_apply_error(phase, entry.index, error));
                        }
                    }
                }
                EntryType::EntryConfChangeV2 => {
                    let change = ConfChangeV2::decode(entry.data.as_ref())
                        .map_err(|error| DurableReplicaError::Raft(error.to_string()))?;
                    let conf_state = self.raw_node.apply_conf_change(&change)?;
                    self.storage.set_conf_state(&conf_state)?;
                    let result = self
                        .state_machine
                        .apply_noop_entry(entry.term, entry.index)
                        .await;
                    if let Err(error) = result {
                        return Err(self.state_machine_apply_error(phase, entry.index, error));
                    }
                    if self.raw_node.raft.state == StateRole::Leader
                        && conf_state.auto_leave
                        && !conf_state.voters_outgoing.is_empty()
                    {
                        if conf_state.voters.binary_search(&self.node_id).is_err() {
                            self.pending_leader_transfer = conf_state.voters.first().copied();
                        } else {
                            self.pending_auto_leave = true;
                        }
                    }
                }
                EntryType::EntryConfChange => {
                    return Err(DurableReplicaError::UnsupportedEntryType);
                }
            }
        }
        Ok(())
    }

    fn state_machine_apply_error(
        &self,
        phase: &'static str,
        entry_index: u64,
        source: ShardRuntimeError,
    ) -> DurableReplicaError {
        DurableReplicaError::StateMachineApply {
            phase,
            entry_index,
            state_machine_applied: self.state_machine.metadata().applied_index,
            raft_applied: self.raw_node.raft.raft_log.applied,
            raft_committed: self.raw_node.raft.raft_log.committed,
            source: Box::new(source),
        }
    }

    fn validate_backend_transition(
        &self,
        body: &raft_command::CommandBodyV1,
    ) -> Result<(), DurableReplicaError> {
        match body {
            raft_command::CommandBodyV1::BeginBackendDualApply(begin) => {
                match self.backend_slot().migration_status() {
                    MigrationStatus::DualApplying {
                        source_generation,
                        target_generation,
                        synchronized_index,
                    } if source_generation == begin.source_generation
                        && target_generation == begin.target_generation
                        && synchronized_index >= begin.fence_index =>
                    {
                        Ok(())
                    }
                    _ => Err(DurableReplicaError::BackendSlotMismatch),
                }
            }
            raft_command::CommandBodyV1::CutoverBackend(cutover) => self
                .validate_finishing_backend_transition(
                    cutover.source_generation,
                    cutover.target_generation,
                ),
            raft_command::CommandBodyV1::AbortBackendMigration(abort) => self
                .validate_finishing_backend_transition(
                    abort.source_generation,
                    abort.target_generation,
                ),
            _ => Ok(()),
        }
    }

    fn validate_finishing_backend_transition(
        &self,
        expected_source: u64,
        expected_target: u64,
    ) -> Result<(), DurableReplicaError> {
        match self.backend_slot().migration_status() {
            MigrationStatus::DualApplying {
                source_generation,
                target_generation,
                ..
            } if source_generation == expected_source && target_generation == expected_target => {
                Ok(())
            }
            MigrationStatus::Idle { generation }
                if generation == expected_source || generation == expected_target =>
            {
                Ok(())
            }
            _ => Err(DurableReplicaError::BackendSlotMismatch),
        }
    }

    fn finish_backend_transition(
        &self,
        body: &raft_command::CommandBodyV1,
    ) -> Result<(), DurableReplicaError> {
        match body {
            raft_command::CommandBodyV1::CutoverBackend(cutover) => {
                if self.backend_slot().generation() == cutover.target_generation {
                    return Ok(());
                }
                self.backend_slot().cutover()?;
                if self.backend_slot().generation() != cutover.target_generation {
                    return Err(DurableReplicaError::BackendSlotMismatch);
                }
                Ok(())
            }
            raft_command::CommandBodyV1::AbortBackendMigration(abort) => {
                if matches!(
                    self.backend_slot().migration_status(),
                    MigrationStatus::Idle { generation } if generation == abort.source_generation
                ) {
                    return Ok(());
                }
                self.backend_slot().abort_migration()?;
                if self.backend_slot().generation() != abort.source_generation {
                    return Err(DurableReplicaError::BackendSlotMismatch);
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    fn run_deferred_membership_action(&mut self) -> Result<(), DurableReplicaError> {
        if let Some(target) = self.pending_leader_transfer.take() {
            self.pending_auto_leave = false;
            self.raw_node.transfer_leader(target);
        } else if std::mem::take(&mut self.pending_auto_leave) && self.is_leader() {
            self.raw_node
                .propose_conf_change(Vec::new(), ConfChangeV2::default())?;
        }
        Ok(())
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

fn reconcile_backend_slot(
    slot: &Arc<HotSwapAdapter>,
    metadata: ReplicaMetadata,
) -> Result<(), DurableReplicaError> {
    match (metadata.backend_lifecycle, slot.migration_status()) {
        (BackendLifecycle::Active, MigrationStatus::Idle { generation })
            if generation == metadata.backend_generation =>
        {
            Ok(())
        }
        (
            BackendLifecycle::Active,
            MigrationStatus::DualApplying {
                target_generation, ..
            },
        ) if target_generation == metadata.backend_generation => {
            slot.cutover()?;
            Ok(())
        }
        (
            BackendLifecycle::Active,
            MigrationStatus::DualApplying {
                source_generation, ..
            },
        ) if source_generation == metadata.backend_generation => Ok(()),
        (
            BackendLifecycle::DualApplying {
                target_generation, ..
            },
            MigrationStatus::DualApplying {
                source_generation,
                target_generation: local_target,
                ..
            },
        ) if source_generation == metadata.backend_generation
            && local_target == target_generation =>
        {
            Ok(())
        }
        _ => Err(DurableReplicaError::BackendSlotMismatch),
    }
}

fn membership_changes(
    current: &ConfState,
    voters: &[u64],
    learners: &[u64],
) -> Result<Vec<ConfChangeSingle>, DurableReplicaError> {
    let valid = !voters.is_empty()
        && voters.iter().all(|node| *node != 0)
        && learners.iter().all(|node| *node != 0)
        && voters.windows(2).all(|pair| pair[0] < pair[1])
        && learners.windows(2).all(|pair| pair[0] < pair[1])
        && voters
            .iter()
            .all(|node| learners.binary_search(node).is_err());
    if !valid {
        return Err(DurableReplicaError::InvalidMembership);
    }
    let mut changes = Vec::new();
    for node in voters {
        if current.voters.binary_search(node).is_err() {
            changes.push(ConfChangeSingle {
                change_type: ConfChangeType::AddNode as i32,
                node_id: *node,
            });
        }
    }
    for node in learners {
        if current.learners.binary_search(node).is_err() {
            changes.push(ConfChangeSingle {
                change_type: ConfChangeType::AddLearnerNode as i32,
                node_id: *node,
            });
        }
    }
    for node in current.voters.iter().chain(&current.learners) {
        if voters.binary_search(node).is_err() && learners.binary_search(node).is_err() {
            changes.push(ConfChangeSingle {
                change_type: ConfChangeType::RemoveNode as i32,
                node_id: *node,
            });
        }
    }
    Ok(changes)
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
    Registry(RegistryError),
    BackendMigration(MigrationError),
    StateMachine(ShardRuntimeError),
    StateMachineApply {
        phase: &'static str,
        entry_index: u64,
        state_machine_applied: u64,
        raft_applied: u64,
        raft_committed: u64,
        source: Box<ShardRuntimeError>,
    },
    Command(raft_command::CommandCodecError),
    LogStore(RaftLogStoreError),
    Raft(String),
    CommitBehindApply {
        commit: u64,
        applied: u64,
    },
    SnapshotInstallRequired {
        applied: u64,
        first_index: u64,
    },
    SnapshotInstallNotConnected,
    InjectedCrashAfterWalBeforeApply,
    UnsupportedEntryType,
    InvalidMembership,
    JointConfigurationInProgress,
    NotLeader,
    BackendSlotMismatch,
    InvalidReadIndexContext,
    DuplicateReadIndexContext,
    TooManyPendingReadIndexRequests,
    ReadIndexLeaderNotReady,
    RequestEnvelopeMismatch {
        expected: u128,
        actual: u128,
    },
}

impl Display for DurableReplicaError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Adapter(error) => write!(formatter, "Adapter error: {error}"),
            Self::Registry(error) => write!(formatter, "Adapter registry error: {error}"),
            Self::BackendMigration(error) => write!(formatter, "backend migration error: {error}"),
            Self::StateMachine(error) => write!(formatter, "state-machine error: {error}"),
            Self::StateMachineApply {
                phase,
                entry_index,
                state_machine_applied,
                raft_applied,
                raft_committed,
                source,
            } => write!(
                formatter,
                "state-machine error while applying {phase} entry {entry_index} \
                 (state-machine={state_machine_applied}, raft-applied={raft_applied}, \
                 raft-committed={raft_committed}): {source}"
            ),
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
            Self::InvalidMembership => formatter.write_str("invalid Raft membership"),
            Self::JointConfigurationInProgress => {
                formatter.write_str("a joint Raft configuration is already in progress")
            }
            Self::NotLeader => formatter.write_str("Replica is not the Raft leader"),
            Self::BackendSlotMismatch => {
                formatter.write_str("local backend slot does not match the replicated lifecycle")
            }
            Self::InvalidReadIndexContext => {
                formatter.write_str("ReadIndex context cannot be empty")
            }
            Self::DuplicateReadIndexContext => {
                formatter.write_str("ReadIndex context is already pending")
            }
            Self::TooManyPendingReadIndexRequests => {
                formatter.write_str("too many pending ReadIndex requests")
            }
            Self::ReadIndexLeaderNotReady => {
                formatter.write_str("Raft leader has not committed an entry in its current term")
            }
            Self::RequestEnvelopeMismatch { expected, actual } => write!(
                formatter,
                "proposal request ID {expected} differs from command request ID {actual}"
            ),
        }
    }
}

impl Error for DurableReplicaError {}

impl From<RegistryError> for DurableReplicaError {
    fn from(error: RegistryError) -> Self {
        Self::Registry(error)
    }
}

impl From<MigrationError> for DurableReplicaError {
    fn from(error: MigrationError) -> Self {
        Self::BackendMigration(error)
    }
}

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
