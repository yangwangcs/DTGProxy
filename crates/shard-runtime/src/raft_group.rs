use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};

use adapter_memory::MemoryAdapter;
use raft::eraftpb::{Entry, EntryType, Message};
use raft::storage::MemStorage;
use raft::{Config, RawNode, StateRole};
use raft_command::CommandEnvelopeV1;
use slog::{Logger, o};

use crate::{DeterministicTransport, ReplicaMetadata, ShardRuntimeError, ShardStateMachine};

const DEFAULT_MAX_DRIVE_ROUNDS: usize = 1_024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProposalReceipt {
    pub request_id: u128,
    pub leader_id: u64,
    pub term: u64,
    pub index: u64,
}

#[derive(Debug)]
pub enum ReplicationError {
    Raft(String),
    Command(raft_command::CommandCodecError),
    StateMachine(ShardRuntimeError),
    NoLeader,
    NodeNotFound { node_id: u64 },
    NodeStopped { node_id: u64 },
    ShardMismatch { expected: u32, actual: u32 },
    StaleEpoch { expected: u64, actual: u64 },
    RequestMismatch { request_id: u128 },
    QuorumUnavailable { request_id: u128, ticks: usize },
    DriveLimitExceeded,
    UnsupportedEntryType,
    GroupAlreadyExists { shard_id: u32 },
    GroupNotFound { shard_id: u32 },
}

impl Display for ReplicationError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Raft(message) => write!(formatter, "Raft error: {message}"),
            Self::Command(error) => write!(formatter, "Raft command error: {error}"),
            Self::StateMachine(error) => write!(formatter, "state-machine error: {error}"),
            Self::NoLeader => formatter.write_str("Raft group has no leader"),
            Self::NodeNotFound { node_id } => write!(formatter, "Raft node {node_id} not found"),
            Self::NodeStopped { node_id } => write!(formatter, "Raft node {node_id} is stopped"),
            Self::ShardMismatch { expected, actual } => {
                write!(formatter, "expected shard {expected}, got shard {actual}")
            }
            Self::StaleEpoch { expected, actual } => {
                write!(
                    formatter,
                    "expected placement epoch {expected}, got {actual}"
                )
            }
            Self::RequestMismatch { request_id } => {
                write!(
                    formatter,
                    "request {request_id} was retried with different bytes"
                )
            }
            Self::QuorumUnavailable { request_id, ticks } => write!(
                formatter,
                "request {request_id} did not commit and apply within {ticks} ticks"
            ),
            Self::DriveLimitExceeded => formatter.write_str("Raft drive loop exceeded its limit"),
            Self::UnsupportedEntryType => {
                formatter.write_str("dynamic Raft membership entry is not supported in Phase 2")
            }
            Self::GroupAlreadyExists { shard_id } => {
                write!(formatter, "Shard Group {shard_id} already exists")
            }
            Self::GroupNotFound { shard_id } => {
                write!(formatter, "Shard Group {shard_id} not found")
            }
        }
    }
}

impl Error for ReplicationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Command(error) => Some(error),
            Self::StateMachine(error) => Some(error),
            _ => None,
        }
    }
}

impl From<ShardRuntimeError> for ReplicationError {
    fn from(error: ShardRuntimeError) -> Self {
        Self::StateMachine(error)
    }
}

impl From<raft_command::CommandCodecError> for ReplicationError {
    fn from(error: raft_command::CommandCodecError) -> Self {
        Self::Command(error)
    }
}

#[derive(Clone)]
struct PendingProposal {
    command: Vec<u8>,
}

#[derive(Clone)]
struct CompletedProposal {
    command: Vec<u8>,
    receipt: ProposalReceipt,
}

struct AppliedEvent {
    request_id: u128,
    term: u64,
    index: u64,
    applied_by_leader: bool,
}

struct ReadyOutput {
    messages: Vec<Message>,
    events: Vec<AppliedEvent>,
}

struct RaftReplica {
    node_id: u64,
    config: Config,
    storage: MemStorage,
    raw_node: Option<RawNode<MemStorage>>,
    state_machine: ShardStateMachine<MemoryAdapter>,
}

impl RaftReplica {
    async fn new(
        node_id: u64,
        voters: &[u64],
        shard_id: u32,
        placement_epoch: u64,
    ) -> Result<Self, ReplicationError> {
        let storage = MemStorage::new_with_conf_state((voters.to_vec(), Vec::new()));
        let state_machine =
            ShardStateMachine::open(MemoryAdapter::new(), shard_id, placement_epoch).await?;
        let config = raft_config(node_id, 0)?;
        let raw_node = RawNode::new(&config, storage.clone(), &discard_logger())
            .map_err(|error| ReplicationError::Raft(error.to_string()))?;
        Ok(Self {
            node_id,
            config,
            storage,
            raw_node: Some(raw_node),
            state_machine,
        })
    }

    fn is_running(&self) -> bool {
        self.raw_node.is_some()
    }

    fn role(&self) -> Option<StateRole> {
        self.raw_node.as_ref().map(|node| node.raft.state)
    }

    fn leader_id(&self) -> Option<u64> {
        self.raw_node
            .as_ref()
            .map(|node| node.raft.leader_id)
            .filter(|leader| *leader != 0)
    }

    fn campaign(&mut self) -> Result<(), ReplicationError> {
        self.raw_node
            .as_mut()
            .ok_or(ReplicationError::NodeStopped {
                node_id: self.node_id,
            })?
            .campaign()
            .map_err(|error| ReplicationError::Raft(error.to_string()))
    }

    fn propose(&mut self, request_id: u128, command: Vec<u8>) -> Result<(), ReplicationError> {
        self.raw_node
            .as_mut()
            .ok_or(ReplicationError::NodeStopped {
                node_id: self.node_id,
            })?
            .propose(request_id.to_be_bytes().to_vec(), command)
            .map_err(|error| ReplicationError::Raft(error.to_string()))
    }

    fn step(&mut self, message: Message) -> Result<(), ReplicationError> {
        let Some(raw_node) = self.raw_node.as_mut() else {
            return Ok(());
        };
        raw_node
            .step(message)
            .map_err(|error| ReplicationError::Raft(error.to_string()))
    }

    fn tick(&mut self) {
        if let Some(raw_node) = self.raw_node.as_mut() {
            raw_node.tick();
        }
    }

    fn has_ready(&self) -> bool {
        self.raw_node.as_ref().is_some_and(RawNode::has_ready)
    }

    async fn process_ready(&mut self) -> Result<ReadyOutput, ReplicationError> {
        let raw_node = self
            .raw_node
            .as_mut()
            .ok_or(ReplicationError::NodeStopped {
                node_id: self.node_id,
            })?;
        if !raw_node.has_ready() {
            return Ok(ReadyOutput {
                messages: Vec::new(),
                events: Vec::new(),
            });
        }
        let applied_by_leader = raw_node.raft.state == StateRole::Leader;
        let mut ready = raw_node.ready();
        let mut messages = ready.take_messages();

        if !ready.snapshot().is_empty() {
            self.storage
                .wl()
                .apply_snapshot(ready.snapshot().clone())
                .map_err(|error| ReplicationError::Raft(error.to_string()))?;
        }
        self.storage
            .wl()
            .append(ready.entries())
            .map_err(|error| ReplicationError::Raft(error.to_string()))?;
        if let Some(hard_state) = ready.hs() {
            self.storage.wl().set_hardstate(hard_state.clone());
        }
        messages.extend(ready.take_persisted_messages());

        let committed = ready.take_committed_entries();
        let mut events =
            apply_entries(&mut self.state_machine, committed, applied_by_leader).await?;
        let mut light_ready = raw_node.advance(ready);
        if let Some(commit_index) = light_ready.commit_index() {
            self.storage.wl().mut_hard_state().set_commit(commit_index);
        }
        messages.extend(light_ready.take_messages());
        events.extend(
            apply_entries(
                &mut self.state_machine,
                light_ready.take_committed_entries(),
                applied_by_leader,
            )
            .await?,
        );
        raw_node.advance_apply();
        Ok(ReadyOutput { messages, events })
    }

    fn stop(&mut self) {
        self.raw_node = None;
    }

    fn restart(&mut self) -> Result<(), ReplicationError> {
        if self.raw_node.is_some() {
            return Ok(());
        }
        self.config.applied = self.state_machine.metadata().applied_index;
        self.raw_node = Some(
            RawNode::new(&self.config, self.storage.clone(), &discard_logger())
                .map_err(|error| ReplicationError::Raft(error.to_string()))?,
        );
        Ok(())
    }
}

async fn apply_entries(
    state_machine: &mut ShardStateMachine<MemoryAdapter>,
    entries: Vec<Entry>,
    applied_by_leader: bool,
) -> Result<Vec<AppliedEvent>, ReplicationError> {
    let mut events = Vec::new();
    for entry in entries {
        match entry.get_entry_type() {
            EntryType::EntryNormal if entry.data.is_empty() => {
                state_machine
                    .apply_noop_entry(entry.term, entry.index)
                    .await?;
            }
            EntryType::EntryNormal => {
                let command = CommandEnvelopeV1::decode(&entry.data)?;
                state_machine
                    .apply_entry(entry.term, entry.index, &entry.data)
                    .await?;
                events.push(AppliedEvent {
                    request_id: command.request_id,
                    term: entry.term,
                    index: entry.index,
                    applied_by_leader,
                });
            }
            EntryType::EntryConfChange | EntryType::EntryConfChangeV2 => {
                return Err(ReplicationError::UnsupportedEntryType);
            }
        }
    }
    Ok(events)
}

fn raft_config(node_id: u64, applied: u64) -> Result<Config, ReplicationError> {
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
        .map_err(|error| ReplicationError::Raft(error.to_string()))?;
    Ok(config)
}

fn discard_logger() -> Logger {
    Logger::root(slog::Discard, o!())
}

pub struct InProcessShardGroup {
    shard_id: u32,
    placement_epoch: u64,
    replicas: BTreeMap<u64, RaftReplica>,
    transport: DeterministicTransport,
    pending: BTreeMap<u128, PendingProposal>,
    completed: BTreeMap<u128, CompletedProposal>,
    applied_events: BTreeMap<(u64, u128), (u64, u64)>,
}

impl InProcessShardGroup {
    pub async fn new(
        shard_id: u32,
        placement_epoch: u64,
        voters: &[u64],
    ) -> Result<Self, ReplicationError> {
        let mut replicas = BTreeMap::new();
        for node_id in voters {
            replicas.insert(
                *node_id,
                RaftReplica::new(*node_id, voters, shard_id, placement_epoch).await?,
            );
        }
        Ok(Self {
            shard_id,
            placement_epoch,
            replicas,
            transport: DeterministicTransport::new(),
            pending: BTreeMap::new(),
            completed: BTreeMap::new(),
            applied_events: BTreeMap::new(),
        })
    }

    #[must_use]
    pub const fn shard_id(&self) -> u32 {
        self.shard_id
    }

    #[must_use]
    pub const fn placement_epoch(&self) -> u64 {
        self.placement_epoch
    }

    #[must_use]
    pub const fn transport(&self) -> &DeterministicTransport {
        &self.transport
    }

    #[must_use]
    pub const fn transport_mut(&mut self) -> &mut DeterministicTransport {
        &mut self.transport
    }

    pub async fn elect(&mut self, node_id: u64) -> Result<(), ReplicationError> {
        self.replicas
            .get_mut(&node_id)
            .ok_or(ReplicationError::NodeNotFound { node_id })?
            .campaign()?;
        for tick in 0..60 {
            self.drain(DEFAULT_MAX_DRIVE_ROUNDS).await?;
            if self.leader_id() == Some(node_id) {
                return Ok(());
            }
            self.tick().await?;
            if tick % 10 == 9 {
                self.replicas
                    .get_mut(&node_id)
                    .expect("candidate was validated before election")
                    .campaign()?;
            }
        }
        Err(ReplicationError::NoLeader)
    }

    #[must_use]
    pub fn leader_id(&self) -> Option<u64> {
        self.replicas.iter().find_map(|(node_id, replica)| {
            (replica.role() == Some(StateRole::Leader)).then_some(*node_id)
        })
    }

    pub async fn propose_and_wait(
        &mut self,
        command: Vec<u8>,
        max_ticks: usize,
    ) -> Result<ProposalReceipt, ReplicationError> {
        let envelope = CommandEnvelopeV1::decode(&command)?;
        if envelope.shard_id != self.shard_id {
            return Err(ReplicationError::ShardMismatch {
                expected: self.shard_id,
                actual: envelope.shard_id,
            });
        }
        if envelope.placement_epoch != self.placement_epoch {
            return Err(ReplicationError::StaleEpoch {
                expected: self.placement_epoch,
                actual: envelope.placement_epoch,
            });
        }
        let request_id = envelope.request_id;
        if let Some(completed) = self.completed.get(&request_id) {
            if completed.command != command {
                return Err(ReplicationError::RequestMismatch { request_id });
            }
            return Ok(completed.receipt);
        }
        let already_pending = if let Some(pending) = self.pending.get(&request_id) {
            if pending.command != command {
                return Err(ReplicationError::RequestMismatch { request_id });
            }
            true
        } else {
            false
        };
        if !already_pending {
            let leader_id = self.leader_id().ok_or(ReplicationError::NoLeader)?;
            self.replicas
                .get_mut(&leader_id)
                .expect("leader belongs to Replica map")
                .propose(request_id, command.clone())?;
            self.pending.insert(
                request_id,
                PendingProposal {
                    command: command.clone(),
                },
            );
        }

        self.drain(DEFAULT_MAX_DRIVE_ROUNDS).await?;
        if let Some(completed) = self.completed.get(&request_id) {
            return Ok(completed.receipt);
        }
        for _ in 0..max_ticks {
            self.tick().await?;
            if let Some(completed) = self.completed.get(&request_id) {
                return Ok(completed.receipt);
            }
        }
        Err(ReplicationError::QuorumUnavailable {
            request_id,
            ticks: max_ticks,
        })
    }

    pub async fn tick(&mut self) -> Result<(), ReplicationError> {
        for replica in self.replicas.values_mut() {
            replica.tick();
        }
        self.transport.advance();
        self.drain(DEFAULT_MAX_DRIVE_ROUNDS).await
    }

    pub async fn drive_ticks(&mut self, ticks: usize) -> Result<(), ReplicationError> {
        for _ in 0..ticks {
            self.tick().await?;
        }
        Ok(())
    }

    async fn drain(&mut self, max_rounds: usize) -> Result<(), ReplicationError> {
        for _ in 0..max_rounds {
            let ready_messages = self.transport.take_ready();
            let mut progressed = !ready_messages.is_empty();
            for message in ready_messages {
                if let Some(replica) = self.replicas.get_mut(&message.to) {
                    replica.step(message)?;
                }
            }

            let node_ids: Vec<_> = self.replicas.keys().copied().collect();
            for node_id in node_ids {
                let has_ready = self
                    .replicas
                    .get(&node_id)
                    .is_some_and(RaftReplica::has_ready);
                if !has_ready {
                    continue;
                }
                progressed = true;
                let output = self
                    .replicas
                    .get_mut(&node_id)
                    .expect("node id came from Replica map")
                    .process_ready()
                    .await?;
                self.transport.send_all(output.messages);
                self.record_events(node_id, output.events);
            }
            self.complete_requests_applied_on_current_leader();
            if !progressed {
                return Ok(());
            }
        }
        Err(ReplicationError::DriveLimitExceeded)
    }

    fn record_events(&mut self, node_id: u64, events: Vec<AppliedEvent>) {
        for event in events {
            self.applied_events
                .insert((node_id, event.request_id), (event.term, event.index));
            if event.applied_by_leader {
                self.complete_request(node_id, event.request_id, event.term, event.index);
            }
        }
    }

    fn complete_requests_applied_on_current_leader(&mut self) {
        let Some(leader_id) = self.leader_id() else {
            return;
        };
        let applied: Vec<_> = self
            .applied_events
            .iter()
            .filter_map(|((node_id, request_id), (term, index))| {
                (*node_id == leader_id).then_some((*request_id, *term, *index))
            })
            .collect();
        for (request_id, term, index) in applied {
            self.complete_request(leader_id, request_id, term, index);
        }
    }

    fn complete_request(&mut self, leader_id: u64, request_id: u128, term: u64, index: u64) {
        let Some(pending) = self.pending.remove(&request_id) else {
            return;
        };
        self.completed.insert(
            request_id,
            CompletedProposal {
                command: pending.command,
                receipt: ProposalReceipt {
                    request_id,
                    leader_id,
                    term,
                    index,
                },
            },
        );
    }

    pub fn stop_node(&mut self, node_id: u64) -> Result<(), ReplicationError> {
        self.replicas
            .get_mut(&node_id)
            .ok_or(ReplicationError::NodeNotFound { node_id })?
            .stop();
        Ok(())
    }

    pub fn restart_node(&mut self, node_id: u64) -> Result<(), ReplicationError> {
        self.replicas
            .get_mut(&node_id)
            .ok_or(ReplicationError::NodeNotFound { node_id })?
            .restart()
    }

    #[must_use]
    pub fn is_running(&self, node_id: u64) -> bool {
        self.replicas
            .get(&node_id)
            .is_some_and(RaftReplica::is_running)
    }

    #[must_use]
    pub fn replica_metadata(&self, node_id: u64) -> Option<ReplicaMetadata> {
        self.replicas
            .get(&node_id)
            .map(|replica| replica.state_machine.metadata())
    }

    #[must_use]
    pub fn replica_adapter(&self, node_id: u64) -> Option<&MemoryAdapter> {
        self.replicas
            .get(&node_id)
            .map(|replica| replica.state_machine.adapter())
    }

    #[must_use]
    pub fn reported_leader(&self, node_id: u64) -> Option<u64> {
        self.replicas.get(&node_id).and_then(RaftReplica::leader_id)
    }
}

pub struct MultiRaftRuntime {
    groups: BTreeMap<u32, InProcessShardGroup>,
    owners: BTreeMap<(u64, u32), ()>,
}

impl MultiRaftRuntime {
    #[must_use]
    pub fn new() -> Self {
        Self {
            groups: BTreeMap::new(),
            owners: BTreeMap::new(),
        }
    }

    pub fn insert_group(&mut self, group: InProcessShardGroup) -> Result<(), ReplicationError> {
        let shard_id = group.shard_id();
        if self.groups.contains_key(&shard_id) {
            return Err(ReplicationError::GroupAlreadyExists { shard_id });
        }
        for node_id in group.replicas.keys() {
            self.owners.insert((*node_id, shard_id), ());
        }
        self.groups.insert(shard_id, group);
        Ok(())
    }

    #[must_use]
    pub fn owns(&self, node_id: u64, shard_id: u32) -> bool {
        self.owners.contains_key(&(node_id, shard_id))
    }

    pub fn group_mut(
        &mut self,
        shard_id: u32,
    ) -> Result<&mut InProcessShardGroup, ReplicationError> {
        self.groups
            .get_mut(&shard_id)
            .ok_or(ReplicationError::GroupNotFound { shard_id })
    }

    #[must_use]
    pub fn group(&self, shard_id: u32) -> Option<&InProcessShardGroup> {
        self.groups.get(&shard_id)
    }
}

impl Default for MultiRaftRuntime {
    fn default() -> Self {
        Self::new()
    }
}
