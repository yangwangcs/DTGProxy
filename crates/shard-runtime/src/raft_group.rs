use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::time::{Duration, Instant};

use adapter_memory::MemoryAdapter;
use raft::eraftpb::{Entry, EntryType, Message};
use raft::storage::MemStorage;
use raft::{Config, RawNode, ReadState, StateRole};
use raft_command::CommandEnvelopeV1;
use slog::{Logger, o};
use temporal_types::TransactionTime;

use crate::{
    DeterministicTransport, FollowerReadProof, ReadBarrierError, ReadPermit, ReadPermitMode,
    ReplicaMetadata, ShardRuntimeError, ShardStateMachine,
};

const DEFAULT_MAX_DRIVE_ROUNDS: usize = 1_024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProposalReceipt {
    pub request_id: u128,
    pub leader_id: u64,
    pub term: u64,
    pub index: u64,
    /// Local time from proposal submission until this Replica observes the
    /// entry in Raft's committed-entry stream.
    pub proposal_to_commit: Duration,
    /// Local time from observing the committed entry to completing its Adapter apply.
    ///
    /// This is intentionally narrower than end-to-end proposal latency and is suitable
    /// for separating state-machine/Adapter delay from Raft quorum delay.
    pub commit_to_apply: Duration,
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
    proposed_at: Instant,
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
    commit_observed_at: Instant,
    commit_to_apply: Duration,
}

struct ReadyOutput {
    messages: Vec<Message>,
    events: Vec<AppliedEvent>,
    read_states: Vec<ReadState>,
}

#[derive(Clone, Copy)]
struct CompletedReadState {
    node_id: u64,
    term: u64,
    index: u64,
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

    fn request_read_index(&mut self, context: Vec<u8>) -> Result<(), ReplicationError> {
        self.raw_node
            .as_mut()
            .ok_or(ReplicationError::NodeStopped {
                node_id: self.node_id,
            })?
            .read_index(context);
        Ok(())
    }

    fn current_term(&self) -> Option<u64> {
        self.raw_node.as_ref().map(|node| node.raft.term)
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
                read_states: Vec::new(),
            });
        }
        let applied_by_leader = raw_node.raft.state == StateRole::Leader;
        let mut ready = raw_node.ready();
        let mut messages = ready.take_messages();
        let read_states = ready.take_read_states();

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
        Ok(ReadyOutput {
            messages,
            events,
            read_states,
        })
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
                let commit_observed_at = Instant::now();
                state_machine
                    .apply_entry(entry.term, entry.index, &entry.data)
                    .await?;
                events.push(AppliedEvent {
                    request_id: command.request_id,
                    term: entry.term,
                    index: entry.index,
                    applied_by_leader,
                    commit_observed_at,
                    commit_to_apply: commit_observed_at.elapsed(),
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
    applied_events: BTreeMap<(u64, u128), (u64, u64, Instant, Duration)>,
    completed_read_states: BTreeMap<Vec<u8>, CompletedReadState>,
    pending_read_contexts: BTreeSet<Vec<u8>>,
    next_read_sequence: u64,
    next_internal_request: u64,
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
            completed_read_states: BTreeMap::new(),
            pending_read_contexts: BTreeSet::new(),
            next_read_sequence: 1,
            next_internal_request: 1,
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
                    proposed_at: Instant::now(),
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

    pub async fn leader_read_permit(
        &mut self,
        node_id: u64,
        placement_epoch: u64,
        max_ticks: usize,
    ) -> Result<ReadPermit, ReadBarrierError> {
        self.validate_read_epoch(placement_epoch)?;
        let leader_id = self.leader_id();
        if leader_id != Some(node_id) {
            return Err(ReadBarrierError::NotLeader {
                node_id,
                leader_hint: leader_id,
            });
        }
        let proof = self
            .issue_follower_read_proof(placement_epoch, max_ticks)
            .await?;
        if self.leader_id() != Some(node_id) || proof.leader_id != node_id {
            return Err(ReadBarrierError::NotLeader {
                node_id,
                leader_hint: self.leader_id(),
            });
        }
        self.validate_applied_index(node_id, proof.read_index)?;
        Ok(ReadPermit::new(
            self.shard_id,
            self.placement_epoch,
            node_id,
            proof.read_index,
            ReadPermitMode::LeaderLinearizable,
        ))
    }

    pub async fn issue_follower_read_proof(
        &mut self,
        placement_epoch: u64,
        max_ticks: usize,
    ) -> Result<FollowerReadProof, ReadBarrierError> {
        self.validate_read_epoch(placement_epoch)?;
        let leader_id = self.leader_id().ok_or(ReadBarrierError::NotReady {
            node_id: None,
            reason: "the Shard Group has no leader",
        })?;
        let leader_term = self
            .replicas
            .get(&leader_id)
            .and_then(RaftReplica::current_term)
            .ok_or(ReadBarrierError::NotReady {
                node_id: Some(leader_id),
                reason: "the leader Replica is stopped",
            })?;
        let context = self.next_read_context()?;
        self.pending_read_contexts.insert(context.clone());
        if self
            .replicas
            .get_mut(&leader_id)
            .expect("leader belongs to Replica map")
            .request_read_index(context.clone())
            .is_err()
        {
            self.finish_read_context(&context);
            return Err(ReadBarrierError::NotReady {
                node_id: Some(leader_id),
                reason: "ReadIndex request could not be submitted",
            });
        }

        for tick in 0..=max_ticks {
            if self.drain(DEFAULT_MAX_DRIVE_ROUNDS).await.is_err() {
                self.finish_read_context(&context);
                return Err(ReadBarrierError::NotReady {
                    node_id: Some(leader_id),
                    reason: "Raft could not complete the ReadIndex barrier",
                });
            }
            if let Some(completed) = self.completed_read_states.get(&context).copied() {
                let current_leader = self.leader_id();
                let current_term = current_leader
                    .and_then(|id| self.replicas.get(&id))
                    .and_then(RaftReplica::current_term);
                if completed.node_id != leader_id
                    || current_leader != Some(leader_id)
                    || current_term != Some(leader_term)
                    || completed.term != leader_term
                {
                    self.finish_read_context(&context);
                    return Err(ReadBarrierError::NotReady {
                        node_id: Some(leader_id),
                        reason: "leadership changed while ReadIndex was in flight",
                    });
                }
                if self
                    .validate_applied_index(leader_id, completed.index)
                    .is_ok()
                {
                    self.finish_read_context(&context);
                    return Ok(FollowerReadProof {
                        shard_id: self.shard_id,
                        placement_epoch: self.placement_epoch,
                        leader_id,
                        leader_term,
                        read_index: completed.index,
                    });
                }
            }
            if tick == max_ticks {
                break;
            }
            if self.tick().await.is_err() {
                self.finish_read_context(&context);
                return Err(ReadBarrierError::NotReady {
                    node_id: Some(leader_id),
                    reason: "Raft could not advance the ReadIndex barrier",
                });
            }
        }
        self.finish_read_context(&context);
        Err(ReadBarrierError::NotReady {
            node_id: Some(leader_id),
            reason: "ReadIndex did not receive quorum confirmation before the deadline",
        })
    }

    pub fn follower_read_permit(
        &self,
        node_id: u64,
        placement_epoch: u64,
        read_ts: TransactionTime,
        proof: &FollowerReadProof,
    ) -> Result<ReadPermit, ReadBarrierError> {
        self.validate_read_epoch(placement_epoch)?;
        if proof.shard_id != self.shard_id || proof.placement_epoch != self.placement_epoch {
            return Err(ReadBarrierError::StaleEpoch {
                expected: self.placement_epoch,
                actual: proof.placement_epoch,
            });
        }
        let replica = self
            .replicas
            .get(&node_id)
            .ok_or(ReadBarrierError::NodeNotFound { node_id })?;
        if !replica.is_running() {
            return Err(ReadBarrierError::NotReady {
                node_id: Some(node_id),
                reason: "the follower Replica is stopped",
            });
        }
        if self.leader_id() == Some(node_id) {
            return Err(ReadBarrierError::NotReady {
                node_id: Some(node_id),
                reason: "a follower snapshot permit cannot target the leader",
            });
        }
        let current_leader = self.leader_id();
        let current_term = current_leader
            .and_then(|leader| self.replicas.get(&leader))
            .and_then(RaftReplica::current_term);
        if current_leader != Some(proof.leader_id) || current_term != Some(proof.leader_term) {
            return Err(ReadBarrierError::NotReady {
                node_id: Some(node_id),
                reason: "the ReadIndex proof belongs to an old leader term",
            });
        }
        self.validate_applied_index(node_id, proof.read_index)?;
        let safe_ts = replica.state_machine.servable_safe_ts().map_err(|_| {
            ReadBarrierError::AdapterLagging {
                node_id,
                required_index: proof.read_index,
                applied_index: replica.state_machine.metadata().applied_index,
            }
        })?;
        if safe_ts < read_ts {
            return Err(ReadBarrierError::NotReady {
                node_id: Some(node_id),
                reason: "follower safe time is below the requested transaction time",
            });
        }
        Ok(ReadPermit::new(
            self.shard_id,
            self.placement_epoch,
            node_id,
            proof.read_index,
            ReadPermitMode::FollowerSnapshot { read_ts },
        ))
    }

    pub async fn advance_closed_timestamp(
        &mut self,
        closed_ts: TransactionTime,
        max_ticks: usize,
    ) -> Result<ProposalReceipt, ReplicationError> {
        let request_id = (1_u128 << 127) | u128::from(self.next_internal_request);
        self.next_internal_request = self.next_internal_request.saturating_add(1);
        let command = CommandEnvelopeV1::new(
            self.shard_id,
            self.placement_epoch,
            request_id,
            raft_command::CommandBodyV1::ClosedTimestampTick(closed_ts),
        )
        .encode()?;
        self.propose_and_wait(command, max_ticks).await
    }

    fn validate_read_epoch(&self, placement_epoch: u64) -> Result<(), ReadBarrierError> {
        if placement_epoch != self.placement_epoch {
            return Err(ReadBarrierError::StaleEpoch {
                expected: self.placement_epoch,
                actual: placement_epoch,
            });
        }
        Ok(())
    }

    fn validate_applied_index(
        &self,
        node_id: u64,
        required_index: u64,
    ) -> Result<(), ReadBarrierError> {
        let replica = self
            .replicas
            .get(&node_id)
            .ok_or(ReadBarrierError::NodeNotFound { node_id })?;
        let applied_index = replica.state_machine.metadata().applied_index;
        if !replica.state_machine.is_healthy() || applied_index < required_index {
            return Err(ReadBarrierError::AdapterLagging {
                node_id,
                required_index,
                applied_index,
            });
        }
        Ok(())
    }

    fn next_read_context(&mut self) -> Result<Vec<u8>, ReadBarrierError> {
        let sequence = self.next_read_sequence;
        self.next_read_sequence =
            self.next_read_sequence
                .checked_add(1)
                .ok_or(ReadBarrierError::NotReady {
                    node_id: self.leader_id(),
                    reason: "ReadIndex context sequence is exhausted",
                })?;
        let mut context = Vec::with_capacity(24);
        context.extend_from_slice(b"DTRI");
        context.extend_from_slice(&self.shard_id.to_be_bytes());
        context.extend_from_slice(&self.placement_epoch.to_be_bytes());
        context.extend_from_slice(&sequence.to_be_bytes());
        Ok(context)
    }

    fn finish_read_context(&mut self, context: &[u8]) {
        self.pending_read_contexts.remove(context);
        self.completed_read_states.remove(context);
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
                self.record_read_states(node_id, output.read_states);
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
            self.applied_events.insert(
                (node_id, event.request_id),
                (
                    event.term,
                    event.index,
                    event.commit_observed_at,
                    event.commit_to_apply,
                ),
            );
            if event.applied_by_leader {
                self.complete_request(
                    node_id,
                    event.request_id,
                    event.term,
                    event.index,
                    event.commit_observed_at,
                    event.commit_to_apply,
                );
            }
        }
    }

    fn record_read_states(&mut self, node_id: u64, read_states: Vec<ReadState>) {
        let Some(term) = self
            .replicas
            .get(&node_id)
            .and_then(RaftReplica::current_term)
        else {
            return;
        };
        for state in read_states {
            if !self.pending_read_contexts.contains(&state.request_ctx) {
                continue;
            }
            self.completed_read_states.insert(
                state.request_ctx,
                CompletedReadState {
                    node_id,
                    term,
                    index: state.index,
                },
            );
        }
    }

    fn complete_requests_applied_on_current_leader(&mut self) {
        let Some(leader_id) = self.leader_id() else {
            return;
        };
        let applied: Vec<_> = self
            .applied_events
            .iter()
            .filter_map(
                |((node_id, request_id), (term, index, commit_observed_at, commit_to_apply))| {
                    (*node_id == leader_id).then_some((
                        *request_id,
                        *term,
                        *index,
                        *commit_observed_at,
                        *commit_to_apply,
                    ))
                },
            )
            .collect();
        for (request_id, term, index, commit_observed_at, commit_to_apply) in applied {
            self.complete_request(
                leader_id,
                request_id,
                term,
                index,
                commit_observed_at,
                commit_to_apply,
            );
        }
    }

    fn complete_request(
        &mut self,
        leader_id: u64,
        request_id: u128,
        term: u64,
        index: u64,
        commit_observed_at: Instant,
        commit_to_apply: Duration,
    ) {
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
                    proposal_to_commit: commit_observed_at
                        .saturating_duration_since(pending.proposed_at),
                    commit_to_apply,
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
