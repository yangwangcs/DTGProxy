use std::sync::Arc;

use dtg_kernel::{Digest32, ReplicaId, TransactionTime};
use dtg_storage::{ConsensusStore, ProviderKind, ReplicaBinding, ReplicaStateStore};
use raft::eraftpb::{Entry, EntryType, Message};
use raft::{Config, RawNode};
use slog::{Logger, o};

use crate::{ApplyOutcome, RaftStore, ShardCommand, ShardError, ShardStateMachine};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplicaLifecycle {
    Added,
    Running,
    Stopped,
    Sealed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProposalReceipt {
    term: u64,
    index: u64,
    command_id: u128,
    digest: Digest32,
    replayed: bool,
}

impl ProposalReceipt {
    pub const fn term(&self) -> u64 {
        self.term
    }

    pub const fn index(&self) -> u64 {
        self.index
    }

    pub const fn command_id(&self) -> u128 {
        self.command_id
    }

    pub const fn digest(&self) -> Digest32 {
        self.digest
    }

    pub const fn replayed(&self) -> bool {
        self.replayed
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaObservation {
    binding: ReplicaBinding,
    lifecycle: ReplicaLifecycle,
    applied_index: u64,
    leader_id: Option<ReplicaId>,
    closed_timestamp: Option<TransactionTime>,
}

#[derive(Default)]
pub struct RaftProgress {
    messages: Vec<Message>,
    receipts: Vec<ProposalReceipt>,
}

impl RaftProgress {
    pub fn messages(&self) -> &[Message] {
        &self.messages
    }

    pub fn take_messages(&mut self) -> Vec<Message> {
        std::mem::take(&mut self.messages)
    }

    pub fn receipts(&self) -> &[ProposalReceipt] {
        &self.receipts
    }
}

impl ReplicaObservation {
    pub const fn binding(&self) -> &ReplicaBinding {
        &self.binding
    }

    pub const fn provider_kind(&self) -> &ProviderKind {
        self.binding.provider_kind()
    }

    pub const fn lifecycle(&self) -> ReplicaLifecycle {
        self.lifecycle
    }

    pub const fn applied_index(&self) -> u64 {
        self.applied_index
    }

    pub const fn leader_id(&self) -> Option<ReplicaId> {
        self.leader_id
    }

    pub const fn closed_timestamp(&self) -> Option<TransactionTime> {
        self.closed_timestamp
    }

    pub const fn is_running(&self) -> bool {
        matches!(self.lifecycle, ReplicaLifecycle::Running)
    }
}

pub struct RaftReplica {
    binding: ReplicaBinding,
    wal: RaftStore,
    machine: ShardStateMachine,
    node: Option<RawNode<RaftStore>>,
    lifecycle: ReplicaLifecycle,
}

impl RaftReplica {
    pub fn open(
        consensus_store: Arc<dyn ConsensusStore>,
        state_store: Arc<dyn ReplicaStateStore>,
    ) -> Result<Self, ShardError> {
        if !same_replica_identity(consensus_store.binding(), state_store.binding()) {
            return Err(ShardError::BindingMismatch);
        }
        let binding = state_store.binding().clone();
        let wal = RaftStore::new(consensus_store)?;
        let machine = ShardStateMachine::new(binding.clone(), state_store)?;
        Ok(Self {
            binding,
            wal,
            machine,
            node: None,
            lifecycle: ReplicaLifecycle::Added,
        })
    }

    pub fn recover(&mut self) -> Result<Vec<ApplyOutcome>, ShardError> {
        if self.lifecycle == ReplicaLifecycle::Running {
            return Err(ShardError::InvalidLifecycle(
                "recovery requires a stopped replica".into(),
            ));
        }
        let previously_applied = self.machine.applied_index();
        let entries = self.wal.recovery_entries(previously_applied)?;
        let (_, outcomes) = self.apply_entries(entries)?;
        Ok(outcomes
            .into_iter()
            .filter(|outcome| outcome.applied_index() > previously_applied)
            .collect())
    }

    pub fn campaign(&mut self) -> Result<(), ShardError> {
        self.running_node()?.campaign().map_err(ShardError::Raft)
    }

    pub fn tick(&mut self) -> Result<bool, ShardError> {
        Ok(self.running_node()?.tick())
    }

    pub fn step(&mut self, message: Message) -> Result<(), ShardError> {
        self.running_node()?.step(message).map_err(ShardError::Raft)
    }

    pub fn propose(&mut self, command: ShardCommand) -> Result<(), ShardError> {
        self.machine.validate_command(&command)?;
        let context = command.header().command_id().get().to_be_bytes().to_vec();
        let data = command.encode_current()?;
        self.running_node()?
            .propose(context, data)
            .map_err(ShardError::Raft)
    }

    pub fn drive_ready(&mut self) -> Result<RaftProgress, ShardError> {
        let mut node = self.node.take().ok_or_else(|| {
            ShardError::InvalidLifecycle("Ready processing requires a running replica".into())
        })?;
        let result = self.drive_node(&mut node);
        self.node = Some(node);
        result
    }

    pub fn start(&mut self) -> Result<(), ShardError> {
        match self.lifecycle {
            ReplicaLifecycle::Running => return Ok(()),
            ReplicaLifecycle::Sealed => {
                return Err(ShardError::InvalidLifecycle(
                    "sealed replica cannot be restarted".into(),
                ));
            }
            ReplicaLifecycle::Added | ReplicaLifecycle::Stopped => {}
        }
        self.recover()?;
        let mut config = Config::new(self.binding.replica_id().get());
        config.applied = self.machine.applied_index();
        config.validate().map_err(ShardError::Raft)?;
        let logger = Logger::root(slog::Discard, o!());
        self.node =
            Some(RawNode::new(&config, self.wal.clone(), &logger).map_err(ShardError::Raft)?);
        self.lifecycle = ReplicaLifecycle::Running;
        Ok(())
    }

    pub fn stop(&mut self) -> Result<(), ShardError> {
        if self.lifecycle == ReplicaLifecycle::Sealed {
            return Err(ShardError::InvalidLifecycle(
                "sealed replica is already terminal".into(),
            ));
        }
        self.node = None;
        self.lifecycle = ReplicaLifecycle::Stopped;
        Ok(())
    }

    pub fn transfer_leader(&mut self, target: ReplicaId) -> Result<(), ShardError> {
        let node = self.node.as_mut().ok_or_else(|| {
            ShardError::InvalidLifecycle("leader transfer requires a running replica".into())
        })?;
        node.transfer_leader(target.get());
        Ok(())
    }

    pub fn seal(&mut self) -> Result<(), ShardError> {
        if self.lifecycle == ReplicaLifecycle::Running {
            return Err(ShardError::InvalidLifecycle(
                "running replica must be stopped before sealing".into(),
            ));
        }
        self.node = None;
        self.lifecycle = ReplicaLifecycle::Sealed;
        Ok(())
    }

    pub fn observe(&self) -> ReplicaObservation {
        let leader_id = self
            .node
            .as_ref()
            .and_then(|node| ReplicaId::new(node.status().ss.leader_id).ok());
        ReplicaObservation {
            binding: self.binding.clone(),
            lifecycle: self.lifecycle,
            applied_index: self.machine.applied_index(),
            leader_id,
            closed_timestamp: self.machine.closed_timestamp(),
        }
    }

    pub const fn binding(&self) -> &ReplicaBinding {
        &self.binding
    }

    pub const fn lifecycle(&self) -> ReplicaLifecycle {
        self.lifecycle
    }

    fn running_node(&mut self) -> Result<&mut RawNode<RaftStore>, ShardError> {
        self.node.as_mut().ok_or_else(|| {
            ShardError::InvalidLifecycle("operation requires a running replica".into())
        })
    }

    fn drive_node(&mut self, node: &mut RawNode<RaftStore>) -> Result<RaftProgress, ShardError> {
        let mut progress = RaftProgress::default();
        while node.has_ready() {
            let mut ready = node.ready();
            if !ready.snapshot().is_empty() {
                return Err(ShardError::InvalidRaftState(
                    "Raft snapshot installation belongs to the logical snapshot task".into(),
                ));
            }
            self.wal.persist_entries(ready.entries())?;
            if let Some(hard_state) = ready.hs() {
                self.wal.persist_hard_state(hard_state)?;
            }
            let mut committed = ready.take_committed_entries();
            progress.messages.extend(ready.take_messages());
            progress.messages.extend(ready.take_persisted_messages());

            let mut light = node.advance_append(ready);
            if light.commit_index().is_some() {
                self.wal.persist_hard_state(&node.status().hs)?;
            }
            committed.extend(light.take_committed_entries());
            progress.messages.extend(light.take_messages());

            let (mut receipts, outcomes) = self.apply_entries(committed)?;
            progress.receipts.append(&mut receipts);
            if let Some(last) = outcomes.last() {
                node.advance_apply_to(last.applied_index());
            }
        }
        Ok(progress)
    }

    fn apply_entries(
        &mut self,
        entries: Vec<Entry>,
    ) -> Result<(Vec<ProposalReceipt>, Vec<ApplyOutcome>), ShardError> {
        let mut receipts = Vec::new();
        let mut outcomes = Vec::with_capacity(entries.len());
        for entry in entries {
            if entry.get_entry_type() != EntryType::EntryNormal {
                return Err(ShardError::InvalidRaftState(
                    "membership changes are outside the Task 7 runtime".into(),
                ));
            }
            if entry.data.is_empty() {
                outcomes.push(self.machine.apply_raft_noop(entry.term, entry.index)?);
                continue;
            }
            let command = ShardCommand::decode(&entry.data)?;
            let command_id = command.header().command_id().get();
            if entry.context.as_slice() != command_id.to_be_bytes() {
                return Err(ShardError::InvalidRaftState(
                    "committed entry context does not match command identifier".into(),
                ));
            }
            let outcome = self
                .machine
                .apply_committed(entry.term, entry.index, command)?;
            receipts.push(ProposalReceipt {
                term: entry.term,
                index: entry.index,
                command_id,
                digest: outcome.digest(),
                replayed: outcome.replayed(),
            });
            outcomes.push(outcome);
        }
        Ok((receipts, outcomes))
    }
}

fn same_replica_identity(consensus: &ReplicaBinding, business: &ReplicaBinding) -> bool {
    consensus.cluster_id() == business.cluster_id()
        && consensus.graph_id() == business.graph_id()
        && consensus.shard_id() == business.shard_id()
        && consensus.placement_epoch() == business.placement_epoch()
        && consensus.replica_id() == business.replica_id()
        && consensus.backend_generation() == business.backend_generation()
}
