use std::{collections::HashMap, sync::Arc};

use dtg_kernel::{Digest32, ReplicaId, TransactionTime};
use dtg_storage::{BindingRole, ConsensusStore, ProviderKind, ReplicaBinding, ReplicaStateStore};
use raft::eraftpb::{Entry, EntryType, Message};
use raft::{Config, RawNode, StateRole, Storage};
use slog::{Logger, o};

use crate::{
    ApplyOutcome, ApplyRejection, FollowerReadProof, FollowerReadProofAuthority, RaftStore,
    ReadError, ReadFailure, ReadPermit, ShardCommand, ShardError, ShardStateMachine,
};

const MAX_PENDING_READ_INDEX_REQUESTS: usize = 1_024;

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
    rejection: Option<ApplyRejection>,
    active_binding: ReplicaBinding,
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

    pub const fn rejection(&self) -> Option<ApplyRejection> {
        self.rejection
    }

    pub const fn active_binding(&self) -> &ReplicaBinding {
        &self.active_binding
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
    read_permits: Vec<ReadPermit>,
    read_failures: Vec<ReadFailure>,
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

    pub fn read_permits(&self) -> &[ReadPermit] {
        &self.read_permits
    }

    pub fn read_failures(&self) -> &[ReadFailure] {
        &self.read_failures
    }
}

#[derive(Clone, Debug)]
struct PendingReadIndex {
    request_id: u128,
    leader_id: u64,
    leader_term: u64,
    binding_digest: Digest32,
    placement_epoch: u64,
    backend_generation: u64,
    read_index: Option<u64>,
    active: bool,
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
    pending_apply: Vec<Entry>,
    pending_messages: Vec<Message>,
    pending_reads: HashMap<Vec<u8>, PendingReadIndex>,
}

impl RaftReplica {
    pub fn open(
        consensus_store: Arc<dyn ConsensusStore>,
        state_store: Arc<dyn ReplicaStateStore>,
    ) -> Result<Self, ShardError> {
        if consensus_store.binding().role() != BindingRole::Active
            || state_store.binding().role() != BindingRole::Active
            || !same_replica_identity(consensus_store.binding(), state_store.binding())
        {
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
            pending_apply: Vec::new(),
            pending_messages: Vec::new(),
            pending_reads: HashMap::new(),
        })
    }

    pub fn stage_migration_state_store(
        &mut self,
        state_store: Arc<dyn ReplicaStateStore>,
    ) -> Result<(), ShardError> {
        self.machine.stage_migration_state_store(state_store)
    }

    pub fn recover(&mut self) -> Result<Vec<ApplyOutcome>, ShardError> {
        if matches!(
            self.lifecycle,
            ReplicaLifecycle::Running | ReplicaLifecycle::Sealed
        ) {
            return Err(ShardError::InvalidLifecycle(
                "recovery requires a non-running, unsealed replica".into(),
            ));
        }
        let previously_applied = self.machine.applied_index();
        self.pending_apply = self.wal.recovery_entries(previously_applied)?;
        let (_, outcomes) = self.apply_entries(self.pending_apply.clone())?;
        self.pending_apply.clear();
        self.pending_messages.clear();
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

    pub fn request_linearizable_read(&mut self, request_id: u128) -> Result<(), ReadError> {
        if request_id == 0 {
            return Err(ReadError::InvalidRequest);
        }
        if self.lifecycle != ReplicaLifecycle::Running {
            return Err(ReadError::NotReady);
        }
        let node = self.node.as_mut().ok_or(ReadError::NotReady)?;
        let status = node.status();
        if status.ss.raft_state != StateRole::Leader
            || status.ss.leader_id != self.binding.replica_id().get()
        {
            return Err(ReadError::NotLeader);
        }
        if !node.raft.commit_to_current_term() {
            return Err(ReadError::NotReady);
        }
        let context = read_index_context(request_id);
        if self.pending_reads.contains_key(&context) {
            return Err(ReadError::DuplicateRequest);
        }
        if self.pending_reads.len() >= MAX_PENDING_READ_INDEX_REQUESTS {
            return Err(ReadError::Overloaded);
        }
        self.pending_reads.insert(
            context.clone(),
            PendingReadIndex {
                request_id,
                leader_id: status.ss.leader_id,
                leader_term: status.hs.term,
                binding_digest: self.binding.identity_digest(),
                placement_epoch: self.binding.placement_epoch().get(),
                backend_generation: self.binding.backend_generation().get(),
                read_index: None,
                active: true,
            },
        );
        node.read_index(context);
        Ok(())
    }

    pub fn cancel_linearizable_read(&mut self, request_id: u128) -> bool {
        let Some(pending) = self
            .pending_reads
            .values_mut()
            .find(|pending| pending.request_id == request_id)
        else {
            return false;
        };
        pending.active = false;
        true
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
        if self.node.as_ref().is_some_and(RawNode::has_ready) {
            return Err(ShardError::InvalidLifecycle(
                "replica has unprocessed Ready work".into(),
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

    pub fn follower_read_permit(
        &self,
        authority: &FollowerReadProofAuthority,
        proof: &FollowerReadProof,
        requested_time: TransactionTime,
    ) -> Result<ReadPermit, ReadError> {
        authority.verify(proof)?;
        let leader = proof.leader_binding();
        if leader.cluster_id() != self.binding.cluster_id()
            || leader.graph_id() != self.binding.graph_id()
            || leader.shard_id() != self.binding.shard_id()
            || leader.backend_class_digest() != self.binding.backend_class_digest()
            || leader.provider_kind() != self.binding.provider_kind()
            || leader.contract_version() != self.binding.contract_version()
            || leader.layout_version() != self.binding.layout_version()
            || leader.capability_digest() != self.binding.capability_digest()
        {
            return Err(ReadError::UnsafeFollowerRead);
        }
        if proof.placement_epoch() != self.binding.placement_epoch().get() {
            return Err(ReadError::StalePlacementEpoch);
        }
        if proof.backend_generation() != self.binding.backend_generation().get() {
            return Err(ReadError::StaleBackendGeneration);
        }
        if self.lifecycle != ReplicaLifecycle::Running {
            return Err(ReadError::NotReady);
        }
        let node = self.node.as_ref().ok_or(ReadError::NotReady)?;
        let status = node.status();
        if status.ss.raft_state == StateRole::Leader
            || status.ss.leader_id != proof.leader_replica().get()
            || status.hs.term != proof.leader_term()
        {
            return Err(ReadError::NotReady);
        }
        if self.machine.applied_index() < proof.applied_index() {
            return Err(ReadError::AdapterLagging);
        }
        let closed_timestamp = self.machine.closed_timestamp().ok_or(ReadError::NotReady)?;
        if closed_timestamp < proof.closed_timestamp() {
            return Err(ReadError::NotReady);
        }
        if requested_time > proof.closed_timestamp() || requested_time > closed_timestamp {
            return Err(ReadError::UnsafeFollowerRead);
        }
        Ok(ReadPermit::new(
            None,
            crate::ReadMode::Follower { requested_time },
            dtg_storage::ReadFence::new(self.binding.clone(), self.machine.applied_index()),
            Some(proof.leader_replica()),
            proof.leader_term(),
            Some(closed_timestamp),
        ))
    }

    pub fn snapshot_read_permit(&self, applied_index: u64) -> Result<ReadPermit, ReadError> {
        if self.lifecycle != ReplicaLifecycle::Running {
            return Err(ReadError::NotReady);
        }
        let local_applied = self.machine.applied_index();
        if local_applied < applied_index {
            return Err(ReadError::AdapterLagging);
        }
        if local_applied > applied_index {
            return Err(ReadError::SnapshotTooOld);
        }
        let node = self.node.as_ref().ok_or(ReadError::NotReady)?;
        let status = node.status();
        let leader_replica = ReplicaId::new(status.ss.leader_id).ok();
        Ok(ReadPermit::new(
            None,
            crate::ReadMode::Snapshot,
            dtg_storage::ReadFence::new(self.binding.clone(), applied_index),
            leader_replica,
            status.hs.term,
            self.machine.closed_timestamp(),
        ))
    }

    pub fn seal(&mut self) -> Result<(), ShardError> {
        if self.lifecycle == ReplicaLifecycle::Running {
            return Err(ShardError::InvalidLifecycle(
                "running replica must be stopped before sealing".into(),
            ));
        }
        if !self.pending_apply.is_empty() || !self.pending_messages.is_empty() {
            return Err(ShardError::InvalidLifecycle(
                "replica has pending committed apply or outbound messages".into(),
            ));
        }
        let committed_index = self.wal.committed_index()?;
        if committed_index > self.machine.applied_index() {
            return Err(ShardError::InvalidLifecycle(
                "replica has committed WAL entries that are not applied".into(),
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
        if !node.has_ready() {
            self.retry_pending(node, &mut progress)?;
        }
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
            for read_state in ready.take_read_states() {
                if let Some(pending) = self.pending_reads.get_mut(&read_state.request_ctx) {
                    pending.read_index = Some(read_state.index);
                }
            }
            self.pending_messages.extend(ready.take_messages());
            self.pending_messages
                .extend(ready.take_persisted_messages());

            let mut light = node.advance_append(ready);
            if light.commit_index().is_some() {
                self.wal.persist_hard_state(&node.status().hs)?;
            }
            committed.extend(light.take_committed_entries());
            self.pending_messages.extend(light.take_messages());
            self.pending_apply.extend(committed);
            self.retry_pending(node, &mut progress)?;
        }
        Ok(progress)
    }

    fn retry_pending(
        &mut self,
        node: &mut RawNode<RaftStore>,
        progress: &mut RaftProgress,
    ) -> Result<(), ShardError> {
        if !self.pending_apply.is_empty() {
            let (mut receipts, outcomes) = self.apply_entries(self.pending_apply.clone())?;
            if let Some(last) = outcomes.last() {
                node.advance_apply_to(last.applied_index());
            }
            self.pending_apply.clear();
            progress.receipts.append(&mut receipts);
        }
        self.resolve_pending_reads(node, progress)?;
        progress.messages.append(&mut self.pending_messages);
        Ok(())
    }

    fn resolve_pending_reads(
        &mut self,
        node: &RawNode<RaftStore>,
        progress: &mut RaftProgress,
    ) -> Result<(), ShardError> {
        let status = node.status();
        let applied_index = self.machine.applied_index();
        let binding_digest = self.binding.identity_digest();
        let placement_epoch = self.binding.placement_epoch().get();
        let backend_generation = self.binding.backend_generation().get();
        let mut completed = Vec::new();
        for (context, pending) in &self.pending_reads {
            let leadership_error = if status.ss.raft_state != StateRole::Leader
                || status.ss.leader_id != pending.leader_id
            {
                Some(ReadError::NotLeader)
            } else if status.hs.term != pending.leader_term
                || !node.raft.commit_to_current_term()
                || binding_digest != pending.binding_digest
                || placement_epoch != pending.placement_epoch
                || backend_generation != pending.backend_generation
            {
                Some(ReadError::NotReady)
            } else {
                None
            };
            if let Some(error) = leadership_error {
                if pending.active {
                    progress
                        .read_failures
                        .push(ReadFailure::new(pending.request_id, error));
                }
                completed.push(context.clone());
                continue;
            }
            let Some(read_index) = pending.read_index else {
                continue;
            };
            if !pending.active {
                completed.push(context.clone());
                continue;
            }
            if applied_index < read_index {
                continue;
            }
            if node.store().term(read_index).map_err(ShardError::Raft)? != pending.leader_term {
                progress
                    .read_failures
                    .push(ReadFailure::new(pending.request_id, ReadError::NotReady));
                completed.push(context.clone());
                continue;
            }
            let leader_replica = ReplicaId::new(pending.leader_id).map_err(|error| {
                ShardError::InvalidRaftState(format!("invalid ReadIndex leader: {error}"))
            })?;
            progress.read_permits.push(ReadPermit::new(
                Some(pending.request_id),
                crate::ReadMode::Linearizable,
                dtg_storage::ReadFence::new(self.binding.clone(), read_index),
                Some(leader_replica),
                pending.leader_term,
                self.machine.closed_timestamp(),
            ));
            completed.push(context.clone());
        }
        for context in completed {
            self.pending_reads.remove(&context);
        }
        Ok(())
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
            self.binding = outcome.active_binding().clone();
            receipts.push(ProposalReceipt {
                term: entry.term,
                index: entry.index,
                command_id,
                digest: outcome.digest(),
                replayed: outcome.replayed(),
                rejection: outcome.rejection(),
                active_binding: outcome.active_binding().clone(),
            });
            outcomes.push(outcome);
        }
        Ok((receipts, outcomes))
    }
}

fn read_index_context(request_id: u128) -> Vec<u8> {
    let mut context = Vec::with_capacity(20);
    context.extend_from_slice(&1_u32.to_be_bytes());
    context.extend_from_slice(&request_id.to_be_bytes());
    context
}

fn same_replica_identity(consensus: &ReplicaBinding, business: &ReplicaBinding) -> bool {
    consensus.cluster_id() == business.cluster_id()
        && consensus.graph_id() == business.graph_id()
        && consensus.shard_id() == business.shard_id()
        && consensus.replica_id() == business.replica_id()
}
