use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use dtg_kernel::{Digest32, KernelError, TransactionTime};
use dtg_storage::{
    CommandId, CommittedShardBatch, LogicalMutation, ReplicaBinding, ReplicaMetadata,
    ReplicaStateStore, StorageError, Value,
};

use crate::ShardCommand;

struct ThreadWaker(std::thread::Thread);

impl Wake for ThreadWaker {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

pub(crate) fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::from(Arc::new(ThreadWaker(std::thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => std::thread::park(),
        }
    }
}

#[derive(Debug)]
pub enum ShardError {
    InvalidCommand(String),
    UnsupportedCommandVersion(u32),
    InvalidRaftState(String),
    SnapshotStateMissing {
        snapshot_index: u64,
        applied_index: u64,
    },
    InvalidLifecycle(String),
    DuplicateReplica,
    HeterogeneousGeneration,
    ReplicaNotFound,
    Raft(raft::Error),
    StalePlacementEpoch {
        expected: u64,
        actual: u64,
    },
    StaleBackendGeneration {
        expected: u64,
        actual: u64,
    },
    BindingMismatch,
    Storage(StorageError),
}

impl ShardError {
    pub const fn code(&self) -> &'static str {
        match self {
            Self::InvalidCommand(_) => "DTG-SHARD-COMMAND",
            Self::UnsupportedCommandVersion(_) => "DTG-SHARD-COMMAND-VERSION",
            Self::InvalidRaftState(_) => "DTG-SHARD-RAFT-STATE",
            Self::SnapshotStateMissing { .. } => "DTG-SHARD-SNAPSHOT-STATE",
            Self::InvalidLifecycle(_) => "DTG-SHARD-LIFECYCLE",
            Self::DuplicateReplica => "DTG-SHARD-DUPLICATE-REPLICA",
            Self::HeterogeneousGeneration => "DTG-SHARD-HETEROGENEOUS-GENERATION",
            Self::ReplicaNotFound => "DTG-SHARD-REPLICA-NOT-FOUND",
            Self::Raft(_) => "DTG-SHARD-RAFT",
            Self::StalePlacementEpoch { .. } => "DTG-SHARD-STALE-EPOCH",
            Self::StaleBackendGeneration { .. } => "DTG-SHARD-STALE-GENERATION",
            Self::BindingMismatch => "DTG-SHARD-BINDING",
            Self::Storage(error) => error.code(),
        }
    }
}

impl core::fmt::Display for ShardError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::InvalidCommand(message) => formatter.write_str(message),
            Self::UnsupportedCommandVersion(version) => {
                write!(formatter, "unsupported Shard command version {version}")
            }
            Self::InvalidRaftState(message) => formatter.write_str(message),
            Self::SnapshotStateMissing {
                snapshot_index,
                applied_index,
            } => write!(
                formatter,
                "snapshot index {snapshot_index} is ahead of business-store applied index {applied_index}"
            ),
            Self::InvalidLifecycle(message) => formatter.write_str(message),
            Self::DuplicateReplica => formatter.write_str("replica already exists on this host"),
            Self::HeterogeneousGeneration => formatter
                .write_str("replicas in one active Shard generation must share a backend class"),
            Self::ReplicaNotFound => formatter.write_str("replica is not hosted on this node"),
            Self::Raft(error) => error.fmt(formatter),
            Self::StalePlacementEpoch { expected, actual } => write!(
                formatter,
                "stale placement epoch: expected {expected}, got {actual}"
            ),
            Self::StaleBackendGeneration { expected, actual } => write!(
                formatter,
                "stale backend generation: expected {expected}, got {actual}"
            ),
            Self::BindingMismatch => {
                formatter.write_str("state store binding does not match replica binding")
            }
            Self::Storage(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for ShardError {}

impl From<StorageError> for ShardError {
    fn from(error: StorageError) -> Self {
        Self::Storage(error)
    }
}

impl From<KernelError> for ShardError {
    fn from(error: KernelError) -> Self {
        Self::InvalidCommand(error.to_string())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplyOutcome {
    digest: Digest32,
    applied_index: u64,
    replayed: bool,
}

impl ApplyOutcome {
    pub const fn digest(&self) -> Digest32 {
        self.digest
    }

    pub const fn applied_index(&self) -> u64 {
        self.applied_index
    }

    pub const fn replayed(&self) -> bool {
        self.replayed
    }
}

pub struct ShardStateMachine {
    binding: ReplicaBinding,
    state_store: Arc<dyn ReplicaStateStore>,
    applied_index: u64,
    closed_timestamp: Option<TransactionTime>,
}

impl ShardStateMachine {
    pub fn new(
        binding: ReplicaBinding,
        state_store: Arc<dyn ReplicaStateStore>,
    ) -> Result<Self, ShardError> {
        if state_store.binding() != &binding {
            return Err(ShardError::BindingMismatch);
        }
        let applied_index = block_on(state_store.applied_index())?;
        Ok(Self {
            binding,
            state_store,
            applied_index,
            closed_timestamp: None,
        })
    }

    pub fn apply_committed(
        &mut self,
        term: u64,
        index: u64,
        command: ShardCommand,
    ) -> Result<ApplyOutcome, ShardError> {
        self.validate_command(&command)?;
        let header = command.header();

        let next_closed_timestamp = match &command {
            ShardCommand::AdvanceClosedTimestamp(command) => Some(command.closed_timestamp()),
            _ => None,
        };

        let batch = CommittedShardBatch::new(
            self.binding.clone(),
            term,
            index,
            header.command_id(),
            command.mutations()?,
        )?;
        let receipt = block_on(self.state_store.apply(batch))?;
        self.applied_index = self.applied_index.max(receipt.raft_index());
        if let Some(closed_timestamp) = next_closed_timestamp {
            self.closed_timestamp = Some(closed_timestamp);
        }
        Ok(ApplyOutcome {
            digest: receipt.mutation_digest(),
            applied_index: receipt.raft_index(),
            replayed: receipt.replayed(),
        })
    }

    pub(crate) fn validate_command(&self, command: &ShardCommand) -> Result<(), ShardError> {
        let header = command.header();
        let expected_epoch = self.binding.placement_epoch().get();
        let actual_epoch = header.placement_epoch().get();
        if actual_epoch != expected_epoch {
            return Err(ShardError::StalePlacementEpoch {
                expected: expected_epoch,
                actual: actual_epoch,
            });
        }
        let expected_generation = self.binding.backend_generation().get();
        let actual_generation = header.backend_generation().get();
        if actual_generation != expected_generation {
            return Err(ShardError::StaleBackendGeneration {
                expected: expected_generation,
                actual: actual_generation,
            });
        }

        let intent = match command {
            ShardCommand::PrewriteIntent(command) => Some(command.intent()),
            ShardCommand::FinalizeParticipant(command) => command.intent(),
            _ => None,
        };
        if intent.is_some_and(|intent| intent.shard_id() != self.binding.shard_id()) {
            return Err(ShardError::InvalidCommand(
                "transaction intent belongs to a different Shard".into(),
            ));
        }

        if let ShardCommand::AdvanceClosedTimestamp(command) = command {
            let proposed = command.closed_timestamp();
            if self
                .closed_timestamp
                .is_some_and(|current| proposed < current)
            {
                return Err(ShardError::InvalidCommand(
                    "closed timestamp cannot move backwards".into(),
                ));
            }
        }
        command.mutations()?;
        Ok(())
    }

    pub(crate) fn apply_raft_noop(
        &mut self,
        term: u64,
        index: u64,
    ) -> Result<ApplyOutcome, ShardError> {
        let synthetic = (u128::from(term) << 64) | u128::from(index);
        let batch = CommittedShardBatch::new(
            self.binding.clone(),
            term,
            index,
            CommandId::new(synthetic)?,
            vec![LogicalMutation::PutReplicaMetadata(ReplicaMetadata::new(
                "dtg.raft_noop",
                Value::Bytes(index.to_be_bytes().to_vec()),
            )?)],
        )?;
        let receipt = block_on(self.state_store.apply(batch))?;
        self.applied_index = self.applied_index.max(receipt.raft_index());
        Ok(ApplyOutcome {
            digest: receipt.mutation_digest(),
            applied_index: receipt.raft_index(),
            replayed: receipt.replayed(),
        })
    }

    pub const fn binding(&self) -> &ReplicaBinding {
        &self.binding
    }

    pub const fn applied_index(&self) -> u64 {
        self.applied_index
    }

    pub const fn closed_timestamp(&self) -> Option<TransactionTime> {
        self.closed_timestamp
    }
}
