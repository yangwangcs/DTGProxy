use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use dtg_kernel::{
    BackendGeneration, Digest32, PlacementEpoch, ShardId, TransactionId, TransactionTime,
};
use dtg_storage::{ChangeRecord, CommandId, LogicalMutation, TransactionRecord};

use crate::TxnError;

pub type TxnFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, TxnError>> + Send + 'a>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShardRequestHeader {
    command_id: CommandId,
    placement_epoch: PlacementEpoch,
    backend_generation: BackendGeneration,
}

impl ShardRequestHeader {
    pub const fn new(
        command_id: CommandId,
        placement_epoch: PlacementEpoch,
        backend_generation: BackendGeneration,
    ) -> Self {
        Self {
            command_id,
            placement_epoch,
            backend_generation,
        }
    }

    pub const fn command_id(self) -> CommandId {
        self.command_id
    }

    pub const fn placement_epoch(self) -> PlacementEpoch {
        self.placement_epoch
    }

    pub const fn backend_generation(self) -> BackendGeneration {
        self.backend_generation
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ShardRequest {
    CommitSingleShard {
        header: ShardRequestHeader,
        mutations: Vec<LogicalMutation>,
    },
    PrewriteIntent {
        header: ShardRequestHeader,
        transaction_id: TransactionId,
        start_time: TransactionTime,
        mutations: Vec<LogicalMutation>,
    },
    RecordHomeDecision {
        header: ShardRequestHeader,
        decision: TransactionRecord,
    },
    FinalizeParticipantCommit {
        header: ShardRequestHeader,
        transaction_id: TransactionId,
        start_time: TransactionTime,
        commit_time: TransactionTime,
        intent_digest: Digest32,
        mutations: Vec<LogicalMutation>,
    },
    FinalizeParticipantAbort {
        header: ShardRequestHeader,
        terminal: TransactionRecord,
    },
}

impl ShardRequest {
    pub const fn header(&self) -> ShardRequestHeader {
        match self {
            Self::CommitSingleShard { header, .. }
            | Self::PrewriteIntent { header, .. }
            | Self::RecordHomeDecision { header, .. }
            | Self::FinalizeParticipantCommit { header, .. }
            | Self::FinalizeParticipantAbort { header, .. } => *header,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SubmissionReceipt {
    applied_index: u64,
    replayed: bool,
    intent_digest: Option<Digest32>,
}

impl SubmissionReceipt {
    pub const fn new(applied_index: u64, replayed: bool) -> Self {
        Self {
            applied_index,
            replayed,
            intent_digest: None,
        }
    }

    pub const fn prepared(applied_index: u64, replayed: bool, intent_digest: Digest32) -> Self {
        Self {
            applied_index,
            replayed,
            intent_digest: Some(intent_digest),
        }
    }

    pub const fn applied_index(self) -> u64 {
        self.applied_index
    }

    pub const fn replayed(self) -> bool {
        self.replayed
    }

    pub const fn intent_digest(self) -> Option<Digest32> {
        self.intent_digest
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveredParticipantIntent {
    shard_id: ShardId,
    start_time: TransactionTime,
    mutations: Vec<LogicalMutation>,
    digest: Digest32,
}

impl RecoveredParticipantIntent {
    pub fn new(
        shard_id: ShardId,
        start_time: TransactionTime,
        mutations: Vec<LogicalMutation>,
        digest: Digest32,
    ) -> Self {
        Self {
            shard_id,
            start_time,
            mutations,
            digest,
        }
    }

    pub const fn shard_id(&self) -> ShardId {
        self.shard_id
    }

    pub const fn start_time(&self) -> TransactionTime {
        self.start_time
    }

    pub fn mutations(&self) -> &[LogicalMutation] {
        &self.mutations
    }

    pub const fn digest(&self) -> Digest32 {
        self.digest
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TransactionHistory {
    prepared: Option<TransactionRecord>,
    intent: Option<RecoveredParticipantIntent>,
    terminal: Vec<TransactionRecord>,
}

impl TransactionHistory {
    pub fn new(
        prepared: Option<TransactionRecord>,
        intent: Option<RecoveredParticipantIntent>,
        terminal: Vec<TransactionRecord>,
    ) -> Self {
        Self {
            prepared,
            intent,
            terminal,
        }
    }

    pub const fn prepared(&self) -> Option<&TransactionRecord> {
        self.prepared.as_ref()
    }

    pub const fn intent(&self) -> Option<&RecoveredParticipantIntent> {
        self.intent.as_ref()
    }

    pub fn terminal(&self) -> &[TransactionRecord] {
        &self.terminal
    }
}

pub trait ShardCommandExecutor: Send + Sync {
    fn submit(&self, shard_id: ShardId, request: ShardRequest) -> TxnFuture<'_, SubmissionReceipt>;

    fn changes_after(
        &self,
        shard_id: ShardId,
        applied_index: u64,
    ) -> TxnFuture<'_, Vec<ChangeRecord>>;

    fn transaction_history(
        &self,
        shard_id: ShardId,
        transaction_id: TransactionId,
    ) -> TxnFuture<'_, TransactionHistory>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParticipantWrite {
    shard_id: ShardId,
    mutations: Vec<LogicalMutation>,
}

impl ParticipantWrite {
    pub fn new(shard_id: ShardId, mutations: Vec<LogicalMutation>) -> Result<Self, TxnError> {
        if mutations.is_empty()
            || mutations.iter().any(|mutation| {
                matches!(
                    mutation,
                    LogicalMutation::PutTransaction(_) | LogicalMutation::PutReplicaMetadata(_)
                )
            })
        {
            return Err(TxnError::InvalidMutation);
        }
        Ok(Self {
            shard_id,
            mutations,
        })
    }

    pub const fn shard_id(&self) -> ShardId {
        self.shard_id
    }

    pub fn mutations(&self) -> &[LogicalMutation] {
        &self.mutations
    }
}

#[derive(Clone)]
pub struct ParticipantService {
    executor: Arc<dyn ShardCommandExecutor>,
}

impl ParticipantService {
    pub fn new(executor: Arc<dyn ShardCommandExecutor>) -> Self {
        Self { executor }
    }

    pub fn submit(
        &self,
        shard_id: ShardId,
        request: ShardRequest,
    ) -> TxnFuture<'_, SubmissionReceipt> {
        self.executor.submit(shard_id, request)
    }

    pub fn changes_after(
        &self,
        shard_id: ShardId,
        applied_index: u64,
    ) -> TxnFuture<'_, Vec<ChangeRecord>> {
        self.executor.changes_after(shard_id, applied_index)
    }

    pub fn transaction_history(
        &self,
        shard_id: ShardId,
        transaction_id: TransactionId,
    ) -> TxnFuture<'_, TransactionHistory> {
        self.executor.transaction_history(shard_id, transaction_id)
    }
}
