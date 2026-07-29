use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use dtg_kernel::Value;
use dtg_kernel::{
    BackendGeneration, Digest32, PlacementEpoch, ShardId, TransactionId, TransactionTime,
};
use dtg_storage::{ChangeRecord, CommandId, LogicalMutation, Properties, TransactionRecord};

use crate::TxnError;

pub type TxnFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, TxnError>> + Send + 'a>>;
pub type SubmissionFuture<'a> =
    Pin<Box<dyn Future<Output = Result<SubmissionReceipt, SubmissionFailure>> + Send + 'a>>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SubmissionFailure {
    Definitive(TxnError),
    Ambiguous(TxnError),
}

impl SubmissionFailure {
    pub const fn error(&self) -> &TxnError {
        match self {
            Self::Definitive(error) | Self::Ambiguous(error) => error,
        }
    }

    pub fn into_error(self) -> TxnError {
        match self {
            Self::Definitive(error) | Self::Ambiguous(error) => error,
        }
    }
}

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
        transaction_id: TransactionId,
        start_time: TransactionTime,
        snapshot_applied_index: u64,
        mutations: Vec<LogicalMutation>,
    },
    PrewriteIntent {
        header: ShardRequestHeader,
        transaction_id: TransactionId,
        start_time: TransactionTime,
        snapshot_applied_index: u64,
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
        snapshot_applied_index: u64,
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

    pub fn digest(&self) -> Digest32 {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"dtg-transaction-shard-request-v1");
        let header = self.header();
        hasher.update(&header.command_id().get().to_be_bytes());
        hasher.update(&header.placement_epoch().get().to_be_bytes());
        hasher.update(&header.backend_generation().get().to_be_bytes());
        match self {
            Self::CommitSingleShard {
                transaction_id,
                start_time,
                snapshot_applied_index,
                mutations,
                ..
            } => {
                hasher.update(&[1]);
                hasher.update(&transaction_id.get().to_be_bytes());
                hasher.update(&start_time.get().to_be_bytes());
                hasher.update(&snapshot_applied_index.to_be_bytes());
                hash_mutations(&mut hasher, mutations);
            }
            Self::PrewriteIntent {
                transaction_id,
                start_time,
                snapshot_applied_index,
                mutations,
                ..
            } => {
                hasher.update(&[2]);
                hasher.update(&transaction_id.get().to_be_bytes());
                hasher.update(&start_time.get().to_be_bytes());
                hasher.update(&snapshot_applied_index.to_be_bytes());
                hash_mutations(&mut hasher, mutations);
            }
            Self::RecordHomeDecision { decision, .. } => {
                hasher.update(&[3]);
                hash_transaction(&mut hasher, decision);
            }
            Self::FinalizeParticipantCommit {
                transaction_id,
                start_time,
                snapshot_applied_index,
                commit_time,
                intent_digest,
                mutations,
                ..
            } => {
                hasher.update(&[4]);
                hasher.update(&transaction_id.get().to_be_bytes());
                hasher.update(&start_time.get().to_be_bytes());
                hasher.update(&snapshot_applied_index.to_be_bytes());
                hasher.update(&commit_time.get().to_be_bytes());
                hasher.update(&intent_digest.get());
                hash_mutations(&mut hasher, mutations);
            }
            Self::FinalizeParticipantAbort { terminal, .. } => {
                hasher.update(&[5]);
                hash_transaction(&mut hasher, terminal);
            }
        }
        Digest32::new(*hasher.finalize().as_bytes())
    }
}

fn hash_mutations(hasher: &mut blake3::Hasher, mutations: &[LogicalMutation]) {
    hasher.update(&(mutations.len() as u64).to_be_bytes());
    for mutation in mutations {
        match mutation {
            LogicalMutation::PutVertex(vertex) => {
                hasher.update(&[1]);
                hasher.update(&vertex.id().get().to_be_bytes());
                hasher.update(&vertex.version().get().to_be_bytes());
                hash_interval(hasher, vertex.valid_time());
                hasher.update(&vertex.transaction_time().get().to_be_bytes());
                hash_properties(hasher, vertex.properties());
            }
            LogicalMutation::DeleteVertex(vertex) => {
                hasher.update(&[2]);
                hasher.update(&vertex.id().get().to_be_bytes());
                hasher.update(&vertex.version().get().to_be_bytes());
                hasher.update(&vertex.transaction_time().get().to_be_bytes());
            }
            LogicalMutation::PutEdge(edge) => {
                hasher.update(&[3]);
                hasher.update(&edge.id().get().to_be_bytes());
                hasher.update(&edge.source().get().to_be_bytes());
                hasher.update(&edge.target().get().to_be_bytes());
                hash_bytes(hasher, edge.edge_type().as_bytes());
                hasher.update(&edge.version().get().to_be_bytes());
                hash_interval(hasher, edge.valid_time());
                hasher.update(&edge.transaction_time().get().to_be_bytes());
                hash_properties(hasher, edge.properties());
            }
            LogicalMutation::DeleteEdge(edge) => {
                hasher.update(&[4]);
                hasher.update(&edge.id().get().to_be_bytes());
                hasher.update(&edge.version().get().to_be_bytes());
                hasher.update(&edge.transaction_time().get().to_be_bytes());
            }
            LogicalMutation::PutTransaction(transaction) => {
                hasher.update(&[5]);
                hash_transaction(hasher, transaction);
            }
            LogicalMutation::PutReplicaMetadata(metadata) => {
                hasher.update(&[6]);
                hash_bytes(hasher, metadata.name().as_bytes());
                hash_value(hasher, metadata.value());
            }
        }
    }
}

fn hash_transaction(hasher: &mut blake3::Hasher, transaction: &TransactionRecord) {
    hasher.update(&transaction.id().get().to_be_bytes());
    hasher.update(&[match transaction.state() {
        dtg_storage::TransactionState::Prepared => 1,
        dtg_storage::TransactionState::Committed => 2,
        dtg_storage::TransactionState::Aborted => 3,
    }]);
    hasher.update(&transaction.transaction_time().get().to_be_bytes());
    hasher.update(&transaction.record_digest().get());
}

fn hash_interval(hasher: &mut blake3::Hasher, interval: dtg_kernel::ValidInterval) {
    hasher.update(&interval.start().to_be_bytes());
    hasher.update(&interval.end().to_be_bytes());
}

fn hash_properties(hasher: &mut blake3::Hasher, properties: &Properties) {
    hasher.update(&(properties.len() as u64).to_be_bytes());
    for (key, value) in properties {
        hash_bytes(hasher, key.as_bytes());
        hash_value(hasher, value);
    }
}

fn hash_value(hasher: &mut blake3::Hasher, value: &Value) {
    match value {
        Value::Null => {
            hasher.update(&[0]);
        }
        Value::Boolean(value) => {
            hasher.update(&[1, u8::from(*value)]);
        }
        Value::Integer(value) => {
            hasher.update(&[2]);
            hasher.update(&value.to_be_bytes());
        }
        Value::FloatBits(value) => {
            hasher.update(&[3]);
            hasher.update(&value.to_be_bytes());
        }
        Value::Bytes(value) => {
            hasher.update(&[4]);
            hash_bytes(hasher, value);
        }
        Value::String(value) => {
            hasher.update(&[5]);
            hash_bytes(hasher, value.as_bytes());
        }
        Value::List(values) => {
            hasher.update(&[6]);
            hasher.update(&(values.len() as u64).to_be_bytes());
            for value in values {
                hash_value(hasher, value);
            }
        }
        Value::Map(values) => {
            hasher.update(&[7]);
            hasher.update(&(values.len() as u64).to_be_bytes());
            for (key, value) in values {
                hash_bytes(hasher, key.as_bytes());
                hash_value(hasher, value);
            }
        }
    }
}

fn hash_bytes(hasher: &mut blake3::Hasher, bytes: &[u8]) {
    hasher.update(&(bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
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
    snapshot_applied_index: u64,
    mutations: Vec<LogicalMutation>,
    digest: Digest32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecoveredSingleShardCommit {
    command_id: CommandId,
    transaction_id: TransactionId,
    start_time: TransactionTime,
    snapshot_applied_index: u64,
    commit_time: TransactionTime,
    request_digest: Digest32,
}

impl RecoveredSingleShardCommit {
    pub const fn new(
        command_id: CommandId,
        transaction_id: TransactionId,
        start_time: TransactionTime,
        snapshot_applied_index: u64,
        commit_time: TransactionTime,
        request_digest: Digest32,
    ) -> Self {
        Self {
            command_id,
            transaction_id,
            start_time,
            snapshot_applied_index,
            commit_time,
            request_digest,
        }
    }

    pub const fn command_id(self) -> CommandId {
        self.command_id
    }

    pub const fn transaction_id(self) -> TransactionId {
        self.transaction_id
    }

    pub const fn start_time(self) -> TransactionTime {
        self.start_time
    }

    pub const fn snapshot_applied_index(self) -> u64 {
        self.snapshot_applied_index
    }

    pub const fn commit_time(self) -> TransactionTime {
        self.commit_time
    }

    pub const fn request_digest(self) -> Digest32 {
        self.request_digest
    }
}

impl RecoveredParticipantIntent {
    pub fn new(
        shard_id: ShardId,
        start_time: TransactionTime,
        snapshot_applied_index: u64,
        mutations: Vec<LogicalMutation>,
        digest: Digest32,
    ) -> Self {
        Self {
            shard_id,
            start_time,
            snapshot_applied_index,
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

    pub const fn snapshot_applied_index(&self) -> u64 {
        self.snapshot_applied_index
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
    single_shard_commit: Option<RecoveredSingleShardCommit>,
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
            single_shard_commit: None,
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

    pub const fn single_shard_commit(&self) -> Option<RecoveredSingleShardCommit> {
        self.single_shard_commit
    }

    pub fn with_single_shard_commit(mut self, commit: RecoveredSingleShardCommit) -> Self {
        self.single_shard_commit = Some(commit);
        self
    }
}

pub trait ShardCommandExecutor: Send + Sync {
    fn submit(&self, shard_id: ShardId, request: ShardRequest) -> SubmissionFuture<'_>;

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

    pub fn submit(&self, shard_id: ShardId, request: ShardRequest) -> SubmissionFuture<'_> {
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
