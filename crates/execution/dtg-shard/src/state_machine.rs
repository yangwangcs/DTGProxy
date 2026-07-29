use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use dtg_kernel::{Digest32, KernelError, TransactionId, TransactionTime, ValidInterval};
use dtg_storage::{
    ChangeRecord, ChangesRead, CommandId, CommittedShardBatch, EdgeId, LogicalMutation, ReadFence,
    ReplicaBinding, ReplicaMetadata, ReplicaStateStore, StorageError, TransactionRecord,
    TransactionState, Value, VertexId,
};

use crate::command::MAX_TRANSACTION_INTENT_ITEMS;
use crate::{ParticipantIntent, ShardCommand, TRANSACTION_INTENT_METADATA_NAME};

const HISTORY_PAGE_LIMIT: u32 = 256;
const HOME_DECISION_METADATA_NAME: &str = "dtg.transaction_home_decision.v1";
pub const SINGLE_SHARD_TRANSACTION_METADATA_NAME: &str = "dtg.single_shard_transaction.v1";

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
    WriteConflict,
    CorruptIntentHistory(String),
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
            Self::WriteConflict => "DTG-SHARD-WRITE-CONFLICT",
            Self::CorruptIntentHistory(_) => "DTG-SHARD-INTENT-HISTORY",
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
            Self::WriteConflict => formatter.write_str("temporal write conflict"),
            Self::CorruptIntentHistory(message) => formatter.write_str(message),
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
    active_intents: BTreeMap<TransactionId, ParticipantIntent>,
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
        let active_intents = rebuild_active_intents(&binding, &state_store, applied_index)?;
        Ok(Self {
            binding,
            state_store,
            applied_index,
            closed_timestamp: None,
            active_intents,
        })
    }

    pub fn apply_committed(
        &mut self,
        term: u64,
        index: u64,
        command: ShardCommand,
    ) -> Result<ApplyOutcome, ShardError> {
        self.validate_command(&command)?;
        let intent_transition = if index <= self.applied_index {
            IntentTransition::None
        } else {
            self.validate_transaction_acceptance(&command)?
        };
        let header = command.header();

        let next_closed_timestamp = match &command {
            ShardCommand::AdvanceClosedTimestamp(command) => Some(command.closed_timestamp()),
            _ => None,
        };

        let mut mutations = command.mutations()?;
        if let ShardCommand::RecordHomeDecision(command) = &command {
            let LogicalMutation::PutTransaction(decision) = &command.mutations()[0] else {
                unreachable!("validated Home decision contains one transaction record");
            };
            mutations.push(LogicalMutation::PutReplicaMetadata(home_decision_metadata(
                decision,
            )?));
        }
        if let ShardCommand::CommitSingleShardTransaction(command) = &command {
            mutations.push(LogicalMutation::PutReplicaMetadata(
                single_shard_transaction_metadata(command)?,
            ));
        }
        let batch = CommittedShardBatch::new(
            self.binding.clone(),
            term,
            index,
            header.command_id(),
            mutations,
        )?;
        let receipt = block_on(self.state_store.apply(batch))?;
        self.applied_index = self.applied_index.max(receipt.raft_index());
        if let Some(closed_timestamp) = next_closed_timestamp {
            self.closed_timestamp = Some(closed_timestamp);
        }
        if !receipt.replayed() {
            match intent_transition {
                IntentTransition::None => {}
                IntentTransition::Prepare(intent) => {
                    self.active_intents.insert(intent.transaction_id(), intent);
                }
                IntentTransition::Finalize(transaction_id) => {
                    self.active_intents.remove(&transaction_id);
                }
            }
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
            ShardCommand::CommitSingleShard(_)
            | ShardCommand::CommitSingleShardTransaction(_)
            | ShardCommand::RecordHomeDecision(_)
            | ShardCommand::AdvanceClosedTimestamp(_)
            | ShardCommand::InstallSnapshot(_)
            | ShardCommand::Migration(_) => None,
        };
        if intent.is_some_and(|intent| intent.shard_id() != self.binding.shard_id()) {
            return Err(ShardError::InvalidCommand(
                "transaction intent belongs to a different Shard".into(),
            ));
        }

        let snapshot_applied_index = match command {
            ShardCommand::CommitSingleShardTransaction(command) => {
                Some(command.snapshot_applied_index())
            }
            ShardCommand::PrewriteIntent(command) => {
                Some(command.intent().snapshot_applied_index())
            }
            ShardCommand::FinalizeParticipant(command) => command
                .intent()
                .map(ParticipantIntent::snapshot_applied_index),
            ShardCommand::CommitSingleShard(_)
            | ShardCommand::RecordHomeDecision(_)
            | ShardCommand::AdvanceClosedTimestamp(_)
            | ShardCommand::InstallSnapshot(_)
            | ShardCommand::Migration(_) => None,
        };
        if snapshot_applied_index.is_some_and(|snapshot| snapshot > self.applied_index) {
            return Err(ShardError::InvalidCommand(
                "transaction snapshot applied index is ahead of the Shard state machine".into(),
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

    fn validate_transaction_acceptance(
        &self,
        command: &ShardCommand,
    ) -> Result<IntentTransition, ShardError> {
        match command {
            ShardCommand::CommitSingleShardTransaction(command) => {
                if self.single_shard_transaction_is_durable(command)? {
                    return Err(ShardError::InvalidCommand(
                        "single-Shard transaction command is already durably applied".into(),
                    ));
                }
                self.validate_write_conflicts(
                    command.transaction_id(),
                    command.start_time(),
                    command.mutations(),
                )?;
                Ok(IntentTransition::None)
            }
            ShardCommand::CommitSingleShard(_) => Ok(IntentTransition::None),
            ShardCommand::PrewriteIntent(command) => {
                let intent = command.intent();
                if let Some(existing) = self.active_intents.get(&intent.transaction_id()) {
                    return if existing == intent {
                        Ok(IntentTransition::None)
                    } else {
                        Err(corrupt_intent_history(
                            "one transaction has contradictory active participant intents",
                        ))
                    };
                }
                self.validate_write_conflicts(
                    intent.transaction_id(),
                    intent.start_time(),
                    intent.mutations(),
                )?;
                Ok(IntentTransition::Prepare(intent.clone()))
            }
            ShardCommand::FinalizeParticipant(command) => {
                let transaction_id = command.terminal().id();
                if let Some(active) = self.active_intents.get(&transaction_id)
                    && (active.digest() != command.terminal().record_digest()
                        || command.intent().is_some_and(|intent| intent != active))
                {
                    return Err(corrupt_intent_history(
                        "participant finalization contradicts the active durable intent",
                    ));
                }
                Ok(IntentTransition::Finalize(transaction_id))
            }
            ShardCommand::RecordHomeDecision(_)
            | ShardCommand::AdvanceClosedTimestamp(_)
            | ShardCommand::InstallSnapshot(_)
            | ShardCommand::Migration(_) => Ok(IntentTransition::None),
        }
    }

    fn validate_write_conflicts(
        &self,
        transaction_id: TransactionId,
        start_time: TransactionTime,
        candidate: &[LogicalMutation],
    ) -> Result<(), ShardError> {
        for intent in self.active_intents.values() {
            if intent.transaction_id() != transaction_id
                && mutations_conflict(candidate, intent.mutations())
            {
                return Err(ShardError::WriteConflict);
            }
        }
        for_each_change(
            &self.binding,
            &self.state_store,
            self.applied_index,
            |change| {
                let mutation = change.mutation();
                if graph_mutation_time(mutation).is_some_and(|time| time > start_time)
                    && mutations_conflict(candidate, core::slice::from_ref(mutation))
                {
                    return Err(ShardError::WriteConflict);
                }
                Ok(())
            },
        )
    }

    fn single_shard_transaction_is_durable(
        &self,
        command: &crate::CommitSingleShardTransaction,
    ) -> Result<bool, ShardError> {
        let expected = single_shard_transaction_metadata(command)?;
        let mut durable = false;
        for_each_change(
            &self.binding,
            &self.state_store,
            self.applied_index,
            |change| {
                let LogicalMutation::PutReplicaMetadata(metadata) = change.mutation() else {
                    return Ok(());
                };
                if metadata.name() != SINGLE_SHARD_TRANSACTION_METADATA_NAME {
                    return Ok(());
                }
                let decoded = decode_single_shard_transaction_metadata(metadata)?;
                if decoded.command_id == command.header().command_id() {
                    if metadata != &expected {
                        return Err(corrupt_intent_history(
                            "single-Shard transaction command has contradictory durable receipts",
                        ));
                    }
                    durable = true;
                }
                Ok(())
            },
        )?;
        Ok(durable)
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

enum IntentTransition {
    None,
    Prepare(ParticipantIntent),
    Finalize(TransactionId),
}

#[derive(Default)]
struct RebuildingIntent {
    prepared: Option<TransactionRecord>,
    intent: Option<ParticipantIntent>,
    unmatched_terminal: Option<TransactionRecord>,
    home_decision: Option<TransactionRecord>,
}

fn rebuild_active_intents(
    binding: &ReplicaBinding,
    state_store: &Arc<dyn ReplicaStateStore>,
    applied_index: u64,
) -> Result<BTreeMap<TransactionId, ParticipantIntent>, ShardError> {
    let mut rebuilding = BTreeMap::<TransactionId, RebuildingIntent>::new();
    for_each_change(binding, state_store, applied_index, |change| {
        match change.mutation() {
            LogicalMutation::PutTransaction(record)
                if record.state() == TransactionState::Prepared =>
            {
                if !rebuilding.contains_key(&record.id())
                    && rebuilding.len() == MAX_TRANSACTION_INTENT_ITEMS
                {
                    return Err(corrupt_intent_history(
                        "active transaction intent count exceeds the supported bound",
                    ));
                }
                let entry = rebuilding.entry(record.id()).or_default();
                if entry
                    .prepared
                    .replace(record.clone())
                    .is_some_and(|existing| existing != *record)
                {
                    return Err(corrupt_intent_history(
                        "transaction has contradictory Prepared records",
                    ));
                }
            }
            LogicalMutation::PutTransaction(record) => {
                if let Some(entry) = rebuilding.get_mut(&record.id())
                    && let Some(intent) = entry.intent.as_ref()
                {
                    if record.record_digest() == intent.digest() {
                        validate_intent_terminal(intent, record)?;
                        rebuilding.remove(&record.id());
                    } else {
                        validate_home_decision_shape(intent, record)?;
                        if entry
                            .unmatched_terminal
                            .replace(record.clone())
                            .is_some_and(|existing| existing != *record)
                        {
                            return Err(corrupt_intent_history(
                                "transaction has contradictory unmatched terminal records",
                            ));
                        }
                    }
                }
            }
            LogicalMutation::PutReplicaMetadata(metadata)
                if metadata.name() == TRANSACTION_INTENT_METADATA_NAME =>
            {
                let Value::Bytes(bytes) = metadata.value() else {
                    return Err(corrupt_intent_history(
                        "transaction intent metadata is not bytes",
                    ));
                };
                let intent = ParticipantIntent::decode_current(bytes).map_err(|error| {
                    corrupt_intent_history(format!(
                        "transaction intent metadata is corrupt: {error}"
                    ))
                })?;
                if intent.shard_id() != binding.shard_id()
                    || intent.snapshot_applied_index() > change.raft_index()
                {
                    return Err(corrupt_intent_history(
                        "transaction intent metadata has inconsistent Shard fences",
                    ));
                }
                if !rebuilding.contains_key(&intent.transaction_id())
                    && rebuilding.len() == MAX_TRANSACTION_INTENT_ITEMS
                {
                    return Err(corrupt_intent_history(
                        "active transaction intent count exceeds the supported bound",
                    ));
                }
                let entry = rebuilding.entry(intent.transaction_id()).or_default();
                if entry
                    .intent
                    .replace(intent.clone())
                    .is_some_and(|existing| existing != intent)
                {
                    return Err(corrupt_intent_history(
                        "transaction has contradictory participant intent payloads",
                    ));
                }
            }
            LogicalMutation::PutReplicaMetadata(metadata)
                if metadata.name().starts_with("dtg.transaction_intent.") =>
            {
                return Err(corrupt_intent_history(
                    "unsupported transaction intent metadata version",
                ));
            }
            LogicalMutation::PutReplicaMetadata(metadata)
                if metadata.name() == HOME_DECISION_METADATA_NAME =>
            {
                let decision = decode_home_decision_metadata(metadata)?;
                if let Some(entry) = rebuilding.get_mut(&decision.id())
                    && entry
                        .home_decision
                        .replace(decision.clone())
                        .is_some_and(|existing| existing != decision)
                {
                    return Err(corrupt_intent_history(
                        "transaction has contradictory Home decision metadata",
                    ));
                }
            }
            LogicalMutation::PutReplicaMetadata(metadata)
                if metadata.name() == SINGLE_SHARD_TRANSACTION_METADATA_NAME =>
            {
                decode_single_shard_transaction_metadata(metadata)?;
            }
            LogicalMutation::PutVertex(_)
            | LogicalMutation::DeleteVertex(_)
            | LogicalMutation::PutEdge(_)
            | LogicalMutation::DeleteEdge(_)
            | LogicalMutation::PutReplicaMetadata(_) => {}
        }
        Ok(())
    })?;

    let mut active = BTreeMap::new();
    for (transaction_id, entry) in rebuilding {
        let (Some(prepared), Some(intent)) = (entry.prepared, entry.intent) else {
            return Err(corrupt_intent_history(
                "durable Prepared record and participant intent are incomplete",
            ));
        };
        if prepared.id() != transaction_id
            || prepared.transaction_time() != intent.start_time()
            || prepared.record_digest() != intent.digest()
        {
            return Err(corrupt_intent_history(
                "durable Prepared record does not match its participant intent",
            ));
        }
        if entry.unmatched_terminal != entry.home_decision {
            return Err(corrupt_intent_history(
                "unmatched transaction terminal lacks an exact Home decision marker",
            ));
        }
        active.insert(transaction_id, intent);
    }
    Ok(active)
}

fn validate_intent_terminal(
    intent: &ParticipantIntent,
    terminal: &TransactionRecord,
) -> Result<(), ShardError> {
    let valid = match terminal.state() {
        TransactionState::Committed => terminal.transaction_time() > intent.start_time(),
        TransactionState::Aborted => terminal.transaction_time() == intent.start_time(),
        TransactionState::Prepared => false,
    };
    if terminal.id() == intent.transaction_id()
        && terminal.record_digest() == intent.digest()
        && valid
    {
        Ok(())
    } else {
        Err(corrupt_intent_history(
            "participant terminal record contradicts its durable intent",
        ))
    }
}

fn validate_home_decision_shape(
    intent: &ParticipantIntent,
    decision: &TransactionRecord,
) -> Result<(), ShardError> {
    let valid_time = match decision.state() {
        TransactionState::Committed => decision.transaction_time() > intent.start_time(),
        TransactionState::Aborted => decision.transaction_time() == intent.start_time(),
        TransactionState::Prepared => false,
    };
    if decision.id() == intent.transaction_id() && valid_time {
        Ok(())
    } else {
        Err(corrupt_intent_history(
            "unmatched terminal record cannot be a valid Home decision",
        ))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SingleShardTransactionReceipt {
    command_id: CommandId,
    transaction_id: TransactionId,
    start_time: TransactionTime,
    snapshot_applied_index: u64,
    commit_time: TransactionTime,
    request_digest: Digest32,
}

impl SingleShardTransactionReceipt {
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

fn single_shard_transaction_metadata(
    command: &crate::CommitSingleShardTransaction,
) -> Result<ReplicaMetadata, ShardError> {
    let commit_time = graph_mutation_time(&command.mutations()[0])
        .expect("typed single-Shard transaction contains graph mutations");
    let mut bytes = Vec::with_capacity(92);
    bytes.extend_from_slice(&1_u32.to_be_bytes());
    bytes.extend_from_slice(&command.header().command_id().get().to_be_bytes());
    bytes.extend_from_slice(&command.transaction_id().get().to_be_bytes());
    bytes.extend_from_slice(&command.start_time().get().to_be_bytes());
    bytes.extend_from_slice(&command.snapshot_applied_index().to_be_bytes());
    bytes.extend_from_slice(&commit_time.get().to_be_bytes());
    bytes.extend_from_slice(&command.request_digest().get());
    Ok(ReplicaMetadata::new(
        SINGLE_SHARD_TRANSACTION_METADATA_NAME,
        Value::Bytes(bytes),
    )?)
}

pub fn decode_single_shard_transaction_metadata(
    metadata: &ReplicaMetadata,
) -> Result<SingleShardTransactionReceipt, ShardError> {
    if metadata.name() != SINGLE_SHARD_TRANSACTION_METADATA_NAME {
        return Err(corrupt_intent_history(
            "metadata is not a single-Shard transaction receipt",
        ));
    }
    let Value::Bytes(bytes) = metadata.value() else {
        return Err(corrupt_intent_history(
            "single-Shard transaction receipt is not bytes",
        ));
    };
    if bytes.len() != 92 || u32::from_be_bytes(bytes[..4].try_into().unwrap()) != 1 {
        return Err(corrupt_intent_history(
            "single-Shard transaction receipt has an unsupported encoding",
        ));
    }
    let receipt = SingleShardTransactionReceipt {
        command_id: CommandId::new(u128::from_be_bytes(bytes[4..20].try_into().unwrap()))?,
        transaction_id: TransactionId::new(u128::from_be_bytes(bytes[20..36].try_into().unwrap()))?,
        start_time: TransactionTime::new(i64::from_be_bytes(bytes[36..44].try_into().unwrap()))?,
        snapshot_applied_index: u64::from_be_bytes(bytes[44..52].try_into().unwrap()),
        commit_time: TransactionTime::new(i64::from_be_bytes(bytes[52..60].try_into().unwrap()))?,
        request_digest: Digest32::new(bytes[60..92].try_into().unwrap()),
    };
    if receipt.commit_time <= receipt.start_time {
        return Err(corrupt_intent_history(
            "single-Shard transaction receipt commit time does not follow its start time",
        ));
    }
    Ok(receipt)
}

fn home_decision_metadata(decision: &TransactionRecord) -> Result<ReplicaMetadata, ShardError> {
    let mut bytes = Vec::with_capacity(61);
    bytes.extend_from_slice(&1_u32.to_be_bytes());
    bytes.extend_from_slice(&decision.id().get().to_be_bytes());
    bytes.push(match decision.state() {
        TransactionState::Committed => 1,
        TransactionState::Aborted => 2,
        TransactionState::Prepared => {
            return Err(corrupt_intent_history(
                "Prepared is not a terminal Home decision",
            ));
        }
    });
    bytes.extend_from_slice(&decision.transaction_time().get().to_be_bytes());
    bytes.extend_from_slice(&decision.record_digest().get());
    Ok(ReplicaMetadata::new(
        HOME_DECISION_METADATA_NAME,
        Value::Bytes(bytes),
    )?)
}

fn decode_home_decision_metadata(
    metadata: &ReplicaMetadata,
) -> Result<TransactionRecord, ShardError> {
    let Value::Bytes(bytes) = metadata.value() else {
        return Err(corrupt_intent_history(
            "Home decision metadata is not bytes",
        ));
    };
    if bytes.len() != 61 || u32::from_be_bytes(bytes[..4].try_into().unwrap()) != 1 {
        return Err(corrupt_intent_history(
            "Home decision metadata has an unsupported encoding",
        ));
    }
    let id = TransactionId::new(u128::from_be_bytes(bytes[4..20].try_into().unwrap()))?;
    let state = match bytes[20] {
        1 => TransactionState::Committed,
        2 => TransactionState::Aborted,
        _ => {
            return Err(corrupt_intent_history(
                "Home decision metadata has an invalid state",
            ));
        }
    };
    let transaction_time =
        TransactionTime::new(i64::from_be_bytes(bytes[21..29].try_into().unwrap()))?;
    let digest = Digest32::new(bytes[29..61].try_into().unwrap());
    Ok(TransactionRecord::new(id, state, transaction_time, digest)?)
}

fn for_each_change(
    binding: &ReplicaBinding,
    state_store: &Arc<dyn ReplicaStateStore>,
    applied_index: u64,
    mut visit: impl FnMut(&ChangeRecord) -> Result<(), ShardError>,
) -> Result<(), ShardError> {
    if applied_index == 0 {
        return Ok(());
    }
    let view =
        block_on(state_store.begin_read_view(ReadFence::new(binding.clone(), applied_index)))?;
    let mut after = None;
    loop {
        let page =
            block_on(view.changes(ChangesRead::new(after, applied_index, HISTORY_PAGE_LIMIT)?))?;
        for change in page.rows() {
            visit(change)?;
        }
        match page.next_after() {
            Some(next)
                if page.rows().last().is_some_and(|row| row.cursor() == next)
                    && after.is_none_or(|previous| previous < next) =>
            {
                after = Some(next);
            }
            Some(_) => {
                return Err(corrupt_intent_history(
                    "durable change history pagination did not make progress",
                ));
            }
            None => break,
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum GraphIdentity {
    Vertex(VertexId),
    Edge(EdgeId),
}

#[derive(Clone, Copy)]
enum WriteExtent {
    Interval(ValidInterval),
    CompleteIdentity,
}

fn mutations_conflict(left: &[LogicalMutation], right: &[LogicalMutation]) -> bool {
    left.iter().filter_map(graph_write).any(|left| {
        right.iter().filter_map(graph_write).any(|right| {
            left.0 == right.0
                && match (left.1, right.1) {
                    (WriteExtent::Interval(left), WriteExtent::Interval(right)) => {
                        left.overlaps(right)
                    }
                    (WriteExtent::CompleteIdentity, _) | (_, WriteExtent::CompleteIdentity) => true,
                }
        })
    })
}

fn graph_write(mutation: &LogicalMutation) -> Option<(GraphIdentity, WriteExtent)> {
    match mutation {
        LogicalMutation::PutVertex(vertex) => Some((
            GraphIdentity::Vertex(vertex.id()),
            WriteExtent::Interval(vertex.valid_time()),
        )),
        LogicalMutation::DeleteVertex(vertex) => Some((
            GraphIdentity::Vertex(vertex.id()),
            WriteExtent::CompleteIdentity,
        )),
        LogicalMutation::PutEdge(edge) => Some((
            GraphIdentity::Edge(edge.id()),
            WriteExtent::Interval(edge.valid_time()),
        )),
        LogicalMutation::DeleteEdge(edge) => Some((
            GraphIdentity::Edge(edge.id()),
            WriteExtent::CompleteIdentity,
        )),
        LogicalMutation::PutTransaction(_) | LogicalMutation::PutReplicaMetadata(_) => None,
    }
}

fn graph_mutation_time(mutation: &LogicalMutation) -> Option<TransactionTime> {
    match mutation {
        LogicalMutation::PutVertex(vertex) => Some(vertex.transaction_time()),
        LogicalMutation::DeleteVertex(vertex) => Some(vertex.transaction_time()),
        LogicalMutation::PutEdge(edge) => Some(edge.transaction_time()),
        LogicalMutation::DeleteEdge(edge) => Some(edge.transaction_time()),
        LogicalMutation::PutTransaction(_) | LogicalMutation::PutReplicaMetadata(_) => None,
    }
}

fn corrupt_intent_history(message: impl Into<String>) -> ShardError {
    ShardError::CorruptIntentHistory(message.into())
}
