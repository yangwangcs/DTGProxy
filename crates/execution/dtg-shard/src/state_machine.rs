use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll, Wake, Waker};

use dtg_kernel::{Digest32, KernelError, TransactionId, TransactionTime, ValidInterval};
use dtg_storage::{
    ApplyReceipt, BindingRole, ChangeCursor, ChangeRecord, ChangesRead, CommandId,
    CommittedShardBatch, EdgeId, LogicalMutation, ReadFence, ReplicaBinding, ReplicaMetadata,
    ReplicaStateStore, StorageError, TransactionRecord, TransactionState, Value, VertexId,
};

use crate::command::MAX_TRANSACTION_INTENT_ITEMS;
use crate::{ParticipantIntent, ShardCommand};

const HISTORY_PAGE_LIMIT: u32 = 256;
const HISTORY_PAGE_BUDGET: usize = 256;
const HISTORY_RECORD_BUDGET: usize = HISTORY_PAGE_LIMIT as usize * HISTORY_PAGE_BUDGET;
const HOME_DECISION_METADATA_PREFIX: &str = "dtg.transaction_home_decision.v2/";
pub const SINGLE_SHARD_TRANSACTION_METADATA_NAME: &str = "dtg.single_shard_transaction.v1/";
pub const ACTIVE_TRANSACTION_INTENTS_METADATA_NAME: &str = "dtg.transaction_active_intents.v1";
pub const TRANSACTION_STATE_METADATA_PREFIX: &str = "dtg.transaction_state.v1/";

static ASYNC_BRIDGE_RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();

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
    if tokio::runtime::Handle::try_current()
        .is_ok_and(|handle| handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread)
    {
        return tokio::task::block_in_place(|| async_bridge_runtime().block_on(future));
    }
    block_on_current_thread(future)
}

fn async_bridge_runtime() -> &'static tokio::runtime::Runtime {
    ASYNC_BRIDGE_RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .thread_name("dtg-shard-async-bridge")
            .build()
            .expect("dtg-shard async bridge runtime must initialize")
    })
}

fn block_on_current_thread<F: Future>(future: F) -> F::Output {
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

#[cfg(test)]
mod tests {
    use std::future::{Future, poll_fn};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, mpsc};
    use std::task::Poll;
    use std::time::{Duration, Instant};

    use super::block_on;

    #[test]
    fn block_on_drives_tokio_future_from_single_worker_runtime() {
        const ASYNC_DELAY: Duration = Duration::from_millis(25);
        const WATCHDOG_DELAY: Duration = Duration::from_millis(500);
        const TEST_TIMEOUT: Duration = Duration::from_secs(2);

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_time()
            .thread_name("dtg-shard-block-on-test")
            .build()
            .expect("single-worker Tokio test runtime must build");
        let (worker_tx, worker_rx) = mpsc::sync_channel(1);
        let (completion_tx, completion_rx) = mpsc::sync_channel(1);
        let fallback_fired = Arc::new(AtomicBool::new(false));
        let task_fallback_fired = Arc::clone(&fallback_fired);

        runtime.spawn(async move {
            worker_tx
                .send(std::thread::current())
                .expect("test must receive the Tokio worker thread");
            let started = Instant::now();
            let completed_before_fallback = block_on(async move {
                let mut timer = std::pin::pin!(tokio::time::sleep(ASYNC_DELAY));
                poll_fn(|context| {
                    if timer.as_mut().poll(context).is_ready() {
                        Poll::Ready(true)
                    } else if task_fallback_fired.load(Ordering::Acquire) {
                        Poll::Ready(false)
                    } else {
                        Poll::Pending
                    }
                })
                .await
            });
            completion_tx
                .send((started.elapsed(), completed_before_fallback))
                .expect("test must receive bridge completion");
        });

        let worker = worker_rx
            .recv_timeout(TEST_TIMEOUT)
            .expect("Tokio worker task must start");
        let (cancel_watchdog_tx, cancel_watchdog_rx) = mpsc::channel();
        let watchdog = std::thread::spawn(move || {
            if cancel_watchdog_rx.recv_timeout(WATCHDOG_DELAY).is_ok() {
                false
            } else {
                fallback_fired.store(true, Ordering::Release);
                worker.unpark();
                true
            }
        });

        let (elapsed, completed_before_fallback) = completion_rx
            .recv_timeout(TEST_TIMEOUT)
            .expect("watchdog must prevent the old bridge from hanging forever");
        let _ = cancel_watchdog_tx.send(());
        let watchdog_fired = watchdog.join().expect("watchdog thread must not panic");

        assert!(
            completed_before_fallback && !watchdog_fired && elapsed < WATCHDOG_DELAY,
            "Tokio-driven future completed after the watchdog fallback: {elapsed:?}"
        );
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
    HomeDecisionConflict,
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
            Self::HomeDecisionConflict => "DTG-SHARD-HOME-DECISION-CONFLICT",
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
            Self::HomeDecisionConflict => {
                formatter.write_str("Home decision is already durably resolved differently")
            }
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApplyRejection {
    InvalidCommand,
    StalePlacementEpoch,
    StaleBackendGeneration,
    WriteConflict,
    ClosedTimestampFenced,
    HomeDecisionConflict,
}

impl ApplyRejection {
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidCommand => "DTG-SHARD-REJECTED-COMMAND",
            Self::StalePlacementEpoch => "DTG-SHARD-REJECTED-STALE-EPOCH",
            Self::StaleBackendGeneration => "DTG-SHARD-REJECTED-STALE-GENERATION",
            Self::WriteConflict => "DTG-SHARD-REJECTED-WRITE-CONFLICT",
            Self::ClosedTimestampFenced => "DTG-SHARD-REJECTED-CLOSED-TIME",
            Self::HomeDecisionConflict => "DTG-SHARD-REJECTED-HOME-DECISION",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplyOutcome {
    digest: Digest32,
    applied_index: u64,
    replayed: bool,
    rejection: Option<ApplyRejection>,
    active_binding: ReplicaBinding,
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

    pub const fn rejection(&self) -> Option<ApplyRejection> {
        self.rejection
    }

    pub const fn active_binding(&self) -> &ReplicaBinding {
        &self.active_binding
    }
}

pub struct ShardStateMachine {
    binding: ReplicaBinding,
    state_store: Arc<dyn ReplicaStateStore>,
    applied_index: u64,
    closed_timestamp: Option<TransactionTime>,
    active_intents: BTreeMap<TransactionId, ParticipantIntent>,
    migration_state_stores: BTreeMap<dtg_kernel::BackendGeneration, Arc<dyn ReplicaStateStore>>,
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
        let closed_timestamp = load_closed_timestamp(&state_store)?;
        Ok(Self {
            binding,
            state_store,
            applied_index,
            closed_timestamp,
            active_intents,
            migration_state_stores: BTreeMap::new(),
        })
    }

    pub fn stage_migration_state_store(
        &mut self,
        state_store: Arc<dyn ReplicaStateStore>,
    ) -> Result<(), ShardError> {
        let binding = state_store.binding();
        if binding.role() != BindingRole::Active
            || binding.cluster_id() != self.binding.cluster_id()
            || binding.graph_id() != self.binding.graph_id()
            || binding.shard_id() != self.binding.shard_id()
            || binding.replica_id() != self.binding.replica_id()
            || binding.backend_generation() <= self.binding.backend_generation()
            || block_on(state_store.applied_index())? != self.applied_index
        {
            return Err(ShardError::BindingMismatch);
        }
        if self
            .migration_state_stores
            .insert(binding.backend_generation(), state_store)
            .is_some()
        {
            return Err(ShardError::InvalidLifecycle(
                "migration state store generation is already staged".into(),
            ));
        }
        Ok(())
    }

    pub fn apply_committed(
        &mut self,
        term: u64,
        index: u64,
        command: ShardCommand,
    ) -> Result<ApplyOutcome, ShardError> {
        if let Err(error) = self.validate_command(&command) {
            return match committed_rejection(&error) {
                Some(rejection) => {
                    self.apply_rejected(term, index, command.header().command_id(), rejection)
                }
                None => Err(error),
            };
        }
        if let ShardCommand::Migration(migration) = &command
            && migration.cuts_over_from(
                self.binding.placement_epoch(),
                self.binding.backend_generation(),
            )
        {
            return self.apply_migration_cutover(term, index, migration);
        }
        if index < self.applied_index
            && matches!(
                command,
                ShardCommand::PrewriteIntent(_) | ShardCommand::FinalizeParticipant(_)
            )
        {
            return self.replay_old_transaction_command(term, index, &command);
        }
        let new_index = index > self.applied_index;
        let intent_transition = match self.validate_transaction_acceptance(&command) {
            Ok(transition) => transition,
            Err(error) => {
                return match committed_rejection(&error) {
                    Some(rejection) => {
                        self.apply_rejected(term, index, command.header().command_id(), rejection)
                    }
                    None => Err(error),
                };
            }
        };
        let logical_replay = new_index && matches!(intent_transition, IntentTransition::Replay);
        let header = command.header();

        let next_closed_timestamp = match &command {
            ShardCommand::AdvanceClosedTimestamp(command) => Some(command.closed_timestamp()),
            _ => None,
        };

        let mut next_active_intents = self.active_intents.clone();
        match &intent_transition {
            IntentTransition::Prepare(intent) => {
                next_active_intents.insert(intent.transaction_id(), intent.clone());
            }
            IntentTransition::Finalize { transaction_id, .. } => {
                next_active_intents.remove(transaction_id);
            }
            IntentTransition::None | IntentTransition::Replay => {}
        }

        let mut mutations = if logical_replay {
            Vec::new()
        } else {
            command.mutations()?
        };
        if let ShardCommand::RecordHomeDecision(command) = &command {
            mutations.push(LogicalMutation::PutReplicaMetadata(home_decision_metadata(
                command,
            )?));
        }
        if let ShardCommand::CommitSingleShardTransaction(command) = &command {
            mutations.push(LogicalMutation::PutReplicaMetadata(
                single_shard_transaction_metadata(command)?,
            ));
        }
        match (&command, &intent_transition) {
            (ShardCommand::PrewriteIntent(command), IntentTransition::Prepare(intent)) => {
                mutations.push(LogicalMutation::PutReplicaMetadata(
                    transaction_state_metadata(intent, None)?,
                ));
                mutations.push(LogicalMutation::PutReplicaMetadata(
                    active_intents_metadata(&next_active_intents)?,
                ));
                debug_assert_eq!(intent, command.intent());
            }
            (ShardCommand::PrewriteIntent(command), IntentTransition::Replay) => {
                mutations.push(LogicalMutation::PutReplicaMetadata(
                    transaction_state_metadata(command.intent(), None)?,
                ));
                mutations.push(LogicalMutation::PutReplicaMetadata(
                    active_intents_metadata(&next_active_intents)?,
                ));
            }
            (
                ShardCommand::FinalizeParticipant(command),
                IntentTransition::Finalize { intent, .. },
            ) => {
                mutations.push(LogicalMutation::PutReplicaMetadata(
                    transaction_state_metadata(intent, Some(command.terminal()))?,
                ));
                mutations.push(LogicalMutation::PutReplicaMetadata(
                    active_intents_metadata(&next_active_intents)?,
                ));
            }
            (ShardCommand::FinalizeParticipant(command), IntentTransition::Replay) => {
                let state = self
                    .durable_transaction_state(command.terminal().id())?
                    .ok_or_else(|| {
                        corrupt_intent_history(
                            "participant finalization replay lost its durable transaction state",
                        )
                    })?;
                mutations.push(LogicalMutation::PutReplicaMetadata(
                    transaction_state_metadata(state.intent(), state.terminal())?,
                ));
                mutations.push(LogicalMutation::PutReplicaMetadata(
                    active_intents_metadata(&next_active_intents)?,
                ));
            }
            _ => {}
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
                IntentTransition::Finalize { transaction_id, .. } => {
                    self.active_intents.remove(&transaction_id);
                }
                IntentTransition::Replay => {}
            }
        }
        Ok(ApplyOutcome {
            digest: receipt.mutation_digest(),
            applied_index: receipt.raft_index(),
            replayed: receipt.replayed() || logical_replay,
            rejection: None,
            active_binding: self.binding.clone(),
        })
    }

    pub fn apply_committed_batch(
        &mut self,
        entries: Vec<(u64, u64, ShardCommand)>,
    ) -> Result<Vec<ApplyOutcome>, ShardError> {
        if entries.len() < 2 || !self.state_store.supports_atomic_batch_apply() {
            return entries
                .into_iter()
                .map(|(term, index, command)| self.apply_committed(term, index, command))
                .collect();
        }
        let Some(outcomes) = self.try_apply_single_shard_transaction_batch(&entries)? else {
            return entries
                .into_iter()
                .map(|(term, index, command)| self.apply_committed(term, index, command))
                .collect();
        };
        Ok(outcomes)
    }

    fn try_apply_single_shard_transaction_batch(
        &mut self,
        entries: &[(u64, u64, ShardCommand)],
    ) -> Result<Option<Vec<ApplyOutcome>>, ShardError> {
        let mut expected_index = self.applied_index.saturating_add(1);
        let mut prior_mutations = Vec::new();
        let mut transaction_ids = BTreeSet::new();
        let mut batches = Vec::with_capacity(entries.len());
        for (term, index, command) in entries {
            let ShardCommand::CommitSingleShardTransaction(transaction) = command else {
                return Ok(None);
            };
            if *index != expected_index || *term == 0 {
                return Ok(None);
            }
            expected_index = expected_index.saturating_add(1);
            if !transaction_ids.insert(transaction.transaction_id()) {
                return Ok(None);
            }
            if self.validate_command(command).is_err()
                || !matches!(
                    self.validate_transaction_acceptance(command),
                    Ok(IntentTransition::None)
                )
            {
                return Ok(None);
            }
            let mutations = transaction.mutations();
            if mutations_conflict(&prior_mutations, mutations) {
                return Ok(None);
            }
            prior_mutations.extend_from_slice(mutations);
            let mut persisted = mutations.to_vec();
            persisted.push(LogicalMutation::PutReplicaMetadata(
                single_shard_transaction_metadata(transaction)?,
            ));
            batches.push(CommittedShardBatch::new(
                self.binding.clone(),
                *term,
                *index,
                transaction.header().command_id(),
                persisted,
            )?);
        }
        let receipts = block_on(self.state_store.apply_batches(batches))?;
        if receipts.len() != entries.len() || receipts.iter().any(ApplyReceipt::replayed) {
            return Err(ShardError::InvalidRaftState(
                "atomic state-store batch returned inconsistent receipts".into(),
            ));
        }
        self.applied_index = receipts
            .last()
            .map_or(self.applied_index, ApplyReceipt::raft_index);
        Ok(Some(
            receipts
                .into_iter()
                .map(|receipt| ApplyOutcome {
                    digest: receipt.mutation_digest(),
                    applied_index: receipt.raft_index(),
                    replayed: false,
                    rejection: None,
                    active_binding: self.binding.clone(),
                })
                .collect(),
        ))
    }

    fn replay_old_transaction_command(
        &self,
        term: u64,
        index: u64,
        command: &ShardCommand,
    ) -> Result<ApplyOutcome, ShardError> {
        let mutations =
            mutations_at_index(&self.binding, &self.state_store, self.applied_index, index)?;
        validate_historical_transaction_batch(command, &mutations)?;
        let batch = CommittedShardBatch::new(
            self.binding.clone(),
            term,
            index,
            command.header().command_id(),
            mutations,
        )?;
        let receipt = block_on(self.state_store.apply(batch))?;
        if !receipt.replayed() {
            return Err(corrupt_intent_history(
                "old-index transaction replay unexpectedly applied a new batch",
            ));
        }
        Ok(ApplyOutcome {
            digest: receipt.mutation_digest(),
            applied_index: receipt.raft_index(),
            replayed: true,
            rejection: None,
            active_binding: self.binding.clone(),
        })
    }

    pub(crate) fn validate_command(&self, command: &ShardCommand) -> Result<(), ShardError> {
        let header = command.header();
        let expected_epoch = self.binding.placement_epoch().get();
        let actual_epoch = header.placement_epoch().get();
        let cuts_over_from_current = matches!(
            command,
            ShardCommand::Migration(migration)
                if migration.cuts_over_from(
                    self.binding.placement_epoch(),
                    self.binding.backend_generation(),
                )
        );
        if !cuts_over_from_current && actual_epoch != expected_epoch {
            return Err(ShardError::StalePlacementEpoch {
                expected: expected_epoch,
                actual: actual_epoch,
            });
        }
        let expected_generation = self.binding.backend_generation().get();
        let actual_generation = header.backend_generation().get();
        if !cuts_over_from_current && actual_generation != expected_generation {
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
            if self.active_intents.values().any(|intent| {
                intent
                    .mutations()
                    .iter()
                    .filter_map(graph_mutation_time)
                    .any(|pending| proposed >= pending)
            }) {
                return Err(ShardError::InvalidCommand(
                    "closed timestamp is fenced by a pending prepared intent".into(),
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
                    return Ok(IntentTransition::Replay);
                }
                self.validate_write_conflicts(
                    command.transaction_id(),
                    command.start_time(),
                    command.snapshot_applied_index(),
                    command.mutations(),
                )?;
                Ok(IntentTransition::None)
            }
            ShardCommand::CommitSingleShard(_) => Ok(IntentTransition::None),
            ShardCommand::PrewriteIntent(command) => {
                let intent = command.intent();
                if let Some(existing) = self.active_intents.get(&intent.transaction_id()) {
                    return if existing == intent {
                        Ok(IntentTransition::Replay)
                    } else {
                        Err(corrupt_intent_history(
                            "one transaction has contradictory active participant intents",
                        ))
                    };
                }
                if self.active_intents.len() == MAX_TRANSACTION_INTENT_ITEMS {
                    return Err(ShardError::InvalidCommand(
                        "active transaction intent count exceeds the supported bound".into(),
                    ));
                }
                if self
                    .durable_transaction_state(intent.transaction_id())?
                    .is_some()
                {
                    return Err(corrupt_intent_history(
                        "new prewrite contradicts existing durable transaction state",
                    ));
                }
                self.validate_write_conflicts(
                    intent.transaction_id(),
                    intent.start_time(),
                    intent.snapshot_applied_index(),
                    intent.mutations(),
                )?;
                Ok(IntentTransition::Prepare(intent.clone()))
            }
            ShardCommand::FinalizeParticipant(command) => {
                let transaction_id = command.terminal().id();
                if let Some(active) = self.active_intents.get(&transaction_id) {
                    if active.digest() != command.terminal().record_digest()
                        || command.intent().is_some_and(|intent| intent != active)
                    {
                        return Err(corrupt_intent_history(
                            "participant finalization contradicts the active durable intent",
                        ));
                    }
                    validate_intent_terminal(active, command.terminal())?;
                    return Ok(IntentTransition::Finalize {
                        transaction_id,
                        intent: active.clone(),
                    });
                }
                let Some(state) = self.durable_transaction_state(transaction_id)? else {
                    return Err(corrupt_intent_history(
                        "participant finalization has no durable Prepared intent",
                    ));
                };
                if state.matches_finalize(command) {
                    Ok(IntentTransition::Replay)
                } else {
                    Err(corrupt_intent_history(
                        "participant finalization contradicts durable terminal state",
                    ))
                }
            }
            ShardCommand::RecordHomeDecision(command) => {
                if self.home_decision_is_durable(command)? {
                    Ok(IntentTransition::Replay)
                } else {
                    Ok(IntentTransition::None)
                }
            }
            ShardCommand::AdvanceClosedTimestamp(_)
            | ShardCommand::InstallSnapshot(_)
            | ShardCommand::Migration(_) => Ok(IntentTransition::None),
        }
    }

    fn validate_write_conflicts(
        &self,
        transaction_id: TransactionId,
        start_time: TransactionTime,
        snapshot_applied_index: u64,
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
            Some(ChangeCursor::new(snapshot_applied_index, u64::MAX)),
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
        let durable = block_on(self.state_store.replica_metadata(expected.name()))?;
        match durable {
            None => Ok(false),
            Some(metadata) if metadata == expected => {
                decode_single_shard_transaction_metadata(&metadata)?;
                Ok(true)
            }
            Some(metadata) => {
                decode_single_shard_transaction_metadata(&metadata)?;
                Err(corrupt_intent_history(
                    "single-Shard transaction command has contradictory durable receipts",
                ))
            }
        }
    }

    fn durable_transaction_state(
        &self,
        transaction_id: TransactionId,
    ) -> Result<Option<DurableTransactionState>, ShardError> {
        let name = transaction_state_metadata_name(transaction_id);
        block_on(self.state_store.replica_metadata(&name))?
            .map(|metadata| decode_transaction_state_metadata(&metadata))
            .transpose()
    }

    fn home_decision_is_durable(
        &self,
        command: &crate::RecordHomeDecision,
    ) -> Result<bool, ShardError> {
        let expected = home_decision_metadata(command)?;
        match block_on(self.state_store.replica_metadata(expected.name()))? {
            None => Ok(false),
            Some(metadata) if metadata == expected => Ok(true),
            Some(metadata) => {
                decode_home_decision_metadata(&metadata)?;
                Err(ShardError::HomeDecisionConflict)
            }
        }
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
            rejection: None,
            active_binding: self.binding.clone(),
        })
    }

    fn apply_rejected(
        &mut self,
        term: u64,
        index: u64,
        command_id: CommandId,
        rejection: ApplyRejection,
    ) -> Result<ApplyOutcome, ShardError> {
        let mut fields = BTreeMap::new();
        fields.insert(
            "command_id".into(),
            Value::Bytes(command_id.get().to_be_bytes().to_vec()),
        );
        fields.insert("reason".into(), Value::String(rejection.code().into()));
        let batch = CommittedShardBatch::new(
            self.binding.clone(),
            term,
            index,
            command_id,
            vec![LogicalMutation::PutReplicaMetadata(ReplicaMetadata::new(
                "dtg.raft_rejection",
                Value::Map(fields),
            )?)],
        )?;
        let receipt = block_on(self.state_store.apply(batch))?;
        self.applied_index = self.applied_index.max(receipt.raft_index());
        Ok(ApplyOutcome {
            digest: receipt.mutation_digest(),
            applied_index: receipt.raft_index(),
            replayed: receipt.replayed(),
            rejection: Some(rejection),
            active_binding: self.binding.clone(),
        })
    }

    fn apply_migration_cutover(
        &mut self,
        term: u64,
        index: u64,
        command: &crate::MigrationCommand,
    ) -> Result<ApplyOutcome, ShardError> {
        let target_generation = command
            .target_generation()
            .ok_or_else(|| ShardError::InvalidCommand("migration cutover lacks a target".into()))?;
        let target = self
            .migration_state_stores
            .get(&target_generation)
            .cloned()
            .ok_or_else(|| {
                ShardError::InvalidLifecycle(
                    "migration cutover target state store is not staged".into(),
                )
            })?;
        let target_binding = target.binding().clone();
        if target_binding.placement_epoch() != command.header().placement_epoch()
            || target_binding.backend_generation() != target_generation
            || target_binding.backend_class_digest()
                != command.target_backend_class_digest().ok_or_else(|| {
                    ShardError::InvalidCommand(
                        "migration cutover lacks a target backend class".into(),
                    )
                })?
            || command.verified_index() != Some(self.applied_index)
        {
            return Err(ShardError::BindingMismatch);
        }
        let batch = CommittedShardBatch::new(
            target_binding.clone(),
            term,
            index,
            command.header().command_id(),
            ShardCommand::Migration(command.clone()).mutations()?,
        )?;
        let receipt = block_on(target.apply(batch))?;
        let active_intents =
            rebuild_active_intents(&target_binding, &target, receipt.raft_index())?;
        self.migration_state_stores.remove(&target_generation);
        self.binding = target_binding.clone();
        self.state_store = target;
        self.applied_index = receipt.raft_index();
        self.active_intents = active_intents;
        Ok(ApplyOutcome {
            digest: receipt.mutation_digest(),
            applied_index: receipt.raft_index(),
            replayed: receipt.replayed(),
            rejection: None,
            active_binding: target_binding,
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

fn committed_rejection(error: &ShardError) -> Option<ApplyRejection> {
    match error {
        ShardError::InvalidCommand(message)
            if message == "closed timestamp is fenced by a pending prepared intent" =>
        {
            Some(ApplyRejection::ClosedTimestampFenced)
        }
        ShardError::InvalidCommand(_) => Some(ApplyRejection::InvalidCommand),
        ShardError::StalePlacementEpoch { .. } => Some(ApplyRejection::StalePlacementEpoch),
        ShardError::StaleBackendGeneration { .. } => Some(ApplyRejection::StaleBackendGeneration),
        ShardError::WriteConflict => Some(ApplyRejection::WriteConflict),
        ShardError::HomeDecisionConflict => Some(ApplyRejection::HomeDecisionConflict),
        ShardError::UnsupportedCommandVersion(_)
        | ShardError::InvalidRaftState(_)
        | ShardError::SnapshotStateMissing { .. }
        | ShardError::InvalidLifecycle(_)
        | ShardError::DuplicateReplica
        | ShardError::HeterogeneousGeneration
        | ShardError::ReplicaNotFound
        | ShardError::Raft(_)
        | ShardError::CorruptIntentHistory(_)
        | ShardError::BindingMismatch
        | ShardError::Storage(_) => None,
    }
}

enum IntentTransition {
    None,
    Prepare(ParticipantIntent),
    Finalize {
        transaction_id: TransactionId,
        intent: ParticipantIntent,
    },
    Replay,
}

fn load_closed_timestamp(
    state_store: &Arc<dyn ReplicaStateStore>,
) -> Result<Option<TransactionTime>, ShardError> {
    let Some(metadata) =
        block_on(state_store.replica_metadata(crate::command::CLOSED_TIMESTAMP_METADATA_NAME))?
    else {
        return Ok(None);
    };
    let Value::Integer(value) = metadata.value() else {
        return Err(ShardError::Storage(StorageError::Internal(
            "closed timestamp metadata has an invalid value".into(),
        )));
    };
    TransactionTime::new(*value).map(Some).map_err(|_| {
        ShardError::Storage(StorageError::Internal(
            "closed timestamp metadata is outside the valid range".into(),
        ))
    })
}

fn rebuild_active_intents(
    binding: &ReplicaBinding,
    state_store: &Arc<dyn ReplicaStateStore>,
    applied_index: u64,
) -> Result<BTreeMap<TransactionId, ParticipantIntent>, ShardError> {
    let Some(metadata) =
        block_on(state_store.replica_metadata(ACTIVE_TRANSACTION_INTENTS_METADATA_NAME))?
    else {
        return Ok(BTreeMap::new());
    };
    let indexed = decode_active_intents_metadata(&metadata)?;
    let mut active = BTreeMap::new();
    for (transaction_id, digest) in indexed {
        let name = transaction_state_metadata_name(transaction_id);
        let Some(metadata) = block_on(state_store.replica_metadata(&name))? else {
            return Err(corrupt_intent_history(
                "active transaction index references missing durable state",
            ));
        };
        let state = decode_transaction_state_metadata(&metadata)?;
        if state.terminal().is_some()
            || state.intent().transaction_id() != transaction_id
            || state.intent().shard_id() != binding.shard_id()
            || state.intent().snapshot_applied_index() > applied_index
            || state.intent().digest() != digest
        {
            return Err(corrupt_intent_history(
                "active transaction index contradicts its durable Prepared state",
            ));
        }
        active.insert(transaction_id, state.intent().clone());
    }
    Ok(active)
}

fn mutations_at_index(
    binding: &ReplicaBinding,
    state_store: &Arc<dyn ReplicaStateStore>,
    applied_index: u64,
    index: u64,
) -> Result<Vec<LogicalMutation>, ShardError> {
    if index == 0 || index >= applied_index {
        return Err(corrupt_intent_history(
            "old-index transaction replay is outside durable history",
        ));
    }
    let view =
        block_on(state_store.begin_read_view(ReadFence::new(binding.clone(), applied_index)))?;
    let mut after = Some(ChangeCursor::new(index - 1, u64::MAX));
    let mut mutations = Vec::new();
    let mut pages = 0_usize;
    loop {
        if pages == HISTORY_PAGE_BUDGET {
            return Err(corrupt_intent_history(
                "old-index transaction batch exceeded the page budget",
            ));
        }
        pages += 1;
        let page = block_on(view.changes(ChangesRead::new(after, index, HISTORY_PAGE_LIMIT)?))?;
        if mutations.len().saturating_add(page.rows().len()) > HISTORY_RECORD_BUDGET {
            return Err(corrupt_intent_history(
                "old-index transaction batch exceeded the record budget",
            ));
        }
        for change in page.rows() {
            if change.raft_index() != index || change.mutation_ordinal() != mutations.len() as u64 {
                return Err(corrupt_intent_history(
                    "old-index transaction batch history is missing or out of order",
                ));
            }
            mutations.push(change.mutation().clone());
        }
        match page.next_after() {
            Some(next)
                if page.rows().last().is_some_and(|row| row.cursor() == next)
                    && after.is_some_and(|previous| previous < next)
                    && next.raft_index() == index =>
            {
                after = Some(next);
            }
            Some(_) => {
                return Err(corrupt_intent_history(
                    "old-index transaction batch pagination did not make progress",
                ));
            }
            None => break,
        }
    }
    if mutations.is_empty() {
        return Err(corrupt_intent_history(
            "old-index transaction batch history is missing",
        ));
    }
    Ok(mutations)
}

fn validate_historical_transaction_batch(
    command: &ShardCommand,
    historical: &[LogicalMutation],
) -> Result<(), ShardError> {
    let command_mutations = command.mutations()?;
    if historical.len() != command_mutations.len() + 2
        || historical[..command_mutations.len()] != command_mutations
    {
        return Err(corrupt_intent_history(
            "old-index transaction batch contradicts the replayed command",
        ));
    }
    let LogicalMutation::PutReplicaMetadata(state_metadata) = &historical[command_mutations.len()]
    else {
        return Err(corrupt_intent_history(
            "old-index transaction batch lacks durable transaction state",
        ));
    };
    let LogicalMutation::PutReplicaMetadata(active_metadata) =
        &historical[command_mutations.len() + 1]
    else {
        return Err(corrupt_intent_history(
            "old-index transaction batch lacks the active transaction index",
        ));
    };
    let state = decode_transaction_state_metadata(state_metadata)?;
    let active = decode_active_intents_metadata(active_metadata)?;
    match command {
        ShardCommand::PrewriteIntent(command)
            if state.intent() == command.intent()
                && state.terminal().is_none()
                && active.get(&command.intent().transaction_id())
                    == Some(&command.intent().digest()) =>
        {
            Ok(())
        }
        ShardCommand::FinalizeParticipant(command)
            if state.matches_finalize(command)
                && !active.contains_key(&command.terminal().id()) =>
        {
            Ok(())
        }
        ShardCommand::PrewriteIntent(_) | ShardCommand::FinalizeParticipant(_) => {
            Err(corrupt_intent_history(
                "old-index transaction metadata contradicts the replayed command",
            ))
        }
        _ => unreachable!("only transaction state commands use historical batch replay"),
    }
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

#[derive(Clone, Debug, Eq, PartialEq)]
struct DurableTransactionState {
    intent: ParticipantIntent,
    terminal: Option<TransactionRecord>,
}

impl DurableTransactionState {
    const fn intent(&self) -> &ParticipantIntent {
        &self.intent
    }

    const fn terminal(&self) -> Option<&TransactionRecord> {
        self.terminal.as_ref()
    }

    fn matches_finalize(&self, command: &crate::FinalizeParticipant) -> bool {
        self.terminal.as_ref() == Some(command.terminal())
            && match command.terminal().state() {
                TransactionState::Committed => command.intent() == Some(&self.intent),
                TransactionState::Aborted => command.intent().is_none(),
                TransactionState::Prepared => false,
            }
    }
}

fn active_intents_metadata(
    active: &BTreeMap<TransactionId, ParticipantIntent>,
) -> Result<ReplicaMetadata, ShardError> {
    if active.len() > MAX_TRANSACTION_INTENT_ITEMS {
        return Err(ShardError::InvalidCommand(
            "active transaction intent count exceeds the supported bound".into(),
        ));
    }
    let mut bytes = Vec::with_capacity(8 + active.len() * 48);
    bytes.extend_from_slice(&1_u32.to_be_bytes());
    bytes.extend_from_slice(&(active.len() as u32).to_be_bytes());
    for (transaction_id, intent) in active {
        bytes.extend_from_slice(&transaction_id.get().to_be_bytes());
        bytes.extend_from_slice(&intent.digest().get());
    }
    Ok(ReplicaMetadata::new(
        ACTIVE_TRANSACTION_INTENTS_METADATA_NAME,
        Value::Bytes(bytes),
    )?)
}

fn decode_active_intents_metadata(
    metadata: &ReplicaMetadata,
) -> Result<BTreeMap<TransactionId, Digest32>, ShardError> {
    if metadata.name() != ACTIVE_TRANSACTION_INTENTS_METADATA_NAME {
        return Err(corrupt_intent_history(
            "metadata is not the active transaction intent index",
        ));
    }
    let Value::Bytes(bytes) = metadata.value() else {
        return Err(corrupt_intent_history(
            "active transaction intent index is not bytes",
        ));
    };
    if bytes.len() < 8 || u32::from_be_bytes(bytes[..4].try_into().unwrap()) != 1 {
        return Err(corrupt_intent_history(
            "active transaction intent index has an unsupported encoding",
        ));
    }
    let count = u32::from_be_bytes(bytes[4..8].try_into().unwrap()) as usize;
    if count > MAX_TRANSACTION_INTENT_ITEMS || bytes.len() != 8 + count * 48 {
        return Err(corrupt_intent_history(
            "active transaction intent index exceeds its bound or is truncated",
        ));
    }
    let mut indexed = BTreeMap::new();
    let mut previous = None;
    for chunk in bytes[8..].chunks_exact(48) {
        let transaction_id =
            TransactionId::new(u128::from_be_bytes(chunk[..16].try_into().unwrap()))?;
        if previous.is_some_and(|previous| previous >= transaction_id) {
            return Err(corrupt_intent_history(
                "active transaction intent index is not strictly ordered",
            ));
        }
        previous = Some(transaction_id);
        indexed.insert(
            transaction_id,
            Digest32::new(chunk[16..48].try_into().unwrap()),
        );
    }
    Ok(indexed)
}

fn transaction_state_metadata_name(transaction_id: TransactionId) -> String {
    format!(
        "{TRANSACTION_STATE_METADATA_PREFIX}{:032x}",
        transaction_id.get()
    )
}

fn transaction_state_metadata(
    intent: &ParticipantIntent,
    terminal: Option<&TransactionRecord>,
) -> Result<ReplicaMetadata, ShardError> {
    if let Some(terminal) = terminal {
        validate_intent_terminal(intent, terminal)?;
    }
    let encoded_intent = intent.encode_current()?;
    let mut bytes = Vec::with_capacity(9 + encoded_intent.len() + 40);
    bytes.extend_from_slice(&1_u32.to_be_bytes());
    bytes.push(match terminal.map(TransactionRecord::state) {
        None => 1,
        Some(TransactionState::Committed) => 2,
        Some(TransactionState::Aborted) => 3,
        Some(TransactionState::Prepared) => {
            return Err(corrupt_intent_history(
                "durable transaction state cannot store Prepared as a terminal",
            ));
        }
    });
    bytes.extend_from_slice(&(encoded_intent.len() as u32).to_be_bytes());
    bytes.extend_from_slice(&encoded_intent);
    if let Some(terminal) = terminal {
        bytes.extend_from_slice(&terminal.transaction_time().get().to_be_bytes());
        bytes.extend_from_slice(&terminal.record_digest().get());
    }
    Ok(ReplicaMetadata::new(
        transaction_state_metadata_name(intent.transaction_id()),
        Value::Bytes(bytes),
    )?)
}

fn decode_transaction_state_metadata(
    metadata: &ReplicaMetadata,
) -> Result<DurableTransactionState, ShardError> {
    let transaction_id = transaction_id_from_metadata_name(
        metadata.name(),
        TRANSACTION_STATE_METADATA_PREFIX,
        "durable transaction state",
    )?;
    let Value::Bytes(bytes) = metadata.value() else {
        return Err(corrupt_intent_history(
            "durable transaction state is not bytes",
        ));
    };
    if bytes.len() < 9 || u32::from_be_bytes(bytes[..4].try_into().unwrap()) != 1 {
        return Err(corrupt_intent_history(
            "durable transaction state has an unsupported encoding",
        ));
    }
    let tag = bytes[4];
    let intent_len = u32::from_be_bytes(bytes[5..9].try_into().unwrap()) as usize;
    let intent_end = 9_usize.saturating_add(intent_len);
    if intent_end > bytes.len() {
        return Err(corrupt_intent_history(
            "durable transaction state intent payload is truncated",
        ));
    }
    let intent = ParticipantIntent::decode_current(&bytes[9..intent_end]).map_err(|error| {
        corrupt_intent_history(format!(
            "durable transaction state intent is corrupt: {error}"
        ))
    })?;
    if intent.transaction_id() != transaction_id {
        return Err(corrupt_intent_history(
            "durable transaction state name contradicts its transaction ID",
        ));
    }
    let terminal = match tag {
        1 if intent_end == bytes.len() => None,
        2 | 3 if bytes.len() == intent_end + 40 => {
            let state = if tag == 2 {
                TransactionState::Committed
            } else {
                TransactionState::Aborted
            };
            let transaction_time = TransactionTime::new(i64::from_be_bytes(
                bytes[intent_end..intent_end + 8].try_into().unwrap(),
            ))?;
            let digest = Digest32::new(bytes[intent_end + 8..].try_into().unwrap());
            let terminal = TransactionRecord::new(transaction_id, state, transaction_time, digest)?;
            validate_intent_terminal(&intent, &terminal)?;
            Some(terminal)
        }
        _ => {
            return Err(corrupt_intent_history(
                "durable transaction state tag or trailing bytes are invalid",
            ));
        }
    };
    Ok(DurableTransactionState { intent, terminal })
}

fn transaction_id_from_metadata_name(
    name: &str,
    prefix: &str,
    description: &str,
) -> Result<TransactionId, ShardError> {
    let Some(suffix) = name.strip_prefix(prefix) else {
        return Err(corrupt_intent_history(format!(
            "metadata is not a {description} record"
        )));
    };
    if suffix.len() != 32 {
        return Err(corrupt_intent_history(format!(
            "{description} metadata name is malformed"
        )));
    }
    let value = u128::from_str_radix(suffix, 16)
        .map_err(|_| corrupt_intent_history(format!("{description} metadata name is malformed")))?;
    Ok(TransactionId::new(value)?)
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
        format!(
            "{SINGLE_SHARD_TRANSACTION_METADATA_NAME}{:032x}",
            command.transaction_id().get()
        ),
        Value::Bytes(bytes),
    )?)
}

pub fn decode_single_shard_transaction_metadata(
    metadata: &ReplicaMetadata,
) -> Result<SingleShardTransactionReceipt, ShardError> {
    let transaction_id = transaction_id_from_metadata_name(
        metadata.name(),
        SINGLE_SHARD_TRANSACTION_METADATA_NAME,
        "single-Shard transaction receipt",
    )?;
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
    if receipt.transaction_id != transaction_id || receipt.commit_time <= receipt.start_time {
        return Err(corrupt_intent_history(
            "single-Shard transaction receipt name or commit time is inconsistent",
        ));
    }
    Ok(receipt)
}

fn home_decision_metadata(
    command: &crate::RecordHomeDecision,
) -> Result<ReplicaMetadata, ShardError> {
    let bytes = ShardCommand::RecordHomeDecision(command.clone()).encode_current()?;
    Ok(ReplicaMetadata::new(
        format!(
            "{HOME_DECISION_METADATA_PREFIX}{:032x}",
            command.decision().id().get()
        ),
        Value::Bytes(bytes),
    )?)
}

fn decode_home_decision_metadata(
    metadata: &ReplicaMetadata,
) -> Result<crate::RecordHomeDecision, ShardError> {
    let transaction_id = transaction_id_from_metadata_name(
        metadata.name(),
        HOME_DECISION_METADATA_PREFIX,
        "Home decision",
    )?;
    let Value::Bytes(bytes) = metadata.value() else {
        return Err(corrupt_intent_history(
            "Home decision metadata is not bytes",
        ));
    };
    let ShardCommand::RecordHomeDecision(command) = ShardCommand::decode(bytes)? else {
        return Err(corrupt_intent_history(
            "Home decision metadata contains another command type",
        ));
    };
    if command.decision().id() != transaction_id {
        return Err(corrupt_intent_history(
            "Home decision metadata name contradicts its transaction ID",
        ));
    }
    Ok(command)
}

fn for_each_change(
    binding: &ReplicaBinding,
    state_store: &Arc<dyn ReplicaStateStore>,
    applied_index: u64,
    initial_after: Option<ChangeCursor>,
    mut visit: impl FnMut(&ChangeRecord) -> Result<(), ShardError>,
) -> Result<(), ShardError> {
    if applied_index == 0 {
        return Ok(());
    }
    let view =
        block_on(state_store.begin_read_view(ReadFence::new(binding.clone(), applied_index)))?;
    let mut after = initial_after;
    let mut pages = 0_usize;
    let mut records = 0_usize;
    loop {
        if pages == HISTORY_PAGE_BUDGET {
            return Err(corrupt_intent_history(
                "durable change history exceeded the page budget",
            ));
        }
        pages += 1;
        let page =
            block_on(view.changes(ChangesRead::new(after, applied_index, HISTORY_PAGE_LIMIT)?))?;
        if records.saturating_add(page.rows().len()) > HISTORY_RECORD_BUDGET {
            return Err(corrupt_intent_history(
                "durable change history exceeded the record budget",
            ));
        }
        records += page.rows().len();
        for change in page.rows() {
            if after.is_some_and(|after| change.cursor() <= after)
                || change.raft_index() > applied_index
            {
                return Err(corrupt_intent_history(
                    "durable change history row is outside the requested range",
                ));
            }
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
