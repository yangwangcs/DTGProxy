use std::collections::BTreeMap;

use dtg_kernel::{ShardId, TransactionTime};
use dtg_storage::{TransactionRecord, TransactionState};

use crate::{
    CommitResolution, CommitTimeReservation, RecoveredParticipantIntent, ShardRequest,
    ShardRequestHeader, TemporalTxnCoordinator, TransactionContext, TransactionHistory,
    TransactionOutcome, TxnError, TxnFuture,
    coordinator::{abort_decision_digest, command_id, decision_digest, stamp_mutations},
};

struct RecoveryState {
    histories: BTreeMap<ShardId, TransactionHistory>,
    intents: BTreeMap<ShardId, RecoveredParticipantIntent>,
}

impl TemporalTxnCoordinator {
    pub fn recover<'a>(
        &'a self,
        context: &'a TransactionContext,
    ) -> TxnFuture<'a, TransactionOutcome> {
        Box::pin(async move {
            let state = self.load_recovery_state(context).await?;
            if context.snapshot().shards.len() == 1
                && state.histories.values().all(history_is_empty)
            {
                return self.recover_single_shard(context).await;
            }

            let home = home_shard(context)?;
            let decision = recover_home_decision(context, home, &state)?;
            let Some(decision) = decision else {
                validate_terminal_history_without_decision(context, &state)?;
                return Ok(TransactionOutcome::Unresolved);
            };
            validate_terminal_history(context, home, &decision, &state)?;

            match decision.state() {
                TransactionState::Committed => {
                    validate_commit_reservation(
                        self.timestamps
                            .commit_time_reservation(context.snapshot().transaction_id)
                            .await?,
                        decision.transaction_time(),
                    )?;
                    if state.intents.len() != context.snapshot().shards.len() {
                        return Err(TxnError::CorruptRecovery);
                    }
                    for (shard_id, fence) in &context.snapshot().shards {
                        let intent = state
                            .intents
                            .get(shard_id)
                            .ok_or(TxnError::CorruptRecovery)?;
                        let request = ShardRequest::FinalizeParticipantCommit {
                            header: ShardRequestHeader::new(
                                command_id(
                                    context.snapshot().transaction_id,
                                    b"finalize-commit",
                                    *shard_id,
                                )?,
                                fence.placement_epoch,
                                fence.backend_generation,
                            ),
                            transaction_id: context.snapshot().transaction_id,
                            start_time: context.snapshot().start_time,
                            commit_time: decision.transaction_time(),
                            intent_digest: intent.digest(),
                            mutations: intent.mutations().to_vec(),
                        };
                        self.participants.submit(*shard_id, request).await?;
                    }
                    self.timestamps
                        .resolve_commit_time(
                            context.snapshot().transaction_id,
                            decision.transaction_time(),
                            CommitResolution::Committed,
                        )
                        .await?;
                    Ok(TransactionOutcome::Committed(decision.transaction_time()))
                }
                TransactionState::Aborted => {
                    for (shard_id, intent) in &state.intents {
                        let fence = context
                            .snapshot()
                            .shards
                            .get(shard_id)
                            .ok_or(TxnError::CorruptRecovery)?;
                        let terminal = TransactionRecord::new(
                            context.snapshot().transaction_id,
                            TransactionState::Aborted,
                            context.snapshot().start_time,
                            intent.digest(),
                        )?;
                        let request = ShardRequest::FinalizeParticipantAbort {
                            header: ShardRequestHeader::new(
                                command_id(
                                    context.snapshot().transaction_id,
                                    b"finalize-abort",
                                    *shard_id,
                                )?,
                                fence.placement_epoch,
                                fence.backend_generation,
                            ),
                            terminal,
                        };
                        self.participants.submit(*shard_id, request).await?;
                    }
                    if let Some(reservation) = self
                        .timestamps
                        .commit_time_reservation(context.snapshot().transaction_id)
                        .await?
                    {
                        self.timestamps
                            .resolve_commit_time(
                                context.snapshot().transaction_id,
                                reservation.commit_time(),
                                CommitResolution::Aborted,
                            )
                            .await?;
                    }
                    Ok(TransactionOutcome::Aborted)
                }
                TransactionState::Prepared => Err(TxnError::CorruptRecovery),
            }
        })
    }

    pub(crate) fn resolve_failed_prewrite<'a>(
        &'a self,
        context: &'a TransactionContext,
    ) -> TxnFuture<'a, TransactionOutcome> {
        Box::pin(async move {
            let state = match self.load_recovery_state(context).await {
                Ok(state) => state,
                Err(TxnError::CorruptRecovery) => return Err(TxnError::CorruptRecovery),
                Err(_) => return Ok(TransactionOutcome::Unresolved),
            };
            let home = home_shard(context)?;
            if recover_home_decision(context, home, &state)?.is_some() {
                return self.recover(context).await;
            }
            if state
                .histories
                .values()
                .any(|history| !history.terminal().is_empty())
            {
                validate_terminal_history_without_decision(context, &state)?;
                return Ok(TransactionOutcome::Unresolved);
            }

            let fence = context
                .snapshot()
                .shards
                .get(&home)
                .ok_or(TxnError::IncompleteSnapshot)?;
            let decision = TransactionRecord::new(
                context.snapshot().transaction_id,
                TransactionState::Aborted,
                context.snapshot().start_time,
                abort_decision_digest(
                    context.snapshot().transaction_id,
                    context.snapshot().start_time,
                    context.snapshot().shards.keys().copied(),
                ),
            )?;
            let request = ShardRequest::RecordHomeDecision {
                header: ShardRequestHeader::new(
                    command_id(context.snapshot().transaction_id, b"decision", home)?,
                    fence.placement_epoch,
                    fence.backend_generation,
                ),
                decision,
            };
            if self.participants.submit(home, request).await.is_err() {
                return match self.recover(context).await {
                    Ok(outcome) => Ok(outcome),
                    Err(TxnError::CorruptRecovery) => Err(TxnError::CorruptRecovery),
                    Err(_) => Ok(TransactionOutcome::Unresolved),
                };
            }
            self.recover(context).await
        })
    }

    fn load_recovery_state<'a>(
        &'a self,
        context: &'a TransactionContext,
    ) -> TxnFuture<'a, RecoveryState> {
        Box::pin(async move {
            let transaction_id = context.snapshot().transaction_id;
            let mut histories = BTreeMap::new();
            let mut intents = BTreeMap::new();
            for shard_id in context.snapshot().shards.keys().copied() {
                let history = self
                    .participants
                    .transaction_history(shard_id, transaction_id)
                    .await?;
                if let Some(intent) = recover_intent(context, shard_id, &history)? {
                    intents.insert(shard_id, intent);
                }
                histories.insert(shard_id, history);
            }
            Ok(RecoveryState { histories, intents })
        })
    }

    fn recover_single_shard<'a>(
        &'a self,
        context: &'a TransactionContext,
    ) -> TxnFuture<'a, TransactionOutcome> {
        Box::pin(async move {
            let transaction_id = context.snapshot().transaction_id;
            let Some(reservation) = self
                .timestamps
                .commit_time_reservation(transaction_id)
                .await?
            else {
                return Ok(TransactionOutcome::Unresolved);
            };
            if reservation.resolution() == Some(CommitResolution::Aborted) {
                return Ok(TransactionOutcome::Aborted);
            }
            if reservation.commit_time() <= context.snapshot().start_time {
                return Err(TxnError::CorruptRecovery);
            }
            let (&shard_id, fence) = context
                .snapshot()
                .shards
                .iter()
                .next()
                .ok_or(TxnError::IncompleteSnapshot)?;
            let request = ShardRequest::CommitSingleShard {
                header: ShardRequestHeader::new(
                    command_id(transaction_id, b"single-commit", shard_id)?,
                    fence.placement_epoch,
                    fence.backend_generation,
                ),
                mutations: stamp_mutations(
                    context.overlay().mutations(),
                    reservation.commit_time(),
                )?,
            };
            self.participants.submit(shard_id, request).await?;
            self.timestamps
                .resolve_commit_time(
                    transaction_id,
                    reservation.commit_time(),
                    CommitResolution::Committed,
                )
                .await?;
            Ok(TransactionOutcome::Committed(reservation.commit_time()))
        })
    }
}

fn history_is_empty(history: &TransactionHistory) -> bool {
    history.prepared().is_none() && history.intent().is_none() && history.terminal().is_empty()
}

fn home_shard(context: &TransactionContext) -> Result<ShardId, TxnError> {
    context
        .snapshot()
        .shards
        .keys()
        .next()
        .copied()
        .ok_or(TxnError::IncompleteSnapshot)
}

fn validate_commit_reservation(
    reservation: Option<CommitTimeReservation>,
    commit_time: TransactionTime,
) -> Result<(), TxnError> {
    match reservation {
        Some(reservation)
            if reservation.commit_time() == commit_time
                && reservation.resolution() != Some(CommitResolution::Aborted) =>
        {
            Ok(())
        }
        _ => Err(TxnError::CorruptRecovery),
    }
}

fn recover_intent(
    context: &TransactionContext,
    shard_id: ShardId,
    history: &TransactionHistory,
) -> Result<Option<RecoveredParticipantIntent>, TxnError> {
    match (history.prepared(), history.intent()) {
        (None, None) => Ok(None),
        (Some(prepared), Some(intent))
            if prepared.id() == context.snapshot().transaction_id
                && prepared.state() == TransactionState::Prepared
                && prepared.transaction_time() == context.snapshot().start_time
                && intent.shard_id() == shard_id
                && intent.start_time() == context.snapshot().start_time
                && prepared.record_digest() == intent.digest() =>
        {
            Ok(Some(intent.clone()))
        }
        _ => Err(TxnError::CorruptRecovery),
    }
}

fn recover_home_decision(
    context: &TransactionContext,
    home: ShardId,
    state: &RecoveryState,
) -> Result<Option<TransactionRecord>, TxnError> {
    let transaction_id = context.snapshot().transaction_id;
    let local_digest = state
        .intents
        .get(&home)
        .map(RecoveredParticipantIntent::digest);
    let mut decision = None;
    for record in state.histories[&home].terminal() {
        if record.id() != transaction_id || record.state() == TransactionState::Prepared {
            return Err(TxnError::CorruptRecovery);
        }
        if local_digest == Some(record.record_digest()) {
            continue;
        }
        let valid = match record.state() {
            TransactionState::Committed
                if record.transaction_time() > context.snapshot().start_time
                    && state.intents.len() == context.snapshot().shards.len() =>
            {
                decision_digest(
                    transaction_id,
                    record.transaction_time(),
                    state
                        .intents
                        .iter()
                        .map(|(shard_id, intent)| (*shard_id, intent.digest())),
                ) == record.record_digest()
            }
            TransactionState::Aborted
                if record.transaction_time() == context.snapshot().start_time =>
            {
                abort_decision_digest(
                    transaction_id,
                    context.snapshot().start_time,
                    context.snapshot().shards.keys().copied(),
                ) == record.record_digest()
            }
            TransactionState::Prepared
            | TransactionState::Committed
            | TransactionState::Aborted => false,
        };
        if !valid {
            return Err(TxnError::CorruptRecovery);
        }
        if decision
            .replace(record.clone())
            .is_some_and(|existing| existing != *record)
        {
            return Err(TxnError::CorruptRecovery);
        }
    }
    Ok(decision)
}

fn validate_terminal_history(
    context: &TransactionContext,
    home: ShardId,
    decision: &TransactionRecord,
    state: &RecoveryState,
) -> Result<(), TxnError> {
    for (shard_id, history) in &state.histories {
        for record in history.terminal() {
            if *shard_id == home && record == decision {
                continue;
            }
            let intent = state
                .intents
                .get(shard_id)
                .ok_or(TxnError::CorruptRecovery)?;
            if record.id() != context.snapshot().transaction_id
                || record.state() != decision.state()
                || record.transaction_time() != decision.transaction_time()
                || record.record_digest() != intent.digest()
            {
                return Err(TxnError::CorruptRecovery);
            }
        }
    }
    Ok(())
}

fn validate_terminal_history_without_decision(
    context: &TransactionContext,
    state: &RecoveryState,
) -> Result<(), TxnError> {
    let mut observed: Option<(TransactionState, TransactionTime)> = None;
    for (shard_id, history) in &state.histories {
        for record in history.terminal() {
            let intent = state
                .intents
                .get(shard_id)
                .ok_or(TxnError::CorruptRecovery)?;
            if record.id() != context.snapshot().transaction_id
                || record.record_digest() != intent.digest()
                || record.state() == TransactionState::Prepared
                || (record.state() == TransactionState::Aborted
                    && record.transaction_time() != context.snapshot().start_time)
                || (record.state() == TransactionState::Committed
                    && record.transaction_time() <= context.snapshot().start_time)
            {
                return Err(TxnError::CorruptRecovery);
            }
            let candidate = (record.state(), record.transaction_time());
            if observed.is_some_and(|existing| existing != candidate) {
                return Err(TxnError::CorruptRecovery);
            }
            observed = Some(candidate);
        }
    }
    Ok(())
}
