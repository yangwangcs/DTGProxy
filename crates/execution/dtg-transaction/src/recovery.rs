use std::collections::BTreeMap;

use dtg_kernel::{ShardId, TransactionTime};
use dtg_storage::{TransactionRecord, TransactionState};

use crate::{
    CommitResolution, CommitTimeReservation, RecoveredParticipantIntent,
    RecoveredSingleShardCommit, ShardRequest, ShardRequestHeader, SubmissionFailure,
    TemporalTxnCoordinator, TransactionContext, TransactionHistory, TransactionOutcome, TxnError,
    TxnFuture,
    coordinator::{
        abort_decision_digest, command_id, decision_digest, stamp_mutations, transaction_manifest,
    },
};

struct RecoveryState {
    histories: BTreeMap<ShardId, TransactionHistory>,
    intents: BTreeMap<ShardId, RecoveredParticipantIntent>,
}

pub(crate) enum AbortPrevalidation {
    Proceed(Option<TransactionTime>),
    Terminal(TransactionOutcome),
}

impl TemporalTxnCoordinator {
    pub fn recover_transaction(
        &self,
        transaction_id: dtg_kernel::TransactionId,
        recovery_owner: u128,
    ) -> TxnFuture<'_, TransactionOutcome> {
        Box::pin(async move {
            let lease = self
                .participants
                .acquire_recovery_lease(transaction_id, recovery_owner)
                .await?;
            let result = async {
                let manifest = self
                    .participants
                    .recovery_manifest(transaction_id, lease)
                    .await?
                    .ok_or(TxnError::CorruptRecovery)?;
                if manifest.transaction_id() != transaction_id {
                    return Err(TxnError::CorruptRecovery);
                }
                let context = TransactionContext::new(manifest.snapshot_token()?);
                let state = self.load_recovery_state(&context).await?;
                for participant in manifest.participants() {
                    if state
                        .intents
                        .get(&participant.shard_id())
                        .map(RecoveredParticipantIntent::digest)
                        != Some(participant.intent_digest())
                    {
                        return Err(TxnError::CorruptRecovery);
                    }
                }
                match self.recover(&context).await? {
                    TransactionOutcome::Unresolved => self.resolve_failed_prewrite(&context).await,
                    outcome => Ok(outcome),
                }
            }
            .await;
            let release = self.participants.release_recovery_lease(lease).await;
            match (result, release) {
                (Ok(outcome), Ok(())) => Ok(outcome),
                (Err(error), _) => Err(error),
                (Ok(_), Err(error)) => Err(error),
            }
        })
    }

    pub fn recover<'a>(
        &'a self,
        context: &'a TransactionContext,
    ) -> TxnFuture<'a, TransactionOutcome> {
        Box::pin(async move {
            let state = self.load_recovery_state(context).await?;
            if let Some((shard_id, commit)) = recovered_single_shard_commit(&state)? {
                return self
                    .recover_durable_single_shard(context, shard_id, commit)
                    .await;
            }
            if context.snapshot().shards.len() == 1
                && state.histories.values().all(history_is_empty)
            {
                return self.recover_single_shard(context).await;
            }
            if state.intents.is_empty() {
                validate_terminal_history_without_decision(context, &state)?;
                if !state.histories.values().all(history_is_empty) {
                    return Ok(TransactionOutcome::Unresolved);
                }
                let Some(reservation) = self
                    .timestamps
                    .commit_time_reservation(context.snapshot().transaction_id)
                    .await?
                else {
                    return Ok(TransactionOutcome::Unresolved);
                };
                if reservation.commit_time() <= context.snapshot().start_time {
                    return Err(TxnError::CorruptRecovery);
                }
                return match reservation.resolution() {
                    Some(CommitResolution::Aborted) => Ok(TransactionOutcome::Aborted),
                    Some(CommitResolution::Committed) => Err(TxnError::CorruptRecovery),
                    None => Ok(TransactionOutcome::Unresolved),
                };
            }

            let home = recovery_home_shard(&state)?;
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
                    for (shard_id, intent) in &state.intents {
                        let fence = context
                            .snapshot()
                            .shards
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
                            snapshot_applied_index: intent.snapshot_applied_index(),
                            commit_time: decision.transaction_time(),
                            intent_digest: intent.digest(),
                            mutations: intent.mutations().to_vec(),
                        };
                        self.participants
                            .submit(*shard_id, request)
                            .await
                            .map_err(SubmissionFailure::into_error)?;
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
                    let reservation = match self
                        .timestamps
                        .commit_time_reservation(context.snapshot().transaction_id)
                        .await
                    {
                        Ok(reservation) => reservation,
                        Err(_) => return Ok(TransactionOutcome::Unresolved),
                    };
                    validate_abort_reservation(reservation, context.snapshot().start_time)?;
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
                        self.participants
                            .submit(*shard_id, request)
                            .await
                            .map_err(SubmissionFailure::into_error)?;
                    }
                    if let Some(reservation) = reservation {
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
            match self.prevalidate_abort_authority(context).await? {
                AbortPrevalidation::Proceed(_) => {}
                AbortPrevalidation::Terminal(outcome) => return Ok(outcome),
            }
            let state = match self.load_recovery_state(context).await {
                Ok(state) => state,
                Err(TxnError::CorruptRecovery) => return Err(TxnError::CorruptRecovery),
                Err(_) => return Ok(TransactionOutcome::Unresolved),
            };
            if state.intents.is_empty() {
                if state
                    .histories
                    .values()
                    .any(|history| !history.terminal().is_empty())
                {
                    validate_terminal_history_without_decision(context, &state)?;
                    return Ok(TransactionOutcome::Unresolved);
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
                return Ok(TransactionOutcome::Aborted);
            }
            let home = recovery_home_shard(&state)?;
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
            let manifest = transaction_manifest(
                context,
                state
                    .intents
                    .iter()
                    .map(|(shard_id, intent)| (*shard_id, intent.digest())),
            )?;
            let decision = TransactionRecord::new(
                context.snapshot().transaction_id,
                TransactionState::Aborted,
                context.snapshot().start_time,
                manifest
                    .decision_digest(TransactionState::Aborted, context.snapshot().start_time)?,
            )?;
            let request = ShardRequest::RecordHomeDecision {
                header: ShardRequestHeader::new(
                    command_id(context.snapshot().transaction_id, b"decision", home)?,
                    fence.placement_epoch,
                    fence.backend_generation,
                ),
                decision,
                manifest,
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

    pub(crate) fn prevalidate_abort_authority<'a>(
        &'a self,
        context: &'a TransactionContext,
    ) -> TxnFuture<'a, AbortPrevalidation> {
        Box::pin(async move {
            let reservation = match self
                .timestamps
                .commit_time_reservation(context.snapshot().transaction_id)
                .await
            {
                Ok(reservation) => reservation,
                Err(_) => {
                    return Ok(AbortPrevalidation::Terminal(TransactionOutcome::Unresolved));
                }
            };
            let state = match self.load_recovery_state(context).await {
                Ok(state) => state,
                Err(TxnError::CorruptRecovery) => return Err(TxnError::CorruptRecovery),
                Err(_) => {
                    return Ok(AbortPrevalidation::Terminal(TransactionOutcome::Unresolved));
                }
            };

            if let Some((shard_id, commit)) = recovered_single_shard_commit(&state)? {
                return self
                    .recover_durable_single_shard(context, shard_id, commit)
                    .await
                    .map(AbortPrevalidation::Terminal);
            }

            if !state.intents.is_empty() {
                let home = recovery_home_shard(&state)?;
                if recover_home_decision(context, home, &state)?.is_some() {
                    return self
                        .recover(context)
                        .await
                        .map(AbortPrevalidation::Terminal);
                }
            }
            if state
                .histories
                .values()
                .any(|history| !history.terminal().is_empty())
            {
                validate_terminal_history_without_decision(context, &state)?;
                return Ok(AbortPrevalidation::Terminal(TransactionOutcome::Unresolved));
            }

            match reservation {
                Some(reservation) if reservation.commit_time() <= context.snapshot().start_time => {
                    Err(TxnError::CorruptRecovery)
                }
                Some(reservation)
                    if reservation.resolution() == Some(CommitResolution::Committed) =>
                {
                    Ok(AbortPrevalidation::Terminal(TransactionOutcome::Committed(
                        reservation.commit_time(),
                    )))
                }
                Some(reservation)
                    if reservation.resolution() == Some(CommitResolution::Aborted) =>
                {
                    Ok(AbortPrevalidation::Terminal(TransactionOutcome::Aborted))
                }
                Some(reservation) => {
                    Ok(AbortPrevalidation::Proceed(Some(reservation.commit_time())))
                }
                None => Ok(AbortPrevalidation::Proceed(None)),
            }
        })
    }

    pub(crate) fn resolve_definitive_single_shard_failure<'a>(
        &'a self,
        context: &'a TransactionContext,
        commit_time: TransactionTime,
        error: TxnError,
    ) -> TxnFuture<'a, TransactionOutcome> {
        Box::pin(async move {
            let reservation = match self
                .timestamps
                .commit_time_reservation(context.snapshot().transaction_id)
                .await
            {
                Ok(reservation) => reservation,
                Err(_) => return Ok(TransactionOutcome::Unresolved),
            };
            let state = match self.load_recovery_state(context).await {
                Ok(state) => state,
                Err(TxnError::CorruptRecovery) => return Err(TxnError::CorruptRecovery),
                Err(_) => return Ok(TransactionOutcome::Unresolved),
            };
            if let Some((shard_id, commit)) = recovered_single_shard_commit(&state)? {
                return self
                    .recover_durable_single_shard(context, shard_id, commit)
                    .await;
            }
            if state
                .histories
                .values()
                .any(|history| !history_is_empty(history))
            {
                return Err(TxnError::CorruptRecovery);
            }

            let Some(reservation) = reservation else {
                return Err(TxnError::CorruptRecovery);
            };
            if reservation.commit_time() != commit_time
                || reservation.commit_time() <= context.snapshot().start_time
            {
                return Err(TxnError::CorruptRecovery);
            }
            match reservation.resolution() {
                Some(CommitResolution::Committed) => Ok(TransactionOutcome::Committed(commit_time)),
                Some(CommitResolution::Aborted) => Ok(TransactionOutcome::Aborted),
                None => {
                    if self
                        .timestamps
                        .resolve_commit_time(
                            context.snapshot().transaction_id,
                            commit_time,
                            CommitResolution::Aborted,
                        )
                        .await
                        .is_err()
                    {
                        return Ok(TransactionOutcome::Unresolved);
                    }
                    Err(error)
                }
            }
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
            let request =
                single_shard_request(context, shard_id, fence, reservation.commit_time())?;
            match self.participants.submit(shard_id, request).await {
                Ok(_) => {}
                Err(SubmissionFailure::Definitive(error)) => {
                    return self
                        .resolve_definitive_single_shard_failure(
                            context,
                            reservation.commit_time(),
                            error,
                        )
                        .await;
                }
                Err(SubmissionFailure::Ambiguous(error)) => return Err(error),
            }
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

    fn recover_durable_single_shard<'a>(
        &'a self,
        context: &'a TransactionContext,
        shard_id: ShardId,
        commit: RecoveredSingleShardCommit,
    ) -> TxnFuture<'a, TransactionOutcome> {
        Box::pin(async move {
            let transaction_id = context.snapshot().transaction_id;
            let Some(reservation) = self
                .timestamps
                .commit_time_reservation(transaction_id)
                .await?
            else {
                return Err(TxnError::CorruptRecovery);
            };
            if commit.commit_time() <= context.snapshot().start_time
                || reservation.resolution() == Some(CommitResolution::Aborted)
                || reservation.commit_time() != commit.commit_time()
            {
                return Err(TxnError::CorruptRecovery);
            }
            let fence = context
                .snapshot()
                .shards
                .get(&shard_id)
                .ok_or(TxnError::IncompleteSnapshot)?;
            let request = single_shard_request(context, shard_id, fence, commit.commit_time())?;
            if commit.transaction_id() != transaction_id
                || commit.command_id() != request.header().command_id()
                || commit.start_time() != context.snapshot().start_time
                || commit.snapshot_applied_index() != fence.applied_index
                || commit.request_digest() != request.digest()
            {
                return Err(TxnError::CorruptRecovery);
            }
            self.timestamps
                .resolve_commit_time(
                    transaction_id,
                    commit.commit_time(),
                    CommitResolution::Committed,
                )
                .await?;
            Ok(TransactionOutcome::Committed(commit.commit_time()))
        })
    }
}

fn history_is_empty(history: &TransactionHistory) -> bool {
    history.prepared().is_none()
        && history.intent().is_none()
        && history.terminal().is_empty()
        && history.single_shard_commit().is_none()
}

fn single_shard_request(
    context: &TransactionContext,
    shard_id: ShardId,
    fence: &crate::ShardSnapshotFence,
    commit_time: TransactionTime,
) -> Result<ShardRequest, TxnError> {
    Ok(ShardRequest::CommitSingleShard {
        header: ShardRequestHeader::new(
            command_id(
                context.snapshot().transaction_id,
                b"single-commit",
                shard_id,
            )?,
            fence.placement_epoch,
            fence.backend_generation,
        ),
        transaction_id: context.snapshot().transaction_id,
        start_time: context.snapshot().start_time,
        snapshot_applied_index: fence.applied_index,
        mutations: stamp_mutations(context.overlay().mutations(), commit_time)?,
    })
}

fn recovered_single_shard_commit(
    state: &RecoveryState,
) -> Result<Option<(ShardId, RecoveredSingleShardCommit)>, TxnError> {
    let mut recovered = None;
    for (shard_id, history) in &state.histories {
        if let Some(commit) = history.single_shard_commit()
            && recovered.replace((*shard_id, commit)).is_some()
        {
            return Err(TxnError::CorruptRecovery);
        }
    }
    Ok(recovered)
}

fn recovery_home_shard(state: &RecoveryState) -> Result<ShardId, TxnError> {
    state
        .intents
        .keys()
        .next()
        .copied()
        .ok_or(TxnError::CorruptRecovery)
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

fn validate_abort_reservation(
    reservation: Option<CommitTimeReservation>,
    start_time: TransactionTime,
) -> Result<(), TxnError> {
    match reservation {
        None => Ok(()),
        Some(reservation)
            if reservation.commit_time() > start_time
                && reservation.resolution() != Some(CommitResolution::Committed) =>
        {
            Ok(())
        }
        Some(_) => Err(TxnError::CorruptRecovery),
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
                && context
                    .snapshot()
                    .shards
                    .get(&shard_id)
                    .is_some_and(|fence| {
                        intent.snapshot_applied_index() == fence.applied_index
                    })
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
                    && !state.intents.is_empty() =>
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
                    state.intents.keys().copied(),
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
