use std::collections::BTreeMap;

use dtg_kernel::{ShardId, TransactionId};
use dtg_storage::{TransactionRecord, TransactionState};

use crate::{
    RecoveredParticipantIntent, ShardRequest, ShardRequestHeader, TemporalTxnCoordinator,
    TransactionContext, TransactionHistory, TransactionOutcome, TxnError, TxnFuture,
    coordinator::{abort_decision_digest, command_id, decision_digest},
};

impl TemporalTxnCoordinator {
    pub fn recover<'a>(
        &'a self,
        context: &'a TransactionContext,
    ) -> TxnFuture<'a, TransactionOutcome> {
        Box::pin(async move {
            let transaction_id = context.snapshot().transaction_id;
            let mut histories = BTreeMap::new();
            let mut intents = BTreeMap::new();
            for shard_id in context.snapshot().shards.keys().copied() {
                let history = self
                    .participants
                    .transaction_history(shard_id, transaction_id)
                    .await?;
                if let Some(intent) = recover_intent(transaction_id, shard_id, &history)? {
                    intents.insert(shard_id, intent);
                }
                histories.insert(shard_id, history);
            }

            let home = *context
                .snapshot()
                .shards
                .keys()
                .next()
                .ok_or(TxnError::IncompleteSnapshot)?;
            let decision = recover_home_decision(context, home, &histories[&home], &intents)?;
            let Some(decision) = decision else {
                return Ok(TransactionOutcome::Unresolved);
            };

            match decision.state() {
                TransactionState::Committed => {
                    if intents.len() != context.snapshot().shards.len() {
                        return Err(TxnError::CorruptRecovery);
                    }
                    for (shard_id, fence) in &context.snapshot().shards {
                        let intent = intents.get(shard_id).ok_or(TxnError::CorruptRecovery)?;
                        let request = ShardRequest::FinalizeParticipantCommit {
                            header: ShardRequestHeader::new(
                                command_id(transaction_id, b"finalize-commit", *shard_id)?,
                                fence.placement_epoch,
                                fence.backend_generation,
                            ),
                            transaction_id,
                            commit_time: decision.transaction_time(),
                            intent_digest: intent.digest(),
                            mutations: intent.mutations().to_vec(),
                        };
                        self.participants.submit(*shard_id, request).await?;
                    }
                    self.timestamps
                        .publish_commit_time(transaction_id, decision.transaction_time())
                        .await?;
                    Ok(TransactionOutcome::Committed(decision.transaction_time()))
                }
                TransactionState::Aborted => {
                    for (shard_id, fence) in &context.snapshot().shards {
                        let digest = intents
                            .get(shard_id)
                            .map_or(decision.record_digest(), RecoveredParticipantIntent::digest);
                        let terminal = TransactionRecord::new(
                            transaction_id,
                            TransactionState::Aborted,
                            decision.transaction_time(),
                            digest,
                        )?;
                        let request = ShardRequest::FinalizeParticipantAbort {
                            header: ShardRequestHeader::new(
                                command_id(transaction_id, b"finalize-abort", *shard_id)?,
                                fence.placement_epoch,
                                fence.backend_generation,
                            ),
                            terminal,
                        };
                        self.participants.submit(*shard_id, request).await?;
                    }
                    Ok(TransactionOutcome::Aborted)
                }
                TransactionState::Prepared => Err(TxnError::CorruptRecovery),
            }
        })
    }
}

fn recover_intent(
    transaction_id: TransactionId,
    shard_id: ShardId,
    history: &TransactionHistory,
) -> Result<Option<RecoveredParticipantIntent>, TxnError> {
    match (history.prepared(), history.intent()) {
        (None, None) => Ok(None),
        (Some(prepared), Some(intent))
            if prepared.id() == transaction_id
                && prepared.state() == TransactionState::Prepared
                && intent.shard_id() == shard_id
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
    history: &TransactionHistory,
    intents: &BTreeMap<ShardId, RecoveredParticipantIntent>,
) -> Result<Option<TransactionRecord>, TxnError> {
    let transaction_id = context.snapshot().transaction_id;
    let local_digest = intents.get(&home).map(RecoveredParticipantIntent::digest);
    let mut decision = None;
    let mut suspicious_terminal = false;
    for record in history.terminal().iter().filter(|record| {
        record.id() == transaction_id
            && matches!(
                record.state(),
                TransactionState::Committed | TransactionState::Aborted
            )
    }) {
        if local_digest == Some(record.record_digest()) {
            continue;
        }
        let valid = match record.state() {
            TransactionState::Committed if intents.len() == context.snapshot().shards.len() => {
                decision_digest(
                    transaction_id,
                    record.transaction_time(),
                    intents
                        .iter()
                        .map(|(shard_id, intent)| (*shard_id, intent.digest())),
                ) == record.record_digest()
            }
            TransactionState::Aborted => {
                abort_decision_digest(
                    transaction_id,
                    context.snapshot().start_time,
                    context.snapshot().shards.keys().copied(),
                ) == record.record_digest()
            }
            TransactionState::Prepared | TransactionState::Committed => false,
        };
        if !valid {
            suspicious_terminal = true;
            continue;
        }
        if decision
            .replace(record.clone())
            .is_some_and(|existing| existing != *record)
        {
            return Err(TxnError::CorruptRecovery);
        }
    }
    if decision.is_none() && suspicious_terminal {
        Err(TxnError::CorruptRecovery)
    } else {
        Ok(decision)
    }
}
