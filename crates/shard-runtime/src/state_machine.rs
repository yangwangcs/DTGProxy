use std::cmp::{max, min};
use std::collections::BTreeMap;
use std::ops::Bound::{Excluded, Unbounded};

use raft_command::{CommandBodyV1, CommandEnvelopeV1};
use storage_api::{
    ApplyReceipt, CommittedMutationBatch, LogicalKey, Mutation, MutationOperation, StorageAdapter,
};
use temporal_types::TransactionTime;

use crate::ShardRuntimeError;
use crate::metadata::{
    ReplicaMetadata, adapter_applied_ts_key, closed_ts_key, decode_entry_digest,
    encode_entry_digest, encode_position, encode_timestamp, encode_unresolved_intent,
    entry_digest_key, is_reserved_metadata_key, load_metadata, load_unresolved_intents,
    position_key, resolved_ts_key, unresolved_intent_key,
};
use txn_protocol::{HomeDecisionEngine, ParticipantEngine, TransactionId};

pub struct ShardStateMachine<A> {
    adapter: A,
    metadata: ReplicaMetadata,
    unresolved: UnresolvedIntents,
    faulted_at: Option<u64>,
}

impl<A> ShardStateMachine<A>
where
    A: StorageAdapter,
{
    pub async fn open(
        adapter: A,
        shard_id: u32,
        placement_epoch: u64,
    ) -> Result<Self, ShardRuntimeError> {
        let metadata = load_metadata(&adapter, shard_id, placement_epoch).await?;
        let unresolved = UnresolvedIntents::new(load_unresolved_intents(&adapter).await?);
        if metadata.resolved_ts != resolved_timestamp(metadata.closed_ts, unresolved.oldest()) {
            return Err(ShardRuntimeError::CorruptMetadata {
                record: "resolved-ts",
            });
        }
        Ok(Self {
            adapter,
            metadata,
            unresolved,
            faulted_at: None,
        })
    }

    #[must_use]
    pub const fn adapter(&self) -> &A {
        &self.adapter
    }

    #[must_use]
    pub const fn metadata(&self) -> ReplicaMetadata {
        self.metadata
    }

    #[must_use]
    pub const fn is_healthy(&self) -> bool {
        self.faulted_at.is_none()
    }

    pub fn servable_safe_ts(&self) -> Result<TransactionTime, ShardRuntimeError> {
        if let Some(failed_index) = self.faulted_at {
            return Err(ShardRuntimeError::ReplicaFaulted { failed_index });
        }
        Ok(self.metadata.safe_ts())
    }

    #[must_use]
    pub fn into_adapter(self) -> A {
        self.adapter
    }

    pub async fn apply_entry(
        &mut self,
        term: u64,
        index: u64,
        command_bytes: &[u8],
    ) -> Result<ApplyReceipt, ShardRuntimeError> {
        if term == 0 || index == 0 {
            return Err(ShardRuntimeError::InvalidLogPosition { term, index });
        }
        let command = CommandEnvelopeV1::decode(command_bytes)?;
        self.validate_authority(&command)?;

        if let Some(failed_index) = self.faulted_at {
            if failed_index != index {
                return Err(ShardRuntimeError::ReplicaFaulted { failed_index });
            }
            self.metadata = load_metadata(
                &self.adapter,
                self.metadata.shard_id,
                self.metadata.placement_epoch,
            )
            .await?;
            self.unresolved = UnresolvedIntents::new(load_unresolved_intents(&self.adapter).await?);
        }

        let adapter_index = match self.adapter.applied_log_index() {
            Ok(index) => index,
            Err(error) => {
                self.faulted_at = Some(index);
                return Err(ShardRuntimeError::Adapter(error));
            }
        };
        if adapter_index != self.metadata.applied_index {
            return Err(ShardRuntimeError::MetadataIndexMismatch {
                metadata: self.metadata.applied_index,
                adapter: adapter_index,
            });
        }
        let digest = entry_digest(term, index, command_bytes);
        if index <= self.metadata.applied_index {
            if let Err(error) = self.verify_replay(term, index, digest).await {
                if matches!(error, ShardRuntimeError::Adapter(_)) {
                    self.faulted_at = Some(index);
                }
                return Err(error);
            }
            self.faulted_at = None;
            return Ok(ApplyReceipt {
                applied_log_index: self.metadata.applied_index,
                duplicate: true,
            });
        }

        let expected_index = self.metadata.applied_index.saturating_add(1);
        if index != expected_index {
            return Err(ShardRuntimeError::NonContiguousIndex {
                expected: expected_index,
                actual: index,
            });
        }
        if term < self.metadata.last_term {
            return Err(ShardRuntimeError::NonMonotonicTerm {
                current: self.metadata.last_term,
                proposed: term,
            });
        }

        let prepared = self.prepare_apply(term, index, command.body).await?;
        let next_metadata = prepared.metadata;
        let mut mutations = prepared.mutations;
        append_unresolved_change(&mut mutations, prepared.unresolved_change)?;
        append_meta_mutation(
            &mut mutations,
            entry_digest_key(index),
            encode_entry_digest(term, digest),
        )?;
        append_meta_mutation(
            &mut mutations,
            position_key(),
            encode_position(next_metadata),
        )?;
        append_watermark_mutations(&mut mutations, self.metadata, next_metadata)?;
        let batch = CommittedMutationBatch {
            shard_id: self.metadata.shard_id,
            log_index: index,
            txn_id: (u128::from(term) << 64) | u128::from(index),
            mutations,
        };
        let receipt = match self.adapter.apply_committed(batch).await {
            Ok(receipt) => receipt,
            Err(error) => {
                self.faulted_at = Some(index);
                return Err(ShardRuntimeError::Adapter(error));
            }
        };
        if receipt.applied_log_index != index {
            self.faulted_at = Some(index);
            return Err(ShardRuntimeError::ApplyReceiptMismatch {
                expected: index,
                actual: receipt.applied_log_index,
            });
        }
        self.metadata = next_metadata;
        self.unresolved.apply(prepared.unresolved_change)?;
        self.faulted_at = None;
        Ok(receipt)
    }

    pub async fn apply_noop_entry(
        &mut self,
        term: u64,
        index: u64,
    ) -> Result<ApplyReceipt, ShardRuntimeError> {
        if term == 0 || index == 0 {
            return Err(ShardRuntimeError::InvalidLogPosition { term, index });
        }
        if let Some(failed_index) = self.faulted_at {
            if failed_index != index {
                return Err(ShardRuntimeError::ReplicaFaulted { failed_index });
            }
            self.metadata = load_metadata(
                &self.adapter,
                self.metadata.shard_id,
                self.metadata.placement_epoch,
            )
            .await?;
            self.unresolved = UnresolvedIntents::new(load_unresolved_intents(&self.adapter).await?);
        }
        let adapter_index = match self.adapter.applied_log_index() {
            Ok(index) => index,
            Err(error) => {
                self.faulted_at = Some(index);
                return Err(ShardRuntimeError::Adapter(error));
            }
        };
        if adapter_index != self.metadata.applied_index {
            return Err(ShardRuntimeError::MetadataIndexMismatch {
                metadata: self.metadata.applied_index,
                adapter: adapter_index,
            });
        }
        let digest = entry_digest(term, index, &[]);
        if index <= self.metadata.applied_index {
            if let Err(error) = self.verify_replay(term, index, digest).await {
                if matches!(error, ShardRuntimeError::Adapter(_)) {
                    self.faulted_at = Some(index);
                }
                return Err(error);
            }
            self.faulted_at = None;
            return Ok(ApplyReceipt {
                applied_log_index: self.metadata.applied_index,
                duplicate: true,
            });
        }
        let expected_index = self.metadata.applied_index.saturating_add(1);
        if index != expected_index {
            return Err(ShardRuntimeError::NonContiguousIndex {
                expected: expected_index,
                actual: index,
            });
        }
        if term < self.metadata.last_term {
            return Err(ShardRuntimeError::NonMonotonicTerm {
                current: self.metadata.last_term,
                proposed: term,
            });
        }
        let next_metadata = ReplicaMetadata {
            last_term: term,
            applied_index: index,
            ..self.metadata
        };
        let mut mutations = Vec::new();
        append_meta_mutation(
            &mut mutations,
            entry_digest_key(index),
            encode_entry_digest(term, digest),
        )?;
        append_meta_mutation(
            &mut mutations,
            position_key(),
            encode_position(next_metadata),
        )?;
        let batch = CommittedMutationBatch {
            shard_id: self.metadata.shard_id,
            log_index: index,
            txn_id: (u128::from(term) << 64) | u128::from(index),
            mutations,
        };
        let receipt = match self.adapter.apply_committed(batch).await {
            Ok(receipt) => receipt,
            Err(error) => {
                self.faulted_at = Some(index);
                return Err(ShardRuntimeError::Adapter(error));
            }
        };
        if receipt.applied_log_index != index {
            self.faulted_at = Some(index);
            return Err(ShardRuntimeError::ApplyReceiptMismatch {
                expected: index,
                actual: receipt.applied_log_index,
            });
        }
        self.metadata = next_metadata;
        self.faulted_at = None;
        Ok(receipt)
    }

    fn validate_authority(&self, command: &CommandEnvelopeV1) -> Result<(), ShardRuntimeError> {
        if command.shard_id != self.metadata.shard_id {
            return Err(ShardRuntimeError::ShardMismatch {
                expected: self.metadata.shard_id,
                actual: command.shard_id,
            });
        }
        if command.placement_epoch != self.metadata.placement_epoch {
            return Err(ShardRuntimeError::StaleEpoch {
                expected: self.metadata.placement_epoch,
                actual: command.placement_epoch,
            });
        }
        Ok(())
    }

    async fn prepare_apply(
        &self,
        term: u64,
        index: u64,
        body: CommandBodyV1,
    ) -> Result<PreparedApply, ShardRuntimeError> {
        match body {
            CommandBodyV1::ApplyPrepared(apply) => {
                validate_business_mutations(&apply.batch.mutations)?;
                if apply.commit_ts <= self.metadata.closed_ts {
                    return Err(ShardRuntimeError::CommitAtOrBeforeClosed {
                        closed: self.metadata.closed_ts,
                        proposed: apply.commit_ts,
                    });
                }
                if apply.commit_ts <= self.metadata.adapter_applied_ts {
                    return Err(ShardRuntimeError::NonMonotonicCommit {
                        current: self.metadata.adapter_applied_ts,
                        proposed: apply.commit_ts,
                    });
                }
                Ok(PreparedApply {
                    metadata: self.metadata.after_apply(term, index, apply.commit_ts),
                    mutations: apply.batch.mutations,
                    unresolved_change: UnresolvedChange::None,
                })
            }
            CommandBodyV1::ClosedTimestampTick(closed_ts) => {
                if closed_ts < self.metadata.closed_ts {
                    return Err(ShardRuntimeError::NonMonotonicClosed {
                        current: self.metadata.closed_ts,
                        proposed: closed_ts,
                    });
                }
                let resolved_ts = resolved_timestamp(closed_ts, self.unresolved.oldest());
                Ok(PreparedApply {
                    metadata: ReplicaMetadata {
                        last_term: term,
                        applied_index: index,
                        closed_ts,
                        resolved_ts,
                        adapter_applied_ts: max(self.metadata.adapter_applied_ts, closed_ts),
                        ..self.metadata
                    },
                    mutations: Vec::new(),
                    unresolved_change: UnresolvedChange::None,
                })
            }
            CommandBodyV1::Prewrite(prewrite) => {
                let keys = ParticipantEngine::prewrite_inspection_keys(&prewrite.request)?;
                let values = self.adapter.multi_get(&keys).await?;
                let outcome = ParticipantEngine::prewrite(&prewrite.request, &values)?;
                if outcome.proof() != &prewrite.expected_proof {
                    return Err(ShardRuntimeError::ParticipantProofMismatch);
                }
                if !outcome.duplicate() && prewrite.request.start_ts() <= self.metadata.closed_ts {
                    return Err(ShardRuntimeError::IntentAtOrBeforeClosed {
                        closed: self.metadata.closed_ts,
                        start: prewrite.request.start_ts(),
                    });
                }
                let change = if outcome.duplicate() {
                    UnresolvedChange::None
                } else {
                    UnresolvedChange::Add {
                        transaction_id: prewrite.request.transaction_id(),
                        start_ts: prewrite.request.start_ts(),
                    }
                };
                self.unresolved.validate(change)?;
                let resolved_ts = self
                    .unresolved
                    .resolved_after(change, self.metadata.closed_ts)?;
                let mutations = outcome.mutations().to_vec();
                validate_business_mutations(&mutations)?;
                Ok(PreparedApply {
                    metadata: ReplicaMetadata {
                        last_term: term,
                        applied_index: index,
                        resolved_ts,
                        ..self.metadata
                    },
                    mutations,
                    unresolved_change: change,
                })
            }
            CommandBodyV1::RecordDecision(record) => {
                let key = HomeDecisionEngine::inspection_key(
                    record.home,
                    record.decision.transaction_id(),
                )?;
                let existing = self
                    .adapter
                    .multi_get(std::slice::from_ref(&key))
                    .await?
                    .pop()
                    .flatten();
                let outcome =
                    HomeDecisionEngine::record(record.home, &record.decision, existing.as_deref())?;
                let mutations = outcome.mutations().to_vec();
                validate_business_mutations(&mutations)?;
                Ok(PreparedApply {
                    metadata: ReplicaMetadata {
                        last_term: term,
                        applied_index: index,
                        ..self.metadata
                    },
                    mutations,
                    unresolved_change: UnresolvedChange::None,
                })
            }
            CommandBodyV1::Finalize(finalize) => {
                let request = self
                    .load_participant_request(
                        finalize.participant,
                        finalize.transaction_id,
                        finalize.intent_digest,
                    )
                    .await?;
                let keys = ParticipantEngine::finalize_inspection_keys(&request)?;
                let values = self.adapter.multi_get(&keys).await?;
                let outcome = ParticipantEngine::finalize(&request, finalize.commit_ts, &values)?;
                let change = if outcome.duplicate() {
                    UnresolvedChange::None
                } else {
                    UnresolvedChange::Remove {
                        transaction_id: finalize.transaction_id,
                        start_ts: request.start_ts(),
                    }
                };
                self.unresolved.validate(change)?;
                let resolved_ts = self
                    .unresolved
                    .resolved_after(change, self.metadata.closed_ts)?;
                let mutations = outcome.mutations().to_vec();
                validate_business_mutations(&mutations)?;
                Ok(PreparedApply {
                    metadata: ReplicaMetadata {
                        last_term: term,
                        applied_index: index,
                        resolved_ts,
                        adapter_applied_ts: max(
                            self.metadata.adapter_applied_ts,
                            finalize.commit_ts,
                        ),
                        ..self.metadata
                    },
                    mutations,
                    unresolved_change: change,
                })
            }
            CommandBodyV1::AbortIntent(abort) => {
                let request = self
                    .load_participant_request(
                        abort.participant,
                        abort.transaction_id,
                        abort.intent_digest,
                    )
                    .await?;
                let keys = ParticipantEngine::abort_inspection_keys(&request)?;
                let values = self.adapter.multi_get(&keys).await?;
                let outcome = ParticipantEngine::abort(&request, &values)?;
                let change = if outcome.duplicate() {
                    UnresolvedChange::None
                } else {
                    UnresolvedChange::Remove {
                        transaction_id: abort.transaction_id,
                        start_ts: request.start_ts(),
                    }
                };
                self.unresolved.validate(change)?;
                let resolved_ts = self
                    .unresolved
                    .resolved_after(change, self.metadata.closed_ts)?;
                let mutations = outcome.mutations().to_vec();
                validate_business_mutations(&mutations)?;
                Ok(PreparedApply {
                    metadata: ReplicaMetadata {
                        last_term: term,
                        applied_index: index,
                        resolved_ts,
                        ..self.metadata
                    },
                    mutations,
                    unresolved_change: change,
                })
            }
            CommandBodyV1::OnePhaseCommit(one_phase) => {
                let keys = ParticipantEngine::prewrite_inspection_keys(&one_phase.request)?;
                let values = self.adapter.multi_get(&keys).await?;
                let outcome = ParticipantEngine::one_phase_commit(
                    &one_phase.request,
                    one_phase.commit_ts,
                    &values,
                )?;
                if one_phase.expected_proof.participant() != one_phase.request.participant()
                    || one_phase.expected_proof.intent_digest() != one_phase.request.intent_digest()
                    || one_phase.expected_proof.min_commit_ts()
                        != timestamp_successor(one_phase.request.start_ts())?
                {
                    return Err(ShardRuntimeError::ParticipantProofMismatch);
                }
                if !outcome.duplicate() && one_phase.request.start_ts() <= self.metadata.closed_ts {
                    return Err(ShardRuntimeError::IntentAtOrBeforeClosed {
                        closed: self.metadata.closed_ts,
                        start: one_phase.request.start_ts(),
                    });
                }
                let mutations = outcome.mutations().to_vec();
                validate_business_mutations(&mutations)?;
                Ok(PreparedApply {
                    metadata: ReplicaMetadata {
                        last_term: term,
                        applied_index: index,
                        adapter_applied_ts: max(
                            self.metadata.adapter_applied_ts,
                            one_phase.commit_ts,
                        ),
                        ..self.metadata
                    },
                    mutations,
                    unresolved_change: UnresolvedChange::None,
                })
            }
        }
    }

    async fn load_participant_request(
        &self,
        participant: txn_protocol::ShardEpoch,
        transaction_id: TransactionId,
        intent_digest: [u8; 32],
    ) -> Result<txn_protocol::PrewriteRequest, ShardRuntimeError> {
        let key = ParticipantEngine::participant_record_key(participant, transaction_id)?;
        let bytes = self
            .adapter
            .multi_get(std::slice::from_ref(&key))
            .await?
            .pop()
            .flatten()
            .ok_or(txn_protocol::TxnProtocolError::MissingIntent)?;
        Ok(ParticipantEngine::request_from_participant_record(
            participant,
            transaction_id,
            intent_digest,
            &bytes,
        )?)
    }

    async fn verify_replay(
        &self,
        term: u64,
        index: u64,
        expected_digest: [u8; 32],
    ) -> Result<(), ShardRuntimeError> {
        let key = entry_digest_key(index);
        let value = self
            .adapter
            .multi_get(&[key])
            .await?
            .pop()
            .flatten()
            .ok_or(ShardRuntimeError::CorruptMetadata {
                record: "entry-digest",
            })?;
        let (stored_term, stored_digest) = decode_entry_digest(&value)?;
        if stored_term != term || stored_digest != expected_digest {
            return Err(ShardRuntimeError::DivergentReplay { index });
        }
        Ok(())
    }
}

struct PreparedApply {
    metadata: ReplicaMetadata,
    mutations: Vec<Mutation>,
    unresolved_change: UnresolvedChange,
}

#[derive(Clone, Copy)]
enum UnresolvedChange {
    None,
    Add {
        transaction_id: TransactionId,
        start_ts: TransactionTime,
    },
    Remove {
        transaction_id: TransactionId,
        start_ts: TransactionTime,
    },
}

struct UnresolvedIntents {
    by_transaction: BTreeMap<u128, TransactionTime>,
    by_start: BTreeMap<TransactionTime, usize>,
}

impl UnresolvedIntents {
    fn new(by_transaction: BTreeMap<u128, TransactionTime>) -> Self {
        let mut by_start = BTreeMap::new();
        for start_ts in by_transaction.values() {
            *by_start.entry(*start_ts).or_insert(0) += 1;
        }
        Self {
            by_transaction,
            by_start,
        }
    }

    fn oldest(&self) -> Option<TransactionTime> {
        self.by_start
            .first_key_value()
            .map(|(timestamp, _)| *timestamp)
    }

    fn validate(&self, change: UnresolvedChange) -> Result<(), ShardRuntimeError> {
        match change {
            UnresolvedChange::None => Ok(()),
            UnresolvedChange::Add { transaction_id, .. } => {
                if self.by_transaction.contains_key(&transaction_id.value()) {
                    return Err(ShardRuntimeError::CorruptMetadata {
                        record: "duplicate-unresolved-intent",
                    });
                }
                Ok(())
            }
            UnresolvedChange::Remove {
                transaction_id,
                start_ts,
            } => {
                if self.by_transaction.get(&transaction_id.value()) != Some(&start_ts) {
                    return Err(ShardRuntimeError::CorruptMetadata {
                        record: "missing-unresolved-intent",
                    });
                }
                Ok(())
            }
        }
    }

    fn resolved_after(
        &self,
        change: UnresolvedChange,
        closed_ts: TransactionTime,
    ) -> Result<TransactionTime, ShardRuntimeError> {
        self.validate(change)?;
        let oldest = match change {
            UnresolvedChange::None => self.oldest(),
            UnresolvedChange::Add { start_ts, .. } => Some(
                self.oldest()
                    .map_or(start_ts, |oldest| min(oldest, start_ts)),
            ),
            UnresolvedChange::Remove { start_ts, .. } => {
                let Some(oldest) = self.oldest() else {
                    return Err(ShardRuntimeError::CorruptMetadata {
                        record: "missing-unresolved-intent",
                    });
                };
                let count = self.by_start.get(&start_ts).copied().unwrap_or(0);
                if start_ts != oldest || count > 1 {
                    Some(oldest)
                } else {
                    self.by_start
                        .range((Excluded(start_ts), Unbounded))
                        .next()
                        .map(|(timestamp, _)| *timestamp)
                }
            }
        };
        Ok(resolved_timestamp(closed_ts, oldest))
    }

    fn apply(&mut self, change: UnresolvedChange) -> Result<(), ShardRuntimeError> {
        self.validate(change)?;
        match change {
            UnresolvedChange::None => {}
            UnresolvedChange::Add {
                transaction_id,
                start_ts,
            } => {
                self.by_transaction.insert(transaction_id.value(), start_ts);
                *self.by_start.entry(start_ts).or_insert(0) += 1;
            }
            UnresolvedChange::Remove {
                transaction_id,
                start_ts,
            } => {
                self.by_transaction.remove(&transaction_id.value());
                let count =
                    self.by_start
                        .get_mut(&start_ts)
                        .ok_or(ShardRuntimeError::CorruptMetadata {
                            record: "missing-unresolved-intent",
                        })?;
                *count -= 1;
                if *count == 0 {
                    self.by_start.remove(&start_ts);
                }
            }
        }
        Ok(())
    }
}

fn resolved_timestamp(
    closed_ts: TransactionTime,
    oldest_intent: Option<TransactionTime>,
) -> TransactionTime {
    oldest_intent.map_or(closed_ts, |timestamp| {
        min(closed_ts, timestamp_predecessor(timestamp))
    })
}

fn timestamp_predecessor(timestamp: TransactionTime) -> TransactionTime {
    if timestamp.logical() > 0 {
        return TransactionTime::new(timestamp.physical_micros(), timestamp.logical() - 1);
    }
    timestamp
        .physical_micros()
        .checked_sub(1)
        .map_or(TransactionTime::new(i64::MIN, 0), |physical_micros| {
            TransactionTime::new(physical_micros, u32::MAX)
        })
}

fn timestamp_successor(timestamp: TransactionTime) -> Result<TransactionTime, ShardRuntimeError> {
    if timestamp.logical() < u32::MAX {
        return Ok(TransactionTime::new(
            timestamp.physical_micros(),
            timestamp.logical() + 1,
        ));
    }
    Ok(TransactionTime::new(
        timestamp
            .physical_micros()
            .checked_add(1)
            .ok_or(txn_protocol::TxnProtocolError::TimestampExhausted)?,
        0,
    ))
}

fn append_unresolved_change(
    mutations: &mut Vec<Mutation>,
    change: UnresolvedChange,
) -> Result<(), ShardRuntimeError> {
    match change {
        UnresolvedChange::None => Ok(()),
        UnresolvedChange::Add {
            transaction_id,
            start_ts,
        } => append_meta_mutation(
            mutations,
            unresolved_intent_key(transaction_id.value()),
            encode_unresolved_intent(start_ts),
        ),
        UnresolvedChange::Remove { transaction_id, .. } => {
            append_meta_delete(mutations, unresolved_intent_key(transaction_id.value()))
        }
    }
}

fn validate_business_mutations(mutations: &[Mutation]) -> Result<(), ShardRuntimeError> {
    if mutations.iter().any(|mutation| {
        let key = match &mutation.operation {
            MutationOperation::Put { key, .. } | MutationOperation::Delete { key } => key,
        };
        is_reserved_metadata_key(key)
    }) {
        return Err(ShardRuntimeError::ReservedMetadataKey);
    }
    Ok(())
}

fn append_watermark_mutations(
    mutations: &mut Vec<Mutation>,
    previous: ReplicaMetadata,
    next: ReplicaMetadata,
) -> Result<(), ShardRuntimeError> {
    if next.closed_ts != previous.closed_ts {
        append_meta_mutation(mutations, closed_ts_key(), encode_timestamp(next.closed_ts))?;
    }
    if next.resolved_ts != previous.resolved_ts {
        append_meta_mutation(
            mutations,
            resolved_ts_key(),
            encode_timestamp(next.resolved_ts),
        )?;
    }
    if next.adapter_applied_ts != previous.adapter_applied_ts {
        append_meta_mutation(
            mutations,
            adapter_applied_ts_key(),
            encode_timestamp(next.adapter_applied_ts),
        )?;
    }
    Ok(())
}

fn append_meta_mutation(
    mutations: &mut Vec<Mutation>,
    key: LogicalKey,
    value: Vec<u8>,
) -> Result<(), ShardRuntimeError> {
    let sequence =
        u32::try_from(mutations.len()).map_err(|_| ShardRuntimeError::TooManyMutations)?;
    mutations.push(Mutation::put(sequence, key, value));
    Ok(())
}

fn append_meta_delete(
    mutations: &mut Vec<Mutation>,
    key: LogicalKey,
) -> Result<(), ShardRuntimeError> {
    let sequence =
        u32::try_from(mutations.len()).map_err(|_| ShardRuntimeError::TooManyMutations)?;
    mutations.push(Mutation::delete(sequence, key));
    Ok(())
}

fn entry_digest(term: u64, index: u64, command_bytes: &[u8]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"DTGProxy/RaftEntryDigest/V1");
    hasher.update(&term.to_be_bytes());
    hasher.update(&index.to_be_bytes());
    hasher.update(
        &u64::try_from(command_bytes.len())
            .expect("command length fits in u64")
            .to_be_bytes(),
    );
    hasher.update(command_bytes);
    *hasher.finalize().as_bytes()
}
