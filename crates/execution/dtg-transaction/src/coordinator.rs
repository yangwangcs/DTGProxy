use std::collections::BTreeSet;
use std::sync::Arc;

use dtg_kernel::{Digest32, ShardId, TransactionId, TransactionTime, Version};
use dtg_storage::{
    CommandId, EdgeTombstone, EdgeVersion, LogicalMutation, TransactionRecord, TransactionState,
    VertexTombstone, VertexVersion,
};

use crate::{
    BaseGraphSnapshot, DurableParticipantManifest, DurableTransactionManifest, ParticipantService,
    ParticipantWrite, ShardRequest, ShardRequestHeader, ShardSnapshotFence, SnapshotToken,
    SubmissionFailure, TransactionOverlay, TxnError, TxnFuture,
    conflict::detect_mutation_conflicts, recovery::AbortPrevalidation,
};

pub trait TimestampAuthority: Send + Sync {
    fn allocate_start_time(&self, transaction_id: TransactionId) -> TxnFuture<'_, TransactionTime>;

    /// Durably creates or returns the transaction's unique pending commit-time reservation.
    fn reserve_commit_time(&self, transaction_id: TransactionId) -> TxnFuture<'_, TransactionTime>;

    /// Reads a durable reservation without creating one.
    fn commit_time_reservation(
        &self,
        transaction_id: TransactionId,
    ) -> TxnFuture<'_, Option<CommitTimeReservation>>;

    /// Resolves one reservation. The authority may advance its published frontier only across the
    /// contiguous prefix of reservations resolved as either committed or aborted.
    fn resolve_commit_time(
        &self,
        transaction_id: TransactionId,
        commit_time: TransactionTime,
        resolution: CommitResolution,
    ) -> TxnFuture<'_, ()>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommitResolution {
    Committed,
    Aborted,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CommitTimeReservation {
    commit_time: TransactionTime,
    resolution: Option<CommitResolution>,
}

impl CommitTimeReservation {
    pub const fn new(commit_time: TransactionTime, resolution: Option<CommitResolution>) -> Self {
        Self {
            commit_time,
            resolution,
        }
    }

    pub const fn commit_time(self) -> TransactionTime {
        self.commit_time
    }

    pub const fn resolution(self) -> Option<CommitResolution> {
        self.resolution
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransactionContext {
    snapshot: SnapshotToken,
    base: BaseGraphSnapshot,
    overlay: TransactionOverlay,
}

impl TransactionContext {
    pub fn new(snapshot: SnapshotToken) -> Self {
        Self {
            snapshot,
            base: BaseGraphSnapshot::default(),
            overlay: TransactionOverlay::new(),
        }
    }

    pub fn with_base(snapshot: SnapshotToken, base: BaseGraphSnapshot) -> Self {
        Self {
            snapshot,
            base,
            overlay: TransactionOverlay::new(),
        }
    }

    pub const fn snapshot(&self) -> &SnapshotToken {
        &self.snapshot
    }

    pub const fn overlay(&self) -> &TransactionOverlay {
        &self.overlay
    }

    pub fn stage(&mut self, mutation: LogicalMutation) -> Result<(), TxnError> {
        self.overlay.stage(mutation)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransactionOutcome {
    Committed(TransactionTime),
    Aborted,
    Unresolved,
}

#[derive(Clone)]
pub struct TemporalTxnCoordinator {
    pub(crate) timestamps: Arc<dyn TimestampAuthority>,
    pub(crate) participants: ParticipantService,
}

impl TemporalTxnCoordinator {
    pub fn new(
        timestamps: Arc<dyn TimestampAuthority>,
        shards: Arc<dyn crate::ShardCommandExecutor>,
    ) -> Self {
        Self {
            timestamps,
            participants: ParticipantService::new(shards),
        }
    }

    pub fn begin(
        &self,
        transaction_id: TransactionId,
        catalog_version: Version,
        shard_fences: Vec<(ShardId, ShardSnapshotFence)>,
    ) -> TxnFuture<'_, TransactionContext> {
        Box::pin(async move {
            let start_time = self.timestamps.allocate_start_time(transaction_id).await?;
            Ok(TransactionContext::new(SnapshotToken::new(
                transaction_id,
                start_time,
                catalog_version,
                shard_fences,
            )?))
        })
    }

    pub fn commit<'a>(
        &'a self,
        context: &'a TransactionContext,
        mut participants: Vec<ParticipantWrite>,
    ) -> TxnFuture<'a, TransactionOutcome> {
        Box::pin(async move {
            context.overlay.validate(&context.base)?;
            validate_participants(context, &participants)?;
            participants.sort_by_key(ParticipantWrite::shard_id);
            if participants.is_empty() {
                return Ok(TransactionOutcome::Committed(context.snapshot.start_time));
            }

            let existing_reservation = self
                .timestamps
                .commit_time_reservation(context.snapshot.transaction_id)
                .await?;
            if existing_reservation.is_some_and(|reservation| {
                reservation.resolution() == Some(CommitResolution::Aborted)
            }) {
                return Ok(TransactionOutcome::Aborted);
            }
            if participants.len() == 1
                && let Some(reservation) = existing_reservation
                && reservation.resolution() == Some(CommitResolution::Committed)
            {
                if reservation.commit_time() <= context.snapshot.start_time {
                    return Err(TxnError::CorruptRecovery);
                }
                return Ok(TransactionOutcome::Committed(reservation.commit_time()));
            }
            let existing_commit_time =
                existing_reservation.map(|reservation| reservation.commit_time());

            for participant in &participants {
                let fence = context
                    .snapshot
                    .shards
                    .get(&participant.shard_id())
                    .ok_or(TxnError::IncompleteSnapshot)?;
                let changes = self
                    .participants
                    .changes_after(participant.shard_id(), fence.applied_index)
                    .await?;
                let intended = match existing_commit_time {
                    Some(commit_time) => stamp_mutations(participant.mutations(), commit_time)?,
                    None => Vec::new(),
                };
                let committed: Vec<_> = changes
                    .iter()
                    .map(|change| change.mutation().clone())
                    .filter(|mutation| !intended.contains(mutation))
                    .collect();
                if let Err(error) = detect_mutation_conflicts(
                    participant.mutations(),
                    &committed,
                    context.snapshot.start_time,
                ) {
                    if let Some(existing_commit_time) = existing_commit_time {
                        if participants.len() > 1 {
                            return self.resolve_failed_prewrite(context).await;
                        }
                        self.timestamps
                            .resolve_commit_time(
                                context.snapshot.transaction_id,
                                existing_commit_time,
                                CommitResolution::Aborted,
                            )
                            .await?;
                    }
                    return Err(error);
                }
            }

            let commit_time = match existing_commit_time {
                Some(commit_time) => commit_time,
                None => {
                    self.timestamps
                        .reserve_commit_time(context.snapshot.transaction_id)
                        .await?
                }
            };
            if commit_time <= context.snapshot.start_time {
                return Err(TxnError::InconsistentSnapshot);
            }

            if participants.len() == 1 {
                let participant = &participants[0];
                let fence = context.snapshot.shards[&participant.shard_id()];
                let request = ShardRequest::CommitSingleShard {
                    header: ShardRequestHeader::new(
                        command_id(
                            context.snapshot.transaction_id,
                            b"single-commit",
                            participant.shard_id(),
                        )?,
                        fence.placement_epoch,
                        fence.backend_generation,
                    ),
                    transaction_id: context.snapshot.transaction_id,
                    start_time: context.snapshot.start_time,
                    snapshot_applied_index: fence.applied_index,
                    mutations: stamp_mutations(participant.mutations(), commit_time)?,
                };
                match self
                    .participants
                    .submit(participant.shard_id(), request)
                    .await
                {
                    Ok(_) => {}
                    Err(SubmissionFailure::Definitive(error)) => {
                        return self
                            .resolve_definitive_single_shard_failure(context, commit_time, error)
                            .await;
                    }
                    Err(SubmissionFailure::Ambiguous(error)) => return Err(error),
                }
                self.timestamps
                    .resolve_commit_time(
                        context.snapshot.transaction_id,
                        commit_time,
                        CommitResolution::Committed,
                    )
                    .await?;
                return Ok(TransactionOutcome::Committed(commit_time));
            }

            let mut intents = Vec::with_capacity(participants.len());
            for participant in &participants {
                let fence = context.snapshot.shards[&participant.shard_id()];
                let mutations = stamp_mutations(participant.mutations(), commit_time)?;
                let request = ShardRequest::PrewriteIntent {
                    header: ShardRequestHeader::new(
                        command_id(
                            context.snapshot.transaction_id,
                            b"prewrite",
                            participant.shard_id(),
                        )?,
                        fence.placement_epoch,
                        fence.backend_generation,
                    ),
                    transaction_id: context.snapshot.transaction_id,
                    start_time: context.snapshot.start_time,
                    snapshot_applied_index: fence.applied_index,
                    mutations: mutations.clone(),
                };
                let receipt = match self
                    .participants
                    .submit(participant.shard_id(), request)
                    .await
                {
                    Ok(receipt) => receipt,
                    Err(_) => return self.resolve_failed_prewrite(context).await,
                };
                let Some(digest) = receipt.intent_digest() else {
                    return self.resolve_failed_prewrite(context).await;
                };
                intents.push((participant, mutations, digest));
            }

            let home = participants[0].shard_id();
            let home_fence = context.snapshot.shards[&home];
            let manifest = transaction_manifest(
                context,
                intents
                    .iter()
                    .map(|(participant, _, digest)| (participant.shard_id(), *digest)),
            )?;
            let decision_digest =
                manifest.decision_digest(TransactionState::Committed, commit_time)?;
            let decision = TransactionRecord::new(
                context.snapshot.transaction_id,
                TransactionState::Committed,
                commit_time,
                decision_digest,
            )?;
            let request = ShardRequest::RecordHomeDecision {
                header: ShardRequestHeader::new(
                    command_id(context.snapshot.transaction_id, b"decision", home)?,
                    home_fence.placement_epoch,
                    home_fence.backend_generation,
                ),
                decision,
                manifest,
            };
            self.participants
                .submit(home, request)
                .await
                .map_err(SubmissionFailure::into_error)?;

            for (participant, mutations, intent_digest) in intents {
                let fence = context.snapshot.shards[&participant.shard_id()];
                let request = ShardRequest::FinalizeParticipantCommit {
                    header: ShardRequestHeader::new(
                        command_id(
                            context.snapshot.transaction_id,
                            b"finalize-commit",
                            participant.shard_id(),
                        )?,
                        fence.placement_epoch,
                        fence.backend_generation,
                    ),
                    transaction_id: context.snapshot.transaction_id,
                    start_time: context.snapshot.start_time,
                    snapshot_applied_index: fence.applied_index,
                    commit_time,
                    intent_digest,
                    mutations,
                };
                self.participants
                    .submit(participant.shard_id(), request)
                    .await
                    .map_err(SubmissionFailure::into_error)?;
            }

            self.timestamps
                .resolve_commit_time(
                    context.snapshot.transaction_id,
                    commit_time,
                    CommitResolution::Committed,
                )
                .await?;
            Ok(TransactionOutcome::Committed(commit_time))
        })
    }

    pub fn abort<'a>(
        &'a self,
        context: &'a TransactionContext,
        mut participants: Vec<ParticipantWrite>,
    ) -> TxnFuture<'a, TransactionOutcome> {
        Box::pin(async move {
            context.overlay.validate(&context.base)?;
            validate_participants(context, &participants)?;
            participants.sort_by_key(ParticipantWrite::shard_id);

            let pending_commit_time = match self.prevalidate_abort_authority(context).await? {
                AbortPrevalidation::Proceed(pending_commit_time) => pending_commit_time,
                AbortPrevalidation::Terminal(outcome) => return Ok(outcome),
            };

            let mut intents = Vec::with_capacity(participants.len());
            for participant in &participants {
                let fence = context.snapshot.shards[&participant.shard_id()];
                let mutations =
                    stamp_mutations(participant.mutations(), context.snapshot.start_time)?;
                let request = ShardRequest::PrewriteIntent {
                    header: ShardRequestHeader::new(
                        command_id(
                            context.snapshot.transaction_id,
                            b"prewrite-abort",
                            participant.shard_id(),
                        )?,
                        fence.placement_epoch,
                        fence.backend_generation,
                    ),
                    transaction_id: context.snapshot.transaction_id,
                    start_time: context.snapshot.start_time,
                    snapshot_applied_index: fence.applied_index,
                    mutations,
                };
                let receipt = match self
                    .participants
                    .submit(participant.shard_id(), request)
                    .await
                {
                    Ok(receipt) => receipt,
                    Err(_) => return self.resolve_failed_prewrite(context).await,
                };
                let Some(intent_digest) = receipt.intent_digest() else {
                    return self.resolve_failed_prewrite(context).await;
                };
                intents.push((participant.shard_id(), intent_digest));
            }

            let home = participants[0].shard_id();
            let home_fence = context.snapshot.shards[&home];
            let manifest = transaction_manifest(context, intents.iter().copied())?;
            let digest =
                manifest.decision_digest(TransactionState::Aborted, context.snapshot.start_time)?;
            let decision = TransactionRecord::new(
                context.snapshot.transaction_id,
                TransactionState::Aborted,
                context.snapshot.start_time,
                digest,
            )?;
            let request = ShardRequest::RecordHomeDecision {
                header: ShardRequestHeader::new(
                    command_id(context.snapshot.transaction_id, b"decision", home)?,
                    home_fence.placement_epoch,
                    home_fence.backend_generation,
                ),
                decision,
                manifest,
            };
            self.participants
                .submit(home, request)
                .await
                .map_err(SubmissionFailure::into_error)?;

            for (shard_id, intent_digest) in intents {
                let fence = context.snapshot.shards[&shard_id];
                let terminal = TransactionRecord::new(
                    context.snapshot.transaction_id,
                    TransactionState::Aborted,
                    context.snapshot.start_time,
                    intent_digest,
                )?;
                let request = ShardRequest::FinalizeParticipantAbort {
                    header: ShardRequestHeader::new(
                        command_id(context.snapshot.transaction_id, b"finalize-abort", shard_id)?,
                        fence.placement_epoch,
                        fence.backend_generation,
                    ),
                    terminal,
                };
                self.participants
                    .submit(shard_id, request)
                    .await
                    .map_err(SubmissionFailure::into_error)?;
            }
            if let Some(commit_time) = pending_commit_time
                && self
                    .timestamps
                    .resolve_commit_time(
                        context.snapshot.transaction_id,
                        commit_time,
                        CommitResolution::Aborted,
                    )
                    .await
                    .is_err()
            {
                return Ok(TransactionOutcome::Unresolved);
            }
            Ok(TransactionOutcome::Aborted)
        })
    }
}

pub(crate) fn transaction_manifest(
    context: &TransactionContext,
    participants: impl Iterator<Item = (ShardId, Digest32)>,
) -> Result<DurableTransactionManifest, TxnError> {
    DurableTransactionManifest::new(
        context.snapshot.transaction_id,
        context.snapshot.start_time,
        context.snapshot.catalog_version,
        participants
            .map(|(shard_id, intent_digest)| {
                let fence = context
                    .snapshot
                    .shards
                    .get(&shard_id)
                    .copied()
                    .ok_or(TxnError::IncompleteSnapshot)?;
                DurableParticipantManifest::new(shard_id, fence, intent_digest)
            })
            .collect::<Result<Vec<_>, _>>()?,
    )
}

fn validate_participants(
    context: &TransactionContext,
    participants: &[ParticipantWrite],
) -> Result<(), TxnError> {
    if participants
        .iter()
        .map(ParticipantWrite::mutations)
        .map(<[LogicalMutation]>::len)
        .sum::<usize>()
        != context.overlay.mutations().len()
    {
        return Err(TxnError::ParticipantsMismatch);
    }
    let mut unmatched: Vec<_> = context.overlay.mutations().iter().collect();
    for mutation in participants.iter().flat_map(ParticipantWrite::mutations) {
        let Some(index) = unmatched
            .iter()
            .position(|candidate| *candidate == mutation)
        else {
            return Err(TxnError::ParticipantsMismatch);
        };
        unmatched.swap_remove(index);
    }
    if !unmatched.is_empty() {
        return Err(TxnError::ParticipantsMismatch);
    }
    let shard_ids: BTreeSet<_> = participants
        .iter()
        .map(ParticipantWrite::shard_id)
        .collect();
    if shard_ids.len() != participants.len()
        || !shard_ids
            .iter()
            .all(|shard_id| context.snapshot.shards.contains_key(shard_id))
        || (participants.is_empty() != context.overlay.mutations().is_empty())
    {
        return Err(TxnError::ParticipantsMismatch);
    }
    Ok(())
}

pub(crate) fn stamp_mutations(
    mutations: &[LogicalMutation],
    commit_time: TransactionTime,
) -> Result<Vec<LogicalMutation>, TxnError> {
    mutations
        .iter()
        .map(|mutation| match mutation {
            LogicalMutation::PutVertex(vertex) => {
                Ok(LogicalMutation::PutVertex(VertexVersion::new(
                    vertex.id(),
                    vertex.version(),
                    vertex.valid_time(),
                    commit_time,
                    vertex.properties().clone(),
                )?))
            }
            LogicalMutation::DeleteVertex(vertex) => Ok(LogicalMutation::DeleteVertex(
                VertexTombstone::new(vertex.id(), vertex.version(), commit_time),
            )),
            LogicalMutation::PutEdge(edge) => Ok(LogicalMutation::PutEdge(EdgeVersion::new(
                edge.id(),
                edge.source(),
                edge.target(),
                edge.edge_type(),
                edge.version(),
                edge.valid_time(),
                commit_time,
                edge.properties().clone(),
            )?)),
            LogicalMutation::DeleteEdge(edge) => Ok(LogicalMutation::DeleteEdge(
                EdgeTombstone::new(edge.id(), edge.version(), commit_time),
            )),
            LogicalMutation::PutTransaction(_) | LogicalMutation::PutReplicaMetadata(_) => {
                Err(TxnError::InvalidMutation)
            }
        })
        .collect()
}

pub(crate) fn command_id(
    transaction_id: TransactionId,
    phase: &[u8],
    shard_id: ShardId,
) -> Result<CommandId, TxnError> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"dtg-transaction-command-id-v1");
    hasher.update(&transaction_id.get().to_be_bytes());
    hasher.update(&(phase.len() as u64).to_be_bytes());
    hasher.update(phase);
    hasher.update(&shard_id.get().to_be_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    let value = u128::from_be_bytes(bytes).max(1);
    Ok(CommandId::new(value)?)
}

pub(crate) fn decision_digest(
    transaction_id: TransactionId,
    commit_time: TransactionTime,
    intents: impl Iterator<Item = (ShardId, Digest32)>,
) -> Digest32 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"dtg-transaction-home-decision-v1");
    hasher.update(&transaction_id.get().to_be_bytes());
    hasher.update(&commit_time.get().to_be_bytes());
    for (shard_id, digest) in intents {
        hasher.update(&shard_id.get().to_be_bytes());
        hasher.update(&digest.get());
    }
    Digest32::new(*hasher.finalize().as_bytes())
}

pub(crate) fn abort_decision_digest(
    transaction_id: TransactionId,
    start_time: TransactionTime,
    shard_ids: impl Iterator<Item = ShardId>,
) -> Digest32 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"dtg-transaction-home-abort-v1");
    hasher.update(&transaction_id.get().to_be_bytes());
    hasher.update(&start_time.get().to_be_bytes());
    for shard_id in shard_ids {
        hasher.update(&shard_id.get().to_be_bytes());
    }
    Digest32::new(*hasher.finalize().as_bytes())
}
