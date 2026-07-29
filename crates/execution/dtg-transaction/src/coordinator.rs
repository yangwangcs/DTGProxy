use std::collections::BTreeSet;
use std::sync::Arc;

use dtg_kernel::{Digest32, ShardId, TransactionId, TransactionTime, Version};
use dtg_storage::{
    CommandId, EdgeTombstone, EdgeVersion, LogicalMutation, TransactionRecord, TransactionState,
    VertexTombstone, VertexVersion,
};

use crate::{
    BaseGraphSnapshot, ParticipantService, ParticipantWrite, ShardRequest, ShardRequestHeader,
    ShardSnapshotFence, SnapshotToken, TransactionOverlay, TxnError, TxnFuture,
    conflict::detect_mutation_conflicts,
};

pub trait TimestampAuthority: Send + Sync {
    fn allocate_start_time(&self, transaction_id: TransactionId) -> TxnFuture<'_, TransactionTime>;

    fn allocate_commit_time(&self, transaction_id: TransactionId)
    -> TxnFuture<'_, TransactionTime>;

    fn publish_commit_time(
        &self,
        transaction_id: TransactionId,
        commit_time: TransactionTime,
    ) -> TxnFuture<'_, ()>;
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

            let commit_time = self
                .timestamps
                .allocate_commit_time(context.snapshot.transaction_id)
                .await?;
            if commit_time <= context.snapshot.start_time {
                return Err(TxnError::InconsistentSnapshot);
            }

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
                let intended = stamp_mutations(participant.mutations(), commit_time)?;
                let committed: Vec<_> = changes
                    .iter()
                    .map(|change| change.mutation().clone())
                    .filter(|mutation| !intended.contains(mutation))
                    .collect();
                detect_mutation_conflicts(
                    participant.mutations(),
                    &committed,
                    context.snapshot.start_time,
                )?;
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
                    mutations: stamp_mutations(participant.mutations(), commit_time)?,
                };
                self.participants
                    .submit(participant.shard_id(), request)
                    .await?;
                self.timestamps
                    .publish_commit_time(context.snapshot.transaction_id, commit_time)
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
                    mutations: mutations.clone(),
                };
                let receipt = self
                    .participants
                    .submit(participant.shard_id(), request)
                    .await?;
                let digest = receipt.intent_digest().ok_or(TxnError::CorruptRecovery)?;
                intents.push((participant, mutations, digest));
            }

            let home = participants[0].shard_id();
            let home_fence = context.snapshot.shards[&home];
            let decision_digest = decision_digest(
                context.snapshot.transaction_id,
                commit_time,
                intents
                    .iter()
                    .map(|(participant, _, digest)| (participant.shard_id(), *digest)),
            );
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
            };
            self.participants.submit(home, request).await?;

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
                    commit_time,
                    intent_digest,
                    mutations,
                };
                self.participants
                    .submit(participant.shard_id(), request)
                    .await?;
            }

            self.timestamps
                .publish_commit_time(context.snapshot.transaction_id, commit_time)
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
                    mutations,
                };
                let receipt = self
                    .participants
                    .submit(participant.shard_id(), request)
                    .await?;
                intents.push((
                    participant.shard_id(),
                    receipt.intent_digest().ok_or(TxnError::CorruptRecovery)?,
                ));
            }

            let home = participants[0].shard_id();
            let home_fence = context.snapshot.shards[&home];
            let digest = abort_decision_digest(
                context.snapshot.transaction_id,
                context.snapshot.start_time,
                participants.iter().map(ParticipantWrite::shard_id),
            );
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
            };
            self.participants.submit(home, request).await?;

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
                self.participants.submit(shard_id, request).await?;
            }
            Ok(TransactionOutcome::Aborted)
        })
    }
}

fn validate_participants(
    context: &TransactionContext,
    participants: &[ParticipantWrite],
) -> Result<(), TxnError> {
    if participants.is_empty()
        || participants
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
        || shard_ids != context.snapshot.shards.keys().copied().collect()
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
