use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};

use raft_command::{
    AbortIntentV1, CommandBodyV1, CommandCodecError, CommandEnvelopeV1, FinalizeV1,
    OnePhaseCommitV1, PrewriteV1, RecordDecisionV1,
};
use shard_runtime::ReadBarrierError;
use shard_runtime::ReplicationError;
use storage_api::{
    AdapterError, LogicalKey, Mutation, MutationOperation, PreparedMutationBatch, StorageAdapter,
};
use temporal_ir::GraphScope;
use temporal_storage::{
    PrepareContext, TemporalStore, TemporalStoreError, TemporalTransaction, decode_graph_key,
    graph_key_scope,
};
use temporal_types::TransactionTime;
use timestamp_oracle::{TimestampOracle, TimestampOracleError};
use txn_protocol::{
    HomeDecisionEngine, HomeTransactionRecord, IsolationLevel, ParticipantEngine, ParticipantProof,
    PrewriteRequest, ShardEpoch, TransactionId, TransactionState, TxnProtocolError,
};

use crate::InProcessDeploymentRuntime;

const SINGLE_SHARD_PHASE: u8 = 1;
const PREWRITE_PHASE: u8 = 2;
const COMMIT_DECISION_PHASE: u8 = 3;
const FINALIZE_PHASE: u8 = 4;
const ABORT_DECISION_PHASE: u8 = 5;
const ABORT_INTENT_PHASE: u8 = 6;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransactionContext {
    transaction_id: TransactionId,
    start_ts: TransactionTime,
    commit_ts: TransactionTime,
    expires_at: TransactionTime,
    schema_version: u64,
    isolation: IsolationLevel,
}

impl TransactionContext {
    #[must_use]
    pub const fn transaction_id(self) -> TransactionId {
        self.transaction_id
    }

    #[must_use]
    pub const fn start_ts(self) -> TransactionTime {
        self.start_ts
    }

    #[must_use]
    pub const fn commit_ts(self) -> TransactionTime {
        self.commit_ts
    }

    #[must_use]
    pub const fn expires_at(self) -> TransactionTime {
        self.expires_at
    }

    #[must_use]
    pub const fn schema_version(self) -> u64 {
        self.schema_version
    }

    #[must_use]
    pub const fn isolation(self) -> IsolationLevel {
        self.isolation
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedShardTransaction {
    participant: ShardEpoch,
    batch: PreparedMutationBatch,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScopedTemporalTransaction {
    scope: GraphScope,
    transaction: TemporalTransaction,
}

impl ScopedTemporalTransaction {
    #[must_use]
    pub const fn new(scope: GraphScope, transaction: TemporalTransaction) -> Self {
        Self { scope, transaction }
    }

    #[must_use]
    pub const fn scope(&self) -> GraphScope {
        self.scope
    }

    #[must_use]
    pub const fn transaction(&self) -> &TemporalTransaction {
        &self.transaction
    }
}

impl PreparedShardTransaction {
    pub fn new(
        shard_id: u32,
        placement_epoch: u64,
        batch: PreparedMutationBatch,
    ) -> Result<Self, TransactionCoordinatorError> {
        if batch.shard_id != shard_id {
            return Err(TransactionCoordinatorError::BatchShardMismatch {
                participant: shard_id,
                batch: batch.shard_id,
            });
        }
        Ok(Self {
            participant: ShardEpoch::new(shard_id, placement_epoch)?,
            batch,
        })
    }

    #[must_use]
    pub const fn participant(&self) -> ShardEpoch {
        self.participant
    }

    #[must_use]
    pub const fn batch(&self) -> &PreparedMutationBatch {
        &self.batch
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransactionReceipt {
    transaction_id: TransactionId,
    start_ts: TransactionTime,
    commit_ts: TransactionTime,
    home: ShardEpoch,
    participants: Vec<ShardEpoch>,
    single_shard_fast_path: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransactionStatus {
    Unknown,
    Preparing,
    Committed { commit_ts: TransactionTime },
    Aborted,
}

impl TransactionReceipt {
    #[must_use]
    pub const fn transaction_id(&self) -> TransactionId {
        self.transaction_id
    }

    #[must_use]
    pub const fn start_ts(&self) -> TransactionTime {
        self.start_ts
    }

    #[must_use]
    pub const fn commit_ts(&self) -> TransactionTime {
        self.commit_ts
    }

    #[must_use]
    pub const fn home(&self) -> ShardEpoch {
        self.home
    }

    #[must_use]
    pub fn participants(&self) -> &[ShardEpoch] {
        &self.participants
    }

    #[must_use]
    pub const fn single_shard_fast_path(&self) -> bool {
        self.single_shard_fast_path
    }
}

pub struct TransactionCoordinator<'oracle> {
    oracle: &'oracle TimestampOracle,
    max_ticks: usize,
}

impl<'oracle> TransactionCoordinator<'oracle> {
    #[must_use]
    pub const fn new(oracle: &'oracle TimestampOracle, max_ticks: usize) -> Self {
        Self { oracle, max_ticks }
    }

    pub fn begin(
        &self,
        schema_version: u64,
        isolation: IsolationLevel,
        ttl_micros: u64,
    ) -> Result<TransactionContext, TransactionCoordinatorError> {
        if schema_version == 0 {
            return Err(TransactionCoordinatorError::InvalidSchemaVersion);
        }
        if ttl_micros == 0 {
            return Err(TransactionCoordinatorError::InvalidTransactionTtl);
        }
        let start_ts = self.oracle.next()?;
        let proof_floor = self.oracle.next_after(start_ts)?;
        let commit_ts = self.oracle.next_after(proof_floor)?;
        let ttl_micros =
            i64::try_from(ttl_micros).map_err(|_| TransactionCoordinatorError::ExpiryOverflow)?;
        let expires_at = TransactionTime::new(
            start_ts
                .physical_micros()
                .checked_add(ttl_micros)
                .ok_or(TransactionCoordinatorError::ExpiryOverflow)?,
            0,
        );
        Ok(TransactionContext {
            transaction_id: transaction_id_from_time(start_ts),
            start_ts,
            commit_ts,
            expires_at,
            schema_version,
            isolation,
        })
    }

    pub async fn commit(
        &self,
        runtime: &mut InProcessDeploymentRuntime,
        context: TransactionContext,
        mut writes: Vec<PreparedShardTransaction>,
    ) -> Result<TransactionReceipt, TransactionCoordinatorError> {
        if writes.is_empty() {
            return Err(TransactionCoordinatorError::EmptyWriteSet);
        }
        writes.sort_by_key(PreparedShardTransaction::participant);
        if writes
            .windows(2)
            .any(|pair| pair[0].participant == pair[1].participant)
        {
            return Err(TransactionCoordinatorError::DuplicateParticipant);
        }
        for write in &writes {
            self.validate_write(runtime, context, write)?;
        }
        let participants = writes
            .iter()
            .map(PreparedShardTransaction::participant)
            .collect::<Vec<_>>();
        let home = participants[0];
        if writes.len() == 1 {
            let write = writes.pop().expect("write set has exactly one element");
            let request = PrewriteRequest::new(
                context.transaction_id,
                context.start_ts,
                context.schema_version,
                write.participant,
                home,
                participants.clone(),
                context.isolation,
                context.expires_at,
                write.batch,
            )?;
            let proof = ParticipantProof::new(
                write.participant,
                timestamp_successor(context.start_ts)?,
                request.intent_digest(),
            );
            let command = CommandEnvelopeV1::new(
                write.participant.shard_id(),
                write.participant.placement_epoch(),
                phase_request_id(
                    context.transaction_id,
                    SINGLE_SHARD_PHASE,
                    write.participant,
                ),
                CommandBodyV1::OnePhaseCommit(OnePhaseCommitV1 {
                    request,
                    expected_proof: proof,
                    commit_ts: context.commit_ts,
                }),
            )
            .encode()?;
            runtime
                .propose_shard(write.participant.shard_id(), command, self.max_ticks)
                .await
                .map_err(|source| TransactionCoordinatorError::Replication {
                    phase: "single-shard-commit",
                    participant: write.participant,
                    source,
                })?;
            return Ok(receipt(context, home, participants, true));
        }

        let requests = writes
            .into_iter()
            .map(|write| {
                PrewriteRequest::new(
                    context.transaction_id,
                    context.start_ts,
                    context.schema_version,
                    write.participant,
                    home,
                    participants.clone(),
                    context.isolation,
                    context.expires_at,
                    write.batch,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let proofs = requests
            .iter()
            .map(|request| {
                Ok(ParticipantProof::new(
                    request.participant(),
                    timestamp_successor(context.start_ts)?,
                    request.intent_digest(),
                ))
            })
            .collect::<Result<Vec<_>, TransactionCoordinatorError>>()?;
        let prewrite_commands = requests
            .iter()
            .zip(&proofs)
            .map(|(request, proof)| {
                Ok((
                    request.participant().shard_id(),
                    CommandEnvelopeV1::new(
                        request.participant().shard_id(),
                        request.participant().placement_epoch(),
                        phase_request_id(
                            context.transaction_id,
                            PREWRITE_PHASE,
                            request.participant(),
                        ),
                        CommandBodyV1::Prewrite(PrewriteV1 {
                            request: request.clone(),
                            expected_proof: proof.clone(),
                        }),
                    )
                    .encode()?,
                ))
            })
            .collect::<Result<Vec<_>, TransactionCoordinatorError>>()?;
        let prewrite_results = runtime
            .propose_shards(prewrite_commands, self.max_ticks)
            .await?;
        let mut prepared = Vec::with_capacity(requests.len());
        let mut failed = Vec::new();
        let mut first_failure = None;
        for (shard_id, result) in prewrite_results {
            let index = requests
                .binary_search_by_key(&shard_id, |request| request.participant().shard_id())
                .expect("fan-out result belongs to a validated transaction participant");
            match result {
                Ok(_) => prepared.push((requests[index].clone(), proofs[index].clone())),
                Err(error) => {
                    failed.push(requests[index].participant());
                    if first_failure.is_none() {
                        first_failure = Some((requests[index].participant(), error));
                    }
                }
            }
        }
        if let Some((participant, source)) = first_failure {
            let (abort_decision_durable, mut cleanup_pending) = self
                .rollback_prepared(runtime, context, home, &participants, &prepared)
                .await;
            cleanup_pending.extend(failed);
            cleanup_pending.sort_unstable();
            cleanup_pending.dedup();
            return Err(TransactionCoordinatorError::PrewriteFailed {
                participant,
                abort_decision_durable,
                cleanup_pending,
                source,
            });
        }
        if proofs
            .iter()
            .any(|proof| context.commit_ts <= proof.min_commit_ts())
        {
            return Err(TransactionCoordinatorError::CommitTimestampTooEarly);
        }

        let decision = HomeTransactionRecord::new(
            context.transaction_id,
            context.start_ts,
            TransactionState::Committed,
            Some(context.commit_ts),
            participants.clone(),
            proofs,
        )?;
        let command = CommandEnvelopeV1::new(
            home.shard_id(),
            home.placement_epoch(),
            phase_request_id(context.transaction_id, COMMIT_DECISION_PHASE, home),
            CommandBodyV1::RecordDecision(RecordDecisionV1 { home, decision }),
        )
        .encode()?;
        if let Err(source) = runtime
            .propose_shard(home.shard_id(), command, self.max_ticks)
            .await
        {
            return Err(TransactionCoordinatorError::DecisionUnknown {
                transaction_id: context.transaction_id,
                home,
                source,
            });
        }

        let finalize_commands = requests
            .iter()
            .map(|request| {
                Ok((
                    request.participant().shard_id(),
                    CommandEnvelopeV1::new(
                        request.participant().shard_id(),
                        request.participant().placement_epoch(),
                        phase_request_id(
                            context.transaction_id,
                            FINALIZE_PHASE,
                            request.participant(),
                        ),
                        CommandBodyV1::Finalize(FinalizeV1 {
                            participant: request.participant(),
                            transaction_id: context.transaction_id,
                            intent_digest: request.intent_digest(),
                            commit_ts: context.commit_ts,
                        }),
                    )
                    .encode()?,
                ))
            })
            .collect::<Result<Vec<_>, TransactionCoordinatorError>>()?;
        let finalize_results = runtime
            .propose_shards(finalize_commands, self.max_ticks)
            .await?;
        let mut pending = Vec::new();
        let mut first_error = None;
        for (shard_id, result) in finalize_results {
            if let Err(error) = result {
                let participant = participants
                    .iter()
                    .find(|participant| participant.shard_id() == shard_id)
                    .copied()
                    .expect("fan-out result belongs to a validated transaction participant");
                pending.push(participant);
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }
        if let Some(source) = first_error {
            return Err(TransactionCoordinatorError::CommittedPendingApply {
                transaction_id: context.transaction_id,
                commit_ts: context.commit_ts,
                pending,
                source,
            });
        }
        Ok(receipt(context, home, participants, false))
    }

    pub async fn commit_temporal(
        &self,
        runtime: &mut InProcessDeploymentRuntime,
        schema_version: u64,
        isolation: IsolationLevel,
        ttl_micros: u64,
        transactions: Vec<ScopedTemporalTransaction>,
    ) -> Result<TransactionReceipt, TransactionCoordinatorError> {
        if transactions.is_empty() {
            return Err(TransactionCoordinatorError::EmptyWriteSet);
        }
        let context = self.begin(schema_version, isolation, ttl_micros)?;
        let mut grouped = BTreeMap::<ShardEpoch, TemporalTransaction>::new();
        let mut endpoint_guards = Vec::new();
        for scoped in transactions {
            if !scoped
                .transaction
                .is_scoped_to(scoped.scope.graph(), scoped.scope.partition())
            {
                return Err(TransactionCoordinatorError::ScopeMismatch);
            }
            let placement = runtime.config().route_scope(scoped.scope);
            let participant = ShardEpoch::new(placement.shard_id(), placement.placement_epoch())?;
            endpoint_guards.extend(scoped.transaction.remote_endpoint_guards());
            grouped
                .entry(participant)
                .or_default()
                .extend(scoped.transaction);
        }

        let mut routed = BTreeMap::<ShardEpoch, BTreeMap<LogicalKey, MutationOperation>>::new();
        for (participant, transaction) in grouped {
            let group = runtime.raft_mut().group_mut(participant.shard_id())?;
            let leader_id = group
                .leader_id()
                .ok_or(TransactionCoordinatorError::NoLeader {
                    shard_id: participant.shard_id(),
                })?;
            let permit = group
                .leader_read_permit(leader_id, participant.placement_epoch(), self.max_ticks)
                .await?;
            let adapter = group.replica_adapter(permit.node_id()).ok_or(
                TransactionCoordinatorError::NoLeader {
                    shard_id: participant.shard_id(),
                },
            )?;
            let batch = TemporalStore::new(adapter)
                .prepare_transaction(
                    PrepareContext::new(
                        participant.shard_id(),
                        context.transaction_id.value(),
                        context.start_ts,
                        context.commit_ts,
                    ),
                    transaction,
                )
                .await?;
            for mutation in batch.mutations {
                let destination = route_operation(runtime, &mutation.operation)?;
                insert_routed_operation(&mut routed, destination, mutation.operation)?;
            }
        }
        for guard in endpoint_guards {
            let scope = GraphScope::new(guard.vertex().graph(), guard.vertex().partition());
            let placement = runtime.config().route_scope(scope);
            let participant = ShardEpoch::new(placement.shard_id(), placement.placement_epoch())?;
            let group = runtime.raft_mut().group_mut(participant.shard_id())?;
            let leader_id = group
                .leader_id()
                .ok_or(TransactionCoordinatorError::NoLeader {
                    shard_id: participant.shard_id(),
                })?;
            let permit = group
                .leader_read_permit(leader_id, participant.placement_epoch(), self.max_ticks)
                .await?;
            let adapter = group.replica_adapter(permit.node_id()).ok_or(
                TransactionCoordinatorError::NoLeader {
                    shard_id: participant.shard_id(),
                },
            )?;
            let operation = TemporalStore::new(adapter)
                .prepare_endpoint_guard(context.start_ts, guard.vertex(), guard.valid())
                .await?;
            insert_routed_operation(&mut routed, participant, operation)?;
        }

        let writes = routed
            .into_iter()
            .map(|(participant, operations)| {
                let mutations = operations
                    .into_values()
                    .enumerate()
                    .map(|(sequence, operation)| {
                        let sequence = u32::try_from(sequence)
                            .map_err(|_| TransactionCoordinatorError::TooManyRoutedMutations)?;
                        Ok(match operation {
                            MutationOperation::Put { key, value } => {
                                Mutation::put(sequence, key, value)
                            }
                            MutationOperation::Delete { key } => Mutation::delete(sequence, key),
                        })
                    })
                    .collect::<Result<Vec<_>, TransactionCoordinatorError>>()?;
                Ok(PreparedShardTransaction {
                    participant,
                    batch: PreparedMutationBatch {
                        shard_id: participant.shard_id(),
                        txn_id: context.transaction_id.value(),
                        mutations,
                    },
                })
            })
            .collect::<Result<Vec<_>, TransactionCoordinatorError>>()?;
        self.commit(runtime, context, writes).await
    }

    pub async fn status(
        &self,
        runtime: &mut InProcessDeploymentRuntime,
        home: ShardEpoch,
        transaction_id: TransactionId,
    ) -> Result<TransactionStatus, TransactionCoordinatorError> {
        let group = runtime.raft_mut().group_mut(home.shard_id())?;
        let leader_id = group
            .leader_id()
            .ok_or(TransactionCoordinatorError::NoLeader {
                shard_id: home.shard_id(),
            })?;
        let permit = group
            .leader_read_permit(leader_id, home.placement_epoch(), self.max_ticks)
            .await?;
        let adapter = group.replica_adapter(permit.node_id()).ok_or(
            TransactionCoordinatorError::NoLeader {
                shard_id: home.shard_id(),
            },
        )?;
        let keys = [
            HomeDecisionEngine::inspection_key(home, transaction_id)?,
            ParticipantEngine::participant_record_key(home, transaction_id)?,
        ];
        let values = adapter.multi_get(&keys).await?;
        if let Some(bytes) = values[0].as_deref() {
            let decision = HomeTransactionRecord::decode(bytes)?;
            return Ok(match decision.state() {
                TransactionState::Committed | TransactionState::Applied => {
                    TransactionStatus::Committed {
                        commit_ts: decision
                            .commit_ts()
                            .expect("validated committed Home record has a timestamp"),
                    }
                }
                TransactionState::Aborted | TransactionState::Cleaned => TransactionStatus::Aborted,
                TransactionState::Active | TransactionState::Preparing => {
                    TransactionStatus::Preparing
                }
            });
        }
        if let Some(bytes) = values[1].as_deref() {
            let participant = ParticipantEngine::participant_record_status(bytes)?;
            return Ok(match participant.state() {
                TransactionState::Applied => TransactionStatus::Committed {
                    commit_ts: participant
                        .commit_ts()
                        .expect("validated applied participant has a commit timestamp"),
                },
                TransactionState::Aborted | TransactionState::Cleaned => TransactionStatus::Aborted,
                TransactionState::Active
                | TransactionState::Preparing
                | TransactionState::Committed => TransactionStatus::Preparing,
            });
        }
        Ok(TransactionStatus::Unknown)
    }

    fn validate_write(
        &self,
        runtime: &InProcessDeploymentRuntime,
        context: TransactionContext,
        write: &PreparedShardTransaction,
    ) -> Result<(), TransactionCoordinatorError> {
        let Some(placement) = runtime
            .config()
            .all_shards()
            .iter()
            .find(|placement| placement.shard_id() == write.participant.shard_id())
        else {
            return Err(TransactionCoordinatorError::UnknownParticipant {
                shard_id: write.participant.shard_id(),
            });
        };
        if placement.placement_epoch() != write.participant.placement_epoch() {
            return Err(TransactionCoordinatorError::StalePlacementEpoch {
                shard_id: write.participant.shard_id(),
                expected: placement.placement_epoch(),
                actual: write.participant.placement_epoch(),
            });
        }
        if write.batch.txn_id != context.transaction_id.value() {
            return Err(TransactionCoordinatorError::BatchTransactionMismatch);
        }
        Ok(())
    }

    async fn rollback_prepared(
        &self,
        runtime: &mut InProcessDeploymentRuntime,
        context: TransactionContext,
        home: ShardEpoch,
        participants: &[ShardEpoch],
        prepared: &[(PrewriteRequest, ParticipantProof)],
    ) -> (bool, Vec<ShardEpoch>) {
        let decision = HomeTransactionRecord::new(
            context.transaction_id,
            context.start_ts,
            TransactionState::Aborted,
            None,
            participants.to_vec(),
            prepared.iter().map(|(_, proof)| proof.clone()).collect(),
        );
        let abort_decision_durable = if let Ok(decision) = decision {
            match CommandEnvelopeV1::new(
                home.shard_id(),
                home.placement_epoch(),
                phase_request_id(context.transaction_id, ABORT_DECISION_PHASE, home),
                CommandBodyV1::RecordDecision(RecordDecisionV1 { home, decision }),
            )
            .encode()
            {
                Ok(command) => runtime
                    .propose_shard(home.shard_id(), command, self.max_ticks)
                    .await
                    .is_ok(),
                Err(_) => false,
            }
        } else {
            false
        };
        let mut pending = Vec::new();
        let mut abort_commands = Vec::with_capacity(prepared.len());
        for (request, _) in prepared {
            match CommandEnvelopeV1::new(
                request.participant().shard_id(),
                request.participant().placement_epoch(),
                phase_request_id(
                    context.transaction_id,
                    ABORT_INTENT_PHASE,
                    request.participant(),
                ),
                CommandBodyV1::AbortIntent(AbortIntentV1 {
                    participant: request.participant(),
                    transaction_id: context.transaction_id,
                    intent_digest: request.intent_digest(),
                }),
            )
            .encode()
            {
                Ok(command) => abort_commands.push((request.participant().shard_id(), command)),
                Err(_) => pending.push(request.participant()),
            }
        }
        match runtime.propose_shards(abort_commands, self.max_ticks).await {
            Ok(results) => {
                for (shard_id, result) in results {
                    if result.is_err() {
                        let participant = prepared
                            .iter()
                            .map(|(request, _)| request.participant())
                            .find(|participant| participant.shard_id() == shard_id)
                            .expect("abort result belongs to a prepared participant");
                        pending.push(participant);
                    }
                }
            }
            Err(_) => pending.extend(prepared.iter().map(|(request, _)| request.participant())),
        }
        (abort_decision_durable, pending)
    }
}

fn receipt(
    context: TransactionContext,
    home: ShardEpoch,
    participants: Vec<ShardEpoch>,
    single_shard_fast_path: bool,
) -> TransactionReceipt {
    TransactionReceipt {
        transaction_id: context.transaction_id,
        start_ts: context.start_ts,
        commit_ts: context.commit_ts,
        home,
        participants,
        single_shard_fast_path,
    }
}

fn route_operation(
    runtime: &InProcessDeploymentRuntime,
    operation: &MutationOperation,
) -> Result<ShardEpoch, TransactionCoordinatorError> {
    let key = operation_key(operation);
    let graph_key = decode_graph_key(key).map_err(TemporalStoreError::from)?;
    let (graph, partition) = graph_key_scope(graph_key);
    let placement = runtime
        .config()
        .route_scope(GraphScope::new(graph, partition));
    Ok(ShardEpoch::new(
        placement.shard_id(),
        placement.placement_epoch(),
    )?)
}

fn insert_routed_operation(
    routed: &mut BTreeMap<ShardEpoch, BTreeMap<LogicalKey, MutationOperation>>,
    participant: ShardEpoch,
    operation: MutationOperation,
) -> Result<(), TransactionCoordinatorError> {
    let key = operation_key(&operation).clone();
    match routed.entry(participant).or_default().entry(key.clone()) {
        std::collections::btree_map::Entry::Vacant(entry) => {
            entry.insert(operation);
            Ok(())
        }
        std::collections::btree_map::Entry::Occupied(entry) if entry.get() == &operation => Ok(()),
        std::collections::btree_map::Entry::Occupied(_) => {
            Err(TransactionCoordinatorError::ConflictingRoutedMutation { key })
        }
    }
}

fn operation_key(operation: &MutationOperation) -> &LogicalKey {
    match operation {
        MutationOperation::Put { key, .. } | MutationOperation::Delete { key } => key,
    }
}

fn transaction_id_from_time(timestamp: TransactionTime) -> TransactionId {
    let physical_offset = u128::from(
        u64::try_from(i128::from(timestamp.physical_micros()) - i128::from(i64::MIN))
            .expect("i64 timestamp offset fits u64"),
    );
    let ordinal = (physical_offset << 32) | u128::from(timestamp.logical());
    TransactionId::new(ordinal + 1)
}

fn timestamp_successor(
    timestamp: TransactionTime,
) -> Result<TransactionTime, TransactionCoordinatorError> {
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
            .ok_or(TransactionCoordinatorError::TimestampExhausted)?,
        0,
    ))
}

fn phase_request_id(transaction_id: TransactionId, phase: u8, participant: ShardEpoch) -> u128 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"DTGProxy/TransactionPhaseRequest/V1");
    hasher.update(&transaction_id.value().to_be_bytes());
    hasher.update(&[phase]);
    hasher.update(&participant.shard_id().to_be_bytes());
    hasher.update(&participant.placement_epoch().to_be_bytes());
    let digest = hasher.finalize();
    let request_id = u128::from_be_bytes(
        digest.as_bytes()[..16]
            .try_into()
            .expect("BLAKE3 digest has at least 16 bytes"),
    );
    request_id.max(1)
}

#[derive(Debug)]
pub enum TransactionCoordinatorError {
    Timestamp(TimestampOracleError),
    Protocol(TxnProtocolError),
    Command(CommandCodecError),
    Adapter(AdapterError),
    Runtime(ReplicationError),
    Barrier(ReadBarrierError),
    Temporal(TemporalStoreError),
    InvalidSchemaVersion,
    InvalidTransactionTtl,
    ExpiryOverflow,
    TimestampExhausted,
    EmptyWriteSet,
    ScopeMismatch,
    NoLeader {
        shard_id: u32,
    },
    DuplicateParticipant,
    UnknownParticipant {
        shard_id: u32,
    },
    StalePlacementEpoch {
        shard_id: u32,
        expected: u64,
        actual: u64,
    },
    BatchShardMismatch {
        participant: u32,
        batch: u32,
    },
    BatchTransactionMismatch,
    TooManyRoutedMutations,
    ConflictingRoutedMutation {
        key: LogicalKey,
    },
    CommitTimestampTooEarly,
    Replication {
        phase: &'static str,
        participant: ShardEpoch,
        source: ReplicationError,
    },
    PrewriteFailed {
        participant: ShardEpoch,
        abort_decision_durable: bool,
        cleanup_pending: Vec<ShardEpoch>,
        source: ReplicationError,
    },
    DecisionUnknown {
        transaction_id: TransactionId,
        home: ShardEpoch,
        source: ReplicationError,
    },
    CommittedPendingApply {
        transaction_id: TransactionId,
        commit_ts: TransactionTime,
        pending: Vec<ShardEpoch>,
        source: ReplicationError,
    },
}

impl Display for TransactionCoordinatorError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Timestamp(error) => Display::fmt(error, formatter),
            Self::Protocol(error) => Display::fmt(error, formatter),
            Self::Command(error) => Display::fmt(error, formatter),
            Self::Adapter(error) => Display::fmt(error, formatter),
            Self::Runtime(error) => Display::fmt(error, formatter),
            Self::Barrier(error) => Display::fmt(error, formatter),
            Self::Temporal(error) => Display::fmt(error, formatter),
            Self::InvalidSchemaVersion => formatter.write_str("schema version must be nonzero"),
            Self::InvalidTransactionTtl => formatter.write_str("transaction TTL must be nonzero"),
            Self::ExpiryOverflow => formatter.write_str("transaction expiry overflows timestamp"),
            Self::TimestampExhausted => {
                formatter.write_str("transaction timestamp space exhausted")
            }
            Self::EmptyWriteSet => formatter.write_str("transaction write set is empty"),
            Self::ScopeMismatch => formatter.write_str(
                "temporal transaction contains an element outside its declared graph scope",
            ),
            Self::NoLeader { shard_id } => write!(formatter, "Shard {shard_id} has no leader"),
            Self::DuplicateParticipant => {
                formatter.write_str("transaction participant is duplicated")
            }
            Self::UnknownParticipant { shard_id } => {
                write!(formatter, "unknown participant Shard {shard_id}")
            }
            Self::StalePlacementEpoch {
                shard_id,
                expected,
                actual,
            } => write!(
                formatter,
                "Shard {shard_id} placement epoch {actual} is stale; expected {expected}"
            ),
            Self::BatchShardMismatch { participant, batch } => write!(
                formatter,
                "participant Shard {participant} differs from batch Shard {batch}"
            ),
            Self::BatchTransactionMismatch => formatter
                .write_str("prepared batch transaction ID differs from coordinator context"),
            Self::TooManyRoutedMutations => formatter
                .write_str("routed temporal transaction exceeds the mutation sequence space"),
            Self::ConflictingRoutedMutation { key } => write!(
                formatter,
                "temporal transaction routes conflicting operations to {key:?}"
            ),
            Self::CommitTimestampTooEarly => {
                formatter.write_str("commit timestamp does not exceed every participant minimum")
            }
            Self::Replication {
                phase,
                participant,
                source,
            } => write!(
                formatter,
                "transaction phase {phase} failed on Shard {}: {source}",
                participant.shard_id()
            ),
            Self::PrewriteFailed {
                participant,
                abort_decision_durable,
                cleanup_pending,
                source,
            } => write!(
                formatter,
                "Prewrite failed on Shard {}; abort_decision_durable={abort_decision_durable}, cleanup_pending={cleanup_pending:?}: {source}",
                participant.shard_id()
            ),
            Self::DecisionUnknown {
                transaction_id,
                home,
                source,
            } => write!(
                formatter,
                "transaction {} Home decision on Shard {} is unknown: {source}",
                transaction_id.value(),
                home.shard_id()
            ),
            Self::CommittedPendingApply {
                transaction_id,
                commit_ts,
                pending,
                source,
            } => write!(
                formatter,
                "transaction {} committed at {commit_ts:?} with pending participants {pending:?}: {source}",
                transaction_id.value()
            ),
        }
    }
}

impl Error for TransactionCoordinatorError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Timestamp(error) => Some(error),
            Self::Protocol(error) => Some(error),
            Self::Command(error) => Some(error),
            Self::Adapter(error) => Some(error),
            Self::Runtime(error) => Some(error),
            Self::Barrier(error) => Some(error),
            Self::Temporal(error) => Some(error),
            Self::Replication { source, .. }
            | Self::PrewriteFailed { source, .. }
            | Self::DecisionUnknown { source, .. }
            | Self::CommittedPendingApply { source, .. } => Some(source),
            _ => None,
        }
    }
}

impl From<TimestampOracleError> for TransactionCoordinatorError {
    fn from(error: TimestampOracleError) -> Self {
        Self::Timestamp(error)
    }
}

impl From<TxnProtocolError> for TransactionCoordinatorError {
    fn from(error: TxnProtocolError) -> Self {
        Self::Protocol(error)
    }
}

impl From<CommandCodecError> for TransactionCoordinatorError {
    fn from(error: CommandCodecError) -> Self {
        Self::Command(error)
    }
}

impl From<AdapterError> for TransactionCoordinatorError {
    fn from(error: AdapterError) -> Self {
        Self::Adapter(error)
    }
}

impl From<ReadBarrierError> for TransactionCoordinatorError {
    fn from(error: ReadBarrierError) -> Self {
        Self::Barrier(error)
    }
}

impl From<TemporalStoreError> for TransactionCoordinatorError {
    fn from(error: TemporalStoreError) -> Self {
        Self::Temporal(error)
    }
}

impl From<ReplicationError> for TransactionCoordinatorError {
    fn from(error: ReplicationError) -> Self {
        Self::Runtime(error)
    }
}
