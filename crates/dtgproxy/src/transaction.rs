#![allow(clippy::result_large_err)]

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use raft_command::{
    AbortIntentV1, CommandBodyV1, CommandCodecError, CommandEnvelopeV1, FinalizeV1,
    OnePhaseCommitV1, PrewriteV1, RecordDecisionV1,
};
use shard_client::{ExecuteCommand, ShardClient, ShardClientStorageAdapter, ShardRequestContext};
use shard_runtime::ReadBarrierError;
use shard_runtime::ReplicationError;
use storage_api::{
    AdapterError, Keyspace, LogicalKey, Mutation, MutationOperation, PreparedMutationBatch,
};
use temporal_ir::GraphScope;
use temporal_storage::{
    ElementId, ElementKind, ElementRef, GraphId, PartitionId, PrepareContext, TemporalStore,
    TemporalStoreError, TemporalTransaction, decode_graph_key, graph_key_scope,
};
use temporal_types::TransactionTime;
use timestamp_oracle::{TimestampOracle, TimestampOracleError};
use txn_protocol::{
    ConstraintClaim, HomeDecisionEngine, HomeTransactionRecord, IsolationLevel,
    MAX_TRANSACTION_MUTATIONS, ParticipantEngine, ParticipantProof, ParticipantRecoveryRecord,
    PrewriteMetadata, PrewriteRequest, RecoveryAction, ShardEpoch, TransactionId, TransactionState,
    TxnProtocolError, recovery_action,
};

use crate::{DeploymentConfig, InProcessDeploymentRuntime};

const SINGLE_SHARD_PHASE: u8 = 1;
const PREWRITE_PHASE: u8 = 2;
const COMMIT_DECISION_PHASE: u8 = 3;
const FINALIZE_PHASE: u8 = 4;
const ABORT_DECISION_PHASE: u8 = 5;
const ABORT_INTENT_PHASE: u8 = 6;
const CONSTRAINT_KEY_PREFIX: &[u8] = b"\x01dtg/constraint/v1/";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RoutedConstraintClaim {
    graph_id: u64,
    key: [u8; 32],
    owner: ElementRef,
}

impl RoutedConstraintClaim {
    pub fn new(
        graph_id: u64,
        key: [u8; 32],
        owner: ElementRef,
    ) -> Result<Self, TransactionCoordinatorError> {
        if graph_id == 0 || key == [0; 32] || owner.graph().value() != graph_id {
            return Err(TransactionCoordinatorError::InvalidConstraintClaim);
        }
        Ok(Self {
            graph_id,
            key,
            owner,
        })
    }

    #[must_use]
    pub const fn owner(&self) -> ElementRef {
        self.owner
    }

    #[must_use]
    pub const fn key(&self) -> [u8; 32] {
        self.key
    }

    fn protocol_claim(&self) -> Result<ConstraintClaim, TransactionCoordinatorError> {
        let mut key = Vec::with_capacity(CONSTRAINT_KEY_PREFIX.len() + 8 + 32);
        key.extend_from_slice(CONSTRAINT_KEY_PREFIX);
        key.extend_from_slice(&self.graph_id.to_be_bytes());
        key.extend_from_slice(&self.key);
        let mut value = Vec::with_capacity(29);
        value.push(self.owner.kind() as u8);
        value.extend_from_slice(&self.owner.graph().value().to_be_bytes());
        value.extend_from_slice(&self.owner.partition().value().to_be_bytes());
        value.extend_from_slice(&self.owner.id().value().to_be_bytes());
        Ok(ConstraintClaim::new(
            LogicalKey::in_keyspace(Keyspace::Txn, key),
            value,
        )?)
    }

    pub fn owner_read_keys(
        &self,
        deployment: &DeploymentConfig,
    ) -> Result<(ShardEpoch, [LogicalKey; 2]), TransactionCoordinatorError> {
        let participant = route_constraint_claim(deployment, self)?;
        let claim = self.protocol_claim()?;
        let keys = ParticipantEngine::constraint_owner_read_keys(participant, claim.key())?;
        Ok((participant, keys))
    }

    pub fn owner_at_snapshot(
        &self,
        owner: Option<&[u8]>,
        committed_write: Option<&[u8]>,
        start_ts: TransactionTime,
    ) -> Result<Option<ElementRef>, TransactionCoordinatorError> {
        let Some(bytes) =
            ParticipantEngine::constraint_owner_at_snapshot(owner, committed_write, start_ts)?
        else {
            return Ok(None);
        };
        if bytes.len() != 29 {
            return Err(TransactionCoordinatorError::InvalidConstraintOwner);
        }
        let kind = match bytes[0] {
            1 => ElementKind::Vertex,
            2 => ElementKind::Edge,
            _ => return Err(TransactionCoordinatorError::InvalidConstraintOwner),
        };
        let graph = u64::from_be_bytes(
            bytes[1..9]
                .try_into()
                .expect("fixed constraint owner graph slice"),
        );
        let partition = u32::from_be_bytes(
            bytes[9..13]
                .try_into()
                .expect("fixed constraint owner partition slice"),
        );
        let id = u128::from_be_bytes(
            bytes[13..29]
                .try_into()
                .expect("fixed constraint owner element slice"),
        );
        if graph != self.graph_id {
            return Err(TransactionCoordinatorError::InvalidConstraintOwner);
        }
        let graph = GraphId::new(graph);
        let partition = PartitionId::new(partition);
        let id = ElementId::new(id);
        Ok(Some(match kind {
            ElementKind::Vertex => ElementRef::vertex(graph, partition, id),
            ElementKind::Edge => ElementRef::edge(graph, partition, id),
        }))
    }
}

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
    pub fn from_allocated(
        start_ts: TransactionTime,
        commit_ts: TransactionTime,
        schema_version: u64,
        isolation: IsolationLevel,
        ttl_micros: u64,
    ) -> Result<Self, TransactionCoordinatorError> {
        if schema_version == 0 {
            return Err(TransactionCoordinatorError::InvalidSchemaVersion);
        }
        if ttl_micros == 0 {
            return Err(TransactionCoordinatorError::InvalidTransactionTtl);
        }
        if commit_ts <= start_ts {
            return Err(TransactionCoordinatorError::CommitTimestampTooEarly);
        }
        let ttl_micros =
            i64::try_from(ttl_micros).map_err(|_| TransactionCoordinatorError::ExpiryOverflow)?;
        let expires_at = TransactionTime::new(
            start_ts
                .physical_micros()
                .checked_add(ttl_micros)
                .ok_or(TransactionCoordinatorError::ExpiryOverflow)?,
            0,
        );
        Ok(Self {
            transaction_id: transaction_id_from_time(start_ts),
            start_ts,
            commit_ts,
            expires_at,
            schema_version,
            isolation,
        })
    }

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
    constraint_claims: Vec<ConstraintClaim>,
    metadata: PrewriteMetadata,
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

    #[must_use]
    pub fn into_parts(self) -> (GraphScope, TemporalTransaction) {
        (self.scope, self.transaction)
    }
}

impl PreparedShardTransaction {
    pub fn new(
        shard_id: u32,
        placement_epoch: u64,
        batch: PreparedMutationBatch,
        constraint_claims: Vec<ConstraintClaim>,
        metadata: PrewriteMetadata,
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
            constraint_claims,
            metadata,
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

    #[must_use]
    pub fn constraint_claims(&self) -> &[ConstraintClaim] {
        &self.constraint_claims
    }

    #[must_use]
    pub const fn metadata(&self) -> &PrewriteMetadata {
        &self.metadata
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

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TransactionRecoveryReceipt {
    scanned_records: usize,
    waiting: usize,
    finalized: usize,
    aborted: usize,
}

impl TransactionRecoveryReceipt {
    #[must_use]
    pub const fn scanned_records(self) -> usize {
        self.scanned_records
    }

    #[must_use]
    pub const fn waiting(self) -> usize {
        self.waiting
    }

    #[must_use]
    pub const fn finalized(self) -> usize {
        self.finalized
    }

    #[must_use]
    pub const fn aborted(self) -> usize {
        self.aborted
    }
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
    oracle: Option<&'oracle TimestampOracle>,
    max_ticks: usize,
}

type DispatchFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, ReplicationError>> + Send + 'a>>;

trait TransactionDispatcher {
    fn validate_participant(
        &self,
        participant: ShardEpoch,
    ) -> Result<(), TransactionCoordinatorError>;

    fn propose<'a>(
        &'a mut self,
        participant: ShardEpoch,
        command: Vec<u8>,
        max_ticks: usize,
    ) -> DispatchFuture<'a, ()>;

    fn propose_many<'a>(
        &'a mut self,
        commands: Vec<(u32, Vec<u8>)>,
        max_ticks: usize,
    ) -> DispatchFuture<'a, Vec<(u32, Result<(), ReplicationError>)>>;
}

struct InProcessDispatcher<'runtime> {
    runtime: &'runtime mut InProcessDeploymentRuntime,
}

impl TransactionDispatcher for InProcessDispatcher<'_> {
    fn validate_participant(
        &self,
        participant: ShardEpoch,
    ) -> Result<(), TransactionCoordinatorError> {
        let Some(placement) = self
            .runtime
            .config()
            .all_shards()
            .iter()
            .find(|placement| placement.shard_id() == participant.shard_id())
        else {
            return Err(TransactionCoordinatorError::UnknownParticipant {
                shard_id: participant.shard_id(),
            });
        };
        if placement.placement_epoch() != participant.placement_epoch() {
            return Err(TransactionCoordinatorError::StalePlacementEpoch {
                shard_id: participant.shard_id(),
                expected: placement.placement_epoch(),
                actual: participant.placement_epoch(),
            });
        }
        Ok(())
    }

    fn propose<'a>(
        &'a mut self,
        participant: ShardEpoch,
        command: Vec<u8>,
        max_ticks: usize,
    ) -> DispatchFuture<'a, ()> {
        Box::pin(async move {
            self.runtime
                .propose_shard(participant.shard_id(), command, max_ticks)
                .await
                .map(|_| ())
        })
    }

    fn propose_many<'a>(
        &'a mut self,
        commands: Vec<(u32, Vec<u8>)>,
        max_ticks: usize,
    ) -> DispatchFuture<'a, Vec<(u32, Result<(), ReplicationError>)>> {
        Box::pin(async move {
            self.runtime
                .propose_shards(commands, max_ticks)
                .await
                .map(|results| {
                    results
                        .into_iter()
                        .map(|(shard_id, result)| (shard_id, result.map(|_| ())))
                        .collect()
                })
        })
    }
}

struct RemoteDispatcher<'client> {
    client: &'client dyn ShardClient,
    graph_id: u64,
    deadline_unix_ms: u64,
}

impl TransactionDispatcher for RemoteDispatcher<'_> {
    fn validate_participant(
        &self,
        _participant: ShardEpoch,
    ) -> Result<(), TransactionCoordinatorError> {
        Ok(())
    }

    fn propose<'a>(
        &'a mut self,
        participant: ShardEpoch,
        command: Vec<u8>,
        _max_ticks: usize,
    ) -> DispatchFuture<'a, ()> {
        Box::pin(async move {
            let envelope = CommandEnvelopeV1::decode(&command)?;
            let context = ShardRequestContext::new(
                self.graph_id,
                participant.shard_id(),
                participant.placement_epoch(),
                envelope.request_id,
                self.deadline_unix_ms,
            )
            .map_err(|error| ReplicationError::Raft(error.to_string()))?;
            self.client
                .execute(
                    ExecuteCommand::new(context, command)
                        .map_err(|error| ReplicationError::Raft(error.to_string()))?,
                )
                .await
                .map(|_| ())
                .map_err(|error| ReplicationError::Raft(error.to_string()))
        })
    }

    fn propose_many<'a>(
        &'a mut self,
        commands: Vec<(u32, Vec<u8>)>,
        max_ticks: usize,
    ) -> DispatchFuture<'a, Vec<(u32, Result<(), ReplicationError>)>> {
        Box::pin(async move {
            let mut results = Vec::with_capacity(commands.len());
            for (shard_id, command) in commands {
                let envelope = CommandEnvelopeV1::decode(&command)?;
                let participant = ShardEpoch::new(shard_id, envelope.placement_epoch)
                    .map_err(|error| ReplicationError::Raft(error.to_string()))?;
                results.push((
                    shard_id,
                    self.propose(participant, command, max_ticks).await,
                ));
            }
            Ok(results)
        })
    }
}

impl<'oracle> TransactionCoordinator<'oracle> {
    #[must_use]
    pub const fn new(oracle: &'oracle TimestampOracle, max_ticks: usize) -> Self {
        Self {
            oracle: Some(oracle),
            max_ticks,
        }
    }

    #[must_use]
    pub const fn remote(max_ticks: usize) -> Self {
        Self {
            oracle: None,
            max_ticks,
        }
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
        let oracle = self
            .oracle
            .ok_or(TransactionCoordinatorError::LocalOracleUnavailable)?;
        let start_ts = oracle.next()?;
        let proof_floor = oracle.next_after(start_ts)?;
        let commit_ts = oracle.next_after(proof_floor)?;
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
        writes: Vec<PreparedShardTransaction>,
    ) -> Result<TransactionReceipt, TransactionCoordinatorError> {
        self.commit_via(&mut InProcessDispatcher { runtime }, context, writes)
            .await
    }

    pub async fn commit_remote(
        &self,
        client: &dyn ShardClient,
        graph_id: u64,
        deadline_unix_ms: u64,
        context: TransactionContext,
        writes: Vec<PreparedShardTransaction>,
    ) -> Result<TransactionReceipt, TransactionCoordinatorError> {
        if graph_id == 0 || deadline_unix_ms == 0 {
            return Err(TransactionCoordinatorError::InvalidRemoteContext);
        }
        self.commit_via(
            &mut RemoteDispatcher {
                client,
                graph_id,
                deadline_unix_ms,
            },
            context,
            writes,
        )
        .await
    }

    async fn commit_via<D: TransactionDispatcher>(
        &self,
        dispatcher: &mut D,
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
            self.validate_write(dispatcher, context, write)?;
        }
        let participants = writes
            .iter()
            .map(PreparedShardTransaction::participant)
            .collect::<Vec<_>>();
        let home = participants[0];
        if writes.len() == 1 {
            let write = writes.pop().expect("write set has exactly one element");
            let participant = write.participant;
            let request = prewrite_request(context, home, participants.clone(), write)?;
            let proof = ParticipantProof::new(
                participant,
                timestamp_successor(context.start_ts)?,
                request.intent_digest(),
            );
            let command = CommandEnvelopeV1::new(
                participant.shard_id(),
                participant.placement_epoch(),
                phase_request_id(context.transaction_id, SINGLE_SHARD_PHASE, participant),
                CommandBodyV1::OnePhaseCommit(OnePhaseCommitV1 {
                    request,
                    expected_proof: proof,
                    commit_ts: context.commit_ts,
                }),
            )
            .encode()?;
            dispatcher
                .propose(participant, command, self.max_ticks)
                .await
                .map_err(|source| TransactionCoordinatorError::Replication {
                    phase: "single-shard-commit",
                    participant,
                    source,
                })?;
            return Ok(receipt(context, home, participants, true));
        }

        let requests = writes
            .into_iter()
            .map(|write| prewrite_request(context, home, participants.clone(), write))
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
        let prewrite_results = dispatcher
            .propose_many(prewrite_commands, self.max_ticks)
            .await?;
        let mut prepared = Vec::with_capacity(requests.len());
        let mut failed_cleanup = Vec::new();
        let mut first_failure = None;
        for (shard_id, result) in prewrite_results {
            let index = requests
                .binary_search_by_key(&shard_id, |request| request.participant().shard_id())
                .expect("fan-out result belongs to a validated transaction participant");
            match result {
                Ok(_) => prepared.push((requests[index].clone(), proofs[index].clone())),
                Err(error) => {
                    if !is_persisted_committed_merge_rejection(&error) {
                        failed_cleanup.push(requests[index].participant());
                    }
                    if first_failure.is_none() {
                        first_failure = Some((requests[index].participant(), error));
                    }
                }
            }
        }
        if let Some((participant, source)) = first_failure {
            let (abort_decision_durable, mut cleanup_pending) = self
                .rollback_prepared(dispatcher, context, home, &participants, &prepared)
                .await;
            cleanup_pending.extend(failed_cleanup);
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
        if let Err(source) = dispatcher.propose(home, command, self.max_ticks).await {
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
        let finalize_results = dispatcher
            .propose_many(finalize_commands, self.max_ticks)
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
        let created_vertices = transactions
            .iter()
            .flat_map(|transaction| transaction.transaction.created_vertex_intervals())
            .collect::<Vec<_>>();
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
            endpoint_guards.extend(
                scoped
                    .transaction
                    .remote_endpoint_guards()
                    .into_iter()
                    .filter(|guard| !endpoint_satisfied_by_overlay(*guard, &created_vertices)),
            );
            grouped
                .entry(participant)
                .or_default()
                .merge_overlay(scoped.transaction)?;
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
                    constraint_claims: Vec::new(),
                    metadata: PrewriteMetadata::new(
                        context.schema_version,
                        participant.placement_epoch(),
                        Vec::new(),
                        Vec::new(),
                    )?,
                })
            })
            .collect::<Result<Vec<_>, TransactionCoordinatorError>>()?;
        self.commit(runtime, context, writes).await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn commit_temporal_remote(
        &self,
        client: Arc<dyn ShardClient>,
        deployment: &DeploymentConfig,
        graph_id: u64,
        deadline_unix_ms: u64,
        context: TransactionContext,
        transactions: Vec<ScopedTemporalTransaction>,
    ) -> Result<TransactionReceipt, TransactionCoordinatorError> {
        self.commit_temporal_remote_with_constraints(
            client,
            deployment,
            graph_id,
            deadline_unix_ms,
            context,
            transactions,
            Vec::new(),
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn commit_temporal_remote_with_constraints(
        &self,
        client: Arc<dyn ShardClient>,
        deployment: &DeploymentConfig,
        graph_id: u64,
        deadline_unix_ms: u64,
        context: TransactionContext,
        transactions: Vec<ScopedTemporalTransaction>,
        constraints: Vec<RoutedConstraintClaim>,
    ) -> Result<TransactionReceipt, TransactionCoordinatorError> {
        let writes = self
            .prepare_temporal_remote_candidate(
                Arc::clone(&client),
                deployment,
                graph_id,
                deadline_unix_ms,
                context,
                transactions,
                constraints,
            )
            .await?;
        self.commit_remote(client.as_ref(), graph_id, deadline_unix_ms, context, writes)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn validate_temporal_remote_candidate(
        &self,
        client: Arc<dyn ShardClient>,
        deployment: &DeploymentConfig,
        graph_id: u64,
        deadline_unix_ms: u64,
        context: TransactionContext,
        transactions: Vec<ScopedTemporalTransaction>,
        constraints: Vec<RoutedConstraintClaim>,
    ) -> Result<(), TransactionCoordinatorError> {
        self.prepare_temporal_remote_candidate(
            client,
            deployment,
            graph_id,
            deadline_unix_ms,
            context,
            transactions,
            constraints,
        )
        .await
        .map(drop)
    }

    #[allow(clippy::too_many_arguments)]
    async fn prepare_temporal_remote_candidate(
        &self,
        client: Arc<dyn ShardClient>,
        deployment: &DeploymentConfig,
        graph_id: u64,
        deadline_unix_ms: u64,
        context: TransactionContext,
        transactions: Vec<ScopedTemporalTransaction>,
        constraints: Vec<RoutedConstraintClaim>,
    ) -> Result<Vec<PreparedShardTransaction>, TransactionCoordinatorError> {
        if transactions.is_empty() {
            return Err(TransactionCoordinatorError::EmptyWriteSet);
        }
        if graph_id == 0 || deadline_unix_ms == 0 {
            return Err(TransactionCoordinatorError::InvalidRemoteContext);
        }
        let namespace = u64::try_from(context.transaction_id.value() & u128::from(u64::MAX))
            .expect("masked transaction namespace fits u64")
            .max(1);
        let created_vertices = transactions
            .iter()
            .flat_map(|transaction| transaction.transaction.created_vertex_intervals())
            .collect::<Vec<_>>();
        let mut grouped = BTreeMap::<ShardEpoch, TemporalTransaction>::new();
        let mut endpoint_guards = Vec::new();
        for scoped in transactions {
            if scoped.scope.graph().value() != graph_id
                || !scoped
                    .transaction
                    .is_scoped_to(scoped.scope.graph(), scoped.scope.partition())
            {
                return Err(TransactionCoordinatorError::ScopeMismatch);
            }
            let placement = deployment.route_scope(scoped.scope);
            let participant = ShardEpoch::new(placement.shard_id(), placement.placement_epoch())?;
            endpoint_guards.extend(
                scoped
                    .transaction
                    .remote_endpoint_guards()
                    .into_iter()
                    .filter(|guard| !endpoint_satisfied_by_overlay(*guard, &created_vertices)),
            );
            grouped
                .entry(participant)
                .or_default()
                .merge_overlay(scoped.transaction)?;
        }

        let mut routed = BTreeMap::<ShardEpoch, BTreeMap<LogicalKey, MutationOperation>>::new();
        for (participant, transaction) in grouped {
            let adapter = ShardClientStorageAdapter::new(
                Arc::clone(&client),
                graph_id,
                participant.shard_id(),
                participant.placement_epoch(),
                deadline_unix_ms,
                namespace ^ u64::from(participant.shard_id()),
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
                let destination = route_operation_with_config(deployment, &mutation.operation)?;
                insert_routed_operation(&mut routed, destination, mutation.operation)?;
            }
        }
        for guard in endpoint_guards {
            let scope = GraphScope::new(guard.vertex().graph(), guard.vertex().partition());
            let placement = deployment.route_scope(scope);
            let participant = ShardEpoch::new(placement.shard_id(), placement.placement_epoch())?;
            let adapter = ShardClientStorageAdapter::new(
                Arc::clone(&client),
                graph_id,
                participant.shard_id(),
                participant.placement_epoch(),
                deadline_unix_ms,
                namespace ^ u64::from(participant.shard_id()),
            )?;
            let operation = TemporalStore::new(adapter)
                .prepare_endpoint_guard(context.start_ts, guard.vertex(), guard.valid())
                .await?;
            insert_routed_operation(&mut routed, participant, operation)?;
        }

        let mut routed_claims =
            BTreeMap::<ShardEpoch, BTreeMap<LogicalKey, ConstraintClaim>>::new();
        for constraint in constraints {
            if constraint.graph_id != graph_id {
                return Err(TransactionCoordinatorError::ScopeMismatch);
            }
            let participant = route_constraint_claim(deployment, &constraint)?;
            let claim = constraint.protocol_claim()?;
            routed.entry(participant).or_default();
            match routed_claims
                .entry(participant)
                .or_default()
                .entry(claim.key().clone())
            {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(claim);
                }
                std::collections::btree_map::Entry::Occupied(entry)
                    if entry.get().value() == claim.value() => {}
                std::collections::btree_map::Entry::Occupied(_) => {
                    return Err(TransactionCoordinatorError::ConflictingConstraintClaim);
                }
            }
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
                    constraint_claims: routed_claims
                        .remove(&participant)
                        .map(BTreeMap::into_values)
                        .unwrap_or_default()
                        .collect(),
                    metadata: PrewriteMetadata::new(
                        context.schema_version,
                        participant.placement_epoch(),
                        Vec::new(),
                        Vec::new(),
                    )?,
                })
            })
            .collect::<Result<Vec<_>, TransactionCoordinatorError>>()?;
        validate_prepared_candidate_with_limits(
            context,
            &writes,
            usize::MAX,
            MAX_TRANSACTION_MUTATIONS,
            usize::MAX,
        )?;
        Ok(writes)
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

    pub async fn recover_pending(
        &self,
        runtime: &mut InProcessDeploymentRuntime,
    ) -> Result<TransactionRecoveryReceipt, TransactionCoordinatorError> {
        let observed_at = self
            .oracle
            .ok_or(TransactionCoordinatorError::LocalOracleUnavailable)?
            .next()?;
        let placements = runtime.config().all_shards().to_vec();
        let mut records = Vec::new();
        let mut receipt = TransactionRecoveryReceipt::default();
        for placement in placements {
            let participant = ShardEpoch::new(placement.shard_id(), placement.placement_epoch())?;
            let rows = {
                let group = runtime.raft_mut().group_mut(participant.shard_id())?;
                let leader_id = group
                    .leader_id()
                    .ok_or(TransactionCoordinatorError::NoLeader {
                        shard_id: participant.shard_id(),
                    })?;
                let permit = group
                    .leader_read_permit(leader_id, participant.placement_epoch(), self.max_ticks)
                    .await?;
                group
                    .replica_adapter(permit.node_id())
                    .ok_or(TransactionCoordinatorError::NoLeader {
                        shard_id: participant.shard_id(),
                    })?
                    .scan(&ParticipantEngine::recovery_span(participant))
                    .await?
            };
            receipt.scanned_records = receipt.scanned_records.saturating_add(rows.len());
            for row in rows {
                let record = ParticipantEngine::recovery_record(row.value())?;
                if record.request().participant() != participant {
                    return Err(TransactionCoordinatorError::StalePlacementEpoch {
                        shard_id: participant.shard_id(),
                        expected: participant.placement_epoch(),
                        actual: record.request().participant().placement_epoch(),
                    });
                }
                if record.state() == TransactionState::Preparing {
                    records.push(record);
                }
            }
        }

        for record in records {
            let request = record.request();
            let home_key =
                HomeDecisionEngine::inspection_key(request.home(), request.transaction_id())?;
            let home_bytes = {
                let group = runtime.raft_mut().group_mut(request.home().shard_id())?;
                let leader_id = group
                    .leader_id()
                    .ok_or(TransactionCoordinatorError::NoLeader {
                        shard_id: request.home().shard_id(),
                    })?;
                let permit = group
                    .leader_read_permit(leader_id, request.home().placement_epoch(), self.max_ticks)
                    .await?;
                group
                    .replica_adapter(permit.node_id())
                    .ok_or(TransactionCoordinatorError::NoLeader {
                        shard_id: request.home().shard_id(),
                    })?
                    .multi_get(std::slice::from_ref(&home_key))
                    .await?
                    .pop()
                    .flatten()
            };
            let home = home_bytes
                .as_deref()
                .map(HomeTransactionRecord::decode)
                .transpose()?;
            match recovery_action(home.as_ref(), request.expires_at(), observed_at) {
                RecoveryAction::Wait => {
                    receipt.waiting = receipt.waiting.saturating_add(1);
                }
                RecoveryAction::RollForward { commit_ts } => {
                    let command = recovery_finalize_command(&record, commit_ts)?;
                    runtime
                        .propose_shard(request.participant().shard_id(), command, self.max_ticks)
                        .await?;
                    receipt.finalized = receipt.finalized.saturating_add(1);
                }
                RecoveryAction::Rollback => {
                    if home.is_none() {
                        let decision = HomeTransactionRecord::new(
                            request.transaction_id(),
                            request.start_ts(),
                            TransactionState::Aborted,
                            None,
                            request.participants().to_vec(),
                            vec![record.proof().clone()],
                        )?;
                        let command = CommandEnvelopeV1::new(
                            request.home().shard_id(),
                            request.home().placement_epoch(),
                            phase_request_id(
                                request.transaction_id(),
                                ABORT_DECISION_PHASE,
                                request.home(),
                            ),
                            CommandBodyV1::RecordDecision(RecordDecisionV1 {
                                home: request.home(),
                                decision,
                            }),
                        )
                        .encode()?;
                        runtime
                            .propose_shard(request.home().shard_id(), command, self.max_ticks)
                            .await?;
                    }
                    let command = recovery_abort_command(&record)?;
                    runtime
                        .propose_shard(request.participant().shard_id(), command, self.max_ticks)
                        .await?;
                    receipt.aborted = receipt.aborted.saturating_add(1);
                }
            }
        }
        Ok(receipt)
    }

    fn validate_write<D: TransactionDispatcher>(
        &self,
        dispatcher: &D,
        context: TransactionContext,
        write: &PreparedShardTransaction,
    ) -> Result<(), TransactionCoordinatorError> {
        dispatcher.validate_participant(write.participant)?;
        if write.batch.txn_id != context.transaction_id.value() {
            return Err(TransactionCoordinatorError::BatchTransactionMismatch);
        }
        Ok(())
    }

    async fn rollback_prepared<D: TransactionDispatcher>(
        &self,
        dispatcher: &mut D,
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
                Ok(command) => dispatcher
                    .propose(home, command, self.max_ticks)
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
        match dispatcher
            .propose_many(abort_commands, self.max_ticks)
            .await
        {
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

fn validate_prepared_candidate_with_limits(
    context: TransactionContext,
    writes: &[PreparedShardTransaction],
    maximum_participants: usize,
    maximum_mutations: usize,
    maximum_constraint_claims: usize,
) -> Result<(), TransactionCoordinatorError> {
    if writes.is_empty() {
        return Err(TransactionCoordinatorError::EmptyWriteSet);
    }
    let mut writes = writes.to_vec();
    writes.sort_by_key(PreparedShardTransaction::participant);
    if writes
        .windows(2)
        .any(|pair| pair[0].participant == pair[1].participant)
    {
        return Err(TransactionCoordinatorError::DuplicateParticipant);
    }
    if writes.len() > maximum_participants {
        return Err(TxnProtocolError::InvalidParticipantCount {
            max: maximum_participants,
            actual: writes.len(),
        }
        .into());
    }
    let participants = writes
        .iter()
        .map(PreparedShardTransaction::participant)
        .collect::<Vec<_>>();
    let home = participants[0];
    for write in writes {
        if write.batch.txn_id != context.transaction_id.value() {
            return Err(TransactionCoordinatorError::BatchTransactionMismatch);
        }
        if (write.batch.mutations.is_empty() && write.constraint_claims.is_empty())
            || write.batch.mutations.len() > maximum_mutations
        {
            return Err(TxnProtocolError::InvalidMutationCount {
                max: maximum_mutations,
                actual: write.batch.mutations.len(),
            }
            .into());
        }
        if write.constraint_claims.len() > maximum_constraint_claims {
            return Err(TxnProtocolError::InvalidConstraintClaimCount {
                max: maximum_constraint_claims,
                actual: write.constraint_claims.len(),
            }
            .into());
        }
        prewrite_request(context, home, participants.clone(), write)?;
    }
    Ok(())
}

fn prewrite_request(
    context: TransactionContext,
    home: ShardEpoch,
    participants: Vec<ShardEpoch>,
    write: PreparedShardTransaction,
) -> Result<PrewriteRequest, TxnProtocolError> {
    PrewriteRequest::new(
        context.transaction_id,
        context.start_ts,
        context.schema_version,
        write.participant,
        home,
        participants,
        context.isolation,
        context.expires_at,
        write.batch,
        write.constraint_claims,
        write.metadata,
    )
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

fn endpoint_satisfied_by_overlay(
    guard: temporal_storage::EndpointGuard,
    created_vertices: &[(
        temporal_storage::ElementRef,
        temporal_types::Interval<temporal_types::ValidTime>,
    )],
) -> bool {
    created_vertices.iter().any(|(vertex, valid)| {
        *vertex == guard.vertex()
            && valid.start() <= guard.valid().start()
            && match (valid.end(), guard.valid().end()) {
                (None, _) => true,
                (Some(_), None) => false,
                (Some(created_end), Some(guard_end)) => created_end >= guard_end,
            }
    })
}

fn route_operation_with_config(
    deployment: &DeploymentConfig,
    operation: &MutationOperation,
) -> Result<ShardEpoch, TransactionCoordinatorError> {
    let key = operation_key(operation);
    let graph_key = decode_graph_key(key).map_err(TemporalStoreError::from)?;
    let (graph, partition) = graph_key_scope(graph_key);
    let placement = deployment.route_scope(GraphScope::new(graph, partition));
    Ok(ShardEpoch::new(
        placement.shard_id(),
        placement.placement_epoch(),
    )?)
}

fn route_constraint_claim(
    deployment: &DeploymentConfig,
    claim: &RoutedConstraintClaim,
) -> Result<ShardEpoch, TransactionCoordinatorError> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"DTGProxy/ConstraintShard/V1");
    hasher.update(&claim.graph_id.to_be_bytes());
    hasher.update(&claim.key);
    let digest = hasher.finalize();
    let partition = u32::from_be_bytes(digest.as_bytes()[..4].try_into().expect("fixed digest"))
        % deployment.virtual_partitions();
    let placement = deployment.route_scope(GraphScope::new(
        temporal_storage::GraphId::new(claim.graph_id),
        temporal_storage::PartitionId::new(partition),
    ));
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

fn recovery_finalize_command(
    record: &ParticipantRecoveryRecord,
    commit_ts: TransactionTime,
) -> Result<Vec<u8>, TransactionCoordinatorError> {
    let request = record.request();
    Ok(CommandEnvelopeV1::new(
        request.participant().shard_id(),
        request.participant().placement_epoch(),
        phase_request_id(
            request.transaction_id(),
            FINALIZE_PHASE,
            request.participant(),
        ),
        CommandBodyV1::Finalize(FinalizeV1 {
            participant: request.participant(),
            transaction_id: request.transaction_id(),
            intent_digest: request.intent_digest(),
            commit_ts,
        }),
    )
    .encode()?)
}

fn recovery_abort_command(
    record: &ParticipantRecoveryRecord,
) -> Result<Vec<u8>, TransactionCoordinatorError> {
    let request = record.request();
    Ok(CommandEnvelopeV1::new(
        request.participant().shard_id(),
        request.participant().placement_epoch(),
        phase_request_id(
            request.transaction_id(),
            ABORT_INTENT_PHASE,
            request.participant(),
        ),
        CommandBodyV1::AbortIntent(AbortIntentV1 {
            participant: request.participant(),
            transaction_id: request.transaction_id(),
            intent_digest: request.intent_digest(),
        }),
    )
    .encode()?)
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
    InvalidRemoteContext,
    InvalidConstraintClaim,
    InvalidConstraintOwner,
    LocalOracleUnavailable,
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
    ConflictingConstraintClaim,
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

impl TransactionCoordinatorError {
    #[must_use]
    pub fn is_retryable_merge_contention(&self) -> bool {
        match self {
            Self::PrewriteFailed {
                abort_decision_durable,
                cleanup_pending,
                source,
                ..
            } => {
                *abort_decision_durable
                    && cleanup_pending.is_empty()
                    && is_explicit_merge_conflict(&source.to_string())
            }
            Self::Replication {
                phase: "single-shard-commit",
                source,
                ..
            } => is_explicit_merge_conflict(&source.to_string()),
            _ => false,
        }
    }
}

fn is_explicit_merge_conflict(message: &str) -> bool {
    message.contains("WriteConflict")
        || message.contains("IntentConflict")
        || message.contains("ConstraintConflict")
}

fn is_persisted_committed_merge_rejection(error: &ReplicationError) -> bool {
    match error {
        ReplicationError::StateMachine(shard_runtime::ShardRuntimeError::CommittedRejection {
            message,
            ..
        }) => is_explicit_merge_conflict(message),
        ReplicationError::Raft(message) => {
            message.contains("was durably rejected:") && is_explicit_merge_conflict(message)
        }
        _ => false,
    }
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
            Self::InvalidRemoteContext => {
                formatter.write_str("remote transaction graph or deadline is invalid")
            }
            Self::InvalidConstraintClaim => {
                formatter.write_str("invalid distributed constraint claim")
            }
            Self::InvalidConstraintOwner => {
                formatter.write_str("invalid distributed constraint owner")
            }
            Self::LocalOracleUnavailable => {
                formatter.write_str("this coordinator requires timestamps from remote Meta")
            }
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
            Self::ConflictingConstraintClaim => {
                formatter.write_str("one transaction contains conflicting constraint owners")
            }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn participant() -> ShardEpoch {
        ShardEpoch::new(7, 1).expect("valid participant")
    }

    #[test]
    fn merge_retry_requires_an_explicit_conflict_after_a_durable_abort() {
        let conflict = TransactionCoordinatorError::PrewriteFailed {
            participant: participant(),
            abort_decision_durable: true,
            cleanup_pending: Vec::new(),
            source: ReplicationError::Raft("WriteConflict".into()),
        };
        assert!(conflict.is_retryable_merge_contention());

        let network_failure = TransactionCoordinatorError::PrewriteFailed {
            participant: participant(),
            abort_decision_durable: true,
            cleanup_pending: Vec::new(),
            source: ReplicationError::Raft("transport unavailable".into()),
        };
        assert!(!network_failure.is_retryable_merge_contention());
    }

    #[test]
    fn only_persisted_committed_merge_rejections_are_safe_from_failed_prewrite_cleanup() {
        let structured =
            ReplicationError::StateMachine(shard_runtime::ShardRuntimeError::CommittedRejection {
                request_id: 9,
                message: "transaction protocol error: IntentConflict".into(),
            });
        assert!(is_persisted_committed_merge_rejection(&structured));

        let remote = ReplicationError::Raft(
            "Shard client internal error: request 9 was durably rejected: transaction protocol error: ConstraintConflict"
                .into(),
        );
        assert!(is_persisted_committed_merge_rejection(&remote));

        let ambiguous =
            ReplicationError::Raft("IntentConflict before apply acknowledgement".into());
        assert!(!is_persisted_committed_merge_rejection(&ambiguous));
        assert!(!is_persisted_committed_merge_rejection(
            &ReplicationError::Raft("transport unavailable".into())
        ));
    }

    #[test]
    fn candidate_validation_counts_prepared_physical_mutations() {
        let context = TransactionContext::from_allocated(
            TransactionTime::new(10, 0),
            TransactionTime::new(20, 0),
            1,
            IsolationLevel::TemporalSnapshot,
            100,
        )
        .expect("context");
        let mutations = (0..3)
            .map(|sequence| {
                Mutation::put(
                    sequence,
                    LogicalKey::in_keyspace(Keyspace::Current, vec![sequence as u8]),
                    vec![sequence as u8],
                )
            })
            .collect();
        let write = PreparedShardTransaction {
            participant: participant(),
            batch: PreparedMutationBatch {
                shard_id: participant().shard_id(),
                txn_id: context.transaction_id().value(),
                mutations,
            },
            constraint_claims: Vec::new(),
            metadata: PrewriteMetadata::new(1, 1, Vec::new(), Vec::new()).expect("metadata"),
        };

        let error = validate_prepared_candidate_with_limits(context, &[write], 64, 2, 1_024)
            .expect_err("prepared physical mutation count must be bounded");

        assert!(matches!(
            error,
            TransactionCoordinatorError::Protocol(TxnProtocolError::InvalidMutationCount {
                max: 2,
                actual: 3,
            })
        ));
    }
}
