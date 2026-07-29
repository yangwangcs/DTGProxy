use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};

use dtg_shard::{
    CommitSingleShard, FinalizeParticipant, ParticipantIntent, PrewriteIntent, RecordHomeDecision,
    ShardCommand as PhysicalShardCommand,
};
use dtg_storage::{
    BackendClass, BindingRole, CapabilityManifest, ChangesRead, CommittedShardBatch, ProviderKind,
    ReadFence, ReplicaBinding, ReplicaStateStore, VertexRead,
};
use dtg_storage_fjall::FjallReplicaStore;
use dtg_transaction::{
    BackendGeneration, ChangeCursor, ChangeRecord, LogicalMutation, ParticipantWrite,
    PlacementEpoch, RecoveredParticipantIntent, ReplicaMetadata, ShardCommandExecutor, ShardId,
    ShardRequest, ShardSnapshotFence, SnapshotToken, SubmissionReceipt, TemporalTxnCoordinator,
    TimestampAuthority, TransactionContext, TransactionHistory, TransactionId, TransactionOutcome,
    TransactionRecord, TransactionState, TransactionTime, TxnError, TxnFuture, ValidInterval,
    Value, Version, VertexId, VertexVersion,
};

#[test]
fn single_shard_fast_path_uses_one_deterministic_command() {
    let timestamps = Arc::new(FakeTimestamps::default());
    let shards = Arc::new(FakeShards::default());
    let coordinator = TemporalTxnCoordinator::new(timestamps.clone(), shards.clone());
    let mut context = context(&[7]);
    let mutation = vertex_mutation(1);
    context.stage(mutation.clone()).unwrap();
    let participant = ParticipantWrite::new(ShardId::new(7).unwrap(), vec![mutation]).unwrap();

    assert_eq!(
        block_on(coordinator.commit(&context, vec![participant.clone()])),
        Ok(TransactionOutcome::Committed(transaction_time(50)))
    );
    assert_eq!(
        block_on(coordinator.commit(&context, vec![participant])),
        Ok(TransactionOutcome::Committed(transaction_time(50)))
    );

    let commands = shards.commands.lock().unwrap();
    assert_eq!(commands.len(), 2);
    assert!(
        commands
            .iter()
            .all(|(_, request)| matches!(request, ShardRequest::CommitSingleShard { .. }))
    );
    let command_ids: BTreeSet<_> = commands
        .iter()
        .map(|(_, request)| request.header().command_id().get())
        .collect();
    assert_eq!(command_ids.len(), 1);
    assert_eq!(timestamps.published.lock().unwrap().as_slice(), &[50]);
}

#[test]
fn begin_allocates_a_published_start_time_and_pins_all_fences() {
    let timestamps = Arc::new(FakeTimestamps::default());
    let coordinator = TemporalTxnCoordinator::new(timestamps, Arc::new(FakeShards::default()));
    let shard_id = ShardId::new(7).unwrap();
    let context = block_on(coordinator.begin(
        TransactionId::new(99).unwrap(),
        Version::new(1),
        vec![(
            shard_id,
            ShardSnapshotFence {
                placement_epoch: PlacementEpoch::new(7).unwrap(),
                backend_generation: BackendGeneration::new(10).unwrap(),
                applied_index: 0,
                closed_time: transaction_time(100),
            },
        )],
    ))
    .unwrap();

    assert_eq!(context.snapshot().start_time, transaction_time(40));
    assert!(context.snapshot().shards.contains_key(&shard_id));
}

#[test]
fn multi_shard_commit_uses_the_smallest_home_and_ordered_phases() {
    let timestamps = Arc::new(FakeTimestamps::default());
    let shards = Arc::new(FakeShards::default());
    let coordinator = TemporalTxnCoordinator::new(timestamps.clone(), shards.clone());
    let mut context = context(&[9, 3]);
    let first = vertex_mutation(1);
    let second = vertex_mutation(2);
    context.stage(first.clone()).unwrap();
    context.stage(second.clone()).unwrap();

    let outcome = block_on(coordinator.commit(
        &context,
        vec![
            ParticipantWrite::new(ShardId::new(9).unwrap(), vec![first]).unwrap(),
            ParticipantWrite::new(ShardId::new(3).unwrap(), vec![second]).unwrap(),
        ],
    ));

    assert_eq!(
        outcome,
        Ok(TransactionOutcome::Committed(transaction_time(50)))
    );
    let commands = shards.commands.lock().unwrap();
    assert_eq!(commands.len(), 5);
    assert!(matches!(commands[0].1, ShardRequest::PrewriteIntent { .. }));
    assert!(matches!(commands[1].1, ShardRequest::PrewriteIntent { .. }));
    assert!(matches!(
        commands[2].1,
        ShardRequest::RecordHomeDecision { .. }
    ));
    assert!(matches!(
        commands[3].1,
        ShardRequest::FinalizeParticipantCommit { .. }
    ));
    assert!(matches!(
        commands[4].1,
        ShardRequest::FinalizeParticipantCommit { .. }
    ));
    assert_eq!(
        commands
            .iter()
            .map(|(shard, _)| shard.get())
            .collect::<Vec<_>>(),
        vec![3, 9, 3, 3, 9]
    );
    assert_eq!(timestamps.published.lock().unwrap().as_slice(), &[50]);
}

#[test]
fn coordinator_rejects_post_snapshot_write_conflicts_before_prewrite() {
    let timestamps = Arc::new(FakeTimestamps::default());
    let shards = Arc::new(FakeShards::default());
    shards.changes.lock().unwrap().insert(
        ShardId::new(7).unwrap(),
        vec![ChangeRecord::new(
            ChangeCursor::new(2, 0),
            LogicalMutation::PutVertex(
                VertexVersion::new(
                    VertexId::new(1).unwrap(),
                    Version::new(2),
                    ValidInterval::new(50, 150).unwrap(),
                    transaction_time(45),
                    Default::default(),
                )
                .unwrap(),
            ),
        )],
    );
    let coordinator = TemporalTxnCoordinator::new(timestamps.clone(), shards.clone());
    let mut context = context(&[7]);
    let mutation = vertex_mutation(1);
    context.stage(mutation.clone()).unwrap();

    assert_eq!(
        block_on(coordinator.commit(
            &context,
            vec![ParticipantWrite::new(ShardId::new(7).unwrap(), vec![mutation]).unwrap()],
        )),
        Err(TxnError::WriteConflict)
    );
    assert!(shards.commands.lock().unwrap().is_empty());
    assert!(timestamps.published.lock().unwrap().is_empty());
}

#[test]
fn participant_payload_must_equal_the_validated_overlay() {
    let timestamps = Arc::new(FakeTimestamps::default());
    let shards = Arc::new(FakeShards::default());
    let coordinator = TemporalTxnCoordinator::new(timestamps, shards.clone());
    let mut context = context(&[7]);
    context.stage(vertex_mutation(1)).unwrap();
    let different = vertex_mutation(2);

    assert_eq!(
        block_on(coordinator.commit(
            &context,
            vec![ParticipantWrite::new(ShardId::new(7).unwrap(), vec![different]).unwrap()],
        )),
        Err(TxnError::ParticipantsMismatch)
    );
    assert!(shards.commands.lock().unwrap().is_empty());
}

#[test]
fn recovery_requires_a_home_decision_and_finishes_commit_or_abort() {
    let timestamps = Arc::new(FakeTimestamps::default());
    let shards = Arc::new(DurableShards::default());
    let coordinator = TemporalTxnCoordinator::new(timestamps.clone(), shards.clone());
    let (context, participants) = two_shard_transaction();

    shards.crash_after(2);
    assert_eq!(
        block_on(coordinator.commit(&context, participants.clone())),
        Err(TxnError::InjectedCrash)
    );
    shards.clear_crash();
    assert_eq!(
        block_on(coordinator.recover(&context)),
        Ok(TransactionOutcome::Unresolved)
    );
    assert!(timestamps.published.lock().unwrap().is_empty());

    let timestamps = Arc::new(FakeTimestamps::default());
    let shards = Arc::new(DurableShards::default());
    let coordinator = TemporalTxnCoordinator::new(timestamps.clone(), shards.clone());
    let (context, participants) = two_shard_transaction();
    shards.crash_after(3);
    assert_eq!(
        block_on(coordinator.commit(&context, participants)),
        Err(TxnError::InjectedCrash)
    );
    assert!(timestamps.published.lock().unwrap().is_empty());
    shards.clear_crash();
    assert_eq!(
        block_on(coordinator.recover(&context)),
        Ok(TransactionOutcome::Committed(transaction_time(50)))
    );
    assert_eq!(timestamps.published.lock().unwrap().as_slice(), &[50]);

    let timestamps = Arc::new(FakeTimestamps::default());
    let shards = Arc::new(DurableShards::default());
    let coordinator = TemporalTxnCoordinator::new(timestamps.clone(), shards.clone());
    let (context, participants) = two_shard_transaction();
    shards.crash_after(3);
    assert_eq!(
        block_on(coordinator.abort(&context, participants)),
        Err(TxnError::InjectedCrash)
    );
    shards.clear_crash();
    assert_eq!(
        block_on(coordinator.recover(&context)),
        Ok(TransactionOutcome::Aborted)
    );
    assert!(timestamps.published.lock().unwrap().is_empty());
    assert!(!shards.graph_is_visible());
}

#[test]
fn crash_matrix_and_visibility_publication_are_recoverable_and_idempotent() {
    for crash in [
        CrashPoint::Before(1),
        CrashPoint::After(1),
        CrashPoint::Before(2),
        CrashPoint::After(2),
        CrashPoint::Before(3),
    ] {
        let timestamps = Arc::new(FakeTimestamps::default());
        let shards = Arc::new(DurableShards::default());
        shards.set_crash(crash);
        let coordinator = TemporalTxnCoordinator::new(timestamps.clone(), shards.clone());
        let (context, participants) = two_shard_transaction();
        assert_eq!(
            block_on(coordinator.commit(&context, participants)),
            Err(TxnError::InjectedCrash)
        );
        shards.clear_crash();
        assert_eq!(
            block_on(coordinator.recover(&context)),
            Ok(TransactionOutcome::Unresolved)
        );
        assert!(timestamps.published.lock().unwrap().is_empty());
    }

    for crash in [
        CrashPoint::After(3),
        CrashPoint::Before(4),
        CrashPoint::After(4),
        CrashPoint::Before(5),
        CrashPoint::After(5),
    ] {
        let timestamps = Arc::new(FakeTimestamps::default());
        let shards = Arc::new(DurableShards::default());
        shards.set_crash(crash);
        let coordinator = TemporalTxnCoordinator::new(timestamps.clone(), shards.clone());
        let (context, participants) = two_shard_transaction();
        assert_eq!(
            block_on(coordinator.commit(&context, participants)),
            Err(TxnError::InjectedCrash)
        );
        assert!(timestamps.published.lock().unwrap().is_empty());
        shards.clear_crash();
        shards.reverse_history();
        assert_eq!(
            block_on(coordinator.recover(&context)),
            Ok(TransactionOutcome::Committed(transaction_time(50)))
        );
        let applied = shards.applied_commands.lock().unwrap().len();
        assert_eq!(
            block_on(coordinator.recover(&context)),
            Ok(TransactionOutcome::Committed(transaction_time(50)))
        );
        assert_eq!(shards.applied_commands.lock().unwrap().len(), applied);
        assert_eq!(timestamps.published.lock().unwrap().as_slice(), &[50]);
    }

    for failure in [PublishFailure::Before, PublishFailure::After] {
        let timestamps = Arc::new(FakeTimestamps::default());
        timestamps.fail_publish(failure);
        let shards = Arc::new(DurableShards::default());
        let coordinator = TemporalTxnCoordinator::new(timestamps.clone(), shards.clone());
        let (context, participants) = two_shard_transaction();
        assert_eq!(
            block_on(coordinator.commit(&context, participants)),
            Err(TxnError::InjectedCrash)
        );
        if failure == PublishFailure::Before {
            assert!(timestamps.published.lock().unwrap().is_empty());
        } else {
            assert_eq!(timestamps.published.lock().unwrap().as_slice(), &[50]);
        }
        timestamps.clear_publish_failure();
        assert_eq!(
            block_on(coordinator.recover(&context)),
            Ok(TransactionOutcome::Committed(transaction_time(50)))
        );
        assert_eq!(timestamps.published.lock().unwrap().as_slice(), &[50]);
    }
}

#[test]
fn restarted_delivery_deduplicates_prewrite_decision_and_finalize_phases() {
    for crash in [CrashPoint::Before(3), CrashPoint::After(3)] {
        let timestamps = Arc::new(FakeTimestamps::default());
        let shards = Arc::new(DurableShards::default());
        shards.set_crash(crash);
        let coordinator = TemporalTxnCoordinator::new(timestamps.clone(), shards.clone());
        let (context, participants) = two_shard_transaction();
        assert_eq!(
            block_on(coordinator.commit(&context, participants.clone())),
            Err(TxnError::InjectedCrash)
        );
        shards.clear_crash();
        assert_eq!(
            block_on(coordinator.commit(&context, participants)),
            Ok(TransactionOutcome::Committed(transaction_time(50)))
        );
        assert_eq!(shards.applied_commands.lock().unwrap().len(), 5);
        assert_eq!(shards.graph_mutation_count(), 2);
        assert_eq!(timestamps.published.lock().unwrap().as_slice(), &[50]);
    }
}

#[test]
fn partial_finalization_never_advances_the_snapshot_frontier() {
    let timestamps = Arc::new(FakeTimestamps::default());
    let shards = Arc::new(DurableShards::default());
    shards.set_crash(CrashPoint::After(4));
    let coordinator = TemporalTxnCoordinator::new(timestamps.clone(), shards.clone());
    let (context, participants) = two_shard_transaction();

    assert_eq!(
        block_on(coordinator.commit(&context, participants)),
        Err(TxnError::InjectedCrash)
    );
    assert_eq!(
        block_on(timestamps.allocate_start_time(TransactionId::new(100).unwrap())),
        Ok(transaction_time(40))
    );

    shards.clear_crash();
    assert_eq!(
        block_on(coordinator.recover(&context)),
        Ok(TransactionOutcome::Committed(transaction_time(50)))
    );
    assert_eq!(
        block_on(timestamps.allocate_start_time(TransactionId::new(101).unwrap())),
        Ok(transaction_time(50))
    );
}

#[test]
fn single_shard_retry_after_prepublication_crash_replays_then_publishes() {
    let timestamps = Arc::new(FakeTimestamps::default());
    timestamps.fail_publish(PublishFailure::Before);
    let shards = Arc::new(DurableShards::default());
    let coordinator = TemporalTxnCoordinator::new(timestamps.clone(), shards.clone());
    let mut context = context(&[7]);
    let mutation = vertex_mutation(1);
    context.stage(mutation.clone()).unwrap();
    let participant = ParticipantWrite::new(ShardId::new(7).unwrap(), vec![mutation]).unwrap();

    assert_eq!(
        block_on(coordinator.commit(&context, vec![participant.clone()])),
        Err(TxnError::InjectedCrash)
    );
    assert_eq!(shards.graph_mutation_count(), 1);
    assert!(timestamps.published.lock().unwrap().is_empty());

    timestamps.clear_publish_failure();
    assert_eq!(
        block_on(coordinator.commit(&context, vec![participant])),
        Ok(TransactionOutcome::Committed(transaction_time(50)))
    );
    assert_eq!(shards.graph_mutation_count(), 1);
    assert_eq!(shards.applied_commands.lock().unwrap().len(), 1);
    assert_eq!(timestamps.published.lock().unwrap().as_slice(), &[50]);
}

#[test]
fn corrupt_unknown_trailing_and_oversized_intents_fail_closed() {
    for corruption in [
        IntentCorruption::Corrupt,
        IntentCorruption::UnknownVersion,
        IntentCorruption::TrailingBytes,
        IntentCorruption::Oversized,
    ] {
        let timestamps = Arc::new(FakeTimestamps::default());
        let shards = Arc::new(DurableShards::default());
        shards.set_crash(CrashPoint::After(3));
        let coordinator = TemporalTxnCoordinator::new(timestamps.clone(), shards.clone());
        let (context, participants) = two_shard_transaction();
        assert_eq!(
            block_on(coordinator.commit(&context, participants)),
            Err(TxnError::InjectedCrash)
        );
        shards.clear_crash();
        shards.corrupt_intent(ShardId::new(3).unwrap(), corruption);

        assert_eq!(
            block_on(coordinator.recover(&context)),
            Err(TxnError::CorruptRecovery)
        );
        assert!(timestamps.published.lock().unwrap().is_empty());
        assert!(!shards.graph_is_visible());
    }
}

#[test]
fn fjall_close_reopen_recovers_participant_intents_and_home_decision() {
    let root = tempfile::tempdir().unwrap();
    let bindings = BTreeMap::from([
        (ShardId::new(3).unwrap(), fixture_binding(3, 4)),
        (ShardId::new(9).unwrap(), fixture_binding(9, 10)),
    ]);
    let paths = BTreeMap::from([
        (ShardId::new(3).unwrap(), root.path().join("shard-3")),
        (ShardId::new(9).unwrap(), root.path().join("shard-9")),
    ]);
    let timestamps = Arc::new(FakeTimestamps::default());

    {
        let shards = Arc::new(FjallShards::open(&paths, &bindings));
        shards.crash_after(3);
        let coordinator = TemporalTxnCoordinator::new(timestamps.clone(), shards);
        let (context, participants) = two_shard_transaction();
        assert_eq!(
            block_on(coordinator.commit(&context, participants)),
            Err(TxnError::InjectedCrash)
        );
    }

    let shards = Arc::new(FjallShards::open(&paths, &bindings));
    let coordinator = TemporalTxnCoordinator::new(timestamps.clone(), shards.clone());
    let (context, _) = two_shard_transaction();
    assert_eq!(
        block_on(coordinator.recover(&context)),
        Ok(TransactionOutcome::Committed(transaction_time(50)))
    );
    assert!(shards.vertex_is_visible(ShardId::new(3).unwrap(), 2));
    assert!(shards.vertex_is_visible(ShardId::new(9).unwrap(), 1));
    assert_eq!(timestamps.published.lock().unwrap().as_slice(), &[50]);
}

fn two_shard_transaction() -> (TransactionContext, Vec<ParticipantWrite>) {
    let mut context = context(&[9, 3]);
    let first = vertex_mutation(1);
    let second = vertex_mutation(2);
    context.stage(first.clone()).unwrap();
    context.stage(second.clone()).unwrap();
    (
        context,
        vec![
            ParticipantWrite::new(ShardId::new(9).unwrap(), vec![first]).unwrap(),
            ParticipantWrite::new(ShardId::new(3).unwrap(), vec![second]).unwrap(),
        ],
    )
}

fn context(shards: &[u64]) -> TransactionContext {
    let fences = shards
        .iter()
        .map(|shard| {
            (
                ShardId::new(*shard).unwrap(),
                ShardSnapshotFence {
                    placement_epoch: PlacementEpoch::new(7).unwrap(),
                    backend_generation: BackendGeneration::new(10).unwrap(),
                    applied_index: 0,
                    closed_time: transaction_time(100),
                },
            )
        })
        .collect();
    TransactionContext::new(
        SnapshotToken::new(
            TransactionId::new(99).unwrap(),
            transaction_time(40),
            Version::new(1),
            fences,
        )
        .unwrap(),
    )
}

fn vertex_mutation(id: u128) -> LogicalMutation {
    LogicalMutation::PutVertex(
        VertexVersion::new(
            VertexId::new(id).unwrap(),
            Version::new(1),
            ValidInterval::new(0, 100).unwrap(),
            transaction_time(40),
            Default::default(),
        )
        .unwrap(),
    )
}

fn transaction_time(value: i64) -> TransactionTime {
    TransactionTime::new(value).unwrap()
}

#[derive(Default)]
struct FakeTimestamps {
    published: Mutex<Vec<i64>>,
    publish_failure: Mutex<Option<PublishFailure>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PublishFailure {
    Before,
    After,
}

impl FakeTimestamps {
    fn fail_publish(&self, failure: PublishFailure) {
        *self.publish_failure.lock().unwrap() = Some(failure);
    }

    fn clear_publish_failure(&self) {
        *self.publish_failure.lock().unwrap() = None;
    }
}

impl TimestampAuthority for FakeTimestamps {
    fn allocate_start_time(
        &self,
        _transaction_id: TransactionId,
    ) -> TxnFuture<'_, TransactionTime> {
        Box::pin(async move {
            Ok(transaction_time(
                self.published
                    .lock()
                    .unwrap()
                    .iter()
                    .copied()
                    .max()
                    .unwrap_or(40),
            ))
        })
    }

    fn allocate_commit_time(
        &self,
        _transaction_id: TransactionId,
    ) -> TxnFuture<'_, TransactionTime> {
        Box::pin(async { Ok(transaction_time(50)) })
    }

    fn publish_commit_time(
        &self,
        _transaction_id: TransactionId,
        commit_time: TransactionTime,
    ) -> TxnFuture<'_, ()> {
        Box::pin(async move {
            if *self.publish_failure.lock().unwrap() == Some(PublishFailure::Before) {
                return Err(TxnError::InjectedCrash);
            }
            let mut published = self.published.lock().unwrap();
            if !published.contains(&commit_time.get()) {
                published.push(commit_time.get());
            }
            if *self.publish_failure.lock().unwrap() == Some(PublishFailure::After) {
                return Err(TxnError::InjectedCrash);
            }
            Ok(())
        })
    }
}

#[derive(Default)]
struct FakeShards {
    commands: Mutex<Vec<(ShardId, ShardRequest)>>,
    changes: Mutex<BTreeMap<ShardId, Vec<ChangeRecord>>>,
}

impl ShardCommandExecutor for FakeShards {
    fn submit(&self, shard_id: ShardId, request: ShardRequest) -> TxnFuture<'_, SubmissionReceipt> {
        Box::pin(async move {
            let (_, intent_digest) = map_request_to_shard(shard_id, &request)?;
            let mut commands = self.commands.lock().unwrap();
            commands.push((shard_id, request));
            Ok(match intent_digest {
                Some(digest) => SubmissionReceipt::prepared(commands.len() as u64, false, digest),
                None => SubmissionReceipt::new(commands.len() as u64, false),
            })
        })
    }

    fn changes_after(
        &self,
        shard_id: ShardId,
        _applied_index: u64,
    ) -> TxnFuture<'_, Vec<ChangeRecord>> {
        Box::pin(async move {
            Ok(self
                .changes
                .lock()
                .unwrap()
                .get(&shard_id)
                .cloned()
                .unwrap_or_default())
        })
    }

    fn transaction_history(
        &self,
        _shard_id: ShardId,
        _transaction_id: TransactionId,
    ) -> TxnFuture<'_, TransactionHistory> {
        Box::pin(async { Ok(TransactionHistory::default()) })
    }
}

#[derive(Default)]
struct DurableShards {
    submit_count: Mutex<usize>,
    crash: Mutex<Option<CrashPoint>>,
    applied_commands: Mutex<BTreeSet<u128>>,
    durable: Mutex<BTreeMap<ShardId, Vec<ChangeRecord>>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CrashPoint {
    Before(usize),
    After(usize),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum IntentCorruption {
    Corrupt,
    UnknownVersion,
    TrailingBytes,
    Oversized,
}

impl DurableShards {
    fn crash_after(&self, boundary: usize) {
        self.set_crash(CrashPoint::After(boundary));
    }

    fn set_crash(&self, crash: CrashPoint) {
        *self.crash.lock().unwrap() = Some(crash);
    }

    fn clear_crash(&self) {
        *self.crash.lock().unwrap() = None;
    }

    fn reverse_history(&self) {
        for history in self.durable.lock().unwrap().values_mut() {
            history.reverse();
        }
    }

    fn corrupt_intent(&self, shard_id: ShardId, corruption: IntentCorruption) {
        let mut durable = self.durable.lock().unwrap();
        let history = durable.get_mut(&shard_id).unwrap();
        let change = history
            .iter_mut()
            .find(|change| {
                matches!(
                    change.mutation(),
                    LogicalMutation::PutReplicaMetadata(metadata)
                        if metadata.name() == dtg_shard::TRANSACTION_INTENT_METADATA_NAME
                )
            })
            .unwrap();
        let LogicalMutation::PutReplicaMetadata(metadata) = change.mutation() else {
            unreachable!();
        };
        let Value::Bytes(original) = metadata.value() else {
            unreachable!();
        };
        let mut bytes = original.clone();
        match corruption {
            IntentCorruption::Corrupt => {
                let last = bytes.last_mut().unwrap();
                *last ^= 1;
            }
            IntentCorruption::UnknownVersion => bytes[..4].copy_from_slice(&2_u32.to_be_bytes()),
            IntentCorruption::TrailingBytes => bytes.push(0),
            IntentCorruption::Oversized => bytes.resize(4 * 1024 * 1024 + 1, 0),
        }
        *change = ChangeRecord::new(
            change.cursor(),
            LogicalMutation::PutReplicaMetadata(
                ReplicaMetadata::new(
                    dtg_shard::TRANSACTION_INTENT_METADATA_NAME,
                    Value::Bytes(bytes),
                )
                .unwrap(),
            ),
        );
    }

    fn graph_is_visible(&self) -> bool {
        self.durable
            .lock()
            .unwrap()
            .values()
            .flatten()
            .any(|change| {
                matches!(
                    change.mutation(),
                    LogicalMutation::PutVertex(_)
                        | LogicalMutation::DeleteVertex(_)
                        | LogicalMutation::PutEdge(_)
                        | LogicalMutation::DeleteEdge(_)
                )
            })
    }

    fn graph_mutation_count(&self) -> usize {
        self.durable
            .lock()
            .unwrap()
            .values()
            .flatten()
            .filter(|change| {
                matches!(
                    change.mutation(),
                    LogicalMutation::PutVertex(_)
                        | LogicalMutation::DeleteVertex(_)
                        | LogicalMutation::PutEdge(_)
                        | LogicalMutation::DeleteEdge(_)
                )
            })
            .count()
    }
}

impl ShardCommandExecutor for DurableShards {
    fn submit(&self, shard_id: ShardId, request: ShardRequest) -> TxnFuture<'_, SubmissionReceipt> {
        Box::pin(async move {
            let boundary = {
                let mut count = self.submit_count.lock().unwrap();
                *count += 1;
                *count
            };
            if *self.crash.lock().unwrap() == Some(CrashPoint::Before(boundary)) {
                return Err(TxnError::InjectedCrash);
            }
            let command_id = request.header().command_id().get();
            let (command, intent_digest) = map_request_to_shard(shard_id, &request)?;
            let replayed = !self.applied_commands.lock().unwrap().insert(command_id);
            if !replayed {
                let mutations = materialize_command(&command)?;
                let mut durable = self.durable.lock().unwrap();
                let history = durable.entry(shard_id).or_default();
                let raft_index = boundary as u64 + 1;
                for (ordinal, mutation) in mutations.into_iter().enumerate() {
                    history.push(ChangeRecord::new(
                        ChangeCursor::new(raft_index, ordinal as u64),
                        mutation,
                    ));
                }
            }
            if *self.crash.lock().unwrap() == Some(CrashPoint::After(boundary)) {
                return Err(TxnError::InjectedCrash);
            }
            Ok(match intent_digest {
                Some(digest) => SubmissionReceipt::prepared(boundary as u64 + 1, replayed, digest),
                None => SubmissionReceipt::new(boundary as u64 + 1, replayed),
            })
        })
    }

    fn changes_after(
        &self,
        shard_id: ShardId,
        applied_index: u64,
    ) -> TxnFuture<'_, Vec<ChangeRecord>> {
        Box::pin(async move {
            Ok(self
                .durable
                .lock()
                .unwrap()
                .get(&shard_id)
                .into_iter()
                .flatten()
                .filter(|change| change.raft_index() > applied_index)
                .cloned()
                .collect())
        })
    }

    fn transaction_history(
        &self,
        shard_id: ShardId,
        transaction_id: TransactionId,
    ) -> TxnFuture<'_, TransactionHistory> {
        Box::pin(async move {
            let changes = self
                .durable
                .lock()
                .unwrap()
                .get(&shard_id)
                .cloned()
                .unwrap_or_default();
            transaction_history_from_changes(shard_id, transaction_id, &changes)
        })
    }
}

fn materialize_command(command: &PhysicalShardCommand) -> Result<Vec<LogicalMutation>, TxnError> {
    match command {
        PhysicalShardCommand::CommitSingleShard(command) => Ok(command.mutations().to_vec()),
        PhysicalShardCommand::PrewriteIntent(command) => Ok(vec![
            LogicalMutation::PutTransaction(command.prepared().clone()),
            LogicalMutation::PutReplicaMetadata(ReplicaMetadata::new(
                dtg_shard::TRANSACTION_INTENT_METADATA_NAME,
                Value::Bytes(shard_result(command.intent().encode_current())?),
            )?),
        ]),
        PhysicalShardCommand::RecordHomeDecision(command) => Ok(command.mutations().to_vec()),
        PhysicalShardCommand::FinalizeParticipant(command) => {
            let mut mutations = command
                .intent()
                .map(|intent| intent.mutations().to_vec())
                .unwrap_or_default();
            mutations.push(LogicalMutation::PutTransaction(command.terminal().clone()));
            Ok(mutations)
        }
        _ => Err(TxnError::InvalidMutation),
    }
}

fn shard_result<T>(result: Result<T, dtg_shard::ShardError>) -> Result<T, TxnError> {
    result.map_err(|error| TxnError::Shard(error.to_string()))
}

fn map_request_to_shard(
    shard_id: ShardId,
    request: &ShardRequest,
) -> Result<(PhysicalShardCommand, Option<dtg_storage::Digest32>), TxnError> {
    match request {
        ShardRequest::CommitSingleShard { header, mutations } => Ok((
            PhysicalShardCommand::CommitSingleShard(shard_result(CommitSingleShard::new(
                header.command_id(),
                header.placement_epoch().get(),
                header.backend_generation().get(),
                mutations.clone(),
            ))?),
            None,
        )),
        ShardRequest::PrewriteIntent {
            header,
            transaction_id,
            start_time,
            mutations,
        } => {
            let intent = shard_result(ParticipantIntent::new(
                *transaction_id,
                shard_id,
                mutations.clone(),
            ))?;
            let digest = intent.digest();
            let prepared = TransactionRecord::new(
                *transaction_id,
                TransactionState::Prepared,
                *start_time,
                digest,
            )?;
            Ok((
                PhysicalShardCommand::PrewriteIntent(shard_result(PrewriteIntent::new(
                    header.command_id(),
                    header.placement_epoch().get(),
                    header.backend_generation().get(),
                    prepared,
                    intent,
                ))?),
                Some(digest),
            ))
        }
        ShardRequest::RecordHomeDecision { header, decision } => Ok((
            PhysicalShardCommand::RecordHomeDecision(shard_result(RecordHomeDecision::new(
                header.command_id(),
                header.placement_epoch().get(),
                header.backend_generation().get(),
                vec![LogicalMutation::PutTransaction(decision.clone())],
            ))?),
            None,
        )),
        ShardRequest::FinalizeParticipantCommit {
            header,
            transaction_id,
            commit_time,
            intent_digest,
            mutations,
        } => {
            let intent = shard_result(ParticipantIntent::new(
                *transaction_id,
                shard_id,
                mutations.clone(),
            ))?;
            if intent.digest() != *intent_digest {
                return Err(TxnError::CorruptRecovery);
            }
            let terminal = TransactionRecord::new(
                *transaction_id,
                TransactionState::Committed,
                *commit_time,
                *intent_digest,
            )?;
            Ok((
                PhysicalShardCommand::FinalizeParticipant(shard_result(FinalizeParticipant::new(
                    header.command_id(),
                    header.placement_epoch().get(),
                    header.backend_generation().get(),
                    terminal,
                    Some(intent),
                ))?),
                None,
            ))
        }
        ShardRequest::FinalizeParticipantAbort { header, terminal } => Ok((
            PhysicalShardCommand::FinalizeParticipant(shard_result(FinalizeParticipant::new(
                header.command_id(),
                header.placement_epoch().get(),
                header.backend_generation().get(),
                terminal.clone(),
                None,
            ))?),
            None,
        )),
    }
}

fn transaction_history_from_changes(
    shard_id: ShardId,
    transaction_id: TransactionId,
    changes: &[ChangeRecord],
) -> Result<TransactionHistory, TxnError> {
    let mut prepared = None;
    let mut intent = None;
    let mut terminal = Vec::new();
    for change in changes {
        match change.mutation() {
            LogicalMutation::PutTransaction(record) if record.id() == transaction_id => {
                if record.state() == TransactionState::Prepared {
                    if prepared
                        .replace(record.clone())
                        .is_some_and(|existing| existing != *record)
                    {
                        return Err(TxnError::CorruptRecovery);
                    }
                } else {
                    terminal.push(record.clone());
                }
            }
            LogicalMutation::PutReplicaMetadata(metadata)
                if metadata.name() == dtg_shard::TRANSACTION_INTENT_METADATA_NAME =>
            {
                let Value::Bytes(bytes) = metadata.value() else {
                    return Err(TxnError::CorruptRecovery);
                };
                let candidate = ParticipantIntent::decode_current(bytes)
                    .map_err(|_| TxnError::CorruptRecovery)?;
                if candidate.transaction_id() == transaction_id && candidate.shard_id() == shard_id
                {
                    let recovered = RecoveredParticipantIntent::new(
                        shard_id,
                        candidate.mutations().to_vec(),
                        candidate.digest(),
                    );
                    if intent
                        .replace(recovered.clone())
                        .is_some_and(|existing| existing != recovered)
                    {
                        return Err(TxnError::CorruptRecovery);
                    }
                }
            }
            _ => {}
        }
    }
    Ok(TransactionHistory::new(prepared, intent, terminal))
}

struct FjallShards {
    stores: Mutex<BTreeMap<ShardId, FjallReplicaStore>>,
    submit_count: Mutex<usize>,
    crash_after: Mutex<Option<usize>>,
}

impl FjallShards {
    fn open(
        paths: &BTreeMap<ShardId, PathBuf>,
        bindings: &BTreeMap<ShardId, ReplicaBinding>,
    ) -> Self {
        let stores = paths
            .iter()
            .map(|(shard_id, path)| {
                (
                    *shard_id,
                    FjallReplicaStore::open(path, bindings[shard_id].clone()).unwrap(),
                )
            })
            .collect();
        Self {
            stores: Mutex::new(stores),
            submit_count: Mutex::new(0),
            crash_after: Mutex::new(None),
        }
    }

    fn crash_after(&self, boundary: usize) {
        *self.crash_after.lock().unwrap() = Some(boundary);
    }

    fn vertex_is_visible(&self, shard_id: ShardId, vertex_id: u128) -> bool {
        let store = self.stores.lock().unwrap()[&shard_id].clone();
        let applied = block_on(store.applied_index()).unwrap();
        let view =
            block_on(store.begin_read_view(ReadFence::new(store.binding().clone(), applied)))
                .unwrap();
        block_on(view.get_vertex(VertexRead::new(
            VertexId::new(vertex_id).unwrap(),
            50,
            transaction_time(50),
        )))
        .unwrap()
        .is_some()
    }
}

impl ShardCommandExecutor for FjallShards {
    fn submit(&self, shard_id: ShardId, request: ShardRequest) -> TxnFuture<'_, SubmissionReceipt> {
        Box::pin(async move {
            let boundary = {
                let mut count = self.submit_count.lock().unwrap();
                *count += 1;
                *count
            };
            let (command, intent_digest) = map_request_to_shard(shard_id, &request)?;
            let store = self.stores.lock().unwrap()[&shard_id].clone();
            let index = store.applied_index().await?.saturating_add(1);
            let batch = CommittedShardBatch::new(
                store.binding().clone(),
                1,
                index,
                command.header().command_id(),
                materialize_command(&command)?,
            )?;
            let receipt = store.apply(batch).await?;
            if *self.crash_after.lock().unwrap() == Some(boundary) {
                return Err(TxnError::InjectedCrash);
            }
            Ok(match intent_digest {
                Some(digest) => {
                    SubmissionReceipt::prepared(receipt.raft_index(), receipt.replayed(), digest)
                }
                None => SubmissionReceipt::new(receipt.raft_index(), receipt.replayed()),
            })
        })
    }

    fn changes_after(
        &self,
        shard_id: ShardId,
        applied_index: u64,
    ) -> TxnFuture<'_, Vec<ChangeRecord>> {
        Box::pin(async move {
            let store = self.stores.lock().unwrap()[&shard_id].clone();
            let through_index = store.applied_index().await?;
            if through_index == 0 || applied_index >= through_index {
                return Ok(Vec::new());
            }
            let view = store
                .begin_read_view(ReadFence::new(store.binding().clone(), through_index))
                .await?;
            let mut after =
                (applied_index > 0).then_some(ChangeCursor::new(applied_index, u64::MAX));
            let mut changes = Vec::new();
            loop {
                let page = view
                    .changes(ChangesRead::new(after, through_index, 1_000)?)
                    .await?;
                changes.extend_from_slice(page.rows());
                match page.next_after() {
                    Some(next) => after = Some(next),
                    None => break,
                }
            }
            Ok(changes)
        })
    }

    fn transaction_history(
        &self,
        shard_id: ShardId,
        transaction_id: TransactionId,
    ) -> TxnFuture<'_, TransactionHistory> {
        Box::pin(async move {
            let changes = self.changes_after(shard_id, 0).await?;
            transaction_history_from_changes(shard_id, transaction_id, &changes)
        })
    }
}

fn fixture_binding(shard_id: u64, replica_id: u64) -> ReplicaBinding {
    let capabilities = CapabilityManifest::from_names([
        "adjacency",
        "immutable-read-view",
        "logical-snapshot",
        "point",
    ])
    .unwrap();
    let class = BackendClass::new(
        ProviderKind::Fjall,
        1,
        1,
        capabilities.names().map(str::to_owned),
    )
    .unwrap();
    ReplicaBinding::builder()
        .cluster_id(1)
        .graph_id(2)
        .shard_id(shard_id)
        .placement_epoch(7)
        .replica_id(replica_id)
        .backend_generation(10)
        .backend_class_digest(class.digest())
        .provider_kind(ProviderKind::Fjall)
        .contract_version(1)
        .layout_version(1)
        .capability_digest(capabilities.digest())
        .namespace_id(format!(
            "cluster-1/graph-2/shard-{shard_id}/replica-{replica_id}/generation-10"
        ))
        .endpoint_profile_ref("local")
        .credential_ref("none")
        .role(BindingRole::Active)
        .build()
        .unwrap()
}

struct ThreadWaker(std::thread::Thread);

impl Wake for ThreadWaker {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

fn block_on<F: Future>(future: F) -> F::Output {
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
