use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::path::PathBuf;
use std::sync::{Arc, Barrier, Condvar, Mutex, mpsc};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

use dtg_shard::{
    ApplyRejection, CommitSingleShardTransaction, FinalizeParticipant,
    HomeDecisionManifest as PhysicalHomeDecisionManifest,
    HomeDecisionParticipant as PhysicalHomeDecisionParticipant, ParticipantIntent, PrewriteIntent,
    RecordHomeDecision, ShardCommand as PhysicalShardCommand, ShardError, ShardStateMachine,
    decode_single_shard_transaction_metadata,
};
use dtg_storage::{
    BackendClass, BindingRole, CapabilityManifest, ChangesRead, CommandId, ProviderKind, ReadFence,
    ReplicaBinding, ReplicaStateStore, VertexRead,
};
use dtg_storage_fjall::FjallReplicaStore;
use dtg_transaction::{
    BackendGeneration, ChangeCursor, ChangeRecord, CommitResolution, CommitTimeReservation,
    DurableParticipantManifest, DurableTransactionManifest, LogicalMutation, ParticipantWrite,
    PlacementEpoch, RecoveredParticipantIntent, RecoveredSingleShardCommit, RecoveryLease,
    ReplicaMetadata, ShardCommandExecutor, ShardId, ShardRequest, ShardSnapshotFence,
    SnapshotToken, SubmissionFailure, SubmissionFuture, SubmissionReceipt, TemporalTxnCoordinator,
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
    assert_eq!(commands.len(), 1);
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
fn read_fence_shards_are_independent_from_write_participants() {
    let timestamps = Arc::new(FakeTimestamps::default());
    let shards = Arc::new(FakeShards::default());
    let coordinator = TemporalTxnCoordinator::new(timestamps, shards.clone());
    let mut context = context(&[3, 9]);
    let mutation = vertex_mutation(1);
    context.stage(mutation.clone()).unwrap();

    assert_eq!(
        block_on(coordinator.commit(
            &context,
            vec![ParticipantWrite::new(ShardId::new(9).unwrap(), vec![mutation]).unwrap()],
        )),
        Ok(TransactionOutcome::Committed(transaction_time(50)))
    );
    let commands = shards.commands.lock().unwrap();
    assert_eq!(commands.len(), 1);
    assert_eq!(commands[0].0, ShardId::new(9).unwrap());
    assert!(matches!(
        commands[0].1,
        ShardRequest::CommitSingleShard { .. }
    ));
}

#[test]
fn asymmetric_single_write_recovery_finds_the_durable_write_shard() {
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
    let mut context = context_with_transaction(&[3, 9], TransactionId::new(502).unwrap());
    let mutation = vertex_mutation(1);
    context.stage(mutation.clone()).unwrap();
    let participant = ParticipantWrite::new(ShardId::new(9).unwrap(), vec![mutation]).unwrap();

    {
        let shards = Arc::new(FjallShards::open(&paths, &bindings));
        shards.crash_after(1);
        let coordinator = TemporalTxnCoordinator::new(timestamps.clone(), shards.clone());
        assert_eq!(
            block_on(coordinator.commit(&context, vec![participant])),
            Err(TxnError::InjectedCrash)
        );
        assert_eq!(shards.applied_index(ShardId::new(9).unwrap()), 1);
        assert_eq!(shards.graph_mutation_count(), 1);
    }

    let reopened = Arc::new(FjallShards::open(&paths, &bindings));
    let recovery = TemporalTxnCoordinator::new(timestamps.clone(), reopened.clone());
    assert_eq!(
        block_on(recovery.recover(&context)),
        Ok(TransactionOutcome::Committed(transaction_time(50)))
    );
    assert_eq!(reopened.applied_index(ShardId::new(9).unwrap()), 1);
    assert_eq!(reopened.graph_mutation_count(), 1);
    assert_eq!(
        block_on(timestamps.commit_time_reservation(context.snapshot().transaction_id)),
        Ok(Some(CommitTimeReservation::new(
            transaction_time(50),
            Some(CommitResolution::Committed)
        )))
    );
}

#[test]
fn asymmetric_two_phase_recovery_uses_only_durable_write_participants() {
    let timestamps = Arc::new(FakeTimestamps::default());
    let shards = Arc::new(DurableShards::default());
    shards.set_crash(CrashPoint::After(3));
    let coordinator = TemporalTxnCoordinator::new(timestamps, shards.clone());
    let mut context = context(&[3, 9, 11]);
    let first = vertex_mutation(1);
    let second = vertex_mutation(2);
    context.stage(first.clone()).unwrap();
    context.stage(second.clone()).unwrap();
    let participants = vec![
        ParticipantWrite::new(ShardId::new(9).unwrap(), vec![first]).unwrap(),
        ParticipantWrite::new(ShardId::new(11).unwrap(), vec![second]).unwrap(),
    ];

    assert_eq!(
        block_on(coordinator.commit(&context, participants)),
        Err(TxnError::InjectedCrash)
    );
    shards.clear_crash();
    assert_eq!(
        block_on(coordinator.recover(&context)),
        Ok(TransactionOutcome::Committed(transaction_time(50)))
    );
    assert_eq!(shards.graph_mutation_count(), 2);
}

#[test]
fn read_only_transaction_commits_without_timestamp_or_shard_writes() {
    let timestamps = Arc::new(FakeTimestamps::default());
    let shards = Arc::new(FakeShards::default());
    let coordinator = TemporalTxnCoordinator::new(timestamps.clone(), shards.clone());
    let context = context(&[3, 9]);

    assert_eq!(
        block_on(coordinator.commit(&context, Vec::new())),
        Ok(TransactionOutcome::Committed(context.snapshot().start_time))
    );
    assert!(shards.commands.lock().unwrap().is_empty());
    assert!(timestamps.reserved.lock().unwrap().is_empty());
    assert!(timestamps.published.lock().unwrap().is_empty());
}

#[test]
fn read_only_transaction_aborts_without_timestamp_or_shard_recovery() {
    let timestamps = Arc::new(FakeTimestamps::default());
    let shards = Arc::new(FakeShards::default());
    let coordinator = TemporalTxnCoordinator::new(timestamps.clone(), shards.clone());
    let context = context(&[3, 9]);

    assert_eq!(
        block_on(coordinator.abort(&context, Vec::new())),
        Ok(TransactionOutcome::Aborted)
    );
    assert!(shards.commands.lock().unwrap().is_empty());
    assert!(timestamps.reserved.lock().unwrap().is_empty());
    assert!(timestamps.published.lock().unwrap().is_empty());
}

#[test]
fn resolved_single_shard_commit_is_terminal_before_new_conflict_checks() {
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
    shards.changes.lock().unwrap().insert(
        ShardId::new(7).unwrap(),
        vec![ChangeRecord::new(
            ChangeCursor::new(2, 0),
            LogicalMutation::PutVertex(
                VertexVersion::new(
                    VertexId::new(1).unwrap(),
                    Version::new(2),
                    ValidInterval::new(50, 150).unwrap(),
                    transaction_time(60),
                    Default::default(),
                )
                .unwrap(),
            ),
        )],
    );

    assert_eq!(
        block_on(coordinator.commit(&context, vec![participant])),
        Ok(TransactionOutcome::Committed(transaction_time(50)))
    );
    assert_eq!(shards.commands.lock().unwrap().len(), 1);
    assert_eq!(
        block_on(timestamps.commit_time_reservation(context.snapshot().transaction_id)),
        Ok(Some(CommitTimeReservation::new(
            transaction_time(50),
            Some(CommitResolution::Committed)
        )))
    );
}

#[test]
fn definitive_single_shard_rejection_aborts_while_ambiguous_apply_replays() {
    for definitive in [
        TxnError::StalePlacementEpoch,
        TxnError::StaleBackendGeneration,
        TxnError::InvalidMutation,
    ] {
        let timestamps = Arc::new(FakeTimestamps::default());
        let shards = Arc::new(ClassifiedFailureShards::definitive(definitive.clone()));
        let coordinator = TemporalTxnCoordinator::new(timestamps.clone(), shards.clone());
        let mut context = context(&[7]);
        let mutation = vertex_mutation(1);
        context.stage(mutation.clone()).unwrap();
        let participant = ParticipantWrite::new(ShardId::new(7).unwrap(), vec![mutation]).unwrap();

        assert_eq!(
            block_on(coordinator.commit(&context, vec![participant])),
            Err(definitive)
        );
        assert_eq!(
            block_on(timestamps.commit_time_reservation(context.snapshot().transaction_id)),
            Ok(Some(CommitTimeReservation::new(
                transaction_time(50),
                Some(CommitResolution::Aborted)
            )))
        );
        assert_eq!(
            block_on(coordinator.recover(&context)),
            Ok(TransactionOutcome::Aborted)
        );
        assert_eq!(*shards.submit_count.lock().unwrap(), 1);
    }

    let timestamps = Arc::new(FakeTimestamps::default());
    let shards = Arc::new(DurableShards::default());
    shards.set_crash(CrashPoint::After(1));
    let coordinator = TemporalTxnCoordinator::new(timestamps.clone(), shards.clone());
    let mut context = context(&[7]);
    let mutation = vertex_mutation(1);
    context.stage(mutation.clone()).unwrap();
    let participant = ParticipantWrite::new(ShardId::new(7).unwrap(), vec![mutation]).unwrap();

    assert_eq!(
        block_on(coordinator.commit(&context, vec![participant])),
        Err(TxnError::InjectedCrash)
    );
    assert_eq!(
        block_on(timestamps.commit_time_reservation(context.snapshot().transaction_id)),
        Ok(Some(CommitTimeReservation::new(transaction_time(50), None)))
    );
    shards.clear_crash();
    assert_eq!(
        block_on(coordinator.recover(&context)),
        Ok(TransactionOutcome::Committed(transaction_time(50)))
    );
    assert_eq!(shards.graph_mutation_count(), 1);
}

#[test]
fn single_shard_recovery_preserves_submission_failure_classification() {
    for (failure, expected_resolution) in [
        (
            SubmissionFailure::Definitive(TxnError::StalePlacementEpoch),
            Some(CommitResolution::Aborted),
        ),
        (SubmissionFailure::Ambiguous(TxnError::InjectedCrash), None),
    ] {
        let timestamps = Arc::new(FakeTimestamps::default());
        let context = context(&[7]);
        block_on(timestamps.reserve_commit_time(context.snapshot().transaction_id)).unwrap();
        let shards = Arc::new(ClassifiedFailureShards::new(failure.clone()));
        let coordinator = TemporalTxnCoordinator::new(timestamps.clone(), shards.clone());

        assert_eq!(
            block_on(coordinator.recover(&context)),
            Err(failure.into_error())
        );
        assert_eq!(
            block_on(timestamps.commit_time_reservation(context.snapshot().transaction_id)),
            Ok(Some(CommitTimeReservation::new(
                transaction_time(50),
                expected_resolution
            )))
        );
        assert_eq!(*shards.submit_count.lock().unwrap(), 1);
    }

    let timestamps = Arc::new(FakeTimestamps::default());
    let context = context(&[7]);
    block_on(timestamps.reserve_commit_time(context.snapshot().transaction_id)).unwrap();
    timestamps.fail_publish(PublishFailure::Before);
    let shards = Arc::new(ClassifiedFailureShards::definitive(
        TxnError::StalePlacementEpoch,
    ));
    let coordinator = TemporalTxnCoordinator::new(timestamps.clone(), shards);

    assert_eq!(
        block_on(coordinator.recover(&context)),
        Ok(TransactionOutcome::Unresolved)
    );
    assert_eq!(
        block_on(timestamps.commit_time_reservation(context.snapshot().transaction_id)),
        Ok(Some(CommitTimeReservation::new(transaction_time(50), None)))
    );
}

#[test]
fn durable_single_shard_receipt_at_start_time_fails_closed() {
    let timestamps = Arc::new(FakeTimestamps::default());
    let shards = Arc::new(FakeShards::default());
    let coordinator = TemporalTxnCoordinator::new(timestamps.clone(), shards.clone());
    let mut context = context(&[7]);
    let mutation = vertex_mutation(1);
    context.stage(mutation.clone()).unwrap();
    let participant = ParticipantWrite::new(ShardId::new(7).unwrap(), vec![mutation]).unwrap();

    assert_eq!(
        block_on(coordinator.commit(&context, vec![participant])),
        Ok(TransactionOutcome::Committed(transaction_time(50)))
    );
    let request = shards.commands.lock().unwrap()[0].1.clone();
    let ShardRequest::CommitSingleShard {
        header,
        transaction_id,
        start_time,
        snapshot_applied_index,
        ..
    } = request
    else {
        unreachable!();
    };
    let corrupt_request = ShardRequest::CommitSingleShard {
        header,
        transaction_id,
        start_time,
        snapshot_applied_index,
        mutations: vec![vertex_mutation(1)],
    };
    shards.histories.lock().unwrap().insert(
        (ShardId::new(7).unwrap(), transaction_id.get()),
        TransactionHistory::default().with_single_shard_commit(RecoveredSingleShardCommit::new(
            header.command_id(),
            transaction_id,
            start_time,
            snapshot_applied_index,
            start_time,
            corrupt_request.digest(),
        )),
    );
    timestamps
        .reserved
        .lock()
        .unwrap()
        .insert(transaction_id.get(), (start_time.get(), None));

    assert_eq!(
        block_on(coordinator.recover(&context)),
        Err(TxnError::CorruptRecovery)
    );
    assert_eq!(shards.commands.lock().unwrap().len(), 1);
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
    assert_eq!(
        block_on(coordinator.recover(&context)),
        Ok(TransactionOutcome::Unresolved)
    );
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
        Ok(TransactionOutcome::Aborted)
    );
    shards.clear_crash();
    assert_eq!(
        block_on(coordinator.recover(&context)),
        Ok(TransactionOutcome::Aborted)
    );
    assert_eq!(timestamps.published.lock().unwrap().as_slice(), &[50]);

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
fn replacement_coordinator_recovers_orphaned_prepares_from_transaction_id_under_lease() {
    let timestamps = Arc::new(FakeTimestamps::default());
    let shards = Arc::new(DurableShards::default());
    let (context, participants) = two_shard_transaction();
    let manifest = recovery_manifest(&context, &participants, transaction_time(50));
    shards.install_recovery_manifest(manifest);
    shards.set_history_unavailable(true);
    shards.set_crash(CrashPoint::Before(3));
    let original = TemporalTxnCoordinator::new(timestamps.clone(), shards.clone());

    assert_eq!(
        block_on(original.commit(&context, participants)),
        Err(TxnError::InjectedCrash)
    );
    shards.clear_crash();
    shards.set_history_unavailable(false);
    drop(original);

    let replacement = TemporalTxnCoordinator::new(timestamps, shards.clone());
    assert_eq!(
        block_on(replacement.recover_transaction(context.snapshot().transaction_id, 900,)),
        Ok(TransactionOutcome::Aborted)
    );
    assert_eq!(shards.recovery_owners.lock().unwrap().as_slice(), &[900]);
    assert!(shards.recovery_leases.lock().unwrap().is_empty());
}

#[test]
fn recovery_rejects_contradictory_or_corrupt_terminal_history_before_publication() {
    for terminal_case in [
        TerminalCase::ContradictHome,
        TerminalCase::ContradictParticipant,
        TerminalCase::Corrupt,
        TerminalCase::InconsistentDuplicate,
    ] {
        let timestamps = Arc::new(FakeTimestamps::default());
        let shards = Arc::new(DurableShards::default());
        shards.crash_after(3);
        let coordinator = TemporalTxnCoordinator::new(timestamps.clone(), shards.clone());
        let (context, participants) = two_shard_transaction();
        assert_eq!(
            block_on(coordinator.commit(&context, participants)),
            Err(TxnError::InjectedCrash)
        );
        shards.clear_crash();

        let target = match terminal_case {
            TerminalCase::ContradictHome
            | TerminalCase::Corrupt
            | TerminalCase::InconsistentDuplicate => ShardId::new(3).unwrap(),
            TerminalCase::ContradictParticipant => ShardId::new(9).unwrap(),
        };
        let history =
            block_on(shards.transaction_history(target, context.snapshot().transaction_id))
                .unwrap();
        let digest = match terminal_case {
            TerminalCase::Corrupt => dtg_storage::Digest32::new([0xA5; 32]),
            TerminalCase::InconsistentDuplicate => {
                let local = history.intent().unwrap().digest();
                history
                    .terminal()
                    .iter()
                    .find(|record| record.record_digest() != local)
                    .unwrap()
                    .record_digest()
            }
            TerminalCase::ContradictHome | TerminalCase::ContradictParticipant => {
                history.intent().unwrap().digest()
            }
        };
        let state = match terminal_case {
            TerminalCase::Corrupt | TerminalCase::InconsistentDuplicate => {
                TransactionState::Committed
            }
            TerminalCase::ContradictHome | TerminalCase::ContradictParticipant => {
                TransactionState::Aborted
            }
        };
        shards.push_transaction_record(
            target,
            TransactionRecord::new(
                context.snapshot().transaction_id,
                state,
                match terminal_case {
                    TerminalCase::InconsistentDuplicate => transaction_time(51),
                    TerminalCase::Corrupt => transaction_time(50),
                    TerminalCase::ContradictHome | TerminalCase::ContradictParticipant => {
                        context.snapshot().start_time
                    }
                },
                digest,
            )
            .unwrap(),
        );

        assert_eq!(
            block_on(coordinator.recover(&context)),
            Err(TxnError::CorruptRecovery),
            "case {terminal_case:?} must fail closed"
        );
        assert!(timestamps.published.lock().unwrap().is_empty());
    }
}

#[test]
fn abort_recovery_validates_reservation_authority_before_finalizing_participants() {
    for authority in [
        AbortAuthorityCase::Committed,
        AbortAuthorityCase::CorruptPending,
        AbortAuthorityCase::Unavailable,
    ] {
        let timestamps = Arc::new(FakeTimestamps::default());
        let shards = Arc::new(DurableShards::default());
        shards.crash_after(3);
        let coordinator = TemporalTxnCoordinator::new(timestamps.clone(), shards.clone());
        let (context, participants) = two_shard_transaction();
        assert_eq!(
            block_on(coordinator.abort(&context, participants)),
            Err(TxnError::InjectedCrash)
        );
        shards.clear_crash();

        match authority {
            AbortAuthorityCase::Committed => {
                timestamps.reserved.lock().unwrap().insert(
                    context.snapshot().transaction_id.get(),
                    (50, Some(CommitResolution::Committed)),
                );
            }
            AbortAuthorityCase::CorruptPending => {
                timestamps
                    .reserved
                    .lock()
                    .unwrap()
                    .insert(context.snapshot().transaction_id.get(), (40, None));
            }
            AbortAuthorityCase::Unavailable => {
                *timestamps.reservation_unavailable.lock().unwrap() = true;
            }
        }
        let applied_before = shards.applied_commands.lock().unwrap().len();

        let expected = match authority {
            AbortAuthorityCase::Unavailable => Ok(TransactionOutcome::Unresolved),
            AbortAuthorityCase::Committed | AbortAuthorityCase::CorruptPending => {
                Err(TxnError::CorruptRecovery)
            }
        };
        assert_eq!(
            block_on(coordinator.recover(&context)),
            expected,
            "authority case {authority:?}"
        );
        assert_eq!(
            shards.applied_commands.lock().unwrap().len(),
            applied_before,
            "authority case {authority:?} mutated participant history"
        );
    }
}

#[test]
fn public_abort_obeys_existing_reservation_before_mutating_participants() {
    for (resolution, expected) in [
        (
            CommitResolution::Committed,
            TransactionOutcome::Committed(transaction_time(50)),
        ),
        (CommitResolution::Aborted, TransactionOutcome::Aborted),
    ] {
        let timestamps = Arc::new(FakeTimestamps::default());
        let shards = Arc::new(FakeShards::default());
        let coordinator = TemporalTxnCoordinator::new(timestamps.clone(), shards.clone());
        let (context, participants) =
            one_shard_transaction_with(TransactionId::new(601).unwrap(), 11, 1);
        let commit_time =
            block_on(timestamps.reserve_commit_time(context.snapshot().transaction_id)).unwrap();
        block_on(timestamps.resolve_commit_time(
            context.snapshot().transaction_id,
            commit_time,
            resolution,
        ))
        .unwrap();

        assert_eq!(
            block_on(coordinator.abort(&context, participants)),
            Ok(expected)
        );
        assert!(shards.commands.lock().unwrap().is_empty());
    }
}

#[test]
fn public_abort_recovers_durable_single_shard_commit_before_mutation() {
    let timestamps = Arc::new(FakeTimestamps::default());
    timestamps.fail_publish(PublishFailure::Before);
    let shards = Arc::new(FakeShards::default());
    let coordinator = TemporalTxnCoordinator::new(timestamps.clone(), shards.clone());
    let (context, participants) =
        one_shard_transaction_with(TransactionId::new(602).unwrap(), 11, 1);

    assert_eq!(
        block_on(coordinator.commit(&context, participants.clone())),
        Err(TxnError::InjectedCrash)
    );
    let request = shards.commands.lock().unwrap()[0].1.clone();
    let request_digest = request.digest();
    let ShardRequest::CommitSingleShard {
        header,
        transaction_id,
        start_time,
        snapshot_applied_index,
        ..
    } = request
    else {
        unreachable!();
    };
    shards.histories.lock().unwrap().insert(
        (ShardId::new(3).unwrap(), transaction_id.get()),
        TransactionHistory::default().with_single_shard_commit(RecoveredSingleShardCommit::new(
            header.command_id(),
            transaction_id,
            start_time,
            snapshot_applied_index,
            transaction_time(50),
            request_digest,
        )),
    );
    timestamps.clear_publish_failure();
    let applied_before = shards.commands.lock().unwrap().len();
    assert_eq!(
        block_on(coordinator.abort(&context, participants)),
        Ok(TransactionOutcome::Committed(transaction_time(50)))
    );
    assert_eq!(shards.commands.lock().unwrap().len(), applied_before);
    assert_eq!(
        block_on(timestamps.commit_time_reservation(context.snapshot().transaction_id)),
        Ok(Some(CommitTimeReservation::new(
            transaction_time(50),
            Some(CommitResolution::Committed)
        )))
    );
}

#[test]
fn successful_abort_resolves_an_existing_pending_reservation() {
    let timestamps = Arc::new(FakeTimestamps::default());
    let shards = Arc::new(DurableShards::default());
    let coordinator = TemporalTxnCoordinator::new(timestamps.clone(), shards);
    let (context, participants) = two_shard_transaction();
    block_on(timestamps.reserve_commit_time(context.snapshot().transaction_id)).unwrap();

    assert_eq!(
        block_on(coordinator.abort(&context, participants)),
        Ok(TransactionOutcome::Aborted)
    );
    assert_eq!(
        block_on(timestamps.commit_time_reservation(context.snapshot().transaction_id)),
        Ok(Some(CommitTimeReservation::new(
            transaction_time(50),
            Some(CommitResolution::Aborted)
        )))
    );
    assert_eq!(timestamps.published.lock().unwrap().as_slice(), &[50]);
}

#[test]
fn failed_prewrite_checks_committed_authority_before_recording_abort() {
    let timestamps = Arc::new(FakeTimestamps::default());
    let shards = Arc::new(ClassifiedFailureShards::definitive(
        TxnError::StalePlacementEpoch,
    ));
    let coordinator = TemporalTxnCoordinator::new(timestamps.clone(), shards.clone());
    let (context, participants) = two_shard_transaction();
    let commit_time =
        block_on(timestamps.reserve_commit_time(context.snapshot().transaction_id)).unwrap();
    block_on(timestamps.resolve_commit_time(
        context.snapshot().transaction_id,
        commit_time,
        CommitResolution::Committed,
    ))
    .unwrap();

    assert_eq!(
        block_on(coordinator.commit(&context, participants)),
        Ok(TransactionOutcome::Committed(transaction_time(50)))
    );
    assert_eq!(*shards.submit_count.lock().unwrap(), 1);
}

#[test]
fn recovered_prepared_and_abort_records_must_match_the_snapshot_start() {
    let timestamps = Arc::new(FakeTimestamps::default());
    let shards = Arc::new(DurableShards::default());
    shards.crash_after(3);
    let coordinator = TemporalTxnCoordinator::new(timestamps.clone(), shards.clone());
    let (context, participants) = two_shard_transaction();
    assert_eq!(
        block_on(coordinator.commit(&context, participants)),
        Err(TxnError::InjectedCrash)
    );
    shards.clear_crash();
    shards.rewrite_prepared_time(ShardId::new(9).unwrap(), transaction_time(41));
    assert_eq!(
        block_on(coordinator.recover(&context)),
        Err(TxnError::CorruptRecovery)
    );
    assert!(timestamps.published.lock().unwrap().is_empty());

    let timestamps = Arc::new(FakeTimestamps::default());
    let shards = Arc::new(DurableShards::default());
    shards.crash_after(3);
    let coordinator = TemporalTxnCoordinator::new(timestamps, shards.clone());
    let (context, participants) = two_shard_transaction();
    assert_eq!(
        block_on(coordinator.abort(&context, participants)),
        Err(TxnError::InjectedCrash)
    );
    shards.clear_crash();
    shards.rewrite_home_abort_time(ShardId::new(3).unwrap(), transaction_time(41));
    assert_eq!(
        block_on(coordinator.recover(&context)),
        Err(TxnError::CorruptRecovery)
    );
}

#[test]
fn failed_prewrite_resolves_through_history_to_one_durable_abort() {
    for crash in [CrashPoint::After(1), CrashPoint::Before(2)] {
        let timestamps = Arc::new(FakeTimestamps::default());
        let shards = Arc::new(DurableShards::default());
        shards.set_crash(crash);
        let coordinator = TemporalTxnCoordinator::new(timestamps, shards.clone());
        let (context, participants) = two_shard_transaction();

        assert_eq!(
            block_on(coordinator.commit(&context, participants)),
            Ok(TransactionOutcome::Aborted)
        );
        assert_eq!(shards.home_abort_decision_count(), 1);
        assert!(!shards.graph_is_visible());
        let applied = shards.applied_commands.lock().unwrap().len();
        shards.clear_crash();
        assert_eq!(
            block_on(coordinator.recover(&context)),
            Ok(TransactionOutcome::Aborted)
        );
        assert_eq!(shards.home_abort_decision_count(), 1);
        assert_eq!(shards.applied_commands.lock().unwrap().len(), applied);
    }
}

#[test]
fn failed_prewrite_with_unavailable_authority_remains_unresolved() {
    let timestamps = Arc::new(FakeTimestamps::default());
    let shards = Arc::new(DurableShards::default());
    shards.set_crash(CrashPoint::After(1));
    shards.set_history_unavailable(true);
    let coordinator = TemporalTxnCoordinator::new(timestamps, shards.clone());
    let (context, participants) = two_shard_transaction();

    assert_eq!(
        block_on(coordinator.commit(&context, participants)),
        Ok(TransactionOutcome::Unresolved)
    );
    assert_eq!(shards.home_abort_decision_count(), 0);
}

#[test]
fn recovery_uses_resolved_abort_when_no_prewrite_was_durable() {
    let timestamps = Arc::new(FakeTimestamps::default());
    let shards = Arc::new(DurableShards::default());
    shards.set_crash(CrashPoint::Before(1));
    let coordinator = TemporalTxnCoordinator::new(timestamps.clone(), shards.clone());
    let (context, participants) = two_shard_transaction();

    assert_eq!(
        block_on(coordinator.commit(&context, participants)),
        Ok(TransactionOutcome::Aborted)
    );
    assert_eq!(shards.home_abort_decision_count(), 0);
    shards.clear_crash();
    assert_eq!(
        block_on(coordinator.recover(&context)),
        Ok(TransactionOutcome::Aborted)
    );
    assert_eq!(shards.home_abort_decision_count(), 0);
    assert_eq!(
        block_on(timestamps.commit_time_reservation(context.snapshot().transaction_id)),
        Ok(Some(CommitTimeReservation::new(
            transaction_time(50),
            Some(CommitResolution::Aborted)
        )))
    );
}

#[test]
fn crash_matrix_and_visibility_publication_are_recoverable_and_idempotent() {
    for crash in [
        CrashPoint::Before(1),
        CrashPoint::After(1),
        CrashPoint::Before(2),
        CrashPoint::After(2),
    ] {
        let timestamps = Arc::new(FakeTimestamps::default());
        let shards = Arc::new(DurableShards::default());
        shards.set_crash(crash);
        let coordinator = TemporalTxnCoordinator::new(timestamps.clone(), shards.clone());
        let (context, participants) = two_shard_transaction();
        assert_eq!(
            block_on(coordinator.commit(&context, participants)),
            Ok(TransactionOutcome::Aborted)
        );
        shards.clear_crash();
        assert_eq!(
            block_on(coordinator.recover(&context)),
            Ok(TransactionOutcome::Aborted)
        );
        assert_eq!(
            shards.home_abort_decision_count(),
            usize::from(crash != CrashPoint::Before(1))
        );
        assert!(!shards.graph_is_visible());
        assert_eq!(timestamps.published.lock().unwrap().as_slice(), &[50]);
    }

    let timestamps = Arc::new(FakeTimestamps::default());
    let shards = Arc::new(DurableShards::default());
    shards.set_crash(CrashPoint::Before(3));
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
fn later_completed_transaction_cannot_publish_past_an_earlier_pending_reservation() {
    let timestamps = Arc::new(FakeTimestamps::default());
    let shards = Arc::new(DurableShards::default());
    let coordinator = TemporalTxnCoordinator::new(timestamps.clone(), shards.clone());
    let (first_context, first_participants) =
        two_shard_transaction_with(TransactionId::new(99).unwrap(), 1, 2);
    shards.set_crash(CrashPoint::After(4));
    assert_eq!(
        block_on(coordinator.commit(&first_context, first_participants)),
        Err(TxnError::InjectedCrash)
    );
    shards.clear_crash();

    let (second_context, second_participants) =
        two_shard_transaction_with(TransactionId::new(100).unwrap(), 3, 4);
    assert_eq!(
        block_on(coordinator.commit(&second_context, second_participants)),
        Ok(TransactionOutcome::Committed(transaction_time(60)))
    );
    assert_eq!(
        block_on(timestamps.allocate_start_time(TransactionId::new(101).unwrap())),
        Ok(transaction_time(40))
    );

    assert_eq!(
        block_on(coordinator.recover(&first_context)),
        Ok(TransactionOutcome::Committed(transaction_time(50)))
    );
    assert_eq!(
        block_on(timestamps.allocate_start_time(TransactionId::new(102).unwrap())),
        Ok(transaction_time(60))
    );
}

#[test]
fn single_shard_recovery_after_prepublication_crash_replays_then_publishes() {
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
    let restarted = TemporalTxnCoordinator::new(timestamps.clone(), shards.clone());
    assert_eq!(
        block_on(restarted.recover(&context)),
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
        shards.crash_after(2);
        let coordinator = TemporalTxnCoordinator::new(timestamps.clone(), shards.clone());
        let (context, participants) = two_shard_transaction();
        assert_eq!(
            block_on(coordinator.commit(&context, participants.clone())),
            Ok(TransactionOutcome::Aborted)
        );
        assert_eq!(*shards.visibility_at_crash.lock().unwrap(), Some(false));
        assert!(!shards.vertex_is_visible(ShardId::new(3).unwrap(), 2));
        assert!(!shards.vertex_is_visible(ShardId::new(9).unwrap(), 1));

        let second_root = tempfile::tempdir().unwrap();
        let second_paths = BTreeMap::from([
            (ShardId::new(3).unwrap(), second_root.path().join("shard-3")),
            (ShardId::new(9).unwrap(), second_root.path().join("shard-9")),
        ]);
        let decision_shards = Arc::new(FjallShards::open(&second_paths, &bindings));
        decision_shards.crash_after(3);
        let decision_coordinator =
            TemporalTxnCoordinator::new(timestamps.clone(), decision_shards.clone());
        let (decision_context, decision_participants) =
            two_shard_transaction_with(TransactionId::new(100).unwrap(), 3, 4);
        assert_eq!(
            block_on(decision_coordinator.commit(&decision_context, decision_participants)),
            Err(TxnError::InjectedCrash)
        );
        assert_eq!(
            *decision_shards.visibility_at_crash.lock().unwrap(),
            Some(false)
        );
        assert!(!decision_shards.vertex_is_visible(ShardId::new(3).unwrap(), 4));
        assert!(!decision_shards.vertex_is_visible(ShardId::new(9).unwrap(), 3));

        drop(decision_coordinator);
        drop(decision_shards);

        let reopened = Arc::new(FjallShards::open(&second_paths, &bindings));
        let recovery = TemporalTxnCoordinator::new(timestamps.clone(), reopened.clone());
        assert_eq!(
            block_on(recovery.recover(&decision_context)),
            Ok(TransactionOutcome::Committed(transaction_time(60)))
        );
        assert!(reopened.vertex_is_visible(ShardId::new(3).unwrap(), 4));
        assert!(reopened.vertex_is_visible(ShardId::new(9).unwrap(), 3));
        assert_eq!(timestamps.published.lock().unwrap().as_slice(), &[50, 60]);
    }
}

#[test]
fn concurrent_prechecks_cannot_admit_two_overlapping_single_or_prepared_writers() {
    let root = tempfile::tempdir().unwrap();
    let bindings = BTreeMap::from([
        (ShardId::new(3).unwrap(), fixture_binding(3, 4)),
        (ShardId::new(9).unwrap(), fixture_binding(9, 10)),
    ]);
    let paths = BTreeMap::from([
        (ShardId::new(3).unwrap(), root.path().join("shard-3")),
        (ShardId::new(9).unwrap(), root.path().join("shard-9")),
    ]);

    let single_timestamps = Arc::new(FakeTimestamps::default());
    let single_shards = Arc::new(BarrierShards::new(FjallShards::open(&paths, &bindings), 2));
    let single = TemporalTxnCoordinator::new(single_timestamps, single_shards.clone());
    let first = one_shard_transaction_with(TransactionId::new(301).unwrap(), 1, 1);
    let second = one_shard_transaction_with(TransactionId::new(302).unwrap(), 1, 2);
    let first_coordinator = single.clone();
    let second_coordinator = single;
    let first_thread =
        std::thread::spawn(move || block_on(first_coordinator.commit(&first.0, first.1)));
    let second_thread =
        std::thread::spawn(move || block_on(second_coordinator.commit(&second.0, second.1)));
    let single_outcomes = [first_thread.join().unwrap(), second_thread.join().unwrap()];
    assert_eq!(
        single_outcomes
            .iter()
            .filter(|outcome| matches!(outcome, Ok(TransactionOutcome::Committed(_))))
            .count(),
        1
    );
    assert_eq!(single_shards.inner.graph_mutation_count(), 1);

    let prepared_root = tempfile::tempdir().unwrap();
    let prepared_paths = BTreeMap::from([
        (
            ShardId::new(3).unwrap(),
            prepared_root.path().join("shard-3"),
        ),
        (
            ShardId::new(9).unwrap(),
            prepared_root.path().join("shard-9"),
        ),
    ]);
    let prepared_timestamps = Arc::new(FakeTimestamps::default());
    let prepared_shards = Arc::new(BarrierShards::with_prepared_pause(
        FjallShards::open(&prepared_paths, &bindings),
        2,
        ShardId::new(9).unwrap(),
    ));
    let prepared = TemporalTxnCoordinator::new(prepared_timestamps, prepared_shards.clone());
    let first = two_shard_transaction_with(TransactionId::new(401).unwrap(), 7, 8);
    let second = two_shard_transaction_with(TransactionId::new(402).unwrap(), 7, 9);
    let first_coordinator = prepared.clone();
    let second_coordinator = prepared;
    let (outcomes, received) = mpsc::channel();
    let first_outcomes = outcomes.clone();
    let first_thread = std::thread::spawn(move || {
        first_outcomes
            .send(block_on(first_coordinator.commit(&first.0, first.1)))
            .unwrap();
    });
    let second_thread = std::thread::spawn(move || {
        outcomes
            .send(block_on(second_coordinator.commit(&second.0, second.1)))
            .unwrap();
    });

    prepared_shards.wait_until_prepared();
    assert_eq!(
        received.recv_timeout(Duration::from_secs(5)).unwrap(),
        Ok(TransactionOutcome::Aborted)
    );
    prepared_shards.inner.reopen_shard(ShardId::new(9).unwrap());
    let competing = ShardRequest::PrewriteIntent {
        header: dtg_transaction::ShardRequestHeader::new(
            CommandId::new(9_001).unwrap(),
            PlacementEpoch::new(7).unwrap(),
            BackendGeneration::new(10).unwrap(),
        ),
        transaction_id: TransactionId::new(403).unwrap(),
        start_time: transaction_time(40),
        snapshot_applied_index: 0,
        mutations: vec![LogicalMutation::PutVertex(
            VertexVersion::new(
                VertexId::new(7).unwrap(),
                Version::new(99),
                ValidInterval::new(0, 100).unwrap(),
                transaction_time(70),
                Default::default(),
            )
            .unwrap(),
        )],
    };
    assert!(matches!(
        block_on(
            prepared_shards
                .inner
                .submit(ShardId::new(9).unwrap(), competing)
        ),
        Err(SubmissionFailure::Definitive(_))
    ));
    prepared_shards.release_prepared();
    assert!(matches!(
        received.recv_timeout(Duration::from_secs(5)).unwrap(),
        Ok(TransactionOutcome::Committed(_))
    ));
    first_thread.join().unwrap();
    second_thread.join().unwrap();
}

#[test]
fn fjall_single_shard_crash_after_apply_recovers_from_receipt_without_new_apply() {
    let root = tempfile::tempdir().unwrap();
    let shard_id = ShardId::new(3).unwrap();
    let bindings = BTreeMap::from([(shard_id, fixture_binding(3, 4))]);
    let paths = BTreeMap::from([(shard_id, root.path().join("shard-3"))]);
    let timestamps = Arc::new(FakeTimestamps::default());
    let (context, participants) =
        one_shard_transaction_with(TransactionId::new(501).unwrap(), 11, 1);

    {
        let shards = Arc::new(FjallShards::open(&paths, &bindings));
        shards.crash_after(1);
        let coordinator = TemporalTxnCoordinator::new(timestamps.clone(), shards.clone());
        assert_eq!(
            block_on(coordinator.commit(&context, participants.clone())),
            Err(TxnError::InjectedCrash)
        );
        assert_eq!(shards.applied_index(shard_id), 1);
        assert_eq!(shards.graph_mutation_count(), 1);
    }

    let reopened = Arc::new(FjallShards::open(&paths, &bindings));
    let recovery = TemporalTxnCoordinator::new(timestamps.clone(), reopened.clone());
    assert_eq!(
        block_on(recovery.commit(&context, participants)),
        Ok(TransactionOutcome::Committed(transaction_time(50)))
    );
    assert_eq!(reopened.applied_index(shard_id), 2);
    assert_eq!(reopened.graph_mutation_count(), 1);
    assert_eq!(
        block_on(timestamps.commit_time_reservation(context.snapshot().transaction_id)),
        Ok(Some(CommitTimeReservation::new(
            transaction_time(50),
            Some(CommitResolution::Committed)
        )))
    );
}

fn two_shard_transaction() -> (TransactionContext, Vec<ParticipantWrite>) {
    two_shard_transaction_with(TransactionId::new(99).unwrap(), 1, 2)
}

fn one_shard_transaction_with(
    transaction_id: TransactionId,
    vertex_id: u128,
    version: u64,
) -> (TransactionContext, Vec<ParticipantWrite>) {
    let mut context = context_with_transaction(&[3], transaction_id);
    let mutation = LogicalMutation::PutVertex(
        VertexVersion::new(
            VertexId::new(vertex_id).unwrap(),
            Version::new(version),
            ValidInterval::new(0, 100).unwrap(),
            transaction_time(40),
            Default::default(),
        )
        .unwrap(),
    );
    context.stage(mutation.clone()).unwrap();
    (
        context,
        vec![ParticipantWrite::new(ShardId::new(3).unwrap(), vec![mutation]).unwrap()],
    )
}

fn two_shard_transaction_with(
    transaction_id: TransactionId,
    first_vertex: u128,
    second_vertex: u128,
) -> (TransactionContext, Vec<ParticipantWrite>) {
    let mut context = context_with_transaction(&[9, 3], transaction_id);
    let first = vertex_mutation(first_vertex);
    let second = vertex_mutation(second_vertex);
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

fn recovery_manifest(
    context: &TransactionContext,
    participants: &[ParticipantWrite],
    commit_time: TransactionTime,
) -> DurableTransactionManifest {
    let entries = participants
        .iter()
        .map(|participant| {
            let fence = context.snapshot().shards[&participant.shard_id()];
            let mutations = participant
                .mutations()
                .iter()
                .map(|mutation| match mutation {
                    LogicalMutation::PutVertex(vertex) => LogicalMutation::PutVertex(
                        VertexVersion::new(
                            vertex.id(),
                            vertex.version(),
                            vertex.valid_time(),
                            commit_time,
                            vertex.properties().clone(),
                        )
                        .unwrap(),
                    ),
                    _ => panic!("fixture uses only vertex puts"),
                })
                .collect();
            let intent = ParticipantIntent::new(
                context.snapshot().transaction_id,
                participant.shard_id(),
                context.snapshot().start_time,
                fence.applied_index,
                mutations,
            )
            .unwrap();
            DurableParticipantManifest::new(participant.shard_id(), fence, intent.digest()).unwrap()
        })
        .collect();
    DurableTransactionManifest::new(
        context.snapshot().transaction_id,
        context.snapshot().start_time,
        context.snapshot().catalog_version,
        entries,
    )
    .unwrap()
}

fn context(shards: &[u64]) -> TransactionContext {
    context_with_transaction(shards, TransactionId::new(99).unwrap())
}

fn context_with_transaction(shards: &[u64], transaction_id: TransactionId) -> TransactionContext {
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
            transaction_id,
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
    reservation_unavailable: Mutex<bool>,
    reserved: Mutex<BTreeMap<u128, (i64, Option<CommitResolution>)>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PublishFailure {
    Before,
    After,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AbortAuthorityCase {
    Committed,
    CorruptPending,
    Unavailable,
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

    fn reserve_commit_time(&self, transaction_id: TransactionId) -> TxnFuture<'_, TransactionTime> {
        Box::pin(async move {
            let mut reserved = self.reserved.lock().unwrap();
            let next = 50 + (reserved.len() as i64 * 10);
            let value = reserved
                .entry(transaction_id.get())
                .or_insert((next, None))
                .0;
            Ok(transaction_time(value))
        })
    }

    fn commit_time_reservation(
        &self,
        transaction_id: TransactionId,
    ) -> TxnFuture<'_, Option<CommitTimeReservation>> {
        Box::pin(async move {
            if *self.reservation_unavailable.lock().unwrap() {
                return Err(TxnError::InjectedCrash);
            }
            Ok(self
                .reserved
                .lock()
                .unwrap()
                .get(&transaction_id.get())
                .map(|(commit_time, resolution)| {
                    CommitTimeReservation::new(transaction_time(*commit_time), *resolution)
                }))
        })
    }

    fn resolve_commit_time(
        &self,
        transaction_id: TransactionId,
        commit_time: TransactionTime,
        resolution: CommitResolution,
    ) -> TxnFuture<'_, ()> {
        Box::pin(async move {
            if *self.publish_failure.lock().unwrap() == Some(PublishFailure::Before) {
                return Err(TxnError::InjectedCrash);
            }
            let mut reserved = self.reserved.lock().unwrap();
            let Some((reserved_time, current_resolution)) = reserved.get_mut(&transaction_id.get())
            else {
                return Err(TxnError::CorruptRecovery);
            };
            if *reserved_time != commit_time.get()
                || current_resolution.is_some_and(|current| current != resolution)
            {
                return Err(TxnError::CorruptRecovery);
            }
            *current_resolution = Some(resolution);

            let current_frontier = self
                .published
                .lock()
                .unwrap()
                .iter()
                .copied()
                .max()
                .unwrap_or(40);
            let mut ordered: Vec<_> = reserved.values().copied().collect();
            ordered.sort_by_key(|(time, _)| *time);
            let mut next_frontier = current_frontier;
            for (time, resolution) in ordered {
                if time <= current_frontier {
                    continue;
                }
                if resolution.is_none() {
                    break;
                }
                next_frontier = time;
            }
            drop(reserved);
            if next_frontier > current_frontier {
                self.published.lock().unwrap().push(next_frontier);
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
    histories: Mutex<BTreeMap<(ShardId, u128), TransactionHistory>>,
}

struct ClassifiedFailureShards {
    failure: SubmissionFailure,
    submit_count: Mutex<usize>,
}

impl ClassifiedFailureShards {
    fn new(failure: SubmissionFailure) -> Self {
        Self {
            failure,
            submit_count: Mutex::new(0),
        }
    }

    fn definitive(failure: TxnError) -> Self {
        Self::new(SubmissionFailure::Definitive(failure))
    }
}

impl ShardCommandExecutor for ClassifiedFailureShards {
    fn submit(&self, _shard_id: ShardId, _request: ShardRequest) -> SubmissionFuture<'_> {
        Box::pin(async move {
            *self.submit_count.lock().unwrap() += 1;
            Err(self.failure.clone())
        })
    }

    fn changes_after(
        &self,
        _shard_id: ShardId,
        _applied_index: u64,
    ) -> TxnFuture<'_, Vec<ChangeRecord>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn transaction_history(
        &self,
        _shard_id: ShardId,
        _transaction_id: TransactionId,
    ) -> TxnFuture<'_, TransactionHistory> {
        Box::pin(async { Ok(TransactionHistory::default()) })
    }
}

impl ShardCommandExecutor for FakeShards {
    fn submit(&self, shard_id: ShardId, request: ShardRequest) -> SubmissionFuture<'_> {
        Box::pin(async move {
            let (_, intent_digest) =
                map_request_to_shard(shard_id, &request).map_err(SubmissionFailure::Definitive)?;
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
        shard_id: ShardId,
        transaction_id: TransactionId,
    ) -> TxnFuture<'_, TransactionHistory> {
        Box::pin(async move {
            Ok(self
                .histories
                .lock()
                .unwrap()
                .get(&(shard_id, transaction_id.get()))
                .cloned()
                .unwrap_or_default())
        })
    }
}

#[derive(Default)]
struct DurableShards {
    submit_count: Mutex<usize>,
    crash: Mutex<Option<CrashPoint>>,
    applied_commands: Mutex<BTreeSet<u128>>,
    durable: Mutex<BTreeMap<ShardId, Vec<ChangeRecord>>>,
    history_unavailable: Mutex<bool>,
    recovery_manifest: Mutex<Option<DurableTransactionManifest>>,
    recovery_leases: Mutex<BTreeMap<u128, RecoveryLease>>,
    recovery_owners: Mutex<Vec<u128>>,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TerminalCase {
    ContradictHome,
    ContradictParticipant,
    Corrupt,
    InconsistentDuplicate,
}

impl DurableShards {
    fn install_recovery_manifest(&self, manifest: DurableTransactionManifest) {
        *self.recovery_manifest.lock().unwrap() = Some(manifest);
    }

    fn crash_after(&self, boundary: usize) {
        self.set_crash(CrashPoint::After(boundary));
    }

    fn set_crash(&self, crash: CrashPoint) {
        *self.crash.lock().unwrap() = Some(crash);
    }

    fn clear_crash(&self) {
        *self.crash.lock().unwrap() = None;
    }

    fn set_history_unavailable(&self, unavailable: bool) {
        *self.history_unavailable.lock().unwrap() = unavailable;
    }

    fn push_transaction_record(&self, shard_id: ShardId, record: TransactionRecord) {
        let mut durable = self.durable.lock().unwrap();
        let history = durable.entry(shard_id).or_default();
        let raft_index = history
            .iter()
            .map(ChangeRecord::raft_index)
            .max()
            .unwrap_or(0)
            .saturating_add(1);
        history.push(ChangeRecord::new(
            ChangeCursor::new(raft_index, 0),
            LogicalMutation::PutTransaction(record),
        ));
    }

    fn rewrite_prepared_time(&self, shard_id: ShardId, time: TransactionTime) {
        self.rewrite_transaction_record(shard_id, |record| {
            (record.state() == TransactionState::Prepared).then(|| {
                TransactionRecord::new(record.id(), record.state(), time, record.record_digest())
                    .unwrap()
            })
        });
    }

    fn rewrite_home_abort_time(&self, shard_id: ShardId, time: TransactionTime) {
        self.rewrite_transaction_record(shard_id, |record| {
            (record.state() == TransactionState::Aborted).then(|| {
                TransactionRecord::new(record.id(), record.state(), time, record.record_digest())
                    .unwrap()
            })
        });
    }

    fn rewrite_transaction_record(
        &self,
        shard_id: ShardId,
        rewrite: impl Fn(&TransactionRecord) -> Option<TransactionRecord>,
    ) {
        let mut durable = self.durable.lock().unwrap();
        let change = durable
            .get_mut(&shard_id)
            .unwrap()
            .iter_mut()
            .find_map(|change| match change.mutation() {
                LogicalMutation::PutTransaction(record) => {
                    rewrite(record).map(|replacement| (change, replacement))
                }
                _ => None,
            })
            .unwrap();
        let cursor = change.0.cursor();
        *change.0 = ChangeRecord::new(cursor, LogicalMutation::PutTransaction(change.1));
    }

    fn home_abort_decision_count(&self) -> usize {
        let shard_id = ShardId::new(3).unwrap();
        let durable = self.durable.lock().unwrap();
        let changes = durable.get(&shard_id).cloned().unwrap_or_default();
        let history =
            transaction_history_from_changes(shard_id, TransactionId::new(99).unwrap(), &changes)
                .unwrap();
        let intent_digest = history.intent().map(RecoveredParticipantIntent::digest);
        history
            .terminal()
            .iter()
            .filter(|record| {
                record.state() == TransactionState::Aborted
                    && Some(record.record_digest()) != intent_digest
            })
            .count()
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
            IntentCorruption::UnknownVersion => bytes[..4].copy_from_slice(&4_u32.to_be_bytes()),
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
    fn submit(&self, shard_id: ShardId, request: ShardRequest) -> SubmissionFuture<'_> {
        Box::pin(async move {
            let boundary = {
                let mut count = self.submit_count.lock().unwrap();
                *count += 1;
                *count
            };
            if *self.crash.lock().unwrap() == Some(CrashPoint::Before(boundary)) {
                return Err(SubmissionFailure::Definitive(TxnError::InjectedCrash));
            }
            let command_id = request.header().command_id().get();
            let (command, intent_digest) =
                map_request_to_shard(shard_id, &request).map_err(SubmissionFailure::Definitive)?;
            let replayed = !self.applied_commands.lock().unwrap().insert(command_id);
            if !replayed {
                let mutations =
                    materialize_command(&command).map_err(SubmissionFailure::Definitive)?;
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
                return Err(SubmissionFailure::Ambiguous(TxnError::InjectedCrash));
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
            if *self.history_unavailable.lock().unwrap() {
                return Err(TxnError::InjectedCrash);
            }
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

    fn acquire_recovery_lease(
        &self,
        transaction_id: TransactionId,
        owner: u128,
    ) -> TxnFuture<'_, RecoveryLease> {
        Box::pin(async move {
            let lease = RecoveryLease::new(transaction_id, owner, 1)?;
            self.recovery_owners.lock().unwrap().push(owner);
            self.recovery_leases
                .lock()
                .unwrap()
                .insert(transaction_id.get(), lease);
            Ok(lease)
        })
    }

    fn recovery_manifest(
        &self,
        transaction_id: TransactionId,
        lease: RecoveryLease,
    ) -> TxnFuture<'_, Option<DurableTransactionManifest>> {
        Box::pin(async move {
            if self
                .recovery_leases
                .lock()
                .unwrap()
                .get(&transaction_id.get())
                != Some(&lease)
            {
                return Err(TxnError::CorruptRecovery);
            }
            Ok(self.recovery_manifest.lock().unwrap().clone())
        })
    }

    fn release_recovery_lease(&self, lease: RecoveryLease) -> TxnFuture<'_, ()> {
        Box::pin(async move {
            self.recovery_leases
                .lock()
                .unwrap()
                .remove(&lease.transaction_id().get());
            Ok(())
        })
    }
}

fn materialize_command(command: &PhysicalShardCommand) -> Result<Vec<LogicalMutation>, TxnError> {
    match command {
        PhysicalShardCommand::CommitSingleShard(command) => Ok(command.mutations().to_vec()),
        PhysicalShardCommand::CommitSingleShardTransaction(command) => {
            Ok(command.mutations().to_vec())
        }
        PhysicalShardCommand::PrewriteIntent(command) => Ok(vec![
            LogicalMutation::PutTransaction(command.prepared().clone()),
            LogicalMutation::PutReplicaMetadata(ReplicaMetadata::new(
                dtg_shard::TRANSACTION_INTENT_METADATA_NAME,
                Value::Bytes(shard_result(command.intent().encode_current())?),
            )?),
        ]),
        PhysicalShardCommand::RecordHomeDecision(command) => {
            Ok(vec![LogicalMutation::PutTransaction(
                command.decision().clone(),
            )])
        }
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

fn shard_submission_failure(error: ShardError) -> SubmissionFailure {
    let definitive = matches!(
        &error,
        ShardError::InvalidCommand(_)
            | ShardError::UnsupportedCommandVersion(_)
            | ShardError::StalePlacementEpoch { .. }
            | ShardError::StaleBackendGeneration { .. }
            | ShardError::WriteConflict
            | ShardError::CorruptIntentHistory(_)
            | ShardError::BindingMismatch
    );
    let error = TxnError::Shard(error.to_string());
    if definitive {
        SubmissionFailure::Definitive(error)
    } else {
        SubmissionFailure::Ambiguous(error)
    }
}

fn map_request_to_shard(
    shard_id: ShardId,
    request: &ShardRequest,
) -> Result<(PhysicalShardCommand, Option<dtg_storage::Digest32>), TxnError> {
    let request_digest = request.digest();
    match request {
        ShardRequest::CommitSingleShard {
            header,
            transaction_id,
            start_time,
            snapshot_applied_index,
            mutations,
        } => Ok((
            PhysicalShardCommand::CommitSingleShardTransaction(shard_result(
                CommitSingleShardTransaction::new(
                    header.command_id(),
                    header.placement_epoch().get(),
                    header.backend_generation().get(),
                    *transaction_id,
                    *start_time,
                    *snapshot_applied_index,
                    request_digest,
                    mutations.clone(),
                ),
            )?),
            None,
        )),
        ShardRequest::PrewriteIntent {
            header,
            transaction_id,
            start_time,
            snapshot_applied_index,
            mutations,
        } => {
            let intent = shard_result(ParticipantIntent::new(
                *transaction_id,
                shard_id,
                *start_time,
                *snapshot_applied_index,
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
        ShardRequest::RecordHomeDecision {
            header,
            decision,
            manifest,
        } => {
            let participants = shard_result(
                manifest
                    .participants()
                    .iter()
                    .map(|participant| {
                        PhysicalHomeDecisionParticipant::new(
                            participant.shard_id(),
                            participant.fence().placement_epoch,
                            participant.fence().backend_generation,
                            participant.fence().applied_index,
                            participant.intent_digest(),
                        )
                    })
                    .collect::<Result<Vec<_>, _>>(),
            )?;
            let physical_manifest = shard_result(PhysicalHomeDecisionManifest::new(
                manifest.start_time(),
                manifest.catalog_version(),
                participants,
            ))?;
            Ok((
                PhysicalShardCommand::RecordHomeDecision(shard_result(RecordHomeDecision::new(
                    header.command_id(),
                    header.placement_epoch().get(),
                    header.backend_generation().get(),
                    decision.clone(),
                    physical_manifest,
                ))?),
                None,
            ))
        }
        ShardRequest::FinalizeParticipantCommit {
            header,
            transaction_id,
            start_time,
            snapshot_applied_index,
            commit_time,
            intent_digest,
            mutations,
        } => {
            let intent = shard_result(ParticipantIntent::new(
                *transaction_id,
                shard_id,
                *start_time,
                *snapshot_applied_index,
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
    let mut single_shard_commit = None;
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
                        candidate.start_time(),
                        candidate.snapshot_applied_index(),
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
            LogicalMutation::PutReplicaMetadata(metadata)
                if metadata
                    .name()
                    .starts_with(dtg_shard::SINGLE_SHARD_TRANSACTION_METADATA_NAME) =>
            {
                let receipt = decode_single_shard_transaction_metadata(metadata)
                    .map_err(|_| TxnError::CorruptRecovery)?;
                if receipt.transaction_id() == transaction_id {
                    let recovered = RecoveredSingleShardCommit::new(
                        receipt.command_id(),
                        receipt.transaction_id(),
                        receipt.start_time(),
                        receipt.snapshot_applied_index(),
                        receipt.commit_time(),
                        receipt.request_digest(),
                    );
                    if single_shard_commit
                        .replace(recovered)
                        .is_some_and(|existing| existing != recovered)
                    {
                        return Err(TxnError::CorruptRecovery);
                    }
                }
            }
            _ => {}
        }
    }
    let history = TransactionHistory::new(prepared, intent, terminal);
    Ok(match single_shard_commit {
        Some(commit) => history.with_single_shard_commit(commit),
        None => history,
    })
}

struct FjallShards {
    replicas: Mutex<BTreeMap<ShardId, FjallReplica>>,
    submit_count: Mutex<usize>,
    crash_after: Mutex<Option<usize>>,
    visibility_at_crash: Mutex<Option<bool>>,
}

struct FjallReplica {
    store: FjallReplicaStore,
    machine: ShardStateMachine,
}

impl FjallShards {
    fn open(
        paths: &BTreeMap<ShardId, PathBuf>,
        bindings: &BTreeMap<ShardId, ReplicaBinding>,
    ) -> Self {
        let replicas = paths
            .iter()
            .map(|(shard_id, path)| {
                let binding = bindings[shard_id].clone();
                let store = FjallReplicaStore::open(path, binding.clone()).unwrap();
                let machine = ShardStateMachine::new(binding, Arc::new(store.clone())).unwrap();
                (*shard_id, FjallReplica { store, machine })
            })
            .collect();
        Self {
            replicas: Mutex::new(replicas),
            submit_count: Mutex::new(0),
            crash_after: Mutex::new(None),
            visibility_at_crash: Mutex::new(None),
        }
    }

    fn crash_after(&self, boundary: usize) {
        *self.crash_after.lock().unwrap() = Some(boundary);
    }

    fn reopen_shard(&self, shard_id: ShardId) {
        let mut replicas = self.replicas.lock().unwrap();
        let store = replicas[&shard_id].store.clone();
        let machine =
            ShardStateMachine::new(store.binding().clone(), Arc::new(store.clone())).unwrap();
        replicas.insert(shard_id, FjallReplica { store, machine });
    }

    fn vertex_is_visible(&self, shard_id: ShardId, vertex_id: u128) -> bool {
        let store = self.replicas.lock().unwrap()[&shard_id].store.clone();
        let applied = block_on(store.applied_index()).unwrap();
        let view =
            block_on(store.begin_read_view(ReadFence::new(store.binding().clone(), applied)))
                .unwrap();
        block_on(view.get_vertex(VertexRead::new(
            VertexId::new(vertex_id).unwrap(),
            50,
            transaction_time(100),
        )))
        .unwrap()
        .is_some()
    }

    fn applied_index(&self, shard_id: ShardId) -> u64 {
        let store = self.replicas.lock().unwrap()[&shard_id].store.clone();
        block_on(store.applied_index()).unwrap()
    }

    fn graph_mutation_count(&self) -> usize {
        let shard_ids: Vec<_> = self.replicas.lock().unwrap().keys().copied().collect();
        shard_ids
            .into_iter()
            .map(|shard_id| {
                block_on(self.changes_after(shard_id, 0))
                    .unwrap()
                    .into_iter()
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
            })
            .sum()
    }

    fn graph_mutations_are_durable(&self) -> TxnFuture<'_, bool> {
        Box::pin(async move {
            let shard_ids: Vec<_> = self.replicas.lock().unwrap().keys().copied().collect();
            for shard_id in shard_ids {
                if self.changes_after(shard_id, 0).await?.iter().any(|change| {
                    matches!(
                        change.mutation(),
                        LogicalMutation::PutVertex(_)
                            | LogicalMutation::DeleteVertex(_)
                            | LogicalMutation::PutEdge(_)
                            | LogicalMutation::DeleteEdge(_)
                    )
                }) {
                    return Ok(true);
                }
            }
            Ok(false)
        })
    }
}

struct BarrierShards {
    inner: FjallShards,
    prechecks: Barrier,
    prewrite_submissions: Barrier,
    pause_shard: Option<ShardId>,
    prepared_pause: Mutex<PreparedPause>,
    prepared_changed: Condvar,
}

#[derive(Default)]
struct PreparedPause {
    reached: bool,
    released: bool,
}

impl BarrierShards {
    fn new(inner: FjallShards, parties: usize) -> Self {
        Self {
            inner,
            prechecks: Barrier::new(parties),
            prewrite_submissions: Barrier::new(parties),
            pause_shard: None,
            prepared_pause: Mutex::new(PreparedPause::default()),
            prepared_changed: Condvar::new(),
        }
    }

    fn with_prepared_pause(inner: FjallShards, parties: usize, pause_shard: ShardId) -> Self {
        Self {
            pause_shard: Some(pause_shard),
            ..Self::new(inner, parties)
        }
    }

    fn wait_until_prepared(&self) {
        let mut pause = self.prepared_pause.lock().unwrap();
        while !pause.reached {
            pause = self.prepared_changed.wait(pause).unwrap();
        }
    }

    fn release_prepared(&self) {
        let mut pause = self.prepared_pause.lock().unwrap();
        pause.released = true;
        self.prepared_changed.notify_all();
    }
}

impl ShardCommandExecutor for BarrierShards {
    fn submit(&self, shard_id: ShardId, request: ShardRequest) -> SubmissionFuture<'_> {
        let prewrite = matches!(request, ShardRequest::PrewriteIntent { .. });
        Box::pin(async move {
            let result = self.inner.submit(shard_id, request).await;
            if prewrite {
                self.prewrite_submissions.wait();
                if self.pause_shard == Some(shard_id) && result.is_ok() {
                    let mut pause = self.prepared_pause.lock().unwrap();
                    pause.reached = true;
                    self.prepared_changed.notify_all();
                    while !pause.released {
                        pause = self.prepared_changed.wait(pause).unwrap();
                    }
                }
            }
            result
        })
    }

    fn changes_after(
        &self,
        shard_id: ShardId,
        applied_index: u64,
    ) -> TxnFuture<'_, Vec<ChangeRecord>> {
        Box::pin(async move {
            let changes = self.inner.changes_after(shard_id, applied_index).await?;
            self.prechecks.wait();
            Ok(changes)
        })
    }

    fn transaction_history(
        &self,
        shard_id: ShardId,
        transaction_id: TransactionId,
    ) -> TxnFuture<'_, TransactionHistory> {
        self.inner.transaction_history(shard_id, transaction_id)
    }
}

impl ShardCommandExecutor for FjallShards {
    fn submit(&self, shard_id: ShardId, request: ShardRequest) -> SubmissionFuture<'_> {
        Box::pin(async move {
            let boundary = {
                let mut count = self.submit_count.lock().unwrap();
                *count += 1;
                *count
            };
            let (command, intent_digest) =
                map_request_to_shard(shard_id, &request).map_err(SubmissionFailure::Definitive)?;
            let receipt = {
                let mut replicas = self.replicas.lock().unwrap();
                let replica = replicas.get_mut(&shard_id).unwrap();
                let index = replica.machine.applied_index().saturating_add(1);
                replica
                    .machine
                    .apply_committed(1, index, command)
                    .map_err(shard_submission_failure)?
            };
            if let Some(rejection) = receipt.rejection() {
                return Err(apply_rejection_failure(rejection));
            }
            if *self.crash_after.lock().unwrap() == Some(boundary) {
                *self.visibility_at_crash.lock().unwrap() = Some(
                    self.graph_mutations_are_durable()
                        .await
                        .map_err(SubmissionFailure::Ambiguous)?,
                );
                return Err(SubmissionFailure::Ambiguous(TxnError::InjectedCrash));
            }
            Ok(match intent_digest {
                Some(digest) => {
                    SubmissionReceipt::prepared(receipt.applied_index(), receipt.replayed(), digest)
                }
                None => SubmissionReceipt::new(receipt.applied_index(), receipt.replayed()),
            })
        })
    }

    fn changes_after(
        &self,
        shard_id: ShardId,
        applied_index: u64,
    ) -> TxnFuture<'_, Vec<ChangeRecord>> {
        Box::pin(async move {
            let store = self.replicas.lock().unwrap()[&shard_id].store.clone();
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

fn apply_rejection_failure(rejection: ApplyRejection) -> SubmissionFailure {
    let error = match rejection {
        ApplyRejection::StalePlacementEpoch => TxnError::StalePlacementEpoch,
        ApplyRejection::StaleBackendGeneration => TxnError::StaleBackendGeneration,
        ApplyRejection::WriteConflict => TxnError::WriteConflict,
        ApplyRejection::InvalidCommand | ApplyRejection::ClosedTimestampFenced => {
            TxnError::InvalidMutation
        }
        ApplyRejection::HomeDecisionConflict => TxnError::Shard(rejection.code().to_owned()),
    };
    SubmissionFailure::Definitive(error)
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
