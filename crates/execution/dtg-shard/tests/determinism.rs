use std::collections::BTreeMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};

use dtg_shard::{
    AdvanceClosedTimestamp, ApplyRejection, CommitSingleShard, CommitSingleShardTransaction,
    FinalizeParticipant, HomeDecisionManifest, HomeDecisionParticipant, ParticipantIntent,
    PrewriteIntent, RecordHomeDecision, SINGLE_SHARD_TRANSACTION_METADATA_NAME,
    SUPPORTED_SHARD_COMMAND_FORMAT_VERSION, ShardCommand, ShardStateMachine,
    decode_single_shard_transaction_metadata,
};
use dtg_storage::{
    ApplyReceipt, BackendClass, BackendGeneration, BindingRole, CapabilityManifest, ChangesRead,
    CommandId, CommittedShardBatch, Digest32, LogicalMutation, PlacementEpoch, Properties,
    ProviderKind, ReadFence, ReplicaBinding, ReplicaMetadata, ReplicaStateStore, StorageError,
    StoreFuture, TemporalReadView, TransactionId, TransactionRecord, TransactionState,
    TransactionTime, ValidInterval, Value, Version, VertexId, VertexRead, VertexVersion,
};
use dtg_storage_fjall::FjallReplicaStore;

#[test]
fn replaying_one_command_is_idempotent() {
    let mut machine = fixture_machine();
    let command = committed_vertex_command(7);
    let first = machine.apply_committed(11, 7, command.clone()).unwrap();
    let second = machine.apply_committed(11, 7, command).unwrap();
    assert_eq!(first.digest(), second.digest());
    assert_eq!(machine.applied_index(), 7);
}

#[test]
fn applies_non_conflicting_single_shard_transactions_in_one_state_store_batch() {
    let root = tempfile::tempdir().unwrap();
    let binding = fjall_fixture_binding();
    let store = Arc::new(FjallReplicaStore::open(root.path(), binding.clone()).unwrap());
    let mut machine = ShardStateMachine::new(binding.clone(), store.clone()).unwrap();
    let commands = vec![
        (
            11,
            1,
            ShardCommand::CommitSingleShardTransaction(
                CommitSingleShardTransaction::new(
                    CommandId::new(601).unwrap(),
                    7,
                    10,
                    TransactionId::new(701).unwrap(),
                    TransactionTime::new(40).unwrap(),
                    0,
                    Digest32::new([6; 32]),
                    vec![vertex_mutation_at(61, 1, 0, 100, 50)],
                )
                .unwrap(),
            ),
        ),
        (
            11,
            2,
            ShardCommand::CommitSingleShardTransaction(
                CommitSingleShardTransaction::new(
                    CommandId::new(602).unwrap(),
                    7,
                    10,
                    TransactionId::new(702).unwrap(),
                    TransactionTime::new(41).unwrap(),
                    0,
                    Digest32::new([7; 32]),
                    vec![vertex_mutation_at(62, 1, 0, 100, 51)],
                )
                .unwrap(),
            ),
        ),
    ];

    let outcomes = machine.apply_committed_batch(commands).unwrap();

    assert_eq!(outcomes.len(), 2);
    assert!(outcomes.iter().all(|outcome| !outcome.replayed()));
    assert_eq!(machine.applied_index(), 2);
    let view = block_on(store.begin_read_view(ReadFence::new(binding, 2))).unwrap();
    assert!(
        block_on(view.get_vertex(VertexRead::new(
            VertexId::new(61).unwrap(),
            1,
            TransactionTime::new(51).unwrap(),
        )))
        .unwrap()
        .is_some()
    );
}

#[test]
fn exact_single_shard_transaction_replay_at_a_new_index_is_metadata_only() {
    let root = tempfile::tempdir().unwrap();
    let binding = fjall_fixture_binding();
    let path = root.path().join("single-replay");
    let command = ShardCommand::CommitSingleShardTransaction(
        CommitSingleShardTransaction::new(
            CommandId::new(501).unwrap(),
            7,
            10,
            TransactionId::new(502).unwrap(),
            TransactionTime::new(40).unwrap(),
            0,
            Digest32::new([5; 32]),
            vec![vertex_mutation_at(50, 1, 0, 100, 50)],
        )
        .unwrap(),
    );

    {
        let store = Arc::new(FjallReplicaStore::open(&path, binding.clone()).unwrap());
        let mut machine = ShardStateMachine::new(binding.clone(), store).unwrap();
        let first = machine.apply_committed(11, 1, command.clone()).unwrap();
        assert!(!first.replayed());
    }

    let store = Arc::new(FjallReplicaStore::open(&path, binding.clone()).unwrap());
    let mut reopened = ShardStateMachine::new(binding.clone(), store.clone()).unwrap();
    let replay = reopened.apply_committed(11, 2, command).unwrap();
    assert!(replay.replayed());
    assert_eq!(replay.applied_index(), 2);
    let view = block_on(store.begin_read_view(ReadFence::new(binding, 2))).unwrap();
    let changes = block_on(view.changes(ChangesRead::new(None, 2, 32).unwrap())).unwrap();
    assert_eq!(
        changes
            .rows()
            .iter()
            .filter(|change| matches!(change.mutation(), LogicalMutation::PutVertex(_)))
            .count(),
        1
    );
}

#[test]
fn stale_backend_generation_is_rejected_as_a_committed_noop() {
    let mut machine = fixture_machine();
    let outcome = machine
        .apply_committed(11, 1, command_for_generation(9))
        .unwrap();
    assert_eq!(
        outcome.rejection(),
        Some(ApplyRejection::StaleBackendGeneration)
    );
    assert_eq!(outcome.applied_index(), 1);
    assert_eq!(machine.applied_index(), 1);
}

#[test]
fn closed_timestamp_is_recorded_through_the_state_store() {
    let binding = fixture_binding();
    let store = Arc::new(RecordingStore::new(binding.clone()));
    let mut machine = ShardStateMachine::new(binding, store.clone()).unwrap();
    let timestamp = TransactionTime::new(44).unwrap();
    let command = ShardCommand::AdvanceClosedTimestamp(
        AdvanceClosedTimestamp::new(CommandId::new(99).unwrap(), 7, 10, timestamp).unwrap(),
    );

    machine.apply_committed(11, 1, command).unwrap();

    assert_eq!(machine.closed_timestamp(), Some(timestamp));
    let state = store.state.lock().unwrap();
    assert_eq!(state.batch.as_ref().unwrap().mutations().len(), 1);
}

#[test]
fn pending_prepared_intent_rejects_unsafe_closed_time_as_an_applied_noop() {
    let root = tempfile::tempdir().unwrap();
    let binding = fjall_fixture_binding();
    let path = root.path().join("closed-time-fence");
    let store = Arc::new(FjallReplicaStore::open(&path, binding.clone()).unwrap());
    let mut machine = ShardStateMachine::new(binding.clone(), store).unwrap();
    let intent = ParticipantIntent::new(
        TransactionId::new(90).unwrap(),
        binding.shard_id(),
        TransactionTime::new(40).unwrap(),
        0,
        vec![vertex_mutation_at(90, 1, 0, 100, 50)],
    )
    .unwrap();
    let prepared = TransactionRecord::new(
        intent.transaction_id(),
        TransactionState::Prepared,
        intent.start_time(),
        intent.digest(),
    )
    .unwrap();
    machine
        .apply_committed(
            11,
            1,
            ShardCommand::PrewriteIntent(
                PrewriteIntent::new(CommandId::new(90).unwrap(), 7, 10, prepared, intent).unwrap(),
            ),
        )
        .unwrap();

    let rejected = machine
        .apply_committed(
            11,
            2,
            ShardCommand::AdvanceClosedTimestamp(
                AdvanceClosedTimestamp::new(
                    CommandId::new(91).unwrap(),
                    7,
                    10,
                    TransactionTime::new(50).unwrap(),
                )
                .unwrap(),
            ),
        )
        .unwrap();

    assert_eq!(
        rejected.rejection(),
        Some(ApplyRejection::ClosedTimestampFenced)
    );
    assert_eq!(machine.applied_index(), 2);
    assert_eq!(machine.closed_timestamp(), None);
    drop(machine);

    let reopened = Arc::new(FjallReplicaStore::open(&path, binding.clone()).unwrap());
    let mut machine = ShardStateMachine::new(binding, reopened).unwrap();
    let later = machine
        .apply_committed(11, 3, committed_vertex_command(92))
        .unwrap();
    assert_eq!(later.rejection(), None);
    assert_eq!(machine.applied_index(), 3);
}

#[test]
fn committed_write_conflict_is_rejected_without_stalling_later_entries_or_restart() {
    let root = tempfile::tempdir().unwrap();
    let binding = fjall_fixture_binding();
    let path = root.path().join("committed-rejection");
    let store = Arc::new(FjallReplicaStore::open(&path, binding.clone()).unwrap());
    let mut machine = ShardStateMachine::new(binding.clone(), store).unwrap();
    machine
        .apply_committed(
            11,
            1,
            ShardCommand::CommitSingleShardTransaction(
                CommitSingleShardTransaction::new(
                    CommandId::new(101).unwrap(),
                    7,
                    10,
                    TransactionId::new(101).unwrap(),
                    TransactionTime::new(40).unwrap(),
                    0,
                    Digest32::new([1; 32]),
                    vec![vertex_mutation_at(101, 1, 0, 100, 50)],
                )
                .unwrap(),
            ),
        )
        .unwrap();
    let conflict = machine
        .apply_committed(
            11,
            2,
            ShardCommand::CommitSingleShardTransaction(
                CommitSingleShardTransaction::new(
                    CommandId::new(102).unwrap(),
                    7,
                    10,
                    TransactionId::new(102).unwrap(),
                    TransactionTime::new(40).unwrap(),
                    0,
                    Digest32::new([2; 32]),
                    vec![vertex_mutation_at(101, 2, 50, 150, 60)],
                )
                .unwrap(),
            ),
        )
        .unwrap();
    assert_eq!(conflict.rejection(), Some(ApplyRejection::WriteConflict));
    machine
        .apply_committed(11, 3, committed_vertex_command(103))
        .unwrap();
    drop(machine);

    let reopened = Arc::new(FjallReplicaStore::open(&path, binding.clone()).unwrap());
    let machine = ShardStateMachine::new(binding, reopened).unwrap();
    assert_eq!(machine.applied_index(), 3);
}

#[test]
fn command_encoding_is_deterministic_and_round_trips() {
    let command = committed_vertex_command(7);
    let first = command.encode_current().unwrap();
    let second = command.clone().encode_current().unwrap();

    assert_eq!(first, second);
    assert_eq!(ShardCommand::decode(&first).unwrap(), command);
}

#[test]
fn transaction_encodings_reject_commit_times_at_or_before_start() {
    let command = ShardCommand::CommitSingleShardTransaction(
        CommitSingleShardTransaction::new(
            CommandId::new(700).unwrap(),
            7,
            10,
            TransactionId::new(701).unwrap(),
            TransactionTime::new(10).unwrap(),
            0,
            Digest32::new([7; 32]),
            vec![vertex_mutation_at(1, 1, 0, 100, 20)],
        )
        .unwrap(),
    );
    let mut encoded = command.encode_current().unwrap();
    let start_time_offset = 4 + 1 + 16 + 8 + 8 + 16;
    encoded[start_time_offset..start_time_offset + 8].copy_from_slice(&20_i64.to_be_bytes());
    assert_eq!(
        ShardCommand::decode(&encoded).unwrap_err().code(),
        "DTG-SHARD-COMMAND"
    );

    let mut receipt = Vec::with_capacity(92);
    receipt.extend_from_slice(&1_u32.to_be_bytes());
    receipt.extend_from_slice(&700_u128.to_be_bytes());
    receipt.extend_from_slice(&701_u128.to_be_bytes());
    receipt.extend_from_slice(&20_i64.to_be_bytes());
    receipt.extend_from_slice(&0_u64.to_be_bytes());
    receipt.extend_from_slice(&20_i64.to_be_bytes());
    receipt.extend_from_slice(&[7; 32]);
    let metadata = ReplicaMetadata::new(
        format!("{SINGLE_SHARD_TRANSACTION_METADATA_NAME}{:032x}", 701_u128),
        Value::Bytes(receipt),
    )
    .unwrap();
    assert_eq!(
        decode_single_shard_transaction_metadata(&metadata)
            .unwrap_err()
            .code(),
        "DTG-SHARD-INTENT-HISTORY"
    );
}

#[test]
fn home_decision_manifest_binds_the_terminal_digest() {
    let manifest = home_manifest(TransactionTime::new(40).unwrap());
    let decision = TransactionRecord::new(
        TransactionId::new(7).unwrap(),
        TransactionState::Committed,
        TransactionTime::new(50).unwrap(),
        Digest32::new([7; 32]),
    )
    .unwrap();
    let error =
        RecordHomeDecision::new(CommandId::new(8).unwrap(), 7, 10, decision, manifest).unwrap_err();

    assert_eq!(error.code(), "DTG-SHARD-COMMAND");
}

#[test]
fn home_decision_is_write_once_and_opposite_replay_advances_as_rejected_noop() {
    let root = tempfile::tempdir().unwrap();
    let binding = fjall_fixture_binding();
    let path = root.path().join("home-decision-cas");
    let transaction_id = TransactionId::new(700).unwrap();
    let manifest = home_manifest(TransactionTime::new(40).unwrap());
    let committed = TransactionRecord::new(
        transaction_id,
        TransactionState::Committed,
        TransactionTime::new(50).unwrap(),
        manifest
            .decision_digest(
                transaction_id,
                TransactionState::Committed,
                TransactionTime::new(50).unwrap(),
            )
            .unwrap(),
    )
    .unwrap();
    let aborted = TransactionRecord::new(
        transaction_id,
        TransactionState::Aborted,
        TransactionTime::new(40).unwrap(),
        manifest
            .decision_digest(
                transaction_id,
                TransactionState::Aborted,
                TransactionTime::new(40).unwrap(),
            )
            .unwrap(),
    )
    .unwrap();
    let commit_command = ShardCommand::RecordHomeDecision(
        RecordHomeDecision::new(
            CommandId::new(701).unwrap(),
            7,
            10,
            committed,
            manifest.clone(),
        )
        .unwrap(),
    );
    let abort_command = ShardCommand::RecordHomeDecision(
        RecordHomeDecision::new(CommandId::new(701).unwrap(), 7, 10, aborted, manifest).unwrap(),
    );

    let store = Arc::new(FjallReplicaStore::open(&path, binding.clone()).unwrap());
    let mut machine = ShardStateMachine::new(binding.clone(), store).unwrap();
    assert_eq!(
        machine
            .apply_committed(11, 1, commit_command.clone())
            .unwrap()
            .rejection(),
        None
    );
    assert!(
        machine
            .apply_committed(11, 2, commit_command)
            .unwrap()
            .replayed()
    );
    assert_eq!(
        machine
            .apply_committed(11, 3, abort_command.clone())
            .unwrap()
            .rejection(),
        Some(ApplyRejection::HomeDecisionConflict)
    );
    drop(machine);

    let reopened = Arc::new(FjallReplicaStore::open(&path, binding.clone()).unwrap());
    let mut machine = ShardStateMachine::new(binding, reopened).unwrap();
    assert_eq!(
        machine
            .apply_committed(11, 4, abort_command)
            .unwrap()
            .rejection(),
        Some(ApplyRejection::HomeDecisionConflict)
    );
    assert_eq!(machine.applied_index(), 4);
}

#[test]
fn generic_commit_rejects_transaction_and_reserved_metadata_mutations() {
    let transaction = TransactionRecord::new(
        TransactionId::new(9).unwrap(),
        TransactionState::Prepared,
        TransactionTime::new(10).unwrap(),
        Digest32::new([7; 32]),
    )
    .unwrap();
    let transaction_error = CommitSingleShard::new(
        CommandId::new(9).unwrap(),
        7,
        10,
        vec![LogicalMutation::PutTransaction(transaction)],
    )
    .unwrap_err();
    assert_eq!(transaction_error.code(), "DTG-SHARD-COMMAND");

    for reserved in [
        "dtg.closed_timestamp",
        "dtg.installed_snapshot",
        "dtg.migration_phase",
        "dtg.raft_noop",
    ] {
        let metadata = ReplicaMetadata::new(reserved, Value::Integer(1)).unwrap();
        let error = CommitSingleShard::new(
            CommandId::new(10).unwrap(),
            7,
            10,
            vec![LogicalMutation::PutReplicaMetadata(metadata)],
        )
        .unwrap_err();
        assert_eq!(error.code(), "DTG-SHARD-COMMAND");
    }
}

#[test]
fn command_value_nesting_and_allocation_budgets_fail_closed() {
    let mut nested = Value::Integer(1);
    for _ in 0..65 {
        nested = Value::List(vec![nested]);
    }
    let mut properties = Properties::new();
    properties.insert("nested".into(), nested);
    let vertex = VertexVersion::new(
        VertexId::new(100).unwrap(),
        Version::new(1),
        ValidInterval::new(0, 100).unwrap(),
        TransactionTime::new(10).unwrap(),
        properties,
    )
    .unwrap();
    let nested_command = ShardCommand::CommitSingleShard(
        CommitSingleShard::new(
            CommandId::new(100).unwrap(),
            7,
            10,
            vec![LogicalMutation::PutVertex(vertex)],
        )
        .unwrap(),
    );
    assert!(nested_command.encode_current().is_err());

    let mut truncated = Vec::new();
    truncated.extend_from_slice(&SUPPORTED_SHARD_COMMAND_FORMAT_VERSION.to_be_bytes());
    truncated.push(1);
    truncated.extend_from_slice(&101_u128.to_be_bytes());
    truncated.extend_from_slice(&7_u64.to_be_bytes());
    truncated.extend_from_slice(&10_u64.to_be_bytes());
    truncated.extend_from_slice(&65_537_u32.to_be_bytes());
    let error = ShardCommand::decode(&truncated).unwrap_err();
    assert!(error.to_string().contains("allocation budget"));
}

#[test]
fn prewrite_hides_graph_mutations_until_commit_finalization() {
    let binding = fixture_binding();
    let store = Arc::new(RecordingStore::new(binding.clone()));
    let mut machine = ShardStateMachine::new(binding, store.clone()).unwrap();
    let transaction_id = TransactionId::new(91).unwrap();
    let intent = ParticipantIntent::new(
        transaction_id,
        dtg_storage::ShardId::new(3).unwrap(),
        TransactionTime::new(5).unwrap(),
        0,
        vec![committed_vertex_mutation(900)],
    )
    .unwrap();
    let prepared = TransactionRecord::new(
        transaction_id,
        TransactionState::Prepared,
        TransactionTime::new(5).unwrap(),
        intent.digest(),
    )
    .unwrap();
    let command = ShardCommand::PrewriteIntent(
        PrewriteIntent::new(
            CommandId::new(901).unwrap(),
            7,
            10,
            prepared,
            intent.clone(),
        )
        .unwrap(),
    );

    machine.apply_committed(11, 1, command).unwrap();

    let state = store.state.lock().unwrap();
    let mutations = state.batch.as_ref().unwrap().mutations();
    assert_eq!(mutations.len(), 4);
    assert!(mutations.iter().any(|mutation| matches!(
        mutation,
        LogicalMutation::PutTransaction(record) if record.state() == TransactionState::Prepared
    )));
    assert!(mutations.iter().any(|mutation| matches!(
        mutation,
        LogicalMutation::PutReplicaMetadata(metadata)
            if metadata.name() == dtg_shard::TRANSACTION_INTENT_METADATA_NAME
                && matches!(metadata.value(), Value::Bytes(bytes) if ParticipantIntent::decode_current(bytes).is_ok())
    )));
    assert!(mutations.iter().any(|mutation| matches!(
        mutation,
        LogicalMutation::PutReplicaMetadata(metadata)
            if metadata.name() == dtg_shard::ACTIVE_TRANSACTION_INTENTS_METADATA_NAME
    )));
    assert!(mutations.iter().any(|mutation| matches!(
        mutation,
        LogicalMutation::PutReplicaMetadata(metadata)
            if metadata.name().starts_with(dtg_shard::TRANSACTION_STATE_METADATA_PREFIX)
    )));
    assert!(!mutations.iter().any(|mutation| matches!(
        mutation,
        LogicalMutation::PutVertex(_)
            | LogicalMutation::DeleteVertex(_)
            | LogicalMutation::PutEdge(_)
            | LogicalMutation::DeleteEdge(_)
    )));

    let committed = TransactionRecord::new(
        transaction_id,
        TransactionState::Committed,
        TransactionTime::new(10).unwrap(),
        intent.digest(),
    )
    .unwrap();
    let binding = fixture_binding();
    let commit_store = Arc::new(RecordingStore::new(binding.clone()));
    let mut commit_machine = ShardStateMachine::new(binding, commit_store).unwrap();
    assert_eq!(
        commit_machine
            .apply_committed(
                11,
                2,
                ShardCommand::FinalizeParticipant(
                    FinalizeParticipant::new(
                        CommandId::new(902).unwrap(),
                        7,
                        10,
                        committed,
                        Some(intent.clone()),
                    )
                    .unwrap(),
                ),
            )
            .unwrap_err()
            .code(),
        "DTG-SHARD-INTENT-HISTORY"
    );

    let binding = fixture_binding();
    let abort_store = Arc::new(RecordingStore::new(binding.clone()));
    let mut abort_machine = ShardStateMachine::new(binding, abort_store.clone()).unwrap();
    let aborted = TransactionRecord::new(
        transaction_id,
        TransactionState::Aborted,
        TransactionTime::new(5).unwrap(),
        intent.digest(),
    )
    .unwrap();
    assert_eq!(
        abort_machine
            .apply_committed(
                11,
                2,
                ShardCommand::FinalizeParticipant(
                    FinalizeParticipant::new(CommandId::new(903).unwrap(), 7, 10, aborted, None)
                        .unwrap(),
                ),
            )
            .unwrap_err()
            .code(),
        "DTG-SHARD-INTENT-HISTORY"
    );
}

#[test]
fn exact_finalize_replay_after_prewrite_is_metadata_only_and_survives_reopen() {
    let root = tempfile::tempdir().unwrap();
    let binding = fjall_fixture_binding();
    let path = root.path().join("finalize-replay");
    let intent = ParticipantIntent::new(
        TransactionId::new(601).unwrap(),
        binding.shard_id(),
        TransactionTime::new(40).unwrap(),
        0,
        vec![vertex_mutation_at(60, 1, 0, 100, 50)],
    )
    .unwrap();
    let prepared = TransactionRecord::new(
        intent.transaction_id(),
        TransactionState::Prepared,
        intent.start_time(),
        intent.digest(),
    )
    .unwrap();
    let terminal = TransactionRecord::new(
        intent.transaction_id(),
        TransactionState::Committed,
        TransactionTime::new(50).unwrap(),
        intent.digest(),
    )
    .unwrap();
    let finalize = ShardCommand::FinalizeParticipant(
        FinalizeParticipant::new(
            CommandId::new(6_002).unwrap(),
            7,
            10,
            terminal,
            Some(intent.clone()),
        )
        .unwrap(),
    );

    {
        let store = Arc::new(FjallReplicaStore::open(&path, binding.clone()).unwrap());
        let mut machine = ShardStateMachine::new(binding.clone(), store).unwrap();
        let prewrite = ShardCommand::PrewriteIntent(
            PrewriteIntent::new(CommandId::new(6_001).unwrap(), 7, 10, prepared, intent).unwrap(),
        );
        machine.apply_committed(11, 1, prewrite.clone()).unwrap();
        assert!(machine.apply_committed(11, 1, prewrite).unwrap().replayed());
        assert!(
            !machine
                .apply_committed(11, 2, finalize.clone())
                .unwrap()
                .replayed()
        );
        assert!(
            machine
                .apply_committed(11, 2, finalize.clone())
                .unwrap()
                .replayed()
        );
    }

    let store = Arc::new(FjallReplicaStore::open(&path, binding.clone()).unwrap());
    let mut reopened = ShardStateMachine::new(binding.clone(), store.clone()).unwrap();
    let replay = reopened.apply_committed(11, 3, finalize).unwrap();
    assert!(replay.replayed());
    assert_eq!(replay.applied_index(), 3);
    let view = block_on(store.begin_read_view(ReadFence::new(binding, 3))).unwrap();
    let changes = block_on(view.changes(ChangesRead::new(None, 3, 64).unwrap())).unwrap();
    assert_eq!(
        changes
            .rows()
            .iter()
            .filter(|change| matches!(change.mutation(), LogicalMutation::PutVertex(_)))
            .count(),
        1
    );
    assert_eq!(
        changes
            .rows()
            .iter()
            .filter(|change| matches!(
                change.mutation(),
                LogicalMutation::PutTransaction(record)
                    if record.state() == TransactionState::Committed
            ))
            .count(),
        1
    );
}

#[test]
fn exact_abort_finalize_replay_after_prewrite_is_metadata_only() {
    let root = tempfile::tempdir().unwrap();
    let binding = fjall_fixture_binding();
    let path = root.path().join("abort-replay");
    let intent = ParticipantIntent::new(
        TransactionId::new(602).unwrap(),
        binding.shard_id(),
        TransactionTime::new(40).unwrap(),
        0,
        vec![vertex_mutation_at(61, 1, 0, 100, 50)],
    )
    .unwrap();
    let prepared = TransactionRecord::new(
        intent.transaction_id(),
        TransactionState::Prepared,
        intent.start_time(),
        intent.digest(),
    )
    .unwrap();
    let terminal = TransactionRecord::new(
        intent.transaction_id(),
        TransactionState::Aborted,
        intent.start_time(),
        intent.digest(),
    )
    .unwrap();
    let finalize = ShardCommand::FinalizeParticipant(
        FinalizeParticipant::new(CommandId::new(6_102).unwrap(), 7, 10, terminal, None).unwrap(),
    );
    let store = Arc::new(FjallReplicaStore::open(&path, binding.clone()).unwrap());
    let mut machine = ShardStateMachine::new(binding.clone(), store.clone()).unwrap();
    machine
        .apply_committed(
            11,
            1,
            ShardCommand::PrewriteIntent(
                PrewriteIntent::new(CommandId::new(6_101).unwrap(), 7, 10, prepared, intent)
                    .unwrap(),
            ),
        )
        .unwrap();
    machine.apply_committed(11, 2, finalize.clone()).unwrap();
    drop(machine);

    let mut reopened = ShardStateMachine::new(binding.clone(), store.clone()).unwrap();
    let replay = reopened.apply_committed(11, 3, finalize).unwrap();
    assert!(replay.replayed());
    let view = block_on(store.begin_read_view(ReadFence::new(binding, 3))).unwrap();
    let changes = block_on(view.changes(ChangesRead::new(None, 3, 64).unwrap())).unwrap();
    assert_eq!(
        changes
            .rows()
            .iter()
            .filter(|change| matches!(
                change.mutation(),
                LogicalMutation::PutTransaction(record)
                    if record.state() == TransactionState::Aborted
            ))
            .count(),
        1
    );
    assert!(!changes.rows().iter().any(|change| matches!(
        change.mutation(),
        LogicalMutation::PutVertex(_)
            | LogicalMutation::DeleteVertex(_)
            | LogicalMutation::PutEdge(_)
            | LogicalMutation::DeleteEdge(_)
    )));
}

#[test]
fn old_index_prewrite_and_finalize_replays_reconstruct_original_batches() {
    let root = tempfile::tempdir().unwrap();
    let binding = fjall_fixture_binding();
    let path = root.path().join("old-index-transaction-replay");
    let intent = ParticipantIntent::new(
        TransactionId::new(603).unwrap(),
        binding.shard_id(),
        TransactionTime::new(40).unwrap(),
        0,
        vec![vertex_mutation_at(62, 1, 0, 100, 50)],
    )
    .unwrap();
    let prepared = TransactionRecord::new(
        intent.transaction_id(),
        TransactionState::Prepared,
        intent.start_time(),
        intent.digest(),
    )
    .unwrap();
    let committed = TransactionRecord::new(
        intent.transaction_id(),
        TransactionState::Committed,
        TransactionTime::new(50).unwrap(),
        intent.digest(),
    )
    .unwrap();
    let prewrite = ShardCommand::PrewriteIntent(
        PrewriteIntent::new(
            CommandId::new(6_201).unwrap(),
            7,
            10,
            prepared,
            intent.clone(),
        )
        .unwrap(),
    );
    let finalize = ShardCommand::FinalizeParticipant(
        FinalizeParticipant::new(
            CommandId::new(6_202).unwrap(),
            7,
            10,
            committed,
            Some(intent),
        )
        .unwrap(),
    );
    let store = Arc::new(FjallReplicaStore::open(&path, binding.clone()).unwrap());
    let mut machine = ShardStateMachine::new(binding.clone(), store).unwrap();

    machine.apply_committed(11, 1, prewrite.clone()).unwrap();
    machine.apply_committed(11, 2, finalize.clone()).unwrap();

    assert!(machine.apply_committed(11, 1, prewrite).unwrap().replayed());
    assert!(machine.apply_committed(11, 2, finalize).unwrap().replayed());
    drop(machine);

    let reopened = Arc::new(FjallReplicaStore::open(&path, binding.clone()).unwrap());
    ShardStateMachine::new(binding, reopened).unwrap();
}

#[test]
fn participant_intent_is_current_only_bounded_and_digest_bound() {
    let transaction_id = TransactionId::new(91).unwrap();
    let original = ParticipantIntent::new(
        transaction_id,
        dtg_storage::ShardId::new(3).unwrap(),
        TransactionTime::new(5).unwrap(),
        0,
        vec![committed_vertex_mutation(900)],
    )
    .unwrap();
    let encoded = original.encode_current().unwrap();
    assert_eq!(
        ParticipantIntent::decode_current(&encoded).unwrap(),
        original
    );

    let mut unknown = encoded.clone();
    unknown[..4].copy_from_slice(&4_u32.to_be_bytes());
    assert!(ParticipantIntent::decode_current(&unknown).is_err());

    let mut trailing = encoded;
    trailing.push(0);
    assert!(ParticipantIntent::decode_current(&trailing).is_err());
    assert!(ParticipantIntent::decode_current(&vec![0; 4 * 1024 * 1024 + 1]).is_err());

    let changed = ParticipantIntent::new(
        transaction_id,
        dtg_storage::ShardId::new(3).unwrap(),
        TransactionTime::new(5).unwrap(),
        0,
        vec![committed_vertex_mutation(901)],
    )
    .unwrap();
    let prepared = TransactionRecord::new(
        transaction_id,
        TransactionState::Prepared,
        TransactionTime::new(5).unwrap(),
        original.digest(),
    )
    .unwrap();
    assert!(PrewriteIntent::new(CommandId::new(904).unwrap(), 7, 10, prepared, changed).is_err());

    let prepared = TransactionRecord::new(
        transaction_id,
        TransactionState::Prepared,
        TransactionTime::new(5).unwrap(),
        original.digest(),
    )
    .unwrap();
    let command = ShardCommand::PrewriteIntent(
        PrewriteIntent::new(CommandId::new(905).unwrap(), 7, 10, prepared, original).unwrap(),
    );
    let bytes = command.encode_current().unwrap();
    assert_eq!(ShardCommand::decode(&bytes).unwrap(), command);
}

#[test]
fn commit_finalization_rejects_intent_timestamp_mismatch() {
    let transaction_id = TransactionId::new(91).unwrap();
    let intent = ParticipantIntent::new(
        transaction_id,
        dtg_storage::ShardId::new(3).unwrap(),
        TransactionTime::new(5).unwrap(),
        0,
        vec![committed_vertex_mutation(900)],
    )
    .unwrap();
    let terminal = TransactionRecord::new(
        transaction_id,
        TransactionState::Committed,
        TransactionTime::new(50).unwrap(),
        intent.digest(),
    )
    .unwrap();

    assert!(
        FinalizeParticipant::new(CommandId::new(906).unwrap(), 7, 10, terminal, Some(intent))
            .is_err()
    );

    let mixed = ParticipantIntent::new(
        TransactionId::new(92).unwrap(),
        dtg_storage::ShardId::new(3).unwrap(),
        TransactionTime::new(5).unwrap(),
        0,
        vec![
            committed_vertex_mutation(901),
            LogicalMutation::PutVertex(
                VertexVersion::new(
                    VertexId::new(902).unwrap(),
                    Version::new(1),
                    ValidInterval::new(0, 100).unwrap(),
                    TransactionTime::new(11).unwrap(),
                    Properties::new(),
                )
                .unwrap(),
            ),
        ],
    );
    assert!(mixed.is_err());
}

#[test]
fn participant_intent_binds_the_transaction_start_timestamp() {
    let transaction_id = TransactionId::new(93).unwrap();
    let shard_id = dtg_storage::ShardId::new(3).unwrap();
    let at_five = ParticipantIntent::new(
        transaction_id,
        shard_id,
        TransactionTime::new(5).unwrap(),
        0,
        vec![committed_vertex_mutation(910)],
    )
    .unwrap();
    let at_six = ParticipantIntent::new(
        transaction_id,
        shard_id,
        TransactionTime::new(6).unwrap(),
        0,
        vec![committed_vertex_mutation(910)],
    )
    .unwrap();
    assert_ne!(at_five.digest(), at_six.digest());

    let wrong_prepared_time = TransactionRecord::new(
        transaction_id,
        TransactionState::Prepared,
        TransactionTime::new(6).unwrap(),
        at_five.digest(),
    )
    .unwrap();
    assert!(
        PrewriteIntent::new(
            CommandId::new(911).unwrap(),
            7,
            10,
            wrong_prepared_time,
            at_five,
        )
        .is_err()
    );
}

#[test]
fn participant_intent_decode_rejects_item_count_before_payload_decode() {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&3_u32.to_be_bytes());
    bytes.extend_from_slice(&93_u128.to_be_bytes());
    bytes.extend_from_slice(&3_u64.to_be_bytes());
    bytes.extend_from_slice(&5_i64.to_be_bytes());
    bytes.extend_from_slice(&0_u64.to_be_bytes());
    bytes.extend_from_slice(&4_097_u32.to_be_bytes());

    let error = ParticipantIntent::decode_current(&bytes).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("transaction intent mutation count")
    );
}

#[test]
fn serialized_acceptance_rejects_committed_and_prepared_interval_conflicts_after_reopen() {
    let root = tempfile::tempdir().unwrap();
    let binding = fjall_fixture_binding();
    let committed_store =
        Arc::new(FjallReplicaStore::open(root.path().join("committed"), binding.clone()).unwrap());
    let mut committed_machine = ShardStateMachine::new(binding.clone(), committed_store).unwrap();
    let first = ShardCommand::CommitSingleShardTransaction(
        CommitSingleShardTransaction::new(
            CommandId::new(1_001).unwrap(),
            7,
            10,
            TransactionId::new(101).unwrap(),
            TransactionTime::new(40).unwrap(),
            0,
            Digest32::new([1; 32]),
            vec![vertex_mutation_at(1, 1, 0, 100, 50)],
        )
        .unwrap(),
    );
    committed_machine.apply_committed(11, 1, first).unwrap();
    let overlapping = ShardCommand::CommitSingleShardTransaction(
        CommitSingleShardTransaction::new(
            CommandId::new(1_002).unwrap(),
            7,
            10,
            TransactionId::new(102).unwrap(),
            TransactionTime::new(40).unwrap(),
            0,
            Digest32::new([2; 32]),
            vec![vertex_mutation_at(1, 2, 50, 150, 60)],
        )
        .unwrap(),
    );
    let committed_conflict = committed_machine
        .apply_committed(11, 2, overlapping)
        .unwrap();
    assert_eq!(
        committed_conflict.rejection(),
        Some(ApplyRejection::WriteConflict)
    );
    assert_eq!(committed_machine.applied_index(), 2);

    let prepared_path = root.path().join("prepared");
    let prepared_store =
        Arc::new(FjallReplicaStore::open(&prepared_path, binding.clone()).unwrap());
    let mut prepared_machine = ShardStateMachine::new(binding.clone(), prepared_store).unwrap();
    let winning_intent = ParticipantIntent::new(
        TransactionId::new(201).unwrap(),
        binding.shard_id(),
        TransactionTime::new(40).unwrap(),
        0,
        vec![vertex_mutation_at(2, 1, 0, 100, 50)],
    )
    .unwrap();
    let winning_prepared = TransactionRecord::new(
        winning_intent.transaction_id(),
        TransactionState::Prepared,
        winning_intent.start_time(),
        winning_intent.digest(),
    )
    .unwrap();
    prepared_machine
        .apply_committed(
            11,
            1,
            ShardCommand::PrewriteIntent(
                PrewriteIntent::new(
                    CommandId::new(2_001).unwrap(),
                    7,
                    10,
                    winning_prepared.clone(),
                    winning_intent.clone(),
                )
                .unwrap(),
            ),
        )
        .unwrap();
    drop(prepared_machine);

    let reopened_store =
        Arc::new(FjallReplicaStore::open(&prepared_path, binding.clone()).unwrap());
    let mut reopened = ShardStateMachine::new(binding.clone(), reopened_store).unwrap();
    let competing_intent = ParticipantIntent::new(
        TransactionId::new(202).unwrap(),
        binding.shard_id(),
        TransactionTime::new(40).unwrap(),
        0,
        vec![vertex_mutation_at(2, 2, 50, 150, 60)],
    )
    .unwrap();
    let competing_prepared = TransactionRecord::new(
        competing_intent.transaction_id(),
        TransactionState::Prepared,
        competing_intent.start_time(),
        competing_intent.digest(),
    )
    .unwrap();
    let competing_command = ShardCommand::PrewriteIntent(
        PrewriteIntent::new(
            CommandId::new(2_002).unwrap(),
            7,
            10,
            competing_prepared.clone(),
            competing_intent.clone(),
        )
        .unwrap(),
    );
    let prepared_conflict = reopened
        .apply_committed(11, 2, competing_command.clone())
        .unwrap();
    assert_eq!(
        prepared_conflict.rejection(),
        Some(ApplyRejection::WriteConflict)
    );
    assert_eq!(reopened.applied_index(), 2);

    let committed = TransactionRecord::new(
        winning_intent.transaction_id(),
        TransactionState::Committed,
        TransactionTime::new(50).unwrap(),
        winning_intent.digest(),
    )
    .unwrap();
    reopened
        .apply_committed(
            11,
            3,
            ShardCommand::FinalizeParticipant(
                FinalizeParticipant::new(
                    CommandId::new(2_003).unwrap(),
                    7,
                    10,
                    committed,
                    Some(winning_intent),
                )
                .unwrap(),
            ),
        )
        .unwrap();
    let post_commit_intent = ParticipantIntent::new(
        TransactionId::new(202).unwrap(),
        binding.shard_id(),
        TransactionTime::new(60).unwrap(),
        3,
        vec![vertex_mutation_at(2, 2, 50, 150, 70)],
    )
    .unwrap();
    let post_commit_prepared = TransactionRecord::new(
        post_commit_intent.transaction_id(),
        TransactionState::Prepared,
        post_commit_intent.start_time(),
        post_commit_intent.digest(),
    )
    .unwrap();
    reopened
        .apply_committed(
            11,
            4,
            ShardCommand::PrewriteIntent(
                PrewriteIntent::new(
                    CommandId::new(2_006).unwrap(),
                    7,
                    10,
                    post_commit_prepared,
                    post_commit_intent.clone(),
                )
                .unwrap(),
            ),
        )
        .unwrap();

    let aborted = TransactionRecord::new(
        post_commit_intent.transaction_id(),
        TransactionState::Aborted,
        post_commit_intent.start_time(),
        post_commit_intent.digest(),
    )
    .unwrap();
    reopened
        .apply_committed(
            11,
            5,
            ShardCommand::FinalizeParticipant(
                FinalizeParticipant::new(CommandId::new(2_004).unwrap(), 7, 10, aborted, None)
                    .unwrap(),
            ),
        )
        .unwrap();
    let third_intent = ParticipantIntent::new(
        TransactionId::new(203).unwrap(),
        binding.shard_id(),
        TransactionTime::new(60).unwrap(),
        5,
        vec![vertex_mutation_at(2, 3, 25, 75, 70)],
    )
    .unwrap();
    let third_prepared = TransactionRecord::new(
        third_intent.transaction_id(),
        TransactionState::Prepared,
        third_intent.start_time(),
        third_intent.digest(),
    )
    .unwrap();
    reopened
        .apply_committed(
            11,
            6,
            ShardCommand::PrewriteIntent(
                PrewriteIntent::new(
                    CommandId::new(2_005).unwrap(),
                    7,
                    10,
                    third_prepared,
                    third_intent,
                )
                .unwrap(),
            ),
        )
        .unwrap();
}

#[test]
fn committed_conflict_scan_starts_strictly_after_the_snapshot_fence() {
    let root = tempfile::tempdir().unwrap();
    let binding = fjall_fixture_binding();
    let store = Arc::new(FjallReplicaStore::open(root.path(), binding.clone()).unwrap());
    let mut machine = ShardStateMachine::new(binding, store).unwrap();
    machine
        .apply_committed(
            11,
            1,
            ShardCommand::CommitSingleShardTransaction(
                CommitSingleShardTransaction::new(
                    CommandId::new(7_001).unwrap(),
                    7,
                    10,
                    TransactionId::new(701).unwrap(),
                    TransactionTime::new(40).unwrap(),
                    0,
                    Digest32::new([1; 32]),
                    vec![vertex_mutation_at(70, 1, 0, 100, 50)],
                )
                .unwrap(),
            ),
        )
        .unwrap();

    machine
        .apply_committed(
            11,
            2,
            ShardCommand::CommitSingleShardTransaction(
                CommitSingleShardTransaction::new(
                    CommandId::new(7_002).unwrap(),
                    7,
                    10,
                    TransactionId::new(702).unwrap(),
                    TransactionTime::new(40).unwrap(),
                    1,
                    Digest32::new([2; 32]),
                    vec![vertex_mutation_at(70, 2, 50, 150, 60)],
                )
                .unwrap(),
            ),
        )
        .unwrap();
}

#[test]
fn committed_conflict_scan_fails_closed_when_history_budget_is_exceeded() {
    let root = tempfile::tempdir().unwrap();
    let binding = fjall_fixture_binding();
    let store = Arc::new(FjallReplicaStore::open(root.path(), binding.clone()).unwrap());
    let mutations = (0..=65_536_u64)
        .map(|value| {
            LogicalMutation::PutReplicaMetadata(
                ReplicaMetadata::new(format!("test.history.{value}"), Value::Integer(1)).unwrap(),
            )
        })
        .collect();
    block_on(
        store.apply(
            CommittedShardBatch::new(
                binding.clone(),
                11,
                1,
                CommandId::new(8_001).unwrap(),
                mutations,
            )
            .unwrap(),
        ),
    )
    .unwrap();
    let mut machine = ShardStateMachine::new(binding, store).unwrap();

    assert_eq!(
        machine
            .apply_committed(
                11,
                2,
                ShardCommand::CommitSingleShardTransaction(
                    CommitSingleShardTransaction::new(
                        CommandId::new(8_002).unwrap(),
                        7,
                        10,
                        TransactionId::new(801).unwrap(),
                        TransactionTime::new(40).unwrap(),
                        0,
                        Digest32::new([8; 32]),
                        vec![vertex_mutation_at(80, 1, 0, 100, 50)],
                    )
                    .unwrap(),
                ),
            )
            .unwrap_err()
            .code(),
        "DTG-SHARD-INTENT-HISTORY"
    );
}

#[test]
fn reopen_fails_closed_on_contradictory_current_intent_state() {
    let root = tempfile::tempdir().unwrap();
    let binding = fjall_fixture_binding();
    let contradictory_store = Arc::new(
        FjallReplicaStore::open(root.path().join("contradictory"), binding.clone()).unwrap(),
    );
    let intent = ParticipantIntent::new(
        TransactionId::new(301).unwrap(),
        binding.shard_id(),
        TransactionTime::new(40).unwrap(),
        0,
        vec![vertex_mutation_at(3, 1, 0, 100, 50)],
    )
    .unwrap();
    let prepared = TransactionRecord::new(
        intent.transaction_id(),
        TransactionState::Prepared,
        intent.start_time(),
        intent.digest(),
    )
    .unwrap();
    let mut machine = ShardStateMachine::new(binding.clone(), contradictory_store.clone()).unwrap();
    machine
        .apply_committed(
            11,
            1,
            ShardCommand::PrewriteIntent(
                PrewriteIntent::new(
                    CommandId::new(3_001).unwrap(),
                    7,
                    10,
                    prepared,
                    intent.clone(),
                )
                .unwrap(),
            ),
        )
        .unwrap();
    let stale_active = block_on(
        contradictory_store.replica_metadata(dtg_shard::ACTIVE_TRANSACTION_INTENTS_METADATA_NAME),
    )
    .unwrap()
    .unwrap();
    let committed = TransactionRecord::new(
        intent.transaction_id(),
        TransactionState::Committed,
        TransactionTime::new(50).unwrap(),
        intent.digest(),
    )
    .unwrap();
    machine
        .apply_committed(
            11,
            2,
            ShardCommand::FinalizeParticipant(
                FinalizeParticipant::new(
                    CommandId::new(3_002).unwrap(),
                    7,
                    10,
                    committed,
                    Some(intent),
                )
                .unwrap(),
            ),
        )
        .unwrap();
    block_on(
        contradictory_store.apply(
            CommittedShardBatch::new(
                binding.clone(),
                11,
                3,
                CommandId::new(3_003).unwrap(),
                vec![LogicalMutation::PutReplicaMetadata(stale_active)],
            )
            .unwrap(),
        ),
    )
    .unwrap();
    assert!(matches!(
        ShardStateMachine::new(binding.clone(), contradictory_store),
        Err(error) if error.code() == "DTG-SHARD-INTENT-HISTORY"
    ));
}

#[test]
fn live_prewrite_enforces_the_reopen_active_intent_bound() {
    let binding = fixture_binding();
    let mut intents = BTreeMap::new();
    let mut metadata = BTreeMap::new();
    for value in 1..=4_096_u128 {
        let intent = ParticipantIntent::new(
            TransactionId::new(10_000 + value).unwrap(),
            binding.shard_id(),
            TransactionTime::new(40).unwrap(),
            0,
            vec![vertex_mutation_at(10_000 + value, 1, 0, 100, 50)],
        )
        .unwrap();
        let state = transaction_state_metadata_for_test(&intent);
        metadata.insert(state.name().to_owned(), state);
        intents.insert(intent.transaction_id(), intent);
    }
    let aggregate = active_intents_metadata_for_test(&intents);
    metadata.insert(aggregate.name().to_owned(), aggregate);
    let store = Arc::new(RecordingStore::with_metadata(binding.clone(), metadata));
    let mut machine = ShardStateMachine::new(binding.clone(), store).unwrap();
    let competing = ParticipantIntent::new(
        TransactionId::new(20_000).unwrap(),
        binding.shard_id(),
        TransactionTime::new(40).unwrap(),
        0,
        vec![vertex_mutation_at(20_000, 1, 0, 100, 50)],
    )
    .unwrap();
    let prepared = TransactionRecord::new(
        competing.transaction_id(),
        TransactionState::Prepared,
        competing.start_time(),
        competing.digest(),
    )
    .unwrap();
    let outcome = machine
        .apply_committed(
            11,
            1,
            ShardCommand::PrewriteIntent(
                PrewriteIntent::new(CommandId::new(4_097).unwrap(), 7, 10, prepared, competing)
                    .unwrap(),
            ),
        )
        .unwrap();
    assert_eq!(outcome.rejection(), Some(ApplyRejection::InvalidCommand));
    assert_eq!(outcome.applied_index(), 1);
    assert_eq!(machine.applied_index(), 1);
}

fn transaction_state_metadata_for_test(intent: &ParticipantIntent) -> ReplicaMetadata {
    let encoded = intent.encode_current().unwrap();
    let mut bytes = Vec::with_capacity(9 + encoded.len());
    bytes.extend_from_slice(&1_u32.to_be_bytes());
    bytes.push(1);
    bytes.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
    bytes.extend_from_slice(&encoded);
    ReplicaMetadata::new(
        format!(
            "{}{:032x}",
            dtg_shard::TRANSACTION_STATE_METADATA_PREFIX,
            intent.transaction_id().get()
        ),
        Value::Bytes(bytes),
    )
    .unwrap()
}

fn active_intents_metadata_for_test(
    intents: &BTreeMap<TransactionId, ParticipantIntent>,
) -> ReplicaMetadata {
    let mut bytes = Vec::with_capacity(8 + intents.len() * 48);
    bytes.extend_from_slice(&1_u32.to_be_bytes());
    bytes.extend_from_slice(&(intents.len() as u32).to_be_bytes());
    for (transaction_id, intent) in intents {
        bytes.extend_from_slice(&transaction_id.get().to_be_bytes());
        bytes.extend_from_slice(&intent.digest().get());
    }
    ReplicaMetadata::new(
        dtg_shard::ACTIVE_TRANSACTION_INTENTS_METADATA_NAME,
        Value::Bytes(bytes),
    )
    .unwrap()
}

fn fixture_machine() -> ShardStateMachine {
    let binding = fixture_binding();
    ShardStateMachine::new(binding.clone(), Arc::new(RecordingStore::new(binding))).unwrap()
}

fn committed_vertex_command(command_id: u128) -> ShardCommand {
    command_for(command_id, 7, 10)
}

fn command_for_generation(generation: u64) -> ShardCommand {
    command_for(1, 7, generation)
}

fn command_for(command_id: u128, placement_epoch: u64, backend_generation: u64) -> ShardCommand {
    ShardCommand::CommitSingleShard(
        CommitSingleShard::new(
            CommandId::new(command_id).unwrap(),
            placement_epoch,
            backend_generation,
            vec![committed_vertex_mutation(command_id)],
        )
        .unwrap(),
    )
}

fn committed_vertex_mutation(id: u128) -> LogicalMutation {
    let vertex = VertexVersion::new(
        VertexId::new(id).unwrap(),
        Version::new(1),
        ValidInterval::new(0, 100).unwrap(),
        TransactionTime::new(10).unwrap(),
        Properties::new(),
    )
    .unwrap();
    LogicalMutation::PutVertex(vertex)
}

fn vertex_mutation_at(
    id: u128,
    version: u64,
    start: i64,
    end: i64,
    transaction_time: i64,
) -> LogicalMutation {
    LogicalMutation::PutVertex(
        VertexVersion::new(
            VertexId::new(id).unwrap(),
            Version::new(version),
            ValidInterval::new(start, end).unwrap(),
            TransactionTime::new(transaction_time).unwrap(),
            Properties::new(),
        )
        .unwrap(),
    )
}

fn fixture_binding() -> ReplicaBinding {
    ReplicaBinding::builder()
        .cluster_id(1)
        .graph_id(2)
        .shard_id(3)
        .placement_epoch(7)
        .replica_id(4)
        .backend_generation(10)
        .backend_class_digest(Digest32::new([1; 32]))
        .provider_kind(ProviderKind::Fjall)
        .contract_version(1)
        .layout_version(1)
        .capability_digest(Digest32::new([2; 32]))
        .namespace_id("cluster-1/graph-2/shard-3/replica-4/generation-10")
        .endpoint_profile_ref("local")
        .credential_ref("none")
        .role(BindingRole::Active)
        .build()
        .unwrap()
}

fn fjall_fixture_binding() -> ReplicaBinding {
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
        .shard_id(3)
        .placement_epoch(7)
        .replica_id(4)
        .backend_generation(10)
        .backend_class_digest(class.digest())
        .provider_kind(ProviderKind::Fjall)
        .contract_version(1)
        .layout_version(1)
        .capability_digest(capabilities.digest())
        .namespace_id("cluster-1/graph-2/shard-3/replica-4/generation-10-fjall")
        .endpoint_profile_ref("local")
        .credential_ref("none")
        .role(BindingRole::Active)
        .build()
        .unwrap()
}

#[derive(Default)]
struct RecordedApply {
    batch: Option<CommittedShardBatch>,
    metadata: BTreeMap<String, ReplicaMetadata>,
}

struct RecordingStore {
    binding: ReplicaBinding,
    state: Mutex<RecordedApply>,
}

impl RecordingStore {
    fn new(binding: ReplicaBinding) -> Self {
        Self {
            binding,
            state: Mutex::new(RecordedApply::default()),
        }
    }

    fn with_metadata(binding: ReplicaBinding, metadata: BTreeMap<String, ReplicaMetadata>) -> Self {
        Self {
            binding,
            state: Mutex::new(RecordedApply {
                batch: None,
                metadata,
            }),
        }
    }
}

impl ReplicaStateStore for RecordingStore {
    fn binding(&self) -> &ReplicaBinding {
        &self.binding
    }

    fn applied_index(&self) -> StoreFuture<'_, u64> {
        Box::pin(async move {
            Ok(self
                .state
                .lock()
                .unwrap()
                .batch
                .as_ref()
                .map_or(0, CommittedShardBatch::raft_index))
        })
    }

    fn replica_metadata<'a>(&'a self, name: &'a str) -> StoreFuture<'a, Option<ReplicaMetadata>> {
        Box::pin(async move { Ok(self.state.lock().unwrap().metadata.get(name).cloned()) })
    }

    fn apply(&self, batch: CommittedShardBatch) -> StoreFuture<'_, ApplyReceipt> {
        Box::pin(async move {
            let mut state = self.state.lock().unwrap();
            let replayed = if let Some(previous) = state.batch.as_ref() {
                if previous != &batch {
                    return Err(StorageError::ReplayMismatch {
                        raft_index: batch.raft_index(),
                    });
                }
                true
            } else {
                state.batch = Some(batch.clone());
                for mutation in batch.mutations() {
                    if let LogicalMutation::PutReplicaMetadata(metadata) = mutation {
                        state
                            .metadata
                            .insert(metadata.name().to_owned(), metadata.clone());
                    }
                }
                false
            };
            Ok(ApplyReceipt::new(&batch, replayed))
        })
    }

    fn begin_read_view(&self, _fence: ReadFence) -> StoreFuture<'_, Box<dyn TemporalReadView>> {
        Box::pin(async { Err(StorageError::Unsupported) })
    }
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

fn home_manifest(start_time: TransactionTime) -> HomeDecisionManifest {
    HomeDecisionManifest::new(
        start_time,
        Version::new(12),
        vec![
            HomeDecisionParticipant::new(
                dtg_storage::ShardId::new(3).unwrap(),
                PlacementEpoch::new(7).unwrap(),
                BackendGeneration::new(10).unwrap(),
                0,
                Digest32::new([3; 32]),
            )
            .unwrap(),
            HomeDecisionParticipant::new(
                dtg_storage::ShardId::new(9).unwrap(),
                PlacementEpoch::new(7).unwrap(),
                BackendGeneration::new(10).unwrap(),
                0,
                Digest32::new([9; 32]),
            )
            .unwrap(),
        ],
    )
    .unwrap()
}
