use std::sync::{Arc, Mutex};

use dtg_shard::{
    AdvanceClosedTimestamp, CommitSingleShard, FinalizeParticipant, ParticipantIntent,
    PrewriteIntent, RecordHomeDecision, ShardCommand, ShardStateMachine,
};
use dtg_storage::{
    ApplyReceipt, BindingRole, CommandId, CommittedShardBatch, Digest32, LogicalMutation,
    Properties, ProviderKind, ReadFence, ReplicaBinding, ReplicaMetadata, ReplicaStateStore,
    StorageError, StoreFuture, TemporalReadView, TransactionId, TransactionRecord,
    TransactionState, TransactionTime, ValidInterval, Value, Version, VertexId, VertexVersion,
};

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
fn stale_backend_generation_is_rejected_before_apply() {
    let mut machine = fixture_machine();
    let error = machine
        .apply_committed(11, 1, command_for_generation(9))
        .unwrap_err();
    assert_eq!(error.code(), "DTG-SHARD-STALE-GENERATION");
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
fn command_encoding_is_deterministic_and_round_trips() {
    let command = committed_vertex_command(7);
    let first = command.encode_current().unwrap();
    let second = command.clone().encode_current().unwrap();

    assert_eq!(first, second);
    assert_eq!(ShardCommand::decode(&first).unwrap(), command);
}

#[test]
fn home_decision_rejects_non_transaction_mutations() {
    let command = committed_vertex_command(7);
    let ShardCommand::CommitSingleShard(commit) = command else {
        unreachable!();
    };
    let error = RecordHomeDecision::new(
        CommandId::new(8).unwrap(),
        7,
        10,
        commit.mutations().to_vec(),
    )
    .unwrap_err();

    assert_eq!(error.code(), "DTG-SHARD-COMMAND");
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
    truncated.extend_from_slice(&1_u32.to_be_bytes());
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
    assert_eq!(mutations.len(), 2);
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
    assert!(!mutations.iter().any(|mutation| matches!(
        mutation,
        LogicalMutation::PutVertex(_)
            | LogicalMutation::DeleteVertex(_)
            | LogicalMutation::PutEdge(_)
            | LogicalMutation::DeleteEdge(_)
    )));

    let binding = fixture_binding();
    let commit_store = Arc::new(RecordingStore::new(binding.clone()));
    let mut commit_machine = ShardStateMachine::new(binding, commit_store.clone()).unwrap();
    let committed = TransactionRecord::new(
        transaction_id,
        TransactionState::Committed,
        TransactionTime::new(10).unwrap(),
        intent.digest(),
    )
    .unwrap();
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
        .unwrap();
    let state = commit_store.state.lock().unwrap();
    let mutations = state.batch.as_ref().unwrap().mutations();
    assert_eq!(mutations.len(), 2);
    assert!(
        mutations
            .iter()
            .any(|mutation| matches!(mutation, LogicalMutation::PutVertex(_)))
    );
    assert!(mutations.iter().any(|mutation| matches!(
        mutation,
        LogicalMutation::PutTransaction(record) if record.state() == TransactionState::Committed
    )));

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
    abort_machine
        .apply_committed(
            11,
            2,
            ShardCommand::FinalizeParticipant(
                FinalizeParticipant::new(CommandId::new(903).unwrap(), 7, 10, aborted, None)
                    .unwrap(),
            ),
        )
        .unwrap();
    let state = abort_store.state.lock().unwrap();
    assert_eq!(state.batch.as_ref().unwrap().mutations().len(), 1);
    assert!(matches!(
        &state.batch.as_ref().unwrap().mutations()[0],
        LogicalMutation::PutTransaction(record) if record.state() == TransactionState::Aborted
    ));
}

#[test]
fn participant_intent_is_current_only_bounded_and_digest_bound() {
    let transaction_id = TransactionId::new(91).unwrap();
    let original = ParticipantIntent::new(
        transaction_id,
        dtg_storage::ShardId::new(3).unwrap(),
        TransactionTime::new(5).unwrap(),
        vec![committed_vertex_mutation(900)],
    )
    .unwrap();
    let encoded = original.encode_current().unwrap();
    assert_eq!(
        ParticipantIntent::decode_current(&encoded).unwrap(),
        original
    );

    let mut unknown = encoded.clone();
    unknown[..4].copy_from_slice(&3_u32.to_be_bytes());
    assert!(ParticipantIntent::decode_current(&unknown).is_err());

    let mut trailing = encoded;
    trailing.push(0);
    assert!(ParticipantIntent::decode_current(&trailing).is_err());
    assert!(ParticipantIntent::decode_current(&vec![0; 4 * 1024 * 1024 + 1]).is_err());

    let changed = ParticipantIntent::new(
        transaction_id,
        dtg_storage::ShardId::new(3).unwrap(),
        TransactionTime::new(5).unwrap(),
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
        vec![committed_vertex_mutation(910)],
    )
    .unwrap();
    let at_six = ParticipantIntent::new(
        transaction_id,
        shard_id,
        TransactionTime::new(6).unwrap(),
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
    bytes.extend_from_slice(&2_u32.to_be_bytes());
    bytes.extend_from_slice(&93_u128.to_be_bytes());
    bytes.extend_from_slice(&3_u64.to_be_bytes());
    bytes.extend_from_slice(&5_i64.to_be_bytes());
    bytes.extend_from_slice(&4_097_u32.to_be_bytes());

    let error = ParticipantIntent::decode_current(&bytes).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("transaction intent mutation count")
    );
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

#[derive(Default)]
struct RecordedApply {
    batch: Option<CommittedShardBatch>,
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
                false
            };
            Ok(ApplyReceipt::new(&batch, replayed))
        })
    }

    fn begin_read_view(&self, _fence: ReadFence) -> StoreFuture<'_, Box<dyn TemporalReadView>> {
        Box::pin(async { Err(StorageError::Unsupported) })
    }
}
