use std::sync::{Arc, Mutex};

use dtg_shard::{
    AdvanceClosedTimestamp, CommitSingleShard, RecordHomeDecision, ShardCommand, ShardStateMachine,
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
    let vertex = VertexVersion::new(
        VertexId::new(command_id).unwrap(),
        Version::new(1),
        ValidInterval::new(0, 100).unwrap(),
        TransactionTime::new(10).unwrap(),
        Properties::new(),
    )
    .unwrap();
    ShardCommand::CommitSingleShard(
        CommitSingleShard::new(
            CommandId::new(command_id).unwrap(),
            placement_epoch,
            backend_generation,
            vec![LogicalMutation::PutVertex(vertex)],
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
