use std::collections::BTreeMap;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};

use dtg_shard::{
    AdvanceClosedTimestamp, ApplyRejection, CommitSingleShard, MigrationCommand, MigrationPhase,
    RaftReplica, RaftStore, SUPPORTED_SHARD_COMMAND_FORMAT_VERSION, ShardCommand, ShardHost,
    ShardStateMachine,
};
use dtg_storage::{
    AdjacencyRead, ApplyReceipt, BackendClass, BindingRole, CapabilityManifest, ChangePage,
    ChangesRead, CommandId, CommittedShardBatch, ConsensusCommandEnvelope, ConsensusEntry,
    ConsensusSnapshotMetadata, ConsensusStore, Digest32, EdgeHistoryRead, EdgeId, EdgeRead,
    EdgeScan, EdgeVersion, LogicalMutation, Properties, ProviderKind, RaftHardState,
    RaftMembership, ReadFence, ReplicaBinding, ReplicaId, ReplicaMetadata, ReplicaStateStore,
    SUPPORTED_CONSENSUS_COMMAND_FORMAT_VERSION, SUPPORTED_CONSENSUS_WAL_FORMAT_VERSION, ScanPage,
    StorageError, StoreFuture, TemporalReadView, TransactionTime, ValidInterval, Value, Version,
    VertexHistoryRead, VertexId, VertexRead, VertexScan, VertexVersion,
};
use dtg_storage_fjall::{FjallConsensusStore, FjallReplicaStore};
use raft::Storage;
use raft::storage::GetEntriesContext;

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

#[test]
fn committed_wal_is_replayed_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let binding = fixture_binding(3, 4, ProviderKind::Fjall, "restart");
    let consensus_path = dir.path().join("consensus");
    let business_path = dir.path().join("business");
    {
        let consensus =
            Arc::new(FjallConsensusStore::open(&consensus_path, binding.clone()).unwrap());
        initialize_membership(&*consensus, binding.replica_id());
        let state = Arc::new(FjallReplicaStore::open(&business_path, binding.clone()).unwrap());
        let failing = Arc::new(ToggleStateStore::new(state.clone()));
        let mut replica = RaftReplica::open(consensus, failing.clone()).unwrap();
        replica.start().unwrap();
        replica.campaign().unwrap();
        replica.drive_ready().unwrap();
        failing.set_failing(true);
        replica.propose(committed_vertex_command(1, 7, 10)).unwrap();
        assert!(replica.drive_ready().is_err());
    }

    let consensus = Arc::new(FjallConsensusStore::open(&consensus_path, binding.clone()).unwrap());
    let state = Arc::new(FjallReplicaStore::open(&business_path, binding).unwrap());
    let mut replica = RaftReplica::open(consensus, state.clone()).unwrap();
    let recovered = replica.recover().unwrap();

    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].applied_index(), 2);
    assert_eq!(block_on(state.applied_index()).unwrap(), 2);
}

#[test]
fn committed_rejection_recovery_advances_to_the_following_wal_entry() {
    let dir = tempfile::tempdir().unwrap();
    let binding = fixture_binding(3, 4, ProviderKind::Fjall, "rejected-restart");
    let consensus_path = dir.path().join("consensus");
    let business_path = dir.path().join("business");
    let consensus = Arc::new(FjallConsensusStore::open(&consensus_path, binding.clone()).unwrap());
    let stale = committed_vertex_command(1, 6, 10);
    let following = committed_vertex_command(2, 7, 10);
    block_on(consensus.append(vec![
        committed_command_entry(1, 1, &stale),
        committed_command_entry(1, 2, &following),
    ]))
    .unwrap();
    block_on(consensus.set_hard_state(RaftHardState {
        current_term: 1,
        voted_for: None,
        committed_index: 2,
    }))
    .unwrap();
    let state = Arc::new(FjallReplicaStore::open(&business_path, binding.clone()).unwrap());
    let mut replica = RaftReplica::open(consensus, state.clone()).unwrap();

    let recovered = replica.recover().unwrap();

    assert_eq!(recovered.len(), 2);
    assert_eq!(
        recovered[0].rejection(),
        Some(ApplyRejection::StalePlacementEpoch)
    );
    assert_eq!(recovered[0].applied_index(), 1);
    assert_eq!(recovered[1].rejection(), None);
    assert_eq!(recovered[1].applied_index(), 2);
    assert_eq!(block_on(state.applied_index()).unwrap(), 2);
    drop(replica);

    let consensus = Arc::new(FjallConsensusStore::open(consensus_path, binding.clone()).unwrap());
    let state = Arc::new(FjallReplicaStore::open(business_path, binding).unwrap());
    let mut reopened = RaftReplica::open(consensus, state).unwrap();
    assert!(reopened.recover().unwrap().is_empty());
    assert_eq!(reopened.observe().applied_index(), 2);
}

#[test]
fn closed_timestamp_is_rebuilt_from_committed_wal_on_restart() {
    let dir = tempfile::tempdir().unwrap();
    let binding = fixture_binding(14, 15, ProviderKind::Fjall, "closed-restart");
    let consensus_path = dir.path().join("consensus");
    let business_path = dir.path().join("business");
    {
        let consensus =
            Arc::new(FjallConsensusStore::open(&consensus_path, binding.clone()).unwrap());
        initialize_membership(&*consensus, binding.replica_id());
        let state = Arc::new(FjallReplicaStore::open(&business_path, binding.clone()).unwrap());
        let mut replica = RaftReplica::open(consensus, state).unwrap();
        replica.start().unwrap();
        replica.campaign().unwrap();
        replica.drive_ready().unwrap();
        replica
            .propose(ShardCommand::AdvanceClosedTimestamp(
                AdvanceClosedTimestamp::new(
                    CommandId::new(55).unwrap(),
                    7,
                    10,
                    TransactionTime::new(44).unwrap(),
                )
                .unwrap(),
            ))
            .unwrap();
        replica.drive_ready().unwrap();
    }

    let consensus = Arc::new(FjallConsensusStore::open(&consensus_path, binding.clone()).unwrap());
    let state = Arc::new(FjallReplicaStore::open(&business_path, binding).unwrap());
    let mut restarted = RaftReplica::open(consensus, state).unwrap();
    restarted.recover().unwrap();

    assert_eq!(
        restarted.observe().closed_timestamp(),
        Some(TransactionTime::new(44).unwrap())
    );
}

#[test]
fn snapshot_recovery_replays_only_the_committed_suffix() {
    let dir = tempfile::tempdir().unwrap();
    let binding = fixture_binding(3, 4, ProviderKind::Fjall, "snapshot-suffix");
    let consensus =
        Arc::new(FjallConsensusStore::open(dir.path().join("consensus"), binding.clone()).unwrap());
    initialize_membership(&*consensus, binding.replica_id());
    block_on(consensus.set_snapshot_metadata(ConsensusSnapshotMetadata {
        snapshot_id: 99,
        last_included_term: 7,
        last_included_index: 2,
        content_digest: Digest32::new([9; 32]),
    }))
    .unwrap();
    block_on(consensus.set_hard_state(RaftHardState {
        current_term: 7,
        voted_for: None,
        committed_index: 2,
    }))
    .unwrap();

    let state = Arc::new(RecordingStateStore::with_applied(binding, 2));
    let toggled = Arc::new(ToggleStateStore::new(state.clone()));
    toggled.set_failing(true);
    {
        let mut replica = RaftReplica::open(consensus.clone(), toggled.clone()).unwrap();
        replica.start().unwrap();
        replica.campaign().unwrap();
        assert!(replica.drive_ready().is_err());
        replica.propose(committed_vertex_command(4, 7, 10)).unwrap();
        assert!(replica.drive_ready().is_err());
    }
    toggled.set_failing(false);
    let mut replica = RaftReplica::open(consensus, state.clone()).unwrap();
    let recovered = replica.recover().unwrap();

    assert_eq!(
        recovered
            .iter()
            .map(|outcome| outcome.applied_index())
            .collect::<Vec<_>>(),
        vec![3, 4]
    );
    assert_eq!(state.applied_indices(), vec![3, 4]);
}

#[test]
fn stale_epoch_and_generation_advance_as_committed_rejected_noops() {
    let binding = fixture_binding(3, 4, ProviderKind::Fjall, "fences");
    let state = Arc::new(RecordingStateStore::with_applied(binding.clone(), 0));
    let mut machine = ShardStateMachine::new(binding, state.clone()).unwrap();

    assert_eq!(
        machine
            .apply_committed(1, 1, committed_vertex_command(1, 6, 10))
            .unwrap()
            .rejection(),
        Some(ApplyRejection::StalePlacementEpoch)
    );
    assert_eq!(
        machine
            .apply_committed(1, 2, committed_vertex_command(2, 7, 9))
            .unwrap()
            .rejection(),
        Some(ApplyRejection::StaleBackendGeneration)
    );
    assert_eq!(machine.applied_index(), 2);
    assert_eq!(state.apply_calls(), 2);
}

#[test]
fn activate_switches_the_authoritative_store_and_following_phases_survive_restart() {
    let source_binding = fixture_binding(3, 10, ProviderKind::Fjall, "migration-source");
    let target_binding = source_binding
        .to_builder()
        .placement_epoch(8)
        .backend_generation(11)
        .backend_class_digest(Digest32::new([11; 32]))
        .provider_kind(ProviderKind::PostgreSql)
        .namespace_id("migration-target")
        .role(BindingRole::Active)
        .build()
        .unwrap();
    let source = Arc::new(RecordingStateStore::with_applied(source_binding.clone(), 1));
    let target = Arc::new(RecordingStateStore::with_applied(target_binding.clone(), 1));
    let mut machine = ShardStateMachine::new(source_binding, source.clone()).unwrap();
    machine.stage_migration_state_store(target.clone()).unwrap();

    let activate = ShardCommand::Migration(
        MigrationCommand::online(
            CommandId::new(8_001).unwrap(),
            77,
            7,
            10,
            10,
            11,
            target_binding.backend_class_digest(),
            Version::new(12),
            1,
            Digest32::new([9; 32]),
            MigrationPhase::Activate,
        )
        .unwrap(),
    );
    let activated = machine.apply_committed(11, 2, activate).unwrap();
    assert_eq!(activated.active_binding(), &target_binding);
    assert_eq!(machine.binding(), &target_binding);
    assert_eq!(source.apply_calls(), 0);
    assert_eq!(target.apply_calls(), 1);

    let grace = ShardCommand::Migration(
        MigrationCommand::online(
            CommandId::new(8_002).unwrap(),
            77,
            7,
            10,
            10,
            11,
            target_binding.backend_class_digest(),
            Version::new(12),
            1,
            Digest32::new([9; 32]),
            MigrationPhase::Grace,
        )
        .unwrap(),
    );
    machine.apply_committed(11, 3, grace).unwrap();
    drop(machine);

    let mut reopened = ShardStateMachine::new(target_binding.clone(), target.clone()).unwrap();
    let retire = ShardCommand::Migration(
        MigrationCommand::online(
            CommandId::new(8_003).unwrap(),
            77,
            7,
            10,
            10,
            11,
            target_binding.backend_class_digest(),
            Version::new(12),
            1,
            Digest32::new([9; 32]),
            MigrationPhase::Retire,
        )
        .unwrap(),
    );
    reopened.apply_committed(11, 4, retire).unwrap();
    assert_eq!(reopened.binding(), &target_binding);
    assert_eq!(target.apply_calls(), 3);
}

#[test]
fn migration_cutover_retains_the_staged_target_after_transient_apply_failure() {
    let source_binding = fixture_binding(3, 10, ProviderKind::Fjall, "migration-retry-source");
    let target_binding = source_binding
        .to_builder()
        .placement_epoch(8)
        .backend_generation(11)
        .backend_class_digest(Digest32::new([11; 32]))
        .provider_kind(ProviderKind::PostgreSql)
        .namespace_id("migration-retry-target")
        .build()
        .unwrap();
    let source = Arc::new(RecordingStateStore::with_applied(source_binding.clone(), 1));
    let target = Arc::new(RecordingStateStore::with_applied(target_binding.clone(), 1));
    let toggled = Arc::new(ToggleStateStore::new(target.clone()));
    let mut machine = ShardStateMachine::new(source_binding.clone(), source).unwrap();
    machine
        .stage_migration_state_store(toggled.clone())
        .unwrap();
    let activate = ShardCommand::Migration(
        MigrationCommand::online(
            CommandId::new(8_010).unwrap(),
            78,
            7,
            10,
            10,
            11,
            target_binding.backend_class_digest(),
            Version::new(12),
            1,
            Digest32::new([10; 32]),
            MigrationPhase::Activate,
        )
        .unwrap(),
    );
    toggled.set_failing(true);

    assert!(machine.apply_committed(11, 2, activate.clone()).is_err());
    assert_eq!(machine.binding(), &source_binding);
    toggled.set_failing(false);
    let retried = machine.apply_committed(11, 2, activate).unwrap();

    assert_eq!(retried.active_binding(), &target_binding);
    assert_eq!(machine.binding(), &target_binding);
    assert_eq!(target.apply_calls(), 1);
}

#[test]
fn rollback_switches_the_raft_replica_binding_and_survives_restart() {
    let root = tempfile::tempdir().unwrap();
    let consensus_path = root.path().join("rollback-consensus");
    let target_path = root.path().join("rollback-target");
    let consensus_binding = fixture_binding(3, 4, ProviderKind::Fjall, "rollback-consensus")
        .to_builder()
        .placement_epoch(8)
        .backend_generation(11)
        .build()
        .unwrap();
    let source_binding = consensus_binding
        .to_builder()
        .backend_class_digest(Digest32::new([11; 32]))
        .provider_kind(ProviderKind::PostgreSql)
        .namespace_id("rollback-source")
        .build()
        .unwrap();
    let target_binding = fixture_binding(3, 4, ProviderKind::Fjall, "rollback-target")
        .to_builder()
        .placement_epoch(9)
        .backend_generation(12)
        .build()
        .unwrap();
    let consensus =
        Arc::new(FjallConsensusStore::open(&consensus_path, consensus_binding.clone()).unwrap());
    initialize_membership(&*consensus, consensus_binding.replica_id());
    let source = Arc::new(RecordingStateStore::with_applied(source_binding, 0));
    let mut replica = RaftReplica::open(consensus, source).unwrap();
    replica.start().unwrap();
    replica.campaign().unwrap();
    replica.drive_ready().unwrap();
    assert_eq!(replica.observe().applied_index(), 1);
    let source_command = committed_vertex_command(8_000, 8, 11);
    replica.propose(source_command.clone()).unwrap();
    let source_progress = replica.drive_ready().unwrap();
    let source_receipt = source_progress.receipts().last().unwrap();
    assert_eq!(source_receipt.index(), 2);

    let target = Arc::new(FjallReplicaStore::open(&target_path, target_binding.clone()).unwrap());
    block_on(
        target.apply(
            CommittedShardBatch::new(
                target_binding.clone(),
                1,
                1,
                CommandId::new((u128::from(1_u64) << 64) | 1).unwrap(),
                vec![LogicalMutation::PutReplicaMetadata(
                    ReplicaMetadata::new(
                        "dtg.raft_noop",
                        Value::Bytes(1_u64.to_be_bytes().to_vec()),
                    )
                    .unwrap(),
                )],
            )
            .unwrap(),
        ),
    )
    .unwrap();
    let ShardCommand::CommitSingleShard(source_commit) = source_command else {
        unreachable!();
    };
    block_on(
        target.apply(
            CommittedShardBatch::new(
                target_binding.clone(),
                source_receipt.term(),
                source_receipt.index(),
                CommandId::new(source_receipt.command_id()).unwrap(),
                source_commit.mutations().to_vec(),
            )
            .unwrap(),
        ),
    )
    .unwrap();
    replica.stage_migration_state_store(target.clone()).unwrap();
    let rollback = ShardCommand::Migration(
        MigrationCommand::online(
            CommandId::new(8_001).unwrap(),
            77,
            8,
            11,
            10,
            12,
            target_binding.backend_class_digest(),
            Version::new(12),
            2,
            Digest32::new([9; 32]),
            MigrationPhase::Rollback,
        )
        .unwrap(),
    );
    replica.propose(rollback).unwrap();
    let progress = replica.drive_ready().unwrap();
    let receipt = progress.receipts().last().unwrap();
    assert_eq!(receipt.rejection(), None);
    assert_eq!(receipt.active_binding(), &target_binding);
    assert_eq!(replica.observe().binding(), &target_binding);
    drop(replica);

    let consensus =
        Arc::new(FjallConsensusStore::open(&consensus_path, consensus_binding.clone()).unwrap());
    let target = Arc::new(FjallReplicaStore::open(&target_path, target_binding.clone()).unwrap());
    let mut reopened = RaftReplica::open(consensus, target.clone()).unwrap();
    assert!(reopened.recover().unwrap().is_empty());
    assert_eq!(reopened.observe().binding(), &target_binding);
    reopened.start().unwrap();
    reopened.campaign().unwrap();
    reopened.drive_ready().unwrap();
    reopened
        .propose(committed_vertex_command(8_002, 9, 12))
        .unwrap();
    let progress = reopened.drive_ready().unwrap();
    assert_eq!(
        progress.receipts().last().unwrap().active_binding(),
        &target_binding
    );
    assert_eq!(block_on(target.applied_index()).unwrap(), 5);
}

#[test]
fn one_host_runs_independent_shards_with_heterogeneous_providers() {
    let root = tempfile::tempdir().unwrap();
    let fjall_binding = fixture_binding(3, 4, ProviderKind::Fjall, "host-fjall");
    let postgres_binding = fixture_binding(5, 6, ProviderKind::PostgreSql, "host-postgres");
    let fjall_consensus = Arc::new(
        FjallConsensusStore::open(root.path().join("fjall-consensus"), fjall_binding.clone())
            .unwrap(),
    );
    let postgres_consensus_binding =
        fixture_binding(5, 6, ProviderKind::Fjall, "host-postgres-consensus");
    let postgres_consensus = Arc::new(
        FjallConsensusStore::open(
            root.path().join("postgres-consensus"),
            postgres_consensus_binding,
        )
        .unwrap(),
    );
    initialize_membership(&*fjall_consensus, fjall_binding.replica_id());
    initialize_membership(&*postgres_consensus, postgres_binding.replica_id());

    let mut host = ShardHost::new();
    let fjall_key = host
        .add(
            fjall_consensus,
            Arc::new(RecordingStateStore::with_applied(fjall_binding, 0)),
        )
        .unwrap();
    let postgres_key = host
        .add(
            postgres_consensus,
            Arc::new(RecordingStateStore::with_applied(postgres_binding, 0)),
        )
        .unwrap();

    host.start(fjall_key).unwrap();
    host.start(postgres_key).unwrap();
    let fjall = host.observe(fjall_key).unwrap();
    let postgres = host.observe(postgres_key).unwrap();

    assert_eq!(host.len(), 2);
    assert_eq!(fjall.provider_kind(), &ProviderKind::Fjall);
    assert_eq!(postgres.provider_kind(), &ProviderKind::PostgreSql);
    assert!(fjall.is_running());
    assert!(postgres.is_running());

    host.stop(fjall_key).unwrap();
    assert!(host.observe(postgres_key).unwrap().is_running());
    host.seal(fjall_key).unwrap();
    host.sealed_remove(fjall_key).unwrap();
    assert_eq!(host.len(), 1);
    assert!(host.observe(postgres_key).unwrap().is_running());
}

#[test]
fn two_raft_groups_commit_and_survive_independent_disruption() {
    let root = tempfile::tempdir().unwrap();
    let first_binding = fixture_binding(40, 41, ProviderKind::Fjall, "multi-first");
    let second_binding = fixture_binding(42, 43, ProviderKind::Fjall, "multi-second");
    let first_consensus = Arc::new(
        FjallConsensusStore::open(root.path().join("first-consensus"), first_binding.clone())
            .unwrap(),
    );
    let second_consensus = Arc::new(
        FjallConsensusStore::open(root.path().join("second-consensus"), second_binding.clone())
            .unwrap(),
    );
    initialize_membership(&*first_consensus, first_binding.replica_id());
    initialize_membership(&*second_consensus, second_binding.replica_id());
    let first_state = Arc::new(
        FjallReplicaStore::open(root.path().join("first-business"), first_binding).unwrap(),
    );
    let second_state = Arc::new(
        FjallReplicaStore::open(root.path().join("second-business"), second_binding).unwrap(),
    );
    let mut host = ShardHost::new();
    let first = host.add(first_consensus, first_state.clone()).unwrap();
    let second = host.add(second_consensus, second_state.clone()).unwrap();
    for key in [first, second] {
        host.start(key).unwrap();
        host.campaign(key).unwrap();
        host.drive_ready(key).unwrap();
    }
    host.propose(first, committed_vertex_command(1, 7, 10))
        .unwrap();
    host.propose(second, committed_vertex_command(2, 7, 10))
        .unwrap();
    host.drive_ready(first).unwrap();
    host.drive_ready(second).unwrap();
    assert_eq!(block_on(first_state.applied_index()).unwrap(), 2);
    assert_eq!(block_on(second_state.applied_index()).unwrap(), 2);

    host.stop(first).unwrap();
    host.seal(first).unwrap();
    let removed: () = host.sealed_remove(first).unwrap();
    assert_eq!(removed, ());
    host.propose(second, committed_vertex_command(3, 7, 10))
        .unwrap();
    host.drive_ready(second).unwrap();

    assert_eq!(block_on(first_state.applied_index()).unwrap(), 2);
    assert_eq!(block_on(second_state.applied_index()).unwrap(), 3);
    assert!(host.observe(second).unwrap().is_running());
}

#[test]
fn single_replica_proposal_is_applied_from_raft_ready() {
    let root = tempfile::tempdir().unwrap();
    let binding = fixture_binding(9, 10, ProviderKind::Fjall, "raft-ready");
    let consensus = Arc::new(
        FjallConsensusStore::open(root.path().join("consensus"), binding.clone()).unwrap(),
    );
    initialize_membership(&*consensus, binding.replica_id());
    let state = Arc::new(RecordingStateStore::with_applied(binding, 0));
    let mut host = ShardHost::new();
    let key = host.add(consensus, state.clone()).unwrap();
    host.start(key).unwrap();
    host.campaign(key).unwrap();
    host.drive_ready(key).unwrap();

    host.propose(key, committed_vertex_command(77, 7, 10))
        .unwrap();
    let progress = host.drive_ready(key).unwrap();

    assert_eq!(progress.receipts().len(), 1);
    assert_eq!(progress.receipts()[0].command_id(), 77);
    assert_eq!(state.applied_indices(), vec![1, 2]);
}

#[test]
fn transient_apply_failure_is_retried_in_process() {
    let root = tempfile::tempdir().unwrap();
    let binding = fixture_binding(30, 31, ProviderKind::Fjall, "pending-retry");
    let consensus = Arc::new(
        FjallConsensusStore::open(root.path().join("consensus"), binding.clone()).unwrap(),
    );
    initialize_membership(&*consensus, binding.replica_id());
    let state = Arc::new(RecordingStateStore::with_applied(binding, 0));
    let toggled = Arc::new(ToggleStateStore::new(state.clone()));
    let mut replica = RaftReplica::open(consensus, toggled.clone()).unwrap();
    replica.start().unwrap();
    replica.campaign().unwrap();
    replica.drive_ready().unwrap();
    toggled.set_failing(true);
    replica
        .propose(committed_vertex_command(88, 7, 10))
        .unwrap();
    assert!(replica.drive_ready().is_err());

    toggled.set_failing(false);
    let retried = replica.drive_ready().unwrap();

    assert_eq!(retried.receipts().len(), 1);
    assert_eq!(retried.receipts()[0].command_id(), 88);
    assert_eq!(state.applied_indices(), vec![1, 2]);
    assert!(replica.observe().is_running());
}

#[test]
fn sealed_replica_is_terminal_and_cannot_recover() {
    let root = tempfile::tempdir().unwrap();
    let binding = fixture_binding(32, 33, ProviderKind::Fjall, "sealed-terminal");
    let consensus = Arc::new(
        FjallConsensusStore::open(root.path().join("consensus"), binding.clone()).unwrap(),
    );
    let state = Arc::new(RecordingStateStore::with_applied(binding, 0));
    let mut replica = RaftReplica::open(consensus, state).unwrap();
    replica.seal().unwrap();

    let error = replica.recover().unwrap_err();

    assert_eq!(error.code(), "DTG-SHARD-LIFECYCLE");
}

#[test]
fn sealing_rejects_pending_committed_apply() {
    let root = tempfile::tempdir().unwrap();
    let binding = fixture_binding(34, 35, ProviderKind::Fjall, "sealed-pending");
    let consensus = Arc::new(
        FjallConsensusStore::open(root.path().join("consensus"), binding.clone()).unwrap(),
    );
    initialize_membership(&*consensus, binding.replica_id());
    let state = Arc::new(RecordingStateStore::with_applied(binding, 0));
    let toggled = Arc::new(ToggleStateStore::new(state));
    let mut replica = RaftReplica::open(consensus, toggled.clone()).unwrap();
    replica.start().unwrap();
    replica.campaign().unwrap();
    replica.drive_ready().unwrap();
    toggled.set_failing(true);
    replica
        .propose(committed_vertex_command(89, 7, 10))
        .unwrap();
    assert!(replica.drive_ready().is_err());
    replica.stop().unwrap();

    let error = replica.seal().unwrap_err();

    assert_eq!(error.code(), "DTG-SHARD-LIFECYCLE");
}

#[test]
fn stopping_rejects_unprocessed_ready_work() {
    let root = tempfile::tempdir().unwrap();
    let binding = fixture_binding(44, 45, ProviderKind::Fjall, "stop-ready");
    let consensus = Arc::new(
        FjallConsensusStore::open(root.path().join("consensus"), binding.clone()).unwrap(),
    );
    initialize_membership(&*consensus, binding.replica_id());
    let state = Arc::new(RecordingStateStore::with_applied(binding, 0));
    let mut replica = RaftReplica::open(consensus, state).unwrap();
    replica.start().unwrap();
    replica.campaign().unwrap();

    let error = replica.stop().unwrap_err();

    assert_eq!(error.code(), "DTG-SHARD-LIFECYCLE");
    assert!(replica.observe().is_running());
}

#[test]
fn host_keys_same_local_ids_by_graph_identity() {
    let root = tempfile::tempdir().unwrap();
    let first = fixture_binding_in_graph(2, 12, 13, ProviderKind::Fjall, "graph-2");
    let second = fixture_binding_in_graph(3, 12, 13, ProviderKind::Fjall, "graph-3");
    let first_consensus =
        Arc::new(FjallConsensusStore::open(root.path().join("first"), first.clone()).unwrap());
    let second_consensus =
        Arc::new(FjallConsensusStore::open(root.path().join("second"), second.clone()).unwrap());
    let mut host = ShardHost::new();

    let first_key = host
        .add(
            first_consensus,
            Arc::new(RecordingStateStore::with_applied(first, 0)),
        )
        .unwrap();
    let second_key = host
        .add(
            second_consensus,
            Arc::new(RecordingStateStore::with_applied(second, 0)),
        )
        .unwrap();

    assert_ne!(first_key, second_key);
    assert_eq!(host.len(), 2);
}

#[test]
fn host_rejects_heterogeneous_replicas_in_one_generation() {
    let root = tempfile::tempdir().unwrap();
    let fjall = fixture_binding(20, 21, ProviderKind::Fjall, "homogeneous-fjall");
    let postgres = fixture_binding(20, 22, ProviderKind::PostgreSql, "homogeneous-postgres");
    let fjall_consensus =
        Arc::new(FjallConsensusStore::open(root.path().join("fjall"), fjall.clone()).unwrap());
    let postgres_consensus_binding = fixture_binding(
        20,
        22,
        ProviderKind::Fjall,
        "homogeneous-postgres-consensus",
    );
    let postgres_consensus = Arc::new(
        FjallConsensusStore::open(root.path().join("postgres"), postgres_consensus_binding)
            .unwrap(),
    );
    let mut host = ShardHost::new();
    host.add(
        fjall_consensus,
        Arc::new(RecordingStateStore::with_applied(fjall, 0)),
    )
    .unwrap();

    let error = host
        .add(
            postgres_consensus,
            Arc::new(RecordingStateStore::with_applied(postgres, 0)),
        )
        .unwrap_err();

    assert_eq!(error.code(), "DTG-SHARD-HETEROGENEOUS-GENERATION");
}

#[test]
fn unknown_command_versions_fail_closed() {
    let command = committed_vertex_command(1, 7, 10);
    let mut encoded = command.encode_current().unwrap();
    encoded[..4].copy_from_slice(&(SUPPORTED_SHARD_COMMAND_FORMAT_VERSION + 1).to_be_bytes());
    let error = ShardCommand::decode(&encoded).unwrap_err();
    assert_eq!(error.code(), "DTG-SHARD-COMMAND-VERSION");
}

#[test]
fn retained_log_gap_after_snapshot_fails_closed() {
    let root = tempfile::tempdir().unwrap();
    let binding = fixture_binding(36, 37, ProviderKind::Fjall, "retained-gap");
    let consensus =
        Arc::new(FjallConsensusStore::open(root.path().join("consensus"), binding).unwrap());
    block_on(consensus.set_snapshot_metadata(ConsensusSnapshotMetadata {
        snapshot_id: 1,
        last_included_term: 2,
        last_included_index: 2,
        content_digest: Digest32::new([3; 32]),
    }))
    .unwrap();
    block_on(consensus.append(vec![internal_noop_entry(3, 4, 0)])).unwrap();
    let wal = RaftStore::new(consensus).unwrap();

    assert!(Storage::first_index(&wal).is_err());
}

#[test]
fn oversized_internal_wal_entry_fails_before_materialization() {
    let root = tempfile::tempdir().unwrap();
    let binding = fixture_binding(38, 39, ProviderKind::Fjall, "oversized-wal");
    let consensus =
        Arc::new(FjallConsensusStore::open(root.path().join("consensus"), binding).unwrap());
    block_on(consensus.append(vec![internal_noop_entry(1, 1, 17 * 1024 * 1024)])).unwrap();
    let wal = RaftStore::new(consensus).unwrap();

    let result = Storage::entries(&wal, 1, 2, None, GetEntriesContext::empty(false));
    assert!(result.is_err());
}

fn internal_noop_entry(term: u64, index: u64, context_len: usize) -> ConsensusEntry {
    let mut payload = Vec::with_capacity(13 + context_len);
    payload.extend_from_slice(&1_u32.to_be_bytes());
    payload.push(0);
    payload.extend_from_slice(&(context_len as u32).to_be_bytes());
    payload.resize(payload.len() + context_len, 0);
    payload.extend_from_slice(&0_u32.to_be_bytes());
    ConsensusEntry::new(
        SUPPORTED_CONSENSUS_WAL_FORMAT_VERSION,
        term,
        index,
        CommandId::new((u128::from(term) << 64) | u128::from(index)).unwrap(),
        ConsensusCommandEnvelope::new(SUPPORTED_CONSENSUS_COMMAND_FORMAT_VERSION, payload).unwrap(),
    )
    .unwrap()
}

fn committed_command_entry(term: u64, index: u64, command: &ShardCommand) -> ConsensusEntry {
    let context = command.header().command_id().get().to_be_bytes();
    let data = command.encode_current().unwrap();
    let mut payload = Vec::with_capacity(13 + context.len() + data.len());
    payload.extend_from_slice(&1_u32.to_be_bytes());
    payload.push(0);
    payload.extend_from_slice(&(context.len() as u32).to_be_bytes());
    payload.extend_from_slice(&context);
    payload.extend_from_slice(&(data.len() as u32).to_be_bytes());
    payload.extend_from_slice(&data);
    ConsensusEntry::new(
        SUPPORTED_CONSENSUS_WAL_FORMAT_VERSION,
        term,
        index,
        command.header().command_id(),
        ConsensusCommandEnvelope::new(SUPPORTED_CONSENSUS_COMMAND_FORMAT_VERSION, payload).unwrap(),
    )
    .unwrap()
}

fn initialize_membership(store: &dyn ConsensusStore, replica_id: ReplicaId) {
    block_on(store.set_membership(RaftMembership {
        voters: vec![replica_id],
        learners: vec![],
        configuration_index: 0,
    }))
    .unwrap();
}

fn committed_vertex_command(
    command_id: u128,
    placement_epoch: u64,
    backend_generation: u64,
) -> ShardCommand {
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

fn fixture_binding(
    shard_id: u64,
    replica_id: u64,
    provider_kind: ProviderKind,
    namespace: &str,
) -> ReplicaBinding {
    fixture_binding_in_graph(2, shard_id, replica_id, provider_kind, namespace)
}

fn fixture_binding_in_graph(
    graph_id: u64,
    shard_id: u64,
    replica_id: u64,
    provider_kind: ProviderKind,
    namespace: &str,
) -> ReplicaBinding {
    let capabilities = match &provider_kind {
        ProviderKind::Fjall => CapabilityManifest::from_names([
            "adjacency",
            "immutable-read-view",
            "logical-snapshot",
            "point",
        ])
        .unwrap(),
        _ => CapabilityManifest::from_names(Vec::<String>::new()).unwrap(),
    };
    let class = BackendClass::new(
        provider_kind.clone(),
        1,
        1,
        capabilities.names().map(str::to_owned),
    )
    .unwrap();
    ReplicaBinding::builder()
        .cluster_id(1)
        .graph_id(graph_id)
        .shard_id(shard_id)
        .placement_epoch(7)
        .replica_id(replica_id)
        .backend_generation(10)
        .backend_class_digest(class.digest())
        .provider_kind(provider_kind)
        .contract_version(1)
        .layout_version(1)
        .capability_digest(capabilities.digest())
        .namespace_id(namespace)
        .endpoint_profile_ref("local")
        .credential_ref("none")
        .role(BindingRole::Active)
        .build()
        .unwrap()
}

#[derive(Clone)]
struct ReplayIdentity {
    term: u64,
    command_id: CommandId,
    digest: Digest32,
}

struct RecordingState {
    applied_index: u64,
    replay: BTreeMap<u64, ReplayIdentity>,
    applied_indices: Vec<u64>,
    apply_calls: usize,
}

struct RecordingStateStore {
    binding: ReplicaBinding,
    state: Mutex<RecordingState>,
}

struct ToggleStateStore<S> {
    inner: Arc<S>,
    failing: AtomicBool,
}

struct RecordingReadView {
    fence: ReadFence,
}

impl<S> ToggleStateStore<S> {
    fn new(inner: Arc<S>) -> Self {
        Self {
            inner,
            failing: AtomicBool::new(false),
        }
    }

    fn set_failing(&self, failing: bool) {
        self.failing.store(failing, Ordering::SeqCst);
    }
}

impl<S: ReplicaStateStore> ReplicaStateStore for ToggleStateStore<S> {
    fn binding(&self) -> &ReplicaBinding {
        self.inner.binding()
    }

    fn applied_index(&self) -> StoreFuture<'_, u64> {
        self.inner.applied_index()
    }

    fn replica_metadata<'a>(&'a self, name: &'a str) -> StoreFuture<'a, Option<ReplicaMetadata>> {
        self.inner.replica_metadata(name)
    }

    fn apply(&self, batch: CommittedShardBatch) -> StoreFuture<'_, ApplyReceipt> {
        Box::pin(async move {
            if self.failing.load(Ordering::SeqCst) {
                return Err(StorageError::Internal("injected restart boundary".into()));
            }
            self.inner.apply(batch).await
        })
    }

    fn begin_read_view(&self, fence: ReadFence) -> StoreFuture<'_, Box<dyn TemporalReadView>> {
        self.inner.begin_read_view(fence)
    }
}

impl RecordingStateStore {
    fn with_applied(binding: ReplicaBinding, applied_index: u64) -> Self {
        Self {
            binding,
            state: Mutex::new(RecordingState {
                applied_index,
                replay: BTreeMap::new(),
                applied_indices: Vec::new(),
                apply_calls: 0,
            }),
        }
    }

    fn applied_indices(&self) -> Vec<u64> {
        self.state.lock().unwrap().applied_indices.clone()
    }

    fn apply_calls(&self) -> usize {
        self.state.lock().unwrap().apply_calls
    }
}

impl ReplicaStateStore for RecordingStateStore {
    fn binding(&self) -> &ReplicaBinding {
        &self.binding
    }

    fn applied_index(&self) -> StoreFuture<'_, u64> {
        Box::pin(async move { Ok(self.state.lock().unwrap().applied_index) })
    }

    fn replica_metadata<'a>(&'a self, _name: &'a str) -> StoreFuture<'a, Option<ReplicaMetadata>> {
        Box::pin(async { Ok(None) })
    }

    fn apply(&self, batch: CommittedShardBatch) -> StoreFuture<'_, ApplyReceipt> {
        Box::pin(async move {
            let mut state = self.state.lock().unwrap();
            state.apply_calls += 1;
            if batch.raft_index() <= state.applied_index {
                let identity =
                    state
                        .replay
                        .get(&batch.raft_index())
                        .ok_or(StorageError::ReplayMismatch {
                            raft_index: batch.raft_index(),
                        })?;
                if identity.term != batch.raft_term()
                    || identity.command_id != batch.command_id()
                    || identity.digest != batch.mutation_digest()
                {
                    return Err(StorageError::ReplayMismatch {
                        raft_index: batch.raft_index(),
                    });
                }
                return Ok(ApplyReceipt::new(&batch, true));
            }
            if batch.raft_index() != state.applied_index + 1 {
                return Err(StorageError::NonMonotonicIndex {
                    applied: state.applied_index,
                    proposed: batch.raft_index(),
                });
            }
            state.applied_index = batch.raft_index();
            state.applied_indices.push(batch.raft_index());
            state.replay.insert(
                batch.raft_index(),
                ReplayIdentity {
                    term: batch.raft_term(),
                    command_id: batch.command_id(),
                    digest: batch.mutation_digest(),
                },
            );
            Ok(ApplyReceipt::new(&batch, false))
        })
    }

    fn begin_read_view(&self, fence: ReadFence) -> StoreFuture<'_, Box<dyn TemporalReadView>> {
        Box::pin(
            async move { Ok(Box::new(RecordingReadView { fence }) as Box<dyn TemporalReadView>) },
        )
    }
}

impl TemporalReadView for RecordingReadView {
    fn fence(&self) -> &ReadFence {
        &self.fence
    }

    fn get_vertex(&self, _request: VertexRead) -> StoreFuture<'_, Option<VertexVersion>> {
        Box::pin(async { Ok(None) })
    }

    fn get_edge(&self, _request: EdgeRead) -> StoreFuture<'_, Option<EdgeVersion>> {
        Box::pin(async { Ok(None) })
    }

    fn vertex_history(&self, _request: VertexHistoryRead) -> StoreFuture<'_, Vec<VertexVersion>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn edge_history(&self, _request: EdgeHistoryRead) -> StoreFuture<'_, Vec<EdgeVersion>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn expand(&self, _request: AdjacencyRead) -> StoreFuture<'_, Vec<EdgeVersion>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn changes(&self, _request: ChangesRead) -> StoreFuture<'_, ChangePage> {
        Box::pin(async { Ok(ChangePage::new(Vec::new(), None)) })
    }

    fn scan_vertices(
        &self,
        _request: VertexScan,
    ) -> StoreFuture<'_, ScanPage<VertexVersion, VertexId>> {
        Box::pin(async { Ok(ScanPage::new(Vec::new(), None)) })
    }

    fn scan_edges(&self, _request: EdgeScan) -> StoreFuture<'_, ScanPage<EdgeVersion, EdgeId>> {
        Box::pin(async { Ok(ScanPage::new(Vec::new(), None)) })
    }
}
