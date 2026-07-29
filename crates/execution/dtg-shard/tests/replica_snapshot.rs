use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use dtg_shard::{
    CommitSingleShard, RaftReplica, ReplicaSnapshot, ReplicaSnapshotInstallState,
    ReplicaSnapshotManifest, SUPPORTED_REPLICA_SNAPSHOT_FORMAT_VERSION, ShardCommand,
    create_replica_snapshot, install_replica_snapshot, recover_replica_snapshot_install,
};
use dtg_storage::{
    BackendClass, BindingRole, CapabilityManifest, CommandId, ConsensusCommandEnvelope,
    ConsensusEntry, ConsensusStore, LogicalMutation, LogicalReplicaActivation,
    LogicalReplicaActivationReceipt, LogicalSnapshotCandidateReceipt, Properties, ProviderKind,
    RaftHardState, RaftMembership, ReplicaBinding, ReplicaStateStore,
    SUPPORTED_CONSENSUS_COMMAND_FORMAT_VERSION, SUPPORTED_CONSENSUS_WAL_FORMAT_VERSION,
    StorageError, StoreFuture, ValidInterval, Version, VertexId, VertexVersion,
};
use dtg_storage_fjall::{FjallConsensusStore, FjallReplicaStore};

#[test]
fn created_snapshot_manifest_binds_the_exact_applied_prefix_without_buffering_chunks() {
    assert_eq!(SUPPORTED_REPLICA_SNAPSHOT_FORMAT_VERSION, 1);
    let root = tempfile::tempdir().unwrap();
    let source_binding = binding(4, BindingRole::Active, "source");
    let snapshot = create_source_snapshot(root.path());
    let manifest: &ReplicaSnapshotManifest = snapshot.manifest();

    assert_eq!(manifest.format(), Version::new(1));
    assert_eq!(manifest.binding(), &source_binding);
    assert_eq!(manifest.last_included_term(), 1);
    assert_eq!(manifest.last_included_index(), 2);
}

#[test]
fn replica_snapshot_stream_applies_backpressure_one_chunk_at_a_time() {
    let root = tempfile::tempdir().unwrap();
    let source_binding = binding(4, BindingRole::Active, "bounded-source");
    let source_consensus = Arc::new(
        FjallConsensusStore::open(
            root.path().join("bounded-consensus"),
            source_binding.clone(),
        )
        .unwrap(),
    );
    block_on(source_consensus.set_membership(RaftMembership {
        voters: vec![source_binding.replica_id()],
        learners: vec![dtg_storage::ReplicaId::new(5).unwrap()],
        configuration_index: 0,
    }))
    .unwrap();
    let source = Arc::new(
        FjallReplicaStore::open(root.path().join("bounded-business"), source_binding.clone())
            .unwrap(),
    );
    let mut replica = RaftReplica::open(source_consensus.clone(), source.clone()).unwrap();
    replica.start().unwrap();
    replica.campaign().unwrap();
    replica.drive_ready().unwrap();
    replica.propose(vertex_command(10)).unwrap();
    replica.drive_ready().unwrap();
    let permit = replica.snapshot_read_permit(2).unwrap();
    let snapshot =
        create_replica_snapshot(source.as_ref(), source_consensus.as_ref(), &permit, 701, 1)
            .unwrap();
    assert_eq!(source.tck_snapshot_buffer_high_watermark(), 0);

    let candidate_binding = binding(5, BindingRole::Candidate, "bounded-target");
    let active_binding = binding(5, BindingRole::Active, "bounded-target");
    let candidate = FjallReplicaStore::open(
        root.path().join("bounded-target-business"),
        candidate_binding.clone(),
    )
    .unwrap();
    let target_consensus = FjallConsensusStore::open(
        root.path().join("bounded-target-consensus"),
        active_binding.clone(),
    )
    .unwrap();
    install_replica_snapshot(
        snapshot,
        &candidate,
        &candidate,
        &target_consensus,
        candidate_binding,
        active_binding,
    )
    .unwrap();

    assert_eq!(source.tck_snapshot_buffer_high_watermark(), 1);
}

#[test]
fn streamed_install_activates_only_after_the_manifest_is_verified() {
    let root = tempfile::tempdir().unwrap();
    let snapshot = create_source_snapshot(root.path());
    let candidate_binding = binding(5, BindingRole::Candidate, "target");
    let active_binding = binding(5, BindingRole::Active, "target");
    let candidate_path = root.path().join("target-business");
    let candidate =
        Arc::new(FjallReplicaStore::open(&candidate_path, candidate_binding.clone()).unwrap());
    let consensus = Arc::new(
        FjallConsensusStore::open(root.path().join("target-consensus"), active_binding.clone())
            .unwrap(),
    );
    let installed = install_replica_snapshot(
        snapshot,
        candidate.as_ref(),
        candidate.as_ref(),
        consensus.as_ref(),
        candidate_binding,
        active_binding.clone(),
    )
    .unwrap();
    assert_eq!(installed.state(), ReplicaSnapshotInstallState::Active);
    assert_eq!(installed.active_binding(), &active_binding);
    let retried = block_on(candidate.activate_candidate(
        installed.candidate_receipt().clone(),
        active_binding.clone(),
    ))
    .unwrap();
    assert_eq!(retried.active_binding(), &active_binding);
    assert!(block_on(candidate.applied_index()).is_err());

    let metadata = block_on(consensus.snapshot_metadata()).unwrap().unwrap();
    assert_eq!(metadata.last_included_index, 2);
    assert_eq!(block_on(consensus.hard_state()).unwrap().committed_index, 2);
    assert_eq!(block_on(consensus.membership()).unwrap().voters.len(), 1);

    drop(candidate);
    let active = Arc::new(FjallReplicaStore::open(candidate_path, active_binding).unwrap());
    assert_eq!(block_on(active.applied_index()).unwrap(), 2);
    let mut recovered = RaftReplica::open(consensus, active).unwrap();
    assert!(recovered.recover().unwrap().is_empty());
}

#[test]
fn interrupted_activation_stays_non_serving_and_full_install_is_retryable() {
    let root = tempfile::tempdir().unwrap();
    let snapshot = create_source_snapshot(root.path());
    let candidate_binding = binding(5, BindingRole::Candidate, "interrupted-target");
    let active_binding = binding(5, BindingRole::Active, "interrupted-target");
    let candidate = Arc::new(
        FjallReplicaStore::open(
            root.path().join("interrupted-business"),
            candidate_binding.clone(),
        )
        .unwrap(),
    );
    let consensus = Arc::new(
        FjallConsensusStore::open(
            root.path().join("interrupted-consensus"),
            active_binding.clone(),
        )
        .unwrap(),
    );
    let fail_once = FailOnceActivation {
        inner: Arc::clone(&candidate),
        failing: AtomicBool::new(true),
    };

    assert!(
        install_replica_snapshot(
            snapshot,
            candidate.as_ref(),
            &fail_once,
            consensus.as_ref(),
            candidate_binding.clone(),
            active_binding.clone(),
        )
        .is_err()
    );
    assert_eq!(block_on(candidate.applied_index()).unwrap(), 2);
    assert!(block_on(consensus.snapshot_install()).unwrap().is_some());
    assert!(block_on(consensus.snapshot_metadata()).unwrap().is_none());
    assert_eq!(
        block_on(consensus.hard_state()).unwrap(),
        RaftHardState::default()
    );
    assert!(RaftReplica::open(consensus.clone(), candidate.clone()).is_err());

    let installed = recover_replica_snapshot_install(candidate.as_ref(), consensus.as_ref())
        .unwrap()
        .unwrap();
    assert_eq!(installed.state(), ReplicaSnapshotInstallState::Active);
}

#[test]
fn startup_recovery_finishes_an_activation_whose_response_was_lost() {
    let root = tempfile::tempdir().unwrap();
    let snapshot = create_source_snapshot(root.path());
    let candidate_binding = binding(5, BindingRole::Candidate, "activation-loss-target");
    let active_binding = binding(5, BindingRole::Active, "activation-loss-target");
    let business_path = root.path().join("activation-loss-business");
    let candidate =
        Arc::new(FjallReplicaStore::open(&business_path, candidate_binding.clone()).unwrap());
    let consensus = Arc::new(
        FjallConsensusStore::open(
            root.path().join("activation-loss-consensus"),
            active_binding.clone(),
        )
        .unwrap(),
    );
    let activation = LoseOnceActivationResponse {
        inner: Arc::clone(&candidate),
        failing: AtomicBool::new(true),
    };

    assert!(
        install_replica_snapshot(
            snapshot,
            candidate.as_ref(),
            &activation,
            consensus.as_ref(),
            candidate_binding,
            active_binding.clone(),
        )
        .is_err()
    );
    assert!(block_on(consensus.snapshot_install()).unwrap().is_some());
    assert!(block_on(consensus.snapshot_metadata()).unwrap().is_none());
    drop(activation);
    drop(candidate);

    let active = Arc::new(FjallReplicaStore::open(&business_path, active_binding).unwrap());
    let recovered = recover_replica_snapshot_install(active.as_ref(), consensus.as_ref())
        .unwrap()
        .unwrap();
    assert_eq!(recovered.state(), ReplicaSnapshotInstallState::Active);
    assert!(block_on(consensus.snapshot_install()).unwrap().is_none());
    assert!(block_on(consensus.snapshot_metadata()).unwrap().is_some());
    assert!(RaftReplica::open(consensus, active).is_ok());
}

#[test]
fn recovery_replays_the_retained_wal_suffix_after_snapshot_activation() {
    let root = tempfile::tempdir().unwrap();
    let snapshot = create_source_snapshot(root.path());
    let candidate_binding = binding(5, BindingRole::Candidate, "suffix-target");
    let active_binding = binding(5, BindingRole::Active, "suffix-target");
    let candidate_path = root.path().join("suffix-business");
    let candidate =
        Arc::new(FjallReplicaStore::open(&candidate_path, candidate_binding.clone()).unwrap());
    let consensus = Arc::new(
        FjallConsensusStore::open(root.path().join("suffix-consensus"), active_binding.clone())
            .unwrap(),
    );
    block_on(consensus.append(vec![consensus_entry(vertex_command(11), 1, 3)])).unwrap();
    block_on(consensus.set_hard_state(RaftHardState {
        current_term: 1,
        voted_for: None,
        committed_index: 3,
    }))
    .unwrap();

    install_replica_snapshot(
        snapshot,
        candidate.as_ref(),
        candidate.as_ref(),
        consensus.as_ref(),
        candidate_binding,
        active_binding.clone(),
    )
    .unwrap();
    drop(candidate);

    let active = Arc::new(FjallReplicaStore::open(candidate_path, active_binding).unwrap());
    let mut recovered = RaftReplica::open(consensus, active.clone()).unwrap();
    let outcomes = recovered.recover().unwrap();

    assert_eq!(outcomes.len(), 1);
    assert_eq!(outcomes[0].applied_index(), 3);
    assert_eq!(block_on(active.applied_index()).unwrap(), 3);
}

#[test]
fn install_rejects_any_binding_change_beyond_candidate_to_active_role() {
    let root = tempfile::tempdir().unwrap();
    let snapshot = create_source_snapshot(root.path());
    let candidate_binding = binding(5, BindingRole::Candidate, "mismatch-target");
    let active_binding = binding(5, BindingRole::Active, "mismatch-target");
    let mismatched_active = active_binding
        .to_builder()
        .endpoint_profile_ref("different-endpoint")
        .build()
        .unwrap();
    let candidate = Arc::new(
        FjallReplicaStore::open(
            root.path().join("mismatch-business"),
            candidate_binding.clone(),
        )
        .unwrap(),
    );
    let consensus =
        FjallConsensusStore::open(root.path().join("mismatch-consensus"), active_binding).unwrap();
    assert!(
        install_replica_snapshot(
            snapshot,
            candidate.as_ref(),
            candidate.as_ref(),
            &consensus,
            candidate_binding,
            mismatched_active,
        )
        .is_err()
    );
    assert_eq!(candidate.binding().role(), BindingRole::Candidate);
    assert_eq!(block_on(candidate.applied_index()).unwrap(), 0);
}

#[test]
fn missing_retained_wal_suffix_is_rejected_before_candidate_restore() {
    let root = tempfile::tempdir().unwrap();
    let snapshot = create_source_snapshot(root.path());
    let candidate_binding = binding(5, BindingRole::Candidate, "missing-suffix-target");
    let active_binding = binding(5, BindingRole::Active, "missing-suffix-target");
    let candidate = Arc::new(
        FjallReplicaStore::open(
            root.path().join("missing-suffix-business"),
            candidate_binding.clone(),
        )
        .unwrap(),
    );
    let consensus = Arc::new(
        FjallConsensusStore::open(
            root.path().join("missing-suffix-consensus"),
            active_binding.clone(),
        )
        .unwrap(),
    );
    block_on(consensus.append(vec![consensus_entry(vertex_command(11), 1, 3)])).unwrap();
    block_on(consensus.set_hard_state(RaftHardState {
        current_term: 1,
        voted_for: None,
        committed_index: 4,
    }))
    .unwrap();

    assert!(
        install_replica_snapshot(
            snapshot,
            candidate.as_ref(),
            candidate.as_ref(),
            consensus.as_ref(),
            candidate_binding,
            active_binding,
        )
        .is_err()
    );
    assert_eq!(block_on(candidate.applied_index()).unwrap(), 0);
    assert!(block_on(consensus.snapshot_metadata()).unwrap().is_none());
}

#[test]
fn snapshot_target_must_be_a_voter_or_learner() {
    let root = tempfile::tempdir().unwrap();
    let snapshot = create_source_snapshot_with_membership(root.path(), vec![], 1);
    let candidate_binding = binding(5, BindingRole::Candidate, "non-member-target");
    let active_binding = binding(5, BindingRole::Active, "non-member-target");
    let candidate = Arc::new(
        FjallReplicaStore::open(
            root.path().join("non-member-business"),
            candidate_binding.clone(),
        )
        .unwrap(),
    );
    let consensus = Arc::new(
        FjallConsensusStore::open(
            root.path().join("non-member-consensus"),
            active_binding.clone(),
        )
        .unwrap(),
    );

    assert!(
        install_replica_snapshot(
            snapshot,
            candidate.as_ref(),
            candidate.as_ref(),
            consensus.as_ref(),
            candidate_binding,
            active_binding,
        )
        .is_err()
    );
    assert_eq!(block_on(candidate.applied_index()).unwrap(), 0);
    assert!(block_on(consensus.snapshot_metadata()).unwrap().is_none());
}

#[test]
fn snapshot_term_increase_clears_a_vote_from_the_old_term() {
    let root = tempfile::tempdir().unwrap();
    let snapshot = create_source_snapshot_with_membership(
        root.path(),
        vec![dtg_storage::ReplicaId::new(5).unwrap()],
        2,
    );
    let candidate_binding = binding(5, BindingRole::Candidate, "term-vote-target");
    let active_binding = binding(5, BindingRole::Active, "term-vote-target");
    let candidate = Arc::new(
        FjallReplicaStore::open(
            root.path().join("term-vote-business"),
            candidate_binding.clone(),
        )
        .unwrap(),
    );
    let consensus = Arc::new(
        FjallConsensusStore::open(
            root.path().join("term-vote-consensus"),
            active_binding.clone(),
        )
        .unwrap(),
    );
    block_on(consensus.set_hard_state(RaftHardState {
        current_term: 1,
        voted_for: Some(dtg_storage::ReplicaId::new(9).unwrap()),
        committed_index: 0,
    }))
    .unwrap();

    install_replica_snapshot(
        snapshot,
        candidate.as_ref(),
        candidate.as_ref(),
        consensus.as_ref(),
        candidate_binding,
        active_binding,
    )
    .unwrap();

    let installed = block_on(consensus.hard_state()).unwrap();
    assert_eq!(installed.current_term, 2);
    assert_eq!(installed.voted_for, None);
}

struct FailOnceActivation {
    inner: Arc<FjallReplicaStore>,
    failing: AtomicBool,
}

struct LoseOnceActivationResponse {
    inner: Arc<FjallReplicaStore>,
    failing: AtomicBool,
}

impl LogicalReplicaActivation for LoseOnceActivationResponse {
    fn activate_candidate(
        &self,
        candidate: LogicalSnapshotCandidateReceipt,
        active_binding: ReplicaBinding,
    ) -> StoreFuture<'_, LogicalReplicaActivationReceipt> {
        Box::pin(async move {
            let receipt = self
                .inner
                .activate_candidate(candidate, active_binding)
                .await?;
            if self.failing.swap(false, Ordering::SeqCst) {
                return Err(StorageError::Internal(
                    "injected activation response loss".into(),
                ));
            }
            Ok(receipt)
        })
    }
}

impl LogicalReplicaActivation for FailOnceActivation {
    fn activate_candidate(
        &self,
        candidate: LogicalSnapshotCandidateReceipt,
        active_binding: ReplicaBinding,
    ) -> StoreFuture<'_, LogicalReplicaActivationReceipt> {
        Box::pin(async move {
            if self.failing.swap(false, Ordering::SeqCst) {
                return Err(StorageError::Internal(
                    "injected activation interruption".into(),
                ));
            }
            self.inner
                .activate_candidate(candidate, active_binding)
                .await
        })
    }
}

fn create_source_snapshot(root: &std::path::Path) -> ReplicaSnapshot {
    create_source_snapshot_with_membership(root, vec![dtg_storage::ReplicaId::new(5).unwrap()], 1)
}

fn create_source_snapshot_with_membership(
    root: &std::path::Path,
    learners: Vec<dtg_storage::ReplicaId>,
    current_term: u64,
) -> ReplicaSnapshot {
    let source_binding = binding(4, BindingRole::Active, "source");
    let consensus = Arc::new(
        FjallConsensusStore::open(root.join("consensus"), source_binding.clone()).unwrap(),
    );
    block_on(consensus.set_membership(RaftMembership {
        voters: vec![source_binding.replica_id()],
        learners,
        configuration_index: 0,
    }))
    .unwrap();
    let state =
        Arc::new(FjallReplicaStore::open(root.join("business"), source_binding.clone()).unwrap());
    let mut replica = RaftReplica::open(consensus.clone(), state.clone()).unwrap();
    replica.start().unwrap();
    replica.campaign().unwrap();
    replica.drive_ready().unwrap();
    replica.propose(vertex_command(10)).unwrap();
    replica.drive_ready().unwrap();
    if current_term > 1 {
        block_on(consensus.set_hard_state(RaftHardState {
            current_term,
            voted_for: Some(source_binding.replica_id()),
            committed_index: 2,
        }))
        .unwrap();
    }
    let permit = replica.snapshot_read_permit(2).unwrap();

    create_replica_snapshot(state.as_ref(), consensus.as_ref(), &permit, 77, 1).unwrap()
}

#[test]
fn candidate_binding_cannot_open_a_serving_raft_replica() {
    let root = tempfile::tempdir().unwrap();
    let candidate = binding(5, BindingRole::Candidate, "candidate-non-serving");
    let consensus = Arc::new(
        FjallConsensusStore::open(root.path().join("consensus"), candidate.clone()).unwrap(),
    );
    let state = Arc::new(FjallReplicaStore::open(root.path().join("business"), candidate).unwrap());

    assert!(RaftReplica::open(consensus, state).is_err());
}

fn vertex_command(command_id: u128) -> ShardCommand {
    let vertex = VertexVersion::new(
        VertexId::new(command_id).unwrap(),
        Version::new(1),
        ValidInterval::new(0, 100).unwrap(),
        dtg_storage::TransactionTime::new(10).unwrap(),
        Properties::new(),
    )
    .unwrap();
    ShardCommand::CommitSingleShard(
        CommitSingleShard::new(
            CommandId::new(command_id).unwrap(),
            7,
            8,
            vec![LogicalMutation::PutVertex(vertex)],
        )
        .unwrap(),
    )
}

fn consensus_entry(command: ShardCommand, term: u64, index: u64) -> ConsensusEntry {
    let command_id = command.header().command_id();
    let context = command_id.get().to_be_bytes();
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
        command_id,
        ConsensusCommandEnvelope::new(SUPPORTED_CONSENSUS_COMMAND_FORMAT_VERSION, payload).unwrap(),
    )
    .unwrap()
}

fn binding(replica_id: u64, role: BindingRole, namespace: &str) -> ReplicaBinding {
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
        .replica_id(replica_id)
        .backend_generation(8)
        .backend_class_digest(class.digest())
        .provider_kind(ProviderKind::Fjall)
        .contract_version(1)
        .layout_version(1)
        .capability_digest(capabilities.digest())
        .namespace_id(namespace)
        .endpoint_profile_ref("local")
        .credential_ref("none")
        .role(role)
        .build()
        .unwrap()
}

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    struct ThreadWaker(std::thread::Thread);

    impl std::task::Wake for ThreadWaker {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.unpark();
        }
    }

    let waker = std::task::Waker::from(Arc::new(ThreadWaker(std::thread::current())));
    let mut context = std::task::Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            std::task::Poll::Ready(output) => return output,
            std::task::Poll::Pending => std::thread::park(),
        }
    }
}
