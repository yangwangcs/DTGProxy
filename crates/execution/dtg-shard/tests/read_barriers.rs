use dtg_shard::{
    AdvanceClosedTimestamp, FollowerReadProofAuthority, RaftReplica,
    SUPPORTED_FOLLOWER_READ_PROOF_VERSION, ShardCommand,
};
use dtg_storage::{
    ApplyReceipt, BackendClass, BindingRole, CapabilityManifest, CommandId, CommittedShardBatch,
    ConsensusStore, ProviderKind, RaftMembership, ReadFence, ReplicaBinding, ReplicaId,
    ReplicaMetadata, ReplicaStateStore, StoreFuture, TemporalReadView, TransactionTime,
};
use dtg_storage_fjall::{FjallConsensusStore, FjallReplicaStore};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

#[test]
fn historical_read_still_requires_authenticated_current_generation_proof() {
    assert!(FollowerReadProofAuthority::new([0; 32]).is_err());
    let authority = FollowerReadProofAuthority::new([7; 32]).unwrap();
    assert_eq!(SUPPORTED_FOLLOWER_READ_PROOF_VERSION, 1);

    let root = tempfile::tempdir().unwrap();
    let leader_binding = binding(4, 8, "leader");
    let follower_binding = binding(5, 9, "follower");
    let proof = authority
        .issue(
            leader_binding,
            ReplicaId::new(4).unwrap(),
            12,
            100,
            TransactionTime::new(90).unwrap(),
        )
        .unwrap();
    let consensus = Arc::new(
        FjallConsensusStore::open(root.path().join("consensus"), follower_binding.clone()).unwrap(),
    );
    let state =
        Arc::new(FjallReplicaStore::open(root.path().join("business"), follower_binding).unwrap());
    let follower = RaftReplica::open(consensus, state).unwrap();

    assert_eq!(
        follower
            .follower_read_permit(&authority, &proof, TransactionTime::new(40).unwrap(),)
            .unwrap_err()
            .code(),
        "DTG-SHARD-READ-STALE-GENERATION"
    );
}

#[test]
fn follower_read_rejects_a_proof_from_the_wrong_mac_key() {
    let issuer = FollowerReadProofAuthority::new([7; 32]).unwrap();
    let verifier = FollowerReadProofAuthority::new([8; 32]).unwrap();
    let proof = issuer
        .issue(
            binding(4, 8, "wrong-key-leader"),
            ReplicaId::new(4).unwrap(),
            12,
            100,
            TransactionTime::new(90).unwrap(),
        )
        .unwrap();
    let root = tempfile::tempdir().unwrap();
    let follower_binding = binding(5, 8, "wrong-key-follower");
    let consensus = Arc::new(
        FjallConsensusStore::open(root.path().join("consensus"), follower_binding.clone()).unwrap(),
    );
    let state =
        Arc::new(FjallReplicaStore::open(root.path().join("business"), follower_binding).unwrap());
    let follower = RaftReplica::open(consensus, state).unwrap();

    assert_eq!(
        follower
            .follower_read_permit(&verifier, &proof, TransactionTime::new(40).unwrap())
            .unwrap_err()
            .code(),
        "DTG-SHARD-READ-PROOF-SIGNATURE"
    );
}

#[test]
fn follower_read_rejects_a_stale_placement_epoch() {
    let authority = FollowerReadProofAuthority::new([7; 32]).unwrap();
    let proof = authority
        .issue(
            binding_at(4, 7, 8, "stale-epoch-leader"),
            ReplicaId::new(4).unwrap(),
            12,
            100,
            TransactionTime::new(90).unwrap(),
        )
        .unwrap();
    let root = tempfile::tempdir().unwrap();
    let follower_binding = binding_at(5, 8, 8, "stale-epoch-follower");
    let consensus = Arc::new(
        FjallConsensusStore::open(root.path().join("consensus"), follower_binding.clone()).unwrap(),
    );
    let state =
        Arc::new(FjallReplicaStore::open(root.path().join("business"), follower_binding).unwrap());
    let follower = RaftReplica::open(consensus, state).unwrap();

    assert_eq!(
        follower
            .follower_read_permit(&authority, &proof, TransactionTime::new(40).unwrap())
            .unwrap_err()
            .code(),
        "DTG-SHARD-READ-STALE-EPOCH"
    );
}

#[test]
fn follower_read_rejects_a_requested_time_above_the_proof_closed_timestamp() {
    let root = tempfile::tempdir().unwrap();
    let mut replicas = open_three(root.path());
    replicas.get_mut(&1).unwrap().campaign().unwrap();
    pump(&mut replicas);
    replicas
        .get_mut(&1)
        .unwrap()
        .propose(ShardCommand::AdvanceClosedTimestamp(
            AdvanceClosedTimestamp::new(
                CommandId::new(906).unwrap(),
                7,
                8,
                TransactionTime::new(90).unwrap(),
            )
            .unwrap(),
        ))
        .unwrap();
    pump(&mut replicas);
    replicas
        .get_mut(&1)
        .unwrap()
        .request_linearizable_read(906)
        .unwrap();
    let permits = pump(&mut replicas);
    let permit = permits
        .iter()
        .find(|permit| permit.request_id() == Some(906))
        .unwrap();
    let authority = FollowerReadProofAuthority::new([7; 32]).unwrap();
    let proof = authority
        .issue(
            replicas[&1].binding().clone(),
            ReplicaId::new(1).unwrap(),
            permit.leader_term(),
            permit.fence().applied_index(),
            TransactionTime::new(90).unwrap(),
        )
        .unwrap();

    assert_eq!(
        replicas[&2]
            .follower_read_permit(&authority, &proof, TransactionTime::new(91).unwrap())
            .unwrap_err()
            .code(),
        "DTG-SHARD-READ-UNSAFE-FOLLOWER"
    );
}

#[test]
fn follower_read_rejects_a_proof_from_a_stale_leader_term() {
    let root = tempfile::tempdir().unwrap();
    let mut replicas = open_three(root.path());
    replicas.get_mut(&1).unwrap().campaign().unwrap();
    pump(&mut replicas);
    replicas
        .get_mut(&1)
        .unwrap()
        .propose(ShardCommand::AdvanceClosedTimestamp(
            AdvanceClosedTimestamp::new(
                CommandId::new(907).unwrap(),
                7,
                8,
                TransactionTime::new(90).unwrap(),
            )
            .unwrap(),
        ))
        .unwrap();
    pump(&mut replicas);
    replicas
        .get_mut(&1)
        .unwrap()
        .request_linearizable_read(907)
        .unwrap();
    let permits = pump(&mut replicas);
    let permit = permits
        .iter()
        .find(|permit| permit.request_id() == Some(907))
        .unwrap();
    let authority = FollowerReadProofAuthority::new([7; 32]).unwrap();
    let proof = authority
        .issue(
            replicas[&1].binding().clone(),
            ReplicaId::new(1).unwrap(),
            permit.leader_term(),
            permit.fence().applied_index(),
            TransactionTime::new(90).unwrap(),
        )
        .unwrap();

    replicas.get_mut(&2).unwrap().campaign().unwrap();
    pump(&mut replicas);

    assert_eq!(
        replicas[&3]
            .follower_read_permit(&authority, &proof, TransactionTime::new(40).unwrap())
            .unwrap_err()
            .code(),
        "DTG-SHARD-READ-NOT-READY"
    );
}

#[test]
fn follower_read_rejects_a_replica_below_the_proof_applied_index() {
    let root = tempfile::tempdir().unwrap();
    let mut replicas = open_three(root.path());
    replicas.get_mut(&1).unwrap().campaign().unwrap();
    pump(&mut replicas);
    replicas
        .get_mut(&1)
        .unwrap()
        .propose(ShardCommand::AdvanceClosedTimestamp(
            AdvanceClosedTimestamp::new(
                CommandId::new(908).unwrap(),
                7,
                8,
                TransactionTime::new(90).unwrap(),
            )
            .unwrap(),
        ))
        .unwrap();
    pump_excluding_target(&mut replicas, 3);
    replicas
        .get_mut(&1)
        .unwrap()
        .request_linearizable_read(908)
        .unwrap();
    let permits = pump_excluding_target(&mut replicas, 3);
    let permit = permits
        .iter()
        .find(|permit| permit.request_id() == Some(908))
        .unwrap();
    let authority = FollowerReadProofAuthority::new([7; 32]).unwrap();
    let proof = authority
        .issue(
            replicas[&1].binding().clone(),
            ReplicaId::new(1).unwrap(),
            permit.leader_term(),
            permit.fence().applied_index(),
            TransactionTime::new(90).unwrap(),
        )
        .unwrap();

    assert_eq!(
        replicas[&3]
            .follower_read_permit(&authority, &proof, TransactionTime::new(40).unwrap())
            .unwrap_err()
            .code(),
        "DTG-SHARD-READ-ADAPTER-LAG"
    );
}

#[test]
fn leader_read_index_requires_current_term_commit_and_correlates_ready_state() {
    let root = tempfile::tempdir().unwrap();
    let replica_binding = binding(4, 8, "linearizable");
    let consensus = Arc::new(
        FjallConsensusStore::open(root.path().join("consensus"), replica_binding.clone()).unwrap(),
    );
    block_on(consensus.set_membership(RaftMembership {
        voters: vec![replica_binding.replica_id()],
        learners: vec![],
        configuration_index: 0,
    }))
    .unwrap();
    let state =
        Arc::new(FjallReplicaStore::open(root.path().join("business"), replica_binding).unwrap());
    let mut replica = RaftReplica::open(consensus, state).unwrap();
    replica.start().unwrap();
    replica.campaign().unwrap();

    assert_eq!(
        replica.request_linearizable_read(900).unwrap_err().code(),
        "DTG-SHARD-READ-NOT-READY"
    );
    replica.drive_ready().unwrap();
    replica.request_linearizable_read(900).unwrap();
    let progress = replica.drive_ready().unwrap();
    let permit = &progress.read_permits()[0];

    assert_eq!(permit.request_id(), Some(900));
    assert_eq!(permit.mode(), dtg_shard::ReadMode::Linearizable);
    assert!(permit.fence().applied_index() > 0);
}

#[test]
fn leader_read_index_waits_for_a_real_voter_quorum() {
    let root = tempfile::tempdir().unwrap();
    let mut replicas = open_three(root.path());
    replicas.get_mut(&1).unwrap().campaign().unwrap();
    pump(&mut replicas);

    replicas
        .get_mut(&1)
        .unwrap()
        .request_linearizable_read(901)
        .unwrap();
    let mut leader_progress = replicas.get_mut(&1).unwrap().drive_ready().unwrap();
    assert!(leader_progress.read_permits().is_empty());

    for message in leader_progress.take_messages() {
        replicas
            .get_mut(&message.to)
            .unwrap()
            .step(message)
            .unwrap();
    }
    let permits = pump(&mut replicas);

    assert!(
        permits
            .iter()
            .any(|permit| permit.request_id() == Some(901))
    );
}

#[test]
fn leadership_change_cancels_an_in_flight_read_index() {
    let root = tempfile::tempdir().unwrap();
    let mut replicas = open_three(root.path());
    replicas.get_mut(&1).unwrap().campaign().unwrap();
    pump(&mut replicas);

    replicas
        .get_mut(&1)
        .unwrap()
        .request_linearizable_read(902)
        .unwrap();
    let pending = replicas.get_mut(&1).unwrap().drive_ready().unwrap();
    assert!(pending.read_permits().is_empty());
    drop(pending);

    replicas.get_mut(&2).unwrap().campaign().unwrap();
    let failures = pump_failures(&mut replicas);

    assert!(
        failures.iter().any(|(request_id, code)| {
            *request_id == 902 && *code == "DTG-SHARD-READ-NOT-LEADER"
        })
    );
}

#[test]
fn read_index_permit_waits_until_the_local_state_machine_applies_the_fence() {
    let root = tempfile::tempdir().unwrap();
    let replica_binding = binding(4, 8, "apply-lag");
    let consensus = Arc::new(
        FjallConsensusStore::open(root.path().join("consensus"), replica_binding.clone()).unwrap(),
    );
    block_on(consensus.set_membership(RaftMembership {
        voters: vec![replica_binding.replica_id()],
        learners: vec![],
        configuration_index: 0,
    }))
    .unwrap();
    let inner =
        Arc::new(FjallReplicaStore::open(root.path().join("business"), replica_binding).unwrap());
    let state = Arc::new(ToggleStateStore::new(inner));
    let mut replica = RaftReplica::open(consensus, state.clone()).unwrap();
    replica.start().unwrap();
    replica.campaign().unwrap();
    replica.drive_ready().unwrap();

    state.set_failing(true);
    replica
        .propose(ShardCommand::AdvanceClosedTimestamp(
            AdvanceClosedTimestamp::new(
                CommandId::new(903).unwrap(),
                7,
                8,
                TransactionTime::new(50).unwrap(),
            )
            .unwrap(),
        ))
        .unwrap();
    assert!(replica.drive_ready().is_err());
    replica.request_linearizable_read(903).unwrap();
    assert!(replica.drive_ready().is_err());

    state.set_failing(false);
    let progress = replica.drive_ready().unwrap();
    let permit = progress
        .read_permits()
        .iter()
        .find(|permit| permit.request_id() == Some(903))
        .unwrap();
    assert_eq!(permit.fence().applied_index(), 2);
}

#[test]
fn pending_read_index_request_ids_are_unique() {
    let root = tempfile::tempdir().unwrap();
    let mut replica = open_single(root.path(), "duplicate");
    replica.campaign().unwrap();
    replica.drive_ready().unwrap();

    replica.request_linearizable_read(904).unwrap();
    assert_eq!(
        replica.request_linearizable_read(904).unwrap_err().code(),
        "DTG-SHARD-READ-DUPLICATE"
    );
}

#[test]
fn linearizable_read_request_id_must_be_nonzero() {
    let root = tempfile::tempdir().unwrap();
    let mut replica = open_single(root.path(), "zero-request");
    replica.campaign().unwrap();
    replica.drive_ready().unwrap();

    assert_eq!(
        replica.request_linearizable_read(0).unwrap_err().code(),
        "DTG-SHARD-READ-REQUEST"
    );
}

#[test]
fn pending_read_index_requests_are_bounded_at_1024() {
    let root = tempfile::tempdir().unwrap();
    let mut replica = open_single(root.path(), "bounded");
    replica.campaign().unwrap();
    replica.drive_ready().unwrap();

    for request_id in 1..=1_024 {
        replica.request_linearizable_read(request_id).unwrap();
    }
    assert_eq!(
        replica.request_linearizable_read(1_025).unwrap_err().code(),
        "DTG-SHARD-READ-OVERLOAD"
    );
}

#[test]
fn cancellation_consumes_late_read_state_without_resurrecting_the_request() {
    let root = tempfile::tempdir().unwrap();
    let mut replica = open_single(root.path(), "cancelled");
    replica.campaign().unwrap();
    replica.drive_ready().unwrap();

    replica.request_linearizable_read(905).unwrap();
    assert!(replica.cancel_linearizable_read(905));
    assert_eq!(
        replica.request_linearizable_read(905).unwrap_err().code(),
        "DTG-SHARD-READ-DUPLICATE"
    );

    let cancelled = replica.drive_ready().unwrap();
    assert!(cancelled.read_permits().is_empty());
    assert!(cancelled.read_failures().is_empty());

    replica.request_linearizable_read(905).unwrap();
    let completed = replica.drive_ready().unwrap();
    assert!(
        completed
            .read_permits()
            .iter()
            .any(|permit| permit.request_id() == Some(905))
    );
}

#[test]
fn snapshot_read_permit_pins_one_exact_applied_prefix() {
    let root = tempfile::tempdir().unwrap();
    let mut replica = open_single(root.path(), "snapshot-prefix");
    replica.campaign().unwrap();
    replica.drive_ready().unwrap();

    assert_eq!(
        replica.snapshot_read_permit(2).unwrap_err().code(),
        "DTG-SHARD-READ-ADAPTER-LAG"
    );
    let permit = replica.snapshot_read_permit(1).unwrap();
    assert_eq!(permit.mode(), dtg_shard::ReadMode::Snapshot);
    assert_eq!(permit.fence().applied_index(), 1);

    replica
        .propose(ShardCommand::AdvanceClosedTimestamp(
            AdvanceClosedTimestamp::new(
                CommandId::new(909).unwrap(),
                7,
                8,
                TransactionTime::new(90).unwrap(),
            )
            .unwrap(),
        ))
        .unwrap();
    replica.drive_ready().unwrap();
    assert_eq!(
        replica.snapshot_read_permit(1).unwrap_err().code(),
        "DTG-SHARD-READ-SNAPSHOT-OLD"
    );
}

fn open_single(root: &std::path::Path, namespace: &str) -> RaftReplica {
    let replica_binding = binding(4, 8, namespace);
    let consensus = Arc::new(
        FjallConsensusStore::open(root.join("consensus"), replica_binding.clone()).unwrap(),
    );
    block_on(consensus.set_membership(RaftMembership {
        voters: vec![replica_binding.replica_id()],
        learners: vec![],
        configuration_index: 0,
    }))
    .unwrap();
    let state = Arc::new(FjallReplicaStore::open(root.join("business"), replica_binding).unwrap());
    let mut replica = RaftReplica::open(consensus, state).unwrap();
    replica.start().unwrap();
    replica
}

struct ToggleStateStore<S> {
    inner: Arc<S>,
    failing: AtomicBool,
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
                return Err(dtg_storage::StorageError::Internal(
                    "injected apply lag".into(),
                ));
            }
            self.inner.apply(batch).await
        })
    }

    fn begin_read_view(&self, fence: ReadFence) -> StoreFuture<'_, Box<dyn TemporalReadView>> {
        self.inner.begin_read_view(fence)
    }
}

fn open_three(root: &std::path::Path) -> BTreeMap<u64, RaftReplica> {
    let voters = [1_u64, 2, 3]
        .into_iter()
        .map(|replica_id| ReplicaId::new(replica_id).unwrap())
        .collect::<Vec<_>>();
    [1_u64, 2, 3]
        .into_iter()
        .map(|replica_id| {
            let namespace = format!("quorum-{replica_id}");
            let replica_binding = binding(replica_id, 8, &namespace);
            let consensus = Arc::new(
                FjallConsensusStore::open(
                    root.join(format!("consensus-{replica_id}")),
                    replica_binding.clone(),
                )
                .unwrap(),
            );
            block_on(consensus.set_membership(RaftMembership {
                voters: voters.clone(),
                learners: vec![],
                configuration_index: 0,
            }))
            .unwrap();
            let state = Arc::new(
                FjallReplicaStore::open(
                    root.join(format!("business-{replica_id}")),
                    replica_binding,
                )
                .unwrap(),
            );
            let mut replica = RaftReplica::open(consensus, state).unwrap();
            replica.start().unwrap();
            (replica_id, replica)
        })
        .collect()
}

fn pump(replicas: &mut BTreeMap<u64, RaftReplica>) -> Vec<dtg_shard::ReadPermit> {
    let mut permits = Vec::new();
    for _ in 0..512 {
        let replica_ids = replicas.keys().copied().collect::<Vec<_>>();
        let mut messages = Vec::new();
        for replica_id in replica_ids {
            let mut progress = replicas
                .get_mut(&replica_id)
                .unwrap()
                .drive_ready()
                .unwrap();
            permits.extend(progress.read_permits().iter().cloned());
            messages.extend(progress.take_messages());
        }
        if messages.is_empty() {
            return permits;
        }
        for message in messages {
            replicas
                .get_mut(&message.to)
                .unwrap()
                .step(message)
                .unwrap();
        }
    }
    panic!("Raft cluster did not quiesce");
}

fn pump_failures(replicas: &mut BTreeMap<u64, RaftReplica>) -> Vec<(u128, &'static str)> {
    let mut failures = Vec::new();
    for _ in 0..512 {
        let replica_ids = replicas.keys().copied().collect::<Vec<_>>();
        let mut messages = Vec::new();
        for replica_id in replica_ids {
            let mut progress = replicas
                .get_mut(&replica_id)
                .unwrap()
                .drive_ready()
                .unwrap();
            failures.extend(
                progress
                    .read_failures()
                    .iter()
                    .map(|failure| (failure.request_id(), failure.error().code())),
            );
            messages.extend(progress.take_messages());
        }
        if messages.is_empty() {
            return failures;
        }
        for message in messages {
            replicas
                .get_mut(&message.to)
                .unwrap()
                .step(message)
                .unwrap();
        }
    }
    panic!("Raft cluster did not quiesce");
}

fn pump_excluding_target(
    replicas: &mut BTreeMap<u64, RaftReplica>,
    excluded_target: u64,
) -> Vec<dtg_shard::ReadPermit> {
    let mut permits = Vec::new();
    for _ in 0..512 {
        let replica_ids = replicas.keys().copied().collect::<Vec<_>>();
        let mut messages = Vec::new();
        for replica_id in replica_ids {
            let mut progress = replicas
                .get_mut(&replica_id)
                .unwrap()
                .drive_ready()
                .unwrap();
            permits.extend(progress.read_permits().iter().cloned());
            messages.extend(
                progress
                    .take_messages()
                    .into_iter()
                    .filter(|message| message.to != excluded_target),
            );
        }
        if messages.is_empty() {
            return permits;
        }
        for message in messages {
            replicas
                .get_mut(&message.to)
                .unwrap()
                .step(message)
                .unwrap();
        }
    }
    panic!("Raft cluster did not quiesce");
}

fn binding(replica_id: u64, generation: u64, namespace: &str) -> ReplicaBinding {
    binding_at(replica_id, 7, generation, namespace)
}

fn binding_at(
    replica_id: u64,
    placement_epoch: u64,
    generation: u64,
    namespace: &str,
) -> ReplicaBinding {
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
        .placement_epoch(placement_epoch)
        .replica_id(replica_id)
        .backend_generation(generation)
        .backend_class_digest(class.digest())
        .provider_kind(ProviderKind::Fjall)
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
