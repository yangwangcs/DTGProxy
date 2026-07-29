use std::collections::BTreeMap;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};

use dtg_shard::{
    AdvanceClosedTimestamp, CommitSingleShard, RaftReplica, ShardCommand, ShardHost,
    ShardStateMachine,
};
use dtg_storage::{
    ApplyReceipt, BackendClass, BindingRole, CapabilityManifest, CommandId, CommittedShardBatch,
    ConsensusSnapshotMetadata, ConsensusStore, Digest32, LogicalMutation, Properties, ProviderKind,
    RaftHardState, RaftMembership, ReadFence, ReplicaBinding, ReplicaId, ReplicaStateStore,
    StorageError, StoreFuture, TemporalReadView, TransactionTime, ValidInterval, Version, VertexId,
    VertexVersion,
};
use dtg_storage_fjall::{FjallConsensusStore, FjallReplicaStore};

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
    let consensus =
        Arc::new(FjallConsensusStore::open(dir.path().join("consensus"), binding.clone()).unwrap());
    initialize_membership(&*consensus, binding.replica_id());
    let state =
        Arc::new(FjallReplicaStore::open(dir.path().join("business"), binding.clone()).unwrap());
    {
        let failing = Arc::new(ToggleStateStore::new(state.clone()));
        let mut replica = RaftReplica::open(consensus.clone(), failing.clone()).unwrap();
        replica.start().unwrap();
        replica.campaign().unwrap();
        replica.drive_ready().unwrap();
        failing.set_failing(true);
        replica.propose(committed_vertex_command(1, 7, 10)).unwrap();
        assert!(replica.drive_ready().is_err());
    }

    let mut replica = RaftReplica::open(consensus, state.clone()).unwrap();
    let recovered = replica.recover().unwrap();

    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].applied_index(), 2);
    assert_eq!(block_on(state.applied_index()).unwrap(), 2);
}

#[test]
fn closed_timestamp_is_rebuilt_from_committed_wal_on_restart() {
    let dir = tempfile::tempdir().unwrap();
    let binding = fixture_binding(14, 15, ProviderKind::Fjall, "closed-restart");
    let consensus =
        Arc::new(FjallConsensusStore::open(dir.path().join("consensus"), binding.clone()).unwrap());
    initialize_membership(&*consensus, binding.replica_id());
    let state =
        Arc::new(FjallReplicaStore::open(dir.path().join("business"), binding.clone()).unwrap());
    {
        let mut replica = RaftReplica::open(consensus.clone(), state.clone()).unwrap();
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
fn stale_epoch_and_generation_are_fenced_before_state_store_apply() {
    let binding = fixture_binding(3, 4, ProviderKind::Fjall, "fences");
    let state = Arc::new(RecordingStateStore::with_applied(binding.clone(), 0));
    let mut machine = ShardStateMachine::new(binding, state.clone()).unwrap();

    assert_eq!(
        machine
            .apply_committed(1, 1, committed_vertex_command(1, 6, 10))
            .unwrap_err()
            .code(),
        "DTG-SHARD-STALE-EPOCH"
    );
    assert_eq!(
        machine
            .apply_committed(1, 1, committed_vertex_command(1, 7, 9))
            .unwrap_err()
            .code(),
        "DTG-SHARD-STALE-GENERATION"
    );
    assert_eq!(state.apply_calls(), 0);
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
    encoded[..4].copy_from_slice(&2_u32.to_be_bytes());
    let error = ShardCommand::decode(&encoded).unwrap_err();
    assert_eq!(error.code(), "DTG-SHARD-COMMAND-VERSION");
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

    fn begin_read_view(&self, _fence: ReadFence) -> StoreFuture<'_, Box<dyn TemporalReadView>> {
        Box::pin(async { Err(StorageError::Unsupported) })
    }
}
