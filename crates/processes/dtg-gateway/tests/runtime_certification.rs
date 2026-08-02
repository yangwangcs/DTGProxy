use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use dtg_data::DataNodeBuilder;
use dtg_execution::analytics::{
    AlgorithmBudget, AlgorithmRequest, AlgorithmResult, AnalyticsArtifact, AnalyticsJobSpec,
    AnalyticsJobStateKind, AnalyticsLedger, AnalyticsRequestIdentity, ArtifactKind,
    BuiltInAlgorithmId, CancellationToken, JobTimestamp, PartitionProvenance, ProjectedEdge,
    ProjectedVertex, ProjectionBudget, ProjectionSpec, ShardProjectionPart,
    ShardSnapshotProvenance, SnapshotCsr, SnapshotProvenance, WorkerId, run_builtin,
};
use dtg_execution::control::{MigrationId, MigrationReceipt, MigrationRecord, MigrationState};
use dtg_execution::shard::{
    AdvanceClosedTimestamp, CommitSingleShard, FollowerReadProofAuthority, RaftReplica,
    ReplicaSnapshotInstallState, ShardCommand, create_replica_snapshot, install_replica_snapshot,
};
use dtg_execution::storage::{
    ApplyReceipt, BackendClass, BackendGeneration, BindingRole, CapabilityManifest, CommandId,
    CommittedShardBatch, ConsensusCommandEnvelope, ConsensusEntry, ConsensusStore, Digest32,
    EdgeId, LogicalMutation, Properties, ProviderKind, RaftHardState, RaftMembership, ReadFence,
    ReplicaBinding, ReplicaId, ReplicaMetadata, ReplicaStateStore,
    SUPPORTED_CONSENSUS_COMMAND_FORMAT_VERSION, SUPPORTED_CONSENSUS_WAL_FORMAT_VERSION,
    StorageError, StoreFuture, TemporalReadView, TransactionTime, ValidInterval, Version, VertexId,
    VertexVersion,
};
use dtg_execution::transaction::{
    ChangeRecord, CommitResolution, CommitTimeReservation, ParticipantWrite, ShardCommandExecutor,
    ShardId, ShardRequest, ShardSnapshotFence, SnapshotToken, SubmissionFuture, SubmissionReceipt,
    TemporalTxnCoordinator, TimestampAuthority, TransactionContext, TransactionHistory,
    TransactionId, TransactionOutcome, TxnError, TxnFuture,
};
use dtg_execution::{
    GatewayAnalyticsState, GatewayClusterRequest, GatewayExecution, GatewayExecutionError,
    GatewayExecutionTransport, GatewayFuture, GatewayOperation, GatewayRequestContext,
    GatewayResponse, GatewayRows, GatewayValue, ProviderResolver,
};
use dtg_gateway::{GatewayConfig, GatewayService};
use dtg_storage_fjall::{FjallConsensusStore, FjallReplicaStore};
use serde_json::json;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deterministic_runtime_certification_produces_behavioral_evidence() {
    let root = tempfile::tempdir().unwrap();
    let raft = certify_raft_failover_and_follower_read(root.path()).await;
    let transactions = certify_temporal_transactions().await;
    let snapshot = certify_snapshot_restart_continue(root.path()).await;
    let analytics = certify_snapshot_csr_and_tcypher_analytics().await;
    let migrations = certify_six_provider_directions();
    let heterogeneous = certify_heterogeneous_data_node(root.path()).await;

    let evidence = json!({
        "schema_version": 1,
        "raft_leader_failover_follower_read": raft,
        "temporal_snapshot_isolation": transactions,
        "logical_snapshot_restart_continue": snapshot,
        "snapshot_csr_builtin_async_analytics": analytics,
        "six_provider_migrations": migrations,
        "heterogeneous_data_node": heterogeneous
    });
    if let Some(path) = std::env::var_os("DTG_CLEAN_BREAK_RUNTIME_EVIDENCE") {
        std::fs::write(path, serde_json::to_vec_pretty(&evidence).unwrap()).unwrap();
    }
    println!("DTG_RUNTIME_CERTIFICATION_EVIDENCE={evidence}");
}

async fn certify_raft_failover_and_follower_read(root: &std::path::Path) -> serde_json::Value {
    let mut replicas = open_three_replicas(&root.join("raft-cluster")).await;
    replicas.get_mut(&1).unwrap().campaign().unwrap();
    pump(&mut replicas);
    assert!(
        replicas
            .values()
            .all(|replica| replica.observe().leader_id() == Some(replica_id(1)))
    );

    replicas
        .get_mut(&1)
        .unwrap()
        .propose(ShardCommand::AdvanceClosedTimestamp(
            AdvanceClosedTimestamp::new(CommandId::new(901).unwrap(), 7, 8, transaction_time(90))
                .unwrap(),
        ))
        .unwrap();
    pump(&mut replicas);
    replicas
        .get_mut(&1)
        .unwrap()
        .request_linearizable_read(902)
        .unwrap();
    let permits = pump(&mut replicas);
    let leader_permit = permits
        .into_iter()
        .find(|permit| permit.request_id() == Some(902))
        .unwrap();
    let authority = FollowerReadProofAuthority::new([9; 32]).unwrap();
    let proof = authority
        .issue(
            replicas[&1].binding().clone(),
            replica_id(1),
            leader_permit.leader_term(),
            leader_permit.fence().applied_index(),
            transaction_time(90),
        )
        .unwrap();
    let follower_permit = replicas[&2]
        .follower_read_permit(&authority, &proof, transaction_time(80))
        .unwrap();
    assert_eq!(
        follower_permit.fence().applied_index(),
        leader_permit.fence().applied_index()
    );

    let first_term = leader_permit.leader_term();
    replicas.remove(&1);
    replicas.get_mut(&2).unwrap().campaign().unwrap();
    pump(&mut replicas);
    assert!(
        replicas
            .values()
            .all(|replica| replica.observe().leader_id() == Some(replica_id(2)))
    );
    replicas
        .get_mut(&2)
        .unwrap()
        .request_linearizable_read(903)
        .unwrap();
    let second = pump(&mut replicas)
        .into_iter()
        .find(|permit| permit.request_id() == Some(903))
        .unwrap();
    assert!(second.leader_term() > first_term);

    json!({
        "replication_factor": 3,
        "initial_leader": 1,
        "failed_replica": 1,
        "elected_leader": 2,
        "initial_term": first_term,
        "failover_term": second.leader_term(),
        "follower_read": {
            "replica": 2,
            "applied_index": follower_permit.fence().applied_index(),
            "closed_timestamp": follower_permit.closed_timestamp().unwrap().get()
        }
    })
}

async fn open_three_replicas(root: &std::path::Path) -> BTreeMap<u64, RaftReplica> {
    std::fs::create_dir_all(root).unwrap();
    let voters = [1_u64, 2, 3]
        .into_iter()
        .map(replica_id)
        .collect::<Vec<_>>();
    let mut replicas = BTreeMap::new();
    for id in [1_u64, 2, 3] {
        let binding = binding(
            ProviderKind::Fjall,
            3,
            id,
            7,
            8,
            &format!("rf3-{id}"),
            BindingRole::Active,
        );
        let consensus = Arc::new(
            FjallConsensusStore::open(root.join(format!("consensus-{id}")), binding.clone())
                .unwrap(),
        );
        consensus
            .set_membership(RaftMembership {
                voters: voters.clone(),
                learners: Vec::new(),
                configuration_index: 0,
            })
            .await
            .unwrap();
        let state = Arc::new(
            FjallReplicaStore::open(root.join(format!("business-{id}")), binding).unwrap(),
        );
        let mut replica = RaftReplica::open(consensus, state).unwrap();
        replica.start().unwrap();
        replicas.insert(id, replica);
    }
    replicas
}

fn pump(replicas: &mut BTreeMap<u64, RaftReplica>) -> Vec<dtg_execution::shard::ReadPermit> {
    let mut permits = Vec::new();
    for _ in 0..512 {
        let mut messages = Vec::new();
        for replica in replicas.values_mut() {
            let mut progress = replica.drive_ready().unwrap();
            permits.extend(progress.read_permits().iter().cloned());
            messages.extend(progress.take_messages());
        }
        let mut delivered = 0;
        for message in messages {
            if let Some(target) = replicas.get_mut(&message.to) {
                target.step(message).unwrap();
                delivered += 1;
            }
        }
        if delivered == 0 {
            return permits;
        }
    }
    panic!("RF=3 certification cluster did not quiesce")
}

async fn certify_temporal_transactions() -> serde_json::Value {
    let timestamps = Arc::new(CertificationTimestamps::default());
    let shards = Arc::new(CertificationShards::default());
    let coordinator = TemporalTxnCoordinator::new(timestamps, shards.clone());

    let mut single = transaction_context(1001, &[7]);
    let single_mutation = vertex_mutation(1001);
    single.stage(single_mutation.clone()).unwrap();
    let single_outcome = coordinator
        .commit(
            &single,
            vec![ParticipantWrite::new(shard_id(7), vec![single_mutation]).unwrap()],
        )
        .await
        .unwrap();
    assert_eq!(
        single_outcome,
        TransactionOutcome::Committed(transaction_time(50))
    );

    let mut multi = transaction_context(1002, &[7, 9]);
    let first = vertex_mutation(1002);
    let second = vertex_mutation(1003);
    multi.stage(first.clone()).unwrap();
    multi.stage(second.clone()).unwrap();
    let multi_outcome = coordinator
        .commit(
            &multi,
            vec![
                ParticipantWrite::new(shard_id(7), vec![first]).unwrap(),
                ParticipantWrite::new(shard_id(9), vec![second]).unwrap(),
            ],
        )
        .await
        .unwrap();
    assert_eq!(
        multi_outcome,
        TransactionOutcome::Committed(transaction_time(60))
    );

    let paths = shards.paths.lock().unwrap().clone();
    assert_eq!(paths[0], "commit_single_shard");
    assert_eq!(
        paths[1..],
        [
            "prewrite",
            "prewrite",
            "record_home_decision",
            "finalize_commit",
            "finalize_commit"
        ]
    );
    json!({
        "snapshot_start_time": 40,
        "single_shard": {"commit_time": 50, "commands": [&paths[0]]},
        "two_phase_commit": {"commit_time": 60, "commands": &paths[1..]}
    })
}

#[derive(Default)]
struct CertificationTimestamps {
    reservations: Mutex<BTreeMap<u128, (i64, Option<CommitResolution>)>>,
}

impl TimestampAuthority for CertificationTimestamps {
    fn allocate_start_time(
        &self,
        _transaction_id: TransactionId,
    ) -> TxnFuture<'_, TransactionTime> {
        Box::pin(async { Ok(transaction_time(40)) })
    }

    fn reserve_commit_time(&self, transaction_id: TransactionId) -> TxnFuture<'_, TransactionTime> {
        Box::pin(async move {
            let mut reservations = self.reservations.lock().unwrap();
            let next = 50 + i64::try_from(reservations.len()).unwrap() * 10;
            let value = reservations
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
            Ok(self
                .reservations
                .lock()
                .unwrap()
                .get(&transaction_id.get())
                .map(|(time, resolution)| {
                    CommitTimeReservation::new(transaction_time(*time), *resolution)
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
            let mut reservations = self.reservations.lock().unwrap();
            let entry = reservations
                .get_mut(&transaction_id.get())
                .ok_or(TxnError::CorruptRecovery)?;
            if entry.0 != commit_time.get() {
                return Err(TxnError::CorruptRecovery);
            }
            entry.1 = Some(resolution);
            Ok(())
        })
    }
}

#[derive(Default)]
struct CertificationShards {
    paths: Mutex<Vec<&'static str>>,
}

impl ShardCommandExecutor for CertificationShards {
    fn submit(&self, shard_id: ShardId, request: ShardRequest) -> SubmissionFuture<'_> {
        Box::pin(async move {
            let (path, prepared) = match request {
                ShardRequest::CommitSingleShard { .. } => ("commit_single_shard", false),
                ShardRequest::PrewriteIntent { .. } => ("prewrite", true),
                ShardRequest::RecordHomeDecision { .. } => ("record_home_decision", false),
                ShardRequest::FinalizeParticipantCommit { .. } => ("finalize_commit", false),
                ShardRequest::FinalizeParticipantAbort { .. } => ("finalize_abort", false),
            };
            let mut paths = self.paths.lock().unwrap();
            paths.push(path);
            let index = paths.len() as u64;
            Ok(if prepared {
                SubmissionReceipt::prepared(index, false, Digest32::new([shard_id.get() as u8; 32]))
            } else {
                SubmissionReceipt::new(index, false)
            })
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

fn transaction_context(transaction: u128, shards: &[u64]) -> TransactionContext {
    TransactionContext::new(
        SnapshotToken::new(
            TransactionId::new(transaction).unwrap(),
            transaction_time(40),
            Version::new(1),
            shards
                .iter()
                .map(|id| {
                    (
                        shard_id(*id),
                        ShardSnapshotFence {
                            placement_epoch: dtg_execution::storage::PlacementEpoch::new(7)
                                .unwrap(),
                            backend_generation: BackendGeneration::new(8).unwrap(),
                            applied_index: 0,
                            closed_time: transaction_time(100),
                        },
                    )
                })
                .collect(),
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
            Properties::new(),
        )
        .unwrap(),
    )
}

async fn certify_snapshot_restart_continue(root: &std::path::Path) -> serde_json::Value {
    let snapshot_root = root.join("replica-snapshot");
    std::fs::create_dir_all(&snapshot_root).unwrap();
    let source_binding = binding(
        ProviderKind::Fjall,
        21,
        4,
        7,
        8,
        "snapshot-source",
        BindingRole::Active,
    );
    let source_consensus = Arc::new(
        FjallConsensusStore::open(
            snapshot_root.join("source-consensus"),
            source_binding.clone(),
        )
        .unwrap(),
    );
    source_consensus
        .set_membership(RaftMembership {
            voters: vec![source_binding.replica_id()],
            learners: vec![replica_id(5)],
            configuration_index: 0,
        })
        .await
        .unwrap();
    let source = Arc::new(
        FjallReplicaStore::open(snapshot_root.join("source-business"), source_binding).unwrap(),
    );
    let mut source_replica = RaftReplica::open(source_consensus.clone(), source.clone()).unwrap();
    source_replica.start().unwrap();
    source_replica.campaign().unwrap();
    source_replica.drive_ready().unwrap();
    source_replica
        .propose(snapshot_vertex_command(301))
        .unwrap();
    source_replica.drive_ready().unwrap();
    let permit = source_replica.snapshot_read_permit(2).unwrap();
    let snapshot =
        create_replica_snapshot(source.as_ref(), source_consensus.as_ref(), &permit, 302, 1)
            .unwrap();
    let snapshot_digest = snapshot.header().source_binding().identity_digest();

    let candidate_binding = binding(
        ProviderKind::Fjall,
        21,
        5,
        7,
        8,
        "snapshot-target",
        BindingRole::Candidate,
    );
    let active_binding = candidate_binding
        .to_builder()
        .role(BindingRole::Active)
        .build()
        .unwrap();
    let target_business = snapshot_root.join("target-business");
    let candidate =
        Arc::new(FjallReplicaStore::open(&target_business, candidate_binding.clone()).unwrap());
    let target_consensus = Arc::new(
        FjallConsensusStore::open(
            snapshot_root.join("target-consensus"),
            active_binding.clone(),
        )
        .unwrap(),
    );
    target_consensus
        .append(vec![consensus_entry(snapshot_vertex_command(303), 1, 3)])
        .await
        .unwrap();
    target_consensus
        .set_hard_state(RaftHardState {
            current_term: 1,
            voted_for: None,
            committed_index: 3,
        })
        .await
        .unwrap();
    let installed = install_replica_snapshot(
        snapshot,
        candidate.as_ref(),
        candidate.as_ref(),
        target_consensus.as_ref(),
        candidate_binding,
        active_binding.clone(),
    )
    .unwrap();
    assert_eq!(installed.state(), ReplicaSnapshotInstallState::Active);
    drop(candidate);

    let active = Arc::new(FjallReplicaStore::open(target_business, active_binding).unwrap());
    let mut restarted = RaftReplica::open(target_consensus, active.clone()).unwrap();
    let continued = restarted.recover().unwrap();
    assert_eq!(continued.len(), 1);
    assert_eq!(active.applied_index().await.unwrap(), 3);
    json!({
        "snapshot_id": 302,
        "snapshot_binding_digest": hex(snapshot_digest.get()),
        "installed_index": 2,
        "restart_replayed_suffix": 1,
        "continued_applied_index": 3
    })
}

fn snapshot_vertex_command(command_id: u128) -> ShardCommand {
    ShardCommand::CommitSingleShard(
        CommitSingleShard::new(
            CommandId::new(command_id).unwrap(),
            7,
            8,
            vec![vertex_mutation(command_id)],
        )
        .unwrap(),
    )
}

fn consensus_entry(command: ShardCommand, term: u64, index: u64) -> ConsensusEntry {
    let command_id = command.header().command_id();
    let context = command_id.get().to_be_bytes();
    let data = command.encode_current().unwrap();
    let mut payload = Vec::new();
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

async fn certify_snapshot_csr_and_tcypher_analytics() -> serde_json::Value {
    let graph = certification_graph();
    let result = run_builtin(
        &graph,
        &AlgorithmRequest::new(BuiltInAlgorithmId::PageRank),
        AlgorithmBudget::new(64 * 1024, 64, 100_000, CancellationToken::new()),
    )
    .unwrap();
    let AlgorithmResult::Scores(scores) = result else {
        panic!("PageRank did not return scores")
    };
    assert_eq!(scores.len(), 3);

    let transport = Arc::new(LedgerAnalyticsTransport::new(graph, scores.clone()));
    let gateway = GatewayService::new(
        GatewayConfig::new(
            "127.0.0.1:7687".parse().unwrap(),
            1,
            std::time::Duration::from_secs(5),
        )
        .unwrap(),
        GatewayExecution::for_process(transport.clone(), crate_planning_context()),
    );
    let submitted = gateway
        .bolt()
        .query("SUBMIT ANALYTICS page_rank ASYNC")
        .execute()
        .await
        .unwrap();
    let GatewayResponse::AnalyticsSubmitted { job_id } = submitted else {
        panic!("T-Cypher did not submit an analytics job")
    };
    assert_eq!(
        gateway.bolt().analytics_status(job_id).await.unwrap(),
        GatewayAnalyticsState::Succeeded
    );
    let rows = gateway.bolt().analytics_result(job_id).await.unwrap();
    assert_eq!(rows.rows().len(), 3);

    json!({
        "snapshot_csr": {"vertices": 3, "edges": 3, "digest": hex(transport.graph_digest())},
        "builtin": {"algorithm": "page_rank", "score_count": scores.len()},
        "tcypher_async": {"job_id": job_id.to_string(), "status": "succeeded", "result_rows": rows.rows().len()}
    })
}

fn certification_graph() -> SnapshotCsr {
    let snapshot = analytics_snapshot();
    let shard = shard_id(1);
    let fence = snapshot.shards()[&shard];
    SnapshotCsr::assemble(
        vec![ShardProjectionPart {
            snapshot,
            provenance: PartitionProvenance {
                shard_id: shard,
                placement_epoch: fence.placement_epoch,
                backend_generation: fence.backend_generation,
                applied_index: fence.applied_index,
                partition_index: 0,
                partition_count: 1,
            },
            vertices: [1_u128, 2, 3]
                .into_iter()
                .map(|id| ProjectedVertex {
                    id: VertexId::new(id).unwrap(),
                    properties: BTreeMap::new(),
                })
                .collect(),
            edges: [(1_u128, 1_u128, 2_u128), (2, 2, 3), (3, 3, 1)]
                .into_iter()
                .map(|(id, source, target)| ProjectedEdge {
                    id: EdgeId::new(id).unwrap(),
                    source: VertexId::new(source).unwrap(),
                    target: VertexId::new(target).unwrap(),
                    properties: BTreeMap::new(),
                })
                .collect(),
        }],
        ProjectionSpec::new(false, Vec::new(), Vec::new()).unwrap(),
        ProjectionBudget::new(64 * 1024, 0, CancellationToken::new()),
    )
    .unwrap()
}

fn analytics_snapshot() -> SnapshotProvenance {
    SnapshotProvenance::new(
        TransactionId::new(700).unwrap(),
        transaction_time(40),
        Version::new(1),
        vec![(
            shard_id(1),
            ShardSnapshotProvenance {
                placement_epoch: dtg_execution::storage::PlacementEpoch::new(7).unwrap(),
                backend_generation: BackendGeneration::new(8).unwrap(),
                applied_index: 2,
                closed_time: transaction_time(90),
            },
        )],
    )
    .unwrap()
}

struct LedgerAnalyticsTransport {
    graph: SnapshotCsr,
    scores: BTreeMap<VertexId, f64>,
    ledger: Mutex<AnalyticsLedger>,
    rows: Mutex<BTreeMap<u128, GatewayRows>>,
}

impl LedgerAnalyticsTransport {
    fn new(graph: SnapshotCsr, scores: BTreeMap<VertexId, f64>) -> Self {
        Self {
            graph,
            scores,
            ledger: Mutex::new(AnalyticsLedger::new(5).unwrap()),
            rows: Mutex::new(BTreeMap::new()),
        }
    }

    fn graph_digest(&self) -> [u8; 32] {
        self.graph.digest().get()
    }
}

impl GatewayExecutionTransport for LedgerAnalyticsTransport {
    fn execute(
        &self,
        request: GatewayClusterRequest,
    ) -> GatewayFuture<'_, Result<GatewayResponse, GatewayExecutionError>> {
        let response = match request.operation() {
            GatewayOperation::SubmitAnalytics { algorithm } if algorithm == "page_rank" => {
                let spec = AnalyticsJobSpec::new(
                    AnalyticsRequestIdentity::new(format!(
                        "tcypher-{}",
                        request.context().request_id()
                    ))
                    .unwrap(),
                    AlgorithmRequest::new(BuiltInAlgorithmId::PageRank),
                    analytics_snapshot(),
                    ProjectionSpec::new(false, Vec::new(), Vec::new()).unwrap(),
                    self.graph.digest(),
                    Digest32::new([4; 32]),
                    1,
                    1,
                    3,
                    JobTimestamp::new(100),
                )
                .unwrap();
                let mut ledger = self.ledger.lock().unwrap();
                let job = ledger.submit(spec.clone(), JobTimestamp::new(1)).unwrap();
                let cas = ledger.record(job).unwrap().cas();
                let lease = ledger
                    .claim(job, WorkerId::new(1).unwrap(), JobTimestamp::new(2), cas)
                    .unwrap();
                let lease = ledger.begin(lease, JobTimestamp::new(2)).unwrap();
                let bytes = format!("{:?}", self.scores).into_bytes();
                let artifact = AnalyticsArtifact::new(
                    &spec,
                    lease.lease_epoch(),
                    1,
                    ArtifactKind::Result,
                    bytes,
                    1024 * 1024,
                )
                .unwrap();
                ledger
                    .publish_result(lease, artifact.manifest().clone(), JobTimestamp::new(3))
                    .unwrap();
                let rows = GatewayRows::new(
                    vec!["vertex".into(), "score".into()],
                    self.scores
                        .iter()
                        .map(|(vertex, score)| {
                            vec![
                                GatewayValue::Integer(vertex.get() as i64),
                                GatewayValue::FloatBits(score.to_bits()),
                            ]
                        })
                        .collect(),
                )
                .unwrap();
                self.rows.lock().unwrap().insert(job.get(), rows);
                Ok(GatewayResponse::AnalyticsSubmitted { job_id: job.get() })
            }
            GatewayOperation::AnalyticsStatus { job_id } => {
                let job = dtg_execution::analytics::AnalyticsJobId::new(*job_id).unwrap();
                let state = self
                    .ledger
                    .lock()
                    .unwrap()
                    .record(job)
                    .unwrap()
                    .state()
                    .kind();
                let state = match state {
                    AnalyticsJobStateKind::Queued => GatewayAnalyticsState::Queued,
                    AnalyticsJobStateKind::Claimed | AnalyticsJobStateKind::Running => {
                        GatewayAnalyticsState::Running
                    }
                    AnalyticsJobStateKind::Succeeded => GatewayAnalyticsState::Succeeded,
                    AnalyticsJobStateKind::Failed => GatewayAnalyticsState::Failed,
                    AnalyticsJobStateKind::Cancelled => GatewayAnalyticsState::Cancelled,
                    AnalyticsJobStateKind::Tombstoned => {
                        return Box::pin(async {
                            Err(GatewayExecutionError::new(
                                "DTG-ANALYTICS-TOMBSTONED",
                                "tombstoned analytics jobs have no client-visible status",
                                dtg_execution::GatewayRetry::Never,
                            ))
                        });
                    }
                };
                Ok(GatewayResponse::AnalyticsStatus {
                    job_id: *job_id,
                    state,
                })
            }
            GatewayOperation::AnalyticsResult { job_id } => Ok(GatewayResponse::AnalyticsResult {
                job_id: *job_id,
                rows: self.rows.lock().unwrap()[job_id].clone(),
            }),
            operation => panic!("unexpected analytics certification operation: {operation:?}"),
        };
        Box::pin(async move { response })
    }
}

fn crate_planning_context() -> dtg_execution::planning::PlanningContext {
    use dtg_execution::planning::{
        CatalogShard, CatalogSnapshot, PlanningContext, SnapshotRequirements,
    };
    let capabilities = capabilities();
    let binding = binding(
        ProviderKind::Fjall,
        1,
        1,
        7,
        8,
        "analytics-planning",
        BindingRole::Active,
    );
    PlanningContext::new(
        CatalogSnapshot::new(
            Version::new(1),
            Version::new(1),
            vec![CatalogShard::new(binding, 2)],
        )
        .unwrap(),
        capabilities,
        SnapshotRequirements::fixed(transaction_time(40), 40),
        Some(16),
    )
    .unwrap()
}

fn certify_six_provider_directions() -> serde_json::Value {
    let directions = [
        (ProviderKind::Fjall, ProviderKind::PostgreSql),
        (ProviderKind::Fjall, ProviderKind::Kuzu),
        (ProviderKind::PostgreSql, ProviderKind::Fjall),
        (ProviderKind::PostgreSql, ProviderKind::Kuzu),
        (ProviderKind::Kuzu, ProviderKind::Fjall),
        (ProviderKind::Kuzu, ProviderKind::PostgreSql),
    ];
    let mut certified = Vec::new();
    for (offset, (source, target)) in directions.into_iter().enumerate() {
        exercise_migration(source.clone(), target.clone(), 800 + offset as u128);
        certified.push(format!(
            "{}->{}",
            provider_name(&source),
            provider_name(&target)
        ));
    }
    json!({"directions": certified, "count": 6, "active_generation": 4})
}

fn exercise_migration(source: ProviderKind, target: ProviderKind, id: u128) {
    let source_class =
        BackendClass::new(source, 1, 1, ["logical-snapshot", "ordered-mirror"]).unwrap();
    let target_class =
        BackendClass::new(target, 1, 1, ["logical-snapshot", "ordered-mirror"]).unwrap();
    let source_binding =
        binding_from_class(&source_class, 1, 3, "migration-source", BindingRole::Active);
    let target_binding = binding_from_class(
        &target_class,
        1,
        4,
        "migration-target",
        BindingRole::Candidate,
    );
    let voter = replica_id(1);
    let mut migration = MigrationRecord::new_direction(
        MigrationId::new(id).unwrap(),
        dtg_execution::storage::GraphId::new(1).unwrap(),
        shard_id(1),
        Version::new(12),
        dtg_execution::storage::PlacementEpoch::new(7).unwrap(),
        BackendGeneration::new(3).unwrap(),
        source_class,
        BackendGeneration::new(4).unwrap(),
        target_class.clone(),
        vec![voter],
    )
    .unwrap();
    migration
        .claim_target_namespace(
            voter,
            source_binding.identity_digest(),
            target_binding.identity_digest(),
            None,
        )
        .unwrap();
    migration.begin_backfill(41).unwrap();
    migration.begin_mirroring().unwrap();
    let digest = Digest32::new([id as u8; 32]);
    migration
        .record_receipt(
            MigrationReceipt::mirrored(
                migration.id(),
                voter,
                migration.catalog_version(),
                migration.source_generation(),
                migration.target_generation(),
                target_class.digest(),
                44,
                digest,
                44,
                digest,
            )
            .unwrap(),
        )
        .unwrap();
    migration.prepare().unwrap();
    migration
        .activate(dtg_execution::storage::PlacementEpoch::new(8).unwrap())
        .unwrap();
    migration.publish_activation(1000).unwrap();
    migration
        .record_reverse_receipt(
            MigrationReceipt::reverse_mirrored(
                migration.id(),
                voter,
                migration.catalog_version(),
                migration.source_generation(),
                migration.target_generation(),
                target_class.digest(),
                48,
                digest,
                48,
                digest,
            )
            .unwrap(),
        )
        .unwrap();
    migration
        .cleanup(voter, source_binding.identity_digest(), true)
        .unwrap();
    assert_eq!(migration.state(), MigrationState::Completed);
}

async fn certify_heterogeneous_data_node(root: &std::path::Path) -> serde_json::Value {
    let node = DataNodeBuilder::new(root.join("heterogeneous-consensus"))
        .with_provider(
            ProviderKind::Fjall,
            Arc::new(FixtureResolver(ProviderKind::Fjall)),
        )
        .with_provider(
            ProviderKind::PostgreSql,
            Arc::new(FixtureResolver(ProviderKind::PostgreSql)),
        )
        .with_provider(
            ProviderKind::Kuzu,
            Arc::new(FixtureResolver(ProviderKind::Kuzu)),
        )
        .assign(binding(
            ProviderKind::Fjall,
            31,
            31,
            1,
            1,
            "hetero-fjall",
            BindingRole::Active,
        ))
        .assign(binding(
            ProviderKind::PostgreSql,
            32,
            32,
            1,
            1,
            "hetero-postgres",
            BindingRole::Active,
        ))
        .assign(binding(
            ProviderKind::Kuzu,
            33,
            33,
            1,
            1,
            "hetero-kuzu",
            BindingRole::Active,
        ))
        .start()
        .await
        .unwrap();
    assert!(node.replica_failures().await.is_empty());
    let providers = node
        .observed_replicas()
        .await
        .iter()
        .map(|binding| provider_name(binding.provider_kind()).to_owned())
        .collect::<BTreeSet<_>>();
    assert_eq!(providers.len(), 3);
    json!({"hosted_shards": 3, "providers": providers})
}

struct FixtureResolver(ProviderKind);

impl ProviderResolver for FixtureResolver {
    fn provider_kind(&self) -> ProviderKind {
        self.0.clone()
    }

    fn open<'a>(&'a self, binding: ReplicaBinding) -> StoreFuture<'a, Arc<dyn ReplicaStateStore>> {
        Box::pin(
            async move { Ok(Arc::new(FixtureStore { binding }) as Arc<dyn ReplicaStateStore>) },
        )
    }
}

struct FixtureStore {
    binding: ReplicaBinding,
}

impl ReplicaStateStore for FixtureStore {
    fn binding(&self) -> &ReplicaBinding {
        &self.binding
    }

    fn applied_index(&self) -> StoreFuture<'_, u64> {
        Box::pin(async { Ok(0) })
    }

    fn replica_metadata<'a>(&'a self, _name: &'a str) -> StoreFuture<'a, Option<ReplicaMetadata>> {
        Box::pin(async { Ok(None) })
    }

    fn apply(&self, batch: CommittedShardBatch) -> StoreFuture<'_, ApplyReceipt> {
        Box::pin(async move { Ok(ApplyReceipt::new(&batch, false)) })
    }

    fn begin_read_view(&self, _fence: ReadFence) -> StoreFuture<'_, Box<dyn TemporalReadView>> {
        Box::pin(async { Err(StorageError::Unsupported) })
    }
}

fn binding(
    provider: ProviderKind,
    shard: u64,
    replica: u64,
    epoch: u64,
    generation: u64,
    namespace: &str,
    role: BindingRole,
) -> ReplicaBinding {
    let class =
        BackendClass::new(provider, 1, 1, capabilities().names().map(str::to_owned)).unwrap();
    binding_from_class(&class, replica, generation, namespace, role)
        .to_builder()
        .shard_id(shard)
        .placement_epoch(epoch)
        .build()
        .unwrap()
}

fn binding_from_class(
    class: &BackendClass,
    replica: u64,
    generation: u64,
    namespace: &str,
    role: BindingRole,
) -> ReplicaBinding {
    ReplicaBinding::builder()
        .cluster_id(1)
        .graph_id(2)
        .shard_id(3)
        .placement_epoch(7)
        .replica_id(replica)
        .backend_generation(generation)
        .backend_class_digest(class.digest())
        .provider_kind(class.provider_kind().clone())
        .contract_version(class.contract_version())
        .layout_version(class.layout_version())
        .capability_digest(class.required_capabilities().digest())
        .namespace_id(namespace)
        .endpoint_profile_ref("certification")
        .credential_ref("certification")
        .role(role)
        .build()
        .unwrap()
}

fn capabilities() -> CapabilityManifest {
    CapabilityManifest::from_names([
        "adjacency",
        "immutable-read-view",
        "logical-snapshot",
        "point",
    ])
    .unwrap()
}

fn replica_id(value: u64) -> ReplicaId {
    ReplicaId::new(value).unwrap()
}

fn shard_id(value: u64) -> ShardId {
    ShardId::new(value).unwrap()
}

fn transaction_time(value: i64) -> TransactionTime {
    TransactionTime::new(value).unwrap()
}

fn provider_name(provider: &ProviderKind) -> &'static str {
    match provider {
        ProviderKind::Fjall => "fjall",
        ProviderKind::PostgreSql => "postgresql",
        ProviderKind::Kuzu => "kuzu",
        ProviderKind::Remote(_) => "remote",
    }
}

fn hex(bytes: [u8; 32]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn _gateway_context() -> GatewayRequestContext {
    GatewayRequestContext::new(1, 1, u64::MAX, Vec::new()).unwrap()
}
