use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use adapter_neo4j::Neo4jAdapterFactory;
use adapter_postgres::PostgresAdapter;
use adapter_rocksdb::RocksAdapter;
use adapter_sidecar::{
    SidecarAdapter, SidecarService, TcpSidecarConfig, TcpSidecarServerConfig,
    TcpSidecarServerHandle, TcpSidecarTransport, spawn_stateful_tcp_sidecar_server,
};
use analytics_ledger::{
    AnalyticsJobId, GraphProjectionScope, JobCommand, JobSpec, LedgerState, ProjectionLimits,
};
use bolt_protocol::{ClientMessage, Value};
use bolt_server::{BoltMachine, BoltService, ServerMessage};
use cluster_protocol::CLUSTER_PROTOCOL_VERSION;
use cluster_protocol::proto::gateway_service_server::GatewayService;
use cluster_protocol::proto::meta_service_client::MetaServiceClient;
use cluster_protocol::proto::meta_service_server::{MetaService, MetaServiceServer};
use cluster_protocol::proto::shard_service_server::ShardServiceServer;
use cluster_protocol::proto::{
    GatewaySubmitRequest, ListAnalyticsJobTombstonesRequest, ProposeAnalyticsJobRequest,
    ProposeRequest, RequestContext,
};
use control_plane::{
    BackendProfile as CatalogBackendProfile, CatalogCommand, DeploymentMode, GraphDefinition,
    Placement, TopologyDefinition,
};
use cypher_engine::{CypherBoltService, schema_id};
use data_node::{
    BackendProfile, BackendSlotState, DataNodeGrpcService, DataNodeHost, DataRaftRuntime,
    NodeConfig, NodeIdentity, ReplicaKey, ReplicaRole, ReplicaSpec, TransportSecurity,
};
use dtgproxy::DeploymentConfig;
use dtgproxy::gateway::{ApiMutation, GATEWAY_API_VERSION, GatewayOperation, GatewayRequest};
use gateway_node::{
    AnalyticsFaultInjector, AnalyticsFaultPoint, GatewayCatalogRouter, RemoteGatewayService,
    process_stop_fault,
};
use meta_node::{MetaNodeService, MetaRaftReplica, ReplicatedTso};
use shard_client::{
    ArtifactKind, GetArtifactGenerationRequest, ListArtifactGenerationsRequest,
    PinArtifactGenerationRequest, PutArtifactChunkRequest, RemoteShardClient, ShardClient,
    ShardRequestContext,
};
use storage_api::{
    AdapterRequirement, LogicalSnapshotExportRequest, MappingBackedAdapter, MappingRequirement,
    StorageAdapter, TemporalBackendMapping,
};
use temporal_ir::GraphScope;
use temporal_storage::{GraphId, PartitionId};
use temporal_types::{CanonicalElement, GraphValue, TransactionTime, ValidTime};
use timestamp_oracle::ManualClock;
use tokio::sync::Mutex;
use tokio_stream::{StreamExt, wrappers::TcpListenerStream};
use tonic::Request;
use tonic::transport::Server;

const CLUSTER_ID: [u8; 16] = [0x76; 16];

struct FailOnceProcessStop {
    point: AnalyticsFaultPoint,
    fired: AtomicBool,
}

struct ObservedProcessStop {
    point: AnalyticsFaultPoint,
    fired: Arc<AtomicBool>,
}

impl AnalyticsFaultInjector for ObservedProcessStop {
    fn check(
        &self,
        point: AnalyticsFaultPoint,
    ) -> Result<(), procedure_runtime::ClusterAnalyticsError> {
        if point == self.point && !self.fired.swap(true, Ordering::AcqRel) {
            return Err(process_stop_fault(point));
        }
        Ok(())
    }
}

impl AnalyticsFaultInjector for FailOnceProcessStop {
    fn check(
        &self,
        point: AnalyticsFaultPoint,
    ) -> Result<(), procedure_runtime::ClusterAnalyticsError> {
        if point == self.point && !self.fired.swap(true, Ordering::AcqRel) {
            return Err(process_stop_fault(point));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
enum Backend {
    RocksDb,
    PostgreSql,
    Neo4j,
}

impl Backend {
    const ALL: [Self; 3] = [Self::RocksDb, Self::PostgreSql, Self::Neo4j];

    const fn name(self) -> &'static str {
        match self {
            Self::RocksDb => "rocksdb",
            Self::PostgreSql => "postgresql",
            Self::Neo4j => "neo4j",
        }
    }

    const fn gateway_id(self) -> u64 {
        match self {
            Self::RocksDb => 701,
            Self::PostgreSql => 702,
            Self::Neo4j => 703,
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
struct SurfaceResult {
    transaction_rows: Vec<Vec<Value>>,
    node_count: Vec<Vec<Value>>,
    degree: Vec<Vec<Value>>,
    interval: Vec<Vec<Value>>,
    delta: Vec<Vec<Value>>,
    pagination: PaginationResult,
    canonical_error: ErrorResult,
    async_result_artifacts: BTreeMap<String, Vec<u8>>,
}

const RESUMABLE_ALGORITHMS: [&str; 3] = ["dtg.graph.degree", "dtg.graph.wcc", "dtg.graph.pageRank"];
const DEGREE_FAULT_POINTS: [(AnalyticsFaultPoint, &str); 9] = [
    (AnalyticsFaultPoint::Claim, "claim"),
    (AnalyticsFaultPoint::LeaseRenew, "lease-renew"),
    (AnalyticsFaultPoint::ExecutionSlice, "execution-slice"),
    (AnalyticsFaultPoint::CheckpointUpload, "checkpoint-upload"),
    (AnalyticsFaultPoint::CheckpointPin, "checkpoint-pin"),
    (AnalyticsFaultPoint::CheckpointCas, "checkpoint-cas"),
    (AnalyticsFaultPoint::ResultUpload, "result-upload"),
    (AnalyticsFaultPoint::ResultPin, "result-pin"),
    (AnalyticsFaultPoint::Publish, "publish"),
];

#[derive(Debug, Eq, PartialEq)]
struct PaginationResult {
    first: Vec<Vec<Value>>,
    second: Vec<Vec<Value>>,
    first_has_more: bool,
    second_has_more: bool,
}

#[derive(Debug, Eq, PartialEq)]
struct ErrorResult {
    code: String,
    message: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SidecarRecovery {
    None,
    SameGateway,
    CrossGateway,
}

struct LiveConfiguration {
    postgres_url: String,
    neo4j_endpoint: String,
    neo4j_username: String,
    neo4j_password: String,
    neo4j_database: String,
    suffix: u128,
}

impl LiveConfiguration {
    fn from_process() -> Self {
        Self {
            postgres_url: std::env::var("DTGPROXY_POSTGRES_URL")
                .expect("DTGPROXY_POSTGRES_URL must point to disposable PostgreSQL"),
            neo4j_endpoint: std::env::var("DTGPROXY_NEO4J_ENDPOINT")
                .expect("DTGPROXY_NEO4J_ENDPOINT must point to disposable Neo4j"),
            neo4j_username: std::env::var("DTGPROXY_NEO4J_USERNAME")
                .unwrap_or_else(|_| "neo4j".into()),
            neo4j_password: std::env::var("DTGPROXY_NEO4J_PASSWORD")
                .expect("DTGPROXY_NEO4J_PASSWORD must authenticate to disposable Neo4j"),
            neo4j_database: std::env::var("DTGPROXY_NEO4J_DATABASE")
                .unwrap_or_else(|_| "neo4j".into()),
            suffix: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn all_backends_are_equivalent_in_both_deployment_modes() {
    let live = LiveConfiguration::from_process();
    let mut reference = None;
    for backend in Backend::ALL {
        let primary = run_surface(
            &live,
            backend,
            DeploymentMode::PrimaryReplica,
            None,
            SidecarRecovery::None,
        )
        .await;
        let shared = run_surface(
            &live,
            backend,
            DeploymentMode::SharedNothing,
            None,
            SidecarRecovery::None,
        )
        .await;
        assert_surface_semantics(&primary);
        assert_surface_semantics(&shared);
        assert_eq!(primary, shared, "{backend:?} deployment-mode equivalence");
        if let Some(reference) = &reference {
            assert_eq!(&primary, reference, "{backend:?} backend equivalence");
        } else {
            reference = Some(primary);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resumable_algorithms_are_byte_identical_after_takeover_on_every_backend_and_mode() {
    let live = LiveConfiguration::from_process();
    let backend_filter = std::env::var("DTGPROXY_CERT_BACKEND").ok();
    let mode_filter = std::env::var("DTGPROXY_CERT_MODE").ok();
    let algorithm_filter = std::env::var("DTGPROXY_CERT_ALGORITHM").ok();
    for backend in Backend::ALL {
        if backend_filter
            .as_deref()
            .is_some_and(|filter| filter != backend.name())
        {
            continue;
        }
        for mode in [
            DeploymentMode::PrimaryReplica,
            DeploymentMode::SharedNothing,
        ] {
            if mode_filter
                .as_deref()
                .is_some_and(|filter| filter != mode_name(mode))
            {
                continue;
            }
            let baseline = run_surface(&live, backend, mode, None, SidecarRecovery::None).await;
            for algorithm in RESUMABLE_ALGORITHMS {
                if algorithm_filter
                    .as_deref()
                    .is_some_and(|filter| filter != algorithm)
                {
                    continue;
                }
                let recovered = run_surface(
                    &live,
                    backend,
                    mode,
                    Some((algorithm, AnalyticsFaultPoint::Begin, false)),
                    SidecarRecovery::None,
                )
                .await;
                assert_surface_semantics(&recovered);
                assert_eq!(
                    recovered.async_result_artifacts.get(algorithm),
                    baseline.async_result_artifacts.get(algorithm),
                    "{backend:?} {mode:?} {algorithm} result Artifact must be byte-identical after takeover"
                );
            }
            if algorithm_filter
                .as_deref()
                .is_none_or(|filter| filter == "dtg.graph.degree")
            {
                let fault_filter = std::env::var("DTGPROXY_CERT_FAULT_POINT").ok();
                for (fault_point, fault_name) in DEGREE_FAULT_POINTS {
                    if fault_filter
                        .as_deref()
                        .is_some_and(|filter| filter != fault_name)
                    {
                        continue;
                    }
                    let recovered = run_surface(
                        &live,
                        backend,
                        mode,
                        Some(("dtg.graph.degree", fault_point, false)),
                        SidecarRecovery::None,
                    )
                    .await;
                    assert_surface_semantics(&recovered);
                    assert_eq!(
                        recovered.async_result_artifacts.get("dtg.graph.degree"),
                        baseline.async_result_artifacts.get("dtg.graph.degree"),
                        "{backend:?} {mode:?} Degree result Artifact must be byte-identical after {fault_name} takeover"
                    );
                }
            }
            if algorithm_filter
                .as_deref()
                .is_none_or(|filter| filter == "dtg.graph.degree")
                && std::env::var("DTGPROXY_CERT_FAULT_POINT")
                    .ok()
                    .as_deref()
                    .is_none_or(|filter| filter == "gateway-restart")
            {
                let recovered = run_surface(
                    &live,
                    backend,
                    mode,
                    Some((
                        "dtg.graph.degree",
                        AnalyticsFaultPoint::ExecutionSlice,
                        true,
                    )),
                    SidecarRecovery::None,
                )
                .await;
                assert_surface_semantics(&recovered);
                assert_eq!(
                    recovered.async_result_artifacts.get("dtg.graph.degree"),
                    baseline.async_result_artifacts.get("dtg.graph.degree"),
                    "{backend:?} {mode:?} Degree Result Artifact must be byte-identical after same-Gateway runtime restart"
                );
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn backend_sidecar_restart_recovers_degree_on_every_backend_and_mode() {
    let live = LiveConfiguration::from_process();
    let backend_filter = std::env::var("DTGPROXY_CERT_BACKEND").ok();
    let mode_filter = std::env::var("DTGPROXY_CERT_MODE").ok();
    for backend in Backend::ALL {
        if backend_filter
            .as_deref()
            .is_some_and(|filter| filter != backend.name())
        {
            continue;
        }
        for mode in [
            DeploymentMode::PrimaryReplica,
            DeploymentMode::SharedNothing,
        ] {
            if mode_filter
                .as_deref()
                .is_some_and(|filter| filter != mode_name(mode))
            {
                continue;
            }
            let baseline = run_surface(&live, backend, mode, None, SidecarRecovery::None).await;
            let recovered =
                run_surface(&live, backend, mode, None, SidecarRecovery::SameGateway).await;
            assert_surface_semantics(&recovered);
            assert_eq!(
                recovered.async_result_artifacts.get("dtg.graph.degree"),
                baseline.async_result_artifacts.get("dtg.graph.degree"),
                "{backend:?} {mode:?} Degree Result Artifact must be byte-identical after backend Sidecar restart"
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn backend_sidecar_restart_allows_cross_gateway_degree_takeover() {
    let live = LiveConfiguration::from_process();
    let backend_filter = std::env::var("DTGPROXY_CERT_BACKEND").ok();
    let mode_filter = std::env::var("DTGPROXY_CERT_MODE").ok();
    for backend in Backend::ALL {
        if backend_filter
            .as_deref()
            .is_some_and(|filter| filter != backend.name())
        {
            continue;
        }
        for mode in [
            DeploymentMode::PrimaryReplica,
            DeploymentMode::SharedNothing,
        ] {
            if mode_filter
                .as_deref()
                .is_some_and(|filter| filter != mode_name(mode))
            {
                continue;
            }
            let baseline = run_surface(&live, backend, mode, None, SidecarRecovery::None).await;
            let recovered =
                run_surface(&live, backend, mode, None, SidecarRecovery::CrossGateway).await;
            assert_surface_semantics(&recovered);
            assert_eq!(
                recovered.async_result_artifacts.get("dtg.graph.degree"),
                baseline.async_result_artifacts.get("dtg.graph.degree"),
                "{backend:?} {mode:?} Degree Result Artifact must be byte-identical after backend Sidecar restart and cross-Gateway takeover"
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tombstone_gc_recovers_at_every_boundary_on_every_backend_and_mode() {
    let live = LiveConfiguration::from_process();
    let backend_filter = std::env::var("DTGPROXY_CERT_BACKEND").ok();
    let mode_filter = std::env::var("DTGPROXY_CERT_MODE").ok();
    let fault_filter = std::env::var("DTGPROXY_CERT_GC_FAULT_POINT").ok();
    for backend in Backend::ALL {
        if backend_filter
            .as_deref()
            .is_some_and(|filter| filter != backend.name())
        {
            continue;
        }
        for mode in [
            DeploymentMode::PrimaryReplica,
            DeploymentMode::SharedNothing,
        ] {
            if mode_filter
                .as_deref()
                .is_some_and(|filter| filter != mode_name(mode))
            {
                continue;
            }
            for (fault_point, fault_name) in [
                (AnalyticsFaultPoint::GcAfterFenceAdvance, "after-fence"),
                (AnalyticsFaultPoint::GcBeforeDelete, "before-delete"),
                (
                    AnalyticsFaultPoint::GcBeforeAcknowledgement,
                    "before-acknowledgement",
                ),
            ] {
                if fault_filter
                    .as_deref()
                    .is_some_and(|filter| filter != fault_name)
                {
                    continue;
                }
                run_gc_recovery_certification(&live, backend, mode, fault_point).await;
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ordered_full_stack_restart_recovers_on_every_backend_and_mode() {
    let live = LiveConfiguration::from_process();
    let backend_filter = std::env::var("DTGPROXY_CERT_BACKEND").ok();
    let mode_filter = std::env::var("DTGPROXY_CERT_MODE").ok();
    for backend in Backend::ALL {
        if backend_filter
            .as_deref()
            .is_some_and(|filter| filter != backend.name())
        {
            continue;
        }
        for mode in [
            DeploymentMode::PrimaryReplica,
            DeploymentMode::SharedNothing,
        ] {
            if mode_filter
                .as_deref()
                .is_some_and(|filter| filter != mode_name(mode))
            {
                continue;
            }
            run_live_full_stack_restart_certification(&live, backend, mode).await;
        }
    }
}

async fn run_live_full_stack_restart_certification(
    live: &LiveConfiguration,
    backend: Backend,
    mode: DeploymentMode,
) {
    eprintln!(
        "three-backend full-stack restart case: backend={} mode={}",
        backend.name(),
        mode_name(mode)
    );
    let temporary = tempfile::tempdir().expect("temporary full-stack fixture");
    let case_suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("wall clock after Unix epoch")
        .as_nanos();
    let placements = match mode {
        DeploymentMode::PrimaryReplica => vec![Placement::new(10, 1, vec![10, 11]).unwrap()],
        DeploymentMode::SharedNothing => vec![
            Placement::new(10, 1, vec![10]).unwrap(),
            Placement::new(20, 1, vec![20]).unwrap(),
        ],
    };
    let graph = GraphDefinition::new(
        7,
        "analytics",
        1,
        TopologyDefinition::new(mode, 99, 128, 1, placements.clone()).unwrap(),
        CatalogBackendProfile::new(
            backend.name(),
            BTreeMap::new(),
            BTreeMap::new(),
            AdapterRequirement::HotPluggableReplica,
            1,
        )
        .unwrap(),
    )
    .unwrap();

    let meta_address = free_address();
    let mut meta = LiveRestartableMeta::new(temporary.path().join("meta"), meta_address);
    meta.start().await;
    let mut meta_client = MetaServiceClient::connect(format!("http://{meta_address}"))
        .await
        .unwrap();
    meta_client
        .propose(ProposeRequest {
            context: Some(context(30_001)),
            command: CatalogCommand::create_graph(30_001, 0, graph)
                .encode()
                .unwrap(),
        })
        .await
        .unwrap();
    drop(meta_client);

    let mut shards = Vec::new();
    let mut data_endpoints = BTreeMap::new();
    let raft_addresses = placements
        .iter()
        .flat_map(|placement| placement.voters().iter().copied())
        .map(|node_id| (node_id, free_address()))
        .collect::<BTreeMap<_, _>>();
    for placement in &placements {
        for &node_id in placement.voters() {
            let api_address = free_address();
            let raft_peers = placement
                .voters()
                .iter()
                .filter(|peer_id| **peer_id != node_id)
                .map(|peer_id| (*peer_id, raft_addresses[peer_id]))
                .collect::<BTreeMap<_, _>>();
            let mut shard = LiveRestartableShard::new(
                node_id,
                temporary.path().join(format!("data-{node_id}")),
                temporary.path().join(format!("backend-{node_id}")),
                api_address,
                raft_addresses[&node_id],
                raft_peers,
                free_address(),
                placement.clone(),
                backend,
                format!(
                    "full-stack-{}-{}-{node_id}-{}-{case_suffix}",
                    backend.name(),
                    mode_name(mode),
                    live.suffix
                ),
            );
            shard.start(live).await;
            data_endpoints.insert(node_id, api_address);
            shards.push(shard);
        }
    }
    campaign_live_shard_leaders(&shards, &placements).await;
    let router = GatewayCatalogRouter::new(
        CLUSTER_ID,
        31,
        7,
        vec![meta_address],
        data_endpoints,
        Duration::from_secs(5),
    )
    .unwrap();
    let snapshot = router.load(1).await.unwrap();
    let (_, authoritative_graph, topology) = snapshot.into_parts();
    let first_client =
        Arc::new(RemoteShardClient::new_loopback_plaintext(CLUSTER_ID, topology.clone()).unwrap());
    let first_gateway = RemoteGatewayService::new_at_revision_with_gateway_id_and_delay(
        backend.gateway_id() + 4_000 + u64::from(mode == DeploymentMode::SharedNothing),
        Duration::from_secs(2),
        CLUSTER_ID,
        1,
        authoritative_graph.clone(),
        Arc::clone(&first_client),
        vec![meta_address],
        32,
        64,
    )
    .unwrap();
    let first_service = Arc::new(CypherBoltService::new(Arc::new(first_gateway), 16).unwrap());
    let mut first_bolt = BoltMachine::new(first_service);
    assert!(matches!(
        first_bolt
            .handle(ClientMessage::Hello(BTreeMap::new()))
            .await
            .as_slice(),
        [ServerMessage::Success(_)]
    ));
    execute_write(
        &mut first_bolt,
        "USE analytics FOR VALID_TIME AS OF 1000 \
         CREATE (a:Person)-[:KNOWS]->(b:Person)",
    )
    .await;
    let baseline_job = submit_async_algorithm(&mut first_bolt, "dtg.graph.degree").await;
    wait_async_algorithm(&mut first_bolt, &baseline_job, "dtg.graph.degree").await;
    let baseline_bytes =
        read_result_artifact(first_client.as_ref(), &placements[0], &baseline_job).await;
    assert_live_replica_groups(&shards, &placements).await;
    assert_live_replica_backends_match(&shards, &placements).await;

    let interrupted_job = submit_async_algorithm(&mut first_bolt, "dtg.graph.degree").await;
    wait_for_analytics_state(&mut first_bolt, &interrupted_job, "RUNNING").await;
    for shard in &mut shards {
        shard.stop().await;
    }
    meta.stop().await;
    drop(first_bolt);
    drop(first_client);
    tokio::time::sleep(Duration::from_secs(3)).await;

    meta.start().await;
    for shard in &mut shards {
        shard.start(live).await;
    }
    campaign_live_shard_leaders(&shards, &placements).await;
    let recovered_client =
        Arc::new(RemoteShardClient::new_loopback_plaintext(CLUSTER_ID, topology).unwrap());
    let second_gateway = RemoteGatewayService::new_at_revision_with_gateway_id(
        backend.gateway_id() + 5_000 + u64::from(mode == DeploymentMode::SharedNothing),
        CLUSTER_ID,
        1,
        authoritative_graph,
        Arc::clone(&recovered_client),
        vec![meta_address],
        32,
        64,
    )
    .unwrap();
    let second_service = Arc::new(CypherBoltService::new(Arc::new(second_gateway), 16).unwrap());
    let mut second_bolt = BoltMachine::new(second_service);
    assert!(matches!(
        second_bolt
            .handle(ClientMessage::Hello(BTreeMap::new()))
            .await
            .as_slice(),
        [ServerMessage::Success(_)]
    ));
    wait_async_algorithm(&mut second_bolt, &interrupted_job, "dtg.graph.degree").await;
    let recovered_bytes =
        read_result_artifact(recovered_client.as_ref(), &placements[0], &interrupted_job).await;
    assert_eq!(
        recovered_bytes, baseline_bytes,
        "{backend:?} {mode:?} ordered full-stack restart changed DTAR bytes"
    );
    let generations = list_gc_generations(
        recovered_client.as_ref(),
        &placements[0],
        u128::from_str_radix(&interrupted_job, 16).unwrap(),
        30_100,
    )
    .await;
    assert_eq!(
        generations.len(),
        1,
        "full-stack restart left a ghost generation"
    );
    assert!(
        generations[0].pinned(),
        "the only Result generation must be pinned"
    );
    assert_live_replica_groups(&shards, &placements).await;
    assert_live_replica_backends_match(&shards, &placements).await;

    drop(second_bolt);
    drop(recovered_client);
    for shard in &mut shards {
        shard.stop().await;
    }
    meta.stop().await;
}

struct LiveRestartableMeta {
    root: PathBuf,
    address: SocketAddr,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    server: Option<tokio::task::JoinHandle<Result<(), tonic::transport::Error>>>,
}

impl LiveRestartableMeta {
    fn new(root: PathBuf, address: SocketAddr) -> Self {
        Self {
            root,
            address,
            shutdown: None,
            server: None,
        }
    }

    async fn start(&mut self) {
        assert!(self.shutdown.is_none());
        assert!(self.server.is_none());
        let listener = tokio::net::TcpListener::bind(self.address).await.unwrap();
        let service = elected_meta(&self.root);
        let (shutdown, shutdown_receiver) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(
            Server::builder()
                .add_service(MetaServiceServer::new(service))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    let _ = shutdown_receiver.await;
                }),
        );
        self.shutdown = Some(shutdown);
        self.server = Some(server);
    }

    async fn stop(&mut self) {
        self.shutdown.take().unwrap().send(()).unwrap();
        self.server.take().unwrap().await.unwrap().unwrap();
    }
}

async fn campaign_live_shard_leaders(shards: &[LiveRestartableShard], placements: &[Placement]) {
    for placement in placements {
        let leader_id = placement.voters()[0];
        let leader = shards
            .iter()
            .find(|shard| {
                shard.node_id == leader_id && shard.placement.shard_id() == placement.shard_id()
            })
            .expect("restartable live Shard leader");
        leader.campaign().await;
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if leader.status().await.is_leader() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("restartable live Shard leader election");
    }
}

async fn assert_live_replica_groups(shards: &[LiveRestartableShard], placements: &[Placement]) {
    for placement in placements {
        let leader_id = placement.voters()[0];
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let mut statuses = Vec::new();
                for &node_id in placement.voters() {
                    let shard = shards
                        .iter()
                        .find(|shard| {
                            shard.node_id == node_id
                                && shard.placement.shard_id() == placement.shard_id()
                        })
                        .expect("restartable live Shard replica");
                    statuses.push((node_id, shard.status().await));
                }
                let leader = statuses
                    .iter()
                    .find(|(node_id, _)| *node_id == leader_id)
                    .unwrap();
                if leader.1.is_leader()
                    && leader.1.applied_index() > 0
                    && statuses
                        .iter()
                        .all(|(_, status)| status.applied_index() == leader.1.applied_index())
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "restartable live Shard {} replicas did not converge",
                placement.shard_id()
            )
        });
    }
}

async fn assert_live_replica_backends_match(
    shards: &[LiveRestartableShard],
    placements: &[Placement],
) {
    for placement in placements {
        if placement.voters().len() < 2 {
            continue;
        }
        let mut expected = None;
        for &node_id in placement.voters() {
            let shard = shards
                .iter()
                .find(|shard| {
                    shard.node_id == node_id && shard.placement.shard_id() == placement.shard_id()
                })
                .expect("restartable live Shard replica");
            let (applied_index, entries) = export_sidecar_snapshot(shard.sidecar_address).await;
            let status = shard.status().await;
            assert_eq!(
                applied_index,
                status.applied_index(),
                "Shard {} node {node_id} Sidecar index differs from Raft applied index",
                placement.shard_id()
            );
            if let Some((expected_node, expected_entries)) = &expected {
                assert_eq!(
                    &entries,
                    expected_entries,
                    "Shard {} backend bytes differ between nodes {expected_node} and {node_id}",
                    placement.shard_id()
                );
            } else {
                expected = Some((node_id, entries));
            }
        }
    }
}

async fn export_sidecar_snapshot(address: SocketAddr) -> (u64, Vec<storage_api::KeyValue>) {
    let config = TcpSidecarConfig::new(address).with_pool_size(1).unwrap();
    let transport = TcpSidecarTransport::connect(config).unwrap();
    let adapter = SidecarAdapter::connect(transport).await.unwrap();
    let applied_index = adapter.applied_log_index().unwrap();
    let mut reader = adapter
        .begin_logical_export(LogicalSnapshotExportRequest::default())
        .await
        .unwrap();
    assert_eq!(reader.header().applied_log_index(), applied_index);
    let mut entries = Vec::new();
    while let Some(chunk) = reader.next_chunk().await.unwrap() {
        entries.extend_from_slice(chunk.entries());
    }
    let manifest = reader.finish().await.unwrap();
    assert_eq!(manifest.header().applied_log_index(), applied_index);
    (applied_index, entries)
}

struct LiveRestartableShard {
    node_id: u64,
    root: PathBuf,
    backend_root: PathBuf,
    api_address: SocketAddr,
    raft_address: SocketAddr,
    raft_peers: BTreeMap<u64, SocketAddr>,
    sidecar_address: SocketAddr,
    placement: Placement,
    backend: Backend,
    instance_id: String,
    sidecar: Option<TcpSidecarServerHandle>,
    host: Option<Arc<DataNodeHost>>,
    api_shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    api_server: Option<tokio::task::JoinHandle<Result<(), tonic::transport::Error>>>,
    raft_runtime: Option<DataRaftRuntime>,
}

impl LiveRestartableShard {
    #[allow(clippy::too_many_arguments)]
    fn new(
        node_id: u64,
        root: PathBuf,
        backend_root: PathBuf,
        api_address: SocketAddr,
        raft_address: SocketAddr,
        raft_peers: BTreeMap<u64, SocketAddr>,
        sidecar_address: SocketAddr,
        placement: Placement,
        backend: Backend,
        instance_id: String,
    ) -> Self {
        Self {
            node_id,
            root,
            backend_root,
            api_address,
            raft_address,
            raft_peers,
            sidecar_address,
            placement,
            backend,
            instance_id,
            sidecar: None,
            host: None,
            api_shutdown: None,
            api_server: None,
            raft_runtime: None,
        }
    }

    async fn start(&mut self, live: &LiveConfiguration) {
        assert!(self.sidecar.is_none());
        assert!(self.host.is_none());
        let sidecar = spawn_backend_sidecar(
            live,
            self.backend,
            &self.backend_root,
            &self.instance_id,
            self.sidecar_address,
        )
        .await;
        let profile = BackendProfile::new(
            "sidecar",
            self.instance_id.clone(),
            BTreeMap::from([
                ("endpoint".into(), self.sidecar_address.to_string()),
                ("pool_size".into(), "1".into()),
            ]),
            BTreeMap::new(),
        )
        .unwrap();
        let host = Arc::new(
            DataNodeHost::open(data_config(&self.root, self.node_id), 32)
                .await
                .unwrap(),
        );
        host.ensure_replica(
            ReplicaSpec::new_with_backend(
                7,
                self.placement.shard_id(),
                self.placement.epoch(),
                self.placement.voters().to_vec(),
                ReplicaRole::Voter,
                1,
                BackendSlotState::active(1, profile).unwrap(),
                format!("live-restart-shard-{}", self.placement.shard_id()),
            )
            .unwrap(),
        )
        .await
        .unwrap();
        let raft_runtime = DataRaftRuntime::start(
            Arc::clone(&host),
            self.raft_address,
            &self.raft_peers,
            64,
            Duration::from_millis(10),
        )
        .await
        .unwrap();
        let listener = tokio::net::TcpListener::bind(self.api_address)
            .await
            .unwrap();
        let (api_shutdown, api_shutdown_receiver) = tokio::sync::oneshot::channel();
        let api_server = tokio::spawn(
            Server::builder()
                .add_service(ShardServiceServer::new(DataNodeGrpcService::new(
                    Arc::clone(&host),
                )))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    let _ = api_shutdown_receiver.await;
                }),
        );
        self.sidecar = Some(sidecar);
        self.host = Some(host);
        self.api_shutdown = Some(api_shutdown);
        self.api_server = Some(api_server);
        self.raft_runtime = Some(raft_runtime);
    }

    async fn campaign(&self) {
        self.host
            .as_ref()
            .unwrap()
            .campaign(ReplicaKey::new(7, self.placement.shard_id()).unwrap())
            .await
            .unwrap();
    }

    async fn status(&self) -> data_node::ReplicaStatus {
        self.host
            .as_ref()
            .unwrap()
            .status(ReplicaKey::new(7, self.placement.shard_id()).unwrap())
            .await
            .unwrap()
    }

    async fn stop(&mut self) {
        self.api_shutdown.take().unwrap().send(()).unwrap();
        self.api_server.take().unwrap().await.unwrap().unwrap();
        self.raft_runtime.take().unwrap().shutdown().await.unwrap();
        let host = self.host.take().unwrap();
        Arc::try_unwrap(host)
            .unwrap_or_else(|_| panic!("live restart Shard retained its DataNode host"))
            .shutdown()
            .await
            .unwrap();
        self.sidecar.take().unwrap().shutdown().unwrap();
    }
}

async fn run_gc_recovery_certification(
    live: &LiveConfiguration,
    backend: Backend,
    mode: DeploymentMode,
    fault_point: AnalyticsFaultPoint,
) {
    eprintln!(
        "three-backend GC certification case: backend={} mode={} fault={fault_point:?}",
        backend.name(),
        mode_name(mode)
    );
    let temporary = tempfile::tempdir().expect("temporary GC fixture");
    let case_suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("wall clock after Unix epoch")
        .as_nanos();
    let placements = match mode {
        DeploymentMode::PrimaryReplica => vec![Placement::new(10, 1, vec![10, 11]).unwrap()],
        DeploymentMode::SharedNothing => vec![
            Placement::new(10, 1, vec![10]).unwrap(),
            Placement::new(20, 1, vec![20]).unwrap(),
        ],
    };
    let graph = GraphDefinition::new(
        7,
        "analytics-gc",
        1,
        TopologyDefinition::new(mode, 99, 128, 1, placements.clone()).unwrap(),
        CatalogBackendProfile::new(
            backend.name(),
            BTreeMap::new(),
            BTreeMap::new(),
            AdapterRequirement::HotPluggableReplica,
            1,
        )
        .unwrap(),
    )
    .unwrap();

    let meta = elected_meta(&temporary.path().join("meta"));
    meta.propose(Request::new(ProposeRequest {
        context: Some(context(20_001)),
        command: CatalogCommand::create_graph(20_001, 0, graph.clone())
            .encode()
            .unwrap(),
    }))
    .await
    .unwrap();
    let submitted_at = now_ms();
    for (request_id, command) in [
        (
            20_002,
            JobCommand::submit(20_002, gc_job(701, 71_001), submitted_at).unwrap(),
        ),
        (
            20_003,
            JobCommand::cancel(20_003, AnalyticsJobId::new(701).unwrap(), 1).unwrap(),
        ),
        (
            20_004,
            JobCommand::prune_terminal(
                20_004,
                AnalyticsJobId::new(701).unwrap(),
                2,
                submitted_at + 1,
            )
            .unwrap(),
        ),
        (
            20_005,
            JobCommand::submit(20_005, gc_job(703, 71_003), submitted_at + 2).unwrap(),
        ),
        (
            20_006,
            JobCommand::claim(
                20_006,
                AnalyticsJobId::new(703).unwrap(),
                1,
                9_999,
                1,
                submitted_at + 300_000,
            )
            .unwrap(),
        ),
    ] {
        meta.propose_analytics_job(Request::new(ProposeAnalyticsJobRequest {
            context: Some(context(request_id)),
            command: command.encode().unwrap(),
        }))
        .await
        .unwrap();
    }
    let meta_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let meta_address = meta_listener.local_addr().unwrap();
    let (meta_shutdown, meta_shutdown_rx) = tokio::sync::oneshot::channel();
    let meta_server = tokio::spawn(
        Server::builder()
            .add_service(MetaServiceServer::new(meta.clone()))
            .serve_with_incoming_shutdown(TcpListenerStream::new(meta_listener), async {
                let _ = meta_shutdown_rx.await;
            }),
    );

    let mut data_endpoints = BTreeMap::new();
    let mut data_shutdowns = Vec::new();
    let mut data_servers = Vec::new();
    let mut sidecars = BTreeMap::new();
    let mut data_hosts = BTreeMap::new();
    let raft_addresses = placements
        .iter()
        .flat_map(|placement| placement.voters().iter().copied())
        .map(|node_id| (node_id, free_address()))
        .collect::<BTreeMap<_, _>>();
    for placement in &placements {
        for &node_id in placement.voters() {
            let instance_id = format!(
                "gc-{}-{}-{node_id}-{}-{case_suffix}",
                backend.name(),
                mode_name(mode),
                live.suffix
            );
            let sidecar_address = free_address();
            let backend_root = temporary.path().join(format!("backend-{node_id}"));
            let sidecar =
                spawn_backend_sidecar(live, backend, &backend_root, &instance_id, sidecar_address)
                    .await;
            let profile = BackendProfile::new(
                "sidecar",
                instance_id,
                BTreeMap::from([
                    ("endpoint".into(), sidecar_address.to_string()),
                    ("pool_size".into(), "1".into()),
                ]),
                BTreeMap::new(),
            )
            .unwrap();
            let host = Arc::new(
                DataNodeHost::open(
                    data_config(&temporary.path().join(format!("data-{node_id}")), node_id),
                    32,
                )
                .await
                .unwrap(),
            );
            host.ensure_replica(
                ReplicaSpec::new_with_backend(
                    7,
                    placement.shard_id(),
                    placement.epoch(),
                    placement.voters().to_vec(),
                    ReplicaRole::Voter,
                    1,
                    BackendSlotState::active(1, profile).unwrap(),
                    format!("gc-shard-{}-node-{node_id}", placement.shard_id()),
                )
                .unwrap(),
            )
            .await
            .unwrap();
            let (address, shutdown, server) = serve_shard(Arc::clone(&host)).await;
            data_endpoints.insert(node_id, address);
            data_shutdowns.push(shutdown);
            data_servers.push(server);
            data_hosts.insert(node_id, host);
            sidecars.insert(node_id, sidecar);
        }
    }
    let mut raft_runtimes = Vec::new();
    for (&node_id, host) in &data_hosts {
        let peers = raft_addresses
            .iter()
            .filter(|(peer_id, _)| **peer_id != node_id)
            .map(|(&peer_id, &address)| (peer_id, address))
            .collect::<BTreeMap<_, _>>();
        raft_runtimes.push(
            DataRaftRuntime::start(
                Arc::clone(host),
                raft_addresses[&node_id],
                &peers,
                64,
                Duration::from_millis(10),
            )
            .await
            .unwrap(),
        );
    }
    for placement in &placements {
        let leader = placement.voters()[0];
        let key = ReplicaKey::new(7, placement.shard_id()).unwrap();
        data_hosts[&leader].campaign(key).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if data_hosts[&leader].status(key).await.unwrap().is_leader() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("Data Shard leader election for GC certification");
    }

    let router = GatewayCatalogRouter::new(
        CLUSTER_ID,
        31,
        7,
        vec![meta_address],
        data_endpoints,
        Duration::from_secs(5),
    )
    .unwrap();
    let snapshot = router.load(1).await.unwrap();
    let (_, authoritative_graph, topology) = snapshot.into_parts();
    let remote_client =
        Arc::new(RemoteShardClient::new_loopback_plaintext(CLUSTER_ID, topology.clone()).unwrap());
    let artifact_placement = placements.first().unwrap();
    for (offset, job_id) in [701_u128, 702, 703].into_iter().enumerate() {
        put_gc_pinned_result(
            remote_client.as_ref(),
            artifact_placement,
            job_id,
            21_000 + offset as u128 * 10,
        )
        .await;
    }

    let fault_fired = Arc::new(AtomicBool::new(false));
    let owner_client =
        Arc::new(RemoteShardClient::new_loopback_plaintext(CLUSTER_ID, topology.clone()).unwrap());
    let owner_gateway =
        RemoteGatewayService::new_at_revision_with_gateway_id_and_delay_and_fault_injector(
            backend.gateway_id() + 1_000 + u64::from(mode == DeploymentMode::SharedNothing),
            Duration::ZERO,
            Arc::new(ObservedProcessStop {
                point: fault_point,
                fired: Arc::clone(&fault_fired),
            }),
            CLUSTER_ID,
            1,
            authoritative_graph.clone(),
            owner_client,
            vec![meta_address],
            32,
            64,
        )
        .unwrap();
    tokio::time::timeout(Duration::from_secs(30), async {
        while !fault_fired.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("GC fault boundary {fault_point:?} was not reached"));

    let owner_metrics = owner_gateway.analytics_scheduler_metrics().unwrap();
    let after_fault =
        list_gc_generations(remote_client.as_ref(), artifact_placement, 701, 22_000).await;
    if fault_point == AnalyticsFaultPoint::GcBeforeAcknowledgement {
        assert!(after_fault.is_empty(), "ack fault must follow deletion");
        assert_eq!(owner_metrics.orphan_deleted, 1);
    } else {
        assert_eq!(
            after_fault.len(),
            1,
            "delete boundary must preserve generation"
        );
        assert_eq!(owner_metrics.orphan_deleted, 0);
    }
    assert!(!gc_tombstone_acknowledged(&meta, 22_100).await);
    drop(owner_gateway);

    let takeover_a_client =
        Arc::new(RemoteShardClient::new_loopback_plaintext(CLUSTER_ID, topology.clone()).unwrap());
    let takeover_b_client =
        Arc::new(RemoteShardClient::new_loopback_plaintext(CLUSTER_ID, topology).unwrap());
    let takeover_a = RemoteGatewayService::new_at_revision_with_gateway_id(
        backend.gateway_id() + 2_000 + u64::from(mode == DeploymentMode::SharedNothing),
        CLUSTER_ID,
        1,
        authoritative_graph.clone(),
        takeover_a_client,
        vec![meta_address],
        32,
        64,
    )
    .unwrap();
    let takeover_b = RemoteGatewayService::new_at_revision_with_gateway_id(
        backend.gateway_id() + 3_000 + u64::from(mode == DeploymentMode::SharedNothing),
        CLUSTER_ID,
        1,
        authoritative_graph,
        takeover_b_client,
        vec![meta_address],
        32,
        64,
    )
    .unwrap();
    tokio::time::timeout(Duration::from_secs(45), async {
        let mut request_id = 22_200_u128;
        loop {
            if gc_tombstone_acknowledged(&meta, request_id).await {
                break;
            }
            request_id += 1;
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("replacement Gateways did not acknowledge {fault_point:?}"));

    assert!(
        list_gc_generations(remote_client.as_ref(), artifact_placement, 701, 23_000,)
            .await
            .is_empty(),
        "tombstoned Artifact must be reclaimed"
    );
    for (offset, job_id) in [702_u128, 703].into_iter().enumerate() {
        assert_eq!(
            list_gc_generations(
                remote_client.as_ref(),
                artifact_placement,
                job_id,
                23_100 + offset as u128,
            )
            .await
            .len(),
            1,
            "Job {job_id} must remain fail-closed"
        );
    }
    let takeover_a_metrics = takeover_a.analytics_scheduler_metrics().unwrap();
    let takeover_b_metrics = takeover_b.analytics_scheduler_metrics().unwrap();
    assert!(
        takeover_a_metrics.gc_lease_conflicts + takeover_b_metrics.gc_lease_conflicts > 0,
        "at least one concurrent replacement Gateway must be rejected by Meta lease fencing"
    );
    assert_eq!(
        owner_metrics.orphan_deleted
            + takeover_a_metrics.orphan_deleted
            + takeover_b_metrics.orphan_deleted,
        1,
        "concurrent GC owners must record exactly one successful deletion"
    );
    assert_eq!(
        owner_metrics.orphan_delete_failures
            + takeover_a_metrics.orphan_delete_failures
            + takeover_b_metrics.orphan_delete_failures,
        0,
        "lease fencing must prevent duplicate delete attempts"
    );
    drop(takeover_a);
    drop(takeover_b);

    for shutdown in data_shutdowns {
        shutdown.send(()).unwrap();
    }
    for server in data_servers {
        server.await.unwrap().unwrap();
    }
    for runtime in raft_runtimes {
        runtime.shutdown().await.unwrap();
    }
    for host in data_hosts.into_values() {
        Arc::try_unwrap(host)
            .unwrap_or_else(|_| panic!("GC DataNodeHost is still retained"))
            .shutdown()
            .await
            .unwrap();
    }
    meta_shutdown.send(()).unwrap();
    meta_server.await.unwrap().unwrap();
    for sidecar in sidecars.into_values() {
        sidecar.shutdown().unwrap();
    }
}

fn gc_job(job_id: u128, submission_request_id: u128) -> JobSpec {
    JobSpec::new(
        AnalyticsJobId::new(job_id).unwrap(),
        submission_request_id,
        7,
        1,
        1,
        1,
        1,
        TransactionTime::new(1_000, 0),
        GraphProjectionScope::Snapshot {
            valid_time: ValidTime::from_micros(900),
        },
        "dtg.graph.degree",
        "1.0.0",
        "dtg.analytics-native",
        "1.0.0",
        Vec::new(),
        [9; 32],
        ProjectionLimits::new(100, 100, 1 << 20).unwrap(),
    )
    .unwrap()
}

async fn put_gc_pinned_result(
    client: &RemoteShardClient,
    placement: &Placement,
    job_id: u128,
    request_seed: u128,
) {
    let payload = format!("gc-result-{job_id}").into_bytes();
    let digest = *blake3::hash(&payload).as_bytes();
    client
        .put_artifact_chunk(
            PutArtifactChunkRequest::new(
                ShardRequestContext::new(
                    7,
                    placement.shard_id(),
                    placement.epoch(),
                    request_seed,
                    now_ms() + 60_000,
                )
                .unwrap(),
                job_id,
                ArtifactKind::Result,
                1,
                now_ms() - 3_700_000,
                0,
                [0; 32],
                payload.clone(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    client
        .pin_artifact_generation(
            PinArtifactGenerationRequest::new(
                ShardRequestContext::new(
                    7,
                    placement.shard_id(),
                    placement.epoch(),
                    request_seed + 1,
                    now_ms() + 60_000,
                )
                .unwrap(),
                job_id,
                ArtifactKind::Result,
                1,
                1,
                u64::try_from(payload.len()).unwrap(),
                digest,
            )
            .unwrap(),
        )
        .await
        .unwrap();
}

async fn list_gc_generations(
    client: &RemoteShardClient,
    placement: &Placement,
    job_id: u128,
    request_id: u128,
) -> Vec<shard_client::ArtifactGenerationSummary> {
    client
        .list_artifact_generations(
            ListArtifactGenerationsRequest::new(
                ShardRequestContext::new(
                    7,
                    placement.shard_id(),
                    placement.epoch(),
                    request_id,
                    now_ms() + 60_000,
                )
                .unwrap(),
                job_id,
                ArtifactKind::Result,
                16,
            )
            .unwrap(),
        )
        .await
        .unwrap()
}

async fn gc_tombstone_acknowledged(meta: &MetaNodeService, request_id: u128) -> bool {
    let page = meta
        .list_analytics_job_tombstones(Request::new(ListAnalyticsJobTombstonesRequest {
            context: Some(context(request_id)),
            after_job_id: Vec::new(),
            limit: 8,
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(page.tombstones.len(), 1);
    let (_, _, tombstone) = LedgerState::decode_tombstone(&page.tombstones[0].record).unwrap();
    tombstone.artifacts_reclaimed()
}

async fn run_surface(
    live: &LiveConfiguration,
    backend: Backend,
    mode: DeploymentMode,
    takeover: Option<(&str, AnalyticsFaultPoint, bool)>,
    sidecar_recovery: SidecarRecovery,
) -> SurfaceResult {
    let case_suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("wall clock after Unix epoch")
        .as_nanos();
    eprintln!(
        "three-backend certification case: backend={} mode={} takeover={takeover:?} sidecar_recovery={sidecar_recovery:?}",
        backend.name(),
        mode_name(mode)
    );
    let temporary = tempfile::tempdir().expect("temporary fixture");
    let placements = match mode {
        DeploymentMode::PrimaryReplica => vec![Placement::new(10, 1, vec![10, 11]).unwrap()],
        DeploymentMode::SharedNothing => vec![
            Placement::new(10, 1, vec![10]).unwrap(),
            Placement::new(20, 1, vec![20]).unwrap(),
        ],
    };
    let graph = GraphDefinition::new(
        7,
        "analytics",
        1,
        TopologyDefinition::new(mode, 99, 128, 1, placements.clone()).unwrap(),
        CatalogBackendProfile::new(
            backend.name(),
            BTreeMap::new(),
            BTreeMap::new(),
            AdapterRequirement::HotPluggableReplica,
            1,
        )
        .unwrap(),
    )
    .unwrap();

    let meta = elected_meta(&temporary.path().join("meta"));
    meta.propose(Request::new(ProposeRequest {
        context: Some(context(101)),
        command: CatalogCommand::create_graph(101, 0, graph)
            .encode()
            .unwrap(),
    }))
    .await
    .unwrap();
    let meta_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let meta_address = meta_listener.local_addr().unwrap();
    let (meta_shutdown, meta_shutdown_rx) = tokio::sync::oneshot::channel();
    let meta_server = tokio::spawn(
        Server::builder()
            .add_service(MetaServiceServer::new(meta))
            .serve_with_incoming_shutdown(TcpListenerStream::new(meta_listener), async {
                let _ = meta_shutdown_rx.await;
            }),
    );

    let mut data_endpoints = BTreeMap::new();
    let mut data_shutdowns = Vec::new();
    let mut data_servers = Vec::new();
    let mut sidecars = BTreeMap::new();
    let mut sidecar_specs = BTreeMap::new();
    let mut data_hosts = BTreeMap::new();
    let raft_addresses = placements
        .iter()
        .flat_map(|placement| placement.voters().iter().copied())
        .map(|node_id| (node_id, free_address()))
        .collect::<BTreeMap<_, _>>();
    for placement in &placements {
        for &node_id in placement.voters() {
            let instance_id = format!(
                "deploy-{}-{}-{node_id}-{}-{case_suffix}",
                backend.name(),
                mode_name(mode),
                live.suffix
            );
            let sidecar_address = free_address();
            let backend_root = temporary.path().join(format!("backend-{node_id}"));
            let sidecar =
                spawn_backend_sidecar(live, backend, &backend_root, &instance_id, sidecar_address)
                    .await;
            let profile = BackendProfile::new(
                "sidecar",
                instance_id.clone(),
                BTreeMap::from([
                    ("endpoint".into(), sidecar_address.to_string()),
                    ("pool_size".into(), "1".into()),
                ]),
                BTreeMap::new(),
            )
            .unwrap();
            let host = Arc::new(
                DataNodeHost::open(
                    data_config(&temporary.path().join(format!("data-{node_id}")), node_id),
                    32,
                )
                .await
                .unwrap(),
            );
            host.ensure_replica(
                ReplicaSpec::new_with_backend(
                    7,
                    placement.shard_id(),
                    placement.epoch(),
                    placement.voters().to_vec(),
                    ReplicaRole::Voter,
                    1,
                    BackendSlotState::active(1, profile).unwrap(),
                    format!("shard-{}-node-{node_id}", placement.shard_id()),
                )
                .unwrap(),
            )
            .await
            .unwrap();
            let (address, shutdown, server) = serve_shard(Arc::clone(&host)).await;
            data_endpoints.insert(node_id, address);
            data_shutdowns.push(shutdown);
            data_servers.push(server);
            data_hosts.insert(node_id, host);
            sidecar_specs.insert(node_id, (backend_root, instance_id, sidecar_address));
            sidecars.insert(node_id, sidecar);
        }
    }
    let mut raft_runtimes = Vec::new();
    for (&node_id, host) in &data_hosts {
        let peers = raft_addresses
            .iter()
            .filter(|(peer_id, _)| **peer_id != node_id)
            .map(|(&peer_id, &address)| (peer_id, address))
            .collect::<BTreeMap<_, _>>();
        raft_runtimes.push(
            DataRaftRuntime::start(
                Arc::clone(host),
                raft_addresses[&node_id],
                &peers,
                64,
                Duration::from_millis(10),
            )
            .await
            .unwrap(),
        );
    }
    for placement in &placements {
        let leader = placement.voters()[0];
        let key = ReplicaKey::new(7, placement.shard_id()).unwrap();
        data_hosts[&leader].campaign(key).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if data_hosts[&leader].status(key).await.unwrap().is_leader() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("Data Shard leader election");
    }

    let router = GatewayCatalogRouter::new(
        CLUSTER_ID,
        31,
        7,
        vec![meta_address],
        data_endpoints,
        Duration::from_secs(5),
    )
    .unwrap();
    let snapshot = router.load(1).await.unwrap();
    let (_, authoritative_graph, topology) = snapshot.into_parts();
    let deployment = DeploymentConfig::from_catalog(&authoritative_graph).unwrap();
    let remote_client =
        Arc::new(RemoteShardClient::new_loopback_plaintext(CLUSTER_ID, topology).unwrap());
    let gateway_id =
        backend.gateway_id() + u64::from(matches!(mode, DeploymentMode::SharedNothing));
    let gateway = if let Some((_, fault_point, _)) = takeover {
        let injector: Arc<dyn AnalyticsFaultInjector> = Arc::new(FailOnceProcessStop {
            point: fault_point,
            fired: AtomicBool::new(false),
        });
        RemoteGatewayService::new_at_revision_with_gateway_id_and_delay_and_fault_injector(
            gateway_id,
            Duration::from_secs(6),
            injector,
            CLUSTER_ID,
            1,
            authoritative_graph.clone(),
            Arc::clone(&remote_client),
            vec![meta_address],
            32,
            64,
        )
    } else {
        RemoteGatewayService::new_at_revision_with_gateway_id_and_delay(
            gateway_id,
            if sidecar_recovery != SidecarRecovery::None {
                Duration::from_secs(2)
            } else {
                Duration::from_millis(500)
            },
            CLUSTER_ID,
            1,
            authoritative_graph.clone(),
            Arc::clone(&remote_client),
            vec![meta_address],
            32,
            64,
        )
    }
    .unwrap();
    let transaction = submit_gateway(
        &gateway,
        501,
        temporal_transaction_request(mode, &deployment),
    )
    .await;
    assert_eq!(transaction["ok"], true, "{transaction}");
    match mode {
        DeploymentMode::PrimaryReplica => {
            assert_eq!(transaction["result"]["single_shard_fast_path"], true);
            assert_eq!(
                transaction["result"]["participants"]
                    .as_array()
                    .unwrap()
                    .len(),
                1
            );
        }
        DeploymentMode::SharedNothing => {
            assert_eq!(transaction["result"]["single_shard_fast_path"], false);
            assert_eq!(
                transaction["result"]["participants"]
                    .as_array()
                    .unwrap()
                    .len(),
                2
            );
        }
    }
    let bolt_service = Arc::new(CypherBoltService::new(Arc::new(gateway), 16).unwrap());
    let mut bolt = BoltMachine::new(bolt_service);
    assert!(matches!(
        bolt.handle(ClientMessage::Hello(BTreeMap::new()))
            .await
            .as_slice(),
        [ServerMessage::Success(_)]
    ));
    let transaction_rows = execute_query(
        &mut bolt,
        "USE analytics FOR VALID_TIME AS OF 1000 MATCH (n:TransactionSeed) \
         RETURN count(n) AS total",
    )
    .await;
    assert_eq!(transaction_rows, vec![vec![Value::Integer(2)]]);
    execute_write(
        &mut bolt,
        "USE analytics FOR VALID_TIME AS OF 1000 \
         MERGE (a:Person {fixtureId: 'person-a'})-[:KNOWS]->\
               (b:Person {fixtureId: 'person-b'})",
    )
    .await;
    let node_count = execute_query(
        &mut bolt,
        "USE analytics FOR VALID_TIME AS OF 1000 MATCH (n) RETURN count(n) AS total",
    )
    .await;
    let degree = execute_query(
        &mut bolt,
        "USE analytics FOR VALID_TIME AS OF 1000 CALL dtg.graph.degree({}) \
         YIELD degree RETURN degree ORDER BY degree",
    )
    .await;
    let interval = execute_query(
        &mut bolt,
        "USE analytics FOR VALID_TIME BETWEEN 1000 AND 2000 \
         CALL dtg.temporal.intervalComponents({}) \
         YIELD componentId RETURN componentId ORDER BY componentId",
    )
    .await;
    execute_write(
        &mut bolt,
        "USE analytics FOR VALID_TIME AS OF 2000 \
         MERGE (:DeltaOnly {fixtureId: 'delta-only'})",
    )
    .await;
    let delta = execute_query(
        &mut bolt,
        "USE analytics FOR VALID_TIME BETWEEN 1000 AND 2000 \
         CALL dtg.temporal.deltaSummary({}) \
         YIELD entityType, change, count \
         RETURN entityType, change, count ORDER BY entityType, change",
    )
    .await;
    let pagination = execute_paginated_query(
        &mut bolt,
        "USE analytics FOR VALID_TIME AS OF 1000 CALL dtg.graph.degree({}) \
         YIELD degree RETURN degree ORDER BY degree",
    )
    .await;
    let canonical_error = execute_failure(
        &mut bolt,
        "USE analytics FOR VALID_TIME AS OF 1000 MATCH (n) RETURN missing",
    )
    .await;

    let mut async_result_artifacts = BTreeMap::new();
    if let Some((algorithm, _, restart_same_gateway)) = takeover {
        let takeover_gateway = RemoteGatewayService::new_at_revision_with_gateway_id(
            if restart_same_gateway {
                gateway_id
            } else {
                gateway_id + 100
            },
            CLUSTER_ID,
            1,
            authoritative_graph,
            Arc::clone(&remote_client),
            vec![meta_address],
            32,
            64,
        )
        .unwrap();
        let takeover_service =
            Arc::new(CypherBoltService::new(Arc::new(takeover_gateway), 16).unwrap());
        let mut takeover_bolt = BoltMachine::new(takeover_service);
        assert!(matches!(
            takeover_bolt
                .handle(ClientMessage::Hello(BTreeMap::new()))
                .await
                .as_slice(),
            [ServerMessage::Success(_)]
        ));
        let job_id = submit_async_algorithm(&mut bolt, algorithm).await;
        tokio::time::sleep(Duration::from_millis(500)).await;
        drop(bolt);
        tokio::time::sleep(Duration::from_millis(250)).await;
        wait_async_algorithm(&mut takeover_bolt, &job_id, algorithm).await;
        async_result_artifacts.insert(
            algorithm.to_owned(),
            read_result_artifact(
                remote_client.as_ref(),
                placements.first().expect("storage Shard placement"),
                &job_id,
            )
            .await,
        );
    } else if sidecar_recovery != SidecarRecovery::None {
        let algorithm = "dtg.graph.degree";
        let job_id = submit_async_algorithm(&mut bolt, algorithm).await;
        wait_for_analytics_state(&mut bolt, &job_id, "RUNNING").await;
        let stopped = sidecars
            .remove(&10)
            .expect("storage Shard Sidecar must be running");
        tokio::task::spawn_blocking(move || stopped.shutdown())
            .await
            .expect("Sidecar shutdown task")
            .expect("Sidecar shutdown");
        tokio::time::sleep(Duration::from_secs(3)).await;
        let (state, error) = analytics_status(&mut bolt, &job_id).await;
        assert_ne!(
            state, "FAILED",
            "{backend:?} {mode:?} Sidecar outage became terminal: {error:?}"
        );
        let (backend_root, instance_id, sidecar_address) = sidecar_specs
            .get(&10)
            .expect("storage Sidecar spec")
            .clone();
        match sidecar_recovery {
            SidecarRecovery::SameGateway => {
                let restarted = spawn_backend_sidecar(
                    live,
                    backend,
                    &backend_root,
                    &instance_id,
                    sidecar_address,
                )
                .await;
                sidecars.insert(10, restarted);
                wait_async_algorithm(&mut bolt, &job_id, algorithm).await;
                drop(bolt);
            }
            SidecarRecovery::CrossGateway => {
                drop(bolt);
                tokio::time::sleep(Duration::from_millis(250)).await;
                let restarted = spawn_backend_sidecar(
                    live,
                    backend,
                    &backend_root,
                    &instance_id,
                    sidecar_address,
                )
                .await;
                sidecars.insert(10, restarted);
                let takeover_gateway = RemoteGatewayService::new_at_revision_with_gateway_id(
                    gateway_id + 100,
                    CLUSTER_ID,
                    1,
                    authoritative_graph,
                    Arc::clone(&remote_client),
                    vec![meta_address],
                    32,
                    64,
                )
                .unwrap();
                let takeover_service =
                    Arc::new(CypherBoltService::new(Arc::new(takeover_gateway), 16).unwrap());
                let mut takeover_bolt = BoltMachine::new(takeover_service);
                assert!(matches!(
                    takeover_bolt
                        .handle(ClientMessage::Hello(BTreeMap::new()))
                        .await
                        .as_slice(),
                    [ServerMessage::Success(_)]
                ));
                wait_async_algorithm(&mut takeover_bolt, &job_id, algorithm).await;
            }
            SidecarRecovery::None => unreachable!("Sidecar recovery branch requires a scenario"),
        }
        async_result_artifacts.insert(
            algorithm.to_owned(),
            read_result_artifact(
                remote_client.as_ref(),
                placements.first().expect("storage Shard placement"),
                &job_id,
            )
            .await,
        );
    } else {
        for algorithm in RESUMABLE_ALGORITHMS {
            let job_id = submit_async_algorithm(&mut bolt, algorithm).await;
            wait_async_algorithm(&mut bolt, &job_id, algorithm).await;
            async_result_artifacts.insert(
                algorithm.to_owned(),
                read_result_artifact(
                    remote_client.as_ref(),
                    placements.first().expect("storage Shard placement"),
                    &job_id,
                )
                .await,
            );
        }
        verify_async_degree_cancel(&mut bolt).await;
        drop(bolt);
    }

    if mode == DeploymentMode::PrimaryReplica {
        let key = ReplicaKey::new(7, 10).unwrap();
        let convergence = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let leader = data_hosts[&10].status(key).await.unwrap();
                let follower = data_hosts[&11].status(key).await.unwrap();
                if leader.applied_index() > 0 && leader.applied_index() == follower.applied_index()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        if convergence.is_err() {
            let leader = data_hosts[&10].status(key).await.unwrap();
            let follower = data_hosts[&11].status(key).await.unwrap();
            panic!(
                "PrimaryReplica follower did not converge: leader applied={}, follower applied={}",
                leader.applied_index(),
                follower.applied_index()
            );
        }
    }

    for shutdown in data_shutdowns {
        shutdown.send(()).unwrap();
    }
    for server in data_servers {
        server.await.unwrap().unwrap();
    }
    for runtime in raft_runtimes {
        runtime.shutdown().await.unwrap();
    }
    for host in data_hosts.into_values() {
        Arc::try_unwrap(host)
            .unwrap_or_else(|_| panic!("DataNodeHost is still retained"))
            .shutdown()
            .await
            .unwrap();
    }
    meta_shutdown.send(()).unwrap();
    meta_server.await.unwrap().unwrap();
    for sidecar in sidecars.into_values() {
        sidecar.shutdown().unwrap();
    }

    SurfaceResult {
        transaction_rows,
        node_count,
        degree,
        interval,
        delta,
        pagination,
        canonical_error,
        async_result_artifacts,
    }
}

fn assert_surface_semantics(result: &SurfaceResult) {
    assert_eq!(result.transaction_rows, vec![vec![Value::Integer(2)]]);
    assert_eq!(result.node_count, vec![vec![Value::Integer(4)]]);
    assert_eq!(
        result.degree,
        vec![
            vec![Value::Integer(0)],
            vec![Value::Integer(0)],
            vec![Value::Integer(1)],
            vec![Value::Integer(1)],
        ]
    );
    assert_eq!(
        result.delta,
        vec![vec![
            Value::String("VERTEX".into()),
            Value::String("ADDED".into()),
            Value::Integer(1),
        ]]
    );
    assert_eq!(
        result.pagination,
        PaginationResult {
            first: vec![vec![Value::Integer(0)], vec![Value::Integer(0)]],
            second: vec![vec![Value::Integer(1)], vec![Value::Integer(1)]],
            first_has_more: true,
            second_has_more: false,
        }
    );
    assert_eq!(
        result.canonical_error.code,
        "Neo.ClientError.Statement.ExecutionFailed"
    );
    assert!(
        result
            .canonical_error
            .message
            .contains("DTG-CYPHER-UNBOUND-VARIABLE")
    );
    assert!(result.canonical_error.message.contains("missing"));
    let component_ids = result
        .interval
        .iter()
        .map(|row| match row.as_slice() {
            [Value::String(component)] => component.as_str(),
            other => panic!("invalid interval component row: {other:?}"),
        })
        .collect::<Vec<_>>();
    assert_eq!(component_ids.len(), 4);
    assert_eq!(
        component_ids
            .iter()
            .filter(|component| **component == "1001")
            .count(),
        1
    );
    assert_eq!(
        component_ids
            .iter()
            .filter(|component| **component == "1002")
            .count(),
        1
    );
    let connected = component_ids
        .iter()
        .filter(|component| **component != "1001" && **component != "1002")
        .copied()
        .collect::<Vec<_>>();
    assert_eq!(connected.len(), 2);
    assert_eq!(connected[0], connected[1]);
}

fn temporal_transaction_request(
    mode: DeploymentMode,
    deployment: &DeploymentConfig,
) -> GatewayRequest {
    let partitions = match mode {
        DeploymentMode::PrimaryReplica => [0, 1],
        DeploymentMode::SharedNothing => {
            let routed =
                (0..128)
                    .map(PartitionId::new)
                    .fold(BTreeMap::new(), |mut by_shard, partition| {
                        let shard = deployment
                            .route_scope(GraphScope::new(GraphId::new(7), partition))
                            .shard_id();
                        by_shard.entry(shard).or_insert(partition.value());
                        by_shard
                    });
            let mut values = routed.values().copied();
            [
                values.next().expect("first Shared-Nothing Shard"),
                values.next().expect("second Shared-Nothing Shard"),
            ]
        }
    };
    let payload = |name: &str| {
        hex(&CanonicalElement::new(
            1,
            BTreeMap::from([(schema_id("name"), GraphValue::String(name.to_owned()))]),
        )
        .encode()
        .unwrap())
    };
    GatewayRequest {
        version: GATEWAY_API_VERSION,
        request_id: format!("{}-temporal-transaction", mode_name(mode)),
        operation: GatewayOperation::Transaction {
            schema_version: 1,
            ttl_micros: 60_000_000,
            mutations: vec![
                ApiMutation::PutVertex {
                    partition: partitions[0],
                    vertex_id: "1001".into(),
                    label_id: schema_id("TransactionSeed"),
                    valid_from_micros: 0,
                    valid_to_micros: None,
                    payload_dtp1: payload("Seed-A"),
                },
                ApiMutation::PutVertex {
                    partition: partitions[1],
                    vertex_id: "1002".into(),
                    label_id: schema_id("TransactionSeed"),
                    valid_from_micros: 0,
                    valid_to_micros: None,
                    payload_dtp1: payload("Seed-B"),
                },
            ],
        },
    }
}

async fn submit_gateway(
    gateway: &RemoteGatewayService,
    request_id: u128,
    request: GatewayRequest,
) -> serde_json::Value {
    let response = GatewayService::submit(
        gateway,
        Request::new(GatewaySubmitRequest {
            context: Some(context(request_id)),
            request_json: serde_json::to_vec(&request).unwrap(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    serde_json::from_slice(&response.response_json).unwrap()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

async fn spawn_backend_sidecar(
    live: &LiveConfiguration,
    backend: Backend,
    root: &Path,
    instance_id: &str,
    bind_address: SocketAddr,
) -> TcpSidecarServerHandle {
    let root = root.to_path_buf();
    let postgres_url = live.postgres_url.clone();
    let neo4j_endpoint = live.neo4j_endpoint.clone();
    let neo4j_database = live.neo4j_database.clone();
    let neo4j_username = live.neo4j_username.clone();
    let neo4j_password = live.neo4j_password.clone();
    let instance_id = instance_id.to_owned();
    tokio::task::spawn_blocking(move || {
        let mapping: Arc<dyn TemporalBackendMapping> = match backend {
            Backend::RocksDb => Arc::new(RocksAdapter::open(root).unwrap()),
            Backend::PostgreSql => Arc::new(
                PostgresAdapter::open(postgres_url, instance_id, 2)
                    .expect("open PostgreSQL deployment backend"),
            ),
            Backend::Neo4j => Neo4jAdapterFactory::open_mapping(
                &neo4j_endpoint,
                &neo4j_database,
                &neo4j_username,
                &neo4j_password,
                &instance_id,
            )
            .expect("open Neo4j deployment backend"),
        };
        let adapter: Arc<dyn StorageAdapter> = Arc::new(
            MappingBackedAdapter::new(mapping, MappingRequirement::HotPluggableReplica).unwrap(),
        );
        spawn_stateful_tcp_sidecar_server(
            TcpSidecarServerConfig::new(bind_address),
            Arc::new(SidecarService::new(adapter, None)),
        )
        .unwrap()
    })
    .await
    .expect("backend Sidecar initialization task")
}

async fn execute_write<S>(bolt: &mut BoltMachine<S>, query: &str)
where
    S: BoltService,
{
    let run = bolt
        .handle(ClientMessage::Run {
            query: query.into(),
            parameters: BTreeMap::new(),
            extra: BTreeMap::new(),
        })
        .await;
    assert!(
        matches!(run.as_slice(), [ServerMessage::Success(_)]),
        "write failed: {run:?}"
    );
    let discard = bolt
        .handle(ClientMessage::Discard {
            n: -1,
            query_id: None,
        })
        .await;
    assert!(
        discard
            .iter()
            .any(|message| matches!(message, ServerMessage::Success(_))),
        "write discard failed: {discard:?}"
    );
}

async fn execute_query<S>(bolt: &mut BoltMachine<S>, query: &str) -> Vec<Vec<Value>>
where
    S: BoltService,
{
    let run = bolt
        .handle(ClientMessage::Run {
            query: query.into(),
            parameters: BTreeMap::new(),
            extra: BTreeMap::new(),
        })
        .await;
    assert!(
        matches!(run.as_slice(), [ServerMessage::Success(_)]),
        "query failed: {run:?}"
    );
    let pull = bolt
        .handle(ClientMessage::Pull {
            n: -1,
            query_id: None,
        })
        .await;
    assert!(
        pull.iter()
            .any(|message| matches!(message, ServerMessage::Success(_))),
        "query pull failed: {pull:?}"
    );
    pull.into_iter()
        .filter_map(|message| match message {
            ServerMessage::Record(values) => Some(values),
            _ => None,
        })
        .collect()
}

async fn submit_async_degree<S>(bolt: &mut BoltMachine<S>) -> String
where
    S: BoltService,
{
    submit_async_algorithm(bolt, "dtg.graph.degree").await
}

async fn submit_async_algorithm<S>(bolt: &mut BoltMachine<S>, algorithm: &str) -> String
where
    S: BoltService,
{
    let parameters = if algorithm == "dtg.graph.pageRank" {
        "{maxIterations: 4, tolerance: 0.000000000000000000000000000001}"
    } else {
        "{}"
    };
    let run = bolt
        .handle(ClientMessage::Run {
            query: format!(
                "USE analytics FOR VALID_TIME AS OF 1000 \
                 CALL dtg.analytics.submit({{algorithm: '{algorithm}', parameters: {parameters}}}) \
                 YIELD jobId RETURN jobId"
            ),
            parameters: BTreeMap::new(),
            extra: BTreeMap::new(),
        })
        .await;
    assert!(
        matches!(run.as_slice(), [ServerMessage::Success(_)]),
        "async submit failed: {run:?}"
    );
    let pulled = bolt
        .handle(ClientMessage::Pull {
            n: -1,
            query_id: None,
        })
        .await;
    pulled
        .iter()
        .find_map(|message| match message {
            ServerMessage::Record(values) if values.len() == 1 => match &values[0] {
                Value::String(job_id) => Some(job_id.clone()),
                _ => None,
            },
            _ => None,
        })
        .unwrap_or_else(|| panic!("async {algorithm} submit must return a job ID"))
}

async fn wait_async_algorithm<S>(bolt: &mut BoltMachine<S>, job_id: &str, algorithm: &str)
where
    S: BoltService,
{
    let mut succeeded = false;
    let mut last_state = None;
    for _ in 0..240 {
        let run = bolt
            .handle(ClientMessage::Run {
                query: format!(
                    "CALL dtg.analytics.status({{jobId: '{job_id}'}}) \
                     YIELD state, error RETURN state, error"
                ),
                parameters: BTreeMap::new(),
                extra: BTreeMap::new(),
            })
            .await;
        assert!(
            matches!(run.as_slice(), [ServerMessage::Success(_)]),
            "async status failed: {run:?}"
        );
        let pulled = bolt
            .handle(ClientMessage::Pull {
                n: -1,
                query_id: None,
            })
            .await;
        let (state, error) = pulled
            .iter()
            .find_map(|message| match message {
                ServerMessage::Record(values) if values.len() == 2 => {
                    let Value::String(state) = &values[0] else {
                        return None;
                    };
                    let error = match &values[1] {
                        Value::String(error) => Some(error.as_str()),
                        Value::Null => None,
                        _ => return None,
                    };
                    Some((Some(state.as_str()), error))
                }
                _ => None,
            })
            .unwrap_or((None, None));
        last_state = Some(error.map_or_else(
            || state.unwrap_or("UNKNOWN").to_owned(),
            |error| format!("{}: {error}", state.unwrap_or("UNKNOWN")),
        ));
        if state == Some("SUCCEEDED") {
            succeeded = true;
            break;
        }
        assert_ne!(state, Some("FAILED"), "async {algorithm} failed: {error:?}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        succeeded,
        "async {algorithm} did not reach SUCCEEDED; last state: {last_state:?}"
    );
    for _ in 0..3 {
        let run = bolt
            .handle(ClientMessage::Run {
                query: format!(
                    "CALL dtg.analytics.status({{jobId: '{job_id}'}}) YIELD state RETURN state"
                ),
                parameters: BTreeMap::new(),
                extra: BTreeMap::new(),
            })
            .await;
        assert!(
            matches!(run.as_slice(), [ServerMessage::Success(_)]),
            "terminal status re-read failed: {run:?}"
        );
        let pulled = bolt
            .handle(ClientMessage::Pull {
                n: -1,
                query_id: None,
            })
            .await;
        assert!(
            pulled.iter().any(|message| {
                matches!(message, ServerMessage::Record(values) if values == &vec![Value::String("SUCCEEDED".into())])
            }),
            "terminal state changed after success: {pulled:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let run = bolt
        .handle(ClientMessage::Run {
            query: format!(
                "CALL dtg.analytics.results({{jobId: '{job_id}', offset: 0, limit: 8}}) \
                 YIELD row RETURN row"
            ),
            parameters: BTreeMap::new(),
            extra: BTreeMap::new(),
        })
        .await;
    assert!(
        matches!(run.as_slice(), [ServerMessage::Success(_)]),
        "async results failed: {run:?}"
    );
    let pulled = bolt
        .handle(ClientMessage::Pull {
            n: -1,
            query_id: None,
        })
        .await;
    assert!(
        pulled
            .iter()
            .any(|message| matches!(message, ServerMessage::Record(_))),
        "async {algorithm} results returned no rows: {pulled:?}"
    );
}

async fn analytics_status<S>(bolt: &mut BoltMachine<S>, job_id: &str) -> (String, Option<String>)
where
    S: BoltService,
{
    let status = bolt
        .handle(ClientMessage::Run {
            query: format!(
                "CALL dtg.analytics.status({{jobId: '{job_id}'}}) YIELD state, error RETURN state, error"
            ),
            parameters: BTreeMap::new(),
            extra: BTreeMap::new(),
        })
        .await;
    assert!(
        matches!(status.as_slice(), [ServerMessage::Success(_)]),
        "analytics status failed: {status:?}"
    );
    let status = bolt
        .handle(ClientMessage::Pull {
            n: -1,
            query_id: None,
        })
        .await;
    status
        .iter()
        .find_map(|message| match message {
            ServerMessage::Record(values) => match values.as_slice() {
                [Value::String(state), Value::String(error)] => {
                    Some((state.clone(), Some(error.clone())))
                }
                [Value::String(state), Value::Null] => Some((state.clone(), None)),
                _ => None,
            },
            _ => None,
        })
        .expect("analytics status returned one state")
}

async fn wait_for_analytics_state<S>(bolt: &mut BoltMachine<S>, job_id: &str, expected: &str)
where
    S: BoltService,
{
    let mut last = String::new();
    for _ in 0..200 {
        let (state, error) = analytics_status(bolt, job_id).await;
        last = state;
        if last == expected {
            return;
        }
        assert_ne!(
            last, "FAILED",
            "analytics Job failed before {expected}: {error:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("analytics Job did not reach {expected}; last state: {last}");
}

async fn read_result_artifact(
    client: &RemoteShardClient,
    placement: &Placement,
    job_id: &str,
) -> Vec<u8> {
    let job_id = u128::from_str_radix(job_id, 16).expect("canonical analytics job ID");
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("wall clock after Unix epoch");
    let request_seed = now.as_nanos() ^ job_id;
    let deadline = u64::try_from(now.as_millis())
        .expect("current Unix time fits u64")
        .saturating_add(10_000);
    let list_context = ShardRequestContext::new(
        7,
        placement.shard_id(),
        placement.epoch(),
        request_seed.max(1),
        deadline,
    )
    .unwrap();
    let summaries = client
        .list_artifact_generations(
            ListArtifactGenerationsRequest::new(list_context, job_id, ArtifactKind::Result, 4_096)
                .unwrap(),
        )
        .await
        .unwrap();
    let pinned = summaries
        .iter()
        .filter(|summary| summary.pinned())
        .collect::<Vec<_>>();
    assert_eq!(
        pinned.len(),
        1,
        "successful analytics job must have exactly one pinned Result Artifact"
    );
    let summary = pinned[0];
    let get_context = ShardRequestContext::new(
        7,
        placement.shard_id(),
        placement.epoch(),
        request_seed.saturating_add(1).max(1),
        deadline,
    )
    .unwrap();
    let mut stream = client
        .get_artifact_generation(
            GetArtifactGenerationRequest::new(
                get_context,
                job_id,
                ArtifactKind::Result,
                summary.generation(),
                summary.expected_chunk_count(),
                summary.expected_total_bytes(),
                summary.expected_content_digest(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        bytes.extend_from_slice(chunk.unwrap().payload());
    }
    assert_eq!(
        u64::try_from(bytes.len()).unwrap(),
        summary.expected_total_bytes(),
        "Result Artifact byte count"
    );
    assert_eq!(
        *blake3::hash(&bytes).as_bytes(),
        summary.expected_content_digest(),
        "Result Artifact content digest"
    );
    bytes
}

async fn verify_async_degree_cancel<S>(bolt: &mut BoltMachine<S>)
where
    S: BoltService,
{
    for _ in 0..8 {
        let job_id = submit_async_degree(bolt).await;
        let run = bolt
            .handle(ClientMessage::Run {
                query: format!(
                    "CALL dtg.analytics.cancel({{jobId: '{job_id}'}}) \
                     YIELD canceled RETURN canceled"
                ),
                parameters: BTreeMap::new(),
                extra: BTreeMap::new(),
            })
            .await;
        if matches!(run.as_slice(), [ServerMessage::Failure { message, .. }] if message.contains("DTG-ANALYTICS-JOB-FINAL"))
        {
            assert!(matches!(
                bolt.handle(ClientMessage::Reset).await.as_slice(),
                [ServerMessage::Success(_)]
            ));
            continue;
        }
        assert!(
            matches!(run.as_slice(), [ServerMessage::Success(_)]),
            "async cancel failed: {run:?}"
        );
        let pulled = bolt
            .handle(ClientMessage::Pull {
                n: -1,
                query_id: None,
            })
            .await;
        assert!(
            pulled.iter().any(|message| {
                matches!(message, ServerMessage::Record(values) if values == &vec![Value::Boolean(true)])
            }),
            "async cancel did not return true: {pulled:?}"
        );
        return;
    }
    panic!("async Degree completed before cancellation in all retry attempts");
}

async fn execute_paginated_query<S>(bolt: &mut BoltMachine<S>, query: &str) -> PaginationResult
where
    S: BoltService,
{
    let run = bolt
        .handle(ClientMessage::Run {
            query: query.into(),
            parameters: BTreeMap::new(),
            extra: BTreeMap::new(),
        })
        .await;
    assert!(
        matches!(run.as_slice(), [ServerMessage::Success(_)]),
        "paginated query failed: {run:?}"
    );
    let first = bolt
        .handle(ClientMessage::Pull {
            n: 2,
            query_id: None,
        })
        .await;
    let second = bolt
        .handle(ClientMessage::Pull {
            n: 2,
            query_id: None,
        })
        .await;
    PaginationResult {
        first: records(&first),
        second: records(&second),
        first_has_more: has_more(&first),
        second_has_more: has_more(&second),
    }
}

async fn execute_failure<S>(bolt: &mut BoltMachine<S>, query: &str) -> ErrorResult
where
    S: BoltService,
{
    let failure = bolt
        .handle(ClientMessage::Run {
            query: query.into(),
            parameters: BTreeMap::new(),
            extra: BTreeMap::new(),
        })
        .await;
    let result = match failure.as_slice() {
        [ServerMessage::Failure { code, message }] => ErrorResult {
            code: code.clone(),
            message: message.clone(),
        },
        other => panic!("query unexpectedly succeeded: {other:?}"),
    };
    assert!(matches!(
        bolt.handle(ClientMessage::Reset).await.as_slice(),
        [ServerMessage::Success(_)]
    ));
    result
}

fn records(messages: &[ServerMessage]) -> Vec<Vec<Value>> {
    messages
        .iter()
        .filter_map(|message| match message {
            ServerMessage::Record(values) => Some(values.clone()),
            _ => None,
        })
        .collect()
}

fn has_more(messages: &[ServerMessage]) -> bool {
    messages.iter().any(|message| {
        matches!(
            message,
            ServerMessage::Success(summary)
                if summary.get("has_more") == Some(&Value::Boolean(true))
        )
    })
}

const fn mode_name(mode: DeploymentMode) -> &'static str {
    match mode {
        DeploymentMode::PrimaryReplica => "primary",
        DeploymentMode::SharedNothing => "shared",
    }
}

fn elected_meta(root: &Path) -> MetaNodeService {
    let mut replica =
        MetaRaftReplica::open(1, &[1], root.join("raft"), root.join("state")).unwrap();
    replica.campaign().unwrap();
    for _ in 0..32 {
        assert!(replica.drain_ready().unwrap().is_empty());
        if replica.is_leader() {
            break;
        }
        replica.tick();
    }
    assert!(replica.is_leader());
    MetaNodeService::new(
        CLUSTER_ID,
        Arc::new(Mutex::new(replica)),
        Arc::new(ReplicatedTso::new(Arc::new(ManualClock::new(1_000_000)), 32, 1_000_000).unwrap()),
    )
}

fn context(request_id: u128) -> RequestContext {
    RequestContext {
        protocol_version: CLUSTER_PROTOCOL_VERSION,
        cluster_id: CLUSTER_ID.to_vec(),
        request_id: request_id.to_be_bytes().to_vec(),
        deadline_unix_ms: now_ms() + 60_000,
    }
}

fn now_ms() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
}

fn free_address() -> SocketAddr {
    let listener = TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).unwrap();
    listener.local_addr().unwrap()
}

fn data_config(root: &Path, node_id: u64) -> NodeConfig {
    let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 7_000 + node_id as u16);
    NodeConfig::new(
        NodeIdentity::new(CLUSTER_ID, node_id).unwrap(),
        address,
        address,
        root,
        vec!["127.0.0.1:7001".parse().unwrap()],
        TransportSecurity::LoopbackPlaintext,
        BTreeMap::new(),
    )
    .unwrap()
}

async fn serve_shard(
    host: Arc<DataNodeHost>,
) -> (
    SocketAddr,
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (shutdown, receiver) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(
        Server::builder()
            .add_service(ShardServiceServer::new(DataNodeGrpcService::new(host)))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = receiver.await;
            }),
    );
    (address, shutdown, server)
}
