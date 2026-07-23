use std::collections::{BTreeMap, BTreeSet};
use std::io::{BufRead, BufReader};
use std::net::SocketAddr;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use analytics_ledger::{
    AnalyticsJobId, GraphProjectionScope, JobCommand, JobSpec, LedgerState, ProjectionLimits,
};
use cluster_protocol::CLUSTER_PROTOCOL_VERSION;
use cluster_protocol::proto::gateway_service_client::GatewayServiceClient;
use cluster_protocol::proto::meta_raft_service_server::MetaRaftServiceServer;
use cluster_protocol::proto::meta_service_client::MetaServiceClient;
use cluster_protocol::proto::meta_service_server::{MetaService, MetaServiceServer};
use cluster_protocol::proto::shard_service_server::ShardServiceServer;
use cluster_protocol::proto::{
    GatewaySubmitRequest, GetCatalogRequest, ListAnalyticsJobTombstonesRequest,
    ProposeAnalyticsJobRequest, ProposeRequest, RequestContext,
};
use control_plane::{
    BackendProfile, CatalogCommand, DeploymentMode, GraphDefinition, Placement, TopologyDefinition,
};
use data_node::{
    DataNodeGrpcService, DataNodeHost, NodeConfig, NodeIdentity, ReplicaKey, ReplicaRole,
    ReplicaSpec, TransportSecurity,
};
use dtgproxy::gateway::GATEWAY_API_VERSION;
use gateway_node::{
    AnalyticsFaultInjector, AnalyticsFaultPoint, RemoteGatewayService, process_stop_fault,
};
use meta_node::{
    MetaNodeService, MetaRaftGrpcService, MetaRaftReplica, MetaRaftRuntime, MetaTransportSecurity,
    ReplicatedTso,
};
use shard_client::{
    ArtifactKind, ListArtifactGenerationsRequest, PinArtifactGenerationRequest,
    PutArtifactChunkRequest, RemoteReplica, RemoteShardClient, RemoteTopology, ShardClient,
    ShardRequestContext,
};
use storage_api::AdapterRequirement;
use temporal_types::{TransactionTime, ValidTime};
use timestamp_oracle::ManualClock;
use tokio::sync::Mutex;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::Request;
use tonic::transport::Server;

const CLUSTER_ID: [u8; 16] = [0x74; 16];

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

fn now_ms() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
}

fn context(request_id: u128) -> RequestContext {
    RequestContext {
        protocol_version: CLUSTER_PROTOCOL_VERSION,
        cluster_id: CLUSTER_ID.to_vec(),
        request_id: request_id.to_be_bytes().to_vec(),
        deadline_unix_ms: now_ms() + 60_000,
    }
}

fn graph() -> GraphDefinition {
    GraphDefinition::new(
        7,
        "process_graph",
        1,
        TopologyDefinition::new(
            DeploymentMode::PrimaryReplica,
            99,
            128,
            1,
            vec![Placement::new(10, 1, vec![10]).unwrap()],
        )
        .unwrap(),
        BackendProfile::new(
            "rocksdb",
            BTreeMap::new(),
            BTreeMap::new(),
            AdapterRequirement::HotPluggableReplica,
            1,
        )
        .unwrap(),
    )
    .unwrap()
}

fn analytics_job(job_id: u128, submission_request_id: u128) -> JobSpec {
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

async fn put_pinned_result(client: &RemoteShardClient, job_id: u128, request_seed: u128) {
    let payload = format!("result-{job_id}").into_bytes();
    let digest = *blake3::hash(&payload).as_bytes();
    client
        .put_artifact_chunk(
            PutArtifactChunkRequest::new(
                ShardRequestContext::new(7, 10, 1, request_seed, now_ms() + 60_000).unwrap(),
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
                ShardRequestContext::new(7, 10, 1, request_seed + 1, now_ms() + 60_000).unwrap(),
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

fn elected_meta(root: &std::path::Path) -> MetaNodeService {
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
    MetaNodeService::new(
        CLUSTER_ID,
        Arc::new(Mutex::new(replica)),
        Arc::new(ReplicatedTso::new(Arc::new(ManualClock::new(1_000_000)), 8, 1_000_000).unwrap()),
    )
}

struct MetaQuorumNode {
    node_id: u64,
    api_address: SocketAddr,
    replica: Arc<Mutex<MetaRaftReplica>>,
    runtime: Option<MetaRaftRuntime>,
    api_shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    raft_shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    api_server: Option<tokio::task::JoinHandle<Result<(), tonic::transport::Error>>>,
    raft_server: Option<tokio::task::JoinHandle<Result<(), tonic::transport::Error>>>,
}

impl MetaQuorumNode {
    async fn campaign(&self) {
        let mut replica = self.replica.lock().await;
        replica.campaign().unwrap();
        drop(replica);
        self.runtime.as_ref().unwrap().notify().notify_one();
    }

    async fn crash(&mut self) {
        if let Some(shutdown) = self.api_shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(shutdown) = self.raft_shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(server) = self.api_server.take() {
            server.await.unwrap().unwrap();
        }
        if let Some(server) = self.raft_server.take() {
            server.await.unwrap().unwrap();
        }
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown().await.unwrap();
        }
    }
}

async fn start_meta_quorum(root: &std::path::Path) -> Vec<MetaQuorumNode> {
    let mut api_listeners = BTreeMap::new();
    let mut raft_listeners = BTreeMap::new();
    for node_id in 1..=3_u64 {
        api_listeners.insert(
            node_id,
            tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap(),
        );
        raft_listeners.insert(
            node_id,
            tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap(),
        );
    }
    let api_addresses = api_listeners
        .iter()
        .map(|(node_id, listener)| (*node_id, listener.local_addr().unwrap()))
        .collect::<BTreeMap<_, _>>();
    let raft_addresses = raft_listeners
        .iter()
        .map(|(node_id, listener)| (*node_id, listener.local_addr().unwrap()))
        .collect::<BTreeMap<_, _>>();

    let mut nodes = Vec::new();
    for node_id in 1..=3_u64 {
        let replica = Arc::new(Mutex::new(
            MetaRaftReplica::open(
                node_id,
                &[1, 2, 3],
                root.join(format!("node-{node_id}/raft")),
                root.join(format!("node-{node_id}/state")),
            )
            .unwrap(),
        ));
        let peers = raft_addresses
            .iter()
            .filter(|(peer_id, _)| **peer_id != node_id)
            .map(|(peer_id, address)| (*peer_id, *address))
            .collect();
        let runtime = MetaRaftRuntime::spawn(
            CLUSTER_ID,
            node_id,
            Arc::clone(&replica),
            peers,
            &MetaTransportSecurity::LoopbackPlaintext,
        )
        .unwrap();
        let notify = runtime.notify();
        let service = MetaNodeService::new(
            CLUSTER_ID,
            Arc::clone(&replica),
            Arc::new(
                ReplicatedTso::new(Arc::new(ManualClock::new(1_000_000)), 8, 1_000_000).unwrap(),
            ),
        )
        .with_runtime_notify(Arc::clone(&notify));
        let raft_service =
            MetaRaftGrpcService::new(CLUSTER_ID, node_id, Arc::clone(&replica), notify);
        let api_listener = api_listeners.remove(&node_id).unwrap();
        let raft_listener = raft_listeners.remove(&node_id).unwrap();
        let (api_shutdown, api_shutdown_rx) = tokio::sync::oneshot::channel();
        let (raft_shutdown, raft_shutdown_rx) = tokio::sync::oneshot::channel();
        let api_server = tokio::spawn(
            Server::builder()
                .add_service(MetaServiceServer::new(service))
                .serve_with_incoming_shutdown(TcpListenerStream::new(api_listener), async {
                    let _ = api_shutdown_rx.await;
                }),
        );
        let raft_server = tokio::spawn(
            Server::builder()
                .add_service(MetaRaftServiceServer::new(raft_service))
                .serve_with_incoming_shutdown(TcpListenerStream::new(raft_listener), async {
                    let _ = raft_shutdown_rx.await;
                }),
        );
        nodes.push(MetaQuorumNode {
            node_id,
            api_address: api_addresses[&node_id],
            replica,
            runtime: Some(runtime),
            api_shutdown: Some(api_shutdown),
            raft_shutdown: Some(raft_shutdown),
            api_server: Some(api_server),
            raft_server: Some(raft_server),
        });
    }
    nodes[0].campaign().await;
    nodes
}

async fn meta_leader(nodes: &[MetaQuorumNode], minimum_revision: u64) -> Option<u64> {
    for attempt in 0..200_u128 {
        for node in nodes.iter().filter(|node| node.runtime.is_some()) {
            let endpoint = format!("http://{}", node.api_address);
            let Ok(mut client) = MetaServiceClient::connect(endpoint).await else {
                continue;
            };
            if client
                .get_catalog(GetCatalogRequest {
                    context: Some(context(10_000 + attempt)),
                    minimum_revision,
                })
                .await
                .is_ok()
            {
                return Some(node.node_id);
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    None
}

async fn create_graph_on_meta_leader(nodes: &[MetaQuorumNode], request_id: u128) {
    let command = CatalogCommand::create_graph(request_id, 0, graph())
        .encode()
        .unwrap();
    for attempt in 0..200_u128 {
        for node in nodes.iter().filter(|node| node.runtime.is_some()) {
            let endpoint = format!("http://{}", node.api_address);
            let Ok(mut client) = MetaServiceClient::connect(endpoint).await else {
                continue;
            };
            if client
                .propose(ProposeRequest {
                    context: Some(context(request_id)),
                    command: command.clone(),
                })
                .await
                .is_ok()
            {
                return;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(25 + attempt.min(3) as u64)).await;
    }
    panic!("Meta quorum did not accept the graph definition");
}

fn reserve_address() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap()
}

fn data_config(root: &std::path::Path) -> NodeConfig {
    NodeConfig::new(
        NodeIdentity::new(CLUSTER_ID, 10).unwrap(),
        "127.0.0.1:7110".parse().unwrap(),
        "127.0.0.1:7110".parse().unwrap(),
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

async fn spawn_gateway_process(config_path: &std::path::Path) -> std::process::Child {
    let mut command = Command::new(env!("CARGO_BIN_EXE_dtgproxy-gateway"));
    command
        .args(["--config", config_path.to_str().unwrap()])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command.spawn().unwrap();
    let stdout = child.stdout.take().unwrap();
    let ready = tokio::task::spawn_blocking(move || {
        let mut reader = BufReader::new(stdout);
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        line
    })
    .await
    .unwrap();
    assert!(ready.starts_with("DTGPROXY_GATEWAY_READY"), "{ready}");
    child
}

async fn submit_cypher(
    client: &mut GatewayServiceClient<tonic::transport::Channel>,
    request_id: u128,
    name: &str,
    text: &str,
) -> serde_json::Value {
    let response = client
        .submit(Request::new(GatewaySubmitRequest {
            context: Some(context(request_id)),
            request_json: serde_json::to_vec(&serde_json::json!({
                "version": GATEWAY_API_VERSION,
                "request_id": name,
                "operation": "cypher",
                "text": text,
            }))
            .unwrap(),
        }))
        .await
        .unwrap()
        .into_inner();
    serde_json::from_slice(&response.response_json).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gateway_process_restart_recovers_an_inflight_analytics_job() {
    let temporary = tempfile::tempdir().unwrap();
    let meta = elected_meta(&temporary.path().join("meta"));
    meta.propose(Request::new(ProposeRequest {
        context: Some(context(301)),
        command: CatalogCommand::create_graph(301, 0, graph())
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

    let host = Arc::new(
        DataNodeHost::open(data_config(&temporary.path().join("data")), 32)
            .await
            .unwrap(),
    );
    host.ensure_replica(
        ReplicaSpec::new(
            7,
            10,
            1,
            vec![10],
            ReplicaRole::Voter,
            1,
            1,
            "process-shard",
        )
        .unwrap(),
    )
    .await
    .unwrap();
    host.campaign(ReplicaKey::new(7, 10).unwrap())
        .await
        .unwrap();
    let (data_address, data_shutdown, data_server) = serve_shard(Arc::clone(&host)).await;

    let gateway_address = reserve_address();
    let config_path = temporary.path().join("analytics-gateway.json");
    std::fs::write(
        &config_path,
        serde_json::to_vec_pretty(&serde_json::json!({
            "version": 1,
            "cluster_id": "74747474747474747474747474747474",
            "node_id": 31,
            "graph_id": 7,
            "listen_address": gateway_address,
            "advertise_address": gateway_address,
            "meta_seeds": [meta_address],
            "data_nodes": {"10": data_address},
            "security": {"mode": "loopback_plaintext"},
            "maximum_inflight": 32,
            "max_raft_ticks": 64,
            "catalog_watch_timeout_ms": 5000,
            "shutdown_grace_ms": 1000
        }))
        .unwrap(),
    )
    .unwrap();

    let mut first = spawn_gateway_process(&config_path).await;
    let endpoint = format!("http://{gateway_address}");
    let mut client = GatewayServiceClient::connect(endpoint.clone())
        .await
        .unwrap();
    let created = submit_cypher(
        &mut client,
        302,
        "create-graph",
        "USE process_graph FOR VALID_TIME AS OF 1000 CREATE (a:Person)-[:KNOWS]->(b:Person)",
    )
    .await;
    assert_eq!(created["ok"], true, "{created}");
    let submitted = submit_cypher(
        &mut client,
        303,
        "submit-degree",
        "USE process_graph FOR VALID_TIME AS OF 1000 \
         CALL dtg.analytics.submit({algorithm: 'dtg.graph.degree', parameters: {}}) \
         YIELD jobId RETURN jobId",
    )
    .await;
    assert_eq!(submitted["ok"], true, "{submitted}");
    let job_id = submitted["result"]["rows"][0][0]
        .as_str()
        .expect("analytics submit job ID")
        .to_owned();
    let mut running = false;
    for attempt in 0..100_u128 {
        let status_request_id = format!("status-before-restart-{attempt}");
        let status = submit_cypher(
            &mut client,
            400 + attempt,
            &status_request_id,
            &format!("CALL dtg.analytics.status({{jobId: '{job_id}'}}) YIELD state RETURN state"),
        )
        .await;
        if status["result"]["rows"][0][0] == "RUNNING" {
            running = true;
            break;
        }
        assert_ne!(status["result"]["rows"][0][0], "SUCCEEDED");
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    assert!(
        running,
        "analytics job never reached RUNNING before restart"
    );
    drop(client);
    first.kill().unwrap();
    first.wait().unwrap();

    let mut second = spawn_gateway_process(&config_path).await;
    let mut client = GatewayServiceClient::connect(endpoint).await.unwrap();
    let mut succeeded = false;
    for attempt in 0..300_u128 {
        let status_request_id = format!("status-after-restart-{attempt}");
        let status = submit_cypher(
            &mut client,
            600 + attempt,
            &status_request_id,
            &format!("CALL dtg.analytics.status({{jobId: '{job_id}'}}) YIELD state RETURN state"),
        )
        .await;
        if status["result"]["rows"][0][0] == "SUCCEEDED" {
            succeeded = true;
            break;
        }
        assert_ne!(status["result"]["rows"][0][0], "FAILED", "{status}");
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(
        succeeded,
        "analytics job did not recover after Gateway restart"
    );
    let results = submit_cypher(
        &mut client,
        1_000,
        "results-after-restart",
        &format!(
            "CALL dtg.analytics.results({{jobId: '{job_id}', offset: 0, limit: 8}}) \
             YIELD row RETURN row"
        ),
    )
    .await;
    assert_eq!(results["ok"], true, "{results}");
    assert_eq!(results["result"]["row_count"], 2, "{results}");
    let raw_job_id = u128::from_str_radix(&job_id, 16).unwrap();
    let topology = RemoteTopology::new(
        1,
        7,
        vec![(
            10,
            1,
            10,
            vec![RemoteReplica::new(10, data_address).unwrap()],
        )],
    )
    .unwrap();
    let shard_client = RemoteShardClient::new_loopback_plaintext(CLUSTER_ID, topology).unwrap();
    let generations = shard_client
        .list_artifact_generations(
            ListArtifactGenerationsRequest::new(
                ShardRequestContext::new(7, 10, 1, 1_001, now_ms() + 60_000).unwrap(),
                raw_job_id,
                ArtifactKind::Result,
                16,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(generations.len(), 1, "duplicate or ghost Result generation");
    assert!(generations[0].pinned());

    drop(client);
    second.kill().unwrap();
    second.wait().unwrap();
    data_shutdown.send(()).unwrap();
    data_server.await.unwrap().unwrap();
    Arc::try_unwrap(host)
        .ok()
        .unwrap()
        .shutdown()
        .await
        .unwrap();
    meta_shutdown.send(()).unwrap();
    meta_server.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gateway_restart_acknowledges_a_deleted_tombstone_and_protects_unknown_or_active_jobs() {
    let temporary = tempfile::tempdir().unwrap();
    let meta = elected_meta(&temporary.path().join("meta-tombstone"));
    meta.propose(Request::new(ProposeRequest {
        context: Some(context(2_301)),
        command: CatalogCommand::create_graph(2_301, 0, graph())
            .encode()
            .unwrap(),
    }))
    .await
    .unwrap();
    let submitted_at = now_ms();
    let tombstoned_job = AnalyticsJobId::new(701).unwrap();
    for (request_id, command) in [
        (
            2_302,
            JobCommand::submit(2_302, analytics_job(701, 71_001), submitted_at).unwrap(),
        ),
        (2_303, JobCommand::cancel(2_303, tombstoned_job, 1).unwrap()),
        (
            2_304,
            JobCommand::prune_terminal(2_304, tombstoned_job, 2, submitted_at + 1).unwrap(),
        ),
        (
            2_305,
            JobCommand::submit(2_305, analytics_job(703, 71_003), submitted_at + 2).unwrap(),
        ),
        (
            2_306,
            JobCommand::claim(
                2_306,
                AnalyticsJobId::new(703).unwrap(),
                1,
                999,
                1,
                submitted_at + 60_000,
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

    let host = Arc::new(
        DataNodeHost::open(data_config(&temporary.path().join("data-tombstone")), 32)
            .await
            .unwrap(),
    );
    host.ensure_replica(
        ReplicaSpec::new(7, 10, 1, vec![10], ReplicaRole::Voter, 1, 1, "gc-shard").unwrap(),
    )
    .await
    .unwrap();
    host.campaign(ReplicaKey::new(7, 10).unwrap())
        .await
        .unwrap();
    let (data_address, data_shutdown, data_server) = serve_shard(Arc::clone(&host)).await;
    let topology = RemoteTopology::new(
        1,
        7,
        vec![(
            10,
            1,
            10,
            vec![RemoteReplica::new(10, data_address).unwrap()],
        )],
    )
    .unwrap();
    let shard_client =
        Arc::new(RemoteShardClient::new_loopback_plaintext(CLUSTER_ID, topology).unwrap());
    put_pinned_result(shard_client.as_ref(), 701, 2_400).await;
    put_pinned_result(shard_client.as_ref(), 702, 2_410).await;
    put_pinned_result(shard_client.as_ref(), 703, 2_420).await;

    let fence_fault_fired = Arc::new(AtomicBool::new(false));
    let faulted_gateway =
        RemoteGatewayService::new_at_revision_with_gateway_id_and_delay_and_fault_injector(
            41,
            std::time::Duration::ZERO,
            Arc::new(ObservedProcessStop {
                point: AnalyticsFaultPoint::GcAfterFenceAdvance,
                fired: Arc::clone(&fence_fault_fired),
            }),
            CLUSTER_ID,
            1,
            graph(),
            Arc::clone(&shard_client),
            vec![meta_address],
            32,
            64,
        )
        .unwrap();
    for _ in 0..80 {
        if fence_fault_fired.load(Ordering::Acquire) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    assert!(
        fence_fault_fired.load(Ordering::Acquire),
        "GC fence-advance fault boundary was not reached"
    );
    let after_fence = shard_client
        .list_artifact_generations(
            ListArtifactGenerationsRequest::new(
                ShardRequestContext::new(7, 10, 1, 2_499, now_ms() + 60_000).unwrap(),
                701,
                ArtifactKind::Result,
                16,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        after_fence.len(),
        1,
        "fence-advance fault must occur before generation deletion"
    );
    drop(faulted_gateway);

    let gateway_address = reserve_address();
    let config_path = temporary.path().join("gc-gateway.json");
    let write_gateway_config = |node_id: u64| {
        std::fs::write(
            &config_path,
            serde_json::to_vec_pretty(&serde_json::json!({
                "version": 1,
                "cluster_id": "74747474747474747474747474747474",
                "node_id": node_id,
                "graph_id": 7,
                "listen_address": gateway_address,
                "advertise_address": gateway_address,
                "meta_seeds": [meta_address],
                "data_nodes": {"10": data_address},
                "security": {"mode": "loopback_plaintext"},
                "maximum_inflight": 32,
                "max_raft_ticks": 64,
                "catalog_watch_timeout_ms": 5000,
                "shutdown_grace_ms": 1000
            }))
            .unwrap(),
        )
        .unwrap();
    };
    write_gateway_config(42);
    let mut first = spawn_gateway_process(&config_path).await;
    let mut deleted = false;
    for attempt in 0..80_u128 {
        let generations = shard_client
            .list_artifact_generations(
                ListArtifactGenerationsRequest::new(
                    ShardRequestContext::new(7, 10, 1, 2_500 + attempt, now_ms() + 60_000).unwrap(),
                    701,
                    ArtifactKind::Result,
                    16,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        if generations.is_empty() {
            deleted = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    assert!(
        deleted,
        "tombstoned latest pinned Artifact was not reclaimed"
    );
    first.kill().unwrap();
    first.wait().unwrap();

    let before_restart = meta
        .list_analytics_job_tombstones(Request::new(ListAnalyticsJobTombstonesRequest {
            context: Some(context(2_600)),
            after_job_id: Vec::new(),
            limit: 8,
        }))
        .await
        .unwrap()
        .into_inner();
    let (_, _, tombstone) =
        LedgerState::decode_tombstone(&before_restart.tombstones[0].record).unwrap();
    assert!(!tombstone.artifacts_reclaimed());

    write_gateway_config(43);
    let mut second = spawn_gateway_process(&config_path).await;
    let mut acknowledged = false;
    for attempt in 0..120_u128 {
        let page = meta
            .list_analytics_job_tombstones(Request::new(ListAnalyticsJobTombstonesRequest {
                context: Some(context(2_700 + attempt)),
                after_job_id: Vec::new(),
                limit: 8,
            }))
            .await
            .unwrap()
            .into_inner();
        let (_, _, tombstone) = LedgerState::decode_tombstone(&page.tombstones[0].record).unwrap();
        if tombstone.artifacts_reclaimed() {
            acknowledged = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    assert!(
        acknowledged,
        "replacement Gateway did not acknowledge reclamation"
    );
    for (offset, job_id) in [702_u128, 703].into_iter().enumerate() {
        let generations = shard_client
            .list_artifact_generations(
                ListArtifactGenerationsRequest::new(
                    ShardRequestContext::new(7, 10, 1, 3_000 + offset as u128, now_ms() + 60_000)
                        .unwrap(),
                    job_id,
                    ArtifactKind::Result,
                    16,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(generations.len(), 1, "Job {job_id} must remain fail-closed");
    }

    second.kill().unwrap();
    second.wait().unwrap();
    data_shutdown.send(()).unwrap();
    data_server.await.unwrap().unwrap();
    Arc::try_unwrap(host)
        .ok()
        .unwrap()
        .shutdown()
        .await
        .unwrap();
    meta_shutdown.send(()).unwrap();
    meta_server.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn meta_leader_change_does_not_interrupt_an_inflight_analytics_job() {
    let temporary = tempfile::tempdir().unwrap();
    let mut meta_nodes = start_meta_quorum(&temporary.path().join("meta-quorum")).await;
    create_graph_on_meta_leader(&meta_nodes, 1_101).await;
    assert!(meta_leader(&meta_nodes, 1).await.is_some());

    let host = Arc::new(
        DataNodeHost::open(data_config(&temporary.path().join("data")), 32)
            .await
            .unwrap(),
    );
    host.ensure_replica(
        ReplicaSpec::new(
            7,
            10,
            1,
            vec![10],
            ReplicaRole::Voter,
            1,
            1,
            "meta-failover-shard",
        )
        .unwrap(),
    )
    .await
    .unwrap();
    host.campaign(ReplicaKey::new(7, 10).unwrap())
        .await
        .unwrap();
    let (data_address, data_shutdown, data_server) = serve_shard(Arc::clone(&host)).await;

    let gateway_address = reserve_address();
    let meta_addresses = meta_nodes
        .iter()
        .map(|node| node.api_address)
        .collect::<Vec<_>>();
    let config_path = temporary.path().join("meta-failover-gateway.json");
    std::fs::write(
        &config_path,
        serde_json::to_vec_pretty(&serde_json::json!({
            "version": 1,
            "cluster_id": "74747474747474747474747474747474",
            "node_id": 32,
            "graph_id": 7,
            "listen_address": gateway_address,
            "advertise_address": gateway_address,
            "meta_seeds": meta_addresses,
            "data_nodes": {"10": data_address},
            "security": {"mode": "loopback_plaintext"},
            "maximum_inflight": 32,
            "max_raft_ticks": 64,
            "catalog_watch_timeout_ms": 5000,
            "shutdown_grace_ms": 1000
        }))
        .unwrap(),
    )
    .unwrap();

    let mut gateway = spawn_gateway_process(&config_path).await;
    let endpoint = format!("http://{gateway_address}");
    let mut client = GatewayServiceClient::connect(endpoint).await.unwrap();
    let created = submit_cypher(
        &mut client,
        1_102,
        "create-before-meta-failover",
        "USE process_graph FOR VALID_TIME AS OF 1000 CREATE (a:Person)-[:KNOWS]->(b:Person)",
    )
    .await;
    assert_eq!(created["ok"], true, "{created}");
    let submitted = submit_cypher(
        &mut client,
        1_103,
        "submit-before-meta-failover",
        "USE process_graph FOR VALID_TIME AS OF 1000 \
         CALL dtg.analytics.submit({algorithm: 'dtg.graph.degree', parameters: {}}) \
         YIELD jobId RETURN jobId",
    )
    .await;
    assert_eq!(submitted["ok"], true, "{submitted}");
    let job_id = submitted["result"]["rows"][0][0]
        .as_str()
        .expect("analytics submit job ID")
        .to_owned();

    let mut running = false;
    for attempt in 0..100_u128 {
        let status_request_id = format!("status-before-meta-failover-{attempt}");
        let status = submit_cypher(
            &mut client,
            1_200 + attempt,
            &status_request_id,
            &format!("CALL dtg.analytics.status({{jobId: '{job_id}'}}) YIELD state RETURN state"),
        )
        .await;
        if status["result"]["rows"][0][0] == "RUNNING" {
            running = true;
            break;
        }
        assert_ne!(status["result"]["rows"][0][0], "SUCCEEDED");
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    assert!(
        running,
        "analytics job never reached RUNNING before failover"
    );

    let failed_leader = meta_leader(&meta_nodes, 1)
        .await
        .expect("Meta quorum lost its Leader before fault injection");
    meta_nodes
        .iter_mut()
        .find(|node| node.node_id == failed_leader)
        .unwrap()
        .crash()
        .await;
    let replacement = meta_leader(&meta_nodes, 1)
        .await
        .expect("surviving Meta quorum did not elect a replacement Leader");
    assert_ne!(replacement, failed_leader);

    let mut succeeded = false;
    let mut last_status = serde_json::Value::Null;
    let raw_job_id = u128::from_str_radix(&job_id, 16).unwrap();
    for attempt in 0..300_u128 {
        let status_request_id = format!("status-after-meta-failover-{attempt}");
        let status = submit_cypher(
            &mut client,
            1_400 + attempt,
            &status_request_id,
            &format!("CALL dtg.analytics.status({{jobId: '{job_id}'}}) YIELD state RETURN state"),
        )
        .await;
        if status["result"]["rows"][0][0] == "SUCCEEDED" {
            succeeded = true;
            break;
        }
        assert_ne!(status["result"]["rows"][0][0], "FAILED", "{status}");
        last_status = status;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(
        succeeded,
        "analytics job did not survive Meta failover; last status: {last_status}"
    );
    let results = submit_cypher(
        &mut client,
        1_800,
        "results-after-meta-failover",
        &format!(
            "CALL dtg.analytics.results({{jobId: '{job_id}', offset: 0, limit: 8}}) \
             YIELD row RETURN row"
        ),
    )
    .await;
    assert_eq!(results["ok"], true, "{results}");
    assert_eq!(results["result"]["row_count"], 2, "{results}");

    let topology = RemoteTopology::new(
        1,
        7,
        vec![(
            10,
            1,
            10,
            vec![RemoteReplica::new(10, data_address).unwrap()],
        )],
    )
    .unwrap();
    let shard_client = RemoteShardClient::new_loopback_plaintext(CLUSTER_ID, topology).unwrap();
    let generations = shard_client
        .list_artifact_generations(
            ListArtifactGenerationsRequest::new(
                ShardRequestContext::new(7, 10, 1, 1_801, now_ms() + 60_000).unwrap(),
                raw_job_id,
                ArtifactKind::Result,
                16,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(generations.len(), 1, "duplicate or ghost Result generation");
    assert!(generations[0].pinned());

    drop(client);
    gateway.kill().unwrap();
    gateway.wait().unwrap();
    data_shutdown.send(()).unwrap();
    data_server.await.unwrap().unwrap();
    Arc::try_unwrap(host)
        .ok()
        .unwrap()
        .shutdown()
        .await
        .unwrap();
    for node in &mut meta_nodes {
        node.crash().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gateway_binary_bootstraps_from_meta_without_creating_replica_storage() {
    let temporary = tempfile::tempdir().unwrap();
    let meta = elected_meta(&temporary.path().join("meta"));
    meta.propose(Request::new(ProposeRequest {
        context: Some(context(101)),
        command: CatalogCommand::create_graph(101, 0, graph())
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

    let gateway_address = reserve_address();
    let config_path = temporary.path().join("gateway.json");
    std::fs::write(
        &config_path,
        serde_json::to_vec_pretty(&serde_json::json!({
            "version": 1,
            "cluster_id": "74747474747474747474747474747474",
            "node_id": 31,
            "graph_id": 7,
            "listen_address": gateway_address,
            "advertise_address": gateway_address,
            "meta_seeds": [meta_address],
            "data_nodes": {"10": "127.0.0.1:7110"},
            "security": {"mode": "loopback_plaintext"},
            "maximum_inflight": 32,
            "max_raft_ticks": 64,
            "catalog_watch_timeout_ms": 5000,
            "shutdown_grace_ms": 1000
        }))
        .unwrap(),
    )
    .unwrap();
    let entries_before = std::fs::read_dir(temporary.path())
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect::<BTreeSet<_>>();

    let mut child = Command::new(env!("CARGO_BIN_EXE_dtgproxy-gateway"))
        .args(["--config", config_path.to_str().unwrap()])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let ready = tokio::task::spawn_blocking(move || {
        let mut reader = BufReader::new(stdout);
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        line
    })
    .await
    .unwrap();
    assert!(ready.starts_with("DTGPROXY_GATEWAY_READY"), "{ready}");

    let mut client = GatewayServiceClient::connect(format!("http://{gateway_address}"))
        .await
        .unwrap();
    let response = client
        .submit(Request::new(GatewaySubmitRequest {
            context: Some(context(201)),
            request_json: serde_json::to_vec(&serde_json::json!({
                "version": GATEWAY_API_VERSION,
                "request_id": "process-status",
                "operation": "status"
            }))
            .unwrap(),
        }))
        .await
        .unwrap()
        .into_inner();
    let response: serde_json::Value = serde_json::from_slice(&response.response_json).unwrap();
    assert_eq!(response["ok"], true, "{response}");
    assert_eq!(response["result"]["replica_storage"], "none");

    child.kill().unwrap();
    child.wait().unwrap();
    let entries_after = std::fs::read_dir(temporary.path())
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect::<BTreeSet<_>>();
    assert_eq!(entries_after, entries_before);
    meta_shutdown.send(()).unwrap();
    meta_server.await.unwrap().unwrap();
}
