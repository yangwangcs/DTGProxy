use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bolt_protocol::{ClientMessage, Value};
use bolt_server::{BoltMachine, ServerMessage};
use cluster_protocol::CLUSTER_PROTOCOL_VERSION;
use cluster_protocol::proto::meta_service_client::MetaServiceClient;
use cluster_protocol::proto::meta_service_server::{MetaService, MetaServiceServer};
use cluster_protocol::proto::shard_service_server::ShardServiceServer;
use cluster_protocol::proto::{ProposeRequest, RequestContext};
use control_plane::{
    BackendProfile, CatalogCommand, DeploymentMode, GraphDefinition, Placement, TopologyDefinition,
};
use cypher_engine::CypherBoltService;
use data_node::{
    DataNodeGrpcService, DataNodeHost, NodeConfig, NodeIdentity, ReplicaKey, ReplicaRole,
    ReplicaSpec, TransportSecurity,
};
use gateway_node::{GatewayCatalogRouter, RemoteGatewayService};
use meta_node::{MetaNodeService, MetaRaftReplica, ReplicatedTso};
use shard_client::{
    ArtifactKind, GetArtifactGenerationRequest, ListArtifactGenerationsRequest, RemoteShardClient,
    ShardClient, ShardRequestContext,
};
use storage_api::AdapterRequirement;
use timestamp_oracle::ManualClock;
use tokio::sync::Mutex;
use tokio_stream::{StreamExt, wrappers::TcpListenerStream};
use tonic::Request;
use tonic::transport::Server;

const CLUSTER_ID: [u8; 16] = [0x75; 16];

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn primary_replica_typed_degree_call_matches_shared_nothing() {
    let primary = run_degree_fixture(DeploymentMode::PrimaryReplica, false, None).await;
    let shared_nothing = run_degree_fixture(DeploymentMode::SharedNothing, false, None).await;

    assert_eq!(primary, shared_nothing);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn primary_replica_async_degree_publishes_and_reads_canonical_artifact() {
    let primary = run_degree_fixture(DeploymentMode::PrimaryReplica, true, None).await;
    let shared_nothing = run_degree_fixture(DeploymentMode::SharedNothing, true, None).await;
    assert_eq!(primary, vec![1, 1]);
    assert_eq!(shared_nothing, primary);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn primary_replica_second_gateway_takes_over_expired_lease() {
    let result = run_degree_fixture(
        DeploymentMode::PrimaryReplica,
        true,
        Some(Duration::from_secs(6)),
    )
    .await;
    assert_eq!(result, vec![1, 1]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn primary_replica_data_node_restart_keeps_running_job_recoverable() {
    run_data_node_restart_fixture(DeploymentMode::PrimaryReplica).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn shared_nothing_data_node_restart_keeps_running_job_recoverable() {
    run_data_node_restart_fixture(DeploymentMode::SharedNothing).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn primary_replica_ordered_full_stack_restart_is_recoverable() {
    run_ordered_full_stack_restart_fixture(DeploymentMode::PrimaryReplica).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn shared_nothing_ordered_full_stack_restart_is_recoverable() {
    run_ordered_full_stack_restart_fixture(DeploymentMode::SharedNothing).await;
}

async fn run_data_node_restart_fixture(mode: DeploymentMode) {
    let temporary = tempfile::tempdir().expect("temporary fixture");
    let placements = match mode {
        DeploymentMode::PrimaryReplica => vec![Placement::new(10, 1, vec![10]).unwrap()],
        DeploymentMode::SharedNothing => vec![
            Placement::new(10, 1, vec![10]).unwrap(),
            Placement::new(20, 1, vec![20]).unwrap(),
        ],
    };
    let storage_placement = placements[0].clone();
    let graph = GraphDefinition::new(
        7,
        "analytics",
        1,
        TopologyDefinition::new(mode, 99, 128, 1, placements.clone()).unwrap(),
        BackendProfile::new(
            "rocksdb",
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
        context: Some(context(2_001)),
        command: CatalogCommand::create_graph(2_001, 0, graph)
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
    let mut data_nodes = Vec::new();
    for placement in placements {
        let node_id = placement.voters()[0];
        let address = reserve_address();
        let mut data_node = RestartableShard::new(
            temporary.path().join(format!("restartable-data-{node_id}")),
            address,
            placement,
        );
        data_node.start().await;
        data_endpoints.insert(node_id, address);
        data_nodes.push(data_node);
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
        Arc::new(RemoteShardClient::new_loopback_plaintext(CLUSTER_ID, topology).unwrap());
    let gateway = RemoteGatewayService::new_at_revision_with_gateway_id_and_delay(
        903,
        Duration::from_secs(2),
        CLUSTER_ID,
        1,
        authoritative_graph,
        Arc::clone(&remote_client),
        vec![meta_address],
        32,
        64,
    )
    .unwrap();
    let bolt_service = Arc::new(CypherBoltService::new(Arc::new(gateway), 16).unwrap());
    let mut bolt = BoltMachine::new(bolt_service);
    assert!(matches!(
        bolt.handle(ClientMessage::Hello(BTreeMap::new()))
            .await
            .as_slice(),
        [ServerMessage::Success(_)]
    ));
    let created = bolt
        .handle(ClientMessage::Run {
            query: "USE analytics FOR VALID_TIME AS OF 1000 CREATE (a:Person)-[:KNOWS]->(b:Person)"
                .into(),
            parameters: BTreeMap::new(),
            extra: BTreeMap::new(),
        })
        .await;
    assert!(matches!(created.as_slice(), [ServerMessage::Success(_)]));
    let discarded = bolt
        .handle(ClientMessage::Discard {
            n: -1,
            query_id: None,
        })
        .await;
    assert!(
        discarded
            .iter()
            .any(|message| matches!(message, ServerMessage::Success(_)))
    );

    let baseline_job = submit_async_algorithm(&mut bolt, "dtg.graph.degree").await;
    wait_async_algorithm(&mut bolt, &baseline_job, "dtg.graph.degree").await;
    let baseline_bytes = read_result_artifact(
        remote_client.as_ref(),
        &storage_placement,
        &baseline_job,
        2_100,
    )
    .await;

    let restarted_job = submit_async_algorithm(&mut bolt, "dtg.graph.degree").await;
    wait_for_analytics_state(&mut bolt, &restarted_job, "RUNNING").await;
    data_nodes[0].stop().await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    let (state_while_down, error_while_down) = analytics_status(&mut bolt, &restarted_job).await;
    assert_ne!(
        state_while_down, "FAILED",
        "a transient DataNode outage must not become a terminal analytics failure: {error_while_down:?}"
    );

    data_nodes[0].start().await;
    wait_async_algorithm(&mut bolt, &restarted_job, "dtg.graph.degree").await;
    let restarted_bytes = read_result_artifact(
        remote_client.as_ref(),
        &storage_placement,
        &restarted_job,
        2_200,
    )
    .await;
    assert_eq!(
        restarted_bytes, baseline_bytes,
        "restart changed DTAR bytes"
    );

    drop(bolt);
    drop(remote_client);
    for data_node in &mut data_nodes {
        data_node.stop().await;
    }
    meta_shutdown.send(()).unwrap();
    meta_server.await.unwrap().unwrap();
}

async fn run_ordered_full_stack_restart_fixture(mode: DeploymentMode) {
    let temporary = tempfile::tempdir().expect("temporary fixture");
    let placements = match mode {
        DeploymentMode::PrimaryReplica => vec![Placement::new(10, 1, vec![10]).unwrap()],
        DeploymentMode::SharedNothing => vec![
            Placement::new(10, 1, vec![10]).unwrap(),
            Placement::new(20, 1, vec![20]).unwrap(),
        ],
    };
    let storage_placement = placements[0].clone();
    let graph = GraphDefinition::new(
        7,
        "analytics",
        1,
        TopologyDefinition::new(mode, 99, 128, 1, placements.clone()).unwrap(),
        BackendProfile::new(
            "rocksdb",
            BTreeMap::new(),
            BTreeMap::new(),
            AdapterRequirement::HotPluggableReplica,
            1,
        )
        .unwrap(),
    )
    .unwrap();

    let meta_address = reserve_address();
    let mut meta = RestartableMeta::new(temporary.path().join("restartable-meta"), meta_address);
    meta.start().await;
    let mut meta_client = MetaServiceClient::connect(format!("http://{meta_address}"))
        .await
        .unwrap();
    meta_client
        .propose(ProposeRequest {
            context: Some(context(3_001)),
            command: CatalogCommand::create_graph(3_001, 0, graph)
                .encode()
                .unwrap(),
        })
        .await
        .unwrap();
    drop(meta_client);

    let mut data_nodes = Vec::new();
    let mut data_endpoints = BTreeMap::new();
    for placement in placements {
        let node_id = placement.voters()[0];
        let data_address = reserve_address();
        let mut data_node = RestartableShard::new(
            temporary.path().join(format!("restartable-data-{node_id}")),
            data_address,
            placement,
        );
        data_node.start().await;
        data_endpoints.insert(node_id, data_address);
        data_nodes.push(data_node);
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
        Arc::new(RemoteShardClient::new_loopback_plaintext(CLUSTER_ID, topology).unwrap());

    let first_gateway = RemoteGatewayService::new_at_revision_with_gateway_id_and_delay(
        904,
        Duration::from_secs(2),
        CLUSTER_ID,
        1,
        authoritative_graph.clone(),
        Arc::clone(&remote_client),
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
    let created = first_bolt
        .handle(ClientMessage::Run {
            query: "USE analytics FOR VALID_TIME AS OF 1000 CREATE (a:Person)-[:KNOWS]->(b:Person)"
                .into(),
            parameters: BTreeMap::new(),
            extra: BTreeMap::new(),
        })
        .await;
    assert!(matches!(created.as_slice(), [ServerMessage::Success(_)]));
    assert!(
        first_bolt
            .handle(ClientMessage::Discard {
                n: -1,
                query_id: None,
            })
            .await
            .iter()
            .any(|message| matches!(message, ServerMessage::Success(_)))
    );

    let baseline_job = submit_async_algorithm(&mut first_bolt, "dtg.graph.degree").await;
    wait_async_algorithm(&mut first_bolt, &baseline_job, "dtg.graph.degree").await;
    let baseline_bytes = read_result_artifact(
        remote_client.as_ref(),
        &storage_placement,
        &baseline_job,
        3_100,
    )
    .await;

    let interrupted_job = submit_async_algorithm(&mut first_bolt, "dtg.graph.degree").await;
    wait_for_analytics_state(&mut first_bolt, &interrupted_job, "RUNNING").await;
    for data_node in &mut data_nodes {
        data_node.stop().await;
    }
    meta.stop().await;
    drop(first_bolt);
    tokio::time::sleep(Duration::from_secs(3)).await;

    meta.start().await;
    for data_node in &mut data_nodes {
        data_node.start().await;
    }
    let second_gateway = RemoteGatewayService::new_at_revision_with_gateway_id(
        1_004,
        CLUSTER_ID,
        1,
        authoritative_graph,
        Arc::clone(&remote_client),
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
    let recovered_bytes = read_result_artifact(
        remote_client.as_ref(),
        &storage_placement,
        &interrupted_job,
        3_200,
    )
    .await;
    assert_eq!(
        recovered_bytes, baseline_bytes,
        "ordered Gateway/Meta/Shard/backend restart changed DTAR bytes"
    );

    drop(second_bolt);
    drop(remote_client);
    for data_node in &mut data_nodes {
        data_node.stop().await;
    }
    meta.stop().await;
}

async fn run_degree_fixture(
    mode: DeploymentMode,
    run_async_job: bool,
    takeover_delay: Option<Duration>,
) -> Vec<i64> {
    let temporary = tempfile::tempdir().expect("temporary fixture");
    let placements = match mode {
        DeploymentMode::PrimaryReplica => vec![Placement::new(10, 1, vec![10]).unwrap()],
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
        BackendProfile::new(
            "rocksdb",
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
    for placement in placements {
        let node_id = placement.voters()[0];
        let host = Arc::new(
            DataNodeHost::open(
                data_config(&temporary.path().join(format!("data-{node_id}")), node_id),
                32,
            )
            .await
            .unwrap(),
        );
        host.ensure_replica(
            ReplicaSpec::new(
                7,
                placement.shard_id(),
                placement.epoch(),
                placement.voters().to_vec(),
                ReplicaRole::Voter,
                1,
                1,
                format!("shard-{}", placement.shard_id()),
            )
            .unwrap(),
        )
        .await
        .unwrap();
        host.campaign(ReplicaKey::new(7, placement.shard_id()).unwrap())
            .await
            .unwrap();
        let (address, shutdown, server) = serve_shard(host).await;
        data_endpoints.insert(node_id, address);
        data_shutdowns.push(shutdown);
        data_servers.push(server);
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
    let takeover_graph = authoritative_graph.clone();
    let remote_client =
        Arc::new(RemoteShardClient::new_loopback_plaintext(CLUSTER_ID, topology).unwrap());
    let gateway = if let Some(delay) = takeover_delay {
        RemoteGatewayService::new_at_revision_with_gateway_id_and_delay(
            901,
            delay,
            CLUSTER_ID,
            1,
            authoritative_graph,
            Arc::clone(&remote_client),
            vec![meta_address],
            32,
            64,
        )
    } else if run_async_job {
        RemoteGatewayService::new_at_revision_with_gateway_id(
            901,
            CLUSTER_ID,
            1,
            authoritative_graph,
            Arc::clone(&remote_client),
            vec![meta_address],
            32,
            64,
        )
    } else {
        RemoteGatewayService::new_at_revision(
            CLUSTER_ID,
            1,
            authoritative_graph,
            Arc::clone(&remote_client),
            vec![meta_address],
            32,
            64,
        )
    }
    .unwrap();
    let mut takeover_bolt = takeover_delay.map(|_delay| {
        let second_gateway = RemoteGatewayService::new_at_revision_with_gateway_id(
            902,
            CLUSTER_ID,
            1,
            takeover_graph,
            Arc::clone(&remote_client),
            vec![meta_address],
            32,
            64,
        )
        .unwrap();
        BoltMachine::new(Arc::new(
            CypherBoltService::new(Arc::new(second_gateway), 16).unwrap(),
        ))
    });
    if let Some(second_bolt) = takeover_bolt.as_mut() {
        assert!(matches!(
            second_bolt
                .handle(ClientMessage::Hello(BTreeMap::new()))
                .await
                .as_slice(),
            [ServerMessage::Success(_)]
        ));
    }
    let bolt_service = Arc::new(CypherBoltService::new(Arc::new(gateway), 16).unwrap());
    let mut bolt = BoltMachine::new(bolt_service);
    assert!(matches!(
        bolt.handle(ClientMessage::Hello(BTreeMap::new()))
            .await
            .as_slice(),
        [ServerMessage::Success(_)]
    ));
    let created = bolt
        .handle(ClientMessage::Run {
            query: "USE analytics FOR VALID_TIME AS OF 1000 CREATE (a:Person)-[:KNOWS]->(b:Person)"
                .into(),
            parameters: BTreeMap::new(),
            extra: BTreeMap::new(),
        })
        .await;
    assert!(
        matches!(created.as_slice(), [ServerMessage::Success(_)]),
        "{mode:?} write failed: {created:?}"
    );
    let discarded = bolt
        .handle(ClientMessage::Discard {
            n: -1,
            query_id: None,
        })
        .await;
    assert!(
        discarded
            .iter()
            .any(|message| matches!(message, ServerMessage::Success(_))),
        "{mode:?} write discard failed: {discarded:?}"
    );
    let called = bolt
        .handle(ClientMessage::Run {
            query: "USE analytics FOR VALID_TIME AS OF 1000 CALL dtg.graph.degree({}) \
                    YIELD degree AS score RETURN score"
                .into(),
            parameters: BTreeMap::new(),
            extra: BTreeMap::new(),
        })
        .await;
    assert!(
        matches!(called.as_slice(), [ServerMessage::Success(_)]),
        "{mode:?} typed CALL failed: {called:?}"
    );
    let pulled = bolt
        .handle(ClientMessage::Pull {
            n: -1,
            query_id: None,
        })
        .await;
    let mut scores = pulled
        .iter()
        .filter_map(|message| match message {
            ServerMessage::Record(values) => match values.as_slice() {
                [Value::Integer(score)] => Some(*score),
                other => panic!("{mode:?} returned non-integer degree row: {other:?}"),
            },
            _ => None,
        })
        .collect::<Vec<_>>();
    scores.sort_unstable();
    assert_eq!(scores, vec![1, 1], "{mode:?} degree result");

    if run_async_job {
        if let Some(second_bolt) = takeover_bolt.as_mut() {
            let mut submitted = Vec::new();
            for algorithm in ["dtg.graph.degree", "dtg.graph.wcc", "dtg.graph.pageRank"] {
                submitted.push((
                    algorithm,
                    submit_async_algorithm(&mut bolt, algorithm).await,
                ));
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
            drop(bolt);
            tokio::time::sleep(Duration::from_millis(250)).await;
            for (algorithm, job_id) in submitted {
                wait_async_algorithm(second_bolt, &job_id, algorithm).await;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        } else {
            for algorithm in ["dtg.graph.degree", "dtg.graph.wcc", "dtg.graph.pageRank"] {
                let job_id = submit_async_algorithm(&mut bolt, algorithm).await;
                wait_async_algorithm(&mut bolt, &job_id, algorithm).await;
            }
        }
    }

    for shutdown in data_shutdowns {
        shutdown.send(()).unwrap();
    }
    for server in data_servers {
        server.await.unwrap().unwrap();
    }
    meta_shutdown.send(()).unwrap();
    meta_server.await.unwrap().unwrap();
    scores
}

async fn submit_async_algorithm<S>(bolt: &mut BoltMachine<S>, algorithm: &str) -> String
where
    S: bolt_server::BoltService,
{
    let parameters = if algorithm == "dtg.graph.pageRank" {
        "{maxIterations: 4, tolerance: 0.000000000000000000000000000001}"
    } else {
        "{}"
    };
    let submitted = bolt
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
    assert!(matches!(submitted.as_slice(), [ServerMessage::Success(_)]));
    let submitted = bolt
        .handle(ClientMessage::Pull {
            n: -1,
            query_id: None,
        })
        .await;
    submitted
        .iter()
        .find_map(|message| match message {
            ServerMessage::Record(values) if values.len() == 1 => match &values[0] {
                Value::String(job_id) => Some(job_id.clone()),
                _ => None,
            },
            _ => None,
        })
        .unwrap_or_else(|| panic!("async {algorithm} submit returned a job ID"))
}

async fn wait_async_algorithm<S>(bolt: &mut BoltMachine<S>, job_id: &str, algorithm: &str)
where
    S: bolt_server::BoltService,
{
    let mut succeeded = false;
    let mut last_state = None;
    for _ in 0..160 {
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
            "async {algorithm} status failed after recovery: {status:?}"
        );
        let status = bolt
            .handle(ClientMessage::Pull {
                n: -1,
                query_id: None,
            })
            .await;
        let (state, error) = status
            .iter()
            .find_map(|message| match message {
                ServerMessage::Record(values) if values.len() == 2 => {
                    let Value::String(state) = &values[0] else {
                        return None;
                    };
                    let error = match &values[1] {
                        Value::String(error) => Some(error.clone()),
                        Value::Null => None,
                        _ => return None,
                    };
                    Some((state.as_str(), error))
                }
                _ => None,
            })
            .unwrap_or(("UNKNOWN", None));
        last_state =
            Some(error.map_or_else(|| state.to_owned(), |error| format!("{state}: {error}")));
        if state == "SUCCEEDED" {
            succeeded = true;
            break;
        }
        assert_ne!(state, "FAILED", "async {algorithm} failed");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        succeeded,
        "async {algorithm} did not reach SUCCEEDED; last state: {last_state:?}"
    );
    let results = bolt
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
        matches!(results.as_slice(), [ServerMessage::Success(_)]),
        "async {algorithm} result request failed: {results:?}"
    );
    let results = bolt
        .handle(ClientMessage::Pull {
            n: -1,
            query_id: None,
        })
        .await;
    assert!(
        results
            .iter()
            .any(|message| matches!(message, ServerMessage::Record(_)))
    );
}

async fn analytics_state<S>(bolt: &mut BoltMachine<S>, job_id: &str) -> String
where
    S: bolt_server::BoltService,
{
    analytics_status(bolt, job_id).await.0
}

async fn analytics_status<S>(bolt: &mut BoltMachine<S>, job_id: &str) -> (String, Option<String>)
where
    S: bolt_server::BoltService,
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
    assert!(matches!(status.as_slice(), [ServerMessage::Success(_)]));
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
    S: bolt_server::BoltService,
{
    let mut last = String::new();
    for _ in 0..200 {
        last = analytics_state(bolt, job_id).await;
        if last == expected {
            return;
        }
        assert_ne!(last, "FAILED", "analytics Job failed before {expected}");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("analytics Job did not reach {expected}; last state: {last}");
}

async fn read_result_artifact(
    client: &RemoteShardClient,
    placement: &Placement,
    job_id: &str,
    request_seed: u128,
) -> Vec<u8> {
    let job_id = u128::from_str_radix(job_id, 16).expect("canonical analytics job ID");
    let deadline = now_ms().saturating_add(60_000);
    let summaries = client
        .list_artifact_generations(
            ListArtifactGenerationsRequest::new(
                ShardRequestContext::new(
                    7,
                    placement.shard_id(),
                    placement.epoch(),
                    request_seed,
                    deadline,
                )
                .unwrap(),
                job_id,
                ArtifactKind::Result,
                16,
            )
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
        "successful Job must have one pinned Result"
    );
    let summary = pinned[0];
    let mut stream = client
        .get_artifact_generation(
            GetArtifactGenerationRequest::new(
                ShardRequestContext::new(
                    7,
                    placement.shard_id(),
                    placement.epoch(),
                    request_seed.saturating_add(1),
                    deadline,
                )
                .unwrap(),
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
        summary.expected_total_bytes()
    );
    assert_eq!(
        *blake3::hash(&bytes).as_bytes(),
        summary.expected_content_digest()
    );
    bytes
}

fn reserve_address() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap()
}

struct RestartableShard {
    root: PathBuf,
    address: SocketAddr,
    backend_address: SocketAddr,
    placement: Placement,
    host: Option<Arc<DataNodeHost>>,
    server_shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    server: Option<tokio::task::JoinHandle<Result<(), tonic::transport::Error>>>,
    proxy: Option<tokio::task::JoinHandle<std::io::Result<()>>>,
    proxy_connections: Arc<Mutex<Vec<tokio::task::JoinHandle<std::io::Result<()>>>>>,
}

struct RestartableMeta {
    root: PathBuf,
    address: SocketAddr,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    server: Option<tokio::task::JoinHandle<Result<(), tonic::transport::Error>>>,
}

impl RestartableMeta {
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
        self.shutdown
            .take()
            .expect("restartable Meta server has a shutdown channel")
            .send(())
            .unwrap();
        self.server
            .take()
            .expect("restartable Meta server is running")
            .await
            .unwrap()
            .unwrap();
    }
}

impl RestartableShard {
    fn new(root: PathBuf, address: SocketAddr, placement: Placement) -> Self {
        Self {
            root,
            address,
            backend_address: reserve_address(),
            placement,
            host: None,
            server_shutdown: None,
            server: None,
            proxy: None,
            proxy_connections: Arc::new(Mutex::new(Vec::new())),
        }
    }

    async fn start(&mut self) {
        assert!(self.host.is_none());
        assert!(self.server.is_none());
        let node_id = self.placement.voters()[0];
        let host = Arc::new(
            DataNodeHost::open(data_config(&self.root, node_id), 32)
                .await
                .unwrap(),
        );
        host.ensure_replica(
            ReplicaSpec::new(
                7,
                self.placement.shard_id(),
                self.placement.epoch(),
                self.placement.voters().to_vec(),
                ReplicaRole::Voter,
                1,
                1,
                format!("restartable-shard-{}", self.placement.shard_id()),
            )
            .unwrap(),
        )
        .await
        .unwrap();
        host.campaign(ReplicaKey::new(7, self.placement.shard_id()).unwrap())
            .await
            .unwrap();
        let listener = tokio::net::TcpListener::bind(self.backend_address)
            .await
            .unwrap();
        let service_host = Arc::clone(&host);
        let (server_shutdown, server_shutdown_receiver) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(
            Server::builder()
                .add_service(ShardServiceServer::new(DataNodeGrpcService::new(
                    service_host,
                )))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    let _ = server_shutdown_receiver.await;
                }),
        );
        let proxy_listener = tokio::net::TcpListener::bind(self.address).await.unwrap();
        let backend_address = self.backend_address;
        let proxy_connections = Arc::clone(&self.proxy_connections);
        let proxy = tokio::spawn(async move {
            loop {
                let (mut inbound, _) = proxy_listener.accept().await?;
                let mut connections = proxy_connections.lock().await;
                let connection = tokio::spawn(async move {
                    let mut outbound = tokio::net::TcpStream::connect(backend_address).await?;
                    tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await?;
                    Ok(())
                });
                connections.push(connection);
            }
        });
        self.host = Some(host);
        self.server_shutdown = Some(server_shutdown);
        self.server = Some(server);
        self.proxy = Some(proxy);
    }

    async fn stop(&mut self) {
        let proxy = self
            .proxy
            .take()
            .expect("restartable Shard proxy is running");
        proxy.abort();
        assert!(proxy.await.unwrap_err().is_cancelled());
        let connections = {
            let mut connections = self.proxy_connections.lock().await;
            std::mem::take(&mut *connections)
        };
        for connection in connections {
            connection.abort();
            let _ = connection.await;
        }
        self.server_shutdown
            .take()
            .expect("restartable Shard server has a shutdown channel")
            .send(())
            .unwrap();
        let server = self.server.take().expect("restartable Shard is running");
        server.await.unwrap().unwrap();
        let host = self.host.take().expect("restartable Shard has a host");
        Arc::try_unwrap(host)
            .unwrap_or_else(|_| panic!("Shard server retained the DataNode host"))
            .shutdown()
            .await
            .unwrap();
    }
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
