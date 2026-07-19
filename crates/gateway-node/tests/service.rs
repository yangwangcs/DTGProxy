use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bolt_protocol::ClientMessage;
use bolt_server::{BoltMachine, ServerMessage};
use cluster_protocol::CLUSTER_PROTOCOL_VERSION;
use cluster_protocol::proto::gateway_service_client::GatewayServiceClient;
use cluster_protocol::proto::gateway_service_server::GatewayServiceServer;
use cluster_protocol::proto::meta_service_server::{MetaService, MetaServiceServer};
use cluster_protocol::proto::shard_service_server::ShardServiceServer;
use cluster_protocol::proto::{GatewaySubmitRequest, ProposeRequest, RequestContext};
use control_plane::{
    BackendProfile, CatalogCommand, DeploymentMode, GraphDefinition, Placement, TopologyDefinition,
};
use cypher_engine::CypherBoltService;
use data_node::{
    DataNodeGrpcService, DataNodeHost, NodeConfig, NodeIdentity, ReplicaKey, ReplicaRole,
    ReplicaSpec, TransportSecurity,
};
use dtgproxy::DeploymentConfig;
use dtgproxy::gateway::{ApiMutation, GATEWAY_API_VERSION, GatewayOperation, GatewayRequest};
use gateway_node::{GatewayCatalogRouter, RemoteGatewayService};
use meta_node::{MetaNodeService, MetaRaftReplica, ReplicatedTso};
use shard_client::RemoteShardClient;
use storage_api::AdapterRequirement;
use temporal_ir::GraphScope;
use temporal_storage::{GraphId, PartitionId};
use temporal_types::{CanonicalElement, GraphValue};
use timestamp_oracle::ManualClock;
use tokio::sync::Mutex;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::Request;
use tonic::transport::Server;

const CLUSTER_ID: [u8; 16] = [0x73; 16];

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
        "social",
        1,
        TopologyDefinition::new(
            DeploymentMode::SharedNothing,
            99,
            128,
            1,
            vec![
                Placement::new(10, 1, vec![10]).unwrap(),
                Placement::new(20, 1, vec![20]).unwrap(),
            ],
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

fn data_config(root: &std::path::Path, node_id: u64) -> NodeConfig {
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

async fn submit(
    client: &mut GatewayServiceClient<tonic::transport::Channel>,
    request_id: u128,
    request: &GatewayRequest,
) -> serde_json::Value {
    let response = client
        .submit(Request::new(GatewaySubmitRequest {
            context: Some(context(request_id)),
            request_json: serde_json::to_vec(request).unwrap(),
        }))
        .await
        .unwrap()
        .into_inner();
    serde_json::from_slice(&response.response_json).unwrap()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remote_gateway_commits_and_queries_a_cross_shard_temporal_transaction() {
    let temporary = tempfile::tempdir().unwrap();
    let graph = graph();
    let meta = elected_meta(&temporary.path().join("meta"));
    meta.propose(Request::new(ProposeRequest {
        context: Some(context(101)),
        command: CatalogCommand::create_graph(101, 0, graph.clone())
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

    let host_10 = Arc::new(
        DataNodeHost::open(data_config(&temporary.path().join("data-10"), 10), 32)
            .await
            .unwrap(),
    );
    let host_20 = Arc::new(
        DataNodeHost::open(data_config(&temporary.path().join("data-20"), 20), 32)
            .await
            .unwrap(),
    );
    for (host, node_id, shard_id) in [(&host_10, 10, 10), (&host_20, 20, 20)] {
        host.ensure_replica(
            ReplicaSpec::new(
                7,
                shard_id,
                1,
                vec![node_id],
                ReplicaRole::Voter,
                1,
                1,
                format!("shard-{shard_id}"),
            )
            .unwrap(),
        )
        .await
        .unwrap();
        host.campaign(ReplicaKey::new(7, shard_id).unwrap())
            .await
            .unwrap();
    }
    let (data_10, shutdown_10, server_10) = serve_shard(Arc::clone(&host_10)).await;
    let (data_20, shutdown_20, server_20) = serve_shard(Arc::clone(&host_20)).await;

    let router = GatewayCatalogRouter::new(
        CLUSTER_ID,
        31,
        7,
        vec![meta_address],
        BTreeMap::from([(10, data_10), (20, data_20)]),
        Duration::from_secs(5),
    )
    .unwrap();
    let snapshot = router.load(1).await.unwrap();
    let (_, authoritative_graph, topology) = snapshot.into_parts();
    let remote_client =
        Arc::new(RemoteShardClient::new_loopback_plaintext(CLUSTER_ID, topology).unwrap());
    let gateway = RemoteGatewayService::new_at_revision(
        CLUSTER_ID,
        1,
        authoritative_graph,
        remote_client,
        vec![meta_address],
        32,
        64,
    )
    .unwrap();
    let gateway_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let gateway_address = gateway_listener.local_addr().unwrap();
    let (gateway_shutdown, gateway_shutdown_rx) = tokio::sync::oneshot::channel();
    let bolt_gateway = gateway.clone();
    let gateway_server = tokio::spawn(
        Server::builder()
            .add_service(GatewayServiceServer::new(gateway))
            .serve_with_incoming_shutdown(TcpListenerStream::new(gateway_listener), async {
                let _ = gateway_shutdown_rx.await;
            }),
    );
    let mut client = GatewayServiceClient::connect(format!("http://{gateway_address}"))
        .await
        .unwrap();

    let deployment = DeploymentConfig::from_catalog(&graph).unwrap();
    let scopes = (0..1_000)
        .map(|partition| {
            let scope = GraphScope::new(GraphId::new(7), PartitionId::new(partition));
            (deployment.route_scope(scope).shard_id(), scope)
        })
        .fold(BTreeMap::new(), |mut scopes, (shard, scope)| {
            scopes.entry(shard).or_insert(scope);
            scopes
        });
    let scope_10 = scopes[&10];
    let scope_20 = scopes[&20];
    let payload = CanonicalElement::new(
        1,
        BTreeMap::from([(1, GraphValue::String("remote-gateway".into()))]),
    );
    let transaction = GatewayRequest {
        version: GATEWAY_API_VERSION,
        request_id: "cross-shard-transaction".into(),
        operation: GatewayOperation::Transaction {
            schema_version: 1,
            ttl_micros: 60_000_000,
            mutations: vec![
                ApiMutation::PutVertex {
                    partition: scope_10.partition().value(),
                    vertex_id: "10".into(),
                    label_id: 1,
                    valid_from_micros: 0,
                    valid_to_micros: None,
                    payload_dtp1: hex(&payload.encode().unwrap()),
                },
                ApiMutation::PutVertex {
                    partition: scope_20.partition().value(),
                    vertex_id: "20".into(),
                    label_id: 1,
                    valid_from_micros: 0,
                    valid_to_micros: None,
                    payload_dtp1: hex(&payload.encode().unwrap()),
                },
            ],
        },
    };
    let committed = submit(&mut client, 201, &transaction).await;
    assert_eq!(committed["ok"], true, "{committed}");
    assert_eq!(
        committed["result"]["participants"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert_eq!(committed["result"]["single_shard_fast_path"], false);

    for (request_id, scope, vertex_id) in [(202, scope_10, 10_u128), (203, scope_20, 20_u128)] {
        let query = GatewayRequest {
            version: GATEWAY_API_VERSION,
            request_id: format!("query-{vertex_id}"),
            operation: GatewayOperation::Query {
                text: format!(
                    "VERTEX {vertex_id} GRAPH 7 PARTITION {} FOR VALID TIME 1 CURRENT LIMIT 1",
                    scope.partition().value()
                ),
            },
        };
        let queried = submit(&mut client, request_id, &query).await;
        assert_eq!(queried["ok"], true, "{queried}");
        assert_eq!(
            queried["result"]["records"][0]["element_id"],
            vertex_id.to_string()
        );
    }

    let cypher_write = GatewayRequest {
        version: GATEWAY_API_VERSION,
        request_id: "cypher-write".into(),
        operation: GatewayOperation::Query {
            text: "USE social AT VALID_TIME AS OF 1000 CREATE (a:Person {name: 'Ada'})-[r:KNOWS]->(b:Person {name: 'Bob'}) SET a.status = 'active'".into(),
        },
    };
    let written = submit(&mut client, 204, &cypher_write).await;
    assert_eq!(written["ok"], true, "{written}");
    assert_eq!(written["result"]["kind"], "cypher_write");
    assert_eq!(written["result"]["bindings"]["a"]["kind"], "vertex");
    assert_eq!(written["result"]["bindings"]["r"]["kind"], "relationship");
    assert!(written["result"]["commit_ts"].is_object());

    let cypher_read = GatewayRequest {
        version: GATEWAY_API_VERSION,
        request_id: "cypher-read-after-write".into(),
        operation: GatewayOperation::Query {
            text: "USE social AT VALID_TIME AS OF 1000 MATCH (n:Person) RETURN n".into(),
        },
    };
    let queried = submit(&mut client, 205, &cypher_read).await;
    assert_eq!(queried["ok"], true, "{queried}");
    assert_eq!(queried["result"]["row_count"], 2);

    let analytics_call = GatewayRequest {
        version: GATEWAY_API_VERSION,
        request_id: "analytics-degree".into(),
        operation: GatewayOperation::Query {
            text:
                "USE social AT VALID_TIME AS OF 1000 CALL dtg.graph.degree() YIELD vertexId, degree"
                    .into(),
        },
    };
    let analytics = submit(&mut client, 206, &analytics_call).await;
    assert_eq!(analytics["ok"], true, "{analytics}");
    assert_eq!(analytics["result"]["kind"], "analytics_result");
    assert_eq!(analytics["result"]["columns"][0], "vertexId");
    assert!(analytics["result"]["rows"].as_array().unwrap().len() >= 2);

    let bolt_service = Arc::new(CypherBoltService::new(Arc::new(bolt_gateway), 16).unwrap());
    let mut bolt = BoltMachine::new(bolt_service);
    assert!(matches!(
        bolt.handle(ClientMessage::Hello(BTreeMap::new()))
            .await
            .as_slice(),
        [ServerMessage::Success(_)]
    ));
    assert!(matches!(
        bolt.handle(ClientMessage::Begin(BTreeMap::new()))
            .await
            .as_slice(),
        [ServerMessage::Success(_)]
    ));
    let staged = bolt
        .handle(ClientMessage::Run {
            query: "CREATE (rolled_back:Person {name: 'Rollback'})".into(),
            parameters: BTreeMap::new(),
            extra: BTreeMap::new(),
        })
        .await;
    assert!(matches!(staged.as_slice(), [ServerMessage::Success(_)]));
    let _ = bolt
        .handle(ClientMessage::Pull {
            n: -1,
            query_id: None,
        })
        .await;
    assert!(matches!(
        bolt.handle(ClientMessage::Rollback).await.as_slice(),
        [ServerMessage::Success(_)]
    ));
    let committed = bolt.handle(ClientMessage::Begin(BTreeMap::new())).await;
    assert!(matches!(committed.as_slice(), [ServerMessage::Success(_)]));
    let staged = bolt
        .handle(ClientMessage::Run {
            query: "CREATE (committed:Person {name: 'Committed'})".into(),
            parameters: BTreeMap::new(),
            extra: BTreeMap::new(),
        })
        .await;
    assert!(matches!(staged.as_slice(), [ServerMessage::Success(_)]));
    let _ = bolt
        .handle(ClientMessage::Discard {
            n: -1,
            query_id: None,
        })
        .await;
    assert!(matches!(
        bolt.handle(ClientMessage::Commit).await.as_slice(),
        [ServerMessage::Success(_)]
    ));

    drop(client);
    gateway_shutdown.send(()).unwrap();
    gateway_server.await.unwrap().unwrap();
    shutdown_10.send(()).unwrap();
    shutdown_20.send(()).unwrap();
    server_10.await.unwrap().unwrap();
    server_20.await.unwrap().unwrap();
    meta_shutdown.send(()).unwrap();
    meta_server.await.unwrap().unwrap();
    Arc::try_unwrap(host_10)
        .ok()
        .unwrap()
        .shutdown()
        .await
        .unwrap();
    Arc::try_unwrap(host_20)
        .ok()
        .unwrap()
        .shutdown()
        .await
        .unwrap();
}
