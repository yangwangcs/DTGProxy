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
use cypher_engine::{BoltQueryBackend, BoltQueryRequest, CypherBoltService};
use data_node::{
    DataNodeGrpcService, DataNodeHost, NodeConfig, NodeIdentity, ReplicaKey, ReplicaRole,
    ReplicaSpec, TransportSecurity,
};
use dtgproxy::DeploymentConfig;
use dtgproxy::gateway::{ApiMutation, GATEWAY_API_VERSION, GatewayOperation, GatewayRequest};
use gateway_node::{GatewayCatalogRouter, RemoteGatewayService};
use meta_node::{MetaNodeService, MetaRaftReplica, ReplicatedTso};
use query_executor::RuntimeValue;
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

    let query = GatewayRequest {
        version: GATEWAY_API_VERSION,
        request_id: "query-cross-shard".into(),
        operation: GatewayOperation::Cypher {
            text: "USE social AT VALID_TIME AS OF 1 MATCH (n) RETURN n".into(),
        },
    };
    let queried = submit(&mut client, 202, &query).await;
    assert_eq!(queried["ok"], true, "{queried}");
    assert_eq!(queried["result"]["row_count"], 2);

    let cypher_write = GatewayRequest {
        version: GATEWAY_API_VERSION,
        request_id: "cypher-write".into(),
        operation: GatewayOperation::Cypher {
            text: "USE social AT VALID_TIME AS OF 1000 CREATE (a:Person {name: 'Ada'})-[r:KNOWS]->(b:Person {name: 'Bob'}) SET a.status = 'active'".into(),
        },
    };
    let written = submit(&mut client, 204, &cypher_write).await;
    assert_eq!(written["ok"], true, "{written}");
    assert_eq!(written["result"]["kind"], "cypher_write");
    assert_eq!(written["result"]["bindings"]["a"]["kind"], "vertex");
    assert_eq!(written["result"]["bindings"]["r"]["kind"], "relationship");
    assert!(written["result"]["commit_ts"].is_object());

    let merge_created = GatewayRequest {
        version: GATEWAY_API_VERSION,
        request_id: "cypher-merge-created".into(),
        operation: GatewayOperation::Cypher {
            text: "USE social AT VALID_TIME AS OF 1000 MERGE (g:Person {name: 'Grace'})".into(),
        },
    };
    let merged_created = submit(&mut client, 2045, &merge_created).await;
    assert_eq!(merged_created["ok"], true, "{merged_created}");
    assert_eq!(merged_created["result"]["kind"], "cypher_write");
    assert!(
        merged_created["result"]["participants"]
            .as_array()
            .is_some_and(|participants| !participants.is_empty())
    );

    let merge_same_constraint = GatewayRequest {
        version: GATEWAY_API_VERSION,
        request_id: "cypher-merge-same-constraint".into(),
        operation: GatewayOperation::Cypher {
            text: "USE social AT VALID_TIME AS OF 1000 MERGE (g:Person {name: 'Grace'})".into(),
        },
    };
    let merged_same_constraint = submit(&mut client, 2046, &merge_same_constraint).await;
    assert_eq!(
        merged_same_constraint["ok"], true,
        "{merged_same_constraint}"
    );
    assert_eq!(
        merged_same_constraint["result"]["bindings"]["g"]["element_id"],
        merged_created["result"]["bindings"]["g"]["element_id"]
    );
    assert_eq!(
        merged_same_constraint["result"]["bindings"]["g"]["partition"],
        merged_created["result"]["bindings"]["g"]["partition"]
    );

    let concurrent_merge = |request_id: &str| GatewayRequest {
        version: GATEWAY_API_VERSION,
        request_id: request_id.into(),
        operation: GatewayOperation::Cypher {
            text: "USE social AT VALID_TIME AS OF 1000 MERGE (c:Person {name: 'Concurrent'})"
                .into(),
        },
    };
    let mut concurrent_left = client.clone();
    let mut concurrent_right = client.clone();
    let concurrent_left_request = concurrent_merge("cypher-merge-concurrent-left");
    let concurrent_right_request = concurrent_merge("cypher-merge-concurrent-right");
    let (merged_left, merged_right) = tokio::join!(
        submit(&mut concurrent_left, 2047, &concurrent_left_request),
        submit(&mut concurrent_right, 2048, &concurrent_right_request,),
    );
    assert_eq!(merged_left["ok"], true, "{merged_left}");
    assert_eq!(merged_right["ok"], true, "{merged_right}");
    assert_eq!(
        merged_left["result"]["bindings"]["c"]["element_id"],
        merged_right["result"]["bindings"]["c"]["element_id"]
    );
    assert_eq!(
        merged_left["result"]["bindings"]["c"]["partition"],
        merged_right["result"]["bindings"]["c"]["partition"]
    );

    let merge_path = |request_id: &str| {
        GatewayRequest {
        version: GATEWAY_API_VERSION,
        request_id: request_id.into(),
        operation: GatewayOperation::Cypher {
            text: "USE social AT VALID_TIME AS OF 1000 MERGE (path_left:MergePath {name: 'Left'})-[path_edge:PATH_LINK]->(path_right:MergePath {name: 'Right'})".into(),
        },
    }
    };
    let merged_path = submit(&mut client, 2061, &merge_path("cypher-merge-path")).await;
    assert_eq!(merged_path["ok"], true, "{merged_path}");
    let repeated_path = submit(&mut client, 2062, &merge_path("cypher-merge-path-repeated")).await;
    assert_eq!(repeated_path["ok"], true, "{repeated_path}");
    for name in ["path_left", "path_edge", "path_right"] {
        assert_eq!(
            repeated_path["result"]["bindings"][name]["element_id"],
            merged_path["result"]["bindings"][name]["element_id"],
            "repeated path MERGE changed {name}: {repeated_path}"
        );
        assert_eq!(
            repeated_path["result"]["bindings"][name]["partition"],
            merged_path["result"]["bindings"][name]["partition"],
            "repeated path MERGE moved {name}: {repeated_path}"
        );
    }
    let path_nodes = submit(
        &mut client,
        2063,
        &GatewayRequest {
            version: GATEWAY_API_VERSION,
            request_id: "query-merge-path-nodes".into(),
            operation: GatewayOperation::Cypher {
                text: "USE social AT VALID_TIME AS OF 1000 MATCH (n:MergePath) RETURN n".into(),
            },
        },
    )
    .await;
    assert_eq!(path_nodes["ok"], true, "{path_nodes}");
    assert_eq!(path_nodes["result"]["row_count"], 2, "{path_nodes}");
    let path_edges = submit(
        &mut client,
        2064,
        &GatewayRequest {
            version: GATEWAY_API_VERSION,
            request_id: "query-merge-path-edges".into(),
            operation: GatewayOperation::Cypher {
                text: "USE social AT VALID_TIME AS OF 1000 MATCH (:MergePath)-[r:PATH_LINK]->(:MergePath) RETURN r".into(),
            },
        },
    )
    .await;
    assert_eq!(path_edges["ok"], true, "{path_edges}");
    assert_eq!(path_edges["result"]["row_count"], 1, "{path_edges}");

    let concurrent_path = |request_id: &str| {
        GatewayRequest {
        version: GATEWAY_API_VERSION,
        request_id: request_id.into(),
        operation: GatewayOperation::Cypher {
            text: "USE social AT VALID_TIME AS OF 1000 MERGE (concurrent_left:ConcurrentPath {name: 'Left'})-[concurrent_edge:CONCURRENT_LINK]->(concurrent_right:ConcurrentPath {name: 'Right'})".into(),
        },
    }
    };
    let mut concurrent_path_left = client.clone();
    let mut concurrent_path_right = client.clone();
    let concurrent_path_left_request = concurrent_path("cypher-merge-concurrent-path-left");
    let concurrent_path_right_request = concurrent_path("cypher-merge-concurrent-path-right");
    let (merged_path_left, merged_path_right) = tokio::join!(
        submit(
            &mut concurrent_path_left,
            2065,
            &concurrent_path_left_request,
        ),
        submit(
            &mut concurrent_path_right,
            2066,
            &concurrent_path_right_request,
        ),
    );
    assert_eq!(merged_path_left["ok"], true, "{merged_path_left}");
    assert_eq!(merged_path_right["ok"], true, "{merged_path_right}");
    for name in ["concurrent_left", "concurrent_edge", "concurrent_right"] {
        assert_eq!(
            merged_path_left["result"]["bindings"][name]["element_id"],
            merged_path_right["result"]["bindings"][name]["element_id"],
            "concurrent path MERGE diverged for {name}"
        );
        assert_eq!(
            merged_path_left["result"]["bindings"][name]["partition"],
            merged_path_right["result"]["bindings"][name]["partition"],
            "concurrent path MERGE moved {name}"
        );
    }

    let bound_relationship = |request_id: &str| {
        GatewayRequest {
        version: GATEWAY_API_VERSION,
        request_id: request_id.into(),
        operation: GatewayOperation::Cypher {
            text: "USE social AT VALID_TIME AS OF 1000 MATCH (a:MergePath {name: 'Left'})-[:PATH_LINK]->(b:MergePath {name: 'Right'}) MERGE (a)-[bound_edge:BOUND_LINK {kind: 'bound'}]->(b)".into(),
        },
    }
    };
    let merged_bound = submit(
        &mut client,
        2067,
        &bound_relationship("cypher-merge-bound-edge"),
    )
    .await;
    assert_eq!(merged_bound["ok"], true, "{merged_bound}");
    let repeated_bound = submit(
        &mut client,
        2068,
        &bound_relationship("cypher-merge-bound-edge-repeated"),
    )
    .await;
    assert_eq!(repeated_bound["ok"], true, "{repeated_bound}");
    assert_eq!(
        repeated_bound["result"]["bindings"]["bound_edge"]["element_id"],
        merged_bound["result"]["bindings"]["bound_edge"]["element_id"]
    );
    assert_eq!(
        repeated_bound["result"]["bindings"]["bound_edge"]["partition"],
        merged_bound["result"]["bindings"]["bound_edge"]["partition"]
    );

    for (request_id, pair) in [(2070_u128, 1), (2071, 2)] {
        let created_pair = submit(
            &mut client,
            request_id,
            &GatewayRequest {
                version: GATEWAY_API_VERSION,
                request_id: format!("create-multi-endpoint-pair-{pair}"),
                operation: GatewayOperation::Cypher {
                    text: format!(
                        "USE social AT VALID_TIME AS OF 1000 CREATE (left_{pair}:MultiEndpoint {{pair: {pair}, side: 'left'}})-[pair_edge_{pair}:MULTI_PAIR]->(right_{pair}:MultiEndpoint {{pair: {pair}, side: 'right'}})"
                    ),
                },
            },
        )
        .await;
        assert_eq!(created_pair["ok"], true, "{created_pair}");
        assert_eq!(
            created_pair["result"]["bindings"][format!("pair_edge_{pair}")]["kind"],
            "relationship",
            "{created_pair}"
        );
    }
    let mut remote_endpoint_tokens = BTreeMap::new();
    for candidate in 0..1_000_u32 {
        let token = format!("remote-endpoint-{candidate}");
        let created = submit(
            &mut client,
            30_000 + u128::from(candidate),
            &GatewayRequest {
                version: GATEWAY_API_VERSION,
                request_id: format!("create-{token}"),
                operation: GatewayOperation::Cypher {
                    text: format!(
                        "USE social AT VALID_TIME AS OF 1000 MERGE (endpoint:RemoteEndpoint {{token: '{token}'}})"
                    ),
                },
            },
        )
        .await;
        assert_eq!(created["ok"], true, "{created}");
        let partition = created["result"]["bindings"]["endpoint"]["partition"]
            .as_u64()
            .expect("remote endpoint partition");
        let shard = deployment
            .route_scope(GraphScope::new(
                GraphId::new(7),
                PartitionId::new(u32::try_from(partition).expect("partition fits u32")),
            ))
            .shard_id();
        remote_endpoint_tokens.entry(shard).or_insert(token);
        if remote_endpoint_tokens.contains_key(&10) && remote_endpoint_tokens.contains_key(&20) {
            break;
        }
    }
    assert_eq!(
        remote_endpoint_tokens.keys().copied().collect::<Vec<_>>(),
        vec![10, 20],
        "fixture must commit endpoints on both physical shards"
    );
    let upstream_nodes = submit(
        &mut client,
        20714,
        &GatewayRequest {
            version: GATEWAY_API_VERSION,
            request_id: "query-multi-endpoint-nodes".into(),
            operation: GatewayOperation::Cypher {
                text: "USE social AT VALID_TIME AS OF 1000 MATCH (n:MultiEndpoint) RETURN n".into(),
            },
        },
    )
    .await;
    assert_eq!(upstream_nodes["ok"], true, "{upstream_nodes}");
    assert_eq!(upstream_nodes["result"]["row_count"], 4, "{upstream_nodes}");
    let upstream_pairs = submit(
        &mut client,
        20715,
        &GatewayRequest {
            version: GATEWAY_API_VERSION,
            request_id: "query-multi-endpoint-pairs".into(),
            operation: GatewayOperation::Cypher {
                text: "USE social AT VALID_TIME AS OF 1000 MATCH (:MultiEndpoint)-[r:MULTI_PAIR]->(:MultiEndpoint) RETURN r".into(),
            },
        },
    )
    .await;
    assert_eq!(upstream_pairs["ok"], true, "{upstream_pairs}");
    assert_eq!(upstream_pairs["result"]["row_count"], 2, "{upstream_pairs}");
    let interval_pairs = submit(
        &mut client,
        207_151,
        &GatewayRequest {
            version: GATEWAY_API_VERSION,
            request_id: "query-multi-endpoint-pairs-interval".into(),
            operation: GatewayOperation::Cypher {
                text: "USE social AT VALID_TIME FROM 999 TO 1001 \
                       MATCH (:MultiEndpoint)-[r:MULTI_PAIR]->(:MultiEndpoint) RETURN r"
                    .into(),
            },
        },
    )
    .await;
    assert_eq!(interval_pairs["ok"], true, "{interval_pairs}");
    assert_eq!(
        interval_pairs["result"]["row_count"], 2,
        "interval Expand must hydrate remote destination segments: {interval_pairs}"
    );
    let batched_write = submit(
        &mut client,
        20_716,
        &GatewayRequest {
            version: GATEWAY_API_VERSION,
            request_id: "write-in-transactions".into(),
            operation: GatewayOperation::Cypher {
                text: "USE social AT VALID_TIME AS OF 1000 \
                       UNWIND [1, 2, 3] AS value \
                       CALL (value) { CREATE (:BatchedItem {id: value}) } \
                       IN TRANSACTIONS OF 2 ROWS"
                    .into(),
            },
        },
    )
    .await;
    assert_eq!(batched_write["ok"], true, "{batched_write}");
    assert_eq!(
        batched_write["result"]["kind"], "cypher_batch_write",
        "{batched_write}"
    );
    assert_eq!(
        batched_write["result"]["committed_batch_count"], 2,
        "{batched_write}"
    );
    assert_eq!(batched_write["result"]["batches"][0]["row_count"], 2);
    assert_eq!(batched_write["result"]["batches"][1]["row_count"], 1);
    assert_ne!(
        batched_write["result"]["batches"][0]["transaction_id"],
        batched_write["result"]["batches"][1]["transaction_id"]
    );
    let replayed_batched_write = submit(
        &mut client,
        20_716,
        &GatewayRequest {
            version: GATEWAY_API_VERSION,
            request_id: "write-in-transactions".into(),
            operation: GatewayOperation::Cypher {
                text: "USE social AT VALID_TIME AS OF 1000 \
                       UNWIND [1, 2, 3] AS value \
                       CALL (value) { CREATE (:BatchedItem {id: value}) } \
                       IN TRANSACTIONS OF 2 ROWS"
                    .into(),
            },
        },
    )
    .await;
    assert_eq!(replayed_batched_write, batched_write);
    let batched_rows = submit(
        &mut client,
        20_717,
        &GatewayRequest {
            version: GATEWAY_API_VERSION,
            request_id: "read-in-transactions-results".into(),
            operation: GatewayOperation::Cypher {
                text: "USE social AT VALID_TIME AS OF 1000 MATCH (n:BatchedItem) RETURN count(n)"
                    .into(),
            },
        },
    )
    .await;
    assert_eq!(batched_rows["ok"], true, "{batched_rows}");
    assert_eq!(batched_rows["result"]["rows"][0][0], 3, "{batched_rows}");
    let partial_batch_request = GatewayRequest {
        version: GATEWAY_API_VERSION,
        request_id: "write-in-transactions-partial-failure".into(),
        operation: GatewayOperation::Cypher {
            text: "USE social AT VALID_TIME AS OF 1000 \
                   UNWIND [1, 2, {bad: 3}] AS value \
                   CALL (value) { CREATE (:BatchPartial {id: value}) } \
                   IN TRANSACTIONS OF 2 ROWS"
                .into(),
        },
    };
    let partial_batch = submit(&mut client, 20_718, &partial_batch_request).await;
    assert_eq!(partial_batch["ok"], false, "{partial_batch}");
    let partial_error = partial_batch["error"].as_str().expect("batch error");
    assert!(
        partial_error.contains("DTG-CYPHER-IN-TRANSACTIONS-BATCH-FAILED")
            && partial_error.contains("\"failing_batch\":1")
            && partial_error.contains("\"committed_batch_count\":1")
            && partial_error.contains("\"committed_row_count\":2")
            && partial_error.contains("\"statement_rolled_back\":false"),
        "partial batch report is incomplete: {partial_batch}"
    );
    let replayed_partial_batch = submit(&mut client, 20_718, &partial_batch_request).await;
    assert_eq!(
        replayed_partial_batch["ok"], false,
        "{replayed_partial_batch}"
    );
    let partial_rows = submit(
        &mut client,
        20_719,
        &GatewayRequest {
            version: GATEWAY_API_VERSION,
            request_id: "read-partial-batch-results".into(),
            operation: GatewayOperation::Cypher {
                text: "USE social AT VALID_TIME AS OF 1000 MATCH (n:BatchPartial) RETURN count(n)"
                    .into(),
            },
        },
    )
    .await;
    assert_eq!(partial_rows["ok"], true, "{partial_rows}");
    assert_eq!(partial_rows["result"]["rows"][0][0], 2, "{partial_rows}");
    let multi_row_merge = |request_id: &str| {
        GatewayRequest {
        version: GATEWAY_API_VERSION,
        request_id: request_id.into(),
        operation: GatewayOperation::Cypher {
            text: "USE social AT VALID_TIME AS OF 1000 MATCH (a:MultiEndpoint)-[:MULTI_PAIR]->(b:MultiEndpoint) MERGE (a)-[row_edge:MULTI_BOUND]->(b)".into(),
        },
    }
    };
    let merged_rows = submit(
        &mut client,
        2072,
        &multi_row_merge("cypher-multi-row-merge"),
    )
    .await;
    assert_eq!(merged_rows["ok"], true, "{merged_rows}");
    let repeated_rows = submit(
        &mut client,
        2073,
        &multi_row_merge("cypher-multi-row-merge-repeat"),
    )
    .await;
    assert_eq!(repeated_rows["ok"], true, "{repeated_rows}");
    let multi_row_edges = submit(
        &mut client,
        2074,
        &GatewayRequest {
            version: GATEWAY_API_VERSION,
            request_id: "query-multi-row-merge-edges".into(),
            operation: GatewayOperation::Cypher {
                text: "USE social AT VALID_TIME AS OF 1000 MATCH (:MultiEndpoint)-[r:MULTI_BOUND]->(:MultiEndpoint) RETURN r".into(),
            },
        },
    )
    .await;
    assert_eq!(multi_row_edges["ok"], true, "{multi_row_edges}");
    assert_eq!(
        multi_row_edges["result"]["row_count"], 2,
        "{multi_row_edges}"
    );

    let anonymous_then_named = |request_id: &str| {
        GatewayRequest {
        version: GATEWAY_API_VERSION,
        request_id: request_id.into(),
        operation: GatewayOperation::Cypher {
            text: "USE social AT VALID_TIME AS OF 1000 MERGE (:AnonymousFirst {id: 1}) MERGE (named_second:NamedSecond {id: 2})".into(),
        },
    }
    };
    let anonymous_named_created = submit(
        &mut client,
        2075,
        &anonymous_then_named("anonymous-first-named-second"),
    )
    .await;
    assert_eq!(
        anonymous_named_created["ok"], true,
        "{anonymous_named_created}"
    );
    let anonymous_named_repeated = submit(
        &mut client,
        30_076,
        &anonymous_then_named("anonymous-first-named-second-repeat"),
    )
    .await;
    assert_eq!(
        anonymous_named_repeated["ok"], true,
        "{anonymous_named_repeated}"
    );
    assert_eq!(
        anonymous_named_repeated["result"]["bindings"]["named_second"]["element_id"],
        anonymous_named_created["result"]["bindings"]["named_second"]["element_id"]
    );

    let identical_aliases = |request_id: &str| {
        GatewayRequest {
        version: GATEWAY_API_VERSION,
        request_id: request_id.into(),
        operation: GatewayOperation::Cypher {
            text: "USE social AT VALID_TIME AS OF 1000 MERGE (alias_a:AliasPerson {id: 1}) MERGE (alias_b:AliasPerson {id: 1})".into(),
        },
    }
    };
    let aliases_created = submit(
        &mut client,
        20765,
        &identical_aliases("identical-merge-aliases"),
    )
    .await;
    assert_eq!(aliases_created["ok"], true, "{aliases_created}");
    assert_eq!(
        aliases_created["result"]["bindings"]["alias_a"]["element_id"],
        aliases_created["result"]["bindings"]["alias_b"]["element_id"]
    );
    let aliases_repeated = submit(
        &mut client,
        20766,
        &identical_aliases("identical-merge-aliases-repeat"),
    )
    .await;
    assert_eq!(aliases_repeated["ok"], true, "{aliases_repeated}");
    for name in ["alias_a", "alias_b"] {
        assert_eq!(
            aliases_repeated["result"]["bindings"][name]["element_id"],
            aliases_created["result"]["bindings"][name]["element_id"]
        );
    }

    let concurrent_path_nodes = submit(
        &mut client,
        2088,
        &GatewayRequest {
            version: GATEWAY_API_VERSION,
            request_id: "query-concurrent-path-nodes".into(),
            operation: GatewayOperation::Cypher {
                text: "USE social AT VALID_TIME AS OF 1000 MATCH (n:ConcurrentPath) RETURN n"
                    .into(),
            },
        },
    )
    .await;
    assert_eq!(concurrent_path_nodes["ok"], true, "{concurrent_path_nodes}");
    assert_eq!(concurrent_path_nodes["result"]["row_count"], 2);
    let concurrent_path_edges = submit(
        &mut client,
        92078,
        &GatewayRequest {
            version: GATEWAY_API_VERSION,
            request_id: "query-concurrent-path-edges".into(),
            operation: GatewayOperation::Cypher {
                text: "USE social AT VALID_TIME AS OF 1000 MATCH (:ConcurrentPath)-[r:CONCURRENT_LINK]->(:ConcurrentPath) RETURN r".into(),
            },
        },
    )
    .await;
    assert_eq!(concurrent_path_edges["ok"], true, "{concurrent_path_edges}");
    assert_eq!(concurrent_path_edges["result"]["row_count"], 1);

    let empty_match_merge = submit(
        &mut client,
        2069,
        &GatewayRequest {
            version: GATEWAY_API_VERSION,
            request_id: "cypher-empty-match-merge".into(),
            operation: GatewayOperation::Cypher {
                text: "USE social AT VALID_TIME AS OF 1000 MATCH (missing:MissingEndpoint) MERGE (missing)-[never:NEVER_CREATED]->(other:NeverCreated)".into(),
            },
        },
    )
    .await;
    assert_eq!(empty_match_merge["ok"], true, "{empty_match_merge}");
    assert_eq!(
        empty_match_merge["result"]["bindings"],
        serde_json::json!({}),
        "an empty upstream MATCH must not materialize its MERGE: {empty_match_merge}"
    );

    let merge_existing = GatewayRequest {
        version: GATEWAY_API_VERSION,
        request_id: "cypher-merge-existing".into(),
        operation: GatewayOperation::Cypher {
            text: "USE social AT VALID_TIME AS OF 1000 MERGE (m:Person {name: 'Ada'})".into(),
        },
    };
    let merged = submit(&mut client, 2050, &merge_existing).await;
    assert_eq!(merged["ok"], true, "{merged}");
    assert_eq!(merged["result"]["kind"], "cypher_write");

    let cypher_read = GatewayRequest {
        version: GATEWAY_API_VERSION,
        request_id: "cypher-read-after-write".into(),
        operation: GatewayOperation::Cypher {
            text: "USE social AT VALID_TIME AS OF 1000 MATCH (n:Person) RETURN n".into(),
        },
    };
    let queried = submit(&mut client, 205, &cypher_read).await;
    assert_eq!(queried["ok"], true, "{queried}");
    assert_eq!(queried["result"]["row_count"], 4);

    let existing_update = GatewayRequest {
        version: GATEWAY_API_VERSION,
        request_id: "cypher-existing-update".into(),
        operation: GatewayOperation::Cypher {
            text: "USE social AT VALID_TIME AS OF 1000 MATCH (n:Person) WHERE n.name = 'Ada' SET n.status = 'updated'".into(),
        },
    };
    let updated = submit(&mut client, 207, &existing_update).await;
    assert_eq!(updated["ok"], true, "{updated}");
    assert_eq!(updated["result"]["kind"], "cypher_write");

    let analytics_call = GatewayRequest {
        version: GATEWAY_API_VERSION,
        request_id: "analytics-degree".into(),
        operation: GatewayOperation::Cypher {
            text: "USE social AT VALID_TIME AS OF 1000 CALL dtg.graph.degree({}) \
                 YIELD vertexId, degree AS score WITH vertexId, score \
                 WHERE score >= 0 RETURN vertexId, score"
                .into(),
        },
    };
    let analytics = submit(&mut client, 206, &analytics_call).await;
    assert_eq!(analytics["ok"], true, "{analytics}");
    assert_eq!(analytics["result"]["kind"], "cypher_result");
    assert_eq!(analytics["result"]["columns"][0]["name"], "vertexId");
    assert_eq!(analytics["result"]["columns"][0]["type"], "String");
    assert_eq!(analytics["result"]["columns"][1]["name"], "score");
    assert_eq!(analytics["result"]["columns"][1]["type"], "Integer");
    assert!(analytics["result"]["rows"].as_array().unwrap().len() >= 2);

    let interval_analytics = submit(
        &mut client,
        92_065,
        &GatewayRequest {
            version: GATEWAY_API_VERSION,
            request_id: "analytics-interval-components".into(),
            operation: GatewayOperation::Cypher {
                text: "USE social AT VALID_TIME FROM 1000 TO 2000 \
                       CALL dtg.temporal.intervalComponents({}) \
                       YIELD vertexId, componentId RETURN vertexId, componentId"
                    .into(),
            },
        },
    )
    .await;
    assert_eq!(interval_analytics["ok"], true, "{interval_analytics}");
    assert_eq!(
        interval_analytics["result"]["columns"],
        serde_json::json!([
            {"name": "vertexId", "type": "String", "nullable": true},
            {"name": "componentId", "type": "String", "nullable": true}
        ])
    );
    assert!(
        interval_analytics["result"]["rows"]
            .as_array()
            .is_some_and(|rows| rows.len() >= 2),
        "interval projection must merge all shard-local segments before endpoint validation: {interval_analytics}"
    );

    let delta_seed = submit(
        &mut client,
        92_066,
        &GatewayRequest {
            version: GATEWAY_API_VERSION,
            request_id: "analytics-delta-seed".into(),
            operation: GatewayOperation::Cypher {
                text: "USE social AT VALID_TIME AS OF 2000 CREATE (:DeltaOnly)".into(),
            },
        },
    )
    .await;
    assert_eq!(delta_seed["ok"], true, "{delta_seed}");
    let delta_analytics = submit(
        &mut client,
        92_067,
        &GatewayRequest {
            version: GATEWAY_API_VERSION,
            request_id: "analytics-delta-summary".into(),
            operation: GatewayOperation::Cypher {
                text: "USE social AT VALID_TIME FROM 1000 TO 2000 \
                       CALL dtg.temporal.deltaSummary({}) \
                       YIELD entityType, change, count RETURN entityType, change, count"
                    .into(),
            },
        },
    )
    .await;
    assert_eq!(delta_analytics["ok"], true, "{delta_analytics}");
    assert_eq!(
        delta_analytics["result"]["rows"],
        serde_json::json!([["VERTEX", "ADDED", 1]]),
        "delta projection must compare valid-time endpoints at one transaction snapshot"
    );

    let submitted_job = submit(
        &mut client,
        92_070,
        &GatewayRequest {
            version: GATEWAY_API_VERSION,
            request_id: "analytics-job-submit".into(),
            operation: GatewayOperation::Cypher {
                text: "USE social AT VALID_TIME AS OF 1000 \
                       CALL dtg.analytics.submit({algorithm: 'dtg.graph.degree', parameters: {}}) \
                       YIELD jobId RETURN jobId"
                    .into(),
            },
        },
    )
    .await;
    assert_eq!(submitted_job["ok"], true, "{submitted_job}");
    let job_id = submitted_job["result"]["rows"][0][0]
        .as_str()
        .expect("analytics job id")
        .to_owned();
    let retried_job = submit(
        &mut client,
        92_070,
        &GatewayRequest {
            version: GATEWAY_API_VERSION,
            request_id: "analytics-job-submit".into(),
            operation: GatewayOperation::Cypher {
                text: "USE social AT VALID_TIME AS OF 1000 \
                       CALL dtg.analytics.submit({algorithm: 'dtg.graph.degree', parameters: {}}) \
                       YIELD jobId RETURN jobId"
                    .into(),
            },
        },
    )
    .await;
    assert_eq!(retried_job["ok"], true, "{retried_job}");
    assert_eq!(
        retried_job["result"]["rows"][0][0], job_id,
        "retry must return the Meta-canonical original job ID"
    );
    let queued = submit(
        &mut client,
        92_200,
        &GatewayRequest {
            version: GATEWAY_API_VERSION,
            request_id: "analytics-job-status-queued".into(),
            operation: GatewayOperation::Cypher {
                text: format!(
                    "CALL dtg.analytics.status({{jobId: '{job_id}'}}) \
                     YIELD state RETURN state"
                ),
            },
        },
    )
    .await;
    assert_eq!(queued["ok"], true, "{queued}");
    assert_eq!(queued["result"]["rows"], serde_json::json!([["QUEUED"]]));
    let not_ready = submit(
        &mut client,
        92_201,
        &GatewayRequest {
            version: GATEWAY_API_VERSION,
            request_id: "analytics-job-results-not-ready".into(),
            operation: GatewayOperation::Cypher {
                text: format!(
                    "CALL dtg.analytics.results({{jobId: '{job_id}'}}) \
                     YIELD row RETURN row"
                ),
            },
        },
    )
    .await;
    assert_eq!(not_ready["ok"], false, "{not_ready}");
    assert!(
        not_ready["error"]
            .as_str()
            .is_some_and(|message| message.contains("DTG-ANALYTICS-JOB-NOT-READY")),
        "{not_ready}"
    );
    let canceled = submit(
        &mut client,
        92_202,
        &GatewayRequest {
            version: GATEWAY_API_VERSION,
            request_id: "analytics-job-cancel".into(),
            operation: GatewayOperation::Cypher {
                text: format!(
                    "CALL dtg.analytics.cancel({{jobId: '{job_id}'}}) \
                     YIELD canceled RETURN canceled"
                ),
            },
        },
    )
    .await;
    assert_eq!(canceled["ok"], true, "{canceled}");
    assert_eq!(canceled["result"]["rows"], serde_json::json!([[true]]));
    let canceled_status = submit(
        &mut client,
        92_205,
        &GatewayRequest {
            version: GATEWAY_API_VERSION,
            request_id: "analytics-job-status-canceled".into(),
            operation: GatewayOperation::Cypher {
                text: format!(
                    "CALL dtg.analytics.status({{jobId: '{job_id}'}}) \
                     YIELD state RETURN state"
                ),
            },
        },
    )
    .await;
    assert_eq!(canceled_status["ok"], true, "{canceled_status}");
    assert_eq!(
        canceled_status["result"]["rows"],
        serde_json::json!([["CANCELED"]])
    );
    let no_result = submit(
        &mut client,
        92_206,
        &GatewayRequest {
            version: GATEWAY_API_VERSION,
            request_id: "analytics-job-results-canceled".into(),
            operation: GatewayOperation::Cypher {
                text: format!(
                    "CALL dtg.analytics.results({{jobId: '{job_id}'}}) \
                     YIELD row RETURN row"
                ),
            },
        },
    )
    .await;
    assert_eq!(no_result["ok"], false, "{no_result}");
    assert!(
        no_result["error"]
            .as_str()
            .is_some_and(|message| message.contains("DTG-ANALYTICS-JOB-NO-RESULT")),
        "{no_result}"
    );
    for (request_id, request_name, page, expected) in [
        (
            92_203,
            "analytics-job-results-negative-offset",
            "offset: -1, limit: 1",
            "DTG-ANALYTICS-RESULT-PAGE",
        ),
        (
            92_204,
            "analytics-job-results-zero-limit",
            "offset: 0, limit: 0",
            "DTG-ANALYTICS-RESULT-LIMIT",
        ),
    ] {
        let rejected = submit(
            &mut client,
            request_id,
            &GatewayRequest {
                version: GATEWAY_API_VERSION,
                request_id: request_name.into(),
                operation: GatewayOperation::Cypher {
                    text: format!(
                        "CALL dtg.analytics.results({{jobId: '{job_id}', {page}}}) \
                         YIELD row RETURN row"
                    ),
                },
            },
        )
        .await;
        assert_eq!(rejected["ok"], false, "{rejected}");
        assert!(
            rejected["error"]
                .as_str()
                .is_some_and(|message| message.contains(expected)),
            "invalid result page must preserve {expected}: {rejected}"
        );
    }
    for (request_id, request_name, text) in [
        (
            92061,
            "reject-write-before-procedure",
            "USE social AT VALID_TIME AS OF 1000 CREATE (:MustNotCommitBefore) \
             CALL dtg.graph.degree({}) YIELD degree RETURN degree",
        ),
        (
            92062,
            "reject-write-after-procedure",
            "USE social AT VALID_TIME AS OF 1000 CALL dtg.graph.degree({}) YIELD degree \
             CREATE (:MustNotCommitAfter)",
        ),
        (
            92065,
            "reject-write-before-nested-procedure",
            "USE social AT VALID_TIME AS OF 1000 CREATE (:MustNotCommitNestedBefore) \
             CALL () { CALL dtg.graph.degree({}) YIELD degree RETURN degree } RETURN degree",
        ),
        (
            92066,
            "reject-write-after-nested-procedure",
            "USE social AT VALID_TIME AS OF 1000 \
             CALL () { CALL dtg.graph.degree({}) YIELD degree RETURN degree } \
             CREATE (:MustNotCommitNestedAfter)",
        ),
    ] {
        let rejected = submit(
            &mut client,
            request_id,
            &GatewayRequest {
                version: GATEWAY_API_VERSION,
                request_id: request_name.into(),
                operation: GatewayOperation::Cypher { text: text.into() },
            },
        )
        .await;
        assert_eq!(rejected["ok"], false, "{rejected}");
        assert!(
            rejected["error"]
                .as_str()
                .is_some_and(|error| error.contains("DTG-CYPHER-WRITE-PROCEDURE-UNSUPPORTED")),
            "mixed write/procedure statement returned the wrong error: {rejected}"
        );
    }
    for (request_id, label) in [
        (92063, "MustNotCommitBefore"),
        (92064, "MustNotCommitAfter"),
        (92067, "MustNotCommitNestedBefore"),
        (92068, "MustNotCommitNestedAfter"),
    ] {
        let verification = submit(
            &mut client,
            request_id,
            &GatewayRequest {
                version: GATEWAY_API_VERSION,
                request_id: format!("verify-{label}"),
                operation: GatewayOperation::Cypher {
                    text: format!(
                        "USE social AT VALID_TIME AS OF 1000 MATCH (n:{label}) RETURN count(n) AS total"
                    ),
                },
            },
        )
        .await;
        assert_eq!(verification["ok"], true, "{verification}");
        assert_eq!(verification["result"]["rows"], serde_json::json!([[0]]));
    }

    let bolt_service =
        Arc::new(CypherBoltService::new(Arc::new(bolt_gateway.clone()), 16).unwrap());

    let failed_write_subquery = submit(
        &mut client,
        2076,
        &GatewayRequest {
            version: GATEWAY_API_VERSION,
            request_id: "failed-write-subquery".into(),
            operation: GatewayOperation::Cypher {
                text: "USE social AT VALID_TIME AS OF 1000 \
                       UNWIND [1, {unsupported: 2}] AS value \
                       CALL (value) { CREATE (:AtomicSubquery {id: value}) }"
                    .into(),
            },
        },
    )
    .await;
    assert_eq!(
        failed_write_subquery["ok"], false,
        "{failed_write_subquery}"
    );
    let failed_write_visibility = submit(
        &mut client,
        30_077,
        &GatewayRequest {
            version: GATEWAY_API_VERSION,
            request_id: "failed-write-subquery-visibility".into(),
            operation: GatewayOperation::Cypher {
                text: "USE social AT VALID_TIME AS OF 1000 \
                       MATCH (n:AtomicSubquery) RETURN count(n) AS total"
                    .into(),
            },
        },
    )
    .await;
    assert_eq!(
        failed_write_visibility["result"]["rows"],
        serde_json::json!([[0]]),
        "a later child failure must not publish the first outer row: {failed_write_visibility}"
    );
    let successful_write_subquery = submit(
        &mut client,
        30_078,
        &GatewayRequest {
            version: GATEWAY_API_VERSION,
            request_id: "successful-write-subquery".into(),
            operation: GatewayOperation::Cypher {
                text: "USE social AT VALID_TIME AS OF 1000 \
                       UNWIND [1, 2] AS value \
                       CALL (value) { CREATE (:AtomicSubquery {id: value}) }"
                    .into(),
            },
        },
    )
    .await;
    assert_eq!(
        successful_write_subquery["ok"], true,
        "{successful_write_subquery}"
    );
    let successful_write_visibility = submit(
        &mut client,
        30_079,
        &GatewayRequest {
            version: GATEWAY_API_VERSION,
            request_id: "successful-write-subquery-visibility".into(),
            operation: GatewayOperation::Cypher {
                text: "USE social AT VALID_TIME AS OF 1000 \
                       MATCH (n:AtomicSubquery) RETURN count(n) AS total"
                    .into(),
            },
        },
    )
    .await;
    assert_eq!(
        successful_write_visibility["result"]["rows"],
        serde_json::json!([[2]]),
        "all outer rows must publish through one successful candidate: {successful_write_visibility}"
    );

    let child_match_seed = submit(
        &mut client,
        30_081,
        &GatewayRequest {
            version: GATEWAY_API_VERSION,
            request_id: "child-match-write-seed".into(),
            operation: GatewayOperation::Cypher {
                text: "USE social AT VALID_TIME AS OF 1000 \
                       CREATE (:ChildReadSource {id: 1}), (:ChildReadSource {id: 2})"
                    .into(),
            },
        },
    )
    .await;
    assert_eq!(child_match_seed["ok"], true, "{child_match_seed}");
    let child_match_write = submit(
        &mut client,
        30_082,
        &GatewayRequest {
            version: GATEWAY_API_VERSION,
            request_id: "child-match-feeds-set-and-create".into(),
            operation: GatewayOperation::Cypher {
                text: "USE social AT VALID_TIME AS OF 1000 \
                       CALL () { \
                         MATCH (source:ChildReadSource) \
                         CREATE (:ChildMatchCreated {id: 1}) \
                         SET source.state = 'seen' \
                       }"
                .into(),
            },
        },
    )
    .await;
    assert_eq!(child_match_write["ok"], true, "{child_match_write}");
    for (request_id, request_name, text) in [
        (
            30_083,
            "verify-child-match-create",
            "USE social AT VALID_TIME AS OF 1000 MATCH (n:ChildMatchCreated) RETURN count(n) AS total",
        ),
        (
            30_084,
            "verify-child-match-set",
            "USE social AT VALID_TIME AS OF 1000 MATCH (n:ChildReadSource) WHERE n.state = 'seen' RETURN count(n) AS total",
        ),
    ] {
        let verification = submit(
            &mut client,
            request_id,
            &GatewayRequest {
                version: GATEWAY_API_VERSION,
                request_id: request_name.into(),
                operation: GatewayOperation::Cypher { text: text.into() },
            },
        )
        .await;
        assert_eq!(verification["ok"], true, "{verification}");
        assert_eq!(verification["result"]["rows"], serde_json::json!([[2]]));
    }

    let child_unwind_write = submit(
        &mut client,
        30_085,
        &GatewayRequest {
            version: GATEWAY_API_VERSION,
            request_id: "child-unwind-multiple-rows-feed-create".into(),
            operation: GatewayOperation::Cypher {
                text: "USE social AT VALID_TIME AS OF 1000 \
                       CALL () { \
                         UNWIND [10, 20] AS value WITH value \
                         CREATE (:ChildUnwindCreated {id: value}) \
                       }"
                .into(),
            },
        },
    )
    .await;
    assert_eq!(child_unwind_write["ok"], true, "{child_unwind_write}");
    let child_unwind_visibility = submit(
        &mut client,
        30_086,
        &GatewayRequest {
            version: GATEWAY_API_VERSION,
            request_id: "verify-child-unwind-create".into(),
            operation: GatewayOperation::Cypher {
                text: "USE social AT VALID_TIME AS OF 1000 \
                       MATCH (n:ChildUnwindCreated) RETURN count(n) AS total"
                    .into(),
            },
        },
    )
    .await;
    assert_eq!(
        child_unwind_visibility["ok"], true,
        "{child_unwind_visibility}"
    );
    assert_eq!(
        child_unwind_visibility["result"]["rows"],
        serde_json::json!([[2]])
    );

    let failed_child_local_prefix = submit(
        &mut client,
        30_087,
        &GatewayRequest {
            version: GATEWAY_API_VERSION,
            request_id: "failed-child-local-prefix".into(),
            operation: GatewayOperation::Cypher {
                text: "USE social AT VALID_TIME AS OF 1000 \
                       CALL () { \
                         UNWIND [1, {unsupported: 2}] AS value \
                         CREATE (:ChildLocalAtomic {id: value}) \
                       }"
                .into(),
            },
        },
    )
    .await;
    assert_eq!(
        failed_child_local_prefix["ok"], false,
        "{failed_child_local_prefix}"
    );
    let failed_child_local_visibility = submit(
        &mut client,
        30_088,
        &GatewayRequest {
            version: GATEWAY_API_VERSION,
            request_id: "failed-child-local-prefix-visibility".into(),
            operation: GatewayOperation::Cypher {
                text: "USE social AT VALID_TIME AS OF 1000 \
                       MATCH (n:ChildLocalAtomic) RETURN count(n) AS total"
                    .into(),
            },
        },
    )
    .await;
    assert_eq!(
        failed_child_local_visibility["ok"], true,
        "{failed_child_local_visibility}"
    );
    assert_eq!(
        failed_child_local_visibility["result"]["rows"],
        serde_json::json!([[0]]),
        "a failing child-local row must leave the complete statement candidate unpublished"
    );
    let cleanup_write_subquery = submit(
        &mut client,
        30_080,
        &GatewayRequest {
            version: GATEWAY_API_VERSION,
            request_id: "cleanup-write-subquery".into(),
            operation: GatewayOperation::Cypher {
                text: "USE social AT VALID_TIME AS OF 1000 \
                       MATCH (n:AtomicSubquery) DETACH DELETE n"
                    .into(),
            },
        },
    )
    .await;
    assert_eq!(
        cleanup_write_subquery["ok"], true,
        "{cleanup_write_subquery}"
    );

    let mut bolt = BoltMachine::new(bolt_service);
    assert!(matches!(
        bolt.handle(ClientMessage::Hello(BTreeMap::new()))
            .await
            .as_slice(),
        [ServerMessage::Success(_)]
    ));

    let typed_call = bolt
        .handle(ClientMessage::Run {
            query: "USE social AT VALID_TIME AS OF 1000 CALL dtg.graph.degree({}) \
                    YIELD vertexId, degree AS score WITH vertexId, score RETURN vertexId, score"
                .into(),
            parameters: BTreeMap::new(),
            extra: BTreeMap::new(),
        })
        .await;
    assert!(
        matches!(typed_call.as_slice(), [ServerMessage::Success(_)]),
        "typed CALL/YIELD must enter the normal Bolt pipeline: {typed_call:?}"
    );
    let typed_rows = bolt
        .handle(ClientMessage::Pull {
            n: -1,
            query_id: None,
        })
        .await;
    assert!(typed_rows.iter().any(|message| matches!(
        message,
        ServerMessage::Record(values)
            if matches!(values.as_slice(), [bolt_protocol::Value::String(_), bolt_protocol::Value::Integer(_)])
    )), "CALL/YIELD must return typed Bolt fields, not JSON: {typed_rows:?}");

    let latest_algorithm_call = bolt
        .handle(ClientMessage::Run {
            query: "USE social AT VALID_TIME AS OF 1000 CALL dtg.graph.louvain({}) \
                    YIELD vertexId, communityId RETURN vertexId, communityId"
                .into(),
            parameters: BTreeMap::new(),
            extra: BTreeMap::new(),
        })
        .await;
    assert!(
        matches!(
            latest_algorithm_call.as_slice(),
            [ServerMessage::Success(_)]
        ),
        "latest catalog algorithms must enter the normal Bolt pipeline: {latest_algorithm_call:?}"
    );
    let latest_algorithm_rows = bolt
        .handle(ClientMessage::Pull {
            n: -1,
            query_id: None,
        })
        .await;
    assert!(latest_algorithm_rows.iter().any(|message| matches!(
        message,
        ServerMessage::Record(values)
            if matches!(values.as_slice(), [bolt_protocol::Value::String(_), bolt_protocol::Value::String(_)])
    )), "Louvain must return typed vertex/community identifiers: {latest_algorithm_rows:?}");

    let parameterized_submit = bolt
        .handle(ClientMessage::Run {
            query: "USE social AT VALID_TIME AS OF 1000 \
                    CALL dtg.analytics.submit({algorithm: $algorithm, parameters: $parameters}) \
                    YIELD jobId RETURN jobId"
                .into(),
            parameters: BTreeMap::from([
                (
                    "algorithm".into(),
                    bolt_protocol::Value::String("dtg.graph.louvain".into()),
                ),
                (
                    "parameters".into(),
                    bolt_protocol::Value::Map(BTreeMap::new()),
                ),
            ]),
            extra: BTreeMap::new(),
        })
        .await;
    assert!(
        matches!(parameterized_submit.as_slice(), [ServerMessage::Success(_)]),
        "parameterized analytics submit must resolve projection and typed parameters: {parameterized_submit:?}"
    );
    let parameterized_job = bolt
        .handle(ClientMessage::Pull {
            n: -1,
            query_id: None,
        })
        .await;
    assert!(
        parameterized_job.iter().any(|message| matches!(
            message,
            ServerMessage::Record(values)
                if matches!(values.as_slice(), [bolt_protocol::Value::String(_)])
        )),
        "parameterized analytics submit must return a typed job handle: {parameterized_job:?}"
    );
    let invalid_algorithm_type = bolt
        .handle(ClientMessage::Run {
            query: "USE social AT VALID_TIME AS OF 1000 \
                    CALL dtg.analytics.submit({algorithm: $algorithm, parameters: $parameters}) \
                    YIELD jobId RETURN jobId"
                .into(),
            parameters: BTreeMap::from([
                ("algorithm".into(), bolt_protocol::Value::Integer(7)),
                (
                    "parameters".into(),
                    bolt_protocol::Value::Map(BTreeMap::new()),
                ),
            ]),
            extra: BTreeMap::new(),
        })
        .await;
    assert!(
        matches!(
            invalid_algorithm_type.as_slice(),
            [ServerMessage::Failure { message, .. }]
                if message.contains("DTG-PROCEDURE-ARGUMENT-TYPE")
        ),
        "non-string parameterized algorithm names must fail with a stable type error: {invalid_algorithm_type:?}"
    );
    assert!(matches!(
        bolt.handle(ClientMessage::Reset).await.as_slice(),
        [ServerMessage::Success(_)]
    ));
    let unknown_parameterized_algorithm = bolt
        .handle(ClientMessage::Run {
            query: "USE social AT VALID_TIME AS OF 1000 \
                    CALL dtg.analytics.submit({algorithm: $algorithm, parameters: $parameters}) \
                    YIELD jobId RETURN jobId"
                .into(),
            parameters: BTreeMap::from([
                (
                    "algorithm".into(),
                    bolt_protocol::Value::String("dtg.unknown.algorithm".into()),
                ),
                (
                    "parameters".into(),
                    bolt_protocol::Value::Map(BTreeMap::new()),
                ),
            ]),
            extra: BTreeMap::new(),
        })
        .await;
    assert!(
        matches!(
            unknown_parameterized_algorithm.as_slice(),
            [ServerMessage::Failure { message, .. }]
                if message.contains("unknown algorithm")
        ),
        "unknown parameterized algorithms must fail without a fallback provider: {unknown_parameterized_algorithm:?}"
    );
    assert!(matches!(
        bolt.handle(ClientMessage::Reset).await.as_slice(),
        [ServerMessage::Success(_)]
    ));

    let begin_analytics = submit(
        &mut client,
        92_080,
        &GatewayRequest {
            version: GATEWAY_API_VERSION,
            request_id: "analytics-degree-before-explicit-transaction".into(),
            operation: GatewayOperation::Cypher {
                text: "USE social AT VALID_TIME AS OF 1000 CALL dtg.graph.degree({}) \
                       YIELD vertexId RETURN count(vertexId)"
                    .into(),
            },
        },
    )
    .await;
    assert_eq!(begin_analytics["ok"], true, "{begin_analytics}");
    let begin_vertex_count = begin_analytics["result"]["rows"][0][0]
        .as_i64()
        .expect("BEGIN baseline vertex count");

    assert!(matches!(
        bolt.handle(ClientMessage::Begin(BTreeMap::new()))
            .await
            .as_slice(),
        [ServerMessage::Success(_)]
    ));
    let illegal_batch = bolt
        .handle(ClientMessage::Run {
            query: "USE social AT VALID_TIME AS OF 1000 \
                    UNWIND [1, 2] AS value \
                    CALL (value) { CREATE (:IllegalBatch {id: value}) } \
                    IN TRANSACTIONS OF 1 ROWS"
                .into(),
            parameters: BTreeMap::new(),
            extra: BTreeMap::new(),
        })
        .await;
    assert!(
        matches!(
            illegal_batch.as_slice(),
            [ServerMessage::Failure { code, message }]
                if code == "Neo.ClientError.Transaction.InvalidType"
                    && message.contains("DTG-CYPHER-IN-TRANSACTIONS-EXPLICIT")
        ),
        "IN TRANSACTIONS must fail before joining the explicit transaction: {illegal_batch:?}"
    );
    assert!(matches!(
        bolt.handle(ClientMessage::Reset).await.as_slice(),
        [ServerMessage::Success(_)]
    ));
    assert!(matches!(
        bolt.handle(ClientMessage::Begin(BTreeMap::new()))
            .await
            .as_slice(),
        [ServerMessage::Success(_)]
    ));
    let staged_write_subquery = bolt
        .handle(ClientMessage::Run {
            query: "USE social AT VALID_TIME AS OF 1000 \
                    UNWIND [1, 2] AS value \
                    CALL (value) { CREATE (n:TransactionSubquery {id: value}) RETURN n } \
                    RETURN n"
                .into(),
            parameters: BTreeMap::new(),
            extra: BTreeMap::new(),
        })
        .await;
    assert!(
        matches!(
            staged_write_subquery.as_slice(),
            [ServerMessage::Success(_)]
        ),
        "write subquery must stage every outer row in the explicit transaction: {staged_write_subquery:?}"
    );
    let _ = bolt
        .handle(ClientMessage::Discard {
            n: -1,
            query_id: None,
        })
        .await;
    assert!(matches!(
        bolt.handle(ClientMessage::Run {
            query: "USE social AT VALID_TIME AS OF 1000 \
                    CALL () { \
                      MATCH (n:TransactionSubquery) \
                      SET n.state = 'seen-through-child-prefix' \
                    }"
            .into(),
            parameters: BTreeMap::new(),
            extra: BTreeMap::new(),
        })
        .await
        .as_slice(),
        [ServerMessage::Success(_)]
    ));
    let _ = bolt
        .handle(ClientMessage::Discard {
            n: -1,
            query_id: None,
        })
        .await;
    assert!(matches!(
        bolt.handle(ClientMessage::Run {
            query: "USE social AT VALID_TIME AS OF 1000 \
                    MATCH (n:TransactionSubquery) \
                    WHERE n.state = 'seen-through-child-prefix' \
                    RETURN count(n)"
                .into(),
            parameters: BTreeMap::new(),
            extra: BTreeMap::new(),
        })
        .await
        .as_slice(),
        [ServerMessage::Success(_)]
    ));
    let updated_subquery_rows = bolt
        .handle(ClientMessage::Pull {
            n: -1,
            query_id: None,
        })
        .await;
    assert!(
        updated_subquery_rows.iter().any(|message| matches!(
            message,
            ServerMessage::Record(values)
                if values == &vec![bolt_protocol::Value::Integer(2)]
        )),
        "child-local MATCH must inherit the explicit transaction overlay: {updated_subquery_rows:?}"
    );
    assert!(matches!(
        bolt.handle(ClientMessage::Run {
            query: "USE social AT VALID_TIME AS OF 1000 \
                    CALL () { MATCH (n:TransactionSubquery) RETURN n } \
                    RETURN count(n)"
                .into(),
            parameters: BTreeMap::new(),
            extra: BTreeMap::new(),
        })
        .await
        .as_slice(),
        [ServerMessage::Success(_)]
    ));
    let staged_subquery_rows = bolt
        .handle(ClientMessage::Pull {
            n: -1,
            query_id: None,
        })
        .await;
    assert!(
        staged_subquery_rows.iter().any(|message| matches!(
            message,
            ServerMessage::Record(values)
                if values == &vec![bolt_protocol::Value::Integer(2)]
        )),
        "read subquery must see both writes in the existing transaction overlay: {staged_subquery_rows:?}"
    );
    let late_commit = submit(
        &mut client,
        2078,
        &GatewayRequest {
            version: GATEWAY_API_VERSION,
            request_id: "late-snapshot-create".into(),
            operation: GatewayOperation::Cypher {
                text: "USE social AT VALID_TIME AS OF 1000 CREATE (late_snapshot_seed:LateSnapshot {name: 'late'})".into(),
            },
        },
    )
    .await;
    assert_eq!(late_commit["ok"], true, "{late_commit}");
    assert!(matches!(
        bolt.handle(ClientMessage::Run {
            query: "USE social AT VALID_TIME AS OF 1000 \
                    CALL () { \
                      MATCH (:LateSnapshot) \
                      CREATE (:LateSnapshotChildLeak) \
                    }"
            .into(),
            parameters: BTreeMap::new(),
            extra: BTreeMap::new(),
        })
        .await
        .as_slice(),
        [ServerMessage::Success(_)]
    ));
    let _ = bolt
        .handle(ClientMessage::Discard {
            n: -1,
            query_id: None,
        })
        .await;
    let procedure_overlay_write = bolt
        .handle(ClientMessage::Run {
            query: "USE social AT VALID_TIME AS OF 1000 CREATE (:ProcedureOverlay)".into(),
            parameters: BTreeMap::new(),
            extra: BTreeMap::new(),
        })
        .await;
    assert!(
        matches!(
            procedure_overlay_write.as_slice(),
            [ServerMessage::Success(_)]
        ),
        "explicit transaction overlay write failed: {procedure_overlay_write:?}"
    );
    let _ = bolt
        .handle(ClientMessage::Discard {
            n: -1,
            query_id: None,
        })
        .await;
    assert!(matches!(
        bolt.handle(ClientMessage::Run {
            query: "USE social AT VALID_TIME AS OF 1000 CALL dtg.graph.degree({}) \
                    YIELD vertexId RETURN count(vertexId)"
                .into(),
            parameters: BTreeMap::new(),
            extra: BTreeMap::new(),
        })
        .await
        .as_slice(),
        [ServerMessage::Success(_)]
    ));
    let overlay_degree = bolt
        .handle(ClientMessage::Pull {
            n: -1,
            query_id: None,
        })
        .await;
    assert!(
        overlay_degree.iter().any(|message| matches!(
            message,
            ServerMessage::Record(values)
                if values == &vec![bolt_protocol::Value::Integer(
                    begin_vertex_count + 3
                )]
        )),
        "procedure must see BEGIN snapshot plus staged overlay and exclude late commit: {overlay_degree:?}"
    );
    assert!(matches!(
        bolt.handle(ClientMessage::Run {
            query: "USE social AT VALID_TIME AS OF 1000 \
                    CALL () { MATCH (n:LateSnapshot) RETURN n } RETURN n"
                .into(),
            parameters: BTreeMap::new(),
            extra: BTreeMap::new(),
        })
        .await
        .as_slice(),
        [ServerMessage::Success(_)]
    ));
    let fixed_snapshot_rows = bolt
        .handle(ClientMessage::Pull {
            n: -1,
            query_id: None,
        })
        .await;
    assert!(
        fixed_snapshot_rows
            .iter()
            .all(|message| !matches!(message, ServerMessage::Record(_))),
        "an external commit after BEGIN leaked into the fixed transaction snapshot: {fixed_snapshot_rows:?}"
    );
    assert!(matches!(
        bolt.handle(ClientMessage::Rollback).await.as_slice(),
        [ServerMessage::Success(_)]
    ));
    let rolled_back_subquery = submit(
        &mut client,
        2079,
        &GatewayRequest {
            version: GATEWAY_API_VERSION,
            request_id: "rolled-back-write-subquery".into(),
            operation: GatewayOperation::Cypher {
                text: "USE social AT VALID_TIME AS OF 1000 \
                       MATCH (n:TransactionSubquery) RETURN count(n) AS total"
                    .into(),
            },
        },
    )
    .await;
    assert_eq!(rolled_back_subquery["ok"], true, "{rolled_back_subquery}");
    assert_eq!(
        rolled_back_subquery["result"]["rows"],
        serde_json::json!([[0]]),
        "ROLLBACK must discard ordinary write-subquery mutations"
    );
    let fixed_child_prefix_visibility = submit(
        &mut client,
        30_089,
        &GatewayRequest {
            version: GATEWAY_API_VERSION,
            request_id: "fixed-child-prefix-visibility".into(),
            operation: GatewayOperation::Cypher {
                text: "USE social AT VALID_TIME AS OF 1000 \
                       MATCH (n:LateSnapshotChildLeak) RETURN count(n) AS total"
                    .into(),
            },
        },
    )
    .await;
    assert_eq!(
        fixed_child_prefix_visibility["ok"], true,
        "{fixed_child_prefix_visibility}"
    );
    assert_eq!(
        fixed_child_prefix_visibility["result"]["rows"],
        serde_json::json!([[0]]),
        "child-local MATCH must not observe commits newer than BEGIN"
    );

    let committed_overlay_target = submit(
        &mut client,
        2077,
        &GatewayRequest {
            version: GATEWAY_API_VERSION,
            request_id: "committed-overlay-target".into(),
            operation: GatewayOperation::Cypher {
                text: "USE social AT VALID_TIME AS OF 1000 CREATE (overlay_committed_seed:OverlayCommitted {state: 'old'})".into(),
            },
        },
    )
    .await;
    assert_eq!(
        committed_overlay_target["ok"], true,
        "{committed_overlay_target}"
    );
    assert!(matches!(
        bolt.handle(ClientMessage::Begin(BTreeMap::new()))
            .await
            .as_slice(),
        [ServerMessage::Success(_)]
    ));
    assert!(matches!(
        bolt.handle(ClientMessage::Run {
            query: "USE social AT VALID_TIME AS OF 1000 MATCH (n:OverlayCommitted) WHERE n.state = 'old' SET n.state = 'new'".into(),
            parameters: BTreeMap::new(),
            extra: BTreeMap::new(),
        })
        .await
        .as_slice(),
        [ServerMessage::Success(_)]
    ));
    let _ = bolt
        .handle(ClientMessage::Discard {
            n: -1,
            query_id: None,
        })
        .await;
    let updated_count_run = bolt
        .handle(ClientMessage::Run {
            query: "USE social AT VALID_TIME AS OF 1000 MATCH (n:OverlayCommitted) WHERE n.state = 'new' RETURN count(n)".into(),
            parameters: BTreeMap::new(),
            extra: BTreeMap::new(),
        })
        .await;
    assert!(
        matches!(updated_count_run.as_slice(), [ServerMessage::Success(_)]),
        "staged committed-node update must enter the scan before Filter/Aggregate: {updated_count_run:?}"
    );
    assert!(
        bolt.handle(ClientMessage::Pull {
            n: -1,
            query_id: None,
        })
        .await
        .iter()
        .any(|message| matches!(message, ServerMessage::Record(values) if values == &vec![bolt_protocol::Value::Integer(1)])),
        "updated committed node must appear exactly once in count"
    );
    assert!(matches!(
        bolt.handle(ClientMessage::Run {
            query: "USE social AT VALID_TIME AS OF 1000 MATCH (n:OverlayCommitted) WHERE n.state = 'new' DELETE n".into(),
            parameters: BTreeMap::new(),
            extra: BTreeMap::new(),
        })
        .await
        .as_slice(),
        [ServerMessage::Success(_)]
    ));
    let _ = bolt
        .handle(ClientMessage::Discard {
            n: -1,
            query_id: None,
        })
        .await;
    assert!(matches!(
        bolt.handle(ClientMessage::Run {
            query: "USE social AT VALID_TIME AS OF 1000 MATCH (n:OverlayCommitted) RETURN count(n)"
                .into(),
            parameters: BTreeMap::new(),
            extra: BTreeMap::new(),
        })
        .await
        .as_slice(),
        [ServerMessage::Success(_)]
    ));
    assert!(
        bolt.handle(ClientMessage::Pull {
            n: -1,
            query_id: None,
        })
        .await
        .iter()
        .any(|message| matches!(message, ServerMessage::Record(values) if values == &vec![bolt_protocol::Value::Integer(0)])),
        "deleted committed node must be suppressed before count"
    );
    assert!(matches!(
        bolt.handle(ClientMessage::Rollback).await.as_slice(),
        [ServerMessage::Success(_)]
    ));

    assert!(matches!(
        bolt.handle(ClientMessage::Begin(BTreeMap::new()))
            .await
            .as_slice(),
        [ServerMessage::Success(_)]
    ));
    let remote_left = &remote_endpoint_tokens[&10];
    let remote_right = &remote_endpoint_tokens[&20];
    let staged_remote_edge = bolt
        .handle(ClientMessage::Run {
            query: format!(
                "USE social AT VALID_TIME AS OF 1000 MERGE (a:RemoteEndpoint {{token: '{remote_left}'}}) MERGE (b:RemoteEndpoint {{token: '{remote_right}'}}) MERGE (a)-[explicit_edge:REMOTE_EXPLICIT]->(b)"
            ),
            parameters: BTreeMap::new(),
            extra: BTreeMap::new(),
        })
        .await;
    assert!(
        matches!(staged_remote_edge.as_slice(), [ServerMessage::Success(_)]),
        "explicit transaction must stage an edge between committed remote endpoints: {staged_remote_edge:?}"
    );
    let _ = bolt
        .handle(ClientMessage::Discard {
            n: -1,
            query_id: None,
        })
        .await;
    for valid_time in [1000, 2000] {
        for pattern in [
            "(a:RemoteEndpoint)-[r:REMOTE_EXPLICIT]->(b:RemoteEndpoint)",
            "(b:RemoteEndpoint)<-[r:REMOTE_EXPLICIT]-(a:RemoteEndpoint)",
        ] {
            let run = bolt
                .handle(ClientMessage::Run {
                    query: format!(
                        "USE social AT VALID_TIME AS OF {valid_time} MATCH {pattern} RETURN count(r)"
                    ),
                    parameters: BTreeMap::new(),
                    extra: BTreeMap::new(),
                })
                .await;
            assert!(
                matches!(run.as_slice(), [ServerMessage::Success(_)]),
                "staged edge with committed cross-partition endpoints must expand: {run:?}"
            );
            let rows = bolt
                .handle(ClientMessage::Pull {
                    n: -1,
                    query_id: None,
                })
                .await;
            assert!(
                rows.iter().any(|message| matches!(message, ServerMessage::Record(values) if values == &vec![bolt_protocol::Value::Integer(1)])),
                "both directions must expand the staged cross-partition edge exactly once at valid time {valid_time}: {rows:?}"
            );
        }
    }
    let multi_row_commit = bolt.handle(ClientMessage::Commit).await;
    assert!(
        matches!(multi_row_commit.as_slice(), [ServerMessage::Success(_)]),
        "explicit remote-endpoint MERGE commit failed: {multi_row_commit:?}"
    );
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
    let read_your_writes = bolt
        .handle(ClientMessage::Run {
            query: "MATCH (n) RETURN n".into(),
            parameters: BTreeMap::new(),
            extra: BTreeMap::new(),
        })
        .await;
    assert!(matches!(
        read_your_writes.as_slice(),
        [ServerMessage::Success(_)]
    ));
    let pulled_overlay = bolt
        .handle(ClientMessage::Pull {
            n: -1,
            query_id: None,
        })
        .await;
    assert!(
        pulled_overlay
            .iter()
            .any(|message| matches!(message, ServerMessage::Record(_)))
    );
    assert!(matches!(
        bolt.handle(ClientMessage::Rollback).await.as_slice(),
        [ServerMessage::Success(_)]
    ));
    let committed = bolt.handle(ClientMessage::Begin(BTreeMap::new())).await;
    assert!(matches!(committed.as_slice(), [ServerMessage::Success(_)]));
    let staged = bolt
        .handle(ClientMessage::Run {
            query:
                "USE social AT VALID_TIME AS OF 1000 CREATE (committed:Person {name: 'Committed'})"
                    .into(),
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
    let staged_update = bolt
        .handle(ClientMessage::Run {
            query: "USE social AT VALID_TIME AS OF 1000 MATCH (committed:Person) WHERE committed.name = 'Committed' SET committed.status = 'active'".into(),
            parameters: BTreeMap::new(),
            extra: BTreeMap::new(),
        })
        .await;
    assert!(
        matches!(staged_update.as_slice(), [ServerMessage::Success(_)]),
        "a later statement must resolve bindings from the explicit transaction overlay: {staged_update:?}"
    );
    let _ = bolt
        .handle(ClientMessage::Discard {
            n: -1,
            query_id: None,
        })
        .await;
    let staged_later_update = bolt
        .handle(ClientMessage::Run {
            query: "USE social AT VALID_TIME AS OF 2000 MATCH (committed:Person) WHERE committed.name = 'Committed' SET committed.status = 'later'".into(),
            parameters: BTreeMap::new(),
            extra: BTreeMap::new(),
        })
        .await;
    assert!(
        matches!(staged_later_update.as_slice(), [ServerMessage::Success(_)]),
        "a later valid-time correction must remain in the explicit overlay: {staged_later_update:?}"
    );
    let _ = bolt
        .handle(ClientMessage::Discard {
            n: -1,
            query_id: None,
        })
        .await;
    let staged_second = bolt
        .handle(ClientMessage::Run {
            query: "CREATE (committed_second:Person {name: 'CommittedSecond'})".into(),
            parameters: BTreeMap::new(),
            extra: BTreeMap::new(),
        })
        .await;
    assert!(matches!(
        staged_second.as_slice(),
        [ServerMessage::Success(_)]
    ));
    let _ = bolt
        .handle(ClientMessage::Discard {
            n: -1,
            query_id: None,
        })
        .await;
    let commit = bolt.handle(ClientMessage::Commit).await;
    assert!(
        matches!(commit.as_slice(), [ServerMessage::Success(_)]),
        "explicit overlay commit failed: {commit:?}"
    );

    assert!(matches!(
        bolt.handle(ClientMessage::Begin(BTreeMap::new()))
            .await
            .as_slice(),
        [ServerMessage::Success(_)]
    ));
    assert!(matches!(
        bolt.handle(ClientMessage::Run {
            query: "USE social AT VALID_TIME AS OF 1000 MATCH (n:Person) RETURN n".into(),
            parameters: BTreeMap::new(),
            extra: BTreeMap::new(),
        })
        .await
        .as_slice(),
        [ServerMessage::Success(_)]
    ));
    let _ = bolt
        .handle(ClientMessage::Pull {
            n: -1,
            query_id: None,
        })
        .await;
    let read_only_commit = bolt.handle(ClientMessage::Commit).await;
    assert!(
        matches!(read_only_commit.as_slice(), [ServerMessage::Success(metadata)] if metadata.contains_key("bookmark")),
        "read-only explicit transaction commit failed: {read_only_commit:?}"
    );

    assert!(matches!(
        bolt.handle(ClientMessage::Begin(BTreeMap::new()))
            .await
            .as_slice(),
        [ServerMessage::Success(_)]
    ));
    assert!(matches!(
        bolt.handle(ClientMessage::Run {
            query: "CREATE (projection_overlay:Person {name: 'Overlay'})".into(),
            parameters: BTreeMap::new(),
            extra: BTreeMap::new(),
        })
        .await
        .as_slice(),
        [ServerMessage::Success(_)]
    ));
    let _ = bolt
        .handle(ClientMessage::Pull {
            n: -1,
            query_id: None,
        })
        .await;
    assert!(matches!(
        bolt.handle(ClientMessage::Run {
            query: "MATCH (n:Person) RETURN n, n.name".into(),
            parameters: BTreeMap::new(),
            extra: BTreeMap::new(),
        })
        .await
        .as_slice(),
        [ServerMessage::Success(_)]
    ));
    let projected_overlay = bolt
        .handle(ClientMessage::Pull {
            n: -1,
            query_id: None,
        })
        .await;
    assert!(
        projected_overlay.iter().any(|message| matches!(
            message,
            ServerMessage::Record(values)
                if matches!(values.as_slice(), [bolt_protocol::Value::Map(_), bolt_protocol::Value::String(value)] if value == "Overlay")
        )),
        "a multi-column projection must read the staged transaction overlay: {projected_overlay:?}"
    );
    assert!(matches!(
        bolt.handle(ClientMessage::Rollback).await.as_slice(),
        [ServerMessage::Success(_)]
    ));

    assert!(matches!(
        bolt.handle(ClientMessage::Begin(BTreeMap::new()))
            .await
            .as_slice(),
        [ServerMessage::Success(_)]
    ));
    assert!(matches!(
        bolt.handle(ClientMessage::Run {
            query: "CREATE (aggregate_overlay:OverlayCount)".into(),
            parameters: BTreeMap::new(),
            extra: BTreeMap::new(),
        })
        .await
        .as_slice(),
        [ServerMessage::Success(_)]
    ));
    let _ = bolt
        .handle(ClientMessage::Discard {
            n: -1,
            query_id: None,
        })
        .await;
    assert!(matches!(
        bolt.handle(ClientMessage::Run {
            query: "MATCH (n:OverlayCount) RETURN count(n)".into(),
            parameters: BTreeMap::new(),
            extra: BTreeMap::new(),
        })
        .await
        .as_slice(),
        [ServerMessage::Success(_)]
    ));
    assert!(
        bolt.handle(ClientMessage::Pull {
            n: -1,
            query_id: None,
        })
        .await
        .iter()
        .any(|message| matches!(message, ServerMessage::Record(values) if values == &vec![bolt_protocol::Value::Integer(1)])),
        "aggregate must include the staged transaction overlay"
    );
    assert!(matches!(
        bolt.handle(ClientMessage::Rollback).await.as_slice(),
        [ServerMessage::Success(_)]
    ));

    assert!(matches!(
        bolt.handle(ClientMessage::Begin(BTreeMap::new()))
            .await
            .as_slice(),
        [ServerMessage::Success(_)]
    ));
    assert!(matches!(
        bolt.handle(ClientMessage::Run {
            query: "USE social AT VALID_TIME AS OF 1000 CREATE (n:AliasOriginal {name: 'local'})"
                .into(),
            parameters: BTreeMap::new(),
            extra: BTreeMap::new(),
        })
        .await
        .as_slice(),
        [ServerMessage::Success(_)]
    ));
    let _ = bolt
        .handle(ClientMessage::Discard {
            n: -1,
            query_id: None,
        })
        .await;
    assert!(matches!(
        bolt.handle(ClientMessage::Run {
            query: "USE social AT VALID_TIME AS OF 1000 MATCH (n:AliasOriginal) WITH n AS rebound SET rebound.with_value = 'yes'".into(),
            parameters: BTreeMap::new(),
            extra: BTreeMap::new(),
        })
        .await
        .as_slice(),
        [ServerMessage::Success(_)]
    ));
    let _ = bolt
        .handle(ClientMessage::Discard {
            n: -1,
            query_id: None,
        })
        .await;
    assert!(matches!(
        bolt.handle(ClientMessage::Run {
            query: "USE social AT VALID_TIME AS OF 1000 MATCH (n:AliasDifferent) WHERE n.name = 'missing' SET n.leaked = 'yes'".into(),
            parameters: BTreeMap::new(),
            extra: BTreeMap::new(),
        })
        .await
        .as_slice(),
        [ServerMessage::Success(_)]
    ));
    let _ = bolt
        .handle(ClientMessage::Discard {
            n: -1,
            query_id: None,
        })
        .await;
    assert!(matches!(
        bolt.handle(ClientMessage::Run {
            query: "USE social AT VALID_TIME AS OF 1000 MATCH (n:AliasOriginal) WHERE n.leaked = 'yes' RETURN count(n)".into(),
            parameters: BTreeMap::new(),
            extra: BTreeMap::new(),
        })
        .await
        .as_slice(),
        [ServerMessage::Success(_)]
    ));
    assert!(
        bolt.handle(ClientMessage::Pull {
            n: -1,
            query_id: None,
        })
        .await
        .iter()
        .any(|message| matches!(message, ServerMessage::Record(values) if values == &vec![bolt_protocol::Value::Integer(0)])),
        "a variable name reused in a later statement must not bypass its MATCH predicate"
    );
    assert!(matches!(
        bolt.handle(ClientMessage::Rollback).await.as_slice(),
        [ServerMessage::Success(_)]
    ));

    assert!(matches!(
        bolt.handle(ClientMessage::Begin(BTreeMap::new()))
            .await
            .as_slice(),
        [ServerMessage::Success(_)]
    ));
    let unwind_write = bolt
        .handle(ClientMessage::Run {
            query: "USE social AT VALID_TIME AS OF 1000 \
                    UNWIND [[1, 2], [3]] AS group_values \
                    WITH group_values AS items UNWIND items AS item \
                    CREATE (unwind_created:UnwindCreated)"
                .into(),
            parameters: BTreeMap::new(),
            extra: BTreeMap::new(),
        })
        .await;
    assert!(
        matches!(unwind_write.as_slice(), [ServerMessage::Success(_)]),
        "UNWIND write prefix must preserve every upstream row: {unwind_write:?}"
    );
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
    let unwind_committed = submit(
        &mut client,
        2081,
        &GatewayRequest {
            version: GATEWAY_API_VERSION,
            request_id: "query-unwind-created".into(),
            operation: GatewayOperation::Cypher {
                text: "USE social AT VALID_TIME AS OF 1000 MATCH (n:UnwindCreated) RETURN n".into(),
            },
        },
    )
    .await;
    assert_eq!(unwind_committed["ok"], true, "{unwind_committed}");
    assert_eq!(unwind_committed["result"]["row_count"], 3);

    let atomic_transaction = BoltQueryBackend::begin(&bolt_gateway, BTreeMap::new())
        .await
        .expect("direct transaction begin");
    for query in [
        "USE social AT VALID_TIME AS OF 1000 CREATE (atomic_first:AtomicCommitted {name: 'first'})",
        "USE social AT VALID_TIME AS OF 1000 CREATE (atomic_second:AtomicCommitted {name: 'second'})",
    ] {
        BoltQueryBackend::execute(
            &bolt_gateway,
            BoltQueryRequest::new(
                query.into(),
                BTreeMap::new(),
                BTreeMap::new(),
                Some(atomic_transaction),
            ),
        )
        .await
        .expect("successful statement must stage");
    }
    let failed_multi_row = BoltQueryBackend::execute(
        &bolt_gateway,
        BoltQueryRequest::new(
            "USE social AT VALID_TIME AS OF 1000 \
             UNWIND [[1], [2]] AS group_values WITH group_values AS items \
             UNWIND items AS item CREATE (bad:AtomicFailed {value: item})"
                .into(),
            BTreeMap::new(),
            BTreeMap::new(),
            Some(atomic_transaction),
        ),
    )
    .await;
    assert!(
        failed_multi_row.is_err(),
        "identifier-backed write properties should fail materialization for the whole multi-row statement"
    );
    let visible_prior_overlay = BoltQueryBackend::execute(
        &bolt_gateway,
        BoltQueryRequest::new(
            "USE social AT VALID_TIME AS OF 1000 MATCH (n:AtomicCommitted) RETURN count(n)".into(),
            BTreeMap::new(),
            BTreeMap::new(),
            Some(atomic_transaction),
        ),
    )
    .await
    .expect("prior overlay must remain readable after statement failure");
    assert_eq!(
        visible_prior_overlay.records(),
        &[vec![RuntimeValue::Integer(2)]]
    );
    let atomic_bookmark = BoltQueryBackend::commit(&bolt_gateway, atomic_transaction)
        .await
        .expect("prior successful statements must still commit");
    let atomic_committed = submit(
        &mut client,
        2082,
        &GatewayRequest {
            version: GATEWAY_API_VERSION,
            request_id: "query-atomic-committed".into(),
            operation: GatewayOperation::Cypher {
                text: "USE social AT VALID_TIME AS OF 1000 MATCH (n:AtomicCommitted) RETURN n"
                    .into(),
            },
        },
    )
    .await;
    assert_eq!(atomic_committed["ok"], true, "{atomic_committed}");
    assert_eq!(atomic_committed["result"]["row_count"], 2);
    let atomic_failed = submit(
        &mut client,
        2083,
        &GatewayRequest {
            version: GATEWAY_API_VERSION,
            request_id: "query-atomic-failed".into(),
            operation: GatewayOperation::Cypher {
                text: "USE social AT VALID_TIME AS OF 1000 MATCH (n:AtomicFailed) RETURN n".into(),
            },
        },
    )
    .await;
    assert_eq!(atomic_failed["ok"], true, "{atomic_failed}");
    assert_eq!(atomic_failed["result"]["row_count"], 0);
    let atomic_history = submit(
        &mut client,
        2084,
        &GatewayRequest {
            version: GATEWAY_API_VERSION,
            request_id: "query-atomic-history".into(),
            operation: GatewayOperation::Cypher {
                text:
                    "USE social AT VALID_TIME FROM 1000 TO 2000 MATCH (n:AtomicCommitted) RETURN n"
                        .into(),
            },
        },
    )
    .await;
    assert_eq!(atomic_history["ok"], true, "{atomic_history}");
    assert!(atomic_bookmark.starts_with("dtg:tx:"), "{atomic_bookmark}");
    let regions = atomic_history["result"]["temporal_regions"]
        .as_array()
        .expect("temporal regions");
    assert_eq!(regions.len(), 2, "{atomic_history}");
    let persisted_transaction = &regions[0]["transaction"];
    assert!(
        regions
            .iter()
            .all(|region| &region["transaction"] == persisted_transaction),
        "both persisted statement writes must share one start/commit interval: {atomic_history}"
    );
    assert!(
        persisted_transaction["from"].is_object(),
        "{atomic_history}"
    );
    assert!(persisted_transaction["to"].is_object(), "{atomic_history}");

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
