use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cluster_protocol::CLUSTER_PROTOCOL_VERSION;
use cluster_protocol::proto::meta_service_server::{MetaService, MetaServiceServer};
use cluster_protocol::proto::{ProposeRequest, RequestContext};
use control_plane::{
    BackendProfile, CatalogCommand, DeploymentMode, GraphDefinition, Placement, TopologyDefinition,
};
use gateway_node::{GatewayCatalogRouter, RemoteGatewayService};
use meta_node::{MetaNodeService, MetaRaftReplica, ReplicatedTso};
use shard_client::RemoteShardClient;
use storage_api::AdapterRequirement;
use timestamp_oracle::ManualClock;
use tokio::sync::{Mutex, watch};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::Request;
use tonic::transport::Server;

const CLUSTER_ID: [u8; 16] = [0x72; 16];

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

fn elected_service(root: &std::path::Path) -> MetaNodeService {
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
        Arc::new(ReplicatedTso::new(Arc::new(ManualClock::new(1_000_000)), 8, 1_000_000).unwrap()),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn loads_authoritative_catalog_and_installs_incremental_watch_updates() {
    let temporary = tempfile::tempdir().unwrap();
    let meta = elected_service(temporary.path());
    let create = CatalogCommand::create_graph(101, 0, graph())
        .encode()
        .unwrap();
    meta.propose(Request::new(ProposeRequest {
        context: Some(context(101)),
        command: create,
    }))
    .await
    .unwrap();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let meta_address = listener.local_addr().unwrap();
    let (server_shutdown, server_shutdown_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(
        Server::builder()
            .add_service(MetaServiceServer::new(meta.clone()))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = server_shutdown_rx.await;
            }),
    );
    let data_nodes = BTreeMap::from([
        (10, "127.0.0.1:7110".parse::<SocketAddr>().unwrap()),
        (20, "127.0.0.1:7120".parse::<SocketAddr>().unwrap()),
    ]);
    let router = Arc::new(
        GatewayCatalogRouter::new(
            CLUSTER_ID,
            31,
            7,
            vec![meta_address],
            data_nodes,
            Duration::from_secs(5),
        )
        .unwrap(),
    );
    let snapshot = router.load(1).await.unwrap();
    assert_eq!(snapshot.revision(), 1);
    assert_eq!(snapshot.graph().schema_version(), 1);

    let (state, initial_graph, topology) = snapshot.into_parts();
    let shard_client =
        Arc::new(RemoteShardClient::new_loopback_plaintext(CLUSTER_ID, topology).unwrap());
    let gateway = Arc::new(
        RemoteGatewayService::new_at_revision(
            CLUSTER_ID,
            1,
            initial_graph,
            Arc::clone(&shard_client),
            vec![meta_address],
            16,
            32,
        )
        .unwrap(),
    );
    let (watch_shutdown, watch_shutdown_rx) = watch::channel(false);
    let watcher = tokio::spawn({
        let router = Arc::clone(&router);
        let gateway = Arc::clone(&gateway);
        let shard_client = Arc::clone(&shard_client);
        async move {
            router
                .run_watch(state, gateway, shard_client, watch_shutdown_rx)
                .await;
        }
    });

    let update = CatalogCommand::publish_schema(102, 1, 7, 1, 2)
        .encode()
        .unwrap();
    meta.propose(Request::new(ProposeRequest {
        context: Some(context(102)),
        command: update,
    }))
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if gateway.catalog_revision().unwrap() == 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();

    watch_shutdown.send(true).unwrap();
    watcher.await.unwrap();
    server_shutdown.send(()).unwrap();
    server.await.unwrap().unwrap();
}

#[test]
fn rejects_catalog_placements_without_a_resolvable_data_node() {
    let error = gateway_node::build_remote_topology(
        1,
        &graph(),
        &BTreeMap::from([(10, "127.0.0.1:7110".parse().unwrap())]),
    )
    .unwrap_err();
    assert!(error.to_string().contains("Data node 20"));
}
