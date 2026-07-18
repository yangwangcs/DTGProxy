use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use cluster_protocol::proto::shard_service_server::ShardServiceServer;
use data_node::{
    DataNodeGrpcService, DataNodeHost, NodeConfig, NodeIdentity, ReplicaKey, ReplicaRole,
    ReplicaSpec, TransportSecurity,
};
use dtgproxy::{PreparedShardTransaction, TransactionCoordinator};
use shard_client::{
    ReadKeysRequest, RemoteReplica, RemoteShardClient, RemoteTopology, ShardClient,
    ShardRequestContext,
};
use storage_api::{Keyspace, LogicalKey, Mutation, PreparedMutationBatch};
use tempfile::tempdir;
use timestamp_oracle::{ManualClock, MemoryTimestampStore, TimestampOracle};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use txn_protocol::IsolationLevel;

fn now_ms() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
}

fn node_config(root: &std::path::Path, node_id: u64) -> NodeConfig {
    let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 7100 + node_id as u16);
    NodeConfig::new(
        NodeIdentity::new([0x72; 16], node_id).unwrap(),
        address,
        address,
        root,
        vec![SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 7001)],
        TransportSecurity::LoopbackPlaintext,
        BTreeMap::new(),
    )
    .unwrap()
}

fn write(
    shard_id: u32,
    transaction_id: u128,
    key: &[u8],
    value: &[u8],
) -> PreparedShardTransaction {
    PreparedShardTransaction::new(
        shard_id,
        7,
        PreparedMutationBatch {
            shard_id,
            txn_id: transaction_id,
            mutations: vec![Mutation::put(
                0,
                LogicalKey::in_keyspace(Keyspace::Current, key.to_vec()),
                value.to_vec(),
            )],
        },
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

#[tokio::test(flavor = "current_thread")]
async fn coordinator_commits_cross_shard_transaction_only_through_remote_clients() {
    let temporary = tempdir().unwrap();
    let host_10 = Arc::new(
        DataNodeHost::open(node_config(&temporary.path().join("node-10"), 10), 16)
            .await
            .unwrap(),
    );
    let host_20 = Arc::new(
        DataNodeHost::open(node_config(&temporary.path().join("node-20"), 20), 16)
            .await
            .unwrap(),
    );
    for (host, node_id, shard_id) in [(&host_10, 10, 10), (&host_20, 20, 20)] {
        host.ensure_replica(
            ReplicaSpec::new(
                1,
                shard_id,
                7,
                vec![node_id],
                ReplicaRole::Voter,
                3,
                1,
                format!("shard-{shard_id}"),
            )
            .unwrap(),
        )
        .await
        .unwrap();
        host.campaign(ReplicaKey::new(1, shard_id).unwrap())
            .await
            .unwrap();
    }
    let (address_10, shutdown_10, server_10) = serve_shard(Arc::clone(&host_10)).await;
    let (address_20, shutdown_20, server_20) = serve_shard(Arc::clone(&host_20)).await;
    let topology = RemoteTopology::new(
        1,
        1,
        vec![
            (10, 7, 10, vec![RemoteReplica::new(10, address_10).unwrap()]),
            (20, 7, 20, vec![RemoteReplica::new(20, address_20).unwrap()]),
        ],
    )
    .unwrap();
    let client = RemoteShardClient::new_loopback_plaintext([0x72; 16], topology).unwrap();
    let oracle = TimestampOracle::open(
        Arc::new(MemoryTimestampStore::new()),
        Arc::new(ManualClock::new(1_000)),
        16,
    )
    .unwrap();
    let coordinator = TransactionCoordinator::new(&oracle, 20);
    let transaction = coordinator
        .begin(3, IsolationLevel::TemporalSnapshot, 10_000)
        .unwrap();
    let receipt = coordinator
        .commit_remote(
            &client,
            1,
            now_ms() + 60_000,
            transaction,
            vec![
                write(20, transaction.transaction_id().value(), b"v/20", b"twenty"),
                write(10, transaction.transaction_id().value(), b"v/10", b"ten"),
            ],
        )
        .await
        .unwrap();
    assert!(!receipt.single_shard_fast_path());
    for (shard_id, request_id, key, expected) in [
        (10, 801, b"v/10".as_slice(), b"ten".as_slice()),
        (20, 802, b"v/20".as_slice(), b"twenty".as_slice()),
    ] {
        let context =
            ShardRequestContext::new(1, shard_id, 7, request_id, now_ms() + 60_000).unwrap();
        let values = client
            .read_keys(
                ReadKeysRequest::new(
                    context,
                    vec![LogicalKey::in_keyspace(Keyspace::Current, key.to_vec())],
                )
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(values, vec![Some(expected.to_vec())]);
    }

    drop(client);
    shutdown_10.send(()).unwrap();
    shutdown_20.send(()).unwrap();
    server_10.await.unwrap().unwrap();
    server_20.await.unwrap().unwrap();
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
