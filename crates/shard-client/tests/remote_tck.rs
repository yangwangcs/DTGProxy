use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use cluster_protocol::proto::shard_service_server::ShardServiceServer;
use data_node::{
    DataNodeGrpcService, DataNodeHost, NodeConfig, NodeIdentity, ReplicaKey, ReplicaRole,
    ReplicaSpec, TransportSecurity,
};
use raft_command::{ApplyPreparedV1, CommandBodyV1, CommandEnvelopeV1};
use shard_client::{
    ExecuteCommand, ReadKeysRequest, RemoteReplica, RemoteShardClient, RemoteTopology, ScanRequest,
    ShardClient, ShardRequestContext,
};
use storage_api::{KeySpan, Keyspace, LogicalKey, Mutation, PreparedMutationBatch};
use tempfile::tempdir;
use temporal_types::TransactionTime;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

fn now_ms() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
}

fn context(request_id: u128) -> ShardRequestContext {
    ShardRequestContext::new(1, 11, 3, request_id, now_ms() + 60_000).unwrap()
}

fn command(request_id: u128) -> Vec<u8> {
    CommandEnvelopeV1::new(
        11,
        3,
        request_id,
        CommandBodyV1::ApplyPrepared(ApplyPreparedV1 {
            commit_ts: TransactionTime::new(100, 0),
            batch: PreparedMutationBatch {
                shard_id: 11,
                txn_id: 800,
                mutations: vec![Mutation::put(
                    0,
                    LogicalKey::in_keyspace(Keyspace::Current, b"vertex/remote".to_vec()),
                    b"remote-value".to_vec(),
                )],
            },
        }),
    )
    .encode()
    .unwrap()
}

#[tokio::test(flavor = "current_thread")]
async fn remote_contract_matches_execute_read_scan_status_and_duplicate_semantics() {
    let temporary = tempdir().unwrap();
    let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 7101);
    let config = NodeConfig::new(
        NodeIdentity::new([0x71; 16], 7).unwrap(),
        address,
        address,
        temporary.path(),
        vec![SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 7001)],
        TransportSecurity::LoopbackPlaintext,
        BTreeMap::new(),
    )
    .unwrap();
    let host = Arc::new(DataNodeHost::open(config, 16).await.unwrap());
    host.ensure_replica(
        ReplicaSpec::new(1, 11, 3, vec![7], ReplicaRole::Voter, 1, 1, "remote-shard").unwrap(),
    )
    .await
    .unwrap();
    host.campaign(ReplicaKey::new(1, 11).unwrap())
        .await
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (shutdown, receiver) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(
        Server::builder()
            .add_service(ShardServiceServer::new(DataNodeGrpcService::new(
                Arc::clone(&host),
            )))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = receiver.await;
            }),
    );

    let topology = RemoteTopology::new(
        1,
        1,
        vec![(11, 3, 7, vec![RemoteReplica::new(7, address).unwrap()])],
    )
    .unwrap();
    let client = RemoteShardClient::new_loopback_plaintext([0x71; 16], topology).unwrap();
    let first = client
        .execute(ExecuteCommand::new(context(301), command(301)).unwrap())
        .await
        .unwrap();
    assert!(!first.duplicate());
    let replay = client
        .execute(ExecuteCommand::new(context(301), command(301)).unwrap())
        .await
        .unwrap();
    assert!(replay.duplicate());
    assert_eq!(replay.raft_index(), first.raft_index());

    let key = LogicalKey::in_keyspace(Keyspace::Current, b"vertex/remote".to_vec());
    assert_eq!(
        client
            .read_keys(ReadKeysRequest::new(context(302), vec![key.clone()]).unwrap())
            .await
            .unwrap(),
        vec![Some(b"remote-value".to_vec())]
    );
    let rows = client
        .scan(ScanRequest::new(
            context(303),
            KeySpan::prefix(Keyspace::Current, b"vertex/".to_vec()),
        ))
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].key(), &key);
    assert!(client.status(context(304)).await.unwrap().applied_index() > 0);

    drop(client);
    shutdown.send(()).unwrap();
    server.await.unwrap().unwrap();
    Arc::try_unwrap(host)
        .ok()
        .unwrap()
        .shutdown()
        .await
        .unwrap();
}
