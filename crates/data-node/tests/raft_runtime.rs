use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener};
use std::sync::Arc;
use std::time::Duration;

use data_node::{
    DataNodeHost, DataRaftRuntime, HostError, NodeConfig, NodeIdentity, ReplicaKey, ReplicaRole,
    ReplicaSpec, TransportSecurity,
};
use raft_command::{ApplyPreparedV1, CommandBodyV1, CommandEnvelopeV1};
use storage_api::{Keyspace, LogicalKey, Mutation, PreparedMutationBatch};
use tempfile::tempdir;
use temporal_types::TransactionTime;

fn localhost(port: u16) -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)
}

fn free_address() -> SocketAddr {
    let listener = TcpListener::bind(localhost(0)).unwrap();
    listener.local_addr().unwrap()
}

fn config(root: &std::path::Path, node_id: u64, service: SocketAddr) -> NodeConfig {
    NodeConfig::new(
        NodeIdentity::new([0x91; 16], node_id).unwrap(),
        service,
        service,
        root,
        vec![localhost(7001)],
        TransportSecurity::LoopbackPlaintext,
        BTreeMap::new(),
    )
    .unwrap()
}

fn spec(node_id: u64) -> ReplicaSpec {
    ReplicaSpec::new(
        1,
        11,
        3,
        vec![1, 2],
        ReplicaRole::Voter,
        5,
        7,
        format!("graph-1-shard-11-node-{node_id}"),
    )
    .unwrap()
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
                txn_id: 1_001,
                mutations: vec![Mutation::put(
                    0,
                    LogicalKey::in_keyspace(Keyspace::Current, b"vertex/1".to_vec()),
                    b"replicated".to_vec(),
                )],
            },
        }),
    )
    .encode()
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shared_runtime_elects_and_replicates_between_two_data_hosts() {
    let first_root = tempdir().unwrap();
    let second_root = tempdir().unwrap();
    let first_raft = free_address();
    let second_raft = free_address();
    let first = Arc::new(
        DataNodeHost::open(config(first_root.path(), 1, free_address()), 32)
            .await
            .unwrap(),
    );
    let second = Arc::new(
        DataNodeHost::open(config(second_root.path(), 2, free_address()), 32)
            .await
            .unwrap(),
    );
    first.ensure_replica(spec(1)).await.unwrap();
    second.ensure_replica(spec(2)).await.unwrap();

    let first_runtime = DataRaftRuntime::start(
        Arc::clone(&first),
        first_raft,
        &BTreeMap::from([(2, second_raft)]),
        32,
        Duration::from_millis(10),
    )
    .await
    .unwrap();
    let second_runtime = DataRaftRuntime::start(
        Arc::clone(&second),
        second_raft,
        &BTreeMap::from([(1, first_raft)]),
        32,
        Duration::from_millis(10),
    )
    .await
    .unwrap();

    let key = ReplicaKey::new(1, 11).unwrap();
    first.campaign(key).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if first.status(key).await.unwrap().is_leader() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();

    let bytes = command(501);
    match first.propose_with_outcome(key, 3, 501, bytes.clone()).await {
        Ok(_) | Err(HostError::ProposalPending { request_id: 501 }) => {}
        other => panic!("unexpected proposal result: {other:?}"),
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if first.proposal_status(key, 501, bytes.clone()).await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();

    let logical_key = LogicalKey::in_keyspace(Keyspace::Current, b"vertex/1".to_vec());
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if second
                .multi_get(key, vec![logical_key.clone()])
                .await
                .unwrap()
                == vec![Some(b"replicated".to_vec())]
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();

    first_runtime.shutdown().await.unwrap();
    second_runtime.shutdown().await.unwrap();
    match Arc::try_unwrap(first) {
        Ok(host) => host.shutdown().await.unwrap(),
        Err(_) => panic!("first host is still retained"),
    }
    match Arc::try_unwrap(second) {
        Ok(host) => host.shutdown().await.unwrap(),
        Err(_) => panic!("second host is still retained"),
    }
}
