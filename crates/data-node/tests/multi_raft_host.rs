use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use data_node::{
    DataNodeHost, EnsureReplicaOutcome, HostError, NodeConfig, NodeIdentity, ReplicaKey,
    ReplicaRole, ReplicaSpec, TransportSecurity,
};
use raft::eraftpb::{Message, MessageType};
use raft_command::{ApplyPreparedV1, CommandBodyV1, CommandEnvelopeV1};
use raft_transport::{RaftRoute, RoutedRaftMessage};
use storage_api::{Keyspace, LogicalKey, Mutation, PreparedMutationBatch};
use tempfile::tempdir;
use temporal_types::TransactionTime;

fn config(root: &std::path::Path) -> NodeConfig {
    let loopback = |port| SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);
    NodeConfig::new(
        NodeIdentity::new([0x51; 16], 7).unwrap(),
        loopback(7101),
        loopback(7101),
        root,
        vec![loopback(7001)],
        TransportSecurity::LoopbackPlaintext,
        BTreeMap::new(),
    )
    .unwrap()
}

fn spec(shard_id: u32) -> ReplicaSpec {
    ReplicaSpec::new(
        1,
        shard_id,
        3,
        vec![7],
        ReplicaRole::Voter,
        5,
        7,
        format!("graph-1-shard-{shard_id}"),
    )
    .unwrap()
}

fn key(shard_id: u32) -> ReplicaKey {
    ReplicaKey::new(1, shard_id).unwrap()
}

fn command(shard_id: u32, request_id: u128, commit: i64, value: &[u8]) -> Vec<u8> {
    CommandEnvelopeV1::new(
        shard_id,
        3,
        request_id,
        CommandBodyV1::ApplyPrepared(ApplyPreparedV1 {
            commit_ts: TransactionTime::new(commit, 0),
            batch: PreparedMutationBatch {
                shard_id,
                txn_id: request_id + 1_000,
                mutations: vec![Mutation::put(
                    0,
                    LogicalKey::in_keyspace(Keyspace::Current, b"vertex/1".to_vec()),
                    value.to_vec(),
                )],
            },
        }),
    )
    .encode()
    .unwrap()
}

#[tokio::test(flavor = "current_thread")]
async fn two_shards_progress_independently_and_recover_from_separate_wals() {
    let temporary = tempdir().unwrap();
    let node_config = config(temporary.path());
    let host = DataNodeHost::open(node_config.clone(), 8).await.unwrap();
    assert_eq!(
        host.ensure_replica(spec(11)).await.unwrap(),
        EnsureReplicaOutcome::Created
    );
    assert_eq!(
        host.ensure_replica(spec(12)).await.unwrap(),
        EnsureReplicaOutcome::Created
    );
    assert_eq!(
        host.ensure_replica(spec(11)).await.unwrap(),
        EnsureReplicaOutcome::Existing
    );

    host.campaign(key(11)).await.unwrap();
    host.campaign(key(12)).await.unwrap();
    host.propose(key(11), 3, 101, command(11, 101, 100, b"shard-11"))
        .await
        .unwrap();
    host.propose(key(12), 3, 102, command(12, 102, 200, b"shard-12"))
        .await
        .unwrap();

    let logical_key = LogicalKey::in_keyspace(Keyspace::Current, b"vertex/1".to_vec());
    assert_eq!(
        host.multi_get(key(11), vec![logical_key.clone()])
            .await
            .unwrap(),
        vec![Some(b"shard-11".to_vec())]
    );
    assert_eq!(
        host.multi_get(key(12), vec![logical_key.clone()])
            .await
            .unwrap(),
        vec![Some(b"shard-12".to_vec())]
    );
    let before_11 = host.status(key(11)).await.unwrap().applied_index();
    let before_12 = host.status(key(12)).await.unwrap().applied_index();
    host.shutdown().await.unwrap();

    let reopened = DataNodeHost::open(node_config, 8).await.unwrap();
    let reopened_status = reopened.status(key(11)).await.unwrap();
    assert_eq!(reopened_status.applied_index(), before_11);
    assert_eq!(reopened_status.role(), ReplicaRole::Voter);
    assert_eq!(reopened_status.schema_version(), 5);
    assert_eq!(reopened_status.backend_generation(), 7);
    assert_eq!(
        reopened.status(key(12)).await.unwrap().applied_index(),
        before_12
    );
    assert_eq!(
        reopened
            .multi_get(key(11), vec![logical_key.clone()])
            .await
            .unwrap(),
        vec![Some(b"shard-11".to_vec())]
    );
    assert_eq!(
        reopened
            .multi_get(key(12), vec![logical_key])
            .await
            .unwrap(),
        vec![Some(b"shard-12".to_vec())]
    );
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn one_full_shard_queue_does_not_consume_another_shards_capacity() {
    let temporary = tempdir().unwrap();
    let host = DataNodeHost::open(config(temporary.path()), 1)
        .await
        .unwrap();
    host.ensure_replica(spec(11)).await.unwrap();
    host.ensure_replica(spec(12)).await.unwrap();

    host.try_tick(key(11)).unwrap();
    assert_eq!(
        host.try_tick(key(11)),
        Err(HostError::Overloaded {
            graph_id: 1,
            shard_id: 11,
        })
    );
    assert_eq!(host.try_tick(key(12)), Ok(()));
    tokio::task::yield_now().await;
    host.shutdown().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn stale_epoch_is_rejected_before_a_command_reaches_raft() {
    let temporary = tempdir().unwrap();
    let host = DataNodeHost::open(config(temporary.path()), 8)
        .await
        .unwrap();
    host.ensure_replica(spec(11)).await.unwrap();
    host.campaign(key(11)).await.unwrap();
    let before = host.status(key(11)).await.unwrap().applied_index();

    assert_eq!(
        host.propose(key(11), 2, 101, command(11, 101, 100, b"must-not-apply"))
            .await,
        Err(HostError::StaleEpoch {
            expected: 3,
            actual: 2,
        })
    );
    assert_eq!(host.status(key(11)).await.unwrap().applied_index(), before);
    host.shutdown().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn routed_raft_messages_are_validated_before_replica_dispatch() {
    let temporary = tempdir().unwrap();
    let host = DataNodeHost::open(config(temporary.path()), 8)
        .await
        .unwrap();
    host.ensure_replica(spec(11)).await.unwrap();
    let message = Message {
        msg_type: MessageType::MsgHeartbeat.into(),
        from: 8,
        to: 7,
        term: 1,
        ..Default::default()
    };

    let wrong_cluster = RoutedRaftMessage::new(
        RaftRoute::new([0x52; 16], 1, 11, 3).unwrap(),
        message.clone(),
    )
    .unwrap();
    assert_eq!(
        host.step_routed(wrong_cluster).await,
        Err(HostError::WrongCluster)
    );

    let wrong_target = RoutedRaftMessage::new(
        RaftRoute::new([0x51; 16], 1, 11, 3).unwrap(),
        Message {
            to: 9,
            ..message.clone()
        },
    )
    .unwrap();
    assert_eq!(
        host.step_routed(wrong_target).await,
        Err(HostError::WrongTarget {
            expected: 7,
            actual: 9,
        })
    );

    let unknown_shard =
        RoutedRaftMessage::new(RaftRoute::new([0x51; 16], 1, 12, 3).unwrap(), message).unwrap();
    assert_eq!(
        host.step_routed(unknown_shard).await,
        Err(HostError::UnknownReplica {
            graph_id: 1,
            shard_id: 12,
        })
    );

    host.shutdown().await.unwrap();
}
