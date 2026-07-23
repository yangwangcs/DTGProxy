use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

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
async fn joint_membership_is_applied_durably_and_retries_are_idempotent() {
    let temporary = tempdir().unwrap();
    let node_config = config(temporary.path());
    let host = DataNodeHost::open(node_config.clone(), 8).await.unwrap();
    host.ensure_replica(spec(11)).await.unwrap();
    host.campaign(key(11)).await.unwrap();

    let (changed, duplicate) = host
        .change_membership(key(11), 3, 800, vec![7], vec![7], vec![8])
        .await
        .unwrap();
    assert!(!duplicate);
    assert!(changed.applied_index() > 0);
    let (same, duplicate) = host
        .change_membership(key(11), 3, 800, vec![7], vec![7], vec![8])
        .await
        .unwrap();
    assert!(duplicate);
    assert_eq!(same.applied_index(), changed.applied_index());
    host.shutdown().await.unwrap();

    let reopened = DataNodeHost::open(node_config, 8).await.unwrap();
    reopened.campaign(key(11)).await.unwrap();
    let (_, duplicate) = reopened
        .change_membership(key(11), 3, 800, vec![7], vec![7], vec![8])
        .await
        .unwrap();
    assert!(duplicate);
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn placement_epoch_fence_precedes_durable_replica_activation() {
    let temporary = tempdir().unwrap();
    let node_config = config(temporary.path());
    let host = DataNodeHost::open(node_config.clone(), 8).await.unwrap();
    host.ensure_replica(spec(11)).await.unwrap();
    host.campaign(key(11)).await.unwrap();
    let fence = CommandEnvelopeV1::new(11, 3, 850, CommandBodyV1::ActivatePlacementEpoch(4))
        .encode()
        .unwrap();
    host.propose(key(11), 3, 850, fence).await.unwrap();

    assert_eq!(host.status(key(11)).await.unwrap().placement_epoch(), 4);
    assert_eq!(
        host.leader_read_permit(
            key(11),
            3,
            851,
            tokio::time::Instant::now() + Duration::from_secs(1),
        )
        .await,
        Err(HostError::StaleEpoch {
            expected: 4,
            actual: 3,
        })
    );

    let (activated, duplicate) = host.activate_replica(key(11), 3, 4, vec![7]).await.unwrap();
    assert!(!duplicate);
    assert_eq!(activated.placement_epoch(), 4);
    assert!(matches!(
        host.propose(key(11), 3, 851, command(11, 851, 300, b"stale"))
            .await
            .unwrap_err(),
        HostError::StaleEpoch {
            expected: 4,
            actual: 3
        }
    ));
    let (_, duplicate) = host.activate_replica(key(11), 3, 4, vec![7]).await.unwrap();
    assert!(duplicate);
    host.shutdown().await.unwrap();

    let reopened = DataNodeHost::open(node_config, 8).await.unwrap();
    assert_eq!(reopened.status(key(11)).await.unwrap().placement_epoch(), 4);
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn safe_delete_removes_admission_and_moves_replica_data_to_recoverable_trash() {
    let temporary = tempdir().unwrap();
    let node_config = config(temporary.path());
    let host = DataNodeHost::open(node_config.clone(), 8).await.unwrap();
    let learner = ReplicaSpec::new(
        1,
        13,
        3,
        vec![8],
        ReplicaRole::Learner,
        5,
        7,
        "graph-1-shard-13",
    )
    .unwrap();
    host.ensure_replica(learner).await.unwrap();
    let replica_directory = temporary.path().join("graph-1-shard-13");
    std::fs::create_dir_all(&replica_directory).unwrap();
    std::fs::write(replica_directory.join("marker"), b"recoverable").unwrap();

    assert!(host.delete_replica(key(13), 3, 990, 0).await.unwrap());
    assert!(matches!(
        host.status(key(13)).await,
        Err(HostError::UnknownReplica { .. })
    ));
    assert_eq!(
        std::fs::read(
            temporary
                .path()
                .join("trash")
                .join(format!("{:032x}", 990_u128))
                .join("marker")
        )
        .unwrap(),
        b"recoverable"
    );
    host.shutdown().await.unwrap();

    let reopened = DataNodeHost::open(node_config, 8).await.unwrap();
    assert!(matches!(
        reopened.status(key(13)).await,
        Err(HostError::UnknownReplica { .. })
    ));
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
async fn single_replica_leader_read_barrier_returns_a_nonzero_applied_index() {
    let temporary = tempdir().unwrap();
    let host = DataNodeHost::open(config(temporary.path()), 8)
        .await
        .unwrap();
    host.ensure_replica(spec(11)).await.unwrap();
    host.campaign(key(11)).await.unwrap();

    let read_index = host
        .leader_read_permit(
            key(11),
            3,
            201,
            tokio::time::Instant::now() + Duration::from_secs(1),
        )
        .await
        .unwrap();

    assert_ne!(read_index, 0);
    assert!(host.status(key(11)).await.unwrap().applied_index() >= read_index);
    host.shutdown().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn leader_read_barrier_rejects_a_stale_placement_epoch() {
    let temporary = tempdir().unwrap();
    let host = DataNodeHost::open(config(temporary.path()), 8)
        .await
        .unwrap();
    host.ensure_replica(spec(11)).await.unwrap();
    host.campaign(key(11)).await.unwrap();

    assert_eq!(
        host.leader_read_permit(
            key(11),
            2,
            202,
            tokio::time::Instant::now() + Duration::from_secs(1),
        )
        .await,
        Err(HostError::StaleEpoch {
            expected: 3,
            actual: 2,
        })
    );
    host.shutdown().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn proposal_outcome_reports_durable_request_replay() {
    let temporary = tempdir().unwrap();
    let node_config = config(temporary.path());
    let host = DataNodeHost::open(node_config.clone(), 8).await.unwrap();
    host.ensure_replica(spec(11)).await.unwrap();
    host.campaign(key(11)).await.unwrap();
    let bytes = command(11, 101, 100, b"once");

    let first = host
        .propose_with_outcome(key(11), 3, 101, bytes.clone())
        .await
        .unwrap();
    assert!(!first.duplicate());
    let replay = host
        .propose_with_outcome(key(11), 3, 101, bytes.clone())
        .await
        .unwrap();
    assert!(replay.duplicate());
    host.shutdown().await.unwrap();

    let reopened = DataNodeHost::open(node_config, 8).await.unwrap();
    reopened.campaign(key(11)).await.unwrap();
    let after_restart = reopened
        .propose_with_outcome(key(11), 3, 101, bytes)
        .await
        .unwrap();
    assert!(after_restart.duplicate());
    reopened.shutdown().await.unwrap();
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
