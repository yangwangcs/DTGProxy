use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cluster_protocol::CLUSTER_PROTOCOL_VERSION;
use cluster_protocol::proto::shard_service_server::ShardService;
use cluster_protocol::proto::{
    AnalyticsArtifactKind, GetAnalyticsArtifactGenerationRequest,
    PinAnalyticsArtifactGenerationRequest, PutAnalyticsArtifactChunkRequest, RequestContext,
    ShardContext,
};
use data_node::{
    DataNodeGrpcService, DataNodeHost, DataRaftRuntime, HostError, NodeConfig, NodeIdentity,
    ReplicaKey, ReplicaRole, ReplicaSpec, TransportSecurity,
};
use raft::eraftpb::{Message, MessageType};
use raft_command::{ApplyPreparedV1, CommandBodyV1, CommandEnvelopeV1};
use storage_api::{Keyspace, LogicalKey, Mutation, PreparedMutationBatch};
use tempfile::tempdir;
use temporal_types::TransactionTime;
use tokio_stream::StreamExt;
use tonic::{Code, Request};

fn localhost(port: u16) -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)
}

fn free_address() -> SocketAddr {
    let listener = TcpListener::bind(localhost(0)).unwrap();
    listener.local_addr().unwrap()
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

fn shard_context(request_id: u128, deadline_unix_ms: u64) -> ShardContext {
    ShardContext {
        request: Some(RequestContext {
            protocol_version: CLUSTER_PROTOCOL_VERSION,
            cluster_id: vec![0x91; 16],
            request_id: request_id.to_be_bytes().to_vec(),
            deadline_unix_ms,
        }),
        graph_id: 1,
        shard_id: 11,
        placement_epoch: 3,
    }
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

fn three_voter_spec(node_id: u64) -> ReplicaSpec {
    ReplicaSpec::new(
        1,
        11,
        3,
        vec![1, 2, 3],
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

fn activation_command(request_id: u128) -> Vec<u8> {
    CommandEnvelopeV1::new(11, 3, request_id, CommandBodyV1::ActivatePlacementEpoch(4))
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pending_read_barrier_fails_when_activation_applies_before_spec_commit() {
    let first_root = tempdir().unwrap();
    let second_root = tempdir().unwrap();
    let first_raft = free_address();
    let second_raft = free_address();
    let first = Arc::new(
        DataNodeHost::open(config(first_root.path(), 1, free_address()), 64)
            .await
            .unwrap(),
    );
    let second = Arc::new(
        DataNodeHost::open(config(second_root.path(), 2, free_address()), 64)
            .await
            .unwrap(),
    );
    first.ensure_replica(spec(1)).await.unwrap();
    second.ensure_replica(spec(2)).await.unwrap();
    let first_runtime = DataRaftRuntime::start(
        Arc::clone(&first),
        first_raft,
        &BTreeMap::from([(2, second_raft)]),
        64,
        Duration::from_millis(10),
    )
    .await
    .unwrap();
    let second_runtime = DataRaftRuntime::start(
        Arc::clone(&second),
        second_raft,
        &BTreeMap::from([(1, first_raft)]),
        64,
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
    first
        .leader_read_permit(
            key,
            3,
            910,
            tokio::time::Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap();

    first_runtime.shutdown().await.unwrap();
    second_runtime.shutdown().await.unwrap();
    let _ = first.take_outbound(key, 64).await.unwrap();
    let _ = second.take_outbound(key, 64).await.unwrap();

    let pending = tokio::spawn({
        let first = Arc::clone(&first);
        async move {
            first
                .leader_read_permit(
                    key,
                    3,
                    911,
                    tokio::time::Instant::now() + Duration::from_secs(2),
                )
                .await
        }
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    let dropped_read_heartbeats = first.take_outbound(key, 64).await.unwrap();
    assert!(dropped_read_heartbeats.iter().any(|message| {
        message.get_msg_type() == MessageType::MsgHeartbeat && !message.context.is_empty()
    }));

    let activation = activation_command(912);
    assert!(matches!(
        first
            .propose_with_outcome(key, 3, 912, activation.clone())
            .await,
        Err(HostError::ProposalPending { request_id: 912 })
    ));
    let append = first
        .take_outbound(key, 64)
        .await
        .unwrap()
        .into_iter()
        .find(|message| message.to == 2 && message.get_msg_type() == MessageType::MsgAppend)
        .expect("activation proposal must append to the follower");
    second.step(key, 3, append).await.unwrap();
    let append_response = second
        .take_outbound(key, 64)
        .await
        .unwrap()
        .into_iter()
        .find(|message| message.get_msg_type() == MessageType::MsgAppendResponse)
        .expect("follower must acknowledge the activation append");
    first.step(key, 3, append_response).await.unwrap();
    assert!(first.proposal_status(key, 912, activation).await.is_ok());
    assert_eq!(first.status(key).await.unwrap().placement_epoch(), 4);

    first.try_tick(key).unwrap();
    first.try_tick(key).unwrap();
    first.status(key).await.unwrap();
    let read_heartbeat = first
        .take_outbound(key, 64)
        .await
        .unwrap()
        .into_iter()
        .find(|message| {
            message.to == 2
                && message.get_msg_type() == MessageType::MsgHeartbeat
                && !message.context.is_empty()
        })
        .expect("pending ReadIndex must be retried with its context");
    second.step(key, 3, read_heartbeat).await.unwrap();
    let read_response = second
        .take_outbound(key, 64)
        .await
        .unwrap()
        .into_iter()
        .find(|message| {
            message.get_msg_type() == MessageType::MsgHeartbeatResponse
                && !message.context.is_empty()
        })
        .expect("follower must acknowledge the pending ReadIndex context");
    first.step(key, 3, read_response).await.unwrap();

    assert_eq!(
        pending.await.unwrap(),
        Err(HostError::StaleEpoch {
            expected: 4,
            actual: 3,
        })
    );

    Arc::try_unwrap(first)
        .ok()
        .unwrap()
        .shutdown()
        .await
        .unwrap();
    Arc::try_unwrap(second)
        .ok()
        .unwrap()
        .shutdown()
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_replica_read_index_requires_quorum_and_fails_on_term_change() {
    let first_root = tempdir().unwrap();
    let second_root = tempdir().unwrap();
    let third_root = tempdir().unwrap();
    let first_raft = free_address();
    let second_raft = free_address();
    let third_raft = free_address();
    let first = Arc::new(
        DataNodeHost::open(config(first_root.path(), 1, free_address()), 64)
            .await
            .unwrap(),
    );
    let second = Arc::new(
        DataNodeHost::open(config(second_root.path(), 2, free_address()), 64)
            .await
            .unwrap(),
    );
    let third = Arc::new(
        DataNodeHost::open(config(third_root.path(), 3, free_address()), 64)
            .await
            .unwrap(),
    );
    first.ensure_replica(three_voter_spec(1)).await.unwrap();
    second.ensure_replica(three_voter_spec(2)).await.unwrap();
    third.ensure_replica(three_voter_spec(3)).await.unwrap();

    let first_runtime = DataRaftRuntime::start(
        Arc::clone(&first),
        first_raft,
        &BTreeMap::from([(2, second_raft), (3, third_raft)]),
        64,
        Duration::from_millis(10),
    )
    .await
    .unwrap();
    let second_runtime = DataRaftRuntime::start(
        Arc::clone(&second),
        second_raft,
        &BTreeMap::from([(1, first_raft), (3, third_raft)]),
        64,
        Duration::from_millis(10),
    )
    .await
    .unwrap();
    let third_runtime = DataRaftRuntime::start(
        Arc::clone(&third),
        third_raft,
        &BTreeMap::from([(1, first_raft), (2, second_raft)]),
        64,
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

    let read_index = first
        .leader_read_permit(
            key,
            3,
            901,
            tokio::time::Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap();
    assert_ne!(read_index, 0);
    assert!(first.status(key).await.unwrap().applied_index() >= read_index);

    let service = DataNodeGrpcService::new(Arc::clone(&first));
    let job_id = 904_u128.to_be_bytes().to_vec();
    let put = service
        .put_analytics_artifact_chunk(Request::new(PutAnalyticsArtifactChunkRequest {
            context: Some(shard_context(904, now_ms() + 5_000)),
            job_id: job_id.clone(),
            kind: AnalyticsArtifactKind::Checkpoint.into(),
            generation: 1,
            created_at_unix_ms: 1_725_000_000_123,
            ordinal: 0,
            previous_digest: vec![0; 32],
            payload: b"quorum-artifact".to_vec(),
        }))
        .await
        .unwrap()
        .into_inner();
    service
        .pin_analytics_artifact_generation(Request::new(PinAnalyticsArtifactGenerationRequest {
            context: Some(shard_context(9041, now_ms() + 5_000)),
            job_id: job_id.clone(),
            kind: AnalyticsArtifactKind::Checkpoint.into(),
            generation: 1,
            expected_chunk_count: 1,
            expected_total_bytes: u64::try_from(b"quorum-artifact".len()).unwrap(),
            expected_content_digest: blake3::hash(b"quorum-artifact").as_bytes().to_vec(),
        }))
        .await
        .unwrap();
    let chunks = service
        .get_analytics_artifact_generation(Request::new(GetAnalyticsArtifactGenerationRequest {
            context: Some(shard_context(905, now_ms() + 5_000)),
            job_id: job_id.clone(),
            kind: AnalyticsArtifactKind::Checkpoint.into(),
            generation: 1,
            expected_chunk_count: 1,
            expected_total_bytes: u64::try_from(b"quorum-artifact".len()).unwrap(),
            expected_content_digest: blake3::hash(b"quorum-artifact").as_bytes().to_vec(),
        }))
        .await
        .unwrap()
        .into_inner()
        .collect::<Vec<_>>()
        .await;
    assert_eq!(chunks.len(), 1);
    assert!(chunks[0].as_ref().unwrap().applied_index >= put.raft_index);

    first_runtime.shutdown().await.unwrap();
    second_runtime.shutdown().await.unwrap();
    third_runtime.shutdown().await.unwrap();
    assert!(first.status(key).await.unwrap().is_leader());
    let unavailable = service
        .get_analytics_artifact_generation(Request::new(GetAnalyticsArtifactGenerationRequest {
            context: Some(shard_context(906, now_ms() + 50)),
            job_id,
            kind: AnalyticsArtifactKind::Checkpoint.into(),
            generation: 1,
            expected_chunk_count: 1,
            expected_total_bytes: u64::try_from(b"quorum-artifact".len()).unwrap(),
            expected_content_digest: blake3::hash(b"quorum-artifact").as_bytes().to_vec(),
        }))
        .await
        .unwrap_err();
    assert_eq!(unavailable.code(), Code::DeadlineExceeded);
    assert!(matches!(
        first
            .leader_read_permit(
                key,
                3,
                902,
                tokio::time::Instant::now() + Duration::from_millis(50),
            )
            .await,
        Err(HostError::ReadBarrierDeadline { request_id: 902 })
    ));

    let dropped = tokio::spawn({
        let first = Arc::clone(&first);
        async move {
            first
                .leader_read_permit(
                    key,
                    3,
                    920,
                    tokio::time::Instant::now() + Duration::from_secs(5),
                )
                .await
        }
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    dropped.abort();
    let _ = dropped.await;
    first.status(key).await.unwrap();
    let _ = first.take_outbound(key, 64).await.unwrap();

    let mut retained_read_heartbeat = None;
    for offset in 0..1_021_u128 {
        let request_id = 10_000 + offset;
        let pending = tokio::spawn({
            let first = Arc::clone(&first);
            async move {
                first
                    .leader_read_permit(
                        key,
                        3,
                        request_id,
                        tokio::time::Instant::now() + Duration::from_secs(5),
                    )
                    .await
            }
        });
        let submitted = loop {
            let submitted = first
                .take_outbound(key, 64)
                .await
                .unwrap()
                .into_iter()
                .find(|message| {
                    message.get_msg_type() == MessageType::MsgHeartbeat
                        && message.to == 2
                        && !message.context.is_empty()
                });
            if let Some(submitted) = submitted {
                break submitted;
            }
            tokio::task::yield_now().await;
        };
        pending.abort();
        let _ = pending.await;
        first.status(key).await.unwrap();
        retained_read_heartbeat = Some(submitted);
    }
    let capacity_error = first
        .leader_read_permit(
            key,
            3,
            11_021,
            tokio::time::Instant::now() + Duration::from_millis(20),
        )
        .await;
    assert_eq!(capacity_error, Err(HostError::ReadBarrierLimit));

    let retained_read_heartbeat = retained_read_heartbeat
        .expect("the latest canceled ReadIndex must retain a routable heartbeat");
    let retained_context = retained_read_heartbeat.context.clone();
    second.step(key, 3, retained_read_heartbeat).await.unwrap();
    let read_response = second
        .take_outbound(key, 64)
        .await
        .unwrap()
        .into_iter()
        .find(|message| {
            message.get_msg_type() == MessageType::MsgHeartbeatResponse
                && message.to == 1
                && message.context == retained_context
        })
        .expect("the restored quorum must acknowledge the retained ReadIndex context");
    first.step(key, 3, read_response).await.unwrap();

    let first_runtime = DataRaftRuntime::start(
        Arc::clone(&first),
        first_raft,
        &BTreeMap::from([(2, second_raft), (3, third_raft)]),
        64,
        Duration::from_millis(10),
    )
    .await
    .unwrap();
    let second_runtime = DataRaftRuntime::start(
        Arc::clone(&second),
        second_raft,
        &BTreeMap::from([(1, first_raft), (3, third_raft)]),
        64,
        Duration::from_millis(10),
    )
    .await
    .unwrap();
    let third_runtime = DataRaftRuntime::start(
        Arc::clone(&third),
        third_raft,
        &BTreeMap::from([(1, first_raft), (2, second_raft)]),
        64,
        Duration::from_millis(10),
    )
    .await
    .unwrap();
    let recovered = first
        .leader_read_permit(
            key,
            3,
            950,
            tokio::time::Instant::now() + Duration::from_secs(5),
        )
        .await
        .unwrap();
    assert_ne!(recovered, 0);
    first_runtime.shutdown().await.unwrap();
    second_runtime.shutdown().await.unwrap();
    third_runtime.shutdown().await.unwrap();
    assert!(first.status(key).await.unwrap().is_leader());

    let pending = tokio::spawn({
        let first = Arc::clone(&first);
        async move {
            first
                .leader_read_permit(
                    key,
                    3,
                    903,
                    tokio::time::Instant::now() + Duration::from_secs(2),
                )
                .await
        }
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    let term = first.status(key).await.unwrap().term();
    first
        .step(
            key,
            3,
            Message {
                msg_type: MessageType::MsgHeartbeat.into(),
                from: 2,
                to: 1,
                term: term + 1,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(matches!(
        pending.await.unwrap(),
        Err(HostError::NotLeader { .. })
    ));

    drop(service);
    Arc::try_unwrap(first)
        .ok()
        .unwrap()
        .shutdown()
        .await
        .unwrap();
    Arc::try_unwrap(second)
        .ok()
        .unwrap()
        .shutdown()
        .await
        .unwrap();
    Arc::try_unwrap(third)
        .ok()
        .unwrap()
        .shutdown()
        .await
        .unwrap();
}
