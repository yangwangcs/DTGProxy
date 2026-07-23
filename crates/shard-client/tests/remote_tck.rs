use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cluster_protocol::proto::shard_service_server::ShardServiceServer;
use data_node::{
    DataNodeGrpcService, DataNodeHost, NodeConfig, NodeIdentity, ReplicaKey, ReplicaRole,
    ReplicaSpec, TransportSecurity,
};
use raft_command::{
    AnalyticsArtifactKindV1, ApplyPreparedV1, CommandBodyV1, CommandEnvelopeV1,
    DeleteAnalyticsArtifactGenerationV1, PinAnalyticsArtifactGenerationV1,
    PutAnalyticsArtifactChunkV1,
};
use shard_client::{
    AdvanceArtifactFenceRequest, ArtifactGenerationCursor, ArtifactKind,
    DeleteArtifactGenerationRequest, ExecuteCommand, GetArtifactGenerationRequest,
    ListArtifactGenerationHeadsRequest, ListArtifactGenerationsRequest,
    PinArtifactGenerationRequest, PutArtifactChunkRequest, ReadKeysRequest, RemoteReplica,
    RemoteShardClient, RemoteTopology, ScanRequest, ShardClient, ShardClientError,
    ShardClientStorageAdapter, ShardRequestContext,
};
use storage_api::{
    AdapterError, KeySpan, Keyspace, LogicalKey, Mutation, PreparedMutationBatch, StorageAdapter,
};
use tempfile::tempdir;
use temporal_types::TransactionTime;
use tokio_stream::StreamExt;
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

fn generic_artifact_command(request_id: u128) -> Vec<u8> {
    CommandEnvelopeV1::new(
        11,
        3,
        request_id,
        CommandBodyV1::PutAnalyticsArtifactChunk(
            PutAnalyticsArtifactChunkV1::new(
                991,
                AnalyticsArtifactKindV1::Result,
                1,
                1_725_000_000_123,
                0,
                [0; 32],
                b"bypass".to_vec(),
            )
            .unwrap(),
        ),
    )
    .encode()
    .unwrap()
}

fn generic_pin_command(request_id: u128) -> Vec<u8> {
    CommandEnvelopeV1::new(
        11,
        3,
        request_id,
        CommandBodyV1::PinAnalyticsArtifactGeneration(
            PinAnalyticsArtifactGenerationV1::new(
                991,
                AnalyticsArtifactKindV1::Result,
                1,
                1,
                6,
                *blake3::hash(b"bypass").as_bytes(),
            )
            .unwrap(),
        ),
    )
    .encode()
    .unwrap()
}

fn generic_delete_command(request_id: u128) -> Vec<u8> {
    CommandEnvelopeV1::new(
        11,
        3,
        request_id,
        CommandBodyV1::DeleteAnalyticsArtifactGeneration(
            DeleteAnalyticsArtifactGenerationV1::new(991, AnalyticsArtifactKindV1::Result, 1)
                .unwrap(),
        ),
    )
    .encode()
    .unwrap()
}

#[tokio::test(flavor = "current_thread")]
async fn remote_execute_retries_until_a_campaigning_catalog_leader_is_ready() {
    let temporary = tempdir().unwrap();
    let follower_config = NodeConfig::new(
        NodeIdentity::new([0x74; 16], 7).unwrap(),
        "127.0.0.1:7110".parse().unwrap(),
        "127.0.0.1:7110".parse().unwrap(),
        temporary.path().join("follower"),
        vec!["127.0.0.1:7001".parse().unwrap()],
        TransportSecurity::LoopbackPlaintext,
        BTreeMap::new(),
    )
    .unwrap();
    let follower = Arc::new(DataNodeHost::open(follower_config, 16).await.unwrap());
    follower
        .ensure_replica(
            ReplicaSpec::new(1, 11, 3, vec![7, 8], ReplicaRole::Voter, 1, 1, "follower").unwrap(),
        )
        .await
        .unwrap();

    let leader_config = NodeConfig::new(
        NodeIdentity::new([0x74; 16], 8).unwrap(),
        "127.0.0.1:7111".parse().unwrap(),
        "127.0.0.1:7111".parse().unwrap(),
        temporary.path().join("leader"),
        vec!["127.0.0.1:7001".parse().unwrap()],
        TransportSecurity::LoopbackPlaintext,
        BTreeMap::new(),
    )
    .unwrap();
    let leader = Arc::new(DataNodeHost::open(leader_config, 16).await.unwrap());
    leader
        .ensure_replica(
            ReplicaSpec::new(1, 11, 3, vec![8], ReplicaRole::Voter, 1, 1, "leader").unwrap(),
        )
        .await
        .unwrap();
    let follower_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let follower_address = follower_listener.local_addr().unwrap();
    let (follower_shutdown, follower_shutdown_rx) = tokio::sync::oneshot::channel();
    let follower_server = tokio::spawn(
        Server::builder()
            .add_service(ShardServiceServer::new(DataNodeGrpcService::new(
                Arc::clone(&follower),
            )))
            .serve_with_incoming_shutdown(TcpListenerStream::new(follower_listener), async {
                let _ = follower_shutdown_rx.await;
            }),
    );
    let leader_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let leader_address = leader_listener.local_addr().unwrap();
    let (leader_shutdown, leader_shutdown_rx) = tokio::sync::oneshot::channel();
    let leader_server = tokio::spawn(
        Server::builder()
            .add_service(ShardServiceServer::new(DataNodeGrpcService::new(
                Arc::clone(&leader),
            )))
            .serve_with_incoming_shutdown(TcpListenerStream::new(leader_listener), async {
                let _ = leader_shutdown_rx.await;
            }),
    );

    let topology = RemoteTopology::new(
        1,
        1,
        vec![(
            11,
            3,
            7,
            vec![
                RemoteReplica::new(7, follower_address).unwrap(),
                RemoteReplica::new(8, leader_address).unwrap(),
            ],
        )],
    )
    .unwrap();
    let client = Arc::new(RemoteShardClient::new_loopback_plaintext([0x74; 16], topology).unwrap());
    let execute = {
        let client = Arc::clone(&client);
        tokio::spawn(async move {
            client
                .execute(ExecuteCommand::new(context(601), command(601)).unwrap())
                .await
        })
    };
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(
        !execute.is_finished(),
        "remote execute must keep retrying while the catalog leader is campaigning"
    );
    leader
        .campaign(ReplicaKey::new(1, 11).unwrap())
        .await
        .unwrap();
    execute.await.unwrap().unwrap();
    let key = LogicalKey::in_keyspace(Keyspace::Current, b"vertex/remote".to_vec());
    assert_eq!(
        client
            .read_keys(ReadKeysRequest::new(context(602), vec![key.clone()]).unwrap())
            .await
            .unwrap(),
        vec![Some(b"remote-value".to_vec())]
    );
    assert_eq!(
        client
            .scan(ScanRequest::new(
                context(603),
                KeySpan::prefix(Keyspace::Current, b"vertex/".to_vec()),
            ))
            .await
            .unwrap()
            .len(),
        1
    );

    follower_shutdown.send(()).unwrap();
    leader_shutdown.send(()).unwrap();
    follower_server.await.unwrap().unwrap();
    leader_server.await.unwrap().unwrap();
    Arc::try_unwrap(follower)
        .ok()
        .unwrap()
        .shutdown()
        .await
        .unwrap();
    Arc::try_unwrap(leader)
        .ok()
        .unwrap()
        .shutdown()
        .await
        .unwrap();
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
    let client = Arc::new(RemoteShardClient::new_loopback_plaintext([0x71; 16], topology).unwrap());
    for (request_id, command) in [
        (3001, generic_artifact_command(3001)),
        (3002, generic_pin_command(3002)),
        (3003, generic_delete_command(3003)),
    ] {
        let rejected = client
            .execute(ExecuteCommand::new(context(request_id), command).unwrap())
            .await
            .unwrap_err();
        assert!(rejected.to_string().contains("explicit artifact RPC"));
    }
    let meta_key = shard_runtime::analytics_artifact_generation_head_key(
        991,
        AnalyticsArtifactKindV1::Result,
        1,
    );
    assert!(
        client
            .read_keys(ReadKeysRequest::new(context(3004), vec![meta_key]).unwrap())
            .await
            .is_err()
    );
    assert!(
        client
            .scan(ScanRequest::new(
                context(3005),
                KeySpan::prefix(Keyspace::Meta, Vec::new()),
            ))
            .await
            .is_err()
    );
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
    let bounded_span = KeySpan::prefix(Keyspace::Current, b"vertex/".to_vec())
        .with_max_bytes(1)
        .unwrap();
    assert_eq!(
        client
            .scan(ScanRequest::new(context(305), bounded_span.clone()))
            .await,
        Err(ShardClientError::ScanByteLimit {
            limit: 1,
            required: 25,
        })
    );
    let adapter_client: Arc<dyn ShardClient> = client.clone();
    let adapter =
        ShardClientStorageAdapter::new(adapter_client, 1, 11, 3, now_ms() + 60_000, 9).unwrap();
    assert_eq!(
        adapter.scan(&bounded_span).await,
        Err(AdapterError::ScanByteLimit {
            limit: 1,
            required: 25,
        })
    );
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

#[tokio::test(flavor = "current_thread")]
async fn remote_contract_writes_reads_and_deletes_analytics_artifacts() {
    let temporary = tempdir().unwrap();
    let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 7102);
    let config = NodeConfig::new(
        NodeIdentity::new([0x72; 16], 7).unwrap(),
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
        ReplicaSpec::new(1, 11, 3, vec![7], ReplicaRole::Voter, 1, 1, "artifacts").unwrap(),
    )
    .await
    .unwrap();
    host.campaign(ReplicaKey::new(1, 11).unwrap())
        .await
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_address = listener.local_addr().unwrap();
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
        vec![(
            11,
            3,
            7,
            vec![RemoteReplica::new(7, server_address).unwrap()],
        )],
    )
    .unwrap();
    let client = RemoteShardClient::new_loopback_plaintext([0x72; 16], topology).unwrap();
    let first = PutArtifactChunkRequest::new(
        context(501),
        901,
        ArtifactKind::Result,
        1,
        1_725_000_000_123,
        0,
        [0; 32],
        b"first".to_vec(),
    )
    .unwrap();
    assert!(
        !client
            .put_artifact_chunk(first.clone())
            .await
            .unwrap()
            .duplicate()
    );
    assert!(client.put_artifact_chunk(first).await.unwrap().duplicate());
    client
        .put_artifact_chunk(
            PutArtifactChunkRequest::new(
                context(502),
                901,
                ArtifactKind::Result,
                1,
                1_725_000_000_123,
                1,
                *blake3::hash(b"first").as_bytes(),
                b"second".to_vec(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let summaries = client
        .list_artifact_generations(
            ListArtifactGenerationsRequest::new(context(5021), 901, ArtifactKind::Result, 16)
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(summaries.len(), 1);
    assert_eq!(summaries[0].created_at_unix_ms(), 1_725_000_000_123);
    let mut content_hasher = blake3::Hasher::new();
    content_hasher.update(b"first");
    content_hasher.update(b"second");
    let content_digest = *content_hasher.finalize().as_bytes();
    assert!(
        client
            .get_artifact_generation(
                GetArtifactGenerationRequest::new(
                    context(503),
                    901,
                    ArtifactKind::Result,
                    1,
                    2,
                    11,
                    content_digest,
                )
                .unwrap(),
            )
            .await
            .is_err()
    );
    client
        .pin_artifact_generation(
            PinArtifactGenerationRequest::new(
                context(504),
                901,
                ArtifactKind::Result,
                1,
                2,
                11,
                content_digest,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let stream = client
        .get_artifact_generation(
            GetArtifactGenerationRequest::new(
                context(503),
                901,
                ArtifactKind::Result,
                1,
                2,
                11,
                content_digest,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let chunks = stream.collect::<Vec<_>>().await;
    assert_eq!(chunks.len(), 2);
    assert!(chunks.into_iter().all(|chunk| chunk.is_ok()));
    let mismatched = client
        .get_artifact_generation(
            GetArtifactGenerationRequest::new(
                context(5031),
                901,
                ArtifactKind::Result,
                1,
                2,
                11,
                [9; 32],
            )
            .unwrap(),
        )
        .await;
    assert!(matches!(
        mismatched,
        Err(ShardClientError::ArtifactCorruption(_))
    ));
    let mut previous_digest = [0; 32];
    let mut large_content_hasher = blake3::Hasher::new();
    for ordinal in 0..17_u64 {
        let payload = vec![u8::try_from(ordinal + 1).unwrap()];
        large_content_hasher.update(&payload);
        client
            .put_artifact_chunk(
                PutArtifactChunkRequest::new(
                    context(520 + u128::from(ordinal)),
                    901,
                    ArtifactKind::Result,
                    2,
                    1_725_000_000_124,
                    ordinal,
                    previous_digest,
                    payload.clone(),
                )
                .unwrap(),
            )
            .await
            .unwrap();
        previous_digest = *blake3::hash(&payload).as_bytes();
    }
    let large_content_digest = *large_content_hasher.finalize().as_bytes();
    client
        .pin_artifact_generation(
            PinArtifactGenerationRequest::new(
                context(549),
                901,
                ArtifactKind::Result,
                2,
                17,
                17,
                large_content_digest,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let mut paused = client
        .get_artifact_generation(
            GetArtifactGenerationRequest::new(
                context(550),
                901,
                ArtifactKind::Result,
                2,
                17,
                17,
                large_content_digest,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    tokio::task::yield_now().await;
    assert!(paused.next().await.unwrap().is_ok());
    tokio::task::yield_now().await;
    drop(paused);
    assert!(client.status(context(551)).await.is_ok());
    assert!(
        client
            .delete_artifact_generation(
                DeleteArtifactGenerationRequest::new(context(504), 901, ArtifactKind::Result, 1)
                    .unwrap(),
            )
            .await
            .is_err()
    );
    let fence_payload = b"fence-target";
    client
        .put_artifact_chunk(
            PutArtifactChunkRequest::new(
                context(552),
                902,
                ArtifactKind::Checkpoint,
                1,
                1_725_000_000_200,
                0,
                [0; 32],
                fence_payload.to_vec(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    client
        .pin_artifact_generation(
            PinArtifactGenerationRequest::new(
                context(553),
                902,
                ArtifactKind::Checkpoint,
                1,
                1,
                u64::try_from(fence_payload.len()).unwrap(),
                *blake3::hash(fence_payload).as_bytes(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    client
        .advance_artifact_fence(
            AdvanceArtifactFenceRequest::new_with_gc_epoch(
                context(554),
                902,
                ArtifactKind::Checkpoint,
                2,
                7,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    client
        .delete_artifact_generation(
            DeleteArtifactGenerationRequest::new_with_gc_epoch(
                context(555),
                902,
                ArtifactKind::Checkpoint,
                1,
                7,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    shutdown.send(()).unwrap();
    server.await.unwrap().unwrap();
    Arc::try_unwrap(host)
        .ok()
        .unwrap()
        .shutdown()
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn remote_shard_wide_artifact_head_pages_match_embedded_semantics() {
    let temporary = tempdir().unwrap();
    let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 7103);
    let config = NodeConfig::new(
        NodeIdentity::new([0x73; 16], 7).unwrap(),
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
        ReplicaSpec::new(1, 11, 3, vec![7], ReplicaRole::Voter, 1, 1, "head-pages").unwrap(),
    )
    .await
    .unwrap();
    host.campaign(ReplicaKey::new(1, 11).unwrap())
        .await
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_address = listener.local_addr().unwrap();
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
        vec![(
            11,
            3,
            7,
            vec![RemoteReplica::new(7, server_address).unwrap()],
        )],
    )
    .unwrap();
    let client = RemoteShardClient::new_loopback_plaintext([0x73; 16], topology).unwrap();
    for (offset, (job_id, kind)) in [
        (900, ArtifactKind::Checkpoint),
        (900, ArtifactKind::Result),
        (901, ArtifactKind::Checkpoint),
        (901, ArtifactKind::Result),
    ]
    .into_iter()
    .enumerate()
    {
        client
            .put_artifact_chunk(
                PutArtifactChunkRequest::new(
                    context(900 + u128::try_from(offset).unwrap()),
                    job_id,
                    kind,
                    1,
                    1_725_000_000_123 + u64::try_from(offset).unwrap(),
                    0,
                    [0; 32],
                    vec![u8::try_from(offset + 1).unwrap()],
                )
                .unwrap(),
            )
            .await
            .unwrap();
    }
    let pinned_digest = *blake3::hash(&[2]).as_bytes();
    client
        .pin_artifact_generation(
            PinArtifactGenerationRequest::new(
                context(909),
                900,
                ArtifactKind::Result,
                1,
                1,
                1,
                pinned_digest,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let first = client
        .list_artifact_generation_heads(
            ListArtifactGenerationHeadsRequest::new(context(910), None, 2).unwrap(),
        )
        .await
        .unwrap();
    let second = client
        .list_artifact_generation_heads(
            ListArtifactGenerationHeadsRequest::new(context(911), first.next(), 2).unwrap(),
        )
        .await
        .unwrap();
    let terminal = client
        .list_artifact_generation_heads(
            ListArtifactGenerationHeadsRequest::new(context(912), second.next(), 2).unwrap(),
        )
        .await
        .unwrap();
    let observed = first
        .generations()
        .iter()
        .chain(second.generations())
        .map(|summary| (summary.job_id(), summary.kind(), summary.generation()))
        .collect::<Vec<_>>();
    assert_eq!(
        observed,
        vec![
            (900, ArtifactKind::Checkpoint, 1),
            (900, ArtifactKind::Result, 1),
            (901, ArtifactKind::Checkpoint, 1),
            (901, ArtifactKind::Result, 1),
        ]
    );
    assert_eq!(
        first.next(),
        Some(ArtifactGenerationCursor::new(900, ArtifactKind::Result, 1).unwrap())
    );
    assert_eq!(
        second.next(),
        Some(ArtifactGenerationCursor::new(901, ArtifactKind::Result, 1).unwrap())
    );
    assert!(first.generations()[1].pinned());
    assert_eq!(first.generations()[1].expected_total_bytes(), 1);
    assert_eq!(
        first.generations()[1].expected_content_digest(),
        pinned_digest
    );
    assert!(terminal.generations().is_empty());
    assert_eq!(terminal.next(), None);

    shutdown.send(()).unwrap();
    server.await.unwrap().unwrap();
    Arc::try_unwrap(host)
        .ok()
        .unwrap()
        .shutdown()
        .await
        .unwrap();
}
