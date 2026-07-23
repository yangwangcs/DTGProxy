use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use cluster_protocol::CLUSTER_PROTOCOL_VERSION;
use cluster_protocol::proto::node_admin_service_client::NodeAdminServiceClient;
use cluster_protocol::proto::node_admin_service_server::NodeAdminService;
use cluster_protocol::proto::node_admin_service_server::NodeAdminServiceServer;
use cluster_protocol::proto::shard_service_client::ShardServiceClient;
use cluster_protocol::proto::shard_service_server::ShardService;
use cluster_protocol::proto::shard_service_server::ShardServiceServer;
use cluster_protocol::proto::{
    AnalyticsArtifactGenerationCursor, AnalyticsArtifactKind,
    DeleteAnalyticsArtifactGenerationRequest, EnsureReplicaRequest, ExecuteRequest,
    GetAnalyticsArtifactGenerationRequest, ListAnalyticsArtifactGenerationHeadsRequest,
    ListAnalyticsArtifactGenerationsRequest, PinAnalyticsArtifactGenerationRequest,
    PutAnalyticsArtifactChunkRequest, ReadRequest, ReplicaRole as WireReplicaRole, RequestContext,
    ScanRequest, ShardContext,
};
use data_node::{
    DataNodeGrpcService, DataNodeHost, DataOperation, NodeConfig, NodeIdentity, ReplicaKey,
    ReplicaRole, ReplicaSpec, RequestAuthorizer, TransportSecurity, decode_key_read_result,
    decode_key_scan_batch, encode_key_read_plan, encode_key_scan_plan,
    encode_rocks_replica_profile,
};
use raft_command::{
    AnalyticsArtifactKindV1, ApplyPreparedV1, CommandBodyV1, CommandEnvelopeV1,
    DeleteAnalyticsArtifactGenerationV1, PinAnalyticsArtifactGenerationV1,
    PutAnalyticsArtifactChunkV1,
};
use storage_api::{KeySpan, Keyspace, LogicalKey, Mutation, PreparedMutationBatch};
use tempfile::tempdir;
use temporal_types::TransactionTime;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Code, Request};

fn now_ms() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
}

fn config(root: &std::path::Path) -> NodeConfig {
    let loopback = |port| SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);
    NodeConfig::new(
        NodeIdentity::new([0x61; 16], 7).unwrap(),
        loopback(7101),
        loopback(7101),
        root,
        vec![loopback(7001)],
        TransportSecurity::LoopbackPlaintext,
        BTreeMap::new(),
    )
    .unwrap()
}

fn spec() -> ReplicaSpec {
    ReplicaSpec::new(
        1,
        11,
        3,
        vec![7],
        ReplicaRole::Voter,
        5,
        7,
        "graph-1-shard-11",
    )
    .unwrap()
}

fn context(request_id: u128, epoch: u64, deadline_unix_ms: u64) -> ShardContext {
    ShardContext {
        request: Some(RequestContext {
            protocol_version: CLUSTER_PROTOCOL_VERSION,
            cluster_id: vec![0x61; 16],
            request_id: request_id.to_be_bytes().to_vec(),
            deadline_unix_ms,
        }),
        graph_id: 1,
        shard_id: 11,
        placement_epoch: epoch,
    }
}

fn command(request_id: u128, value: &[u8]) -> Vec<u8> {
    CommandEnvelopeV1::new(
        11,
        3,
        request_id,
        CommandBodyV1::ApplyPrepared(ApplyPreparedV1 {
            commit_ts: TransactionTime::new(100, 0),
            batch: PreparedMutationBatch {
                shard_id: 11,
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
async fn analytics_artifact_rpc_puts_reads_and_deletes_a_generation() {
    let temporary = tempdir().unwrap();
    let host = Arc::new(
        DataNodeHost::open(config(temporary.path()), 8)
            .await
            .unwrap(),
    );
    host.ensure_replica(spec()).await.unwrap();
    host.campaign(ReplicaKey::new(1, 11).unwrap())
        .await
        .unwrap();
    let service = DataNodeGrpcService::new(Arc::clone(&host));
    let job_id = 701_u128.to_be_bytes().to_vec();
    let created_at_unix_ms = 1_725_000_000_123;

    let first = service
        .put_analytics_artifact_chunk(Request::new(PutAnalyticsArtifactChunkRequest {
            context: Some(context(701, 3, now_ms() + 60_000)),
            job_id: job_id.clone(),
            kind: AnalyticsArtifactKind::Checkpoint.into(),
            generation: 1,
            created_at_unix_ms,
            ordinal: 0,
            previous_digest: vec![0; 32],
            payload: b"first".to_vec(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(!first.duplicate);
    let duplicate = service
        .put_analytics_artifact_chunk(Request::new(PutAnalyticsArtifactChunkRequest {
            context: Some(context(701, 3, now_ms() + 60_000)),
            job_id: job_id.clone(),
            kind: AnalyticsArtifactKind::Checkpoint.into(),
            generation: 1,
            created_at_unix_ms,
            ordinal: 0,
            previous_digest: vec![0; 32],
            payload: b"first".to_vec(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(duplicate.duplicate);

    let digest = blake3::hash(b"first").as_bytes().to_vec();
    let mut content_hasher = blake3::Hasher::new();
    content_hasher.update(b"first");
    content_hasher.update(b"second");
    let content_digest = content_hasher.finalize();
    let second = service
        .put_analytics_artifact_chunk(Request::new(PutAnalyticsArtifactChunkRequest {
            context: Some(context(702, 3, now_ms() + 60_000)),
            job_id: job_id.clone(),
            kind: AnalyticsArtifactKind::Checkpoint.into(),
            generation: 1,
            created_at_unix_ms,
            ordinal: 1,
            previous_digest: digest,
            payload: b"second".to_vec(),
        }))
        .await
        .unwrap()
        .into_inner();
    let listed = service
        .list_analytics_artifact_generations(Request::new(
            ListAnalyticsArtifactGenerationsRequest {
                context: Some(context(7021, 3, now_ms() + 60_000)),
                job_id: job_id.clone(),
                kind: AnalyticsArtifactKind::Checkpoint.into(),
                limit: 16,
            },
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(listed.generations.len(), 1);
    assert_eq!(listed.generations[0].created_at_unix_ms, created_at_unix_ms);
    let invalid_cursor = service
        .list_analytics_artifact_generation_heads(Request::new(
            ListAnalyticsArtifactGenerationHeadsRequest {
                context: Some(context(70211, 3, now_ms() + 60_000)),
                after: Some(AnalyticsArtifactGenerationCursor {
                    job_id: Vec::new(),
                    kind: AnalyticsArtifactKind::Checkpoint.into(),
                    generation: 1,
                }),
                limit: 16,
            },
        ))
        .await
        .unwrap_err();
    assert_eq!(invalid_cursor.code(), Code::InvalidArgument);

    let mismatched_created_at = service
        .put_analytics_artifact_chunk(Request::new(PutAnalyticsArtifactChunkRequest {
            context: Some(context(7022, 3, now_ms() + 60_000)),
            job_id: job_id.clone(),
            kind: AnalyticsArtifactKind::Checkpoint.into(),
            generation: 1,
            created_at_unix_ms: created_at_unix_ms + 1,
            ordinal: 2,
            previous_digest: blake3::hash(b"second").as_bytes().to_vec(),
            payload: b"third".to_vec(),
        }))
        .await
        .unwrap_err();
    assert_eq!(mismatched_created_at.code(), Code::FailedPrecondition);
    let unpinned = service
        .get_analytics_artifact_generation(Request::new(GetAnalyticsArtifactGenerationRequest {
            context: Some(context(703, 3, now_ms() + 60_000)),
            job_id: job_id.clone(),
            kind: AnalyticsArtifactKind::Checkpoint.into(),
            generation: 1,
            expected_chunk_count: 2,
            expected_total_bytes: 11,
            expected_content_digest: content_digest.as_bytes().to_vec(),
        }))
        .await
        .unwrap_err();
    assert_eq!(unpinned.code(), Code::FailedPrecondition);
    service
        .pin_analytics_artifact_generation(Request::new(PinAnalyticsArtifactGenerationRequest {
            context: Some(context(704, 3, now_ms() + 60_000)),
            job_id: job_id.clone(),
            kind: AnalyticsArtifactKind::Checkpoint.into(),
            generation: 1,
            expected_chunk_count: 2,
            expected_total_bytes: 11,
            expected_content_digest: content_digest.as_bytes().to_vec(),
        }))
        .await
        .unwrap();
    service
        .pin_analytics_artifact_generation(Request::new(PinAnalyticsArtifactGenerationRequest {
            context: Some(context(7042, 3, now_ms() + 60_000)),
            job_id: job_id.clone(),
            kind: AnalyticsArtifactKind::Checkpoint.into(),
            generation: 1,
            expected_chunk_count: 2,
            expected_total_bytes: 11,
            expected_content_digest: content_digest.as_bytes().to_vec(),
        }))
        .await
        .unwrap();
    let conflicting_pin = service
        .pin_analytics_artifact_generation(Request::new(PinAnalyticsArtifactGenerationRequest {
            context: Some(context(7043, 3, now_ms() + 60_000)),
            job_id: job_id.clone(),
            kind: AnalyticsArtifactKind::Checkpoint.into(),
            generation: 1,
            expected_chunk_count: 2,
            expected_total_bytes: 11,
            expected_content_digest: vec![0x77; 32],
        }))
        .await
        .unwrap_err();
    assert_eq!(conflicting_pin.code(), Code::FailedPrecondition);
    let stream = service
        .get_analytics_artifact_generation(Request::new(GetAnalyticsArtifactGenerationRequest {
            context: Some(context(703, 3, now_ms() + 60_000)),
            job_id: job_id.clone(),
            kind: AnalyticsArtifactKind::Checkpoint.into(),
            generation: 1,
            expected_chunk_count: 2,
            expected_total_bytes: 11,
            expected_content_digest: content_digest.as_bytes().to_vec(),
        }))
        .await
        .unwrap()
        .into_inner();
    let chunks = stream.collect::<Vec<_>>().await;
    assert_eq!(chunks.len(), 2);
    assert!(chunks.into_iter().all(|chunk| {
        chunk
            .is_ok_and(|chunk| chunk.applied_index != 0 && chunk.applied_index >= second.raft_index)
    }));

    let digest_mismatch = service
        .get_analytics_artifact_generation(Request::new(GetAnalyticsArtifactGenerationRequest {
            context: Some(context(7031, 3, now_ms() + 60_000)),
            job_id: job_id.clone(),
            kind: AnalyticsArtifactKind::Checkpoint.into(),
            generation: 1,
            expected_chunk_count: 2,
            expected_total_bytes: 11,
            expected_content_digest: vec![9; 32],
        }))
        .await
        .unwrap_err();
    assert_eq!(digest_mismatch.code(), Code::FailedPrecondition);

    let invalid_manifest = service
        .get_analytics_artifact_generation(Request::new(GetAnalyticsArtifactGenerationRequest {
            context: Some(context(7032, 3, now_ms() + 60_000)),
            job_id: job_id.clone(),
            kind: AnalyticsArtifactKind::Checkpoint.into(),
            generation: 1,
            expected_chunk_count: 2,
            expected_total_bytes: 1,
            expected_content_digest: vec![0; 32],
        }))
        .await
        .unwrap_err();
    assert_eq!(invalid_manifest.code(), Code::InvalidArgument);

    let fenced = service
        .put_analytics_artifact_chunk(Request::new(PutAnalyticsArtifactChunkRequest {
            context: Some(context(7041, 3, now_ms() + 60_000)),
            job_id: job_id.clone(),
            kind: AnalyticsArtifactKind::Checkpoint.into(),
            generation: 1,
            created_at_unix_ms,
            ordinal: 2,
            previous_digest: vec![9; 32],
            payload: b"wrong".to_vec(),
        }))
        .await
        .unwrap_err();
    assert_eq!(fenced.code(), Code::FailedPrecondition);

    let pinned_delete = service
        .delete_analytics_artifact_generation(Request::new(
            DeleteAnalyticsArtifactGenerationRequest {
                context: Some(context(705, 3, now_ms() + 60_000)),
                job_id: job_id.clone(),
                kind: AnalyticsArtifactKind::Checkpoint.into(),
                generation: 1,
                gc_epoch: 1,
            },
        ))
        .await
        .unwrap_err();
    assert_eq!(pinned_delete.code(), Code::FailedPrecondition);

    drop(service);
    Arc::try_unwrap(host)
        .ok()
        .unwrap()
        .shutdown()
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn generic_execute_rejects_all_artifact_mutations() {
    let temporary = tempdir().unwrap();
    let host = Arc::new(
        DataNodeHost::open(config(temporary.path()), 8)
            .await
            .unwrap(),
    );
    host.ensure_replica(spec()).await.unwrap();
    host.campaign(ReplicaKey::new(1, 11).unwrap())
        .await
        .unwrap();
    let service = DataNodeGrpcService::new(Arc::clone(&host));
    let digest = *blake3::hash(b"bypass").as_bytes();
    let bodies = [
        CommandBodyV1::PutAnalyticsArtifactChunk(
            PutAnalyticsArtifactChunkV1::new(
                790,
                AnalyticsArtifactKindV1::Result,
                1,
                1_725_000_000_123,
                0,
                [0; 32],
                b"bypass".to_vec(),
            )
            .unwrap(),
        ),
        CommandBodyV1::PinAnalyticsArtifactGeneration(
            PinAnalyticsArtifactGenerationV1::new(
                790,
                AnalyticsArtifactKindV1::Result,
                1,
                1,
                6,
                digest,
            )
            .unwrap(),
        ),
        CommandBodyV1::DeleteAnalyticsArtifactGeneration(
            DeleteAnalyticsArtifactGenerationV1::new(790, AnalyticsArtifactKindV1::Result, 1)
                .unwrap(),
        ),
    ];
    for (offset, body) in bodies.into_iter().enumerate() {
        let request_id = 790_u128 + u128::try_from(offset).unwrap();
        let encoded = CommandEnvelopeV1::new(11, 3, request_id, body)
            .encode()
            .unwrap();
        let rejected = service
            .execute(Request::new(ExecuteRequest {
                context: Some(context(request_id, 3, now_ms() + 60_000)),
                command: encoded,
            }))
            .await
            .unwrap_err();
        assert_eq!(rejected.code(), Code::PermissionDenied);
    }
    drop(service);
    Arc::try_unwrap(host)
        .ok()
        .unwrap()
        .shutdown()
        .await
        .unwrap();
}

struct DenyPinAuthorizer;

impl RequestAuthorizer for DenyPinAuthorizer {
    fn authorize(
        &self,
        _context: &cluster_protocol::CommonRequestContext,
        operation: DataOperation,
    ) -> Result<(), tonic::Status> {
        if operation == DataOperation::PinAnalyticsArtifact {
            Err(tonic::Status::permission_denied("pin denied"))
        } else {
            Ok(())
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn artifact_pin_uses_its_own_authorization_operation() {
    let temporary = tempdir().unwrap();
    let host = Arc::new(
        DataNodeHost::open(config(temporary.path()), 8)
            .await
            .unwrap(),
    );
    host.ensure_replica(spec()).await.unwrap();
    host.campaign(ReplicaKey::new(1, 11).unwrap())
        .await
        .unwrap();
    let service =
        DataNodeGrpcService::with_authorizer(Arc::clone(&host), Arc::new(DenyPinAuthorizer));
    let job_id = 791_u128.to_be_bytes().to_vec();
    service
        .put_analytics_artifact_chunk(Request::new(PutAnalyticsArtifactChunkRequest {
            context: Some(context(791, 3, now_ms() + 60_000)),
            job_id: job_id.clone(),
            kind: AnalyticsArtifactKind::Result.into(),
            generation: 1,
            created_at_unix_ms: 1_725_000_000_123,
            ordinal: 0,
            previous_digest: vec![0; 32],
            payload: b"authorized-put".to_vec(),
        }))
        .await
        .unwrap();
    let denied = service
        .pin_analytics_artifact_generation(Request::new(PinAnalyticsArtifactGenerationRequest {
            context: Some(context(792, 3, now_ms() + 60_000)),
            job_id,
            kind: AnalyticsArtifactKind::Result.into(),
            generation: 1,
            expected_chunk_count: 1,
            expected_total_bytes: u64::try_from(b"authorized-put".len()).unwrap(),
            expected_content_digest: blake3::hash(b"authorized-put").as_bytes().to_vec(),
        }))
        .await
        .unwrap_err();
    assert_eq!(denied.code(), Code::PermissionDenied);
    drop(service);
    Arc::try_unwrap(host)
        .ok()
        .unwrap()
        .shutdown()
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn analytics_artifact_get_rejects_old_epoch_after_activation_apply() {
    let temporary = tempdir().unwrap();
    let host = Arc::new(
        DataNodeHost::open(config(temporary.path()), 8)
            .await
            .unwrap(),
    );
    let key = ReplicaKey::new(1, 11).unwrap();
    host.ensure_replica(spec()).await.unwrap();
    host.campaign(key).await.unwrap();
    let service = DataNodeGrpcService::new(Arc::clone(&host));
    let job_id = 750_u128.to_be_bytes().to_vec();
    service
        .put_analytics_artifact_chunk(Request::new(PutAnalyticsArtifactChunkRequest {
            context: Some(context(750, 3, now_ms() + 60_000)),
            job_id: job_id.clone(),
            kind: AnalyticsArtifactKind::Checkpoint.into(),
            generation: 1,
            created_at_unix_ms: 1_725_000_000_123,
            ordinal: 0,
            previous_digest: vec![0; 32],
            payload: b"epoch-three".to_vec(),
        }))
        .await
        .unwrap();
    service
        .pin_analytics_artifact_generation(Request::new(PinAnalyticsArtifactGenerationRequest {
            context: Some(context(7501, 3, now_ms() + 60_000)),
            job_id: job_id.clone(),
            kind: AnalyticsArtifactKind::Checkpoint.into(),
            generation: 1,
            expected_chunk_count: 1,
            expected_total_bytes: u64::try_from(b"epoch-three".len()).unwrap(),
            expected_content_digest: blake3::hash(b"epoch-three").as_bytes().to_vec(),
        }))
        .await
        .unwrap();
    let activation = CommandEnvelopeV1::new(11, 3, 751, CommandBodyV1::ActivatePlacementEpoch(4))
        .encode()
        .unwrap();
    host.propose(key, 3, 751, activation).await.unwrap();
    assert_eq!(host.status(key).await.unwrap().placement_epoch(), 4);

    let stale = service
        .get_analytics_artifact_generation(Request::new(GetAnalyticsArtifactGenerationRequest {
            context: Some(context(752, 3, now_ms() + 60_000)),
            job_id,
            kind: AnalyticsArtifactKind::Checkpoint.into(),
            generation: 1,
            expected_chunk_count: 1,
            expected_total_bytes: u64::try_from(b"epoch-three".len()).unwrap(),
            expected_content_digest: blake3::hash(b"epoch-three").as_bytes().to_vec(),
        }))
        .await
        .unwrap_err();
    assert_eq!(stale.code(), Code::FailedPrecondition);
    assert_eq!(
        stale.metadata().get("dtgproxy-reason").unwrap(),
        "stale_epoch"
    );
    assert_eq!(stale.metadata().get("dtgproxy-current-epoch").unwrap(), "4");

    drop(service);
    Arc::try_unwrap(host)
        .ok()
        .unwrap()
        .shutdown()
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn execute_validates_authority_and_reports_durable_duplicates() {
    let temporary = tempdir().unwrap();
    let host = Arc::new(
        DataNodeHost::open(config(temporary.path()), 8)
            .await
            .unwrap(),
    );
    host.ensure_replica(spec()).await.unwrap();
    let service = DataNodeGrpcService::new(Arc::clone(&host));

    let not_leader = service
        .execute(Request::new(ExecuteRequest {
            context: Some(context(101, 3, now_ms() + 60_000)),
            command: command(101, b"once"),
        }))
        .await
        .unwrap_err();
    assert_eq!(not_leader.code(), Code::FailedPrecondition);
    assert_eq!(
        not_leader.metadata().get("dtgproxy-reason").unwrap(),
        "not_leader"
    );

    host.campaign(ReplicaKey::new(1, 11).unwrap())
        .await
        .unwrap();
    let first = service
        .execute(Request::new(ExecuteRequest {
            context: Some(context(101, 3, now_ms() + 60_000)),
            command: command(101, b"once"),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(!first.duplicate);
    assert!(first.raft_index > 0);

    let replay = service
        .execute(Request::new(ExecuteRequest {
            context: Some(context(101, 3, now_ms() + 60_000)),
            command: command(101, b"once"),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(replay.duplicate);

    let mismatch = service
        .execute(Request::new(ExecuteRequest {
            context: Some(context(101, 3, now_ms() + 60_000)),
            command: command(101, b"different"),
        }))
        .await
        .unwrap_err();
    assert_eq!(mismatch.code(), Code::AlreadyExists);

    drop(service);
    Arc::try_unwrap(host)
        .ok()
        .unwrap()
        .shutdown()
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn deadline_cluster_epoch_and_request_identity_fail_before_apply() {
    let temporary = tempdir().unwrap();
    let host = Arc::new(
        DataNodeHost::open(config(temporary.path()), 8)
            .await
            .unwrap(),
    );
    host.ensure_replica(spec()).await.unwrap();
    host.campaign(ReplicaKey::new(1, 11).unwrap())
        .await
        .unwrap();
    let service = DataNodeGrpcService::new(Arc::clone(&host));

    let expired = service
        .execute(Request::new(ExecuteRequest {
            context: Some(context(102, 3, now_ms().saturating_sub(1))),
            command: command(102, b"expired"),
        }))
        .await
        .unwrap_err();
    assert_eq!(expired.code(), Code::DeadlineExceeded);

    let mut wrong_cluster = context(103, 3, now_ms() + 60_000);
    wrong_cluster.request.as_mut().unwrap().cluster_id = vec![0x62; 16];
    let denied = service
        .execute(Request::new(ExecuteRequest {
            context: Some(wrong_cluster),
            command: command(103, b"denied"),
        }))
        .await
        .unwrap_err();
    assert_eq!(denied.code(), Code::PermissionDenied);

    let stale = service
        .execute(Request::new(ExecuteRequest {
            context: Some(context(104, 2, now_ms() + 60_000)),
            command: command(104, b"stale"),
        }))
        .await
        .unwrap_err();
    assert_eq!(stale.code(), Code::FailedPrecondition);
    assert_eq!(
        stale.metadata().get("dtgproxy-reason").unwrap(),
        "stale_epoch"
    );

    let envelope_mismatch = service
        .execute(Request::new(ExecuteRequest {
            context: Some(context(105, 3, now_ms() + 60_000)),
            command: command(106, b"wrong-id"),
        }))
        .await
        .unwrap_err();
    assert_eq!(envelope_mismatch.code(), Code::InvalidArgument);

    drop(service);
    Arc::try_unwrap(host)
        .ok()
        .unwrap()
        .shutdown()
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn bounded_key_read_plan_round_trips_values_through_the_remote_contract() {
    let temporary = tempdir().unwrap();
    let host = Arc::new(
        DataNodeHost::open(config(temporary.path()), 8)
            .await
            .unwrap(),
    );
    host.ensure_replica(spec()).await.unwrap();
    host.campaign(ReplicaKey::new(1, 11).unwrap())
        .await
        .unwrap();
    let service = DataNodeGrpcService::new(Arc::clone(&host));
    service
        .execute(Request::new(ExecuteRequest {
            context: Some(context(107, 3, now_ms() + 60_000)),
            command: command(107, b"value"),
        }))
        .await
        .unwrap();

    let keys = vec![
        LogicalKey::in_keyspace(Keyspace::Current, b"vertex/1".to_vec()),
        LogicalKey::in_keyspace(Keyspace::Current, b"missing".to_vec()),
    ];
    let response = service
        .read(Request::new(ReadRequest {
            context: Some(context(108, 3, now_ms() + 60_000)),
            plan: encode_key_read_plan(&keys).unwrap(),
            read_proof: Vec::new(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        decode_key_read_result(&response.result).unwrap(),
        vec![Some(b"value".to_vec()), None]
    );

    drop(service);
    Arc::try_unwrap(host)
        .ok()
        .unwrap()
        .shutdown()
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn bounded_key_scan_streams_ordered_batches_through_the_remote_contract() {
    let temporary = tempdir().unwrap();
    let host = Arc::new(
        DataNodeHost::open(config(temporary.path()), 8)
            .await
            .unwrap(),
    );
    host.ensure_replica(spec()).await.unwrap();
    host.campaign(ReplicaKey::new(1, 11).unwrap())
        .await
        .unwrap();
    let service = DataNodeGrpcService::new(Arc::clone(&host));
    service
        .execute(Request::new(ExecuteRequest {
            context: Some(context(208, 3, now_ms() + 60_000)),
            command: command(208, b"scan-value"),
        }))
        .await
        .unwrap();

    let span = KeySpan::prefix(Keyspace::Current, b"vertex/".to_vec());
    let mut stream = service
        .scan(Request::new(ScanRequest {
            context: Some(context(209, 3, now_ms() + 60_000)),
            plan: encode_key_scan_plan(&span).unwrap(),
            read_proof: Vec::new(),
            maximum_batch_bytes: 1024,
        }))
        .await
        .unwrap()
        .into_inner();
    let batch = stream.next().await.unwrap().unwrap();
    assert!(batch.terminal);
    let rows = decode_key_scan_batch(&batch.arrow_record_batch).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].value(), b"scan-value");

    drop(stream);
    drop(service);
    Arc::try_unwrap(host)
        .ok()
        .unwrap()
        .shutdown()
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn node_admin_ensure_replica_is_idempotent_and_can_start_a_leader() {
    let temporary = tempdir().unwrap();
    let host = Arc::new(
        DataNodeHost::open(config(temporary.path()), 8)
            .await
            .unwrap(),
    );
    let service = DataNodeGrpcService::new(Arc::clone(&host));
    let request = || EnsureReplicaRequest {
        context: Some(context(109, 3, now_ms() + 60_000)),
        operation_id: 501_u128.to_be_bytes().to_vec(),
        local_node_id: 7,
        initial_role: WireReplicaRole::Leader.into(),
        schema_version: 5,
        backend_generation: 7,
        backend_profile: encode_rocks_replica_profile(&[7], "admin-graph-1-shard-11").unwrap(),
    };

    let created = service
        .ensure_replica(Request::new(request()))
        .await
        .unwrap()
        .into_inner();
    assert!(created.created);
    assert_eq!(created.status.unwrap().role, WireReplicaRole::Leader as i32);

    let existing = service
        .ensure_replica(Request::new(request()))
        .await
        .unwrap()
        .into_inner();
    assert!(!existing.created);
    assert_eq!(
        host.status(ReplicaKey::new(1, 11).unwrap())
            .await
            .unwrap()
            .backend_generation(),
        7
    );

    drop(service);
    Arc::try_unwrap(host)
        .ok()
        .unwrap()
        .shutdown()
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn tonic_http2_boundary_serves_admin_and_shard_clients() {
    let temporary = tempdir().unwrap();
    let host = Arc::new(
        DataNodeHost::open(config(temporary.path()), 8)
            .await
            .unwrap(),
    );
    let service = DataNodeGrpcService::new(Arc::clone(&host));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (shutdown_sender, shutdown_receiver) = tokio::sync::oneshot::channel();
    let server_service = service.clone();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(ShardServiceServer::new(server_service.clone()))
            .add_service(NodeAdminServiceServer::new(server_service))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = shutdown_receiver.await;
            })
            .await
    });

    let endpoint = format!("http://{address}");
    let mut admin = NodeAdminServiceClient::connect(endpoint.clone())
        .await
        .unwrap();
    let ensured = admin
        .ensure_replica(EnsureReplicaRequest {
            context: Some(context(110, 3, now_ms() + 60_000)),
            operation_id: 502_u128.to_be_bytes().to_vec(),
            local_node_id: 7,
            initial_role: WireReplicaRole::Leader.into(),
            schema_version: 5,
            backend_generation: 7,
            backend_profile: encode_rocks_replica_profile(&[7], "network-graph-1-shard-11")
                .unwrap(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(ensured.created);

    let mut shard = ShardServiceClient::connect(endpoint).await.unwrap();
    let executed = shard
        .execute(ExecuteRequest {
            context: Some(context(111, 3, now_ms() + 60_000)),
            command: command(111, b"over-http2"),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(!executed.duplicate);

    drop(admin);
    drop(shard);
    shutdown_sender.send(()).unwrap();
    server.await.unwrap().unwrap();
    drop(service);
    Arc::try_unwrap(host)
        .ok()
        .unwrap()
        .shutdown()
        .await
        .unwrap();
}
