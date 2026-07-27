use std::net::TcpListener;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cluster_protocol::CLUSTER_PROTOCOL_VERSION;
use cluster_protocol::proto::node_admin_service_client::NodeAdminServiceClient;
use cluster_protocol::proto::shard_service_client::ShardServiceClient;
use cluster_protocol::proto::{
    AnalyticsArtifactKind, EnsureReplicaRequest, ExecuteRequest,
    GetAnalyticsArtifactGenerationRequest, PinAnalyticsArtifactGenerationRequest,
    PutAnalyticsArtifactChunkRequest, ReplicaRole, RequestContext, ShardContext,
};
use data_node::encode_rocks_replica_profile;
use raft_command::{ApplyPreparedV1, CommandBodyV1, CommandEnvelopeV1};
use storage_api::{Keyspace, LogicalKey, Mutation, PreparedMutationBatch};
use tempfile::tempdir;
use temporal_types::TransactionTime;
use tokio_stream::StreamExt;

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
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

fn context(request_id: u128) -> ShardContext {
    ShardContext {
        request: Some(RequestContext {
            protocol_version: CLUSTER_PROTOCOL_VERSION,
            cluster_id: vec![0x81; 16],
            request_id: request_id.to_be_bytes().to_vec(),
            deadline_unix_ms: now_ms() + 60_000,
        }),
        graph_id: 1,
        shard_id: 11,
        placement_epoch: 3,
    }
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
                    b"survives-process".to_vec(),
                )],
            },
        }),
    )
    .encode()
    .unwrap()
}

fn write_config(path: &Path, data_directory: &Path, port: u16) {
    let json = serde_json::json!({
        "version": 1,
        "cluster_id": "81818181818181818181818181818181",
        "node_id": 7,
        "backend": "rocksdb",
        "listen_address": format!("127.0.0.1:{port}"),
        "advertise_address": format!("127.0.0.1:{port}"),
        "data_directory": data_directory,
        "meta_seeds": ["127.0.0.1:7001"],
        "security": { "mode": "loopback_plaintext" },
        "actor_queue_capacity": 8,
        "shutdown_grace_ms": 5000
    });
    std::fs::write(path, serde_json::to_vec_pretty(&json).unwrap()).unwrap();
}

fn spawn_data(config: &Path) -> Child {
    Command::new(env!("CARGO_BIN_EXE_dtgproxy-data"))
        .args(["--config", config.to_str().unwrap()])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap()
}

async fn wait_for_admin(endpoint: &str) -> NodeAdminServiceClient<tonic::transport::Channel> {
    for _ in 0..100 {
        if let Ok(client) = NodeAdminServiceClient::connect(endpoint.to_owned()).await {
            return client;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("Data process did not become ready at {endpoint}");
}

fn terminate(child: &mut Child) {
    let status = Command::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .status()
        .unwrap();
    assert!(status.success());
    assert!(child.wait().unwrap().success());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn data_process_restarts_persisted_replicas_and_request_deduplication() {
    let temporary = tempdir().unwrap();
    let config_path = temporary.path().join("data-node.json");
    let data_directory = temporary.path().join("data");
    let port = free_port();
    write_config(&config_path, &data_directory, port);
    let endpoint = format!("http://127.0.0.1:{port}");

    let mut first_process = spawn_data(&config_path);
    let mut admin = wait_for_admin(&endpoint).await;
    let ensured = admin
        .ensure_replica(EnsureReplicaRequest {
            context: Some(context(201)),
            operation_id: 901_u128.to_be_bytes().to_vec(),
            local_node_id: 7,
            initial_role: ReplicaRole::Leader.into(),
            schema_version: 5,
            backend_generation: 7,
            backend_profile: encode_rocks_replica_profile(&[7], "process-shard-11").unwrap(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(ensured.created);
    let mut shard = ShardServiceClient::connect(endpoint.clone()).await.unwrap();
    let first = shard
        .execute(ExecuteRequest {
            context: Some(context(202)),
            command: command(202),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(!first.duplicate);
    shard
        .put_analytics_artifact_chunk(PutAnalyticsArtifactChunkRequest {
            context: Some(context(204)),
            job_id: 701_u128.to_be_bytes().to_vec(),
            kind: AnalyticsArtifactKind::Result.into(),
            generation: 1,
            created_at_unix_ms: 1_725_000_000_123,
            ordinal: 0,
            previous_digest: vec![0; 32],
            payload: b"survives-process-restart".to_vec(),
        })
        .await
        .unwrap();
    shard
        .pin_analytics_artifact_generation(PinAnalyticsArtifactGenerationRequest {
            context: Some(context(205)),
            job_id: 701_u128.to_be_bytes().to_vec(),
            kind: AnalyticsArtifactKind::Result.into(),
            generation: 1,
            expected_chunk_count: 1,
            expected_total_bytes: u64::try_from(b"survives-process-restart".len()).unwrap(),
            expected_content_digest: blake3::hash(b"survives-process-restart")
                .as_bytes()
                .to_vec(),
        })
        .await
        .unwrap();
    drop(admin);
    drop(shard);
    terminate(&mut first_process);

    let mut second_process = spawn_data(&config_path);
    let mut admin = wait_for_admin(&endpoint).await;
    let ensured = admin
        .ensure_replica(EnsureReplicaRequest {
            context: Some(context(203)),
            operation_id: 902_u128.to_be_bytes().to_vec(),
            local_node_id: 7,
            initial_role: ReplicaRole::Leader.into(),
            schema_version: 5,
            backend_generation: 7,
            backend_profile: encode_rocks_replica_profile(&[7], "process-shard-11").unwrap(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(!ensured.created);
    let mut shard = ShardServiceClient::connect(endpoint).await.unwrap();
    let replay = shard
        .execute(ExecuteRequest {
            context: Some(context(202)),
            command: command(202),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(replay.duplicate);
    let mut artifacts = shard
        .get_analytics_artifact_generation(GetAnalyticsArtifactGenerationRequest {
            context: Some(context(206)),
            job_id: 701_u128.to_be_bytes().to_vec(),
            kind: AnalyticsArtifactKind::Result.into(),
            generation: 1,
            expected_chunk_count: 1,
            expected_total_bytes: u64::try_from(b"survives-process-restart".len()).unwrap(),
            expected_content_digest: blake3::hash(b"survives-process-restart")
                .as_bytes()
                .to_vec(),
        })
        .await
        .unwrap()
        .into_inner();
    let artifact = artifacts.next().await.unwrap().unwrap();
    assert_eq!(artifact.ordinal, 0);
    assert_eq!(artifact.payload, b"survives-process-restart");
    assert!(artifacts.next().await.is_none());
    drop(admin);
    drop(shard);
    terminate(&mut second_process);
}
