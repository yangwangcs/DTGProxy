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
    EnsureReplicaRequest, ExecuteRequest, ReadRequest, ReplicaRole as WireReplicaRole,
    RequestContext, ShardContext,
};
use data_node::{
    DataNodeGrpcService, DataNodeHost, NodeConfig, NodeIdentity, ReplicaKey, ReplicaRole,
    ReplicaSpec, TransportSecurity, decode_key_read_result, encode_key_read_plan,
    encode_rocks_replica_profile,
};
use raft_command::{ApplyPreparedV1, CommandBodyV1, CommandEnvelopeV1};
use storage_api::{Keyspace, LogicalKey, Mutation, PreparedMutationBatch};
use tempfile::tempdir;
use temporal_types::TransactionTime;
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
