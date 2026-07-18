use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use adapter_rocksdb::RocksAdapter;
use cluster_protocol::CLUSTER_PROTOCOL_VERSION;
use cluster_protocol::proto::node_admin_service_client::NodeAdminServiceClient;
use cluster_protocol::proto::node_admin_service_server::NodeAdminServiceServer;
use cluster_protocol::proto::shard_service_client::ShardServiceClient;
use cluster_protocol::proto::shard_service_server::ShardServiceServer;
use cluster_protocol::proto::{
    EnsureReplicaRequest, GetMigrationReceiptRequest, ReplicaRole as WireReplicaRole,
    ReplicaStatusRequest, RequestContext, ShardContext, SnapshotChunk,
};
use data_node::{
    DataNodeGrpcService, DataNodeHost, NodeConfig, NodeIdentity, ReplicaKey, ReplicaRole,
    TransportSecurity, encode_rocks_replica_profile,
};
use replica_snapshot::{create_snapshot_bundle, write_snapshot_archive};
use shard_runtime::ShardStateMachine;
use tokio_stream::wrappers::TcpListenerStream;

fn now_ms() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
}

fn common(request_id: u128) -> RequestContext {
    RequestContext {
        protocol_version: CLUSTER_PROTOCOL_VERSION,
        cluster_id: vec![0x65; 16],
        request_id: request_id.to_be_bytes().to_vec(),
        deadline_unix_ms: now_ms() + 60_000,
    }
}

fn shard_context(request_id: u128) -> ShardContext {
    ShardContext {
        request: Some(common(request_id)),
        graph_id: 1,
        shard_id: 11,
        placement_epoch: 3,
    }
}

fn config(root: &std::path::Path) -> NodeConfig {
    let loopback = |port| SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);
    NodeConfig::new(
        NodeIdentity::new([0x65; 16], 7).unwrap(),
        loopback(7101),
        loopback(7101),
        root,
        vec![loopback(7001)],
        TransportSecurity::LoopbackPlaintext,
        BTreeMap::new(),
    )
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn streaming_snapshot_install_is_verified_durable_and_idempotent() {
    let temporary = tempfile::tempdir().unwrap();
    let source = temporary.path().join("source");
    let bundle = temporary.path().join("bundle");
    let mut machine = ShardStateMachine::open(RocksAdapter::open(source).unwrap(), 11, 3)
        .await
        .unwrap();
    machine.apply_noop_entry(1, 1).await.unwrap();
    let manifest = create_snapshot_bundle(&machine, &[7], &bundle).unwrap();
    let mut archive = Vec::new();
    let archive_digest = write_snapshot_archive(&bundle, &mut archive).unwrap();

    let data_root = temporary.path().join("data");
    let host = Arc::new(DataNodeHost::open(config(&data_root), 8).await.unwrap());
    let service = DataNodeGrpcService::new(Arc::clone(&host));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (shutdown, shutdown_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn({
        let service = service.clone();
        async move {
            tonic::transport::Server::builder()
                .add_service(ShardServiceServer::new(service.clone()))
                .add_service(NodeAdminServiceServer::new(service))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    let _ = shutdown_rx.await;
                })
                .await
        }
    });
    let endpoint = format!("http://{address}");
    let mut shard = ShardServiceClient::connect(endpoint.clone()).await.unwrap();
    let mut admin = NodeAdminServiceClient::connect(endpoint).await.unwrap();
    let learner = admin
        .ensure_replica(EnsureReplicaRequest {
            context: Some(shard_context(300)),
            operation_id: 900_u128.to_be_bytes().to_vec(),
            local_node_id: 7,
            initial_role: WireReplicaRole::Learner.into(),
            schema_version: 1,
            backend_generation: 1,
            backend_profile: encode_rocks_replica_profile(&[7], "learner-graph-1-shard-11")
                .unwrap(),
        })
        .await
        .unwrap()
        .into_inner()
        .status
        .unwrap();
    assert!(!learner.ready);
    assert_eq!(learner.role, WireReplicaRole::Learner as i32);

    let migration_id = 901_u128.to_be_bytes();
    let chunks = || {
        archive
            .chunks(64 * 1024)
            .enumerate()
            .map(|(ordinal, payload)| {
                let terminal = ordinal + 1 == archive.len().div_ceil(64 * 1024);
                SnapshotChunk {
                    context: Some(shard_context(301)),
                    migration_id: migration_id.to_vec(),
                    ordinal: ordinal as u64,
                    payload: payload.to_vec(),
                    checksum: crc32fast::hash(payload),
                    terminal,
                    manifest_digest: if terminal {
                        archive_digest.to_vec()
                    } else {
                        Vec::new()
                    },
                }
            })
            .collect::<Vec<_>>()
    };

    let installed = shard
        .install_snapshot(tokio_stream::iter(chunks()))
        .await
        .unwrap()
        .into_inner();
    assert!(!installed.duplicate);
    assert_eq!(installed.installed_index, manifest.applied_index);
    assert_eq!(installed.content_digest, archive_digest);

    let ready = shard
        .replica_status(ReplicaStatusRequest {
            context: Some(shard_context(303)),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(ready.ready);
    assert_eq!(ready.role, WireReplicaRole::Learner as i32);
    assert_eq!(ready.snapshot_index, manifest.applied_index);

    let receipt = admin
        .get_migration_receipt(GetMigrationReceiptRequest {
            context: Some(common(302)),
            migration_id: migration_id.to_vec(),
            step: 2,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(receipt.present);
    assert_eq!(receipt.input_digest, archive_digest);

    let duplicate = shard
        .install_snapshot(tokio_stream::iter(chunks()))
        .await
        .unwrap()
        .into_inner();
    assert!(duplicate.duplicate);
    assert_eq!(duplicate.installed_index, manifest.applied_index);

    drop((shard, admin, service));
    shutdown.send(()).unwrap();
    server.await.unwrap().unwrap();
    Arc::try_unwrap(host)
        .ok()
        .unwrap()
        .shutdown()
        .await
        .unwrap();

    let reopened = DataNodeHost::open(config(&data_root), 8).await.unwrap();
    let recovered = reopened
        .status(ReplicaKey::new(1, 11).unwrap())
        .await
        .unwrap();
    assert_eq!(recovered.role(), ReplicaRole::Learner);
    assert!(recovered.ready());
    assert_eq!(recovered.snapshot_index(), manifest.applied_index);
    reopened.shutdown().await.unwrap();
}
