use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use cluster_protocol::CLUSTER_PROTOCOL_VERSION;
use cluster_protocol::proto::node_admin_service_client::NodeAdminServiceClient;
use cluster_protocol::proto::node_admin_service_server::NodeAdminServiceServer;
use cluster_protocol::proto::shard_service_client::ShardServiceClient;
use cluster_protocol::proto::shard_service_server::ShardServiceServer;
use cluster_protocol::proto::{
    EnsureReplicaRequest, ExportSnapshotRequest, GetMigrationReceiptRequest,
    ReplicaRole as WireReplicaRole, RequestContext, ShardContext,
};
use data_node::{
    DataNodeGrpcService, DataNodeHost, NodeConfig, NodeIdentity, TransportSecurity,
    encode_rocks_replica_profile,
};
use tokio_stream::StreamExt;
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
        cluster_id: vec![0x66; 16],
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
        NodeIdentity::new([0x66; 16], 7).unwrap(),
        loopback(7201),
        loopback(7201),
        root,
        vec![loopback(7001)],
        TransportSecurity::LoopbackPlaintext,
        BTreeMap::new(),
    )
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn leader_exports_a_canonical_resumable_snapshot_stream_and_receipt() {
    let temporary = tempfile::tempdir().unwrap();
    let host = Arc::new(
        DataNodeHost::open(config(temporary.path()), 8)
            .await
            .unwrap(),
    );
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
    admin
        .ensure_replica(EnsureReplicaRequest {
            context: Some(shard_context(700)),
            operation_id: 700_u128.to_be_bytes().to_vec(),
            local_node_id: 7,
            initial_role: WireReplicaRole::Leader.into(),
            schema_version: 1,
            backend_generation: 1,
            backend_profile: encode_rocks_replica_profile(&[7], "export-shard-11").unwrap(),
        })
        .await
        .unwrap();

    let migration_id = 701_u128.to_be_bytes();
    let mut stream = shard
        .export_snapshot(ExportSnapshotRequest {
            context: Some(shard_context(701)),
            migration_id: migration_id.to_vec(),
        })
        .await
        .unwrap()
        .into_inner();
    let mut archive = Vec::new();
    let mut expected_ordinal = 0;
    let mut terminal_digest = None;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.unwrap();
        assert_eq!(chunk.ordinal, expected_ordinal);
        assert_eq!(chunk.checksum, crc32fast::hash(&chunk.payload));
        archive.extend_from_slice(&chunk.payload);
        if chunk.terminal {
            terminal_digest = Some(chunk.manifest_digest);
        }
        expected_ordinal += 1;
    }
    let digest = *blake3::hash(&archive).as_bytes();
    assert_eq!(terminal_digest.unwrap(), digest);
    let extracted = temporary.path().join("extracted");
    let manifest =
        replica_snapshot::extract_snapshot_archive(archive.as_slice(), &extracted).unwrap();
    assert_eq!(manifest.shard_id, 11);
    assert_eq!(manifest.placement_epoch, 3);

    let receipt = admin
        .get_migration_receipt(GetMigrationReceiptRequest {
            context: Some(common(702)),
            migration_id: migration_id.to_vec(),
            step: 1,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(receipt.present);
    assert_eq!(receipt.input_digest, digest);

    drop((shard, admin, service));
    shutdown.send(()).unwrap();
    server.await.unwrap().unwrap();
    Arc::try_unwrap(host)
        .ok()
        .unwrap()
        .shutdown()
        .await
        .unwrap();
}
