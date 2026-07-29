use std::sync::Arc;

use dtg_data::{
    CredentialProfile, DataNodeBuilder, DataProcessConfig, EndpointProfile, LifecycleState,
};
use dtg_execution::ProviderKind;
use dtg_execution::cluster_protocol::PROTOCOL_MAJOR;
use dtg_execution::cluster_protocol::checksum_bytes;
use dtg_execution::cluster_protocol::proto::data_service_server::DataService;
use dtg_execution::cluster_protocol::proto::{
    BoundedPayload, ExecutionFragment, LogicalReplicaSnapshot, RaftEnvelope, RaftMessageKind,
    RequestContext, ShardContext, StatusCode, TransactionOperation, TransactionRequest,
};
use dtg_execution::shard::{CommitSingleShard, ShardCommand};
use dtg_execution::storage::{
    BackendClass, BindingRole, CapabilityManifest, CommandId, LogicalMutation, Properties,
    ReplicaBinding, StorageTckFactory, TransactionTime, ValidInterval, Version, VertexId,
    VertexVersion,
};
use dtg_storage_fjall::FjallStorageTckFactory;
use dtg_storage_remote::{ReferenceServerConfig, ReferenceStorageServer};
use prost_011::Message as _;
use tokio_stream::StreamExt;
use tonic::{Code, Request};

fn fjall_binding(namespace: &str) -> ReplicaBinding {
    let capabilities = CapabilityManifest::from_names([
        "adjacency",
        "immutable-read-view",
        "logical-snapshot",
        "point",
    ])
    .unwrap();
    let class = BackendClass::new(
        ProviderKind::Fjall,
        1,
        1,
        capabilities.names().map(str::to_owned),
    )
    .unwrap();
    ReplicaBinding::builder()
        .cluster_id(7)
        .graph_id(11)
        .shard_id(13)
        .placement_epoch(17)
        .replica_id(19)
        .backend_generation(23)
        .backend_class_digest(class.digest())
        .provider_kind(ProviderKind::Fjall)
        .contract_version(1)
        .layout_version(1)
        .capability_digest(capabilities.digest())
        .namespace_id(namespace)
        .endpoint_profile_ref("local")
        .credential_ref("local")
        .role(BindingRole::Active)
        .build()
        .unwrap()
}

fn shard_context(binding: &ReplicaBinding) -> ShardContext {
    ShardContext {
        request: Some(RequestContext {
            protocol_major: PROTOCOL_MAJOR,
            protocol_minor: 1,
            cluster_id: binding.cluster_id().get().to_be_bytes().to_vec(),
            request_id: 29_u128.to_be_bytes().to_vec(),
            deadline_unix_ms: u64::MAX,
            trace_context: Vec::new(),
        }),
        graph_id: binding.graph_id().get(),
        shard_id: u32::try_from(binding.shard_id().get()).unwrap(),
        placement_epoch: binding.placement_epoch().get(),
        backend_generation: binding.backend_generation().get(),
        catalog_version: 31,
    }
}

#[tokio::test]
async fn process_composes_official_providers_and_v2_lifecycle_metrics() {
    let root = tempfile::tempdir().unwrap();
    let config = DataProcessConfig::new(root.path().join("business"), root.path().join("raft"))
        .with_endpoint_profile(
            "postgres-primary",
            EndpointProfile::PostgreSql("host=127.0.0.1 port=5432 dbname=dtg".into()),
        )
        .with_credential_profile(
            "postgres-primary",
            CredentialProfile::PostgreSql("user=dtg password=secret".into()),
        )
        .with_endpoint_profile(
            "neo4j-primary",
            EndpointProfile::Neo4j {
                endpoint: "http://127.0.0.1:7474".into(),
                database: "neo4j".into(),
            },
        )
        .with_credential_profile(
            "neo4j-primary",
            CredentialProfile::Neo4jBasic {
                username: "neo4j".into(),
                password: "secret".into(),
            },
        );
    let node = DataNodeBuilder::from_config(config).start().await.unwrap();

    assert_eq!(
        node.provider_kinds(),
        vec![
            ProviderKind::Fjall,
            ProviderKind::PostgreSql,
            ProviderKind::Neo4j,
        ]
    );
    assert_eq!(node.rpc_service().protocol_major(), PROTOCOL_MAJOR);
    assert_eq!(node.lifecycle(), LifecycleState::Ready);
    assert_eq!(node.metrics().hosted_replicas(), 0);
    assert_eq!(node.metrics().failed_replicas(), 0);

    node.begin_draining();
    assert_eq!(node.lifecycle(), LifecycleState::Draining);
    node.stop();
    assert_eq!(node.lifecycle(), LifecycleState::Stopped);
}

#[tokio::test]
async fn data_node_background_driver_ticks_running_replicas() {
    let root = tempfile::tempdir().unwrap();
    let binding = fjall_binding("background-tick");
    let node = DataNodeBuilder::from_config(
        DataProcessConfig::new(root.path().join("business"), root.path().join("raft"))
            .assign(binding),
    )
    .start()
    .await
    .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    assert!(node.metrics().raft_ticks() > 0);
}

#[tokio::test]
async fn v2_rpc_rejects_malformed_requests_and_records_metrics() {
    let root = tempfile::tempdir().unwrap();
    let node = DataNodeBuilder::from_config(DataProcessConfig::new(
        root.path().join("business"),
        root.path().join("raft"),
    ))
    .start()
    .await
    .unwrap();
    let service = node.rpc_service();

    let error = service
        .send_raft(Request::new(RaftEnvelope::default()))
        .await
        .unwrap_err();

    assert_eq!(error.code(), Code::InvalidArgument);
    assert_eq!(service.metrics().rpc_requests(), 1);
    assert_eq!(service.metrics().rpc_failures(), 1);
}

#[tokio::test]
async fn apply_transaction_proposes_the_typed_shard_command() {
    let root = tempfile::tempdir().unwrap();
    let binding = fjall_binding("rpc-transaction");
    let node = DataNodeBuilder::from_config(
        DataProcessConfig::new(root.path().join("business"), root.path().join("raft"))
            .assign(binding.clone()),
    )
    .start()
    .await
    .unwrap();
    let vertex = VertexVersion::new(
        VertexId::new(37).unwrap(),
        Version::new(1),
        ValidInterval::new(1, 100).unwrap(),
        TransactionTime::new(41).unwrap(),
        Properties::new(),
    )
    .unwrap();
    let command = ShardCommand::CommitSingleShard(
        CommitSingleShard::new(
            CommandId::new(43).unwrap(),
            binding.placement_epoch().get(),
            binding.backend_generation().get(),
            vec![LogicalMutation::PutVertex(vertex)],
        )
        .unwrap(),
    );
    let body = command.encode_current().unwrap();
    let response = node
        .rpc_service()
        .apply_transaction(Request::new(TransactionRequest {
            context: Some(shard_context(&binding)),
            transaction_id: 47_u128.to_be_bytes().to_vec(),
            operation: TransactionOperation::Commit.into(),
            idempotency_key: 43_u128.to_be_bytes().to_vec(),
            payload: Some(BoundedPayload {
                format_version: 1,
                declared_len: body.len() as u64,
                item_count: 1,
                checksum: checksum_bytes(&body).to_vec(),
                body,
            }),
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(response.code, StatusCode::Ok as i32);
    assert!(node.replica_observations().await[0].applied_index() >= 2);
}

#[tokio::test]
async fn execute_fragment_reads_the_fenced_replica_store() {
    let root = tempfile::tempdir().unwrap();
    let binding = fjall_binding("rpc-fragment");
    let node = DataNodeBuilder::from_config(
        DataProcessConfig::new(root.path().join("business"), root.path().join("raft"))
            .assign(binding.clone()),
    )
    .start()
    .await
    .unwrap();
    let vertex = VertexVersion::new(
        VertexId::new(37).unwrap(),
        Version::new(1),
        ValidInterval::new(1, 100).unwrap(),
        TransactionTime::new(41).unwrap(),
        Properties::new(),
    )
    .unwrap();
    let command = ShardCommand::CommitSingleShard(
        CommitSingleShard::new(
            CommandId::new(53).unwrap(),
            binding.placement_epoch().get(),
            binding.backend_generation().get(),
            vec![LogicalMutation::PutVertex(vertex)],
        )
        .unwrap(),
    );
    let command_body = command.encode_current().unwrap();
    node.rpc_service()
        .apply_transaction(Request::new(TransactionRequest {
            context: Some(shard_context(&binding)),
            transaction_id: 59_u128.to_be_bytes().to_vec(),
            operation: TransactionOperation::Commit.into(),
            idempotency_key: 53_u128.to_be_bytes().to_vec(),
            payload: Some(BoundedPayload {
                format_version: 1,
                declared_len: command_body.len() as u64,
                item_count: 1,
                checksum: checksum_bytes(&command_body).to_vec(),
                body: command_body,
            }),
        }))
        .await
        .unwrap();
    let applied_index = node.replica_observations().await[0].applied_index();
    let mut body = Vec::new();
    body.extend_from_slice(&1_u64.to_be_bytes());
    body.extend_from_slice(&1_u32.to_be_bytes());
    body.extend_from_slice(&0_u32.to_be_bytes());
    body.extend_from_slice(&1_u32.to_be_bytes());
    body.extend_from_slice(&1_u32.to_be_bytes());
    body.push(1);
    body.extend_from_slice(&1_u32.to_be_bytes());
    body.extend_from_slice(&0_u32.to_be_bytes());
    body.push(1);
    body.extend_from_slice(&10_i64.to_be_bytes());
    body.extend_from_slice(&41_i64.to_be_bytes());
    body.push(0);
    body.extend_from_slice(&10_u32.to_be_bytes());
    body.push(0x1f);
    body.push(0);
    let mut stream = node
        .rpc_service()
        .execute_fragment(Request::new(ExecutionFragment {
            context: Some(shard_context(&binding)),
            fragment_id: 61_u128.to_be_bytes().to_vec(),
            payload: Some(BoundedPayload {
                format_version: 1,
                declared_len: body.len() as u64,
                item_count: 1,
                checksum: checksum_bytes(&body).to_vec(),
                body,
            }),
            schema_version: 31,
            capability_digest: binding.capability_digest().get().to_vec(),
            applied_index,
            transaction_time: 41,
            valid_at: 10,
            snapshot_immutable: true,
        }))
        .await
        .unwrap()
        .into_inner();
    let batch = stream.next().await.unwrap().unwrap();

    assert_eq!(batch.row_count, 1);
    assert!(stream.next().await.is_none());
}

#[tokio::test]
async fn send_raft_steps_the_target_replica() {
    let root = tempfile::tempdir().unwrap();
    let binding = fjall_binding("rpc-raft");
    let node = DataNodeBuilder::from_config(
        DataProcessConfig::new(root.path().join("business"), root.path().join("raft"))
            .assign(binding.clone()),
    )
    .start()
    .await
    .unwrap();
    let message = raft::eraftpb::Message {
        msg_type: raft::eraftpb::MessageType::MsgHeartbeat as i32,
        to: binding.replica_id().get(),
        from: 101,
        term: 1,
        commit: 1,
        ..Default::default()
    };
    let body = message.encode_to_vec();
    let response = node
        .rpc_service()
        .send_raft(Request::new(RaftEnvelope {
            context: Some(shard_context(&binding)),
            from_replica_id: 101,
            to_replica_id: binding.replica_id().get(),
            term: 1,
            committed_index: 1,
            kind: RaftMessageKind::Heartbeat.into(),
            payload: Some(BoundedPayload {
                format_version: 1,
                declared_len: body.len() as u64,
                item_count: 1,
                checksum: checksum_bytes(&body).to_vec(),
                body,
            }),
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(response.code, StatusCode::Ok as i32);
}

#[tokio::test]
async fn send_raft_rejects_an_envelope_kind_that_disagrees_with_the_payload() {
    let root = tempfile::tempdir().unwrap();
    let binding = fjall_binding("rpc-raft-kind");
    let node = DataNodeBuilder::from_config(
        DataProcessConfig::new(root.path().join("business"), root.path().join("raft"))
            .assign(binding.clone()),
    )
    .start()
    .await
    .unwrap();
    let message = raft::eraftpb::Message {
        msg_type: raft::eraftpb::MessageType::MsgHeartbeat as i32,
        to: binding.replica_id().get(),
        from: 101,
        term: 1,
        commit: 1,
        ..Default::default()
    };
    let body = message.encode_to_vec();

    let error = node
        .rpc_service()
        .send_raft(Request::new(RaftEnvelope {
            context: Some(shard_context(&binding)),
            from_replica_id: 101,
            to_replica_id: binding.replica_id().get(),
            term: 1,
            committed_index: 1,
            kind: RaftMessageKind::Vote.into(),
            payload: Some(BoundedPayload {
                format_version: 1,
                declared_len: body.len() as u64,
                item_count: 1,
                checksum: checksum_bytes(&body).to_vec(),
                body,
            }),
        }))
        .await
        .unwrap_err();

    assert_eq!(error.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn snapshot_rpc_fails_closed_with_a_typed_retryable_status() {
    let root = tempfile::tempdir().unwrap();
    let binding = fjall_binding("rpc-snapshot");
    let node = DataNodeBuilder::from_config(
        DataProcessConfig::new(root.path().join("business"), root.path().join("raft"))
            .assign(binding.clone()),
    )
    .start()
    .await
    .unwrap();
    let body = vec![1_u8];
    let response = node
        .rpc_service()
        .install_replica_snapshot(Request::new(LogicalReplicaSnapshot {
            context: Some(shard_context(&binding)),
            snapshot_id: 67_u128.to_be_bytes().to_vec(),
            snapshot_version: 1,
            last_included_term: 1,
            last_included_index: 1,
            chunk_index: 0,
            chunk_count: 1,
            payload: Some(BoundedPayload {
                format_version: 1,
                declared_len: body.len() as u64,
                item_count: 1,
                checksum: checksum_bytes(&body).to_vec(),
                body,
            }),
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(response.code, StatusCode::Unavailable as i32);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_provider_uses_the_versioned_storage_client_path() {
    let root = tempfile::tempdir().unwrap();
    let remote_root = root.path().join("remote");
    std::fs::create_dir_all(&remote_root).unwrap();
    let server = ReferenceStorageServer::spawn(
        Arc::new(FjallStorageTckFactory::new(remote_root)),
        ReferenceServerConfig::default(),
    )
    .await
    .unwrap();
    let remote = server.tck_factory("third-party").unwrap();
    let binding = remote.binding("remote-process", 1).unwrap();
    let endpoint_profile = binding.endpoint_profile_ref().to_owned();
    let credential_profile = binding.credential_ref().to_owned();
    let config = DataProcessConfig::new(root.path().join("business"), root.path().join("raft"))
        .with_remote_provider("third-party")
        .with_endpoint_profile(endpoint_profile, EndpointProfile::Remote(server.uri()))
        .with_credential_profile(
            credential_profile,
            CredentialProfile::RemoteSignedToken([0xd7; 32]),
        )
        .assign(binding);

    let node = DataNodeBuilder::from_config(config).start().await.unwrap();

    let failures = node.replica_failures().await;
    assert!(
        failures.is_empty(),
        "unexpected remote failure: {failures:#?}"
    );
    assert_eq!(node.observed_replicas().await.len(), 1);
    assert!(
        node.provider_kinds()
            .contains(&ProviderKind::Remote("third-party".into()))
    );
}
