use std::sync::Arc;

use dtg_data::{
    CredentialProfile, DataNodeBuilder, DataProcessConfig, EndpointProfile, LifecycleState,
};
use dtg_execution::ProviderKind;
use dtg_execution::cluster_protocol::PROTOCOL_MAJOR;
use dtg_execution::cluster_protocol::proto::RaftEnvelope;
use dtg_execution::cluster_protocol::proto::data_service_server::DataService;
use dtg_execution::storage::StorageTckFactory;
use dtg_storage_fjall::FjallStorageTckFactory;
use dtg_storage_remote::{ReferenceServerConfig, ReferenceStorageServer};
use tonic::{Code, Request};

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
