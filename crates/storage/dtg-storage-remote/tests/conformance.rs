#![forbid(unsafe_code)]

use std::sync::Arc;

use dtg_storage::{
    BackendClass, BindingRole, CapabilityManifest, CommandId, CommittedShardBatch, Digest32,
    LogicalMutation, LogicalReplicaActivation, LogicalSnapshotCandidateReceipt,
    LogicalSnapshotSink, LogicalSnapshotSource, Properties, ProviderKind, ReadFence,
    ReplicaBinding, ReplicaMetadata, ReplicaStateStore, SnapshotHeader, SnapshotId,
    SnapshotManifest, SnapshotRequest, StorageError, StorageTckFactory, TransactionTime,
    ValidInterval, Value, Version, VertexId, VertexRead, VertexVersion, run_storage_tck,
};
use dtg_storage_fjall::FjallStorageTckFactory;
use dtg_storage_remote::{
    ReferenceServerConfig, ReferenceStorageServer, RemoteAuthToken, StorageRemoteClient,
};

fn auth(server: &ReferenceStorageServer, binding: &ReplicaBinding) -> RemoteAuthToken {
    server.auth_token(binding.credential_ref()).unwrap()
}

fn fixture_binding(capabilities: &CapabilityManifest) -> ReplicaBinding {
    let provider = ProviderKind::Remote("reference".into());
    let class = BackendClass::new(
        provider.clone(),
        1,
        1,
        capabilities.names().map(str::to_owned),
    )
    .unwrap();
    ReplicaBinding::builder()
        .cluster_id(1)
        .graph_id(7)
        .shard_id(11)
        .placement_epoch(13)
        .replica_id(17)
        .backend_generation(1)
        .backend_class_digest(class.digest())
        .provider_kind(provider)
        .contract_version(1)
        .layout_version(1)
        .capability_digest(capabilities.digest())
        .namespace_id("remote-handshake-major")
        .endpoint_profile_ref("remote-reference")
        .credential_ref("remote-reference")
        .role(BindingRole::Active)
        .build()
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remote_client_passes_the_shared_storage_tck() {
    let root = tempfile::tempdir().unwrap();
    let factory = Arc::new(FjallStorageTckFactory::new(root.path()));
    let server = ReferenceStorageServer::spawn(factory, ReferenceServerConfig::default())
        .await
        .unwrap();
    let remote = server.tck_factory("reference").unwrap();
    run_storage_tck(&remote).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reference_factory_opens_an_empty_remote_store() {
    let root = tempfile::tempdir().unwrap();
    let factory = Arc::new(FjallStorageTckFactory::new(root.path()));
    let server = ReferenceStorageServer::spawn(factory, ReferenceServerConfig::default())
        .await
        .unwrap();
    let remote = server.tck_factory("reference").unwrap();
    let binding = remote.binding("remote-empty", 1).unwrap();
    let store = remote.open(binding.clone()).await.unwrap();

    assert_eq!(ReplicaStateStore::binding(&*store), &binding);
    assert_eq!(store.applied_index().await.unwrap(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wrong_binding_scoped_authentication_is_rejected() {
    let root = tempfile::tempdir().unwrap();
    let factory = Arc::new(FjallStorageTckFactory::new(root.path()));
    let server = ReferenceStorageServer::spawn(factory, ReferenceServerConfig::default())
        .await
        .unwrap();
    let capabilities = server.tck_factory("reference").unwrap().capabilities();
    let binding = fixture_binding(&capabilities);

    let error =
        StorageRemoteClient::connect(server.uri(), binding, RemoteAuthToken::new([0x5a; 32]))
            .await
            .unwrap_err();

    assert!(error.to_string().contains("authentication failed"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_apply_is_atomic_and_exactly_idempotent() {
    let root = tempfile::tempdir().unwrap();
    let factory = Arc::new(FjallStorageTckFactory::new(root.path()));
    let server = ReferenceStorageServer::spawn(factory, ReferenceServerConfig::default())
        .await
        .unwrap();
    let remote = server.tck_factory("reference").unwrap();
    let binding = remote.binding("remote-apply", 1).unwrap();
    let store = remote.open(binding.clone()).await.unwrap();
    let batch = CommittedShardBatch::new(
        binding,
        3,
        1,
        CommandId::new(41).unwrap(),
        vec![LogicalMutation::PutReplicaMetadata(
            ReplicaMetadata::new("owner", Value::String("remote".into())).unwrap(),
        )],
    )
    .unwrap();

    let first = store.apply(batch.clone()).await.unwrap();
    let replay = store.apply(batch).await.unwrap();

    assert!(!first.replayed());
    assert!(replay.replayed());
    assert_eq!(store.applied_index().await.unwrap(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_read_sessions_are_immutable() {
    let root = tempfile::tempdir().unwrap();
    let factory = Arc::new(FjallStorageTckFactory::new(root.path()));
    let server = ReferenceStorageServer::spawn(factory, ReferenceServerConfig::default())
        .await
        .unwrap();
    let remote = server.tck_factory("reference").unwrap();
    let binding = remote.binding("remote-read", 1).unwrap();
    let store = remote.open(binding.clone()).await.unwrap();
    let before = store
        .begin_read_view(ReadFence::new(binding.clone(), 0))
        .await
        .unwrap();
    let vertex = VertexVersion::new(
        VertexId::new(91).unwrap(),
        Version::new(1),
        ValidInterval::new(0, 100).unwrap(),
        TransactionTime::new(1).unwrap(),
        Properties::new(),
    )
    .unwrap();
    store
        .apply(
            CommittedShardBatch::new(
                binding.clone(),
                1,
                1,
                CommandId::new(92).unwrap(),
                vec![LogicalMutation::PutVertex(vertex.clone())],
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let request = VertexRead::new(vertex.id(), 10, TransactionTime::new(10).unwrap());
    let after = store
        .begin_read_view(ReadFence::new(binding, 1))
        .await
        .unwrap();

    assert_eq!(before.get_vertex(request.clone()).await.unwrap(), None);
    assert_eq!(after.get_vertex(request).await.unwrap(), Some(vertex));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_a_remote_read_view_closes_its_session() {
    let root = tempfile::tempdir().unwrap();
    let factory = Arc::new(FjallStorageTckFactory::new(root.path()));
    let server = ReferenceStorageServer::spawn(factory, ReferenceServerConfig::default())
        .await
        .unwrap();
    let remote = server.tck_factory("reference").unwrap();
    let binding = remote.binding("remote-session-close", 1).unwrap();
    let store = remote.open(binding.clone()).await.unwrap();
    let view = store
        .begin_read_view(ReadFence::new(binding, 0))
        .await
        .unwrap();
    assert_eq!(server.active_read_session_count().await, 1);

    drop(view);
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while server.active_read_session_count().await != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn candidate_activation_is_authenticated_fenced_and_retry_safe() {
    let root = tempfile::tempdir().unwrap();
    let factory = Arc::new(FjallStorageTckFactory::new(root.path()));
    let server = ReferenceStorageServer::spawn(factory, ReferenceServerConfig::default())
        .await
        .unwrap();
    let remote = server.tck_factory("reference").unwrap();
    let source_binding = remote.binding("activation-source", 1).unwrap();
    let source = StorageRemoteClient::connect(
        server.uri(),
        source_binding.clone(),
        auth(&server, &source_binding),
    )
    .await
    .unwrap();
    let mut reader = source
        .begin_snapshot(
            ReadFence::new(source_binding.clone(), 0),
            SnapshotRequest::new(700, 2).unwrap(),
        )
        .await
        .unwrap();
    let header = reader.header().clone();
    let mut chunks = Vec::new();
    while let Some(chunk) = reader.next_chunk().await.unwrap() {
        chunks.push(chunk);
    }
    let manifest = reader.finish().await.unwrap();

    let candidate_binding = remote
        .binding("activation-target", 2)
        .unwrap()
        .to_builder()
        .role(BindingRole::Candidate)
        .build()
        .unwrap();
    let active_binding = candidate_binding
        .to_builder()
        .role(BindingRole::Active)
        .build()
        .unwrap();
    let candidate = StorageRemoteClient::connect(
        server.uri(),
        candidate_binding.clone(),
        auth(&server, &candidate_binding),
    )
    .await
    .unwrap();
    let mut writer = candidate
        .begin_restore(candidate_binding.clone(), header.clone())
        .await
        .unwrap();
    for chunk in chunks {
        writer.write_chunk(chunk).await.unwrap();
    }
    writer.commit(manifest.clone()).await.unwrap();
    let receipt = LogicalSnapshotCandidateReceipt::new(
        candidate_binding.clone(),
        header.clone(),
        manifest.clone(),
    )
    .unwrap();
    let old_candidate_view = candidate
        .begin_read_view(ReadFence::new(candidate_binding.clone(), 0))
        .await
        .unwrap();

    let fake_header = SnapshotHeader::new(
        SnapshotId::new(701).unwrap(),
        header.source_binding().clone(),
        header.applied_index(),
        header.format_version(),
    )
    .unwrap();
    let fake_manifest = SnapshotManifest {
        snapshot_id: fake_header.snapshot_id(),
        chunk_count: 0,
        record_count: 0,
        content_digest: Digest32::new([7; 32]),
    };
    let fake_receipt =
        LogicalSnapshotCandidateReceipt::new(candidate_binding.clone(), fake_header, fake_manifest)
            .unwrap();
    assert!(matches!(
        candidate
            .activate_candidate(fake_receipt, active_binding.clone())
            .await,
        Err(StorageError::SnapshotIdentityMismatch)
    ));

    server.arm_activation_response_loss();
    let first = candidate
        .activate_candidate(receipt.clone(), active_binding.clone())
        .await
        .unwrap();
    let replay = candidate
        .activate_candidate(receipt, active_binding.clone())
        .await
        .unwrap();

    assert_eq!(first, replay);
    assert!(matches!(
        candidate.applied_index().await,
        Err(StorageError::StaleBinding { .. })
    ));
    assert!(matches!(
        old_candidate_view
            .get_vertex(VertexRead::new(
                VertexId::new(999).unwrap(),
                1,
                TransactionTime::new(1).unwrap(),
            ))
            .await,
        Err(StorageError::StaleBinding { .. })
    ));
    let active_token = auth(&server, &active_binding);
    let active = StorageRemoteClient::connect(server.uri(), active_binding, active_token)
        .await
        .unwrap();
    assert_eq!(active.applied_index().await.unwrap(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn snapshot_export_resumes_after_a_stream_failure() {
    let root = tempfile::tempdir().unwrap();
    let factory = Arc::new(FjallStorageTckFactory::new(root.path()));
    let server = ReferenceStorageServer::spawn(factory, ReferenceServerConfig::default())
        .await
        .unwrap();
    let remote = server.tck_factory("reference").unwrap();
    let binding = remote.binding("snapshot-resume", 1).unwrap();
    let store = remote.open(binding.clone()).await.unwrap();
    store
        .apply(
            CommittedShardBatch::new(
                binding.clone(),
                1,
                1,
                CommandId::new(801).unwrap(),
                vec![LogicalMutation::PutReplicaMetadata(
                    ReplicaMetadata::new("resume", Value::Integer(1)).unwrap(),
                )],
            )
            .unwrap(),
        )
        .await
        .unwrap();
    server.arm_snapshot_export_failure_after(0);
    let mut reader = store
        .begin_snapshot(
            ReadFence::new(binding, 1),
            SnapshotRequest::new(802, 1).unwrap(),
        )
        .await
        .unwrap();
    let mut ordinals = Vec::new();
    while let Some(chunk) = reader.next_chunk().await.unwrap() {
        ordinals.push(chunk.ordinal());
    }
    let manifest = reader.finish().await.unwrap();

    assert_eq!(ordinals, (0..manifest.chunk_count()).collect::<Vec<_>>());
    assert!(manifest.chunk_count() >= 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn major_contract_mismatch_fails_closed() {
    let root = tempfile::tempdir().unwrap();
    let factory = Arc::new(FjallStorageTckFactory::new(root.path()));
    let capabilities = factory.capabilities();
    let server = ReferenceStorageServer::spawn(
        factory,
        ReferenceServerConfig::default().with_contract_major(2),
    )
    .await
    .unwrap();

    let binding = fixture_binding(&capabilities);
    let error =
        StorageRemoteClient::connect(server.uri(), binding.clone(), auth(&server, &binding))
            .await
            .unwrap_err();
    assert_eq!(error.code(), "DTG-REMOTE-CONTRACT-MAJOR");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn major_protocol_mismatch_fails_closed() {
    let root = tempfile::tempdir().unwrap();
    let factory = Arc::new(FjallStorageTckFactory::new(root.path()));
    let capabilities = factory.capabilities();
    let server = ReferenceStorageServer::spawn(
        factory,
        ReferenceServerConfig::default().with_protocol_major(2),
    )
    .await
    .unwrap();

    let binding = fixture_binding(&capabilities);
    let error =
        StorageRemoteClient::connect(server.uri(), binding.clone(), auth(&server, &binding))
            .await
            .unwrap_err();
    assert_eq!(error.code(), "DTG-REMOTE-PROTOCOL-MAJOR");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn capability_subsets_are_negotiated_and_drift_fails_closed() {
    let root = tempfile::tempdir().unwrap();
    let factory = Arc::new(FjallStorageTckFactory::new(root.path()));
    let full_capabilities = factory.capabilities();
    let subset = CapabilityManifest::from_names(["point"]).unwrap();
    let server = ReferenceStorageServer::spawn(
        factory,
        ReferenceServerConfig::default().with_capabilities(subset.clone()),
    )
    .await
    .unwrap();

    let binding = fixture_binding(&full_capabilities);
    let error =
        StorageRemoteClient::connect(server.uri(), binding.clone(), auth(&server, &binding))
            .await
            .unwrap_err();
    assert_eq!(error.code(), "DTG-REMOTE-CAPABILITIES");

    let remote = server.tck_factory("reference").unwrap();
    let binding = remote.binding("capability-subset", 1).unwrap();
    let token = auth(&server, &binding);
    let client = StorageRemoteClient::connect(server.uri(), binding, token)
        .await
        .unwrap();
    assert_eq!(client.capabilities(), &subset);
}
