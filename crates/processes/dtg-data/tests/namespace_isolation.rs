use dtg_data::{DataNodeBuilder, FjallResolver};
use dtg_execution::storage::{BackendClass, BindingRole, CapabilityManifest};
use dtg_execution::{ProviderKind, ReplicaBinding};

fn fjall_binding(namespace: &str, placement_epoch: u64, generation: u64) -> ReplicaBinding {
    let capabilities = CapabilityManifest::from_names([
        "adjacency",
        "immutable-read-view",
        "logical-snapshot",
        "point",
    ])
    .unwrap();
    let backend = BackendClass::new(
        ProviderKind::Fjall,
        1,
        1,
        capabilities.names().map(str::to_owned),
    )
    .unwrap();
    ReplicaBinding::builder()
        .cluster_id(1)
        .graph_id(7)
        .shard_id(11)
        .placement_epoch(placement_epoch)
        .replica_id(17)
        .backend_generation(generation)
        .backend_class_digest(backend.digest())
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

#[tokio::test]
async fn namespace_owner_fences_epoch_generation_and_replica_owner() {
    let root = tempfile::tempdir().unwrap();
    let first = fjall_binding("complete-owner", 1, 1);
    let conflicting = fjall_binding("complete-owner", 2, 2)
        .to_builder()
        .replica_id(19)
        .build()
        .unwrap();
    let node = DataNodeBuilder::new(root.path().join("raft"))
        .with_provider(
            ProviderKind::Fjall,
            std::sync::Arc::new(FjallResolver::new(root.path().join("business"))),
        )
        .assign(first.clone())
        .assign(conflicting.clone())
        .start()
        .await
        .unwrap();

    assert_eq!(node.observed_replicas().await, vec![first]);
    let failures = node.replica_failures().await;
    assert_eq!(failures.len(), 1);
    assert_eq!(failures[0].binding(), &conflicting);
    assert!(failures[0].message().contains("namespace owner"));
}

#[tokio::test]
async fn restart_reopens_the_same_business_and_consensus_namespaces() {
    let root = tempfile::tempdir().unwrap();
    let business_root = root.path().join("business");
    let consensus_root = root.path().join("raft");
    let binding = fjall_binding("restart-stable", 3, 5);

    let first = DataNodeBuilder::new(&consensus_root)
        .with_provider(
            ProviderKind::Fjall,
            std::sync::Arc::new(FjallResolver::new(&business_root)),
        )
        .assign(binding.clone())
        .start()
        .await
        .unwrap();
    assert_eq!(first.observed_replicas().await, vec![binding.clone()]);
    drop(first);

    let restarted = DataNodeBuilder::new(&consensus_root)
        .with_provider(
            ProviderKind::Fjall,
            std::sync::Arc::new(FjallResolver::new(&business_root)),
        )
        .assign(binding.clone())
        .start()
        .await
        .unwrap();

    assert!(restarted.replica_failures().await.is_empty());
    assert_eq!(restarted.observed_replicas().await, vec![binding]);
}
