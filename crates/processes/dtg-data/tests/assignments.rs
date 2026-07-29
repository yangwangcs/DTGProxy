use dtg_data::{AssignmentUpdate, DataNodeBuilder, FjallResolver};
use dtg_execution::storage::{BackendClass, BindingRole, CapabilityManifest};
use dtg_execution::{ProviderKind, ReplicaBinding};
use std::sync::Arc;

fn binding(namespace: &str) -> ReplicaBinding {
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
        .cluster_id(1)
        .graph_id(7)
        .shard_id(11)
        .placement_epoch(13)
        .replica_id(17)
        .backend_generation(19)
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

#[tokio::test]
async fn assignment_updates_add_and_remove_a_running_replica() {
    let root = tempfile::tempdir().unwrap();
    let assigned_binding = binding("watched-assignment");
    let node = DataNodeBuilder::new(root.path().join("raft"))
        .with_provider(
            ProviderKind::Fjall,
            std::sync::Arc::new(FjallResolver::new(root.path().join("business"))),
        )
        .start()
        .await
        .unwrap();

    node.apply_assignment(AssignmentUpdate::Assign(assigned_binding.clone()))
        .await
        .unwrap();
    node.apply_assignment(AssignmentUpdate::Assign(assigned_binding.clone()))
        .await
        .unwrap();
    assert_eq!(
        node.observed_replicas().await,
        vec![assigned_binding.clone()]
    );
    assert!(node.replica_observations().await[0].is_running());

    node.apply_assignment(AssignmentUpdate::Remove(assigned_binding))
        .await
        .unwrap();
    node.apply_assignment(AssignmentUpdate::Remove(binding("watched-assignment")))
        .await
        .unwrap();
    assert!(node.observed_replicas().await.is_empty());
    assert!(node.replica_observations().await.is_empty());
}

#[tokio::test]
async fn assignment_watch_applies_meta_updates_until_the_stream_closes() {
    let root = tempfile::tempdir().unwrap();
    let binding = binding("watched-stream");
    let node = Arc::new(
        DataNodeBuilder::new(root.path().join("raft"))
            .with_provider(
                ProviderKind::Fjall,
                Arc::new(FjallResolver::new(root.path().join("business"))),
            )
            .start()
            .await
            .unwrap(),
    );
    let (sender, receiver) = tokio::sync::mpsc::channel(2);
    sender
        .send(AssignmentUpdate::Assign(binding.clone()))
        .await
        .unwrap();
    drop(sender);

    Arc::clone(&node).watch_assignments(receiver).await.unwrap();

    assert_eq!(node.observed_replicas().await, vec![binding]);
}
