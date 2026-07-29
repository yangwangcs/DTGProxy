use dtg_execution::planning::{
    CatalogShard, CatalogSnapshot, PlanningContext, SnapshotRequirements,
};
use dtg_execution::storage::{
    BackendClass, BindingRole, CapabilityManifest, ProviderKind, ReplicaBinding, TransactionTime,
    Version,
};

pub fn planning_context() -> PlanningContext {
    let capabilities =
        CapabilityManifest::from_names(dtg_execution::planning::EXACT_VERTEX_SCAN_CAPABILITIES)
            .unwrap();
    let class = BackendClass::new(
        ProviderKind::Fjall,
        1,
        1,
        capabilities.names().map(str::to_owned),
    )
    .unwrap();
    let binding = ReplicaBinding::builder()
        .cluster_id(7)
        .graph_id(1)
        .shard_id(13)
        .placement_epoch(17)
        .replica_id(19)
        .backend_generation(23)
        .backend_class_digest(class.digest())
        .provider_kind(ProviderKind::Fjall)
        .contract_version(1)
        .layout_version(1)
        .capability_digest(capabilities.digest())
        .namespace_id("gateway-process-test")
        .endpoint_profile_ref("fixture-endpoint")
        .credential_ref("fixture-credential")
        .role(BindingRole::Active)
        .build()
        .unwrap();
    PlanningContext::new(
        CatalogSnapshot::new(
            Version::new(29),
            Version::new(31),
            vec![CatalogShard::new(binding, 37)],
        )
        .unwrap(),
        capabilities,
        SnapshotRequirements::fixed(TransactionTime::new(41).unwrap(), 43),
        Some(128),
    )
    .unwrap()
}
