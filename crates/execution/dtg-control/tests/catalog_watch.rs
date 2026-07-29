use dtg_control::{
    BackendClass, BackendGeneration, BindingRole, CatalogCommand, CatalogReplica, CatalogState,
    ControlError, GraphId, PlacementEpoch, ProviderKind, ReplicaBinding, ReplicaBindingRecord,
    RetentionPin, ShardId, ShardPlacement, Version,
};

#[test]
fn catalog_replica_rejects_revision_regression() {
    let revision_one = catalog_with_epoch(Version::new(0), 1);
    let mut replica = CatalogReplica::new(revision_one.clone()).unwrap();

    assert_eq!(
        replica.install(CatalogState::new()),
        Err(ControlError::CatalogRevisionRegression {
            current: Version::new(1),
            received: Version::new(0),
        })
    );
    assert_eq!(replica.snapshot(), &revision_one);
}

#[test]
fn catalog_replica_rejects_stale_placement_epoch_at_new_revision() {
    let revision_one = catalog_with_epoch(Version::new(0), 2);
    let revision_two_with_stale_epoch = catalog_with_epoch(Version::new(0), 1)
        .apply(CatalogCommand::pin_retention(
            Version::new(1),
            GraphId::new(1).unwrap(),
            ShardId::new(1).unwrap(),
            BackendGeneration::new(1).unwrap(),
            RetentionPin::new("watch-test".into()).unwrap(),
        ))
        .unwrap();
    let mut replica = CatalogReplica::new(revision_one.clone()).unwrap();

    assert!(matches!(
        replica.install(revision_two_with_stale_epoch),
        Err(ControlError::CatalogEpochRegression { .. })
    ));
    assert_eq!(replica.snapshot(), &revision_one);
}

fn catalog_with_epoch(expected_version: Version, epoch: u64) -> CatalogState {
    let backend_class = BackendClass::new(ProviderKind::Fjall, 1, 1, ["point"]).unwrap();
    let binding = ReplicaBinding::builder()
        .cluster_id(7)
        .graph_id(1)
        .shard_id(1)
        .placement_epoch(epoch)
        .replica_id(1)
        .backend_generation(1)
        .backend_class_digest(backend_class.digest())
        .provider_kind(ProviderKind::Fjall)
        .contract_version(1)
        .layout_version(1)
        .capability_digest(backend_class.required_capabilities().digest())
        .namespace_id(format!("graph-1-shard-1-epoch-{epoch}"))
        .endpoint_profile_ref("local-fjall")
        .credential_ref("env://DTG_FJALL_ROOT")
        .role(BindingRole::Active)
        .build()
        .unwrap();
    CatalogState::new()
        .apply(CatalogCommand::put_placement(
            expected_version,
            ShardPlacement {
                graph_id: GraphId::new(1).unwrap(),
                shard_id: ShardId::new(1).unwrap(),
                placement_epoch: PlacementEpoch::new(epoch).unwrap(),
                active_generation: BackendGeneration::new(1).unwrap(),
                backend_class: backend_class.clone(),
                replicas: vec![ReplicaBindingRecord::new(binding, backend_class).unwrap()],
            },
        ))
        .unwrap()
}
