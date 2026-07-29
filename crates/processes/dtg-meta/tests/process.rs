use dtg_control::{
    BackendClass, BindingRole, CatalogCommand, GraphId, PlacementEpoch, ProviderKind,
    ReplicaBinding, ReplicaBindingRecord, RetentionPin, ShardId, ShardPlacement, Version,
};
use dtg_meta::{MetaConfig, MetaProcess};
use dtg_storage::DurabilityPolicy;
use dtg_transaction::{CommitResolution, TransactionId};

#[tokio::test]
async fn meta_process_replays_catalog_and_timestamps_from_fjall_consensus() {
    let root = tempfile::tempdir().unwrap();
    let config = MetaConfig::for_test(root.path(), 7, 1).unwrap();
    let first_transaction = TransactionId::new(41).unwrap();

    let process = MetaProcess::open(config.clone()).await.unwrap();
    process.propose(put_placement()).await.unwrap();
    let first_start = process
        .timestamps()
        .allocate_start_time(first_transaction)
        .await
        .unwrap();
    let first_commit = process
        .timestamps()
        .reserve_commit_time(first_transaction)
        .await
        .unwrap();
    process
        .timestamps()
        .resolve_commit_time(first_transaction, first_commit, CommitResolution::Committed)
        .await
        .unwrap();
    drop(process);

    let restarted = MetaProcess::open(config).await.unwrap();
    assert_eq!(restarted.catalog_version().await, Version::new(1));
    restarted
        .propose(CatalogCommand::pin_retention(
            Version::new(1),
            GraphId::new(1).unwrap(),
            ShardId::new(1).unwrap(),
            dtg_control::BackendGeneration::new(1).unwrap(),
            RetentionPin::new("restart-proof".into()).unwrap(),
        ))
        .await
        .unwrap();
    assert_eq!(restarted.catalog_version().await, Version::new(2));
    assert_eq!(
        restarted
            .timestamps()
            .allocate_start_time(first_transaction)
            .await
            .unwrap(),
        first_start
    );
    assert!(
        restarted
            .timestamps()
            .allocate_start_time(TransactionId::new(42).unwrap())
            .await
            .unwrap()
            > first_commit
    );
    assert_eq!(restarted.rpc_service().protocol_major(), 2);
}

fn put_placement() -> CatalogCommand {
    let backend_class = BackendClass::with_durability(
        ProviderKind::Fjall,
        1,
        1,
        DurabilityPolicy::DurableCommit,
        ["point"],
    )
    .unwrap();
    let binding = ReplicaBinding::builder()
        .cluster_id(7)
        .graph_id(1)
        .shard_id(1)
        .placement_epoch(1)
        .replica_id(1)
        .backend_generation(1)
        .backend_class_digest(backend_class.digest())
        .provider_kind(ProviderKind::Fjall)
        .contract_version(1)
        .layout_version(1)
        .capability_digest(backend_class.required_capabilities().digest())
        .namespace_id("graph-1-shard-1")
        .endpoint_profile_ref("local-fjall")
        .credential_ref("env://DTG_FJALL_ROOT")
        .role(BindingRole::Active)
        .build()
        .unwrap();
    CatalogCommand::put_placement(
        Version::new(0),
        ShardPlacement {
            graph_id: GraphId::new(1).unwrap(),
            shard_id: ShardId::new(1).unwrap(),
            placement_epoch: PlacementEpoch::new(1).unwrap(),
            active_generation: dtg_control::BackendGeneration::new(1).unwrap(),
            backend_class: backend_class.clone(),
            replicas: vec![ReplicaBindingRecord::new(binding, backend_class).unwrap()],
        },
    )
}
