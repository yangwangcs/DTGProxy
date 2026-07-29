use dtg_cluster_v2::{
    PROTOCOL_MAJOR, checksum_bytes,
    proto::{
        BoundedPayload, ControlObservation, RequestContext,
        controller_service_server::ControllerService,
    },
};
use dtg_control::{
    BackendClass, BackendGeneration, BindingRole, CatalogCommand, CatalogState, GraphId,
    ObservedNodeState, PlacementEpoch, ProviderKind, ReplicaBinding, ReplicaBindingRecord, ShardId,
    ShardPlacement, Version,
};
use dtg_controller::{ControllerConfig, ControllerProcess};
use tonic::Request;

#[tokio::test]
async fn controller_process_delegates_observation_and_reconciliation_to_execution() {
    let root = tempfile::tempdir().unwrap();
    let config = ControllerConfig::for_test(root.path(), 7, 2).unwrap();
    let process = ControllerProcess::open_for_test(config, CatalogState::new())
        .await
        .unwrap();

    process
        .record_observation(
            ObservedNodeState::new("data-1".into(), Version::new(0), Vec::new()).unwrap(),
        )
        .await
        .unwrap();
    assert!(process.reconcile().await.unwrap().is_empty());
    assert_eq!(process.rpc_service().protocol_major(), 2);

    let response = process
        .rpc_service()
        .observe(Request::new(ControlObservation::default()))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.code, 2);
    assert!(response.message.contains("DTG-PROTOCOL"));
}

#[tokio::test]
async fn controller_rpc_preserves_complete_replica_observation() {
    let root = tempfile::tempdir().unwrap();
    let config = ControllerConfig::for_test(root.path(), 7, 2).unwrap();
    let (catalog, binding, backend_class) = catalog_fixture();
    let process = ControllerProcess::open_for_test(config, catalog)
        .await
        .unwrap();
    let body = serde_json::to_vec(&serde_json::json!({
        "catalog_version": 1,
        "replicas": [{
            "binding": {
                "cluster_id": 7,
                "graph_id": 1,
                "shard_id": 1,
                "placement_epoch": 3,
                "replica_id": 11,
                "backend_generation": 4,
                "provider": "fjall",
                "contract_version": 1,
                "layout_version": 1,
                "namespace_id": binding.namespace_id().as_str(),
                "endpoint_profile_ref": binding.endpoint_profile_ref(),
                "credential_ref": binding.credential_ref(),
                "role": "active"
            },
            "backend_class": {
                "provider": "fjall",
                "contract_version": 1,
                "layout_version": 1,
                "durability": "durable_commit",
                "capabilities": backend_class.required_capabilities().names().collect::<Vec<_>>()
            },
            "lifecycle": "voter",
            "applied_index": 44,
            "leader_id": 11,
            "closed_timestamp": 30,
            "caught_up": true
        }]
    }))
    .unwrap();
    let response = process
        .rpc_service()
        .observe(Request::new(ControlObservation {
            request: Some(RequestContext {
                protocol_major: PROTOCOL_MAJOR,
                protocol_minor: 0,
                cluster_id: 7_u64.to_be_bytes().to_vec(),
                request_id: 99_u128.to_be_bytes().to_vec(),
                deadline_unix_ms: 1_900_000_000_000,
                trace_context: Vec::new(),
            }),
            node_id: b"data-node-000001".to_vec(),
            observation_version: 1,
            observed_at_unix_ms: 1_800_000_000_000,
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

    assert_eq!(response.code, 1, "{}", response.message);
    assert!(process.reconcile().await.unwrap().is_empty());
}

fn catalog_fixture() -> (CatalogState, ReplicaBinding, BackendClass) {
    let backend_class = BackendClass::new(ProviderKind::Fjall, 1, 1, ["point"]).unwrap();
    let binding = ReplicaBinding::builder()
        .cluster_id(7)
        .graph_id(1)
        .shard_id(1)
        .placement_epoch(3)
        .replica_id(11)
        .backend_generation(4)
        .backend_class_digest(backend_class.digest())
        .provider_kind(ProviderKind::Fjall)
        .contract_version(1)
        .layout_version(1)
        .capability_digest(backend_class.required_capabilities().digest())
        .namespace_id("graph-1-shard-1-replica-11")
        .endpoint_profile_ref("local-fjall")
        .credential_ref("env://DTG_FJALL_ROOT")
        .role(BindingRole::Active)
        .build()
        .unwrap();
    let catalog = CatalogState::new()
        .apply(CatalogCommand::put_placement(
            Version::new(0),
            ShardPlacement {
                graph_id: GraphId::new(1).unwrap(),
                shard_id: ShardId::new(1).unwrap(),
                placement_epoch: PlacementEpoch::new(3).unwrap(),
                active_generation: BackendGeneration::new(4).unwrap(),
                backend_class: backend_class.clone(),
                replicas: vec![
                    ReplicaBindingRecord::new(binding.clone(), backend_class.clone()).unwrap(),
                ],
            },
        ))
        .unwrap();
    (catalog, binding, backend_class)
}
