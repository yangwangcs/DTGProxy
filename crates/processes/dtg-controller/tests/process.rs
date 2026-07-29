use dtg_cluster_v2::proto::{ControlObservation, controller_service_server::ControllerService};
use dtg_control::{CatalogState, ObservedNodeState, Version};
use dtg_controller::{ControllerConfig, ControllerProcess};
use tonic::Request;

#[tokio::test]
async fn controller_process_delegates_observation_and_reconciliation_to_execution() {
    let root = tempfile::tempdir().unwrap();
    let config = ControllerConfig::for_test(root.path(), 7, 2).unwrap();
    let process = ControllerProcess::open(config, CatalogState::new())
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
