use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use dtg_controller::ControllerConfig;
use dtg_data::{DataNodeBuilder, DataProcessConfig, LifecycleState};
use dtg_execution::control::{CatalogState, ObservedNodeState, Version};
use dtg_execution::transaction::TransactionId;
use dtg_execution::{
    GatewayClusterRequest, GatewayExecution, GatewayExecutionError, GatewayExecutionTransport,
    GatewayFuture, GatewayResponse, GatewayRows, GatewayValue,
};
use dtg_gateway::{GatewayConfig, GatewayService};
use dtg_meta::MetaConfig;
use serde_json::Value;
use support::planning_context;

const FIXTURE_ROOT: &str = "../../../config/examples/clean-break-cluster";

#[test]
fn clean_break_fixtures_define_four_roles_and_two_heterogeneous_data_nodes() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join(FIXTURE_ROOT);
    let meta = read(&root.join("meta-1.json"));
    let controller = read(&root.join("controller-1.json"));
    let gateway = read(&root.join("gateway-1.json"));
    let data_1 = read(&root.join("data-1.json"));
    let data_2 = read(&root.join("data-2.json"));

    let cluster_ids = [&meta, &controller, &gateway, &data_1, &data_2]
        .map(|document| document["cluster_id"].as_u64().unwrap())
        .into_iter()
        .collect::<BTreeSet<_>>();
    assert_eq!(cluster_ids, BTreeSet::from([9001]));

    assert_eq!(providers(&data_1), approved_providers());
    assert_eq!(providers(&data_2), approved_providers());
    assert_ne!(data_1["node_id"], data_2["node_id"]);
    assert_ne!(data_1["rpc_addr"], data_2["rpc_addr"]);
    assert_ne!(data_1["fjall_root"], data_2["fjall_root"]);
    assert_ne!(data_1["consensus_root"], data_2["consensus_root"]);
    assert_eq!(data_1["shards"], serde_json::json!([1, 3]));
    assert_eq!(data_2["shards"], serde_json::json!([1, 2]));
    assert_eq!(
        gateway["cluster_endpoint"],
        Value::String(format!("http://{}", data_1["rpc_addr"].as_str().unwrap()))
    );

    MetaConfig::load(root.join("meta-1.json")).unwrap();
    ControllerConfig::load(root.join("controller-1.json")).unwrap();
    assert!(Path::new(env!("CARGO_BIN_EXE_dtgproxy-gateway")).is_file());

    let encoded = [meta, controller, gateway, data_1, data_2]
        .into_iter()
        .map(|document| document.to_string())
        .collect::<String>()
        .to_ascii_lowercase();
    let forbidden = [
        ["rocks", "db"].concat(),
        ["adapter", "-sidecar"].concat(),
        ["procedure", "-runtime"].concat(),
        ["cluster", ".v1"].concat(),
    ];
    for forbidden in forbidden {
        assert!(!encoded.contains(&forbidden));
    }
}

fn read(path: &Path) -> Value {
    serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
}

fn providers(document: &Value) -> BTreeSet<String> {
    document["provider_classes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|value| value.as_str().unwrap().to_owned())
        .collect()
}

fn approved_providers() -> BTreeSet<String> {
    ["fjall", "postgresql", "neo4j", "remote"]
        .into_iter()
        .map(str::to_owned)
        .collect()
}

#[test]
fn fixture_paths_remain_repository_relative_and_environment_neutral() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(FIXTURE_ROOT);
    for name in [
        "meta-1.json",
        "controller-1.json",
        "gateway-1.json",
        "data-1.json",
        "data-2.json",
    ] {
        let document = read(&root.join(name));
        for field in ["data_directory", "fjall_root", "consensus_root"] {
            if let Some(path) = document.get(field).and_then(Value::as_str) {
                assert!(!Path::new(path).is_absolute());
            }
        }
    }
}

struct CertifiedTransport;

impl GatewayExecutionTransport for CertifiedTransport {
    fn execute(
        &self,
        _request: GatewayClusterRequest,
    ) -> GatewayFuture<'_, Result<GatewayResponse, GatewayExecutionError>> {
        Box::pin(async {
            Ok(GatewayResponse::Rows(
                GatewayRows::new(vec!["n.id".into()], vec![vec![GatewayValue::Integer(7)]])
                    .unwrap(),
            ))
        })
    }
}

#[tokio::test]
async fn four_role_composition_uses_only_clean_break_process_contracts() {
    let root = tempfile::tempdir().unwrap();
    let meta = dtg_meta::MetaProcess::open(
        MetaConfig::for_test(root.path().join("meta"), 9001, 1).unwrap(),
    )
    .await
    .unwrap();
    let start = meta
        .timestamps()
        .allocate_start_time(TransactionId::new(1).unwrap())
        .await
        .unwrap();
    assert!(start.get() > 0);

    let controller = dtg_controller::ControllerProcess::open(
        ControllerConfig::for_test(root.path().join("controller"), 9001, 2).unwrap(),
        CatalogState::new(),
    )
    .await
    .unwrap();
    controller
        .record_observation(
            ObservedNodeState::new("data-11".into(), Version::new(0), Vec::new()).unwrap(),
        )
        .await
        .unwrap();
    assert!(controller.reconcile().await.unwrap().is_empty());

    let data = DataNodeBuilder::from_config(DataProcessConfig::new(
        root.path().join("data/business"),
        root.path().join("data/raft"),
    ))
    .start()
    .await
    .unwrap();
    assert_eq!(data.lifecycle(), LifecycleState::Ready);
    assert_eq!(data.rpc_service().protocol_major(), 2);

    let gateway = GatewayService::new(
        GatewayConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            9001,
            std::time::Duration::from_secs(5),
        )
        .unwrap(),
        GatewayExecution::for_process(Arc::new(CertifiedTransport), planning_context()),
    );
    let rows = gateway
        .bolt()
        .query("MATCH (n) FOR SYSTEM_TIME AS OF $t RETURN n.id ORDER BY n.id")
        .param("t", 1_i64)
        .run()
        .await
        .unwrap();
    assert_eq!(rows.rows(), &[vec![GatewayValue::Integer(7)]]);

    data.stop();
    assert_eq!(data.lifecycle(), LifecycleState::Stopped);
}
mod support;
