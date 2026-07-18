use std::collections::BTreeMap;
use std::future::Future;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use dtgproxy::config::NodeConfig;
use dtgproxy::control_plane::{
    BackendProfile, DeploymentMode, GraphDefinition, Placement, TopologyDefinition,
};
use dtgproxy::gateway::{
    ApiMutation, GATEWAY_API_VERSION, GatewayOperation, GatewayRequest, GatewayService,
    initialize_node,
};
use storage_api::AdapterRequirement;
use temporal_ir::GraphScope;
use temporal_storage::{GraphId, PartitionId};
use temporal_types::{CanonicalElement, GraphValue};

#[test]
fn shared_nothing_global_scan_fans_out_merges_canonically_and_applies_limit_last() {
    let temporary = tempfile::tempdir().unwrap();
    let config_path = temporary.path().join("node.json");
    let config = NodeConfig::new(
        temporary.path().join("data"),
        7,
        SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        40,
        32,
    )
    .unwrap();
    initialize_node(&config_path, &config, graph()).unwrap();
    let mut gateway = block_on(GatewayService::open(config)).unwrap();
    let mut partitions = BTreeMap::new();
    for partition in 0..10_000_u32 {
        let shard = gateway
            .runtime()
            .config()
            .route_scope(GraphScope::new(
                GraphId::new(7),
                PartitionId::new(partition),
            ))
            .shard_id();
        partitions.entry(shard).or_insert(partition);
        if partitions.len() == 2 {
            break;
        }
    }
    assert_eq!(partitions.len(), 2);
    let mut placements = partitions.into_iter().collect::<Vec<_>>();
    placements.sort_by_key(|(_, partition)| *partition);
    let payload = CanonicalElement::new(
        1,
        BTreeMap::from([(1, GraphValue::String("distributed".into()))]),
    );
    let transaction = GatewayRequest {
        version: GATEWAY_API_VERSION,
        request_id: "distributed-input".into(),
        operation: GatewayOperation::Transaction {
            schema_version: 1,
            ttl_micros: 10_000,
            mutations: placements
                .iter()
                .enumerate()
                .map(|(index, (_, partition))| ApiMutation::PutVertex {
                    partition: *partition,
                    vertex_id: (100 - index).to_string(),
                    label_id: 1,
                    valid_from_micros: 0,
                    valid_to_micros: None,
                    payload_dtp1: hex(&payload.encode().unwrap()),
                })
                .collect(),
        },
    };
    let committed = serde_json::to_value(block_on(gateway.execute_request(transaction))).unwrap();
    assert_eq!(committed["ok"], true, "{committed}");
    assert_eq!(
        committed["result"]["participants"]
            .as_array()
            .unwrap()
            .len(),
        2
    );

    let all = query(&mut gateway, 10);
    let records = all["result"]["records"].as_array().unwrap();
    assert_eq!(records.len(), 2, "{all}");
    assert_eq!(
        records[0]["partition"].as_str().unwrap(),
        placements[0].1.to_string()
    );
    assert_eq!(
        records[1]["partition"].as_str().unwrap(),
        placements[1].1.to_string()
    );

    let limited = query(&mut gateway, 1);
    let limited_records = limited["result"]["records"].as_array().unwrap();
    assert_eq!(limited_records.len(), 1);
    assert_eq!(limited_records[0], records[0]);
}

fn query(gateway: &mut GatewayService, limit: u32) -> serde_json::Value {
    serde_json::to_value(block_on(gateway.execute_request(GatewayRequest {
        version: GATEWAY_API_VERSION,
        request_id: format!("global-{limit}"),
        operation: GatewayOperation::Query {
            text: format!("SCAN VERTICES GRAPH 7 FOR VALID TIME 1 CURRENT LIMIT {limit}"),
        },
    })))
    .unwrap()
}

fn graph() -> GraphDefinition {
    GraphDefinition::new(
        7,
        "distributed-query",
        1,
        TopologyDefinition::new(
            DeploymentMode::SharedNothing,
            99,
            128,
            1,
            vec![
                Placement::new(10, 1, vec![10]).unwrap(),
                Placement::new(20, 1, vec![20]).unwrap(),
            ],
        )
        .unwrap(),
        BackendProfile::new(
            "rocksdb",
            BTreeMap::from([("path".into(), "backends/distributed-query".into())]),
            BTreeMap::new(),
            AdapterRequirement::HotPluggableReplica,
            1,
        )
        .unwrap(),
    )
    .unwrap()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

struct ThreadWake(std::thread::Thread);

impl Wake for ThreadWake {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::from(Arc::new(ThreadWake(std::thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => std::thread::park(),
        }
    }
}
