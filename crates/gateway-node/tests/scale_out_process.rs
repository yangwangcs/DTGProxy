use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions};
use std::io::Read;
use std::net::{SocketAddr, TcpListener};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bolt_protocol::{Value as BoltValue, encode as encode_bolt};
use cluster_protocol::CLUSTER_PROTOCOL_VERSION;
use cluster_protocol::proto::gateway_service_client::GatewayServiceClient;
use cluster_protocol::proto::meta_service_client::MetaServiceClient;
use cluster_protocol::proto::node_admin_service_client::NodeAdminServiceClient;
use cluster_protocol::proto::{
    EnsureReplicaRequest, GatewaySubmitRequest, ProposeRequest, ReplicaRole, RequestContext,
    ShardContext,
};
use control_plane::{
    BackendProfile, CatalogCommand, DeploymentMode, GraphDefinition, Placement, TopologyDefinition,
};
use data_node::encode_rocks_replica_profile;
use dtgproxy::DeploymentConfig;
use dtgproxy::gateway::{ApiMutation, GATEWAY_API_VERSION, GatewayOperation, GatewayRequest};
use serde_json::{Value, json};
use storage_api::AdapterRequirement;
use temporal_storage::PartitionId;
use temporal_types::{CanonicalElement, GraphValue};
use tonic::Request;

const CLUSTER_ID: [u8; 16] = [0x74; 16];
const CLUSTER_ID_HEX: &str = "74747474747474747474747474747474";
const GRAPH_ID: u64 = 7;
const GRAPH_NAME: &str = "scale_graph";
const DEFAULT_DATASET_ROWS: u32 = 4_096;
const QUERY: &str = "USE scale_graph FOR VALID_TIME AS OF 1000 MATCH (n) RETURN count(n) AS count";

fn harness() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../scripts/certify-scale-out.sh")
}

fn validate(report: &Path) -> std::process::Output {
    Command::new("bash")
        .arg(harness())
        .args(["--validate-report", report.to_str().unwrap()])
        .output()
        .unwrap()
}

fn validate_suite(report: &Path) -> std::process::Output {
    Command::new("bash")
        .arg(harness())
        .args(["--validate-suite", report.to_str().unwrap()])
        .output()
        .unwrap()
}

fn error_code(output: &std::process::Output) -> String {
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    report["error"]["code"].as_str().unwrap().to_owned()
}

fn fixture_report(node_count: u64) -> Value {
    let processes = (1..=node_count)
        .map(|node_id| {
            json!({
                "role": "data-node",
                "node_id": node_id,
                "pid": 40_000 + node_id,
                "listen_address": format!("127.0.0.1:{}", 7_000 + node_id),
                "data_directory": format!("/tmp/dtgproxy-scale-out/node-{node_id}"),
                "rss_bytes": 1_024
            })
        })
        .chain(std::iter::once(json!({
            "role": "gateway",
            "pid": 50_000,
            "listen_address": "127.0.0.1:7687",
            "rss_bytes": 1_024
        })))
        .collect::<Vec<_>>();
    json!({
        "schema_version": 1,
        "status": "passed",
        "topology": {
            "node_count": node_count,
            "processes": processes,
            "network_rx_bytes": 2_048,
            "network_tx_bytes": 4_096,
            "workload": {
                "dataset_seed": "dtgproxy-scale-out-v1",
                "query": QUERY,
                "duration_seconds": 1,
                "concurrency": 1,
                "result_digest": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
                "row_count": 1,
                "ttfr_ms": 1,
                "total_latency_ms": 2,
                "throughput_ops_per_second": 1
            }
        }
    })
}

#[test]
fn all_scale_topologies_use_the_same_shared_nothing_deployment_semantics() {
    for node_count in [1, 4, 8] {
        let graph = graph(node_count, DEFAULT_DATASET_ROWS);
        assert_eq!(graph.topology().mode(), DeploymentMode::SharedNothing);
        assert_eq!(graph.topology().placements().len(), 8);
    }
}

#[test]
fn scale_report_writes_are_immutable_and_count_identity_is_exact() {
    let temporary = tempfile::tempdir().unwrap();
    let report = temporary.path().join("report.json");
    write_report_new(&report, json!({"run": 1}));
    let overwrite = std::panic::catch_unwind(|| write_report_new(&report, json!({"run": 2})));

    assert!(overwrite.is_err(), "scale evidence must not be overwritten");
    assert_eq!(
        expected_count_digest(256),
        "d4bc6e359b4fb16377fa01e4b52c97e626e205e5fee5501357f0b436c799f566"
    );
}

#[test]
fn short_diagnostic_suite_does_not_claim_formal_scale_thresholds() {
    let temporary = tempfile::tempdir().unwrap();
    let suite_path = temporary.path().join("suite.json");
    let topologies = [1, 4, 8]
        .into_iter()
        .map(|nodes| fixture_report(nodes)["topology"].clone())
        .collect::<Vec<_>>();
    write_report_new(
        &suite_path,
        json!({"schema_version": 1, "status": "passed", "topologies": topologies}),
    );

    let output = validate_suite(&suite_path);

    assert!(
        output.status.success(),
        "short diagnostics validate authenticity and identity, not formal speedup: {}",
        String::from_utf8_lossy(&output.stdout)
    );
}

#[test]
fn rejects_duplicate_process_identity_listener_and_directory() {
    let temporary = tempfile::tempdir().unwrap();
    for (field, code) in [
        ("pid", "duplicate_data_node_pid"),
        ("listen_address", "duplicate_data_node_listener"),
        ("data_directory", "duplicate_data_node_data_directory"),
    ] {
        let path = temporary.path().join(format!("duplicate-{field}.json"));
        let mut report = fixture_report(4);
        report["topology"]["processes"][1][field] =
            report["topology"]["processes"][0][field].clone();
        std::fs::write(&path, serde_json::to_vec_pretty(&report).unwrap()).unwrap();
        let output = validate(&path);
        assert!(!output.status.success());
        assert_eq!(error_code(&output), code);
    }
}

#[test]
fn rejects_missing_result_latency_rss_and_network_observations() {
    let temporary = tempfile::tempdir().unwrap();
    for (pointer, code) in [
        ("result_digest", "missing_result_digest"),
        ("row_count", "missing_row_count"),
        ("ttfr_ms", "missing_ttfr_ms"),
    ] {
        let path = temporary.path().join(format!("missing-{pointer}.json"));
        let mut report = fixture_report(1);
        report["topology"]["workload"]
            .as_object_mut()
            .unwrap()
            .remove(pointer);
        std::fs::write(&path, serde_json::to_vec_pretty(&report).unwrap()).unwrap();
        let output = validate(&path);
        assert!(!output.status.success());
        assert_eq!(error_code(&output), code);
    }
    for (pointer, code) in [
        ("network_rx_bytes", "missing_network_rx_bytes"),
        ("network_tx_bytes", "missing_network_tx_bytes"),
    ] {
        let path = temporary.path().join(format!("missing-{pointer}.json"));
        let mut report = fixture_report(1);
        report["topology"].as_object_mut().unwrap().remove(pointer);
        std::fs::write(&path, serde_json::to_vec_pretty(&report).unwrap()).unwrap();
        let output = validate(&path);
        assert!(!output.status.success());
        assert_eq!(error_code(&output), code);
    }
    let path = temporary.path().join("missing-rss.json");
    let mut report = fixture_report(1);
    report["topology"]["processes"][0]
        .as_object_mut()
        .unwrap()
        .remove("rss_bytes");
    std::fs::write(&path, serde_json::to_vec_pretty(&report).unwrap()).unwrap();
    let output = validate(&path);
    assert!(!output.status.success());
    assert_eq!(error_code(&output), "missing_rss_bytes");
}

#[test]
fn persistent_driver_is_launched_once_per_topology() {
    if let Some(report) = std::env::var_os("DTGPROXY_LOADGEN_TEST_REPORT") {
        let workload = run_workload(
            &required_env("DTGPROXY_BOLT_LOADGEN_BIN"),
            "127.0.0.1:7687".parse().unwrap(),
            Duration::ZERO,
            Duration::from_secs(1),
            3,
            Duration::from_secs(60),
            Path::new(&report).parent().unwrap(),
            None,
        );
        std::fs::write(report, serde_json::to_vec(&workload).unwrap()).unwrap();
        return;
    }

    let temporary = tempfile::tempdir().unwrap();
    let driver = temporary.path().join("dtgproxy-bolt-loadgen");
    let launches = temporary.path().join("launches.log");
    let report = temporary.path().join("workload.json");
    let arguments = temporary.path().join("arguments.log");
    let driver_report = temporary.path().join("bolt-loadgen.json");
    std::fs::write(
        &driver,
        r##"#!/usr/bin/env bash
set -euo pipefail
printf 'launch\n' >> "$DTGPROXY_LOADGEN_LAUNCH_COUNTER"
printf '%s\n' "$@" > "$DTGPROXY_LOADGEN_ARGUMENTS"
output=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --output) output="$2"; shift 2 ;;
    *) shift ;;
  esac
done
if [[ -n "$output" ]]; then
  printf '%s\n' '{"schema_version":1,"timing_boundary":"RUN send to first decoded RECORD; connection setup excluded","connections":3,"warmup_ns":0,"measured_elapsed_ns":10000000,"completed_operations":12,"throughput_ops_per_second":1200,"connection_setup_samples_ns":[100,200,300],"ttfr_samples_ns":[1000000,2000000],"total_latency_samples_ns":[2000000,3000000],"result_digest":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","row_count":1,"error_count":0}' > "$output"
else
  printf '%s\n' '{"result_digest":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","row_count":1,"samples":[{"ttfr_ns":1000000,"total_latency_ns":2000000}]}'
fi
"##,
    )
    .unwrap();
    let mut permissions = std::fs::metadata(&driver).unwrap().permissions();
    permissions.set_mode(0o755);
    std::fs::set_permissions(&driver, permissions).unwrap();

    let child = Command::new(std::env::current_exe().unwrap())
        .args(["persistent_driver_is_launched_once_per_topology", "--exact"])
        .env("DTGPROXY_LOADGEN_TEST_REPORT", &report)
        .env("DTGPROXY_BOLT_PROBE_BIN", &driver)
        .env("DTGPROXY_BOLT_LOADGEN_BIN", &driver)
        .env("DTGPROXY_LOADGEN_LAUNCH_COUNTER", &launches)
        .env("DTGPROXY_LOADGEN_ARGUMENTS", &arguments)
        .output()
        .unwrap();
    assert!(
        child.status.success(),
        "loadgen fixture failed: {}",
        String::from_utf8_lossy(&child.stderr)
    );
    let workload: Value = serde_json::from_slice(&std::fs::read(report).unwrap()).unwrap();

    assert_eq!(
        std::fs::read_to_string(&launches).unwrap().lines().count(),
        1,
        "one persistent loadgen process is required per topology"
    );
    assert_eq!(workload["completed_operations"], 12);
    assert_eq!(workload["connections"], 3);
    assert_eq!(workload["ttfr_samples_ns"], json!([1_000_000, 2_000_000]));
    assert_eq!(
        workload["result_digest"],
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
    );
    assert_eq!(
        std::fs::read_to_string(arguments)
            .unwrap()
            .lines()
            .collect::<Vec<_>>(),
        [
            "--address",
            "127.0.0.1:7687",
            "--query",
            QUERY,
            "--connections",
            "3",
            "--warmup-seconds",
            "0",
            "--duration-seconds",
            "1",
            "--timeout-ms",
            "60000",
            "--output",
            driver_report.to_str().unwrap(),
        ]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_process_scale_out_certification() {
    if std::env::var_os("DTGPROXY_RUN_SCALE_OUT").is_none() {
        return;
    }
    let report_directory = PathBuf::from(required_env("DTGPROXY_SCALE_OUT_REPORT_DIR"));
    std::fs::create_dir_all(&report_directory).unwrap();
    write_report_new(
        &report_directory.join("run-claim.json"),
        json!({"pid": std::process::id(), "started_unix_ms": now_ms()}),
    );
    let duration = Duration::from_secs(parse_env("DTGPROXY_SCALE_OUT_DURATION_SECONDS", 1));
    let concurrency = parse_env("DTGPROXY_SCALE_OUT_CONCURRENCY", 1_usize);
    let warmup = Duration::from_secs(parse_env("DTGPROXY_SCALE_OUT_WARMUP_SECONDS", 0));
    let timeout = Duration::from_millis(parse_env("DTGPROXY_SCALE_OUT_TIMEOUT_MS", 60_000));
    let dataset_rows = parse_env("DTGPROXY_SCALE_OUT_DATASET_ROWS", DEFAULT_DATASET_ROWS);
    let enforce_thresholds = parse_env("DTGPROXY_SCALE_OUT_ENFORCE_THRESHOLDS", 1_u8) != 0;
    let mut topologies = Vec::new();
    for node_count in [1_u64, 4, 8] {
        let topology = run_topology(
            node_count,
            dataset_rows,
            warmup,
            duration,
            concurrency,
            timeout,
        )
        .await;
        write_report_new(
            &report_directory.join(format!("scale-out-{node_count}.json")),
            json!({
                "schema_version": 1,
                "status": "passed",
                "topology": topology
            }),
        );
        topologies.push(topology);
    }
    let suite = json!({"schema_version": 1, "status": "passed", "topologies": topologies});
    write_report_new(&report_directory.join("scale-out-summary.json"), suite);
    if enforce_thresholds {
        let gate = Command::new("bash")
            .arg(harness())
            .args([
                "--validate-suite",
                report_directory
                    .join("scale-out-summary.json")
                    .to_str()
                    .unwrap(),
            ])
            .output()
            .unwrap();
        assert!(
            gate.status.success(),
            "scale-out threshold gate failed: {}",
            String::from_utf8_lossy(&gate.stdout)
        );
    }
}

async fn run_topology(
    node_count: u64,
    dataset_rows: u32,
    warmup: Duration,
    duration: Duration,
    concurrency: usize,
    timeout: Duration,
) -> Value {
    let temporary = tempfile::tempdir().unwrap();
    let mut children = Children::default();
    let meta_address = reserve_address();
    let meta_raft_address = reserve_address();
    let meta_config = temporary.path().join("meta.json");
    write_json(
        &meta_config,
        json!({
            "version": 1,
            "cluster_id": CLUSTER_ID_HEX,
            "node_id": 1,
            "voters": [1],
            "listen_address": meta_address,
            "advertise_address": meta_address,
            "raft_listen_address": meta_raft_address,
            "peer_addresses": {},
            "data_directory": temporary.path().join("meta-data"),
            "security": {"mode": "loopback_plaintext"},
            "timestamp_reservation_size": 16,
            "maximum_future_drift_ms": 5000,
            "shutdown_grace_ms": 1000
        }),
    );
    children.spawn_ready(
        "meta",
        &required_env("DTGPROXY_META_BIN"),
        &meta_config,
        "DTGPROXY_META_READY",
        temporary.path(),
    );
    let mut meta = wait_meta(meta_address).await;

    let mut data_addresses = BTreeMap::new();
    let mut process_rows = Vec::new();
    for node_id in 1..=node_count {
        let address = reserve_address();
        let data_directory = temporary.path().join(format!("data-{node_id}"));
        let config = temporary.path().join(format!("data-{node_id}.json"));
        write_json(
            &config,
            json!({
                "version": 1,
                "cluster_id": CLUSTER_ID_HEX,
                "node_id": node_id,
                "listen_address": address,
                "advertise_address": address,
                "data_directory": data_directory,
                "meta_seeds": [meta_address],
                "security": {"mode": "loopback_plaintext"},
                "actor_queue_capacity": 256,
                "shutdown_grace_ms": 1000
            }),
        );
        let pid = children.spawn_ready(
            &format!("data-{node_id}"),
            &required_env("DTGPROXY_DATA_BIN"),
            &config,
            "DTGPROXY_DATA_READY",
            temporary.path(),
        );
        data_addresses.insert(node_id, address);
        process_rows.push(json!({
            "role": "data-node",
            "node_id": node_id,
            "pid": pid,
            "listen_address": address.to_string(),
            "data_directory": data_directory,
            "rss_bytes": rss_bytes(pid)
        }));
    }

    let graph = graph(node_count, dataset_rows);
    meta.propose(ProposeRequest {
        context: Some(request_context(100)),
        command: CatalogCommand::create_graph(100, 0, graph.clone())
            .encode()
            .unwrap(),
    })
    .await
    .unwrap();
    for placement in graph.topology().placements() {
        let shard_id = placement.shard_id();
        let node_id = placement.voters()[0];
        let mut admin = wait_data(data_addresses[&node_id]).await;
        admin
            .ensure_replica(EnsureReplicaRequest {
                context: Some(shard_context(200 + u128::from(shard_id), shard_id)),
                operation_id: (300_u128 + u128::from(shard_id)).to_be_bytes().to_vec(),
                local_node_id: node_id,
                initial_role: ReplicaRole::Leader.into(),
                schema_version: 1,
                backend_generation: 1,
                backend_profile: encode_rocks_replica_profile(
                    &[node_id],
                    &format!("scale-shard-{shard_id}"),
                )
                .unwrap(),
            })
            .await
            .unwrap();
    }

    let gateway_address = reserve_address();
    let bolt_address = reserve_address();
    let gateway_config = temporary.path().join("gateway.json");
    write_json(
        &gateway_config,
        json!({
            "version": 1,
            "cluster_id": CLUSTER_ID_HEX,
            "node_id": 31,
            "graph_id": GRAPH_ID,
            "listen_address": gateway_address,
            "bolt_listen_address": bolt_address,
            "advertise_address": gateway_address,
            "meta_seeds": [meta_address],
            "data_nodes": data_addresses,
            "security": {"mode": "loopback_plaintext"},
            "maximum_inflight": 256,
            "max_raft_ticks": 256,
            "catalog_watch_timeout_ms": 5000,
            "shutdown_grace_ms": 1000
        }),
    );
    let gateway_pid = children.spawn_ready(
        "gateway",
        &required_env("DTGPROXY_GATEWAY_BIN"),
        &gateway_config,
        "DTGPROXY_GATEWAY_READY",
        temporary.path(),
    );
    process_rows.push(json!({
        "role": "gateway",
        "pid": gateway_pid,
        "listen_address": bolt_address.to_string(),
        "rss_bytes": rss_bytes(gateway_pid)
    }));

    let mut gateway = wait_gateway(gateway_address).await;
    seed_all_placements(&mut gateway, &graph, dataset_rows).await;
    let network_before = network_bytes();
    let workload = run_workload(
        &required_env("DTGPROXY_BOLT_LOADGEN_BIN"),
        bolt_address,
        warmup,
        duration,
        concurrency,
        timeout,
        temporary.path(),
        Some(dataset_rows),
    );
    let network_after = network_bytes();
    for process in &mut process_rows {
        let pid = process["pid"].as_u64().unwrap();
        process["rss_bytes"] = json!(rss_bytes(u32::try_from(pid).unwrap()));
    }
    drop(gateway);
    drop(meta);
    drop(children);

    json!({
        "node_count": node_count,
        "dataset_rows": dataset_rows,
        "processes": process_rows,
        "network_rx_bytes": network_after.0.checked_sub(network_before.0).unwrap(),
        "network_tx_bytes": network_after.1.checked_sub(network_before.1).unwrap(),
        "workload": workload
    })
}

fn graph(node_count: u64, dataset_rows: u32) -> GraphDefinition {
    let placements = (0..8_u64)
        .map(|shard_index| {
            let node_id = shard_index % node_count + 1;
            Placement::new(
                u32::try_from(1_001 + shard_index).unwrap(),
                1,
                vec![node_id],
            )
            .unwrap()
        })
        .collect();
    GraphDefinition::new(
        GRAPH_ID,
        GRAPH_NAME,
        1,
        TopologyDefinition::new(
            DeploymentMode::SharedNothing,
            99,
            dataset_rows,
            1,
            placements,
        )
        .unwrap(),
        BackendProfile::new(
            "rocksdb",
            BTreeMap::new(),
            BTreeMap::new(),
            AdapterRequirement::HotPluggableReplica,
            1,
        )
        .unwrap(),
    )
    .unwrap()
}

async fn seed_all_placements(
    gateway: &mut GatewayServiceClient<tonic::transport::Channel>,
    graph: &GraphDefinition,
    dataset_rows: u32,
) {
    let deployment = DeploymentConfig::from_catalog(graph).unwrap();
    let owners = (0..dataset_rows)
        .map(|partition| {
            deployment
                .route_scope(temporal_ir::GraphScope::new(
                    temporal_storage::GraphId::new(GRAPH_ID),
                    PartitionId::new(partition),
                ))
                .shard_id()
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(owners.len(), graph.topology().placements().len());
    let payload = CanonicalElement::new(1, BTreeMap::<u32, GraphValue>::new())
        .encode()
        .unwrap();
    let payload = payload
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    for (batch_index, start) in (0..dataset_rows).step_by(512).enumerate() {
        let end = start.saturating_add(512).min(dataset_rows);
        let request = GatewayRequest {
            version: GATEWAY_API_VERSION,
            request_id: format!("scale-out-seed-v1-{batch_index}"),
            operation: GatewayOperation::Transaction {
                schema_version: 1,
                ttl_micros: 60_000_000,
                mutations: (start..end)
                    .map(|partition| ApiMutation::PutVertex {
                        partition,
                        vertex_id: (10_000_u64 + u64::from(partition)).to_string(),
                        label_id: 1,
                        valid_from_micros: 0,
                        valid_to_micros: None,
                        payload_dtp1: payload.clone(),
                    })
                    .collect(),
            },
        };
        let response = gateway
            .submit(Request::new(GatewaySubmitRequest {
                context: Some(request_context(500 + batch_index as u128)),
                request_json: serde_json::to_vec(&request).unwrap(),
            }))
            .await
            .unwrap()
            .into_inner();
        let response: Value = serde_json::from_slice(&response.response_json).unwrap();
        assert_eq!(response["ok"], true, "{response}");
    }
}

fn run_workload(
    driver: &str,
    address: SocketAddr,
    warmup: Duration,
    duration: Duration,
    concurrency: usize,
    timeout: Duration,
    report_directory: &Path,
    expected_count: Option<u32>,
) -> Value {
    let report_path = report_directory.join("bolt-loadgen.json");
    let address = address.to_string();
    let output = Command::new(driver)
        .args([
            "--address",
            &address,
            "--query",
            QUERY,
            "--connections",
            &concurrency.to_string(),
            "--warmup-seconds",
            &warmup.as_secs().to_string(),
            "--duration-seconds",
            &duration.as_secs().to_string(),
            "--timeout-ms",
            &timeout.as_millis().to_string(),
            "--output",
            report_path.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "Bolt loadgen failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let mut report: Value = serde_json::from_slice(&std::fs::read(&report_path).unwrap()).unwrap();
    let ttfr_ms = mean_sample_ms(&report, "ttfr_samples_ns");
    let total_latency_ms = mean_sample_ms(&report, "total_latency_samples_ns");
    let measured_elapsed_ns = report["measured_elapsed_ns"].as_u64().unwrap();
    assert_eq!(report["error_count"], 0);
    assert_eq!(report["row_count"], 1);
    if let Some(expected_count) = expected_count {
        assert_eq!(
            report["result_digest"],
            expected_count_digest(expected_count),
            "count result must equal the seeded dataset cardinality"
        );
    }
    let workload = report.as_object_mut().unwrap();
    workload.insert("dataset_seed".into(), json!("dtgproxy-scale-out-v1"));
    workload.insert("query".into(), json!(QUERY));
    workload.insert(
        "duration_seconds".into(),
        json!(measured_elapsed_ns as f64 / 1_000_000_000.0),
    );
    workload.insert("concurrency".into(), json!(concurrency));
    workload.insert("ttfr_ms".into(), json!(ttfr_ms));
    workload.insert("total_latency_ms".into(), json!(total_latency_ms));
    report
}

fn mean_sample_ms(report: &Value, name: &str) -> f64 {
    let samples = report[name].as_array().unwrap();
    assert!(!samples.is_empty(), "loadgen report is missing {name}");
    samples
        .iter()
        .map(|sample| sample.as_f64().unwrap())
        .sum::<f64>()
        / samples.len() as f64
        / 1_000_000.0
}

fn reserve_address() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}

fn write_json(path: &Path, value: Value) {
    std::fs::write(path, serde_json::to_vec_pretty(&value).unwrap()).unwrap();
}

fn write_report_new(path: &Path, value: Value) {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .unwrap();
    serde_json::to_writer_pretty(&mut file, &value).unwrap();
    use std::io::Write as _;
    file.write_all(b"\n").unwrap();
    file.sync_all().unwrap();
}

fn expected_count_digest(count: u32) -> String {
    let record = BoltValue::Structure {
        signature: 0x71,
        fields: vec![BoltValue::List(vec![BoltValue::Integer(i64::from(count))])],
    };
    let canonical = encode_bolt(&record).unwrap();
    let mut digest = blake3::Hasher::new();
    digest.update(&(canonical.len() as u64).to_be_bytes());
    digest.update(&canonical);
    digest.finalize().to_hex().to_string()
}

async fn wait_meta(address: SocketAddr) -> MetaServiceClient<tonic::transport::Channel> {
    for _ in 0..200 {
        if let Ok(client) = MetaServiceClient::connect(format!("http://{address}")).await {
            return client;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("Meta process did not become ready at {address}");
}

async fn wait_data(address: SocketAddr) -> NodeAdminServiceClient<tonic::transport::Channel> {
    for _ in 0..200 {
        if let Ok(client) = NodeAdminServiceClient::connect(format!("http://{address}")).await {
            return client;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("Data process did not become ready at {address}");
}

async fn wait_gateway(address: SocketAddr) -> GatewayServiceClient<tonic::transport::Channel> {
    for _ in 0..200 {
        if let Ok(client) = GatewayServiceClient::connect(format!("http://{address}")).await {
            return client;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("Gateway process did not become ready at {address}");
}

fn request_context(request_id: u128) -> RequestContext {
    RequestContext {
        protocol_version: CLUSTER_PROTOCOL_VERSION,
        cluster_id: CLUSTER_ID.to_vec(),
        request_id: request_id.to_be_bytes().to_vec(),
        deadline_unix_ms: now_ms() + 60_000,
    }
}

fn shard_context(request_id: u128, shard_id: u32) -> ShardContext {
    ShardContext {
        request: Some(request_context(request_id)),
        graph_id: GRAPH_ID,
        shard_id,
        placement_epoch: 1,
    }
}

fn now_ms() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
}

fn rss_bytes(pid: u32) -> u64 {
    let output = Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()])
        .output()
        .unwrap();
    assert!(output.status.success());
    String::from_utf8(output.stdout)
        .unwrap()
        .trim()
        .parse::<u64>()
        .unwrap()
        * 1_024
}

fn network_bytes() -> (u64, u64) {
    let linux_rx = Path::new("/sys/class/net/lo/statistics/rx_bytes");
    if linux_rx.is_file() {
        let read = |name: &str| {
            std::fs::read_to_string(format!("/sys/class/net/lo/statistics/{name}"))
                .unwrap()
                .trim()
                .parse()
                .unwrap()
        };
        return (read("rx_bytes"), read("tx_bytes"));
    }
    let output = Command::new("netstat")
        .args(["-ibn", "-I", "lo0"])
        .output()
        .unwrap();
    assert!(output.status.success());
    let text = String::from_utf8(output.stdout).unwrap();
    let mut lines = text.lines();
    let header = lines
        .find(|line| line.contains("Ibytes") && line.contains("Obytes"))
        .unwrap();
    let columns = header.split_whitespace().collect::<Vec<_>>();
    let rx = columns
        .iter()
        .position(|column| *column == "Ibytes")
        .unwrap();
    let tx = columns
        .iter()
        .position(|column| *column == "Obytes")
        .unwrap();
    let values = lines
        .find(|line| line.starts_with("lo0 "))
        .unwrap()
        .split_whitespace()
        .collect::<Vec<_>>();
    (values[rx].parse().unwrap(), values[tx].parse().unwrap())
}

fn required_env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("missing required environment variable {name}"))
}

fn parse_env<T>(name: &str, default: T) -> T
where
    T: std::str::FromStr,
    T::Err: std::fmt::Debug,
{
    std::env::var(name).map_or(default, |value| value.parse().unwrap())
}

#[derive(Default)]
struct Children(Vec<Child>);

impl Children {
    fn spawn_ready(
        &mut self,
        name: &str,
        binary: &str,
        config: &Path,
        marker: &str,
        root: &Path,
    ) -> u32 {
        let stdout_path = root.join(format!("{name}.stdout.log"));
        let stderr_path = root.join(format!("{name}.stderr.log"));
        let child = Command::new(binary)
            .args(["--config", config.to_str().unwrap()])
            .stdin(Stdio::null())
            .stdout(Stdio::from(File::create(&stdout_path).unwrap()))
            .stderr(Stdio::from(File::create(&stderr_path).unwrap()))
            .spawn()
            .unwrap();
        let pid = child.id();
        self.0.push(child);
        let deadline = Instant::now() + Duration::from_secs(15);
        while Instant::now() < deadline {
            let mut text = String::new();
            OpenOptions::new()
                .read(true)
                .open(&stdout_path)
                .unwrap()
                .read_to_string(&mut text)
                .unwrap();
            if text.lines().any(|line| line.starts_with(marker)) {
                assert!(self.0.last_mut().unwrap().try_wait().unwrap().is_none());
                return pid;
            }
            if let Some(status) = self.0.last_mut().unwrap().try_wait().unwrap() {
                panic!(
                    "{name} exited before READY ({status}): {}",
                    std::fs::read_to_string(&stderr_path).unwrap()
                );
            }
            thread::sleep(Duration::from_millis(25));
        }
        panic!("{name} did not emit {marker}");
    }
}

impl Drop for Children {
    fn drop(&mut self) {
        for child in self.0.iter_mut().rev() {
            if child.try_wait().ok().flatten().is_none() {
                let _ = child.kill();
            }
            let _ = child.wait();
        }
    }
}
