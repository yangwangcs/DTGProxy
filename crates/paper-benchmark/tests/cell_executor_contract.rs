use std::collections::BTreeMap;
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::thread;

use adapter_neo4j::Neo4jAdapterFactory;
use adapter_postgres::PostgresAdapter;
use adapter_registry::{AdapterOpenRequest, AdapterRegistry, SecretString};
use adapter_rocksdb::RocksAdapter;
use bolt_protocol::{Value as BoltValue, encode as encode_bolt};
use paper_benchmark::{
    Backend, CellConfig, CellKey, ExperimentPath, ExperimentProtocol, MatrixCell, RawObservation,
    ResultIdentity, SCHEMA_VERSION,
};
use serde_json::{Value, json};
use storage_api::{AdapterRequirement, CommittedMutationBatch, Mutation, StorageAdapter};
use temporal_storage::{
    ElementId, ElementRef, GraphId, PartitionId, ProjectionRecord, ValidSegment, current_vertex_key,
};
use temporal_types::{CanonicalElement, Interval, TransactionTime, ValidTime};

const QUERY: &str = "USE scale_graph FOR VALID_TIME AS OF 1000 MATCH (n) RETURN count(n) AS count";

#[test]
fn rocksdb_backend_and_adapter_direct_return_the_same_exact_identity() {
    let root = tempfile::tempdir().unwrap();
    let database = root.path().join("rocks-snapshot");
    seed_rocks(&database, 2);
    let runtime = write_runtime(
        root.path(),
        json!({
            "schema_version": 1,
            "dataset_digest": digest('1'),
            "snapshot": "as_of:1000",
            "workloads": [{
                "workload_digest": digest('2'),
                "kind": "count_current_vertices",
                "graph_id": 7
            }],
            "backends": {
                "rocksdb": {"status": "available", "snapshot_paths": [database]},
                "postgresql": {"status": "unavailable", "reason": "fixture has no PostgreSQL"},
                "neo4j": {"status": "unavailable", "reason": "fixture has no Neo4j"}
            },
            "proxy_targets": []
        }),
    );

    let backend = run_cell(
        root.path(),
        &runtime,
        cell(Backend::Rocksdb, ExperimentPath::BackendDirect),
        "backend.json",
    );
    let adapter = run_cell(
        root.path(),
        &runtime,
        cell(Backend::Rocksdb, ExperimentPath::AdapterDirect),
        "adapter.json",
    );

    backend.validate().unwrap();
    adapter.validate().unwrap();
    assert_eq!(backend.identity, adapter.identity);
    assert_eq!(backend.identity, expected_count_identity(2));
    assert_eq!(backend.identity.row_count, 1);
    assert_eq!(
        backend.operations as usize,
        backend.samples.latency_ns.len()
    );
    assert_eq!(
        adapter.operations as usize,
        adapter.samples.latency_ns.len()
    );
    assert!(observed_resources(&backend));
    assert!(observed_resources(&adapter));
}

#[test]
fn live_postgresql_backend_and_adapter_direct_return_the_same_exact_identity() {
    let Ok(url) = std::env::var("DTGPROXY_PAPER_TEST_POSTGRES_URL") else {
        return;
    };
    let root = tempfile::tempdir().unwrap();
    let instance_id = unique_instance("paper-postgres");
    let adapter = PostgresAdapter::open(&url, &instance_id, 4).unwrap();
    block_on(adapter.apply_committed(vertex_batch(2))).unwrap();
    drop(adapter);
    let runtime = write_runtime(
        root.path(),
        json!({
            "schema_version": 1,
            "dataset_digest": digest('1'),
            "snapshot": "as_of:1000",
            "workloads": [{
                "workload_digest": digest('2'),
                "kind": "count_current_vertices",
                "graph_id": 7
            }],
            "backends": {
                "rocksdb": {"status": "unavailable", "reason": "unused"},
                "postgresql": {
                    "status": "available",
                    "connection_string": url,
                    "instance_id": instance_id,
                    "pool_size": 4
                },
                "neo4j": {"status": "unavailable", "reason": "unused"}
            },
            "proxy_targets": []
        }),
    );

    let backend = run_cell(
        root.path(),
        &runtime,
        cell(Backend::Postgresql, ExperimentPath::BackendDirect),
        "postgres-backend.json",
    );
    let adapter = run_cell(
        root.path(),
        &runtime,
        cell(Backend::Postgresql, ExperimentPath::AdapterDirect),
        "postgres-adapter.json",
    );

    assert_eq!(backend.identity, adapter.identity);
    assert_eq!(backend.identity, expected_count_identity(2));
    assert_eq!(backend.identity.row_count, 1);
}

#[test]
fn live_neo4j_backend_and_adapter_direct_return_the_same_exact_identity() {
    let Ok(endpoint) = std::env::var("DTGPROXY_PAPER_TEST_NEO4J_ENDPOINT") else {
        return;
    };
    let password = std::env::var("DTGPROXY_PAPER_TEST_NEO4J_PASSWORD").unwrap();
    let username = "neo4j";
    let database = "neo4j";
    let root = tempfile::tempdir().unwrap();
    let instance_id = unique_instance("paper-neo4j");
    let mut registry = AdapterRegistry::new();
    registry
        .register(std::sync::Arc::new(Neo4jAdapterFactory))
        .unwrap();
    let request = AdapterOpenRequest::new(&instance_id)
        .with_parameter("endpoint", &endpoint)
        .with_parameter("database", database)
        .with_parameter("username", username)
        .with_secret("password", SecretString::new(&password));
    let opened =
        block_on(registry.open("neo4j", &request, AdapterRequirement::HotPluggableReplica))
            .unwrap();
    block_on(opened.adapter().apply_committed(vertex_batch(2))).unwrap();
    drop(opened);
    let runtime = write_runtime(
        root.path(),
        json!({
            "schema_version": 1,
            "dataset_digest": digest('1'),
            "snapshot": "as_of:1000",
            "workloads": [{
                "workload_digest": digest('2'),
                "kind": "count_current_vertices",
                "graph_id": 7
            }],
            "backends": {
                "rocksdb": {"status": "unavailable", "reason": "unused"},
                "postgresql": {"status": "unavailable", "reason": "unused"},
                "neo4j": {
                    "status": "available",
                    "endpoint": endpoint,
                    "database": database,
                    "username": username,
                    "password": password,
                    "instance_id": instance_id,
                    "timeout_seconds": 30
                }
            },
            "proxy_targets": []
        }),
    );

    let backend = run_cell(
        root.path(),
        &runtime,
        cell(Backend::Neo4j, ExperimentPath::BackendDirect),
        "neo4j-backend.json",
    );
    let adapter = run_cell(
        root.path(),
        &runtime,
        cell(Backend::Neo4j, ExperimentPath::AdapterDirect),
        "neo4j-adapter.json",
    );

    assert_eq!(backend.identity, adapter.identity);
    assert_eq!(backend.identity.row_count, 1);
}

#[test]
fn unavailable_backends_fail_explicitly_without_creating_an_observation() {
    let root = tempfile::tempdir().unwrap();
    let runtime = write_runtime(
        root.path(),
        json!({
            "schema_version": 1,
            "dataset_digest": digest('1'),
            "snapshot": "as_of:1000",
            "workloads": [{
                "workload_digest": digest('2'),
                "kind": "count_current_vertices",
                "graph_id": 7
            }],
            "backends": {
                "rocksdb": {"status": "unavailable", "reason": "fixture has no RocksDB"},
                "postgresql": {"status": "unavailable", "reason": "PostgreSQL service is offline"},
                "neo4j": {"status": "unavailable", "reason": "Neo4j service is offline"}
            },
            "proxy_targets": []
        }),
    );

    for backend in [Backend::Postgresql, Backend::Neo4j] {
        let config = root.path().join(format!("{backend}-cell.json"));
        fs::write(
            &config,
            serde_json::to_vec_pretty(&cell(backend, ExperimentPath::BackendDirect)).unwrap(),
        )
        .unwrap();
        let output = root.path().join(format!("{backend}-output.json"));
        let result = executor_command(&runtime, &config, &output)
            .output()
            .unwrap();

        assert_eq!(result.status.code(), Some(3));
        assert!(!output.exists());
        let stderr = String::from_utf8_lossy(&result.stderr);
        assert!(stderr.contains("unavailable"), "{stderr}");
        assert!(stderr.contains(&backend.to_string()), "{stderr}");
    }
}

#[test]
fn postgresql_runtime_binding_reaches_the_native_connection_layer() {
    let root = tempfile::tempdir().unwrap();
    let runtime = write_runtime(
        root.path(),
        json!({
            "schema_version": 1,
            "dataset_digest": digest('1'),
            "snapshot": "as_of:1000",
            "workloads": [{
                "workload_digest": digest('2'),
                "kind": "count_current_vertices",
                "graph_id": 7
            }],
            "backends": {
                "rocksdb": {"status": "unavailable", "reason": "unused"},
                "postgresql": {
                    "status": "available",
                    "connection_string": "host=127.0.0.1 port=1 user=none dbname=none connect_timeout=1",
                    "instance_id": "paper-postgres-unreachable",
                    "pool_size": 1
                },
                "neo4j": {"status": "unavailable", "reason": "unused"}
            },
            "proxy_targets": []
        }),
    );
    let config = root.path().join("postgres-cell.json");
    fs::write(
        &config,
        serde_json::to_vec_pretty(&cell(Backend::Postgresql, ExperimentPath::BackendDirect))
            .unwrap(),
    )
    .unwrap();
    let output = root.path().join("postgres-output.json");

    let result = executor_command(&runtime, &config, &output)
        .output()
        .unwrap();

    assert!(!result.status.success());
    assert!(!output.exists());
    let stderr = String::from_utf8_lossy(&result.stderr);
    assert!(stderr.contains("native PostgreSQL"), "{stderr}");
    assert!(!stderr.contains("invalid runtime manifest"), "{stderr}");
}

#[test]
fn neo4j_runtime_binding_reaches_the_native_query_layer() {
    let root = tempfile::tempdir().unwrap();
    let runtime = write_runtime(
        root.path(),
        json!({
            "schema_version": 1,
            "dataset_digest": digest('1'),
            "snapshot": "as_of:1000",
            "workloads": [{
                "workload_digest": digest('2'),
                "kind": "count_current_vertices",
                "graph_id": 7
            }],
            "backends": {
                "rocksdb": {"status": "unavailable", "reason": "unused"},
                "postgresql": {"status": "unavailable", "reason": "unused"},
                "neo4j": {
                    "status": "available",
                    "endpoint": "http://127.0.0.1:1",
                    "database": "neo4j",
                    "username": "neo4j",
                    "password": "unused",
                    "instance_id": "paper-neo4j-unreachable",
                    "timeout_seconds": 1
                }
            },
            "proxy_targets": []
        }),
    );
    let config = root.path().join("neo4j-cell.json");
    fs::write(
        &config,
        serde_json::to_vec_pretty(&cell(Backend::Neo4j, ExperimentPath::BackendDirect)).unwrap(),
    )
    .unwrap();
    let output = root.path().join("neo4j-output.json");

    let result = executor_command(&runtime, &config, &output)
        .output()
        .unwrap();

    assert!(!result.status.success());
    assert!(!output.exists());
    let stderr = String::from_utf8_lossy(&result.stderr);
    assert!(stderr.contains("native Neo4j"), "{stderr}");
    assert!(!stderr.contains("invalid runtime manifest"), "{stderr}");
}

#[test]
fn proxy_invokes_one_persistent_loadgen_and_records_live_independent_processes() {
    let root = tempfile::tempdir().unwrap();
    let invocation_log = root.path().join("loadgen-invocations.log");
    let loadgen = fake_loadgen(root.path());
    let mut data = spawn_sleep();
    let mut gateway = spawn_sleep();
    let data_pid = data.id();
    let gateway_pid = gateway.id();
    let control_socket = root.path().join("gateway-control.sock");
    let gateway_control = fake_gateway(&control_socket, gateway_pid);
    let data_directory = root.path().join("data-1");
    fs::create_dir(&data_directory).unwrap();
    let runtime = write_runtime(
        root.path(),
        proxy_runtime(
            &loadgen,
            data_pid,
            gateway_pid,
            &data_directory,
            &control_socket,
        ),
    );
    let config = root.path().join("proxy-cell.json");
    fs::write(
        &config,
        serde_json::to_vec_pretty(&cell(Backend::Rocksdb, ExperimentPath::Proxy)).unwrap(),
    )
    .unwrap();
    let output = root.path().join("proxy-output.json");

    let result = executor_command(&runtime, &config, &output)
        .env("FAKE_LOADGEN_INVOCATIONS", &invocation_log)
        .output()
        .unwrap();
    stop(&mut data);
    stop(&mut gateway);
    let requests = gateway_control.join().unwrap();

    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let observation: RawObservation = serde_json::from_slice(&fs::read(output).unwrap()).unwrap();
    observation.validate().unwrap();
    assert_eq!(observation.path, ExperimentPath::Proxy);
    assert_eq!(observation.identity.digest, digest('d'));
    assert_eq!(observation.identity.row_count, 1);
    assert!(observed_resources(&observation));
    let invocations = fs::read_to_string(invocation_log).unwrap();
    assert_eq!(invocations.lines().count(), 1, "{invocations}");
    assert!(invocations.contains("--connections 1"), "{invocations}");
    assert!(invocations.contains("--warmup-seconds 1"), "{invocations}");
    assert!(
        invocations.contains("--benchmark-session session_1"),
        "{invocations}"
    );
    assert!(
        invocations.contains("--duration-seconds 1"),
        "{invocations}"
    );
    let audit: Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(audit["event"], "paper_proxy_topology_verified");
    assert_eq!(audit["data_node_pids"], json!([data_pid]));
    assert_eq!(requests[0]["operation"], "begin_cell");
    assert_eq!(requests[0]["cell_id"], "executor-contract-test:1");
    assert_eq!(requests[0]["config"]["native_pushdown"], true);
    assert_eq!(requests[1]["operation"], "finish_cell");
    assert_eq!(requests[1]["session_token"], "session_1");
}

#[test]
fn probe_process_emits_one_strict_json_line_for_the_claimed_process() {
    let mut process = spawn_sleep();
    let output = Command::new(executor_binary())
        .args([
            "probe-process",
            "--pid",
            &process.id().to_string(),
            "--executable",
            "/bin/sleep",
            "--network-interface",
            loopback_interface(),
        ])
        .output()
        .unwrap();
    stop(&mut process);

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert_eq!(stdout.lines().count(), 1, "{stdout:?}");
    assert!(stdout.ends_with('\n'), "{stdout:?}");
    let probe: Value = serde_json::from_str(stdout.trim_end()).unwrap();
    let keys = probe
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect::<Vec<_>>();
    assert_eq!(
        keys,
        [
            "boot_id",
            "cpu_time_ns",
            "data_directory",
            "executable",
            "executable_sha256",
            "host_id",
            "listen_address",
            "network_interface",
            "network_rx_bytes",
            "network_tx_bytes",
            "peak_rss_bytes",
            "pid",
            "probe_binary_sha256",
            "process_start_id",
            "rss_bytes",
            "sampled_unix_ns",
            "schema_version",
        ]
    );
    assert_eq!(probe["schema_version"], SCHEMA_VERSION);
    assert_eq!(probe["pid"], process.id());
    assert_eq!(probe["executable"], "/bin/sleep");
    assert_eq!(probe["network_interface"], loopback_interface());
    assert_eq!(probe["executable_sha256"].as_str().unwrap().len(), 64);
    assert_eq!(probe["probe_binary_sha256"].as_str().unwrap().len(), 64);
    assert!(!probe["process_start_id"].as_str().unwrap().is_empty());
    assert!(!probe["host_id"].as_str().unwrap().is_empty());
    assert!(!probe["boot_id"].as_str().unwrap().is_empty());
}

#[test]
fn probe_resources_omits_hashing_and_topology_fields() {
    let mut process = spawn_sleep();
    let output = Command::new(executor_binary())
        .args([
            "probe-resources",
            "--pid",
            &process.id().to_string(),
            "--network-interface",
            loopback_interface(),
        ])
        .output()
        .unwrap();
    stop(&mut process);

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let snapshot: Value = serde_json::from_slice(&output.stdout).unwrap();
    let keys = snapshot
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect::<Vec<_>>();
    assert_eq!(
        keys,
        [
            "cpu_time_ns",
            "network_interface",
            "network_rx_bytes",
            "network_tx_bytes",
            "pid",
            "process_start_id",
            "rss_bytes",
            "sampled_unix_ns",
            "schema_version",
        ]
    );
    assert_eq!(snapshot["pid"], process.id());
    assert_eq!(snapshot["network_interface"], loopback_interface());
}

#[test]
fn remote_formal_uses_sealed_ssh_probes_outside_loadgen_and_aggregates_data_interface() {
    let root = tempfile::tempdir().unwrap();
    let event_log = root.path().join("events.log");
    let ssh_log = root.path().join("ssh.log");
    let ssh_state = root.path().join("ssh.state");
    let fake_bin = root.path().join("fake-bin");
    fs::create_dir(&fake_bin).unwrap();
    fake_ssh(&fake_bin);
    let loadgen = fake_loadgen(root.path());
    let mut gateway = spawn_sleep();
    let control_socket = root.path().join("gateway-control.sock");
    let gateway_control = fake_gateway(&control_socket, gateway.id());
    let runtime = write_runtime(
        root.path(),
        remote_proxy_runtime(&loadgen, gateway.id(), &control_socket),
    );
    let config = root.path().join("proxy-cell.json");
    fs::write(
        &config,
        serde_json::to_vec_pretty(&cell(Backend::Rocksdb, ExperimentPath::Proxy)).unwrap(),
    )
    .unwrap();
    let output = root.path().join("proxy-output.json");
    let path = format!("{}:{}", fake_bin.display(), std::env::var("PATH").unwrap());

    let result = executor_command(&runtime, &config, &output)
        .env("PATH", path)
        .env("FAKE_EVENT_LOG", &event_log)
        .env("FAKE_LOADGEN_INVOCATIONS", root.path().join("loadgen.log"))
        .env("FAKE_SSH_LOG", &ssh_log)
        .env("FAKE_SSH_STATE", &ssh_state)
        .env("FAKE_DYNAMIC_TIMING", "1")
        .env(
            "FAKE_SSH_BEFORE",
            remote_probe_json(90, 100, 1_000, 2_000, 4_096),
        )
        .env(
            "FAKE_SSH_AFTER",
            remote_probe_json(110, 160, 1_090, 2_140, 8_192),
        )
        .env(
            "FAKE_SSH_RESOURCE",
            remote_resource_probe_json(100, 130, 16_384, 1_050, 2_080),
        )
        .output()
        .unwrap();
    stop(&mut gateway);
    let requests = gateway_control.join().unwrap();

    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert_eq!(requests.len(), 2);
    let events = fs::read_to_string(&event_log).unwrap();
    let events = events.lines().collect::<Vec<_>>();
    assert_eq!(events.first(), Some(&"probe-full"));
    assert_eq!(events.last(), Some(&"probe-full"));
    assert!(events.contains(&"loadgen"), "{events:?}");
    assert!(events.contains(&"probe-resource"), "{events:?}");
    let ssh = fs::read_to_string(&ssh_log).unwrap();
    assert!(ssh.contains("-o BatchMode=yes"), "{ssh}");
    assert!(ssh.contains("-o ConnectionAttempts=1"), "{ssh}");
    assert!(ssh.contains("-o ClearAllForwardings=yes"), "{ssh}");
    assert!(ssh.contains("-o RequestTTY=no"), "{ssh}");
    assert!(ssh.contains("probe-process"), "{ssh}");
    assert!(ssh.contains("--listen-address 10.0.0.10:7101"), "{ssh}");
    assert!(
        ssh.contains("--data-directory /var/lib/dtg/node-1"),
        "{ssh}"
    );
    let observation: RawObservation = serde_json::from_slice(&fs::read(&output).unwrap()).unwrap();
    let resources = serde_json::to_value(observation.resources).unwrap();
    let rx = resources["network_rx_bytes"]["value"].as_u64().unwrap();
    let tx = resources["network_tx_bytes"]["value"].as_u64().unwrap();
    let cpu = resources["cpu_time_ns"]["value"].as_u64().unwrap();
    assert!((1..=90).contains(&rx), "{resources}");
    assert!((1..=140).contains(&tx), "{resources}");
    assert!((1..=60).contains(&cpu), "{resources}");
    assert_eq!(resources["peak_rss_bytes"]["value"], 16_384);
    let audit: Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(audit["deployment_mode"], "remote_formal");
    assert_eq!(
        audit["data_node_processes"][0]["probe"]["host_id"],
        "host-a"
    );
    assert_eq!(
        audit["data_node_processes"][0]["probe"]["probe_binary_sha256"],
        digest('b')
    );
}

#[test]
fn proxy_target_requires_an_explicit_deployment_mode() {
    let root = tempfile::tempdir().unwrap();
    let loadgen = fake_loadgen(root.path());
    let mut data = spawn_sleep();
    let mut gateway = spawn_sleep();
    let control_socket = root.path().join("gateway-control.sock");
    let _listener = UnixListener::bind(&control_socket).unwrap();
    let data_directory = root.path().join("data-1");
    fs::create_dir(&data_directory).unwrap();
    let mut value = proxy_runtime(
        &loadgen,
        data.id(),
        gateway.id(),
        &data_directory,
        &control_socket,
    );
    value["proxy_targets"][0]
        .as_object_mut()
        .unwrap()
        .remove("deployment_mode");
    let runtime = write_runtime(root.path(), value);
    let config = root.path().join("proxy-cell.json");
    fs::write(
        &config,
        serde_json::to_vec_pretty(&cell(Backend::Rocksdb, ExperimentPath::Proxy)).unwrap(),
    )
    .unwrap();
    let output = root.path().join("proxy-output.json");

    let result = executor_command(&runtime, &config, &output)
        .output()
        .unwrap();
    stop(&mut data);
    stop(&mut gateway);

    assert!(!result.status.success());
    assert!(!output.exists());
    assert!(
        String::from_utf8_lossy(&result.stderr).contains("deployment_mode"),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
}

#[test]
fn remote_formal_rejects_loopback_listeners_and_shared_interfaces_before_ssh() {
    let root = tempfile::tempdir().unwrap();
    let loadgen = fake_loadgen(root.path());
    let mut gateway = spawn_sleep();
    let control_socket = root.path().join("gateway-control.sock");
    let _listener = UnixListener::bind(&control_socket).unwrap();
    let config = root.path().join("proxy-cell.json");
    fs::write(
        &config,
        serde_json::to_vec_pretty(&cell(Backend::Rocksdb, ExperimentPath::Proxy)).unwrap(),
    )
    .unwrap();

    for (index, mutate) in ["loopback", "shared_interface"].into_iter().enumerate() {
        let mut value = remote_proxy_runtime(&loadgen, gateway.id(), &control_socket);
        if mutate == "loopback" {
            value["proxy_targets"][0]["data_node_processes"][0]["listen_address"] =
                json!("127.0.0.1:7101");
        } else {
            value["proxy_targets"][0]["data_node_processes"][0]["probe"]["management_interface"] =
                json!("eth-data");
        }
        let runtime = write_runtime(root.path(), value);
        let output = root.path().join(format!("invalid-{index}.json"));
        let result = executor_command(&runtime, &config, &output)
            .env("FAKE_LOADGEN_INVOCATIONS", root.path().join("loadgen.log"))
            .output()
            .unwrap();
        assert!(!result.status.success(), "{mutate}");
        assert!(!output.exists());
        assert!(!root.path().join("loadgen.log").exists());
    }
    stop(&mut gateway);
}

#[test]
fn remote_formal_requires_unique_host_ids_per_data_node() {
    let root = tempfile::tempdir().unwrap();
    let loadgen = fake_loadgen(root.path());
    let mut gateway = spawn_sleep();
    let control_socket = root.path().join("gateway-control.sock");
    let _listener = UnixListener::bind(&control_socket).unwrap();
    let mut value = remote_proxy_runtime(&loadgen, gateway.id(), &control_socket);
    let mut second = value["proxy_targets"][0]["data_node_processes"][0].clone();
    second["pid"] = json!(4243);
    second["listen_address"] = json!("10.0.0.11:7101");
    second["data_directory"] = json!("/var/lib/dtg/node-2");
    second["probe"]["ssh_target"] = json!("bench@node-b");
    value["proxy_targets"][0]["data_nodes"] = json!(2);
    value["proxy_targets"][0]["data_node_processes"]
        .as_array_mut()
        .unwrap()
        .push(second);
    let runtime = write_runtime(root.path(), value);
    let mut proxy_cell = cell(Backend::Rocksdb, ExperimentPath::Proxy);
    proxy_cell.cell.key.data_nodes = 2;
    let config = root.path().join("proxy-cell.json");
    fs::write(&config, serde_json::to_vec_pretty(&proxy_cell).unwrap()).unwrap();
    let output = root.path().join("output.json");

    let result = executor_command(&runtime, &config, &output)
        .output()
        .unwrap();
    stop(&mut gateway);

    assert!(!result.status.success());
    assert!(!output.exists());
    assert!(
        String::from_utf8_lossy(&result.stderr).contains("host_id"),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
}

#[test]
fn remote_formal_rejects_probe_evidence_that_differs_from_the_manifest() {
    let root = tempfile::tempdir().unwrap();
    let fake_bin = root.path().join("fake-bin");
    fs::create_dir(&fake_bin).unwrap();
    fake_ssh(&fake_bin);
    let loadgen = fake_loadgen(root.path());
    let mut gateway = spawn_sleep();
    let control_socket = root.path().join("gateway-control.sock");
    let _listener = UnixListener::bind(&control_socket).unwrap();
    let runtime = write_runtime(
        root.path(),
        remote_proxy_runtime(&loadgen, gateway.id(), &control_socket),
    );
    let config = root.path().join("proxy-cell.json");
    fs::write(
        &config,
        serde_json::to_vec_pretty(&cell(Backend::Rocksdb, ExperimentPath::Proxy)).unwrap(),
    )
    .unwrap();
    let output = root.path().join("output.json");
    let mut mismatched: Value =
        serde_json::from_str(&remote_probe_json(1_500_000_000, 100, 1, 2, 4)).unwrap();
    mismatched["executable_sha256"] = json!(digest('f'));
    let path = format!("{}:{}", fake_bin.display(), std::env::var("PATH").unwrap());

    let result = executor_command(&runtime, &config, &output)
        .env("PATH", path)
        .env("FAKE_EVENT_LOG", root.path().join("events.log"))
        .env("FAKE_LOADGEN_INVOCATIONS", root.path().join("loadgen.log"))
        .env("FAKE_SSH_LOG", root.path().join("ssh.log"))
        .env("FAKE_SSH_STATE", root.path().join("ssh.state"))
        .env(
            "FAKE_SSH_BEFORE",
            serde_json::to_string(&mismatched).unwrap(),
        )
        .env(
            "FAKE_SSH_AFTER",
            remote_probe_json(3_500_000_000, 160, 2, 3, 8),
        )
        .output()
        .unwrap();
    stop(&mut gateway);

    assert!(!result.status.success());
    assert!(!output.exists());
    assert!(!root.path().join("loadgen.log").exists());
    assert!(
        String::from_utf8_lossy(&result.stderr).contains("executable_sha256"),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
}

#[test]
fn proxy_rejects_duplicate_process_identity_before_starting_loadgen() {
    let root = tempfile::tempdir().unwrap();
    let invocation_log = root.path().join("loadgen-invocations.log");
    let loadgen = fake_loadgen(root.path());
    let mut process = spawn_sleep();
    let data_directory = root.path().join("data-1");
    fs::create_dir(&data_directory).unwrap();
    let control_socket = root.path().join("gateway-control.sock");
    let _control_listener = UnixListener::bind(&control_socket).unwrap();
    let runtime = write_runtime(
        root.path(),
        proxy_runtime(
            &loadgen,
            process.id(),
            process.id(),
            &data_directory,
            &control_socket,
        ),
    );
    let config = root.path().join("proxy-cell.json");
    fs::write(
        &config,
        serde_json::to_vec_pretty(&cell(Backend::Rocksdb, ExperimentPath::Proxy)).unwrap(),
    )
    .unwrap();
    let output = root.path().join("proxy-output.json");

    let result = executor_command(&runtime, &config, &output)
        .env("FAKE_LOADGEN_INVOCATIONS", &invocation_log)
        .output()
        .unwrap();
    stop(&mut process);

    assert!(!result.status.success());
    assert!(!output.exists());
    assert!(!invocation_log.exists());
    assert!(
        String::from_utf8_lossy(&result.stderr).contains("distinct OS processes"),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
}

#[test]
fn proxy_aborts_gateway_session_after_loadgen_failure() {
    let root = tempfile::tempdir().unwrap();
    let loadgen = root.path().join("failing-loadgen");
    fs::write(&loadgen, "#!/bin/sh\nexit 9\n").unwrap();
    let mut permissions = fs::metadata(&loadgen).unwrap().permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&loadgen, permissions).unwrap();
    let mut data = spawn_sleep();
    let mut gateway = spawn_sleep();
    let data_directory = root.path().join("data-1");
    fs::create_dir(&data_directory).unwrap();
    let control_socket = root.path().join("gateway-control.sock");
    let gateway_control = fake_failing_gateway(&control_socket, gateway.id());
    let runtime = write_runtime(
        root.path(),
        proxy_runtime(
            &loadgen,
            data.id(),
            gateway.id(),
            &data_directory,
            &control_socket,
        ),
    );
    let config = root.path().join("proxy-cell.json");
    fs::write(
        &config,
        serde_json::to_vec_pretty(&cell(Backend::Rocksdb, ExperimentPath::Proxy)).unwrap(),
    )
    .unwrap();
    let output = root.path().join("proxy-output.json");

    let result = executor_command(&runtime, &config, &output)
        .output()
        .unwrap();
    stop(&mut data);
    stop(&mut gateway);
    let requests = gateway_control.join().unwrap();

    assert!(!result.status.success());
    assert!(!output.exists());
    assert!(String::from_utf8_lossy(&result.stderr).contains("loadgen failed"));
    assert_eq!(
        requests
            .iter()
            .map(|request| request["operation"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["begin_cell", "finish_cell", "abort_cell"]
    );
}

#[test]
fn existing_output_is_rejected_before_any_measurement() {
    let root = tempfile::tempdir().unwrap();
    let runtime = write_runtime(
        root.path(),
        json!({
            "schema_version": 1,
            "dataset_digest": digest('1'),
            "snapshot": "as_of:1000",
            "workloads": [{
                "workload_digest": digest('2'),
                "kind": "count_current_vertices",
                "graph_id": 7
            }],
            "backends": {
                "rocksdb": {"status": "unavailable", "reason": "must not be consulted"},
                "postgresql": {"status": "unavailable", "reason": "unused"},
                "neo4j": {"status": "unavailable", "reason": "unused"}
            },
            "proxy_targets": []
        }),
    );
    let config = root.path().join("cell.json");
    fs::write(
        &config,
        serde_json::to_vec_pretty(&cell(Backend::Rocksdb, ExperimentPath::BackendDirect)).unwrap(),
    )
    .unwrap();
    let output = root.path().join("output.json");
    fs::write(&output, b"original\n").unwrap();

    let result = executor_command(&runtime, &config, &output)
        .output()
        .unwrap();

    assert!(!result.status.success());
    assert_eq!(fs::read(output).unwrap(), b"original\n");
    assert!(
        String::from_utf8_lossy(&result.stderr).contains("already exists"),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
}

fn run_cell(root: &Path, runtime: &Path, cell: CellConfig, output_name: &str) -> RawObservation {
    let config = root.join(format!("{output_name}.config"));
    fs::write(&config, serde_json::to_vec_pretty(&cell).unwrap()).unwrap();
    let output = root.join(output_name);
    let result = executor_command(runtime, &config, &output)
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    serde_json::from_slice(&fs::read(output).unwrap()).unwrap()
}

fn executor_command(runtime: &Path, config: &Path, output: &Path) -> Command {
    let mut command = Command::new(executor_binary());
    command
        .args(["--cell-config", config.to_str().unwrap()])
        .args(["--output", output.to_str().unwrap()])
        .env("DTGPROXY_PAPER_RUNTIME_MANIFEST", runtime);
    command
}

fn executor_binary() -> &'static str {
    env!("CARGO_BIN_EXE_dtgproxy-paper-cell-executor")
}

fn cell(backend: Backend, path: ExperimentPath) -> CellConfig {
    CellConfig {
        schema_version: SCHEMA_VERSION,
        run_id: "executor-contract-test".into(),
        sequence: 1,
        cell: MatrixCell {
            key: CellKey {
                backend,
                workload: "scale_count".into(),
                data_nodes: 1,
                concurrency: 1,
                ablation: "production".into(),
            },
            path,
            repetition: 1,
        },
        dataset_digest: digest('1'),
        workload_digest: digest('2'),
        snapshot: "as_of:1000".into(),
        parameters: BTreeMap::new(),
        parameters_digest: digest('3'),
        query: QUERY.into(),
        protocol: ExperimentProtocol {
            warmup_seconds: 1,
            measurement_seconds: 1,
            repetitions: 1,
        },
        configuration_digest: digest('4'),
    }
}

fn seed_rocks(path: &Path, count: u32) {
    let adapter = RocksAdapter::open(path).unwrap();
    let graph = GraphId::new(7);
    let mutations = (0..count)
        .map(|index| {
            Mutation::put(
                index,
                current_vertex_key(ElementRef::vertex(
                    graph,
                    PartitionId::new(index),
                    ElementId::new(u128::from(index) + 1),
                )),
                current_vertex_value(),
            )
        })
        .collect();
    block_on(adapter.apply_committed(CommittedMutationBatch {
        shard_id: 1,
        log_index: 1,
        txn_id: 1,
        mutations,
    }))
    .unwrap();
}

fn vertex_batch(count: u32) -> CommittedMutationBatch {
    let graph = GraphId::new(7);
    let mutations = (0..count)
        .map(|index| {
            Mutation::put(
                index,
                current_vertex_key(ElementRef::vertex(
                    graph,
                    PartitionId::new(index),
                    ElementId::new(u128::from(index) + 1),
                )),
                current_vertex_value(),
            )
        })
        .collect();
    CommittedMutationBatch {
        shard_id: 1,
        log_index: 1,
        txn_id: 1,
        mutations,
    }
}

fn unique_instance(prefix: &str) -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    format!(
        "{prefix}-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

fn current_vertex_value() -> Vec<u8> {
    ProjectionRecord::new(
        TransactionTime::new(10, 0),
        vec![ValidSegment::new(
            Interval::new(ValidTime::from_micros(1), None).unwrap(),
            CanonicalElement::new(1, BTreeMap::new()),
        )],
    )
    .unwrap()
    .encode()
    .unwrap()
}

fn write_runtime(root: &Path, value: Value) -> PathBuf {
    let path = root.join("runtime.json");
    fs::write(&path, serde_json::to_vec_pretty(&value).unwrap()).unwrap();
    path
}

fn proxy_runtime(
    loadgen: &Path,
    data_pid: u32,
    gateway_pid: u32,
    data_directory: &Path,
    control_socket: &Path,
) -> Value {
    json!({
        "schema_version": 1,
        "dataset_digest": digest('1'),
        "snapshot": "as_of:1000",
        "workloads": [{
            "workload_digest": digest('2'),
            "kind": "count_current_vertices",
            "graph_id": 7
        }],
        "backends": {
            "rocksdb": {"status": "unavailable", "reason": "Proxy owns the live RocksDB"},
            "postgresql": {"status": "unavailable", "reason": "fixture has no PostgreSQL"},
            "neo4j": {"status": "unavailable", "reason": "fixture has no Neo4j"}
        },
        "proxy_targets": [{
            "deployment_mode": "local_diagnostic",
            "backend": "rocksdb",
            "data_nodes": 1,
            "ablation": "production",
            "bolt_address": "127.0.0.1:7687",
            "bolt_loadgen_binary": loadgen,
            "ablation_control_socket": control_socket,
            "data_node_processes": [{
                "pid": data_pid,
                "executable": "/bin/sleep",
                "listen_address": "127.0.0.1:7101",
                "data_directory": data_directory
            }],
            "gateway_process": {
                "pid": gateway_pid,
                "executable": "/bin/sleep",
                "listen_address": "127.0.0.1:7201"
            }
        }]
    })
}

fn remote_proxy_runtime(loadgen: &Path, gateway_pid: u32, control_socket: &Path) -> Value {
    json!({
        "schema_version": 1,
        "dataset_digest": digest('1'),
        "snapshot": "as_of:1000",
        "workloads": [{
            "workload_digest": digest('2'),
            "kind": "count_current_vertices",
            "graph_id": 7
        }],
        "backends": {
            "rocksdb": {"status": "unavailable", "reason": "Proxy owns RocksDB"},
            "postgresql": {"status": "unavailable", "reason": "unused"},
            "neo4j": {"status": "unavailable", "reason": "unused"}
        },
        "proxy_targets": [{
            "deployment_mode": "remote_formal",
            "backend": "rocksdb",
            "data_nodes": 1,
            "ablation": "production",
            "bolt_address": "127.0.0.1:7687",
            "bolt_loadgen_binary": loadgen,
            "ablation_control_socket": control_socket,
            "timeout_ms": 2_000,
            "data_node_processes": [{
                "pid": 4242,
                "executable": "/opt/dtg/data-node",
                "executable_sha256": digest('a'),
                "listen_address": "10.0.0.10:7101",
                "data_directory": "/var/lib/dtg/node-1",
                "probe": {
                    "ssh_target": "bench@node-a",
                    "host_id": "host-a",
                    "boot_id": "boot-a",
                    "probe_binary": "/opt/dtg/dtgproxy-paper-cell-executor",
                    "probe_binary_sha256": digest('b'),
                    "data_interface": "eth-data",
                    "management_interface": "eth-mgmt"
                }
            }],
            "gateway_process": {
                "pid": gateway_pid,
                "executable": "/bin/sleep",
                "listen_address": "127.0.0.1:7201"
            }
        }]
    })
}

fn remote_probe_json(
    sampled_unix_ns: u64,
    cpu_time_ns: u64,
    network_rx_bytes: u64,
    network_tx_bytes: u64,
    peak_rss_bytes: u64,
) -> String {
    serde_json::to_string(&json!({
        "schema_version": 1,
        "sampled_unix_ns": sampled_unix_ns,
        "host_id": "host-a",
        "boot_id": "boot-a",
        "process_start_id": "process-4242-start",
        "pid": 4242,
        "executable": "/opt/dtg/data-node",
        "executable_sha256": digest('a'),
        "probe_binary_sha256": digest('b'),
        "listen_address": "10.0.0.10:7101",
        "data_directory": "/var/lib/dtg/node-1",
        "cpu_time_ns": cpu_time_ns,
        "rss_bytes": peak_rss_bytes / 2,
        "peak_rss_bytes": peak_rss_bytes,
        "network_interface": "eth-data",
        "network_rx_bytes": network_rx_bytes,
        "network_tx_bytes": network_tx_bytes
    }))
    .unwrap()
}

fn remote_resource_probe_json(
    sampled_unix_ns: u64,
    cpu_time_ns: u64,
    rss_bytes: u64,
    network_rx_bytes: u64,
    network_tx_bytes: u64,
) -> String {
    serde_json::to_string(&json!({
        "schema_version": 1,
        "sampled_unix_ns": sampled_unix_ns,
        "process_start_id": "process-4242-start",
        "pid": 4242,
        "cpu_time_ns": cpu_time_ns,
        "rss_bytes": rss_bytes,
        "network_interface": "eth-data",
        "network_rx_bytes": network_rx_bytes,
        "network_tx_bytes": network_tx_bytes
    }))
    .unwrap()
}

fn fake_ssh(root: &Path) -> PathBuf {
    let path = root.join("ssh");
    fs::write(
        &path,
        r#"#!/bin/sh
set -eu
printf '%s\n' "$*" >> "$FAKE_SSH_LOG"
case " $* " in
  *" probe-resources "*)
    printf 'probe-resource\n' >> "$FAKE_EVENT_LOG"
    resource_state="$FAKE_SSH_STATE.resource"
    resource_count=0
    if [ -f "$resource_state" ]; then resource_count=$(cat "$resource_state"); fi
    resource_count=$((resource_count + 1))
    printf '%s\n' "$resource_count" > "$resource_state"
    printf '%s\n' "$FAKE_SSH_RESOURCE" | jq -c --argjson count "$resource_count" '
      .cpu_time_ns += (10 * $count)
      | .network_rx_bytes += (10 * $count)
      | .network_tx_bytes += (20 * $count)'
    exit 0
    ;;
esac
printf 'probe-full\n' >> "$FAKE_EVENT_LOG"
count=0
if [ -f "$FAKE_SSH_STATE" ]; then count=$(cat "$FAKE_SSH_STATE"); fi
count=$((count + 1))
printf '%s\n' "$count" > "$FAKE_SSH_STATE"
if [ "$count" -eq 1 ]; then
  printf '%s\n' "$FAKE_SSH_BEFORE"
else
  printf '%s\n' "$FAKE_SSH_AFTER"
fi
"#,
    )
    .unwrap();
    let mut permissions = fs::metadata(&path).unwrap().permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&path, permissions).unwrap();
    path
}

fn loopback_interface() -> &'static str {
    if cfg!(target_os = "linux") {
        "lo"
    } else {
        "lo0"
    }
}

fn fake_loadgen(root: &Path) -> PathBuf {
    let path = root.join("dtgproxy-bolt-loadgen");
    fs::write(
        &path,
        format!(
            r#"#!/bin/sh
set -eu
printf '%s\n' "$*" >> "$FAKE_LOADGEN_INVOCATIONS"
if [ -n "${{FAKE_EVENT_LOG:-}}" ]; then printf 'loadgen\n' >> "$FAKE_EVENT_LOG"; fi
if [ -n "${{FAKE_DYNAMIC_TIMING:-}}" ]; then
  warmup_started=$(python3 -c 'import time; print(time.time_ns() - 500_000_000)')
  measurement_started=$((warmup_started + 1000000000))
  measurement_ended=$((measurement_started + 1000000000))
  sleep 2
else
  warmup_started=1000000000
  measurement_started=2000000000
  measurement_ended=3000000000
fi
output=
while [ "$#" -gt 0 ]; do
  if [ "$1" = "--output" ]; then output=$2; shift 2; else shift; fi
done
cat > "$output" <<JSON
{{
  "schema_version": 1,
  "timing_boundary": "RUN send to first decoded RECORD; connection setup excluded",
  "connections": 1,
  "warmup_ns": 1000000000,
  "warmup_started_unix_ns": $warmup_started,
  "measurement_started_unix_ns": $measurement_started,
  "measurement_ended_unix_ns": $measurement_ended,
  "measured_elapsed_ns": 1000000000,
  "total_operations": 2,
  "completed_operations": 1,
  "throughput_ops_per_second": 1.0,
  "connection_setup_samples_ns": [10],
  "ttfr_samples_ns": [20],
  "total_latency_samples_ns": [30],
  "result_digest": "{}",
  "row_count": 1,
  "error_count": 0
}}
JSON
"#,
            digest('d')
        ),
    )
    .unwrap();
    let mut permissions = fs::metadata(&path).unwrap().permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&path, permissions).unwrap();
    path
}

fn fake_gateway(socket: &Path, gateway_pid: u32) -> thread::JoinHandle<Vec<Value>> {
    let listener = UnixListener::bind(socket).unwrap();
    thread::spawn(move || {
        let mut requests = Vec::new();
        for index in 0..2 {
            let (mut stream, _) = listener.accept().unwrap();
            let mut line = String::new();
            BufReader::new(stream.try_clone().unwrap())
                .read_line(&mut line)
                .unwrap();
            let request: Value = serde_json::from_str(&line).unwrap();
            requests.push(request.clone());
            let response = if index == 0 {
                json!({
                    "status": "begun",
                    "schema_version": 1,
                    "gateway_pid": gateway_pid,
                    "session_token": "session_1",
                    "accepted_config": request["config"]
                })
            } else {
                json!({
                    "status": "finished",
                    "schema_version": 1,
                    "gateway_pid": gateway_pid,
                    "cell_id": "executor-contract-test:1",
                    "configuration_digest": digest('4'),
                    "accepted_config": {
                        "native_pushdown": true,
                        "column_batches": true,
                        "bounded_lazy_pages": true,
                        "parallel_shard_fanout": true,
                        "batched_property_gather": true
                    },
                    "queries_started": 2,
                    "queries_completed": 2,
                    "queries_failed": 0,
                    "queries_in_flight": 0,
                    "counters": {
                        "canonical_residual_scans": 0,
                        "row_column_conversion_boundaries": 0,
                        "eager_page_collections": 0,
                        "serial_shard_opens": 0,
                        "singleton_property_gather_reads": 0
                    }
                })
            };
            writeln!(stream, "{}", serde_json::to_string(&response).unwrap()).unwrap();
        }
        requests
    })
}

fn fake_failing_gateway(socket: &Path, gateway_pid: u32) -> thread::JoinHandle<Vec<Value>> {
    let listener = UnixListener::bind(socket).unwrap();
    thread::spawn(move || {
        let mut requests = Vec::new();
        for index in 0..3 {
            let (mut stream, _) = listener.accept().unwrap();
            let mut line = String::new();
            BufReader::new(stream.try_clone().unwrap())
                .read_line(&mut line)
                .unwrap();
            let request: Value = serde_json::from_str(&line).unwrap();
            requests.push(request.clone());
            let response = match index {
                0 => json!({
                    "status": "begun",
                    "schema_version": 1,
                    "gateway_pid": gateway_pid,
                    "session_token": "session_1",
                    "accepted_config": request["config"]
                }),
                1 => json!({
                    "status": "error",
                    "schema_version": 1,
                    "gateway_pid": gateway_pid,
                    "message": "queries still in flight"
                }),
                _ => json!({
                    "status": "aborted",
                    "schema_version": 1,
                    "gateway_pid": gateway_pid
                }),
            };
            writeln!(stream, "{}", serde_json::to_string(&response).unwrap()).unwrap();
        }
        requests
    })
}

fn spawn_sleep() -> Child {
    Command::new("/bin/sleep").arg("30").spawn().unwrap()
}

fn stop(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

fn observed_resources(observation: &RawObservation) -> bool {
    serde_json::to_value(&observation.resources)
        .unwrap()
        .as_object()
        .unwrap()
        .values()
        .all(|metric| metric["status"] == "observed")
}

fn digest(character: char) -> String {
    std::iter::repeat_n(character, 64).collect()
}

fn expected_count_identity(count: i64) -> ResultIdentity {
    let record = BoltValue::Structure {
        signature: 0x71,
        fields: vec![BoltValue::List(vec![BoltValue::Integer(count)])],
    };
    let encoded = encode_bolt(&record).unwrap();
    let mut digest = blake3::Hasher::new();
    digest.update(&(encoded.len() as u64).to_be_bytes());
    digest.update(&encoded);
    ResultIdentity {
        digest: digest.finalize().to_hex().to_string(),
        row_count: 1,
    }
}

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    use std::sync::Arc;
    use std::task::{Context, Poll, Wake, Waker};

    struct NoopWake;
    impl Wake for NoopWake {
        fn wake(self: Arc<Self>) {}
    }

    let waker = Waker::from(Arc::new(NoopWake));
    let mut context = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}
