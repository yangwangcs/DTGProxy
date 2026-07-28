use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use adapter_neo4j::Neo4jAdapterFactory;
use adapter_postgres::PostgresAdapter;
use adapter_registry::{AdapterOpenRequest, AdapterRegistry, SecretString};
use adapter_rocksdb::RocksAdapter;
use paper_benchmark::{
    Backend, CellConfig, CellKey, ExperimentPath, ExperimentProtocol, MatrixCell, RawObservation,
    SCHEMA_VERSION, sha256_file, summarize_samples,
};
use serde_json::{Value, json};
use storage_api::{AdapterRequirement, CommittedMutationBatch, Mutation, StorageAdapter};
use temporal_storage::{
    ElementId, ElementRef, GraphId, PartitionId, ProjectionRecord, ValidSegment, current_vertex_key,
};
use temporal_types::{CanonicalElement, Interval, TransactionTime, ValidTime};

const QUERY: &str = "USE scale_graph FOR VALID_TIME AS OF 1000 MATCH (n) RETURN count(n) AS count";
const VERTEX_COUNT: u32 = 4_096;

#[test]
fn capture_selected_backend_diagnostic() {
    let Ok(backend_name) = std::env::var("DTGPROXY_DIAGNOSTIC_BACKEND") else {
        return;
    };
    let output = PathBuf::from(required_env("DTGPROXY_DIAGNOSTIC_OUTPUT_DIR"));
    assert!(!output.exists(), "diagnostic output already exists");
    let warmup_seconds = u64::from(positive_env("DTGPROXY_DIAGNOSTIC_WARMUP_SECONDS"));
    let measurement_seconds = u64::from(positive_env("DTGPROXY_DIAGNOSTIC_MEASUREMENT_SECONDS"));
    let repetitions = positive_env("DTGPROXY_DIAGNOSTIC_REPETITIONS");
    let backend = match backend_name.as_str() {
        "rocksdb" => Backend::Rocksdb,
        "postgresql" => Backend::Postgresql,
        "neo4j" => Backend::Neo4j,
        _ => panic!("unsupported diagnostic backend: {backend_name}"),
    };

    fs::create_dir_all(output.join("raw")).unwrap();
    let root = tempfile::tempdir().unwrap();
    let instance_id = unique_instance(&format!("diagnostic-{backend_name}"));
    let runtime = prepare_runtime(root.path(), backend, &instance_id);
    let run_id = unique_instance(&format!("diagnostic-{backend_name}"));
    let mut observations = Vec::new();
    let mut sequence = 0_usize;

    for repetition in 1..=repetitions {
        for path in [ExperimentPath::BackendDirect, ExperimentPath::AdapterDirect] {
            sequence += 1;
            let config = cell(
                backend,
                path,
                &run_id,
                sequence,
                repetition,
                warmup_seconds,
                measurement_seconds,
                repetitions,
            );
            let stem = format!("{path}-r{repetition}");
            let config_path = root.path().join(format!("{stem}.config.json"));
            fs::write(&config_path, serde_json::to_vec_pretty(&config).unwrap()).unwrap();
            let raw_path = output.join("raw").join(format!("{stem}.json"));
            let result = Command::new(env!("CARGO_BIN_EXE_dtgproxy-paper-cell-executor"))
                .args(["--cell-config", config_path.to_str().unwrap()])
                .args(["--output", raw_path.to_str().unwrap()])
                .env("DTGPROXY_PAPER_RUNTIME_MANIFEST", &runtime)
                .output()
                .unwrap();
            assert!(
                result.status.success(),
                "{stem}: {}",
                String::from_utf8_lossy(&result.stderr)
            );
            let observation: RawObservation =
                serde_json::from_slice(&fs::read(&raw_path).unwrap()).unwrap();
            observation.validate().unwrap();
            observations.push((raw_path, observation));
        }
    }

    let identity = observations[0].1.identity.clone();
    assert!(
        observations
            .iter()
            .all(|(_, value)| value.identity == identity)
    );
    assert!(observations.iter().all(|(_, value)| value.errors == 0));
    let summaries = [ExperimentPath::BackendDirect, ExperimentPath::AdapterDirect]
        .into_iter()
        .map(|path| {
            let selected = observations
                .iter()
                .filter(|(_, value)| value.path == path)
                .map(|(_, value)| value)
                .collect::<Vec<_>>();
            let latency = selected
                .iter()
                .flat_map(|value| value.samples.latency_ns.iter().copied())
                .collect::<Vec<_>>();
            let operations = selected.iter().map(|value| value.operations).sum::<u64>();
            let measured_seconds = selected
                .iter()
                .map(|value| {
                    (value.timing.measurement_ended_unix_ns
                        - value.timing.measurement_started_unix_ns) as f64
                        / 1_000_000_000.0
                })
                .sum::<f64>();
            json!({
                "path": path,
                "repetitions": repetitions,
                "operations": operations,
                "throughput_ops_per_second": operations as f64 / measured_seconds,
                "latency_ns": summarize_samples(&latency).unwrap(),
            })
        })
        .collect::<Vec<_>>();
    let raw_files = observations
        .iter()
        .map(|(path, _)| {
            json!({
                "path": path.strip_prefix(&output).unwrap(),
                "sha256": sha256_file(path).unwrap(),
            })
        })
        .collect::<Vec<_>>();
    let manifest = json!({
        "schema_version": SCHEMA_VERSION,
        "mode": "diagnostic",
        "selected_backend": backend,
        "run_id": run_id,
        "dataset": {
            "kind": "count_current_vertices",
            "vertex_count": VERTEX_COUNT,
            "graph_id": 7,
            "digest": digest('1'),
        },
        "protocol": {
            "warmup_seconds": warmup_seconds,
            "measurement_seconds": measurement_seconds,
            "repetitions": repetitions,
            "concurrency": 1,
        },
        "paths": [ExperimentPath::BackendDirect, ExperimentPath::AdapterDirect],
        "result_identity": identity,
        "summaries": summaries,
        "raw_observations": raw_files,
        "environment": {
            "os": std::env::consts::OS,
            "arch": std::env::consts::ARCH,
            "backend_service": sanitized_backend_service(backend),
        },
    });
    fs::write(
        output.join("diagnostic-manifest.json"),
        serde_json::to_vec_pretty(&manifest).unwrap(),
    )
    .unwrap();
}

fn prepare_runtime(root: &Path, backend: Backend, instance_id: &str) -> PathBuf {
    let selected = match backend {
        Backend::Rocksdb => {
            let database = root.join("rocks-snapshot");
            let adapter = RocksAdapter::open(&database).unwrap();
            block_on(adapter.apply_committed(vertex_batch(VERTEX_COUNT))).unwrap();
            drop(adapter);
            json!({"status": "available", "snapshot_paths": [database]})
        }
        Backend::Postgresql => {
            let url = required_env("DTGPROXY_PAPER_TEST_POSTGRES_URL");
            let adapter = PostgresAdapter::open(&url, instance_id, 4).unwrap();
            block_on(adapter.apply_committed(vertex_batch(VERTEX_COUNT))).unwrap();
            drop(adapter);
            json!({
                "status": "available",
                "connection_string": url,
                "instance_id": instance_id,
                "pool_size": 4,
            })
        }
        Backend::Neo4j => {
            let endpoint = required_env("DTGPROXY_PAPER_TEST_NEO4J_ENDPOINT");
            let username = required_env("DTGPROXY_PAPER_TEST_NEO4J_USERNAME");
            let password = required_env("DTGPROXY_PAPER_TEST_NEO4J_PASSWORD");
            let database = required_env("DTGPROXY_PAPER_TEST_NEO4J_DATABASE");
            let mut registry = AdapterRegistry::new();
            registry
                .register(std::sync::Arc::new(Neo4jAdapterFactory))
                .unwrap();
            let request = AdapterOpenRequest::new(instance_id)
                .with_parameter("endpoint", &endpoint)
                .with_parameter("database", &database)
                .with_parameter("username", &username)
                .with_secret("password", SecretString::new(&password));
            let opened =
                block_on(registry.open("neo4j", &request, AdapterRequirement::HotPluggableReplica))
                    .unwrap();
            block_on(opened.adapter().apply_committed(vertex_batch(VERTEX_COUNT))).unwrap();
            drop(opened);
            json!({
                "status": "available",
                "endpoint": endpoint,
                "database": database,
                "username": username,
                "password": password,
                "instance_id": instance_id,
                "timeout_seconds": 30,
            })
        }
    };
    let mut backends = serde_json::Map::new();
    for candidate in [Backend::Rocksdb, Backend::Postgresql, Backend::Neo4j] {
        backends.insert(
            candidate.to_string(),
            if candidate == backend {
                selected.clone()
            } else {
                json!({"status": "unavailable", "reason": "not selected"})
            },
        );
    }
    let runtime = root.join("runtime.json");
    fs::write(
        &runtime,
        serde_json::to_vec_pretty(&json!({
            "schema_version": SCHEMA_VERSION,
            "dataset_digest": digest('1'),
            "snapshot": "as_of:1000",
            "workloads": [{
                "workload_digest": digest('2'),
                "kind": "count_current_vertices",
                "graph_id": 7,
            }],
            "backends": Value::Object(backends),
            "proxy_targets": [],
        }))
        .unwrap(),
    )
    .unwrap();
    runtime
}

fn cell(
    backend: Backend,
    path: ExperimentPath,
    run_id: &str,
    sequence: usize,
    repetition: u32,
    warmup_seconds: u64,
    measurement_seconds: u64,
    repetitions: u32,
) -> CellConfig {
    CellConfig {
        schema_version: SCHEMA_VERSION,
        run_id: run_id.into(),
        sequence,
        cell: MatrixCell {
            key: CellKey {
                backend,
                workload: "scale_count".into(),
                data_nodes: 1,
                concurrency: 1,
                ablation: "production".into(),
            },
            path,
            repetition,
        },
        dataset_digest: digest('1'),
        workload_digest: digest('2'),
        snapshot: "as_of:1000".into(),
        parameters: BTreeMap::new(),
        parameters_digest: digest('3'),
        query: QUERY.into(),
        protocol: ExperimentProtocol {
            warmup_seconds,
            measurement_seconds,
            repetitions,
        },
        configuration_digest: digest('4'),
    }
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
                ProjectionRecord::new(
                    TransactionTime::new(10, 0),
                    vec![ValidSegment::new(
                        Interval::new(ValidTime::from_micros(1), None).unwrap(),
                        CanonicalElement::new(1, BTreeMap::new()),
                    )],
                )
                .unwrap()
                .encode()
                .unwrap(),
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

fn required_env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("{name} is required"))
}

fn positive_env(name: &str) -> u32 {
    required_env(name)
        .parse::<u32>()
        .ok()
        .filter(|value| *value > 0)
        .unwrap_or_else(|| panic!("{name} must be a positive integer"))
}

fn sanitized_backend_service(backend: Backend) -> Value {
    match backend {
        Backend::Rocksdb => json!({"kind": "embedded_rocksdb"}),
        Backend::Postgresql => json!({
            "kind": "postgresql",
            "image": std::env::var("DTGPROXY_DIAGNOSTIC_BACKEND_IMAGE").ok(),
        }),
        Backend::Neo4j => json!({
            "kind": "neo4j",
            "endpoint": std::env::var("DTGPROXY_PAPER_TEST_NEO4J_ENDPOINT").ok(),
            "image": std::env::var("DTGPROXY_DIAGNOSTIC_BACKEND_IMAGE").ok(),
        }),
    }
}

fn digest(character: char) -> String {
    std::iter::repeat_n(character, 64).collect()
}

fn unique_instance(prefix: &str) -> String {
    format!(
        "{prefix}-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
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
