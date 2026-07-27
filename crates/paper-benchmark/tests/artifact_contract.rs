use paper_benchmark::{
    AblationCounters, AblationEvidence, ArtifactManifest, Backend, BinaryFingerprints,
    DatasetManifest, EnvironmentFingerprint, EnvironmentFingerprintSource, ExperimentMatrix,
    ExperimentPath, ExperimentProtocol, ExperimentSpec, ExperimentSuite, ExperimentSuiteKind,
    FORMAL_CONCURRENCIES, FORMAL_DATA_NODES, FORMAL_EDGE_COUNT, FORMAL_MEASUREMENT_SECONDS,
    FORMAL_REPETITIONS, FORMAL_TEMPORAL_UPDATE_COUNT, FORMAL_VERTEX_COUNT, FORMAL_WARMUP_SECONDS,
    HostResourceEvidence, ProcessTopologyEvidence, RawObservation, ResourceMetric, ResourceMetrics,
    ResourceScope, ResultIdentity, RunMode, SCHEMA_VERSION, SampleSeries, SoftwareVersions,
    TimingBoundaries, Topology, TopologyDeploymentMode, TopologyEvidence, WorkloadCase,
    WorkloadManifest, ablation_config_for_label, derive_run_mode, run_experiment, sha256_file,
    validate_artifact_observations, verify_artifact,
};
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);
type SpecMutation = Box<dyn Fn(&mut ExperimentSpec)>;

fn digest(character: char) -> String {
    std::iter::repeat_n(character, 64).collect()
}

fn comparison_paths() -> Vec<ExperimentPath> {
    vec![
        ExperimentPath::BackendDirect,
        ExperimentPath::AdapterDirect,
        ExperimentPath::Proxy,
    ]
}

fn captured_environment() -> EnvironmentFingerprint {
    let mut environment = EnvironmentFingerprint {
        schema_version: SCHEMA_VERSION,
        source: EnvironmentFingerprintSource::Captured,
        os_name: "Linux".into(),
        os_version: "6.8.0-40-generic".into(),
        architecture: "x86_64".into(),
        cpu_model: "AMD EPYC 7763 64-Core Processor".into(),
        logical_cpu_count: 64,
        total_memory_bytes: 274_877_906_944,
        versions: SoftwareVersions {
            rustc: "rustc 1.88.0 (6b00bc388 2025-06-23)".into(),
            cargo: "cargo 1.88.0 (873a06493 2025-05-10)".into(),
            rocksdb: "9.10.0".into(),
            postgresql: "PostgreSQL 17.5".into(),
            neo4j: "Neo4j 2025.06.0".into(),
        },
        binaries: BinaryFingerprints {
            executor_sha256: digest('1'),
            gateway_sha256: digest('2'),
            data_node_sha256: digest('3'),
            meta_node_sha256: digest('4'),
            loadgen_sha256: digest('5'),
        },
        digest: String::new(),
    };
    environment.digest = environment
        .computed_digest()
        .expect("environment fingerprint digest");
    environment
}

fn synthetic_environment() -> EnvironmentFingerprint {
    EnvironmentFingerprint::synthetic()
}

fn workload_case() -> WorkloadCase {
    let mut manifest = WorkloadManifest {
        schema_version: SCHEMA_VERSION,
        workload_id: "point_lookup".into(),
        query: "MATCH (n {id: $id}) RETURN n.id".into(),
        parameters: BTreeMap::from([("id".into(), json!(7))]),
        available_paths: comparison_paths(),
        digest: String::new(),
    };
    manifest.digest = manifest.computed_digest().expect("workload digest");
    WorkloadCase {
        manifest,
        snapshot: "as_of:100".into(),
    }
}

fn proxy_workload_case(id: &str, query: &str) -> WorkloadCase {
    let mut manifest = WorkloadManifest {
        schema_version: SCHEMA_VERSION,
        workload_id: id.into(),
        query: query.into(),
        parameters: BTreeMap::new(),
        available_paths: vec![ExperimentPath::Proxy],
        digest: String::new(),
    };
    manifest.digest = manifest.computed_digest().expect("workload digest");
    WorkloadCase {
        manifest,
        snapshot: "current".into(),
    }
}

fn formal_workloads() -> Vec<WorkloadCase> {
    vec![
        workload_case(),
        proxy_workload_case(
            "partition_parallel_scan",
            "MATCH (n) WHERE n.active = true WITH n.id AS id ORDER BY id LIMIT 4096 RETURN id",
        ),
        proxy_workload_case(
            "native_pushdown_filter",
            "MATCH (n) WHERE n.active = true RETURN count(n)",
        ),
        proxy_workload_case("column_batch_scan", "MATCH (n) RETURN count(n)"),
        proxy_workload_case(
            "lazy_paged_scan",
            "MATCH (n) WHERE n.active = true RETURN n.id ORDER BY n.id",
        ),
        proxy_workload_case("parallel_fanout_count", "MATCH (n) RETURN count(n)"),
        proxy_workload_case(
            "batched_expand_gather",
            "MATCH (n)-[r]->(m) RETURN count(m)",
        ),
    ]
}

fn formal_spec(run_id: &str, selected_backend: Backend) -> ExperimentSpec {
    ExperimentSpec {
        schema_version: SCHEMA_VERSION,
        run_id: run_id.into(),
        selected_backend,
        revision: "0123456789abcdef".into(),
        dirty_worktree_digest: digest('a'),
        environment: captured_environment(),
        dataset: DatasetManifest {
            schema_version: SCHEMA_VERSION,
            dataset_id: "paper-1m-5m-v1".into(),
            seed: 42,
            vertex_count: FORMAL_VERTEX_COUNT,
            edge_count: FORMAL_EDGE_COUNT,
            temporal_update_count: FORMAL_TEMPORAL_UPDATE_COUNT,
            content_digest: digest('b'),
        },
        workloads: formal_workloads(),
        matrix: formal_matrix(selected_backend),
        protocol: ExperimentProtocol {
            warmup_seconds: FORMAL_WARMUP_SECONDS,
            measurement_seconds: FORMAL_MEASUREMENT_SECONDS,
            repetitions: FORMAL_REPETITIONS,
        },
        shuffle_seed: 99,
    }
}

fn json_digest(value: &impl serde::Serialize) -> String {
    blake3::hash(&serde_json::to_vec(value).expect("JSON digest input"))
        .to_hex()
        .to_string()
}

fn formal_topology_evidence(observation: &RawObservation) -> TopologyEvidence {
    let nodes = observation.topology.data_nodes;
    let mut cpu_remainder = 50 % u64::from(nodes);
    let mut rss_remainder = 4096 % u64::from(nodes);
    let mut rx_remainder = 128 % u64::from(nodes);
    let mut tx_remainder = 64 % u64::from(nodes);
    let mut data_nodes = Vec::new();
    let mut resource_samples = Vec::new();
    for index in 0..nodes {
        let host_id = format!("host-{index}");
        let boot_id = format!("boot-{index}");
        let process_start_id = format!("start-{index}");
        let pid = 1000 + index;
        data_nodes.push(ProcessTopologyEvidence {
            host_id: host_id.clone(),
            boot_id: boot_id.clone(),
            process_start_id: process_start_id.clone(),
            pid,
            executable: format!("/opt/dtgproxy/data-node-{index}").into(),
            executable_sha256: digest(char::from_digit(index + 1, 16).unwrap_or('a')),
            listen_address: format!("10.0.0.{}:{}", index + 10, 7000 + index),
            data_interface: "eth0".into(),
            management_interface: "eth1".into(),
        });
        resource_samples.push(HostResourceEvidence {
            host_id,
            boot_id,
            process_start_id,
            pid,
            sampled_before_unix_ns: observation.timing.warmup_started_unix_ns - 1,
            sampled_after_unix_ns: observation.timing.measurement_ended_unix_ns + 1,
            cpu_time_ns: 50 / u64::from(nodes) + take_remainder(&mut cpu_remainder),
            peak_rss_bytes: 4096 / u64::from(nodes) + take_remainder(&mut rss_remainder),
            network_rx_bytes: 128 / u64::from(nodes) + take_remainder(&mut rx_remainder),
            network_tx_bytes: 64 / u64::from(nodes) + take_remainder(&mut tx_remainder),
        });
    }
    TopologyEvidence {
        deployment_mode: TopologyDeploymentMode::RemoteFormal,
        data_nodes,
        resource_samples,
    }
}

fn take_remainder(remainder: &mut u64) -> u64 {
    if *remainder == 0 {
        0
    } else {
        *remainder -= 1;
        1
    }
}

fn ablation_counters(label: &str) -> AblationCounters {
    let mut counters = AblationCounters::default();
    match label {
        "no_native_pushdown" => counters.canonical_residual_scans = 1,
        "no_column_batch" => counters.row_column_conversion_boundaries = 1,
        "no_lazy_pages" => counters.eager_page_collections = 1,
        "no_parallel_fanout" => counters.serial_shard_opens = 1,
        "no_batched_gather" => counters.singleton_property_gather_reads = 1,
        _ => {}
    }
    counters
}

fn formal_observations(manifest: &ArtifactManifest) -> Vec<RawObservation> {
    manifest
        .shuffled_cells()
        .expect("formal cells")
        .into_iter()
        .enumerate()
        .map(|(sequence, cell)| {
            let workload = manifest
                .workloads
                .iter()
                .find(|workload| workload.manifest.workload_id == cell.key.workload)
                .expect("cell workload");
            let base = 1_000_000_000_000_u64 + sequence as u64 * 100_000_000_000;
            let operations = 1;
            let mut observation = RawObservation {
                schema_version: SCHEMA_VERSION,
                run_id: manifest.run.run_id.clone(),
                path: cell.path,
                backend: cell.key.backend,
                workload: cell.key.workload.clone(),
                dataset_digest: manifest.dataset.content_digest.clone(),
                workload_digest: workload.manifest.digest.clone(),
                snapshot: workload.snapshot.clone(),
                parameters_digest: json_digest(&workload.manifest.parameters),
                topology: Topology {
                    data_nodes: cell.key.data_nodes,
                },
                concurrency: cell.key.concurrency,
                ablation: cell.key.ablation.clone(),
                repetition: cell.repetition,
                timing: TimingBoundaries {
                    warmup_started_unix_ns: base,
                    measurement_started_unix_ns: base + FORMAL_WARMUP_SECONDS * 1_000_000_000,
                    measurement_ended_unix_ns: base
                        + (FORMAL_WARMUP_SECONDS + FORMAL_MEASUREMENT_SECONDS) * 1_000_000_000,
                },
                operations,
                errors: 0,
                samples: SampleSeries {
                    latency_ns: vec![10],
                    ttfr_ns: vec![5],
                },
                resources: ResourceMetrics {
                    cpu_time_ns: ResourceMetric::Observed { value: 50 },
                    peak_rss_bytes: ResourceMetric::Observed { value: 4096 },
                    network_rx_bytes: ResourceMetric::Observed { value: 128 },
                    network_tx_bytes: ResourceMetric::Observed { value: 64 },
                },
                resource_scope: if cell.path == ExperimentPath::Proxy {
                    ResourceScope::DataNodeProcesses
                } else {
                    ResourceScope::ClientProcess
                },
                topology_evidence: None,
                identity: ResultIdentity {
                    digest: digest('d'),
                    row_count: 1,
                },
                configuration_digest: manifest.run.configuration_digest.clone(),
                ablation_evidence: (cell.path == ExperimentPath::Proxy).then(|| AblationEvidence {
                    gateway_pid: 42,
                    cell_id: format!("{}:{sequence}", manifest.run.run_id),
                    configuration_digest: manifest.run.configuration_digest.clone(),
                    config: ablation_config_for_label(&cell.key.ablation).expect("known ablation"),
                    total_operations: operations,
                    queries_started: operations,
                    queries_completed: operations,
                    queries_failed: 0,
                    queries_in_flight: 0,
                    counters: ablation_counters(&cell.key.ablation),
                }),
            };
            if cell.path == ExperimentPath::Proxy {
                observation.topology_evidence = Some(formal_topology_evidence(&observation));
            }
            observation
        })
        .collect()
}

fn tiny_spec(run_id: &str) -> ExperimentSpec {
    let mut spec = formal_spec(run_id, Backend::Rocksdb);
    spec.environment = synthetic_environment();
    spec.workloads = vec![workload_case()];
    spec.dataset.vertex_count = 100;
    spec.dataset.edge_count = 500;
    spec.dataset.temporal_update_count = 60;
    spec.matrix = ExperimentMatrix {
        suites: vec![ExperimentSuite {
            kind: ExperimentSuiteKind::Comparison,
            backends: vec![Backend::Rocksdb],
            paths: comparison_paths(),
            workloads: vec!["point_lookup".into()],
            data_nodes: vec![1],
            concurrencies: vec![1],
            ablations: vec!["production".into()],
            workload_ablations: BTreeMap::new(),
        }],
    };
    spec.protocol.warmup_seconds = 1;
    spec.protocol.measurement_seconds = 1;
    spec.protocol.repetitions = 2;
    spec
}

#[test]
fn formal_environment_fingerprint_is_sealed_into_run_identity() {
    let manifest = formal_spec("sealed-environment", Backend::Rocksdb)
        .into_manifest(false)
        .expect("formal manifest with captured environment");

    assert_eq!(manifest.run.mode, RunMode::Formal);
    assert_eq!(manifest.run.environment_digest, manifest.environment.digest);

    let mut changed = formal_spec("sealed-environment", Backend::Rocksdb);
    let original_configuration = changed
        .clone()
        .into_manifest(false)
        .expect("original manifest")
        .run
        .configuration_digest;
    changed.environment.cpu_model = "Intel Xeon Gold 6338 CPU @ 2.00GHz".into();
    changed.environment.digest = changed.environment.computed_digest().unwrap();
    let changed_configuration = changed
        .into_manifest(false)
        .expect("changed environment manifest")
        .run
        .configuration_digest;

    assert_ne!(original_configuration, changed_configuration);
}

#[test]
fn formal_environment_rejects_missing_placeholder_and_synthetic_values() {
    let mut missing_version = formal_spec("missing-version", Backend::Rocksdb);
    missing_version.environment.versions.postgresql.clear();
    missing_version.environment.digest = missing_version.environment.computed_digest().unwrap();
    assert!(missing_version.into_manifest(false).is_err());

    let mut placeholder_version = formal_spec("placeholder-version", Backend::Rocksdb);
    placeholder_version.environment.versions.neo4j = "unknown".into();
    placeholder_version.environment.digest =
        placeholder_version.environment.computed_digest().unwrap();
    assert!(placeholder_version.into_manifest(false).is_err());

    let mut malformed_binary_digest = formal_spec("malformed-binary-digest", Backend::Rocksdb);
    malformed_binary_digest.environment.binaries.gateway_sha256 = "not-sha256".into();
    malformed_binary_digest.environment.digest = malformed_binary_digest
        .environment
        .computed_digest()
        .unwrap();
    assert!(malformed_binary_digest.into_manifest(false).is_err());

    let mut synthetic = formal_spec("synthetic-formal", Backend::Rocksdb);
    synthetic.environment = synthetic_environment();
    assert!(synthetic.into_manifest(false).is_err());
}

#[test]
fn diagnostic_simulator_seals_an_explicit_synthetic_environment() {
    let root = unique_temp("synthetic-environment");
    fs::create_dir(&root).expect("temporary output root");

    let artifact = run_experiment(tiny_spec("synthetic-run"), &root, true, None)
        .expect("diagnostic simulator artifact");
    let manifest: ArtifactManifest =
        serde_json::from_slice(&fs::read(artifact.join("manifest.json")).expect("manifest bytes"))
            .expect("artifact manifest");

    assert_eq!(
        manifest.environment.source,
        EnvironmentFingerprintSource::Synthetic
    );
    assert_eq!(manifest.run.environment_digest, manifest.environment.digest);
    manifest.validate().expect("sealed synthetic manifest");
    fs::remove_dir_all(root).expect("remove test output");
}

#[test]
fn executor_digest_mismatch_is_rejected_before_artifact_creation() {
    let root = unique_temp("executor-digest-mismatch");
    fs::create_dir(&root).expect("temporary output root");
    let executor = root.join("cell-executor");
    fs::write(&executor, b"not the sealed executor").expect("fake executor");
    let mut spec = tiny_spec("mismatched-executor");
    spec.environment = captured_environment();

    let error = run_experiment(spec, &root, false, Some(&executor))
        .expect_err("unsealed executor must be rejected");

    assert!(error.to_string().contains("executor SHA-256 mismatch"));
    assert!(!root.join("mismatched-executor").exists());
    fs::remove_dir_all(root).expect("remove test output");
}

fn formal_matrix(selected_backend: Backend) -> ExperimentMatrix {
    let backends = vec![selected_backend];
    let workloads = vec!["point_lookup".into()];
    let ablation_workloads = vec![
        "native_pushdown_filter".into(),
        "column_batch_scan".into(),
        "lazy_paged_scan".into(),
        "parallel_fanout_count".into(),
        "batched_expand_gather".into(),
    ];
    ExperimentMatrix {
        suites: vec![
            ExperimentSuite {
                kind: ExperimentSuiteKind::Comparison,
                backends: backends.clone(),
                paths: comparison_paths(),
                workloads: workloads.clone(),
                data_nodes: vec![1],
                concurrencies: FORMAL_CONCURRENCIES.to_vec(),
                ablations: vec!["production".into()],
                workload_ablations: BTreeMap::new(),
            },
            ExperimentSuite {
                kind: ExperimentSuiteKind::Scale,
                backends: backends.clone(),
                paths: vec![ExperimentPath::Proxy],
                workloads: vec!["partition_parallel_scan".into()],
                data_nodes: FORMAL_DATA_NODES.to_vec(),
                concurrencies: FORMAL_CONCURRENCIES.to_vec(),
                ablations: vec!["production".into()],
                workload_ablations: BTreeMap::new(),
            },
            ExperimentSuite {
                kind: ExperimentSuiteKind::Ablation,
                backends,
                paths: vec![ExperimentPath::Proxy],
                workloads: ablation_workloads,
                data_nodes: vec![8],
                concurrencies: vec![32],
                ablations: vec![
                    "production".into(),
                    "no_native_pushdown".into(),
                    "no_column_batch".into(),
                    "no_lazy_pages".into(),
                    "no_parallel_fanout".into(),
                    "no_batched_gather".into(),
                ],
                workload_ablations: BTreeMap::from([
                    (
                        "native_pushdown_filter".into(),
                        vec!["production".into(), "no_native_pushdown".into()],
                    ),
                    (
                        "column_batch_scan".into(),
                        vec!["production".into(), "no_column_batch".into()],
                    ),
                    (
                        "lazy_paged_scan".into(),
                        vec!["production".into(), "no_lazy_pages".into()],
                    ),
                    (
                        "parallel_fanout_count".into(),
                        vec!["production".into(), "no_parallel_fanout".into()],
                    ),
                    (
                        "batched_expand_gather".into(),
                        vec!["production".into(), "no_batched_gather".into()],
                    ),
                ]),
            },
        ],
    }
}

#[test]
fn formal_manifest_accepts_only_the_selected_backend() {
    let manifest = formal_spec("backend-identity", Backend::Rocksdb)
        .into_manifest(false)
        .expect("one-backend formal manifest");
    manifest.validate().expect("selected backend manifest");
    assert_eq!(manifest.selected_backend, Backend::Rocksdb);
    assert_eq!(manifest.run.selected_backend, Backend::Rocksdb);
    let postgresql = formal_spec("backend-identity", Backend::Postgresql)
        .into_manifest(false)
        .expect("PostgreSQL formal manifest");
    assert_ne!(
        manifest.run.configuration_digest,
        postgresql.run.configuration_digest
    );

    let mut invalid = manifest.clone();
    invalid.matrix.suites[0].backends = vec![Backend::Postgresql];
    assert!(
        invalid
            .validate()
            .unwrap_err()
            .to_string()
            .contains("selected_backend")
    );
}

#[test]
fn formal_mode_requires_every_fixed_default_and_simulator_is_never_formal() {
    let exact = formal_spec("formal-defaults", Backend::Rocksdb);
    assert_eq!(derive_run_mode(&exact, false), RunMode::Formal);
    assert_eq!(derive_run_mode(&exact, true), RunMode::Diagnostic);

    let mutations: Vec<SpecMutation> = vec![
        Box::new(|spec| spec.dataset.vertex_count -= 1),
        Box::new(|spec| spec.dataset.edge_count -= 1),
        Box::new(|spec| spec.dataset.temporal_update_count -= 1),
        Box::new(|spec| {
            spec.matrix
                .suites
                .retain(|suite| suite.kind != ExperimentSuiteKind::Scale)
        }),
        Box::new(|spec| spec.matrix.suites[0].concurrencies = vec![1, 8, 32]),
        Box::new(|spec| spec.protocol.warmup_seconds -= 1),
        Box::new(|spec| spec.protocol.measurement_seconds -= 1),
        Box::new(|spec| spec.protocol.repetitions -= 1),
    ];
    for mutate in mutations {
        let mut changed = formal_spec("changed-default", Backend::Rocksdb);
        mutate(&mut changed);
        assert_eq!(derive_run_mode(&changed, false), RunMode::Diagnostic);
    }

    let mut six_repetitions = formal_spec("six-repetitions", Backend::Rocksdb);
    six_repetitions.protocol.repetitions = 6;
    let manifest = six_repetitions
        .into_manifest(false)
        .expect("changed repetition count must remain a valid diagnostic");
    assert_eq!(manifest.run.mode, RunMode::Diagnostic);
}

#[test]
fn formal_mode_rejects_cross_axis_workload_ablation_mappings() {
    let mut spec = formal_spec("cross-axis-mapping", Backend::Rocksdb);
    let ablation = spec
        .matrix
        .suites
        .iter_mut()
        .find(|suite| suite.kind == ExperimentSuiteKind::Ablation)
        .unwrap();
    ablation.workload_ablations.insert(
        "native_pushdown_filter".into(),
        vec!["production".into(), "no_column_batch".into()],
    );
    ablation.workload_ablations.insert(
        "column_batch_scan".into(),
        vec!["production".into(), "no_native_pushdown".into()],
    );
    assert_eq!(derive_run_mode(&spec, false), RunMode::Diagnostic);
}

#[test]
fn matrix_shuffle_is_deterministic_and_cell_key_excludes_path_and_repetition() {
    let manifest: ArtifactManifest = tiny_spec("shuffle")
        .into_manifest(true)
        .expect("diagnostic manifest");
    let first = manifest
        .matrix
        .shuffled_cells(manifest.run.repetitions, 123)
        .expect("first shuffle");
    let second = manifest
        .matrix
        .shuffled_cells(manifest.run.repetitions, 123)
        .expect("second shuffle");
    let different = manifest
        .matrix
        .shuffled_cells(manifest.run.repetitions, 124)
        .expect("different shuffle");
    assert_eq!(first, second);
    assert_ne!(first, different);
    assert_eq!(first[0].key, first[0].key.clone());
    assert!(first.iter().any(|cell| {
        first.iter().any(|other| {
            cell.key == other.key
                && (cell.path != other.path || cell.repetition != other.repetition)
        })
    }));
}

#[test]
fn layered_matrix_generates_only_semantically_meaningful_cells() {
    let manifest = formal_spec("layered-cells", Backend::Rocksdb)
        .into_manifest(false)
        .expect("formal layered manifest");
    let cells = manifest.shuffled_cells().expect("layered cells");

    assert!(
        cells
            .iter()
            .filter(|cell| cell.path != ExperimentPath::Proxy)
            .all(|cell| { cell.key.data_nodes == 1 && cell.key.ablation == "production" })
    );
    assert!(
        cells
            .iter()
            .filter(|cell| cell.key.data_nodes != 1)
            .all(|cell| { cell.path == ExperimentPath::Proxy })
    );
    assert!(
        cells
            .iter()
            .filter(|cell| cell.key.ablation != "production")
            .all(|cell| {
                cell.path == ExperimentPath::Proxy
                    && cell.key.data_nodes == 8
                    && cell.key.concurrency == 32
            })
    );

    // The comparison, scale, and ablation suites use disjoint workloads.
    let expected = 34 * FORMAL_REPETITIONS as usize;
    assert_eq!(cells.len(), expected);
}

#[test]
fn formal_artifact_rejects_proxy_observation_without_remote_data_node_evidence() {
    let manifest = formal_spec("formal-artifact-topology", Backend::Rocksdb)
        .into_manifest(false)
        .expect("formal manifest");
    let observations = formal_observations(&manifest);
    validate_artifact_observations(&manifest, &observations)
        .expect("remote data-node evidence is valid");

    let proxy_index = observations
        .iter()
        .position(|observation| observation.path == ExperimentPath::Proxy)
        .expect("proxy observation");

    let mut local_scope = observations.clone();
    local_scope[proxy_index].resource_scope = ResourceScope::ClientAndProxyProcesses;
    local_scope[proxy_index].topology_evidence = None;
    assert!(validate_artifact_observations(&manifest, &local_scope).is_err());

    let mut full_system_scope = observations.clone();
    full_system_scope[proxy_index].resource_scope = ResourceScope::FullSystem;
    assert!(validate_artifact_observations(&manifest, &full_system_scope).is_err());

    let mut local_diagnostic = observations;
    local_diagnostic[proxy_index]
        .topology_evidence
        .as_mut()
        .expect("topology evidence")
        .deployment_mode = TopologyDeploymentMode::LocalDiagnostic;
    assert!(validate_artifact_observations(&manifest, &local_diagnostic).is_err());
}

#[test]
fn formal_matrix_rejects_direct_paths_on_scale_or_ablation_suites() {
    for kind in [ExperimentSuiteKind::Scale, ExperimentSuiteKind::Ablation] {
        let mut spec = formal_spec("invalid-suite-path", Backend::Rocksdb);
        let suite = spec
            .matrix
            .suites
            .iter_mut()
            .find(|suite| suite.kind == kind)
            .expect("suite");
        suite.paths.push(ExperimentPath::BackendDirect);
        assert!(spec.into_manifest(false).is_err());
    }
}

#[test]
fn formal_scale_requires_the_bounded_partition_parallel_scan() {
    let spec = formal_spec("suite-workloads", Backend::Rocksdb);
    assert_eq!(
        spec.clone()
            .into_manifest(false)
            .expect("purpose-selected suites")
            .run
            .mode,
        RunMode::Formal
    );

    let mut comparison_only = spec;
    comparison_only
        .matrix
        .suites
        .iter_mut()
        .find(|suite| suite.kind == ExperimentSuiteKind::Scale)
        .expect("scale suite")
        .workloads = vec!["point_lookup".into()];
    assert_eq!(
        derive_run_mode(&comparison_only, false),
        RunMode::Diagnostic
    );
}

#[test]
fn ablation_suite_pairs_each_workload_with_only_its_single_disable() {
    let matrix = formal_matrix(Backend::Rocksdb);
    let cells = matrix.shuffled_cells(1, 17).unwrap();
    let ablation_cells = cells
        .iter()
        .filter(|cell| {
            cell.path == ExperimentPath::Proxy
                && cell.key.data_nodes == 8
                && cell.key.concurrency == 32
                && cell.key.workload != "point_lookup"
                && cell.key.workload != "partition_parallel_scan"
        })
        .collect::<Vec<_>>();
    assert_eq!(ablation_cells.len(), 5 * 2);
    for workload in [
        "native_pushdown_filter",
        "column_batch_scan",
        "lazy_paged_scan",
        "parallel_fanout_count",
        "batched_expand_gather",
    ] {
        let labels = ablation_cells
            .iter()
            .filter(|cell| cell.key.workload == workload)
            .map(|cell| cell.key.ablation.as_str())
            .collect::<BTreeSet<_>>();
        assert_eq!(labels.len(), 2, "{workload}");
        assert!(labels.contains("production"), "{workload}");
    }
}

#[test]
fn simulator_refuses_an_existing_run_id_without_changing_final_files() {
    let root = unique_temp("no-overwrite");
    fs::create_dir(&root).expect("temporary output root");
    let artifact =
        run_experiment(tiny_spec("same-run"), &root, true, None).expect("first diagnostic run");
    let before = fs::read(artifact.join("manifest.json")).expect("first manifest");
    let error = run_experiment(tiny_spec("same-run"), &root, true, None)
        .expect_err("second run must not overwrite");
    assert!(error.to_string().contains("already exists"));
    assert_eq!(
        fs::read(artifact.join("manifest.json")).expect("unchanged manifest"),
        before
    );
    fs::remove_dir_all(root).expect("remove test output");
}

#[test]
fn verifier_rejects_regeneration_inside_the_sealed_artifact_without_mutating_it() {
    let root = unique_temp("internal-regeneration");
    fs::create_dir(&root).expect("temporary output root");
    let artifact =
        run_experiment(tiny_spec("sealed-run"), &root, true, None).expect("diagnostic artifact");
    let checksum_before = fs::read(artifact.join("SHA256SUMS")).expect("checksum before");
    let regeneration = artifact.join("offline-regeneration");

    let error = verify_artifact(&artifact, &regeneration)
        .expect_err("regeneration inside artifact must be rejected");

    assert!(error.to_string().contains("outside the artifact"));
    assert!(!regeneration.exists());
    assert_eq!(
        fs::read(artifact.join("SHA256SUMS")).expect("checksum after"),
        checksum_before
    );
    fs::remove_dir_all(root).expect("remove test output");
}

#[test]
fn verifier_derives_required_files_instead_of_trusting_the_checksum_inventory() {
    let root = unique_temp("missing-required-file");
    fs::create_dir(&root).expect("temporary output root");
    let artifact =
        run_experiment(tiny_spec("missing-log"), &root, true, None).expect("diagnostic artifact");
    fs::remove_file(artifact.join("logs/run.log")).expect("remove required log");
    let checksums = fs::read_to_string(artifact.join("SHA256SUMS")).expect("checksums");
    let resealed = checksums
        .lines()
        .filter(|line| !line.ends_with("  logs/run.log"))
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";
    fs::write(artifact.join("SHA256SUMS"), resealed).expect("reseal incomplete artifact");

    let error = verify_artifact(&artifact, &root.join("regenerated"))
        .expect_err("missing required file must not pass after resealing");

    assert!(error.to_string().contains("required artifact file set"));
    fs::remove_dir_all(root).expect("remove test output");
}

#[test]
fn summary_reports_latency_and_ttfr_statistics_at_repetition_granularity() {
    let root = unique_temp("repetition-statistics");
    fs::create_dir(&root).expect("temporary output root");
    let artifact = run_experiment(tiny_spec("repetition-stats"), &root, true, None)
        .expect("diagnostic artifact");
    let summary: serde_json::Value =
        serde_json::from_slice(&fs::read(artifact.join("summary/summary.json")).expect("summary"))
            .expect("summary json");
    let first = &summary["cells"][0];

    assert_eq!(first["latency_repetitions_ns"]["p50"]["count"], 2);
    assert_eq!(first["latency_repetitions_ns"]["p95"]["count"], 2);
    assert_eq!(first["latency_repetitions_ns"]["p99"]["count"], 2);
    assert_eq!(first["ttfr_repetitions_ns"]["p50"]["count"], 2);
    assert_eq!(first["ttfr_repetitions_ns"]["p95"]["count"], 2);
    assert_eq!(first["ttfr_repetitions_ns"]["p99"]["count"], 2);
    fs::remove_dir_all(root).expect("remove test output");
}

#[cfg(unix)]
#[test]
fn failed_cell_executor_is_called_once_and_never_produces_summary() {
    use std::os::unix::fs::PermissionsExt;

    let root = unique_temp("no-retry");
    fs::create_dir(&root).expect("temporary output root");
    let counter = root.join("calls");
    let executor = root.join("fail-cell.sh");
    fs::write(
        &executor,
        format!("#!/bin/sh\nprintf x >> '{}'\nexit 9\n", counter.display()),
    )
    .expect("write fake executor");
    let mut permissions = fs::metadata(&executor)
        .expect("executor metadata")
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&executor, permissions).expect("make executor executable");

    let mut spec = tiny_spec("failed-run");
    spec.environment = captured_environment();
    spec.environment.binaries.executor_sha256 = sha256_file(&executor).expect("executor digest");
    spec.environment.digest = spec
        .environment
        .computed_digest()
        .expect("environment digest");
    let error = run_experiment(spec, &root, false, Some(&executor))
        .expect_err("the first failed cell must stop the run");
    assert!(error.to_string().contains("no automatic retry"));
    assert_eq!(fs::read(&counter).expect("one executor call"), b"x");
    let partial = root.join("failed-run");
    assert!(!partial.join("summary/summary.json").exists());
    assert!(!partial.join("SHA256SUMS").exists());
    assert!(partial.join("logs/failure.json").is_file());
    assert!(partial.join("PARTIAL_SHA256SUMS").is_file());
    fs::remove_dir_all(root).expect("remove test output");
}

fn unique_temp(label: &str) -> PathBuf {
    let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "dtgproxy-paper-{label}-{}-{sequence}",
        std::process::id()
    ))
}
