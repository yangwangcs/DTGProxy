use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::SocketAddr;
use std::os::unix::fs::FileTypeExt;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode, Output, Stdio};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use adapter_neo4j::Neo4jAdapterFactory;
use adapter_postgres::PostgresAdapter;
use adapter_registry::{AdapterOpenRequest, AdapterRegistry, SecretString};
use adapter_rocksdb::RocksAdapter;
use base64::Engine as _;
use bolt_protocol::{Value as BoltValue, encode as encode_bolt};
use paper_benchmark::{
    AblationConfig, AblationCounters, AblationEvidence, Backend, CellConfig, ExperimentPath,
    HostResourceEvidence, ProcessTopologyEvidence, RawObservation, ResourceMetric, ResourceMetrics,
    ResourceScope, ResultIdentity, SCHEMA_VERSION, SampleSeries, TimingBoundaries, Topology,
    TopologyDeploymentMode, TopologyEvidence, ablation_config_for_label,
};
use postgres::{Client as PostgresClient, NoTls, Statement as PostgresStatement};
use rocksdb::{
    DBWithThreadMode, Direction, IteratorMode, MultiThreaded, Options, SnapshotWithThreadMode,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};
use storage_api::{
    AdapterRequirement, CanonicalScanRequest, KeySpan, Keyspace, QueryPageBounds, ReadSnapshot,
    StorageAdapter,
};
use temporal_storage::{GraphId, current_vertex_graph_prefix};

const USAGE: &str = "Usage: dtgproxy-paper-cell-executor --cell-config FILE --output FILE\n       dtgproxy-paper-cell-executor probe-process --pid PID --network-interface INTERFACE [--executable FILE --listen-address ADDRESS --data-directory DIR]\n       dtgproxy-paper-cell-executor probe-resources --pid PID --network-interface INTERFACE";
const RUNTIME_ENV: &str = "DTGPROXY_PAPER_RUNTIME_MANIFEST";
const PAGE_ITEMS: usize = 4_096;
const PAGE_BYTES: u64 = 16 * 1024 * 1024;

type RocksDb = DBWithThreadMode<MultiThreaded>;

fn main() -> ExitCode {
    let arguments = env::args().skip(1).collect::<Vec<_>>();
    let result = if arguments.first().map(String::as_str) == Some("probe-process") {
        execute_probe_process(arguments.into_iter().skip(1).collect())
    } else if arguments.first().map(String::as_str) == Some("probe-resources") {
        execute_probe_resources(arguments.into_iter().skip(1).collect())
    } else {
        execute(arguments)
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(ExecutorError::Usage(message)) => {
            eprintln!("{message}\n{USAGE}");
            ExitCode::from(2)
        }
        Err(ExecutorError::Unavailable(message)) => {
            eprintln!("paper cell unavailable: {message}");
            ExitCode::from(3)
        }
        Err(ExecutorError::Failed(message)) => {
            eprintln!("paper cell executor failed: {message}");
            ExitCode::FAILURE
        }
    }
}

fn execute_probe_resources(arguments: Vec<String>) -> Result<(), ExecutorError> {
    let mut options = parse_options(arguments)?;
    let pid = required_option(&mut options, "--pid")?
        .parse::<u32>()
        .map_err(|_| ExecutorError::Usage("--pid must be a positive integer".into()))?;
    if pid <= 1 {
        return Err(ExecutorError::Usage(
            "--pid must identify a non-system process".into(),
        ));
    }
    let network_interface = required_option(&mut options, "--network-interface")?;
    validate_remote_token(&network_interface)?;
    if !options.is_empty() {
        return Err(ExecutorError::Usage(format!(
            "unknown options: {}",
            options.keys().cloned().collect::<Vec<_>>().join(", ")
        )));
    }
    println!(
        "{}",
        serde_json::to_string(&collect_resource_probe(pid, network_interface)?).map_err(failed)?
    );
    Ok(())
}

fn collect_resource_probe(
    pid: u32,
    network_interface: String,
) -> Result<ResourceProbeSnapshot, ExecutorError> {
    let (rss_bytes, _) = process_rss(pid)?;
    let (network_rx_bytes, network_tx_bytes) = interface_network_bytes(&network_interface)?;
    Ok(ResourceProbeSnapshot {
        schema_version: SCHEMA_VERSION,
        sampled_unix_ns: unix_time_ns()?,
        process_start_id: process_start_identity(pid)?,
        pid,
        cpu_time_ns: aggregate_cpu_time_ns(&[pid])?,
        rss_bytes,
        network_interface,
        network_rx_bytes,
        network_tx_bytes,
    })
}

fn execute(arguments: Vec<String>) -> Result<(), ExecutorError> {
    let options = OptionsMap::parse(arguments)?;
    if options.output.exists() {
        return Err(ExecutorError::failed(format!(
            "output already exists: {}",
            options.output.display()
        )));
    }
    let config: CellConfig = read_json(&options.cell_config, "cell config")?;
    validate_cell_config(&config)?;
    let runtime_path = env::var_os(RUNTIME_ENV)
        .map(PathBuf::from)
        .ok_or_else(|| ExecutorError::failed(format!("{RUNTIME_ENV} is required")))?;
    let runtime: RuntimeManifest = read_json(&runtime_path, "runtime manifest")?;
    let workload = runtime.bind(&config)?;

    let observation = match config.cell.path {
        ExperimentPath::BackendDirect => execute_backend_direct(&config, &runtime, workload)?,
        ExperimentPath::AdapterDirect => execute_adapter_direct(&config, &runtime, workload)?,
        ExperimentPath::Proxy => execute_proxy(&config, &runtime)?,
    };
    observation
        .validate()
        .map_err(|error| ExecutorError::failed(format!("invalid RawObservation: {error}")))?;
    write_new_atomic(
        &options.output,
        &serde_json::to_vec_pretty(&observation).map_err(failed)?,
    )
}

fn execute_probe_process(arguments: Vec<String>) -> Result<(), ExecutorError> {
    let mut options = parse_options(arguments)?;
    let pid = required_option(&mut options, "--pid")?
        .parse::<u32>()
        .map_err(|_| ExecutorError::Usage("--pid must be a positive integer".into()))?;
    if pid <= 1 {
        return Err(ExecutorError::Usage(
            "--pid must identify a non-system process".into(),
        ));
    }
    let network_interface = required_option(&mut options, "--network-interface")?;
    validate_remote_token(&network_interface)?;
    let claimed_executable = options.remove("--executable").map(PathBuf::from);
    let listen_address = options.remove("--listen-address").unwrap_or_default();
    let data_directory = options
        .remove("--data-directory")
        .map(PathBuf::from)
        .unwrap_or_default();
    if !options.is_empty() {
        return Err(ExecutorError::Usage(format!(
            "unknown options: {}",
            options.keys().cloned().collect::<Vec<_>>().join(", ")
        )));
    }

    let actual_executable = process_executable(pid)?;
    let executable = if let Some(claimed) = claimed_executable {
        let actual = fs::canonicalize(&actual_executable).map_err(failed)?;
        let expected = fs::canonicalize(&claimed).map_err(failed)?;
        if actual != expected {
            return Err(ExecutorError::failed(format!(
                "PID {pid} executable does not match {}",
                claimed.display()
            )));
        }
        claimed
    } else {
        actual_executable
    };
    if !listen_address.is_empty() {
        verify_process_listener(pid, &listen_address)?;
    }
    if !data_directory.as_os_str().is_empty() {
        verify_process_data_directory(pid, &data_directory)?;
    }
    let probe_binary = env::current_exe().map_err(failed)?;
    let executable_sha256 = paper_benchmark::sha256_file(&executable).map_err(failed)?;
    let probe_binary_sha256 = paper_benchmark::sha256_file(&probe_binary).map_err(failed)?;
    let resources = collect_resource_probe(pid, network_interface)?;
    let peak_rss_bytes = process_rss(pid)?.1.max(resources.rss_bytes);
    let snapshot = ProcessProbeSnapshot {
        schema_version: SCHEMA_VERSION,
        sampled_unix_ns: resources.sampled_unix_ns,
        host_id: stable_host_id()?,
        boot_id: boot_identity()?,
        process_start_id: resources.process_start_id,
        pid,
        executable_sha256,
        executable,
        probe_binary_sha256,
        listen_address,
        data_directory,
        cpu_time_ns: resources.cpu_time_ns,
        rss_bytes: resources.rss_bytes,
        peak_rss_bytes,
        network_interface: resources.network_interface,
        network_rx_bytes: resources.network_rx_bytes,
        network_tx_bytes: resources.network_tx_bytes,
    };
    println!("{}", serde_json::to_string(&snapshot).map_err(failed)?);
    Ok(())
}

fn execute_backend_direct(
    config: &CellConfig,
    runtime: &RuntimeManifest,
    workload: &RuntimeWorkload,
) -> Result<RawObservation, ExecutorError> {
    require_production_direct(config)?;
    let measurement = match config.cell.key.backend {
        Backend::Rocksdb => measure_rocksdb_backend_direct(config, runtime, workload),
        Backend::Postgresql => measure_postgres_backend_direct(config, runtime, workload),
        Backend::Neo4j => measure_neo4j_backend_direct(config, runtime, workload),
    }?;
    let resources = measurement.resources;
    let measurement = measurement.measurement;
    observation(config, measurement, resources)
}

fn execute_adapter_direct(
    config: &CellConfig,
    runtime: &RuntimeManifest,
    workload: &RuntimeWorkload,
) -> Result<RawObservation, ExecutorError> {
    require_production_direct(config)?;
    let prefix = workload.vertex_prefix()?;
    let adapters = open_production_adapters(config, runtime)?;
    let workers = prepare_adapter_workers(&adapters, config.cell.key.concurrency)?;
    let resources_before = process_resources(&[std::process::id()])?;
    let measurement = measure_prepared_workers(
        workers,
        &prefix,
        config.protocol.warmup_seconds,
        config.protocol.measurement_seconds,
        |snapshots, prefix| count_adapter_snapshots(snapshots, prefix),
    )?;
    let resources = process_resources_after(
        &[std::process::id()],
        resources_before,
        config.cell.key.backend == Backend::Rocksdb,
    )?;
    observation(config, measurement, resources)
}

fn execute_proxy(
    config: &CellConfig,
    runtime: &RuntimeManifest,
) -> Result<RawObservation, ExecutorError> {
    let target = runtime.proxy_target(config)?;
    let topology = target.validate(config.cell.key.data_nodes)?;
    let cell_id = format!("{}:{}", config.run_id, config.sequence);
    let ablation_config = ablation_config_for_label(&config.cell.key.ablation).map_err(failed)?;
    let report_path = unique_loadgen_report_path(config)?;
    let mut resource_pids = topology.local_pids.clone();
    resource_pids.push(std::process::id());
    let mut resources_before = proxy_resources_before(target, &resource_pids)?;
    let address = target
        .bolt_address
        .parse::<SocketAddr>()
        .map_err(|_| ExecutorError::failed("invalid Proxy Bolt address"))?;
    let begun = gateway_begin_cell(
        target,
        &cell_id,
        &config.configuration_digest,
        ablation_config,
    )?;
    let loadgen_result = (|| {
        let mut child = Command::new(&target.bolt_loadgen_binary)
            .args(["--address", &address.to_string()])
            .args(["--query", &config.query])
            .args(["--connections", &config.cell.key.concurrency.to_string()])
            .args([
                "--warmup-seconds",
                &config.protocol.warmup_seconds.to_string(),
            ])
            .args([
                "--duration-seconds",
                &config.protocol.measurement_seconds.to_string(),
            ])
            .args(["--timeout-ms", &target.timeout_ms.to_string()])
            .args(["--benchmark-session", &begun.session_token])
            .args(["--output", report_path.to_str().ok_or_else(non_utf8_path)?])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|error| {
                ExecutorError::failed(format!(
                    "failed to start persistent Bolt loadgen {}: {error}",
                    target.bolt_loadgen_binary.display()
                ))
            })?;
        loop {
            if child.try_wait().map_err(failed)?.is_some() {
                break;
            }
            thread::sleep(Duration::from_secs(1));
            sample_remote_resources(target, &mut resources_before)?;
        }
        let output = child.wait_with_output().map_err(failed)?;
        if !output.status.success() {
            return Err(ExecutorError::failed(format!(
                "persistent Bolt loadgen failed without retry: stdout={} stderr={}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            )));
        }
        let report_bytes = fs::read(&report_path).map_err(|error| {
            ExecutorError::failed(format!(
                "persistent Bolt loadgen did not create {}: {error}",
                report_path.display()
            ))
        })?;
        let report: BoltLoadgenReport = serde_json::from_slice(&report_bytes).map_err(|error| {
            ExecutorError::failed(format!("invalid persistent Bolt loadgen report: {error}"))
        })?;
        report.validate(config)?;
        Ok((report, report_bytes))
    })();
    let _ = fs::remove_file(&report_path);
    let finish_result = gateway_finish_cell(target, &begun.session_token);
    let (report, report_bytes, evidence) = match (loadgen_result, finish_result) {
        (Ok((report, report_bytes)), Ok(finished)) => {
            let evidence = match finished.into_evidence(
                target,
                &cell_id,
                &config.configuration_digest,
                ablation_config,
                report.total_operations,
            ) {
                Ok(evidence) => evidence,
                Err(error) => {
                    gateway_abort_cell_best_effort(target, &begun.session_token);
                    return Err(error);
                }
            };
            (report, report_bytes, evidence)
        }
        (Err(error), _) => {
            gateway_abort_cell_best_effort(target, &begun.session_token);
            return Err(error);
        }
        (Ok(_), Err(error)) => {
            gateway_abort_cell_best_effort(target, &begun.session_token);
            return Err(error);
        }
    };
    let measured_resources =
        proxy_resources_after(target, &resource_pids, resources_before, &report)?;
    let observation = proxy_observation(
        config,
        report,
        measured_resources.resources,
        measured_resources.resource_scope,
        measured_resources.topology_evidence,
        evidence,
    )?;
    if let Err(error) = observation.validate() {
        gateway_abort_cell_best_effort(target, &begun.session_token);
        return Err(ExecutorError::failed(format!(
            "invalid Gateway ablation evidence: {error}"
        )));
    }
    let data_node_processes = target
        .data_node_processes
        .iter()
        .map(|process| {
            let mut evidence = json!({
                "pid": process.common.pid,
                "executable": process.common.executable,
                "listen_address": process.common.listen_address,
                "data_directory": process.data_directory
            });
            if let Some(object) = evidence.as_object_mut() {
                if let Some(digest) = &process.executable_sha256 {
                    object.insert("executable_sha256".into(), json!(digest));
                }
                if let Some(probe) = &process.probe {
                    object.insert(
                        "probe".into(),
                        json!({
                            "ssh_target": probe.ssh_target,
                            "host_id": probe.host_id,
                            "boot_id": probe.boot_id,
                            "probe_binary": probe.probe_binary,
                            "probe_binary_sha256": probe.probe_binary_sha256,
                            "data_interface": probe.data_interface,
                            "management_interface": probe.management_interface
                        }),
                    );
                }
            }
            evidence
        })
        .collect::<Vec<_>>();
    println!(
        "{}",
        serde_json::to_string(&json!({
            "event": "paper_proxy_topology_verified",
            "deployment_mode": target.deployment_mode,
            "backend": config.cell.key.backend,
            "data_nodes": config.cell.key.data_nodes,
            "data_node_pids": topology.data_node_pids,
            "data_node_processes": data_node_processes,
            "gateway_pid": target.gateway_process.pid,
            "gateway_process": {
                "pid": target.gateway_process.pid,
                "executable": target.gateway_process.executable,
                "listen_address": target.gateway_process.listen_address
            },
            "bolt_loadgen_binary": target.bolt_loadgen_binary,
            "bolt_loadgen_blake3": blake3_file(&target.bolt_loadgen_binary)?,
            "loadgen_report_blake3": blake3::hash(&report_bytes).to_hex().to_string()
        }))
        .map_err(failed)?
    );
    Ok(observation)
}

fn observation(
    config: &CellConfig,
    measurement: Measurement,
    resources: ResourceMetrics,
) -> Result<RawObservation, ExecutorError> {
    Ok(RawObservation {
        schema_version: SCHEMA_VERSION,
        run_id: config.run_id.clone(),
        path: config.cell.path,
        backend: config.cell.key.backend,
        workload: config.cell.key.workload.clone(),
        dataset_digest: config.dataset_digest.clone(),
        workload_digest: config.workload_digest.clone(),
        snapshot: config.snapshot.clone(),
        parameters_digest: config.parameters_digest.clone(),
        topology: Topology {
            data_nodes: config.cell.key.data_nodes,
        },
        concurrency: config.cell.key.concurrency,
        ablation: config.cell.key.ablation.clone(),
        repetition: config.cell.repetition,
        timing: measurement.timing,
        operations: measurement.latency_ns.len() as u64,
        errors: 0,
        samples: SampleSeries {
            ttfr_ns: measurement.latency_ns.clone(),
            latency_ns: measurement.latency_ns,
        },
        resources,
        resource_scope: ResourceScope::ClientProcess,
        topology_evidence: None,
        identity: count_identity(measurement.count)?,
        configuration_digest: config.configuration_digest.clone(),
        ablation_evidence: None,
    })
}

fn proxy_observation(
    config: &CellConfig,
    report: BoltLoadgenReport,
    resources: ResourceMetrics,
    resource_scope: ResourceScope,
    topology_evidence: Option<TopologyEvidence>,
    ablation_evidence: AblationEvidence,
) -> Result<RawObservation, ExecutorError> {
    Ok(RawObservation {
        schema_version: SCHEMA_VERSION,
        run_id: config.run_id.clone(),
        path: ExperimentPath::Proxy,
        backend: config.cell.key.backend,
        workload: config.cell.key.workload.clone(),
        dataset_digest: config.dataset_digest.clone(),
        workload_digest: config.workload_digest.clone(),
        snapshot: config.snapshot.clone(),
        parameters_digest: config.parameters_digest.clone(),
        topology: Topology {
            data_nodes: config.cell.key.data_nodes,
        },
        concurrency: config.cell.key.concurrency,
        ablation: config.cell.key.ablation.clone(),
        repetition: config.cell.repetition,
        timing: TimingBoundaries {
            warmup_started_unix_ns: report.warmup_started_unix_ns,
            measurement_started_unix_ns: report.measurement_started_unix_ns,
            measurement_ended_unix_ns: report.measurement_ended_unix_ns,
        },
        operations: u64::try_from(report.completed_operations)
            .map_err(|_| ExecutorError::failed("Proxy operation count overflow"))?,
        errors: 0,
        samples: SampleSeries {
            latency_ns: report.total_latency_samples_ns,
            ttfr_ns: report.ttfr_samples_ns,
        },
        resources,
        resource_scope,
        topology_evidence,
        identity: ResultIdentity {
            digest: report.result_digest,
            row_count: u64::try_from(report.row_count)
                .map_err(|_| ExecutorError::failed("Proxy row count overflow"))?,
        },
        configuration_digest: config.configuration_digest.clone(),
        ablation_evidence: Some(ablation_evidence),
    })
}

fn measure_rocksdb_backend_direct(
    config: &CellConfig,
    runtime: &RuntimeManifest,
    workload: &RuntimeWorkload,
) -> Result<MeasurementWithResources, ExecutorError> {
    let paths = runtime.rocks_paths(config.cell.key.backend)?;
    let prefix = workload.vertex_prefix()?;
    let databases = paths
        .iter()
        .map(open_native_rocks)
        .collect::<Result<Vec<_>, _>>()?;
    let worker_count = worker_count(config.cell.key.concurrency)?;
    let workers = (0..worker_count)
        .map(|_| databases.iter().map(RocksDb::snapshot).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    let resources_before = process_resources(&[std::process::id()])?;
    let measurement = measure_prepared_workers(
        workers,
        &prefix,
        config.protocol.warmup_seconds,
        config.protocol.measurement_seconds,
        |snapshots, prefix| count_native_snapshots(&databases, snapshots, prefix),
    )?;
    let resources = process_resources_after(&[std::process::id()], resources_before, true)?;
    Ok(MeasurementWithResources {
        measurement,
        resources,
    })
}

fn measure_postgres_backend_direct(
    config: &CellConfig,
    runtime: &RuntimeManifest,
    workload: &RuntimeWorkload,
) -> Result<MeasurementWithResources, ExecutorError> {
    let backend = runtime.postgres_runtime(config.cell.key.backend)?;
    let workers =
        prepare_postgres_native_workers(backend, workload.graph_id, config.cell.key.concurrency)?;
    let resources_before = process_resources(&[std::process::id()])?;
    let measurement = measure_prepared_workers(
        workers,
        &[],
        config.protocol.warmup_seconds,
        config.protocol.measurement_seconds,
        |worker, _| worker.count(),
    )?;
    let resources = process_resources_after(&[std::process::id()], resources_before, false)?;
    Ok(MeasurementWithResources {
        measurement,
        resources,
    })
}

fn measure_neo4j_backend_direct(
    config: &CellConfig,
    runtime: &RuntimeManifest,
    workload: &RuntimeWorkload,
) -> Result<MeasurementWithResources, ExecutorError> {
    let backend = runtime.neo4j_runtime(config.cell.key.backend)?;
    let workers =
        prepare_neo4j_native_workers(backend, workload.graph_id, config.cell.key.concurrency)?;
    let resources_before = process_resources(&[std::process::id()])?;
    let measurement = measure_prepared_workers(
        workers,
        &[],
        config.protocol.warmup_seconds,
        config.protocol.measurement_seconds,
        |worker, _| worker.count(),
    )?;
    let resources = process_resources_after(&[std::process::id()], resources_before, false)?;
    Ok(MeasurementWithResources {
        measurement,
        resources,
    })
}

fn open_production_adapters(
    config: &CellConfig,
    runtime: &RuntimeManifest,
) -> Result<Vec<Arc<dyn StorageAdapter>>, ExecutorError> {
    match config.cell.key.backend {
        Backend::Rocksdb => runtime
            .rocks_paths(Backend::Rocksdb)?
            .iter()
            .map(|path| {
                RocksAdapter::open(path)
                    .map(|adapter| Arc::new(adapter) as Arc<dyn StorageAdapter>)
                    .map_err(|error| {
                        ExecutorError::failed(format!(
                            "failed to open production RocksDB Adapter at {}: {error}",
                            path.display()
                        ))
                    })
            })
            .collect(),
        Backend::Postgresql => {
            let backend = runtime.postgres_runtime(Backend::Postgresql)?;
            if backend.pool_size < worker_count(config.cell.key.concurrency)? {
                return Err(ExecutorError::failed(
                    "PostgreSQL pool_size must cover Adapter Direct concurrency",
                ));
            }
            let adapter = PostgresAdapter::open(
                &backend.connection_string,
                &backend.instance_id,
                backend.pool_size,
            )
            .map_err(|error| {
                ExecutorError::failed(format!(
                    "failed to open production PostgreSQL Adapter: {error}"
                ))
            })?;
            Ok(vec![Arc::new(adapter) as Arc<dyn StorageAdapter>])
        }
        Backend::Neo4j => {
            let backend = runtime.neo4j_runtime(Backend::Neo4j)?;
            let mut registry = AdapterRegistry::new();
            registry
                .register(Arc::new(Neo4jAdapterFactory))
                .map_err(failed)?;
            let request = AdapterOpenRequest::new(&backend.instance_id)
                .with_parameter("endpoint", &backend.endpoint)
                .with_parameter("database", &backend.database)
                .with_parameter("username", &backend.username)
                .with_parameter("timeout_seconds", backend.timeout_seconds.to_string())
                .with_secret("password", SecretString::new(&backend.password));
            let opened =
                block_on(registry.open("neo4j", &request, AdapterRequirement::HotPluggableReplica))
                    .map_err(|error| {
                        ExecutorError::failed(format!(
                            "failed to open production Neo4j Adapter: {error}"
                        ))
                    })?;
            Ok(vec![opened.into_adapter()])
        }
    }
}

fn prepare_adapter_workers<'a>(
    adapters: &'a [Arc<dyn StorageAdapter>],
    concurrency: u32,
) -> Result<Vec<Vec<Box<dyn ReadSnapshot + 'a>>>, ExecutorError> {
    (0..worker_count(concurrency)?)
        .map(|_| {
            adapters
                .iter()
                .map(|adapter| {
                    block_on(adapter.begin_read_snapshot()).map_err(|error| {
                        ExecutorError::failed(format!(
                            "failed to pin production Adapter snapshot: {error}"
                        ))
                    })
                })
                .collect()
        })
        .collect()
}

fn measure_prepared_workers<W: Send>(
    workers: Vec<W>,
    prefix: &[u8],
    warmup_seconds: u64,
    measurement_seconds: u64,
    operation: impl Fn(&mut W, &[u8]) -> Result<u64, ExecutorError> + Sync,
) -> Result<Measurement, ExecutorError> {
    let boundaries = MeasurementBoundaries::new(warmup_seconds, measurement_seconds)?;
    let results = thread::scope(|scope| {
        let mut handles = Vec::with_capacity(workers.len());
        for mut worker in workers {
            let boundaries = boundaries.clone();
            let operation = &operation;
            handles.push(
                scope.spawn(move || measure_worker(boundaries, || operation(&mut worker, prefix))),
            );
        }
        join_workers(handles)
    })?;
    Measurement::merge(boundaries.timing, results)
}

fn worker_count(concurrency: u32) -> Result<usize, ExecutorError> {
    usize::try_from(concurrency)
        .map_err(|_| ExecutorError::failed("concurrency does not fit usize"))
}

struct PostgresNativeWorker {
    client: PostgresClient,
    statement: PostgresStatement,
    instance_id: String,
    graph_id: Vec<u8>,
}

impl PostgresNativeWorker {
    fn count(&mut self) -> Result<u64, ExecutorError> {
        let row = self
            .client
            .query_one(&self.statement, &[&self.instance_id, &self.graph_id])
            .map_err(|error| {
                ExecutorError::failed(format!("native PostgreSQL count failed: {error}"))
            })?;
        let count = row.get::<_, i64>(0);
        u64::try_from(count)
            .map_err(|_| ExecutorError::failed("native PostgreSQL returned a negative count"))
    }
}

fn prepare_postgres_native_workers(
    runtime: &PostgresRuntime,
    graph_id: u64,
    concurrency: u32,
) -> Result<Vec<PostgresNativeWorker>, ExecutorError> {
    const COUNT_SQL: &str = "SELECT count(*)::bigint FROM dtgproxy.vertex_current WHERE instance_id = $1 AND graph_id = $2";
    (0..worker_count(concurrency)?)
        .map(|_| {
            let mut client =
                PostgresClient::connect(&runtime.connection_string, NoTls).map_err(|error| {
                    ExecutorError::failed(format!("failed to connect native PostgreSQL: {error}"))
                })?;
            client
                .batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
                .map_err(|error| {
                    ExecutorError::failed(format!(
                        "failed to pin native PostgreSQL snapshot: {error}"
                    ))
                })?;
            let statement = client.prepare(COUNT_SQL).map_err(|error| {
                ExecutorError::failed(format!(
                    "failed to prepare native PostgreSQL count: {error}"
                ))
            })?;
            let mut worker = PostgresNativeWorker {
                client,
                statement,
                instance_id: runtime.instance_id.clone(),
                graph_id: graph_id.to_be_bytes().to_vec(),
            };
            worker.count()?;
            Ok(worker)
        })
        .collect()
}

struct Neo4jNativeWorker {
    agent: ureq::Agent,
    url: String,
    authorization: String,
    instance_id: String,
    graph_hex: String,
}

impl Neo4jNativeWorker {
    fn count(&mut self) -> Result<u64, ExecutorError> {
        const COUNT_CYPHER: &str = "MATCH (record:DTGCanonicalRecord:DTGVertexCurrent {instance_id: $instance_id, graph_hex: $graph_hex, present: true}) RETURN count(record)";
        let response = self
            .agent
            .post(&self.url)
            .set("Accept", "application/json")
            .set("Authorization", &self.authorization)
            .send_json(json!({
                "statement": COUNT_CYPHER,
                "parameters": {
                    "instance_id": self.instance_id,
                    "graph_hex": self.graph_hex
                }
            }));
        let response = match response {
            Ok(response) => response,
            Err(ureq::Error::Status(_, response)) => {
                let status = response.status();
                let body = response.into_string().unwrap_or_default();
                return Err(ExecutorError::failed(format!(
                    "native Neo4j count returned HTTP {status}: {body}"
                )));
            }
            Err(ureq::Error::Transport(error)) => {
                return Err(ExecutorError::failed(format!(
                    "native Neo4j count transport failed: {error}"
                )));
            }
        };
        let body: JsonValue = response.into_json().map_err(|error| {
            ExecutorError::failed(format!("native Neo4j returned invalid JSON: {error}"))
        })?;
        if let Some(errors) = body.get("errors").and_then(JsonValue::as_array)
            && !errors.is_empty()
        {
            return Err(ExecutorError::failed(format!(
                "native Neo4j query failed: {}",
                errors[0]
            )));
        }
        let rows = body
            .pointer("/data/values")
            .and_then(JsonValue::as_array)
            .ok_or_else(|| ExecutorError::failed("native Neo4j response omitted data.values"))?;
        if rows.len() != 1 || rows[0].as_array().is_none_or(|row| row.len() != 1) {
            return Err(ExecutorError::failed(
                "native Neo4j count must return exactly one row and one column",
            ));
        }
        rows[0][0].as_u64().ok_or_else(|| {
            ExecutorError::failed("native Neo4j count is not a non-negative integer")
        })
    }
}

fn prepare_neo4j_native_workers(
    runtime: &Neo4jRuntime,
    graph_id: u64,
    concurrency: u32,
) -> Result<Vec<Neo4jNativeWorker>, ExecutorError> {
    let credentials = base64::engine::general_purpose::STANDARD
        .encode(format!("{}:{}", runtime.username, runtime.password));
    (0..worker_count(concurrency)?)
        .map(|_| {
            let mut worker = Neo4jNativeWorker {
                agent: ureq::AgentBuilder::new()
                    .timeout(Duration::from_secs(runtime.timeout_seconds))
                    .build(),
                url: format!(
                    "{}/db/{}/query/v2",
                    runtime.endpoint.trim_end_matches('/'),
                    runtime.database
                ),
                authorization: format!("Basic {credentials}"),
                instance_id: runtime.instance_id.clone(),
                graph_hex: format!("{graph_id:016x}"),
            };
            worker.count()?;
            Ok(worker)
        })
        .collect()
}

fn join_workers<'scope>(
    handles: Vec<thread::ScopedJoinHandle<'scope, Result<WorkerMeasurement, ExecutorError>>>,
) -> Result<Vec<WorkerMeasurement>, ExecutorError> {
    handles
        .into_iter()
        .map(|handle| {
            handle
                .join()
                .map_err(|_| ExecutorError::failed("measurement worker panicked"))?
        })
        .collect()
}

fn measure_worker(
    boundaries: MeasurementBoundaries,
    mut operation: impl FnMut() -> Result<u64, ExecutorError>,
) -> Result<WorkerMeasurement, ExecutorError> {
    let mut expected = None;
    while Instant::now() < boundaries.measurement_started {
        merge_count(&mut expected, operation()?)?;
    }
    let mut latency_ns = Vec::new();
    while Instant::now() < boundaries.measurement_ended || latency_ns.is_empty() {
        let started = Instant::now();
        merge_count(&mut expected, operation()?)?;
        latency_ns.push(duration_ns(started.elapsed())?);
    }
    Ok(WorkerMeasurement {
        count: expected.ok_or_else(|| ExecutorError::failed("measurement returned no result"))?,
        latency_ns,
    })
}

fn count_native_snapshots(
    databases: &[RocksDb],
    snapshots: &[SnapshotWithThreadMode<'_, RocksDb>],
    prefix: &[u8],
) -> Result<u64, ExecutorError> {
    let mut count = 0_u64;
    for (database, snapshot) in databases.iter().zip(snapshots) {
        let current = database
            .cf_handle(Keyspace::Current.column_family())
            .ok_or_else(|| ExecutorError::failed("RocksDB current column family is missing"))?;
        let iterator =
            snapshot.iterator_cf(&current, IteratorMode::From(prefix, Direction::Forward));
        for item in iterator {
            let (key, _) = item.map_err(failed)?;
            if !key.starts_with(prefix) {
                break;
            }
            count = count
                .checked_add(1)
                .ok_or_else(|| ExecutorError::failed("vertex count overflow"))?;
        }
    }
    Ok(count)
}

fn count_adapter_snapshots(
    snapshots: &[Box<dyn ReadSnapshot + '_>],
    prefix: &[u8],
) -> Result<u64, ExecutorError> {
    let mut count = 0_u64;
    for snapshot in snapshots {
        count = count
            .checked_add(count_pinned_snapshot(snapshot.as_ref(), prefix)?)
            .ok_or_else(|| ExecutorError::failed("vertex count overflow"))?;
    }
    Ok(count)
}

fn count_pinned_snapshot(snapshot: &dyn ReadSnapshot, prefix: &[u8]) -> Result<u64, ExecutorError> {
    let bounds = QueryPageBounds::new(PAGE_ITEMS, PAGE_BYTES).map_err(failed)?;
    let mut start = prefix.to_vec();
    let mut count = 0_u64;
    loop {
        let span =
            KeySpan::prefix_from(Keyspace::Current, prefix.to_vec(), start).map_err(failed)?;
        let request = CanonicalScanRequest::new(span, bounds).map_err(failed)?;
        let page = block_on(snapshot.scan_canonical(&request)).map_err(failed)?;
        count = count
            .checked_add(page.entries().len() as u64)
            .ok_or_else(|| ExecutorError::failed("vertex count overflow"))?;
        let Some(next) = page.next_start() else {
            break;
        };
        start = next.as_bytes().to_vec();
    }
    Ok(count)
}

fn open_native_rocks(path: &PathBuf) -> Result<RocksDb, ExecutorError> {
    let options = Options::default();
    let column_families = RocksDb::list_cf(&options, path).map_err(|error| {
        ExecutorError::failed(format!(
            "failed to inspect native RocksDB at {}: {error}",
            path.display()
        ))
    })?;
    RocksDb::open_cf_for_read_only(&options, path, column_families, false).map_err(|error| {
        ExecutorError::failed(format!(
            "failed to open native read-only RocksDB at {}: {error}",
            path.display()
        ))
    })
}

fn count_identity(count: u64) -> Result<ResultIdentity, ExecutorError> {
    let count = i64::try_from(count)
        .map_err(|_| ExecutorError::failed("count result does not fit Bolt integer"))?;
    let record = BoltValue::Structure {
        signature: 0x71,
        fields: vec![BoltValue::List(vec![BoltValue::Integer(count)])],
    };
    let encoded = encode_bolt(&record).map_err(failed)?;
    let mut digest = blake3::Hasher::new();
    digest.update(&(encoded.len() as u64).to_be_bytes());
    digest.update(&encoded);
    Ok(ResultIdentity {
        digest: digest.finalize().to_hex().to_string(),
        row_count: 1,
    })
}

fn merge_count(expected: &mut Option<u64>, actual: u64) -> Result<(), ExecutorError> {
    match expected {
        Some(expected) if *expected != actual => Err(ExecutorError::failed(format!(
            "result identity changed within one measurement: expected {expected}, got {actual}"
        ))),
        Some(_) => Ok(()),
        None => {
            *expected = Some(actual);
            Ok(())
        }
    }
}

fn require_production_direct(config: &CellConfig) -> Result<(), ExecutorError> {
    if config.cell.key.ablation != "production" {
        return Err(ExecutorError::failed(
            "Direct paths only support the production configuration",
        ));
    }
    Ok(())
}

fn validate_cell_config(config: &CellConfig) -> Result<(), ExecutorError> {
    if config.schema_version != SCHEMA_VERSION
        || config.run_id.trim().is_empty()
        || config.cell.key.workload.trim().is_empty()
        || config.cell.key.data_nodes == 0
        || config.cell.key.concurrency == 0
        || config.cell.repetition == 0
        || config.protocol.warmup_seconds == 0
        || config.protocol.measurement_seconds == 0
        || config.query.trim().is_empty()
    {
        return Err(ExecutorError::failed("invalid CellConfig"));
    }
    if !config.parameters.is_empty() {
        return Err(ExecutorError::Unavailable(
            "count_current_vertices does not accept query parameters".into(),
        ));
    }
    Ok(())
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RuntimeManifest {
    schema_version: u32,
    dataset_digest: String,
    snapshot: String,
    workloads: Vec<RuntimeWorkload>,
    backends: BTreeMap<Backend, BackendRuntime>,
    proxy_targets: Vec<ProxyTarget>,
}

impl RuntimeManifest {
    fn bind<'a>(&'a self, config: &CellConfig) -> Result<&'a RuntimeWorkload, ExecutorError> {
        if self.schema_version != SCHEMA_VERSION
            || self.dataset_digest != config.dataset_digest
            || self.snapshot != config.snapshot
        {
            return Err(ExecutorError::failed(
                "runtime manifest dataset or snapshot does not match CellConfig",
            ));
        }
        let matches = self
            .workloads
            .iter()
            .filter(|workload| workload.workload_digest == config.workload_digest)
            .collect::<Vec<_>>();
        if matches.len() != 1 {
            return Err(ExecutorError::Unavailable(format!(
                "workload digest {} has no unique typed runtime binding",
                config.workload_digest
            )));
        }
        Ok(matches[0])
    }

    fn rocks_paths(&self, backend: Backend) -> Result<&[PathBuf], ExecutorError> {
        let runtime = self.backend_runtime(backend)?;
        match runtime {
            BackendRuntime::Unavailable(runtime) => Err(ExecutorError::Unavailable(format!(
                "{backend} backend is unavailable: {}",
                runtime.reason
            ))),
            BackendRuntime::Rocksdb(runtime) if backend == Backend::Rocksdb => {
                validate_snapshot_paths(&runtime.snapshot_paths)?;
                Ok(&runtime.snapshot_paths)
            }
            _ => Err(backend_runtime_mismatch(backend, runtime)),
        }
    }

    fn postgres_runtime(&self, backend: Backend) -> Result<&PostgresRuntime, ExecutorError> {
        let runtime = self.backend_runtime(backend)?;
        match runtime {
            BackendRuntime::Unavailable(runtime) => Err(ExecutorError::Unavailable(format!(
                "{backend} backend is unavailable: {}",
                runtime.reason
            ))),
            BackendRuntime::Postgresql(runtime) if backend == Backend::Postgresql => {
                runtime.validate()?;
                Ok(runtime)
            }
            _ => Err(backend_runtime_mismatch(backend, runtime)),
        }
    }

    fn neo4j_runtime(&self, backend: Backend) -> Result<&Neo4jRuntime, ExecutorError> {
        let runtime = self.backend_runtime(backend)?;
        match runtime {
            BackendRuntime::Unavailable(runtime) => Err(ExecutorError::Unavailable(format!(
                "{backend} backend is unavailable: {}",
                runtime.reason
            ))),
            BackendRuntime::Neo4j(runtime) if backend == Backend::Neo4j => {
                runtime.validate()?;
                Ok(runtime)
            }
            _ => Err(backend_runtime_mismatch(backend, runtime)),
        }
    }

    fn backend_runtime(&self, backend: Backend) -> Result<&BackendRuntime, ExecutorError> {
        self.backends.get(&backend).ok_or_else(|| {
            ExecutorError::Unavailable(format!("{backend} runtime is not configured"))
        })
    }

    fn proxy_target(&self, config: &CellConfig) -> Result<&ProxyTarget, ExecutorError> {
        let matches = self
            .proxy_targets
            .iter()
            .filter(|target| {
                target.backend == config.cell.key.backend
                    && target.data_nodes == config.cell.key.data_nodes
                    && target.ablation == config.cell.key.ablation
            })
            .collect::<Vec<_>>();
        if matches.len() != 1 {
            return Err(ExecutorError::Unavailable(format!(
                "Proxy target is unavailable for backend={} data_nodes={} ablation={}",
                config.cell.key.backend, config.cell.key.data_nodes, config.cell.key.ablation
            )));
        }
        Ok(matches[0])
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RuntimeWorkload {
    workload_digest: String,
    #[serde(default)]
    kind: Option<RuntimeWorkloadKind>,
    #[serde(default)]
    graph_id: u64,
}

impl RuntimeWorkload {
    fn vertex_prefix(&self) -> Result<Vec<u8>, ExecutorError> {
        match self.kind {
            Some(RuntimeWorkloadKind::CountCurrentVertices) if self.graph_id != 0 => {
                Ok(current_vertex_graph_prefix(GraphId::new(self.graph_id)))
            }
            _ => Err(ExecutorError::Unavailable(
                "Direct paths require a typed count_current_vertices runtime binding".into(),
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RuntimeWorkloadKind {
    CountCurrentVertices,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum BackendRuntime {
    Rocksdb(RocksdbRuntime),
    Postgresql(PostgresRuntime),
    Neo4j(Neo4jRuntime),
    Unavailable(UnavailableRuntime),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RocksdbRuntime {
    #[serde(rename = "status")]
    _status: AvailableStatus,
    snapshot_paths: Vec<PathBuf>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PostgresRuntime {
    #[serde(rename = "status")]
    _status: AvailableStatus,
    connection_string: String,
    instance_id: String,
    pool_size: usize,
}

impl PostgresRuntime {
    fn validate(&self) -> Result<(), ExecutorError> {
        if self.connection_string.trim().is_empty()
            || self.instance_id.trim().is_empty()
            || self.instance_id.len() > 255
            || self.pool_size == 0
            || self.pool_size > 128
        {
            return Err(ExecutorError::failed(
                "invalid PostgreSQL runtime configuration",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Neo4jRuntime {
    #[serde(rename = "status")]
    _status: AvailableStatus,
    endpoint: String,
    database: String,
    username: String,
    password: String,
    instance_id: String,
    timeout_seconds: u64,
}

impl Neo4jRuntime {
    fn validate(&self) -> Result<(), ExecutorError> {
        if !(self.endpoint.starts_with("http://") || self.endpoint.starts_with("https://"))
            || !valid_runtime_identifier(&self.database)
            || self.username.trim().is_empty()
            || self.password.is_empty()
            || self.instance_id.trim().is_empty()
            || self.instance_id.len() > 255
            || !(1..=300).contains(&self.timeout_seconds)
        {
            return Err(ExecutorError::failed("invalid Neo4j runtime configuration"));
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct UnavailableRuntime {
    #[serde(rename = "status")]
    _status: UnavailableStatus,
    reason: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum AvailableStatus {
    Available,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum UnavailableStatus {
    Unavailable,
}

fn backend_runtime_mismatch(backend: Backend, runtime: &BackendRuntime) -> ExecutorError {
    let configured = match runtime {
        BackendRuntime::Rocksdb(_) => "rocksdb",
        BackendRuntime::Postgresql(_) => "postgresql",
        BackendRuntime::Neo4j(_) => "neo4j",
        BackendRuntime::Unavailable(_) => "unavailable",
    };
    ExecutorError::failed(format!(
        "{backend} runtime contains {configured} backend fields"
    ))
}

fn valid_runtime_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 63
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
}

#[derive(Serialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
enum GatewayRequest<'a> {
    BeginCell {
        schema_version: u32,
        cell_id: &'a str,
        configuration_digest: &'a str,
        config: AblationConfig,
    },
    FinishCell {
        schema_version: u32,
        session_token: &'a str,
    },
    AbortCell {
        schema_version: u32,
        session_token: &'a str,
    },
}

#[derive(Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
enum GatewayResponse {
    Begun {
        schema_version: u32,
        gateway_pid: u32,
        session_token: String,
        accepted_config: AblationConfig,
    },
    Finished {
        schema_version: u32,
        gateway_pid: u32,
        cell_id: String,
        configuration_digest: String,
        accepted_config: AblationConfig,
        queries_started: u64,
        queries_completed: u64,
        queries_failed: u64,
        queries_in_flight: u64,
        counters: AblationCounters,
    },
    Aborted {
        schema_version: u32,
        gateway_pid: u32,
    },
    Error {
        schema_version: u32,
        gateway_pid: u32,
        message: String,
    },
}

struct GatewayBegun {
    session_token: String,
}

struct GatewayFinished {
    schema_version: u32,
    gateway_pid: u32,
    cell_id: String,
    configuration_digest: String,
    accepted_config: AblationConfig,
    queries_started: u64,
    queries_completed: u64,
    queries_failed: u64,
    queries_in_flight: u64,
    counters: AblationCounters,
}

impl GatewayFinished {
    fn into_evidence(
        self,
        target: &ProxyTarget,
        cell_id: &str,
        configuration_digest: &str,
        config: AblationConfig,
        total_operations: usize,
    ) -> Result<AblationEvidence, ExecutorError> {
        let total_operations = u64::try_from(total_operations)
            .map_err(|_| ExecutorError::failed("loadgen total operation count overflow"))?;
        let evidence = AblationEvidence {
            gateway_pid: self.gateway_pid,
            cell_id: self.cell_id,
            configuration_digest: self.configuration_digest,
            config: self.accepted_config,
            total_operations,
            queries_started: self.queries_started,
            queries_completed: self.queries_completed,
            queries_failed: self.queries_failed,
            queries_in_flight: self.queries_in_flight,
            counters: self.counters,
        };
        if self.schema_version != SCHEMA_VERSION
            || evidence.gateway_pid != target.gateway_process.pid
            || evidence.cell_id != cell_id
            || evidence.configuration_digest != configuration_digest
            || evidence.config != config
            || evidence.queries_started != total_operations
            || evidence.queries_completed != total_operations
            || evidence.queries_failed != 0
            || evidence.queries_in_flight != 0
        {
            return Err(ExecutorError::failed(
                "Gateway finish_cell evidence does not match the loadgen cell",
            ));
        }
        Ok(evidence)
    }
}

fn gateway_begin_cell(
    target: &ProxyTarget,
    cell_id: &str,
    configuration_digest: &str,
    config: AblationConfig,
) -> Result<GatewayBegun, ExecutorError> {
    let request = GatewayRequest::BeginCell {
        schema_version: SCHEMA_VERSION,
        cell_id,
        configuration_digest,
        config,
    };
    let response = gateway_request(&target.ablation_control_socket, &request)
        .or_else(|_| gateway_request(&target.ablation_control_socket, &request))?;
    match response {
        GatewayResponse::Begun {
            schema_version,
            gateway_pid,
            session_token,
            accepted_config,
        } => {
            if schema_version == SCHEMA_VERSION
                && gateway_pid == target.gateway_process.pid
                && accepted_config == config
                && valid_session_token(&session_token)
            {
                Ok(GatewayBegun { session_token })
            } else {
                if valid_session_token(&session_token) {
                    gateway_abort_cell_best_effort(target, &session_token);
                }
                Err(ExecutorError::failed(
                    "Gateway begin_cell response does not match the requested cell",
                ))
            }
        }
        GatewayResponse::Error {
            schema_version,
            gateway_pid,
            message,
        } => gateway_error(target, schema_version, gateway_pid, "begin_cell", &message),
        GatewayResponse::Finished { .. } => Err(ExecutorError::failed(
            "Gateway returned finished for begin_cell",
        )),
        GatewayResponse::Aborted { .. } => Err(ExecutorError::failed(
            "Gateway returned aborted for begin_cell",
        )),
    }
}

fn gateway_finish_cell(
    target: &ProxyTarget,
    session_token: &str,
) -> Result<GatewayFinished, ExecutorError> {
    let request = GatewayRequest::FinishCell {
        schema_version: SCHEMA_VERSION,
        session_token,
    };
    match gateway_request(&target.ablation_control_socket, &request)
        .or_else(|_| gateway_request(&target.ablation_control_socket, &request))?
    {
        GatewayResponse::Finished {
            schema_version,
            gateway_pid,
            cell_id,
            configuration_digest,
            accepted_config,
            queries_started,
            queries_completed,
            queries_failed,
            queries_in_flight,
            counters,
        } => Ok(GatewayFinished {
            schema_version,
            gateway_pid,
            cell_id,
            configuration_digest,
            accepted_config,
            queries_started,
            queries_completed,
            queries_failed,
            queries_in_flight,
            counters,
        }),
        GatewayResponse::Error {
            schema_version,
            gateway_pid,
            message,
        } => gateway_error(target, schema_version, gateway_pid, "finish_cell", &message),
        GatewayResponse::Begun { .. } => Err(ExecutorError::failed(
            "Gateway returned begun for finish_cell",
        )),
        GatewayResponse::Aborted { .. } => Err(ExecutorError::failed(
            "Gateway returned aborted for finish_cell",
        )),
    }
}

fn gateway_abort_cell_best_effort(target: &ProxyTarget, session_token: &str) {
    let Ok(response) = gateway_request(
        &target.ablation_control_socket,
        &GatewayRequest::AbortCell {
            schema_version: SCHEMA_VERSION,
            session_token,
        },
    ) else {
        return;
    };
    if let GatewayResponse::Aborted {
        schema_version,
        gateway_pid,
    } = response
    {
        let _ = schema_version == SCHEMA_VERSION && gateway_pid == target.gateway_process.pid;
    }
}

fn gateway_error<T>(
    target: &ProxyTarget,
    schema_version: u32,
    gateway_pid: u32,
    operation: &str,
    message: &str,
) -> Result<T, ExecutorError> {
    if schema_version != SCHEMA_VERSION || gateway_pid != target.gateway_process.pid {
        return Err(ExecutorError::failed(format!(
            "Gateway {operation} error response has the wrong schema or PID"
        )));
    }
    Err(ExecutorError::failed(format!(
        "Gateway {operation} rejected: {message}"
    )))
}

fn gateway_request(
    socket: &Path,
    request: &GatewayRequest<'_>,
) -> Result<GatewayResponse, ExecutorError> {
    let mut stream = UnixStream::connect(socket).map_err(|error| {
        ExecutorError::failed(format!(
            "failed to connect Gateway ablation socket {}: {error}",
            socket.display()
        ))
    })?;
    let timeout = Some(Duration::from_secs(5));
    stream.set_read_timeout(timeout).map_err(failed)?;
    stream.set_write_timeout(timeout).map_err(failed)?;
    let mut encoded = serde_json::to_vec(request).map_err(failed)?;
    encoded.push(b'\n');
    stream.write_all(&encoded).map_err(failed)?;
    stream.flush().map_err(failed)?;
    let mut reader = BufReader::new(stream);
    let mut response = Vec::new();
    let read = reader.read_until(b'\n', &mut response).map_err(failed)?;
    if read == 0 || response.len() > 16 * 1024 || !response.ends_with(b"\n") {
        return Err(ExecutorError::failed(
            "Gateway response must be one newline-terminated line of at most 16 KiB",
        ));
    }
    serde_json::from_slice(&response)
        .map_err(|error| ExecutorError::failed(format!("invalid Gateway response: {error}")))
}

fn valid_session_token(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_'))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProxyTarget {
    deployment_mode: DeploymentMode,
    backend: Backend,
    data_nodes: u32,
    ablation: String,
    bolt_address: String,
    bolt_loadgen_binary: PathBuf,
    ablation_control_socket: PathBuf,
    #[serde(default = "default_timeout_ms")]
    timeout_ms: u64,
    data_node_processes: Vec<DataNodeProcess>,
    gateway_process: ProcessEvidence,
}

impl ProxyTarget {
    fn validate(&self, expected_nodes: u32) -> Result<VerifiedTopology, ExecutorError> {
        if self.data_node_processes.len() != expected_nodes as usize {
            return Err(ExecutorError::failed(format!(
                "expected {expected_nodes} data-node OS processes, found {}",
                self.data_node_processes.len()
            )));
        }
        validate_executable(&self.bolt_loadgen_binary, "Bolt loadgen")?;
        if !self.ablation_control_socket.is_absolute()
            || !fs::metadata(&self.ablation_control_socket)
                .map(|metadata| metadata.file_type().is_socket())
                .unwrap_or(false)
        {
            return Err(ExecutorError::failed(
                "ablation control socket must be an existing absolute Unix socket",
            ));
        }
        let mut pids = BTreeSet::new();
        let mut listeners = BTreeSet::new();
        let mut directories = BTreeSet::new();
        let mut host_ids = BTreeSet::new();
        let mut data_node_pids = Vec::with_capacity(self.data_node_processes.len());
        for process in &self.data_node_processes {
            process.common.validate_address("data-node")?;
            if self.deployment_mode == DeploymentMode::LocalDiagnostic {
                process.common.validate_local("data-node")?;
            } else if process.common.address()?.ip().is_loopback() {
                return Err(ExecutorError::failed(
                    "remote_formal data-node listener must be non-loopback",
                ));
            }
            if !process.data_directory.is_absolute()
                || (self.deployment_mode == DeploymentMode::LocalDiagnostic
                    && !process.data_directory.is_dir())
            {
                return Err(ExecutorError::failed(
                    "data-node data directories must be absolute and locally existing in local_diagnostic",
                ));
            }
            if !directories.insert(process.data_directory.clone()) {
                return Err(ExecutorError::failed(
                    "data-node data directories must be distinct",
                ));
            }
            if !pids.insert(process.common.pid) || !listeners.insert(&process.common.listen_address)
            {
                return Err(ExecutorError::failed(
                    "data-node and gateway must be distinct OS processes with distinct listeners",
                ));
            }
            match (
                &process.executable_sha256,
                &process.probe,
                self.deployment_mode,
            ) {
                (None, None, DeploymentMode::LocalDiagnostic) => {}
                (Some(digest), Some(probe), DeploymentMode::RemoteFormal) => {
                    validate_sha256(digest, "data-node executable_sha256")?;
                    probe.validate()?;
                    if !host_ids.insert(&probe.host_id) {
                        return Err(ExecutorError::failed(
                            "remote_formal requires a unique host_id per data node",
                        ));
                    }
                }
                (_, _, DeploymentMode::LocalDiagnostic) => {
                    return Err(ExecutorError::failed(
                        "local_diagnostic data-node evidence must not contain remote probe fields",
                    ));
                }
                (_, _, DeploymentMode::RemoteFormal) => {
                    return Err(ExecutorError::failed(
                        "remote_formal data-node evidence requires executable_sha256 and probe",
                    ));
                }
            }
            data_node_pids.push(process.common.pid);
        }
        self.gateway_process.validate_local("gateway")?;
        if !pids.insert(self.gateway_process.pid)
            || !listeners.insert(&self.gateway_process.listen_address)
        {
            return Err(ExecutorError::failed(
                "data-node and gateway must be distinct OS processes with distinct listeners",
            ));
        }
        if self.deployment_mode == DeploymentMode::LocalDiagnostic
            && pids.contains(&std::process::id())
        {
            return Err(ExecutorError::failed(
                "Proxy topology processes must be external to the cell executor",
            ));
        }
        Ok(VerifiedTopology {
            data_node_pids,
            local_pids: match self.deployment_mode {
                DeploymentMode::LocalDiagnostic => pids.into_iter().collect(),
                DeploymentMode::RemoteFormal => vec![self.gateway_process.pid],
            },
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum DeploymentMode {
    LocalDiagnostic,
    RemoteFormal,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DataNodeProcess {
    #[serde(flatten)]
    common: ProcessEvidence,
    data_directory: PathBuf,
    executable_sha256: Option<String>,
    probe: Option<RemoteProbeConfig>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RemoteProbeConfig {
    ssh_target: String,
    host_id: String,
    boot_id: String,
    probe_binary: PathBuf,
    probe_binary_sha256: String,
    data_interface: String,
    management_interface: String,
}

impl RemoteProbeConfig {
    fn validate(&self) -> Result<(), ExecutorError> {
        for (name, value) in [
            ("ssh_target", self.ssh_target.as_str()),
            ("host_id", self.host_id.as_str()),
            ("boot_id", self.boot_id.as_str()),
            ("data_interface", self.data_interface.as_str()),
            ("management_interface", self.management_interface.as_str()),
        ] {
            if value.is_empty() || value.bytes().any(|byte| byte.is_ascii_control()) {
                return Err(ExecutorError::failed(format!(
                    "remote probe {name} must be non-empty and contain no control characters"
                )));
            }
        }
        if !self.probe_binary.is_absolute() {
            return Err(ExecutorError::failed(
                "remote probe_binary must be an absolute path",
            ));
        }
        validate_sha256(&self.probe_binary_sha256, "probe_binary_sha256")?;
        if self.data_interface == self.management_interface {
            return Err(ExecutorError::failed(
                "remote data_interface and management_interface must be distinct",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProcessEvidence {
    pid: u32,
    executable: PathBuf,
    listen_address: String,
}

impl ProcessEvidence {
    fn address(&self) -> Result<SocketAddr, ExecutorError> {
        self.listen_address
            .parse::<SocketAddr>()
            .map_err(|_| ExecutorError::failed("invalid process listener"))
    }

    fn validate_address(&self, role: &str) -> Result<(), ExecutorError> {
        if self.pid <= 1 {
            return Err(ExecutorError::failed(format!("invalid {role} PID")));
        }
        self.listen_address
            .parse::<SocketAddr>()
            .map_err(|_| ExecutorError::failed(format!("invalid {role} listener")))?;
        if !self.executable.is_absolute() {
            return Err(ExecutorError::failed(format!(
                "{role} executable must be an absolute path"
            )));
        }
        Ok(())
    }

    fn validate_local(&self, role: &str) -> Result<(), ExecutorError> {
        self.validate_address(role)?;
        let address = self.address()?;
        if !address.ip().is_loopback() {
            return Err(ExecutorError::failed(format!(
                "{role} listener must be loopback"
            )));
        }
        validate_executable(&self.executable, role)?;
        verify_live_process(self.pid, &self.executable, role)
    }
}

struct VerifiedTopology {
    data_node_pids: Vec<u32>,
    local_pids: Vec<u32>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct BoltLoadgenReport {
    schema_version: u32,
    timing_boundary: String,
    connections: usize,
    warmup_ns: u64,
    warmup_started_unix_ns: u64,
    measurement_started_unix_ns: u64,
    measurement_ended_unix_ns: u64,
    measured_elapsed_ns: u64,
    total_operations: usize,
    completed_operations: usize,
    throughput_ops_per_second: f64,
    connection_setup_samples_ns: Vec<u64>,
    ttfr_samples_ns: Vec<u64>,
    total_latency_samples_ns: Vec<u64>,
    result_digest: String,
    row_count: usize,
    error_count: usize,
}

impl BoltLoadgenReport {
    fn validate(&self, config: &CellConfig) -> Result<(), ExecutorError> {
        let expected_warmup = seconds_ns(config.protocol.warmup_seconds)?;
        let expected_measurement = seconds_ns(config.protocol.measurement_seconds)?;
        if self.schema_version != SCHEMA_VERSION
            || self.timing_boundary.trim().is_empty()
            || self.connections != config.cell.key.concurrency as usize
            || self.warmup_ns != expected_warmup
            || self.measurement_started_unix_ns
                != self
                    .warmup_started_unix_ns
                    .checked_add(expected_warmup)
                    .ok_or_else(|| ExecutorError::failed("loadgen timing overflow"))?
            || self.measurement_ended_unix_ns
                != self
                    .measurement_started_unix_ns
                    .checked_add(expected_measurement)
                    .ok_or_else(|| ExecutorError::failed("loadgen timing overflow"))?
            || self.measured_elapsed_ns < expected_measurement
            || self.completed_operations == 0
            || self.total_operations < self.completed_operations
            || self.error_count != 0
            || self.ttfr_samples_ns.len() != self.completed_operations
            || self.total_latency_samples_ns.len() != self.completed_operations
            || self
                .ttfr_samples_ns
                .iter()
                .zip(&self.total_latency_samples_ns)
                .any(|(ttfr, latency)| ttfr > latency)
            || self.result_digest.len() != 64
            || !self
                .result_digest
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
            || !self.throughput_ops_per_second.is_finite()
            || self.throughput_ops_per_second <= 0.0
            || self.connection_setup_samples_ns.len() != self.connections
        {
            return Err(ExecutorError::failed(
                "persistent Bolt loadgen report violates the cell contract",
            ));
        }
        Ok(())
    }
}

#[derive(Clone)]
struct MeasurementBoundaries {
    measurement_started: Instant,
    measurement_ended: Instant,
    timing: TimingBoundaries,
}

impl MeasurementBoundaries {
    fn new(warmup_seconds: u64, measurement_seconds: u64) -> Result<Self, ExecutorError> {
        let warmup = Duration::from_secs(warmup_seconds);
        let measurement = Duration::from_secs(measurement_seconds);
        let warmup_started_unix_ns = unix_time_ns()?;
        let measurement_started_unix_ns = warmup_started_unix_ns
            .checked_add(duration_ns(warmup)?)
            .ok_or_else(|| ExecutorError::failed("measurement timing overflow"))?;
        let measurement_ended_unix_ns = measurement_started_unix_ns
            .checked_add(duration_ns(measurement)?)
            .ok_or_else(|| ExecutorError::failed("measurement timing overflow"))?;
        let started = Instant::now();
        let measurement_started = started + warmup;
        Ok(Self {
            measurement_started,
            measurement_ended: measurement_started + measurement,
            timing: TimingBoundaries {
                warmup_started_unix_ns,
                measurement_started_unix_ns,
                measurement_ended_unix_ns,
            },
        })
    }
}

struct WorkerMeasurement {
    count: u64,
    latency_ns: Vec<u64>,
}

struct Measurement {
    timing: TimingBoundaries,
    count: u64,
    latency_ns: Vec<u64>,
}

struct MeasurementWithResources {
    measurement: Measurement,
    resources: ResourceMetrics,
}

impl Measurement {
    fn merge(
        timing: TimingBoundaries,
        workers: Vec<WorkerMeasurement>,
    ) -> Result<Self, ExecutorError> {
        let first = workers
            .first()
            .ok_or_else(|| ExecutorError::failed("measurement has no workers"))?
            .count;
        if workers.iter().any(|worker| worker.count != first) {
            return Err(ExecutorError::failed(
                "result identity differs across concurrent workers",
            ));
        }
        let latency_ns = workers
            .into_iter()
            .flat_map(|worker| worker.latency_ns)
            .collect::<Vec<_>>();
        if latency_ns.is_empty() {
            return Err(ExecutorError::failed("measurement has no operations"));
        }
        Ok(Self {
            timing,
            count: first,
            latency_ns,
        })
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ProcessProbeSnapshot {
    schema_version: u32,
    sampled_unix_ns: u64,
    host_id: String,
    boot_id: String,
    process_start_id: String,
    pid: u32,
    executable: PathBuf,
    executable_sha256: String,
    probe_binary_sha256: String,
    listen_address: String,
    data_directory: PathBuf,
    cpu_time_ns: u64,
    rss_bytes: u64,
    peak_rss_bytes: u64,
    network_interface: String,
    network_rx_bytes: u64,
    network_tx_bytes: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ResourceProbeSnapshot {
    schema_version: u32,
    sampled_unix_ns: u64,
    process_start_id: String,
    pid: u32,
    cpu_time_ns: u64,
    rss_bytes: u64,
    network_interface: String,
    network_rx_bytes: u64,
    network_tx_bytes: u64,
}

impl ResourceProbeSnapshot {
    fn validate_for(
        &self,
        process: &DataNodeProcess,
        expected_start_id: &str,
        previous: Option<&Self>,
    ) -> Result<(), ExecutorError> {
        let probe = process.probe.as_ref().ok_or_else(|| {
            ExecutorError::failed("remote_formal data node has no probe configuration")
        })?;
        if self.schema_version != SCHEMA_VERSION
            || self.sampled_unix_ns == 0
            || self.process_start_id != expected_start_id
            || self.pid != process.common.pid
            || self.network_interface != probe.data_interface
        {
            return Err(ExecutorError::failed(
                "remote resource probe identity does not match the sealed process",
            ));
        }
        if let Some(previous) = previous {
            if self.sampled_unix_ns < previous.sampled_unix_ns
                || self.cpu_time_ns < previous.cpu_time_ns
                || self.network_rx_bytes < previous.network_rx_bytes
                || self.network_tx_bytes < previous.network_tx_bytes
            {
                return Err(ExecutorError::failed(
                    "remote resource probe counters moved backwards",
                ));
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
struct ResourceProbeEnvelope {
    requested_unix_ns: u64,
    completed_unix_ns: u64,
    sample: ResourceProbeSnapshot,
}

impl ResourceProbeEnvelope {
    fn from_full(
        requested_unix_ns: u64,
        completed_unix_ns: u64,
        sample: &ProcessProbeSnapshot,
    ) -> Self {
        Self {
            requested_unix_ns,
            completed_unix_ns,
            sample: ResourceProbeSnapshot {
                schema_version: sample.schema_version,
                sampled_unix_ns: sample.sampled_unix_ns,
                process_start_id: sample.process_start_id.clone(),
                pid: sample.pid,
                cpu_time_ns: sample.cpu_time_ns,
                rss_bytes: sample.rss_bytes,
                network_interface: sample.network_interface.clone(),
                network_rx_bytes: sample.network_rx_bytes,
                network_tx_bytes: sample.network_tx_bytes,
            },
        }
    }

    fn midpoint_unix_ns(&self) -> u64 {
        self.requested_unix_ns.saturating_add(
            self.completed_unix_ns
                .saturating_sub(self.requested_unix_ns)
                / 2,
        )
    }
}

struct FullProbeEnvelope {
    requested_unix_ns: u64,
    completed_unix_ns: u64,
    sample: ProcessProbeSnapshot,
}

impl ProcessProbeSnapshot {
    fn validate_for(
        &self,
        process: &DataNodeProcess,
        previous: Option<&Self>,
    ) -> Result<(), ExecutorError> {
        let probe = process.probe.as_ref().ok_or_else(|| {
            ExecutorError::failed("remote_formal data node has no probe configuration")
        })?;
        let expected_digest = process.executable_sha256.as_deref().ok_or_else(|| {
            ExecutorError::failed("remote_formal data node has no executable_sha256")
        })?;
        if self.schema_version != SCHEMA_VERSION {
            return Err(ExecutorError::failed(
                "remote probe schema_version does not match the executor",
            ));
        }
        if self.host_id != probe.host_id {
            return Err(ExecutorError::failed("remote probe host_id mismatch"));
        }
        if self.boot_id != probe.boot_id {
            return Err(ExecutorError::failed("remote probe boot_id mismatch"));
        }
        if self.process_start_id.is_empty() {
            return Err(ExecutorError::failed(
                "remote probe process_start_id is empty",
            ));
        }
        if self.pid != process.common.pid {
            return Err(ExecutorError::failed("remote probe pid mismatch"));
        }
        if self.executable != process.common.executable {
            return Err(ExecutorError::failed("remote probe executable mismatch"));
        }
        if self.executable_sha256 != expected_digest {
            return Err(ExecutorError::failed(
                "remote probe executable_sha256 mismatch",
            ));
        }
        if self.probe_binary_sha256 != probe.probe_binary_sha256 {
            return Err(ExecutorError::failed(
                "remote probe binary SHA-256 mismatch",
            ));
        }
        if self.listen_address != process.common.listen_address
            || self.data_directory != process.data_directory
        {
            return Err(ExecutorError::failed(
                "remote probe listener or data directory mismatch",
            ));
        }
        if self.network_interface != probe.data_interface {
            return Err(ExecutorError::failed(
                "remote probe network_interface is not the declared data_interface",
            ));
        }
        if self.sampled_unix_ns == 0 || self.rss_bytes > self.peak_rss_bytes {
            return Err(ExecutorError::failed(
                "remote probe returned invalid timestamp or RSS evidence",
            ));
        }
        validate_sha256(&self.executable_sha256, "remote executable_sha256")?;
        validate_sha256(&self.probe_binary_sha256, "remote probe_binary_sha256")?;
        if let Some(previous) = previous {
            if self.sampled_unix_ns < previous.sampled_unix_ns
                || self.cpu_time_ns < previous.cpu_time_ns
                || self.network_rx_bytes < previous.network_rx_bytes
                || self.network_tx_bytes < previous.network_tx_bytes
                || self.process_start_id != previous.process_start_id
            {
                return Err(ExecutorError::failed(
                    "remote probe counters or timestamp moved backwards",
                ));
            }
        }
        Ok(())
    }
}

enum ProxyResourceBaseline {
    Local(ResourceBaseline),
    Remote {
        local: ResourceBaseline,
        identities: Vec<ProcessProbeSnapshot>,
        samples: Vec<Vec<ResourceProbeEnvelope>>,
    },
}

struct MeasuredProxyResources {
    resources: ResourceMetrics,
    resource_scope: ResourceScope,
    topology_evidence: Option<TopologyEvidence>,
}

fn proxy_resources_before(
    target: &ProxyTarget,
    local_pids: &[u32],
) -> Result<ProxyResourceBaseline, ExecutorError> {
    match target.deployment_mode {
        DeploymentMode::LocalDiagnostic => {
            Ok(ProxyResourceBaseline::Local(process_resources(local_pids)?))
        }
        DeploymentMode::RemoteFormal => {
            let data_nodes = remote_full_probe_snapshots(target, None)?;
            let identities = data_nodes
                .iter()
                .map(|sample| sample.sample.clone())
                .collect::<Vec<_>>();
            let samples = data_nodes
                .iter()
                .map(|sample| {
                    vec![ResourceProbeEnvelope::from_full(
                        sample.requested_unix_ns,
                        sample.completed_unix_ns,
                        &sample.sample,
                    )]
                })
                .collect();
            Ok(ProxyResourceBaseline::Remote {
                local: process_resources(local_pids)?,
                identities,
                samples,
            })
        }
    }
}

fn sample_remote_resources(
    target: &ProxyTarget,
    baseline: &mut ProxyResourceBaseline,
) -> Result<(), ExecutorError> {
    let ProxyResourceBaseline::Remote {
        identities,
        samples,
        ..
    } = baseline
    else {
        return Ok(());
    };
    let previous = samples
        .iter()
        .map(|samples| {
            samples
                .last()
                .cloned()
                .ok_or_else(|| ExecutorError::failed("remote probe history is empty"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let current = remote_resource_probe_snapshots(target, identities, Some(&previous))?;
    for (samples, sample) in samples.iter_mut().zip(current) {
        samples.push(sample);
    }
    Ok(())
}

fn proxy_resources_after(
    target: &ProxyTarget,
    local_pids: &[u32],
    mut before: ProxyResourceBaseline,
    report: &BoltLoadgenReport,
) -> Result<MeasuredProxyResources, ExecutorError> {
    if let ProxyResourceBaseline::Remote {
        identities,
        samples,
        ..
    } = &mut before
    {
        let final_samples = remote_full_probe_snapshots(target, Some(identities))?;
        for ((identity, samples), final_sample) in
            identities.iter_mut().zip(samples).zip(final_samples)
        {
            let final_resource = ResourceProbeEnvelope::from_full(
                final_sample.requested_unix_ns,
                final_sample.completed_unix_ns,
                &final_sample.sample,
            );
            final_resource.sample.validate_for(
                target
                    .data_node_processes
                    .iter()
                    .find(|process| process.common.pid == identity.pid)
                    .ok_or_else(|| {
                        ExecutorError::failed("final probe process is not in topology")
                    })?,
                &identity.process_start_id,
                samples.last().map(|sample| &sample.sample),
            )?;
            *identity = final_sample.sample.clone();
            samples.push(final_resource);
        }
    }
    match before {
        ProxyResourceBaseline::Local(before) => Ok(MeasuredProxyResources {
            resources: process_resources_after(local_pids, before, false)?,
            resource_scope: ResourceScope::ClientAndProxyProcesses,
            topology_evidence: None,
        }),
        ProxyResourceBaseline::Remote {
            local,
            identities,
            samples,
        } => {
            let _ = process_resources_after(local_pids, local, true)?;
            measured_remote_resources(target, &identities, &samples, report)
        }
    }
}

fn measured_remote_resources(
    target: &ProxyTarget,
    identities: &[ProcessProbeSnapshot],
    samples: &[Vec<ResourceProbeEnvelope>],
    report: &BoltLoadgenReport,
) -> Result<MeasuredProxyResources, ExecutorError> {
    let mut cpu = 0_u64;
    let mut peak_rss = 0_u64;
    let mut rx = 0_u64;
    let mut tx = 0_u64;
    let mut processes = Vec::with_capacity(samples.len());
    let mut evidence = Vec::with_capacity(samples.len());
    for ((process, identity), samples) in target
        .data_node_processes
        .iter()
        .zip(identities)
        .zip(samples)
    {
        let before = samples
            .iter()
            .filter(|sample| sample.completed_unix_ns <= report.measurement_started_unix_ns)
            .max_by_key(|sample| sample.completed_unix_ns)
            .ok_or_else(|| {
                ExecutorError::failed("remote samples do not bracket measurement start")
            })?;
        let after = samples
            .iter()
            .filter(|sample| sample.requested_unix_ns >= report.measurement_ended_unix_ns)
            .min_by_key(|sample| sample.requested_unix_ns)
            .ok_or_else(|| {
                ExecutorError::failed("remote samples do not bracket measurement end")
            })?;
        let window_peak = samples
            .iter()
            .filter(|sample| {
                let midpoint = sample.midpoint_unix_ns();
                midpoint >= report.measurement_started_unix_ns
                    && midpoint <= report.measurement_ended_unix_ns
            })
            .map(|sample| sample.sample.rss_bytes)
            .max()
            .ok_or_else(|| ExecutorError::failed("remote measurement window has no RSS samples"))?;
        cpu = checked_add(
            cpu,
            after.sample.cpu_time_ns - before.sample.cpu_time_ns,
            "remote CPU aggregation",
        )?;
        peak_rss = checked_add(peak_rss, window_peak, "remote RSS aggregation")?;
        rx = checked_add(
            rx,
            after.sample.network_rx_bytes - before.sample.network_rx_bytes,
            "remote RX aggregation",
        )?;
        tx = checked_add(
            tx,
            after.sample.network_tx_bytes - before.sample.network_tx_bytes,
            "remote TX aggregation",
        )?;
        let probe = process.probe.as_ref().ok_or_else(|| {
            ExecutorError::failed("remote process is missing probe configuration")
        })?;
        processes.push(ProcessTopologyEvidence {
            host_id: identity.host_id.clone(),
            boot_id: identity.boot_id.clone(),
            process_start_id: identity.process_start_id.clone(),
            pid: identity.pid,
            executable: identity.executable.clone(),
            executable_sha256: identity.executable_sha256.clone(),
            listen_address: identity.listen_address.clone(),
            data_interface: probe.data_interface.clone(),
            management_interface: probe.management_interface.clone(),
        });
        evidence.push(HostResourceEvidence {
            host_id: identity.host_id.clone(),
            boot_id: identity.boot_id.clone(),
            process_start_id: identity.process_start_id.clone(),
            pid: identity.pid,
            sampled_before_unix_ns: before.completed_unix_ns,
            sampled_after_unix_ns: after.requested_unix_ns,
            cpu_time_ns: after.sample.cpu_time_ns - before.sample.cpu_time_ns,
            peak_rss_bytes: window_peak,
            network_rx_bytes: after.sample.network_rx_bytes - before.sample.network_rx_bytes,
            network_tx_bytes: after.sample.network_tx_bytes - before.sample.network_tx_bytes,
        });
    }
    Ok(MeasuredProxyResources {
        resources: ResourceMetrics {
            cpu_time_ns: ResourceMetric::Observed { value: cpu },
            peak_rss_bytes: ResourceMetric::Observed { value: peak_rss },
            network_rx_bytes: ResourceMetric::Observed { value: rx },
            network_tx_bytes: ResourceMetric::Observed { value: tx },
        },
        resource_scope: ResourceScope::DataNodeProcesses,
        topology_evidence: Some(TopologyEvidence {
            deployment_mode: TopologyDeploymentMode::RemoteFormal,
            data_nodes: processes,
            resource_samples: evidence,
        }),
    })
}

fn checked_add(left: u64, right: u64, name: &str) -> Result<u64, ExecutorError> {
    left.checked_add(right)
        .ok_or_else(|| ExecutorError::failed(format!("{name} overflow")))
}

fn remote_full_probe_snapshots(
    target: &ProxyTarget,
    previous: Option<&[ProcessProbeSnapshot]>,
) -> Result<Vec<FullProbeEnvelope>, ExecutorError> {
    if target.data_node_processes.len() > 8 {
        return Err(ExecutorError::failed(
            "formal remote probing supports at most 8 data nodes",
        ));
    }
    thread::scope(|scope| {
        let handles = target
            .data_node_processes
            .iter()
            .enumerate()
            .map(|(index, process)| {
                scope.spawn(move || {
                    let requested_unix_ns = unix_time_ns()?;
                    let snapshot = run_remote_probe(process, target.timeout_ms)?;
                    let completed_unix_ns = unix_time_ns()?;
                    snapshot
                        .validate_for(process, previous.and_then(|values| values.get(index)))?;
                    Ok(FullProbeEnvelope {
                        requested_unix_ns,
                        completed_unix_ns,
                        sample: snapshot,
                    })
                })
            })
            .collect::<Vec<_>>();
        handles
            .into_iter()
            .map(|handle| {
                handle
                    .join()
                    .map_err(|_| ExecutorError::failed("remote probe worker panicked"))?
            })
            .collect()
    })
}

fn remote_resource_probe_snapshots(
    target: &ProxyTarget,
    identities: &[ProcessProbeSnapshot],
    previous: Option<&[ResourceProbeEnvelope]>,
) -> Result<Vec<ResourceProbeEnvelope>, ExecutorError> {
    if target.data_node_processes.len() != identities.len() || target.data_node_processes.len() > 8
    {
        return Err(ExecutorError::failed(
            "remote resource probe identity count does not match the topology",
        ));
    }
    thread::scope(|scope| {
        let handles = target
            .data_node_processes
            .iter()
            .zip(identities)
            .enumerate()
            .map(|(index, (process, identity))| {
                scope.spawn(move || {
                    let requested_unix_ns = unix_time_ns()?;
                    let sample = run_remote_resource_probe(process, target.timeout_ms)?;
                    let completed_unix_ns = unix_time_ns()?;
                    sample.validate_for(
                        process,
                        &identity.process_start_id,
                        previous
                            .and_then(|values| values.get(index))
                            .map(|value| &value.sample),
                    )?;
                    Ok(ResourceProbeEnvelope {
                        requested_unix_ns,
                        completed_unix_ns,
                        sample,
                    })
                })
            })
            .collect::<Vec<_>>();
        handles
            .into_iter()
            .map(|handle| {
                handle
                    .join()
                    .map_err(|_| ExecutorError::failed("remote resource probe worker panicked"))?
            })
            .collect()
    })
}

fn run_remote_probe(
    process: &DataNodeProcess,
    timeout_ms: u64,
) -> Result<ProcessProbeSnapshot, ExecutorError> {
    let probe = process.probe.as_ref().ok_or_else(|| {
        ExecutorError::failed("remote_formal data node has no probe configuration")
    })?;
    for value in [
        probe.ssh_target.as_str(),
        probe.probe_binary.to_str().ok_or_else(non_utf8_path)?,
        process
            .common
            .executable
            .to_str()
            .ok_or_else(non_utf8_path)?,
        probe.data_interface.as_str(),
        process.common.listen_address.as_str(),
        process.data_directory.to_str().ok_or_else(non_utf8_path)?,
    ] {
        validate_remote_token(value)?;
    }
    let timeout_seconds = timeout_ms.div_ceil(1_000).max(1).to_string();
    let mut command = Command::new("ssh");
    command
        .args(["-o", "BatchMode=yes"])
        .args(["-o", "ConnectionAttempts=1"])
        .args(["-o", "ClearAllForwardings=yes"])
        .args(["-o", "RequestTTY=no"])
        .args(["-o", "StrictHostKeyChecking=yes"])
        .args(["-o", &format!("ConnectTimeout={timeout_seconds}")])
        .args(["-o", "ServerAliveInterval=5"])
        .args(["-o", "ServerAliveCountMax=1"])
        .arg("--")
        .arg(&probe.ssh_target)
        .arg(&probe.probe_binary)
        .arg("probe-process")
        .args(["--pid", &process.common.pid.to_string()])
        .args(["--network-interface", &probe.data_interface])
        .arg("--executable")
        .arg(&process.common.executable)
        .args(["--listen-address", &process.common.listen_address])
        .arg("--data-directory")
        .arg(&process.data_directory);
    let output = bounded_output(&mut command, Duration::from_millis(timeout_ms.max(1)))?;
    if !output.status.success() {
        return Err(ExecutorError::failed(format!(
            "sealed remote probe failed for {}: {}",
            probe.host_id,
            String::from_utf8_lossy(&output.stderr)
        )));
    }
    parse_probe_line(&output.stdout, "remote probe")
}

fn run_remote_resource_probe(
    process: &DataNodeProcess,
    timeout_ms: u64,
) -> Result<ResourceProbeSnapshot, ExecutorError> {
    let probe = process.probe.as_ref().ok_or_else(|| {
        ExecutorError::failed("remote_formal data node has no probe configuration")
    })?;
    for value in [
        probe.ssh_target.as_str(),
        probe.probe_binary.to_str().ok_or_else(non_utf8_path)?,
        probe.data_interface.as_str(),
    ] {
        validate_remote_token(value)?;
    }
    let timeout_seconds = timeout_ms.div_ceil(1_000).max(1).to_string();
    let mut command = Command::new("ssh");
    command
        .args(["-o", "BatchMode=yes"])
        .args(["-o", "ConnectionAttempts=1"])
        .args(["-o", "ClearAllForwardings=yes"])
        .args(["-o", "RequestTTY=no"])
        .args(["-o", "StrictHostKeyChecking=yes"])
        .args(["-o", &format!("ConnectTimeout={timeout_seconds}")])
        .args(["-o", "ServerAliveInterval=5"])
        .args(["-o", "ServerAliveCountMax=1"])
        .arg("--")
        .arg(&probe.ssh_target)
        .arg(&probe.probe_binary)
        .arg("probe-resources")
        .args(["--pid", &process.common.pid.to_string()])
        .args(["--network-interface", &probe.data_interface]);
    let output = bounded_output(&mut command, Duration::from_millis(timeout_ms.max(1)))?;
    if !output.status.success() {
        return Err(ExecutorError::failed(format!(
            "sealed remote resource probe failed for {}: {}",
            probe.host_id,
            String::from_utf8_lossy(&output.stderr)
        )));
    }
    parse_resource_probe_line(&output.stdout, "remote resource probe")
}

fn parse_probe_line(bytes: &[u8], name: &str) -> Result<ProcessProbeSnapshot, ExecutorError> {
    if bytes.len() > 16 * 1_024
        || !bytes.ends_with(b"\n")
        || bytes[..bytes.len().saturating_sub(1)].contains(&b'\n')
    {
        return Err(ExecutorError::failed(format!(
            "{name} must return one newline-terminated JSON line of at most 16 KiB"
        )));
    }
    serde_json::from_slice(bytes)
        .map_err(|error| ExecutorError::failed(format!("invalid {name} JSON: {error}")))
}

fn parse_resource_probe_line(
    bytes: &[u8],
    name: &str,
) -> Result<ResourceProbeSnapshot, ExecutorError> {
    if bytes.len() > 4 * 1_024
        || !bytes.ends_with(b"\n")
        || bytes[..bytes.len().saturating_sub(1)].contains(&b'\n')
    {
        return Err(ExecutorError::failed(format!(
            "{name} must return one newline-terminated JSON line of at most 4 KiB"
        )));
    }
    serde_json::from_slice(bytes)
        .map_err(|error| ExecutorError::failed(format!("invalid {name} JSON: {error}")))
}

fn bounded_output(command: &mut Command, timeout: Duration) -> Result<Output, ExecutorError> {
    const OUTPUT_LIMIT: usize = 64 * 1_024;
    command.stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = command.spawn().map_err(failed)?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| ExecutorError::failed("failed to capture command stdout"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| ExecutorError::failed("failed to capture command stderr"))?;
    let stdout_reader = thread::spawn(move || read_bounded(stdout, OUTPUT_LIMIT));
    let stderr_reader = thread::spawn(move || read_bounded(stderr, OUTPUT_LIMIT));
    let deadline = Instant::now() + timeout;
    let status = loop {
        if let Some(status) = child.try_wait().map_err(failed)? {
            break status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            let _ = stdout_reader.join();
            let _ = stderr_reader.join();
            return Err(ExecutorError::failed("command exceeded its time bound"));
        }
        thread::sleep(Duration::from_millis(10));
    };
    let (stdout, stdout_overflow) = stdout_reader
        .join()
        .map_err(|_| ExecutorError::failed("stdout reader panicked"))??;
    let (stderr, stderr_overflow) = stderr_reader
        .join()
        .map_err(|_| ExecutorError::failed("stderr reader panicked"))??;
    if stdout_overflow || stderr_overflow {
        return Err(ExecutorError::failed(
            "command output exceeded the 64 KiB bound",
        ));
    }
    Ok(Output {
        status,
        stdout,
        stderr,
    })
}

fn read_bounded(mut reader: impl Read, limit: usize) -> Result<(Vec<u8>, bool), ExecutorError> {
    let mut output = Vec::with_capacity(limit.min(8 * 1_024));
    let mut overflow = false;
    let mut buffer = [0_u8; 8 * 1_024];
    loop {
        let read = reader.read(&mut buffer).map_err(failed)?;
        if read == 0 {
            break;
        }
        let remaining = limit.saturating_sub(output.len());
        output.extend_from_slice(&buffer[..read.min(remaining)]);
        overflow |= read > remaining;
    }
    Ok((output, overflow))
}

fn validate_sha256(value: &str, name: &str) -> Result<(), ExecutorError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(ExecutorError::failed(format!(
            "{name} must be a lowercase SHA-256 digest"
        )));
    }
    Ok(())
}

fn validate_remote_token(value: &str) -> Result<(), ExecutorError> {
    if value.is_empty()
        || value.starts_with('-')
        || !value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric()
                || matches!(
                    byte,
                    b'/' | b'.' | b'_' | b'-' | b':' | b'@' | b'[' | b']' | b'%' | b'+' | b'='
                )
        })
    {
        return Err(ExecutorError::failed(
            "remote command token contains unsupported characters",
        ));
    }
    Ok(())
}

#[derive(Clone, Copy)]
struct ResourceBaseline {
    cpu_time_ns: u64,
    peak_rss_bytes: u64,
    network_rx_bytes: u64,
    network_tx_bytes: u64,
}

fn process_resources(pids: &[u32]) -> Result<ResourceBaseline, ExecutorError> {
    let (network_rx_bytes, network_tx_bytes) = network_bytes()?;
    Ok(ResourceBaseline {
        cpu_time_ns: aggregate_cpu_time_ns(pids)?,
        peak_rss_bytes: aggregate_rss_bytes(pids)?,
        network_rx_bytes,
        network_tx_bytes,
    })
}

fn process_resources_after(
    pids: &[u32],
    before: ResourceBaseline,
    local_storage: bool,
) -> Result<ResourceMetrics, ExecutorError> {
    let after = process_resources(pids)?;
    let (network_rx_bytes, network_tx_bytes) = if local_storage {
        (0, 0)
    } else {
        (
            after
                .network_rx_bytes
                .saturating_sub(before.network_rx_bytes),
            after
                .network_tx_bytes
                .saturating_sub(before.network_tx_bytes),
        )
    };
    Ok(ResourceMetrics {
        cpu_time_ns: ResourceMetric::Observed {
            value: after.cpu_time_ns.saturating_sub(before.cpu_time_ns),
        },
        peak_rss_bytes: ResourceMetric::Observed {
            value: before.peak_rss_bytes.max(after.peak_rss_bytes),
        },
        network_rx_bytes: ResourceMetric::Observed {
            value: network_rx_bytes,
        },
        network_tx_bytes: ResourceMetric::Observed {
            value: network_tx_bytes,
        },
    })
}

fn aggregate_rss_bytes(pids: &[u32]) -> Result<u64, ExecutorError> {
    pids.iter().try_fold(0_u64, |total, pid| {
        let output = Command::new("ps")
            .args(["-o", "rss=", "-p", &pid.to_string()])
            .output()
            .map_err(failed)?;
        if !output.status.success() {
            return Err(ExecutorError::failed(format!(
                "failed to observe RSS for PID {pid}"
            )));
        }
        let rss_kib = String::from_utf8(output.stdout)
            .map_err(failed)?
            .trim()
            .parse::<u64>()
            .map_err(failed)?;
        total
            .checked_add(rss_kib.saturating_mul(1_024))
            .ok_or_else(|| ExecutorError::failed("RSS observation overflow"))
    })
}

fn aggregate_cpu_time_ns(pids: &[u32]) -> Result<u64, ExecutorError> {
    pids.iter().try_fold(0_u64, |total, pid| {
        let output = Command::new("ps")
            .args(["-o", "time=", "-p", &pid.to_string()])
            .output()
            .map_err(failed)?;
        if !output.status.success() {
            return Err(ExecutorError::failed(format!(
                "failed to observe CPU time for PID {pid}"
            )));
        }
        total
            .checked_add(parse_cpu_time_ns(
                String::from_utf8(output.stdout).map_err(failed)?.trim(),
            )?)
            .ok_or_else(|| ExecutorError::failed("CPU observation overflow"))
    })
}

fn process_executable(pid: u32) -> Result<PathBuf, ExecutorError> {
    let proc_exe = PathBuf::from(format!("/proc/{pid}/exe"));
    if proc_exe.exists() {
        return fs::read_link(proc_exe).map_err(failed);
    }
    let output = Command::new("ps")
        .args(["-o", "comm=", "-p", &pid.to_string()])
        .output()
        .map_err(failed)?;
    let value = String::from_utf8(output.stdout).map_err(failed)?;
    let executable = PathBuf::from(value.trim());
    if !output.status.success() || !executable.is_absolute() || !executable.is_file() {
        return Err(ExecutorError::failed(format!(
            "failed to identify executable for PID {pid}"
        )));
    }
    Ok(executable)
}

fn process_start_identity(pid: u32) -> Result<String, ExecutorError> {
    let stat = PathBuf::from(format!("/proc/{pid}/stat"));
    if stat.is_file() {
        let value = fs::read_to_string(stat).map_err(failed)?;
        let (_, fields) = value
            .rsplit_once(") ")
            .ok_or_else(|| ExecutorError::failed("invalid Linux process stat"))?;
        let start_ticks = fields
            .split_whitespace()
            .nth(19)
            .ok_or_else(|| ExecutorError::failed("Linux process start time is unavailable"))?;
        return Ok(format!("linux-start-ticks:{start_ticks}"));
    }
    let output = Command::new("ps")
        .args(["-o", "lstart=", "-p", &pid.to_string()])
        .output()
        .map_err(failed)?;
    let value = String::from_utf8(output.stdout).map_err(failed)?;
    if !output.status.success() || value.trim().is_empty() {
        return Err(ExecutorError::failed(format!(
            "failed to identify start time for PID {pid}"
        )));
    }
    Ok(format!(
        "ps-lstart-sha256:{}",
        paper_benchmark::sha256_bytes(value.trim().as_bytes())
    ))
}

fn stable_host_id() -> Result<String, ExecutorError> {
    for path in ["/etc/machine-id", "/var/lib/dbus/machine-id"] {
        if let Ok(value) = fs::read_to_string(path) {
            let value = value.trim();
            if !value.is_empty() {
                return Ok(format!(
                    "machine-sha256:{}",
                    paper_benchmark::sha256_bytes(value.as_bytes())
                ));
            }
        }
    }
    let output = Command::new("ioreg")
        .args(["-rd1", "-c", "IOPlatformExpertDevice"])
        .output()
        .map_err(failed)?;
    let text = String::from_utf8(output.stdout).map_err(failed)?;
    let uuid = text
        .lines()
        .find(|line| line.contains("IOPlatformUUID"))
        .and_then(|line| line.split('=').nth(1))
        .map(|value| value.trim().trim_matches('"'))
        .filter(|value| !value.is_empty())
        .ok_or_else(|| ExecutorError::failed("stable host identity is unavailable"))?;
    Ok(format!(
        "platform-sha256:{}",
        paper_benchmark::sha256_bytes(uuid.as_bytes())
    ))
}

fn boot_identity() -> Result<String, ExecutorError> {
    if let Ok(value) = fs::read_to_string("/proc/sys/kernel/random/boot_id") {
        let value = value.trim();
        if !value.is_empty() {
            return Ok(value.to_owned());
        }
    }
    let output = Command::new("sysctl")
        .args(["-n", "kern.boottime"])
        .output()
        .map_err(failed)?;
    let value = String::from_utf8(output.stdout).map_err(failed)?;
    if !output.status.success() || value.trim().is_empty() {
        return Err(ExecutorError::failed("boot identity is unavailable"));
    }
    Ok(format!(
        "boottime-sha256:{}",
        paper_benchmark::sha256_bytes(value.trim().as_bytes())
    ))
}

fn process_rss(pid: u32) -> Result<(u64, u64), ExecutorError> {
    let status = PathBuf::from(format!("/proc/{pid}/status"));
    if status.is_file() {
        let text = fs::read_to_string(status).map_err(failed)?;
        let kib = |name: &str| -> Result<u64, ExecutorError> {
            text.lines()
                .find(|line| line.starts_with(name))
                .and_then(|line| line.split_whitespace().nth(1))
                .ok_or_else(|| ExecutorError::failed(format!("{name} is unavailable")))?
                .parse::<u64>()
                .map_err(failed)
        };
        return Ok((
            kib("VmRSS:")?.saturating_mul(1_024),
            kib("VmHWM:")?.saturating_mul(1_024),
        ));
    }
    let rss = aggregate_rss_bytes(&[pid])?;
    Ok((rss, rss))
}

fn interface_network_bytes(interface: &str) -> Result<(u64, u64), ExecutorError> {
    let statistics = PathBuf::from("/sys/class/net")
        .join(interface)
        .join("statistics");
    if statistics.is_dir() {
        let read = |name: &str| -> Result<u64, ExecutorError> {
            fs::read_to_string(statistics.join(name))
                .map_err(failed)?
                .trim()
                .parse::<u64>()
                .map_err(failed)
        };
        return Ok((read("rx_bytes")?, read("tx_bytes")?));
    }
    let output = Command::new("netstat")
        .args(["-ibn", "-I", interface])
        .output()
        .map_err(failed)?;
    if !output.status.success() {
        return Err(ExecutorError::failed(format!(
            "failed to observe network interface {interface}"
        )));
    }
    parse_netstat_interface_bytes(
        &String::from_utf8(output.stdout).map_err(failed)?,
        interface,
    )
}

fn parse_netstat_interface_bytes(text: &str, interface: &str) -> Result<(u64, u64), ExecutorError> {
    let mut lines = text.lines();
    let header = lines
        .find(|line| line.contains("Ibytes") && line.contains("Obytes"))
        .ok_or_else(|| ExecutorError::failed("netstat byte columns are unavailable"))?;
    let columns = header.split_whitespace().collect::<Vec<_>>();
    let rx = columns
        .iter()
        .position(|column| *column == "Ibytes")
        .ok_or_else(|| ExecutorError::failed("netstat Ibytes column is unavailable"))?;
    let tx = columns
        .iter()
        .position(|column| *column == "Obytes")
        .ok_or_else(|| ExecutorError::failed("netstat Obytes column is unavailable"))?;
    let values = lines
        .find(|line| line.split_whitespace().next() == Some(interface))
        .ok_or_else(|| ExecutorError::failed("netstat interface row is unavailable"))?
        .split_whitespace()
        .collect::<Vec<_>>();
    Ok((
        values
            .get(rx)
            .ok_or_else(|| ExecutorError::failed("invalid netstat row"))?
            .parse()
            .map_err(failed)?,
        values
            .get(tx)
            .ok_or_else(|| ExecutorError::failed("invalid netstat row"))?
            .parse()
            .map_err(failed)?,
    ))
}

fn verify_process_listener(pid: u32, listen_address: &str) -> Result<(), ExecutorError> {
    listen_address
        .parse::<SocketAddr>()
        .map_err(|_| ExecutorError::failed("invalid claimed process listener"))?;
    let mut command = Command::new("lsof");
    command.args([
        "-nP",
        "-a",
        "-p",
        &pid.to_string(),
        "-iTCP",
        "-sTCP:LISTEN",
        "-Fn",
    ]);
    let output = bounded_output(&mut command, Duration::from_secs(5))?;
    let text = String::from_utf8(output.stdout).map_err(failed)?;
    if !output.status.success()
        || !text
            .lines()
            .any(|line| line.strip_prefix('n') == Some(listen_address))
    {
        return Err(ExecutorError::failed(format!(
            "PID {pid} does not own listener {listen_address}"
        )));
    }
    Ok(())
}

fn verify_process_data_directory(pid: u32, directory: &Path) -> Result<(), ExecutorError> {
    if !directory.is_absolute() || !directory.is_dir() {
        return Err(ExecutorError::failed(
            "claimed process data directory must be an existing absolute directory",
        ));
    }
    let expected = fs::canonicalize(directory).map_err(failed)?;
    let fd_root = PathBuf::from(format!("/proc/{pid}/fd"));
    if fd_root.is_dir() {
        for entry in fs::read_dir(fd_root).map_err(failed)? {
            if let Ok(target) = fs::read_link(entry.map_err(failed)?.path()) {
                if target.starts_with(&expected) {
                    return Ok(());
                }
            }
        }
    } else {
        let mut command = Command::new("lsof");
        command.args(["-nP", "-a", "-p", &pid.to_string(), "-Fn"]);
        let output = bounded_output(&mut command, Duration::from_secs(5))?;
        let expected = expected.to_string_lossy();
        if output.status.success()
            && String::from_utf8(output.stdout)
                .map_err(failed)?
                .lines()
                .filter_map(|line| line.strip_prefix('n'))
                .any(|path| path == expected || path.starts_with(&format!("{expected}/")))
        {
            return Ok(());
        }
    }
    Err(ExecutorError::failed(format!(
        "PID {pid} has no open file under {}",
        directory.display()
    )))
}

fn parse_cpu_time_ns(value: &str) -> Result<u64, ExecutorError> {
    let (days, clock) = value
        .split_once('-')
        .map_or((0_u64, value), |(days, clock)| {
            (days.parse::<u64>().unwrap_or(u64::MAX), clock)
        });
    if days == u64::MAX {
        return Err(ExecutorError::failed("invalid ps CPU time"));
    }
    let parts = clock.split(':').collect::<Vec<_>>();
    if !(2..=3).contains(&parts.len()) {
        return Err(ExecutorError::failed("invalid ps CPU time"));
    }
    let seconds = parts[parts.len() - 1].parse::<f64>().map_err(failed)?;
    let minutes = parts[parts.len() - 2].parse::<u64>().map_err(failed)?;
    let hours = if parts.len() == 3 {
        parts[0].parse::<u64>().map_err(failed)?
    } else {
        0
    };
    let whole_seconds = days
        .checked_mul(86_400)
        .and_then(|value| value.checked_add(hours.saturating_mul(3_600)))
        .and_then(|value| value.checked_add(minutes.saturating_mul(60)))
        .ok_or_else(|| ExecutorError::failed("CPU observation overflow"))?;
    let total = whole_seconds as f64 + seconds;
    if !total.is_finite() || total < 0.0 || total > u64::MAX as f64 / 1_000_000_000.0 {
        return Err(ExecutorError::failed("invalid ps CPU time"));
    }
    Ok((total * 1_000_000_000.0).round() as u64)
}

fn network_bytes() -> Result<(u64, u64), ExecutorError> {
    let linux = Path::new("/sys/class/net/lo/statistics");
    if linux.is_dir() {
        let read = |name: &str| -> Result<u64, ExecutorError> {
            fs::read_to_string(linux.join(name))
                .map_err(failed)?
                .trim()
                .parse::<u64>()
                .map_err(failed)
        };
        return Ok((read("rx_bytes")?, read("tx_bytes")?));
    }
    let output = Command::new("netstat")
        .args(["-ibn", "-I", "lo0"])
        .output()
        .map_err(failed)?;
    if !output.status.success() {
        return Err(ExecutorError::failed(
            "failed to observe loopback network bytes",
        ));
    }
    let text = String::from_utf8(output.stdout).map_err(failed)?;
    let mut lines = text.lines();
    let header = lines
        .find(|line| line.contains("Ibytes") && line.contains("Obytes"))
        .ok_or_else(|| ExecutorError::failed("netstat byte columns are unavailable"))?;
    let columns = header.split_whitespace().collect::<Vec<_>>();
    let rx = columns
        .iter()
        .position(|column| *column == "Ibytes")
        .ok_or_else(|| ExecutorError::failed("netstat Ibytes column is unavailable"))?;
    let tx = columns
        .iter()
        .position(|column| *column == "Obytes")
        .ok_or_else(|| ExecutorError::failed("netstat Obytes column is unavailable"))?;
    let values = lines
        .find(|line| line.starts_with("lo0 "))
        .ok_or_else(|| ExecutorError::failed("netstat lo0 row is unavailable"))?
        .split_whitespace()
        .collect::<Vec<_>>();
    Ok((
        values
            .get(rx)
            .ok_or_else(|| ExecutorError::failed("invalid netstat lo0 row"))?
            .parse()
            .map_err(failed)?,
        values
            .get(tx)
            .ok_or_else(|| ExecutorError::failed("invalid netstat lo0 row"))?
            .parse()
            .map_err(failed)?,
    ))
}

fn validate_snapshot_paths(paths: &[PathBuf]) -> Result<(), ExecutorError> {
    if paths.is_empty() {
        return Err(ExecutorError::Unavailable(
            "RocksDB snapshot path list is empty".into(),
        ));
    }
    let mut unique = BTreeSet::new();
    for path in paths {
        if !path.is_absolute() || !path.is_dir() || !unique.insert(path) {
            return Err(ExecutorError::failed(
                "RocksDB snapshot paths must be existing, absolute, and distinct",
            ));
        }
    }
    Ok(())
}

fn validate_executable(path: &Path, name: &str) -> Result<(), ExecutorError> {
    if !path.is_absolute() || !path.is_file() {
        return Err(ExecutorError::failed(format!(
            "{name} executable must be an existing absolute file"
        )));
    }
    Ok(())
}

fn verify_live_process(pid: u32, executable: &Path, role: &str) -> Result<(), ExecutorError> {
    let proc_exe = PathBuf::from(format!("/proc/{pid}/exe"));
    if proc_exe.exists() {
        let actual = fs::canonicalize(&proc_exe).map_err(failed)?;
        let expected = fs::canonicalize(executable).map_err(failed)?;
        if actual != expected {
            return Err(ExecutorError::failed(format!(
                "{role} PID {pid} is not running {}",
                executable.display()
            )));
        }
        return Ok(());
    }
    let output = Command::new("ps")
        .args(["-o", "command=", "-p", &pid.to_string()])
        .output()
        .map_err(failed)?;
    let command = String::from_utf8(output.stdout).map_err(failed)?;
    if !output.status.success() || !command.contains(executable.to_string_lossy().as_ref()) {
        return Err(ExecutorError::failed(format!(
            "{role} PID {pid} is not a live {} process",
            executable.display()
        )));
    }
    Ok(())
}

fn unique_loadgen_report_path(config: &CellConfig) -> Result<PathBuf, ExecutorError> {
    let root = env::temp_dir();
    let path = root.join(format!(
        "dtgproxy-paper-loadgen-{}-{:06}.json",
        std::process::id(),
        config.sequence
    ));
    if path.exists() {
        return Err(ExecutorError::failed(format!(
            "loadgen report path already exists: {}",
            path.display()
        )));
    }
    Ok(path)
}

fn write_new_atomic(path: &Path, bytes: &[u8]) -> Result<(), ExecutorError> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    if !parent.is_dir() {
        return Err(ExecutorError::failed(format!(
            "output parent does not exist: {}",
            parent.display()
        )));
    }
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("output");
    let temporary = parent.join(format!(".{file_name}.{}.tmp", std::process::id()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)
        .map_err(failed)?;
    let write_result = (|| -> Result<(), ExecutorError> {
        file.write_all(bytes).map_err(failed)?;
        file.write_all(b"\n").map_err(failed)?;
        file.sync_all().map_err(failed)?;
        drop(file);
        fs::hard_link(&temporary, path).map_err(failed)?;
        fs::remove_file(&temporary).map_err(failed)?;
        OpenOptions::new()
            .read(true)
            .open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(failed)
    })();
    if write_result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    write_result
}

fn blake3_file(path: &Path) -> Result<String, ExecutorError> {
    Ok(blake3::hash(&fs::read(path).map_err(failed)?)
        .to_hex()
        .to_string())
}

fn read_json<T: for<'de> Deserialize<'de>>(path: &Path, name: &str) -> Result<T, ExecutorError> {
    let bytes = fs::read(path).map_err(|error| {
        ExecutorError::failed(format!("failed to read {name} {}: {error}", path.display()))
    })?;
    serde_json::from_slice(&bytes).map_err(|error| {
        ExecutorError::failed(format!("invalid {name} {}: {error}", path.display()))
    })
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
            Poll::Pending => thread::yield_now(),
        }
    }
}

fn default_timeout_ms() -> u64 {
    30_000
}

fn duration_ns(duration: Duration) -> Result<u64, ExecutorError> {
    u64::try_from(duration.as_nanos())
        .map_err(|_| ExecutorError::failed("duration does not fit u64 nanoseconds"))
}

fn seconds_ns(seconds: u64) -> Result<u64, ExecutorError> {
    seconds
        .checked_mul(1_000_000_000)
        .ok_or_else(|| ExecutorError::failed("duration overflow"))
}

fn unix_time_ns() -> Result<u64, ExecutorError> {
    duration_ns(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(failed)?,
    )
}

fn non_utf8_path() -> ExecutorError {
    ExecutorError::failed("path is not valid UTF-8")
}

fn failed(error: impl std::fmt::Display) -> ExecutorError {
    ExecutorError::failed(error.to_string())
}

struct OptionsMap {
    cell_config: PathBuf,
    output: PathBuf,
}

impl OptionsMap {
    fn parse(arguments: Vec<String>) -> Result<Self, ExecutorError> {
        if arguments.len() == 1 && matches!(arguments[0].as_str(), "--help" | "-h") {
            println!("{USAGE}");
            std::process::exit(0);
        }
        let mut options = parse_options(arguments)?;
        let cell_config = required_option(&mut options, "--cell-config")?;
        let output = required_option(&mut options, "--output")?;
        if !options.is_empty() {
            return Err(ExecutorError::Usage(format!(
                "unknown options: {}",
                options.keys().cloned().collect::<Vec<_>>().join(", ")
            )));
        }
        Ok(Self {
            cell_config: PathBuf::from(cell_config),
            output: PathBuf::from(output),
        })
    }
}

fn parse_options(arguments: Vec<String>) -> Result<BTreeMap<String, String>, ExecutorError> {
    let mut options = BTreeMap::new();
    let mut index = 0;
    while index < arguments.len() {
        let key = arguments[index].clone();
        if !key.starts_with("--") {
            return Err(ExecutorError::Usage(format!(
                "expected an option, found {key}"
            )));
        }
        let value = arguments
            .get(index + 1)
            .ok_or_else(|| ExecutorError::Usage(format!("missing value for {key}")))?
            .clone();
        if options.insert(key.clone(), value).is_some() {
            return Err(ExecutorError::Usage(format!("duplicate option {key}")));
        }
        index += 2;
    }
    Ok(options)
}

fn required_option(
    options: &mut BTreeMap<String, String>,
    name: &str,
) -> Result<String, ExecutorError> {
    options
        .remove(name)
        .ok_or_else(|| ExecutorError::Usage(format!("missing required option {name}")))
}

#[derive(Debug)]
enum ExecutorError {
    Usage(String),
    Unavailable(String),
    Failed(String),
}

impl ExecutorError {
    fn failed(message: impl Into<String>) -> Self {
        Self::Failed(message.into())
    }
}
