mod backend_e2e_support;

use std::collections::BTreeMap;
use std::env;
use std::io;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use backend_e2e_support::{
    Backend, CellSpec, DiagnosticCluster, DiagnosticRuntime, RawObservation, Workload,
    percentile_ns, stage_metrics_window_from_log,
};

#[derive(serde::Serialize)]
struct QuickResult {
    backend: Backend,
    workload: Workload,
    concurrency: usize,
    operations: u64,
    warmup_operations: u64,
    persisted_operations: u64,
    throughput_ops_per_second: f64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    errors: u64,
    result_digest: String,
}

#[derive(serde::Serialize)]
struct StageMean {
    process: &'static str,
    stage: String,
    calls: u64,
    mean_nanoseconds: u64,
    errors: u64,
}

fn stage_means(observation: &RawObservation) -> Vec<StageMean> {
    [
        ("gateway", observation.gateway_stage_metrics.as_ref()),
        ("data", observation.data_stage_metrics.as_ref()),
    ]
    .into_iter()
    .flat_map(|(process, window)| {
        window.into_iter().flat_map(move |window| {
            window
                .delta
                .stages
                .iter()
                .filter(|stage| stage.success != 0)
                .map(move |stage| StageMean {
                    process,
                    stage: stage.stage.clone(),
                    calls: stage.success,
                    mean_nanoseconds: stage.total_nanoseconds / stage.success,
                    errors: stage.error,
                })
        })
    })
    .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "requires release DTGProxy binaries and the selected live backend"]
async fn quick_selected_backend_e2e_comparison() {
    let runtime = DiagnosticRuntime::from_env().unwrap();
    let backend = match env::var("DTG_BACKEND_E2E_SELECTED_BACKEND").as_deref() {
        Ok("fjall") => Backend::Fjall,
        Ok("postgresql") => Backend::PostgreSql,
        Ok("kuzu") => Backend::Kuzu,
        _ => panic!("DTG_BACKEND_E2E_SELECTED_BACKEND must be fjall, postgresql, or kuzu"),
    };
    let output = env::var_os("DTG_BACKEND_E2E_QUICK_OUTPUT").map(PathBuf::from);
    let repetitions = if output.is_some() {
        let configured = env::var("DTG_BACKEND_E2E_QUICK_REPETITIONS")
            .expect("DTG_BACKEND_E2E_QUICK_REPETITIONS is required with quick output")
            .parse::<u8>()
            .expect("DTG_BACKEND_E2E_QUICK_REPETITIONS must be an integer");
        assert_eq!(configured, 3, "quick artifact requires three repetitions");
        configured
    } else {
        1
    };
    let mut observations = Vec::with_capacity(usize::from(repetitions) * 8);
    for repetition in 0..repetitions {
        for workload in [
            Workload::CreateVertex,
            Workload::PointLookup,
            Workload::OneHopExpand,
            Workload::TwoHopExpand,
            Workload::CountVertices,
        ] {
            for concurrency in [1, 8, 64] {
                let spec = CellSpec {
                    backend,
                    workload,
                    concurrency,
                    repetition,
                };
                let mut cluster = DiagnosticCluster::start(&runtime, spec).await.unwrap();
                if !workload.is_write() {
                    cluster.seed_read_dataset(4_096).await.unwrap();
                }
                let observation = cluster.measure_cell(spec).await.unwrap();
                cluster.shutdown().await.unwrap();
                assert_eq!(observation.errors, 0);
                assert!(!observation.latency_samples_ns.is_empty());
                let result = QuickResult {
                    backend,
                    workload,
                    concurrency,
                    operations: observation.operations,
                    warmup_operations: observation.warmup_operations,
                    persisted_operations: observation.persisted_operations,
                    throughput_ops_per_second: observation.operations as f64 * 1_000_000_000.0
                        / observation.measured_duration_ns as f64,
                    p50_ms: percentile_ns(&observation.latency_samples_ns, 50) as f64 / 1_000_000.0,
                    p95_ms: percentile_ns(&observation.latency_samples_ns, 95) as f64 / 1_000_000.0,
                    p99_ms: percentile_ns(&observation.latency_samples_ns, 99) as f64 / 1_000_000.0,
                    errors: observation.errors,
                    result_digest: observation.result_digest.clone(),
                };
                println!(
                    "DTG_BACKEND_E2E_QUICK_RESULT={}",
                    serde_json::to_string(&result).unwrap()
                );
                observations.push(observation);
            }
        }
    }
    if let Some(output) = output {
        let artifact =
            backend_e2e_support::QuickDiagnosticArtifact::new(current_revision(), observations)
                .unwrap();
        backend_e2e_support::write_quick_artifact(&output, &artifact).unwrap();
    }
}

fn current_revision() -> String {
    let output = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .expect("git must resolve the quick diagnostic revision");
    assert!(output.status.success(), "git rev-parse HEAD failed");
    String::from_utf8(output.stdout)
        .expect("git revision must be UTF-8")
        .trim()
        .to_owned()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires release DTGProxy binaries"]
async fn fjall_cell_uses_real_four_process_bolt_path() {
    let runtime = DiagnosticRuntime::from_env().unwrap();
    let spec = CellSpec::one(Backend::Fjall, Workload::PointLookup, 1, 1);
    let mut cluster = DiagnosticCluster::start(&runtime, spec).await.unwrap();
    cluster.seed_read_dataset(4_096).await.unwrap();
    let mut session = backend_e2e_support::BoltSession::connect(cluster.bolt_address())
        .await
        .unwrap();
    let created = session
        .run("CREATE (n:Bench {value: 1}) VALID FROM 1", BTreeMap::new())
        .await
        .unwrap();
    assert!(created.fields.is_empty());
    assert!(created.rows.is_empty());
    let count = session
        .run("MATCH (n) RETURN COUNT(*)", BTreeMap::new())
        .await
        .unwrap();
    assert_eq!(count.fields, vec!["COUNT(*)"]);
    assert_eq!(
        count.rows,
        vec![vec![backend_e2e_support::BoltValue::Integer(4_097)]]
    );
    let observation = cluster.measure_cell(spec).await.unwrap();
    assert_eq!(observation.errors, 0);
    assert_eq!(observation.row_count, 1);
    let provider_execution = observation
        .data_stage_metrics
        .as_ref()
        .unwrap()
        .delta
        .stages
        .iter()
        .find(|stage| stage.stage == "data_provider_execution")
        .unwrap();
    assert_eq!(provider_execution.error, 0);
    println!(
        "DTG_GATEWAY_FINAL_METRICS={}",
        cluster.last_request_metrics_line("gateway").unwrap()
    );
    println!(
        "DTG_DATA_FINAL_METRICS={}",
        cluster.last_request_metrics_line("data").unwrap()
    );
    cluster.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires release DTGProxy binaries"]
async fn fjall_one_hop_uses_the_real_four_process_bolt_path() {
    let runtime = DiagnosticRuntime::from_env().unwrap();
    let spec = CellSpec::one(Backend::Fjall, Workload::OneHopExpand, 1, 1);
    let mut cluster = DiagnosticCluster::start(&runtime, spec).await.unwrap();
    cluster.seed_read_dataset(4_096).await.unwrap();
    let mut session = backend_e2e_support::BoltSession::connect(cluster.bolt_address())
        .await
        .unwrap();
    let one_hop = session
        .run(
            "MATCH (a)-[r]->(b) WHERE a.id = $id RETURN r",
            BTreeMap::from([("id".into(), backend_e2e_support::BoltValue::Integer(2048))]),
        )
        .await
        .unwrap();
    assert_eq!(one_hop.rows.len(), 1);
    let observation = cluster.measure_cell(spec).await.unwrap();
    assert_eq!(observation.errors, 0);
    let provider_execution = observation
        .data_stage_metrics
        .as_ref()
        .unwrap()
        .delta
        .stages
        .iter()
        .find(|stage| stage.stage == "data_provider_execution")
        .unwrap();
    assert_eq!(provider_execution.error, 0);
    let details = &observation
        .data_stage_metrics
        .as_ref()
        .unwrap()
        .delta
        .details;
    assert!(
        details
            .iter()
            .find(|detail| detail.detail == "data_snapshot_csr_cache_hit")
            .unwrap()
            .success
            > 0
    );
    assert_eq!(
        details
            .iter()
            .find(|detail| detail.detail == "data_adjacency_backend_expand")
            .unwrap()
            .success,
        0
    );
    println!(
        "DTG_BACKEND_E2E_QUICK_RESULT={}",
        serde_json::to_string(&QuickResult {
            backend: Backend::Fjall,
            workload: Workload::OneHopExpand,
            concurrency: 1,
            operations: observation.operations,
            warmup_operations: observation.warmup_operations,
            persisted_operations: observation.persisted_operations,
            throughput_ops_per_second: observation.operations as f64 * 1_000_000_000.0
                / observation.measured_duration_ns as f64,
            p50_ms: percentile_ns(&observation.latency_samples_ns, 50) as f64 / 1_000_000.0,
            p95_ms: percentile_ns(&observation.latency_samples_ns, 95) as f64 / 1_000_000.0,
            p99_ms: percentile_ns(&observation.latency_samples_ns, 99) as f64 / 1_000_000.0,
            errors: observation.errors,
            result_digest: observation.result_digest.clone(),
        })
        .unwrap()
    );
    println!(
        "DTG_BACKEND_E2E_STAGE_MEANS={}",
        serde_json::to_string(&stage_means(&observation)).unwrap()
    );
    cluster.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "requires release DTGProxy binaries"]
async fn fjall_pipeline_c64_one_hop_completes_without_stalling() {
    let runtime = DiagnosticRuntime::from_env().unwrap();
    let spec = CellSpec::one(Backend::Fjall, Workload::OneHopExpand, 64, 1);
    let mut cluster = DiagnosticCluster::start(&runtime, spec).await.unwrap();
    cluster.seed_read_dataset(4_096).await.unwrap();
    let observation = tokio::time::timeout(Duration::from_secs(20), cluster.measure_cell(spec))
        .await
        .expect("pipeline c64 one-hop measurement must not stall")
        .unwrap();
    assert_eq!(observation.errors, 0);
    assert!(!observation.latency_samples_ns.is_empty());
    let gateway_metrics = observation
        .gateway_stage_metrics
        .as_ref()
        .expect("Gateway stage metrics must be available");
    for detail in [
        "gateway_query_pipeline_submit",
        "gateway_query_pipeline_response_wait",
    ] {
        assert!(
            gateway_metrics
                .delta
                .details
                .iter()
                .any(|candidate| candidate.detail == detail && candidate.success != 0),
            "pipeline c64 traffic must record {detail}",
        );
    }
    cluster.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "requires release DTGProxy binaries"]
async fn fjall_pipeline_point_lookup_c64_completes_after_lower_concurrency_cells() {
    let runtime = DiagnosticRuntime::from_env().unwrap();
    for concurrency in [1, 8, 64] {
        let spec = CellSpec::one(Backend::Fjall, Workload::PointLookup, concurrency, 1);
        let mut cluster = DiagnosticCluster::start(&runtime, spec).await.unwrap();
        cluster.seed_read_dataset(4_096).await.unwrap();
        let observation = tokio::time::timeout(Duration::from_secs(20), cluster.measure_cell(spec))
            .await
            .expect("pipeline point lookup c64 measurement must not stall")
            .unwrap();
        assert_eq!(observation.errors, 0);
        cluster.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "requires release DTGProxy binaries and DTG_GATEWAY_BOLT_READ_PIPELINE=1"]
async fn fjall_bolt_read_pipeline_depth_matrix() {
    assert_eq!(
        env::var("DTG_GATEWAY_BOLT_READ_PIPELINE").as_deref(),
        Ok("1"),
        "the real Bolt pipeline benchmark requires DTG_GATEWAY_BOLT_READ_PIPELINE=1",
    );
    let runtime = DiagnosticRuntime::from_env().unwrap();
    for depth in [1, 8, 64] {
        let spec = CellSpec::one(Backend::Fjall, Workload::PointLookup, 64, 1);
        let mut cluster = DiagnosticCluster::start(&runtime, spec).await.unwrap();
        cluster.seed_read_dataset(4_096).await.unwrap();
        let observation = tokio::time::timeout(
            Duration::from_secs(30),
            cluster.measure_pipeline_cell(spec, depth),
        )
        .await
        .expect("Bolt pipeline depth cell must not stall")
        .unwrap();
        assert_eq!(observation.errors, 0);
        assert!(!observation.latency_samples_ns.is_empty());
        let gateway_metrics = observation
            .gateway_stage_metrics
            .as_ref()
            .expect("pipeline benchmark must capture Gateway metrics");
        for detail in [
            "bolt_read_pipeline_enqueue_wait",
            "bolt_read_pipeline_execution_wait",
            "bolt_read_pipeline_ordered_write_wait",
            "gateway_query_pipeline_credit_wait",
        ] {
            assert!(
                gateway_metrics
                    .delta
                    .details
                    .iter()
                    .any(|candidate| candidate.detail == detail && candidate.success != 0),
                "pipeline benchmark must record {detail}",
            );
        }
        let data_metrics = observation
            .data_stage_metrics
            .as_ref()
            .expect("pipeline benchmark must capture Data metrics");
        println!(
            "DTG_BOLT_PIPELINE_RESULT={}",
            serde_json::json!({
                "depth": depth,
                "concurrency": spec.concurrency,
                "operations": observation.operations,
                "throughput_ops_per_second": observation.operations as f64 * 1_000_000_000.0 / observation.measured_duration_ns as f64,
                "p50_ms": percentile_ns(&observation.latency_samples_ns, 50) as f64 / 1_000_000.0,
                "p95_ms": percentile_ns(&observation.latency_samples_ns, 95) as f64 / 1_000_000.0,
                "p99_ms": percentile_ns(&observation.latency_samples_ns, 99) as f64 / 1_000_000.0,
                "result_digest": observation.result_digest,
                "gateway_details": gateway_metrics.delta.details.iter().filter(|detail| (detail.detail.starts_with("bolt_read_pipeline_") || detail.detail.starts_with("gateway_query_pipeline_")) && detail.success != 0).map(|detail| serde_json::json!({"detail": detail.detail, "calls": detail.success, "mean_nanoseconds": detail.total_nanoseconds / detail.success})).collect::<Vec<_>>(),
                "gateway_stages": gateway_metrics.delta.stages.iter().filter(|stage| stage.success != 0).map(|stage| serde_json::json!({"stage": stage.stage, "calls": stage.success, "mean_nanoseconds": stage.total_nanoseconds / stage.success})).collect::<Vec<_>>(),
                "data_stages": data_metrics.delta.stages.iter().filter(|stage| stage.success != 0).map(|stage| serde_json::json!({"stage": stage.stage, "calls": stage.success, "mean_nanoseconds": stage.total_nanoseconds / stage.success})).collect::<Vec<_>>(),
                "data_details": data_metrics.delta.details.iter().filter(|detail| detail.detail.starts_with("data_gateway_pipeline_") && detail.success != 0).map(|detail| serde_json::json!({"detail": detail.detail, "calls": detail.success, "mean_nanoseconds": detail.total_nanoseconds / detail.success})).collect::<Vec<_>>(),
            })
        );
        cluster.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires release DTGProxy binaries"]
async fn fjall_two_hop_uses_the_real_four_process_bolt_path() {
    let runtime = DiagnosticRuntime::from_env().unwrap();
    let spec = CellSpec::one(Backend::Fjall, Workload::TwoHopExpand, 1, 1);
    let mut cluster = DiagnosticCluster::start(&runtime, spec).await.unwrap();
    cluster.seed_read_dataset(4_096).await.unwrap();
    let mut session = backend_e2e_support::BoltSession::connect(cluster.bolt_address())
        .await
        .unwrap();
    let two_hop = session
        .run(
            "MATCH (a)-[first]->(middle)-[second]->(destination) WHERE a.id = $id RETURN second",
            BTreeMap::from([("id".into(), backend_e2e_support::BoltValue::Integer(2048))]),
        )
        .await
        .unwrap();
    assert_eq!(two_hop.rows.len(), 1);
    let observation = cluster.measure_cell(spec).await.unwrap();
    assert_eq!(observation.errors, 0);
    let provider_execution = observation
        .data_stage_metrics
        .as_ref()
        .unwrap()
        .delta
        .stages
        .iter()
        .find(|stage| stage.stage == "data_provider_execution")
        .unwrap();
    assert_eq!(provider_execution.error, 0);
    println!(
        "DTG_BACKEND_E2E_QUICK_RESULT={}",
        serde_json::to_string(&QuickResult {
            backend: Backend::Fjall,
            workload: Workload::TwoHopExpand,
            concurrency: 1,
            operations: observation.operations,
            warmup_operations: observation.warmup_operations,
            persisted_operations: observation.persisted_operations,
            throughput_ops_per_second: observation.operations as f64 * 1_000_000_000.0
                / observation.measured_duration_ns as f64,
            p50_ms: percentile_ns(&observation.latency_samples_ns, 50) as f64 / 1_000_000.0,
            p95_ms: percentile_ns(&observation.latency_samples_ns, 95) as f64 / 1_000_000.0,
            p99_ms: percentile_ns(&observation.latency_samples_ns, 99) as f64 / 1_000_000.0,
            errors: observation.errors,
            result_digest: observation.result_digest.clone(),
        })
        .unwrap()
    );
    println!(
        "DTG_BACKEND_E2E_STAGE_MEANS={}",
        serde_json::to_string(&stage_means(&observation)).unwrap()
    );
    cluster.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires release DTGProxy binaries"]
async fn fjall_two_hop_builds_a_snapshot_csr_once_then_reuses_it_over_bolt() {
    let runtime = DiagnosticRuntime::from_env().unwrap();
    let spec = CellSpec::one(Backend::Fjall, Workload::TwoHopExpand, 1, 1);
    let mut cluster = DiagnosticCluster::start(&runtime, spec).await.unwrap();
    cluster.seed_read_dataset(4_096).await.unwrap();
    let mut session = backend_e2e_support::BoltSession::connect(cluster.bolt_address())
        .await
        .unwrap();
    let query =
        "MATCH (a)-[first]->(middle)-[second]->(destination) WHERE a.id = $id RETURN second";
    let parameters = BTreeMap::from([("id".into(), backend_e2e_support::BoltValue::Integer(2048))]);

    let before = cluster
        .next_request_metrics_snapshot("data", None)
        .await
        .unwrap();
    let cold = session.run(query, parameters.clone()).await.unwrap();
    assert_eq!(cold.rows.len(), 1);
    let after_cold = cluster
        .next_request_metrics_snapshot("data", Some(before.sequence))
        .await
        .unwrap();
    assert!(
        detail_success(&after_cold, "data_snapshot_csr_cache_miss")
            > detail_success(&before, "data_snapshot_csr_cache_miss")
    );
    assert!(
        detail_success(&after_cold, "data_snapshot_csr_build")
            > detail_success(&before, "data_snapshot_csr_build")
    );

    let warm = session.run(query, parameters).await.unwrap();
    assert_eq!(warm.rows, cold.rows);
    let after_warm = cluster
        .next_request_metrics_snapshot("data", Some(after_cold.sequence))
        .await
        .unwrap();
    assert!(
        detail_success(&after_warm, "data_snapshot_csr_cache_hit")
            > detail_success(&after_cold, "data_snapshot_csr_cache_hit")
    );
    assert_eq!(
        detail_success(&after_warm, "data_snapshot_csr_build"),
        detail_success(&after_cold, "data_snapshot_csr_build")
    );
    cluster.shutdown().await.unwrap();
}

fn detail_success(snapshot: &backend_e2e_support::ProcessMetricsSnapshot, detail: &str) -> u64 {
    snapshot
        .details
        .iter()
        .find(|candidate| candidate.detail == detail)
        .unwrap_or_else(|| panic!("missing request detail {detail}"))
        .success
}

#[test]
fn matrix_has_exact_diagnostic_cells() {
    let cells = backend_e2e_support::CellSpec::matrix(4_923_929_926_749_575_257);
    assert_eq!(cells.len(), 135);
    assert_eq!(
        cells.iter().filter(|cell| cell.concurrency == 8).count(),
        45
    );
    assert_eq!(
        cells.iter().filter(|cell| cell.concurrency == 64).count(),
        45
    );
    assert!(
        cells
            .iter()
            .any(|cell| cell.workload == backend_e2e_support::Workload::TwoHopExpand)
    );
}

#[test]
fn percentiles_use_nearest_rank() {
    let samples = vec![10, 20, 30, 40, 50, 60, 70, 80, 90, 100];
    assert_eq!(backend_e2e_support::percentile_ns(&samples, 50), 50);
    assert_eq!(backend_e2e_support::percentile_ns(&samples, 95), 100);
    assert_eq!(backend_e2e_support::percentile_ns(&samples, 99), 100);
}

#[tokio::test]
async fn bolt_session_reuses_a_single_socket_and_keeps_read_identity_stable() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let accepted_connections = Arc::new(AtomicUsize::new(0));
    let accepted = Arc::clone(&accepted_connections);
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        accepted.fetch_add(1, Ordering::SeqCst);
        serve_fake_bolt_session(&mut socket, 2).await;
    });

    let mut session = backend_e2e_support::BoltSession::connect(address)
        .await
        .unwrap();
    assert!(session.nodelay().unwrap());
    let first: backend_e2e_support::BoltResult = session
        .run(
            "MATCH (n) WHERE n.id = $id RETURN n.id",
            BTreeMap::<String, backend_e2e_support::BoltValue>::new(),
        )
        .await
        .unwrap();
    let second = session
        .run("MATCH (n) WHERE n.id = $id RETURN n.id", BTreeMap::new())
        .await
        .unwrap();

    assert_eq!(accepted_connections.load(Ordering::SeqCst), 1);
    assert_eq!(first.fields, vec!["n.id"]);
    assert_eq!(first.rows.len(), 1);
    assert_eq!(first.result_digest, second.result_digest);
    drop(session);
    server.await.unwrap();
}

#[tokio::test]
async fn bolt_pipeline_session_submits_a_batch_before_reading_ordered_results() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        serve_fake_pipelined_bolt_session(&mut socket, 2).await;
    });

    let mut session = backend_e2e_support::BoltSession::connect(address)
        .await
        .unwrap();
    let results = session
        .run_pipeline(&[
            (
                "MATCH (n) WHERE n.id = $id RETURN n.id".to_owned(),
                BTreeMap::new(),
            ),
            (
                "MATCH (n) WHERE n.id = $id RETURN n.id".to_owned(),
                BTreeMap::new(),
            ),
        ])
        .await
        .unwrap();

    assert_eq!(results.len(), 2);
    assert_eq!(results[0].fields, vec!["n.id"]);
    assert_eq!(results[0].result_digest, results[1].result_digest);
    drop(session);
    server.await.unwrap();
}

#[tokio::test]
async fn measure_cell_records_each_measured_read_on_one_worker_connection() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        serve_fake_bolt_until_closed(&mut socket).await;
    });
    let cell = backend_e2e_support::CellSpec {
        backend: backend_e2e_support::Backend::Fjall,
        workload: backend_e2e_support::Workload::PointLookup,
        concurrency: 1,
        repetition: 0,
    };

    let warmup = Duration::from_millis(10);
    let observation = backend_e2e_support::measure_cell_with_durations(
        address,
        cell,
        warmup,
        Duration::from_millis(10),
    )
    .await
    .unwrap();

    assert!(observation.operations > 0);
    assert_eq!(observation.errors, 0);
    assert_eq!(observation.row_count, 1);
    assert_eq!(observation.result_digest.len(), 64);
    assert_eq!(observation.query_digest.len(), 64);
    let artifact = serde_json::to_value(&observation).unwrap();
    let warmup_finished_at_unix_ns = artifact["warmup_finished_at_unix_ns"].as_u64().unwrap();
    let measurement_started_at_unix_ns =
        artifact["measurement_started_at_unix_ns"].as_u64().unwrap();
    let measurement_finished_at_unix_ns = artifact["measurement_finished_at_unix_ns"]
        .as_u64()
        .unwrap();
    assert_eq!(warmup_finished_at_unix_ns, measurement_started_at_unix_ns);
    assert!(
        measurement_started_at_unix_ns
            > observation
                .started_at_unix_ns
                .saturating_add(warmup.as_nanos() as u64),
        "the measurement boundary must be sampled when warmup actually finishes"
    );
    assert!(measurement_finished_at_unix_ns >= measurement_started_at_unix_ns);
    server.await.unwrap();
}

#[tokio::test]
async fn measure_pipeline_cell_counts_every_depth_two_response() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        serve_fake_bolt_pipeline_until_closed(&mut socket, 2).await;
    });
    let cell = backend_e2e_support::CellSpec {
        backend: backend_e2e_support::Backend::Fjall,
        workload: backend_e2e_support::Workload::PointLookup,
        concurrency: 1,
        repetition: 0,
    };

    let observation = backend_e2e_support::measure_pipeline_cell_with_durations(
        address,
        cell,
        2,
        Duration::ZERO,
        Duration::from_millis(10),
    )
    .await
    .unwrap();

    assert!(observation.operations >= 2);
    assert_eq!(observation.operations % 2, 0);
    assert_eq!(observation.errors, 0);
    server.await.unwrap();
}

#[tokio::test]
async fn measure_cell_returns_an_error_when_a_measurement_bolt_operation_fails() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        serve_fake_bolt_failure_during_measurement(&mut socket).await;
    });
    let cell = backend_e2e_support::CellSpec {
        backend: backend_e2e_support::Backend::Fjall,
        workload: backend_e2e_support::Workload::PointLookup,
        concurrency: 1,
        repetition: 0,
    };

    let error = backend_e2e_support::measure_cell_with_durations(
        address,
        cell,
        Duration::ZERO,
        Duration::from_millis(10),
    )
    .await
    .expect_err("a Bolt failure in the measurement window must reject the cell");

    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(
        error
            .to_string()
            .contains("Neo.ClientError.Statement.SyntaxError")
    );
    server.await.unwrap();
}

#[test]
fn summary_groups_raw_observations_and_serializes_snake_case_enums() {
    let observation = backend_e2e_support::RawObservation {
        backend: backend_e2e_support::Backend::Fjall,
        workload: backend_e2e_support::Workload::PointLookup,
        concurrency: 8,
        repetition: 0,
        started_at_unix_ns: 10,
        finished_at_unix_ns: 20,
        warmup_finished_at_unix_ns: 15,
        measurement_started_at_unix_ns: 15,
        measurement_finished_at_unix_ns: 20,
        measured_duration_ns: 10,
        operations: 3,
        warmup_operations: 0,
        persisted_operations: 0,
        errors: 0,
        latency_samples_ns: vec![10, 20, 30],
        row_count: 1,
        result_digest: "result".into(),
        query_digest: "query".into(),
        transport_mode: "unary".into(),
        gateway_stage_metrics: None,
        data_stage_metrics: None,
    };

    let summary: Vec<backend_e2e_support::Summary> =
        backend_e2e_support::summarize(std::slice::from_ref(&observation));
    assert_eq!(summary.len(), 1);
    assert_eq!(summary[0].p95_ns, 30);
    let artifact = serde_json::to_value(observation).unwrap();
    assert_eq!(artifact["backend"], "fjall");
    assert_eq!(artifact["workload"], "point_lookup");
    assert_eq!(artifact["warmup_operations"], 0);
    assert_eq!(artifact["persisted_operations"], 0);
}

#[test]
fn stage_metrics_window_requires_valid_bracketing_cumulative_snapshots() {
    let log = format!(
        "ordinary stderr\n{}\n{}\n{}\n",
        stage_metrics_line("gateway", 10, 1, 2),
        stage_metrics_line("gateway", 15, 2, 5),
        stage_metrics_line("gateway", 20, 3, 9),
    );

    let window = stage_metrics_window_from_log(&log, "gateway", 15, 20).unwrap();
    assert_eq!(window.before.unix_timestamp_ns, 15);
    assert_eq!(window.after.unix_timestamp_ns, 20);
    assert_eq!(window.before.sequence, 2);
    assert_eq!(window.after.sequence, 3);
    assert_eq!(window.delta.stages[0].success, 4);
    assert_eq!(window.delta.stages[0].buckets[0], 4);
    let delta = serde_json::to_value(&window.delta.stages[0]).unwrap();
    assert!(delta.get("max_nanoseconds").is_none());
}

#[test]
fn stage_metrics_window_accepts_complete_schema_v2_details_and_rejects_incomplete_details() {
    let log = format!(
        "{}\n{}\n",
        stage_metrics_line_with_details("gateway", 10, 1, 1),
        stage_metrics_line_with_details("gateway", 20, 2, 2),
    );
    let window = stage_metrics_window_from_log(&log, "gateway", 15, 20).unwrap();
    assert_eq!(window.delta.details.len(), 19);
    assert_eq!(
        window.delta.details[0].detail,
        "gateway_query_request_encode"
    );
    assert_eq!(window.delta.details[0].success, 1);

    let mut incomplete = serde_json::from_str::<serde_json::Value>(
        stage_metrics_line_with_details("gateway", 10, 1, 1)
            .strip_prefix("DTG_REQUEST_STAGE_METRICS=")
            .unwrap(),
    )
    .unwrap();
    incomplete["details"].as_array_mut().unwrap().pop();
    let error = stage_metrics_window_from_log(
        &format!(
            "DTG_REQUEST_STAGE_METRICS={incomplete}\n{}\n",
            stage_metrics_line_with_details("gateway", 20, 2, 2),
        ),
        "gateway",
        15,
        20,
    )
    .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
}

#[test]
fn stage_metrics_window_accepts_schema_v3_prepare_write_detail() {
    let first = stage_metrics_line_with_prepare_write_details("gateway", 10, 1, 1);
    let second = stage_metrics_line_with_prepare_write_details("gateway", 20, 2, 2);
    let window =
        stage_metrics_window_from_log(&format!("{first}\n{second}\n"), "gateway", 15, 20).unwrap();

    assert_eq!(window.delta.details.len(), 20);
    assert_eq!(
        window.delta.details.last().unwrap().detail,
        "gateway_meta_prepare_write"
    );
    assert_eq!(window.delta.details.last().unwrap().success, 1);
}

#[test]
fn stage_metrics_window_accepts_schema_v4_raft_batch_details() {
    let first = stage_metrics_line_with_raft_queue_details("data", 10, 1, 1);
    let second = stage_metrics_line_with_raft_queue_details("data", 20, 2, 2);
    let window =
        stage_metrics_window_from_log(&format!("{first}\n{second}\n"), "data", 15, 20).unwrap();

    assert_eq!(window.delta.details.len(), 23);
    assert_eq!(window.delta.details[20].detail, "data_raft_batch_admission");
    assert_eq!(window.delta.details[20].success, 1);
    assert_eq!(window.delta.details[21].detail, "data_raft_batch_queue");
    assert_eq!(
        window.delta.details[22].detail,
        "data_raft_blocking_dispatch"
    );
}

#[test]
fn stage_metrics_window_accepts_schema_v5_adjacency_details() {
    let first = stage_metrics_line_with_adjacency_details("data", 10, 1, 1);
    let second = stage_metrics_line_with_adjacency_details("data", 20, 2, 2);
    let window =
        stage_metrics_window_from_log(&format!("{first}\n{second}\n"), "data", 15, 20).unwrap();

    assert_eq!(window.delta.details.len(), 26);
    assert_eq!(window.delta.details[23].detail, "data_adjacency_cache_hit");
    assert_eq!(window.delta.details[24].detail, "data_adjacency_cache_miss");
    assert_eq!(
        window.delta.details[25].detail,
        "data_adjacency_backend_expand"
    );
}

#[test]
fn stage_metrics_window_accepts_schema_v6_snapshot_csr_details() {
    let first = stage_metrics_line_with_snapshot_csr_details("data", 10, 1, 1);
    let second = stage_metrics_line_with_snapshot_csr_details("data", 20, 2, 2);
    let window =
        stage_metrics_window_from_log(&format!("{first}\n{second}\n"), "data", 15, 20).unwrap();

    assert_eq!(window.delta.details.len(), 29);
    assert_eq!(
        window.delta.details[26].detail,
        "data_snapshot_csr_cache_hit"
    );
    assert_eq!(
        window.delta.details[27].detail,
        "data_snapshot_csr_cache_miss"
    );
    assert_eq!(window.delta.details[28].detail, "data_snapshot_csr_build");
}

#[test]
fn stage_metrics_window_accepts_schema_v14_pipeline_boundary_details() {
    let first = stage_metrics_line_with_pipeline_details("gateway", 10, 1, 1);
    let second = stage_metrics_line_with_pipeline_details("gateway", 20, 2, 2);
    let log = format!("{first}\n{second}\n");
    let window = stage_metrics_window_from_log(&log, "gateway", 15, 20).unwrap();
    assert_eq!(window.delta.details.len(), 45);
    assert_eq!(
        window.delta.details[32].detail,
        "gateway_query_pipeline_submit"
    );
    assert_eq!(
        window.delta.details[33].detail,
        "gateway_query_pipeline_response_wait"
    );
    assert_eq!(
        window.delta.details[34].detail,
        "bolt_read_pipeline_enqueue_wait"
    );
    assert_eq!(
        window.delta.details[35].detail,
        "bolt_read_pipeline_execution_wait"
    );
    assert_eq!(
        window.delta.details[36].detail,
        "bolt_read_pipeline_ordered_write_wait"
    );
    assert_eq!(
        window.delta.details[37].detail,
        "gateway_query_pipeline_credit_wait"
    );
    assert_eq!(
        window.delta.details[38].detail,
        "data_gateway_pipeline_dispatch_wait"
    );
    assert_eq!(
        window.delta.details[39].detail,
        "data_gateway_pipeline_completion_send_wait"
    );
    assert_eq!(
        window.delta.details[40].detail,
        "data_gateway_pipeline_completion_frame"
    );
    assert_eq!(
        window.delta.details[41].detail,
        "gateway_query_pipeline_response_transport_wait"
    );
    assert_eq!(
        window.delta.details[42].detail,
        "gateway_query_pipeline_response_dispatch_wait"
    );
    assert_eq!(
        window.delta.details[43].detail,
        "data_gateway_pipeline_request_transport_wait"
    );
    assert_eq!(
        window.delta.details[44].detail,
        "gateway_query_pipeline_writer_wait"
    );
}

#[test]
fn stage_metrics_window_rejects_invalid_or_unbracketed_snapshots() {
    let mut incomplete_snapshot = serde_json::from_str::<serde_json::Value>(
        stage_metrics_line("gateway", 10, 1, 1)
            .strip_prefix("DTG_REQUEST_STAGE_METRICS=")
            .unwrap(),
    )
    .unwrap();
    incomplete_snapshot["stages"].as_array_mut().unwrap().pop();
    let cases = [
        (
            "wrong schema",
            format!(
                "{}\n{}\n",
                stage_metrics_line_with("gateway", 1, 1, 1, 2),
                stage_metrics_line("gateway", 20, 2, 2),
            ),
        ),
        (
            "wrong role",
            format!(
                "{}\n{}\n",
                stage_metrics_line("data", 10, 1, 1),
                stage_metrics_line("data", 20, 2, 2),
            ),
        ),
        (
            "duplicate sequence",
            format!(
                "{}\n{}\n",
                stage_metrics_line("gateway", 10, 1, 1),
                stage_metrics_line("gateway", 20, 1, 2),
            ),
        ),
        (
            "counter regression",
            format!(
                "{}\n{}\n",
                stage_metrics_line("gateway", 10, 1, 3),
                stage_metrics_line("gateway", 20, 2, 2),
            ),
        ),
        (
            "incomplete stage snapshot",
            format!(
                "DTG_REQUEST_STAGE_METRICS={incomplete_snapshot}\n{}\n",
                stage_metrics_line("gateway", 20, 2, 2),
            ),
        ),
        (
            "outside measurement interval",
            format!(
                "{}\n{}\n",
                stage_metrics_line("gateway", 16, 1, 1),
                stage_metrics_line("gateway", 19, 2, 2),
            ),
        ),
    ];

    for (name, log) in cases {
        let error = stage_metrics_window_from_log(&log, "gateway", 15, 20).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{name}");
    }
}

#[test]
fn quick_artifact_requires_three_complete_repetitions_and_refuses_overwrite() {
    let observations = complete_quick_observations(backend_e2e_support::Backend::Fjall, 3);
    let artifact =
        backend_e2e_support::QuickDiagnosticArtifact::new("test-revision", observations).unwrap();
    assert_eq!(artifact.repetitions, 3);
    assert_eq!(artifact.observations.len(), 45);
    assert_eq!(artifact.summaries.len(), 15);
    let serialized = serde_json::to_value(&artifact).unwrap();
    assert_eq!(serialized["format_version"], 1);
    assert_eq!(serialized["backend"], "fjall");
    assert_eq!(serialized["revision"], "test-revision");
    assert_eq!(serialized["transport_mode"], "unary");
    assert_eq!(serialized["repetitions"], 3);
    assert_eq!(serialized["observations"].as_array().unwrap().len(), 45);
    assert_eq!(serialized["summaries"].as_array().unwrap().len(), 15);
    let summary = serialized["summaries"]
        .as_array()
        .unwrap()
        .iter()
        .find(|summary| summary["workload"] == "point_lookup" && summary["concurrency"] == 8)
        .unwrap();
    assert_eq!(summary["latency_samples"], 6);
    assert_eq!(summary["operations"], 6);
    assert_eq!(summary["measured_duration_ns"], 15);
    assert_eq!(summary["throughput_ops_per_second"], 400_000_000.0);
    assert_eq!(summary["p50_ns"], 10);
    assert_eq!(summary["p95_ns"], 20);
    assert_eq!(summary["p99_ns"], 20);
    assert_eq!(
        summary["gateway_stage_means"][0]["stage"],
        "gateway_plan_routing"
    );
    assert_eq!(summary["data_stage_means"][0]["stage"], "data_validation");
    let directory = tempfile::tempdir().unwrap();
    let output = directory.path().join("quick.json");
    backend_e2e_support::write_quick_artifact(&output, &artifact).unwrap();
    assert!(backend_e2e_support::write_quick_artifact(&output, &artifact).is_err());
}

#[test]
fn quick_artifact_rejects_incomplete_matrix_and_changed_read_identity() {
    let mut incomplete = complete_quick_observations(backend_e2e_support::Backend::Fjall, 3);
    incomplete.pop();
    let error =
        backend_e2e_support::QuickDiagnosticArtifact::new("revision", incomplete).unwrap_err();
    assert!(error.to_string().contains("three complete repetitions"));

    let mut changed = complete_quick_observations(backend_e2e_support::Backend::Fjall, 3);
    let observation = changed
        .iter_mut()
        .find(|observation| {
            observation.repetition == 2
                && observation.workload == backend_e2e_support::Workload::PointLookup
                && observation.concurrency == 1
        })
        .unwrap();
    observation.result_digest = "changed-result".into();
    let error = backend_e2e_support::QuickDiagnosticArtifact::new("revision", changed).unwrap_err();
    assert!(error.to_string().contains("read identity changed"));

    let mut errors = complete_quick_observations(backend_e2e_support::Backend::Fjall, 3);
    errors[0].errors = 1;
    let error = backend_e2e_support::QuickDiagnosticArtifact::new("revision", errors).unwrap_err();
    assert!(error.to_string().contains("contains errors"));

    let mut missing_metrics = complete_quick_observations(backend_e2e_support::Backend::Fjall, 3);
    missing_metrics[0].gateway_stage_metrics = None;
    let error =
        backend_e2e_support::QuickDiagnosticArtifact::new("revision", missing_metrics).unwrap_err();
    assert!(error.to_string().contains("lacks bracketing stage metrics"));

    let mut missing_csr = complete_quick_observations(backend_e2e_support::Backend::Fjall, 3);
    let observation = missing_csr
        .iter_mut()
        .find(|observation| {
            observation.workload == backend_e2e_support::Workload::TwoHopExpand
                && observation.concurrency == 1
                && observation.repetition == 0
        })
        .unwrap();
    observation
        .data_stage_metrics
        .as_mut()
        .unwrap()
        .delta
        .details
        .iter_mut()
        .find(|detail| detail.detail == "data_snapshot_csr_cache_hit")
        .unwrap()
        .success = 0;
    let error =
        backend_e2e_support::QuickDiagnosticArtifact::new("revision", missing_csr).unwrap_err();
    assert!(error.to_string().contains("lacks snapshot CSR cache hits"));
}

#[test]
fn quick_artifact_rejects_mixed_transport_modes() {
    let mut observations = complete_quick_observations(backend_e2e_support::Backend::Fjall, 3);
    observations[0].transport_mode = "pipeline".into();
    let error =
        backend_e2e_support::QuickDiagnosticArtifact::new("revision", observations).unwrap_err();
    assert!(error.to_string().contains("one transport mode"));
}

#[test]
fn quick_artifact_rejects_writes_without_one_persisted_vertex_per_create() {
    let mut observations = complete_quick_observations(backend_e2e_support::Backend::Fjall, 3);
    let write = observations
        .iter_mut()
        .find(|observation| {
            observation.workload == backend_e2e_support::Workload::CreateVertex
                && observation.concurrency == 1
                && observation.repetition == 0
        })
        .unwrap();
    write.warmup_operations = 3;
    write.persisted_operations = 4;

    let error =
        backend_e2e_support::QuickDiagnosticArtifact::new("revision", observations).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("does not prove every accepted CREATE")
    );
}

#[test]
fn quick_artifact_requires_required_stage_set() {
    let mut observations = complete_quick_observations(backend_e2e_support::Backend::Fjall, 3);
    let point_read = observations
        .iter_mut()
        .find(|observation| {
            observation.workload == backend_e2e_support::Workload::PointLookup
                && observation.concurrency == 1
                && observation.repetition == 0
        })
        .unwrap();
    point_read
        .data_stage_metrics
        .as_mut()
        .unwrap()
        .delta
        .details
        .retain(|detail| detail.detail != "data_provider_apply");

    assert!(
        backend_e2e_support::QuickDiagnosticArtifact::new("revision", observations)
            .unwrap_err()
            .to_string()
            .contains("data_provider_apply")
    );
}

fn complete_quick_observations(
    backend: backend_e2e_support::Backend,
    repetitions: u8,
) -> Vec<backend_e2e_support::RawObservation> {
    let mut observations = Vec::new();
    for repetition in 0..repetitions {
        for workload in [
            backend_e2e_support::Workload::CreateVertex,
            backend_e2e_support::Workload::PointLookup,
            backend_e2e_support::Workload::OneHopExpand,
            backend_e2e_support::Workload::TwoHopExpand,
            backend_e2e_support::Workload::CountVertices,
        ] {
            for concurrency in [1, 8, 64] {
                let mut gateway_stage_metrics = synthetic_stage_metrics_window_v6("gateway");
                for detail in ["gateway_plan_routing", "gateway_transport_wait"] {
                    add_required_stage_detail(&mut gateway_stage_metrics, detail);
                }
                let mut data_stage_metrics = synthetic_stage_metrics_window_v6("data");
                for detail in [
                    "data_validation",
                    "data_execution",
                    "data_raft_queue",
                    "data_raft_apply",
                    "data_provider_apply",
                ] {
                    add_required_stage_detail(&mut data_stage_metrics, detail);
                }
                if matches!(
                    workload,
                    backend_e2e_support::Workload::OneHopExpand
                        | backend_e2e_support::Workload::TwoHopExpand
                ) {
                    data_stage_metrics
                        .delta
                        .details
                        .iter_mut()
                        .find(|detail| detail.detail == "data_adjacency_backend_expand")
                        .unwrap()
                        .success = 0;
                }
                observations.push(backend_e2e_support::RawObservation {
                    backend,
                    workload,
                    concurrency,
                    repetition,
                    started_at_unix_ns: 10,
                    finished_at_unix_ns: 20,
                    warmup_finished_at_unix_ns: 15,
                    measurement_started_at_unix_ns: 15,
                    measurement_finished_at_unix_ns: 20,
                    measured_duration_ns: 5,
                    operations: 2,
                    warmup_operations: 0,
                    persisted_operations: if workload.is_write() { 2 } else { 0 },
                    errors: 0,
                    latency_samples_ns: vec![10, 20],
                    row_count: u64::from(!workload.is_write()),
                    result_digest: if workload.is_write() {
                        String::new()
                    } else {
                        "stable-result".into()
                    },
                    query_digest: format!("{workload:?}"),
                    transport_mode: "unary".into(),
                    gateway_stage_metrics: Some(gateway_stage_metrics),
                    data_stage_metrics: Some(data_stage_metrics),
                });
            }
        }
    }
    observations
}

fn add_required_stage_detail(window: &mut backend_e2e_support::StageMetricsWindow, name: &str) {
    let mut detail = window.delta.details[0].clone();
    detail.detail = name.into();
    detail.success = 2;
    detail.error = 0;
    detail.cancelled = 0;
    detail.total_nanoseconds = 2;
    window.delta.details.push(detail);
}

fn stage_metrics_line(role: &str, timestamp: u64, sequence: u64, success: u64) -> String {
    stage_metrics_line_with(role, timestamp, sequence, success, 1)
}

fn synthetic_stage_metrics_window_v6(role: &str) -> backend_e2e_support::StageMetricsWindow {
    let log = format!(
        "{}\n{}\n",
        stage_metrics_line_with_snapshot_csr_details(role, 10, 1, 1),
        stage_metrics_line_with_snapshot_csr_details(role, 20, 2, 2),
    );
    stage_metrics_window_from_log(&log, role, 15, 20).unwrap()
}

fn stage_metrics_line_with(
    role: &str,
    timestamp: u64,
    sequence: u64,
    success: u64,
    schema_version: u64,
) -> String {
    const STAGES: [&str; 10] = [
        "bolt_decode",
        "gateway_compile",
        "gateway_plan",
        "gateway_internal_rpc",
        "gateway_local_execution",
        "bolt_encode",
        "data_validation",
        "data_routing",
        "data_raft_apply",
        "data_provider_execution",
    ];
    let stages = STAGES
        .into_iter()
        .map(|stage| {
            serde_json::json!({
                "stage": stage,
                "buckets": vec![success; 64],
                "success": success,
                "error": success,
                "cancelled": success,
                "total_nanoseconds": success,
                "max_nanoseconds": success,
            })
        })
        .collect::<Vec<_>>();
    format!(
        "DTG_REQUEST_STAGE_METRICS={}",
        serde_json::json!({
            "schema_version": schema_version,
            "process_role": role,
            "unix_timestamp_ns": timestamp,
            "sequence": sequence,
            "stages": stages,
        })
    )
}

fn stage_metrics_line_with_details(
    role: &str,
    timestamp: u64,
    sequence: u64,
    success: u64,
) -> String {
    const DETAILS: [&str; 19] = [
        "gateway_query_request_encode",
        "gateway_query_response_collect",
        "gateway_query_response_decode",
        "gateway_query_local_materialize",
        "gateway_meta_allocate_start",
        "gateway_meta_reserve_commit",
        "gateway_data_apply_rpc",
        "gateway_meta_resolve_commit",
        "data_route_lock_wait",
        "data_route_lookup",
        "data_raft_propose",
        "data_raft_drive_ready",
        "data_read_view_cache_hit",
        "data_read_view_cache_miss",
        "data_read_view_open",
        "data_temporal_point_evaluation",
        "data_temporal_scan_id_collection",
        "data_temporal_scan_visibility",
        "data_raft_lock_wait",
    ];
    let mut value = serde_json::from_str::<serde_json::Value>(
        stage_metrics_line_with(role, timestamp, sequence, success, 2)
            .strip_prefix("DTG_REQUEST_STAGE_METRICS=")
            .unwrap(),
    )
    .unwrap();
    value["details"] = serde_json::Value::Array(
        DETAILS
            .into_iter()
            .map(|detail| {
                serde_json::json!({
                    "detail": detail,
                    "buckets": vec![success; 64],
                    "success": success,
                    "error": success,
                    "cancelled": success,
                    "total_nanoseconds": success,
                    "max_nanoseconds": success,
                })
            })
            .collect(),
    );
    format!("DTG_REQUEST_STAGE_METRICS={value}")
}

fn stage_metrics_line_with_prepare_write_details(
    role: &str,
    timestamp: u64,
    sequence: u64,
    success: u64,
) -> String {
    let mut value = serde_json::from_str::<serde_json::Value>(
        stage_metrics_line_with_details(role, timestamp, sequence, success)
            .strip_prefix("DTG_REQUEST_STAGE_METRICS=")
            .unwrap(),
    )
    .unwrap();
    value["schema_version"] = serde_json::Value::from(3);
    value["details"]
        .as_array_mut()
        .unwrap()
        .push(serde_json::json!({
            "detail": "gateway_meta_prepare_write",
            "buckets": vec![success; 64],
            "success": success,
            "error": success,
            "cancelled": success,
            "total_nanoseconds": success,
            "max_nanoseconds": success,
        }));
    format!("DTG_REQUEST_STAGE_METRICS={value}")
}

fn stage_metrics_line_with_raft_queue_details(
    role: &str,
    timestamp: u64,
    sequence: u64,
    success: u64,
) -> String {
    let mut value = serde_json::from_str::<serde_json::Value>(
        stage_metrics_line_with_prepare_write_details(role, timestamp, sequence, success)
            .strip_prefix("DTG_REQUEST_STAGE_METRICS=")
            .unwrap(),
    )
    .unwrap();
    value["schema_version"] = serde_json::Value::from(4);
    for detail in [
        "data_raft_batch_admission",
        "data_raft_batch_queue",
        "data_raft_blocking_dispatch",
    ] {
        value["details"]
            .as_array_mut()
            .unwrap()
            .push(serde_json::json!({
                "detail": detail,
                "buckets": vec![success; 64],
                "success": success,
                "error": success,
                "cancelled": success,
                "total_nanoseconds": success,
                "max_nanoseconds": success,
            }));
    }
    format!("DTG_REQUEST_STAGE_METRICS={value}")
}

fn stage_metrics_line_with_adjacency_details(
    role: &str,
    timestamp: u64,
    sequence: u64,
    success: u64,
) -> String {
    let mut value = serde_json::from_str::<serde_json::Value>(
        stage_metrics_line_with_raft_queue_details(role, timestamp, sequence, success)
            .strip_prefix("DTG_REQUEST_STAGE_METRICS=")
            .unwrap(),
    )
    .unwrap();
    value["schema_version"] = serde_json::Value::from(5);
    for detail in [
        "data_adjacency_cache_hit",
        "data_adjacency_cache_miss",
        "data_adjacency_backend_expand",
    ] {
        value["details"]
            .as_array_mut()
            .unwrap()
            .push(serde_json::json!({
                "detail": detail,
                "buckets": vec![success; 64],
                "success": success,
                "error": success,
                "cancelled": success,
                "total_nanoseconds": success,
                "max_nanoseconds": success,
            }));
    }
    format!("DTG_REQUEST_STAGE_METRICS={value}")
}

fn stage_metrics_line_with_snapshot_csr_details(
    role: &str,
    timestamp: u64,
    sequence: u64,
    success: u64,
) -> String {
    let mut value = serde_json::from_str::<serde_json::Value>(
        stage_metrics_line_with_adjacency_details(role, timestamp, sequence, success)
            .strip_prefix("DTG_REQUEST_STAGE_METRICS=")
            .unwrap(),
    )
    .unwrap();
    value["schema_version"] = serde_json::Value::from(6);
    for detail in [
        "data_snapshot_csr_cache_hit",
        "data_snapshot_csr_cache_miss",
        "data_snapshot_csr_build",
    ] {
        value["details"]
            .as_array_mut()
            .unwrap()
            .push(serde_json::json!({
                "detail": detail,
                "buckets": vec![success; 64],
                "success": success,
                "error": success,
                "cancelled": success,
                "total_nanoseconds": success,
                "max_nanoseconds": success,
            }));
    }
    format!("DTG_REQUEST_STAGE_METRICS={value}")
}

fn stage_metrics_line_with_pipeline_details(
    role: &str,
    timestamp: u64,
    sequence: u64,
    success: u64,
) -> String {
    let mut value = serde_json::from_str::<serde_json::Value>(
        stage_metrics_line_with_snapshot_csr_details(role, timestamp, sequence, success)
            .strip_prefix("DTG_REQUEST_STAGE_METRICS=")
            .unwrap(),
    )
    .unwrap();
    value["schema_version"] = serde_json::Value::from(14);
    for detail in [
        "gateway_query_session_submit",
        "gateway_query_session_response_wait",
        "data_gateway_session_execution",
        "gateway_query_pipeline_submit",
        "gateway_query_pipeline_response_wait",
        "bolt_read_pipeline_enqueue_wait",
        "bolt_read_pipeline_execution_wait",
        "bolt_read_pipeline_ordered_write_wait",
        "gateway_query_pipeline_credit_wait",
        "data_gateway_pipeline_dispatch_wait",
        "data_gateway_pipeline_completion_send_wait",
        "data_gateway_pipeline_completion_frame",
        "gateway_query_pipeline_response_transport_wait",
        "gateway_query_pipeline_response_dispatch_wait",
        "data_gateway_pipeline_request_transport_wait",
        "gateway_query_pipeline_writer_wait",
    ] {
        value["details"]
            .as_array_mut()
            .unwrap()
            .push(serde_json::json!({
                "detail": detail,
                "buckets": vec![success; 64],
                "success": success,
                "error": success,
                "cancelled": success,
                "total_nanoseconds": success,
                "max_nanoseconds": success,
            }));
    }
    format!("DTG_REQUEST_STAGE_METRICS={value}")
}

async fn serve_fake_bolt_session(socket: &mut TcpStream, exchanges: usize) {
    let mut handshake = [0_u8; 20];
    socket.read_exact(&mut handshake).await.unwrap();
    assert_eq!(&handshake[..4], &[0x60, 0x60, 0xb0, 0x17]);
    socket.write_all(&[0, 0, 4, 5]).await.unwrap();

    let hello = read_bolt_message(socket).await;
    assert_eq!(hello[1], 0x01);
    write_bolt_message(socket, &[0xb1, 0x70, 0xa0]).await;

    for _ in 0..exchanges {
        let run = read_bolt_message(socket).await;
        assert_eq!(run[1], 0x10);
        write_bolt_message(
            socket,
            &[
                0xb1, 0x70, 0xa1, 0x86, b'f', b'i', b'e', b'l', b'd', b's', 0x91, 0x84, b'n', b'.',
                b'i', b'd',
            ],
        )
        .await;

        let pull = read_bolt_message(socket).await;
        assert_eq!(pull, vec![0xb1, 0x3f, 0xa0]);
        write_bolt_message(socket, &[0xb1, 0x71, 0x91, 0xc9, 0x08, 0x00]).await;
        write_bolt_message(
            socket,
            &[
                0xb1, 0x70, 0xa1, 0x88, b'h', b'a', b's', b'_', b'm', b'o', b'r', b'e', 0xc2,
            ],
        )
        .await;
    }
}

async fn serve_fake_pipelined_bolt_session(socket: &mut TcpStream, exchanges: usize) {
    let mut handshake = [0_u8; 20];
    socket.read_exact(&mut handshake).await.unwrap();
    assert_eq!(&handshake[..4], &[0x60, 0x60, 0xb0, 0x17]);
    socket.write_all(&[0, 0, 4, 5]).await.unwrap();

    let hello = read_bolt_message(socket).await;
    assert_eq!(hello[1], 0x01);
    write_bolt_message(socket, &[0xb1, 0x70, 0xa0]).await;

    for _ in 0..exchanges {
        let run = read_bolt_message(socket).await;
        assert_eq!(run[1], 0x10);
        assert_eq!(read_bolt_message(socket).await, vec![0xb1, 0x3f, 0xa0]);
    }
    for _ in 0..exchanges {
        write_bolt_message(
            socket,
            &[
                0xb1, 0x70, 0xa1, 0x86, b'f', b'i', b'e', b'l', b'd', b's', 0x91, 0x84, b'n', b'.',
                b'i', b'd',
            ],
        )
        .await;
        write_bolt_message(socket, &[0xb1, 0x71, 0x91, 0xc9, 0x08, 0x00]).await;
        write_bolt_message(
            socket,
            &[
                0xb1, 0x70, 0xa1, 0x88, b'h', b'a', b's', b'_', b'm', b'o', b'r', b'e', 0xc2,
            ],
        )
        .await;
    }
}

async fn serve_fake_bolt_until_closed(socket: &mut TcpStream) {
    let mut handshake = [0_u8; 20];
    socket.read_exact(&mut handshake).await.unwrap();
    socket.write_all(&[0, 0, 4, 5]).await.unwrap();
    if try_read_bolt_message(socket).await.is_err() {
        return;
    }
    write_bolt_message(socket, &[0xb1, 0x70, 0xa0]).await;
    loop {
        let Ok(run) = try_read_bolt_message(socket).await else {
            return;
        };
        assert_eq!(run[1], 0x10);
        write_bolt_message(
            socket,
            &[
                0xb1, 0x70, 0xa1, 0x86, b'f', b'i', b'e', b'l', b'd', b's', 0x91, 0x84, b'n', b'.',
                b'i', b'd',
            ],
        )
        .await;
        let Ok(pull) = try_read_bolt_message(socket).await else {
            return;
        };
        assert_eq!(pull, vec![0xb1, 0x3f, 0xa0]);
        write_bolt_message(socket, &[0xb1, 0x71, 0x91, 0xc9, 0x08, 0x00]).await;
        write_bolt_message(
            socket,
            &[
                0xb1, 0x70, 0xa1, 0x88, b'h', b'a', b's', b'_', b'm', b'o', b'r', b'e', 0xc2,
            ],
        )
        .await;
    }
}

async fn serve_fake_bolt_pipeline_until_closed(socket: &mut TcpStream, depth: usize) {
    let mut handshake = [0_u8; 20];
    socket.read_exact(&mut handshake).await.unwrap();
    socket.write_all(&[0, 0, 4, 5]).await.unwrap();
    if try_read_bolt_message(socket).await.is_err() {
        return;
    }
    write_bolt_message(socket, &[0xb1, 0x70, 0xa0]).await;
    loop {
        for _ in 0..depth {
            let Ok(run) = try_read_bolt_message(socket).await else {
                return;
            };
            assert_eq!(run[1], 0x10);
            let Ok(pull) = try_read_bolt_message(socket).await else {
                return;
            };
            assert_eq!(pull, vec![0xb1, 0x3f, 0xa0]);
        }
        for _ in 0..depth {
            write_bolt_message(
                socket,
                &[
                    0xb1, 0x70, 0xa1, 0x86, b'f', b'i', b'e', b'l', b'd', b's', 0x91, 0x84, b'n',
                    b'.', b'i', b'd',
                ],
            )
            .await;
            write_bolt_message(socket, &[0xb1, 0x71, 0x91, 0xc9, 0x08, 0x00]).await;
            write_bolt_message(
                socket,
                &[
                    0xb1, 0x70, 0xa1, 0x88, b'h', b'a', b's', b'_', b'm', b'o', b'r', b'e', 0xc2,
                ],
            )
            .await;
        }
    }
}

async fn serve_fake_bolt_failure_during_measurement(socket: &mut TcpStream) {
    let mut handshake = [0_u8; 20];
    socket.read_exact(&mut handshake).await.unwrap();
    socket.write_all(&[0, 0, 4, 5]).await.unwrap();
    let hello = read_bolt_message(socket).await;
    assert_eq!(hello[1], 0x01);
    write_bolt_message(socket, &[0xb1, 0x70, 0xa0]).await;

    let run = read_bolt_message(socket).await;
    assert_eq!(run[1], 0x10);
    write_bolt_message(
        socket,
        &[
            0xb1, 0x7f, 0xa2, 0x84, b'c', b'o', b'd', b'e', 0xd0, 0x25, b'N', b'e', b'o', b'.',
            b'C', b'l', b'i', b'e', b'n', b't', b'E', b'r', b'r', b'o', b'r', b'.', b'S', b't',
            b'a', b't', b'e', b'm', b'e', b'n', b't', b'.', b'S', b'y', b'n', b't', b'a', b'x',
            b'E', b'r', b'r', b'o', b'r', 0x87, b'm', b'e', b's', b's', b'a', b'g', b'e', 0x84,
            b'b', b'o', b'o', b'm',
        ],
    )
    .await;
}

async fn read_bolt_message(socket: &mut TcpStream) -> Vec<u8> {
    try_read_bolt_message(socket).await.unwrap()
}

async fn try_read_bolt_message(socket: &mut TcpStream) -> io::Result<Vec<u8>> {
    let mut message = Vec::new();
    loop {
        let mut size = [0_u8; 2];
        socket.read_exact(&mut size).await?;
        let size = usize::from(u16::from_be_bytes(size));
        if size == 0 {
            return Ok(message);
        }
        let start = message.len();
        message.resize(start + size, 0);
        socket.read_exact(&mut message[start..]).await?;
    }
}

async fn write_bolt_message(socket: &mut TcpStream, message: &[u8]) {
    socket
        .write_all(&(message.len() as u16).to_be_bytes())
        .await
        .unwrap();
    socket.write_all(message).await.unwrap();
    socket.write_all(&[0, 0]).await.unwrap();
}
