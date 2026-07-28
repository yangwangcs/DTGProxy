use std::collections::BTreeMap;
use std::process::Command;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bolt_protocol::Value;
use bolt_server::{
    BoltConnectionConfig, BoltService, CursorId, ExternalLoadConfig, ExternalTtfrProbeError,
    PullOutcome, RunOutcome, RunRequest, ServiceError, ServiceFuture, TransactionId,
    run_external_load, serve_connection,
};
use tokio::net::TcpListener;

#[test]
fn loadgen_cli_exposes_the_formal_measurement_interface() {
    let output = Command::new(env!("CARGO_BIN_EXE_dtgproxy-bolt-loadgen"))
        .arg("--help")
        .output()
        .expect("run loadgen CLI");
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).expect("UTF-8 help");
    for option in [
        "--address",
        "--query",
        "--connections",
        "--warmup-seconds",
        "--duration-seconds",
        "--timeout-ms",
        "--benchmark-session",
        "--output",
    ] {
        assert!(stdout.contains(option), "help is missing {option}");
    }
}

#[tokio::test]
async fn persistent_load_reuses_connections_and_excludes_warmup_operations() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
    let address = listener.local_addr().expect("address");
    let service = Arc::new(CountingService::new(false));
    let accepted = Arc::new(AtomicUsize::new(0));
    let server = tokio::spawn(serve_connections(
        listener,
        Arc::clone(&service),
        Arc::clone(&accepted),
        2,
    ));

    let config = ExternalLoadConfig::new(
        address,
        "RETURN 1 AS value",
        BTreeMap::new(),
        2,
        Duration::from_millis(30),
        Duration::from_millis(80),
        Duration::from_secs(1),
    )
    .expect("load config");
    let report = run_external_load(config).await.expect("load report");
    server.await.expect("server task");

    assert_eq!(accepted.load(Ordering::Acquire), 2);
    assert_eq!(report.connection_setup_samples_ns().len(), 2);
    assert_eq!(
        report.ttfr_samples_ns().len(),
        report.completed_operations()
    );
    assert_eq!(
        report.total_latency_samples_ns().len(),
        report.completed_operations()
    );
    assert!(report.completed_operations() > 2);
    assert_eq!(
        report.total_operations(),
        service.run_count.load(Ordering::Acquire)
    );
    assert!(report.total_operations() > report.completed_operations());
    assert!(
        service
            .run_extras
            .lock()
            .expect("RUN extras lock")
            .iter()
            .all(BTreeMap::is_empty),
        "the no-session path must preserve the empty RUN extra map"
    );
    assert_eq!(report.row_count(), 1);
    assert!(report.throughput_ops_per_second() > 0.0);
    let report_json = serde_json::to_value(&report).expect("serialize report");
    let warmup_started = report_json["warmup_started_unix_ns"].as_u64().unwrap();
    let measurement_started = report_json["measurement_started_unix_ns"].as_u64().unwrap();
    let measurement_ended = report_json["measurement_ended_unix_ns"].as_u64().unwrap();
    assert_eq!(measurement_started - warmup_started, 30_000_000);
    assert_eq!(measurement_ended - measurement_started, 80_000_000);
    assert_eq!(
        report_json["total_operations"].as_u64(),
        Some(report.total_operations() as u64)
    );
}

#[tokio::test]
async fn benchmark_session_is_attached_to_every_warmup_and_measurement_run() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
    let address = listener.local_addr().expect("address");
    let service = Arc::new(CountingService::new(false));
    let server = tokio::spawn(serve_connections(
        listener,
        Arc::clone(&service),
        Arc::new(AtomicUsize::new(0)),
        1,
    ));
    let config = ExternalLoadConfig::new(
        address,
        "RETURN 1 AS value",
        BTreeMap::new(),
        1,
        Duration::from_millis(20),
        Duration::from_millis(30),
        Duration::from_secs(1),
    )
    .expect("load config")
    .with_benchmark_session("paper.Session_1")
    .expect("benchmark session");

    let report = run_external_load(config).await.expect("load report");
    server.await.expect("server task");

    let extras = service.run_extras.lock().expect("RUN extras lock");
    assert_eq!(extras.len(), report.total_operations());
    assert!(report.total_operations() > report.completed_operations());
    assert!(extras.iter().all(|extra| {
        extra
            == &BTreeMap::from([(
                "dtgproxy.paper.session".to_owned(),
                Value::String("paper.Session_1".to_owned()),
            )])
    }));
}

#[tokio::test]
async fn persistent_load_rejects_result_identity_drift() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
    let address = listener.local_addr().expect("address");
    let service = Arc::new(CountingService::new(true));
    let server = tokio::spawn(serve_connections(
        listener,
        Arc::clone(&service),
        Arc::new(AtomicUsize::new(0)),
        1,
    ));
    let config = ExternalLoadConfig::new(
        address,
        "RETURN 1 AS value",
        BTreeMap::new(),
        1,
        Duration::ZERO,
        Duration::from_millis(30),
        Duration::from_secs(1),
    )
    .expect("load config");

    let error = run_external_load(config)
        .await
        .expect_err("identity drift must fail the load");
    server.await.expect("server task");
    assert_eq!(error, ExternalTtfrProbeError::ResultMismatch);
}

#[tokio::test]
async fn worker_failure_cancels_peers_waiting_at_the_measurement_barrier() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
    let address = listener.local_addr().expect("address");
    let service = Arc::new(CountingService::new(false));
    let server = tokio::spawn(async move {
        let (mut healthy, _) = listener.accept().await.expect("healthy connection");
        let healthy_task = tokio::spawn(async move {
            serve_connection(&mut healthy, service, BoltConnectionConfig::default()).await
        });
        let (failed, _) = listener.accept().await.expect("failed connection");
        drop(failed);
        let _ = healthy_task.await;
    });
    let config = ExternalLoadConfig::new(
        address,
        "RETURN 1 AS value",
        BTreeMap::new(),
        2,
        Duration::from_millis(20),
        Duration::from_millis(30),
        Duration::from_millis(100),
    )
    .expect("load config");

    let result = tokio::time::timeout(Duration::from_millis(500), run_external_load(config)).await;

    assert!(
        result.is_ok(),
        "one failed worker must cancel barrier peers"
    );
    assert!(
        result.unwrap().is_err(),
        "worker failure must invalidate the run"
    );
    server.await.expect("server task");
}

async fn serve_connections(
    listener: TcpListener,
    service: Arc<CountingService>,
    accepted: Arc<AtomicUsize>,
    connection_count: usize,
) {
    let mut tasks = Vec::with_capacity(connection_count);
    for _ in 0..connection_count {
        let (mut stream, _) = listener.accept().await.expect("accept");
        accepted.fetch_add(1, Ordering::AcqRel);
        let service = Arc::clone(&service);
        tasks.push(tokio::spawn(async move {
            serve_connection(&mut stream, service, BoltConnectionConfig::default()).await
        }));
    }
    for task in tasks {
        task.await.expect("connection task").expect("connection");
    }
}

struct CountingService {
    drift: bool,
    run_count: AtomicUsize,
    pull_count: AtomicUsize,
    run_extras: Mutex<Vec<BTreeMap<String, Value>>>,
}

impl CountingService {
    const fn new(drift: bool) -> Self {
        Self {
            drift,
            run_count: AtomicUsize::new(0),
            pull_count: AtomicUsize::new(0),
            run_extras: Mutex::new(Vec::new()),
        }
    }
}

impl BoltService for CountingService {
    fn hello<'a>(
        &'a self,
        _metadata: BTreeMap<String, Value>,
    ) -> ServiceFuture<'a, BTreeMap<String, Value>> {
        Box::pin(async { Ok(BTreeMap::new()) })
    }

    fn run<'a>(&'a self, request: RunRequest) -> ServiceFuture<'a, RunOutcome> {
        Box::pin(async move {
            self.run_count.fetch_add(1, Ordering::AcqRel);
            self.run_extras
                .lock()
                .expect("RUN extras lock")
                .push(request.extra().clone());
            tokio::time::sleep(Duration::from_millis(2)).await;
            Ok(RunOutcome::new(CursorId::new(1), vec!["value".into()]))
        })
    }

    fn pull<'a>(&'a self, _cursor: CursorId, _n: i64) -> ServiceFuture<'a, PullOutcome> {
        Box::pin(async move {
            tokio::time::sleep(Duration::from_millis(2)).await;
            let ordinal = self.pull_count.fetch_add(1, Ordering::AcqRel);
            let value = if self.drift && ordinal % 2 == 1 { 2 } else { 1 };
            Ok(PullOutcome::new(
                vec![vec![Value::Integer(value)]],
                false,
                BTreeMap::new(),
            ))
        })
    }

    fn discard<'a>(&'a self, _cursor: CursorId, _n: i64) -> ServiceFuture<'a, bool> {
        Box::pin(async { Ok(false) })
    }

    fn begin<'a>(&'a self, _extra: BTreeMap<String, Value>) -> ServiceFuture<'a, TransactionId> {
        Box::pin(async {
            Err(ServiceError::new(
                "Neo.ClientError.Request.Invalid",
                "unused",
            ))
        })
    }

    fn commit<'a>(&'a self, _transaction: TransactionId) -> ServiceFuture<'a, String> {
        Box::pin(async {
            Err(ServiceError::new(
                "Neo.ClientError.Request.Invalid",
                "unused",
            ))
        })
    }

    fn rollback<'a>(&'a self, _transaction: TransactionId) -> ServiceFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }

    fn route<'a>(
        &'a self,
        _routing: BTreeMap<String, Value>,
        _bookmarks: Vec<Value>,
        _database: Option<String>,
    ) -> ServiceFuture<'a, BTreeMap<String, Value>> {
        Box::pin(async {
            Err(ServiceError::new(
                "Neo.ClientError.Request.Invalid",
                "unused",
            ))
        })
    }
}
