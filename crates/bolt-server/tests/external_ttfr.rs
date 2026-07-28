use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bolt_protocol::Value;
use bolt_server::{
    BoltConnectionConfig, BoltProbeSession, BoltService, CursorId, ExternalTtfrProbeConfig,
    ExternalTtfrProbeError, PullOutcome, RunOutcome, RunRequest, ServiceError, ServiceFuture,
    TransactionId, probe_external_ttfr, serve_connection,
};
use tokio::net::TcpListener;

const CONNECTION_DELAY: Duration = Duration::from_millis(40);
const HELLO_DELAY: Duration = Duration::from_millis(40);
const RUN_DELAY: Duration = Duration::from_millis(35);
const PULL_DELAY: Duration = Duration::from_millis(35);
const OPERATION_TIMEOUT: Duration = Duration::from_secs(2);

#[tokio::test]
async fn persistent_session_reuses_one_negotiated_connection() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
    let address = listener.local_addr().expect("address");
    let server = tokio::spawn(serve_connections(
        listener,
        Arc::new(DelayedService::new(false, false)),
        1,
        CONNECTION_DELAY,
    ));

    let mut session = BoltProbeSession::connect(address, OPERATION_TIMEOUT)
        .await
        .expect("persistent session");
    let mut samples = Vec::new();
    for _ in 0..3 {
        samples.push(
            session
                .execute("RETURN 1 AS value", BTreeMap::new())
                .await
                .expect("persistent sample"),
        );
    }
    session.goodbye().await.expect("GOODBYE");
    server.await.expect("server task");

    assert!(
        samples
            .iter()
            .all(|sample| sample.ttfr() >= RUN_DELAY + PULL_DELAY)
    );
    assert!(
        samples
            .iter()
            .all(|sample| sample.total_latency() >= sample.ttfr())
    );
    assert!(samples.iter().all(|sample| sample.row_count() == 1));
    assert!(
        samples
            .windows(2)
            .all(|pair| pair[0].result_digest() == pair[1].result_digest())
    );
}

#[tokio::test]
async fn measures_run_and_pull_to_first_record_but_excludes_connection_handshake_and_hello() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
    let address = listener.local_addr().expect("address");
    let server = tokio::spawn(serve_connections(
        listener,
        Arc::new(DelayedService::new(false, false)),
        1,
        CONNECTION_DELAY,
    ));

    let wall_started = Instant::now();
    let report = probe(address, 0, 1).await.expect("probe result");
    let wall_elapsed = wall_started.elapsed();
    server.await.expect("server task");

    let sample = report.samples().first().expect("one sample");
    assert!(sample.ttfr() >= RUN_DELAY + PULL_DELAY);
    assert!(wall_elapsed >= CONNECTION_DELAY + HELLO_DELAY + RUN_DELAY + PULL_DELAY);
    assert!(
        wall_elapsed.saturating_sub(sample.ttfr()) >= CONNECTION_DELAY + HELLO_DELAY,
        "TTFR must begin after the connection, handshake, and HELLO"
    );
    assert!(sample.total_latency() >= sample.ttfr());
}

#[tokio::test]
async fn returns_a_stable_digest_and_row_count_for_bounded_samples() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
    let address = listener.local_addr().expect("address");
    let server = tokio::spawn(serve_connections(
        listener,
        Arc::new(DelayedService::new(false, false)),
        3,
        Duration::ZERO,
    ));

    let report = probe(address, 1, 2).await.expect("probe result");
    server.await.expect("server task");

    assert_eq!(report.samples().len(), 2);
    assert_eq!(report.row_count(), 1);
    assert_eq!(report.samples()[0].row_count(), 1);
    assert_eq!(report.samples()[1].row_count(), 1);
    assert_eq!(
        report.samples()[0].result_digest(),
        report.samples()[1].result_digest()
    );
    assert_eq!(report.result_digest(), report.samples()[0].result_digest());
}

#[tokio::test]
async fn rejects_server_failures_and_required_record_empty_results() {
    let failure_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("failure listener");
    let failure_address = failure_listener.local_addr().expect("failure address");
    let failure_server = tokio::spawn(serve_connections(
        failure_listener,
        Arc::new(DelayedService::new(true, false)),
        1,
        Duration::ZERO,
    ));
    let failure = probe(failure_address, 0, 1)
        .await
        .expect_err("RUN failure must reject probe");
    failure_server.await.expect("failure server task");
    assert!(matches!(
        failure,
        ExternalTtfrProbeError::ServerFailure { .. }
    ));

    let empty_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("empty listener");
    let empty_address = empty_listener.local_addr().expect("empty address");
    let empty_server = tokio::spawn(serve_connections(
        empty_listener,
        Arc::new(DelayedService::new(false, true)),
        1,
        Duration::ZERO,
    ));
    let empty = probe(empty_address, 0, 1)
        .await
        .expect_err("empty records must reject TTFR certification");
    empty_server.await.expect("empty server task");
    assert_eq!(empty, ExternalTtfrProbeError::EmptyResult);
}

#[tokio::test]
async fn rejects_zero_timeout_and_times_out_a_half_open_peer() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
    let address = listener.local_addr().expect("address");
    let zero_timeout =
        ExternalTtfrProbeConfig::new(address, "RETURN 1", BTreeMap::new(), 0, 1, Duration::ZERO);
    assert_eq!(
        zero_timeout.expect_err("zero timeout must be rejected"),
        ExternalTtfrProbeError::InvalidConfiguration
    );

    let server = tokio::spawn(async move {
        let (_stream, _) = listener.accept().await.expect("accept");
        tokio::time::sleep(Duration::from_secs(1)).await;
    });
    let config = ExternalTtfrProbeConfig::new(
        address,
        "RETURN 1",
        BTreeMap::new(),
        0,
        1,
        Duration::from_millis(30),
    )
    .expect("config");
    let started = Instant::now();
    let error = probe_external_ttfr(config)
        .await
        .expect_err("half-open peer must time out");
    server.abort();

    assert_eq!(error, ExternalTtfrProbeError::Timeout);
    assert!(started.elapsed() < Duration::from_millis(500));
}

#[test]
fn probe_cli_exposes_machine_readable_interface() {
    let output = Command::new(env!("CARGO_BIN_EXE_dtgproxy-bolt-probe"))
        .arg("--help")
        .output()
        .expect("run probe CLI");

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).expect("UTF-8 help");
    for option in [
        "--address",
        "--query",
        "--warmups",
        "--samples",
        "--timeout-ms",
    ] {
        assert!(stdout.contains(option), "help is missing {option}");
    }
}

async fn probe(
    address: SocketAddr,
    warmup_count: usize,
    sample_count: usize,
) -> Result<bolt_server::ExternalTtfrProbeReport, ExternalTtfrProbeError> {
    let config = ExternalTtfrProbeConfig::new(
        address,
        "RETURN 1 AS value",
        BTreeMap::new(),
        warmup_count,
        sample_count,
        OPERATION_TIMEOUT,
    )?;
    probe_external_ttfr(config).await
}

async fn serve_connections(
    listener: TcpListener,
    service: Arc<DelayedService>,
    connections: usize,
    connection_delay: Duration,
) {
    let mut tasks = Vec::with_capacity(connections);
    for _ in 0..connections {
        let (mut stream, _) = listener.accept().await.expect("accept");
        let service = Arc::clone(&service);
        tasks.push(tokio::spawn(async move {
            tokio::time::sleep(connection_delay).await;
            serve_connection(&mut stream, service, BoltConnectionConfig::default()).await
        }));
    }
    for task in tasks {
        task.await.expect("connection task").expect("connection");
    }
}

struct DelayedService {
    fail_run: bool,
    empty: bool,
}

impl DelayedService {
    const fn new(fail_run: bool, empty: bool) -> Self {
        Self { fail_run, empty }
    }
}

impl BoltService for DelayedService {
    fn hello<'a>(
        &'a self,
        _metadata: BTreeMap<String, Value>,
    ) -> ServiceFuture<'a, BTreeMap<String, Value>> {
        Box::pin(async move {
            tokio::time::sleep(HELLO_DELAY).await;
            Ok(BTreeMap::new())
        })
    }

    fn run<'a>(&'a self, _request: RunRequest) -> ServiceFuture<'a, RunOutcome> {
        Box::pin(async move {
            tokio::time::sleep(RUN_DELAY).await;
            if self.fail_run {
                return Err(ServiceError::new(
                    "Neo.ClientError.Statement.SyntaxError",
                    "bad RUN",
                ));
            }
            Ok(RunOutcome::new(CursorId::new(1), vec!["value".into()]))
        })
    }

    fn pull<'a>(&'a self, _cursor: CursorId, _n: i64) -> ServiceFuture<'a, PullOutcome> {
        Box::pin(async move {
            tokio::time::sleep(PULL_DELAY).await;
            let records = if self.empty {
                Vec::new()
            } else {
                vec![vec![Value::Integer(1)]]
            };
            Ok(PullOutcome::new(records, false, BTreeMap::new()))
        })
    }

    fn discard<'a>(&'a self, _cursor: CursorId, _n: i64) -> ServiceFuture<'a, bool> {
        Box::pin(async move { Ok(false) })
    }

    fn begin<'a>(&'a self, _extra: BTreeMap<String, Value>) -> ServiceFuture<'a, TransactionId> {
        Box::pin(async move {
            Err(ServiceError::new(
                "Neo.ClientError.Request.Invalid",
                "unused",
            ))
        })
    }

    fn commit<'a>(&'a self, _transaction: TransactionId) -> ServiceFuture<'a, String> {
        Box::pin(async move {
            Err(ServiceError::new(
                "Neo.ClientError.Request.Invalid",
                "unused",
            ))
        })
    }

    fn rollback<'a>(&'a self, _transaction: TransactionId) -> ServiceFuture<'a, ()> {
        Box::pin(async move { Ok(()) })
    }

    fn route<'a>(
        &'a self,
        _routing: BTreeMap<String, Value>,
        _bookmarks: Vec<Value>,
        _database: Option<String>,
    ) -> ServiceFuture<'a, BTreeMap<String, Value>> {
        Box::pin(async move {
            Err(ServiceError::new(
                "Neo.ClientError.Request.Invalid",
                "unused",
            ))
        })
    }
}
