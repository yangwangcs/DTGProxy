use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dtg_execution::{
    GatewayCancellationToken, GatewayClusterRequest, GatewayExecution, GatewayExecutionError,
    GatewayExecutionTransport, GatewayFuture, GatewayOperation, GatewayResponse, GatewayRetry,
    GatewayRows, GatewayTemporalMode, GatewayTime, GatewayValue, RequestDetail,
};
use dtg_gateway::{
    BoltStatementClass, GatewayConfig, GatewayService, bolt_read_pipeline_enabled_from, serve_bolt,
};
use support::planning_context;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::Semaphore;

#[derive(Default)]
struct CleanBreakTransport {
    requests: Mutex<Vec<GatewayClusterRequest>>,
}

struct PipelinedReadTransport {
    started: AtomicUsize,
    block_every_read: bool,
    release_first: Arc<Semaphore>,
}

impl PipelinedReadTransport {
    fn new() -> Self {
        Self {
            started: AtomicUsize::new(0),
            block_every_read: false,
            release_first: Arc::new(Semaphore::new(0)),
        }
    }

    fn blocking_all_reads() -> Self {
        Self {
            started: AtomicUsize::new(0),
            block_every_read: true,
            release_first: Arc::new(Semaphore::new(0)),
        }
    }

    async fn wait_until_started(&self, expected: usize) {
        tokio::time::timeout(Duration::from_secs(1), async {
            while self.started.load(Ordering::Acquire) < expected {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("pipeline must dispatch every queued read before the first response");
    }
}

impl GatewayExecutionTransport for PipelinedReadTransport {
    fn execute(
        &self,
        _request: GatewayClusterRequest,
    ) -> GatewayFuture<'_, Result<GatewayResponse, GatewayExecutionError>> {
        let position = self.started.fetch_add(1, Ordering::AcqRel);
        let release_first = Arc::clone(&self.release_first);
        let block_every_read = self.block_every_read;
        Box::pin(async move {
            if block_every_read || position == 0 {
                let permit = release_first
                    .acquire()
                    .await
                    .expect("test semaphore remains open");
                drop(permit);
            }
            Ok(GatewayResponse::Rows(
                GatewayRows::new(vec!["n.id".into()], vec![vec![GatewayValue::Integer(1)]])
                    .unwrap(),
            ))
        })
    }
}

impl GatewayExecutionTransport for CleanBreakTransport {
    fn execute(
        &self,
        request: GatewayClusterRequest,
    ) -> GatewayFuture<'_, Result<GatewayResponse, GatewayExecutionError>> {
        let response = if request
            .result_fields()
            .iter()
            .any(|field| field.contains("fail"))
        {
            Err(GatewayExecutionError::new(
                "DTG-CLUSTER-UNAVAILABLE",
                "fixture transport unavailable",
                GatewayRetry::Safe,
            ))
        } else {
            Ok(match request.operation() {
                GatewayOperation::Write => GatewayResponse::Acknowledged,
                _ => GatewayResponse::Rows(
                    GatewayRows::new(
                        vec!["n.id".into()],
                        vec![
                            vec![GatewayValue::Integer(1)],
                            vec![GatewayValue::Integer(2)],
                        ],
                    )
                    .unwrap(),
                ),
            })
        };
        self.requests.lock().unwrap().push(request);
        Box::pin(async move { response })
    }
}

#[tokio::test]
async fn bolt_query_uses_new_language_and_execution_path() {
    let transport = Arc::new(CleanBreakTransport::default());
    let execution = GatewayExecution::for_process(transport.clone(), planning_context());
    let config = GatewayConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 7687),
        7,
        Duration::from_secs(5),
    )
    .unwrap();
    let gateway = GatewayService::new(config, execution);

    let rows = gateway
        .bolt()
        .query("MATCH (n) FOR SYSTEM_TIME AS OF $t RETURN n.id ORDER BY n.id")
        .param("t", 41_i64)
        .run()
        .await
        .unwrap();

    assert_eq!(rows.fields(), &["n.id"]);
    assert_eq!(
        rows.rows(),
        &[
            vec![GatewayValue::Integer(1)],
            vec![GatewayValue::Integer(2)],
        ]
    );
    let requests = transport.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].parameters()["t"], GatewayValue::Integer(41));
    assert!(requests[0].is_query());
}

#[tokio::test]
async fn bolt_dispatches_normalized_temporal_queries_and_writes() {
    let transport = Arc::new(CleanBreakTransport::default());
    let execution = GatewayExecution::for_process(transport.clone(), planning_context());
    let config = GatewayConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 7687),
        7,
        Duration::from_secs(5),
    )
    .unwrap();
    let gateway = GatewayService::new(config, execution);

    gateway
        .bolt()
        .query("MATCH (n) RETURN n.id")
        .run()
        .await
        .unwrap();
    gateway
        .bolt()
        .query("MATCH (n) FOR SYSTEM_TIME AS OF $t RETURN n.id")
        .param("t", 41_i64)
        .run()
        .await
        .unwrap();
    let changes = gateway
        .bolt()
        .query("CHANGES FOR SYSTEM_TIME BETWEEN $from AND $to MATCH (n) RETURN n.id")
        .param("from", 20_i64)
        .param("to", 40_i64)
        .run()
        .await
        .unwrap_err();
    assert_eq!(changes.code(), "DTG-EXECUTION-LOWER");
    let write = gateway
        .bolt()
        .query("CREATE (n {id: $id}) VALID FROM $t")
        .param("id", 3_i64)
        .param("t", 40_i64)
        .execute()
        .await
        .unwrap_err();
    assert_eq!(write.code(), "DTG-EXECUTION-WRITE-TRANSPORT");

    let requests = transport.requests.lock().unwrap();
    assert_eq!(requests[0].temporal_mode(), &GatewayTemporalMode::Current);
    assert_eq!(
        requests[1].temporal_mode(),
        &GatewayTemporalMode::AsOf(GatewayTime::Parameter("t".into()))
    );
    assert_eq!(requests.len(), 2);
}

#[tokio::test]
async fn cancellation_and_deadline_stop_before_cluster_dispatch() {
    let transport = Arc::new(CleanBreakTransport::default());
    let execution = GatewayExecution::for_process(transport.clone(), planning_context());
    let config = GatewayConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 7687),
        7,
        Duration::from_secs(5),
    )
    .unwrap();
    let gateway = GatewayService::new(config, execution);
    let cancellation = GatewayCancellationToken::new();
    cancellation.cancel();

    let cancelled = gateway
        .bolt()
        .query("MATCH (n) RETURN n.id")
        .cancellation(cancellation)
        .run()
        .await
        .unwrap_err();
    assert_eq!(cancelled.code(), "DTG-EXECUTION-CANCELLED");

    let expired = gateway
        .bolt()
        .query("MATCH (n) RETURN n.id")
        .timeout(Duration::ZERO)
        .run()
        .await
        .unwrap_err();
    assert_eq!(expired.code(), "DTG-EXECUTION-DEADLINE");
    assert!(transport.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn language_and_cluster_errors_keep_stable_codes() {
    let transport = Arc::new(CleanBreakTransport::default());
    let execution = GatewayExecution::for_process(transport.clone(), planning_context());
    let config = GatewayConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 7687),
        7,
        Duration::from_secs(5),
    )
    .unwrap();
    let gateway = GatewayService::new(config, execution);

    let user_code = gateway
        .bolt()
        .query("CALL user.uploaded_code()")
        .run()
        .await
        .unwrap_err();
    assert_eq!(user_code.code(), "DTG-LANG-UNKNOWN-BUILTIN");
    assert!(transport.requests.lock().unwrap().is_empty());

    let unavailable = gateway
        .bolt()
        .query("MATCH (n) RETURN n.fail")
        .run()
        .await
        .unwrap_err();
    assert_eq!(unavailable.code(), "DTG-CLUSTER-UNAVAILABLE");
}

#[test]
fn bolt_read_pipeline_gate_defaults_off_and_only_accepts_one() {
    assert!(!bolt_read_pipeline_enabled_from(None));
    assert!(bolt_read_pipeline_enabled_from(Some("1")));
    assert!(!bolt_read_pipeline_enabled_from(Some("0")));
    assert!(!bolt_read_pipeline_enabled_from(Some("invalid")));
}

#[test]
fn compiled_bolt_query_is_eligible_but_writes_and_transactions_are_barriers() {
    let transport = Arc::new(CleanBreakTransport::default());
    let execution = GatewayExecution::for_process(transport, planning_context());
    let config = GatewayConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 7687),
        7,
        Duration::from_secs(5),
    )
    .unwrap();
    let gateway = GatewayService::new(config, execution);

    assert_eq!(
        gateway
            .classify_bolt_statement("MATCH (n) RETURN n.id")
            .unwrap(),
        BoltStatementClass::Read
    );
    assert_eq!(
        gateway
            .classify_bolt_statement("CREATE (n) VALID FROM 1")
            .unwrap(),
        BoltStatementClass::Barrier
    );
    assert_eq!(
        gateway.classify_bolt_statement("BEGIN").unwrap(),
        BoltStatementClass::Barrier
    );
}

#[test]
fn gateway_binary_is_published_under_the_cutover_name() {
    assert!(std::path::Path::new(env!("CARGO_BIN_EXE_dtgproxy-gateway")).exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bolt_tcp_listener_serves_handshake_run_and_pull() {
    let transport = Arc::new(CleanBreakTransport::default());
    let execution = GatewayExecution::for_process(transport.clone(), planning_context());
    let config = GatewayConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
        7,
        Duration::from_secs(5),
    )
    .unwrap();
    let gateway = Arc::new(GatewayService::new(config, execution));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { serve_bolt(listener, gateway).await });

    let mut socket = tokio::net::TcpStream::connect(address).await.unwrap();
    socket
        .write_all(&[
            0x60, 0x60, 0xb0, 0x17, 0x00, 0x00, 0x04, 0x05, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ])
        .await
        .unwrap();
    let mut selected = [0_u8; 4];
    socket.read_exact(&mut selected).await.unwrap();
    assert_eq!(selected, [0x00, 0x00, 0x04, 0x05]);

    write_bolt_message(&mut socket, &[0xb1, 0x01, 0xa0]).await;
    assert_eq!(read_bolt_message(&mut socket).await[1], 0x70);

    let statement = b"MATCH (n) WHERE n.id = $id RETURN n.id";
    let mut run = vec![0xb3, 0x10, 0xd0, statement.len() as u8];
    run.extend_from_slice(statement);
    run.extend_from_slice(&[0xa1, 0x82, b'i', b'd', 0xc9, 0x00, 0xc8, 0xa0]);
    write_bolt_message(&mut socket, &run).await;
    assert_eq!(read_bolt_message(&mut socket).await[1], 0x70);

    write_bolt_message(&mut socket, &[0xb1, 0x3f, 0xa0]).await;
    let first = read_bolt_message(&mut socket).await;
    let second = read_bolt_message(&mut socket).await;
    let summary = read_bolt_message(&mut socket).await;
    assert_eq!(first, vec![0xb1, 0x71, 0x91, 0x01]);
    assert_eq!(second, vec![0xb1, 0x71, 0x91, 0x02]);
    assert_eq!(summary[1], 0x70);
    let requests = transport.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].parameters()["id"], GatewayValue::Integer(200));

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bolt_read_pipeline_dispatches_two_reads_before_the_first_response() {
    let transport = Arc::new(PipelinedReadTransport::new());
    let execution = GatewayExecution::for_process(transport.clone(), planning_context());
    let config = GatewayConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
        7,
        Duration::from_secs(5),
    )
    .unwrap()
    .with_bolt_read_pipeline_enabled(true);
    let gateway = Arc::new(GatewayService::new(config, execution));
    let request_metrics = gateway.request_metrics();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { serve_bolt(listener, gateway).await });

    let mut socket = tokio::net::TcpStream::connect(address).await.unwrap();
    socket
        .write_all(&[
            0x60, 0x60, 0xb0, 0x17, 0x00, 0x00, 0x04, 0x05, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ])
        .await
        .unwrap();
    let mut selected = [0_u8; 4];
    socket.read_exact(&mut selected).await.unwrap();
    write_bolt_message(&mut socket, &[0xb1, 0x01, 0xa0]).await;
    assert_eq!(read_bolt_message(&mut socket).await[1], 0x70);

    let statement = b"MATCH (n) RETURN n.id";
    let mut run = vec![0xb3, 0x10, 0xd0, statement.len() as u8];
    run.extend_from_slice(statement);
    run.extend_from_slice(&[0xa0, 0xa0]);
    let pull = [0xb1, 0x3f, 0xa0];
    write_bolt_message(&mut socket, &run).await;
    write_bolt_message(&mut socket, &pull).await;
    write_bolt_message(&mut socket, &run).await;
    write_bolt_message(&mut socket, &pull).await;

    transport.wait_until_started(2).await;
    transport.release_first.add_permits(1);

    for signature in [0x70, 0x71, 0x70, 0x70, 0x71, 0x70] {
        assert_eq!(read_bolt_message(&mut socket).await[1], signature);
    }
    let details = request_metrics.snapshot().details().collect::<Vec<_>>();
    for detail in [
        RequestDetail::BoltReadPipelineEnqueueWait,
        RequestDetail::BoltReadPipelineExecutionWait,
        RequestDetail::BoltReadPipelineOrderedWriteWait,
    ] {
        assert!(
            details
                .iter()
                .find(|(candidate, _)| *candidate == detail)
                .is_some_and(|(_, snapshot)| snapshot.success >= 2),
            "pipeline must record {detail:?} for every completed read",
        );
    }
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bolt_read_pipeline_reset_cancels_pending_reads_without_late_responses() {
    let transport = Arc::new(PipelinedReadTransport::blocking_all_reads());
    let execution = GatewayExecution::for_process(transport.clone(), planning_context());
    let config = GatewayConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
        7,
        Duration::from_secs(5),
    )
    .unwrap()
    .with_bolt_read_pipeline_enabled(true);
    let gateway = Arc::new(GatewayService::new(config, execution));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { serve_bolt(listener, gateway).await });

    let mut socket = connect_bolt(address).await;
    let run = read_run_message();
    let pull = [0xb1, 0x3f, 0xa0];
    write_bolt_message(&mut socket, &run).await;
    write_bolt_message(&mut socket, &pull).await;
    write_bolt_message(&mut socket, &run).await;
    write_bolt_message(&mut socket, &pull).await;
    transport.wait_until_started(2).await;

    write_bolt_message(&mut socket, &[0xb0, 0x02]).await;
    assert_eq!(read_bolt_message(&mut socket).await[1], 0x70);
    transport.release_first.add_permits(2);
    let late =
        tokio::time::timeout(Duration::from_millis(100), read_bolt_message(&mut socket)).await;
    assert!(late.is_err(), "late response: {late:?}");
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bolt_read_pipeline_drains_before_a_write_barrier() {
    let transport = Arc::new(PipelinedReadTransport::new());
    let execution = GatewayExecution::for_process(transport.clone(), planning_context());
    let config = GatewayConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
        7,
        Duration::from_secs(5),
    )
    .unwrap()
    .with_bolt_read_pipeline_enabled(true);
    let gateway = Arc::new(GatewayService::new(config, execution));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { serve_bolt(listener, gateway).await });

    let mut socket = connect_bolt(address).await;
    let pull = [0xb1, 0x3f, 0xa0];
    write_bolt_message(&mut socket, &run_message("MATCH (n) RETURN n.id")).await;
    write_bolt_message(&mut socket, &pull).await;
    transport.wait_until_started(1).await;

    write_bolt_message(&mut socket, &run_message("CREATE (n) VALID FROM 1")).await;
    write_bolt_message(&mut socket, &pull).await;

    transport.release_first.add_permits(1);
    for (index, signature) in [0x70, 0x71, 0x70, 0x7f, 0x70].into_iter().enumerate() {
        let message = tokio::time::timeout(Duration::from_secs(1), read_bolt_message(&mut socket))
            .await
            .unwrap_or_else(|_| panic!("timed out waiting for barrier response {index}"));
        assert_eq!(message[1], signature);
    }
    server.abort();
}

async fn connect_bolt(address: SocketAddr) -> tokio::net::TcpStream {
    let mut socket = tokio::net::TcpStream::connect(address).await.unwrap();
    socket
        .write_all(&[
            0x60, 0x60, 0xb0, 0x17, 0x00, 0x00, 0x04, 0x05, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ])
        .await
        .unwrap();
    let mut selected = [0_u8; 4];
    socket.read_exact(&mut selected).await.unwrap();
    write_bolt_message(&mut socket, &[0xb1, 0x01, 0xa0]).await;
    assert_eq!(read_bolt_message(&mut socket).await[1], 0x70);
    socket
}

fn read_run_message() -> Vec<u8> {
    run_message("MATCH (n) RETURN n.id")
}

fn run_message(statement: &str) -> Vec<u8> {
    let statement = statement.as_bytes();
    let mut run = vec![0xb3, 0x10, 0xd0, statement.len() as u8];
    run.extend_from_slice(statement);
    run.extend_from_slice(&[0xa0, 0xa0]);
    run
}

async fn write_bolt_message(socket: &mut tokio::net::TcpStream, message: &[u8]) {
    socket
        .write_all(&(message.len() as u16).to_be_bytes())
        .await
        .unwrap();
    socket.write_all(message).await.unwrap();
    socket.write_all(&[0, 0]).await.unwrap();
}

async fn read_bolt_message(socket: &mut tokio::net::TcpStream) -> Vec<u8> {
    let mut message = Vec::new();
    loop {
        let mut size = [0_u8; 2];
        socket.read_exact(&mut size).await.unwrap();
        let size = usize::from(u16::from_be_bytes(size));
        if size == 0 {
            return message;
        }
        let start = message.len();
        message.resize(start + size, 0);
        socket.read_exact(&mut message[start..]).await.unwrap();
    }
}
mod support;
