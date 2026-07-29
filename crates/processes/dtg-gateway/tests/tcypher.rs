use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dtg_execution::{
    GatewayCancellationToken, GatewayClusterRequest, GatewayExecution, GatewayExecutionError,
    GatewayExecutionTransport, GatewayFuture, GatewayOperation, GatewayResponse, GatewayRetry,
    GatewayRows, GatewayTemporalMode, GatewayTime, GatewayValue,
};
use dtg_gateway::{GatewayConfig, GatewayService, serve_bolt};
use support::planning_context;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[derive(Default)]
struct CleanBreakTransport {
    requests: Mutex<Vec<GatewayClusterRequest>>,
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
        .param("t", 40_i64)
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
    assert_eq!(requests[0].parameters()["t"], GatewayValue::Integer(40));
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
        .param("t", 40_i64)
        .run()
        .await
        .unwrap();
    gateway
        .bolt()
        .query("CHANGES FOR SYSTEM_TIME BETWEEN $from AND $to MATCH (n) RETURN n.id")
        .param("from", 20_i64)
        .param("to", 40_i64)
        .run()
        .await
        .unwrap();
    assert_eq!(
        gateway
            .bolt()
            .query("CREATE (n {id: $id}) VALID FROM $t")
            .param("id", 3_i64)
            .param("t", 40_i64)
            .execute()
            .await
            .unwrap(),
        GatewayResponse::Acknowledged
    );

    let requests = transport.requests.lock().unwrap();
    assert_eq!(requests[0].temporal_mode(), &GatewayTemporalMode::Current);
    assert_eq!(
        requests[1].temporal_mode(),
        &GatewayTemporalMode::AsOf(GatewayTime::Parameter("t".into()))
    );
    assert_eq!(
        requests[2].temporal_mode(),
        &GatewayTemporalMode::Changes {
            from: GatewayTime::Parameter("from".into()),
            to: GatewayTime::Parameter("to".into()),
        }
    );
    assert_eq!(requests[3].operation(), &GatewayOperation::Write);
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
