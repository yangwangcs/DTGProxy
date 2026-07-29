use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dtg_execution::{
    GatewayCancellationToken, GatewayClusterRequest, GatewayExecution, GatewayExecutionError,
    GatewayExecutionTransport, GatewayFuture, GatewayOperation, GatewayResponse, GatewayRetry,
    GatewayRows, GatewayTemporalMode, GatewayTime, GatewayValue,
};
use dtg_gateway::{GatewayConfig, GatewayService};
use support::planning_context;

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
mod support;
