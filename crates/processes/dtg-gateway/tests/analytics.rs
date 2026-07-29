use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dtg_execution::{
    GatewayAnalyticsState, GatewayClusterRequest, GatewayExecution, GatewayExecutionError,
    GatewayExecutionTransport, GatewayFuture, GatewayOperation, GatewayResponse, GatewayRows,
    GatewayValue,
};
use dtg_gateway::{GatewayConfig, GatewayService};
use support::planning_context;

#[derive(Default)]
struct AnalyticsTransport {
    operations: Mutex<Vec<GatewayOperation>>,
}

impl GatewayExecutionTransport for AnalyticsTransport {
    fn execute(
        &self,
        request: GatewayClusterRequest,
    ) -> GatewayFuture<'_, Result<GatewayResponse, GatewayExecutionError>> {
        let response = match request.operation() {
            GatewayOperation::SubmitAnalytics { algorithm } if algorithm == "page_rank" => {
                GatewayResponse::AnalyticsSubmitted { job_id: 17 }
            }
            GatewayOperation::AnalyticsStatus { job_id: 17 } => GatewayResponse::AnalyticsStatus {
                job_id: 17,
                state: GatewayAnalyticsState::Running,
            },
            GatewayOperation::AnalyticsResult { job_id: 17 } => GatewayResponse::AnalyticsResult {
                job_id: 17,
                rows: GatewayRows::new(
                    vec!["vertex".into(), "score".into()],
                    vec![vec![GatewayValue::Integer(1), GatewayValue::FloatBits(42)]],
                )
                .unwrap(),
            },
            GatewayOperation::CancelAnalytics { job_id: 17 } => {
                GatewayResponse::AnalyticsCancelled { job_id: 17 }
            }
            operation => panic!("unexpected analytics operation: {operation:?}"),
        };
        self.operations
            .lock()
            .unwrap()
            .push(request.operation().clone());
        Box::pin(async move { Ok(response) })
    }
}

fn gateway(transport: Arc<AnalyticsTransport>) -> GatewayService {
    let execution = GatewayExecution::for_process(transport, planning_context());
    let config = GatewayConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 7687),
        7,
        Duration::from_secs(5),
    )
    .unwrap();
    GatewayService::new(config, execution)
}

#[tokio::test]
async fn built_in_analytics_submit_status_result_and_cancel_use_execution_facade() {
    let transport = Arc::new(AnalyticsTransport::default());
    let gateway = gateway(transport.clone());

    assert_eq!(
        gateway
            .bolt()
            .query("SUBMIT ANALYTICS page_rank ASYNC")
            .execute()
            .await
            .unwrap(),
        GatewayResponse::AnalyticsSubmitted { job_id: 17 }
    );
    assert_eq!(
        gateway.bolt().analytics_status(17).await.unwrap(),
        GatewayAnalyticsState::Running
    );
    let rows = gateway.bolt().analytics_result(17).await.unwrap();
    assert_eq!(rows.fields(), &["vertex", "score"]);
    gateway.bolt().analytics_cancel(17).await.unwrap();

    assert_eq!(
        transport.operations.lock().unwrap().as_slice(),
        &[
            GatewayOperation::SubmitAnalytics {
                algorithm: "page_rank".into(),
            },
            GatewayOperation::AnalyticsStatus { job_id: 17 },
            GatewayOperation::AnalyticsResult { job_id: 17 },
            GatewayOperation::CancelAnalytics { job_id: 17 },
        ]
    );
}
mod support;
