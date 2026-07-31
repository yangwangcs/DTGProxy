#![forbid(unsafe_code)]

use dtg_execution::{
    GatewayExecution, GatewayExecutionTransportFactory, TonicGatewayProtocolV2TransportFactory,
    TonicGatewayWriteTransport, encode_request_metrics_snapshot,
};
use dtg_gateway::{GatewayConfig, GatewayService, build_gateway_runtime};
use std::io::Write;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::net::TcpListener;
use tokio::time::MissedTickBehavior;

const REQUEST_METRICS_PREFIX: &str = "DTG_REQUEST_STAGE_METRICS=";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    build_gateway_runtime()?.block_on(run())
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let config = GatewayConfig::from_env()?;
    let factory = TonicGatewayProtocolV2TransportFactory;
    let transport = factory.connect(config.cluster_endpoint()).await?;
    let write_transport =
        TonicGatewayWriteTransport::connect(config.meta_endpoint(), config.cluster_endpoint())
            .await?;
    let planning_context = config.planning_context_from_env()?;
    let execution =
        GatewayExecution::for_process_with_writes(transport, write_transport, planning_context);
    let bind_addr = config.bind_addr();
    let service = Arc::new(GatewayService::new(config, execution));
    let metrics_exporter = spawn_metrics_exporter("gateway", service.request_metrics());
    let listener = TcpListener::bind(bind_addr).await?;
    let result = tokio::select! {
        result = dtg_gateway::serve_bolt(listener, service) => result,
        result = tokio::signal::ctrl_c() => result,
    };
    metrics_exporter.abort();
    let _ = metrics_exporter.await;
    result?;
    Ok(())
}

fn spawn_metrics_exporter(
    role: &'static str,
    metrics: Arc<dtg_execution::RequestStageMetrics>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut sequence = 0_u64;
        loop {
            interval.tick().await;
            sequence = sequence.saturating_add(1);
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
                .try_into()
                .unwrap_or(u64::MAX);
            if let Ok(encoded) =
                encode_request_metrics_snapshot(role, timestamp, sequence, &metrics.snapshot())
            {
                let mut stderr = std::io::stderr().lock();
                let _ = writeln!(stderr, "{REQUEST_METRICS_PREFIX}{encoded}");
            }
        }
    })
}
