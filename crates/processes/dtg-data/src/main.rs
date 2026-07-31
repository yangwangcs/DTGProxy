use dtg_data::{DataNodeBuilder, DataProcessConfig};
use dtg_execution::cluster_protocol::proto::data_service_server::DataServiceServer;
use dtg_execution::cluster_protocol::proto::gateway_service_server::GatewayServiceServer;
use dtg_execution::{RequestStageMetrics, encode_request_metrics_snapshot};
use std::io::Write;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::MissedTickBehavior;
use tonic::transport::Server;

const REQUEST_METRICS_PREFIX: &str = "DTG_REQUEST_STAGE_METRICS=";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = DataProcessConfig::from_env()?;
    let rpc_addr = config.rpc_addr();
    let node = DataNodeBuilder::from_config(config).start().await?;
    let metrics_exporter = spawn_metrics_exporter("data", node.request_metrics());
    let service = node.rpc_service();
    let shutdown = service.clone();

    let result = Server::builder()
        .add_service(DataServiceServer::new(service.clone()))
        .add_service(GatewayServiceServer::new(service))
        .serve_with_shutdown(rpc_addr, async move {
            let _ = tokio::signal::ctrl_c().await;
            shutdown.begin_draining();
        })
        .await;
    metrics_exporter.abort();
    let _ = metrics_exporter.await;
    result?;
    node.stop();
    Ok(())
}

fn spawn_metrics_exporter(
    role: &'static str,
    metrics: Arc<RequestStageMetrics>,
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
