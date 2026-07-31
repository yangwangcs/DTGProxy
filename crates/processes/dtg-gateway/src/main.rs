#![forbid(unsafe_code)]

use dtg_execution::{
    GatewayExecution, GatewayExecutionTransportFactory, TonicGatewayProtocolV2TransportFactory,
    TonicGatewayWriteTransport,
};
use dtg_gateway::{GatewayConfig, GatewayService, build_gateway_runtime};
use std::sync::Arc;
use tokio::net::TcpListener;

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
    let listener = TcpListener::bind(bind_addr).await?;
    tokio::select! {
        result = dtg_gateway::serve_bolt(listener, service) => result?,
        result = tokio::signal::ctrl_c() => result?,
    }
    Ok(())
}
