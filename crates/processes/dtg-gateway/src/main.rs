#![forbid(unsafe_code)]

use dtg_execution::{
    GatewayExecution, GatewayExecutionTransportFactory, TonicGatewayProtocolV2TransportFactory,
};
use dtg_gateway::{GatewayConfig, GatewayService};
use std::sync::Arc;
use tokio::net::TcpListener;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = GatewayConfig::from_env()?;
    let factory = TonicGatewayProtocolV2TransportFactory;
    let transport = factory.connect(config.cluster_endpoint()).await?;
    let planning_context = config.planning_context_from_env()?;
    let execution = GatewayExecution::for_process(transport, planning_context);
    let bind_addr = config.bind_addr();
    let service = Arc::new(GatewayService::new(config, execution));
    let listener = TcpListener::bind(bind_addr).await?;
    tokio::select! {
        result = dtg_gateway::serve_bolt(listener, service) => result?,
        result = tokio::signal::ctrl_c() => result?,
    }
    Ok(())
}
