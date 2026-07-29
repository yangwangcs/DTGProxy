#![forbid(unsafe_code)]

use dtg_execution::{
    GatewayExecution, GatewayExecutionTransportFactory, TonicGatewayProtocolV2TransportFactory,
};
use dtg_gateway::{GatewayConfig, GatewayService};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = GatewayConfig::from_env()?;
    let factory = TonicGatewayProtocolV2TransportFactory;
    let transport = factory.connect(config.cluster_endpoint()).await?;
    let execution = GatewayExecution::for_process(transport);
    let _service = GatewayService::new(config, execution);
    tokio::signal::ctrl_c().await?;
    Ok(())
}
