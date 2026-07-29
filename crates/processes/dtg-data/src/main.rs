use dtg_cluster_v2::proto::data_service_server::DataServiceServer;
use dtg_data::{DataNodeBuilder, DataProcessConfig};
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = DataProcessConfig::from_env()?;
    let rpc_addr = config.rpc_addr();
    let node = DataNodeBuilder::from_config(config).start().await?;
    let service = node.rpc_service();
    let shutdown = service.clone();

    Server::builder()
        .add_service(DataServiceServer::new(service))
        .serve_with_shutdown(rpc_addr, async move {
            let _ = tokio::signal::ctrl_c().await;
            shutdown.begin_draining();
        })
        .await?;
    node.stop();
    Ok(())
}
