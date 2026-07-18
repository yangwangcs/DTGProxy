use std::error::Error;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

use cluster_protocol::MAX_COMMAND_BYTES;
use cluster_protocol::proto::gateway_service_server::GatewayServiceServer;
use gateway_node::{
    GatewayCatalogRouter, GatewayNodeRuntimeConfig, GatewayTransportSecurity, RemoteGatewayService,
};
use shard_client::RemoteShardClient;
use tokio::sync::watch;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

const GRPC_ENVELOPE_ALLOWANCE: usize = 64 * 1024;

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("dtgproxy-gateway: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn Error>> {
    let config = GatewayNodeRuntimeConfig::load(config_path_from_args()?)?;
    let router = Arc::new(GatewayCatalogRouter::new(
        *config.cluster_id(),
        config.node_id(),
        config.graph_id(),
        config.meta_seeds().to_vec(),
        config.data_nodes().clone(),
        config.catalog_watch_timeout(),
    )?);
    let snapshot = router.load(1).await?;
    let (catalog_state, graph, topology) = snapshot.into_parts();
    let shard_client = Arc::new(RemoteShardClient::new_loopback_plaintext(
        *config.cluster_id(),
        topology,
    )?);
    let gateway = RemoteGatewayService::new_at_revision(
        *config.cluster_id(),
        catalog_state.revision(),
        graph,
        Arc::clone(&shard_client),
        config.meta_seeds().to_vec(),
        config.maximum_inflight(),
        config.max_raft_ticks(),
    )?;
    let listener = tokio::net::TcpListener::bind(config.listen_address()).await?;
    match config.transport_security() {
        GatewayTransportSecurity::LoopbackPlaintext => {}
    }

    let (shutdown_sender, shutdown_receiver) = watch::channel(false);
    let watcher = tokio::spawn({
        let router = Arc::clone(&router);
        let gateway = Arc::new(gateway.clone());
        let shard_client = Arc::clone(&shard_client);
        let receiver = shutdown_receiver.clone();
        async move {
            router
                .run_watch(catalog_state, gateway, shard_client, receiver)
                .await;
        }
    });
    let signal = tokio::spawn({
        let shutdown_sender = shutdown_sender.clone();
        async move {
            shutdown_signal().await;
            let _ = shutdown_sender.send(true);
        }
    });

    println!(
        "DTGPROXY_GATEWAY_READY node={} graph={} revision={} address={} advertise={}",
        config.node_id(),
        config.graph_id(),
        gateway.catalog_revision()?,
        listener.local_addr()?,
        config.advertise_address()
    );
    std::io::stdout().flush()?;

    let maximum_message = MAX_COMMAND_BYTES + GRPC_ENVELOPE_ALLOWANCE;
    let gateway_lifecycle = gateway.clone();
    let service = GatewayServiceServer::new(gateway)
        .max_decoding_message_size(maximum_message)
        .max_encoding_message_size(maximum_message);
    let server_result = Server::builder()
        .add_service(service)
        .serve_with_incoming_shutdown(
            TcpListenerStream::new(listener),
            shutdown_requested(shutdown_receiver, gateway_lifecycle),
        )
        .await;
    let _ = shutdown_sender.send(true);
    signal.abort();
    tokio::time::timeout(config.shutdown_grace(), watcher)
        .await
        .map_err(|_| "Gateway Catalog watcher exceeded shutdown grace")??;
    server_result?;
    Ok(())
}

async fn shutdown_requested(mut receiver: watch::Receiver<bool>, gateway: RemoteGatewayService) {
    while !*receiver.borrow() {
        if receiver.changed().await.is_err() {
            return;
        }
    }
    gateway.close_admission();
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        match signal(SignalKind::terminate()) {
            Ok(mut terminate) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = terminate.recv() => {}
                }
            }
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

fn config_path_from_args() -> Result<PathBuf, Box<dyn Error>> {
    let mut arguments = std::env::args_os().skip(1);
    let flag = arguments
        .next()
        .ok_or("usage: dtgproxy-gateway --config PATH")?;
    let path = arguments
        .next()
        .ok_or("usage: dtgproxy-gateway --config PATH")?;
    if flag != "--config" || arguments.next().is_some() {
        return Err("usage: dtgproxy-gateway --config PATH".into());
    }
    Ok(path.into())
}
