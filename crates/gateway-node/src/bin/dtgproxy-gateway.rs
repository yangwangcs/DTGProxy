use std::error::Error;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
#[cfg(all(feature = "paper-benchmark-control", unix))]
use std::{os::unix::fs::FileTypeExt, os::unix::fs::PermissionsExt, time::Duration};

use bolt_server::{BoltConnectionConfig, serve_connection};
use cluster_protocol::MAX_COMMAND_BYTES;
use cluster_protocol::proto::gateway_service_server::GatewayServiceServer;
use cypher_engine::CypherBoltService;
#[cfg(all(feature = "paper-benchmark-control", unix))]
use gateway_node::{BenchmarkAblationRuntime, serve_benchmark_ablation_control};
use gateway_node::{
    GatewayCatalogRouter, GatewayNodeRuntimeConfig, GatewayTransportSecurity, RemoteGatewayService,
};
use shard_client::RemoteShardClient;
use tokio::sync::Semaphore;
use tokio::sync::watch;
use tokio::task::JoinSet;
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
    let gateway = RemoteGatewayService::new_at_revision_with_gateway_id(
        config.node_id(),
        *config.cluster_id(),
        catalog_state.revision(),
        graph,
        Arc::clone(&shard_client),
        config.meta_seeds().to_vec(),
        config.maximum_inflight(),
        config.max_raft_ticks(),
    )?;
    #[cfg(all(feature = "paper-benchmark-control", unix))]
    let (gateway, benchmark_runtime, benchmark_control_path) = {
        let path = std::env::var_os("DTGPROXY_PAPER_ABLATION_CONTROL").map(PathBuf::from);
        let runtime = path
            .as_ref()
            .map(|_| Arc::new(BenchmarkAblationRuntime::new()));
        let gateway = if let Some(runtime) = runtime.as_ref() {
            gateway.with_benchmark_ablation_runtime(Arc::clone(runtime))
        } else {
            gateway
        };
        (gateway, runtime, path)
    };
    let listener = tokio::net::TcpListener::bind(config.listen_address()).await?;
    let bolt_listener = match config.bolt_listen_address() {
        Some(address) => Some(tokio::net::TcpListener::bind(address).await?),
        None => None,
    };
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
    #[cfg(all(feature = "paper-benchmark-control", unix))]
    let benchmark_control = match (benchmark_runtime, benchmark_control_path) {
        (Some(runtime), Some(path)) => {
            if !path.is_absolute() {
                return Err("DTGPROXY_PAPER_ABLATION_CONTROL must be an absolute path".into());
            }
            let receiver = shutdown_receiver.clone();
            let task_path = path.clone();
            let mut task = tokio::spawn(async move {
                serve_benchmark_ablation_control(&task_path, runtime, receiver).await
            });
            await_benchmark_control_ready(&path, &mut task).await?;
            Some(task)
        }
        _ => None,
    };
    let bolt_address = bolt_listener
        .as_ref()
        .map(|listener| listener.local_addr())
        .transpose()?;
    let bolt_server = bolt_listener.map(|listener| {
        let gateway = gateway.clone();
        let receiver = shutdown_receiver.clone();
        let maximum_connections = config.maximum_inflight();
        tokio::spawn(async move {
            run_bolt_listener(listener, gateway, receiver, maximum_connections).await
        })
    });

    println!(
        "DTGPROXY_GATEWAY_READY node={} graph={} revision={} address={} advertise={} bolt={}",
        config.node_id(),
        config.graph_id(),
        gateway.catalog_revision()?,
        listener.local_addr()?,
        config.advertise_address(),
        bolt_address.map_or_else(|| "disabled".into(), |address| address.to_string())
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
    if let Some(bolt_server) = bolt_server {
        tokio::time::timeout(config.shutdown_grace(), bolt_server)
            .await
            .map_err(|_| "Gateway Bolt server exceeded shutdown grace")??
            .map_err(|error| format!("Gateway Bolt server failed: {error}"))?;
    }
    #[cfg(all(feature = "paper-benchmark-control", unix))]
    if let Some(benchmark_control) = benchmark_control {
        tokio::time::timeout(config.shutdown_grace(), benchmark_control)
            .await
            .map_err(|_| "Gateway benchmark control exceeded shutdown grace")???;
    }
    server_result?;
    Ok(())
}

#[cfg(all(feature = "paper-benchmark-control", unix))]
async fn await_benchmark_control_ready(
    path: &std::path::Path,
    task: &mut tokio::task::JoinHandle<std::io::Result<()>>,
) -> Result<(), Box<dyn Error>> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if task.is_finished() {
            return Err(format!(
                "Gateway benchmark control failed before ready: {:?}",
                task.await?
            )
            .into());
        }
        if let Ok(metadata) = std::fs::metadata(path) {
            if !metadata.file_type().is_socket() || metadata.permissions().mode() & 0o777 != 0o600 {
                return Err("Gateway benchmark control path is not a mode-0600 Unix socket".into());
            }
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err("Gateway benchmark control did not become ready within 5 seconds".into());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn run_bolt_listener(
    listener: tokio::net::TcpListener,
    gateway: RemoteGatewayService,
    mut shutdown: watch::Receiver<bool>,
    maximum_connections: usize,
) -> Result<(), String> {
    let permits = Arc::new(Semaphore::new(maximum_connections));
    let mut connections = JoinSet::new();
    while !*shutdown.borrow() {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            accepted = listener.accept() => {
                let (mut socket, _) = accepted.map_err(|error| error.to_string())?;
                let Ok(permit) = Arc::clone(&permits).try_acquire_owned() else {
                    continue;
                };
                let backend = Arc::new(gateway.clone());
                connections.spawn(async move {
                    let _permit = permit;
                    let Ok(service) = CypherBoltService::new(backend, 64) else {
                        return;
                    };
                    let _ = serve_connection(
                        &mut socket,
                        Arc::new(service),
                        BoltConnectionConfig::default(),
                    )
                    .await;
                });
            }
        }
    }
    connections.abort_all();
    while connections.join_next().await.is_some() {}
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
