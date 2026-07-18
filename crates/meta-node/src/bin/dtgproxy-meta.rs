use std::error::Error;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

use cluster_protocol::MAX_COMMAND_BYTES;
use cluster_protocol::proto::meta_raft_service_server::MetaRaftServiceServer;
use cluster_protocol::proto::meta_service_server::MetaServiceServer;
use meta_node::{
    MetaNodeRuntimeConfig, MetaNodeService, MetaRaftGrpcService, MetaRaftReplica, MetaRaftRuntime,
    MetaTransportSecurity, ReplicatedTso,
};
use timestamp_oracle::{PhysicalClock, SystemClock, TimestampOracleError};
use tokio::sync::{Mutex, watch};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};

const GRPC_ENVELOPE_ALLOWANCE: usize = 64 * 1024;
const LEADERSHIP_ROUNDS: usize = 128;

#[derive(Debug, Default)]
struct MillisecondClock;

impl PhysicalClock for MillisecondClock {
    fn now_micros(&self) -> Result<i64, TimestampOracleError> {
        SystemClock
            .now_micros()
            .map(|micros| micros.div_euclid(1_000) * 1_000)
    }
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("dtgproxy-meta: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn Error>> {
    let config = MetaNodeRuntimeConfig::load(config_path_from_args()?)?;
    std::fs::create_dir_all(config.data_directory())?;
    let replica = MetaRaftReplica::open(
        config.node_id(),
        config.voters(),
        config.data_directory().join("raft"),
        config.data_directory().join("state"),
    )?;
    let listen_address = config.listen_address();
    let raft_listen_address = config.raft_listen_address();
    let security = config.transport_security().clone();
    let shutdown_grace = config.shutdown_grace();
    let tso = Arc::new(ReplicatedTso::new(
        Arc::new(MillisecondClock),
        config.timestamp_reservation_size(),
        config.maximum_future_drift_micros(),
    )?);
    let replica = Arc::new(Mutex::new(replica));
    let runtime = MetaRaftRuntime::spawn(
        *config.cluster_id(),
        config.node_id(),
        Arc::clone(&replica),
        config.peer_addresses().clone(),
        &security,
    )?;
    let runtime_notify = runtime.notify();
    let service = MetaNodeService::new(*config.cluster_id(), Arc::clone(&replica), tso)
        .with_runtime_notify(Arc::clone(&runtime_notify));
    let raft_service = MetaRaftGrpcService::new(
        *config.cluster_id(),
        config.node_id(),
        Arc::clone(&replica),
        runtime_notify,
    );
    let listener = tokio::net::TcpListener::bind(listen_address).await?;
    let raft_listener = tokio::net::TcpListener::bind(raft_listen_address).await?;

    {
        let mut replica = replica.lock().await;
        replica.campaign()?;
    }
    runtime.notify().notify_one();
    if config.voters().len() == 1 {
        for _ in 0..LEADERSHIP_ROUNDS {
            if replica.lock().await.is_leader() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        if !replica.lock().await.is_leader() {
            return Err("single-voter Meta did not elect itself Leader".into());
        }
    }

    let mut api_builder = Server::builder();
    let mut raft_builder = Server::builder();
    if let MetaTransportSecurity::MutualTls(files) = security {
        let ca = std::fs::read(files.ca_certificate())?;
        let certificate = std::fs::read(files.node_certificate())?;
        let private_key = std::fs::read(files.private_key())?;
        api_builder = api_builder.tls_config(
            ServerTlsConfig::new()
                .identity(Identity::from_pem(certificate.clone(), private_key.clone()))
                .client_ca_root(Certificate::from_pem(ca.clone())),
        )?;
        raft_builder = raft_builder.tls_config(
            ServerTlsConfig::new()
                .identity(Identity::from_pem(certificate, private_key))
                .client_ca_root(Certificate::from_pem(ca)),
        )?;
    }
    let maximum_message = MAX_COMMAND_BYTES + GRPC_ENVELOPE_ALLOWANCE;
    let meta_service = MetaServiceServer::new(service)
        .max_decoding_message_size(maximum_message)
        .max_encoding_message_size(maximum_message);
    let raft_service = MetaRaftServiceServer::new(raft_service)
        .max_decoding_message_size(maximum_message)
        .max_encoding_message_size(maximum_message);
    println!(
        "DTGPROXY_META_READY node={} address={} raft_address={}",
        config.node_id(),
        listener.local_addr()?,
        raft_listener.local_addr()?
    );
    std::io::stdout().flush()?;

    let (shutdown_sender, shutdown_receiver) = watch::channel(false);
    let signal = tokio::spawn(async move {
        shutdown_signal().await;
        let _ = shutdown_sender.send(true);
    });
    let api_server = api_builder
        .add_service(meta_service)
        .serve_with_incoming_shutdown(
            TcpListenerStream::new(listener),
            shutdown_requested(shutdown_receiver.clone()),
        );
    let raft_server = raft_builder
        .add_service(raft_service)
        .serve_with_incoming_shutdown(
            TcpListenerStream::new(raft_listener),
            shutdown_requested(shutdown_receiver),
        );
    let (api_result, raft_result) = tokio::join!(api_server, raft_server);
    api_result?;
    raft_result?;
    signal.abort();
    runtime.shutdown().await?;
    tokio::time::timeout(shutdown_grace, async {
        let mut replica = replica.lock().await;
        if replica.state().applied_index() > 0 {
            replica.create_snapshot()?;
        }
        Ok::<(), meta_node::MetaRaftError>(())
    })
    .await
    .map_err(|_| "Meta node graceful shutdown exceeded its deadline")??;
    Ok(())
}

async fn shutdown_requested(mut receiver: watch::Receiver<bool>) {
    while !*receiver.borrow() {
        if receiver.changed().await.is_err() {
            return;
        }
    }
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
        .ok_or("usage: dtgproxy-meta --config PATH")?;
    let path = arguments
        .next()
        .ok_or("usage: dtgproxy-meta --config PATH")?;
    if flag != "--config" || arguments.next().is_some() {
        return Err("usage: dtgproxy-meta --config PATH".into());
    }
    Ok(path.into())
}
