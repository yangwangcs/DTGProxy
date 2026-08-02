use dtg_data::{DataNodeBuilder, DataProcessConfig};
use dtg_execution::cluster_protocol::proto::data_service_server::DataServiceServer;
use dtg_execution::cluster_protocol::proto::gateway_service_server::GatewayServiceServer;
use dtg_execution::{RequestMetricsSink, RequestStageMetrics, encode_request_metrics_snapshot};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::net::UnixListener;
use tokio::time::MissedTickBehavior;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::Server;

const REQUEST_METRICS_PREFIX: &str = "DTG_REQUEST_STAGE_METRICS=";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = DataProcessConfig::from_env()?;
    let rpc_addr = config.rpc_addr();
    let gateway_unix_socket = config.gateway_unix_socket().map(ToOwned::to_owned);
    let node = DataNodeBuilder::from_config(config).start().await?;
    let metrics_sink =
        RequestMetricsSink::stderr().unwrap_or_else(|_| RequestMetricsSink::disabled());
    let metrics_exporter = spawn_metrics_exporter("data", node.request_metrics(), metrics_sink);
    let service = node.rpc_service();
    let shutdown = service.clone();
    let unix_server = if let Some(path) = gateway_unix_socket.as_deref() {
        prepare_gateway_unix_socket(path)?;
        let listener = UnixListener::bind(path)?;
        restrict_gateway_unix_socket_permissions(path)?;
        let service = service.clone();
        Some(tokio::spawn(async move {
            Server::builder()
                .add_service(DataServiceServer::new(service.clone()))
                .add_service(GatewayServiceServer::new(service))
                .serve_with_incoming_shutdown(UnixListenerStream::new(listener), async {
                    let _ = tokio::signal::ctrl_c().await;
                })
                .await
        }))
    } else {
        None
    };

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
    if let Some(server) = unix_server {
        server.abort();
        let _ = server.await;
    }
    if let Some(path) = gateway_unix_socket.as_deref() {
        prepare_gateway_unix_socket(path)?;
    }
    result?;
    node.stop();
    Ok(())
}

#[cfg(unix)]
fn prepare_gateway_unix_socket(path: &Path) -> Result<(), std::io::Error> {
    use std::os::unix::fs::FileTypeExt as _;

    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_socket() => std::fs::remove_file(path),
        Ok(_) => Err(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            format!(
                "gateway unix socket path is not a socket: {}",
                path.display()
            ),
        )),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

#[cfg(unix)]
fn restrict_gateway_unix_socket_permissions(path: &Path) -> Result<(), std::io::Error> {
    use std::os::unix::fs::PermissionsExt as _;

    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
}

#[cfg(not(unix))]
fn restrict_gateway_unix_socket_permissions(_path: &Path) -> Result<(), std::io::Error> {
    Ok(())
}

#[cfg(not(unix))]
fn prepare_gateway_unix_socket(_path: &Path) -> Result<(), std::io::Error> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "gateway unix sockets are unsupported on this platform",
    ))
}

fn spawn_metrics_exporter(
    role: &'static str,
    metrics: Arc<RequestStageMetrics>,
    sink: RequestMetricsSink,
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
                sink.try_write(format!("{REQUEST_METRICS_PREFIX}{encoded}"));
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use std::fs::File;

    use super::*;

    #[tokio::test]
    async fn gateway_unix_socket_preparation_removes_only_a_stale_socket() {
        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("gateway.sock");
        drop(tokio::net::UnixListener::bind(&socket).unwrap());
        prepare_gateway_unix_socket(&socket).unwrap();
        assert!(!socket.exists());

        let regular = directory.path().join("regular");
        File::create(&regular).unwrap();
        assert!(prepare_gateway_unix_socket(&regular).is_err());
        assert!(regular.exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn gateway_unix_socket_permissions_are_owner_only() {
        use std::os::unix::fs::PermissionsExt as _;

        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("gateway.sock");
        let _listener = tokio::net::UnixListener::bind(&socket).unwrap();
        restrict_gateway_unix_socket_permissions(&socket).unwrap();
        assert_eq!(
            std::fs::metadata(socket).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
}
