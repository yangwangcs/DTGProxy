#![forbid(unsafe_code)]

use std::net::SocketAddr;
use std::sync::Arc;

use adapter_postgres::{DEFAULT_POOL_SIZE, PostgresAdapter, PostgresAdapterFactory};
use adapter_registry::AdapterRegistry;
use adapter_sidecar::{
    SidecarRestoreBackend, SidecarService, TcpSidecarServerConfig,
    spawn_stateful_tcp_sidecar_server,
};
use storage_api::{AdapterRequirement, StorageAdapter};

fn main() {
    if let Err(error) = run() {
        eprintln!("dtgproxy-postgres-sidecar: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let connection_string = std::env::var("DTGPROXY_POSTGRES_URL")
        .map_err(|_| "DTGPROXY_POSTGRES_URL is required".to_owned())?;
    let instance_id = std::env::var("DTGPROXY_INSTANCE_ID")
        .map_err(|_| "DTGPROXY_INSTANCE_ID is required".to_owned())?;
    let listen: SocketAddr = std::env::var("DTGPROXY_LISTEN")
        .unwrap_or_else(|_| "127.0.0.1:9711".to_owned())
        .parse()
        .map_err(|_| "DTGPROXY_LISTEN must be a socket address".to_owned())?;
    if !listen.ip().is_loopback() {
        return Err(
            "the unauthenticated TCP v1 Sidecar may listen only on a loopback address".to_owned(),
        );
    }
    let pool_size = std::env::var("DTGPROXY_POSTGRES_POOL_SIZE")
        .ok()
        .map(|value| value.parse::<usize>())
        .transpose()
        .map_err(|_| "DTGPROXY_POSTGRES_POOL_SIZE must be an integer".to_owned())?
        .unwrap_or(DEFAULT_POOL_SIZE);
    let adapter = PostgresAdapter::open(&connection_string, instance_id, pool_size)
        .map_err(|error| error.to_string())?;
    adapter
        .descriptor()
        .validate(AdapterRequirement::HotPluggableReplica)
        .map_err(|error| error.to_string())?;
    let adapter: Arc<dyn StorageAdapter> = Arc::new(adapter);
    let descriptor = adapter.descriptor();
    let mut registry = AdapterRegistry::new();
    registry
        .register(Arc::new(PostgresAdapterFactory))
        .map_err(|error| error.to_string())?;
    let restore = SidecarRestoreBackend::new(
        Arc::new(registry),
        "postgresql",
        AdapterRequirement::HotPluggableReplica,
        descriptor,
    )
    .with_secret("connection_string", connection_string);
    let service = Arc::new(SidecarService::new(adapter, Some(restore)));
    let server = spawn_stateful_tcp_sidecar_server(TcpSidecarServerConfig::new(listen), service)
        .map_err(|error| error.to_string())?;
    eprintln!("dtgproxy-postgres-sidecar ready on {}", server.local_addr());
    loop {
        std::thread::park();
    }
}
