#![forbid(unsafe_code)]

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use adapter_neo4j::Neo4jAdapterFactory;
use adapter_registry::{AdapterOpenRequest, AdapterRegistry, SecretString};
use adapter_sidecar::{
    SidecarRestoreBackend, SidecarService, TcpSidecarServerConfig,
    spawn_stateful_tcp_sidecar_server,
};
use storage_api::AdapterRequirement;

fn main() {
    if let Err(error) = run() {
        eprintln!("dtgproxy-neo4j-sidecar: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let endpoint = required("DTGPROXY_NEO4J_ENDPOINT")?;
    let password = required("DTGPROXY_NEO4J_PASSWORD")?;
    let instance_id = required("DTGPROXY_INSTANCE_ID")?;
    let username = std::env::var("DTGPROXY_NEO4J_USERNAME").unwrap_or_else(|_| "neo4j".into());
    let database = std::env::var("DTGPROXY_NEO4J_DATABASE").unwrap_or_else(|_| "neo4j".into());
    let listen: SocketAddr = std::env::var("DTGPROXY_LISTEN")
        .unwrap_or_else(|_| "127.0.0.1:9712".into())
        .parse()
        .map_err(|_| "DTGPROXY_LISTEN must be a socket address".to_owned())?;
    if !listen.ip().is_loopback() {
        return Err(
            "the unauthenticated TCP v1 Sidecar may listen only on a loopback address".into(),
        );
    }

    let mut registry = AdapterRegistry::new();
    registry
        .register(Arc::new(Neo4jAdapterFactory))
        .map_err(|error| error.to_string())?;
    let registry = Arc::new(registry);
    let request = AdapterOpenRequest::new(&instance_id)
        .with_parameter("endpoint", &endpoint)
        .with_parameter("database", &database)
        .with_parameter("username", &username)
        .with_secret("password", SecretString::new(&password));
    let opened =
        block_on(registry.open("neo4j", &request, AdapterRequirement::HotPluggableReplica))
            .map_err(|error| error.to_string())?;
    let descriptor = opened.descriptor().clone();
    let active = opened.into_adapter();
    let restore = SidecarRestoreBackend::new(
        Arc::clone(&registry),
        "neo4j",
        AdapterRequirement::HotPluggableReplica,
        descriptor,
    )
    .with_secret("password", password);
    let service = Arc::new(SidecarService::new(active, Some(restore)));
    let server = spawn_stateful_tcp_sidecar_server(TcpSidecarServerConfig::new(listen), service)
        .map_err(|error| error.to_string())?;
    eprintln!("dtgproxy-neo4j-sidecar ready on {}", server.local_addr());
    loop {
        std::thread::park();
    }
}

fn required(name: &str) -> Result<String, String> {
    std::env::var(name).map_err(|_| format!("{name} is required"))
}

struct NoopWake;

impl Wake for NoopWake {
    fn wake(self: Arc<Self>) {}
}

fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::from(Arc::new(NoopWake));
    let mut context = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}
