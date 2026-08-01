use std::io;

const DEFAULT_GATEWAY_WORKER_THREADS: usize = 4;
const MAX_EXPLICIT_GATEWAY_WORKER_THREADS: usize = 64;

pub fn build_gateway_runtime() -> io::Result<tokio::runtime::Runtime> {
    let configured = std::env::var("DTG_GATEWAY_WORKER_THREADS").ok();
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(gateway_worker_threads(configured.as_deref()))
        .enable_all()
        .thread_name("dtg-gateway")
        .build()
}

fn gateway_worker_threads(configured: Option<&str>) -> usize {
    configured
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|threads| (1..=MAX_EXPLICIT_GATEWAY_WORKER_THREADS).contains(threads))
        .unwrap_or(DEFAULT_GATEWAY_WORKER_THREADS)
}

#[cfg(test)]
mod tests {
    use super::{build_gateway_runtime, gateway_worker_threads};
    use tokio::runtime::RuntimeFlavor;

    #[test]
    fn gateway_runtime_uses_the_explicit_worker_override() {
        assert_eq!(gateway_worker_threads(Some("6")), 6);
    }

    #[test]
    fn gateway_runtime_rejects_an_invalid_worker_override() {
        assert_eq!(gateway_worker_threads(None), 4);
        assert_eq!(gateway_worker_threads(Some("zero")), 4);
        assert_eq!(gateway_worker_threads(Some("0")), 4);
    }

    #[test]
    fn gateway_runtime_has_multiple_workers_for_concurrent_sessions() {
        let runtime = build_gateway_runtime().unwrap();
        assert_eq!(
            runtime.handle().runtime_flavor(),
            RuntimeFlavor::MultiThread
        );
        assert!(runtime.metrics().num_workers() >= 2);
    }
}
