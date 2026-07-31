use std::io;

pub fn build_gateway_runtime() -> io::Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .thread_name("dtg-gateway")
        .build()
}

#[cfg(test)]
mod tests {
    use super::build_gateway_runtime;
    use std::sync::Arc;
    use std::time::{Duration, Instant};
    use tokio::runtime::RuntimeFlavor;
    use tokio::sync::Barrier;

    #[test]
    fn gateway_runtime_runs_blocking_session_tasks_in_parallel() {
        let runtime = build_gateway_runtime().unwrap();
        assert_eq!(
            runtime.handle().runtime_flavor(),
            RuntimeFlavor::MultiThread
        );

        let elapsed = runtime.block_on(async {
            let barrier = Arc::new(Barrier::new(3));
            let mut tasks = Vec::new();
            for _ in 0..2 {
                let barrier = Arc::clone(&barrier);
                tasks.push(tokio::spawn(async move {
                    barrier.wait().await;
                    std::thread::sleep(Duration::from_millis(100));
                }));
            }
            barrier.wait().await;
            let started = Instant::now();
            for task in tasks {
                task.await.unwrap();
            }
            started.elapsed()
        });

        assert!(elapsed < Duration::from_millis(175), "elapsed: {elapsed:?}");
    }
}
