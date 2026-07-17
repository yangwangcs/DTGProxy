use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use adapter_postgres::{POSTGRES_SCHEMA_V1, POSTGRES_SCHEMA_VERSION, PostgresAdapterFactory};
use adapter_registry::{AdapterFactory, AdapterOpenRequest};

#[test]
fn schema_is_static_byte_preserving_and_scoped_by_instance() {
    assert_eq!(POSTGRES_SCHEMA_VERSION, 1);
    assert!(POSTGRES_SCHEMA_V1.contains("CREATE SCHEMA IF NOT EXISTS dtgproxy"));
    assert!(POSTGRES_SCHEMA_V1.contains("dtgproxy.schema_meta"));
    assert!(POSTGRES_SCHEMA_V1.contains("ON CONFLICT (singleton) DO NOTHING"));
    assert!(POSTGRES_SCHEMA_V1.contains("instance_id TEXT NOT NULL"));
    assert!(POSTGRES_SCHEMA_V1.contains("logical_key BYTEA NOT NULL"));
    assert!(POSTGRES_SCHEMA_V1.contains("value BYTEA NOT NULL"));
    assert!(POSTGRES_SCHEMA_V1.contains("published BOOLEAN NOT NULL"));
    assert!(POSTGRES_SCHEMA_V1.contains("PRIMARY KEY (instance_id, keyspace, logical_key)"));
    assert!(POSTGRES_SCHEMA_V1.contains("keyspace BETWEEN 0 AND 7"));
    assert!(!POSTGRES_SCHEMA_V1.contains("{}"));
}

#[test]
fn factory_name_is_stable_and_connection_string_is_mandatory_secret() {
    let factory = PostgresAdapterFactory;
    assert_eq!(factory.provider_name(), "postgresql");
    let error = match block_on(factory.open(&AdapterOpenRequest::new("pg-1"))) {
        Ok(_) => panic!("factory unexpectedly opened without a connection secret"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("secret connection_string"));
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
