use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use adapter_postgres::{POSTGRES_SCHEMA, POSTGRES_SCHEMA_VERSION, PostgresAdapterFactory};
use adapter_registry::{AdapterFactory, AdapterOpenRequest};

#[test]
fn schema_is_static_byte_preserving_and_scoped_by_instance() {
    assert_eq!(POSTGRES_SCHEMA_VERSION, 1);
    assert!(POSTGRES_SCHEMA.contains("CREATE SCHEMA IF NOT EXISTS dtgproxy"));
    assert!(POSTGRES_SCHEMA.contains("dtgproxy.schema_meta"));
    assert!(POSTGRES_SCHEMA.contains("ON CONFLICT (singleton) DO NOTHING"));
    assert!(POSTGRES_SCHEMA.contains("instance_id TEXT NOT NULL"));
    for table in [
        "vertex_identity",
        "edge_identity",
        "vertex_current",
        "edge_current",
        "history",
        "out_adjacency",
        "in_adjacency",
        "opaque_records",
        "replay_log",
        "replay_mutation",
    ] {
        assert!(
            POSTGRES_SCHEMA.contains(&format!("dtgproxy.{table}")),
            "native table {table} missing from schema"
        );
    }
    assert!(POSTGRES_SCHEMA.contains("published BOOLEAN NOT NULL"));
    assert!(POSTGRES_SCHEMA.contains("mapping_fingerprint BYTEA NOT NULL"));
    assert!(POSTGRES_SCHEMA.contains("schema_fingerprint BYTEA NOT NULL"));
    assert!(POSTGRES_SCHEMA.contains("has_applied_index_record BOOLEAN NOT NULL"));
    assert!(!POSTGRES_SCHEMA.contains("canonical_kv"));
    assert!(!POSTGRES_SCHEMA.contains("{}"));
}

#[test]
fn factory_name_is_stable_and_connection_string_is_mandatory_secret() {
    let factory = PostgresAdapterFactory;
    assert_eq!(factory.provider_name(), "postgresql");
    let mapping = factory
        .mapping_descriptor()
        .expect("PostgreSQL must declare its native Mapping");
    assert_eq!(mapping.name(), "postgresql-native-temporal");
    assert_eq!(mapping.version(), "1.0.0");
    assert_ne!(mapping.schema_fingerprint(), [0; 32]);
    assert!(mapping.capabilities().native_temporal_layout);
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
