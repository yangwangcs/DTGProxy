use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use adapter_postgres::{
    POSTGRES_CANONICAL_SCAN_SQL, POSTGRES_QUERY_PRIMITIVE_CAPABILITIES, POSTGRES_SCHEMA,
    POSTGRES_SCHEMA_VERSION, POSTGRES_TYPED_ADJACENCY_EXPAND_SQL,
    POSTGRES_TYPED_CANDIDATE_SCAN_SQL, POSTGRES_TYPED_CHANGE_SCAN_SQL,
    POSTGRES_TYPED_PROPERTY_GATHER_SQL, PostgresAdapterFactory,
};
use adapter_registry::{AdapterFactory, AdapterOpenRequest};
use storage_api::PushdownGuarantee;

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
    assert!(!mapping.capabilities().predicate_pushdown);
    assert!(mapping.capabilities().adjacency_pushdown);
    assert!(mapping.capabilities().change_feed);
    let error = match block_on(factory.open(&AdapterOpenRequest::new("pg-1"))) {
        Ok(_) => panic!("factory unexpectedly opened without a connection secret"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("secret connection_string"));
}

#[test]
fn canonical_scan_sql_is_keyspace_scoped_ordered_and_bounded() {
    assert!(POSTGRES_CANONICAL_SCAN_SQL.contains("keyspace = $2"));
    assert!(POSTGRES_CANONICAL_SCAN_SQL.contains("logical_key >= $3"));
    assert!(POSTGRES_CANONICAL_SCAN_SQL.contains("($4::bytea IS NULL OR logical_key < $4)"));
    assert!(POSTGRES_CANONICAL_SCAN_SQL.contains("ORDER BY logical_key"));
    assert!(POSTGRES_CANONICAL_SCAN_SQL.contains("LIMIT $5"));
    assert!(POSTGRES_CANONICAL_SCAN_SQL.contains("dtgproxy.opaque_records"));
    assert_eq!(
        POSTGRES_CANONICAL_SCAN_SQL
            .matches("to_hex(key_tag::integer)")
            .count(),
        2,
    );
    assert!(POSTGRES_CANONICAL_SCAN_SQL.contains("to_hex(element_kind::integer)"));
}

#[test]
fn canonical_scan_sql_returns_stored_canonical_values_without_per_entry_reload() {
    assert!(
        POSTGRES_CANONICAL_SCAN_SQL
            .contains("WITH canonical_keys(keyspace, logical_key, canonical_value) AS")
    );
    assert!(
        POSTGRES_CANONICAL_SCAN_SQL
            .contains("decode('08', 'hex') || graph_id || partition_id || vertex_id, projection")
    );
    assert!(
        POSTGRES_CANONICAL_SCAN_SQL
            .contains("decode('09', 'hex') || graph_id || partition_id || edge_id, projection")
    );
    assert!(
        POSTGRES_CANONICAL_SCAN_SQL.contains(
            "decode(lpad(to_hex(element_kind::integer), 2, '0'), 'hex') || element_id ||"
        )
    );
    assert!(POSTGRES_CANONICAL_SCAN_SQL.contains("segment_id, history_value"));
    assert!(
        POSTGRES_CANONICAL_SCAN_SQL
            .contains("SELECT keyspace, logical_key, value FROM dtgproxy.opaque_records")
    );
    assert!(POSTGRES_CANONICAL_SCAN_SQL.contains("NULL::bytea"));
    assert!(
        POSTGRES_CANONICAL_SCAN_SQL
            .contains("SELECT logical_key, canonical_value FROM canonical_keys")
    );
}

#[test]
fn typed_primitive_sql_is_parameterized_and_native_table_scoped() {
    for statement in [
        POSTGRES_TYPED_CANDIDATE_SCAN_SQL,
        POSTGRES_TYPED_PROPERTY_GATHER_SQL,
        POSTGRES_TYPED_ADJACENCY_EXPAND_SQL,
        POSTGRES_TYPED_CHANGE_SCAN_SQL,
    ] {
        assert!(statement.contains("instance_id = $1"));
        assert!(!statement.contains("format!("));
        assert!(!statement.contains("{instance"));
    }
    assert!(POSTGRES_TYPED_PROPERTY_GATHER_SQL.contains("unnest($2::smallint[], $3::bytea[])"));
    assert!(POSTGRES_TYPED_PROPERTY_GATHER_SQL.contains("WITH ORDINALITY"));
    assert!(POSTGRES_TYPED_PROPERTY_GATHER_SQL.contains("LEFT JOIN"));
    assert!(POSTGRES_TYPED_PROPERTY_GATHER_SQL.contains("input_ordinal"));
    assert!(POSTGRES_TYPED_PROPERTY_GATHER_SQL.contains("retained_bytes <= $4::bigint"));
    assert!(POSTGRES_TYPED_PROPERTY_GATHER_SQL.contains("ORDER BY input_ordinal"));
    assert!(POSTGRES_TYPED_PROPERTY_GATHER_SQL.contains("dtgproxy.vertex_current"));
    assert!(POSTGRES_TYPED_PROPERTY_GATHER_SQL.contains("dtgproxy.edge_current"));
    assert!(POSTGRES_TYPED_PROPERTY_GATHER_SQL.contains("dtgproxy.history"));
    assert!(POSTGRES_TYPED_CANDIDATE_SCAN_SQL.contains("dtgproxy.history"));
    assert!(POSTGRES_TYPED_ADJACENCY_EXPAND_SQL.contains("dtgproxy.out_adjacency"));
    assert!(POSTGRES_TYPED_ADJACENCY_EXPAND_SQL.contains("dtgproxy.in_adjacency"));
    assert!(POSTGRES_TYPED_CHANGE_SCAN_SQL.contains("dtgproxy.opaque_records"));
    assert!(POSTGRES_TYPED_CHANGE_SCAN_SQL.contains("keyspace = $2"));
    assert!(POSTGRES_TYPED_CHANGE_SCAN_SQL.contains("logical_key >= $3"));
    assert!(POSTGRES_TYPED_CHANGE_SCAN_SQL.contains("($4::bytea IS NULL OR logical_key < $4)"));
    assert!(POSTGRES_TYPED_CHANGE_SCAN_SQL.contains("ORDER BY logical_key"));
    assert!(POSTGRES_TYPED_CHANGE_SCAN_SQL.contains("LIMIT $5"));
    assert!(!POSTGRES_TYPED_CHANGE_SCAN_SQL.contains("dtgproxy.history"));
}

#[test]
fn typed_primitive_guarantees_match_native_keyspace_coverage() {
    let primitives = POSTGRES_QUERY_PRIMITIVE_CAPABILITIES;
    assert_eq!(primitives.candidate_scan(), PushdownGuarantee::Candidate);
    assert_eq!(primitives.property_gather(), PushdownGuarantee::Candidate);
    assert_eq!(primitives.adjacency_expand(), PushdownGuarantee::Exact);
    assert_eq!(primitives.change_scan(), PushdownGuarantee::Exact);
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
