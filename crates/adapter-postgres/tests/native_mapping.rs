use std::collections::BTreeMap;
use std::future::Future;
use std::time::{SystemTime, UNIX_EPOCH};

use adapter_postgres::PostgresAdapter;
use postgres::{Client, NoTls};
use storage_api::{KeySpan, Keyspace, StorageAdapter};
use temporal_storage::{
    CommitContext, EdgeMutation, EdgeTypeId, ElementId, ElementRef, GraphId, LabelId, PartitionId,
    TemporalStore, VertexMutation, current_vertex_key,
};
use temporal_types::{CanonicalElement, GraphValue, Interval, TransactionTime, ValidTime};

#[test]
fn live_vertex_commit_uses_native_tables_and_reconstructs_canonical_current() {
    let url = std::env::var("DTGPROXY_POSTGRES_URL")
        .expect("DTGPROXY_POSTGRES_URL must point to a disposable PostgreSQL database");
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let instance_id = format!("native-vertex-{suffix}");
    let adapter = PostgresAdapter::open(&url, &instance_id, 2).unwrap();
    let store = TemporalStore::new(adapter);
    let vertex = ElementRef::vertex(GraphId::new(7), PartitionId::new(3), ElementId::new(11));
    let payload = CanonicalElement::new(
        1,
        BTreeMap::from([(1, GraphValue::String("alice".to_owned()))]),
    );
    let valid = Interval::new(ValidTime::from_micros(1), None).unwrap();
    let mutation = VertexMutation::put(vertex, LabelId::new(9), valid, payload.clone()).unwrap();

    block_on(store.commit_vertex(
        CommitContext::new(
            1,
            1,
            100,
            TransactionTime::new(10, 0),
            TransactionTime::new(20, 0),
        ),
        mutation,
    ))
    .unwrap();

    assert_eq!(
        block_on(store.vertex_current(vertex, ValidTime::from_micros(5))).unwrap(),
        Some(payload)
    );
    let current = block_on(store.adapter().multi_get(&[current_vertex_key(vertex)])).unwrap();
    assert!(current[0].is_some());
    assert_eq!(
        block_on(
            store
                .adapter()
                .scan(&KeySpan::prefix(Keyspace::Current, vec![0x08],))
        )
        .unwrap()
        .len(),
        1
    );

    let mut client = Client::connect(&url, NoTls).unwrap();
    for table in ["vertex_identity", "vertex_current", "history"] {
        let count: i64 = client
            .query_one(
                &format!("SELECT count(*) FROM dtgproxy.{table} WHERE instance_id = $1"),
                &[&instance_id],
            )
            .unwrap()
            .get(0);
        assert_eq!(count, 1, "expected one native row in {table}");
    }
    assert!(
        client
            .query_opt(
                "SELECT 1 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'dtgproxy' AND c.relname = 'canonical_kv'",
                &[],
            )
            .unwrap()
            .is_none()
    );
    drop(store);
    client
        .execute(
            "DELETE FROM dtgproxy.adapter_instance WHERE instance_id = $1",
            &[&instance_id],
        )
        .unwrap();
}

#[test]
fn live_cross_partition_edge_uses_native_history_and_symmetric_adjacency() {
    let url = std::env::var("DTGPROXY_POSTGRES_URL")
        .expect("DTGPROXY_POSTGRES_URL must point to a disposable PostgreSQL database");
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let instance_id = format!("native-edge-{suffix}");
    let adapter = PostgresAdapter::open(&url, &instance_id, 2).unwrap();
    let store = TemporalStore::new(adapter);
    let source = ElementRef::vertex(GraphId::new(8), PartitionId::new(3), ElementId::new(1));
    let destination = ElementRef::vertex(GraphId::new(8), PartitionId::new(9), ElementId::new(2));
    let edge = ElementRef::edge(GraphId::new(8), PartitionId::new(3), ElementId::new(10));
    let valid = Interval::new(ValidTime::from_micros(1), None).unwrap();
    let vertex_payload = CanonicalElement::new(
        1,
        BTreeMap::from([(1, GraphValue::String("source".to_owned()))]),
    );
    block_on(store.commit_vertex(
        CommitContext::new(
            1,
            1,
            201,
            TransactionTime::new(10, 0),
            TransactionTime::new(20, 0),
        ),
        VertexMutation::put(source, LabelId::new(1), valid, vertex_payload).unwrap(),
    ))
    .unwrap();

    let edge_payload = CanonicalElement::new(
        1,
        BTreeMap::from([(2, GraphValue::String("cross".to_owned()))]),
    );
    block_on(
        store.commit_edge(
            CommitContext::new(
                1,
                2,
                202,
                TransactionTime::new(20, 0),
                TransactionTime::new(30, 0),
            ),
            EdgeMutation::put_between(
                edge,
                EdgeTypeId::new(4),
                source,
                destination,
                valid,
                edge_payload.clone(),
            )
            .unwrap(),
        ),
    )
    .unwrap();

    assert_eq!(
        block_on(store.edge_current(edge, ValidTime::from_micros(5))).unwrap(),
        Some(edge_payload)
    );
    assert_eq!(
        block_on(
            store
                .adapter()
                .scan(&KeySpan::prefix(Keyspace::AdjOut, Vec::new()))
        )
        .unwrap()
        .len(),
        1
    );
    assert_eq!(
        block_on(
            store
                .adapter()
                .scan(&KeySpan::prefix(Keyspace::AdjIn, Vec::new()))
        )
        .unwrap()
        .len(),
        1
    );

    block_on(store.commit_edge(
        CommitContext::new(
            1,
            3,
            203,
            TransactionTime::new(30, 0),
            TransactionTime::new(40, 0),
        ),
        EdgeMutation::delete_between(edge, EdgeTypeId::new(4), source, destination, valid).unwrap(),
    ))
    .unwrap();
    assert_eq!(
        block_on(store.edge_current(edge, ValidTime::from_micros(5))).unwrap(),
        None
    );
    assert!(
        block_on(
            store
                .adapter()
                .scan(&KeySpan::prefix(Keyspace::AdjOut, Vec::new()))
        )
        .unwrap()
        .is_empty()
    );
    assert!(
        block_on(
            store
                .adapter()
                .scan(&KeySpan::prefix(Keyspace::AdjIn, Vec::new()))
        )
        .unwrap()
        .is_empty()
    );

    let mut client = Client::connect(&url, NoTls).unwrap();
    let history_count: i64 = client
        .query_one(
            "SELECT count(*) FROM dtgproxy.history WHERE instance_id = $1 AND element_kind = 2",
            &[&instance_id],
        )
        .unwrap()
        .get(0);
    assert_eq!(history_count, 2);
    let edge_count: i64 = client
        .query_one(
            "SELECT count(*) FROM dtgproxy.edge_identity WHERE instance_id = $1",
            &[&instance_id],
        )
        .unwrap()
        .get(0);
    assert_eq!(edge_count, 1);
    drop(store);
    client
        .execute(
            "DELETE FROM dtgproxy.adapter_instance WHERE instance_id = $1",
            &[&instance_id],
        )
        .unwrap();
}

fn block_on<F: Future>(future: F) -> F::Output {
    use std::sync::Arc;
    use std::task::{Context, Poll, Wake, Waker};

    struct NoopWake;
    impl Wake for NoopWake {
        fn wake(self: Arc<Self>) {}
    }

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
