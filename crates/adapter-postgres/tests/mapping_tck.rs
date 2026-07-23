use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::time::{SystemTime, UNIX_EPOCH};

use adapter_postgres::PostgresAdapter;
use postgres::{Client, NoTls};
use storage_api::{
    CommittedMutationBatch, Keyspace, LogicalKey, MappingRequirement, Mutation,
    TemporalBackendMapping, run_mapping_restore_tck, run_mapping_tck,
};
use temporal_storage::run_temporal_graph_mapping_tck;

#[test]
fn live_postgres_mapping_has_no_visibility_before_commit() {
    let url = std::env::var("DTGPROXY_POSTGRES_URL")
        .expect("DTGPROXY_POSTGRES_URL must point to a disposable PostgreSQL database");
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let instance_id = format!("mapping-lifecycle-{suffix}");
    let mapping = PostgresAdapter::open(&url, &instance_id, 2).unwrap();
    let key = LogicalKey::in_keyspace(Keyspace::TemporalIndex, b"staged".to_vec());
    let batch = CommittedMutationBatch {
        shard_id: 1,
        log_index: 1,
        txn_id: 1,
        mutations: vec![Mutation::put(0, key.clone(), b"value".to_vec())],
    };

    let mut aborted = block_on(mapping.prepare(batch.clone())).unwrap();
    block_on(aborted.apply()).unwrap();
    assert_eq!(
        block_on(TemporalBackendMapping::multi_get(
            &mapping,
            std::slice::from_ref(&key),
        ))
        .unwrap(),
        vec![None]
    );
    assert_eq!(
        TemporalBackendMapping::applied_log_index(&mapping).unwrap(),
        0
    );
    block_on(aborted.abort()).unwrap();

    let mut committed = block_on(mapping.prepare(batch)).unwrap();
    block_on(committed.apply()).unwrap();
    block_on(committed.commit()).unwrap();
    assert_eq!(
        block_on(TemporalBackendMapping::multi_get(&mapping, &[key])).unwrap(),
        vec![Some(b"value".to_vec())]
    );
    drop(committed);
    drop(mapping);
    let mut client = Client::connect(&url, NoTls).unwrap();
    client
        .execute(
            "DELETE FROM dtgproxy.adapter_instance WHERE instance_id = $1",
            &[&instance_id],
        )
        .unwrap();
}

#[test]
fn live_postgres_passes_shared_mapping_tck_and_restore() {
    let url = std::env::var("DTGPROXY_POSTGRES_URL")
        .expect("DTGPROXY_POSTGRES_URL must point to a disposable PostgreSQL database");
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let source_id = format!("mapping-tck-source-{suffix}");
    let destination_id = format!("mapping-tck-destination-{suffix}");
    let source = PostgresAdapter::open(&url, &source_id, 2).unwrap();
    source
        .describe_schema()
        .validate(MappingRequirement::HotPluggableReplica)
        .unwrap();
    source.validate_mapping().unwrap();
    let destination = PostgresAdapter::open_restore_target(&url, &destination_id, 2).unwrap();

    run_mapping_tck(&source);
    run_mapping_restore_tck(&source, &destination);

    drop(destination);
    drop(source);
    let mut client = Client::connect(&url, NoTls).unwrap();
    for instance_id in [&source_id, &destination_id] {
        client
            .execute(
                "DELETE FROM dtgproxy.adapter_instance WHERE instance_id = $1",
                &[&instance_id],
            )
            .unwrap();
    }
}

#[test]
fn live_postgres_passes_temporal_graph_mapping_tck() {
    let url = std::env::var("DTGPROXY_POSTGRES_URL")
        .expect("DTGPROXY_POSTGRES_URL must point to a disposable PostgreSQL database");
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let source_id = format!("graph-tck-source-{suffix}");
    let destination_id = format!("graph-tck-destination-{suffix}");
    let source: Arc<dyn TemporalBackendMapping> =
        Arc::new(PostgresAdapter::open(&url, &source_id, 2).unwrap());
    let destination: Arc<dyn TemporalBackendMapping> =
        Arc::new(PostgresAdapter::open_restore_target(&url, &destination_id, 2).unwrap());

    run_temporal_graph_mapping_tck(source, destination);

    let mut client = Client::connect(&url, NoTls).unwrap();
    for instance_id in [&source_id, &destination_id] {
        client
            .execute(
                "DELETE FROM dtgproxy.adapter_instance WHERE instance_id = $1",
                &[&instance_id],
            )
            .unwrap();
    }
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
