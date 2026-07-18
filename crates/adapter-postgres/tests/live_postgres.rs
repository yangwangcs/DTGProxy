use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::time::{SystemTime, UNIX_EPOCH};

use adapter_postgres::{PostgresAdapter, PostgresAdapterFactory};
use adapter_registry::{AdapterFactory, AdapterOpenRequest, AdapterRegistry, SecretString};
use adapter_rocksdb::{RocksAdapter, RocksAdapterFactory};
use postgres::{Client, NoTls};
use storage_api::{
    AdapterRequirement, CommittedMutationBatch, KeySpan, Keyspace, LogicalKey,
    LogicalSnapshotExportRequest, LogicalSnapshotHeaderV1, Mutation, StorageAdapter,
};

#[test]
#[ignore = "requires DTGPROXY_POSTGRES_URL and a disposable PostgreSQL database"]
fn live_postgres_apply_export_restore_and_continue() {
    let url = std::env::var("DTGPROXY_POSTGRES_URL")
        .expect("DTGPROXY_POSTGRES_URL must point to a disposable PostgreSQL database");
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let source_id = format!("live-source-{suffix}");
    let target_id = format!("live-target-{suffix}");
    let source = PostgresAdapter::open(&url, &source_id, 2).unwrap();
    assert!(PostgresAdapter::open(&url, &source_id, 1).is_err());

    let first = batch(1, b"vertex/1", b"payload");
    assert!(
        !block_on(source.apply_committed(first.clone()))
            .unwrap()
            .duplicate
    );
    assert!(
        block_on(source.apply_committed(first.clone()))
            .unwrap()
            .duplicate
    );
    assert_eq!(
        block_on(source.multi_get(&[key(b"vertex/1"), key(b"missing")])).unwrap(),
        vec![Some(b"payload".to_vec()), None]
    );
    assert_eq!(
        block_on(source.scan(&KeySpan::prefix(Keyspace::Current, b"vertex/".to_vec())))
            .unwrap()
            .len(),
        1
    );

    let reader =
        block_on(source.begin_logical_export(LogicalSnapshotExportRequest::new(2, 4096).unwrap()))
            .unwrap();
    let request = AdapterOpenRequest::new(&target_id)
        .with_parameter("pool_size", "2")
        .with_secret("connection_string", SecretString::new(&url));
    let mut registry = AdapterRegistry::new();
    registry.register(Arc::new(PostgresAdapterFactory)).unwrap();
    let target = block_on(registry.restore(
        "postgresql",
        &request,
        AdapterRequirement::HotPluggableReplica,
        reader,
    ))
    .unwrap();
    assert_eq!(target.adapter().applied_log_index().unwrap(), 1);
    assert_eq!(
        block_on(target.adapter().multi_get(&[key(b"vertex/1")])).unwrap(),
        vec![Some(b"payload".to_vec())]
    );
    block_on(
        target
            .adapter()
            .apply_committed(batch(2, b"vertex/2", b"second")),
    )
    .unwrap();
    assert_eq!(target.adapter().applied_log_index().unwrap(), 2);

    drop(target);
    drop(source);
    let reopened = PostgresAdapter::open(&url, &source_id, 1).unwrap();
    assert_eq!(reopened.applied_log_index().unwrap(), 1);
    drop(reopened);
    cleanup(&url, &[&source_id, &target_id]);
}

#[test]
#[ignore = "requires DTGPROXY_POSTGRES_URL and a disposable PostgreSQL database"]
fn live_canonical_snapshots_move_in_both_directions_between_rocksdb_and_postgres() {
    let url = std::env::var("DTGPROXY_POSTGRES_URL")
        .expect("DTGPROXY_POSTGRES_URL must point to a disposable PostgreSQL database");
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let postgres_source_id = format!("cross-pg-source-{suffix}");
    let postgres_target_id = format!("cross-pg-target-{suffix}");
    let root = tempfile::tempdir().unwrap();

    let rocks_source = RocksAdapter::open(root.path().join("rocks-source")).unwrap();
    block_on(rocks_source.apply_committed(batch(1, b"from/rocks", b"rocks"))).unwrap();
    let rocks_reader =
        block_on(rocks_source.begin_logical_export(LogicalSnapshotExportRequest::default()))
            .unwrap();
    let pg_request = AdapterOpenRequest::new(&postgres_target_id)
        .with_secret("connection_string", SecretString::new(&url));
    let mut pg_registry = AdapterRegistry::new();
    pg_registry
        .register(Arc::new(PostgresAdapterFactory))
        .unwrap();
    let pg_target = block_on(pg_registry.restore(
        "postgresql",
        &pg_request,
        AdapterRequirement::HotPluggableReplica,
        rocks_reader,
    ))
    .unwrap();
    assert_eq!(
        block_on(pg_target.adapter().multi_get(&[key(b"from/rocks")])).unwrap(),
        vec![Some(b"rocks".to_vec())]
    );

    let pg_source = PostgresAdapter::open(&url, &postgres_source_id, 2).unwrap();
    block_on(pg_source.apply_committed(batch(1, b"from/postgres", b"postgres"))).unwrap();
    let pg_reader =
        block_on(pg_source.begin_logical_export(LogicalSnapshotExportRequest::default())).unwrap();
    let rocks_target_path = root.path().join("rocks-target");
    let rocks_request = AdapterOpenRequest::new("cross-rocks-target")
        .with_parameter("path", rocks_target_path.to_str().unwrap());
    let mut rocks_registry = AdapterRegistry::new();
    rocks_registry
        .register(Arc::new(RocksAdapterFactory))
        .unwrap();
    let rocks_target = block_on(rocks_registry.restore(
        "rocksdb",
        &rocks_request,
        AdapterRequirement::HotPluggableReplica,
        pg_reader,
    ))
    .unwrap();
    assert_eq!(
        block_on(rocks_target.adapter().multi_get(&[key(b"from/postgres")])).unwrap(),
        vec![Some(b"postgres".to_vec())]
    );

    drop(rocks_target);
    drop(pg_source);
    drop(pg_target);
    drop(rocks_source);
    cleanup(&url, &[&postgres_source_id, &postgres_target_id]);
}

#[test]
#[ignore = "requires DTGPROXY_POSTGRES_URL and a disposable PostgreSQL database"]
fn live_unpublished_restore_residue_is_never_served_and_can_be_reclaimed() {
    let url = std::env::var("DTGPROXY_POSTGRES_URL")
        .expect("DTGPROXY_POSTGRES_URL must point to a disposable PostgreSQL database");
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let bootstrap_id = format!("live-bootstrap-{suffix}");
    let residue_id = format!("live-residue-{suffix}");
    drop(PostgresAdapter::open(&url, &bootstrap_id, 1).unwrap());

    let mut client = Client::connect(&url, NoTls).unwrap();
    client
        .execute(
            "INSERT INTO dtgproxy.adapter_instance(instance_id, schema_version, applied_log_index, published) VALUES ($1, 1, $2, FALSE)",
            &[&residue_id, &0_u64.to_be_bytes().as_slice()],
        )
        .unwrap();
    client
        .execute(
            "INSERT INTO dtgproxy.canonical_kv(instance_id, keyspace, logical_key, value) VALUES ($1, 0, $2, $3)",
            &[&residue_id, &b"partial".as_slice(), &b"must-not-serve".as_slice()],
        )
        .unwrap();
    drop(client);

    assert!(PostgresAdapter::open(&url, &residue_id, 1).is_err());
    let request = AdapterOpenRequest::new(&residue_id)
        .with_secret("connection_string", SecretString::new(&url));
    let factory = PostgresAdapterFactory;
    let restore =
        block_on(factory.begin_restore(&request, LogicalSnapshotHeaderV1::new(7, 0))).unwrap();
    block_on(restore.abort()).unwrap();

    let mut client = Client::connect(&url, NoTls).unwrap();
    assert!(
        client
            .query_opt(
                "SELECT 1 FROM dtgproxy.adapter_instance WHERE instance_id = $1",
                &[&residue_id],
            )
            .unwrap()
            .is_none()
    );
    drop(client);
    cleanup(&url, &[&bootstrap_id]);
}

fn batch(index: u64, value_key: &[u8], value: &[u8]) -> CommittedMutationBatch {
    CommittedMutationBatch {
        shard_id: 1,
        log_index: index,
        txn_id: u128::from(index),
        mutations: vec![Mutation::put(0, key(value_key), value.to_vec())],
    }
}

fn key(value: &[u8]) -> LogicalKey {
    LogicalKey::in_keyspace(Keyspace::Current, value.to_vec())
}

fn cleanup(url: &str, instance_ids: &[&str]) {
    let mut client = Client::connect(url, NoTls).unwrap();
    for instance_id in instance_ids {
        client
            .execute(
                "DELETE FROM dtgproxy.adapter_instance WHERE instance_id = $1",
                &[instance_id],
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
