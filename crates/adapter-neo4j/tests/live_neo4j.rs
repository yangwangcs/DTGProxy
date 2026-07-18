use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::time::{SystemTime, UNIX_EPOCH};

use adapter_neo4j::Neo4jAdapterFactory;
use adapter_registry::{AdapterOpenRequest, AdapterRegistry, SecretString};
use storage_api::{
    AdapterRequirement, CommittedMutationBatch, KeySpan, Keyspace, LogicalKey,
    LogicalSnapshotExportRequest, Mutation,
};

#[test]
#[ignore = "requires DTGPROXY_NEO4J_ENDPOINT, DTGPROXY_NEO4J_PASSWORD and a disposable Neo4j database"]
fn live_neo4j_apply_query_export_restore_and_continue() {
    let endpoint = std::env::var("DTGPROXY_NEO4J_ENDPOINT").unwrap();
    let password = std::env::var("DTGPROXY_NEO4J_PASSWORD").unwrap();
    let username = std::env::var("DTGPROXY_NEO4J_USERNAME").unwrap_or_else(|_| "neo4j".into());
    let database = std::env::var("DTGPROXY_NEO4J_DATABASE").unwrap_or_else(|_| "neo4j".into());
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let source_request = request(
        format!("live-source-{suffix}"),
        &endpoint,
        &database,
        &username,
        &password,
    );
    let target_request = request(
        format!("live-target-{suffix}"),
        &endpoint,
        &database,
        &username,
        &password,
    );
    let mut registry = AdapterRegistry::new();
    registry.register(Arc::new(Neo4jAdapterFactory)).unwrap();
    let source = block_on(registry.open(
        "neo4j",
        &source_request,
        AdapterRequirement::HotPluggableReplica,
    ))
    .unwrap();
    block_on(
        source
            .adapter()
            .apply_committed(batch(1, b"vertex/1", b"payload")),
    )
    .unwrap();
    assert_eq!(
        block_on(source.adapter().multi_get(&[key(b"vertex/1")])).unwrap(),
        vec![Some(b"payload".to_vec())]
    );
    assert_eq!(
        block_on(
            source
                .adapter()
                .scan(&KeySpan::prefix(Keyspace::Current, b"vertex/".to_vec()))
        )
        .unwrap()
        .len(),
        1
    );

    let reader = block_on(
        source
            .adapter()
            .begin_logical_export(LogicalSnapshotExportRequest::default()),
    )
    .unwrap();
    let target = block_on(registry.restore(
        "neo4j",
        &target_request,
        AdapterRequirement::HotPluggableReplica,
        reader,
    ))
    .unwrap();
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
}

fn request(
    instance_id: String,
    endpoint: &str,
    database: &str,
    username: &str,
    password: &str,
) -> AdapterOpenRequest {
    AdapterOpenRequest::new(instance_id)
        .with_parameter("endpoint", endpoint)
        .with_parameter("database", database)
        .with_parameter("username", username)
        .with_secret("password", SecretString::new(password))
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
