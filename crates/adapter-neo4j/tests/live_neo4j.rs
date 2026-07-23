use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::time::{SystemTime, UNIX_EPOCH};

use adapter_neo4j::Neo4jAdapterFactory;
use adapter_registry::{AdapterOpenRequest, AdapterRegistry, SecretString};
use base64::Engine;
use storage_api::{
    AdapterError, AdapterRequirement, CommittedMutationBatch, KeySpan, Keyspace, LogicalKey,
    LogicalSnapshotExportRequest, Mutation,
};
use temporal_storage::{
    CommitContext, EdgeMutation, EdgeTypeId, ElementId, ElementRef, GraphId, LabelId, PartitionId,
    TemporalStore, VertexMutation,
};
use temporal_types::{CanonicalElement, GraphValue, Interval, TransactionTime, ValidTime};

#[test]
fn live_neo4j_apply_query_export_restore_and_continue() {
    let endpoint = std::env::var("DTGPROXY_NEO4J_ENDPOINT")
        .expect("DTGPROXY_NEO4J_ENDPOINT must point to a disposable Neo4j database");
    let password = std::env::var("DTGPROXY_NEO4J_PASSWORD")
        .expect("DTGPROXY_NEO4J_PASSWORD must authenticate to the disposable Neo4j database");
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
        block_on(source.adapter().scan(&KeySpan::prefix(
            Keyspace::TemporalIndex,
            b"vertex/".to_vec(),
        )))
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

#[test]
fn live_neo4j_materializes_native_temporal_nodes_and_relationships() {
    let endpoint = std::env::var("DTGPROXY_NEO4J_ENDPOINT")
        .expect("DTGPROXY_NEO4J_ENDPOINT must point to a disposable Neo4j database");
    let password = std::env::var("DTGPROXY_NEO4J_PASSWORD")
        .expect("DTGPROXY_NEO4J_PASSWORD must authenticate to the disposable Neo4j database");
    let username = std::env::var("DTGPROXY_NEO4J_USERNAME").unwrap_or_else(|_| "neo4j".into());
    let database = std::env::var("DTGPROXY_NEO4J_DATABASE").unwrap_or_else(|_| "neo4j".into());
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let instance_id = format!("native-temporal-{suffix}");
    let mut registry = AdapterRegistry::new();
    registry.register(Arc::new(Neo4jAdapterFactory)).unwrap();
    let opened = block_on(registry.open(
        "neo4j",
        &request(
            instance_id.clone(),
            &endpoint,
            &database,
            &username,
            &password,
        ),
        AdapterRequirement::HotPluggableReplica,
    ))
    .unwrap();
    let store = TemporalStore::new(opened.into_adapter());
    let graph = GraphId::new(9);
    let source = ElementRef::vertex(graph, PartitionId::new(1), ElementId::new(10));
    let destination = ElementRef::vertex(graph, PartitionId::new(2), ElementId::new(20));
    let edge = ElementRef::edge(graph, PartitionId::new(1), ElementId::new(30));
    let valid = Interval::new(ValidTime::from_micros(1), None).unwrap();
    let vertex_payload = CanonicalElement::new(
        1,
        BTreeMap::from([(1, GraphValue::String("vertex".into()))]),
    );
    block_on(store.commit_vertex(
        CommitContext::new(
            1,
            1,
            501,
            TransactionTime::new(10, 0),
            TransactionTime::new(20, 0),
        ),
        VertexMutation::put(source, LabelId::new(3), valid, vertex_payload).unwrap(),
    ))
    .unwrap();
    let edge_payload =
        CanonicalElement::new(1, BTreeMap::from([(2, GraphValue::String("edge".into()))]));
    block_on(
        store.commit_edge(
            CommitContext::new(
                1,
                2,
                502,
                TransactionTime::new(20, 0),
                TransactionTime::new(30, 0),
            ),
            EdgeMutation::put_between(
                edge,
                EdgeTypeId::new(4),
                source,
                destination,
                valid,
                edge_payload,
            )
            .unwrap(),
        ),
    )
    .unwrap();

    let rows = query_rows(
        &endpoint,
        &database,
        &username,
        &password,
        "MATCH (vertex:DTGVertexCurrent {instance_id: $instance_id}) MATCH (:DTGVertexEndpoint)-[edge:DTG_EDGE {instance_id: $instance_id}]->(:DTGVertexEndpoint) MATCH (history:DTGEdgeHistory {instance_id: $instance_id}) RETURN count(DISTINCT vertex), count(DISTINCT edge), count(DISTINCT history)",
        serde_json::json!({"instance_id": instance_id}),
    );
    assert_eq!(
        rows,
        vec![vec![
            serde_json::json!(1),
            serde_json::json!(1),
            serde_json::json!(1)
        ]]
    );
}

#[test]
fn live_neo4j_edge_identity_delete_hides_native_relationship() {
    let endpoint = std::env::var("DTGPROXY_NEO4J_ENDPOINT")
        .expect("DTGPROXY_NEO4J_ENDPOINT must point to a disposable Neo4j database");
    let password = std::env::var("DTGPROXY_NEO4J_PASSWORD")
        .expect("DTGPROXY_NEO4J_PASSWORD must authenticate to the disposable Neo4j database");
    let username = std::env::var("DTGPROXY_NEO4J_USERNAME").unwrap_or_else(|_| "neo4j".into());
    let database = std::env::var("DTGPROXY_NEO4J_DATABASE").unwrap_or_else(|_| "neo4j".into());
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let instance_id = format!("native-edge-delete-{suffix}");
    let mut registry = AdapterRegistry::new();
    registry.register(Arc::new(Neo4jAdapterFactory)).unwrap();
    let opened = block_on(registry.open(
        "neo4j",
        &request(
            instance_id.clone(),
            &endpoint,
            &database,
            &username,
            &password,
        ),
        AdapterRequirement::HotPluggableReplica,
    ))
    .unwrap();

    let graph = GraphId::new(19);
    let source = ElementRef::vertex(graph, PartitionId::new(1), ElementId::new(10));
    let destination = ElementRef::vertex(graph, PartitionId::new(2), ElementId::new(20));
    let edge = ElementRef::edge(graph, PartitionId::new(1), ElementId::new(30));
    let identity =
        temporal_storage::EdgeIdentity::new_between(edge, EdgeTypeId::new(4), source, destination)
            .unwrap();
    let identity_key = temporal_storage::edge_identity_key(edge);
    block_on(opened.adapter().apply_committed(CommittedMutationBatch {
        shard_id: 1,
        log_index: 1,
        txn_id: 701,
        mutations: vec![Mutation::put(0, identity_key.clone(), identity.encode())],
    }))
    .unwrap();
    assert_eq!(
        native_present_edge_count(&endpoint, &database, &username, &password, &instance_id,),
        1
    );

    block_on(opened.adapter().apply_committed(CommittedMutationBatch {
        shard_id: 1,
        log_index: 2,
        txn_id: 702,
        mutations: vec![Mutation::delete(0, identity_key)],
    }))
    .unwrap();
    assert_eq!(
        native_present_edge_count(&endpoint, &database, &username, &password, &instance_id,),
        0
    );
}

#[test]
fn live_neo4j_commit_atomically_rejects_divergent_mutation_replay() {
    let endpoint = std::env::var("DTGPROXY_NEO4J_ENDPOINT")
        .expect("DTGPROXY_NEO4J_ENDPOINT must point to a disposable Neo4j database");
    let password = std::env::var("DTGPROXY_NEO4J_PASSWORD")
        .expect("DTGPROXY_NEO4J_PASSWORD must authenticate to the disposable Neo4j database");
    let username = std::env::var("DTGPROXY_NEO4J_USERNAME").unwrap_or_else(|_| "neo4j".into());
    let database = std::env::var("DTGPROXY_NEO4J_DATABASE").unwrap_or_else(|_| "neo4j".into());
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let instance_id = format!("atomic-replay-{suffix}");
    let mapping = Neo4jAdapterFactory::open_mapping(
        endpoint.clone(),
        database.clone(),
        username.clone(),
        password.clone(),
        &instance_id,
    )
    .unwrap();
    let mutation_key = key(b"atomic-replay");
    let batch = CommittedMutationBatch {
        shard_id: 1,
        log_index: 1,
        txn_id: 801,
        mutations: vec![Mutation::put(0, mutation_key.clone(), b"expected".to_vec())],
    };
    let mut prepared = block_on(mapping.prepare(batch)).unwrap();

    query_rows(
        &endpoint,
        &database,
        &username,
        &password,
        "CREATE (:DTGProxyMutation {instance_id: $instance_id, txn_id_hex: $txn_id_hex, sequence: 0, fingerprint_hex: $fingerprint_hex}) RETURN 1",
        serde_json::json!({
            "instance_id": instance_id,
            "txn_id_hex": format!("{:032x}", 801_u128),
            "fingerprint_hex": "ffffffffffffffff",
        }),
    );

    block_on(prepared.apply()).unwrap();
    assert!(matches!(
        block_on(prepared.commit()),
        Err(AdapterError::MutationReplayMismatch {
            txn_id: 801,
            sequence: 0,
        })
    ));
    assert_eq!(mapping.applied_log_index().unwrap(), 0);
    assert_eq!(
        block_on(mapping.multi_get(&[mutation_key])).unwrap(),
        vec![None]
    );
}

#[test]
fn live_neo4j_commits_and_replays_an_empty_batch() {
    let endpoint = std::env::var("DTGPROXY_NEO4J_ENDPOINT")
        .expect("DTGPROXY_NEO4J_ENDPOINT must point to a disposable Neo4j database");
    let password = std::env::var("DTGPROXY_NEO4J_PASSWORD")
        .expect("DTGPROXY_NEO4J_PASSWORD must authenticate to the disposable Neo4j database");
    let username = std::env::var("DTGPROXY_NEO4J_USERNAME").unwrap_or_else(|_| "neo4j".into());
    let database = std::env::var("DTGPROXY_NEO4J_DATABASE").unwrap_or_else(|_| "neo4j".into());
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let mapping = Neo4jAdapterFactory::open_mapping(
        &endpoint,
        &database,
        &username,
        &password,
        &format!("empty-batch-{suffix}"),
    )
    .unwrap();
    let batch = CommittedMutationBatch {
        shard_id: 1,
        log_index: 1,
        txn_id: 901,
        mutations: Vec::new(),
    };

    let first = commit_mapping_batch(mapping.as_ref(), batch.clone()).unwrap();
    assert_eq!(first.applied_log_index, 1);
    assert!(!first.duplicate);
    let replay = commit_mapping_batch(mapping.as_ref(), batch).unwrap();
    assert_eq!(replay.applied_log_index, 1);
    assert!(replay.duplicate);
}

fn commit_mapping_batch(
    mapping: &dyn storage_api::TemporalBackendMapping,
    batch: CommittedMutationBatch,
) -> Result<storage_api::ApplyReceipt, AdapterError> {
    let mut prepared = block_on(mapping.prepare(batch))?;
    block_on(prepared.apply())?;
    block_on(prepared.commit())
}

fn native_present_edge_count(
    endpoint: &str,
    database: &str,
    username: &str,
    password: &str,
    instance_id: &str,
) -> u64 {
    query_rows(
        endpoint,
        database,
        username,
        password,
        "MATCH ()-[edge:DTG_EDGE {instance_id: $instance_id, present: true}]->() RETURN count(edge)",
        serde_json::json!({"instance_id": instance_id}),
    )[0][0]
        .as_u64()
        .unwrap()
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
    LogicalKey::in_keyspace(Keyspace::TemporalIndex, value.to_vec())
}

fn query_rows(
    endpoint: &str,
    database: &str,
    username: &str,
    password: &str,
    statement: &str,
    parameters: serde_json::Value,
) -> Vec<Vec<serde_json::Value>> {
    let response: serde_json::Value = ureq::post(&format!(
        "{}/db/{}/query/v2",
        endpoint.trim_end_matches('/'),
        database
    ))
    .set(
        "Authorization",
        &format!(
            "Basic {}",
            base64::engine::general_purpose::STANDARD.encode(format!("{username}:{password}"))
        ),
    )
    .send_json(serde_json::json!({
        "statement": statement,
        "parameters": parameters,
    }))
    .unwrap()
    .into_json()
    .unwrap();
    if let Some(errors) = response.get("errors") {
        assert_eq!(errors, &serde_json::json!([]));
    }
    response["data"]["values"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row.as_array().unwrap().clone())
        .collect()
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
