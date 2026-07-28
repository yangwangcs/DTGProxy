use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::time::{SystemTime, UNIX_EPOCH};

use adapter_neo4j::Neo4jAdapterFactory;
use adapter_registry::{AdapterOpenRequest, AdapterRegistry, SecretString};
use base64::Engine;
use storage_api::{
    AdapterError, AdapterRequirement, AdjacencyExpandRequest, CandidateScanRequest,
    CanonicalScanRequest, ChangeScanRequest, CommittedMutationBatch, ComparisonOperator, KeySpan,
    Keyspace, LogicalKey, LogicalSnapshotExportRequest, Mutation, PropertyConstraint,
    PropertyGatherRequest, PropertyId, PushdownGuarantee, QueryPageBounds, ReadSnapshot,
    StorageAdapter,
};
use temporal_storage::{
    CommitContext, EdgeMutation, EdgeTypeId, ElementId, ElementRef, GraphId, HistoryAnchor,
    HistoryDelta, HistoryEntry, LabelId, PartitionId, ProjectionRecord, TemporalStore,
    ValidSegment, VertexMutation, current_vertex_graph_prefix, current_vertex_key,
    history_anchor_key, history_prefix, out_adjacency_prefix, temporal_event_graph_prefix,
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
fn live_neo4j_history_anchor_and_fifteen_deltas_page_through_one_snapshot() {
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
    let mut registry = AdapterRegistry::new();
    registry.register(Arc::new(Neo4jAdapterFactory)).unwrap();
    let opened = block_on(registry.open(
        "neo4j",
        &request(
            format!("history-page-{suffix}"),
            &endpoint,
            &database,
            &username,
            &password,
        ),
        AdapterRequirement::HotPluggableReplica,
    ))
    .unwrap();

    assert_history_page_continuation(opened.adapter());
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
fn live_neo4j_executes_all_native_typed_query_primitives() {
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
    let mut registry = AdapterRegistry::new();
    registry.register(Arc::new(Neo4jAdapterFactory)).unwrap();
    let opened = block_on(registry.open(
        "neo4j",
        &request(
            format!("typed-primitives-{suffix}"),
            &endpoint,
            &database,
            &username,
            &password,
        ),
        AdapterRequirement::HotPluggableReplica,
    ))
    .unwrap();
    let adapter = opened.into_adapter();
    let store = TemporalStore::new(Arc::clone(&adapter));
    let graph = GraphId::new(29);
    let source = ElementRef::vertex(graph, PartitionId::new(1), ElementId::new(10));
    let destination = ElementRef::vertex(graph, PartitionId::new(1), ElementId::new(20));
    let edge = ElementRef::edge(graph, PartitionId::new(1), ElementId::new(30));
    let valid =
        Interval::new(ValidTime::from_micros(10), Some(ValidTime::from_micros(30))).unwrap();
    let source_payload = CanonicalElement::new(
        1,
        BTreeMap::from([(1, GraphValue::String("active".into()))]),
    );
    block_on(store.commit_vertex(
        CommitContext::new(
            1,
            1,
            1001,
            TransactionTime::new(10, 0),
            TransactionTime::new(20, 0),
        ),
        VertexMutation::put(source, LabelId::new(3), valid, source_payload).unwrap(),
    ))
    .unwrap();
    block_on(
        store.commit_edge(
            CommitContext::new(
                1,
                2,
                1002,
                TransactionTime::new(20, 0),
                TransactionTime::new(30, 0),
            ),
            EdgeMutation::put_between(
                edge,
                EdgeTypeId::new(4),
                source,
                destination,
                valid,
                CanonicalElement::new(1, BTreeMap::new()),
            )
            .unwrap(),
        ),
    )
    .unwrap();
    let bounds = QueryPageBounds::new(32, 16 * 1024).unwrap();

    let candidates = block_on(
        adapter.scan_candidates(
            &CandidateScanRequest::new(
                KeySpan::prefix(Keyspace::Current, current_vertex_graph_prefix(graph)),
                ValidTime::from_micros(20),
                vec![PropertyConstraint::new(
                    PropertyId::new(1),
                    ComparisonOperator::Equal,
                    GraphValue::String("active".into()),
                )],
                bounds,
            )
            .unwrap(),
        ),
    )
    .unwrap();
    assert_eq!(candidates.guarantee(), PushdownGuarantee::Candidate);
    assert_eq!(candidates.applied_log_index(), 2);
    assert_eq!(candidates.entries().len(), 1);

    let properties = block_on(
        adapter.gather_properties(
            &PropertyGatherRequest::new(
                vec![current_vertex_key(source)],
                vec![PropertyId::new(1)],
                bounds,
            )
            .unwrap(),
        ),
    )
    .unwrap();
    assert_eq!(properties.guarantee(), PushdownGuarantee::Candidate);
    assert_eq!(properties.applied_log_index(), 2);
    assert_eq!(
        properties.rows()[0].values(),
        &[Some(GraphValue::String("active".into()))]
    );

    let adjacency = block_on(
        adapter.expand_adjacency(
            &AdjacencyExpandRequest::new(
                vec![KeySpan::prefix(
                    Keyspace::AdjOut,
                    out_adjacency_prefix(graph, PartitionId::new(1), source.id()),
                )],
                bounds,
            )
            .unwrap(),
        ),
    )
    .unwrap();
    assert_eq!(adjacency.guarantee(), PushdownGuarantee::Candidate);
    assert_eq!(adjacency.applied_log_index(), 2);
    assert_eq!(adjacency.entries().len(), 1);

    let changes = block_on(
        adapter.scan_changes(
            &ChangeScanRequest::new(
                KeySpan::prefix(Keyspace::TemporalIndex, temporal_event_graph_prefix(graph)),
                bounds,
            )
            .unwrap(),
        ),
    )
    .unwrap();
    assert_eq!(changes.guarantee(), PushdownGuarantee::Candidate);
    assert_eq!(changes.applied_log_index(), 2);
    assert!(!changes.entries().is_empty());
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

fn assert_history_page_continuation(adapter: &dyn StorageAdapter) {
    let element = ElementRef::vertex(GraphId::new(7), PartitionId::new(3), ElementId::new(9));
    let valid =
        Interval::new(ValidTime::from_micros(0), Some(ValidTime::from_micros(100))).unwrap();
    let payload = CanonicalElement::new(1, BTreeMap::new());
    let anchor = HistoryEntry::Anchor(
        HistoryAnchor::new(
            TransactionTime::new(100, 0),
            valid,
            ProjectionRecord::new(
                TransactionTime::new(100, 0),
                vec![ValidSegment::new(valid, payload.clone())],
            )
            .unwrap(),
        )
        .unwrap(),
    );
    let anchor_key = history_anchor_key(element, TransactionTime::new(100, 0), 0);
    let mut records = vec![(anchor_key, anchor.encode().unwrap())];
    for offset in 1..=15_u32 {
        let commit = 100 + i64::from(offset);
        let delta = HistoryEntry::Delta(HistoryDelta::put(
            TransactionTime::new(commit, 0),
            valid,
            payload.clone(),
        ));
        let key = history_anchor_key(element, TransactionTime::new(commit, 0), 0);
        records.push((key, delta.encode().unwrap()));
    }
    let mut expected_keys = records
        .iter()
        .map(|(key, _)| key.as_bytes().to_vec())
        .collect::<Vec<_>>();
    expected_keys.sort();
    let retained_sizes = records
        .iter()
        .map(|(key, value)| key.as_bytes().len() + value.len())
        .collect::<Vec<_>>();
    let byte_budget = u64::try_from(*retained_sizes.iter().max().unwrap()).unwrap();
    assert!(
        retained_sizes.iter().min().unwrap().saturating_mul(2)
            > usize::try_from(byte_budget).unwrap(),
        "byte budget fixture must admit one record but never two"
    );
    let mutations = records
        .iter()
        .enumerate()
        .map(|(sequence, (key, value))| {
            Mutation::put(u32::try_from(sequence).unwrap(), key.clone(), value.clone())
        })
        .collect();
    block_on(adapter.apply_committed(CommittedMutationBatch {
        shard_id: 1,
        log_index: 1,
        txn_id: 401,
        mutations,
    }))
    .unwrap();

    let snapshot = block_on(adapter.begin_read_snapshot()).unwrap();
    assert_eq!(snapshot.applied_log_index(), 1);
    let later = HistoryEntry::Delta(HistoryDelta::put(
        TransactionTime::new(99, 0),
        valid,
        payload,
    ));
    let later_key = history_anchor_key(element, TransactionTime::new(99, 0), 0);
    assert!(
        expected_keys
            .last()
            .is_some_and(|key| key.as_slice() < later_key.as_bytes()),
        "intervening record must sort into a later page"
    );
    block_on(adapter.apply_committed(CommittedMutationBatch {
        shard_id: 1,
        log_index: 2,
        txn_id: 402,
        mutations: vec![Mutation::put(0, later_key.clone(), later.encode().unwrap())],
    }))
    .unwrap();

    let prefix = history_prefix(element);
    let (item_records, item_page_lengths) = collect_history_pages(
        snapshot.as_ref(),
        &prefix,
        QueryPageBounds::new(3, 16 * 1024).unwrap(),
        1,
    );
    let (byte_records, byte_page_lengths) = collect_history_pages(
        snapshot.as_ref(),
        &prefix,
        QueryPageBounds::new(3, byte_budget).unwrap(),
        1,
    );

    assert_eq!(item_page_lengths, vec![3, 3, 3, 3, 3, 1]);
    assert_eq!(byte_page_lengths, vec![1; 16]);
    assert_eq!(record_keys(&item_records), expected_keys);
    assert_eq!(record_keys(&byte_records), expected_keys);
    assert!(!record_keys(&item_records).contains(&later_key.as_bytes().to_vec()));
    assert_history_record_counts(&item_records, 1, 15);

    let fresh = block_on(adapter.begin_read_snapshot()).unwrap();
    assert_eq!(fresh.applied_log_index(), 2);
    let (fresh_records, _) = collect_history_pages(
        fresh.as_ref(),
        &prefix,
        QueryPageBounds::new(32, 16 * 1024).unwrap(),
        2,
    );
    assert!(record_keys(&fresh_records).contains(&later_key.as_bytes().to_vec()));
    assert_history_record_counts(&fresh_records, 1, 16);
}

fn collect_history_pages(
    snapshot: &dyn ReadSnapshot,
    prefix: &[u8],
    bounds: QueryPageBounds,
    expected_log_index: u64,
) -> (Vec<(Vec<u8>, Vec<u8>)>, Vec<usize>) {
    let mut span = KeySpan::prefix(Keyspace::History, prefix.to_vec());
    let mut records = Vec::new();
    let mut page_lengths = Vec::new();
    loop {
        let page =
            block_on(snapshot.scan_canonical(&CanonicalScanRequest::new(span, bounds).unwrap()))
                .unwrap();
        assert_eq!(page.applied_log_index(), expected_log_index);
        let retained_bytes = page.entries().iter().fold(0_u64, |total, entry| {
            total
                .saturating_add(u64::try_from(entry.key().as_bytes().len()).unwrap())
                .saturating_add(u64::try_from(entry.value().len()).unwrap())
        });
        assert!(retained_bytes <= bounds.max_bytes());
        page_lengths.push(page.entries().len());
        records.extend(
            page.entries()
                .iter()
                .map(|entry| (entry.key().as_bytes().to_vec(), entry.value().to_vec())),
        );
        assert!(records.len() <= 17, "history pagination did not terminate");
        let Some(next_start) = page.next_start() else {
            break;
        };
        span = KeySpan::prefix_from(
            Keyspace::History,
            prefix.to_vec(),
            next_start.as_bytes().to_vec(),
        )
        .unwrap();
    }
    (records, page_lengths)
}

fn record_keys(records: &[(Vec<u8>, Vec<u8>)]) -> Vec<Vec<u8>> {
    records.iter().map(|(key, _)| key.clone()).collect()
}

fn assert_history_record_counts(
    records: &[(Vec<u8>, Vec<u8>)],
    expected_anchors: usize,
    expected_deltas: usize,
) {
    let (anchors, deltas) =
        records.iter().fold(
            (0, 0),
            |(anchors, deltas), (_, value)| match HistoryEntry::decode(value).unwrap() {
                HistoryEntry::Anchor(_) => (anchors + 1, deltas),
                HistoryEntry::Delta(_) => (anchors, deltas + 1),
            },
        );
    assert_eq!((anchors, deltas), (expected_anchors, expected_deltas));
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
