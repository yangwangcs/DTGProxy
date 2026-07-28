use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::time::{SystemTime, UNIX_EPOCH};

use adapter_postgres::{PostgresAdapter, PostgresAdapterFactory};
use adapter_registry::{AdapterFactory, AdapterOpenRequest, AdapterRegistry, SecretString};
use adapter_rocksdb::{RocksAdapter, RocksAdapterFactory};
use postgres::{Client, NoTls};
use storage_api::{
    AdapterError, AdapterRequirement, AdjacencyExpandRequest, CanonicalScanRequest,
    ChangeScanRequest, CommittedMutationBatch, KeySpan, Keyspace, LogicalKey,
    LogicalSnapshotExportRequest, LogicalSnapshotHeaderV1, Mutation, PropertyConstraint,
    PropertyGatherRequest, PropertyId, PushdownGuarantee, QueryPageBounds, ReadSnapshot,
    StorageAdapter,
};
use temporal_storage::{
    CommitContext, EdgeMutation, EdgeTypeId, ElementId, ElementRef, GraphId, HistoryAnchor,
    HistoryDelta, HistoryEntry, LabelId, PartitionId, ProjectionRecord, TemporalStore,
    ValidSegment, VertexMutation, current_vertex_key, history_anchor_key, history_prefix,
    out_adjacency_prefix, temporal_event_graph_prefix,
};
use temporal_types::{CanonicalElement, GraphValue, Interval, TransactionTime, ValidTime};

#[test]
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
        block_on(source.scan(&KeySpan::prefix(
            Keyspace::TemporalIndex,
            b"vertex/".to_vec()
        )))
        .unwrap()
        .len(),
        1
    );
    assert_eq!(
        block_on(
            source.scan(
                &KeySpan::prefix(Keyspace::TemporalIndex, b"vertex/".to_vec())
                    .with_max_bytes(1)
                    .unwrap()
            )
        ),
        Err(AdapterError::ScanByteLimit {
            limit: 1,
            required: 15,
        })
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
fn live_read_snapshot_pins_paginated_reads_to_one_repeatable_read_transaction() {
    let url = std::env::var("DTGPROXY_POSTGRES_URL")
        .expect("DTGPROXY_POSTGRES_URL must point to a disposable PostgreSQL database");
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let instance_id = format!("live-read-snapshot-{suffix}");
    let adapter = PostgresAdapter::open(&url, &instance_id, 2).unwrap();
    let first = key(b"event:1");
    let second = key(b"event:2");
    let later = key(b"event:3");
    block_on(adapter.apply_committed(batch(1, b"event:1", b"one"))).unwrap();
    block_on(adapter.apply_committed(batch(2, b"event:2", b"two"))).unwrap();

    let snapshot = block_on(adapter.begin_read_snapshot()).unwrap();
    assert_eq!(snapshot.applied_log_index(), 2);

    block_on(adapter.apply_committed(batch(3, b"event:3", b"three"))).unwrap();
    assert_eq!(
        block_on(snapshot.multi_get(&[first.clone(), later.clone()])).unwrap(),
        vec![Some(b"one".to_vec()), None]
    );
    let first_page = block_on(
        snapshot.scan_canonical(
            &CanonicalScanRequest::new(
                KeySpan::prefix(Keyspace::TemporalIndex, b"event:".to_vec()),
                QueryPageBounds::new(1, 64).unwrap(),
            )
            .unwrap(),
        ),
    )
    .unwrap();
    assert_eq!(first_page.applied_log_index(), 2);
    assert_eq!(first_page.entries().len(), 1);
    assert_eq!(first_page.entries()[0].key(), &first);
    assert_eq!(
        first_page.next_start().map(LogicalKey::as_bytes),
        Some(second.as_bytes())
    );

    let second_page = block_on(
        snapshot.scan_canonical(
            &CanonicalScanRequest::new(
                KeySpan::prefix_from(
                    Keyspace::TemporalIndex,
                    b"event:".to_vec(),
                    first_page.next_start().unwrap().as_bytes().to_vec(),
                )
                .unwrap(),
                QueryPageBounds::new(1, 64).unwrap(),
            )
            .unwrap(),
        ),
    )
    .unwrap();
    assert_eq!(second_page.applied_log_index(), 2);
    assert_eq!(second_page.entries().len(), 1);
    assert_eq!(second_page.entries()[0].key(), &second);
    assert_eq!(second_page.next_start(), None);
    assert_eq!(
        block_on(
            snapshot.scan(
                &KeySpan::prefix(Keyspace::TemporalIndex, b"event:".to_vec())
                    .with_limit(1)
                    .unwrap(),
            )
        )
        .unwrap()
        .into_iter()
        .map(|entry| entry.key().as_bytes().to_vec())
        .collect::<Vec<_>>(),
        vec![b"event:1".to_vec()]
    );
    assert_eq!(
        block_on(
            snapshot.scan(
                &KeySpan::prefix_from(
                    Keyspace::TemporalIndex,
                    b"event:".to_vec(),
                    second.as_bytes().to_vec(),
                )
                .unwrap(),
            )
        )
        .unwrap()
        .into_iter()
        .map(|entry| entry.key().as_bytes().to_vec())
        .collect::<Vec<_>>(),
        vec![b"event:2".to_vec()]
    );
    assert_eq!(
        block_on(adapter.scan(&KeySpan::prefix(
            Keyspace::TemporalIndex,
            b"event:".to_vec(),
        )))
        .unwrap()
        .len(),
        3
    );

    drop(snapshot);
    drop(adapter);
    cleanup(&url, &[&instance_id]);
}

#[test]
fn live_postgres_history_anchor_and_fifteen_deltas_page_through_one_snapshot() {
    let url = std::env::var("DTGPROXY_POSTGRES_URL")
        .expect("DTGPROXY_POSTGRES_URL must point to a disposable PostgreSQL database");
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let instance_id = format!("history-page-{suffix}");
    let adapter = PostgresAdapter::open(&url, &instance_id, 2).unwrap();

    assert_history_page_continuation(&adapter);

    drop(adapter);
    cleanup(&url, &[&instance_id]);
}

#[test]
fn live_typed_primitives_keep_candidate_residuals_and_exact_native_pages() {
    let url = std::env::var("DTGPROXY_POSTGRES_URL")
        .expect("DTGPROXY_POSTGRES_URL must point to a disposable PostgreSQL database");
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let instance_id = format!("live-typed-primitives-{suffix}");
    let store = TemporalStore::new(PostgresAdapter::open(&url, &instance_id, 2).unwrap());
    assert!(!store.adapter().capabilities().predicate_pushdown);
    assert!(store.adapter().capabilities().adjacency_pushdown);
    assert!(store.adapter().capabilities().change_feed);
    assert_eq!(
        store.adapter().query_primitive_capabilities(),
        adapter_postgres::POSTGRES_QUERY_PRIMITIVE_CAPABILITIES
    );
    let graph = GraphId::new(44);
    let partition = PartitionId::new(7);
    let source = ElementRef::vertex(graph, partition, ElementId::new(1));
    let destination = ElementRef::vertex(graph, partition, ElementId::new(2));
    let edge = ElementRef::edge(graph, partition, ElementId::new(3));
    let second_edge = ElementRef::edge(graph, partition, ElementId::new(4));
    let valid = Interval::new(ValidTime::from_micros(10), None).unwrap();
    let source_payload = CanonicalElement::new(
        1,
        std::collections::BTreeMap::from([(9, GraphValue::String("source".to_owned()))]),
    );
    let destination_payload = CanonicalElement::new(
        1,
        std::collections::BTreeMap::from([(9, GraphValue::String("destination".to_owned()))]),
    );
    block_on(store.commit_vertex(
        CommitContext::new(
            1,
            1,
            901,
            TransactionTime::new(10, 0),
            TransactionTime::new(20, 0),
        ),
        VertexMutation::put(source, LabelId::new(1), valid, source_payload).unwrap(),
    ))
    .unwrap();
    block_on(store.commit_vertex(
        CommitContext::new(
            1,
            2,
            902,
            TransactionTime::new(20, 0),
            TransactionTime::new(30, 0),
        ),
        VertexMutation::put(destination, LabelId::new(1), valid, destination_payload).unwrap(),
    ))
    .unwrap();
    block_on(
        store.commit_edge(
            CommitContext::new(
                1,
                3,
                903,
                TransactionTime::new(30, 0),
                TransactionTime::new(40, 0),
            ),
            EdgeMutation::put_between(
                edge,
                EdgeTypeId::new(2),
                source,
                destination,
                valid,
                CanonicalElement::new(1, std::collections::BTreeMap::new()),
            )
            .unwrap(),
        ),
    )
    .unwrap();
    block_on(
        store.commit_edge(
            CommitContext::new(
                1,
                4,
                904,
                TransactionTime::new(40, 0),
                TransactionTime::new(50, 0),
            ),
            EdgeMutation::put_between(
                second_edge,
                EdgeTypeId::new(3),
                source,
                destination,
                valid,
                CanonicalElement::new(1, std::collections::BTreeMap::new()),
            )
            .unwrap(),
        ),
    )
    .unwrap();

    let bounds = QueryPageBounds::new(1, 4096).unwrap();
    let candidates = block_on(
        store.adapter().scan_candidates(
            &storage_api::CandidateScanRequest::new(
                KeySpan::prefix(Keyspace::Current, vec![0x08]),
                ValidTime::from_micros(10),
                vec![PropertyConstraint::new(
                    PropertyId::new(9),
                    storage_api::ComparisonOperator::Equal,
                    GraphValue::String("does-not-match".to_owned()),
                )],
                bounds,
            )
            .unwrap(),
        ),
    )
    .unwrap();
    assert_eq!(candidates.guarantee(), PushdownGuarantee::Candidate);
    assert_eq!(candidates.applied_log_index(), 4);
    assert_eq!(candidates.entries().len(), 1);
    let next = candidates
        .next_start()
        .expect("candidate page continuation");
    let second_candidates = block_on(
        store.adapter().scan_candidates(
            &storage_api::CandidateScanRequest::new(
                KeySpan::prefix_from(Keyspace::Current, vec![0x08], next.as_bytes().to_vec())
                    .unwrap(),
                ValidTime::from_micros(10),
                Vec::new(),
                bounds,
            )
            .unwrap(),
        ),
    )
    .unwrap();
    assert_eq!(second_candidates.entries().len(), 1);
    assert!(second_candidates.next_start().is_none());

    let properties = block_on(
        store.adapter().gather_properties(
            &PropertyGatherRequest::new(
                vec![current_vertex_key(destination), current_vertex_key(source)],
                vec![PropertyId::new(9)],
                QueryPageBounds::new(4, 4096).unwrap(),
            )
            .unwrap(),
        ),
    )
    .unwrap();
    assert_eq!(properties.guarantee(), PushdownGuarantee::Candidate);
    assert_eq!(properties.applied_log_index(), 4);
    assert_eq!(properties.rows()[0].key(), &current_vertex_key(destination));
    assert_eq!(
        properties.rows()[0].values(),
        &[Some(GraphValue::String("destination".to_owned()))]
    );
    assert_eq!(properties.rows()[1].key(), &current_vertex_key(source));
    assert_eq!(
        properties.rows()[1].values(),
        &[Some(GraphValue::String("source".to_owned()))]
    );
    let bounded_properties = block_on(
        store.adapter().gather_properties(
            &PropertyGatherRequest::new(
                vec![current_vertex_key(destination), current_vertex_key(source)],
                vec![PropertyId::new(9)],
                QueryPageBounds::new(4, 63).unwrap(),
            )
            .unwrap(),
        ),
    )
    .unwrap();
    assert!(
        bounded_properties
            .rows()
            .iter()
            .all(|row| row.values() == [None])
    );

    let mut history_graph_prefix = vec![0x20];
    history_graph_prefix.extend_from_slice(&graph.value().to_be_bytes());
    let history = block_on(
        store.adapter().scan_candidates(
            &storage_api::CandidateScanRequest::new(
                KeySpan::prefix(Keyspace::History, history_graph_prefix.clone()),
                ValidTime::from_micros(10),
                Vec::new(),
                QueryPageBounds::new(1, 4096).unwrap(),
            )
            .unwrap(),
        ),
    )
    .unwrap();
    assert_eq!(history.guarantee(), PushdownGuarantee::Candidate);
    assert_eq!(history.entries().len(), 1);
    let mut history_keys = vec![history.entries()[0].key().as_bytes().to_vec()];
    let mut history_next = history.next_start().cloned();
    while let Some(next) = history_next {
        let page = block_on(
            store.adapter().scan_candidates(
                &storage_api::CandidateScanRequest::new(
                    KeySpan::prefix_from(
                        Keyspace::History,
                        history_graph_prefix.clone(),
                        next.as_bytes().to_vec(),
                    )
                    .unwrap(),
                    ValidTime::from_micros(10),
                    Vec::new(),
                    QueryPageBounds::new(1, 4096).unwrap(),
                )
                .unwrap(),
            ),
        )
        .unwrap();
        assert_eq!(page.entries().len(), 1);
        history_keys.push(page.entries()[0].key().as_bytes().to_vec());
        assert!(
            history_keys.len() <= 4,
            "history pagination did not terminate"
        );
        history_next = page.next_start().cloned();
    }
    assert_eq!(history_keys.len(), 4);
    assert!(history_keys.windows(2).all(|keys| keys[0] < keys[1]));

    let adjacency = block_on(
        store.adapter().expand_adjacency(
            &AdjacencyExpandRequest::new(
                vec![KeySpan::prefix(
                    Keyspace::AdjOut,
                    out_adjacency_prefix(graph, partition, source.id()),
                )],
                QueryPageBounds::new(1, 4096).unwrap(),
            )
            .unwrap(),
        ),
    )
    .unwrap();
    assert_eq!(adjacency.guarantee(), PushdownGuarantee::Exact);
    assert_eq!(adjacency.applied_log_index(), 4);
    assert_eq!(adjacency.entries().len(), 1);
    let adjacency_next = adjacency.next().expect("adjacency page continuation");
    assert_eq!(adjacency_next.input_ordinal(), 0);
    let second_adjacency = block_on(
        store.adapter().expand_adjacency(
            &AdjacencyExpandRequest::new(
                vec![
                    KeySpan::prefix_from(
                        Keyspace::AdjOut,
                        out_adjacency_prefix(graph, partition, source.id()),
                        adjacency_next.start().as_bytes().to_vec(),
                    )
                    .unwrap(),
                ],
                QueryPageBounds::new(1, 4096).unwrap(),
            )
            .unwrap(),
        ),
    )
    .unwrap();
    assert_eq!(second_adjacency.entries().len(), 1);
    assert!(second_adjacency.next().is_none());

    let changes = block_on(
        store.adapter().scan_changes(
            &ChangeScanRequest::new(
                KeySpan::prefix(Keyspace::TemporalIndex, temporal_event_graph_prefix(graph)),
                QueryPageBounds::new(2, 4096).unwrap(),
            )
            .unwrap(),
        ),
    )
    .unwrap();
    assert_eq!(changes.guarantee(), PushdownGuarantee::Exact);
    assert_eq!(changes.applied_log_index(), 4);
    assert_eq!(changes.entries().len(), 2);
    let changes_next = changes.next_start().expect("change page continuation");
    let second_changes = block_on(
        store.adapter().scan_changes(
            &ChangeScanRequest::new(
                KeySpan::prefix_from(
                    Keyspace::TemporalIndex,
                    temporal_event_graph_prefix(graph),
                    changes_next.as_bytes().to_vec(),
                )
                .unwrap(),
                QueryPageBounds::new(2, 4096).unwrap(),
            )
            .unwrap(),
        ),
    )
    .unwrap();
    assert_eq!(second_changes.entries().len(), 2);
    assert!(second_changes.next_start().is_none());

    drop(store);
    cleanup(&url, &[&instance_id]);
}

#[test]
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
    let fingerprint = PostgresAdapterFactory
        .mapping_descriptor()
        .expect("PostgreSQL Mapping descriptor")
        .schema_fingerprint();
    client
        .execute(
            "INSERT INTO dtgproxy.adapter_instance(instance_id, schema_version, mapping_fingerprint, applied_log_index, has_applied_index_record, published) VALUES ($1, 1, $2, $3, FALSE, FALSE)",
            &[
                &residue_id,
                &fingerprint.as_slice(),
                &0_u64.to_be_bytes().as_slice(),
            ],
        )
        .unwrap();
    client
        .execute(
            "INSERT INTO dtgproxy.opaque_records(instance_id, keyspace, logical_key, value) VALUES ($1, 6, $2, $3)",
            &[
                &residue_id,
                &b"partial".as_slice(),
                &b"must-not-serve".as_slice(),
            ],
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

#[test]
fn live_published_mapping_rejects_canonical_restore_without_deleting_online_data() {
    let url = std::env::var("DTGPROXY_POSTGRES_URL")
        .expect("DTGPROXY_POSTGRES_URL must point to a disposable PostgreSQL database");
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let instance_id = format!("live-published-restore-{suffix}");
    let mapping = PostgresAdapter::open(&url, &instance_id, 2).unwrap();
    let record_key = key(b"online/record");
    block_on(mapping.apply_committed(batch(1, b"online/record", b"must-survive"))).unwrap();

    let restore = block_on(storage_api::TemporalBackendMapping::restore_canonical(
        &mapping,
        LogicalSnapshotHeaderV1::new(7, 1),
    ));
    assert!(
        restore.is_err(),
        "a published Mapping must never open a canonical restore session"
    );
    drop(restore);

    assert_eq!(
        block_on(mapping.multi_get(&[record_key])).unwrap(),
        vec![Some(b"must-survive".to_vec())]
    );
    drop(mapping);
    cleanup(&url, &[&instance_id]);
}

#[test]
fn live_schema_drift_rejects_reintroduced_canonical_shadow_table() {
    let url = std::env::var("DTGPROXY_POSTGRES_URL")
        .expect("DTGPROXY_POSTGRES_URL must point to a disposable PostgreSQL database");
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let instance_id = format!("live-schema-drift-{suffix}");
    drop(PostgresAdapter::open(&url, &instance_id, 1).unwrap());

    let mut client = Client::connect(&url, NoTls).unwrap();
    client
        .batch_execute("CREATE TABLE dtgproxy.canonical_kv (instance_id TEXT NOT NULL)")
        .unwrap();
    drop(client);

    assert!(
        PostgresAdapter::open(&url, &instance_id, 1).is_err(),
        "schema drift must fail closed even when schema_meta is unchanged"
    );
    let mut client = Client::connect(&url, NoTls).unwrap();
    client
        .batch_execute("DROP TABLE dtgproxy.canonical_kv")
        .unwrap();
    cleanup(&url, &[&instance_id]);
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
