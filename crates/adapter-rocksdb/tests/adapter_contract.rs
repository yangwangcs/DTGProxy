use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use adapter_rocksdb::RocksAdapter;
use storage_api::{
    AdapterError, AdjacencyExpandRequest, CandidateScanRequest, CanonicalScanRequest,
    ChangeScanRequest, CommittedMutationBatch, ComparisonOperator, KeySpan, Keyspace, LogicalKey,
    Mutation, PropertyConstraint, PropertyGatherRequest, PropertyId, PushdownGuarantee,
    QueryPageBounds, QueryPrimitiveCapabilities, ReadSnapshot, StorageAdapter,
};
use temporal_storage::{
    ElementId, ElementRef, GraphId, HistoryAnchor, HistoryDelta, HistoryEntry, PartitionId,
    ProjectionRecord, ValidSegment, history_anchor_key, history_prefix,
};
use temporal_types::{CanonicalElement, Interval, TransactionTime};
use temporal_types::{GraphValue, ValidTime};

fn open() -> (tempfile::TempDir, RocksAdapter) {
    let directory = tempfile::tempdir().unwrap();
    let adapter = RocksAdapter::open(directory.path()).unwrap();
    (directory, adapter)
}

fn key(value: &str) -> LogicalKey {
    LogicalKey::new(value.as_bytes().to_vec())
}

fn batch(log_index: u64, txn_id: u128, mutations: Vec<Mutation>) -> CommittedMutationBatch {
    CommittedMutationBatch {
        shard_id: 3,
        log_index,
        txn_id,
        mutations,
    }
}

#[test]
fn committed_batch_is_visible_and_identical_log_replay_is_idempotent() {
    let (_directory, adapter) = open();
    let committed = batch(
        1,
        7,
        vec![Mutation::put(0, key("account:1"), b"A".to_vec())],
    );

    let first = block_on(adapter.apply_committed(committed.clone())).unwrap();
    let duplicate = block_on(adapter.apply_committed(committed)).unwrap();
    let values = block_on(adapter.multi_get(&[key("account:1"), key("missing")])).unwrap();

    assert_eq!(first.applied_log_index, 1);
    assert!(!first.duplicate);
    assert_eq!(duplicate.applied_log_index, 1);
    assert!(duplicate.duplicate);
    assert_eq!(values, vec![Some(b"A".to_vec()), None]);
    assert_eq!(adapter.applied_log_index().unwrap(), 1);
}

#[test]
fn mutation_replay_mismatch_rejects_the_entire_batch() {
    let (_directory, adapter) = open();
    block_on(adapter.apply_committed(batch(
        1,
        7,
        vec![Mutation::put(0, key("protected"), b"old".to_vec())],
    )))
    .unwrap();

    let error = block_on(adapter.apply_committed(batch(
        2,
        7,
        vec![
            Mutation::put(1, key("new"), b"must-not-appear".to_vec()),
            Mutation::put(0, key("protected"), b"different".to_vec()),
        ],
    )))
    .unwrap_err();

    assert_eq!(
        error,
        AdapterError::MutationReplayMismatch {
            txn_id: 7,
            sequence: 0,
        }
    );
    assert_eq!(adapter.applied_log_index().unwrap(), 1);
    assert_eq!(
        block_on(adapter.multi_get(&[key("protected"), key("new")])).unwrap(),
        vec![Some(b"old".to_vec()), None]
    );
}

#[test]
fn adapter_rejects_a_non_contiguous_log_index() {
    let (_directory, adapter) = open();

    let error = block_on(adapter.apply_committed(batch(2, 9, Vec::new()))).unwrap_err();

    assert_eq!(
        error,
        AdapterError::NonContiguousLogIndex {
            expected: 1,
            actual: 2,
        }
    );
    assert_eq!(adapter.applied_log_index().unwrap(), 0);
}

#[test]
fn adapter_rejects_different_content_for_an_applied_log_index() {
    let (_directory, adapter) = open();
    block_on(adapter.apply_committed(batch(
        1,
        10,
        vec![Mutation::put(0, key("a"), b"one".to_vec())],
    )))
    .unwrap();

    let error = block_on(adapter.apply_committed(batch(
        1,
        10,
        vec![Mutation::put(0, key("a"), b"two".to_vec())],
    )))
    .unwrap_err();

    assert_eq!(
        error,
        AdapterError::CommittedLogReplayMismatch { log_index: 1 }
    );
}

#[test]
fn delete_mutation_removes_a_committed_key() {
    let (_directory, adapter) = open();
    block_on(adapter.apply_committed(batch(
        1,
        11,
        vec![Mutation::put(0, key("a"), b"one".to_vec())],
    )))
    .unwrap();
    block_on(adapter.apply_committed(batch(2, 12, vec![Mutation::delete(0, key("a"))]))).unwrap();

    assert_eq!(
        block_on(adapter.multi_get(&[key("a")])).unwrap(),
        vec![None]
    );
}

#[test]
fn duplicate_sequence_inside_one_batch_is_rejected_atomically() {
    let (_directory, adapter) = open();

    let error = block_on(adapter.apply_committed(batch(
        1,
        13,
        vec![
            Mutation::put(0, key("a"), b"one".to_vec()),
            Mutation::put(0, key("b"), b"two".to_vec()),
        ],
    )))
    .unwrap_err();

    assert_eq!(
        error,
        AdapterError::DuplicateMutationSequence {
            txn_id: 13,
            sequence: 0,
        }
    );
    assert_eq!(adapter.applied_log_index().unwrap(), 0);
    assert_eq!(
        block_on(adapter.multi_get(&[key("a"), key("b")])).unwrap(),
        vec![None, None]
    );
}

#[test]
fn identical_bytes_in_current_and_history_are_isolated() {
    let (_directory, adapter) = open();
    let current = LogicalKey::in_keyspace(Keyspace::Current, b"same".to_vec());
    let history = LogicalKey::in_keyspace(Keyspace::History, b"same".to_vec());

    block_on(adapter.apply_committed(batch(
        1,
        14,
        vec![
            Mutation::put(0, current.clone(), b"current".to_vec()),
            Mutation::put(1, history.clone(), b"history".to_vec()),
        ],
    )))
    .unwrap();

    assert_eq!(
        block_on(adapter.multi_get(&[current, history])).unwrap(),
        vec![Some(b"current".to_vec()), Some(b"history".to_vec())]
    );
}

#[test]
fn prefix_scan_is_ordered_and_isolated_to_one_keyspace() {
    let (_directory, adapter) = open();
    block_on(adapter.apply_committed(batch(
        1,
        15,
        vec![
            Mutation::put(0, LogicalKey::new(b"edge:2".to_vec()), b"two".to_vec()),
            Mutation::put(1, LogicalKey::new(b"edge:1".to_vec()), b"one".to_vec()),
            Mutation::put(2, LogicalKey::new(b"other".to_vec()), b"skip".to_vec()),
            Mutation::put(
                3,
                LogicalKey::in_keyspace(Keyspace::History, b"edge:0".to_vec()),
                b"history".to_vec(),
            ),
        ],
    )))
    .unwrap();

    let values =
        block_on(adapter.scan(&KeySpan::prefix(Keyspace::Current, b"edge:".to_vec()))).unwrap();

    assert_eq!(values.len(), 2);
    assert_eq!(values[0].key().as_bytes(), b"edge:1");
    assert_eq!(values[0].value(), b"one");
    assert_eq!(values[1].key().as_bytes(), b"edge:2");
    assert_eq!(values[1].value(), b"two");

    let bounded = block_on(
        adapter.scan(
            &KeySpan::range(
                Keyspace::Current,
                b"edge:1".to_vec(),
                Some(b"edge:3".to_vec()),
            )
            .unwrap()
            .with_limit(1)
            .unwrap(),
        ),
    )
    .unwrap();
    assert_eq!(bounded.len(), 1);
    assert_eq!(bounded[0].key().as_bytes(), b"edge:1");

    assert_eq!(
        block_on(
            adapter.scan(
                &KeySpan::prefix(Keyspace::Current, b"edge:".to_vec())
                    .with_max_bytes(8)
                    .unwrap(),
            )
        ),
        Err(AdapterError::ScanByteLimit {
            limit: 8,
            required: 9,
        })
    );
}

#[test]
fn canonical_scan_pages_follow_one_pinned_snapshot_without_gaps() {
    let (_directory, adapter) = open();
    block_on(
        adapter.apply_committed(batch(
            1,
            16,
            (1..=5)
                .map(|index| {
                    Mutation::put(
                        index - 1,
                        LogicalKey::new(format!("node:{index}").into_bytes()),
                        vec![index as u8],
                    )
                })
                .collect(),
        )),
    )
    .unwrap();
    let snapshot = block_on(adapter.begin_read_snapshot()).unwrap();
    block_on(adapter.apply_committed(batch(
        2,
        17,
        vec![Mutation::put(
            0,
            LogicalKey::new(b"node:6".to_vec()),
            vec![6],
        )],
    )))
    .unwrap();
    let prefix = b"node:".to_vec();
    let bounds = QueryPageBounds::new(2, 64).unwrap();
    let mut span = KeySpan::prefix(Keyspace::Current, prefix.clone());
    let mut keys = Vec::new();

    loop {
        let request = CanonicalScanRequest::new(span, bounds).unwrap();
        let page = block_on(snapshot.scan_canonical(&request)).unwrap();
        assert_eq!(page.applied_log_index(), 1);
        keys.extend(
            page.entries()
                .iter()
                .map(|entry| entry.key().as_bytes().to_vec()),
        );
        let Some(next_start) = page.next_start() else {
            break;
        };
        span = KeySpan::prefix_from(
            Keyspace::Current,
            prefix.clone(),
            next_start.as_bytes().to_vec(),
        )
        .unwrap();
    }

    assert_eq!(
        keys,
        (1..=5)
            .map(|index| format!("node:{index}").into_bytes())
            .collect::<Vec<_>>()
    );
}

#[test]
fn history_anchor_and_fifteen_deltas_page_through_one_pinned_snapshot() {
    let (_directory, adapter) = open();
    assert_history_page_continuation(&adapter);
}

#[test]
fn read_snapshot_pins_multi_get_and_paginated_scans_to_its_committed_prefix() {
    let (_directory, adapter) = open();
    let first = LogicalKey::new(b"event:1".to_vec());
    let second = LogicalKey::new(b"event:2".to_vec());
    let later = LogicalKey::new(b"event:3".to_vec());
    block_on(adapter.apply_committed(batch(
        1,
        21,
        vec![
            Mutation::put(0, first.clone(), b"one".to_vec()),
            Mutation::put(1, second.clone(), b"two".to_vec()),
        ],
    )))
    .unwrap();

    let snapshot = block_on(adapter.begin_read_snapshot()).unwrap();
    assert_eq!(snapshot.applied_log_index(), 1);

    block_on(adapter.apply_committed(batch(
        2,
        22,
        vec![Mutation::put(0, later.clone(), b"three".to_vec())],
    )))
    .unwrap();

    assert_eq!(
        block_on(snapshot.multi_get(&[first.clone(), later.clone()])).unwrap(),
        vec![Some(b"one".to_vec()), None]
    );
    assert_eq!(
        block_on(
            snapshot.scan(
                &KeySpan::prefix(Keyspace::Current, b"event:".to_vec())
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
                &KeySpan::prefix_from(Keyspace::Current, b"event:".to_vec(), b"event:2".to_vec(),)
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
        block_on(adapter.scan(&KeySpan::prefix(Keyspace::Current, b"event:".to_vec())))
            .unwrap()
            .len(),
        3
    );
}

#[test]
fn typed_primitives_advertise_conservative_native_candidate_support() {
    let (_directory, adapter) = open();

    assert_eq!(
        adapter.query_primitive_capabilities(),
        QueryPrimitiveCapabilities::new(
            PushdownGuarantee::Candidate,
            PushdownGuarantee::Candidate,
            PushdownGuarantee::Exact,
            PushdownGuarantee::Exact,
        )
    );
    let mapping = adapter.mapping_descriptor().unwrap();
    assert!(mapping.capabilities().predicate_pushdown);
    assert!(mapping.capabilities().adjacency_pushdown);
    assert!(mapping.capabilities().change_feed);
}

#[test]
fn candidate_and_change_scans_are_ordered_bounded_and_keep_the_residual() {
    let (_directory, adapter) = open();
    block_on(adapter.apply_committed(batch(
        1,
        23,
        vec![
            Mutation::put(
                0,
                LogicalKey::in_keyspace(Keyspace::Current, b"vertex/1".to_vec()),
                b"first".to_vec(),
            ),
            Mutation::put(
                1,
                LogicalKey::in_keyspace(Keyspace::Current, b"vertex/2".to_vec()),
                b"second".to_vec(),
            ),
            Mutation::put(
                2,
                LogicalKey::in_keyspace(Keyspace::Current, b"vertex/3".to_vec()),
                b"third".to_vec(),
            ),
            Mutation::put(
                3,
                LogicalKey::in_keyspace(Keyspace::TemporalIndex, b"event/1".to_vec()),
                b"first-change".to_vec(),
            ),
            Mutation::put(
                4,
                LogicalKey::in_keyspace(Keyspace::TemporalIndex, b"event/2".to_vec()),
                b"second-change".to_vec(),
            ),
        ],
    )))
    .unwrap();

    let bounds = QueryPageBounds::new(2, 128).unwrap();
    let candidates = CandidateScanRequest::new(
        KeySpan::prefix(Keyspace::Current, b"vertex/".to_vec()),
        ValidTime::from_micros(99),
        vec![PropertyConstraint::new(
            PropertyId::new(7),
            ComparisonOperator::Equal,
            GraphValue::String("cannot-be-proven".to_owned()),
        )],
        bounds,
    )
    .unwrap();
    let first = block_on(adapter.scan_candidates(&candidates)).unwrap();
    assert_eq!(first.applied_log_index(), 1);
    assert_eq!(first.guarantee(), PushdownGuarantee::Candidate);
    assert_eq!(
        first
            .entries()
            .iter()
            .map(|entry| entry.key().as_bytes())
            .collect::<Vec<_>>(),
        vec![b"vertex/1".as_slice(), b"vertex/2".as_slice()]
    );
    assert_eq!(
        first.next_start().map(LogicalKey::as_bytes),
        Some(b"vertex/3".as_slice())
    );

    let second = CandidateScanRequest::new(
        KeySpan::prefix_from(
            Keyspace::Current,
            b"vertex/".to_vec(),
            first.next_start().unwrap().as_bytes().to_vec(),
        )
        .unwrap(),
        ValidTime::from_micros(99),
        Vec::new(),
        bounds,
    )
    .unwrap();
    let second = block_on(adapter.scan_candidates(&second)).unwrap();
    assert_eq!(
        second
            .entries()
            .iter()
            .map(|entry| entry.key().as_bytes())
            .collect::<Vec<_>>(),
        vec![b"vertex/3".as_slice()]
    );
    assert!(second.next_start().is_none());

    let changes = ChangeScanRequest::new(
        KeySpan::prefix(Keyspace::TemporalIndex, b"event/".to_vec()),
        QueryPageBounds::new(1, 128).unwrap(),
    )
    .unwrap();
    let changes = block_on(adapter.scan_changes(&changes)).unwrap();
    assert_eq!(changes.applied_log_index(), 1);
    assert_eq!(changes.guarantee(), PushdownGuarantee::Exact);
    assert_eq!(changes.entries()[0].key().as_bytes(), b"event/1");
    assert_eq!(
        changes.next_start().map(LogicalKey::as_bytes),
        Some(b"event/2".as_slice())
    );
}

#[test]
fn adjacency_expand_respects_input_order_bounds_and_continuation() {
    let (_directory, adapter) = open();
    block_on(adapter.apply_committed(batch(
        1,
        24,
        vec![
            Mutation::put(
                0,
                LogicalKey::in_keyspace(Keyspace::AdjOut, b"vertex/1/edge/2".to_vec()),
                b"second".to_vec(),
            ),
            Mutation::put(
                1,
                LogicalKey::in_keyspace(Keyspace::AdjOut, b"vertex/1/edge/1".to_vec()),
                b"first".to_vec(),
            ),
            Mutation::put(
                2,
                LogicalKey::in_keyspace(Keyspace::AdjOut, b"vertex/2/edge/1".to_vec()),
                b"third".to_vec(),
            ),
        ],
    )))
    .unwrap();

    let request = AdjacencyExpandRequest::new(
        vec![
            KeySpan::prefix(Keyspace::AdjOut, b"vertex/1/".to_vec()),
            KeySpan::prefix(Keyspace::AdjOut, b"vertex/2/".to_vec()),
        ],
        QueryPageBounds::new(2, 128).unwrap(),
    )
    .unwrap();
    let page = block_on(adapter.expand_adjacency(&request)).unwrap();
    assert_eq!(page.applied_log_index(), 1);
    assert_eq!(page.guarantee(), PushdownGuarantee::Exact);
    assert_eq!(
        page.entries()
            .iter()
            .map(|entry| (entry.input_ordinal(), entry.entry().key().as_bytes()))
            .collect::<Vec<_>>(),
        vec![
            (0, b"vertex/1/edge/1".as_slice()),
            (0, b"vertex/1/edge/2".as_slice()),
        ]
    );
    assert_eq!(page.next().unwrap().input_ordinal(), 1);
    assert_eq!(page.next().unwrap().start().as_bytes(), b"vertex/2/edge/1");
}

#[test]
fn property_gather_uses_multiget_order_and_never_claims_an_exact_projection() {
    let (_directory, adapter) = open();
    let first = LogicalKey::in_keyspace(Keyspace::Current, b"vertex/1".to_vec());
    let second = LogicalKey::in_keyspace(Keyspace::Current, b"vertex/2".to_vec());
    let projection = projection_with_property(7, GraphValue::String("Ada".to_owned()));
    block_on(adapter.apply_committed(batch(
        1,
        25,
        vec![Mutation::put(0, first.clone(), projection)],
    )))
    .unwrap();

    let request = PropertyGatherRequest::new(
        vec![second.clone(), first.clone()],
        vec![PropertyId::new(7)],
        QueryPageBounds::new(2, 256).unwrap(),
    )
    .unwrap();
    let page = block_on(adapter.gather_properties(&request)).unwrap();
    assert_eq!(page.applied_log_index(), 1);
    assert_eq!(page.guarantee(), PushdownGuarantee::Candidate);
    assert_eq!(page.rows()[0].key(), &second);
    assert_eq!(page.rows()[0].values(), &[None]);
    assert_eq!(page.rows()[1].key(), &first);
    assert_eq!(
        page.rows()[1].values(),
        &[Some(GraphValue::String("Ada".to_owned()))]
    );
}

#[test]
fn typed_scan_uses_the_pinned_snapshot_and_applied_index() {
    let (_directory, adapter) = open();
    block_on(adapter.apply_committed(batch(
        1,
        26,
        vec![Mutation::put(
            0,
            LogicalKey::in_keyspace(Keyspace::Current, b"vertex/1".to_vec()),
            b"first".to_vec(),
        )],
    )))
    .unwrap();
    let snapshot = block_on(adapter.begin_read_snapshot()).unwrap();
    block_on(adapter.apply_committed(batch(
        2,
        27,
        vec![Mutation::put(
            0,
            LogicalKey::in_keyspace(Keyspace::Current, b"vertex/2".to_vec()),
            b"second".to_vec(),
        )],
    )))
    .unwrap();

    let request = CandidateScanRequest::new(
        KeySpan::prefix(Keyspace::Current, b"vertex/".to_vec()),
        ValidTime::from_micros(500),
        Vec::new(),
        QueryPageBounds::new(8, 128).unwrap(),
    )
    .unwrap();
    let page = block_on(snapshot.scan_candidates(&request)).unwrap();
    assert_eq!(page.applied_log_index(), 1);
    assert_eq!(
        page.entries()
            .iter()
            .map(|entry| entry.key().as_bytes())
            .collect::<Vec<_>>(),
        vec![b"vertex/1".as_slice()]
    );
}

fn projection_with_property(property: u32, value: GraphValue) -> Vec<u8> {
    let mut payload = b"DTP1".to_vec();
    payload.extend_from_slice(&1_u64.to_be_bytes());
    payload.extend_from_slice(&1_u32.to_be_bytes());
    payload.extend_from_slice(&property.to_be_bytes());
    match value {
        GraphValue::String(value) => {
            payload.push(5);
            payload.extend_from_slice(&(value.len() as u32).to_be_bytes());
            payload.extend_from_slice(value.as_bytes());
        }
        other => panic!("test only encodes string properties, got {other:?}"),
    }

    let mut projection = b"DTGP".to_vec();
    projection.extend_from_slice(&1_u16.to_be_bytes());
    projection.extend_from_slice(&1_i64.to_be_bytes());
    projection.extend_from_slice(&0_u32.to_be_bytes());
    projection.extend_from_slice(&1_u32.to_be_bytes());
    projection.extend_from_slice(&0_i64.to_be_bytes());
    projection.push(0);
    projection.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    projection.extend_from_slice(&payload);
    projection.extend_from_slice(&fnv1a(&projection).to_be_bytes());
    projection
}

fn fnv1a(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325_u64, |value, byte| {
        (value ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
    })
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
    block_on(adapter.apply_committed(batch(1, 401, mutations))).unwrap();

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
    block_on(adapter.apply_committed(batch(
        2,
        402,
        vec![Mutation::put(0, later_key.clone(), later.encode().unwrap())],
    )))
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

struct NoopWake;

impl Wake for NoopWake {
    fn wake(self: Arc<Self>) {}
}

fn block_on<F>(future: F) -> F::Output
where
    F: Future,
{
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
