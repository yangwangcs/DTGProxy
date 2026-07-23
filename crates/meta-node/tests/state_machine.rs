use std::collections::BTreeMap;

use analytics_ledger::{
    AnalyticsJobId, GraphProjectionScope, JobCommand, JobSpec, JobState, ProjectionLimits,
};
use control_plane::{
    BackendProfile, CatalogCommand, DeploymentMode, GraphDefinition, Placement, TopologyDefinition,
};
use meta_node::{AnalyticsGcLeaseCommand, MetaStateError, MetaStateMachine};
use storage_api::AdapterRequirement;
use temporal_types::{TransactionTime, ValidTime};

fn graph() -> GraphDefinition {
    GraphDefinition::new(
        7,
        "social",
        1,
        TopologyDefinition::new(
            DeploymentMode::SharedNothing,
            99,
            128,
            1,
            vec![
                Placement::new(10, 1, vec![1, 2, 3]).unwrap(),
                Placement::new(20, 1, vec![1, 2, 3]).unwrap(),
            ],
        )
        .unwrap(),
        BackendProfile::new(
            "rocksdb",
            BTreeMap::from([("path".into(), "data/graph-7".into())]),
            BTreeMap::new(),
            AdapterRequirement::HotPluggableReplica,
            1,
        )
        .unwrap(),
    )
    .unwrap()
}

fn analytics_job(job_id: u128) -> JobSpec {
    JobSpec::new(
        AnalyticsJobId::new(job_id).unwrap(),
        job_id + 10_000,
        7,
        1,
        1,
        1,
        1,
        TransactionTime::new(1_000, 0),
        GraphProjectionScope::Snapshot {
            valid_time: ValidTime::from_micros(900),
        },
        "dtg.graph.pageRank",
        "1.0.0",
        "dtg.analytics-native",
        "1.0.0",
        Vec::new(),
        [9; 32],
        ProjectionLimits::new(100, 100, 1 << 20).unwrap(),
    )
    .unwrap()
}

#[test]
fn only_contiguous_committed_entries_advance_catalog_and_emit_events() {
    let mut state = MetaStateMachine::new(8).unwrap();
    let command = CatalogCommand::create_graph(101, 0, graph())
        .encode()
        .unwrap();

    assert!(matches!(
        state.apply_committed(1, 2, &command),
        Err(MetaStateError::NonContiguousIndex {
            expected: 1,
            actual: 2
        })
    ));
    assert_eq!(state.catalog().revision(), 0);
    let receipt = state.apply_committed(1, 1, &command).unwrap();
    assert_eq!(receipt.applied_index(), 1);
    assert_eq!(receipt.catalog_revision(), 1);
    assert!(!receipt.duplicate());
    let watch = state.watch_after(0).unwrap();
    assert_eq!(watch.events().len(), 1);
    assert_eq!(watch.events()[0].revision(), 1);
    assert_eq!(watch.events()[0].command(), command);
}

#[test]
fn command_replay_across_new_raft_indices_is_idempotent_and_payload_bound() {
    let mut state = MetaStateMachine::new(8).unwrap();
    let command = CatalogCommand::create_graph(101, 0, graph())
        .encode()
        .unwrap();
    state.apply_committed(1, 1, &command).unwrap();
    let replay = state.apply_committed(2, 2, &command).unwrap();
    assert!(replay.duplicate());
    assert_eq!(replay.catalog_revision(), 1);
    assert_eq!(state.applied_index(), 2);

    let different = CatalogCommand::publish_schema(101, 1, 7, 1, 2)
        .encode()
        .unwrap();
    assert!(matches!(
        state.apply_committed(2, 3, &different),
        Err(MetaStateError::Catalog(_))
    ));
    assert_eq!(state.applied_index(), 2);
}

#[test]
fn snapshot_restores_exact_state_and_bounded_watch_compaction() {
    let mut state = MetaStateMachine::new(2).unwrap();
    state
        .apply_committed(
            1,
            1,
            &CatalogCommand::create_graph(101, 0, graph())
                .encode()
                .unwrap(),
        )
        .unwrap();
    state
        .apply_committed(
            1,
            2,
            &CatalogCommand::publish_schema(102, 1, 7, 1, 2)
                .encode()
                .unwrap(),
        )
        .unwrap();
    state
        .apply_committed(
            1,
            3,
            &CatalogCommand::publish_schema(103, 2, 7, 2, 3)
                .encode()
                .unwrap(),
        )
        .unwrap();
    assert!(matches!(
        state.watch_after(0),
        Err(MetaStateError::RevisionCompacted {
            compacted_through: 1
        })
    ));

    let snapshot = state.encode_snapshot().unwrap();
    let restored = MetaStateMachine::decode_snapshot(&snapshot, 2).unwrap();
    assert_eq!(restored.applied_index(), 3);
    assert_eq!(restored.catalog(), state.catalog());
    assert!(restored.watch_after(3).unwrap().events().is_empty());

    let mut corrupted = snapshot;
    let middle = corrupted.len() / 2;
    corrupted[middle] ^= 1;
    assert_eq!(
        MetaStateMachine::decode_snapshot(&corrupted, 2).unwrap_err(),
        MetaStateError::SnapshotChecksumMismatch
    );
}

#[test]
fn predecessor_snapshot_version_is_rejected() {
    let state = MetaStateMachine::new(2).unwrap();
    let mut snapshot = state.encode_snapshot().unwrap();
    snapshot[4..6].copy_from_slice(&1_u16.to_be_bytes());
    let checksum_offset = snapshot.len() - 4;
    let checksum = crc32fast::hash(&snapshot[..checksum_offset]).to_be_bytes();
    snapshot[checksum_offset..].copy_from_slice(&checksum);

    assert_eq!(
        MetaStateMachine::decode_snapshot(&snapshot, 2),
        Err(MetaStateError::UnsupportedSnapshotVersion { actual: 1 })
    );
}

#[test]
fn analytics_ledger_is_raft_applied_snapshotted_and_catalog_revision_independent() {
    let mut state = MetaStateMachine::new(8).unwrap();
    state
        .apply_committed(
            1,
            1,
            &CatalogCommand::create_graph(101, 0, graph())
                .encode()
                .unwrap(),
        )
        .unwrap();
    let submit = JobCommand::submit(201, analytics_job(301), 1_100)
        .unwrap()
        .encode()
        .unwrap();
    let receipt = state.apply_committed(1, 2, &submit).unwrap();

    assert_eq!(receipt.catalog_revision(), 1);
    assert_eq!(receipt.analytics_revision(), 1);
    assert_eq!(state.catalog().revision(), 1);
    assert_eq!(state.analytics().revision(), 1);
    assert_eq!(
        state
            .analytics()
            .job(AnalyticsJobId::new(301).unwrap())
            .unwrap()
            .state(),
        JobState::Queued
    );
    assert_eq!(state.watch_after(0).unwrap().events().len(), 1);

    let snapshot = state.encode_snapshot().unwrap();
    let restored = MetaStateMachine::decode_snapshot(&snapshot, 8).unwrap();
    assert_eq!(restored.catalog(), state.catalog());
    assert_eq!(restored.analytics(), state.analytics());
    assert_eq!(restored.applied_index(), 2);
}

#[test]
fn analytics_gc_epoch_is_raft_applied_and_snapshot_restored() {
    let mut state = MetaStateMachine::new(8).unwrap();
    let first = AnalyticsGcLeaseCommand::new(701, 0, 10, 3, 1, 100, 200).unwrap();
    let first_record = state.apply_committed(3, 1, &first.encode()).unwrap();
    assert!(!first_record.duplicate());
    let lease = state.analytics_gc_lease().unwrap();
    assert_eq!(lease.gateway_id(), 10);
    assert_eq!(lease.owner_term(), 3);
    assert_eq!(lease.gc_epoch(), 1);
    assert_eq!(lease.expires_unix_ms(), 200);
    assert!(
        state
            .apply_committed(3, 2, &first.encode())
            .unwrap()
            .duplicate()
    );

    let second = AnalyticsGcLeaseCommand::new(702, 1, 11, 3, 2, 200, 300).unwrap();
    state.apply_committed(3, 3, &second.encode()).unwrap();
    assert_eq!(state.analytics_gc_lease().unwrap().gc_epoch(), 2);

    let snapshot = state.encode_snapshot().unwrap();
    let restored = MetaStateMachine::decode_snapshot(&snapshot, 8).unwrap();
    assert_eq!(restored.analytics_gc_lease(), state.analytics_gc_lease());
}
