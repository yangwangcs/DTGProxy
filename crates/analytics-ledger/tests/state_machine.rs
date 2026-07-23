use std::collections::BTreeMap;

use analytics_ledger::{
    AnalyticsJobId, ArtifactGeneration, ArtifactGenerationKey, ArtifactKind, ArtifactManifest,
    GraphProjectionScope, JobCommand, JobError, JobSpec, JobState, LedgerState,
    MAX_ARTIFACT_CHUNK_BYTES, MAX_ARTIFACT_CHUNKS, NextExecutionStage, ProjectionLimits,
    RetentionPolicy,
};
use raft_command::MAX_ANALYTICS_ARTIFACT_CHUNK_BYTES;
use temporal_types::{TransactionTime, ValidTime};

fn spec(job: u128) -> JobSpec {
    spec_with_request(job, job + 10_000)
}

fn spec_with_request(job: u128, request: u128) -> JobSpec {
    spec_with_request_and_parameters(job, request, vec![1, 2, 3])
}

fn spec_with_request_and_parameters(job: u128, request: u128, parameters: Vec<u8>) -> JobSpec {
    JobSpec::new(
        AnalyticsJobId::new(job).unwrap(),
        request,
        7,
        11,
        13,
        17,
        19,
        TransactionTime::new(23, 29),
        GraphProjectionScope::Snapshot {
            valid_time: ValidTime::from_micros(31),
        },
        "dtg.graph.pageRank",
        "1.0.0",
        "dtg.analytics-native",
        "1.0.0",
        parameters,
        [37; 32],
        ProjectionLimits::new(100, 200, 300).unwrap(),
    )
    .unwrap()
}

fn spec_with_reallocated_fences(job: u128, request: u128) -> JobSpec {
    JobSpec::new(
        AnalyticsJobId::new(job).unwrap(),
        request,
        7,
        12,
        14,
        18,
        20,
        TransactionTime::new(24, 30),
        GraphProjectionScope::Snapshot {
            valid_time: ValidTime::from_micros(31),
        },
        "dtg.graph.pageRank",
        "1.0.1",
        "dtg.analytics-native",
        "1.0.1",
        vec![1, 2, 3],
        [37; 32],
        ProjectionLimits::new(101, 201, 301).unwrap(),
    )
    .unwrap()
}

fn manifest(kind: ArtifactKind, generation: u64) -> ArtifactManifest {
    ArtifactManifest::new(
        kind,
        generation,
        3,
        4,
        5_000,
        [41; 32],
        [43; 32],
        "1.0.0",
        "1.0.0",
        match kind {
            ArtifactKind::Checkpoint => NextExecutionStage::Provider { completed_units: 4 },
            ArtifactKind::Result => NextExecutionStage::Complete,
        },
        BTreeMap::from([(3, 47), (5, 53)]),
    )
    .unwrap()
}

fn manifest_with_dimensions(
    chunk_count: u64,
    total_bytes: u64,
) -> Result<ArtifactManifest, JobError> {
    ArtifactManifest::new(
        ArtifactKind::Checkpoint,
        1,
        3,
        chunk_count,
        total_bytes,
        [41; 32],
        [43; 32],
        "provider-1",
        "algorithm-1",
        NextExecutionStage::Provider { completed_units: 4 },
        BTreeMap::from([(3, 47), (5, 53)]),
    )
}

fn rewrite_u64_and_checksum(bytes: &mut [u8], offset: usize, value: u64) {
    bytes[offset..offset + 8].copy_from_slice(&value.to_be_bytes());
    let checksum_offset = bytes.len() - 4;
    let checksum = crc32fast::hash(&bytes[..checksum_offset]).to_be_bytes();
    bytes[checksum_offset..].copy_from_slice(&checksum);
}

#[test]
fn retention_plan_protects_pinned_and_respects_count_and_ttl() {
    let mut ledger = LedgerState::new();
    ledger
        .apply(JobCommand::submit(1, spec(1), 1_000).unwrap())
        .unwrap();
    let record = ledger.job(AnalyticsJobId::new(1).unwrap()).unwrap();
    let generations = vec![
        ArtifactGeneration::new(ArtifactKind::Checkpoint, 1, 10, 1, false).unwrap(),
        ArtifactGeneration::new(ArtifactKind::Checkpoint, 2, 20, 2, false).unwrap(),
        ArtifactGeneration::new(ArtifactKind::Checkpoint, 3, 30, 3, false).unwrap(),
        ArtifactGeneration::new(ArtifactKind::Result, 1, 40, 4, true).unwrap(),
        ArtifactGeneration::new(ArtifactKind::Result, 2, 50, 95, false).unwrap(),
    ];
    let plan = RetentionPolicy::new(1, 10_000, 10, 10)
        .unwrap()
        .plan(record, &generations, 100)
        .unwrap();

    assert_eq!(plan.observed_bytes(), 150);
    assert_eq!(plan.reclaim_bytes(), 30);
    assert_eq!(
        plan.deletable(),
        &[
            ArtifactGenerationKey::new(ArtifactKind::Checkpoint, 1),
            ArtifactGenerationKey::new(ArtifactKind::Checkpoint, 2),
        ]
    );
    assert!(
        plan.protected()
            .contains(&ArtifactGenerationKey::new(ArtifactKind::Result, 1))
    );
    assert!(
        !plan
            .deletable()
            .contains(&ArtifactGenerationKey::new(ArtifactKind::Result, 2))
    );
}

#[test]
fn retention_plan_keeps_meta_referenced_generation_even_when_over_byte_limit() {
    let mut ledger = LedgerState::new();
    ledger
        .apply(JobCommand::submit(11, spec(11), 1_000).unwrap())
        .unwrap();
    ledger
        .apply(JobCommand::claim(12, AnalyticsJobId::new(11).unwrap(), 1, 7, 13, 2_000).unwrap())
        .unwrap();
    ledger
        .apply(JobCommand::begin_run(13, AnalyticsJobId::new(11).unwrap(), 2, 7, 1, 13).unwrap())
        .unwrap();
    ledger
        .apply(
            JobCommand::commit_checkpoint(
                14,
                AnalyticsJobId::new(11).unwrap(),
                3,
                7,
                1,
                13,
                manifest(ArtifactKind::Checkpoint, 3),
            )
            .unwrap(),
        )
        .unwrap();
    let record = ledger.job(AnalyticsJobId::new(11).unwrap()).unwrap();
    let generations = vec![
        ArtifactGeneration::new(ArtifactKind::Checkpoint, 1, 10, 1, false).unwrap(),
        ArtifactGeneration::new(ArtifactKind::Checkpoint, 2, 20, 2, false).unwrap(),
        ArtifactGeneration::new(ArtifactKind::Checkpoint, 3, 5_000, 3, false).unwrap(),
    ];
    let plan = RetentionPolicy::new(1, 1, 10, 10)
        .unwrap()
        .plan(record, &generations, 100)
        .unwrap();

    assert!(
        plan.protected()
            .contains(&ArtifactGenerationKey::new(ArtifactKind::Checkpoint, 3))
    );
    assert_eq!(
        plan.deletable(),
        &[
            ArtifactGenerationKey::new(ArtifactKind::Checkpoint, 1),
            ArtifactGenerationKey::new(ArtifactKind::Checkpoint, 2),
        ]
    );
}

#[test]
fn maintenance_job_listing_is_bounded_and_canonically_paginated() {
    let mut ledger = LedgerState::new();
    for job in 21..=23 {
        ledger
            .apply(JobCommand::submit(job + 100, spec(job), 1_000 + job as u64).unwrap())
            .unwrap();
    }

    assert_eq!(
        ledger.list_jobs(None, 2).unwrap(),
        vec![
            AnalyticsJobId::new(21).unwrap(),
            AnalyticsJobId::new(22).unwrap(),
        ]
    );
    assert_eq!(
        ledger
            .list_jobs(Some(AnalyticsJobId::new(22).unwrap()), 2)
            .unwrap(),
        vec![AnalyticsJobId::new(23).unwrap()]
    );
    assert_eq!(
        ledger.list_jobs(None, 0),
        Err(JobError::InvalidJobListRequest)
    );
}

#[test]
fn artifact_manifests_enforce_current_shard_bounds() {
    let max_chunk_bytes = u64::try_from(MAX_ANALYTICS_ARTIFACT_CHUNK_BYTES).unwrap();

    assert!(manifest_with_dimensions(1, 1).is_ok());
    assert!(manifest_with_dimensions(4_096, 4_096 * max_chunk_bytes).is_ok());

    for (chunk_count, total_bytes) in [
        (4_097, 4_097),
        (2, 1),
        (1, max_chunk_bytes + 1),
        (u64::MAX, u64::MAX),
    ] {
        assert_eq!(
            manifest_with_dimensions(chunk_count, total_bytes).unwrap_err(),
            JobError::InvalidArtifactManifest
        );
    }
}

#[test]
fn artifact_manifest_codec_rejects_bounds_violations_in_checksum_valid_frames() {
    let command = JobCommand::commit_checkpoint(
        901,
        AnalyticsJobId::new(1).unwrap(),
        1,
        61,
        1,
        13,
        manifest_with_dimensions(2, 2).unwrap(),
    )
    .unwrap();
    let encoded = command.encode().unwrap();

    // Command header (22) + tag (1) + lease fence (48) + manifest kind/generation/shard (13).
    const CHUNK_COUNT_OFFSET: usize = 84;
    const TOTAL_BYTES_OFFSET: usize = CHUNK_COUNT_OFFSET + 8;
    let max_chunk_bytes = u64::try_from(MAX_ANALYTICS_ARTIFACT_CHUNK_BYTES).unwrap();
    for (offset, value) in [
        (CHUNK_COUNT_OFFSET, 4_097),
        (TOTAL_BYTES_OFFSET, 1),
        (TOTAL_BYTES_OFFSET, max_chunk_bytes * 2 + 1),
        (CHUNK_COUNT_OFFSET, u64::MAX),
    ] {
        let mut corrupt = encoded.clone();
        rewrite_u64_and_checksum(&mut corrupt, offset, value);
        assert_eq!(
            JobCommand::decode(&corrupt).unwrap_err(),
            JobError::InvalidArtifactManifest
        );
    }
}

#[test]
fn artifact_manifest_exposes_scheduler_metadata() {
    assert_eq!(MAX_ARTIFACT_CHUNKS, 4_096);
    assert_eq!(
        MAX_ARTIFACT_CHUNK_BYTES,
        u64::try_from(MAX_ANALYTICS_ARTIFACT_CHUNK_BYTES).unwrap()
    );

    let manifest = manifest_with_dimensions(1, 1).unwrap();
    assert_eq!(manifest.kind(), ArtifactKind::Checkpoint);
    assert_eq!(manifest.generation(), 1);
    assert_eq!(manifest.storage_shard_id(), 3);
    assert_eq!(manifest.chunk_count(), 1);
    assert_eq!(manifest.total_bytes(), 1);
    assert_eq!(manifest.content_digest(), [41; 32]);
    assert_eq!(manifest.projection_identity(), [43; 32]);
    assert_eq!(manifest.provider_version(), "provider-1");
    assert_eq!(manifest.algorithm_version(), "algorithm-1");
    assert_eq!(
        manifest.next_stage(),
        NextExecutionStage::Provider { completed_units: 4 }
    );
    assert_eq!(manifest.input_applied_index_count(), 2);
    assert_ne!(manifest.input_applied_indexes_digest(), [0; 32]);
}

#[test]
fn leased_job_checkpoints_and_publishes_only_under_the_current_fence() {
    let mut ledger = LedgerState::new();
    let job = AnalyticsJobId::new(1).unwrap();
    ledger
        .apply(JobCommand::submit(101, spec(1), 1_000).unwrap())
        .unwrap();
    ledger
        .apply(JobCommand::claim(102, job, 1, 61, 13, 2_000).unwrap())
        .unwrap();
    ledger
        .apply(JobCommand::begin_run(103, job, 2, 61, 1, 13).unwrap())
        .unwrap();
    ledger
        .apply(
            JobCommand::commit_checkpoint(
                104,
                job,
                3,
                61,
                1,
                13,
                manifest(ArtifactKind::Checkpoint, 1),
            )
            .unwrap(),
        )
        .unwrap();
    ledger
        .apply(
            JobCommand::publish_result(105, job, 4, 61, 1, 13, manifest(ArtifactKind::Result, 2))
                .unwrap(),
        )
        .unwrap();

    let record = ledger.job(job).unwrap();
    assert_eq!(record.state(), JobState::Succeeded);
    assert_eq!(record.job_revision(), 5);
    assert_eq!(record.checkpoint().unwrap().generation(), 1);
    assert_eq!(record.result().unwrap().generation(), 2);
    assert_eq!(
        ledger
            .apply(JobCommand::fail(106, job, 5, 61, 1, 13, "late", "late").unwrap())
            .unwrap_err(),
        JobError::TerminalJob
    );
}

#[test]
fn expired_lease_is_requeued_and_takeover_increments_the_fence() {
    let mut ledger = LedgerState::new();
    let job = AnalyticsJobId::new(2).unwrap();
    ledger
        .apply(JobCommand::submit(201, spec(2), 1_000).unwrap())
        .unwrap();
    ledger
        .apply(JobCommand::claim(202, job, 1, 71, 13, 2_000).unwrap())
        .unwrap();
    assert_eq!(
        ledger
            .apply(JobCommand::expire_lease(203, job, 2, 1, 1_999).unwrap())
            .unwrap_err(),
        JobError::LeaseActive
    );
    ledger
        .apply(JobCommand::expire_lease(204, job, 2, 1, 2_000).unwrap())
        .unwrap();
    ledger
        .apply(JobCommand::claim(205, job, 3, 73, 13, 3_000).unwrap())
        .unwrap();

    let record = ledger.job(job).unwrap();
    assert_eq!(record.state(), JobState::Leased);
    assert_eq!(record.lease().unwrap().owner_gateway_id(), 73);
    assert_eq!(record.lease().unwrap().lease_epoch(), 2);
    assert_eq!(
        ledger
            .apply(JobCommand::begin_run(206, job, 4, 71, 1, 13).unwrap())
            .unwrap_err(),
        JobError::StaleLease
    );
}

#[test]
fn command_replay_and_snapshot_codec_are_canonical() {
    let mut ledger = LedgerState::new();
    let command = JobCommand::submit(301, spec(3), 1_000).unwrap();
    let encoded = command.encode().unwrap();
    assert_eq!(JobCommand::decode(&encoded).unwrap(), command);

    let first = ledger.apply(command.clone()).unwrap();
    let duplicate = ledger.apply(command).unwrap();
    assert!(!first.duplicate());
    assert!(duplicate.duplicate());
    assert_eq!(
        ledger
            .apply(JobCommand::submit(301, spec(4), 1_000).unwrap())
            .unwrap_err(),
        JobError::CommandReplayMismatch { command_id: 301 }
    );

    let snapshot = ledger.encode_snapshot().unwrap();
    assert_eq!(LedgerState::decode_snapshot(&snapshot).unwrap(), ledger);
    let mut corrupt = snapshot;
    let corrupt_offset = corrupt.len() / 2;
    corrupt[corrupt_offset] ^= 0x01;
    assert_eq!(
        LedgerState::decode_snapshot(&corrupt).unwrap_err(),
        JobError::ChecksumMismatch
    );
}

#[test]
fn cluster_job_ids_use_only_the_latest_canonical_hex_format() {
    let id = AnalyticsJobId::new(0x1234).unwrap();
    let encoded = id.to_string();

    assert_eq!(encoded, "00000000000000000000000000001234");
    assert_eq!(encoded.parse::<AnalyticsJobId>().unwrap(), id);
    assert!("17:42".parse::<AnalyticsJobId>().is_err());
    assert!(
        "0000000000000000000000000000ABCD"
            .parse::<AnalyticsJobId>()
            .is_err()
    );
}

#[test]
fn renew_cancel_and_topology_fences_are_durable_state_transitions() {
    let mut ledger = LedgerState::new();
    let job = AnalyticsJobId::new(5).unwrap();
    ledger
        .apply(JobCommand::submit(501, spec(5), 1_000).unwrap())
        .unwrap();
    ledger
        .apply(JobCommand::claim(502, job, 1, 81, 13, 2_000).unwrap())
        .unwrap();
    let before_stale_topology = ledger.clone();
    assert_eq!(
        ledger
            .apply(JobCommand::begin_run(503, job, 2, 81, 1, 99).unwrap())
            .unwrap_err(),
        JobError::StaleTopology {
            expected: 13,
            actual: 99,
        }
    );
    assert_eq!(ledger, before_stale_topology);

    ledger
        .apply(JobCommand::renew(504, job, 2, 81, 1, 13, 3_000).unwrap())
        .unwrap();
    ledger
        .apply(JobCommand::begin_run(505, job, 3, 81, 1, 13).unwrap())
        .unwrap();
    ledger
        .apply(JobCommand::cancel(506, job, 4).unwrap())
        .unwrap();
    assert_eq!(ledger.job(job).unwrap().state(), JobState::Canceled);
    assert_eq!(ledger.job(job).unwrap().lease(), None);
    assert_eq!(
        ledger
            .apply(
                JobCommand::commit_checkpoint(
                    507,
                    job,
                    5,
                    81,
                    1,
                    13,
                    manifest(ArtifactKind::Checkpoint, 1),
                )
                .unwrap(),
            )
            .unwrap_err(),
        JobError::TerminalJob
    );

    let restored = LedgerState::decode_snapshot(&ledger.encode_snapshot().unwrap()).unwrap();
    assert_eq!(restored.job(job).unwrap().state(), JobState::Canceled);
}

#[test]
fn competing_claims_and_cancel_complete_races_have_one_cas_winner() {
    let mut claimed = LedgerState::new();
    let job = AnalyticsJobId::new(6).unwrap();
    claimed
        .apply(JobCommand::submit(601, spec(6), 1_000).unwrap())
        .unwrap();
    claimed
        .apply(JobCommand::claim(602, job, 1, 91, 13, 2_000).unwrap())
        .unwrap();
    let after_first_claim = claimed.clone();
    assert_eq!(
        claimed
            .apply(JobCommand::claim(603, job, 1, 93, 13, 2_000).unwrap())
            .unwrap_err(),
        JobError::StaleJobRevision {
            expected: 2,
            actual: 1,
        }
    );
    assert_eq!(claimed, after_first_claim);
    claimed
        .apply(JobCommand::begin_run(604, job, 2, 91, 1, 13).unwrap())
        .unwrap();

    let mut complete_first = claimed.clone();
    complete_first
        .apply(
            JobCommand::publish_result(605, job, 3, 91, 1, 13, manifest(ArtifactKind::Result, 1))
                .unwrap(),
        )
        .unwrap();
    assert!(matches!(
        complete_first
            .apply(JobCommand::cancel(606, job, 3).unwrap())
            .unwrap_err(),
        JobError::StaleJobRevision { .. }
    ));

    let mut cancel_first = claimed;
    cancel_first
        .apply(JobCommand::cancel(607, job, 3).unwrap())
        .unwrap();
    assert!(matches!(
        cancel_first
            .apply(
                JobCommand::publish_result(
                    608,
                    job,
                    3,
                    91,
                    1,
                    13,
                    manifest(ArtifactKind::Result, 1),
                )
                .unwrap(),
            )
            .unwrap_err(),
        JobError::StaleJobRevision { .. }
    ));
}

#[test]
fn fail_and_artifact_compatibility_are_persisted() {
    let mut ledger = LedgerState::new();
    let job = AnalyticsJobId::new(7).unwrap();
    ledger
        .apply(JobCommand::submit(701, spec(7), 1_000).unwrap())
        .unwrap();
    ledger
        .apply(JobCommand::claim(702, job, 1, 101, 13, 2_000).unwrap())
        .unwrap();
    ledger
        .apply(JobCommand::begin_run(703, job, 2, 101, 1, 13).unwrap())
        .unwrap();
    ledger
        .apply(
            JobCommand::commit_checkpoint(
                704,
                job,
                3,
                101,
                1,
                13,
                manifest(ArtifactKind::Checkpoint, 1),
            )
            .unwrap(),
        )
        .unwrap();
    let incompatible = ArtifactManifest::new(
        ArtifactKind::Result,
        2,
        3,
        4,
        5_000,
        [41; 32],
        [99; 32],
        "1.0.0",
        "1.0.0",
        NextExecutionStage::Complete,
        BTreeMap::from([(3, 47), (5, 53)]),
    )
    .unwrap();
    assert_eq!(
        ledger
            .apply(JobCommand::publish_result(705, job, 4, 101, 1, 13, incompatible).unwrap(),)
            .unwrap_err(),
        JobError::IncompatibleArtifact
    );
    ledger
        .apply(JobCommand::fail(706, job, 4, 101, 1, 13, "DTG-X", "failed").unwrap())
        .unwrap();
    assert_eq!(ledger.job(job).unwrap().state(), JobState::Failed);
    assert_eq!(
        LedgerState::decode_snapshot(&ledger.encode_snapshot().unwrap())
            .unwrap()
            .job(job)
            .unwrap()
            .state(),
        JobState::Failed
    );
}

#[test]
fn every_ledger_command_has_one_canonical_round_trip() {
    let job = AnalyticsJobId::new(8).unwrap();
    let commands = vec![
        JobCommand::submit(801, spec(8), 1_000).unwrap(),
        JobCommand::claim(802, job, 1, 111, 13, 2_000).unwrap(),
        JobCommand::renew(803, job, 2, 111, 1, 13, 3_000).unwrap(),
        JobCommand::begin_run(804, job, 3, 111, 1, 13).unwrap(),
        JobCommand::commit_checkpoint(
            805,
            job,
            4,
            111,
            1,
            13,
            manifest(ArtifactKind::Checkpoint, 1),
        )
        .unwrap(),
        JobCommand::publish_result(806, job, 5, 111, 1, 13, manifest(ArtifactKind::Result, 2))
            .unwrap(),
        JobCommand::fail(807, job, 5, 111, 1, 13, "DTG-X", "failure").unwrap(),
        JobCommand::cancel(808, job, 5).unwrap(),
        JobCommand::expire_lease(809, job, 5, 1, 3_000).unwrap(),
        JobCommand::prune_terminal(810, job, 5, 3_000).unwrap(),
        JobCommand::compact_tombstones(811, 1_000).unwrap(),
        JobCommand::acknowledge_artifacts_reclaimed(812, job, 5, 111, 1, 3_001).unwrap(),
    ];

    for command in commands {
        assert_eq!(
            JobCommand::decode(&command.encode().unwrap()).unwrap(),
            command
        );
    }
}

#[test]
fn submission_request_id_is_the_cluster_idempotency_key() {
    let mut ledger = LedgerState::new();
    let first = ledger
        .apply(JobCommand::submit(901, spec_with_request(9, 50_000), 1_000).unwrap())
        .unwrap();
    let retry = ledger
        .apply(JobCommand::submit(902, spec_with_request(10, 50_000), 1_000).unwrap())
        .unwrap();

    assert!(!first.request_duplicate());
    assert!(retry.request_duplicate());
    assert_eq!(
        ledger.job_for_submission(50_000),
        Some(AnalyticsJobId::new(9).unwrap())
    );
    assert!(ledger.job(AnalyticsJobId::new(9).unwrap()).is_some());
    assert!(ledger.job(AnalyticsJobId::new(10).unwrap()).is_none());
    assert_eq!(
        ledger
            .apply(
                JobCommand::submit(
                    903,
                    spec_with_request_and_parameters(11, 50_000, vec![9]),
                    1_001,
                )
                .unwrap(),
            )
            .unwrap_err(),
        JobError::SubmissionReplayMismatch {
            submission_request_id: 50_000,
        }
    );
}

#[test]
fn submission_retry_keeps_the_first_durable_execution_fences() {
    let mut ledger = LedgerState::new();
    ledger
        .apply(JobCommand::submit(911, spec_with_request(19, 60_000), 1_000).unwrap())
        .unwrap();
    let retry = ledger
        .apply(JobCommand::submit(912, spec_with_reallocated_fences(20, 60_000), 1_001).unwrap())
        .unwrap();

    assert!(retry.request_duplicate());
    assert_eq!(
        ledger.job_for_submission(60_000),
        Some(AnalyticsJobId::new(19).unwrap())
    );
    let original = ledger.job(AnalyticsJobId::new(19).unwrap()).unwrap();
    assert_eq!(original.spec().catalog_revision(), 11);
    assert_eq!(
        original.spec().transaction_time(),
        TransactionTime::new(23, 29)
    );
    assert!(ledger.job(AnalyticsJobId::new(20).unwrap()).is_none());
}

#[test]
fn terminal_job_pruning_releases_capacity_without_reviving_the_job() {
    let mut ledger = LedgerState::new();
    let job = AnalyticsJobId::new(12).unwrap();
    ledger
        .apply(JobCommand::submit(1_201, spec(12), 1_000).unwrap())
        .unwrap();
    assert_eq!(
        ledger
            .apply(JobCommand::compact_tombstones(1_204, 1_000).unwrap())
            .unwrap_err(),
        JobError::ActiveSubmissionBeforeCompactionFloor
    );
    ledger
        .apply(JobCommand::cancel(1_202, job, 1).unwrap())
        .unwrap();
    ledger
        .apply(JobCommand::prune_terminal(1_203, job, 2, 2_000).unwrap())
        .unwrap();
    ledger
        .apply(JobCommand::acknowledge_artifacts_reclaimed(1_205, job, 2, 7, 1, 2_000).unwrap())
        .unwrap();
    ledger
        .apply(JobCommand::compact_tombstones(1_206, 2_000).unwrap())
        .unwrap();

    assert!(ledger.job(job).is_none());
    assert_eq!(
        LedgerState::decode_snapshot(&ledger.encode_snapshot().unwrap()).unwrap(),
        ledger
    );
}

#[test]
fn terminal_prune_creates_a_durable_unreclaimed_tombstone() {
    let mut ledger = LedgerState::new();
    let job = AnalyticsJobId::new(13).unwrap();
    ledger
        .apply(JobCommand::submit(1_301, spec(13), 1_000).unwrap())
        .unwrap();
    assert_eq!(
        ledger
            .apply(JobCommand::prune_terminal(1_300, job, 1, 2_000).unwrap())
            .unwrap_err(),
        JobError::InvalidStateTransition
    );
    ledger
        .apply(JobCommand::cancel(1_302, job, 1).unwrap())
        .unwrap();
    ledger
        .apply(JobCommand::prune_terminal(1_303, job, 2, 2_000).unwrap())
        .unwrap();

    let tombstone = ledger.tombstone(job).expect("terminal prune tombstone");
    assert_eq!(tombstone.final_job_revision(), 2);
    assert_eq!(tombstone.terminal_state(), JobState::Canceled);
    assert_eq!(tombstone.pruned_at_unix_ms(), 2_000);
    assert!(!tombstone.artifacts_reclaimed());
    assert_eq!(tombstone.reclaimed_gc_epoch(), None);
    assert_eq!(ledger.list_tombstones(None, 8).unwrap(), vec![job]);

    let restored = LedgerState::decode_snapshot(&ledger.encode_snapshot().unwrap()).unwrap();
    assert_eq!(restored.tombstone(job), Some(tombstone));
    let encoded = ledger.encode_tombstone(job).unwrap();
    let (revision, decoded_job, decoded_tombstone) =
        LedgerState::decode_tombstone(&encoded).unwrap();
    assert_eq!(revision, ledger.revision());
    assert_eq!(decoded_job, job);
    assert_eq!(&decoded_tombstone, tombstone);
    assert_eq!(
        ledger
            .apply(JobCommand::compact_tombstones(1_304, 2_000).unwrap())
            .unwrap_err(),
        JobError::UnreclaimedTombstoneBeforeCompactionFloor
    );
}

#[test]
fn artifact_reclamation_acknowledgement_is_revision_fenced_and_compactable() {
    let mut ledger = LedgerState::new();
    let job = AnalyticsJobId::new(14).unwrap();
    ledger
        .apply(JobCommand::submit(1_401, spec(14), 1_000).unwrap())
        .unwrap();
    ledger
        .apply(JobCommand::cancel(1_402, job, 1).unwrap())
        .unwrap();
    ledger
        .apply(JobCommand::prune_terminal(1_403, job, 2, 2_000).unwrap())
        .unwrap();

    assert_eq!(
        ledger
            .apply(
                JobCommand::acknowledge_artifacts_reclaimed(1_404, job, 1, 77, 9, 3_000,).unwrap(),
            )
            .unwrap_err(),
        JobError::StaleTombstoneRevision {
            expected: 2,
            actual: 1,
        }
    );
    ledger
        .apply(JobCommand::acknowledge_artifacts_reclaimed(1_405, job, 2, 77, 9, 3_000).unwrap())
        .unwrap();
    let tombstone = ledger.tombstone(job).unwrap();
    assert!(tombstone.artifacts_reclaimed());
    assert_eq!(tombstone.reclaimed_gc_epoch(), Some(9));
    assert_eq!(tombstone.reclaimed_at_unix_ms(), Some(3_000));
    assert_eq!(
        ledger
            .apply(
                JobCommand::acknowledge_artifacts_reclaimed(1_407, job, 2, 77, 8, 3_001).unwrap(),
            )
            .unwrap_err(),
        JobError::StaleGcEpoch
    );

    ledger
        .apply(JobCommand::compact_tombstones(1_406, 3_000).unwrap())
        .unwrap();
    assert!(ledger.tombstone(job).is_none());
}
