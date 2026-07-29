use dtg_analytics::{
    AlgorithmRequest, AnalyticsArtifact, AnalyticsJobSpec, AnalyticsJobState, AnalyticsLedger,
    AnalyticsRequestIdentity, AnalyticsRuntimeFences, ArtifactKind, BackendGeneration,
    BuiltInAlgorithmId, CancellationToken, Digest32, JobTimestamp, PlacementEpoch, ProjectionSpec,
    ShardId, ShardSnapshotProvenance, SnapshotProvenance, TransactionId, TransactionTime, Version,
    WorkerId,
};

fn time(value: u64) -> JobTimestamp {
    JobTimestamp::new(value)
}

fn worker(value: u64) -> WorkerId {
    WorkerId::new(value).unwrap()
}

fn digest(byte: u8) -> Digest32 {
    Digest32::new([byte; 32])
}

fn spec_with_backend(identity: &str, backend_fence: Digest32) -> AnalyticsJobSpec {
    let snapshot = SnapshotProvenance::new(
        TransactionId::new(7).unwrap(),
        TransactionTime::new(10).unwrap(),
        Version::new(3),
        vec![(
            ShardId::new(1).unwrap(),
            ShardSnapshotProvenance {
                placement_epoch: PlacementEpoch::new(4).unwrap(),
                backend_generation: BackendGeneration::new(5).unwrap(),
                applied_index: 6,
                closed_time: TransactionTime::new(10).unwrap(),
            },
        )],
    )
    .unwrap();
    AnalyticsJobSpec::new(
        AnalyticsRequestIdentity::new(identity.to_owned()).unwrap(),
        AlgorithmRequest::new(BuiltInAlgorithmId::PageRank),
        snapshot,
        ProjectionSpec::new(false, Vec::new(), Vec::new()).unwrap(),
        digest(11),
        backend_fence,
        1,
        1,
        3,
        time(100),
    )
    .unwrap()
}

fn spec(identity: &str) -> AnalyticsJobSpec {
    spec_with_backend(identity, digest(12))
}

#[test]
fn identical_submission_is_idempotent_and_divergent_same_id_is_rejected() {
    let mut ledger = AnalyticsLedger::new(5).unwrap();
    let original = spec("request-1");
    let job_id = ledger.submit(original.clone(), time(1)).unwrap();

    assert_eq!(ledger.submit(original, time(2)).unwrap(), job_id);

    let divergent = spec_with_backend("request-1", digest(99));
    let error = ledger.submit(divergent, time(2)).unwrap_err();
    assert_eq!(error.code(), "DTG-ANALYTICS-SPEC-CONFLICT");
}

#[test]
fn claim_is_revision_and_state_cas_protected() {
    let mut ledger = AnalyticsLedger::new(5).unwrap();
    let job_id = ledger.submit(spec("claim-race"), time(1)).unwrap();
    let queued = ledger.record(job_id).unwrap().cas();

    let lease = ledger.claim(job_id, worker(1), time(2), queued).unwrap();
    let error = ledger
        .claim(job_id, worker(2), time(2), queued)
        .unwrap_err();

    assert_eq!(error.code(), "DTG-ANALYTICS-STALE-REVISION");
    assert_eq!(lease.lease_epoch(), 1);
    assert!(matches!(
        ledger.record(job_id).unwrap().state(),
        AnalyticsJobState::Claimed { worker, .. } if *worker == WorkerId::new(1).unwrap()
    ));
}

#[test]
fn expired_worker_cannot_publish_result() {
    let mut ledger = AnalyticsLedger::new(5).unwrap();
    let job_spec = spec("expired-publish");
    let job_id = ledger.submit(job_spec.clone(), time(1)).unwrap();
    let queued = ledger.record(job_id).unwrap().cas();
    let lease = ledger.claim(job_id, worker(4), time(10), queued).unwrap();
    let lease = ledger.begin(lease, time(10)).unwrap();
    ledger.expire_leases(time(20), 8).unwrap();
    let artifact = AnalyticsArtifact::new(
        &job_spec,
        lease.lease_epoch(),
        1,
        ArtifactKind::Result,
        b"result".to_vec(),
        64,
    )
    .unwrap();

    let error = ledger
        .publish_result(lease, artifact.manifest().clone(), time(20))
        .unwrap_err();
    assert_eq!(error.code(), "DTG-ANALYTICS-STALE-LEASE");
}

#[test]
fn reclaimed_worker_fences_old_lease_and_artifact_generation() {
    let mut ledger = AnalyticsLedger::new(5).unwrap();
    let job_spec = spec("reclaim");
    let job_id = ledger.submit(job_spec.clone(), time(1)).unwrap();
    let first = ledger.record(job_id).unwrap().cas();
    let first = ledger.claim(job_id, worker(1), time(2), first).unwrap();
    let first = ledger.begin(first, time(2)).unwrap();
    let checkpoint = AnalyticsArtifact::new(
        &job_spec,
        first.lease_epoch(),
        1,
        ArtifactKind::Checkpoint,
        b"checkpoint-1".to_vec(),
        64,
    )
    .unwrap();
    let first = ledger
        .checkpoint(first, checkpoint.manifest().clone(), time(3))
        .unwrap();

    ledger.expire_leases(time(9), 8).unwrap();
    let queued = ledger.record(job_id).unwrap().cas();
    let second = ledger.claim(job_id, worker(2), time(10), queued).unwrap();
    let second = ledger.begin(second, time(10)).unwrap();

    let old_worker_artifact = AnalyticsArtifact::new(
        &job_spec,
        first.lease_epoch(),
        2,
        ArtifactKind::Checkpoint,
        b"stale".to_vec(),
        64,
    )
    .unwrap();
    assert_eq!(
        ledger
            .checkpoint(first, old_worker_artifact.manifest().clone(), time(10),)
            .unwrap_err()
            .code(),
        "DTG-ANALYTICS-STALE-LEASE"
    );

    let skipped_generation = AnalyticsArtifact::new(
        &job_spec,
        second.lease_epoch(),
        3,
        ArtifactKind::Result,
        b"skipped".to_vec(),
        64,
    )
    .unwrap();
    assert_eq!(
        ledger
            .publish_result(second, skipped_generation.manifest().clone(), time(10))
            .unwrap_err()
            .code(),
        "DTG-ANALYTICS-STALE-GENERATION"
    );
}

#[test]
fn retries_stop_at_max_attempts_and_cancellation_fences_worker() {
    let mut ledger = AnalyticsLedger::new(5).unwrap();
    let job_spec = spec("retry-limit");
    let job_id = ledger.submit(job_spec.clone(), time(1)).unwrap();
    for attempt in 0..3 {
        let queued = ledger.record(job_id).unwrap().cas();
        let lease = ledger
            .claim(job_id, worker(1), time(2 + attempt * 2), queued)
            .unwrap();
        let lease = ledger.begin(lease, time(2 + attempt * 2)).unwrap();
        ledger
            .fail(lease, true, "TRANSIENT", time(3 + attempt * 2))
            .unwrap();
    }
    assert!(matches!(
        ledger.record(job_id).unwrap().state(),
        AnalyticsJobState::Failed {
            retryable: false,
            code
        } if code == "TRANSIENT"
    ));

    let cancel_id = ledger.submit(spec("cancel"), time(1)).unwrap();
    let queued = ledger.record(cancel_id).unwrap().cas();
    let lease = ledger.claim(cancel_id, worker(2), time(2), queued).unwrap();
    let lease = ledger.begin(lease, time(2)).unwrap();
    let running = ledger.record(cancel_id).unwrap().cas();
    ledger.cancel(cancel_id, running, time(3)).unwrap();
    let result = AnalyticsArtifact::new(
        ledger.record(cancel_id).unwrap().spec(),
        lease.lease_epoch(),
        1,
        ArtifactKind::Result,
        b"cancelled".to_vec(),
        64,
    )
    .unwrap();
    assert_eq!(
        ledger
            .publish_result(lease, result.manifest().clone(), time(3))
            .unwrap_err()
            .code(),
        "DTG-ANALYTICS-STALE-LEASE"
    );
}

#[test]
fn runtime_fence_drift_is_rejected() {
    let job_spec = spec("fence-drift");
    let mut fences: AnalyticsRuntimeFences = job_spec.runtime_fences();
    fences.provider_version += 1;
    assert_eq!(
        job_spec.validate_runtime_fences(fences).unwrap_err().code(),
        "DTG-ANALYTICS-FENCE-DRIFT"
    );
}

#[test]
fn retention_gc_is_budgeted_cancellable_and_preserves_pins() {
    let mut ledger = AnalyticsLedger::new(5).unwrap();
    let job_spec = spec("retention-gc");
    let job_id = ledger.submit(job_spec.clone(), time(1)).unwrap();
    let queued = ledger.record(job_id).unwrap().cas();
    let lease = ledger.claim(job_id, worker(1), time(2), queued).unwrap();
    let lease = ledger.begin(lease, time(2)).unwrap();
    let checkpoint = AnalyticsArtifact::new(
        &job_spec,
        lease.lease_epoch(),
        1,
        ArtifactKind::Checkpoint,
        b"checkpoint".to_vec(),
        64,
    )
    .unwrap();
    let lease = ledger
        .checkpoint(lease, checkpoint.manifest().clone(), time(3))
        .unwrap();
    let result = AnalyticsArtifact::new(
        &job_spec,
        lease.lease_epoch(),
        2,
        ArtifactKind::Result,
        b"result".to_vec(),
        64,
    )
    .unwrap();
    ledger
        .publish_result(lease, result.manifest().clone(), time(4))
        .unwrap();

    let cancelled = CancellationToken::new();
    cancelled.cancel();
    assert_eq!(
        ledger
            .garbage_collect_with_cancellation(time(100), 1, &cancelled)
            .unwrap_err()
            .code(),
        "DTG-ANALYTICS-CANCELLED"
    );

    let reclaimed = ledger
        .garbage_collect_with_cancellation(time(100), 1, &CancellationToken::new())
        .unwrap();
    assert_eq!(reclaimed.len(), 1);
    assert_eq!(reclaimed[0].kind(), ArtifactKind::Checkpoint);
    assert!(
        ledger
            .record(job_id)
            .unwrap()
            .is_pinned(ArtifactKind::Result, 2)
    );

    let succeeded = ledger.record(job_id).unwrap().cas();
    ledger
        .unpin_artifact(job_id, ArtifactKind::Result, 2, succeeded, time(100))
        .unwrap();
    let reclaimed = ledger
        .garbage_collect_with_cancellation(time(100), 1, &CancellationToken::new())
        .unwrap();
    assert_eq!(reclaimed.len(), 1);
    assert_eq!(reclaimed[0].kind(), ArtifactKind::Result);
}

#[test]
fn active_job_cannot_release_its_checkpoint_pin() {
    let mut ledger = AnalyticsLedger::new(5).unwrap();
    let job_spec = spec("active-pin");
    let job_id = ledger.submit(job_spec.clone(), time(1)).unwrap();
    let queued = ledger.record(job_id).unwrap().cas();
    let lease = ledger.claim(job_id, worker(1), time(2), queued).unwrap();
    let lease = ledger.begin(lease, time(2)).unwrap();
    let checkpoint = AnalyticsArtifact::new(
        &job_spec,
        lease.lease_epoch(),
        1,
        ArtifactKind::Checkpoint,
        b"checkpoint".to_vec(),
        64,
    )
    .unwrap();
    ledger
        .checkpoint(lease, checkpoint.manifest().clone(), time(3))
        .unwrap();
    let running = ledger.record(job_id).unwrap().cas();

    assert_eq!(
        ledger
            .unpin_artifact(job_id, ArtifactKind::Checkpoint, 1, running, time(100),)
            .unwrap_err()
            .code(),
        "DTG-ANALYTICS-INVALID-TRANSITION"
    );
    assert!(
        ledger
            .record(job_id)
            .unwrap()
            .is_pinned(ArtifactKind::Checkpoint, 1)
    );
}
