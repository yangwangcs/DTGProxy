use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};

use dtg_analytics::{
    AlgorithmRequest, AnalyticsAlgorithmStep, AnalyticsArtifact, AnalyticsArtifactGarbageCollector,
    AnalyticsArtifactIoBudget, AnalyticsArtifactRepository, AnalyticsJobSpec, AnalyticsJobState,
    AnalyticsLedger, AnalyticsProjection, AnalyticsProjectionProvider, AnalyticsRequestIdentity,
    AnalyticsScheduler, AnalyticsSchedulerConfig, AnalyticsSchedulerTick, AnalyticsStepProvider,
    ArtifactKind, BackendGeneration, BuiltInAlgorithmId, CancellationToken, Digest32, JobTimestamp,
    PartitionProvenance, PlacementEpoch, ProjectionBudget, ProjectionSpec, SchedulerFailure,
    ShardId, ShardProjectionPart, ShardSnapshotProvenance, SnapshotCsr, SnapshotProvenance,
    StorageArtifactRepository, TransactionId, TransactionTime, Version, WorkerId,
};
use dtg_storage::{
    ArtifactChunk, ArtifactKey, ArtifactManifest, ArtifactStore, BindingRole, ProviderKind,
    ReplicaBinding, StorageError, StoreFuture,
};

fn time(value: u64) -> JobTimestamp {
    JobTimestamp::new(value)
}

fn digest(byte: u8) -> Digest32 {
    Digest32::new([byte; 32])
}

fn spec(identity: &str) -> AnalyticsJobSpec {
    let snapshot = SnapshotProvenance::new(
        TransactionId::new(17).unwrap(),
        TransactionTime::new(20).unwrap(),
        Version::new(4),
        vec![(
            ShardId::new(2).unwrap(),
            ShardSnapshotProvenance {
                placement_epoch: PlacementEpoch::new(7).unwrap(),
                backend_generation: BackendGeneration::new(8).unwrap(),
                applied_index: 9,
                closed_time: TransactionTime::new(20).unwrap(),
            },
        )],
    )
    .unwrap();
    AnalyticsJobSpec::new(
        AnalyticsRequestIdentity::new(identity.to_owned()).unwrap(),
        AlgorithmRequest::new(BuiltInAlgorithmId::PageRank),
        snapshot,
        ProjectionSpec::new(false, Vec::new(), Vec::new()).unwrap(),
        digest(21),
        digest(22),
        1,
        1,
        3,
        time(100),
    )
    .unwrap()
}

#[test]
fn ledger_snapshot_restores_queued_running_checkpoint_and_succeeded_jobs() {
    let mut ledger = AnalyticsLedger::new(5).unwrap();
    let queued = ledger.submit(spec("queued"), time(1)).unwrap();

    let running_spec = spec("running");
    let running = ledger.submit(running_spec.clone(), time(1)).unwrap();
    let running_cas = ledger.record(running).unwrap().cas();
    let running_lease = ledger
        .claim(running, WorkerId::new(1).unwrap(), time(2), running_cas)
        .unwrap();
    let running_lease = ledger.begin(running_lease, time(2)).unwrap();
    let checkpoint = AnalyticsArtifact::new(
        &running_spec,
        running_lease.lease_epoch(),
        1,
        ArtifactKind::Checkpoint,
        b"checkpoint".to_vec(),
        64,
    )
    .unwrap();
    ledger
        .checkpoint(running_lease, checkpoint.manifest().clone(), time(3))
        .unwrap();

    let succeeded_spec = spec("succeeded");
    let succeeded = ledger.submit(succeeded_spec.clone(), time(1)).unwrap();
    let succeeded_cas = ledger.record(succeeded).unwrap().cas();
    let succeeded_lease = ledger
        .claim(succeeded, WorkerId::new(2).unwrap(), time(2), succeeded_cas)
        .unwrap();
    let succeeded_lease = ledger.begin(succeeded_lease, time(2)).unwrap();
    let result = AnalyticsArtifact::new(
        &succeeded_spec,
        succeeded_lease.lease_epoch(),
        1,
        ArtifactKind::Result,
        b"result".to_vec(),
        64,
    )
    .unwrap();
    ledger
        .publish_result(succeeded_lease, result.manifest().clone(), time(3))
        .unwrap();

    let bytes = ledger.encode_snapshot();
    let restored = AnalyticsLedger::decode_snapshot(&bytes).unwrap();

    assert_eq!(restored.encode_snapshot(), bytes);

    assert!(matches!(
        restored.record(queued).unwrap().state(),
        AnalyticsJobState::Queued
    ));
    assert!(matches!(
        restored.record(running).unwrap().state(),
        AnalyticsJobState::Running {
            checkpoint: Some(1),
            ..
        }
    ));
    assert!(matches!(
        restored.record(succeeded).unwrap().state(),
        AnalyticsJobState::Succeeded {
            result_generation: 1
        }
    ));
}

#[test]
fn corrupt_or_unknown_ledger_snapshots_are_rejected() {
    let ledger = AnalyticsLedger::new(5).unwrap();
    let mut unknown = ledger.encode_snapshot();
    unknown[4..8].copy_from_slice(&2_u32.to_be_bytes());
    assert_eq!(
        AnalyticsLedger::decode_snapshot(&unknown)
            .unwrap_err()
            .code(),
        "DTG-ANALYTICS-LEDGER-VERSION"
    );

    let mut corrupt = ledger.encode_snapshot();
    corrupt.push(1);
    assert_eq!(
        AnalyticsLedger::decode_snapshot(&corrupt)
            .unwrap_err()
            .code(),
        "DTG-ANALYTICS-LEDGER-CORRUPT"
    );
}

#[test]
fn terminal_job_can_be_tombstoned_only_after_result_is_unpinned() {
    let mut ledger = AnalyticsLedger::new(5).unwrap();
    let job_spec = spec("tombstone");
    let job_id = ledger.submit(job_spec.clone(), time(1)).unwrap();
    let cas = ledger.record(job_id).unwrap().cas();
    let lease = ledger
        .claim(job_id, WorkerId::new(3).unwrap(), time(2), cas)
        .unwrap();
    let lease = ledger.begin(lease, time(2)).unwrap();
    let result = AnalyticsArtifact::new(
        &job_spec,
        lease.lease_epoch(),
        1,
        ArtifactKind::Result,
        b"result".to_vec(),
        64,
    )
    .unwrap();
    ledger
        .publish_result(lease, result.manifest().clone(), time(3))
        .unwrap();

    let pinned = ledger.record(job_id).unwrap().cas();
    assert_eq!(
        ledger
            .tombstone(job_id, pinned, time(100))
            .unwrap_err()
            .code(),
        "DTG-ANALYTICS-ARTIFACT-PINNED"
    );

    let succeeded = ledger.record(job_id).unwrap().cas();
    ledger
        .unpin_artifact(job_id, ArtifactKind::Result, 1, succeeded, time(100))
        .unwrap();
    let unpinned = ledger.record(job_id).unwrap().cas();
    ledger.tombstone(job_id, unpinned, time(100)).unwrap();
    assert!(matches!(
        ledger.record(job_id).unwrap().state(),
        AnalyticsJobState::Tombstoned
    ));
    let tombstoned = ledger.record(job_id).unwrap().cas();
    assert_eq!(
        ledger
            .pin_artifact(job_id, ArtifactKind::Result, 1, tombstoned, time(101),)
            .unwrap_err()
            .code(),
        "DTG-ANALYTICS-INVALID-TRANSITION"
    );
}

#[derive(Debug)]
struct MemoryArtifactStore {
    binding: ReplicaBinding,
    chunks: Mutex<BTreeMap<(ArtifactKey, u64), ArtifactChunk>>,
    manifests: Mutex<BTreeMap<ArtifactKey, ArtifactManifest>>,
}

impl MemoryArtifactStore {
    fn new() -> Self {
        Self {
            binding: ReplicaBinding::builder()
                .cluster_id(1)
                .graph_id(1)
                .shard_id(1)
                .placement_epoch(1)
                .replica_id(1)
                .backend_generation(1)
                .backend_class_digest(digest(31))
                .provider_kind(ProviderKind::Fjall)
                .contract_version(1)
                .layout_version(1)
                .capability_digest(digest(32))
                .namespace_id("analytics")
                .endpoint_profile_ref("local")
                .credential_ref("none")
                .role(BindingRole::Active)
                .build()
                .unwrap(),
            chunks: Mutex::new(BTreeMap::new()),
            manifests: Mutex::new(BTreeMap::new()),
        }
    }

    fn validate(&self, binding: &ReplicaBinding) -> Result<(), StorageError> {
        if binding == &self.binding {
            Ok(())
        } else {
            Err(StorageError::StaleBinding {
                expected: Box::new(self.binding.clone()),
                actual: Box::new(binding.clone()),
            })
        }
    }
}

impl ArtifactStore for MemoryArtifactStore {
    fn binding(&self) -> &ReplicaBinding {
        &self.binding
    }

    fn put_chunk(&self, binding: ReplicaBinding, chunk: ArtifactChunk) -> StoreFuture<'_, ()> {
        Box::pin(async move {
            self.validate(&binding)?;
            self.chunks
                .lock()
                .unwrap()
                .insert((chunk.key(), chunk.ordinal()), chunk);
            Ok(())
        })
    }

    fn get_chunk(
        &self,
        binding: ReplicaBinding,
        key: ArtifactKey,
        ordinal: u64,
    ) -> StoreFuture<'_, Option<ArtifactChunk>> {
        Box::pin(async move {
            self.validate(&binding)?;
            Ok(self.chunks.lock().unwrap().get(&(key, ordinal)).cloned())
        })
    }

    fn commit_manifest(
        &self,
        binding: ReplicaBinding,
        manifest: ArtifactManifest,
    ) -> StoreFuture<'_, ()> {
        Box::pin(async move {
            self.validate(&binding)?;
            self.manifests
                .lock()
                .unwrap()
                .insert(manifest.key(), manifest);
            Ok(())
        })
    }

    fn manifest(
        &self,
        binding: ReplicaBinding,
        key: ArtifactKey,
    ) -> StoreFuture<'_, Option<ArtifactManifest>> {
        Box::pin(async move {
            self.validate(&binding)?;
            Ok(self.manifests.lock().unwrap().get(&key).cloned())
        })
    }

    fn delete(&self, binding: ReplicaBinding, key: ArtifactKey) -> StoreFuture<'_, ()> {
        Box::pin(async move {
            self.validate(&binding)?;
            self.manifests.lock().unwrap().remove(&key);
            self.chunks
                .lock()
                .unwrap()
                .retain(|(chunk_key, _), _| *chunk_key != key);
            Ok(())
        })
    }
}

#[test]
fn storage_repository_round_trips_typed_chunks_and_manifest() {
    let job_spec = spec("artifact-store");
    let artifact = AnalyticsArtifact::new(
        &job_spec,
        1,
        1,
        ArtifactKind::Checkpoint,
        b"durable-checkpoint".to_vec(),
        7,
    )
    .unwrap();
    let store = Arc::new(MemoryArtifactStore::new());
    let repository = StorageArtifactRepository::new(store.clone());

    repository.persist(&artifact).unwrap();
    let restored = repository
        .load(job_spec.id(), 1, ArtifactKind::Checkpoint)
        .unwrap()
        .unwrap();

    assert_eq!(restored.manifest(), artifact.manifest());
    assert_eq!(restored.payload(), b"durable-checkpoint");
    assert_eq!(
        store.manifests.lock().unwrap().len(),
        1,
        "the adapter must commit the typed storage manifest"
    );
    assert!(
        store.chunks.lock().unwrap().len() > 1,
        "the adapter must stream typed storage chunks"
    );
}

#[test]
fn storage_repository_enforces_chunk_budget_and_cancellation() {
    let job_spec = spec("artifact-budget");
    let artifact = AnalyticsArtifact::new(
        &job_spec,
        1,
        1,
        ArtifactKind::Checkpoint,
        b"durable-checkpoint".to_vec(),
        7,
    )
    .unwrap();
    let store = Arc::new(MemoryArtifactStore::new());
    let repository = StorageArtifactRepository::new(store.clone());

    assert_eq!(
        repository
            .persist_controlled(
                &artifact,
                &AnalyticsArtifactIoBudget::new(1, CancellationToken::new()).unwrap(),
            )
            .unwrap_err()
            .code(),
        "DTG-ANALYTICS-BUDGET-EXHAUSTED"
    );
    assert!(store.chunks.lock().unwrap().is_empty());

    let cancellation = CancellationToken::new();
    cancellation.cancel();
    assert_eq!(
        repository
            .persist_controlled(
                &artifact,
                &AnalyticsArtifactIoBudget::new(100, cancellation).unwrap(),
            )
            .unwrap_err()
            .code(),
        "DTG-ANALYTICS-CANCELLED"
    );
    assert!(store.chunks.lock().unwrap().is_empty());
}

#[test]
fn artifact_gc_deletes_storage_before_acknowledging_ledger_reclamation() {
    let mut ledger = AnalyticsLedger::new(5).unwrap();
    let job_spec = spec("artifact-gc");
    let job_id = ledger.submit(job_spec.clone(), time(1)).unwrap();
    let queued = ledger.record(job_id).unwrap().cas();
    let lease = ledger
        .claim(job_id, WorkerId::new(1).unwrap(), time(2), queued)
        .unwrap();
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
    let store = Arc::new(MemoryArtifactStore::new());
    let repository = StorageArtifactRepository::new(store.clone());
    repository.persist(&checkpoint).unwrap();
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
    repository.persist(&result).unwrap();
    ledger
        .publish_result(lease, result.manifest().clone(), time(4))
        .unwrap();
    let succeeded = ledger.record(job_id).unwrap().cas();
    ledger
        .unpin_artifact(job_id, ArtifactKind::Result, 2, succeeded, time(100))
        .unwrap();

    let collector = AnalyticsArtifactGarbageCollector::new(repository, 16).unwrap();
    assert_eq!(
        collector
            .collect(&mut ledger, time(100), 1, &CancellationToken::new(),)
            .unwrap(),
        1
    );
    assert_eq!(store.manifests.lock().unwrap().len(), 1);
    assert_eq!(
        ledger
            .record(job_id)
            .unwrap()
            .artifact(ArtifactKind::Checkpoint, 1),
        None
    );

    assert_eq!(
        collector
            .collect(&mut ledger, time(100), 1, &CancellationToken::new(),)
            .unwrap(),
        1
    );
    assert!(store.manifests.lock().unwrap().is_empty());
    assert_eq!(
        ledger
            .record(job_id)
            .unwrap()
            .artifact(ArtifactKind::Result, 2),
        None
    );
}

#[derive(Clone)]
struct FixedProjectionProvider {
    projection: AnalyticsProjection,
}

impl AnalyticsProjectionProvider for FixedProjectionProvider {
    fn project(
        &mut self,
        _spec: &AnalyticsJobSpec,
        _cancellation: &CancellationToken,
    ) -> Result<AnalyticsProjection, SchedulerFailure> {
        Ok(self.projection.clone())
    }
}

struct ScriptedStepProvider {
    steps: VecDeque<AnalyticsAlgorithmStep>,
    checkpoints_seen: Arc<Mutex<Vec<Option<Vec<u8>>>>>,
}

impl AnalyticsStepProvider for ScriptedStepProvider {
    fn step(
        &mut self,
        request: dtg_analytics::AnalyticsStepRequest<'_>,
    ) -> Result<AnalyticsAlgorithmStep, SchedulerFailure> {
        self.checkpoints_seen
            .lock()
            .unwrap()
            .push(request.checkpoint().map(ToOwned::to_owned));
        self.steps
            .pop_front()
            .ok_or_else(|| SchedulerFailure::retryable("NO_STEP"))
    }
}

struct CancellingStepProvider {
    cancellation: CancellationToken,
}

struct IncompatibleCheckpointRepository {
    artifact: AnalyticsArtifact,
}

impl AnalyticsArtifactRepository for IncompatibleCheckpointRepository {
    fn persist_controlled(
        &self,
        _artifact: &AnalyticsArtifact,
        _budget: &AnalyticsArtifactIoBudget,
    ) -> Result<dtg_analytics::AnalyticsArtifactManifest, dtg_analytics::AnalyticsJobError> {
        Err(dtg_analytics::AnalyticsJobError::Storage)
    }

    fn load_controlled(
        &self,
        _job_id: dtg_analytics::AnalyticsJobId,
        _generation: u64,
        _kind: ArtifactKind,
        _budget: &AnalyticsArtifactIoBudget,
    ) -> Result<Option<AnalyticsArtifact>, dtg_analytics::AnalyticsJobError> {
        Ok(Some(self.artifact.clone()))
    }

    fn delete(
        &self,
        _job_id: dtg_analytics::AnalyticsJobId,
        _generation: u64,
        _kind: ArtifactKind,
    ) -> Result<(), dtg_analytics::AnalyticsJobError> {
        Ok(())
    }
}

impl AnalyticsStepProvider for CancellingStepProvider {
    fn step(
        &mut self,
        _request: dtg_analytics::AnalyticsStepRequest<'_>,
    ) -> Result<AnalyticsAlgorithmStep, SchedulerFailure> {
        self.cancellation.cancel();
        Ok(AnalyticsAlgorithmStep::Complete(
            b"must-not-publish".to_vec(),
        ))
    }
}

fn projection(job_spec: &AnalyticsJobSpec) -> AnalyticsProjection {
    let (shard_id, fence) = job_spec.snapshot().shards().iter().next().unwrap();
    let part = ShardProjectionPart {
        snapshot: job_spec.snapshot().clone(),
        provenance: PartitionProvenance {
            shard_id: *shard_id,
            placement_epoch: fence.placement_epoch,
            backend_generation: fence.backend_generation,
            applied_index: fence.applied_index,
            partition_index: 0,
            partition_count: 1,
        },
        vertices: Vec::new(),
        edges: Vec::new(),
    };
    let csr = SnapshotCsr::assemble(
        vec![part],
        job_spec.projection().clone(),
        ProjectionBudget::new(1_000_000, 0, CancellationToken::new()),
    )
    .unwrap();
    AnalyticsProjection::new(csr, job_spec.runtime_fences())
}

#[test]
fn scheduler_resumes_compatible_checkpoint_after_lease_reclaim() {
    let mut ledger = AnalyticsLedger::new(5).unwrap();
    let job_spec = spec("scheduler-resume");
    let job_id = ledger.submit(job_spec.clone(), time(1)).unwrap();
    let store = Arc::new(MemoryArtifactStore::new());
    let seen = Arc::new(Mutex::new(Vec::new()));
    let mut first = AnalyticsScheduler::new(
        WorkerId::new(1).unwrap(),
        FixedProjectionProvider {
            projection: projection(&job_spec),
        },
        ScriptedStepProvider {
            steps: VecDeque::from([AnalyticsAlgorithmStep::Checkpoint(b"checkpoint-1".to_vec())]),
            checkpoints_seen: seen.clone(),
        },
        StorageArtifactRepository::new(store.clone()),
        AnalyticsSchedulerConfig::new(64, 8, 1).unwrap(),
    );
    assert!(matches!(
        first
            .tick(&mut ledger, time(2), &CancellationToken::new())
            .unwrap(),
        AnalyticsSchedulerTick::Checkpointed {
            job_id: actual,
            generation: 1
        } if actual == job_id
    ));

    let bytes = ledger.encode_snapshot();
    let mut restored = AnalyticsLedger::decode_snapshot(&bytes).unwrap();
    restored.expire_leases(time(8), 8).unwrap();
    let mut second = AnalyticsScheduler::new(
        WorkerId::new(2).unwrap(),
        FixedProjectionProvider {
            projection: projection(&job_spec),
        },
        ScriptedStepProvider {
            steps: VecDeque::from([AnalyticsAlgorithmStep::Complete(b"result".to_vec())]),
            checkpoints_seen: seen.clone(),
        },
        StorageArtifactRepository::new(store),
        AnalyticsSchedulerConfig::new(64, 8, 1).unwrap(),
    );
    assert!(matches!(
        second
            .tick(&mut restored, time(9), &CancellationToken::new())
            .unwrap(),
        AnalyticsSchedulerTick::Succeeded {
            job_id: actual,
            generation: 2
        } if actual == job_id
    ));
    assert_eq!(
        seen.lock().unwrap().as_slice(),
        &[None, Some(b"checkpoint-1".to_vec())]
    );
}

#[test]
fn scheduler_checks_cancellation_before_result_publication() {
    let mut ledger = AnalyticsLedger::new(5).unwrap();
    let job_spec = spec("scheduler-cancel");
    let job_id = ledger.submit(job_spec.clone(), time(1)).unwrap();
    let cancellation = CancellationToken::new();
    let mut scheduler = AnalyticsScheduler::new(
        WorkerId::new(1).unwrap(),
        FixedProjectionProvider {
            projection: projection(&job_spec),
        },
        CancellingStepProvider {
            cancellation: cancellation.clone(),
        },
        StorageArtifactRepository::new(Arc::new(MemoryArtifactStore::new())),
        AnalyticsSchedulerConfig::new(64, 8, 1).unwrap(),
    );

    assert!(matches!(
        scheduler
            .tick(&mut ledger, time(2), &cancellation)
            .unwrap(),
        AnalyticsSchedulerTick::Cancelled { job_id: actual } if actual == job_id
    ));
    assert!(matches!(
        ledger.record(job_id).unwrap().state(),
        AnalyticsJobState::Running { .. }
    ));
}

#[test]
fn scheduler_renews_lease_when_bounded_step_yields() {
    let mut ledger = AnalyticsLedger::new(5).unwrap();
    let job_spec = spec("scheduler-heartbeat");
    let job_id = ledger.submit(job_spec.clone(), time(1)).unwrap();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let mut scheduler = AnalyticsScheduler::new(
        WorkerId::new(1).unwrap(),
        FixedProjectionProvider {
            projection: projection(&job_spec),
        },
        ScriptedStepProvider {
            steps: VecDeque::from([AnalyticsAlgorithmStep::Yield, AnalyticsAlgorithmStep::Yield]),
            checkpoints_seen: seen,
        },
        StorageArtifactRepository::new(Arc::new(MemoryArtifactStore::new())),
        AnalyticsSchedulerConfig::new(64, 8, 1).unwrap(),
    );
    assert!(matches!(
        scheduler
            .tick(&mut ledger, time(2), &CancellationToken::new())
            .unwrap(),
        AnalyticsSchedulerTick::Yielded { job_id: actual } if actual == job_id
    ));
    let first_expiry = match ledger.record(job_id).unwrap().state() {
        AnalyticsJobState::Running { expires_at, .. } => *expires_at,
        state => panic!("unexpected state: {state:?}"),
    };

    assert!(matches!(
        scheduler
            .tick(&mut ledger, time(6), &CancellationToken::new())
            .unwrap(),
        AnalyticsSchedulerTick::Yielded { job_id: actual } if actual == job_id
    ));
    let renewed_expiry = match ledger.record(job_id).unwrap().state() {
        AnalyticsJobState::Running { expires_at, .. } => *expires_at,
        state => panic!("unexpected state: {state:?}"),
    };
    assert!(renewed_expiry > first_expiry);
}

#[test]
fn scheduler_rejects_changed_runtime_fences() {
    let mut ledger = AnalyticsLedger::new(5).unwrap();
    let job_spec = spec("scheduler-fence-drift");
    let job_id = ledger.submit(job_spec.clone(), time(1)).unwrap();
    let valid_projection = projection(&job_spec);
    let mut changed = job_spec.runtime_fences();
    changed.provider_version += 1;
    let drifted_projection = AnalyticsProjection::new(valid_projection.csr().clone(), changed);
    let mut scheduler = AnalyticsScheduler::new(
        WorkerId::new(1).unwrap(),
        FixedProjectionProvider {
            projection: drifted_projection,
        },
        ScriptedStepProvider {
            steps: VecDeque::new(),
            checkpoints_seen: Arc::new(Mutex::new(Vec::new())),
        },
        StorageArtifactRepository::new(Arc::new(MemoryArtifactStore::new())),
        AnalyticsSchedulerConfig::new(64, 8, 1).unwrap(),
    );

    assert!(matches!(
        scheduler
            .tick(&mut ledger, time(2), &CancellationToken::new())
            .unwrap(),
        AnalyticsSchedulerTick::Failed {
            job_id: actual,
            retryable: false,
            ref code
        } if actual == job_id && code == "DTG-ANALYTICS-FENCE-DRIFT"
    ));
    assert!(matches!(
        ledger.record(job_id).unwrap().state(),
        AnalyticsJobState::Failed {
            retryable: false,
            ..
        }
    ));
}

#[test]
fn scheduler_rejects_incompatible_checkpoint_metadata() {
    let mut ledger = AnalyticsLedger::new(5).unwrap();
    let job_spec = spec("checkpoint-compatibility");
    let job_id = ledger.submit(job_spec.clone(), time(1)).unwrap();
    let queued = ledger.record(job_id).unwrap().cas();
    let lease = ledger
        .claim(job_id, WorkerId::new(1).unwrap(), time(2), queued)
        .unwrap();
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
    ledger.expire_leases(time(8), 8).unwrap();

    let incompatible_spec = AnalyticsJobSpec::new(
        AnalyticsRequestIdentity::new("checkpoint-compatibility".to_owned()).unwrap(),
        AlgorithmRequest::new(BuiltInAlgorithmId::PageRank),
        job_spec.snapshot().clone(),
        job_spec.projection().clone(),
        job_spec.topology_digest(),
        digest(99),
        job_spec.provider_version(),
        job_spec.algorithm_version(),
        job_spec.max_attempts(),
        job_spec.retention_until(),
    )
    .unwrap();
    let incompatible = AnalyticsArtifact::new(
        &incompatible_spec,
        1,
        1,
        ArtifactKind::Checkpoint,
        b"wrong".to_vec(),
        64,
    )
    .unwrap();
    let mut scheduler = AnalyticsScheduler::new(
        WorkerId::new(2).unwrap(),
        FixedProjectionProvider {
            projection: projection(&job_spec),
        },
        ScriptedStepProvider {
            steps: VecDeque::new(),
            checkpoints_seen: Arc::new(Mutex::new(Vec::new())),
        },
        IncompatibleCheckpointRepository {
            artifact: incompatible,
        },
        AnalyticsSchedulerConfig::new(64, 8, 1).unwrap(),
    );

    assert!(matches!(
        scheduler
            .tick(&mut ledger, time(9), &CancellationToken::new())
            .unwrap(),
        AnalyticsSchedulerTick::Failed {
            job_id: actual,
            retryable: false,
            ref code
        } if actual == job_id && code == "DTG-ANALYTICS-CHECKPOINT-INCOMPATIBLE"
    ));
}
