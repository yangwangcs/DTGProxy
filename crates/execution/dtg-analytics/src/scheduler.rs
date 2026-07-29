use dtg_storage::ArtifactKind;

use crate::{
    AnalyticsArtifact, AnalyticsArtifactIoBudget, AnalyticsArtifactRepository, AnalyticsJobError,
    AnalyticsJobId, AnalyticsJobSpec, AnalyticsJobState, AnalyticsLedger, AnalyticsRuntimeFences,
    CancellationToken, JobLease, JobTimestamp, SnapshotCsr, WorkerId,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchedulerFailure {
    retryable: bool,
    code: String,
}

impl SchedulerFailure {
    pub fn retryable(code: impl Into<String>) -> Self {
        Self {
            retryable: true,
            code: code.into(),
        }
    }

    pub fn terminal(code: impl Into<String>) -> Self {
        Self {
            retryable: false,
            code: code.into(),
        }
    }

    pub const fn is_retryable(&self) -> bool {
        self.retryable
    }

    pub fn code(&self) -> &str {
        &self.code
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AnalyticsProjection {
    csr: SnapshotCsr,
    runtime_fences: AnalyticsRuntimeFences,
}

impl AnalyticsProjection {
    pub const fn new(csr: SnapshotCsr, runtime_fences: AnalyticsRuntimeFences) -> Self {
        Self {
            csr,
            runtime_fences,
        }
    }

    pub const fn csr(&self) -> &SnapshotCsr {
        &self.csr
    }

    pub const fn runtime_fences(&self) -> AnalyticsRuntimeFences {
        self.runtime_fences
    }
}

pub trait AnalyticsProjectionProvider {
    fn project(
        &mut self,
        spec: &AnalyticsJobSpec,
        cancellation: &CancellationToken,
    ) -> Result<AnalyticsProjection, SchedulerFailure>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AnalyticsAlgorithmStep {
    Yield,
    Checkpoint(Vec<u8>),
    Complete(Vec<u8>),
}

pub struct AnalyticsStepRequest<'a> {
    spec: &'a AnalyticsJobSpec,
    projection: &'a AnalyticsProjection,
    checkpoint: Option<&'a [u8]>,
    cancellation: &'a CancellationToken,
}

impl<'a> AnalyticsStepRequest<'a> {
    const fn new(
        spec: &'a AnalyticsJobSpec,
        projection: &'a AnalyticsProjection,
        checkpoint: Option<&'a [u8]>,
        cancellation: &'a CancellationToken,
    ) -> Self {
        Self {
            spec,
            projection,
            checkpoint,
            cancellation,
        }
    }

    pub const fn spec(&self) -> &AnalyticsJobSpec {
        self.spec
    }

    pub const fn projection(&self) -> &AnalyticsProjection {
        self.projection
    }

    pub const fn checkpoint(&self) -> Option<&[u8]> {
        self.checkpoint
    }

    pub const fn cancellation(&self) -> &CancellationToken {
        self.cancellation
    }
}

pub trait AnalyticsStepProvider {
    fn step(
        &mut self,
        request: AnalyticsStepRequest<'_>,
    ) -> Result<AnalyticsAlgorithmStep, SchedulerFailure>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AnalyticsSchedulerConfig {
    artifact_chunk_size: usize,
    expiration_budget: usize,
    heartbeat_margin: u64,
}

impl AnalyticsSchedulerConfig {
    pub fn new(
        artifact_chunk_size: usize,
        expiration_budget: usize,
        heartbeat_margin: u64,
    ) -> Result<Self, AnalyticsJobError> {
        if artifact_chunk_size == 0 || expiration_budget == 0 {
            return Err(AnalyticsJobError::InvalidSpec);
        }
        Ok(Self {
            artifact_chunk_size,
            expiration_budget,
            heartbeat_margin,
        })
    }

    pub const fn artifact_chunk_size(self) -> usize {
        self.artifact_chunk_size
    }

    pub const fn expiration_budget(self) -> usize {
        self.expiration_budget
    }

    pub const fn heartbeat_margin(self) -> u64 {
        self.heartbeat_margin
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AnalyticsSchedulerTick {
    Idle,
    Yielded {
        job_id: AnalyticsJobId,
    },
    Checkpointed {
        job_id: AnalyticsJobId,
        generation: u64,
    },
    Succeeded {
        job_id: AnalyticsJobId,
        generation: u64,
    },
    Failed {
        job_id: AnalyticsJobId,
        retryable: bool,
        code: String,
    },
    Cancelled {
        job_id: AnalyticsJobId,
    },
}

pub struct AnalyticsScheduler<P, A, R> {
    worker: WorkerId,
    projection_provider: P,
    step_provider: A,
    artifacts: R,
    config: AnalyticsSchedulerConfig,
}

impl<P, A, R> AnalyticsScheduler<P, A, R>
where
    P: AnalyticsProjectionProvider,
    A: AnalyticsStepProvider,
    R: AnalyticsArtifactRepository,
{
    pub const fn new(
        worker: WorkerId,
        projection_provider: P,
        step_provider: A,
        artifacts: R,
        config: AnalyticsSchedulerConfig,
    ) -> Self {
        Self {
            worker,
            projection_provider,
            step_provider,
            artifacts,
            config,
        }
    }

    pub fn tick(
        &mut self,
        ledger: &mut AnalyticsLedger,
        now: JobTimestamp,
        cancellation: &CancellationToken,
    ) -> Result<AnalyticsSchedulerTick, AnalyticsJobError> {
        if cancellation.is_cancelled() {
            return Ok(AnalyticsSchedulerTick::Idle);
        }
        ledger.expire_leases_with_cancellation(now, self.config.expiration_budget, cancellation)?;
        let Some((job_id, mut lease)) = self.acquire(ledger, now)? else {
            return Ok(AnalyticsSchedulerTick::Idle);
        };
        if cancellation.is_cancelled() {
            return Ok(AnalyticsSchedulerTick::Cancelled { job_id });
        }

        let (spec, checkpoint_manifest, generation) = {
            let record = ledger
                .record(job_id)
                .ok_or(AnalyticsJobError::JobNotFound)?;
            (
                record.spec().clone(),
                record.checkpoint_manifest().cloned(),
                record.next_artifact_generation(),
            )
        };
        let projection = match self.projection_provider.project(&spec, cancellation) {
            Ok(projection) => projection,
            Err(failure) => {
                return self.record_failure(ledger, lease, failure, now);
            }
        };
        if let Err(error) = spec.validate_runtime_fences(projection.runtime_fences()) {
            let failure = SchedulerFailure::terminal(error.code());
            return self.record_failure(ledger, lease, failure, now);
        }
        if cancellation.is_cancelled() {
            return Ok(AnalyticsSchedulerTick::Cancelled { job_id });
        }

        let checkpoint = match checkpoint_manifest {
            Some(ref manifest) => {
                manifest.validate_checkpoint_for_resume(&spec)?;
                let io_budget = AnalyticsArtifactIoBudget::new(4_096, cancellation.clone())?;
                let artifact = self
                    .artifacts
                    .load_controlled(
                        job_id,
                        manifest.generation(),
                        ArtifactKind::Checkpoint,
                        &io_budget,
                    )?
                    .ok_or(AnalyticsJobError::CheckpointIncompatible)?;
                if artifact.manifest() != manifest {
                    return self.record_failure(
                        ledger,
                        lease,
                        SchedulerFailure::terminal(
                            AnalyticsJobError::CheckpointIncompatible.code(),
                        ),
                        now,
                    );
                }
                Some(artifact)
            }
            None => None,
        };

        if lease.expires_at().get().saturating_sub(now.get()) <= self.config.heartbeat_margin {
            lease = ledger.renew(lease, now)?;
        }
        let step = match self.step_provider.step(AnalyticsStepRequest::new(
            &spec,
            &projection,
            checkpoint.as_ref().map(|artifact| artifact.payload()),
            cancellation,
        )) {
            Ok(step) => step,
            Err(failure) => return self.record_failure(ledger, lease, failure, now),
        };
        if cancellation.is_cancelled() {
            return Ok(AnalyticsSchedulerTick::Cancelled { job_id });
        }

        match step {
            AnalyticsAlgorithmStep::Yield => Ok(AnalyticsSchedulerTick::Yielded { job_id }),
            AnalyticsAlgorithmStep::Checkpoint(payload) => {
                let artifact = AnalyticsArtifact::new(
                    &spec,
                    lease.lease_epoch(),
                    generation,
                    ArtifactKind::Checkpoint,
                    payload,
                    self.config.artifact_chunk_size,
                )?;
                let io_budget = AnalyticsArtifactIoBudget::new(4_096, cancellation.clone())?;
                let manifest = self.artifacts.persist_controlled(&artifact, &io_budget)?;
                if let Err(error) = ledger.checkpoint(lease, manifest, now) {
                    let _ = self
                        .artifacts
                        .delete(job_id, generation, ArtifactKind::Checkpoint);
                    return Err(error);
                }
                Ok(AnalyticsSchedulerTick::Checkpointed { job_id, generation })
            }
            AnalyticsAlgorithmStep::Complete(payload) => {
                let artifact = AnalyticsArtifact::new(
                    &spec,
                    lease.lease_epoch(),
                    generation,
                    ArtifactKind::Result,
                    payload,
                    self.config.artifact_chunk_size,
                )?;
                let io_budget = AnalyticsArtifactIoBudget::new(4_096, cancellation.clone())?;
                let manifest = self.artifacts.persist_controlled(&artifact, &io_budget)?;
                if let Err(error) = ledger.publish_result(lease, manifest, now) {
                    let _ = self
                        .artifacts
                        .delete(job_id, generation, ArtifactKind::Result);
                    return Err(error);
                }
                Ok(AnalyticsSchedulerTick::Succeeded { job_id, generation })
            }
        }
    }

    fn acquire(
        &self,
        ledger: &mut AnalyticsLedger,
        now: JobTimestamp,
    ) -> Result<Option<(AnalyticsJobId, JobLease)>, AnalyticsJobError> {
        let owned = ledger
            .records()
            .take(self.config.expiration_budget)
            .find_map(|(job_id, record)| match record.state() {
                AnalyticsJobState::Claimed { worker, .. }
                | AnalyticsJobState::Running { worker, .. }
                    if *worker == self.worker =>
                {
                    record.current_lease().map(|lease| (*job_id, lease))
                }
                _ => None,
            });
        if let Some((job_id, lease)) = owned {
            let lease = if matches!(
                ledger
                    .record(job_id)
                    .ok_or(AnalyticsJobError::JobNotFound)?
                    .state(),
                AnalyticsJobState::Claimed { .. }
            ) {
                ledger.begin(lease, now)?
            } else {
                lease
            };
            return Ok(Some((job_id, lease)));
        }

        let queued = ledger
            .records()
            .take(self.config.expiration_budget)
            .find_map(|(job_id, record)| {
                matches!(record.state(), AnalyticsJobState::Queued)
                    .then_some((*job_id, record.cas()))
            });
        let Some((job_id, expected)) = queued else {
            return Ok(None);
        };
        let lease = ledger.claim(job_id, self.worker, now, expected)?;
        let lease = ledger.begin(lease, now)?;
        Ok(Some((job_id, lease)))
    }

    fn record_failure(
        &self,
        ledger: &mut AnalyticsLedger,
        lease: JobLease,
        failure: SchedulerFailure,
        now: JobTimestamp,
    ) -> Result<AnalyticsSchedulerTick, AnalyticsJobError> {
        let job_id = lease.job_id();
        ledger.fail(
            lease,
            failure.is_retryable(),
            failure.code().to_owned(),
            now,
        )?;
        Ok(AnalyticsSchedulerTick::Failed {
            job_id,
            retryable: failure.is_retryable(),
            code: failure.code().to_owned(),
        })
    }
}
