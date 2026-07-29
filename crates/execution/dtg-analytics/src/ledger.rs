use std::collections::{BTreeMap, BTreeSet};

use dtg_kernel::{
    BackendGeneration, Digest32, PlacementEpoch, ShardId, TransactionId, TransactionTime, Version,
};
use dtg_language_ir::{AnalyticsRequestIdentity, BuiltInAlgorithmId};
use dtg_storage::{ArtifactKind, VertexId};

use crate::{
    AlgorithmRequest, AnalyticsArtifactManifest, AnalyticsJobError, AnalyticsJobId,
    AnalyticsJobSpec, AnalyticsJobState, CancellationToken, JobCas, JobLease, JobTimestamp,
    ProjectionSpec, PropertyColumnSpec, PropertyType, ShardSnapshotProvenance, SnapshotProvenance,
    WorkerId,
};

const LEDGER_MAGIC: &[u8; 4] = b"DTGL";
const LEDGER_VERSION: u32 = 1;

#[derive(Clone, Debug, PartialEq)]
pub struct AnalyticsJobRecord {
    spec: AnalyticsJobSpec,
    state: AnalyticsJobState,
    revision: u64,
    lease_epoch: u64,
    attempts: u32,
    next_artifact_generation: u64,
    artifacts: BTreeMap<(ArtifactKind, u64), AnalyticsArtifactManifest>,
    pins: BTreeSet<(ArtifactKind, u64)>,
    submitted_at: JobTimestamp,
    updated_at: JobTimestamp,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AnalyticsGcCandidate {
    job_id: AnalyticsJobId,
    expected: JobCas,
    manifest: AnalyticsArtifactManifest,
}

impl AnalyticsGcCandidate {
    pub const fn job_id(&self) -> AnalyticsJobId {
        self.job_id
    }

    pub const fn expected(&self) -> JobCas {
        self.expected
    }

    pub const fn manifest(&self) -> &AnalyticsArtifactManifest {
        &self.manifest
    }
}

impl AnalyticsJobRecord {
    pub const fn spec(&self) -> &AnalyticsJobSpec {
        &self.spec
    }

    pub const fn state(&self) -> &AnalyticsJobState {
        &self.state
    }

    pub const fn revision(&self) -> u64 {
        self.revision
    }

    pub const fn lease_epoch(&self) -> u64 {
        self.lease_epoch
    }

    pub const fn attempts(&self) -> u32 {
        self.attempts
    }

    pub const fn next_artifact_generation(&self) -> u64 {
        self.next_artifact_generation
    }

    pub const fn submitted_at(&self) -> JobTimestamp {
        self.submitted_at
    }

    pub const fn updated_at(&self) -> JobTimestamp {
        self.updated_at
    }

    pub const fn cas(&self) -> JobCas {
        JobCas::new(self.state.kind(), self.revision, self.lease_epoch)
    }

    pub fn artifact(
        &self,
        kind: ArtifactKind,
        generation: u64,
    ) -> Option<&AnalyticsArtifactManifest> {
        self.artifacts.get(&(kind, generation))
    }

    pub fn checkpoint_manifest(&self) -> Option<&AnalyticsArtifactManifest> {
        let generation = match self.state {
            AnalyticsJobState::Running { checkpoint, .. } => checkpoint,
            _ => self.artifacts.keys().rev().find_map(|(kind, generation)| {
                (*kind == ArtifactKind::Checkpoint).then_some(*generation)
            }),
        }?;
        self.artifact(ArtifactKind::Checkpoint, generation)
    }

    pub fn result_manifest(&self) -> Option<&AnalyticsArtifactManifest> {
        let AnalyticsJobState::Succeeded { result_generation } = self.state else {
            return None;
        };
        self.artifact(ArtifactKind::Result, result_generation)
    }

    pub fn is_pinned(&self, kind: ArtifactKind, generation: u64) -> bool {
        self.pins.contains(&(kind, generation))
    }

    pub fn current_lease(&self) -> Option<JobLease> {
        match self.state {
            AnalyticsJobState::Claimed {
                worker,
                lease_epoch,
                expires_at,
            }
            | AnalyticsJobState::Running {
                worker,
                lease_epoch,
                expires_at,
                ..
            } => Some(JobLease::new(
                self.spec.id(),
                worker,
                lease_epoch,
                self.revision,
                expires_at,
            )),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct AnalyticsLedger {
    lease_duration: u64,
    jobs: BTreeMap<AnalyticsJobId, AnalyticsJobRecord>,
}

impl AnalyticsLedger {
    pub fn new(lease_duration: u64) -> Result<Self, AnalyticsJobError> {
        if lease_duration == 0 {
            return Err(AnalyticsJobError::InvalidSpec);
        }
        Ok(Self {
            lease_duration,
            jobs: BTreeMap::new(),
        })
    }

    pub const fn lease_duration(&self) -> u64 {
        self.lease_duration
    }

    pub fn submit(
        &mut self,
        spec: AnalyticsJobSpec,
        submitted_at: JobTimestamp,
    ) -> Result<AnalyticsJobId, AnalyticsJobError> {
        let job_id = spec.id();
        if let Some(existing) = self.jobs.get(&job_id) {
            return (existing.spec.digest() == spec.digest())
                .then_some(job_id)
                .ok_or(AnalyticsJobError::SpecConflict);
        }
        self.jobs.insert(
            job_id,
            AnalyticsJobRecord {
                spec,
                state: AnalyticsJobState::Queued,
                revision: 1,
                lease_epoch: 0,
                attempts: 0,
                next_artifact_generation: 1,
                artifacts: BTreeMap::new(),
                pins: BTreeSet::new(),
                submitted_at,
                updated_at: submitted_at,
            },
        );
        Ok(job_id)
    }

    pub fn record(&self, job_id: AnalyticsJobId) -> Option<&AnalyticsJobRecord> {
        self.jobs.get(&job_id)
    }

    pub fn records(&self) -> impl Iterator<Item = (&AnalyticsJobId, &AnalyticsJobRecord)> {
        self.jobs.iter()
    }

    pub fn claim(
        &mut self,
        job_id: AnalyticsJobId,
        worker: WorkerId,
        now: JobTimestamp,
        expected: JobCas,
    ) -> Result<JobLease, AnalyticsJobError> {
        let expires_at = now.checked_add(self.lease_duration)?;
        let record = self
            .jobs
            .get_mut(&job_id)
            .ok_or(AnalyticsJobError::JobNotFound)?;
        check_cas(record, expected)?;
        if !matches!(record.state, AnalyticsJobState::Queued)
            || record.attempts >= record.spec.max_attempts()
        {
            return Err(AnalyticsJobError::InvalidTransition);
        }
        record.lease_epoch = record
            .lease_epoch
            .checked_add(1)
            .ok_or(AnalyticsJobError::InvalidTransition)?;
        record.attempts += 1;
        record.revision += 1;
        record.updated_at = now;
        record.state = AnalyticsJobState::Claimed {
            worker,
            lease_epoch: record.lease_epoch,
            expires_at,
        };
        Ok(JobLease::new(
            job_id,
            worker,
            record.lease_epoch,
            record.revision,
            expires_at,
        ))
    }

    pub fn begin(
        &mut self,
        lease: JobLease,
        now: JobTimestamp,
    ) -> Result<JobLease, AnalyticsJobError> {
        let record = self.active_record_mut(lease, now)?;
        if !matches!(record.state, AnalyticsJobState::Claimed { .. }) {
            return Err(AnalyticsJobError::StaleState);
        }
        let checkpoint = record
            .artifacts
            .keys()
            .rev()
            .find_map(|(kind, generation)| {
                (*kind == ArtifactKind::Checkpoint).then_some(*generation)
            });
        let expires_at = lease.expires_at();
        record.revision += 1;
        record.updated_at = now;
        record.state = AnalyticsJobState::Running {
            worker: lease.worker(),
            lease_epoch: lease.lease_epoch(),
            expires_at,
            checkpoint,
        };
        Ok(JobLease::new(
            lease.job_id(),
            lease.worker(),
            lease.lease_epoch(),
            record.revision,
            expires_at,
        ))
    }

    pub fn renew(
        &mut self,
        lease: JobLease,
        now: JobTimestamp,
    ) -> Result<JobLease, AnalyticsJobError> {
        let expires_at = now.checked_add(self.lease_duration)?;
        let record = self.active_record_mut(lease, now)?;
        match &mut record.state {
            AnalyticsJobState::Claimed {
                expires_at: current,
                ..
            }
            | AnalyticsJobState::Running {
                expires_at: current,
                ..
            } => *current = expires_at,
            _ => return Err(AnalyticsJobError::StaleState),
        }
        record.revision += 1;
        record.updated_at = now;
        Ok(JobLease::new(
            lease.job_id(),
            lease.worker(),
            lease.lease_epoch(),
            record.revision,
            expires_at,
        ))
    }

    pub fn checkpoint(
        &mut self,
        lease: JobLease,
        manifest: AnalyticsArtifactManifest,
        now: JobTimestamp,
    ) -> Result<JobLease, AnalyticsJobError> {
        let lease_duration = self.lease_duration;
        let record = self.active_record_mut(lease, now)?;
        if !matches!(record.state, AnalyticsJobState::Running { .. }) {
            return Err(AnalyticsJobError::StaleState);
        }
        validate_artifact(record, lease, &manifest, ArtifactKind::Checkpoint)?;
        if let AnalyticsJobState::Running {
            checkpoint: Some(previous),
            ..
        } = record.state
        {
            record.pins.remove(&(ArtifactKind::Checkpoint, previous));
        }
        let generation = manifest.generation();
        record
            .artifacts
            .insert((ArtifactKind::Checkpoint, generation), manifest);
        record.pins.insert((ArtifactKind::Checkpoint, generation));
        record.next_artifact_generation += 1;
        record.revision += 1;
        record.updated_at = now;
        let expires_at = now.checked_add(lease_duration)?;
        record.state = AnalyticsJobState::Running {
            worker: lease.worker(),
            lease_epoch: lease.lease_epoch(),
            expires_at,
            checkpoint: Some(generation),
        };
        Ok(JobLease::new(
            lease.job_id(),
            lease.worker(),
            lease.lease_epoch(),
            record.revision,
            expires_at,
        ))
    }

    pub fn publish_result(
        &mut self,
        lease: JobLease,
        manifest: AnalyticsArtifactManifest,
        now: JobTimestamp,
    ) -> Result<(), AnalyticsJobError> {
        let record = self.active_record_mut(lease, now)?;
        if !matches!(record.state, AnalyticsJobState::Running { .. }) {
            return Err(AnalyticsJobError::StaleState);
        }
        validate_artifact(record, lease, &manifest, ArtifactKind::Result)?;
        if let AnalyticsJobState::Running {
            checkpoint: Some(previous),
            ..
        } = record.state
        {
            record.pins.remove(&(ArtifactKind::Checkpoint, previous));
        }
        let generation = manifest.generation();
        record
            .artifacts
            .insert((ArtifactKind::Result, generation), manifest);
        record.pins.insert((ArtifactKind::Result, generation));
        record.next_artifact_generation += 1;
        record.revision += 1;
        record.updated_at = now;
        record.state = AnalyticsJobState::Succeeded {
            result_generation: generation,
        };
        Ok(())
    }

    pub fn fail(
        &mut self,
        lease: JobLease,
        retryable: bool,
        code: impl Into<String>,
        now: JobTimestamp,
    ) -> Result<(), AnalyticsJobError> {
        let code = code.into();
        if code.is_empty() || code.len() > 256 {
            return Err(AnalyticsJobError::InvalidSpec);
        }
        let record = self.active_record_mut(lease, now)?;
        record.lease_epoch += 1;
        record.revision += 1;
        record.updated_at = now;
        if retryable && record.attempts < record.spec.max_attempts() {
            record.state = AnalyticsJobState::Queued;
        } else {
            record.state = AnalyticsJobState::Failed {
                retryable: retryable && record.attempts < record.spec.max_attempts(),
                code,
            };
        }
        Ok(())
    }

    pub fn cancel(
        &mut self,
        job_id: AnalyticsJobId,
        expected: JobCas,
        now: JobTimestamp,
    ) -> Result<(), AnalyticsJobError> {
        let record = self
            .jobs
            .get_mut(&job_id)
            .ok_or(AnalyticsJobError::JobNotFound)?;
        check_cas(record, expected)?;
        if !matches!(
            record.state,
            AnalyticsJobState::Queued
                | AnalyticsJobState::Claimed { .. }
                | AnalyticsJobState::Running { .. }
        ) {
            return Err(AnalyticsJobError::InvalidTransition);
        }
        record.lease_epoch += 1;
        record.revision += 1;
        record.updated_at = now;
        record.state = AnalyticsJobState::Cancelled;
        Ok(())
    }

    pub fn expire_leases(
        &mut self,
        now: JobTimestamp,
        budget: usize,
    ) -> Result<usize, AnalyticsJobError> {
        self.expire_leases_with_cancellation(now, budget, &CancellationToken::new())
    }

    pub fn expire_leases_with_cancellation(
        &mut self,
        now: JobTimestamp,
        budget: usize,
        cancellation: &CancellationToken,
    ) -> Result<usize, AnalyticsJobError> {
        if budget == 0 {
            return Err(AnalyticsJobError::BudgetExhausted);
        }
        if cancellation.is_cancelled() {
            return Err(AnalyticsJobError::Cancelled);
        }
        let mut expired = 0;
        for record in self.jobs.values_mut().take(budget) {
            if cancellation.is_cancelled() {
                return Err(AnalyticsJobError::Cancelled);
            }
            let expires_at = match record.state {
                AnalyticsJobState::Claimed { expires_at, .. }
                | AnalyticsJobState::Running { expires_at, .. } => expires_at,
                _ => continue,
            };
            if expires_at > now {
                continue;
            }
            record.lease_epoch += 1;
            record.revision += 1;
            record.updated_at = now;
            record.state = if record.attempts < record.spec.max_attempts() {
                AnalyticsJobState::Queued
            } else {
                AnalyticsJobState::Failed {
                    retryable: false,
                    code: AnalyticsJobError::LeaseExpired.code().to_owned(),
                }
            };
            expired += 1;
        }
        Ok(expired)
    }

    pub fn pin_artifact(
        &mut self,
        job_id: AnalyticsJobId,
        kind: ArtifactKind,
        generation: u64,
        expected: JobCas,
        now: JobTimestamp,
    ) -> Result<(), AnalyticsJobError> {
        let record = self
            .jobs
            .get_mut(&job_id)
            .ok_or(AnalyticsJobError::JobNotFound)?;
        check_cas(record, expected)?;
        if matches!(record.state, AnalyticsJobState::Tombstoned) {
            return Err(AnalyticsJobError::InvalidTransition);
        }
        if !record.artifacts.contains_key(&(kind, generation)) {
            return Err(AnalyticsJobError::ArtifactMismatch);
        }
        record.pins.insert((kind, generation));
        record.revision += 1;
        record.updated_at = now;
        Ok(())
    }

    pub fn unpin_artifact(
        &mut self,
        job_id: AnalyticsJobId,
        kind: ArtifactKind,
        generation: u64,
        expected: JobCas,
        now: JobTimestamp,
    ) -> Result<(), AnalyticsJobError> {
        let record = self
            .jobs
            .get_mut(&job_id)
            .ok_or(AnalyticsJobError::JobNotFound)?;
        check_cas(record, expected)?;
        if !matches!(
            record.state,
            AnalyticsJobState::Succeeded { .. }
                | AnalyticsJobState::Failed { .. }
                | AnalyticsJobState::Cancelled
                | AnalyticsJobState::Tombstoned
        ) {
            return Err(AnalyticsJobError::InvalidTransition);
        }
        if now < record.spec.retention_until() {
            return Err(AnalyticsJobError::RetentionActive);
        }
        if !record.pins.remove(&(kind, generation)) {
            return Err(AnalyticsJobError::ArtifactMismatch);
        }
        record.revision += 1;
        record.updated_at = now;
        Ok(())
    }

    pub fn garbage_collect(
        &mut self,
        now: JobTimestamp,
        budget: usize,
    ) -> Result<Vec<AnalyticsArtifactManifest>, AnalyticsJobError> {
        self.garbage_collect_with_cancellation(now, budget, &CancellationToken::new())
    }

    pub fn garbage_collect_with_cancellation(
        &mut self,
        now: JobTimestamp,
        budget: usize,
        cancellation: &CancellationToken,
    ) -> Result<Vec<AnalyticsArtifactManifest>, AnalyticsJobError> {
        if budget == 0 {
            return Err(AnalyticsJobError::BudgetExhausted);
        }
        if cancellation.is_cancelled() {
            return Err(AnalyticsJobError::Cancelled);
        }
        let mut reclaimed = Vec::new();
        let scan_budget = budget.saturating_mul(64).max(2);
        while reclaimed.len() < budget {
            let Some(candidate) = self.next_gc_candidate(now, scan_budget, cancellation)? else {
                break;
            };
            reclaimed.push(candidate.manifest.clone());
            self.acknowledge_artifact_reclaimed(candidate, now)?;
        }
        Ok(reclaimed)
    }

    pub fn next_gc_candidate(
        &self,
        now: JobTimestamp,
        scan_budget: usize,
        cancellation: &CancellationToken,
    ) -> Result<Option<AnalyticsGcCandidate>, AnalyticsJobError> {
        if scan_budget == 0 {
            return Err(AnalyticsJobError::BudgetExhausted);
        }
        let mut scanned = 0;
        for (job_id, record) in &self.jobs {
            if cancellation.is_cancelled() {
                return Err(AnalyticsJobError::Cancelled);
            }
            if scanned == scan_budget {
                break;
            }
            scanned += 1;
            if now < record.spec.retention_until()
                || !matches!(
                    record.state,
                    AnalyticsJobState::Succeeded { .. }
                        | AnalyticsJobState::Failed { .. }
                        | AnalyticsJobState::Cancelled
                        | AnalyticsJobState::Tombstoned
                )
            {
                continue;
            }
            for (key, manifest) in &record.artifacts {
                if cancellation.is_cancelled() {
                    return Err(AnalyticsJobError::Cancelled);
                }
                if scanned == scan_budget {
                    return Ok(None);
                }
                scanned += 1;
                if !record.pins.contains(key) {
                    return Ok(Some(AnalyticsGcCandidate {
                        job_id: *job_id,
                        expected: record.cas(),
                        manifest: manifest.clone(),
                    }));
                }
            }
        }
        Ok(None)
    }

    pub fn acknowledge_artifact_reclaimed(
        &mut self,
        candidate: AnalyticsGcCandidate,
        now: JobTimestamp,
    ) -> Result<(), AnalyticsJobError> {
        let record = self
            .jobs
            .get_mut(&candidate.job_id)
            .ok_or(AnalyticsJobError::JobNotFound)?;
        check_cas(record, candidate.expected)?;
        let key = (candidate.manifest.kind(), candidate.manifest.generation());
        if record.pins.contains(&key) || record.artifacts.get(&key) != Some(&candidate.manifest) {
            return Err(AnalyticsJobError::ArtifactMismatch);
        }
        record.artifacts.remove(&key);
        record.revision += 1;
        record.updated_at = now;
        Ok(())
    }

    pub fn tombstone(
        &mut self,
        job_id: AnalyticsJobId,
        expected: JobCas,
        now: JobTimestamp,
    ) -> Result<(), AnalyticsJobError> {
        let record = self
            .jobs
            .get_mut(&job_id)
            .ok_or(AnalyticsJobError::JobNotFound)?;
        check_cas(record, expected)?;
        if now < record.spec.retention_until() {
            return Err(AnalyticsJobError::RetentionActive);
        }
        if !record.pins.is_empty() {
            return Err(AnalyticsJobError::ArtifactPinned);
        }
        if !matches!(
            record.state,
            AnalyticsJobState::Succeeded { .. }
                | AnalyticsJobState::Failed { .. }
                | AnalyticsJobState::Cancelled
        ) {
            return Err(AnalyticsJobError::InvalidTransition);
        }
        record.revision += 1;
        record.updated_at = now;
        record.state = AnalyticsJobState::Tombstoned;
        Ok(())
    }

    pub fn encode_snapshot(&self) -> Vec<u8> {
        let mut encoder = Encoder::default();
        encoder.bytes.extend_from_slice(LEDGER_MAGIC);
        encoder.u32(LEDGER_VERSION);
        encoder.u64(self.lease_duration);
        encoder.u32(self.jobs.len() as u32);
        for record in self.jobs.values() {
            encode_record(&mut encoder, record);
        }
        let digest = blake3::hash(&encoder.bytes);
        encoder.bytes.extend_from_slice(digest.as_bytes());
        encoder.bytes
    }

    pub fn decode_snapshot(bytes: &[u8]) -> Result<Self, AnalyticsJobError> {
        if bytes.len() < 4 + 4 + 8 + 4 + 32 || &bytes[..4] != LEDGER_MAGIC {
            return Err(AnalyticsJobError::LedgerCorrupt);
        }
        let version = u32::from_be_bytes(
            bytes[4..8]
                .try_into()
                .map_err(|_| AnalyticsJobError::LedgerCorrupt)?,
        );
        if version != LEDGER_VERSION {
            return Err(AnalyticsJobError::LedgerVersion);
        }
        let content_length = bytes.len() - 32;
        if blake3::hash(&bytes[..content_length]).as_bytes() != &bytes[content_length..] {
            return Err(AnalyticsJobError::LedgerCorrupt);
        }
        let mut decoder = Decoder::new(&bytes[8..content_length]);
        let lease_duration = decoder.u64()?;
        let count = decoder.u32()? as usize;
        let mut jobs = BTreeMap::new();
        for _ in 0..count {
            let record = decode_record(&mut decoder)?;
            if jobs.insert(record.spec.id(), record).is_some() {
                return Err(AnalyticsJobError::LedgerCorrupt);
            }
        }
        if !decoder.is_finished() || lease_duration == 0 {
            return Err(AnalyticsJobError::LedgerCorrupt);
        }
        Ok(Self {
            lease_duration,
            jobs,
        })
    }

    fn active_record_mut(
        &mut self,
        lease: JobLease,
        now: JobTimestamp,
    ) -> Result<&mut AnalyticsJobRecord, AnalyticsJobError> {
        let record = self
            .jobs
            .get_mut(&lease.job_id())
            .ok_or(AnalyticsJobError::JobNotFound)?;
        let (worker, lease_epoch, expires_at) = match record.state {
            AnalyticsJobState::Claimed {
                worker,
                lease_epoch,
                expires_at,
            }
            | AnalyticsJobState::Running {
                worker,
                lease_epoch,
                expires_at,
                ..
            } => (worker, lease_epoch, expires_at),
            _ => return Err(AnalyticsJobError::StaleLease),
        };
        if lease_epoch != lease.lease_epoch() || worker != lease.worker() {
            return Err(AnalyticsJobError::StaleLease);
        }
        if record.revision != lease.revision() {
            return Err(AnalyticsJobError::StaleRevision);
        }
        if expires_at <= now {
            return Err(AnalyticsJobError::LeaseExpired);
        }
        Ok(record)
    }
}

fn check_cas(record: &AnalyticsJobRecord, expected: JobCas) -> Result<(), AnalyticsJobError> {
    if record.revision != expected.revision() {
        return Err(AnalyticsJobError::StaleRevision);
    }
    if record.lease_epoch != expected.lease_epoch() {
        return Err(AnalyticsJobError::StaleLease);
    }
    if record.state.kind() != expected.state() {
        return Err(AnalyticsJobError::StaleState);
    }
    Ok(())
}

fn validate_artifact(
    record: &AnalyticsJobRecord,
    lease: JobLease,
    manifest: &AnalyticsArtifactManifest,
    kind: ArtifactKind,
) -> Result<(), AnalyticsJobError> {
    manifest.validate_for(&record.spec, lease.lease_epoch(), kind)?;
    if manifest.generation() != record.next_artifact_generation {
        return Err(AnalyticsJobError::StaleGeneration);
    }
    Ok(())
}

fn encode_record(encoder: &mut Encoder, record: &AnalyticsJobRecord) {
    encode_spec(encoder, &record.spec);
    encode_state(encoder, &record.state);
    encoder.u64(record.revision);
    encoder.u64(record.lease_epoch);
    encoder.u32(record.attempts);
    encoder.u64(record.next_artifact_generation);
    encoder.u64(record.submitted_at.get());
    encoder.u64(record.updated_at.get());
    encoder.u32(record.artifacts.len() as u32);
    for manifest in record.artifacts.values() {
        encode_manifest(encoder, manifest);
    }
    encoder.u32(record.pins.len() as u32);
    for (kind, generation) in &record.pins {
        encoder.u8(kind_tag(*kind));
        encoder.u64(*generation);
    }
}

fn decode_record(decoder: &mut Decoder<'_>) -> Result<AnalyticsJobRecord, AnalyticsJobError> {
    let spec = decode_spec(decoder)?;
    let state = decode_state(decoder)?;
    let revision = decoder.u64()?;
    let lease_epoch = decoder.u64()?;
    let attempts = decoder.u32()?;
    let next_artifact_generation = decoder.u64()?;
    let submitted_at = JobTimestamp::new(decoder.u64()?);
    let updated_at = JobTimestamp::new(decoder.u64()?);
    let artifact_count = decoder.u32()? as usize;
    let mut artifacts = BTreeMap::new();
    for _ in 0..artifact_count {
        let manifest = decode_manifest(decoder)?;
        let key = (manifest.kind(), manifest.generation());
        if artifacts.insert(key, manifest).is_some() {
            return Err(AnalyticsJobError::LedgerCorrupt);
        }
    }
    let pin_count = decoder.u32()? as usize;
    let mut pins = BTreeSet::new();
    for _ in 0..pin_count {
        let key = (decode_kind(decoder.u8()?)?, decoder.u64()?);
        if !artifacts.contains_key(&key) || !pins.insert(key) {
            return Err(AnalyticsJobError::LedgerCorrupt);
        }
    }
    if revision == 0
        || next_artifact_generation == 0
        || attempts > spec.max_attempts()
        || state_lease_epoch(&state).is_some_and(|epoch| epoch != lease_epoch)
    {
        return Err(AnalyticsJobError::LedgerCorrupt);
    }
    Ok(AnalyticsJobRecord {
        spec,
        state,
        revision,
        lease_epoch,
        attempts,
        next_artifact_generation,
        artifacts,
        pins,
        submitted_at,
        updated_at,
    })
}

fn encode_spec(encoder: &mut Encoder, spec: &AnalyticsJobSpec) {
    encoder.u128(spec.id().get());
    encoder.digest(spec.digest());
    encoder.string(spec.request_identity());
    encode_request(encoder, spec.request());
    encode_snapshot(encoder, spec.snapshot());
    encode_projection(encoder, spec.projection());
    encoder.digest(spec.topology_digest());
    encoder.digest(spec.backend_fence_digest());
    encoder.u32(spec.provider_version());
    encoder.u32(spec.algorithm_version());
    encoder.u32(spec.result_schema_version());
    encoder.u32(spec.checkpoint_schema_version());
    encoder.u32(spec.max_attempts());
    encoder.u64(spec.retention_until().get());
}

fn decode_spec(decoder: &mut Decoder<'_>) -> Result<AnalyticsJobSpec, AnalyticsJobError> {
    let expected_id = AnalyticsJobId::new(decoder.u128()?)?;
    let expected_digest = decoder.digest()?;
    let identity = AnalyticsRequestIdentity::new(decoder.string()?)
        .map_err(|_| AnalyticsJobError::LedgerCorrupt)?;
    let request = decode_request(decoder)?;
    let snapshot = decode_snapshot(decoder)?;
    let projection = decode_projection(decoder)?;
    let topology_digest = decoder.digest()?;
    let backend_fence_digest = decoder.digest()?;
    let provider_version = decoder.u32()?;
    let algorithm_version = decoder.u32()?;
    let result_schema_version = decoder.u32()?;
    let checkpoint_schema_version = decoder.u32()?;
    let max_attempts = decoder.u32()?;
    let retention_until = JobTimestamp::new(decoder.u64()?);
    let spec = AnalyticsJobSpec::new(
        identity,
        request,
        snapshot,
        projection,
        topology_digest,
        backend_fence_digest,
        provider_version,
        algorithm_version,
        max_attempts,
        retention_until,
    )
    .map_err(|_| AnalyticsJobError::LedgerCorrupt)?;
    if spec.id() != expected_id
        || spec.digest() != expected_digest
        || spec.result_schema_version() != result_schema_version
        || spec.checkpoint_schema_version() != checkpoint_schema_version
    {
        return Err(AnalyticsJobError::LedgerCorrupt);
    }
    Ok(spec)
}

fn encode_state(encoder: &mut Encoder, state: &AnalyticsJobState) {
    match state {
        AnalyticsJobState::Queued => encoder.u8(1),
        AnalyticsJobState::Claimed {
            worker,
            lease_epoch,
            expires_at,
        } => {
            encoder.u8(2);
            encoder.u64(worker.get());
            encoder.u64(*lease_epoch);
            encoder.u64(expires_at.get());
        }
        AnalyticsJobState::Running {
            worker,
            lease_epoch,
            expires_at,
            checkpoint,
        } => {
            encoder.u8(3);
            encoder.u64(worker.get());
            encoder.u64(*lease_epoch);
            encoder.u64(expires_at.get());
            encoder.optional_u64(*checkpoint);
        }
        AnalyticsJobState::Succeeded { result_generation } => {
            encoder.u8(4);
            encoder.u64(*result_generation);
        }
        AnalyticsJobState::Failed { retryable, code } => {
            encoder.u8(5);
            encoder.boolean(*retryable);
            encoder.string(code);
        }
        AnalyticsJobState::Cancelled => encoder.u8(6),
        AnalyticsJobState::Tombstoned => encoder.u8(7),
    }
}

fn decode_state(decoder: &mut Decoder<'_>) -> Result<AnalyticsJobState, AnalyticsJobError> {
    match decoder.u8()? {
        1 => Ok(AnalyticsJobState::Queued),
        2 => Ok(AnalyticsJobState::Claimed {
            worker: WorkerId::new(decoder.u64()?)?,
            lease_epoch: nonzero(decoder.u64()?)?,
            expires_at: JobTimestamp::new(decoder.u64()?),
        }),
        3 => Ok(AnalyticsJobState::Running {
            worker: WorkerId::new(decoder.u64()?)?,
            lease_epoch: nonzero(decoder.u64()?)?,
            expires_at: JobTimestamp::new(decoder.u64()?),
            checkpoint: decoder.optional_u64()?,
        }),
        4 => Ok(AnalyticsJobState::Succeeded {
            result_generation: nonzero(decoder.u64()?)?,
        }),
        5 => Ok(AnalyticsJobState::Failed {
            retryable: decoder.boolean()?,
            code: decoder.string()?,
        }),
        6 => Ok(AnalyticsJobState::Cancelled),
        7 => Ok(AnalyticsJobState::Tombstoned),
        _ => Err(AnalyticsJobError::LedgerCorrupt),
    }
}

fn encode_manifest(encoder: &mut Encoder, manifest: &AnalyticsArtifactManifest) {
    encoder.u128(manifest.job_id().get());
    encoder.u64(manifest.lease_epoch());
    encoder.u64(manifest.generation());
    encoder.u8(kind_tag(manifest.kind()));
    encoder.u32(manifest.schema_version());
    encoder.u8(algorithm_tag(manifest.algorithm()));
    encoder.u32(manifest.algorithm_version());
    encoder.digest(manifest.snapshot_digest());
    encoder.digest(manifest.spec_digest());
    encoder.digest(manifest.payload_digest());
    encoder.u64(manifest.chunk_count());
    encoder.u64(manifest.total_bytes());
    encoder.digest(manifest.content_digest());
}

fn decode_manifest(
    decoder: &mut Decoder<'_>,
) -> Result<AnalyticsArtifactManifest, AnalyticsJobError> {
    Ok(AnalyticsArtifactManifest::from_parts(
        AnalyticsJobId::new(decoder.u128()?)?,
        nonzero(decoder.u64()?)?,
        nonzero(decoder.u64()?)?,
        decode_kind(decoder.u8()?)?,
        nonzero_u32(decoder.u32()?)?,
        decode_algorithm(decoder.u8()?)?,
        nonzero_u32(decoder.u32()?)?,
        decoder.digest()?,
        decoder.digest()?,
        decoder.digest()?,
        nonzero(decoder.u64()?)?,
        nonzero(decoder.u64()?)?,
        decoder.digest()?,
    ))
}

fn encode_request(encoder: &mut Encoder, request: &AlgorithmRequest) {
    encoder.u8(algorithm_tag(request.algorithm));
    encoder.optional_u128(request.source.map(VertexId::get));
    encoder.optional_u128(request.target.map(VertexId::get));
    encoder.u32(request.max_depth);
    encoder.u64(request.max_pairs as u64);
    encoder.u32(request.iterations);
    encoder.u32(request.k);
    encoder.u64(request.damping.to_bits());
    encoder.optional_string(request.weight_property.as_deref());
    encoder.optional_string(request.departure_property.as_deref());
    encoder.optional_string(request.arrival_property.as_deref());
    encoder.optional_string(request.event_time_property.as_deref());
    encoder.optional_string(request.signal_property.as_deref());
    encoder.optional_i64(request.time_start);
    encoder.optional_i64(request.time_end);
    encoder.u64(request.change_threshold.to_bits());
}

fn decode_request(decoder: &mut Decoder<'_>) -> Result<AlgorithmRequest, AnalyticsJobError> {
    let algorithm = decode_algorithm(decoder.u8()?)?;
    let source = decoder
        .optional_u128()?
        .map(VertexId::new)
        .transpose()
        .map_err(|_| AnalyticsJobError::LedgerCorrupt)?;
    let target = decoder
        .optional_u128()?
        .map(VertexId::new)
        .transpose()
        .map_err(|_| AnalyticsJobError::LedgerCorrupt)?;
    let max_depth = decoder.u32()?;
    let max_pairs =
        usize::try_from(decoder.u64()?).map_err(|_| AnalyticsJobError::LedgerCorrupt)?;
    let iterations = decoder.u32()?;
    let k = decoder.u32()?;
    let damping = f64::from_bits(decoder.u64()?);
    let weight_property = decoder.optional_string()?;
    let departure_property = decoder.optional_string()?;
    let arrival_property = decoder.optional_string()?;
    let event_time_property = decoder.optional_string()?;
    let signal_property = decoder.optional_string()?;
    let time_start = decoder.optional_i64()?;
    let time_end = decoder.optional_i64()?;
    let change_threshold = f64::from_bits(decoder.u64()?);
    Ok(AlgorithmRequest {
        algorithm,
        source,
        target,
        max_depth,
        max_pairs,
        iterations,
        k,
        damping,
        weight_property,
        departure_property,
        arrival_property,
        event_time_property,
        signal_property,
        time_start,
        time_end,
        change_threshold,
    })
}

fn encode_snapshot(encoder: &mut Encoder, snapshot: &SnapshotProvenance) {
    encoder.u128(snapshot.transaction_id().get());
    encoder.i64(snapshot.start_time().get());
    encoder.u64(snapshot.catalog_version().get());
    encoder.u32(snapshot.shards().len() as u32);
    for (shard, fence) in snapshot.shards() {
        encoder.u64(shard.get());
        encoder.u64(fence.placement_epoch.get());
        encoder.u64(fence.backend_generation.get());
        encoder.u64(fence.applied_index);
        encoder.i64(fence.closed_time.get());
    }
}

fn decode_snapshot(decoder: &mut Decoder<'_>) -> Result<SnapshotProvenance, AnalyticsJobError> {
    let transaction_id =
        TransactionId::new(decoder.u128()?).map_err(|_| AnalyticsJobError::LedgerCorrupt)?;
    let start_time =
        TransactionTime::new(decoder.i64()?).map_err(|_| AnalyticsJobError::LedgerCorrupt)?;
    let catalog_version = Version::new(nonzero(decoder.u64()?)?);
    let shard_count = decoder.u32()? as usize;
    let mut shards = Vec::with_capacity(shard_count);
    for _ in 0..shard_count {
        shards.push((
            ShardId::new(decoder.u64()?).map_err(|_| AnalyticsJobError::LedgerCorrupt)?,
            ShardSnapshotProvenance {
                placement_epoch: PlacementEpoch::new(decoder.u64()?)
                    .map_err(|_| AnalyticsJobError::LedgerCorrupt)?,
                backend_generation: BackendGeneration::new(decoder.u64()?)
                    .map_err(|_| AnalyticsJobError::LedgerCorrupt)?,
                applied_index: decoder.u64()?,
                closed_time: TransactionTime::new(decoder.i64()?)
                    .map_err(|_| AnalyticsJobError::LedgerCorrupt)?,
            },
        ));
    }
    SnapshotProvenance::new(transaction_id, start_time, catalog_version, shards)
        .map_err(|_| AnalyticsJobError::LedgerCorrupt)
}

fn encode_projection(encoder: &mut Encoder, projection: &ProjectionSpec) {
    encoder.boolean(projection.include_reverse());
    encoder.u32(projection.vertex_properties().len() as u32);
    for property in projection.vertex_properties() {
        encode_property(encoder, property);
    }
    encoder.u32(projection.edge_properties().len() as u32);
    for property in projection.edge_properties() {
        encode_property(encoder, property);
    }
}

fn decode_projection(decoder: &mut Decoder<'_>) -> Result<ProjectionSpec, AnalyticsJobError> {
    let include_reverse = decoder.boolean()?;
    let vertex_count = decoder.u32()? as usize;
    let mut vertex_properties = Vec::with_capacity(vertex_count);
    for _ in 0..vertex_count {
        vertex_properties.push(decode_property(decoder)?);
    }
    let edge_count = decoder.u32()? as usize;
    let mut edge_properties = Vec::with_capacity(edge_count);
    for _ in 0..edge_count {
        edge_properties.push(decode_property(decoder)?);
    }
    ProjectionSpec::new(include_reverse, vertex_properties, edge_properties)
        .map_err(|_| AnalyticsJobError::LedgerCorrupt)
}

fn encode_property(encoder: &mut Encoder, property: &PropertyColumnSpec) {
    encoder.string(property.name());
    encoder.u8(property_type_tag(property.property_type()));
    encoder.boolean(property.is_required());
}

fn decode_property(decoder: &mut Decoder<'_>) -> Result<PropertyColumnSpec, AnalyticsJobError> {
    let name = decoder.string()?;
    let property_type = decode_property_type(decoder.u8()?)?;
    Ok(if decoder.boolean()? {
        PropertyColumnSpec::required(name, property_type)
    } else {
        PropertyColumnSpec::optional(name, property_type)
    })
}

fn state_lease_epoch(state: &AnalyticsJobState) -> Option<u64> {
    match state {
        AnalyticsJobState::Claimed { lease_epoch, .. }
        | AnalyticsJobState::Running { lease_epoch, .. } => Some(*lease_epoch),
        _ => None,
    }
}

const fn kind_tag(kind: ArtifactKind) -> u8 {
    match kind {
        ArtifactKind::Checkpoint => 1,
        ArtifactKind::Result => 2,
    }
}

fn decode_kind(tag: u8) -> Result<ArtifactKind, AnalyticsJobError> {
    match tag {
        1 => Ok(ArtifactKind::Checkpoint),
        2 => Ok(ArtifactKind::Result),
        _ => Err(AnalyticsJobError::LedgerCorrupt),
    }
}

const fn algorithm_tag(algorithm: BuiltInAlgorithmId) -> u8 {
    crate::job::algorithm_tag(algorithm)
}

fn decode_algorithm(tag: u8) -> Result<BuiltInAlgorithmId, AnalyticsJobError> {
    match tag {
        1 => Ok(BuiltInAlgorithmId::BreadthFirstSearch),
        2 => Ok(BuiltInAlgorithmId::DepthFirstSearch),
        3 => Ok(BuiltInAlgorithmId::BoundedSingleSourceShortestPath),
        4 => Ok(BuiltInAlgorithmId::BoundedAllPairsShortestPaths),
        5 => Ok(BuiltInAlgorithmId::StronglyConnectedComponents),
        6 => Ok(BuiltInAlgorithmId::WeaklyConnectedComponents),
        7 => Ok(BuiltInAlgorithmId::PageRank),
        8 => Ok(BuiltInAlgorithmId::DegreeCentrality),
        9 => Ok(BuiltInAlgorithmId::ClosenessCentrality),
        10 => Ok(BuiltInAlgorithmId::BetweennessCentrality),
        11 => Ok(BuiltInAlgorithmId::TriangleCount),
        12 => Ok(BuiltInAlgorithmId::ClusteringCoefficient),
        13 => Ok(BuiltInAlgorithmId::KCore),
        14 => Ok(BuiltInAlgorithmId::LabelPropagation),
        15 => Ok(BuiltInAlgorithmId::Louvain),
        16 => Ok(BuiltInAlgorithmId::EarliestArrival),
        17 => Ok(BuiltInAlgorithmId::LatestDeparture),
        18 => Ok(BuiltInAlgorithmId::TemporalReachability),
        19 => Ok(BuiltInAlgorithmId::TemporalMotif),
        20 => Ok(BuiltInAlgorithmId::ChangePoint),
        _ => Err(AnalyticsJobError::LedgerCorrupt),
    }
}

const fn property_type_tag(property_type: PropertyType) -> u8 {
    match property_type {
        PropertyType::Boolean => 1,
        PropertyType::Integer => 2,
        PropertyType::Float => 3,
        PropertyType::Bytes => 4,
        PropertyType::String => 5,
        PropertyType::List => 6,
        PropertyType::Map => 7,
    }
}

fn decode_property_type(tag: u8) -> Result<PropertyType, AnalyticsJobError> {
    match tag {
        1 => Ok(PropertyType::Boolean),
        2 => Ok(PropertyType::Integer),
        3 => Ok(PropertyType::Float),
        4 => Ok(PropertyType::Bytes),
        5 => Ok(PropertyType::String),
        6 => Ok(PropertyType::List),
        7 => Ok(PropertyType::Map),
        _ => Err(AnalyticsJobError::LedgerCorrupt),
    }
}

fn nonzero(value: u64) -> Result<u64, AnalyticsJobError> {
    (value != 0)
        .then_some(value)
        .ok_or(AnalyticsJobError::LedgerCorrupt)
}

fn nonzero_u32(value: u32) -> Result<u32, AnalyticsJobError> {
    (value != 0)
        .then_some(value)
        .ok_or(AnalyticsJobError::LedgerCorrupt)
}

#[derive(Default)]
struct Encoder {
    bytes: Vec<u8>,
}

impl Encoder {
    fn u8(&mut self, value: u8) {
        self.bytes.push(value);
    }

    fn boolean(&mut self, value: bool) {
        self.u8(u8::from(value));
    }

    fn u32(&mut self, value: u32) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    fn u64(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    fn i64(&mut self, value: i64) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    fn u128(&mut self, value: u128) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    fn digest(&mut self, value: Digest32) {
        self.bytes.extend_from_slice(&value.get());
    }

    fn string(&mut self, value: &str) {
        self.u32(value.len() as u32);
        self.bytes.extend_from_slice(value.as_bytes());
    }

    fn optional_string(&mut self, value: Option<&str>) {
        self.boolean(value.is_some());
        if let Some(value) = value {
            self.string(value);
        }
    }

    fn optional_u64(&mut self, value: Option<u64>) {
        self.boolean(value.is_some());
        if let Some(value) = value {
            self.u64(value);
        }
    }

    fn optional_u128(&mut self, value: Option<u128>) {
        self.boolean(value.is_some());
        if let Some(value) = value {
            self.u128(value);
        }
    }

    fn optional_i64(&mut self, value: Option<i64>) {
        self.boolean(value.is_some());
        if let Some(value) = value {
            self.i64(value);
        }
    }
}

struct Decoder<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Decoder<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn is_finished(&self) -> bool {
        self.position == self.bytes.len()
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], AnalyticsJobError> {
        let end = self
            .position
            .checked_add(length)
            .ok_or(AnalyticsJobError::LedgerCorrupt)?;
        let value = self
            .bytes
            .get(self.position..end)
            .ok_or(AnalyticsJobError::LedgerCorrupt)?;
        self.position = end;
        Ok(value)
    }

    fn u8(&mut self) -> Result<u8, AnalyticsJobError> {
        Ok(self.take(1)?[0])
    }

    fn boolean(&mut self) -> Result<bool, AnalyticsJobError> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(AnalyticsJobError::LedgerCorrupt),
        }
    }

    fn u32(&mut self) -> Result<u32, AnalyticsJobError> {
        Ok(u32::from_be_bytes(
            self.take(4)?
                .try_into()
                .map_err(|_| AnalyticsJobError::LedgerCorrupt)?,
        ))
    }

    fn u64(&mut self) -> Result<u64, AnalyticsJobError> {
        Ok(u64::from_be_bytes(
            self.take(8)?
                .try_into()
                .map_err(|_| AnalyticsJobError::LedgerCorrupt)?,
        ))
    }

    fn i64(&mut self) -> Result<i64, AnalyticsJobError> {
        Ok(i64::from_be_bytes(
            self.take(8)?
                .try_into()
                .map_err(|_| AnalyticsJobError::LedgerCorrupt)?,
        ))
    }

    fn u128(&mut self) -> Result<u128, AnalyticsJobError> {
        Ok(u128::from_be_bytes(
            self.take(16)?
                .try_into()
                .map_err(|_| AnalyticsJobError::LedgerCorrupt)?,
        ))
    }

    fn digest(&mut self) -> Result<Digest32, AnalyticsJobError> {
        Ok(Digest32::new(
            self.take(32)?
                .try_into()
                .map_err(|_| AnalyticsJobError::LedgerCorrupt)?,
        ))
    }

    fn string(&mut self) -> Result<String, AnalyticsJobError> {
        let length = self.u32()? as usize;
        let bytes = self.take(length)?;
        std::str::from_utf8(bytes)
            .map(str::to_owned)
            .map_err(|_| AnalyticsJobError::LedgerCorrupt)
    }

    fn optional_string(&mut self) -> Result<Option<String>, AnalyticsJobError> {
        self.boolean()?.then(|| self.string()).transpose()
    }

    fn optional_u64(&mut self) -> Result<Option<u64>, AnalyticsJobError> {
        self.boolean()?.then(|| self.u64()).transpose()
    }

    fn optional_u128(&mut self) -> Result<Option<u128>, AnalyticsJobError> {
        self.boolean()?.then(|| self.u128()).transpose()
    }

    fn optional_i64(&mut self) -> Result<Option<i64>, AnalyticsJobError> {
        self.boolean()?.then(|| self.i64()).transpose()
    }
}
