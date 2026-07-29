use dtg_kernel::Digest32;
use dtg_language_ir::{AnalyticsRequestIdentity, BuiltInAlgorithmId};

use crate::{AlgorithmCatalog, AlgorithmRequest, CsrShape, ProjectionSpec, SnapshotProvenance};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub struct AnalyticsJobId(u128);

impl AnalyticsJobId {
    pub fn new(value: u128) -> Result<Self, AnalyticsJobError> {
        (value != 0)
            .then_some(Self(value))
            .ok_or(AnalyticsJobError::InvalidJobId)
    }

    pub const fn get(self) -> u128 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub struct WorkerId(u64);

impl WorkerId {
    pub fn new(value: u64) -> Result<Self, AnalyticsJobError> {
        (value != 0)
            .then_some(Self(value))
            .ok_or(AnalyticsJobError::InvalidWorkerId)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub struct JobTimestamp(u64);

impl JobTimestamp {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    pub(crate) fn checked_add(self, value: u64) -> Result<Self, AnalyticsJobError> {
        self.0
            .checked_add(value)
            .map(Self)
            .ok_or(AnalyticsJobError::TimeOverflow)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct AnalyticsJobSpec {
    id: AnalyticsJobId,
    request_identity: String,
    request: AlgorithmRequest,
    snapshot: SnapshotProvenance,
    projection: ProjectionSpec,
    snapshot_digest: Digest32,
    topology_digest: Digest32,
    backend_fence_digest: Digest32,
    provider_version: u32,
    algorithm_version: u32,
    result_schema_version: u32,
    checkpoint_schema_version: u32,
    max_attempts: u32,
    retention_until: JobTimestamp,
    digest: Digest32,
}

impl AnalyticsJobSpec {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        request_identity: AnalyticsRequestIdentity,
        request: AlgorithmRequest,
        snapshot: SnapshotProvenance,
        projection: ProjectionSpec,
        topology_digest: Digest32,
        backend_fence_digest: Digest32,
        provider_version: u32,
        algorithm_version: u32,
        max_attempts: u32,
        retention_until: JobTimestamp,
    ) -> Result<Self, AnalyticsJobError> {
        let identity = request_identity.as_str();
        if identity.len() > 256 {
            return Err(AnalyticsJobError::InvalidRequestIdentity);
        }
        if provider_version == 0 || algorithm_version == 0 || max_attempts == 0 {
            return Err(AnalyticsJobError::InvalidSpec);
        }
        let descriptor = AlgorithmCatalog::descriptor(request.algorithm);
        if descriptor.required_shape == CsrShape::ForwardAndReverse && !projection.include_reverse()
        {
            return Err(AnalyticsJobError::InvalidProjection);
        }
        for required in descriptor.required_edge_properties {
            if !projection
                .edge_properties()
                .iter()
                .any(|property| property.name() == *required && property.is_required())
            {
                return Err(AnalyticsJobError::InvalidProjection);
            }
        }

        let id = derive_job_id(identity);
        let snapshot_digest = digest_snapshot(&snapshot);
        let digest = digest_spec(
            identity,
            &request,
            &snapshot,
            &projection,
            topology_digest,
            backend_fence_digest,
            provider_version,
            algorithm_version,
            descriptor.result_schema_version,
            descriptor.checkpoint_schema_version,
            max_attempts,
            retention_until,
        );
        Ok(Self {
            id,
            request_identity: identity.to_owned(),
            request,
            snapshot,
            projection,
            snapshot_digest,
            topology_digest,
            backend_fence_digest,
            provider_version,
            algorithm_version,
            result_schema_version: descriptor.result_schema_version,
            checkpoint_schema_version: descriptor.checkpoint_schema_version,
            max_attempts,
            retention_until,
            digest,
        })
    }

    pub const fn id(&self) -> AnalyticsJobId {
        self.id
    }

    pub fn request_identity(&self) -> &str {
        &self.request_identity
    }

    pub const fn request(&self) -> &AlgorithmRequest {
        &self.request
    }

    pub const fn algorithm(&self) -> BuiltInAlgorithmId {
        self.request.algorithm
    }

    pub const fn snapshot(&self) -> &SnapshotProvenance {
        &self.snapshot
    }

    pub const fn projection(&self) -> &ProjectionSpec {
        &self.projection
    }

    pub const fn snapshot_digest(&self) -> Digest32 {
        self.snapshot_digest
    }

    pub const fn topology_digest(&self) -> Digest32 {
        self.topology_digest
    }

    pub const fn backend_fence_digest(&self) -> Digest32 {
        self.backend_fence_digest
    }

    pub const fn provider_version(&self) -> u32 {
        self.provider_version
    }

    pub const fn algorithm_version(&self) -> u32 {
        self.algorithm_version
    }

    pub const fn result_schema_version(&self) -> u32 {
        self.result_schema_version
    }

    pub const fn checkpoint_schema_version(&self) -> u32 {
        self.checkpoint_schema_version
    }

    pub const fn max_attempts(&self) -> u32 {
        self.max_attempts
    }

    pub const fn retention_until(&self) -> JobTimestamp {
        self.retention_until
    }

    pub const fn digest(&self) -> Digest32 {
        self.digest
    }

    pub fn runtime_fences(&self) -> AnalyticsRuntimeFences {
        AnalyticsRuntimeFences {
            snapshot_digest: self.snapshot_digest,
            topology_digest: self.topology_digest,
            backend_fence_digest: self.backend_fence_digest,
            provider_version: self.provider_version,
            algorithm_version: self.algorithm_version,
            result_schema_version: self.result_schema_version,
            checkpoint_schema_version: self.checkpoint_schema_version,
        }
    }

    pub fn validate_runtime_fences(
        &self,
        fences: AnalyticsRuntimeFences,
    ) -> Result<(), AnalyticsJobError> {
        (self.runtime_fences() == fences)
            .then_some(())
            .ok_or(AnalyticsJobError::FenceDrift)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AnalyticsRuntimeFences {
    pub snapshot_digest: Digest32,
    pub topology_digest: Digest32,
    pub backend_fence_digest: Digest32,
    pub provider_version: u32,
    pub algorithm_version: u32,
    pub result_schema_version: u32,
    pub checkpoint_schema_version: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AnalyticsJobStateKind {
    Queued,
    Claimed,
    Running,
    Succeeded,
    Failed,
    Cancelled,
    Tombstoned,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AnalyticsJobState {
    Queued,
    Claimed {
        worker: WorkerId,
        lease_epoch: u64,
        expires_at: JobTimestamp,
    },
    Running {
        worker: WorkerId,
        lease_epoch: u64,
        expires_at: JobTimestamp,
        checkpoint: Option<u64>,
    },
    Succeeded {
        result_generation: u64,
    },
    Failed {
        retryable: bool,
        code: String,
    },
    Cancelled,
    Tombstoned,
}

impl AnalyticsJobState {
    pub const fn kind(&self) -> AnalyticsJobStateKind {
        match self {
            Self::Queued => AnalyticsJobStateKind::Queued,
            Self::Claimed { .. } => AnalyticsJobStateKind::Claimed,
            Self::Running { .. } => AnalyticsJobStateKind::Running,
            Self::Succeeded { .. } => AnalyticsJobStateKind::Succeeded,
            Self::Failed { .. } => AnalyticsJobStateKind::Failed,
            Self::Cancelled => AnalyticsJobStateKind::Cancelled,
            Self::Tombstoned => AnalyticsJobStateKind::Tombstoned,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JobCas {
    state: AnalyticsJobStateKind,
    revision: u64,
    lease_epoch: u64,
}

impl JobCas {
    pub const fn new(state: AnalyticsJobStateKind, revision: u64, lease_epoch: u64) -> Self {
        Self {
            state,
            revision,
            lease_epoch,
        }
    }

    pub const fn state(self) -> AnalyticsJobStateKind {
        self.state
    }

    pub const fn revision(self) -> u64 {
        self.revision
    }

    pub const fn lease_epoch(self) -> u64 {
        self.lease_epoch
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JobLease {
    job_id: AnalyticsJobId,
    worker: WorkerId,
    lease_epoch: u64,
    revision: u64,
    expires_at: JobTimestamp,
}

impl JobLease {
    pub(crate) const fn new(
        job_id: AnalyticsJobId,
        worker: WorkerId,
        lease_epoch: u64,
        revision: u64,
        expires_at: JobTimestamp,
    ) -> Self {
        Self {
            job_id,
            worker,
            lease_epoch,
            revision,
            expires_at,
        }
    }

    pub const fn job_id(self) -> AnalyticsJobId {
        self.job_id
    }

    pub const fn worker(self) -> WorkerId {
        self.worker
    }

    pub const fn lease_epoch(self) -> u64 {
        self.lease_epoch
    }

    pub const fn revision(self) -> u64 {
        self.revision
    }

    pub const fn expires_at(self) -> JobTimestamp {
        self.expires_at
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AnalyticsJobError {
    InvalidJobId,
    InvalidWorkerId,
    InvalidRequestIdentity,
    InvalidSpec,
    InvalidProjection,
    TimeOverflow,
    SpecConflict,
    JobNotFound,
    StaleState,
    StaleRevision,
    StaleLease,
    LeaseExpired,
    StaleGeneration,
    ArtifactMismatch,
    ArtifactPinned,
    RetentionActive,
    InvalidTransition,
    FenceDrift,
    CheckpointIncompatible,
    LedgerVersion,
    LedgerCorrupt,
    BudgetExhausted,
    Cancelled,
    Storage,
}

impl AnalyticsJobError {
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidJobId => "DTG-ANALYTICS-JOB-ID",
            Self::InvalidWorkerId => "DTG-ANALYTICS-WORKER-ID",
            Self::InvalidRequestIdentity => "DTG-ANALYTICS-REQUEST-IDENTITY",
            Self::InvalidSpec => "DTG-ANALYTICS-SPEC-INVALID",
            Self::InvalidProjection => "DTG-ANALYTICS-PROJECTION-INVALID",
            Self::TimeOverflow => "DTG-ANALYTICS-TIME-OVERFLOW",
            Self::SpecConflict => "DTG-ANALYTICS-SPEC-CONFLICT",
            Self::JobNotFound => "DTG-ANALYTICS-JOB-NOT-FOUND",
            Self::StaleState => "DTG-ANALYTICS-STALE-STATE",
            Self::StaleRevision => "DTG-ANALYTICS-STALE-REVISION",
            Self::StaleLease => "DTG-ANALYTICS-STALE-LEASE",
            Self::LeaseExpired => "DTG-ANALYTICS-LEASE-EXPIRED",
            Self::StaleGeneration => "DTG-ANALYTICS-STALE-GENERATION",
            Self::ArtifactMismatch => "DTG-ANALYTICS-ARTIFACT-MISMATCH",
            Self::ArtifactPinned => "DTG-ANALYTICS-ARTIFACT-PINNED",
            Self::RetentionActive => "DTG-ANALYTICS-RETENTION-ACTIVE",
            Self::InvalidTransition => "DTG-ANALYTICS-INVALID-TRANSITION",
            Self::FenceDrift => "DTG-ANALYTICS-FENCE-DRIFT",
            Self::CheckpointIncompatible => "DTG-ANALYTICS-CHECKPOINT-INCOMPATIBLE",
            Self::LedgerVersion => "DTG-ANALYTICS-LEDGER-VERSION",
            Self::LedgerCorrupt => "DTG-ANALYTICS-LEDGER-CORRUPT",
            Self::BudgetExhausted => "DTG-ANALYTICS-BUDGET-EXHAUSTED",
            Self::Cancelled => "DTG-ANALYTICS-CANCELLED",
            Self::Storage => "DTG-ANALYTICS-STORAGE",
        }
    }
}

impl std::fmt::Display for AnalyticsJobError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.code())
    }
}

impl std::error::Error for AnalyticsJobError {}

fn derive_job_id(identity: &str) -> AnalyticsJobId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"dtg-analytics-job-id-v1");
    encode_string(&mut hasher, identity);
    let bytes = hasher.finalize();
    let mut value = [0_u8; 16];
    value.copy_from_slice(&bytes.as_bytes()[..16]);
    let value = u128::from_be_bytes(value).max(1);
    AnalyticsJobId(value)
}

fn digest_snapshot(snapshot: &SnapshotProvenance) -> Digest32 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"dtg-analytics-snapshot-fence-v1");
    encode_snapshot(&mut hasher, snapshot);
    Digest32::new(*hasher.finalize().as_bytes())
}

#[allow(clippy::too_many_arguments)]
fn digest_spec(
    identity: &str,
    request: &AlgorithmRequest,
    snapshot: &SnapshotProvenance,
    projection: &ProjectionSpec,
    topology_digest: Digest32,
    backend_fence_digest: Digest32,
    provider_version: u32,
    algorithm_version: u32,
    result_schema_version: u32,
    checkpoint_schema_version: u32,
    max_attempts: u32,
    retention_until: JobTimestamp,
) -> Digest32 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"dtg-analytics-job-spec-v1");
    encode_string(&mut hasher, identity);
    encode_request(&mut hasher, request);
    encode_snapshot(&mut hasher, snapshot);
    encode_projection(&mut hasher, projection);
    hasher.update(&topology_digest.get());
    hasher.update(&backend_fence_digest.get());
    hasher.update(&provider_version.to_be_bytes());
    hasher.update(&algorithm_version.to_be_bytes());
    hasher.update(&result_schema_version.to_be_bytes());
    hasher.update(&checkpoint_schema_version.to_be_bytes());
    hasher.update(&max_attempts.to_be_bytes());
    hasher.update(&retention_until.get().to_be_bytes());
    Digest32::new(*hasher.finalize().as_bytes())
}

fn encode_snapshot(hasher: &mut blake3::Hasher, snapshot: &SnapshotProvenance) {
    hasher.update(&snapshot.transaction_id().get().to_be_bytes());
    hasher.update(&snapshot.start_time().get().to_be_bytes());
    hasher.update(&snapshot.catalog_version().get().to_be_bytes());
    hasher.update(&(snapshot.shards().len() as u64).to_be_bytes());
    for (shard, fence) in snapshot.shards() {
        hasher.update(&shard.get().to_be_bytes());
        hasher.update(&fence.placement_epoch.get().to_be_bytes());
        hasher.update(&fence.backend_generation.get().to_be_bytes());
        hasher.update(&fence.applied_index.to_be_bytes());
        hasher.update(&fence.closed_time.get().to_be_bytes());
    }
}

fn encode_projection(hasher: &mut blake3::Hasher, projection: &ProjectionSpec) {
    hasher.update(&[u8::from(projection.include_reverse())]);
    hasher.update(&(projection.vertex_properties().len() as u64).to_be_bytes());
    for property in projection.vertex_properties() {
        encode_string(hasher, property.name());
        hasher.update(&[property_type_tag(property.property_type())]);
        hasher.update(&[u8::from(property.is_required())]);
    }
    hasher.update(&(projection.edge_properties().len() as u64).to_be_bytes());
    for property in projection.edge_properties() {
        encode_string(hasher, property.name());
        hasher.update(&[property_type_tag(property.property_type())]);
        hasher.update(&[u8::from(property.is_required())]);
    }
}

fn encode_request(hasher: &mut blake3::Hasher, request: &AlgorithmRequest) {
    hasher.update(&[algorithm_tag(request.algorithm)]);
    encode_optional_u128(hasher, request.source.map(|id| id.get()));
    encode_optional_u128(hasher, request.target.map(|id| id.get()));
    hasher.update(&request.max_depth.to_be_bytes());
    hasher.update(&(request.max_pairs as u64).to_be_bytes());
    hasher.update(&request.iterations.to_be_bytes());
    hasher.update(&request.k.to_be_bytes());
    hasher.update(&request.damping.to_bits().to_be_bytes());
    encode_optional_string(hasher, request.weight_property.as_deref());
    encode_optional_string(hasher, request.departure_property.as_deref());
    encode_optional_string(hasher, request.arrival_property.as_deref());
    encode_optional_string(hasher, request.event_time_property.as_deref());
    encode_optional_string(hasher, request.signal_property.as_deref());
    encode_optional_i64(hasher, request.time_start);
    encode_optional_i64(hasher, request.time_end);
    hasher.update(&request.change_threshold.to_bits().to_be_bytes());
}

pub(crate) const fn algorithm_tag(algorithm: BuiltInAlgorithmId) -> u8 {
    match algorithm {
        BuiltInAlgorithmId::BreadthFirstSearch => 1,
        BuiltInAlgorithmId::DepthFirstSearch => 2,
        BuiltInAlgorithmId::BoundedSingleSourceShortestPath => 3,
        BuiltInAlgorithmId::BoundedAllPairsShortestPaths => 4,
        BuiltInAlgorithmId::StronglyConnectedComponents => 5,
        BuiltInAlgorithmId::WeaklyConnectedComponents => 6,
        BuiltInAlgorithmId::PageRank => 7,
        BuiltInAlgorithmId::DegreeCentrality => 8,
        BuiltInAlgorithmId::ClosenessCentrality => 9,
        BuiltInAlgorithmId::BetweennessCentrality => 10,
        BuiltInAlgorithmId::TriangleCount => 11,
        BuiltInAlgorithmId::ClusteringCoefficient => 12,
        BuiltInAlgorithmId::KCore => 13,
        BuiltInAlgorithmId::LabelPropagation => 14,
        BuiltInAlgorithmId::Louvain => 15,
        BuiltInAlgorithmId::EarliestArrival => 16,
        BuiltInAlgorithmId::LatestDeparture => 17,
        BuiltInAlgorithmId::TemporalReachability => 18,
        BuiltInAlgorithmId::TemporalMotif => 19,
        BuiltInAlgorithmId::ChangePoint => 20,
    }
}

fn property_type_tag(property_type: crate::PropertyType) -> u8 {
    match property_type {
        crate::PropertyType::Boolean => 1,
        crate::PropertyType::Integer => 2,
        crate::PropertyType::Float => 3,
        crate::PropertyType::Bytes => 4,
        crate::PropertyType::String => 5,
        crate::PropertyType::List => 6,
        crate::PropertyType::Map => 7,
    }
}

fn encode_string(hasher: &mut blake3::Hasher, value: &str) {
    hasher.update(&(value.len() as u64).to_be_bytes());
    hasher.update(value.as_bytes());
}

fn encode_optional_string(hasher: &mut blake3::Hasher, value: Option<&str>) {
    match value {
        Some(value) => {
            hasher.update(&[1]);
            encode_string(hasher, value);
        }
        None => {
            hasher.update(&[0]);
        }
    }
}

fn encode_optional_u128(hasher: &mut blake3::Hasher, value: Option<u128>) {
    match value {
        Some(value) => {
            hasher.update(&[1]);
            hasher.update(&value.to_be_bytes());
        }
        None => {
            hasher.update(&[0]);
        }
    }
}

fn encode_optional_i64(hasher: &mut blake3::Hasher, value: Option<i64>) {
    match value {
        Some(value) => {
            hasher.update(&[1]);
            hasher.update(&value.to_be_bytes());
        }
        None => {
            hasher.update(&[0]);
        }
    }
}
