use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use dtg_kernel::Digest32;
use dtg_storage::{ArtifactChunk, ArtifactKey, ArtifactKind, ArtifactManifest, ArtifactStore};

use crate::{
    AnalyticsJobError, AnalyticsJobId, AnalyticsJobSpec, AnalyticsLedger, BuiltInAlgorithmId,
    CancellationToken, JobTimestamp,
};

const ARTIFACT_MAGIC: &[u8; 4] = b"DTGA";
const ARTIFACT_VERSION: u32 = 1;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AnalyticsArtifactManifest {
    job_id: AnalyticsJobId,
    lease_epoch: u64,
    generation: u64,
    kind: ArtifactKind,
    schema_version: u32,
    algorithm: BuiltInAlgorithmId,
    algorithm_version: u32,
    snapshot_digest: Digest32,
    spec_digest: Digest32,
    payload_digest: Digest32,
    chunk_count: u64,
    total_bytes: u64,
    content_digest: Digest32,
}

impl AnalyticsArtifactManifest {
    pub const fn job_id(&self) -> AnalyticsJobId {
        self.job_id
    }

    pub const fn lease_epoch(&self) -> u64 {
        self.lease_epoch
    }

    pub const fn generation(&self) -> u64 {
        self.generation
    }

    pub const fn kind(&self) -> ArtifactKind {
        self.kind
    }

    pub const fn schema_version(&self) -> u32 {
        self.schema_version
    }

    pub const fn algorithm(&self) -> BuiltInAlgorithmId {
        self.algorithm
    }

    pub const fn algorithm_version(&self) -> u32 {
        self.algorithm_version
    }

    pub const fn snapshot_digest(&self) -> Digest32 {
        self.snapshot_digest
    }

    pub const fn spec_digest(&self) -> Digest32 {
        self.spec_digest
    }

    pub const fn payload_digest(&self) -> Digest32 {
        self.payload_digest
    }

    pub const fn chunk_count(&self) -> u64 {
        self.chunk_count
    }

    pub const fn total_bytes(&self) -> u64 {
        self.total_bytes
    }

    pub const fn content_digest(&self) -> Digest32 {
        self.content_digest
    }

    pub fn validate_for(
        &self,
        spec: &AnalyticsJobSpec,
        lease_epoch: u64,
        kind: ArtifactKind,
    ) -> Result<(), AnalyticsJobError> {
        let expected_schema = match kind {
            ArtifactKind::Checkpoint => spec.checkpoint_schema_version(),
            ArtifactKind::Result => spec.result_schema_version(),
        };
        if self.job_id != spec.id()
            || self.lease_epoch != lease_epoch
            || self.kind != kind
            || self.schema_version != expected_schema
            || self.algorithm != spec.algorithm()
            || self.algorithm_version != spec.algorithm_version()
            || self.snapshot_digest != spec.snapshot_digest()
            || self.spec_digest != spec.digest()
        {
            return Err(AnalyticsJobError::ArtifactMismatch);
        }
        Ok(())
    }

    pub fn validate_checkpoint_for_resume(
        &self,
        spec: &AnalyticsJobSpec,
    ) -> Result<(), AnalyticsJobError> {
        if self.job_id != spec.id()
            || self.kind != ArtifactKind::Checkpoint
            || self.schema_version != spec.checkpoint_schema_version()
            || self.algorithm != spec.algorithm()
            || self.algorithm_version != spec.algorithm_version()
            || self.snapshot_digest != spec.snapshot_digest()
            || self.spec_digest != spec.digest()
        {
            return Err(AnalyticsJobError::CheckpointIncompatible);
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) const fn from_parts(
        job_id: AnalyticsJobId,
        lease_epoch: u64,
        generation: u64,
        kind: ArtifactKind,
        schema_version: u32,
        algorithm: BuiltInAlgorithmId,
        algorithm_version: u32,
        snapshot_digest: Digest32,
        spec_digest: Digest32,
        payload_digest: Digest32,
        chunk_count: u64,
        total_bytes: u64,
        content_digest: Digest32,
    ) -> Self {
        Self {
            job_id,
            lease_epoch,
            generation,
            kind,
            schema_version,
            algorithm,
            algorithm_version,
            snapshot_digest,
            spec_digest,
            payload_digest,
            chunk_count,
            total_bytes,
            content_digest,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AnalyticsArtifact {
    manifest: AnalyticsArtifactManifest,
    storage_manifest: ArtifactManifest,
    chunks: Vec<ArtifactChunk>,
    payload: Vec<u8>,
}

impl AnalyticsArtifact {
    pub fn new(
        spec: &AnalyticsJobSpec,
        lease_epoch: u64,
        generation: u64,
        kind: ArtifactKind,
        payload: Vec<u8>,
        chunk_size: usize,
    ) -> Result<Self, AnalyticsJobError> {
        if lease_epoch == 0 || generation == 0 || chunk_size == 0 {
            return Err(AnalyticsJobError::InvalidSpec);
        }
        let schema_version = match kind {
            ArtifactKind::Checkpoint => spec.checkpoint_schema_version(),
            ArtifactKind::Result => spec.result_schema_version(),
        };
        let payload_digest = Digest32::new(*blake3::hash(&payload).as_bytes());
        let envelope = encode_envelope(
            spec,
            lease_epoch,
            generation,
            kind,
            schema_version,
            payload_digest,
            &payload,
        );
        let key = ArtifactKey::new(spec.id().get(), generation, kind)
            .map_err(|_| AnalyticsJobError::InvalidSpec)?;
        let chunks = envelope
            .chunks(chunk_size)
            .enumerate()
            .map(|(ordinal, bytes)| {
                ArtifactChunk::new(key, ordinal as u64, bytes.to_vec())
                    .map_err(|_| AnalyticsJobError::ArtifactMismatch)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let storage_manifest =
            ArtifactManifest::new(key, &chunks).map_err(|_| AnalyticsJobError::ArtifactMismatch)?;
        let manifest = AnalyticsArtifactManifest {
            job_id: spec.id(),
            lease_epoch,
            generation,
            kind,
            schema_version,
            algorithm: spec.algorithm(),
            algorithm_version: spec.algorithm_version(),
            snapshot_digest: spec.snapshot_digest(),
            spec_digest: spec.digest(),
            payload_digest,
            chunk_count: storage_manifest.chunk_count(),
            total_bytes: storage_manifest.total_bytes(),
            content_digest: storage_manifest.content_digest(),
        };
        Ok(Self {
            manifest,
            storage_manifest,
            chunks,
            payload,
        })
    }

    pub const fn manifest(&self) -> &AnalyticsArtifactManifest {
        &self.manifest
    }

    pub const fn storage_manifest(&self) -> &ArtifactManifest {
        &self.storage_manifest
    }

    pub fn chunks(&self) -> &[ArtifactChunk] {
        &self.chunks
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    fn from_storage(
        storage_manifest: ArtifactManifest,
        chunks: Vec<ArtifactChunk>,
    ) -> Result<Self, AnalyticsJobError> {
        let rebuilt = ArtifactManifest::new(storage_manifest.key(), &chunks)
            .map_err(|_| AnalyticsJobError::ArtifactMismatch)?;
        if rebuilt != storage_manifest {
            return Err(AnalyticsJobError::ArtifactMismatch);
        }
        let mut envelope = Vec::with_capacity(storage_manifest.total_bytes() as usize);
        for chunk in &chunks {
            envelope.extend_from_slice(chunk.payload());
        }
        let decoded = decode_envelope(&envelope)?;
        let key = storage_manifest.key();
        if decoded.job_id.get() != key.job_id()
            || decoded.generation != key.generation()
            || decoded.kind != key.kind()
        {
            return Err(AnalyticsJobError::ArtifactMismatch);
        }
        let manifest = AnalyticsArtifactManifest {
            job_id: decoded.job_id,
            lease_epoch: decoded.lease_epoch,
            generation: decoded.generation,
            kind: decoded.kind,
            schema_version: decoded.schema_version,
            algorithm: decoded.algorithm,
            algorithm_version: decoded.algorithm_version,
            snapshot_digest: decoded.snapshot_digest,
            spec_digest: decoded.spec_digest,
            payload_digest: decoded.payload_digest,
            chunk_count: storage_manifest.chunk_count(),
            total_bytes: storage_manifest.total_bytes(),
            content_digest: storage_manifest.content_digest(),
        };
        Ok(Self {
            manifest,
            storage_manifest,
            chunks,
            payload: decoded.payload,
        })
    }
}

pub trait AnalyticsArtifactRepository: Send + Sync {
    fn persist(
        &self,
        artifact: &AnalyticsArtifact,
    ) -> Result<AnalyticsArtifactManifest, AnalyticsJobError> {
        let budget = AnalyticsArtifactIoBudget::new(
            artifact.chunks().len().max(1),
            CancellationToken::new(),
        )?;
        self.persist_controlled(artifact, &budget)
    }

    fn persist_controlled(
        &self,
        artifact: &AnalyticsArtifact,
        budget: &AnalyticsArtifactIoBudget,
    ) -> Result<AnalyticsArtifactManifest, AnalyticsJobError>;

    fn load(
        &self,
        job_id: AnalyticsJobId,
        generation: u64,
        kind: ArtifactKind,
    ) -> Result<Option<AnalyticsArtifact>, AnalyticsJobError> {
        let budget = AnalyticsArtifactIoBudget::new(4_096, CancellationToken::new())?;
        self.load_controlled(job_id, generation, kind, &budget)
    }

    fn load_controlled(
        &self,
        job_id: AnalyticsJobId,
        generation: u64,
        kind: ArtifactKind,
        budget: &AnalyticsArtifactIoBudget,
    ) -> Result<Option<AnalyticsArtifact>, AnalyticsJobError>;

    fn delete(
        &self,
        job_id: AnalyticsJobId,
        generation: u64,
        kind: ArtifactKind,
    ) -> Result<(), AnalyticsJobError>;
}

#[derive(Clone, Debug)]
pub struct AnalyticsArtifactIoBudget {
    max_chunks: usize,
    cancellation: CancellationToken,
}

impl AnalyticsArtifactIoBudget {
    pub fn new(
        max_chunks: usize,
        cancellation: CancellationToken,
    ) -> Result<Self, AnalyticsJobError> {
        if max_chunks == 0 {
            return Err(AnalyticsJobError::BudgetExhausted);
        }
        Ok(Self {
            max_chunks,
            cancellation,
        })
    }

    pub const fn max_chunks(&self) -> usize {
        self.max_chunks
    }

    pub const fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }

    fn validate(&self, chunks: usize) -> Result<(), AnalyticsJobError> {
        if self.cancellation.is_cancelled() {
            return Err(AnalyticsJobError::Cancelled);
        }
        if chunks > self.max_chunks {
            return Err(AnalyticsJobError::BudgetExhausted);
        }
        Ok(())
    }
}

pub struct StorageArtifactRepository<S> {
    store: Arc<S>,
}

pub struct AnalyticsArtifactGarbageCollector<R> {
    repository: R,
    scan_budget: usize,
}

impl<R> AnalyticsArtifactGarbageCollector<R>
where
    R: AnalyticsArtifactRepository,
{
    pub fn new(repository: R, scan_budget: usize) -> Result<Self, AnalyticsJobError> {
        if scan_budget == 0 {
            return Err(AnalyticsJobError::BudgetExhausted);
        }
        Ok(Self {
            repository,
            scan_budget,
        })
    }

    pub fn collect(
        &self,
        ledger: &mut AnalyticsLedger,
        now: JobTimestamp,
        max_deletions: usize,
        cancellation: &CancellationToken,
    ) -> Result<usize, AnalyticsJobError> {
        if max_deletions == 0 {
            return Err(AnalyticsJobError::BudgetExhausted);
        }
        let mut deleted = 0;
        while deleted < max_deletions {
            if cancellation.is_cancelled() {
                return Err(AnalyticsJobError::Cancelled);
            }
            let Some(candidate) = ledger.next_gc_candidate(now, self.scan_budget, cancellation)?
            else {
                break;
            };
            let manifest = candidate.manifest();
            self.repository
                .delete(candidate.job_id(), manifest.generation(), manifest.kind())?;
            ledger.acknowledge_artifact_reclaimed(candidate, now)?;
            deleted += 1;
        }
        Ok(deleted)
    }

    pub const fn repository(&self) -> &R {
        &self.repository
    }
}

impl<S> Clone for StorageArtifactRepository<S> {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
        }
    }
}

impl<S> StorageArtifactRepository<S> {
    pub const fn new(store: Arc<S>) -> Self {
        Self { store }
    }

    pub const fn store(&self) -> &Arc<S> {
        &self.store
    }
}

impl<S> AnalyticsArtifactRepository for StorageArtifactRepository<S>
where
    S: ArtifactStore + 'static,
{
    fn persist_controlled(
        &self,
        artifact: &AnalyticsArtifact,
        budget: &AnalyticsArtifactIoBudget,
    ) -> Result<AnalyticsArtifactManifest, AnalyticsJobError> {
        budget.validate(artifact.chunks().len())?;
        let binding = self.store.binding().clone();
        for chunk in artifact.chunks() {
            if budget.cancellation.is_cancelled() {
                return Err(AnalyticsJobError::Cancelled);
            }
            block_on(self.store.put_chunk(binding.clone(), chunk.clone()))
                .map_err(|_| AnalyticsJobError::Storage)?;
        }
        if budget.cancellation.is_cancelled() {
            return Err(AnalyticsJobError::Cancelled);
        }
        block_on(
            self.store
                .commit_manifest(binding, artifact.storage_manifest().clone()),
        )
        .map_err(|_| AnalyticsJobError::Storage)?;
        Ok(artifact.manifest().clone())
    }

    fn load_controlled(
        &self,
        job_id: AnalyticsJobId,
        generation: u64,
        kind: ArtifactKind,
        budget: &AnalyticsArtifactIoBudget,
    ) -> Result<Option<AnalyticsArtifact>, AnalyticsJobError> {
        if budget.cancellation.is_cancelled() {
            return Err(AnalyticsJobError::Cancelled);
        }
        let key = ArtifactKey::new(job_id.get(), generation, kind)
            .map_err(|_| AnalyticsJobError::ArtifactMismatch)?;
        let binding = self.store.binding().clone();
        let Some(manifest) = block_on(self.store.manifest(binding.clone(), key))
            .map_err(|_| AnalyticsJobError::Storage)?
        else {
            return Ok(None);
        };
        let chunk_count = usize::try_from(manifest.chunk_count())
            .map_err(|_| AnalyticsJobError::BudgetExhausted)?;
        budget.validate(chunk_count)?;
        let mut chunks = Vec::with_capacity(manifest.chunk_count() as usize);
        for ordinal in 0..manifest.chunk_count() {
            if budget.cancellation.is_cancelled() {
                return Err(AnalyticsJobError::Cancelled);
            }
            let chunk = block_on(self.store.get_chunk(binding.clone(), key, ordinal))
                .map_err(|_| AnalyticsJobError::Storage)?
                .ok_or(AnalyticsJobError::ArtifactMismatch)?;
            chunks.push(chunk);
        }
        AnalyticsArtifact::from_storage(manifest, chunks).map(Some)
    }

    fn delete(
        &self,
        job_id: AnalyticsJobId,
        generation: u64,
        kind: ArtifactKind,
    ) -> Result<(), AnalyticsJobError> {
        let key = ArtifactKey::new(job_id.get(), generation, kind)
            .map_err(|_| AnalyticsJobError::ArtifactMismatch)?;
        let binding = self.store.binding().clone();
        block_on(self.store.delete(binding, key)).map_err(|_| AnalyticsJobError::Storage)
    }
}

#[allow(clippy::too_many_arguments)]
fn encode_envelope(
    spec: &AnalyticsJobSpec,
    lease_epoch: u64,
    generation: u64,
    kind: ArtifactKind,
    schema_version: u32,
    payload_digest: Digest32,
    payload: &[u8],
) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(150 + payload.len());
    bytes.extend_from_slice(ARTIFACT_MAGIC);
    bytes.extend_from_slice(&ARTIFACT_VERSION.to_be_bytes());
    bytes.extend_from_slice(&spec.id().get().to_be_bytes());
    bytes.extend_from_slice(&lease_epoch.to_be_bytes());
    bytes.extend_from_slice(&generation.to_be_bytes());
    bytes.push(match kind {
        ArtifactKind::Checkpoint => 1,
        ArtifactKind::Result => 2,
    });
    bytes.extend_from_slice(&schema_version.to_be_bytes());
    bytes.push(crate::job::algorithm_tag(spec.algorithm()));
    bytes.extend_from_slice(&spec.algorithm_version().to_be_bytes());
    bytes.extend_from_slice(&spec.snapshot_digest().get());
    bytes.extend_from_slice(&spec.digest().get());
    bytes.extend_from_slice(&payload_digest.get());
    bytes.extend_from_slice(&(payload.len() as u64).to_be_bytes());
    bytes.extend_from_slice(payload);
    bytes
}

struct DecodedEnvelope {
    job_id: AnalyticsJobId,
    lease_epoch: u64,
    generation: u64,
    kind: ArtifactKind,
    schema_version: u32,
    algorithm: BuiltInAlgorithmId,
    algorithm_version: u32,
    snapshot_digest: Digest32,
    spec_digest: Digest32,
    payload_digest: Digest32,
    payload: Vec<u8>,
}

fn decode_envelope(bytes: &[u8]) -> Result<DecodedEnvelope, AnalyticsJobError> {
    const HEADER_LENGTH: usize = 154;
    if bytes.len() < HEADER_LENGTH || &bytes[..4] != ARTIFACT_MAGIC {
        return Err(AnalyticsJobError::ArtifactMismatch);
    }
    let mut cursor = ArtifactCursor::new(bytes);
    cursor.take(4)?;
    if cursor.u32()? != ARTIFACT_VERSION {
        return Err(AnalyticsJobError::CheckpointIncompatible);
    }
    let job_id = AnalyticsJobId::new(cursor.u128()?)?;
    let lease_epoch = nonzero(cursor.u64()?)?;
    let generation = nonzero(cursor.u64()?)?;
    let kind = match cursor.u8()? {
        1 => ArtifactKind::Checkpoint,
        2 => ArtifactKind::Result,
        _ => return Err(AnalyticsJobError::ArtifactMismatch),
    };
    let schema_version = nonzero_u32(cursor.u32()?)?;
    let algorithm = decode_algorithm(cursor.u8()?)?;
    let algorithm_version = nonzero_u32(cursor.u32()?)?;
    let snapshot_digest = cursor.digest()?;
    let spec_digest = cursor.digest()?;
    let payload_digest = cursor.digest()?;
    let payload_length =
        usize::try_from(cursor.u64()?).map_err(|_| AnalyticsJobError::ArtifactMismatch)?;
    let payload = cursor.take(payload_length)?.to_vec();
    if !cursor.is_finished() || Digest32::new(*blake3::hash(&payload).as_bytes()) != payload_digest
    {
        return Err(AnalyticsJobError::ArtifactMismatch);
    }
    Ok(DecodedEnvelope {
        job_id,
        lease_epoch,
        generation,
        kind,
        schema_version,
        algorithm,
        algorithm_version,
        snapshot_digest,
        spec_digest,
        payload_digest,
        payload,
    })
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
        _ => Err(AnalyticsJobError::ArtifactMismatch),
    }
}

fn nonzero(value: u64) -> Result<u64, AnalyticsJobError> {
    (value != 0)
        .then_some(value)
        .ok_or(AnalyticsJobError::ArtifactMismatch)
}

fn nonzero_u32(value: u32) -> Result<u32, AnalyticsJobError> {
    (value != 0)
        .then_some(value)
        .ok_or(AnalyticsJobError::ArtifactMismatch)
}

struct ArtifactCursor<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> ArtifactCursor<'a> {
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
            .ok_or(AnalyticsJobError::ArtifactMismatch)?;
        let value = self
            .bytes
            .get(self.position..end)
            .ok_or(AnalyticsJobError::ArtifactMismatch)?;
        self.position = end;
        Ok(value)
    }

    fn u8(&mut self) -> Result<u8, AnalyticsJobError> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, AnalyticsJobError> {
        Ok(u32::from_be_bytes(
            self.take(4)?
                .try_into()
                .map_err(|_| AnalyticsJobError::ArtifactMismatch)?,
        ))
    }

    fn u64(&mut self) -> Result<u64, AnalyticsJobError> {
        Ok(u64::from_be_bytes(
            self.take(8)?
                .try_into()
                .map_err(|_| AnalyticsJobError::ArtifactMismatch)?,
        ))
    }

    fn u128(&mut self) -> Result<u128, AnalyticsJobError> {
        Ok(u128::from_be_bytes(
            self.take(16)?
                .try_into()
                .map_err(|_| AnalyticsJobError::ArtifactMismatch)?,
        ))
    }

    fn digest(&mut self) -> Result<Digest32, AnalyticsJobError> {
        Ok(Digest32::new(
            self.take(32)?
                .try_into()
                .map_err(|_| AnalyticsJobError::ArtifactMismatch)?,
        ))
    }
}

struct ThreadWaker(std::thread::Thread);

impl Wake for ThreadWaker {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::from(Arc::new(ThreadWaker(std::thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = Box::pin(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => std::thread::park(),
        }
    }
}
