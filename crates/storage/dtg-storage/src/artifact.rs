use dtg_kernel::Digest32;

use crate::{ReplicaBinding, StorageError, StoreFuture};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub enum ArtifactKind {
    Checkpoint,
    Result,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub struct ArtifactKey {
    job_id: u128,
    generation: u64,
    kind: ArtifactKind,
}

impl ArtifactKey {
    pub fn new(job_id: u128, generation: u64, kind: ArtifactKind) -> Result<Self, StorageError> {
        if job_id == 0 || generation == 0 {
            return Err(StorageError::InvalidArtifact(
                "artifact job and generation identifiers must be nonzero".into(),
            ));
        }
        Ok(Self {
            job_id,
            generation,
            kind,
        })
    }

    pub const fn job_id(self) -> u128 {
        self.job_id
    }

    pub const fn generation(self) -> u64 {
        self.generation
    }

    pub const fn kind(self) -> ArtifactKind {
        self.kind
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArtifactChunk {
    key: ArtifactKey,
    ordinal: u64,
    payload: Vec<u8>,
    digest: Digest32,
}

impl ArtifactChunk {
    pub fn new(key: ArtifactKey, ordinal: u64, payload: Vec<u8>) -> Result<Self, StorageError> {
        if payload.is_empty() {
            return Err(StorageError::InvalidArtifact(
                "artifact chunk payload must be nonempty".into(),
            ));
        }
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"dtg-artifact-chunk-v1");
        hasher.update(&key.job_id.to_be_bytes());
        hasher.update(&key.generation.to_be_bytes());
        hasher.update(&[match key.kind {
            ArtifactKind::Checkpoint => 1,
            ArtifactKind::Result => 2,
        }]);
        hasher.update(&ordinal.to_be_bytes());
        hasher.update(&payload);
        let digest = Digest32::new(*hasher.finalize().as_bytes());
        Ok(Self {
            key,
            ordinal,
            payload,
            digest,
        })
    }

    pub const fn key(&self) -> ArtifactKey {
        self.key
    }

    pub const fn ordinal(&self) -> u64 {
        self.ordinal
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    pub const fn digest(&self) -> Digest32 {
        self.digest
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArtifactManifest {
    key: ArtifactKey,
    chunk_count: u64,
    total_bytes: u64,
    content_digest: Digest32,
}

impl ArtifactManifest {
    pub fn new(key: ArtifactKey, chunks: &[ArtifactChunk]) -> Result<Self, StorageError> {
        if chunks.is_empty()
            || chunks
                .iter()
                .enumerate()
                .any(|(ordinal, chunk)| chunk.key != key || chunk.ordinal != ordinal as u64)
        {
            return Err(StorageError::InvalidArtifact(
                "artifact chunks must be contiguous and share one identity".into(),
            ));
        }
        let total_bytes = chunks.iter().try_fold(0_u64, |total, chunk| {
            total
                .checked_add(chunk.payload.len() as u64)
                .ok_or_else(|| StorageError::InvalidArtifact("artifact byte count overflow".into()))
        })?;
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"dtg-artifact-manifest-v1");
        for chunk in chunks {
            hasher.update(&chunk.digest.get());
        }
        Ok(Self {
            key,
            chunk_count: chunks.len() as u64,
            total_bytes,
            content_digest: Digest32::new(*hasher.finalize().as_bytes()),
        })
    }

    pub const fn key(&self) -> ArtifactKey {
        self.key
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
}

pub trait ArtifactStore: Send + Sync {
    fn binding(&self) -> &ReplicaBinding;
    fn put_chunk(&self, binding: ReplicaBinding, chunk: ArtifactChunk) -> StoreFuture<'_, ()>;
    fn get_chunk(
        &self,
        binding: ReplicaBinding,
        key: ArtifactKey,
        ordinal: u64,
    ) -> StoreFuture<'_, Option<ArtifactChunk>>;
    fn commit_manifest(
        &self,
        binding: ReplicaBinding,
        manifest: ArtifactManifest,
    ) -> StoreFuture<'_, ()>;
    fn manifest(
        &self,
        binding: ReplicaBinding,
        key: ArtifactKey,
    ) -> StoreFuture<'_, Option<ArtifactManifest>>;
    fn delete(&self, binding: ReplicaBinding, key: ArtifactKey) -> StoreFuture<'_, ()>;
}
