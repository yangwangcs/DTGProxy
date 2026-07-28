use std::{fmt, path::Path};

use dtg_storage::{
    ArtifactChunk, ArtifactKey, ArtifactKind, ArtifactManifest, ArtifactStore, ReplicaBinding,
    StorageError, StoreFuture,
};
use fjall::PersistMode;

use crate::{
    codec::{
        decode_artifact_chunk, decode_artifact_manifest, encode_artifact_chunk,
        encode_artifact_manifest,
    },
    namespace::{NamespaceDb, fjall_error},
};

pub struct FjallArtifactStore {
    namespace: NamespaceDb,
    binding: ReplicaBinding,
}

impl fmt::Debug for FjallArtifactStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FjallArtifactStore")
            .field("binding", &self.binding)
            .finish_non_exhaustive()
    }
}

impl FjallArtifactStore {
    pub fn open(path: impl AsRef<Path>, binding: ReplicaBinding) -> Result<Self, StorageError> {
        Ok(Self {
            namespace: NamespaceDb::open(path.as_ref(), &binding)?,
            binding,
        })
    }

    fn verify_binding(&self, binding: &ReplicaBinding) -> Result<(), StorageError> {
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

impl ArtifactStore for FjallArtifactStore {
    fn binding(&self) -> &ReplicaBinding {
        &self.binding
    }

    fn put_chunk(&self, binding: ReplicaBinding, chunk: ArtifactChunk) -> StoreFuture<'_, ()> {
        Box::pin(async move {
            self.verify_binding(&binding)?;
            self.namespace
                .artifact
                .insert(
                    chunk_key(chunk.key(), chunk.ordinal()),
                    encode_artifact_chunk(&chunk)?,
                )
                .map_err(fjall_error)?;
            self.namespace
                .db
                .persist(PersistMode::SyncAll)
                .map_err(fjall_error)
        })
    }

    fn get_chunk(
        &self,
        binding: ReplicaBinding,
        key: ArtifactKey,
        ordinal: u64,
    ) -> StoreFuture<'_, Option<ArtifactChunk>> {
        Box::pin(async move {
            self.verify_binding(&binding)?;
            self.namespace
                .artifact
                .get(chunk_key(key, ordinal))
                .map_err(fjall_error)?
                .map(|bytes| decode_artifact_chunk(&bytes))
                .transpose()
        })
    }

    fn commit_manifest(
        &self,
        binding: ReplicaBinding,
        manifest: ArtifactManifest,
    ) -> StoreFuture<'_, ()> {
        Box::pin(async move {
            self.verify_binding(&binding)?;
            let chunks = load_chunks(&self.namespace, manifest.key())?;
            let expected = ArtifactManifest::new(manifest.key(), &chunks)?;
            if expected != manifest {
                return Err(StorageError::InvalidArtifact(
                    "artifact manifest does not match stored chunks".into(),
                ));
            }
            self.namespace
                .artifact
                .insert(
                    manifest_key(manifest.key()),
                    encode_artifact_manifest(&manifest)?,
                )
                .map_err(fjall_error)?;
            self.namespace
                .db
                .persist(PersistMode::SyncAll)
                .map_err(fjall_error)
        })
    }

    fn manifest(
        &self,
        binding: ReplicaBinding,
        key: ArtifactKey,
    ) -> StoreFuture<'_, Option<ArtifactManifest>> {
        Box::pin(async move {
            self.verify_binding(&binding)?;
            let Some(bytes) = self
                .namespace
                .artifact
                .get(manifest_key(key))
                .map_err(fjall_error)?
            else {
                return Ok(None);
            };
            let stored = decode_artifact_manifest(&bytes)?;
            if stored.key != key {
                return Err(StorageError::InvalidArtifact(
                    "stored artifact manifest identity mismatch".into(),
                ));
            }
            let manifest = ArtifactManifest::new(key, &load_chunks(&self.namespace, key)?)?;
            if manifest.chunk_count() != stored.chunk_count
                || manifest.total_bytes() != stored.total_bytes
                || manifest.content_digest() != stored.content_digest
            {
                return Err(StorageError::InvalidArtifact(
                    "stored artifact manifest is corrupt".into(),
                ));
            }
            Ok(Some(manifest))
        })
    }

    fn delete(&self, binding: ReplicaBinding, key: ArtifactKey) -> StoreFuture<'_, ()> {
        Box::pin(async move {
            self.verify_binding(&binding)?;
            let prefix = chunk_prefix(key);
            let keys = self
                .namespace
                .artifact
                .prefix(&prefix)
                .map(|item| item.key().map(|key| key.to_vec()).map_err(fjall_error))
                .collect::<Result<Vec<_>, _>>()?;
            let mut batch = self
                .namespace
                .db
                .batch()
                .durability(Some(PersistMode::SyncAll));
            for item_key in keys {
                batch.remove(&self.namespace.artifact, item_key);
            }
            batch.remove(&self.namespace.artifact, manifest_key(key));
            batch.commit().map_err(fjall_error)
        })
    }
}

fn load_chunks(
    namespace: &NamespaceDb,
    key: ArtifactKey,
) -> Result<Vec<ArtifactChunk>, StorageError> {
    namespace
        .artifact
        .prefix(chunk_prefix(key))
        .map(|item| {
            let (_, value) = item.into_inner().map_err(fjall_error)?;
            decode_artifact_chunk(&value)
        })
        .collect()
}

fn artifact_identity(prefix: u8, key: ArtifactKey) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(26);
    encoded.push(prefix);
    encoded.extend_from_slice(&key.job_id().to_be_bytes());
    encoded.extend_from_slice(&key.generation().to_be_bytes());
    encoded.push(match key.kind() {
        ArtifactKind::Checkpoint => 1,
        ArtifactKind::Result => 2,
    });
    encoded
}

fn chunk_prefix(key: ArtifactKey) -> Vec<u8> {
    artifact_identity(b'c', key)
}

fn chunk_key(key: ArtifactKey, ordinal: u64) -> Vec<u8> {
    let mut encoded = chunk_prefix(key);
    encoded.extend_from_slice(&ordinal.to_be_bytes());
    encoded
}

fn manifest_key(key: ArtifactKey) -> Vec<u8> {
    artifact_identity(b'm', key)
}
