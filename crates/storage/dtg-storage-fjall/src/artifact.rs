use std::{fmt, path::Path, sync::MutexGuard};

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

    fn lock(&self) -> Result<MutexGuard<'_, ()>, StorageError> {
        self.namespace
            .artifact_guard
            .lock()
            .map_err(|_| StorageError::Internal("Fjall artifact lock is poisoned".into()))
    }
}

impl ArtifactStore for FjallArtifactStore {
    fn binding(&self) -> &ReplicaBinding {
        &self.binding
    }

    fn put_chunk(&self, binding: ReplicaBinding, chunk: ArtifactChunk) -> StoreFuture<'_, ()> {
        Box::pin(async move {
            let _guard = self.lock()?;
            self.verify_binding(&binding)?;
            let key = chunk_key(chunk.key(), chunk.ordinal());
            if self
                .namespace
                .artifact
                .get(manifest_key(chunk.key()))
                .map_err(fjall_error)?
                .is_some()
            {
                let existing = self
                    .namespace
                    .artifact
                    .get(&key)
                    .map_err(fjall_error)?
                    .ok_or_else(|| {
                        StorageError::InvalidArtifact(
                            "committed artifact is missing an immutable chunk".into(),
                        )
                    })?;
                if decode_artifact_chunk(&existing)? == chunk {
                    return Ok(());
                }
                return Err(StorageError::InvalidArtifact(
                    "committed artifact chunks are immutable".into(),
                ));
            }
            self.namespace
                .artifact
                .insert(key, encode_artifact_chunk(&chunk)?)
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
            let _guard = self.lock()?;
            self.verify_binding(&binding)?;
            if let Some((_, chunks)) = load_published_artifact(&self.namespace, key)? {
                return Ok(chunks.into_iter().find(|chunk| chunk.ordinal() == ordinal));
            }
            let chunk = self
                .namespace
                .artifact
                .get(chunk_key(key, ordinal))
                .map_err(fjall_error)?
                .map(|bytes| decode_artifact_chunk(&bytes))
                .transpose()?;
            if chunk
                .as_ref()
                .is_some_and(|chunk| chunk.key() != key || chunk.ordinal() != ordinal)
            {
                return Err(StorageError::InvalidArtifact(
                    "stored artifact chunk identity mismatch".into(),
                ));
            }
            Ok(chunk)
        })
    }

    fn commit_manifest(
        &self,
        binding: ReplicaBinding,
        manifest: ArtifactManifest,
    ) -> StoreFuture<'_, ()> {
        Box::pin(async move {
            let _guard = self.lock()?;
            self.verify_binding(&binding)?;
            if let Some(stored) = self
                .namespace
                .artifact
                .get(manifest_key(manifest.key()))
                .map_err(fjall_error)?
            {
                let stored = decode_artifact_manifest(&stored)?;
                if stored.key != manifest.key()
                    || stored.chunk_count != manifest.chunk_count()
                    || stored.total_bytes != manifest.total_bytes()
                    || stored.content_digest != manifest.content_digest()
                {
                    return Err(StorageError::InvalidArtifact(
                        "committed artifact manifest is immutable".into(),
                    ));
                }
            }
            let chunks = load_chunks(&self.namespace, manifest.key())?;
            let expected = ArtifactManifest::new(manifest.key(), &chunks)?;
            if expected != manifest {
                return Err(StorageError::InvalidArtifact(
                    "artifact manifest does not match stored chunks".into(),
                ));
            }
            let mut batch = self
                .namespace
                .db
                .batch()
                .durability(Some(PersistMode::SyncAll));
            for chunk in &chunks {
                batch.insert(
                    &self.namespace.artifact,
                    chunk_key(chunk.key(), chunk.ordinal()),
                    encode_artifact_chunk(chunk)?,
                );
            }
            batch.insert(
                &self.namespace.artifact,
                manifest_key(manifest.key()),
                encode_artifact_manifest(&manifest)?,
            );
            batch.commit().map_err(fjall_error)
        })
    }

    fn manifest(
        &self,
        binding: ReplicaBinding,
        key: ArtifactKey,
    ) -> StoreFuture<'_, Option<ArtifactManifest>> {
        Box::pin(async move {
            let _guard = self.lock()?;
            self.verify_binding(&binding)?;
            Ok(load_published_artifact(&self.namespace, key)?.map(|(manifest, _)| manifest))
        })
    }

    fn delete(&self, binding: ReplicaBinding, key: ArtifactKey) -> StoreFuture<'_, ()> {
        Box::pin(async move {
            let _guard = self.lock()?;
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

fn load_published_artifact(
    namespace: &NamespaceDb,
    key: ArtifactKey,
) -> Result<Option<(ArtifactManifest, Vec<ArtifactChunk>)>, StorageError> {
    let Some(bytes) = namespace
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
    let chunks = load_chunks(namespace, key)?;
    let manifest = ArtifactManifest::new(key, &chunks)?;
    if manifest.chunk_count() != stored.chunk_count
        || manifest.total_bytes() != stored.total_bytes
        || manifest.content_digest() != stored.content_digest
    {
        return Err(StorageError::InvalidArtifact(
            "stored artifact manifest is corrupt".into(),
        ));
    }
    Ok(Some((manifest, chunks)))
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
