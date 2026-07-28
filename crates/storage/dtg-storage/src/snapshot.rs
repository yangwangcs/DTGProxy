use dtg_kernel::Digest32;

use crate::{
    EdgeVersion, ReadFence, ReplicaBinding, ReplicaMetadata, StorageError, StoreFuture,
    TransactionRecord, VertexVersion,
    mutation::{encode_edge, encode_metadata, encode_transaction, encode_vertex},
};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub struct SnapshotId(u128);

impl SnapshotId {
    pub fn new(value: u128) -> Result<Self, StorageError> {
        (value != 0).then_some(Self(value)).ok_or_else(|| {
            StorageError::CorruptSnapshot("snapshot identifier must be nonzero".into())
        })
    }

    pub const fn get(self) -> u128 {
        self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotRequest {
    snapshot_id: SnapshotId,
    max_records_per_chunk: u32,
}

impl SnapshotRequest {
    pub fn new(snapshot_id: u128, max_records_per_chunk: u32) -> Result<Self, StorageError> {
        let request = Self {
            snapshot_id: SnapshotId::new(snapshot_id)?,
            max_records_per_chunk,
        };
        request.validate()?;
        Ok(request)
    }

    pub fn validate(&self) -> Result<(), StorageError> {
        if self.max_records_per_chunk == 0 {
            return Err(StorageError::CorruptSnapshot(
                "snapshot chunk bound must be nonzero".into(),
            ));
        }
        Ok(())
    }

    pub const fn snapshot_id(&self) -> SnapshotId {
        self.snapshot_id
    }

    pub const fn max_records_per_chunk(&self) -> u32 {
        self.max_records_per_chunk
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotHeader {
    snapshot_id: SnapshotId,
    source_binding: ReplicaBinding,
    applied_index: u64,
    format_version: u32,
}

impl SnapshotHeader {
    pub fn new(
        snapshot_id: SnapshotId,
        source_binding: ReplicaBinding,
        applied_index: u64,
        format_version: u32,
    ) -> Result<Self, StorageError> {
        if format_version == 0 {
            return Err(StorageError::CorruptSnapshot(
                "snapshot format version must be nonzero".into(),
            ));
        }
        Ok(Self {
            snapshot_id,
            source_binding,
            applied_index,
            format_version,
        })
    }

    pub const fn snapshot_id(&self) -> SnapshotId {
        self.snapshot_id
    }

    pub const fn source_binding(&self) -> &ReplicaBinding {
        &self.source_binding
    }

    pub const fn applied_index(&self) -> u64 {
        self.applied_index
    }

    pub const fn format_version(&self) -> u32 {
        self.format_version
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SnapshotRecord {
    Vertex(VertexVersion),
    Edge(EdgeVersion),
    Transaction(TransactionRecord),
    ReplicaMetadata(ReplicaMetadata),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotChunk {
    pub snapshot_id: SnapshotId,
    pub ordinal: u64,
    pub records: Vec<SnapshotRecord>,
    pub digest: Digest32,
}

impl SnapshotChunk {
    pub fn new(
        snapshot_id: SnapshotId,
        ordinal: u64,
        records: Vec<SnapshotRecord>,
    ) -> Result<Self, StorageError> {
        if records.is_empty() {
            return Err(StorageError::CorruptSnapshot(
                "snapshot chunks must be nonempty".into(),
            ));
        }
        let digest = digest_chunk(snapshot_id, ordinal, &records);
        Ok(Self {
            snapshot_id,
            ordinal,
            records,
            digest,
        })
    }

    pub fn validate(&self) -> Result<(), StorageError> {
        if self.records.is_empty()
            || self.digest != digest_chunk(self.snapshot_id, self.ordinal, &self.records)
        {
            return Err(StorageError::CorruptSnapshot(
                "snapshot chunk digest mismatch".into(),
            ));
        }
        Ok(())
    }

    pub const fn snapshot_id(&self) -> SnapshotId {
        self.snapshot_id
    }

    pub const fn ordinal(&self) -> u64 {
        self.ordinal
    }

    pub fn records(&self) -> &[SnapshotRecord] {
        &self.records
    }

    pub fn into_records(self) -> std::vec::IntoIter<SnapshotRecord> {
        self.records.into_iter()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotManifest {
    pub snapshot_id: SnapshotId,
    pub chunk_count: u64,
    pub record_count: u64,
    pub content_digest: Digest32,
}

impl SnapshotManifest {
    pub fn new(header: &SnapshotHeader, chunks: &[SnapshotChunk]) -> Result<Self, StorageError> {
        validate_chunks(header, chunks)?;
        let record_count = chunks.iter().try_fold(0_u64, |count, chunk| {
            count
                .checked_add(chunk.records.len() as u64)
                .ok_or_else(|| StorageError::CorruptSnapshot("record count overflow".into()))
        })?;
        Ok(Self {
            snapshot_id: header.snapshot_id,
            chunk_count: chunks.len() as u64,
            record_count,
            content_digest: digest_manifest(header, chunks),
        })
    }

    pub fn validate(
        &self,
        header: &SnapshotHeader,
        chunks: &[SnapshotChunk],
    ) -> Result<(), StorageError> {
        let expected = Self::new(header, chunks)?;
        if self == &expected {
            Ok(())
        } else {
            Err(StorageError::CorruptSnapshot(
                "snapshot manifest mismatch".into(),
            ))
        }
    }

    pub const fn snapshot_id(&self) -> SnapshotId {
        self.snapshot_id
    }

    pub const fn chunk_count(&self) -> u64 {
        self.chunk_count
    }

    pub const fn record_count(&self) -> u64 {
        self.record_count
    }

    pub const fn content_digest(&self) -> Digest32 {
        self.content_digest
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotRestoreReceipt {
    binding: ReplicaBinding,
    manifest: SnapshotManifest,
}

impl SnapshotRestoreReceipt {
    pub const fn new(binding: ReplicaBinding, manifest: SnapshotManifest) -> Self {
        Self { binding, manifest }
    }

    pub const fn binding(&self) -> &ReplicaBinding {
        &self.binding
    }

    pub const fn manifest(&self) -> &SnapshotManifest {
        &self.manifest
    }
}

pub trait LogicalSnapshotSource: Send + Sync {
    fn begin_snapshot(
        &self,
        fence: ReadFence,
        request: SnapshotRequest,
    ) -> StoreFuture<'_, Box<dyn LogicalSnapshotReader>>;
}

pub trait LogicalSnapshotReader: Send {
    fn header(&self) -> &SnapshotHeader;
    fn next_chunk(&mut self) -> StoreFuture<'_, Option<SnapshotChunk>>;
    fn finish(self: Box<Self>) -> StoreFuture<'static, SnapshotManifest>;
}

pub trait LogicalSnapshotSink: Send + Sync {
    fn begin_restore(
        &self,
        binding: ReplicaBinding,
        header: SnapshotHeader,
    ) -> StoreFuture<'_, Box<dyn LogicalSnapshotWriter>>;
}

pub trait LogicalSnapshotWriter: Send {
    fn target_binding(&self) -> &ReplicaBinding;
    fn header(&self) -> &SnapshotHeader;
    fn write_chunk(&mut self, chunk: SnapshotChunk) -> StoreFuture<'_, ()>;
    fn commit(
        self: Box<Self>,
        manifest: SnapshotManifest,
    ) -> StoreFuture<'static, SnapshotRestoreReceipt>;
    fn abort(self: Box<Self>) -> StoreFuture<'static, ()>;
}

fn validate_chunks(header: &SnapshotHeader, chunks: &[SnapshotChunk]) -> Result<(), StorageError> {
    for (expected_ordinal, chunk) in chunks.iter().enumerate() {
        chunk.validate()?;
        if chunk.snapshot_id != header.snapshot_id || chunk.ordinal != expected_ordinal as u64 {
            return Err(StorageError::CorruptSnapshot(
                "snapshot chunk identity or order mismatch".into(),
            ));
        }
    }
    Ok(())
}

fn digest_chunk(snapshot_id: SnapshotId, ordinal: u64, records: &[SnapshotRecord]) -> Digest32 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"dtg-logical-snapshot-chunk-v1");
    hasher.update(&snapshot_id.get().to_be_bytes());
    hasher.update(&ordinal.to_be_bytes());
    hasher.update(&(records.len() as u64).to_be_bytes());
    for record in records {
        encode_record(&mut hasher, record);
    }
    Digest32::new(*hasher.finalize().as_bytes())
}

fn digest_manifest(header: &SnapshotHeader, chunks: &[SnapshotChunk]) -> Digest32 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"dtg-logical-snapshot-manifest-v1");
    hasher.update(&header.snapshot_id.get().to_be_bytes());
    hasher.update(&header.source_binding.identity_digest().get());
    hasher.update(&header.applied_index.to_be_bytes());
    hasher.update(&header.format_version.to_be_bytes());
    hasher.update(&(chunks.len() as u64).to_be_bytes());
    for chunk in chunks {
        hasher.update(&chunk.digest.get());
    }
    Digest32::new(*hasher.finalize().as_bytes())
}

fn encode_record(hasher: &mut blake3::Hasher, record: &SnapshotRecord) {
    match record {
        SnapshotRecord::Vertex(vertex) => {
            hasher.update(&[1]);
            encode_vertex(hasher, vertex);
        }
        SnapshotRecord::Edge(edge) => {
            hasher.update(&[2]);
            encode_edge(hasher, edge);
        }
        SnapshotRecord::Transaction(transaction) => {
            hasher.update(&[3]);
            encode_transaction(hasher, transaction);
        }
        SnapshotRecord::ReplicaMetadata(metadata) => {
            hasher.update(&[4]);
            encode_metadata(hasher, metadata);
        }
    }
}
