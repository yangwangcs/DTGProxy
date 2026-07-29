use dtg_kernel::Digest32;

use crate::{
    BindingRole, ChangeRecord, CommandId, EdgeTombstone, EdgeVersion, LogicalMutation, Properties,
    ReadFence, ReplicaBinding, ReplicaMetadata, StorageError, StoreFuture, TransactionRecord,
    Value, VertexTombstone, VertexVersion,
    mutation::{encode_edge, encode_metadata, encode_mutation, encode_transaction, encode_vertex},
};

pub const SUPPORTED_SNAPSHOT_FORMAT_VERSION: u32 = 2;

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
        if format_version != SUPPORTED_SNAPSHOT_FORMAT_VERSION {
            return Err(StorageError::CorruptSnapshot(
                "unsupported snapshot format version".into(),
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
    VertexTombstone(VertexTombstone),
    Edge(EdgeVersion),
    EdgeTombstone(EdgeTombstone),
    Transaction(TransactionRecord),
    ReplicaMetadata(ReplicaMetadata),
    Replay(SnapshotReplayRecord),
    Change(ChangeRecord),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotReplayRecord {
    raft_index: u64,
    raft_term: u64,
    command_id: CommandId,
    mutation_digest: Digest32,
}

impl SnapshotReplayRecord {
    pub fn new(
        raft_index: u64,
        raft_term: u64,
        command_id: CommandId,
        mutation_digest: Digest32,
    ) -> Result<Self, StorageError> {
        if raft_index == 0 || raft_term == 0 || mutation_digest.get() == [0; 32] {
            return Err(StorageError::CorruptSnapshot(
                "snapshot replay identity is incomplete".into(),
            ));
        }
        Ok(Self {
            raft_index,
            raft_term,
            command_id,
            mutation_digest,
        })
    }

    pub const fn raft_index(&self) -> u64 {
        self.raft_index
    }

    pub const fn raft_term(&self) -> u64 {
        self.raft_term
    }

    pub const fn command_id(&self) -> CommandId {
        self.command_id
    }

    pub const fn mutation_digest(&self) -> Digest32 {
        self.mutation_digest
    }
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

    pub fn encoded_len(&self) -> Result<u64, StorageError> {
        self.records.iter().try_fold(64_u64, |bytes, record| {
            checked_add(bytes, snapshot_record_len(record)?)
        })
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
        let mut builder = SnapshotManifestBuilder::new(header.clone());
        for chunk in chunks {
            builder.push(chunk)?;
        }
        Ok(builder.finish())
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

#[derive(Clone, Debug)]
pub struct SnapshotManifestBuilder {
    header: SnapshotHeader,
    next_ordinal: u64,
    record_count: u64,
    chunk_digest_hasher: Box<blake3::Hasher>,
}

impl SnapshotManifestBuilder {
    pub fn new(header: SnapshotHeader) -> Self {
        let mut chunk_digest_hasher = blake3::Hasher::new();
        chunk_digest_hasher.update(b"dtg-logical-snapshot-manifest-chunks-v2");
        Self {
            header,
            next_ordinal: 0,
            record_count: 0,
            chunk_digest_hasher: Box::new(chunk_digest_hasher),
        }
    }

    pub fn push(&mut self, chunk: &SnapshotChunk) -> Result<(), StorageError> {
        chunk.validate()?;
        if chunk.snapshot_id() != self.header.snapshot_id() || chunk.ordinal() != self.next_ordinal
        {
            return Err(StorageError::CorruptSnapshot(
                "snapshot chunk identity or order mismatch".into(),
            ));
        }
        self.record_count = self
            .record_count
            .checked_add(chunk.records().len() as u64)
            .ok_or_else(|| StorageError::CorruptSnapshot("record count overflow".into()))?;
        self.chunk_digest_hasher.update(&chunk.digest.get());
        self.next_ordinal += 1;
        Ok(())
    }

    pub const fn next_ordinal(&self) -> u64 {
        self.next_ordinal
    }

    pub fn finish(self) -> SnapshotManifest {
        let chunk_digest = Digest32::new(*self.chunk_digest_hasher.finalize().as_bytes());
        SnapshotManifest {
            snapshot_id: self.header.snapshot_id(),
            chunk_count: self.next_ordinal,
            record_count: self.record_count,
            content_digest: digest_manifest_summary(&self.header, self.next_ordinal, chunk_digest),
        }
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicalSnapshotCandidateReceipt {
    candidate_binding: ReplicaBinding,
    header: SnapshotHeader,
    manifest: SnapshotManifest,
}

impl LogicalSnapshotCandidateReceipt {
    pub fn new(
        candidate_binding: ReplicaBinding,
        header: SnapshotHeader,
        manifest: SnapshotManifest,
    ) -> Result<Self, StorageError> {
        if candidate_binding.role() != BindingRole::Candidate
            || manifest.snapshot_id() != header.snapshot_id()
            || manifest.content_digest().get() == [0; 32]
            || header.format_version() != SUPPORTED_SNAPSHOT_FORMAT_VERSION
        {
            return Err(StorageError::CorruptSnapshot(
                "logical snapshot candidate receipt is invalid".into(),
            ));
        }
        Ok(Self {
            candidate_binding,
            header,
            manifest,
        })
    }

    pub const fn candidate_binding(&self) -> &ReplicaBinding {
        &self.candidate_binding
    }

    pub const fn binding(&self) -> &ReplicaBinding {
        &self.candidate_binding
    }

    pub const fn header(&self) -> &SnapshotHeader {
        &self.header
    }

    pub const fn manifest(&self) -> &SnapshotManifest {
        &self.manifest
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicalReplicaActivationReceipt {
    active_binding: ReplicaBinding,
    snapshot_id: SnapshotId,
    applied_index: u64,
    content_digest: Digest32,
    format_version: u32,
}

impl LogicalReplicaActivationReceipt {
    pub fn new(
        candidate: &LogicalSnapshotCandidateReceipt,
        active_binding: ReplicaBinding,
    ) -> Result<Self, StorageError> {
        if candidate.candidate_binding.role() != BindingRole::Candidate
            || active_binding.role() != BindingRole::Active
            || !same_binding_except_role(&candidate.candidate_binding, &active_binding)
        {
            return Err(StorageError::SnapshotIdentityMismatch);
        }
        Ok(Self {
            active_binding,
            snapshot_id: candidate.header.snapshot_id(),
            applied_index: candidate.header.applied_index(),
            content_digest: candidate.manifest.content_digest(),
            format_version: candidate.header.format_version(),
        })
    }

    pub const fn active_binding(&self) -> &ReplicaBinding {
        &self.active_binding
    }

    pub const fn snapshot_id(&self) -> SnapshotId {
        self.snapshot_id
    }

    pub const fn applied_index(&self) -> u64 {
        self.applied_index
    }

    pub const fn content_digest(&self) -> Digest32 {
        self.content_digest
    }

    pub const fn format_version(&self) -> u32 {
        self.format_version
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

pub trait LogicalReplicaActivation: Send + Sync {
    fn activate_candidate(
        &self,
        candidate: LogicalSnapshotCandidateReceipt,
        active_binding: ReplicaBinding,
    ) -> StoreFuture<'_, LogicalReplicaActivationReceipt>;
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

fn same_binding_except_role(left: &ReplicaBinding, right: &ReplicaBinding) -> bool {
    left.to_builder()
        .role(right.role())
        .build()
        .is_ok_and(|normalized| normalized == *right)
}

fn snapshot_record_len(record: &SnapshotRecord) -> Result<u64, StorageError> {
    checked_add(
        1,
        match record {
            SnapshotRecord::Vertex(vertex) => vertex_len(vertex)?,
            SnapshotRecord::VertexTombstone(_) => 32,
            SnapshotRecord::Edge(edge) => edge_len(edge)?,
            SnapshotRecord::EdgeTombstone(_) => 32,
            SnapshotRecord::Transaction(_) => 57,
            SnapshotRecord::ReplicaMetadata(metadata) => metadata_len(metadata)?,
            SnapshotRecord::Replay(_) => 64,
            SnapshotRecord::Change(change) => checked_add(16, mutation_len(change.mutation())?)?,
        },
    )
}

fn mutation_len(mutation: &LogicalMutation) -> Result<u64, StorageError> {
    checked_add(
        1,
        match mutation {
            LogicalMutation::PutVertex(vertex) => vertex_len(vertex)?,
            LogicalMutation::DeleteVertex(_) => 32,
            LogicalMutation::PutEdge(edge) => edge_len(edge)?,
            LogicalMutation::DeleteEdge(_) => 32,
            LogicalMutation::PutTransaction(_) => 57,
            LogicalMutation::PutReplicaMetadata(metadata) => metadata_len(metadata)?,
        },
    )
}

fn vertex_len(vertex: &VertexVersion) -> Result<u64, StorageError> {
    checked_add(48, properties_len(vertex.properties())?)
}

fn edge_len(edge: &EdgeVersion) -> Result<u64, StorageError> {
    checked_add(
        checked_add(72, string_len(edge.edge_type())?)?,
        properties_len(edge.properties())?,
    )
}

fn metadata_len(metadata: &ReplicaMetadata) -> Result<u64, StorageError> {
    checked_add(string_len(metadata.name())?, value_len(metadata.value())?)
}

fn properties_len(properties: &Properties) -> Result<u64, StorageError> {
    properties.iter().try_fold(8_u64, |bytes, (name, value)| {
        checked_add(checked_add(bytes, string_len(name)?)?, value_len(value)?)
    })
}

fn value_len(value: &Value) -> Result<u64, StorageError> {
    match value {
        Value::Null => Ok(1),
        Value::Boolean(_) => Ok(2),
        Value::Integer(_) | Value::FloatBits(_) => Ok(9),
        Value::Bytes(bytes) => checked_add(9, length(bytes.len())?),
        Value::String(value) => checked_add(1, string_len(value)?),
        Value::List(values) => values
            .iter()
            .try_fold(9_u64, |bytes, value| checked_add(bytes, value_len(value)?)),
        Value::Map(values) => values.iter().try_fold(9_u64, |bytes, (name, value)| {
            checked_add(checked_add(bytes, string_len(name)?)?, value_len(value)?)
        }),
    }
}

fn string_len(value: &str) -> Result<u64, StorageError> {
    checked_add(8, length(value.len())?)
}

fn length(value: usize) -> Result<u64, StorageError> {
    value
        .try_into()
        .map_err(|_| StorageError::CorruptSnapshot("snapshot byte count overflow".into()))
}

fn checked_add(left: u64, right: u64) -> Result<u64, StorageError> {
    left.checked_add(right)
        .ok_or_else(|| StorageError::CorruptSnapshot("snapshot byte count overflow".into()))
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

fn digest_manifest_summary(
    header: &SnapshotHeader,
    chunk_count: u64,
    chunk_digest: Digest32,
) -> Digest32 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"dtg-logical-snapshot-manifest-v2");
    hasher.update(&header.snapshot_id.get().to_be_bytes());
    hasher.update(&header.source_binding.identity_digest().get());
    hasher.update(&header.applied_index.to_be_bytes());
    hasher.update(&header.format_version.to_be_bytes());
    hasher.update(&chunk_count.to_be_bytes());
    hasher.update(&chunk_digest.get());
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
        SnapshotRecord::VertexTombstone(tombstone) => {
            hasher.update(&[3]);
            hasher.update(&tombstone.id().get().to_be_bytes());
            hasher.update(&tombstone.version().get().to_be_bytes());
            hasher.update(&tombstone.transaction_time().get().to_be_bytes());
        }
        SnapshotRecord::EdgeTombstone(tombstone) => {
            hasher.update(&[4]);
            hasher.update(&tombstone.id().get().to_be_bytes());
            hasher.update(&tombstone.version().get().to_be_bytes());
            hasher.update(&tombstone.transaction_time().get().to_be_bytes());
        }
        SnapshotRecord::Transaction(transaction) => {
            hasher.update(&[5]);
            encode_transaction(hasher, transaction);
        }
        SnapshotRecord::ReplicaMetadata(metadata) => {
            hasher.update(&[6]);
            encode_metadata(hasher, metadata);
        }
        SnapshotRecord::Replay(replay) => {
            hasher.update(&[7]);
            hasher.update(&replay.raft_index.to_be_bytes());
            hasher.update(&replay.raft_term.to_be_bytes());
            hasher.update(&replay.command_id.get().to_be_bytes());
            hasher.update(&replay.mutation_digest.get());
        }
        SnapshotRecord::Change(change) => {
            hasher.update(&[8]);
            hasher.update(&change.raft_index().to_be_bytes());
            hasher.update(&change.mutation_ordinal().to_be_bytes());
            encode_mutation(hasher, change.mutation());
        }
    }
}
