#![forbid(unsafe_code)]

mod embedded;
mod remote;
mod storage_adapter;

pub use embedded::EmbeddedShardClient;
pub use remote::{RemoteReplica, RemoteShardClient, RemoteTopology, RemoteTopologyError};
pub use storage_adapter::ShardClientStorageAdapter;

use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use storage_api::{
    CandidateScanPage, CandidateScanRequest, KeySpan, KeyValue, LogicalKey,
    QueryCapabilitySnapshot, QueryPrimitiveCapabilities,
};
use temporal_types::TransactionTime;
use tokio_stream::Stream;

pub const MAX_ARTIFACT_CHUNK_BYTES: usize = raft_command::MAX_ANALYTICS_ARTIFACT_CHUNK_BYTES;
pub const MAX_ARTIFACT_CHUNKS: u16 = shard_runtime::MAX_ANALYTICS_ARTIFACT_CHUNKS;

pub type ShardClientFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, ShardClientError>> + Send + 'a>>;
pub type ArtifactChunkStream =
    Pin<Box<dyn Stream<Item = Result<ArtifactChunk, ShardClientError>> + Send + 'static>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShardRequestContext {
    graph_id: u64,
    shard_id: u32,
    placement_epoch: u64,
    request_id: u128,
    deadline_unix_ms: u64,
}

impl ShardRequestContext {
    pub fn new(
        graph_id: u64,
        shard_id: u32,
        placement_epoch: u64,
        request_id: u128,
        deadline_unix_ms: u64,
    ) -> Result<Self, ShardClientError> {
        if graph_id == 0
            || shard_id == 0
            || placement_epoch == 0
            || request_id == 0
            || deadline_unix_ms == 0
        {
            return Err(ShardClientError::InvalidContext);
        }
        Ok(Self {
            graph_id,
            shard_id,
            placement_epoch,
            request_id,
            deadline_unix_ms,
        })
    }

    #[must_use]
    pub const fn graph_id(self) -> u64 {
        self.graph_id
    }

    #[must_use]
    pub const fn shard_id(self) -> u32 {
        self.shard_id
    }

    #[must_use]
    pub const fn placement_epoch(self) -> u64 {
        self.placement_epoch
    }

    #[must_use]
    pub const fn request_id(self) -> u128 {
        self.request_id
    }

    #[must_use]
    pub const fn deadline_unix_ms(self) -> u64 {
        self.deadline_unix_ms
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecuteCommand {
    context: ShardRequestContext,
    command: Vec<u8>,
}

impl ExecuteCommand {
    pub fn new(context: ShardRequestContext, command: Vec<u8>) -> Result<Self, ShardClientError> {
        if command.is_empty() {
            return Err(ShardClientError::EmptyCommand);
        }
        Ok(Self { context, command })
    }

    #[must_use]
    pub const fn context(&self) -> ShardRequestContext {
        self.context
    }

    #[must_use]
    pub fn command(&self) -> &[u8] {
        &self.command
    }

    #[must_use]
    pub fn into_command(self) -> Vec<u8> {
        self.command
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExecuteReceipt {
    raft_index: u64,
    duplicate: bool,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ArtifactKind {
    Checkpoint,
    Result,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PutArtifactChunkRequest {
    context: ShardRequestContext,
    job_id: u128,
    kind: ArtifactKind,
    generation: u64,
    created_at_unix_ms: u64,
    ordinal: u64,
    previous_digest: [u8; 32],
    payload: Vec<u8>,
}

impl PutArtifactChunkRequest {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        context: ShardRequestContext,
        job_id: u128,
        kind: ArtifactKind,
        generation: u64,
        created_at_unix_ms: u64,
        ordinal: u64,
        previous_digest: [u8; 32],
        payload: Vec<u8>,
    ) -> Result<Self, ShardClientError> {
        if job_id == 0
            || generation == 0
            || created_at_unix_ms == 0
            || payload.is_empty()
            || payload.len() > MAX_ARTIFACT_CHUNK_BYTES
            || (ordinal == 0) != (previous_digest == [0; 32])
        {
            return Err(ShardClientError::InvalidArtifact);
        }
        Ok(Self {
            context,
            job_id,
            kind,
            generation,
            created_at_unix_ms,
            ordinal,
            previous_digest,
            payload,
        })
    }

    #[must_use]
    pub const fn context(&self) -> ShardRequestContext {
        self.context
    }
    #[must_use]
    pub const fn job_id(&self) -> u128 {
        self.job_id
    }
    #[must_use]
    pub const fn kind(&self) -> ArtifactKind {
        self.kind
    }
    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }
    #[must_use]
    pub const fn created_at_unix_ms(&self) -> u64 {
        self.created_at_unix_ms
    }
    #[must_use]
    pub const fn ordinal(&self) -> u64 {
        self.ordinal
    }
    #[must_use]
    pub const fn previous_digest(&self) -> [u8; 32] {
        self.previous_digest
    }
    #[must_use]
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GetArtifactGenerationRequest {
    context: ShardRequestContext,
    job_id: u128,
    kind: ArtifactKind,
    generation: u64,
    expected_chunk_count: u16,
    expected_total_bytes: u64,
    expected_content_digest: [u8; 32],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PinArtifactGenerationRequest {
    context: ShardRequestContext,
    job_id: u128,
    kind: ArtifactKind,
    generation: u64,
    expected_chunk_count: u16,
    expected_total_bytes: u64,
    expected_content_digest: [u8; 32],
}

impl PinArtifactGenerationRequest {
    pub fn new(
        context: ShardRequestContext,
        job_id: u128,
        kind: ArtifactKind,
        generation: u64,
        expected_chunk_count: u16,
        expected_total_bytes: u64,
        expected_content_digest: [u8; 32],
    ) -> Result<Self, ShardClientError> {
        validate_artifact_manifest(
            job_id,
            generation,
            expected_chunk_count,
            expected_total_bytes,
            expected_content_digest,
        )?;
        Ok(Self {
            context,
            job_id,
            kind,
            generation,
            expected_chunk_count,
            expected_total_bytes,
            expected_content_digest,
        })
    }

    #[must_use]
    pub const fn context(self) -> ShardRequestContext {
        self.context
    }
    #[must_use]
    pub const fn job_id(self) -> u128 {
        self.job_id
    }
    #[must_use]
    pub const fn kind(self) -> ArtifactKind {
        self.kind
    }
    #[must_use]
    pub const fn generation(self) -> u64 {
        self.generation
    }
    #[must_use]
    pub const fn expected_chunk_count(self) -> u16 {
        self.expected_chunk_count
    }
    #[must_use]
    pub const fn expected_total_bytes(self) -> u64 {
        self.expected_total_bytes
    }
    #[must_use]
    pub const fn expected_content_digest(self) -> [u8; 32] {
        self.expected_content_digest
    }
}

impl GetArtifactGenerationRequest {
    pub fn new(
        context: ShardRequestContext,
        job_id: u128,
        kind: ArtifactKind,
        generation: u64,
        expected_chunk_count: u16,
        expected_total_bytes: u64,
        expected_content_digest: [u8; 32],
    ) -> Result<Self, ShardClientError> {
        validate_artifact_manifest(
            job_id,
            generation,
            expected_chunk_count,
            expected_total_bytes,
            expected_content_digest,
        )?;
        Ok(Self {
            context,
            job_id,
            kind,
            generation,
            expected_chunk_count,
            expected_total_bytes,
            expected_content_digest,
        })
    }

    #[must_use]
    pub const fn context(self) -> ShardRequestContext {
        self.context
    }
    #[must_use]
    pub const fn job_id(self) -> u128 {
        self.job_id
    }
    #[must_use]
    pub const fn kind(self) -> ArtifactKind {
        self.kind
    }
    #[must_use]
    pub const fn generation(self) -> u64 {
        self.generation
    }
    #[must_use]
    pub const fn expected_chunk_count(self) -> u16 {
        self.expected_chunk_count
    }

    #[must_use]
    pub const fn expected_total_bytes(self) -> u64 {
        self.expected_total_bytes
    }

    #[must_use]
    pub const fn expected_content_digest(self) -> [u8; 32] {
        self.expected_content_digest
    }
}

fn validate_artifact_manifest(
    job_id: u128,
    generation: u64,
    expected_chunk_count: u16,
    expected_total_bytes: u64,
    expected_content_digest: [u8; 32],
) -> Result<(), ShardClientError> {
    let maximum_total_bytes = u64::from(expected_chunk_count)
        .checked_mul(
            u64::try_from(MAX_ARTIFACT_CHUNK_BYTES).expect("artifact chunk limit fits in u64"),
        )
        .ok_or(ShardClientError::InvalidArtifact)?;
    if job_id == 0
        || generation == 0
        || expected_chunk_count == 0
        || expected_chunk_count > MAX_ARTIFACT_CHUNKS
        || expected_total_bytes < u64::from(expected_chunk_count)
        || expected_total_bytes > maximum_total_bytes
        || expected_content_digest == [0; 32]
    {
        return Err(ShardClientError::InvalidArtifact);
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeleteArtifactGenerationRequest {
    context: ShardRequestContext,
    job_id: u128,
    kind: ArtifactKind,
    generation: u64,
    gc_epoch: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdvanceArtifactFenceRequest {
    context: ShardRequestContext,
    job_id: u128,
    kind: ArtifactKind,
    generation: u64,
    gc_epoch: u64,
}

impl AdvanceArtifactFenceRequest {
    pub fn new_with_gc_epoch(
        context: ShardRequestContext,
        job_id: u128,
        kind: ArtifactKind,
        generation: u64,
        gc_epoch: u64,
    ) -> Result<Self, ShardClientError> {
        if job_id == 0 || generation == 0 || gc_epoch == 0 {
            return Err(ShardClientError::InvalidArtifact);
        }
        Ok(Self {
            context,
            job_id,
            kind,
            generation,
            gc_epoch,
        })
    }

    pub const fn context(self) -> ShardRequestContext {
        self.context
    }
    pub const fn job_id(self) -> u128 {
        self.job_id
    }
    pub const fn kind(self) -> ArtifactKind {
        self.kind
    }
    pub const fn generation(self) -> u64 {
        self.generation
    }
    pub const fn gc_epoch(self) -> u64 {
        self.gc_epoch
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ListArtifactGenerationsRequest {
    context: ShardRequestContext,
    job_id: u128,
    kind: ArtifactKind,
    limit: u32,
}

impl ListArtifactGenerationsRequest {
    pub fn new(
        context: ShardRequestContext,
        job_id: u128,
        kind: ArtifactKind,
        limit: u32,
    ) -> Result<Self, ShardClientError> {
        if job_id == 0 || !(1..=4096).contains(&limit) {
            return Err(ShardClientError::InvalidArtifact);
        }
        Ok(Self {
            context,
            job_id,
            kind,
            limit,
        })
    }

    #[must_use]
    pub const fn context(self) -> ShardRequestContext {
        self.context
    }
    #[must_use]
    pub const fn job_id(self) -> u128 {
        self.job_id
    }
    #[must_use]
    pub const fn kind(self) -> ArtifactKind {
        self.kind
    }
    #[must_use]
    pub const fn limit(self) -> u32 {
        self.limit
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArtifactGenerationSummary {
    job_id: u128,
    kind: ArtifactKind,
    generation: u64,
    created_at_unix_ms: u64,
    expected_chunk_count: u16,
    expected_total_bytes: u64,
    expected_content_digest: [u8; 32],
    pinned: bool,
    applied_index: u64,
}

impl ArtifactGenerationSummary {
    #[must_use]
    pub const fn job_id(&self) -> u128 {
        self.job_id
    }
    #[must_use]
    pub const fn kind(&self) -> ArtifactKind {
        self.kind
    }
    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }
    #[must_use]
    pub const fn created_at_unix_ms(&self) -> u64 {
        self.created_at_unix_ms
    }
    #[must_use]
    pub const fn expected_chunk_count(&self) -> u16 {
        self.expected_chunk_count
    }
    #[must_use]
    pub const fn expected_total_bytes(&self) -> u64 {
        self.expected_total_bytes
    }
    #[must_use]
    pub const fn expected_content_digest(&self) -> [u8; 32] {
        self.expected_content_digest
    }
    #[must_use]
    pub const fn pinned(&self) -> bool {
        self.pinned
    }
    #[must_use]
    pub const fn applied_index(&self) -> u64 {
        self.applied_index
    }

    #[must_use]
    pub const fn cursor(&self) -> ArtifactGenerationCursor {
        ArtifactGenerationCursor {
            job_id: self.job_id,
            kind: self.kind,
            generation: self.generation,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ArtifactGenerationCursor {
    job_id: u128,
    kind: ArtifactKind,
    generation: u64,
}

impl ArtifactGenerationCursor {
    pub fn new(
        job_id: u128,
        kind: ArtifactKind,
        generation: u64,
    ) -> Result<Self, ShardClientError> {
        if job_id == 0 || generation == 0 {
            return Err(ShardClientError::InvalidArtifact);
        }
        Ok(Self {
            job_id,
            kind,
            generation,
        })
    }

    #[must_use]
    pub const fn job_id(self) -> u128 {
        self.job_id
    }

    #[must_use]
    pub const fn kind(self) -> ArtifactKind {
        self.kind
    }

    #[must_use]
    pub const fn generation(self) -> u64 {
        self.generation
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ListArtifactGenerationHeadsRequest {
    context: ShardRequestContext,
    after: Option<ArtifactGenerationCursor>,
    limit: u32,
}

impl ListArtifactGenerationHeadsRequest {
    pub fn new(
        context: ShardRequestContext,
        after: Option<ArtifactGenerationCursor>,
        limit: u32,
    ) -> Result<Self, ShardClientError> {
        if !(1..=4096).contains(&limit) {
            return Err(ShardClientError::InvalidArtifact);
        }
        Ok(Self {
            context,
            after,
            limit,
        })
    }

    #[must_use]
    pub const fn context(self) -> ShardRequestContext {
        self.context
    }

    #[must_use]
    pub const fn after(self) -> Option<ArtifactGenerationCursor> {
        self.after
    }

    #[must_use]
    pub const fn limit(self) -> u32 {
        self.limit
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArtifactGenerationHeadPage {
    applied_index: u64,
    generations: Vec<ArtifactGenerationSummary>,
    next: Option<ArtifactGenerationCursor>,
}

impl ArtifactGenerationHeadPage {
    #[must_use]
    pub const fn applied_index(&self) -> u64 {
        self.applied_index
    }

    #[must_use]
    pub fn generations(&self) -> &[ArtifactGenerationSummary] {
        &self.generations
    }

    #[must_use]
    pub const fn next(&self) -> Option<ArtifactGenerationCursor> {
        self.next
    }
}

impl DeleteArtifactGenerationRequest {
    pub fn new(
        context: ShardRequestContext,
        job_id: u128,
        kind: ArtifactKind,
        generation: u64,
    ) -> Result<Self, ShardClientError> {
        Self::new_with_gc_epoch(context, job_id, kind, generation, 1)
    }

    pub fn new_with_gc_epoch(
        context: ShardRequestContext,
        job_id: u128,
        kind: ArtifactKind,
        generation: u64,
        gc_epoch: u64,
    ) -> Result<Self, ShardClientError> {
        if job_id == 0 || generation == 0 || gc_epoch == 0 {
            return Err(ShardClientError::InvalidArtifact);
        }
        Ok(Self {
            context,
            job_id,
            kind,
            generation,
            gc_epoch,
        })
    }
    #[must_use]
    pub const fn context(self) -> ShardRequestContext {
        self.context
    }
    #[must_use]
    pub const fn job_id(self) -> u128 {
        self.job_id
    }
    #[must_use]
    pub const fn kind(self) -> ArtifactKind {
        self.kind
    }
    #[must_use]
    pub const fn generation(self) -> u64 {
        self.generation
    }
    #[must_use]
    pub const fn gc_epoch(self) -> u64 {
        self.gc_epoch
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArtifactChunk {
    applied_index: u64,
    ordinal: u64,
    previous_digest: [u8; 32],
    payload_digest: [u8; 32],
    payload: Vec<u8>,
}

impl ArtifactChunk {
    pub(crate) const fn new(
        applied_index: u64,
        ordinal: u64,
        previous_digest: [u8; 32],
        payload_digest: [u8; 32],
        payload: Vec<u8>,
    ) -> Self {
        Self {
            applied_index,
            ordinal,
            previous_digest,
            payload_digest,
            payload,
        }
    }
    #[must_use]
    pub const fn applied_index(&self) -> u64 {
        self.applied_index
    }
    #[must_use]
    pub const fn ordinal(&self) -> u64 {
        self.ordinal
    }
    #[must_use]
    pub const fn previous_digest(&self) -> [u8; 32] {
        self.previous_digest
    }
    #[must_use]
    pub const fn payload_digest(&self) -> [u8; 32] {
        self.payload_digest
    }
    #[must_use]
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }
}

impl ExecuteReceipt {
    #[must_use]
    pub const fn new(raft_index: u64, duplicate: bool) -> Self {
        Self {
            raft_index,
            duplicate,
        }
    }

    #[must_use]
    pub const fn raft_index(self) -> u64 {
        self.raft_index
    }

    #[must_use]
    pub const fn duplicate(self) -> bool {
        self.duplicate
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadKeysRequest {
    context: ShardRequestContext,
    keys: Vec<LogicalKey>,
}

impl ReadKeysRequest {
    pub fn new(
        context: ShardRequestContext,
        keys: Vec<LogicalKey>,
    ) -> Result<Self, ShardClientError> {
        if keys.is_empty() {
            return Err(ShardClientError::EmptyRead);
        }
        Ok(Self { context, keys })
    }

    #[must_use]
    pub const fn context(&self) -> ShardRequestContext {
        self.context
    }

    #[must_use]
    pub fn keys(&self) -> &[LogicalKey] {
        &self.keys
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScanRequest {
    context: ShardRequestContext,
    span: KeySpan,
}

impl ScanRequest {
    #[must_use]
    pub const fn new(context: ShardRequestContext, span: KeySpan) -> Self {
        Self { context, span }
    }

    #[must_use]
    pub const fn context(&self) -> ShardRequestContext {
        self.context
    }

    #[must_use]
    pub const fn span(&self) -> &KeySpan {
        &self.span
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct CandidateScanCommand {
    context: ShardRequestContext,
    request: CandidateScanRequest,
}

impl CandidateScanCommand {
    #[must_use]
    pub const fn new(context: ShardRequestContext, request: CandidateScanRequest) -> Self {
        Self { context, request }
    }

    #[must_use]
    pub const fn context(&self) -> ShardRequestContext {
        self.context
    }

    #[must_use]
    pub const fn request(&self) -> &CandidateScanRequest {
        &self.request
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShardStatus {
    node_id: u64,
    leader_id: u64,
    term: u64,
    applied_index: u64,
    closed_timestamp: TransactionTime,
    query_capabilities: QueryCapabilitySnapshot,
}

impl ShardStatus {
    #[must_use]
    pub const fn new(
        node_id: u64,
        leader_id: u64,
        term: u64,
        applied_index: u64,
        closed_timestamp: TransactionTime,
        query_capabilities: QueryCapabilitySnapshot,
    ) -> Self {
        Self {
            node_id,
            leader_id,
            term,
            applied_index,
            closed_timestamp,
            query_capabilities,
        }
    }

    #[must_use]
    pub const fn node_id(self) -> u64 {
        self.node_id
    }

    #[must_use]
    pub const fn leader_id(self) -> u64 {
        self.leader_id
    }

    #[must_use]
    pub const fn term(self) -> u64 {
        self.term
    }

    #[must_use]
    pub const fn applied_index(self) -> u64 {
        self.applied_index
    }

    #[must_use]
    pub const fn closed_timestamp(self) -> TransactionTime {
        self.closed_timestamp
    }

    #[must_use]
    pub const fn query_capabilities(self) -> QueryCapabilitySnapshot {
        self.query_capabilities
    }
}

pub trait ShardClient: Send + Sync {
    fn query_primitive_capabilities(&self) -> QueryPrimitiveCapabilities {
        QueryPrimitiveCapabilities::NONE
    }

    fn execute<'a>(&'a self, request: ExecuteCommand) -> ShardClientFuture<'a, ExecuteReceipt>;

    fn read_keys<'a>(
        &'a self,
        request: ReadKeysRequest,
    ) -> ShardClientFuture<'a, Vec<Option<Vec<u8>>>>;

    fn scan<'a>(&'a self, request: ScanRequest) -> ShardClientFuture<'a, Vec<KeyValue>>;

    fn scan_fenced<'a>(&'a self, request: ScanRequest) -> ShardClientFuture<'a, FencedScan>;

    fn scan_candidates<'a>(
        &'a self,
        _request: CandidateScanCommand,
    ) -> ShardClientFuture<'a, CandidateScanPage> {
        Box::pin(async {
            Err(ShardClientError::UnsupportedQueryPrimitive(
                "candidate scan",
            ))
        })
    }

    fn read_barrier<'a>(&'a self, context: ShardRequestContext) -> ShardClientFuture<'a, u64> {
        Box::pin(async move {
            Err(ShardClientError::ReadBarrier(format!(
                "Shard {} does not expose a linearizable ReadIndex barrier",
                context.shard_id()
            )))
        })
    }

    fn put_artifact_chunk<'a>(
        &'a self,
        request: PutArtifactChunkRequest,
    ) -> ShardClientFuture<'a, ExecuteReceipt>;

    fn pin_artifact_generation<'a>(
        &'a self,
        request: PinArtifactGenerationRequest,
    ) -> ShardClientFuture<'a, ExecuteReceipt>;

    fn get_artifact_generation<'a>(
        &'a self,
        request: GetArtifactGenerationRequest,
    ) -> ShardClientFuture<'a, ArtifactChunkStream>;

    fn delete_artifact_generation<'a>(
        &'a self,
        request: DeleteArtifactGenerationRequest,
    ) -> ShardClientFuture<'a, ExecuteReceipt>;

    fn advance_artifact_fence<'a>(
        &'a self,
        request: AdvanceArtifactFenceRequest,
    ) -> ShardClientFuture<'a, ExecuteReceipt>;

    fn list_artifact_generations<'a>(
        &'a self,
        request: ListArtifactGenerationsRequest,
    ) -> ShardClientFuture<'a, Vec<ArtifactGenerationSummary>>;

    fn list_artifact_generation_heads<'a>(
        &'a self,
        request: ListArtifactGenerationHeadsRequest,
    ) -> ShardClientFuture<'a, ArtifactGenerationHeadPage>;

    fn status<'a>(&'a self, context: ShardRequestContext) -> ShardClientFuture<'a, ShardStatus>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FencedScan {
    applied_index: u64,
    entries: Vec<KeyValue>,
}

impl FencedScan {
    #[must_use]
    pub const fn new(applied_index: u64, entries: Vec<KeyValue>) -> Self {
        Self {
            applied_index,
            entries,
        }
    }

    #[must_use]
    pub const fn applied_index(&self) -> u64 {
        self.applied_index
    }

    #[must_use]
    pub fn into_entries(self) -> Vec<KeyValue> {
        self.entries
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ShardClientError {
    InvalidContext,
    EmptyCommand,
    EmptyRead,
    InvalidArtifact,
    ArtifactCorruption(String),
    WrongGraph { expected: u64, actual: u64 },
    DeadlineExpired,
    NoLeader { shard_id: u32 },
    NotLeader { leader_hint: Option<u64> },
    StaleEpoch { current_epoch: Option<u64> },
    Replication(String),
    ReadBarrier(String),
    Adapter(String),
    ScanByteLimit { limit: u64, required: u64 },
    UnsupportedQueryPrimitive(&'static str),
    RequestMismatch { expected: u128, actual: u128 },
    Internal(String),
}

impl Display for ShardClientError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidContext => formatter.write_str("invalid Shard request context"),
            Self::EmptyCommand => formatter.write_str("Shard command cannot be empty"),
            Self::EmptyRead => formatter.write_str("Shard key read cannot be empty"),
            Self::InvalidArtifact => formatter.write_str("invalid analytics artifact request"),
            Self::ArtifactCorruption(message) => {
                write!(formatter, "analytics artifact corruption: {message}")
            }
            Self::WrongGraph { expected, actual } => {
                write!(formatter, "expected graph {expected}, got {actual}")
            }
            Self::DeadlineExpired => formatter.write_str("Shard request deadline expired"),
            Self::NoLeader { shard_id } => write!(formatter, "Shard {shard_id} has no Leader"),
            Self::NotLeader { leader_hint } => {
                write!(
                    formatter,
                    "remote Replica is not Leader; hint={leader_hint:?}"
                )
            }
            Self::StaleEpoch { current_epoch } => {
                write!(
                    formatter,
                    "remote Shard placement epoch is stale; current={current_epoch:?}"
                )
            }
            Self::Replication(message) => write!(formatter, "Shard replication failed: {message}"),
            Self::ReadBarrier(message) => write!(formatter, "Shard read barrier failed: {message}"),
            Self::Adapter(message) => write!(formatter, "Shard Adapter failed: {message}"),
            Self::ScanByteLimit { limit, required } => write!(
                formatter,
                "scan requires {required} bytes, exceeding byte limit {limit}"
            ),
            Self::UnsupportedQueryPrimitive(operation) => {
                write!(
                    formatter,
                    "Shard query primitive is unsupported: {operation}"
                )
            }
            Self::RequestMismatch { expected, actual } => write!(
                formatter,
                "request ID {actual} differs from command request ID {expected}"
            ),
            Self::Internal(message) => write!(formatter, "Shard client internal error: {message}"),
        }
    }
}

impl Error for ShardClientError {}

struct ArtifactStreamChunk {
    applied_index: u64,
    ordinal: u64,
    previous_digest: [u8; 32],
    payload_digest: [u8; 32],
    payload: Vec<u8>,
}

type ArtifactInputStream =
    Pin<Box<dyn Stream<Item = Result<ArtifactStreamChunk, ShardClientError>> + Send + 'static>>;

struct ArtifactStreamValidator {
    expected_count: usize,
    expected_total_bytes: u64,
    expected_content_digest: [u8; 32],
    observed_count: usize,
    observed_total_bytes: u64,
    previous_digest: [u8; 32],
    applied_index: Option<u64>,
    content_hasher: blake3::Hasher,
}

impl ArtifactStreamValidator {
    fn new(
        expected_count: usize,
        expected_total_bytes: u64,
        expected_content_digest: [u8; 32],
    ) -> Self {
        Self {
            expected_count,
            expected_total_bytes,
            expected_content_digest,
            observed_count: 0,
            observed_total_bytes: 0,
            previous_digest: [0; 32],
            applied_index: None,
            content_hasher: blake3::Hasher::new(),
        }
    }

    fn push(
        &mut self,
        applied_index: u64,
        ordinal: u64,
        previous_digest: [u8; 32],
        payload_digest: [u8; 32],
        payload: Vec<u8>,
    ) -> Result<ArtifactChunk, ShardClientError> {
        let expected_ordinal = u64::try_from(self.observed_count)
            .map_err(|_| artifact_corruption("artifact ordinal overflow"))?;
        let payload_bytes = u64::try_from(payload.len())
            .map_err(|_| artifact_corruption("artifact payload length overflow"))?;
        let next_total = self
            .observed_total_bytes
            .checked_add(payload_bytes)
            .ok_or_else(|| artifact_corruption("artifact total byte count overflow"))?;
        if self.observed_count >= self.expected_count {
            return Err(artifact_corruption(
                "artifact stream contains more chunks than its manifest",
            ));
        }
        if ordinal != expected_ordinal {
            return Err(artifact_corruption("artifact ordinal is not contiguous"));
        }
        if applied_index == 0
            || self
                .applied_index
                .is_some_and(|expected| expected != applied_index)
        {
            return Err(artifact_corruption(
                "artifact applied index is zero or changed within the stream",
            ));
        }
        if previous_digest != self.previous_digest {
            return Err(artifact_corruption(
                "artifact previous digest chain is discontinuous",
            ));
        }
        if payload.is_empty() || payload.len() > MAX_ARTIFACT_CHUNK_BYTES {
            return Err(artifact_corruption("artifact payload size is invalid"));
        }
        if payload_digest != *blake3::hash(&payload).as_bytes() {
            return Err(artifact_corruption("artifact payload digest is invalid"));
        }
        if next_total > self.expected_total_bytes {
            return Err(artifact_corruption(
                "artifact stream exceeds its manifest total byte count",
            ));
        }

        self.observed_count += 1;
        self.observed_total_bytes = next_total;
        self.previous_digest = payload_digest;
        self.applied_index = Some(applied_index);
        self.content_hasher.update(&payload);
        Ok(ArtifactChunk::new(
            applied_index,
            ordinal,
            previous_digest,
            payload_digest,
            payload,
        ))
    }

    fn finish(self) -> Result<(), ShardClientError> {
        if self.observed_count != self.expected_count {
            return Err(artifact_corruption(
                "artifact stream ended before its manifest chunk count",
            ));
        }
        if self.observed_total_bytes != self.expected_total_bytes {
            return Err(artifact_corruption(
                "artifact stream total byte count differs from its manifest",
            ));
        }
        if *self.content_hasher.finalize().as_bytes() != self.expected_content_digest {
            return Err(artifact_corruption(
                "artifact content digest differs from its manifest",
            ));
        }
        Ok(())
    }
}

struct ValidatedArtifactStream {
    input: ArtifactInputStream,
    validator: Option<ArtifactStreamValidator>,
}

impl Stream for ValidatedArtifactStream {
    type Item = Result<ArtifactChunk, ShardClientError>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.validator.is_none() {
            return Poll::Ready(None);
        }
        match self.input.as_mut().poll_next(context) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(Ok(chunk))) => {
                let result = self
                    .validator
                    .as_mut()
                    .expect("validator exists above")
                    .push(
                        chunk.applied_index,
                        chunk.ordinal,
                        chunk.previous_digest,
                        chunk.payload_digest,
                        chunk.payload,
                    );
                if result.is_err() {
                    self.validator = None;
                }
                Poll::Ready(Some(result))
            }
            Poll::Ready(Some(Err(error))) => {
                self.validator = None;
                Poll::Ready(Some(Err(error)))
            }
            Poll::Ready(None) => {
                let validator = self.validator.take().expect("validator exists above");
                match validator.finish() {
                    Ok(()) => Poll::Ready(None),
                    Err(error) => Poll::Ready(Some(Err(error))),
                }
            }
        }
    }
}

fn validated_artifact_stream(
    input: ArtifactInputStream,
    request: GetArtifactGenerationRequest,
) -> ArtifactChunkStream {
    Box::pin(ValidatedArtifactStream {
        input,
        validator: Some(ArtifactStreamValidator::new(
            usize::from(request.expected_chunk_count()),
            request.expected_total_bytes(),
            request.expected_content_digest(),
        )),
    })
}

fn artifact_corruption(message: &'static str) -> ShardClientError {
    ShardClientError::ArtifactCorruption(message.into())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::{
        ArtifactKind, ArtifactStreamChunk, ArtifactStreamValidator, GetArtifactGenerationRequest,
        PinArtifactGenerationRequest, ShardClientError, ShardRequestContext,
        validated_artifact_stream,
    };
    use tokio_stream::StreamExt;

    fn context() -> ShardRequestContext {
        ShardRequestContext::new(1, 1, 1, 1, u64::MAX).unwrap()
    }

    fn digest(payloads: &[&[u8]]) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        for payload in payloads {
            hasher.update(payload);
        }
        *hasher.finalize().as_bytes()
    }

    #[test]
    fn artifact_get_request_requires_a_complete_bounded_manifest_contract() {
        let content_digest = digest(&[b"a", b"b"]);
        let request = GetArtifactGenerationRequest::new(
            context(),
            9,
            ArtifactKind::Result,
            1,
            2,
            2,
            content_digest,
        )
        .unwrap();
        assert_eq!(request.expected_chunk_count(), 2);
        assert_eq!(request.expected_total_bytes(), 2);
        assert_eq!(request.expected_content_digest(), content_digest);
        assert!(
            PinArtifactGenerationRequest::new(
                context(),
                9,
                ArtifactKind::Result,
                1,
                2,
                2,
                content_digest,
            )
            .is_ok()
        );

        for (count, total_bytes, digest) in [
            (0, 1, content_digest),
            (1, 0, content_digest),
            (2, 1, content_digest),
            (
                1,
                super::MAX_ARTIFACT_CHUNK_BYTES as u64 + 1,
                content_digest,
            ),
            (1, 1, [0; 32]),
        ] {
            assert_eq!(
                GetArtifactGenerationRequest::new(
                    context(),
                    9,
                    ArtifactKind::Result,
                    1,
                    count,
                    total_bytes,
                    digest,
                ),
                Err(ShardClientError::InvalidArtifact)
            );
            assert_eq!(
                PinArtifactGenerationRequest::new(
                    context(),
                    9,
                    ArtifactKind::Result,
                    1,
                    count,
                    total_bytes,
                    digest,
                ),
                Err(ShardClientError::InvalidArtifact)
            );
        }
    }

    #[test]
    fn shard_wide_artifact_generation_cursor_is_complete_or_absent() {
        assert!(super::ArtifactGenerationCursor::new(9, ArtifactKind::Checkpoint, 1).is_ok());
        assert_eq!(
            super::ArtifactGenerationCursor::new(0, ArtifactKind::Checkpoint, 1),
            Err(ShardClientError::InvalidArtifact)
        );
        assert_eq!(
            super::ArtifactGenerationCursor::new(9, ArtifactKind::Checkpoint, 0),
            Err(ShardClientError::InvalidArtifact)
        );
        assert!(super::ListArtifactGenerationHeadsRequest::new(context(), None, 2).is_ok());
    }

    #[test]
    fn artifact_stream_validator_accepts_two_chunks_and_checks_the_full_manifest() {
        let first = b"first";
        let second = b"second";
        let first_digest = *blake3::hash(first).as_bytes();
        let mut validator = ArtifactStreamValidator::new(
            2,
            (first.len() + second.len()) as u64,
            digest(&[first, second]),
        );
        let first = validator
            .push(7, 0, [0; 32], first_digest, first.to_vec())
            .unwrap();
        let second = validator
            .push(
                7,
                1,
                first_digest,
                *blake3::hash(second).as_bytes(),
                second.to_vec(),
            )
            .unwrap();
        assert_eq!(first.applied_index(), 7);
        assert_eq!(second.ordinal(), 1);
        validator.finish().unwrap();
    }

    #[test]
    fn artifact_stream_validator_rejects_every_stream_corruption_class() {
        let good_digest = digest(&[b"a", b"b"]);
        let first_digest = *blake3::hash(b"a").as_bytes();

        let mut truncated = ArtifactStreamValidator::new(2, 2, good_digest);
        truncated
            .push(7, 0, [0; 32], first_digest, b"a".to_vec())
            .unwrap();
        assert!(truncated.finish().is_err());

        for invalid in [
            (
                8,
                1,
                first_digest,
                *blake3::hash(b"b").as_bytes(),
                b"b".to_vec(),
            ),
            (
                7,
                2,
                first_digest,
                *blake3::hash(b"b").as_bytes(),
                b"b".to_vec(),
            ),
            (7, 1, [9; 32], *blake3::hash(b"b").as_bytes(), b"b".to_vec()),
            (7, 1, first_digest, [9; 32], b"b".to_vec()),
            (
                0,
                1,
                first_digest,
                *blake3::hash(b"b").as_bytes(),
                b"b".to_vec(),
            ),
        ] {
            let mut validator = ArtifactStreamValidator::new(2, 2, good_digest);
            validator
                .push(7, 0, [0; 32], first_digest, b"a".to_vec())
                .unwrap();
            assert!(
                validator
                    .push(invalid.0, invalid.1, invalid.2, invalid.3, invalid.4)
                    .is_err()
            );
        }

        let mut overfull = ArtifactStreamValidator::new(1, 1, digest(&[b"a"]));
        overfull
            .push(7, 0, [0; 32], first_digest, b"a".to_vec())
            .unwrap();
        assert!(
            overfull
                .push(
                    7,
                    1,
                    first_digest,
                    *blake3::hash(b"b").as_bytes(),
                    b"b".to_vec()
                )
                .is_err()
        );

        let mut too_many_bytes = ArtifactStreamValidator::new(2, 2, good_digest);
        assert!(
            too_many_bytes
                .push(
                    7,
                    0,
                    [0; 32],
                    *blake3::hash(b"abc").as_bytes(),
                    b"abc".to_vec()
                )
                .is_err()
        );

        let mut wrong_total = ArtifactStreamValidator::new(2, 3, digest(&[b"a", b"b", b"c"]));
        wrong_total
            .push(7, 0, [0; 32], first_digest, b"a".to_vec())
            .unwrap();
        wrong_total
            .push(
                7,
                1,
                first_digest,
                *blake3::hash(b"b").as_bytes(),
                b"b".to_vec(),
            )
            .unwrap();
        assert!(wrong_total.finish().is_err());

        let mut wrong_digest = ArtifactStreamValidator::new(2, 2, digest(&[b"b", b"a"]));
        wrong_digest
            .push(7, 0, [0; 32], first_digest, b"a".to_vec())
            .unwrap();
        wrong_digest
            .push(
                7,
                1,
                first_digest,
                *blake3::hash(b"b").as_bytes(),
                b"b".to_vec(),
            )
            .unwrap();
        assert!(wrong_digest.finish().is_err());
    }

    #[tokio::test]
    async fn artifact_stream_is_demand_driven_and_consumer_drop_stops_polling_input() {
        let first_digest = *blake3::hash(b"a").as_bytes();
        let polled = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&polled);
        let input = tokio_stream::iter(vec![
            Ok(ArtifactStreamChunk {
                applied_index: 7,
                ordinal: 0,
                previous_digest: [0; 32],
                payload_digest: first_digest,
                payload: b"a".to_vec(),
            }),
            Ok(ArtifactStreamChunk {
                applied_index: 7,
                ordinal: 1,
                previous_digest: first_digest,
                payload_digest: *blake3::hash(b"b").as_bytes(),
                payload: b"b".to_vec(),
            }),
        ])
        .map(move |item| {
            observed.fetch_add(1, Ordering::SeqCst);
            item
        });
        let request = GetArtifactGenerationRequest::new(
            context(),
            9,
            ArtifactKind::Result,
            1,
            2,
            2,
            digest(&[b"a", b"b"]),
        )
        .unwrap();
        let mut stream = validated_artifact_stream(Box::pin(input), request);

        tokio::task::yield_now().await;
        assert_eq!(polled.load(Ordering::SeqCst), 0);
        assert!(stream.next().await.unwrap().is_ok());
        assert_eq!(polled.load(Ordering::SeqCst), 1);
        drop(stream);
        tokio::task::yield_now().await;
        assert_eq!(polled.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn artifact_stream_emits_one_terminal_error_on_truncated_eof() {
        let input = tokio_stream::iter(vec![Ok(ArtifactStreamChunk {
            applied_index: 7,
            ordinal: 0,
            previous_digest: [0; 32],
            payload_digest: *blake3::hash(b"a").as_bytes(),
            payload: b"a".to_vec(),
        })]);
        let request = GetArtifactGenerationRequest::new(
            context(),
            9,
            ArtifactKind::Result,
            1,
            2,
            2,
            digest(&[b"a", b"b"]),
        )
        .unwrap();
        let mut stream = validated_artifact_stream(Box::pin(input), request);

        assert!(stream.next().await.unwrap().is_ok());
        assert!(matches!(
            stream.next().await,
            Some(Err(ShardClientError::ArtifactCorruption(_)))
        ));
        assert!(stream.next().await.is_none());
    }
}
