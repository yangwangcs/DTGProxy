use raft_command::{
    AdvanceAnalyticsArtifactFenceV1, AnalyticsArtifactKindV1, DeleteAnalyticsArtifactGenerationV1,
    MAX_ANALYTICS_ARTIFACT_CHUNK_BYTES, PinAnalyticsArtifactGenerationV1,
    PutAnalyticsArtifactChunkV1,
};
use storage_api::{Keyspace, LogicalKey, Mutation, StorageAdapter};

use crate::ShardRuntimeError;

pub const ANALYTICS_ARTIFACT_PREFIX: &[u8] = b"\x01dtg/analytics/v1/";
pub const MAX_ANALYTICS_ARTIFACT_CHUNKS: u16 = raft_command::MAX_ANALYTICS_ARTIFACT_CHUNKS;

const FENCE_PREFIX: &[u8] = b"\x01dtg/analytics/v1/fence/";
const GC_FENCE_KEY: &[u8] = b"\x01dtg/analytics/v1/gc-fence";
const HEAD_PREFIX: &[u8] = b"\x01dtg/analytics/v1/head/";
const CHUNK_PREFIX: &[u8] = b"\x01dtg/analytics/v1/chunk/";
const PIN_PREFIX: &[u8] = b"\x01dtg/analytics/v1/pin/";
const VERSION: u16 = 1;
const FENCE_MAGIC: [u8; 4] = *b"DTAF";
const GC_FENCE_MAGIC: [u8; 4] = *b"DTGF";
const HEAD_MAGIC: [u8; 4] = *b"DTAH";
const CHUNK_MAGIC: [u8; 4] = *b"DTAC";
const PIN_MAGIC: [u8; 4] = *b"DTAP";
const FENCE_VALUE_BYTES: usize = 19;
const GC_FENCE_VALUE_BYTES: usize = 18;
const HEAD_VALUE_BYTES: usize = 52;
const CHUNK_FIXED_BYTES: usize = 78;
const PIN_VALUE_BYTES: usize = 52;
const GENERATION_VALIDATION_BATCH_CHUNKS: usize = 8;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AnalyticsArtifactChunk {
    previous_digest: [u8; 32],
    payload_digest: [u8; 32],
    payload: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AnalyticsArtifactGenerationHead {
    count: u16,
    created_at_unix_ms: u64,
    last_digest: [u8; 32],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AnalyticsArtifactGenerationPin {
    expected_chunk_count: u16,
    expected_total_bytes: u64,
    expected_content_digest: [u8; 32],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AnalyticsArtifactGenerationHeadIdentity {
    job_id: u128,
    kind: AnalyticsArtifactKindV1,
    generation: u64,
}

impl AnalyticsArtifactGenerationHeadIdentity {
    #[must_use]
    pub const fn job_id(self) -> u128 {
        self.job_id
    }

    #[must_use]
    pub const fn kind(self) -> AnalyticsArtifactKindV1 {
        self.kind
    }

    #[must_use]
    pub const fn generation(self) -> u64 {
        self.generation
    }
}

impl AnalyticsArtifactGenerationPin {
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

impl AnalyticsArtifactGenerationHead {
    #[must_use]
    pub const fn count(self) -> u16 {
        self.count
    }

    #[must_use]
    pub const fn created_at_unix_ms(self) -> u64 {
        self.created_at_unix_ms
    }

    #[must_use]
    pub const fn last_digest(self) -> [u8; 32] {
        self.last_digest
    }
}

impl AnalyticsArtifactChunk {
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ArtifactFence {
    generation: u64,
    sealed: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ArtifactHead {
    count: u16,
    created_at_unix_ms: u64,
    last_digest: [u8; 32],
}

pub fn analytics_artifact_chunk_key(
    job_id: u128,
    kind: AnalyticsArtifactKindV1,
    generation: u64,
    ordinal: u64,
) -> LogicalKey {
    let mut key = Vec::with_capacity(CHUNK_PREFIX.len() + 16 + 1 + 8 + 8);
    key.extend_from_slice(CHUNK_PREFIX);
    key.extend_from_slice(&job_id.to_be_bytes());
    key.push(kind_tag(kind));
    key.extend_from_slice(&generation.to_be_bytes());
    key.extend_from_slice(&ordinal.to_be_bytes());
    meta_key(key)
}

pub fn analytics_artifact_generation_head_key(
    job_id: u128,
    kind: AnalyticsArtifactKindV1,
    generation: u64,
) -> LogicalKey {
    analytics_artifact_head_key(job_id, kind, generation)
}

/// Returns the shard-wide persistent GC ownership fence key.
pub fn analytics_artifact_gc_fence_key() -> LogicalKey {
    meta_key(GC_FENCE_KEY.to_vec())
}

/// Returns the reserved Meta-key prefix containing all generation heads for one job and kind.
///
/// This is used only by the explicit artifact-maintenance RPC; generic user scans must continue
/// to reject the reserved Meta keyspace.
pub fn analytics_artifact_generation_head_prefix(
    job_id: u128,
    kind: AnalyticsArtifactKindV1,
) -> Vec<u8> {
    let mut key = Vec::with_capacity(HEAD_PREFIX.len() + 16 + 1);
    key.extend_from_slice(HEAD_PREFIX);
    key.extend_from_slice(&job_id.to_be_bytes());
    key.push(kind_tag(kind));
    key
}

/// Returns the reserved Meta-key prefix containing every analytics Artifact generation head on a
/// Shard. Generic user scans must continue to reject this namespace.
#[must_use]
pub const fn analytics_artifact_generation_heads_prefix() -> &'static [u8] {
    HEAD_PREFIX
}

/// Strictly decodes the shard-wide identity encoded by one generation head key.
pub fn decode_analytics_artifact_generation_head_identity(
    bytes: &[u8],
) -> Result<AnalyticsArtifactGenerationHeadIdentity, ShardRuntimeError> {
    const IDENTITY_BYTES: usize = 16 + 1 + 8;
    if bytes.len() != HEAD_PREFIX.len() + IDENTITY_BYTES || !bytes.starts_with(HEAD_PREFIX) {
        return Err(ShardRuntimeError::CorruptAnalyticsArtifact { record: "head-key" });
    }
    let suffix = &bytes[HEAD_PREFIX.len()..];
    let job_id = u128::from_be_bytes(
        suffix[..16]
            .try_into()
            .expect("validated artifact head job id length"),
    );
    let kind = match suffix[16] {
        1 => AnalyticsArtifactKindV1::Checkpoint,
        2 => AnalyticsArtifactKindV1::Result,
        _ => return Err(ShardRuntimeError::CorruptAnalyticsArtifact { record: "head-key" }),
    };
    let generation = u64::from_be_bytes(
        suffix[17..]
            .try_into()
            .expect("validated artifact head generation length"),
    );
    if job_id == 0 || generation == 0 {
        return Err(ShardRuntimeError::CorruptAnalyticsArtifact { record: "head-key" });
    }
    Ok(AnalyticsArtifactGenerationHeadIdentity {
        job_id,
        kind,
        generation,
    })
}

/// Decodes a generation number from a head key produced by
/// [`analytics_artifact_generation_head_key`].
pub fn decode_analytics_artifact_generation_head_key(
    bytes: &[u8],
    job_id: u128,
    kind: AnalyticsArtifactKindV1,
) -> Result<u64, ShardRuntimeError> {
    let prefix = analytics_artifact_generation_head_prefix(job_id, kind);
    if bytes.len() != prefix.len() + 8 || !bytes.starts_with(&prefix) {
        return Err(ShardRuntimeError::CorruptAnalyticsArtifact { record: "head-key" });
    }
    let generation = u64::from_be_bytes(
        bytes[prefix.len()..]
            .try_into()
            .expect("validated artifact head key length"),
    );
    if generation == 0 {
        return Err(ShardRuntimeError::CorruptAnalyticsArtifact { record: "head-key" });
    }
    Ok(generation)
}

pub fn analytics_artifact_generation_pin_key(
    job_id: u128,
    kind: AnalyticsArtifactKindV1,
    generation: u64,
) -> LogicalKey {
    let mut key = Vec::with_capacity(PIN_PREFIX.len() + 16 + 1 + 8);
    key.extend_from_slice(PIN_PREFIX);
    key.extend_from_slice(&job_id.to_be_bytes());
    key.push(kind_tag(kind));
    key.extend_from_slice(&generation.to_be_bytes());
    meta_key(key)
}

pub fn decode_analytics_artifact_generation_head(
    bytes: &[u8],
) -> Result<AnalyticsArtifactGenerationHead, ShardRuntimeError> {
    let head = decode_head(bytes)?;
    Ok(AnalyticsArtifactGenerationHead {
        count: head.count,
        created_at_unix_ms: head.created_at_unix_ms,
        last_digest: head.last_digest,
    })
}

pub fn decode_analytics_artifact_generation_pin(
    bytes: &[u8],
) -> Result<AnalyticsArtifactGenerationPin, ShardRuntimeError> {
    decode_pin(bytes)
}

pub(crate) async fn prepare_put<A: StorageAdapter>(
    adapter: &A,
    command: &PutAnalyticsArtifactChunkV1,
) -> Result<Vec<Mutation>, ShardRuntimeError> {
    let fence_key = analytics_artifact_fence_key(command.job_id, command.kind);
    let head_key = analytics_artifact_head_key(command.job_id, command.kind, command.generation);
    let chunk_key = analytics_artifact_chunk_key(
        command.job_id,
        command.kind,
        command.generation,
        command.ordinal,
    );
    let pin_key =
        analytics_artifact_generation_pin_key(command.job_id, command.kind, command.generation);
    let values = adapter
        .multi_get(&[
            fence_key.clone(),
            head_key.clone(),
            chunk_key.clone(),
            pin_key,
        ])
        .await?;
    let mut values = values.into_iter();
    let stored_fence = values.next().flatten();
    let stored_head = values.next().flatten();
    let stored_chunk = values.next().flatten();
    let stored_pin = values.next().flatten();
    let expected_chunk = encode_chunk(command);

    let mut mutations = Vec::with_capacity(3);
    let Some(stored_fence) = stored_fence else {
        if stored_head.is_some() || stored_chunk.is_some() || stored_pin.is_some() {
            return Err(ShardRuntimeError::CorruptAnalyticsArtifact {
                record: "missing-fence",
            });
        }
        require_initial_chunk(command)?;
        append_put(
            &mut mutations,
            fence_key,
            encode_fence(ArtifactFence {
                generation: command.generation,
                sealed: false,
            }),
        )?;
        append_put(
            &mut mutations,
            head_key,
            encode_head(ArtifactHead {
                count: 1,
                created_at_unix_ms: command.created_at_unix_ms,
                last_digest: command.payload_digest,
            }),
        )?;
        append_put(&mut mutations, chunk_key, expected_chunk)?;
        return Ok(mutations);
    };
    let fence = decode_fence(&stored_fence)?;

    if let Some(stored_pin) = stored_pin {
        decode_pin(&stored_pin)?;
        return Err(ShardRuntimeError::AnalyticsArtifactFence);
    }

    if command.generation < fence.generation
        || (command.generation == fence.generation && fence.sealed)
    {
        return Err(ShardRuntimeError::AnalyticsArtifactFence);
    }
    if command.generation > fence.generation {
        require_initial_chunk(command)?;
        if stored_head.is_some() || stored_chunk.is_some() {
            return Err(ShardRuntimeError::AnalyticsArtifactConflict);
        }
        append_put(
            &mut mutations,
            fence_key,
            encode_fence(ArtifactFence {
                generation: command.generation,
                sealed: false,
            }),
        )?;
        append_put(
            &mut mutations,
            head_key,
            encode_head(ArtifactHead {
                count: 1,
                created_at_unix_ms: command.created_at_unix_ms,
                last_digest: command.payload_digest,
            }),
        )?;
        append_put(&mut mutations, chunk_key, expected_chunk)?;
        return Ok(mutations);
    }

    let head = stored_head
        .as_deref()
        .ok_or(ShardRuntimeError::CorruptAnalyticsArtifact { record: "head" })
        .and_then(decode_head)?;
    if command.created_at_unix_ms != head.created_at_unix_ms {
        return Err(ShardRuntimeError::AnalyticsArtifactConflict);
    }
    let tail_key = analytics_artifact_chunk_key(
        command.job_id,
        command.kind,
        command.generation,
        u64::from(head.count - 1),
    );
    let tail = adapter
        .multi_get(&[tail_key])
        .await?
        .pop()
        .flatten()
        .ok_or(ShardRuntimeError::CorruptAnalyticsArtifact { record: "tail" })
        .and_then(|bytes| decode_analytics_artifact_chunk(&bytes))?;
    if tail.payload_digest() != head.last_digest {
        return Err(ShardRuntimeError::CorruptAnalyticsArtifact { record: "tail" });
    }
    if let Some(stored_chunk) = stored_chunk {
        if command.ordinal >= u64::from(head.count) {
            return Err(ShardRuntimeError::CorruptAnalyticsArtifact {
                record: "chunk-head",
            });
        }
        decode_analytics_artifact_chunk(&stored_chunk)?;
        if stored_chunk == expected_chunk {
            return Ok(Vec::new());
        }
        return Err(ShardRuntimeError::AnalyticsArtifactConflict);
    }
    if command.ordinal < u64::from(head.count) {
        return Err(ShardRuntimeError::CorruptAnalyticsArtifact {
            record: "missing-chunk",
        });
    }
    if head.count >= MAX_ANALYTICS_ARTIFACT_CHUNKS {
        return Err(ShardRuntimeError::AnalyticsArtifactLimit);
    }
    if command.ordinal != u64::from(head.count) || command.previous_digest != head.last_digest {
        return Err(ShardRuntimeError::AnalyticsArtifactFence);
    }
    append_put(
        &mut mutations,
        head_key,
        encode_head(ArtifactHead {
            count: head.count + 1,
            created_at_unix_ms: head.created_at_unix_ms,
            last_digest: command.payload_digest,
        }),
    )?;
    append_put(&mut mutations, chunk_key, expected_chunk)?;
    Ok(mutations)
}

pub(crate) async fn prepare_delete<A: StorageAdapter>(
    adapter: &A,
    command: &DeleteAnalyticsArtifactGenerationV1,
) -> Result<Vec<Mutation>, ShardRuntimeError> {
    let gc_fence_key = analytics_artifact_gc_fence_key();
    let fence_key = analytics_artifact_fence_key(command.job_id, command.kind);
    let head_key = analytics_artifact_head_key(command.job_id, command.kind, command.generation);
    let pin_key =
        analytics_artifact_generation_pin_key(command.job_id, command.kind, command.generation);
    let values = adapter
        .multi_get(&[
            gc_fence_key.clone(),
            fence_key.clone(),
            head_key.clone(),
            pin_key.clone(),
        ])
        .await?;
    let mut values = values.into_iter();
    let stored_gc_epoch = values
        .next()
        .flatten()
        .as_deref()
        .map(decode_gc_fence)
        .transpose()?
        .unwrap_or(0);
    if command.gc_epoch < stored_gc_epoch {
        return Err(ShardRuntimeError::AnalyticsArtifactFence);
    }
    let update_gc_fence = command.gc_epoch > stored_gc_epoch;
    let fence = values
        .next()
        .flatten()
        .as_deref()
        .ok_or(ShardRuntimeError::AnalyticsArtifactFence)
        .and_then(decode_fence)?;
    let stored_head = values.next().flatten();
    let stored_pin = values.next().flatten();
    if let Some(stored_pin) = &stored_pin {
        decode_pin(stored_pin)?;
        if command.generation >= fence.generation {
            return Err(ShardRuntimeError::AnalyticsArtifactFence);
        }
    }
    if command.generation > fence.generation {
        return Err(ShardRuntimeError::AnalyticsArtifactFence);
    }
    if command.generation == fence.generation && fence.sealed {
        if stored_head.is_some() {
            return Err(ShardRuntimeError::CorruptAnalyticsArtifact {
                record: "sealed-head",
            });
        }
        let mut mutations = Vec::new();
        if update_gc_fence {
            append_put(
                &mut mutations,
                gc_fence_key,
                encode_gc_fence(command.gc_epoch),
            )?;
        }
        return Ok(mutations);
    }
    if command.generation < fence.generation && stored_head.is_none() {
        let mut mutations = Vec::new();
        if update_gc_fence {
            append_put(
                &mut mutations,
                gc_fence_key,
                encode_gc_fence(command.gc_epoch),
            )?;
        }
        return Ok(mutations);
    }
    let head = stored_head
        .as_deref()
        .ok_or(ShardRuntimeError::CorruptAnalyticsArtifact { record: "head" })
        .and_then(decode_head)?;
    validate_generation_chunks(
        adapter,
        command.job_id,
        command.kind,
        command.generation,
        &head,
    )
    .await?;

    let mut mutations = Vec::with_capacity(usize::from(head.count) + 3);
    if update_gc_fence {
        append_put(
            &mut mutations,
            gc_fence_key,
            encode_gc_fence(command.gc_epoch),
        )?;
    }
    for ordinal in 0..u64::from(head.count) {
        append_delete(
            &mut mutations,
            analytics_artifact_chunk_key(command.job_id, command.kind, command.generation, ordinal),
        )?;
    }
    append_delete(&mut mutations, head_key)?;
    if stored_pin.is_some() {
        append_delete(&mut mutations, pin_key)?;
    }
    if command.generation < fence.generation {
        return Ok(mutations);
    }
    append_put(
        &mut mutations,
        fence_key,
        encode_fence(ArtifactFence {
            generation: command.generation,
            sealed: true,
        }),
    )?;
    Ok(mutations)
}

pub(crate) async fn prepare_advance_fence<A: StorageAdapter>(
    adapter: &A,
    command: &AdvanceAnalyticsArtifactFenceV1,
) -> Result<Vec<Mutation>, ShardRuntimeError> {
    let gc_fence_key = analytics_artifact_gc_fence_key();
    let fence_key = analytics_artifact_fence_key(command.job_id, command.kind);
    let head_key = analytics_artifact_head_key(command.job_id, command.kind, command.generation);
    let pin_key =
        analytics_artifact_generation_pin_key(command.job_id, command.kind, command.generation);
    let values = adapter
        .multi_get(&[gc_fence_key.clone(), fence_key.clone(), head_key, pin_key])
        .await?;
    let mut values = values.into_iter();
    let stored_gc_epoch = values
        .next()
        .flatten()
        .as_deref()
        .map(decode_gc_fence)
        .transpose()?
        .unwrap_or(0);
    if command.gc_epoch < stored_gc_epoch {
        return Err(ShardRuntimeError::AnalyticsArtifactFence);
    }
    let fence = values
        .next()
        .flatten()
        .as_deref()
        .ok_or(ShardRuntimeError::AnalyticsArtifactFence)
        .and_then(decode_fence)?;
    let target_head = values.next().flatten();
    let target_pin = values.next().flatten();
    if command.generation < fence.generation
        || (command.generation == fence.generation && !fence.sealed)
    {
        return Err(ShardRuntimeError::AnalyticsArtifactFence);
    }
    if command.generation > fence.generation && (target_head.is_some() || target_pin.is_some()) {
        return Err(ShardRuntimeError::AnalyticsArtifactConflict);
    }
    let mut mutations = Vec::with_capacity(2);
    if command.gc_epoch > stored_gc_epoch {
        append_put(
            &mut mutations,
            gc_fence_key,
            encode_gc_fence(command.gc_epoch),
        )?;
    }
    if command.generation > fence.generation {
        append_put(
            &mut mutations,
            fence_key,
            encode_fence(ArtifactFence {
                generation: command.generation,
                sealed: true,
            }),
        )?;
    }
    Ok(mutations)
}

pub(crate) async fn prepare_pin<A: StorageAdapter>(
    adapter: &A,
    command: &PinAnalyticsArtifactGenerationV1,
) -> Result<Vec<Mutation>, ShardRuntimeError> {
    let fence_key = analytics_artifact_fence_key(command.job_id, command.kind);
    let head_key = analytics_artifact_head_key(command.job_id, command.kind, command.generation);
    let pin_key =
        analytics_artifact_generation_pin_key(command.job_id, command.kind, command.generation);
    let values = adapter
        .multi_get(&[fence_key, head_key, pin_key.clone()])
        .await?;
    let mut values = values.into_iter();
    let fence = values
        .next()
        .flatten()
        .as_deref()
        .ok_or(ShardRuntimeError::AnalyticsArtifactFence)
        .and_then(decode_fence)?;
    if command.generation > fence.generation
        || (command.generation == fence.generation && fence.sealed)
    {
        return Err(ShardRuntimeError::AnalyticsArtifactFence);
    }
    let head = values
        .next()
        .flatten()
        .as_deref()
        .ok_or_else(|| corrupt("head"))
        .and_then(decode_head)?;
    if head.count != command.expected_chunk_count {
        return Err(ShardRuntimeError::AnalyticsArtifactConflict);
    }
    let expected_pin = AnalyticsArtifactGenerationPin {
        expected_chunk_count: command.expected_chunk_count,
        expected_total_bytes: command.expected_total_bytes,
        expected_content_digest: command.expected_content_digest,
    };
    let stored_pin = values.next().flatten();
    if let Some(stored_pin) = stored_pin.as_deref()
        && decode_pin(stored_pin)? != expected_pin
    {
        return Err(ShardRuntimeError::AnalyticsArtifactConflict);
    }
    let validated = validate_generation_chunks(
        adapter,
        command.job_id,
        command.kind,
        command.generation,
        &head,
    )
    .await?;
    if validated.total_bytes != command.expected_total_bytes
        || validated.content_digest != command.expected_content_digest
    {
        return Err(ShardRuntimeError::AnalyticsArtifactConflict);
    }
    if stored_pin.is_some() {
        return Ok(Vec::new());
    }
    let mut mutations = Vec::with_capacity(1);
    append_put(&mut mutations, pin_key, encode_pin(expected_pin))?;
    Ok(mutations)
}

pub fn decode_analytics_artifact_chunk(
    bytes: &[u8],
) -> Result<AnalyticsArtifactChunk, ShardRuntimeError> {
    if bytes.len() < CHUNK_FIXED_BYTES
        || bytes[..4] != CHUNK_MAGIC
        || version(bytes) != Some(VERSION)
    {
        return Err(corrupt("chunk"));
    }
    let payload_length = usize::try_from(u32::from_be_bytes(
        bytes[70..74]
            .try_into()
            .expect("fixed artifact chunk length slice"),
    ))
    .expect("u32 fits usize on supported platforms");
    if payload_length == 0
        || payload_length > MAX_ANALYTICS_ARTIFACT_CHUNK_BYTES
        || bytes.len() != CHUNK_FIXED_BYTES + payload_length
        || !valid_checksum(bytes)
    {
        return Err(corrupt("chunk"));
    }
    let previous_digest = bytes[6..38]
        .try_into()
        .expect("fixed artifact previous digest slice");
    let payload_digest = bytes[38..70]
        .try_into()
        .expect("fixed artifact payload digest slice");
    let payload = bytes[74..74 + payload_length].to_vec();
    if payload_digest != *blake3::hash(&payload).as_bytes() {
        return Err(corrupt("chunk"));
    }
    Ok(AnalyticsArtifactChunk {
        previous_digest,
        payload_digest,
        payload,
    })
}

fn analytics_artifact_fence_key(job_id: u128, kind: AnalyticsArtifactKindV1) -> LogicalKey {
    let mut key = Vec::with_capacity(FENCE_PREFIX.len() + 16 + 1);
    key.extend_from_slice(FENCE_PREFIX);
    key.extend_from_slice(&job_id.to_be_bytes());
    key.push(kind_tag(kind));
    meta_key(key)
}

fn analytics_artifact_head_key(
    job_id: u128,
    kind: AnalyticsArtifactKindV1,
    generation: u64,
) -> LogicalKey {
    let mut key = Vec::with_capacity(HEAD_PREFIX.len() + 16 + 1 + 8);
    key.extend_from_slice(HEAD_PREFIX);
    key.extend_from_slice(&job_id.to_be_bytes());
    key.push(kind_tag(kind));
    key.extend_from_slice(&generation.to_be_bytes());
    meta_key(key)
}

fn kind_tag(kind: AnalyticsArtifactKindV1) -> u8 {
    match kind {
        AnalyticsArtifactKindV1::Checkpoint => 1,
        AnalyticsArtifactKindV1::Result => 2,
    }
}

fn meta_key(bytes: Vec<u8>) -> LogicalKey {
    LogicalKey::in_keyspace(Keyspace::Meta, bytes)
}

fn require_initial_chunk(command: &PutAnalyticsArtifactChunkV1) -> Result<(), ShardRuntimeError> {
    if command.ordinal == 0 && command.previous_digest == [0; 32] {
        Ok(())
    } else {
        Err(ShardRuntimeError::AnalyticsArtifactFence)
    }
}

fn encode_fence(fence: ArtifactFence) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(FENCE_VALUE_BYTES);
    bytes.extend_from_slice(&FENCE_MAGIC);
    bytes.extend_from_slice(&VERSION.to_be_bytes());
    bytes.extend_from_slice(&fence.generation.to_be_bytes());
    bytes.push(u8::from(fence.sealed));
    append_checksum(&mut bytes);
    bytes
}

fn encode_gc_fence(gc_epoch: u64) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(GC_FENCE_VALUE_BYTES);
    bytes.extend_from_slice(&GC_FENCE_MAGIC);
    bytes.extend_from_slice(&VERSION.to_be_bytes());
    bytes.extend_from_slice(&gc_epoch.to_be_bytes());
    append_checksum(&mut bytes);
    bytes
}

fn decode_gc_fence(bytes: &[u8]) -> Result<u64, ShardRuntimeError> {
    if bytes.len() != GC_FENCE_VALUE_BYTES
        || bytes[..4] != GC_FENCE_MAGIC
        || version(bytes) != Some(VERSION)
        || !valid_checksum(bytes)
    {
        return Err(corrupt("gc-fence"));
    }
    let epoch = u64::from_be_bytes(
        bytes[6..14]
            .try_into()
            .expect("fixed analytics GC epoch slice"),
    );
    if epoch == 0 {
        return Err(corrupt("gc-fence"));
    }
    Ok(epoch)
}

fn decode_fence(bytes: &[u8]) -> Result<ArtifactFence, ShardRuntimeError> {
    if bytes.len() != FENCE_VALUE_BYTES
        || bytes[..4] != FENCE_MAGIC
        || version(bytes) != Some(VERSION)
        || !valid_checksum(bytes)
    {
        return Err(corrupt("fence"));
    }
    let generation = u64::from_be_bytes(
        bytes[6..14]
            .try_into()
            .expect("fixed artifact generation slice"),
    );
    let sealed = match bytes[14] {
        0 => false,
        1 => true,
        _ => return Err(corrupt("fence")),
    };
    if generation == 0 {
        return Err(corrupt("fence"));
    }
    Ok(ArtifactFence { generation, sealed })
}

fn encode_head(head: ArtifactHead) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(HEAD_VALUE_BYTES);
    bytes.extend_from_slice(&HEAD_MAGIC);
    bytes.extend_from_slice(&VERSION.to_be_bytes());
    bytes.extend_from_slice(&head.count.to_be_bytes());
    bytes.extend_from_slice(&head.created_at_unix_ms.to_be_bytes());
    bytes.extend_from_slice(&head.last_digest);
    append_checksum(&mut bytes);
    bytes
}

fn decode_head(bytes: &[u8]) -> Result<ArtifactHead, ShardRuntimeError> {
    if bytes.len() != HEAD_VALUE_BYTES
        || bytes[..4] != HEAD_MAGIC
        || version(bytes) != Some(VERSION)
        || !valid_checksum(bytes)
    {
        return Err(corrupt("head"));
    }
    let count = u16::from_be_bytes(bytes[6..8].try_into().expect("fixed artifact count slice"));
    let created_at_unix_ms = u64::from_be_bytes(
        bytes[8..16]
            .try_into()
            .expect("fixed artifact creation time slice"),
    );
    if count == 0 || count > MAX_ANALYTICS_ARTIFACT_CHUNKS || created_at_unix_ms == 0 {
        return Err(corrupt("head"));
    }
    Ok(ArtifactHead {
        count,
        created_at_unix_ms,
        last_digest: bytes[16..48]
            .try_into()
            .expect("fixed artifact last digest slice"),
    })
}

fn encode_pin(pin: AnalyticsArtifactGenerationPin) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(PIN_VALUE_BYTES);
    bytes.extend_from_slice(&PIN_MAGIC);
    bytes.extend_from_slice(&VERSION.to_be_bytes());
    bytes.extend_from_slice(&pin.expected_chunk_count.to_be_bytes());
    bytes.extend_from_slice(&pin.expected_total_bytes.to_be_bytes());
    bytes.extend_from_slice(&pin.expected_content_digest);
    append_checksum(&mut bytes);
    bytes
}

fn decode_pin(bytes: &[u8]) -> Result<AnalyticsArtifactGenerationPin, ShardRuntimeError> {
    if bytes.len() != PIN_VALUE_BYTES
        || bytes[..4] != PIN_MAGIC
        || version(bytes) != Some(VERSION)
        || !valid_checksum(bytes)
    {
        return Err(corrupt("pin"));
    }
    let expected_chunk_count =
        u16::from_be_bytes(bytes[6..8].try_into().expect("fixed artifact pin count"));
    let expected_total_bytes = u64::from_be_bytes(
        bytes[8..16]
            .try_into()
            .expect("fixed artifact pin total bytes"),
    );
    let expected_content_digest = bytes[16..48]
        .try_into()
        .expect("fixed artifact pin content digest");
    let maximum_total_bytes = u64::from(expected_chunk_count)
        .checked_mul(
            u64::try_from(MAX_ANALYTICS_ARTIFACT_CHUNK_BYTES)
                .expect("artifact chunk limit fits u64"),
        )
        .ok_or_else(|| corrupt("pin"))?;
    if expected_chunk_count == 0
        || expected_chunk_count > MAX_ANALYTICS_ARTIFACT_CHUNKS
        || expected_total_bytes < u64::from(expected_chunk_count)
        || expected_total_bytes > maximum_total_bytes
        || expected_content_digest == [0; 32]
    {
        return Err(corrupt("pin"));
    }
    Ok(AnalyticsArtifactGenerationPin {
        expected_chunk_count,
        expected_total_bytes,
        expected_content_digest,
    })
}

struct GenerationValidation {
    total_bytes: u64,
    content_digest: [u8; 32],
}

async fn validate_generation_chunks<A: StorageAdapter>(
    adapter: &A,
    job_id: u128,
    kind: AnalyticsArtifactKindV1,
    generation: u64,
    head: &ArtifactHead,
) -> Result<GenerationValidation, ShardRuntimeError> {
    let mut previous_digest = [0; 32];
    let mut total_bytes = 0_u64;
    let mut content_hasher = blake3::Hasher::new();
    let count = usize::from(head.count);
    for start in (0..count).step_by(GENERATION_VALIDATION_BATCH_CHUNKS) {
        let end = (start + GENERATION_VALIDATION_BATCH_CHUNKS).min(count);
        let keys = (start..end)
            .map(|ordinal| {
                analytics_artifact_chunk_key(
                    job_id,
                    kind,
                    generation,
                    u64::try_from(ordinal).expect("artifact ordinal fits u64"),
                )
            })
            .collect::<Vec<_>>();
        let chunks = adapter.multi_get(&keys).await?;
        if chunks.len() != keys.len() {
            return Err(corrupt("chunks"));
        }
        for bytes in chunks {
            let bytes = bytes.as_deref().ok_or_else(|| corrupt("missing-chunk"))?;
            let chunk = decode_analytics_artifact_chunk(bytes)?;
            if chunk.previous_digest() != previous_digest {
                return Err(corrupt("chain"));
            }
            previous_digest = chunk.payload_digest();
            total_bytes = total_bytes
                .checked_add(
                    u64::try_from(chunk.payload().len()).map_err(|_| corrupt("total-bytes"))?,
                )
                .ok_or_else(|| corrupt("total-bytes"))?;
            content_hasher.update(chunk.payload());
        }
    }
    if previous_digest != head.last_digest {
        return Err(corrupt("head-tail"));
    }
    Ok(GenerationValidation {
        total_bytes,
        content_digest: *content_hasher.finalize().as_bytes(),
    })
}

fn encode_chunk(command: &PutAnalyticsArtifactChunkV1) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(CHUNK_FIXED_BYTES + command.payload.len());
    bytes.extend_from_slice(&CHUNK_MAGIC);
    bytes.extend_from_slice(&VERSION.to_be_bytes());
    bytes.extend_from_slice(&command.previous_digest);
    bytes.extend_from_slice(&command.payload_digest);
    bytes.extend_from_slice(
        &u32::try_from(command.payload.len())
            .expect("artifact command payload bound fits u32")
            .to_be_bytes(),
    );
    bytes.extend_from_slice(&command.payload);
    append_checksum(&mut bytes);
    bytes
}

fn version(bytes: &[u8]) -> Option<u16> {
    bytes
        .get(4..6)
        .map(|value| u16::from_be_bytes(value.try_into().expect("fixed artifact version slice")))
}

fn append_checksum(bytes: &mut Vec<u8>) {
    bytes.extend_from_slice(&crc32fast::hash(bytes).to_be_bytes());
}

fn valid_checksum(bytes: &[u8]) -> bool {
    if bytes.len() < 4 {
        return false;
    }
    let offset = bytes.len() - 4;
    let stored = u32::from_be_bytes(bytes[offset..].try_into().expect("fixed artifact checksum"));
    crc32fast::hash(&bytes[..offset]) == stored
}

fn append_put(
    mutations: &mut Vec<Mutation>,
    key: LogicalKey,
    value: Vec<u8>,
) -> Result<(), ShardRuntimeError> {
    let sequence =
        u32::try_from(mutations.len()).map_err(|_| ShardRuntimeError::TooManyMutations)?;
    mutations.push(Mutation::put(sequence, key, value));
    Ok(())
}

fn append_delete(mutations: &mut Vec<Mutation>, key: LogicalKey) -> Result<(), ShardRuntimeError> {
    let sequence =
        u32::try_from(mutations.len()).map_err(|_| ShardRuntimeError::TooManyMutations)?;
    mutations.push(Mutation::delete(sequence, key));
    Ok(())
}

fn corrupt(record: &'static str) -> ShardRuntimeError {
    ShardRuntimeError::CorruptAnalyticsArtifact { record }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll, Wake, Waker};

    use adapter_memory::MemoryAdapter;
    use raft_command::{
        AdvanceAnalyticsArtifactFenceV1, AnalyticsArtifactKindV1,
        DeleteAnalyticsArtifactGenerationV1, PinAnalyticsArtifactGenerationV1,
        PutAnalyticsArtifactChunkV1,
    };
    use storage_api::{
        AdapterCapabilities, AdapterFuture, ApplyReceipt, CommittedMutationBatch, KeySpan,
        KeyValue, LogicalKey, Mutation, StorageAdapter,
    };

    use super::{
        ArtifactFence, ArtifactHead, analytics_artifact_chunk_key, analytics_artifact_fence_key,
        analytics_artifact_generation_head_prefix, analytics_artifact_generation_heads_prefix,
        analytics_artifact_generation_pin_key, analytics_artifact_head_key, append_checksum,
        decode_analytics_artifact_generation_head,
        decode_analytics_artifact_generation_head_identity,
        decode_analytics_artifact_generation_head_key, encode_chunk, encode_fence, encode_head,
        prepare_advance_fence, prepare_delete, prepare_pin, prepare_put,
    };
    use crate::ShardRuntimeError;

    #[test]
    fn generation_head_prefix_is_canonical_and_rejects_cross_job_keys() {
        let job_id = 42_u128;
        let kind = AnalyticsArtifactKindV1::Result;
        let key = analytics_artifact_head_key(job_id, kind, 7);
        let prefix = analytics_artifact_generation_head_prefix(job_id, kind);
        assert!(key.as_bytes().starts_with(&prefix));
        assert_eq!(
            decode_analytics_artifact_generation_head_key(key.as_bytes(), job_id, kind).unwrap(),
            7
        );
        assert!(
            decode_analytics_artifact_generation_head_key(key.as_bytes(), job_id + 1, kind)
                .is_err()
        );
        assert!(
            decode_analytics_artifact_generation_head_key(
                &key.as_bytes()[..key.as_bytes().len() - 1],
                job_id,
                kind
            )
            .is_err()
        );
    }

    #[test]
    fn shard_wide_generation_head_identity_decoder_is_strict_and_canonical() {
        let prefix = analytics_artifact_generation_heads_prefix();
        let key = analytics_artifact_head_key(501, AnalyticsArtifactKindV1::Result, 7);
        assert!(key.as_bytes().starts_with(prefix));
        let decoded = decode_analytics_artifact_generation_head_identity(key.as_bytes()).unwrap();
        assert_eq!(decoded.job_id(), 501);
        assert_eq!(decoded.kind(), AnalyticsArtifactKindV1::Result);
        assert_eq!(decoded.generation(), 7);

        let ordered = [
            analytics_artifact_head_key(500, AnalyticsArtifactKindV1::Checkpoint, 9),
            analytics_artifact_head_key(500, AnalyticsArtifactKindV1::Result, 1),
            analytics_artifact_head_key(501, AnalyticsArtifactKindV1::Checkpoint, 1),
        ];
        assert!(ordered.windows(2).all(|pair| pair[0] < pair[1]));

        let mut zero_job = key.as_bytes().to_vec();
        zero_job[prefix.len()..prefix.len() + 16].fill(0);
        let mut unknown_kind = key.as_bytes().to_vec();
        unknown_kind[prefix.len() + 16] = 3;
        let mut zero_generation = key.as_bytes().to_vec();
        zero_generation[prefix.len() + 17..].fill(0);
        let mut cross_prefix = key.as_bytes().to_vec();
        cross_prefix[0] ^= 0xff;
        let mut trailing = key.as_bytes().to_vec();
        trailing.push(0);
        for invalid in [
            zero_job,
            unknown_kind,
            zero_generation,
            cross_prefix,
            key.as_bytes()[..key.as_bytes().len() - 1].to_vec(),
            trailing,
        ] {
            assert!(decode_analytics_artifact_generation_head_identity(&invalid).is_err());
        }
    }

    #[test]
    fn generation_head_persists_created_at_and_rejects_legacy_or_corrupt_encodings() {
        let encoded = encode_head(ArtifactHead {
            count: 2,
            created_at_unix_ms: 1_725_000_000_123,
            last_digest: [7; 32],
        });
        let decoded = decode_analytics_artifact_generation_head(&encoded).unwrap();
        assert_eq!(decoded.count(), 2);
        assert_eq!(decoded.created_at_unix_ms(), 1_725_000_000_123);
        assert_eq!(decoded.last_digest(), [7; 32]);

        let mut legacy = encoded.clone();
        legacy.drain(8..16);
        let checksum = crc32fast::hash(&legacy[..legacy.len() - 4]);
        let offset = legacy.len() - 4;
        legacy[offset..].copy_from_slice(&checksum.to_be_bytes());
        assert!(decode_analytics_artifact_generation_head(&legacy).is_err());
        assert!(decode_analytics_artifact_generation_head(&encoded[..encoded.len() - 1]).is_err());

        let mut corrupt = encoded;
        corrupt[8] ^= 1;
        assert!(decode_analytics_artifact_generation_head(&corrupt).is_err());
    }

    #[test]
    fn put_rejects_a_different_created_at_within_one_generation() {
        let adapter = MemoryAdapter::new();
        let first = PutAnalyticsArtifactChunkV1::new(
            602,
            AnalyticsArtifactKindV1::Checkpoint,
            1,
            1_725_000_000_123,
            0,
            [0; 32],
            b"first".to_vec(),
        )
        .unwrap();
        persist_artifact(
            &adapter,
            first.job_id,
            first.kind,
            ArtifactFence {
                generation: first.generation,
                sealed: false,
            },
            ArtifactHead {
                count: 1,
                created_at_unix_ms: first.created_at_unix_ms,
                last_digest: first.payload_digest,
            },
            &[(0, &first)],
        );

        let second = PutAnalyticsArtifactChunkV1::new(
            602,
            AnalyticsArtifactKindV1::Checkpoint,
            1,
            1_725_000_000_124,
            1,
            first.payload_digest,
            b"second".to_vec(),
        )
        .unwrap();
        assert!(matches!(
            block_on(prepare_put(&adapter, &second)),
            Err(ShardRuntimeError::AnalyticsArtifactConflict)
        ));
    }

    #[test]
    fn exact_chunk_beyond_head_count_is_corrupt_not_idempotent() {
        let adapter = MemoryAdapter::new();
        let tail = PutAnalyticsArtifactChunkV1::new(
            601,
            AnalyticsArtifactKindV1::Checkpoint,
            1,
            1_725_000_000_123,
            0,
            [0; 32],
            b"tail".to_vec(),
        )
        .unwrap();
        let command = PutAnalyticsArtifactChunkV1::new(
            601,
            AnalyticsArtifactKindV1::Checkpoint,
            1,
            1_725_000_000_123,
            1,
            [9; 32],
            b"orphaned".to_vec(),
        )
        .unwrap();
        block_on(adapter.apply_committed(CommittedMutationBatch {
            shard_id: 7,
            log_index: 1,
            txn_id: 1,
            mutations: vec![
                Mutation::put(
                    0,
                    analytics_artifact_fence_key(command.job_id, command.kind),
                    encode_fence(ArtifactFence {
                        generation: command.generation,
                        sealed: false,
                    }),
                ),
                Mutation::put(
                    1,
                    analytics_artifact_head_key(command.job_id, command.kind, command.generation),
                    encode_head(ArtifactHead {
                        count: 1,
                        created_at_unix_ms: tail.created_at_unix_ms,
                        last_digest: tail.payload_digest,
                    }),
                ),
                Mutation::put(
                    2,
                    analytics_artifact_chunk_key(
                        tail.job_id,
                        tail.kind,
                        tail.generation,
                        tail.ordinal,
                    ),
                    encode_chunk(&tail),
                ),
                Mutation::put(
                    3,
                    analytics_artifact_chunk_key(
                        command.job_id,
                        command.kind,
                        command.generation,
                        command.ordinal,
                    ),
                    encode_chunk(&command),
                ),
            ],
        }))
        .unwrap();

        assert!(matches!(
            block_on(prepare_put(&adapter, &command)),
            Err(ShardRuntimeError::CorruptAnalyticsArtifact {
                record: "chunk-head"
            })
        ));
    }

    #[test]
    fn put_rejects_a_forged_tail_and_a_missing_retained_chunk() {
        let forged_tail = PutAnalyticsArtifactChunkV1::new(
            602,
            AnalyticsArtifactKindV1::Checkpoint,
            1,
            1_725_000_000_123,
            0,
            [0; 32],
            b"tail".to_vec(),
        )
        .unwrap();
        let forged_append = PutAnalyticsArtifactChunkV1::new(
            602,
            AnalyticsArtifactKindV1::Checkpoint,
            1,
            1_725_000_000_123,
            1,
            [7; 32],
            b"append".to_vec(),
        )
        .unwrap();
        let adapter = MemoryAdapter::new();
        persist_artifact(
            &adapter,
            forged_append.job_id,
            forged_append.kind,
            ArtifactFence {
                generation: 1,
                sealed: false,
            },
            ArtifactHead {
                count: 1,
                created_at_unix_ms: forged_tail.created_at_unix_ms,
                last_digest: [7; 32],
            },
            &[(0, &forged_tail)],
        );
        assert!(matches!(
            block_on(prepare_put(&adapter, &forged_append)),
            Err(ShardRuntimeError::CorruptAnalyticsArtifact { record: "tail" })
        ));
        assert_eq!(adapter.applied_log_index().unwrap(), 1);

        let retained_tail = PutAnalyticsArtifactChunkV1::new(
            603,
            AnalyticsArtifactKindV1::Checkpoint,
            1,
            1_725_000_000_123,
            1,
            [4; 32],
            b"retained-tail".to_vec(),
        )
        .unwrap();
        let missing_first = PutAnalyticsArtifactChunkV1::new(
            603,
            AnalyticsArtifactKindV1::Checkpoint,
            1,
            1_725_000_000_123,
            0,
            [0; 32],
            b"missing-first".to_vec(),
        )
        .unwrap();
        let adapter = MemoryAdapter::new();
        persist_artifact(
            &adapter,
            missing_first.job_id,
            missing_first.kind,
            ArtifactFence {
                generation: 1,
                sealed: false,
            },
            ArtifactHead {
                count: 2,
                created_at_unix_ms: retained_tail.created_at_unix_ms,
                last_digest: retained_tail.payload_digest,
            },
            &[(1, &retained_tail)],
        );
        assert!(matches!(
            block_on(prepare_put(&adapter, &missing_first)),
            Err(ShardRuntimeError::CorruptAnalyticsArtifact {
                record: "missing-chunk"
            })
        ));
        assert_eq!(adapter.applied_log_index().unwrap(), 1);
    }

    #[test]
    fn delete_rejects_a_broken_chain_before_constructing_mutations() {
        let first = PutAnalyticsArtifactChunkV1::new(
            604,
            AnalyticsArtifactKindV1::Result,
            1,
            1_725_000_000_123,
            0,
            [0; 32],
            b"first".to_vec(),
        )
        .unwrap();
        let broken = PutAnalyticsArtifactChunkV1::new(
            604,
            AnalyticsArtifactKindV1::Result,
            1,
            1_725_000_000_123,
            1,
            [9; 32],
            b"broken".to_vec(),
        )
        .unwrap();
        let adapter = MemoryAdapter::new();
        persist_artifact(
            &adapter,
            first.job_id,
            first.kind,
            ArtifactFence {
                generation: 1,
                sealed: false,
            },
            ArtifactHead {
                count: 2,
                created_at_unix_ms: first.created_at_unix_ms,
                last_digest: broken.payload_digest,
            },
            &[(0, &first), (1, &broken)],
        );
        let delete =
            DeleteAnalyticsArtifactGenerationV1::new(first.job_id, first.kind, first.generation)
                .unwrap();
        assert!(matches!(
            block_on(super::prepare_delete(&adapter, &delete)),
            Err(ShardRuntimeError::CorruptAnalyticsArtifact { record: "chain" })
        ));
        assert_eq!(adapter.applied_log_index().unwrap(), 1);
    }

    #[test]
    fn delete_fails_closed_for_missing_checksum_and_head_tail_corruption() {
        let first = PutAnalyticsArtifactChunkV1::new(
            607,
            AnalyticsArtifactKindV1::Result,
            1,
            1_725_000_000_123,
            0,
            [0; 32],
            b"first".to_vec(),
        )
        .unwrap();
        let delete =
            DeleteAnalyticsArtifactGenerationV1::new(first.job_id, first.kind, first.generation)
                .unwrap();

        let adapter = MemoryAdapter::new();
        persist_artifact(
            &adapter,
            first.job_id,
            first.kind,
            ArtifactFence {
                generation: 1,
                sealed: false,
            },
            ArtifactHead {
                count: 1,
                created_at_unix_ms: first.created_at_unix_ms,
                last_digest: first.payload_digest,
            },
            &[],
        );
        assert!(matches!(
            block_on(super::prepare_delete(&adapter, &delete)),
            Err(ShardRuntimeError::CorruptAnalyticsArtifact {
                record: "missing-chunk"
            })
        ));
        assert_eq!(adapter.applied_log_index().unwrap(), 1);

        let adapter = MemoryAdapter::new();
        persist_artifact(
            &adapter,
            first.job_id,
            first.kind,
            ArtifactFence {
                generation: 1,
                sealed: false,
            },
            ArtifactHead {
                count: 1,
                created_at_unix_ms: first.created_at_unix_ms,
                last_digest: first.payload_digest,
            },
            &[(0, &first)],
        );
        let mut corrupted = encode_chunk(&first);
        corrupted[16] ^= 1;
        block_on(adapter.apply_committed(CommittedMutationBatch {
            shard_id: 7,
            log_index: 2,
            txn_id: 2,
            mutations: vec![Mutation::put(
                0,
                analytics_artifact_chunk_key(first.job_id, first.kind, first.generation, 0),
                corrupted,
            )],
        }))
        .unwrap();
        assert!(matches!(
            block_on(super::prepare_delete(&adapter, &delete)),
            Err(ShardRuntimeError::CorruptAnalyticsArtifact { record: "chunk" })
        ));
        assert_eq!(adapter.applied_log_index().unwrap(), 2);

        let adapter = MemoryAdapter::new();
        persist_artifact(
            &adapter,
            first.job_id,
            first.kind,
            ArtifactFence {
                generation: 1,
                sealed: false,
            },
            ArtifactHead {
                count: 1,
                created_at_unix_ms: first.created_at_unix_ms,
                last_digest: [7; 32],
            },
            &[(0, &first)],
        );
        assert!(matches!(
            block_on(super::prepare_delete(&adapter, &delete)),
            Err(ShardRuntimeError::CorruptAnalyticsArtifact {
                record: "head-tail"
            })
        ));
        assert_eq!(adapter.applied_log_index().unwrap(), 1);
    }

    #[test]
    fn deleting_missing_old_or_sealed_current_generation_is_idempotent() {
        let old =
            DeleteAnalyticsArtifactGenerationV1::new(608, AnalyticsArtifactKindV1::Checkpoint, 1)
                .unwrap();
        let adapter = MemoryAdapter::new();
        persist_artifact(
            &adapter,
            old.job_id,
            old.kind,
            ArtifactFence {
                generation: 2,
                sealed: false,
            },
            ArtifactHead {
                count: 1,
                created_at_unix_ms: 1_725_000_000_123,
                last_digest: [4; 32],
            },
            &[],
        );
        assert_eq!(
            block_on(super::prepare_delete(&adapter, &old))
                .unwrap()
                .len(),
            1
        );

        let sealed =
            DeleteAnalyticsArtifactGenerationV1::new(609, AnalyticsArtifactKindV1::Checkpoint, 1)
                .unwrap();
        let adapter = MemoryAdapter::new();
        block_on(adapter.apply_committed(CommittedMutationBatch {
            shard_id: 7,
            log_index: 1,
            txn_id: 1,
            mutations: vec![Mutation::put(
                0,
                analytics_artifact_fence_key(sealed.job_id, sealed.kind),
                encode_fence(ArtifactFence {
                    generation: sealed.generation,
                    sealed: true,
                }),
            )],
        }))
        .unwrap();
        assert_eq!(
            block_on(super::prepare_delete(&adapter, &sealed))
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn delete_rejects_an_owner_with_an_older_persisted_gc_epoch() {
        let first = PutAnalyticsArtifactChunkV1::new(
            612,
            AnalyticsArtifactKindV1::Result,
            1,
            1_725_000_000_123,
            0,
            [0; 32],
            b"result".to_vec(),
        )
        .unwrap();
        let adapter = MemoryAdapter::new();
        persist_artifact(
            &adapter,
            first.job_id,
            first.kind,
            ArtifactFence {
                generation: 1,
                sealed: false,
            },
            ArtifactHead {
                count: 1,
                created_at_unix_ms: first.created_at_unix_ms,
                last_digest: first.payload_digest,
            },
            &[(0, &first)],
        );
        let current = DeleteAnalyticsArtifactGenerationV1::new_with_gc_epoch(
            first.job_id,
            first.kind,
            first.generation,
            7,
        )
        .unwrap();
        let mutations = block_on(super::prepare_delete(&adapter, &current)).unwrap();
        block_on(adapter.apply_committed(CommittedMutationBatch {
            shard_id: 7,
            log_index: 2,
            txn_id: 2,
            mutations,
        }))
        .unwrap();

        let stale = DeleteAnalyticsArtifactGenerationV1::new_with_gc_epoch(
            first.job_id,
            first.kind,
            first.generation,
            6,
        )
        .unwrap();
        assert!(matches!(
            block_on(super::prepare_delete(&adapter, &stale)),
            Err(ShardRuntimeError::AnalyticsArtifactFence)
        ));
    }

    #[test]
    fn pinned_latest_generation_can_only_be_reclaimed_after_epoch_fenced_advance() {
        let first = PutAnalyticsArtifactChunkV1::new(
            613,
            AnalyticsArtifactKindV1::Result,
            1,
            1_725_000_000_123,
            0,
            [0; 32],
            b"pinned-result".to_vec(),
        )
        .unwrap();
        let adapter = MemoryAdapter::new();
        persist_artifact(
            &adapter,
            first.job_id,
            first.kind,
            ArtifactFence {
                generation: 1,
                sealed: false,
            },
            ArtifactHead {
                count: 1,
                created_at_unix_ms: first.created_at_unix_ms,
                last_digest: first.payload_digest,
            },
            &[(0, &first)],
        );
        let pin = PinAnalyticsArtifactGenerationV1::new(
            first.job_id,
            first.kind,
            first.generation,
            1,
            u64::try_from(first.payload.len()).unwrap(),
            *blake3::hash(&first.payload).as_bytes(),
        )
        .unwrap();
        let pin_mutations = block_on(prepare_pin(&adapter, &pin)).unwrap();
        block_on(adapter.apply_committed(CommittedMutationBatch {
            shard_id: 7,
            log_index: 2,
            txn_id: 2,
            mutations: pin_mutations,
        }))
        .unwrap();

        let advance = AdvanceAnalyticsArtifactFenceV1::new(first.job_id, first.kind, 2, 7).unwrap();
        let advance_mutations = block_on(prepare_advance_fence(&adapter, &advance)).unwrap();
        block_on(adapter.apply_committed(CommittedMutationBatch {
            shard_id: 7,
            log_index: 3,
            txn_id: 3,
            mutations: advance_mutations,
        }))
        .unwrap();
        let stale = AdvanceAnalyticsArtifactFenceV1::new(first.job_id, first.kind, 3, 6).unwrap();
        assert!(matches!(
            block_on(prepare_advance_fence(&adapter, &stale)),
            Err(ShardRuntimeError::AnalyticsArtifactFence)
        ));
        let delete = DeleteAnalyticsArtifactGenerationV1::new_with_gc_epoch(
            first.job_id,
            first.kind,
            first.generation,
            7,
        )
        .unwrap();
        assert!(block_on(prepare_delete(&adapter, &delete)).is_ok());
    }

    #[test]
    fn delete_validates_a_large_generation_in_bounded_reads() {
        const CHUNK_COUNT: usize = 4096;
        const PAYLOAD_BYTES: usize = 6 * 1024;
        let (commands, head) = generation_commands(
            610,
            AnalyticsArtifactKindV1::Result,
            CHUNK_COUNT,
            PAYLOAD_BYTES,
            None,
        );
        let adapter = TrackingAdapter::new();
        let chunks = commands
            .iter()
            .enumerate()
            .map(|(ordinal, command)| (u64::try_from(ordinal).unwrap(), command))
            .collect::<Vec<_>>();
        persist_artifact(
            &adapter.inner,
            610,
            AnalyticsArtifactKindV1::Result,
            ArtifactFence {
                generation: 1,
                sealed: false,
            },
            head,
            &chunks,
        );
        let delete =
            DeleteAnalyticsArtifactGenerationV1::new(610, AnalyticsArtifactKindV1::Result, 1)
                .unwrap();

        let mutations = block_on(super::prepare_delete(&adapter, &delete)).unwrap();
        assert_eq!(mutations.len(), CHUNK_COUNT + 3);
        assert!(adapter.max_keys.load(Ordering::Relaxed) <= 8);
        assert!(
            adapter.max_bytes.load(Ordering::Relaxed)
                <= 8 * (PAYLOAD_BYTES + super::CHUNK_FIXED_BYTES)
        );
    }

    #[test]
    fn delete_detects_a_final_batch_break_without_unbounded_reads() {
        let (commands, head) =
            generation_commands(611, AnalyticsArtifactKindV1::Result, 9, 32, Some(8));
        let adapter = TrackingAdapter::new();
        let chunks = commands
            .iter()
            .enumerate()
            .map(|(ordinal, command)| (u64::try_from(ordinal).unwrap(), command))
            .collect::<Vec<_>>();
        persist_artifact(
            &adapter.inner,
            611,
            AnalyticsArtifactKindV1::Result,
            ArtifactFence {
                generation: 1,
                sealed: false,
            },
            head,
            &chunks,
        );
        let delete =
            DeleteAnalyticsArtifactGenerationV1::new(611, AnalyticsArtifactKindV1::Result, 1)
                .unwrap();

        assert!(matches!(
            block_on(super::prepare_delete(&adapter, &delete)),
            Err(ShardRuntimeError::CorruptAnalyticsArtifact { record: "chain" })
        ));
        assert!(adapter.max_keys.load(Ordering::Relaxed) <= 8);
        assert_eq!(adapter.inner.applied_log_index().unwrap(), 1);
    }

    #[test]
    fn pin_fails_closed_for_missing_chain_payload_and_manifest_corruption() {
        let (commands, head) =
            generation_commands(612, AnalyticsArtifactKindV1::Result, 2, 4, None);
        let total_bytes = 8;
        let expected_digest = content_digest(&commands);
        let pin = PinAnalyticsArtifactGenerationV1::new(
            612,
            AnalyticsArtifactKindV1::Result,
            1,
            2,
            total_bytes,
            expected_digest,
        )
        .unwrap();

        let adapter = MemoryAdapter::new();
        persist_artifact(
            &adapter,
            612,
            AnalyticsArtifactKindV1::Result,
            ArtifactFence {
                generation: 1,
                sealed: false,
            },
            head,
            &[(0, &commands[0])],
        );
        assert!(matches!(
            block_on(prepare_pin(&adapter, &pin)),
            Err(ShardRuntimeError::CorruptAnalyticsArtifact {
                record: "missing-chunk"
            })
        ));
        assert_unpinned(&adapter, &pin, 1);

        let (broken, broken_head) =
            generation_commands(613, AnalyticsArtifactKindV1::Result, 2, 4, Some(1));
        let broken_pin = PinAnalyticsArtifactGenerationV1::new(
            613,
            AnalyticsArtifactKindV1::Result,
            1,
            2,
            total_bytes,
            content_digest(&broken),
        )
        .unwrap();
        let adapter = MemoryAdapter::new();
        persist_artifact(
            &adapter,
            613,
            AnalyticsArtifactKindV1::Result,
            ArtifactFence {
                generation: 1,
                sealed: false,
            },
            broken_head,
            &[(0, &broken[0]), (1, &broken[1])],
        );
        assert!(matches!(
            block_on(prepare_pin(&adapter, &broken_pin)),
            Err(ShardRuntimeError::CorruptAnalyticsArtifact { record: "chain" })
        ));
        assert_unpinned(&adapter, &broken_pin, 1);

        let adapter = MemoryAdapter::new();
        persist_artifact(
            &adapter,
            612,
            AnalyticsArtifactKindV1::Result,
            ArtifactFence {
                generation: 1,
                sealed: false,
            },
            head,
            &[(0, &commands[0]), (1, &commands[1])],
        );
        let mut corrupt_payload = encode_chunk(&commands[1]);
        corrupt_payload[super::CHUNK_FIXED_BYTES - 4] ^= 1;
        corrupt_payload.truncate(corrupt_payload.len() - 4);
        append_checksum(&mut corrupt_payload);
        block_on(adapter.apply_committed(CommittedMutationBatch {
            shard_id: 7,
            log_index: 2,
            txn_id: 2,
            mutations: vec![Mutation::put(
                0,
                analytics_artifact_chunk_key(612, AnalyticsArtifactKindV1::Result, 1, 1),
                corrupt_payload,
            )],
        }))
        .unwrap();
        assert!(matches!(
            block_on(prepare_pin(&adapter, &pin)),
            Err(ShardRuntimeError::CorruptAnalyticsArtifact { record: "chunk" })
        ));
        assert_unpinned(&adapter, &pin, 2);

        let adapter = MemoryAdapter::new();
        persist_artifact(
            &adapter,
            612,
            AnalyticsArtifactKindV1::Result,
            ArtifactFence {
                generation: 1,
                sealed: false,
            },
            head,
            &[(0, &commands[0]), (1, &commands[1])],
        );
        for mismatched in [
            PinAnalyticsArtifactGenerationV1::new(
                612,
                AnalyticsArtifactKindV1::Result,
                1,
                1,
                4,
                *blake3::hash(&commands[0].payload).as_bytes(),
            )
            .unwrap(),
            PinAnalyticsArtifactGenerationV1::new(
                612,
                AnalyticsArtifactKindV1::Result,
                1,
                2,
                total_bytes + 1,
                expected_digest,
            )
            .unwrap(),
            PinAnalyticsArtifactGenerationV1::new(
                612,
                AnalyticsArtifactKindV1::Result,
                1,
                2,
                total_bytes,
                [0x7f; 32],
            )
            .unwrap(),
        ] {
            assert!(matches!(
                block_on(prepare_pin(&adapter, &mismatched)),
                Err(ShardRuntimeError::AnalyticsArtifactConflict)
            ));
            assert_unpinned(&adapter, &pin, 1);
        }
    }

    #[test]
    fn pin_validates_4096_chunks_in_batches_of_at_most_eight() {
        const CHUNK_COUNT: usize = 4096;
        let (commands, head) = generation_commands(
            614,
            AnalyticsArtifactKindV1::Checkpoint,
            CHUNK_COUNT,
            1,
            None,
        );
        let adapter = TrackingAdapter::new();
        let chunks = commands
            .iter()
            .enumerate()
            .map(|(ordinal, command)| (u64::try_from(ordinal).unwrap(), command))
            .collect::<Vec<_>>();
        persist_artifact(
            &adapter.inner,
            614,
            AnalyticsArtifactKindV1::Checkpoint,
            ArtifactFence {
                generation: 1,
                sealed: false,
            },
            head,
            &chunks,
        );
        let pin = PinAnalyticsArtifactGenerationV1::new(
            614,
            AnalyticsArtifactKindV1::Checkpoint,
            1,
            4096,
            4096,
            content_digest(&commands),
        )
        .unwrap();

        assert_eq!(block_on(prepare_pin(&adapter, &pin)).unwrap().len(), 1);
        assert!(adapter.max_keys.load(Ordering::Relaxed) <= 8);
    }

    fn content_digest(commands: &[PutAnalyticsArtifactChunkV1]) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        for command in commands {
            hasher.update(&command.payload);
        }
        *hasher.finalize().as_bytes()
    }

    fn assert_unpinned(
        adapter: &MemoryAdapter,
        pin: &PinAnalyticsArtifactGenerationV1,
        expected_log_index: u64,
    ) {
        assert_eq!(adapter.applied_log_index().unwrap(), expected_log_index);
        assert!(
            block_on(adapter.multi_get(&[analytics_artifact_generation_pin_key(
                pin.job_id,
                pin.kind,
                pin.generation,
            )]))
            .unwrap()[0]
                .is_none()
        );
    }

    fn generation_commands(
        job_id: u128,
        kind: AnalyticsArtifactKindV1,
        count: usize,
        payload_bytes: usize,
        broken_ordinal: Option<usize>,
    ) -> (Vec<PutAnalyticsArtifactChunkV1>, ArtifactHead) {
        let mut previous_digest = [0; 32];
        let mut commands = Vec::with_capacity(count);
        for ordinal in 0..count {
            let previous = if broken_ordinal == Some(ordinal) {
                [0x7f; 32]
            } else {
                previous_digest
            };
            let command = PutAnalyticsArtifactChunkV1::new(
                job_id,
                kind,
                1,
                1_725_000_000_123,
                u64::try_from(ordinal).unwrap(),
                previous,
                vec![u8::try_from(ordinal).unwrap_or(u8::MAX); payload_bytes],
            )
            .unwrap();
            previous_digest = command.payload_digest;
            commands.push(command);
        }
        (
            commands,
            ArtifactHead {
                count: u16::try_from(count).unwrap(),
                created_at_unix_ms: 1_725_000_000_123,
                last_digest: previous_digest,
            },
        )
    }

    struct TrackingAdapter {
        inner: MemoryAdapter,
        max_keys: AtomicUsize,
        max_bytes: AtomicUsize,
    }

    impl TrackingAdapter {
        fn new() -> Self {
            Self {
                inner: MemoryAdapter::new(),
                max_keys: AtomicUsize::new(0),
                max_bytes: AtomicUsize::new(0),
            }
        }
    }

    impl StorageAdapter for TrackingAdapter {
        fn capabilities(&self) -> AdapterCapabilities {
            self.inner.capabilities()
        }

        fn apply_committed<'a>(
            &'a self,
            batch: CommittedMutationBatch,
        ) -> AdapterFuture<'a, ApplyReceipt> {
            self.inner.apply_committed(batch)
        }

        fn multi_get<'a>(
            &'a self,
            keys: &'a [LogicalKey],
        ) -> AdapterFuture<'a, Vec<Option<Vec<u8>>>> {
            Box::pin(async move {
                self.max_keys.fetch_max(keys.len(), Ordering::Relaxed);
                let values = self.inner.multi_get(keys).await?;
                let bytes = values.iter().filter_map(Option::as_ref).map(Vec::len).sum();
                self.max_bytes.fetch_max(bytes, Ordering::Relaxed);
                Ok(values)
            })
        }

        fn scan<'a>(&'a self, span: &'a KeySpan) -> AdapterFuture<'a, Vec<KeyValue>> {
            self.inner.scan(span)
        }

        fn applied_log_index(&self) -> Result<u64, storage_api::AdapterError> {
            self.inner.applied_log_index()
        }
    }

    #[test]
    fn put_allows_chunk_4096_and_rejects_chunk_4097() {
        let tail = PutAnalyticsArtifactChunkV1::new(
            605,
            AnalyticsArtifactKindV1::Result,
            1,
            1_725_000_000_123,
            4094,
            [3; 32],
            b"tail".to_vec(),
        )
        .unwrap();
        let chunk_4096 = PutAnalyticsArtifactChunkV1::new(
            605,
            AnalyticsArtifactKindV1::Result,
            1,
            1_725_000_000_123,
            4095,
            tail.payload_digest,
            b"chunk-4096".to_vec(),
        )
        .unwrap();
        let adapter = MemoryAdapter::new();
        persist_artifact(
            &adapter,
            tail.job_id,
            tail.kind,
            ArtifactFence {
                generation: 1,
                sealed: false,
            },
            ArtifactHead {
                count: 4095,
                created_at_unix_ms: tail.created_at_unix_ms,
                last_digest: tail.payload_digest,
            },
            &[(4094, &tail)],
        );
        assert_eq!(
            block_on(prepare_put(&adapter, &chunk_4096)).unwrap().len(),
            2
        );

        let tail = PutAnalyticsArtifactChunkV1::new(
            606,
            AnalyticsArtifactKindV1::Result,
            1,
            1_725_000_000_123,
            4095,
            [3; 32],
            b"limit-tail".to_vec(),
        )
        .unwrap();
        let chunk_4097 = PutAnalyticsArtifactChunkV1::new(
            606,
            AnalyticsArtifactKindV1::Result,
            1,
            1_725_000_000_123,
            4096,
            tail.payload_digest,
            b"chunk-4097".to_vec(),
        )
        .unwrap();
        let adapter = MemoryAdapter::new();
        persist_artifact(
            &adapter,
            tail.job_id,
            tail.kind,
            ArtifactFence {
                generation: 1,
                sealed: false,
            },
            ArtifactHead {
                count: 4096,
                created_at_unix_ms: tail.created_at_unix_ms,
                last_digest: tail.payload_digest,
            },
            &[(4095, &tail)],
        );
        assert!(matches!(
            block_on(prepare_put(&adapter, &chunk_4097)),
            Err(ShardRuntimeError::AnalyticsArtifactLimit)
        ));
    }

    fn persist_artifact(
        adapter: &MemoryAdapter,
        job_id: u128,
        kind: AnalyticsArtifactKindV1,
        fence: ArtifactFence,
        head: ArtifactHead,
        chunks: &[(u64, &PutAnalyticsArtifactChunkV1)],
    ) {
        let mut mutations = vec![
            Mutation::put(
                0,
                analytics_artifact_fence_key(job_id, kind),
                encode_fence(fence),
            ),
            Mutation::put(
                1,
                analytics_artifact_head_key(job_id, kind, fence.generation),
                encode_head(head),
            ),
        ];
        for (ordinal, chunk) in chunks {
            mutations.push(Mutation::put(
                u32::try_from(mutations.len()).unwrap(),
                analytics_artifact_chunk_key(job_id, kind, fence.generation, *ordinal),
                encode_chunk(chunk),
            ));
        }
        block_on(adapter.apply_committed(CommittedMutationBatch {
            shard_id: 7,
            log_index: 1,
            txn_id: 1,
            mutations,
        }))
        .unwrap();
    }

    fn block_on<F: Future>(future: F) -> F::Output {
        struct ThreadWaker(std::thread::Thread);

        impl Wake for ThreadWaker {
            fn wake(self: Arc<Self>) {
                self.0.unpark();
            }

            fn wake_by_ref(self: &Arc<Self>) {
                self.0.unpark();
            }
        }

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
}
