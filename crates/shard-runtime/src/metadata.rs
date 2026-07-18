use std::cmp::min;
use std::collections::BTreeMap;

use storage_api::{KeySpan, Keyspace, LogicalKey, StorageAdapter};
use temporal_types::TransactionTime;

use crate::ShardRuntimeError;

const META_PREFIX: &[u8] = b"\x01dtg/replica/v1/";
const POSITION_KEY: &[u8] = b"\x01dtg/replica/v1/position";
const CLOSED_TS_KEY: &[u8] = b"\x01dtg/replica/v1/closed-ts";
const RESOLVED_TS_KEY: &[u8] = b"\x01dtg/replica/v1/resolved-ts";
const ADAPTER_APPLIED_TS_KEY: &[u8] = b"\x01dtg/replica/v1/adapter-applied-ts";
const BACKEND_STATE_KEY: &[u8] = b"\x01dtg/replica/v1/backend-state";
const ENTRY_DIGEST_PREFIX: &[u8] = b"\x01dtg/replica/v1/entry/";
const REQUEST_DIGEST_PREFIX: &[u8] = b"\x01dtg/replica/v1/request/";
const UNRESOLVED_INTENT_PREFIX: &[u8] = b"\x01dtg/replica/v1/unresolved/";
const META_VERSION: u16 = 1;
const POSITION_MAGIC: [u8; 4] = *b"DTRP";
const TIMESTAMP_MAGIC: [u8; 4] = *b"DTTM";
const ENTRY_DIGEST_MAGIC: [u8; 4] = *b"DTRE";
const REQUEST_DIGEST_MAGIC: [u8; 4] = *b"DTRQ";
const UNRESOLVED_INTENT_MAGIC: [u8; 4] = *b"DTRU";
const BACKEND_STATE_MAGIC: [u8; 4] = *b"DTBG";
const POSITION_VALUE_BYTES: usize = 38;
const TIMESTAMP_VALUE_BYTES: usize = 22;
const ENTRY_DIGEST_VALUE_BYTES: usize = 50;
const REQUEST_DIGEST_VALUE_BYTES: usize = 42;
const BACKEND_STATE_VALUE_BYTES: usize = 67;

pub const MIN_REPLICA_TIME: TransactionTime = TransactionTime::new(i64::MIN, 0);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackendLifecycle {
    Active,
    DualApplying {
        target_generation: u64,
        target_profile_digest: [u8; 32],
        fence_index: u64,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReplicaMetadata {
    pub shard_id: u32,
    pub placement_epoch: u64,
    pub last_term: u64,
    pub applied_index: u64,
    pub closed_ts: TransactionTime,
    pub resolved_ts: TransactionTime,
    pub adapter_applied_ts: TransactionTime,
    pub backend_generation: u64,
    pub backend_lifecycle: BackendLifecycle,
}

impl ReplicaMetadata {
    pub(crate) const fn initial(
        shard_id: u32,
        placement_epoch: u64,
        backend_generation: u64,
    ) -> Self {
        Self {
            shard_id,
            placement_epoch,
            last_term: 0,
            applied_index: 0,
            closed_ts: MIN_REPLICA_TIME,
            resolved_ts: MIN_REPLICA_TIME,
            adapter_applied_ts: MIN_REPLICA_TIME,
            backend_generation,
            backend_lifecycle: BackendLifecycle::Active,
        }
    }

    #[must_use]
    pub fn safe_ts(&self) -> TransactionTime {
        min(
            self.closed_ts,
            min(self.resolved_ts, self.adapter_applied_ts),
        )
    }

    pub(crate) fn after_apply(self, term: u64, index: u64, commit_ts: TransactionTime) -> Self {
        Self {
            last_term: term,
            applied_index: index,
            adapter_applied_ts: commit_ts,
            ..self
        }
    }
}

pub(crate) async fn load_metadata<A: StorageAdapter>(
    adapter: &A,
    shard_id: u32,
    placement_epoch: u64,
    initial_backend_generation: u64,
) -> Result<ReplicaMetadata, ShardRuntimeError> {
    if initial_backend_generation == 0 {
        return Err(ShardRuntimeError::InvalidBackendGeneration {
            generation: initial_backend_generation,
        });
    }
    let keys = [
        position_key(),
        closed_ts_key(),
        resolved_ts_key(),
        adapter_applied_ts_key(),
        backend_state_key(),
    ];
    let values = adapter.multi_get(&keys).await?;
    let adapter_index = adapter.applied_log_index()?;
    let mut values = values.into_iter();
    let position = values.next().flatten();
    let closed = values.next().flatten();
    let resolved = values.next().flatten();
    let adapter_applied = values.next().flatten();
    let backend_state = values.next().flatten();
    if position.is_none() {
        if closed.is_some()
            || resolved.is_some()
            || adapter_applied.is_some()
            || backend_state.is_some()
        {
            return Err(ShardRuntimeError::CorruptMetadata { record: "position" });
        }
        if adapter_index != 0 {
            return Err(ShardRuntimeError::MetadataIndexMismatch {
                metadata: 0,
                adapter: adapter_index,
            });
        }
        return Ok(ReplicaMetadata::initial(
            shard_id,
            placement_epoch,
            initial_backend_generation,
        ));
    }

    let (stored_shard, stored_epoch, last_term, applied_index) =
        decode_position(position.as_deref().expect("position checked as present"))?;
    if stored_shard != shard_id {
        return Err(ShardRuntimeError::ShardMismatch {
            expected: shard_id,
            actual: stored_shard,
        });
    }
    if stored_epoch != placement_epoch {
        return Err(ShardRuntimeError::StaleEpoch {
            expected: stored_epoch,
            actual: placement_epoch,
        });
    }
    if applied_index != adapter_index {
        return Err(ShardRuntimeError::MetadataIndexMismatch {
            metadata: applied_index,
            adapter: adapter_index,
        });
    }
    let (backend_generation, backend_lifecycle) = backend_state.map_or(
        Ok((initial_backend_generation, BackendLifecycle::Active)),
        |bytes| decode_backend_state(&bytes),
    )?;
    Ok(ReplicaMetadata {
        shard_id,
        placement_epoch,
        last_term,
        applied_index,
        closed_ts: decode_optional_timestamp(closed, "closed-ts")?,
        resolved_ts: decode_optional_timestamp(resolved, "resolved-ts")?,
        adapter_applied_ts: decode_optional_timestamp(adapter_applied, "adapter-applied-ts")?,
        backend_generation,
        backend_lifecycle,
    })
}

pub(crate) async fn load_unresolved_intents<A: StorageAdapter>(
    adapter: &A,
) -> Result<BTreeMap<u128, TransactionTime>, ShardRuntimeError> {
    let entries = adapter
        .scan(&KeySpan::prefix(
            Keyspace::Meta,
            UNRESOLVED_INTENT_PREFIX.to_vec(),
        ))
        .await?;
    let mut unresolved = BTreeMap::new();
    for entry in entries {
        let key = entry.key().as_bytes();
        if entry.key().keyspace() != Keyspace::Meta
            || key.len() != UNRESOLVED_INTENT_PREFIX.len() + 16
            || !key.starts_with(UNRESOLVED_INTENT_PREFIX)
        {
            return Err(ShardRuntimeError::CorruptMetadata {
                record: "unresolved-intent-key",
            });
        }
        let transaction_id = u128::from_be_bytes(
            key[UNRESOLVED_INTENT_PREFIX.len()..]
                .try_into()
                .expect("validated unresolved intent key length"),
        );
        if transaction_id == 0
            || unresolved
                .insert(transaction_id, decode_unresolved_intent(entry.value())?)
                .is_some()
        {
            return Err(ShardRuntimeError::CorruptMetadata {
                record: "unresolved-intent-key",
            });
        }
    }
    Ok(unresolved)
}

fn decode_optional_timestamp(
    bytes: Option<Vec<u8>>,
    record: &'static str,
) -> Result<TransactionTime, ShardRuntimeError> {
    bytes
        .map(|bytes| decode_timestamp(&bytes, record))
        .transpose()
        .map(|timestamp| timestamp.unwrap_or(MIN_REPLICA_TIME))
}

pub(crate) fn is_reserved_metadata_key(key: &LogicalKey) -> bool {
    key.keyspace() == Keyspace::Meta && key.as_bytes().starts_with(META_PREFIX)
}

pub(crate) fn position_key() -> LogicalKey {
    meta_key(POSITION_KEY.to_vec())
}

pub(crate) fn closed_ts_key() -> LogicalKey {
    meta_key(CLOSED_TS_KEY.to_vec())
}

pub(crate) fn resolved_ts_key() -> LogicalKey {
    meta_key(RESOLVED_TS_KEY.to_vec())
}

pub(crate) fn adapter_applied_ts_key() -> LogicalKey {
    meta_key(ADAPTER_APPLIED_TS_KEY.to_vec())
}

pub(crate) fn backend_state_key() -> LogicalKey {
    meta_key(BACKEND_STATE_KEY.to_vec())
}

pub(crate) fn entry_digest_key(index: u64) -> LogicalKey {
    let mut key = Vec::with_capacity(ENTRY_DIGEST_PREFIX.len() + 8);
    key.extend_from_slice(ENTRY_DIGEST_PREFIX);
    key.extend_from_slice(&index.to_be_bytes());
    meta_key(key)
}

pub(crate) fn request_digest_key(request_id: u128) -> LogicalKey {
    let mut key = Vec::with_capacity(REQUEST_DIGEST_PREFIX.len() + 16);
    key.extend_from_slice(REQUEST_DIGEST_PREFIX);
    key.extend_from_slice(&request_id.to_be_bytes());
    meta_key(key)
}

pub(crate) fn unresolved_intent_key(transaction_id: u128) -> LogicalKey {
    let mut key = Vec::with_capacity(UNRESOLVED_INTENT_PREFIX.len() + 16);
    key.extend_from_slice(UNRESOLVED_INTENT_PREFIX);
    key.extend_from_slice(&transaction_id.to_be_bytes());
    meta_key(key)
}

pub(crate) fn encode_unresolved_intent(start_ts: TransactionTime) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(TIMESTAMP_VALUE_BYTES);
    bytes.extend_from_slice(&UNRESOLVED_INTENT_MAGIC);
    bytes.extend_from_slice(&META_VERSION.to_be_bytes());
    bytes.extend_from_slice(&start_ts.physical_micros().to_be_bytes());
    bytes.extend_from_slice(&start_ts.logical().to_be_bytes());
    append_checksum(&mut bytes);
    bytes
}

fn decode_unresolved_intent(bytes: &[u8]) -> Result<TransactionTime, ShardRuntimeError> {
    validate_record(
        bytes,
        TIMESTAMP_VALUE_BYTES,
        UNRESOLVED_INTENT_MAGIC,
        "unresolved-intent",
    )?;
    Ok(TransactionTime::new(
        i64::from_be_bytes(bytes[6..14].try_into().expect("fixed timestamp slice")),
        u32::from_be_bytes(bytes[14..18].try_into().expect("fixed timestamp slice")),
    ))
}

fn meta_key(bytes: Vec<u8>) -> LogicalKey {
    LogicalKey::in_keyspace(Keyspace::Meta, bytes)
}

pub(crate) fn encode_position(metadata: ReplicaMetadata) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(POSITION_VALUE_BYTES);
    bytes.extend_from_slice(&POSITION_MAGIC);
    bytes.extend_from_slice(&META_VERSION.to_be_bytes());
    bytes.extend_from_slice(&metadata.shard_id.to_be_bytes());
    bytes.extend_from_slice(&metadata.placement_epoch.to_be_bytes());
    bytes.extend_from_slice(&metadata.last_term.to_be_bytes());
    bytes.extend_from_slice(&metadata.applied_index.to_be_bytes());
    append_checksum(&mut bytes);
    bytes
}

fn decode_position(bytes: &[u8]) -> Result<(u32, u64, u64, u64), ShardRuntimeError> {
    validate_record(bytes, POSITION_VALUE_BYTES, POSITION_MAGIC, "position")?;
    Ok((
        u32::from_be_bytes(bytes[6..10].try_into().expect("fixed position slice")),
        u64::from_be_bytes(bytes[10..18].try_into().expect("fixed position slice")),
        u64::from_be_bytes(bytes[18..26].try_into().expect("fixed position slice")),
        u64::from_be_bytes(bytes[26..34].try_into().expect("fixed position slice")),
    ))
}

pub(crate) fn encode_timestamp(timestamp: TransactionTime) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(TIMESTAMP_VALUE_BYTES);
    bytes.extend_from_slice(&TIMESTAMP_MAGIC);
    bytes.extend_from_slice(&META_VERSION.to_be_bytes());
    bytes.extend_from_slice(&timestamp.physical_micros().to_be_bytes());
    bytes.extend_from_slice(&timestamp.logical().to_be_bytes());
    append_checksum(&mut bytes);
    bytes
}

pub(crate) fn encode_backend_state(metadata: ReplicaMetadata) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(BACKEND_STATE_VALUE_BYTES);
    bytes.extend_from_slice(&BACKEND_STATE_MAGIC);
    bytes.extend_from_slice(&META_VERSION.to_be_bytes());
    bytes.extend_from_slice(&metadata.backend_generation.to_be_bytes());
    match metadata.backend_lifecycle {
        BackendLifecycle::Active => {
            bytes.push(1);
            bytes.extend_from_slice(&0_u64.to_be_bytes());
            bytes.extend_from_slice(&[0; 32]);
            bytes.extend_from_slice(&0_u64.to_be_bytes());
        }
        BackendLifecycle::DualApplying {
            target_generation,
            target_profile_digest,
            fence_index,
        } => {
            bytes.push(2);
            bytes.extend_from_slice(&target_generation.to_be_bytes());
            bytes.extend_from_slice(&target_profile_digest);
            bytes.extend_from_slice(&fence_index.to_be_bytes());
        }
    }
    append_checksum(&mut bytes);
    bytes
}

fn decode_backend_state(bytes: &[u8]) -> Result<(u64, BackendLifecycle), ShardRuntimeError> {
    validate_record(
        bytes,
        BACKEND_STATE_VALUE_BYTES,
        BACKEND_STATE_MAGIC,
        "backend-state",
    )?;
    let generation = u64::from_be_bytes(
        bytes[6..14]
            .try_into()
            .expect("fixed backend generation slice"),
    );
    let target_generation = u64::from_be_bytes(
        bytes[15..23]
            .try_into()
            .expect("fixed target generation slice"),
    );
    let target_profile_digest = bytes[23..55]
        .try_into()
        .expect("fixed profile digest slice");
    let fence_index =
        u64::from_be_bytes(bytes[55..63].try_into().expect("fixed backend fence slice"));
    if generation == 0 {
        return Err(ShardRuntimeError::InvalidBackendGeneration { generation });
    }
    let lifecycle = match bytes[14] {
        1 if target_generation == 0 && target_profile_digest == [0; 32] && fence_index == 0 => {
            BackendLifecycle::Active
        }
        2 if target_generation == generation.checked_add(1).unwrap_or(0)
            && target_profile_digest != [0; 32] =>
        {
            BackendLifecycle::DualApplying {
                target_generation,
                target_profile_digest,
                fence_index,
            }
        }
        _ => {
            return Err(ShardRuntimeError::CorruptMetadata {
                record: "backend-state",
            });
        }
    };
    Ok((generation, lifecycle))
}

fn decode_timestamp(
    bytes: &[u8],
    record: &'static str,
) -> Result<TransactionTime, ShardRuntimeError> {
    validate_record(bytes, TIMESTAMP_VALUE_BYTES, TIMESTAMP_MAGIC, record)?;
    Ok(TransactionTime::new(
        i64::from_be_bytes(bytes[6..14].try_into().expect("fixed timestamp slice")),
        u32::from_be_bytes(bytes[14..18].try_into().expect("fixed timestamp slice")),
    ))
}

pub(crate) fn encode_entry_digest(term: u64, digest: [u8; 32]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(ENTRY_DIGEST_VALUE_BYTES);
    bytes.extend_from_slice(&ENTRY_DIGEST_MAGIC);
    bytes.extend_from_slice(&META_VERSION.to_be_bytes());
    bytes.extend_from_slice(&term.to_be_bytes());
    bytes.extend_from_slice(&digest);
    append_checksum(&mut bytes);
    bytes
}

pub(crate) fn decode_entry_digest(bytes: &[u8]) -> Result<(u64, [u8; 32]), ShardRuntimeError> {
    validate_record(
        bytes,
        ENTRY_DIGEST_VALUE_BYTES,
        ENTRY_DIGEST_MAGIC,
        "entry-digest",
    )?;
    Ok((
        u64::from_be_bytes(bytes[6..14].try_into().expect("fixed entry digest slice")),
        bytes[14..46]
            .try_into()
            .expect("fixed entry digest hash slice"),
    ))
}

pub(crate) fn encode_request_digest(digest: [u8; 32]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(REQUEST_DIGEST_VALUE_BYTES);
    bytes.extend_from_slice(&REQUEST_DIGEST_MAGIC);
    bytes.extend_from_slice(&META_VERSION.to_be_bytes());
    bytes.extend_from_slice(&digest);
    append_checksum(&mut bytes);
    bytes
}

pub(crate) fn decode_request_digest(bytes: &[u8]) -> Result<[u8; 32], ShardRuntimeError> {
    validate_record(
        bytes,
        REQUEST_DIGEST_VALUE_BYTES,
        REQUEST_DIGEST_MAGIC,
        "request-digest",
    )?;
    Ok(bytes[6..38]
        .try_into()
        .expect("fixed request digest hash slice"))
}

fn append_checksum(bytes: &mut Vec<u8>) {
    let checksum = crc32fast::hash(bytes);
    bytes.extend_from_slice(&checksum.to_be_bytes());
}

fn validate_record(
    bytes: &[u8],
    expected_length: usize,
    magic: [u8; 4],
    record: &'static str,
) -> Result<(), ShardRuntimeError> {
    if bytes.len() != expected_length || bytes[..4] != magic {
        return Err(ShardRuntimeError::CorruptMetadata { record });
    }
    if u16::from_be_bytes(
        bytes[4..6]
            .try_into()
            .expect("fixed metadata version slice"),
    ) != META_VERSION
    {
        return Err(ShardRuntimeError::CorruptMetadata { record });
    }
    let checksum_offset = bytes.len() - 4;
    let stored = u32::from_be_bytes(
        bytes[checksum_offset..]
            .try_into()
            .expect("fixed metadata checksum slice"),
    );
    if crc32fast::hash(&bytes[..checksum_offset]) != stored {
        return Err(ShardRuntimeError::CorruptMetadata { record });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        BackendLifecycle, ENTRY_DIGEST_VALUE_BYTES, ReplicaMetadata, decode_backend_state,
        decode_entry_digest, decode_position, decode_request_digest, decode_timestamp,
        encode_backend_state, encode_entry_digest, encode_position, encode_request_digest,
        encode_timestamp,
    };
    use crate::ShardRuntimeError;
    use temporal_types::TransactionTime;

    #[test]
    fn replica_metadata_codecs_round_trip_and_reject_corruption() {
        let metadata = ReplicaMetadata {
            shard_id: 7,
            placement_epoch: 9,
            last_term: 11,
            applied_index: 13,
            closed_ts: TransactionTime::new(17, 1),
            resolved_ts: TransactionTime::new(17, 1),
            adapter_applied_ts: TransactionTime::new(19, 2),
            backend_generation: 4,
            backend_lifecycle: BackendLifecycle::DualApplying {
                target_generation: 5,
                target_profile_digest: [0x44; 32],
                fence_index: 12,
            },
        };
        assert_eq!(
            decode_position(&encode_position(metadata)).unwrap(),
            (7, 9, 11, 13)
        );
        assert_eq!(
            decode_timestamp(&encode_timestamp(metadata.closed_ts), "test").unwrap(),
            metadata.closed_ts
        );
        let digest = [23_u8; 32];
        assert_eq!(
            decode_entry_digest(&encode_entry_digest(29, digest)).unwrap(),
            (29, digest)
        );
        assert_eq!(
            decode_request_digest(&encode_request_digest(digest)).unwrap(),
            digest
        );
        assert_eq!(
            decode_backend_state(&encode_backend_state(metadata)).unwrap(),
            (metadata.backend_generation, metadata.backend_lifecycle)
        );

        let mut corrupted = encode_entry_digest(29, digest);
        corrupted[ENTRY_DIGEST_VALUE_BYTES / 2] ^= 1;
        assert!(matches!(
            decode_entry_digest(&corrupted),
            Err(ShardRuntimeError::CorruptMetadata {
                record: "entry-digest"
            })
        ));
    }
}
