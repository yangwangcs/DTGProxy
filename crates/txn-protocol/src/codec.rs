use prost::Message;
use storage_api::{Keyspace, LogicalKey, Mutation, MutationOperation, PreparedMutationBatch};
use temporal_types::TransactionTime;

use crate::model::{
    CommittedWrite, ConstraintClaim, HomeTransactionRecord, IntentLock, IsolationLevel,
    ParticipantProof, ParticipantRecord, PointReadVersion, PrewriteMetadata, PrewriteRequest,
    RangeReadFingerprint, ShardEpoch, TransactionId, TransactionState, TxnProtocolError,
};

const PREWRITE_MAGIC: [u8; 4] = *b"DTPW";
const HOME_MAGIC: [u8; 4] = *b"DTHR";
const PARTICIPANT_MAGIC: [u8; 4] = *b"DTPI";
const LOCK_MAGIC: [u8; 4] = *b"DTLK";
const WRITE_MAGIC: [u8; 4] = *b"DTWR";
const RECORD_VERSION: u16 = 1;
const HEADER_BYTES: usize = 10;
const CHECKSUM_BYTES: usize = 4;
const MAX_RECORD_BYTES: usize = 8 * 1024 * 1024;

pub(crate) fn encode_prewrite_record(
    request: &PrewriteRequest,
) -> Result<Vec<u8>, TxnProtocolError> {
    encode_record(PREWRITE_MAGIC, &encode_prewrite(request))
}

pub(crate) fn decode_prewrite_record(bytes: &[u8]) -> Result<PrewriteRequest, TxnProtocolError> {
    let payload = decode_record(PREWRITE_MAGIC, bytes)?;
    let wire = wire::PrewriteRequest::decode(payload)
        .map_err(|error| TxnProtocolError::PayloadDecode(error.to_string()))?;
    let request = decode_prewrite(wire)?;
    if encode_prewrite(&request).encode_to_vec() != payload {
        return Err(TxnProtocolError::NonCanonicalRecord);
    }
    Ok(request)
}

pub(crate) fn encode_home_record(
    record: &HomeTransactionRecord,
) -> Result<Vec<u8>, TxnProtocolError> {
    encode_record(HOME_MAGIC, &encode_home(record))
}

pub(crate) fn decode_home_record(bytes: &[u8]) -> Result<HomeTransactionRecord, TxnProtocolError> {
    let payload = decode_record(HOME_MAGIC, bytes)?;
    let wire = wire::HomeRecord::decode(payload)
        .map_err(|error| TxnProtocolError::PayloadDecode(error.to_string()))?;
    let record = decode_home(wire)?;
    if encode_home(&record).encode_to_vec() != payload {
        return Err(TxnProtocolError::NonCanonicalRecord);
    }
    Ok(record)
}

pub(crate) fn encode_participant_record(
    record: &ParticipantRecord,
) -> Result<Vec<u8>, TxnProtocolError> {
    let wire = wire::ParticipantRecord {
        request: Some(encode_prewrite(&record.request)),
        proof: Some(encode_proof(&record.proof)),
        state: u32::from(record.state as u8),
        commit_ts: record.commit_ts.map(encode_time),
    };
    encode_record(PARTICIPANT_MAGIC, &wire)
}

pub(crate) fn decode_participant_record(
    bytes: &[u8],
) -> Result<ParticipantRecord, TxnProtocolError> {
    let payload = decode_record(PARTICIPANT_MAGIC, bytes)?;
    let wire = wire::ParticipantRecord::decode(payload)
        .map_err(|error| TxnProtocolError::PayloadDecode(error.to_string()))?;
    let record = ParticipantRecord {
        request: decode_prewrite(
            wire.request
                .ok_or(TxnProtocolError::MissingField("participant request"))?,
        )?,
        proof: decode_proof(
            wire.proof
                .ok_or(TxnProtocolError::MissingField("participant proof"))?,
        )?,
        state: decode_state(wire.state)?,
        commit_ts: wire.commit_ts.map(decode_time).transpose()?,
    };
    validate_participant_record(&record)?;
    let canonical = wire::ParticipantRecord {
        request: Some(encode_prewrite(&record.request)),
        proof: Some(encode_proof(&record.proof)),
        state: u32::from(record.state as u8),
        commit_ts: record.commit_ts.map(encode_time),
    }
    .encode_to_vec();
    if canonical != payload {
        return Err(TxnProtocolError::NonCanonicalRecord);
    }
    Ok(record)
}

pub(crate) fn encode_lock(lock: &IntentLock) -> Result<Vec<u8>, TxnProtocolError> {
    encode_record(
        LOCK_MAGIC,
        &wire::IntentLock {
            transaction_id: encode_id(lock.transaction_id),
            start_ts: Some(encode_time(lock.start_ts)),
            expires_at: Some(encode_time(lock.expires_at)),
            intent_digest: lock.intent_digest.to_vec(),
        },
    )
}

pub(crate) fn decode_lock(bytes: &[u8]) -> Result<IntentLock, TxnProtocolError> {
    let payload = decode_record(LOCK_MAGIC, bytes)?;
    let wire = wire::IntentLock::decode(payload)
        .map_err(|error| TxnProtocolError::PayloadDecode(error.to_string()))?;
    let lock = IntentLock {
        transaction_id: decode_id(&wire.transaction_id, "lock transaction ID")?,
        start_ts: decode_time(
            wire.start_ts
                .ok_or(TxnProtocolError::MissingField("lock start timestamp"))?,
        )?,
        expires_at: decode_time(
            wire.expires_at
                .ok_or(TxnProtocolError::MissingField("lock expiry"))?,
        )?,
        intent_digest: decode_digest(&wire.intent_digest)?,
    };
    if lock.transaction_id.value() == 0 || lock.expires_at <= lock.start_ts {
        return Err(TxnProtocolError::CorruptParticipantState);
    }
    let canonical = wire::IntentLock {
        transaction_id: encode_id(lock.transaction_id),
        start_ts: Some(encode_time(lock.start_ts)),
        expires_at: Some(encode_time(lock.expires_at)),
        intent_digest: lock.intent_digest.to_vec(),
    }
    .encode_to_vec();
    if canonical != payload {
        return Err(TxnProtocolError::NonCanonicalRecord);
    }
    Ok(lock)
}

pub(crate) fn encode_committed_write(write: &CommittedWrite) -> Result<Vec<u8>, TxnProtocolError> {
    encode_record(
        WRITE_MAGIC,
        &wire::CommittedWrite {
            transaction_id: encode_id(write.transaction_id),
            commit_ts: Some(encode_time(write.commit_ts)),
        },
    )
}

pub(crate) fn decode_committed_write(bytes: &[u8]) -> Result<CommittedWrite, TxnProtocolError> {
    let payload = decode_record(WRITE_MAGIC, bytes)?;
    let wire = wire::CommittedWrite::decode(payload)
        .map_err(|error| TxnProtocolError::PayloadDecode(error.to_string()))?;
    let write = CommittedWrite {
        transaction_id: decode_id(&wire.transaction_id, "write transaction ID")?,
        commit_ts: decode_time(
            wire.commit_ts
                .ok_or(TxnProtocolError::MissingField("write commit timestamp"))?,
        )?,
    };
    if write.transaction_id.value() == 0 {
        return Err(TxnProtocolError::CorruptParticipantState);
    }
    let canonical = wire::CommittedWrite {
        transaction_id: encode_id(write.transaction_id),
        commit_ts: Some(encode_time(write.commit_ts)),
    }
    .encode_to_vec();
    if canonical != payload {
        return Err(TxnProtocolError::NonCanonicalRecord);
    }
    Ok(write)
}

fn encode_record<M: Message>(magic: [u8; 4], message: &M) -> Result<Vec<u8>, TxnProtocolError> {
    let payload_length = message.encoded_len();
    let total = HEADER_BYTES
        .checked_add(payload_length)
        .and_then(|value| value.checked_add(CHECKSUM_BYTES))
        .ok_or(TxnProtocolError::LengthOverflow)?;
    if total > MAX_RECORD_BYTES {
        return Err(TxnProtocolError::RecordTooLarge {
            max: MAX_RECORD_BYTES,
            actual: total,
        });
    }
    let mut bytes = Vec::with_capacity(total);
    bytes.extend_from_slice(&magic);
    bytes.extend_from_slice(&RECORD_VERSION.to_be_bytes());
    bytes.extend_from_slice(
        &u32::try_from(payload_length)
            .map_err(|_| TxnProtocolError::LengthOverflow)?
            .to_be_bytes(),
    );
    message
        .encode(&mut bytes)
        .map_err(|error| TxnProtocolError::PayloadEncode(error.to_string()))?;
    bytes.extend_from_slice(&crc32fast::hash(&bytes).to_be_bytes());
    Ok(bytes)
}

fn decode_record(magic: [u8; 4], bytes: &[u8]) -> Result<&[u8], TxnProtocolError> {
    if bytes.len() < HEADER_BYTES + CHECKSUM_BYTES || bytes[..4] != magic {
        return Err(TxnProtocolError::CorruptRecord);
    }
    if bytes.len() > MAX_RECORD_BYTES {
        return Err(TxnProtocolError::RecordTooLarge {
            max: MAX_RECORD_BYTES,
            actual: bytes.len(),
        });
    }
    let version = u16::from_be_bytes(bytes[4..6].try_into().expect("fixed version slice"));
    if version != RECORD_VERSION {
        return Err(TxnProtocolError::UnsupportedRecordVersion { version });
    }
    let payload_length = usize::try_from(u32::from_be_bytes(
        bytes[6..10].try_into().expect("fixed payload length slice"),
    ))
    .map_err(|_| TxnProtocolError::LengthOverflow)?;
    let expected = HEADER_BYTES
        .checked_add(payload_length)
        .and_then(|value| value.checked_add(CHECKSUM_BYTES))
        .ok_or(TxnProtocolError::LengthOverflow)?;
    if bytes.len() != expected {
        return Err(TxnProtocolError::CorruptRecord);
    }
    let checksum_offset = expected - CHECKSUM_BYTES;
    let checksum = u32::from_be_bytes(
        bytes[checksum_offset..]
            .try_into()
            .expect("fixed checksum slice"),
    );
    if crc32fast::hash(&bytes[..checksum_offset]) != checksum {
        return Err(TxnProtocolError::CorruptRecord);
    }
    Ok(&bytes[HEADER_BYTES..checksum_offset])
}

fn encode_prewrite(request: &PrewriteRequest) -> wire::PrewriteRequest {
    wire::PrewriteRequest {
        transaction_id: encode_id(request.transaction_id()),
        start_ts: Some(encode_time(request.start_ts())),
        schema_version: request.schema_version(),
        participant: Some(encode_shard(request.participant())),
        home: Some(encode_shard(request.home())),
        participants: request
            .participants()
            .iter()
            .copied()
            .map(encode_shard)
            .collect(),
        isolation: u32::from(request.isolation() as u8),
        expires_at: Some(encode_time(request.expires_at())),
        batch: Some(encode_batch(request.batch())),
        constraint_claims: request
            .constraint_claims()
            .iter()
            .map(encode_constraint_claim)
            .collect(),
        metadata: Some(encode_prewrite_metadata(request.metadata())),
    }
}

fn decode_prewrite(wire: wire::PrewriteRequest) -> Result<PrewriteRequest, TxnProtocolError> {
    let participants = wire
        .participants
        .into_iter()
        .map(decode_shard)
        .collect::<Result<Vec<_>, _>>()?;
    if participants.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(TxnProtocolError::NonCanonicalRecord);
    }
    let metadata = decode_prewrite_metadata(
        wire.metadata
            .ok_or(TxnProtocolError::MissingField("prewrite metadata"))?,
    )?;
    let transaction_id = decode_id(&wire.transaction_id, "prewrite transaction ID")?;
    let start_ts = decode_time(
        wire.start_ts
            .ok_or(TxnProtocolError::MissingField("prewrite start timestamp"))?,
    )?;
    let participant = decode_shard(
        wire.participant
            .ok_or(TxnProtocolError::MissingField("prewrite participant"))?,
    )?;
    let home = decode_shard(
        wire.home
            .ok_or(TxnProtocolError::MissingField("prewrite Home participant"))?,
    )?;
    let expires_at = decode_time(
        wire.expires_at
            .ok_or(TxnProtocolError::MissingField("prewrite expiry"))?,
    )?;
    let batch = decode_batch(
        wire.batch
            .ok_or(TxnProtocolError::MissingField("prewrite mutation batch"))?,
    )?;
    let constraint_claims = wire
        .constraint_claims
        .into_iter()
        .map(decode_constraint_claim)
        .collect::<Result<Vec<_>, _>>()?;
    let arguments = (
        transaction_id,
        start_ts,
        wire.schema_version,
        participant,
        home,
        participants,
        decode_isolation(wire.isolation)?,
        expires_at,
        batch,
        constraint_claims,
    );
    PrewriteRequest::new(
        arguments.0,
        arguments.1,
        arguments.2,
        arguments.3,
        arguments.4,
        arguments.5,
        arguments.6,
        arguments.7,
        arguments.8,
        arguments.9,
        metadata,
    )
}

fn encode_constraint_claim(claim: &ConstraintClaim) -> wire::ConstraintClaim {
    wire::ConstraintClaim {
        key: Some(encode_key(claim.key())),
        value: claim.value().to_vec(),
    }
}

fn decode_constraint_claim(
    wire: wire::ConstraintClaim,
) -> Result<ConstraintClaim, TxnProtocolError> {
    ConstraintClaim::new(
        decode_key(
            wire.key
                .ok_or(TxnProtocolError::MissingField("constraint claim key"))?,
        )?,
        wire.value,
    )
}

fn encode_prewrite_metadata(metadata: &PrewriteMetadata) -> wire::PrewriteMetadata {
    wire::PrewriteMetadata {
        schema_version: metadata.schema_version(),
        topology_epoch: metadata.topology_epoch(),
        point_reads: metadata
            .point_reads()
            .iter()
            .map(|read| wire::PointReadVersion {
                key: Some(encode_key(read.key())),
                observed_commit_ts: read.observed_commit_ts().map(encode_time),
            })
            .collect(),
        range_reads: metadata
            .range_reads()
            .iter()
            .map(|read| wire::RangeReadFingerprint {
                keyspace: u32::from(read.keyspace().tag()),
                prefix: read.prefix().to_vec(),
                fingerprint: read.fingerprint().to_vec(),
            })
            .collect(),
    }
}

fn decode_prewrite_metadata(
    wire: wire::PrewriteMetadata,
) -> Result<PrewriteMetadata, TxnProtocolError> {
    let point_reads = wire
        .point_reads
        .into_iter()
        .map(|read| {
            PointReadVersion::new(
                decode_key(
                    read.key
                        .ok_or(TxnProtocolError::MissingField("point read key"))?,
                )?,
                read.observed_commit_ts.map(decode_time).transpose()?,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let range_reads = wire
        .range_reads
        .into_iter()
        .map(|read| {
            let fingerprint: [u8; 32] = read.fingerprint.as_slice().try_into().map_err(|_| {
                TxnProtocolError::InvalidDigestLength {
                    actual: read.fingerprint.len(),
                }
            })?;
            RangeReadFingerprint::new(decode_keyspace(read.keyspace)?, read.prefix, fingerprint)
        })
        .collect::<Result<Vec<_>, _>>()?;
    PrewriteMetadata::new(
        wire.schema_version,
        wire.topology_epoch,
        point_reads,
        range_reads,
    )
}

fn encode_home(record: &HomeTransactionRecord) -> wire::HomeRecord {
    wire::HomeRecord {
        transaction_id: encode_id(record.transaction_id()),
        start_ts: Some(encode_time(record.start_ts())),
        state: u32::from(record.state() as u8),
        commit_ts: record.commit_ts().map(encode_time),
        participants: record
            .participants()
            .iter()
            .copied()
            .map(encode_shard)
            .collect(),
        proofs: record.proofs().iter().map(encode_proof).collect(),
    }
}

fn decode_home(wire: wire::HomeRecord) -> Result<HomeTransactionRecord, TxnProtocolError> {
    let participants = wire
        .participants
        .into_iter()
        .map(decode_shard)
        .collect::<Result<Vec<_>, _>>()?;
    if participants.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(TxnProtocolError::NonCanonicalRecord);
    }
    let proofs = wire
        .proofs
        .into_iter()
        .map(decode_proof)
        .collect::<Result<Vec<_>, _>>()?;
    if proofs
        .windows(2)
        .any(|pair| pair[0].participant() >= pair[1].participant())
    {
        return Err(TxnProtocolError::NonCanonicalRecord);
    }
    HomeTransactionRecord::new(
        decode_id(&wire.transaction_id, "Home transaction ID")?,
        decode_time(
            wire.start_ts
                .ok_or(TxnProtocolError::MissingField("Home start timestamp"))?,
        )?,
        decode_state(wire.state)?,
        wire.commit_ts.map(decode_time).transpose()?,
        participants,
        proofs,
    )
}

fn encode_proof(proof: &ParticipantProof) -> wire::ParticipantProof {
    wire::ParticipantProof {
        participant: Some(encode_shard(proof.participant())),
        min_commit_ts: Some(encode_time(proof.min_commit_ts())),
        intent_digest: proof.intent_digest().to_vec(),
    }
}

fn decode_proof(wire: wire::ParticipantProof) -> Result<ParticipantProof, TxnProtocolError> {
    Ok(ParticipantProof::new(
        decode_shard(
            wire.participant
                .ok_or(TxnProtocolError::MissingField("proof participant"))?,
        )?,
        decode_time(
            wire.min_commit_ts
                .ok_or(TxnProtocolError::MissingField("proof minimum commit"))?,
        )?,
        decode_digest(&wire.intent_digest)?,
    ))
}

fn encode_batch(batch: &PreparedMutationBatch) -> wire::PreparedMutationBatch {
    wire::PreparedMutationBatch {
        shard_id: batch.shard_id,
        transaction_id: batch.txn_id.to_be_bytes().to_vec(),
        mutations: batch.mutations.iter().map(encode_mutation).collect(),
    }
}

fn decode_batch(
    wire: wire::PreparedMutationBatch,
) -> Result<PreparedMutationBatch, TxnProtocolError> {
    Ok(PreparedMutationBatch {
        shard_id: wire.shard_id,
        txn_id: decode_raw_u128(&wire.transaction_id, "batch transaction ID")?,
        mutations: wire
            .mutations
            .into_iter()
            .map(decode_mutation)
            .collect::<Result<_, _>>()?,
    })
}

fn encode_mutation(mutation: &Mutation) -> wire::Mutation {
    match &mutation.operation {
        MutationOperation::Put { key, value } => wire::Mutation {
            sequence: mutation.sequence,
            operation: 1,
            key: Some(encode_key(key)),
            value: value.clone(),
        },
        MutationOperation::Delete { key } => wire::Mutation {
            sequence: mutation.sequence,
            operation: 2,
            key: Some(encode_key(key)),
            value: Vec::new(),
        },
    }
}

fn decode_mutation(wire: wire::Mutation) -> Result<Mutation, TxnProtocolError> {
    let key = decode_key(
        wire.key
            .ok_or(TxnProtocolError::MissingField("mutation key"))?,
    )?;
    match wire.operation {
        1 => Ok(Mutation::put(wire.sequence, key, wire.value)),
        2 if wire.value.is_empty() => Ok(Mutation::delete(wire.sequence, key)),
        2 => Err(TxnProtocolError::NonCanonicalDelete),
        tag => Err(TxnProtocolError::UnknownMutationOperation { tag }),
    }
}

fn encode_key(key: &LogicalKey) -> wire::LogicalKey {
    wire::LogicalKey {
        keyspace: u32::from(key.keyspace().tag()),
        key: key.as_bytes().to_vec(),
    }
}

fn decode_key(wire: wire::LogicalKey) -> Result<LogicalKey, TxnProtocolError> {
    Ok(LogicalKey::in_keyspace(
        decode_keyspace(wire.keyspace)?,
        wire.key,
    ))
}

fn decode_keyspace(tag: u32) -> Result<Keyspace, TxnProtocolError> {
    Ok(match tag {
        0 => Keyspace::Meta,
        1 => Keyspace::Identity,
        2 => Keyspace::Current,
        3 => Keyspace::AdjOut,
        4 => Keyspace::AdjIn,
        5 => Keyspace::History,
        6 => Keyspace::TemporalIndex,
        7 => Keyspace::Txn,
        tag => return Err(TxnProtocolError::UnknownKeyspace { tag }),
    })
}

fn encode_shard(shard: ShardEpoch) -> wire::ShardEpoch {
    wire::ShardEpoch {
        shard_id: shard.shard_id(),
        placement_epoch: shard.placement_epoch(),
    }
}

fn decode_shard(wire: wire::ShardEpoch) -> Result<ShardEpoch, TxnProtocolError> {
    ShardEpoch::new(wire.shard_id, wire.placement_epoch)
}

fn encode_time(timestamp: TransactionTime) -> wire::TransactionTime {
    wire::TransactionTime {
        physical_micros: timestamp.physical_micros(),
        logical: timestamp.logical(),
    }
}

fn decode_time(wire: wire::TransactionTime) -> Result<TransactionTime, TxnProtocolError> {
    Ok(TransactionTime::new(wire.physical_micros, wire.logical))
}

fn encode_id(transaction_id: TransactionId) -> Vec<u8> {
    transaction_id.value().to_be_bytes().to_vec()
}

fn decode_id(bytes: &[u8], field: &'static str) -> Result<TransactionId, TxnProtocolError> {
    let value = decode_raw_u128(bytes, field)?;
    if value == 0 {
        return Err(TxnProtocolError::InvalidTransactionId);
    }
    Ok(TransactionId::new(value))
}

fn decode_raw_u128(bytes: &[u8], field: &'static str) -> Result<u128, TxnProtocolError> {
    let array: [u8; 16] =
        bytes
            .try_into()
            .map_err(|_| TxnProtocolError::InvalidIdentifierLength {
                field,
                actual: bytes.len(),
            })?;
    Ok(u128::from_be_bytes(array))
}

fn decode_digest(bytes: &[u8]) -> Result<[u8; 32], TxnProtocolError> {
    bytes
        .try_into()
        .map_err(|_| TxnProtocolError::InvalidDigestLength {
            actual: bytes.len(),
        })
}

fn decode_isolation(tag: u32) -> Result<IsolationLevel, TxnProtocolError> {
    match tag {
        1 => Ok(IsolationLevel::TemporalSnapshot),
        2 => Ok(IsolationLevel::TemporalSerializable),
        tag => Err(TxnProtocolError::UnknownIsolation { tag }),
    }
}

fn decode_state(tag: u32) -> Result<TransactionState, TxnProtocolError> {
    match tag {
        1 => Ok(TransactionState::Active),
        2 => Ok(TransactionState::Preparing),
        3 => Ok(TransactionState::Committed),
        4 => Ok(TransactionState::Aborted),
        5 => Ok(TransactionState::Applied),
        6 => Ok(TransactionState::Cleaned),
        tag => Err(TxnProtocolError::UnknownTransactionState { tag }),
    }
}

fn validate_participant_record(record: &ParticipantRecord) -> Result<(), TxnProtocolError> {
    if record.proof.participant() != record.request.participant()
        || record.proof.intent_digest() != record.request.intent_digest()
        || record.proof.min_commit_ts() <= record.request.start_ts()
    {
        return Err(TxnProtocolError::CorruptParticipantState);
    }
    match record.state {
        TransactionState::Preparing | TransactionState::Aborted => {
            if record.commit_ts.is_some() {
                return Err(TxnProtocolError::CorruptParticipantState);
            }
        }
        TransactionState::Applied => {
            if record
                .commit_ts
                .is_none_or(|commit| commit <= record.proof.min_commit_ts())
            {
                return Err(TxnProtocolError::CorruptParticipantState);
            }
        }
        _ => return Err(TxnProtocolError::CorruptParticipantState),
    }
    Ok(())
}

mod wire {
    use prost::Message;

    #[derive(Clone, PartialEq, Message)]
    pub struct TransactionTime {
        #[prost(int64, tag = "1")]
        pub physical_micros: i64,
        #[prost(uint32, tag = "2")]
        pub logical: u32,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct ShardEpoch {
        #[prost(uint32, tag = "1")]
        pub shard_id: u32,
        #[prost(uint64, tag = "2")]
        pub placement_epoch: u64,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct LogicalKey {
        #[prost(uint32, tag = "1")]
        pub keyspace: u32,
        #[prost(bytes = "vec", tag = "2")]
        pub key: Vec<u8>,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct Mutation {
        #[prost(uint32, tag = "1")]
        pub sequence: u32,
        #[prost(uint32, tag = "2")]
        pub operation: u32,
        #[prost(message, optional, tag = "3")]
        pub key: Option<LogicalKey>,
        #[prost(bytes = "vec", tag = "4")]
        pub value: Vec<u8>,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct PreparedMutationBatch {
        #[prost(uint32, tag = "1")]
        pub shard_id: u32,
        #[prost(bytes = "vec", tag = "2")]
        pub transaction_id: Vec<u8>,
        #[prost(message, repeated, tag = "3")]
        pub mutations: Vec<Mutation>,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct PrewriteRequest {
        #[prost(bytes = "vec", tag = "1")]
        pub transaction_id: Vec<u8>,
        #[prost(message, optional, tag = "2")]
        pub start_ts: Option<TransactionTime>,
        #[prost(uint64, tag = "3")]
        pub schema_version: u64,
        #[prost(message, optional, tag = "4")]
        pub participant: Option<ShardEpoch>,
        #[prost(message, optional, tag = "5")]
        pub home: Option<ShardEpoch>,
        #[prost(message, repeated, tag = "6")]
        pub participants: Vec<ShardEpoch>,
        #[prost(uint32, tag = "7")]
        pub isolation: u32,
        #[prost(message, optional, tag = "8")]
        pub expires_at: Option<TransactionTime>,
        #[prost(message, optional, tag = "9")]
        pub batch: Option<PreparedMutationBatch>,
        #[prost(message, repeated, tag = "10")]
        pub constraint_claims: Vec<ConstraintClaim>,
        #[prost(message, optional, tag = "11")]
        pub metadata: Option<PrewriteMetadata>,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct PrewriteMetadata {
        #[prost(uint64, tag = "1")]
        pub schema_version: u64,
        #[prost(uint64, tag = "2")]
        pub topology_epoch: u64,
        #[prost(message, repeated, tag = "3")]
        pub point_reads: Vec<PointReadVersion>,
        #[prost(message, repeated, tag = "4")]
        pub range_reads: Vec<RangeReadFingerprint>,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct PointReadVersion {
        #[prost(message, optional, tag = "1")]
        pub key: Option<LogicalKey>,
        #[prost(message, optional, tag = "2")]
        pub observed_commit_ts: Option<TransactionTime>,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct RangeReadFingerprint {
        #[prost(uint32, tag = "1")]
        pub keyspace: u32,
        #[prost(bytes = "vec", tag = "2")]
        pub prefix: Vec<u8>,
        #[prost(bytes = "vec", tag = "3")]
        pub fingerprint: Vec<u8>,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct ConstraintClaim {
        #[prost(message, optional, tag = "1")]
        pub key: Option<LogicalKey>,
        #[prost(bytes = "vec", tag = "2")]
        pub value: Vec<u8>,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct ParticipantProof {
        #[prost(message, optional, tag = "1")]
        pub participant: Option<ShardEpoch>,
        #[prost(message, optional, tag = "2")]
        pub min_commit_ts: Option<TransactionTime>,
        #[prost(bytes = "vec", tag = "3")]
        pub intent_digest: Vec<u8>,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct HomeRecord {
        #[prost(bytes = "vec", tag = "1")]
        pub transaction_id: Vec<u8>,
        #[prost(message, optional, tag = "2")]
        pub start_ts: Option<TransactionTime>,
        #[prost(uint32, tag = "3")]
        pub state: u32,
        #[prost(message, optional, tag = "4")]
        pub commit_ts: Option<TransactionTime>,
        #[prost(message, repeated, tag = "5")]
        pub participants: Vec<ShardEpoch>,
        #[prost(message, repeated, tag = "6")]
        pub proofs: Vec<ParticipantProof>,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct ParticipantRecord {
        #[prost(message, optional, tag = "1")]
        pub request: Option<PrewriteRequest>,
        #[prost(message, optional, tag = "2")]
        pub proof: Option<ParticipantProof>,
        #[prost(uint32, tag = "3")]
        pub state: u32,
        #[prost(message, optional, tag = "4")]
        pub commit_ts: Option<TransactionTime>,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct IntentLock {
        #[prost(bytes = "vec", tag = "1")]
        pub transaction_id: Vec<u8>,
        #[prost(message, optional, tag = "2")]
        pub start_ts: Option<TransactionTime>,
        #[prost(message, optional, tag = "3")]
        pub expires_at: Option<TransactionTime>,
        #[prost(bytes = "vec", tag = "4")]
        pub intent_digest: Vec<u8>,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct CommittedWrite {
        #[prost(bytes = "vec", tag = "1")]
        pub transaction_id: Vec<u8>,
        #[prost(message, optional, tag = "2")]
        pub commit_ts: Option<TransactionTime>,
    }
}
