use storage_api::{Keyspace, LogicalKey, Mutation, MutationOperation};
use temporal_types::TransactionTime;

use crate::codec::{
    decode_committed_write, decode_lock, decode_participant_record, encode_committed_write,
    encode_lock, encode_participant_record,
};
use crate::model::{
    CommittedWrite, IntentLock, ParticipantProof, ParticipantRecord, PrewriteRequest,
    TransactionState, TxnProtocolError,
};

const PARTICIPANT_PREFIX: &[u8] = b"\x01dtg/txn/v1/participant/";
const LOCK_PREFIX: &[u8] = b"\x01dtg/txn/v1/lock/";
const WRITE_PREFIX: &[u8] = b"\x01dtg/txn/v1/write/";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PrewriteOutcome {
    proof: ParticipantProof,
    mutations: Vec<Mutation>,
    duplicate: bool,
}

impl PrewriteOutcome {
    #[must_use]
    pub const fn proof(&self) -> &ParticipantProof {
        &self.proof
    }

    #[must_use]
    pub fn mutations(&self) -> &[Mutation] {
        &self.mutations
    }

    #[must_use]
    pub const fn duplicate(&self) -> bool {
        self.duplicate
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FinalizeOutcome {
    mutations: Vec<Mutation>,
    duplicate: bool,
}

impl FinalizeOutcome {
    #[must_use]
    pub fn mutations(&self) -> &[Mutation] {
        &self.mutations
    }

    #[must_use]
    pub const fn duplicate(&self) -> bool {
        self.duplicate
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AbortOutcome {
    mutations: Vec<Mutation>,
    duplicate: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ParticipantRecordStatus {
    state: TransactionState,
    commit_ts: Option<TransactionTime>,
    expires_at: TransactionTime,
}

impl ParticipantRecordStatus {
    #[must_use]
    pub const fn state(self) -> TransactionState {
        self.state
    }

    #[must_use]
    pub const fn commit_ts(self) -> Option<TransactionTime> {
        self.commit_ts
    }

    #[must_use]
    pub const fn expires_at(self) -> TransactionTime {
        self.expires_at
    }
}

impl AbortOutcome {
    #[must_use]
    pub fn mutations(&self) -> &[Mutation] {
        &self.mutations
    }

    #[must_use]
    pub const fn duplicate(&self) -> bool {
        self.duplicate
    }
}

pub struct ParticipantEngine;

impl ParticipantEngine {
    pub fn participant_record_key(
        participant: crate::ShardEpoch,
        transaction_id: crate::TransactionId,
    ) -> Result<LogicalKey, TxnProtocolError> {
        if transaction_id.value() == 0 {
            return Err(TxnProtocolError::InvalidTransactionId);
        }
        let mut bytes = Vec::with_capacity(PARTICIPANT_PREFIX.len() + 4 + 16);
        bytes.extend_from_slice(PARTICIPANT_PREFIX);
        bytes.extend_from_slice(&participant.shard_id().to_be_bytes());
        bytes.extend_from_slice(&transaction_id.value().to_be_bytes());
        Ok(LogicalKey::in_keyspace(Keyspace::Txn, bytes))
    }

    pub fn request_from_participant_record(
        participant: crate::ShardEpoch,
        transaction_id: crate::TransactionId,
        intent_digest: [u8; 32],
        bytes: &[u8],
    ) -> Result<PrewriteRequest, TxnProtocolError> {
        let record = decode_participant_record(bytes)?;
        if record.request.participant() != participant
            || record.request.transaction_id() != transaction_id
            || record.request.intent_digest() != intent_digest
            || record.proof.intent_digest() != intent_digest
        {
            return Err(TxnProtocolError::RequestReplayMismatch);
        }
        Ok(record.request)
    }

    pub fn participant_record_status(
        bytes: &[u8],
    ) -> Result<ParticipantRecordStatus, TxnProtocolError> {
        let record = decode_participant_record(bytes)?;
        Ok(ParticipantRecordStatus {
            state: record.state,
            commit_ts: record.commit_ts,
            expires_at: record.request.expires_at(),
        })
    }

    pub fn prewrite_inspection_keys(
        request: &PrewriteRequest,
    ) -> Result<Vec<LogicalKey>, TxnProtocolError> {
        let mut keys = Vec::with_capacity(
            request
                .batch()
                .mutations
                .len()
                .checked_mul(2)
                .and_then(|count| count.checked_add(1))
                .ok_or(TxnProtocolError::LengthOverflow)?,
        );
        keys.push(participant_key(request));
        for key in business_keys(request) {
            keys.push(metadata_key(LOCK_PREFIX, request, key)?);
            keys.push(metadata_key(WRITE_PREFIX, request, key)?);
        }
        Ok(keys)
    }

    pub fn prewrite(
        request: &PrewriteRequest,
        inspected_values: &[Option<Vec<u8>>],
    ) -> Result<PrewriteOutcome, TxnProtocolError> {
        let inspection_keys = Self::prewrite_inspection_keys(request)?;
        validate_inspection_count(&inspection_keys, inspected_values)?;
        let digest = request.intent_digest();

        if let Some(bytes) = inspected_values[0].as_deref() {
            let record = decode_participant_record(bytes)?;
            validate_replay(request, &record)?;
            return match record.state {
                TransactionState::Preparing => {
                    validate_locks(request, inspected_values.iter().skip(1).step_by(2), digest)?;
                    Ok(PrewriteOutcome {
                        proof: record.proof,
                        mutations: Vec::new(),
                        duplicate: true,
                    })
                }
                TransactionState::Applied => Ok(PrewriteOutcome {
                    proof: record.proof,
                    mutations: Vec::new(),
                    duplicate: true,
                }),
                TransactionState::Aborted => Err(TxnProtocolError::TransactionAlreadyAborted),
                _ => Err(TxnProtocolError::CorruptParticipantState),
            };
        }

        for ((key, lock), write) in business_keys(request)
            .zip(inspected_values.iter().skip(1).step_by(2))
            .zip(inspected_values.iter().skip(2).step_by(2))
        {
            if let Some(bytes) = lock.as_deref() {
                let lock = decode_lock(bytes)?;
                if lock.transaction_id == request.transaction_id() {
                    return Err(TxnProtocolError::CorruptParticipantState);
                }
                return Err(TxnProtocolError::IntentConflict {
                    key: key.clone(),
                    owner: lock.transaction_id,
                });
            }
            if let Some(bytes) = write.as_deref() {
                let write = decode_committed_write(bytes)?;
                if write.commit_ts > request.start_ts() {
                    return Err(TxnProtocolError::WriteConflict {
                        key: key.clone(),
                        committed_at: write.commit_ts,
                    });
                }
            }
        }

        let proof = ParticipantProof::new(
            request.participant(),
            timestamp_successor(request.start_ts())?,
            digest,
        );
        let record = ParticipantRecord {
            request: request.clone(),
            proof: proof.clone(),
            state: TransactionState::Preparing,
            commit_ts: None,
        };
        let lock = IntentLock {
            transaction_id: request.transaction_id(),
            start_ts: request.start_ts(),
            expires_at: request.expires_at(),
            intent_digest: digest,
        };
        let mut operations = Vec::with_capacity(request.batch().mutations.len() + 1);
        operations.push(MutationOperation::Put {
            key: inspection_keys[0].clone(),
            value: encode_participant_record(&record)?,
        });
        let lock_value = encode_lock(&lock)?;
        for lock_key in inspection_keys.iter().skip(1).step_by(2) {
            operations.push(MutationOperation::Put {
                key: lock_key.clone(),
                value: lock_value.clone(),
            });
        }
        Ok(PrewriteOutcome {
            proof,
            mutations: mutations_from_operations(operations)?,
            duplicate: false,
        })
    }

    pub fn one_phase_commit(
        request: &PrewriteRequest,
        commit_ts: TransactionTime,
        inspected_values: &[Option<Vec<u8>>],
    ) -> Result<FinalizeOutcome, TxnProtocolError> {
        let prewrite_keys = Self::prewrite_inspection_keys(request)?;
        validate_inspection_count(&prewrite_keys, inspected_values)?;
        let prewrite = Self::prewrite(request, inspected_values)?;
        let finalize_values = if prewrite.duplicate() {
            let mut values = Vec::with_capacity(request.batch().mutations.len() + 1);
            values.push(inspected_values[0].clone());
            values.extend(inspected_values.iter().skip(1).step_by(2).cloned());
            values
        } else {
            let mut values = Vec::with_capacity(request.batch().mutations.len() + 1);
            for mutation in prewrite.mutations() {
                let MutationOperation::Put { value, .. } = &mutation.operation else {
                    return Err(TxnProtocolError::CorruptParticipantState);
                };
                values.push(Some(value.clone()));
            }
            values
        };
        Self::finalize(request, commit_ts, &finalize_values)
    }

    pub fn finalize_inspection_keys(
        request: &PrewriteRequest,
    ) -> Result<Vec<LogicalKey>, TxnProtocolError> {
        let mut keys = Vec::with_capacity(request.batch().mutations.len() + 1);
        keys.push(participant_key(request));
        for key in business_keys(request) {
            keys.push(metadata_key(LOCK_PREFIX, request, key)?);
        }
        Ok(keys)
    }

    pub fn finalize(
        request: &PrewriteRequest,
        commit_ts: TransactionTime,
        inspected_values: &[Option<Vec<u8>>],
    ) -> Result<FinalizeOutcome, TxnProtocolError> {
        let inspection_keys = Self::finalize_inspection_keys(request)?;
        validate_inspection_count(&inspection_keys, inspected_values)?;
        let bytes = inspected_values[0]
            .as_deref()
            .ok_or(TxnProtocolError::MissingIntent)?;
        let mut record = decode_participant_record(bytes)?;
        validate_replay(request, &record)?;
        if record.state == TransactionState::Applied {
            if record.commit_ts == Some(commit_ts) {
                return Ok(FinalizeOutcome {
                    mutations: Vec::new(),
                    duplicate: true,
                });
            }
            return Err(TxnProtocolError::RequestReplayMismatch);
        }
        if record.state == TransactionState::Aborted {
            return Err(TxnProtocolError::TransactionAlreadyAborted);
        }
        if record.state != TransactionState::Preparing {
            return Err(TxnProtocolError::CorruptParticipantState);
        }
        if commit_ts <= record.proof.min_commit_ts() {
            return Err(TxnProtocolError::InvalidCommitTimestamp {
                start_ts: record.proof.min_commit_ts(),
                commit_ts,
            });
        }
        validate_locks(
            request,
            inspected_values.iter().skip(1),
            request.intent_digest(),
        )?;

        let mutation_count = request.batch().mutations.len();
        let mut operations = Vec::with_capacity(
            mutation_count
                .checked_mul(3)
                .and_then(|count| count.checked_add(1))
                .ok_or(TxnProtocolError::LengthOverflow)?,
        );
        operations.extend(
            request
                .batch()
                .mutations
                .iter()
                .map(|mutation| mutation.operation.clone()),
        );
        let write_value = encode_committed_write(&CommittedWrite {
            transaction_id: request.transaction_id(),
            commit_ts,
        })?;
        for (key, lock_key) in business_keys(request).zip(inspection_keys.iter().skip(1)) {
            operations.push(MutationOperation::Put {
                key: metadata_key(WRITE_PREFIX, request, key)?,
                value: write_value.clone(),
            });
            operations.push(MutationOperation::Delete {
                key: lock_key.clone(),
            });
        }
        record.state = TransactionState::Applied;
        record.commit_ts = Some(commit_ts);
        operations.push(MutationOperation::Put {
            key: inspection_keys[0].clone(),
            value: encode_participant_record(&record)?,
        });
        Ok(FinalizeOutcome {
            mutations: mutations_from_operations(operations)?,
            duplicate: false,
        })
    }

    pub fn abort_inspection_keys(
        request: &PrewriteRequest,
    ) -> Result<Vec<LogicalKey>, TxnProtocolError> {
        Self::finalize_inspection_keys(request)
    }

    pub fn abort(
        request: &PrewriteRequest,
        inspected_values: &[Option<Vec<u8>>],
    ) -> Result<AbortOutcome, TxnProtocolError> {
        let inspection_keys = Self::abort_inspection_keys(request)?;
        validate_inspection_count(&inspection_keys, inspected_values)?;
        let Some(bytes) = inspected_values[0].as_deref() else {
            return Ok(AbortOutcome {
                mutations: Vec::new(),
                duplicate: true,
            });
        };
        let mut record = decode_participant_record(bytes)?;
        validate_replay(request, &record)?;
        match record.state {
            TransactionState::Aborted => {
                return Ok(AbortOutcome {
                    mutations: Vec::new(),
                    duplicate: true,
                });
            }
            TransactionState::Applied => {
                return Err(TxnProtocolError::TransactionAlreadyCommitted);
            }
            TransactionState::Preparing => {}
            _ => return Err(TxnProtocolError::CorruptParticipantState),
        }
        validate_locks(
            request,
            inspected_values.iter().skip(1),
            request.intent_digest(),
        )?;
        let mut operations = Vec::with_capacity(inspection_keys.len());
        for lock_key in inspection_keys.iter().skip(1) {
            operations.push(MutationOperation::Delete {
                key: lock_key.clone(),
            });
        }
        record.state = TransactionState::Aborted;
        operations.push(MutationOperation::Put {
            key: inspection_keys[0].clone(),
            value: encode_participant_record(&record)?,
        });
        Ok(AbortOutcome {
            mutations: mutations_from_operations(operations)?,
            duplicate: false,
        })
    }
}

fn participant_key(request: &PrewriteRequest) -> LogicalKey {
    ParticipantEngine::participant_record_key(request.participant(), request.transaction_id())
        .expect("validated Prewrite request has a nonzero transaction ID")
}

fn metadata_key(
    prefix: &[u8],
    request: &PrewriteRequest,
    business_key: &LogicalKey,
) -> Result<LogicalKey, TxnProtocolError> {
    let key_length = u32::try_from(business_key.as_bytes().len())
        .map_err(|_| TxnProtocolError::LengthOverflow)?;
    let mut bytes = Vec::with_capacity(
        prefix
            .len()
            .checked_add(9)
            .and_then(|length| length.checked_add(business_key.as_bytes().len()))
            .ok_or(TxnProtocolError::LengthOverflow)?,
    );
    bytes.extend_from_slice(prefix);
    bytes.extend_from_slice(&request.participant().shard_id().to_be_bytes());
    bytes.push(business_key.keyspace().tag());
    bytes.extend_from_slice(&key_length.to_be_bytes());
    bytes.extend_from_slice(business_key.as_bytes());
    Ok(LogicalKey::in_keyspace(Keyspace::Txn, bytes))
}

fn business_keys(request: &PrewriteRequest) -> impl Iterator<Item = &LogicalKey> {
    request
        .batch()
        .mutations
        .iter()
        .map(|mutation| match &mutation.operation {
            MutationOperation::Put { key, .. } | MutationOperation::Delete { key } => key,
        })
}

fn validate_replay(
    request: &PrewriteRequest,
    record: &ParticipantRecord,
) -> Result<(), TxnProtocolError> {
    if &record.request != request
        || record.proof.participant() != request.participant()
        || record.proof.intent_digest() != request.intent_digest()
    {
        return Err(TxnProtocolError::RequestReplayMismatch);
    }
    Ok(())
}

fn validate_locks<'a>(
    request: &PrewriteRequest,
    locks: impl Iterator<Item = &'a Option<Vec<u8>>>,
    digest: [u8; 32],
) -> Result<(), TxnProtocolError> {
    for (key, value) in business_keys(request).zip(locks) {
        let bytes = value
            .as_deref()
            .ok_or_else(|| TxnProtocolError::MissingIntentLock { key: key.clone() })?;
        let lock = decode_lock(bytes)?;
        if lock.transaction_id != request.transaction_id()
            || lock.start_ts != request.start_ts()
            || lock.expires_at != request.expires_at()
            || lock.intent_digest != digest
        {
            return Err(TxnProtocolError::CorruptParticipantState);
        }
    }
    Ok(())
}

fn validate_inspection_count(
    keys: &[LogicalKey],
    values: &[Option<Vec<u8>>],
) -> Result<(), TxnProtocolError> {
    if keys.len() != values.len() {
        return Err(TxnProtocolError::InspectionCountMismatch {
            expected: keys.len(),
            actual: values.len(),
        });
    }
    Ok(())
}

fn timestamp_successor(timestamp: TransactionTime) -> Result<TransactionTime, TxnProtocolError> {
    if timestamp.logical() < u32::MAX {
        return Ok(TransactionTime::new(
            timestamp.physical_micros(),
            timestamp.logical() + 1,
        ));
    }
    let physical_micros = timestamp
        .physical_micros()
        .checked_add(1)
        .ok_or(TxnProtocolError::TimestampExhausted)?;
    Ok(TransactionTime::new(physical_micros, 0))
}

fn mutations_from_operations(
    operations: Vec<MutationOperation>,
) -> Result<Vec<Mutation>, TxnProtocolError> {
    operations
        .into_iter()
        .enumerate()
        .map(|(sequence, operation)| {
            Ok(Mutation {
                sequence: u32::try_from(sequence).map_err(|_| TxnProtocolError::LengthOverflow)?,
                operation,
            })
        })
        .collect()
}
