use std::error::Error;
use std::fmt::{self, Display, Formatter};

use storage_api::{Keyspace, LogicalKey, Mutation, MutationOperation, PreparedMutationBatch};
use temporal_types::TransactionTime;

pub const MAX_TRANSACTION_PARTICIPANTS: usize = 64;
pub const MAX_TRANSACTION_MUTATIONS: usize = 16_384;
pub const MAX_TRANSACTION_CONSTRAINT_CLAIMS: usize = 1_024;
pub const MAX_TRANSACTION_POINT_READS: usize = 16_384;
pub const MAX_TRANSACTION_RANGE_READS: usize = 1_024;
const MAX_CONSTRAINT_VALUE_BYTES: usize = 1_024;
const MAX_RANGE_PREFIX_BYTES: usize = 1_024;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TransactionId(u128);

impl TransactionId {
    #[must_use]
    pub const fn new(value: u128) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn value(self) -> u128 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ShardEpoch {
    shard_id: u32,
    placement_epoch: u64,
}

impl ShardEpoch {
    pub fn new(shard_id: u32, placement_epoch: u64) -> Result<Self, TxnProtocolError> {
        if placement_epoch == 0 {
            return Err(TxnProtocolError::InvalidPlacementEpoch { shard_id });
        }
        Ok(Self {
            shard_id,
            placement_epoch,
        })
    }

    #[must_use]
    pub const fn shard_id(self) -> u32 {
        self.shard_id
    }

    #[must_use]
    pub const fn placement_epoch(self) -> u64 {
        self.placement_epoch
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum IsolationLevel {
    TemporalSnapshot = 1,
    TemporalSerializable = 2,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum TransactionState {
    Active = 1,
    Preparing = 2,
    Committed = 3,
    Aborted = 4,
    Applied = 5,
    Cleaned = 6,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParticipantProof {
    participant: ShardEpoch,
    min_commit_ts: TransactionTime,
    intent_digest: [u8; 32],
}

impl ParticipantProof {
    #[must_use]
    pub const fn new(
        participant: ShardEpoch,
        min_commit_ts: TransactionTime,
        intent_digest: [u8; 32],
    ) -> Self {
        Self {
            participant,
            min_commit_ts,
            intent_digest,
        }
    }

    #[must_use]
    pub const fn participant(&self) -> ShardEpoch {
        self.participant
    }

    #[must_use]
    pub const fn min_commit_ts(&self) -> TransactionTime {
        self.min_commit_ts
    }

    #[must_use]
    pub const fn intent_digest(&self) -> [u8; 32] {
        self.intent_digest
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConstraintClaim {
    key: LogicalKey,
    value: Vec<u8>,
}

impl ConstraintClaim {
    pub fn new(key: LogicalKey, value: Vec<u8>) -> Result<Self, TxnProtocolError> {
        if key.keyspace() != Keyspace::Txn || key.as_bytes().is_empty() {
            return Err(TxnProtocolError::InvalidConstraintKey);
        }
        if value.is_empty() || value.len() > MAX_CONSTRAINT_VALUE_BYTES {
            return Err(TxnProtocolError::InvalidConstraintValue {
                max: MAX_CONSTRAINT_VALUE_BYTES,
                actual: value.len(),
            });
        }
        Ok(Self { key, value })
    }

    #[must_use]
    pub const fn key(&self) -> &LogicalKey {
        &self.key
    }

    #[must_use]
    pub fn value(&self) -> &[u8] {
        &self.value
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PointReadVersion {
    key: LogicalKey,
    observed_commit_ts: Option<TransactionTime>,
}

impl PointReadVersion {
    pub fn new(
        key: LogicalKey,
        observed_commit_ts: Option<TransactionTime>,
    ) -> Result<Self, TxnProtocolError> {
        if key.as_bytes().is_empty() {
            return Err(TxnProtocolError::InvalidReadDependencyKey);
        }
        Ok(Self {
            key,
            observed_commit_ts,
        })
    }

    #[must_use]
    pub const fn key(&self) -> &LogicalKey {
        &self.key
    }

    #[must_use]
    pub const fn observed_commit_ts(&self) -> Option<TransactionTime> {
        self.observed_commit_ts
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RangeReadFingerprint {
    keyspace: Keyspace,
    prefix: Vec<u8>,
    fingerprint: [u8; 32],
}

impl RangeReadFingerprint {
    pub fn new(
        keyspace: Keyspace,
        prefix: Vec<u8>,
        fingerprint: [u8; 32],
    ) -> Result<Self, TxnProtocolError> {
        if prefix.len() > MAX_RANGE_PREFIX_BYTES {
            return Err(TxnProtocolError::InvalidRangePrefixLength {
                max: MAX_RANGE_PREFIX_BYTES,
                actual: prefix.len(),
            });
        }
        Ok(Self {
            keyspace,
            prefix,
            fingerprint,
        })
    }

    #[must_use]
    pub const fn keyspace(&self) -> Keyspace {
        self.keyspace
    }

    #[must_use]
    pub fn prefix(&self) -> &[u8] {
        &self.prefix
    }

    #[must_use]
    pub const fn fingerprint(&self) -> [u8; 32] {
        self.fingerprint
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PrewriteMetadata {
    schema_version: u64,
    topology_epoch: u64,
    point_reads: Vec<PointReadVersion>,
    range_reads: Vec<RangeReadFingerprint>,
}

impl PrewriteMetadata {
    pub fn new(
        schema_version: u64,
        topology_epoch: u64,
        mut point_reads: Vec<PointReadVersion>,
        mut range_reads: Vec<RangeReadFingerprint>,
    ) -> Result<Self, TxnProtocolError> {
        if schema_version == 0 || topology_epoch == 0 {
            return Err(TxnProtocolError::InvalidMetadataFence);
        }
        if point_reads.len() > MAX_TRANSACTION_POINT_READS {
            return Err(TxnProtocolError::InvalidPointReadCount {
                max: MAX_TRANSACTION_POINT_READS,
                actual: point_reads.len(),
            });
        }
        if range_reads.len() > MAX_TRANSACTION_RANGE_READS {
            return Err(TxnProtocolError::InvalidRangeReadCount {
                max: MAX_TRANSACTION_RANGE_READS,
                actual: range_reads.len(),
            });
        }
        point_reads.sort_by(|left, right| left.key.cmp(&right.key));
        if point_reads
            .windows(2)
            .any(|pair| pair[0].key == pair[1].key)
        {
            return Err(TxnProtocolError::DuplicatePointReadKey);
        }
        range_reads.sort_by(|left, right| {
            (left.keyspace.tag(), &left.prefix).cmp(&(right.keyspace.tag(), &right.prefix))
        });
        if range_reads
            .windows(2)
            .any(|pair| pair[0].keyspace == pair[1].keyspace && pair[0].prefix == pair[1].prefix)
        {
            return Err(TxnProtocolError::DuplicateRangeRead);
        }
        Ok(Self {
            schema_version,
            topology_epoch,
            point_reads,
            range_reads,
        })
    }

    #[must_use]
    pub const fn schema_version(&self) -> u64 {
        self.schema_version
    }

    #[must_use]
    pub const fn topology_epoch(&self) -> u64 {
        self.topology_epoch
    }

    #[must_use]
    pub fn point_reads(&self) -> &[PointReadVersion] {
        &self.point_reads
    }

    #[must_use]
    pub fn range_reads(&self) -> &[RangeReadFingerprint] {
        &self.range_reads
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PrewriteRequest {
    transaction_id: TransactionId,
    start_ts: TransactionTime,
    schema_version: u64,
    participant: ShardEpoch,
    home: ShardEpoch,
    participants: Vec<ShardEpoch>,
    isolation: IsolationLevel,
    expires_at: TransactionTime,
    batch: PreparedMutationBatch,
    constraint_claims: Vec<ConstraintClaim>,
    metadata: PrewriteMetadata,
}

impl PrewriteRequest {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        transaction_id: TransactionId,
        start_ts: TransactionTime,
        schema_version: u64,
        participant: ShardEpoch,
        home: ShardEpoch,
        mut participants: Vec<ShardEpoch>,
        isolation: IsolationLevel,
        expires_at: TransactionTime,
        batch: PreparedMutationBatch,
        mut constraint_claims: Vec<ConstraintClaim>,
        metadata: PrewriteMetadata,
    ) -> Result<Self, TxnProtocolError> {
        if transaction_id.value() == 0 {
            return Err(TxnProtocolError::InvalidTransactionId);
        }
        if schema_version == 0 {
            return Err(TxnProtocolError::InvalidSchemaVersion);
        }
        if expires_at <= start_ts {
            return Err(TxnProtocolError::InvalidExpiry {
                start_ts,
                expires_at,
            });
        }
        if participants.is_empty() || participants.len() > MAX_TRANSACTION_PARTICIPANTS {
            return Err(TxnProtocolError::InvalidParticipantCount {
                max: MAX_TRANSACTION_PARTICIPANTS,
                actual: participants.len(),
            });
        }
        participants.sort_unstable();
        if participants.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(TxnProtocolError::DuplicateParticipant);
        }
        if !participants.contains(&participant) {
            return Err(TxnProtocolError::ParticipantMissing { participant });
        }
        if !participants.contains(&home) {
            return Err(TxnProtocolError::HomeParticipantMissing { home });
        }
        validate_batch(
            transaction_id,
            participant,
            &batch,
            !constraint_claims.is_empty(),
        )?;
        if constraint_claims.len() > MAX_TRANSACTION_CONSTRAINT_CLAIMS {
            return Err(TxnProtocolError::InvalidConstraintClaimCount {
                max: MAX_TRANSACTION_CONSTRAINT_CLAIMS,
                actual: constraint_claims.len(),
            });
        }
        constraint_claims.sort_by(|left, right| left.key.cmp(&right.key));
        if constraint_claims
            .windows(2)
            .any(|pair| pair[0].key == pair[1].key)
        {
            return Err(TxnProtocolError::DuplicateConstraintKey);
        }
        if metadata.schema_version != schema_version
            || metadata.topology_epoch != participant.placement_epoch
        {
            return Err(TxnProtocolError::MetadataFenceMismatch);
        }
        Ok(Self {
            transaction_id,
            start_ts,
            schema_version,
            participant,
            home,
            participants,
            isolation,
            expires_at,
            batch,
            constraint_claims,
            metadata,
        })
    }

    pub fn encode(&self) -> Result<Vec<u8>, TxnProtocolError> {
        crate::codec::encode_prewrite_record(self)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, TxnProtocolError> {
        crate::codec::decode_prewrite_record(bytes)
    }

    #[must_use]
    pub const fn transaction_id(&self) -> TransactionId {
        self.transaction_id
    }

    #[must_use]
    pub const fn start_ts(&self) -> TransactionTime {
        self.start_ts
    }

    #[must_use]
    pub const fn schema_version(&self) -> u64 {
        self.schema_version
    }

    #[must_use]
    pub const fn participant(&self) -> ShardEpoch {
        self.participant
    }

    #[must_use]
    pub const fn home(&self) -> ShardEpoch {
        self.home
    }

    #[must_use]
    pub fn participants(&self) -> &[ShardEpoch] {
        &self.participants
    }

    #[must_use]
    pub const fn isolation(&self) -> IsolationLevel {
        self.isolation
    }

    #[must_use]
    pub const fn expires_at(&self) -> TransactionTime {
        self.expires_at
    }

    #[must_use]
    pub const fn batch(&self) -> &PreparedMutationBatch {
        &self.batch
    }

    #[must_use]
    pub fn constraint_claims(&self) -> &[ConstraintClaim] {
        &self.constraint_claims
    }

    #[must_use]
    pub const fn metadata(&self) -> &PrewriteMetadata {
        &self.metadata
    }

    #[must_use]
    pub fn intent_digest(&self) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"DTGProxy/PrewriteIntent");
        hasher.update(&self.transaction_id.value().to_be_bytes());
        hash_time(&mut hasher, self.start_ts);
        hasher.update(&self.schema_version.to_be_bytes());
        hash_shard(&mut hasher, self.participant);
        hash_shard(&mut hasher, self.home);
        hasher.update(&[self.isolation as u8]);
        hash_time(&mut hasher, self.expires_at);
        hasher.update(
            &u64::try_from(self.participants.len())
                .expect("participant count fits u64")
                .to_be_bytes(),
        );
        for participant in &self.participants {
            hash_shard(&mut hasher, *participant);
        }
        hasher.update(&self.batch.shard_id.to_be_bytes());
        hasher.update(&self.batch.txn_id.to_be_bytes());
        hasher.update(
            &u64::try_from(self.batch.mutations.len())
                .expect("mutation count fits u64")
                .to_be_bytes(),
        );
        for mutation in &self.batch.mutations {
            hasher.update(&mutation.sequence.to_be_bytes());
            match &mutation.operation {
                MutationOperation::Put { key, value } => {
                    hasher.update(&[1, key.keyspace().tag()]);
                    hash_bytes(&mut hasher, key.as_bytes());
                    hash_bytes(&mut hasher, value);
                }
                MutationOperation::Delete { key } => {
                    hasher.update(&[2, key.keyspace().tag()]);
                    hash_bytes(&mut hasher, key.as_bytes());
                }
            }
        }
        hasher.update(
            &u64::try_from(self.constraint_claims.len())
                .expect("constraint claim count fits u64")
                .to_be_bytes(),
        );
        for claim in &self.constraint_claims {
            hasher.update(&[claim.key.keyspace().tag()]);
            hash_bytes(&mut hasher, claim.key.as_bytes());
            hash_bytes(&mut hasher, &claim.value);
        }
        let metadata = &self.metadata;
        hasher.update(b"DTGProxy/PrewriteMetadata");
        hasher.update(&metadata.schema_version.to_be_bytes());
        hasher.update(&metadata.topology_epoch.to_be_bytes());
        hasher.update(
            &u64::try_from(metadata.point_reads.len())
                .expect("point read count fits u64")
                .to_be_bytes(),
        );
        for read in &metadata.point_reads {
            hasher.update(&[read.key.keyspace().tag()]);
            hash_bytes(&mut hasher, read.key.as_bytes());
            match read.observed_commit_ts {
                Some(timestamp) => {
                    hasher.update(&[1]);
                    hash_time(&mut hasher, timestamp);
                }
                None => {
                    hasher.update(&[0]);
                }
            }
        }
        hasher.update(
            &u64::try_from(metadata.range_reads.len())
                .expect("range read count fits u64")
                .to_be_bytes(),
        );
        for read in &metadata.range_reads {
            hasher.update(&[read.keyspace.tag()]);
            hash_bytes(&mut hasher, &read.prefix);
            hasher.update(&read.fingerprint);
        }
        *hasher.finalize().as_bytes()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HomeTransactionRecord {
    transaction_id: TransactionId,
    start_ts: TransactionTime,
    state: TransactionState,
    commit_ts: Option<TransactionTime>,
    participants: Vec<ShardEpoch>,
    proofs: Vec<ParticipantProof>,
}

impl HomeTransactionRecord {
    pub fn new(
        transaction_id: TransactionId,
        start_ts: TransactionTime,
        state: TransactionState,
        commit_ts: Option<TransactionTime>,
        mut participants: Vec<ShardEpoch>,
        mut proofs: Vec<ParticipantProof>,
    ) -> Result<Self, TxnProtocolError> {
        if transaction_id.value() == 0 {
            return Err(TxnProtocolError::InvalidTransactionId);
        }
        canonicalize_participants(&mut participants)?;
        proofs.sort_by_key(ParticipantProof::participant);
        if proofs
            .windows(2)
            .any(|pair| pair[0].participant == pair[1].participant)
        {
            return Err(TxnProtocolError::DuplicateParticipantProof);
        }
        if proofs.iter().any(|proof| {
            !participants.contains(&proof.participant()) || proof.min_commit_ts() <= start_ts
        }) {
            return Err(TxnProtocolError::InvalidParticipantProof);
        }
        match state {
            TransactionState::Committed | TransactionState::Applied => {
                let commit_ts = commit_ts.ok_or(TxnProtocolError::CommitTimestampMissing)?;
                if commit_ts <= start_ts {
                    return Err(TxnProtocolError::InvalidCommitTimestamp {
                        start_ts,
                        commit_ts,
                    });
                }
                let proof_participants = proofs
                    .iter()
                    .map(ParticipantProof::participant)
                    .collect::<Vec<_>>();
                if proof_participants != participants {
                    return Err(TxnProtocolError::ParticipantProofSetMismatch);
                }
                if proofs
                    .iter()
                    .any(|proof| commit_ts <= proof.min_commit_ts())
                {
                    return Err(TxnProtocolError::CommitBeforeParticipantMinimum);
                }
            }
            TransactionState::Active => {
                if !proofs.is_empty() {
                    return Err(TxnProtocolError::UnexpectedParticipantProofs { state });
                }
                if commit_ts.is_some() {
                    return Err(TxnProtocolError::UnexpectedCommitTimestamp { state });
                }
            }
            TransactionState::Preparing | TransactionState::Aborted | TransactionState::Cleaned => {
                if commit_ts.is_some() {
                    return Err(TxnProtocolError::UnexpectedCommitTimestamp { state });
                }
            }
        }
        Ok(Self {
            transaction_id,
            start_ts,
            state,
            commit_ts,
            participants,
            proofs,
        })
    }

    pub fn encode(&self) -> Result<Vec<u8>, TxnProtocolError> {
        crate::codec::encode_home_record(self)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, TxnProtocolError> {
        crate::codec::decode_home_record(bytes)
    }

    #[must_use]
    pub const fn transaction_id(&self) -> TransactionId {
        self.transaction_id
    }

    #[must_use]
    pub const fn start_ts(&self) -> TransactionTime {
        self.start_ts
    }

    #[must_use]
    pub const fn state(&self) -> TransactionState {
        self.state
    }

    #[must_use]
    pub const fn commit_ts(&self) -> Option<TransactionTime> {
        self.commit_ts
    }

    #[must_use]
    pub fn participants(&self) -> &[ShardEpoch] {
        &self.participants
    }

    #[must_use]
    pub fn proofs(&self) -> &[ParticipantProof] {
        &self.proofs
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecoveryAction {
    Wait,
    Rollback,
    RollForward { commit_ts: TransactionTime },
}

#[must_use]
pub fn recovery_action(
    home: Option<&HomeTransactionRecord>,
    expires_at: TransactionTime,
    observed_at: TransactionTime,
) -> RecoveryAction {
    if let Some(home) = home {
        match home.state {
            TransactionState::Committed | TransactionState::Applied => {
                return RecoveryAction::RollForward {
                    commit_ts: home
                        .commit_ts
                        .expect("committed Home record validated with commit timestamp"),
                };
            }
            TransactionState::Aborted | TransactionState::Cleaned => {
                return RecoveryAction::Rollback;
            }
            TransactionState::Active | TransactionState::Preparing => {}
        }
    }
    if observed_at > expires_at {
        RecoveryAction::Rollback
    } else {
        RecoveryAction::Wait
    }
}

fn canonicalize_participants(participants: &mut [ShardEpoch]) -> Result<(), TxnProtocolError> {
    if participants.is_empty() || participants.len() > MAX_TRANSACTION_PARTICIPANTS {
        return Err(TxnProtocolError::InvalidParticipantCount {
            max: MAX_TRANSACTION_PARTICIPANTS,
            actual: participants.len(),
        });
    }
    participants.sort_unstable();
    if participants.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(TxnProtocolError::DuplicateParticipant);
    }
    Ok(())
}

fn validate_batch(
    transaction_id: TransactionId,
    participant: ShardEpoch,
    batch: &PreparedMutationBatch,
    allow_empty: bool,
) -> Result<(), TxnProtocolError> {
    if batch.shard_id != participant.shard_id {
        return Err(TxnProtocolError::BatchShardMismatch {
            participant: participant.shard_id,
            batch: batch.shard_id,
        });
    }
    if batch.txn_id != transaction_id.value() {
        return Err(TxnProtocolError::BatchTransactionMismatch);
    }
    if (batch.mutations.is_empty() && !allow_empty)
        || batch.mutations.len() > MAX_TRANSACTION_MUTATIONS
    {
        return Err(TxnProtocolError::InvalidMutationCount {
            max: MAX_TRANSACTION_MUTATIONS,
            actual: batch.mutations.len(),
        });
    }
    let mut keys = Vec::with_capacity(batch.mutations.len());
    for (expected, mutation) in batch.mutations.iter().enumerate() {
        let expected = u32::try_from(expected).map_err(|_| TxnProtocolError::LengthOverflow)?;
        if mutation.sequence != expected {
            return Err(TxnProtocolError::NonCanonicalMutationSequence {
                expected,
                actual: mutation.sequence,
            });
        }
        keys.push(mutation_key(mutation));
    }
    keys.sort();
    if keys.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(TxnProtocolError::DuplicateMutationKey);
    }
    Ok(())
}

fn mutation_key(mutation: &Mutation) -> &LogicalKey {
    match &mutation.operation {
        MutationOperation::Put { key, .. } | MutationOperation::Delete { key } => key,
    }
}

fn hash_time(hasher: &mut blake3::Hasher, timestamp: TransactionTime) {
    hasher.update(&timestamp.physical_micros().to_be_bytes());
    hasher.update(&timestamp.logical().to_be_bytes());
}

fn hash_shard(hasher: &mut blake3::Hasher, shard: ShardEpoch) {
    hasher.update(&shard.shard_id.to_be_bytes());
    hasher.update(&shard.placement_epoch.to_be_bytes());
}

fn hash_bytes(hasher: &mut blake3::Hasher, bytes: &[u8]) {
    hasher.update(
        &u64::try_from(bytes.len())
            .expect("byte length fits u64")
            .to_be_bytes(),
    );
    hasher.update(bytes);
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ParticipantRecord {
    pub request: PrewriteRequest,
    pub proof: ParticipantProof,
    pub state: TransactionState,
    pub commit_ts: Option<TransactionTime>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IntentLock {
    pub transaction_id: TransactionId,
    pub start_ts: TransactionTime,
    pub expires_at: TransactionTime,
    pub intent_digest: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CommittedWrite {
    pub transaction_id: TransactionId,
    pub commit_ts: TransactionTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TxnProtocolError {
    InvalidTransactionId,
    InvalidPlacementEpoch {
        shard_id: u32,
    },
    InvalidSchemaVersion,
    InvalidExpiry {
        start_ts: TransactionTime,
        expires_at: TransactionTime,
    },
    InvalidParticipantCount {
        max: usize,
        actual: usize,
    },
    DuplicateParticipant,
    ParticipantMissing {
        participant: ShardEpoch,
    },
    HomeParticipantMissing {
        home: ShardEpoch,
    },
    BatchShardMismatch {
        participant: u32,
        batch: u32,
    },
    BatchTransactionMismatch,
    InvalidMutationCount {
        max: usize,
        actual: usize,
    },
    NonCanonicalMutationSequence {
        expected: u32,
        actual: u32,
    },
    DuplicateMutationKey,
    InvalidConstraintClaimCount {
        max: usize,
        actual: usize,
    },
    InvalidConstraintKey,
    InvalidConstraintValue {
        max: usize,
        actual: usize,
    },
    DuplicateConstraintKey,
    InvalidReadDependencyKey,
    InvalidRangePrefixLength {
        max: usize,
        actual: usize,
    },
    InvalidMetadataFence,
    MetadataFenceMismatch,
    InvalidPointReadCount {
        max: usize,
        actual: usize,
    },
    InvalidRangeReadCount {
        max: usize,
        actual: usize,
    },
    DuplicatePointReadKey,
    DuplicateRangeRead,
    ReadDependencyConflict {
        key: LogicalKey,
        expected: Option<TransactionTime>,
        actual: Option<TransactionTime>,
    },
    DuplicateParticipantProof,
    ParticipantProofSetMismatch,
    InvalidParticipantProof,
    UnexpectedParticipantProofs {
        state: TransactionState,
    },
    CommitBeforeParticipantMinimum,
    CommitTimestampMissing,
    UnexpectedCommitTimestamp {
        state: TransactionState,
    },
    InvalidCommitTimestamp {
        start_ts: TransactionTime,
        commit_ts: TransactionTime,
    },
    RecordTooLarge {
        max: usize,
        actual: usize,
    },
    CorruptRecord,
    UnsupportedRecordVersion {
        version: u16,
    },
    NonCanonicalRecord,
    PayloadEncode(String),
    PayloadDecode(String),
    MissingField(&'static str),
    InvalidIdentifierLength {
        field: &'static str,
        actual: usize,
    },
    InvalidDigestLength {
        actual: usize,
    },
    UnknownIsolation {
        tag: u32,
    },
    UnknownTransactionState {
        tag: u32,
    },
    UnknownKeyspace {
        tag: u32,
    },
    UnknownMutationOperation {
        tag: u32,
    },
    NonCanonicalDelete,
    LengthOverflow,
    InspectionCountMismatch {
        expected: usize,
        actual: usize,
    },
    IntentConflict {
        key: LogicalKey,
        owner: TransactionId,
    },
    WriteConflict {
        key: LogicalKey,
        committed_at: TransactionTime,
    },
    ConstraintConflict {
        key: LogicalKey,
    },
    RequestReplayMismatch,
    MissingIntent,
    MissingIntentLock {
        key: LogicalKey,
    },
    CorruptParticipantState,
    TransactionAlreadyAborted,
    TransactionAlreadyCommitted,
    InvalidHomeDecisionState {
        state: TransactionState,
    },
    HomeDecisionConflict,
    TimestampExhausted,
}

impl Display for TxnProtocolError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "distributed transaction protocol error: {self:?}"
        )
    }
}

impl Error for TxnProtocolError {}
