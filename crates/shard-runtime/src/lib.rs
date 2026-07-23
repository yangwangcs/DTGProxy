#![forbid(unsafe_code)]

mod artifact;
mod durable_replica;
mod metadata;
mod raft_group;
mod read_barrier;
mod state_machine;
mod transport;

use std::error::Error;
use std::fmt::{self, Display, Formatter};

pub use artifact::{
    AnalyticsArtifactChunk, AnalyticsArtifactGenerationHead,
    AnalyticsArtifactGenerationHeadIdentity, AnalyticsArtifactGenerationPin,
    MAX_ANALYTICS_ARTIFACT_CHUNKS, analytics_artifact_chunk_key,
    analytics_artifact_generation_head_key, analytics_artifact_generation_head_prefix,
    analytics_artifact_generation_heads_prefix, analytics_artifact_generation_pin_key,
    decode_analytics_artifact_chunk, decode_analytics_artifact_generation_head,
    decode_analytics_artifact_generation_head_identity,
    decode_analytics_artifact_generation_head_key, decode_analytics_artifact_generation_pin,
};
pub use durable_replica::{DurableRaftReplica, DurableReplicaError};
pub use metadata::{BackendLifecycle, MIN_REPLICA_TIME, ReplicaMetadata};
pub use raft_group::{InProcessShardGroup, MultiRaftRuntime, ProposalReceipt, ReplicationError};
pub use read_barrier::{FollowerReadProof, ReadBarrierError, ReadPermit, ReadPermitMode};
pub use state_machine::ShardStateMachine;
use storage_api::{AdapterError, ApplyReceipt};
use temporal_types::TransactionTime;
pub use transport::DeterministicTransport;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CommittedEntryOutcome {
    Applied(ApplyReceipt),
    Rejected {
        receipt: ApplyReceipt,
        message: String,
    },
}

#[derive(Debug)]
pub enum ShardRuntimeError {
    Adapter(AdapterError),
    Command(raft_command::CommandCodecError),
    Transaction(txn_protocol::TxnProtocolError),
    InvalidLogPosition {
        term: u64,
        index: u64,
    },
    ShardMismatch {
        expected: u32,
        actual: u32,
    },
    StaleEpoch {
        expected: u64,
        actual: u64,
    },
    NonContiguousIndex {
        expected: u64,
        actual: u64,
    },
    NonMonotonicTerm {
        current: u64,
        proposed: u64,
    },
    NonMonotonicCommit {
        current: TransactionTime,
        proposed: TransactionTime,
    },
    CommitAtOrBeforeClosed {
        closed: TransactionTime,
        proposed: TransactionTime,
    },
    IntentAtOrBeforeClosed {
        closed: TransactionTime,
        start: TransactionTime,
    },
    ParticipantProofMismatch,
    NonMonotonicClosed {
        current: TransactionTime,
        proposed: TransactionTime,
    },
    DivergentReplay {
        index: u64,
    },
    ReservedMetadataKey,
    MetadataIndexMismatch {
        metadata: u64,
        adapter: u64,
    },
    CorruptMetadata {
        record: &'static str,
    },
    ReplicaFaulted {
        failed_index: u64,
    },
    ApplyReceiptMismatch {
        expected: u64,
        actual: u64,
    },
    RequestEnvelopeMismatch {
        expected: u128,
        actual: u128,
    },
    RequestMismatch {
        request_id: u128,
    },
    CommittedRejection {
        request_id: u128,
        message: String,
    },
    TooManyMutations,
    InvalidBackendGeneration {
        generation: u64,
    },
    BackendLifecycleConflict,
    AnalyticsArtifactFence,
    AnalyticsArtifactConflict,
    AnalyticsArtifactLimit,
    CorruptAnalyticsArtifact {
        record: &'static str,
    },
}

impl Display for ShardRuntimeError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Adapter(error) => write!(formatter, "Adapter error: {error}"),
            Self::Command(error) => write!(formatter, "Raft command error: {error}"),
            Self::Transaction(error) => write!(formatter, "transaction protocol error: {error}"),
            Self::InvalidLogPosition { term, index } => {
                write!(
                    formatter,
                    "invalid Raft log position term={term}, index={index}"
                )
            }
            Self::ShardMismatch { expected, actual } => {
                write!(formatter, "expected shard {expected}, got shard {actual}")
            }
            Self::StaleEpoch { expected, actual } => {
                write!(
                    formatter,
                    "expected placement epoch {expected}, got {actual}"
                )
            }
            Self::NonContiguousIndex { expected, actual } => {
                write!(formatter, "expected log index {expected}, got {actual}")
            }
            Self::NonMonotonicTerm { current, proposed } => write!(
                formatter,
                "Raft term regressed from {current} to {proposed}"
            ),
            Self::NonMonotonicCommit { current, proposed } => write!(
                formatter,
                "commit timestamp {proposed:?} does not follow applied frontier {current:?}"
            ),
            Self::CommitAtOrBeforeClosed { closed, proposed } => write!(
                formatter,
                "commit timestamp {proposed:?} is at or before closed timestamp {closed:?}"
            ),
            Self::IntentAtOrBeforeClosed { closed, start } => write!(
                formatter,
                "transaction start timestamp {start:?} is at or before closed timestamp {closed:?}"
            ),
            Self::ParticipantProofMismatch => {
                formatter.write_str("replicated participant proof differs from deterministic proof")
            }
            Self::NonMonotonicClosed { current, proposed } => write!(
                formatter,
                "closed timestamp regressed from {current:?} to {proposed:?}"
            ),
            Self::DivergentReplay { index } => {
                write!(formatter, "Raft entry replay differs at index {index}")
            }
            Self::ReservedMetadataKey => {
                formatter.write_str("business mutation targets reserved Replica metadata")
            }
            Self::MetadataIndexMismatch { metadata, adapter } => write!(
                formatter,
                "Replica metadata index {metadata} differs from Adapter index {adapter}"
            ),
            Self::CorruptMetadata { record } => {
                write!(
                    formatter,
                    "corrupt or missing Replica metadata record {record}"
                )
            }
            Self::ReplicaFaulted { failed_index } => {
                write!(formatter, "Replica is faulted at log index {failed_index}")
            }
            Self::ApplyReceiptMismatch { expected, actual } => write!(
                formatter,
                "Adapter receipt index {actual} differs from applied entry {expected}"
            ),
            Self::RequestEnvelopeMismatch { expected, actual } => write!(
                formatter,
                "Raft proposal request ID {expected} differs from command request ID {actual}"
            ),
            Self::RequestMismatch { request_id } => write!(
                formatter,
                "request {request_id} was retried with different command bytes"
            ),
            Self::CommittedRejection {
                request_id,
                message,
            } => write!(
                formatter,
                "request {request_id} was durably rejected: {message}"
            ),
            Self::TooManyMutations => {
                formatter.write_str("Replica metadata exceeds mutation sequence space")
            }
            Self::InvalidBackendGeneration { generation } => {
                write!(formatter, "invalid backend generation {generation}")
            }
            Self::BackendLifecycleConflict => {
                formatter.write_str("backend lifecycle command conflicts with replicated state")
            }
            Self::AnalyticsArtifactFence => {
                formatter.write_str("analytics artifact generation is stale or sealed")
            }
            Self::AnalyticsArtifactConflict => {
                formatter.write_str("analytics artifact chunk conflicts with stored content")
            }
            Self::AnalyticsArtifactLimit => {
                formatter.write_str("analytics artifact generation exceeds the chunk limit")
            }
            Self::CorruptAnalyticsArtifact { record } => {
                write!(formatter, "corrupt analytics artifact {record}")
            }
        }
    }
}

impl Error for ShardRuntimeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Adapter(error) => Some(error),
            Self::Command(error) => Some(error),
            Self::Transaction(error) => Some(error),
            _ => None,
        }
    }
}

impl From<AdapterError> for ShardRuntimeError {
    fn from(error: AdapterError) -> Self {
        Self::Adapter(error)
    }
}

impl From<raft_command::CommandCodecError> for ShardRuntimeError {
    fn from(error: raft_command::CommandCodecError) -> Self {
        Self::Command(error)
    }
}

impl From<txn_protocol::TxnProtocolError> for ShardRuntimeError {
    fn from(error: txn_protocol::TxnProtocolError) -> Self {
        Self::Transaction(error)
    }
}

pub(crate) fn is_deterministic_business_error(error: &ShardRuntimeError) -> bool {
    match error {
        ShardRuntimeError::Transaction(error) => is_deterministic_transaction_error(error),
        ShardRuntimeError::StaleEpoch { .. }
        | ShardRuntimeError::NonMonotonicCommit { .. }
        | ShardRuntimeError::CommitAtOrBeforeClosed { .. }
        | ShardRuntimeError::IntentAtOrBeforeClosed { .. }
        | ShardRuntimeError::ParticipantProofMismatch
        | ShardRuntimeError::NonMonotonicClosed { .. }
        | ShardRuntimeError::ReservedMetadataKey
        | ShardRuntimeError::TooManyMutations
        | ShardRuntimeError::BackendLifecycleConflict
        | ShardRuntimeError::AnalyticsArtifactFence
        | ShardRuntimeError::AnalyticsArtifactConflict
        | ShardRuntimeError::AnalyticsArtifactLimit => true,
        ShardRuntimeError::Adapter(_)
        | ShardRuntimeError::Command(_)
        | ShardRuntimeError::InvalidLogPosition { .. }
        | ShardRuntimeError::ShardMismatch { .. }
        | ShardRuntimeError::NonContiguousIndex { .. }
        | ShardRuntimeError::NonMonotonicTerm { .. }
        | ShardRuntimeError::DivergentReplay { .. }
        | ShardRuntimeError::MetadataIndexMismatch { .. }
        | ShardRuntimeError::CorruptMetadata { .. }
        | ShardRuntimeError::ReplicaFaulted { .. }
        | ShardRuntimeError::ApplyReceiptMismatch { .. }
        | ShardRuntimeError::RequestEnvelopeMismatch { .. }
        | ShardRuntimeError::RequestMismatch { .. }
        | ShardRuntimeError::CommittedRejection { .. }
        | ShardRuntimeError::InvalidBackendGeneration { .. } => false,
        ShardRuntimeError::CorruptAnalyticsArtifact { .. } => false,
    }
}

fn is_deterministic_transaction_error(error: &txn_protocol::TxnProtocolError) -> bool {
    use txn_protocol::TxnProtocolError;

    match error {
        TxnProtocolError::InvalidTransactionId
        | TxnProtocolError::InvalidPlacementEpoch { .. }
        | TxnProtocolError::InvalidSchemaVersion
        | TxnProtocolError::InvalidExpiry { .. }
        | TxnProtocolError::InvalidParticipantCount { .. }
        | TxnProtocolError::DuplicateParticipant
        | TxnProtocolError::ParticipantMissing { .. }
        | TxnProtocolError::HomeParticipantMissing { .. }
        | TxnProtocolError::BatchShardMismatch { .. }
        | TxnProtocolError::BatchTransactionMismatch
        | TxnProtocolError::InvalidMutationCount { .. }
        | TxnProtocolError::NonCanonicalMutationSequence { .. }
        | TxnProtocolError::DuplicateMutationKey
        | TxnProtocolError::InvalidConstraintClaimCount { .. }
        | TxnProtocolError::InvalidConstraintKey
        | TxnProtocolError::InvalidConstraintValue { .. }
        | TxnProtocolError::DuplicateConstraintKey
        | TxnProtocolError::InvalidReadDependencyKey
        | TxnProtocolError::InvalidMetadataFence
        | TxnProtocolError::MetadataFenceMismatch
        | TxnProtocolError::InvalidPointReadCount { .. }
        | TxnProtocolError::InvalidRangeReadCount { .. }
        | TxnProtocolError::DuplicatePointReadKey
        | TxnProtocolError::DuplicateRangeRead
        | TxnProtocolError::ReadDependencyConflict { .. }
        | TxnProtocolError::DuplicateParticipantProof
        | TxnProtocolError::ParticipantProofSetMismatch
        | TxnProtocolError::InvalidParticipantProof
        | TxnProtocolError::UnexpectedParticipantProofs { .. }
        | TxnProtocolError::CommitBeforeParticipantMinimum
        | TxnProtocolError::CommitTimestampMissing
        | TxnProtocolError::UnexpectedCommitTimestamp { .. }
        | TxnProtocolError::InvalidCommitTimestamp { .. }
        | TxnProtocolError::IntentConflict { .. }
        | TxnProtocolError::WriteConflict { .. }
        | TxnProtocolError::ConstraintConflict { .. }
        | TxnProtocolError::MissingIntent
        | TxnProtocolError::TransactionAlreadyAborted
        | TxnProtocolError::TransactionAlreadyCommitted
        | TxnProtocolError::InvalidHomeDecisionState { .. }
        | TxnProtocolError::HomeDecisionConflict
        | TxnProtocolError::TimestampExhausted => true,
        TxnProtocolError::InvalidRangePrefixLength { .. }
        | TxnProtocolError::RecordTooLarge { .. }
        | TxnProtocolError::CorruptRecord
        | TxnProtocolError::UnsupportedRecordVersion { .. }
        | TxnProtocolError::NonCanonicalRecord
        | TxnProtocolError::PayloadEncode(_)
        | TxnProtocolError::PayloadDecode(_)
        | TxnProtocolError::MissingField(_)
        | TxnProtocolError::InvalidIdentifierLength { .. }
        | TxnProtocolError::InvalidDigestLength { .. }
        | TxnProtocolError::UnknownIsolation { .. }
        | TxnProtocolError::UnknownTransactionState { .. }
        | TxnProtocolError::UnknownKeyspace { .. }
        | TxnProtocolError::UnknownMutationOperation { .. }
        | TxnProtocolError::NonCanonicalDelete
        | TxnProtocolError::LengthOverflow
        | TxnProtocolError::InspectionCountMismatch { .. }
        | TxnProtocolError::RequestReplayMismatch
        | TxnProtocolError::MissingIntentLock { .. }
        | TxnProtocolError::CorruptParticipantState => false,
    }
}

#[cfg(test)]
mod tests {
    use storage_api::{Keyspace, LogicalKey};
    use txn_protocol::TxnProtocolError;

    use super::{ShardRuntimeError, is_deterministic_business_error};

    #[test]
    fn committed_rejection_classifier_accepts_conflicts_but_not_corruption() {
        let conflict = ShardRuntimeError::Transaction(TxnProtocolError::ConstraintConflict {
            key: LogicalKey::in_keyspace(Keyspace::Current, b"unique/email".to_vec()),
        });
        assert!(is_deterministic_business_error(&conflict));

        let corruption = ShardRuntimeError::Transaction(TxnProtocolError::CorruptRecord);
        assert!(!is_deterministic_business_error(&corruption));
        let invariant = ShardRuntimeError::Transaction(TxnProtocolError::InspectionCountMismatch {
            expected: 2,
            actual: 1,
        });
        assert!(!is_deterministic_business_error(&invariant));
        let replay_invariant =
            ShardRuntimeError::Transaction(TxnProtocolError::RequestReplayMismatch);
        assert!(!is_deterministic_business_error(&replay_invariant));
        let missing_lock = ShardRuntimeError::Transaction(TxnProtocolError::MissingIntentLock {
            key: LogicalKey::in_keyspace(Keyspace::Current, b"intent/lock".to_vec()),
        });
        assert!(!is_deterministic_business_error(&missing_lock));
    }
}
