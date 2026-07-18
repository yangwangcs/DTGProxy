#![forbid(unsafe_code)]

mod durable_replica;
mod metadata;
mod raft_group;
mod read_barrier;
mod state_machine;
mod transport;

use std::error::Error;
use std::fmt::{self, Display, Formatter};

pub use durable_replica::{DurableRaftReplica, DurableReplicaError};
pub use metadata::{BackendLifecycle, MIN_REPLICA_TIME, ReplicaMetadata};
pub use raft_group::{InProcessShardGroup, MultiRaftRuntime, ProposalReceipt, ReplicationError};
pub use read_barrier::{FollowerReadProof, ReadBarrierError, ReadPermit, ReadPermitMode};
pub use state_machine::ShardStateMachine;
use storage_api::AdapterError;
use temporal_types::TransactionTime;
pub use transport::DeterministicTransport;

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
    TooManyMutations,
    InvalidBackendGeneration {
        generation: u64,
    },
    BackendLifecycleConflict,
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
            Self::TooManyMutations => {
                formatter.write_str("Replica metadata exceeds mutation sequence space")
            }
            Self::InvalidBackendGeneration { generation } => {
                write!(formatter, "invalid backend generation {generation}")
            }
            Self::BackendLifecycleConflict => {
                formatter.write_str("backend lifecycle command conflicts with replicated state")
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
