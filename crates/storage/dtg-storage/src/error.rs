use core::fmt;

use crate::ReplicaBinding;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StorageError {
    InvalidBinding(String),
    InvalidCapability(String),
    InvalidMutation(String),
    InvalidBatch(String),
    /// Certification-only failure injected after private mutation staging.
    /// Production providers must not use this for business constraint failures.
    InjectedApplyFailure {
        staged_mutations: usize,
    },
    StaleBinding {
        expected: Box<ReplicaBinding>,
        actual: Box<ReplicaBinding>,
    },
    NamespaceOwnerMismatch {
        expected: Box<ReplicaBinding>,
        actual: Box<ReplicaBinding>,
    },
    NonMonotonicIndex {
        applied: u64,
        proposed: u64,
    },
    ReplayMismatch {
        raft_index: u64,
    },
    ReadFenceUnavailable {
        requested: u64,
        applied: u64,
    },
    CapabilityDrift,
    Unsupported,
    CorruptSnapshot(String),
    SnapshotIdentityMismatch,
    SnapshotNotExhausted,
    InvalidConsensus(String),
    InvalidArtifact(String),
    NotFound,
    TckViolation(String),
    Internal(String),
}

impl StorageError {
    pub const fn code(&self) -> &'static str {
        match self {
            Self::InvalidBinding(_) => "DTG-STORAGE-BINDING",
            Self::InvalidCapability(_) => "DTG-STORAGE-CAPABILITY",
            Self::InvalidMutation(_) => "DTG-STORAGE-MUTATION",
            Self::InvalidBatch(_) => "DTG-STORAGE-BATCH",
            Self::InjectedApplyFailure { .. } => "DTG-STORAGE-INJECTED-APPLY",
            Self::StaleBinding { .. } => "DTG-STORAGE-STALE-BINDING",
            Self::NamespaceOwnerMismatch { .. } => "DTG-STORAGE-NAMESPACE-OWNER",
            Self::NonMonotonicIndex { .. } => "DTG-STORAGE-RAFT-INDEX",
            Self::ReplayMismatch { .. } => "DTG-STORAGE-REPLAY",
            Self::ReadFenceUnavailable { .. } => "DTG-STORAGE-READ-FENCE",
            Self::CapabilityDrift => "DTG-STORAGE-CAPABILITY-DRIFT",
            Self::Unsupported => "DTG-STORAGE-UNSUPPORTED",
            Self::CorruptSnapshot(_) => "DTG-STORAGE-SNAPSHOT-CORRUPT",
            Self::SnapshotIdentityMismatch => "DTG-STORAGE-SNAPSHOT-IDENTITY",
            Self::SnapshotNotExhausted => "DTG-STORAGE-SNAPSHOT-NOT-EXHAUSTED",
            Self::InvalidConsensus(_) => "DTG-STORAGE-CONSENSUS",
            Self::InvalidArtifact(_) => "DTG-STORAGE-ARTIFACT",
            Self::NotFound => "DTG-STORAGE-NOT-FOUND",
            Self::TckViolation(_) => "DTG-STORAGE-TCK",
            Self::Internal(_) => "DTG-STORAGE-INTERNAL",
        }
    }
}

impl fmt::Display for StorageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidBinding(message)
            | Self::InvalidCapability(message)
            | Self::InvalidMutation(message)
            | Self::InvalidBatch(message)
            | Self::CorruptSnapshot(message)
            | Self::InvalidConsensus(message)
            | Self::InvalidArtifact(message)
            | Self::TckViolation(message)
            | Self::Internal(message) => write!(formatter, "{}: {message}", self.code()),
            Self::StaleBinding { .. } => formatter.write_str("stale replica binding"),
            Self::NamespaceOwnerMismatch { .. } => {
                formatter.write_str("namespace owner does not match requested binding")
            }
            Self::NonMonotonicIndex { applied, proposed } => write!(
                formatter,
                "Raft index must advance monotonically: applied {applied}, proposed {proposed}"
            ),
            Self::ReplayMismatch { raft_index } => {
                write!(
                    formatter,
                    "replay identity mismatch at Raft index {raft_index}"
                )
            }
            Self::ReadFenceUnavailable { requested, applied } => write!(
                formatter,
                "read fence {requested} is unavailable at applied index {applied}"
            ),
            Self::CapabilityDrift => formatter.write_str("capability digest changed"),
            Self::InjectedApplyFailure { staged_mutations } => write!(
                formatter,
                "injected apply failure after staging {staged_mutations} mutations"
            ),
            Self::Unsupported => formatter.write_str("operation is unsupported"),
            Self::SnapshotIdentityMismatch => {
                formatter.write_str("snapshot logical identity does not match target")
            }
            Self::SnapshotNotExhausted => {
                formatter.write_str("snapshot reader still contains unread chunks")
            }
            Self::NotFound => formatter.write_str("storage record was not found"),
        }
    }
}

impl std::error::Error for StorageError {}
