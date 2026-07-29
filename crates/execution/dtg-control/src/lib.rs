#![forbid(unsafe_code)]

mod binding;
mod catalog;
mod migration;
mod observation;
mod placement;
mod reconcile;

use core::fmt;

pub use binding::{ReplicaBindingRecord, RetentionPin};
pub use catalog::{CatalogCommand, CatalogState, LineageEntry};
pub use dtg_kernel::{
    BackendGeneration, ClusterId, Digest32, GraphId, PlacementEpoch, ReplicaId, ShardId, Version,
};
pub use dtg_storage::{BackendClass, BindingRole, NamespaceId, ProviderKind, ReplicaBinding};
pub use migration::{
    MigrationAction, MigrationActionKind, MigrationId, MigrationReceipt, MigrationReceiptKind,
    MigrationRecord, MigrationState,
};
pub use observation::{ObservedNodeState, ObservedReplicaLifecycle, ObservedReplicaState};
pub use placement::ShardPlacement;
pub use reconcile::{ReconcileAction, Reconciler};

pub const CLEAN_BREAK_ARCHITECTURE_VERSION: u32 = 1;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ControlError {
    PlaintextCredential,
    BindingBackendClassMismatch,
    InvalidPlacement(&'static str),
    MixedBackendClass,
    InvalidObservation(&'static str),
    StaleCatalog { expected: Version, actual: Version },
    LineageConflict,
    RetentionPinned,
    UnknownGeneration,
    StaleObservation { expected: Version, actual: Version },
    VersionOverflow,
    InvalidMigration(&'static str),
    IllegalMigrationTransition,
    InvalidMigrationReceipt,
    IncompleteMigrationReceipts,
    MigrationReceiptConflict,
    MigrationDigestMismatch,
    MigrationNamespaceCollision,
    MigrationCleanupFenced,
}

impl ControlError {
    pub const fn code(&self) -> &'static str {
        match self {
            Self::PlaintextCredential => "DTG-CONTROL-CREDENTIAL-PLAINTEXT",
            Self::BindingBackendClassMismatch => "DTG-CONTROL-BINDING-BACKEND-CLASS",
            Self::InvalidPlacement(_) => "DTG-CONTROL-INVALID-PLACEMENT",
            Self::MixedBackendClass => "DTG-CONTROL-MIXED-BACKEND-CLASS",
            Self::InvalidObservation(_) => "DTG-CONTROL-INVALID-OBSERVATION",
            Self::StaleCatalog { .. } => "DTG-CONTROL-STALE-CATALOG",
            Self::LineageConflict => "DTG-CONTROL-LINEAGE",
            Self::RetentionPinned => "DTG-CONTROL-RETENTION-PINNED",
            Self::UnknownGeneration => "DTG-CONTROL-UNKNOWN-GENERATION",
            Self::StaleObservation { .. } => "DTG-CONTROL-STALE-OBSERVATION",
            Self::VersionOverflow => "DTG-CONTROL-VERSION-OVERFLOW",
            Self::InvalidMigration(_) => "DTG-CONTROL-MIGRATION-INVALID",
            Self::IllegalMigrationTransition => "DTG-CONTROL-MIGRATION-TRANSITION",
            Self::InvalidMigrationReceipt => "DTG-CONTROL-MIGRATION-RECEIPT",
            Self::IncompleteMigrationReceipts => "DTG-CONTROL-MIGRATION-INCOMPLETE",
            Self::MigrationReceiptConflict => "DTG-CONTROL-MIGRATION-RECEIPT-CONFLICT",
            Self::MigrationDigestMismatch => "DTG-CONTROL-MIGRATION-DIGEST",
            Self::MigrationNamespaceCollision => "DTG-CONTROL-MIGRATION-NAMESPACE-COLLISION",
            Self::MigrationCleanupFenced => "DTG-CONTROL-MIGRATION-CLEANUP-FENCED",
        }
    }
}

impl fmt::Display for ControlError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PlaintextCredential => {
                formatter.write_str("catalog credentials must be external references")
            }
            Self::BindingBackendClassMismatch => {
                formatter.write_str("replica binding does not match its backend class")
            }
            Self::InvalidPlacement(reason) => {
                write!(formatter, "invalid shard placement: {reason}")
            }
            Self::MixedBackendClass => {
                formatter.write_str("one shard generation cannot mix backend classes")
            }
            Self::InvalidObservation(reason) => {
                write!(formatter, "invalid replica observation: {reason}")
            }
            Self::StaleCatalog { expected, actual } => write!(
                formatter,
                "stale catalog command: expected version {}, actual version {}",
                expected.get(),
                actual.get()
            ),
            Self::LineageConflict => formatter.write_str(
                "a backend generation cannot be reassigned to a different backend class",
            ),
            Self::RetentionPinned => {
                formatter.write_str("retention pins prevent removing the backend generation")
            }
            Self::UnknownGeneration => {
                formatter.write_str("retention pin references an unknown backend generation")
            }
            Self::StaleObservation { expected, actual } => write!(
                formatter,
                "stale observation: expected catalog version {}, actual version {}",
                expected.get(),
                actual.get()
            ),
            Self::VersionOverflow => formatter.write_str("catalog version overflow"),
            Self::InvalidMigration(reason) => write!(formatter, "invalid migration: {reason}"),
            Self::IllegalMigrationTransition => {
                formatter.write_str("illegal migration state transition")
            }
            Self::InvalidMigrationReceipt => {
                formatter.write_str("migration receipt does not match the migration")
            }
            Self::IncompleteMigrationReceipts => {
                formatter.write_str("not all migration voters have acknowledged")
            }
            Self::MigrationReceiptConflict => {
                formatter.write_str("migration receipt conflicts with a durable receipt")
            }
            Self::MigrationDigestMismatch => {
                formatter.write_str("migration voters disagree on index or logical digest")
            }
            Self::MigrationNamespaceCollision => {
                formatter.write_str("migration target namespace belongs to another owner")
            }
            Self::MigrationCleanupFenced => formatter.write_str(
                "migration cleanup requires exact owner identity and released retention pins",
            ),
        }
    }
}

impl std::error::Error for ControlError {}
