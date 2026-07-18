use std::collections::BTreeMap;

use crate::{BackendProfile, CatalogError, Placement};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum BackendMigrationState {
    Preparing,
    Restored,
    DualApplying,
    Verified,
    CutOver,
    Published,
    SourceRetired,
    Aborting,
    Aborted,
}

impl BackendMigrationState {
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::SourceRetired | Self::Aborted)
    }

    pub(crate) const fn can_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::Preparing, Self::Restored)
                | (Self::Restored, Self::DualApplying)
                | (Self::DualApplying, Self::Verified)
                | (Self::Verified, Self::CutOver)
                | (Self::CutOver, Self::Published)
                | (Self::Published, Self::SourceRetired)
                | (Self::Preparing, Self::Aborting)
                | (Self::Restored, Self::Aborting)
                | (Self::DualApplying, Self::Aborting)
                | (Self::Verified, Self::Aborting)
                | (Self::Aborting, Self::Aborted)
        )
    }

    pub(crate) const fn requires_full_receipts(self) -> bool {
        matches!(
            self,
            Self::Restored
                | Self::DualApplying
                | Self::Verified
                | Self::CutOver
                | Self::SourceRetired
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackendReplicaReceipt {
    state: BackendMigrationState,
    shard_id: u32,
    node_id: u64,
    applied_index: u64,
    profile_digest: [u8; 32],
}

impl BackendReplicaReceipt {
    pub fn new(
        state: BackendMigrationState,
        shard_id: u32,
        node_id: u64,
        applied_index: u64,
        profile_digest: [u8; 32],
    ) -> Result<Self, CatalogError> {
        if shard_id == 0
            || node_id == 0
            || profile_digest == [0; 32]
            || matches!(
                state,
                BackendMigrationState::Preparing | BackendMigrationState::Published
            )
        {
            return Err(CatalogError::InvalidBackendMigrationReceipt);
        }
        Ok(Self {
            state,
            shard_id,
            node_id,
            applied_index,
            profile_digest,
        })
    }

    #[must_use]
    pub const fn state(&self) -> BackendMigrationState {
        self.state
    }

    #[must_use]
    pub const fn shard_id(&self) -> u32 {
        self.shard_id
    }

    #[must_use]
    pub const fn node_id(&self) -> u64 {
        self.node_id
    }

    #[must_use]
    pub const fn applied_index(&self) -> u64 {
        self.applied_index
    }

    #[must_use]
    pub const fn profile_digest(&self) -> [u8; 32] {
        self.profile_digest
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackendMigrationRecord {
    migration_id: u128,
    graph_id: u64,
    source: BackendProfile,
    target: BackendProfile,
    state: BackendMigrationState,
    state_revision: u64,
    owner_term: u64,
    created_at_unix_ms: u64,
    updated_at_unix_ms: u64,
    receipts: BTreeMap<(BackendMigrationState, u32, u64), BackendReplicaReceipt>,
}

impl BackendMigrationRecord {
    pub fn new(
        migration_id: u128,
        graph_id: u64,
        source: BackendProfile,
        target: BackendProfile,
        owner_term: u64,
        created_at_unix_ms: u64,
    ) -> Result<Self, CatalogError> {
        if migration_id == 0 || graph_id == 0 || owner_term == 0 || created_at_unix_ms == 0 {
            return Err(CatalogError::InvalidBackendMigration);
        }
        let expected = source
            .generation()
            .checked_add(1)
            .ok_or(CatalogError::RevisionExhausted)?;
        if target.generation() != expected {
            return Err(CatalogError::NonSequentialBackendGeneration {
                graph_id,
                expected,
                actual: target.generation(),
            });
        }
        Ok(Self {
            migration_id,
            graph_id,
            source,
            target,
            state: BackendMigrationState::Preparing,
            state_revision: 0,
            owner_term,
            created_at_unix_ms,
            updated_at_unix_ms: created_at_unix_ms,
            receipts: BTreeMap::new(),
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn restore(
        migration_id: u128,
        graph_id: u64,
        source: BackendProfile,
        target: BackendProfile,
        state: BackendMigrationState,
        state_revision: u64,
        owner_term: u64,
        created_at_unix_ms: u64,
        updated_at_unix_ms: u64,
        receipts: Vec<BackendReplicaReceipt>,
    ) -> Result<Self, CatalogError> {
        let mut record = Self::new(
            migration_id,
            graph_id,
            source,
            target,
            owner_term,
            created_at_unix_ms,
        )?;
        if updated_at_unix_ms < created_at_unix_ms
            || (state_revision == 0) != (state == BackendMigrationState::Preparing)
        {
            return Err(CatalogError::InvalidBackendMigration);
        }
        record.state = state;
        record.state_revision = state_revision;
        record.updated_at_unix_ms = updated_at_unix_ms;
        for receipt in receipts {
            record.insert_receipt(receipt)?;
        }
        Ok(record)
    }

    pub(crate) fn validate_recovered(&self, placements: &[Placement]) -> Result<(), CatalogError> {
        for receipt in self.receipts.values() {
            if receipt.state != BackendMigrationState::Restored {
                let restored = self.receipts.get(&(
                    BackendMigrationState::Restored,
                    receipt.shard_id,
                    receipt.node_id,
                ));
                if restored.is_none_or(|binding| binding.profile_digest != receipt.profile_digest) {
                    return Err(CatalogError::InvalidBackendMigrationReceipt);
                }
            }
        }
        let required: &[BackendMigrationState] = match self.state {
            BackendMigrationState::Preparing
            | BackendMigrationState::Aborting
            | BackendMigrationState::Aborted => &[],
            BackendMigrationState::Restored => &[BackendMigrationState::Restored],
            BackendMigrationState::DualApplying => &[
                BackendMigrationState::Restored,
                BackendMigrationState::DualApplying,
            ],
            BackendMigrationState::Verified => &[
                BackendMigrationState::Restored,
                BackendMigrationState::DualApplying,
                BackendMigrationState::Verified,
            ],
            BackendMigrationState::CutOver
            | BackendMigrationState::Published
            | BackendMigrationState::SourceRetired => &[
                BackendMigrationState::Restored,
                BackendMigrationState::DualApplying,
                BackendMigrationState::Verified,
                BackendMigrationState::CutOver,
            ],
        };
        if required
            .iter()
            .any(|state| !self.has_full_receipts(*state, placements))
            || (self.state == BackendMigrationState::SourceRetired
                && !self.has_full_receipts(BackendMigrationState::SourceRetired, placements))
        {
            return Err(CatalogError::IncompleteBackendMigrationReceipts {
                migration_id: self.migration_id,
                state: self.state,
            });
        }
        Ok(())
    }

    #[must_use]
    pub const fn migration_id(&self) -> u128 {
        self.migration_id
    }
    #[must_use]
    pub const fn graph_id(&self) -> u64 {
        self.graph_id
    }
    #[must_use]
    pub const fn source(&self) -> &BackendProfile {
        &self.source
    }
    #[must_use]
    pub const fn target(&self) -> &BackendProfile {
        &self.target
    }
    #[must_use]
    pub const fn state(&self) -> BackendMigrationState {
        self.state
    }
    #[must_use]
    pub const fn state_revision(&self) -> u64 {
        self.state_revision
    }
    #[must_use]
    pub const fn owner_term(&self) -> u64 {
        self.owner_term
    }
    #[must_use]
    pub const fn created_at_unix_ms(&self) -> u64 {
        self.created_at_unix_ms
    }
    #[must_use]
    pub const fn updated_at_unix_ms(&self) -> u64 {
        self.updated_at_unix_ms
    }
    #[must_use]
    pub const fn receipts(
        &self,
    ) -> &BTreeMap<(BackendMigrationState, u32, u64), BackendReplicaReceipt> {
        &self.receipts
    }

    pub(crate) fn advance(
        &mut self,
        expected_state_revision: u64,
        next_state: BackendMigrationState,
        owner_term: u64,
        updated_at_unix_ms: u64,
        receipts: Vec<BackendReplicaReceipt>,
        placements: &[Placement],
    ) -> Result<(), CatalogError> {
        if self.state_revision != expected_state_revision {
            return Err(CatalogError::StaleBackendMigrationRevision {
                migration_id: self.migration_id,
                expected: self.state_revision,
                actual: expected_state_revision,
            });
        }
        if !self.state.can_transition_to(next_state) {
            return Err(CatalogError::IllegalBackendMigrationTransition {
                from: self.state,
                to: next_state,
            });
        }
        if owner_term < self.owner_term || updated_at_unix_ms < self.updated_at_unix_ms {
            return Err(CatalogError::StaleBackendMigrationOwner);
        }
        for receipt in receipts {
            if receipt.state != next_state {
                return Err(CatalogError::InvalidBackendMigrationReceipt);
            }
            if next_state != BackendMigrationState::Restored {
                let restored = self.receipts.get(&(
                    BackendMigrationState::Restored,
                    receipt.shard_id,
                    receipt.node_id,
                ));
                if restored.is_none_or(|binding| binding.profile_digest != receipt.profile_digest) {
                    return Err(CatalogError::InvalidBackendMigrationReceipt);
                }
            }
            self.insert_receipt(receipt)?;
        }
        if next_state.requires_full_receipts() && !self.has_full_receipts(next_state, placements) {
            return Err(CatalogError::IncompleteBackendMigrationReceipts {
                migration_id: self.migration_id,
                state: next_state,
            });
        }
        self.state = next_state;
        self.state_revision = self
            .state_revision
            .checked_add(1)
            .ok_or(CatalogError::RevisionExhausted)?;
        self.owner_term = owner_term;
        self.updated_at_unix_ms = updated_at_unix_ms;
        Ok(())
    }

    fn insert_receipt(&mut self, receipt: BackendReplicaReceipt) -> Result<(), CatalogError> {
        let key = (receipt.state, receipt.shard_id, receipt.node_id);
        if let Some(existing) = self.receipts.get(&key) {
            if existing != &receipt {
                return Err(CatalogError::BackendMigrationReceiptConflict {
                    migration_id: self.migration_id,
                    shard_id: receipt.shard_id,
                    node_id: receipt.node_id,
                });
            }
            return Ok(());
        }
        self.receipts.insert(key, receipt);
        Ok(())
    }

    fn has_full_receipts(&self, state: BackendMigrationState, placements: &[Placement]) -> bool {
        placements.iter().all(|placement| {
            placement.voters().iter().all(|node_id| {
                self.receipts
                    .contains_key(&(state, placement.shard_id(), *node_id))
            })
        })
    }
}
