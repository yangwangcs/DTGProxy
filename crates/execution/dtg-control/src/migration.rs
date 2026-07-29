use std::collections::{BTreeMap, BTreeSet};

use crate::{
    BackendClass, BackendGeneration, ControlError, Digest32, GraphId, PlacementEpoch, ReplicaId,
    ShardId, Version,
};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub struct MigrationId(u128);

impl MigrationId {
    pub fn new(value: u128) -> Result<Self, ControlError> {
        (value != 0)
            .then_some(Self(value))
            .ok_or(ControlError::InvalidMigration(
                "migration identifier is zero",
            ))
    }

    pub const fn get(self) -> u128 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MigrationState {
    Allocating,
    Backfilling {
        snapshot_index: u64,
    },
    Mirroring {
        snapshot_index: u64,
    },
    Prepared {
        verified_index: u64,
        digest: Digest32,
    },
    Activating {
        next_epoch: PlacementEpoch,
    },
    Grace {
        activated_epoch: PlacementEpoch,
        activated_at: u64,
    },
    Completed,
    Aborted,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum MigrationReceiptKind {
    Verified,
    ForwardMirrored,
    ReverseMirrored,
    Cleanup,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MigrationReceipt {
    kind: MigrationReceiptKind,
    migration_id: MigrationId,
    replica_id: ReplicaId,
    catalog_version: Version,
    source_generation: BackendGeneration,
    target_generation: BackendGeneration,
    target_backend_class_digest: Digest32,
    applied_index: u64,
    logical_digest: Digest32,
    owner_identity: Option<Digest32>,
    pins_released: bool,
}

impl MigrationReceipt {
    #[allow(clippy::too_many_arguments)]
    pub fn verified(
        migration_id: MigrationId,
        replica_id: ReplicaId,
        catalog_version: Version,
        source_generation: BackendGeneration,
        target_generation: BackendGeneration,
        target_backend_class_digest: Digest32,
        applied_index: u64,
        logical_digest: Digest32,
    ) -> Result<Self, ControlError> {
        Self::proof(
            MigrationReceiptKind::Verified,
            migration_id,
            replica_id,
            catalog_version,
            source_generation,
            target_generation,
            target_backend_class_digest,
            applied_index,
            logical_digest,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn mirrored(
        migration_id: MigrationId,
        replica_id: ReplicaId,
        catalog_version: Version,
        source_generation: BackendGeneration,
        target_generation: BackendGeneration,
        target_backend_class_digest: Digest32,
        source_applied_index: u64,
        source_digest: Digest32,
        target_applied_index: u64,
        target_digest: Digest32,
    ) -> Result<Self, ControlError> {
        Self::dual_apply(
            MigrationReceiptKind::ForwardMirrored,
            migration_id,
            replica_id,
            catalog_version,
            source_generation,
            target_generation,
            target_backend_class_digest,
            source_applied_index,
            source_digest,
            target_applied_index,
            target_digest,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn reverse_mirrored(
        migration_id: MigrationId,
        replica_id: ReplicaId,
        catalog_version: Version,
        source_generation: BackendGeneration,
        target_generation: BackendGeneration,
        target_backend_class_digest: Digest32,
        target_applied_index: u64,
        target_digest: Digest32,
        retained_applied_index: u64,
        retained_digest: Digest32,
    ) -> Result<Self, ControlError> {
        Self::dual_apply(
            MigrationReceiptKind::ReverseMirrored,
            migration_id,
            replica_id,
            catalog_version,
            source_generation,
            target_generation,
            target_backend_class_digest,
            target_applied_index,
            target_digest,
            retained_applied_index,
            retained_digest,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn proof(
        kind: MigrationReceiptKind,
        migration_id: MigrationId,
        replica_id: ReplicaId,
        catalog_version: Version,
        source_generation: BackendGeneration,
        target_generation: BackendGeneration,
        target_backend_class_digest: Digest32,
        applied_index: u64,
        logical_digest: Digest32,
    ) -> Result<Self, ControlError> {
        if applied_index == 0
            || target_backend_class_digest.get() == [0; 32]
            || logical_digest.get() == [0; 32]
        {
            return Err(ControlError::InvalidMigrationReceipt);
        }
        Ok(Self {
            kind,
            migration_id,
            replica_id,
            catalog_version,
            source_generation,
            target_generation,
            target_backend_class_digest,
            applied_index,
            logical_digest,
            owner_identity: None,
            pins_released: false,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn dual_apply(
        kind: MigrationReceiptKind,
        migration_id: MigrationId,
        replica_id: ReplicaId,
        catalog_version: Version,
        source_generation: BackendGeneration,
        target_generation: BackendGeneration,
        target_backend_class_digest: Digest32,
        left_applied_index: u64,
        left_digest: Digest32,
        right_applied_index: u64,
        right_digest: Digest32,
    ) -> Result<Self, ControlError> {
        if left_applied_index != right_applied_index || left_digest != right_digest {
            return Err(ControlError::MigrationDigestMismatch);
        }
        Self::proof(
            kind,
            migration_id,
            replica_id,
            catalog_version,
            source_generation,
            target_generation,
            target_backend_class_digest,
            left_applied_index,
            left_digest,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn cleanup(
        migration_id: MigrationId,
        replica_id: ReplicaId,
        catalog_version: Version,
        source_generation: BackendGeneration,
        target_generation: BackendGeneration,
        target_backend_class_digest: Digest32,
        applied_index: u64,
        logical_digest: Digest32,
        owner_identity: Digest32,
    ) -> Self {
        Self {
            kind: MigrationReceiptKind::Cleanup,
            migration_id,
            replica_id,
            catalog_version,
            source_generation,
            target_generation,
            target_backend_class_digest,
            applied_index,
            logical_digest,
            owner_identity: Some(owner_identity),
            pins_released: true,
        }
    }

    pub const fn kind(&self) -> MigrationReceiptKind {
        self.kind
    }

    pub const fn migration_id(&self) -> MigrationId {
        self.migration_id
    }

    pub const fn replica_id(&self) -> ReplicaId {
        self.replica_id
    }

    pub const fn catalog_version(&self) -> Version {
        self.catalog_version
    }

    pub const fn source_generation(&self) -> BackendGeneration {
        self.source_generation
    }

    pub const fn target_generation(&self) -> BackendGeneration {
        self.target_generation
    }

    pub const fn target_backend_class_digest(&self) -> Digest32 {
        self.target_backend_class_digest
    }

    pub const fn applied_index(&self) -> u64 {
        self.applied_index
    }

    pub const fn logical_digest(&self) -> Digest32 {
        self.logical_digest
    }

    pub const fn owner_identity(&self) -> Option<Digest32> {
        self.owner_identity
    }

    pub const fn pins_released(&self) -> bool {
        self.pins_released
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MigrationActionKind {
    Allocate,
    Backfill,
    MirrorForward,
    Prepare,
    CommitCutover,
    PublishActivation,
    MirrorReverse,
    Cleanup,
    Rollback,
    Abort,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MigrationAction {
    kind: MigrationActionKind,
    migration_id: MigrationId,
    graph_id: GraphId,
    shard_id: ShardId,
    source_generation: BackendGeneration,
    source_backend_class: Option<BackendClass>,
    target_generation: BackendGeneration,
    target_backend_class: BackendClass,
    catalog_version: Version,
    generation: BackendGeneration,
    replica_id: Option<ReplicaId>,
    snapshot_index: Option<u64>,
    next_epoch: Option<PlacementEpoch>,
    owner_identity: Option<Digest32>,
    retained_owner_identity: Option<Digest32>,
}

impl MigrationAction {
    pub const fn kind(&self) -> MigrationActionKind {
        self.kind
    }

    pub const fn migration_id(&self) -> MigrationId {
        self.migration_id
    }

    pub const fn graph_id(&self) -> GraphId {
        self.graph_id
    }

    pub const fn shard_id(&self) -> ShardId {
        self.shard_id
    }

    pub const fn source_generation(&self) -> BackendGeneration {
        self.source_generation
    }

    pub const fn source_backend_class(&self) -> Option<&BackendClass> {
        self.source_backend_class.as_ref()
    }

    pub const fn target_generation(&self) -> BackendGeneration {
        self.target_generation
    }

    pub const fn target_backend_class(&self) -> &BackendClass {
        &self.target_backend_class
    }

    pub const fn catalog_version(&self) -> Version {
        self.catalog_version
    }

    pub const fn generation(&self) -> BackendGeneration {
        self.generation
    }

    pub const fn replica_id(&self) -> Option<ReplicaId> {
        self.replica_id
    }

    pub const fn snapshot_index(&self) -> Option<u64> {
        self.snapshot_index
    }

    pub const fn next_epoch(&self) -> Option<PlacementEpoch> {
        self.next_epoch
    }

    pub const fn owner_identity(&self) -> Option<Digest32> {
        self.owner_identity
    }

    pub const fn retained_owner_identity(&self) -> Option<Digest32> {
        self.retained_owner_identity
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MigrationRecord {
    id: MigrationId,
    graph_id: GraphId,
    shard_id: ShardId,
    catalog_version: Version,
    source_epoch: PlacementEpoch,
    source_generation: BackendGeneration,
    source_backend_class: Option<BackendClass>,
    target_generation: BackendGeneration,
    target_backend_class: BackendClass,
    expected_voters: BTreeSet<ReplicaId>,
    receipts: BTreeMap<(MigrationReceiptKind, ReplicaId), MigrationReceipt>,
    source_owners: BTreeMap<ReplicaId, Digest32>,
    target_owners: BTreeMap<ReplicaId, Digest32>,
    state: MigrationState,
    active_epoch: PlacementEpoch,
    active_generation: BackendGeneration,
}

impl MigrationRecord {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: MigrationId,
        graph_id: GraphId,
        shard_id: ShardId,
        catalog_version: Version,
        source_epoch: PlacementEpoch,
        source_generation: BackendGeneration,
        target_generation: BackendGeneration,
        target_backend_class: BackendClass,
        expected_voters: Vec<ReplicaId>,
    ) -> Result<Self, ControlError> {
        Self::new_inner(
            id,
            graph_id,
            shard_id,
            catalog_version,
            source_epoch,
            source_generation,
            None,
            target_generation,
            target_backend_class,
            expected_voters,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_direction(
        id: MigrationId,
        graph_id: GraphId,
        shard_id: ShardId,
        catalog_version: Version,
        source_epoch: PlacementEpoch,
        source_generation: BackendGeneration,
        source_backend_class: BackendClass,
        target_generation: BackendGeneration,
        target_backend_class: BackendClass,
        expected_voters: Vec<ReplicaId>,
    ) -> Result<Self, ControlError> {
        if source_backend_class.digest() == target_backend_class.digest() {
            return Err(ControlError::InvalidMigration(
                "source and target backend classes are identical",
            ));
        }
        Self::new_inner(
            id,
            graph_id,
            shard_id,
            catalog_version,
            source_epoch,
            source_generation,
            Some(source_backend_class),
            target_generation,
            target_backend_class,
            expected_voters,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn new_inner(
        id: MigrationId,
        graph_id: GraphId,
        shard_id: ShardId,
        catalog_version: Version,
        source_epoch: PlacementEpoch,
        source_generation: BackendGeneration,
        source_backend_class: Option<BackendClass>,
        target_generation: BackendGeneration,
        target_backend_class: BackendClass,
        expected_voters: Vec<ReplicaId>,
    ) -> Result<Self, ControlError> {
        let expected_target =
            source_generation
                .get()
                .checked_add(1)
                .ok_or(ControlError::InvalidMigration(
                    "backend generation overflow",
                ))?;
        let voter_count = expected_voters.len();
        let expected_voters = expected_voters.into_iter().collect::<BTreeSet<_>>();
        if target_generation.get() != expected_target
            || expected_voters.is_empty()
            || expected_voters.len() != voter_count
        {
            return Err(ControlError::InvalidMigration(
                "migration requires the next generation and unique voters",
            ));
        }
        Ok(Self {
            id,
            graph_id,
            shard_id,
            catalog_version,
            source_epoch,
            source_generation,
            source_backend_class,
            target_generation,
            target_backend_class,
            expected_voters,
            receipts: BTreeMap::new(),
            source_owners: BTreeMap::new(),
            target_owners: BTreeMap::new(),
            state: MigrationState::Allocating,
            active_epoch: source_epoch,
            active_generation: source_generation,
        })
    }

    pub const fn id(&self) -> MigrationId {
        self.id
    }

    pub const fn graph_id(&self) -> GraphId {
        self.graph_id
    }

    pub const fn shard_id(&self) -> ShardId {
        self.shard_id
    }

    pub const fn catalog_version(&self) -> Version {
        self.catalog_version
    }

    pub const fn source_generation(&self) -> BackendGeneration {
        self.source_generation
    }

    pub const fn source_backend_class(&self) -> Option<&BackendClass> {
        self.source_backend_class.as_ref()
    }

    pub fn provider_direction(&self) -> Option<(&crate::ProviderKind, &crate::ProviderKind)> {
        self.source_backend_class.as_ref().map(|source| {
            (
                source.provider_kind(),
                self.target_backend_class.provider_kind(),
            )
        })
    }

    pub const fn target_generation(&self) -> BackendGeneration {
        self.target_generation
    }

    pub const fn target_backend_class(&self) -> &BackendClass {
        &self.target_backend_class
    }

    pub const fn state(&self) -> MigrationState {
        self.state
    }

    pub const fn active_generation(&self) -> BackendGeneration {
        self.active_generation
    }

    pub fn receipts(&self) -> impl Iterator<Item = &MigrationReceipt> {
        self.receipts.values()
    }

    pub fn accepts(&self, epoch: PlacementEpoch, generation: BackendGeneration) -> bool {
        epoch == self.active_epoch && generation == self.active_generation
    }

    pub fn claim_target_namespace(
        &mut self,
        replica_id: ReplicaId,
        source_owner_identity: Digest32,
        target_owner_identity: Digest32,
        observed_target_owner: Option<Digest32>,
    ) -> Result<MigrationAction, ControlError> {
        if self.state != MigrationState::Allocating
            || !self.expected_voters.contains(&replica_id)
            || source_owner_identity.get() == [0; 32]
            || target_owner_identity.get() == [0; 32]
        {
            return Err(ControlError::IllegalMigrationTransition);
        }
        if observed_target_owner.is_some_and(|owner| owner != target_owner_identity) {
            return Err(ControlError::MigrationNamespaceCollision);
        }
        if self
            .source_owners
            .get(&replica_id)
            .is_some_and(|owner| *owner != source_owner_identity)
            || self
                .target_owners
                .get(&replica_id)
                .is_some_and(|owner| *owner != target_owner_identity)
        {
            return Err(ControlError::MigrationNamespaceCollision);
        }
        self.source_owners.insert(replica_id, source_owner_identity);
        self.target_owners.insert(replica_id, target_owner_identity);
        Ok(self.action(
            MigrationActionKind::Allocate,
            self.target_generation,
            Some(replica_id),
            None,
            None,
            Some(target_owner_identity),
            None,
        ))
    }

    pub fn begin_backfill(&mut self, snapshot_index: u64) -> Result<MigrationAction, ControlError> {
        if snapshot_index == 0 {
            return Err(ControlError::IllegalMigrationTransition);
        }
        match self.state {
            MigrationState::Allocating => {
                self.state = MigrationState::Backfilling { snapshot_index };
            }
            MigrationState::Backfilling {
                snapshot_index: existing,
            } if existing == snapshot_index => {}
            _ => return Err(ControlError::IllegalMigrationTransition),
        }
        Ok(self.action(
            MigrationActionKind::Backfill,
            self.target_generation,
            None,
            Some(snapshot_index),
            None,
            None,
            None,
        ))
    }

    pub fn begin_mirroring(&mut self) -> Result<MigrationAction, ControlError> {
        let snapshot_index = match self.state {
            MigrationState::Backfilling { snapshot_index } => {
                self.state = MigrationState::Mirroring { snapshot_index };
                snapshot_index
            }
            MigrationState::Mirroring { snapshot_index } => snapshot_index,
            _ => return Err(ControlError::IllegalMigrationTransition),
        };
        Ok(self.action(
            MigrationActionKind::MirrorForward,
            self.target_generation,
            None,
            Some(snapshot_index),
            None,
            None,
            None,
        ))
    }

    pub fn record_receipt(&mut self, receipt: MigrationReceipt) -> Result<(), ControlError> {
        let MigrationState::Mirroring { snapshot_index } = self.state else {
            return Err(ControlError::IllegalMigrationTransition);
        };
        if !matches!(
            receipt.kind,
            MigrationReceiptKind::Verified | MigrationReceiptKind::ForwardMirrored
        ) || receipt.applied_index < snapshot_index
        {
            return Err(ControlError::InvalidMigrationReceipt);
        }
        self.insert_receipt(receipt)
    }

    pub fn prepare(&mut self) -> Result<MigrationAction, ControlError> {
        let (verified_index, digest) = match self.state {
            MigrationState::Prepared {
                verified_index,
                digest,
            } => (verified_index, digest),
            MigrationState::Mirroring { .. } => self.equal_voter_proof(
                MigrationReceiptKind::ForwardMirrored,
                MigrationReceiptKind::ForwardMirrored,
            )?,
            _ => return Err(ControlError::IllegalMigrationTransition),
        };
        self.state = MigrationState::Prepared {
            verified_index,
            digest,
        };
        Ok(self.action(
            MigrationActionKind::Prepare,
            self.target_generation,
            None,
            Some(verified_index),
            None,
            None,
            None,
        ))
    }

    pub fn activate(
        &mut self,
        next_epoch: PlacementEpoch,
    ) -> Result<MigrationAction, ControlError> {
        let valid_next = next_epoch.get() == self.source_epoch.get().checked_add(1).unwrap_or(0);
        match self.state {
            MigrationState::Prepared { .. } if valid_next => {
                self.state = MigrationState::Activating { next_epoch };
                self.active_epoch = next_epoch;
                self.active_generation = self.target_generation;
            }
            MigrationState::Activating {
                next_epoch: existing,
            } if existing == next_epoch && self.active_generation == self.target_generation => {}
            _ => return Err(ControlError::IllegalMigrationTransition),
        }
        Ok(self.action(
            MigrationActionKind::CommitCutover,
            self.target_generation,
            None,
            None,
            Some(next_epoch),
            None,
            None,
        ))
    }

    pub fn publish_activation(
        &mut self,
        activated_at: u64,
    ) -> Result<MigrationAction, ControlError> {
        if activated_at == 0 {
            return Err(ControlError::InvalidMigration(
                "activation timestamp is zero",
            ));
        }
        let activated_epoch = match self.state {
            MigrationState::Activating { next_epoch } => {
                self.state = MigrationState::Grace {
                    activated_epoch: next_epoch,
                    activated_at,
                };
                next_epoch
            }
            MigrationState::Grace {
                activated_epoch,
                activated_at: existing,
            } if existing == activated_at => activated_epoch,
            _ => return Err(ControlError::IllegalMigrationTransition),
        };
        Ok(self.action(
            MigrationActionKind::PublishActivation,
            self.active_generation,
            None,
            None,
            Some(activated_epoch),
            None,
            None,
        ))
    }

    pub fn record_reverse_receipt(
        &mut self,
        receipt: MigrationReceipt,
    ) -> Result<MigrationAction, ControlError> {
        if !matches!(self.state, MigrationState::Grace { .. })
            || receipt.kind != MigrationReceiptKind::ReverseMirrored
        {
            return Err(ControlError::IllegalMigrationTransition);
        }
        let replica_id = receipt.replica_id;
        let applied_index = receipt.applied_index;
        self.insert_receipt(receipt)?;
        Ok(self.action(
            MigrationActionKind::MirrorReverse,
            self.source_generation,
            Some(replica_id),
            Some(applied_index),
            None,
            self.source_owners.get(&replica_id).copied(),
            None,
        ))
    }

    pub fn abort(
        &mut self,
        expected_catalog_version: Version,
    ) -> Result<MigrationAction, ControlError> {
        self.require_catalog(expected_catalog_version)?;
        match self.state {
            MigrationState::Allocating
            | MigrationState::Backfilling { .. }
            | MigrationState::Mirroring { .. }
            | MigrationState::Prepared { .. } => self.state = MigrationState::Aborted,
            MigrationState::Aborted => {}
            _ => return Err(ControlError::IllegalMigrationTransition),
        }
        Ok(self.action(
            MigrationActionKind::Abort,
            self.source_generation,
            None,
            None,
            None,
            None,
            None,
        ))
    }

    pub fn rollback(
        &mut self,
        next_epoch: PlacementEpoch,
        generation: BackendGeneration,
    ) -> Result<MigrationAction, ControlError> {
        let exact_retry = matches!(self.state, MigrationState::Activating { next_epoch: existing } if existing == next_epoch)
            && self.active_epoch == next_epoch
            && self.active_generation == generation;
        if !exact_retry
            && (!matches!(self.state, MigrationState::Grace { .. })
                || next_epoch.get() != self.active_epoch.get().checked_add(1).unwrap_or(0)
                || generation.get() != self.active_generation.get().checked_add(1).unwrap_or(0))
        {
            return Err(ControlError::IllegalMigrationTransition);
        }
        let retained_owner_identity = self.source_owners.values().next().copied();
        self.state = MigrationState::Activating { next_epoch };
        self.active_epoch = next_epoch;
        self.active_generation = generation;
        Ok(self.action(
            MigrationActionKind::Rollback,
            generation,
            None,
            None,
            Some(next_epoch),
            None,
            retained_owner_identity,
        ))
    }

    pub fn cleanup(
        &mut self,
        replica_id: ReplicaId,
        owner_identity: Digest32,
        pins_released: bool,
    ) -> Result<MigrationAction, ControlError> {
        if !matches!(
            self.state,
            MigrationState::Grace { .. } | MigrationState::Completed
        ) || !pins_released
            || self.source_owners.get(&replica_id) != Some(&owner_identity)
        {
            return Err(ControlError::MigrationCleanupFenced);
        }
        let (applied_index, digest) = self
            .equal_voter_proof(
                MigrationReceiptKind::ReverseMirrored,
                MigrationReceiptKind::ReverseMirrored,
            )
            .map_err(|_| ControlError::MigrationCleanupFenced)?;
        let receipt = MigrationReceipt::cleanup(
            self.id,
            replica_id,
            self.catalog_version,
            self.source_generation,
            self.target_generation,
            self.target_backend_class.digest(),
            applied_index,
            digest,
            owner_identity,
        );
        self.insert_receipt(receipt)?;
        if self.expected_voters.iter().all(|voter| {
            self.receipts
                .contains_key(&(MigrationReceiptKind::Cleanup, *voter))
        }) {
            self.state = MigrationState::Completed;
        }
        Ok(self.action(
            MigrationActionKind::Cleanup,
            self.source_generation,
            Some(replica_id),
            Some(applied_index),
            None,
            Some(owner_identity),
            None,
        ))
    }

    fn insert_receipt(&mut self, receipt: MigrationReceipt) -> Result<(), ControlError> {
        self.validate_receipt(&receipt)?;
        let key = (receipt.kind, receipt.replica_id);
        if let Some(existing) = self.receipts.get(&key) {
            return if existing == &receipt {
                Ok(())
            } else {
                Err(ControlError::MigrationReceiptConflict)
            };
        }
        self.receipts.insert(key, receipt);
        Ok(())
    }

    fn validate_receipt(&self, receipt: &MigrationReceipt) -> Result<(), ControlError> {
        self.require_catalog(receipt.catalog_version)?;
        if receipt.migration_id != self.id
            || receipt.source_generation != self.source_generation
            || receipt.target_generation != self.target_generation
            || receipt.target_backend_class_digest != self.target_backend_class.digest()
            || !self.expected_voters.contains(&receipt.replica_id)
        {
            return Err(ControlError::InvalidMigrationReceipt);
        }
        Ok(())
    }

    fn require_catalog(&self, actual: Version) -> Result<(), ControlError> {
        if actual == self.catalog_version {
            Ok(())
        } else {
            Err(ControlError::StaleCatalog {
                expected: self.catalog_version,
                actual,
            })
        }
    }

    fn equal_voter_proof(
        &self,
        preferred: MigrationReceiptKind,
        fallback: MigrationReceiptKind,
    ) -> Result<(u64, Digest32), ControlError> {
        let mut proof = None;
        for voter in &self.expected_voters {
            let receipt = self
                .receipts
                .get(&(preferred, *voter))
                .or_else(|| self.receipts.get(&(fallback, *voter)))
                .ok_or(ControlError::IncompleteMigrationReceipts)?;
            match proof {
                None => proof = Some((receipt.applied_index, receipt.logical_digest)),
                Some((index, digest))
                    if index == receipt.applied_index && digest == receipt.logical_digest => {}
                Some(_) => return Err(ControlError::MigrationDigestMismatch),
            }
        }
        proof.ok_or(ControlError::IncompleteMigrationReceipts)
    }

    #[allow(clippy::too_many_arguments)]
    fn action(
        &self,
        kind: MigrationActionKind,
        generation: BackendGeneration,
        replica_id: Option<ReplicaId>,
        snapshot_index: Option<u64>,
        next_epoch: Option<PlacementEpoch>,
        owner_identity: Option<Digest32>,
        retained_owner_identity: Option<Digest32>,
    ) -> MigrationAction {
        MigrationAction {
            kind,
            migration_id: self.id,
            graph_id: self.graph_id,
            shard_id: self.shard_id,
            source_generation: self.source_generation,
            source_backend_class: self.source_backend_class.clone(),
            target_generation: self.target_generation,
            target_backend_class: self.target_backend_class.clone(),
            catalog_version: self.catalog_version,
            generation,
            replica_id,
            snapshot_index,
            next_epoch,
            owner_identity,
            retained_owner_identity,
        }
    }
}
