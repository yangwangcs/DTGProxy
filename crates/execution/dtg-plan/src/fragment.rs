use dtg_storage::{BackendGeneration, Digest32, PlacementEpoch, ReadFence, ShardId, Version};

use crate::{CatalogShard, SnapshotRequirements, StorageAccess};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub struct FragmentId(u32);

impl FragmentId {
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u32 {
        self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlanFence {
    read_fence: ReadFence,
    catalog_version: Version,
    schema_version: Version,
    snapshot_requirements: SnapshotRequirements,
}

impl PlanFence {
    pub(crate) fn new(
        shard: &CatalogShard,
        catalog_version: Version,
        schema_version: Version,
        snapshot_requirements: SnapshotRequirements,
    ) -> Self {
        Self {
            read_fence: ReadFence::new(shard.binding().clone(), shard.applied_index()),
            catalog_version,
            schema_version,
            snapshot_requirements,
        }
    }

    pub const fn read_fence(&self) -> &ReadFence {
        &self.read_fence
    }

    pub const fn shard_id(&self) -> ShardId {
        self.read_fence.binding().shard_id()
    }

    pub const fn placement_epoch(&self) -> PlacementEpoch {
        self.read_fence.binding().placement_epoch()
    }

    pub const fn backend_generation(&self) -> BackendGeneration {
        self.read_fence.binding().backend_generation()
    }

    pub const fn capability_digest(&self) -> Digest32 {
        self.read_fence.capability_digest()
    }

    pub const fn applied_index(&self) -> u64 {
        self.read_fence.applied_index()
    }

    pub const fn catalog_version(&self) -> Version {
        self.catalog_version
    }

    pub const fn schema_version(&self) -> Version {
        self.schema_version
    }

    pub const fn snapshot_requirements(&self) -> &SnapshotRequirements {
        &self.snapshot_requirements
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlanFragment {
    id: FragmentId,
    fence: PlanFence,
    storage_accesses: Vec<StorageAccess>,
}

impl PlanFragment {
    pub(crate) const fn new(
        id: FragmentId,
        fence: PlanFence,
        storage_accesses: Vec<StorageAccess>,
    ) -> Self {
        Self {
            id,
            fence,
            storage_accesses,
        }
    }

    pub const fn id(&self) -> FragmentId {
        self.id
    }

    pub const fn fence(&self) -> &PlanFence {
        &self.fence
    }

    pub fn storage_accesses(&self) -> &[StorageAccess] {
        &self.storage_accesses
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExchangeKind {
    Gather,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Exchange {
    source: FragmentId,
    kind: ExchangeKind,
}

impl Exchange {
    pub(crate) const fn gather(source: FragmentId) -> Self {
        Self {
            source,
            kind: ExchangeKind::Gather,
        }
    }

    pub const fn source(self) -> FragmentId {
        self.source
    }

    pub const fn kind(self) -> ExchangeKind {
        self.kind
    }
}
