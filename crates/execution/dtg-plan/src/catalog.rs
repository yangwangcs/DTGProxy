use std::collections::BTreeSet;

use dtg_storage::{
    BindingRole, CapabilityManifest, Digest32, GraphId, ReplicaBinding, TransactionTime, Version,
};

use crate::PlanError;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogShard {
    binding: ReplicaBinding,
    applied_index: u64,
}

impl CatalogShard {
    pub const fn new(binding: ReplicaBinding, applied_index: u64) -> Self {
        Self {
            binding,
            applied_index,
        }
    }

    pub const fn binding(&self) -> &ReplicaBinding {
        &self.binding
    }

    pub const fn applied_index(&self) -> u64 {
        self.applied_index
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogSnapshot {
    version: Version,
    schema_version: Version,
    graph_id: GraphId,
    shards: Vec<CatalogShard>,
}

impl CatalogSnapshot {
    pub fn new(
        version: Version,
        schema_version: Version,
        mut shards: Vec<CatalogShard>,
    ) -> Result<Self, PlanError> {
        if version.get() == 0 || schema_version.get() == 0 || shards.is_empty() {
            return Err(PlanError::InvalidCatalog(
                "catalog and schema versions and the Shard set must be nonempty".into(),
            ));
        }
        let graph_id = shards[0].binding.graph_id();
        let mut shard_ids = BTreeSet::new();
        for shard in &shards {
            if shard.binding.role() != BindingRole::Active {
                return Err(PlanError::InvalidCatalog(
                    "catalog planning requires active replica bindings".into(),
                ));
            }
            if shard.binding.graph_id() != graph_id || !shard_ids.insert(shard.binding.shard_id()) {
                return Err(PlanError::InvalidCatalog(
                    "catalog Shards must have one graph and unique Shard identities".into(),
                ));
            }
        }
        shards.sort_unstable_by_key(|shard| shard.binding.shard_id());
        Ok(Self {
            version,
            schema_version,
            graph_id,
            shards,
        })
    }

    pub const fn version(&self) -> Version {
        self.version
    }

    pub const fn schema_version(&self) -> Version {
        self.schema_version
    }

    pub const fn graph_id(&self) -> GraphId {
        self.graph_id
    }

    pub fn shards(&self) -> &[CatalogShard] {
        &self.shards
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotRequirements {
    transaction_time: TransactionTime,
    valid_at: i64,
    immutable: bool,
}

impl SnapshotRequirements {
    pub const fn fixed(transaction_time: TransactionTime, valid_at: i64) -> Self {
        Self {
            transaction_time,
            valid_at,
            immutable: true,
        }
    }

    pub const fn new(transaction_time: TransactionTime, valid_at: i64, immutable: bool) -> Self {
        Self {
            transaction_time,
            valid_at,
            immutable,
        }
    }

    pub const fn transaction_time(&self) -> TransactionTime {
        self.transaction_time
    }

    pub const fn valid_at(&self) -> i64 {
        self.valid_at
    }

    pub const fn immutable(&self) -> bool {
        self.immutable
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlanningContext {
    catalog: CatalogSnapshot,
    capabilities: CapabilityManifest,
    snapshot_requirements: SnapshotRequirements,
    logical_scan_bound: Option<u32>,
}

impl PlanningContext {
    pub fn new(
        catalog: CatalogSnapshot,
        capabilities: CapabilityManifest,
        snapshot_requirements: SnapshotRequirements,
        logical_scan_bound: Option<u32>,
    ) -> Result<Self, PlanError> {
        if logical_scan_bound == Some(0) {
            return Err(PlanError::InvalidCatalog(
                "logical scan bound must be nonzero".into(),
            ));
        }
        for shard in catalog.shards() {
            let actual = shard.binding().capability_digest();
            let expected = capabilities.digest();
            if actual != expected {
                return Err(PlanError::CapabilityDrift {
                    shard_id: shard.binding().shard_id(),
                    expected,
                    actual,
                });
            }
        }
        Ok(Self {
            catalog,
            capabilities,
            snapshot_requirements,
            logical_scan_bound,
        })
    }

    pub const fn catalog(&self) -> &CatalogSnapshot {
        &self.catalog
    }

    pub const fn capabilities(&self) -> &CapabilityManifest {
        &self.capabilities
    }

    pub const fn snapshot_requirements(&self) -> &SnapshotRequirements {
        &self.snapshot_requirements
    }

    pub const fn logical_scan_bound(&self) -> Option<u32> {
        self.logical_scan_bound
    }

    pub const fn capability_digest(&self) -> Digest32 {
        self.capabilities.digest()
    }
}
