use std::collections::BTreeMap;

use dtg_kernel::{
    BackendGeneration, PlacementEpoch, ShardId, TransactionId, TransactionTime, Version,
};

use crate::TxnError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShardSnapshotFence {
    pub placement_epoch: PlacementEpoch,
    pub backend_generation: BackendGeneration,
    pub applied_index: u64,
    pub closed_time: TransactionTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotToken {
    pub transaction_id: TransactionId,
    pub start_time: TransactionTime,
    pub catalog_version: Version,
    pub shards: BTreeMap<ShardId, ShardSnapshotFence>,
}

impl SnapshotToken {
    pub fn new(
        transaction_id: TransactionId,
        start_time: TransactionTime,
        catalog_version: Version,
        shard_fences: Vec<(ShardId, ShardSnapshotFence)>,
    ) -> Result<Self, TxnError> {
        if catalog_version.get() == 0 || shard_fences.is_empty() {
            return Err(TxnError::IncompleteSnapshot);
        }

        let mut shards = BTreeMap::new();
        for (shard_id, fence) in shard_fences {
            if fence.closed_time < start_time {
                return Err(TxnError::InconsistentSnapshot);
            }
            if shards.insert(shard_id, fence).is_some() {
                return Err(TxnError::DuplicateShardFence);
            }
        }

        Ok(Self {
            transaction_id,
            start_time,
            catalog_version,
            shards,
        })
    }

    pub fn validate_shard(
        &self,
        shard_id: ShardId,
        placement_epoch: PlacementEpoch,
        backend_generation: BackendGeneration,
        available_applied_index: u64,
    ) -> Result<(), TxnError> {
        let fence = self
            .shards
            .get(&shard_id)
            .ok_or(TxnError::IncompleteSnapshot)?;
        if fence.placement_epoch != placement_epoch {
            return Err(TxnError::StalePlacementEpoch);
        }
        if fence.backend_generation != backend_generation {
            return Err(TxnError::StaleBackendGeneration);
        }
        if available_applied_index < fence.applied_index {
            return Err(TxnError::AppliedIndexUnavailable);
        }
        Ok(())
    }

    pub fn validate_read_time(&self, read_time: TransactionTime) -> Result<(), TxnError> {
        if read_time != self.start_time {
            return Err(TxnError::InconsistentSnapshot);
        }
        if self
            .shards
            .values()
            .any(|fence| read_time > fence.closed_time)
        {
            return Err(TxnError::ReadTimeExceedsClosedTime);
        }
        Ok(())
    }
}
