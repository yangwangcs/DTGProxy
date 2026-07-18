use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use storage_api::{
    AdapterCapabilities, AdapterDescriptorV1, AdapterError, AdapterFuture, ApplyReceipt,
    BackendFamily, CommittedMutationBatch, Durability, KeySpan, KeyValue, LogicalKey,
    SnapshotCapability, StorageAdapter,
};

use crate::{ReadKeysRequest, ScanRequest, ShardClient, ShardRequestContext};

pub struct ShardClientStorageAdapter {
    client: Arc<dyn ShardClient>,
    graph_id: u64,
    shard_id: u32,
    placement_epoch: u64,
    deadline_unix_ms: u64,
    request_namespace: u64,
    sequence: AtomicU64,
    observed_applied_index: AtomicU64,
}

impl ShardClientStorageAdapter {
    pub fn new(
        client: Arc<dyn ShardClient>,
        graph_id: u64,
        shard_id: u32,
        placement_epoch: u64,
        deadline_unix_ms: u64,
        request_namespace: u64,
    ) -> Result<Self, AdapterError> {
        if graph_id == 0
            || shard_id == 0
            || placement_epoch == 0
            || deadline_unix_ms == 0
            || request_namespace == 0
        {
            return Err(AdapterError::Backend(
                "invalid ShardClient Adapter context".into(),
            ));
        }
        Ok(Self {
            client,
            graph_id,
            shard_id,
            placement_epoch,
            deadline_unix_ms,
            request_namespace,
            sequence: AtomicU64::new(1),
            observed_applied_index: AtomicU64::new(0),
        })
    }

    fn context(&self) -> Result<ShardRequestContext, AdapterError> {
        let sequence = self.sequence.fetch_add(1, Ordering::Relaxed);
        if sequence == u64::MAX {
            return Err(AdapterError::Backend(
                "ShardClient Adapter request IDs exhausted".into(),
            ));
        }
        let request_id = (u128::from(self.request_namespace) << 64) | u128::from(sequence);
        ShardRequestContext::new(
            self.graph_id,
            self.shard_id,
            self.placement_epoch,
            request_id,
            self.deadline_unix_ms,
        )
        .map_err(|error| AdapterError::Backend(error.to_string()))
    }
}

impl StorageAdapter for ShardClientStorageAdapter {
    fn descriptor(&self) -> AdapterDescriptorV1 {
        AdapterDescriptorV1::new(
            "dtgproxy-shard-client",
            env!("CARGO_PKG_VERSION"),
            BackendFamily::KeyValue,
            self.capabilities(),
        )
    }

    fn capabilities(&self) -> AdapterCapabilities {
        AdapterCapabilities {
            local_atomic_batch: false,
            idempotent_apply: false,
            consistent_multi_get: true,
            ordered_scan: true,
            durable_applied_index: false,
            durability: Durability::Volatile,
            snapshot: SnapshotCapability::None,
            logical_export: false,
            logical_restore: false,
            predicate_pushdown: false,
            adjacency_pushdown: false,
            change_feed: false,
        }
    }

    fn apply_committed<'a>(
        &'a self,
        _batch: CommittedMutationBatch,
    ) -> AdapterFuture<'a, ApplyReceipt> {
        Box::pin(async {
            Err(AdapterError::UnsupportedOperation {
                operation: "apply through read-only ShardClient Adapter",
            })
        })
    }

    fn multi_get<'a>(&'a self, keys: &'a [LogicalKey]) -> AdapterFuture<'a, Vec<Option<Vec<u8>>>> {
        Box::pin(async move {
            let context = self.context()?;
            self.client
                .read_keys(
                    ReadKeysRequest::new(context, keys.to_vec())
                        .map_err(|error| AdapterError::Backend(error.to_string()))?,
                )
                .await
                .map_err(|error| AdapterError::Backend(error.to_string()))
        })
    }

    fn scan<'a>(&'a self, span: &'a KeySpan) -> AdapterFuture<'a, Vec<KeyValue>> {
        Box::pin(async move {
            let context = self.context()?;
            self.client
                .scan(ScanRequest::new(context, span.clone()))
                .await
                .map_err(|error| AdapterError::Backend(error.to_string()))
        })
    }

    fn applied_log_index(&self) -> Result<u64, AdapterError> {
        Ok(self.observed_applied_index.load(Ordering::Acquire))
    }
}
