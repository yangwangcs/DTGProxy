use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use storage_api::{
    AdapterCapabilities, AdapterDescriptorV1, AdapterError, AdapterFuture, ApplyReceipt,
    BackendFamily, CandidateScanPage, CandidateScanRequest, CommittedMutationBatch, Durability,
    FencedScan as AdapterFencedScan, KeySpan, KeyValue, LogicalKey, PushdownGuarantee,
    QueryCapabilitySnapshot, QueryPrimitiveCapabilities, ReadSnapshot, SnapshotCapability,
    StorageAdapter,
};

use crate::{
    CandidateScanCommand, FencedScan, ReadKeysRequest, ScanRequest, ShardClient, ShardClientError,
    ShardRequestContext,
};

pub struct ShardClientStorageAdapter {
    client: Arc<dyn ShardClient>,
    graph_id: u64,
    shard_id: u32,
    placement_epoch: u64,
    deadline_unix_ms: u64,
    request_namespace: u64,
    sequence: AtomicU64,
    observed_applied_index: AtomicU64,
    query_capabilities: QueryCapabilitySnapshot,
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
            query_capabilities: QueryCapabilitySnapshot::new(1, QueryPrimitiveCapabilities::NONE),
        })
    }

    pub fn with_query_capabilities(
        mut self,
        query_capabilities: QueryCapabilitySnapshot,
    ) -> Result<Self, AdapterError> {
        if query_capabilities.generation() == 0 {
            return Err(AdapterError::InvalidCapabilityGeneration { generation: 0 });
        }
        self.query_capabilities = query_capabilities;
        Ok(self)
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

    #[must_use]
    pub const fn shard_id(&self) -> u32 {
        self.shard_id
    }

    async fn perform_fenced_scan(&self, span: &KeySpan) -> Result<FencedScan, AdapterError> {
        let context = self.context()?;
        let result = self
            .client
            .scan_fenced(ScanRequest::new(context, span.clone()))
            .await
            .map_err(|error| match error {
                ShardClientError::ScanByteLimit { limit, required } => {
                    AdapterError::ScanByteLimit { limit, required }
                }
                other => adapter_read_error(other),
            })?;
        self.observed_applied_index
            .fetch_max(result.applied_index(), Ordering::AcqRel);
        Ok(result)
    }
}

fn adapter_read_error(error: ShardClientError) -> AdapterError {
    if let ShardClientError::UnsupportedQueryPrimitive(operation) = error {
        return AdapterError::UnsupportedOperation { operation };
    }
    if matches!(
        error,
        ShardClientError::DeadlineExpired
            | ShardClientError::NoLeader { .. }
            | ShardClientError::NotLeader { .. }
            | ShardClientError::Replication(_)
            | ShardClientError::ReadBarrier(_)
    ) {
        return AdapterError::Unavailable(error.to_string());
    }
    AdapterError::Backend(error.to_string())
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

    fn query_primitive_capabilities(&self) -> QueryPrimitiveCapabilities {
        self.query_capabilities.capabilities()
    }

    fn query_capability_generation(&self) -> u64 {
        self.query_capabilities.generation()
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
                .map_err(adapter_read_error)
        })
    }

    fn scan<'a>(&'a self, span: &'a KeySpan) -> AdapterFuture<'a, Vec<KeyValue>> {
        Box::pin(async move { Ok(self.perform_fenced_scan(span).await?.into_entries()) })
    }

    fn scan_fenced<'a>(&'a self, span: &'a KeySpan) -> AdapterFuture<'a, AdapterFencedScan> {
        Box::pin(async move {
            let result = self.perform_fenced_scan(span).await?;
            Ok(AdapterFencedScan::new(
                result.applied_index(),
                result.into_entries(),
            ))
        })
    }

    fn scan_candidates<'a>(
        &'a self,
        request: &'a CandidateScanRequest,
    ) -> AdapterFuture<'a, CandidateScanPage> {
        Box::pin(async move {
            let context = self.context()?;
            let page = self
                .client
                .scan_candidates(CandidateScanCommand::new(context, request.clone()))
                .await
                .map_err(adapter_read_error)?;
            self.observed_applied_index
                .fetch_max(page.applied_log_index(), Ordering::AcqRel);
            Ok(page)
        })
    }

    fn applied_log_index(&self) -> Result<u64, AdapterError> {
        Ok(self.observed_applied_index.load(Ordering::Acquire))
    }

    fn begin_read_snapshot<'a>(&'a self) -> AdapterFuture<'a, Box<dyn ReadSnapshot + 'a>> {
        Box::pin(async move {
            if self.query_capabilities.capabilities().candidate_scan()
                == PushdownGuarantee::Unsupported
            {
                return Err(AdapterError::UnsupportedOperation {
                    operation: "remote query read snapshot without negotiated CandidateScan",
                });
            }
            let read_index = self
                .client
                .read_barrier(self.context()?)
                .await
                .map_err(adapter_read_error)?;
            self.observed_applied_index
                .fetch_max(read_index, Ordering::AcqRel);
            Ok(Box::new(RemoteReadSnapshot {
                adapter: self,
                read_index,
            }) as Box<dyn ReadSnapshot + 'a>)
        })
    }
}

struct RemoteReadSnapshot<'a> {
    adapter: &'a ShardClientStorageAdapter,
    read_index: u64,
}

impl RemoteReadSnapshot<'_> {
    fn require_fixed_index(&self, actual: u64) -> Result<(), AdapterError> {
        if actual != self.read_index {
            return Err(AdapterError::Unavailable(format!(
                "remote query read view moved from ReadIndex {} to {actual}",
                self.read_index
            )));
        }
        Ok(())
    }
}

impl ReadSnapshot for RemoteReadSnapshot<'_> {
    fn applied_log_index(&self) -> u64 {
        self.read_index
    }

    fn multi_get<'a>(&'a self, _keys: &'a [LogicalKey]) -> AdapterFuture<'a, Vec<Option<Vec<u8>>>> {
        Box::pin(async {
            Err(AdapterError::UnsupportedOperation {
                operation: "point reads in a remote fixed query snapshot",
            })
        })
    }

    fn scan<'a>(&'a self, span: &'a KeySpan) -> AdapterFuture<'a, Vec<KeyValue>> {
        Box::pin(async move {
            let page = self.adapter.perform_fenced_scan(span).await?;
            self.require_fixed_index(page.applied_index())?;
            Ok(page.into_entries())
        })
    }

    fn scan_candidates<'a>(
        &'a self,
        request: &'a CandidateScanRequest,
    ) -> AdapterFuture<'a, CandidateScanPage> {
        Box::pin(async move {
            let page = self.adapter.scan_candidates(request).await?;
            self.require_fixed_index(page.applied_log_index())?;
            Ok(page)
        })
    }
}
