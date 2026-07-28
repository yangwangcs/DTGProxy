use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use dtgproxy::{DeploymentConfig, ShardPlacement};
use gateway_node::test_support::routed_shard_read_adapter;
use shard_client::{
    AdvanceArtifactFenceRequest, ArtifactChunkStream, ArtifactGenerationHeadPage,
    ArtifactGenerationSummary, DeleteArtifactGenerationRequest, ExecuteCommand, ExecuteReceipt,
    FencedScan, GetArtifactGenerationRequest, ListArtifactGenerationHeadsRequest,
    ListArtifactGenerationsRequest, PinArtifactGenerationRequest, PutArtifactChunkRequest,
    ReadKeysRequest, ScanRequest, ShardClient, ShardClientError, ShardClientFuture,
    ShardRequestContext, ShardStatus,
};
use storage_api::StorageAdapter;
use temporal_ir::GraphScope;
use temporal_storage::{ElementId, ElementRef, GraphId, PartitionId, vertex_identity_key};

#[test]
fn gateway_read_execution_requires_a_precompiled_query() {
    let source = include_str!("../src/service.rs");
    assert!(
        !source.contains("read_prefix: Option<&CompiledQuery>"),
        "the read execution boundary must not expose a compile fallback"
    );
    assert!(
        !source.contains("let owned_compiled;"),
        "Gateway reads must reuse the query compiled at the request boundary"
    );
    assert!(
        !source.contains("let adapter = RoutedShardReadAdapter::new("),
        "a request must build one shared shard-adapter map, not one map per worker"
    );
}

#[derive(Default)]
struct ShardCallGates {
    started: BTreeMap<u32, Arc<AtomicU64>>,
    changed: Arc<tokio::sync::Notify>,
    released: AtomicBool,
    release_changed: tokio::sync::Notify,
}

impl ShardCallGates {
    fn new(shards: impl IntoIterator<Item = u32>) -> Self {
        Self {
            started: shards
                .into_iter()
                .map(|shard| (shard, Arc::new(AtomicU64::new(0))))
                .collect(),
            ..Self::default()
        }
    }

    async fn wait_until_started(&self, shard: u32) {
        loop {
            let notified = self.changed.notified();
            if self.started[&shard].load(Ordering::Acquire) > 0 {
                return;
            }
            notified.await;
        }
    }

    fn release_all(&self) {
        self.released.store(true, Ordering::Release);
        self.release_changed.notify_waiters();
    }

    async fn enter(&self, shard: u32) {
        self.started[&shard].fetch_add(1, Ordering::AcqRel);
        self.changed.notify_waiters();
        loop {
            let notified = self.release_changed.notified();
            if self.released.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }
}

struct GatedShardClient {
    gates: Arc<ShardCallGates>,
}

struct FailingShardClient {
    pending_started: tokio::sync::Notify,
    pending_entered: AtomicBool,
    pending_dropped: Arc<AtomicBool>,
}

struct PendingReadGuard(Arc<AtomicBool>);

impl Drop for PendingReadGuard {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

impl FailingShardClient {
    async fn wait_until_pending(&self) {
        loop {
            let notified = self.pending_started.notified();
            if self.pending_entered.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }
}

impl ShardClient for GatedShardClient {
    fn execute<'a>(&'a self, _request: ExecuteCommand) -> ShardClientFuture<'a, ExecuteReceipt> {
        unsupported()
    }

    fn read_keys<'a>(
        &'a self,
        request: ReadKeysRequest,
    ) -> ShardClientFuture<'a, Vec<Option<Vec<u8>>>> {
        Box::pin(async move {
            let shard = request.context().shard_id();
            self.gates.enter(shard).await;
            Ok(request
                .keys()
                .iter()
                .map(|key| {
                    let mut value = shard.to_be_bytes().to_vec();
                    value.extend_from_slice(key.as_bytes());
                    Some(value)
                })
                .collect())
        })
    }

    fn scan<'a>(
        &'a self,
        _request: ScanRequest,
    ) -> ShardClientFuture<'a, Vec<storage_api::KeyValue>> {
        unsupported()
    }

    fn scan_fenced<'a>(&'a self, _request: ScanRequest) -> ShardClientFuture<'a, FencedScan> {
        unsupported()
    }

    fn put_artifact_chunk<'a>(
        &'a self,
        _request: PutArtifactChunkRequest,
    ) -> ShardClientFuture<'a, ExecuteReceipt> {
        unsupported()
    }

    fn pin_artifact_generation<'a>(
        &'a self,
        _request: PinArtifactGenerationRequest,
    ) -> ShardClientFuture<'a, ExecuteReceipt> {
        unsupported()
    }

    fn get_artifact_generation<'a>(
        &'a self,
        _request: GetArtifactGenerationRequest,
    ) -> ShardClientFuture<'a, ArtifactChunkStream> {
        unsupported()
    }

    fn delete_artifact_generation<'a>(
        &'a self,
        _request: DeleteArtifactGenerationRequest,
    ) -> ShardClientFuture<'a, ExecuteReceipt> {
        unsupported()
    }

    fn advance_artifact_fence<'a>(
        &'a self,
        _request: AdvanceArtifactFenceRequest,
    ) -> ShardClientFuture<'a, ExecuteReceipt> {
        unsupported()
    }

    fn list_artifact_generations<'a>(
        &'a self,
        _request: ListArtifactGenerationsRequest,
    ) -> ShardClientFuture<'a, Vec<ArtifactGenerationSummary>> {
        unsupported()
    }

    fn list_artifact_generation_heads<'a>(
        &'a self,
        _request: ListArtifactGenerationHeadsRequest,
    ) -> ShardClientFuture<'a, ArtifactGenerationHeadPage> {
        unsupported()
    }

    fn status<'a>(&'a self, _context: ShardRequestContext) -> ShardClientFuture<'a, ShardStatus> {
        unsupported()
    }
}

impl ShardClient for FailingShardClient {
    fn execute<'a>(&'a self, _request: ExecuteCommand) -> ShardClientFuture<'a, ExecuteReceipt> {
        unsupported()
    }

    fn read_keys<'a>(
        &'a self,
        request: ReadKeysRequest,
    ) -> ShardClientFuture<'a, Vec<Option<Vec<u8>>>> {
        Box::pin(async move {
            if request.context().shard_id() == 10 {
                self.wait_until_pending().await;
                return Err(ShardClientError::Internal("injected shard failure".into()));
            }
            let _guard = PendingReadGuard(Arc::clone(&self.pending_dropped));
            self.pending_entered.store(true, Ordering::Release);
            self.pending_started.notify_waiters();
            std::future::pending().await
        })
    }

    fn scan<'a>(
        &'a self,
        _request: ScanRequest,
    ) -> ShardClientFuture<'a, Vec<storage_api::KeyValue>> {
        unsupported()
    }

    fn scan_fenced<'a>(&'a self, _request: ScanRequest) -> ShardClientFuture<'a, FencedScan> {
        unsupported()
    }

    fn put_artifact_chunk<'a>(
        &'a self,
        _request: PutArtifactChunkRequest,
    ) -> ShardClientFuture<'a, ExecuteReceipt> {
        unsupported()
    }

    fn pin_artifact_generation<'a>(
        &'a self,
        _request: PinArtifactGenerationRequest,
    ) -> ShardClientFuture<'a, ExecuteReceipt> {
        unsupported()
    }

    fn get_artifact_generation<'a>(
        &'a self,
        _request: GetArtifactGenerationRequest,
    ) -> ShardClientFuture<'a, ArtifactChunkStream> {
        unsupported()
    }

    fn delete_artifact_generation<'a>(
        &'a self,
        _request: DeleteArtifactGenerationRequest,
    ) -> ShardClientFuture<'a, ExecuteReceipt> {
        unsupported()
    }

    fn advance_artifact_fence<'a>(
        &'a self,
        _request: AdvanceArtifactFenceRequest,
    ) -> ShardClientFuture<'a, ExecuteReceipt> {
        unsupported()
    }

    fn list_artifact_generations<'a>(
        &'a self,
        _request: ListArtifactGenerationsRequest,
    ) -> ShardClientFuture<'a, Vec<ArtifactGenerationSummary>> {
        unsupported()
    }

    fn list_artifact_generation_heads<'a>(
        &'a self,
        _request: ListArtifactGenerationHeadsRequest,
    ) -> ShardClientFuture<'a, ArtifactGenerationHeadPage> {
        unsupported()
    }

    fn status<'a>(&'a self, _context: ShardRequestContext) -> ShardClientFuture<'a, ShardStatus> {
        unsupported()
    }
}

fn unsupported<'a, T: 'a>() -> ShardClientFuture<'a, T> {
    Box::pin(async { Err(ShardClientError::Internal("unsupported test call".into())) })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gateway_starts_independent_shards_before_releasing_the_first() {
    let (deployment, key_10, key_20) = two_shard_fixture();
    let keys = vec![key_20.clone(), key_10.clone(), key_20.clone()];
    let gates = Arc::new(ShardCallGates::new([10, 20]));
    let client: Arc<dyn ShardClient> = Arc::new(GatedShardClient {
        gates: Arc::clone(&gates),
    });
    let adapter = Arc::new(
        routed_shard_read_adapter(client, 7, 10, deployment, now_ms() + 60_000, 41).unwrap(),
    );
    let task = tokio::spawn({
        let adapter = Arc::clone(&adapter);
        let keys = keys.clone();
        async move { adapter.multi_get(&keys).await }
    });

    gates.wait_until_started(10).await;
    gates.wait_until_started(20).await;
    gates.release_all();

    let values = task.await.unwrap().unwrap();
    assert_eq!(values.len(), keys.len());
    for ((key, value), expected_shard) in keys.iter().zip(values).zip([20_u32, 10, 20]) {
        let mut expected = expected_shard.to_be_bytes().to_vec();
        expected.extend_from_slice(key.as_bytes());
        assert_eq!(value, Some(expected));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gateway_aborts_and_drains_pending_shards_after_the_first_error() {
    let (deployment, key_10, key_20) = two_shard_fixture();
    let pending_dropped = Arc::new(AtomicBool::new(false));
    let client = Arc::new(FailingShardClient {
        pending_started: tokio::sync::Notify::new(),
        pending_entered: AtomicBool::new(false),
        pending_dropped: Arc::clone(&pending_dropped),
    });
    let adapter =
        routed_shard_read_adapter(client.clone(), 7, 10, deployment, now_ms() + 60_000, 42)
            .unwrap();

    let error = adapter.multi_get(&[key_10, key_20]).await.unwrap_err();

    assert!(error.to_string().contains("injected shard failure"));
    assert!(client.pending_entered.load(Ordering::Acquire));
    assert!(pending_dropped.load(Ordering::Acquire));
}

fn two_shard_fixture() -> (
    Arc<DeploymentConfig>,
    storage_api::LogicalKey,
    storage_api::LogicalKey,
) {
    let deployment = Arc::new(
        DeploymentConfig::shared_nothing_with_virtual_partitions(
            99,
            128,
            vec![
                ShardPlacement::new(10, 1, vec![10]).unwrap(),
                ShardPlacement::new(20, 1, vec![20]).unwrap(),
            ],
        )
        .unwrap(),
    );
    let scopes = (0..1_000)
        .map(|partition| {
            let scope = GraphScope::new(GraphId::new(7), PartitionId::new(partition));
            (deployment.route_scope(scope).shard_id(), scope)
        })
        .fold(BTreeMap::new(), |mut scopes, (shard, scope)| {
            scopes.entry(shard).or_insert(scope);
            scopes
        });
    let key_10 = vertex_identity_key(ElementRef::vertex(
        GraphId::new(7),
        scopes[&10].partition(),
        ElementId::new(10),
    ));
    let key_20 = vertex_identity_key(ElementRef::vertex(
        GraphId::new(7),
        scopes[&20].partition(),
        ElementId::new(20),
    ));
    (deployment, key_10, key_20)
}

fn now_ms() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
}
