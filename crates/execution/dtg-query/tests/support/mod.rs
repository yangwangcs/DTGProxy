#![allow(dead_code)]

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use dtg_language_ir::{RowSchema, Value};
use dtg_query::{
    ExecutableAccess, ExecutableFragment, ExecutablePlan, ExecutionFence, LogicalRead,
    QueryStorage, ReadOperation, ResidualPredicate, SnapshotGuard, SnapshotShardFence,
};
use dtg_storage::{
    AdjacencyRead, BackendClass, BindingRole, CapabilityManifest, ChangePage, ChangesRead,
    EdgeHistoryRead, EdgeId, EdgeRead, EdgeScan, EdgeVersion, ProviderKind, PushdownExecutor,
    PushdownOperation, PushdownOutcome, PushdownRequest, ReadFence, ReplicaBinding, ScanPage,
    SnapshotRecord, StorageError, StoreFuture, TemporalReadView, TransactionTime, ValidInterval,
    Version, VertexHistoryRead, VertexId, VertexRead, VertexScan, VertexVersion,
};

pub const CAP_VERTEX_POINT: &str = "read.vertex.point";
pub const CAP_TEMPORAL_EXACT: &str = "semantics.temporal.exact";
pub const CAP_NULL_EXACT: &str = "semantics.null.exact";
pub const CAP_DUPLICATE_EXACT: &str = "semantics.duplicate.exact";
pub const CAP_ORDER_EXACT: &str = "semantics.order.exact";
pub const CAP_SNAPSHOT_EXACT: &str = "semantics.snapshot.exact";

pub const EXACT_VERTEX_POINT_CAPABILITIES: [&str; 6] = [
    CAP_VERTEX_POINT,
    CAP_TEMPORAL_EXACT,
    CAP_NULL_EXACT,
    CAP_DUPLICATE_EXACT,
    CAP_ORDER_EXACT,
    CAP_SNAPSHOT_EXACT,
];

pub fn block_on<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    match future.as_mut().poll(&mut context) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("deterministic query future unexpectedly yielded"),
    }
}

pub fn vertex(id: u128, score: i64) -> VertexVersion {
    VertexVersion::new(
        VertexId::new(id).unwrap(),
        Version::new(1),
        ValidInterval::new(0, 100).unwrap(),
        TransactionTime::new(10).unwrap(),
        BTreeMap::from([("score".into(), Value::Integer(score))]),
    )
    .unwrap()
}

pub fn edge(id: u128, source: u128, target: u128) -> EdgeVersion {
    EdgeVersion::new(
        EdgeId::new(id).unwrap(),
        VertexId::new(source).unwrap(),
        VertexId::new(target).unwrap(),
        "KNOWS",
        Version::new(1),
        ValidInterval::new(0, 100).unwrap(),
        TransactionTime::new(10).unwrap(),
        BTreeMap::new(),
    )
    .unwrap()
}

pub fn binding(capabilities: &CapabilityManifest) -> ReplicaBinding {
    binding_for_shard(capabilities, 13)
}

pub fn binding_for_shard(capabilities: &CapabilityManifest, shard_id: u64) -> ReplicaBinding {
    let class = BackendClass::new(
        ProviderKind::Fjall,
        1,
        1,
        capabilities.names().map(str::to_owned),
    )
    .unwrap();
    ReplicaBinding::builder()
        .cluster_id(1)
        .graph_id(9)
        .shard_id(shard_id)
        .placement_epoch(7)
        .replica_id(19)
        .backend_generation(3)
        .backend_class_digest(class.digest())
        .provider_kind(ProviderKind::Fjall)
        .contract_version(1)
        .layout_version(1)
        .capability_digest(capabilities.digest())
        .namespace_id(format!("graph-9-shard-{shard_id}"))
        .endpoint_profile_ref("fixture-endpoint")
        .credential_ref("fixture-credential")
        .role(BindingRole::Active)
        .build()
        .unwrap()
}

fn execution_fence(capabilities: &CapabilityManifest) -> ExecutionFence {
    execution_fence_for_shard(capabilities, 13)
}

pub fn execution_fence_for_shard(
    capabilities: &CapabilityManifest,
    shard_id: u64,
) -> ExecutionFence {
    let binding = binding_for_shard(capabilities, shard_id);
    ExecutionFence::new(
        ReadFence::new(binding, 29),
        Version::new(11),
        Version::new(5),
        TransactionTime::new(23).unwrap(),
        17,
        true,
    )
    .unwrap()
}

pub fn point_plan(capabilities: CapabilityManifest, id: i64) -> ExecutablePlan {
    point_plan_with_residual(capabilities, id, None)
}

pub fn point_plan_with_residual(
    capabilities: CapabilityManifest,
    id: i64,
    residual_override: Option<ResidualPredicate>,
) -> ExecutablePlan {
    let id = VertexId::new(id as u128).unwrap();
    let fence = execution_fence(&capabilities);
    let access = if capabilities.supports(CAP_VERTEX_POINT) {
        let request = PushdownRequest::new(
            dtg_storage::SUPPORTED_PUSHDOWN_CONTRACT_VERSION,
            fence.read_fence().clone(),
            CapabilityManifest::from_names([CAP_VERTEX_POINT]).unwrap(),
            PushdownOperation::Vertex(VertexRead::new(id, 17, TransactionTime::new(23).unwrap())),
        )
        .unwrap();
        let exact = [
            CAP_TEMPORAL_EXACT,
            CAP_NULL_EXACT,
            CAP_DUPLICATE_EXACT,
            CAP_ORDER_EXACT,
            CAP_SNAPSHOT_EXACT,
        ]
        .into_iter()
        .all(|name| capabilities.supports(name));
        ExecutableAccess::Pushdown {
            request: Box::new(request),
            residual: residual_override
                .or_else(|| (!exact).then_some(ResidualPredicate::StorageSemantics)),
        }
    } else {
        ExecutableAccess::Logical(
            LogicalRead::new(
                ReadOperation::VertexPoint(id),
                1,
                TransactionTime::new(23).unwrap(),
                17,
            )
            .unwrap(),
        )
    };
    ExecutablePlan::new(
        Version::new(1),
        vec![ExecutableFragment::new(1, fence, vec![access]).unwrap()],
        RowSchema::empty(),
    )
    .unwrap()
}

pub fn scan_plan(capabilities: CapabilityManifest, bound: u32) -> ExecutablePlan {
    let fence = execution_fence(&capabilities);
    let access = ExecutableAccess::Logical(
        LogicalRead::new(
            ReadOperation::VertexScan,
            bound,
            TransactionTime::new(23).unwrap(),
            17,
        )
        .unwrap(),
    );
    ExecutablePlan::new(
        Version::new(1),
        vec![ExecutableFragment::new(1, fence, vec![access]).unwrap()],
        RowSchema::empty(),
    )
    .unwrap()
}

pub fn snapshot() -> SnapshotGuard {
    SnapshotGuard::new(
        TransactionTime::new(23).unwrap(),
        Version::new(11),
        vec![(
            dtg_storage::ShardId::new(13).unwrap(),
            SnapshotShardFence {
                placement_epoch: dtg_storage::PlacementEpoch::new(7).unwrap(),
                backend_generation: dtg_storage::BackendGeneration::new(3).unwrap(),
                applied_index: 29,
            },
        )],
    )
    .unwrap()
}

pub struct FixtureStore {
    binding: ReplicaBinding,
    capabilities: CapabilityManifest,
    fence: ReadFence,
    vertices: Vec<VertexVersion>,
    edges: Vec<EdgeVersion>,
    pushdown_outcome: Mutex<Option<PushdownOutcome>>,
    scan_calls: AtomicUsize,
    max_scan_limit: AtomicUsize,
    pushdown_calls: AtomicUsize,
}

impl FixtureStore {
    pub fn new(
        capabilities: CapabilityManifest,
        vertices: Vec<VertexVersion>,
        edges: Vec<EdgeVersion>,
    ) -> Arc<Self> {
        Self::new_for_shard(capabilities, 13, vertices, edges)
    }

    pub fn new_for_shard(
        capabilities: CapabilityManifest,
        shard_id: u64,
        vertices: Vec<VertexVersion>,
        edges: Vec<EdgeVersion>,
    ) -> Arc<Self> {
        let binding = binding_for_shard(&capabilities, shard_id);
        Arc::new(Self {
            fence: ReadFence::new(binding.clone(), 29),
            binding,
            capabilities,
            vertices,
            edges,
            pushdown_outcome: Mutex::new(None),
            scan_calls: AtomicUsize::new(0),
            max_scan_limit: AtomicUsize::new(0),
            pushdown_calls: AtomicUsize::new(0),
        })
    }

    pub fn with_pushdown_outcome(self: &Arc<Self>, outcome: PushdownOutcome) {
        *self.pushdown_outcome.lock().unwrap() = Some(outcome);
    }

    pub fn storage(self: &Arc<Self>, with_pushdown: bool) -> QueryStorage {
        let view: Arc<dyn TemporalReadView> = self.clone();
        let storage = QueryStorage::new(view);
        if with_pushdown {
            let executor: Arc<dyn PushdownExecutor> = self.clone();
            storage.with_pushdown(executor)
        } else {
            storage
        }
    }

    pub fn scan_calls(&self) -> usize {
        self.scan_calls.load(Ordering::SeqCst)
    }

    pub fn max_scan_limit(&self) -> usize {
        self.max_scan_limit.load(Ordering::SeqCst)
    }

    pub fn pushdown_calls(&self) -> usize {
        self.pushdown_calls.load(Ordering::SeqCst)
    }
}

impl TemporalReadView for FixtureStore {
    fn fence(&self) -> &ReadFence {
        &self.fence
    }

    fn get_vertex(&self, request: VertexRead) -> StoreFuture<'_, Option<VertexVersion>> {
        let row = self.vertices.iter().find(|vertex| {
            vertex.id() == request.id()
                && vertex.valid_time().start() <= request.valid_at()
                && request.valid_at() < vertex.valid_time().end()
                && vertex.transaction_time() <= request.transaction_at()
        });
        Box::pin(std::future::ready(Ok(row.cloned())))
    }

    fn get_edge(&self, request: EdgeRead) -> StoreFuture<'_, Option<EdgeVersion>> {
        let row = self.edges.iter().find(|edge| {
            edge.id() == request.id()
                && edge.valid_time().start() <= request.valid_at()
                && request.valid_at() < edge.valid_time().end()
                && edge.transaction_time() <= request.transaction_at()
        });
        Box::pin(std::future::ready(Ok(row.cloned())))
    }

    fn vertex_history(&self, _request: VertexHistoryRead) -> StoreFuture<'_, Vec<VertexVersion>> {
        Box::pin(std::future::ready(Err(StorageError::Unsupported)))
    }

    fn edge_history(&self, _request: EdgeHistoryRead) -> StoreFuture<'_, Vec<EdgeVersion>> {
        Box::pin(std::future::ready(Err(StorageError::Unsupported)))
    }

    fn expand(&self, request: AdjacencyRead) -> StoreFuture<'_, Vec<EdgeVersion>> {
        let rows = self
            .edges
            .iter()
            .filter(|edge| request.matches(edge))
            .take(request.limit() as usize)
            .cloned()
            .collect();
        Box::pin(std::future::ready(Ok(rows)))
    }

    fn changes(&self, _request: ChangesRead) -> StoreFuture<'_, ChangePage> {
        Box::pin(std::future::ready(Err(StorageError::Unsupported)))
    }

    fn scan_vertices(
        &self,
        request: VertexScan,
    ) -> StoreFuture<'_, ScanPage<VertexVersion, VertexId>> {
        self.scan_calls.fetch_add(1, Ordering::SeqCst);
        self.max_scan_limit
            .fetch_max(request.limit() as usize, Ordering::SeqCst);
        let mut visible: Vec<_> = self
            .vertices
            .iter()
            .filter(|vertex| {
                request.after().is_none_or(|after| vertex.id() > after)
                    && vertex.valid_time().start() <= request.valid_at()
                    && request.valid_at() < vertex.valid_time().end()
                    && vertex.transaction_time() <= request.transaction_at()
            })
            .cloned()
            .collect();
        visible.sort_by_key(VertexVersion::id);
        let more = visible.len() > request.limit() as usize;
        visible.truncate(request.limit() as usize);
        let next = more.then(|| visible.last().unwrap().id());
        Box::pin(std::future::ready(Ok(ScanPage::new(visible, next))))
    }

    fn scan_edges(&self, _request: EdgeScan) -> StoreFuture<'_, ScanPage<EdgeVersion, EdgeId>> {
        Box::pin(std::future::ready(Err(StorageError::Unsupported)))
    }
}

impl PushdownExecutor for FixtureStore {
    fn binding(&self) -> &ReplicaBinding {
        &self.binding
    }

    fn capabilities(&self) -> &CapabilityManifest {
        &self.capabilities
    }

    fn execute_pushdown(&self, _request: PushdownRequest) -> StoreFuture<'_, PushdownOutcome> {
        self.pushdown_calls.fetch_add(1, Ordering::SeqCst);
        let outcome = self
            .pushdown_outcome
            .lock()
            .unwrap()
            .clone()
            .unwrap_or(PushdownOutcome::Unsupported);
        Box::pin(std::future::ready(Ok(outcome)))
    }
}

pub fn storage_map(storage: QueryStorage) -> BTreeMap<dtg_storage::ShardId, QueryStorage> {
    BTreeMap::from([(dtg_storage::ShardId::new(13).unwrap(), storage)])
}

pub fn records(vertices: &[VertexVersion]) -> Vec<SnapshotRecord> {
    vertices
        .iter()
        .cloned()
        .map(SnapshotRecord::Vertex)
        .collect()
}
