use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

use analytics_api::{
    DeltaGraph, EventGraph, GraphModel, IntervalGraph, PartitionedSnapshotGraph, ProjectedGraph,
    SnapshotEdge, SnapshotGraph, SnapshotPartition, VertexId,
};
use analytics_ledger::{
    ArtifactKind as LedgerArtifactKind, ArtifactManifest, GraphProjectionScope, JobRecord,
    ProjectionLimits as JobProjectionLimits, decode_algorithm_result_artifact,
};
use analytics_runtime::{
    BuiltInProvider, ProjectionLimits, project_event_part_bounded, project_interval_part_bounded,
    project_snapshot_identity_part_bounded, project_valid_time_delta_part_bounded,
};
use cluster_protocol::proto::gateway_service_server::GatewayService;
use cluster_protocol::proto::{GatewaySubmitRequest, GatewaySubmitResponse};
use cluster_protocol::{CommonRequestContext, MAX_COMMAND_BYTES};
use control_plane::GraphDefinition;
use cypher_compiler::{CompileSession, CompiledMutation, CompiledQuery, CypherCompiler};
#[cfg(test)]
use cypher_engine::materialize_write;
use cypher_engine::{
    BackendFuture, BackendQueryResult, BoltQueryBackend, BoltQueryRequest, CypherQueryEngine,
    CypherQueryRequest, CypherQueryResponse, DeploymentMode as QueryDeploymentMode, EngineConfig,
    MaterializedElement, MaterializedElementKind, ResourceLimits, WriteContext, WriteSubqueryInput,
    WriteSubqueryRow, materialize_write_with_subquery_inputs,
    probe_merge_constraints_with_subquery_inputs, resolve_compiled_temporal_scope,
};
use distributed_query::{DistributedCoordinator, LocalFragmentWorker};
use dtgproxy::gateway::{GATEWAY_API_VERSION, GatewayOperation, GatewayRequest};
use dtgproxy::{DeploymentConfig, DeploymentMode, TransactionContext, TransactionCoordinator};
use procedure_runtime::{
    ClusterAnalyticsCoordinator, ClusterAnalyticsError, JobInvocationContext, ProcedureAccess,
    ProcedureRegistry,
};
use query_executor::{
    GraphOverlay, GraphOverlayEntry, RuntimeValue, TemporalBatchExecutor, VertexRecord,
    preflight_procedure_parameters,
};
use serde_json::{Map, Value, json};
use shard_client::{
    ArtifactKind as ShardArtifactKind, GetArtifactGenerationRequest, ReadKeysRequest,
    RemoteShardClient, ShardClient, ShardClientStorageAdapter, ShardRequestContext,
};
use storage_api::{
    AdapterCapabilities, AdapterDescriptorV1, AdapterError, AdapterFuture, ApplyReceipt,
    CandidateScanPage, CandidateScanRequest, CommittedMutationBatch, KeySpan, KeyValue, LogicalKey,
    QueryPrimitiveCapabilities, StorageAdapter,
};
use temporal_storage::{
    TemporalStore, TransactionOverlay, decode_graph_key, graph_key_prefix_scope, graph_key_scope,
};
use temporal_types::{Interval, TransactionTime, ValidTime};
use timestamp_oracle::advance_timestamp;
use tokio::task::JoinSet;
use tokio_stream::StreamExt;
use tonic::{Request, Response, Status};

static NEXT_GATEWAY_REQUEST_NONCE: AtomicU64 = AtomicU64::new(1);
use txn_protocol::{IsolationLevel, MAX_TRANSACTION_MUTATIONS};

use crate::analytics_coordinator::{
    AnalyticsResultReader, MetaClusterAnalyticsCoordinator, ResultReaderFuture,
    page_algorithm_result,
};
use crate::analytics_scheduler::{
    AnalyticsFaultInjector, AnalyticsScheduler, AnalyticsSchedulerMetricsSnapshot,
};
use crate::meta_client::MetaTimestampClient;
use crate::{AdmissionController, AdmissionError};
#[cfg(feature = "paper-benchmark-control")]
use crate::{BenchmarkAblationRuntime, BenchmarkQueryLease};

#[derive(Clone)]
pub struct RemoteGatewayService {
    cluster_id: [u8; 16],
    routing: Arc<RwLock<GatewayRoutingState>>,
    shard_client: Arc<RemoteShardClient>,
    meta_timestamp_client: MetaTimestampClient,
    admission: Arc<AdmissionController>,
    max_raft_ticks: usize,
    bolt_request_nonce: u64,
    bolt_request_sequence: Arc<AtomicU64>,
    bolt_transactions: Arc<Mutex<BTreeMap<u64, PendingBoltTransaction>>>,
    procedure_registry: Arc<ProcedureRegistry>,
    _scheduler: Option<Arc<AnalyticsScheduler>>,
    #[cfg(feature = "paper-benchmark-control")]
    benchmark_ablations: Option<Arc<BenchmarkAblationRuntime>>,
}

#[derive(Clone)]
pub(crate) struct GatewayRoutingState {
    pub(crate) revision: u64,
    pub(crate) graph: GraphDefinition,
    pub(crate) deployment: Arc<DeploymentConfig>,
}

struct GatewayAnalyticsResultReader {
    shard_client: Arc<dyn ShardClient>,
    routing: Arc<RwLock<GatewayRoutingState>>,
}

impl AnalyticsResultReader for GatewayAnalyticsResultReader {
    fn read<'a>(
        &'a self,
        record: &'a JobRecord,
        manifest: &'a ArtifactManifest,
        invocation_request_id: u128,
        offset: usize,
        limit: usize,
        deadline_unix_ms: u64,
    ) -> ResultReaderFuture<'a> {
        Box::pin(async move {
            let spec = record.spec();
            let (graph_id, placement_epoch, shard_id) = {
                let routing = self.routing.read().map_err(|_| {
                    analytics_reader_error("gateway routing state lock is poisoned")
                })?;
                if spec.graph_id() != routing.graph.graph_id()
                    || spec.topology_epoch() != routing.graph.topology().epoch()
                    || spec.schema_version() != routing.graph.schema_version()
                    || spec.backend_generation() != routing.graph.backend().generation()
                {
                    return Err(analytics_reader_error(
                        "analytics result topology or backend fence is stale",
                    ));
                }
                if manifest.kind() != LedgerArtifactKind::Result || manifest.storage_shard_id() == 0
                {
                    return Err(analytics_reader_error(
                        "analytics result manifest has an invalid artifact kind or storage Shard",
                    ));
                }
                let placement = routing
                    .deployment
                    .all_shards()
                    .iter()
                    .find(|placement: &&dtgproxy::ShardPlacement| {
                        placement.shard_id() == manifest.storage_shard_id()
                    })
                    .ok_or_else(|| {
                        analytics_reader_error("analytics result storage Shard is absent")
                    })?;
                (
                    routing.graph.graph_id(),
                    placement.placement_epoch(),
                    placement.shard_id(),
                )
            };
            let request_id = result_read_request_id(
                invocation_request_id,
                spec.job_id().value(),
                manifest.generation(),
                shard_id,
            );
            let context = ShardRequestContext::new(
                graph_id,
                shard_id,
                placement_epoch,
                request_id,
                deadline_unix_ms,
            )
            .map_err(|error| analytics_reader_error(error.to_string()))?;
            let chunk_count = u16::try_from(manifest.chunk_count()).map_err(|_| {
                analytics_reader_error("analytics result chunk count overflows Shard API")
            })?;
            let request = GetArtifactGenerationRequest::new(
                context,
                spec.job_id().value(),
                ShardArtifactKind::Result,
                manifest.generation(),
                chunk_count,
                manifest.total_bytes(),
                manifest.content_digest(),
            )
            .map_err(|error| analytics_reader_error(error.to_string()))?;
            let mut stream = self
                .shard_client
                .get_artifact_generation(request)
                .await
                .map_err(|error| analytics_reader_error(error.to_string()))?;
            let capacity = usize::try_from(manifest.total_bytes()).map_err(|_| {
                analytics_reader_error("analytics result size overflows memory bound")
            })?;
            let mut bytes = Vec::with_capacity(capacity);
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.map_err(|error: shard_client::ShardClientError| {
                    analytics_reader_error(error.to_string())
                })?;
                bytes.extend_from_slice(chunk.payload());
            }
            let result = decode_algorithm_result_artifact(&bytes)
                .map_err(|error| analytics_reader_error(error.to_string()))?;
            page_algorithm_result(result, offset, limit)
        })
    }
}

struct ProcedureProjection {
    graph: Option<Arc<ProjectedGraph>>,
    scope: GraphProjectionScope,
    limits: JobProjectionLimits,
    transaction_snapshot: TransactionTime,
}

impl ProcedureProjection {
    fn with_graph(
        graph: Arc<ProjectedGraph>,
        scope: GraphProjectionScope,
        limits: JobProjectionLimits,
        transaction_snapshot: TransactionTime,
    ) -> Self {
        Self {
            graph: Some(graph),
            scope,
            limits,
            transaction_snapshot,
        }
    }
}

pub(crate) struct RoutedShardReadAdapter {
    graph_id: u64,
    local_shard_id: u32,
    deployment: Arc<DeploymentConfig>,
    adapters: Arc<BTreeMap<u32, Arc<ShardClientStorageAdapter>>>,
}

const MAX_INFLIGHT_SHARD_READS: usize = 16;

impl RoutedShardReadAdapter {
    #[allow(dead_code)]
    pub(crate) fn new(
        client: Arc<dyn ShardClient>,
        graph_id: u64,
        local_shard_id: u32,
        deployment: Arc<DeploymentConfig>,
        deadline_unix_ms: u64,
        request_id: u128,
    ) -> Result<Self, AdapterError> {
        let adapters =
            Self::build_adapters(client, graph_id, &deployment, deadline_unix_ms, request_id)?;
        Self::with_shared_adapters(graph_id, local_shard_id, deployment, adapters)
    }

    fn build_adapters(
        client: Arc<dyn ShardClient>,
        graph_id: u64,
        deployment: &DeploymentConfig,
        deadline_unix_ms: u64,
        request_id: u128,
    ) -> Result<Arc<BTreeMap<u32, Arc<ShardClientStorageAdapter>>>, AdapterError> {
        let mut adapters = BTreeMap::new();
        for placement in deployment.all_shards() {
            adapters.insert(
                placement.shard_id(),
                Arc::new(ShardClientStorageAdapter::new(
                    Arc::clone(&client),
                    graph_id,
                    placement.shard_id(),
                    placement.placement_epoch(),
                    deadline_unix_ms,
                    request_namespace(request_id, placement.shard_id()),
                )?),
            );
        }
        Ok(Arc::new(adapters))
    }

    async fn build_negotiated_adapters(
        client: Arc<dyn ShardClient>,
        graph_id: u64,
        deployment: &DeploymentConfig,
        deadline_unix_ms: u64,
        request_id: u128,
    ) -> Result<Arc<BTreeMap<u32, Arc<ShardClientStorageAdapter>>>, AdapterError> {
        let mut tasks = JoinSet::new();
        for placement in deployment.all_shards() {
            let client = Arc::clone(&client);
            let shard_id = placement.shard_id();
            let placement_epoch = placement.placement_epoch();
            tasks.spawn(async move {
                let context = ShardRequestContext::new(
                    graph_id,
                    shard_id,
                    placement_epoch,
                    u128::from(request_namespace(
                        request_id ^ 0x4454_475f_4341_5053,
                        shard_id,
                    )),
                    deadline_unix_ms,
                )
                .map_err(|error| AdapterError::Backend(error.to_string()))?;
                let capabilities = client
                    .status(context)
                    .await
                    .map_err(|error| AdapterError::Unavailable(error.to_string()))?
                    .query_capabilities();
                let adapter = ShardClientStorageAdapter::new(
                    client,
                    graph_id,
                    shard_id,
                    placement_epoch,
                    deadline_unix_ms,
                    request_namespace(request_id, shard_id),
                )?
                .with_query_capabilities(capabilities)?;
                Ok::<_, AdapterError>((shard_id, Arc::new(adapter)))
            });
        }
        let mut adapters = BTreeMap::new();
        while let Some(result) = tasks.join_next().await {
            let (shard_id, adapter) = result.map_err(|error| {
                AdapterError::Backend(format!("query capability negotiation task failed: {error}"))
            })??;
            adapters.insert(shard_id, adapter);
        }
        Ok(Arc::new(adapters))
    }

    fn with_shared_adapters(
        graph_id: u64,
        local_shard_id: u32,
        deployment: Arc<DeploymentConfig>,
        adapters: Arc<BTreeMap<u32, Arc<ShardClientStorageAdapter>>>,
    ) -> Result<Self, AdapterError> {
        if !adapters.contains_key(&local_shard_id) {
            return Err(AdapterError::Backend(
                "local query shard is absent from the deployment".into(),
            ));
        }
        Ok(Self {
            graph_id,
            local_shard_id,
            deployment,
            adapters,
        })
    }

    fn local(&self) -> &ShardClientStorageAdapter {
        self.adapters
            .get(&self.local_shard_id)
            .expect("validated routed Adapter has its local Shard")
            .as_ref()
    }

    fn adapter_for_key(
        &self,
        key: &LogicalKey,
    ) -> Result<&ShardClientStorageAdapter, AdapterError> {
        let graph_key =
            decode_graph_key(key).map_err(|error| AdapterError::Backend(error.to_string()))?;
        let (graph, partition) = graph_key_scope(graph_key);
        if graph.value() != self.graph_id {
            return Err(AdapterError::Backend(
                "query key graph does not match the routed Adapter graph".into(),
            ));
        }
        let shard_id = self
            .deployment
            .route_scope(temporal_ir::GraphScope::new(graph, partition))
            .shard_id();
        self.adapters
            .get(&shard_id)
            .map(AsRef::as_ref)
            .ok_or_else(|| AdapterError::Backend("routed query Shard is unavailable".into()))
    }

    fn adapter_for_span(&self, span: &KeySpan) -> Result<&ShardClientStorageAdapter, AdapterError> {
        let Some((graph, partition)) = graph_key_prefix_scope(span.keyspace(), span.start())
            .map_err(|error| AdapterError::Backend(error.to_string()))?
        else {
            return Ok(self.local());
        };
        if graph.value() != self.graph_id {
            return Err(AdapterError::Backend(
                "query span graph does not match the routed Adapter graph".into(),
            ));
        }
        let shard_id = self
            .deployment
            .route_scope(temporal_ir::GraphScope::new(graph, partition))
            .shard_id();
        self.adapters
            .get(&shard_id)
            .map(AsRef::as_ref)
            .ok_or_else(|| AdapterError::Backend("routed query Shard is unavailable".into()))
    }
}

impl StorageAdapter for RoutedShardReadAdapter {
    fn descriptor(&self) -> AdapterDescriptorV1 {
        self.local().descriptor()
    }

    fn capabilities(&self) -> AdapterCapabilities {
        self.local().capabilities()
    }

    fn query_primitive_capabilities(&self) -> QueryPrimitiveCapabilities {
        self.local().query_primitive_capabilities()
    }

    fn query_capability_generation(&self) -> u64 {
        self.local().query_capability_generation()
    }

    fn apply_committed<'a>(
        &'a self,
        batch: CommittedMutationBatch,
    ) -> AdapterFuture<'a, ApplyReceipt> {
        self.local().apply_committed(batch)
    }

    fn multi_get<'a>(&'a self, keys: &'a [LogicalKey]) -> AdapterFuture<'a, Vec<Option<Vec<u8>>>> {
        Box::pin(async move {
            let mut grouped = BTreeMap::<u32, Vec<(usize, LogicalKey)>>::new();
            for (index, key) in keys.iter().enumerate() {
                let adapter = self.adapter_for_key(key)?;
                grouped
                    .entry(adapter.shard_id())
                    .or_default()
                    .push((index, key.clone()));
            }
            let mut output = vec![None; keys.len()];
            let mut groups = grouped.into_iter();
            let mut tasks = JoinSet::new();
            for _ in 0..MAX_INFLIGHT_SHARD_READS {
                let Some((shard_id, indexed_keys)) = groups.next() else {
                    break;
                };
                spawn_shard_multi_get(&mut tasks, &self.adapters, shard_id, indexed_keys)?;
            }
            while let Some(result) = tasks.join_next().await {
                let (indexed_keys, values) = match result {
                    Ok(Ok(result)) => result,
                    Ok(Err(error)) => {
                        abort_and_drain(&mut tasks).await;
                        return Err(error);
                    }
                    Err(error) => {
                        abort_and_drain(&mut tasks).await;
                        return Err(AdapterError::Backend(format!(
                            "routed multi-get task failed: {error}"
                        )));
                    }
                };
                if values.len() != indexed_keys.len() {
                    abort_and_drain(&mut tasks).await;
                    return Err(AdapterError::Backend(
                        "routed multi-get returned the wrong result count".into(),
                    ));
                }
                for ((index, _), value) in indexed_keys.into_iter().zip(values) {
                    output[index] = value;
                }
                if let Some((shard_id, indexed_keys)) = groups.next() {
                    spawn_shard_multi_get(&mut tasks, &self.adapters, shard_id, indexed_keys)?;
                }
            }
            Ok(output)
        })
    }

    fn scan<'a>(&'a self, span: &'a KeySpan) -> AdapterFuture<'a, Vec<KeyValue>> {
        Box::pin(async move { self.adapter_for_span(span)?.scan(span).await })
    }

    fn scan_fenced<'a>(&'a self, span: &'a KeySpan) -> AdapterFuture<'a, storage_api::FencedScan> {
        Box::pin(async move { self.adapter_for_span(span)?.scan_fenced(span).await })
    }

    fn scan_candidates<'a>(
        &'a self,
        request: &'a CandidateScanRequest,
    ) -> AdapterFuture<'a, CandidateScanPage> {
        Box::pin(async move {
            self.adapter_for_span(request.span())?
                .scan_candidates(request)
                .await
        })
    }

    fn applied_log_index(&self) -> Result<u64, AdapterError> {
        self.local().applied_log_index()
    }
}

type IndexedShardValues = (Vec<(usize, LogicalKey)>, Vec<Option<Vec<u8>>>);

fn spawn_shard_multi_get(
    tasks: &mut JoinSet<Result<IndexedShardValues, AdapterError>>,
    adapters: &BTreeMap<u32, Arc<ShardClientStorageAdapter>>,
    shard_id: u32,
    indexed_keys: Vec<(usize, LogicalKey)>,
) -> Result<(), AdapterError> {
    let adapter = Arc::clone(
        adapters
            .get(&shard_id)
            .ok_or_else(|| AdapterError::Backend("routed query Shard is unavailable".into()))?,
    );
    tasks.spawn(async move {
        let request_keys = indexed_keys
            .iter()
            .map(|(_, key)| key.clone())
            .collect::<Vec<_>>();
        let values = adapter.multi_get(&request_keys).await?;
        Ok((indexed_keys, values))
    });
    Ok(())
}

async fn abort_and_drain<T: 'static>(tasks: &mut JoinSet<T>) {
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
}

#[derive(Clone)]
struct PreparedCypherWrite {
    context: TransactionContext,
    scoped: Vec<dtgproxy::ScopedTemporalTransaction>,
    bindings: Map<String, Value>,
    overlay_values: Vec<RuntimeValue>,
    graph_overlay_entries: Vec<GraphOverlayEntry>,
    fingerprint: [u8; 32],
    constraints: Vec<dtgproxy::RoutedConstraintClaim>,
}

struct PendingBoltTransaction {
    routing: GatewayRoutingState,
    deadline_unix_ms: u64,
    start_ts: TransactionTime,
    commit_ts: TransactionTime,
    writes: Vec<PreparedCypherWrite>,
    graph_overlay: GraphOverlay,
    revision: u64,
}

impl RemoteGatewayService {
    /// Returns process-local scheduler counters for diagnostics and metrics
    /// exporters. Meta/Shard state remains authoritative for correctness.
    pub fn analytics_scheduler_metrics(&self) -> Option<AnalyticsSchedulerMetricsSnapshot> {
        self._scheduler
            .as_ref()
            .map(|scheduler| scheduler.metrics())
    }

    #[cfg(feature = "paper-benchmark-control")]
    #[must_use]
    pub fn with_benchmark_ablation_runtime(
        mut self,
        runtime: Arc<BenchmarkAblationRuntime>,
    ) -> Self {
        self.benchmark_ablations = Some(runtime);
        self
    }

    #[cfg(feature = "paper-benchmark-control")]
    fn acquire_benchmark_query(
        &self,
        token: Option<&str>,
        transaction: Option<bolt_server::TransactionId>,
        read_only: bool,
    ) -> Result<Option<BenchmarkQueryLease>, bolt_server::ServiceError> {
        let Some(runtime) = self.benchmark_ablations.as_ref() else {
            if token.is_some() {
                return Err(benchmark_request_error(
                    "benchmark session control is not enabled by this Gateway",
                ));
            }
            return Ok(None);
        };
        let active = runtime.has_active_session().map_err(benchmark_bolt_error)?;
        let Some(token) = token else {
            if active {
                return Err(benchmark_request_error(
                    "an active benchmark cell requires dtgproxy.paper.session",
                ));
            }
            return Ok(None);
        };
        if transaction.is_some() || !read_only {
            return Err(benchmark_request_error(
                "benchmark sessions require auto-commit read-only queries",
            ));
        }
        runtime
            .acquire(token)
            .map(Some)
            .map_err(benchmark_bolt_error)
    }

    pub fn new(
        cluster_id: [u8; 16],
        graph: GraphDefinition,
        shard_client: Arc<RemoteShardClient>,
        meta_endpoints: Vec<SocketAddr>,
        maximum_inflight: usize,
        max_raft_ticks: usize,
    ) -> Result<Self, RemoteGatewayServiceError> {
        Self::new_at_revision(
            cluster_id,
            1,
            graph,
            shard_client,
            meta_endpoints,
            maximum_inflight,
            max_raft_ticks,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_at_revision(
        cluster_id: [u8; 16],
        catalog_revision: u64,
        graph: GraphDefinition,
        shard_client: Arc<RemoteShardClient>,
        meta_endpoints: Vec<SocketAddr>,
        maximum_inflight: usize,
        max_raft_ticks: usize,
    ) -> Result<Self, RemoteGatewayServiceError> {
        Self::new_at_revision_internal(
            None,
            cluster_id,
            catalog_revision,
            graph,
            shard_client,
            meta_endpoints,
            maximum_inflight,
            max_raft_ticks,
            Duration::ZERO,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_at_revision_with_gateway_id(
        gateway_id: u64,
        cluster_id: [u8; 16],
        catalog_revision: u64,
        graph: GraphDefinition,
        shard_client: Arc<RemoteShardClient>,
        meta_endpoints: Vec<SocketAddr>,
        maximum_inflight: usize,
        max_raft_ticks: usize,
    ) -> Result<Self, RemoteGatewayServiceError> {
        if gateway_id == 0 {
            return Err(RemoteGatewayServiceError::InvalidConfiguration);
        }
        Self::new_at_revision_internal(
            Some(gateway_id),
            cluster_id,
            catalog_revision,
            graph,
            shard_client,
            meta_endpoints,
            maximum_inflight,
            max_raft_ticks,
            Duration::ZERO,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_at_revision_with_gateway_id_and_delay(
        gateway_id: u64,
        scheduler_execution_delay: Duration,
        cluster_id: [u8; 16],
        catalog_revision: u64,
        graph: GraphDefinition,
        shard_client: Arc<RemoteShardClient>,
        meta_endpoints: Vec<SocketAddr>,
        maximum_inflight: usize,
        max_raft_ticks: usize,
    ) -> Result<Self, RemoteGatewayServiceError> {
        if gateway_id == 0 {
            return Err(RemoteGatewayServiceError::InvalidConfiguration);
        }
        Self::new_at_revision_internal(
            Some(gateway_id),
            cluster_id,
            catalog_revision,
            graph,
            shard_client,
            meta_endpoints,
            maximum_inflight,
            max_raft_ticks,
            scheduler_execution_delay,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_at_revision_with_gateway_id_and_delay_and_fault_injector(
        gateway_id: u64,
        scheduler_execution_delay: Duration,
        fault_injector: Arc<dyn AnalyticsFaultInjector>,
        cluster_id: [u8; 16],
        catalog_revision: u64,
        graph: GraphDefinition,
        shard_client: Arc<RemoteShardClient>,
        meta_endpoints: Vec<SocketAddr>,
        maximum_inflight: usize,
        max_raft_ticks: usize,
    ) -> Result<Self, RemoteGatewayServiceError> {
        if gateway_id == 0 {
            return Err(RemoteGatewayServiceError::InvalidConfiguration);
        }
        Self::new_at_revision_internal(
            Some(gateway_id),
            cluster_id,
            catalog_revision,
            graph,
            shard_client,
            meta_endpoints,
            maximum_inflight,
            max_raft_ticks,
            scheduler_execution_delay,
            Some(fault_injector),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn new_at_revision_internal(
        scheduler_gateway_id: Option<u64>,
        cluster_id: [u8; 16],
        catalog_revision: u64,
        graph: GraphDefinition,
        shard_client: Arc<RemoteShardClient>,
        meta_endpoints: Vec<SocketAddr>,
        maximum_inflight: usize,
        max_raft_ticks: usize,
        scheduler_execution_delay: Duration,
        fault_injector: Option<Arc<dyn AnalyticsFaultInjector>>,
    ) -> Result<Self, RemoteGatewayServiceError> {
        if cluster_id == [0; 16]
            || catalog_revision == 0
            || meta_endpoints.is_empty()
            || meta_endpoints.iter().any(|address| address.port() == 0)
            || maximum_inflight == 0
            || max_raft_ticks == 0
        {
            return Err(RemoteGatewayServiceError::InvalidConfiguration);
        }
        let deployment = DeploymentConfig::from_catalog(&graph)
            .map_err(|error| RemoteGatewayServiceError::Topology(error.to_string()))?;
        let meta_timestamp_client =
            MetaTimestampClient::new(cluster_id, meta_endpoints.clone(), maximum_inflight)
                .map_err(|error| RemoteGatewayServiceError::Meta(error.to_string()))?;
        let bolt_request_nonce =
            gateway_request_nonce(cluster_id, scheduler_gateway_id.unwrap_or_default());
        let routing = Arc::new(RwLock::new(GatewayRoutingState {
            revision: catalog_revision,
            graph,
            deployment: Arc::new(deployment),
        }));
        let result_reader: Arc<dyn AnalyticsResultReader> =
            Arc::new(GatewayAnalyticsResultReader {
                shard_client: shard_client.clone(),
                routing: Arc::clone(&routing),
            });
        let analytics_coordinator: Arc<dyn ClusterAnalyticsCoordinator> = Arc::new(
            MetaClusterAnalyticsCoordinator::new_with_reader(
                cluster_id,
                meta_endpoints.clone(),
                maximum_inflight,
                result_reader,
            )
            .map_err(|error| RemoteGatewayServiceError::Meta(error.to_string()))?,
        );
        let procedure_registry = Arc::new(
            ProcedureRegistry::builtin_analytics(
                Arc::new(BuiltInProvider::new()),
                analytics_coordinator,
            )
            .map_err(|error| RemoteGatewayServiceError::Query(error.to_string()))?,
        );
        let scheduler = if let Some(gateway_id) = scheduler_gateway_id {
            let scheduler = if let Some(fault_injector) = fault_injector {
                AnalyticsScheduler::spawn_with_delay_and_fault_injector(
                    cluster_id,
                    gateway_id,
                    meta_endpoints.clone(),
                    Arc::clone(&shard_client) as Arc<dyn ShardClient>,
                    Arc::clone(&routing),
                    Arc::new(BuiltInProvider::new()),
                    scheduler_execution_delay,
                    fault_injector,
                )
            } else {
                AnalyticsScheduler::spawn_with_delay(
                    cluster_id,
                    gateway_id,
                    meta_endpoints.clone(),
                    Arc::clone(&shard_client) as Arc<dyn ShardClient>,
                    Arc::clone(&routing),
                    Arc::new(BuiltInProvider::new()),
                    scheduler_execution_delay,
                )
            };
            Some(Arc::new(scheduler.map_err(|error| {
                RemoteGatewayServiceError::Meta(error.to_string())
            })?))
        } else {
            None
        };
        Ok(Self {
            cluster_id,
            routing,
            shard_client,
            meta_timestamp_client,
            admission: Arc::new(
                AdmissionController::new(maximum_inflight)
                    .map_err(|_| RemoteGatewayServiceError::InvalidConfiguration)?,
            ),
            max_raft_ticks,
            bolt_request_nonce,
            bolt_request_sequence: Arc::new(AtomicU64::new(1)),
            bolt_transactions: Arc::new(Mutex::new(BTreeMap::new())),
            procedure_registry,
            _scheduler: scheduler,
            #[cfg(feature = "paper-benchmark-control")]
            benchmark_ablations: None,
        })
    }

    pub fn install_catalog(
        &self,
        revision: u64,
        graph: GraphDefinition,
    ) -> Result<bool, RemoteGatewayServiceError> {
        let deployment = Arc::new(
            DeploymentConfig::from_catalog(&graph)
                .map_err(|error| RemoteGatewayServiceError::Topology(error.to_string()))?,
        );
        let mut current = self
            .routing
            .write()
            .map_err(|_| RemoteGatewayServiceError::Topology("routing lock poisoned".into()))?;
        if graph.graph_id() != current.graph.graph_id() {
            return Err(RemoteGatewayServiceError::Topology(
                "Catalog update targets another graph".into(),
            ));
        }
        if revision <= current.revision {
            return Ok(false);
        }
        *current = GatewayRoutingState {
            revision,
            graph,
            deployment,
        };
        Ok(true)
    }

    pub fn catalog_revision(&self) -> Result<u64, RemoteGatewayServiceError> {
        Ok(self.routing_snapshot()?.revision)
    }

    pub fn close_admission(&self) {
        self.admission.close();
    }

    #[must_use]
    pub fn inflight_requests(&self) -> usize {
        self.admission.inflight()
    }

    fn routing_snapshot(&self) -> Result<GatewayRoutingState, RemoteGatewayServiceError> {
        self.routing
            .read()
            .map(|routing| routing.clone())
            .map_err(|_| RemoteGatewayServiceError::Topology("routing lock poisoned".into()))
    }

    async fn execute(
        &self,
        request_id: u128,
        deadline_unix_ms: u64,
        request: GatewayRequest,
    ) -> Result<Value, RemoteGatewayServiceError> {
        if request.version != GATEWAY_API_VERSION {
            return Err(RemoteGatewayServiceError::Request(format!(
                "unsupported Gateway API version {}",
                request.version
            )));
        }
        if request.request_id.is_empty() || request.request_id.len() > 128 {
            return Err(RemoteGatewayServiceError::Request(
                "invalid Gateway request ID".into(),
            ));
        }
        let routing = self.routing_snapshot()?;
        match request.operation {
            GatewayOperation::Status => Ok(json!({
                "graph_id": routing.graph.graph_id(),
                "schema_version": routing.graph.schema_version(),
                "topology_epoch": routing.graph.topology().epoch(),
                "catalog_revision": routing.revision,
                "deployment_mode": format!("{:?}", routing.deployment.mode()),
                "catalog_source": "meta_quorum",
                "replica_storage": "none"
            })),
            GatewayOperation::Transaction {
                schema_version,
                ttl_micros,
                mutations,
            } => {
                if schema_version != routing.graph.schema_version() {
                    return Err(RemoteGatewayServiceError::Request(format!(
                        "schema version {schema_version} differs from current {}",
                        routing.graph.schema_version()
                    )));
                }
                let scoped = mutations
                    .into_iter()
                    .map(|mutation| mutation.into_scoped(routing.graph.graph_id()))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|error| RemoteGatewayServiceError::Request(error.to_string()))?;
                let (start_ts, commit_ts) = self
                    .allocate_transaction_timestamps(request_id, deadline_unix_ms)
                    .await?;
                let context = TransactionContext::from_allocated(
                    start_ts,
                    commit_ts,
                    schema_version,
                    IsolationLevel::TemporalSnapshot,
                    ttl_micros,
                )
                .map_err(|error| RemoteGatewayServiceError::Transaction(error.to_string()))?;
                let client: Arc<dyn ShardClient> = self.shard_client.clone();
                let receipt = TransactionCoordinator::remote(self.max_raft_ticks)
                    .commit_temporal_remote(
                        client,
                        &routing.deployment,
                        routing.graph.graph_id(),
                        deadline_unix_ms,
                        context,
                        scoped,
                    )
                    .await
                    .map_err(|error| RemoteGatewayServiceError::Transaction(error.to_string()))?;
                Ok(json!({
                    "transaction_id": receipt.transaction_id().value().to_string(),
                    "start_ts": timestamp_json(receipt.start_ts()),
                    "commit_ts": timestamp_json(receipt.commit_ts()),
                    "home_shard": receipt.home().shard_id(),
                    "home_epoch": receipt.home().placement_epoch(),
                    "participants": receipt.participants().iter().map(|participant| json!({
                        "shard_id": participant.shard_id(),
                        "placement_epoch": participant.placement_epoch(),
                    })).collect::<Vec<_>>(),
                    "single_shard_fast_path": receipt.single_shard_fast_path(),
                }))
            }
            GatewayOperation::Cypher { text } => {
                self.execute_cypher_query(request_id, deadline_unix_ms, &routing, &text)
                    .await
            }
            GatewayOperation::MigrateBackend { .. } => Err(RemoteGatewayServiceError::Request(
                "backend migration is owned by the Controller service".into(),
            )),
        }
    }

    async fn execute_cypher_query(
        &self,
        request_id: u128,
        deadline_unix_ms: u64,
        routing: &GatewayRoutingState,
        text: &str,
    ) -> Result<Value, RemoteGatewayServiceError> {
        let compiled = compile_cypher(routing, text)?;
        if !compiled.is_read_only() {
            return self
                .execute_cypher_write(
                    request_id,
                    deadline_unix_ms,
                    routing,
                    &compiled,
                    text,
                    BTreeMap::new(),
                )
                .await;
        }
        let response = self
            .execute_cypher_response(
                request_id,
                deadline_unix_ms,
                routing,
                text,
                BTreeMap::new(),
                None,
                GraphOverlay::default(),
                &compiled,
                None,
            )
            .await?;
        cypher_response_json(&response)
    }

    async fn execute_cypher_write(
        &self,
        request_id: u128,
        deadline_unix_ms: u64,
        routing: &GatewayRoutingState,
        compiled: &cypher_compiler::CompiledQuery,
        text: &str,
        parameters: BTreeMap<String, RuntimeValue>,
    ) -> Result<Value, RemoteGatewayServiceError> {
        if let Some(batch_rows) = mutation_plan_batch_rows(compiled.mutation_plan())? {
            return self
                .execute_cypher_batch_write(
                    request_id,
                    deadline_unix_ms,
                    routing,
                    compiled,
                    text,
                    parameters,
                    batch_rows,
                )
                .await;
        }
        const MAX_MERGE_ATTEMPTS: usize = 3;
        let has_merge = mutation_plan_has_merge(compiled.mutation_plan());
        for attempt in 0..MAX_MERGE_ATTEMPTS {
            let prepared = self
                .prepare_cypher_writes(
                    request_id,
                    deadline_unix_ms,
                    routing,
                    compiled,
                    text,
                    parameters.clone(),
                    GraphOverlay::default(),
                    None,
                    None,
                )
                .await?;
            match self
                .commit_prepared_cypher_writes(routing, deadline_unix_ms, prepared)
                .await
            {
                Ok(response) => return Ok(response),
                Err(RemoteGatewayServiceError::RetryableMergeContention(message))
                    if has_merge && attempt + 1 < MAX_MERGE_ATTEMPTS =>
                {
                    let _ = message;
                }
                Err(RemoteGatewayServiceError::RetryableMergeContention(message)) => {
                    return Err(RemoteGatewayServiceError::Transaction(message));
                }
                Err(error) => return Err(error),
            }
        }
        unreachable!("bounded MERGE retry loop always returns")
    }

    #[allow(clippy::too_many_arguments)]
    async fn execute_cypher_batch_write(
        &self,
        request_id: u128,
        deadline_unix_ms: u64,
        routing: &GatewayRoutingState,
        compiled: &cypher_compiler::CompiledQuery,
        text: &str,
        parameters: BTreeMap<String, RuntimeValue>,
        batch_rows: u32,
    ) -> Result<Value, RemoteGatewayServiceError> {
        let batch_rows = usize::try_from(batch_rows).map_err(|_| {
            RemoteGatewayServiceError::Query(
                "DTG-CYPHER-INVALID-SUBQUERY-BATCH: batch size exceeds this platform".into(),
            )
        })?;
        let first_request_id = batch_request_id(request_id, 0);
        let first_timestamps = self
            .allocate_transaction_timestamps(first_request_id, deadline_unix_ms)
            .await?;
        let first_ledger_key = batch_ledger_key(
            self.cluster_id,
            routing.graph.graph_id(),
            request_id,
            compiled.fingerprint(),
            0,
        );
        let first_committed = self
            .lookup_batch_ledger(
                batch_request_id(request_id, usize::MAX),
                deadline_unix_ms,
                routing,
                first_ledger_key,
                first_timestamps.0,
            )
            .await?;
        let input_snapshot = first_committed
            .map(|transaction_id| transaction_time_from_id(transaction_id.value()))
            .transpose()?
            .unwrap_or(first_timestamps.0);
        let input_rows = self
            .resolve_existing_write_bindings(
                first_request_id,
                deadline_unix_ms,
                routing,
                compiled,
                text,
                parameters.clone(),
                GraphOverlay::default(),
                Some(input_snapshot),
            )
            .await?;
        let total_row_count = input_rows.len();
        let mut summaries = Vec::new();
        for (batch_index, rows) in input_rows.chunks(batch_rows).enumerate() {
            let batch_request = batch_request_id(request_id, batch_index);
            let ledger_key = batch_ledger_key(
                self.cluster_id,
                routing.graph.graph_id(),
                request_id,
                compiled.fingerprint(),
                batch_index,
            );
            let committed = if batch_index == 0 {
                first_committed
            } else {
                self.lookup_batch_ledger(
                    batch_request_id(request_id, usize::MAX - batch_index),
                    deadline_unix_ms,
                    routing,
                    ledger_key,
                    first_timestamps.0,
                )
                .await?
            };
            let timestamps = if let Some(transaction_id) = committed {
                let start = transaction_time_from_id(transaction_id.value())?;
                let commit = advance_timestamp(start, 2)
                    .map_err(|error| RemoteGatewayServiceError::Meta(error.to_string()))?;
                (start, commit)
            } else if batch_index == 0 {
                first_timestamps
            } else {
                self.allocate_transaction_timestamps(batch_request, deadline_unix_ms)
                    .await?
            };
            let prepared = match self
                .prepare_cypher_writes(
                    batch_request,
                    deadline_unix_ms,
                    routing,
                    compiled,
                    text,
                    parameters.clone(),
                    GraphOverlay::default(),
                    Some(timestamps),
                    Some(rows.to_vec()),
                )
                .await
            {
                Ok(prepared) => prepared,
                Err(error) => {
                    return Err(batch_failure_error(
                        batch_index,
                        batch_index * batch_rows,
                        rows.len(),
                        &summaries,
                        error,
                    ));
                }
            };
            let ledger_claim = batch_ledger_claim(routing, ledger_key, timestamps.0)?;
            let mut prepared = prepared;
            let first = prepared.first_mut().ok_or_else(|| {
                RemoteGatewayServiceError::Transaction(
                    "batch subtransaction produced no row candidate".into(),
                )
            })?;
            first.constraints.push(ledger_claim);
            let mut summary = if committed.is_some() {
                replayed_batch_summary(routing, &prepared)?
            } else {
                match self
                    .commit_prepared_cypher_writes(routing, deadline_unix_ms, prepared)
                    .await
                {
                    Ok(summary) => summary,
                    Err(error) => {
                        return Err(batch_failure_error(
                            batch_index,
                            batch_index * batch_rows,
                            rows.len(),
                            &summaries,
                            error,
                        ));
                    }
                }
            };
            let object = summary.as_object_mut().ok_or_else(|| {
                RemoteGatewayServiceError::Transaction(
                    "batch commit returned a non-object summary".into(),
                )
            })?;
            object.insert("batch_index".into(), json!(batch_index));
            object.insert("row_start".into(), json!(batch_index * batch_rows));
            object.insert("row_count".into(), json!(rows.len()));
            summaries.push(summary);
        }
        let last = summaries.last();
        let result = json!({
            "kind": "cypher_batch_write",
            "query_fingerprint": hex_bytes(&compiled.fingerprint()),
            "batch_rows": batch_rows,
            "total_row_count": total_row_count,
            "committed_batch_count": summaries.len(),
            "transaction_id": last.and_then(|summary| summary.get("transaction_id")).cloned().unwrap_or(Value::Null),
            "start_ts": last.and_then(|summary| summary.get("start_ts")).cloned().unwrap_or(Value::Null),
            "commit_ts": last.and_then(|summary| summary.get("commit_ts")).cloned().unwrap_or(Value::Null),
            "batches": summaries,
        });
        Ok(result)
    }

    async fn lookup_batch_ledger(
        &self,
        request_id: u128,
        deadline_unix_ms: u64,
        routing: &GatewayRoutingState,
        ledger_key: [u8; 32],
        snapshot: TransactionTime,
    ) -> Result<Option<txn_protocol::TransactionId>, RemoteGatewayServiceError> {
        let probe = batch_ledger_claim(routing, ledger_key, snapshot)?;
        let (participant, keys) = probe
            .owner_read_keys(&routing.deployment)
            .map_err(|error| RemoteGatewayServiceError::Transaction(error.to_string()))?;
        let context = ShardRequestContext::new(
            routing.graph.graph_id(),
            participant.shard_id(),
            participant.placement_epoch(),
            request_id,
            deadline_unix_ms,
        )
        .map_err(|error| RemoteGatewayServiceError::Transaction(error.to_string()))?;
        let values = self
            .shard_client
            .read_keys(
                ReadKeysRequest::new(context, keys.into_iter().collect())
                    .map_err(|error| RemoteGatewayServiceError::Transaction(error.to_string()))?,
            )
            .await
            .map_err(|error| RemoteGatewayServiceError::Transaction(error.to_string()))?;
        if values.len() != 2 {
            return Err(RemoteGatewayServiceError::Transaction(
                "batch ledger lookup returned an invalid value count".into(),
            ));
        }
        let owner = probe
            .owner_at_snapshot(values[0].as_deref(), values[1].as_deref(), snapshot)
            .map_err(|error| RemoteGatewayServiceError::Transaction(error.to_string()))?;
        owner
            .map(|owner| {
                if owner.kind() != temporal_storage::ElementKind::Vertex {
                    return Err(RemoteGatewayServiceError::Transaction(
                        "batch ledger owner has the wrong element kind".into(),
                    ));
                }
                if owner.id().value() == 0 {
                    return Err(RemoteGatewayServiceError::Transaction(
                        "batch ledger contains an invalid transaction identity".into(),
                    ));
                }
                Ok(txn_protocol::TransactionId::new(owner.id().value()))
            })
            .transpose()
    }

    #[allow(clippy::too_many_arguments)]
    async fn prepare_cypher_writes(
        &self,
        request_id: u128,
        deadline_unix_ms: u64,
        routing: &GatewayRoutingState,
        compiled: &cypher_compiler::CompiledQuery,
        text: &str,
        parameters: BTreeMap<String, RuntimeValue>,
        graph_overlay: GraphOverlay,
        fixed_timestamps: Option<(TransactionTime, TransactionTime)>,
        input_rows: Option<Vec<(BTreeMap<String, RuntimeValue>, bool)>>,
    ) -> Result<Vec<PreparedCypherWrite>, RemoteGatewayServiceError> {
        let (start_ts, commit_ts) = if let Some(timestamps) = fixed_timestamps {
            timestamps
        } else {
            self.allocate_transaction_timestamps(request_id, deadline_unix_ms)
                .await?
        };
        let current_valid_time = ValidTime::from_micros(unix_time_micros()?);
        let resolved = resolve_compiled_temporal_scope(
            compiled,
            routing.graph.graph_id(),
            current_valid_time,
            start_ts,
            parameters.clone(),
        )
        .map_err(|error| RemoteGatewayServiceError::Query(error.to_string()))?;
        let valid = match resolved.valid_time() {
            query_executor::ResolvedValidTime::Point(value) => Interval::forever_from(value),
            query_executor::ResolvedValidTime::Interval { start, end } => {
                Interval::new(start, Some(end))
                    .map_err(|error| RemoteGatewayServiceError::Query(error.to_string()))?
            }
        };
        let input_rows = if let Some(input_rows) = input_rows {
            input_rows
        } else {
            self.resolve_existing_write_bindings(
                request_id,
                deadline_unix_ms,
                routing,
                compiled,
                text,
                parameters.clone(),
                graph_overlay.clone(),
                Some(start_ts),
            )
            .await?
        };
        let has_merge = mutation_plan_has_merge(compiled.mutation_plan());
        let context = TransactionContext::from_allocated(
            start_ts,
            commit_ts,
            routing.graph.schema_version(),
            IsolationLevel::TemporalSnapshot,
            60_000_000,
        )
        .map_err(|error| RemoteGatewayServiceError::Transaction(error.to_string()))?;
        let multiple_rows = input_rows.len() > 1;
        let base_seed = write_request_seed(self.cluster_id, request_id);
        let nested_seed = base_seed;
        let mut prepared = Vec::with_capacity(input_rows.len());
        for (row_index, (mut existing_bindings, has_input_row)) in
            input_rows.into_iter().enumerate()
        {
            let row_seed = write_row_seed(base_seed, row_index, multiple_rows);
            let row_nested_seed = write_row_seed(nested_seed, row_index, multiple_rows);
            let mut write_context = WriteContext::new(
                routing.graph.graph_id(),
                routing.graph.schema_version(),
                routing.deployment.virtual_partitions(),
                row_seed,
                valid,
                parameters.clone(),
            )
            .map_err(|error| RemoteGatewayServiceError::Query(error.to_string()))?
            .with_nested_seed(row_nested_seed);
            if !has_input_row {
                write_context = write_context.without_input_row();
            }
            let subquery_inputs = if has_input_row {
                self.resolve_write_subquery_inputs(
                    request_id,
                    deadline_unix_ms,
                    routing,
                    compiled,
                    text,
                    &parameters,
                    &graph_overlay,
                    start_ts,
                    compiled.mutation_plan(),
                    &existing_bindings,
                )
                .await?
            } else {
                Vec::new()
            };
            let probe_context = write_context
                .clone()
                .with_existing_bindings(existing_bindings.clone())
                .map_err(|error| RemoteGatewayServiceError::Query(error.to_string()))?;
            let merge_probe = has_merge
                .then(|| {
                    probe_merge_constraints_with_subquery_inputs(
                        compiled,
                        &probe_context,
                        &subquery_inputs,
                    )
                })
                .transpose()
                .map_err(|error| RemoteGatewayServiceError::Query(error.to_string()))?;
            let mut resolved_merge_keys = BTreeSet::new();
            if let Some(probe) = merge_probe.as_ref() {
                let (restored, resolved) = self
                    .resolve_committed_merge_bindings(
                        merge_row_request_id(request_id, row_index),
                        deadline_unix_ms,
                        routing,
                        probe,
                        start_ts,
                    )
                    .await?;
                for (name, value) in restored {
                    if let Some(existing) = existing_bindings.get(&name)
                        && existing != &value
                    {
                        return Err(RemoteGatewayServiceError::Transaction(format!(
                            "MERGE binding {name} conflicts with an existing query binding"
                        )));
                    }
                    existing_bindings.insert(name, value);
                }
                resolved_merge_keys = resolved;
            }
            let write_context = write_context
                .with_existing_bindings(existing_bindings)
                .map_err(|error| RemoteGatewayServiceError::Query(error.to_string()))?
                .with_resolved_merge_keys(resolved_merge_keys);
            let materialized =
                materialize_write_with_subquery_inputs(compiled, &write_context, &subquery_inputs)
                    .map_err(|error| RemoteGatewayServiceError::Query(error.to_string()))?;
            let constraints = self.merge_constraints(&materialized, routing)?;
            let bindings = materialized
                .bindings()
                .iter()
                .map(|(name, element)| Ok((name.clone(), materialized_element_json(element)?)))
                .collect::<Result<Map<_, _>, RemoteGatewayServiceError>>()?;
            let overlay_values = materialized.overlay_values();
            let graph_overlay_entries = materialized
                .overlay_elements()
                .iter()
                .map(|element| {
                    let scope = temporal_ir::GraphScope::new(
                        temporal_storage::GraphId::new(routing.graph.graph_id()),
                        element.element().partition(),
                    );
                    let owner_shard = routing.deployment.route_scope(scope).shard_id();
                    let adjacency_shards = match element.kind() {
                        MaterializedElementKind::Vertex => vec![owner_shard],
                        MaterializedElementKind::Relationship => [
                            element.source().expect("relationship source"),
                            element.destination().expect("relationship destination"),
                        ]
                        .into_iter()
                        .map(|endpoint| {
                            routing
                                .deployment
                                .route_scope(temporal_ir::GraphScope::new(
                                    endpoint.graph(),
                                    endpoint.partition(),
                                ))
                                .shard_id()
                        })
                        .chain(std::iter::once(owner_shard))
                        .collect(),
                    };
                    if element.deleted() {
                        Ok(GraphOverlayEntry::delete_with_adjacency(
                            owner_shard,
                            adjacency_shards,
                            element.element(),
                            valid,
                        ))
                    } else {
                        GraphOverlayEntry::put_with_adjacency(
                            owner_shard,
                            adjacency_shards,
                            valid,
                            element.runtime_value(),
                        )
                        .map_err(|error| RemoteGatewayServiceError::Query(error.to_string()))
                    }
                })
                .collect::<Result<Vec<_>, _>>()?;
            let scoped = materialized
                .into_scoped_transactions()
                .into_iter()
                .map(|write| {
                    let (scope, transaction) = write.into_parts();
                    dtgproxy::ScopedTemporalTransaction::new(scope, transaction)
                })
                .collect();
            prepared.push(PreparedCypherWrite {
                context,
                scoped,
                bindings,
                overlay_values,
                graph_overlay_entries,
                fingerprint: compiled.fingerprint(),
                constraints,
            });
        }
        Ok(prepared)
    }

    fn merge_constraints(
        &self,
        materialized: &cypher_engine::MaterializedWriteSet,
        routing: &GatewayRoutingState,
    ) -> Result<Vec<dtgproxy::RoutedConstraintClaim>, RemoteGatewayServiceError> {
        materialized
            .merge_constraints()
            .iter()
            .map(|claim| {
                dtgproxy::RoutedConstraintClaim::new(
                    routing.graph.graph_id(),
                    claim.key(),
                    claim.owner(),
                )
                .map_err(|error| RemoteGatewayServiceError::Transaction(error.to_string()))
            })
            .collect()
    }

    async fn resolve_committed_merge_bindings(
        &self,
        request_id: u128,
        deadline_unix_ms: u64,
        routing: &GatewayRoutingState,
        probe: &cypher_engine::MaterializedWriteSet,
        start_ts: TransactionTime,
    ) -> Result<(BTreeMap<String, RuntimeValue>, BTreeSet<[u8; 32]>), RemoteGatewayServiceError>
    {
        let mut bindings = BTreeMap::new();
        let mut resolved_keys = BTreeSet::new();
        for (index, constraint) in probe.merge_constraints().iter().enumerate() {
            let routed = dtgproxy::RoutedConstraintClaim::new(
                routing.graph.graph_id(),
                constraint.key(),
                constraint.owner(),
            )
            .map_err(|error| RemoteGatewayServiceError::Transaction(error.to_string()))?;
            let (participant, keys) = routed
                .owner_read_keys(&routing.deployment)
                .map_err(|error| RemoteGatewayServiceError::Transaction(error.to_string()))?;
            let lookup_id =
                (request_id ^ 0x4454_475f_4d45_5247_455f_4c4f_4f4b_u128 ^ u128::from(index as u64))
                    .max(1);
            let context = ShardRequestContext::new(
                routing.graph.graph_id(),
                participant.shard_id(),
                participant.placement_epoch(),
                lookup_id,
                deadline_unix_ms,
            )
            .map_err(|error| RemoteGatewayServiceError::Transaction(error.to_string()))?;
            let values = self
                .shard_client
                .read_keys(
                    ReadKeysRequest::new(context, keys.into_iter().collect()).map_err(|error| {
                        RemoteGatewayServiceError::Transaction(error.to_string())
                    })?,
                )
                .await
                .map_err(|error| RemoteGatewayServiceError::Transaction(error.to_string()))?;
            if values.len() != 2 {
                return Err(RemoteGatewayServiceError::Transaction(
                    "constraint owner lookup returned an invalid value count".into(),
                ));
            }
            let owner = routed
                .owner_at_snapshot(values[0].as_deref(), values[1].as_deref(), start_ts)
                .map_err(|error| RemoteGatewayServiceError::Transaction(error.to_string()))?;
            let Some(owner) = owner else {
                continue;
            };
            if owner != routed.owner() {
                return Err(RemoteGatewayServiceError::Transaction(
                    "MERGE constraint is owned by a different canonical element".into(),
                ));
            }
            for name in constraint.binding_names() {
                let element = probe.binding(name).ok_or_else(|| {
                    RemoteGatewayServiceError::Transaction(format!(
                        "MERGE constraint binding {name} is absent from its deterministic probe"
                    ))
                })?;
                let value = element.runtime_value();
                if let Some(existing) = bindings.get(name)
                    && existing != &value
                {
                    return Err(RemoteGatewayServiceError::Transaction(format!(
                        "MERGE binding {name} resolves to conflicting canonical elements"
                    )));
                }
                bindings.insert(name.clone(), value);
            }
            if !constraint.binding_names().iter().any(|name| {
                probe
                    .binding(name)
                    .is_some_and(|element| element.element() == owner)
            }) {
                return Err(RemoteGatewayServiceError::Transaction(
                    "MERGE constraint owner has no claim-local probe binding".into(),
                ));
            }
            resolved_keys.insert(constraint.key());
        }
        Ok((bindings, resolved_keys))
    }

    #[allow(clippy::too_many_arguments)]
    fn resolve_write_subquery_inputs<'a>(
        &'a self,
        request_id: u128,
        deadline_unix_ms: u64,
        routing: &'a GatewayRoutingState,
        compiled: &'a CompiledQuery,
        text: &'a str,
        parameters: &'a BTreeMap<String, RuntimeValue>,
        graph_overlay: &'a GraphOverlay,
        fixed_snapshot: TransactionTime,
        mutation_plan: &'a cypher_compiler::MutationPlan,
        scope: &'a BTreeMap<String, RuntimeValue>,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Vec<WriteSubqueryInput>, RemoteGatewayServiceError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            let mut inputs = Vec::new();
            for mutation in mutation_plan.mutations() {
                let CompiledMutation::Subquery(subquery) = mutation else {
                    continue;
                };
                let imports = subquery
                    .imports()
                    .iter()
                    .map(|name| {
                        scope
                            .get(name)
                            .cloned()
                            .map(|value| (name.clone(), value))
                            .ok_or_else(|| {
                                RemoteGatewayServiceError::Query(format!(
                                    "DTG-CYPHER-SUBQUERY-IMPORT-MISSING: write subquery import {name} has no outer-row value"
                                ))
                            })
                    })
                    .collect::<Result<BTreeMap<_, _>, _>>()?;
                let prefix_rows = if let Some(prefix) = subquery.read_prefix_plan() {
                    let response = self
                        .execute_cypher_response(
                            request_id
                                ^ 0x4454_475f_4348_494c_445f_5245_4144_u128
                                ^ u128::from(subquery.clause_start() as u64),
                            deadline_unix_ms,
                            routing,
                            text,
                            parameters.clone(),
                            Some(fixed_snapshot),
                            graph_overlay.clone(),
                            compiled,
                            Some((prefix, &imports)),
                        )
                        .await?;
                    response
                        .batches()
                        .iter()
                        .flat_map(|batch| batch.rows())
                        .map(|row| {
                            response
                                .schema()
                                .columns()
                                .iter()
                                .zip(row)
                                .map(|(column, value)| (column.name().to_owned(), value.clone()))
                                .collect::<BTreeMap<_, _>>()
                        })
                        .collect::<Vec<_>>()
                } else if mutation_plan_has_child_read_prefix(subquery.mutation_plan()) {
                    vec![BTreeMap::new()]
                } else {
                    continue;
                };
                let mut rows = Vec::with_capacity(prefix_rows.len());
                for (row_index, row) in prefix_rows.into_iter().enumerate() {
                    let child_scope = merge_runtime_bindings(&imports, &row)?;
                    let nested = self
                        .resolve_write_subquery_inputs(
                            request_id
                                ^ u128::from(subquery.clause_start() as u64)
                                ^ u128::from(row_index as u64),
                            deadline_unix_ms,
                            routing,
                            compiled,
                            text,
                            parameters,
                            graph_overlay,
                            fixed_snapshot,
                            subquery.mutation_plan(),
                            &child_scope,
                        )
                        .await?;
                    rows.push(WriteSubqueryRow::new(row, nested));
                }
                inputs.push(WriteSubqueryInput::new(subquery.clause_start(), rows));
            }
            Ok(inputs)
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn resolve_existing_write_bindings(
        &self,
        request_id: u128,
        deadline_unix_ms: u64,
        routing: &GatewayRoutingState,
        compiled: &cypher_compiler::CompiledQuery,
        text: &str,
        parameters: BTreeMap<String, RuntimeValue>,
        graph_overlay: GraphOverlay,
        fixed_snapshot: Option<TransactionTime>,
    ) -> Result<Vec<(BTreeMap<String, RuntimeValue>, bool)>, RemoteGatewayServiceError> {
        let mut names = BTreeMap::new();
        fn collect_names(plan: &cypher_compiler::MutationPlan, names: &mut BTreeMap<String, ()>) {
            for mutation in plan.mutations() {
                match mutation {
                    CompiledMutation::SetProperty { target, .. }
                    | CompiledMutation::RemoveProperty(target) => {
                        names.insert(target.variable().to_owned(), ());
                    }
                    CompiledMutation::Delete { variables, .. } => {
                        names.extend(variables.iter().cloned().map(|name| (name, ())));
                    }
                    CompiledMutation::Merge(pattern) => {
                        for path in pattern.paths() {
                            if let Some(variable) = path.start().variable() {
                                names.insert(variable.value().to_owned(), ());
                            }
                            for chain in path.chains() {
                                if let Some(variable) = chain.node().variable() {
                                    names.insert(variable.value().to_owned(), ());
                                }
                                if let Some(variable) = chain.relationship().variable() {
                                    names.insert(variable.value().to_owned(), ());
                                }
                            }
                        }
                    }
                    CompiledMutation::Create(_) => {}
                    CompiledMutation::Subquery(subquery) => {
                        names.extend(subquery.imports().iter().cloned().map(|name| (name, ())));
                        collect_names(subquery.mutation_plan(), names);
                    }
                }
            }
        }
        collect_names(compiled.mutation_plan(), &mut names);
        if compiled.read_prefix_plan().is_none() {
            return Ok(vec![(BTreeMap::new(), true)]);
        }
        let response = self
            .execute_cypher_response(
                request_id ^ 0x4454_475f_5752_4954,
                deadline_unix_ms,
                routing,
                text,
                parameters,
                fixed_snapshot,
                graph_overlay,
                compiled,
                None,
            )
            .await?;
        let rows = response
            .batches()
            .iter()
            .flat_map(|batch| batch.rows().iter())
            .collect::<Vec<_>>();
        if rows.is_empty() {
            return Ok(vec![(
                BTreeMap::new(),
                compiled.uses_standalone_merge_match_prefix(),
            )]);
        }
        let mut resolved_rows = Vec::with_capacity(rows.len());
        for row in rows {
            let mut bindings = BTreeMap::new();
            for (index, column) in response.schema().columns().iter().enumerate() {
                if names.contains_key(column.name()) {
                    bindings.insert(column.name().to_owned(), row[index].clone());
                }
            }
            resolved_rows.push((bindings, true));
        }
        Ok(resolved_rows)
    }

    async fn commit_prepared_cypher_writes(
        &self,
        routing: &GatewayRoutingState,
        deadline_unix_ms: u64,
        prepared: Vec<PreparedCypherWrite>,
    ) -> Result<Value, RemoteGatewayServiceError> {
        let first = prepared.first().ok_or_else(|| {
            RemoteGatewayServiceError::Transaction("empty staged write set".into())
        })?;
        let context = first.context;
        let fingerprint = first.fingerprint;
        if prepared.iter().any(|write| write.context != context) {
            return Err(RemoteGatewayServiceError::Transaction(
                "row-scoped writes do not share one transaction context".into(),
            ));
        }
        let bindings = unambiguous_response_bindings(&prepared);
        let overlay_values = prepared
            .iter()
            .flat_map(|write| write.overlay_values.iter().cloned())
            .collect::<Vec<_>>();
        let constraints = prepared
            .iter()
            .flat_map(|write| write.constraints.iter().cloned())
            .collect::<Vec<_>>();
        let mut grouped =
            BTreeMap::<(u64, u32), (temporal_ir::GraphScope, TransactionOverlay)>::new();
        for write in prepared {
            for scoped in write.scoped {
                let (scope, transaction) = scoped.into_parts();
                match grouped.entry((scope.graph().value(), scope.partition().value())) {
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        let mut overlay = TransactionOverlay::new(MAX_TRANSACTION_MUTATIONS)
                            .map_err(|error| {
                                RemoteGatewayServiceError::Transaction(error.to_string())
                            })?;
                        overlay.stage(transaction).map_err(|error| {
                            RemoteGatewayServiceError::Transaction(error.to_string())
                        })?;
                        entry.insert((scope, overlay));
                    }
                    std::collections::btree_map::Entry::Occupied(mut entry) => {
                        entry.get_mut().1.stage(transaction).map_err(|error| {
                            RemoteGatewayServiceError::Transaction(error.to_string())
                        })?;
                    }
                }
            }
        }
        let scoped: Vec<dtgproxy::ScopedTemporalTransaction> = grouped
            .into_iter()
            .map(|(_, (scope, overlay))| {
                dtgproxy::ScopedTemporalTransaction::new(scope, overlay.into_transaction())
            })
            .collect();
        if scoped.is_empty() {
            return Ok(json!({
                "kind": "cypher_write",
                "query_fingerprint": hex_bytes(&fingerprint),
                "transaction_id": Value::Null,
                "start_ts": Value::Null,
                "commit_ts": Value::Null,
                "participants": Vec::<u32>::new(),
                "single_shard_fast_path": true,
                "bindings": bindings,
                "rows": overlay_values
                    .iter()
                    .map(|value| runtime_value_json(value).unwrap_or(Value::Null))
                    .collect::<Vec<_>>(),
            }));
        }
        let client: Arc<dyn ShardClient> = self.shard_client.clone();
        let has_constraints = !constraints.is_empty();
        let receipt = TransactionCoordinator::remote(self.max_raft_ticks)
            .commit_temporal_remote_with_constraints(
                client,
                &routing.deployment,
                routing.graph.graph_id(),
                deadline_unix_ms,
                context,
                scoped,
                constraints,
            )
            .await
            .map_err(|error| {
                if has_constraints && error.is_retryable_merge_contention() {
                    RemoteGatewayServiceError::RetryableMergeContention(error.to_string())
                } else {
                    RemoteGatewayServiceError::Transaction(error.to_string())
                }
            })?;
        Ok(json!({
            "kind": "cypher_write",
            "query_fingerprint": hex_bytes(&fingerprint),
            "transaction_id": receipt.transaction_id().value().to_string(),
            "start_ts": timestamp_json(receipt.start_ts()),
            "commit_ts": timestamp_json(receipt.commit_ts()),
            "home_shard": receipt.home().shard_id(),
            "participants": receipt.participants().iter().map(|participant| participant.shard_id()).collect::<Vec<_>>(),
            "single_shard_fast_path": receipt.single_shard_fast_path(),
            "bindings": bindings,
            "rows": overlay_values
                .iter()
                .map(|value| {
                    runtime_value_json(value).unwrap_or(Value::Null)
                })
                .collect::<Vec<_>>(),
        }))
    }

    #[allow(clippy::too_many_arguments)]
    async fn project_procedure_graph(
        &self,
        request_id: u128,
        deadline_unix_ms: u64,
        routing: &GatewayRoutingState,
        compiled: &CompiledQuery,
        parameters: BTreeMap<String, RuntimeValue>,
        current_valid_time: ValidTime,
        query_snapshot: TransactionTime,
        graph_overlay: &GraphOverlay,
    ) -> Result<ProcedureProjection, RemoteGatewayServiceError> {
        let resolved = resolve_compiled_temporal_scope(
            compiled,
            routing.graph.graph_id(),
            current_valid_time,
            query_snapshot,
            parameters.clone(),
        )
        .map_err(|error| RemoteGatewayServiceError::Query(error.to_string()))?;
        let procedures = compiled
            .logical_plan()
            .nodes()
            .iter()
            .filter_map(|node| match node.operator() {
                temporal_ir::LogicalOperator::ProcedureCall { procedure } => Some(procedure),
                _ => None,
            })
            .collect::<Vec<_>>();
        let mut models = BTreeSet::new();
        let mut graph_vertices = u64::MAX;
        let mut graph_edges = u64::MAX;
        let mut graph_bytes = u64::MAX;
        for procedure in procedures {
            let descriptor = self
                .procedure_registry
                .catalog()
                .resolve_by_identity(procedure.identity())
                .ok_or_else(|| {
                    RemoteGatewayServiceError::Query(
                        "compiled procedure identity is absent from the runtime catalog".into(),
                    )
                })?;
            let algorithm = if let Some(algorithm) = descriptor.algorithm() {
                Some(algorithm)
            } else if descriptor.name() == "dtg.analytics.submit" {
                let name = submitted_algorithm_name(procedure, &parameters)?;
                self.procedure_registry
                    .catalog()
                    .resolve(&name)
                    .and_then(|descriptor| descriptor.algorithm())
                    .ok_or_else(|| {
                        RemoteGatewayServiceError::Query(format!(
                            "analytics job targets unknown algorithm {name}"
                        ))
                    })
                    .map(Some)?
            } else if matches!(
                descriptor.name(),
                "dtg.analytics.status" | "dtg.analytics.results" | "dtg.analytics.cancel"
            ) {
                None
            } else {
                return Err(RemoteGatewayServiceError::Query(
                    "registered procedure has no analytics graph contract".into(),
                ));
            };
            if let Some(algorithm) = algorithm {
                models.extend(algorithm.graph_models().iter().copied());
                if !graph_overlay.is_empty() && algorithm.graph_models() != [GraphModel::Snapshot] {
                    return Err(RemoteGatewayServiceError::Query(
                        "procedure cannot project the current transaction overlay".into(),
                    ));
                }
            }
            let limits = descriptor.limits();
            graph_vertices = graph_vertices.min(limits.max_graph_vertices());
            graph_edges = graph_edges.min(limits.max_graph_edges());
            graph_bytes = graph_bytes.min(limits.max_graph_bytes());
            if algorithm.is_some() && !graph_overlay.is_empty() && !descriptor.supports_overlay() {
                return Err(RemoteGatewayServiceError::Query(
                    "procedure cannot project the current transaction overlay".into(),
                ));
            }
        }
        let mut models = models.into_iter();
        let requested_model = models.next();
        if models.next().is_some() {
            return Err(RemoteGatewayServiceError::Query(
                "one query must use one supported procedure graph model".into(),
            ));
        }
        let job_limits = JobProjectionLimits::new(graph_vertices, graph_edges, graph_bytes)
            .map_err(|error| RemoteGatewayServiceError::Query(error.to_string()))?;
        let model = requested_model.unwrap_or(match resolved.valid_time() {
            query_executor::ResolvedValidTime::Point(_) => GraphModel::Snapshot,
            query_executor::ResolvedValidTime::Interval { .. } => GraphModel::Interval,
        });
        let scope = job_projection_scope(model, resolved.valid_time())?;
        if requested_model.is_none() {
            return Ok(ProcedureProjection {
                graph: None,
                scope,
                limits: job_limits,
                transaction_snapshot: resolved.transaction_time(),
            });
        }
        let projection_limits = ProjectionLimits::new(
            usize::try_from(graph_vertices).map_err(|_| {
                RemoteGatewayServiceError::Query(
                    "procedure vertex projection bound exceeds this platform".into(),
                )
            })?,
            usize::try_from(graph_edges).map_err(|_| {
                RemoteGatewayServiceError::Query(
                    "procedure edge projection bound exceeds this platform".into(),
                )
            })?,
            graph_bytes,
        )
        .map_err(|error| RemoteGatewayServiceError::Query(error.to_string()))?;
        let client: Arc<dyn ShardClient> = self.shard_client.clone();
        match model {
            GraphModel::Snapshot => {
                let valid_time = match resolved.valid_time() {
                    query_executor::ResolvedValidTime::Point(value) => value,
                    query_executor::ResolvedValidTime::Interval { .. } => {
                        return Err(RemoteGatewayServiceError::Query(
                            "snapshot analytics procedure requires a point valid-time scope".into(),
                        ));
                    }
                };
                let mut vertices = BTreeMap::new();
                let mut edges = BTreeMap::new();
                let preserve_partitions = graph_overlay.is_empty();
                let mut partitions = Vec::new();
                let mut remaining_vertices = projection_limits.max_vertices();
                let mut remaining_edges = projection_limits.max_edges();
                let mut remaining_bytes = projection_limits.max_bytes();
                let placements = routing.deployment.all_shards();
                for (placement_index, placement) in placements.iter().enumerate() {
                    let adapter = ShardClientStorageAdapter::new(
                        Arc::clone(&client),
                        routing.graph.graph_id(),
                        placement.shard_id(),
                        placement.placement_epoch(),
                        deadline_unix_ms,
                        request_namespace(request_id, placement.shard_id()),
                    )
                    .map_err(|error| RemoteGatewayServiceError::Query(error.to_string()))?;
                    let (part, part_bytes) = project_snapshot_identity_part_bounded(
                        &TemporalStore::new(adapter),
                        temporal_storage::GraphId::new(routing.graph.graph_id()),
                        valid_time,
                        resolved.transaction_time(),
                        None,
                        ProjectionLimits::new(
                            remaining_vertices.max(1),
                            remaining_edges.max(1),
                            remaining_bytes.max(1),
                        )
                        .map_err(|error| RemoteGatewayServiceError::Query(error.to_string()))?,
                    )
                    .await
                    .map_err(|error| RemoteGatewayServiceError::Query(error.to_string()))?;
                    let (part_vertices, part_edges) = part.into_parts();
                    if part_vertices.len() > remaining_vertices
                        || part_edges.len() > remaining_edges
                        || part_bytes > remaining_bytes
                    {
                        return Err(RemoteGatewayServiceError::Query(
                            "procedure snapshot projection exceeds its catalog budget".into(),
                        ));
                    }
                    remaining_vertices -= part_vertices.len();
                    remaining_edges -= part_edges.len();
                    remaining_bytes -= part_bytes;
                    ensure_projection_budget_before_next_shard(
                        placement_index + 1 < placements.len(),
                        remaining_vertices,
                        remaining_edges,
                        remaining_bytes,
                        "snapshot",
                    )?;
                    if preserve_partitions {
                        partitions.push(SnapshotPartition::new(
                            placement.shard_id(),
                            part_vertices.into_values().collect(),
                            part_edges.into_values().collect(),
                        ));
                    } else {
                        vertices.extend(part_vertices);
                        edges.extend(part_edges);
                    }
                }
                if preserve_partitions {
                    return Ok(ProcedureProjection::with_graph(
                        partitioned_snapshot_projection(partitions)?,
                        scope,
                        job_limits,
                        resolved.transaction_time(),
                    ));
                }
                apply_snapshot_overlay(
                    &mut vertices,
                    &mut edges,
                    graph_overlay.visible_projection(valid_time),
                    projection_limits.max_vertices(),
                    projection_limits.max_edges(),
                    &mut remaining_bytes,
                )?;
                let vertex_ids = vertices.values().copied().collect::<BTreeSet<_>>();
                let graph = SnapshotGraph::new(
                    vertex_ids.into_iter().collect(),
                    edges.into_values().collect(),
                    true,
                )
                .map_err(|error| RemoteGatewayServiceError::Query(error.to_string()))?;
                Ok(ProcedureProjection::with_graph(
                    Arc::new(ProjectedGraph::Snapshot(graph)),
                    scope,
                    job_limits,
                    resolved.transaction_time(),
                ))
            }
            GraphModel::Event => {
                if !graph_overlay.is_empty() {
                    return Err(RemoteGatewayServiceError::Query(
                        "event analytics cannot project an explicit transaction overlay".into(),
                    ));
                }
                let mut vertices = BTreeSet::new();
                let mut events = Vec::new();
                let mut remaining_vertices = projection_limits.max_vertices();
                let mut remaining_events = projection_limits.max_edges();
                let mut remaining_bytes = projection_limits.max_bytes();
                let placements = routing.deployment.all_shards();
                for (placement_index, placement) in placements.iter().enumerate() {
                    let adapter = ShardClientStorageAdapter::new(
                        Arc::clone(&client),
                        routing.graph.graph_id(),
                        placement.shard_id(),
                        placement.placement_epoch(),
                        deadline_unix_ms,
                        request_namespace(request_id, placement.shard_id()),
                    )
                    .map_err(|error| RemoteGatewayServiceError::Query(error.to_string()))?;
                    let (part, part_bytes) = project_event_part_bounded(
                        &TemporalStore::new(adapter),
                        temporal_storage::GraphId::new(routing.graph.graph_id()),
                        resolved.transaction_time(),
                        None,
                        None,
                        ProjectionLimits::new(
                            remaining_vertices.max(1),
                            remaining_events.max(1),
                            remaining_bytes.max(1),
                        )
                        .map_err(|error| RemoteGatewayServiceError::Query(error.to_string()))?,
                    )
                    .await
                    .map_err(|error| RemoteGatewayServiceError::Query(error.to_string()))?;
                    if part.vertices().len() > remaining_vertices
                        || part.events().len() > remaining_events
                        || part_bytes > remaining_bytes
                    {
                        return Err(RemoteGatewayServiceError::Query(
                            "event procedure projection exceeds its catalog budget".into(),
                        ));
                    }
                    remaining_vertices -= part.vertices().len();
                    remaining_events -= part.events().len();
                    remaining_bytes -= part_bytes;
                    ensure_projection_budget_before_next_shard(
                        placement_index + 1 < placements.len(),
                        remaining_vertices,
                        remaining_events,
                        remaining_bytes,
                        "event",
                    )?;
                    vertices.extend(part.vertices().iter().copied());
                    events.extend(part.events().iter().cloned());
                }
                let graph = EventGraph::new(vertices.into_iter().collect(), events)
                    .map_err(|error| RemoteGatewayServiceError::Query(error.to_string()))?;
                Ok(ProcedureProjection::with_graph(
                    Arc::new(ProjectedGraph::Event(graph)),
                    scope,
                    job_limits,
                    resolved.transaction_time(),
                ))
            }
            GraphModel::Interval => {
                let window = match resolved.valid_time() {
                    query_executor::ResolvedValidTime::Interval { start, end } => {
                        Interval::new(start, Some(end))
                            .map_err(|error| RemoteGatewayServiceError::Query(error.to_string()))?
                    }
                    query_executor::ResolvedValidTime::Point(_) => {
                        return Err(RemoteGatewayServiceError::Query(
                            "interval analytics procedure requires an interval valid-time scope"
                                .into(),
                        ));
                    }
                };
                let mut vertices = Vec::new();
                let mut edges = Vec::new();
                let mut remaining_vertices = projection_limits.max_vertices();
                let mut remaining_edges = projection_limits.max_edges();
                let mut remaining_bytes = projection_limits.max_bytes();
                let placements = routing.deployment.all_shards();
                for (placement_index, placement) in placements.iter().enumerate() {
                    let adapter = ShardClientStorageAdapter::new(
                        Arc::clone(&client),
                        routing.graph.graph_id(),
                        placement.shard_id(),
                        placement.placement_epoch(),
                        deadline_unix_ms,
                        request_namespace(request_id, placement.shard_id()),
                    )
                    .map_err(|error| RemoteGatewayServiceError::Query(error.to_string()))?;
                    let (part, usage) = project_interval_part_bounded(
                        &TemporalStore::new(adapter),
                        temporal_storage::GraphId::new(routing.graph.graph_id()),
                        window,
                        resolved.transaction_time(),
                        None,
                        ProjectionLimits::new(
                            remaining_vertices.max(1),
                            remaining_edges.max(1),
                            remaining_bytes.max(1),
                        )
                        .map_err(|error| RemoteGatewayServiceError::Query(error.to_string()))?,
                    )
                    .await
                    .map_err(|error| RemoteGatewayServiceError::Query(error.to_string()))?;
                    if usage.vertices() > remaining_vertices
                        || usage.edges() > remaining_edges
                        || usage.bytes() > remaining_bytes
                    {
                        return Err(RemoteGatewayServiceError::Query(
                            "procedure interval projection exceeds its catalog budget".into(),
                        ));
                    }
                    remaining_vertices -= usage.vertices();
                    remaining_edges -= usage.edges();
                    remaining_bytes -= usage.bytes();
                    ensure_projection_budget_before_next_shard(
                        placement_index + 1 < placements.len(),
                        remaining_vertices,
                        remaining_edges,
                        remaining_bytes,
                        "interval",
                    )?;
                    let (part_vertices, part_edges) = part.into_parts();
                    vertices.extend(part_vertices);
                    edges.extend(part_edges);
                }
                let graph = IntervalGraph::new(vertices, edges, true)
                    .map_err(|error| RemoteGatewayServiceError::Query(error.to_string()))?;
                Ok(ProcedureProjection::with_graph(
                    Arc::new(ProjectedGraph::Interval(graph)),
                    scope,
                    job_limits,
                    resolved.transaction_time(),
                ))
            }
            GraphModel::Delta => {
                let (from_valid_time, to_valid_time) = match resolved.valid_time() {
                    query_executor::ResolvedValidTime::Interval { start, end } => (start, end),
                    query_executor::ResolvedValidTime::Point(_) => {
                        return Err(RemoteGatewayServiceError::Query(
                            "delta analytics procedure requires an interval valid-time scope"
                                .into(),
                        ));
                    }
                };
                let mut vertices = Vec::new();
                let mut edges = Vec::new();
                let mut remaining_vertices = projection_limits.max_vertices();
                let mut remaining_edges = projection_limits.max_edges();
                let mut remaining_bytes = projection_limits.max_bytes();
                let placements = routing.deployment.all_shards();
                for (placement_index, placement) in placements.iter().enumerate() {
                    let adapter = ShardClientStorageAdapter::new(
                        Arc::clone(&client),
                        routing.graph.graph_id(),
                        placement.shard_id(),
                        placement.placement_epoch(),
                        deadline_unix_ms,
                        request_namespace(request_id, placement.shard_id()),
                    )
                    .map_err(|error| RemoteGatewayServiceError::Query(error.to_string()))?;
                    let (part, usage) = project_valid_time_delta_part_bounded(
                        &TemporalStore::new(adapter),
                        temporal_storage::GraphId::new(routing.graph.graph_id()),
                        from_valid_time,
                        to_valid_time,
                        resolved.transaction_time(),
                        None,
                        ProjectionLimits::new(
                            remaining_vertices.max(1),
                            remaining_edges.max(1),
                            remaining_bytes.max(1),
                        )
                        .map_err(|error| RemoteGatewayServiceError::Query(error.to_string()))?,
                    )
                    .await
                    .map_err(|error| RemoteGatewayServiceError::Query(error.to_string()))?;
                    if usage.vertices() > remaining_vertices
                        || usage.edges() > remaining_edges
                        || usage.bytes() > remaining_bytes
                    {
                        return Err(RemoteGatewayServiceError::Query(
                            "procedure delta projection exceeds its catalog budget".into(),
                        ));
                    }
                    remaining_vertices -= usage.vertices();
                    remaining_edges -= usage.edges();
                    remaining_bytes -= usage.bytes();
                    ensure_projection_budget_before_next_shard(
                        placement_index + 1 < placements.len(),
                        remaining_vertices,
                        remaining_edges,
                        remaining_bytes,
                        "delta",
                    )?;
                    let (part_vertices, part_edges) = part.into_parts();
                    vertices.extend(part_vertices);
                    edges.extend(part_edges);
                }
                Ok(ProcedureProjection::with_graph(
                    Arc::new(ProjectedGraph::Delta(DeltaGraph::new(vertices, edges))),
                    scope,
                    job_limits,
                    resolved.transaction_time(),
                ))
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn hydrate_staged_edge_endpoints(
        &self,
        request_id: u128,
        deadline_unix_ms: u64,
        routing: &GatewayRoutingState,
        compiled: &CompiledQuery,
        parameters: BTreeMap<String, RuntimeValue>,
        current_valid_time: ValidTime,
        query_snapshot: TransactionTime,
        mut graph_overlay: GraphOverlay,
    ) -> Result<GraphOverlay, RemoteGatewayServiceError> {
        let resolved = resolve_compiled_temporal_scope(
            compiled,
            routing.graph.graph_id(),
            current_valid_time,
            query_snapshot,
            parameters,
        )
        .map_err(|error| RemoteGatewayServiceError::Query(error.to_string()))?;
        if resolved.transaction_time() != query_snapshot {
            return Ok(graph_overlay);
        }
        let valid_time = match resolved.valid_time() {
            query_executor::ResolvedValidTime::Point(valid_time) => valid_time,
            query_executor::ResolvedValidTime::Interval { .. } => return Ok(graph_overlay),
        };
        let endpoints = graph_overlay
            .visible_relationship_endpoints(valid_time)
            .into_iter()
            .filter(|endpoint| !graph_overlay.contains_visible(*endpoint, valid_time))
            .collect::<Vec<_>>();
        if endpoints.is_empty() {
            return Ok(graph_overlay);
        }
        let lookup_end = valid_time
            .as_micros()
            .checked_add(1)
            .map(ValidTime::from_micros)
            .ok_or_else(|| {
                RemoteGatewayServiceError::Query(
                    "relationship endpoint lookup valid time overflow".into(),
                )
            })?;
        let lookup_valid = Interval::new(valid_time, Some(lookup_end))
            .map_err(|error| RemoteGatewayServiceError::Query(error.to_string()))?;
        let client: Arc<dyn ShardClient> = self.shard_client.clone();
        let mut hydrated = Vec::new();
        for endpoint in endpoints {
            let scope = temporal_ir::GraphScope::new(endpoint.graph(), endpoint.partition());
            let placement = routing.deployment.route_scope(scope);
            let adapter = ShardClientStorageAdapter::new(
                Arc::clone(&client),
                routing.graph.graph_id(),
                placement.shard_id(),
                placement.placement_epoch(),
                deadline_unix_ms,
                request_namespace(request_id, placement.shard_id()),
            )
            .map_err(|error| RemoteGatewayServiceError::Query(error.to_string()))?;
            if let Some(vertex) = TemporalStore::new(adapter)
                .vertex_view_as_of(endpoint, valid_time, query_snapshot)
                .await
                .map_err(|error| RemoteGatewayServiceError::Query(error.to_string()))?
            {
                hydrated.push(
                    GraphOverlayEntry::put(
                        placement.shard_id(),
                        lookup_valid,
                        RuntimeValue::Node(VertexRecord::from(vertex)),
                    )
                    .map_err(|error| RemoteGatewayServiceError::Query(error.to_string()))?,
                );
            }
        }
        graph_overlay
            .stage(hydrated)
            .map_err(|error| RemoteGatewayServiceError::Query(error.to_string()))?;
        Ok(graph_overlay)
    }

    #[allow(clippy::too_many_arguments)]
    async fn execute_cypher_response(
        &self,
        request_id: u128,
        deadline_unix_ms: u64,
        routing: &GatewayRoutingState,
        text: &str,
        parameters: BTreeMap<String, RuntimeValue>,
        fixed_snapshot: Option<TransactionTime>,
        graph_overlay: GraphOverlay,
        compiled: &CompiledQuery,
        child_prefix: Option<(&temporal_ir::LogicalPlan, &BTreeMap<String, RuntimeValue>)>,
    ) -> Result<CypherQueryResponse, RemoteGatewayServiceError> {
        self.execute_cypher_response_with_benchmark(
            request_id,
            deadline_unix_ms,
            routing,
            text,
            parameters,
            fixed_snapshot,
            graph_overlay,
            compiled,
            child_prefix,
            #[cfg(feature = "paper-benchmark-control")]
            None,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn execute_cypher_response_with_benchmark(
        &self,
        request_id: u128,
        deadline_unix_ms: u64,
        routing: &GatewayRoutingState,
        text: &str,
        parameters: BTreeMap<String, RuntimeValue>,
        fixed_snapshot: Option<TransactionTime>,
        graph_overlay: GraphOverlay,
        compiled: &CompiledQuery,
        child_prefix: Option<(&temporal_ir::LogicalPlan, &BTreeMap<String, RuntimeValue>)>,
        #[cfg(feature = "paper-benchmark-control")] benchmark: Option<&BenchmarkQueryLease>,
    ) -> Result<CypherQueryResponse, RemoteGatewayServiceError> {
        let security_fingerprint = request_security_fingerprint(self.cluster_id, request_id);
        if compiled.uses_procedures() {
            preflight_procedure_parameters(
                compiled.logical_plan().nodes().iter().filter_map(|node| {
                    let temporal_ir::LogicalOperator::ProcedureCall { procedure } = node.operator()
                    else {
                        return None;
                    };
                    Some(procedure)
                }),
                &self.procedure_registry,
                &parameters,
                security_fingerprint,
                &ProcedureAccess::analytics_read(),
            )
            .map_err(|error| RemoteGatewayServiceError::Query(error.to_string()))?;
        }
        let shard_ids = routing
            .deployment
            .all_shards()
            .iter()
            .map(|placement| placement.shard_id())
            .collect::<Vec<_>>();
        let mut required_applied_indexes = BTreeMap::new();
        if compiled.logical_plan().nodes().iter().any(|node| {
            matches!(
                node.operator(),
                temporal_ir::LogicalOperator::ChangeScan { .. }
            )
        }) {
            let mut barrier_tasks = tokio::task::JoinSet::new();
            for placement in routing.deployment.all_shards() {
                let client = Arc::clone(&self.shard_client);
                let graph_id = routing.graph.graph_id();
                let shard_id = placement.shard_id();
                let placement_epoch = placement.placement_epoch();
                barrier_tasks.spawn(async move {
                    let context = ShardRequestContext::new(
                        graph_id,
                        shard_id,
                        placement_epoch,
                        u128::from(request_namespace(
                            request_id ^ 0x4454_475f_5245_4144,
                            shard_id,
                        )),
                        deadline_unix_ms,
                    )?;
                    client
                        .read_barrier(context)
                        .await
                        .map(|read_index| (shard_id, read_index))
                });
            }
            while let Some(result) = barrier_tasks.join_next().await {
                let (shard_id, read_index) = result
                    .map_err(|error| RemoteGatewayServiceError::Query(error.to_string()))?
                    .map_err(|error| RemoteGatewayServiceError::Query(error.to_string()))?;
                required_applied_indexes.insert(shard_id, read_index);
            }
        }
        let mode = match routing.deployment.mode() {
            DeploymentMode::PrimaryReplica => QueryDeploymentMode::PrimaryReplica,
            DeploymentMode::SharedNothing => QueryDeploymentMode::SharedNothing,
        };
        let limits = ResourceLimits::new(64 << 20, 256 << 20, 1_024)
            .map_err(|error| RemoteGatewayServiceError::Query(error.to_string()))?;
        let config = EngineConfig::new(
            routing.graph.name(),
            routing.graph.graph_id(),
            routing.graph.schema_version(),
            routing.graph.topology().epoch(),
            mode,
            shard_ids,
            limits,
        )
        .map_err(|error| RemoteGatewayServiceError::Query(error.to_string()))?;
        let mut coordinator = DistributedCoordinator::new(64 << 20, 64)
            .map_err(|error| RemoteGatewayServiceError::Query(error.to_string()))?;
        let client: Arc<dyn ShardClient> = self.shard_client.clone();
        let shared_adapters = RoutedShardReadAdapter::build_negotiated_adapters(
            Arc::clone(&client),
            routing.graph.graph_id(),
            &routing.deployment,
            deadline_unix_ms,
            request_id,
        )
        .await
        .map_err(|error| RemoteGatewayServiceError::Query(error.to_string()))?;
        for placement in routing.deployment.all_shards() {
            let adapter = RoutedShardReadAdapter::with_shared_adapters(
                routing.graph.graph_id(),
                placement.shard_id(),
                Arc::clone(&routing.deployment),
                Arc::clone(&shared_adapters),
            )
            .map_err(|error| RemoteGatewayServiceError::Query(error.to_string()))?;
            coordinator
                .register(Arc::new(LocalFragmentWorker::new(
                    placement.shard_id(),
                    routing.graph.graph_id(),
                    routing.graph.schema_version(),
                    routing.graph.topology().epoch(),
                    security_fingerprint,
                    TemporalBatchExecutor::new(TemporalStore::new(adapter)),
                )))
                .map_err(|error| RemoteGatewayServiceError::Query(error.to_string()))?;
        }
        let query_snapshot = if let Some(snapshot) = fixed_snapshot {
            snapshot
        } else {
            self.meta_timestamp_client
                .allocate_read_snapshot(request_id, deadline_unix_ms)
                .await
                .map_err(|error| RemoteGatewayServiceError::Meta(error.to_string()))?
        };
        let wall_clock_valid_time = ValidTime::from_micros(unix_time_micros()?);
        let current_valid_time = if child_prefix.is_some() {
            match resolve_compiled_temporal_scope(
                compiled,
                routing.graph.graph_id(),
                wall_clock_valid_time,
                query_snapshot,
                parameters.clone(),
            )
            .map_err(|error| RemoteGatewayServiceError::Query(error.to_string()))?
            .valid_time()
            {
                query_executor::ResolvedValidTime::Point(value) => value,
                query_executor::ResolvedValidTime::Interval { .. } => {
                    return Err(RemoteGatewayServiceError::Query(
                        "child-local write prefixes require a point valid-time scope".into(),
                    ));
                }
            }
        } else {
            wall_clock_valid_time
        };
        let graph_overlay = if graph_overlay.is_empty() {
            graph_overlay
        } else {
            self.hydrate_staged_edge_endpoints(
                request_id,
                deadline_unix_ms,
                routing,
                compiled,
                parameters.clone(),
                current_valid_time,
                query_snapshot,
                graph_overlay,
            )
            .await?
        };
        let procedure_projection = if compiled.uses_procedures() {
            Some(
                self.project_procedure_graph(
                    request_id ^ 0x4454_475f_5052_4f43,
                    deadline_unix_ms,
                    routing,
                    compiled,
                    parameters.clone(),
                    current_valid_time,
                    query_snapshot,
                    &graph_overlay,
                )
                .await?,
            )
        } else {
            None
        };
        let mut request = CypherQueryRequest::new(
            text,
            parameters,
            current_valid_time,
            query_snapshot,
            security_fingerprint,
            deadline_unix_ms,
        )
        .with_graph_overlay(graph_overlay)
        .with_required_applied_indexes(required_applied_indexes);
        if let Some(projection) = procedure_projection {
            let job_context = JobInvocationContext::new(
                request_id,
                deadline_unix_ms,
                routing.graph.graph_id(),
                routing.revision,
                routing.graph.topology().epoch(),
                routing.graph.schema_version(),
                routing.graph.backend().generation(),
                projection.transaction_snapshot,
                projection.scope,
                projection.limits,
            )
            .map_err(|error| RemoteGatewayServiceError::Query(error.to_string()))?;
            request = request
                .with_procedure_runtime_optional_graph(
                    Arc::clone(&self.procedure_registry),
                    projection.graph,
                    ProcedureAccess::analytics_read(),
                )
                .with_job_invocation_context(job_context);
        }
        if let Some(snapshot) = fixed_snapshot {
            request = request.with_fixed_transaction_snapshot(snapshot);
        }
        #[cfg(feature = "paper-benchmark-control")]
        if let Some(lease) = benchmark {
            request = request.with_benchmark_ablations(lease.config(), lease.counters());
        }
        let engine = CypherQueryEngine::new(config);
        let response = if let Some((logical_plan, bindings)) = child_prefix {
            engine
                .execute_child_read_prefix(
                    &coordinator,
                    logical_plan,
                    bindings,
                    compiled.fingerprint(),
                    request,
                )
                .await
        } else {
            engine
                .execute_compiled(&coordinator, compiled, request)
                .await
        };
        response.map_err(|error| RemoteGatewayServiceError::Query(error.to_string()))
    }

    async fn allocate_transaction_timestamps(
        &self,
        request_id: u128,
        deadline_unix_ms: u64,
    ) -> Result<(TransactionTime, TransactionTime), RemoteGatewayServiceError> {
        self.meta_timestamp_client
            .allocate_transaction_timestamps(request_id, deadline_unix_ms)
            .await
            .map_err(|error| RemoteGatewayServiceError::Meta(error.to_string()))
    }
}

fn job_projection_scope(
    model: GraphModel,
    valid_time: query_executor::ResolvedValidTime,
) -> Result<GraphProjectionScope, RemoteGatewayServiceError> {
    match (model, valid_time) {
        (GraphModel::Snapshot, query_executor::ResolvedValidTime::Point(valid_time)) => {
            Ok(GraphProjectionScope::Snapshot { valid_time })
        }
        (GraphModel::Event, _) => Ok(GraphProjectionScope::Event),
        (GraphModel::Interval, query_executor::ResolvedValidTime::Interval { start, end }) => {
            Ok(GraphProjectionScope::Interval {
                valid_from: start,
                valid_to: end,
            })
        }
        (GraphModel::Delta, query_executor::ResolvedValidTime::Interval { start, end }) => {
            Ok(GraphProjectionScope::Delta {
                before: start,
                after: end,
            })
        }
        (GraphModel::Snapshot, query_executor::ResolvedValidTime::Interval { .. })
        | (GraphModel::Interval | GraphModel::Delta, query_executor::ResolvedValidTime::Point(_)) => {
            Err(RemoteGatewayServiceError::Query(
                "procedure graph model does not match the resolved valid-time scope".into(),
            ))
        }
    }
}

fn ensure_projection_budget_before_next_shard(
    has_more_shards: bool,
    remaining_vertices: usize,
    remaining_edges: usize,
    remaining_bytes: u64,
    model: &str,
) -> Result<(), RemoteGatewayServiceError> {
    if !has_more_shards {
        return Ok(());
    }
    let exhausted = if remaining_vertices == 0 {
        Some("vertex")
    } else if remaining_edges == 0 {
        Some("edge")
    } else if remaining_bytes == 0 {
        Some("byte")
    } else {
        None
    };
    if let Some(resource) = exhausted {
        return Err(RemoteGatewayServiceError::Query(format!(
            "procedure {model} projection exhausted its {resource} budget before all shards were scanned"
        )));
    }
    Ok(())
}

fn partitioned_snapshot_projection(
    partitions: Vec<SnapshotPartition>,
) -> Result<Arc<ProjectedGraph>, RemoteGatewayServiceError> {
    PartitionedSnapshotGraph::new(partitions, true)
        .map(ProjectedGraph::PartitionedSnapshot)
        .map(Arc::new)
        .map_err(|error| RemoteGatewayServiceError::Query(error.to_string()))
}

fn apply_snapshot_overlay(
    vertices: &mut BTreeMap<temporal_storage::ElementRef, VertexId>,
    edges: &mut BTreeMap<temporal_storage::ElementRef, SnapshotEdge>,
    overlay: BTreeMap<temporal_storage::ElementRef, Option<RuntimeValue>>,
    max_vertices: usize,
    max_edges: usize,
    remaining_bytes: &mut u64,
) -> Result<(), RemoteGatewayServiceError> {
    for (element, replacement) in overlay {
        match replacement {
            Some(RuntimeValue::Node(node)) => {
                charge_overlay_bytes(node.payload(), remaining_bytes)?;
                vertices.insert(element, VertexId::new(node.element().id().value()));
            }
            Some(RuntimeValue::Relationship(edge)) => {
                charge_overlay_bytes(edge.payload(), remaining_bytes)?;
                edges.insert(
                    element,
                    SnapshotEdge::new(
                        VertexId::new(edge.source_ref().id().value()),
                        VertexId::new(edge.destination_ref().id().value()),
                        1.0,
                    )
                    .map_err(|error| RemoteGatewayServiceError::Query(error.to_string()))?,
                );
            }
            Some(_) => {
                return Err(RemoteGatewayServiceError::Query(
                    "procedure overlay contains a non-graph value".into(),
                ));
            }
            None if element.kind() == temporal_storage::ElementKind::Vertex => {
                vertices.remove(&element);
            }
            None => {
                edges.remove(&element);
            }
        }
    }
    let vertex_ids = vertices.values().copied().collect::<BTreeSet<_>>();
    edges.retain(|_, edge| {
        vertex_ids.contains(&edge.source()) && vertex_ids.contains(&edge.destination())
    });
    if vertices.len() > max_vertices {
        return Err(RemoteGatewayServiceError::Query(
            "procedure snapshot projection exceeds its vertex budget".into(),
        ));
    }
    if edges.len() > max_edges {
        return Err(RemoteGatewayServiceError::Query(
            "procedure snapshot projection exceeds its edge budget".into(),
        ));
    }
    Ok(())
}

fn charge_overlay_bytes(
    payload: &temporal_types::CanonicalElement,
    remaining_bytes: &mut u64,
) -> Result<(), RemoteGatewayServiceError> {
    let bytes = u64::try_from(
        payload
            .encode()
            .map_err(|error| RemoteGatewayServiceError::Query(error.to_string()))?
            .len(),
    )
    .map_err(|_| {
        RemoteGatewayServiceError::Query("procedure overlay byte count overflow".into())
    })?;
    if bytes > *remaining_bytes {
        return Err(RemoteGatewayServiceError::Query(
            "procedure snapshot projection exceeds its byte budget".into(),
        ));
    }
    *remaining_bytes -= bytes;
    Ok(())
}

fn submitted_algorithm_name(
    procedure: &temporal_ir::ResolvedProcedure,
    parameters: &BTreeMap<String, RuntimeValue>,
) -> Result<String, RemoteGatewayServiceError> {
    let expression = procedure
        .arguments()
        .iter()
        .find(|argument| argument.name() == "algorithm")
        .map(temporal_ir::ProcedureArgument::expression)
        .ok_or_else(|| {
            RemoteGatewayServiceError::Query(
                "analytics submit requires an algorithm argument".into(),
            )
        })?;
    match expression {
        temporal_ir::ScalarExpr::Literal(temporal_types::GraphValue::String(value)) => {
            Ok(value.clone())
        }
        temporal_ir::ScalarExpr::Parameter(name) => match parameters.get(name) {
            Some(RuntimeValue::String(value)) => Ok(value.clone()),
            _ => Err(RemoteGatewayServiceError::Query(
                "analytics submit algorithm parameter must be a String".into(),
            )),
        },
        _ => Err(RemoteGatewayServiceError::Query(
            "analytics submit algorithm must be a literal or query parameter".into(),
        )),
    }
}

impl BoltQueryBackend for RemoteGatewayService {
    fn execute<'a>(&'a self, request: BoltQueryRequest) -> BackendFuture<'a, BackendQueryResult> {
        Box::pin(async move {
            let sequence = self.bolt_request_sequence.fetch_add(1, Ordering::Relaxed);
            if sequence == u64::MAX {
                return Err(bolt_server::ServiceError::new(
                    "Neo.TransientError.General.DatabaseUnavailable",
                    "Bolt request identity space exhausted",
                ));
            }
            let request_id = (u128::from(self.bolt_request_nonce) << 64) | u128::from(sequence);
            let deadline = unix_time_ms()
                .map_err(|error| {
                    bolt_server::ServiceError::new(
                        "Neo.TransientError.General.DatabaseUnavailable",
                        error.to_string(),
                    )
                })?
                .checked_add(30_000)
                .ok_or_else(|| {
                    bolt_server::ServiceError::new(
                        "Neo.ClientError.Request.Invalid",
                        "Bolt query deadline overflow",
                    )
                })?;
            let transaction = request.transaction();
            let routing = if let Some(transaction) = transaction {
                self.bolt_transactions
                    .lock()
                    .map_err(|_| {
                        bolt_server::ServiceError::new(
                            "Neo.DatabaseError.General.UnknownError",
                            "Bolt transaction state lock is poisoned",
                        )
                    })?
                    .get(&transaction.value())
                    .map(|pending| pending.routing.clone())
                    .ok_or_else(|| {
                        bolt_server::ServiceError::new(
                            "Neo.ClientError.Transaction.TransactionNotFound",
                            "Bolt transaction is no longer active",
                        )
                    })?
            } else {
                self.routing_snapshot().map_err(gateway_bolt_error)?
            };
            let compiled = compile_cypher(&routing, request.query()).map_err(gateway_bolt_error)?;
            #[cfg(feature = "paper-benchmark-control")]
            let benchmark_lease = self.acquire_benchmark_query(
                benchmark_session_token(request.extra())?,
                transaction,
                compiled.is_read_only(),
            )?;
            #[cfg(not(feature = "paper-benchmark-control"))]
            reject_benchmark_session_when_control_is_disabled(request.extra())?;
            if !compiled.is_read_only() {
                if let Some(transaction) = transaction {
                    if mutation_plan_has_batch_subtransaction(compiled.mutation_plan()) {
                        return Err(bolt_server::ServiceError::new(
                            "Neo.ClientError.Transaction.InvalidType",
                            "DTG-CYPHER-IN-TRANSACTIONS-EXPLICIT: IN TRANSACTIONS requires auto-commit execution",
                        ));
                    }
                    let (fixed_timestamps, graph_overlay, pending_revision, existing_writes) = self
                        .bolt_transactions
                        .lock()
                        .map_err(|_| {
                            bolt_server::ServiceError::new(
                                "Neo.DatabaseError.General.UnknownError",
                                "Bolt transaction state lock is poisoned",
                            )
                        })?
                        .get(&transaction.value())
                        .map(|pending| {
                            (
                                (pending.start_ts, pending.commit_ts),
                                pending.graph_overlay.clone(),
                                pending.revision,
                                pending.writes.clone(),
                            )
                        })
                        .ok_or_else(|| {
                            bolt_server::ServiceError::new(
                                "Neo.ClientError.Transaction.TransactionNotFound",
                                "Bolt transaction is no longer active",
                            )
                        })?;
                    let prepared = self
                        .prepare_cypher_writes(
                            request_id,
                            deadline,
                            &routing,
                            &compiled,
                            request.query(),
                            request.parameters().clone(),
                            graph_overlay.clone(),
                            Some(fixed_timestamps),
                            None,
                        )
                        .await
                        .map_err(gateway_bolt_error)?;
                    validate_pending_statement(&existing_writes, &prepared).map_err(|error| {
                        bolt_server::ServiceError::new(
                            "Neo.ClientError.Transaction.TransactionLimitReached",
                            error.to_string(),
                        )
                    })?;
                    let (candidate_context, candidate_scoped, candidate_constraints) =
                        pending_candidate_commit_inputs(&existing_writes, &prepared).map_err(
                            |error| {
                                bolt_server::ServiceError::new(
                                    "Neo.ClientError.Transaction.TransactionLimitReached",
                                    error.to_string(),
                                )
                            },
                        )?;
                    if let Some(context) = candidate_context {
                        let client: Arc<dyn ShardClient> = self.shard_client.clone();
                        TransactionCoordinator::remote(self.max_raft_ticks)
                            .validate_temporal_remote_candidate(
                                client,
                                &routing.deployment,
                                routing.graph.graph_id(),
                                deadline,
                                context,
                                candidate_scoped,
                                candidate_constraints,
                            )
                            .await
                            .map_err(|error| {
                                bolt_server::ServiceError::new(
                                    "Neo.ClientError.Transaction.TransactionLimitReached",
                                    error.to_string(),
                                )
                            })?;
                    }
                    let mut transactions = self.bolt_transactions.lock().map_err(|_| {
                        bolt_server::ServiceError::new(
                            "Neo.DatabaseError.General.UnknownError",
                            "Bolt transaction state lock is poisoned",
                        )
                    })?;
                    let pending = transactions.get_mut(&transaction.value()).ok_or_else(|| {
                        bolt_server::ServiceError::new(
                            "Neo.ClientError.Transaction.TransactionNotFound",
                            "Bolt transaction is no longer active",
                        )
                    })?;
                    let PendingBoltTransaction {
                        revision,
                        graph_overlay,
                        writes,
                        ..
                    } = pending;
                    publish_pending_statement(
                        revision,
                        graph_overlay,
                        writes,
                        pending_revision,
                        prepared,
                        MAX_TRANSACTION_MUTATIONS,
                    )?;
                    return Ok(BackendQueryResult::new(
                        Vec::new(),
                        Vec::new(),
                        BTreeMap::from([(
                            "dtg_transaction_state".into(),
                            bolt_protocol::Value::String("staged".into()),
                        )]),
                    ));
                }
                if let Some(batch_rows) = mutation_plan_batch_rows(compiled.mutation_plan())
                    .map_err(gateway_bolt_error)?
                {
                    let summary = self
                        .execute_cypher_batch_write(
                            request_id,
                            deadline,
                            &routing,
                            &compiled,
                            request.query(),
                            request.parameters().clone(),
                            batch_rows,
                        )
                        .await
                        .map_err(gateway_bolt_error)?;
                    let transaction_id = summary
                        .get("transaction_id")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_owned();
                    let mut metadata = BTreeMap::from([
                        ("type".into(), bolt_protocol::Value::String("w".into())),
                        (
                            "dtg_transaction_id".into(),
                            bolt_protocol::Value::String(transaction_id),
                        ),
                        (
                            "dtg_write_summary".into(),
                            bolt_protocol::Value::String(summary.to_string()),
                        ),
                    ]);
                    if summary
                        .get("commit_ts")
                        .is_some_and(|value| !value.is_null())
                    {
                        metadata.insert(
                            "bookmark".into(),
                            bolt_protocol::Value::String(
                                bookmark_from_commit_summary(&summary)
                                    .map_err(gateway_bolt_error)?,
                            ),
                        );
                    }
                    return Ok(BackendQueryResult::new(Vec::new(), Vec::new(), metadata));
                }
                let prepared = self
                    .prepare_cypher_writes(
                        request_id,
                        deadline,
                        &routing,
                        &compiled,
                        request.query(),
                        request.parameters().clone(),
                        GraphOverlay::default(),
                        None,
                        None,
                    )
                    .await
                    .map_err(gateway_bolt_error)?;
                let overlay = prepared
                    .iter()
                    .flat_map(|write| write.overlay_values.iter().cloned())
                    .collect::<Vec<_>>();
                let summary = self
                    .commit_prepared_cypher_writes(&routing, deadline, prepared)
                    .await
                    .map_err(gateway_bolt_error)?;
                let transaction_id = summary
                    .get("transaction_id")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned();
                let bookmark =
                    bookmark_from_commit_summary(&summary).map_err(gateway_bolt_error)?;
                let has_return = request.query().to_ascii_uppercase().contains("RETURN");
                let records = if has_return {
                    overlay.into_iter().map(|value| vec![value]).collect()
                } else {
                    Vec::new()
                };
                let fields = if has_return {
                    vec!["result".into()]
                } else {
                    Vec::new()
                };
                return Ok(BackendQueryResult::new(
                    fields,
                    records,
                    BTreeMap::from([
                        ("type".into(), bolt_protocol::Value::String("w".into())),
                        (
                            "dtg_transaction_id".into(),
                            bolt_protocol::Value::String(transaction_id),
                        ),
                        (
                            "dtg_write_summary".into(),
                            bolt_protocol::Value::String(summary.to_string()),
                        ),
                        ("bookmark".into(), bolt_protocol::Value::String(bookmark)),
                    ]),
                ));
            }
            let transaction_read = transaction
                .map(|transaction| {
                    self.bolt_transactions
                        .lock()
                        .map_err(|_| {
                            bolt_server::ServiceError::new(
                                "Neo.DatabaseError.General.UnknownError",
                                "Bolt transaction state lock is poisoned",
                            )
                        })?
                        .get(&transaction.value())
                        .map(|pending| (pending.start_ts, pending.graph_overlay.clone()))
                        .ok_or_else(|| {
                            bolt_server::ServiceError::new(
                                "Neo.ClientError.Transaction.TransactionNotFound",
                                "Bolt transaction is no longer active",
                            )
                        })
                })
                .transpose()?;
            let (fixed_snapshot, graph_overlay) = transaction_read
                .map_or((None, GraphOverlay::default()), |(snapshot, overlay)| {
                    (Some(snapshot), overlay)
                });
            let response = self
                .execute_cypher_response_with_benchmark(
                    request_id,
                    deadline,
                    &routing,
                    request.query(),
                    request.parameters().clone(),
                    fixed_snapshot,
                    graph_overlay,
                    &compiled,
                    None,
                    #[cfg(feature = "paper-benchmark-control")]
                    benchmark_lease.as_ref(),
                )
                .await
                .map_err(gateway_bolt_error)?;
            let fields = response
                .schema()
                .columns()
                .iter()
                .map(|column| column.name().to_owned())
                .collect::<Vec<_>>();
            let records = response
                .batches()
                .iter()
                .flat_map(|batch| batch.rows().iter().cloned())
                .collect::<Vec<_>>();
            #[cfg(feature = "paper-benchmark-control")]
            if let Some(lease) = benchmark_lease {
                lease.complete().map_err(benchmark_bolt_error)?;
            }
            Ok(BackendQueryResult::new(
                fields,
                records,
                BTreeMap::from([(
                    "dtg_query_fingerprint".into(),
                    bolt_protocol::Value::String(hex_bytes(&response.fingerprint())),
                )]),
            ))
        })
    }

    fn begin<'a>(
        &'a self,
        _extra: BTreeMap<String, bolt_protocol::Value>,
    ) -> BackendFuture<'a, bolt_server::TransactionId> {
        Box::pin(async move {
            let id = self.bolt_request_sequence.fetch_add(1, Ordering::Relaxed);
            if id == u64::MAX {
                return Err(bolt_server::ServiceError::new(
                    "Neo.TransientError.General.DatabaseUnavailable",
                    "Bolt transaction identity space exhausted",
                ));
            }
            let request_id = (u128::from(self.bolt_request_nonce) << 64) | u128::from(id);
            let routing = self.routing_snapshot().map_err(gateway_bolt_error)?;
            let deadline = unix_time_ms()
                .map_err(|error| {
                    bolt_server::ServiceError::new(
                        "Neo.ClientError.Request.Invalid",
                        error.to_string(),
                    )
                })?
                .checked_add(30_000)
                .ok_or_else(|| {
                    bolt_server::ServiceError::new(
                        "Neo.ClientError.Request.Invalid",
                        "Bolt transaction deadline overflow",
                    )
                })?;
            let (start_ts, commit_ts) = self
                .allocate_transaction_timestamps(request_id, deadline)
                .await
                .map_err(gateway_bolt_error)?;
            let mut transactions = self.bolt_transactions.lock().map_err(|_| {
                bolt_server::ServiceError::new(
                    "Neo.DatabaseError.General.UnknownError",
                    "Bolt transaction state lock is poisoned",
                )
            })?;
            if transactions
                .insert(
                    id,
                    PendingBoltTransaction {
                        routing,
                        deadline_unix_ms: deadline,
                        start_ts,
                        commit_ts,
                        writes: Vec::new(),
                        graph_overlay: GraphOverlay::new(MAX_TRANSACTION_MUTATIONS).map_err(
                            |error| {
                                bolt_server::ServiceError::new(
                                    "Neo.DatabaseError.General.UnknownError",
                                    error.to_string(),
                                )
                            },
                        )?,
                        revision: 0,
                    },
                )
                .is_some()
            {
                return Err(bolt_server::ServiceError::new(
                    "Neo.TransientError.General.DatabaseUnavailable",
                    "Bolt transaction identity collision",
                ));
            }
            Ok(bolt_server::TransactionId::new(id))
        })
    }

    fn commit<'a>(&'a self, transaction: bolt_server::TransactionId) -> BackendFuture<'a, String> {
        Box::pin(async move {
            let pending = self
                .bolt_transactions
                .lock()
                .map_err(|_| {
                    bolt_server::ServiceError::new(
                        "Neo.DatabaseError.General.UnknownError",
                        "Bolt transaction state lock is poisoned",
                    )
                })?
                .remove(&transaction.value())
                .ok_or_else(|| {
                    bolt_server::ServiceError::new(
                        "Neo.ClientError.Transaction.TransactionNotFound",
                        "Bolt transaction is no longer active",
                    )
                })?;
            let bookmark = if pending.writes.is_empty() {
                format!(
                    "dtg:tx:{}:{}",
                    pending.commit_ts.physical_micros(),
                    pending.commit_ts.logical()
                )
            } else {
                let result = self
                    .commit_prepared_cypher_writes(
                        &pending.routing,
                        pending.deadline_unix_ms,
                        pending.writes,
                    )
                    .await
                    .map_err(gateway_bolt_error)?;
                bookmark_from_commit_summary(&result).map_err(gateway_bolt_error)?
            };
            Ok(bookmark)
        })
    }

    fn rollback<'a>(&'a self, transaction: bolt_server::TransactionId) -> BackendFuture<'a, ()> {
        Box::pin(async move {
            self.bolt_transactions
                .lock()
                .map_err(|_| {
                    bolt_server::ServiceError::new(
                        "Neo.DatabaseError.General.UnknownError",
                        "Bolt transaction state lock is poisoned",
                    )
                })?
                .remove(&transaction.value())
                .ok_or_else(|| {
                    bolt_server::ServiceError::new(
                        "Neo.ClientError.Transaction.TransactionNotFound",
                        "Bolt transaction is no longer active",
                    )
                })?;
            Ok(())
        })
    }
}

fn gateway_bolt_error(error: RemoteGatewayServiceError) -> bolt_server::ServiceError {
    bolt_server::ServiceError::new(
        "Neo.ClientError.Statement.ExecutionFailed",
        error.to_string(),
    )
}

const BENCHMARK_SESSION_KEY: &str = "dtgproxy.paper.session";

fn benchmark_session_token(
    extra: &BTreeMap<String, bolt_protocol::Value>,
) -> Result<Option<&str>, bolt_server::ServiceError> {
    match extra.get(BENCHMARK_SESSION_KEY) {
        None => Ok(None),
        Some(bolt_protocol::Value::String(token)) if !token.is_empty() => Ok(Some(token)),
        Some(_) => Err(benchmark_request_error(
            "dtgproxy.paper.session must be a non-empty string",
        )),
    }
}

#[cfg(not(feature = "paper-benchmark-control"))]
fn reject_benchmark_session_when_control_is_disabled(
    extra: &BTreeMap<String, bolt_protocol::Value>,
) -> Result<(), bolt_server::ServiceError> {
    if extra.contains_key(BENCHMARK_SESSION_KEY) {
        return Err(benchmark_request_error(
            "benchmark session control is not compiled into this Gateway",
        ));
    }
    Ok(())
}

fn benchmark_request_error(message: &str) -> bolt_server::ServiceError {
    bolt_server::ServiceError::new("Neo.ClientError.Request.Invalid", message)
}

#[cfg(feature = "paper-benchmark-control")]
fn benchmark_bolt_error(error: crate::BenchmarkControlError) -> bolt_server::ServiceError {
    benchmark_request_error(&error.to_string())
}

fn unambiguous_response_bindings(prepared: &[PreparedCypherWrite]) -> Map<String, Value> {
    let mut bindings = Map::new();
    let mut conflicts = BTreeSet::new();
    for write in prepared {
        for (name, value) in &write.bindings {
            if conflicts.contains(name) {
                continue;
            }
            if bindings.get(name).is_some_and(|existing| existing != value) {
                bindings.remove(name);
                conflicts.insert(name.clone());
            } else {
                bindings.insert(name.clone(), value.clone());
            }
        }
    }
    bindings
}

fn validate_pending_statement(
    existing: &[PreparedCypherWrite],
    candidate: &[PreparedCypherWrite],
) -> Result<(), RemoteGatewayServiceError> {
    validate_pending_statement_with_limit(existing, candidate, MAX_TRANSACTION_MUTATIONS)
}

fn mutation_plan_has_merge(plan: &cypher_compiler::MutationPlan) -> bool {
    plan.mutations().iter().any(|mutation| match mutation {
        CompiledMutation::Merge(_) => true,
        CompiledMutation::Subquery(subquery) => mutation_plan_has_merge(subquery.mutation_plan()),
        _ => false,
    })
}

fn mutation_plan_has_batch_subtransaction(plan: &cypher_compiler::MutationPlan) -> bool {
    plan.mutations().iter().any(|mutation| match mutation {
        CompiledMutation::Subquery(subquery) => {
            subquery.batch_rows().is_some()
                || mutation_plan_has_batch_subtransaction(subquery.mutation_plan())
        }
        _ => false,
    })
}

fn mutation_plan_batch_rows(
    plan: &cypher_compiler::MutationPlan,
) -> Result<Option<u32>, RemoteGatewayServiceError> {
    fn collect(
        plan: &cypher_compiler::MutationPlan,
        depth: usize,
        batches: &mut Vec<u32>,
    ) -> Result<(), RemoteGatewayServiceError> {
        for mutation in plan.mutations() {
            let CompiledMutation::Subquery(subquery) = mutation else {
                continue;
            };
            if let Some(batch_rows) = subquery.batch_rows() {
                if depth != 0 {
                    return Err(RemoteGatewayServiceError::Query(
                        "DTG-CYPHER-IN-TRANSACTIONS-NESTED: a batch subtransaction cannot run inside another write child"
                            .into(),
                    ));
                }
                batches.push(batch_rows);
            }
            collect(subquery.mutation_plan(), depth + 1, batches)?;
        }
        Ok(())
    }

    let mut batches = Vec::new();
    collect(plan, 0, &mut batches)?;
    match batches.as_slice() {
        [] => Ok(None),
        [batch_rows] => Ok(Some(*batch_rows)),
        _ => Err(RemoteGatewayServiceError::Query(
            "DTG-CYPHER-IN-TRANSACTIONS-MULTIPLE: one statement may own only one batch subtransaction boundary"
                .into(),
        )),
    }
}

fn mutation_plan_has_child_read_prefix(plan: &cypher_compiler::MutationPlan) -> bool {
    plan.mutations().iter().any(|mutation| match mutation {
        CompiledMutation::Subquery(subquery) => {
            subquery.read_prefix_plan().is_some()
                || mutation_plan_has_child_read_prefix(subquery.mutation_plan())
        }
        _ => false,
    })
}

fn merge_runtime_bindings(
    imports: &BTreeMap<String, RuntimeValue>,
    row: &BTreeMap<String, RuntimeValue>,
) -> Result<BTreeMap<String, RuntimeValue>, RemoteGatewayServiceError> {
    let mut bindings = imports.clone();
    for (name, value) in row {
        if let Some(imported) = bindings.get(name) {
            if imported != value {
                return Err(RemoteGatewayServiceError::Query(format!(
                    "DTG-CYPHER-SUBQUERY-IMPORT-MISMATCH: child prefix changed imported binding {name}"
                )));
            }
            continue;
        }
        bindings.insert(name.clone(), value.clone());
    }
    Ok(bindings)
}

fn publish_pending_statement(
    revision: &mut u64,
    graph_overlay: &mut GraphOverlay,
    writes: &mut Vec<PreparedCypherWrite>,
    expected_revision: u64,
    candidate: Vec<PreparedCypherWrite>,
    maximum_items: usize,
) -> Result<(), bolt_server::ServiceError> {
    if *revision != expected_revision {
        return Err(bolt_server::ServiceError::new(
            "Neo.TransientError.Transaction.ConcurrentAccess",
            "Bolt transaction changed while a statement was being prepared",
        ));
    }
    let next_revision = revision.checked_add(1).ok_or_else(|| {
        bolt_server::ServiceError::new(
            "Neo.DatabaseError.General.UnknownError",
            "Bolt transaction revision overflow",
        )
    })?;
    validate_pending_statement_with_limit(writes, &candidate, maximum_items).map_err(|error| {
        bolt_server::ServiceError::new(
            "Neo.ClientError.Transaction.TransactionLimitReached",
            error.to_string(),
        )
    })?;
    let mut candidate_overlay = graph_overlay.clone();
    candidate_overlay
        .stage(
            candidate
                .iter()
                .flat_map(|write| write.graph_overlay_entries.iter().cloned()),
        )
        .map_err(|error| {
            bolt_server::ServiceError::new(
                "Neo.ClientError.Transaction.TransactionLimitReached",
                error.to_string(),
            )
        })?;

    *graph_overlay = candidate_overlay;
    writes.extend(candidate);
    *revision = next_revision;
    Ok(())
}

fn validate_pending_statement_with_limit(
    existing: &[PreparedCypherWrite],
    candidate: &[PreparedCypherWrite],
    maximum_items: usize,
) -> Result<(), RemoteGatewayServiceError> {
    let mut writes = existing.iter().chain(candidate);
    if let Some(first) = writes.next()
        && writes.any(|write| write.context != first.context)
    {
        return Err(RemoteGatewayServiceError::Transaction(
            "staged statements do not share one transaction context".into(),
        ));
    }
    let row_count = existing.len().checked_add(candidate.len()).ok_or_else(|| {
        RemoteGatewayServiceError::Transaction("staged row count overflow".into())
    })?;
    if row_count > maximum_items {
        return Err(RemoteGatewayServiceError::Transaction(format!(
            "transaction has {row_count} staged rows, exceeding its limit of {maximum_items}"
        )));
    }
    let mut grouped = BTreeMap::<(u64, u32), TransactionOverlay>::new();
    let mut constraint_owners = BTreeMap::new();
    for write in existing.iter().chain(candidate) {
        for scoped in &write.scoped {
            let scope = scoped.scope();
            grouped
                .entry((scope.graph().value(), scope.partition().value()))
                .or_insert_with(|| {
                    TransactionOverlay::new(maximum_items)
                        .expect("transaction mutation limit is non-zero")
                })
                .stage(scoped.transaction().clone())
                .map_err(|error| RemoteGatewayServiceError::Transaction(error.to_string()))?;
        }
        for constraint in &write.constraints {
            if let Some(owner) = constraint_owners.insert(constraint.key(), constraint.owner())
                && owner != constraint.owner()
            {
                return Err(RemoteGatewayServiceError::Transaction(
                    "one transaction assigns different owners to the same MERGE constraint".into(),
                ));
            }
        }
    }
    let operation_count = grouped.values().try_fold(0_usize, |count, overlay| {
        count.checked_add(overlay.operation_count()).ok_or_else(|| {
            RemoteGatewayServiceError::Transaction("staged operation count overflow".into())
        })
    })?;
    let item_count = operation_count
        .checked_add(constraint_owners.len())
        .ok_or_else(|| {
            RemoteGatewayServiceError::Transaction("staged item count overflow".into())
        })?;
    if item_count > maximum_items {
        return Err(RemoteGatewayServiceError::Transaction(format!(
            "transaction has {item_count} staged mutation/constraint items, exceeding its limit of {maximum_items}"
        )));
    }
    Ok(())
}

#[allow(clippy::type_complexity)]
fn pending_candidate_commit_inputs(
    existing: &[PreparedCypherWrite],
    candidate: &[PreparedCypherWrite],
) -> Result<
    (
        Option<TransactionContext>,
        Vec<dtgproxy::ScopedTemporalTransaction>,
        Vec<dtgproxy::RoutedConstraintClaim>,
    ),
    RemoteGatewayServiceError,
> {
    let context = existing
        .first()
        .or_else(|| candidate.first())
        .map(|write| write.context);
    let mut grouped = BTreeMap::<(u64, u32), (temporal_ir::GraphScope, TransactionOverlay)>::new();
    let mut constraints = Vec::new();
    for write in existing.iter().chain(candidate) {
        constraints.extend(write.constraints.iter().cloned());
        for scoped in &write.scoped {
            let scope = scoped.scope();
            match grouped.entry((scope.graph().value(), scope.partition().value())) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    let mut overlay =
                        TransactionOverlay::new(MAX_TRANSACTION_MUTATIONS).map_err(|error| {
                            RemoteGatewayServiceError::Transaction(error.to_string())
                        })?;
                    overlay
                        .stage(scoped.transaction().clone())
                        .map_err(|error| {
                            RemoteGatewayServiceError::Transaction(error.to_string())
                        })?;
                    entry.insert((scope, overlay));
                }
                std::collections::btree_map::Entry::Occupied(mut entry) => {
                    entry
                        .get_mut()
                        .1
                        .stage(scoped.transaction().clone())
                        .map_err(|error| {
                            RemoteGatewayServiceError::Transaction(error.to_string())
                        })?;
                }
            }
        }
    }
    let scoped = grouped
        .into_values()
        .map(|(scope, overlay)| {
            dtgproxy::ScopedTemporalTransaction::new(scope, overlay.into_transaction())
        })
        .collect::<Vec<_>>();
    Ok((context.filter(|_| !scoped.is_empty()), scoped, constraints))
}

fn compile_cypher(
    routing: &GatewayRoutingState,
    text: &str,
) -> Result<cypher_compiler::CompiledQuery, RemoteGatewayServiceError> {
    let session = CompileSession::new(
        routing.graph.name(),
        routing.graph.graph_id(),
        routing.graph.schema_version(),
        routing.graph.topology().epoch(),
    )
    .map_err(|error| RemoteGatewayServiceError::Query(error.to_string()))?;
    CypherCompiler::new()
        .compile(text, &session)
        .map_err(|error| RemoteGatewayServiceError::Query(error.to_string()))
}

fn materialized_element_json(
    element: &MaterializedElement,
) -> Result<Value, RemoteGatewayServiceError> {
    let endpoint_json = |endpoint: temporal_storage::ElementRef| {
        json!({
            "partition": endpoint.partition().value(),
            "element_id": endpoint.id().value().to_string(),
        })
    };
    Ok(json!({
        "kind": match element.kind() {
            MaterializedElementKind::Vertex => "vertex",
            MaterializedElementKind::Relationship => "relationship",
        },
        "partition": element.element().partition().value(),
        "element_id": element.element().id().value().to_string(),
        "type_id": element.type_id(),
        "payload_dtp1": hex_bytes(
            &element
                .payload()
                .encode()
                .map_err(|error| RemoteGatewayServiceError::Query(error.to_string()))?,
        ),
        "source": element.source().map(endpoint_json),
        "destination": element.destination().map(endpoint_json),
        "deleted": element.deleted(),
    }))
}

#[cfg(test)]
mod pending_statement_tests {
    use super::*;

    use temporal_storage::{
        ElementId, ElementRef, GraphId, LabelId, PartitionId, TemporalTransaction, VertexMutation,
    };
    use temporal_types::{CanonicalElement, Interval};

    #[test]
    fn call_subquery_write_stages_all_outer_rows_and_failed_child_leaves_overlay_unchanged() {
        let compiled = cypher_compiler::CypherCompiler::new()
            .compile(
                "UNWIND [1, 2] AS value CALL (value) { CREATE (n:Item {id: value}) }",
                &cypher_compiler::CompileSession::new("accounts", 7, 3, 11).unwrap(),
            )
            .expect("ordinary write subquery");
        let materialize = |row_index: u8, value: RuntimeValue| {
            materialize_write(
                &compiled,
                &WriteContext::new(
                    7,
                    3,
                    128,
                    [row_index; 32],
                    Interval::forever_from(ValidTime::from_micros(5)),
                    BTreeMap::new(),
                )
                .unwrap()
                .with_existing_bindings(BTreeMap::from([("value".to_owned(), value)]))
                .unwrap(),
            )
        };
        let first = materialize(1, RuntimeValue::Integer(1)).expect("first outer row");
        let second = materialize(2, RuntimeValue::Integer(2)).expect("second outer row");
        assert_eq!(first.scoped_transactions().len(), 1);
        assert_eq!(second.scoped_transactions().len(), 1);
        assert_ne!(
            first.overlay_elements()[0].element(),
            second.overlay_elements()[0].element()
        );

        let mut revision = 3;
        let mut overlay = GraphOverlay::new(8).expect("overlay");
        overlay
            .stage([overlay_entry(9)])
            .expect("pre-statement overlay");
        let original_overlay = overlay.clone();
        let mut writes = vec![prepared_write(9, vec![vertex_mutation(9)], Vec::new())];
        let original_fingerprints = writes
            .iter()
            .map(|write| write.fingerprint)
            .collect::<Vec<_>>();

        let error = materialize(
            3,
            RuntimeValue::Map(BTreeMap::from([(1, RuntimeValue::Integer(3))])),
        )
        .expect_err("later child row must fail before candidate publication");
        assert_eq!(error.code(), "DTG-CYPHER-INVALID-PROPERTY-VALUE");
        assert_eq!(revision, 3);
        assert_eq!(overlay, original_overlay);
        assert_eq!(
            writes
                .iter()
                .map(|write| write.fingerprint)
                .collect::<Vec<_>>(),
            original_fingerprints
        );

        let mut candidate_first = prepared_write(1, vec![vertex_mutation(1)], Vec::new());
        candidate_first.graph_overlay_entries.push(overlay_entry(1));
        let mut candidate_second = prepared_write(2, vec![vertex_mutation(2)], Vec::new());
        candidate_second
            .graph_overlay_entries
            .push(overlay_entry(2));
        publish_pending_statement(
            &mut revision,
            &mut overlay,
            &mut writes,
            3,
            vec![candidate_first, candidate_second],
            8,
        )
        .expect("the complete successful statement candidate publishes once");
        assert_eq!(revision, 4);
        assert_eq!(writes.len(), 3);
    }

    #[test]
    fn explicit_transaction_subquery_read_and_write_use_the_begin_snapshot() {
        let compiled = cypher_compiler::CypherCompiler::new()
            .compile(
                "UNWIND [1] AS value CALL (value) { CREATE (n:Item {id: value}) }",
                &cypher_compiler::CompileSession::new("accounts", 7, 3, 11).unwrap(),
            )
            .expect("ordinary write subquery");
        let begin = TransactionTime::new(10, 0);
        let commit = TransactionTime::new(20, 0);
        let context = TransactionContext::from_allocated(
            begin,
            commit,
            1,
            IsolationLevel::TemporalSnapshot,
            100,
        )
        .expect("BEGIN context");
        let mut revision = 0;
        let mut graph_overlay = GraphOverlay::new(8).expect("transaction overlay");
        let mut writes = Vec::new();
        graph_overlay
            .stage([overlay_entry(1)])
            .expect("existing staged overlay");
        let inherited_overlay = graph_overlay.clone();
        let mut child = prepared_write(2, vec![vertex_mutation(2)], Vec::new());
        child.context = context;
        child.graph_overlay_entries.push(overlay_entry(2));
        let materialized = materialize_write(
            &compiled,
            &WriteContext::new(
                7,
                3,
                128,
                [2; 32],
                Interval::forever_from(ValidTime::from_micros(5)),
                BTreeMap::new(),
            )
            .unwrap()
            .with_existing_bindings(BTreeMap::from([(
                "value".to_owned(),
                RuntimeValue::Integer(1),
            )]))
            .unwrap(),
        )
        .expect("write child materializes against the transaction row");

        let fixed_read_snapshot = begin;
        assert_eq!(
            fixed_read_snapshot, begin,
            "reads inherit the BEGIN snapshot"
        );
        assert_eq!(child.context.start_ts(), begin, "writes inherit BEGIN");
        assert_eq!(materialized.scoped_transactions().len(), 1);
        publish_pending_statement(
            &mut revision,
            &mut graph_overlay,
            &mut writes,
            0,
            vec![child],
            8,
        )
        .expect("write subquery remains staged");
        assert_eq!(writes.len(), 1);
        assert_ne!(graph_overlay, inherited_overlay);

        drop((writes, graph_overlay));
        let rolled_back_overlay = GraphOverlay::new(8).expect("new transaction state");
        assert!(
            rolled_back_overlay.is_empty(),
            "ROLLBACK discards staged writes"
        );
    }

    #[test]
    fn oversized_multi_operation_candidate_leaves_prior_pending_writes_unchanged() {
        let existing = vec![prepared_write(1, vec![vertex_mutation(1)], Vec::new())];
        let candidate = vec![prepared_write(
            2,
            vec![vertex_mutation(2), vertex_mutation(3)],
            Vec::new(),
        )];

        assert!(validate_pending_statement_with_limit(&existing, &candidate, 2).is_err());
        assert_eq!(existing.len(), 1);
        assert_eq!(existing[0].scoped[0].transaction().operation_count(), 1);
        assert!(validate_pending_statement_with_limit(&existing, &[], 2).is_ok());
    }

    #[test]
    fn conflicting_candidate_constraints_are_rejected_before_publish() {
        let key = [9; 32];
        let first =
            dtgproxy::RoutedConstraintClaim::new(7, key, vertex_ref(1)).expect("first constraint");
        let second =
            dtgproxy::RoutedConstraintClaim::new(7, key, vertex_ref(2)).expect("second constraint");
        let candidate = vec![prepared_write(1, Vec::new(), vec![first, second])];

        assert!(validate_pending_statement_with_limit(&[], &candidate, 8).is_err());
    }

    #[test]
    fn later_candidate_failure_keeps_revision_overlay_and_writes_unchanged() {
        let mut revision = 7;
        let mut overlay = GraphOverlay::new(8).expect("overlay");
        overlay.stage([overlay_entry(1)]).expect("prior overlay");
        let original_overlay = overlay.clone();
        let mut writes = vec![prepared_write(1, vec![vertex_mutation(1)], Vec::new())];
        let original_fingerprints = writes
            .iter()
            .map(|write| write.fingerprint)
            .collect::<Vec<_>>();

        let key = [9; 32];
        let first_constraint =
            dtgproxy::RoutedConstraintClaim::new(7, key, vertex_ref(2)).expect("first constraint");
        let conflicting_constraint =
            dtgproxy::RoutedConstraintClaim::new(7, key, vertex_ref(3)).expect("conflict");
        let mut first = prepared_write(2, vec![vertex_mutation(2)], vec![first_constraint]);
        first.graph_overlay_entries.push(overlay_entry(2));
        let mut later = prepared_write(3, vec![vertex_mutation(3)], vec![conflicting_constraint]);
        later.graph_overlay_entries.push(overlay_entry(3));

        let error = publish_pending_statement(
            &mut revision,
            &mut overlay,
            &mut writes,
            7,
            vec![first, later],
            8,
        )
        .expect_err("a later conflicting row must reject the complete candidate");

        assert_eq!(
            error.code(),
            "Neo.ClientError.Transaction.TransactionLimitReached"
        );
        assert_eq!(revision, 7);
        assert_eq!(overlay, original_overlay);
        assert_eq!(writes.len(), original_fingerprints.len());
        assert_eq!(
            writes
                .iter()
                .map(|write| write.fingerprint)
                .collect::<Vec<_>>(),
            original_fingerprints
        );
    }

    #[test]
    fn statement_writes_keep_one_transaction_identity_and_timestamp_pair() {
        let first = prepared_write(1, vec![vertex_mutation(1)], Vec::new());
        let second = prepared_write(2, vec![vertex_mutation(2)], Vec::new());

        assert_eq!(
            first.context.transaction_id(),
            second.context.transaction_id()
        );
        assert_eq!(first.context.start_ts(), second.context.start_ts());
        assert_eq!(first.context.commit_ts(), second.context.commit_ts());
        assert!(validate_pending_statement_with_limit(&[first], &[second], 8).is_ok());
    }

    #[test]
    fn mixed_transaction_context_is_rejected_before_candidate_publish() {
        let existing = prepared_write(1, vec![vertex_mutation(1)], Vec::new());
        let mut candidate = prepared_write(2, vec![vertex_mutation(2)], Vec::new());
        candidate.context = TransactionContext::from_allocated(
            TransactionTime::new(11, 0),
            TransactionTime::new(21, 0),
            1,
            IsolationLevel::TemporalSnapshot,
            100,
        )
        .expect("different context");

        assert!(validate_pending_statement_with_limit(&[existing], &[candidate], 8).is_err());
    }

    fn prepared_write(
        id: u128,
        mutations: Vec<VertexMutation>,
        constraints: Vec<dtgproxy::RoutedConstraintClaim>,
    ) -> PreparedCypherWrite {
        let context = TransactionContext::from_allocated(
            TransactionTime::new(10, 0),
            TransactionTime::new(20, 0),
            1,
            IsolationLevel::TemporalSnapshot,
            100,
        )
        .expect("context");
        let transaction = mutations
            .into_iter()
            .fold(TemporalTransaction::new(), TemporalTransaction::with_vertex);
        let scoped = if transaction.operation_count() == 0 {
            Vec::new()
        } else {
            vec![dtgproxy::ScopedTemporalTransaction::new(
                temporal_ir::GraphScope::new(GraphId::new(7), PartitionId::new(0)),
                transaction,
            )]
        };
        PreparedCypherWrite {
            context,
            scoped,
            bindings: Map::new(),
            overlay_values: Vec::new(),
            graph_overlay_entries: Vec::new(),
            fingerprint: [u8::try_from(id).unwrap_or(1).max(1); 32],
            constraints,
        }
    }

    fn vertex_mutation(id: u128) -> VertexMutation {
        VertexMutation::put(
            vertex_ref(id),
            LabelId::new(1),
            Interval::forever_from(ValidTime::from_micros(1)),
            CanonicalElement::new(1, BTreeMap::new()),
        )
        .expect("vertex mutation")
    }

    fn vertex_ref(id: u128) -> ElementRef {
        ElementRef::vertex(GraphId::new(7), PartitionId::new(0), ElementId::new(id))
    }

    fn overlay_entry(id: u128) -> GraphOverlayEntry {
        GraphOverlayEntry::put(
            0,
            Interval::forever_from(ValidTime::from_micros(1)),
            RuntimeValue::Node(VertexRecord::new(
                vertex_ref(id),
                Some(LabelId::new(1)),
                CanonicalElement::new(1, BTreeMap::new()),
            )),
        )
        .expect("overlay entry")
    }
}

#[tonic::async_trait]
impl GatewayService for RemoteGatewayService {
    async fn submit(
        &self,
        request: Request<GatewaySubmitRequest>,
    ) -> Result<Response<GatewaySubmitResponse>, Status> {
        let request = request.into_inner();
        let context: CommonRequestContext = request
            .context
            .ok_or_else(|| Status::invalid_argument("missing request context"))?
            .try_into()
            .map_err(|error: cluster_protocol::ProtocolError| {
                Status::invalid_argument(error.to_string())
            })?;
        if context.cluster_id() != &self.cluster_id {
            return Err(Status::permission_denied("cluster identity mismatch"));
        }
        context
            .ensure_active_at(unix_time_ms()?)
            .map_err(|error| Status::deadline_exceeded(error.to_string()))?;
        if request.request_json.is_empty() || request.request_json.len() > MAX_COMMAND_BYTES {
            return Err(Status::invalid_argument("invalid Gateway JSON frame size"));
        }
        let _permit = self.admission.try_enter().map_err(|error| match error {
            AdmissionError::Exhausted => Status::resource_exhausted(error.to_string()),
            AdmissionError::Closed => Status::unavailable(error.to_string()),
            AdmissionError::InvalidMaximum => Status::internal(error.to_string()),
        })?;
        let parsed = serde_json::from_slice::<GatewayRequest>(&request.request_json);
        let (request_name, result) = match parsed {
            Ok(request) => {
                let request_name = request.request_id.clone();
                let result = self
                    .execute(
                        u128::from_be_bytes(*context.request_id()),
                        context.deadline_unix_ms(),
                        request,
                    )
                    .await;
                (request_name, result)
            }
            Err(error) => (
                "unknown".into(),
                Err(RemoteGatewayServiceError::Request(format!(
                    "invalid request JSON: {error}"
                ))),
            ),
        };
        let response = match result {
            Ok(result) => json!({
                "version": GATEWAY_API_VERSION,
                "request_id": request_name,
                "ok": true,
                "result": result
            }),
            Err(error) => json!({
                "version": GATEWAY_API_VERSION,
                "request_id": request_name,
                "ok": false,
                "error": error.to_string()
            }),
        };
        let response_json =
            serde_json::to_vec(&response).map_err(|error| Status::internal(error.to_string()))?;
        Ok(Response::new(GatewaySubmitResponse { response_json }))
    }
}

fn request_namespace(request_id: u128, shard_id: u32) -> u64 {
    let low = u64::try_from(request_id & u128::from(u64::MAX))
        .expect("masked request namespace fits u64");
    (low ^ u64::from(shard_id)).max(1)
}

fn analytics_reader_error(message: impl Into<String>) -> ClusterAnalyticsError {
    ClusterAnalyticsError::new("DTG-ANALYTICS-RESULT-STORE", message)
}

fn result_read_request_id(
    invocation_request_id: u128,
    job_id: u128,
    generation: u64,
    shard_id: u32,
) -> u128 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"DTGProxy/Analytics/ResultRead/V1");
    hasher.update(&invocation_request_id.to_be_bytes());
    hasher.update(&job_id.to_be_bytes());
    hasher.update(&generation.to_be_bytes());
    hasher.update(&shard_id.to_be_bytes());
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    u128::from_be_bytes(bytes).max(1)
}

fn request_security_fingerprint(cluster_id: [u8; 16], request_id: u128) -> [u8; 32] {
    let _ = request_id;
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"DTGProxy/GatewayPrincipal/V1");
    hasher.update(&cluster_id);
    *hasher.finalize().as_bytes()
}

fn write_request_seed(cluster_id: [u8; 16], request_id: u128) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"DTGProxy/CypherWriteRequest/Latest");
    hasher.update(&cluster_id);
    hasher.update(&request_id.to_be_bytes());
    *hasher.finalize().as_bytes()
}

fn batch_request_id(request_id: u128, batch_index: usize) -> u128 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"DTGProxy/CypherBatchRequest/Latest");
    hasher.update(&request_id.to_be_bytes());
    hasher.update(
        &u64::try_from(batch_index)
            .expect("batch index fits u64")
            .to_be_bytes(),
    );
    u128::from_be_bytes(
        hasher.finalize().as_bytes()[..16]
            .try_into()
            .expect("digest has sixteen bytes"),
    )
    .max(1)
}

fn batch_ledger_key(
    cluster_id: [u8; 16],
    graph_id: u64,
    request_id: u128,
    query_fingerprint: [u8; 32],
    batch_index: usize,
) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"DTGProxy/CypherBatchLedger/Latest");
    hasher.update(&cluster_id);
    hasher.update(&graph_id.to_be_bytes());
    hasher.update(&request_id.to_be_bytes());
    hasher.update(&query_fingerprint);
    hasher.update(
        &u64::try_from(batch_index)
            .expect("batch index fits u64")
            .to_be_bytes(),
    );
    *hasher.finalize().as_bytes()
}

fn batch_ledger_claim(
    routing: &GatewayRoutingState,
    ledger_key: [u8; 32],
    start_ts: TransactionTime,
) -> Result<dtgproxy::RoutedConstraintClaim, RemoteGatewayServiceError> {
    let partition = u32::from_be_bytes(
        ledger_key[..4]
            .try_into()
            .expect("ledger digest has four partition bytes"),
    ) % routing.deployment.virtual_partitions();
    let owner = temporal_storage::ElementRef::vertex(
        temporal_storage::GraphId::new(routing.graph.graph_id()),
        temporal_storage::PartitionId::new(partition),
        temporal_storage::ElementId::new(transaction_id_value_from_time(start_ts)),
    );
    dtgproxy::RoutedConstraintClaim::new(routing.graph.graph_id(), ledger_key, owner)
        .map_err(|error| RemoteGatewayServiceError::Transaction(error.to_string()))
}

fn transaction_id_value_from_time(timestamp: TransactionTime) -> u128 {
    let physical_offset = u128::from(
        u64::try_from(i128::from(timestamp.physical_micros()) - i128::from(i64::MIN))
            .expect("i64 timestamp offset fits u64"),
    );
    ((physical_offset << 32) | u128::from(timestamp.logical())) + 1
}

fn transaction_time_from_id(
    transaction_id: u128,
) -> Result<TransactionTime, RemoteGatewayServiceError> {
    let ordinal = transaction_id.checked_sub(1).ok_or_else(|| {
        RemoteGatewayServiceError::Transaction(
            "batch ledger transaction identity cannot be zero".into(),
        )
    })?;
    let physical_offset = ordinal >> 32;
    let physical_offset = u64::try_from(physical_offset).map_err(|_| {
        RemoteGatewayServiceError::Transaction(
            "batch ledger transaction physical component overflowed".into(),
        )
    })?;
    let physical = i128::from(physical_offset) + i128::from(i64::MIN);
    let physical = i64::try_from(physical).map_err(|_| {
        RemoteGatewayServiceError::Transaction(
            "batch ledger transaction physical component is invalid".into(),
        )
    })?;
    Ok(TransactionTime::new(
        physical,
        u32::try_from(ordinal & u128::from(u32::MAX))
            .expect("masked transaction logical component fits u32"),
    ))
}

fn replayed_batch_summary(
    routing: &GatewayRoutingState,
    prepared: &[PreparedCypherWrite],
) -> Result<Value, RemoteGatewayServiceError> {
    let first = prepared.first().ok_or_else(|| {
        RemoteGatewayServiceError::Transaction("empty replayed batch candidate".into())
    })?;
    let context = first.context;
    if prepared.iter().any(|write| write.context != context) {
        return Err(RemoteGatewayServiceError::Transaction(
            "replayed batch rows do not share one transaction context".into(),
        ));
    }
    let mut participants = BTreeSet::new();
    for write in prepared {
        for scoped in &write.scoped {
            participants.insert(routing.deployment.route_scope(scoped.scope()).shard_id());
        }
        for constraint in &write.constraints {
            participants.insert(
                constraint
                    .owner_read_keys(&routing.deployment)
                    .map_err(|error| RemoteGatewayServiceError::Transaction(error.to_string()))?
                    .0
                    .shard_id(),
            );
        }
    }
    let participants = participants.into_iter().collect::<Vec<_>>();
    let bindings = unambiguous_response_bindings(prepared);
    let rows = prepared
        .iter()
        .flat_map(|write| write.overlay_values.iter())
        .map(|value| runtime_value_json(value).unwrap_or(Value::Null))
        .collect::<Vec<_>>();
    Ok(json!({
        "kind": "cypher_write",
        "query_fingerprint": hex_bytes(&first.fingerprint),
        "transaction_id": context.transaction_id().value().to_string(),
        "start_ts": timestamp_json(context.start_ts()),
        "commit_ts": timestamp_json(context.commit_ts()),
        "home_shard": participants.first().copied().unwrap_or(0),
        "participants": participants,
        "single_shard_fast_path": participants.len() == 1,
        "bindings": bindings,
        "rows": rows,
    }))
}

fn batch_failure_error(
    failing_batch: usize,
    row_start: usize,
    row_count: usize,
    committed: &[Value],
    error: RemoteGatewayServiceError,
) -> RemoteGatewayServiceError {
    let committed_row_count = committed.iter().fold(0_u64, |total, summary| {
        total.saturating_add(
            summary
                .get("row_count")
                .and_then(Value::as_u64)
                .unwrap_or(0),
        )
    });
    let report = json!({
        "code": "DTG-CYPHER-IN-TRANSACTIONS-BATCH-FAILED",
        "failing_batch": failing_batch,
        "failing_row_start": row_start,
        "failing_row_count": row_count,
        "committed_batch_count": committed.len(),
        "committed_row_count": committed_row_count,
        "batches": committed,
        "error": error.to_string(),
        "statement_rolled_back": false,
    });
    RemoteGatewayServiceError::Transaction(format!(
        "DTG-CYPHER-IN-TRANSACTIONS-BATCH-FAILED: {report}"
    ))
}

fn write_row_seed(base: [u8; 32], row_index: usize, multiple_rows: bool) -> [u8; 32] {
    if !multiple_rows {
        return base;
    }
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"DTGProxy/CypherWriteRow/Latest");
    hasher.update(&base);
    hasher.update(
        &u64::try_from(row_index)
            .expect("write row index fits u64")
            .to_be_bytes(),
    );
    *hasher.finalize().as_bytes()
}

fn merge_row_request_id(request_id: u128, row_index: usize) -> u128 {
    (request_id
        ^ 0x4454_475f_4d45_5247_455f_524f_575f_4944_u128
        ^ u128::from(u64::try_from(row_index).expect("write row index fits u64")))
    .max(1)
}

fn cypher_response_json(
    response: &cypher_engine::CypherQueryResponse,
) -> Result<Value, RemoteGatewayServiceError> {
    let columns = response
        .schema()
        .columns()
        .iter()
        .map(|column| {
            json!({
                "name": column.name(),
                "type": format!("{:?}", column.value_type()),
                "nullable": column.nullable(),
            })
        })
        .collect::<Vec<_>>();
    let rows = response
        .batches()
        .iter()
        .flat_map(|batch| batch.rows())
        .map(|row| {
            row.iter()
                .map(runtime_value_json)
                .collect::<Result<Vec<_>, _>>()
        })
        .collect::<Result<Vec<_>, _>>()?;
    let temporal_regions = response.temporal_rows().map(|rows| {
        rows.iter()
            .map(|row| {
                let region = row.region();
                json!({
                    "valid": {
                        "from_micros": region.valid().start().as_micros(),
                        "to_micros": region.valid().end().map(ValidTime::as_micros),
                    },
                    "transaction": {
                        "from": timestamp_json(region.transaction().start()),
                        "to": region.transaction().end().map(timestamp_json),
                    },
                })
            })
            .collect::<Vec<_>>()
    });
    Ok(json!({
        "version": 2,
        "kind": "cypher_result",
        "query_fingerprint": hex_bytes(&response.fingerprint()),
        "columns": columns,
        "row_count": response.row_count(),
        "rows": rows,
        "temporal_regions": temporal_regions,
        "optimizer_trace": response.optimizer_trace(),
    }))
}

fn runtime_value_json(value: &RuntimeValue) -> Result<Value, RemoteGatewayServiceError> {
    match value {
        RuntimeValue::Null => Ok(Value::Null),
        RuntimeValue::Boolean(value) => Ok(Value::Bool(*value)),
        RuntimeValue::Integer(value) => Ok(json!(value)),
        RuntimeValue::FloatBits(bits) => Ok(json!({
            "type": "float",
            "bits": format!("{bits:016x}"),
        })),
        RuntimeValue::String(value) => Ok(Value::String(value.clone())),
        RuntimeValue::Bytes(value) => Ok(json!({"type": "bytes", "hex": hex_bytes(value)})),
        RuntimeValue::TimestampMicros(value) => {
            Ok(json!({"type": "timestamp", "micros": value.to_string()}))
        }
        RuntimeValue::List(values) => values
            .iter()
            .map(runtime_value_json)
            .collect::<Result<Vec<_>, _>>()
            .map(Value::Array),
        RuntimeValue::Map(values) => {
            let values = values
                .iter()
                .map(|(key, value)| Ok((key.to_string(), runtime_value_json(value)?)))
                .collect::<Result<Map<_, _>, RemoteGatewayServiceError>>()?;
            Ok(Value::Object(values))
        }
        RuntimeValue::Node(node) => {
            let element = node.element();
            Ok(json!({
                "type": "node",
                "graph_id": element.graph().value(),
                "partition_id": element.partition().value(),
                "element_id": element.id().value().to_string(),
                "label_id": node.label().map(|label| label.value()),
                "payload_dtp1": hex_bytes(&node.payload().encode().map_err(|error| {
                    RemoteGatewayServiceError::Query(error.to_string())
                })?),
            }))
        }
        RuntimeValue::Relationship(relationship) => {
            let element = relationship.element();
            Ok(json!({
                "type": "relationship",
                "graph_id": element.graph().value(),
                "partition_id": element.partition().value(),
                "element_id": element.id().value().to_string(),
                "relationship_type_id": relationship.edge_type().value(),
                "source_id": relationship.source().value().to_string(),
                "destination_id": relationship.destination().value().to_string(),
                "payload_dtp1": hex_bytes(&relationship.payload().encode().map_err(|error| {
                    RemoteGatewayServiceError::Query(error.to_string())
                })?),
            }))
        }
    }
}

fn hex_bytes(bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    let mut encoded = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        write!(encoded, "{byte:02x}").expect("writing to String cannot fail");
    }
    encoded
}

fn timestamp_json(timestamp: TransactionTime) -> Value {
    json!({
        "physical_micros": timestamp.physical_micros(),
        "logical": timestamp.logical(),
    })
}

fn bookmark_from_commit_summary(summary: &Value) -> Result<String, RemoteGatewayServiceError> {
    let commit = summary
        .get("commit_ts")
        .and_then(Value::as_object)
        .ok_or_else(|| {
            RemoteGatewayServiceError::Transaction("commit receipt has no commit_ts".into())
        })?;
    let physical = commit
        .get("physical_micros")
        .and_then(Value::as_i64)
        .ok_or_else(|| {
            RemoteGatewayServiceError::Transaction(
                "commit receipt has an invalid physical timestamp".into(),
            )
        })?;
    let logical = commit
        .get("logical")
        .and_then(Value::as_u64)
        .and_then(|value| u32::try_from(value).ok())
        .ok_or_else(|| {
            RemoteGatewayServiceError::Transaction(
                "commit receipt has an invalid logical timestamp".into(),
            )
        })?;
    Ok(format!("dtg:tx:{physical}:{logical}"))
}

fn gateway_request_nonce(cluster_id: [u8; 16], gateway_id: u64) -> u64 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"DTGProxy/Gateway/RequestNonce/V1");
    hasher.update(&cluster_id);
    hasher.update(&gateway_id.to_be_bytes());
    hasher.update(&std::process::id().to_be_bytes());
    hasher.update(
        &SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
            .to_be_bytes(),
    );
    hasher.update(
        &NEXT_GATEWAY_REQUEST_NONCE
            .fetch_add(1, Ordering::Relaxed)
            .to_be_bytes(),
    );
    let mut bytes = [0; 8];
    bytes.copy_from_slice(&hasher.finalize().as_bytes()[..8]);
    u64::from_be_bytes(bytes).max(1)
}

fn unix_time_ms() -> Result<u64, Status> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| Status::internal("system clock is before Unix epoch"))?
        .as_millis();
    u64::try_from(millis).map_err(|_| Status::internal("system clock overflow"))
}

fn unix_time_micros() -> Result<i64, RemoteGatewayServiceError> {
    let micros = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| RemoteGatewayServiceError::Query("system clock is before Unix epoch".into()))?
        .as_micros();
    i64::try_from(micros)
        .map_err(|_| RemoteGatewayServiceError::Query("system clock overflow".into()))
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RemoteGatewayServiceError {
    InvalidConfiguration,
    Topology(String),
    Request(String),
    Meta(String),
    Transaction(String),
    RetryableMergeContention(String),
    Query(String),
}

impl Display for RemoteGatewayServiceError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfiguration => formatter.write_str("invalid Gateway configuration"),
            Self::Topology(message) => write!(formatter, "Gateway topology error: {message}"),
            Self::Request(message) => write!(formatter, "Gateway request error: {message}"),
            Self::Meta(message) => write!(formatter, "Gateway Meta error: {message}"),
            Self::Transaction(message) => write!(formatter, "Gateway transaction error: {message}"),
            Self::RetryableMergeContention(message) => {
                write!(formatter, "Gateway transaction error: {message}")
            }
            Self::Query(message) => write!(formatter, "Gateway query error: {message}"),
        }
    }
}

impl Error for RemoteGatewayServiceError {}

#[cfg(test)]
mod tests {
    use analytics_api::{
        PartitionedSnapshotGraph, ProjectedGraph, SnapshotEdge, SnapshotPartition, VertexId,
    };
    use query_executor::RuntimeValue;
    use serde_json::json;

    use super::{
        apply_snapshot_overlay, bookmark_from_commit_summary, partitioned_snapshot_projection,
        runtime_value_json,
    };

    #[test]
    fn snapshot_overlay_cardinality_uses_the_final_projection_not_element_order() {
        use query_executor::VertexRecord;
        use temporal_storage::{ElementId, ElementRef, GraphId, LabelId, PartitionId};
        use temporal_types::CanonicalElement;

        let existing =
            ElementRef::vertex(GraphId::new(7), PartitionId::new(0), ElementId::new(100));
        let created = ElementRef::vertex(GraphId::new(7), PartitionId::new(0), ElementId::new(1));
        assert!(
            created < existing,
            "creation must be visited before deletion"
        );
        let mut vertices =
            std::collections::BTreeMap::from([(existing, VertexId::new(existing.id().value()))]);
        let mut edges = std::collections::BTreeMap::new();
        let overlay = std::collections::BTreeMap::from([
            (
                created,
                Some(RuntimeValue::Node(VertexRecord::new(
                    created,
                    Some(LabelId::new(1)),
                    CanonicalElement::new(1, std::collections::BTreeMap::new()),
                ))),
            ),
            (existing, None),
        ]);
        let mut remaining_bytes = 1 << 20;

        apply_snapshot_overlay(
            &mut vertices,
            &mut edges,
            overlay,
            1,
            1,
            &mut remaining_bytes,
        )
        .expect("the final one-vertex projection fits the catalog limit");

        assert_eq!(
            vertices.values().copied().collect::<Vec<_>>(),
            vec![VertexId::new(1)]
        );
    }

    #[test]
    fn snapshot_parts_without_an_overlay_keep_the_partitioned_projection() {
        let projected = partitioned_snapshot_projection(vec![
            SnapshotPartition::new(
                7,
                vec![VertexId::new(3)],
                vec![SnapshotEdge::new(VertexId::new(3), VertexId::new(1), 1.0).expect("edge")],
            ),
            SnapshotPartition::new(2, vec![VertexId::new(1)], Vec::new()),
        ])
        .expect("partitioned gateway projection");

        let ProjectedGraph::PartitionedSnapshot(graph) = projected.as_ref() else {
            panic!("snapshot projection was gathered before provider execution");
        };
        assert_eq!(
            graph,
            &PartitionedSnapshotGraph::new(
                vec![
                    SnapshotPartition::new(
                        7,
                        vec![VertexId::new(3)],
                        vec![
                            SnapshotEdge::new(VertexId::new(3), VertexId::new(1), 1.0)
                                .expect("edge")
                        ],
                    ),
                    SnapshotPartition::new(2, vec![VertexId::new(1)], Vec::new()),
                ],
                true,
            )
            .expect("expected graph")
        );
    }

    #[test]
    fn cypher_json_keeps_temporal_and_binary_values_typed() {
        assert_eq!(
            runtime_value_json(&RuntimeValue::TimestampMicros(123)).expect("timestamp"),
            json!({"type": "timestamp", "micros": "123"})
        );
        assert_eq!(
            runtime_value_json(&RuntimeValue::Bytes(vec![0, 15, 255])).expect("bytes"),
            json!({"type": "bytes", "hex": "000fff"})
        );
    }

    #[test]
    fn commit_bookmarks_encode_the_two_component_transaction_timestamp() {
        assert_eq!(
            bookmark_from_commit_summary(&json!({
                "commit_ts": {"physical_micros": 42, "logical": 7}
            }))
            .expect("bookmark"),
            "dtg:tx:42:7"
        );
        assert!(bookmark_from_commit_summary(&json!({"commit_ts": {"logical": 7}})).is_err());
    }
}
