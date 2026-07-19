use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use analytics_api::{
    AlgorithmRequest, AlgorithmValue, AnalyticsProvider, ProjectedGraph, SnapshotGraph, VertexId,
};
use analytics_runtime::{BuiltInProvider, project_snapshot_parts};
use cluster_protocol::proto::gateway_service_server::GatewayService;
use cluster_protocol::proto::meta_service_client::MetaServiceClient;
use cluster_protocol::proto::{
    AllocateTimestampRequest, GatewaySubmitRequest, GatewaySubmitResponse, RequestContext,
};
use cluster_protocol::{CLUSTER_PROTOCOL_VERSION, CommonRequestContext, MAX_COMMAND_BYTES};
use control_plane::GraphDefinition;
use cypher_compiler::{CompileSession, CypherCompiler};
use cypher_engine::{
    BackendFuture, BackendQueryResult, BoltQueryBackend, BoltQueryRequest, CypherQueryEngine,
    CypherQueryRequest, CypherQueryResponse, DeploymentMode as QueryDeploymentMode, EngineConfig,
    MaterializedElement, MaterializedElementKind, ResourceLimits, WriteContext, materialize_write,
    resolve_compiled_temporal_scope,
};
use distributed_query::{DistributedCoordinator, LocalFragmentWorker};
use dtgproxy::gateway::{GATEWAY_API_VERSION, GatewayOperation, GatewayRequest};
use dtgproxy::{DeploymentConfig, DeploymentMode, TransactionContext, TransactionCoordinator};
use query_executor::v2::{RuntimeValue, TemporalBatchExecutor};
use query_executor::{LocalExecutor, ShardQueryBatch, SnapshotToken, merge_distributed_results};
use serde_json::{Map, Value, json};
use shard_client::{RemoteShardClient, ShardClient, ShardClientStorageAdapter};
use temporal_ir::PlanBody;
use temporal_storage::TemporalStore;
use temporal_types::{Interval, TransactionTime, ValidTime};
use timestamp_oracle::advance_timestamp;
use tonic::{Request, Response, Status};
use txn_protocol::IsolationLevel;

use crate::{AdmissionController, AdmissionError};

#[derive(Clone)]
pub struct RemoteGatewayService {
    cluster_id: [u8; 16],
    routing: Arc<RwLock<GatewayRoutingState>>,
    shard_client: Arc<RemoteShardClient>,
    meta_endpoints: Arc<Vec<SocketAddr>>,
    admission: Arc<AdmissionController>,
    max_raft_ticks: usize,
    bolt_request_sequence: Arc<AtomicU64>,
}

#[derive(Clone)]
struct GatewayRoutingState {
    revision: u64,
    graph: GraphDefinition,
    deployment: Arc<DeploymentConfig>,
}

impl RemoteGatewayService {
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
        Ok(Self {
            cluster_id,
            routing: Arc::new(RwLock::new(GatewayRoutingState {
                revision: catalog_revision,
                graph,
                deployment: Arc::new(deployment),
            })),
            shard_client,
            meta_endpoints: Arc::new(meta_endpoints),
            admission: Arc::new(
                AdmissionController::new(maximum_inflight)
                    .map_err(|_| RemoteGatewayServiceError::InvalidConfiguration)?,
            ),
            max_raft_ticks,
            bolt_request_sequence: Arc::new(AtomicU64::new(1)),
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
            GatewayOperation::Query { text } => {
                self.execute_query(request_id, deadline_unix_ms, &routing, &text)
                    .await
            }
            GatewayOperation::MigrateBackend { .. } => Err(RemoteGatewayServiceError::Request(
                "backend migration is owned by the Controller service".into(),
            )),
        }
    }

    async fn execute_query(
        &self,
        request_id: u128,
        deadline_unix_ms: u64,
        routing: &GatewayRoutingState,
        text: &str,
    ) -> Result<Value, RemoteGatewayServiceError> {
        if is_legacy_query(text) {
            return self
                .execute_legacy_query(request_id, deadline_unix_ms, routing, text)
                .await;
        }
        self.execute_cypher_query(request_id, deadline_unix_ms, routing, text)
            .await
    }

    async fn execute_cypher_query(
        &self,
        request_id: u128,
        deadline_unix_ms: u64,
        routing: &GatewayRoutingState,
        text: &str,
    ) -> Result<Value, RemoteGatewayServiceError> {
        let compiled = compile_cypher(routing, text)?;
        if compiled.is_procedure() {
            return self
                .execute_analytics_call(
                    request_id,
                    deadline_unix_ms,
                    routing,
                    text,
                    BTreeMap::new(),
                )
                .await;
        }
        if !compiled.is_read_only() {
            return self
                .execute_cypher_write(
                    request_id,
                    deadline_unix_ms,
                    routing,
                    &compiled,
                    BTreeMap::new(),
                )
                .await;
        }
        let response = self
            .execute_cypher_response(request_id, deadline_unix_ms, routing, text, BTreeMap::new())
            .await?;
        cypher_response_json(&response)
    }

    async fn execute_cypher_write(
        &self,
        request_id: u128,
        deadline_unix_ms: u64,
        routing: &GatewayRoutingState,
        compiled: &cypher_compiler::CompiledQuery,
        parameters: BTreeMap<String, RuntimeValue>,
    ) -> Result<Value, RemoteGatewayServiceError> {
        let (start_ts, commit_ts) = self
            .allocate_transaction_timestamps(request_id, deadline_unix_ms)
            .await?;
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
            query_executor::v2::ResolvedValidTime::Point(value) => Interval::forever_from(value),
            query_executor::v2::ResolvedValidTime::Interval { start, end } => {
                Interval::new(start, Some(end))
                    .map_err(|error| RemoteGatewayServiceError::Query(error.to_string()))?
            }
        };
        let materialized = materialize_write(
            compiled,
            &WriteContext::new(
                routing.graph.graph_id(),
                routing.graph.schema_version(),
                routing.deployment.virtual_partitions(),
                request_security_fingerprint(self.cluster_id, request_id),
                valid,
                parameters,
            )
            .map_err(|error| RemoteGatewayServiceError::Query(error.to_string()))?,
        )
        .map_err(|error| RemoteGatewayServiceError::Query(error.to_string()))?;
        let bindings = materialized
            .bindings()
            .iter()
            .map(|(name, element)| Ok((name.clone(), materialized_element_json(element)?)))
            .collect::<Result<Map<_, _>, RemoteGatewayServiceError>>()?;
        let scoped = materialized
            .into_scoped_transactions()
            .into_iter()
            .map(|write| {
                let (scope, transaction) = write.into_parts();
                dtgproxy::ScopedTemporalTransaction::new(scope, transaction)
            })
            .collect();
        let context = TransactionContext::from_allocated(
            start_ts,
            commit_ts,
            routing.graph.schema_version(),
            IsolationLevel::TemporalSnapshot,
            60_000_000,
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
            "kind": "cypher_write",
            "query_fingerprint": hex_bytes(&compiled.fingerprint()),
            "transaction_id": receipt.transaction_id().value().to_string(),
            "start_ts": timestamp_json(receipt.start_ts()),
            "commit_ts": timestamp_json(receipt.commit_ts()),
            "home_shard": receipt.home().shard_id(),
            "participants": receipt.participants().iter().map(|participant| participant.shard_id()).collect::<Vec<_>>(),
            "single_shard_fast_path": receipt.single_shard_fast_path(),
            "bindings": bindings,
        }))
    }

    async fn execute_analytics_call(
        &self,
        request_id: u128,
        deadline_unix_ms: u64,
        routing: &GatewayRoutingState,
        text: &str,
        parameters: BTreeMap<String, RuntimeValue>,
    ) -> Result<Value, RemoteGatewayServiceError> {
        let compiled = compile_cypher(routing, text)?;
        let (start_ts, _) = self
            .allocate_transaction_timestamps(request_id, deadline_unix_ms)
            .await?;
        let resolved = resolve_compiled_temporal_scope(
            &compiled,
            routing.graph.graph_id(),
            ValidTime::from_micros(unix_time_micros()?),
            start_ts,
            parameters.clone(),
        )
        .map_err(|error| RemoteGatewayServiceError::Query(error.to_string()))?;
        let valid_time = match resolved.valid_time() {
            query_executor::v2::ResolvedValidTime::Point(value) => value,
            query_executor::v2::ResolvedValidTime::Interval { .. } => {
                return Err(RemoteGatewayServiceError::Query(
                    "analytics CALL currently requires a point valid-time scope".into(),
                ));
            }
        };
        let client: Arc<dyn ShardClient> = self.shard_client.clone();
        let mut vertices = Vec::new();
        let mut edges = Vec::new();
        for placement in routing.deployment.all_shards() {
            let adapter = ShardClientStorageAdapter::new(
                Arc::clone(&client),
                routing.graph.graph_id(),
                placement.shard_id(),
                placement.placement_epoch(),
                deadline_unix_ms,
                request_namespace(request_id, placement.shard_id()),
            )
            .map_err(|error| RemoteGatewayServiceError::Query(error.to_string()))?;
            let (fragment_vertices, fragment_edges) = project_snapshot_parts(
                &TemporalStore::new(adapter),
                temporal_storage::GraphId::new(routing.graph.graph_id()),
                valid_time,
                resolved.transaction_time(),
                None,
            )
            .await
            .map_err(|error| RemoteGatewayServiceError::Query(error.to_string()))?;
            vertices.extend(fragment_vertices);
            edges.extend(fragment_edges);
        }
        vertices.sort_unstable();
        vertices.dedup();
        edges.sort_by_key(|edge| (edge.source(), edge.destination(), edge.weight().to_bits()));
        edges.dedup_by(|left, right| {
            left.source() == right.source()
                && left.destination() == right.destination()
                && left.weight().to_bits() == right.weight().to_bits()
        });
        let graph = SnapshotGraph::new(vertices, edges, true)
            .map_err(|error| RemoteGatewayServiceError::Query(error.to_string()))?;
        let (algorithm, call_parameters) = parse_algorithm_call(text, parameters)?;
        let request =
            AlgorithmRequest::new(algorithm, ProjectedGraph::Snapshot(graph), call_parameters)
                .map_err(|error| RemoteGatewayServiceError::Query(error.to_string()))?;
        let result = BuiltInProvider::new()
            .execute(request)
            .map_err(|error| RemoteGatewayServiceError::Query(error.to_string()))?;
        Ok(json!({
            "version": 1,
            "kind": "analytics_result",
            "columns": result.columns(),
            "rows": result.rows().iter().map(|row| row.iter().map(algorithm_value_json).collect::<Vec<_>>()).collect::<Vec<_>>(),
            "metadata": result
                .metadata()
                .iter()
                .map(|(key, value)| (key.clone(), algorithm_value_json(value)))
                .collect::<Map<_, _>>(),
        }))
    }

    async fn execute_cypher_response(
        &self,
        request_id: u128,
        deadline_unix_ms: u64,
        routing: &GatewayRoutingState,
        text: &str,
        parameters: BTreeMap<String, RuntimeValue>,
    ) -> Result<CypherQueryResponse, RemoteGatewayServiceError> {
        let security_fingerprint = request_security_fingerprint(self.cluster_id, request_id);
        let shard_ids = routing
            .deployment
            .all_shards()
            .iter()
            .map(|placement| placement.shard_id())
            .collect::<Vec<_>>();
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
        for placement in routing.deployment.all_shards() {
            let adapter = ShardClientStorageAdapter::new(
                Arc::clone(&client),
                routing.graph.graph_id(),
                placement.shard_id(),
                placement.placement_epoch(),
                deadline_unix_ms,
                request_namespace(request_id, placement.shard_id()),
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
        let (query_snapshot, _) = self
            .allocate_transaction_timestamps(request_id, deadline_unix_ms)
            .await?;
        let request = CypherQueryRequest::new(
            text,
            parameters,
            ValidTime::from_micros(unix_time_micros()?),
            query_snapshot,
            security_fingerprint,
            deadline_unix_ms,
        );
        CypherQueryEngine::new(config)
            .execute(&coordinator, request)
            .await
            .map_err(|error| RemoteGatewayServiceError::Query(error.to_string()))
    }

    async fn execute_legacy_query(
        &self,
        request_id: u128,
        deadline_unix_ms: u64,
        routing: &GatewayRoutingState,
        text: &str,
    ) -> Result<Value, RemoteGatewayServiceError> {
        let plan = temporal_query::parse(text)
            .map_err(|error| RemoteGatewayServiceError::Query(error.to_string()))?;
        if plan.scope().graph().value() != routing.graph.graph_id() {
            return Err(RemoteGatewayServiceError::Query(
                "query targets another graph".into(),
            ));
        }
        let client: Arc<dyn ShardClient> = self.shard_client.clone();
        let result = if plan.is_global() {
            let PlanBody::Scan { transaction, .. } = plan.body() else {
                return Err(RemoteGatewayServiceError::Query(
                    "global plan is not a scan".into(),
                ));
            };
            let topology_epoch = routing.graph.topology().epoch();
            let expected = routing
                .deployment
                .all_shards()
                .iter()
                .map(|placement| placement.shard_id())
                .collect::<Vec<_>>();
            let mut batches = Vec::with_capacity(expected.len());
            for placement in routing.deployment.all_shards() {
                let adapter = ShardClientStorageAdapter::new(
                    Arc::clone(&client),
                    routing.graph.graph_id(),
                    placement.shard_id(),
                    placement.placement_epoch(),
                    deadline_unix_ms,
                    request_namespace(request_id, placement.shard_id()),
                )
                .map_err(|error| RemoteGatewayServiceError::Query(error.to_string()))?;
                let result = LocalExecutor::new(TemporalStore::new(adapter))
                    .execute(&plan)
                    .await
                    .map_err(|error| RemoteGatewayServiceError::Query(error.to_string()))?;
                batches.push(ShardQueryBatch::new(
                    placement.shard_id(),
                    topology_epoch,
                    SnapshotToken::from(*transaction),
                    result,
                ));
            }
            merge_distributed_results(&plan, topology_epoch, &expected, batches)
                .map_err(|error| RemoteGatewayServiceError::Query(error.to_string()))?
        } else {
            let route = routing
                .deployment
                .route_plan(&plan)
                .map_err(|error| RemoteGatewayServiceError::Query(error.to_string()))?;
            let adapter = ShardClientStorageAdapter::new(
                client,
                routing.graph.graph_id(),
                route.shard().shard_id(),
                route.shard().placement_epoch(),
                deadline_unix_ms,
                request_namespace(request_id, route.shard().shard_id()),
            )
            .map_err(|error| RemoteGatewayServiceError::Query(error.to_string()))?;
            LocalExecutor::new(TemporalStore::new(adapter))
                .execute(&plan)
                .await
                .map_err(|error| RemoteGatewayServiceError::Query(error.to_string()))?
        };
        let canonical = result
            .to_canonical_json()
            .map_err(|error| RemoteGatewayServiceError::Query(error.to_string()))?;
        serde_json::from_str(&canonical)
            .map_err(|error| RemoteGatewayServiceError::Query(error.to_string()))
    }

    async fn allocate_transaction_timestamps(
        &self,
        request_id: u128,
        deadline_unix_ms: u64,
    ) -> Result<(TransactionTime, TransactionTime), RemoteGatewayServiceError> {
        let mut last_error = None;
        for endpoint in self.meta_endpoints.iter() {
            let address = format!("http://{endpoint}");
            let Ok(mut client) = MetaServiceClient::connect(address).await else {
                continue;
            };
            let response = client
                .allocate_timestamp(AllocateTimestampRequest {
                    context: Some(RequestContext {
                        protocol_version: CLUSTER_PROTOCOL_VERSION,
                        cluster_id: self.cluster_id.to_vec(),
                        request_id: request_id.to_be_bytes().to_vec(),
                        deadline_unix_ms,
                    }),
                    count: 3,
                    observed_physical_ms: unix_time_ms()
                        .map_err(|status| RemoteGatewayServiceError::Meta(status.to_string()))?,
                })
                .await;
            match response {
                Ok(response) => {
                    let response = response.into_inner();
                    if response.count != 3 {
                        return Err(RemoteGatewayServiceError::Meta(
                            "Meta returned the wrong timestamp batch size".into(),
                        ));
                    }
                    let physical = response
                        .first_physical_ms
                        .checked_mul(1_000)
                        .and_then(|value| i64::try_from(value).ok())
                        .ok_or_else(|| {
                            RemoteGatewayServiceError::Meta(
                                "Meta timestamp physical value overflowed".into(),
                            )
                        })?;
                    let start = TransactionTime::new(physical, response.first_logical);
                    let commit = advance_timestamp(start, 2)
                        .map_err(|error| RemoteGatewayServiceError::Meta(error.to_string()))?;
                    return Ok((start, commit));
                }
                Err(error) => last_error = Some(error.to_string()),
            }
        }
        Err(RemoteGatewayServiceError::Meta(
            last_error.unwrap_or_else(|| "no Meta endpoint was reachable".into()),
        ))
    }
}

impl BoltQueryBackend for RemoteGatewayService {
    fn execute<'a>(&'a self, request: BoltQueryRequest) -> BackendFuture<'a, BackendQueryResult> {
        Box::pin(async move {
            if request.transaction().is_some() {
                return Err(bolt_server::ServiceError::new(
                    "Neo.ClientError.Transaction.TransactionStartFailed",
                    "explicit Bolt transactions are not enabled for Temporal Cypher yet",
                ));
            }
            let sequence = self.bolt_request_sequence.fetch_add(1, Ordering::Relaxed);
            if sequence == u64::MAX {
                return Err(bolt_server::ServiceError::new(
                    "Neo.TransientError.General.DatabaseUnavailable",
                    "Bolt request identity space exhausted",
                ));
            }
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
            let routing = self.routing_snapshot().map_err(gateway_bolt_error)?;
            let compiled = compile_cypher(&routing, request.query()).map_err(gateway_bolt_error)?;
            if compiled.is_procedure() {
                let result = self
                    .execute_analytics_call(
                        u128::from(sequence),
                        deadline,
                        &routing,
                        request.query(),
                        request.parameters().clone(),
                    )
                    .await
                    .map_err(gateway_bolt_error)?;
                return Ok(BackendQueryResult::new(
                    vec!["result".into()],
                    vec![vec![RuntimeValue::String(result.to_string())]],
                    BTreeMap::new(),
                ));
            }
            if !compiled.is_read_only() {
                let summary = self
                    .execute_cypher_write(
                        u128::from(sequence),
                        deadline,
                        &routing,
                        &compiled,
                        request.parameters().clone(),
                    )
                    .await
                    .map_err(gateway_bolt_error)?;
                let transaction_id = summary
                    .get("transaction_id")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned();
                return Ok(BackendQueryResult::new(
                    Vec::new(),
                    Vec::new(),
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
                    ]),
                ));
            }
            let response = self
                .execute_cypher_response(
                    u128::from(sequence),
                    deadline,
                    &routing,
                    request.query(),
                    request.parameters().clone(),
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
}

fn gateway_bolt_error(error: RemoteGatewayServiceError) -> bolt_server::ServiceError {
    bolt_server::ServiceError::new(
        "Neo.ClientError.Statement.ExecutionFailed",
        error.to_string(),
    )
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

fn parse_algorithm_call(
    text: &str,
    parameters: BTreeMap<String, RuntimeValue>,
) -> Result<(String, BTreeMap<String, AlgorithmValue>), RemoteGatewayServiceError> {
    let call = text
        .find("CALL")
        .ok_or_else(|| RemoteGatewayServiceError::Query("CALL keyword is required".into()))?;
    let body = text[call + 4..].trim_start();
    let open = body.find('(').ok_or_else(|| {
        RemoteGatewayServiceError::Query("procedure call requires parentheses".into())
    })?;
    let name = body[..open].trim().to_owned();
    if !name.starts_with("dtg.") {
        return Err(RemoteGatewayServiceError::Query(
            "only dtg.* procedures are exposed by this gateway".into(),
        ));
    }
    let close = body[open + 1..]
        .find(')')
        .map(|offset| open + 1 + offset)
        .ok_or_else(|| {
            RemoteGatewayServiceError::Query("procedure call has no closing parenthesis".into())
        })?;
    let argument_source = body[open + 1..close].trim();
    let mut result = BTreeMap::new();
    if !argument_source.is_empty() {
        let source = argument_source
            .strip_prefix('{')
            .and_then(|value| value.strip_suffix('}'))
            .ok_or_else(|| {
                RemoteGatewayServiceError::Query("procedure arguments must be a map".into())
            })?;
        for item in source.split(',') {
            let (key, value) = item.split_once(':').ok_or_else(|| {
                RemoteGatewayServiceError::Query("procedure map argument requires key:value".into())
            })?;
            let key = key.trim().to_owned();
            let value = value.trim();
            let parsed = if let Some(name) = value.strip_prefix('$') {
                parameters
                    .get(name)
                    .ok_or_else(|| {
                        RemoteGatewayServiceError::Query(format!(
                            "missing procedure parameter ${name}"
                        ))
                    })
                    .and_then(runtime_algorithm_value)
            } else if let Ok(integer) = value.parse::<i64>() {
                if matches!(key.as_str(), "source" | "destination") {
                    Ok(AlgorithmValue::Vertex(VertexId::new(integer as u128)))
                } else {
                    Ok(AlgorithmValue::Integer(integer))
                }
            } else if value.eq_ignore_ascii_case("true") || value.eq_ignore_ascii_case("false") {
                Ok(AlgorithmValue::Boolean(value.eq_ignore_ascii_case("true")))
            } else {
                Ok(AlgorithmValue::String(value.trim_matches('\'').to_owned()))
            }?;
            let normalized = normalize_algorithm_parameter(&name, &key, parsed);
            result.insert(key, normalized);
        }
    }
    Ok((name, result))
}

fn normalize_algorithm_parameter(name: &str, key: &str, value: AlgorithmValue) -> AlgorithmValue {
    if matches!(key, "source" | "destination") {
        return match value {
            AlgorithmValue::Integer(integer) if integer >= 0 => {
                AlgorithmValue::Vertex(VertexId::new(integer as u128))
            }
            other => other,
        };
    }
    if matches!(key, "validFrom" | "validTo" | "deadline") {
        return match value {
            AlgorithmValue::Integer(integer) => {
                AlgorithmValue::Time(ValidTime::from_micros(integer))
            }
            other => other,
        };
    }
    let _ = name;
    value
}

fn runtime_algorithm_value(
    value: &RuntimeValue,
) -> Result<AlgorithmValue, RemoteGatewayServiceError> {
    match value {
        RuntimeValue::Null => Ok(AlgorithmValue::Null),
        RuntimeValue::Boolean(value) => Ok(AlgorithmValue::Boolean(*value)),
        RuntimeValue::Integer(value) => Ok(AlgorithmValue::Integer(*value)),
        RuntimeValue::FloatBits(value) => Ok(AlgorithmValue::FloatBits(*value)),
        RuntimeValue::String(value) => Ok(AlgorithmValue::String(value.clone())),
        RuntimeValue::TimestampMicros(value) => {
            Ok(AlgorithmValue::Time(ValidTime::from_micros(*value)))
        }
        _ => Err(RemoteGatewayServiceError::Query(
            "unsupported runtime value in analytics procedure parameter".into(),
        )),
    }
}

fn algorithm_value_json(value: &AlgorithmValue) -> Value {
    match value {
        AlgorithmValue::Null => Value::Null,
        AlgorithmValue::Boolean(value) => json!(value),
        AlgorithmValue::Integer(value) => json!(value),
        AlgorithmValue::FloatBits(value) => {
            json!({"type": "float", "bits": format!("{value:016x}")})
        }
        AlgorithmValue::String(value) => json!(value),
        AlgorithmValue::Vertex(value) => json!({"type": "vertex", "id": value.value().to_string()}),
        AlgorithmValue::Time(value) => {
            json!({"type": "timestamp", "micros": value.as_micros().to_string()})
        }
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

fn is_legacy_query(text: &str) -> bool {
    let mut words = text
        .split_ascii_whitespace()
        .map(|word| word.to_ascii_uppercase());
    match words.next().as_deref() {
        Some("VERTEX" | "EDGE" | "SCAN") => true,
        Some("DIFF") => matches!(words.next().as_deref(), Some("VERTEX" | "EDGE")),
        _ => false,
    }
}

fn request_security_fingerprint(cluster_id: [u8; 16], request_id: u128) -> [u8; 32] {
    let mut fingerprint = [0; 32];
    fingerprint[..16].copy_from_slice(&cluster_id);
    fingerprint[16..].copy_from_slice(&request_id.to_be_bytes());
    fingerprint
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
    Ok(json!({
        "version": 2,
        "query_fingerprint": hex_bytes(&response.fingerprint()),
        "columns": columns,
        "row_count": response.row_count(),
        "rows": rows,
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
            Self::Query(message) => write!(formatter, "Gateway query error: {message}"),
        }
    }
}

impl Error for RemoteGatewayServiceError {}

#[cfg(test)]
mod tests {
    use query_executor::v2::RuntimeValue;
    use serde_json::json;

    use super::runtime_value_json;

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
}
