use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use cluster_protocol::proto::gateway_service_server::GatewayService;
use cluster_protocol::proto::meta_service_client::MetaServiceClient;
use cluster_protocol::proto::{
    AllocateTimestampRequest, GatewaySubmitRequest, GatewaySubmitResponse, RequestContext,
};
use cluster_protocol::{CLUSTER_PROTOCOL_VERSION, CommonRequestContext, MAX_COMMAND_BYTES};
use control_plane::GraphDefinition;
use dtgproxy::gateway::{GATEWAY_API_VERSION, GatewayOperation, GatewayRequest};
use dtgproxy::{DeploymentConfig, TransactionContext, TransactionCoordinator};
use query_executor::{LocalExecutor, ShardQueryBatch, SnapshotToken, merge_distributed_results};
use serde_json::{Value, json};
use shard_client::{RemoteShardClient, ShardClient, ShardClientStorageAdapter};
use temporal_ir::PlanBody;
use temporal_storage::TemporalStore;
use temporal_types::TransactionTime;
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
