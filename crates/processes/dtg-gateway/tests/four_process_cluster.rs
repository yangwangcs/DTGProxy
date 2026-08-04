use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use dtg_controller::ControllerConfig;
use dtg_data::{DataNodeBuilder, DataProcessConfig, LifecycleState};
use dtg_execution::cluster_protocol::proto::data_service_server::DataService;
use dtg_execution::cluster_protocol::proto::gateway_service_server::GatewayService as ClusterGatewayService;
use dtg_execution::cluster_protocol::proto::{
    self, BoundedPayload, RequestContext, ShardContext, TransactionOperation, TransactionRequest,
};
use dtg_execution::cluster_protocol::{PROTOCOL_MAJOR, checksum_bytes};
use dtg_execution::control::{CatalogState, ObservedNodeState, Version};
use dtg_execution::planning::{
    CatalogShard, CatalogSnapshot, PlanningContext, SnapshotRequirements,
};
use dtg_execution::shard::{CommitSingleShard, ShardCommand};
use dtg_execution::storage::{
    BackendClass, BindingRole, CapabilityManifest, CommandId, EdgeId, EdgeVersion, LogicalMutation,
    Properties, ProviderKind, ReplicaBinding, TransactionTime, ValidInterval, VertexId,
    VertexVersion,
};
use dtg_execution::transaction::TransactionId;
use dtg_execution::{
    GatewayClusterRequest, GatewayExecution, GatewayExecutionError, GatewayExecutionTransport,
    GatewayFuture, GatewayProtocolV2Client, GatewayProtocolV2Transport, GatewayResponse,
    GatewayRetry, GatewayRows, GatewayValue, ShardRoutedGatewayTransport,
};
use dtg_gateway::{GatewayConfig, GatewayService};
use dtg_meta::MetaConfig;
use serde_json::Value;
use support::planning_context;
use tonic::Request;
use tonic::codegen::tokio_stream::StreamExt;

const FIXTURE_ROOT: &str = "../../../config/examples/clean-break-cluster";
const STATIC_SHARD_CASE_TIMEOUT: Duration = Duration::from_secs(20);
static NEXT_STATIC_SHARD_CASE_ID: AtomicUsize = AtomicUsize::new(1);

#[test]
fn clean_break_fixtures_define_four_roles_and_two_heterogeneous_data_nodes() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join(FIXTURE_ROOT);
    let meta = read(&root.join("meta-1.json"));
    let controller = read(&root.join("controller-1.json"));
    let gateway = read(&root.join("gateway-1.json"));
    let data_1 = read(&root.join("data-1.json"));
    let data_2 = read(&root.join("data-2.json"));

    let cluster_ids = [&meta, &controller, &gateway, &data_1, &data_2]
        .map(|document| document["cluster_id"].as_u64().unwrap())
        .into_iter()
        .collect::<BTreeSet<_>>();
    assert_eq!(cluster_ids, BTreeSet::from([9001]));

    assert_eq!(provider(&data_1), "fjall");
    assert_eq!(provider(&data_2), "postgresql");
    assert_ne!(data_1["node_id"], data_2["node_id"]);
    assert_ne!(data_1["rpc_addr"], data_2["rpc_addr"]);
    assert_ne!(data_1["fjall_root"], data_2["fjall_root"]);
    assert_ne!(data_1["consensus_root"], data_2["consensus_root"]);
    assert_eq!(data_1["shards"], serde_json::json!([1, 3]));
    assert_eq!(data_2["shards"], serde_json::json!([1, 2]));
    assert_eq!(
        gateway["cluster_endpoint"],
        Value::String(format!("http://{}", data_1["rpc_addr"].as_str().unwrap()))
    );

    MetaConfig::load(root.join("meta-1.json")).unwrap();
    ControllerConfig::load(root.join("controller-1.json")).unwrap();
    assert!(Path::new(env!("CARGO_BIN_EXE_dtgproxy-gateway")).is_file());

    let encoded = [meta, controller, gateway, data_1, data_2]
        .into_iter()
        .map(|document| document.to_string())
        .collect::<String>()
        .to_ascii_lowercase();
    let forbidden = [
        ["rocks", "db"].concat(),
        ["adapter", "-sidecar"].concat(),
        ["procedure", "-runtime"].concat(),
        ["cluster", ".v1"].concat(),
    ];
    for forbidden in forbidden {
        assert!(!encoded.contains(&forbidden));
    }
}

fn read(path: &Path) -> Value {
    serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
}

fn provider(document: &Value) -> &str {
    document["provider_class"].as_str().unwrap()
}

#[test]
fn fixture_paths_remain_repository_relative_and_environment_neutral() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(FIXTURE_ROOT);
    for name in [
        "meta-1.json",
        "controller-1.json",
        "gateway-1.json",
        "data-1.json",
        "data-2.json",
    ] {
        let document = read(&root.join(name));
        for field in ["data_directory", "fjall_root", "consensus_root"] {
            if let Some(path) = document.get(field).and_then(Value::as_str) {
                assert!(!Path::new(path).is_absolute());
            }
        }
    }
}

struct CertifiedTransport;

impl GatewayExecutionTransport for CertifiedTransport {
    fn execute(
        &self,
        _request: GatewayClusterRequest,
    ) -> GatewayFuture<'_, Result<GatewayResponse, GatewayExecutionError>> {
        Box::pin(async {
            Ok(GatewayResponse::Rows(
                GatewayRows::new(vec!["n.id".into()], vec![vec![GatewayValue::Integer(7)]])
                    .unwrap(),
            ))
        })
    }
}

#[tokio::test]
async fn four_role_composition_uses_only_clean_break_process_contracts() {
    let root = tempfile::tempdir().unwrap();
    let meta = dtg_meta::MetaProcess::open(
        MetaConfig::for_test(root.path().join("meta"), 9001, 1).unwrap(),
    )
    .await
    .unwrap();
    let start = meta
        .timestamps()
        .allocate_start_time(TransactionId::new(1).unwrap())
        .await
        .unwrap();
    assert!(start.get() > 0);

    let controller = dtg_controller::ControllerProcess::open_for_test(
        ControllerConfig::for_test(root.path().join("controller"), 9001, 2).unwrap(),
        CatalogState::new(),
    )
    .await
    .unwrap();
    controller
        .record_observation(
            ObservedNodeState::new("data-11".into(), Version::new(0), Vec::new()).unwrap(),
        )
        .await
        .unwrap();
    assert!(controller.reconcile().await.unwrap().is_empty());

    let data = DataNodeBuilder::from_config(DataProcessConfig::new(
        root.path().join("data/business"),
        root.path().join("data/raft"),
    ))
    .start()
    .await
    .unwrap();
    assert_eq!(data.lifecycle(), LifecycleState::Ready);
    assert_eq!(data.rpc_service().protocol_major(), 2);

    let gateway = GatewayService::new(
        GatewayConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            9001,
            std::time::Duration::from_secs(5),
        )
        .unwrap(),
        GatewayExecution::for_process(Arc::new(CertifiedTransport), planning_context()),
    );
    let rows = gateway
        .bolt()
        .query("MATCH (n) FOR SYSTEM_TIME AS OF $t RETURN n.id ORDER BY n.id")
        .param("t", 41_i64)
        .run()
        .await
        .unwrap();
    assert_eq!(rows.rows(), &[vec![GatewayValue::Integer(7)]]);

    data.stop();
    assert_eq!(data.lifecycle(), LifecycleState::Stopped);
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct FragmentFenceEvidence {
    shard_id: u32,
    placement_epoch: u64,
    backend_generation: u64,
    catalog_revision: u64,
    schema_version: u64,
    capability_digest: Vec<u8>,
    applied_index: u64,
    transaction_time: i64,
    valid_at: i64,
    snapshot_immutable: bool,
}

struct InProcessDataEndpoint {
    service: dtg_data::DataRpcService,
    calls: AtomicUsize,
    fences: Mutex<Vec<FragmentFenceEvidence>>,
}

impl InProcessDataEndpoint {
    fn new(service: dtg_data::DataRpcService) -> Self {
        Self {
            service,
            calls: AtomicUsize::new(0),
            fences: Mutex::new(Vec::new()),
        }
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    fn fences(&self) -> Vec<FragmentFenceEvidence> {
        self.fences.lock().unwrap().clone()
    }
}

impl GatewayProtocolV2Client for InProcessDataEndpoint {
    fn execute(
        &self,
        request: proto::GatewayRequest,
    ) -> GatewayFuture<'_, Result<Vec<proto::GatewayResponse>, GatewayExecutionError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.fences
            .lock()
            .unwrap()
            .extend(request.fragments.iter().map(|fragment| {
                let context = fragment.context.as_ref().unwrap();
                FragmentFenceEvidence {
                    shard_id: context.shard_id,
                    placement_epoch: context.placement_epoch,
                    backend_generation: context.backend_generation,
                    catalog_revision: context.catalog_version,
                    schema_version: fragment.schema_version,
                    capability_digest: fragment.capability_digest.clone(),
                    applied_index: fragment.applied_index,
                    transaction_time: fragment.transaction_time,
                    valid_at: fragment.valid_at,
                    snapshot_immutable: fragment.snapshot_immutable,
                }
            }));
        let service = self.service.clone();
        Box::pin(async move {
            let mut stream = ClusterGatewayService::execute(&service, Request::new(request))
                .await
                .map_err(|error| {
                    GatewayExecutionError::new(
                        "DTG-TEST-DATA-RPC",
                        error.to_string(),
                        GatewayRetry::Never,
                    )
                })?
                .into_inner();
            let mut responses = Vec::new();
            while let Some(response) = stream.next().await {
                responses.push(response.map_err(|error| {
                    GatewayExecutionError::new(
                        "DTG-TEST-DATA-STREAM",
                        error.to_string(),
                        GatewayRetry::Never,
                    )
                })?);
            }
            Ok(responses)
        })
    }
}

fn static_shard_capabilities() -> CapabilityManifest {
    CapabilityManifest::from_names([
        "adjacency",
        "immutable-read-view",
        "logical-snapshot",
        "point",
    ])
    .unwrap()
}

fn static_shard_binding(
    capabilities: &CapabilityManifest,
    provider: ProviderKind,
    shard_id: u64,
    replica_id: u64,
    namespace: &str,
) -> ReplicaBinding {
    let class = BackendClass::new(
        provider.clone(),
        1,
        1,
        capabilities.names().map(str::to_owned),
    )
    .unwrap();
    ReplicaBinding::builder()
        .cluster_id(9001)
        .graph_id(1)
        .shard_id(shard_id)
        .placement_epoch(17)
        .replica_id(replica_id)
        .backend_generation(23)
        .backend_class_digest(class.digest())
        .provider_kind(provider)
        .contract_version(1)
        .layout_version(1)
        .capability_digest(capabilities.digest())
        .namespace_id(namespace)
        .endpoint_profile_ref("local")
        .credential_ref("local")
        .role(BindingRole::Active)
        .build()
        .unwrap()
}

fn static_shard_context(binding: &ReplicaBinding, request_id: u128) -> ShardContext {
    ShardContext {
        request: Some(RequestContext {
            protocol_major: PROTOCOL_MAJOR,
            protocol_minor: 1,
            cluster_id: binding.cluster_id().get().to_be_bytes().to_vec(),
            request_id: request_id.to_be_bytes().to_vec(),
            deadline_unix_ms: u64::MAX,
            trace_context: Vec::new(),
        }),
        graph_id: binding.graph_id().get(),
        shard_id: u32::try_from(binding.shard_id().get()).unwrap(),
        placement_epoch: binding.placement_epoch().get(),
        backend_generation: binding.backend_generation().get(),
        catalog_version: 29,
    }
}

async fn commit_static_shard(
    node: &dtg_data::DataNode,
    binding: &ReplicaBinding,
    command_id: u128,
    mutations: Vec<LogicalMutation>,
) -> u64 {
    let item_count = u32::try_from(mutations.len()).unwrap();
    let command = ShardCommand::CommitSingleShard(
        CommitSingleShard::new(
            CommandId::new(command_id).unwrap(),
            binding.placement_epoch().get(),
            binding.backend_generation().get(),
            mutations,
        )
        .unwrap(),
    );
    let body = command.encode_current().unwrap();
    node.rpc_service()
        .apply_transaction(Request::new(TransactionRequest {
            context: Some(static_shard_context(binding, command_id)),
            transaction_id: command_id.to_be_bytes().to_vec(),
            operation: TransactionOperation::Commit.into(),
            idempotency_key: command_id.to_be_bytes().to_vec(),
            payload: Some(BoundedPayload {
                format_version: 1,
                declared_len: u64::try_from(body.len()).unwrap(),
                item_count,
                checksum: checksum_bytes(&body).to_vec(),
                body,
            }),
        }))
        .await
        .unwrap();
    node.replica_observations().await[0].applied_index()
}

fn static_vertex(id: u128) -> LogicalMutation {
    LogicalMutation::PutVertex(
        VertexVersion::new(
            VertexId::new(id).unwrap(),
            Version::new(1),
            ValidInterval::new(1, 100).unwrap(),
            TransactionTime::new(41).unwrap(),
            Properties::new(),
        )
        .unwrap(),
    )
}

fn static_edge(id: u128, source: u128, target: u128) -> LogicalMutation {
    LogicalMutation::PutEdge(
        EdgeVersion::new(
            EdgeId::new(id).unwrap(),
            VertexId::new(source).unwrap(),
            VertexId::new(target).unwrap(),
            "KNOWS",
            Version::new(1),
            ValidInterval::new(1, 100).unwrap(),
            TransactionTime::new(41).unwrap(),
            Properties::new(),
        )
        .unwrap(),
    )
}

fn static_shard_planning_context(
    capabilities: CapabilityManifest,
    shards: Vec<(ReplicaBinding, u64)>,
) -> PlanningContext {
    PlanningContext::new(
        CatalogSnapshot::new(
            Version::new(29),
            Version::new(31),
            shards
                .into_iter()
                .map(|(binding, applied_index)| CatalogShard::new(binding, applied_index))
                .collect(),
        )
        .unwrap(),
        capabilities,
        SnapshotRequirements::fixed(TransactionTime::new(41).unwrap(), 10),
        Some(128),
    )
    .unwrap()
}

fn endpoint_call_counts(endpoints: &[Arc<InProcessDataEndpoint>]) -> Vec<usize> {
    endpoints.iter().map(|endpoint| endpoint.calls()).collect()
}

fn endpoint_call_delta(before: &[usize], after: &[usize]) -> Vec<usize> {
    before
        .iter()
        .zip(after)
        .map(|(before, after)| after - before)
        .collect()
}

#[tokio::test]
async fn static_shard_snapshot_routes_fjall() {
    let result = run_static_shard_case(ProviderKind::Fjall, StaticShardRuntime::empty()).await;

    assert_eq!(result.point_call_delta, [0, 0, 1]);
    assert_eq!(result.adjacency_call_delta, [0, 0, 1]);
    assert_eq!(result.count_call_delta, [1, 1, 1]);
    assert!(result.fences.iter().all(|fence| fence.snapshot_immutable));
}

#[tokio::test]
async fn static_shard_snapshot_routes_kuzu() {
    let result = run_static_shard_case(ProviderKind::Kuzu, StaticShardRuntime::empty()).await;

    assert_eq!(result.point_call_delta, [0, 0, 1]);
    assert_eq!(result.adjacency_call_delta, [0, 0, 1]);
    assert_eq!(result.count_call_delta, [1, 1, 1]);
    assert!(result.fences.iter().all(|fence| fence.snapshot_immutable));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires PostgreSQL endpoint and credential test environment"]
async fn static_shard_snapshot_routes_postgresql() {
    let runtime = StaticShardRuntime::from_environment();
    let result = run_static_shard_case(ProviderKind::PostgreSql, runtime).await;

    assert_eq!(result.point_call_delta, [0, 0, 1]);
    assert_eq!(result.adjacency_call_delta, [0, 0, 1]);
    assert_eq!(result.count_call_delta, [1, 1, 1]);
    assert!(result.fences.iter().all(|fence| fence.snapshot_immutable));
}

struct StaticShardCase {
    point_call_delta: [usize; 3],
    adjacency_call_delta: [usize; 3],
    count_call_delta: [usize; 3],
    fences: Vec<FragmentFenceEvidence>,
}

#[derive(Clone, Debug)]
struct StaticShardRuntime {
    postgres_endpoint: String,
    postgres_credential: String,
}

impl StaticShardRuntime {
    fn empty() -> Self {
        Self {
            postgres_endpoint: String::new(),
            postgres_credential: String::new(),
        }
    }

    fn from_environment() -> Self {
        Self {
            postgres_endpoint: std::env::var("DTG_STATIC_SHARD_POSTGRES_ENDPOINT")
                .or_else(|_| std::env::var("DTG_BACKEND_E2E_POSTGRES_ENDPOINT"))
                .or_else(|_| std::env::var("DTG_POSTGRES_URL"))
                .expect("a PostgreSQL endpoint is required"),
            postgres_credential: std::env::var("DTG_STATIC_SHARD_POSTGRES_CREDENTIAL")
                .or_else(|_| std::env::var("DTG_BACKEND_E2E_POSTGRES_CREDENTIAL"))
                .unwrap_or_default(),
        }
    }
}

async fn run_static_shard_case(
    backend: ProviderKind,
    runtime: StaticShardRuntime,
) -> StaticShardCase {
    tokio::time::timeout(
        STATIC_SHARD_CASE_TIMEOUT,
        static_shard_case(backend, runtime),
    )
    .await
    .expect("static three-Data-shard topology must complete before its bounded timeout")
}

async fn static_shard_case(backend: ProviderKind, runtime: StaticShardRuntime) -> StaticShardCase {
    let root = tempfile::tempdir().unwrap();
    let capabilities = static_shard_capabilities();
    let case_id = format!(
        "{}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        NEXT_STATIC_SHARD_CASE_ID.fetch_add(1, Ordering::SeqCst)
    );
    let bindings = [
        static_shard_binding(
            &capabilities,
            backend.clone(),
            13,
            19,
            &format!("static-shard-{case_id}-13"),
        ),
        static_shard_binding(
            &capabilities,
            backend.clone(),
            14,
            20,
            &format!("static-shard-{case_id}-14"),
        ),
        static_shard_binding(
            &capabilities,
            backend.clone(),
            15,
            21,
            &format!("static-shard-{case_id}-15"),
        ),
    ];
    assert_eq!(
        bindings
            .iter()
            .map(|binding| binding.namespace_id().as_str())
            .collect::<BTreeSet<_>>()
            .len(),
        3
    );
    let mut nodes = Vec::new();
    for (ordinal, binding) in bindings.iter().enumerate() {
        let config = DataProcessConfig::new(
            root.path().join(format!("data-{ordinal}/business")),
            root.path().join(format!("data-{ordinal}/raft")),
        )
        .with_backend_kind(backend.clone())
        .with_kuzu_root(root.path().join(format!("data-{ordinal}/kuzu")))
        .assign(binding.clone());
        let config = if backend == ProviderKind::PostgreSql {
            config
                .with_endpoint_profile(
                    "local",
                    dtg_data::EndpointProfile::PostgreSql(runtime.postgres_endpoint.clone()),
                )
                .with_credential_profile(
                    "local",
                    dtg_data::CredentialProfile::PostgreSql(runtime.postgres_credential.clone()),
                )
        } else {
            config
        };
        nodes.push(DataNodeBuilder::from_config(config).start().await.unwrap());
    }
    for node in &nodes {
        assert_eq!(node.provider_kinds(), vec![backend.clone()]);
    }

    let applied_13 =
        commit_static_shard(&nodes[0], &bindings[0], 101, vec![static_vertex(39)]).await;
    let applied_14 =
        commit_static_shard(&nodes[1], &bindings[1], 102, vec![static_vertex(40)]).await;
    let applied_15 = commit_static_shard(
        &nodes[2],
        &bindings[2],
        103,
        vec![
            static_vertex(41),
            static_vertex(44),
            static_edge(73, 41, 44),
        ],
    )
    .await;
    let applied_indices = [applied_13, applied_14, applied_15];
    assert_eq!(
        applied_indices.into_iter().collect::<BTreeSet<_>>().len(),
        1
    );

    let endpoints = nodes
        .iter()
        .map(|node| Arc::new(InProcessDataEndpoint::new(node.rpc_service())))
        .collect::<Vec<_>>();
    let transports = endpoints
        .iter()
        .map(|endpoint| {
            Arc::new(GatewayProtocolV2Transport::new(endpoint.clone()))
                as Arc<dyn GatewayExecutionTransport>
        })
        .collect::<Vec<_>>();
    let transport = Arc::new(ShardRoutedGatewayTransport::new(
        transports[0].clone(),
        BTreeMap::from([(14, transports[1].clone()), (15, transports[2].clone())]),
    ));
    let planning_context = static_shard_planning_context(
        capabilities.clone(),
        bindings.iter().cloned().zip(applied_indices).collect(),
    );
    let gateway = GatewayService::new(
        GatewayConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            9001,
            std::time::Duration::from_secs(5),
        )
        .unwrap(),
        GatewayExecution::for_process(transport, planning_context),
    );

    let before_point = endpoint_call_counts(&endpoints);
    let point = gateway
        .bolt()
        .query("MATCH (n) WHERE n.id = $id RETURN n.id")
        .param("id", 41_i64)
        .run()
        .await
        .unwrap();
    assert_eq!(point.rows(), &[vec![GatewayValue::Integer(41)]]);
    let point_call_delta = endpoint_call_delta(&before_point, &endpoint_call_counts(&endpoints));

    let before_adjacency = endpoint_call_counts(&endpoints);
    let adjacency = gateway
        .bolt()
        .query("MATCH (a)-[r]->(b) WHERE a.id = $id RETURN r")
        .param("id", 41_i64)
        .run()
        .await
        .unwrap();
    assert_eq!(adjacency.rows().len(), 1);
    let adjacency_call_delta =
        endpoint_call_delta(&before_adjacency, &endpoint_call_counts(&endpoints));

    let before_count = endpoint_call_counts(&endpoints);
    let count = gateway
        .bolt()
        .query("MATCH (n) RETURN COUNT(*)")
        .run()
        .await
        .unwrap();
    assert_eq!(count.rows(), &[vec![GatewayValue::Integer(4)]]);
    let count_call_delta = endpoint_call_delta(&before_count, &endpoint_call_counts(&endpoints));

    let evidence = endpoints
        .iter()
        .flat_map(|endpoint| endpoint.fences())
        .collect::<Vec<_>>();
    assert_eq!(evidence.len(), 5);
    for fence in &evidence {
        assert_eq!(fence.placement_epoch, 17);
        assert_eq!(fence.backend_generation, 23);
        assert_eq!(fence.catalog_revision, 29);
        assert_eq!(fence.schema_version, 31);
        assert_eq!(fence.capability_digest, capabilities.digest().get());
        assert_eq!(
            fence.applied_index,
            applied_indices[usize::try_from(fence.shard_id - 13).unwrap()]
        );
        assert_eq!(fence.transaction_time, 41);
        assert_eq!(fence.valid_at, 10);
        assert!(fence.snapshot_immutable);
    }

    for node in nodes {
        node.stop();
    }

    StaticShardCase {
        point_call_delta: point_call_delta.try_into().unwrap(),
        adjacency_call_delta: adjacency_call_delta.try_into().unwrap(),
        count_call_delta: count_call_delta.try_into().unwrap(),
        fences: evidence,
    }
}

mod support;
