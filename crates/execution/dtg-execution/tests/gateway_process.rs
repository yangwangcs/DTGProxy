use std::collections::BTreeMap;
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use dtg_cluster_v2::{checksum_bytes, proto};
use dtg_execution::{
    GatewayCancellationToken, GatewayClusterRequest, GatewayExecution, GatewayExecutionError,
    GatewayExecutionTransport, GatewayFuture, GatewayProtocolV2Client, GatewayProtocolV2Transport,
    GatewayQueryResponse, GatewayRequestContext, GatewayResponse, GatewayRows, GatewayValue,
    GatewayWriteReceipt, GatewayWriteRequest, GatewayWriteTransport, RequestDetail, RequestStage,
    ShardRoutedGatewayTransport, StageOutcome,
};
use dtg_language_ir::{
    Aggregate, AggregateFunction, AggregateKind, BinaryOperator, Field, GraphScope, LogicalExpr,
    LogicalNode, LogicalNodeId, LogicalNodeKind, LogicalPlan, LogicalProgram, LogicalStatement,
    LogicalType, NodeScan, Parameter, Projection, ReadScope, RowSchema, Sort, SortDirection,
    SortKey, UnaryOperator, Unwind, Value,
};
use dtg_plan::{
    CatalogShard, CatalogSnapshot, Planner, PlanningContext, SnapshotRequirements, StorageAccess,
};
use dtg_storage::{
    BackendClass, BindingRole, CapabilityManifest, LogicalMutation, ProviderKind, ReplicaBinding,
    TransactionTime, Version,
};

#[tokio::test]
async fn cancellation_token_notifies_waiters_without_polling() {
    let token = GatewayCancellationToken::new();
    let waiter_token = token.clone();
    let waiter = tokio::spawn(async move {
        waiter_token.cancelled().await;
    });

    tokio::task::yield_now().await;
    token.cancel();
    tokio::time::timeout(Duration::from_millis(100), waiter)
        .await
        .expect("cancellation waiter should be notified")
        .expect("cancellation waiter should not panic");
}

#[derive(Clone, Copy, Default)]
enum ProtocolFixture {
    #[default]
    RawTwo,
    RawPoint,
    PartialVertexCount,
    UnknownFragment,
    MalformedFragment,
    DuplicateBatch,
    MissingFragment,
}

#[derive(Default)]
struct RecordingProtocolClient {
    requests: Mutex<Vec<proto::GatewayRequest>>,
    fixture: ProtocolFixture,
}

impl RecordingProtocolClient {
    fn with_fixture(fixture: ProtocolFixture) -> Self {
        Self {
            requests: Mutex::new(Vec::new()),
            fixture,
        }
    }
}

impl GatewayProtocolV2Client for RecordingProtocolClient {
    fn execute(
        &self,
        request: proto::GatewayRequest,
    ) -> GatewayFuture<'_, Result<Vec<proto::GatewayResponse>, GatewayExecutionError>> {
        let status = proto::TypedStatus {
            request: request.request.clone(),
            code: 1,
            retry: 1,
            message: "ok".into(),
            idempotency_key: Vec::new(),
            details: (request.execution_request.as_ref().unwrap().body[0] == 3).then(|| {
                let mut body = vec![1];
                body.extend_from_slice(&23_u128.to_be_bytes());
                proto::BoundedPayload {
                    format_version: 1,
                    declared_len: body.len() as u64,
                    item_count: 1,
                    checksum: checksum_bytes(&body).to_vec(),
                    body,
                }
            }),
        };
        let mut batches = if request.execution_request.as_ref().unwrap().body[0] == 1 {
            match self.fixture {
                ProtocolFixture::RawTwo => {
                    vec![encoded_batch(1, encoded_vertex_rows(1..=2), 2)]
                }
                ProtocolFixture::RawPoint => {
                    let encoded_id = 2048_u128.to_be_bytes();
                    let point_lookup = request.fragments.iter().all(|fragment| {
                        fragment.payload.as_ref().is_some_and(|payload| {
                            payload
                                .body
                                .windows(encoded_id.len())
                                .any(|window| window == encoded_id)
                        })
                    });
                    let (rows, row_count) = if point_lookup {
                        (2048..=2048, 1)
                    } else {
                        (1..=4096, 4096)
                    };
                    vec![encoded_batch(1, encoded_vertex_rows(rows), row_count)]
                }
                ProtocolFixture::PartialVertexCount => request
                    .fragments
                    .iter()
                    .map(|fragment| {
                        let fragment_id = u128::from_be_bytes(
                            fragment.fragment_id.as_slice().try_into().unwrap(),
                        );
                        encoded_batch(
                            fragment_id,
                            encoded_integer_rows("__dtg_partial_vertex_count", 2048),
                            1,
                        )
                    })
                    .collect::<Vec<_>>(),
                ProtocolFixture::UnknownFragment => {
                    vec![encoded_batch(2, encoded_vertex_rows(1..=1), 1)]
                }
                ProtocolFixture::MalformedFragment => {
                    let mut batch = encoded_batch(1, encoded_vertex_rows(1..=1), 1);
                    batch.fragment_id.pop();
                    vec![batch]
                }
                ProtocolFixture::DuplicateBatch => vec![
                    encoded_batch(1, encoded_vertex_rows(1..=1), 1),
                    encoded_batch(1, encoded_vertex_rows(2..=2), 1),
                ],
                ProtocolFixture::MissingFragment => {
                    vec![encoded_batch(1, encoded_vertex_rows(1..=1), 1)]
                }
            }
        } else {
            Vec::new()
        };
        for batch in &mut batches {
            batch.request = request.request.clone();
        }
        self.requests.lock().unwrap().push(request);
        Box::pin(async move {
            if batches.is_empty() {
                return Ok(vec![proto::GatewayResponse {
                    status: Some(status),
                    batch: None,
                }]);
            }
            Ok(batches
                .into_iter()
                .map(|batch| proto::GatewayResponse {
                    status: Some(status.clone()),
                    batch: Some(batch),
                })
                .collect())
        })
    }
}

#[derive(Default)]
struct SessionRecordingProtocolClient {
    session_requests: Mutex<Vec<proto::GatewayRequest>>,
    pipeline_requests: Mutex<Vec<proto::GatewayRequest>>,
    direct_requests: Mutex<Vec<proto::GatewayRequest>>,
    session_available: bool,
    pipeline_available: bool,
}

impl SessionRecordingProtocolClient {
    fn unavailable() -> Self {
        Self {
            session_available: false,
            ..Self::default()
        }
    }

    fn available() -> Self {
        Self {
            session_available: true,
            ..Self::default()
        }
    }

    fn pipeline_available() -> Self {
        Self {
            session_available: true,
            pipeline_available: true,
            ..Self::default()
        }
    }
}

impl GatewayProtocolV2Client for SessionRecordingProtocolClient {
    fn execute(
        &self,
        request: proto::GatewayRequest,
    ) -> GatewayFuture<'_, Result<Vec<proto::GatewayResponse>, GatewayExecutionError>> {
        self.direct_requests.lock().unwrap().push(request.clone());
        Box::pin(async move {
            Ok(vec![session_gateway_response(
                &request,
                encoded_batch(1, encoded_vertex_rows(1..=1), 1),
            )])
        })
    }

    fn execute_session(
        &self,
        request: proto::GatewayRequest,
    ) -> GatewayFuture<'_, Result<Option<Vec<proto::GatewayResponse>>, GatewayExecutionError>> {
        self.session_requests.lock().unwrap().push(request.clone());
        let session_available = self.session_available;
        Box::pin(async move {
            Ok(session_available.then(|| {
                vec![session_gateway_response(
                    &request,
                    encoded_batch(1, encoded_vertex_rows(1..=1), 1),
                )]
            }))
        })
    }

    fn execute_pipeline_with_metrics_and_cancellation(
        &self,
        request: proto::GatewayRequest,
        metrics: Arc<dtg_execution::RequestStageMetrics>,
        _cancellation: &GatewayCancellationToken,
    ) -> GatewayFuture<'_, Result<Option<Vec<proto::GatewayResponse>>, GatewayExecutionError>> {
        self.pipeline_requests.lock().unwrap().push(request.clone());
        metrics.record_detail(
            RequestDetail::GatewayQueryPipelineSubmit,
            StageOutcome::Success,
            1,
        );
        metrics.record_detail(
            RequestDetail::GatewayQueryPipelineResponseWait,
            StageOutcome::Success,
            1,
        );
        let pipeline_available = self.pipeline_available;
        Box::pin(async move {
            Ok(pipeline_available.then(|| {
                vec![session_gateway_response(
                    &request,
                    encoded_batch(1, encoded_vertex_rows(1..=1), 1),
                )]
            }))
        })
    }
}

fn session_gateway_response(
    request: &proto::GatewayRequest,
    mut batch: proto::ColumnBatch,
) -> proto::GatewayResponse {
    batch.request = request.request.clone();
    proto::GatewayResponse {
        status: Some(proto::TypedStatus {
            request: request.request.clone(),
            code: 1,
            retry: 1,
            message: "ok".into(),
            idempotency_key: Vec::new(),
            details: None,
        }),
        batch: Some(batch),
    }
}

#[derive(Default)]
struct RecordingWriteTransport {
    events: Mutex<Vec<&'static str>>,
    requests: Mutex<Vec<GatewayWriteRequest>>,
    apply_calls: AtomicUsize,
}

impl GatewayWriteTransport for RecordingWriteTransport {
    fn apply_single_shard(
        &self,
        request: GatewayWriteRequest,
    ) -> GatewayFuture<'_, Result<GatewayWriteReceipt, GatewayExecutionError>> {
        self.events.lock().unwrap().push("apply");
        self.requests.lock().unwrap().push(request);
        let call = self.apply_calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            Ok(GatewayWriteReceipt::with_commit_time(
                38 + call as u64,
                call > 0,
                TransactionTime::new(43).unwrap(),
            ))
        })
    }
}

#[derive(Default)]
struct CapturingClusterTransport {
    requests: Mutex<Vec<GatewayClusterRequest>>,
}

#[derive(Default)]
struct MaterializingClusterTransport {
    requests: Mutex<Vec<GatewayClusterRequest>>,
}

impl GatewayExecutionTransport for MaterializingClusterTransport {
    fn execute(
        &self,
        request: GatewayClusterRequest,
    ) -> GatewayFuture<'_, Result<GatewayResponse, GatewayExecutionError>> {
        self.requests.lock().unwrap().push(request);
        Box::pin(async { Ok(GatewayResponse::Acknowledged) })
    }

    fn execute_query(
        &self,
        request: GatewayClusterRequest,
    ) -> GatewayFuture<'_, Result<GatewayQueryResponse, GatewayExecutionError>> {
        self.requests.lock().unwrap().push(request.clone());
        Box::pin(async move {
            Ok(GatewayQueryResponse::Materialized(
                materialized_fragment_ids(&request),
            ))
        })
    }
}

impl GatewayExecutionTransport for CapturingClusterTransport {
    fn execute(
        &self,
        request: GatewayClusterRequest,
    ) -> GatewayFuture<'_, Result<GatewayResponse, GatewayExecutionError>> {
        self.requests.lock().unwrap().push(request);
        Box::pin(async { Ok(GatewayResponse::Acknowledged) })
    }
}

struct PendingClusterTransport;

impl GatewayExecutionTransport for PendingClusterTransport {
    fn execute(
        &self,
        _request: GatewayClusterRequest,
    ) -> GatewayFuture<'_, Result<GatewayResponse, GatewayExecutionError>> {
        Box::pin(std::future::pending())
    }
}

struct DelayedClusterTransport {
    delay: Duration,
    query_calls: AtomicUsize,
}

struct FixedFragmentClusterTransport {
    response_fragment_id: u32,
}

impl GatewayExecutionTransport for FixedFragmentClusterTransport {
    fn execute(
        &self,
        _request: GatewayClusterRequest,
    ) -> GatewayFuture<'_, Result<GatewayResponse, GatewayExecutionError>> {
        Box::pin(async { Ok(GatewayResponse::Acknowledged) })
    }

    fn execute_query(
        &self,
        _request: GatewayClusterRequest,
    ) -> GatewayFuture<'_, Result<GatewayQueryResponse, GatewayExecutionError>> {
        let fragment_id = self.response_fragment_id;
        Box::pin(async move {
            Ok(GatewayQueryResponse::Materialized(BTreeMap::from([(
                fragment_id,
                Vec::new(),
            )])))
        })
    }
}

impl DelayedClusterTransport {
    fn new(delay: Duration) -> Self {
        Self {
            delay,
            query_calls: AtomicUsize::new(0),
        }
    }
}

impl GatewayExecutionTransport for DelayedClusterTransport {
    fn execute(
        &self,
        _request: GatewayClusterRequest,
    ) -> GatewayFuture<'_, Result<GatewayResponse, GatewayExecutionError>> {
        Box::pin(async { Ok(GatewayResponse::Acknowledged) })
    }

    fn execute_query(
        &self,
        request: GatewayClusterRequest,
    ) -> GatewayFuture<'_, Result<GatewayQueryResponse, GatewayExecutionError>> {
        self.query_calls.fetch_add(1, Ordering::SeqCst);
        let delay = self.delay;
        Box::pin(async move {
            tokio::time::sleep(delay).await;
            Ok(GatewayQueryResponse::Materialized(
                materialized_fragment_ids(&request),
            ))
        })
    }
}

fn materialized_fragment_ids(
    request: &GatewayClusterRequest,
) -> BTreeMap<u32, Vec<dtg_query::ColumnBatch>> {
    request
        .physical_plan()
        .into_iter()
        .flat_map(|plan| plan.fragments())
        .map(|fragment| (fragment.id().get(), Vec::new()))
        .collect()
}

#[test]
fn process_create_commits_through_data_without_meta_round_trips() {
    let writes = Arc::new(RecordingWriteTransport::default());
    let client = Arc::new(RecordingProtocolClient::default());
    let execution = GatewayExecution::for_process_with_writes(
        Arc::new(GatewayProtocolV2Transport::new(client.clone())),
        writes.clone(),
        planning_context(),
    );

    let response = block_on(execution.execute_statement(
        GatewayRequestContext::new(7, 81, u64::MAX, Vec::new()).unwrap(),
        "CREATE (n:Bench {value: 1}) VALID FROM 1".into(),
        BTreeMap::new(),
        None,
        &GatewayCancellationToken::new(),
    ))
    .unwrap();

    assert_eq!(response, GatewayResponse::Acknowledged);
    let apply = execution
        .request_metrics()
        .snapshot()
        .details()
        .find_map(|(detail, snapshot)| {
            (detail == RequestDetail::GatewayDataApplyRpc).then_some(snapshot)
        })
        .unwrap();
    assert_eq!(apply.success, 1);
    assert_eq!(apply.error, 0);
    assert_eq!(apply.cancelled, 0);
    assert_eq!(writes.events.lock().unwrap().as_slice(), ["apply"]);
    let requests = writes.requests.lock().unwrap();
    let request = requests.first().unwrap();
    assert_eq!(
        request.transaction_id().get(),
        0x68097dfc0e984bbabae54d2b1af0a090
    );
    assert_eq!(request.snapshot_applied_index(), 37);
    let dtg_execution::shard::ShardCommand::CommitSingleShardTransaction(command) =
        request.command()
    else {
        panic!("expected a single-shard transaction command")
    };
    assert_eq!(
        command.header().command_id().get(),
        0x0af214337536ce02fe7b52a208925362
    );
    assert_eq!(
        command.request_digest().get(),
        [
            0x28, 0xc3, 0x63, 0xbb, 0x8a, 0xbd, 0x66, 0x5f, 0xa0, 0xd6, 0xc7, 0x85, 0x4a, 0x73,
            0x9f, 0xb3, 0x04, 0xf3, 0x12, 0x4a, 0x35, 0xed, 0xf8, 0x77, 0xa4, 0x58, 0xc1, 0x43,
            0xd6, 0x68, 0x48, 0xae,
        ]
    );
    let [LogicalMutation::PutVertex(vertex)] = request.mutations() else {
        panic!("expected one persisted vertex mutation")
    };
    assert_eq!(vertex.id().get(), 0x55a0592a07ab5f0a6ad41fd79b1e5a6a);
    assert_eq!(vertex.properties().get("value"), Some(&Value::Integer(1)));
    assert_eq!(
        vertex.properties().get("\0dtg.labels"),
        Some(&Value::List(vec![Value::String("Bench".into())]))
    );
    assert_eq!(vertex.valid_time().start(), 1);
    assert_eq!(vertex.valid_time().end(), i64::MAX);
    drop(requests);

    block_on(execution.execute_statement(
        GatewayRequestContext::new(7, 82, u64::MAX, Vec::new()).unwrap(),
        "MATCH (n) RETURN n.id".into(),
        BTreeMap::new(),
        None,
        &GatewayCancellationToken::new(),
    ))
    .unwrap();
    let queries = client.requests.lock().unwrap();
    assert_eq!(queries[0].fragments[0].applied_index, 38);
    assert_eq!(queries[0].fragments[0].transaction_time, 43);
}

struct ThreadWake;

impl Wake for ThreadWake {
    fn wake(self: Arc<Self>) {
        std::thread::current().unpark();
    }
}

fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::from(Arc::new(ThreadWake));
    let mut context = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => std::thread::park(),
        }
    }
}

fn planning_context() -> PlanningContext {
    planning_context_with_fence(29, 17, 23)
}

fn two_shard_planning_context() -> PlanningContext {
    let first = planning_context().catalog().shards()[0].clone();
    let second_binding = first
        .binding()
        .clone()
        .to_builder()
        .shard_id(14)
        .replica_id(20)
        .namespace_id("gateway-wire-fixture-second-shard")
        .build()
        .unwrap();
    let second = CatalogShard::new(second_binding, 37);
    PlanningContext::new(
        CatalogSnapshot::new(Version::new(29), Version::new(31), vec![first, second]).unwrap(),
        planning_context().capabilities().clone(),
        SnapshotRequirements::fixed(TransactionTime::new(41).unwrap(), 43),
        Some(128),
    )
    .unwrap()
}

fn planning_context_with_fence(
    catalog_version: u64,
    placement_epoch: u64,
    backend_generation: u64,
) -> PlanningContext {
    let capabilities =
        CapabilityManifest::from_names(dtg_plan::EXACT_VERTEX_SCAN_CAPABILITIES).unwrap();
    let class = BackendClass::new(
        ProviderKind::Fjall,
        1,
        1,
        capabilities.names().map(str::to_owned),
    )
    .unwrap();
    let binding = ReplicaBinding::builder()
        .cluster_id(7)
        .graph_id(1)
        .shard_id(13)
        .placement_epoch(placement_epoch)
        .replica_id(19)
        .backend_generation(backend_generation)
        .backend_class_digest(class.digest())
        .provider_kind(ProviderKind::Fjall)
        .contract_version(1)
        .layout_version(1)
        .capability_digest(capabilities.digest())
        .namespace_id(format!(
            "gateway-wire-fixture-{placement_epoch}-{backend_generation}"
        ))
        .endpoint_profile_ref("fixture-endpoint")
        .credential_ref("fixture-credential")
        .role(BindingRole::Active)
        .build()
        .unwrap();
    PlanningContext::new(
        CatalogSnapshot::new(
            Version::new(catalog_version),
            Version::new(31),
            vec![CatalogShard::new(binding, 37)],
        )
        .unwrap(),
        capabilities,
        SnapshotRequirements::fixed(TransactionTime::new(41).unwrap(), 43),
        Some(128),
    )
    .unwrap()
}

#[test]
fn process_catalog_install_atomically_replaces_the_planning_fence() {
    let client = Arc::new(RecordingProtocolClient::default());
    let transport = Arc::new(GatewayProtocolV2Transport::new(client.clone()));
    let execution = GatewayExecution::for_process(transport, planning_context());

    assert!(
        execution
            .install_planning_context(planning_context_with_fence(30, 18, 24))
            .unwrap()
    );

    let deadline = u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
        + 5_000;
    block_on(execution.execute_statement(
        GatewayRequestContext::new(7, 81, deadline, Vec::new()).unwrap(),
        "MATCH (n) RETURN n.id".into(),
        BTreeMap::new(),
        None,
        &GatewayCancellationToken::new(),
    ))
    .unwrap();

    let requests = client.requests.lock().unwrap();
    let context = requests[0].fragments[0].context.as_ref().unwrap();
    assert_eq!(context.catalog_version, 30);
    assert_eq!(context.placement_epoch, 18);
    assert_eq!(context.backend_generation, 24);
}

#[test]
fn single_fragment_query_prefers_the_session_and_falls_back_when_unavailable() {
    for (client, expect_session) in [
        (Arc::new(SessionRecordingProtocolClient::available()), true),
        (
            Arc::new(SessionRecordingProtocolClient::unavailable()),
            false,
        ),
    ] {
        let execution = GatewayExecution::for_process(
            Arc::new(GatewayProtocolV2Transport::new(client.clone())),
            planning_context(),
        );
        let response = block_on(execution.execute_statement(
            GatewayRequestContext::new(7, 0x7654, u64::MAX, Vec::new()).unwrap(),
            "MATCH (n) RETURN n.id".into(),
            BTreeMap::new(),
            None,
            &GatewayCancellationToken::new(),
        ));

        assert_eq!(
            response.unwrap(),
            GatewayResponse::Rows(
                GatewayRows::new(vec!["n.id".into()], vec![vec![GatewayValue::Integer(1)]])
                    .unwrap()
            )
        );
        assert_eq!(client.session_requests.lock().unwrap().len(), 1);
        assert_eq!(
            client.direct_requests.lock().unwrap().len(),
            usize::from(!expect_session)
        );
        for detail in [
            RequestDetail::GatewayQueryResponseCollect,
            RequestDetail::GatewayQueryResponseDecode,
        ] {
            assert_eq!(
                execution
                    .request_metrics()
                    .snapshot()
                    .details()
                    .find_map(|(observed, snapshot)| (observed == detail).then_some(snapshot))
                    .unwrap()
                    .success,
                1,
                "{detail:?} must remain separately observable for session and fallback queries"
            );
        }
    }
}

#[test]
fn routed_single_fragment_query_preserves_transport_metrics() {
    let client = Arc::new(SessionRecordingProtocolClient::pipeline_available());
    let transport = Arc::new(ShardRoutedGatewayTransport::new(
        Arc::new(GatewayProtocolV2Transport::new(client.clone())),
        BTreeMap::new(),
    ));
    let execution = GatewayExecution::for_process(transport, planning_context());

    let response = block_on(execution.execute_statement(
        GatewayRequestContext::new(7, 0x7655, u64::MAX, Vec::new()).unwrap(),
        "MATCH (n) WHERE n.id = $id RETURN n.id".into(),
        BTreeMap::from([("id".into(), GatewayValue::Integer(2048))]),
        None,
        &GatewayCancellationToken::new(),
    ));

    assert!(matches!(response, Ok(GatewayResponse::Rows(_))));
    assert_eq!(client.pipeline_requests.lock().unwrap().len(), 1);
    assert_eq!(client.session_requests.lock().unwrap().len(), 0);
    for detail in [
        RequestDetail::GatewayQueryRequestEncode,
        RequestDetail::GatewayQueryResponseCollect,
        RequestDetail::GatewayQueryResponseDecode,
        RequestDetail::GatewayQueryPipelineSubmit,
        RequestDetail::GatewayQueryPipelineResponseWait,
    ] {
        assert_eq!(
            execution
                .request_metrics()
                .snapshot()
                .details()
                .find_map(|(observed, snapshot)| (observed == detail).then_some(snapshot))
                .unwrap()
                .success,
            1,
            "{detail:?} must remain observable through shard routing"
        );
    }
}

#[test]
fn process_catalog_install_rejects_revision_regression_and_conflict() {
    let execution = GatewayExecution::for_process(
        Arc::new(GatewayProtocolV2Transport::new(Arc::new(
            RecordingProtocolClient::default(),
        ))),
        planning_context(),
    );

    let regression = execution
        .install_planning_context(planning_context_with_fence(28, 18, 24))
        .unwrap_err();
    assert_eq!(
        regression.code(),
        "DTG-EXECUTION-CATALOG-REVISION-REGRESSION"
    );

    let conflict = execution
        .install_planning_context(planning_context_with_fence(29, 18, 24))
        .unwrap_err();
    assert_eq!(conflict.code(), "DTG-EXECUTION-CATALOG-REVISION-CONFLICT");
}

#[test]
fn process_catalog_install_rejects_epoch_and_generation_regression() {
    let execution = GatewayExecution::for_process(
        Arc::new(GatewayProtocolV2Transport::new(Arc::new(
            RecordingProtocolClient::default(),
        ))),
        planning_context_with_fence(29, 17, 23),
    );

    let epoch = execution
        .install_planning_context(planning_context_with_fence(30, 16, 24))
        .unwrap_err();
    assert_eq!(epoch.code(), "DTG-EXECUTION-CATALOG-EPOCH-REGRESSION");

    let generation = execution
        .install_planning_context(planning_context_with_fence(30, 18, 22))
        .unwrap_err();
    assert_eq!(
        generation.code(),
        "DTG-EXECUTION-CATALOG-GENERATION-REGRESSION"
    );
}

#[test]
fn query_wire_carries_versioned_physical_fragments_and_every_planning_fence() {
    let client = Arc::new(RecordingProtocolClient::default());
    let transport = Arc::new(GatewayProtocolV2Transport::new(client.clone()));
    let execution = GatewayExecution::for_process(transport, planning_context());
    let deadline = u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
        + 5_000;
    let context = GatewayRequestContext::new(7, 8, deadline, Vec::new()).unwrap();
    let source = "MATCH (n) RETURN n.id";

    block_on(execution.execute_statement(
        context,
        source.into(),
        BTreeMap::new(),
        None,
        &GatewayCancellationToken::new(),
    ))
    .unwrap();

    let requests = client.requests.lock().unwrap();
    let request = &requests[0];
    assert_eq!(request.fragments.len(), 1);
    let fragment = &request.fragments[0];
    let shard = fragment.context.as_ref().unwrap();
    assert_eq!(shard.graph_id, 1);
    assert_eq!(shard.shard_id, 13);
    assert_eq!(shard.placement_epoch, 17);
    assert_eq!(shard.backend_generation, 23);
    assert_eq!(shard.catalog_version, 29);
    assert_eq!(fragment.schema_version, 31);
    assert_eq!(
        fragment.capability_digest,
        planning_context().capability_digest().get()
    );
    assert_eq!(fragment.applied_index, 37);
    assert_eq!(fragment.transaction_time, 41);
    assert_eq!(fragment.valid_at, 43);
    assert!(fragment.snapshot_immutable);
    let payload = fragment.payload.as_ref().unwrap();
    assert_eq!(payload.format_version, 1);
    assert!(
        !payload
            .body
            .windows(source.len())
            .any(|window| window == source.as_bytes())
    );
}

#[test]
fn process_write_without_transaction_transport_fails_closed() {
    let client = Arc::new(RecordingProtocolClient::default());
    let transport = Arc::new(GatewayProtocolV2Transport::new(client.clone()));
    let execution = GatewayExecution::for_process(transport, planning_context());
    let deadline = u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
        + 5_000;
    let context = GatewayRequestContext::new(7, 9, deadline, vec![1, 2]).unwrap();
    let error = block_on(execution.execute_statement(
        context,
        "CREATE (n {id: $id}) VALID FROM 40".into(),
        BTreeMap::from([("id".into(), GatewayValue::Integer(1))]),
        None,
        &GatewayCancellationToken::new(),
    ))
    .unwrap_err();

    assert_eq!(error.code(), "DTG-EXECUTION-WRITE-TRANSPORT");
    let requests = client.requests.lock().unwrap();
    assert!(requests.is_empty());
}

#[test]
fn process_execution_decodes_protocol_v2_typed_rows() {
    let client = Arc::new(RecordingProtocolClient::default());
    let transport = Arc::new(GatewayProtocolV2Transport::new(client));
    let execution = GatewayExecution::for_process(transport, planning_context());
    let deadline = u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
        + 5_000;
    let context = GatewayRequestContext::new(7, 10, deadline, Vec::new()).unwrap();
    let response = block_on(execution.execute_statement(
        context,
        "MATCH (n) RETURN n.id".into(),
        BTreeMap::new(),
        None,
        &GatewayCancellationToken::new(),
    ))
    .unwrap();

    assert_eq!(
        response,
        GatewayResponse::Rows(
            dtg_execution::GatewayRows::new(
                vec!["n.id".into()],
                vec![
                    vec![GatewayValue::Integer(1)],
                    vec![GatewayValue::Integer(2)],
                ],
            )
            .unwrap()
        )
    );
}

#[test]
fn process_executes_remote_point_query_at_gateway() {
    let client = Arc::new(RecordingProtocolClient::with_fixture(
        ProtocolFixture::RawPoint,
    ));
    let execution = GatewayExecution::for_process(
        Arc::new(GatewayProtocolV2Transport::new(client)),
        planning_context(),
    );
    let deadline = u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
        + 5_000;

    let response = block_on(execution.execute_statement(
        GatewayRequestContext::new(7, 101, deadline, Vec::new()).unwrap(),
        "MATCH (n) WHERE n.id = $id RETURN n.id".into(),
        BTreeMap::from([("id".into(), GatewayValue::Integer(2048))]),
        None,
        &GatewayCancellationToken::new(),
    ))
    .unwrap();

    let GatewayResponse::Rows(point) = response else {
        panic!("expected point rows")
    };
    assert_eq!(point.fields(), &["n.id"]);
    assert_eq!(point.rows(), &[vec![GatewayValue::Integer(2048)]]);

    let metrics = execution.request_metrics().snapshot();
    for stage in [
        RequestStage::GatewayCompile,
        RequestStage::GatewayPlan,
        RequestStage::GatewayInternalRpc,
        RequestStage::GatewayLocalExecution,
    ] {
        let stage = metrics.stage(stage);
        assert_eq!(stage.success, 1);
        assert_eq!(stage.error, 0);
        assert_eq!(stage.cancelled, 0);
    }
}

#[test]
fn invalid_process_query_records_compile_error() {
    let execution = GatewayExecution::for_process(
        Arc::new(CapturingClusterTransport::default()),
        planning_context(),
    );

    let error = block_on(execution.execute_statement(
        GatewayRequestContext::new(7, 103, u64::MAX, Vec::new()).unwrap(),
        "MATCH".into(),
        BTreeMap::new(),
        None,
        &GatewayCancellationToken::new(),
    ))
    .unwrap_err();
    assert!(!error.code().is_empty());

    let compile = execution
        .request_metrics()
        .snapshot()
        .stage(RequestStage::GatewayCompile);
    assert_eq!(compile.success, 0);
    assert_eq!(compile.error, 1);
    assert_eq!(compile.cancelled, 0);
}

#[test]
fn dropped_process_query_future_records_internal_rpc_cancellation() {
    let execution =
        GatewayExecution::for_process(Arc::new(PendingClusterTransport), planning_context());
    let cancellation = GatewayCancellationToken::new();
    let mut future = execution.execute_statement(
        GatewayRequestContext::new(7, 104, u64::MAX, Vec::new()).unwrap(),
        "MATCH (n) RETURN n.id".into(),
        BTreeMap::new(),
        None,
        &cancellation,
    );
    let waker = Waker::from(Arc::new(ThreadWake));
    let mut context = Context::from_waker(&waker);
    assert!(matches!(future.as_mut().poll(&mut context), Poll::Pending));
    drop(future);

    let rpc = execution
        .request_metrics()
        .snapshot()
        .stage(RequestStage::GatewayInternalRpc);
    assert_eq!(rpc.success, 0);
    assert_eq!(rpc.error, 0);
    assert_eq!(rpc.cancelled, 1);
}

#[test]
fn process_merges_scalar_vertex_counts_from_data_fragments() {
    let client = Arc::new(RecordingProtocolClient::with_fixture(
        ProtocolFixture::PartialVertexCount,
    ));
    let execution = GatewayExecution::for_process(
        Arc::new(GatewayProtocolV2Transport::new(client)),
        two_shard_planning_context(),
    );
    let deadline = u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
        + 5_000;

    let response = block_on(execution.execute_statement(
        GatewayRequestContext::new(7, 105, deadline, Vec::new()).unwrap(),
        "MATCH (n) RETURN COUNT(*)".into(),
        BTreeMap::new(),
        None,
        &GatewayCancellationToken::new(),
    ))
    .unwrap();

    let GatewayResponse::Rows(count) = response else {
        panic!("expected count rows")
    };
    assert_eq!(count.fields(), &["COUNT(*)"]);
    assert_eq!(count.rows(), &[vec![GatewayValue::Integer(4096)]]);
    assert_eq!(
        execution
            .request_metrics()
            .snapshot()
            .stage(RequestStage::GatewayLocalExecution)
            .success,
        0
    );
}

#[test]
fn process_routes_each_fenced_fragment_to_its_data_transport() {
    let default = Arc::new(MaterializingClusterTransport::default());
    let secondary = Arc::new(MaterializingClusterTransport::default());
    let execution = GatewayExecution::for_process(
        Arc::new(ShardRoutedGatewayTransport::new(
            default.clone(),
            BTreeMap::from([(14, secondary.clone() as Arc<dyn GatewayExecutionTransport>)]),
        )),
        two_shard_planning_context(),
    );

    let error = block_on(execution.execute_statement(
        GatewayRequestContext::new(7, 106, u64::MAX, Vec::new()).unwrap(),
        "MATCH (n) RETURN COUNT(*)".into(),
        BTreeMap::new(),
        None,
        &GatewayCancellationToken::new(),
    ))
    .unwrap_err();

    assert_eq!(error.code(), "DTG-EXECUTION-PARTIAL-COUNT");
    assert_eq!(default.requests.lock().unwrap().len(), 1);
    assert_eq!(secondary.requests.lock().unwrap().len(), 1);
    assert_eq!(
        default.requests.lock().unwrap()[0]
            .physical_plan()
            .unwrap()
            .fragments()[0]
            .fence()
            .shard_id()
            .get(),
        13
    );
    assert_eq!(
        secondary.requests.lock().unwrap()[0]
            .physical_plan()
            .unwrap()
            .fragments()[0]
            .fence()
            .shard_id()
            .get(),
        14
    );
}

#[tokio::test]
async fn shard_routed_fragments_execute_concurrently() {
    let delay = Duration::from_millis(150);
    let default = Arc::new(DelayedClusterTransport::new(delay));
    let secondary = Arc::new(DelayedClusterTransport::new(delay));
    let execution = GatewayExecution::for_process(
        Arc::new(ShardRoutedGatewayTransport::new(
            default.clone(),
            BTreeMap::from([(14, secondary.clone() as Arc<dyn GatewayExecutionTransport>)]),
        )),
        two_shard_planning_context(),
    );

    let started = std::time::Instant::now();
    let response = execution
        .execute_statement(
            GatewayRequestContext::new(7, 107, u64::MAX, Vec::new()).unwrap(),
            "MATCH (n) RETURN n.id".into(),
            BTreeMap::new(),
            None,
            &GatewayCancellationToken::new(),
        )
        .await
        .unwrap();

    let GatewayResponse::Rows(rows) = response else {
        panic!("expected rows response")
    };
    assert_eq!(rows.fields(), &["n.id"]);
    assert!(rows.rows().is_empty());
    assert!(started.elapsed() < Duration::from_millis(240));
    assert_eq!(default.query_calls.load(Ordering::SeqCst), 1);
    assert_eq!(secondary.query_calls.load(Ordering::SeqCst), 1);
}

#[test]
fn shard_routed_transport_rejects_fragment_from_wrong_owner() {
    let default = Arc::new(FixedFragmentClusterTransport {
        response_fragment_id: 2,
    });
    let secondary = Arc::new(FixedFragmentClusterTransport {
        response_fragment_id: 1,
    });
    let execution = GatewayExecution::for_process(
        Arc::new(ShardRoutedGatewayTransport::new(
            default,
            BTreeMap::from([(14, secondary as Arc<dyn GatewayExecutionTransport>)]),
        )),
        two_shard_planning_context(),
    );

    let error = block_on(execution.execute_statement(
        GatewayRequestContext::new(7, 108, u64::MAX, Vec::new()).unwrap(),
        "MATCH (n) RETURN n.id".into(),
        BTreeMap::new(),
        None,
        &GatewayCancellationToken::new(),
    ))
    .unwrap_err();

    assert_eq!(error.code(), "DTG-CLUSTER-ROUTING");
    assert_eq!(error.retry(), dtg_execution::GatewayRetry::Safe);
}

#[test]
fn process_rejects_unknown_remote_fragment() {
    assert_remote_fragment_error(
        ProtocolFixture::UnknownFragment,
        planning_context(),
        "DTG-EXECUTION-QUERY",
    );
}

#[test]
fn process_rejects_malformed_remote_fragment_id() {
    assert_remote_fragment_error(
        ProtocolFixture::MalformedFragment,
        planning_context(),
        "DTG-PROTOCOL-ROW-CODEC",
    );
}

#[test]
fn process_rejects_duplicate_remote_fragment_batch() {
    assert_remote_fragment_error(
        ProtocolFixture::DuplicateBatch,
        planning_context(),
        "DTG-PROTOCOL-ROW-CODEC",
    );
}

#[test]
fn process_rejects_missing_remote_fragment() {
    assert_remote_fragment_error(
        ProtocolFixture::MissingFragment,
        two_shard_planning_context(),
        "DTG-EXECUTION-QUERY",
    );
}

fn assert_remote_fragment_error(
    fixture: ProtocolFixture,
    planning_context: PlanningContext,
    expected_code: &str,
) {
    let client = Arc::new(RecordingProtocolClient::with_fixture(fixture));
    let execution = GatewayExecution::for_process(
        Arc::new(GatewayProtocolV2Transport::new(client)),
        planning_context,
    );
    let deadline = u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
        + 5_000;
    let error = block_on(execution.execute_statement(
        GatewayRequestContext::new(7, 103, deadline, Vec::new()).unwrap(),
        "MATCH (n) RETURN n.id".into(),
        BTreeMap::new(),
        None,
        &GatewayCancellationToken::new(),
    ))
    .unwrap_err();
    assert_eq!(error.code(), expected_code);
}

#[test]
fn binds_parameters_recursively_in_expression_shapes_and_values() {
    let expression = LogicalExpr::Map(vec![
        (
            "binary".into(),
            LogicalExpr::Binary {
                left: Box::new(LogicalExpr::Parameter("id".into())),
                operator: BinaryOperator::Equal,
                right: Box::new(LogicalExpr::Literal(Value::Integer(2048))),
            },
        ),
        (
            "list".into(),
            LogicalExpr::List(vec![
                LogicalExpr::Parameter("null".into()),
                LogicalExpr::Parameter("boolean".into()),
                LogicalExpr::Parameter("float".into()),
                LogicalExpr::Parameter("bytes".into()),
                LogicalExpr::Parameter("string".into()),
            ]),
        ),
        (
            "unary".into(),
            LogicalExpr::Unary {
                operator: UnaryOperator::Negate,
                input: Box::new(LogicalExpr::Parameter("integer".into())),
            },
        ),
        (
            "projection".into(),
            LogicalExpr::Parameter("projection".into()),
        ),
        (
            "aggregate".into(),
            LogicalExpr::Parameter("aggregate".into()),
        ),
        ("sort".into(), LogicalExpr::Parameter("sort".into())),
        ("unwind".into(), LogicalExpr::Parameter("unwind".into())),
        ("nested".into(), LogicalExpr::Parameter("nested".into())),
    ]);
    let parameters = BTreeMap::from([
        ("id".into(), GatewayValue::Integer(2048)),
        ("null".into(), GatewayValue::Null),
        ("boolean".into(), GatewayValue::Boolean(true)),
        ("float".into(), GatewayValue::FloatBits(17)),
        ("bytes".into(), GatewayValue::Bytes(vec![1, 2])),
        ("string".into(), GatewayValue::String("value".into())),
        ("integer".into(), GatewayValue::Integer(9)),
        ("projection".into(), GatewayValue::Integer(1)),
        ("aggregate".into(), GatewayValue::Integer(2)),
        ("sort".into(), GatewayValue::Integer(3)),
        (
            "unwind".into(),
            GatewayValue::List(vec![GatewayValue::Integer(4)]),
        ),
        (
            "nested".into(),
            GatewayValue::Map(BTreeMap::from([(
                "items".into(),
                GatewayValue::List(vec![GatewayValue::String("nested".into())]),
            )])),
        ),
    ]);

    let bound = GatewayExecution::bind_logical_expr(&expression, &parameters).unwrap();

    let LogicalExpr::Map(values) = bound else {
        panic!("expected bound map expression")
    };
    let LogicalExpr::Binary { left, .. } = &values[0].1 else {
        panic!("expected binary expression")
    };
    assert_eq!(left.as_ref(), &LogicalExpr::Literal(Value::Integer(2048)));
    assert!(format!("{values:?}").find("Parameter").is_none());
    assert_eq!(
        values.last().unwrap().1,
        LogicalExpr::Literal(Value::Map(BTreeMap::from([(
            "items".into(),
            Value::List(vec![Value::String("nested".into())]),
        )])))
    );
}

#[test]
fn plans_a_literal_node_id_predicate_as_a_vertex_point() {
    let client = Arc::new(RecordingProtocolClient::default());
    let execution = GatewayExecution::for_process(
        Arc::new(GatewayProtocolV2Transport::new(client)),
        planning_context(),
    );
    let program = execution
        .compile("MATCH (n) WHERE n.id = 2048 RETURN n.id")
        .unwrap();
    let physical = Planner.plan(&program, &planning_context()).unwrap();
    let access = &physical.fragments()[0].storage_accesses()[0];
    let point_id = match access {
        dtg_plan::StorageAccess::Logical(request) => {
            let dtg_plan::LogicalReadOperation::VertexPoint(id) = request.operation() else {
                panic!("exact node identity must use a logical vertex point read")
            };
            id.get()
        }
        dtg_plan::StorageAccess::Pushdown { request, .. } => {
            let dtg_storage::PushdownOperation::Vertex(read) = request.operation() else {
                panic!("exact node identity must use a pushed-down vertex point read")
            };
            read.id().get()
        }
    };
    assert_eq!(point_id, 2048);
    assert!(!physical.operators().iter().any(|operator| matches!(
        operator.kind(),
        dtg_plan::PhysicalOperatorKind::Filter { .. }
    )));
}

#[test]
fn plans_a_point_anchored_one_hop_pattern_as_an_adjacency_read() {
    let client = Arc::new(RecordingProtocolClient::default());
    let execution = GatewayExecution::for_process(
        Arc::new(GatewayProtocolV2Transport::new(client)),
        planning_context(),
    );
    let program = execution
        .compile("MATCH (a)-[r]->(b) WHERE a.id = 2048 RETURN r")
        .unwrap();
    let physical = Planner.plan(&program, &planning_context()).unwrap();

    for fragment in physical.fragments() {
        let [StorageAccess::Logical(read)] = fragment.storage_accesses() else {
            panic!("point-anchored one-hop pattern must have one logical adjacency access")
        };
        assert!(matches!(
            read.operation(),
            dtg_plan::LogicalReadOperation::Adjacency { vertex_id, direction }
                if vertex_id.get() == 2048
                    && *direction == dtg_language_ir::ExpandDirection::Outgoing
        ));
    }
    assert!(physical.operators().iter().any(|operator| matches!(
        operator.kind(),
        dtg_plan::PhysicalOperatorKind::Source { output, .. } if output == "r"
    )));
}

#[test]
fn binds_parameters_in_every_non_filter_physical_operator_position() {
    let client = Arc::new(RecordingProtocolClient::default());
    let execution = GatewayExecution::for_process(
        Arc::new(GatewayProtocolV2Transport::new(client)),
        planning_context(),
    );
    let parameter = |name: &str| LogicalExpr::Parameter(name.into());
    let program = LogicalProgram {
        version: dtg_language_ir::IrVersion::CURRENT,
        graph_scope: GraphScope::Explicit(dtg_storage::GraphId::new(1).unwrap()),
        parameters: ["project", "aggregate", "sort", "unwind"]
            .into_iter()
            .map(|name| Parameter {
                name: name.into(),
                data_type: LogicalType::Any,
                required: true,
            })
            .collect(),
        statement: LogicalStatement::Query(LogicalPlan {
            root: LogicalNodeId::new(5),
            nodes: vec![
                LogicalNode {
                    id: LogicalNodeId::new(1),
                    kind: LogicalNodeKind::NodeScan(NodeScan {
                        variable: "n".into(),
                        labels: Vec::new(),
                        read_scope: ReadScope::current(),
                    }),
                },
                LogicalNode {
                    id: LogicalNodeId::new(2),
                    kind: LogicalNodeKind::Project {
                        input: LogicalNodeId::new(1),
                        projections: vec![Projection {
                            expression: parameter("project"),
                            alias: "projected".into(),
                        }],
                    },
                },
                LogicalNode {
                    id: LogicalNodeId::new(3),
                    kind: LogicalNodeKind::Aggregate(Aggregate {
                        input: LogicalNodeId::new(2),
                        groups: Vec::new(),
                        aggregates: vec![AggregateFunction {
                            function: AggregateKind::Count,
                            argument: Some(parameter("aggregate")),
                            alias: "counted".into(),
                            distinct: false,
                        }],
                    }),
                },
                LogicalNode {
                    id: LogicalNodeId::new(4),
                    kind: LogicalNodeKind::Sort(Sort {
                        input: LogicalNodeId::new(3),
                        keys: vec![SortKey {
                            expression: parameter("sort"),
                            direction: SortDirection::Descending,
                        }],
                    }),
                },
                LogicalNode {
                    id: LogicalNodeId::new(5),
                    kind: LogicalNodeKind::Unwind(Unwind {
                        input: LogicalNodeId::new(4),
                        expression: parameter("unwind"),
                        alias: "item".into(),
                    }),
                },
            ],
        }),
        result_schema: RowSchema {
            fields: vec![Field {
                name: "item".into(),
                data_type: LogicalType::Any,
                nullable: true,
            }],
        },
    };
    let physical = Planner.plan(&program, &planning_context()).unwrap();

    let executable = execution
        .lower_plan_with_parameters(
            &physical,
            &BTreeMap::from([
                ("project".into(), GatewayValue::Integer(11)),
                ("aggregate".into(), GatewayValue::Integer(12)),
                ("sort".into(), GatewayValue::Integer(13)),
                (
                    "unwind".into(),
                    GatewayValue::List(vec![GatewayValue::Integer(14)]),
                ),
            ]),
        )
        .unwrap();

    let project = executable
        .operators()
        .iter()
        .find_map(|operator| match operator.kind() {
            dtg_query::ExecutableOperatorKind::Project { projections, .. } => Some(projections),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        project[0].expression().logical(),
        &LogicalExpr::Literal(Value::Integer(11))
    );

    let aggregate = executable
        .operators()
        .iter()
        .find_map(|operator| match operator.kind() {
            dtg_query::ExecutableOperatorKind::Aggregate { aggregates, .. } => Some(aggregates),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        aggregate[0].argument.as_ref().unwrap().logical(),
        &LogicalExpr::Literal(Value::Integer(12))
    );

    let sort = executable
        .operators()
        .iter()
        .find_map(|operator| match operator.kind() {
            dtg_query::ExecutableOperatorKind::Sort { keys, .. } => Some(keys),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        sort[0].expression.logical(),
        &LogicalExpr::Literal(Value::Integer(13))
    );

    let unwind = executable
        .operators()
        .iter()
        .find_map(|operator| match operator.kind() {
            dtg_query::ExecutableOperatorKind::Unwind { expression, .. } => Some(expression),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        unwind.logical(),
        &LogicalExpr::Literal(Value::List(vec![Value::Integer(14)]))
    );
}

#[test]
fn binds_missing_names_before_remote_success() {
    let client = Arc::new(RecordingProtocolClient::default());
    let execution = GatewayExecution::for_process(
        Arc::new(GatewayProtocolV2Transport::new(client.clone())),
        planning_context(),
    );
    let deadline = u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
        + 5_000;

    let error = block_on(execution.execute_statement(
        GatewayRequestContext::new(7, 12, deadline, Vec::new()).unwrap(),
        "MATCH (n) WHERE n.id = $id RETURN n.id".into(),
        BTreeMap::new(),
        None,
        &GatewayCancellationToken::new(),
    ))
    .unwrap_err();

    assert_eq!(error.code(), "DTG-EXECUTION-MISSING-PARAMETER");
    assert!(client.requests.lock().unwrap().is_empty());
}

#[test]
fn binds_missing_sort_parameter_before_remote_success() {
    let client = Arc::new(RecordingProtocolClient::default());
    let execution = GatewayExecution::for_process(
        Arc::new(GatewayProtocolV2Transport::new(client.clone())),
        planning_context(),
    );
    let deadline = u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
        + 5_000;

    let error = block_on(execution.execute_statement(
        GatewayRequestContext::new(7, 13, deadline, Vec::new()).unwrap(),
        "MATCH (n) RETURN n.id ORDER BY $sort".into(),
        BTreeMap::new(),
        None,
        &GatewayCancellationToken::new(),
    ))
    .unwrap_err();

    assert_eq!(error.code(), "DTG-EXECUTION-MISSING-PARAMETER");
    assert!(client.requests.lock().unwrap().is_empty());
}

#[test]
fn process_execution_decodes_protocol_v2_transaction_boundaries() {
    let client = Arc::new(RecordingProtocolClient::default());
    let transport = Arc::new(GatewayProtocolV2Transport::new(client));
    let execution = GatewayExecution::for_process(transport, planning_context());
    let deadline = u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
        + 5_000;
    let context = GatewayRequestContext::new(7, 11, deadline, Vec::new()).unwrap();
    let response = block_on(execution.execute_statement(
        context,
        "BEGIN".into(),
        BTreeMap::new(),
        None,
        &GatewayCancellationToken::new(),
    ))
    .unwrap();

    assert_eq!(
        response,
        GatewayResponse::Transaction { transaction_id: 23 }
    );
}

fn encoded_batch(fragment_id: u128, body: Vec<u8>, row_count: u32) -> proto::ColumnBatch {
    proto::ColumnBatch {
        request: None,
        fragment_id: fragment_id.to_be_bytes().to_vec(),
        sequence: 1,
        row_count,
        payload: Some(proto::BoundedPayload {
            format_version: 1,
            declared_len: body.len() as u64,
            item_count: row_count,
            checksum: checksum_bytes(&body).to_vec(),
            body,
        }),
    }
}

fn encoded_vertex_rows(ids: impl IntoIterator<Item = i64>) -> Vec<u8> {
    let ids = ids.into_iter().collect::<Vec<_>>();
    let mut body = Vec::new();
    body.extend_from_slice(&1_u32.to_be_bytes());
    body.extend_from_slice(&5_u32.to_be_bytes());
    body.extend_from_slice(b"value");
    body.extend_from_slice(&u32::try_from(ids.len()).unwrap().to_be_bytes());
    for id in ids {
        body.push(7);
        body.extend_from_slice(&2_u32.to_be_bytes());
        body.extend_from_slice(&2_u32.to_be_bytes());
        body.extend_from_slice(b"id");
        body.push(2);
        body.extend_from_slice(&id.to_be_bytes());
        body.extend_from_slice(&10_u32.to_be_bytes());
        body.extend_from_slice(b"properties");
        body.push(7);
        body.extend_from_slice(&0_u32.to_be_bytes());
    }
    body
}

fn encoded_integer_rows(field: &str, value: i64) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&1_u32.to_be_bytes());
    body.extend_from_slice(&u32::try_from(field.len()).unwrap().to_be_bytes());
    body.extend_from_slice(field.as_bytes());
    body.extend_from_slice(&1_u32.to_be_bytes());
    body.push(2);
    body.extend_from_slice(&value.to_be_bytes());
    body
}
