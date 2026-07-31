use std::collections::BTreeMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::time::{SystemTime, UNIX_EPOCH};

use dtg_cluster_v2::{checksum_bytes, proto};
use dtg_execution::{
    GatewayCancellationToken, GatewayExecution, GatewayExecutionError, GatewayFuture,
    GatewayProtocolV2Client, GatewayProtocolV2Transport, GatewayRequestContext, GatewayResponse,
    GatewayValue,
};
use dtg_language_ir::{BinaryOperator, LogicalExpr, UnaryOperator, Value};
use dtg_plan::{CatalogShard, CatalogSnapshot, Planner, PlanningContext, SnapshotRequirements};
use dtg_storage::{
    BackendClass, BindingRole, CapabilityManifest, ProviderKind, ReplicaBinding, TransactionTime,
    Version,
};

#[derive(Default)]
struct RecordingProtocolClient {
    requests: Mutex<Vec<proto::GatewayRequest>>,
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
        let batch = (request.execution_request.as_ref().unwrap().body[0] == 1).then(|| {
            let body = encoded_rows();
            proto::ColumnBatch {
                request: request.request.clone(),
                fragment_id: 1_u128.to_be_bytes().to_vec(),
                sequence: 1,
                row_count: 2,
                payload: Some(proto::BoundedPayload {
                    format_version: 1,
                    declared_len: body.len() as u64,
                    item_count: 2,
                    checksum: checksum_bytes(&body).to_vec(),
                    body,
                }),
            }
        });
        self.requests.lock().unwrap().push(request);
        Box::pin(async move {
            Ok(vec![proto::GatewayResponse {
                status: Some(status),
                batch,
            }])
        })
    }
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
    let source = "MATCH (n) RETURN n.id ORDER BY n.id";

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
fn process_execution_encodes_normalized_requests_on_protocol_v2() {
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
    let response = block_on(execution.execute_statement(
        context,
        "CREATE (n {id: $id}) VALID FROM 40".into(),
        BTreeMap::from([("id".into(), GatewayValue::Integer(1))]),
        None,
        &GatewayCancellationToken::new(),
    ))
    .unwrap();

    assert_eq!(response, GatewayResponse::Acknowledged);
    let requests = client.requests.lock().unwrap();
    let request = &requests[0];
    assert_eq!(request.request.as_ref().unwrap().protocol_major, 2);
    let payload = request.execution_request.as_ref().unwrap();
    assert_eq!(payload.format_version, 1);
    assert_eq!(payload.declared_len as usize, payload.body.len());
    assert_eq!(payload.checksum, checksum_bytes(&payload.body));
    assert_eq!(payload.body[0], 2);
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
        "MATCH (n) RETURN n.id ORDER BY n.id".into(),
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
fn binds_parameters_while_lowering_the_fixed_point_predicate() {
    let client = Arc::new(RecordingProtocolClient::default());
    let execution = GatewayExecution::for_process(
        Arc::new(GatewayProtocolV2Transport::new(client)),
        planning_context(),
    );
    let program = execution
        .compile("MATCH (n) WHERE n.id = $id RETURN n.id")
        .unwrap();
    let physical = Planner.plan(&program, &planning_context()).unwrap();

    let executable = execution
        .lower_plan_with_parameters(
            &physical,
            &BTreeMap::from([("id".into(), GatewayValue::Integer(2048))]),
        )
        .unwrap();

    let predicate = executable
        .operators()
        .iter()
        .find_map(|operator| match operator.kind() {
            dtg_query::ExecutableOperatorKind::Filter { predicate, .. } => Some(predicate),
            _ => None,
        })
        .unwrap();
    let LogicalExpr::Binary { right, .. } = predicate.logical() else {
        panic!("expected binary predicate")
    };
    assert_eq!(right.as_ref(), &LogicalExpr::Literal(Value::Integer(2048)));
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

fn encoded_rows() -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&1_u32.to_be_bytes());
    body.extend_from_slice(&4_u32.to_be_bytes());
    body.extend_from_slice(b"n.id");
    body.extend_from_slice(&2_u32.to_be_bytes());
    body.push(2);
    body.extend_from_slice(&1_i64.to_be_bytes());
    body.push(2);
    body.extend_from_slice(&2_i64.to_be_bytes());
    body
}
