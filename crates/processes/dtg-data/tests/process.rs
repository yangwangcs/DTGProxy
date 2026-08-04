use std::sync::Arc;
use std::time::Instant;
use std::{collections::BTreeMap, ffi::OsString};

use dtg_data::{
    CredentialProfile, DataNodeBuilder, DataNodeError, DataProcessConfig, DataRpcService,
    EndpointProfile, LifecycleState, RemoteResolver,
};
use dtg_execution::cluster_protocol::PROTOCOL_MAJOR;
use dtg_execution::cluster_protocol::checksum_bytes;
use dtg_execution::cluster_protocol::proto::data_service_server::DataService;
use dtg_execution::cluster_protocol::proto::gateway_service_server::GatewayService as ClusterGatewayService;
use dtg_execution::cluster_protocol::proto::{
    BoundedPayload, ExecutionFragment, GatewayRequest, LogicalReplicaSnapshot, RaftEnvelope,
    RaftMessageKind, RequestContext, ShardContext, SnapshotIngestBatch, SnapshotIngestItem,
    SnapshotIngestReceiptRequest, SnapshotIngestState, StatusCode, TransactionOperation,
    TransactionRequest,
};
use dtg_execution::planning::{
    CatalogShard, CatalogSnapshot, Planner, PlanningContext, SnapshotRequirements, StorageAccess,
};
use dtg_execution::shard::{CommitSingleShard, CommitSingleShardTransaction, ShardCommand};
use dtg_execution::storage::{
    BackendClass, BindingRole, CapabilityManifest, CommandId, EdgeId, EdgeVersion, LogicalMutation,
    Properties, ReplicaBinding, StorageTckFactory, TransactionTime, ValidInterval, Version,
    VertexId, VertexVersion,
};
use dtg_execution::{
    GatewayCancellationToken, GatewayExecution, GatewayExecutionError, GatewayFuture,
    GatewayProtocolV2Client, GatewayProtocolV2Transport, GatewayRequestContext, GatewayResponse,
    GatewayRetry, ProviderKind, RequestDetail, RequestStage, encode_physical_fragment_body,
};
use dtg_language_ir::{
    Expand, ExpandDirection, Field, GraphScope, LogicalExpr, LogicalNode, LogicalNodeId,
    LogicalNodeKind, LogicalPlan, LogicalProgram, LogicalStatement, LogicalType, ReadScope,
    RowSchema, TemporalScope, TimeExpr, Value, VertexLookup,
};
use dtg_storage_fjall::FjallStorageTckFactory;
use dtg_storage_remote::{ReferenceServerConfig, ReferenceStorageServer};
use prost_011::Message as _;
use tokio_stream::StreamExt;
use tonic::{Code, Request};

fn fjall_binding(namespace: &str) -> ReplicaBinding {
    let capabilities = fjall_capabilities();
    fjall_binding_with_capabilities(namespace, &capabilities)
}

fn binding_for_provider(provider_kind: ProviderKind, namespace: &str) -> ReplicaBinding {
    let capabilities = fjall_capabilities();
    let class = BackendClass::new(
        provider_kind.clone(),
        1,
        1,
        capabilities.names().map(str::to_owned),
    )
    .unwrap();
    ReplicaBinding::builder()
        .cluster_id(7)
        .graph_id(11)
        .shard_id(13)
        .placement_epoch(17)
        .replica_id(19)
        .backend_generation(23)
        .backend_class_digest(class.digest())
        .provider_kind(provider_kind)
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

fn fjall_capabilities() -> CapabilityManifest {
    CapabilityManifest::from_names([
        "adjacency",
        "immutable-read-view",
        "logical-snapshot",
        "point",
    ])
    .unwrap()
}

fn fjall_binding_with_capabilities(
    namespace: &str,
    capabilities: &CapabilityManifest,
) -> ReplicaBinding {
    let class = BackendClass::new(
        ProviderKind::Fjall,
        1,
        1,
        capabilities.names().map(str::to_owned),
    )
    .unwrap();
    ReplicaBinding::builder()
        .cluster_id(7)
        .graph_id(11)
        .shard_id(13)
        .placement_epoch(17)
        .replica_id(19)
        .backend_generation(23)
        .backend_class_digest(class.digest())
        .provider_kind(ProviderKind::Fjall)
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

fn planning_context(
    binding: ReplicaBinding,
    capabilities: CapabilityManifest,
    applied_index: u64,
) -> PlanningContext {
    PlanningContext::new(
        CatalogSnapshot::new(
            Version::new(29),
            Version::new(31),
            vec![CatalogShard::new(binding, applied_index)],
        )
        .unwrap(),
        capabilities,
        SnapshotRequirements::fixed(TransactionTime::new(41).unwrap(), 10),
        Some(128),
    )
    .unwrap()
}

fn logical_vertex_point_body(
    binding: &ReplicaBinding,
    capabilities: &CapabilityManifest,
    applied_index: u64,
) -> Vec<u8> {
    let physical = Planner
        .plan(
            &LogicalProgram {
                version: dtg_language_ir::IrVersion::CURRENT,
                graph_scope: GraphScope::Explicit(binding.graph_id()),
                parameters: Vec::new(),
                statement: LogicalStatement::Query(LogicalPlan {
                    root: LogicalNodeId::new(1),
                    nodes: vec![LogicalNode {
                        id: LogicalNodeId::new(1),
                        kind: LogicalNodeKind::VertexLookup(VertexLookup {
                            variable: "n".into(),
                            id: LogicalExpr::Literal(Value::Integer(37)),
                            labels: Vec::new(),
                            read_scope: ReadScope::current(),
                        }),
                    }],
                }),
                result_schema: RowSchema {
                    fields: vec![Field {
                        name: "n".into(),
                        data_type: LogicalType::Vertex,
                        nullable: false,
                    }],
                },
            },
            &planning_context(binding.clone(), capabilities.clone(), applied_index),
        )
        .unwrap();
    let [StorageAccess::Logical(read)] = physical.fragments()[0].storage_accesses() else {
        panic!("test requires a logical vertex-point access")
    };
    assert_eq!(read.row_bound(), 1);
    encode_physical_fragment_body(&physical, &physical.fragments()[0])
}

fn logical_adjacency_body(
    binding: &ReplicaBinding,
    capabilities: &CapabilityManifest,
    applied_index: u64,
) -> Vec<u8> {
    logical_adjacency_body_with_scope(binding, capabilities, applied_index, ReadScope::current())
}

fn logical_adjacency_body_with_scope(
    binding: &ReplicaBinding,
    capabilities: &CapabilityManifest,
    applied_index: u64,
    read_scope: ReadScope,
) -> Vec<u8> {
    let physical = Planner
        .plan(
            &LogicalProgram {
                version: dtg_language_ir::IrVersion::CURRENT,
                graph_scope: GraphScope::Explicit(binding.graph_id()),
                parameters: Vec::new(),
                statement: LogicalStatement::Query(LogicalPlan {
                    root: LogicalNodeId::new(2),
                    nodes: vec![
                        LogicalNode {
                            id: LogicalNodeId::new(1),
                            kind: LogicalNodeKind::VertexLookup(VertexLookup {
                                variable: "source".into(),
                                id: LogicalExpr::Literal(Value::Integer(41)),
                                labels: Vec::new(),
                                read_scope: read_scope.clone(),
                            }),
                        },
                        LogicalNode {
                            id: LogicalNodeId::new(2),
                            kind: LogicalNodeKind::Expand(Expand {
                                input: LogicalNodeId::new(1),
                                source: "source".into(),
                                relationship: "relationship".into(),
                                destination: "destination".into(),
                                destination_labels: Vec::new(),
                                direction: ExpandDirection::Outgoing,
                                relationship_types: Vec::new(),
                                read_scope,
                            }),
                        },
                    ],
                }),
                result_schema: RowSchema {
                    fields: vec![Field {
                        name: "relationship".into(),
                        data_type: LogicalType::Relationship,
                        nullable: false,
                    }],
                },
            },
            &planning_context(binding.clone(), capabilities.clone(), applied_index),
        )
        .unwrap();
    let [StorageAccess::Logical(read)] = physical.fragments()[0].storage_accesses() else {
        panic!("test requires a logical adjacency access")
    };
    assert_eq!(read.node(), LogicalNodeId::new(2));
    encode_physical_fragment_body(&physical, &physical.fragments()[0])
}

fn logical_two_hop_traversal_body(
    binding: &ReplicaBinding,
    capabilities: &CapabilityManifest,
    applied_index: u64,
) -> Vec<u8> {
    let physical = Planner
        .plan(
            &LogicalProgram {
                version: dtg_language_ir::IrVersion::CURRENT,
                graph_scope: GraphScope::Explicit(binding.graph_id()),
                parameters: Vec::new(),
                statement: LogicalStatement::Query(LogicalPlan {
                    root: LogicalNodeId::new(3),
                    nodes: vec![
                        LogicalNode {
                            id: LogicalNodeId::new(1),
                            kind: LogicalNodeKind::VertexLookup(VertexLookup {
                                variable: "source".into(),
                                id: LogicalExpr::Literal(Value::Integer(41)),
                                labels: Vec::new(),
                                read_scope: ReadScope::current(),
                            }),
                        },
                        LogicalNode {
                            id: LogicalNodeId::new(2),
                            kind: LogicalNodeKind::Expand(Expand {
                                input: LogicalNodeId::new(1),
                                source: "source".into(),
                                relationship: "first".into(),
                                destination: "middle".into(),
                                destination_labels: Vec::new(),
                                direction: ExpandDirection::Outgoing,
                                relationship_types: Vec::new(),
                                read_scope: ReadScope::current(),
                            }),
                        },
                        LogicalNode {
                            id: LogicalNodeId::new(3),
                            kind: LogicalNodeKind::Expand(Expand {
                                input: LogicalNodeId::new(2),
                                source: "middle".into(),
                                relationship: "second".into(),
                                destination: "destination".into(),
                                destination_labels: Vec::new(),
                                direction: ExpandDirection::Outgoing,
                                relationship_types: Vec::new(),
                                read_scope: ReadScope::current(),
                            }),
                        },
                    ],
                }),
                result_schema: RowSchema {
                    fields: vec![Field {
                        name: "second".into(),
                        data_type: LogicalType::Relationship,
                        nullable: false,
                    }],
                },
            },
            &planning_context(binding.clone(), capabilities.clone(), applied_index),
        )
        .unwrap();
    let [StorageAccess::Logical(read)] = physical.fragments()[0].storage_accesses() else {
        panic!("test requires a logical traversal access")
    };
    assert_eq!(read.node(), LogicalNodeId::new(3));
    encode_physical_fragment_body(&physical, &physical.fragments()[0])
}

#[derive(Clone)]
struct InProcessDataGatewayClient {
    service: DataRpcService,
}

impl GatewayProtocolV2Client for InProcessDataGatewayClient {
    fn execute(
        &self,
        request: GatewayRequest,
    ) -> GatewayFuture<
        '_,
        Result<Vec<dtg_execution::cluster_protocol::proto::GatewayResponse>, GatewayExecutionError>,
    > {
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

#[tokio::test]
async fn environment_bootstrap_assignments_start_a_fenced_replica() {
    let root = tempfile::tempdir().unwrap();
    let values = BTreeMap::from([
        ("DTG_DATA_BACKEND_KIND", OsString::from("fjall")),
        (
            "DTG_DATA_FJALL_ROOT",
            root.path().join("business").into_os_string(),
        ),
        (
            "DTG_DATA_CONSENSUS_ROOT",
            root.path().join("raft").into_os_string(),
        ),
        ("DTG_DATA_RPC_ADDR", OsString::from("127.0.0.1:0")),
        (
            "DTG_DATA_CAPABILITIES",
            OsString::from("adjacency,immutable-read-view,logical-snapshot,point"),
        ),
        (
            "DTG_DATA_ASSIGNMENTS",
            OsString::from("7:11:13:17:19:23:fjall:1:1:environment-bootstrap"),
        ),
    ]);
    let config = DataProcessConfig::from_environment(|name| values.get(name).cloned()).unwrap();
    let node = DataNodeBuilder::from_config(config).start().await.unwrap();

    let observed = node.observed_replicas().await;
    assert_eq!(observed.len(), 1);
    assert_eq!(observed[0].cluster_id().get(), 7);
    assert_eq!(observed[0].graph_id().get(), 11);
    assert_eq!(observed[0].shard_id().get(), 13);
    assert_eq!(observed[0].provider_kind(), &ProviderKind::Fjall);
    assert_eq!(observed[0].namespace_id().as_str(), "environment-bootstrap");
}

fn shard_context(binding: &ReplicaBinding) -> ShardContext {
    ShardContext {
        request: Some(RequestContext {
            protocol_major: PROTOCOL_MAJOR,
            protocol_minor: 1,
            cluster_id: binding.cluster_id().get().to_be_bytes().to_vec(),
            request_id: 29_u128.to_be_bytes().to_vec(),
            deadline_unix_ms: u64::MAX,
            trace_context: Vec::new(),
        }),
        graph_id: binding.graph_id().get(),
        shard_id: u32::try_from(binding.shard_id().get()).unwrap(),
        placement_epoch: binding.placement_epoch().get(),
        backend_generation: binding.backend_generation().get(),
        catalog_version: 31,
    }
}

#[tokio::test]
async fn process_composes_only_the_selected_official_provider_and_v2_lifecycle_metrics() {
    let root = tempfile::tempdir().unwrap();
    let config = DataProcessConfig::new(root.path().join("business"), root.path().join("raft"))
        .with_endpoint_profile(
            "postgres-primary",
            EndpointProfile::PostgreSql("host=127.0.0.1 port=5432 dbname=dtg".into()),
        )
        .with_credential_profile(
            "postgres-primary",
            CredentialProfile::PostgreSql("user=dtg password=secret".into()),
        )
        .with_kuzu_root(root.path().join("kuzu"));
    let node = DataNodeBuilder::from_config(config).start().await.unwrap();

    assert_eq!(node.provider_kinds(), vec![ProviderKind::Fjall]);
    assert_eq!(node.rpc_service().protocol_major(), PROTOCOL_MAJOR);
    assert_eq!(node.lifecycle(), LifecycleState::Ready);
    assert_eq!(node.metrics().hosted_replicas(), 0);
    assert_eq!(node.metrics().failed_replicas(), 0);

    node.begin_draining();
    assert_eq!(node.lifecycle(), LifecycleState::Draining);
    node.stop();
    assert_eq!(node.lifecycle(), LifecycleState::Stopped);
}

#[test]
fn production_config_exposes_only_its_selected_provider() {
    let root = tempfile::tempdir().unwrap();
    let configs = [
        (
            DataProcessConfig::new(root.path().join("fjall"), root.path().join("fjall-raft"))
                .assign(binding_for_provider(ProviderKind::Fjall, "fjall-shard")),
            ProviderKind::Fjall,
        ),
        (
            DataProcessConfig::new(
                root.path().join("postgres"),
                root.path().join("postgres-raft"),
            )
            .with_backend_kind(ProviderKind::PostgreSql)
            .with_endpoint_profile(
                "local",
                EndpointProfile::PostgreSql("host=127.0.0.1 port=5432 dbname=dtg".into()),
            )
            .with_credential_profile(
                "local",
                CredentialProfile::PostgreSql("user=dtg password=secret".into()),
            )
            .assign(binding_for_provider(
                ProviderKind::PostgreSql,
                "postgres-shard",
            )),
            ProviderKind::PostgreSql,
        ),
        (
            DataProcessConfig::new(root.path().join("kuzu"), root.path().join("kuzu-raft"))
                .with_backend_kind(ProviderKind::Kuzu)
                .with_kuzu_root(root.path().join("kuzu-business"))
                .assign(binding_for_provider(ProviderKind::Kuzu, "kuzu-shard")),
            ProviderKind::Kuzu,
        ),
    ];

    for (config, expected) in configs {
        assert_eq!(
            DataNodeBuilder::from_config(config).provider_kinds(),
            vec![expected]
        );
    }
}

#[tokio::test]
async fn production_config_rejects_mismatched_assignment_before_opening_namespace() {
    let root = tempfile::tempdir().unwrap();
    let business_root = root.path().join("business");
    let consensus_root = root.path().join("raft");
    let config = DataProcessConfig::new(business_root.clone(), consensus_root.clone())
        .assign(binding_for_provider(ProviderKind::Kuzu, "must-not-open"));

    let result = DataNodeBuilder::from_config(config).start().await;

    assert!(matches!(result, Err(DataNodeError::Build(message))
        if message.contains("does not match configured backend")));
    assert!(!business_root.exists());
    assert!(!consensus_root.exists());
}

#[tokio::test]
async fn data_node_background_driver_ticks_running_replicas() {
    let root = tempfile::tempdir().unwrap();
    let binding = fjall_binding("background-tick");
    let node = DataNodeBuilder::from_config(
        DataProcessConfig::new(root.path().join("business"), root.path().join("raft"))
            .assign(binding),
    )
    .start()
    .await
    .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    assert!(node.metrics().raft_ticks() > 0);
}

#[tokio::test]
async fn v2_rpc_rejects_malformed_requests_and_records_metrics() {
    let root = tempfile::tempdir().unwrap();
    let node = DataNodeBuilder::from_config(DataProcessConfig::new(
        root.path().join("business"),
        root.path().join("raft"),
    ))
    .start()
    .await
    .unwrap();
    let service = node.rpc_service();

    let error = service
        .send_raft(Request::new(RaftEnvelope::default()))
        .await
        .unwrap_err();

    assert_eq!(error.code(), Code::InvalidArgument);
    assert_eq!(service.metrics().rpc_requests(), 1);
    assert_eq!(service.metrics().rpc_failures(), 1);
    let validation = service
        .request_metrics()
        .snapshot()
        .stage(RequestStage::DataValidation);
    assert_eq!(validation.success, 0);
    assert_eq!(validation.error, 1);
}

#[tokio::test]
async fn apply_transaction_proposes_the_typed_shard_command() {
    let root = tempfile::tempdir().unwrap();
    let binding = fjall_binding("rpc-transaction");
    let node = DataNodeBuilder::from_config(
        DataProcessConfig::new(root.path().join("business"), root.path().join("raft"))
            .assign(binding.clone()),
    )
    .start()
    .await
    .unwrap();
    let vertex = VertexVersion::new(
        VertexId::new(37).unwrap(),
        Version::new(1),
        ValidInterval::new(1, 100).unwrap(),
        TransactionTime::new(41).unwrap(),
        Properties::new(),
    )
    .unwrap();
    let command = ShardCommand::CommitSingleShard(
        CommitSingleShard::new(
            CommandId::new(43).unwrap(),
            binding.placement_epoch().get(),
            binding.backend_generation().get(),
            vec![LogicalMutation::PutVertex(vertex)],
        )
        .unwrap(),
    );
    let body = command.encode_current().unwrap();
    let response = node
        .rpc_service()
        .apply_transaction(Request::new(TransactionRequest {
            context: Some(shard_context(&binding)),
            transaction_id: 47_u128.to_be_bytes().to_vec(),
            operation: TransactionOperation::Commit.into(),
            idempotency_key: 43_u128.to_be_bytes().to_vec(),
            payload: Some(BoundedPayload {
                format_version: 1,
                declared_len: body.len() as u64,
                item_count: 1,
                checksum: checksum_bytes(&body).to_vec(),
                body,
            }),
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(response.code, StatusCode::Ok as i32);
    assert!(node.replica_observations().await[0].applied_index() >= 2);

    let metrics = node.request_metrics().snapshot();
    for stage in [
        RequestStage::DataValidation,
        RequestStage::DataRouting,
        RequestStage::DataRaftApply,
    ] {
        let stage = metrics.stage(stage);
        assert_eq!(stage.success, 1);
        assert_eq!(stage.error, 0);
        assert_eq!(stage.cancelled, 0);
    }
    for detail in [
        RequestDetail::DataRaftBatchAdmission,
        RequestDetail::DataRaftBatchQueue,
        RequestDetail::DataRaftBlockingDispatch,
    ] {
        let snapshot = metrics
            .details()
            .find_map(|(recorded, snapshot)| (recorded == detail).then_some(snapshot))
            .unwrap();
        assert_eq!(snapshot.success, 1);
    }
}

fn transaction_request(
    binding: &ReplicaBinding,
    command_id: u128,
    vertex_id: u128,
) -> TransactionRequest {
    let vertex = VertexVersion::new(
        VertexId::new(vertex_id).unwrap(),
        Version::new(1),
        ValidInterval::new(1, 100).unwrap(),
        TransactionTime::new(41).unwrap(),
        Properties::new(),
    )
    .unwrap();
    let command = ShardCommand::CommitSingleShard(
        CommitSingleShard::new(
            CommandId::new(command_id).unwrap(),
            binding.placement_epoch().get(),
            binding.backend_generation().get(),
            vec![LogicalMutation::PutVertex(vertex)],
        )
        .unwrap(),
    );
    let body = command.encode_current().unwrap();
    TransactionRequest {
        context: Some(shard_context(binding)),
        transaction_id: command_id.to_be_bytes().to_vec(),
        operation: TransactionOperation::Commit.into(),
        idempotency_key: command_id.to_be_bytes().to_vec(),
        payload: Some(BoundedPayload {
            format_version: 1,
            declared_len: body.len() as u64,
            item_count: 1,
            checksum: checksum_bytes(&body).to_vec(),
            body,
        }),
    }
}

fn snapshot_transaction_request(
    binding: &ReplicaBinding,
    command_id: u128,
    vertex_id: u128,
) -> TransactionRequest {
    let vertex = VertexVersion::new(
        VertexId::new(vertex_id).unwrap(),
        Version::new(1),
        ValidInterval::new(1, 100).unwrap(),
        TransactionTime::new(42).unwrap(),
        Properties::new(),
    )
    .unwrap();
    let command = ShardCommand::CommitSingleShardTransaction(
        CommitSingleShardTransaction::new(
            CommandId::new(command_id).unwrap(),
            binding.placement_epoch().get(),
            binding.backend_generation().get(),
            dtg_execution::storage::TransactionId::new(command_id).unwrap(),
            TransactionTime::new(41).unwrap(),
            1,
            dtg_execution::storage::Digest32::new([7; 32]),
            vec![LogicalMutation::PutVertex(vertex)],
        )
        .unwrap(),
    );
    let body = command.encode_current().unwrap();
    TransactionRequest {
        context: Some(shard_context(binding)),
        transaction_id: command_id.to_be_bytes().to_vec(),
        operation: TransactionOperation::CommitSnapshot.into(),
        idempotency_key: command_id.to_be_bytes().to_vec(),
        payload: Some(BoundedPayload {
            format_version: 1,
            declared_len: body.len() as u64,
            item_count: 1,
            checksum: checksum_bytes(&body).to_vec(),
            body,
        }),
    }
}

#[tokio::test]
async fn snapshot_ingest_acknowledges_once_and_exposes_its_committed_receipt() {
    let root = tempfile::tempdir().unwrap();
    let binding = fjall_binding("snapshot-ingest-receipt");
    let node = DataNodeBuilder::from_config(
        DataProcessConfig::new(root.path().join("business"), root.path().join("raft"))
            .assign(binding.clone()),
    )
    .start()
    .await
    .unwrap();
    let service = node.rpc_service();
    let receipt_id = 911_u128;
    let batch = || SnapshotIngestBatch {
        request: Some(shard_context(&binding).request.unwrap()),
        items: vec![SnapshotIngestItem {
            receipt_id: receipt_id.to_be_bytes().to_vec(),
            transaction: Some(snapshot_transaction_request(&binding, 912, 913)),
        }],
    };

    let mut first = service
        .accept_snapshot_ingest(Request::new(batch()))
        .await
        .unwrap()
        .into_inner();
    let accepted = first.next().await.unwrap().unwrap();
    assert_eq!(accepted.state, SnapshotIngestState::Pending as i32);
    assert_eq!(accepted.receipt_id, receipt_id.to_be_bytes());

    let mut duplicate = service
        .accept_snapshot_ingest(Request::new(batch()))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        duplicate.next().await.unwrap().unwrap().state,
        SnapshotIngestState::Pending as i32
    );

    let committed = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let receipt = service
                .get_snapshot_ingest_receipt(Request::new(SnapshotIngestReceiptRequest {
                    request: Some(shard_context(&binding).request.unwrap()),
                    receipt_id: receipt_id.to_be_bytes().to_vec(),
                }))
                .await
                .unwrap()
                .into_inner()
                .receipt
                .unwrap();
            if receipt.state != SnapshotIngestState::Pending as i32 {
                break receipt;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("ingest receipt should reach a terminal state");
    assert_eq!(committed.state, SnapshotIngestState::Committed as i32);
    assert!(committed.applied_index > 1);
    assert!(committed.commit_time > 0);
}

#[tokio::test]
async fn snapshot_ingest_rejects_a_reused_receipt_with_different_content() {
    let root = tempfile::tempdir().unwrap();
    let binding = fjall_binding("snapshot-ingest-digest");
    let node = DataNodeBuilder::from_config(
        DataProcessConfig::new(root.path().join("business"), root.path().join("raft"))
            .assign(binding.clone()),
    )
    .start()
    .await
    .unwrap();
    let service = node.rpc_service();
    let batch = |vertex_id| SnapshotIngestBatch {
        request: Some(shard_context(&binding).request.unwrap()),
        items: vec![SnapshotIngestItem {
            receipt_id: 914_u128.to_be_bytes().to_vec(),
            transaction: Some(snapshot_transaction_request(&binding, 915, vertex_id)),
        }],
    };

    let _ = service
        .accept_snapshot_ingest(Request::new(batch(916)))
        .await
        .unwrap();
    let result = service
        .accept_snapshot_ingest(Request::new(batch(917)))
        .await;
    let Err(error) = result else {
        panic!("reused receipt must reject different content");
    };
    assert_eq!(error.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn snapshot_ingest_rejects_a_non_snapshot_transaction_before_admission() {
    let root = tempfile::tempdir().unwrap();
    let binding = fjall_binding("snapshot-ingest-operation");
    let node = DataNodeBuilder::from_config(
        DataProcessConfig::new(root.path().join("business"), root.path().join("raft"))
            .assign(binding.clone()),
    )
    .start()
    .await
    .unwrap();
    let result = node
        .rpc_service()
        .accept_snapshot_ingest(Request::new(SnapshotIngestBatch {
            request: Some(shard_context(&binding).request.unwrap()),
            items: vec![SnapshotIngestItem {
                receipt_id: 918_u128.to_be_bytes().to_vec(),
                transaction: Some(transaction_request(&binding, 919, 920)),
            }],
        }))
        .await;
    let Err(error) = result else {
        panic!("ingest must reject non-snapshot transactions");
    };
    assert_eq!(error.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn snapshot_commit_replay_returns_the_original_commit_time() {
    let root = tempfile::tempdir().unwrap();
    let binding = fjall_binding("snapshot-commit-replay");
    let node = DataNodeBuilder::from_config(
        DataProcessConfig::new(root.path().join("business"), root.path().join("raft"))
            .assign(binding.clone()),
    )
    .start()
    .await
    .unwrap();
    let request = snapshot_transaction_request(&binding, 921, 922);

    let first = node
        .rpc_service()
        .apply_transaction(Request::new(request.clone()))
        .await
        .unwrap()
        .into_inner();
    let second = node
        .rpc_service()
        .apply_transaction(Request::new(request))
        .await
        .unwrap()
        .into_inner();
    let first_receipt = first.details.unwrap().body;
    let second_receipt = second.details.unwrap().body;

    assert_eq!(first_receipt.len(), 17);
    assert_eq!(second_receipt.len(), 17);
    assert_eq!(&first_receipt[9..], &second_receipt[9..]);
    assert_eq!(second_receipt[8], 1);
}

#[tokio::test]
#[ignore = "development-host diagnostic; run explicitly to measure Data ingress only"]
async fn diagnostic_snapshot_ingest_admission_latency() {
    const WARMUP_SAMPLES: u128 = 128;
    const MEASURED_SAMPLES: u128 = 1_024;

    let root = tempfile::tempdir().unwrap();
    let binding = fjall_binding("snapshot-ingest-admission-diagnostic");
    let node = DataNodeBuilder::from_config(
        DataProcessConfig::new(root.path().join("business"), root.path().join("raft"))
            .assign(binding.clone()),
    )
    .start()
    .await
    .unwrap();
    let service = node.rpc_service();
    let submit = |ordinal: u128| {
        let service = service.clone();
        let binding = binding.clone();
        async move {
            let request = Request::new(SnapshotIngestBatch {
                request: Some(shard_context(&binding).request.unwrap()),
                items: vec![SnapshotIngestItem {
                    receipt_id: ordinal.to_be_bytes().to_vec(),
                    transaction: Some(snapshot_transaction_request(
                        &binding,
                        ordinal + 10_000,
                        ordinal + 20_000,
                    )),
                }],
            });
            let started = Instant::now();
            let mut response = service
                .accept_snapshot_ingest(request)
                .await
                .unwrap()
                .into_inner();
            let receipt = response.next().await.unwrap().unwrap();
            assert!(matches!(
                receipt.state,
                state if state == SnapshotIngestState::Pending as i32
                    || state == SnapshotIngestState::Committed as i32
            ));
            started.elapsed().as_nanos() as u64
        }
    };

    for ordinal in 1..=WARMUP_SAMPLES {
        let _ = submit(ordinal).await;
    }
    let mut samples = Vec::with_capacity(MEASURED_SAMPLES as usize);
    for ordinal in WARMUP_SAMPLES + 1..=WARMUP_SAMPLES + MEASURED_SAMPLES {
        samples.push(submit(ordinal).await);
    }
    samples.sort_unstable();
    let percentile = |percentile: usize| samples[(samples.len() * percentile).div_ceil(100) - 1];
    println!(
        "snapshot_ingest_admission samples={} p50_ns={} p95_ns={} p99_ns={}",
        samples.len(),
        percentile(50),
        percentile(95),
        percentile(99),
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_same_shard_writes_share_one_raft_ready_drive() {
    let root = tempfile::tempdir().unwrap();
    let binding = fjall_binding("rpc-transaction-batch");
    let node = DataNodeBuilder::from_config(
        DataProcessConfig::new(root.path().join("business"), root.path().join("raft"))
            .assign(binding.clone()),
    )
    .start()
    .await
    .unwrap();
    let service = node.rpc_service();
    let first = transaction_request(&binding, 143, 137);
    let second = transaction_request(&binding, 144, 138);

    let (first, second) = tokio::join!(
        service.apply_transaction(Request::new(first)),
        service.apply_transaction(Request::new(second)),
    );

    assert_eq!(first.unwrap().into_inner().code, StatusCode::Ok as i32);
    assert_eq!(second.unwrap().into_inner().code, StatusCode::Ok as i32);
    assert!(node.replica_observations().await[0].applied_index() >= 3);
    let drive_ready = node
        .request_metrics()
        .snapshot()
        .details()
        .find_map(|(detail, snapshot)| {
            (detail == RequestDetail::DataRaftDriveReady).then_some(snapshot)
        })
        .unwrap();
    assert_eq!(drive_ready.success, 1);
}

#[tokio::test]
async fn execute_fragment_rejects_invalid_transaction_time_before_provider_timing() {
    let root = tempfile::tempdir().unwrap();
    let binding = fjall_binding("rpc-invalid-fragment-time");
    let node = DataNodeBuilder::from_config(
        DataProcessConfig::new(root.path().join("business"), root.path().join("raft"))
            .assign(binding.clone()),
    )
    .start()
    .await
    .unwrap();
    let body = scan_fragment_body();

    let result = node
        .rpc_service()
        .execute_fragment(Request::new(ExecutionFragment {
            context: Some(shard_context(&binding)),
            fragment_id: 74_u128.to_be_bytes().to_vec(),
            payload: Some(BoundedPayload {
                format_version: 1,
                declared_len: body.len() as u64,
                item_count: 2,
                checksum: checksum_bytes(&body).to_vec(),
                body,
            }),
            schema_version: 31,
            capability_digest: binding.capability_digest().get().to_vec(),
            applied_index: 1,
            transaction_time: -1,
            valid_at: 10,
            snapshot_immutable: true,
        }))
        .await;
    let error = match result {
        Ok(_) => panic!("invalid transaction time produced a fragment response"),
        Err(error) => error,
    };

    assert_eq!(error.code(), Code::InvalidArgument);
    let provider = node
        .request_metrics()
        .snapshot()
        .stage(RequestStage::DataProviderExecution);
    assert_eq!(provider.success, 0);
    assert_eq!(provider.error, 0);
    assert_eq!(provider.cancelled, 0);
}

#[tokio::test]
async fn single_shard_transaction_is_applied_once_with_durable_metadata() {
    let root = tempfile::tempdir().unwrap();
    let binding = fjall_binding("rpc-single-shard-transaction");
    let node = DataNodeBuilder::from_config(
        DataProcessConfig::new(root.path().join("business"), root.path().join("raft"))
            .assign(binding.clone()),
    )
    .start()
    .await
    .unwrap();
    let vertex = VertexVersion::new(
        VertexId::new(71).unwrap(),
        Version::new(1),
        ValidInterval::new(1, i64::MAX).unwrap(),
        TransactionTime::new(43).unwrap(),
        Properties::new(),
    )
    .unwrap();
    let command = ShardCommand::CommitSingleShardTransaction(
        CommitSingleShardTransaction::new(
            CommandId::new(73).unwrap(),
            binding.placement_epoch().get(),
            binding.backend_generation().get(),
            dtg_execution::storage::TransactionId::new(79).unwrap(),
            TransactionTime::new(41).unwrap(),
            1,
            dtg_execution::storage::Digest32::new([83; 32]),
            vec![LogicalMutation::PutVertex(vertex)],
        )
        .unwrap(),
    );
    let body = command.encode_current().unwrap();
    let request = || TransactionRequest {
        context: Some(shard_context(&binding)),
        transaction_id: 79_u128.to_be_bytes().to_vec(),
        operation: TransactionOperation::Commit.into(),
        idempotency_key: 73_u128.to_be_bytes().to_vec(),
        payload: Some(BoundedPayload {
            format_version: 1,
            declared_len: body.len() as u64,
            item_count: 1,
            checksum: checksum_bytes(&body).to_vec(),
            body: body.clone(),
        }),
    };
    let status = node
        .rpc_service()
        .apply_transaction(Request::new(request()))
        .await
        .unwrap()
        .into_inner();
    let applied = node.replica_observations().await[0].applied_index();
    assert!(applied > 1);
    let details = status.details.unwrap().body;
    assert_eq!(
        u64::from_be_bytes(details[..8].try_into().unwrap()),
        applied
    );
    assert_eq!(details[8], 0);

    let mut stream = node
        .rpc_service()
        .execute_fragment(Request::new(ExecutionFragment {
            context: Some(shard_context(&binding)),
            fragment_id: 89_u128.to_be_bytes().to_vec(),
            payload: Some(BoundedPayload {
                format_version: 1,
                declared_len: scan_fragment_body().len() as u64,
                item_count: 2,
                checksum: checksum_bytes(&scan_fragment_body()).to_vec(),
                body: scan_fragment_body(),
            }),
            schema_version: 31,
            capability_digest: binding.capability_digest().get().to_vec(),
            applied_index: applied,
            transaction_time: 43,
            valid_at: 1,
            snapshot_immutable: true,
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(stream.next().await.unwrap().unwrap().row_count, 1);

    let replay = node
        .rpc_service()
        .apply_transaction(Request::new(request()))
        .await
        .unwrap()
        .into_inner();
    let replayed_applied = node.replica_observations().await[0].applied_index();
    assert!(replayed_applied > applied);
    let details = replay.details.unwrap().body;
    assert_eq!(
        u64::from_be_bytes(details[..8].try_into().unwrap()),
        replayed_applied
    );
    assert_eq!(details[8], 1);
}

#[tokio::test]
async fn execute_fragment_reads_the_fenced_replica_store() {
    let root = tempfile::tempdir().unwrap();
    let binding = fjall_binding("rpc-fragment");
    let node = DataNodeBuilder::from_config(
        DataProcessConfig::new(root.path().join("business"), root.path().join("raft"))
            .assign(binding.clone()),
    )
    .start()
    .await
    .unwrap();
    let vertex = VertexVersion::new(
        VertexId::new(37).unwrap(),
        Version::new(1),
        ValidInterval::new(1, 100).unwrap(),
        TransactionTime::new(41).unwrap(),
        Properties::new(),
    )
    .unwrap();
    let command = ShardCommand::CommitSingleShard(
        CommitSingleShard::new(
            CommandId::new(53).unwrap(),
            binding.placement_epoch().get(),
            binding.backend_generation().get(),
            vec![LogicalMutation::PutVertex(vertex)],
        )
        .unwrap(),
    );
    let command_body = command.encode_current().unwrap();
    node.rpc_service()
        .apply_transaction(Request::new(TransactionRequest {
            context: Some(shard_context(&binding)),
            transaction_id: 59_u128.to_be_bytes().to_vec(),
            operation: TransactionOperation::Commit.into(),
            idempotency_key: 53_u128.to_be_bytes().to_vec(),
            payload: Some(BoundedPayload {
                format_version: 1,
                declared_len: command_body.len() as u64,
                item_count: 1,
                checksum: checksum_bytes(&command_body).to_vec(),
                body: command_body,
            }),
        }))
        .await
        .unwrap();
    let applied_index = node.replica_observations().await[0].applied_index();
    let body = scan_fragment_body();
    let mut stream = node
        .rpc_service()
        .execute_fragment(Request::new(ExecutionFragment {
            context: Some(shard_context(&binding)),
            fragment_id: 61_u128.to_be_bytes().to_vec(),
            payload: Some(BoundedPayload {
                format_version: 1,
                declared_len: body.len() as u64,
                item_count: 1,
                checksum: checksum_bytes(&body).to_vec(),
                body,
            }),
            schema_version: 31,
            capability_digest: binding.capability_digest().get().to_vec(),
            applied_index,
            transaction_time: 41,
            valid_at: 10,
            snapshot_immutable: true,
        }))
        .await
        .unwrap()
        .into_inner();
    let batch = stream.next().await.unwrap().unwrap();

    assert_eq!(batch.row_count, 1);
    assert!(stream.next().await.is_none());
}

#[tokio::test]
async fn gateway_encoder_logical_vertex_point_reaches_the_data_service() {
    let root = tempfile::tempdir().unwrap();
    let capabilities = fjall_capabilities();
    let binding = fjall_binding_with_capabilities("gateway-logical-point", &capabilities);
    let node = DataNodeBuilder::from_config(
        DataProcessConfig::new(root.path().join("business"), root.path().join("raft"))
            .assign(binding.clone()),
    )
    .start()
    .await
    .unwrap();
    let vertex = VertexVersion::new(
        VertexId::new(37).unwrap(),
        Version::new(1),
        ValidInterval::new(1, 100).unwrap(),
        TransactionTime::new(41).unwrap(),
        Properties::new(),
    )
    .unwrap();
    let command = ShardCommand::CommitSingleShard(
        CommitSingleShard::new(
            CommandId::new(81).unwrap(),
            binding.placement_epoch().get(),
            binding.backend_generation().get(),
            vec![LogicalMutation::PutVertex(vertex)],
        )
        .unwrap(),
    );
    let command_body = command.encode_current().unwrap();
    node.rpc_service()
        .apply_transaction(Request::new(TransactionRequest {
            context: Some(shard_context(&binding)),
            transaction_id: 82_u128.to_be_bytes().to_vec(),
            operation: TransactionOperation::Commit.into(),
            idempotency_key: 81_u128.to_be_bytes().to_vec(),
            payload: Some(BoundedPayload {
                format_version: 1,
                declared_len: command_body.len() as u64,
                item_count: 1,
                checksum: checksum_bytes(&command_body).to_vec(),
                body: command_body,
            }),
        }))
        .await
        .unwrap();
    let applied_index = node.replica_observations().await[0].applied_index();
    let body = logical_vertex_point_body(&binding, &capabilities, applied_index);
    let mut stream = node
        .rpc_service()
        .execute_fragment(Request::new(ExecutionFragment {
            context: Some(shard_context(&binding)),
            fragment_id: 83_u128.to_be_bytes().to_vec(),
            payload: Some(BoundedPayload {
                format_version: 1,
                declared_len: body.len() as u64,
                item_count: 1,
                checksum: checksum_bytes(&body).to_vec(),
                body,
            }),
            schema_version: 31,
            capability_digest: binding.capability_digest().get().to_vec(),
            applied_index,
            transaction_time: 41,
            valid_at: 10,
            snapshot_immutable: true,
        }))
        .await
        .unwrap()
        .into_inner();
    let batch = stream.next().await.unwrap().unwrap();
    assert_eq!(batch.row_count, 1);
    assert!(stream.next().await.is_none());
}

#[tokio::test]
async fn gateway_encoder_logical_adjacency_reaches_the_data_service() {
    let root = tempfile::tempdir().unwrap();
    let capabilities = fjall_capabilities();
    let binding = fjall_binding_with_capabilities("gateway-logical-adjacency", &capabilities);
    let node = DataNodeBuilder::from_config(
        DataProcessConfig::new(root.path().join("business"), root.path().join("raft"))
            .assign(binding.clone()),
    )
    .start()
    .await
    .unwrap();
    let vertices = [41_u128, 42]
        .into_iter()
        .map(|id| {
            VertexVersion::new(
                VertexId::new(id).unwrap(),
                Version::new(1),
                ValidInterval::new(1, 100).unwrap(),
                TransactionTime::new(41).unwrap(),
                Properties::new(),
            )
            .unwrap()
        })
        .map(LogicalMutation::PutVertex);
    let edge = EdgeVersion::new(
        EdgeId::new(73).unwrap(),
        VertexId::new(41).unwrap(),
        VertexId::new(42).unwrap(),
        "KNOWS",
        Version::new(1),
        ValidInterval::new(1, 100).unwrap(),
        TransactionTime::new(41).unwrap(),
        Properties::new(),
    )
    .unwrap();
    let command = ShardCommand::CommitSingleShard(
        CommitSingleShard::new(
            CommandId::new(84).unwrap(),
            binding.placement_epoch().get(),
            binding.backend_generation().get(),
            vertices
                .chain(std::iter::once(LogicalMutation::PutEdge(edge)))
                .collect(),
        )
        .unwrap(),
    );
    let command_body = command.encode_current().unwrap();
    node.rpc_service()
        .apply_transaction(Request::new(TransactionRequest {
            context: Some(shard_context(&binding)),
            transaction_id: 84_u128.to_be_bytes().to_vec(),
            operation: TransactionOperation::Commit.into(),
            idempotency_key: 84_u128.to_be_bytes().to_vec(),
            payload: Some(BoundedPayload {
                format_version: 1,
                declared_len: command_body.len() as u64,
                item_count: 3,
                checksum: checksum_bytes(&command_body).to_vec(),
                body: command_body,
            }),
        }))
        .await
        .unwrap();
    let applied_index = node.replica_observations().await[0].applied_index();
    let body = logical_adjacency_body(&binding, &capabilities, applied_index);
    let mut stream = node
        .rpc_service()
        .execute_fragment(Request::new(ExecutionFragment {
            context: Some(shard_context(&binding)),
            fragment_id: 84_u128.to_be_bytes().to_vec(),
            payload: Some(BoundedPayload {
                format_version: 1,
                declared_len: body.len() as u64,
                item_count: 1,
                checksum: checksum_bytes(&body).to_vec(),
                body,
            }),
            schema_version: 31,
            capability_digest: binding.capability_digest().get().to_vec(),
            applied_index,
            transaction_time: 41,
            valid_at: 10,
            snapshot_immutable: true,
        }))
        .await
        .unwrap()
        .into_inner();
    let batch = stream.next().await.unwrap().unwrap();
    assert_eq!(batch.row_count, 1);
    assert!(stream.next().await.is_none());
}

#[tokio::test]
async fn gateway_encoder_logical_two_hop_traversal_reaches_the_data_service() {
    let root = tempfile::tempdir().unwrap();
    let capabilities = fjall_capabilities();
    let binding = fjall_binding_with_capabilities("gateway-logical-two-hop", &capabilities);
    let node = DataNodeBuilder::from_config(
        DataProcessConfig::new(root.path().join("business"), root.path().join("raft"))
            .assign(binding.clone()),
    )
    .start()
    .await
    .unwrap();
    let vertices = [41_u128, 42, 43]
        .into_iter()
        .map(|id| {
            VertexVersion::new(
                VertexId::new(id).unwrap(),
                Version::new(1),
                ValidInterval::new(1, 100).unwrap(),
                TransactionTime::new(41).unwrap(),
                Properties::new(),
            )
            .unwrap()
        })
        .map(LogicalMutation::PutVertex);
    let first = EdgeVersion::new(
        EdgeId::new(73).unwrap(),
        VertexId::new(41).unwrap(),
        VertexId::new(42).unwrap(),
        "KNOWS",
        Version::new(1),
        ValidInterval::new(1, 100).unwrap(),
        TransactionTime::new(41).unwrap(),
        Properties::new(),
    )
    .unwrap();
    let second = EdgeVersion::new(
        EdgeId::new(74).unwrap(),
        VertexId::new(42).unwrap(),
        VertexId::new(43).unwrap(),
        "KNOWS",
        Version::new(1),
        ValidInterval::new(1, 100).unwrap(),
        TransactionTime::new(41).unwrap(),
        Properties::new(),
    )
    .unwrap();
    let command = ShardCommand::CommitSingleShard(
        CommitSingleShard::new(
            CommandId::new(87).unwrap(),
            binding.placement_epoch().get(),
            binding.backend_generation().get(),
            vertices
                .chain([
                    LogicalMutation::PutEdge(first),
                    LogicalMutation::PutEdge(second),
                ])
                .collect(),
        )
        .unwrap(),
    );
    let command_body = command.encode_current().unwrap();
    node.rpc_service()
        .apply_transaction(Request::new(TransactionRequest {
            context: Some(shard_context(&binding)),
            transaction_id: 87_u128.to_be_bytes().to_vec(),
            operation: TransactionOperation::Commit.into(),
            idempotency_key: 87_u128.to_be_bytes().to_vec(),
            payload: Some(BoundedPayload {
                format_version: 1,
                declared_len: command_body.len() as u64,
                item_count: 5,
                checksum: checksum_bytes(&command_body).to_vec(),
                body: command_body,
            }),
        }))
        .await
        .unwrap();
    let applied_index = node.replica_observations().await[0].applied_index();
    let body = logical_two_hop_traversal_body(&binding, &capabilities, applied_index);
    let mut stream = node
        .rpc_service()
        .execute_fragment(Request::new(ExecutionFragment {
            context: Some(shard_context(&binding)),
            fragment_id: 87_u128.to_be_bytes().to_vec(),
            payload: Some(BoundedPayload {
                format_version: 1,
                declared_len: body.len() as u64,
                item_count: 1,
                checksum: checksum_bytes(&body).to_vec(),
                body,
            }),
            schema_version: 31,
            capability_digest: binding.capability_digest().get().to_vec(),
            applied_index,
            transaction_time: 41,
            valid_at: 10,
            snapshot_immutable: true,
        }))
        .await
        .unwrap()
        .into_inner();
    let batch = stream.next().await.unwrap().unwrap();
    assert_eq!(batch.row_count, 1);
    assert!(stream.next().await.is_none());
}

#[tokio::test]
async fn logical_adjacency_rejects_a_temporal_scope_that_drifts_from_the_execution_fence() {
    let root = tempfile::tempdir().unwrap();
    let capabilities = fjall_capabilities();
    let binding = fjall_binding_with_capabilities("gateway-logical-adjacency-drift", &capabilities);
    let node = DataNodeBuilder::from_config(
        DataProcessConfig::new(root.path().join("business"), root.path().join("raft"))
            .assign(binding.clone()),
    )
    .start()
    .await
    .unwrap();
    let applied_index = node.replica_observations().await[0].applied_index();
    let body = logical_adjacency_body_with_scope(
        &binding,
        &capabilities,
        applied_index,
        ReadScope {
            transaction_time: TemporalScope::AsOf(TimeExpr::Literal(
                TransactionTime::new(40).unwrap(),
            )),
            valid_time: None,
        },
    );

    let result = node
        .rpc_service()
        .execute_fragment(Request::new(ExecutionFragment {
            context: Some(shard_context(&binding)),
            fragment_id: 86_u128.to_be_bytes().to_vec(),
            payload: Some(BoundedPayload {
                format_version: 1,
                declared_len: body.len() as u64,
                item_count: 1,
                checksum: checksum_bytes(&body).to_vec(),
                body,
            }),
            schema_version: 31,
            capability_digest: binding.capability_digest().get().to_vec(),
            applied_index,
            transaction_time: 41,
            valid_at: 10,
            snapshot_immutable: true,
        }))
        .await;
    let error = match result {
        Ok(_) => panic!("temporal scope drift returned a fragment stream"),
        Err(error) => error,
    };
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(error.message().contains("scope diverges"));
}

#[tokio::test]
async fn gateway_executes_a_point_anchored_one_hop_query() {
    let root = tempfile::tempdir().unwrap();
    let capabilities = fjall_capabilities();
    let binding = fjall_binding_with_capabilities("gateway-one-hop-query", &capabilities);
    let node = DataNodeBuilder::from_config(
        DataProcessConfig::new(root.path().join("business"), root.path().join("raft"))
            .assign(binding.clone()),
    )
    .start()
    .await
    .unwrap();
    let vertices = [41_u128, 42]
        .into_iter()
        .map(|id| {
            VertexVersion::new(
                VertexId::new(id).unwrap(),
                Version::new(1),
                ValidInterval::new(1, 100).unwrap(),
                TransactionTime::new(41).unwrap(),
                Properties::new(),
            )
            .unwrap()
        })
        .map(LogicalMutation::PutVertex);
    let edge = EdgeVersion::new(
        EdgeId::new(73).unwrap(),
        VertexId::new(41).unwrap(),
        VertexId::new(42).unwrap(),
        "KNOWS",
        Version::new(1),
        ValidInterval::new(1, 100).unwrap(),
        TransactionTime::new(41).unwrap(),
        Properties::new(),
    )
    .unwrap();
    let command = ShardCommand::CommitSingleShard(
        CommitSingleShard::new(
            CommandId::new(85).unwrap(),
            binding.placement_epoch().get(),
            binding.backend_generation().get(),
            vertices
                .chain(std::iter::once(LogicalMutation::PutEdge(edge)))
                .collect(),
        )
        .unwrap(),
    );
    let command_body = command.encode_current().unwrap();
    node.rpc_service()
        .apply_transaction(Request::new(TransactionRequest {
            context: Some(shard_context(&binding)),
            transaction_id: 85_u128.to_be_bytes().to_vec(),
            operation: TransactionOperation::Commit.into(),
            idempotency_key: 85_u128.to_be_bytes().to_vec(),
            payload: Some(BoundedPayload {
                format_version: 1,
                declared_len: command_body.len() as u64,
                item_count: 3,
                checksum: checksum_bytes(&command_body).to_vec(),
                body: command_body,
            }),
        }))
        .await
        .unwrap();
    let applied_index = node.replica_observations().await[0].applied_index();
    let execution = GatewayExecution::for_process(
        Arc::new(GatewayProtocolV2Transport::new(Arc::new(
            InProcessDataGatewayClient {
                service: node.rpc_service(),
            },
        ))),
        planning_context(binding, capabilities, applied_index),
    );

    let response = execution
        .execute_statement(
            GatewayRequestContext::new(7, 85, u64::MAX, Vec::new()).unwrap(),
            "MATCH (a)-[r]->(b) WHERE a.id = $id RETURN r".into(),
            BTreeMap::from([("id".into(), dtg_execution::GatewayValue::Integer(41))]),
            None,
            &GatewayCancellationToken::new(),
        )
        .await
        .unwrap();

    let GatewayResponse::Rows(rows) = response else {
        panic!("expected relationship rows")
    };
    assert_eq!(rows.fields(), &["r"]);
    assert_eq!(
        rows.rows(),
        &[vec![dtg_execution::GatewayValue::Map(BTreeMap::from([
            ("id".into(), dtg_execution::GatewayValue::Integer(73)),
            ("source".into(), dtg_execution::GatewayValue::Integer(41)),
            ("target".into(), dtg_execution::GatewayValue::Integer(42)),
            (
                "type".into(),
                dtg_execution::GatewayValue::String("KNOWS".into()),
            ),
            (
                "properties".into(),
                dtg_execution::GatewayValue::Map(BTreeMap::new()),
            ),
        ]))]]
    );
}

#[tokio::test]
async fn logical_vertex_point_rejects_a_non_unit_row_bound() {
    let root = tempfile::tempdir().unwrap();
    let capabilities = fjall_capabilities();
    let binding = fjall_binding_with_capabilities("gateway-logical-point-bound", &capabilities);
    let node = DataNodeBuilder::from_config(
        DataProcessConfig::new(root.path().join("business"), root.path().join("raft"))
            .assign(binding.clone()),
    )
    .start()
    .await
    .unwrap();
    let applied_index = node.replica_observations().await[0].applied_index();
    let mut body = logical_vertex_point_body(&binding, &capabilities, applied_index);
    let point_id_end = body
        .windows(16)
        .position(|bytes| bytes == 37_u128.to_be_bytes())
        .unwrap()
        + 16;
    body[point_id_end + 2..point_id_end + 6].copy_from_slice(&2_u32.to_be_bytes());

    let result = node
        .rpc_service()
        .execute_fragment(Request::new(ExecutionFragment {
            context: Some(shard_context(&binding)),
            fragment_id: 85_u128.to_be_bytes().to_vec(),
            payload: Some(BoundedPayload {
                format_version: 1,
                declared_len: body.len() as u64,
                item_count: 1,
                checksum: checksum_bytes(&body).to_vec(),
                body,
            }),
            schema_version: 31,
            capability_digest: binding.capability_digest().get().to_vec(),
            applied_index,
            transaction_time: 41,
            valid_at: 10,
            snapshot_immutable: true,
        }))
        .await;
    let error = match result {
        Ok(_) => panic!("invalid logical point bound returned a response stream"),
        Err(error) => error,
    };

    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(error.message().contains("logical point row bound"));
}

#[tokio::test]
async fn gateway_v2_rpc_executes_a_real_fenced_fragment() {
    let root = tempfile::tempdir().unwrap();
    let binding = fjall_binding("gateway-v2-fragment");
    let node = DataNodeBuilder::from_config(
        DataProcessConfig::new(root.path().join("business"), root.path().join("raft"))
            .assign(binding.clone()),
    )
    .start()
    .await
    .unwrap();
    let vertex = VertexVersion::new(
        VertexId::new(37).unwrap(),
        Version::new(1),
        ValidInterval::new(1, 100).unwrap(),
        TransactionTime::new(41).unwrap(),
        Properties::new(),
    )
    .unwrap();
    let command = ShardCommand::CommitSingleShard(
        CommitSingleShard::new(
            CommandId::new(63).unwrap(),
            binding.placement_epoch().get(),
            binding.backend_generation().get(),
            vec![LogicalMutation::PutVertex(vertex)],
        )
        .unwrap(),
    );
    let command_body = command.encode_current().unwrap();
    node.rpc_service()
        .apply_transaction(Request::new(TransactionRequest {
            context: Some(shard_context(&binding)),
            transaction_id: 67_u128.to_be_bytes().to_vec(),
            operation: TransactionOperation::Commit.into(),
            idempotency_key: 63_u128.to_be_bytes().to_vec(),
            payload: Some(BoundedPayload {
                format_version: 1,
                declared_len: command_body.len() as u64,
                item_count: 1,
                checksum: checksum_bytes(&command_body).to_vec(),
                body: command_body,
            }),
        }))
        .await
        .unwrap();
    let applied_index = node.replica_observations().await[0].applied_index();
    let fragment_body = scan_fragment_body();
    let execution_body = vec![1, 0, 0, 0, 0, 0, 0];
    let mut stream = ClusterGatewayService::execute(
        &node.rpc_service(),
        Request::new(GatewayRequest {
            request: Some(shard_context(&binding).request.unwrap()),
            execution_request: Some(BoundedPayload {
                format_version: 1,
                declared_len: execution_body.len() as u64,
                item_count: 1,
                checksum: checksum_bytes(&execution_body).to_vec(),
                body: execution_body,
            }),
            fragments: vec![ExecutionFragment {
                context: Some(shard_context(&binding)),
                fragment_id: 71_u128.to_be_bytes().to_vec(),
                payload: Some(BoundedPayload {
                    format_version: 1,
                    declared_len: fragment_body.len() as u64,
                    item_count: 1,
                    checksum: checksum_bytes(&fragment_body).to_vec(),
                    body: fragment_body,
                }),
                schema_version: 31,
                capability_digest: binding.capability_digest().get().to_vec(),
                applied_index,
                transaction_time: 41,
                valid_at: 10,
                snapshot_immutable: true,
            }],
        }),
    )
    .await
    .unwrap()
    .into_inner();
    let response = stream.next().await.unwrap().unwrap();

    assert_eq!(response.status.unwrap().code, StatusCode::Ok as i32);
    assert_eq!(response.batch.unwrap().row_count, 1);
    assert!(stream.next().await.is_none());
}

#[tokio::test]
async fn gateway_write_is_not_treated_as_query() {
    let root = tempfile::tempdir().unwrap();
    let binding = fjall_binding("gateway-write-rejection");
    let node = DataNodeBuilder::from_config(
        DataProcessConfig::new(root.path().join("business"), root.path().join("raft"))
            .assign(binding.clone()),
    )
    .start()
    .await
    .unwrap();
    let body = vec![2];
    let result = ClusterGatewayService::execute(
        &node.rpc_service(),
        Request::new(GatewayRequest {
            request: shard_context(&binding).request,
            execution_request: Some(BoundedPayload {
                format_version: 1,
                declared_len: body.len() as u64,
                item_count: 1,
                checksum: checksum_bytes(&body).to_vec(),
                body,
            }),
            fragments: Vec::new(),
        }),
    )
    .await;
    let error = match result {
        Ok(_) => panic!("Gateway write was treated as a query"),
        Err(error) => error,
    };
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(error.message().contains("only planned query fragments"));
}

#[tokio::test]
async fn execute_fragment_preserves_a_present_zero_row_fragment() {
    let root = tempfile::tempdir().unwrap();
    let binding = fjall_binding("rpc-empty-fragment");
    let node = DataNodeBuilder::from_config(
        DataProcessConfig::new(root.path().join("business"), root.path().join("raft"))
            .assign(binding.clone()),
    )
    .start()
    .await
    .unwrap();
    let body = scan_fragment_body();
    let mut stream = node
        .rpc_service()
        .execute_fragment(Request::new(ExecutionFragment {
            context: Some(shard_context(&binding)),
            fragment_id: 72_u128.to_be_bytes().to_vec(),
            payload: Some(BoundedPayload {
                format_version: 1,
                declared_len: body.len() as u64,
                item_count: 2,
                checksum: checksum_bytes(&body).to_vec(),
                body,
            }),
            schema_version: 31,
            capability_digest: binding.capability_digest().get().to_vec(),
            applied_index: 1,
            transaction_time: 41,
            valid_at: 10,
            snapshot_immutable: true,
        }))
        .await
        .unwrap()
        .into_inner();
    let batch = stream.next().await.unwrap().unwrap();
    assert_eq!(batch.fragment_id, 72_u128.to_be_bytes());
    assert_eq!(batch.row_count, 0);
    assert!(stream.next().await.is_none());

    let metrics = node.request_metrics().snapshot();
    for stage in [
        RequestStage::DataValidation,
        RequestStage::DataRouting,
        RequestStage::DataProviderExecution,
    ] {
        let stage = metrics.stage(stage);
        assert_eq!(stage.success, 1);
        assert_eq!(stage.error, 0);
        assert_eq!(stage.cancelled, 0);
    }
}

#[tokio::test]
async fn present_zero_row_fragment_round_trips_from_data_to_gateway() {
    let root = tempfile::tempdir().unwrap();
    let capabilities = fjall_capabilities();
    let binding = fjall_binding_with_capabilities("gateway-empty-fragment", &capabilities);
    let node = DataNodeBuilder::from_config(
        DataProcessConfig::new(root.path().join("business"), root.path().join("raft"))
            .assign(binding.clone()),
    )
    .start()
    .await
    .unwrap();
    let applied_index = node.replica_observations().await[0].applied_index();
    let execution = GatewayExecution::for_process(
        Arc::new(GatewayProtocolV2Transport::new(Arc::new(
            InProcessDataGatewayClient {
                service: node.rpc_service(),
            },
        ))),
        planning_context(binding, capabilities, applied_index),
    );

    let response = execution
        .execute_statement(
            GatewayRequestContext::new(7, 84, u64::MAX, Vec::new()).unwrap(),
            "MATCH (n) RETURN n.id".into(),
            BTreeMap::new(),
            None,
            &GatewayCancellationToken::new(),
        )
        .await
        .unwrap();

    let GatewayResponse::Rows(rows) = response else {
        panic!("expected query rows")
    };
    assert_eq!(rows.fields(), &["n.id"]);
    assert!(rows.rows().is_empty());
}

#[tokio::test]
async fn current_vertex_count_round_trips_as_one_scalar_from_data() {
    let root = tempfile::tempdir().unwrap();
    let capabilities = fjall_capabilities();
    let binding = fjall_binding_with_capabilities("gateway-partial-vertex-count", &capabilities);
    let node = DataNodeBuilder::from_config(
        DataProcessConfig::new(root.path().join("business"), root.path().join("raft"))
            .assign(binding.clone()),
    )
    .start()
    .await
    .unwrap();
    let vertices = [91_u128, 92]
        .into_iter()
        .map(|id| {
            VertexVersion::new(
                VertexId::new(id).unwrap(),
                Version::new(1),
                ValidInterval::new(1, 100).unwrap(),
                TransactionTime::new(41).unwrap(),
                Properties::new(),
            )
            .unwrap()
        })
        .map(LogicalMutation::PutVertex)
        .collect();
    let command = ShardCommand::CommitSingleShard(
        CommitSingleShard::new(
            CommandId::new(91).unwrap(),
            binding.placement_epoch().get(),
            binding.backend_generation().get(),
            vertices,
        )
        .unwrap(),
    );
    let command_body = command.encode_current().unwrap();
    node.rpc_service()
        .apply_transaction(Request::new(TransactionRequest {
            context: Some(shard_context(&binding)),
            transaction_id: 91_u128.to_be_bytes().to_vec(),
            operation: TransactionOperation::Commit.into(),
            idempotency_key: 91_u128.to_be_bytes().to_vec(),
            payload: Some(BoundedPayload {
                format_version: 1,
                declared_len: command_body.len() as u64,
                item_count: 2,
                checksum: checksum_bytes(&command_body).to_vec(),
                body: command_body,
            }),
        }))
        .await
        .unwrap();
    let applied_index = node.replica_observations().await[0].applied_index();
    let execution = GatewayExecution::for_process(
        Arc::new(GatewayProtocolV2Transport::new(Arc::new(
            InProcessDataGatewayClient {
                service: node.rpc_service(),
            },
        ))),
        planning_context(binding, capabilities, applied_index),
    );

    let response = execution
        .execute_statement(
            GatewayRequestContext::new(7, 91, u64::MAX, Vec::new()).unwrap(),
            "MATCH (n) RETURN COUNT(*)".into(),
            BTreeMap::new(),
            None,
            &GatewayCancellationToken::new(),
        )
        .await
        .unwrap();

    let GatewayResponse::Rows(rows) = response else {
        panic!("expected count rows")
    };
    assert_eq!(rows.fields(), &["COUNT(*)"]);
    assert_eq!(
        rows.rows(),
        &[vec![dtg_execution::GatewayValue::Integer(2)]]
    );
    assert_eq!(
        execution
            .request_metrics()
            .snapshot()
            .stage(RequestStage::GatewayLocalExecution)
            .success,
        0
    );
}

fn scan_fragment_body() -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&1_u64.to_be_bytes());
    body.extend_from_slice(&1_u32.to_be_bytes());
    body.extend_from_slice(&0_u32.to_be_bytes());
    body.extend_from_slice(&1_u32.to_be_bytes());
    body.extend_from_slice(&1_u32.to_be_bytes());
    body.push(1);
    body.extend_from_slice(&1_u32.to_be_bytes());
    body.extend_from_slice(&0_u32.to_be_bytes());
    body.push(1);
    body.extend_from_slice(&10_i64.to_be_bytes());
    body.extend_from_slice(&41_i64.to_be_bytes());
    body.push(0);
    body.extend_from_slice(&10_u32.to_be_bytes());
    body.push(0x1f);
    body.push(0);
    body.extend_from_slice(&1_u32.to_be_bytes());
    body.extend_from_slice(&1_u32.to_be_bytes());
    body.push(0);
    body.extend_from_slice(&1_u32.to_be_bytes());
    body.extend_from_slice(&1_u32.to_be_bytes());
    body.extend_from_slice(&1_u32.to_be_bytes());
    body.extend_from_slice(&5_u32.to_be_bytes());
    body.extend_from_slice(b"value");
    body
}

#[tokio::test]
async fn fragment_rejects_two_storage_accesses() {
    let mut body = scan_fragment_body();
    body[16..20].copy_from_slice(&2_u32.to_be_bytes());
    let access_end = 57;
    let access = body[20..access_end].to_vec();
    body.splice(access_end..access_end, access);
    assert_fragment_failed_precondition("fragment-two-accesses", body).await;
}

#[tokio::test]
async fn fragment_rejects_truncated_operator_section() {
    let mut body = scan_fragment_body();
    body.pop();
    assert_fragment_failed_precondition("fragment-truncated-operator", body).await;
}

#[tokio::test]
async fn fragment_rejects_trailing_bytes() {
    let mut body = scan_fragment_body();
    body.push(0);
    assert_fragment_failed_precondition("fragment-trailing-bytes", body).await;
}

async fn assert_fragment_failed_precondition(namespace: &str, body: Vec<u8>) {
    let root = tempfile::tempdir().unwrap();
    let binding = fjall_binding(namespace);
    let node = DataNodeBuilder::from_config(
        DataProcessConfig::new(root.path().join("business"), root.path().join("raft"))
            .assign(binding.clone()),
    )
    .start()
    .await
    .unwrap();
    let result = node
        .rpc_service()
        .execute_fragment(Request::new(ExecutionFragment {
            context: Some(shard_context(&binding)),
            fragment_id: 73_u128.to_be_bytes().to_vec(),
            payload: Some(BoundedPayload {
                format_version: 1,
                declared_len: body.len() as u64,
                item_count: 1,
                checksum: checksum_bytes(&body).to_vec(),
                body,
            }),
            schema_version: 31,
            capability_digest: binding.capability_digest().get().to_vec(),
            applied_index: 1,
            transaction_time: 41,
            valid_at: 10,
            snapshot_immutable: true,
        }))
        .await;
    let error = match result {
        Ok(_) => panic!("malformed fragment returned a plausible response stream"),
        Err(error) => error,
    };
    assert_eq!(error.code(), Code::FailedPrecondition);
}

#[tokio::test]
async fn send_raft_steps_the_target_replica() {
    let root = tempfile::tempdir().unwrap();
    let binding = fjall_binding("rpc-raft");
    let node = DataNodeBuilder::from_config(
        DataProcessConfig::new(root.path().join("business"), root.path().join("raft"))
            .assign(binding.clone()),
    )
    .start()
    .await
    .unwrap();
    let message = raft::eraftpb::Message {
        msg_type: raft::eraftpb::MessageType::MsgHeartbeat as i32,
        to: binding.replica_id().get(),
        from: 101,
        term: 1,
        commit: 1,
        ..Default::default()
    };
    let body = message.encode_to_vec();
    let response = node
        .rpc_service()
        .send_raft(Request::new(RaftEnvelope {
            context: Some(shard_context(&binding)),
            from_replica_id: 101,
            to_replica_id: binding.replica_id().get(),
            term: 1,
            committed_index: 1,
            kind: RaftMessageKind::Heartbeat.into(),
            payload: Some(BoundedPayload {
                format_version: 1,
                declared_len: body.len() as u64,
                item_count: 1,
                checksum: checksum_bytes(&body).to_vec(),
                body,
            }),
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(response.code, StatusCode::Ok as i32);
}

#[tokio::test]
async fn send_raft_rejects_an_envelope_kind_that_disagrees_with_the_payload() {
    let root = tempfile::tempdir().unwrap();
    let binding = fjall_binding("rpc-raft-kind");
    let node = DataNodeBuilder::from_config(
        DataProcessConfig::new(root.path().join("business"), root.path().join("raft"))
            .assign(binding.clone()),
    )
    .start()
    .await
    .unwrap();
    let message = raft::eraftpb::Message {
        msg_type: raft::eraftpb::MessageType::MsgHeartbeat as i32,
        to: binding.replica_id().get(),
        from: 101,
        term: 1,
        commit: 1,
        ..Default::default()
    };
    let body = message.encode_to_vec();

    let error = node
        .rpc_service()
        .send_raft(Request::new(RaftEnvelope {
            context: Some(shard_context(&binding)),
            from_replica_id: 101,
            to_replica_id: binding.replica_id().get(),
            term: 1,
            committed_index: 1,
            kind: RaftMessageKind::Vote.into(),
            payload: Some(BoundedPayload {
                format_version: 1,
                declared_len: body.len() as u64,
                item_count: 1,
                checksum: checksum_bytes(&body).to_vec(),
                body,
            }),
        }))
        .await
        .unwrap_err();

    assert_eq!(error.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn snapshot_rpc_fails_closed_with_a_typed_retryable_status() {
    let root = tempfile::tempdir().unwrap();
    let binding = fjall_binding("rpc-snapshot");
    let node = DataNodeBuilder::from_config(
        DataProcessConfig::new(root.path().join("business"), root.path().join("raft"))
            .assign(binding.clone()),
    )
    .start()
    .await
    .unwrap();
    let body = vec![1_u8];
    let response = node
        .rpc_service()
        .install_replica_snapshot(Request::new(LogicalReplicaSnapshot {
            context: Some(shard_context(&binding)),
            snapshot_id: 67_u128.to_be_bytes().to_vec(),
            snapshot_version: 1,
            last_included_term: 1,
            last_included_index: 1,
            chunk_index: 0,
            chunk_count: 1,
            payload: Some(BoundedPayload {
                format_version: 1,
                declared_len: body.len() as u64,
                item_count: 1,
                checksum: checksum_bytes(&body).to_vec(),
                body,
            }),
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(response.code, StatusCode::Unavailable as i32);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_provider_uses_the_versioned_storage_client_path() {
    let root = tempfile::tempdir().unwrap();
    let remote_root = root.path().join("remote");
    std::fs::create_dir_all(&remote_root).unwrap();
    let server = ReferenceStorageServer::spawn(
        Arc::new(FjallStorageTckFactory::new(remote_root)),
        ReferenceServerConfig::default(),
    )
    .await
    .unwrap();
    let remote = server.tck_factory("third-party").unwrap();
    let binding = remote.binding("remote-process", 1).unwrap();
    let endpoint_profile = binding.endpoint_profile_ref().to_owned();
    let credential_profile = binding.credential_ref().to_owned();
    let config = DataProcessConfig::new(root.path().join("business"), root.path().join("raft"))
        .with_endpoint_profile(endpoint_profile, EndpointProfile::Remote(server.uri()))
        .with_credential_profile(
            credential_profile,
            CredentialProfile::RemoteSignedToken([0xd7; 32]),
        );

    let node = DataNodeBuilder::new(config.consensus_root())
        .with_provider(
            ProviderKind::Remote("third-party".into()),
            Arc::new(RemoteResolver::from_config("third-party", &config)),
        )
        .assign(binding)
        .start()
        .await
        .unwrap();

    let failures = node.replica_failures().await;
    assert!(
        failures.is_empty(),
        "unexpected remote failure: {failures:#?}"
    );
    assert_eq!(node.observed_replicas().await.len(), 1);
    assert!(
        node.provider_kinds()
            .contains(&ProviderKind::Remote("third-party".into()))
    );
}
