use dtg_language_ir::{
    GraphScope, Limit, LogicalExpr, LogicalNode, LogicalNodeId, LogicalNodeKind, LogicalPlan,
    LogicalProgram, LogicalStatement, NodeScan, ReadScope, RowSchema, Value, VertexLookup,
};
use dtg_plan::{
    CatalogShard, CatalogSnapshot, PlanError, PlanningContext, SnapshotRequirements, StorageAccess,
    plan,
};
use dtg_storage::{
    BackendClass, BindingRole, CapabilityManifest, ProviderKind, PushdownOperation, ReplicaBinding,
    TransactionTime, Version,
};

fn binding(capabilities: &CapabilityManifest) -> ReplicaBinding {
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
        .shard_id(13)
        .placement_epoch(7)
        .replica_id(19)
        .backend_generation(3)
        .backend_class_digest(class.digest())
        .provider_kind(ProviderKind::Fjall)
        .contract_version(1)
        .layout_version(1)
        .capability_digest(capabilities.digest())
        .namespace_id("graph-9-shard-13")
        .endpoint_profile_ref("fixture-endpoint")
        .credential_ref("fixture-credential")
        .role(BindingRole::Active)
        .build()
        .unwrap()
}

fn context(capabilities: CapabilityManifest, logical_scan_bound: Option<u32>) -> PlanningContext {
    let catalog = CatalogSnapshot::new(
        Version::new(11),
        Version::new(5),
        vec![CatalogShard::new(binding(&capabilities), 29)],
    )
    .unwrap();
    PlanningContext::new(
        catalog,
        capabilities,
        SnapshotRequirements::fixed(TransactionTime::new(23).unwrap(), 17),
        logical_scan_bound,
    )
    .unwrap()
}

fn query(kind: LogicalNodeKind) -> LogicalProgram {
    LogicalProgram {
        version: dtg_language_ir::IrVersion::CURRENT,
        graph_scope: GraphScope::Explicit(dtg_storage::GraphId::new(9).unwrap()),
        parameters: Vec::new(),
        statement: LogicalStatement::Query(LogicalPlan {
            root: LogicalNodeId::new(1),
            nodes: vec![LogicalNode {
                id: LogicalNodeId::new(1),
                kind,
            }],
        }),
        result_schema: RowSchema::empty(),
    }
}

fn point_query() -> LogicalProgram {
    query(LogicalNodeKind::VertexLookup(VertexLookup {
        variable: "vertex".into(),
        id: LogicalExpr::Literal(Value::Integer(41)),
        labels: Vec::new(),
        read_scope: ReadScope::current(),
    }))
}

fn scan_query() -> LogicalProgram {
    query(LogicalNodeKind::NodeScan(NodeScan {
        variable: "vertex".into(),
        labels: Vec::new(),
        read_scope: ReadScope::current(),
    }))
}

fn skipped_scan_query() -> LogicalProgram {
    let mut program = scan_query();
    let LogicalStatement::Query(plan) = &mut program.statement else {
        unreachable!()
    };
    plan.nodes.push(LogicalNode {
        id: LogicalNodeId::new(2),
        kind: LogicalNodeKind::Limit(Limit {
            input: LogicalNodeId::new(1),
            skip: Some(LogicalExpr::Literal(Value::Integer(5))),
            limit: Some(LogicalExpr::Literal(Value::Integer(2))),
        }),
    });
    plan.root = LogicalNodeId::new(2);
    program
}

#[test]
fn exact_pushdown_removes_the_execution_residual() {
    let capabilities =
        CapabilityManifest::from_names(dtg_plan::EXACT_VERTEX_POINT_CAPABILITIES).unwrap();
    let plan = plan(&point_query(), &context(capabilities, None)).unwrap();

    assert_eq!(
        plan.fragments()[0].storage_accesses()[0].node(),
        LogicalNodeId::new(1)
    );

    match &plan.fragments()[0].storage_accesses()[0] {
        StorageAccess::Pushdown {
            guarantee,
            residual,
            ..
        } => {
            assert!(guarantee.is_exact());
            assert!(residual.is_none());
        }
        StorageAccess::Logical(_) => panic!("exact provider capability must use pushdown"),
    }
}

#[test]
fn scan_pushdown_bound_includes_rows_consumed_by_skip() {
    let capabilities =
        CapabilityManifest::from_names(dtg_plan::EXACT_VERTEX_SCAN_CAPABILITIES).unwrap();
    let plan = plan(&skipped_scan_query(), &context(capabilities, None)).unwrap();

    let StorageAccess::Pushdown { request, .. } = &plan.fragments()[0].storage_accesses()[0] else {
        panic!("exact scan capability must use pushdown");
    };
    let PushdownOperation::VertexScan(scan) = request.operation() else {
        panic!("scan query must lower to a vertex scan");
    };
    assert_eq!(scan.limit(), 7);
}

#[test]
fn partial_semantic_guarantees_retain_an_execution_residual() {
    let capabilities = CapabilityManifest::from_names([
        dtg_plan::CAP_VERTEX_POINT,
        dtg_plan::CAP_TEMPORAL_EXACT,
        dtg_plan::CAP_NULL_EXACT,
    ])
    .unwrap();
    let plan = plan(&point_query(), &context(capabilities, None)).unwrap();

    match &plan.fragments()[0].storage_accesses()[0] {
        StorageAccess::Pushdown {
            guarantee,
            residual,
            ..
        } => {
            assert!(!guarantee.is_exact());
            assert!(guarantee.temporal());
            assert!(guarantee.nulls());
            assert!(!guarantee.duplicates());
            assert!(residual.is_some());
        }
        StorageAccess::Logical(_) => panic!("supported point access must remain a pushdown"),
    }
}

#[test]
fn unsupported_point_pushdown_uses_intrinsically_bounded_logical_read() {
    let capabilities = CapabilityManifest::from_names([] as [&str; 0]).unwrap();
    let plan = plan(&point_query(), &context(capabilities, None)).unwrap();

    match &plan.fragments()[0].storage_accesses()[0] {
        StorageAccess::Logical(request) => assert_eq!(request.row_bound(), 1),
        StorageAccess::Pushdown { .. } => panic!("unsupported point access must fall back"),
    }
}

#[test]
fn unsupported_scan_without_a_bound_fails_planning() {
    let capabilities = CapabilityManifest::from_names([] as [&str; 0]).unwrap();

    assert_eq!(
        plan(&scan_query(), &context(capabilities, None)),
        Err(PlanError::NoBoundedAccess {
            node: LogicalNodeId::new(1)
        })
    );
}

#[test]
fn unsupported_scan_with_a_bound_uses_bounded_logical_read() {
    let capabilities = CapabilityManifest::from_names([] as [&str; 0]).unwrap();
    let plan = plan(&scan_query(), &context(capabilities, Some(64))).unwrap();

    match &plan.fragments()[0].storage_accesses()[0] {
        StorageAccess::Logical(request) => assert_eq!(request.row_bound(), 64),
        StorageAccess::Pushdown { .. } => panic!("unsupported scan must fall back"),
    }
}
