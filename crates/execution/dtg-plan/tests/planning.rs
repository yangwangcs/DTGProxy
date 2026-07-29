use dtg_language_ir::{
    GraphScope, LogicalExpr, LogicalNode, LogicalNodeId, LogicalNodeKind, LogicalPlan,
    LogicalProgram, LogicalStatement, ReadScope, RowSchema, Value, VertexLookup,
};
use dtg_plan::{
    CatalogShard, CatalogSnapshot, PlanError, PlanningContext, SnapshotRequirements, plan,
};
use dtg_storage::{
    BackendClass, BindingRole, CapabilityManifest, ProviderKind, ReplicaBinding, TransactionTime,
    Version,
};

fn exact_capabilities() -> CapabilityManifest {
    CapabilityManifest::from_names(dtg_plan::EXACT_VERTEX_POINT_CAPABILITIES).unwrap()
}

fn binding(
    shard_id: u64,
    placement_epoch: u64,
    backend_generation: u64,
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
        .cluster_id(1)
        .graph_id(9)
        .shard_id(shard_id)
        .placement_epoch(placement_epoch)
        .replica_id(shard_id)
        .backend_generation(backend_generation)
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

fn point_query() -> LogicalProgram {
    LogicalProgram {
        version: dtg_language_ir::IrVersion::CURRENT,
        graph_scope: GraphScope::Explicit(dtg_storage::GraphId::new(9).unwrap()),
        parameters: Vec::new(),
        statement: LogicalStatement::Query(LogicalPlan {
            root: LogicalNodeId::new(1),
            nodes: vec![LogicalNode {
                id: LogicalNodeId::new(1),
                kind: LogicalNodeKind::VertexLookup(VertexLookup {
                    variable: "vertex".into(),
                    id: LogicalExpr::Literal(Value::Integer(41)),
                    labels: vec!["Person".into()],
                    read_scope: ReadScope::current(),
                }),
            }],
        }),
        result_schema: RowSchema::empty(),
    }
}

fn fixture_context() -> PlanningContext {
    let capabilities = exact_capabilities();
    let catalog = CatalogSnapshot::new(
        Version::new(11),
        Version::new(5),
        vec![CatalogShard::new(binding(13, 7, 3, &capabilities), 29)],
    )
    .unwrap();
    PlanningContext::new(
        catalog,
        capabilities,
        SnapshotRequirements::fixed(TransactionTime::new(23).unwrap(), 17),
        Some(128),
    )
    .unwrap()
}

#[test]
fn plan_pins_epoch_generation_and_capability_digest() {
    let plan = plan(&point_query(), &fixture_context()).unwrap();
    let fence = plan.fragments()[0].fence();
    assert_eq!(fence.placement_epoch().get(), 7);
    assert_eq!(fence.backend_generation().get(), 3);
    assert_eq!(fence.capability_digest(), exact_capabilities().digest());
}

#[test]
fn plan_pins_catalog_schema_and_snapshot_requirements() {
    let plan = plan(&point_query(), &fixture_context()).unwrap();
    let fence = plan.fragments()[0].fence();
    assert_eq!(fence.catalog_version(), Version::new(11));
    assert_eq!(fence.schema_version(), Version::new(5));
    assert_eq!(
        fence.snapshot_requirements(),
        &SnapshotRequirements::fixed(TransactionTime::new(23).unwrap(), 17)
    );
    assert_eq!(fence.applied_index(), 29);
}

#[test]
fn one_fragment_is_created_per_pinned_shard_and_exchanges_are_explicit() {
    let capabilities = exact_capabilities();
    let catalog = CatalogSnapshot::new(
        Version::new(2),
        Version::new(3),
        vec![
            CatalogShard::new(binding(17, 8, 4, &capabilities), 31),
            CatalogShard::new(binding(13, 7, 3, &capabilities), 29),
        ],
    )
    .unwrap();
    let context = PlanningContext::new(
        catalog,
        capabilities,
        SnapshotRequirements::fixed(TransactionTime::new(23).unwrap(), 17),
        Some(128),
    )
    .unwrap();

    let plan = plan(&point_query(), &context).unwrap();

    assert_eq!(plan.fragments().len(), 2);
    assert_eq!(plan.exchanges().len(), 2);
    assert_eq!(plan.fragments()[0].fence().shard_id().get(), 13);
    assert_eq!(plan.fragments()[1].fence().shard_id().get(), 17);
}

#[test]
fn catalog_rejects_a_non_active_replica_binding() {
    let capabilities = exact_capabilities();
    let candidate = binding(13, 7, 3, &capabilities)
        .to_builder()
        .role(BindingRole::Candidate)
        .build()
        .unwrap();

    assert!(matches!(
        CatalogSnapshot::new(
            Version::new(2),
            Version::new(3),
            vec![CatalogShard::new(candidate, 29)]
        ),
        Err(PlanError::InvalidCatalog(_))
    ));
}

#[test]
fn capability_digest_drift_fails_closed_before_planning() {
    let catalog_capabilities = exact_capabilities();
    let planning_capabilities =
        CapabilityManifest::from_names([dtg_plan::CAP_VERTEX_POINT]).unwrap();
    let catalog = CatalogSnapshot::new(
        Version::new(2),
        Version::new(3),
        vec![CatalogShard::new(
            binding(13, 7, 3, &catalog_capabilities),
            29,
        )],
    )
    .unwrap();

    assert!(matches!(
        PlanningContext::new(
            catalog,
            planning_capabilities,
            SnapshotRequirements::fixed(TransactionTime::new(23).unwrap(), 17),
            Some(128),
        ),
        Err(PlanError::CapabilityDrift { .. })
    ));
}
