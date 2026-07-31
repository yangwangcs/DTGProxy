use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use dtg_analytics::{
    AlgorithmRequest, AnalyticsAlgorithmStep, AnalyticsArtifact, AnalyticsArtifactIoBudget,
    AnalyticsArtifactManifest, AnalyticsArtifactRepository, AnalyticsJobError, AnalyticsJobId,
    AnalyticsJobSpec, AnalyticsLedger, AnalyticsProjection, AnalyticsProjectionProvider,
    AnalyticsRequestIdentity, AnalyticsScheduler, AnalyticsSchedulerConfig, AnalyticsSchedulerTick,
    AnalyticsStepProvider, AnalyticsStepRequest, ArtifactKind, BuiltInAlgorithmId,
    CancellationToken as AnalyticsCancellationToken, Digest32, JobTimestamp, ProjectionSpec,
    SchedulerFailure, ShardSnapshotProvenance, SnapshotProvenance, WorkerId,
};
use dtg_control::{
    ActionCommand, ActionState, CatalogCommand, CatalogState, ControlError, GraphId,
    ObservedNodeState, PlacementEpoch as ControlPlacementEpoch, ReconcileAction, ReplicaId,
    RetentionPin,
};
use dtg_execution::{
    ControlActionExecutor, ControllerExecution, DataExecution, GatewayCancellationToken,
    GatewayExecution, GatewayRequestContext, GatewayValue, MetaExecution, ProviderKind,
    ProviderResolver, ReplicaBinding, ReplicaStateStore, RequestStage, StoreFuture,
};
use dtg_language::{EmptySchemaCatalog, Language};
use dtg_plan::{
    CAP_NULL_EXACT, CAP_TEMPORAL_EXACT, CAP_VERTEX_SCAN, CatalogShard, CatalogSnapshot,
    EXACT_VERTEX_SCAN_CAPABILITIES, Planner, PlanningContext, SnapshotRequirements,
};
use dtg_query::{
    CancellationToken as QueryCancellationToken, ExecutableAccess, ExecutableOperatorKind,
    QueryBudget, QueryRuntime, ReadOperation, ResidualPredicate,
};
use dtg_shard::ShardError;
use dtg_storage::{
    ApplyReceipt, BackendClass, BackendGeneration, BindingRole, CapabilityManifest, ChangeRecord,
    CommittedShardBatch, ConsensusEntry, ConsensusSnapshotInstall, ConsensusSnapshotMetadata,
    ConsensusStore, PlacementEpoch, RaftHardState, RaftMembership, ReadFence, ReplicaMetadata,
    ShardId, StorageError, TemporalReadView, TransactionId, TransactionTime, Version,
};
use dtg_transaction::{
    CommitResolution, CommitTimeReservation, ShardCommandExecutor, ShardRequest,
    ShardSnapshotFence, SnapshotToken, SubmissionFailure, SubmissionFuture, TemporalTxnCoordinator,
    TimestampAuthority, TransactionHistory, TxnError, TxnFuture,
};

struct FailingResolver {
    kind: ProviderKind,
}

impl ProviderResolver for FailingResolver {
    fn provider_kind(&self) -> ProviderKind {
        self.kind.clone()
    }

    fn open<'a>(&'a self, _binding: ReplicaBinding) -> StoreFuture<'a, Arc<dyn ReplicaStateStore>> {
        Box::pin(async { Err(dtg_execution::StorageError::Unsupported) })
    }
}

fn resolver(kind: ProviderKind) -> Arc<dyn ProviderResolver> {
    Arc::new(FailingResolver { kind })
}

struct EchoResolver {
    kind: ProviderKind,
}

impl ProviderResolver for EchoResolver {
    fn provider_kind(&self) -> ProviderKind {
        self.kind.clone()
    }

    fn open<'a>(&'a self, binding: ReplicaBinding) -> StoreFuture<'a, Arc<dyn ReplicaStateStore>> {
        Box::pin(
            async move { Ok(Arc::new(BindingStore { binding }) as Arc<dyn ReplicaStateStore>) },
        )
    }
}

struct BindingStore {
    binding: ReplicaBinding,
}

struct BindingConsensus {
    binding: ReplicaBinding,
}

impl ConsensusStore for BindingConsensus {
    fn binding(&self) -> &ReplicaBinding {
        &self.binding
    }

    fn append(&self, _entries: Vec<ConsensusEntry>) -> StoreFuture<'_, ()> {
        Box::pin(async { Err(StorageError::Unsupported) })
    }

    fn entries(
        &self,
        _low: u64,
        _high: u64,
        _max_bytes: u64,
    ) -> StoreFuture<'_, Vec<ConsensusEntry>> {
        Box::pin(async { Err(StorageError::Unsupported) })
    }

    fn truncate_suffix(&self, _from_index: u64) -> StoreFuture<'_, ()> {
        Box::pin(async { Err(StorageError::Unsupported) })
    }

    fn hard_state(&self) -> StoreFuture<'_, RaftHardState> {
        Box::pin(async { Err(StorageError::Unsupported) })
    }

    fn set_hard_state(&self, _state: RaftHardState) -> StoreFuture<'_, ()> {
        Box::pin(async { Err(StorageError::Unsupported) })
    }

    fn membership(&self) -> StoreFuture<'_, RaftMembership> {
        Box::pin(async { Err(StorageError::Unsupported) })
    }

    fn set_membership(&self, _membership: RaftMembership) -> StoreFuture<'_, ()> {
        Box::pin(async { Err(StorageError::Unsupported) })
    }

    fn snapshot_metadata(&self) -> StoreFuture<'_, Option<ConsensusSnapshotMetadata>> {
        Box::pin(async { Err(StorageError::Unsupported) })
    }

    fn set_snapshot_metadata(&self, _metadata: ConsensusSnapshotMetadata) -> StoreFuture<'_, ()> {
        Box::pin(async { Err(StorageError::Unsupported) })
    }

    fn snapshot_install(&self) -> StoreFuture<'_, Option<ConsensusSnapshotInstall>> {
        Box::pin(async { Err(StorageError::Unsupported) })
    }

    fn stage_snapshot_install(&self, _install: ConsensusSnapshotInstall) -> StoreFuture<'_, ()> {
        Box::pin(async { Err(StorageError::Unsupported) })
    }

    fn commit_snapshot_install(&self, _install: ConsensusSnapshotInstall) -> StoreFuture<'_, ()> {
        Box::pin(async { Err(StorageError::Unsupported) })
    }
}

impl ReplicaStateStore for BindingStore {
    fn binding(&self) -> &ReplicaBinding {
        &self.binding
    }

    fn applied_index(&self) -> StoreFuture<'_, u64> {
        Box::pin(async { Ok(0) })
    }

    fn replica_metadata<'a>(&'a self, _name: &'a str) -> StoreFuture<'a, Option<ReplicaMetadata>> {
        Box::pin(async { Ok(None) })
    }

    fn apply(&self, _batch: CommittedShardBatch) -> StoreFuture<'_, ApplyReceipt> {
        Box::pin(async { Err(StorageError::Unsupported) })
    }

    fn begin_read_view(&self, _fence: ReadFence) -> StoreFuture<'_, Box<dyn TemporalReadView>> {
        Box::pin(async { Err(StorageError::Unsupported) })
    }
}

fn binding(
    provider_kind: ProviderKind,
    shard_id: u64,
    replica_id: u64,
    namespace: &str,
) -> ReplicaBinding {
    let backend = BackendClass::new(provider_kind.clone(), 1, 1, [] as [&str; 0]).unwrap();
    ReplicaBinding::builder()
        .cluster_id(1)
        .graph_id(2)
        .shard_id(shard_id)
        .placement_epoch(3)
        .replica_id(replica_id)
        .backend_generation(4)
        .backend_class_digest(backend.digest())
        .provider_kind(provider_kind)
        .contract_version(1)
        .layout_version(1)
        .capability_digest(backend.required_capabilities().digest())
        .namespace_id(namespace)
        .endpoint_profile_ref("fixture-endpoint")
        .credential_ref("fixture-credential")
        .role(BindingRole::Active)
        .build()
        .unwrap()
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

struct FixedTimestamps;

impl TimestampAuthority for FixedTimestamps {
    fn allocate_start_time(
        &self,
        _transaction_id: TransactionId,
    ) -> TxnFuture<'_, TransactionTime> {
        Box::pin(async { Ok(TransactionTime::new(23).unwrap()) })
    }

    fn reserve_commit_time(
        &self,
        _transaction_id: TransactionId,
    ) -> TxnFuture<'_, TransactionTime> {
        Box::pin(async { Ok(TransactionTime::new(24).unwrap()) })
    }

    fn commit_time_reservation(
        &self,
        _transaction_id: TransactionId,
    ) -> TxnFuture<'_, Option<CommitTimeReservation>> {
        Box::pin(async { Ok(None) })
    }

    fn resolve_commit_time(
        &self,
        _transaction_id: TransactionId,
        _commit_time: TransactionTime,
        _resolution: CommitResolution,
    ) -> TxnFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
}

struct UnusedShardExecutor;

impl ShardCommandExecutor for UnusedShardExecutor {
    fn submit(
        &self,
        _shard_id: dtg_storage::ShardId,
        _request: ShardRequest,
    ) -> SubmissionFuture<'_> {
        Box::pin(async { Err(SubmissionFailure::Definitive(TxnError::InjectedCrash)) })
    }

    fn changes_after(
        &self,
        _shard_id: dtg_storage::ShardId,
        _applied_index: u64,
    ) -> TxnFuture<'_, Vec<ChangeRecord>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn transaction_history(
        &self,
        _shard_id: dtg_storage::ShardId,
        _transaction_id: TransactionId,
    ) -> TxnFuture<'_, TransactionHistory> {
        Box::pin(async { Err(TxnError::InjectedCrash) })
    }
}

struct UnusedProjectionProvider;

impl AnalyticsProjectionProvider for UnusedProjectionProvider {
    fn project(
        &mut self,
        _spec: &dtg_analytics::AnalyticsJobSpec,
        _cancellation: &AnalyticsCancellationToken,
    ) -> Result<AnalyticsProjection, SchedulerFailure> {
        Err(SchedulerFailure::terminal("unused-projection"))
    }
}

struct UnusedStepProvider;

impl AnalyticsStepProvider for UnusedStepProvider {
    fn step(
        &mut self,
        _request: AnalyticsStepRequest<'_>,
    ) -> Result<AnalyticsAlgorithmStep, SchedulerFailure> {
        Err(SchedulerFailure::terminal("unused-step"))
    }
}

struct UnusedArtifactRepository;

impl AnalyticsArtifactRepository for UnusedArtifactRepository {
    fn persist_controlled(
        &self,
        _artifact: &AnalyticsArtifact,
        _budget: &AnalyticsArtifactIoBudget,
    ) -> Result<AnalyticsArtifactManifest, AnalyticsJobError> {
        Err(AnalyticsJobError::Storage)
    }

    fn load_controlled(
        &self,
        _job_id: AnalyticsJobId,
        _generation: u64,
        _kind: ArtifactKind,
        _budget: &AnalyticsArtifactIoBudget,
    ) -> Result<Option<AnalyticsArtifact>, AnalyticsJobError> {
        Err(AnalyticsJobError::Storage)
    }

    fn delete(
        &self,
        _job_id: AnalyticsJobId,
        _generation: u64,
        _kind: ArtifactKind,
    ) -> Result<(), AnalyticsJobError> {
        Err(AnalyticsJobError::Storage)
    }
}

fn gateway() -> GatewayExecution {
    let scheduler = AnalyticsScheduler::new(
        WorkerId::new(1).unwrap(),
        UnusedProjectionProvider,
        UnusedStepProvider,
        UnusedArtifactRepository,
        AnalyticsSchedulerConfig::new(64, 8, 1).unwrap(),
    );
    GatewayExecution::builder()
        .with_language(Language::new(Arc::new(EmptySchemaCatalog)))
        .with_planner(Planner)
        .with_query(QueryRuntime::new(64))
        .with_transactions(TemporalTxnCoordinator::new(
            Arc::new(FixedTimestamps),
            Arc::new(UnusedShardExecutor),
        ))
        .with_analytics_scheduler(scheduler)
        .build()
        .unwrap()
}

fn planning_context(capabilities: CapabilityManifest) -> PlanningContext {
    let backend = BackendClass::new(
        ProviderKind::Fjall,
        1,
        1,
        capabilities.names().map(str::to_owned),
    )
    .unwrap();
    let binding = ReplicaBinding::builder()
        .cluster_id(1)
        .graph_id(2)
        .shard_id(5)
        .placement_epoch(3)
        .replica_id(6)
        .backend_generation(4)
        .backend_class_digest(backend.digest())
        .provider_kind(ProviderKind::Fjall)
        .contract_version(1)
        .layout_version(1)
        .capability_digest(capabilities.digest())
        .namespace_id("gateway-shard-5")
        .endpoint_profile_ref("fixture-endpoint")
        .credential_ref("fixture-credential")
        .role(BindingRole::Active)
        .build()
        .unwrap();
    let catalog = CatalogSnapshot::new(
        Version::new(11),
        Version::new(5),
        vec![CatalogShard::new(binding, 29)],
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

fn analytics_spec(identity: &str) -> AnalyticsJobSpec {
    let snapshot = SnapshotProvenance::new(
        TransactionId::new(31).unwrap(),
        TransactionTime::new(23).unwrap(),
        Version::new(11),
        vec![(
            ShardId::new(5).unwrap(),
            ShardSnapshotProvenance {
                placement_epoch: PlacementEpoch::new(3).unwrap(),
                backend_generation: BackendGeneration::new(4).unwrap(),
                applied_index: 29,
                closed_time: TransactionTime::new(30).unwrap(),
            },
        )],
    )
    .unwrap();
    AnalyticsJobSpec::new(
        AnalyticsRequestIdentity::new(identity.to_owned()).unwrap(),
        AlgorithmRequest::new(BuiltInAlgorithmId::PageRank),
        snapshot,
        ProjectionSpec::new(false, Vec::new(), Vec::new()).unwrap(),
        Digest32::new([7; 32]),
        Digest32::new([8; 32]),
        1,
        1,
        3,
        JobTimestamp::new(100),
    )
    .unwrap()
}

#[test]
fn data_builder_accepts_multiple_provider_resolvers() {
    let runtime = DataExecution::builder()
        .with_provider(ProviderKind::Fjall, resolver(ProviderKind::Fjall))
        .with_provider(ProviderKind::Neo4j, resolver(ProviderKind::Neo4j))
        .build()
        .unwrap();

    assert_eq!(runtime.provider_kinds().len(), 2);
    assert!(runtime.provider_kinds().contains(&ProviderKind::Fjall));
    assert!(runtime.provider_kinds().contains(&ProviderKind::Neo4j));
}

#[test]
fn data_builder_rejects_provider_kind_drift() {
    let result = DataExecution::builder()
        .with_provider(ProviderKind::Fjall, resolver(ProviderKind::Neo4j))
        .build();

    assert!(result.is_err());
}

#[test]
fn data_execution_rejects_namespace_reuse_by_a_different_binding() {
    let runtime = DataExecution::builder()
        .with_provider(
            ProviderKind::Fjall,
            Arc::new(EchoResolver {
                kind: ProviderKind::Fjall,
            }),
        )
        .with_provider(
            ProviderKind::Neo4j,
            Arc::new(EchoResolver {
                kind: ProviderKind::Neo4j,
            }),
        )
        .build()
        .unwrap();
    let fjall = binding(ProviderKind::Fjall, 5, 6, "exclusive-namespace");
    let neo4j = binding(ProviderKind::Neo4j, 7, 8, "exclusive-namespace");

    assert!(block_on(runtime.open_store(fjall.clone())).is_ok());
    assert!(block_on(runtime.open_store(fjall)).is_ok());
    assert!(matches!(
        block_on(runtime.open_store(neo4j)),
        Err(StorageError::NamespaceOwnerMismatch { .. })
    ));
}

#[test]
fn data_execution_keeps_each_shard_generation_backend_homogeneous() {
    let runtime = DataExecution::builder()
        .with_provider(
            ProviderKind::Fjall,
            Arc::new(EchoResolver {
                kind: ProviderKind::Fjall,
            }),
        )
        .with_provider(
            ProviderKind::Neo4j,
            Arc::new(EchoResolver {
                kind: ProviderKind::Neo4j,
            }),
        )
        .build()
        .unwrap();
    let fjall = binding(ProviderKind::Fjall, 5, 6, "fjall-shard-5");
    let neo4j = binding(ProviderKind::Neo4j, 5, 7, "neo4j-shard-5");
    let fjall_store = block_on(runtime.open_store(fjall.clone())).unwrap();
    let neo4j_store = block_on(runtime.open_store(neo4j.clone())).unwrap();

    runtime
        .add_replica(Arc::new(BindingConsensus { binding: fjall }), fjall_store)
        .unwrap();
    assert!(matches!(
        runtime.add_replica(Arc::new(BindingConsensus { binding: neo4j }), neo4j_store,),
        Err(ShardError::HeterogeneousGeneration)
    ));
}

#[test]
fn every_stable_facade_has_a_builder_entry_point() {
    let _ = GatewayExecution::builder();
    let _ = DataExecution::builder();
    let _ = MetaExecution::builder();
    let _ = ControllerExecution::builder();
}

#[test]
fn execution_exposes_thin_process_composition_contracts() {
    let _ = dtg_execution::analytics::AnalyticsLedger::new(1).unwrap();
    let _ = dtg_execution::control::CatalogState::new();
    let _ = dtg_execution::storage::ProviderKind::Fjall;
    let _: Option<&dyn dtg_execution::transaction::TimestampAuthority> = None;
    assert_eq!(dtg_execution::cluster_protocol::PROTOCOL_MAJOR, 2);
}

#[test]
fn gateway_composition_lowers_every_execution_fence_without_drift() {
    let mut gateway = gateway();
    let capabilities = CapabilityManifest::from_names(EXACT_VERTEX_SCAN_CAPABILITIES).unwrap();
    let context = planning_context(capabilities);
    let program = gateway.compile("MATCH (n) RETURN n").unwrap();
    let physical = gateway.plan(&program, &context).unwrap();

    let executable = gateway.lower_plan(&physical).unwrap();
    let planned = physical.fragments()[0].fence();
    let lowered = executable.fragments()[0].fence();

    assert_eq!(lowered.catalog_version(), planned.catalog_version());
    assert_eq!(lowered.schema_version(), planned.schema_version());
    assert_eq!(lowered.placement_epoch(), planned.placement_epoch());
    assert_eq!(lowered.backend_generation(), planned.backend_generation());
    assert_eq!(lowered.capability_digest(), planned.capability_digest());
    assert_eq!(lowered.applied_index(), planned.applied_index());
    assert_eq!(
        lowered.transaction_time(),
        planned.snapshot_requirements().transaction_time()
    );
    assert_eq!(
        lowered.valid_at(),
        planned.snapshot_requirements().valid_at()
    );
    assert!(lowered.immutable());
    assert!(matches!(
        executable.fragments()[0].accesses(),
        [ExecutableAccess::Pushdown { residual: None, .. }]
    ));

    let mut ledger = AnalyticsLedger::new(5).unwrap();
    assert_eq!(
        gateway
            .tick_analytics(
                &mut ledger,
                JobTimestamp::new(1),
                &AnalyticsCancellationToken::new(),
            )
            .unwrap(),
        AnalyticsSchedulerTick::Idle
    );
}

#[test]
fn composed_gateway_process_query_does_not_cancel_plan_metric() {
    let gateway = gateway();
    let error = block_on(gateway.execute_statement(
        GatewayRequestContext::new(1, 7, u64::MAX, Vec::new()).unwrap(),
        "MATCH (n) RETURN n".into(),
        BTreeMap::<String, GatewayValue>::new(),
        None,
        &GatewayCancellationToken::new(),
    ))
    .unwrap_err();

    assert_eq!(error.code(), "DTG-EXECUTION-PROCESS-TRANSPORT");
    let plan = gateway
        .request_metrics()
        .snapshot()
        .stage(RequestStage::GatewayPlan);
    assert_eq!(plan.success, 0);
    assert_eq!(plan.error, 0);
    assert_eq!(plan.cancelled, 0);
}

#[test]
fn gateway_lowering_preserves_the_complete_physical_operator_dag() {
    let gateway = gateway();
    let capabilities = CapabilityManifest::from_names(EXACT_VERTEX_SCAN_CAPABILITIES).unwrap();
    let context = planning_context(capabilities);
    let program = gateway
        .compile("MATCH (n) RETURN n.id ORDER BY n.id")
        .unwrap();
    let physical = gateway.plan(&program, &context).unwrap();

    let executable = gateway.lower_plan(&physical).unwrap();

    assert_eq!(executable.root_operator(), physical.root_operator().get());
    assert_eq!(executable.operators().len(), physical.operators().len());
    assert!(
        executable
            .operators()
            .iter()
            .any(|operator| matches!(operator.kind(), ExecutableOperatorKind::Source { .. }))
    );
    assert!(
        executable
            .operators()
            .iter()
            .any(|operator| matches!(operator.kind(), ExecutableOperatorKind::Sort { .. }))
    );
    assert!(
        executable
            .operators()
            .iter()
            .any(|operator| matches!(operator.kind(), ExecutableOperatorKind::Project { .. }))
    );
}

#[test]
fn gateway_lowering_preserves_required_storage_semantics_residuals() {
    let gateway = gateway();
    let capabilities =
        CapabilityManifest::from_names([CAP_VERTEX_SCAN, CAP_TEMPORAL_EXACT, CAP_NULL_EXACT])
            .unwrap();
    let context = planning_context(capabilities);
    let program = gateway.compile("MATCH (n) RETURN n").unwrap();
    let physical = gateway.plan(&program, &context).unwrap();

    let executable = gateway.lower_plan(&physical).unwrap();

    assert!(matches!(
        executable.fragments()[0].accesses(),
        [ExecutableAccess::Pushdown {
            residual: Some(ResidualPredicate::StorageSemantics),
            ..
        }]
    ));
}

#[test]
fn gateway_lowering_preserves_bounded_logical_fallbacks() {
    let gateway = gateway();
    let context = planning_context(CapabilityManifest::from_names([] as [&str; 0]).unwrap());
    let program = gateway.compile("MATCH (n) RETURN n").unwrap();
    let physical = gateway.plan(&program, &context).unwrap();

    let executable = gateway.lower_plan(&physical).unwrap();

    let [ExecutableAccess::Logical(read)] = executable.fragments()[0].accesses() else {
        panic!("unsupported pushdown must remain a bounded logical read");
    };
    assert_eq!(read.operation(), ReadOperation::VertexScan);
    assert_eq!(read.row_bound(), 128);
    assert_eq!(read.transaction_time(), TransactionTime::new(23).unwrap());
    assert_eq!(read.valid_at(), 17);
}

#[test]
fn gateway_lowering_fails_closed_on_unresolved_read_scopes() {
    let gateway = gateway();
    let context = planning_context(CapabilityManifest::from_names([] as [&str; 0]).unwrap());
    let program = gateway
        .compile("FOR SYSTEM_TIME AS OF $t MATCH (n) RETURN n")
        .unwrap();
    let physical = gateway.plan(&program, &context).unwrap();

    assert!(matches!(
        gateway.lower_plan(&physical),
        Err(dtg_query::QueryError::Unsupported(message))
            if message == "logical read scope must be resolved before execution"
    ));
}

#[test]
fn gateway_lowering_preserves_literal_point_in_time_scopes() {
    let gateway = gateway();
    let context = planning_context(CapabilityManifest::from_names([] as [&str; 0]).unwrap());
    let program = gateway
        .compile("FOR SYSTEM_TIME AS OF 23 MATCH (n) FOR VALID_TIME AS OF 17 RETURN n")
        .unwrap();
    let physical = gateway.plan(&program, &context).unwrap();

    let executable = gateway.lower_plan(&physical).unwrap();

    let [ExecutableAccess::Logical(read)] = executable.fragments()[0].accesses() else {
        panic!("literal point-in-time scope must remain a logical read");
    };
    assert_eq!(read.transaction_time(), TransactionTime::new(23).unwrap());
    assert_eq!(read.valid_at(), 17);
}

#[test]
fn gateway_snapshot_mappings_preserve_query_fences_and_analytics_provenance() {
    let gateway = gateway();
    let capabilities = CapabilityManifest::from_names(EXACT_VERTEX_SCAN_CAPABILITIES).unwrap();
    let context = planning_context(capabilities);
    let program = gateway.compile("MATCH (n) RETURN n").unwrap();
    let physical = gateway.plan(&program, &context).unwrap();
    let executable = gateway.lower_plan(&physical).unwrap();
    let token = SnapshotToken::new(
        TransactionId::new(31).unwrap(),
        TransactionTime::new(23).unwrap(),
        Version::new(11),
        vec![(
            ShardId::new(5).unwrap(),
            ShardSnapshotFence {
                placement_epoch: PlacementEpoch::new(3).unwrap(),
                backend_generation: BackendGeneration::new(4).unwrap(),
                applied_index: 29,
                closed_time: TransactionTime::new(30).unwrap(),
            },
        )],
    )
    .unwrap();

    let query_snapshot = gateway.query_snapshot(&token).unwrap();
    query_snapshot
        .validate(executable.fragments()[0].fence())
        .unwrap();
    let provenance = gateway.analytics_provenance(&token).unwrap();

    assert_eq!(provenance.transaction_id(), TransactionId::new(31).unwrap());
    assert_eq!(provenance.start_time(), TransactionTime::new(23).unwrap());
    assert_eq!(provenance.catalog_version(), Version::new(11));
    assert_eq!(
        provenance.shards()[&ShardId::new(5).unwrap()].closed_time,
        TransactionTime::new(30).unwrap()
    );
}

#[test]
fn gateway_dispatches_queries_through_the_composed_runtime() {
    let gateway = gateway();
    let capabilities = CapabilityManifest::from_names(EXACT_VERTEX_SCAN_CAPABILITIES).unwrap();
    let context = planning_context(capabilities);
    let program = gateway.compile("MATCH (n) RETURN n").unwrap();
    let physical = gateway.plan(&program, &context).unwrap();
    let executable = gateway.lower_plan(&physical).unwrap();
    let token = SnapshotToken::new(
        TransactionId::new(31).unwrap(),
        TransactionTime::new(23).unwrap(),
        Version::new(11),
        vec![(
            ShardId::new(5).unwrap(),
            ShardSnapshotFence {
                placement_epoch: PlacementEpoch::new(3).unwrap(),
                backend_generation: BackendGeneration::new(4).unwrap(),
                applied_index: 29,
                closed_time: TransactionTime::new(30).unwrap(),
            },
        )],
    )
    .unwrap();
    let snapshot = gateway.query_snapshot(&token).unwrap();

    assert!(matches!(
        block_on(gateway.execute_query(
            &executable,
            BTreeMap::new(),
            &snapshot,
            QueryBudget::unlimited(),
            QueryCancellationToken::new(),
            None,
        )),
        Err(dtg_query::QueryError::MissingStorage(shard_id))
            if shard_id == ShardId::new(5).unwrap()
    ));
}

#[test]
fn gateway_starts_transactions_with_exact_snapshot_fences() {
    let gateway = gateway();
    let context = block_on(gateway.begin_transaction(
        TransactionId::new(31).unwrap(),
        Version::new(11),
        vec![(
            ShardId::new(5).unwrap(),
            ShardSnapshotFence {
                placement_epoch: PlacementEpoch::new(3).unwrap(),
                backend_generation: BackendGeneration::new(4).unwrap(),
                applied_index: 29,
                closed_time: TransactionTime::new(30).unwrap(),
            },
        )],
    ))
    .unwrap();

    assert_eq!(
        context.snapshot().transaction_id,
        TransactionId::new(31).unwrap()
    );
    assert_eq!(
        context.snapshot().start_time,
        TransactionTime::new(23).unwrap()
    );
    assert_eq!(context.snapshot().catalog_version, Version::new(11));
    assert_eq!(
        context.snapshot().shards[&ShardId::new(5).unwrap()].applied_index,
        29
    );
}

#[test]
fn gateway_exposes_transaction_commit_and_abort_boundaries() {
    let gateway = gateway();
    let context = block_on(gateway.begin_transaction(
        TransactionId::new(31).unwrap(),
        Version::new(11),
        vec![(
            ShardId::new(5).unwrap(),
            ShardSnapshotFence {
                placement_epoch: PlacementEpoch::new(3).unwrap(),
                backend_generation: BackendGeneration::new(4).unwrap(),
                applied_index: 29,
                closed_time: TransactionTime::new(30).unwrap(),
            },
        )],
    ))
    .unwrap();

    assert_eq!(
        block_on(gateway.commit_transaction(&context, Vec::new())),
        Ok(dtg_transaction::TransactionOutcome::Committed(
            context.snapshot().start_time,
        ))
    );
    assert_eq!(
        block_on(gateway.abort_transaction(&context, Vec::new())),
        Ok(dtg_transaction::TransactionOutcome::Aborted)
    );
}

#[test]
fn meta_composes_catalog_timestamps_and_analytics_leases() {
    let mut meta = MetaExecution::builder()
        .with_catalog(CatalogState::new())
        .with_timestamps(Arc::new(FixedTimestamps))
        .with_analytics_ledger(AnalyticsLedger::new(5).unwrap())
        .build()
        .unwrap();

    assert_eq!(meta.catalog_version(), Version::new(0));
    assert_eq!(
        block_on(meta.allocate_start_time(TransactionId::new(41).unwrap())).unwrap(),
        TransactionTime::new(23).unwrap()
    );
    let job_id = meta
        .submit_analytics_job(analytics_spec("meta-lease"), JobTimestamp::new(1))
        .unwrap();
    let lease = meta
        .claim_analytics_job(
            job_id,
            WorkerId::new(9).unwrap(),
            JobTimestamp::new(2),
            meta.analytics_job_cas(job_id).unwrap(),
        )
        .unwrap();

    assert_eq!(lease.lease_epoch(), 1);
    assert_eq!(lease.expires_at(), JobTimestamp::new(7));
    let renewed = meta
        .renew_analytics_job_lease(lease, JobTimestamp::new(3))
        .unwrap();
    assert_eq!(renewed.expires_at(), JobTimestamp::new(8));
}

#[test]
fn meta_exposes_catalog_and_commit_time_mutations() {
    let mut meta = MetaExecution::builder()
        .with_catalog(CatalogState::new())
        .with_timestamps(Arc::new(FixedTimestamps))
        .with_analytics_ledger(AnalyticsLedger::new(5).unwrap())
        .build()
        .unwrap();
    let transaction_id = TransactionId::new(41).unwrap();

    assert_eq!(
        block_on(meta.reserve_commit_time(transaction_id)).unwrap(),
        TransactionTime::new(24).unwrap()
    );
    block_on(meta.resolve_commit_time(
        transaction_id,
        TransactionTime::new(24).unwrap(),
        CommitResolution::Committed,
    ))
    .unwrap();
    let command = CatalogCommand::unpin_retention(
        Version::new(0),
        GraphId::new(2).unwrap(),
        ShardId::new(5).unwrap(),
        BackendGeneration::new(4).unwrap(),
        RetentionPin::new("missing-pin".into()).unwrap(),
    );
    assert_eq!(
        meta.apply_catalog_command(command),
        Err(ControlError::UnknownGeneration)
    );
    assert_eq!(meta.catalog_version(), Version::new(0));
}

#[test]
fn meta_is_the_authority_for_reconciliation_action_state() {
    let mut meta = MetaExecution::builder()
        .with_catalog(CatalogState::new())
        .with_timestamps(Arc::new(FixedTimestamps))
        .with_analytics_ledger(AnalyticsLedger::new(5).unwrap())
        .build()
        .unwrap();
    let action = ReconcileAction::TransferLeader {
        graph_id: GraphId::new(2).unwrap(),
        shard_id: ShardId::new(5).unwrap(),
        placement_epoch: ControlPlacementEpoch::new(3).unwrap(),
        from: ReplicaId::new(7).unwrap(),
        to: ReplicaId::new(8).unwrap(),
    };

    let first = meta
        .apply_action_command(ActionCommand::enqueue(Version::new(0), action.clone()))
        .unwrap();
    let duplicate = meta
        .apply_action_command(ActionCommand::enqueue(Version::new(0), action))
        .unwrap();

    assert_eq!(first.action_id(), duplicate.action_id());
    assert_eq!(
        meta.action_record(first.action_id()).unwrap().state(),
        &ActionState::Pending { attempt: 0 }
    );
}

#[test]
fn controller_composes_observations_and_fails_closed_on_catalog_drift() {
    let mut controller = ControllerExecution::builder()
        .with_catalog(CatalogState::new())
        .build()
        .unwrap();
    let current = ObservedNodeState::new("node-1".into(), Version::new(0), Vec::new()).unwrap();
    let stale = ObservedNodeState::new("node-2".into(), Version::new(1), Vec::new()).unwrap();

    controller.record_observation(current).unwrap();
    assert!(controller.reconcile().unwrap().is_empty());
    assert!(matches!(
        controller.record_observation(stale),
        Err(ControlError::StaleObservation { expected, actual })
            if expected == Version::new(0) && actual == Version::new(1)
    ));
}

#[test]
fn controller_catalog_watch_rejects_revision_regression() {
    let revision_one = CatalogState::new()
        .apply(CatalogCommand::put_placement(
            Version::new(0),
            control_placement(1),
        ))
        .unwrap();
    let mut controller = ControllerExecution::builder()
        .with_catalog(revision_one)
        .build()
        .unwrap();

    assert!(matches!(
        controller.install_catalog(CatalogState::new()),
        Err(ControlError::CatalogRevisionRegression { current, received })
            if current == Version::new(1) && received == Version::new(0)
    ));
    assert_eq!(controller.catalog_version(), Version::new(1));
}

struct SuccessfulControlExecutor;

impl ControlActionExecutor for SuccessfulControlExecutor {
    fn execute(&self, _action: &ReconcileAction) -> Result<(), dtg_control::ActionFailure> {
        Ok(())
    }
}

#[test]
fn controller_execution_returns_authoritative_completion_command() {
    let mut meta = MetaExecution::builder()
        .with_catalog(CatalogState::new())
        .with_timestamps(Arc::new(FixedTimestamps))
        .with_analytics_ledger(AnalyticsLedger::new(5).unwrap())
        .build()
        .unwrap();
    let action = ReconcileAction::TransferLeader {
        graph_id: GraphId::new(2).unwrap(),
        shard_id: ShardId::new(5).unwrap(),
        placement_epoch: ControlPlacementEpoch::new(3).unwrap(),
        from: ReplicaId::new(7).unwrap(),
        to: ReplicaId::new(8).unwrap(),
    };
    let queued = meta
        .apply_action_command(ActionCommand::enqueue(Version::new(0), action))
        .unwrap();
    let claimed = meta
        .apply_action_command(ActionCommand::claim(
            queued.action_id(),
            "controller-1",
            10,
            5,
        ))
        .unwrap();
    let controller = ControllerExecution::builder()
        .with_catalog(CatalogState::new())
        .build()
        .unwrap();

    let completion = controller
        .execute_claimed_action(&claimed, &SuccessfulControlExecutor)
        .unwrap();
    let completed = meta.apply_action_command(completion).unwrap();
    assert!(matches!(completed.state(), ActionState::Completed { .. }));
}

fn control_placement(epoch: u64) -> dtg_control::ShardPlacement {
    let backend_class = BackendClass::new(ProviderKind::Fjall, 1, 1, ["point"]).unwrap();
    let binding = ReplicaBinding::builder()
        .cluster_id(7)
        .graph_id(2)
        .shard_id(5)
        .placement_epoch(epoch)
        .replica_id(7)
        .backend_generation(4)
        .backend_class_digest(backend_class.digest())
        .provider_kind(ProviderKind::Fjall)
        .contract_version(1)
        .layout_version(1)
        .capability_digest(backend_class.required_capabilities().digest())
        .namespace_id(format!("controller-watch-{epoch}"))
        .endpoint_profile_ref("local-fjall")
        .credential_ref("env://DTG_FJALL_ROOT")
        .role(BindingRole::Active)
        .build()
        .unwrap();
    dtg_control::ShardPlacement {
        graph_id: GraphId::new(2).unwrap(),
        shard_id: ShardId::new(5).unwrap(),
        placement_epoch: ControlPlacementEpoch::new(epoch).unwrap(),
        active_generation: BackendGeneration::new(4).unwrap(),
        backend_class: backend_class.clone(),
        replicas: vec![dtg_control::ReplicaBindingRecord::new(binding, backend_class).unwrap()],
    }
}
