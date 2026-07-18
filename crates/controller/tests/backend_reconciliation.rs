use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use control_plane::{
    BackendMigrationRecord, BackendMigrationState, BackendProfile, BackendReplicaReceipt,
    CatalogCommand, CatalogState, DeploymentMode, GraphDefinition, Placement, TopologyDefinition,
};
use controller::{
    BackendDataPlaneApi, BackendReconcileOutcome, BackendReconciler, CatalogApi, ControllerError,
};
use storage_api::AdapterRequirement;

#[derive(Clone)]
struct MemoryCatalog(Arc<Mutex<CatalogState>>);

impl CatalogApi for MemoryCatalog {
    async fn load(&self) -> Result<CatalogState, ControllerError> {
        Ok(self.0.lock().unwrap().clone())
    }

    async fn propose(&self, command: CatalogCommand) -> Result<(), ControllerError> {
        self.0
            .lock()
            .unwrap()
            .apply(command)
            .map(|_| ())
            .map_err(|error| ControllerError::Catalog(error.to_string()))
    }
}

#[derive(Clone, Default)]
struct MemoryData {
    calls: Arc<Mutex<Vec<&'static str>>>,
}

impl BackendDataPlaneApi for MemoryData {
    async fn prepare_target(
        &self,
        migration: &BackendMigrationRecord,
        graph: &GraphDefinition,
    ) -> Result<Vec<BackendReplicaReceipt>, ControllerError> {
        self.receipts("prepare", BackendMigrationState::Restored, migration, graph)
    }

    async fn begin_dual_apply(
        &self,
        migration: &BackendMigrationRecord,
        graph: &GraphDefinition,
    ) -> Result<Vec<BackendReplicaReceipt>, ControllerError> {
        self.receipts(
            "dual",
            BackendMigrationState::DualApplying,
            migration,
            graph,
        )
    }

    async fn verify_dual_apply(
        &self,
        migration: &BackendMigrationRecord,
        graph: &GraphDefinition,
    ) -> Result<Option<Vec<BackendReplicaReceipt>>, ControllerError> {
        self.receipts("verify", BackendMigrationState::Verified, migration, graph)
            .map(Some)
    }

    async fn cutover(
        &self,
        migration: &BackendMigrationRecord,
        graph: &GraphDefinition,
    ) -> Result<Vec<BackendReplicaReceipt>, ControllerError> {
        self.receipts("cutover", BackendMigrationState::CutOver, migration, graph)
    }

    async fn retire_source(
        &self,
        migration: &BackendMigrationRecord,
        graph: &GraphDefinition,
    ) -> Result<Vec<BackendReplicaReceipt>, ControllerError> {
        self.receipts(
            "retire",
            BackendMigrationState::SourceRetired,
            migration,
            graph,
        )
    }

    async fn abort(
        &self,
        _migration: &BackendMigrationRecord,
        _graph: &GraphDefinition,
    ) -> Result<(), ControllerError> {
        self.calls.lock().unwrap().push("abort");
        Ok(())
    }
}

impl MemoryData {
    fn receipts(
        &self,
        call: &'static str,
        state: BackendMigrationState,
        migration: &BackendMigrationRecord,
        graph: &GraphDefinition,
    ) -> Result<Vec<BackendReplicaReceipt>, ControllerError> {
        self.calls.lock().unwrap().push(call);
        graph
            .topology()
            .placements()
            .iter()
            .flat_map(|placement| {
                placement.voters().iter().map(move |node_id| {
                    let digest = migration
                        .receipts()
                        .get(&(
                            BackendMigrationState::Restored,
                            placement.shard_id(),
                            *node_id,
                        ))
                        .map_or([placement.shard_id() as u8; 32], |receipt| {
                            receipt.profile_digest()
                        });
                    BackendReplicaReceipt::new(
                        state,
                        placement.shard_id(),
                        *node_id,
                        100 + u64::from(placement.shard_id()),
                        digest,
                    )
                    .map_err(|error| ControllerError::Data(error.to_string()))
                })
            })
            .collect()
    }
}

#[tokio::test]
async fn backend_workflow_reconciles_to_source_retired_and_survives_catalog_reloads() {
    let state = initialized();
    let catalog = MemoryCatalog(Arc::new(Mutex::new(state)));
    let data = MemoryData::default();
    let reconciler = BackendReconciler::new(catalog.clone(), data.clone(), 8).unwrap();

    let expected = [
        BackendMigrationState::Restored,
        BackendMigrationState::DualApplying,
        BackendMigrationState::Verified,
        BackendMigrationState::Committing,
        BackendMigrationState::CutOver,
        BackendMigrationState::Published,
        BackendMigrationState::SourceRetired,
    ];
    for (turn, target) in expected.into_iter().enumerate() {
        let outcome = reconciler.reconcile(91, 2_000 + turn as u64).await.unwrap();
        assert!(matches!(
            outcome,
            BackendReconcileOutcome::Advanced { to, .. } if to == target
        ));
        let snapshot = catalog.load().await.unwrap().encode_snapshot().unwrap();
        *catalog.0.lock().unwrap() = CatalogState::decode_snapshot(&snapshot).unwrap();
    }

    assert!(matches!(
        reconciler.reconcile(91, 3_000).await.unwrap(),
        BackendReconcileOutcome::Terminal {
            state: BackendMigrationState::SourceRetired,
            ..
        }
    ));
    assert_eq!(
        *data.calls.lock().unwrap(),
        vec!["prepare", "dual", "verify", "cutover", "retire"]
    );
    let final_state = catalog.load().await.unwrap();
    assert_eq!(
        final_state.graph(7).unwrap().backend().provider(),
        "postgresql"
    );
    assert!(final_state.active_backend_migration(7).is_none());
}

fn initialized() -> CatalogState {
    let mut state = CatalogState::new();
    let source = profile("rocksdb", 1);
    state
        .apply(CatalogCommand::create_graph(
            1,
            0,
            GraphDefinition::new(
                7,
                "social",
                1,
                TopologyDefinition::new(
                    DeploymentMode::SharedNothing,
                    9,
                    128,
                    1,
                    vec![
                        Placement::new(10, 1, vec![1, 2]).unwrap(),
                        Placement::new(20, 1, vec![2, 3]).unwrap(),
                    ],
                )
                .unwrap(),
                source.clone(),
            )
            .unwrap(),
        ))
        .unwrap();
    state
        .apply(CatalogCommand::create_backend_migration(
            2,
            1,
            BackendMigrationRecord::new(91, 7, source, profile("postgresql", 2), 7, 1_000).unwrap(),
        ))
        .unwrap();
    state
}

fn profile(provider: &str, generation: u64) -> BackendProfile {
    BackendProfile::new(
        provider,
        BTreeMap::from([("sidecar_endpoint".into(), "127.0.0.1:9900".into())]),
        BTreeMap::new(),
        AdapterRequirement::HotPluggableReplica,
        generation,
    )
    .unwrap()
}
