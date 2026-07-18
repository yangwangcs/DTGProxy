use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use control_plane::{
    BackendProfile, CatalogCommand, CatalogState, DeploymentMode, GraphDefinition, MigrationRecord,
    MigrationState, Placement, RetentionPin, RetentionPinKind, TopologyDefinition,
};
use controller::{
    CatalogApi, ControllerError, DataPlaneApi, ReconcileOutcome, Reconciler, SnapshotFence,
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
struct MemoryData(Arc<Mutex<BTreeMap<&'static str, usize>>>);

impl MemoryData {
    fn record(&self, step: &'static str) {
        *self.0.lock().unwrap().entry(step).or_default() += 1;
    }
}

impl DataPlaneApi for MemoryData {
    async fn ensure_target_learners(
        &self,
        _migration: &MigrationRecord,
        _graph: &GraphDefinition,
    ) -> Result<(), ControllerError> {
        self.record("ensure");
        Ok(())
    }

    async fn copy_snapshot(
        &self,
        _migration: &MigrationRecord,
    ) -> Result<SnapshotFence, ControllerError> {
        self.record("copy");
        Ok(SnapshotFence {
            index: 50,
            checksum: [0x55; 32],
        })
    }

    async fn catch_up(&self, _migration: &MigrationRecord) -> Result<u64, ControllerError> {
        self.record("catchup");
        Ok(75)
    }

    async fn commit_membership(
        &self,
        _migration: &MigrationRecord,
    ) -> Result<u64, ControllerError> {
        self.record("membership");
        Ok(80)
    }

    async fn activate_target_replicas(
        &self,
        _migration: &MigrationRecord,
    ) -> Result<(), ControllerError> {
        self.record("activate");
        Ok(())
    }

    async fn cleanup_safe(&self, _migration: &MigrationRecord) -> Result<bool, ControllerError> {
        self.record("pins");
        Ok(true)
    }

    async fn delete_source_replicas(
        &self,
        _migration: &MigrationRecord,
    ) -> Result<(), ControllerError> {
        self.record("delete_source");
        Ok(())
    }

    async fn delete_target_learners(
        &self,
        _migration: &MigrationRecord,
    ) -> Result<(), ControllerError> {
        self.record("delete_target");
        Ok(())
    }
}

fn initial_state() -> CatalogState {
    let graph = GraphDefinition::new(
        7,
        "social",
        1,
        TopologyDefinition::new(
            DeploymentMode::PrimaryReplica,
            99,
            1,
            1,
            vec![Placement::new(10, 1, vec![10, 11, 12]).unwrap()],
        )
        .unwrap(),
        BackendProfile::new(
            "rocksdb",
            BTreeMap::new(),
            BTreeMap::new(),
            AdapterRequirement::HotPluggableReplica,
            1,
        )
        .unwrap(),
    )
    .unwrap();
    let migration = MigrationRecord::new_shard(
        100,
        7,
        10,
        1,
        2,
        vec![10, 11, 12],
        vec![11, 12, 13],
        9,
        1_000,
    )
    .unwrap();
    let mut state = CatalogState::new();
    state
        .apply(CatalogCommand::create_graph(1, 0, graph))
        .unwrap();
    state
        .apply(CatalogCommand::create_migration(2, 1, migration))
        .unwrap();
    state
}

#[tokio::test]
async fn level_reconciliation_converges_and_publishes_one_authoritative_lineage() {
    let catalog = MemoryCatalog(Arc::new(Mutex::new(initial_state())));
    let data = MemoryData::default();
    let reconciler = Reconciler::new(catalog.clone(), data.clone(), 10).unwrap();
    for offset in 0..7 {
        assert!(matches!(
            reconciler.reconcile(100, 2_000 + offset).await.unwrap(),
            ReconcileOutcome::Advanced { .. }
        ));
    }
    assert_eq!(
        reconciler.reconcile(100, 3_000).await.unwrap(),
        ReconcileOutcome::Terminal {
            migration_id: 100,
            state: MigrationState::Cleaned,
        }
    );
    let state = catalog.load().await.unwrap();
    assert_eq!(
        state.migration(100).unwrap().state(),
        MigrationState::Cleaned
    );
    assert_eq!(state.graph(7).unwrap().topology().epoch(), 2);
    assert_eq!(state.lineages().len(), 1);
    assert_eq!(state.lineage(7, 10, 1).unwrap().target_epoch(), 2);
    assert_eq!(
        *data.0.lock().unwrap(),
        BTreeMap::from([
            ("activate", 1),
            ("catchup", 1),
            ("copy", 1),
            ("delete_source", 1),
            ("ensure", 1),
            ("membership", 1),
            ("pins", 1),
        ])
    );
}

#[tokio::test]
async fn an_expired_controller_cannot_advance_a_workflow() {
    let catalog = MemoryCatalog(Arc::new(Mutex::new(initial_state())));
    let error = Reconciler::new(catalog, MemoryData::default(), 8)
        .unwrap()
        .reconcile(100, 2_000)
        .await
        .unwrap_err();
    assert_eq!(
        error,
        ControllerError::ExpiredOwner {
            active: 9,
            actual: 8
        }
    );
}

#[tokio::test]
async fn committed_migration_waits_for_durable_transaction_backup_or_cdc_pins() {
    let mut initial = initial_state();
    initial
        .apply(CatalogCommand::acquire_retention_pin(
            900,
            initial.revision(),
            RetentionPin::new(901, 7, 10, 1, RetentionPinKind::Transaction, 5_000).unwrap(),
        ))
        .unwrap();
    let catalog = MemoryCatalog(Arc::new(Mutex::new(initial)));
    let data = MemoryData::default();
    let reconciler = Reconciler::new(catalog.clone(), data.clone(), 10).unwrap();
    for offset in 0..5 {
        reconciler.reconcile(100, 2_000 + offset).await.unwrap();
    }
    assert_eq!(
        catalog
            .load()
            .await
            .unwrap()
            .migration(100)
            .unwrap()
            .state(),
        MigrationState::Committed
    );
    assert_eq!(
        reconciler.reconcile(100, 4_000).await.unwrap(),
        ReconcileOutcome::Waiting {
            migration_id: 100,
            state: MigrationState::Committed,
        }
    );
    assert!(!data.0.lock().unwrap().contains_key("delete_source"));
    assert!(matches!(
        reconciler.reconcile(100, 5_000).await.unwrap(),
        ReconcileOutcome::Advanced {
            to: MigrationState::Cleaning,
            ..
        }
    ));
}

#[tokio::test]
async fn catchup_and_cleanup_are_level_gates_not_wall_clock_assumptions() {
    #[derive(Clone)]
    struct WaitingData;
    impl DataPlaneApi for WaitingData {
        async fn ensure_target_learners(
            &self,
            _: &MigrationRecord,
            _: &GraphDefinition,
        ) -> Result<(), ControllerError> {
            Ok(())
        }
        async fn copy_snapshot(
            &self,
            _: &MigrationRecord,
        ) -> Result<SnapshotFence, ControllerError> {
            unreachable!()
        }
        async fn catch_up(&self, _: &MigrationRecord) -> Result<u64, ControllerError> {
            Ok(49)
        }
        async fn commit_membership(&self, _: &MigrationRecord) -> Result<u64, ControllerError> {
            unreachable!()
        }
        async fn activate_target_replicas(
            &self,
            _: &MigrationRecord,
        ) -> Result<(), ControllerError> {
            Ok(())
        }
        async fn cleanup_safe(&self, _: &MigrationRecord) -> Result<bool, ControllerError> {
            Ok(false)
        }
        async fn delete_source_replicas(&self, _: &MigrationRecord) -> Result<(), ControllerError> {
            unreachable!()
        }
        async fn delete_target_learners(&self, _: &MigrationRecord) -> Result<(), ControllerError> {
            unreachable!()
        }
    }

    let mut state = initial_state();
    state
        .apply(CatalogCommand::advance_migration(
            30,
            state.revision(),
            100,
            1,
            MigrationState::Copying,
            control_plane::MigrationProgress::new(9, 1_100).unwrap(),
        ))
        .unwrap();
    state
        .apply(CatalogCommand::advance_migration(
            31,
            state.revision(),
            100,
            2,
            MigrationState::CatchingUp,
            control_plane::MigrationProgress::new(9, 1_200)
                .unwrap()
                .with_snapshot(50, [0x55; 32])
                .unwrap(),
        ))
        .unwrap();
    let catalog = MemoryCatalog(Arc::new(Mutex::new(state)));
    let outcome = Reconciler::new(catalog.clone(), WaitingData, 10)
        .unwrap()
        .reconcile(100, 2_000)
        .await
        .unwrap();
    assert_eq!(
        outcome,
        ReconcileOutcome::Waiting {
            migration_id: 100,
            state: MigrationState::CatchingUp,
        }
    );
    assert_eq!(
        catalog
            .load()
            .await
            .unwrap()
            .migration(100)
            .unwrap()
            .state(),
        MigrationState::CatchingUp
    );
}
