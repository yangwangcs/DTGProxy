use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use control_plane::{
    BackendProfile, CatalogCommand, CatalogState, DeploymentMode, GraphDefinition, MigrationRecord,
    MigrationState, Placement, TopologyDefinition,
};
use controller::{CatalogApi, ControllerError, DataPlaneApi, Reconciler, SnapshotFence};
use storage_api::AdapterRequirement;

#[derive(Clone, Copy)]
enum CrashPoint {
    BeforeCommit,
    AfterCommit,
}

struct CatalogInner {
    state: CatalogState,
    proposal: usize,
    fail_at: usize,
    point: CrashPoint,
    fired: bool,
}

#[derive(Clone)]
struct CrashCatalog(Arc<Mutex<CatalogInner>>);

impl CatalogApi for CrashCatalog {
    async fn load(&self) -> Result<CatalogState, ControllerError> {
        Ok(self.0.lock().unwrap().state.clone())
    }

    async fn propose(&self, command: CatalogCommand) -> Result<(), ControllerError> {
        let mut inner = self.0.lock().unwrap();
        inner.proposal += 1;
        let crash = inner.proposal == inner.fail_at && !inner.fired;
        if crash && matches!(inner.point, CrashPoint::BeforeCommit) {
            inner.fired = true;
            return Err(ControllerError::Catalog("injected before commit".into()));
        }
        let receipt = inner
            .state
            .apply(command)
            .map_err(|error| ControllerError::Catalog(error.to_string()))?;
        if crash {
            inner.fired = true;
            return Err(ControllerError::Catalog(
                "lost response after commit".into(),
            ));
        }
        let _ = receipt;
        Ok(())
    }
}

#[derive(Default)]
struct Receipts {
    effects: BTreeSet<&'static str>,
}

#[derive(Clone, Default)]
struct ReceiptData(Arc<Mutex<Receipts>>);

impl ReceiptData {
    fn apply(&self, step: &'static str) {
        self.0.lock().unwrap().effects.insert(step);
    }
}

impl DataPlaneApi for ReceiptData {
    async fn ensure_target_learners(
        &self,
        _: &MigrationRecord,
        _: &GraphDefinition,
    ) -> Result<(), ControllerError> {
        self.apply("ensure");
        Ok(())
    }
    async fn copy_snapshot(&self, _: &MigrationRecord) -> Result<SnapshotFence, ControllerError> {
        self.apply("copy");
        Ok(SnapshotFence {
            index: 50,
            checksum: [0x55; 32],
        })
    }
    async fn catch_up(&self, _: &MigrationRecord) -> Result<u64, ControllerError> {
        self.apply("catchup");
        Ok(75)
    }
    async fn commit_membership(&self, _: &MigrationRecord) -> Result<u64, ControllerError> {
        self.apply("membership");
        Ok(80)
    }
    async fn activate_target_replicas(&self, _: &MigrationRecord) -> Result<(), ControllerError> {
        self.apply("activate");
        Ok(())
    }
    async fn cleanup_safe(&self, _: &MigrationRecord) -> Result<bool, ControllerError> {
        Ok(true)
    }
    async fn delete_source_replicas(&self, _: &MigrationRecord) -> Result<(), ControllerError> {
        self.apply("delete_source");
        Ok(())
    }
    async fn delete_target_learners(&self, _: &MigrationRecord) -> Result<(), ControllerError> {
        self.apply("delete_target");
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

async fn run_case(fail_at: usize, point: CrashPoint) {
    let catalog = CrashCatalog(Arc::new(Mutex::new(CatalogInner {
        state: initial_state(),
        proposal: 0,
        fail_at,
        point,
        fired: false,
    })));
    let data = ReceiptData::default();
    for turn in 0..20_u64 {
        let reconciler = Reconciler::new(catalog.clone(), data.clone(), 10 + turn).unwrap();
        let _ = reconciler.reconcile(100, 2_000 + turn).await;
        if catalog
            .load()
            .await
            .unwrap()
            .migration(100)
            .unwrap()
            .state()
            == MigrationState::Cleaned
        {
            break;
        }
    }
    let state = catalog.load().await.unwrap();
    assert_eq!(
        state.migration(100).unwrap().state(),
        MigrationState::Cleaned
    );
    assert_eq!(state.lineages().len(), 1);
    assert_eq!(state.graph(7).unwrap().topology().epoch(), 2);
    assert_eq!(
        data.0.lock().unwrap().effects,
        BTreeSet::from([
            "activate",
            "catchup",
            "copy",
            "delete_source",
            "ensure",
            "membership",
        ]),
    );
}

#[tokio::test]
async fn every_transition_converges_across_controller_crashes_before_and_after_meta_commit() {
    for proposal in 1..=7 {
        run_case(proposal, CrashPoint::BeforeCommit).await;
        run_case(proposal, CrashPoint::AfterCommit).await;
    }
}
