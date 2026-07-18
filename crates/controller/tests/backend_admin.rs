use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use control_plane::{
    BackendProfile, CatalogCommand, CatalogState, DeploymentMode, GraphDefinition, Placement,
    TopologyDefinition,
};
use controller::{
    BackendTargetSpec, CatalogApi, ControllerError, abort_backend_migration,
    start_backend_migration,
};
use storage_api::AdapterRequirement;

const MIGRATION_ID: u128 = 0x1234;

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

#[tokio::test]
async fn admin_starts_an_idempotent_next_generation_backend_migration() {
    let state = Arc::new(Mutex::new(initial_state()));
    let catalog = MemoryCatalog(Arc::clone(&state));
    let target = BackendTargetSpec::new(
        "postgresql",
        BTreeMap::from([
            ("sidecar_endpoint.shard.10".into(), "127.0.0.1:9711".into()),
            ("pool_size".into(), "8".into()),
        ]),
        BTreeMap::new(),
    )
    .unwrap();

    let created = start_backend_migration(&catalog, MIGRATION_ID, 7, target.clone(), 9, 1_000)
        .await
        .unwrap();
    assert_eq!(created.migration_id(), MIGRATION_ID);
    assert_eq!(created.source().provider(), "rocksdb");
    assert_eq!(created.source().generation(), 1);
    assert_eq!(created.target().provider(), "postgresql");
    assert_eq!(created.target().generation(), 2);
    assert_eq!(
        created.target().requirement(),
        AdapterRequirement::HotPluggableReplica
    );

    let revision = state.lock().unwrap().revision();
    let duplicate = start_backend_migration(&catalog, MIGRATION_ID, 7, target, 9, 2_000)
        .await
        .unwrap();
    assert_eq!(duplicate, created);
    assert_eq!(state.lock().unwrap().revision(), revision);
}

#[tokio::test]
async fn admin_rejects_reusing_a_migration_id_for_a_different_target() {
    let catalog = MemoryCatalog(Arc::new(Mutex::new(initial_state())));
    let postgres = BackendTargetSpec::new(
        "postgresql",
        BTreeMap::from([("sidecar_endpoint".into(), "127.0.0.1:9711".into())]),
        BTreeMap::new(),
    )
    .unwrap();
    start_backend_migration(&catalog, MIGRATION_ID, 7, postgres, 9, 1_000)
        .await
        .unwrap();

    let neo4j = BackendTargetSpec::new(
        "neo4j",
        BTreeMap::from([("sidecar_endpoint".into(), "127.0.0.1:9712".into())]),
        BTreeMap::new(),
    )
    .unwrap();
    let error = start_backend_migration(&catalog, MIGRATION_ID, 7, neo4j, 9, 2_000)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("different backend migration"));
}

#[tokio::test]
async fn admin_requests_an_idempotent_abort_for_a_pre_cutover_migration() {
    let state = Arc::new(Mutex::new(initial_state()));
    let catalog = MemoryCatalog(Arc::clone(&state));
    let target = BackendTargetSpec::new(
        "postgresql",
        BTreeMap::from([("sidecar_endpoint".into(), "127.0.0.1:9711".into())]),
        BTreeMap::new(),
    )
    .unwrap();
    start_backend_migration(&catalog, MIGRATION_ID, 7, target, 9, 1_000)
        .await
        .unwrap();

    let aborting = abort_backend_migration(&catalog, MIGRATION_ID, 9, 2_000)
        .await
        .unwrap();
    assert_eq!(
        aborting.state(),
        control_plane::BackendMigrationState::Aborting
    );
    let revision = state.lock().unwrap().revision();
    let duplicate = abort_backend_migration(&catalog, MIGRATION_ID, 9, 3_000)
        .await
        .unwrap();
    assert_eq!(duplicate, aborting);
    assert_eq!(state.lock().unwrap().revision(), revision);
}

fn initial_state() -> CatalogState {
    let mut state = CatalogState::new();
    let topology = TopologyDefinition::new(
        DeploymentMode::PrimaryReplica,
        99,
        128,
        1,
        vec![Placement::new(10, 1, vec![1]).unwrap()],
    )
    .unwrap();
    let backend = BackendProfile::new(
        "rocksdb",
        BTreeMap::from([("path".into(), "backend-generation-1".into())]),
        BTreeMap::new(),
        AdapterRequirement::HotPluggableReplica,
        1,
    )
    .unwrap();
    let graph = GraphDefinition::new(7, "graph-7", 1, topology, backend).unwrap();
    state
        .apply(CatalogCommand::create_graph(1, 0, graph))
        .unwrap();
    state
}
