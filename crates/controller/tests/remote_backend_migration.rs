use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use cluster_protocol::proto::node_admin_service_server::NodeAdminServiceServer;
use cluster_protocol::proto::shard_service_server::ShardServiceServer;
use control_plane::{
    BackendMigrationRecord, BackendMigrationState, BackendProfile, CatalogCommand, CatalogState,
    DeploymentMode, GraphDefinition, Placement, TopologyDefinition,
};
use controller::{
    BackendDataPlaneApi, BackendReconciler, CatalogApi, ControllerError, RemoteDataPlane,
};
use data_node::{
    DataNodeGrpcService, DataNodeHost, NodeConfig, NodeIdentity, ReplicaKey, ReplicaRole,
    ReplicaSpec, TransportSecurity,
};
use raft_command::{ApplyPreparedV1, CommandBodyV1, CommandEnvelopeV1};
use storage_api::{AdapterRequirement, Keyspace, LogicalKey, Mutation, PreparedMutationBatch};
use temporal_types::TransactionTime;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

const MIGRATION_ID: u128 = 800;

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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remote_controller_hot_swaps_a_live_backend_and_preserves_temporal_writes() {
    run_backend_migration(profile(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires a disposable stateful Sidecar configured through DTGPROXY_TEST_* variables"]
async fn remote_controller_migrates_rocksdb_to_a_live_external_sidecar() {
    let provider = std::env::var("DTGPROXY_TEST_TARGET_PROVIDER").unwrap();
    let sidecar_endpoint = std::env::var("DTGPROXY_TEST_SIDECAR_ENDPOINT").unwrap();
    let mut parameters = BTreeMap::from([("sidecar_endpoint".into(), sidecar_endpoint)]);
    match provider.as_str() {
        "postgresql" => {
            parameters.insert("pool_size".into(), "2".into());
        }
        "neo4j" => {
            parameters.insert(
                "endpoint".into(),
                std::env::var("DTGPROXY_TEST_NEO4J_ENDPOINT").unwrap(),
            );
            parameters.insert(
                "database".into(),
                std::env::var("DTGPROXY_TEST_NEO4J_DATABASE").unwrap_or_else(|_| "neo4j".into()),
            );
            parameters.insert(
                "username".into(),
                std::env::var("DTGPROXY_TEST_NEO4J_USERNAME").unwrap_or_else(|_| "neo4j".into()),
            );
        }
        value => panic!("unsupported test provider {value}"),
    }
    let target = BackendProfile::new(
        provider,
        parameters,
        BTreeMap::new(),
        AdapterRequirement::HotPluggableReplica,
        2,
    )
    .unwrap();
    run_backend_migration(target).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remote_controller_aborts_targets_prepared_before_catalog_receipts_were_committed() {
    let root = tempfile::tempdir().unwrap();
    let service_address = free_address();
    let config = node_config(root.path(), service_address);
    let host = Arc::new(DataNodeHost::open(config, 64).await.unwrap());
    let key = ReplicaKey::new(7, 10).unwrap();
    host.ensure_replica(
        ReplicaSpec::new(
            7,
            10,
            1,
            vec![1],
            ReplicaRole::Voter,
            1,
            1,
            "graph-7-shard-10",
        )
        .unwrap(),
    )
    .await
    .unwrap();
    host.campaign(key).await.unwrap();
    let (stop, server) = serve_data(Arc::clone(&host), service_address).await;
    let catalog = MemoryCatalog(Arc::new(Mutex::new(initial_state(profile(2)))));
    let data = RemoteDataPlane::new(
        [0x44; 16],
        9,
        BTreeMap::from([(1, service_address)]),
        Duration::from_secs(2),
    )
    .unwrap();

    let before = catalog.load().await.unwrap();
    let migration = before.backend_migration(MIGRATION_ID).unwrap().clone();
    let graph = before.graph(7).unwrap().clone();
    data.prepare_target(&migration, &graph).await.unwrap();
    assert!(matches!(
        host.backend_runtime_status(key).await.unwrap().slot(),
        data_node::BackendSlotState::DualApplying { .. }
    ));

    catalog
        .propose(CatalogCommand::advance_backend_migration(
            3,
            before.revision(),
            MIGRATION_ID,
            migration.state_revision(),
            BackendMigrationState::Aborting,
            10,
            2_000,
            Vec::new(),
        ))
        .await
        .unwrap();
    BackendReconciler::new(catalog.clone(), data, 10)
        .unwrap()
        .reconcile(MIGRATION_ID, 2_001)
        .await
        .unwrap();

    assert_eq!(
        catalog
            .load()
            .await
            .unwrap()
            .backend_migration(MIGRATION_ID)
            .unwrap()
            .state(),
        BackendMigrationState::Aborted
    );
    assert!(matches!(
        host.backend_runtime_status(key).await.unwrap().slot(),
        data_node::BackendSlotState::Active { generation: 1, .. }
    ));
    let _ = stop.send(());
    server.await.unwrap().unwrap();
}

async fn run_backend_migration(target: BackendProfile) {
    let root = tempfile::tempdir().unwrap();
    let service_address = free_address();
    let config = node_config(root.path(), service_address);
    let host = Arc::new(DataNodeHost::open(config.clone(), 64).await.unwrap());
    let key = ReplicaKey::new(7, 10).unwrap();
    host.ensure_replica(
        ReplicaSpec::new(
            7,
            10,
            1,
            vec![1],
            ReplicaRole::Voter,
            1,
            1,
            "graph-7-shard-10",
        )
        .unwrap(),
    )
    .await
    .unwrap();
    host.campaign(key).await.unwrap();
    host.propose(key, 1, 100, put_command(100, b"before", b"v1"))
        .await
        .unwrap();

    let (stop, server) = serve_data(Arc::clone(&host), service_address).await;
    let catalog = MemoryCatalog(Arc::new(Mutex::new(initial_state(target))));
    let data = RemoteDataPlane::new(
        [0x44; 16],
        9,
        BTreeMap::from([(1, service_address)]),
        Duration::from_secs(2),
    )
    .unwrap();
    let reconciler = BackendReconciler::new(catalog.clone(), data, 10).unwrap();

    let mut wrote_during_dual = false;
    let mut last_error = None;
    tokio::time::timeout(Duration::from_secs(15), async {
        for turn in 0..500_u64 {
            match reconciler.reconcile(MIGRATION_ID, 2_000 + turn).await {
                Ok(_) => last_error = None,
                Err(error) => last_error = Some(error),
            }
            let state = catalog.load().await.unwrap();
            let migration = state.backend_migration(MIGRATION_ID).unwrap();
            if migration.state() == BackendMigrationState::DualApplying && !wrote_during_dual {
                host.propose(key, 1, 101, put_command(101, b"during", b"v2"))
                    .await
                    .unwrap();
                wrote_during_dual = true;
            }
            if migration.state() == BackendMigrationState::SourceRetired {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("backend migration did not converge: {last_error:?}");
    })
    .await
    .unwrap();

    assert!(wrote_during_dual);
    let state = catalog.load().await.unwrap();
    assert_eq!(state.graph(7).unwrap().backend().generation(), 2);
    assert_eq!(host.status(key).await.unwrap().backend_generation(), 2);
    assert_eq!(read(&host, key, b"before").await, Some(b"v1".to_vec()));
    assert_eq!(read(&host, key, b"during").await, Some(b"v2".to_vec()));

    let _ = stop.send(());
    server.await.unwrap().unwrap();
    drop(host);

    let reopened = Arc::new(DataNodeHost::open(config, 64).await.unwrap());
    assert_eq!(reopened.status(key).await.unwrap().backend_generation(), 2);
    assert_eq!(read(&reopened, key, b"before").await, Some(b"v1".to_vec()));
    assert_eq!(read(&reopened, key, b"during").await, Some(b"v2".to_vec()));
}

fn initial_state(target: BackendProfile) -> CatalogState {
    let source = profile(1);
    let graph = GraphDefinition::new(
        7,
        "social",
        1,
        TopologyDefinition::new(
            DeploymentMode::PrimaryReplica,
            9,
            1,
            1,
            vec![Placement::new(10, 1, vec![1]).unwrap()],
        )
        .unwrap(),
        source.clone(),
    )
    .unwrap();
    let mut state = CatalogState::new();
    state
        .apply(CatalogCommand::create_graph(1, 0, graph))
        .unwrap();
    state
        .apply(CatalogCommand::create_backend_migration(
            2,
            1,
            BackendMigrationRecord::new(MIGRATION_ID, 7, source, target, 9, 1_000).unwrap(),
        ))
        .unwrap();
    state
}

fn profile(generation: u64) -> BackendProfile {
    BackendProfile::new(
        "rocksdb",
        BTreeMap::new(),
        BTreeMap::new(),
        AdapterRequirement::HotPluggableReplica,
        generation,
    )
    .unwrap()
}

fn node_config(root: &std::path::Path, service: SocketAddr) -> NodeConfig {
    NodeConfig::new(
        NodeIdentity::new([0x44; 16], 1).unwrap(),
        service,
        service,
        root,
        vec![SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 7001)],
        TransportSecurity::LoopbackPlaintext,
        BTreeMap::new(),
    )
    .unwrap()
}

fn free_address() -> SocketAddr {
    TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
        .unwrap()
        .local_addr()
        .unwrap()
}

async fn serve_data(
    host: Arc<DataNodeHost>,
    address: SocketAddr,
) -> (
    oneshot::Sender<()>,
    tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
) {
    let listener = tokio::net::TcpListener::bind(address).await.unwrap();
    let service = DataNodeGrpcService::new(host);
    let (shutdown, receiver) = oneshot::channel();
    let join = tokio::spawn(async move {
        Server::builder()
            .add_service(ShardServiceServer::new(service.clone()))
            .add_service(NodeAdminServiceServer::new(service))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = receiver.await;
            })
            .await
    });
    (shutdown, join)
}

fn put_command(request_id: u128, key: &[u8], value: &[u8]) -> Vec<u8> {
    CommandEnvelopeV1::new(
        10,
        1,
        request_id,
        CommandBodyV1::ApplyPrepared(ApplyPreparedV1 {
            commit_ts: TransactionTime::new(request_id as i64, 0),
            batch: PreparedMutationBatch {
                shard_id: 10,
                txn_id: request_id + 1_000,
                mutations: vec![Mutation::put(
                    0,
                    LogicalKey::in_keyspace(Keyspace::Current, key.to_vec()),
                    value.to_vec(),
                )],
            },
        }),
    )
    .encode()
    .unwrap()
}

async fn read(host: &DataNodeHost, replica: ReplicaKey, key: &[u8]) -> Option<Vec<u8>> {
    host.multi_get(
        replica,
        vec![LogicalKey::in_keyspace(Keyspace::Current, key.to_vec())],
    )
    .await
    .unwrap()
    .into_iter()
    .next()
    .unwrap()
}
