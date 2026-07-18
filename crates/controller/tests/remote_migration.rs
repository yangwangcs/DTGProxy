use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use cluster_protocol::proto::node_admin_service_server::NodeAdminServiceServer;
use cluster_protocol::proto::shard_service_server::ShardServiceServer;
use control_plane::{
    BackendProfile, CatalogCommand, CatalogState, DeploymentMode, GraphDefinition, MigrationRecord,
    MigrationState, Placement, TopologyDefinition,
};
use controller::{CatalogApi, ControllerError, Reconciler, RemoteDataPlane};
use data_node::{
    DataNodeGrpcService, DataNodeHost, DataRaftRuntime, HostError, NodeConfig, NodeIdentity,
    ReplicaKey, ReplicaRole, ReplicaSpec, TransportSecurity,
};
use raft_command::{ApplyPreparedV1, CommandBodyV1, CommandEnvelopeV1};
use storage_api::{AdapterRequirement, Keyspace, LogicalKey, Mutation, PreparedMutationBatch};
use tempfile::tempdir;
use temporal_types::TransactionTime;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

const MIGRATION_ID: u128 = 100;

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

fn localhost(port: u16) -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)
}

fn free_address() -> SocketAddr {
    TcpListener::bind(localhost(0))
        .unwrap()
        .local_addr()
        .unwrap()
}

fn node_config(root: &std::path::Path, node_id: u64, service: SocketAddr) -> NodeConfig {
    NodeConfig::new(
        NodeIdentity::new([0x92; 16], node_id).unwrap(),
        service,
        service,
        root,
        vec![localhost(7001)],
        TransportSecurity::LoopbackPlaintext,
        BTreeMap::new(),
    )
    .unwrap()
}

fn source_spec() -> ReplicaSpec {
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
    .unwrap()
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
            vec![Placement::new(10, 1, vec![1]).unwrap()],
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
    let migration =
        MigrationRecord::new_shard(MIGRATION_ID, 7, 10, 1, 2, vec![1], vec![2], 9, 1_000).unwrap();
    let mut state = CatalogState::new();
    state
        .apply(CatalogCommand::create_graph(1, 0, graph))
        .unwrap();
    state
        .apply(CatalogCommand::create_migration(2, 1, migration))
        .unwrap();
    state
}

fn seed_command() -> Vec<u8> {
    CommandEnvelopeV1::new(
        10,
        1,
        700,
        CommandBodyV1::ApplyPrepared(ApplyPreparedV1 {
            commit_ts: TransactionTime::new(100, 0),
            batch: PreparedMutationBatch {
                shard_id: 10,
                txn_id: 701,
                mutations: vec![Mutation::put(
                    0,
                    LogicalKey::in_keyspace(Keyspace::Current, b"vertex/42".to_vec()),
                    b"temporal-payload".to_vec(),
                )],
            },
        }),
    )
    .encode()
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

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn remote_controller_moves_a_live_shard_and_preserves_temporal_state() {
    let source_root = tempdir().unwrap();
    let target_root = tempdir().unwrap();
    let source_service = free_address();
    let target_service = free_address();
    let source_raft = free_address();
    let target_raft = free_address();
    let source = Arc::new(
        DataNodeHost::open(node_config(source_root.path(), 1, source_service), 64)
            .await
            .unwrap(),
    );
    let target = Arc::new(
        DataNodeHost::open(node_config(target_root.path(), 2, target_service), 64)
            .await
            .unwrap(),
    );
    let key = ReplicaKey::new(7, 10).unwrap();
    source.ensure_replica(source_spec()).await.unwrap();
    source.campaign(key).await.unwrap();
    source.propose(key, 1, 700, seed_command()).await.unwrap();

    let source_runtime = DataRaftRuntime::start(
        Arc::clone(&source),
        source_raft,
        &BTreeMap::from([(2, target_raft)]),
        64,
        Duration::from_millis(10),
    )
    .await
    .unwrap();
    let target_runtime = DataRaftRuntime::start(
        Arc::clone(&target),
        target_raft,
        &BTreeMap::from([(1, source_raft)]),
        64,
        Duration::from_millis(10),
    )
    .await
    .unwrap();
    let (source_stop, source_server) = serve_data(Arc::clone(&source), source_service).await;
    let (target_stop, target_server) = serve_data(Arc::clone(&target), target_service).await;

    let catalog = MemoryCatalog(Arc::new(Mutex::new(initial_state())));
    let data = RemoteDataPlane::new(
        [0x92; 16],
        10,
        BTreeMap::from([(1, source_service), (2, target_service)]),
        Duration::from_secs(2),
    )
    .unwrap();
    let reconciler = Reconciler::new(catalog.clone(), data, 10).unwrap();
    let mut last_error = None;
    tokio::time::timeout(Duration::from_secs(20), async {
        for turn in 0..1_000_u64 {
            match reconciler.reconcile(MIGRATION_ID, 2_000 + turn).await {
                Ok(_) => last_error = None,
                Err(error) => last_error = Some(error),
            }
            if catalog
                .load()
                .await
                .unwrap()
                .migration(MIGRATION_ID)
                .unwrap()
                .state()
                == MigrationState::Cleaned
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!(
            "migration did not converge; last error: {last_error:?}; source={:?}; target={:?}",
            source.status(key).await,
            target.status(key).await
        );
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "migration timed out; last error: {last_error:?}; inspect state with a longer test bound"
        )
    });

    let state = catalog.load().await.unwrap();
    assert_eq!(state.graph(7).unwrap().topology().epoch(), 2);
    assert_eq!(
        state.graph(7).unwrap().topology().placements()[0].voters(),
        &[2]
    );
    assert_eq!(state.lineage(7, 10, 1).unwrap().target_epoch(), 2);
    assert!(matches!(
        source.status(key).await,
        Err(HostError::UnknownReplica { .. })
    ));
    assert_eq!(target.status(key).await.unwrap().placement_epoch(), 2);
    let logical_key = LogicalKey::in_keyspace(Keyspace::Current, b"vertex/42".to_vec());
    assert_eq!(
        target.multi_get(key, vec![logical_key]).await.unwrap(),
        vec![Some(b"temporal-payload".to_vec())]
    );

    let _ = source_stop.send(());
    let _ = target_stop.send(());
    source_server.await.unwrap().unwrap();
    target_server.await.unwrap().unwrap();
    source_runtime.shutdown().await.unwrap();
    target_runtime.shutdown().await.unwrap();
    match Arc::try_unwrap(source) {
        Ok(host) => host.shutdown().await.unwrap(),
        Err(_) => panic!("source host is still retained"),
    }
    match Arc::try_unwrap(target) {
        Ok(host) => host.shutdown().await.unwrap(),
        Err(_) => panic!("target host is still retained"),
    }
}
