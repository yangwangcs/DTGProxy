use std::collections::{BTreeMap, HashMap};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use adapter_registry::AdapterRegistry;
use adapter_rocksdb::{RocksAdapter, RocksAdapterFactory};
use adapter_sidecar::{
    SidecarRestoreBackend, SidecarService, TcpSidecarServerConfig,
    spawn_stateful_tcp_sidecar_server,
};
use cluster_protocol::CLUSTER_PROTOCOL_VERSION;
use cluster_protocol::proto::node_admin_service_server::NodeAdminService;
use cluster_protocol::proto::{
    BackendLifecyclePhase, BackendProfileSpec, BeginBackendDualApplyRequest,
    FinishBackendMigrationRequest, GetBackendStatusRequest, PrepareBackendTargetRequest,
    RequestContext, ShardContext,
};
use data_node::{
    DataNodeGrpcService, DataNodeHost, NodeConfig, NodeIdentity, ReplicaKey, ReplicaRole,
    ReplicaSpec, TransportSecurity,
};
use raft_command::{ApplyPreparedV1, CommandBodyV1, CommandEnvelopeV1};
use storage_api::{
    AdapterRequirement, Keyspace, LogicalKey, Mutation, PreparedMutationBatch, StorageAdapter,
};
use temporal_types::TransactionTime;
use tonic::Request;

fn now_ms() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
}

fn config(root: &std::path::Path) -> NodeConfig {
    let loopback = |port| SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);
    NodeConfig::new(
        NodeIdentity::new([0x71; 16], 7).unwrap(),
        loopback(7101),
        loopback(7101),
        root,
        vec![loopback(7001)],
        TransportSecurity::LoopbackPlaintext,
        BTreeMap::new(),
    )
    .unwrap()
}

fn context(request_id: u128) -> ShardContext {
    ShardContext {
        request: Some(RequestContext {
            protocol_version: CLUSTER_PROTOCOL_VERSION,
            cluster_id: vec![0x71; 16],
            request_id: request_id.to_be_bytes().to_vec(),
            deadline_unix_ms: now_ms() + 60_000,
        }),
        graph_id: 1,
        shard_id: 11,
        placement_epoch: 3,
    }
}

fn command(request_id: u128, commit: i64, value: &[u8]) -> Vec<u8> {
    CommandEnvelopeV1::new(
        11,
        3,
        request_id,
        CommandBodyV1::ApplyPrepared(ApplyPreparedV1 {
            commit_ts: TransactionTime::new(commit, 0),
            batch: PreparedMutationBatch {
                shard_id: 11,
                txn_id: request_id + 1_000,
                mutations: vec![Mutation::put(
                    0,
                    LogicalKey::in_keyspace(Keyspace::Current, b"vertex/1".to_vec()),
                    value.to_vec(),
                )],
            },
        }),
    )
    .encode()
    .unwrap()
}

#[tokio::test(flavor = "current_thread")]
async fn admin_backend_workflow_dual_applies_cuts_over_and_survives_restart() {
    let temporary = tempfile::tempdir().unwrap();
    let key = ReplicaKey::new(1, 11).unwrap();
    let host = Arc::new(
        DataNodeHost::open(config(temporary.path()), 32)
            .await
            .unwrap(),
    );
    host.ensure_replica(
        ReplicaSpec::new(
            1,
            11,
            3,
            vec![7],
            ReplicaRole::Voter,
            5,
            7,
            "graph-1-shard-11",
        )
        .unwrap(),
    )
    .await
    .unwrap();
    host.campaign(key).await.unwrap();
    host.propose(key, 3, 900, command(900, 100, b"before-migration"))
        .await
        .unwrap();
    let service = DataNodeGrpcService::new(Arc::clone(&host));

    let prepared = service
        .prepare_backend_target(Request::new(PrepareBackendTargetRequest {
            context: Some(context(901)),
            operation_id: 901_u128.to_be_bytes().to_vec(),
            target_generation: 8,
            target_profile: Some(BackendProfileSpec {
                provider: "rocksdb".into(),
                instance_id: "graph-1-shard-11-generation-8".into(),
                public_parameters: HashMap::from([("path".into(), "adapter-generation-8".into())]),
                credential_refs: HashMap::new(),
            }),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(prepared.fence_index > 0);
    assert_eq!(prepared.target_profile_digest.len(), 32);

    let begin = service
        .begin_backend_dual_apply(Request::new(BeginBackendDualApplyRequest {
            context: Some(context(902)),
            operation_id: 902_u128.to_be_bytes().to_vec(),
            source_generation: 7,
            target_generation: 8,
            target_profile_digest: prepared.target_profile_digest.clone(),
            fence_index: prepared.fence_index,
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(!begin.duplicate);

    host.propose(key, 3, 903, command(903, 200, b"during-dual-apply"))
        .await
        .unwrap();
    let dual = service
        .get_backend_status(Request::new(GetBackendStatusRequest {
            context: Some(context(904)),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(dual.phase, BackendLifecyclePhase::DualApplying as i32);
    assert_eq!(dual.logical_backend, "rocksdb");
    assert_eq!(dual.loaded_backends, vec!["rocksdb"]);
    assert_eq!(
        dual.synchronized_index,
        dual.status.as_ref().unwrap().applied_index
    );

    service
        .cutover_backend(Request::new(FinishBackendMigrationRequest {
            context: Some(context(905)),
            operation_id: 905_u128.to_be_bytes().to_vec(),
            source_generation: 7,
            target_generation: 8,
            target_profile_digest: prepared.target_profile_digest,
        }))
        .await
        .unwrap();
    let active = service
        .get_backend_status(Request::new(GetBackendStatusRequest {
            context: Some(context(906)),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(active.phase, BackendLifecyclePhase::Active as i32);
    assert_eq!(active.logical_backend, "rocksdb");
    assert_eq!(active.loaded_backends, vec!["rocksdb"]);
    assert_eq!(active.source_generation, 8);
    assert_eq!(active.status.unwrap().backend_generation, 8);

    drop(service);
    Arc::try_unwrap(host)
        .ok()
        .unwrap()
        .shutdown()
        .await
        .unwrap();
    let reopened = DataNodeHost::open(config(temporary.path()), 32)
        .await
        .unwrap();
    let status = reopened.status(key).await.unwrap();
    assert_eq!(status.backend_generation(), 8);
    let value = reopened
        .multi_get(
            key,
            vec![LogicalKey::in_keyspace(
                Keyspace::Current,
                b"vertex/1".to_vec(),
            )],
        )
        .await
        .unwrap();
    assert_eq!(value, vec![Some(b"during-dual-apply".to_vec())]);
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn admin_backend_status_serializes_heterogeneous_dual_apply_and_cutover() {
    let temporary = tempfile::tempdir().unwrap();
    let sidecar_backend =
        Arc::new(RocksAdapter::open(temporary.path().join("postgres-target")).unwrap());
    let mut restore_registry = AdapterRegistry::new();
    restore_registry
        .register(Arc::new(RocksAdapterFactory))
        .unwrap();
    let restore_backend = SidecarRestoreBackend::new(
        Arc::new(restore_registry),
        "rocksdb",
        AdapterRequirement::HotPluggableReplica,
        sidecar_backend.descriptor(),
    );
    let sidecar = spawn_stateful_tcp_sidecar_server(
        TcpSidecarServerConfig::new(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)),
        Arc::new(SidecarService::new(sidecar_backend, Some(restore_backend))),
    )
    .unwrap();
    let key = ReplicaKey::new(1, 11).unwrap();
    let host = Arc::new(
        DataNodeHost::open(config(temporary.path()), 32)
            .await
            .unwrap(),
    );
    host.ensure_replica(
        ReplicaSpec::new(
            1,
            11,
            3,
            vec![7],
            ReplicaRole::Voter,
            5,
            7,
            "graph-1-shard-11",
        )
        .unwrap(),
    )
    .await
    .unwrap();
    host.campaign(key).await.unwrap();
    host.propose(key, 3, 1_900, command(1_900, 100, b"before-migration"))
        .await
        .unwrap();
    let service = DataNodeGrpcService::new(Arc::clone(&host));

    let prepared = service
        .prepare_backend_target(Request::new(PrepareBackendTargetRequest {
            context: Some(context(1_901)),
            operation_id: 1_901_u128.to_be_bytes().to_vec(),
            target_generation: 8,
            target_profile: Some(BackendProfileSpec {
                provider: "sidecar".into(),
                instance_id: "graph-1-shard-11-postgresql-generation-8".into(),
                public_parameters: HashMap::from([
                    ("endpoint".into(), sidecar.local_addr().to_string()),
                    ("pool_size".into(), "1".into()),
                    ("target_provider".into(), "postgresql".into()),
                    (
                        "target.path".into(),
                        temporary
                            .path()
                            .join("postgres-restored-target")
                            .to_string_lossy()
                            .into_owned(),
                    ),
                ]),
                credential_refs: HashMap::from([("password".into(), "postgres-ref".into())]),
            }),
        }))
        .await
        .unwrap()
        .into_inner();
    service
        .begin_backend_dual_apply(Request::new(BeginBackendDualApplyRequest {
            context: Some(context(1_902)),
            operation_id: 1_902_u128.to_be_bytes().to_vec(),
            source_generation: 7,
            target_generation: 8,
            target_profile_digest: prepared.target_profile_digest.clone(),
            fence_index: prepared.fence_index,
        }))
        .await
        .unwrap();
    let dual = service
        .get_backend_status(Request::new(GetBackendStatusRequest {
            context: Some(context(1_903)),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(dual.phase, BackendLifecyclePhase::DualApplying as i32);
    assert_eq!(dual.logical_backend, "rocksdb");
    assert_eq!(dual.loaded_backends, vec!["postgresql", "rocksdb"]);

    service
        .cutover_backend(Request::new(FinishBackendMigrationRequest {
            context: Some(context(1_904)),
            operation_id: 1_904_u128.to_be_bytes().to_vec(),
            source_generation: 7,
            target_generation: 8,
            target_profile_digest: prepared.target_profile_digest,
        }))
        .await
        .unwrap();
    let active = service
        .get_backend_status(Request::new(GetBackendStatusRequest {
            context: Some(context(1_905)),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(active.phase, BackendLifecyclePhase::Active as i32);
    assert_eq!(active.logical_backend, "postgresql");
    assert_eq!(active.loaded_backends, vec!["postgresql"]);

    drop(service);
    Arc::try_unwrap(host)
        .ok()
        .unwrap()
        .shutdown()
        .await
        .unwrap();
    sidecar.shutdown().unwrap();
}
