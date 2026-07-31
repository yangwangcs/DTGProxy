use dtg_control::{
    ActionCommand, ActionState, BackendClass, BindingRole, CatalogCommand, GraphId, PlacementEpoch,
    ProviderKind, ReconcileAction, ReplicaBinding, ReplicaBindingRecord, ReplicaId, RetentionPin,
    ShardId, ShardPlacement, Version,
};
use dtg_execution::cluster_protocol::proto::{
    BoundedPayload, CatalogWatchRequest, RequestContext, ShardContext, StatusCode,
    TransactionRequest, meta_service_server::MetaService,
};
use dtg_execution::cluster_protocol::{PROTOCOL_MAJOR, checksum_bytes};
use dtg_meta::{MetaConfig, MetaProcess};
use dtg_storage::DurabilityPolicy;
use dtg_transaction::{CommitResolution, TransactionId};
use tokio_stream::StreamExt;
use tonic::Request;

#[tokio::test]
async fn meta_rpc_resolve_committed_is_idempotent_and_cannot_be_reversed() {
    let root = tempfile::tempdir().unwrap();
    let process = MetaProcess::open(MetaConfig::for_test(root.path(), 7, 1).unwrap())
        .await
        .unwrap();
    let service = process.rpc_service();
    let transaction_id = 91_u128.to_be_bytes().to_vec();

    let reserved = MetaService::submit_transaction(
        &service,
        Request::new(transaction_request(transaction_id.clone(), 2)),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(reserved.code, StatusCode::Ok as i32);
    let commit_time = i64::from_be_bytes(reserved.details.unwrap().body.try_into().unwrap());

    let recovered = MetaService::submit_transaction(
        &service,
        Request::new(transaction_request(transaction_id.clone(), 4)),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(recovered.code, StatusCode::Ok as i32);
    assert_eq!(
        i64::from_be_bytes(recovered.details.unwrap().body.try_into().unwrap()),
        commit_time
    );

    let committed = MetaService::submit_transaction(
        &service,
        Request::new(transaction_request(transaction_id.clone(), 5)),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(committed.code, StatusCode::Ok as i32);
    assert_eq!(
        i64::from_be_bytes(committed.details.unwrap().body.try_into().unwrap()),
        commit_time
    );

    let repeated = MetaService::submit_transaction(
        &service,
        Request::new(transaction_request(transaction_id.clone(), 5)),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(repeated.code, StatusCode::Ok as i32);

    let aborted = MetaService::submit_transaction(
        &service,
        Request::new(transaction_request(transaction_id, 3)),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(aborted.code, StatusCode::Conflict as i32);
}

fn transaction_request(transaction_id: Vec<u8>, operation: i32) -> TransactionRequest {
    let body = vec![operation as u8];
    TransactionRequest {
        context: Some(ShardContext {
            request: Some(RequestContext {
                protocol_major: PROTOCOL_MAJOR,
                protocol_minor: 1,
                cluster_id: 7_u64.to_be_bytes().to_vec(),
                request_id: 92_u128.to_be_bytes().to_vec(),
                deadline_unix_ms: u64::MAX,
                trace_context: Vec::new(),
            }),
            graph_id: 1,
            shard_id: 1,
            placement_epoch: 1,
            backend_generation: 1,
            catalog_version: 1,
        }),
        transaction_id,
        operation,
        idempotency_key: 93_u128.to_be_bytes().to_vec(),
        payload: Some(BoundedPayload {
            format_version: 1,
            declared_len: body.len() as u64,
            item_count: 1,
            checksum: checksum_bytes(&body).to_vec(),
            body,
        }),
    }
}

#[tokio::test]
async fn meta_process_replays_catalog_and_timestamps_from_fjall_consensus() {
    let root = tempfile::tempdir().unwrap();
    let config = MetaConfig::for_test(root.path(), 7, 1).unwrap();
    let first_transaction = TransactionId::new(41).unwrap();

    let process = MetaProcess::open(config.clone()).await.unwrap();
    process.propose(put_placement()).await.unwrap();
    let first_start = process
        .timestamps()
        .allocate_start_time(first_transaction)
        .await
        .unwrap();
    let first_commit = process
        .timestamps()
        .reserve_commit_time(first_transaction)
        .await
        .unwrap();
    process
        .timestamps()
        .resolve_commit_time(first_transaction, first_commit, CommitResolution::Committed)
        .await
        .unwrap();
    drop(process);

    let restarted = MetaProcess::open(config).await.unwrap();
    assert_eq!(restarted.catalog_version().await, Version::new(1));
    restarted
        .propose(CatalogCommand::pin_retention(
            Version::new(1),
            GraphId::new(1).unwrap(),
            ShardId::new(1).unwrap(),
            dtg_control::BackendGeneration::new(1).unwrap(),
            RetentionPin::new("restart-proof".into()).unwrap(),
        ))
        .await
        .unwrap();
    assert_eq!(restarted.catalog_version().await, Version::new(2));
    assert_eq!(
        restarted
            .timestamps()
            .allocate_start_time(first_transaction)
            .await
            .unwrap(),
        first_start
    );
    assert!(
        restarted
            .timestamps()
            .allocate_start_time(TransactionId::new(42).unwrap())
            .await
            .unwrap()
            > first_commit
    );
    assert_eq!(restarted.rpc_service().protocol_major(), 2);
}

#[tokio::test]
async fn meta_raft_recovers_authoritative_action_state_after_restart() {
    let root = tempfile::tempdir().unwrap();
    let config = MetaConfig::for_test(root.path(), 7, 1).unwrap();
    let action = ReconcileAction::TransferLeader {
        graph_id: GraphId::new(1).unwrap(),
        shard_id: ShardId::new(1).unwrap(),
        placement_epoch: PlacementEpoch::new(1).unwrap(),
        from: ReplicaId::new(1).unwrap(),
        to: ReplicaId::new(2).unwrap(),
    };
    let process = MetaProcess::open(config.clone()).await.unwrap();
    let queued = process
        .apply_action_command(ActionCommand::enqueue(Version::new(0), action))
        .await
        .unwrap();
    drop(process);

    let restarted = MetaProcess::open(config).await.unwrap();
    assert_eq!(
        restarted
            .action_record(queued.action_id())
            .await
            .unwrap()
            .state(),
        &ActionState::Pending { attempt: 0 }
    );
}

#[tokio::test]
async fn meta_streams_versioned_catalog_snapshots() {
    let root = tempfile::tempdir().unwrap();
    let process = MetaProcess::open(MetaConfig::for_test(root.path(), 7, 1).unwrap())
        .await
        .unwrap();
    let mut stream = process
        .rpc_service()
        .watch_catalog(Request::new(CatalogWatchRequest {
            request: Some(RequestContext {
                protocol_major: 2,
                protocol_minor: 0,
                cluster_id: 7_u64.to_be_bytes().to_vec(),
                request_id: 77_u128.to_be_bytes().to_vec(),
                deadline_unix_ms: 1_900_000_000_000,
                trace_context: Vec::new(),
            }),
            after_revision: 0,
        }))
        .await
        .unwrap()
        .into_inner();

    let initial = stream.next().await.unwrap().unwrap();
    assert_eq!(initial.revision, 0);
    process.propose(put_placement()).await.unwrap();
    let changed = stream.next().await.unwrap().unwrap();
    assert_eq!(changed.revision, 1);
    assert_eq!(changed.payload.unwrap().item_count, 1);
}

fn put_placement() -> CatalogCommand {
    let backend_class = BackendClass::with_durability(
        ProviderKind::Fjall,
        1,
        1,
        DurabilityPolicy::DurableCommit,
        ["point"],
    )
    .unwrap();
    let binding = ReplicaBinding::builder()
        .cluster_id(7)
        .graph_id(1)
        .shard_id(1)
        .placement_epoch(1)
        .replica_id(1)
        .backend_generation(1)
        .backend_class_digest(backend_class.digest())
        .provider_kind(ProviderKind::Fjall)
        .contract_version(1)
        .layout_version(1)
        .capability_digest(backend_class.required_capabilities().digest())
        .namespace_id("graph-1-shard-1")
        .endpoint_profile_ref("local-fjall")
        .credential_ref("env://DTG_FJALL_ROOT")
        .role(BindingRole::Active)
        .build()
        .unwrap();
    CatalogCommand::put_placement(
        Version::new(0),
        ShardPlacement {
            graph_id: GraphId::new(1).unwrap(),
            shard_id: ShardId::new(1).unwrap(),
            placement_epoch: PlacementEpoch::new(1).unwrap(),
            active_generation: dtg_control::BackendGeneration::new(1).unwrap(),
            backend_class: backend_class.clone(),
            replicas: vec![ReplicaBindingRecord::new(binding, backend_class).unwrap()],
        },
    )
}
