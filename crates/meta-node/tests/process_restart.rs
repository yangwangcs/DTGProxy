use std::collections::BTreeMap;
use std::net::TcpListener;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use analytics_ledger::{
    AnalyticsJobId, GraphProjectionScope, JobCommand, JobSpec, LedgerState, ProjectionLimits,
};
use cluster_protocol::CLUSTER_PROTOCOL_VERSION;
use cluster_protocol::proto::meta_service_client::MetaServiceClient;
use cluster_protocol::proto::{
    AcquireAnalyticsGcLeaseRequest, AllocateTimestampRequest, ListAnalyticsJobTombstonesRequest,
    ProposeAnalyticsJobRequest, ProposeRequest, RequestContext,
};
use control_plane::{
    BackendProfile, CatalogCommand, DeploymentMode, GraphDefinition, Placement, TopologyDefinition,
};
use storage_api::AdapterRequirement;
use tempfile::tempdir;
use temporal_types::{TransactionTime, ValidTime};

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn now_ms() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
}

fn context(request_id: u128) -> RequestContext {
    RequestContext {
        protocol_version: CLUSTER_PROTOCOL_VERSION,
        cluster_id: vec![0x93; 16],
        request_id: request_id.to_be_bytes().to_vec(),
        deadline_unix_ms: now_ms() + 60_000,
    }
}

fn graph() -> GraphDefinition {
    GraphDefinition::new(
        7,
        "social",
        1,
        TopologyDefinition::new(
            DeploymentMode::SharedNothing,
            99,
            128,
            1,
            vec![
                Placement::new(10, 1, vec![1]).unwrap(),
                Placement::new(20, 1, vec![1]).unwrap(),
            ],
        )
        .unwrap(),
        BackendProfile::new(
            "rocksdb",
            BTreeMap::from([("path".into(), "data/graph-7".into())]),
            BTreeMap::new(),
            AdapterRequirement::HotPluggableReplica,
            1,
        )
        .unwrap(),
    )
    .unwrap()
}

fn analytics_job(job_id: u128, submission_request_id: u128) -> JobSpec {
    JobSpec::new(
        AnalyticsJobId::new(job_id).unwrap(),
        submission_request_id,
        7,
        1,
        1,
        1,
        1,
        TransactionTime::new(1_000, 0),
        GraphProjectionScope::Snapshot {
            valid_time: ValidTime::from_micros(900),
        },
        "dtg.graph.pageRank",
        "1.0.0",
        "dtg.analytics-native",
        "1.0.0",
        Vec::new(),
        [9; 32],
        ProjectionLimits::new(100, 100, 1 << 20).unwrap(),
    )
    .unwrap()
}

fn write_config(path: &Path, data_directory: &Path, service_port: u16, raft_port: u16) {
    let json = serde_json::json!({
        "version": 1,
        "cluster_id": "93939393939393939393939393939393",
        "node_id": 7,
        "voters": [7],
        "listen_address": format!("127.0.0.1:{service_port}"),
        "advertise_address": format!("127.0.0.1:{service_port}"),
        "raft_listen_address": format!("127.0.0.1:{raft_port}"),
        "peer_addresses": {},
        "data_directory": data_directory,
        "security": { "mode": "loopback_plaintext" },
        "timestamp_reservation_size": 16,
        "maximum_future_drift_ms": 5000,
        "shutdown_grace_ms": 5000
    });
    std::fs::write(path, serde_json::to_vec_pretty(&json).unwrap()).unwrap();
}

fn spawn_meta(config: &Path) -> Child {
    Command::new(env!("CARGO_BIN_EXE_dtgproxy-meta"))
        .args(["--config", config.to_str().unwrap()])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap()
}

async fn wait_for_meta(endpoint: &str) -> MetaServiceClient<tonic::transport::Channel> {
    for _ in 0..200 {
        if let Ok(client) = MetaServiceClient::connect(endpoint.to_owned()).await {
            return client;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("Meta process did not become ready at {endpoint}");
}

fn terminate(child: &mut Child) {
    assert!(
        Command::new("kill")
            .args(["-TERM", &child.id().to_string()])
            .status()
            .unwrap()
            .success()
    );
    assert!(child.wait().unwrap().success());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn meta_process_restart_never_reuses_a_committed_timestamp_lease() {
    let temporary = tempdir().unwrap();
    let config = temporary.path().join("meta.json");
    let service_port = free_port();
    let raft_port = free_port();
    write_config(
        &config,
        &temporary.path().join("data"),
        service_port,
        raft_port,
    );
    let endpoint = format!("http://127.0.0.1:{service_port}");

    let mut first_process = spawn_meta(&config);
    let mut client = wait_for_meta(&endpoint).await;
    let first = client
        .allocate_timestamp(AllocateTimestampRequest {
            context: Some(context(101)),
            count: 3,
            observed_physical_ms: 0,
        })
        .await
        .unwrap()
        .into_inner();
    drop(client);
    terminate(&mut first_process);

    let mut second_process = spawn_meta(&config);
    let mut client = wait_for_meta(&endpoint).await;
    let second = client
        .allocate_timestamp(AllocateTimestampRequest {
            context: Some(context(102)),
            count: 3,
            observed_physical_ms: 0,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(
        (second.first_physical_ms, second.first_logical)
            > (
                first.lease_high_water_physical_ms,
                first.lease_high_water_logical
            )
    );
    terminate(&mut second_process);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn meta_process_restart_advances_the_committed_analytics_gc_epoch() {
    let temporary = tempdir().unwrap();
    let config = temporary.path().join("meta-gc.json");
    let service_port = free_port();
    let raft_port = free_port();
    write_config(
        &config,
        &temporary.path().join("gc-data"),
        service_port,
        raft_port,
    );
    let endpoint = format!("http://127.0.0.1:{service_port}");

    let mut first_process = spawn_meta(&config);
    let mut client = wait_for_meta(&endpoint).await;
    let first = client
        .acquire_analytics_gc_lease(AcquireAnalyticsGcLeaseRequest {
            context: Some(context(201)),
            gateway_id: 10,
        })
        .await
        .unwrap()
        .into_inner();
    drop(client);
    terminate(&mut first_process);

    let mut second_process = spawn_meta(&config);
    let mut client = wait_for_meta(&endpoint).await;
    let second = client
        .acquire_analytics_gc_lease(AcquireAnalyticsGcLeaseRequest {
            context: Some(context(202)),
            gateway_id: 11,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(second.owner_term > first.owner_term);
    assert!(second.gc_epoch > first.gc_epoch);
    terminate(&mut second_process);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn meta_process_restart_preserves_unacknowledged_tombstone_and_gc_epoch() {
    let temporary = tempdir().unwrap();
    let config = temporary.path().join("meta-tombstone.json");
    let service_port = free_port();
    let raft_port = free_port();
    write_config(
        &config,
        &temporary.path().join("tombstone-data"),
        service_port,
        raft_port,
    );
    let endpoint = format!("http://127.0.0.1:{service_port}");
    let submitted_at = now_ms();
    let job = AnalyticsJobId::new(701).unwrap();

    let mut first_process = spawn_meta(&config);
    let mut client = wait_for_meta(&endpoint).await;
    client
        .propose(ProposeRequest {
            context: Some(context(301)),
            command: CatalogCommand::create_graph(301, 0, graph())
                .encode()
                .unwrap(),
        })
        .await
        .unwrap();
    for (request_id, command) in [
        (
            302,
            JobCommand::submit(302, analytics_job(701, 70_001), submitted_at).unwrap(),
        ),
        (303, JobCommand::cancel(303, job, 1).unwrap()),
        (
            304,
            JobCommand::prune_terminal(304, job, 2, submitted_at + 1).unwrap(),
        ),
    ] {
        client
            .propose_analytics_job(ProposeAnalyticsJobRequest {
                context: Some(context(request_id)),
                command: command.encode().unwrap(),
            })
            .await
            .unwrap();
    }
    let first_lease = client
        .acquire_analytics_gc_lease(AcquireAnalyticsGcLeaseRequest {
            context: Some(context(305)),
            gateway_id: 77,
        })
        .await
        .unwrap()
        .into_inner();
    let before = client
        .list_analytics_job_tombstones(ListAnalyticsJobTombstonesRequest {
            context: Some(context(306)),
            after_job_id: Vec::new(),
            limit: 8,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(before.tombstones.len(), 1);
    let (_, decoded_job, tombstone) =
        LedgerState::decode_tombstone(&before.tombstones[0].record).unwrap();
    assert_eq!(decoded_job, job);
    assert!(!tombstone.artifacts_reclaimed());
    drop(client);
    terminate(&mut first_process);

    let mut second_process = spawn_meta(&config);
    let mut client = wait_for_meta(&endpoint).await;
    let after = client
        .list_analytics_job_tombstones(ListAnalyticsJobTombstonesRequest {
            context: Some(context(307)),
            after_job_id: Vec::new(),
            limit: 8,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(after.ledger_revision, before.ledger_revision);
    assert_eq!(after.tombstones[0].record, before.tombstones[0].record);
    let second_lease = client
        .acquire_analytics_gc_lease(AcquireAnalyticsGcLeaseRequest {
            context: Some(context(308)),
            gateway_id: 78,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(second_lease.gc_epoch > first_lease.gc_epoch);
    terminate(&mut second_process);
}
