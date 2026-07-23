use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use analytics_ledger::{
    AnalyticsJobId, GraphProjectionScope, JobCommand, JobSpec, JobState, LedgerState,
    ProjectionLimits,
};
use cluster_protocol::CLUSTER_PROTOCOL_VERSION;
use cluster_protocol::proto::meta_service_server::MetaService;
use cluster_protocol::proto::{
    AcquireAnalyticsGcLeaseRequest, AcquireControllerLeaseRequest, AllocateTimestampRequest,
    GetAnalyticsJobRequest, GetCatalogRequest, ListAnalyticsJobTombstonesRequest,
    ListAnalyticsJobsRequest, ListClaimableAnalyticsJobsRequest, ProposeAnalyticsJobRequest,
    ProposeRequest, RequestContext, WatchCatalogRequest,
};
use control_plane::{
    BackendProfile, CatalogCommand, CatalogState, DeploymentMode, GraphDefinition, Placement,
    TopologyDefinition,
};
use meta_node::{MetaNodeService, MetaRaftReplica, ReplicatedTso};
use storage_api::AdapterRequirement;
use temporal_types::{TransactionTime, ValidTime};
use timestamp_oracle::ManualClock;
use tokio::sync::Mutex;
use tokio_stream::StreamExt;
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

fn context(request_id: u128) -> RequestContext {
    RequestContext {
        protocol_version: CLUSTER_PROTOCOL_VERSION,
        cluster_id: vec![0x91; 16],
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

fn analytics_job(job_id: u128, request_id: u128) -> JobSpec {
    JobSpec::new(
        AnalyticsJobId::new(job_id).unwrap(),
        request_id,
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

fn elected_service(root: &std::path::Path) -> MetaNodeService {
    let mut replica =
        MetaRaftReplica::open(1, &[1], root.join("raft"), root.join("state")).unwrap();
    replica.campaign().unwrap();
    for _ in 0..32 {
        assert!(replica.drain_ready().unwrap().is_empty());
        if replica.is_leader() {
            break;
        }
        replica.tick();
    }
    assert!(replica.is_leader());
    MetaNodeService::new(
        [0x91; 16],
        Arc::new(Mutex::new(replica)),
        Arc::new(ReplicatedTso::new(Arc::new(ManualClock::new(1_000_000)), 8, 1_000_000).unwrap()),
    )
}

#[tokio::test(flavor = "current_thread")]
async fn catalog_propose_get_watch_and_duplicate_share_one_committed_revision() {
    let temporary = tempfile::tempdir().unwrap();
    let service = elected_service(temporary.path());
    let command = CatalogCommand::create_graph(101, 0, graph())
        .encode()
        .unwrap();
    let proposed = service
        .propose(Request::new(ProposeRequest {
            context: Some(context(101)),
            command: command.clone(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(proposed.revision, 1);
    assert!(!proposed.duplicate);

    let duplicate = service
        .propose(Request::new(ProposeRequest {
            context: Some(context(101)),
            command,
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(duplicate.revision, 1);
    assert!(duplicate.duplicate);

    let snapshot = service
        .get_catalog(Request::new(GetCatalogRequest {
            context: Some(context(102)),
            minimum_revision: 1,
        }))
        .await
        .unwrap()
        .into_inner()
        .snapshot
        .unwrap();
    assert_eq!(snapshot.revision, 1);
    assert_eq!(
        CatalogState::decode_snapshot(&snapshot.payload)
            .unwrap()
            .revision(),
        1
    );

    let mut watch = service
        .watch_catalog(Request::new(WatchCatalogRequest {
            context: Some(context(103)),
            after_revision: 0,
        }))
        .await
        .unwrap()
        .into_inner();
    let event = watch.next().await.unwrap().unwrap();
    assert_eq!(event.revision, 1);
}

#[tokio::test(flavor = "current_thread")]
async fn timestamp_rpc_commits_lease_before_returning_disjoint_batches() {
    let temporary = tempfile::tempdir().unwrap();
    let service = elected_service(temporary.path());
    let first = service
        .allocate_timestamp(Request::new(AllocateTimestampRequest {
            context: Some(context(201)),
            count: 3,
            observed_physical_ms: 0,
        }))
        .await
        .unwrap()
        .into_inner();
    let second = service
        .allocate_timestamp(Request::new(AllocateTimestampRequest {
            context: Some(context(202)),
            count: 3,
            observed_physical_ms: 0,
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(
        (second.first_physical_ms, second.first_logical)
            > (first.first_physical_ms, first.first_logical)
    );
    assert_eq!(first.count, 3);
    assert_eq!(second.count, 3);
    assert!(first.lease_high_water_physical_ms >= first.first_physical_ms);
}

#[tokio::test(flavor = "current_thread")]
async fn one_controller_owns_the_current_meta_term_lease() {
    let temporary = tempfile::tempdir().unwrap();
    let service = elected_service(temporary.path());
    let first = service
        .acquire_controller_lease(Request::new(AcquireControllerLeaseRequest {
            context: Some(context(301)),
            controller_id: 10,
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(first.owner_term > 0);
    assert!(first.lease_expires_unix_ms > now_ms());
    let renewed = service
        .acquire_controller_lease(Request::new(AcquireControllerLeaseRequest {
            context: Some(context(302)),
            controller_id: 10,
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(renewed.owner_term, first.owner_term);
    let conflict = service
        .acquire_controller_lease(Request::new(AcquireControllerLeaseRequest {
            context: Some(context(303)),
            controller_id: 11,
        }))
        .await
        .unwrap_err();
    assert_eq!(conflict.code(), tonic::Code::ResourceExhausted);
}

#[tokio::test(flavor = "current_thread")]
async fn one_gateway_owns_the_analytics_gc_lease_for_the_current_meta_term() {
    let temporary = tempfile::tempdir().unwrap();
    let service = elected_service(temporary.path());
    let first = service
        .acquire_analytics_gc_lease(Request::new(AcquireAnalyticsGcLeaseRequest {
            context: Some(context(351)),
            gateway_id: 10,
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(first.owner_term > 0);
    assert!(first.gc_epoch > 0);
    assert!(first.lease_expires_unix_ms > now_ms());
    let renewed = service
        .acquire_analytics_gc_lease(Request::new(AcquireAnalyticsGcLeaseRequest {
            context: Some(context(352)),
            gateway_id: 10,
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(renewed.owner_term, first.owner_term);
    assert_eq!(renewed.gc_epoch, first.gc_epoch);
    let conflict = service
        .acquire_analytics_gc_lease(Request::new(AcquireAnalyticsGcLeaseRequest {
            context: Some(context(353)),
            gateway_id: 11,
        }))
        .await
        .unwrap_err();
    assert_eq!(conflict.code(), tonic::Code::ResourceExhausted);
    assert_eq!(
        conflict
            .metadata()
            .get("dtgproxy-reason")
            .and_then(|value| value.to_str().ok()),
        Some("analytics_gc_lease_owned")
    );
}

#[tokio::test(flavor = "current_thread")]
async fn analytics_rpc_commits_gets_and_lists_claimable_jobs() {
    let temporary = tempfile::tempdir().unwrap();
    let service = elected_service(temporary.path());
    service
        .propose(Request::new(ProposeRequest {
            context: Some(context(401)),
            command: CatalogCommand::create_graph(401, 0, graph())
                .encode()
                .unwrap(),
        }))
        .await
        .unwrap();
    let job = AnalyticsJobId::new(501).unwrap();
    let submitted = service
        .propose_analytics_job(Request::new(ProposeAnalyticsJobRequest {
            context: Some(context(402)),
            command: JobCommand::submit(402, analytics_job(501, 50_001), now_ms())
                .unwrap()
                .encode()
                .unwrap(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(submitted.ledger_revision, 1);
    assert_eq!(submitted.job_revision, 1);
    assert_eq!(submitted.canonical_job_id, job.value().to_be_bytes());

    let retry = service
        .propose_analytics_job(Request::new(ProposeAnalyticsJobRequest {
            context: Some(context(405)),
            command: JobCommand::submit(405, analytics_job(502, 50_001), now_ms() + 1)
                .unwrap()
                .encode()
                .unwrap(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(retry.request_duplicate);
    assert_eq!(retry.ledger_revision, 2);
    assert_eq!(retry.canonical_job_id, job.value().to_be_bytes());

    let fetched = service
        .get_analytics_job(Request::new(GetAnalyticsJobRequest {
            context: Some(context(403)),
            job_id: job.value().to_be_bytes().to_vec(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(fetched.checksum, crc32fast::hash(&fetched.record));
    let (revision, record) = LedgerState::decode_job(&fetched.record).unwrap();
    assert_eq!(revision, 2);
    assert_eq!(record.state(), JobState::Queued);

    let queued = service
        .list_claimable_analytics_jobs(Request::new(ListClaimableAnalyticsJobsRequest {
            context: Some(context(404)),
            now_unix_ms: now_ms(),
            after_job_id: Vec::new(),
            limit: 10,
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(queued.candidates.len(), 1);
    assert_eq!(queued.candidates[0].job_revision, 1);

    let listed = service
        .list_analytics_jobs(Request::new(ListAnalyticsJobsRequest {
            context: Some(context(406)),
            after_job_id: Vec::new(),
            limit: 10,
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(listed.ledger_revision, 2);
    assert_eq!(listed.jobs.len(), 1);
    assert_eq!(listed.jobs[0].job_id, job.value().to_be_bytes());
    assert_eq!(
        listed.jobs[0].checksum,
        crc32fast::hash(&listed.jobs[0].record)
    );
    let (_, listed_record) = LedgerState::decode_job(&listed.jobs[0].record).unwrap();
    assert_eq!(listed_record.state(), JobState::Queued);
}

#[tokio::test(flavor = "current_thread")]
async fn analytics_tombstones_are_paginated_and_reclamation_ack_is_gc_lease_fenced() {
    let temporary = tempfile::tempdir().unwrap();
    let service = elected_service(temporary.path());
    service
        .propose(Request::new(ProposeRequest {
            context: Some(context(451)),
            command: CatalogCommand::create_graph(451, 0, graph())
                .encode()
                .unwrap(),
        }))
        .await
        .unwrap();
    let job = AnalyticsJobId::new(551).unwrap();
    let submitted_at = now_ms();
    for (request_id, command) in [
        (
            452,
            JobCommand::submit(452, analytics_job(551, 55_001), submitted_at).unwrap(),
        ),
        (453, JobCommand::cancel(453, job, 1).unwrap()),
        (
            454,
            JobCommand::prune_terminal(454, job, 2, submitted_at + 1).unwrap(),
        ),
    ] {
        service
            .propose_analytics_job(Request::new(ProposeAnalyticsJobRequest {
                context: Some(context(request_id)),
                command: command.encode().unwrap(),
            }))
            .await
            .unwrap();
    }

    let page = service
        .list_analytics_job_tombstones(Request::new(ListAnalyticsJobTombstonesRequest {
            context: Some(context(455)),
            after_job_id: Vec::new(),
            limit: 1,
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(page.tombstones.len(), 1);
    assert_eq!(page.tombstones[0].job_id, job.value().to_be_bytes());
    assert_eq!(
        page.tombstones[0].checksum,
        crc32fast::hash(&page.tombstones[0].record)
    );
    assert_eq!(page.next_job_id, job.value().to_be_bytes());
    let (revision, decoded_job, tombstone) =
        LedgerState::decode_tombstone(&page.tombstones[0].record).unwrap();
    assert_eq!(revision, page.ledger_revision);
    assert_eq!(decoded_job, job);
    assert!(!tombstone.artifacts_reclaimed());
    let terminal_page = service
        .list_analytics_job_tombstones(Request::new(ListAnalyticsJobTombstonesRequest {
            context: Some(context(459)),
            after_job_id: page.next_job_id.clone(),
            limit: 1,
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(terminal_page.tombstones.is_empty());
    assert!(terminal_page.next_job_id.is_empty());
    let malformed = service
        .list_analytics_job_tombstones(Request::new(ListAnalyticsJobTombstonesRequest {
            context: Some(context(460)),
            after_job_id: vec![1; 15],
            limit: 1,
        }))
        .await
        .unwrap_err();
    assert_eq!(malformed.code(), tonic::Code::InvalidArgument);

    let lease = service
        .acquire_analytics_gc_lease(Request::new(AcquireAnalyticsGcLeaseRequest {
            context: Some(context(456)),
            gateway_id: 77,
        }))
        .await
        .unwrap()
        .into_inner();
    let stale = service
        .propose_analytics_job(Request::new(ProposeAnalyticsJobRequest {
            context: Some(context(457)),
            command: JobCommand::acknowledge_artifacts_reclaimed(
                457,
                job,
                2,
                78,
                lease.gc_epoch,
                submitted_at + 2,
            )
            .unwrap()
            .encode()
            .unwrap(),
        }))
        .await
        .unwrap_err();
    assert_eq!(stale.code(), tonic::Code::FailedPrecondition);

    service
        .propose_analytics_job(Request::new(ProposeAnalyticsJobRequest {
            context: Some(context(458)),
            command: JobCommand::acknowledge_artifacts_reclaimed(
                458,
                job,
                2,
                77,
                lease.gc_epoch,
                submitted_at + 2,
            )
            .unwrap()
            .encode()
            .unwrap(),
        }))
        .await
        .unwrap();
}
