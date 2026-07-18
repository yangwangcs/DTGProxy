use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use cluster_protocol::CLUSTER_PROTOCOL_VERSION;
use cluster_protocol::proto::meta_service_server::MetaService;
use cluster_protocol::proto::{
    AllocateTimestampRequest, GetCatalogRequest, ProposeRequest, RequestContext,
    WatchCatalogRequest,
};
use control_plane::{
    BackendProfile, CatalogCommand, CatalogState, DeploymentMode, GraphDefinition, Placement,
    TopologyDefinition,
};
use meta_node::{MetaNodeService, MetaRaftReplica, ReplicatedTso};
use storage_api::AdapterRequirement;
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
