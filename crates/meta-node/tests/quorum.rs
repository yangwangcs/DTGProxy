use std::collections::{BTreeMap, BTreeSet};

use control_plane::{
    BackendProfile, CatalogCommand, DeploymentMode, GraphDefinition, Placement, TopologyDefinition,
};
use meta_node::MetaRaftReplica;
use raft::eraftpb::Message;
use storage_api::AdapterRequirement;

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
                Placement::new(10, 1, vec![1, 2, 3]).unwrap(),
                Placement::new(20, 1, vec![1, 2, 3]).unwrap(),
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

fn open_three(root: &std::path::Path) -> BTreeMap<u64, MetaRaftReplica> {
    [1_u64, 2, 3]
        .into_iter()
        .map(|node_id| {
            (
                node_id,
                MetaRaftReplica::open(
                    node_id,
                    &[1, 2, 3],
                    root.join(format!("node-{node_id}/raft")),
                    root.join(format!("node-{node_id}/state")),
                )
                .unwrap(),
            )
        })
        .collect()
}

fn pump(replicas: &mut BTreeMap<u64, MetaRaftReplica>, blocked: &BTreeSet<(u64, u64)>) {
    for _ in 0..512 {
        let node_ids = replicas.keys().copied().collect::<Vec<_>>();
        let mut messages = Vec::<Message>::new();
        for node_id in node_ids {
            messages.extend(replicas.get_mut(&node_id).unwrap().drain_ready().unwrap());
        }
        if messages.is_empty() && replicas.values().all(|replica| !replica.has_ready()) {
            return;
        }
        for message in messages {
            if blocked.contains(&(message.from, message.to)) {
                continue;
            }
            if let Some(target) = replicas.get_mut(&message.to) {
                target.step(message).unwrap();
            }
        }
    }
    panic!("Meta quorum did not quiesce");
}

fn isolate(node_id: u64) -> BTreeSet<(u64, u64)> {
    [1_u64, 2, 3]
        .into_iter()
        .filter(|peer| *peer != node_id)
        .flat_map(|peer| [(node_id, peer), (peer, node_id)])
        .collect()
}

#[test]
fn quorum_commit_leader_change_idempotence_and_minority_refusal() {
    let root = tempfile::tempdir().unwrap();
    let mut replicas = open_three(root.path());
    let no_blocks = BTreeSet::new();
    replicas.get_mut(&1).unwrap().campaign().unwrap();
    pump(&mut replicas, &no_blocks);
    assert!(replicas[&1].is_leader());

    let create = CatalogCommand::create_graph(101, 0, graph())
        .encode()
        .unwrap();
    replicas
        .get_mut(&1)
        .unwrap()
        .propose(create.clone())
        .unwrap();
    pump(&mut replicas, &no_blocks);
    assert!(
        replicas
            .values()
            .all(|replica| replica.state().catalog().revision() == 1)
    );

    let old_leader_isolated = isolate(1);
    for _ in 0..12 {
        replicas.get_mut(&2).unwrap().tick();
        replicas.get_mut(&3).unwrap().tick();
        pump(&mut replicas, &old_leader_isolated);
    }
    if !replicas[&2].is_leader() {
        replicas.get_mut(&2).unwrap().campaign().unwrap();
    }
    pump(&mut replicas, &old_leader_isolated);
    assert!(replicas[&2].is_leader());
    replicas.get_mut(&2).unwrap().propose(create).unwrap();
    pump(&mut replicas, &old_leader_isolated);
    assert_eq!(replicas[&2].state().catalog().revision(), 1);
    assert_eq!(replicas[&3].state().catalog().revision(), 1);

    let schema = CatalogCommand::publish_schema(102, 1, 7, 1, 2)
        .encode()
        .unwrap();
    let new_leader_isolated = isolate(2);
    replicas.get_mut(&2).unwrap().propose(schema).unwrap();
    pump(&mut replicas, &new_leader_isolated);
    assert_eq!(replicas[&2].state().catalog().revision(), 1);
    assert_eq!(replicas[&3].state().catalog().revision(), 1);

    for _ in 0..8 {
        replicas.get_mut(&2).unwrap().tick();
        pump(&mut replicas, &no_blocks);
    }
    assert!(
        replicas
            .values()
            .all(|replica| replica.state().catalog().revision() == 2)
    );
}

#[test]
fn local_snapshot_and_restart_restore_exact_catalog_and_continue() {
    let root = tempfile::tempdir().unwrap();
    let raft = root.path().join("raft");
    let state = root.path().join("state");
    let mut replica = MetaRaftReplica::open(1, &[1], &raft, &state).unwrap();
    replica.campaign().unwrap();
    for _ in 0..32 {
        assert!(replica.drain_ready().unwrap().is_empty());
        if replica.is_leader() {
            break;
        }
        replica.tick();
    }
    assert!(replica.is_leader());
    replica
        .propose(
            CatalogCommand::create_graph(101, 0, graph())
                .encode()
                .unwrap(),
        )
        .unwrap();
    assert!(replica.drain_ready().unwrap().is_empty());
    let snapshot = replica.create_snapshot().unwrap();
    let applied = replica.state().applied_index();
    assert_eq!(snapshot.metadata.unwrap().index, applied);
    drop(replica);

    let mut reopened = MetaRaftReplica::open(1, &[1], &raft, &state).unwrap();
    assert_eq!(reopened.state().applied_index(), applied);
    assert_eq!(reopened.state().catalog().revision(), 1);
    reopened.campaign().unwrap();
    for _ in 0..32 {
        assert!(reopened.drain_ready().unwrap().is_empty());
        if reopened.is_leader() {
            break;
        }
        reopened.tick();
    }
    reopened
        .propose(
            CatalogCommand::publish_schema(102, 1, 7, 1, 2)
                .encode()
                .unwrap(),
        )
        .unwrap();
    assert!(reopened.drain_ready().unwrap().is_empty());
    assert_eq!(
        reopened
            .state()
            .catalog()
            .graph(7)
            .unwrap()
            .schema_version(),
        2
    );
}
