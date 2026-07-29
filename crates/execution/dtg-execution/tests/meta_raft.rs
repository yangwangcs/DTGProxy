use std::collections::BTreeMap;
use std::sync::Arc;

use dtg_execution::{MetaRaftHost, MetaRaftRole, ProviderKind, ReplicaBinding};
use dtg_storage::{BackendClass, BindingRole, ConsensusStore, RaftMembership, ReplicaId};
use dtg_storage_fjall::FjallConsensusStore;
use raft::eraftpb::Message;

#[tokio::test]
async fn configured_meta_raft_commits_and_survives_leader_change() {
    let root = tempfile::tempdir().unwrap();
    let voters = vec![
        ReplicaId::new(1).unwrap(),
        ReplicaId::new(2).unwrap(),
        ReplicaId::new(3).unwrap(),
    ];
    let mut stores = BTreeMap::new();
    let mut hosts = BTreeMap::new();
    for id in 1..=3 {
        let store = Arc::new(
            FjallConsensusStore::open(root.path().join(format!("meta-{id}")), binding(id)).unwrap(),
        );
        store
            .set_membership(RaftMembership {
                voters: voters.clone(),
                learners: Vec::new(),
                configuration_index: 1,
            })
            .await
            .unwrap();
        hosts.insert(
            id,
            MetaRaftHost::open(store.clone(), ReplicaId::new(id).unwrap(), 0)
                .await
                .unwrap(),
        );
        stores.insert(id, store);
    }

    hosts.get_mut(&1).unwrap().campaign().unwrap();
    pump(&mut hosts).await;
    assert_eq!(hosts[&1].role(), MetaRaftRole::Leader);
    hosts
        .get_mut(&1)
        .unwrap()
        .propose(41, b"catalog-v1".to_vec())
        .unwrap();
    let committed = pump(&mut hosts).await;
    assert!(committed.iter().any(|payload| payload == b"catalog-v1"));

    hosts.remove(&1);
    for _ in 0..20 {
        hosts.get_mut(&2).unwrap().tick();
        hosts.get_mut(&3).unwrap().tick();
        pump(&mut hosts).await;
        if hosts
            .values()
            .any(|host| host.role() == MetaRaftRole::Leader)
        {
            break;
        }
    }
    let new_leader = hosts
        .iter()
        .find_map(|(id, host)| (host.role() == MetaRaftRole::Leader).then_some(*id))
        .unwrap();
    hosts
        .get_mut(&new_leader)
        .unwrap()
        .propose(42, b"catalog-v2".to_vec())
        .unwrap();
    let committed = pump(&mut hosts).await;
    assert!(committed.iter().any(|payload| payload == b"catalog-v2"));

    let restarted = MetaRaftHost::open(
        stores.remove(&new_leader).unwrap(),
        ReplicaId::new(new_leader).unwrap(),
        0,
    )
    .await
    .unwrap();
    assert!(
        restarted
            .recovery_entries()
            .iter()
            .any(|entry| entry.payload() == b"catalog-v2")
    );
}

async fn pump(hosts: &mut BTreeMap<u64, MetaRaftHost>) -> Vec<Vec<u8>> {
    let mut committed = Vec::new();
    for _ in 0..40 {
        let mut messages = Vec::<Message>::new();
        let ids = hosts.keys().copied().collect::<Vec<_>>();
        for id in ids {
            let mut progress = hosts.get_mut(&id).unwrap().drive_ready().await.unwrap();
            messages.append(&mut progress.messages);
            committed.extend(
                progress
                    .committed
                    .into_iter()
                    .map(|entry| entry.payload().to_vec()),
            );
        }
        if messages.is_empty() {
            break;
        }
        for message in messages {
            if let Some(target) = hosts.get_mut(&message.to) {
                target.step(message).unwrap();
            }
        }
    }
    committed
}

fn binding(replica_id: u64) -> ReplicaBinding {
    let class = BackendClass::new(
        ProviderKind::Fjall,
        1,
        1,
        [
            "adjacency",
            "immutable-read-view",
            "logical-snapshot",
            "point",
        ],
    )
    .unwrap();
    ReplicaBinding::builder()
        .cluster_id(7)
        .graph_id(u64::MAX - 1)
        .shard_id(1)
        .placement_epoch(1)
        .replica_id(replica_id)
        .backend_generation(1)
        .backend_class_digest(class.digest())
        .provider_kind(ProviderKind::Fjall)
        .contract_version(1)
        .layout_version(1)
        .capability_digest(class.required_capabilities().digest())
        .namespace_id(format!("meta-{replica_id}"))
        .endpoint_profile_ref("process://meta-raft")
        .credential_ref("process://meta-raft")
        .role(BindingRole::Active)
        .build()
        .unwrap()
}
