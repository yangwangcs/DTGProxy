use std::sync::Arc;

use dtg_data::DataNodeBuilder;
use dtg_execution::shard::ReplicaLifecycle;
use dtg_execution::storage::{
    ApplyReceipt, BackendClass, BindingRole, CapabilityManifest, CommittedShardBatch, ReadFence,
    ReplicaMetadata, TemporalReadView,
};
use dtg_execution::{
    ProviderKind, ProviderResolver, ReplicaBinding, ReplicaStateStore, StorageError, StoreFuture,
};

struct FixtureResolver {
    kind: ProviderKind,
    fail: bool,
}

impl FixtureResolver {
    fn new(kind: ProviderKind) -> Self {
        Self { kind, fail: false }
    }

    fn failing(kind: ProviderKind) -> Self {
        Self { kind, fail: true }
    }
}

impl ProviderResolver for FixtureResolver {
    fn provider_kind(&self) -> ProviderKind {
        self.kind.clone()
    }

    fn open<'a>(&'a self, binding: ReplicaBinding) -> StoreFuture<'a, Arc<dyn ReplicaStateStore>> {
        Box::pin(async move {
            if self.fail {
                return Err(StorageError::Internal("injected provider failure".into()));
            }
            Ok(Arc::new(FixtureStore { binding }) as Arc<dyn ReplicaStateStore>)
        })
    }
}

struct FixtureStore {
    binding: ReplicaBinding,
}

impl ReplicaStateStore for FixtureStore {
    fn binding(&self) -> &ReplicaBinding {
        &self.binding
    }

    fn applied_index(&self) -> StoreFuture<'_, u64> {
        Box::pin(async { Ok(0) })
    }

    fn replica_metadata<'a>(&'a self, _name: &'a str) -> StoreFuture<'a, Option<ReplicaMetadata>> {
        Box::pin(async { Ok(None) })
    }

    fn apply(&self, batch: CommittedShardBatch) -> StoreFuture<'_, ApplyReceipt> {
        Box::pin(async move { Ok(ApplyReceipt::new(&batch, false)) })
    }

    fn begin_read_view(&self, _fence: ReadFence) -> StoreFuture<'_, Box<dyn TemporalReadView>> {
        Box::pin(async { Err(StorageError::Unsupported) })
    }
}

fn fixture_data_node() -> DataNodeBuilder {
    let consensus_root = tempfile::tempdir().unwrap().keep();
    DataNodeBuilder::new(consensus_root)
        .with_provider(
            ProviderKind::Fjall,
            Arc::new(FixtureResolver::new(ProviderKind::Fjall)),
        )
        .with_provider(
            ProviderKind::PostgreSql,
            Arc::new(FixtureResolver::new(ProviderKind::PostgreSql)),
        )
        .with_provider(
            ProviderKind::Kuzu,
            Arc::new(FixtureResolver::new(ProviderKind::Kuzu)),
        )
}

fn fjall_shard(shard_id: u64) -> ReplicaBinding {
    binding(ProviderKind::Fjall, shard_id)
}

fn postgres_shard(shard_id: u64) -> ReplicaBinding {
    binding(ProviderKind::PostgreSql, shard_id)
}

fn kuzu_shard(shard_id: u64) -> ReplicaBinding {
    binding(ProviderKind::Kuzu, shard_id)
}

fn binding(provider_kind: ProviderKind, shard_id: u64) -> ReplicaBinding {
    let capabilities = CapabilityManifest::from_names(["point"]).unwrap();
    let backend = BackendClass::new(
        provider_kind.clone(),
        1,
        1,
        capabilities.names().map(str::to_owned),
    )
    .unwrap();
    ReplicaBinding::builder()
        .cluster_id(1)
        .graph_id(7)
        .shard_id(shard_id)
        .placement_epoch(1)
        .replica_id(shard_id)
        .backend_generation(1)
        .backend_class_digest(backend.digest())
        .provider_kind(provider_kind)
        .contract_version(1)
        .layout_version(1)
        .capability_digest(capabilities.digest())
        .namespace_id(format!("fixture-shard-{shard_id}"))
        .endpoint_profile_ref("fixture")
        .credential_ref("fixture")
        .role(BindingRole::Active)
        .build()
        .unwrap()
}

#[tokio::test]
// This intentionally exercises the legacy hand-built fixture path. Production
// DataProcessConfig construction is single-provider and covered by process.rs.
async fn fixture_builder_hosts_three_independent_backend_classes() {
    let node = fixture_data_node()
        .assign(fjall_shard(1))
        .assign(postgres_shard(2))
        .assign(kuzu_shard(3))
        .start()
        .await
        .unwrap();

    let failures = node.replica_failures().await;
    assert!(
        failures.is_empty(),
        "unexpected replica failures: {failures:#?}"
    );
    assert_eq!(node.observed_replicas().await.len(), 3);
    assert!(
        node.replica_observations()
            .await
            .iter()
            .all(|observation| observation.lifecycle() == ReplicaLifecycle::Running)
    );
}

#[tokio::test]
async fn same_shard_generation_rejects_heterogeneous_backend_classes() {
    let fjall = fjall_shard(9);
    let postgres = postgres_shard(9)
        .to_builder()
        .replica_id(10)
        .namespace_id("fixture-shard-9-postgres")
        .build()
        .unwrap();
    let node = fixture_data_node()
        .assign(fjall.clone())
        .assign(postgres.clone())
        .start()
        .await
        .unwrap();

    assert_eq!(node.observed_replicas().await, vec![fjall]);
    let failures = node.replica_failures().await;
    assert_eq!(failures.len(), 1);
    assert_eq!(failures[0].binding(), &postgres);
    assert!(
        failures[0]
            .message()
            .contains("one active Shard generation")
    );
}

#[tokio::test]
async fn one_provider_failure_does_not_block_an_independent_replica() {
    let consensus_root = tempfile::tempdir().unwrap().keep();
    let healthy = fjall_shard(21);
    let failed = postgres_shard(22);
    let node = DataNodeBuilder::new(consensus_root)
        .with_provider(
            ProviderKind::Fjall,
            Arc::new(FixtureResolver::new(ProviderKind::Fjall)),
        )
        .with_provider(
            ProviderKind::PostgreSql,
            Arc::new(FixtureResolver::failing(ProviderKind::PostgreSql)),
        )
        .assign(failed.clone())
        .assign(healthy.clone())
        .start()
        .await
        .unwrap();

    assert_eq!(node.observed_replicas().await, vec![healthy]);
    let failures = node.replica_failures().await;
    assert_eq!(failures.len(), 1);
    assert_eq!(failures[0].binding(), &failed);
    assert!(failures[0].message().contains("injected provider failure"));
    assert_eq!(node.metrics().hosted_replicas(), 1);
    assert_eq!(node.metrics().failed_replicas(), 1);
}
