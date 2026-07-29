#![forbid(unsafe_code)]

use std::path::{Path, PathBuf};

use dtg_execution::storage::{
    BindingRole, CommandId, CommittedShardBatch, LogicalMutation, LogicalReplicaActivation,
    LogicalReplicaActivationReceipt, LogicalSnapshotCandidateReceipt, LogicalSnapshotReader,
    LogicalSnapshotSink, LogicalSnapshotSource, LogicalSnapshotWriter, ProviderKind, ReadFence,
    ReplicaBinding, ReplicaMetadata, ReplicaStateStore, SnapshotChunk, SnapshotHeader,
    SnapshotManifest, SnapshotRecord, SnapshotRequest, StorageTckFactory, TransactionTime,
    ValidInterval, Value, Version, VertexId, VertexVersion,
};
use dtg_storage_fjall::{FjallReplicaStore, FjallStorageTckFactory};
use dtg_storage_neo4j::{Neo4jConfig, Neo4jReplicaStore, Neo4jStorageTckFactory};
use dtg_storage_postgres::{PostgresReplicaStore, PostgresStorageTckFactory};
use serde_json::json;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires simultaneous disposable PostgreSQL 17 and Neo4j 5.26 services"]
async fn every_physical_provider_direction_preserves_the_logical_snapshot_digest() {
    let root = tempfile::tempdir().unwrap();
    let environment = LiveProviderEnvironment::from_environment(root.path());
    let directions = [
        (ProviderKind::Fjall, ProviderKind::PostgreSql),
        (ProviderKind::Fjall, ProviderKind::Neo4j),
        (ProviderKind::PostgreSql, ProviderKind::Fjall),
        (ProviderKind::PostgreSql, ProviderKind::Neo4j),
        (ProviderKind::Neo4j, ProviderKind::Fjall),
        (ProviderKind::Neo4j, ProviderKind::PostgreSql),
    ];
    let mut evidence = Vec::new();

    for (offset, (source_provider, target_provider)) in directions.into_iter().enumerate() {
        evidence.push(
            exercise_direction(
                &environment,
                source_provider,
                target_provider,
                offset as u64,
            )
            .await,
        );
    }

    let evidence = json!({
        "schema_version": 1,
        "six_provider_migrations": {
            "count": evidence.len(),
            "directions": evidence,
        }
    });
    if let Some(path) = std::env::var_os("DTG_CLEAN_BREAK_PROVIDER_MIGRATION_EVIDENCE") {
        std::fs::write(path, serde_json::to_vec_pretty(&evidence).unwrap()).unwrap();
    }
    println!("DTG_PROVIDER_MIGRATION_EVIDENCE={evidence}");
}

async fn exercise_direction(
    environment: &LiveProviderEnvironment,
    source_provider: ProviderKind,
    target_provider: ProviderKind,
    offset: u64,
) -> serde_json::Value {
    let namespace = format!("migration-{}-{offset}", std::process::id());
    let source_binding = environment.binding(
        source_provider.clone(),
        &format!("{namespace}-source"),
        3,
        BindingRole::Active,
    );
    let candidate_binding = environment.binding(
        target_provider.clone(),
        &format!("{namespace}-target"),
        4,
        BindingRole::Candidate,
    );
    let active_binding = candidate_binding
        .to_builder()
        .role(BindingRole::Active)
        .build()
        .unwrap();
    let source = environment
        .open(source_provider.clone(), source_binding.clone())
        .await;
    let marker = ReplicaMetadata::new(
        "dtg.certification.provider-migration",
        Value::Map(std::collections::BTreeMap::from([
            (
                "direction".into(),
                Value::String(format!(
                    "{}->{}",
                    provider_name(&source_provider),
                    provider_name(&target_provider)
                )),
            ),
            ("offset".into(), Value::Integer(offset as i64)),
        ])),
    )
    .unwrap();
    let vertex = VertexVersion::new(
        VertexId::new(10_000 + u128::from(offset)).unwrap(),
        Version::new(1),
        ValidInterval::new(0, 100).unwrap(),
        TransactionTime::new(40).unwrap(),
        dtg_execution::storage::Properties::new(),
    )
    .unwrap();
    source
        .apply(
            CommittedShardBatch::new(
                source_binding.clone(),
                7,
                1,
                CommandId::new(20_000 + u128::from(offset)).unwrap(),
                vec![
                    LogicalMutation::PutVertex(vertex),
                    LogicalMutation::PutReplicaMetadata(marker.clone()),
                ],
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let snapshot_id = 30_000 + u128::from(offset) * 10;
    let source_snapshot = export_snapshot(&source, source_binding, snapshot_id).await;
    assert_eq!(
        canonical_digest(&source_snapshot.header, &source_snapshot.records),
        source_snapshot.manifest.content_digest()
    );

    let target = environment
        .open(target_provider.clone(), candidate_binding.clone())
        .await;
    let mut writer = target
        .begin_restore(candidate_binding.clone(), source_snapshot.header.clone())
        .await
        .unwrap();
    for chunk in source_snapshot.chunks.iter().cloned() {
        writer.write_chunk(chunk).await.unwrap();
    }
    let restore = writer
        .commit(source_snapshot.manifest.clone())
        .await
        .unwrap();
    assert_eq!(
        restore.manifest().content_digest(),
        source_snapshot.manifest.content_digest()
    );
    let candidate = LogicalSnapshotCandidateReceipt::new(
        candidate_binding,
        source_snapshot.header.clone(),
        source_snapshot.manifest.clone(),
    )
    .unwrap();
    let activation = target
        .activate_candidate(candidate, active_binding.clone())
        .await
        .unwrap();
    assert_eq!(
        activation.content_digest(),
        source_snapshot.manifest.content_digest()
    );
    drop(target);

    let active = environment
        .open(target_provider.clone(), active_binding.clone())
        .await;
    assert_eq!(active.applied_index().await.unwrap(), 1);
    assert_eq!(
        active
            .replica_metadata("dtg.certification.provider-migration")
            .await
            .unwrap(),
        Some(marker)
    );
    let target_snapshot = export_snapshot(&active, active_binding, snapshot_id + 1).await;
    assert_eq!(target_snapshot.records, source_snapshot.records);
    let target_digest = canonical_digest(&source_snapshot.header, &target_snapshot.records);
    assert_eq!(target_digest, source_snapshot.manifest.content_digest());

    json!({
        "source": provider_name(&source_provider),
        "target": provider_name(&target_provider),
        "applied_index": activation.applied_index(),
        "record_count": target_snapshot.records.len(),
        "source_content_digest": digest_hex(source_snapshot.manifest.content_digest()),
        "target_canonical_digest": digest_hex(target_digest),
    })
}

struct ExportedSnapshot {
    header: SnapshotHeader,
    chunks: Vec<SnapshotChunk>,
    manifest: SnapshotManifest,
    records: Vec<SnapshotRecord>,
}

async fn export_snapshot(
    store: &LiveStore,
    binding: ReplicaBinding,
    snapshot_id: u128,
) -> ExportedSnapshot {
    let applied_index = store.applied_index().await.unwrap();
    let mut reader = store
        .begin_snapshot(
            ReadFence::new(binding, applied_index),
            SnapshotRequest::new(snapshot_id, 2).unwrap(),
        )
        .await
        .unwrap();
    let header = reader.header().clone();
    let mut chunks = Vec::new();
    while let Some(chunk) = reader.next_chunk().await.unwrap() {
        chunks.push(chunk);
    }
    let manifest = reader.finish().await.unwrap();
    let records = chunks
        .iter()
        .flat_map(|chunk| chunk.records().iter().cloned())
        .collect();
    ExportedSnapshot {
        header,
        chunks,
        manifest,
        records,
    }
}

fn canonical_digest(
    header: &SnapshotHeader,
    records: &[SnapshotRecord],
) -> dtg_execution::storage::Digest32 {
    let chunks = records
        .chunks(2)
        .enumerate()
        .map(|(ordinal, records)| {
            SnapshotChunk::new(header.snapshot_id(), ordinal as u64, records.to_vec()).unwrap()
        })
        .collect::<Vec<_>>();
    SnapshotManifest::new(header, &chunks)
        .unwrap()
        .content_digest()
}

struct LiveProviderEnvironment {
    fjall_root: PathBuf,
    postgres_url: String,
    neo4j: Neo4jConfig,
}

impl LiveProviderEnvironment {
    fn from_environment(root: &Path) -> Self {
        Self {
            fjall_root: root.join("fjall"),
            postgres_url: std::env::var("DTG_POSTGRES_URL")
                .expect("DTG_POSTGRES_URL must name a disposable PostgreSQL 17 database"),
            neo4j: Neo4jConfig::new(
                std::env::var("DTG_NEO4J_URL")
                    .expect("DTG_NEO4J_URL must name a disposable Neo4j 5.26 service"),
                std::env::var("DTG_NEO4J_USER").unwrap_or_else(|_| "neo4j".into()),
                std::env::var("DTG_NEO4J_PASSWORD")
                    .expect("DTG_NEO4J_PASSWORD must authenticate the disposable Neo4j service"),
            )
            .unwrap(),
        }
    }

    fn binding(
        &self,
        provider: ProviderKind,
        namespace: &str,
        generation: u64,
        role: BindingRole,
    ) -> ReplicaBinding {
        let binding = match provider {
            ProviderKind::Fjall => {
                FjallStorageTckFactory::new(&self.fjall_root).binding(namespace, generation)
            }
            ProviderKind::PostgreSql => {
                PostgresStorageTckFactory::new(&self.postgres_url).binding(namespace, generation)
            }
            ProviderKind::Neo4j => {
                Neo4jStorageTckFactory::new(self.neo4j.clone()).binding(namespace, generation)
            }
            ProviderKind::Remote(_) => {
                panic!("remote storage is not an official provider migration endpoint")
            }
        }
        .unwrap();
        binding.to_builder().role(role).build().unwrap()
    }

    async fn open(&self, provider: ProviderKind, binding: ReplicaBinding) -> LiveStore {
        match provider {
            ProviderKind::Fjall => {
                std::fs::create_dir_all(&self.fjall_root).unwrap();
                LiveStore::Fjall(
                    FjallReplicaStore::open(
                        self.fjall_root.join(binding.namespace_id().as_str()),
                        binding,
                    )
                    .unwrap(),
                )
            }
            ProviderKind::PostgreSql => LiveStore::PostgreSql(
                PostgresReplicaStore::open(&self.postgres_url, binding)
                    .await
                    .unwrap(),
            ),
            ProviderKind::Neo4j => LiveStore::Neo4j(
                Neo4jReplicaStore::open(self.neo4j.clone(), binding)
                    .await
                    .unwrap(),
            ),
            ProviderKind::Remote(_) => {
                panic!("remote storage is not an official provider migration endpoint")
            }
        }
    }
}

enum LiveStore {
    Fjall(FjallReplicaStore),
    PostgreSql(PostgresReplicaStore),
    Neo4j(Neo4jReplicaStore),
}

impl LiveStore {
    async fn apply(
        &self,
        batch: CommittedShardBatch,
    ) -> Result<(), dtg_execution::storage::StorageError> {
        match self {
            Self::Fjall(store) => store.apply(batch).await?,
            Self::PostgreSql(store) => store.apply(batch).await?,
            Self::Neo4j(store) => store.apply(batch).await?,
        };
        Ok(())
    }

    async fn applied_index(&self) -> Result<u64, dtg_execution::storage::StorageError> {
        match self {
            Self::Fjall(store) => store.applied_index().await,
            Self::PostgreSql(store) => store.applied_index().await,
            Self::Neo4j(store) => store.applied_index().await,
        }
    }

    async fn replica_metadata(
        &self,
        name: &str,
    ) -> Result<Option<ReplicaMetadata>, dtg_execution::storage::StorageError> {
        match self {
            Self::Fjall(store) => store.replica_metadata(name).await,
            Self::PostgreSql(store) => store.replica_metadata(name).await,
            Self::Neo4j(store) => store.replica_metadata(name).await,
        }
    }

    async fn begin_snapshot(
        &self,
        fence: ReadFence,
        request: SnapshotRequest,
    ) -> Result<Box<dyn LogicalSnapshotReader>, dtg_execution::storage::StorageError> {
        match self {
            Self::Fjall(store) => store.begin_snapshot(fence, request).await,
            Self::PostgreSql(store) => store.begin_snapshot(fence, request).await,
            Self::Neo4j(store) => store.begin_snapshot(fence, request).await,
        }
    }

    async fn begin_restore(
        &self,
        binding: ReplicaBinding,
        header: SnapshotHeader,
    ) -> Result<Box<dyn LogicalSnapshotWriter>, dtg_execution::storage::StorageError> {
        match self {
            Self::Fjall(store) => store.begin_restore(binding, header).await,
            Self::PostgreSql(store) => store.begin_restore(binding, header).await,
            Self::Neo4j(store) => store.begin_restore(binding, header).await,
        }
    }

    async fn activate_candidate(
        &self,
        candidate: LogicalSnapshotCandidateReceipt,
        active_binding: ReplicaBinding,
    ) -> Result<LogicalReplicaActivationReceipt, dtg_execution::storage::StorageError> {
        match self {
            Self::Fjall(store) => store.activate_candidate(candidate, active_binding).await,
            Self::PostgreSql(store) => store.activate_candidate(candidate, active_binding).await,
            Self::Neo4j(store) => store.activate_candidate(candidate, active_binding).await,
        }
    }
}

fn provider_name(provider: &ProviderKind) -> &'static str {
    match provider {
        ProviderKind::Fjall => "fjall",
        ProviderKind::PostgreSql => "postgresql",
        ProviderKind::Neo4j => "neo4j",
        ProviderKind::Remote(_) => "remote",
    }
}

fn digest_hex(digest: dtg_execution::storage::Digest32) -> String {
    digest
        .get()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
