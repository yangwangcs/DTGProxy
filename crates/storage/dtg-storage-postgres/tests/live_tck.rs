#![forbid(unsafe_code)]

use dtg_storage::{
    BindingRole, CommandId, CommittedShardBatch, LogicalMutation, LogicalReplicaActivation,
    LogicalSnapshotCandidateReceipt, LogicalSnapshotSink, LogicalSnapshotSource, ReadFence,
    ReplicaMetadata, ReplicaStateStore, SUPPORTED_SNAPSHOT_FORMAT_VERSION, SnapshotHeader,
    SnapshotManifest, SnapshotRequest, StorageError, StorageTckFactory, Value, run_storage_tck,
};
use dtg_storage_postgres::{PostgresReplicaStore, PostgresStorageTckFactory};

fn database_url() -> String {
    std::env::var("DTG_POSTGRES_URL")
        .expect("DTG_POSTGRES_URL must name a disposable PostgreSQL 17 database")
}

fn all_value_shapes() -> Value {
    Value::Map(std::collections::BTreeMap::from([
        ("null".into(), Value::Null),
        ("boolean".into(), Value::Boolean(true)),
        ("integer".into(), Value::Integer(i64::MIN + 7)),
        ("float_bits".into(), Value::FloatBits(0x7ff8_0000_0000_0042)),
        ("bytes".into(), Value::Bytes(vec![0, 1, 2, 255])),
        ("string".into(), Value::String("typed-postgres".into())),
        (
            "list".into(),
            Value::List(vec![
                Value::Integer(9),
                Value::Map(std::collections::BTreeMap::from([(
                    "nested".into(),
                    Value::Boolean(false),
                )])),
            ]),
        ),
    ]))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a disposable PostgreSQL 17 service"]
async fn postgres_passes_the_shared_storage_tck() {
    let factory = PostgresStorageTckFactory::new(database_url());
    run_storage_tck(&factory).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a disposable PostgreSQL 17 service"]
async fn current_replica_metadata_point_read_returns_the_latest_value() {
    let factory = PostgresStorageTckFactory::new(database_url());
    let binding = factory.binding("postgres-metadata-point", 1).unwrap();
    let store = factory.open(binding.clone()).await.unwrap();
    let first = ReplicaMetadata::new("dtg.test.current", Value::String("first".into())).unwrap();
    let second = ReplicaMetadata::new("dtg.test.current", Value::String("second".into())).unwrap();

    store
        .apply(
            CommittedShardBatch::new(
                binding.clone(),
                1,
                1,
                CommandId::new(8101).unwrap(),
                vec![LogicalMutation::PutReplicaMetadata(first)],
            )
            .unwrap(),
        )
        .await
        .unwrap();
    store
        .apply(
            CommittedShardBatch::new(
                binding,
                1,
                2,
                CommandId::new(8102).unwrap(),
                vec![LogicalMutation::PutReplicaMetadata(second.clone())],
            )
            .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(
        store.replica_metadata("dtg.test.current").await.unwrap(),
        Some(second)
    );
    assert_eq!(
        store.replica_metadata("dtg.test.absent").await.unwrap(),
        None
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a disposable PostgreSQL 17 service"]
async fn typed_value_variants_round_trip_losslessly() {
    let factory = PostgresStorageTckFactory::new(database_url());
    let binding = factory.binding("postgres-typed-values", 1).unwrap();
    let store = factory.open(binding.clone()).await.unwrap();
    let metadata = ReplicaMetadata::new("dtg.test.values", all_value_shapes()).unwrap();
    store
        .apply(
            CommittedShardBatch::new(
                binding,
                1,
                1,
                CommandId::new(8201).unwrap(),
                vec![LogicalMutation::PutReplicaMetadata(metadata.clone())],
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        store.replica_metadata("dtg.test.values").await.unwrap(),
        Some(metadata)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a disposable PostgreSQL 17 service"]
async fn candidate_restore_activation_is_atomic_fenced_and_exactly_retryable() {
    let factory = PostgresStorageTckFactory::new(database_url());
    let namespace = format!("postgres-activation-{}", std::process::id());
    let source_binding = factory.binding(&format!("{namespace}-source"), 1).unwrap();
    let candidate_binding = factory
        .binding(&format!("{namespace}-candidate"), 2)
        .unwrap()
        .to_builder()
        .role(BindingRole::Candidate)
        .build()
        .unwrap();
    let active_binding = candidate_binding
        .to_builder()
        .role(BindingRole::Active)
        .build()
        .unwrap();
    let source = PostgresReplicaStore::open(database_url(), source_binding.clone())
        .await
        .unwrap();
    let metadata = ReplicaMetadata::new(
        "dtg.test.activation",
        Value::String("candidate-state".into()),
    )
    .unwrap();
    source
        .apply(
            CommittedShardBatch::new(
                source_binding.clone(),
                1,
                1,
                CommandId::new(8301).unwrap(),
                vec![LogicalMutation::PutReplicaMetadata(metadata.clone())],
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let mut reader = source
        .begin_snapshot(
            ReadFence::new(source_binding, 1),
            SnapshotRequest::new(9301, 1).unwrap(),
        )
        .await
        .unwrap();
    let header = reader.header().clone();
    let mut chunks = Vec::new();
    while let Some(chunk) = reader.next_chunk().await.unwrap() {
        chunks.push(chunk);
    }
    let manifest = reader.finish().await.unwrap();
    let candidate_receipt = LogicalSnapshotCandidateReceipt::new(
        candidate_binding.clone(),
        header.clone(),
        manifest.clone(),
    )
    .unwrap();
    let candidate = PostgresReplicaStore::open(database_url(), candidate_binding.clone())
        .await
        .unwrap();

    assert!(candidate.applied_index().await.is_err());
    assert!(matches!(
        candidate
            .activate_candidate(candidate_receipt.clone(), active_binding.clone())
            .await,
        Err(StorageError::CorruptSnapshot(_))
    ));
    let mut writer = candidate
        .begin_restore(candidate_binding.clone(), header.clone())
        .await
        .unwrap();
    writer.write_chunk(chunks[0].clone()).await.unwrap();
    assert!(matches!(
        candidate
            .activate_candidate(candidate_receipt.clone(), active_binding.clone())
            .await,
        Err(StorageError::CorruptSnapshot(_))
    ));
    for chunk in chunks.into_iter().skip(1) {
        writer.write_chunk(chunk).await.unwrap();
    }
    writer.commit(manifest.clone()).await.unwrap();

    let receipt = candidate
        .activate_candidate(candidate_receipt.clone(), active_binding.clone())
        .await
        .unwrap();
    assert_eq!(receipt.active_binding(), &active_binding);
    assert_eq!(receipt.snapshot_id(), header.snapshot_id());
    assert_eq!(receipt.applied_index(), header.applied_index());
    assert_eq!(receipt.content_digest(), manifest.content_digest());
    assert_eq!(receipt.format_version(), SUPPORTED_SNAPSHOT_FORMAT_VERSION);
    assert_eq!(
        candidate
            .activate_candidate(candidate_receipt.clone(), active_binding.clone())
            .await
            .unwrap(),
        receipt
    );

    assert!(candidate.applied_index().await.is_err());
    assert!(
        candidate
            .begin_snapshot(
                ReadFence::new(candidate_binding.clone(), 1),
                SnapshotRequest::new(9302, 1).unwrap(),
            )
            .await
            .is_err()
    );
    assert!(
        candidate
            .begin_restore(candidate_binding.clone(), header.clone())
            .await
            .is_err()
    );
    assert!(
        candidate
            .apply(
                CommittedShardBatch::new(
                    candidate_binding,
                    1,
                    2,
                    CommandId::new(8302).unwrap(),
                    vec![LogicalMutation::PutReplicaMetadata(
                        ReplicaMetadata::new("dtg.test.fenced", Value::Boolean(true)).unwrap(),
                    )],
                )
                .unwrap(),
            )
            .await
            .is_err()
    );

    let drifted_active = active_binding
        .to_builder()
        .endpoint_profile_ref("postgres-drifted-endpoint")
        .build()
        .unwrap();
    assert!(matches!(
        candidate
            .activate_candidate(candidate_receipt.clone(), drifted_active)
            .await,
        Err(StorageError::SnapshotIdentityMismatch)
    ));
    let drifted_header = SnapshotHeader::new(
        dtg_storage::SnapshotId::new(9303).unwrap(),
        header.source_binding().clone(),
        header.applied_index(),
        SUPPORTED_SNAPSHOT_FORMAT_VERSION,
    )
    .unwrap();
    let drifted_manifest = SnapshotManifest::new(&drifted_header, &[]).unwrap();
    let drifted_candidate = LogicalSnapshotCandidateReceipt::new(
        candidate_receipt.candidate_binding().clone(),
        drifted_header,
        drifted_manifest,
    )
    .unwrap();
    assert!(matches!(
        candidate
            .activate_candidate(drifted_candidate, active_binding.clone())
            .await,
        Err(StorageError::SnapshotIdentityMismatch)
    ));

    let active = PostgresReplicaStore::open(database_url(), active_binding)
        .await
        .unwrap();
    assert_eq!(active.applied_index().await.unwrap(), 1);
    assert_eq!(
        active
            .replica_metadata("dtg.test.activation")
            .await
            .unwrap(),
        Some(metadata)
    );
}
