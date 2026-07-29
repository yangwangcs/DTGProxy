#![forbid(unsafe_code)]

use dtg_storage::{
    CommandId, CommittedShardBatch, LogicalMutation, ReplicaMetadata, StorageTckFactory, Value,
    run_storage_tck,
};
use dtg_storage_postgres::PostgresStorageTckFactory;

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
