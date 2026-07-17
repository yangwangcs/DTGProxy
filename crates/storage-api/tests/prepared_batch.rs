use storage_api::{CommittedMutationBatch, Keyspace, LogicalKey, Mutation, PreparedMutationBatch};

#[test]
fn prepared_batch_is_log_position_independent_until_committed_apply() {
    let prepared = PreparedMutationBatch {
        shard_id: 7,
        txn_id: 99,
        mutations: vec![Mutation::put(
            0,
            LogicalKey::in_keyspace(Keyspace::Current, b"vertex".to_vec()),
            b"value".to_vec(),
        )],
    };
    let fingerprint = prepared.fingerprint();

    assert_eq!(
        prepared.clone().commit_at(41),
        CommittedMutationBatch {
            shard_id: 7,
            log_index: 41,
            txn_id: 99,
            mutations: prepared.mutations.clone(),
        }
    );
    assert_eq!(prepared.clone().commit_at(42).log_index, 42);
    assert_eq!(prepared.fingerprint(), fingerprint);
}
