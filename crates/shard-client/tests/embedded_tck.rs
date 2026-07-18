use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use raft_command::{ApplyPreparedV1, CommandBodyV1, CommandEnvelopeV1};
use shard_client::{
    EmbeddedShardClient, ExecuteCommand, ReadKeysRequest, ScanRequest, ShardClient,
    ShardRequestContext,
};
use shard_runtime::{InProcessShardGroup, MultiRaftRuntime};
use storage_api::{KeySpan, Keyspace, LogicalKey, Mutation, PreparedMutationBatch};
use temporal_types::TransactionTime;

fn now_ms() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
}

fn context(request_id: u128) -> ShardRequestContext {
    ShardRequestContext::new(9, 11, 3, request_id, now_ms() + 60_000).unwrap()
}

fn command(request_id: u128) -> Vec<u8> {
    CommandEnvelopeV1::new(
        11,
        3,
        request_id,
        CommandBodyV1::ApplyPrepared(ApplyPreparedV1 {
            commit_ts: TransactionTime::new(100, 0),
            batch: PreparedMutationBatch {
                shard_id: 11,
                txn_id: 901,
                mutations: vec![Mutation::put(
                    0,
                    LogicalKey::in_keyspace(Keyspace::Current, b"vertex/1".to_vec()),
                    b"value-1".to_vec(),
                )],
            },
        }),
    )
    .encode()
    .unwrap()
}

async fn client() -> EmbeddedShardClient {
    let mut runtime = MultiRaftRuntime::new();
    runtime
        .insert_group(InProcessShardGroup::new(11, 3, &[1, 2, 3]).await.unwrap())
        .unwrap();
    runtime.group_mut(11).unwrap().elect(1).await.unwrap();
    EmbeddedShardClient::new(9, 30, Arc::new(tokio::sync::Mutex::new(runtime))).unwrap()
}

#[tokio::test(flavor = "current_thread")]
async fn embedded_contract_executes_idempotently_and_serves_proven_reads() {
    let client = client().await;
    let first = client
        .execute(ExecuteCommand::new(context(101), command(101)).unwrap())
        .await
        .unwrap();
    assert!(!first.duplicate());
    let replay = client
        .execute(ExecuteCommand::new(context(101), command(101)).unwrap())
        .await
        .unwrap();
    assert!(replay.duplicate());
    assert_eq!(replay.raft_index(), first.raft_index());

    let key = LogicalKey::in_keyspace(Keyspace::Current, b"vertex/1".to_vec());
    let values = client
        .read_keys(ReadKeysRequest::new(context(102), vec![key.clone()]).unwrap())
        .await
        .unwrap();
    assert_eq!(values, vec![Some(b"value-1".to_vec())]);
    let rows = client
        .scan(ScanRequest::new(
            context(103),
            KeySpan::prefix(Keyspace::Current, b"vertex/".to_vec()),
        ))
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].key(), &key);
    assert!(client.status(context(104)).await.unwrap().applied_index() > 0);
}

#[tokio::test(flavor = "current_thread")]
async fn embedded_contract_rejects_context_and_command_identity_mismatch() {
    let client = client().await;
    let error = client
        .execute(ExecuteCommand::new(context(202), command(201)).unwrap())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("differs"));
    let wrong_graph = ShardRequestContext::new(10, 11, 3, 203, now_ms() + 60_000).unwrap();
    assert!(client.status(wrong_graph).await.is_err());
}
