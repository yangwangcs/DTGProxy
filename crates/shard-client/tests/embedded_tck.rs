use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use adapter_memory::MemoryAdapter;
use raft_command::{
    AnalyticsArtifactKindV1, ApplyPreparedV1, CommandBodyV1, CommandEnvelopeV1,
    DeleteAnalyticsArtifactGenerationV1, PinAnalyticsArtifactGenerationV1,
    PutAnalyticsArtifactChunkV1,
};
use shard_client::{
    AdvanceArtifactFenceRequest, ArtifactGenerationCursor, ArtifactKind,
    DeleteArtifactGenerationRequest, EmbeddedShardClient, ExecuteCommand,
    GetArtifactGenerationRequest, ListArtifactGenerationHeadsRequest,
    ListArtifactGenerationsRequest, PinArtifactGenerationRequest, PutArtifactChunkRequest,
    ReadKeysRequest, ScanRequest, ShardClient, ShardClientError, ShardClientStorageAdapter,
    ShardRequestContext,
};
use shard_runtime::{InProcessShardGroup, MultiRaftRuntime};
use storage_api::{
    AdapterCapabilities, AdapterDescriptorV1, AdapterError, AdapterFuture, ApplyReceipt,
    CommittedMutationBatch, KeySpan, KeyValue, Keyspace, LogicalKey, Mutation,
    PreparedMutationBatch, StorageAdapter,
};
use temporal_types::TransactionTime;
use tokio_stream::StreamExt;

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

fn generic_artifact_command(request_id: u128) -> Vec<u8> {
    CommandEnvelopeV1::new(
        11,
        3,
        request_id,
        CommandBodyV1::PutAnalyticsArtifactChunk(
            PutAnalyticsArtifactChunkV1::new(
                991,
                AnalyticsArtifactKindV1::Result,
                1,
                1_725_000_000_123,
                0,
                [0; 32],
                b"bypass".to_vec(),
            )
            .unwrap(),
        ),
    )
    .encode()
    .unwrap()
}

fn generic_pin_command(request_id: u128) -> Vec<u8> {
    CommandEnvelopeV1::new(
        11,
        3,
        request_id,
        CommandBodyV1::PinAnalyticsArtifactGeneration(
            PinAnalyticsArtifactGenerationV1::new(
                991,
                AnalyticsArtifactKindV1::Result,
                1,
                1,
                6,
                *blake3::hash(b"bypass").as_bytes(),
            )
            .unwrap(),
        ),
    )
    .encode()
    .unwrap()
}

fn generic_delete_command(request_id: u128) -> Vec<u8> {
    CommandEnvelopeV1::new(
        11,
        3,
        request_id,
        CommandBodyV1::DeleteAnalyticsArtifactGeneration(
            DeleteAnalyticsArtifactGenerationV1::new(991, AnalyticsArtifactKindV1::Result, 1)
                .unwrap(),
        ),
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

struct CountingAdapter {
    inner: MemoryAdapter,
    artifact_chunk_reads: AtomicUsize,
}

impl CountingAdapter {
    fn new() -> Self {
        Self {
            inner: MemoryAdapter::new(),
            artifact_chunk_reads: AtomicUsize::new(0),
        }
    }

    fn reset_artifact_chunk_reads(&self) {
        self.artifact_chunk_reads.store(0, Ordering::SeqCst);
    }

    fn artifact_chunk_reads(&self) -> usize {
        self.artifact_chunk_reads.load(Ordering::SeqCst)
    }
}

impl StorageAdapter for CountingAdapter {
    fn descriptor(&self) -> AdapterDescriptorV1 {
        self.inner.descriptor()
    }

    fn capabilities(&self) -> AdapterCapabilities {
        self.inner.capabilities()
    }

    fn apply_committed<'a>(
        &'a self,
        batch: CommittedMutationBatch,
    ) -> AdapterFuture<'a, ApplyReceipt> {
        self.inner.apply_committed(batch)
    }

    fn multi_get<'a>(&'a self, keys: &'a [LogicalKey]) -> AdapterFuture<'a, Vec<Option<Vec<u8>>>> {
        if keys
            .iter()
            .any(|key| key.as_bytes().starts_with(b"\x01dtg/analytics/v1/chunk/"))
        {
            self.artifact_chunk_reads.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.multi_get(keys)
    }

    fn scan<'a>(&'a self, span: &'a KeySpan) -> AdapterFuture<'a, Vec<KeyValue>> {
        self.inner.scan(span)
    }

    fn applied_log_index(&self) -> Result<u64, AdapterError> {
        self.inner.applied_log_index()
    }
}

async fn counting_client() -> (EmbeddedShardClient, Arc<CountingAdapter>) {
    let adapter = Arc::new(CountingAdapter::new());
    let adapter_dyn: Arc<dyn StorageAdapter> = adapter.clone();
    let group =
        InProcessShardGroup::new_with_adapters(11, 3, &[1], BTreeMap::from([(1, adapter_dyn)]))
            .await
            .unwrap();
    let mut runtime = MultiRaftRuntime::new();
    runtime.insert_group(group).unwrap();
    runtime.group_mut(11).unwrap().elect(1).await.unwrap();
    (
        EmbeddedShardClient::new(9, 30, Arc::new(tokio::sync::Mutex::new(runtime))).unwrap(),
        adapter,
    )
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
    let read_index = client.read_barrier(context(105)).await.unwrap();
    assert_ne!(read_index, 0);
    assert!(client.status(context(106)).await.unwrap().applied_index() >= read_index);
}

#[tokio::test(flavor = "current_thread")]
async fn shard_storage_adapter_exposes_fenced_scan_without_claiming_a_snapshot() {
    let client = Arc::new(client().await);
    client
        .execute(ExecuteCommand::new(context(105), command(105)).unwrap())
        .await
        .unwrap();
    let adapter_client: Arc<dyn ShardClient> = client.clone();
    let adapter =
        ShardClientStorageAdapter::new(adapter_client, 9, 11, 3, now_ms() + 60_000, 10).unwrap();
    let span = KeySpan::prefix(Keyspace::Current, b"vertex/".to_vec());

    assert!(matches!(
        adapter.begin_read_snapshot().await,
        Err(AdapterError::UnsupportedOperation { .. })
    ));
    let scan = StorageAdapter::scan_fenced(&adapter, &span).await.unwrap();
    assert_eq!(scan.entries().len(), 1);
    assert_eq!(
        scan.applied_log_index(),
        adapter.applied_log_index().unwrap()
    );
    assert!(scan.applied_log_index() > 0);
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

#[tokio::test(flavor = "current_thread")]
async fn embedded_contract_rejects_generic_artifact_and_metadata_bypasses() {
    let client = client().await;
    for (request_id, command) in [
        (211, generic_artifact_command(211)),
        (212, generic_pin_command(212)),
        (213, generic_delete_command(213)),
    ] {
        assert_eq!(
            client
                .execute(ExecuteCommand::new(context(request_id), command).unwrap())
                .await,
            Err(ShardClientError::InvalidArtifact)
        );
    }
    let meta_key = shard_runtime::analytics_artifact_generation_head_key(
        991,
        AnalyticsArtifactKindV1::Result,
        1,
    );
    assert!(
        client
            .read_keys(ReadKeysRequest::new(context(214), vec![meta_key]).unwrap())
            .await
            .is_err()
    );
    assert!(
        client
            .scan(ScanRequest::new(
                context(215),
                KeySpan::prefix(Keyspace::Meta, Vec::new()),
            ))
            .await
            .is_err()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn embedded_contract_writes_reads_and_fences_analytics_artifacts() {
    let client = client().await;
    let first = PutArtifactChunkRequest::new(
        context(401),
        801,
        ArtifactKind::Checkpoint,
        1,
        1_725_000_000_123,
        0,
        [0; 32],
        b"first".to_vec(),
    )
    .unwrap();
    assert!(
        !client
            .put_artifact_chunk(first.clone())
            .await
            .unwrap()
            .duplicate()
    );
    assert!(client.put_artifact_chunk(first).await.unwrap().duplicate());
    let previous = *blake3::hash(b"first").as_bytes();
    client
        .put_artifact_chunk(
            PutArtifactChunkRequest::new(
                context(402),
                801,
                ArtifactKind::Checkpoint,
                1,
                1_725_000_000_123,
                1,
                previous,
                b"second".to_vec(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let summaries = client
        .list_artifact_generations(
            ListArtifactGenerationsRequest::new(context(4021), 801, ArtifactKind::Checkpoint, 16)
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(summaries.len(), 1);
    assert_eq!(summaries[0].created_at_unix_ms(), 1_725_000_000_123);
    let mut content_hasher = blake3::Hasher::new();
    content_hasher.update(b"first");
    content_hasher.update(b"second");
    let content_digest = *content_hasher.finalize().as_bytes();
    assert!(
        client
            .get_artifact_generation(
                GetArtifactGenerationRequest::new(
                    context(403),
                    801,
                    ArtifactKind::Checkpoint,
                    1,
                    2,
                    11,
                    content_digest,
                )
                .unwrap(),
            )
            .await
            .is_err()
    );
    client
        .pin_artifact_generation(
            PinArtifactGenerationRequest::new(
                context(404),
                801,
                ArtifactKind::Checkpoint,
                1,
                2,
                11,
                content_digest,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let stream = client
        .get_artifact_generation(
            GetArtifactGenerationRequest::new(
                context(405),
                801,
                ArtifactKind::Checkpoint,
                1,
                2,
                11,
                content_digest,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let runtime = client.runtime();
    assert!(runtime.try_lock().is_ok());
    let chunks = stream.collect::<Vec<_>>().await;
    assert_eq!(chunks.len(), 2);
    assert!(chunks.into_iter().all(|chunk| chunk.is_ok()));
    let mismatched = client
        .get_artifact_generation(
            GetArtifactGenerationRequest::new(
                context(4031),
                801,
                ArtifactKind::Checkpoint,
                1,
                2,
                11,
                [9; 32],
            )
            .unwrap(),
        )
        .await;
    assert!(matches!(
        mismatched,
        Err(ShardClientError::ArtifactCorruption(_))
    ));
    assert!(
        client
            .put_artifact_chunk(
                PutArtifactChunkRequest::new(
                    context(404),
                    801,
                    ArtifactKind::Checkpoint,
                    1,
                    1_725_000_000_123,
                    2,
                    [9; 32],
                    b"wrong".to_vec(),
                )
                .unwrap(),
            )
            .await
            .is_err()
    );
    assert!(
        client
            .delete_artifact_generation(
                DeleteArtifactGenerationRequest::new(
                    context(405),
                    801,
                    ArtifactKind::Checkpoint,
                    1
                )
                .unwrap(),
            )
            .await
            .is_err()
    );
    client
        .advance_artifact_fence(
            AdvanceArtifactFenceRequest::new_with_gc_epoch(
                context(406),
                801,
                ArtifactKind::Checkpoint,
                2,
                7,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    client
        .delete_artifact_generation(
            DeleteArtifactGenerationRequest::new_with_gc_epoch(
                context(407),
                801,
                ArtifactKind::Checkpoint,
                1,
                7,
            )
            .unwrap(),
        )
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn embedded_shard_wide_artifact_head_pages_cross_job_and_kind_without_gaps() {
    let client = client().await;
    for (offset, (job_id, kind)) in [
        (700, ArtifactKind::Checkpoint),
        (700, ArtifactKind::Result),
        (701, ArtifactKind::Checkpoint),
        (701, ArtifactKind::Result),
    ]
    .into_iter()
    .enumerate()
    {
        client
            .put_artifact_chunk(
                PutArtifactChunkRequest::new(
                    context(800 + u128::try_from(offset).unwrap()),
                    job_id,
                    kind,
                    1,
                    1_725_000_000_123 + u64::try_from(offset).unwrap(),
                    0,
                    [0; 32],
                    vec![u8::try_from(offset + 1).unwrap()],
                )
                .unwrap(),
            )
            .await
            .unwrap();
    }
    let pinned_digest = *blake3::hash(&[2]).as_bytes();
    client
        .pin_artifact_generation(
            PinArtifactGenerationRequest::new(
                context(809),
                700,
                ArtifactKind::Result,
                1,
                1,
                1,
                pinned_digest,
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let first = client
        .list_artifact_generation_heads(
            ListArtifactGenerationHeadsRequest::new(context(810), None, 2).unwrap(),
        )
        .await
        .unwrap();
    let second = client
        .list_artifact_generation_heads(
            ListArtifactGenerationHeadsRequest::new(context(811), first.next(), 2).unwrap(),
        )
        .await
        .unwrap();
    let terminal = client
        .list_artifact_generation_heads(
            ListArtifactGenerationHeadsRequest::new(context(812), second.next(), 2).unwrap(),
        )
        .await
        .unwrap();

    let observed = first
        .generations()
        .iter()
        .chain(second.generations())
        .map(|summary| (summary.job_id(), summary.kind(), summary.generation()))
        .collect::<Vec<_>>();
    assert_eq!(
        observed,
        vec![
            (700, ArtifactKind::Checkpoint, 1),
            (700, ArtifactKind::Result, 1),
            (701, ArtifactKind::Checkpoint, 1),
            (701, ArtifactKind::Result, 1),
        ]
    );
    assert_eq!(
        first.next(),
        Some(ArtifactGenerationCursor::new(700, ArtifactKind::Result, 1).unwrap())
    );
    assert_eq!(
        second.next(),
        Some(ArtifactGenerationCursor::new(701, ArtifactKind::Result, 1).unwrap())
    );
    assert!(first.generations()[1].pinned());
    assert_eq!(first.generations()[1].expected_total_bytes(), 1);
    assert_eq!(
        first.generations()[1].expected_content_digest(),
        pinned_digest
    );
    assert!(terminal.generations().is_empty());
    assert_eq!(terminal.next(), None);
}

#[tokio::test(flavor = "current_thread")]
async fn embedded_shard_wide_artifact_head_scan_rejects_corrupt_head() {
    let (client, adapter) = counting_client().await;
    client
        .put_artifact_chunk(
            PutArtifactChunkRequest::new(
                context(820),
                702,
                ArtifactKind::Checkpoint,
                1,
                1_725_000_000_123,
                0,
                [0; 32],
                b"head".to_vec(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let next_index = adapter.applied_log_index().unwrap() + 1;
    adapter
        .apply_committed(CommittedMutationBatch {
            shard_id: 11,
            log_index: next_index,
            txn_id: 8_020,
            mutations: vec![Mutation::put(
                0,
                shard_runtime::analytics_artifact_generation_head_key(
                    702,
                    AnalyticsArtifactKindV1::Checkpoint,
                    1,
                ),
                b"corrupt-head".to_vec(),
            )],
        })
        .await
        .unwrap();

    assert!(matches!(
        client
            .list_artifact_generation_heads(
                ListArtifactGenerationHeadsRequest::new(context(821), None, 2).unwrap(),
            )
            .await,
        Err(ShardClientError::ArtifactCorruption(_))
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn embedded_shard_wide_artifact_head_scan_rejects_pin_head_mismatch() {
    let (client, adapter) = counting_client().await;
    client
        .put_artifact_chunk(
            PutArtifactChunkRequest::new(
                context(830),
                703,
                ArtifactKind::Result,
                1,
                1_725_000_000_123,
                0,
                [0; 32],
                b"a".to_vec(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    client
        .put_artifact_chunk(
            PutArtifactChunkRequest::new(
                context(831),
                703,
                ArtifactKind::Result,
                1,
                1_725_000_000_123,
                1,
                *blake3::hash(b"a").as_bytes(),
                b"b".to_vec(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"a");
    hasher.update(b"b");
    client
        .pin_artifact_generation(
            PinArtifactGenerationRequest::new(
                context(832),
                703,
                ArtifactKind::Result,
                1,
                2,
                2,
                *hasher.finalize().as_bytes(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    client
        .put_artifact_chunk(
            PutArtifactChunkRequest::new(
                context(833),
                704,
                ArtifactKind::Checkpoint,
                1,
                1_725_000_000_124,
                0,
                [0; 32],
                b"source".to_vec(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let source_head = adapter
        .multi_get(&[shard_runtime::analytics_artifact_generation_head_key(
            704,
            AnalyticsArtifactKindV1::Checkpoint,
            1,
        )])
        .await
        .unwrap()
        .pop()
        .flatten()
        .unwrap();
    let next_index = adapter.applied_log_index().unwrap() + 1;
    adapter
        .apply_committed(CommittedMutationBatch {
            shard_id: 11,
            log_index: next_index,
            txn_id: 8_033,
            mutations: vec![Mutation::put(
                0,
                shard_runtime::analytics_artifact_generation_head_key(
                    703,
                    AnalyticsArtifactKindV1::Result,
                    1,
                ),
                source_head,
            )],
        })
        .await
        .unwrap();

    assert!(matches!(
        client
            .list_artifact_generations(
                ListArtifactGenerationsRequest::new(context(834), 703, ArtifactKind::Result, 8,)
                    .unwrap(),
            )
            .await,
        Err(ShardClientError::ArtifactCorruption(_))
    ));
    assert!(matches!(
        client
            .list_artifact_generation_heads(
                ListArtifactGenerationHeadsRequest::new(context(835), None, 8).unwrap(),
            )
            .await,
        Err(ShardClientError::ArtifactCorruption(_))
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn embedded_artifact_producer_reads_only_on_demand_and_stops_after_drop() {
    let (client, adapter) = counting_client().await;
    let mut previous_digest = [0; 32];
    let mut content_hasher = blake3::Hasher::new();
    for ordinal in 0..17_u64 {
        let payload = vec![u8::try_from(ordinal + 1).unwrap()];
        content_hasher.update(&payload);
        client
            .put_artifact_chunk(
                PutArtifactChunkRequest::new(
                    context(600 + u128::from(ordinal)),
                    1_001,
                    ArtifactKind::Result,
                    1,
                    1_725_000_000_123,
                    ordinal,
                    previous_digest,
                    payload.clone(),
                )
                .unwrap(),
            )
            .await
            .unwrap();
        previous_digest = *blake3::hash(&payload).as_bytes();
    }
    let content_digest = *content_hasher.finalize().as_bytes();
    client
        .pin_artifact_generation(
            PinArtifactGenerationRequest::new(
                context(699),
                1_001,
                ArtifactKind::Result,
                1,
                17,
                17,
                content_digest,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    adapter.reset_artifact_chunk_reads();
    let mut stream = client
        .get_artifact_generation(
            GetArtifactGenerationRequest::new(
                context(700),
                1_001,
                ArtifactKind::Result,
                1,
                17,
                17,
                content_digest,
            )
            .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(adapter.artifact_chunk_reads(), 0);
    tokio::task::yield_now().await;
    assert_eq!(adapter.artifact_chunk_reads(), 0);
    assert!(stream.next().await.unwrap().is_ok());
    assert_eq!(adapter.artifact_chunk_reads(), 1);
    for _ in 1..8 {
        assert!(stream.next().await.unwrap().is_ok());
    }
    tokio::task::yield_now().await;
    assert_eq!(adapter.artifact_chunk_reads(), 1);
    assert!(stream.next().await.unwrap().is_ok());
    assert_eq!(adapter.artifact_chunk_reads(), 2);

    drop(stream);
    tokio::task::yield_now().await;
    assert_eq!(adapter.artifact_chunk_reads(), 2);

    let next_index = adapter.applied_log_index().unwrap() + 1;
    adapter
        .apply_committed(CommittedMutationBatch {
            shard_id: 11,
            log_index: next_index,
            txn_id: 9_999,
            mutations: vec![Mutation::put(
                0,
                shard_runtime::analytics_artifact_generation_pin_key(
                    1_001,
                    AnalyticsArtifactKindV1::Result,
                    1,
                ),
                b"corrupt-pin".to_vec(),
            )],
        })
        .await
        .unwrap();
    let corrupted = client
        .get_artifact_generation(
            GetArtifactGenerationRequest::new(
                context(701),
                1_001,
                ArtifactKind::Result,
                1,
                17,
                17,
                content_digest,
            )
            .unwrap(),
        )
        .await;
    assert!(matches!(
        corrupted,
        Err(ShardClientError::ArtifactCorruption(_))
    ));
}
