use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex as StdMutex};
use std::task::{Context, Poll};
use std::time::{SystemTime, UNIX_EPOCH};

use raft_command::{
    AdvanceAnalyticsArtifactFenceV1, AnalyticsArtifactKindV1, CommandBodyV1, CommandEnvelopeV1,
    DeleteAnalyticsArtifactGenerationV1, PinAnalyticsArtifactGenerationV1,
    PutAnalyticsArtifactChunkV1,
};
use shard_runtime::{
    MultiRaftRuntime, analytics_artifact_chunk_key, analytics_artifact_generation_head_key,
    analytics_artifact_generation_head_prefix, analytics_artifact_generation_heads_prefix,
    analytics_artifact_generation_pin_key, decode_analytics_artifact_chunk,
    decode_analytics_artifact_generation_head, decode_analytics_artifact_generation_head_identity,
    decode_analytics_artifact_generation_head_key, decode_analytics_artifact_generation_pin,
};
use storage_api::{KeySpan, Keyspace, StorageAdapter};
use tokio::sync::Mutex;
use tokio_stream::Stream;

use crate::{
    AdvanceArtifactFenceRequest, ArtifactChunkStream, ArtifactGenerationHeadPage,
    ArtifactGenerationSummary, ArtifactKind, ArtifactStreamChunk, DeleteArtifactGenerationRequest,
    ExecuteCommand, ExecuteReceipt, GetArtifactGenerationRequest,
    ListArtifactGenerationHeadsRequest, ListArtifactGenerationsRequest,
    PinArtifactGenerationRequest, PutArtifactChunkRequest, ReadKeysRequest, ScanRequest,
    ShardClient, ShardClientError, ShardClientFuture, ShardRequestContext, ShardStatus,
    validated_artifact_stream,
};

const ARTIFACT_READ_BATCH_CHUNKS: usize = 8;

struct EmbeddedArtifactInput {
    adapter: Arc<dyn StorageAdapter>,
    request: GetArtifactGenerationRequest,
    applied_index: u64,
    count: usize,
    next_start: usize,
    buffered: VecDeque<ArtifactStreamChunk>,
    pending: Option<ArtifactBatchFuture>,
    expected_tail_digest: [u8; 32],
    observed_tail_digest: [u8; 32],
    terminal: bool,
}

type ArtifactBatchFuture = Pin<
    Box<
        dyn Future<Output = Result<(usize, Vec<Option<Vec<u8>>>), ShardClientError>>
            + Send
            + 'static,
    >,
>;

impl EmbeddedArtifactInput {
    fn start_next_batch(&mut self) -> Result<(), ShardClientError> {
        if unix_time_ms()? >= self.request.context().deadline_unix_ms() {
            return Err(ShardClientError::DeadlineExpired);
        }
        let start = self.next_start;
        let end = (start + ARTIFACT_READ_BATCH_CHUNKS).min(self.count);
        let keys = (start..end)
            .map(|ordinal| {
                analytics_artifact_chunk_key(
                    self.request.job_id(),
                    artifact_kind(self.request.kind()),
                    self.request.generation(),
                    u64::try_from(ordinal).expect("bounded artifact ordinal"),
                )
            })
            .collect::<Vec<_>>();
        let adapter = Arc::clone(&self.adapter);
        self.next_start = end;
        self.pending = Some(Box::pin(async move {
            let values = adapter
                .multi_get(&keys)
                .await
                .map_err(|error| ShardClientError::Adapter(error.to_string()))?;
            if values.len() != keys.len() {
                return Err(corrupt("artifact read returned the wrong number of values"));
            }
            Ok((start, values))
        }));
        Ok(())
    }

    fn retain_batch(
        &mut self,
        start: usize,
        values: Vec<Option<Vec<u8>>>,
    ) -> Result<(), ShardClientError> {
        for (offset, value) in values.into_iter().enumerate() {
            let value = value.ok_or_else(|| corrupt("artifact generation has a missing chunk"))?;
            let chunk = decode_analytics_artifact_chunk(&value)
                .map_err(|_| corrupt("artifact chunk failed integrity validation"))?;
            self.observed_tail_digest = chunk.payload_digest();
            self.buffered.push_back(ArtifactStreamChunk {
                applied_index: self.applied_index,
                ordinal: u64::try_from(start + offset).expect("bounded artifact ordinal"),
                previous_digest: chunk.previous_digest(),
                payload_digest: chunk.payload_digest(),
                payload: chunk.payload().to_vec(),
            });
        }
        Ok(())
    }
}

impl Stream for EmbeddedArtifactInput {
    type Item = Result<ArtifactStreamChunk, ShardClientError>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if self.terminal {
                return Poll::Ready(None);
            }
            if let Some(chunk) = self.buffered.pop_front() {
                return Poll::Ready(Some(Ok(chunk)));
            }
            if let Some(pending) = self.pending.as_mut() {
                match pending.as_mut().poll(context) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok((start, values))) => {
                        self.pending = None;
                        if let Err(error) = self.retain_batch(start, values) {
                            self.terminal = true;
                            return Poll::Ready(Some(Err(error)));
                        }
                        continue;
                    }
                    Poll::Ready(Err(error)) => {
                        self.pending = None;
                        self.terminal = true;
                        return Poll::Ready(Some(Err(error)));
                    }
                }
            }
            if self.next_start < self.count {
                if let Err(error) = self.start_next_batch() {
                    self.terminal = true;
                    return Poll::Ready(Some(Err(error)));
                }
                continue;
            }
            self.terminal = true;
            if self.observed_tail_digest != self.expected_tail_digest {
                return Poll::Ready(Some(Err(corrupt(
                    "artifact generation tail differs from its head",
                ))));
            }
            return Poll::Ready(None);
        }
    }
}

pub struct EmbeddedShardClient {
    graph_id: u64,
    max_ticks: usize,
    runtime: Arc<Mutex<MultiRaftRuntime>>,
    completed: StdMutex<BTreeMap<u128, Vec<u8>>>,
}

impl EmbeddedShardClient {
    pub fn new(
        graph_id: u64,
        max_ticks: usize,
        runtime: Arc<Mutex<MultiRaftRuntime>>,
    ) -> Result<Self, ShardClientError> {
        if graph_id == 0 || max_ticks == 0 {
            return Err(ShardClientError::InvalidContext);
        }
        Ok(Self {
            graph_id,
            max_ticks,
            runtime,
            completed: StdMutex::new(BTreeMap::new()),
        })
    }

    #[must_use]
    pub fn runtime(&self) -> Arc<Mutex<MultiRaftRuntime>> {
        Arc::clone(&self.runtime)
    }

    fn validate(&self, context: ShardRequestContext) -> Result<(), ShardClientError> {
        if context.graph_id() != self.graph_id {
            return Err(ShardClientError::WrongGraph {
                expected: self.graph_id,
                actual: context.graph_id(),
            });
        }
        if unix_time_ms()? >= context.deadline_unix_ms() {
            return Err(ShardClientError::DeadlineExpired);
        }
        Ok(())
    }

    async fn execute_command(
        &self,
        request: ExecuteCommand,
        artifact_rpc: bool,
    ) -> Result<ExecuteReceipt, ShardClientError> {
        let context = request.context();
        self.validate(context)?;
        let envelope = CommandEnvelopeV1::decode(request.command())
            .map_err(|error| ShardClientError::Replication(error.to_string()))?;
        if !artifact_rpc
            && matches!(
                envelope.body,
                CommandBodyV1::PutAnalyticsArtifactChunk(_)
                    | CommandBodyV1::PinAnalyticsArtifactGeneration(_)
                    | CommandBodyV1::DeleteAnalyticsArtifactGeneration(_)
            )
        {
            return Err(ShardClientError::InvalidArtifact);
        }
        if envelope.request_id != context.request_id() {
            return Err(ShardClientError::RequestMismatch {
                expected: envelope.request_id,
                actual: context.request_id(),
            });
        }
        let duplicate = {
            let completed = self
                .completed
                .lock()
                .map_err(|_| ShardClientError::Internal("dedup lock poisoned".into()))?;
            completed.contains_key(&context.request_id())
        };
        let mut runtime = self.runtime.lock().await;
        let receipt = runtime
            .group_mut(context.shard_id())
            .map_err(|error| ShardClientError::Replication(error.to_string()))?
            .propose_and_wait(request.command().to_vec(), self.max_ticks)
            .await
            .map_err(|error| ShardClientError::Replication(error.to_string()))?;
        self.completed
            .lock()
            .map_err(|_| ShardClientError::Internal("dedup lock poisoned".into()))?
            .insert(context.request_id(), request.into_command());
        Ok(ExecuteReceipt::new(receipt.index, duplicate))
    }
}

impl ShardClient for EmbeddedShardClient {
    fn execute<'a>(&'a self, request: ExecuteCommand) -> ShardClientFuture<'a, ExecuteReceipt> {
        Box::pin(self.execute_command(request, false))
    }

    fn read_keys<'a>(
        &'a self,
        request: ReadKeysRequest,
    ) -> ShardClientFuture<'a, Vec<Option<Vec<u8>>>> {
        Box::pin(async move {
            let context = request.context();
            self.validate(context)?;
            if request
                .keys()
                .iter()
                .any(|key| key.keyspace() == storage_api::Keyspace::Meta)
            {
                return Err(ShardClientError::InvalidArtifact);
            }
            let mut runtime = self.runtime.lock().await;
            let group = runtime
                .group_mut(context.shard_id())
                .map_err(|error| ShardClientError::Replication(error.to_string()))?;
            let leader = group.leader_id().ok_or(ShardClientError::NoLeader {
                shard_id: context.shard_id(),
            })?;
            group
                .leader_read_permit(leader, context.placement_epoch(), self.max_ticks)
                .await
                .map_err(|error| ShardClientError::ReadBarrier(error.to_string()))?;
            group
                .replica_adapter(leader)
                .ok_or(ShardClientError::NoLeader {
                    shard_id: context.shard_id(),
                })?
                .multi_get(request.keys())
                .await
                .map_err(|error| ShardClientError::Adapter(error.to_string()))
        })
    }

    fn scan<'a>(
        &'a self,
        request: ScanRequest,
    ) -> ShardClientFuture<'a, Vec<storage_api::KeyValue>> {
        Box::pin(async move { Ok(self.scan_fenced(request).await?.into_entries()) })
    }

    fn scan_fenced<'a>(&'a self, request: ScanRequest) -> ShardClientFuture<'a, crate::FencedScan> {
        Box::pin(async move {
            let context = request.context();
            self.validate(context)?;
            if request.span().keyspace() == storage_api::Keyspace::Meta {
                return Err(ShardClientError::InvalidArtifact);
            }
            let mut runtime = self.runtime.lock().await;
            let group = runtime
                .group_mut(context.shard_id())
                .map_err(|error| ShardClientError::Replication(error.to_string()))?;
            let leader = group.leader_id().ok_or(ShardClientError::NoLeader {
                shard_id: context.shard_id(),
            })?;
            let permit = group
                .leader_read_permit(leader, context.placement_epoch(), self.max_ticks)
                .await
                .map_err(|error| ShardClientError::ReadBarrier(error.to_string()))?;
            let adapter = group
                .replica_adapter(leader)
                .ok_or(ShardClientError::NoLeader {
                    shard_id: context.shard_id(),
                })?;
            let scan = adapter
                .scan_fenced(request.span())
                .await
                .map_err(|error| ShardClientError::Adapter(error.to_string()))?;
            let applied_index = scan.applied_log_index();
            if applied_index < permit.read_index() {
                return Err(ShardClientError::ReadBarrier(format!(
                    "Adapter applied index {applied_index} is below ReadIndex {}",
                    permit.read_index()
                )));
            }
            Ok(crate::FencedScan::new(applied_index, scan.into_entries()))
        })
    }

    fn read_barrier<'a>(&'a self, context: ShardRequestContext) -> ShardClientFuture<'a, u64> {
        Box::pin(async move {
            self.validate(context)?;
            let mut runtime = self.runtime.lock().await;
            let group = runtime
                .group_mut(context.shard_id())
                .map_err(|error| ShardClientError::Replication(error.to_string()))?;
            let leader = group.leader_id().ok_or(ShardClientError::NoLeader {
                shard_id: context.shard_id(),
            })?;
            let permit = group
                .leader_read_permit(leader, context.placement_epoch(), self.max_ticks)
                .await
                .map_err(|error| ShardClientError::ReadBarrier(error.to_string()))?;
            Ok(permit.read_index())
        })
    }

    fn put_artifact_chunk<'a>(
        &'a self,
        request: PutArtifactChunkRequest,
    ) -> ShardClientFuture<'a, ExecuteReceipt> {
        Box::pin(async move {
            let context = request.context();
            self.validate(context)?;
            let command = PutAnalyticsArtifactChunkV1::new(
                request.job_id(),
                artifact_kind(request.kind()),
                request.generation(),
                request.created_at_unix_ms(),
                request.ordinal(),
                request.previous_digest(),
                request.payload().to_vec(),
            )
            .map_err(|_| ShardClientError::InvalidArtifact)?;
            let encoded = CommandEnvelopeV1::new(
                context.shard_id(),
                context.placement_epoch(),
                context.request_id(),
                CommandBodyV1::PutAnalyticsArtifactChunk(command),
            )
            .encode()
            .map_err(|error| ShardClientError::Replication(error.to_string()))?;
            self.execute_command(ExecuteCommand::new(context, encoded)?, true)
                .await
        })
    }

    fn pin_artifact_generation<'a>(
        &'a self,
        request: PinArtifactGenerationRequest,
    ) -> ShardClientFuture<'a, ExecuteReceipt> {
        Box::pin(async move {
            let context = request.context();
            self.validate(context)?;
            let command = PinAnalyticsArtifactGenerationV1::new(
                request.job_id(),
                artifact_kind(request.kind()),
                request.generation(),
                request.expected_chunk_count(),
                request.expected_total_bytes(),
                request.expected_content_digest(),
            )
            .map_err(|_| ShardClientError::InvalidArtifact)?;
            let encoded = CommandEnvelopeV1::new(
                context.shard_id(),
                context.placement_epoch(),
                context.request_id(),
                CommandBodyV1::PinAnalyticsArtifactGeneration(command),
            )
            .encode()
            .map_err(|error| ShardClientError::Replication(error.to_string()))?;
            self.execute_command(ExecuteCommand::new(context, encoded)?, true)
                .await
        })
    }

    fn get_artifact_generation<'a>(
        &'a self,
        request: GetArtifactGenerationRequest,
    ) -> ShardClientFuture<'a, ArtifactChunkStream> {
        Box::pin(async move {
            let context = request.context();
            self.validate(context)?;
            let (applied_index, adapter) = {
                let mut runtime = self.runtime.lock().await;
                let group = runtime
                    .group_mut(context.shard_id())
                    .map_err(|error| ShardClientError::Replication(error.to_string()))?;
                let leader = group.leader_id().ok_or(ShardClientError::NoLeader {
                    shard_id: context.shard_id(),
                })?;
                let permit = group
                    .leader_read_permit(leader, context.placement_epoch(), self.max_ticks)
                    .await
                    .map_err(|error| ShardClientError::ReadBarrier(error.to_string()))?;
                let adapter =
                    group
                        .replica_adapter_arc(leader)
                        .ok_or(ShardClientError::NoLeader {
                            shard_id: context.shard_id(),
                        })?;
                (permit.read_index(), adapter)
            };
            let count = usize::from(request.expected_chunk_count());
            let mut records = adapter
                .multi_get(&[
                    analytics_artifact_generation_pin_key(
                        request.job_id(),
                        artifact_kind(request.kind()),
                        request.generation(),
                    ),
                    analytics_artifact_generation_head_key(
                        request.job_id(),
                        artifact_kind(request.kind()),
                        request.generation(),
                    ),
                ])
                .await
                .map_err(|error| ShardClientError::Adapter(error.to_string()))?;
            if records.len() != 2 {
                return Err(corrupt(
                    "artifact pin/head read returned an invalid result count",
                ));
            }
            let pin = records
                .remove(0)
                .ok_or_else(|| corrupt("artifact generation is not pinned"))?;
            let pin = decode_analytics_artifact_generation_pin(&pin)
                .map_err(|_| corrupt("artifact generation pin failed integrity validation"))?;
            if pin.expected_chunk_count() != request.expected_chunk_count()
                || pin.expected_total_bytes() != request.expected_total_bytes()
                || pin.expected_content_digest() != request.expected_content_digest()
            {
                return Err(corrupt(
                    "artifact generation pin differs from the requested manifest",
                ));
            }
            let head = records
                .pop()
                .flatten()
                .ok_or_else(|| corrupt("artifact generation has no head"))?;
            let head = decode_analytics_artifact_generation_head(&head)
                .map_err(|_| corrupt("artifact generation head failed integrity validation"))?;
            if usize::from(head.count()) != count {
                return Err(corrupt(
                    "artifact generation count differs from the requested count",
                ));
            }
            let expected_tail_digest = head.last_digest();
            Ok(validated_artifact_stream(
                Box::pin(EmbeddedArtifactInput {
                    adapter,
                    request,
                    applied_index,
                    count,
                    next_start: 0,
                    buffered: VecDeque::with_capacity(ARTIFACT_READ_BATCH_CHUNKS),
                    pending: None,
                    expected_tail_digest,
                    observed_tail_digest: [0; 32],
                    terminal: false,
                }),
                request,
            ))
        })
    }

    fn delete_artifact_generation<'a>(
        &'a self,
        request: DeleteArtifactGenerationRequest,
    ) -> ShardClientFuture<'a, ExecuteReceipt> {
        Box::pin(async move {
            let context = request.context();
            self.validate(context)?;
            let command = DeleteAnalyticsArtifactGenerationV1::new_with_gc_epoch(
                request.job_id(),
                artifact_kind(request.kind()),
                request.generation(),
                request.gc_epoch(),
            )
            .map_err(|_| ShardClientError::InvalidArtifact)?;
            let encoded = CommandEnvelopeV1::new(
                context.shard_id(),
                context.placement_epoch(),
                context.request_id(),
                CommandBodyV1::DeleteAnalyticsArtifactGeneration(command),
            )
            .encode()
            .map_err(|error| ShardClientError::Replication(error.to_string()))?;
            self.execute_command(ExecuteCommand::new(context, encoded)?, true)
                .await
        })
    }

    fn advance_artifact_fence<'a>(
        &'a self,
        request: AdvanceArtifactFenceRequest,
    ) -> ShardClientFuture<'a, ExecuteReceipt> {
        Box::pin(async move {
            let context = request.context();
            self.validate(context)?;
            let command = AdvanceAnalyticsArtifactFenceV1::new(
                request.job_id(),
                artifact_kind(request.kind()),
                request.generation(),
                request.gc_epoch(),
            )
            .map_err(|_| ShardClientError::InvalidArtifact)?;
            let encoded = CommandEnvelopeV1::new(
                context.shard_id(),
                context.placement_epoch(),
                context.request_id(),
                CommandBodyV1::AdvanceAnalyticsArtifactFence(command),
            )
            .encode()
            .map_err(|error| ShardClientError::Replication(error.to_string()))?;
            self.execute_command(ExecuteCommand::new(context, encoded)?, true)
                .await
        })
    }

    fn list_artifact_generations<'a>(
        &'a self,
        request: ListArtifactGenerationsRequest,
    ) -> ShardClientFuture<'a, Vec<ArtifactGenerationSummary>> {
        Box::pin(async move {
            let context = request.context();
            self.validate(context)?;
            let mut runtime = self.runtime.lock().await;
            let group = runtime
                .group_mut(context.shard_id())
                .map_err(|error| ShardClientError::Replication(error.to_string()))?;
            let leader = group.leader_id().ok_or(ShardClientError::NoLeader {
                shard_id: context.shard_id(),
            })?;
            let permit = group
                .leader_read_permit(leader, context.placement_epoch(), self.max_ticks)
                .await
                .map_err(|error| ShardClientError::ReadBarrier(error.to_string()))?;
            let adapter = group
                .replica_adapter(leader)
                .ok_or(ShardClientError::NoLeader {
                    shard_id: context.shard_id(),
                })?;
            let span = KeySpan::prefix(
                Keyspace::Meta,
                analytics_artifact_generation_head_prefix(
                    request.job_id(),
                    artifact_kind(request.kind()),
                ),
            )
            .with_limit(usize::try_from(request.limit()).expect("bounded artifact limit"))
            .map_err(|error| ShardClientError::Internal(error.to_string()))?;
            let rows = adapter
                .scan(&span)
                .await
                .map_err(|error| ShardClientError::Adapter(error.to_string()))?;
            let mut summaries = Vec::with_capacity(rows.len());
            for row in rows {
                let generation = decode_analytics_artifact_generation_head_key(
                    row.key().as_bytes(),
                    request.job_id(),
                    artifact_kind(request.kind()),
                )
                .map_err(|_| ShardClientError::ArtifactCorruption("invalid head key".into()))?;
                let head = decode_analytics_artifact_generation_head(row.value())
                    .map_err(|_| ShardClientError::ArtifactCorruption("invalid head".into()))?;
                let pin_key = analytics_artifact_generation_pin_key(
                    request.job_id(),
                    artifact_kind(request.kind()),
                    generation,
                );
                let pin = adapter
                    .multi_get(&[pin_key])
                    .await
                    .map_err(|error| ShardClientError::Adapter(error.to_string()))?
                    .into_iter()
                    .next()
                    .flatten()
                    .map(|bytes| {
                        decode_analytics_artifact_generation_pin(&bytes)
                            .map_err(|_| ShardClientError::ArtifactCorruption("invalid pin".into()))
                    })
                    .transpose()?;
                if pin.is_some_and(|pin| pin.expected_chunk_count() != head.count()) {
                    return Err(ShardClientError::ArtifactCorruption(
                        "pin disagrees with head".into(),
                    ));
                }
                summaries.push(ArtifactGenerationSummary {
                    job_id: request.job_id(),
                    kind: request.kind(),
                    generation,
                    created_at_unix_ms: head.created_at_unix_ms(),
                    expected_chunk_count: pin
                        .map_or(head.count(), |value| value.expected_chunk_count()),
                    expected_total_bytes: pin.map_or(0, |value| value.expected_total_bytes()),
                    expected_content_digest: pin
                        .map_or([0; 32], |value| value.expected_content_digest()),
                    pinned: pin.is_some(),
                    applied_index: permit.read_index(),
                });
            }
            Ok(summaries)
        })
    }

    fn list_artifact_generation_heads<'a>(
        &'a self,
        request: ListArtifactGenerationHeadsRequest,
    ) -> ShardClientFuture<'a, ArtifactGenerationHeadPage> {
        Box::pin(async move {
            let context = request.context();
            self.validate(context)?;
            let mut runtime = self.runtime.lock().await;
            let group = runtime
                .group_mut(context.shard_id())
                .map_err(|error| ShardClientError::Replication(error.to_string()))?;
            let leader = group.leader_id().ok_or(ShardClientError::NoLeader {
                shard_id: context.shard_id(),
            })?;
            let permit = group
                .leader_read_permit(leader, context.placement_epoch(), self.max_ticks)
                .await
                .map_err(|error| ShardClientError::ReadBarrier(error.to_string()))?;
            let adapter = group
                .replica_adapter(leader)
                .ok_or(ShardClientError::NoLeader {
                    shard_id: context.shard_id(),
                })?;
            let prefix = analytics_artifact_generation_heads_prefix().to_vec();
            let span = if let Some(after) = request.after() {
                let mut start = analytics_artifact_generation_head_key(
                    after.job_id(),
                    artifact_kind(after.kind()),
                    after.generation(),
                )
                .as_bytes()
                .to_vec();
                start.push(0);
                KeySpan::prefix_from(Keyspace::Meta, prefix, start)
            } else {
                Ok(KeySpan::prefix(Keyspace::Meta, prefix))
            }
            .and_then(|span| {
                span.with_limit(
                    usize::try_from(request.limit()).expect("bounded artifact head page limit"),
                )
            })
            .map_err(|error| ShardClientError::Internal(error.to_string()))?;
            let rows = adapter
                .scan(&span)
                .await
                .map_err(|error| ShardClientError::Adapter(error.to_string()))?;
            let full_page = rows.len()
                == usize::try_from(request.limit()).expect("bounded artifact head page limit");
            let mut heads = Vec::with_capacity(rows.len());
            let mut pin_keys = Vec::with_capacity(rows.len());
            for row in rows {
                let identity =
                    decode_analytics_artifact_generation_head_identity(row.key().as_bytes())
                        .map_err(|_| {
                            ShardClientError::ArtifactCorruption("invalid head key".into())
                        })?;
                let head = decode_analytics_artifact_generation_head(row.value())
                    .map_err(|_| ShardClientError::ArtifactCorruption("invalid head".into()))?;
                pin_keys.push(analytics_artifact_generation_pin_key(
                    identity.job_id(),
                    identity.kind(),
                    identity.generation(),
                ));
                heads.push((identity, head));
            }
            let pins = adapter
                .multi_get(&pin_keys)
                .await
                .map_err(|error| ShardClientError::Adapter(error.to_string()))?;
            if pins.len() != heads.len() {
                return Err(ShardClientError::ArtifactCorruption(
                    "invalid pin result count".into(),
                ));
            }
            let mut generations = Vec::with_capacity(heads.len());
            for ((identity, head), pin) in heads.into_iter().zip(pins) {
                let pin = pin
                    .map(|bytes| {
                        decode_analytics_artifact_generation_pin(&bytes)
                            .map_err(|_| ShardClientError::ArtifactCorruption("invalid pin".into()))
                    })
                    .transpose()?;
                if pin.is_some_and(|pin| pin.expected_chunk_count() != head.count()) {
                    return Err(ShardClientError::ArtifactCorruption(
                        "pin disagrees with head".into(),
                    ));
                }
                generations.push(ArtifactGenerationSummary {
                    job_id: identity.job_id(),
                    kind: client_artifact_kind(identity.kind()),
                    generation: identity.generation(),
                    created_at_unix_ms: head.created_at_unix_ms(),
                    expected_chunk_count: pin
                        .map_or(head.count(), |value| value.expected_chunk_count()),
                    expected_total_bytes: pin.map_or(0, |value| value.expected_total_bytes()),
                    expected_content_digest: pin
                        .map_or([0; 32], |value| value.expected_content_digest()),
                    pinned: pin.is_some(),
                    applied_index: permit.read_index(),
                });
            }
            let next = full_page
                .then(|| generations.last().map(ArtifactGenerationSummary::cursor))
                .flatten();
            Ok(ArtifactGenerationHeadPage {
                applied_index: permit.read_index(),
                generations,
                next,
            })
        })
    }

    fn status<'a>(&'a self, context: ShardRequestContext) -> ShardClientFuture<'a, ShardStatus> {
        Box::pin(async move {
            self.validate(context)?;
            let runtime = self.runtime.lock().await;
            let group = runtime.group(context.shard_id()).ok_or_else(|| {
                ShardClientError::Replication(format!(
                    "Shard {} is not hosted by the embedded runtime",
                    context.shard_id()
                ))
            })?;
            let leader = group.leader_id().ok_or(ShardClientError::NoLeader {
                shard_id: context.shard_id(),
            })?;
            let metadata = group
                .replica_metadata(leader)
                .ok_or(ShardClientError::NoLeader {
                    shard_id: context.shard_id(),
                })?;
            if metadata.placement_epoch != context.placement_epoch() {
                return Err(ShardClientError::ReadBarrier(format!(
                    "stale placement epoch {}; current is {}",
                    context.placement_epoch(),
                    metadata.placement_epoch
                )));
            }
            Ok(ShardStatus::new(
                leader,
                leader,
                metadata.last_term,
                metadata.applied_index,
                metadata.closed_ts,
                storage_api::QueryCapabilitySnapshot::new(
                    1,
                    storage_api::QueryPrimitiveCapabilities::NONE,
                ),
            ))
        })
    }
}

fn artifact_kind(kind: ArtifactKind) -> AnalyticsArtifactKindV1 {
    match kind {
        ArtifactKind::Checkpoint => AnalyticsArtifactKindV1::Checkpoint,
        ArtifactKind::Result => AnalyticsArtifactKindV1::Result,
    }
}

fn client_artifact_kind(kind: AnalyticsArtifactKindV1) -> ArtifactKind {
    match kind {
        AnalyticsArtifactKindV1::Checkpoint => ArtifactKind::Checkpoint,
        AnalyticsArtifactKindV1::Result => ArtifactKind::Result,
    }
}

fn corrupt(message: &'static str) -> ShardClientError {
    ShardClientError::ArtifactCorruption(message.into())
}

fn unix_time_ms() -> Result<u64, ShardClientError> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ShardClientError::Internal("system clock before Unix epoch".into()))?
        .as_millis();
    u64::try_from(millis).map_err(|_| ShardClientError::Internal("system clock overflow".into()))
}
