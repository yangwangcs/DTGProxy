use std::collections::BTreeMap;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{SystemTime, UNIX_EPOCH};

use raft_command::CommandEnvelopeV1;
use shard_runtime::MultiRaftRuntime;
use tokio::sync::Mutex;

use crate::{
    ExecuteCommand, ExecuteReceipt, ReadKeysRequest, ScanRequest, ShardClient, ShardClientError,
    ShardClientFuture, ShardRequestContext, ShardStatus,
};

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
}

impl ShardClient for EmbeddedShardClient {
    fn execute<'a>(&'a self, request: ExecuteCommand) -> ShardClientFuture<'a, ExecuteReceipt> {
        Box::pin(async move {
            let context = request.context();
            self.validate(context)?;
            let envelope = CommandEnvelopeV1::decode(request.command())
                .map_err(|error| ShardClientError::Replication(error.to_string()))?;
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
        })
    }

    fn read_keys<'a>(
        &'a self,
        request: ReadKeysRequest,
    ) -> ShardClientFuture<'a, Vec<Option<Vec<u8>>>> {
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
            group
                .leader_read_permit(leader, context.placement_epoch(), self.max_ticks)
                .await
                .map_err(|error| ShardClientError::ReadBarrier(error.to_string()))?;
            group
                .replica_adapter(leader)
                .ok_or(ShardClientError::NoLeader {
                    shard_id: context.shard_id(),
                })?
                .scan(request.span())
                .await
                .map_err(|error| ShardClientError::Adapter(error.to_string()))
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
            ))
        })
    }
}

fn unix_time_ms() -> Result<u64, ShardClientError> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ShardClientError::Internal("system clock before Unix epoch".into()))?
        .as_millis();
    u64::try_from(millis).map_err(|_| ShardClientError::Internal("system clock overflow".into()))
}
