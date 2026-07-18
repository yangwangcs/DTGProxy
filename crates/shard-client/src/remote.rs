use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cluster_protocol::CLUSTER_PROTOCOL_VERSION;
use cluster_protocol::proto::shard_service_client::ShardServiceClient;
use cluster_protocol::proto::{
    ExecuteRequest as WireExecuteRequest, ReadRequest as WireReadRequest, ReplicaStatusRequest,
    RequestContext, ScanRequest as WireScanRequest, ShardContext,
};
use data_node::{
    decode_key_read_result, decode_key_scan_batch, encode_key_read_plan, encode_key_scan_plan,
};
use storage_api::KeyValue;
use temporal_types::TransactionTime;
use tokio_stream::StreamExt;
use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Request, Status};

use crate::{
    ExecuteCommand, ExecuteReceipt, ReadKeysRequest, ScanRequest, ShardClient, ShardClientError,
    ShardClientFuture, ShardRequestContext, ShardStatus,
};

const MAXIMUM_SCAN_BATCH_BYTES: u32 = 4 * 1024 * 1024;
const MAXIMUM_RETRY_ATTEMPTS: usize = 3;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RemoteReplica {
    node_id: u64,
    address: SocketAddr,
}

impl RemoteReplica {
    pub fn new(node_id: u64, address: SocketAddr) -> Result<Self, RemoteTopologyError> {
        if node_id == 0 || address.port() == 0 || !address.ip().is_loopback() {
            return Err(RemoteTopologyError::InvalidReplica);
        }
        Ok(Self { node_id, address })
    }

    #[must_use]
    pub const fn node_id(self) -> u64 {
        self.node_id
    }

    #[must_use]
    pub const fn address(self) -> SocketAddr {
        self.address
    }
}

#[derive(Clone, Debug)]
struct RemoteShardRoute {
    placement_epoch: u64,
    leader_id: u64,
    replicas: Vec<RemoteReplica>,
}

#[derive(Clone, Debug)]
pub struct RemoteTopology {
    revision: u64,
    graph_id: u64,
    shards: BTreeMap<u32, RemoteShardRoute>,
}

impl RemoteTopology {
    pub fn new(
        revision: u64,
        graph_id: u64,
        routes: Vec<(u32, u64, u64, Vec<RemoteReplica>)>,
    ) -> Result<Self, RemoteTopologyError> {
        if revision == 0 || graph_id == 0 || routes.is_empty() {
            return Err(RemoteTopologyError::InvalidTopology);
        }
        let mut shards = BTreeMap::new();
        for (shard_id, placement_epoch, leader_id, replicas) in routes {
            let node_ids: BTreeSet<_> = replicas.iter().map(|replica| replica.node_id).collect();
            if shard_id == 0
                || placement_epoch == 0
                || leader_id == 0
                || replicas.is_empty()
                || node_ids.len() != replicas.len()
                || !node_ids.contains(&leader_id)
                || shards
                    .insert(
                        shard_id,
                        RemoteShardRoute {
                            placement_epoch,
                            leader_id,
                            replicas,
                        },
                    )
                    .is_some()
            {
                return Err(RemoteTopologyError::InvalidTopology);
            }
        }
        Ok(Self {
            revision,
            graph_id,
            shards,
        })
    }

    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }
}

pub struct RemoteShardClient {
    cluster_id: [u8; 16],
    topology: RwLock<Arc<RemoteTopology>>,
    channels: Mutex<BTreeMap<u64, Channel>>,
}

impl RemoteShardClient {
    pub fn new_loopback_plaintext(
        cluster_id: [u8; 16],
        topology: RemoteTopology,
    ) -> Result<Self, RemoteTopologyError> {
        if cluster_id == [0; 16] {
            return Err(RemoteTopologyError::InvalidTopology);
        }
        Ok(Self {
            cluster_id,
            topology: RwLock::new(Arc::new(topology)),
            channels: Mutex::new(BTreeMap::new()),
        })
    }

    pub fn install_topology(&self, topology: RemoteTopology) -> Result<bool, RemoteTopologyError> {
        let mut current = self
            .topology
            .write()
            .map_err(|_| RemoteTopologyError::LockPoisoned)?;
        if topology.graph_id != current.graph_id {
            return Err(RemoteTopologyError::GraphChanged);
        }
        if topology.revision <= current.revision {
            return Ok(false);
        }
        *current = Arc::new(topology);
        Ok(true)
    }

    fn route(
        &self,
        context: ShardRequestContext,
    ) -> Result<(Arc<RemoteTopology>, RemoteShardRoute), ShardClientError> {
        validate_deadline(context)?;
        let topology = {
            let current = self
                .topology
                .read()
                .map_err(|_| ShardClientError::Internal("topology lock poisoned".into()))?;
            Arc::clone(&current)
        };
        if context.graph_id() != topology.graph_id {
            return Err(ShardClientError::WrongGraph {
                expected: topology.graph_id,
                actual: context.graph_id(),
            });
        }
        let route = topology
            .shards
            .get(&context.shard_id())
            .cloned()
            .ok_or_else(|| {
                ShardClientError::Replication(format!(
                    "Shard {} is absent from topology revision {}",
                    context.shard_id(),
                    topology.revision
                ))
            })?;
        if context.placement_epoch() != route.placement_epoch {
            return Err(ShardClientError::StaleEpoch {
                current_epoch: Some(route.placement_epoch),
            });
        }
        Ok((topology, route))
    }

    fn channel(&self, replica: RemoteReplica) -> Result<Channel, ShardClientError> {
        let mut channels = self
            .channels
            .lock()
            .map_err(|_| ShardClientError::Internal("channel pool lock poisoned".into()))?;
        if let Some(channel) = channels.get(&replica.node_id) {
            return Ok(channel.clone());
        }
        let endpoint = Endpoint::from_shared(format!("http://{}", replica.address))
            .map_err(|error| ShardClientError::Internal(error.to_string()))?
            .connect_timeout(Duration::from_secs(2))
            .tcp_nodelay(true);
        let channel = endpoint.connect_lazy();
        channels.insert(replica.node_id, channel.clone());
        Ok(channel)
    }

    fn candidates(route: &RemoteShardRoute) -> Vec<RemoteReplica> {
        let mut replicas = route.replicas.clone();
        replicas.sort_by_key(|replica| (replica.node_id != route.leader_id, replica.node_id));
        replicas.truncate(MAXIMUM_RETRY_ATTEMPTS);
        replicas
    }

    fn wire_context(&self, context: ShardRequestContext) -> ShardContext {
        ShardContext {
            request: Some(RequestContext {
                protocol_version: CLUSTER_PROTOCOL_VERSION,
                cluster_id: self.cluster_id.to_vec(),
                request_id: context.request_id().to_be_bytes().to_vec(),
                deadline_unix_ms: context.deadline_unix_ms(),
            }),
            graph_id: context.graph_id(),
            shard_id: context.shard_id(),
            placement_epoch: context.placement_epoch(),
        }
    }

    fn request<T>(
        context: ShardRequestContext,
        payload: T,
    ) -> Result<Request<T>, ShardClientError> {
        let remaining = context
            .deadline_unix_ms()
            .checked_sub(unix_time_ms()?)
            .filter(|remaining| *remaining > 0)
            .ok_or(ShardClientError::DeadlineExpired)?;
        let mut request = Request::new(payload);
        request.set_timeout(Duration::from_millis(remaining));
        Ok(request)
    }
}

impl ShardClient for RemoteShardClient {
    fn execute<'a>(&'a self, request: ExecuteCommand) -> ShardClientFuture<'a, ExecuteReceipt> {
        Box::pin(async move {
            let context = request.context();
            let (_, route) = self.route(context)?;
            let payload = request.command().to_vec();
            let mut last_error = None;
            for replica in Self::candidates(&route) {
                validate_deadline(context)?;
                let channel = self.channel(replica)?;
                let mut client = ShardServiceClient::new(channel);
                let wire = WireExecuteRequest {
                    context: Some(self.wire_context(context)),
                    command: payload.clone(),
                };
                match client.execute(Self::request(context, wire)?).await {
                    Ok(response) => {
                        let response = response.into_inner();
                        return Ok(ExecuteReceipt::new(response.raft_index, response.duplicate));
                    }
                    Err(status) => {
                        let error = map_status(&status);
                        if !retryable(&error) {
                            return Err(error);
                        }
                        last_error = Some(error);
                    }
                }
            }
            Err(last_error.unwrap_or(ShardClientError::NoLeader {
                shard_id: context.shard_id(),
            }))
        })
    }

    fn read_keys<'a>(
        &'a self,
        request: ReadKeysRequest,
    ) -> ShardClientFuture<'a, Vec<Option<Vec<u8>>>> {
        Box::pin(async move {
            let context = request.context();
            let (_, route) = self.route(context)?;
            let plan = encode_key_read_plan(request.keys())
                .map_err(|error| ShardClientError::Internal(error.to_string()))?;
            let replica =
                Self::candidates(&route)
                    .into_iter()
                    .next()
                    .ok_or(ShardClientError::NoLeader {
                        shard_id: context.shard_id(),
                    })?;
            let mut client = ShardServiceClient::new(self.channel(replica)?);
            let wire = WireReadRequest {
                context: Some(self.wire_context(context)),
                plan,
                read_proof: Vec::new(),
            };
            let response = client
                .read(Self::request(context, wire)?)
                .await
                .map_err(|status| map_status(&status))?
                .into_inner();
            decode_key_read_result(&response.result)
                .map_err(|error| ShardClientError::Internal(error.to_string()))
        })
    }

    fn scan<'a>(&'a self, request: ScanRequest) -> ShardClientFuture<'a, Vec<KeyValue>> {
        Box::pin(async move {
            let context = request.context();
            let (_, route) = self.route(context)?;
            let plan = encode_key_scan_plan(request.span())
                .map_err(|error| ShardClientError::Internal(error.to_string()))?;
            let replica =
                Self::candidates(&route)
                    .into_iter()
                    .next()
                    .ok_or(ShardClientError::NoLeader {
                        shard_id: context.shard_id(),
                    })?;
            let mut client = ShardServiceClient::new(self.channel(replica)?);
            let wire = WireScanRequest {
                context: Some(self.wire_context(context)),
                plan,
                read_proof: Vec::new(),
                maximum_batch_bytes: MAXIMUM_SCAN_BATCH_BYTES,
            };
            let mut stream = client
                .scan(Self::request(context, wire)?)
                .await
                .map_err(|status| map_status(&status))?
                .into_inner();
            let mut rows = Vec::new();
            let mut expected_sequence = 0_u64;
            let mut applied_index = None;
            let mut terminal = false;
            while let Some(batch) = stream.next().await {
                validate_deadline(context)?;
                let batch = batch.map_err(|status| map_status(&status))?;
                if batch.sequence != expected_sequence
                    || applied_index.is_some_and(|index| index != batch.applied_index)
                    || terminal
                {
                    return Err(ShardClientError::Internal(
                        "invalid remote scan stream ordering".into(),
                    ));
                }
                applied_index = Some(batch.applied_index);
                expected_sequence = expected_sequence.saturating_add(1);
                terminal = batch.terminal;
                rows.extend(
                    decode_key_scan_batch(&batch.arrow_record_batch)
                        .map_err(|error| ShardClientError::Internal(error.to_string()))?,
                );
            }
            if !terminal {
                return Err(ShardClientError::Internal(
                    "remote scan ended without a terminal batch".into(),
                ));
            }
            Ok(rows)
        })
    }

    fn status<'a>(&'a self, context: ShardRequestContext) -> ShardClientFuture<'a, ShardStatus> {
        Box::pin(async move {
            let (_, route) = self.route(context)?;
            let replica =
                Self::candidates(&route)
                    .into_iter()
                    .next()
                    .ok_or(ShardClientError::NoLeader {
                        shard_id: context.shard_id(),
                    })?;
            let mut client = ShardServiceClient::new(self.channel(replica)?);
            let wire = ReplicaStatusRequest {
                context: Some(self.wire_context(context)),
            };
            let status = client
                .replica_status(Self::request(context, wire)?)
                .await
                .map_err(|status| map_status(&status))?
                .into_inner();
            Ok(ShardStatus::new(
                status.node_id,
                route.leader_id,
                status.term,
                status.applied_index,
                TransactionTime::new(i64::MIN, 0),
            ))
        })
    }
}

fn retryable(error: &ShardClientError) -> bool {
    matches!(
        error,
        ShardClientError::NotLeader { .. } | ShardClientError::Replication(_)
    )
}

fn map_status(status: &Status) -> ShardClientError {
    let reason = status
        .metadata()
        .get("dtgproxy-reason")
        .and_then(|value| value.to_str().ok());
    if reason == Some("not_leader") {
        return ShardClientError::NotLeader {
            leader_hint: metadata_u64(status, "dtgproxy-leader-node"),
        };
    }
    if reason == Some("stale_epoch") {
        return ShardClientError::StaleEpoch {
            current_epoch: metadata_u64(status, "dtgproxy-current-epoch"),
        };
    }
    match status.code() {
        Code::DeadlineExceeded | Code::Cancelled => ShardClientError::DeadlineExpired,
        Code::Unavailable | Code::ResourceExhausted => {
            ShardClientError::Replication(status.to_string())
        }
        _ => ShardClientError::Internal(status.to_string()),
    }
}

fn metadata_u64(status: &Status, name: &'static str) -> Option<u64> {
    status
        .metadata()
        .get(name)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok())
}

fn validate_deadline(context: ShardRequestContext) -> Result<(), ShardClientError> {
    if unix_time_ms()? >= context.deadline_unix_ms() {
        return Err(ShardClientError::DeadlineExpired);
    }
    Ok(())
}

fn unix_time_ms() -> Result<u64, ShardClientError> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ShardClientError::Internal("system clock before Unix epoch".into()))?
        .as_millis();
    u64::try_from(millis).map_err(|_| ShardClientError::Internal("system clock overflow".into()))
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RemoteTopologyError {
    InvalidReplica,
    InvalidTopology,
    GraphChanged,
    LockPoisoned,
}

impl Display for RemoteTopologyError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidReplica => formatter.write_str("invalid remote Replica endpoint"),
            Self::InvalidTopology => formatter.write_str("invalid remote Shard topology"),
            Self::GraphChanged => formatter.write_str("topology update changed graph identity"),
            Self::LockPoisoned => formatter.write_str("remote topology lock poisoned"),
        }
    }
}

impl Error for RemoteTopologyError {}
