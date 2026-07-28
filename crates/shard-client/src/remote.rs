use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cluster_protocol::CLUSTER_PROTOCOL_VERSION;
use cluster_protocol::proto::shard_service_client::ShardServiceClient;
use cluster_protocol::proto::{
    AdvanceAnalyticsArtifactFenceRequest, AnalyticsArtifactGeneration,
    AnalyticsArtifactGenerationCursor as WireArtifactGenerationCursor,
    AnalyticsArtifactKind as WireArtifactKind, DeleteAnalyticsArtifactGenerationRequest,
    ExecuteRequest as WireExecuteRequest, GetAnalyticsArtifactGenerationRequest,
    ListAnalyticsArtifactGenerationHeadsRequest, ListAnalyticsArtifactGenerationHeadsResponse,
    ListAnalyticsArtifactGenerationsRequest, ListAnalyticsArtifactGenerationsResponse,
    PinAnalyticsArtifactGenerationRequest, PutAnalyticsArtifactChunkRequest,
    QueryPushdownGuarantee as WirePushdownGuarantee, ReadBarrierRequest,
    ReadRequest as WireReadRequest, ReplicaStatusRequest, RequestContext,
    ScanRequest as WireScanRequest, ShardContext,
};
use data_node::{
    ReadCodecError, decode_candidate_scan_batch, decode_key_read_result,
    decode_key_scan_batch_bounded, encode_candidate_scan_plan, encode_key_read_plan,
    encode_key_scan_plan,
};
use storage_api::{
    CandidateScanPage, KeyValue, PushdownGuarantee, QueryCapabilitySnapshot,
    QueryPrimitiveCapabilities,
};
use temporal_types::TransactionTime;
use tokio_stream::StreamExt;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};
use tonic::{Code, Request, Status};

use crate::{
    AdvanceArtifactFenceRequest, ArtifactChunkStream, ArtifactGenerationCursor,
    ArtifactGenerationHeadPage, ArtifactGenerationSummary, ArtifactKind, ArtifactStreamChunk,
    CandidateScanCommand, DeleteArtifactGenerationRequest, ExecuteCommand, ExecuteReceipt,
    GetArtifactGenerationRequest, ListArtifactGenerationHeadsRequest,
    ListArtifactGenerationsRequest, MAX_ARTIFACT_CHUNK_BYTES, MAX_ARTIFACT_CHUNKS,
    PinArtifactGenerationRequest, PutArtifactChunkRequest as ClientPutArtifactChunkRequest,
    ReadKeysRequest, ScanRequest, ShardClient, ShardClientError, ShardClientFuture,
    ShardRequestContext, ShardStatus, artifact_corruption, validated_artifact_stream,
};

const MAXIMUM_SCAN_BATCH_BYTES: u32 = 4 * 1024 * 1024;
const MAXIMUM_RETRY_ATTEMPTS: usize = 3;
const MAXIMUM_READ_RETRY_ATTEMPTS: usize = 64;

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
    transport: RemoteTransport,
}

#[derive(Clone, Debug)]
enum RemoteTransport {
    LoopbackPlaintext,
    MutualTls {
        domain: String,
        ca_pem: Vec<u8>,
        cert_pem: Vec<u8>,
        key_pem: Vec<u8>,
    },
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
            transport: RemoteTransport::LoopbackPlaintext,
        })
    }

    pub fn new_mtls(
        cluster_id: [u8; 16],
        topology: RemoteTopology,
        domain: impl Into<String>,
        ca_pem: Vec<u8>,
        cert_pem: Vec<u8>,
        key_pem: Vec<u8>,
    ) -> Result<Self, RemoteTopologyError> {
        let domain = domain.into();
        if cluster_id == [0; 16]
            || domain.is_empty()
            || ca_pem.is_empty()
            || cert_pem.is_empty()
            || key_pem.is_empty()
        {
            return Err(RemoteTopologyError::InvalidTopology);
        }
        Ok(Self {
            cluster_id,
            topology: RwLock::new(Arc::new(topology)),
            channels: Mutex::new(BTreeMap::new()),
            transport: RemoteTransport::MutualTls {
                domain,
                ca_pem,
                cert_pem,
                key_pem,
            },
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
        let scheme = match &self.transport {
            RemoteTransport::LoopbackPlaintext => "http",
            RemoteTransport::MutualTls { .. } => "https",
        };
        let mut endpoint = Endpoint::from_shared(format!("{scheme}://{}", replica.address))
            .map_err(|error| ShardClientError::Internal(error.to_string()))?
            .connect_timeout(Duration::from_secs(2))
            .tcp_nodelay(true);
        if let RemoteTransport::MutualTls {
            ref domain,
            ref ca_pem,
            ref cert_pem,
            ref key_pem,
        } = self.transport
        {
            let tls = ClientTlsConfig::new()
                .domain_name(domain)
                .ca_certificate(Certificate::from_pem(ca_pem))
                .identity(Identity::from_pem(cert_pem, key_pem));
            endpoint = endpoint
                .tls_config(tls)
                .map_err(|error| ShardClientError::Internal(error.to_string()))?;
        }
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
            let candidates = Self::candidates(&route);
            if candidates.is_empty() {
                return Err(ShardClientError::NoLeader {
                    shard_id: context.shard_id(),
                });
            }
            for replica in candidates.into_iter().cycle() {
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
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                }
            }
            unreachable!("cycled remote Shard candidates are non-empty")
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
            let mut last_error = None;
            let candidates = Self::candidates(&route);
            for attempt in 0..MAXIMUM_READ_RETRY_ATTEMPTS {
                validate_deadline(context)?;
                let replica = candidates.get(attempt % candidates.len()).copied().ok_or(
                    ShardClientError::NoLeader {
                        shard_id: context.shard_id(),
                    },
                )?;
                let mut client = ShardServiceClient::new(self.channel(replica)?);
                let wire = WireReadRequest {
                    context: Some(self.wire_context(context)),
                    plan: plan.clone(),
                    read_proof: Vec::new(),
                };
                match client.read(Self::request(context, wire)?).await {
                    Ok(response) => {
                        return decode_key_read_result(&response.into_inner().result)
                            .map_err(|error| ShardClientError::Internal(error.to_string()));
                    }
                    Err(status) => {
                        let error = map_status(&status);
                        if !retryable(&error) {
                            return Err(error);
                        }
                        last_error = Some(error);
                        if attempt + 1 < MAXIMUM_READ_RETRY_ATTEMPTS {
                            tokio::time::sleep(Duration::from_millis(10)).await;
                        }
                    }
                }
            }
            Err(last_error.unwrap_or(ShardClientError::NoLeader {
                shard_id: context.shard_id(),
            }))
        })
    }

    fn scan<'a>(&'a self, request: ScanRequest) -> ShardClientFuture<'a, Vec<KeyValue>> {
        Box::pin(async move { Ok(self.scan_fenced(request).await?.into_entries()) })
    }

    fn scan_fenced<'a>(&'a self, request: ScanRequest) -> ShardClientFuture<'a, crate::FencedScan> {
        Box::pin(async move {
            let context = request.context();
            let (_, route) = self.route(context)?;
            let plan = encode_key_scan_plan(request.span())
                .map_err(|error| ShardClientError::Internal(error.to_string()))?;
            let mut last_error = None;
            let candidates = Self::candidates(&route);
            'replicas: for attempt in 0..MAXIMUM_READ_RETRY_ATTEMPTS {
                validate_deadline(context)?;
                let replica = candidates.get(attempt % candidates.len()).copied().ok_or(
                    ShardClientError::NoLeader {
                        shard_id: context.shard_id(),
                    },
                )?;
                let mut client = ShardServiceClient::new(self.channel(replica)?);
                let wire = WireScanRequest {
                    context: Some(self.wire_context(context)),
                    plan: plan.clone(),
                    read_proof: Vec::new(),
                    maximum_batch_bytes: MAXIMUM_SCAN_BATCH_BYTES,
                };
                let mut stream = match client.scan(Self::request(context, wire)?).await {
                    Ok(response) => response.into_inner(),
                    Err(status) => {
                        let error = map_status(&status);
                        if !retryable(&error) {
                            return Err(error);
                        }
                        last_error = Some(error);
                        if attempt + 1 < MAXIMUM_READ_RETRY_ATTEMPTS {
                            tokio::time::sleep(Duration::from_millis(10)).await;
                        }
                        continue;
                    }
                };
                let mut rows = Vec::new();
                let mut expected_sequence = 0_u64;
                let mut applied_index = None;
                let mut terminal = false;
                let mut retained = 0_u64;
                while let Some(batch) = stream.next().await {
                    validate_deadline(context)?;
                    let batch = match batch {
                        Ok(batch) => batch,
                        Err(status) => {
                            let error = map_status(&status);
                            if rows.is_empty() && retryable(&error) {
                                last_error = Some(error);
                                if attempt + 1 < MAXIMUM_READ_RETRY_ATTEMPTS {
                                    tokio::time::sleep(Duration::from_millis(10)).await;
                                }
                                continue 'replicas;
                            }
                            return Err(error);
                        }
                    };
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
                    let limit = request.span().max_bytes().unwrap_or(u64::MAX);
                    let remaining =
                        limit
                            .checked_sub(retained)
                            .ok_or(ShardClientError::ScanByteLimit {
                                limit,
                                required: retained,
                            })?;
                    let (decoded, decoded_bytes) =
                        match decode_key_scan_batch_bounded(&batch.arrow_record_batch, remaining) {
                            Ok(decoded) => decoded,
                            Err(ReadCodecError::ScanByteLimit { required, .. }) => {
                                return Err(ShardClientError::ScanByteLimit {
                                    limit,
                                    required: retained.saturating_add(required),
                                });
                            }
                            Err(error) => {
                                return Err(ShardClientError::Internal(error.to_string()));
                            }
                        };
                    rows.extend(decoded);
                    retained = retained.checked_add(decoded_bytes).ok_or_else(|| {
                        ShardClientError::Adapter("remote scan byte limit exceeded".into())
                    })?;
                }
                if !terminal {
                    return Err(ShardClientError::Internal(
                        "remote scan ended without a terminal batch".into(),
                    ));
                }
                let applied_index = applied_index.ok_or_else(|| {
                    ShardClientError::Internal("remote scan omitted its applied index".into())
                })?;
                return Ok(crate::FencedScan::new(applied_index, rows));
            }
            Err(last_error.unwrap_or(ShardClientError::NoLeader {
                shard_id: context.shard_id(),
            }))
        })
    }

    fn scan_candidates<'a>(
        &'a self,
        command: CandidateScanCommand,
    ) -> ShardClientFuture<'a, CandidateScanPage> {
        Box::pin(async move {
            let context = command.context();
            let request = command.request().clone();
            let (_, route) = self.route(context)?;
            let plan = encode_candidate_scan_plan(&request)
                .map_err(|error| ShardClientError::Internal(error.to_string()))?;
            let candidates = Self::candidates(&route);
            let mut last_error = None;
            'replicas: for attempt in 0..MAXIMUM_READ_RETRY_ATTEMPTS {
                validate_deadline(context)?;
                let replica = candidates.get(attempt % candidates.len()).copied().ok_or(
                    ShardClientError::NoLeader {
                        shard_id: context.shard_id(),
                    },
                )?;
                let mut client = ShardServiceClient::new(self.channel(replica)?);
                let wire = WireScanRequest {
                    context: Some(self.wire_context(context)),
                    plan: plan.clone(),
                    read_proof: Vec::new(),
                    maximum_batch_bytes: MAXIMUM_SCAN_BATCH_BYTES,
                };
                let mut stream = match client.scan(Self::request(context, wire)?).await {
                    Ok(response) => response.into_inner(),
                    Err(status) if status.code() == Code::Unimplemented => {
                        return Err(ShardClientError::UnsupportedQueryPrimitive(
                            "candidate scan",
                        ));
                    }
                    Err(status) => {
                        let error = map_status(&status);
                        if !retryable(&error) {
                            return Err(error);
                        }
                        last_error = Some(error);
                        continue;
                    }
                };
                let mut entries = Vec::new();
                let mut expected_sequence = 0_u64;
                let mut applied_index = None;
                let mut wire_guarantee = None;
                let mut next_start = None;
                let mut terminal = false;
                while let Some(batch) = stream.next().await {
                    validate_deadline(context)?;
                    let batch = match batch {
                        Ok(batch) => batch,
                        Err(status) if status.code() == Code::Unimplemented => {
                            return Err(ShardClientError::UnsupportedQueryPrimitive(
                                "candidate scan",
                            ));
                        }
                        Err(status) => {
                            let error = map_status(&status);
                            if entries.is_empty() && retryable(&error) {
                                last_error = Some(error);
                                continue 'replicas;
                            }
                            return Err(error);
                        }
                    };
                    if batch.sequence != expected_sequence
                        || applied_index.is_some_and(|index| index != batch.applied_index)
                        || terminal
                    {
                        return Err(ShardClientError::Internal(
                            "invalid remote candidate scan stream ordering".into(),
                        ));
                    }
                    let (guarantee, decoded, continuation) =
                        decode_candidate_scan_batch(&batch.arrow_record_batch)
                            .map_err(|error| ShardClientError::Internal(error.to_string()))?;
                    if wire_guarantee.is_some_and(|value| value != guarantee)
                        || (!batch.terminal && continuation.is_some())
                    {
                        return Err(ShardClientError::Internal(
                            "inconsistent remote candidate scan batch".into(),
                        ));
                    }
                    wire_guarantee = Some(guarantee);
                    applied_index = Some(batch.applied_index);
                    expected_sequence = expected_sequence.saturating_add(1);
                    terminal = batch.terminal;
                    if terminal {
                        next_start = continuation;
                    }
                    entries.extend(decoded);
                }
                if !terminal {
                    return Err(ShardClientError::Internal(
                        "remote candidate scan ended without a terminal batch".into(),
                    ));
                }
                let applied_index = applied_index.ok_or_else(|| {
                    ShardClientError::Internal(
                        "remote candidate scan omitted its applied index".into(),
                    )
                })?;
                wire_guarantee.ok_or_else(|| {
                    ShardClientError::Internal("remote candidate scan omitted its guarantee".into())
                })?;
                return CandidateScanPage::new(
                    &request,
                    applied_index,
                    PushdownGuarantee::Candidate,
                    entries,
                    next_start,
                )
                .map_err(|error| ShardClientError::Internal(error.to_string()));
            }
            Err(last_error.unwrap_or(ShardClientError::NoLeader {
                shard_id: context.shard_id(),
            }))
        })
    }

    fn read_barrier<'a>(&'a self, context: ShardRequestContext) -> ShardClientFuture<'a, u64> {
        Box::pin(async move {
            let (_, route) = self.route(context)?;
            let candidates = Self::candidates(&route);
            let mut last_error = None;
            for attempt in 0..MAXIMUM_READ_RETRY_ATTEMPTS {
                validate_deadline(context)?;
                let replica = candidates.get(attempt % candidates.len()).copied().ok_or(
                    ShardClientError::NoLeader {
                        shard_id: context.shard_id(),
                    },
                )?;
                let mut client = ShardServiceClient::new(self.channel(replica)?);
                let wire = ReadBarrierRequest {
                    context: Some(self.wire_context(context)),
                };
                match client.read_barrier(Self::request(context, wire)?).await {
                    Ok(response) if response.get_ref().read_index != 0 => {
                        return Ok(response.into_inner().read_index);
                    }
                    Ok(_) => {
                        return Err(ShardClientError::ReadBarrier(
                            "remote ReadIndex barrier returned zero".into(),
                        ));
                    }
                    Err(status) => {
                        let error = map_status(&status);
                        if !retryable(&error) {
                            return Err(error);
                        }
                        last_error = Some(error);
                        if attempt + 1 < MAXIMUM_READ_RETRY_ATTEMPTS {
                            tokio::time::sleep(Duration::from_millis(10)).await;
                        }
                    }
                }
            }
            Err(last_error.unwrap_or(ShardClientError::NoLeader {
                shard_id: context.shard_id(),
            }))
        })
    }

    fn put_artifact_chunk<'a>(
        &'a self,
        request: ClientPutArtifactChunkRequest,
    ) -> ShardClientFuture<'a, ExecuteReceipt> {
        Box::pin(async move {
            let context = request.context();
            let (_, route) = self.route(context)?;
            let payload = PutAnalyticsArtifactChunkRequest {
                context: Some(self.wire_context(context)),
                job_id: request.job_id().to_be_bytes().to_vec(),
                kind: wire_artifact_kind(request.kind()).into(),
                generation: request.generation(),
                created_at_unix_ms: request.created_at_unix_ms(),
                ordinal: request.ordinal(),
                previous_digest: request.previous_digest().to_vec(),
                payload: request.payload().to_vec(),
            };
            let mut last_error = None;
            for replica in Self::candidates(&route) {
                validate_deadline(context)?;
                let mut client = ShardServiceClient::new(self.channel(replica)?);
                match client
                    .put_analytics_artifact_chunk(Self::request(context, payload.clone())?)
                    .await
                {
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

    fn pin_artifact_generation<'a>(
        &'a self,
        request: PinArtifactGenerationRequest,
    ) -> ShardClientFuture<'a, ExecuteReceipt> {
        Box::pin(async move {
            let context = request.context();
            let (_, route) = self.route(context)?;
            let payload = PinAnalyticsArtifactGenerationRequest {
                context: Some(self.wire_context(context)),
                job_id: request.job_id().to_be_bytes().to_vec(),
                kind: wire_artifact_kind(request.kind()).into(),
                generation: request.generation(),
                expected_chunk_count: u32::from(request.expected_chunk_count()),
                expected_total_bytes: request.expected_total_bytes(),
                expected_content_digest: request.expected_content_digest().to_vec(),
            };
            let mut last_error = None;
            for replica in Self::candidates(&route) {
                validate_deadline(context)?;
                let mut client = ShardServiceClient::new(self.channel(replica)?);
                match client
                    .pin_analytics_artifact_generation(Self::request(context, payload.clone())?)
                    .await
                {
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

    fn get_artifact_generation<'a>(
        &'a self,
        request: GetArtifactGenerationRequest,
    ) -> ShardClientFuture<'a, ArtifactChunkStream> {
        Box::pin(async move {
            let context = request.context();
            let (_, route) = self.route(context)?;
            let payload = GetAnalyticsArtifactGenerationRequest {
                context: Some(self.wire_context(context)),
                job_id: request.job_id().to_be_bytes().to_vec(),
                kind: wire_artifact_kind(request.kind()).into(),
                generation: request.generation(),
                expected_chunk_count: u32::from(request.expected_chunk_count()),
                expected_total_bytes: request.expected_total_bytes(),
                expected_content_digest: request.expected_content_digest().to_vec(),
            };
            let mut last_error = None;
            let mut stream = None;
            for attempt in 0..MAXIMUM_RETRY_ATTEMPTS {
                validate_deadline(context)?;
                let replica = Self::candidates(&route)
                    .into_iter()
                    .nth(attempt % route.replicas.len())
                    .ok_or(ShardClientError::NoLeader {
                        shard_id: context.shard_id(),
                    })?;
                let mut client = ShardServiceClient::new(self.channel(replica)?);
                match client
                    .get_analytics_artifact_generation(Self::request(context, payload.clone())?)
                    .await
                {
                    Ok(response) => {
                        stream = Some(response.into_inner());
                        break;
                    }
                    Err(status) => {
                        let error = artifact_stream_status(&status);
                        if !retryable(&error) {
                            return Err(error);
                        }
                        last_error = Some(error);
                        if attempt + 1 < MAXIMUM_RETRY_ATTEMPTS {
                            tokio::time::sleep(Duration::from_millis(10)).await;
                        }
                    }
                }
            }
            let stream = stream.ok_or_else(|| {
                last_error.unwrap_or(ShardClientError::NoLeader {
                    shard_id: context.shard_id(),
                })
            })?;
            let input = stream.map(move |chunk| {
                validate_deadline(context)?;
                let chunk = chunk.map_err(|status| artifact_stream_status(&status))?;
                let previous_digest =
                    chunk.previous_digest.as_slice().try_into().map_err(|_| {
                        artifact_corruption("artifact response has an invalid previous digest")
                    })?;
                let payload_digest = chunk.payload_digest.as_slice().try_into().map_err(|_| {
                    artifact_corruption("artifact response has an invalid payload digest")
                })?;
                Ok(ArtifactStreamChunk {
                    applied_index: chunk.applied_index,
                    ordinal: chunk.ordinal,
                    previous_digest,
                    payload_digest,
                    payload: chunk.payload,
                })
            });
            Ok(validated_artifact_stream(Box::pin(input), request))
        })
    }

    fn delete_artifact_generation<'a>(
        &'a self,
        request: DeleteArtifactGenerationRequest,
    ) -> ShardClientFuture<'a, ExecuteReceipt> {
        Box::pin(async move {
            let context = request.context();
            let (_, route) = self.route(context)?;
            let payload = DeleteAnalyticsArtifactGenerationRequest {
                context: Some(self.wire_context(context)),
                job_id: request.job_id().to_be_bytes().to_vec(),
                kind: wire_artifact_kind(request.kind()).into(),
                generation: request.generation(),
                gc_epoch: request.gc_epoch(),
            };
            let mut last_error = None;
            for replica in Self::candidates(&route) {
                validate_deadline(context)?;
                let mut client = ShardServiceClient::new(self.channel(replica)?);
                match client
                    .delete_analytics_artifact_generation(Self::request(context, payload.clone())?)
                    .await
                {
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

    fn advance_artifact_fence<'a>(
        &'a self,
        request: AdvanceArtifactFenceRequest,
    ) -> ShardClientFuture<'a, ExecuteReceipt> {
        Box::pin(async move {
            let context = request.context();
            let (_, route) = self.route(context)?;
            let payload = AdvanceAnalyticsArtifactFenceRequest {
                context: Some(self.wire_context(context)),
                job_id: request.job_id().to_be_bytes().to_vec(),
                kind: wire_artifact_kind(request.kind()).into(),
                generation: request.generation(),
                gc_epoch: request.gc_epoch(),
            };
            let mut last_error = None;
            for replica in Self::candidates(&route) {
                validate_deadline(context)?;
                let mut client = ShardServiceClient::new(self.channel(replica)?);
                match client
                    .advance_analytics_artifact_fence(Self::request(context, payload.clone())?)
                    .await
                {
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

    fn list_artifact_generations<'a>(
        &'a self,
        request: ListArtifactGenerationsRequest,
    ) -> ShardClientFuture<'a, Vec<ArtifactGenerationSummary>> {
        Box::pin(async move {
            let context = request.context();
            let (_, route) = self.route(context)?;
            let payload = ListAnalyticsArtifactGenerationsRequest {
                context: Some(self.wire_context(context)),
                job_id: request.job_id().to_be_bytes().to_vec(),
                kind: wire_artifact_kind(request.kind()).into(),
                limit: request.limit(),
            };
            let mut last_error = None;
            for replica in Self::candidates(&route) {
                validate_deadline(context)?;
                let mut client = ShardServiceClient::new(self.channel(replica)?);
                match client
                    .list_analytics_artifact_generations(Self::request(context, payload.clone())?)
                    .await
                {
                    Ok(response) => {
                        return decode_filtered_artifact_generations(
                            request,
                            response.into_inner(),
                        );
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

    fn list_artifact_generation_heads<'a>(
        &'a self,
        request: ListArtifactGenerationHeadsRequest,
    ) -> ShardClientFuture<'a, ArtifactGenerationHeadPage> {
        Box::pin(async move {
            let context = request.context();
            let (_, route) = self.route(context)?;
            let payload = ListAnalyticsArtifactGenerationHeadsRequest {
                context: Some(self.wire_context(context)),
                after: request.after().map(|after| WireArtifactGenerationCursor {
                    job_id: after.job_id().to_be_bytes().to_vec(),
                    kind: wire_artifact_kind(after.kind()).into(),
                    generation: after.generation(),
                }),
                limit: request.limit(),
            };
            let mut last_error = None;
            for replica in Self::candidates(&route) {
                validate_deadline(context)?;
                let mut client = ShardServiceClient::new(self.channel(replica)?);
                match client
                    .list_analytics_artifact_generation_heads(Self::request(
                        context,
                        payload.clone(),
                    )?)
                    .await
                {
                    Ok(response) => {
                        return decode_artifact_generation_head_page(
                            request,
                            response.into_inner(),
                        );
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
                decode_query_capabilities(&status)?,
            ))
        })
    }
}

fn decode_query_capabilities(
    status: &cluster_protocol::proto::ReplicaStatusResponse,
) -> Result<QueryCapabilitySnapshot, ShardClientError> {
    if status.query_capability_generation == 0 {
        return Err(ShardClientError::Internal(
            "remote replica reported a zero query capability generation".into(),
        ));
    }
    let decode = |wire: i32| {
        let wire = WirePushdownGuarantee::try_from(wire).map_err(|_| {
            ShardClientError::Internal("remote replica reported an unknown query guarantee".into())
        })?;
        match wire {
            WirePushdownGuarantee::Unspecified => Err(ShardClientError::Internal(
                "remote replica omitted a query guarantee".into(),
            )),
            WirePushdownGuarantee::Unsupported => Ok(PushdownGuarantee::Unsupported),
            WirePushdownGuarantee::Candidate => Ok(PushdownGuarantee::Candidate),
            WirePushdownGuarantee::Exact => Ok(PushdownGuarantee::Exact),
        }
    };
    Ok(QueryCapabilitySnapshot::new(
        status.query_capability_generation,
        QueryPrimitiveCapabilities::new(
            decode(status.candidate_scan)?,
            decode(status.property_gather)?,
            decode(status.adjacency_expand)?,
            decode(status.change_scan)?,
        ),
    ))
}

fn wire_artifact_kind(kind: ArtifactKind) -> WireArtifactKind {
    match kind {
        ArtifactKind::Checkpoint => WireArtifactKind::Checkpoint,
        ArtifactKind::Result => WireArtifactKind::Result,
    }
}

fn decode_artifact_generation_head_page(
    request: ListArtifactGenerationHeadsRequest,
    response: ListAnalyticsArtifactGenerationHeadsResponse,
) -> Result<ArtifactGenerationHeadPage, ShardClientError> {
    if response.applied_index == 0 {
        return Err(artifact_corruption(
            "artifact generation head response has a zero ReadIndex",
        ));
    }
    let limit = usize::try_from(request.limit()).expect("bounded artifact head page limit");
    if response.generations.len() > limit {
        return Err(artifact_corruption(
            "artifact generation head response exceeds the requested limit",
        ));
    }
    let mut previous = request.after();
    let mut generations = Vec::with_capacity(response.generations.len());
    for generation in response.generations {
        let summary = decode_artifact_generation_summary(generation, response.applied_index)?;
        let cursor = summary.cursor();
        if previous.is_some_and(|previous| cursor <= previous) {
            return Err(artifact_corruption(
                "artifact generation response is not in canonical cursor order",
            ));
        }
        generations.push(summary);
        previous = Some(cursor);
    }
    let next = response
        .next
        .map(|next| {
            let job_id = next
                .job_id
                .as_slice()
                .try_into()
                .map(u128::from_be_bytes)
                .map_err(|_| artifact_corruption("artifact page cursor has an invalid job ID"))?;
            ArtifactGenerationCursor::new(job_id, client_artifact_kind(next.kind)?, next.generation)
                .map_err(|_| artifact_corruption("artifact page cursor is invalid"))
        })
        .transpose()?;
    let expected_next = (generations.len() == limit)
        .then(|| generations.last().map(ArtifactGenerationSummary::cursor))
        .flatten();
    if next != expected_next {
        return Err(artifact_corruption(
            "artifact generation page cursor does not match the last record",
        ));
    }
    Ok(ArtifactGenerationHeadPage {
        applied_index: response.applied_index,
        generations,
        next,
    })
}

fn decode_filtered_artifact_generations(
    request: ListArtifactGenerationsRequest,
    response: ListAnalyticsArtifactGenerationsResponse,
) -> Result<Vec<ArtifactGenerationSummary>, ShardClientError> {
    if response.applied_index == 0 {
        return Err(artifact_corruption(
            "filtered artifact generation response has a zero ReadIndex",
        ));
    }
    let limit = usize::try_from(request.limit()).expect("bounded artifact generation limit");
    if response.generations.len() > limit {
        return Err(artifact_corruption(
            "filtered artifact generation response exceeds the requested limit",
        ));
    }
    let mut previous_generation = 0_u64;
    let mut summaries = Vec::with_capacity(response.generations.len());
    for generation in response.generations {
        let summary = decode_artifact_generation_summary(generation, response.applied_index)?;
        if summary.job_id() != request.job_id()
            || summary.kind() != request.kind()
            || summary.generation() <= previous_generation
        {
            return Err(artifact_corruption(
                "filtered artifact generation response violates its requested identity or order",
            ));
        }
        previous_generation = summary.generation();
        summaries.push(summary);
    }
    Ok(summaries)
}

fn decode_artifact_generation_summary(
    generation: AnalyticsArtifactGeneration,
    applied_index: u64,
) -> Result<ArtifactGenerationSummary, ShardClientError> {
    let job_id = generation
        .job_id
        .as_slice()
        .try_into()
        .map(u128::from_be_bytes)
        .map_err(|_| artifact_corruption("artifact generation response has an invalid job ID"))?;
    let kind = client_artifact_kind(generation.kind)?;
    ArtifactGenerationCursor::new(job_id, kind, generation.generation)
        .map_err(|_| artifact_corruption("artifact generation response has an invalid identity"))?;
    if generation.created_at_unix_ms == 0 || generation.applied_index != applied_index {
        return Err(artifact_corruption(
            "artifact generation response has invalid provenance fields",
        ));
    }
    let count = u16::try_from(generation.expected_chunk_count).map_err(|_| {
        artifact_corruption("artifact generation response has an invalid chunk count")
    })?;
    if count == 0 || count > MAX_ARTIFACT_CHUNKS {
        return Err(artifact_corruption(
            "artifact generation response has an invalid chunk count",
        ));
    }
    let maximum_total_bytes = u64::from(count)
        .checked_mul(
            u64::try_from(MAX_ARTIFACT_CHUNK_BYTES).expect("artifact chunk byte limit fits in u64"),
        )
        .ok_or_else(|| artifact_corruption("artifact generation byte limit overflow"))?;
    let digest = if generation.pinned {
        let digest: [u8; 32] = generation
            .expected_content_digest
            .as_slice()
            .try_into()
            .map_err(|_| artifact_corruption("pinned artifact generation has an invalid digest"))?;
        if digest == [0; 32]
            || generation.expected_total_bytes < u64::from(count)
            || generation.expected_total_bytes > maximum_total_bytes
        {
            return Err(artifact_corruption(
                "pinned artifact generation has an invalid manifest",
            ));
        }
        digest
    } else {
        if !generation.expected_content_digest.is_empty() || generation.expected_total_bytes != 0 {
            return Err(artifact_corruption(
                "unpinned artifact generation fabricates a manifest",
            ));
        }
        [0; 32]
    };
    Ok(ArtifactGenerationSummary {
        job_id,
        kind,
        generation: generation.generation,
        created_at_unix_ms: generation.created_at_unix_ms,
        expected_chunk_count: count,
        expected_total_bytes: generation.expected_total_bytes,
        expected_content_digest: digest,
        pinned: generation.pinned,
        applied_index: generation.applied_index,
    })
}

fn client_artifact_kind(value: i32) -> Result<ArtifactKind, ShardClientError> {
    match WireArtifactKind::try_from(value) {
        Ok(WireArtifactKind::Checkpoint) => Ok(ArtifactKind::Checkpoint),
        Ok(WireArtifactKind::Result) => Ok(ArtifactKind::Result),
        _ => Err(artifact_corruption(
            "artifact generation response has an invalid kind",
        )),
    }
}

fn artifact_stream_status(status: &Status) -> ShardClientError {
    match map_status(status) {
        ShardClientError::Internal(message)
            if matches!(status.code(), Code::DataLoss | Code::FailedPrecondition) =>
        {
            ShardClientError::ArtifactCorruption(message)
        }
        error => error,
    }
}

fn retryable(error: &ShardClientError) -> bool {
    match error {
        ShardClientError::NotLeader { .. } | ShardClientError::Replication(_) => true,
        ShardClientError::Internal(message) => message.contains("Service was not ready"),
        _ => false,
    }
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
    if reason == Some("scan_byte_limit") {
        return match (
            metadata_u64(status, "dtgproxy-scan-limit"),
            metadata_u64(status, "dtgproxy-scan-required"),
        ) {
            (Some(limit), Some(required)) => ShardClientError::ScanByteLimit { limit, required },
            _ => ShardClientError::Internal(
                "remote scan byte limit status is missing required metadata".into(),
            ),
        };
    }
    if status.code() == Code::Unknown && status.message() == "transport error" {
        return ShardClientError::Replication(status.to_string());
    }
    if is_transport_status(status.code(), status.source().is_some()) {
        return ShardClientError::Replication(status.to_string());
    }
    match status.code() {
        Code::DeadlineExceeded | Code::Cancelled => ShardClientError::DeadlineExpired,
        Code::Aborted | Code::Unavailable | Code::ResourceExhausted => {
            ShardClientError::Replication(status.to_string())
        }
        _ => ShardClientError::Internal(status.to_string()),
    }
}

fn is_transport_status(code: Code, has_local_source: bool) -> bool {
    code == Code::Internal && has_local_source
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

#[cfg(test)]
mod tests {
    use cluster_protocol::proto::{
        AnalyticsArtifactGeneration, AnalyticsArtifactGenerationCursor,
        AnalyticsArtifactKind as WireArtifactKind, ListAnalyticsArtifactGenerationHeadsResponse,
        ListAnalyticsArtifactGenerationsResponse,
    };

    use super::{decode_artifact_generation_head_page, decode_filtered_artifact_generations, *};

    fn context() -> ShardRequestContext {
        ShardRequestContext::new(1, 1, 1, 1, u64::MAX).unwrap()
    }

    fn request(
        after: Option<ArtifactGenerationCursor>,
        limit: u32,
    ) -> ListArtifactGenerationHeadsRequest {
        ListArtifactGenerationHeadsRequest::new(context(), after, limit).unwrap()
    }

    fn filtered_request(limit: u32) -> ListArtifactGenerationsRequest {
        ListArtifactGenerationsRequest::new(context(), 9, ArtifactKind::Result, limit).unwrap()
    }

    fn generation(
        job_id: u128,
        kind: WireArtifactKind,
        generation: u64,
    ) -> AnalyticsArtifactGeneration {
        AnalyticsArtifactGeneration {
            job_id: job_id.to_be_bytes().to_vec(),
            kind: kind.into(),
            generation,
            expected_chunk_count: 1,
            expected_total_bytes: 0,
            expected_content_digest: Vec::new(),
            pinned: false,
            applied_index: 7,
            created_at_unix_ms: 9,
        }
    }

    fn cursor(
        job_id: u128,
        kind: WireArtifactKind,
        generation: u64,
    ) -> AnalyticsArtifactGenerationCursor {
        AnalyticsArtifactGenerationCursor {
            job_id: job_id.to_be_bytes().to_vec(),
            kind: kind.into(),
            generation,
        }
    }

    #[test]
    fn remote_artifact_head_page_validator_rejects_order_cursor_and_pin_corruption() {
        let valid = ListAnalyticsArtifactGenerationHeadsResponse {
            applied_index: 7,
            generations: vec![generation(9, WireArtifactKind::Result, 2)],
            next: Some(cursor(9, WireArtifactKind::Result, 2)),
        };
        assert!(decode_artifact_generation_head_page(request(None, 1), valid.clone()).is_ok());

        let after = ArtifactGenerationCursor::new(9, ArtifactKind::Result, 2).unwrap();
        assert!(
            decode_artifact_generation_head_page(request(Some(after), 1), valid.clone()).is_err()
        );

        let mut wrong_next = valid.clone();
        wrong_next.next = Some(cursor(10, WireArtifactKind::Checkpoint, 1));
        assert!(decode_artifact_generation_head_page(request(None, 1), wrong_next).is_err());

        let mut pinned_without_manifest = valid.clone();
        pinned_without_manifest.generations[0].pinned = true;
        assert!(
            decode_artifact_generation_head_page(request(None, 1), pinned_without_manifest)
                .is_err()
        );

        let mut unpinned_with_digest = valid;
        unpinned_with_digest.generations[0].expected_content_digest = vec![7; 32];
        assert!(
            decode_artifact_generation_head_page(request(None, 1), unpinned_with_digest).is_err()
        );
    }

    #[test]
    fn remote_filtered_artifact_validator_rejects_malicious_response_fields() {
        let valid = ListAnalyticsArtifactGenerationsResponse {
            applied_index: 7,
            generations: vec![generation(9, WireArtifactKind::Result, 2)],
        };
        assert!(decode_filtered_artifact_generations(filtered_request(1), valid.clone()).is_ok());

        let mut zero_outer = valid.clone();
        zero_outer.applied_index = 0;
        assert!(decode_filtered_artifact_generations(filtered_request(1), zero_outer).is_err());

        let mut over_limit = valid.clone();
        over_limit
            .generations
            .push(generation(9, WireArtifactKind::Result, 3));
        assert!(decode_filtered_artifact_generations(filtered_request(1), over_limit).is_err());

        for corrupt in [
            {
                let mut value = valid.clone();
                value.generations[0].job_id = 10_u128.to_be_bytes().to_vec();
                value
            },
            {
                let mut value = valid.clone();
                value.generations[0].kind = WireArtifactKind::Checkpoint.into();
                value
            },
            {
                let mut value = valid.clone();
                value.generations[0].generation = 0;
                value
            },
            {
                let mut value = valid.clone();
                value.generations[0].applied_index = 8;
                value
            },
            {
                let mut value = valid.clone();
                value.generations[0].created_at_unix_ms = 0;
                value
            },
            {
                let mut value = valid.clone();
                value.generations[0].expected_chunk_count = 0;
                value
            },
            {
                let mut value = valid.clone();
                value.generations[0].expected_chunk_count = u32::from(MAX_ARTIFACT_CHUNKS) + 1;
                value
            },
            {
                let mut value = valid.clone();
                value.generations[0].pinned = true;
                value
            },
            {
                let mut value = valid.clone();
                value.generations[0].pinned = true;
                value.generations[0].expected_total_bytes = 1;
                value.generations[0].expected_content_digest = vec![0; 32];
                value
            },
            {
                let mut value = valid.clone();
                value.generations[0].pinned = true;
                value.generations[0].expected_total_bytes =
                    u64::try_from(MAX_ARTIFACT_CHUNK_BYTES).unwrap() + 1;
                value.generations[0].expected_content_digest = vec![7; 32];
                value
            },
            {
                let mut value = valid.clone();
                value.generations[0].expected_content_digest = vec![7; 32];
                value
            },
            {
                let mut value = valid.clone();
                value.generations[0].expected_total_bytes = 1;
                value
            },
        ] {
            assert!(decode_filtered_artifact_generations(filtered_request(1), corrupt).is_err());
        }

        let duplicate = ListAnalyticsArtifactGenerationsResponse {
            applied_index: 7,
            generations: vec![
                generation(9, WireArtifactKind::Result, 2),
                generation(9, WireArtifactKind::Result, 2),
            ],
        };
        assert!(decode_filtered_artifact_generations(filtered_request(2), duplicate).is_err());

        let mut pinned = valid;
        pinned.generations[0].pinned = true;
        pinned.generations[0].expected_total_bytes = 1;
        pinned.generations[0].expected_content_digest = vec![7; 32];
        assert!(decode_filtered_artifact_generations(filtered_request(1), pinned).is_ok());

        assert!(retryable(&map_status(&Status::aborted(
            "artifact snapshot changed"
        ))));
        assert!(matches!(
            map_status(&Status::unknown("transport error")),
            ShardClientError::Replication(_)
        ));
        assert!(is_transport_status(Code::Internal, true));
        assert!(!is_transport_status(Code::Internal, false));
        assert!(matches!(
            map_status(&Status::internal("server-side internal error")),
            ShardClientError::Internal(_)
        ));
    }
}
