use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use dtg_execution::cluster_protocol::proto::{
    BoundedPayload, RaftEnvelope, RaftMessageKind, RequestContext, ShardContext, StatusCode,
    data_service_client::DataServiceClient,
};
use dtg_execution::cluster_protocol::{PROTOCOL_MAJOR, checksum_bytes, validate_typed_status};
use dtg_execution::storage::{
    BackendGeneration, ClusterId, GraphId, PlacementEpoch, ReplicaBinding, ReplicaId, ShardId,
    Version,
};
use prost_011::Message as _;
use raft::eraftpb::{Message, MessageType};

use crate::service::RaftTransportFuture;
use crate::{DataNodeError, RaftTransport};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct RouteKey {
    cluster_id: ClusterId,
    graph_id: GraphId,
    shard_id: ShardId,
    placement_epoch: PlacementEpoch,
    backend_generation: BackendGeneration,
    from_replica_id: ReplicaId,
    to_replica_id: ReplicaId,
}

impl RouteKey {
    fn new(binding: &ReplicaBinding, to_replica_id: ReplicaId) -> Self {
        Self {
            cluster_id: binding.cluster_id(),
            graph_id: binding.graph_id(),
            shard_id: binding.shard_id(),
            placement_epoch: binding.placement_epoch(),
            backend_generation: binding.backend_generation(),
            from_replica_id: binding.replica_id(),
            to_replica_id,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Route {
    catalog_version: Version,
    endpoint: String,
}

#[derive(Clone, Default)]
pub struct TonicRaftTransport {
    routes: Arc<RwLock<BTreeMap<RouteKey, Route>>>,
    request_sequence: Arc<AtomicU64>,
}

impl TonicRaftTransport {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn install_route(
        &self,
        source: &ReplicaBinding,
        to_replica_id: ReplicaId,
        catalog_version: Version,
        endpoint: impl Into<String>,
    ) -> Result<bool, DataNodeError> {
        if catalog_version.get() == 0 {
            return Err(DataNodeError::Build(
                "Raft route catalog version must be nonzero".into(),
            ));
        }
        let endpoint = endpoint.into();
        if endpoint.trim().is_empty() {
            return Err(DataNodeError::Build(
                "Raft route endpoint must be nonempty".into(),
            ));
        }
        let key = RouteKey::new(source, to_replica_id);
        let route = Route {
            catalog_version,
            endpoint,
        };
        let mut routes = self
            .routes
            .write()
            .map_err(|_| DataNodeError::Build("Raft route registry lock is poisoned".into()))?;
        if let Some(current) = routes.get(&key) {
            if route.catalog_version < current.catalog_version {
                return Err(DataNodeError::Build(
                    "Raft route catalog version regressed".into(),
                ));
            }
            if route.catalog_version == current.catalog_version {
                if current == &route {
                    return Ok(false);
                }
                return Err(DataNodeError::Build(
                    "equal Raft route revisions disagree".into(),
                ));
            }
        }
        routes.insert(key, route);
        Ok(true)
    }

    fn route(&self, binding: &ReplicaBinding, message: &Message) -> Result<Route, DataNodeError> {
        if message.from != binding.replica_id().get() {
            return Err(DataNodeError::Build(
                "outbound Raft message source does not match the local binding".into(),
            ));
        }
        let target =
            ReplicaId::new(message.to).map_err(|error| DataNodeError::Build(error.to_string()))?;
        self.routes
            .read()
            .map_err(|_| DataNodeError::Build("Raft route registry lock is poisoned".into()))?
            .get(&RouteKey::new(binding, target))
            .cloned()
            .ok_or_else(|| {
                DataNodeError::Build("no exact Raft route for the fenced replica pair".into())
            })
    }
}

impl RaftTransport for TonicRaftTransport {
    fn send<'a>(
        &'a self,
        binding: &'a ReplicaBinding,
        message: Message,
    ) -> RaftTransportFuture<'a> {
        Box::pin(async move {
            let route = self.route(binding, &message)?;
            let body = message.encode_to_vec();
            let request_id = next_request_id(&self.request_sequence);
            let deadline_unix_ms = unix_time_millis()
                .checked_add(5_000)
                .ok_or_else(|| DataNodeError::Build("Raft request deadline overflow".into()))?;
            let shard_id = u32::try_from(binding.shard_id().get())
                .map_err(|_| DataNodeError::Build("Shard identity exceeds protocol u32".into()))?;
            let envelope = RaftEnvelope {
                context: Some(ShardContext {
                    request: Some(RequestContext {
                        protocol_major: PROTOCOL_MAJOR,
                        protocol_minor: 0,
                        cluster_id: binding.cluster_id().get().to_be_bytes().to_vec(),
                        request_id: request_id.to_be_bytes().to_vec(),
                        deadline_unix_ms,
                        trace_context: Vec::new(),
                    }),
                    graph_id: binding.graph_id().get(),
                    shard_id,
                    placement_epoch: binding.placement_epoch().get(),
                    backend_generation: binding.backend_generation().get(),
                    catalog_version: route.catalog_version.get(),
                }),
                from_replica_id: message.from,
                to_replica_id: message.to,
                term: message.term,
                committed_index: message.commit,
                kind: raft_message_kind(message.get_msg_type())?.into(),
                payload: Some(BoundedPayload {
                    format_version: 1,
                    declared_len: body.len() as u64,
                    item_count: 1,
                    checksum: checksum_bytes(&body).to_vec(),
                    body,
                }),
            };
            let mut client = DataServiceClient::connect(route.endpoint)
                .await
                .map_err(|error| DataNodeError::Build(error.to_string()))?;
            let status = client
                .send_raft(envelope)
                .await
                .map_err(|error| DataNodeError::Build(error.to_string()))?
                .into_inner();
            validate_typed_status(status.clone())
                .map_err(|error| DataNodeError::Build(error.to_string()))?;
            if status.code != i32::from(StatusCode::Ok) {
                return Err(DataNodeError::Build(format!(
                    "remote Data process rejected Raft message: {}",
                    status.message
                )));
            }
            Ok(())
        })
    }
}

pub(crate) fn raft_message_kind(
    message_type: MessageType,
) -> Result<RaftMessageKind, DataNodeError> {
    match message_type {
        MessageType::MsgAppend | MessageType::MsgAppendResponse => Ok(RaftMessageKind::Append),
        MessageType::MsgRequestVote
        | MessageType::MsgRequestVoteResponse
        | MessageType::MsgRequestPreVote
        | MessageType::MsgRequestPreVoteResponse => Ok(RaftMessageKind::Vote),
        MessageType::MsgSnapshot => Ok(RaftMessageKind::Snapshot),
        MessageType::MsgHeartbeat | MessageType::MsgHeartbeatResponse => {
            Ok(RaftMessageKind::Heartbeat)
        }
        MessageType::MsgSnapStatus
        | MessageType::MsgTransferLeader
        | MessageType::MsgTimeoutNow
        | MessageType::MsgReadIndex
        | MessageType::MsgReadIndexResp => Ok(RaftMessageKind::Control),
        _ => Err(DataNodeError::Build(format!(
            "Raft protocol v2 cannot encode outbound message type {message_type:?}"
        ))),
    }
}

fn next_request_id(sequence: &AtomicU64) -> u128 {
    let next = sequence.fetch_add(1, Ordering::Relaxed).saturating_add(1);
    (u128::from(unix_time_millis()) << 64) | u128::from(next)
}

fn unix_time_millis() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(u64::MAX)
}
