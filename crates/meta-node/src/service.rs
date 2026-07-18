use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cluster_protocol::proto::meta_service_server::MetaService;
use cluster_protocol::proto::{
    AllocateTimestampRequest, AllocateTimestampResponse, CatalogEvent as WireCatalogEvent,
    CatalogSnapshot, GetCatalogRequest, GetCatalogResponse, HeartbeatRequest, HeartbeatResponse,
    ProposeRequest, ProposeResponse, WatchCatalogRequest,
};
use cluster_protocol::{CommandPayload, CommonRequestContext, ProtocolError};
use control_plane::CatalogCommand;
use temporal_types::TransactionTime;
use tokio::sync::{Mutex, Notify, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::metadata::MetadataValue;
use tonic::{Request, Response, Status};

use crate::{MetaRaftError, MetaRaftReplica, MetaStateError, ReplicatedTso, TsoError};

const WATCH_CHANNEL_CAPACITY: usize = 64;
const WATCH_POLL_INTERVAL: Duration = Duration::from_millis(25);
const HEARTBEAT_LEASE_MS: u64 = 10_000;

#[derive(Clone)]
pub struct MetaNodeService {
    cluster_id: [u8; 16],
    replica: Arc<Mutex<MetaRaftReplica>>,
    tso: Arc<ReplicatedTso>,
    runtime_notify: Option<Arc<Notify>>,
    proposal_gate: Arc<Mutex<()>>,
    reservation_gate: Arc<Mutex<()>>,
}

impl MetaNodeService {
    #[must_use]
    pub fn new(
        cluster_id: [u8; 16],
        replica: Arc<Mutex<MetaRaftReplica>>,
        tso: Arc<ReplicatedTso>,
    ) -> Self {
        Self {
            cluster_id,
            replica,
            tso,
            runtime_notify: None,
            proposal_gate: Arc::new(Mutex::new(())),
            reservation_gate: Arc::new(Mutex::new(())),
        }
    }

    #[must_use]
    pub fn with_runtime_notify(mut self, notify: Arc<Notify>) -> Self {
        self.runtime_notify = Some(notify);
        self
    }

    fn validate(
        &self,
        context: Option<cluster_protocol::proto::RequestContext>,
    ) -> Result<(CommonRequestContext, u128), Status> {
        let context: CommonRequestContext = context
            .ok_or_else(|| Status::invalid_argument("missing request context"))?
            .try_into()
            .map_err(protocol_status)?;
        if context.cluster_id() != &self.cluster_id {
            return Err(Status::permission_denied("cluster identity mismatch"));
        }
        context
            .ensure_active_at(unix_time_ms()?)
            .map_err(protocol_status)?;
        let request_id = u128::from_be_bytes(*context.request_id());
        Ok((context, request_id))
    }
}

#[tonic::async_trait]
impl MetaService for MetaNodeService {
    async fn get_catalog(
        &self,
        request: Request<GetCatalogRequest>,
    ) -> Result<Response<GetCatalogResponse>, Status> {
        let request = request.into_inner();
        self.validate(request.context)?;
        let replica = self.replica.lock().await;
        require_leader(&replica, &self.tso)?;
        let state = replica.state().catalog();
        if state.revision() < request.minimum_revision {
            return Err(Status::unavailable(format!(
                "Catalog revision {} is below requested minimum {}",
                state.revision(),
                request.minimum_revision
            )));
        }
        let payload = state
            .encode_snapshot()
            .map_err(|error| Status::internal(error.to_string()))?;
        Ok(Response::new(GetCatalogResponse {
            snapshot: Some(CatalogSnapshot {
                revision: state.revision(),
                checksum: crc32fast::hash(&payload),
                payload,
            }),
        }))
    }

    type WatchCatalogStream = ReceiverStream<Result<WireCatalogEvent, Status>>;

    async fn watch_catalog(
        &self,
        request: Request<WatchCatalogRequest>,
    ) -> Result<Response<Self::WatchCatalogStream>, Status> {
        let request = request.into_inner();
        let (context, _) = self.validate(request.context)?;
        {
            let replica = self.replica.lock().await;
            require_leader(&replica, &self.tso)?;
            replica
                .state()
                .watch_after(request.after_revision)
                .map_err(meta_state_status)?;
        }
        let replica = Arc::clone(&self.replica);
        let tso = Arc::clone(&self.tso);
        let deadline = context.deadline_unix_ms();
        let (sender, receiver) = mpsc::channel(WATCH_CHANNEL_CAPACITY);
        tokio::spawn(async move {
            let mut after_revision = request.after_revision;
            loop {
                match unix_time_ms() {
                    Ok(now) if now < deadline => {}
                    Ok(_) => {
                        let _ = sender
                            .send(Err(Status::deadline_exceeded(
                                "Catalog watch deadline expired",
                            )))
                            .await;
                        return;
                    }
                    Err(status) => {
                        let _ = sender.send(Err(status)).await;
                        return;
                    }
                }
                let batch = {
                    let replica = replica.lock().await;
                    match require_leader(&replica, &tso) {
                        Ok(()) => replica
                            .state()
                            .watch_after(after_revision)
                            .map_err(meta_state_status),
                        Err(status) => Err(status),
                    }
                };
                match batch {
                    Ok(batch) => {
                        for event in batch.events() {
                            if sender
                                .send(Ok(WireCatalogEvent {
                                    revision: event.revision(),
                                    command: event.command().to_vec(),
                                    checksum: event.checksum(),
                                }))
                                .await
                                .is_err()
                            {
                                return;
                            }
                            after_revision = event.revision();
                        }
                    }
                    Err(status) => {
                        let _ = sender.send(Err(status)).await;
                        return;
                    }
                }
                tokio::time::sleep(WATCH_POLL_INTERVAL).await;
            }
        });
        Ok(Response::new(ReceiverStream::new(receiver)))
    }

    async fn propose(
        &self,
        request: Request<ProposeRequest>,
    ) -> Result<Response<ProposeResponse>, Status> {
        let _gate = self.proposal_gate.lock().await;
        let request = request.into_inner();
        let (context, request_id) = self.validate(request.context)?;
        let command = CommandPayload::try_from(request.command).map_err(protocol_status)?;
        let decoded = CatalogCommand::decode(command.as_bytes())
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        if decoded.command_id() != request_id {
            return Err(Status::invalid_argument(
                "request ID differs from Catalog command ID",
            ));
        }
        let mut replica = self.replica.lock().await;
        require_leader(&replica, &self.tso)?;
        let mut validation = replica.state().catalog().clone();
        let expected = validation
            .apply(decoded)
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
        replica
            .propose(command.into_bytes())
            .map_err(meta_raft_status)?;
        if let Some(notify) = &self.runtime_notify {
            drop(replica);
            notify.notify_one();
            self.wait_for_catalog_commit(expected.revision(), context.deadline_unix_ms())
                .await?;
        } else {
            ensure_locally_committed(&mut replica)?;
            if replica.state().catalog().revision() < expected.revision() {
                return Err(Status::unavailable(
                    "Meta quorum did not commit the proposal",
                ));
            }
        }
        Ok(Response::new(ProposeResponse {
            revision: expected.revision(),
            duplicate: expected.duplicate(),
        }))
    }

    async fn allocate_timestamp(
        &self,
        request: Request<AllocateTimestampRequest>,
    ) -> Result<Response<AllocateTimestampResponse>, Status> {
        let _gate = self.reservation_gate.lock().await;
        let request = request.into_inner();
        let (context, request_id) = self.validate(request.context)?;
        if request.count == 0 {
            return Err(Status::invalid_argument("timestamp count cannot be zero"));
        }
        let observed_micros = request
            .observed_physical_ms
            .checked_mul(1_000)
            .and_then(|value| i64::try_from(value).ok())
            .ok_or_else(|| Status::invalid_argument("observed physical time is out of range"))?;
        let mut replica = self.replica.lock().await;
        require_leader(&replica, &self.tso)?;
        let batch = match self.tso.allocate(request.count) {
            Ok(batch) => batch,
            Err(TsoError::LeaseExhausted) => {
                let reservation = self
                    .tso
                    .plan_reservation_after(
                        replica.state().timestamp_high_water(),
                        request_id,
                        request.count,
                        observed_micros,
                    )
                    .map_err(tso_status)?;
                replica
                    .propose_timestamp(reservation.clone())
                    .map_err(meta_raft_status)?;
                if let Some(notify) = &self.runtime_notify {
                    let target = reservation.new_high_water();
                    drop(replica);
                    notify.notify_one();
                    self.wait_for_timestamp_commit(target, context.deadline_unix_ms())
                        .await?;
                    replica = self.replica.lock().await;
                    require_leader(&replica, &self.tso)?;
                } else {
                    ensure_locally_committed(&mut replica)?;
                }
                self.tso
                    .activate_committed(&reservation, replica.state().timestamp_high_water())
                    .map_err(tso_status)?;
                self.tso.allocate(request.count).map_err(tso_status)?
            }
            Err(error) => return Err(tso_status(error)),
        };
        let high_water = replica.state().timestamp_high_water();
        Ok(Response::new(AllocateTimestampResponse {
            first_physical_ms: physical_ms(batch.first())?,
            first_logical: batch.first().logical(),
            count: batch.count(),
            lease_high_water_physical_ms: physical_ms(high_water)?,
            lease_high_water_logical: high_water.logical(),
        }))
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let request = request.into_inner();
        self.validate(request.context)?;
        if request.node_id == 0
            || request.advertise_address.is_empty()
            || request.capacity.len() > 1024 * 1024
        {
            return Err(Status::invalid_argument("invalid node heartbeat"));
        }
        let replica = self.replica.lock().await;
        require_leader(&replica, &self.tso)?;
        Ok(Response::new(HeartbeatResponse {
            catalog_revision: replica.state().catalog().revision(),
            lease_expires_unix_ms: unix_time_ms()?.saturating_add(HEARTBEAT_LEASE_MS),
        }))
    }
}

impl MetaNodeService {
    async fn wait_for_catalog_commit(&self, revision: u64, deadline: u64) -> Result<(), Status> {
        loop {
            {
                let replica = self.replica.lock().await;
                if replica.state().catalog().revision() >= revision {
                    return Ok(());
                }
                require_leader(&replica, &self.tso)?;
            }
            wait_for_commit_poll(deadline).await?;
        }
    }

    async fn wait_for_timestamp_commit(
        &self,
        high_water: TransactionTime,
        deadline: u64,
    ) -> Result<(), Status> {
        loop {
            {
                let replica = self.replica.lock().await;
                let committed = replica.state().timestamp_high_water();
                if committed == high_water {
                    return Ok(());
                }
                if committed > high_water {
                    return Err(Status::aborted(
                        "timestamp reservation was superseded before acknowledgement",
                    ));
                }
                require_leader(&replica, &self.tso)?;
            }
            wait_for_commit_poll(deadline).await?;
        }
    }
}

async fn wait_for_commit_poll(deadline: u64) -> Result<(), Status> {
    if unix_time_ms()? >= deadline {
        return Err(Status::deadline_exceeded(
            "Meta quorum commit deadline expired",
        ));
    }
    tokio::time::sleep(Duration::from_millis(5)).await;
    Ok(())
}

fn ensure_locally_committed(replica: &mut MetaRaftReplica) -> Result<(), Status> {
    let messages = replica.drain_ready().map_err(meta_raft_status)?;
    if messages.is_empty() {
        Ok(())
    } else {
        Err(Status::unavailable(
            "Meta proposal requires peer delivery before it can be acknowledged",
        ))
    }
}

fn require_leader(replica: &MetaRaftReplica, tso: &ReplicatedTso) -> Result<(), Status> {
    if replica.is_leader() {
        return Ok(());
    }
    let _ = tso.fence();
    let mut status = Status::failed_precondition("Meta node is not Leader");
    status
        .metadata_mut()
        .insert("dtgproxy-reason", MetadataValue::from_static("not_leader"));
    if let Some(leader) = replica.leader_id()
        && let Ok(value) = MetadataValue::try_from(leader.to_string())
    {
        status.metadata_mut().insert("dtgproxy-leader-node", value);
    }
    Err(status)
}

fn meta_state_status(error: MetaStateError) -> Status {
    match error {
        MetaStateError::RevisionCompacted { compacted_through } => {
            let mut status = Status::out_of_range(error.to_string());
            status.metadata_mut().insert(
                "dtgproxy-reason",
                MetadataValue::from_static("revision_compacted"),
            );
            if let Ok(value) = MetadataValue::try_from(compacted_through.to_string()) {
                status
                    .metadata_mut()
                    .insert("dtgproxy-current-revision", value);
            }
            status
        }
        _ => Status::internal(error.to_string()),
    }
}

fn meta_raft_status(error: MetaRaftError) -> Status {
    let message = error.to_string();
    match error {
        MetaRaftError::NotLeader { leader_id } => {
            let mut status = Status::failed_precondition(message);
            status
                .metadata_mut()
                .insert("dtgproxy-reason", MetadataValue::from_static("not_leader"));
            if let Some(leader) = leader_id
                && let Ok(value) = MetadataValue::try_from(leader.to_string())
            {
                status.metadata_mut().insert("dtgproxy-leader-node", value);
            }
            status
        }
        MetaRaftError::Catalog(_) | MetaRaftError::Tso(_) => Status::failed_precondition(message),
        _ => Status::internal(message),
    }
}

fn tso_status(error: TsoError) -> Status {
    let message = error.to_string();
    match error {
        TsoError::InvalidConfiguration | TsoError::InvalidReservation => {
            Status::invalid_argument(message)
        }
        TsoError::FutureDriftExceeded { .. } => Status::unavailable(message),
        TsoError::LeaseExhausted => Status::resource_exhausted(message),
        _ => Status::internal(message),
    }
}

fn protocol_status(error: ProtocolError) -> Status {
    let message = error.to_string();
    match error {
        ProtocolError::DeadlineExpired { .. } => Status::deadline_exceeded(message),
        ProtocolError::CommandTooLarge { .. } => Status::resource_exhausted(message),
        _ => Status::invalid_argument(message),
    }
}

fn unix_time_ms() -> Result<u64, Status> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| Status::internal("system clock is before Unix epoch"))?
        .as_millis();
    u64::try_from(millis).map_err(|_| Status::internal("system clock overflow"))
}

fn physical_ms(timestamp: TransactionTime) -> Result<u64, Status> {
    let millis = timestamp.physical_micros().div_euclid(1_000);
    u64::try_from(millis).map_err(|_| Status::internal("negative timestamp cannot be served"))
}
