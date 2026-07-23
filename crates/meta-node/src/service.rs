use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use analytics_ledger::{AnalyticsJobId, JobCommand, JobError};
use cluster_protocol::proto::meta_service_server::MetaService;
use cluster_protocol::proto::{
    AcquireAnalyticsGcLeaseRequest, AcquireAnalyticsGcLeaseResponse, AcquireControllerLeaseRequest,
    AcquireControllerLeaseResponse, AllocateTimestampRequest, AllocateTimestampResponse,
    AnalyticsJobCandidate as WireAnalyticsJobCandidate,
    AnalyticsJobRecord as WireAnalyticsJobRecord,
    AnalyticsJobTombstoneRecord as WireAnalyticsJobTombstoneRecord,
    CatalogEvent as WireCatalogEvent, CatalogSnapshot, GetAnalyticsJobRequest,
    GetAnalyticsJobResponse, GetCatalogRequest, GetCatalogResponse, HeartbeatRequest,
    HeartbeatResponse, ListAnalyticsJobTombstonesRequest, ListAnalyticsJobTombstonesResponse,
    ListAnalyticsJobsRequest, ListAnalyticsJobsResponse, ListClaimableAnalyticsJobsRequest,
    ListClaimableAnalyticsJobsResponse, ProposeAnalyticsJobRequest, ProposeAnalyticsJobResponse,
    ProposeRequest, ProposeResponse, WatchCatalogRequest,
};
use cluster_protocol::{CommandPayload, CommonRequestContext, ProtocolError};
use control_plane::CatalogCommand;
use temporal_types::TransactionTime;
use tokio::sync::{Mutex, Notify, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::metadata::MetadataValue;
use tonic::{Request, Response, Status};

use crate::{
    AnalyticsGcLeaseCommand, MetaRaftError, MetaRaftReplica, MetaStateError, ReplicatedTso,
    TsoError,
};

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
    controller_lease: Arc<Mutex<Option<ControllerLease>>>,
}

#[derive(Clone, Copy)]
struct ControllerLease {
    controller_id: u64,
    owner_term: u64,
    expires_unix_ms: u64,
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
            controller_lease: Arc::new(Mutex::new(None)),
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

    async fn propose_analytics_job(
        &self,
        request: Request<ProposeAnalyticsJobRequest>,
    ) -> Result<Response<ProposeAnalyticsJobResponse>, Status> {
        let _gate = self.proposal_gate.lock().await;
        let request = request.into_inner();
        let (context, request_id) = self.validate(request.context)?;
        let payload = CommandPayload::try_from(request.command).map_err(protocol_status)?;
        let command = JobCommand::decode(payload.as_bytes()).map_err(analytics_status)?;
        if command.command_id() != request_id {
            return Err(Status::invalid_argument(
                "request ID differs from analytics command ID",
            ));
        }
        let submission_request_id = command
            .submitted_spec()
            .map(|spec| spec.submission_request_id());
        let target_job_id = command.target_job_id();
        let mut replica = self.replica.lock().await;
        require_leader(&replica, &self.tso)?;
        if let Some((gateway_id, gc_epoch)) = command.reclamation_gc_fence() {
            let now_unix_ms = unix_time_ms()?;
            let lease = replica
                .state()
                .analytics_gc_lease()
                .ok_or_else(|| Status::failed_precondition("analytics GC lease is not held"))?;
            if lease.gateway_id() != gateway_id
                || lease.gc_epoch() != gc_epoch
                || lease.expires_unix_ms() <= now_unix_ms
            {
                return Err(Status::failed_precondition(
                    "analytics Artifact reclamation acknowledgement has a stale GC lease",
                ));
            }
        }
        if let Some(spec) = command.submitted_spec() {
            let catalog = replica.state().catalog();
            let graph = catalog
                .graph(spec.graph_id())
                .ok_or_else(|| Status::failed_precondition("analytics job graph does not exist"))?;
            if spec.catalog_revision() != catalog.revision()
                || spec.topology_epoch() != graph.topology().epoch()
                || spec.schema_version() != graph.schema_version()
                || spec.backend_generation() != graph.backend().generation()
            {
                return Err(Status::failed_precondition(
                    "analytics job snapshot fence is stale",
                ));
            }
        }
        let mut validation = replica.state().analytics().clone();
        let expected = validation
            .apply(command.clone())
            .map_err(analytics_status)?;
        let canonical_job_id = submission_request_id
            .and_then(|submission_id| validation.job_for_submission(submission_id))
            .or(target_job_id)
            .map(|job_id| job_id.value().to_be_bytes().to_vec())
            .unwrap_or_default();
        replica
            .propose_analytics(command)
            .map_err(meta_raft_status)?;
        if let Some(notify) = &self.runtime_notify {
            drop(replica);
            notify.notify_one();
            self.wait_for_analytics_commit(expected.ledger_revision(), context.deadline_unix_ms())
                .await?;
        } else {
            ensure_locally_committed(&mut replica)?;
            if replica.state().analytics().revision() < expected.ledger_revision() {
                return Err(Status::unavailable(
                    "Meta quorum did not commit the analytics proposal",
                ));
            }
        }
        Ok(Response::new(ProposeAnalyticsJobResponse {
            ledger_revision: expected.ledger_revision(),
            job_revision: expected.job_revision(),
            duplicate: expected.duplicate(),
            request_duplicate: expected.request_duplicate(),
            canonical_job_id,
        }))
    }

    async fn get_analytics_job(
        &self,
        request: Request<GetAnalyticsJobRequest>,
    ) -> Result<Response<GetAnalyticsJobResponse>, Status> {
        let request = request.into_inner();
        self.validate(request.context)?;
        let job_id = decode_job_id(&request.job_id)?;
        let replica = self.replica.lock().await;
        require_leader(&replica, &self.tso)?;
        let analytics = replica.state().analytics();
        let record = analytics.encode_job(job_id).map_err(analytics_status)?;
        Ok(Response::new(GetAnalyticsJobResponse {
            ledger_revision: analytics.revision(),
            checksum: crc32fast::hash(&record),
            record,
        }))
    }

    async fn list_claimable_analytics_jobs(
        &self,
        request: Request<ListClaimableAnalyticsJobsRequest>,
    ) -> Result<Response<ListClaimableAnalyticsJobsResponse>, Status> {
        let request = request.into_inner();
        self.validate(request.context)?;
        let after = (!request.after_job_id.is_empty())
            .then(|| decode_job_id(&request.after_job_id))
            .transpose()?;
        let limit = usize::try_from(request.limit)
            .map_err(|_| Status::invalid_argument("analytics list limit is invalid"))?;
        let replica = self.replica.lock().await;
        require_leader(&replica, &self.tso)?;
        let analytics = replica.state().analytics();
        let candidates = analytics
            .claimable_jobs(request.now_unix_ms, after, limit)
            .map_err(analytics_status)?
            .into_iter()
            .map(|candidate| WireAnalyticsJobCandidate {
                job_id: candidate.job_id().value().to_be_bytes().to_vec(),
                job_revision: candidate.job_revision(),
                expired_lease_epoch: candidate.lease_epoch().unwrap_or(0),
            })
            .collect();
        Ok(Response::new(ListClaimableAnalyticsJobsResponse {
            ledger_revision: analytics.revision(),
            candidates,
        }))
    }

    async fn list_analytics_jobs(
        &self,
        request: Request<ListAnalyticsJobsRequest>,
    ) -> Result<Response<ListAnalyticsJobsResponse>, Status> {
        let request = request.into_inner();
        self.validate(request.context)?;
        let after = (!request.after_job_id.is_empty())
            .then(|| decode_job_id(&request.after_job_id))
            .transpose()?;
        let limit = usize::try_from(request.limit)
            .map_err(|_| Status::invalid_argument("analytics list limit is invalid"))?;
        let replica = self.replica.lock().await;
        require_leader(&replica, &self.tso)?;
        let analytics = replica.state().analytics();
        let jobs = analytics
            .list_jobs(after, limit)
            .map_err(analytics_status)?
            .into_iter()
            .map(|job_id| {
                let record = analytics.encode_job(job_id).map_err(analytics_status)?;
                Ok(WireAnalyticsJobRecord {
                    job_id: job_id.value().to_be_bytes().to_vec(),
                    checksum: crc32fast::hash(&record),
                    record,
                })
            })
            .collect::<Result<Vec<_>, Status>>()?;
        Ok(Response::new(ListAnalyticsJobsResponse {
            ledger_revision: analytics.revision(),
            jobs,
        }))
    }

    async fn list_analytics_job_tombstones(
        &self,
        request: Request<ListAnalyticsJobTombstonesRequest>,
    ) -> Result<Response<ListAnalyticsJobTombstonesResponse>, Status> {
        let request = request.into_inner();
        self.validate(request.context)?;
        let after = (!request.after_job_id.is_empty())
            .then(|| decode_job_id(&request.after_job_id))
            .transpose()?;
        let limit = usize::try_from(request.limit)
            .map_err(|_| Status::invalid_argument("analytics tombstone list limit is invalid"))?;
        let replica = self.replica.lock().await;
        require_leader(&replica, &self.tso)?;
        let analytics = replica.state().analytics();
        let job_ids = analytics
            .list_tombstones(after, limit)
            .map_err(analytics_status)?;
        let next_job_id = (job_ids.len() == limit)
            .then(|| {
                job_ids
                    .last()
                    .map(|job_id| job_id.value().to_be_bytes().to_vec())
            })
            .flatten()
            .unwrap_or_default();
        let tombstones = job_ids
            .into_iter()
            .map(|job_id| {
                let record = analytics
                    .encode_tombstone(job_id)
                    .map_err(analytics_status)?;
                Ok(WireAnalyticsJobTombstoneRecord {
                    job_id: job_id.value().to_be_bytes().to_vec(),
                    checksum: crc32fast::hash(&record),
                    record,
                })
            })
            .collect::<Result<Vec<_>, Status>>()?;
        Ok(Response::new(ListAnalyticsJobTombstonesResponse {
            ledger_revision: analytics.revision(),
            tombstones,
            next_job_id,
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

    async fn acquire_controller_lease(
        &self,
        request: Request<AcquireControllerLeaseRequest>,
    ) -> Result<Response<AcquireControllerLeaseResponse>, Status> {
        let request = request.into_inner();
        self.validate(request.context)?;
        if request.controller_id == 0 {
            return Err(Status::invalid_argument("Controller ID must be nonzero"));
        }
        let replica = self.replica.lock().await;
        require_leader(&replica, &self.tso)?;
        let owner_term = replica.current_term();
        let now = unix_time_ms()?;
        let mut lease = self.controller_lease.lock().await;
        if let Some(active) = *lease
            && active.owner_term == owner_term
            && active.expires_unix_ms > now
            && active.controller_id != request.controller_id
        {
            return Err(Status::resource_exhausted(
                "another Controller owns the active Meta lease",
            ));
        }
        let expires_unix_ms = now
            .checked_add(HEARTBEAT_LEASE_MS)
            .ok_or_else(|| Status::internal("Controller lease time overflow"))?;
        *lease = Some(ControllerLease {
            controller_id: request.controller_id,
            owner_term,
            expires_unix_ms,
        });
        Ok(Response::new(AcquireControllerLeaseResponse {
            owner_term,
            lease_expires_unix_ms: expires_unix_ms,
        }))
    }

    async fn acquire_analytics_gc_lease(
        &self,
        request: Request<AcquireAnalyticsGcLeaseRequest>,
    ) -> Result<Response<AcquireAnalyticsGcLeaseResponse>, Status> {
        let _gate = self.proposal_gate.lock().await;
        let request = request.into_inner();
        let (context, request_id) = self.validate(request.context)?;
        if request.gateway_id == 0 {
            return Err(Status::invalid_argument("Gateway ID must be nonzero"));
        }
        let mut replica = self.replica.lock().await;
        require_leader(&replica, &self.tso)?;
        let owner_term = replica.current_term();
        let now = unix_time_ms()?;
        let expires_unix_ms = now
            .checked_add(HEARTBEAT_LEASE_MS)
            .ok_or_else(|| Status::internal("Analytics GC lease time overflow"))?;
        let current = replica.state().analytics_gc_lease();
        let expected_gc_epoch = current.map_or(0, |lease| lease.gc_epoch());
        let gc_epoch = match current {
            Some(active)
                if active.owner_term() == owner_term
                    && active.expires_unix_ms() > now
                    && active.gateway_id() != request.gateway_id =>
            {
                return Err(analytics_gc_resource_exhausted(
                    "another Gateway owns the active analytics GC lease",
                    "analytics_gc_lease_owned",
                ));
            }
            Some(active)
                if active.owner_term() == owner_term
                    && active.gateway_id() == request.gateway_id =>
            {
                active.gc_epoch()
            }
            Some(active) => active.gc_epoch().checked_add(1).ok_or_else(|| {
                analytics_gc_resource_exhausted(
                    "Analytics GC epoch exhausted",
                    "analytics_gc_epoch_exhausted",
                )
            })?,
            None => analytics_gc_epoch(owner_term, 1)?,
        };
        let command = AnalyticsGcLeaseCommand::new(
            request_id,
            expected_gc_epoch,
            request.gateway_id,
            owner_term,
            gc_epoch,
            now,
            expires_unix_ms,
        )
        .map_err(meta_state_status)?;
        replica
            .propose_analytics_gc_lease(command)
            .map_err(meta_raft_status)?;
        if let Some(notify) = &self.runtime_notify {
            drop(replica);
            notify.notify_one();
            self.wait_for_analytics_gc_lease(
                request.gateway_id,
                gc_epoch,
                expires_unix_ms,
                context.deadline_unix_ms(),
            )
            .await?;
        } else {
            ensure_locally_committed(&mut replica)?;
            let committed = replica
                .state()
                .analytics_gc_lease()
                .ok_or_else(|| Status::unavailable("analytics GC lease was not committed"))?;
            if committed.gateway_id() != request.gateway_id
                || committed.gc_epoch() != gc_epoch
                || committed.expires_unix_ms() != expires_unix_ms
            {
                return Err(Status::unavailable(
                    "analytics GC lease commit differs from the proposal",
                ));
            }
        }
        Ok(Response::new(AcquireAnalyticsGcLeaseResponse {
            owner_term,
            gc_epoch,
            lease_expires_unix_ms: expires_unix_ms,
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

    async fn wait_for_analytics_commit(&self, revision: u64, deadline: u64) -> Result<(), Status> {
        loop {
            {
                let replica = self.replica.lock().await;
                if replica.state().analytics().revision() >= revision {
                    return Ok(());
                }
                require_leader(&replica, &self.tso)?;
            }
            wait_for_commit_poll(deadline).await?;
        }
    }

    async fn wait_for_analytics_gc_lease(
        &self,
        gateway_id: u64,
        gc_epoch: u64,
        expires_unix_ms: u64,
        deadline: u64,
    ) -> Result<(), Status> {
        loop {
            {
                let replica = self.replica.lock().await;
                if let Some(lease) = replica.state().analytics_gc_lease()
                    && lease.gateway_id() == gateway_id
                    && lease.gc_epoch() == gc_epoch
                    && lease.expires_unix_ms() == expires_unix_ms
                {
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

fn analytics_gc_resource_exhausted(message: impl Into<String>, reason: &'static str) -> Status {
    let mut status = Status::resource_exhausted(message);
    status
        .metadata_mut()
        .insert("dtgproxy-reason", MetadataValue::from_static(reason));
    status
}

fn meta_state_status(error: MetaStateError) -> Status {
    let message = error.to_string();
    match error {
        MetaStateError::RevisionCompacted { compacted_through } => {
            let mut status = Status::out_of_range(message);
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
        MetaStateError::GcLeaseConflict => {
            analytics_gc_resource_exhausted(message, "analytics_gc_lease_owned")
        }
        MetaStateError::GcLeaseStaleEpoch | MetaStateError::GcLeaseReplayMismatch => {
            Status::failed_precondition(message)
        }
        MetaStateError::InvalidGcLeaseCommand | MetaStateError::GcLeaseChecksumMismatch => {
            Status::invalid_argument(message)
        }
        _ => Status::internal(message),
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
        MetaRaftError::Catalog(_) | MetaRaftError::Analytics(_) | MetaRaftError::Tso(_) => {
            Status::failed_precondition(message)
        }
        MetaRaftError::State(error) => meta_state_status(error),
        _ => Status::internal(message),
    }
}

fn analytics_status(error: JobError) -> Status {
    let message = error.to_string();
    match error {
        JobError::UnknownJob(_) => Status::not_found(message),
        JobError::JobCapacity | JobError::SnapshotCapacity => Status::resource_exhausted(message),
        JobError::StaleJobRevision { .. }
        | JobError::StaleTopology { .. }
        | JobError::StaleLease
        | JobError::LeaseActive
        | JobError::TerminalJob
        | JobError::JobNotClaimable
        | JobError::InvalidStateTransition
        | JobError::SubmissionReplayMismatch { .. }
        | JobError::CommandReplayMismatch { .. } => Status::failed_precondition(message),
        _ => Status::invalid_argument(message),
    }
}

fn decode_job_id(bytes: &[u8]) -> Result<AnalyticsJobId, Status> {
    let encoded: [u8; 16] = bytes
        .try_into()
        .map_err(|_| Status::invalid_argument("analytics job ID must contain 16 bytes"))?;
    AnalyticsJobId::new(u128::from_be_bytes(encoded)).map_err(analytics_status)
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

fn analytics_gc_epoch(owner_term: u64, sequence: u32) -> Result<u64, Status> {
    let term = u32::try_from(owner_term)
        .map_err(|_| Status::resource_exhausted("Meta term exceeds analytics GC epoch space"))?;
    Ok((u64::from(term) << 32) | u64::from(sequence))
}

fn physical_ms(timestamp: TransactionTime) -> Result<u64, Status> {
    let millis = timestamp.physical_micros().div_euclid(1_000);
    u64::try_from(millis).map_err(|_| Status::internal("negative timestamp cannot be served"))
}
