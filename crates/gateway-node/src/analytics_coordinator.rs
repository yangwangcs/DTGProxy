use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, SyncSender, sync_channel};
use std::sync::{Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use analytics_api::AlgorithmResult;
use analytics_ledger::{
    AnalyticsJobId, ArtifactManifest, JobCommand, JobRecord, JobSpec, JobState, LedgerState,
    NextExecutionStage,
};
use cluster_protocol::CLUSTER_PROTOCOL_VERSION;
use cluster_protocol::proto::meta_service_client::MetaServiceClient;
use cluster_protocol::proto::{
    AllocateTimestampRequest, GetAnalyticsJobRequest, ProposeAnalyticsJobRequest, RequestContext,
};
use procedure_runtime::{
    AnalyticsCancelRequest, AnalyticsCancelResponse, AnalyticsResultPage, AnalyticsResultsRequest,
    AnalyticsStatus, AnalyticsStatusRequest, AnalyticsSubmitRequest, AnalyticsSubmitResponse,
    ClusterAnalyticsCoordinator, ClusterAnalyticsError,
};
use temporal_types::TransactionTime;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;
use tonic::transport::{Channel, Endpoint};

const STARTUP_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_FAILURE_BYTES: usize = 4_096;

static NEXT_WORKER_NONCE: AtomicU64 = AtomicU64::new(1);

pub(crate) type ResultReaderFuture<'a> =
    Pin<Box<dyn Future<Output = Result<AnalyticsResultPage, ClusterAnalyticsError>> + Send + 'a>>;

pub(crate) trait AnalyticsResultReader: Send + Sync {
    fn read<'a>(
        &'a self,
        record: &'a JobRecord,
        manifest: &'a ArtifactManifest,
        invocation_request_id: u128,
        offset: usize,
        limit: usize,
        deadline_unix_ms: u64,
    ) -> ResultReaderFuture<'a>;
}

#[cfg(test)]
struct UnavailableResultReader;

#[cfg(test)]
impl AnalyticsResultReader for UnavailableResultReader {
    fn read<'a>(
        &'a self,
        _record: &'a JobRecord,
        _manifest: &'a ArtifactManifest,
        _invocation_request_id: u128,
        _offset: usize,
        _limit: usize,
        _deadline_unix_ms: u64,
    ) -> ResultReaderFuture<'a> {
        Box::pin(async {
            Err(coordinator_error(
                "DTG-ANALYTICS-RESULT-READER-UNAVAILABLE",
                "cluster artifact result reading is not configured",
            ))
        })
    }
}

pub(crate) struct MetaClusterAnalyticsCoordinator {
    sender: Mutex<Option<mpsc::Sender<Work>>>,
    shutdown: watch::Sender<bool>,
    worker: Mutex<Option<JoinHandle<()>>>,
}

impl MetaClusterAnalyticsCoordinator {
    #[cfg(test)]
    pub(crate) fn new(
        cluster_id: [u8; 16],
        meta_endpoints: Vec<SocketAddr>,
        queue_capacity: usize,
    ) -> Result<Self, ClusterAnalyticsError> {
        Self::new_with_reader(
            cluster_id,
            meta_endpoints,
            queue_capacity,
            Arc::new(UnavailableResultReader),
        )
    }

    pub(crate) fn new_with_reader(
        cluster_id: [u8; 16],
        meta_endpoints: Vec<SocketAddr>,
        queue_capacity: usize,
        result_reader: Arc<dyn AnalyticsResultReader>,
    ) -> Result<Self, ClusterAnalyticsError> {
        if cluster_id == [0; 16]
            || meta_endpoints.is_empty()
            || meta_endpoints.iter().any(|endpoint| endpoint.port() == 0)
            || queue_capacity == 0
        {
            return Err(coordinator_error(
                "DTG-ANALYTICS-COORDINATOR-CONFIG",
                "cluster analytics coordinator configuration is invalid",
            ));
        }
        let (sender, receiver) = mpsc::channel(queue_capacity);
        let (shutdown, shutdown_receiver) = watch::channel(false);
        let (startup_sender, startup_receiver) = sync_channel(1);
        let nonce = worker_nonce(cluster_id, &meta_endpoints);
        let max_in_flight = queue_capacity.min(64);
        let worker = thread::Builder::new()
            .name("dtg-analytics-meta".into())
            .spawn(move || {
                let runtime = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(runtime) => runtime,
                    Err(error) => {
                        let _ = startup_sender.send(Err(error.to_string()));
                        return;
                    }
                };
                let worker = match MetaWorker::new(
                    cluster_id,
                    meta_endpoints,
                    nonce,
                    result_reader,
                    &runtime,
                ) {
                    Ok(worker) => worker,
                    Err(error) => {
                        let _ = startup_sender.send(Err(error.to_string()));
                        return;
                    }
                };
                let _ = startup_sender.send(Ok(()));
                runtime.block_on(worker.run(receiver, shutdown_receiver, max_in_flight));
            })
            .map_err(|error| {
                coordinator_error(
                    "DTG-ANALYTICS-COORDINATOR-START",
                    format!("failed to start analytics Meta worker: {error}"),
                )
            })?;
        match startup_receiver.recv_timeout(STARTUP_TIMEOUT) {
            Ok(Ok(())) => Ok(Self {
                sender: Mutex::new(Some(sender)),
                shutdown,
                worker: Mutex::new(Some(worker)),
            }),
            Ok(Err(message)) => {
                let _ = worker.join();
                Err(coordinator_error(
                    "DTG-ANALYTICS-COORDINATOR-START",
                    message,
                ))
            }
            Err(_) => Err(coordinator_error(
                "DTG-ANALYTICS-COORDINATOR-START",
                "analytics Meta worker did not start before its bound",
            )),
        }
    }

    fn execute(
        &self,
        mut work: Work,
        deadline_unix_ms: u64,
    ) -> Result<WorkResult, ClusterAnalyticsError> {
        let remaining = remaining_duration(deadline_unix_ms)?;
        let sender = lock(&self.sender, "analytics worker sender")?
            .as_ref()
            .cloned()
            .ok_or_else(|| {
                coordinator_error(
                    "DTG-ANALYTICS-COORDINATOR-UNAVAILABLE",
                    "analytics Meta worker is stopped",
                )
            })?;
        let response = work.response();
        match sender.try_send(work) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                return Err(coordinator_error(
                    "DTG-ANALYTICS-COORDINATOR-BUSY",
                    "analytics Meta worker queue is full",
                ));
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                return Err(coordinator_error(
                    "DTG-ANALYTICS-COORDINATOR-UNAVAILABLE",
                    "analytics Meta worker is unavailable",
                ));
            }
        }
        match response.recv_timeout(remaining) {
            Ok(result) => result,
            Err(RecvTimeoutError::Timeout) => Err(coordinator_error(
                "DTG-ANALYTICS-DEADLINE",
                "analytics Meta operation exceeded its absolute deadline",
            )),
            Err(RecvTimeoutError::Disconnected) => Err(coordinator_error(
                "DTG-ANALYTICS-COORDINATOR-UNAVAILABLE",
                "analytics Meta worker dropped the response",
            )),
        }
    }
}

impl ClusterAnalyticsCoordinator for MetaClusterAnalyticsCoordinator {
    fn submit(
        &self,
        request: AnalyticsSubmitRequest,
    ) -> Result<AnalyticsSubmitResponse, ClusterAnalyticsError> {
        let deadline = request.deadline_unix_ms();
        match self.execute(Work::submit(request), deadline)? {
            WorkResult::Submit(response) => Ok(response),
            _ => Err(internal_result_error()),
        }
    }

    fn status(
        &self,
        request: AnalyticsStatusRequest,
    ) -> Result<AnalyticsStatus, ClusterAnalyticsError> {
        let deadline = request.deadline_unix_ms();
        match self.execute(Work::status(request), deadline)? {
            WorkResult::Status(response) => Ok(response),
            _ => Err(internal_result_error()),
        }
    }

    fn results(
        &self,
        request: AnalyticsResultsRequest,
    ) -> Result<AnalyticsResultPage, ClusterAnalyticsError> {
        let deadline = request.deadline_unix_ms();
        match self.execute(Work::results(request), deadline)? {
            WorkResult::Results(response) => Ok(response),
            _ => Err(internal_result_error()),
        }
    }

    fn cancel(
        &self,
        request: AnalyticsCancelRequest,
    ) -> Result<AnalyticsCancelResponse, ClusterAnalyticsError> {
        let deadline = request.deadline_unix_ms();
        match self.execute(Work::cancel(request), deadline)? {
            WorkResult::Cancel(response) => Ok(response),
            _ => Err(internal_result_error()),
        }
    }
}

impl Drop for MetaClusterAnalyticsCoordinator {
    fn drop(&mut self) {
        let _ = self.shutdown.send(true);
        if let Ok(sender) = self.sender.get_mut() {
            sender.take();
        }
        if let Ok(worker) = self.worker.get_mut()
            && let Some(worker) = worker.take()
        {
            let _ = worker.join();
        }
    }
}

#[allow(clippy::large_enum_variant)]
enum Work {
    Submit {
        request: AnalyticsSubmitRequest,
        response: SyncSender<Result<WorkResult, ClusterAnalyticsError>>,
    },
    Status {
        request: AnalyticsStatusRequest,
        response: SyncSender<Result<WorkResult, ClusterAnalyticsError>>,
    },
    Results {
        request: AnalyticsResultsRequest,
        response: SyncSender<Result<WorkResult, ClusterAnalyticsError>>,
    },
    Cancel {
        request: AnalyticsCancelRequest,
        response: SyncSender<Result<WorkResult, ClusterAnalyticsError>>,
    },
}

impl Work {
    fn submit(request: AnalyticsSubmitRequest) -> Self {
        let (response, _) = sync_channel(1);
        Self::Submit { request, response }
    }

    fn status(request: AnalyticsStatusRequest) -> Self {
        let (response, _) = sync_channel(1);
        Self::Status { request, response }
    }

    fn results(request: AnalyticsResultsRequest) -> Self {
        let (response, _) = sync_channel(1);
        Self::Results { request, response }
    }

    fn cancel(request: AnalyticsCancelRequest) -> Self {
        let (response, _) = sync_channel(1);
        Self::Cancel { request, response }
    }

    fn response(&mut self) -> Receiver<Result<WorkResult, ClusterAnalyticsError>> {
        let (sender, receiver) = sync_channel(1);
        match self {
            Self::Submit { response, .. }
            | Self::Status { response, .. }
            | Self::Results { response, .. }
            | Self::Cancel { response, .. } => *response = sender,
        }
        receiver
    }
}

enum WorkResult {
    Submit(AnalyticsSubmitResponse),
    Status(AnalyticsStatus),
    Results(AnalyticsResultPage),
    Cancel(AnalyticsCancelResponse),
}

#[cfg(test)]
struct Probe {
    delay: Duration,
    value: u8,
    response: tokio::sync::oneshot::Sender<u8>,
}

#[derive(Clone)]
struct MetaWorker {
    cluster_id: [u8; 16],
    clients: Arc<Vec<MetaServiceClient<Channel>>>,
    nonce: u64,
    sequence: Arc<AtomicU64>,
    result_reader: Arc<dyn AnalyticsResultReader>,
}

impl MetaWorker {
    fn new(
        cluster_id: [u8; 16],
        meta_endpoints: Vec<SocketAddr>,
        nonce: u64,
        result_reader: Arc<dyn AnalyticsResultReader>,
        runtime: &tokio::runtime::Runtime,
    ) -> Result<Self, ClusterAnalyticsError> {
        let _guard = runtime.enter();
        let clients = meta_endpoints
            .into_iter()
            .map(|endpoint| {
                Endpoint::from_shared(format!("http://{endpoint}"))
                    .map(|endpoint| MetaServiceClient::new(endpoint.connect_lazy()))
                    .map_err(|error| {
                        coordinator_error(
                            "DTG-ANALYTICS-COORDINATOR-CONFIG",
                            format!("invalid Meta endpoint: {error}"),
                        )
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            cluster_id,
            clients: Arc::new(clients),
            nonce,
            sequence: Arc::new(AtomicU64::new(0)),
            result_reader,
        })
    }

    async fn run(
        self,
        mut receiver: mpsc::Receiver<Work>,
        mut shutdown: watch::Receiver<bool>,
        max_in_flight: usize,
    ) {
        let mut tasks = JoinSet::new();
        let mut ingress_open = true;
        while ingress_open || !tasks.is_empty() {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        tasks.abort_all();
                        while tasks.join_next().await.is_some() {}
                        return;
                    }
                }
                work = receiver.recv(), if ingress_open && tasks.len() < max_in_flight => {
                    match work {
                        Some(work) => {
                            let worker = self.clone();
                            tasks.spawn(async move { worker.handle(work).await });
                        }
                        None => ingress_open = false,
                    }
                }
                completed = tasks.join_next(), if !tasks.is_empty() => {
                    let _ = completed;
                }
            }
        }
    }

    async fn handle(&self, work: Work) {
        let response = match work {
            Work::Submit { request, response } => {
                (response, self.submit(request).await.map(WorkResult::Submit))
            }
            Work::Status { request, response } => {
                (response, self.status(request).await.map(WorkResult::Status))
            }
            Work::Results { request, response } => (
                response,
                self.results(request).await.map(WorkResult::Results),
            ),
            Work::Cancel { request, response } => {
                (response, self.cancel(request).await.map(WorkResult::Cancel))
            }
        };
        let _ = response.0.send(response.1);
    }

    #[cfg(test)]
    async fn run_probes(mut receiver: mpsc::Receiver<Probe>, max_in_flight: usize) {
        let mut tasks = JoinSet::new();
        let mut ingress_open = true;
        while ingress_open || !tasks.is_empty() {
            tokio::select! {
                probe = receiver.recv(), if ingress_open && tasks.len() < max_in_flight => {
                    match probe {
                        Some(probe) => {
                            tasks.spawn(async move {
                                tokio::time::sleep(probe.delay).await;
                                let _ = probe.response.send(probe.value);
                            });
                        }
                        None => ingress_open = false,
                    }
                }
                completed = tasks.join_next(), if !tasks.is_empty() => {
                    let _ = completed;
                }
            }
        }
    }

    async fn submit(
        &self,
        request: AnalyticsSubmitRequest,
    ) -> Result<AnalyticsSubmitResponse, ClusterAnalyticsError> {
        let (job_id, submitted_at_unix_ms) = self
            .allocate_job_candidate(request.deadline_unix_ms())
            .await?;
        let spec = JobSpec::new(
            job_id,
            request.submission_request_id(),
            request.graph_id(),
            request.catalog_revision(),
            request.topology_epoch(),
            request.schema_version(),
            request.backend_generation(),
            request.transaction_snapshot(),
            request.projection().clone(),
            request.algorithm(),
            request.algorithm_version(),
            request.provider(),
            request.provider_version(),
            request.parameters().to_vec(),
            request.security_fingerprint(),
            request.limits(),
        )
        .map_err(ledger_error)?;
        let command =
            JobCommand::submit(job_id.value(), spec, submitted_at_unix_ms).map_err(ledger_error)?;
        let response = self.propose(command, request.deadline_unix_ms()).await?;
        let canonical = decode_canonical_job_id(&response.canonical_job_id)?;
        Ok(AnalyticsSubmitResponse::new(canonical))
    }

    async fn status(
        &self,
        request: AnalyticsStatusRequest,
    ) -> Result<AnalyticsStatus, ClusterAnalyticsError> {
        let record = self
            .authorized_job(
                request.invocation_request_id(),
                request.job_id(),
                request.security_fingerprint(),
                request.deadline_unix_ms(),
            )
            .await?;
        status_from_record(&record)
    }

    async fn results(
        &self,
        request: AnalyticsResultsRequest,
    ) -> Result<AnalyticsResultPage, ClusterAnalyticsError> {
        let record = self
            .authorized_job(
                request.invocation_request_id(),
                request.job_id(),
                request.security_fingerprint(),
                request.deadline_unix_ms(),
            )
            .await?;
        match record.state() {
            JobState::Queued | JobState::Leased | JobState::Running => Err(coordinator_error(
                "DTG-ANALYTICS-JOB-NOT-READY",
                "analytics job has not reached a terminal state",
            )),
            JobState::Failed | JobState::Canceled if record.result().is_none() => {
                Err(coordinator_error(
                    "DTG-ANALYTICS-JOB-NO-RESULT",
                    "analytics job terminated without a result artifact",
                ))
            }
            JobState::Succeeded if record.result().is_some() => {
                let manifest = record.result().expect("matched Some result manifest");
                self.result_reader
                    .read(
                        &record,
                        manifest,
                        request.invocation_request_id(),
                        request.offset(),
                        request.limit(),
                        request.deadline_unix_ms(),
                    )
                    .await
            }
            JobState::Succeeded | JobState::Failed | JobState::Canceled => Err(coordinator_error(
                "DTG-ANALYTICS-JOB-NO-RESULT",
                "analytics job has no result artifact",
            )),
        }
    }

    async fn cancel(
        &self,
        request: AnalyticsCancelRequest,
    ) -> Result<AnalyticsCancelResponse, ClusterAnalyticsError> {
        let record = self
            .authorized_job(
                request.invocation_request_id(),
                request.job_id(),
                request.security_fingerprint(),
                request.deadline_unix_ms(),
            )
            .await?;
        if record.state() == JobState::Canceled {
            return Ok(AnalyticsCancelResponse::new(true));
        }
        if record.state().is_terminal() {
            return Err(coordinator_error(
                "DTG-ANALYTICS-JOB-FINAL",
                "analytics job is already terminal",
            ));
        }
        let command_id = action_command_id(
            request.invocation_request_id(),
            request.job_id(),
            record.job_revision(),
            b"cancel",
        );
        let command = JobCommand::cancel(command_id, request.job_id(), record.job_revision())
            .map_err(ledger_error)?;
        let response = self.propose(command, request.deadline_unix_ms()).await?;
        let canonical = decode_canonical_job_id(&response.canonical_job_id)?;
        if canonical != request.job_id() {
            return Err(coordinator_error(
                "DTG-ANALYTICS-META-PROTOCOL",
                "Meta returned another canonical job for cancel",
            ));
        }
        Ok(AnalyticsCancelResponse::new(true))
    }

    async fn allocate_job_candidate(
        &self,
        deadline_unix_ms: u64,
    ) -> Result<(AnalyticsJobId, u64), ClusterAnalyticsError> {
        let allocation_request_id = self.next_request_id()?;
        let request = AllocateTimestampRequest {
            context: Some(self.request_context(allocation_request_id, deadline_unix_ms)?),
            count: 1,
            observed_physical_ms: unix_time_ms()?,
        };
        let mut last_error = None;
        for client in self.clients.iter() {
            let mut client = client.clone();
            let remaining = remaining_duration(deadline_unix_ms)?;
            match tokio::time::timeout(remaining, client.allocate_timestamp(request.clone())).await
            {
                Ok(Ok(response)) => {
                    let response = response.into_inner();
                    if response.count != 1 || response.first_physical_ms == 0 {
                        return Err(coordinator_error(
                            "DTG-ANALYTICS-META-PROTOCOL",
                            "Meta returned an invalid analytics timestamp allocation",
                        ));
                    }
                    let physical_micros = response
                        .first_physical_ms
                        .checked_mul(1_000)
                        .and_then(|value| i64::try_from(value).ok())
                        .ok_or_else(|| {
                            coordinator_error(
                                "DTG-ANALYTICS-META-PROTOCOL",
                                "Meta analytics timestamp overflowed",
                            )
                        })?;
                    let job_id = timestamp_job_candidate(TransactionTime::new(
                        physical_micros,
                        response.first_logical,
                    ))?;
                    return Ok((job_id, response.first_physical_ms));
                }
                Ok(Err(status)) => last_error = Some(status.to_string()),
                Err(_) => {
                    last_error = Some("Meta timestamp allocation timed out".into());
                }
            }
        }
        Err(meta_unavailable(last_error))
    }

    async fn propose(
        &self,
        command: JobCommand,
        deadline_unix_ms: u64,
    ) -> Result<cluster_protocol::proto::ProposeAnalyticsJobResponse, ClusterAnalyticsError> {
        let command_id = command.command_id();
        let request = ProposeAnalyticsJobRequest {
            context: Some(self.request_context(command_id, deadline_unix_ms)?),
            command: command.encode().map_err(ledger_error)?,
        };
        let mut last_error = None;
        for client in self.clients.iter() {
            let mut client = client.clone();
            let remaining = remaining_duration(deadline_unix_ms)?;
            match tokio::time::timeout(remaining, client.propose_analytics_job(request.clone()))
                .await
            {
                Ok(Ok(response)) => return Ok(response.into_inner()),
                Ok(Err(status)) => last_error = Some(status.to_string()),
                Err(_) => last_error = Some("Meta analytics proposal timed out".into()),
            }
        }
        Err(meta_unavailable(last_error))
    }

    async fn authorized_job(
        &self,
        invocation_request_id: u128,
        job_id: AnalyticsJobId,
        security_fingerprint: [u8; 32],
        deadline_unix_ms: u64,
    ) -> Result<JobRecord, ClusterAnalyticsError> {
        let request_id = read_request_id(invocation_request_id, job_id);
        let request = GetAnalyticsJobRequest {
            context: Some(self.request_context(request_id, deadline_unix_ms)?),
            job_id: job_id.value().to_be_bytes().to_vec(),
        };
        let mut last_error = None;
        for client in self.clients.iter() {
            let mut client = client.clone();
            let remaining = remaining_duration(deadline_unix_ms)?;
            match tokio::time::timeout(remaining, client.get_analytics_job(request.clone())).await {
                Ok(Ok(response)) => {
                    let response = response.into_inner();
                    if crc32fast::hash(&response.record) != response.checksum {
                        return Err(coordinator_error(
                            "DTG-ANALYTICS-META-PROTOCOL",
                            "Meta analytics job checksum is invalid",
                        ));
                    }
                    let (ledger_revision, record) =
                        LedgerState::decode_job(&response.record).map_err(ledger_error)?;
                    if ledger_revision != response.ledger_revision
                        || record.spec().job_id() != job_id
                    {
                        return Err(coordinator_error(
                            "DTG-ANALYTICS-META-PROTOCOL",
                            "Meta analytics job response is misbound",
                        ));
                    }
                    if record.spec().security_fingerprint() != security_fingerprint {
                        return Err(coordinator_error(
                            "DTG-ANALYTICS-JOB-FORBIDDEN",
                            "analytics job security fingerprint does not match",
                        ));
                    }
                    return Ok(record);
                }
                Ok(Err(status)) => {
                    if status.code() == tonic::Code::NotFound {
                        return Err(coordinator_error(
                            "DTG-ANALYTICS-JOB-NOT-FOUND",
                            "analytics job does not exist",
                        ));
                    }
                    last_error = Some(status.to_string());
                }
                Err(_) => last_error = Some("Meta analytics read timed out".into()),
            }
        }
        Err(meta_unavailable(last_error))
    }

    fn next_request_id(&self) -> Result<u128, ClusterAnalyticsError> {
        let previous = self
            .sequence
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                value.checked_add(1)
            })
            .map_err(|_| {
                coordinator_error(
                    "DTG-ANALYTICS-COORDINATOR-EXHAUSTED",
                    "analytics worker request identity space is exhausted",
                )
            })?;
        let sequence = previous + 1;
        Ok((u128::from(self.nonce) << 64) | u128::from(sequence))
    }

    fn request_context(
        &self,
        request_id: u128,
        deadline_unix_ms: u64,
    ) -> Result<RequestContext, ClusterAnalyticsError> {
        remaining_duration(deadline_unix_ms)?;
        Ok(RequestContext {
            protocol_version: CLUSTER_PROTOCOL_VERSION,
            cluster_id: self.cluster_id.to_vec(),
            request_id: request_id.to_be_bytes().to_vec(),
            deadline_unix_ms,
        })
    }
}

fn status_from_record(record: &JobRecord) -> Result<AnalyticsStatus, ClusterAnalyticsError> {
    let completed_units = record
        .checkpoint()
        .and_then(|manifest| match manifest.next_stage() {
            NextExecutionStage::Provider { completed_units } => Some(completed_units),
            NextExecutionStage::PublishResult | NextExecutionStage::Complete => None,
        })
        .unwrap_or(0);
    let failure = record
        .failure()
        .map(|(code, message)| bounded_text(format!("{code}: {message}"), MAX_FAILURE_BYTES));
    AnalyticsStatus::new(record.state(), completed_units, None, failure)
}

pub(crate) fn page_algorithm_result(
    result: AlgorithmResult,
    offset: usize,
    limit: usize,
) -> Result<AnalyticsResultPage, ClusterAnalyticsError> {
    let total_rows = u64::try_from(result.rows().len()).map_err(|_| {
        coordinator_error(
            "DTG-ANALYTICS-RESULT-BOUNDS",
            "analytics result row count exceeds the current bound",
        )
    })?;
    let start = offset.min(result.rows().len());
    let end = start.saturating_add(limit).min(result.rows().len());
    AnalyticsResultPage::new(
        result.columns().to_vec(),
        result.rows()[start..end].to_vec(),
        total_rows,
    )
}

pub(super) fn timestamp_job_candidate(
    timestamp: TransactionTime,
) -> Result<AnalyticsJobId, ClusterAnalyticsError> {
    let physical = u128::try_from(timestamp.physical_micros()).map_err(|_| {
        coordinator_error(
            "DTG-ANALYTICS-META-PROTOCOL",
            "Meta returned a non-positive analytics timestamp",
        )
    })?;
    let value = (physical << 32) | u128::from(timestamp.logical());
    AnalyticsJobId::new(value).map_err(ledger_error)
}

pub(super) fn decode_canonical_job_id(
    bytes: &[u8],
) -> Result<AnalyticsJobId, ClusterAnalyticsError> {
    let raw: [u8; 16] = bytes.try_into().map_err(|_| {
        coordinator_error(
            "DTG-ANALYTICS-META-PROTOCOL",
            "Meta canonical analytics job ID is not exactly 16 bytes",
        )
    })?;
    AnalyticsJobId::new(u128::from_be_bytes(raw)).map_err(ledger_error)
}

fn action_command_id(
    invocation_request_id: u128,
    job_id: AnalyticsJobId,
    job_revision: u64,
    operation: &[u8],
) -> u128 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"DTGProxy/Analytics/ActionCommand/V1");
    hasher.update(&invocation_request_id.to_be_bytes());
    hasher.update(&job_id.value().to_be_bytes());
    hasher.update(&job_revision.to_be_bytes());
    hasher.update(operation);
    nonzero_digest_id(hasher.finalize().as_bytes())
}

fn read_request_id(invocation_request_id: u128, job_id: AnalyticsJobId) -> u128 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"DTGProxy/Analytics/GetJob/V1");
    hasher.update(&invocation_request_id.to_be_bytes());
    hasher.update(&job_id.value().to_be_bytes());
    nonzero_digest_id(hasher.finalize().as_bytes())
}

fn nonzero_digest_id(digest: &[u8; 32]) -> u128 {
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&digest[..16]);
    u128::from_be_bytes(bytes).max(1)
}

fn worker_nonce(cluster_id: [u8; 16], endpoints: &[SocketAddr]) -> u64 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"DTGProxy/Analytics/WorkerNonce/V1");
    hasher.update(&cluster_id);
    hasher.update(
        &SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
            .to_be_bytes(),
    );
    hasher.update(
        &NEXT_WORKER_NONCE
            .fetch_add(1, Ordering::Relaxed)
            .to_be_bytes(),
    );
    for endpoint in endpoints {
        hasher.update(endpoint.to_string().as_bytes());
    }
    let mut bytes = [0; 8];
    bytes.copy_from_slice(&hasher.finalize().as_bytes()[..8]);
    u64::from_be_bytes(bytes).max(1)
}

fn remaining_duration(deadline_unix_ms: u64) -> Result<Duration, ClusterAnalyticsError> {
    let remaining = deadline_unix_ms
        .checked_sub(unix_time_ms()?)
        .ok_or_else(|| {
            coordinator_error(
                "DTG-ANALYTICS-DEADLINE",
                "analytics operation deadline has expired",
            )
        })?;
    if remaining == 0 {
        return Err(coordinator_error(
            "DTG-ANALYTICS-DEADLINE",
            "analytics operation deadline has expired",
        ));
    }
    Ok(Duration::from_millis(remaining))
}

fn unix_time_ms() -> Result<u64, ClusterAnalyticsError> {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| {
                coordinator_error(
                    "DTG-ANALYTICS-CLOCK",
                    "system clock is before the Unix epoch",
                )
            })?
            .as_millis(),
    )
    .map_err(|_| coordinator_error("DTG-ANALYTICS-CLOCK", "system clock overflowed"))
}

fn bounded_text(mut value: String, maximum: usize) -> String {
    if value.len() <= maximum {
        return value;
    }
    let boundary = (0..=maximum)
        .rev()
        .find(|index| value.is_char_boundary(*index))
        .unwrap_or(0);
    value.truncate(boundary);
    value
}

fn lock<'a, T>(
    mutex: &'a Mutex<T>,
    name: &str,
) -> Result<MutexGuard<'a, T>, ClusterAnalyticsError> {
    mutex.lock().map_err(|_| {
        coordinator_error(
            "DTG-ANALYTICS-COORDINATOR-POISONED",
            format!("{name} lock is poisoned"),
        )
    })
}

fn ledger_error(error: analytics_ledger::JobError) -> ClusterAnalyticsError {
    coordinator_error("DTG-ANALYTICS-LEDGER", error.to_string())
}

fn meta_unavailable(last_error: Option<String>) -> ClusterAnalyticsError {
    coordinator_error(
        "DTG-ANALYTICS-META-UNAVAILABLE",
        last_error.unwrap_or_else(|| "no Meta endpoint was reachable".into()),
    )
}

fn internal_result_error() -> ClusterAnalyticsError {
    coordinator_error(
        "DTG-ANALYTICS-COORDINATOR-PROTOCOL",
        "analytics worker returned the wrong response kind",
    )
}

fn coordinator_error(code: &'static str, message: impl Into<String>) -> ClusterAnalyticsError {
    ClusterAnalyticsError::new(code, message)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::time::Duration;

    use analytics_api::{AlgorithmResult, AlgorithmValue, ProjectedGraph, SnapshotGraph, VertexId};
    use analytics_ledger::{GraphProjectionScope, ProjectionLimits};
    use analytics_runtime::BuiltInProvider;
    use cluster_protocol::CLUSTER_PROTOCOL_VERSION;
    use cluster_protocol::proto::meta_service_server::{MetaService, MetaServiceServer};
    use cluster_protocol::proto::{ProposeRequest, RequestContext};
    use control_plane::{
        BackendProfile, CatalogCommand, DeploymentMode, GraphDefinition, Placement,
        TopologyDefinition,
    };
    use meta_node::{MetaNodeService, MetaRaftReplica, ReplicatedTso};
    use procedure_runtime::{
        ClusterAnalyticsCoordinator, JobInvocationContext, ProcedureAccess, ProcedureInvocation,
        ProcedureRegistry, ProcedureValue,
    };
    use storage_api::AdapterRequirement;
    use temporal_types::{TransactionTime, ValidTime};
    use timestamp_oracle::ManualClock;
    use tokio::sync::{mpsc, oneshot};
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::Request;
    use tonic::transport::Server;

    use super::{MetaClusterAnalyticsCoordinator, MetaWorker, Probe, unix_time_ms};

    const CLUSTER_ID: [u8; 16] = [0x8a; 16];

    #[tokio::test(flavor = "current_thread")]
    async fn stalled_request_does_not_head_of_line_block_an_independent_request() {
        let (sender, receiver) = mpsc::channel(2);
        let worker = tokio::spawn(MetaWorker::run_probes(receiver, 2));
        let (slow_sender, slow_receiver) = oneshot::channel();
        let (fast_sender, fast_receiver) = oneshot::channel();
        sender
            .send(Probe {
                delay: Duration::from_millis(200),
                value: 1,
                response: slow_sender,
            })
            .await
            .unwrap();
        sender
            .send(Probe {
                delay: Duration::from_millis(1),
                value: 2,
                response: fast_sender,
            })
            .await
            .unwrap();
        drop(sender);

        assert_eq!(
            tokio::time::timeout(Duration::from_millis(50), fast_receiver)
                .await
                .unwrap()
                .unwrap(),
            2,
        );
        assert_eq!(slow_receiver.await.unwrap(), 1);
        worker.await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn meta_only_job_control_is_canonical_cross_gateway_and_fail_closed() {
        let temporary = tempfile::tempdir().unwrap();
        let meta = elected_meta(temporary.path());
        meta.propose(Request::new(ProposeRequest {
            context: Some(meta_context(101)),
            command: CatalogCommand::create_graph(101, 0, graph_definition())
                .encode()
                .unwrap(),
        }))
        .await
        .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = listener.local_addr().unwrap();
        let (shutdown, shutdown_receiver) = oneshot::channel();
        let server = tokio::spawn(
            Server::builder()
                .add_service(MetaServiceServer::new(meta))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    let _ = shutdown_receiver.await;
                }),
        );

        let first_gateway = registry(endpoint);
        let arguments = BTreeMap::from([
            (
                "algorithm".into(),
                ProcedureValue::String("dtg.graph.degree".into()),
            ),
            ("parameters".into(), ProcedureValue::Map(BTreeMap::new())),
        ]);
        let first = invoke(
            &first_gateway,
            "dtg.analytics.submit",
            arguments.clone(),
            501,
            [37; 32],
            Some(projected_graph()),
        )
        .await
        .unwrap();
        let retry = invoke(
            &first_gateway,
            "dtg.analytics.submit",
            arguments,
            501,
            [37; 32],
            Some(projected_graph()),
        )
        .await
        .unwrap();
        let ProcedureValue::String(job_id) = &first.rows()[0][0] else {
            panic!("submit must return a string job ID")
        };
        assert_eq!(&retry.rows()[0][0], &ProcedureValue::String(job_id.clone()));
        assert_eq!(job_id.len(), 32);

        let other_gateway = registry(endpoint);
        let job_argument =
            BTreeMap::from([("jobId".into(), ProcedureValue::String(job_id.clone()))]);
        let status = invoke(
            &other_gateway,
            "dtg.analytics.status",
            job_argument.clone(),
            502,
            [37; 32],
            None,
        )
        .await
        .unwrap();
        assert_eq!(status.rows()[0][0], ProcedureValue::String("QUEUED".into()));

        let not_ready = invoke(
            &other_gateway,
            "dtg.analytics.results",
            job_argument.clone(),
            503,
            [37; 32],
            None,
        )
        .await
        .unwrap_err();
        assert!(
            not_ready
                .to_string()
                .contains("DTG-ANALYTICS-JOB-NOT-READY")
        );

        let forbidden = invoke(
            &other_gateway,
            "dtg.analytics.status",
            job_argument.clone(),
            504,
            [38; 32],
            None,
        )
        .await
        .unwrap_err();
        assert!(
            forbidden
                .to_string()
                .contains("DTG-ANALYTICS-JOB-FORBIDDEN")
        );

        let canceled = invoke(
            &other_gateway,
            "dtg.analytics.cancel",
            job_argument.clone(),
            505,
            [37; 32],
            None,
        )
        .await
        .unwrap();
        assert_eq!(canceled.rows()[0][0], ProcedureValue::Boolean(true));
        let no_result = invoke(
            &other_gateway,
            "dtg.analytics.results",
            job_argument,
            506,
            [37; 32],
            None,
        )
        .await
        .unwrap_err();
        assert!(
            no_result
                .to_string()
                .contains("DTG-ANALYTICS-JOB-NO-RESULT")
        );

        drop(other_gateway);
        drop(first_gateway);
        shutdown.send(()).unwrap();
        server.await.unwrap().unwrap();
    }

    fn registry(endpoint: SocketAddr) -> ProcedureRegistry {
        let coordinator: Arc<dyn ClusterAnalyticsCoordinator> =
            Arc::new(MetaClusterAnalyticsCoordinator::new(CLUSTER_ID, vec![endpoint], 8).unwrap());
        ProcedureRegistry::builtin_analytics(Arc::new(BuiltInProvider::new()), coordinator).unwrap()
    }

    async fn invoke(
        registry: &ProcedureRegistry,
        name: &str,
        arguments: BTreeMap<String, ProcedureValue>,
        outer_request_id: u128,
        security_fingerprint: [u8; 32],
        graph: Option<Arc<ProjectedGraph>>,
    ) -> Result<procedure_runtime::ProcedureResult, procedure_runtime::ProcedureError> {
        let identity = *registry.catalog().resolve(name).unwrap().identity();
        registry
            .invoke(
                ProcedureInvocation::new(
                    identity,
                    arguments,
                    graph,
                    security_fingerprint,
                    &ProcedureAccess::analytics_read(),
                )
                .with_job_context(job_context(outer_request_id)),
            )
            .await
    }

    fn job_context(outer_request_id: u128) -> JobInvocationContext {
        JobInvocationContext::new(
            outer_request_id,
            unix_time_ms().unwrap() + 60_000,
            7,
            1,
            1,
            1,
            1,
            TransactionTime::new(23_000, 29),
            GraphProjectionScope::Snapshot {
                valid_time: ValidTime::from_micros(31),
            },
            ProjectionLimits::new(100, 200, 1 << 20).unwrap(),
        )
        .unwrap()
    }

    fn projected_graph() -> Arc<ProjectedGraph> {
        Arc::new(ProjectedGraph::Snapshot(
            SnapshotGraph::new(vec![VertexId::new(1)], Vec::new(), true).unwrap(),
        ))
    }

    fn graph_definition() -> GraphDefinition {
        GraphDefinition::new(
            7,
            "analytics",
            1,
            TopologyDefinition::new(
                DeploymentMode::PrimaryReplica,
                1,
                16,
                1,
                vec![Placement::new(1, 1, vec![1]).unwrap()],
            )
            .unwrap(),
            BackendProfile::new(
                "rocksdb",
                BTreeMap::new(),
                BTreeMap::new(),
                AdapterRequirement::HotPluggableReplica,
                1,
            )
            .unwrap(),
        )
        .unwrap()
    }

    fn elected_meta(root: &std::path::Path) -> MetaNodeService {
        let mut replica =
            MetaRaftReplica::open(1, &[1], root.join("raft"), root.join("state")).unwrap();
        replica.campaign().unwrap();
        for _ in 0..32 {
            assert!(replica.drain_ready().unwrap().is_empty());
            if replica.is_leader() {
                break;
            }
            replica.tick();
        }
        assert!(replica.is_leader());
        MetaNodeService::new(
            CLUSTER_ID,
            Arc::new(tokio::sync::Mutex::new(replica)),
            Arc::new(
                ReplicatedTso::new(Arc::new(ManualClock::new(1_000_000)), 32, 1_000_000).unwrap(),
            ),
        )
    }

    fn meta_context(request_id: u128) -> RequestContext {
        RequestContext {
            protocol_version: CLUSTER_PROTOCOL_VERSION,
            cluster_id: CLUSTER_ID.to_vec(),
            request_id: request_id.to_be_bytes().to_vec(),
            deadline_unix_ms: unix_time_ms().unwrap() + 60_000,
        }
    }

    #[test]
    fn result_pages_are_deterministic_and_report_total_rows() {
        let result = AlgorithmResult::new(
            vec!["vertex".into(), "degree".into()],
            vec![
                vec![
                    AlgorithmValue::Vertex(VertexId::new(1)),
                    AlgorithmValue::Integer(2),
                ],
                vec![
                    AlgorithmValue::Vertex(VertexId::new(2)),
                    AlgorithmValue::Integer(1),
                ],
                vec![
                    AlgorithmValue::Vertex(VertexId::new(3)),
                    AlgorithmValue::Integer(0),
                ],
            ],
            BTreeMap::new(),
        )
        .unwrap();
        let page = super::page_algorithm_result(result, 1, 1).unwrap();
        assert_eq!(page.columns(), &["vertex", "degree"]);
        assert_eq!(page.total_rows(), 3);
        assert_eq!(
            page.rows(),
            &[vec![
                AlgorithmValue::Vertex(VertexId::new(2)),
                AlgorithmValue::Integer(1)
            ]]
        );
    }
}
