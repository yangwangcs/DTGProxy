use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use analytics_api::{
    AlgorithmRequest, AlgorithmResult, AnalyticsProvider, CancellationSignal,
    PartitionedSnapshotGraph, ProjectedGraph, ProviderError, SnapshotPartition,
};
use analytics_ledger::{
    AnalyticsJobId, ArtifactGeneration, ArtifactKind, ArtifactManifest, JobCommand, JobRecord,
    JobState, JobTombstone, LedgerState, MAX_ARTIFACT_CHUNK_BYTES, NextExecutionStage,
    ProviderCheckpointArtifactV1, RetentionPolicy, canonical_applied_indexes_digest,
    decode_algorithm_parameters, decode_algorithm_result_artifact,
    decode_provider_checkpoint_artifact, encode_algorithm_result_artifact,
    encode_provider_checkpoint_artifact,
};
use analytics_runtime::{
    ProjectionError, ProjectionLimits, project_snapshot_identity_part_bounded,
};
use cluster_protocol::CLUSTER_PROTOCOL_VERSION;
use cluster_protocol::proto::meta_service_client::MetaServiceClient;
use cluster_protocol::proto::{
    AcquireAnalyticsGcLeaseRequest, GetAnalyticsJobRequest, ListAnalyticsJobTombstonesRequest,
    ListAnalyticsJobsRequest, ListClaimableAnalyticsJobsRequest, ProposeAnalyticsJobRequest,
    RequestContext,
};
use dtgproxy::ShardPlacement;
use procedure_runtime::ClusterAnalyticsError;
use shard_client::{
    AdvanceArtifactFenceRequest, ArtifactGenerationCursor, ArtifactKind as ShardArtifactKind,
    DeleteArtifactGenerationRequest, GetArtifactGenerationRequest, ListArtifactGenerationsRequest,
    PinArtifactGenerationRequest, PutArtifactChunkRequest, ShardClient, ShardClientError,
    ShardClientStorageAdapter, ShardRequestContext,
};
use temporal_storage::TemporalStore;
use tokio::sync::{mpsc, watch};
use tokio_stream::StreamExt;
use tonic::transport::{Channel, Endpoint};

use crate::service::GatewayRoutingState;

const DEFAULT_POLL_INTERVAL: Duration = Duration::from_millis(100);
const DEFAULT_LEASE_DURATION: Duration = Duration::from_secs(5);
const META_ENDPOINT_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(1);
const DEFAULT_CLAIM_LIMIT: u32 = 16;
const MAINTENANCE_TICK_INTERVAL: u64 = 100;
const DEFAULT_RETENTION_MAX_GENERATIONS: usize = 2;
const DEFAULT_RETENTION_MAX_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_RETENTION_TTL_MS: u64 = 60 * 60 * 1_000;
const ARTIFACT_GENERATION_LIST_LIMIT: u32 = 4_096;
const GC_LEASE_RENEW_MARGIN_MS: u64 = 2_000;
static NEXT_SCHEDULER_REQUEST_NONCE: AtomicU64 = AtomicU64::new(1);
pub const ANALYTICS_PROCESS_STOP_FAULT_CODE: &str = "DTG-ANALYTICS-FAULT-PROCESS-STOP";
const ANALYTICS_RETRYABLE_INFRASTRUCTURE_CODE: &str = "DTG-ANALYTICS-RETRYABLE-INFRASTRUCTURE";

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum AnalyticsFaultPoint {
    Claim,
    Begin,
    LeaseRenew,
    ExecutionSlice,
    CheckpointUpload,
    CheckpointPin,
    CheckpointCas,
    ResultUpload,
    ResultPin,
    Publish,
    GcAfterFenceAdvance,
    GcBeforeDelete,
    GcBeforeAcknowledgement,
}

pub trait AnalyticsFaultInjector: Send + Sync {
    fn check(&self, point: AnalyticsFaultPoint) -> Result<(), ClusterAnalyticsError>;
}

struct NoopFaultInjector;

impl AnalyticsFaultInjector for NoopFaultInjector {
    fn check(&self, _point: AnalyticsFaultPoint) -> Result<(), ClusterAnalyticsError> {
        Ok(())
    }
}

pub(crate) struct AnalyticsScheduler {
    shutdown: watch::Sender<bool>,
    worker: Mutex<Option<JoinHandle<()>>>,
    metrics: Arc<AnalyticsSchedulerMetrics>,
}

/// Monotonic process-local counters for scheduler maintenance and recovery.
#[derive(Debug, Default)]
pub struct AnalyticsSchedulerMetrics {
    maintenance_runs: AtomicU64,
    maintenance_failures: AtomicU64,
    orphan_discovered: AtomicU64,
    orphan_protected_current: AtomicU64,
    orphan_skipped_ttl: AtomicU64,
    orphan_skipped_pinned_latest: AtomicU64,
    orphan_deleted: AtomicU64,
    orphan_delete_failures: AtomicU64,
    orphan_bytes_reclaimed: AtomicU64,
    resumed_from_checkpoint: AtomicU64,
    gc_lease_grants: AtomicU64,
    gc_lease_renewals: AtomicU64,
    gc_lease_conflicts: AtomicU64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AnalyticsSchedulerMetricsSnapshot {
    pub maintenance_runs: u64,
    pub maintenance_failures: u64,
    pub orphan_discovered: u64,
    pub orphan_protected_current: u64,
    pub orphan_skipped_ttl: u64,
    pub orphan_skipped_pinned_latest: u64,
    pub orphan_deleted: u64,
    pub orphan_delete_failures: u64,
    pub orphan_bytes_reclaimed: u64,
    pub resumed_from_checkpoint: u64,
    pub gc_lease_grants: u64,
    pub gc_lease_renewals: u64,
    pub gc_lease_conflicts: u64,
}

impl AnalyticsSchedulerMetrics {
    fn snapshot(&self) -> AnalyticsSchedulerMetricsSnapshot {
        AnalyticsSchedulerMetricsSnapshot {
            maintenance_runs: self.maintenance_runs.load(Ordering::Relaxed),
            maintenance_failures: self.maintenance_failures.load(Ordering::Relaxed),
            orphan_discovered: self.orphan_discovered.load(Ordering::Relaxed),
            orphan_protected_current: self.orphan_protected_current.load(Ordering::Relaxed),
            orphan_skipped_ttl: self.orphan_skipped_ttl.load(Ordering::Relaxed),
            orphan_skipped_pinned_latest: self.orphan_skipped_pinned_latest.load(Ordering::Relaxed),
            orphan_deleted: self.orphan_deleted.load(Ordering::Relaxed),
            orphan_delete_failures: self.orphan_delete_failures.load(Ordering::Relaxed),
            orphan_bytes_reclaimed: self.orphan_bytes_reclaimed.load(Ordering::Relaxed),
            resumed_from_checkpoint: self.resumed_from_checkpoint.load(Ordering::Relaxed),
            gc_lease_grants: self.gc_lease_grants.load(Ordering::Relaxed),
            gc_lease_renewals: self.gc_lease_renewals.load(Ordering::Relaxed),
            gc_lease_conflicts: self.gc_lease_conflicts.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct AnalyticsGcLease {
    epoch: u64,
    expires_unix_ms: u64,
}

impl AnalyticsGcLease {
    fn requires_renewal(self, now_unix_ms: u64) -> bool {
        self.expires_unix_ms <= now_unix_ms.saturating_add(GC_LEASE_RENEW_MARGIN_MS)
    }

    fn request_deadline(self, now_unix_ms: u64) -> Result<u64, ClusterAnalyticsError> {
        let deadline = self.expires_unix_ms.min(now_unix_ms.saturating_add(10_000));
        if deadline <= now_unix_ms {
            return Err(scheduler_error(
                "DTG-ANALYTICS-GC-LEASE",
                "analytics GC lease expired before the Shard request",
            ));
        }
        Ok(deadline)
    }
}

fn apply_gc_lease_renewal(
    lease: &mut AnalyticsGcLease,
    renewed: AnalyticsGcLease,
) -> Result<(), ClusterAnalyticsError> {
    if renewed.epoch < lease.epoch {
        return Err(scheduler_error(
            "DTG-ANALYTICS-GC-LEASE",
            "Meta returned a regressed analytics GC epoch",
        ));
    }
    *lease = renewed;
    Ok(())
}

impl AnalyticsScheduler {
    pub fn metrics(&self) -> AnalyticsSchedulerMetricsSnapshot {
        self.metrics.snapshot()
    }
}

impl AnalyticsScheduler {
    pub(crate) fn spawn_with_delay(
        cluster_id: [u8; 16],
        gateway_id: u64,
        meta_endpoints: Vec<SocketAddr>,
        shard_client: Arc<dyn ShardClient>,
        routing: Arc<RwLock<GatewayRoutingState>>,
        provider: Arc<dyn AnalyticsProvider>,
        execution_delay: Duration,
    ) -> Result<Self, ClusterAnalyticsError> {
        Self::spawn_with_delay_and_fault_injector(
            cluster_id,
            gateway_id,
            meta_endpoints,
            shard_client,
            routing,
            provider,
            execution_delay,
            Arc::new(NoopFaultInjector),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn spawn_with_delay_and_fault_injector(
        cluster_id: [u8; 16],
        gateway_id: u64,
        meta_endpoints: Vec<SocketAddr>,
        shard_client: Arc<dyn ShardClient>,
        routing: Arc<RwLock<GatewayRoutingState>>,
        provider: Arc<dyn AnalyticsProvider>,
        execution_delay: Duration,
        fault_injector: Arc<dyn AnalyticsFaultInjector>,
    ) -> Result<Self, ClusterAnalyticsError> {
        if cluster_id == [0; 16] || gateway_id == 0 || meta_endpoints.is_empty() {
            return Err(scheduler_error(
                "DTG-ANALYTICS-SCHEDULER-CONFIG",
                "analytics scheduler configuration is invalid",
            ));
        }
        let (shutdown, shutdown_receiver) = watch::channel(false);
        let metrics = Arc::new(AnalyticsSchedulerMetrics::default());
        let worker_metrics = Arc::clone(&metrics);
        let (ready_sender, ready_receiver) = std::sync::mpsc::sync_channel(1);
        let worker = thread::Builder::new()
            .name(format!("dtg-analytics-scheduler-{gateway_id}"))
            .spawn(move || {
                let runtime = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(runtime) => runtime,
                    Err(error) => {
                        let _ = ready_sender.send(Err(error.to_string()));
                        return;
                    }
                };
                let worker = match SchedulerWorker::new_with_fault_injector(
                    cluster_id,
                    gateway_id,
                    meta_endpoints,
                    shard_client,
                    routing,
                    provider,
                    execution_delay,
                    fault_injector,
                    worker_metrics,
                    &runtime,
                ) {
                    Ok(worker) => worker,
                    Err(error) => {
                        let _ = ready_sender.send(Err(error.to_string()));
                        return;
                    }
                };
                let _ = ready_sender.send(Ok(()));
                runtime.block_on(worker.run(shutdown_receiver));
            })
            .map_err(|error| scheduler_error("DTG-ANALYTICS-SCHEDULER-START", error.to_string()))?;
        match ready_receiver.recv_timeout(Duration::from_secs(5)) {
            Ok(Ok(())) => Ok(Self {
                shutdown,
                worker: Mutex::new(Some(worker)),
                metrics,
            }),
            Ok(Err(error)) => {
                let _ = worker.join();
                Err(scheduler_error("DTG-ANALYTICS-SCHEDULER-START", error))
            }
            Err(_) => Err(scheduler_error(
                "DTG-ANALYTICS-SCHEDULER-START",
                "analytics scheduler did not start before its bound",
            )),
        }
    }
}

impl Drop for AnalyticsScheduler {
    fn drop(&mut self) {
        let _ = self.shutdown.send(true);
        if let Ok(worker) = self.worker.get_mut()
            && let Some(worker) = worker.take()
        {
            let _ = worker.join();
        }
    }
}

struct SchedulerWorker {
    cluster_id: [u8; 16],
    gateway_id: u64,
    clients: Vec<MetaServiceClient<Channel>>,
    shard_client: Arc<dyn ShardClient>,
    routing: Arc<RwLock<GatewayRoutingState>>,
    provider: Arc<dyn AnalyticsProvider>,
    execution_delay: Duration,
    request_nonce: u64,
    sequence: AtomicU64,
    preferred_meta_index: AtomicU64,
    maintenance_ticks: AtomicU64,
    fault_injector: Arc<dyn AnalyticsFaultInjector>,
    metrics: Arc<AnalyticsSchedulerMetrics>,
}

impl SchedulerWorker {
    #[allow(clippy::too_many_arguments)]
    fn new_with_fault_injector(
        cluster_id: [u8; 16],
        gateway_id: u64,
        meta_endpoints: Vec<SocketAddr>,
        shard_client: Arc<dyn ShardClient>,
        routing: Arc<RwLock<GatewayRoutingState>>,
        provider: Arc<dyn AnalyticsProvider>,
        execution_delay: Duration,
        fault_injector: Arc<dyn AnalyticsFaultInjector>,
        metrics: Arc<AnalyticsSchedulerMetrics>,
        runtime: &tokio::runtime::Runtime,
    ) -> Result<Self, ClusterAnalyticsError> {
        let _guard = runtime.enter();
        let clients = meta_endpoints
            .into_iter()
            .map(|endpoint| {
                Endpoint::from_shared(format!("http://{endpoint}"))
                    .map(|endpoint| MetaServiceClient::new(endpoint.connect_lazy()))
                    .map_err(|error| {
                        scheduler_error("DTG-ANALYTICS-SCHEDULER-CONFIG", error.to_string())
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            cluster_id,
            gateway_id,
            clients,
            shard_client,
            routing,
            provider,
            execution_delay,
            request_nonce: scheduler_request_nonce(cluster_id, gateway_id),
            sequence: AtomicU64::new(1),
            preferred_meta_index: AtomicU64::new(0),
            maintenance_ticks: AtomicU64::new(0),
            fault_injector,
            metrics,
        })
    }

    fn inject(&self, point: AnalyticsFaultPoint) -> Result<(), ClusterAnalyticsError> {
        self.fault_injector.check(point)
    }

    fn meta_client_indexes(&self) -> impl Iterator<Item = usize> + '_ {
        let start = usize::try_from(self.preferred_meta_index.load(Ordering::Relaxed)).unwrap_or(0)
            % self.clients.len();
        (0..self.clients.len()).map(move |offset| (start + offset) % self.clients.len())
    }

    fn remember_meta_client(&self, index: usize) {
        self.preferred_meta_index.store(
            u64::try_from(index).expect("Meta client index fits u64"),
            Ordering::Relaxed,
        );
    }

    async fn run(self, mut shutdown: watch::Receiver<bool>) {
        let mut ticker = tokio::time::interval(DEFAULT_POLL_INTERVAL);
        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() { return; }
                }
                _ = ticker.tick() => {
                    if let Err(error) = self.poll_once().await {
                        eprintln!("dtgproxy analytics scheduler poll failed: {error}");
                    }
                }
            }
        }
    }

    async fn poll_once(&self) -> Result<(), ClusterAnalyticsError> {
        let now = unix_time_ms()?;
        let candidates = self.list_claimable(now).await?;
        for candidate in candidates {
            if let Err(error) = self.execute_candidate(candidate, now).await
                && !is_process_stop_fault(&error)
                && !is_retryable_infrastructure_error(&error)
            {
                self.fail_owned_running_job(candidate.job_id(), error).await;
            }
        }
        let maintenance_tick = self
            .maintenance_ticks
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        if maintenance_tick.is_multiple_of(MAINTENANCE_TICK_INTERVAL) {
            self.metrics
                .maintenance_runs
                .fetch_add(1, Ordering::Relaxed);
            if let Err(error) = self.maintain_artifacts(now).await {
                self.metrics
                    .maintenance_failures
                    .fetch_add(1, Ordering::Relaxed);
                return Err(error);
            }
        }
        Ok(())
    }

    async fn maintain_artifacts(&self, now_unix_ms: u64) -> Result<(), ClusterAnalyticsError> {
        let mut gc_lease = self.acquire_gc_lease().await?;
        if gc_lease.expires_unix_ms <= now_unix_ms {
            return Err(scheduler_error(
                "DTG-ANALYTICS-GC-LEASE",
                "analytics GC lease expired before maintenance started",
            ));
        }
        let policy = RetentionPolicy::new(
            DEFAULT_RETENTION_MAX_GENERATIONS,
            DEFAULT_RETENTION_MAX_BYTES,
            DEFAULT_RETENTION_TTL_MS,
            DEFAULT_RETENTION_TTL_MS,
        )
        .map_err(|error| scheduler_error("DTG-ANALYTICS-RETENTION", error.to_string()))?;
        let jobs = self.list_jobs(&mut gc_lease).await?;
        let tombstones = self.list_tombstones(&mut gc_lease).await?;
        for (job_id, record) in &jobs {
            for (kind, manifest) in [
                (ArtifactKind::Checkpoint, record.checkpoint()),
                (ArtifactKind::Result, record.result()),
            ] {
                let Some(manifest) = manifest else { continue };
                let placement = {
                    let routing = self.routing.read().map_err(|_| {
                        scheduler_error("DTG-ANALYTICS-ROUTING", "routing state lock is poisoned")
                    })?;
                    routing
                        .deployment
                        .all_shards()
                        .iter()
                        .find(|placement| placement.shard_id() == manifest.storage_shard_id())
                        .cloned()
                        .ok_or_else(|| {
                            scheduler_error(
                                "DTG-ANALYTICS-RETENTION",
                                "artifact storage Shard is absent",
                            )
                        })?
                };
                let context = ShardRequestContext::new(
                    record.spec().graph_id(),
                    placement.shard_id(),
                    placement.placement_epoch(),
                    self.next_request_id()?,
                    {
                        self.ensure_gc_lease(&mut gc_lease, false).await?;
                        gc_lease.request_deadline(unix_time_ms()?)?
                    },
                )
                .map_err(|error| scheduler_error("DTG-ANALYTICS-RETENTION", error.to_string()))?;
                let summaries = self
                    .shard_client
                    .list_artifact_generations(
                        ListArtifactGenerationsRequest::new(
                            context,
                            job_id.value(),
                            match kind {
                                ArtifactKind::Checkpoint => ShardArtifactKind::Checkpoint,
                                ArtifactKind::Result => ShardArtifactKind::Result,
                            },
                            ARTIFACT_GENERATION_LIST_LIMIT,
                        )
                        .map_err(|error| {
                            scheduler_error("DTG-ANALYTICS-RETENTION", error.to_string())
                        })?,
                    )
                    .await
                    .map_err(|error| {
                        scheduler_error("DTG-ANALYTICS-RETENTION", error.to_string())
                    })?;
                let observed = summaries
                    .iter()
                    .map(|summary| {
                        retention_observation(
                            kind,
                            summary.generation(),
                            summary.expected_total_bytes(),
                            summary.created_at_unix_ms(),
                            summary.pinned(),
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let plan = policy
                    .plan(record, &observed, now_unix_ms)
                    .map_err(|error| {
                        scheduler_error("DTG-ANALYTICS-RETENTION", error.to_string())
                    })?;
                for key in plan.deletable() {
                    let fresh = self.get_record(*job_id).await?;
                    let fresh_manifest = match kind {
                        ArtifactKind::Checkpoint => fresh.checkpoint(),
                        ArtifactKind::Result => fresh.result(),
                    };
                    if !gc_delete_fence_allows(
                        record.job_revision(),
                        manifest,
                        fresh.job_revision(),
                        fresh_manifest,
                        key.generation(),
                    ) {
                        break;
                    }
                    self.ensure_gc_lease(&mut gc_lease, true).await?;
                    let request_now = unix_time_ms()?;
                    let request = DeleteArtifactGenerationRequest::new_with_gc_epoch(
                        ShardRequestContext::new(
                            record.spec().graph_id(),
                            placement.shard_id(),
                            placement.placement_epoch(),
                            self.next_request_id()?,
                            gc_lease.request_deadline(request_now)?,
                        )
                        .map_err(|error| {
                            scheduler_error("DTG-ANALYTICS-RETENTION", error.to_string())
                        })?,
                        job_id.value(),
                        match key.kind() {
                            ArtifactKind::Checkpoint => ShardArtifactKind::Checkpoint,
                            ArtifactKind::Result => ShardArtifactKind::Result,
                        },
                        key.generation(),
                        gc_lease.epoch,
                    )
                    .map_err(|error| {
                        scheduler_error("DTG-ANALYTICS-RETENTION", error.to_string())
                    })?;
                    self.shard_client
                        .delete_artifact_generation(request)
                        .await
                        .map_err(|error| {
                            scheduler_error("DTG-ANALYTICS-RETENTION", error.to_string())
                        })?;
                }
            }
        }
        self.maintain_orphan_artifacts(
            &jobs,
            &tombstones,
            now_unix_ms,
            DEFAULT_RETENTION_TTL_MS,
            &mut gc_lease,
        )
        .await?;
        Ok(())
    }

    async fn maintain_orphan_artifacts(
        &self,
        jobs: &[(AnalyticsJobId, JobRecord)],
        tombstones: &[(AnalyticsJobId, JobTombstone)],
        now_unix_ms: u64,
        orphan_ttl_ms: u64,
        gc_lease: &mut AnalyticsGcLease,
    ) -> Result<(), ClusterAnalyticsError> {
        let graph_id = self.current_graph_id()?;
        let placements = self
            .routing
            .read()
            .map_err(|_| {
                scheduler_error("DTG-ANALYTICS-ROUTING", "routing state lock is poisoned")
            })?
            .deployment
            .all_shards()
            .to_vec();
        let mut protections = jobs
            .iter()
            .map(|(job_id, record)| (job_id.value(), MetaArtifactProtection::from_record(record)))
            .collect::<BTreeMap<_, _>>();
        for (job_id, tombstone) in tombstones {
            insert_tombstone_protection(
                &mut protections,
                job_id.value(),
                MetaArtifactProtection::from_tombstone(tombstone),
            );
        }
        let placements_by_id = placements
            .iter()
            .map(|placement| (placement.shard_id(), placement.clone()))
            .collect::<BTreeMap<_, _>>();
        if placements_by_id.len() != placements.len() {
            return Err(scheduler_error(
                "DTG-ANALYTICS-ARTIFACT-GC",
                "analytics GC deployment contains duplicate Shard identities",
            ));
        }

        let mut observations = Vec::new();
        for placement in &placements {
            let mut guard = GlobalHeadPageGuard::default();
            loop {
                self.ensure_gc_lease(gc_lease, false).await?;
                let request_now = unix_time_ms()?;
                let context = ShardRequestContext::new(
                    graph_id,
                    placement.shard_id(),
                    placement.placement_epoch(),
                    self.next_request_id()?,
                    gc_lease.request_deadline(request_now)?,
                )
                .map_err(|error| scheduler_error("DTG-ANALYTICS-ARTIFACT-GC", error.to_string()))?;
                let page = self
                    .shard_client
                    .list_artifact_generation_heads(
                        shard_client::ListArtifactGenerationHeadsRequest::new(
                            context,
                            guard.after,
                            ARTIFACT_GENERATION_LIST_LIMIT,
                        )
                        .map_err(|error| {
                            scheduler_error("DTG-ANALYTICS-ARTIFACT-GC", error.to_string())
                        })?,
                    )
                    .await
                    .map_err(|error| {
                        scheduler_error("DTG-ANALYTICS-ARTIFACT-GC", error.to_string())
                    })?;
                let continue_scan = guard.observe(
                    page.generations()
                        .iter()
                        .map(shard_client::ArtifactGenerationSummary::cursor),
                    page.next(),
                    usize::try_from(ARTIFACT_GENERATION_LIST_LIMIT)
                        .expect("bounded artifact head page limit"),
                )?;
                observations.extend(
                    page.generations()
                        .iter()
                        .map(|summary| {
                            OrphanArtifactObservation::from_summary(placement.shard_id(), summary)
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                );
                if !continue_scan {
                    break;
                }
            }
        }

        let plan = plan_orphan_artifacts(&observations, &protections, now_unix_ms, orphan_ttl_ms)?;
        self.metrics.orphan_discovered.fetch_add(
            u64::try_from(observations.len()).map_err(|_| {
                scheduler_error("DTG-ANALYTICS-ARTIFACT-GC", "orphan count overflow")
            })?,
            Ordering::Relaxed,
        );
        self.metrics.orphan_protected_current.fetch_add(
            u64::try_from(plan.stats().protected_current()).map_err(|_| {
                scheduler_error("DTG-ANALYTICS-ARTIFACT-GC", "protected count overflow")
            })?,
            Ordering::Relaxed,
        );
        self.metrics.orphan_skipped_ttl.fetch_add(
            u64::try_from(plan.stats().skipped_ttl()).map_err(|_| {
                scheduler_error("DTG-ANALYTICS-ARTIFACT-GC", "TTL skip count overflow")
            })?,
            Ordering::Relaxed,
        );
        self.metrics.orphan_skipped_pinned_latest.fetch_add(
            u64::try_from(plan.stats().skipped_pinned_latest()).map_err(|_| {
                scheduler_error("DTG-ANALYTICS-ARTIFACT-GC", "pinned skip count overflow")
            })?,
            Ordering::Relaxed,
        );
        for candidate in plan.candidates() {
            let job_id = AnalyticsJobId::new(candidate.job_id())
                .map_err(|error| scheduler_error("DTG-ANALYTICS-ARTIFACT-GC", error.to_string()))?;
            let protection = protections
                .get(&candidate.job_id())
                .copied()
                .ok_or_else(|| {
                    scheduler_error(
                        "DTG-ANALYTICS-ARTIFACT-GC-META-RACE",
                        "orphan candidate lost its Meta protection proof",
                    )
                })?;
            let fresh = self.get_record_optional(job_id).await?;
            if protection.is_active() {
                let fresh = fresh.as_ref().ok_or_else(|| {
                    scheduler_error(
                        "DTG-ANALYTICS-ARTIFACT-GC-META-RACE",
                        "an active artifact Job disappeared during orphan maintenance",
                    )
                })?;
                if record_protects_orphan_candidate(fresh, *candidate) {
                    continue;
                }
                if !orphan_revision_matches(protection.revision(), fresh.job_revision()) {
                    return Err(scheduler_error(
                        "DTG-ANALYTICS-ARTIFACT-GC-META-RACE",
                        "an artifact Job changed during orphan maintenance",
                    ));
                }
            } else if fresh.is_some() {
                return Err(scheduler_error(
                    "DTG-ANALYTICS-ARTIFACT-GC-META-RACE",
                    "a tombstoned artifact Job is active during orphan maintenance",
                ));
            }
            let placement = placements_by_id.get(&candidate.shard_id()).ok_or_else(|| {
                scheduler_error(
                    "DTG-ANALYTICS-ARTIFACT-GC",
                    "orphan candidate refers to an absent Shard",
                )
            })?;
            if candidate.requires_fence_advance() {
                let advance_generation =
                    candidate.generation().checked_add(1).ok_or_else(|| {
                        scheduler_error(
                            "DTG-ANALYTICS-ARTIFACT-GC",
                            "pinned orphan fence generation overflow",
                        )
                    })?;
                let advance = AdvanceArtifactFenceRequest::new_with_gc_epoch(
                    ShardRequestContext::new(
                        graph_id,
                        placement.shard_id(),
                        placement.placement_epoch(),
                        self.next_request_id()?,
                        {
                            self.ensure_gc_lease(gc_lease, true).await?;
                            gc_lease.request_deadline(unix_time_ms()?)?
                        },
                    )
                    .map_err(|error| {
                        scheduler_error("DTG-ANALYTICS-ARTIFACT-GC", error.to_string())
                    })?,
                    candidate.job_id(),
                    candidate.kind(),
                    advance_generation,
                    gc_lease.epoch,
                )
                .map_err(|error| scheduler_error("DTG-ANALYTICS-ARTIFACT-GC", error.to_string()))?;
                self.shard_client
                    .advance_artifact_fence(advance)
                    .await
                    .map_err(|error| {
                        scheduler_error("DTG-ANALYTICS-ARTIFACT-GC-FENCE", error.to_string())
                    })?;
                self.inject(AnalyticsFaultPoint::GcAfterFenceAdvance)?;
            }
            self.inject(AnalyticsFaultPoint::GcBeforeDelete)?;
            self.ensure_gc_lease(gc_lease, true).await?;
            let delete_now = unix_time_ms()?;
            let request = DeleteArtifactGenerationRequest::new_with_gc_epoch(
                ShardRequestContext::new(
                    graph_id,
                    placement.shard_id(),
                    placement.placement_epoch(),
                    self.next_request_id()?,
                    gc_lease.request_deadline(delete_now)?,
                )
                .map_err(|error| scheduler_error("DTG-ANALYTICS-ARTIFACT-GC", error.to_string()))?,
                candidate.job_id(),
                candidate.kind(),
                candidate.generation(),
                gc_lease.epoch,
            )
            .map_err(|error| scheduler_error("DTG-ANALYTICS-ARTIFACT-GC", error.to_string()))?;
            match self.shard_client.delete_artifact_generation(request).await {
                Ok(_) => {
                    self.metrics.orphan_deleted.fetch_add(1, Ordering::Relaxed);
                    self.metrics
                        .orphan_bytes_reclaimed
                        .fetch_add(candidate.expected_total_bytes, Ordering::Relaxed);
                }
                Err(error) => {
                    self.metrics
                        .orphan_delete_failures
                        .fetch_add(1, Ordering::Relaxed);
                    return Err(scheduler_error(
                        "DTG-ANALYTICS-ARTIFACT-GC-DELETE",
                        error.to_string(),
                    ));
                }
            }
        }
        for (job_id, tombstone_revision) in plan.acknowledgements() {
            self.ensure_gc_lease(gc_lease, true).await?;
            let job_id = AnalyticsJobId::new(*job_id)
                .map_err(|error| scheduler_error("DTG-ANALYTICS-ARTIFACT-GC", error.to_string()))?;
            let command = JobCommand::acknowledge_artifacts_reclaimed(
                self.command_id(
                    job_id,
                    *tombstone_revision,
                    gc_lease.epoch,
                    b"ack-artifacts-reclaimed",
                ),
                job_id,
                *tombstone_revision,
                self.gateway_id,
                gc_lease.epoch,
                unix_time_ms()?,
            )
            .map_err(|error| scheduler_error("DTG-ANALYTICS-LEDGER", error.to_string()))?;
            self.inject(AnalyticsFaultPoint::GcBeforeAcknowledgement)?;
            self.propose(command).await?;
        }
        Ok(())
    }

    async fn acquire_gc_lease(&self) -> Result<AnalyticsGcLease, ClusterAnalyticsError> {
        let request_id = self.next_request_id()?;
        let request = AcquireAnalyticsGcLeaseRequest {
            context: Some(
                self.request_context(request_id, unix_time_ms()?.saturating_add(10_000))?,
            ),
            gateway_id: self.gateway_id,
        };
        let mut last_error = None;
        for index in self.meta_client_indexes() {
            let mut client = self.clients[index].clone();
            match tokio::time::timeout(
                META_ENDPOINT_ATTEMPT_TIMEOUT,
                client.acquire_analytics_gc_lease(request.clone()),
            )
            .await
            {
                Ok(Ok(response)) => {
                    self.remember_meta_client(index);
                    let response = response.into_inner();
                    if response.gc_epoch == 0 || response.lease_expires_unix_ms == 0 {
                        return Err(scheduler_error(
                            "DTG-ANALYTICS-GC-LEASE",
                            "Meta returned an invalid analytics GC lease",
                        ));
                    }
                    self.metrics.gc_lease_grants.fetch_add(1, Ordering::Relaxed);
                    return Ok(AnalyticsGcLease {
                        epoch: response.gc_epoch,
                        expires_unix_ms: response.lease_expires_unix_ms,
                    });
                }
                Ok(Err(error)) => {
                    if status_is_gc_lease_conflict(&error) {
                        self.metrics
                            .gc_lease_conflicts
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    last_error = Some(error.to_string());
                }
                Err(_) => last_error = Some("Meta analytics GC lease attempt timed out".into()),
            }
        }
        Err(scheduler_error(
            "DTG-ANALYTICS-GC-LEASE",
            last_error.unwrap_or_else(|| "no Meta endpoint granted the analytics GC lease".into()),
        ))
    }

    async fn renew_gc_lease(
        &self,
        lease: &mut AnalyticsGcLease,
    ) -> Result<(), ClusterAnalyticsError> {
        let renewed = self.acquire_gc_lease().await?;
        apply_gc_lease_renewal(lease, renewed)?;
        self.metrics
            .gc_lease_renewals
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    async fn ensure_gc_lease(
        &self,
        lease: &mut AnalyticsGcLease,
        force_renewal: bool,
    ) -> Result<(), ClusterAnalyticsError> {
        let now = unix_time_ms()?;
        if force_renewal || lease.requires_renewal(now) {
            self.renew_gc_lease(lease).await?;
        }
        if lease.expires_unix_ms <= unix_time_ms()? {
            return Err(scheduler_error(
                "DTG-ANALYTICS-GC-LEASE",
                "analytics GC lease expired during maintenance",
            ));
        }
        Ok(())
    }

    async fn list_jobs(
        &self,
        gc_lease: &mut AnalyticsGcLease,
    ) -> Result<Vec<(AnalyticsJobId, JobRecord)>, ClusterAnalyticsError> {
        let mut jobs = Vec::new();
        let mut after_job_id = Vec::new();
        loop {
            self.ensure_gc_lease(gc_lease, false).await?;
            let request_id = self.next_request_id()?;
            let now = unix_time_ms()?;
            let request = ListAnalyticsJobsRequest {
                context: Some(self.request_context(request_id, now.saturating_add(10_000))?),
                after_job_id: after_job_id.clone(),
                limit: DEFAULT_CLAIM_LIMIT,
            };
            let mut page = None;
            for index in self.meta_client_indexes() {
                let mut client = self.clients[index].clone();
                if let Ok(Ok(response)) = tokio::time::timeout(
                    META_ENDPOINT_ATTEMPT_TIMEOUT,
                    client.list_analytics_jobs(request.clone()),
                )
                .await
                {
                    self.remember_meta_client(index);
                    page = Some(response.into_inner());
                    break;
                }
            }
            let page = page.ok_or_else(|| {
                scheduler_error(
                    "DTG-ANALYTICS-META-UNAVAILABLE",
                    "Meta job maintenance scan failed",
                )
            })?;
            let page_len = page.jobs.len();
            for job in page.jobs {
                if crc32fast::hash(&job.record) != job.checksum {
                    return Err(scheduler_error(
                        "DTG-ANALYTICS-META-PROTOCOL",
                        "Meta job checksum is invalid",
                    ));
                }
                let raw: [u8; 16] = job.job_id.as_slice().try_into().map_err(|_| {
                    scheduler_error(
                        "DTG-ANALYTICS-META-PROTOCOL",
                        "Meta returned an invalid job ID",
                    )
                })?;
                let job_id = AnalyticsJobId::new(u128::from_be_bytes(raw))
                    .map_err(|error| scheduler_error("DTG-ANALYTICS-LEDGER", error.to_string()))?;
                let (_, record) = LedgerState::decode_job(&job.record)
                    .map_err(|error| scheduler_error("DTG-ANALYTICS-LEDGER", error.to_string()))?;
                after_job_id = job_id.value().to_be_bytes().to_vec();
                jobs.push((job_id, record));
            }
            if page_len < usize::try_from(DEFAULT_CLAIM_LIMIT).expect("claim limit fits usize") {
                return Ok(jobs);
            }
        }
    }

    async fn list_tombstones(
        &self,
        gc_lease: &mut AnalyticsGcLease,
    ) -> Result<Vec<(AnalyticsJobId, JobTombstone)>, ClusterAnalyticsError> {
        let mut tombstones = Vec::new();
        let mut after_job_id = Vec::new();
        let mut expected_ledger_revision = None;
        loop {
            self.ensure_gc_lease(gc_lease, false).await?;
            let request_id = self.next_request_id()?;
            let request = ListAnalyticsJobTombstonesRequest {
                context: Some(
                    self.request_context(request_id, unix_time_ms()?.saturating_add(10_000))?,
                ),
                after_job_id: after_job_id.clone(),
                limit: DEFAULT_CLAIM_LIMIT,
            };
            let mut page = None;
            for index in self.meta_client_indexes() {
                let mut client = self.clients[index].clone();
                if let Ok(Ok(response)) = tokio::time::timeout(
                    META_ENDPOINT_ATTEMPT_TIMEOUT,
                    client.list_analytics_job_tombstones(request.clone()),
                )
                .await
                {
                    self.remember_meta_client(index);
                    page = Some(response.into_inner());
                    break;
                }
            }
            let page = page.ok_or_else(|| {
                scheduler_error(
                    "DTG-ANALYTICS-META-UNAVAILABLE",
                    "Meta tombstone maintenance scan failed",
                )
            })?;
            if expected_ledger_revision
                .replace(page.ledger_revision)
                .is_some_and(|expected| expected != page.ledger_revision)
            {
                return Err(scheduler_error(
                    "DTG-ANALYTICS-META-PROTOCOL",
                    "Meta tombstone pages span different Ledger revisions",
                ));
            }
            let page_len = page.tombstones.len();
            let mut last_job_id = if after_job_id.is_empty() {
                None
            } else {
                Some(decode_wire_job_id(&after_job_id, "Meta tombstone cursor")?)
            };
            for wire in page.tombstones {
                if crc32fast::hash(&wire.record) != wire.checksum {
                    return Err(scheduler_error(
                        "DTG-ANALYTICS-META-PROTOCOL",
                        "Meta tombstone checksum is invalid",
                    ));
                }
                let raw: [u8; 16] = wire.job_id.as_slice().try_into().map_err(|_| {
                    scheduler_error(
                        "DTG-ANALYTICS-META-PROTOCOL",
                        "Meta returned an invalid tombstone job ID",
                    )
                })?;
                let wire_job_id = AnalyticsJobId::new(u128::from_be_bytes(raw))
                    .map_err(|error| scheduler_error("DTG-ANALYTICS-LEDGER", error.to_string()))?;
                let (ledger_revision, decoded_job_id, tombstone) =
                    LedgerState::decode_tombstone(&wire.record).map_err(|error| {
                        scheduler_error("DTG-ANALYTICS-LEDGER", error.to_string())
                    })?;
                if ledger_revision != page.ledger_revision
                    || decoded_job_id != wire_job_id
                    || last_job_id.is_some_and(|previous| previous >= wire_job_id)
                {
                    return Err(scheduler_error(
                        "DTG-ANALYTICS-META-PROTOCOL",
                        "Meta tombstone page is not canonical",
                    ));
                }
                last_job_id = Some(wire_job_id);
                tombstones.push((wire_job_id, tombstone));
            }
            let full =
                page_len == usize::try_from(DEFAULT_CLAIM_LIMIT).expect("claim limit fits usize");
            if full {
                let last_job_id = last_job_id.ok_or_else(|| {
                    scheduler_error(
                        "DTG-ANALYTICS-META-PROTOCOL",
                        "full Meta tombstone page is empty",
                    )
                })?;
                if page.next_job_id != last_job_id.value().to_be_bytes() {
                    return Err(scheduler_error(
                        "DTG-ANALYTICS-META-PROTOCOL",
                        "Meta tombstone next cursor is not canonical",
                    ));
                }
                after_job_id = page.next_job_id;
            } else {
                if !page.next_job_id.is_empty() {
                    return Err(scheduler_error(
                        "DTG-ANALYTICS-META-PROTOCOL",
                        "short Meta tombstone page has a next cursor",
                    ));
                }
                return Ok(tombstones);
            }
        }
    }

    async fn fail_owned_running_job(&self, job_id: AnalyticsJobId, error: ClusterAnalyticsError) {
        let Ok(record) = self.get_record(job_id).await else {
            return;
        };
        if record.state() != JobState::Running {
            return;
        }
        let Some(lease) = record.lease() else {
            return;
        };
        if lease.owner_gateway_id() != self.gateway_id {
            return;
        }
        let Ok(topology_epoch) = self.current_topology_epoch() else {
            return;
        };
        let Ok(command) = JobCommand::fail(
            self.command_id(
                job_id,
                record.job_revision(),
                lease.lease_epoch(),
                b"fail-scheduler",
            ),
            job_id,
            record.job_revision(),
            self.gateway_id,
            lease.lease_epoch(),
            topology_epoch,
            error.code(),
            error.message(),
        ) else {
            return;
        };
        let _ = self.propose(command).await;
    }

    async fn list_claimable(
        &self,
        now: u64,
    ) -> Result<Vec<analytics_ledger::JobCandidate>, ClusterAnalyticsError> {
        let request_id = self.next_request_id()?;
        let request = ListClaimableAnalyticsJobsRequest {
            context: Some(self.request_context(request_id, now.saturating_add(5_000))?),
            now_unix_ms: now,
            after_job_id: Vec::new(),
            limit: DEFAULT_CLAIM_LIMIT,
        };
        let mut last_error = None;
        for index in self.meta_client_indexes() {
            let mut client = self.clients[index].clone();
            match tokio::time::timeout(
                META_ENDPOINT_ATTEMPT_TIMEOUT,
                client.list_claimable_analytics_jobs(request.clone()),
            )
            .await
            {
                Ok(Ok(response)) => {
                    self.remember_meta_client(index);
                    let response = response.into_inner();
                    let mut candidates = Vec::with_capacity(response.candidates.len());
                    for candidate in response.candidates {
                        let raw: [u8; 16] =
                            candidate.job_id.as_slice().try_into().map_err(|_| {
                                scheduler_error(
                                    "DTG-ANALYTICS-META-PROTOCOL",
                                    "Meta returned an invalid job ID",
                                )
                            })?;
                        let job_id =
                            AnalyticsJobId::new(u128::from_be_bytes(raw)).map_err(|error| {
                                scheduler_error("DTG-ANALYTICS-META-PROTOCOL", error.to_string())
                            })?;
                        candidates.push(analytics_ledger::JobCandidate::new(
                            job_id,
                            candidate.job_revision,
                            (candidate.expired_lease_epoch != 0)
                                .then_some(candidate.expired_lease_epoch),
                        ));
                    }
                    return Ok(candidates);
                }
                Ok(Err(error)) => last_error = Some(error.to_string()),
                Err(_) => last_error = Some("Meta claimable-job scan attempt timed out".into()),
            }
        }
        Err(scheduler_error(
            "DTG-ANALYTICS-META-UNAVAILABLE",
            last_error.unwrap_or_else(|| "no Meta endpoint was reachable".into()),
        ))
    }

    async fn execute_candidate(
        &self,
        candidate: analytics_ledger::JobCandidate,
        now: u64,
    ) -> Result<(), ClusterAnalyticsError> {
        let topology_epoch = self.current_topology_epoch()?;
        let mut expected_revision = candidate.job_revision();
        if let Some(expired_lease_epoch) = candidate.lease_epoch() {
            let expire = JobCommand::expire_lease(
                self.command_id(
                    candidate.job_id(),
                    expected_revision,
                    expired_lease_epoch,
                    b"expire",
                ),
                candidate.job_id(),
                expected_revision,
                expired_lease_epoch,
                now,
            )
            .map_err(|error| scheduler_error("DTG-ANALYTICS-LEDGER", error.to_string()))?;
            let response = self.propose(expire).await?;
            expected_revision = response.job_revision;
        }
        let claim = JobCommand::claim(
            self.command_id(candidate.job_id(), expected_revision, 0, b"claim"),
            candidate.job_id(),
            expected_revision,
            self.gateway_id,
            topology_epoch,
            now.saturating_add(DEFAULT_LEASE_DURATION.as_millis() as u64),
        )
        .map_err(|error| scheduler_error("DTG-ANALYTICS-LEDGER", error.to_string()))?;
        self.inject(AnalyticsFaultPoint::Claim)?;
        let response = match self.propose(claim).await {
            Ok(response) => response,
            Err(_) => return Ok(()),
        };
        let record = self.get_record(candidate.job_id()).await?;
        let lease = record
            .lease()
            .ok_or_else(|| scheduler_error("DTG-ANALYTICS-LEDGER", "claimed job has no lease"))?;
        let begin = JobCommand::begin_run(
            self.command_id(
                candidate.job_id(),
                response.job_revision,
                lease.lease_epoch(),
                b"begin",
            ),
            candidate.job_id(),
            response.job_revision,
            self.gateway_id,
            lease.lease_epoch(),
            topology_epoch,
        )
        .map_err(|error| scheduler_error("DTG-ANALYTICS-LEDGER", error.to_string()))?;
        self.inject(AnalyticsFaultPoint::Begin)?;
        let begin_response = self.propose(begin).await?;
        let running = self.get_record(candidate.job_id()).await?;
        let lease = running
            .lease()
            .ok_or_else(|| scheduler_error("DTG-ANALYTICS-LEDGER", "running job has no lease"))?;
        let renew = JobCommand::renew(
            self.command_id(
                candidate.job_id(),
                begin_response.job_revision,
                lease.lease_epoch(),
                b"renew",
            ),
            candidate.job_id(),
            begin_response.job_revision,
            self.gateway_id,
            lease.lease_epoch(),
            topology_epoch,
            unix_time_ms()?.saturating_add(DEFAULT_LEASE_DURATION.as_millis() as u64),
        )
        .map_err(|error| scheduler_error("DTG-ANALYTICS-LEDGER", error.to_string()))?;
        self.inject(AnalyticsFaultPoint::LeaseRenew)?;
        let renew_response = self.propose(renew).await?;
        let (graph, input_indexes) = self.project_snapshot(&running).await?;
        let projection_identity = projection_identity(running.spec());
        let input_indexes_digest = canonical_applied_indexes_digest(&input_indexes);
        let parameters = decode_algorithm_parameters(running.spec().parameters())
            .map_err(|error| scheduler_error("DTG-ANALYTICS-PARAMETER", error.to_string()))?;
        let request = AlgorithmRequest::from_shared(
            running.spec().algorithm().to_owned(),
            Arc::new(graph),
            parameters,
        )
        .map_err(|error| scheduler_error("DTG-ANALYTICS-PROVIDER", error.to_string()))?;
        let (
            checkpoint_revision,
            input_fence_count,
            input_fence_digest,
            completed_units,
            checkpoint_generation,
            restored_prefix,
            restored_provider_state,
        ) = if running
            .checkpoint()
            .is_some_and(|checkpoint| checkpoint.projection_identity() == projection_identity)
        {
            self.metrics
                .resumed_from_checkpoint
                .fetch_add(1, Ordering::Relaxed);
            let checkpoint = running.checkpoint().expect("checkpoint matched above");
            let checkpoint_bytes = self
                .read_artifact_bytes(
                    running.spec().graph_id(),
                    running.spec().job_id(),
                    checkpoint,
                    ShardArtifactKind::Checkpoint,
                )
                .await?;
            let decoded = decode_provider_checkpoint_artifact(&checkpoint_bytes)
                .map_err(|error| scheduler_error("DTG-ANALYTICS-CHECKPOINT", error.to_string()))?;
            if decoded.projection_identity() != projection_identity
                || decoded.input_applied_indexes_digest()
                    != checkpoint.input_applied_indexes_digest()
                || decoded.algorithm() != request.algorithm()
            {
                return Err(scheduler_error(
                    "DTG-ANALYTICS-CHECKPOINT",
                    "checkpoint payload does not match its Meta manifest",
                ));
            }
            let provider_checkpoint = analytics_api::ProviderCheckpoint::new(
                request.algorithm(),
                decoded.completed_units(),
                decoded.provider_state().to_vec(),
            )
            .map_err(|error| scheduler_error("DTG-ANALYTICS-CHECKPOINT", error.to_string()))?;
            self.provider
                .restore_checkpoint(&request, &provider_checkpoint)
                .map_err(|error| scheduler_error("DTG-ANALYTICS-CHECKPOINT", error.to_string()))?;
            (
                renew_response.job_revision,
                checkpoint.input_applied_index_count(),
                checkpoint.input_applied_indexes_digest(),
                decoded.completed_units(),
                checkpoint.generation(),
                if decoded.result_prefix().is_empty() {
                    AlgorithmResult::empty()
                } else {
                    decode_algorithm_result_artifact(decoded.result_prefix()).map_err(|error| {
                        scheduler_error("DTG-ANALYTICS-CHECKPOINT", error.to_string())
                    })?
                },
                decoded.provider_state().to_vec(),
            )
        } else {
            let provider_checkpoint = self
                .provider
                .checkpoint(&request, 0)
                .map_err(|error| scheduler_error("DTG-ANALYTICS-CHECKPOINT", error.to_string()))?;
            let checkpoint = ProviderCheckpointArtifactV1::new(
                projection_identity,
                input_indexes_digest,
                request.algorithm(),
                provider_checkpoint.completed_units(),
                provider_checkpoint.payload().to_vec(),
                Vec::new(),
            )
            .map_err(|error| scheduler_error("DTG-ANALYTICS-CHECKPOINT", error.to_string()))?;
            let checkpoint_bytes = encode_provider_checkpoint_artifact(checkpoint);
            let minimum_checkpoint_generation = running
                .checkpoint()
                .map_or(1, |manifest| manifest.generation().saturating_add(1));
            let storage_shard = self.storage_shard()?;
            let checkpoint_generation = self
                .next_available_artifact_generation(
                    running.spec().job_id(),
                    &storage_shard,
                    ShardArtifactKind::Checkpoint,
                    minimum_checkpoint_generation,
                )
                .await?;
            let (chunk_count, content_digest) = self
                .put_and_pin_artifact(
                    running.spec().job_id(),
                    &storage_shard,
                    checkpoint_generation,
                    ShardArtifactKind::Checkpoint,
                    &checkpoint_bytes,
                    running.spec().job_id().value()
                        ^ u128::from(renew_response.job_revision)
                        ^ 0x4350,
                )
                .await?;
            let manifest = ArtifactManifest::new(
                ArtifactKind::Checkpoint,
                checkpoint_generation,
                storage_shard.shard_id(),
                chunk_count,
                u64::try_from(checkpoint_bytes.len()).map_err(|_| {
                    scheduler_error("DTG-ANALYTICS-CHECKPOINT", "checkpoint size overflow")
                })?,
                content_digest,
                projection_identity,
                running.spec().provider_version(),
                running.spec().algorithm_version(),
                NextExecutionStage::Provider { completed_units: 0 },
                input_indexes.clone(),
            )
            .map_err(|error| scheduler_error("DTG-ANALYTICS-CHECKPOINT", error.to_string()))?;
            let commit = JobCommand::commit_checkpoint(
                self.command_id(
                    running.spec().job_id(),
                    renew_response.job_revision,
                    lease.lease_epoch(),
                    b"checkpoint",
                ),
                running.spec().job_id(),
                renew_response.job_revision,
                self.gateway_id,
                lease.lease_epoch(),
                topology_epoch,
                manifest,
            )
            .map_err(|error| scheduler_error("DTG-ANALYTICS-LEDGER", error.to_string()))?;
            self.inject(AnalyticsFaultPoint::CheckpointCas)?;
            let committed_revision = self.propose(commit).await?.job_revision;
            (
                committed_revision,
                u32::try_from(input_indexes.len()).map_err(|_| {
                    scheduler_error("DTG-ANALYTICS-CHECKPOINT", "input Shard count overflow")
                })?,
                input_indexes_digest,
                provider_checkpoint.completed_units(),
                checkpoint_generation,
                AlgorithmResult::empty(),
                provider_checkpoint.payload().to_vec(),
            )
        };
        let (provider_result, publish_revision, checkpoint_generation) = self
            .execute_with_heartbeat(
                request,
                running.spec().job_id(),
                checkpoint_revision,
                lease.lease_epoch(),
                topology_epoch,
                completed_units,
                checkpoint_generation,
                projection_identity,
                input_fence_count,
                input_fence_digest,
                running.spec().provider_version(),
                running.spec().algorithm_version(),
                restored_prefix,
                restored_provider_state,
            )
            .await?;
        let result = match provider_result {
            Ok(result) => result,
            Err(error) => {
                let failure = JobCommand::fail(
                    self.command_id(
                        candidate.job_id(),
                        publish_revision,
                        lease.lease_epoch(),
                        b"fail",
                    ),
                    candidate.job_id(),
                    publish_revision,
                    self.gateway_id,
                    lease.lease_epoch(),
                    topology_epoch,
                    error.code(),
                    error.message(),
                )
                .map_err(|ledger_error| {
                    scheduler_error("DTG-ANALYTICS-LEDGER", ledger_error.to_string())
                })?;
                let _ = self.propose(failure).await;
                return Ok(());
            }
        };
        let bytes = encode_algorithm_result_artifact(&result)
            .map_err(|error| scheduler_error("DTG-ANALYTICS-RESULT", error.to_string()))?;
        let storage_shard = self.storage_shard()?;
        let artifact_chunk_bytes = usize::try_from(MAX_ARTIFACT_CHUNK_BYTES).map_err(|_| {
            scheduler_error(
                "DTG-ANALYTICS-RESULT",
                "artifact chunk byte limit does not fit this platform",
            )
        })?;
        let expected_chunk_count = u16::try_from(bytes.len().div_ceil(artifact_chunk_bytes))
            .map_err(|_| scheduler_error("DTG-ANALYTICS-RESULT", "result chunk count overflow"))?;
        let expected_total_bytes = u64::try_from(bytes.len())
            .map_err(|_| scheduler_error("DTG-ANALYTICS-RESULT", "result size overflow"))?;
        let expected_content_digest = *blake3::hash(&bytes).as_bytes();
        let reusable_generation = self
            .find_reusable_pinned_result_generation(
                running.spec().job_id(),
                &storage_shard,
                expected_chunk_count,
                expected_total_bytes,
                expected_content_digest,
            )
            .await?;
        let (generation, chunk_count, content_digest) =
            if let Some(generation) = reusable_generation {
                (
                    generation,
                    u64::from(expected_chunk_count),
                    expected_content_digest,
                )
            } else {
                let generation = checkpoint_generation.saturating_add(lease.lease_epoch());
                let (chunk_count, content_digest) = self
                    .put_and_pin_artifact(
                        running.spec().job_id(),
                        &storage_shard,
                        generation,
                        ShardArtifactKind::Result,
                        &bytes,
                        running.spec().job_id().value() ^ u128::from(publish_revision) ^ 0x5253,
                    )
                    .await?;
                (generation, chunk_count, content_digest)
            };
        let manifest = ArtifactManifest::new_with_input_fence(
            ArtifactKind::Result,
            generation,
            storage_shard.shard_id(),
            chunk_count,
            expected_total_bytes,
            content_digest,
            projection_identity,
            running.spec().provider_version(),
            running.spec().algorithm_version(),
            NextExecutionStage::Complete,
            input_fence_count,
            input_fence_digest,
        )
        .map_err(|error| scheduler_error("DTG-ANALYTICS-RESULT", error.to_string()))?;
        let publish = JobCommand::publish_result(
            self.command_id(
                running.spec().job_id(),
                publish_revision,
                lease.lease_epoch(),
                b"publish",
            ),
            running.spec().job_id(),
            publish_revision,
            self.gateway_id,
            lease.lease_epoch(),
            topology_epoch,
            manifest,
        )
        .map_err(|error| scheduler_error("DTG-ANALYTICS-LEDGER", error.to_string()))?;
        self.inject(AnalyticsFaultPoint::Publish)?;
        let _ = self.propose(publish).await?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn execute_with_heartbeat(
        &self,
        request: AlgorithmRequest,
        job_id: AnalyticsJobId,
        initial_revision: u64,
        lease_epoch: u64,
        topology_epoch: u64,
        completed_units: u64,
        checkpoint_generation: u64,
        projection_identity: [u8; 32],
        input_fence_count: u32,
        input_fence_digest: [u8; 32],
        provider_version: &str,
        algorithm_version: &str,
        restored_prefix: AlgorithmResult,
        restored_provider_state: Vec<u8>,
    ) -> Result<(Result<AlgorithmResult, ProviderError>, u64, u64), ClusterAnalyticsError> {
        let cancellation = Arc::new(AtomicBool::new(false));
        let algorithm = request.algorithm().to_owned();
        let worker_request =
            request.with_cancellation(CancellationSignal::from_flag(Arc::clone(&cancellation)));
        let provider = Arc::clone(&self.provider);
        let delay = self.execution_delay;
        let (progress_sender, mut progress_receiver) = mpsc::unbounded_channel();
        let mut execution = tokio::task::spawn_blocking(move || {
            if !delay.is_zero() {
                std::thread::sleep(delay);
            }
            if !matches!(
                worker_request.algorithm(),
                "dtg.graph.degree" | "dtg.graph.wcc" | "dtg.graph.pageRank"
            ) {
                return provider.execute(worker_request);
            }
            let mut cursor = completed_units;
            let mut assembled = restored_prefix;
            if matches!(
                worker_request.algorithm(),
                "dtg.graph.wcc" | "dtg.graph.pageRank"
            ) && !assembled.columns().is_empty()
            {
                return Ok(assembled);
            }
            let mut checkpoint = analytics_api::ProviderCheckpoint::new(
                worker_request.algorithm(),
                completed_units,
                restored_provider_state,
            )?;
            let slice_units = if worker_request.algorithm() == "dtg.graph.degree" {
                1_024
            } else {
                1
            };
            loop {
                let mut slice = AlgorithmResult::empty();
                let state = provider.execute_slice_from_checkpoint(
                    &worker_request,
                    &checkpoint,
                    slice_units,
                    &mut slice,
                )?;
                assembled.append(slice)?;
                if progress_sender
                    .send((
                        state.next_unit(),
                        state.checkpoint_payload().to_vec(),
                        assembled.clone(),
                    ))
                    .is_err()
                {
                    return Err(ProviderError::new(
                        "DTG-ANALYTICS-SLICE-CANCELED",
                        "scheduler stopped consuming execution progress",
                    ));
                }
                if state.complete() {
                    if state.next_unit() < completed_units {
                        return Err(ProviderError::new(
                            "DTG-ANALYTICS-SLICE-STATE",
                            "checkpoint cursor is beyond the provider result",
                        ));
                    }
                    return Ok(assembled);
                }
                let next = state.next_unit();
                if next <= cursor {
                    return Err(ProviderError::new(
                        "DTG-ANALYTICS-SLICE-STATE",
                        "provider did not advance the execution cursor",
                    ));
                }
                cursor = next;
                checkpoint = analytics_api::ProviderCheckpoint::new(
                    worker_request.algorithm(),
                    cursor,
                    state.checkpoint_payload().to_vec(),
                )?;
            }
        });
        let mut revision = initial_revision;
        let mut generation = checkpoint_generation;
        let mut persisted_units = completed_units;
        let heartbeat = DEFAULT_LEASE_DURATION
            .checked_div(3)
            .unwrap_or(Duration::from_secs(1));
        let mut ticker = tokio::time::interval(heartbeat.max(Duration::from_millis(50)));
        loop {
            tokio::select! {
                result = &mut execution => {
                    let result = result.map_err(|error| scheduler_error("DTG-ANALYTICS-PROVIDER", error.to_string()))?;
                    while let Ok((cursor, provider_state, prefix)) = progress_receiver.try_recv() {
                        if cursor > persisted_units {
                            self.inject(AnalyticsFaultPoint::ExecutionSlice)?;
                            (revision, generation) = self.persist_slice_checkpoint(
                                job_id,
                                revision,
                                lease_epoch,
                                topology_epoch,
                                cursor,
                                generation,
                                projection_identity,
                                input_fence_count,
                                input_fence_digest,
                                &algorithm,
                                provider_version,
                                algorithm_version,
                                &provider_state,
                                &prefix,
                            ).await?;
                            persisted_units = cursor;
                        }
                    }
                    return Ok((result, revision, generation));
                }
                Some((cursor, provider_state, prefix)) = progress_receiver.recv() => {
                    if cursor > persisted_units {
                        self.inject(AnalyticsFaultPoint::ExecutionSlice)?;
                        (revision, generation) = self.persist_slice_checkpoint(
                            job_id,
                            revision,
                            lease_epoch,
                            topology_epoch,
                            cursor,
                            generation,
                            projection_identity,
                            input_fence_count,
                            input_fence_digest,
                            &algorithm,
                            provider_version,
                            algorithm_version,
                            &provider_state,
                            &prefix,
                        ).await?;
                        persisted_units = cursor;
                    }
                }
                _ = ticker.tick() => {
                    let now = unix_time_ms()?;
                    let renew = JobCommand::renew(
                        self.command_id(job_id, revision, lease_epoch, b"heartbeat"),
                        job_id,
                        revision,
                        self.gateway_id,
                        lease_epoch,
                        topology_epoch,
                        now.saturating_add(DEFAULT_LEASE_DURATION.as_millis() as u64),
                    ).map_err(|error| scheduler_error("DTG-ANALYTICS-LEDGER", error.to_string()))?;
                    self.inject(AnalyticsFaultPoint::LeaseRenew)?;
                    match self.propose(renew).await {
                        Ok(response) => revision = response.job_revision,
                        Err(error) => {
                            cancellation.store(true, Ordering::Release);
                            return Err(error);
                        }
                    }
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn persist_slice_checkpoint(
        &self,
        job_id: AnalyticsJobId,
        expected_revision: u64,
        lease_epoch: u64,
        topology_epoch: u64,
        completed_units: u64,
        previous_generation: u64,
        projection_identity: [u8; 32],
        input_fence_count: u32,
        input_fence_digest: [u8; 32],
        algorithm: &str,
        provider_version: &str,
        algorithm_version: &str,
        provider_state: &[u8],
        result_prefix: &AlgorithmResult,
    ) -> Result<(u64, u64), ClusterAnalyticsError> {
        let result_prefix = if result_prefix.columns().is_empty() {
            Vec::new()
        } else {
            encode_algorithm_result_artifact(result_prefix)
                .map_err(|error| scheduler_error("DTG-ANALYTICS-CHECKPOINT", error.to_string()))?
        };
        let checkpoint = ProviderCheckpointArtifactV1::new(
            projection_identity,
            input_fence_digest,
            algorithm,
            completed_units,
            provider_state.to_vec(),
            result_prefix,
        )
        .map_err(|error| scheduler_error("DTG-ANALYTICS-CHECKPOINT", error.to_string()))?;
        let checkpoint_bytes = encode_provider_checkpoint_artifact(checkpoint);
        let storage_shard = self.storage_shard()?;
        let generation = self
            .next_available_artifact_generation(
                job_id,
                &storage_shard,
                ShardArtifactKind::Checkpoint,
                previous_generation.saturating_add(1),
            )
            .await?;
        let (chunk_count, content_digest) = self
            .put_and_pin_artifact(
                job_id,
                &storage_shard,
                generation,
                ShardArtifactKind::Checkpoint,
                &checkpoint_bytes,
                job_id.value() ^ u128::from(expected_revision) ^ u128::from(generation) ^ 0x4350,
            )
            .await?;
        let manifest = ArtifactManifest::new_with_input_fence(
            ArtifactKind::Checkpoint,
            generation,
            storage_shard.shard_id(),
            chunk_count,
            u64::try_from(checkpoint_bytes.len()).map_err(|_| {
                scheduler_error("DTG-ANALYTICS-CHECKPOINT", "checkpoint size overflow")
            })?,
            content_digest,
            projection_identity,
            provider_version,
            algorithm_version,
            NextExecutionStage::Provider { completed_units },
            input_fence_count,
            input_fence_digest,
        )
        .map_err(|error| scheduler_error("DTG-ANALYTICS-CHECKPOINT", error.to_string()))?;
        let command = JobCommand::commit_checkpoint(
            self.command_id(job_id, expected_revision, lease_epoch, b"checkpoint-slice"),
            job_id,
            expected_revision,
            self.gateway_id,
            lease_epoch,
            topology_epoch,
            manifest,
        )
        .map_err(|error| scheduler_error("DTG-ANALYTICS-LEDGER", error.to_string()))?;
        self.inject(AnalyticsFaultPoint::CheckpointCas)?;
        let response = self.propose(command).await?;
        let _ = self
            .delete_superseded_checkpoint(job_id, &storage_shard, previous_generation)
            .await;
        Ok((response.job_revision, generation))
    }

    async fn delete_superseded_checkpoint(
        &self,
        job_id: AnalyticsJobId,
        placement: &ShardPlacement,
        generation: u64,
    ) -> Result<(), ClusterAnalyticsError> {
        let mut gc_lease = self.acquire_gc_lease().await?;
        self.ensure_gc_lease(&mut gc_lease, true).await?;
        let request_now = unix_time_ms()?;
        let context = ShardRequestContext::new(
            self.current_graph_id()?,
            placement.shard_id(),
            placement.placement_epoch(),
            self.next_request_id()?,
            gc_lease.request_deadline(request_now)?,
        )
        .map_err(|error| scheduler_error("DTG-ANALYTICS-ARTIFACT-GC", error.to_string()))?;
        let request = DeleteArtifactGenerationRequest::new_with_gc_epoch(
            context,
            job_id.value(),
            ShardArtifactKind::Checkpoint,
            generation,
            gc_lease.epoch,
        )
        .map_err(|error| scheduler_error("DTG-ANALYTICS-ARTIFACT-GC", error.to_string()))?;
        self.shard_client
            .delete_artifact_generation(request)
            .await
            .map_err(|error| scheduler_error("DTG-ANALYTICS-ARTIFACT-GC", error.to_string()))?;
        Ok(())
    }

    async fn project_snapshot(
        &self,
        record: &JobRecord,
    ) -> Result<(ProjectedGraph, BTreeMap<u32, u64>), ClusterAnalyticsError> {
        if !matches!(
            record.spec().algorithm(),
            "dtg.graph.degree" | "dtg.graph.wcc" | "dtg.graph.pageRank"
        ) {
            return Err(scheduler_error(
                "DTG-ANALYTICS-UNSUPPORTED",
                "asynchronous scheduler supports Degree, WCC, and PageRank",
            ));
        }
        let valid_time = match record.spec().projection() {
            analytics_ledger::GraphProjectionScope::Snapshot { valid_time } => *valid_time,
            _ => {
                return Err(scheduler_error(
                    "DTG-ANALYTICS-GRAPH-MODEL",
                    "asynchronous graph analytics requires a Snapshot projection",
                ));
            }
        };
        let limits = ProjectionLimits::new(
            usize::try_from(record.spec().limits().max_vertices()).map_err(|_| {
                scheduler_error("DTG-ANALYTICS-CAPACITY", "vertex limit overflows platform")
            })?,
            usize::try_from(record.spec().limits().max_edges()).map_err(|_| {
                scheduler_error("DTG-ANALYTICS-CAPACITY", "edge limit overflows platform")
            })?,
            record.spec().limits().max_bytes(),
        )
        .map_err(|error| scheduler_error("DTG-ANALYTICS-CAPACITY", error.to_string()))?;
        let (graph_id, placements) = {
            let routing = self.routing.read().map_err(|_| {
                scheduler_error("DTG-ANALYTICS-ROUTING", "routing state lock is poisoned")
            })?;
            (
                routing.graph.graph_id(),
                routing.deployment.all_shards().to_vec(),
            )
        };
        let mut partitions = Vec::new();
        let mut input_indexes = BTreeMap::new();
        for placement in placements {
            let context = ShardRequestContext::new(
                graph_id,
                placement.shard_id(),
                placement.placement_epoch(),
                self.next_request_id()?,
                unix_time_ms()?.saturating_add(5_000),
            )
            .map_err(|error| scheduler_error("DTG-ANALYTICS-SHARD", error.to_string()))?;
            let status = self
                .shard_client
                .status(context)
                .await
                .map_err(|error| shard_operation_error("DTG-ANALYTICS-SHARD", error))?;
            if status.applied_index() == 0 {
                return Err(scheduler_error(
                    "DTG-ANALYTICS-SHARD",
                    "Shard applied index is zero",
                ));
            }
            input_indexes.insert(placement.shard_id(), status.applied_index());
            let adapter = ShardClientStorageAdapter::new(
                Arc::clone(&self.shard_client),
                graph_id,
                placement.shard_id(),
                placement.placement_epoch(),
                unix_time_ms()?.saturating_add(5_000),
                self.gateway_id ^ u64::from(placement.shard_id()),
            )
            .map_err(|error| scheduler_error("DTG-ANALYTICS-SHARD", error.to_string()))?;
            let (part, _) = project_snapshot_identity_part_bounded(
                &TemporalStore::new(adapter),
                temporal_storage::GraphId::new(graph_id),
                valid_time,
                record.spec().transaction_time(),
                None,
                limits,
            )
            .await
            .map_err(|error| match error {
                ProjectionError::StorageUnavailable(message) => scheduler_error(
                    ANALYTICS_RETRYABLE_INFRASTRUCTURE_CODE,
                    format!("DTG-ANALYTICS-PROJECTION: {message}"),
                ),
                error => scheduler_error("DTG-ANALYTICS-PROJECTION", error.to_string()),
            })?;
            let (vertices, edges) = part.into_parts();
            partitions.push(SnapshotPartition::new(
                placement.shard_id(),
                vertices.into_values().collect(),
                edges.into_values().collect(),
            ));
        }
        let graph = PartitionedSnapshotGraph::new(partitions, true)
            .map_err(|error| scheduler_error("DTG-ANALYTICS-PROJECTION", error.to_string()))?;
        Ok((ProjectedGraph::PartitionedSnapshot(graph), input_indexes))
    }

    async fn put_and_pin_artifact(
        &self,
        job_id: AnalyticsJobId,
        placement: &ShardPlacement,
        generation: u64,
        kind: ShardArtifactKind,
        bytes: &[u8],
        request_seed: u128,
    ) -> Result<(u64, [u8; 32]), ClusterAnalyticsError> {
        let content_digest = *blake3::hash(bytes).as_bytes();
        let now_unix_ms = unix_time_ms()?;
        let created_at_unix_ms = self
            .resolve_artifact_created_at(job_id, placement, generation, kind, now_unix_ms)
            .await?;
        let mut previous = [0; 32];
        let mut ordinal = 0_u64;
        for chunk in bytes.chunks(MAX_ARTIFACT_CHUNK_BYTES as usize) {
            self.inject(match kind {
                ShardArtifactKind::Checkpoint => AnalyticsFaultPoint::CheckpointUpload,
                ShardArtifactKind::Result => AnalyticsFaultPoint::ResultUpload,
            })?;
            let request_id = artifact_request_id(job_id, generation, kind, ordinal, request_seed);
            let context = ShardRequestContext::new(
                self.current_graph_id()?,
                placement.shard_id(),
                placement.placement_epoch(),
                request_id,
                unix_time_ms()?.saturating_add(10_000),
            )
            .map_err(|error| scheduler_error("DTG-ANALYTICS-ARTIFACT", error.to_string()))?;
            let request = PutArtifactChunkRequest::new(
                context,
                job_id.value(),
                kind,
                generation,
                created_at_unix_ms,
                ordinal,
                previous,
                chunk.to_vec(),
            )
            .map_err(|error| scheduler_error("DTG-ANALYTICS-ARTIFACT", error.to_string()))?;
            self.shard_client
                .put_artifact_chunk(request)
                .await
                .map_err(|error| shard_operation_error("DTG-ANALYTICS-ARTIFACT", error))?;
            previous = *blake3::hash(chunk).as_bytes();
            ordinal = ordinal.saturating_add(1);
        }
        let request_id = artifact_request_id(job_id, generation, kind, u64::MAX, request_seed);
        let context = ShardRequestContext::new(
            self.current_graph_id()?,
            placement.shard_id(),
            placement.placement_epoch(),
            request_id,
            unix_time_ms()?.saturating_add(10_000),
        )
        .map_err(|error| scheduler_error("DTG-ANALYTICS-ARTIFACT", error.to_string()))?;
        let request = PinArtifactGenerationRequest::new(
            context,
            job_id.value(),
            kind,
            generation,
            u16::try_from(ordinal)
                .map_err(|_| scheduler_error("DTG-ANALYTICS-ARTIFACT", "chunk count overflow"))?,
            u64::try_from(bytes.len())
                .map_err(|_| scheduler_error("DTG-ANALYTICS-ARTIFACT", "result size overflow"))?,
            content_digest,
        )
        .map_err(|error| scheduler_error("DTG-ANALYTICS-ARTIFACT", error.to_string()))?;
        self.inject(match kind {
            ShardArtifactKind::Checkpoint => AnalyticsFaultPoint::CheckpointPin,
            ShardArtifactKind::Result => AnalyticsFaultPoint::ResultPin,
        })?;
        self.shard_client
            .pin_artifact_generation(request)
            .await
            .map_err(|error| shard_operation_error("DTG-ANALYTICS-ARTIFACT", error))?;
        Ok((ordinal, content_digest))
    }

    async fn resolve_artifact_created_at(
        &self,
        job_id: AnalyticsJobId,
        placement: &ShardPlacement,
        generation: u64,
        kind: ShardArtifactKind,
        now_unix_ms: u64,
    ) -> Result<u64, ClusterAnalyticsError> {
        let context = ShardRequestContext::new(
            self.current_graph_id()?,
            placement.shard_id(),
            placement.placement_epoch(),
            self.next_request_id()?,
            now_unix_ms.saturating_add(10_000),
        )
        .map_err(|error| {
            scheduler_error("DTG-ANALYTICS-ARTIFACT-GENERATION-SCAN", error.to_string())
        })?;
        let summaries = self
            .shard_client
            .list_artifact_generations(
                ListArtifactGenerationsRequest::new(
                    context,
                    job_id.value(),
                    kind,
                    ARTIFACT_GENERATION_LIST_LIMIT,
                )
                .map_err(|error| {
                    scheduler_error("DTG-ANALYTICS-ARTIFACT-GENERATION-SCAN", error.to_string())
                })?,
            )
            .await
            .map_err(|error| {
                shard_operation_error("DTG-ANALYTICS-ARTIFACT-GENERATION-SCAN", error)
            })?;
        resolve_generation_created_at(
            summaries
                .iter()
                .map(|summary| (summary.generation(), summary.created_at_unix_ms())),
            generation,
            now_unix_ms,
        )
    }

    async fn next_available_artifact_generation(
        &self,
        job_id: AnalyticsJobId,
        placement: &ShardPlacement,
        kind: ShardArtifactKind,
        minimum: u64,
    ) -> Result<u64, ClusterAnalyticsError> {
        if minimum == 0 {
            return Err(scheduler_error(
                "DTG-ANALYTICS-ARTIFACT-GENERATION-SCAN",
                "minimum artifact generation must be non-zero",
            ));
        }
        let now = unix_time_ms()?;
        let context = ShardRequestContext::new(
            self.current_graph_id()?,
            placement.shard_id(),
            placement.placement_epoch(),
            self.next_request_id()?,
            now.saturating_add(10_000),
        )
        .map_err(|error| {
            scheduler_error("DTG-ANALYTICS-ARTIFACT-GENERATION-SCAN", error.to_string())
        })?;
        let summaries = self
            .shard_client
            .list_artifact_generations(
                ListArtifactGenerationsRequest::new(
                    context,
                    job_id.value(),
                    kind,
                    ARTIFACT_GENERATION_LIST_LIMIT,
                )
                .map_err(|error| {
                    scheduler_error("DTG-ANALYTICS-ARTIFACT-GENERATION-SCAN", error.to_string())
                })?,
            )
            .await
            .map_err(|error| {
                shard_operation_error("DTG-ANALYTICS-ARTIFACT-GENERATION-SCAN", error)
            })?;
        if summaries.len()
            == usize::try_from(ARTIFACT_GENERATION_LIST_LIMIT)
                .expect("artifact generation list limit fits usize")
        {
            return Err(scheduler_error(
                "DTG-ANALYTICS-ARTIFACT-GENERATION-SCAN",
                "artifact generation scan is saturated and cannot prove the next generation",
            ));
        }
        summaries.last().map_or(Ok(minimum), |summary| {
            summary
                .generation()
                .checked_add(1)
                .map(|next| next.max(minimum))
                .ok_or_else(|| {
                    scheduler_error(
                        "DTG-ANALYTICS-ARTIFACT-GENERATION-SCAN",
                        "artifact generation space is exhausted",
                    )
                })
        })
    }

    async fn find_reusable_pinned_result_generation(
        &self,
        job_id: AnalyticsJobId,
        placement: &ShardPlacement,
        expected_chunk_count: u16,
        expected_total_bytes: u64,
        expected_content_digest: [u8; 32],
    ) -> Result<Option<u64>, ClusterAnalyticsError> {
        let now = unix_time_ms()?;
        let context = ShardRequestContext::new(
            self.current_graph_id()?,
            placement.shard_id(),
            placement.placement_epoch(),
            self.next_request_id()?,
            now.saturating_add(10_000),
        )
        .map_err(|error| {
            scheduler_error("DTG-ANALYTICS-ARTIFACT-GENERATION-SCAN", error.to_string())
        })?;
        let summaries = self
            .shard_client
            .list_artifact_generations(
                ListArtifactGenerationsRequest::new(
                    context,
                    job_id.value(),
                    ShardArtifactKind::Result,
                    ARTIFACT_GENERATION_LIST_LIMIT,
                )
                .map_err(|error| {
                    scheduler_error("DTG-ANALYTICS-ARTIFACT-GENERATION-SCAN", error.to_string())
                })?,
            )
            .await
            .map_err(|error| {
                shard_operation_error("DTG-ANALYTICS-ARTIFACT-GENERATION-SCAN", error)
            })?;
        let reusable = select_reusable_pinned_generation(
            summaries.iter().map(|summary| {
                (
                    summary.generation(),
                    summary.pinned(),
                    summary.expected_chunk_count(),
                    summary.expected_total_bytes(),
                    summary.expected_content_digest(),
                )
            }),
            expected_chunk_count,
            expected_total_bytes,
            expected_content_digest,
        );
        if reusable.is_none()
            && summaries.len()
                == usize::try_from(ARTIFACT_GENERATION_LIST_LIMIT)
                    .expect("artifact generation list limit fits usize")
        {
            return Err(scheduler_error(
                "DTG-ANALYTICS-ARTIFACT-GENERATION-SCAN",
                "result generation scan is saturated and cannot prove reusable pin absence",
            ));
        }
        Ok(reusable)
    }

    async fn read_artifact_bytes(
        &self,
        graph_id: u64,
        job_id: AnalyticsJobId,
        manifest: &ArtifactManifest,
        kind: ShardArtifactKind,
    ) -> Result<Vec<u8>, ClusterAnalyticsError> {
        let placement = {
            let routing = self.routing.read().map_err(|_| {
                scheduler_error("DTG-ANALYTICS-ROUTING", "routing state lock is poisoned")
            })?;
            routing
                .deployment
                .all_shards()
                .iter()
                .find(|placement| placement.shard_id() == manifest.storage_shard_id())
                .ok_or_else(|| {
                    scheduler_error("DTG-ANALYTICS-ARTIFACT", "artifact storage Shard is absent")
                })?
                .clone()
        };
        let context = ShardRequestContext::new(
            graph_id,
            placement.shard_id(),
            placement.placement_epoch(),
            self.next_request_id()?,
            unix_time_ms()?.saturating_add(10_000),
        )
        .map_err(|error| scheduler_error("DTG-ANALYTICS-ARTIFACT", error.to_string()))?;
        let request = GetArtifactGenerationRequest::new(
            context,
            job_id.value(),
            kind,
            manifest.generation(),
            u16::try_from(manifest.chunk_count()).map_err(|_| {
                scheduler_error("DTG-ANALYTICS-ARTIFACT", "artifact chunk count overflow")
            })?,
            manifest.total_bytes(),
            manifest.content_digest(),
        )
        .map_err(|error| scheduler_error("DTG-ANALYTICS-ARTIFACT", error.to_string()))?;
        let mut stream = self
            .shard_client
            .get_artifact_generation(request)
            .await
            .map_err(|error| shard_operation_error("DTG-ANALYTICS-ARTIFACT", error))?;
        let mut bytes =
            Vec::with_capacity(usize::try_from(manifest.total_bytes()).map_err(|_| {
                scheduler_error("DTG-ANALYTICS-ARTIFACT", "artifact size overflow")
            })?);
        while let Some(chunk) = stream.next().await {
            let chunk =
                chunk.map_err(|error| shard_operation_error("DTG-ANALYTICS-ARTIFACT", error))?;
            bytes.extend_from_slice(chunk.payload());
        }
        Ok(bytes)
    }

    async fn propose(
        &self,
        command: JobCommand,
    ) -> Result<cluster_protocol::proto::ProposeAnalyticsJobResponse, ClusterAnalyticsError> {
        let request_id = command.command_id();
        let request = ProposeAnalyticsJobRequest {
            context: Some(
                self.request_context(request_id, unix_time_ms()?.saturating_add(10_000))?,
            ),
            command: command
                .encode()
                .map_err(|error| scheduler_error("DTG-ANALYTICS-LEDGER", error.to_string()))?,
        };
        let mut last_error = None;
        for index in self.meta_client_indexes() {
            let mut client = self.clients[index].clone();
            match tokio::time::timeout(
                META_ENDPOINT_ATTEMPT_TIMEOUT,
                client.propose_analytics_job(request.clone()),
            )
            .await
            {
                Ok(Ok(response)) => {
                    self.remember_meta_client(index);
                    return Ok(response.into_inner());
                }
                Ok(Err(error)) => last_error = Some(error.to_string()),
                Err(_) => last_error = Some("Meta analytics proposal attempt timed out".into()),
            }
        }
        Err(scheduler_error(
            "DTG-ANALYTICS-META-UNAVAILABLE",
            last_error.unwrap_or_else(|| "no Meta endpoint was reachable".into()),
        ))
    }

    async fn get_record(&self, job_id: AnalyticsJobId) -> Result<JobRecord, ClusterAnalyticsError> {
        self.get_record_optional(job_id).await?.ok_or_else(|| {
            scheduler_error("DTG-ANALYTICS-META-UNAVAILABLE", "Meta job read failed")
        })
    }

    async fn get_record_optional(
        &self,
        job_id: AnalyticsJobId,
    ) -> Result<Option<JobRecord>, ClusterAnalyticsError> {
        let request_id = self.next_request_id()?;
        let request = GetAnalyticsJobRequest {
            context: Some(
                self.request_context(request_id, unix_time_ms()?.saturating_add(10_000))?,
            ),
            job_id: job_id.value().to_be_bytes().to_vec(),
        };
        let mut last_error = None;
        let mut saw_not_found = false;
        let mut saw_unavailable = false;
        for index in self.meta_client_indexes() {
            let mut client = self.clients[index].clone();
            match tokio::time::timeout(
                META_ENDPOINT_ATTEMPT_TIMEOUT,
                client.get_analytics_job(request.clone()),
            )
            .await
            {
                Ok(Ok(response)) => {
                    self.remember_meta_client(index);
                    let response = response.into_inner();
                    if crc32fast::hash(&response.record) != response.checksum {
                        return Err(scheduler_error(
                            "DTG-ANALYTICS-META-PROTOCOL",
                            "Meta job checksum is invalid",
                        ));
                    }
                    let (_, record) =
                        LedgerState::decode_job(&response.record).map_err(|error| {
                            scheduler_error("DTG-ANALYTICS-LEDGER", error.to_string())
                        })?;
                    return Ok(Some(record));
                }
                Ok(Err(error)) if error.code() == tonic::Code::NotFound => {
                    saw_not_found = true;
                }
                Ok(Err(error)) => {
                    saw_unavailable = true;
                    last_error = Some(error.to_string());
                }
                Err(_) => {
                    saw_unavailable = true;
                    last_error = Some("Meta analytics read attempt timed out".into());
                }
            }
        }
        if all_meta_clients_explicitly_not_found(saw_not_found, saw_unavailable) {
            return Ok(None);
        }
        Err(scheduler_error(
            "DTG-ANALYTICS-META-UNAVAILABLE",
            last_error.unwrap_or_else(|| "Meta job read failed".into()),
        ))
    }

    fn current_topology_epoch(&self) -> Result<u64, ClusterAnalyticsError> {
        Ok(self
            .routing
            .read()
            .map_err(|_| {
                scheduler_error("DTG-ANALYTICS-ROUTING", "routing state lock is poisoned")
            })?
            .graph
            .topology()
            .epoch())
    }

    fn current_graph_id(&self) -> Result<u64, ClusterAnalyticsError> {
        Ok(self
            .routing
            .read()
            .map_err(|_| {
                scheduler_error("DTG-ANALYTICS-ROUTING", "routing state lock is poisoned")
            })?
            .graph
            .graph_id())
    }

    fn storage_shard(&self) -> Result<ShardPlacement, ClusterAnalyticsError> {
        let routing = self.routing.read().map_err(|_| {
            scheduler_error("DTG-ANALYTICS-ROUTING", "routing state lock is poisoned")
        })?;
        let placement = routing.deployment.all_shards().first().ok_or_else(|| {
            scheduler_error("DTG-ANALYTICS-ROUTING", "deployment has no storage Shard")
        })?;
        Ok(placement.clone())
    }

    fn request_context(
        &self,
        request_id: u128,
        deadline: u64,
    ) -> Result<RequestContext, ClusterAnalyticsError> {
        Ok(RequestContext {
            protocol_version: CLUSTER_PROTOCOL_VERSION,
            cluster_id: self.cluster_id.to_vec(),
            request_id: request_id.to_be_bytes().to_vec(),
            deadline_unix_ms: deadline,
        })
    }

    fn next_request_id(&self) -> Result<u128, ClusterAnalyticsError> {
        let sequence = self.sequence.fetch_add(1, Ordering::Relaxed);
        if sequence == u64::MAX {
            return Err(scheduler_error(
                "DTG-ANALYTICS-SCHEDULER-EXHAUSTED",
                "scheduler request identity space is exhausted",
            ));
        }
        Ok(scheduler_request_id(self.request_nonce, sequence))
    }

    fn command_id(
        &self,
        job_id: AnalyticsJobId,
        revision: u64,
        lease_epoch: u64,
        operation: &[u8],
    ) -> u128 {
        scheduler_command_id(self.gateway_id, job_id, revision, lease_epoch, operation)
    }
}

fn scheduler_command_id(
    gateway_id: u64,
    job_id: AnalyticsJobId,
    revision: u64,
    lease_epoch: u64,
    operation: &[u8],
) -> u128 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"DTGProxy/Analytics/SchedulerCommand/V1");
    hasher.update(&gateway_id.to_be_bytes());
    hasher.update(&job_id.value().to_be_bytes());
    hasher.update(&revision.to_be_bytes());
    hasher.update(&lease_epoch.to_be_bytes());
    hasher.update(operation);
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    u128::from_be_bytes(bytes).max(1)
}

fn scheduler_request_nonce(cluster_id: [u8; 16], gateway_id: u64) -> u64 {
    scheduler_request_nonce_from_parts(
        cluster_id,
        gateway_id,
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos(),
        NEXT_SCHEDULER_REQUEST_NONCE.fetch_add(1, Ordering::Relaxed),
    )
}

fn status_is_gc_lease_conflict(status: &tonic::Status) -> bool {
    status.code() == tonic::Code::ResourceExhausted
        && status
            .metadata()
            .get("dtgproxy-reason")
            .is_some_and(|value| value == "analytics_gc_lease_owned")
}

fn scheduler_request_nonce_from_parts(
    cluster_id: [u8; 16],
    gateway_id: u64,
    process_id: u32,
    started_at_unix_nanos: u128,
    instance_sequence: u64,
) -> u64 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"DTGProxy/Analytics/SchedulerRequestNonce/V1");
    hasher.update(&cluster_id);
    hasher.update(&gateway_id.to_be_bytes());
    hasher.update(&process_id.to_be_bytes());
    hasher.update(&started_at_unix_nanos.to_be_bytes());
    hasher.update(&instance_sequence.to_be_bytes());
    let mut bytes = [0; 8];
    bytes.copy_from_slice(&hasher.finalize().as_bytes()[..8]);
    u64::from_be_bytes(bytes).max(1)
}

const fn scheduler_request_id(request_nonce: u64, sequence: u64) -> u128 {
    (request_nonce as u128) << 64 | sequence as u128
}

fn select_reusable_pinned_generation(
    summaries: impl IntoIterator<Item = (u64, bool, u16, u64, [u8; 32])>,
    expected_chunk_count: u16,
    expected_total_bytes: u64,
    expected_content_digest: [u8; 32],
) -> Option<u64> {
    summaries
        .into_iter()
        .filter_map(
            |(generation, pinned, chunk_count, total_bytes, content_digest)| {
                (generation != 0
                    && pinned
                    && chunk_count == expected_chunk_count
                    && total_bytes == expected_total_bytes
                    && content_digest == expected_content_digest)
                    .then_some(generation)
            },
        )
        .max()
}

fn resolve_generation_created_at(
    summaries: impl IntoIterator<Item = (u64, u64)>,
    target_generation: u64,
    now_unix_ms: u64,
) -> Result<u64, ClusterAnalyticsError> {
    if target_generation == 0 || now_unix_ms == 0 {
        return Err(scheduler_error(
            "DTG-ANALYTICS-ARTIFACT-GENERATION-SCAN",
            "analytics artifact generation scan inputs are invalid",
        ));
    }
    let mut count = 0_usize;
    let mut previous_generation = 0_u64;
    let mut resolved = None;
    for (generation, created_at_unix_ms) in summaries {
        count = count.saturating_add(1);
        if count > ARTIFACT_GENERATION_LIST_LIMIT as usize
            || generation == 0
            || created_at_unix_ms == 0
            || generation <= previous_generation
        {
            return Err(scheduler_error(
                "DTG-ANALYTICS-ARTIFACT-GENERATION-SCAN",
                "analytics artifact generation scan is not canonical",
            ));
        }
        if generation == target_generation {
            resolved = Some(created_at_unix_ms);
        }
        previous_generation = generation;
    }
    if let Some(created_at_unix_ms) = resolved {
        return Ok(created_at_unix_ms);
    }
    if count == ARTIFACT_GENERATION_LIST_LIMIT as usize && previous_generation < target_generation {
        return Err(scheduler_error(
            "DTG-ANALYTICS-ARTIFACT-GENERATION-SCAN",
            "analytics artifact generation scan is saturated before the target generation",
        ));
    }
    Ok(now_unix_ms)
}

fn retention_observation(
    kind: ArtifactKind,
    generation: u64,
    expected_total_bytes: u64,
    created_at_unix_ms: u64,
    pinned: bool,
) -> Result<ArtifactGeneration, ClusterAnalyticsError> {
    let total_bytes = if pinned { expected_total_bytes } else { 1 };
    ArtifactGeneration::new(kind, generation, total_bytes, created_at_unix_ms, pinned)
        .map_err(|error| scheduler_error("DTG-ANALYTICS-RETENTION", error.to_string()))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MetaArtifactProtectionSource {
    Active,
    Tombstone { reclaimed: bool },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct MetaArtifactProtection {
    revision: u64,
    source: MetaArtifactProtectionSource,
    checkpoint: Option<(u32, u64)>,
    result: Option<(u32, u64)>,
}

impl MetaArtifactProtection {
    #[cfg(test)]
    const fn new(
        revision: u64,
        terminal: bool,
        checkpoint: Option<(u32, u64)>,
        result: Option<(u32, u64)>,
    ) -> Self {
        Self::active(revision, terminal, checkpoint, result)
    }

    const fn active(
        revision: u64,
        _terminal: bool,
        checkpoint: Option<(u32, u64)>,
        result: Option<(u32, u64)>,
    ) -> Self {
        Self {
            revision,
            source: MetaArtifactProtectionSource::Active,
            checkpoint,
            result,
        }
    }

    const fn tombstone(revision: u64, reclaimed: bool) -> Self {
        Self {
            revision,
            source: MetaArtifactProtectionSource::Tombstone { reclaimed },
            checkpoint: None,
            result: None,
        }
    }

    fn from_record(record: &JobRecord) -> Self {
        Self::active(
            record.job_revision(),
            record.state().is_terminal(),
            record
                .checkpoint()
                .map(|manifest| (manifest.storage_shard_id(), manifest.generation())),
            record
                .result()
                .map(|manifest| (manifest.storage_shard_id(), manifest.generation())),
        )
    }

    fn from_tombstone(tombstone: &JobTombstone) -> Self {
        Self::tombstone(
            tombstone.final_job_revision(),
            tombstone.artifacts_reclaimed(),
        )
    }

    const fn revision(self) -> u64 {
        self.revision
    }

    const fn is_active(self) -> bool {
        matches!(self.source, MetaArtifactProtectionSource::Active)
    }

    const fn permits_fence_advance(self) -> bool {
        matches!(
            self.source,
            MetaArtifactProtectionSource::Tombstone { reclaimed: false }
        )
    }

    const fn permits_artifact_reclamation(self) -> bool {
        match self.source {
            MetaArtifactProtectionSource::Active => true,
            MetaArtifactProtectionSource::Tombstone { reclaimed } => !reclaimed,
        }
    }

    const fn needs_reclamation_acknowledgement(self) -> bool {
        self.permits_fence_advance()
    }

    fn protects(self, shard_id: u32, kind: ShardArtifactKind, generation: u64) -> bool {
        if !self.is_active() {
            return false;
        }
        let protected = match kind {
            ShardArtifactKind::Checkpoint => self.checkpoint,
            ShardArtifactKind::Result => self.result,
        };
        protected == Some((shard_id, generation))
    }
}

fn insert_tombstone_protection(
    protections: &mut BTreeMap<u128, MetaArtifactProtection>,
    job_id: u128,
    tombstone: MetaArtifactProtection,
) {
    protections.entry(job_id).or_insert(tombstone);
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct OrphanArtifactObservation {
    shard_id: u32,
    job_id: u128,
    kind: ShardArtifactKind,
    generation: u64,
    created_at_unix_ms: u64,
    expected_total_bytes: u64,
    pinned: bool,
}

impl OrphanArtifactObservation {
    fn new(
        shard_id: u32,
        job_id: u128,
        kind: ShardArtifactKind,
        generation: u64,
        created_at_unix_ms: u64,
        pinned: bool,
    ) -> Result<Self, ClusterAnalyticsError> {
        if shard_id == 0 || job_id == 0 || generation == 0 || created_at_unix_ms == 0 {
            return Err(scheduler_error(
                "DTG-ANALYTICS-ARTIFACT-GC-OBSERVATION",
                "analytics artifact orphan observation is invalid",
            ));
        }
        Ok(Self {
            shard_id,
            job_id,
            kind,
            generation,
            created_at_unix_ms,
            expected_total_bytes: 0,
            pinned,
        })
    }

    fn from_summary(
        shard_id: u32,
        summary: &shard_client::ArtifactGenerationSummary,
    ) -> Result<Self, ClusterAnalyticsError> {
        let mut observation = Self::new(
            shard_id,
            summary.job_id(),
            summary.kind(),
            summary.generation(),
            summary.created_at_unix_ms(),
            summary.pinned(),
        )?;
        observation.expected_total_bytes = summary.expected_total_bytes();
        Ok(observation)
    }

    const fn cursor(self) -> (u128, ShardArtifactKind, u64) {
        (self.job_id, self.kind, self.generation)
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct OrphanArtifactKey {
    shard_id: u32,
    job_id: u128,
    kind: ShardArtifactKind,
    generation: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct OrphanArtifactCandidate {
    key: OrphanArtifactKey,
    created_at_unix_ms: u64,
    expected_total_bytes: u64,
    pinned: bool,
    requires_fence_advance: bool,
}

impl OrphanArtifactCandidate {
    const fn shard_id(self) -> u32 {
        self.key.shard_id
    }

    const fn job_id(self) -> u128 {
        self.key.job_id
    }

    const fn kind(self) -> ShardArtifactKind {
        self.key.kind
    }

    const fn generation(self) -> u64 {
        self.key.generation
    }

    const fn requires_fence_advance(self) -> bool {
        self.requires_fence_advance
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct OrphanGcPlanStats {
    protected_current: usize,
    skipped_ttl: usize,
    skipped_pinned_latest: usize,
}

impl OrphanGcPlanStats {
    const fn protected_current(self) -> usize {
        self.protected_current
    }

    const fn skipped_ttl(self) -> usize {
        self.skipped_ttl
    }

    const fn skipped_pinned_latest(self) -> usize {
        self.skipped_pinned_latest
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct OrphanGcPlan {
    candidates: Vec<OrphanArtifactCandidate>,
    acknowledgements: Vec<(u128, u64)>,
    stats: OrphanGcPlanStats,
}

impl OrphanGcPlan {
    fn candidates(&self) -> &[OrphanArtifactCandidate] {
        &self.candidates
    }

    fn acknowledgements(&self) -> &[(u128, u64)] {
        &self.acknowledgements
    }

    const fn stats(&self) -> OrphanGcPlanStats {
        self.stats
    }
}

fn plan_orphan_artifacts(
    observations: &[OrphanArtifactObservation],
    protections: &BTreeMap<u128, MetaArtifactProtection>,
    now_unix_ms: u64,
    orphan_ttl_ms: u64,
) -> Result<OrphanGcPlan, ClusterAnalyticsError> {
    if now_unix_ms == 0 || orphan_ttl_ms == 0 {
        return Err(scheduler_error(
            "DTG-ANALYTICS-ARTIFACT-GC-PLANNER",
            "analytics orphan planner inputs are invalid",
        ));
    }
    let mut previous_by_shard = BTreeMap::<u32, (u128, ShardArtifactKind, u64)>::new();
    let mut highest = BTreeMap::<(u32, u128, ShardArtifactKind), u64>::new();
    for observation in observations {
        let current = observation.cursor();
        if previous_by_shard
            .get(&observation.shard_id)
            .is_some_and(|previous| *previous >= current)
        {
            return Err(scheduler_error(
                "DTG-ANALYTICS-ARTIFACT-GC-PLANNER",
                "analytics orphan observations are not in canonical cursor order",
            ));
        }
        previous_by_shard.insert(observation.shard_id, current);
        highest
            .entry((observation.shard_id, observation.job_id, observation.kind))
            .and_modify(|generation| *generation = (*generation).max(observation.generation))
            .or_insert(observation.generation);
    }

    let mut stats = OrphanGcPlanStats::default();
    let mut candidates = Vec::new();
    for observation in observations {
        if protections
            .get(&observation.job_id)
            .is_some_and(|protection| {
                protection.protects(
                    observation.shard_id,
                    observation.kind,
                    observation.generation,
                )
            })
        {
            stats.protected_current = stats.protected_current.saturating_add(1);
            continue;
        }
        if now_unix_ms.saturating_sub(observation.created_at_unix_ms) < orphan_ttl_ms {
            stats.skipped_ttl = stats.skipped_ttl.saturating_add(1);
            continue;
        }
        let Some(protection) = protections.get(&observation.job_id).copied() else {
            continue;
        };
        if !protection.permits_artifact_reclamation() {
            continue;
        }
        let pinned_latest = observation.pinned
            && highest[&(observation.shard_id, observation.job_id, observation.kind)]
                <= observation.generation;
        let requires_fence_advance = if pinned_latest {
            if protection.permits_fence_advance() {
                true
            } else {
                stats.skipped_pinned_latest = stats.skipped_pinned_latest.saturating_add(1);
                continue;
            }
        } else {
            false
        };
        candidates.push(OrphanArtifactCandidate {
            key: OrphanArtifactKey {
                shard_id: observation.shard_id,
                job_id: observation.job_id,
                kind: observation.kind,
                generation: observation.generation,
            },
            created_at_unix_ms: observation.created_at_unix_ms,
            expected_total_bytes: observation.expected_total_bytes,
            pinned: observation.pinned,
            requires_fence_advance,
        });
    }
    candidates.sort_by_key(|candidate| {
        (
            candidate.created_at_unix_ms,
            candidate.kind(),
            candidate.generation(),
            candidate.shard_id(),
            candidate.job_id(),
        )
    });
    let acknowledgements = protections
        .iter()
        .filter(|(job_id, protection)| {
            protection.needs_reclamation_acknowledgement()
                && !observations
                    .iter()
                    .any(|observation| observation.job_id == **job_id)
        })
        .map(|(job_id, protection)| (*job_id, protection.revision()))
        .collect();
    Ok(OrphanGcPlan {
        candidates,
        acknowledgements,
        stats,
    })
}

#[derive(Default)]
struct GlobalHeadPageGuard {
    after: Option<ArtifactGenerationCursor>,
    done: bool,
}

impl GlobalHeadPageGuard {
    fn observe(
        &mut self,
        cursors: impl IntoIterator<Item = ArtifactGenerationCursor>,
        next: Option<ArtifactGenerationCursor>,
        page_limit: usize,
    ) -> Result<bool, ClusterAnalyticsError> {
        if self.done {
            return Err(scheduler_error(
                "DTG-ANALYTICS-ARTIFACT-GC-PAGE",
                "analytics artifact page loop continued after termination",
            ));
        }
        let mut last = self.after;
        let mut count = 0_usize;
        for cursor in cursors {
            if last.is_some_and(|previous| cursor <= previous) {
                return Err(scheduler_error(
                    "DTG-ANALYTICS-ARTIFACT-GC-PAGE",
                    "analytics artifact page cursor did not advance",
                ));
            }
            last = Some(cursor);
            count = count.saturating_add(1);
        }
        if count > page_limit {
            return Err(scheduler_error(
                "DTG-ANALYTICS-ARTIFACT-GC-PAGE",
                "analytics artifact page exceeds the requested limit",
            ));
        }
        if count == page_limit {
            if next.is_none() || last != next {
                return Err(scheduler_error(
                    "DTG-ANALYTICS-ARTIFACT-GC-PAGE",
                    "analytics artifact page next cursor is not canonical",
                ));
            }
        } else if next.is_some() {
            return Err(scheduler_error(
                "DTG-ANALYTICS-ARTIFACT-GC-PAGE",
                "short analytics artifact page unexpectedly has a next cursor",
            ));
        }
        if let Some(next) = next {
            self.after = Some(next);
            Ok(true)
        } else {
            self.done = true;
            Ok(false)
        }
    }

    #[cfg(test)]
    const fn is_done(&self) -> bool {
        self.done
    }
}

fn all_meta_clients_explicitly_not_found(saw_not_found: bool, saw_unavailable: bool) -> bool {
    saw_not_found && !saw_unavailable
}

fn decode_wire_job_id(bytes: &[u8], field: &str) -> Result<AnalyticsJobId, ClusterAnalyticsError> {
    let raw: [u8; 16] = bytes.try_into().map_err(|_| {
        scheduler_error(
            "DTG-ANALYTICS-META-PROTOCOL",
            format!("{field} must contain 16 bytes"),
        )
    })?;
    AnalyticsJobId::new(u128::from_be_bytes(raw))
        .map_err(|error| scheduler_error("DTG-ANALYTICS-META-PROTOCOL", error.to_string()))
}

const fn orphan_revision_matches(expected_revision: u64, fresh_revision: u64) -> bool {
    expected_revision == fresh_revision
}

fn projection_identity(spec: &analytics_ledger::JobSpec) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"DTGProxy/Analytics/Projection/V1");
    hasher.update(&spec.graph_id().to_be_bytes());
    hasher.update(&spec.topology_epoch().to_be_bytes());
    hasher.update(&spec.schema_version().to_be_bytes());
    hasher.update(&spec.backend_generation().to_be_bytes());
    hasher.update(&spec.transaction_time().physical_micros().to_be_bytes());
    hasher.update(&spec.transaction_time().logical().to_be_bytes());
    hasher.update(format!("{:?}", spec.projection()).as_bytes());
    *hasher.finalize().as_bytes()
}

fn unix_time_ms() -> Result<u64, ClusterAnalyticsError> {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| {
                scheduler_error("DTG-ANALYTICS-CLOCK", "system clock is before Unix epoch")
            })?
            .as_millis(),
    )
    .map_err(|_| scheduler_error("DTG-ANALYTICS-CLOCK", "system clock overflowed"))
}

fn gc_delete_fence_allows(
    expected_job_revision: u64,
    expected_manifest: &ArtifactManifest,
    fresh_job_revision: u64,
    fresh_manifest: Option<&ArtifactManifest>,
    target_generation: u64,
) -> bool {
    fresh_job_revision == expected_job_revision
        && fresh_manifest == Some(expected_manifest)
        && target_generation < expected_manifest.generation()
}

fn record_protects_orphan_candidate(
    record: &JobRecord,
    candidate: OrphanArtifactCandidate,
) -> bool {
    let manifest = match candidate.kind() {
        ShardArtifactKind::Checkpoint => record.checkpoint(),
        ShardArtifactKind::Result => record.result(),
    };
    manifest.is_some_and(|manifest| {
        manifest.storage_shard_id() == candidate.shard_id()
            && manifest.generation() == candidate.generation()
    })
}

fn artifact_request_id(
    job_id: AnalyticsJobId,
    generation: u64,
    kind: ShardArtifactKind,
    ordinal: u64,
    request_seed: u128,
) -> u128 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"DTGProxy/ArtifactRequest/Latest");
    hasher.update(&job_id.value().to_be_bytes());
    hasher.update(&generation.to_be_bytes());
    hasher.update(&[match kind {
        ShardArtifactKind::Checkpoint => 1,
        ShardArtifactKind::Result => 2,
    }]);
    hasher.update(&ordinal.to_be_bytes());
    hasher.update(&request_seed.to_be_bytes());
    u128::from_be_bytes(
        hasher.finalize().as_bytes()[..16]
            .try_into()
            .expect("fixed artifact request digest"),
    )
    .max(1)
}

fn scheduler_error(code: &'static str, message: impl Into<String>) -> ClusterAnalyticsError {
    ClusterAnalyticsError::new(code, message)
}

fn shard_operation_error(
    operation_code: &'static str,
    error: ShardClientError,
) -> ClusterAnalyticsError {
    if matches!(
        error,
        ShardClientError::DeadlineExpired
            | ShardClientError::NoLeader { .. }
            | ShardClientError::NotLeader { .. }
            | ShardClientError::Replication(_)
            | ShardClientError::ReadBarrier(_)
    ) {
        return scheduler_error(
            ANALYTICS_RETRYABLE_INFRASTRUCTURE_CODE,
            format!("{operation_code}: {error}"),
        );
    }
    scheduler_error(operation_code, error.to_string())
}

fn is_retryable_infrastructure_error(error: &ClusterAnalyticsError) -> bool {
    error.code() == ANALYTICS_RETRYABLE_INFRASTRUCTURE_CODE
}

#[must_use]
pub fn process_stop_fault(point: AnalyticsFaultPoint) -> ClusterAnalyticsError {
    ClusterAnalyticsError::new(
        ANALYTICS_PROCESS_STOP_FAULT_CODE,
        format!("simulated Gateway process stop at {point:?}"),
    )
}

#[must_use]
fn is_process_stop_fault(error: &ClusterAnalyticsError) -> bool {
    error.code() == ANALYTICS_PROCESS_STOP_FAULT_CODE
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::{
        ANALYTICS_PROCESS_STOP_FAULT_CODE, AnalyticsFaultInjector, AnalyticsFaultPoint,
        AnalyticsGcLease, GlobalHeadPageGuard, MetaArtifactProtection, OrphanArtifactObservation,
        all_meta_clients_explicitly_not_found, apply_gc_lease_renewal, artifact_request_id,
        gc_delete_fence_allows, insert_tombstone_protection, is_process_stop_fault,
        is_retryable_infrastructure_error, orphan_revision_matches, plan_orphan_artifacts,
        process_stop_fault, resolve_generation_created_at, retention_observation,
        scheduler_command_id, scheduler_request_id, scheduler_request_nonce_from_parts,
        select_reusable_pinned_generation, shard_operation_error, status_is_gc_lease_conflict,
    };
    use analytics_ledger::{
        AnalyticsJobId, ArtifactKind as LedgerArtifactKind, ArtifactManifest, NextExecutionStage,
    };
    use procedure_runtime::ClusterAnalyticsError;
    use shard_client::{ArtifactKind, ShardClientError};

    fn observation(
        shard_id: u32,
        job_id: u128,
        kind: ArtifactKind,
        generation: u64,
        created_at_unix_ms: u64,
        pinned: bool,
    ) -> OrphanArtifactObservation {
        OrphanArtifactObservation::new(
            shard_id,
            job_id,
            kind,
            generation,
            created_at_unix_ms,
            pinned,
        )
        .unwrap()
    }

    #[test]
    fn orphan_planner_protects_current_manifest_and_orders_ttl_candidates_oldest_first() {
        let now = 1_000;
        let current = BTreeMap::from([
            (7, MetaArtifactProtection::new(7, false, Some((3, 2)), None)),
            (8, MetaArtifactProtection::tombstone(1, false)),
        ]);
        let plan = plan_orphan_artifacts(
            &[
                observation(3, 7, ArtifactKind::Checkpoint, 1, 1, false),
                observation(3, 7, ArtifactKind::Checkpoint, 2, 1, false),
                observation(3, 8, ArtifactKind::Result, 1, 2, false),
            ],
            &current,
            now,
            100,
        )
        .unwrap();
        assert_eq!(plan.candidates().len(), 2);
        assert_eq!(plan.candidates()[0].job_id(), 7);
        assert_eq!(plan.candidates()[0].generation(), 1);
        assert_eq!(plan.candidates()[1].job_id(), 8);
        assert_eq!(plan.stats().protected_current(), 1);
    }

    #[test]
    fn tombstone_orphan_planner_protects_missing_jobs_without_tombstones() {
        let plan = plan_orphan_artifacts(
            &[
                observation(3, 99, ArtifactKind::Checkpoint, 1, 950, false),
                observation(3, 100, ArtifactKind::Checkpoint, 1, 800, false),
            ],
            &BTreeMap::new(),
            1_000,
            100,
        )
        .unwrap();
        assert!(plan.candidates().is_empty());
        assert_eq!(plan.stats().skipped_ttl(), 1);
    }

    #[test]
    fn orphan_planner_skips_latest_pinned_but_reclaims_pinned_superseded() {
        let protections =
            BTreeMap::from([(7, MetaArtifactProtection::active(1, false, None, None))]);
        let plan = plan_orphan_artifacts(
            &[
                observation(3, 7, ArtifactKind::Result, 1, 1, true),
                observation(3, 7, ArtifactKind::Result, 2, 2, true),
            ],
            &protections,
            1_000,
            100,
        )
        .unwrap();
        assert_eq!(plan.candidates().len(), 1);
        assert_eq!(plan.candidates()[0].generation(), 1);
        assert_eq!(plan.stats().skipped_pinned_latest(), 1);
    }

    #[test]
    fn tombstone_orphan_planner_advances_the_fence_for_latest_pinned_artifacts() {
        let protections = BTreeMap::from([(7, MetaArtifactProtection::tombstone(9, false))]);
        let plan = plan_orphan_artifacts(
            &[observation(3, 7, ArtifactKind::Result, 5, 1, true)],
            &protections,
            1_000,
            100,
        )
        .unwrap();
        assert_eq!(plan.candidates().len(), 1);
        assert!(plan.candidates()[0].requires_fence_advance());
        assert_eq!(plan.stats().skipped_pinned_latest(), 0);
    }

    #[test]
    fn tombstone_active_job_protection_overrides_a_duplicate_tombstone() {
        let mut protections =
            BTreeMap::from([(7, MetaArtifactProtection::active(9, false, None, None))]);
        insert_tombstone_protection(
            &mut protections,
            7,
            MetaArtifactProtection::tombstone(9, false),
        );
        let plan = plan_orphan_artifacts(
            &[observation(3, 7, ArtifactKind::Result, 5, 1, true)],
            &protections,
            1_000,
            100,
        )
        .unwrap();
        assert!(plan.candidates().is_empty());
    }

    #[test]
    fn tombstone_empty_complete_scan_produces_an_acknowledgement_candidate() {
        let protections = BTreeMap::from([(7, MetaArtifactProtection::tombstone(9, false))]);
        let plan = plan_orphan_artifacts(&[], &protections, 1_000, 100).unwrap();
        assert_eq!(plan.acknowledgements(), &[(7, 9)]);
    }

    #[test]
    fn orphan_planner_orders_candidates_deterministically_across_shards() {
        let protections = BTreeMap::from([
            (8, MetaArtifactProtection::active(1, false, None, None)),
            (9, MetaArtifactProtection::active(1, false, None, None)),
        ]);
        let plan = plan_orphan_artifacts(
            &[
                observation(1, 9, ArtifactKind::Result, 1, 10, false),
                observation(2, 8, ArtifactKind::Checkpoint, 1, 10, false),
                observation(2, 9, ArtifactKind::Result, 2, 10, false),
            ],
            &protections,
            1_000,
            100,
        )
        .unwrap();
        assert_eq!(
            plan.candidates()
                .iter()
                .map(|candidate| (
                    candidate.shard_id(),
                    candidate.job_id(),
                    candidate.kind(),
                    candidate.generation()
                ))
                .collect::<Vec<_>>(),
            [
                (2, 8, ArtifactKind::Checkpoint, 1),
                (1, 9, ArtifactKind::Result, 1),
                (2, 9, ArtifactKind::Result, 2),
            ]
        );
    }

    #[test]
    fn global_head_page_guard_advances_and_terminates_on_empty_page() {
        let first =
            shard_client::ArtifactGenerationCursor::new(7, ArtifactKind::Result, 1).unwrap();
        let second =
            shard_client::ArtifactGenerationCursor::new(7, ArtifactKind::Result, 2).unwrap();
        let mut guard = GlobalHeadPageGuard::default();
        assert!(guard.observe([first], Some(first), 1).unwrap());
        assert!(guard.observe([second], Some(second), 1).unwrap());
        assert!(!guard.observe([], None, 1).unwrap());
        assert!(guard.is_done());
    }

    #[test]
    fn global_head_page_guard_rejects_non_advancing_cursor() {
        let cursor =
            shard_client::ArtifactGenerationCursor::new(7, ArtifactKind::Result, 1).unwrap();
        let mut guard = GlobalHeadPageGuard::default();
        assert!(guard.observe([cursor], Some(cursor), 1).is_ok());
        let error = guard.observe([cursor], Some(cursor), 1).unwrap_err();
        assert_eq!(error.code(), "DTG-ANALYTICS-ARTIFACT-GC-PAGE");
    }

    #[test]
    fn global_head_page_guard_rejects_full_page_without_next_and_short_page_with_next() {
        let first =
            shard_client::ArtifactGenerationCursor::new(7, ArtifactKind::Result, 1).unwrap();
        let mut full_page = GlobalHeadPageGuard::default();
        assert!(full_page.observe([first], None, 1).is_err());

        let mut short_page = GlobalHeadPageGuard::default();
        assert!(short_page.observe([], Some(first), 1).is_err());
    }

    #[test]
    fn meta_optional_read_requires_every_client_to_confirm_not_found() {
        assert!(all_meta_clients_explicitly_not_found(true, false));
        assert!(!all_meta_clients_explicitly_not_found(true, true));
    }

    #[test]
    fn orphan_revision_fence_rejects_a_changed_meta_record() {
        assert!(orphan_revision_matches(7, 7));
        assert!(!orphan_revision_matches(7, 8));
    }

    struct FailOnceFaultInjector {
        point: AnalyticsFaultPoint,
        armed: AtomicBool,
    }

    impl AnalyticsFaultInjector for FailOnceFaultInjector {
        fn check(
            &self,
            point: AnalyticsFaultPoint,
        ) -> Result<(), procedure_runtime::ClusterAnalyticsError> {
            if point == self.point && self.armed.swap(false, Ordering::AcqRel) {
                return Err(procedure_runtime::ClusterAnalyticsError::new(
                    "DTG-ANALYTICS-FAULT-INJECTED",
                    format!("injected {point:?}"),
                ));
            }
            Ok(())
        }
    }

    #[test]
    fn scheduler_command_identity_is_fenced_by_revision_and_operation() {
        let job = AnalyticsJobId::new(7).unwrap();
        let claim = scheduler_command_id(11, job, 1, 0, b"claim");
        let repeat = scheduler_command_id(11, job, 1, 0, b"claim");
        let changed_revision = scheduler_command_id(11, job, 2, 0, b"claim");
        let changed_operation = scheduler_command_id(11, job, 1, 0, b"begin");
        assert_ne!(claim, 0);
        assert_eq!(claim, repeat);
        assert_ne!(claim, changed_revision);
        assert_ne!(claim, changed_operation);
    }

    #[test]
    fn scheduler_request_identity_changes_across_same_gateway_process_instances() {
        let first_nonce = scheduler_request_nonce_from_parts([7; 16], 11, 100, 1_000, 1);
        let restarted_nonce = scheduler_request_nonce_from_parts([7; 16], 11, 101, 2_000, 1);

        assert_ne!(first_nonce, restarted_nonce);
        assert_ne!(
            scheduler_request_id(first_nonce, 1),
            scheduler_request_id(restarted_nonce, 1),
            "a stable Gateway restart must not reuse a durable Shard request ID"
        );
    }

    #[test]
    fn gc_lease_conflict_metric_requires_the_dedicated_meta_reason() {
        let mut owned = tonic::Status::resource_exhausted("lease owned");
        owned.metadata_mut().insert(
            "dtgproxy-reason",
            tonic::metadata::MetadataValue::from_static("analytics_gc_lease_owned"),
        );
        let mut exhausted = tonic::Status::resource_exhausted("epoch exhausted");
        exhausted.metadata_mut().insert(
            "dtgproxy-reason",
            tonic::metadata::MetadataValue::from_static("analytics_gc_epoch_exhausted"),
        );

        assert!(status_is_gc_lease_conflict(&owned));
        assert!(!status_is_gc_lease_conflict(&exhausted));
        assert!(!status_is_gc_lease_conflict(&tonic::Status::unavailable(
            "Meta unavailable"
        )));
    }

    #[test]
    fn gc_lease_term_change_refreshes_epoch_and_next_request_deadline() {
        let now = 8_500;
        let mut current = AnalyticsGcLease {
            epoch: 7,
            expires_unix_ms: 10_000,
        };
        assert!(current.requires_renewal(now));

        apply_gc_lease_renewal(
            &mut current,
            AnalyticsGcLease {
                epoch: 9,
                expires_unix_ms: 20_000,
            },
        )
        .unwrap();

        assert_eq!(current.epoch, 9);
        assert_eq!(current.request_deadline(now).unwrap(), 18_500);
        let regressed = apply_gc_lease_renewal(
            &mut current,
            AnalyticsGcLease {
                epoch: 8,
                expires_unix_ms: 30_000,
            },
        )
        .unwrap_err();
        assert_eq!(regressed.code(), "DTG-ANALYTICS-GC-LEASE");
    }

    #[test]
    fn artifact_request_identity_is_unique_across_generations_and_kinds() {
        let job = AnalyticsJobId::new(7).unwrap();
        let checkpoint_one = artifact_request_id(job, 1, ArtifactKind::Checkpoint, 0, 11);
        let checkpoint_two = artifact_request_id(job, 2, ArtifactKind::Checkpoint, 0, 12);
        let result_one = artifact_request_id(job, 1, ArtifactKind::Result, 0, 11);
        assert_ne!(checkpoint_one, checkpoint_two);
        assert_ne!(checkpoint_one, result_one);
        assert_ne!(checkpoint_one, 0);
    }

    #[test]
    fn reusable_result_generation_requires_an_exact_complete_pin() {
        let digest = [7; 32];
        let summaries = [
            (1, true, 2, 100, digest),
            (2, false, 2, 100, digest),
            (3, true, 3, 100, digest),
            (4, true, 2, 101, digest),
            (5, true, 2, 100, [8; 32]),
            (6, true, 2, 100, digest),
        ];
        assert_eq!(
            select_reusable_pinned_generation(summaries, 2, 100, digest),
            Some(6)
        );
        assert_eq!(
            select_reusable_pinned_generation([(1, false, 2, 100, digest)], 2, 100, digest),
            None
        );
    }

    #[test]
    fn generation_creation_time_resolution_reuses_existing_and_fails_closed_when_saturated() {
        let now = 1_725_000_000_999;
        assert_eq!(
            resolve_generation_created_at([(1, 101), (3, 303)], 3, now).unwrap(),
            303
        );
        assert_eq!(
            resolve_generation_created_at([(1, 101), (3, 303)], 2, now).unwrap(),
            now
        );

        let noncanonical = resolve_generation_created_at([(1, 101), (1, 102)], 3, now).unwrap_err();
        assert_eq!(
            noncanonical.code(),
            "DTG-ANALYTICS-ARTIFACT-GENERATION-SCAN"
        );

        let saturated = (1_u64..=4_096)
            .map(|generation| (generation, generation + 100))
            .collect::<Vec<_>>();
        let ambiguous = resolve_generation_created_at(saturated, 4_097, now).unwrap_err();
        assert_eq!(ambiguous.code(), "DTG-ANALYTICS-ARTIFACT-GENERATION-SCAN");
    }

    #[test]
    fn retention_observation_uses_persisted_time_for_pinned_and_unpinned_generations() {
        let pinned = retention_observation(
            LedgerArtifactKind::Checkpoint,
            7,
            512,
            1_725_000_000_123,
            true,
        )
        .unwrap();
        let unpinned = retention_observation(
            LedgerArtifactKind::Result,
            8,
            1_024,
            1_725_000_000_456,
            false,
        )
        .unwrap();

        assert_eq!(pinned.created_at_unix_ms(), 1_725_000_000_123);
        assert_eq!(unpinned.created_at_unix_ms(), 1_725_000_000_456);
        assert_eq!(pinned.total_bytes(), 512);
        assert_eq!(unpinned.total_bytes(), 1);
    }

    #[test]
    fn gc_delete_requires_an_unchanged_meta_manifest_and_older_target() {
        let manifest = ArtifactManifest::new(
            LedgerArtifactKind::Checkpoint,
            3,
            5,
            1,
            7,
            [11; 32],
            [13; 32],
            "provider-1",
            "algorithm-1",
            NextExecutionStage::Provider { completed_units: 2 },
            BTreeMap::from([(5, 17)]),
        )
        .unwrap();
        let replacement = ArtifactManifest::new(
            LedgerArtifactKind::Checkpoint,
            4,
            5,
            1,
            7,
            [19; 32],
            [13; 32],
            "provider-1",
            "algorithm-1",
            NextExecutionStage::Provider { completed_units: 3 },
            BTreeMap::from([(5, 17)]),
        )
        .unwrap();

        assert!(gc_delete_fence_allows(7, &manifest, 7, Some(&manifest), 2));
        assert!(!gc_delete_fence_allows(7, &manifest, 8, Some(&manifest), 2));
        assert!(!gc_delete_fence_allows(
            7,
            &manifest,
            7,
            Some(&replacement),
            2
        ));
        assert!(!gc_delete_fence_allows(7, &manifest, 7, Some(&manifest), 3));
        assert!(!gc_delete_fence_allows(7, &manifest, 7, None, 2));
    }

    #[test]
    fn fault_injector_fails_the_selected_boundary_exactly_once() {
        let injector = FailOnceFaultInjector {
            point: AnalyticsFaultPoint::CheckpointCas,
            armed: AtomicBool::new(true),
        };
        assert!(injector.check(AnalyticsFaultPoint::LeaseRenew).is_ok());
        assert!(injector.check(AnalyticsFaultPoint::CheckpointCas).is_err());
        assert!(injector.check(AnalyticsFaultPoint::CheckpointCas).is_ok());
    }

    #[test]
    fn gc_fault_injector_distinguishes_fence_delete_and_ack_boundaries() {
        for point in [
            AnalyticsFaultPoint::GcAfterFenceAdvance,
            AnalyticsFaultPoint::GcBeforeDelete,
            AnalyticsFaultPoint::GcBeforeAcknowledgement,
        ] {
            let injector = FailOnceFaultInjector {
                point,
                armed: AtomicBool::new(true),
            };
            assert!(injector.check(point).is_err());
            assert!(injector.check(point).is_ok());
        }
    }

    #[test]
    fn gc_lease_renewal_margin_prevents_expiry_between_scan_and_delete() {
        let lease = AnalyticsGcLease {
            epoch: 7,
            expires_unix_ms: 10_000,
        };
        assert!(!lease.requires_renewal(7_999));
        assert!(lease.requires_renewal(8_000));
        assert!(lease.requires_renewal(10_000));
    }

    #[test]
    fn process_stop_fault_is_distinguished_from_a_terminal_job_failure() {
        let error = process_stop_fault(AnalyticsFaultPoint::CheckpointUpload);
        assert_eq!(error.code(), ANALYTICS_PROCESS_STOP_FAULT_CODE);
        assert!(is_process_stop_fault(&error));
        assert!(!is_process_stop_fault(&ClusterAnalyticsError::new(
            "DTG-ANALYTICS-FAULT-INJECTED",
            "ordinary injected operation failure",
        )));
    }

    #[test]
    fn only_transient_shard_failures_are_retryable_infrastructure_errors() {
        let unavailable = shard_operation_error(
            "DTG-ANALYTICS-ARTIFACT",
            ShardClientError::Replication("transport closed".into()),
        );
        assert!(is_retryable_infrastructure_error(&unavailable));

        let corruption = shard_operation_error(
            "DTG-ANALYTICS-ARTIFACT",
            ShardClientError::ArtifactCorruption("digest mismatch".into()),
        );
        assert!(!is_retryable_infrastructure_error(&corruption));
        assert_eq!(corruption.code(), "DTG-ANALYTICS-ARTIFACT");
    }
}
