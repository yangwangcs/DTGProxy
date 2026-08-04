use std::io::{self, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{SyncSender, sync_channel};
use std::time::Instant;

const HISTOGRAM_BUCKETS: usize = 64;
const REQUEST_STAGES: usize = 10;
const REQUEST_DETAILS: usize = 50;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(usize)]
pub enum RequestStage {
    BoltDecode,
    GatewayCompile,
    GatewayPlan,
    GatewayInternalRpc,
    GatewayLocalExecution,
    BoltEncode,
    DataValidation,
    DataRouting,
    DataRaftApply,
    DataProviderExecution,
}

impl RequestStage {
    const ALL: [Self; REQUEST_STAGES] = [
        Self::BoltDecode,
        Self::GatewayCompile,
        Self::GatewayPlan,
        Self::GatewayInternalRpc,
        Self::GatewayLocalExecution,
        Self::BoltEncode,
        Self::DataValidation,
        Self::DataRouting,
        Self::DataRaftApply,
        Self::DataProviderExecution,
    ];

    const fn index(self) -> usize {
        self as usize
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::BoltDecode => "bolt_decode",
            Self::GatewayCompile => "gateway_compile",
            Self::GatewayPlan => "gateway_plan",
            Self::GatewayInternalRpc => "gateway_internal_rpc",
            Self::GatewayLocalExecution => "gateway_local_execution",
            Self::BoltEncode => "bolt_encode",
            Self::DataValidation => "data_validation",
            Self::DataRouting => "data_routing",
            Self::DataRaftApply => "data_raft_apply",
            Self::DataProviderExecution => "data_provider_execution",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(usize)]
pub enum RequestDetail {
    GatewayQueryRequestEncode,
    GatewayQueryResponseCollect,
    GatewayQueryResponseDecode,
    GatewayQueryLocalMaterialize,
    GatewayDataApplyRpc,
    DataRouteLockWait,
    DataRouteLookup,
    DataRaftPropose,
    DataRaftDriveReady,
    DataReadViewCacheHit,
    DataReadViewCacheMiss,
    DataReadViewOpen,
    DataTemporalPointEvaluation,
    DataTemporalScanIdCollection,
    DataTemporalScanVisibility,
    DataRaftLockWait,
    DataRaftBatchAdmission,
    DataRaftBatchQueue,
    DataRaftBlockingDispatch,
    DataAdjacencyCacheHit,
    DataAdjacencyCacheMiss,
    DataAdjacencyBackendExpand,
    DataSnapshotCsrCacheHit,
    DataSnapshotCsrCacheMiss,
    DataSnapshotCsrBuild,
    GatewayQuerySessionSubmit,
    GatewayQuerySessionResponseWait,
    DataGatewaySessionExecution,
    GatewayQueryPipelineSubmit,
    GatewayQueryPipelineResponseWait,
    BoltReadPipelineEnqueueWait,
    BoltReadPipelineExecutionWait,
    BoltReadPipelineOrderedWriteWait,
    GatewayQueryPipelineCreditWait,
    DataGatewayPipelineDispatchWait,
    DataGatewayPipelineCompletionSendWait,
    DataGatewayPipelineCompletionFrame,
    GatewayQueryPipelineResponseTransportWait,
    GatewayQueryPipelineResponseDispatchWait,
    DataGatewayPipelineRequestTransportWait,
    GatewayQueryPipelineWriterWait,
    DataSnapshotIngestAdmission,
    DataSnapshotIngestReceiptLookup,
    GatewayPlanRouting,
    GatewayTransportWait,
    DataValidation,
    DataExecution,
    DataRaftQueue,
    DataRaftApply,
    DataProviderApply,
}

impl RequestDetail {
    const ALL: [Self; REQUEST_DETAILS] = [
        Self::GatewayQueryRequestEncode,
        Self::GatewayQueryResponseCollect,
        Self::GatewayQueryResponseDecode,
        Self::GatewayQueryLocalMaterialize,
        Self::GatewayDataApplyRpc,
        Self::DataRouteLockWait,
        Self::DataRouteLookup,
        Self::DataRaftPropose,
        Self::DataRaftDriveReady,
        Self::DataReadViewCacheHit,
        Self::DataReadViewCacheMiss,
        Self::DataReadViewOpen,
        Self::DataTemporalPointEvaluation,
        Self::DataTemporalScanIdCollection,
        Self::DataTemporalScanVisibility,
        Self::DataRaftLockWait,
        Self::DataRaftBatchAdmission,
        Self::DataRaftBatchQueue,
        Self::DataRaftBlockingDispatch,
        Self::DataAdjacencyCacheHit,
        Self::DataAdjacencyCacheMiss,
        Self::DataAdjacencyBackendExpand,
        Self::DataSnapshotCsrCacheHit,
        Self::DataSnapshotCsrCacheMiss,
        Self::DataSnapshotCsrBuild,
        Self::GatewayQuerySessionSubmit,
        Self::GatewayQuerySessionResponseWait,
        Self::DataGatewaySessionExecution,
        Self::GatewayQueryPipelineSubmit,
        Self::GatewayQueryPipelineResponseWait,
        Self::BoltReadPipelineEnqueueWait,
        Self::BoltReadPipelineExecutionWait,
        Self::BoltReadPipelineOrderedWriteWait,
        Self::GatewayQueryPipelineCreditWait,
        Self::DataGatewayPipelineDispatchWait,
        Self::DataGatewayPipelineCompletionSendWait,
        Self::DataGatewayPipelineCompletionFrame,
        Self::GatewayQueryPipelineResponseTransportWait,
        Self::GatewayQueryPipelineResponseDispatchWait,
        Self::DataGatewayPipelineRequestTransportWait,
        Self::GatewayQueryPipelineWriterWait,
        Self::DataSnapshotIngestAdmission,
        Self::DataSnapshotIngestReceiptLookup,
        Self::GatewayPlanRouting,
        Self::GatewayTransportWait,
        Self::DataValidation,
        Self::DataExecution,
        Self::DataRaftQueue,
        Self::DataRaftApply,
        Self::DataProviderApply,
    ];

    const fn index(self) -> usize {
        self as usize
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::GatewayQueryRequestEncode => "gateway_query_request_encode",
            Self::GatewayQueryResponseCollect => "gateway_query_response_collect",
            Self::GatewayQueryResponseDecode => "gateway_query_response_decode",
            Self::GatewayQueryLocalMaterialize => "gateway_query_local_materialize",
            Self::GatewayDataApplyRpc => "gateway_data_apply_rpc",
            Self::DataRouteLockWait => "data_route_lock_wait",
            Self::DataRouteLookup => "data_route_lookup",
            Self::DataRaftPropose => "data_raft_propose",
            Self::DataRaftDriveReady => "data_raft_drive_ready",
            Self::DataReadViewCacheHit => "data_read_view_cache_hit",
            Self::DataReadViewCacheMiss => "data_read_view_cache_miss",
            Self::DataReadViewOpen => "data_read_view_open",
            Self::DataTemporalPointEvaluation => "data_temporal_point_evaluation",
            Self::DataTemporalScanIdCollection => "data_temporal_scan_id_collection",
            Self::DataTemporalScanVisibility => "data_temporal_scan_visibility",
            Self::DataRaftLockWait => "data_raft_lock_wait",
            Self::DataRaftBatchAdmission => "data_raft_batch_admission",
            Self::DataRaftBatchQueue => "data_raft_batch_queue",
            Self::DataRaftBlockingDispatch => "data_raft_blocking_dispatch",
            Self::DataAdjacencyCacheHit => "data_adjacency_cache_hit",
            Self::DataAdjacencyCacheMiss => "data_adjacency_cache_miss",
            Self::DataAdjacencyBackendExpand => "data_adjacency_backend_expand",
            Self::DataSnapshotCsrCacheHit => "data_snapshot_csr_cache_hit",
            Self::DataSnapshotCsrCacheMiss => "data_snapshot_csr_cache_miss",
            Self::DataSnapshotCsrBuild => "data_snapshot_csr_build",
            Self::GatewayQuerySessionSubmit => "gateway_query_session_submit",
            Self::GatewayQuerySessionResponseWait => "gateway_query_session_response_wait",
            Self::DataGatewaySessionExecution => "data_gateway_session_execution",
            Self::GatewayQueryPipelineSubmit => "gateway_query_pipeline_submit",
            Self::GatewayQueryPipelineResponseWait => "gateway_query_pipeline_response_wait",
            Self::BoltReadPipelineEnqueueWait => "bolt_read_pipeline_enqueue_wait",
            Self::BoltReadPipelineExecutionWait => "bolt_read_pipeline_execution_wait",
            Self::BoltReadPipelineOrderedWriteWait => "bolt_read_pipeline_ordered_write_wait",
            Self::GatewayQueryPipelineCreditWait => "gateway_query_pipeline_credit_wait",
            Self::DataGatewayPipelineDispatchWait => "data_gateway_pipeline_dispatch_wait",
            Self::DataGatewayPipelineCompletionSendWait => {
                "data_gateway_pipeline_completion_send_wait"
            }
            Self::DataGatewayPipelineCompletionFrame => "data_gateway_pipeline_completion_frame",
            Self::GatewayQueryPipelineResponseTransportWait => {
                "gateway_query_pipeline_response_transport_wait"
            }
            Self::GatewayQueryPipelineResponseDispatchWait => {
                "gateway_query_pipeline_response_dispatch_wait"
            }
            Self::DataGatewayPipelineRequestTransportWait => {
                "data_gateway_pipeline_request_transport_wait"
            }
            Self::GatewayQueryPipelineWriterWait => "gateway_query_pipeline_writer_wait",
            Self::DataSnapshotIngestAdmission => "data_snapshot_ingest_admission",
            Self::DataSnapshotIngestReceiptLookup => "data_snapshot_ingest_receipt_lookup",
            Self::GatewayPlanRouting => "gateway_plan_routing",
            Self::GatewayTransportWait => "gateway_transport_wait",
            Self::DataValidation => "data_validation",
            Self::DataExecution => "data_execution",
            Self::DataRaftQueue => "data_raft_queue",
            Self::DataRaftApply => "data_raft_apply",
            Self::DataProviderApply => "data_provider_apply",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StageOutcome {
    Success,
    Error,
    Cancelled,
}

struct StageMetrics {
    buckets: [AtomicU64; HISTOGRAM_BUCKETS],
    success: AtomicU64,
    error: AtomicU64,
    cancelled: AtomicU64,
    total_nanoseconds: AtomicU64,
    max_nanoseconds: AtomicU64,
}

impl Default for StageMetrics {
    fn default() -> Self {
        Self {
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            success: AtomicU64::new(0),
            error: AtomicU64::new(0),
            cancelled: AtomicU64::new(0),
            total_nanoseconds: AtomicU64::new(0),
            max_nanoseconds: AtomicU64::new(0),
        }
    }
}

impl StageMetrics {
    fn record(&self, outcome: StageOutcome, nanoseconds: u64) {
        let bucket = 63_usize.saturating_sub(nanoseconds.max(1).leading_zeros() as usize);
        self.buckets[bucket].fetch_add(1, Ordering::Relaxed);
        match outcome {
            StageOutcome::Success => &self.success,
            StageOutcome::Error => &self.error,
            StageOutcome::Cancelled => &self.cancelled,
        }
        .fetch_add(1, Ordering::Relaxed);
        saturating_fetch_add(&self.total_nanoseconds, nanoseconds);
        self.max_nanoseconds
            .fetch_max(nanoseconds, Ordering::Relaxed);
    }

    fn snapshot(&self) -> RequestStageSnapshot {
        RequestStageSnapshot {
            buckets: std::array::from_fn(|index| self.buckets[index].load(Ordering::Relaxed)),
            success: self.success.load(Ordering::Relaxed),
            error: self.error.load(Ordering::Relaxed),
            cancelled: self.cancelled.load(Ordering::Relaxed),
            total_nanoseconds: self.total_nanoseconds.load(Ordering::Relaxed),
            max_nanoseconds: self.max_nanoseconds.load(Ordering::Relaxed),
        }
    }
}

fn saturating_fetch_add(value: &AtomicU64, increment: u64) {
    let mut current = value.load(Ordering::Relaxed);
    loop {
        let next = current.saturating_add(increment);
        match value.compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return,
            Err(observed) => current = observed,
        }
    }
}

pub struct RequestStageMetrics {
    stages: [StageMetrics; REQUEST_STAGES],
    details: [StageMetrics; REQUEST_DETAILS],
}

impl Default for RequestStageMetrics {
    fn default() -> Self {
        Self {
            stages: std::array::from_fn(|_| StageMetrics::default()),
            details: std::array::from_fn(|_| StageMetrics::default()),
        }
    }
}

impl RequestStageMetrics {
    pub fn start(self: &Arc<Self>, stage: RequestStage) -> StageTimer {
        StageTimer {
            metrics: Arc::clone(self),
            stage,
            started: Instant::now(),
            finished: false,
        }
    }

    pub fn snapshot(&self) -> RequestMetricsSnapshot {
        RequestMetricsSnapshot {
            stages: std::array::from_fn(|index| self.stages[index].snapshot()),
            details: std::array::from_fn(|index| self.details[index].snapshot()),
        }
    }

    fn record(&self, stage: RequestStage, outcome: StageOutcome, nanoseconds: u64) {
        self.stages[stage.index()].record(outcome, nanoseconds);
    }

    pub fn record_detail(&self, detail: RequestDetail, outcome: StageOutcome, nanoseconds: u64) {
        self.details[detail.index()].record(outcome, nanoseconds);
    }

    pub fn start_detail(self: &Arc<Self>, detail: RequestDetail) -> DetailTimer {
        DetailTimer {
            metrics: Arc::clone(self),
            detail,
            started: Instant::now(),
            finished: false,
        }
    }
}

pub struct DetailTimer {
    metrics: Arc<RequestStageMetrics>,
    detail: RequestDetail,
    started: Instant,
    finished: bool,
}

impl DetailTimer {
    pub fn finish(mut self, outcome: StageOutcome) {
        self.metrics.record_detail(
            self.detail,
            outcome,
            elapsed_nanoseconds(self.started.elapsed().as_nanos()),
        );
        self.finished = true;
    }

    pub fn finish_result<T, E>(self, result: Result<T, E>) -> Result<T, E> {
        let outcome = if result.is_ok() {
            StageOutcome::Success
        } else {
            StageOutcome::Error
        };
        self.finish(outcome);
        result
    }
}

impl Drop for DetailTimer {
    fn drop(&mut self) {
        if !self.finished {
            self.metrics.record_detail(
                self.detail,
                StageOutcome::Cancelled,
                elapsed_nanoseconds(self.started.elapsed().as_nanos()),
            );
        }
    }
}

pub struct StageTimer {
    metrics: Arc<RequestStageMetrics>,
    stage: RequestStage,
    started: Instant,
    finished: bool,
}

impl StageTimer {
    pub fn finish(mut self, outcome: StageOutcome) {
        self.metrics.record(
            self.stage,
            outcome,
            elapsed_nanoseconds(self.started.elapsed().as_nanos()),
        );
        self.finished = true;
    }

    pub fn finish_result<T, E>(self, result: Result<T, E>) -> Result<T, E> {
        let outcome = if result.is_ok() {
            StageOutcome::Success
        } else {
            StageOutcome::Error
        };
        self.finish(outcome);
        result
    }
}

impl Drop for StageTimer {
    fn drop(&mut self) {
        if !self.finished {
            self.metrics.record(
                self.stage,
                StageOutcome::Cancelled,
                elapsed_nanoseconds(self.started.elapsed().as_nanos()),
            );
        }
    }
}

fn elapsed_nanoseconds(nanoseconds: u128) -> u64 {
    u64::try_from(nanoseconds).unwrap_or(u64::MAX)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RequestStageSnapshot {
    pub buckets: [u64; HISTOGRAM_BUCKETS],
    pub success: u64,
    pub error: u64,
    pub cancelled: u64,
    pub total_nanoseconds: u64,
    pub max_nanoseconds: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestMetricsSnapshot {
    pub stages: [RequestStageSnapshot; REQUEST_STAGES],
    pub details: [RequestStageSnapshot; REQUEST_DETAILS],
}

impl RequestMetricsSnapshot {
    pub fn stage(&self, stage: RequestStage) -> RequestStageSnapshot {
        self.stages[stage.index()]
    }

    pub fn stages(&self) -> impl Iterator<Item = (RequestStage, RequestStageSnapshot)> + '_ {
        RequestStage::ALL
            .into_iter()
            .zip(self.stages.iter().copied())
    }

    pub fn details(&self) -> impl Iterator<Item = (RequestDetail, RequestStageSnapshot)> + '_ {
        RequestDetail::ALL
            .into_iter()
            .zip(self.details.iter().copied())
    }
}

pub fn encode_request_metrics_snapshot(
    process_role: &str,
    unix_timestamp_ns: u64,
    sequence: u64,
    snapshot: &RequestMetricsSnapshot,
) -> serde_json::Result<String> {
    let stages = snapshot
        .stages()
        .map(|(stage, snapshot)| {
            serde_json::json!({
                "stage": stage.as_str(),
                "buckets": snapshot.buckets.as_slice(),
                "success": snapshot.success,
                "error": snapshot.error,
                "cancelled": snapshot.cancelled,
                "total_nanoseconds": snapshot.total_nanoseconds,
                "max_nanoseconds": snapshot.max_nanoseconds,
            })
        })
        .collect::<Vec<_>>();
    let details = snapshot
        .details()
        .map(|(detail, snapshot)| {
            serde_json::json!({
                "detail": detail.as_str(),
                "buckets": snapshot.buckets.as_slice(),
                "success": snapshot.success,
                "error": snapshot.error,
                "cancelled": snapshot.cancelled,
                "total_nanoseconds": snapshot.total_nanoseconds,
                "max_nanoseconds": snapshot.max_nanoseconds,
            })
        })
        .collect::<Vec<_>>();
    serde_json::to_string(&serde_json::json!({
        "schema_version": 16,
        "process_role": process_role,
        "unix_timestamp_ns": unix_timestamp_ns,
        "sequence": sequence,
        "stages": stages,
        "details": details,
    }))
}

pub struct RequestMetricsSink {
    sender: SyncSender<String>,
}

impl RequestMetricsSink {
    pub fn disabled() -> Self {
        let (sender, receiver) = sync_channel(1);
        drop(receiver);
        Self { sender }
    }

    pub fn stderr() -> io::Result<Self> {
        Self::spawn_with_writer(|line| {
            let mut stderr = io::stderr().lock();
            let _ = writeln!(stderr, "{line}");
        })
    }

    pub fn try_write(&self, line: String) {
        let _ = self.sender.try_send(line);
    }

    fn spawn_with_writer(mut writer: impl FnMut(String) + Send + 'static) -> io::Result<Self> {
        let (sender, receiver) = sync_channel::<String>(1);
        std::thread::Builder::new()
            .name("dtg-request-metrics".into())
            .spawn(move || {
                for line in receiver {
                    writer(line);
                }
            })?;
        Ok(Self { sender })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, mpsc};
    use std::thread;
    use std::time::{Duration, Instant};

    use super::{
        RequestDetail, RequestMetricsSink, RequestStage, RequestStageMetrics, StageOutcome,
        encode_request_metrics_snapshot,
    };

    #[test]
    fn bounded_metrics_sink_never_waits_for_a_blocked_writer() {
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let sink = RequestMetricsSink::spawn_with_writer(move |_| {
            if entered_tx.send(thread::current().id()).is_ok() {
                let _ = release_rx.recv();
            }
        })
        .unwrap();

        let caller = thread::current().id();
        sink.try_write("first".into());
        let writer = entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_ne!(caller, writer);

        let started = Instant::now();
        for ordinal in 0..10_000 {
            sink.try_write(format!("queued-{ordinal}"));
        }
        assert!(started.elapsed() < Duration::from_millis(100));

        drop(sink);
        release_tx.send(()).unwrap();
    }

    #[test]
    fn disabled_metrics_sink_drops_without_blocking() {
        let sink = RequestMetricsSink::disabled();
        let started = Instant::now();
        for ordinal in 0..10_000 {
            sink.try_write(format!("ignored-{ordinal}"));
        }
        assert!(started.elapsed() < Duration::from_millis(100));
    }

    #[test]
    fn encoded_snapshot_has_stable_process_schema_without_request_content() {
        let metrics = RequestStageMetrics::default();
        metrics.record(RequestStage::GatewayCompile, StageOutcome::Success, 8);
        metrics.record_detail(
            RequestDetail::DataReadViewCacheHit,
            StageOutcome::Success,
            0,
        );

        let encoded =
            encode_request_metrics_snapshot("gateway", 7, 11, &metrics.snapshot()).unwrap();
        let value: serde_json::Value = serde_json::from_str(&encoded).unwrap();

        assert_eq!(value["schema_version"], 16);
        assert_eq!(value["process_role"], "gateway");
        assert_eq!(value["unix_timestamp_ns"], 7);
        assert_eq!(value["sequence"], 11);
        assert_eq!(value["stages"].as_array().unwrap().len(), 10);
        assert_eq!(value["stages"][1]["stage"], "gateway_compile");
        assert_eq!(value["stages"][1]["success"], 1);
        assert_eq!(value["stages"][1]["buckets"].as_array().unwrap().len(), 64);
        assert_eq!(value["details"].as_array().unwrap().len(), 50);
        assert_eq!(value["details"][9]["detail"], "data_read_view_cache_hit");
        assert_eq!(value["details"][9]["success"], 1);
        assert_eq!(value["details"][16]["detail"], "data_raft_batch_admission");
        assert_eq!(value["details"][17]["detail"], "data_raft_batch_queue");
        assert_eq!(
            value["details"][18]["detail"],
            "data_raft_blocking_dispatch"
        );
        assert_eq!(value["details"][19]["detail"], "data_adjacency_cache_hit");
        assert_eq!(value["details"][20]["detail"], "data_adjacency_cache_miss");
        assert_eq!(
            value["details"][21]["detail"],
            "data_adjacency_backend_expand"
        );
        assert_eq!(
            value["details"][22]["detail"],
            "data_snapshot_csr_cache_hit"
        );
        assert_eq!(
            value["details"][23]["detail"],
            "data_snapshot_csr_cache_miss"
        );
        assert_eq!(value["details"][24]["detail"], "data_snapshot_csr_build");
        assert_eq!(
            value["details"][25]["detail"],
            "gateway_query_session_submit"
        );
        assert_eq!(
            value["details"][26]["detail"],
            "gateway_query_session_response_wait"
        );
        assert_eq!(
            value["details"][27]["detail"],
            "data_gateway_session_execution"
        );
        assert_eq!(
            value["details"][28]["detail"],
            "gateway_query_pipeline_submit"
        );
        assert_eq!(
            value["details"][29]["detail"],
            "gateway_query_pipeline_response_wait"
        );
        assert_eq!(
            value["details"][30]["detail"],
            "bolt_read_pipeline_enqueue_wait"
        );
        assert_eq!(
            value["details"][31]["detail"],
            "bolt_read_pipeline_execution_wait"
        );
        assert_eq!(
            value["details"][32]["detail"],
            "bolt_read_pipeline_ordered_write_wait"
        );
        assert_eq!(
            value["details"][33]["detail"],
            "gateway_query_pipeline_credit_wait"
        );
        assert_eq!(
            value["details"][34]["detail"],
            "data_gateway_pipeline_dispatch_wait"
        );
        assert_eq!(
            value["details"][35]["detail"],
            "data_gateway_pipeline_completion_send_wait"
        );
        assert_eq!(
            value["details"][36]["detail"],
            "data_gateway_pipeline_completion_frame"
        );
        assert_eq!(
            value["details"][37]["detail"],
            "gateway_query_pipeline_response_transport_wait"
        );
        assert_eq!(
            value["details"][38]["detail"],
            "gateway_query_pipeline_response_dispatch_wait"
        );
        assert_eq!(
            value["details"][39]["detail"],
            "data_gateway_pipeline_request_transport_wait"
        );
        assert_eq!(
            value["details"][40]["detail"],
            "gateway_query_pipeline_writer_wait"
        );
        assert_eq!(
            value["details"][41]["detail"],
            "data_snapshot_ingest_admission"
        );
        assert_eq!(
            value["details"][42]["detail"],
            "data_snapshot_ingest_receipt_lookup"
        );
        assert_eq!(value["details"][43]["detail"], "gateway_plan_routing");
        assert_eq!(value["details"][44]["detail"], "gateway_transport_wait");
        assert_eq!(value["details"][45]["detail"], "data_validation");
        assert_eq!(value["details"][46]["detail"], "data_execution");
        assert_eq!(value["details"][47]["detail"], "data_raft_queue");
        assert_eq!(value["details"][48]["detail"], "data_raft_apply");
        assert_eq!(value["details"][49]["detail"], "data_provider_apply");
        assert!(!encoded.contains("statement"));
        assert!(!encoded.contains("parameter"));
    }

    #[test]
    fn records_logarithmic_nanosecond_buckets_and_outcomes() {
        let metrics = RequestStageMetrics::default();
        metrics.record(RequestStage::BoltDecode, StageOutcome::Success, 0);
        metrics.record(RequestStage::BoltDecode, StageOutcome::Error, 2);
        metrics.record(RequestStage::BoltDecode, StageOutcome::Cancelled, u64::MAX);

        let snapshot = metrics.snapshot();
        let stage = snapshot.stage(RequestStage::BoltDecode);
        assert_eq!(stage.buckets.len(), 64);
        assert_eq!(stage.buckets[0], 1);
        assert_eq!(stage.buckets[1], 1);
        assert_eq!(stage.buckets[63], 1);
        assert_eq!(stage.success, 1);
        assert_eq!(stage.error, 1);
        assert_eq!(stage.cancelled, 1);
        assert_eq!(stage.total_nanoseconds, u64::MAX);
        assert_eq!(stage.max_nanoseconds, u64::MAX);
    }

    #[test]
    fn total_nanoseconds_saturates() {
        let metrics = RequestStageMetrics::default();
        metrics.record(
            RequestStage::GatewayCompile,
            StageOutcome::Success,
            u64::MAX - 1,
        );
        metrics.record(RequestStage::GatewayCompile, StageOutcome::Success, 10);

        let stage = metrics.snapshot().stage(RequestStage::GatewayCompile);
        assert_eq!(stage.total_nanoseconds, u64::MAX);
        assert_eq!(stage.success, 2);
    }

    #[test]
    fn concurrent_recording_preserves_every_observation() {
        const THREADS: usize = 8;
        const RECORDS: usize = 1_000;
        let metrics = Arc::new(RequestStageMetrics::default());
        let threads = (0..THREADS)
            .map(|_| {
                let metrics = Arc::clone(&metrics);
                thread::spawn(move || {
                    for _ in 0..RECORDS {
                        metrics.record(RequestStage::DataRouting, StageOutcome::Success, 8);
                    }
                })
            })
            .collect::<Vec<_>>();
        for thread in threads {
            thread.join().unwrap();
        }

        let stage = metrics.snapshot().stage(RequestStage::DataRouting);
        assert_eq!(stage.success, (THREADS * RECORDS) as u64);
        assert_eq!(stage.buckets[3], (THREADS * RECORDS) as u64);
        assert_eq!(stage.total_nanoseconds, (THREADS * RECORDS * 8) as u64);
        assert_eq!(stage.max_nanoseconds, 8);
    }

    #[test]
    fn finished_stage_timer_records_requested_outcome_once() {
        let metrics = Arc::new(RequestStageMetrics::default());
        metrics
            .start(RequestStage::BoltEncode)
            .finish(StageOutcome::Error);

        let stage = metrics.snapshot().stage(RequestStage::BoltEncode);
        assert_eq!(stage.error, 1);
        assert_eq!(stage.cancelled, 0);
        assert_eq!(stage.buckets.iter().sum::<u64>(), 1);
    }

    #[test]
    fn dropped_stage_timer_records_cancellation_once() {
        let metrics = Arc::new(RequestStageMetrics::default());
        drop(metrics.start(RequestStage::GatewayCompile));
        let snapshot = metrics.snapshot();
        let stage = snapshot.stage(RequestStage::GatewayCompile);
        assert_eq!(stage.cancelled, 1);
        assert_eq!(stage.success, 0);
        assert_eq!(stage.error, 0);
        assert_eq!(stage.buckets.iter().sum::<u64>(), 1);
    }
}
