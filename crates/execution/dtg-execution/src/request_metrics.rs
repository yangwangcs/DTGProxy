use std::io::{self, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{SyncSender, sync_channel};
use std::time::Instant;

const HISTOGRAM_BUCKETS: usize = 64;
const REQUEST_STAGES: usize = 10;

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
}

impl Default for RequestStageMetrics {
    fn default() -> Self {
        Self {
            stages: std::array::from_fn(|_| StageMetrics::default()),
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
        }
    }

    fn record(&self, stage: RequestStage, outcome: StageOutcome, nanoseconds: u64) {
        self.stages[stage.index()].record(outcome, nanoseconds);
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
    serde_json::to_string(&serde_json::json!({
        "schema_version": 1,
        "process_role": process_role,
        "unix_timestamp_ns": unix_timestamp_ns,
        "sequence": sequence,
        "stages": stages,
    }))
}

pub struct RequestMetricsSink {
    sender: SyncSender<String>,
}

impl RequestMetricsSink {
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
        RequestMetricsSink, RequestStage, RequestStageMetrics, StageOutcome,
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
    fn encoded_snapshot_has_stable_process_schema_without_request_content() {
        let metrics = RequestStageMetrics::default();
        metrics.record(RequestStage::GatewayCompile, StageOutcome::Success, 8);

        let encoded =
            encode_request_metrics_snapshot("gateway", 7, 11, &metrics.snapshot()).unwrap();
        let value: serde_json::Value = serde_json::from_str(&encoded).unwrap();

        assert_eq!(value["schema_version"], 1);
        assert_eq!(value["process_role"], "gateway");
        assert_eq!(value["unix_timestamp_ns"], 7);
        assert_eq!(value["sequence"], 11);
        assert_eq!(value["stages"].as_array().unwrap().len(), 10);
        assert_eq!(value["stages"][1]["stage"], "gateway_compile");
        assert_eq!(value["stages"][1]["success"], 1);
        assert_eq!(value["stages"][1]["buckets"].as_array().unwrap().len(), 64);
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
