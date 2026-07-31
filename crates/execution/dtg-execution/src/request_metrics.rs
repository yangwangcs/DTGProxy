use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread;

    use super::{RequestStage, RequestStageMetrics, StageOutcome};

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
