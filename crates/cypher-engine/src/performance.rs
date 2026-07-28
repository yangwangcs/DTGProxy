use std::fmt::{self, Debug, Display, Formatter};
use std::future::Future;
use std::time::Instant;

use query_executor::{QueryExecutionMetricsSnapshot, RecordBatch};
use temporal_types::TransactionTime;

pub type MaterializedRunError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExternalTtfr {
    UnavailableMaterializedApi,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryMetricUnavailableReason {
    MaterializedExecutionApi,
    InstrumentationNotInstalled,
    ExternalBackendNotObserved,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryMetricAvailability<T> {
    Observed(T),
    Unavailable(QueryMetricUnavailableReason),
}

impl<T> QueryMetricAvailability<T> {
    #[must_use]
    pub const fn unavailable(reason: QueryMetricUnavailableReason) -> Self {
        Self::Unavailable(reason)
    }

    #[must_use]
    pub const fn observed(value: T) -> Self {
        Self::Observed(value)
    }

    #[must_use]
    pub const fn as_observed(&self) -> Option<&T> {
        match self {
            Self::Observed(value) => Some(value),
            Self::Unavailable(_) => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryScopedMetric {
    CompileCount,
    OptimizeCount,
    AdapterRpcCount,
    SentFrameCount,
    WireEncodedBytes,
    WireDecodedBytes,
    ValueCopyBytes,
    PeakRetainedMemoryBytes,
}

impl Display for QueryScopedMetric {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::CompileCount => "compile count",
            Self::OptimizeCount => "optimize count",
            Self::AdapterRpcCount => "adapter RPC count",
            Self::SentFrameCount => "sent frame count",
            Self::WireEncodedBytes => "wire encoded bytes",
            Self::WireDecodedBytes => "wire decoded bytes",
            Self::ValueCopyBytes => "value-copy bytes",
            Self::PeakRetainedMemoryBytes => "peak retained memory bytes",
        };
        formatter.write_str(name)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryScopedOverhead {
    compile_count: QueryMetricAvailability<u64>,
    optimize_count: QueryMetricAvailability<u64>,
    adapter_rpc_count: QueryMetricAvailability<u64>,
    sent_frame_count: QueryMetricAvailability<u64>,
    wire_encoded_bytes: QueryMetricAvailability<u64>,
    wire_decoded_bytes: QueryMetricAvailability<u64>,
    value_copy_bytes: QueryMetricAvailability<u64>,
    peak_retained_memory_bytes: QueryMetricAvailability<u64>,
}

impl QueryScopedOverhead {
    #[must_use]
    pub fn unavailable(reason: QueryMetricUnavailableReason) -> Self {
        Self {
            compile_count: QueryMetricAvailability::unavailable(reason),
            optimize_count: QueryMetricAvailability::unavailable(reason),
            adapter_rpc_count: QueryMetricAvailability::unavailable(reason),
            sent_frame_count: QueryMetricAvailability::unavailable(reason),
            wire_encoded_bytes: QueryMetricAvailability::unavailable(reason),
            wire_decoded_bytes: QueryMetricAvailability::unavailable(reason),
            value_copy_bytes: QueryMetricAvailability::unavailable(reason),
            peak_retained_memory_bytes: QueryMetricAvailability::unavailable(reason),
        }
    }

    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub const fn observed(
        compile_count: u64,
        optimize_count: u64,
        adapter_rpc_count: u64,
        sent_frame_count: u64,
        wire_encoded_bytes: u64,
        wire_decoded_bytes: u64,
        value_copy_bytes: u64,
        peak_retained_memory_bytes: u64,
    ) -> Self {
        Self {
            compile_count: QueryMetricAvailability::observed(compile_count),
            optimize_count: QueryMetricAvailability::observed(optimize_count),
            adapter_rpc_count: QueryMetricAvailability::observed(adapter_rpc_count),
            sent_frame_count: QueryMetricAvailability::observed(sent_frame_count),
            wire_encoded_bytes: QueryMetricAvailability::observed(wire_encoded_bytes),
            wire_decoded_bytes: QueryMetricAvailability::observed(wire_decoded_bytes),
            value_copy_bytes: QueryMetricAvailability::observed(value_copy_bytes),
            peak_retained_memory_bytes: QueryMetricAvailability::observed(
                peak_retained_memory_bytes,
            ),
        }
    }

    #[must_use]
    pub const fn from_execution_metrics(
        compile_count: u64,
        optimize_count: u64,
        metrics: QueryExecutionMetricsSnapshot,
    ) -> Self {
        Self::observed(
            compile_count,
            optimize_count,
            metrics.adapter_rpc_count(),
            metrics.sent_frame_count(),
            metrics.wire_encoded_bytes(),
            metrics.wire_decoded_bytes(),
            metrics.value_copy_bytes(),
            metrics.peak_retained_memory_bytes(),
        )
    }

    fn merge_max(self, other: Self) -> Self {
        Self {
            compile_count: merge_metric_max(self.compile_count, other.compile_count),
            optimize_count: merge_metric_max(self.optimize_count, other.optimize_count),
            adapter_rpc_count: merge_metric_max(self.adapter_rpc_count, other.adapter_rpc_count),
            sent_frame_count: merge_metric_max(self.sent_frame_count, other.sent_frame_count),
            wire_encoded_bytes: merge_metric_max(self.wire_encoded_bytes, other.wire_encoded_bytes),
            wire_decoded_bytes: merge_metric_max(self.wire_decoded_bytes, other.wire_decoded_bytes),
            value_copy_bytes: merge_metric_max(self.value_copy_bytes, other.value_copy_bytes),
            peak_retained_memory_bytes: merge_metric_max(
                self.peak_retained_memory_bytes,
                other.peak_retained_memory_bytes,
            ),
        }
    }

    #[must_use]
    pub const fn compile_count(&self) -> &QueryMetricAvailability<u64> {
        &self.compile_count
    }

    #[must_use]
    pub const fn optimize_count(&self) -> &QueryMetricAvailability<u64> {
        &self.optimize_count
    }

    #[must_use]
    pub const fn adapter_rpc_count(&self) -> &QueryMetricAvailability<u64> {
        &self.adapter_rpc_count
    }

    #[must_use]
    pub const fn sent_frame_count(&self) -> &QueryMetricAvailability<u64> {
        &self.sent_frame_count
    }

    #[must_use]
    pub const fn wire_encoded_bytes(&self) -> &QueryMetricAvailability<u64> {
        &self.wire_encoded_bytes
    }

    #[must_use]
    pub const fn wire_decoded_bytes(&self) -> &QueryMetricAvailability<u64> {
        &self.wire_decoded_bytes
    }

    #[must_use]
    pub const fn value_copy_bytes(&self) -> &QueryMetricAvailability<u64> {
        &self.value_copy_bytes
    }

    #[must_use]
    pub const fn peak_retained_memory_bytes(&self) -> &QueryMetricAvailability<u64> {
        &self.peak_retained_memory_bytes
    }

    pub(crate) fn evaluate(
        &self,
        limits: &QueryScopedOverheadLimits,
    ) -> Result<(), QueryOverheadGateError> {
        evaluate_metric(
            QueryScopedMetric::CompileCount,
            &self.compile_count,
            limits.max_compile_count,
        )?;
        evaluate_metric(
            QueryScopedMetric::OptimizeCount,
            &self.optimize_count,
            limits.max_optimize_count,
        )?;
        evaluate_metric(
            QueryScopedMetric::AdapterRpcCount,
            &self.adapter_rpc_count,
            limits.max_adapter_rpc_count,
        )?;
        evaluate_metric(
            QueryScopedMetric::SentFrameCount,
            &self.sent_frame_count,
            limits.max_sent_frame_count,
        )?;
        evaluate_metric(
            QueryScopedMetric::WireEncodedBytes,
            &self.wire_encoded_bytes,
            limits.max_wire_encoded_bytes,
        )?;
        evaluate_metric(
            QueryScopedMetric::WireDecodedBytes,
            &self.wire_decoded_bytes,
            limits.max_wire_decoded_bytes,
        )?;
        evaluate_metric(
            QueryScopedMetric::ValueCopyBytes,
            &self.value_copy_bytes,
            limits.max_value_copy_bytes,
        )?;
        evaluate_metric(
            QueryScopedMetric::PeakRetainedMemoryBytes,
            &self.peak_retained_memory_bytes,
            limits.max_peak_retained_memory_bytes,
        )
    }
}

fn merge_metric_max(
    left: QueryMetricAvailability<u64>,
    right: QueryMetricAvailability<u64>,
) -> QueryMetricAvailability<u64> {
    match (left, right) {
        (QueryMetricAvailability::Observed(left), QueryMetricAvailability::Observed(right)) => {
            QueryMetricAvailability::Observed(left.max(right))
        }
        (QueryMetricAvailability::Unavailable(reason), _)
        | (_, QueryMetricAvailability::Unavailable(reason)) => {
            QueryMetricAvailability::Unavailable(reason)
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueryScopedOverheadLimits {
    max_compile_count: u64,
    max_optimize_count: u64,
    max_adapter_rpc_count: u64,
    max_sent_frame_count: u64,
    max_wire_encoded_bytes: u64,
    max_wire_decoded_bytes: u64,
    max_value_copy_bytes: u64,
    max_peak_retained_memory_bytes: u64,
}

impl QueryScopedOverheadLimits {
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub const fn new(
        max_compile_count: u64,
        max_optimize_count: u64,
        max_adapter_rpc_count: u64,
        max_sent_frame_count: u64,
        max_wire_encoded_bytes: u64,
        max_wire_decoded_bytes: u64,
        max_value_copy_bytes: u64,
        max_peak_retained_memory_bytes: u64,
    ) -> Self {
        Self {
            max_compile_count,
            max_optimize_count,
            max_adapter_rpc_count,
            max_sent_frame_count,
            max_wire_encoded_bytes,
            max_wire_decoded_bytes,
            max_value_copy_bytes,
            max_peak_retained_memory_bytes,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryOverheadGateError {
    MetricUnavailable {
        metric: QueryScopedMetric,
        reason: QueryMetricUnavailableReason,
    },
    LimitExceeded {
        metric: QueryScopedMetric,
        observed: u64,
        limit: u64,
    },
}

impl Display for QueryOverheadGateError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::MetricUnavailable { metric, reason } => {
                write!(formatter, "{metric} is unavailable: {reason:?}")
            }
            Self::LimitExceeded {
                metric,
                observed,
                limit,
            } => write!(
                formatter,
                "{metric} exceeds limit: observed {observed}, limit {limit}"
            ),
        }
    }
}

impl std::error::Error for QueryOverheadGateError {}

fn evaluate_metric(
    metric: QueryScopedMetric,
    availability: &QueryMetricAvailability<u64>,
    limit: u64,
) -> Result<(), QueryOverheadGateError> {
    match availability {
        QueryMetricAvailability::Observed(observed) if *observed <= limit => Ok(()),
        QueryMetricAvailability::Observed(observed) => Err(QueryOverheadGateError::LimitExceeded {
            metric,
            observed: *observed,
            limit,
        }),
        QueryMetricAvailability::Unavailable(reason) => {
            Err(QueryOverheadGateError::MetricUnavailable {
                metric,
                reason: *reason,
            })
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LatencyPercentiles {
    sample_count: usize,
    p50: u64,
    p95: u64,
    p99: u64,
}

impl LatencyPercentiles {
    #[must_use]
    pub fn from_samples(mut samples: Vec<u64>) -> Self {
        samples.sort_unstable();
        Self {
            sample_count: samples.len(),
            p50: nearest_rank(&samples, 50),
            p95: nearest_rank(&samples, 95),
            p99: nearest_rank(&samples, 99),
        }
    }

    #[must_use]
    pub const fn sample_count(&self) -> usize {
        self.sample_count
    }

    #[must_use]
    pub const fn p50(&self) -> u64 {
        self.p50
    }

    #[must_use]
    pub const fn p95(&self) -> u64 {
        self.p95
    }

    #[must_use]
    pub const fn p99(&self) -> u64 {
        self.p99
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MaterializedPathObservation {
    result_digest: [u8; 32],
    row_count: usize,
    batch_count: usize,
    latency_micros: LatencyPercentiles,
    query_scoped_overhead: QueryScopedOverhead,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MaterializedRunObservation {
    batches: Vec<RecordBatch>,
    query_scoped_overhead: QueryScopedOverhead,
}

impl MaterializedRunObservation {
    #[must_use]
    pub const fn observed(
        batches: Vec<RecordBatch>,
        query_scoped_overhead: QueryScopedOverhead,
    ) -> Self {
        Self {
            batches,
            query_scoped_overhead,
        }
    }

    #[must_use]
    pub fn unavailable(batches: Vec<RecordBatch>, reason: QueryMetricUnavailableReason) -> Self {
        Self::observed(batches, QueryScopedOverhead::unavailable(reason))
    }
}

impl MaterializedPathObservation {
    #[must_use]
    pub fn from_samples(batches: &[RecordBatch], latency_micros: Vec<u64>) -> Self {
        Self::from_samples_with_query_scoped_overhead(
            batches,
            latency_micros,
            QueryScopedOverhead::unavailable(
                QueryMetricUnavailableReason::MaterializedExecutionApi,
            ),
        )
    }

    #[must_use]
    pub fn from_samples_with_query_scoped_overhead(
        batches: &[RecordBatch],
        latency_micros: Vec<u64>,
        query_scoped_overhead: QueryScopedOverhead,
    ) -> Self {
        Self {
            result_digest: materialized_result_digest(batches),
            row_count: batches.iter().map(|batch| batch.rows().len()).sum(),
            batch_count: batches.len(),
            latency_micros: LatencyPercentiles::from_samples(latency_micros),
            query_scoped_overhead,
        }
    }

    #[must_use]
    pub const fn result_digest(&self) -> [u8; 32] {
        self.result_digest
    }

    #[must_use]
    pub const fn row_count(&self) -> usize {
        self.row_count
    }

    #[must_use]
    pub const fn batch_count(&self) -> usize {
        self.batch_count
    }

    #[must_use]
    pub const fn latency_micros(&self) -> LatencyPercentiles {
        self.latency_micros
    }

    #[must_use]
    pub const fn query_scoped_overhead(&self) -> &QueryScopedOverhead {
        &self.query_scoped_overhead
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PairedMaterializedReport {
    fixture_id: String,
    snapshot: TransactionTime,
    semantic_id: String,
    direct: MaterializedPathObservation,
    proxy: MaterializedPathObservation,
    external_ttfr: ExternalTtfr,
}

impl PairedMaterializedReport {
    pub fn new(
        fixture_id: impl Into<String>,
        snapshot: TransactionTime,
        semantic_id: impl Into<String>,
        direct: MaterializedPathObservation,
        proxy: MaterializedPathObservation,
    ) -> Result<Self, PairedReportError> {
        let fixture_id = fixture_id.into();
        let semantic_id = semantic_id.into();
        if fixture_id.is_empty() || semantic_id.is_empty() {
            return Err(PairedReportError::InvalidIdentity);
        }
        if direct.result_digest != proxy.result_digest {
            return Err(PairedReportError::ResultMismatch);
        }
        Ok(Self {
            fixture_id,
            snapshot,
            semantic_id,
            direct,
            proxy,
            external_ttfr: ExternalTtfr::UnavailableMaterializedApi,
        })
    }

    #[must_use]
    pub fn fixture_id(&self) -> &str {
        &self.fixture_id
    }

    #[must_use]
    pub const fn snapshot(&self) -> TransactionTime {
        self.snapshot
    }

    #[must_use]
    pub fn semantic_id(&self) -> &str {
        &self.semantic_id
    }

    #[must_use]
    pub const fn direct(&self) -> &MaterializedPathObservation {
        &self.direct
    }

    #[must_use]
    pub const fn proxy(&self) -> &MaterializedPathObservation {
        &self.proxy
    }

    #[must_use]
    pub const fn external_ttfr(&self) -> ExternalTtfr {
        self.external_ttfr
    }

    /// Evaluates the proxy-side, query-scoped overhead against a release budget.
    ///
    /// Materialized paired execution cannot observe these counters. Its unavailable values are
    /// intentionally rejected instead of being treated as zero.
    pub fn evaluate_query_scoped_overhead(
        &self,
        limits: &QueryScopedOverheadLimits,
    ) -> Result<(), QueryOverheadGateError> {
        self.proxy.query_scoped_overhead.evaluate(limits)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PairedReportError {
    InvalidIdentity,
    InvalidSampleCount,
    DirectRun(String),
    ProxyRun(String),
    ResultMismatch,
}

impl Display for PairedReportError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidIdentity => {
                formatter.write_str("fixture and semantic identity are required")
            }
            Self::InvalidSampleCount => {
                formatter.write_str("at least one paired sample is required")
            }
            Self::DirectRun(error) => write!(formatter, "direct run failed: {error}"),
            Self::ProxyRun(error) => write!(formatter, "proxy run failed: {error}"),
            Self::ResultMismatch => formatter.write_str("direct and proxy result digests differ"),
        }
    }
}

impl std::error::Error for PairedReportError {}

pub async fn run_materialized_pair<Direct, DirectFuture, Proxy, ProxyFuture>(
    fixture_id: impl Into<String>,
    snapshot: TransactionTime,
    semantic_id: impl Into<String>,
    sample_count: usize,
    mut direct: Direct,
    mut proxy: Proxy,
) -> Result<PairedMaterializedReport, PairedReportError>
where
    Direct: FnMut() -> DirectFuture,
    DirectFuture: Future<Output = Result<Vec<RecordBatch>, MaterializedRunError>>,
    Proxy: FnMut() -> ProxyFuture,
    ProxyFuture: Future<Output = Result<Vec<RecordBatch>, MaterializedRunError>>,
{
    if sample_count == 0 {
        return Err(PairedReportError::InvalidSampleCount);
    }
    let mut direct_samples = Vec::with_capacity(sample_count);
    let mut proxy_samples = Vec::with_capacity(sample_count);
    let mut direct_result: Option<Vec<RecordBatch>> = None;
    let mut proxy_result: Option<Vec<RecordBatch>> = None;
    let mut expected_digest = None;
    for _ in 0..sample_count {
        let started = Instant::now();
        let result = direct()
            .await
            .map_err(|error| PairedReportError::DirectRun(error.to_string()))?;
        direct_samples.push(elapsed_micros(started));
        let direct_digest = materialized_result_digest(&result);
        if expected_digest.is_some_and(|expected| expected != direct_digest) {
            return Err(PairedReportError::ResultMismatch);
        }
        expected_digest = Some(direct_digest);
        direct_result = Some(result);

        let started = Instant::now();
        let result = proxy()
            .await
            .map_err(|error| PairedReportError::ProxyRun(error.to_string()))?;
        proxy_samples.push(elapsed_micros(started));
        if materialized_result_digest(&result) != direct_digest {
            return Err(PairedReportError::ResultMismatch);
        }
        proxy_result = Some(result);
    }
    PairedMaterializedReport::new(
        fixture_id,
        snapshot,
        semantic_id,
        MaterializedPathObservation::from_samples(
            direct_result.as_deref().unwrap_or_default(),
            direct_samples,
        ),
        MaterializedPathObservation::from_samples(
            proxy_result.as_deref().unwrap_or_default(),
            proxy_samples,
        ),
    )
}

pub async fn run_observed_materialized_pair<Direct, DirectFuture, Proxy, ProxyFuture>(
    fixture_id: impl Into<String>,
    snapshot: TransactionTime,
    semantic_id: impl Into<String>,
    sample_count: usize,
    mut direct: Direct,
    mut proxy: Proxy,
) -> Result<PairedMaterializedReport, PairedReportError>
where
    Direct: FnMut() -> DirectFuture,
    DirectFuture: Future<Output = Result<MaterializedRunObservation, MaterializedRunError>>,
    Proxy: FnMut() -> ProxyFuture,
    ProxyFuture: Future<Output = Result<MaterializedRunObservation, MaterializedRunError>>,
{
    if sample_count == 0 {
        return Err(PairedReportError::InvalidSampleCount);
    }
    let mut direct_samples = Vec::with_capacity(sample_count);
    let mut proxy_samples = Vec::with_capacity(sample_count);
    let mut direct_result: Option<MaterializedRunObservation> = None;
    let mut proxy_result: Option<MaterializedRunObservation> = None;
    let mut expected_digest = None;
    for _ in 0..sample_count {
        let started = Instant::now();
        let result = direct()
            .await
            .map_err(|error| PairedReportError::DirectRun(error.to_string()))?;
        direct_samples.push(elapsed_micros(started));
        let direct_digest = materialized_result_digest(&result.batches);
        if expected_digest.is_some_and(|expected| expected != direct_digest) {
            return Err(PairedReportError::ResultMismatch);
        }
        expected_digest = Some(direct_digest);
        direct_result = Some(match direct_result {
            None => result,
            Some(prior) => MaterializedRunObservation {
                batches: result.batches,
                query_scoped_overhead: prior
                    .query_scoped_overhead
                    .merge_max(result.query_scoped_overhead),
            },
        });

        let started = Instant::now();
        let result = proxy()
            .await
            .map_err(|error| PairedReportError::ProxyRun(error.to_string()))?;
        proxy_samples.push(elapsed_micros(started));
        if materialized_result_digest(&result.batches) != direct_digest {
            return Err(PairedReportError::ResultMismatch);
        }
        proxy_result = Some(match proxy_result {
            None => result,
            Some(prior) => MaterializedRunObservation {
                batches: result.batches,
                query_scoped_overhead: prior
                    .query_scoped_overhead
                    .merge_max(result.query_scoped_overhead),
            },
        });
    }
    let direct = direct_result.expect("sample count validated");
    let proxy = proxy_result.expect("sample count validated");
    PairedMaterializedReport::new(
        fixture_id,
        snapshot,
        semantic_id,
        MaterializedPathObservation::from_samples_with_query_scoped_overhead(
            &direct.batches,
            direct_samples,
            direct.query_scoped_overhead,
        ),
        MaterializedPathObservation::from_samples_with_query_scoped_overhead(
            &proxy.batches,
            proxy_samples,
            proxy.query_scoped_overhead,
        ),
    )
}

fn nearest_rank(samples: &[u64], percentile: usize) -> u64 {
    if samples.is_empty() {
        return 0;
    }
    let rank = samples
        .len()
        .saturating_mul(percentile)
        .div_ceil(100)
        .saturating_sub(1);
    samples[rank]
}

fn elapsed_micros(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX)
}

fn materialized_result_digest(batches: &[RecordBatch]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"DTGProxy/MaterializedResult/V1");
    if let Some(schema) = batches.first().map(RecordBatch::schema) {
        hash_debug(&mut hasher, schema);
    }
    for row in batches.iter().flat_map(|batch| batch.rows()) {
        hash_debug(&mut hasher, row);
    }
    *hasher.finalize().as_bytes()
}

fn hash_debug(hasher: &mut blake3::Hasher, value: &impl Debug) {
    let encoded = format!("{value:?}");
    hasher.update(&encoded.len().to_be_bytes());
    hasher.update(encoded.as_bytes());
}
