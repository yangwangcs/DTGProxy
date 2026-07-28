use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;
use tokio::sync::Notify;

use super::{GraphOverlay, RuntimeError, RuntimeValue};
use analytics_api::ProjectedGraph;
use procedure_runtime::{JobInvocationContext, ProcedureAccess, ProcedureRegistry};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BenchmarkAblationConfig {
    pub native_pushdown: bool,
    pub column_batches: bool,
    pub bounded_lazy_pages: bool,
    pub parallel_shard_fanout: bool,
    pub batched_property_gather: bool,
}

impl Default for BenchmarkAblationConfig {
    fn default() -> Self {
        Self {
            native_pushdown: true,
            column_batches: true,
            bounded_lazy_pages: true,
            parallel_shard_fanout: true,
            batched_property_gather: true,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AblationAxis {
    NativePushdown,
    ColumnBatches,
    BoundedLazyPages,
    ParallelShardFanout,
    BatchedPropertyGather,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BenchmarkAblationCountersSnapshot {
    canonical_residual_scans: u64,
    row_column_conversion_boundaries: u64,
    eager_page_collections: u64,
    serial_shard_opens: u64,
    singleton_property_gather_reads: u64,
}

impl BenchmarkAblationCountersSnapshot {
    #[must_use]
    pub const fn canonical_residual_scans(self) -> u64 {
        self.canonical_residual_scans
    }

    #[must_use]
    pub const fn row_column_conversion_boundaries(self) -> u64 {
        self.row_column_conversion_boundaries
    }

    #[must_use]
    pub const fn eager_page_collections(self) -> u64 {
        self.eager_page_collections
    }

    #[must_use]
    pub const fn serial_shard_opens(self) -> u64 {
        self.serial_shard_opens
    }

    #[must_use]
    pub const fn singleton_property_gather_reads(self) -> u64 {
        self.singleton_property_gather_reads
    }

    #[must_use]
    pub fn exercised_axes(self) -> Vec<AblationAxis> {
        let mut axes = Vec::new();
        if self.canonical_residual_scans != 0 {
            axes.push(AblationAxis::NativePushdown);
        }
        if self.row_column_conversion_boundaries != 0 {
            axes.push(AblationAxis::ColumnBatches);
        }
        if self.eager_page_collections != 0 {
            axes.push(AblationAxis::BoundedLazyPages);
        }
        if self.serial_shard_opens != 0 {
            axes.push(AblationAxis::ParallelShardFanout);
        }
        if self.singleton_property_gather_reads != 0 {
            axes.push(AblationAxis::BatchedPropertyGather);
        }
        axes
    }
}

#[derive(Debug, Default)]
pub struct BenchmarkAblationCounters {
    canonical_residual_scans: AtomicU64,
    row_column_conversion_boundaries: AtomicU64,
    eager_page_collections: AtomicU64,
    serial_shard_opens: AtomicU64,
    singleton_property_gather_reads: AtomicU64,
}

impl BenchmarkAblationCounters {
    #[must_use]
    pub fn snapshot(&self) -> BenchmarkAblationCountersSnapshot {
        BenchmarkAblationCountersSnapshot {
            canonical_residual_scans: self.canonical_residual_scans.load(Ordering::Relaxed),
            row_column_conversion_boundaries: self
                .row_column_conversion_boundaries
                .load(Ordering::Relaxed),
            eager_page_collections: self.eager_page_collections.load(Ordering::Relaxed),
            serial_shard_opens: self.serial_shard_opens.load(Ordering::Relaxed),
            singleton_property_gather_reads: self
                .singleton_property_gather_reads
                .load(Ordering::Relaxed),
        }
    }

    pub fn record_canonical_residual_scan(&self) {
        self.canonical_residual_scans
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_row_column_conversion_boundary(&self) {
        self.row_column_conversion_boundaries
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_eager_page_collection(&self) {
        self.eager_page_collections.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_serial_shard_open(&self) {
        self.serial_shard_opens.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_singleton_property_gather_reads(&self, reads: usize) {
        self.singleton_property_gather_reads
            .fetch_add(u64::try_from(reads).unwrap_or(u64::MAX), Ordering::Relaxed);
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct QueryExecutionMetricsSnapshot {
    adapter_rpc_count: u64,
    sent_frame_count: u64,
    wire_encoded_bytes: u64,
    wire_decoded_bytes: u64,
    value_copy_bytes: u64,
    peak_retained_memory_bytes: u64,
}

impl QueryExecutionMetricsSnapshot {
    #[must_use]
    pub const fn adapter_rpc_count(self) -> u64 {
        self.adapter_rpc_count
    }

    #[must_use]
    pub const fn sent_frame_count(self) -> u64 {
        self.sent_frame_count
    }

    #[must_use]
    pub const fn wire_encoded_bytes(self) -> u64 {
        self.wire_encoded_bytes
    }

    #[must_use]
    pub const fn wire_decoded_bytes(self) -> u64 {
        self.wire_decoded_bytes
    }

    #[must_use]
    pub const fn value_copy_bytes(self) -> u64 {
        self.value_copy_bytes
    }

    #[must_use]
    pub const fn peak_retained_memory_bytes(self) -> u64 {
        self.peak_retained_memory_bytes
    }
}

#[derive(Debug, Default)]
pub struct QueryExecutionMetrics {
    adapter_rpc_count: AtomicU64,
    sent_frame_count: AtomicU64,
    wire_encoded_bytes: AtomicU64,
    wire_decoded_bytes: AtomicU64,
    value_copy_bytes: AtomicU64,
    peak_retained_memory_bytes: AtomicU64,
}

impl QueryExecutionMetrics {
    #[must_use]
    pub fn snapshot(&self) -> QueryExecutionMetricsSnapshot {
        QueryExecutionMetricsSnapshot {
            adapter_rpc_count: self.adapter_rpc_count.load(Ordering::Relaxed),
            sent_frame_count: self.sent_frame_count.load(Ordering::Relaxed),
            wire_encoded_bytes: self.wire_encoded_bytes.load(Ordering::Relaxed),
            wire_decoded_bytes: self.wire_decoded_bytes.load(Ordering::Relaxed),
            value_copy_bytes: self.value_copy_bytes.load(Ordering::Relaxed),
            peak_retained_memory_bytes: self.peak_retained_memory_bytes.load(Ordering::Relaxed),
        }
    }

    pub fn record_adapter_rpc(&self) {
        self.adapter_rpc_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_sent_frame(&self) {
        self.sent_frame_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_wire_encoded_bytes(&self, bytes: u64) {
        self.wire_encoded_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn record_wire_decoded_bytes(&self, bytes: u64) {
        self.wire_decoded_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn record_value_copy_bytes(&self, bytes: u64) {
        self.value_copy_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn observe_retained_memory_bytes(&self, bytes: u64) {
        self.peak_retained_memory_bytes
            .fetch_max(bytes, Ordering::Relaxed);
    }
}

impl temporal_storage::AdapterCallObserver for QueryExecutionMetrics {
    fn record_adapter_call(&self) {
        self.record_adapter_rpc();
    }
}

#[derive(Debug, Default)]
struct CancellationState {
    cancelled: AtomicBool,
    changed: Notify,
}

#[derive(Clone, Debug, Default)]
pub struct CancellationToken {
    state: Arc<CancellationState>,
}

impl CancellationToken {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn cancel(&self) {
        self.state.cancelled.store(true, Ordering::Release);
        self.state.changed.notify_waiters();
    }

    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.state.cancelled.load(Ordering::Acquire)
    }

    pub async fn cancelled(&self) {
        let mut changed = Box::pin(self.state.changed.notified());
        loop {
            changed.as_mut().enable();
            if self.is_cancelled() {
                return;
            }
            changed.as_mut().await;
            changed.set(self.state.changed.notified());
        }
    }
}

#[derive(Clone, Debug)]
pub struct ExecutionContext {
    parameters: BTreeMap<String, RuntimeValue>,
    cancellation: CancellationToken,
    deadline: Option<Instant>,
    graph_overlay: GraphOverlay,
    procedure_registry: Option<Arc<ProcedureRegistry>>,
    procedure_graph: Option<Arc<ProjectedGraph>>,
    security_fingerprint: [u8; 32],
    procedure_access: ProcedureAccess,
    job_invocation_context: Option<JobInvocationContext>,
    query_metrics: Option<Arc<QueryExecutionMetrics>>,
    benchmark_ablations: BenchmarkAblationConfig,
    benchmark_ablation_counters: Option<Arc<BenchmarkAblationCounters>>,
}

impl ExecutionContext {
    #[must_use]
    pub fn new(parameters: BTreeMap<String, RuntimeValue>) -> Self {
        Self {
            parameters,
            cancellation: CancellationToken::new(),
            deadline: None,
            graph_overlay: GraphOverlay::default(),
            procedure_registry: None,
            procedure_graph: None,
            security_fingerprint: [0; 32],
            procedure_access: ProcedureAccess::denied(),
            job_invocation_context: None,
            query_metrics: None,
            benchmark_ablations: BenchmarkAblationConfig::default(),
            benchmark_ablation_counters: None,
        }
    }

    #[must_use]
    pub fn with_cancellation(mut self, cancellation: CancellationToken) -> Self {
        self.cancellation = cancellation;
        self
    }

    #[must_use]
    pub fn with_deadline(mut self, deadline: Instant) -> Self {
        self.deadline = Some(deadline);
        self
    }

    #[must_use]
    pub fn with_graph_overlay(mut self, graph_overlay: GraphOverlay) -> Self {
        self.graph_overlay = graph_overlay;
        self
    }

    #[must_use]
    pub fn for_shard(mut self, shard_id: u32) -> Self {
        self.graph_overlay = self.graph_overlay.for_shard(shard_id);
        self
    }

    #[must_use]
    pub fn with_procedure_runtime(
        mut self,
        registry: Arc<ProcedureRegistry>,
        graph: Option<Arc<ProjectedGraph>>,
        security_fingerprint: [u8; 32],
        access: ProcedureAccess,
    ) -> Self {
        self.procedure_registry = Some(registry);
        self.procedure_graph = graph;
        self.security_fingerprint = security_fingerprint;
        self.procedure_access = access;
        self
    }

    #[must_use]
    pub fn with_job_invocation_context(mut self, context: JobInvocationContext) -> Self {
        self.job_invocation_context = Some(context);
        self
    }

    #[must_use]
    pub fn with_query_metrics(mut self, metrics: Arc<QueryExecutionMetrics>) -> Self {
        self.query_metrics = Some(metrics);
        self
    }

    #[must_use]
    pub fn query_metrics(&self) -> Option<Arc<QueryExecutionMetrics>> {
        self.query_metrics.clone()
    }

    #[must_use]
    pub fn with_benchmark_ablations(
        mut self,
        config: BenchmarkAblationConfig,
        counters: Arc<BenchmarkAblationCounters>,
    ) -> Self {
        self.benchmark_ablations = config;
        self.benchmark_ablation_counters = Some(counters);
        self
    }

    #[must_use]
    pub const fn benchmark_ablations(&self) -> BenchmarkAblationConfig {
        self.benchmark_ablations
    }

    pub fn record_canonical_residual_scan(&self) {
        if let Some(counters) = &self.benchmark_ablation_counters {
            counters.record_canonical_residual_scan();
        }
    }

    pub fn record_row_column_conversion_boundary(&self) {
        if let Some(counters) = &self.benchmark_ablation_counters {
            counters.record_row_column_conversion_boundary();
        }
    }

    pub fn record_eager_page_collection(&self) {
        if let Some(counters) = &self.benchmark_ablation_counters {
            counters.record_eager_page_collection();
        }
    }

    pub fn record_serial_shard_open(&self) {
        if let Some(counters) = &self.benchmark_ablation_counters {
            counters.record_serial_shard_open();
        }
    }

    pub fn record_singleton_property_gather_reads(&self, reads: usize) {
        if let Some(counters) = &self.benchmark_ablation_counters {
            counters.record_singleton_property_gather_reads(reads);
        }
    }

    pub(crate) const fn graph_overlay(&self) -> &GraphOverlay {
        &self.graph_overlay
    }

    pub(crate) fn check_fences(&self) -> Result<(), RuntimeError> {
        if self.cancellation.is_cancelled() {
            return Err(RuntimeError::Cancelled);
        }
        if self
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            return Err(RuntimeError::DeadlineExceeded);
        }
        Ok(())
    }

    pub async fn cancelled(&self) {
        self.cancellation.cancelled().await;
    }

    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }

    pub const fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    pub(crate) fn parameter(&self, name: &str) -> Result<&RuntimeValue, RuntimeError> {
        self.parameters
            .get(name)
            .ok_or_else(|| RuntimeError::MissingParameter(name.to_owned()))
    }

    pub(crate) fn procedure_registry(&self) -> Result<&ProcedureRegistry, RuntimeError> {
        self.procedure_registry
            .as_deref()
            .ok_or(RuntimeError::ProcedureRuntimeMissing)
    }

    pub(crate) fn procedure_graph(&self) -> Option<Arc<ProjectedGraph>> {
        self.procedure_graph.clone()
    }

    pub const fn security_fingerprint(&self) -> [u8; 32] {
        self.security_fingerprint
    }

    pub(crate) const fn procedure_access(&self) -> &ProcedureAccess {
        &self.procedure_access
    }

    pub(crate) const fn job_invocation_context(&self) -> Option<&JobInvocationContext> {
        self.job_invocation_context.as_ref()
    }
}

impl Default for ExecutionContext {
    fn default() -> Self {
        Self::new(BTreeMap::new())
    }
}
