use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::Path;

use serde::{Deserialize, Serialize};

const REQUEST_METRICS_PREFIX: &str = "DTG_REQUEST_STAGE_METRICS=";
const REQUEST_METRIC_STAGES: [&str; 10] = [
    "bolt_decode",
    "gateway_compile",
    "gateway_plan",
    "gateway_internal_rpc",
    "gateway_local_execution",
    "bolt_encode",
    "data_validation",
    "data_routing",
    "data_raft_apply",
    "data_provider_execution",
];
const HISTOGRAM_BUCKETS: usize = 64;
const REQUEST_METRIC_DETAILS_V2: [&str; 19] = [
    "gateway_query_request_encode",
    "gateway_query_response_collect",
    "gateway_query_response_decode",
    "gateway_query_local_materialize",
    "gateway_meta_allocate_start",
    "gateway_meta_reserve_commit",
    "gateway_data_apply_rpc",
    "gateway_meta_resolve_commit",
    "data_route_lock_wait",
    "data_route_lookup",
    "data_raft_propose",
    "data_raft_drive_ready",
    "data_read_view_cache_hit",
    "data_read_view_cache_miss",
    "data_read_view_open",
    "data_temporal_point_evaluation",
    "data_temporal_scan_id_collection",
    "data_temporal_scan_visibility",
    "data_raft_lock_wait",
];
const REQUEST_METRIC_DETAILS_V3: [&str; 20] = [
    "gateway_query_request_encode",
    "gateway_query_response_collect",
    "gateway_query_response_decode",
    "gateway_query_local_materialize",
    "gateway_meta_allocate_start",
    "gateway_meta_reserve_commit",
    "gateway_data_apply_rpc",
    "gateway_meta_resolve_commit",
    "data_route_lock_wait",
    "data_route_lookup",
    "data_raft_propose",
    "data_raft_drive_ready",
    "data_read_view_cache_hit",
    "data_read_view_cache_miss",
    "data_read_view_open",
    "data_temporal_point_evaluation",
    "data_temporal_scan_id_collection",
    "data_temporal_scan_visibility",
    "data_raft_lock_wait",
    "gateway_meta_prepare_write",
];
const REQUEST_METRIC_DETAILS_V4: [&str; 23] = [
    "gateway_query_request_encode",
    "gateway_query_response_collect",
    "gateway_query_response_decode",
    "gateway_query_local_materialize",
    "gateway_meta_allocate_start",
    "gateway_meta_reserve_commit",
    "gateway_data_apply_rpc",
    "gateway_meta_resolve_commit",
    "data_route_lock_wait",
    "data_route_lookup",
    "data_raft_propose",
    "data_raft_drive_ready",
    "data_read_view_cache_hit",
    "data_read_view_cache_miss",
    "data_read_view_open",
    "data_temporal_point_evaluation",
    "data_temporal_scan_id_collection",
    "data_temporal_scan_visibility",
    "data_raft_lock_wait",
    "gateway_meta_prepare_write",
    "data_raft_batch_admission",
    "data_raft_batch_queue",
    "data_raft_blocking_dispatch",
];

const REQUEST_METRIC_DETAILS_V5: [&str; 26] = [
    "gateway_query_request_encode",
    "gateway_query_response_collect",
    "gateway_query_response_decode",
    "gateway_query_local_materialize",
    "gateway_meta_allocate_start",
    "gateway_meta_reserve_commit",
    "gateway_data_apply_rpc",
    "gateway_meta_resolve_commit",
    "data_route_lock_wait",
    "data_route_lookup",
    "data_raft_propose",
    "data_raft_drive_ready",
    "data_read_view_cache_hit",
    "data_read_view_cache_miss",
    "data_read_view_open",
    "data_temporal_point_evaluation",
    "data_temporal_scan_id_collection",
    "data_temporal_scan_visibility",
    "data_raft_lock_wait",
    "gateway_meta_prepare_write",
    "data_raft_batch_admission",
    "data_raft_batch_queue",
    "data_raft_blocking_dispatch",
    "data_adjacency_cache_hit",
    "data_adjacency_cache_miss",
    "data_adjacency_backend_expand",
];

const REQUEST_METRIC_DETAILS_V6: [&str; 29] = [
    "gateway_query_request_encode",
    "gateway_query_response_collect",
    "gateway_query_response_decode",
    "gateway_query_local_materialize",
    "gateway_meta_allocate_start",
    "gateway_meta_reserve_commit",
    "gateway_data_apply_rpc",
    "gateway_meta_resolve_commit",
    "data_route_lock_wait",
    "data_route_lookup",
    "data_raft_propose",
    "data_raft_drive_ready",
    "data_read_view_cache_hit",
    "data_read_view_cache_miss",
    "data_read_view_open",
    "data_temporal_point_evaluation",
    "data_temporal_scan_id_collection",
    "data_temporal_scan_visibility",
    "data_raft_lock_wait",
    "gateway_meta_prepare_write",
    "data_raft_batch_admission",
    "data_raft_batch_queue",
    "data_raft_blocking_dispatch",
    "data_adjacency_cache_hit",
    "data_adjacency_cache_miss",
    "data_adjacency_backend_expand",
    "data_snapshot_csr_cache_hit",
    "data_snapshot_csr_cache_miss",
    "data_snapshot_csr_build",
];

const REQUEST_METRIC_DETAILS_V7: [&str; 32] = [
    "gateway_query_request_encode",
    "gateway_query_response_collect",
    "gateway_query_response_decode",
    "gateway_query_local_materialize",
    "gateway_meta_allocate_start",
    "gateway_meta_reserve_commit",
    "gateway_data_apply_rpc",
    "gateway_meta_resolve_commit",
    "data_route_lock_wait",
    "data_route_lookup",
    "data_raft_propose",
    "data_raft_drive_ready",
    "data_read_view_cache_hit",
    "data_read_view_cache_miss",
    "data_read_view_open",
    "data_temporal_point_evaluation",
    "data_temporal_scan_id_collection",
    "data_temporal_scan_visibility",
    "data_raft_lock_wait",
    "gateway_meta_prepare_write",
    "data_raft_batch_admission",
    "data_raft_batch_queue",
    "data_raft_blocking_dispatch",
    "data_adjacency_cache_hit",
    "data_adjacency_cache_miss",
    "data_adjacency_backend_expand",
    "data_snapshot_csr_cache_hit",
    "data_snapshot_csr_cache_miss",
    "data_snapshot_csr_build",
    "gateway_query_session_submit",
    "gateway_query_session_response_wait",
    "data_gateway_session_execution",
];

const REQUEST_METRIC_DETAILS_V8: [&str; 34] = [
    "gateway_query_request_encode",
    "gateway_query_response_collect",
    "gateway_query_response_decode",
    "gateway_query_local_materialize",
    "gateway_meta_allocate_start",
    "gateway_meta_reserve_commit",
    "gateway_data_apply_rpc",
    "gateway_meta_resolve_commit",
    "data_route_lock_wait",
    "data_route_lookup",
    "data_raft_propose",
    "data_raft_drive_ready",
    "data_read_view_cache_hit",
    "data_read_view_cache_miss",
    "data_read_view_open",
    "data_temporal_point_evaluation",
    "data_temporal_scan_id_collection",
    "data_temporal_scan_visibility",
    "data_raft_lock_wait",
    "gateway_meta_prepare_write",
    "data_raft_batch_admission",
    "data_raft_batch_queue",
    "data_raft_blocking_dispatch",
    "data_adjacency_cache_hit",
    "data_adjacency_cache_miss",
    "data_adjacency_backend_expand",
    "data_snapshot_csr_cache_hit",
    "data_snapshot_csr_cache_miss",
    "data_snapshot_csr_build",
    "gateway_query_session_submit",
    "gateway_query_session_response_wait",
    "data_gateway_session_execution",
    "gateway_query_pipeline_submit",
    "gateway_query_pipeline_response_wait",
];

const REQUEST_METRIC_DETAILS_V9: [&str; 37] = [
    "gateway_query_request_encode",
    "gateway_query_response_collect",
    "gateway_query_response_decode",
    "gateway_query_local_materialize",
    "gateway_meta_allocate_start",
    "gateway_meta_reserve_commit",
    "gateway_data_apply_rpc",
    "gateway_meta_resolve_commit",
    "data_route_lock_wait",
    "data_route_lookup",
    "data_raft_propose",
    "data_raft_drive_ready",
    "data_read_view_cache_hit",
    "data_read_view_cache_miss",
    "data_read_view_open",
    "data_temporal_point_evaluation",
    "data_temporal_scan_id_collection",
    "data_temporal_scan_visibility",
    "data_raft_lock_wait",
    "gateway_meta_prepare_write",
    "data_raft_batch_admission",
    "data_raft_batch_queue",
    "data_raft_blocking_dispatch",
    "data_adjacency_cache_hit",
    "data_adjacency_cache_miss",
    "data_adjacency_backend_expand",
    "data_snapshot_csr_cache_hit",
    "data_snapshot_csr_cache_miss",
    "data_snapshot_csr_build",
    "gateway_query_session_submit",
    "gateway_query_session_response_wait",
    "data_gateway_session_execution",
    "gateway_query_pipeline_submit",
    "gateway_query_pipeline_response_wait",
    "bolt_read_pipeline_enqueue_wait",
    "bolt_read_pipeline_execution_wait",
    "bolt_read_pipeline_ordered_write_wait",
];

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Backend {
    Fjall,
    #[serde(rename = "postgresql")]
    PostgreSql,
    Kuzu,
}

impl Backend {
    const ALL: [Self; 3] = [Self::Fjall, Self::PostgreSql, Self::Kuzu];
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Workload {
    CreateVertex,
    PointLookup,
    OneHopExpand,
    TwoHopExpand,
    CountVertices,
}

impl Workload {
    const ALL: [Self; 5] = [
        Self::CreateVertex,
        Self::PointLookup,
        Self::OneHopExpand,
        Self::TwoHopExpand,
        Self::CountVertices,
    ];

    pub const fn is_write(self) -> bool {
        matches!(self, Self::CreateVertex)
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct CellSpec {
    pub backend: Backend,
    pub workload: Workload,
    pub concurrency: usize,
    pub repetition: u8,
}

impl CellSpec {
    pub fn matrix(seed: u64) -> Vec<Self> {
        let mut cells = Vec::with_capacity(135);
        for backend in Backend::ALL {
            for workload in Workload::ALL {
                for concurrency in [1, 8, 64] {
                    for repetition in 0..3 {
                        cells.push(Self {
                            backend,
                            workload,
                            concurrency,
                            repetition,
                        });
                    }
                }
            }
        }
        shuffle(&mut cells, seed);
        cells
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct RawObservation {
    pub backend: Backend,
    pub workload: Workload,
    pub concurrency: usize,
    pub repetition: u8,
    pub started_at_unix_ns: u64,
    pub finished_at_unix_ns: u64,
    pub warmup_finished_at_unix_ns: u64,
    pub measurement_started_at_unix_ns: u64,
    pub measurement_finished_at_unix_ns: u64,
    pub measured_duration_ns: u64,
    pub operations: u64,
    pub errors: u64,
    pub latency_samples_ns: Vec<u64>,
    pub row_count: u64,
    pub result_digest: String,
    pub query_digest: String,
    pub gateway_stage_metrics: Option<StageMetricsWindow>,
    pub data_stage_metrics: Option<StageMetricsWindow>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct StageMetricSnapshot {
    pub stage: String,
    pub buckets: Vec<u64>,
    pub success: u64,
    pub error: u64,
    pub cancelled: u64,
    pub total_nanoseconds: u64,
    pub max_nanoseconds: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProcessMetricsSnapshot {
    pub schema_version: u32,
    pub process_role: String,
    pub unix_timestamp_ns: u64,
    pub sequence: u64,
    pub stages: Vec<StageMetricSnapshot>,
    #[serde(default)]
    pub details: Vec<DetailMetricSnapshot>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DetailMetricSnapshot {
    pub detail: String,
    pub buckets: Vec<u64>,
    pub success: u64,
    pub error: u64,
    pub cancelled: u64,
    pub total_nanoseconds: u64,
    pub max_nanoseconds: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct StageMetricsDelta {
    pub before_sequence: u64,
    pub after_sequence: u64,
    pub stages: Vec<StageMetricDelta>,
    pub details: Vec<DetailMetricDelta>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct DetailMetricDelta {
    pub detail: String,
    pub buckets: Vec<u64>,
    pub success: u64,
    pub error: u64,
    pub cancelled: u64,
    pub total_nanoseconds: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct StageMetricDelta {
    pub stage: String,
    pub buckets: Vec<u64>,
    pub success: u64,
    pub error: u64,
    pub cancelled: u64,
    pub total_nanoseconds: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct StageMetricsWindow {
    pub before: ProcessMetricsSnapshot,
    pub after: ProcessMetricsSnapshot,
    pub delta: StageMetricsDelta,
}

pub fn stage_metrics_window_from_log(
    log: &str,
    expected_role: &str,
    measurement_started_at_unix_ns: u64,
    measurement_finished_at_unix_ns: u64,
) -> io::Result<StageMetricsWindow> {
    let snapshots = log
        .lines()
        .filter_map(|line| line.strip_prefix(REQUEST_METRICS_PREFIX))
        .map(|json| {
            serde_json::from_str::<ProcessMetricsSnapshot>(json)
                .map_err(|error| invalid_data(format!("invalid request metrics snapshot: {error}")))
        })
        .collect::<io::Result<Vec<_>>>()?;
    if snapshots.is_empty() {
        return Err(invalid_data(format!(
            "{expected_role} did not export request metrics"
        )));
    }

    let mut previous: Option<&ProcessMetricsSnapshot> = None;
    for snapshot in &snapshots {
        validate_metrics_snapshot(snapshot, expected_role)?;
        if let Some(previous) = previous {
            if snapshot.sequence <= previous.sequence {
                return Err(invalid_data("request metrics sequence did not increase"));
            }
            if snapshot.unix_timestamp_ns < previous.unix_timestamp_ns {
                return Err(invalid_data("request metrics timestamp regressed"));
            }
            ensure_counters_do_not_regress(previous, snapshot)?;
        }
        previous = Some(snapshot);
    }

    let before = snapshots
        .iter()
        .filter(|snapshot| snapshot.unix_timestamp_ns <= measurement_started_at_unix_ns)
        .max_by_key(|snapshot| snapshot.unix_timestamp_ns)
        .cloned()
        .ok_or_else(|| invalid_data("request metrics lack a pre-measurement snapshot"))?;
    let after = snapshots
        .iter()
        .filter(|snapshot| snapshot.unix_timestamp_ns >= measurement_finished_at_unix_ns)
        .min_by_key(|snapshot| snapshot.unix_timestamp_ns)
        .cloned()
        .ok_or_else(|| invalid_data("request metrics lack a post-measurement snapshot"))?;

    Ok(StageMetricsWindow {
        delta: metric_delta(&before, &after),
        before,
        after,
    })
}

fn validate_metrics_snapshot(
    snapshot: &ProcessMetricsSnapshot,
    expected_role: &str,
) -> io::Result<()> {
    if !matches!(snapshot.schema_version, 1..=9) {
        return Err(invalid_data(
            "request metrics schema version must be between 1 and 9",
        ));
    }
    if snapshot.process_role != expected_role {
        return Err(invalid_data(format!(
            "request metrics role must be {expected_role}"
        )));
    }
    if snapshot.unix_timestamp_ns == 0 {
        return Err(invalid_data("request metrics timestamp must be non-zero"));
    }
    if snapshot.stages.len() != REQUEST_METRIC_STAGES.len() {
        return Err(invalid_data("request metrics stage snapshot is incomplete"));
    }
    for (stage, expected_name) in snapshot.stages.iter().zip(REQUEST_METRIC_STAGES) {
        if stage.stage != expected_name || stage.buckets.len() != HISTOGRAM_BUCKETS {
            return Err(invalid_data("request metrics stage snapshot is incomplete"));
        }
    }
    if snapshot.schema_version == 1 {
        if !snapshot.details.is_empty() {
            return Err(invalid_data(
                "schema v1 request metrics cannot contain details",
            ));
        }
    } else {
        let expected_details: &[&str] = match snapshot.schema_version {
            2 => &REQUEST_METRIC_DETAILS_V2,
            3 => &REQUEST_METRIC_DETAILS_V3,
            4 => &REQUEST_METRIC_DETAILS_V4,
            5 => &REQUEST_METRIC_DETAILS_V5,
            6 => &REQUEST_METRIC_DETAILS_V6,
            7 => &REQUEST_METRIC_DETAILS_V7,
            8 => &REQUEST_METRIC_DETAILS_V8,
            9 => &REQUEST_METRIC_DETAILS_V9,
            _ => unreachable!("schema v1 is handled above"),
        };
        if snapshot.details.len() != expected_details.len()
            || snapshot
                .details
                .iter()
                .zip(expected_details)
                .any(|(detail, expected)| {
                    detail.detail != *expected || detail.buckets.len() != HISTOGRAM_BUCKETS
                })
        {
            return Err(invalid_data(
                "request metrics detail snapshot is incomplete",
            ));
        }
    }
    Ok(())
}

fn ensure_counters_do_not_regress(
    previous: &ProcessMetricsSnapshot,
    current: &ProcessMetricsSnapshot,
) -> io::Result<()> {
    for (previous, current) in previous.stages.iter().zip(&current.stages) {
        if counters_regressed(stage_counters(previous), stage_counters(current)) {
            return Err(invalid_data("request metrics counter regressed"));
        }
    }
    for (previous, current) in previous.details.iter().zip(&current.details) {
        if counters_regressed(detail_counters(previous), detail_counters(current)) {
            return Err(invalid_data("request metrics detail counter regressed"));
        }
    }
    Ok(())
}

struct MetricCounters<'a> {
    buckets: &'a [u64],
    success: u64,
    error: u64,
    cancelled: u64,
    total_nanoseconds: u64,
    max_nanoseconds: u64,
}

fn stage_counters(snapshot: &StageMetricSnapshot) -> MetricCounters<'_> {
    MetricCounters {
        buckets: &snapshot.buckets,
        success: snapshot.success,
        error: snapshot.error,
        cancelled: snapshot.cancelled,
        total_nanoseconds: snapshot.total_nanoseconds,
        max_nanoseconds: snapshot.max_nanoseconds,
    }
}

fn detail_counters(snapshot: &DetailMetricSnapshot) -> MetricCounters<'_> {
    MetricCounters {
        buckets: &snapshot.buckets,
        success: snapshot.success,
        error: snapshot.error,
        cancelled: snapshot.cancelled,
        total_nanoseconds: snapshot.total_nanoseconds,
        max_nanoseconds: snapshot.max_nanoseconds,
    }
}

fn counters_regressed(previous: MetricCounters<'_>, current: MetricCounters<'_>) -> bool {
    previous
        .buckets
        .iter()
        .zip(current.buckets)
        .any(|(before, after)| after < before)
        || current.success < previous.success
        || current.error < previous.error
        || current.cancelled < previous.cancelled
        || current.total_nanoseconds < previous.total_nanoseconds
        || current.max_nanoseconds < previous.max_nanoseconds
}

fn metric_delta(
    before: &ProcessMetricsSnapshot,
    after: &ProcessMetricsSnapshot,
) -> StageMetricsDelta {
    let stages = before
        .stages
        .iter()
        .zip(&after.stages)
        .map(|(before, after)| StageMetricDelta {
            stage: after.stage.clone(),
            buckets: after
                .buckets
                .iter()
                .zip(&before.buckets)
                .map(|(after, before)| after - before)
                .collect(),
            success: after.success - before.success,
            error: after.error - before.error,
            cancelled: after.cancelled - before.cancelled,
            total_nanoseconds: after.total_nanoseconds - before.total_nanoseconds,
        })
        .collect();
    let details = before
        .details
        .iter()
        .zip(&after.details)
        .map(|(before, after)| DetailMetricDelta {
            detail: after.detail.clone(),
            buckets: after
                .buckets
                .iter()
                .zip(&before.buckets)
                .map(|(after, before)| after - before)
                .collect(),
            success: after.success - before.success,
            error: after.error - before.error,
            cancelled: after.cancelled - before.cancelled,
            total_nanoseconds: after.total_nanoseconds - before.total_nanoseconds,
        })
        .collect();
    StageMetricsDelta {
        before_sequence: before.sequence,
        after_sequence: after.sequence,
        stages,
        details,
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct Summary {
    pub backend: Backend,
    pub workload: Workload,
    pub concurrency: usize,
    pub repetitions: usize,
    pub operations: u64,
    pub errors: u64,
    pub latency_samples: usize,
    pub p50_ns: u64,
    pub p95_ns: u64,
    pub p99_ns: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct QuickSummary {
    pub backend: Backend,
    pub workload: Workload,
    pub concurrency: usize,
    pub repetitions: usize,
    pub operations: u64,
    pub measured_duration_ns: u64,
    pub throughput_ops_per_second: f64,
    pub latency_samples: usize,
    pub p50_ns: u64,
    pub p95_ns: u64,
    pub p99_ns: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct QuickDiagnosticArtifact {
    pub format_version: u32,
    pub backend: Backend,
    pub revision: String,
    pub repetitions: usize,
    pub observations: Vec<RawObservation>,
    pub summaries: Vec<QuickSummary>,
}

impl QuickDiagnosticArtifact {
    pub fn new(revision: impl Into<String>, observations: Vec<RawObservation>) -> io::Result<Self> {
        let revision = revision.into();
        if revision.trim().is_empty() {
            return Err(invalid_data("quick diagnostic revision is empty"));
        }
        let backend = observations
            .first()
            .map(|observation| observation.backend)
            .ok_or_else(|| invalid_data("quick diagnostic has no observations"))?;
        let mut cells = BTreeSet::new();
        let mut result_identity = BTreeMap::<(Workload, usize), (u64, String, String)>::new();
        for observation in &observations {
            if observation.backend != backend {
                return Err(invalid_data("quick diagnostic mixes backend families"));
            }
            if !matches!(observation.concurrency, 1 | 8 | 64) {
                return Err(invalid_data(
                    "quick diagnostic concurrency must be 1, 8, or 64",
                ));
            }
            if observation.errors != 0
                || observation.operations == 0
                || observation.measured_duration_ns == 0
                || observation.latency_samples_ns.is_empty()
            {
                return Err(invalid_data(
                    "quick diagnostic observation is incomplete or contains errors",
                ));
            }
            if observation.gateway_stage_metrics.is_none()
                || observation.data_stage_metrics.is_none()
            {
                return Err(invalid_data(
                    "quick diagnostic observation lacks bracketing stage metrics",
                ));
            }
            if matches!(
                observation.workload,
                Workload::OneHopExpand | Workload::TwoHopExpand
            ) {
                let details = &observation
                    .data_stage_metrics
                    .as_ref()
                    .expect("data stage metrics were checked above")
                    .delta
                    .details;
                let csr_hits = details
                    .iter()
                    .find(|detail| detail.detail == "data_snapshot_csr_cache_hit")
                    .ok_or_else(|| {
                        invalid_data(
                            "quick diagnostic expand observation lacks snapshot CSR metrics",
                        )
                    })?;
                if csr_hits.success == 0 {
                    return Err(invalid_data(
                        "quick diagnostic expand observation lacks snapshot CSR cache hits",
                    ));
                }
                let adjacency_expands = details
                    .iter()
                    .find(|detail| detail.detail == "data_adjacency_backend_expand")
                    .ok_or_else(|| {
                        invalid_data("quick diagnostic expand observation lacks adjacency metrics")
                    })?;
                if adjacency_expands.success != 0 {
                    return Err(invalid_data(
                        "quick diagnostic expand observation used backend adjacency expansion",
                    ));
                }
            }
            if !cells.insert((
                observation.repetition,
                observation.workload,
                observation.concurrency,
            )) {
                return Err(invalid_data("quick diagnostic repeats a cell"));
            }
            if observation.workload.is_write() {
                if observation.row_count != 0 {
                    return Err(invalid_data("quick diagnostic write returned rows"));
                }
            } else {
                let identity = (
                    observation.row_count,
                    observation.result_digest.clone(),
                    observation.query_digest.clone(),
                );
                match result_identity.entry((observation.workload, observation.concurrency)) {
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        entry.insert(identity);
                    }
                    std::collections::btree_map::Entry::Occupied(entry)
                        if entry.get() != &identity =>
                    {
                        return Err(invalid_data("quick diagnostic read identity changed"));
                    }
                    std::collections::btree_map::Entry::Occupied(_) => {}
                }
            }
        }
        let repetitions = cells
            .iter()
            .map(|(repetition, _, _)| *repetition)
            .collect::<BTreeSet<_>>();
        let expected_observations = Workload::ALL.len() * 3 * 3;
        if repetitions != BTreeSet::from([0, 1, 2]) || observations.len() != expected_observations {
            return Err(invalid_data(
                "quick diagnostic requires exactly three complete repetitions",
            ));
        }
        for repetition in 0..3 {
            for workload in Workload::ALL {
                for concurrency in [1, 8, 64] {
                    if !cells.contains(&(repetition, workload, concurrency)) {
                        return Err(invalid_data("quick diagnostic matrix is incomplete"));
                    }
                }
            }
        }
        let summaries = summarize(&observations)
            .into_iter()
            .map(|summary| {
                let measured_duration_ns = observations
                    .iter()
                    .filter(|observation| {
                        observation.workload == summary.workload
                            && observation.concurrency == summary.concurrency
                    })
                    .map(|observation| observation.measured_duration_ns)
                    .sum::<u64>();
                let throughput_ops_per_second =
                    summary.operations as f64 * 1_000_000_000.0 / measured_duration_ns as f64;
                QuickSummary {
                    backend: summary.backend,
                    workload: summary.workload,
                    concurrency: summary.concurrency,
                    repetitions: summary.repetitions,
                    operations: summary.operations,
                    measured_duration_ns,
                    throughput_ops_per_second,
                    latency_samples: summary.latency_samples,
                    p50_ns: summary.p50_ns,
                    p95_ns: summary.p95_ns,
                    p99_ns: summary.p99_ns,
                }
            })
            .collect();
        Ok(Self {
            format_version: 1,
            backend,
            revision,
            repetitions: 3,
            observations,
            summaries,
        })
    }
}

pub fn write_quick_artifact(path: &Path, artifact: &QuickDiagnosticArtifact) -> io::Result<()> {
    if !path.is_absolute() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "quick diagnostic output path must be absolute",
        ));
    }
    let parent = path.parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "quick diagnostic output has no parent directory",
        )
    })?;
    let mut output = OpenOptions::new().create_new(true).write(true).open(path)?;
    serde_json::to_writer_pretty(&mut output, artifact).map_err(io::Error::other)?;
    output.write_all(b"\n")?;
    output.sync_all()?;
    File::open(parent)?.sync_all()
}

pub fn percentile_ns(samples: &[u64], percentile: u8) -> u64 {
    assert!((1..=100).contains(&percentile));
    assert!(!samples.is_empty());
    let mut ordered = samples.to_vec();
    ordered.sort_unstable();
    let rank = (ordered.len() * usize::from(percentile)).div_ceil(100);
    ordered[rank.saturating_sub(1)]
}

pub fn summarize(observations: &[RawObservation]) -> Vec<Summary> {
    let mut groups = BTreeMap::<(Backend, Workload, usize), Vec<&RawObservation>>::new();
    for observation in observations {
        groups
            .entry((
                observation.backend,
                observation.workload,
                observation.concurrency,
            ))
            .or_default()
            .push(observation);
    }
    groups
        .into_iter()
        .map(|((backend, workload, concurrency), observations)| {
            let samples = observations
                .iter()
                .flat_map(|observation| observation.latency_samples_ns.iter().copied())
                .collect::<Vec<_>>();
            Summary {
                backend,
                workload,
                concurrency,
                repetitions: observations.len(),
                operations: observations
                    .iter()
                    .map(|observation| observation.operations)
                    .sum(),
                errors: observations
                    .iter()
                    .map(|observation| observation.errors)
                    .sum(),
                latency_samples: samples.len(),
                p50_ns: percentile_or_zero(&samples, 50),
                p95_ns: percentile_or_zero(&samples, 95),
                p99_ns: percentile_or_zero(&samples, 99),
            }
        })
        .collect()
}

fn percentile_or_zero(samples: &[u64], percentile: u8) -> u64 {
    if samples.is_empty() {
        0
    } else {
        percentile_ns(samples, percentile)
    }
}

fn shuffle<T>(values: &mut [T], seed: u64) {
    let mut state = seed;
    for index in (1..values.len()).rev() {
        let swap = (splitmix64(&mut state) as usize) % (index + 1);
        values.swap(index, swap);
    }
}

fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut value = *state;
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn invalid_data(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}
