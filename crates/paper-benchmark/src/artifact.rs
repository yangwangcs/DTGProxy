use crate::manifest::validate_formal_proxy_evidence;
use crate::{
    Backend, ContractError, DatasetManifest, EnvironmentFingerprint, ExperimentPath,
    RawObservation, ResourceMetric, ResourceMetrics, ResultIdentity, RunManifest, RunMode,
    SCHEMA_VERSION, SampleSeries, SampleSummary, StatisticsError, TimingBoundaries, Topology,
    WorkloadManifest, summarize_repetitions, summarize_samples,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Display, Formatter};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

pub const FORMAL_VERTEX_COUNT: u64 = 1_000_000;
pub const FORMAL_EDGE_COUNT: u64 = 5_000_000;
pub const FORMAL_TEMPORAL_UPDATE_COUNT: u64 = 600_000;
pub const FORMAL_DATA_NODES: [u32; 3] = [1, 4, 8];
pub const FORMAL_CONCURRENCIES: [u32; 4] = [1, 8, 32, 64];
pub const FORMAL_WARMUP_SECONDS: u64 = 30;
pub const FORMAL_MEASUREMENT_SECONDS: u64 = 60;
pub const FORMAL_REPETITIONS: u32 = 5;

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug)]
pub struct ArtifactError {
    message: String,
}

impl ArtifactError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl Display for ArtifactError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for ArtifactError {}

impl From<std::io::Error> for ArtifactError {
    fn from(error: std::io::Error) -> Self {
        Self::new(format!("I/O error: {error}"))
    }
}

impl From<serde_json::Error> for ArtifactError {
    fn from(error: serde_json::Error) -> Self {
        Self::new(format!("JSON schema error: {error}"))
    }
}

impl From<ContractError> for ArtifactError {
    fn from(error: ContractError) -> Self {
        Self::new(format!("report contract error: {error}"))
    }
}

impl From<StatisticsError> for ArtifactError {
    fn from(error: StatisticsError) -> Self {
        Self::new(format!("statistics error: {error}"))
    }
}

pub type ArtifactResult<T> = Result<T, ArtifactError>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExperimentProtocol {
    pub warmup_seconds: u64,
    pub measurement_seconds: u64,
    pub repetitions: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExperimentSuite {
    pub kind: ExperimentSuiteKind,
    pub backends: Vec<Backend>,
    pub paths: Vec<ExperimentPath>,
    pub workloads: Vec<String>,
    pub data_nodes: Vec<u32>,
    pub concurrencies: Vec<u32>,
    pub ablations: Vec<String>,
    #[serde(default)]
    pub workload_ablations: BTreeMap<String, Vec<String>>,
}

impl ExperimentSuite {
    fn validate(&self) -> ArtifactResult<()> {
        require_unique_nonempty(&self.backends, "matrix.backends")?;
        require_unique_nonempty(&self.paths, "matrix.paths")?;
        require_unique_nonempty(&self.workloads, "matrix.workloads")?;
        require_unique_nonempty(&self.data_nodes, "matrix.data_nodes")?;
        require_unique_nonempty(&self.concurrencies, "matrix.concurrencies")?;
        require_unique_nonempty(&self.ablations, "matrix.ablations")?;
        if self.workloads.iter().any(|value| value.trim().is_empty()) {
            return Err(ArtifactError::new("invalid matrix.workloads"));
        }
        if self.ablations.iter().any(|value| value.trim().is_empty()) {
            return Err(ArtifactError::new("invalid matrix.ablations"));
        }
        if self.data_nodes.contains(&0) {
            return Err(ArtifactError::new("invalid matrix.data_nodes"));
        }
        if self.concurrencies.contains(&0) {
            return Err(ArtifactError::new("invalid matrix.concurrencies"));
        }
        let paths: BTreeSet<_> = self.paths.iter().copied().collect();
        let proxy_only = BTreeSet::from([ExperimentPath::Proxy]);
        match self.kind {
            ExperimentSuiteKind::Comparison => {
                let comparison = BTreeSet::from([
                    ExperimentPath::BackendDirect,
                    ExperimentPath::AdapterDirect,
                    ExperimentPath::Proxy,
                ]);
                if paths != comparison
                    || self.data_nodes.as_slice() != [1]
                    || self.ablations.as_slice() != ["production"]
                    || !self.workload_ablations.is_empty()
                {
                    return Err(ArtifactError::new("invalid comparison suite"));
                }
            }
            ExperimentSuiteKind::Scale => {
                if paths != proxy_only
                    || self.ablations.as_slice() != ["production"]
                    || !self.workload_ablations.is_empty()
                {
                    return Err(ArtifactError::new("invalid scale suite"));
                }
            }
            ExperimentSuiteKind::Ablation => {
                if paths != proxy_only
                    || self.data_nodes.len() != 1
                    || self.concurrencies.len() != 1
                    || !self.ablations.iter().any(|value| value == "production")
                    || self.ablations.len() < 2
                    || self.workload_ablations.len() != self.workloads.len()
                {
                    return Err(ArtifactError::new("invalid ablation suite"));
                }
                let workloads = self.workloads.iter().collect::<BTreeSet<_>>();
                if self.workload_ablations.keys().collect::<BTreeSet<_>>() != workloads
                    || self.workload_ablations.values().any(|labels| {
                        labels.len() != 2
                            || labels.first().map(String::as_str) != Some("production")
                            || labels[1] == "production"
                            || !self.ablations.contains(&labels[1])
                    })
                {
                    return Err(ArtifactError::new(
                        "ablation workloads must bind production and one single-disable label",
                    ));
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExperimentSuiteKind {
    Comparison,
    Scale,
    Ablation,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExperimentMatrix {
    pub suites: Vec<ExperimentSuite>,
}

impl ExperimentMatrix {
    pub fn validate(&self) -> ArtifactResult<()> {
        if self.suites.is_empty() {
            return Err(ArtifactError::new("matrix suites are empty"));
        }
        let mut kinds = BTreeSet::new();
        for suite in &self.suites {
            if !kinds.insert(suite.kind) {
                return Err(ArtifactError::new("duplicate matrix suite"));
            }
            suite.validate()?;
        }
        Ok(())
    }

    pub fn shuffled_cells(
        &self,
        repetitions: u32,
        shuffle_seed: u64,
    ) -> ArtifactResult<Vec<MatrixCell>> {
        self.validate()?;
        if repetitions == 0 {
            return Err(ArtifactError::new("invalid repetitions"));
        }
        let mut cells = BTreeSet::new();
        for suite in &self.suites {
            for backend in &suite.backends {
                for workload in &suite.workloads {
                    for data_nodes in &suite.data_nodes {
                        for concurrency in &suite.concurrencies {
                            let ablations = suite
                                .workload_ablations
                                .get(workload)
                                .unwrap_or(&suite.ablations);
                            for ablation in ablations {
                                let key = CellKey {
                                    backend: *backend,
                                    workload: workload.clone(),
                                    data_nodes: *data_nodes,
                                    concurrency: *concurrency,
                                    ablation: ablation.clone(),
                                };
                                for path in &suite.paths {
                                    for repetition in 1..=repetitions {
                                        cells.insert(MatrixCell {
                                            key: key.clone(),
                                            path: *path,
                                            repetition,
                                        });
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        let mut cells: Vec<_> = cells.into_iter().collect();
        deterministic_shuffle(&mut cells, shuffle_seed);
        Ok(cells)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkloadCase {
    pub manifest: WorkloadManifest,
    pub snapshot: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExperimentSpec {
    pub schema_version: u32,
    pub run_id: String,
    pub selected_backend: Backend,
    pub revision: String,
    pub dirty_worktree_digest: String,
    pub environment: EnvironmentFingerprint,
    pub dataset: DatasetManifest,
    pub workloads: Vec<WorkloadCase>,
    pub matrix: ExperimentMatrix,
    pub protocol: ExperimentProtocol,
    pub shuffle_seed: u64,
}

impl ExperimentSpec {
    pub fn into_manifest(self, simulator: bool) -> ArtifactResult<ArtifactManifest> {
        if self.schema_version != SCHEMA_VERSION {
            return Err(ArtifactError::new("invalid schema_version"));
        }
        require_safe_component(&self.run_id, "run_id")?;
        if self.revision.trim().is_empty() {
            return Err(ArtifactError::new("invalid revision"));
        }
        require_digest(&self.dirty_worktree_digest, "dirty_worktree_digest")?;
        self.dataset.validate()?;
        self.matrix.validate()?;
        if self.protocol.warmup_seconds == 0
            || self.protocol.measurement_seconds == 0
            || self.protocol.repetitions == 0
        {
            return Err(ArtifactError::new("invalid experiment protocol"));
        }
        validate_workloads(&self.workloads, &self.matrix)?;

        let mode = derive_run_mode(&self, simulator);
        self.environment.validate(mode)?;
        let workload_digest = digest_json(&self.workloads)?;
        let mut run = RunManifest {
            schema_version: SCHEMA_VERSION,
            mode,
            selected_backend: self.selected_backend,
            run_id: self.run_id,
            revision: self.revision,
            dirty_worktree_digest: self.dirty_worktree_digest,
            dataset_digest: self.dataset.content_digest.clone(),
            workload_digest,
            environment_digest: self.environment.digest.clone(),
            configuration_digest: String::new(),
            warmup_seconds: self.protocol.warmup_seconds,
            measurement_seconds: self.protocol.measurement_seconds,
            repetitions: self.protocol.repetitions,
        };
        run.configuration_digest = run.computed_configuration_digest()?;
        let manifest = ArtifactManifest {
            schema_version: SCHEMA_VERSION,
            selected_backend: self.selected_backend,
            run,
            environment: self.environment,
            dataset: self.dataset,
            workloads: self.workloads,
            matrix: self.matrix,
            shuffle_seed: self.shuffle_seed,
            simulator,
        };
        manifest.validate()?;
        Ok(manifest)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactManifest {
    pub schema_version: u32,
    pub selected_backend: Backend,
    pub run: RunManifest,
    pub environment: EnvironmentFingerprint,
    pub dataset: DatasetManifest,
    pub workloads: Vec<WorkloadCase>,
    pub matrix: ExperimentMatrix,
    pub shuffle_seed: u64,
    pub simulator: bool,
}

impl ArtifactManifest {
    pub fn validate(&self) -> ArtifactResult<()> {
        if self.schema_version != SCHEMA_VERSION {
            return Err(ArtifactError::new("invalid schema_version"));
        }
        self.run.validate()?;
        if self.run.selected_backend != self.selected_backend {
            return Err(ArtifactError::new("selected_backend identity mismatch"));
        }
        self.environment.validate(self.run.mode)?;
        if self.run.environment_digest != self.environment.digest {
            return Err(ArtifactError::new("environment identity mismatch"));
        }
        self.dataset.validate()?;
        self.matrix.validate()?;
        validate_workloads(&self.workloads, &self.matrix)?;
        if self.run.dataset_digest != self.dataset.content_digest {
            return Err(ArtifactError::new("dataset identity mismatch"));
        }
        if self.run.workload_digest != digest_json(&self.workloads)? {
            return Err(ArtifactError::new("workload identity mismatch"));
        }
        let spec = ExperimentSpec {
            schema_version: self.schema_version,
            run_id: self.run.run_id.clone(),
            selected_backend: self.selected_backend,
            revision: self.run.revision.clone(),
            dirty_worktree_digest: self.run.dirty_worktree_digest.clone(),
            environment: self.environment.clone(),
            dataset: self.dataset.clone(),
            workloads: self.workloads.clone(),
            matrix: self.matrix.clone(),
            protocol: ExperimentProtocol {
                warmup_seconds: self.run.warmup_seconds,
                measurement_seconds: self.run.measurement_seconds,
                repetitions: self.run.repetitions,
            },
            shuffle_seed: self.shuffle_seed,
        };
        if self.run.mode != derive_run_mode(&spec, self.simulator) {
            return Err(ArtifactError::new(
                "run mode does not match formal defaults",
            ));
        }
        if self.run.mode == RunMode::Formal {
            require_formal_comparison_axes(self.selected_backend, &self.matrix, &self.workloads)?;
        }
        Ok(())
    }

    pub fn protocol(&self) -> ExperimentProtocol {
        ExperimentProtocol {
            warmup_seconds: self.run.warmup_seconds,
            measurement_seconds: self.run.measurement_seconds,
            repetitions: self.run.repetitions,
        }
    }

    pub fn shuffled_cells(&self) -> ArtifactResult<Vec<MatrixCell>> {
        self.matrix
            .shuffled_cells(self.run.repetitions, self.shuffle_seed)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CellKey {
    pub backend: Backend,
    pub workload: String,
    pub data_nodes: u32,
    pub concurrency: u32,
    pub ablation: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MatrixCell {
    pub key: CellKey,
    pub path: ExperimentPath,
    pub repetition: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CellConfig {
    pub schema_version: u32,
    pub run_id: String,
    pub sequence: usize,
    pub cell: MatrixCell,
    pub dataset_digest: String,
    pub workload_digest: String,
    pub snapshot: String,
    pub parameters: BTreeMap<String, Value>,
    pub parameters_digest: String,
    pub query: String,
    pub protocol: ExperimentProtocol,
    pub configuration_digest: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MatrixPlanDocument {
    schema_version: u32,
    shuffle_seed: u64,
    cells: Vec<MatrixCell>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NumericSummary {
    pub count: usize,
    pub mean: f64,
    pub median: f64,
    pub sample_standard_deviation: f64,
    pub ci95_lower: Option<f64>,
    pub ci95_upper: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PercentileRepetitionSummary {
    pub p50: NumericSummary,
    pub p95: NumericSummary,
    pub p99: NumericSummary,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactCellSummary {
    pub key: CellKey,
    pub path: ExperimentPath,
    pub repetitions: u32,
    pub operations_total: u64,
    pub throughput: NumericSummary,
    pub latency_ns: SampleSummary,
    pub ttfr_ns: SampleSummary,
    pub latency_repetitions_ns: PercentileRepetitionSummary,
    pub ttfr_repetitions_ns: PercentileRepetitionSummary,
    pub cpu_time_ns: NumericSummary,
    pub peak_rss_bytes: NumericSummary,
    pub network_rx_bytes: NumericSummary,
    pub network_tx_bytes: NumericSummary,
    pub identity: ResultIdentity,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactSummary {
    pub schema_version: u32,
    pub run_id: String,
    pub mode: RunMode,
    pub cells: Vec<ArtifactCellSummary>,
}

#[derive(Debug)]
struct GeneratedFiles {
    summary_json: Vec<u8>,
    summary_csv: Vec<u8>,
    figure_csv: Vec<u8>,
}

pub fn derive_run_mode(spec: &ExperimentSpec, simulator: bool) -> RunMode {
    if simulator
        || spec.dataset.vertex_count != FORMAL_VERTEX_COUNT
        || spec.dataset.edge_count != FORMAL_EDGE_COUNT
        || spec.dataset.temporal_update_count != FORMAL_TEMPORAL_UPDATE_COUNT
        || !has_formal_suite_shape(&spec.matrix)
        || spec.protocol.warmup_seconds != FORMAL_WARMUP_SECONDS
        || spec.protocol.measurement_seconds != FORMAL_MEASUREMENT_SECONDS
        || spec.protocol.repetitions != FORMAL_REPETITIONS
    {
        RunMode::Diagnostic
    } else {
        RunMode::Formal
    }
}

fn validate_workloads(workloads: &[WorkloadCase], matrix: &ExperimentMatrix) -> ArtifactResult<()> {
    if workloads.is_empty() {
        return Err(ArtifactError::new("workloads are empty"));
    }
    let mut ids = BTreeSet::new();
    for workload in workloads {
        workload.manifest.validate()?;
        if workload.snapshot.trim().is_empty() {
            return Err(ArtifactError::new("invalid workload snapshot"));
        }
        if !ids.insert(workload.manifest.workload_id.clone()) {
            return Err(ArtifactError::new("duplicate workload manifest"));
        }
    }
    let matrix_ids: BTreeSet<_> = matrix
        .suites
        .iter()
        .flat_map(|suite| suite.workloads.iter().cloned())
        .collect();
    if ids != matrix_ids {
        return Err(ArtifactError::new(
            "matrix workloads do not match workload manifests",
        ));
    }
    Ok(())
}

fn require_formal_comparison_axes(
    selected_backend: Backend,
    matrix: &ExperimentMatrix,
    workloads: &[WorkloadCase],
) -> ArtifactResult<()> {
    let expected_paths = BTreeSet::from([
        ExperimentPath::BackendDirect,
        ExperimentPath::AdapterDirect,
        ExperimentPath::Proxy,
    ]);
    let expected_backends = BTreeSet::from([selected_backend]);
    if !has_formal_suite_shape(matrix) {
        return Err(ArtifactError::new("formal matrix suite shape mismatch"));
    }
    for suite in &matrix.suites {
        let actual_backends: BTreeSet<_> = suite.backends.iter().copied().collect();
        if actual_backends != expected_backends {
            return Err(ArtifactError::new(
                "suite backend differs from selected_backend",
            ));
        }
    }
    let manifests: BTreeMap<_, _> = workloads
        .iter()
        .map(|workload| (workload.manifest.workload_id.as_str(), &workload.manifest))
        .collect();
    let scale = matrix
        .suites
        .iter()
        .find(|suite| suite.kind == ExperimentSuiteKind::Scale)
        .ok_or_else(|| ArtifactError::new("formal scale suite is missing"))?;
    if scale.workloads.as_slice() != ["partition_parallel_scan"] {
        return Err(ArtifactError::new(
            "formal scale suite requires partition_parallel_scan as its bounded partition-parallel workload",
        ));
    }
    let scale_workload = manifests
        .get("partition_parallel_scan")
        .ok_or_else(|| ArtifactError::new("partition_parallel_scan manifest is missing"))?;
    if scale_workload.query
        != "MATCH (n) WHERE n.active = true WITH n.id AS id ORDER BY id LIMIT 4096 RETURN id"
        || scale_workload.available_paths.as_slice() != [ExperimentPath::Proxy]
    {
        return Err(ArtifactError::new(
            "partition_parallel_scan manifest does not match the formal contract",
        ));
    }
    for suite in &matrix.suites {
        for workload_id in &suite.workloads {
            let available: BTreeSet<_> = manifests[workload_id.as_str()]
                .available_paths
                .iter()
                .copied()
                .collect();
            let valid = match suite.kind {
                ExperimentSuiteKind::Comparison => available == expected_paths,
                ExperimentSuiteKind::Scale | ExperimentSuiteKind::Ablation => {
                    available.contains(&ExperimentPath::Proxy)
                }
            };
            if !valid {
                return Err(ArtifactError::new(format!(
                    "formal workload {workload_id} does not expose paths required by {:?}",
                    suite.kind
                )));
            }
        }
    }
    Ok(())
}

fn has_formal_suite_shape(matrix: &ExperimentMatrix) -> bool {
    let Some(comparison) = matrix
        .suites
        .iter()
        .find(|suite| suite.kind == ExperimentSuiteKind::Comparison)
    else {
        return false;
    };
    let Some(scale) = matrix
        .suites
        .iter()
        .find(|suite| suite.kind == ExperimentSuiteKind::Scale)
    else {
        return false;
    };
    let Some(ablation) = matrix
        .suites
        .iter()
        .find(|suite| suite.kind == ExperimentSuiteKind::Ablation)
    else {
        return false;
    };
    if matrix.suites.len() != 3 {
        return false;
    }
    let formal_ablations = BTreeSet::from([
        "production",
        "no_native_pushdown",
        "no_column_batch",
        "no_lazy_pages",
        "no_parallel_fanout",
        "no_batched_gather",
    ]);
    let expected_workload_ablations = BTreeMap::from([
        ("native_pushdown_filter", "no_native_pushdown"),
        ("column_batch_scan", "no_column_batch"),
        ("lazy_paged_scan", "no_lazy_pages"),
        ("parallel_fanout_count", "no_parallel_fanout"),
        ("batched_expand_gather", "no_batched_gather"),
    ]);
    comparison.data_nodes.as_slice() == [1]
        && same_u32_set(&comparison.concurrencies, &FORMAL_CONCURRENCIES)
        && comparison.ablations.as_slice() == ["production"]
        && comparison.workload_ablations.is_empty()
        && same_u32_set(&scale.data_nodes, &FORMAL_DATA_NODES)
        && scale.workloads.as_slice() == ["partition_parallel_scan"]
        && same_u32_set(&scale.concurrencies, &FORMAL_CONCURRENCIES)
        && scale.ablations.as_slice() == ["production"]
        && scale.workload_ablations.is_empty()
        && ablation.data_nodes.as_slice() == [8]
        && ablation.concurrencies.as_slice() == [32]
        && ablation
            .ablations
            .iter()
            .map(String::as_str)
            .collect::<BTreeSet<_>>()
            == formal_ablations
        && ablation.workload_ablations.len() == expected_workload_ablations.len()
        && expected_workload_ablations
            .iter()
            .all(|(workload, disabled)| {
                ablation
                    .workload_ablations
                    .get(*workload)
                    .is_some_and(|labels| labels.as_slice() == ["production", *disabled])
            })
}

fn same_u32_set(values: &[u32], expected: &[u32]) -> bool {
    let actual: BTreeSet<_> = values.iter().copied().collect();
    let expected: BTreeSet<_> = expected.iter().copied().collect();
    actual == expected && values.len() == expected.len()
}

fn require_unique_nonempty<T>(values: &[T], field: &str) -> ArtifactResult<()>
where
    T: Ord + Clone,
{
    if values.is_empty() || values.iter().cloned().collect::<BTreeSet<_>>().len() != values.len() {
        return Err(ArtifactError::new(format!("invalid {field}")));
    }
    Ok(())
}

fn require_safe_component(value: &str, field: &str) -> ArtifactResult<()> {
    if value.is_empty()
        || value == "."
        || value == ".."
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(ArtifactError::new(format!("invalid {field}")));
    }
    Ok(())
}

fn require_digest(value: &str, field: &str) -> ArtifactResult<()> {
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(ArtifactError::new(format!("invalid {field}")));
    }
    Ok(())
}

fn digest_json<T: Serialize>(value: &T) -> ArtifactResult<String> {
    Ok(blake3::hash(&serde_json::to_vec(value)?)
        .to_hex()
        .to_string())
}

fn deterministic_shuffle<T>(values: &mut [T], seed: u64) {
    let mut state = seed;
    for upper in (1..values.len()).rev() {
        let random = splitmix64(&mut state);
        let selected = (random % (upper as u64 + 1)) as usize;
        values.swap(upper, selected);
    }
}

fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut value = *state;
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn workload_for<'a>(
    manifest: &'a ArtifactManifest,
    workload_id: &str,
) -> ArtifactResult<&'a WorkloadCase> {
    manifest
        .workloads
        .iter()
        .find(|workload| workload.manifest.workload_id == workload_id)
        .ok_or_else(|| ArtifactError::new(format!("unknown workload: {workload_id}")))
}

fn cell_config(
    manifest: &ArtifactManifest,
    sequence: usize,
    cell: &MatrixCell,
) -> ArtifactResult<CellConfig> {
    let workload = workload_for(manifest, &cell.key.workload)?;
    let parameters_digest = digest_json(&workload.manifest.parameters)?;
    Ok(CellConfig {
        schema_version: SCHEMA_VERSION,
        run_id: manifest.run.run_id.clone(),
        sequence,
        cell: cell.clone(),
        dataset_digest: manifest.dataset.content_digest.clone(),
        workload_digest: workload.manifest.digest.clone(),
        snapshot: workload.snapshot.clone(),
        parameters: workload.manifest.parameters.clone(),
        parameters_digest,
        query: workload.manifest.query.clone(),
        protocol: manifest.protocol(),
        configuration_digest: manifest.run.configuration_digest.clone(),
    })
}

fn validate_cell_config(
    manifest: &ArtifactManifest,
    expected_sequence: usize,
    expected_cell: &MatrixCell,
    config: &CellConfig,
) -> ArtifactResult<()> {
    let expected = cell_config(manifest, expected_sequence, expected_cell)?;
    if config != &expected {
        return Err(ArtifactError::new(format!(
            "cell config mismatch at sequence {expected_sequence}"
        )));
    }
    Ok(())
}

pub fn validate_artifact_observations(
    manifest: &ArtifactManifest,
    observations: &[RawObservation],
) -> ArtifactResult<()> {
    manifest.validate()?;
    let expected_cells = manifest.shuffled_cells()?;
    let expected_set: BTreeSet<_> = expected_cells.iter().cloned().collect();
    let expected_keys: BTreeSet<_> = expected_cells.iter().map(|cell| cell.key.clone()).collect();
    let mut actual = BTreeMap::<MatrixCell, &RawObservation>::new();

    for observation in observations {
        observation.validate()?;
        if manifest.run.mode == RunMode::Formal {
            validate_formal_proxy_evidence(observation)?;
        }
        let cell = MatrixCell {
            key: CellKey {
                backend: observation.backend,
                workload: observation.workload.clone(),
                data_nodes: observation.topology.data_nodes,
                concurrency: observation.concurrency,
                ablation: observation.ablation.clone(),
            },
            path: observation.path,
            repetition: observation.repetition,
        };
        if actual.insert(cell.clone(), observation).is_some() {
            return Err(ArtifactError::new(format!(
                "duplicate raw observation: {}",
                describe_cell(&cell)
            )));
        }
        if !expected_set.contains(&cell) {
            return Err(ArtifactError::new(format!(
                "unexpected raw observation: {}",
                describe_cell(&cell)
            )));
        }
        validate_observation_binding(manifest, &cell, observation)?;
        if let Some(evidence) = &observation.ablation_evidence {
            let sequence = expected_cells
                .iter()
                .position(|expected| expected == &cell)
                .expect("observation cell was checked against expected_set");
            let expected_cell_id = format!("{}:{sequence}", manifest.run.run_id);
            if evidence.cell_id != expected_cell_id {
                return Err(ArtifactError::new(format!(
                    "ablation evidence cell_id mismatch: {}",
                    describe_cell(&cell)
                )));
            }
        }
        require_observed_metrics(observation)?;
    }

    if actual.len() != expected_cells.len() {
        let actual_keys: BTreeSet<_> = actual.keys().map(|cell| cell.key.clone()).collect();
        if actual_keys != expected_keys {
            return Err(ArtifactError::new(format!(
                "incomplete matrix: expected {} cells, found {}",
                expected_cells.len(),
                actual.len()
            )));
        }
        let missing = expected_cells
            .iter()
            .find(|cell| !actual.contains_key(*cell))
            .expect("matrix count differs, so one expected cell is absent");
        return Err(ArtifactError::new(format!(
            "missing repetition: {}",
            describe_cell(missing)
        )));
    }

    let mut workload_identities = BTreeMap::<&str, &ResultIdentity>::new();
    for observation in actual.values().copied() {
        match workload_identities.get(observation.workload.as_str()) {
            Some(identity) if *identity != &observation.identity => {
                return Err(ArtifactError::new(format!(
                    "result identity mismatch across paths, nodes, ablations, or repetitions for workload {}",
                    observation.workload
                )));
            }
            Some(_) => {}
            None => {
                workload_identities.insert(&observation.workload, &observation.identity);
            }
        }
    }
    Ok(())
}

fn validate_observation_binding(
    manifest: &ArtifactManifest,
    cell: &MatrixCell,
    observation: &RawObservation,
) -> ArtifactResult<()> {
    let workload = workload_for(manifest, &cell.key.workload)?;
    if observation.schema_version != SCHEMA_VERSION
        || observation.run_id != manifest.run.run_id
        || observation.dataset_digest != manifest.dataset.content_digest
        || observation.workload_digest != workload.manifest.digest
        || observation.snapshot != workload.snapshot
        || observation.parameters_digest != digest_json(&workload.manifest.parameters)?
        || observation.configuration_digest != manifest.run.configuration_digest
    {
        return Err(ArtifactError::new(format!(
            "raw observation binding mismatch: {}",
            describe_cell(cell)
        )));
    }
    let warmup_ns = manifest
        .run
        .warmup_seconds
        .checked_mul(1_000_000_000)
        .ok_or_else(|| ArtifactError::new("warmup duration overflow"))?;
    let measurement_ns = manifest
        .run
        .measurement_seconds
        .checked_mul(1_000_000_000)
        .ok_or_else(|| ArtifactError::new("measurement duration overflow"))?;
    let actual_warmup = observation
        .timing
        .measurement_started_unix_ns
        .checked_sub(observation.timing.warmup_started_unix_ns);
    let actual_measurement = observation
        .timing
        .measurement_ended_unix_ns
        .checked_sub(observation.timing.measurement_started_unix_ns);
    if actual_warmup != Some(warmup_ns) || actual_measurement != Some(measurement_ns) {
        return Err(ArtifactError::new(format!(
            "raw observation timing mismatch: {}",
            describe_cell(cell)
        )));
    }
    Ok(())
}

fn require_observed_metrics(observation: &RawObservation) -> ArtifactResult<()> {
    for (name, metric) in [
        ("cpu_time_ns", &observation.resources.cpu_time_ns),
        ("peak_rss_bytes", &observation.resources.peak_rss_bytes),
        ("network_rx_bytes", &observation.resources.network_rx_bytes),
        ("network_tx_bytes", &observation.resources.network_tx_bytes),
    ] {
        if let ResourceMetric::Unavailable { reason } = metric {
            return Err(ArtifactError::new(format!(
                "required metric unavailable: {name}: {reason}"
            )));
        }
    }
    Ok(())
}

fn observed_metric(metric: &ResourceMetric) -> ArtifactResult<u64> {
    match metric {
        ResourceMetric::Observed { value } => Ok(*value),
        ResourceMetric::Unavailable { reason } => Err(ArtifactError::new(format!(
            "required metric unavailable: {reason}"
        ))),
    }
}

fn describe_cell(cell: &MatrixCell) -> String {
    format!(
        "backend={} workload={} nodes={} concurrency={} ablation={} path={:?} repetition={}",
        cell.key.backend,
        cell.key.workload,
        cell.key.data_nodes,
        cell.key.concurrency,
        cell.key.ablation,
        cell.path,
        cell.repetition
    )
}

pub fn summarize_artifact(
    manifest: &ArtifactManifest,
    observations: &[RawObservation],
) -> ArtifactResult<ArtifactSummary> {
    validate_artifact_observations(manifest, observations)?;
    let mut groups = BTreeMap::<(CellKey, ExperimentPath), Vec<&RawObservation>>::new();
    for observation in observations {
        let key = CellKey {
            backend: observation.backend,
            workload: observation.workload.clone(),
            data_nodes: observation.topology.data_nodes,
            concurrency: observation.concurrency,
            ablation: observation.ablation.clone(),
        };
        groups
            .entry((key, observation.path))
            .or_default()
            .push(observation);
    }

    let mut cells = Vec::with_capacity(groups.len());
    for ((key, path), mut repetitions) in groups {
        repetitions.sort_by_key(|observation| observation.repetition);
        let throughput: Vec<_> = repetitions
            .iter()
            .map(|observation| {
                observation.operations as f64 / manifest.run.measurement_seconds as f64
            })
            .collect();
        let mut latency = Vec::new();
        let mut ttfr = Vec::new();
        let mut latency_repetitions = Vec::new();
        let mut ttfr_repetitions = Vec::new();
        let mut cpu = Vec::new();
        let mut rss = Vec::new();
        let mut network_rx = Vec::new();
        let mut network_tx = Vec::new();
        let mut operations_total = 0_u64;
        for observation in &repetitions {
            operations_total = operations_total
                .checked_add(observation.operations)
                .ok_or_else(|| ArtifactError::new("operation count overflow"))?;
            latency.extend_from_slice(&observation.samples.latency_ns);
            ttfr.extend_from_slice(&observation.samples.ttfr_ns);
            latency_repetitions.push(summarize_samples(&observation.samples.latency_ns)?);
            ttfr_repetitions.push(summarize_samples(&observation.samples.ttfr_ns)?);
            cpu.push(observed_metric(&observation.resources.cpu_time_ns)? as f64);
            rss.push(observed_metric(&observation.resources.peak_rss_bytes)? as f64);
            network_rx.push(observed_metric(&observation.resources.network_rx_bytes)? as f64);
            network_tx.push(observed_metric(&observation.resources.network_tx_bytes)? as f64);
        }
        cells.push(ArtifactCellSummary {
            key,
            path,
            repetitions: repetitions.len() as u32,
            operations_total,
            throughput: summarize_numeric(&throughput)?,
            latency_ns: summarize_samples(&latency)?,
            ttfr_ns: summarize_samples(&ttfr)?,
            latency_repetitions_ns: summarize_repetition_percentiles(&latency_repetitions)?,
            ttfr_repetitions_ns: summarize_repetition_percentiles(&ttfr_repetitions)?,
            cpu_time_ns: summarize_numeric(&cpu)?,
            peak_rss_bytes: summarize_numeric(&rss)?,
            network_rx_bytes: summarize_numeric(&network_rx)?,
            network_tx_bytes: summarize_numeric(&network_tx)?,
            identity: repetitions[0].identity.clone(),
        });
    }
    Ok(ArtifactSummary {
        schema_version: SCHEMA_VERSION,
        run_id: manifest.run.run_id.clone(),
        mode: manifest.run.mode,
        cells,
    })
}

fn summarize_repetition_percentiles(
    repetitions: &[SampleSummary],
) -> ArtifactResult<PercentileRepetitionSummary> {
    let p50: Vec<_> = repetitions
        .iter()
        .map(|summary| summary.p50 as f64)
        .collect();
    let p95: Vec<_> = repetitions
        .iter()
        .map(|summary| summary.p95 as f64)
        .collect();
    let p99: Vec<_> = repetitions
        .iter()
        .map(|summary| summary.p99 as f64)
        .collect();
    Ok(PercentileRepetitionSummary {
        p50: summarize_numeric(&p50)?,
        p95: summarize_numeric(&p95)?,
        p99: summarize_numeric(&p99)?,
    })
}

fn summarize_numeric(values: &[f64]) -> ArtifactResult<NumericSummary> {
    if values.is_empty() || values.iter().any(|value| !value.is_finite()) {
        return Err(ArtifactError::new(
            "statistics input is empty or non-finite",
        ));
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let mean = sorted.iter().sum::<f64>() / sorted.len() as f64;
    let median = if sorted.len().is_multiple_of(2) {
        let middle = sorted.len() / 2;
        (sorted[middle - 1] + sorted[middle]) / 2.0
    } else {
        sorted[sorted.len() / 2]
    };
    let sample_standard_deviation = if sorted.len() == 1 {
        0.0
    } else {
        let sum_of_squares = sorted
            .iter()
            .map(|value| {
                let difference = *value - mean;
                difference * difference
            })
            .sum::<f64>();
        (sum_of_squares / (sorted.len() - 1) as f64).sqrt()
    };
    let (ci95_lower, ci95_upper) = if sorted.len() >= 5 {
        let summary = summarize_repetitions(&sorted)?;
        (Some(summary.ci95_lower), Some(summary.ci95_upper))
    } else {
        (None, None)
    };
    Ok(NumericSummary {
        count: sorted.len(),
        mean,
        median,
        sample_standard_deviation,
        ci95_lower,
        ci95_upper,
    })
}

fn generate_files(
    manifest: &ArtifactManifest,
    observations: &[RawObservation],
) -> ArtifactResult<GeneratedFiles> {
    let summary = summarize_artifact(manifest, observations)?;
    let summary_json = pretty_json_bytes(&summary)?;
    let summary_csv = summary_csv(&summary).into_bytes();
    let figure_csv = figure_csv(&summary).into_bytes();
    Ok(GeneratedFiles {
        summary_json,
        summary_csv,
        figure_csv,
    })
}

fn summary_csv(summary: &ArtifactSummary) -> String {
    let mut output = String::from(
        "backend,path,workload,data_nodes,concurrency,ablation,repetitions,operations_total,throughput_mean,throughput_median,throughput_stddev,throughput_ci95_lower,throughput_ci95_upper,latency_p50_ns,latency_p95_ns,latency_p99_ns,ttfr_p50_ns,ttfr_p95_ns,ttfr_p99_ns,row_count,digest\n",
    );
    for cell in &summary.cells {
        output.push_str(&format!(
            "{},{},{},{},{},{},{},{},{:.12},{:.12},{:.12},{},{},{},{},{},{},{},{},{},{}\n",
            cell.key.backend,
            cell.path,
            csv_escape(&cell.key.workload),
            cell.key.data_nodes,
            cell.key.concurrency,
            csv_escape(&cell.key.ablation),
            cell.repetitions,
            cell.operations_total,
            cell.throughput.mean,
            cell.throughput.median,
            cell.throughput.sample_standard_deviation,
            optional_float(cell.throughput.ci95_lower),
            optional_float(cell.throughput.ci95_upper),
            cell.latency_ns.p50,
            cell.latency_ns.p95,
            cell.latency_ns.p99,
            cell.ttfr_ns.p50,
            cell.ttfr_ns.p95,
            cell.ttfr_ns.p99,
            cell.identity.row_count,
            cell.identity.digest,
        ));
    }
    output
}

fn figure_csv(summary: &ArtifactSummary) -> String {
    let mut output = String::from(
        "backend,path,workload,data_nodes,concurrency,ablation,throughput_mean,throughput_ci95_lower,throughput_ci95_upper\n",
    );
    for cell in &summary.cells {
        output.push_str(&format!(
            "{},{},{},{},{},{},{:.12},{},{}\n",
            cell.key.backend,
            cell.path,
            csv_escape(&cell.key.workload),
            cell.key.data_nodes,
            cell.key.concurrency,
            csv_escape(&cell.key.ablation),
            cell.throughput.mean,
            optional_float(cell.throughput.ci95_lower),
            optional_float(cell.throughput.ci95_upper),
        ));
    }
    output
}

fn optional_float(value: Option<f64>) -> String {
    value
        .map(|value| format!("{value:.12}"))
        .unwrap_or_default()
}

fn csv_escape(value: &str) -> String {
    if value.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_owned()
    }
}

fn pretty_json_bytes<T: Serialize>(value: &T) -> ArtifactResult<Vec<u8>> {
    let mut bytes = serde_json::to_vec_pretty(value)?;
    bytes.push(b'\n');
    Ok(bytes)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerificationReport {
    pub schema_version: u32,
    pub run_id: String,
    pub mode: RunMode,
    pub raw_observations: usize,
    pub summary_cells: usize,
    pub regenerated_directory: PathBuf,
}

pub fn run_experiment(
    spec: ExperimentSpec,
    output_root: &Path,
    simulator: bool,
    executor: Option<&Path>,
) -> ArtifactResult<PathBuf> {
    let manifest = spec.into_manifest(simulator)?;
    enforce_output_boundary(output_root, manifest.run.mode)?;
    if simulator && manifest.run.mode != RunMode::Diagnostic {
        return Err(ArtifactError::new("simulator runs must be diagnostic"));
    }
    if !simulator && executor.is_none() {
        return Err(ArtifactError::new(
            "an explicit cell executor is required for non-simulator runs",
        ));
    }
    if let Some(executor) = executor {
        let actual = crate::sha256::sha256_file(executor)?;
        if actual != manifest.environment.binaries.executor_sha256 {
            return Err(ArtifactError::new("executor SHA-256 mismatch"));
        }
    }

    fs::create_dir_all(output_root)?;
    let artifact = output_root.join(&manifest.run.run_id);
    fs::create_dir(&artifact).map_err(|error| {
        ArtifactError::new(format!(
            "artifact directory already exists or cannot be created: {}: {error}",
            artifact.display()
        ))
    })?;
    for directory in ["configs", "raw", "summary", "figures", "logs"] {
        fs::create_dir(artifact.join(directory))?;
    }
    fs::create_dir(artifact.join("configs/cells"))?;
    sync_directory(output_root)?;

    publish_json(&artifact.join("manifest.json"), &manifest)?;
    let cells = manifest.shuffled_cells()?;
    let matrix_plan = MatrixPlanDocument {
        schema_version: SCHEMA_VERSION,
        shuffle_seed: manifest.shuffle_seed,
        cells: cells.clone(),
    };
    publish_json(&artifact.join("configs/matrix.json"), &matrix_plan)?;

    let mut configs = Vec::with_capacity(cells.len());
    for (sequence, cell) in cells.iter().enumerate() {
        let config = cell_config(&manifest, sequence, cell)?;
        let path = artifact
            .join("configs/cells")
            .join(format!("{sequence:06}.json"));
        publish_json(&path, &config)?;
        configs.push((config, path));
    }

    let mut observations = Vec::with_capacity(cells.len());
    for (config, config_path) in &configs {
        let observation = if simulator {
            simulated_observation(&manifest, config)?
        } else {
            execute_external_cell(
                executor.expect("executor is checked above"),
                &artifact,
                config_path,
                config,
            )?
        };
        validate_single_observation(&manifest, config, &observation)?;
        let raw_path = artifact
            .join("raw")
            .join(format!("{:06}.json", config.sequence));
        publish_json(&raw_path, &observation)?;
        observations.push(observation);
    }

    validate_artifact_observations(&manifest, &observations)?;
    let generated = generate_files(&manifest, &observations)?;
    publish_new(
        &artifact.join("summary/summary.json"),
        &generated.summary_json,
    )?;
    publish_new(
        &artifact.join("summary/summary.csv"),
        &generated.summary_csv,
    )?;
    publish_new(
        &artifact.join("figures/throughput.csv"),
        &generated.figure_csv,
    )?;
    let log = format!(
        "schema_version={}\nrun_id={}\nmode={:?}\nraw_observations={}\nautomatic_retries=0\n",
        SCHEMA_VERSION,
        manifest.run.run_id,
        manifest.run.mode,
        observations.len()
    );
    publish_new(&artifact.join("logs/run.log"), log.as_bytes())?;
    write_checksums(&artifact)?;
    Ok(artifact)
}

pub fn verify_artifact(
    artifact: &Path,
    regenerate_dir: &Path,
) -> ArtifactResult<VerificationReport> {
    let artifact_root = fs::canonicalize(artifact).map_err(|error| {
        ArtifactError::new(format!(
            "artifact cannot be canonicalized: {}: {error}",
            artifact.display()
        ))
    })?;
    let regeneration_target = canonicalize_future_path(regenerate_dir)?;
    if regeneration_target == artifact_root || regeneration_target.starts_with(&artifact_root) {
        return Err(ArtifactError::new(
            "regeneration directory must be outside the artifact",
        ));
    }
    for directory in ["configs", "raw", "summary", "figures", "logs"] {
        if !artifact.join(directory).is_dir() {
            return Err(ArtifactError::new(format!(
                "artifact directory is missing: {directory}"
            )));
        }
    }
    verify_checksums(artifact)?;
    let manifest: ArtifactManifest = read_json(&artifact.join("manifest.json"))?;
    manifest.validate()?;
    let expected_cells = manifest.shuffled_cells()?;
    validate_required_artifact_files(artifact, &manifest, expected_cells.len())?;
    let plan: MatrixPlanDocument = read_json(&artifact.join("configs/matrix.json"))?;
    if plan.schema_version != SCHEMA_VERSION
        || plan.shuffle_seed != manifest.shuffle_seed
        || plan.cells != expected_cells
    {
        return Err(ArtifactError::new("matrix config does not match manifest"));
    }
    let config_paths = regular_files(&artifact.join("configs/cells"))?;
    if config_paths.len() != expected_cells.len() {
        return Err(ArtifactError::new("incomplete matrix cell configs"));
    }
    for (sequence, (expected_cell, path)) in expected_cells.iter().zip(&config_paths).enumerate() {
        let config: CellConfig = read_json(path)?;
        validate_cell_config(&manifest, sequence, expected_cell, &config)?;
    }

    let observations = read_raw_observations(&artifact.join("raw"))?;
    validate_artifact_observations(&manifest, &observations)?;
    let generated = generate_files(&manifest, &observations)?;
    if fs::read(artifact.join("summary/summary.json"))? != generated.summary_json
        || fs::read(artifact.join("summary/summary.csv"))? != generated.summary_csv
        || fs::read(artifact.join("figures/throughput.csv"))? != generated.figure_csv
    {
        return Err(ArtifactError::new(
            "summary is not reproducible from raw observations",
        ));
    }

    if let Some(parent) = regenerate_dir.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::create_dir(regenerate_dir).map_err(|error| {
        ArtifactError::new(format!(
            "regeneration directory already exists or cannot be created: {}: {error}",
            regenerate_dir.display()
        ))
    })?;
    fs::create_dir(regenerate_dir.join("summary"))?;
    fs::create_dir(regenerate_dir.join("figures"))?;
    publish_new(
        &regenerate_dir.join("summary/summary.json"),
        &generated.summary_json,
    )?;
    publish_new(
        &regenerate_dir.join("summary/summary.csv"),
        &generated.summary_csv,
    )?;
    publish_new(
        &regenerate_dir.join("figures/throughput.csv"),
        &generated.figure_csv,
    )?;
    sync_directory(regenerate_dir)?;

    Ok(VerificationReport {
        schema_version: SCHEMA_VERSION,
        run_id: manifest.run.run_id,
        mode: manifest.run.mode,
        raw_observations: observations.len(),
        summary_cells: generated_summary_cell_count(&generated.summary_json)?,
        regenerated_directory: regenerate_dir.to_path_buf(),
    })
}

fn validate_required_artifact_files(
    artifact: &Path,
    manifest: &ArtifactManifest,
    cell_count: usize,
) -> ArtifactResult<()> {
    let mut required = BTreeSet::from([
        PathBuf::from("manifest.json"),
        PathBuf::from("configs/matrix.json"),
        PathBuf::from("summary/summary.json"),
        PathBuf::from("summary/summary.csv"),
        PathBuf::from("figures/throughput.csv"),
        PathBuf::from("logs/run.log"),
    ]);
    for sequence in 0..cell_count {
        required.insert(PathBuf::from(format!("configs/cells/{sequence:06}.json")));
        required.insert(PathBuf::from(format!("raw/{sequence:06}.json")));
        if !manifest.simulator {
            required.insert(PathBuf::from(format!("logs/{sequence:06}.stdout.log")));
            required.insert(PathBuf::from(format!("logs/{sequence:06}.stderr.log")));
        }
    }
    let actual: BTreeSet<_> = recursive_files(artifact, true)?.into_iter().collect();
    if actual != required {
        return Err(ArtifactError::new(
            "required artifact file set does not match manifest",
        ));
    }
    Ok(())
}

fn canonicalize_future_path(path: &Path) -> ArtifactResult<PathBuf> {
    let mut existing = path;
    let mut suffix = Vec::new();
    while !existing.exists() {
        let name = existing
            .file_name()
            .ok_or_else(|| ArtifactError::new("regeneration path has no existing ancestor"))?;
        suffix.push(name.to_os_string());
        existing = existing
            .parent()
            .ok_or_else(|| ArtifactError::new("regeneration path has no existing ancestor"))?;
    }
    let mut canonical = fs::canonicalize(existing)?;
    for component in suffix.into_iter().rev() {
        canonical.push(component);
    }
    Ok(canonical)
}

fn generated_summary_cell_count(bytes: &[u8]) -> ArtifactResult<usize> {
    let summary: ArtifactSummary = serde_json::from_slice(bytes)?;
    Ok(summary.cells.len())
}

fn enforce_output_boundary(output_root: &Path, mode: RunMode) -> ArtifactResult<()> {
    let is_formal_root = path_ends_with(output_root, &["artifacts", "paper-performance"]);
    match mode {
        RunMode::Formal if !is_formal_root => Err(ArtifactError::new(
            "formal runs must use artifacts/paper-performance as output root",
        )),
        RunMode::Diagnostic if is_formal_root => Err(ArtifactError::new(
            "diagnostic runs must not write under artifacts/paper-performance",
        )),
        _ => Ok(()),
    }
}

fn path_ends_with(path: &Path, expected: &[&str]) -> bool {
    let components: Vec<_> = path
        .components()
        .filter_map(|component| match component {
            Component::Normal(value) => value.to_str(),
            _ => None,
        })
        .collect();
    components.len() >= expected.len()
        && components[components.len() - expected.len()..] == *expected
}

fn simulated_observation(
    manifest: &ArtifactManifest,
    config: &CellConfig,
) -> ArtifactResult<RawObservation> {
    if !manifest.simulator || manifest.run.measurement_seconds != 1 {
        return Err(ArtifactError::new(
            "the built-in simulator requires a one-second diagnostic protocol",
        ));
    }
    let identity = ResultIdentity {
        digest: digest_json(&(
            &config.cell.key,
            &config.dataset_digest,
            &config.workload_digest,
            &config.snapshot,
            &config.parameters_digest,
        ))?,
        row_count: 4,
    };
    let operations = 4_u64 + (config.sequence as u64 % 3);
    let latency_ns: Vec<_> = (0..operations)
        .map(|index| {
            1_000_000
                + index * 10_000
                + u64::from(config.cell.key.concurrency) * 100
                + u64::from(path_ordinal(config.cell.path)) * 1_000
        })
        .collect();
    let ttfr_ns: Vec<_> = latency_ns.iter().map(|latency| latency / 2).collect();
    let warmup_ns = manifest.run.warmup_seconds * 1_000_000_000;
    let measurement_ns = manifest.run.measurement_seconds * 1_000_000_000;
    let base = 1_000_000_000_000_u64
        .checked_add(config.sequence as u64 * 10_000_000_000)
        .ok_or_else(|| ArtifactError::new("simulator timestamp overflow"))?;
    Ok(RawObservation {
        schema_version: SCHEMA_VERSION,
        run_id: manifest.run.run_id.clone(),
        path: config.cell.path,
        backend: config.cell.key.backend,
        workload: config.cell.key.workload.clone(),
        dataset_digest: config.dataset_digest.clone(),
        workload_digest: config.workload_digest.clone(),
        snapshot: config.snapshot.clone(),
        parameters_digest: config.parameters_digest.clone(),
        topology: Topology {
            data_nodes: config.cell.key.data_nodes,
        },
        concurrency: config.cell.key.concurrency,
        ablation: config.cell.key.ablation.clone(),
        repetition: config.cell.repetition,
        timing: TimingBoundaries {
            warmup_started_unix_ns: base,
            measurement_started_unix_ns: base + warmup_ns,
            measurement_ended_unix_ns: base + warmup_ns + measurement_ns,
        },
        operations,
        errors: 0,
        samples: SampleSeries {
            latency_ns,
            ttfr_ns,
        },
        resources: ResourceMetrics {
            cpu_time_ns: ResourceMetric::Observed {
                value: operations * 100_000,
            },
            peak_rss_bytes: ResourceMetric::Observed { value: 1_048_576 },
            network_rx_bytes: ResourceMetric::Observed {
                value: operations * 128,
            },
            network_tx_bytes: ResourceMetric::Observed {
                value: operations * 64,
            },
        },
        resource_scope: if config.cell.path == ExperimentPath::Proxy {
            crate::ResourceScope::ClientAndProxyProcesses
        } else {
            crate::ResourceScope::ClientProcess
        },
        topology_evidence: None,
        identity,
        configuration_digest: config.configuration_digest.clone(),
        ablation_evidence: if config.cell.path == ExperimentPath::Proxy {
            Some(crate::AblationEvidence {
                gateway_pid: 1,
                cell_id: format!("{}:{}", config.run_id, config.sequence),
                configuration_digest: config.configuration_digest.clone(),
                config: crate::ablation_config_for_label(&config.cell.key.ablation)?,
                total_operations: operations,
                queries_started: operations,
                queries_completed: operations,
                queries_failed: 0,
                queries_in_flight: 0,
                counters: simulated_ablation_counters(&config.cell.key.ablation),
            })
        } else {
            None
        },
    })
}

fn simulated_ablation_counters(label: &str) -> crate::AblationCounters {
    let mut counters = crate::AblationCounters::default();
    match label {
        "no_native_pushdown" => counters.canonical_residual_scans = 1,
        "no_column_batch" => counters.row_column_conversion_boundaries = 1,
        "no_lazy_pages" => counters.eager_page_collections = 1,
        "no_parallel_fanout" => counters.serial_shard_opens = 1,
        "no_batched_gather" => counters.singleton_property_gather_reads = 1,
        _ => {}
    }
    counters
}

fn path_ordinal(path: ExperimentPath) -> u32 {
    match path {
        ExperimentPath::BackendDirect => 1,
        ExperimentPath::AdapterDirect => 2,
        ExperimentPath::Proxy => 3,
    }
}

fn execute_external_cell(
    executor: &Path,
    artifact: &Path,
    config_path: &Path,
    config: &CellConfig,
) -> ArtifactResult<RawObservation> {
    let output_path = artifact
        .join("raw")
        .join(format!(".executor-{:06}.tmp", config.sequence));
    if output_path.exists() {
        return Err(ArtifactError::new(
            "executor output temp file already exists",
        ));
    }
    let started_unix_ns = unix_now_ns()?;
    let output = match Command::new(executor)
        .arg("--cell-config")
        .arg(config_path)
        .arg("--output")
        .arg(&output_path)
        .output()
    {
        Ok(output) => output,
        Err(error) => {
            let message = format!("failed to start cell executor exactly once: {error}");
            record_executor_failure(
                executor,
                artifact,
                config,
                "spawn",
                started_unix_ns,
                None,
                &message,
                &output_path,
            )?;
            return Err(ArtifactError::new(message));
        }
    };
    publish_new(
        &artifact
            .join("logs")
            .join(format!("{:06}.stdout.log", config.sequence)),
        &output.stdout,
    )?;
    publish_new(
        &artifact
            .join("logs")
            .join(format!("{:06}.stderr.log", config.sequence)),
        &output.stderr,
    )?;
    if !output.status.success() {
        let message = format!(
            "cell executor failed at sequence {} with no automatic retry",
            config.sequence
        );
        record_executor_failure(
            executor,
            artifact,
            config,
            "exit_status",
            started_unix_ns,
            output.status.code(),
            &message,
            &output_path,
        )?;
        return Err(ArtifactError::new(message));
    }
    let bytes = match fs::read(&output_path) {
        Ok(bytes) => bytes,
        Err(error) => {
            let message = format!(
                "cell executor did not produce output at sequence {}: {error}",
                config.sequence
            );
            record_executor_failure(
                executor,
                artifact,
                config,
                "missing_output",
                started_unix_ns,
                output.status.code(),
                &message,
                &output_path,
            )?;
            return Err(ArtifactError::new(message));
        }
    };
    let observation = match serde_json::from_slice(&bytes) {
        Ok(observation) => observation,
        Err(error) => {
            let message = format!(
                "cell executor produced invalid JSON at sequence {}: {error}",
                config.sequence
            );
            record_executor_failure(
                executor,
                artifact,
                config,
                "invalid_output",
                started_unix_ns,
                output.status.code(),
                &message,
                &output_path,
            )?;
            return Err(ArtifactError::new(message));
        }
    };
    fs::remove_file(&output_path)?;
    Ok(observation)
}

#[derive(Serialize)]
struct ExecutorFailure<'a> {
    schema_version: u32,
    sequence: usize,
    stage: &'a str,
    executor_sha256: String,
    exit_code: Option<i32>,
    started_unix_ns: u64,
    ended_unix_ns: u64,
    message: &'a str,
    preserved_output: Option<String>,
}

#[allow(clippy::too_many_arguments)]
fn record_executor_failure(
    executor: &Path,
    artifact: &Path,
    config: &CellConfig,
    stage: &str,
    started_unix_ns: u64,
    exit_code: Option<i32>,
    message: &str,
    output_path: &Path,
) -> ArtifactResult<()> {
    let preserved_output = if output_path.is_file() {
        let relative = format!("logs/{:06}.executor-output.bin", config.sequence);
        publish_new(&artifact.join(&relative), &fs::read(output_path)?)?;
        fs::remove_file(output_path)?;
        Some(relative)
    } else {
        None
    };
    publish_json(
        &artifact.join("logs/failure.json"),
        &ExecutorFailure {
            schema_version: SCHEMA_VERSION,
            sequence: config.sequence,
            stage,
            executor_sha256: crate::sha256::sha256_file(executor)?,
            exit_code,
            started_unix_ns,
            ended_unix_ns: unix_now_ns()?,
            message,
            preserved_output,
        },
    )?;
    write_partial_checksums(artifact)
}

fn unix_now_ns() -> ArtifactResult<u64> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ArtifactError::new("system clock is before the Unix epoch"))?
        .as_nanos();
    u64::try_from(nanos).map_err(|_| ArtifactError::new("Unix timestamp does not fit u64"))
}

fn validate_single_observation(
    manifest: &ArtifactManifest,
    config: &CellConfig,
    observation: &RawObservation,
) -> ArtifactResult<()> {
    let actual = MatrixCell {
        key: CellKey {
            backend: observation.backend,
            workload: observation.workload.clone(),
            data_nodes: observation.topology.data_nodes,
            concurrency: observation.concurrency,
            ablation: observation.ablation.clone(),
        },
        path: observation.path,
        repetition: observation.repetition,
    };
    if actual != config.cell {
        return Err(ArtifactError::new(format!(
            "executor returned the wrong cell at sequence {}",
            config.sequence
        )));
    }
    observation.validate()?;
    validate_observation_binding(manifest, &actual, observation)?;
    require_observed_metrics(observation)
}

fn read_raw_observations(raw_directory: &Path) -> ArtifactResult<Vec<RawObservation>> {
    let paths = regular_files(raw_directory)?;
    let mut observations = Vec::with_capacity(paths.len());
    for path in paths {
        if path.extension().and_then(|value| value.to_str()) != Some("json") {
            return Err(ArtifactError::new(format!(
                "unexpected raw artifact file: {}",
                path.display()
            )));
        }
        observations.push(read_json(&path)?);
    }
    Ok(observations)
}

fn regular_files(directory: &Path) -> ArtifactResult<Vec<PathBuf>> {
    let mut paths = Vec::new();
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        if !file_type.is_file() {
            return Err(ArtifactError::new(format!(
                "unexpected non-file artifact entry: {}",
                entry.path().display()
            )));
        }
        paths.push(entry.path());
    }
    paths.sort();
    Ok(paths)
}

fn read_json<T>(path: &Path) -> ArtifactResult<T>
where
    T: for<'de> Deserialize<'de>,
{
    let mut bytes = Vec::new();
    File::open(path)?.read_to_end(&mut bytes)?;
    Ok(serde_json::from_slice(&bytes)?)
}

fn publish_json<T: Serialize>(path: &Path, value: &T) -> ArtifactResult<()> {
    publish_new(path, &pretty_json_bytes(value)?)
}

fn publish_new(path: &Path, bytes: &[u8]) -> ArtifactResult<()> {
    if path.exists() {
        return Err(ArtifactError::new(format!(
            "refusing to overwrite final artifact file: {}",
            path.display()
        )));
    }
    let parent = path
        .parent()
        .ok_or_else(|| ArtifactError::new("artifact file has no parent directory"))?;
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| ArtifactError::new("artifact filename is not valid UTF-8"))?;
    let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let temp = parent.join(format!(
        ".{file_name}.tmp-{}-{sequence}",
        std::process::id()
    ));
    let result = (|| -> ArtifactResult<()> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        fs::hard_link(&temp, path).map_err(|error| {
            ArtifactError::new(format!(
                "no-replace artifact publish failed for {}: {error}",
                path.display()
            ))
        })?;
        fs::remove_file(&temp)?;
        sync_directory(parent)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    result
}

fn sync_directory(path: &Path) -> ArtifactResult<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}

fn write_checksums(artifact: &Path) -> ArtifactResult<()> {
    let files = recursive_files(artifact, true)?;
    let mut output = String::new();
    for relative in files {
        let digest = crate::sha256::sha256_file(&artifact.join(&relative))?;
        output.push_str(&format!("{digest}  {}\n", relative.to_string_lossy()));
    }
    publish_new(&artifact.join("SHA256SUMS"), output.as_bytes())
}

fn write_partial_checksums(artifact: &Path) -> ArtifactResult<()> {
    let files = recursive_files(artifact, true)?;
    let mut output = String::new();
    for relative in files {
        if relative == Path::new("PARTIAL_SHA256SUMS") {
            continue;
        }
        let digest = crate::sha256::sha256_file(&artifact.join(&relative))?;
        output.push_str(&format!("{digest}  {}\n", relative.to_string_lossy()));
    }
    publish_new(&artifact.join("PARTIAL_SHA256SUMS"), output.as_bytes())
}

fn verify_checksums(artifact: &Path) -> ArtifactResult<()> {
    let checksum_path = artifact.join("SHA256SUMS");
    let content = fs::read_to_string(&checksum_path)
        .map_err(|error| ArtifactError::new(format!("missing SHA256SUMS: {error}")))?;
    let mut expected = BTreeMap::<PathBuf, String>::new();
    for (line_number, line) in content.lines().enumerate() {
        let (digest, relative) = line.split_once("  ").ok_or_else(|| {
            ArtifactError::new(format!("invalid SHA256SUMS line {}", line_number + 1))
        })?;
        require_digest(digest, "SHA256SUMS digest")?;
        let relative = PathBuf::from(relative);
        require_relative_artifact_path(&relative)?;
        if expected
            .insert(relative, digest.to_ascii_lowercase())
            .is_some()
        {
            return Err(ArtifactError::new("duplicate SHA256SUMS path"));
        }
    }
    let actual_files: BTreeSet<_> = recursive_files(artifact, true)?.into_iter().collect();
    let expected_files: BTreeSet<_> = expected.keys().cloned().collect();
    if actual_files != expected_files {
        return Err(ArtifactError::new(
            "checksum file set does not match artifact files",
        ));
    }
    for (relative, expected_digest) in expected {
        let actual_digest = crate::sha256::sha256_file(&artifact.join(&relative))?;
        if actual_digest != expected_digest {
            return Err(ArtifactError::new(format!(
                "checksum mismatch: {}",
                relative.display()
            )));
        }
    }
    Ok(())
}

fn recursive_files(artifact: &Path, exclude_checksums: bool) -> ArtifactResult<Vec<PathBuf>> {
    let mut pending = vec![artifact.to_path_buf()];
    let mut files = Vec::new();
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(&directory)? {
            let entry = entry?;
            let path = entry.path();
            let file_type = entry.file_type()?;
            if file_type.is_dir() {
                pending.push(path);
            } else if file_type.is_file() {
                let relative = path
                    .strip_prefix(artifact)
                    .map_err(|_| ArtifactError::new("artifact path escaped root"))?
                    .to_path_buf();
                if exclude_checksums && relative == Path::new("SHA256SUMS") {
                    continue;
                }
                if relative
                    .file_name()
                    .and_then(|value| value.to_str())
                    .is_some_and(|value| value.contains(".tmp-"))
                {
                    return Err(ArtifactError::new(
                        "temporary artifact file was not cleaned",
                    ));
                }
                files.push(relative);
            } else {
                return Err(ArtifactError::new(format!(
                    "unsupported artifact entry: {}",
                    path.display()
                )));
            }
        }
    }
    files.sort();
    Ok(files)
}

fn require_relative_artifact_path(path: &Path) -> ArtifactResult<()> {
    if path.as_os_str().is_empty()
        || path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(ArtifactError::new("invalid relative artifact path"));
    }
    Ok(())
}
