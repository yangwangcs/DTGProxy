mod ablation;
mod artifact;
mod identity;
mod manifest;
mod path;
mod sha256;
mod statistics;

pub use ablation::{
    AblationAxis, AblationConfig, AblationCounters, AblationEvidence, BenchmarkAblationConfig,
    BenchmarkAblationCounters, BenchmarkAblationCountersSnapshot, UnsupportedAblation,
    ablation_config_for_label, validate_ablation_exercised,
};
pub use artifact::{
    ArtifactCellSummary, ArtifactError, ArtifactManifest, ArtifactResult, ArtifactSummary,
    CellConfig, CellKey, CombinedBackendRun, CombinedReport, CombinedVerification,
    ExperimentMatrix, ExperimentProtocol, ExperimentSpec, ExperimentSuite, ExperimentSuiteKind,
    FORMAL_CONCURRENCIES, FORMAL_DATA_NODES, FORMAL_EDGE_COUNT, FORMAL_MEASUREMENT_SECONDS,
    FORMAL_REPETITIONS, FORMAL_TEMPORAL_UPDATE_COUNT, FORMAL_VERTEX_COUNT, FORMAL_WARMUP_SECONDS,
    MatrixCell, NumericSummary, PercentileRepetitionSummary, VerificationReport, WorkloadCase,
    combine_verified_runs, derive_run_mode, run_experiment, summarize_artifact,
    validate_artifact_observations, verify_artifact,
};
pub use identity::{ResultIdentity, validate_comparable_identities};
pub use manifest::{
    BinaryFingerprints, DatasetManifest, EnvironmentFingerprint, EnvironmentFingerprintSource,
    HostResourceEvidence, ProcessTopologyEvidence, RawObservation, ResourceMetric, ResourceMetrics,
    ResourceScope, RunManifest, RunMode, SCHEMA_VERSION, SampleSeries, SoftwareVersions,
    TimingBoundaries, Topology, TopologyDeploymentMode, TopologyEvidence, WorkloadManifest,
    validate_report_contract,
};
pub use path::{
    AdapterDirectRunner, AdapterPrimitive, AdapterSnapshotRequest, Backend, BackendDirectRunner,
    BackendNativeRequest, BenchmarkRequest, ExperimentPath, PathError, PathExecution,
    ProxyLoadgenReport, ProxyLoadgenRequest, ProxyRunner,
};
pub use sha256::{sha256_bytes, sha256_file};
pub use statistics::{
    RepetitionSummary, SampleSummary, StatisticsError, summarize_formal_repetitions,
    summarize_repetitions, summarize_samples,
};

use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContractError {
    InvalidField(&'static str),
    IdentityMismatch,
    ComparisonMismatch,
    EmptyComparison,
    Serialization(String),
}

impl Display for ContractError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidField(field) => write!(formatter, "invalid {field}"),
            Self::IdentityMismatch => formatter.write_str("result identity mismatch"),
            Self::ComparisonMismatch => formatter.write_str("comparison cell mismatch"),
            Self::EmptyComparison => formatter.write_str("no observations to compare"),
            Self::Serialization(message) => write!(formatter, "serialization failed: {message}"),
        }
    }
}

impl std::error::Error for ContractError {}

pub(crate) fn valid_digest(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}
