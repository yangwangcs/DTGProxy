use crate::{
    AblationEvidence, Backend, ContractError, ExperimentPath, ResultIdentity, valid_digest,
    validate_comparable_identities,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::path::PathBuf;

pub const SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunMode {
    Formal,
    Diagnostic,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EnvironmentFingerprintSource {
    Captured,
    Synthetic,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SoftwareVersions {
    pub rustc: String,
    pub cargo: String,
    pub rocksdb: String,
    pub postgresql: String,
    pub neo4j: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BinaryFingerprints {
    pub executor_sha256: String,
    pub gateway_sha256: String,
    pub data_node_sha256: String,
    pub meta_node_sha256: String,
    pub loadgen_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnvironmentFingerprint {
    pub schema_version: u32,
    pub source: EnvironmentFingerprintSource,
    pub os_name: String,
    pub os_version: String,
    pub architecture: String,
    pub cpu_model: String,
    pub logical_cpu_count: u32,
    pub total_memory_bytes: u64,
    pub versions: SoftwareVersions,
    pub binaries: BinaryFingerprints,
    pub digest: String,
}

impl EnvironmentFingerprint {
    pub fn synthetic() -> Self {
        let synthetic = "synthetic-diagnostic".to_owned();
        let sha = "0".repeat(64);
        let mut value = Self {
            schema_version: SCHEMA_VERSION,
            source: EnvironmentFingerprintSource::Synthetic,
            os_name: synthetic.clone(),
            os_version: synthetic.clone(),
            architecture: synthetic.clone(),
            cpu_model: synthetic.clone(),
            logical_cpu_count: 1,
            total_memory_bytes: 1,
            versions: SoftwareVersions {
                rustc: synthetic.clone(),
                cargo: synthetic.clone(),
                rocksdb: synthetic.clone(),
                postgresql: synthetic.clone(),
                neo4j: synthetic,
            },
            binaries: BinaryFingerprints {
                executor_sha256: sha.clone(),
                gateway_sha256: sha.clone(),
                data_node_sha256: sha.clone(),
                meta_node_sha256: sha.clone(),
                loadgen_sha256: sha,
            },
            digest: String::new(),
        };
        value.digest = value
            .computed_digest()
            .expect("synthetic fingerprint serializes");
        value
    }

    pub fn computed_digest(&self) -> Result<String, ContractError> {
        let bytes = serde_json::to_vec(&EnvironmentFingerprintDigestInput {
            schema_version: self.schema_version,
            source: self.source,
            os_name: &self.os_name,
            os_version: &self.os_version,
            architecture: &self.architecture,
            cpu_model: &self.cpu_model,
            logical_cpu_count: self.logical_cpu_count,
            total_memory_bytes: self.total_memory_bytes,
            versions: &self.versions,
            binaries: &self.binaries,
        })
        .map_err(|error| ContractError::Serialization(error.to_string()))?;
        Ok(crate::sha256::sha256_bytes(&bytes))
    }

    pub fn validate(&self, mode: RunMode) -> Result<(), ContractError> {
        validate_schema(self.schema_version)?;
        for value in [
            &self.os_name,
            &self.os_version,
            &self.architecture,
            &self.cpu_model,
        ] {
            nonempty(value, "environment")?;
        }
        nonzero(self.logical_cpu_count, "logical_cpu_count")?;
        nonzero(self.total_memory_bytes, "total_memory_bytes")?;
        for value in [
            &self.versions.rustc,
            &self.versions.cargo,
            &self.versions.rocksdb,
            &self.versions.postgresql,
            &self.versions.neo4j,
        ] {
            nonempty(value, "software_version")?;
            if mode == RunMode::Formal
                && matches!(
                    value.trim().to_ascii_lowercase().as_str(),
                    "unknown" | "synthetic" | "synthetic-diagnostic"
                )
            {
                return Err(ContractError::InvalidField("software_version"));
            }
        }
        for digest in [
            &self.binaries.executor_sha256,
            &self.binaries.gateway_sha256,
            &self.binaries.data_node_sha256,
            &self.binaries.meta_node_sha256,
            &self.binaries.loadgen_sha256,
        ] {
            validate_digest(digest, "binary_sha256")?;
        }
        if mode == RunMode::Formal && self.source != EnvironmentFingerprintSource::Captured {
            return Err(ContractError::InvalidField("environment.source"));
        }
        validate_digest(&self.digest, "environment.digest")?;
        if self.digest != self.computed_digest()? {
            return Err(ContractError::InvalidField("environment.digest"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DatasetManifest {
    pub schema_version: u32,
    pub dataset_id: String,
    pub seed: u64,
    pub vertex_count: u64,
    pub edge_count: u64,
    pub temporal_update_count: u64,
    pub content_digest: String,
}

impl DatasetManifest {
    pub fn validate(&self) -> Result<(), ContractError> {
        validate_schema(self.schema_version)?;
        nonempty(&self.dataset_id, "dataset_id")?;
        nonzero(self.vertex_count, "vertex_count")?;
        nonzero(self.edge_count, "edge_count")?;
        validate_digest(&self.content_digest, "content_digest")
    }

    pub fn canonical_digest(&self) -> Result<String, ContractError> {
        canonical_digest(&DatasetManifestDigestInput {
            schema_version: self.schema_version,
            dataset_id: &self.dataset_id,
            seed: self.seed,
            vertex_count: self.vertex_count,
            edge_count: self.edge_count,
            temporal_update_count: self.temporal_update_count,
            content_digest: &self.content_digest,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkloadManifest {
    pub schema_version: u32,
    pub workload_id: String,
    pub query: String,
    pub parameters: BTreeMap<String, Value>,
    pub available_paths: Vec<ExperimentPath>,
    pub digest: String,
}

impl WorkloadManifest {
    pub fn validate(&self) -> Result<(), ContractError> {
        validate_schema(self.schema_version)?;
        nonempty(&self.workload_id, "workload_id")?;
        nonempty(&self.query, "query")?;
        validate_digest(&self.digest, "digest")?;
        if self.available_paths.is_empty() {
            return Err(ContractError::InvalidField("available_paths"));
        }
        let unique: BTreeSet<_> = self.available_paths.iter().copied().collect();
        if unique.len() != self.available_paths.len() {
            return Err(ContractError::InvalidField("available_paths"));
        }
        if self.digest != self.computed_digest()? {
            return Err(ContractError::InvalidField("digest"));
        }
        Ok(())
    }

    pub fn computed_digest(&self) -> Result<String, ContractError> {
        canonical_digest(&WorkloadManifestDigestInput {
            schema_version: self.schema_version,
            workload_id: &self.workload_id,
            query: &self.query,
            parameters: &self.parameters,
            available_paths: &self.available_paths,
        })
    }

    pub fn canonical_digest(&self) -> Result<String, ContractError> {
        self.computed_digest()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunManifest {
    pub schema_version: u32,
    pub mode: RunMode,
    pub selected_backend: Backend,
    pub run_id: String,
    pub revision: String,
    pub dirty_worktree_digest: String,
    pub dataset_digest: String,
    pub workload_digest: String,
    pub environment_digest: String,
    pub configuration_digest: String,
    pub warmup_seconds: u64,
    pub measurement_seconds: u64,
    pub repetitions: u32,
}

impl RunManifest {
    pub fn validate(&self) -> Result<(), ContractError> {
        validate_schema(self.schema_version)?;
        nonempty(&self.run_id, "run_id")?;
        nonempty(&self.revision, "revision")?;
        validate_digest(&self.dirty_worktree_digest, "dirty_worktree_digest")?;
        validate_digest(&self.dataset_digest, "dataset_digest")?;
        validate_digest(&self.workload_digest, "workload_digest")?;
        validate_digest(&self.environment_digest, "environment_digest")?;
        validate_digest(&self.configuration_digest, "configuration_digest")?;
        match self.mode {
            RunMode::Formal => {
                if self.warmup_seconds != 30 {
                    return Err(ContractError::InvalidField("warmup_seconds"));
                }
                if self.measurement_seconds != 60 {
                    return Err(ContractError::InvalidField("measurement_seconds"));
                }
                if self.repetitions != 5 {
                    return Err(ContractError::InvalidField("repetitions"));
                }
            }
            RunMode::Diagnostic => {
                nonzero(self.warmup_seconds, "warmup_seconds")?;
                nonzero(self.measurement_seconds, "measurement_seconds")?;
                nonzero(self.repetitions, "repetitions")?;
            }
        }
        if self.configuration_digest != self.computed_configuration_digest()? {
            return Err(ContractError::InvalidField("configuration_digest"));
        }
        Ok(())
    }

    pub fn computed_configuration_digest(&self) -> Result<String, ContractError> {
        canonical_digest(&RunConfigurationDigestInput {
            schema_version: self.schema_version,
            mode: self.mode,
            selected_backend: self.selected_backend,
            run_id: &self.run_id,
            revision: &self.revision,
            dirty_worktree_digest: &self.dirty_worktree_digest,
            dataset_digest: &self.dataset_digest,
            workload_digest: &self.workload_digest,
            environment_digest: &self.environment_digest,
            warmup_seconds: self.warmup_seconds,
            measurement_seconds: self.measurement_seconds,
            repetitions: self.repetitions,
        })
    }

    pub fn canonical_digest(&self) -> Result<String, ContractError> {
        self.computed_configuration_digest()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Topology {
    pub data_nodes: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TimingBoundaries {
    pub warmup_started_unix_ns: u64,
    pub measurement_started_unix_ns: u64,
    pub measurement_ended_unix_ns: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SampleSeries {
    pub latency_ns: Vec<u64>,
    pub ttfr_ns: Vec<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResourceMetric {
    Observed { value: u64 },
    Unavailable { reason: String },
}

impl ResourceMetric {
    fn validate(&self) -> Result<(), ContractError> {
        match self {
            Self::Observed { .. } => Ok(()),
            Self::Unavailable { reason } => nonempty(reason, "resources.unavailable.reason"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceMetrics {
    pub cpu_time_ns: ResourceMetric,
    pub peak_rss_bytes: ResourceMetric,
    pub network_rx_bytes: ResourceMetric,
    pub network_tx_bytes: ResourceMetric,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceScope {
    ClientProcess,
    ClientAndProxyProcesses,
    DataNodeProcesses,
    FullSystem,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TopologyDeploymentMode {
    LocalDiagnostic,
    RemoteFormal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessTopologyEvidence {
    pub host_id: String,
    pub boot_id: String,
    pub process_start_id: String,
    pub pid: u32,
    pub executable: PathBuf,
    pub executable_sha256: String,
    pub listen_address: String,
    pub data_interface: String,
    pub management_interface: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostResourceEvidence {
    pub host_id: String,
    pub boot_id: String,
    pub process_start_id: String,
    pub pid: u32,
    pub sampled_before_unix_ns: u64,
    pub sampled_after_unix_ns: u64,
    pub cpu_time_ns: u64,
    pub peak_rss_bytes: u64,
    pub network_rx_bytes: u64,
    pub network_tx_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TopologyEvidence {
    pub deployment_mode: TopologyDeploymentMode,
    pub data_nodes: Vec<ProcessTopologyEvidence>,
    pub resource_samples: Vec<HostResourceEvidence>,
}

impl TopologyEvidence {
    fn validate(
        &self,
        expected_nodes: u32,
        timing: &TimingBoundaries,
        resources: &ResourceMetrics,
    ) -> Result<(), ContractError> {
        if self.deployment_mode != TopologyDeploymentMode::RemoteFormal
            || self.data_nodes.len() != expected_nodes as usize
            || self.resource_samples.len() != expected_nodes as usize
        {
            return Err(ContractError::InvalidField("topology_evidence"));
        }
        let mut hosts = BTreeSet::new();
        let mut processes = BTreeSet::new();
        for process in &self.data_nodes {
            nonempty(&process.host_id, "topology_evidence.host_id")?;
            nonempty(&process.boot_id, "topology_evidence.boot_id")?;
            nonempty(
                &process.process_start_id,
                "topology_evidence.process_start_id",
            )?;
            nonempty(&process.data_interface, "topology_evidence.data_interface")?;
            nonempty(
                &process.management_interface,
                "topology_evidence.management_interface",
            )?;
            if process.pid <= 1
                || !process.executable.is_absolute()
                || !valid_digest(&process.executable_sha256)
                || process.data_interface == process.management_interface
                || process
                    .listen_address
                    .parse::<SocketAddr>()
                    .map(|address| address.ip().is_loopback())
                    .unwrap_or(true)
                || !hosts.insert(process.host_id.as_str())
                || !processes.insert((
                    process.host_id.as_str(),
                    process.pid,
                    process.process_start_id.as_str(),
                ))
            {
                return Err(ContractError::InvalidField("topology_evidence"));
            }
        }

        let mut sampled = BTreeSet::new();
        let mut cpu_time_ns = 0_u64;
        let mut peak_rss_bytes = 0_u64;
        let mut network_rx_bytes = 0_u64;
        let mut network_tx_bytes = 0_u64;
        for sample in &self.resource_samples {
            let process = self
                .data_nodes
                .iter()
                .find(|process| process.host_id == sample.host_id)
                .ok_or(ContractError::InvalidField("topology_evidence"))?;
            if sample.boot_id != process.boot_id
                || sample.process_start_id != process.process_start_id
                || sample.pid != process.pid
                || sample.sampled_before_unix_ns > timing.measurement_started_unix_ns
                || sample.sampled_after_unix_ns < timing.measurement_ended_unix_ns
                || sample.sampled_before_unix_ns >= sample.sampled_after_unix_ns
                || !sampled.insert(sample.host_id.as_str())
            {
                return Err(ContractError::InvalidField("topology_evidence"));
            }
            cpu_time_ns = cpu_time_ns
                .checked_add(sample.cpu_time_ns)
                .ok_or(ContractError::InvalidField("resources"))?;
            peak_rss_bytes = peak_rss_bytes
                .checked_add(sample.peak_rss_bytes)
                .ok_or(ContractError::InvalidField("resources"))?;
            network_rx_bytes = network_rx_bytes
                .checked_add(sample.network_rx_bytes)
                .ok_or(ContractError::InvalidField("resources"))?;
            network_tx_bytes = network_tx_bytes
                .checked_add(sample.network_tx_bytes)
                .ok_or(ContractError::InvalidField("resources"))?;
        }
        if observed_value(&resources.cpu_time_ns) != Some(cpu_time_ns)
            || observed_value(&resources.peak_rss_bytes) != Some(peak_rss_bytes)
            || observed_value(&resources.network_rx_bytes) != Some(network_rx_bytes)
            || observed_value(&resources.network_tx_bytes) != Some(network_tx_bytes)
        {
            return Err(ContractError::InvalidField("resources"));
        }
        Ok(())
    }
}

fn observed_value(metric: &ResourceMetric) -> Option<u64> {
    match metric {
        ResourceMetric::Observed { value } => Some(*value),
        ResourceMetric::Unavailable { .. } => None,
    }
}

impl ResourceMetrics {
    fn validate(&self) -> Result<(), ContractError> {
        self.cpu_time_ns.validate()?;
        self.peak_rss_bytes.validate()?;
        self.network_rx_bytes.validate()?;
        self.network_tx_bytes.validate()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawObservation {
    pub schema_version: u32,
    pub run_id: String,
    pub path: ExperimentPath,
    pub backend: Backend,
    pub workload: String,
    pub dataset_digest: String,
    pub workload_digest: String,
    pub snapshot: String,
    pub parameters_digest: String,
    pub topology: Topology,
    pub concurrency: u32,
    pub ablation: String,
    pub repetition: u32,
    pub timing: TimingBoundaries,
    pub operations: u64,
    pub errors: u64,
    pub samples: SampleSeries,
    pub resources: ResourceMetrics,
    pub resource_scope: ResourceScope,
    pub topology_evidence: Option<TopologyEvidence>,
    pub identity: ResultIdentity,
    pub configuration_digest: String,
    pub ablation_evidence: Option<AblationEvidence>,
}

impl RawObservation {
    pub fn validate(&self) -> Result<(), ContractError> {
        validate_schema(self.schema_version)?;
        nonempty(&self.run_id, "run_id")?;
        nonempty(&self.workload, "workload")?;
        validate_digest(&self.dataset_digest, "dataset_digest")?;
        validate_digest(&self.workload_digest, "workload_digest")?;
        nonempty(&self.snapshot, "snapshot")?;
        validate_digest(&self.parameters_digest, "parameters_digest")?;
        nonempty(&self.ablation, "ablation")?;
        nonzero(self.topology.data_nodes, "topology.data_nodes")?;
        nonzero(self.concurrency, "concurrency")?;
        nonzero(self.repetition, "repetition")?;
        nonzero(self.operations, "operations")?;
        if self.errors != 0 {
            return Err(ContractError::InvalidField("errors"));
        }
        if self.timing.warmup_started_unix_ns >= self.timing.measurement_started_unix_ns
            || self.timing.measurement_started_unix_ns >= self.timing.measurement_ended_unix_ns
        {
            return Err(ContractError::InvalidField("timing"));
        }
        if self.samples.latency_ns.len() as u64 != self.operations
            || self.samples.ttfr_ns.len() as u64 != self.operations
            || self
                .samples
                .latency_ns
                .iter()
                .zip(&self.samples.ttfr_ns)
                .any(|(latency, ttfr)| ttfr > latency)
        {
            return Err(ContractError::InvalidField("samples"));
        }
        self.resources.validate()?;
        match (self.path, self.resource_scope, &self.topology_evidence) {
            (
                ExperimentPath::BackendDirect | ExperimentPath::AdapterDirect,
                ResourceScope::ClientProcess,
                None,
            ) => {}
            (ExperimentPath::Proxy, ResourceScope::ClientAndProxyProcesses, None) => {}
            (ExperimentPath::Proxy, ResourceScope::DataNodeProcesses, Some(evidence))
            | (ExperimentPath::Proxy, ResourceScope::FullSystem, Some(evidence)) => {
                evidence.validate(self.topology.data_nodes, &self.timing, &self.resources)?;
            }
            _ => return Err(ContractError::InvalidField("resource_scope")),
        }
        self.identity.validate()?;
        validate_digest(&self.configuration_digest, "configuration_digest")?;
        match (self.path, &self.ablation_evidence) {
            (ExperimentPath::Proxy, Some(evidence)) => {
                evidence.validate(&self.ablation, &self.configuration_digest, self.operations)
            }
            (ExperimentPath::Proxy, None) | (_, Some(_)) => {
                Err(ContractError::InvalidField("ablation_evidence"))
            }
            (_, None) => Ok(()),
        }
    }
}

#[derive(Serialize)]
struct DatasetManifestDigestInput<'a> {
    schema_version: u32,
    dataset_id: &'a str,
    seed: u64,
    vertex_count: u64,
    edge_count: u64,
    temporal_update_count: u64,
    content_digest: &'a str,
}

#[derive(Serialize)]
struct WorkloadManifestDigestInput<'a> {
    schema_version: u32,
    workload_id: &'a str,
    query: &'a str,
    parameters: &'a BTreeMap<String, Value>,
    available_paths: &'a [ExperimentPath],
}

#[derive(Serialize)]
struct RunConfigurationDigestInput<'a> {
    schema_version: u32,
    mode: RunMode,
    selected_backend: Backend,
    run_id: &'a str,
    revision: &'a str,
    dirty_worktree_digest: &'a str,
    dataset_digest: &'a str,
    workload_digest: &'a str,
    environment_digest: &'a str,
    warmup_seconds: u64,
    measurement_seconds: u64,
    repetitions: u32,
}

#[derive(Serialize)]
struct EnvironmentFingerprintDigestInput<'a> {
    schema_version: u32,
    source: EnvironmentFingerprintSource,
    os_name: &'a str,
    os_version: &'a str,
    architecture: &'a str,
    cpu_model: &'a str,
    logical_cpu_count: u32,
    total_memory_bytes: u64,
    versions: &'a SoftwareVersions,
    binaries: &'a BinaryFingerprints,
}

pub fn validate_report_contract<'a>(
    run: &RunManifest,
    dataset: &DatasetManifest,
    workload: &WorkloadManifest,
    observations: impl IntoIterator<Item = &'a RawObservation>,
) -> Result<(), ContractError> {
    dataset.validate()?;
    workload.validate()?;
    run.validate()?;

    if run.dataset_digest != dataset.content_digest || run.workload_digest != workload.digest {
        return Err(ContractError::IdentityMismatch);
    }

    let observations: Vec<_> = observations.into_iter().collect();
    if observations.is_empty() {
        return Err(ContractError::EmptyComparison);
    }

    let expected_configuration_digest = run.computed_configuration_digest()?;
    for observation in &observations {
        observation.validate()?;
        if observation.backend != run.selected_backend {
            return Err(ContractError::InvalidField(
                "observation.backend differs from selected_backend",
            ));
        }
        if observation.run_id != run.run_id
            || observation.dataset_digest != dataset.content_digest
            || observation.workload_digest != workload.digest
            || observation.workload != workload.workload_id
            || observation.configuration_digest != expected_configuration_digest
            || !workload.available_paths.contains(&observation.path)
        {
            return Err(ContractError::IdentityMismatch);
        }
        if observation.repetition == 0 || observation.repetition > run.repetitions {
            return Err(ContractError::InvalidField("repetition"));
        }
        validate_observation_timing(observation, run)?;
    }

    if run.mode == RunMode::Formal {
        for observation in &observations {
            validate_formal_proxy_evidence(observation)?;
        }
        validate_formal_cells(&observations, workload, run.repetitions)?;
    }

    Ok(())
}

pub(crate) fn validate_formal_proxy_evidence(
    observation: &RawObservation,
) -> Result<(), ContractError> {
    if observation.path == ExperimentPath::Proxy
        && (observation.resource_scope != ResourceScope::DataNodeProcesses
            || observation
                .topology_evidence
                .as_ref()
                .is_none_or(|evidence| {
                    evidence.deployment_mode != TopologyDeploymentMode::RemoteFormal
                }))
    {
        return Err(ContractError::InvalidField("topology_evidence"));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ComparableCellKey<'a> {
    run_id: &'a str,
    backend: Backend,
    workload: &'a str,
    dataset_digest: &'a str,
    workload_digest: &'a str,
    snapshot: &'a str,
    parameters_digest: &'a str,
    data_nodes: u32,
    concurrency: u32,
    ablation: &'a str,
    configuration_digest: &'a str,
}

impl<'a> From<&'a RawObservation> for ComparableCellKey<'a> {
    fn from(observation: &'a RawObservation) -> Self {
        Self {
            run_id: &observation.run_id,
            backend: observation.backend,
            workload: &observation.workload,
            dataset_digest: &observation.dataset_digest,
            workload_digest: &observation.workload_digest,
            snapshot: &observation.snapshot,
            parameters_digest: &observation.parameters_digest,
            data_nodes: observation.topology.data_nodes,
            concurrency: observation.concurrency,
            ablation: &observation.ablation,
            configuration_digest: &observation.configuration_digest,
        }
    }
}

fn validate_formal_cells(
    observations: &[&RawObservation],
    workload: &WorkloadManifest,
    repetitions: u32,
) -> Result<(), ContractError> {
    let mut cells: BTreeMap<
        ComparableCellKey<'_>,
        BTreeMap<(ExperimentPath, u32), &RawObservation>,
    > = BTreeMap::new();
    for observation in observations {
        let entries = cells
            .entry(ComparableCellKey::from(*observation))
            .or_default();
        if entries
            .insert((observation.path, observation.repetition), *observation)
            .is_some()
        {
            return Err(ContractError::ComparisonMismatch);
        }
    }

    let expected_paths = workload.available_paths.as_slice();
    for entries in cells.values() {
        let expected_count = expected_paths.len().saturating_mul(repetitions as usize);
        if entries.len() != expected_count {
            return Err(ContractError::ComparisonMismatch);
        }
        for repetition in 1..=repetitions {
            let comparable: Vec<_> = expected_paths
                .iter()
                .map(|path| {
                    entries
                        .get(&(*path, repetition))
                        .copied()
                        .ok_or(ContractError::ComparisonMismatch)
                })
                .collect::<Result<_, _>>()?;
            match expected_paths {
                [ExperimentPath::Proxy] => {}
                paths if paths.len() == 3 => {
                    validate_comparable_identities(comparable)?;
                }
                _ => return Err(ContractError::ComparisonMismatch),
            }
        }
    }
    Ok(())
}

fn validate_observation_timing(
    observation: &RawObservation,
    run: &RunManifest,
) -> Result<(), ContractError> {
    let expected_warmup = duration_ns(run.warmup_seconds, "warmup_seconds")?;
    let expected_measurement = duration_ns(run.measurement_seconds, "measurement_seconds")?;
    let actual_warmup = observation
        .timing
        .measurement_started_unix_ns
        .checked_sub(observation.timing.warmup_started_unix_ns)
        .ok_or(ContractError::InvalidField("timing"))?;
    let actual_measurement = observation
        .timing
        .measurement_ended_unix_ns
        .checked_sub(observation.timing.measurement_started_unix_ns)
        .ok_or(ContractError::InvalidField("timing"))?;
    if actual_warmup != expected_warmup || actual_measurement != expected_measurement {
        return Err(ContractError::InvalidField("timing"));
    }
    Ok(())
}

fn duration_ns(seconds: u64, field: &'static str) -> Result<u64, ContractError> {
    seconds
        .checked_mul(1_000_000_000)
        .ok_or(ContractError::InvalidField(field))
}

fn canonical_digest<T: Serialize>(value: &T) -> Result<String, ContractError> {
    let bytes = serde_json::to_vec(value)
        .map_err(|error| ContractError::Serialization(error.to_string()))?;
    Ok(blake3::hash(&bytes).to_hex().to_string())
}

fn validate_schema(schema_version: u32) -> Result<(), ContractError> {
    if schema_version != SCHEMA_VERSION {
        return Err(ContractError::InvalidField("schema_version"));
    }
    Ok(())
}

fn validate_digest(value: &str, field: &'static str) -> Result<(), ContractError> {
    if !valid_digest(value) {
        return Err(ContractError::InvalidField(field));
    }
    Ok(())
}

fn nonempty(value: &str, field: &'static str) -> Result<(), ContractError> {
    if value.trim().is_empty() {
        return Err(ContractError::InvalidField(field));
    }
    Ok(())
}

fn nonzero<T>(value: T, field: &'static str) -> Result<(), ContractError>
where
    T: Default + PartialEq,
{
    if value == T::default() {
        return Err(ContractError::InvalidField(field));
    }
    Ok(())
}
