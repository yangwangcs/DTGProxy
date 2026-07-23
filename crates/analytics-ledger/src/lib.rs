#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::str::FromStr;

use analytics_api::{AlgorithmResult, AlgorithmValue, VertexId};
use temporal_types::{TransactionTime, ValidTime};

const COMMAND_MAGIC: [u8; 4] = *b"DTJA";
const SNAPSHOT_MAGIC: [u8; 4] = *b"DTJL";
const JOB_RECORD_MAGIC: [u8; 4] = *b"DTJR";
const TOMBSTONE_RECORD_MAGIC: [u8; 4] = *b"DTJT";
const PARAMETER_MAGIC: [u8; 4] = *b"DTJP";
const FORMAT_VERSION: u16 = 1;
const CHECKSUM_BYTES: usize = 4;
const MAX_COMMAND_BYTES: usize = 16 * 1024 * 1024;
const MAX_SNAPSHOT_BYTES: usize = 64 * 1024 * 1024;
const MAX_JOBS: usize = 16_384;
const MAX_TOMBSTONES: usize = 262_144;
const MAX_SUBMISSION_REQUESTS: usize = 262_144;
const MAX_APPLIED_COMMANDS: usize = 8_192;
const MAX_PARAMETER_BYTES: usize = 1024 * 1024;
const MAX_PARAMETERS: usize = 4_096;
const RESULT_ARTIFACT_MAGIC: [u8; 4] = *b"DTAR";
const PROVIDER_CHECKPOINT_MAGIC: [u8; 4] = *b"DTPC";
const MAX_RESULT_ARTIFACT_BYTES: usize = 64 * 1024 * 1024;
const MAX_PROVIDER_STATE_BYTES: usize = 16 * 1024 * 1024;
const MAX_PROVIDER_CHECKPOINT_BYTES: usize =
    MAX_RESULT_ARTIFACT_BYTES + MAX_PROVIDER_STATE_BYTES + 1024;
const MAX_RESULT_COLUMNS: usize = 256;
const MAX_RESULT_ROWS: usize = 1_000_000;
const MAX_RESULT_METADATA: usize = 4_096;
const MAX_NAME_BYTES: usize = 256;
const MAX_ERROR_BYTES: usize = 4_096;
const MAX_INPUT_SHARDS: usize = 65_536;

/// Maximum number of chunks in a current analytics artifact generation.
pub const MAX_ARTIFACT_CHUNKS: u64 = 4_096;
/// Maximum payload size for a single current analytics artifact chunk.
pub const MAX_ARTIFACT_CHUNK_BYTES: u64 = 1024 * 1024;

/// Encodes typed algorithm parameters in the current canonical scheduler format.
///
/// Keys are emitted in `BTreeMap` order and values are scalar `AlgorithmValue`s, so the
/// current format has a fixed maximum nesting depth of one. The complete frame, including
/// its checksum, is bounded by the ledger parameter budget.
pub fn encode_algorithm_parameters(
    parameters: &BTreeMap<String, AlgorithmValue>,
) -> Result<Vec<u8>, JobError> {
    if parameters.len() > MAX_PARAMETERS {
        return Err(JobError::RecordTooLarge);
    }
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&PARAMETER_MAGIC);
    bytes.extend_from_slice(&FORMAT_VERSION.to_be_bytes());
    write_count(&mut bytes, parameters.len())?;
    for (name, value) in parameters {
        validate_name(name)?;
        write_string(&mut bytes, name)?;
        match value {
            AlgorithmValue::Null => bytes.push(1),
            AlgorithmValue::Boolean(value) => {
                bytes.push(2);
                bytes.push(u8::from(*value));
            }
            AlgorithmValue::Integer(value) => {
                bytes.push(3);
                bytes.extend_from_slice(&value.to_be_bytes());
            }
            AlgorithmValue::FloatBits(value) => {
                bytes.push(4);
                bytes.extend_from_slice(&value.to_be_bytes());
            }
            AlgorithmValue::String(value) => {
                bytes.push(5);
                write_bounded_string(&mut bytes, value, MAX_PARAMETER_BYTES)?;
            }
            AlgorithmValue::Vertex(value) => {
                bytes.push(6);
                bytes.extend_from_slice(&value.value().to_be_bytes());
            }
            AlgorithmValue::Time(value) => {
                bytes.push(7);
                bytes.extend_from_slice(&value.as_micros().to_be_bytes());
            }
        }
        if bytes.len() > MAX_PARAMETER_BYTES.saturating_sub(CHECKSUM_BYTES) {
            return Err(JobError::RecordTooLarge);
        }
    }
    append_checksum(&mut bytes);
    if bytes.len() > MAX_PARAMETER_BYTES {
        return Err(JobError::RecordTooLarge);
    }
    Ok(bytes)
}

/// Decodes and canonicality-checks typed algorithm parameters for a future scheduler.
pub fn decode_algorithm_parameters(
    bytes: &[u8],
) -> Result<BTreeMap<String, AlgorithmValue>, JobError> {
    if bytes.len() > MAX_PARAMETER_BYTES {
        return Err(JobError::RecordTooLarge);
    }
    verify_checksum(bytes)?;
    let mut reader = Reader::new(bytes)?;
    reader.expect_magic(PARAMETER_MAGIC)?;
    reader.expect_version()?;
    let mut parameters = BTreeMap::new();
    for _ in 0..reader.count(MAX_PARAMETERS)? {
        let name = reader.string(MAX_NAME_BYTES)?;
        validate_name(&name)?;
        let value = match reader.u8()? {
            1 => AlgorithmValue::Null,
            2 => match reader.u8()? {
                0 => AlgorithmValue::Boolean(false),
                1 => AlgorithmValue::Boolean(true),
                _ => return Err(JobError::NonCanonicalRecord),
            },
            3 => AlgorithmValue::Integer(reader.i64()?),
            4 => AlgorithmValue::FloatBits(reader.u64()?),
            5 => AlgorithmValue::String(reader.string(MAX_PARAMETER_BYTES)?),
            6 => AlgorithmValue::Vertex(VertexId::new(reader.u128()?)),
            7 => AlgorithmValue::Time(ValidTime::from_micros(reader.i64()?)),
            tag => return Err(JobError::UnknownParameterTag { tag }),
        };
        if parameters.insert(name, value).is_some() {
            return Err(JobError::NonCanonicalRecord);
        }
    }
    reader.finish()?;
    if encode_algorithm_parameters(&parameters)? != bytes {
        return Err(JobError::NonCanonicalRecord);
    }
    Ok(parameters)
}

/// Encodes a complete typed analytics result into the current canonical Artifact format.
///
/// The byte stream is independent of the physical backend. Shard Artifact storage may split this
/// stream into bounded chunks, while Meta publishes only the digest-bound generation Manifest.
pub fn encode_algorithm_result_artifact(result: &AlgorithmResult) -> Result<Vec<u8>, JobError> {
    if result.columns().is_empty()
        || result.columns().len() > MAX_RESULT_COLUMNS
        || result.rows().len() > MAX_RESULT_ROWS
        || result.metadata().len() > MAX_RESULT_METADATA
        || result
            .rows()
            .iter()
            .any(|row| row.len() != result.columns().len())
    {
        return Err(JobError::RecordTooLarge);
    }
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&RESULT_ARTIFACT_MAGIC);
    bytes.extend_from_slice(&FORMAT_VERSION.to_be_bytes());
    write_count(&mut bytes, result.columns().len())?;
    for column in result.columns() {
        validate_name(column)?;
        write_string(&mut bytes, column)?;
    }
    write_count(&mut bytes, result.rows().len())?;
    for row in result.rows() {
        for value in row {
            encode_algorithm_value(&mut bytes, value)?;
        }
        if bytes.len() > MAX_RESULT_ARTIFACT_BYTES.saturating_sub(CHECKSUM_BYTES) {
            return Err(JobError::RecordTooLarge);
        }
    }
    write_count(&mut bytes, result.metadata().len())?;
    for (name, value) in result.metadata() {
        validate_name(name)?;
        write_string(&mut bytes, name)?;
        encode_algorithm_value(&mut bytes, value)?;
    }
    if bytes.len() > MAX_RESULT_ARTIFACT_BYTES.saturating_sub(CHECKSUM_BYTES) {
        return Err(JobError::RecordTooLarge);
    }
    append_checksum(&mut bytes);
    Ok(bytes)
}

/// Decodes and canonicality-checks a complete typed analytics result Artifact.
pub fn decode_algorithm_result_artifact(bytes: &[u8]) -> Result<AlgorithmResult, JobError> {
    if bytes.len() > MAX_RESULT_ARTIFACT_BYTES {
        return Err(JobError::RecordTooLarge);
    }
    verify_checksum(bytes)?;
    let mut reader = Reader::new(bytes)?;
    reader.expect_magic(RESULT_ARTIFACT_MAGIC)?;
    reader.expect_version()?;
    let column_count = reader.count(MAX_RESULT_COLUMNS)?;
    if column_count == 0 {
        return Err(JobError::NonCanonicalRecord);
    }
    let mut columns = Vec::with_capacity(column_count);
    for _ in 0..column_count {
        let column = reader.string(MAX_NAME_BYTES)?;
        validate_name(&column)?;
        columns.push(column);
    }
    let row_count = reader.count(MAX_RESULT_ROWS)?;
    let mut rows = Vec::with_capacity(row_count);
    for _ in 0..row_count {
        let mut row = Vec::with_capacity(column_count);
        for _ in 0..column_count {
            row.push(decode_algorithm_value(&mut reader)?);
        }
        rows.push(row);
    }
    let mut metadata = BTreeMap::new();
    for _ in 0..reader.count(MAX_RESULT_METADATA)? {
        let name = reader.string(MAX_NAME_BYTES)?;
        validate_name(&name)?;
        if metadata
            .insert(name, decode_algorithm_value(&mut reader)?)
            .is_some()
        {
            return Err(JobError::NonCanonicalRecord);
        }
    }
    reader.finish()?;
    let result =
        AlgorithmResult::new(columns, rows, metadata).map_err(|_| JobError::NonCanonicalRecord)?;
    if encode_algorithm_result_artifact(&result)? != bytes {
        return Err(JobError::NonCanonicalRecord);
    }
    Ok(result)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderCheckpointArtifactV1 {
    projection_identity: [u8; 32],
    input_applied_indexes_digest: [u8; 32],
    algorithm: String,
    completed_units: u64,
    provider_state: Vec<u8>,
    result_prefix: Vec<u8>,
}

impl ProviderCheckpointArtifactV1 {
    pub fn new(
        projection_identity: [u8; 32],
        input_applied_indexes_digest: [u8; 32],
        algorithm: impl Into<String>,
        completed_units: u64,
        provider_state: Vec<u8>,
        result_prefix: Vec<u8>,
    ) -> Result<Self, JobError> {
        let algorithm = algorithm.into();
        if projection_identity == [0; 32] || input_applied_indexes_digest == [0; 32] {
            return Err(JobError::InvalidArtifactManifest);
        }
        validate_name(&algorithm)?;
        if provider_state.len() > MAX_PROVIDER_STATE_BYTES
            || result_prefix.len() > MAX_RESULT_ARTIFACT_BYTES
        {
            return Err(JobError::RecordTooLarge);
        }
        if !result_prefix.is_empty() {
            decode_algorithm_result_artifact(&result_prefix)?;
        }
        Ok(Self {
            projection_identity,
            input_applied_indexes_digest,
            algorithm,
            completed_units,
            provider_state,
            result_prefix,
        })
    }

    #[must_use]
    pub const fn projection_identity(&self) -> [u8; 32] {
        self.projection_identity
    }

    #[must_use]
    pub const fn input_applied_indexes_digest(&self) -> [u8; 32] {
        self.input_applied_indexes_digest
    }

    #[must_use]
    pub const fn completed_units(&self) -> u64 {
        self.completed_units
    }

    #[must_use]
    pub fn algorithm(&self) -> &str {
        &self.algorithm
    }

    #[must_use]
    pub fn provider_state(&self) -> &[u8] {
        &self.provider_state
    }

    #[must_use]
    pub fn result_prefix(&self) -> &[u8] {
        &self.result_prefix
    }
}

pub fn encode_provider_checkpoint_artifact(checkpoint: ProviderCheckpointArtifactV1) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(
        4 + 2
            + 32
            + 32
            + 4
            + checkpoint.algorithm.len()
            + 8
            + 4
            + checkpoint.provider_state.len()
            + 4
            + checkpoint.result_prefix.len()
            + CHECKSUM_BYTES,
    );
    bytes.extend_from_slice(&PROVIDER_CHECKPOINT_MAGIC);
    bytes.extend_from_slice(&FORMAT_VERSION.to_be_bytes());
    bytes.extend_from_slice(&checkpoint.projection_identity);
    bytes.extend_from_slice(&checkpoint.input_applied_indexes_digest);
    write_string(&mut bytes, &checkpoint.algorithm).expect("validated checkpoint algorithm");
    bytes.extend_from_slice(&checkpoint.completed_units.to_be_bytes());
    write_bytes(&mut bytes, &checkpoint.provider_state).expect("bounded provider state");
    write_bytes(&mut bytes, &checkpoint.result_prefix).expect("bounded result prefix");
    append_checksum(&mut bytes);
    bytes
}

pub fn decode_provider_checkpoint_artifact(
    bytes: &[u8],
) -> Result<ProviderCheckpointArtifactV1, JobError> {
    if bytes.len() > MAX_PROVIDER_CHECKPOINT_BYTES {
        return Err(JobError::RecordTooLarge);
    }
    verify_checksum(bytes)?;
    let mut reader = Reader::new(bytes)?;
    reader.expect_magic(PROVIDER_CHECKPOINT_MAGIC)?;
    reader.expect_version()?;
    let mut projection_identity = [0; 32];
    projection_identity.copy_from_slice(reader.bytes(32)?);
    let mut input_applied_indexes_digest = [0; 32];
    input_applied_indexes_digest.copy_from_slice(reader.bytes(32)?);
    let algorithm = reader.string(MAX_NAME_BYTES)?;
    let completed_units = reader.u64()?;
    let provider_state = reader.sized_bytes(MAX_PROVIDER_STATE_BYTES)?;
    let result_prefix = reader.sized_bytes(MAX_RESULT_ARTIFACT_BYTES)?;
    reader.finish()?;
    let checkpoint = ProviderCheckpointArtifactV1::new(
        projection_identity,
        input_applied_indexes_digest,
        algorithm,
        completed_units,
        provider_state,
        result_prefix,
    )?;
    if encode_provider_checkpoint_artifact(checkpoint.clone()) != bytes {
        return Err(JobError::NonCanonicalRecord);
    }
    Ok(checkpoint)
}

fn encode_algorithm_value(output: &mut Vec<u8>, value: &AlgorithmValue) -> Result<(), JobError> {
    match value {
        AlgorithmValue::Null => output.push(1),
        AlgorithmValue::Boolean(value) => {
            output.push(2);
            output.push(u8::from(*value));
        }
        AlgorithmValue::Integer(value) => {
            output.push(3);
            output.extend_from_slice(&value.to_be_bytes());
        }
        AlgorithmValue::FloatBits(value) => {
            output.push(4);
            output.extend_from_slice(&value.to_be_bytes());
        }
        AlgorithmValue::String(value) => {
            output.push(5);
            write_bounded_string(output, value, MAX_PARAMETER_BYTES)?;
        }
        AlgorithmValue::Vertex(value) => {
            output.push(6);
            output.extend_from_slice(&value.value().to_be_bytes());
        }
        AlgorithmValue::Time(value) => {
            output.push(7);
            output.extend_from_slice(&value.as_micros().to_be_bytes());
        }
    }
    Ok(())
}

fn decode_algorithm_value(reader: &mut Reader<'_>) -> Result<AlgorithmValue, JobError> {
    match reader.u8()? {
        1 => Ok(AlgorithmValue::Null),
        2 => match reader.u8()? {
            0 => Ok(AlgorithmValue::Boolean(false)),
            1 => Ok(AlgorithmValue::Boolean(true)),
            _ => Err(JobError::NonCanonicalRecord),
        },
        3 => Ok(AlgorithmValue::Integer(reader.i64()?)),
        4 => Ok(AlgorithmValue::FloatBits(reader.u64()?)),
        5 => Ok(AlgorithmValue::String(reader.string(MAX_PARAMETER_BYTES)?)),
        6 => Ok(AlgorithmValue::Vertex(VertexId::new(reader.u128()?))),
        7 => Ok(AlgorithmValue::Time(ValidTime::from_micros(reader.i64()?))),
        tag => Err(JobError::UnknownParameterTag { tag }),
    }
}

fn write_bounded_string(output: &mut Vec<u8>, value: &str, maximum: usize) -> Result<(), JobError> {
    if value.len() > maximum {
        return Err(JobError::RecordTooLarge);
    }
    write_count(output, value.len())?;
    output.extend_from_slice(value.as_bytes());
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct AnalyticsJobId(u128);

impl AnalyticsJobId {
    pub fn new(value: u128) -> Result<Self, JobError> {
        if value == 0 {
            return Err(JobError::InvalidJobId);
        }
        Ok(Self(value))
    }

    #[must_use]
    pub const fn value(self) -> u128 {
        self.0
    }
}

impl Display for AnalyticsJobId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:032x}", self.0)
    }
}

impl FromStr for AnalyticsJobId {
    type Err = JobError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.len() != 32
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(JobError::InvalidJobId);
        }
        let parsed = u128::from_str_radix(value, 16).map_err(|_| JobError::InvalidJobId)?;
        Self::new(parsed)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProjectionLimits {
    max_vertices: u64,
    max_edges: u64,
    max_bytes: u64,
}

impl ProjectionLimits {
    pub fn new(max_vertices: u64, max_edges: u64, max_bytes: u64) -> Result<Self, JobError> {
        if max_vertices == 0 || max_edges == 0 || max_bytes == 0 {
            return Err(JobError::InvalidProjectionLimits);
        }
        Ok(Self {
            max_vertices,
            max_edges,
            max_bytes,
        })
    }

    #[must_use]
    pub const fn max_vertices(self) -> u64 {
        self.max_vertices
    }

    #[must_use]
    pub const fn max_edges(self) -> u64 {
        self.max_edges
    }

    #[must_use]
    pub const fn max_bytes(self) -> u64 {
        self.max_bytes
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GraphProjectionScope {
    Snapshot {
        valid_time: ValidTime,
    },
    Event,
    Interval {
        valid_from: ValidTime,
        valid_to: ValidTime,
    },
    Delta {
        before: ValidTime,
        after: ValidTime,
    },
}

impl GraphProjectionScope {
    fn validate(&self) -> Result<(), JobError> {
        match self {
            Self::Interval {
                valid_from,
                valid_to,
            } if valid_from >= valid_to => Err(JobError::InvalidProjectionScope),
            Self::Delta { before, after } if before >= after => {
                Err(JobError::InvalidProjectionScope)
            }
            Self::Snapshot { .. } | Self::Event | Self::Interval { .. } | Self::Delta { .. } => {
                Ok(())
            }
        }
    }

    #[must_use]
    pub const fn graph_model(&self) -> analytics_api::GraphModel {
        match self {
            Self::Snapshot { .. } => analytics_api::GraphModel::Snapshot,
            Self::Event => analytics_api::GraphModel::Event,
            Self::Interval { .. } => analytics_api::GraphModel::Interval,
            Self::Delta { .. } => analytics_api::GraphModel::Delta,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JobSpec {
    job_id: AnalyticsJobId,
    submission_request_id: u128,
    graph_id: u64,
    catalog_revision: u64,
    topology_epoch: u64,
    schema_version: u64,
    backend_generation: u64,
    transaction_time: TransactionTime,
    projection: GraphProjectionScope,
    algorithm: String,
    algorithm_version: String,
    provider: String,
    provider_version: String,
    parameters: Vec<u8>,
    security_fingerprint: [u8; 32],
    limits: ProjectionLimits,
}

impl JobSpec {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        job_id: AnalyticsJobId,
        submission_request_id: u128,
        graph_id: u64,
        catalog_revision: u64,
        topology_epoch: u64,
        schema_version: u64,
        backend_generation: u64,
        transaction_time: TransactionTime,
        projection: GraphProjectionScope,
        algorithm: impl Into<String>,
        algorithm_version: impl Into<String>,
        provider: impl Into<String>,
        provider_version: impl Into<String>,
        parameters: Vec<u8>,
        security_fingerprint: [u8; 32],
        limits: ProjectionLimits,
    ) -> Result<Self, JobError> {
        let algorithm = algorithm.into();
        let algorithm_version = algorithm_version.into();
        let provider = provider.into();
        let provider_version = provider_version.into();
        if submission_request_id == 0
            || graph_id == 0
            || catalog_revision == 0
            || topology_epoch == 0
            || schema_version == 0
            || backend_generation == 0
            || security_fingerprint == [0; 32]
        {
            return Err(JobError::InvalidJobSpec);
        }
        validate_name(&algorithm)?;
        validate_name(&algorithm_version)?;
        validate_name(&provider)?;
        validate_name(&provider_version)?;
        if parameters.len() > MAX_PARAMETER_BYTES {
            return Err(JobError::RecordTooLarge);
        }
        projection.validate()?;
        Ok(Self {
            job_id,
            submission_request_id,
            graph_id,
            catalog_revision,
            topology_epoch,
            schema_version,
            backend_generation,
            transaction_time,
            projection,
            algorithm,
            algorithm_version,
            provider,
            provider_version,
            parameters,
            security_fingerprint,
            limits,
        })
    }

    #[must_use]
    pub const fn job_id(&self) -> AnalyticsJobId {
        self.job_id
    }

    #[must_use]
    pub const fn graph_id(&self) -> u64 {
        self.graph_id
    }

    #[must_use]
    pub const fn topology_epoch(&self) -> u64 {
        self.topology_epoch
    }

    #[must_use]
    pub const fn catalog_revision(&self) -> u64 {
        self.catalog_revision
    }

    #[must_use]
    pub const fn schema_version(&self) -> u64 {
        self.schema_version
    }

    #[must_use]
    pub const fn backend_generation(&self) -> u64 {
        self.backend_generation
    }

    #[must_use]
    pub const fn submission_request_id(&self) -> u128 {
        self.submission_request_id
    }

    #[must_use]
    pub const fn transaction_time(&self) -> TransactionTime {
        self.transaction_time
    }

    #[must_use]
    pub const fn projection(&self) -> &GraphProjectionScope {
        &self.projection
    }

    #[must_use]
    pub const fn graph_model(&self) -> analytics_api::GraphModel {
        self.projection.graph_model()
    }

    #[must_use]
    pub fn algorithm(&self) -> &str {
        &self.algorithm
    }

    #[must_use]
    pub fn algorithm_version(&self) -> &str {
        &self.algorithm_version
    }

    #[must_use]
    pub fn provider(&self) -> &str {
        &self.provider
    }

    #[must_use]
    pub fn provider_version(&self) -> &str {
        &self.provider_version
    }

    #[must_use]
    pub fn parameters(&self) -> &[u8] {
        &self.parameters
    }

    #[must_use]
    pub const fn security_fingerprint(&self) -> [u8; 32] {
        self.security_fingerprint
    }

    #[must_use]
    pub const fn limits(&self) -> ProjectionLimits {
        self.limits
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ArtifactKind {
    Checkpoint,
    Result,
}

/// Policy used by the cluster artifact garbage collector.
///
/// The policy is deliberately a pure ledger concern: it does not perform storage I/O and it
/// never treats a generation referenced by the Meta job record as deletable.  Storage adapters
/// can therefore scan generations independently and apply the returned plan with their own
/// idempotent, fenced delete command.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RetentionPolicy {
    max_generations_per_kind: usize,
    max_bytes_per_job: u64,
    terminal_ttl_ms: u64,
    orphan_ttl_ms: u64,
}

impl RetentionPolicy {
    pub fn new(
        max_generations_per_kind: usize,
        max_bytes_per_job: u64,
        terminal_ttl_ms: u64,
        orphan_ttl_ms: u64,
    ) -> Result<Self, JobError> {
        if max_generations_per_kind == 0
            || max_bytes_per_job == 0
            || terminal_ttl_ms == 0
            || orphan_ttl_ms == 0
        {
            return Err(JobError::InvalidRetentionPolicy);
        }
        Ok(Self {
            max_generations_per_kind,
            max_bytes_per_job,
            terminal_ttl_ms,
            orphan_ttl_ms,
        })
    }

    #[must_use]
    pub const fn max_generations_per_kind(self) -> usize {
        self.max_generations_per_kind
    }

    #[must_use]
    pub const fn max_bytes_per_job(self) -> u64 {
        self.max_bytes_per_job
    }

    #[must_use]
    pub const fn terminal_ttl_ms(self) -> u64 {
        self.terminal_ttl_ms
    }

    #[must_use]
    pub const fn orphan_ttl_ms(self) -> u64 {
        self.orphan_ttl_ms
    }

    /// Computes a deterministic deletion plan for one job.
    ///
    /// `generations` is the result of a storage scan.  The scan may contain generations that are
    /// not present in Meta (orphans), but the current checkpoint/result manifests are always
    /// protected.  Candidates are ordered by `(created_at, kind, generation)` and are selected
    /// only after their safety TTL has elapsed.  The plan is advisory and must be re-fenced by
    /// the storage layer immediately before deletion.
    pub fn plan(
        self,
        record: &JobRecord,
        generations: &[ArtifactGeneration],
        now_unix_ms: u64,
    ) -> Result<RetentionPlan, JobError> {
        if now_unix_ms == 0 {
            return Err(JobError::InvalidTimestamp);
        }
        let current = [
            record.checkpoint.as_ref().map(|manifest| {
                ArtifactGenerationKey::new(ArtifactKind::Checkpoint, manifest.generation)
            }),
            record.result.as_ref().map(|manifest| {
                ArtifactGenerationKey::new(ArtifactKind::Result, manifest.generation)
            }),
        ];
        let mut protected = Vec::new();
        let mut candidates = Vec::new();
        let mut total_bytes = 0_u64;
        for generation in generations {
            total_bytes = total_bytes
                .checked_add(generation.total_bytes)
                .ok_or(JobError::RetentionBytesExhausted)?;
            if current.iter().flatten().any(|key| *key == generation.key()) || generation.pinned {
                protected.push(generation.key());
                continue;
            }
            let age = now_unix_ms.saturating_sub(generation.created_at_unix_ms);
            let ttl = if record.state.is_terminal() {
                self.terminal_ttl_ms
            } else {
                self.orphan_ttl_ms
            };
            if age >= ttl {
                candidates.push(generation.clone());
            }
        }
        candidates.sort_by_key(|generation| {
            (
                generation.created_at_unix_ms,
                generation.key().kind,
                generation.key().generation,
            )
        });

        let mut by_kind = BTreeMap::<ArtifactKind, usize>::new();
        for key in &protected {
            let count = by_kind.entry(key.kind).or_default();
            *count = count.saturating_add(1);
        }
        for generation in &candidates {
            let count = by_kind.entry(generation.key().kind).or_default();
            *count = count.saturating_add(1);
        }
        let mut deletable = Vec::new();
        let mut reclaim_bytes = 0_u64;
        for generation in candidates {
            let count = by_kind.entry(generation.key().kind).or_default();
            let over_count = *count > self.max_generations_per_kind;
            let over_bytes = total_bytes.saturating_sub(reclaim_bytes) > self.max_bytes_per_job;
            if over_count || over_bytes {
                *count = count.saturating_sub(1);
                reclaim_bytes = reclaim_bytes
                    .checked_add(generation.total_bytes)
                    .ok_or(JobError::RetentionBytesExhausted)?;
                deletable.push(generation.key());
            }
        }
        Ok(RetentionPlan {
            protected,
            deletable,
            observed_bytes: total_bytes,
            reclaim_bytes,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ArtifactGenerationKey {
    kind: ArtifactKind,
    generation: u64,
}

impl ArtifactGenerationKey {
    #[must_use]
    pub const fn new(kind: ArtifactKind, generation: u64) -> Self {
        Self { kind, generation }
    }

    #[must_use]
    pub const fn kind(self) -> ArtifactKind {
        self.kind
    }

    #[must_use]
    pub const fn generation(self) -> u64 {
        self.generation
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArtifactGeneration {
    key: ArtifactGenerationKey,
    total_bytes: u64,
    created_at_unix_ms: u64,
    pinned: bool,
}

impl ArtifactGeneration {
    pub fn new(
        kind: ArtifactKind,
        generation: u64,
        total_bytes: u64,
        created_at_unix_ms: u64,
        pinned: bool,
    ) -> Result<Self, JobError> {
        if generation == 0 || total_bytes == 0 || created_at_unix_ms == 0 {
            return Err(JobError::InvalidArtifactGeneration);
        }
        Ok(Self {
            key: ArtifactGenerationKey::new(kind, generation),
            total_bytes,
            created_at_unix_ms,
            pinned,
        })
    }

    #[must_use]
    pub const fn key(&self) -> ArtifactGenerationKey {
        self.key
    }

    #[must_use]
    pub const fn total_bytes(&self) -> u64 {
        self.total_bytes
    }

    #[must_use]
    pub const fn created_at_unix_ms(&self) -> u64 {
        self.created_at_unix_ms
    }

    #[must_use]
    pub const fn pinned(&self) -> bool {
        self.pinned
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetentionPlan {
    protected: Vec<ArtifactGenerationKey>,
    deletable: Vec<ArtifactGenerationKey>,
    observed_bytes: u64,
    reclaim_bytes: u64,
}

impl RetentionPlan {
    #[must_use]
    pub fn protected(&self) -> &[ArtifactGenerationKey] {
        &self.protected
    }

    #[must_use]
    pub fn deletable(&self) -> &[ArtifactGenerationKey] {
        &self.deletable
    }

    #[must_use]
    pub const fn observed_bytes(&self) -> u64 {
        self.observed_bytes
    }

    #[must_use]
    pub const fn reclaim_bytes(&self) -> u64 {
        self.reclaim_bytes
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NextExecutionStage {
    Provider { completed_units: u64 },
    PublishResult,
    Complete,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArtifactManifest {
    kind: ArtifactKind,
    generation: u64,
    storage_shard_id: u32,
    chunk_count: u64,
    total_bytes: u64,
    content_digest: [u8; 32],
    projection_identity: [u8; 32],
    provider_version: String,
    algorithm_version: String,
    next_stage: NextExecutionStage,
    input_applied_index_count: u32,
    input_applied_indexes_digest: [u8; 32],
}

impl ArtifactManifest {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        kind: ArtifactKind,
        generation: u64,
        storage_shard_id: u32,
        chunk_count: u64,
        total_bytes: u64,
        content_digest: [u8; 32],
        projection_identity: [u8; 32],
        provider_version: impl Into<String>,
        algorithm_version: impl Into<String>,
        next_stage: NextExecutionStage,
        input_applied_indexes: BTreeMap<u32, u64>,
    ) -> Result<Self, JobError> {
        let provider_version = provider_version.into();
        let algorithm_version = algorithm_version.into();
        if input_applied_indexes.is_empty()
            || input_applied_indexes.len() > MAX_INPUT_SHARDS
            || input_applied_indexes
                .iter()
                .any(|(shard, index)| *shard == 0 || *index == 0)
        {
            return Err(JobError::InvalidArtifactManifest);
        }
        let input_applied_index_count = u32::try_from(input_applied_indexes.len())
            .map_err(|_| JobError::InvalidArtifactManifest)?;
        let input_applied_indexes_digest = applied_indexes_digest(&input_applied_indexes);
        Self::new_with_input_fence(
            kind,
            generation,
            storage_shard_id,
            chunk_count,
            total_bytes,
            content_digest,
            projection_identity,
            provider_version,
            algorithm_version,
            next_stage,
            input_applied_index_count,
            input_applied_indexes_digest,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_with_input_fence(
        kind: ArtifactKind,
        generation: u64,
        storage_shard_id: u32,
        chunk_count: u64,
        total_bytes: u64,
        content_digest: [u8; 32],
        projection_identity: [u8; 32],
        provider_version: impl Into<String>,
        algorithm_version: impl Into<String>,
        next_stage: NextExecutionStage,
        input_applied_index_count: u32,
        input_applied_indexes_digest: [u8; 32],
    ) -> Result<Self, JobError> {
        let provider_version = provider_version.into();
        let algorithm_version = algorithm_version.into();
        validate_artifact_manifest(
            kind,
            generation,
            storage_shard_id,
            chunk_count,
            total_bytes,
            content_digest,
            projection_identity,
            &provider_version,
            &algorithm_version,
            next_stage,
            input_applied_index_count,
            input_applied_indexes_digest,
        )?;
        Ok(Self {
            kind,
            generation,
            storage_shard_id,
            chunk_count,
            total_bytes,
            content_digest,
            projection_identity,
            provider_version,
            algorithm_version,
            next_stage,
            input_applied_index_count,
            input_applied_indexes_digest,
        })
    }

    #[must_use]
    pub const fn kind(&self) -> ArtifactKind {
        self.kind
    }

    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    #[must_use]
    pub const fn storage_shard_id(&self) -> u32 {
        self.storage_shard_id
    }

    #[must_use]
    pub const fn chunk_count(&self) -> u64 {
        self.chunk_count
    }

    #[must_use]
    pub const fn total_bytes(&self) -> u64 {
        self.total_bytes
    }

    #[must_use]
    pub const fn content_digest(&self) -> [u8; 32] {
        self.content_digest
    }

    #[must_use]
    pub const fn projection_identity(&self) -> [u8; 32] {
        self.projection_identity
    }

    #[must_use]
    pub fn provider_version(&self) -> &str {
        &self.provider_version
    }

    #[must_use]
    pub fn algorithm_version(&self) -> &str {
        &self.algorithm_version
    }

    #[must_use]
    pub const fn next_stage(&self) -> NextExecutionStage {
        self.next_stage
    }

    #[must_use]
    pub const fn input_applied_index_count(&self) -> u32 {
        self.input_applied_index_count
    }

    #[must_use]
    pub const fn input_applied_indexes_digest(&self) -> [u8; 32] {
        self.input_applied_indexes_digest
    }
}

#[allow(clippy::too_many_arguments)]
fn validate_artifact_manifest(
    kind: ArtifactKind,
    generation: u64,
    storage_shard_id: u32,
    chunk_count: u64,
    total_bytes: u64,
    content_digest: [u8; 32],
    projection_identity: [u8; 32],
    provider_version: &str,
    algorithm_version: &str,
    next_stage: NextExecutionStage,
    input_applied_index_count: u32,
    input_applied_indexes_digest: [u8; 32],
) -> Result<(), JobError> {
    let maximum_total_bytes = chunk_count
        .checked_mul(MAX_ARTIFACT_CHUNK_BYTES)
        .ok_or(JobError::InvalidArtifactManifest)?;
    validate_name(provider_version)?;
    validate_name(algorithm_version)?;
    if generation == 0
        || storage_shard_id == 0
        || chunk_count == 0
        || chunk_count > MAX_ARTIFACT_CHUNKS
        || total_bytes < chunk_count
        || total_bytes > maximum_total_bytes
        || content_digest == [0; 32]
        || projection_identity == [0; 32]
        || input_applied_index_count == 0
        || usize::try_from(input_applied_index_count).map_or(true, |count| count > MAX_INPUT_SHARDS)
        || input_applied_indexes_digest == [0; 32]
        || matches!(
            (kind, next_stage),
            (ArtifactKind::Checkpoint, NextExecutionStage::Complete)
                | (ArtifactKind::Result, NextExecutionStage::Provider { .. })
                | (ArtifactKind::Result, NextExecutionStage::PublishResult)
        )
    {
        return Err(JobError::InvalidArtifactManifest);
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JobState {
    Queued,
    Leased,
    Running,
    Succeeded,
    Failed,
    Canceled,
}

impl JobState {
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Canceled)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JobLease {
    owner_gateway_id: u64,
    lease_epoch: u64,
    expires_unix_ms: u64,
}

impl JobLease {
    #[must_use]
    pub const fn owner_gateway_id(self) -> u64 {
        self.owner_gateway_id
    }

    #[must_use]
    pub const fn lease_epoch(self) -> u64 {
        self.lease_epoch
    }

    #[must_use]
    pub const fn expires_unix_ms(self) -> u64 {
        self.expires_unix_ms
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JobRecord {
    spec: JobSpec,
    submitted_at_unix_ms: u64,
    job_revision: u64,
    state: JobState,
    lease: Option<JobLease>,
    last_lease_epoch: u64,
    checkpoint: Option<ArtifactManifest>,
    result: Option<ArtifactManifest>,
    failure: Option<(String, String)>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JobTombstone {
    final_job_revision: u64,
    terminal_state: JobState,
    pruned_at_unix_ms: u64,
    checkpoint: Option<(u32, u64)>,
    result: Option<(u32, u64)>,
    reclaimed_gc_epoch: Option<u64>,
    reclaimed_at_unix_ms: Option<u64>,
}

impl JobTombstone {
    #[must_use]
    pub const fn final_job_revision(&self) -> u64 {
        self.final_job_revision
    }

    #[must_use]
    pub const fn terminal_state(&self) -> JobState {
        self.terminal_state
    }

    #[must_use]
    pub const fn pruned_at_unix_ms(&self) -> u64 {
        self.pruned_at_unix_ms
    }

    #[must_use]
    pub const fn checkpoint(&self) -> Option<(u32, u64)> {
        self.checkpoint
    }

    #[must_use]
    pub const fn result(&self) -> Option<(u32, u64)> {
        self.result
    }

    #[must_use]
    pub const fn artifacts_reclaimed(&self) -> bool {
        self.reclaimed_gc_epoch.is_some()
    }

    #[must_use]
    pub const fn reclaimed_gc_epoch(&self) -> Option<u64> {
        self.reclaimed_gc_epoch
    }

    #[must_use]
    pub const fn reclaimed_at_unix_ms(&self) -> Option<u64> {
        self.reclaimed_at_unix_ms
    }
}

impl JobRecord {
    #[must_use]
    pub const fn spec(&self) -> &JobSpec {
        &self.spec
    }

    #[must_use]
    pub const fn job_revision(&self) -> u64 {
        self.job_revision
    }

    #[must_use]
    pub const fn state(&self) -> JobState {
        self.state
    }

    #[must_use]
    pub const fn lease(&self) -> Option<JobLease> {
        self.lease
    }

    #[must_use]
    pub const fn checkpoint(&self) -> Option<&ArtifactManifest> {
        self.checkpoint.as_ref()
    }

    #[must_use]
    pub const fn result(&self) -> Option<&ArtifactManifest> {
        self.result.as_ref()
    }

    #[must_use]
    pub const fn submitted_at_unix_ms(&self) -> u64 {
        self.submitted_at_unix_ms
    }

    #[must_use]
    pub fn failure(&self) -> Option<(&str, &str)> {
        self.failure
            .as_ref()
            .map(|(code, message)| (code.as_str(), message.as_str()))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JobCandidate {
    job_id: AnalyticsJobId,
    job_revision: u64,
    lease_epoch: Option<u64>,
}

impl JobCandidate {
    #[must_use]
    pub const fn new(job_id: AnalyticsJobId, job_revision: u64, lease_epoch: Option<u64>) -> Self {
        Self {
            job_id,
            job_revision,
            lease_epoch,
        }
    }

    #[must_use]
    pub const fn job_id(self) -> AnalyticsJobId {
        self.job_id
    }

    #[must_use]
    pub const fn job_revision(self) -> u64 {
        self.job_revision
    }

    #[must_use]
    pub const fn lease_epoch(self) -> Option<u64> {
        self.lease_epoch
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JobCommand {
    command_id: u128,
    body: JobCommandBody,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum JobCommandBody {
    Submit {
        spec: JobSpec,
        submitted_at_unix_ms: u64,
    },
    Claim {
        job_id: AnalyticsJobId,
        expected_job_revision: u64,
        gateway_id: u64,
        topology_epoch: u64,
        expires_unix_ms: u64,
    },
    Renew {
        job_id: AnalyticsJobId,
        expected_job_revision: u64,
        gateway_id: u64,
        lease_epoch: u64,
        topology_epoch: u64,
        expires_unix_ms: u64,
    },
    BeginRun {
        job_id: AnalyticsJobId,
        expected_job_revision: u64,
        gateway_id: u64,
        lease_epoch: u64,
        topology_epoch: u64,
    },
    CommitCheckpoint {
        job_id: AnalyticsJobId,
        expected_job_revision: u64,
        gateway_id: u64,
        lease_epoch: u64,
        topology_epoch: u64,
        manifest: ArtifactManifest,
    },
    PublishResult {
        job_id: AnalyticsJobId,
        expected_job_revision: u64,
        gateway_id: u64,
        lease_epoch: u64,
        topology_epoch: u64,
        manifest: ArtifactManifest,
    },
    Fail {
        job_id: AnalyticsJobId,
        expected_job_revision: u64,
        gateway_id: u64,
        lease_epoch: u64,
        topology_epoch: u64,
        code: String,
        message: String,
    },
    Cancel {
        job_id: AnalyticsJobId,
        expected_job_revision: u64,
    },
    ExpireLease {
        job_id: AnalyticsJobId,
        expected_job_revision: u64,
        lease_epoch: u64,
        now_unix_ms: u64,
    },
    PruneTerminal {
        job_id: AnalyticsJobId,
        expected_job_revision: u64,
        pruned_at_unix_ms: u64,
    },
    AcknowledgeArtifactsReclaimed {
        job_id: AnalyticsJobId,
        expected_tombstone_revision: u64,
        gateway_id: u64,
        gc_epoch: u64,
        reclaimed_at_unix_ms: u64,
    },
    CompactTombstones {
        through_unix_ms: u64,
    },
}

impl JobCommand {
    fn new(command_id: u128, body: JobCommandBody) -> Result<Self, JobError> {
        if command_id == 0 {
            return Err(JobError::InvalidCommandId);
        }
        Ok(Self { command_id, body })
    }

    pub fn submit(
        command_id: u128,
        spec: JobSpec,
        submitted_at_unix_ms: u64,
    ) -> Result<Self, JobError> {
        if submitted_at_unix_ms == 0 {
            return Err(JobError::InvalidTimestamp);
        }
        Self::new(
            command_id,
            JobCommandBody::Submit {
                spec,
                submitted_at_unix_ms,
            },
        )
    }

    pub fn claim(
        command_id: u128,
        job_id: AnalyticsJobId,
        expected_job_revision: u64,
        gateway_id: u64,
        topology_epoch: u64,
        expires_unix_ms: u64,
    ) -> Result<Self, JobError> {
        validate_fenced_command(expected_job_revision, gateway_id, topology_epoch)?;
        if expires_unix_ms == 0 {
            return Err(JobError::InvalidTimestamp);
        }
        Self::new(
            command_id,
            JobCommandBody::Claim {
                job_id,
                expected_job_revision,
                gateway_id,
                topology_epoch,
                expires_unix_ms,
            },
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn renew(
        command_id: u128,
        job_id: AnalyticsJobId,
        expected_job_revision: u64,
        gateway_id: u64,
        lease_epoch: u64,
        topology_epoch: u64,
        expires_unix_ms: u64,
    ) -> Result<Self, JobError> {
        validate_lease_command(
            expected_job_revision,
            gateway_id,
            lease_epoch,
            topology_epoch,
        )?;
        if expires_unix_ms == 0 {
            return Err(JobError::InvalidTimestamp);
        }
        Self::new(
            command_id,
            JobCommandBody::Renew {
                job_id,
                expected_job_revision,
                gateway_id,
                lease_epoch,
                topology_epoch,
                expires_unix_ms,
            },
        )
    }

    pub fn begin_run(
        command_id: u128,
        job_id: AnalyticsJobId,
        expected_job_revision: u64,
        gateway_id: u64,
        lease_epoch: u64,
        topology_epoch: u64,
    ) -> Result<Self, JobError> {
        validate_lease_command(
            expected_job_revision,
            gateway_id,
            lease_epoch,
            topology_epoch,
        )?;
        Self::new(
            command_id,
            JobCommandBody::BeginRun {
                job_id,
                expected_job_revision,
                gateway_id,
                lease_epoch,
                topology_epoch,
            },
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn commit_checkpoint(
        command_id: u128,
        job_id: AnalyticsJobId,
        expected_job_revision: u64,
        gateway_id: u64,
        lease_epoch: u64,
        topology_epoch: u64,
        manifest: ArtifactManifest,
    ) -> Result<Self, JobError> {
        validate_lease_command(
            expected_job_revision,
            gateway_id,
            lease_epoch,
            topology_epoch,
        )?;
        if manifest.kind != ArtifactKind::Checkpoint {
            return Err(JobError::InvalidArtifactManifest);
        }
        Self::new(
            command_id,
            JobCommandBody::CommitCheckpoint {
                job_id,
                expected_job_revision,
                gateway_id,
                lease_epoch,
                topology_epoch,
                manifest,
            },
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn publish_result(
        command_id: u128,
        job_id: AnalyticsJobId,
        expected_job_revision: u64,
        gateway_id: u64,
        lease_epoch: u64,
        topology_epoch: u64,
        manifest: ArtifactManifest,
    ) -> Result<Self, JobError> {
        validate_lease_command(
            expected_job_revision,
            gateway_id,
            lease_epoch,
            topology_epoch,
        )?;
        if manifest.kind != ArtifactKind::Result {
            return Err(JobError::InvalidArtifactManifest);
        }
        Self::new(
            command_id,
            JobCommandBody::PublishResult {
                job_id,
                expected_job_revision,
                gateway_id,
                lease_epoch,
                topology_epoch,
                manifest,
            },
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn fail(
        command_id: u128,
        job_id: AnalyticsJobId,
        expected_job_revision: u64,
        gateway_id: u64,
        lease_epoch: u64,
        topology_epoch: u64,
        code: impl Into<String>,
        message: impl Into<String>,
    ) -> Result<Self, JobError> {
        validate_lease_command(
            expected_job_revision,
            gateway_id,
            lease_epoch,
            topology_epoch,
        )?;
        let code = code.into();
        let message = message.into();
        validate_error_text(&code)?;
        validate_error_text(&message)?;
        Self::new(
            command_id,
            JobCommandBody::Fail {
                job_id,
                expected_job_revision,
                gateway_id,
                lease_epoch,
                topology_epoch,
                code,
                message,
            },
        )
    }

    pub fn cancel(
        command_id: u128,
        job_id: AnalyticsJobId,
        expected_job_revision: u64,
    ) -> Result<Self, JobError> {
        if expected_job_revision == 0 {
            return Err(JobError::InvalidJobRevision);
        }
        Self::new(
            command_id,
            JobCommandBody::Cancel {
                job_id,
                expected_job_revision,
            },
        )
    }

    pub fn expire_lease(
        command_id: u128,
        job_id: AnalyticsJobId,
        expected_job_revision: u64,
        lease_epoch: u64,
        now_unix_ms: u64,
    ) -> Result<Self, JobError> {
        if expected_job_revision == 0 || lease_epoch == 0 {
            return Err(JobError::InvalidJobRevision);
        }
        if now_unix_ms == 0 {
            return Err(JobError::InvalidTimestamp);
        }
        Self::new(
            command_id,
            JobCommandBody::ExpireLease {
                job_id,
                expected_job_revision,
                lease_epoch,
                now_unix_ms,
            },
        )
    }

    pub fn prune_terminal(
        command_id: u128,
        job_id: AnalyticsJobId,
        expected_job_revision: u64,
        pruned_at_unix_ms: u64,
    ) -> Result<Self, JobError> {
        if expected_job_revision == 0 {
            return Err(JobError::InvalidJobRevision);
        }
        if pruned_at_unix_ms == 0 {
            return Err(JobError::InvalidTimestamp);
        }
        Self::new(
            command_id,
            JobCommandBody::PruneTerminal {
                job_id,
                expected_job_revision,
                pruned_at_unix_ms,
            },
        )
    }

    pub fn acknowledge_artifacts_reclaimed(
        command_id: u128,
        job_id: AnalyticsJobId,
        expected_tombstone_revision: u64,
        gateway_id: u64,
        gc_epoch: u64,
        reclaimed_at_unix_ms: u64,
    ) -> Result<Self, JobError> {
        if expected_tombstone_revision == 0 || gateway_id == 0 || gc_epoch == 0 {
            return Err(JobError::InvalidJobRevision);
        }
        if reclaimed_at_unix_ms == 0 {
            return Err(JobError::InvalidTimestamp);
        }
        Self::new(
            command_id,
            JobCommandBody::AcknowledgeArtifactsReclaimed {
                job_id,
                expected_tombstone_revision,
                gateway_id,
                gc_epoch,
                reclaimed_at_unix_ms,
            },
        )
    }

    pub fn compact_tombstones(command_id: u128, through_unix_ms: u64) -> Result<Self, JobError> {
        if through_unix_ms == 0 {
            return Err(JobError::InvalidTimestamp);
        }
        Self::new(
            command_id,
            JobCommandBody::CompactTombstones { through_unix_ms },
        )
    }

    #[must_use]
    pub const fn command_id(&self) -> u128 {
        self.command_id
    }

    #[must_use]
    pub const fn submitted_spec(&self) -> Option<&JobSpec> {
        match &self.body {
            JobCommandBody::Submit { spec, .. } => Some(spec),
            JobCommandBody::Claim { .. }
            | JobCommandBody::Renew { .. }
            | JobCommandBody::BeginRun { .. }
            | JobCommandBody::CommitCheckpoint { .. }
            | JobCommandBody::PublishResult { .. }
            | JobCommandBody::Fail { .. }
            | JobCommandBody::Cancel { .. }
            | JobCommandBody::ExpireLease { .. }
            | JobCommandBody::PruneTerminal { .. }
            | JobCommandBody::AcknowledgeArtifactsReclaimed { .. }
            | JobCommandBody::CompactTombstones { .. } => None,
        }
    }

    #[must_use]
    pub const fn target_job_id(&self) -> Option<AnalyticsJobId> {
        match &self.body {
            JobCommandBody::Submit { spec, .. } => Some(spec.job_id),
            JobCommandBody::Claim { job_id, .. }
            | JobCommandBody::Renew { job_id, .. }
            | JobCommandBody::BeginRun { job_id, .. }
            | JobCommandBody::CommitCheckpoint { job_id, .. }
            | JobCommandBody::PublishResult { job_id, .. }
            | JobCommandBody::Fail { job_id, .. }
            | JobCommandBody::Cancel { job_id, .. }
            | JobCommandBody::ExpireLease { job_id, .. }
            | JobCommandBody::PruneTerminal { job_id, .. }
            | JobCommandBody::AcknowledgeArtifactsReclaimed { job_id, .. } => Some(*job_id),
            JobCommandBody::CompactTombstones { .. } => None,
        }
    }

    #[must_use]
    pub const fn reclamation_gc_fence(&self) -> Option<(u64, u64)> {
        match &self.body {
            JobCommandBody::AcknowledgeArtifactsReclaimed {
                gateway_id,
                gc_epoch,
                ..
            } => Some((*gateway_id, *gc_epoch)),
            _ => None,
        }
    }

    #[must_use]
    pub fn has_magic(bytes: &[u8]) -> bool {
        bytes.starts_with(&COMMAND_MAGIC)
    }

    pub fn encode(&self) -> Result<Vec<u8>, JobError> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&COMMAND_MAGIC);
        bytes.extend_from_slice(&FORMAT_VERSION.to_be_bytes());
        bytes.extend_from_slice(&self.command_id.to_be_bytes());
        encode_command_body(&mut bytes, &self.body)?;
        append_checksum(&mut bytes);
        if bytes.len() > MAX_COMMAND_BYTES {
            return Err(JobError::RecordTooLarge);
        }
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, JobError> {
        if bytes.len() > MAX_COMMAND_BYTES {
            return Err(JobError::RecordTooLarge);
        }
        verify_checksum(bytes)?;
        let mut reader = Reader::new(bytes)?;
        reader.expect_magic(COMMAND_MAGIC)?;
        reader.expect_version()?;
        let command_id = reader.u128()?;
        let body = decode_command_body(&mut reader)?;
        reader.finish()?;
        let command = Self::new(command_id, body)?;
        if command.encode()? != bytes {
            return Err(JobError::NonCanonicalRecord);
        }
        Ok(command)
    }

    fn digest(&self) -> Result<[u8; 32], JobError> {
        Ok(*blake3::hash(&self.encode()?).as_bytes())
    }
}

fn validate_fenced_command(
    expected_job_revision: u64,
    gateway_id: u64,
    topology_epoch: u64,
) -> Result<(), JobError> {
    if expected_job_revision == 0 {
        return Err(JobError::InvalidJobRevision);
    }
    if gateway_id == 0 || topology_epoch == 0 {
        return Err(JobError::InvalidLease);
    }
    Ok(())
}

fn validate_lease_command(
    expected_job_revision: u64,
    gateway_id: u64,
    lease_epoch: u64,
    topology_epoch: u64,
) -> Result<(), JobError> {
    validate_fenced_command(expected_job_revision, gateway_id, topology_epoch)?;
    if lease_epoch == 0 {
        return Err(JobError::InvalidLease);
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LedgerApplyReceipt {
    ledger_revision: u64,
    job_revision: u64,
    duplicate: bool,
    request_duplicate: bool,
}

impl LedgerApplyReceipt {
    #[must_use]
    pub const fn ledger_revision(self) -> u64 {
        self.ledger_revision
    }

    #[must_use]
    pub const fn job_revision(self) -> u64 {
        self.job_revision
    }

    #[must_use]
    pub const fn duplicate(self) -> bool {
        self.duplicate
    }

    #[must_use]
    pub const fn request_duplicate(self) -> bool {
        self.request_duplicate
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct AppliedCommand {
    digest: [u8; 32],
    receipt: LedgerApplyReceipt,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SubmissionRecord {
    job_id: AnalyticsJobId,
    digest: [u8; 32],
    submitted_at_unix_ms: u64,
    job_revision: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct LedgerState {
    revision: u64,
    compacted_through_revision: u64,
    submission_floor_unix_ms: u64,
    max_job_id: u128,
    jobs: BTreeMap<AnalyticsJobId, JobRecord>,
    tombstones: BTreeMap<AnalyticsJobId, JobTombstone>,
    submission_requests: BTreeMap<u128, SubmissionRecord>,
    applied_commands: BTreeMap<u128, AppliedCommand>,
}

impl LedgerState {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            revision: 0,
            compacted_through_revision: 0,
            submission_floor_unix_ms: 0,
            max_job_id: 0,
            jobs: BTreeMap::new(),
            tombstones: BTreeMap::new(),
            submission_requests: BTreeMap::new(),
            applied_commands: BTreeMap::new(),
        }
    }

    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    #[must_use]
    pub fn job(&self, id: AnalyticsJobId) -> Option<&JobRecord> {
        self.jobs.get(&id)
    }

    #[must_use]
    pub fn tombstone(&self, id: AnalyticsJobId) -> Option<&JobTombstone> {
        self.tombstones.get(&id)
    }

    #[must_use]
    pub fn job_for_submission(&self, submission_request_id: u128) -> Option<AnalyticsJobId> {
        self.submission_requests
            .get(&submission_request_id)
            .map(|submission| submission.job_id)
    }

    #[must_use]
    pub const fn jobs(&self) -> &BTreeMap<AnalyticsJobId, JobRecord> {
        &self.jobs
    }

    pub fn encode_job(&self, id: AnalyticsJobId) -> Result<Vec<u8>, JobError> {
        let record = self.jobs.get(&id).ok_or(JobError::UnknownJob(id))?;
        validate_job_record(id, record, self.revision)?;
        let mut bytes = Vec::with_capacity(4 + 2 + 8 + job_record_bytes(record)? + CHECKSUM_BYTES);
        bytes.extend_from_slice(&JOB_RECORD_MAGIC);
        bytes.extend_from_slice(&FORMAT_VERSION.to_be_bytes());
        bytes.extend_from_slice(&self.revision.to_be_bytes());
        encode_job_record(&mut bytes, record)?;
        append_checksum(&mut bytes);
        if bytes.len() > MAX_COMMAND_BYTES {
            return Err(JobError::RecordTooLarge);
        }
        Ok(bytes)
    }

    pub fn decode_job(bytes: &[u8]) -> Result<(u64, JobRecord), JobError> {
        if bytes.len() > MAX_COMMAND_BYTES {
            return Err(JobError::RecordTooLarge);
        }
        verify_checksum(bytes)?;
        let mut reader = Reader::new(bytes)?;
        reader.expect_magic(JOB_RECORD_MAGIC)?;
        reader.expect_version()?;
        let ledger_revision = reader.u64()?;
        let record = decode_job_record(&mut reader)?;
        reader.finish()?;
        validate_job_record(record.spec.job_id, &record, ledger_revision)?;
        let mut canonical = Vec::with_capacity(bytes.len());
        canonical.extend_from_slice(&JOB_RECORD_MAGIC);
        canonical.extend_from_slice(&FORMAT_VERSION.to_be_bytes());
        canonical.extend_from_slice(&ledger_revision.to_be_bytes());
        encode_job_record(&mut canonical, &record)?;
        append_checksum(&mut canonical);
        if canonical != bytes {
            return Err(JobError::NonCanonicalRecord);
        }
        Ok((ledger_revision, record))
    }

    pub fn encode_tombstone(&self, id: AnalyticsJobId) -> Result<Vec<u8>, JobError> {
        let tombstone = self.tombstones.get(&id).ok_or(JobError::UnknownJob(id))?;
        let submission = self
            .submission_requests
            .values()
            .find(|submission| submission.job_id == id)
            .ok_or(JobError::NonCanonicalRecord)?;
        validate_tombstone(tombstone, submission, self.revision)?;
        let mut bytes = Vec::with_capacity(4 + 2 + 8 + tombstone_bytes(tombstone) + CHECKSUM_BYTES);
        bytes.extend_from_slice(&TOMBSTONE_RECORD_MAGIC);
        bytes.extend_from_slice(&FORMAT_VERSION.to_be_bytes());
        bytes.extend_from_slice(&self.revision.to_be_bytes());
        encode_tombstone_entry(&mut bytes, id, tombstone);
        append_checksum(&mut bytes);
        if bytes.len() > MAX_COMMAND_BYTES {
            return Err(JobError::RecordTooLarge);
        }
        Ok(bytes)
    }

    pub fn decode_tombstone(bytes: &[u8]) -> Result<(u64, AnalyticsJobId, JobTombstone), JobError> {
        if bytes.len() > MAX_COMMAND_BYTES {
            return Err(JobError::RecordTooLarge);
        }
        verify_checksum(bytes)?;
        let mut reader = Reader::new(bytes)?;
        reader.expect_magic(TOMBSTONE_RECORD_MAGIC)?;
        reader.expect_version()?;
        let ledger_revision = reader.u64()?;
        let (job_id, tombstone) = decode_tombstone_entry(&mut reader)?;
        reader.finish()?;
        validate_tombstone_record(&tombstone, ledger_revision)?;
        let mut canonical = Vec::with_capacity(bytes.len());
        canonical.extend_from_slice(&TOMBSTONE_RECORD_MAGIC);
        canonical.extend_from_slice(&FORMAT_VERSION.to_be_bytes());
        canonical.extend_from_slice(&ledger_revision.to_be_bytes());
        encode_tombstone_entry(&mut canonical, job_id, &tombstone);
        append_checksum(&mut canonical);
        if canonical != bytes {
            return Err(JobError::NonCanonicalRecord);
        }
        Ok((ledger_revision, job_id, tombstone))
    }

    pub fn claimable_jobs(
        &self,
        now_unix_ms: u64,
        after: Option<AnalyticsJobId>,
        limit: usize,
    ) -> Result<Vec<JobCandidate>, JobError> {
        if now_unix_ms == 0 || limit == 0 || limit > 1_024 {
            return Err(JobError::InvalidClaimableRequest);
        }
        Ok(self
            .jobs
            .range((
                std::ops::Bound::Excluded(after.unwrap_or(AnalyticsJobId(0))),
                std::ops::Bound::Unbounded,
            ))
            .filter_map(|(job_id, record)| match (record.state, record.lease) {
                (JobState::Queued, None) => Some(JobCandidate {
                    job_id: *job_id,
                    job_revision: record.job_revision,
                    lease_epoch: None,
                }),
                (JobState::Leased | JobState::Running, Some(lease))
                    if lease.expires_unix_ms <= now_unix_ms =>
                {
                    Some(JobCandidate {
                        job_id: *job_id,
                        job_revision: record.job_revision,
                        lease_epoch: Some(lease.lease_epoch),
                    })
                }
                _ => None,
            })
            .take(limit)
            .collect())
    }

    /// Lists every retained job in canonical ID order for bounded maintenance scans.
    pub fn list_jobs(
        &self,
        after: Option<AnalyticsJobId>,
        limit: usize,
    ) -> Result<Vec<AnalyticsJobId>, JobError> {
        if limit == 0 || limit > 1_024 {
            return Err(JobError::InvalidJobListRequest);
        }
        Ok(self
            .jobs
            .range((
                std::ops::Bound::Excluded(after.unwrap_or(AnalyticsJobId(0))),
                std::ops::Bound::Unbounded,
            ))
            .map(|(job_id, _)| *job_id)
            .take(limit)
            .collect())
    }

    pub fn list_tombstones(
        &self,
        after: Option<AnalyticsJobId>,
        limit: usize,
    ) -> Result<Vec<AnalyticsJobId>, JobError> {
        if limit == 0 || limit > 1_024 {
            return Err(JobError::InvalidJobListRequest);
        }
        Ok(self
            .tombstones
            .range((
                std::ops::Bound::Excluded(after.unwrap_or(AnalyticsJobId(0))),
                std::ops::Bound::Unbounded,
            ))
            .map(|(job_id, _)| *job_id)
            .take(limit)
            .collect())
    }

    pub fn apply(&mut self, command: JobCommand) -> Result<LedgerApplyReceipt, JobError> {
        let digest = command.digest()?;
        let is_submit = matches!(&command.body, JobCommandBody::Submit { .. });
        let target_job_id = command.target_job_id();
        let submission_request_id = command.submitted_spec().map(JobSpec::submission_request_id);
        if let Some(applied) = self.applied_commands.get(&command.command_id) {
            if applied.digest != digest {
                return Err(JobError::CommandReplayMismatch {
                    command_id: command.command_id,
                });
            }
            return Ok(LedgerApplyReceipt {
                duplicate: true,
                ..applied.receipt
            });
        }
        let mut next = self.clone();
        let jobs_before = next.jobs.len();
        let job_revision = next.apply_body(command.body)?;
        let canonical_job_id = submission_request_id
            .and_then(|request_id| next.submission_requests.get(&request_id))
            .map(|submission| submission.job_id)
            .or(target_job_id);
        if let Some(job_id) = canonical_job_id {
            next.refresh_submission_job_revision(job_id, job_revision)?;
        }
        let request_duplicate = is_submit && next.jobs.len() == jobs_before;
        next.revision = next
            .revision
            .checked_add(1)
            .ok_or(JobError::RevisionExhausted)?;
        let receipt = LedgerApplyReceipt {
            ledger_revision: next.revision,
            job_revision,
            duplicate: false,
            request_duplicate,
        };
        next.applied_commands
            .insert(command.command_id, AppliedCommand { digest, receipt });
        next.compact_replay_history();
        next.ensure_admission_capacity()?;
        *self = next;
        Ok(receipt)
    }

    fn apply_body(&mut self, body: JobCommandBody) -> Result<u64, JobError> {
        match body {
            JobCommandBody::Submit {
                spec,
                submitted_at_unix_ms,
            } => {
                let submission_request_id = spec.submission_request_id;
                let submission_digest = spec_submission_digest(&spec)?;
                if let Some(existing) = self.submission_requests.get(&spec.submission_request_id) {
                    if existing.digest != submission_digest {
                        return Err(JobError::SubmissionReplayMismatch {
                            submission_request_id: spec.submission_request_id,
                        });
                    }
                    return Ok(existing.job_revision);
                }
                if submitted_at_unix_ms <= self.submission_floor_unix_ms {
                    return Err(JobError::ExpiredSubmission {
                        floor_unix_ms: self.submission_floor_unix_ms,
                        actual_unix_ms: submitted_at_unix_ms,
                    });
                }
                if self.jobs.len() >= MAX_JOBS {
                    return Err(JobError::JobCapacity);
                }
                let id = spec.job_id;
                if id.value() <= self.max_job_id || self.jobs.contains_key(&id) {
                    return Err(JobError::JobAlreadyExists(id));
                }
                self.max_job_id = id.value();
                self.jobs.insert(
                    id,
                    JobRecord {
                        spec,
                        submitted_at_unix_ms,
                        job_revision: 1,
                        state: JobState::Queued,
                        lease: None,
                        last_lease_epoch: 0,
                        checkpoint: None,
                        result: None,
                        failure: None,
                    },
                );
                self.submission_requests.insert(
                    submission_request_id,
                    SubmissionRecord {
                        job_id: id,
                        digest: submission_digest,
                        submitted_at_unix_ms,
                        job_revision: 1,
                    },
                );
                Ok(1)
            }
            JobCommandBody::Claim {
                job_id,
                expected_job_revision,
                gateway_id,
                topology_epoch,
                expires_unix_ms,
            } => {
                let record = self.job_mut(job_id, expected_job_revision)?;
                if record.state.is_terminal() {
                    return Err(JobError::TerminalJob);
                }
                if record.state != JobState::Queued || record.lease.is_some() {
                    return Err(JobError::JobNotClaimable);
                }
                ensure_topology(record, topology_epoch)?;
                let lease_epoch = record
                    .last_lease_epoch
                    .checked_add(1)
                    .ok_or(JobError::LeaseEpochExhausted)?;
                record.last_lease_epoch = lease_epoch;
                record.lease = Some(JobLease {
                    owner_gateway_id: gateway_id,
                    lease_epoch,
                    expires_unix_ms,
                });
                record.state = JobState::Leased;
                advance_job_revision(record)
            }
            JobCommandBody::Renew {
                job_id,
                expected_job_revision,
                gateway_id,
                lease_epoch,
                topology_epoch,
                expires_unix_ms,
            } => {
                let record = self.job_mut(job_id, expected_job_revision)?;
                ensure_active(record)?;
                ensure_fence(record, gateway_id, lease_epoch, topology_epoch)?;
                let lease = record.lease.as_mut().expect("active job has a lease");
                if expires_unix_ms <= lease.expires_unix_ms {
                    return Err(JobError::InvalidLeaseExtension);
                }
                lease.expires_unix_ms = expires_unix_ms;
                advance_job_revision(record)
            }
            JobCommandBody::BeginRun {
                job_id,
                expected_job_revision,
                gateway_id,
                lease_epoch,
                topology_epoch,
            } => {
                let record = self.job_mut(job_id, expected_job_revision)?;
                if record.state != JobState::Leased {
                    return Err(if record.state.is_terminal() {
                        JobError::TerminalJob
                    } else {
                        JobError::InvalidStateTransition
                    });
                }
                ensure_fence(record, gateway_id, lease_epoch, topology_epoch)?;
                record.state = JobState::Running;
                advance_job_revision(record)
            }
            JobCommandBody::CommitCheckpoint {
                job_id,
                expected_job_revision,
                gateway_id,
                lease_epoch,
                topology_epoch,
                manifest,
            } => {
                let record = self.job_mut(job_id, expected_job_revision)?;
                ensure_running_and_fenced(record, gateway_id, lease_epoch, topology_epoch)?;
                ensure_manifest_compatibility(record, &manifest)?;
                if record
                    .checkpoint
                    .as_ref()
                    .is_some_and(|existing| existing.generation >= manifest.generation)
                {
                    return Err(JobError::StaleArtifactGeneration);
                }
                record.checkpoint = Some(manifest);
                advance_job_revision(record)
            }
            JobCommandBody::PublishResult {
                job_id,
                expected_job_revision,
                gateway_id,
                lease_epoch,
                topology_epoch,
                manifest,
            } => {
                let record = self.job_mut(job_id, expected_job_revision)?;
                ensure_running_and_fenced(record, gateway_id, lease_epoch, topology_epoch)?;
                ensure_manifest_compatibility(record, &manifest)?;
                record.result = Some(manifest);
                record.state = JobState::Succeeded;
                record.lease = None;
                advance_job_revision(record)
            }
            JobCommandBody::Fail {
                job_id,
                expected_job_revision,
                gateway_id,
                lease_epoch,
                topology_epoch,
                code,
                message,
            } => {
                let record = self.job_mut(job_id, expected_job_revision)?;
                if record.state.is_terminal() {
                    return Err(JobError::TerminalJob);
                }
                ensure_fence(record, gateway_id, lease_epoch, topology_epoch)?;
                record.failure = Some((code, message));
                record.state = JobState::Failed;
                record.lease = None;
                advance_job_revision(record)
            }
            JobCommandBody::Cancel {
                job_id,
                expected_job_revision,
            } => {
                let record = self.job_mut(job_id, expected_job_revision)?;
                if record.state.is_terminal() {
                    return Err(JobError::TerminalJob);
                }
                record.state = JobState::Canceled;
                record.lease = None;
                advance_job_revision(record)
            }
            JobCommandBody::ExpireLease {
                job_id,
                expected_job_revision,
                lease_epoch,
                now_unix_ms,
            } => {
                let record = self.job_mut(job_id, expected_job_revision)?;
                ensure_active(record)?;
                let lease = record.lease.ok_or(JobError::StaleLease)?;
                if lease.lease_epoch != lease_epoch {
                    return Err(JobError::StaleLease);
                }
                if now_unix_ms < lease.expires_unix_ms {
                    return Err(JobError::LeaseActive);
                }
                record.state = JobState::Queued;
                record.lease = None;
                advance_job_revision(record)
            }
            JobCommandBody::PruneTerminal {
                job_id,
                expected_job_revision,
                pruned_at_unix_ms,
            } => {
                let record = self.jobs.get(&job_id).ok_or(JobError::UnknownJob(job_id))?;
                if record.job_revision != expected_job_revision {
                    return Err(JobError::StaleJobRevision {
                        expected: record.job_revision,
                        actual: expected_job_revision,
                    });
                }
                if !record.state.is_terminal() {
                    return Err(JobError::InvalidStateTransition);
                }
                if pruned_at_unix_ms < record.submitted_at_unix_ms {
                    return Err(JobError::InvalidTimestamp);
                }
                let submission_request_id = record.spec.submission_request_id;
                let tombstone = JobTombstone {
                    final_job_revision: expected_job_revision,
                    terminal_state: record.state,
                    pruned_at_unix_ms,
                    checkpoint: record
                        .checkpoint
                        .as_ref()
                        .map(|manifest| (manifest.storage_shard_id, manifest.generation)),
                    result: record
                        .result
                        .as_ref()
                        .map(|manifest| (manifest.storage_shard_id, manifest.generation)),
                    reclaimed_gc_epoch: None,
                    reclaimed_at_unix_ms: None,
                };
                self.jobs.remove(&job_id);
                if self.tombstones.insert(job_id, tombstone).is_some() {
                    return Err(JobError::NonCanonicalRecord);
                }
                let tombstone = self
                    .submission_requests
                    .get_mut(&submission_request_id)
                    .ok_or(JobError::NonCanonicalRecord)?;
                tombstone.job_revision = expected_job_revision;
                Ok(expected_job_revision)
            }
            JobCommandBody::AcknowledgeArtifactsReclaimed {
                job_id,
                expected_tombstone_revision,
                gateway_id: _,
                gc_epoch,
                reclaimed_at_unix_ms,
            } => {
                let tombstone = self
                    .tombstones
                    .get_mut(&job_id)
                    .ok_or(JobError::UnknownJob(job_id))?;
                if tombstone.final_job_revision != expected_tombstone_revision {
                    return Err(JobError::StaleTombstoneRevision {
                        expected: tombstone.final_job_revision,
                        actual: expected_tombstone_revision,
                    });
                }
                if reclaimed_at_unix_ms < tombstone.pruned_at_unix_ms {
                    return Err(JobError::InvalidTimestamp);
                }
                match (tombstone.reclaimed_gc_epoch, tombstone.reclaimed_at_unix_ms) {
                    (None, None) => {
                        tombstone.reclaimed_gc_epoch = Some(gc_epoch);
                        tombstone.reclaimed_at_unix_ms = Some(reclaimed_at_unix_ms);
                    }
                    (Some(existing_epoch), Some(_)) if gc_epoch >= existing_epoch => {}
                    _ => return Err(JobError::StaleGcEpoch),
                }
                Ok(expected_tombstone_revision)
            }
            JobCommandBody::CompactTombstones { through_unix_ms } => {
                if through_unix_ms <= self.submission_floor_unix_ms {
                    return Err(JobError::InvalidTombstoneCompaction);
                }
                if self
                    .jobs
                    .values()
                    .any(|record| record.submitted_at_unix_ms <= through_unix_ms)
                {
                    return Err(JobError::ActiveSubmissionBeforeCompactionFloor);
                }
                if self.tombstones.values().any(|tombstone| {
                    tombstone.pruned_at_unix_ms <= through_unix_ms
                        && (!tombstone.artifacts_reclaimed()
                            || tombstone
                                .reclaimed_at_unix_ms
                                .is_some_and(|reclaimed| reclaimed > through_unix_ms))
                }) {
                    return Err(JobError::UnreclaimedTombstoneBeforeCompactionFloor);
                }
                self.tombstones.retain(|_, tombstone| {
                    tombstone.pruned_at_unix_ms > through_unix_ms
                        || tombstone
                            .reclaimed_at_unix_ms
                            .is_some_and(|reclaimed| reclaimed > through_unix_ms)
                });
                self.submission_requests.retain(|_, submission| {
                    self.jobs.contains_key(&submission.job_id)
                        || self.tombstones.contains_key(&submission.job_id)
                        || submission.submitted_at_unix_ms > through_unix_ms
                });
                self.submission_floor_unix_ms = through_unix_ms;
                Ok(1)
            }
        }
    }

    fn job_mut(
        &mut self,
        id: AnalyticsJobId,
        expected_revision: u64,
    ) -> Result<&mut JobRecord, JobError> {
        let record = self.jobs.get_mut(&id).ok_or(JobError::UnknownJob(id))?;
        if record.job_revision != expected_revision {
            return Err(JobError::StaleJobRevision {
                expected: record.job_revision,
                actual: expected_revision,
            });
        }
        Ok(record)
    }

    pub fn encode_snapshot(&self) -> Result<Vec<u8>, JobError> {
        self.validate_state_invariants()?;
        let snapshot_bytes = self.snapshot_bytes()?;
        if snapshot_bytes > MAX_SNAPSHOT_BYTES {
            return Err(JobError::SnapshotCapacity);
        }
        let mut bytes = Vec::with_capacity(snapshot_bytes);
        bytes.extend_from_slice(&SNAPSHOT_MAGIC);
        bytes.extend_from_slice(&FORMAT_VERSION.to_be_bytes());
        bytes.extend_from_slice(&self.revision.to_be_bytes());
        bytes.extend_from_slice(&self.compacted_through_revision.to_be_bytes());
        bytes.extend_from_slice(&self.submission_floor_unix_ms.to_be_bytes());
        bytes.extend_from_slice(&self.max_job_id.to_be_bytes());
        write_count(&mut bytes, self.jobs.len())?;
        for record in self.jobs.values() {
            encode_job_record(&mut bytes, record)?;
        }
        write_count(&mut bytes, self.tombstones.len())?;
        for (job_id, tombstone) in &self.tombstones {
            encode_tombstone_entry(&mut bytes, *job_id, tombstone);
        }
        write_count(&mut bytes, self.submission_requests.len())?;
        for (request_id, submission) in &self.submission_requests {
            bytes.extend_from_slice(&request_id.to_be_bytes());
            bytes.extend_from_slice(&submission.job_id.value().to_be_bytes());
            bytes.extend_from_slice(&submission.digest);
            bytes.extend_from_slice(&submission.submitted_at_unix_ms.to_be_bytes());
            bytes.extend_from_slice(&submission.job_revision.to_be_bytes());
        }
        write_count(&mut bytes, self.applied_commands.len())?;
        for (command_id, applied) in &self.applied_commands {
            bytes.extend_from_slice(&command_id.to_be_bytes());
            bytes.extend_from_slice(&applied.digest);
            encode_receipt(&mut bytes, applied.receipt);
        }
        append_checksum(&mut bytes);
        debug_assert_eq!(bytes.len(), snapshot_bytes);
        Ok(bytes)
    }

    pub fn decode_snapshot(bytes: &[u8]) -> Result<Self, JobError> {
        if bytes.len() > MAX_SNAPSHOT_BYTES {
            return Err(JobError::RecordTooLarge);
        }
        verify_checksum(bytes)?;
        let mut reader = Reader::new(bytes)?;
        reader.expect_magic(SNAPSHOT_MAGIC)?;
        reader.expect_version()?;
        let revision = reader.u64()?;
        let compacted_through_revision = reader.u64()?;
        let submission_floor_unix_ms = reader.u64()?;
        let max_job_id = reader.u128()?;
        let mut jobs = BTreeMap::new();
        for _ in 0..reader.count(MAX_JOBS)? {
            let record = decode_job_record(&mut reader)?;
            if jobs.insert(record.spec.job_id, record).is_some() {
                return Err(JobError::NonCanonicalRecord);
            }
        }
        let mut tombstones = BTreeMap::new();
        for _ in 0..reader.count(MAX_TOMBSTONES)? {
            let (job_id, tombstone) = decode_tombstone_entry(&mut reader)?;
            if tombstones.insert(job_id, tombstone).is_some() {
                return Err(JobError::NonCanonicalRecord);
            }
        }
        let mut submission_requests = BTreeMap::new();
        for _ in 0..reader.count(MAX_SUBMISSION_REQUESTS)? {
            let request_id = reader.u128()?;
            let job_id = AnalyticsJobId::new(reader.u128()?)?;
            let mut digest = [0; 32];
            digest.copy_from_slice(reader.bytes(32)?);
            let submitted_at_unix_ms = nonzero_timestamp(reader.u64()?)?;
            let job_revision = nonzero_revision(reader.u64()?)?;
            if request_id == 0
                || submission_requests
                    .insert(
                        request_id,
                        SubmissionRecord {
                            job_id,
                            digest,
                            submitted_at_unix_ms,
                            job_revision,
                        },
                    )
                    .is_some()
            {
                return Err(JobError::NonCanonicalRecord);
            }
        }
        let mut applied_commands = BTreeMap::new();
        for _ in 0..reader.count(MAX_APPLIED_COMMANDS)? {
            let command_id = reader.u128()?;
            let mut digest = [0; 32];
            digest.copy_from_slice(reader.bytes(32)?);
            let receipt = decode_receipt(&mut reader)?;
            if command_id == 0
                || receipt.duplicate
                || receipt.ledger_revision == 0
                || receipt.ledger_revision > revision
                || receipt.job_revision == 0
                || applied_commands
                    .insert(command_id, AppliedCommand { digest, receipt })
                    .is_some()
            {
                return Err(JobError::NonCanonicalRecord);
            }
        }
        reader.finish()?;
        let state = Self {
            revision,
            compacted_through_revision,
            submission_floor_unix_ms,
            max_job_id,
            jobs,
            tombstones,
            submission_requests,
            applied_commands,
        };
        state.validate_state_invariants()?;
        if state.encode_snapshot()? != bytes {
            return Err(JobError::NonCanonicalRecord);
        }
        Ok(state)
    }

    fn ensure_admission_capacity(&self) -> Result<(), JobError> {
        let replay_reserve = self.replay_reserve_bytes()?;
        let required = self
            .snapshot_bytes()?
            .checked_add(self.lifecycle_reserve_bytes()?)
            .and_then(|value| value.checked_add(replay_reserve))
            .ok_or(JobError::SnapshotCapacity)?;
        if required > MAX_SNAPSHOT_BYTES {
            Err(JobError::SnapshotCapacity)
        } else {
            Ok(())
        }
    }

    fn validate_state_invariants(&self) -> Result<(), JobError> {
        self.validate_replay_history()?;
        let submissions_by_job = self
            .submission_requests
            .values()
            .map(|submission| (submission.job_id, submission))
            .collect::<BTreeMap<_, _>>();
        if self.jobs.len() > MAX_JOBS
            || self.tombstones.len() > MAX_TOMBSTONES
            || self.jobs.len().checked_add(self.tombstones.len())
                != Some(self.submission_requests.len())
            || self
                .jobs
                .keys()
                .any(|job_id| self.tombstones.contains_key(job_id))
            || self
                .submission_requests
                .values()
                .any(|submission| submission.job_id.value() > self.max_job_id)
            || self
                .submission_requests
                .values()
                .map(|submission| submission.job_id)
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                != self.submission_requests.len()
            || self
                .submission_requests
                .iter()
                .any(|(request_id, submission)| {
                    *request_id == 0
                        || submission.submitted_at_unix_ms == 0
                        || submission.submitted_at_unix_ms <= self.submission_floor_unix_ms
                        || submission.job_revision == 0
                        || submission.job_revision > self.revision
                })
        {
            return Err(JobError::NonCanonicalRecord);
        }
        for (job_id, record) in &self.jobs {
            validate_job_record(*job_id, record, self.revision)?;
            let submission = submissions_by_job
                .get(job_id)
                .copied()
                .ok_or(JobError::NonCanonicalRecord)?;
            if self
                .submission_requests
                .get(&record.spec.submission_request_id)
                != Some(submission)
                || submission.digest != spec_submission_digest(&record.spec)?
                || submission.submitted_at_unix_ms != record.submitted_at_unix_ms
                || submission.job_revision != record.job_revision
            {
                return Err(JobError::NonCanonicalRecord);
            }
        }
        for (job_id, tombstone) in &self.tombstones {
            let submission = submissions_by_job
                .get(job_id)
                .copied()
                .ok_or(JobError::NonCanonicalRecord)?;
            validate_tombstone(tombstone, submission, self.revision)?;
        }
        Ok(())
    }

    fn validate_replay_history(&self) -> Result<(), JobError> {
        let applied_count =
            u64::try_from(self.applied_commands.len()).map_err(|_| JobError::NonCanonicalRecord)?;
        let revisions = self
            .applied_commands
            .values()
            .map(|applied| applied.receipt.ledger_revision)
            .collect::<std::collections::BTreeSet<_>>();
        let retained = self
            .revision
            .checked_sub(self.compacted_through_revision)
            .ok_or(JobError::NonCanonicalRecord)?;
        if applied_count != retained
            || self.applied_commands.values().any(|applied| {
                applied.receipt.duplicate
                    || applied.receipt.job_revision == 0
                    || applied.receipt.ledger_revision == 0
                    || applied.receipt.ledger_revision > self.revision
            })
            || revisions.len() != self.applied_commands.len()
            || revisions.first().copied()
                != (retained > 0).then_some(self.compacted_through_revision + 1)
            || revisions.last().copied() != (self.revision > 0).then_some(self.revision)
        {
            return Err(JobError::NonCanonicalRecord);
        }
        Ok(())
    }

    fn snapshot_bytes(&self) -> Result<usize, JobError> {
        const FIXED: usize = 4 + 2 + 8 + 8 + 8 + 16 + 4 + 4 + 4 + 4 + CHECKSUM_BYTES;
        const SUBMISSION_BYTES: usize = 16 + 16 + 32 + 8 + 8;
        const APPLIED_COMMAND_BYTES: usize = 16 + 32 + 8 + 8 + 1 + 1;
        let jobs = self.jobs.values().try_fold(0_usize, |total, record| {
            total
                .checked_add(job_record_bytes(record)?)
                .ok_or(JobError::SnapshotCapacity)
        })?;
        let applied = self
            .applied_commands
            .len()
            .checked_mul(APPLIED_COMMAND_BYTES)
            .ok_or(JobError::SnapshotCapacity)?;
        let submissions = self
            .submission_requests
            .len()
            .checked_mul(SUBMISSION_BYTES)
            .ok_or(JobError::SnapshotCapacity)?;
        let tombstones = self
            .tombstones
            .values()
            .try_fold(0_usize, |total, tombstone| {
                total
                    .checked_add(tombstone_bytes(tombstone))
                    .ok_or(JobError::SnapshotCapacity)
            })?;
        FIXED
            .checked_add(jobs)
            .and_then(|value| value.checked_add(tombstones))
            .and_then(|value| value.checked_add(submissions))
            .and_then(|value| value.checked_add(applied))
            .ok_or(JobError::SnapshotCapacity)
    }

    fn lifecycle_reserve_bytes(&self) -> Result<usize, JobError> {
        self.jobs.values().try_fold(0_usize, |total, record| {
            let reserve = remaining_lifecycle_bytes(record)?;
            total.checked_add(reserve).ok_or(JobError::SnapshotCapacity)
        })
    }

    fn replay_reserve_bytes(&self) -> Result<usize, JobError> {
        const APPLIED_COMMAND_BYTES: usize = 16 + 32 + 8 + 8 + 1 + 1;
        MAX_APPLIED_COMMANDS
            .saturating_sub(self.applied_commands.len())
            .checked_mul(APPLIED_COMMAND_BYTES)
            .ok_or(JobError::SnapshotCapacity)
    }

    fn refresh_submission_job_revision(
        &mut self,
        job_id: AnalyticsJobId,
        job_revision: u64,
    ) -> Result<(), JobError> {
        let request_id = if let Some(record) = self.jobs.get(&job_id) {
            Some(record.spec.submission_request_id)
        } else if self.tombstones.contains_key(&job_id) {
            self.submission_requests
                .iter()
                .find_map(|(request, record)| (record.job_id == job_id).then_some(*request))
        } else {
            None
        }
        .or_else(|| {
            self.submission_requests
                .iter()
                .find_map(|(request, record)| (record.job_id == job_id).then_some(*request))
        })
        .ok_or(JobError::NonCanonicalRecord)?;
        self.submission_requests
            .get_mut(&request_id)
            .ok_or(JobError::NonCanonicalRecord)?
            .job_revision = job_revision;
        Ok(())
    }

    fn compact_replay_history(&mut self) {
        while self.applied_commands.len() > MAX_APPLIED_COMMANDS {
            let Some((command_id, revision)) = self
                .applied_commands
                .iter()
                .min_by_key(|(_, applied)| applied.receipt.ledger_revision)
                .map(|(id, applied)| (*id, applied.receipt.ledger_revision))
            else {
                break;
            };
            self.applied_commands.remove(&command_id);
            self.compacted_through_revision = revision;
        }
    }
}

fn ensure_active(record: &JobRecord) -> Result<(), JobError> {
    if record.state.is_terminal() {
        return Err(JobError::TerminalJob);
    }
    if !matches!(record.state, JobState::Leased | JobState::Running) {
        return Err(JobError::InvalidStateTransition);
    }
    Ok(())
}

fn ensure_topology(record: &JobRecord, topology_epoch: u64) -> Result<(), JobError> {
    if record.spec.topology_epoch != topology_epoch {
        return Err(JobError::StaleTopology {
            expected: record.spec.topology_epoch,
            actual: topology_epoch,
        });
    }
    Ok(())
}

fn ensure_fence(
    record: &JobRecord,
    gateway_id: u64,
    lease_epoch: u64,
    topology_epoch: u64,
) -> Result<(), JobError> {
    ensure_topology(record, topology_epoch)?;
    let lease = record.lease.ok_or(JobError::StaleLease)?;
    if lease.owner_gateway_id != gateway_id || lease.lease_epoch != lease_epoch {
        return Err(JobError::StaleLease);
    }
    Ok(())
}

fn ensure_running_and_fenced(
    record: &JobRecord,
    gateway_id: u64,
    lease_epoch: u64,
    topology_epoch: u64,
) -> Result<(), JobError> {
    if record.state.is_terminal() {
        return Err(JobError::TerminalJob);
    }
    if record.state != JobState::Running {
        return Err(JobError::InvalidStateTransition);
    }
    ensure_fence(record, gateway_id, lease_epoch, topology_epoch)
}

fn ensure_manifest_compatibility(
    record: &JobRecord,
    manifest: &ArtifactManifest,
) -> Result<(), JobError> {
    if manifest.provider_version != record.spec.provider_version
        || manifest.algorithm_version != record.spec.algorithm_version
    {
        return Err(JobError::IncompatibleArtifact);
    }
    if let Some(checkpoint) = &record.checkpoint
        && (checkpoint.projection_identity != manifest.projection_identity
            || checkpoint.input_applied_index_count != manifest.input_applied_index_count
            || checkpoint.input_applied_indexes_digest != manifest.input_applied_indexes_digest)
    {
        return Err(JobError::IncompatibleArtifact);
    }
    Ok(())
}

fn validate_job_record(
    job_id: AnalyticsJobId,
    record: &JobRecord,
    ledger_revision: u64,
) -> Result<(), JobError> {
    let minimum_revision = match record.state {
        JobState::Queued => 1,
        JobState::Leased | JobState::Canceled => 2,
        JobState::Running | JobState::Failed => 3,
        JobState::Succeeded => 4,
    };
    let active = matches!(record.state, JobState::Leased | JobState::Running);
    if record.spec.job_id != job_id
        || record.job_revision < minimum_revision
        || record.job_revision > ledger_revision
        || record.last_lease_epoch > record.job_revision
        || active != record.lease.is_some()
        || lease_mismatch(record)
        || matches!(
            record.state,
            JobState::Leased | JobState::Running | JobState::Succeeded | JobState::Failed
        ) && record.last_lease_epoch == 0
        || (record.state == JobState::Succeeded) != record.result.is_some()
        || (record.state == JobState::Failed) != record.failure.is_some()
        || (record.state != JobState::Succeeded && record.result.is_some())
        || (record.state != JobState::Failed && record.failure.is_some())
    {
        return Err(JobError::NonCanonicalRecord);
    }
    if let Some(checkpoint) = &record.checkpoint {
        ensure_manifest_compatibility(record, checkpoint)
            .map_err(|_| JobError::NonCanonicalRecord)?;
    }
    if let Some(result) = &record.result {
        ensure_manifest_compatibility(record, result).map_err(|_| JobError::NonCanonicalRecord)?;
    }
    Ok(())
}

fn lease_mismatch(record: &JobRecord) -> bool {
    record.lease.is_some_and(|lease| {
        lease.lease_epoch != record.last_lease_epoch
            || lease.owner_gateway_id == 0
            || lease.expires_unix_ms == 0
    })
}

fn spec_submission_digest(spec: &JobSpec) -> Result<[u8; 32], JobError> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"DTGProxy/AnalyticsSubmissionIntent/Latest");
    hasher.update(&spec.graph_id.to_be_bytes());
    match &spec.projection {
        GraphProjectionScope::Snapshot { valid_time } => {
            hasher.update(&[1]);
            hasher.update(&valid_time.as_micros().to_be_bytes());
        }
        GraphProjectionScope::Event => {
            hasher.update(&[2]);
        }
        GraphProjectionScope::Interval {
            valid_from,
            valid_to,
        } => {
            hasher.update(&[3]);
            hasher.update(&valid_from.as_micros().to_be_bytes());
            hasher.update(&valid_to.as_micros().to_be_bytes());
        }
        GraphProjectionScope::Delta { before, after } => {
            hasher.update(&[4]);
            hasher.update(&before.as_micros().to_be_bytes());
            hasher.update(&after.as_micros().to_be_bytes());
        }
    }
    hasher.update(&(spec.algorithm.len() as u64).to_be_bytes());
    hasher.update(spec.algorithm.as_bytes());
    hasher.update(&(spec.parameters.len() as u64).to_be_bytes());
    hasher.update(&spec.parameters);
    hasher.update(&spec.security_fingerprint);
    Ok(*hasher.finalize().as_bytes())
}

fn applied_indexes_digest(indexes: &BTreeMap<u32, u64>) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"DTGProxy/Analytics/InputAppliedIndexes/1");
    for (shard_id, index) in indexes {
        hasher.update(&shard_id.to_be_bytes());
        hasher.update(&index.to_be_bytes());
    }
    *hasher.finalize().as_bytes()
}

#[must_use]
pub fn canonical_applied_indexes_digest(indexes: &BTreeMap<u32, u64>) -> [u8; 32] {
    applied_indexes_digest(indexes)
}

fn advance_job_revision(record: &mut JobRecord) -> Result<u64, JobError> {
    record.job_revision = record
        .job_revision
        .checked_add(1)
        .ok_or(JobError::RevisionExhausted)?;
    Ok(record.job_revision)
}

fn validate_name(value: &str) -> Result<(), JobError> {
    if value.is_empty() || value.len() > MAX_NAME_BYTES || value.chars().any(char::is_control) {
        return Err(JobError::InvalidName);
    }
    Ok(())
}

fn validate_error_text(value: &str) -> Result<(), JobError> {
    if value.is_empty() || value.len() > MAX_ERROR_BYTES || value.chars().any(char::is_control) {
        return Err(JobError::InvalidFailure);
    }
    Ok(())
}

fn encode_command_body(output: &mut Vec<u8>, body: &JobCommandBody) -> Result<(), JobError> {
    match body {
        JobCommandBody::Submit {
            spec,
            submitted_at_unix_ms,
        } => {
            output.push(1);
            encode_job_spec(output, spec)?;
            output.extend_from_slice(&submitted_at_unix_ms.to_be_bytes());
        }
        JobCommandBody::Claim {
            job_id,
            expected_job_revision,
            gateway_id,
            topology_epoch,
            expires_unix_ms,
        } => {
            output.push(2);
            encode_job_fence(
                output,
                *job_id,
                *expected_job_revision,
                *gateway_id,
                *topology_epoch,
            );
            output.extend_from_slice(&expires_unix_ms.to_be_bytes());
        }
        JobCommandBody::Renew {
            job_id,
            expected_job_revision,
            gateway_id,
            lease_epoch,
            topology_epoch,
            expires_unix_ms,
        } => {
            output.push(3);
            encode_lease_fence(
                output,
                *job_id,
                *expected_job_revision,
                *gateway_id,
                *lease_epoch,
                *topology_epoch,
            );
            output.extend_from_slice(&expires_unix_ms.to_be_bytes());
        }
        JobCommandBody::BeginRun {
            job_id,
            expected_job_revision,
            gateway_id,
            lease_epoch,
            topology_epoch,
        } => {
            output.push(4);
            encode_lease_fence(
                output,
                *job_id,
                *expected_job_revision,
                *gateway_id,
                *lease_epoch,
                *topology_epoch,
            );
        }
        JobCommandBody::CommitCheckpoint {
            job_id,
            expected_job_revision,
            gateway_id,
            lease_epoch,
            topology_epoch,
            manifest,
        } => {
            output.push(5);
            encode_lease_fence(
                output,
                *job_id,
                *expected_job_revision,
                *gateway_id,
                *lease_epoch,
                *topology_epoch,
            );
            encode_manifest(output, manifest)?;
        }
        JobCommandBody::PublishResult {
            job_id,
            expected_job_revision,
            gateway_id,
            lease_epoch,
            topology_epoch,
            manifest,
        } => {
            output.push(6);
            encode_lease_fence(
                output,
                *job_id,
                *expected_job_revision,
                *gateway_id,
                *lease_epoch,
                *topology_epoch,
            );
            encode_manifest(output, manifest)?;
        }
        JobCommandBody::Fail {
            job_id,
            expected_job_revision,
            gateway_id,
            lease_epoch,
            topology_epoch,
            code,
            message,
        } => {
            output.push(7);
            encode_lease_fence(
                output,
                *job_id,
                *expected_job_revision,
                *gateway_id,
                *lease_epoch,
                *topology_epoch,
            );
            write_string(output, code)?;
            write_string(output, message)?;
        }
        JobCommandBody::Cancel {
            job_id,
            expected_job_revision,
        } => {
            output.push(8);
            output.extend_from_slice(&job_id.0.to_be_bytes());
            output.extend_from_slice(&expected_job_revision.to_be_bytes());
        }
        JobCommandBody::ExpireLease {
            job_id,
            expected_job_revision,
            lease_epoch,
            now_unix_ms,
        } => {
            output.push(9);
            output.extend_from_slice(&job_id.0.to_be_bytes());
            output.extend_from_slice(&expected_job_revision.to_be_bytes());
            output.extend_from_slice(&lease_epoch.to_be_bytes());
            output.extend_from_slice(&now_unix_ms.to_be_bytes());
        }
        JobCommandBody::PruneTerminal {
            job_id,
            expected_job_revision,
            pruned_at_unix_ms,
        } => {
            output.push(10);
            output.extend_from_slice(&job_id.0.to_be_bytes());
            output.extend_from_slice(&expected_job_revision.to_be_bytes());
            output.extend_from_slice(&pruned_at_unix_ms.to_be_bytes());
        }
        JobCommandBody::CompactTombstones { through_unix_ms } => {
            output.push(11);
            output.extend_from_slice(&through_unix_ms.to_be_bytes());
        }
        JobCommandBody::AcknowledgeArtifactsReclaimed {
            job_id,
            expected_tombstone_revision,
            gateway_id,
            gc_epoch,
            reclaimed_at_unix_ms,
        } => {
            output.push(12);
            output.extend_from_slice(&job_id.0.to_be_bytes());
            output.extend_from_slice(&expected_tombstone_revision.to_be_bytes());
            output.extend_from_slice(&gateway_id.to_be_bytes());
            output.extend_from_slice(&gc_epoch.to_be_bytes());
            output.extend_from_slice(&reclaimed_at_unix_ms.to_be_bytes());
        }
    }
    Ok(())
}

fn decode_command_body(reader: &mut Reader<'_>) -> Result<JobCommandBody, JobError> {
    match reader.u8()? {
        1 => Ok(JobCommandBody::Submit {
            spec: decode_job_spec(reader)?,
            submitted_at_unix_ms: nonzero_timestamp(reader.u64()?)?,
        }),
        2 => {
            let (job_id, expected_job_revision, gateway_id, topology_epoch) =
                decode_job_fence(reader)?;
            Ok(JobCommandBody::Claim {
                job_id,
                expected_job_revision,
                gateway_id,
                topology_epoch,
                expires_unix_ms: nonzero_timestamp(reader.u64()?)?,
            })
        }
        3 => {
            let (job_id, expected_job_revision, gateway_id, lease_epoch, topology_epoch) =
                decode_lease_fence(reader)?;
            Ok(JobCommandBody::Renew {
                job_id,
                expected_job_revision,
                gateway_id,
                lease_epoch,
                topology_epoch,
                expires_unix_ms: nonzero_timestamp(reader.u64()?)?,
            })
        }
        4 => {
            let (job_id, expected_job_revision, gateway_id, lease_epoch, topology_epoch) =
                decode_lease_fence(reader)?;
            Ok(JobCommandBody::BeginRun {
                job_id,
                expected_job_revision,
                gateway_id,
                lease_epoch,
                topology_epoch,
            })
        }
        5 | 6 => {
            let tag = reader.last_u8();
            let (job_id, expected_job_revision, gateway_id, lease_epoch, topology_epoch) =
                decode_lease_fence(reader)?;
            let manifest = decode_manifest(reader)?;
            if tag == 5 {
                if manifest.kind != ArtifactKind::Checkpoint {
                    return Err(JobError::NonCanonicalRecord);
                }
                Ok(JobCommandBody::CommitCheckpoint {
                    job_id,
                    expected_job_revision,
                    gateway_id,
                    lease_epoch,
                    topology_epoch,
                    manifest,
                })
            } else {
                if manifest.kind != ArtifactKind::Result {
                    return Err(JobError::NonCanonicalRecord);
                }
                Ok(JobCommandBody::PublishResult {
                    job_id,
                    expected_job_revision,
                    gateway_id,
                    lease_epoch,
                    topology_epoch,
                    manifest,
                })
            }
        }
        7 => {
            let (job_id, expected_job_revision, gateway_id, lease_epoch, topology_epoch) =
                decode_lease_fence(reader)?;
            let code = reader.string(MAX_ERROR_BYTES)?;
            let message = reader.string(MAX_ERROR_BYTES)?;
            validate_error_text(&code)?;
            validate_error_text(&message)?;
            Ok(JobCommandBody::Fail {
                job_id,
                expected_job_revision,
                gateway_id,
                lease_epoch,
                topology_epoch,
                code,
                message,
            })
        }
        8 => Ok(JobCommandBody::Cancel {
            job_id: AnalyticsJobId::new(reader.u128()?)?,
            expected_job_revision: nonzero_revision(reader.u64()?)?,
        }),
        9 => Ok(JobCommandBody::ExpireLease {
            job_id: AnalyticsJobId::new(reader.u128()?)?,
            expected_job_revision: nonzero_revision(reader.u64()?)?,
            lease_epoch: nonzero_lease(reader.u64()?)?,
            now_unix_ms: nonzero_timestamp(reader.u64()?)?,
        }),
        10 => Ok(JobCommandBody::PruneTerminal {
            job_id: AnalyticsJobId::new(reader.u128()?)?,
            expected_job_revision: nonzero_revision(reader.u64()?)?,
            pruned_at_unix_ms: nonzero_timestamp(reader.u64()?)?,
        }),
        11 => Ok(JobCommandBody::CompactTombstones {
            through_unix_ms: nonzero_timestamp(reader.u64()?)?,
        }),
        12 => Ok(JobCommandBody::AcknowledgeArtifactsReclaimed {
            job_id: AnalyticsJobId::new(reader.u128()?)?,
            expected_tombstone_revision: nonzero_revision(reader.u64()?)?,
            gateway_id: nonzero_lease(reader.u64()?)?,
            gc_epoch: nonzero_lease(reader.u64()?)?,
            reclaimed_at_unix_ms: nonzero_timestamp(reader.u64()?)?,
        }),
        tag => Err(JobError::UnknownCommandTag { tag }),
    }
}

fn encode_job_fence(
    output: &mut Vec<u8>,
    job_id: AnalyticsJobId,
    expected_job_revision: u64,
    gateway_id: u64,
    topology_epoch: u64,
) {
    output.extend_from_slice(&job_id.0.to_be_bytes());
    output.extend_from_slice(&expected_job_revision.to_be_bytes());
    output.extend_from_slice(&gateway_id.to_be_bytes());
    output.extend_from_slice(&topology_epoch.to_be_bytes());
}

fn encode_lease_fence(
    output: &mut Vec<u8>,
    job_id: AnalyticsJobId,
    expected_job_revision: u64,
    gateway_id: u64,
    lease_epoch: u64,
    topology_epoch: u64,
) {
    encode_job_fence(
        output,
        job_id,
        expected_job_revision,
        gateway_id,
        topology_epoch,
    );
    output.extend_from_slice(&lease_epoch.to_be_bytes());
}

fn decode_job_fence(reader: &mut Reader<'_>) -> Result<(AnalyticsJobId, u64, u64, u64), JobError> {
    let job_id = AnalyticsJobId::new(reader.u128()?)?;
    let expected_job_revision = nonzero_revision(reader.u64()?)?;
    let gateway_id = nonzero_lease(reader.u64()?)?;
    let topology_epoch = nonzero_lease(reader.u64()?)?;
    Ok((job_id, expected_job_revision, gateway_id, topology_epoch))
}

fn decode_lease_fence(
    reader: &mut Reader<'_>,
) -> Result<(AnalyticsJobId, u64, u64, u64, u64), JobError> {
    let (job_id, expected_job_revision, gateway_id, topology_epoch) = decode_job_fence(reader)?;
    let lease_epoch = nonzero_lease(reader.u64()?)?;
    Ok((
        job_id,
        expected_job_revision,
        gateway_id,
        lease_epoch,
        topology_epoch,
    ))
}

fn encode_job_spec(output: &mut Vec<u8>, spec: &JobSpec) -> Result<(), JobError> {
    output.extend_from_slice(&spec.job_id.0.to_be_bytes());
    output.extend_from_slice(&spec.submission_request_id.to_be_bytes());
    output.extend_from_slice(&spec.graph_id.to_be_bytes());
    output.extend_from_slice(&spec.catalog_revision.to_be_bytes());
    output.extend_from_slice(&spec.topology_epoch.to_be_bytes());
    output.extend_from_slice(&spec.schema_version.to_be_bytes());
    output.extend_from_slice(&spec.backend_generation.to_be_bytes());
    encode_transaction_time(output, spec.transaction_time);
    encode_projection(output, &spec.projection);
    write_string(output, &spec.algorithm)?;
    write_string(output, &spec.algorithm_version)?;
    write_string(output, &spec.provider)?;
    write_string(output, &spec.provider_version)?;
    write_bytes(output, &spec.parameters)?;
    output.extend_from_slice(&spec.security_fingerprint);
    encode_limits(output, spec.limits);
    Ok(())
}

fn decode_job_spec(reader: &mut Reader<'_>) -> Result<JobSpec, JobError> {
    let job_id = AnalyticsJobId::new(reader.u128()?)?;
    let submission_request_id = reader.u128()?;
    let graph_id = nonzero_spec(reader.u64()?)?;
    let catalog_revision = nonzero_spec(reader.u64()?)?;
    let topology_epoch = nonzero_spec(reader.u64()?)?;
    let schema_version = nonzero_spec(reader.u64()?)?;
    let backend_generation = nonzero_spec(reader.u64()?)?;
    let transaction_time = decode_transaction_time(reader)?;
    let projection = decode_projection(reader)?;
    let algorithm = reader.string(MAX_NAME_BYTES)?;
    let algorithm_version = reader.string(MAX_NAME_BYTES)?;
    let provider = reader.string(MAX_NAME_BYTES)?;
    let provider_version = reader.string(MAX_NAME_BYTES)?;
    let parameters = reader.sized_bytes(MAX_PARAMETER_BYTES)?;
    let mut security_fingerprint = [0; 32];
    security_fingerprint.copy_from_slice(reader.bytes(32)?);
    let limits = decode_limits(reader)?;
    JobSpec::new(
        job_id,
        submission_request_id,
        graph_id,
        catalog_revision,
        topology_epoch,
        schema_version,
        backend_generation,
        transaction_time,
        projection,
        algorithm,
        algorithm_version,
        provider,
        provider_version,
        parameters,
        security_fingerprint,
        limits,
    )
}

fn encode_projection(output: &mut Vec<u8>, projection: &GraphProjectionScope) {
    match projection {
        GraphProjectionScope::Snapshot { valid_time } => {
            output.push(1);
            output.extend_from_slice(&valid_time.as_micros().to_be_bytes());
        }
        GraphProjectionScope::Event => output.push(2),
        GraphProjectionScope::Interval {
            valid_from,
            valid_to,
        } => {
            output.push(3);
            output.extend_from_slice(&valid_from.as_micros().to_be_bytes());
            output.extend_from_slice(&valid_to.as_micros().to_be_bytes());
        }
        GraphProjectionScope::Delta { before, after } => {
            output.push(4);
            output.extend_from_slice(&before.as_micros().to_be_bytes());
            output.extend_from_slice(&after.as_micros().to_be_bytes());
        }
    }
}

fn decode_projection(reader: &mut Reader<'_>) -> Result<GraphProjectionScope, JobError> {
    let projection = match reader.u8()? {
        1 => GraphProjectionScope::Snapshot {
            valid_time: ValidTime::from_micros(reader.i64()?),
        },
        2 => GraphProjectionScope::Event,
        3 => GraphProjectionScope::Interval {
            valid_from: ValidTime::from_micros(reader.i64()?),
            valid_to: ValidTime::from_micros(reader.i64()?),
        },
        4 => GraphProjectionScope::Delta {
            before: ValidTime::from_micros(reader.i64()?),
            after: ValidTime::from_micros(reader.i64()?),
        },
        tag => return Err(JobError::UnknownProjectionTag { tag }),
    };
    projection.validate()?;
    Ok(projection)
}

fn encode_limits(output: &mut Vec<u8>, limits: ProjectionLimits) {
    output.extend_from_slice(&limits.max_vertices.to_be_bytes());
    output.extend_from_slice(&limits.max_edges.to_be_bytes());
    output.extend_from_slice(&limits.max_bytes.to_be_bytes());
}

fn decode_limits(reader: &mut Reader<'_>) -> Result<ProjectionLimits, JobError> {
    ProjectionLimits::new(reader.u64()?, reader.u64()?, reader.u64()?)
}

fn encode_transaction_time(output: &mut Vec<u8>, time: TransactionTime) {
    output.extend_from_slice(&time.physical_micros().to_be_bytes());
    output.extend_from_slice(&time.logical().to_be_bytes());
}

fn decode_transaction_time(reader: &mut Reader<'_>) -> Result<TransactionTime, JobError> {
    Ok(TransactionTime::new(reader.i64()?, reader.u32()?))
}

fn encode_manifest(output: &mut Vec<u8>, manifest: &ArtifactManifest) -> Result<(), JobError> {
    output.push(match manifest.kind {
        ArtifactKind::Checkpoint => 1,
        ArtifactKind::Result => 2,
    });
    output.extend_from_slice(&manifest.generation.to_be_bytes());
    output.extend_from_slice(&manifest.storage_shard_id.to_be_bytes());
    output.extend_from_slice(&manifest.chunk_count.to_be_bytes());
    output.extend_from_slice(&manifest.total_bytes.to_be_bytes());
    output.extend_from_slice(&manifest.content_digest);
    output.extend_from_slice(&manifest.projection_identity);
    write_string(output, &manifest.provider_version)?;
    write_string(output, &manifest.algorithm_version)?;
    encode_next_stage(output, manifest.next_stage);
    output.extend_from_slice(&manifest.input_applied_index_count.to_be_bytes());
    output.extend_from_slice(&manifest.input_applied_indexes_digest);
    Ok(())
}

fn decode_manifest(reader: &mut Reader<'_>) -> Result<ArtifactManifest, JobError> {
    let kind = match reader.u8()? {
        1 => ArtifactKind::Checkpoint,
        2 => ArtifactKind::Result,
        tag => return Err(JobError::UnknownArtifactKind { tag }),
    };
    let generation = reader.u64()?;
    let storage_shard_id = reader.u32()?;
    let chunk_count = reader.u64()?;
    let total_bytes = reader.u64()?;
    let mut content_digest = [0; 32];
    content_digest.copy_from_slice(reader.bytes(32)?);
    let mut projection_identity = [0; 32];
    projection_identity.copy_from_slice(reader.bytes(32)?);
    let provider_version = reader.string(MAX_NAME_BYTES)?;
    let algorithm_version = reader.string(MAX_NAME_BYTES)?;
    let next_stage = decode_next_stage(reader)?;
    let input_applied_index_count = reader.u32()?;
    let mut input_applied_indexes_digest = [0; 32];
    input_applied_indexes_digest.copy_from_slice(reader.bytes(32)?);
    validate_artifact_manifest(
        kind,
        generation,
        storage_shard_id,
        chunk_count,
        total_bytes,
        content_digest,
        projection_identity,
        &provider_version,
        &algorithm_version,
        next_stage,
        input_applied_index_count,
        input_applied_indexes_digest,
    )?;
    Ok(ArtifactManifest {
        kind,
        generation,
        storage_shard_id,
        chunk_count,
        total_bytes,
        content_digest,
        projection_identity,
        provider_version,
        algorithm_version,
        next_stage,
        input_applied_index_count,
        input_applied_indexes_digest,
    })
}

fn encode_next_stage(output: &mut Vec<u8>, stage: NextExecutionStage) {
    match stage {
        NextExecutionStage::Provider { completed_units } => {
            output.push(1);
            output.extend_from_slice(&completed_units.to_be_bytes());
        }
        NextExecutionStage::PublishResult => output.push(2),
        NextExecutionStage::Complete => output.push(3),
    }
}

fn decode_next_stage(reader: &mut Reader<'_>) -> Result<NextExecutionStage, JobError> {
    match reader.u8()? {
        1 => Ok(NextExecutionStage::Provider {
            completed_units: reader.u64()?,
        }),
        2 => Ok(NextExecutionStage::PublishResult),
        3 => Ok(NextExecutionStage::Complete),
        tag => Err(JobError::UnknownExecutionStage { tag }),
    }
}

fn encode_job_record(output: &mut Vec<u8>, record: &JobRecord) -> Result<(), JobError> {
    encode_job_spec(output, &record.spec)?;
    output.extend_from_slice(&record.submitted_at_unix_ms.to_be_bytes());
    output.extend_from_slice(&record.job_revision.to_be_bytes());
    output.push(encode_state(record.state));
    encode_optional_lease(output, record.lease);
    output.extend_from_slice(&record.last_lease_epoch.to_be_bytes());
    encode_optional_manifest(output, record.checkpoint.as_ref())?;
    encode_optional_manifest(output, record.result.as_ref())?;
    match &record.failure {
        None => output.push(0),
        Some((code, message)) => {
            output.push(1);
            write_string(output, code)?;
            write_string(output, message)?;
        }
    }
    Ok(())
}

fn encode_tombstone_entry(output: &mut Vec<u8>, job_id: AnalyticsJobId, tombstone: &JobTombstone) {
    output.extend_from_slice(&job_id.value().to_be_bytes());
    output.extend_from_slice(&tombstone.final_job_revision.to_be_bytes());
    output.push(encode_state(tombstone.terminal_state));
    output.extend_from_slice(&tombstone.pruned_at_unix_ms.to_be_bytes());
    encode_optional_artifact_identity(output, tombstone.checkpoint);
    encode_optional_artifact_identity(output, tombstone.result);
    match (tombstone.reclaimed_gc_epoch, tombstone.reclaimed_at_unix_ms) {
        (None, None) => output.push(0),
        (Some(gc_epoch), Some(reclaimed_at_unix_ms)) => {
            output.push(1);
            output.extend_from_slice(&gc_epoch.to_be_bytes());
            output.extend_from_slice(&reclaimed_at_unix_ms.to_be_bytes());
        }
        _ => unreachable!("validated tombstone reclamation pair"),
    }
}

fn decode_tombstone_entry(
    reader: &mut Reader<'_>,
) -> Result<(AnalyticsJobId, JobTombstone), JobError> {
    let job_id = AnalyticsJobId::new(reader.u128()?)?;
    let final_job_revision = nonzero_revision(reader.u64()?)?;
    let terminal_state = decode_state(reader.u8()?)?;
    let pruned_at_unix_ms = nonzero_timestamp(reader.u64()?)?;
    let checkpoint = decode_optional_artifact_identity(reader)?;
    let result = decode_optional_artifact_identity(reader)?;
    let (reclaimed_gc_epoch, reclaimed_at_unix_ms) = match reader.u8()? {
        0 => (None, None),
        1 => (
            Some(nonzero_lease(reader.u64()?)?),
            Some(nonzero_timestamp(reader.u64()?)?),
        ),
        _ => return Err(JobError::NonCanonicalRecord),
    };
    Ok((
        job_id,
        JobTombstone {
            final_job_revision,
            terminal_state,
            pruned_at_unix_ms,
            checkpoint,
            result,
            reclaimed_gc_epoch,
            reclaimed_at_unix_ms,
        },
    ))
}

fn encode_optional_artifact_identity(output: &mut Vec<u8>, identity: Option<(u32, u64)>) {
    match identity {
        None => output.push(0),
        Some((shard_id, generation)) => {
            output.push(1);
            output.extend_from_slice(&shard_id.to_be_bytes());
            output.extend_from_slice(&generation.to_be_bytes());
        }
    }
}

fn decode_optional_artifact_identity(
    reader: &mut Reader<'_>,
) -> Result<Option<(u32, u64)>, JobError> {
    match reader.u8()? {
        0 => Ok(None),
        1 => {
            let shard_id = reader.u32()?;
            let generation = reader.u64()?;
            if shard_id == 0 || generation == 0 {
                return Err(JobError::NonCanonicalRecord);
            }
            Ok(Some((shard_id, generation)))
        }
        _ => Err(JobError::NonCanonicalRecord),
    }
}

fn tombstone_bytes(tombstone: &JobTombstone) -> usize {
    const FIXED: usize = 16 + 8 + 1 + 8;
    let checkpoint = if tombstone.checkpoint.is_some() {
        1 + 4 + 8
    } else {
        1
    };
    let result = if tombstone.result.is_some() {
        1 + 4 + 8
    } else {
        1
    };
    let reclamation = if tombstone.artifacts_reclaimed() {
        1 + 8 + 8
    } else {
        1
    };
    FIXED + checkpoint + result + reclamation
}

fn validate_tombstone(
    tombstone: &JobTombstone,
    submission: &SubmissionRecord,
    ledger_revision: u64,
) -> Result<(), JobError> {
    validate_tombstone_record(tombstone, ledger_revision)?;
    if tombstone.final_job_revision != submission.job_revision
        || tombstone.pruned_at_unix_ms < submission.submitted_at_unix_ms
    {
        return Err(JobError::NonCanonicalRecord);
    }
    Ok(())
}

fn validate_tombstone_record(
    tombstone: &JobTombstone,
    ledger_revision: u64,
) -> Result<(), JobError> {
    let valid_identity = |identity: Option<(u32, u64)>| {
        identity.is_none_or(|(shard_id, generation)| shard_id != 0 && generation != 0)
    };
    let valid_reclamation = match (tombstone.reclaimed_gc_epoch, tombstone.reclaimed_at_unix_ms) {
        (None, None) => true,
        (Some(gc_epoch), Some(reclaimed_at_unix_ms)) => {
            gc_epoch != 0 && reclaimed_at_unix_ms >= tombstone.pruned_at_unix_ms
        }
        _ => false,
    };
    if tombstone.final_job_revision == 0
        || tombstone.final_job_revision > ledger_revision
        || !tombstone.terminal_state.is_terminal()
        || tombstone.pruned_at_unix_ms == 0
        || !valid_identity(tombstone.checkpoint)
        || !valid_identity(tombstone.result)
        || (tombstone.terminal_state == JobState::Succeeded && tombstone.result.is_none())
        || !valid_reclamation
    {
        return Err(JobError::NonCanonicalRecord);
    }
    Ok(())
}

fn job_record_bytes(record: &JobRecord) -> Result<usize, JobError> {
    let mut total = job_spec_bytes(&record.spec)?
        .checked_add(8 + 8 + 1 + 8)
        .ok_or(JobError::SnapshotCapacity)?;
    total = total
        .checked_add(if record.lease.is_some() { 1 + 24 } else { 1 })
        .ok_or(JobError::SnapshotCapacity)?;
    let checkpoint = optional_manifest_bytes(record.checkpoint.as_ref())?;
    let result = optional_manifest_bytes(record.result.as_ref())?;
    total = total
        .checked_add(checkpoint)
        .and_then(|value| value.checked_add(result))
        .ok_or(JobError::SnapshotCapacity)?;
    total
        .checked_add(match &record.failure {
            None => 1,
            Some((code, message)) => 1 + string_bytes(code) + string_bytes(message),
        })
        .ok_or(JobError::SnapshotCapacity)
}

fn job_spec_bytes(spec: &JobSpec) -> Result<usize, JobError> {
    let fixed = 16 + 16 + 40 + 12 + 32 + 24;
    let projection = match spec.projection {
        GraphProjectionScope::Snapshot { .. } => 1 + 8,
        GraphProjectionScope::Event => 1,
        GraphProjectionScope::Interval { .. } | GraphProjectionScope::Delta { .. } => 1 + 16,
    };
    [
        fixed,
        projection,
        string_bytes(&spec.algorithm),
        string_bytes(&spec.algorithm_version),
        string_bytes(&spec.provider),
        string_bytes(&spec.provider_version),
        4 + spec.parameters.len(),
    ]
    .into_iter()
    .try_fold(0_usize, |total, value| {
        total.checked_add(value).ok_or(JobError::SnapshotCapacity)
    })
}

fn optional_manifest_bytes(manifest: Option<&ArtifactManifest>) -> Result<usize, JobError> {
    match manifest {
        None => Ok(1),
        Some(manifest) => manifest_bytes(manifest)?
            .checked_add(1)
            .ok_or(JobError::SnapshotCapacity),
    }
}

fn manifest_bytes(manifest: &ArtifactManifest) -> Result<usize, JobError> {
    let stage = match manifest.next_stage {
        NextExecutionStage::Provider { .. } => 1 + 8,
        NextExecutionStage::PublishResult | NextExecutionStage::Complete => 1,
    };
    [
        1 + 8 + 4 + 8 + 8 + 32 + 32,
        string_bytes(&manifest.provider_version),
        string_bytes(&manifest.algorithm_version),
        stage,
        4 + 32,
    ]
    .into_iter()
    .try_fold(0_usize, |total, value| {
        total.checked_add(value).ok_or(JobError::SnapshotCapacity)
    })
}

fn remaining_lifecycle_bytes(record: &JobRecord) -> Result<usize, JobError> {
    if record.state.is_terminal() {
        return Ok(0);
    }
    const MAX_STAGE_BYTES: usize = 1 + 8;
    const MAX_MANIFEST_BYTES: usize =
        1 + 8 + 4 + 8 + 8 + 32 + 32 + (4 + MAX_NAME_BYTES) * 2 + MAX_STAGE_BYTES + 4 + 32;
    let checkpoint_growth = match &record.checkpoint {
        Some(checkpoint) => MAX_MANIFEST_BYTES.saturating_sub(manifest_bytes(checkpoint)?),
        None => 1 + MAX_MANIFEST_BYTES,
    };
    const MAX_FAILURE_BYTES: usize = 1 + (4 + MAX_ERROR_BYTES) * 2;
    let terminal_growth = (1 + MAX_MANIFEST_BYTES).max(MAX_FAILURE_BYTES);
    let lease_growth = usize::from(record.lease.is_none()) * 24;
    checkpoint_growth
        .checked_add(terminal_growth)
        .and_then(|value| value.checked_add(lease_growth))
        .ok_or(JobError::SnapshotCapacity)
}

fn string_bytes(value: &str) -> usize {
    4 + value.len()
}

fn decode_job_record(reader: &mut Reader<'_>) -> Result<JobRecord, JobError> {
    let spec = decode_job_spec(reader)?;
    let submitted_at_unix_ms = nonzero_timestamp(reader.u64()?)?;
    let job_revision = nonzero_revision(reader.u64()?)?;
    let state = decode_state(reader.u8()?)?;
    let lease = decode_optional_lease(reader)?;
    let last_lease_epoch = reader.u64()?;
    let checkpoint = decode_optional_manifest(reader)?;
    let result = decode_optional_manifest(reader)?;
    let failure = match reader.u8()? {
        0 => None,
        1 => {
            let code = reader.string(MAX_ERROR_BYTES)?;
            let message = reader.string(MAX_ERROR_BYTES)?;
            validate_error_text(&code)?;
            validate_error_text(&message)?;
            Some((code, message))
        }
        _ => return Err(JobError::NonCanonicalRecord),
    };
    let active = matches!(state, JobState::Leased | JobState::Running);
    if active != lease.is_some()
        || lease.is_some_and(|value| value.lease_epoch != last_lease_epoch)
        || checkpoint
            .as_ref()
            .is_some_and(|value| value.kind != ArtifactKind::Checkpoint)
        || result
            .as_ref()
            .is_some_and(|value| value.kind != ArtifactKind::Result)
        || (state == JobState::Succeeded) != result.is_some()
        || (state == JobState::Failed) != failure.is_some()
    {
        return Err(JobError::NonCanonicalRecord);
    }
    Ok(JobRecord {
        spec,
        submitted_at_unix_ms,
        job_revision,
        state,
        lease,
        last_lease_epoch,
        checkpoint,
        result,
        failure,
    })
}

fn encode_optional_lease(output: &mut Vec<u8>, lease: Option<JobLease>) {
    match lease {
        None => output.push(0),
        Some(lease) => {
            output.push(1);
            output.extend_from_slice(&lease.owner_gateway_id.to_be_bytes());
            output.extend_from_slice(&lease.lease_epoch.to_be_bytes());
            output.extend_from_slice(&lease.expires_unix_ms.to_be_bytes());
        }
    }
}

fn decode_optional_lease(reader: &mut Reader<'_>) -> Result<Option<JobLease>, JobError> {
    match reader.u8()? {
        0 => Ok(None),
        1 => Ok(Some(JobLease {
            owner_gateway_id: nonzero_lease(reader.u64()?)?,
            lease_epoch: nonzero_lease(reader.u64()?)?,
            expires_unix_ms: nonzero_timestamp(reader.u64()?)?,
        })),
        _ => Err(JobError::NonCanonicalRecord),
    }
}

fn encode_optional_manifest(
    output: &mut Vec<u8>,
    manifest: Option<&ArtifactManifest>,
) -> Result<(), JobError> {
    match manifest {
        None => output.push(0),
        Some(manifest) => {
            output.push(1);
            encode_manifest(output, manifest)?;
        }
    }
    Ok(())
}

fn decode_optional_manifest(reader: &mut Reader<'_>) -> Result<Option<ArtifactManifest>, JobError> {
    match reader.u8()? {
        0 => Ok(None),
        1 => decode_manifest(reader).map(Some),
        _ => Err(JobError::NonCanonicalRecord),
    }
}

fn encode_state(state: JobState) -> u8 {
    match state {
        JobState::Queued => 1,
        JobState::Leased => 2,
        JobState::Running => 3,
        JobState::Succeeded => 4,
        JobState::Failed => 5,
        JobState::Canceled => 6,
    }
}

fn decode_state(value: u8) -> Result<JobState, JobError> {
    match value {
        1 => Ok(JobState::Queued),
        2 => Ok(JobState::Leased),
        3 => Ok(JobState::Running),
        4 => Ok(JobState::Succeeded),
        5 => Ok(JobState::Failed),
        6 => Ok(JobState::Canceled),
        tag => Err(JobError::UnknownJobState { tag }),
    }
}

fn encode_receipt(output: &mut Vec<u8>, receipt: LedgerApplyReceipt) {
    output.extend_from_slice(&receipt.ledger_revision.to_be_bytes());
    output.extend_from_slice(&receipt.job_revision.to_be_bytes());
    output.push(u8::from(receipt.duplicate));
    output.push(u8::from(receipt.request_duplicate));
}

fn decode_receipt(reader: &mut Reader<'_>) -> Result<LedgerApplyReceipt, JobError> {
    Ok(LedgerApplyReceipt {
        ledger_revision: reader.u64()?,
        job_revision: reader.u64()?,
        duplicate: match reader.u8()? {
            0 => false,
            1 => true,
            _ => return Err(JobError::NonCanonicalRecord),
        },
        request_duplicate: match reader.u8()? {
            0 => false,
            1 => true,
            _ => return Err(JobError::NonCanonicalRecord),
        },
    })
}

fn nonzero_timestamp(value: u64) -> Result<u64, JobError> {
    (value != 0)
        .then_some(value)
        .ok_or(JobError::InvalidTimestamp)
}

fn nonzero_revision(value: u64) -> Result<u64, JobError> {
    (value != 0)
        .then_some(value)
        .ok_or(JobError::InvalidJobRevision)
}

fn nonzero_lease(value: u64) -> Result<u64, JobError> {
    (value != 0).then_some(value).ok_or(JobError::InvalidLease)
}

fn nonzero_spec(value: u64) -> Result<u64, JobError> {
    (value != 0)
        .then_some(value)
        .ok_or(JobError::InvalidJobSpec)
}

fn write_count(output: &mut Vec<u8>, count: usize) -> Result<(), JobError> {
    let count = u32::try_from(count).map_err(|_| JobError::RecordTooLarge)?;
    output.extend_from_slice(&count.to_be_bytes());
    Ok(())
}

fn write_string(output: &mut Vec<u8>, value: &str) -> Result<(), JobError> {
    write_bytes(output, value.as_bytes())
}

fn write_bytes(output: &mut Vec<u8>, value: &[u8]) -> Result<(), JobError> {
    let length = u32::try_from(value.len()).map_err(|_| JobError::RecordTooLarge)?;
    output.extend_from_slice(&length.to_be_bytes());
    output.extend_from_slice(value);
    Ok(())
}

fn append_checksum(bytes: &mut Vec<u8>) {
    bytes.extend_from_slice(&crc32fast::hash(bytes).to_be_bytes());
}

fn verify_checksum(bytes: &[u8]) -> Result<(), JobError> {
    if bytes.len() < CHECKSUM_BYTES {
        return Err(JobError::TruncatedRecord);
    }
    let payload_len = bytes.len() - CHECKSUM_BYTES;
    let expected = u32::from_be_bytes(
        bytes[payload_len..]
            .try_into()
            .map_err(|_| JobError::TruncatedRecord)?,
    );
    if crc32fast::hash(&bytes[..payload_len]) != expected {
        return Err(JobError::ChecksumMismatch);
    }
    Ok(())
}

struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
    last_u8: u8,
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Result<Self, JobError> {
        let payload_len = bytes
            .len()
            .checked_sub(CHECKSUM_BYTES)
            .ok_or(JobError::TruncatedRecord)?;
        Ok(Self {
            bytes: &bytes[..payload_len],
            offset: 0,
            last_u8: 0,
        })
    }

    fn expect_magic(&mut self, expected: [u8; 4]) -> Result<(), JobError> {
        if self.bytes(4)? != expected {
            return Err(JobError::InvalidMagic);
        }
        Ok(())
    }

    fn expect_version(&mut self) -> Result<(), JobError> {
        if self.u16()? != FORMAT_VERSION {
            return Err(JobError::UnsupportedVersion);
        }
        Ok(())
    }

    fn finish(&self) -> Result<(), JobError> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(JobError::TrailingBytes)
        }
    }

    fn last_u8(&self) -> u8 {
        self.last_u8
    }

    fn bytes(&mut self, length: usize) -> Result<&'a [u8], JobError> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(JobError::TruncatedRecord)?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or(JobError::TruncatedRecord)?;
        self.offset = end;
        Ok(value)
    }

    fn u8(&mut self) -> Result<u8, JobError> {
        let value = self.bytes(1)?[0];
        self.last_u8 = value;
        Ok(value)
    }

    fn u16(&mut self) -> Result<u16, JobError> {
        Ok(u16::from_be_bytes(
            self.bytes(2)?
                .try_into()
                .map_err(|_| JobError::TruncatedRecord)?,
        ))
    }

    fn u32(&mut self) -> Result<u32, JobError> {
        Ok(u32::from_be_bytes(
            self.bytes(4)?
                .try_into()
                .map_err(|_| JobError::TruncatedRecord)?,
        ))
    }

    fn u64(&mut self) -> Result<u64, JobError> {
        Ok(u64::from_be_bytes(
            self.bytes(8)?
                .try_into()
                .map_err(|_| JobError::TruncatedRecord)?,
        ))
    }

    fn i64(&mut self) -> Result<i64, JobError> {
        Ok(i64::from_be_bytes(
            self.bytes(8)?
                .try_into()
                .map_err(|_| JobError::TruncatedRecord)?,
        ))
    }

    fn u128(&mut self) -> Result<u128, JobError> {
        Ok(u128::from_be_bytes(
            self.bytes(16)?
                .try_into()
                .map_err(|_| JobError::TruncatedRecord)?,
        ))
    }

    fn count(&mut self, max: usize) -> Result<usize, JobError> {
        let count = usize::try_from(self.u32()?).map_err(|_| JobError::RecordTooLarge)?;
        if count > max {
            return Err(JobError::RecordTooLarge);
        }
        Ok(count)
    }

    fn sized_bytes(&mut self, max: usize) -> Result<Vec<u8>, JobError> {
        let length = self.count(max)?;
        Ok(self.bytes(length)?.to_vec())
    }

    fn string(&mut self, max: usize) -> Result<String, JobError> {
        String::from_utf8(self.sized_bytes(max)?).map_err(|_| JobError::InvalidUtf8)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum JobError {
    InvalidJobId,
    InvalidCommandId,
    InvalidJobRevision,
    InvalidProjectionLimits,
    InvalidProjectionScope,
    InvalidJobSpec,
    InvalidName,
    InvalidTimestamp,
    InvalidLease,
    InvalidLeaseExtension,
    InvalidFailure,
    InvalidArtifactManifest,
    InvalidArtifactGeneration,
    InvalidRetentionPolicy,
    RetentionBytesExhausted,
    InvalidClaimableRequest,
    InvalidJobListRequest,
    InvalidTombstoneCompaction,
    ActiveSubmissionBeforeCompactionFloor,
    UnreclaimedTombstoneBeforeCompactionFloor,
    UnknownJob(AnalyticsJobId),
    JobAlreadyExists(AnalyticsJobId),
    JobCapacity,
    AppliedCommandCapacity,
    JobNotClaimable,
    InvalidStateTransition,
    TerminalJob,
    LeaseActive,
    StaleLease,
    StaleJobRevision {
        expected: u64,
        actual: u64,
    },
    StaleTombstoneRevision {
        expected: u64,
        actual: u64,
    },
    StaleGcEpoch,
    StaleTopology {
        expected: u64,
        actual: u64,
    },
    StaleArtifactGeneration,
    IncompatibleArtifact,
    LeaseEpochExhausted,
    RevisionExhausted,
    CommandReplayMismatch {
        command_id: u128,
    },
    SubmissionReplayMismatch {
        submission_request_id: u128,
    },
    ExpiredSubmission {
        floor_unix_ms: u64,
        actual_unix_ms: u64,
    },
    InvalidMagic,
    UnsupportedVersion,
    UnknownCommandTag {
        tag: u8,
    },
    UnknownProjectionTag {
        tag: u8,
    },
    UnknownArtifactKind {
        tag: u8,
    },
    UnknownExecutionStage {
        tag: u8,
    },
    UnknownJobState {
        tag: u8,
    },
    UnknownParameterTag {
        tag: u8,
    },
    InvalidUtf8,
    TruncatedRecord,
    TrailingBytes,
    ChecksumMismatch,
    NonCanonicalRecord,
    RecordTooLarge,
    SnapshotCapacity,
}

impl Display for JobError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "analytics ledger rejected operation: {self:?}")
    }
}

impl Error for JobError {}

#[cfg(test)]
mod internal_tests {
    use super::*;

    fn large_spec(id: u128) -> JobSpec {
        JobSpec::new(
            AnalyticsJobId::new(id).unwrap(),
            id + 10_000,
            1,
            1,
            1,
            1,
            1,
            TransactionTime::new(1, 0),
            GraphProjectionScope::Event,
            "dtg.graph.pageRank",
            "1.0.0",
            "dtg.analytics-native",
            "1.0.0",
            vec![0; MAX_PARAMETER_BYTES],
            [1; 32],
            ProjectionLimits::new(1, 1, 1).unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn apply_rejects_before_the_state_can_exceed_its_snapshot_budget() {
        let mut ledger = LedgerState::new();
        let mut accepted = 0_u128;
        for id in 1_u128..=128 {
            ledger
                .apply_body(JobCommandBody::Submit {
                    spec: large_spec(id),
                    submitted_at_unix_ms: 1,
                })
                .unwrap();
            let required = ledger
                .snapshot_bytes()
                .and_then(|snapshot| {
                    snapshot
                        .checked_add(ledger.lifecycle_reserve_bytes()?)
                        .and_then(|value| value.checked_add(ledger.replay_reserve_bytes().ok()?))
                        .ok_or(JobError::SnapshotCapacity)
                })
                .unwrap_or(usize::MAX);
            if required > MAX_SNAPSHOT_BYTES {
                ledger.jobs.remove(&AnalyticsJobId::new(id).unwrap());
                ledger.submission_requests.remove(&(id + 10_000));
                ledger.max_job_id = accepted;
                break;
            }
            accepted = id;
        }
        ledger.revision = u64::try_from(accepted).unwrap();
        for revision in 1..=ledger.revision {
            ledger.applied_commands.insert(
                u128::from(revision),
                AppliedCommand {
                    digest: [1; 32],
                    receipt: LedgerApplyReceipt {
                        ledger_revision: revision,
                        job_revision: 1,
                        duplicate: false,
                        request_duplicate: false,
                    },
                },
            );
        }
        assert!(ledger.ensure_admission_capacity().is_ok());
        let next_id = accepted + 1;
        assert_eq!(
            ledger
                .apply(JobCommand::submit(next_id, large_spec(next_id), 1).unwrap())
                .unwrap_err(),
            JobError::SnapshotCapacity
        );
        assert!(ledger.encode_snapshot().unwrap().len() <= MAX_SNAPSHOT_BYTES);
    }

    #[test]
    fn snapshot_encoding_rejects_an_impossible_replay_revision() {
        let mut impossible = LedgerState::new();
        impossible.revision = 1;

        assert_eq!(
            impossible.encode_snapshot().unwrap_err(),
            JobError::NonCanonicalRecord
        );
    }

    #[test]
    fn snapshot_encoding_rejects_unreachable_job_and_manifest_state() {
        let mut impossible = LedgerState::new();
        impossible
            .apply(JobCommand::submit(101, large_spec(101), 1).unwrap())
            .unwrap();
        impossible
            .jobs
            .get_mut(&AnalyticsJobId::new(101).unwrap())
            .unwrap()
            .job_revision = u64::MAX;
        assert_eq!(
            impossible.encode_snapshot().unwrap_err(),
            JobError::NonCanonicalRecord
        );

        let mut incompatible = LedgerState::new();
        let job = AnalyticsJobId::new(102).unwrap();
        incompatible
            .apply(JobCommand::submit(201, large_spec(102), 1).unwrap())
            .unwrap();
        incompatible
            .apply(JobCommand::claim(202, job, 1, 1, 1, 10).unwrap())
            .unwrap();
        incompatible
            .apply(JobCommand::begin_run(203, job, 2, 1, 1, 1).unwrap())
            .unwrap();
        incompatible.jobs.get_mut(&job).unwrap().checkpoint = Some(
            ArtifactManifest::new(
                ArtifactKind::Checkpoint,
                1,
                1,
                1,
                1,
                [1; 32],
                [2; 32],
                "different-provider-version",
                "1.0.0",
                NextExecutionStage::Provider { completed_units: 0 },
                BTreeMap::from([(1, 1)]),
            )
            .unwrap(),
        );
        assert_eq!(
            incompatible.encode_snapshot().unwrap_err(),
            JobError::NonCanonicalRecord
        );
    }

    #[test]
    fn compacted_submit_replay_cannot_resurrect_a_pruned_job() {
        let mut ledger = LedgerState::new();
        let job = AnalyticsJobId::new(301).unwrap();
        let submit = JobCommand::submit(301, large_spec(301), 1).unwrap();
        ledger.apply(submit.clone()).unwrap();
        ledger
            .apply(JobCommand::cancel(302, job, 1).unwrap())
            .unwrap();
        ledger
            .apply(JobCommand::prune_terminal(303, job, 2, 2).unwrap())
            .unwrap();
        ledger
            .apply(JobCommand::acknowledge_artifacts_reclaimed(304, job, 2, 1, 1, 2).unwrap())
            .unwrap();
        ledger.applied_commands.remove(&301);
        ledger.compacted_through_revision = 1;
        assert!(ledger.encode_snapshot().is_ok());

        let replay = ledger.apply(submit).unwrap();
        assert!(replay.request_duplicate());
        assert!(ledger.job(job).is_none());
        ledger
            .apply(JobCommand::compact_tombstones(305, 2).unwrap())
            .unwrap();
        for command_id in [301_u128, 302, 303, 304] {
            ledger.applied_commands.remove(&command_id);
        }
        ledger.compacted_through_revision = 5;
        assert!(ledger.encode_snapshot().is_ok());
        assert_eq!(
            ledger.apply(JobCommand::submit(301, large_spec(301), 1).unwrap()),
            Err(JobError::ExpiredSubmission {
                floor_unix_ms: 2,
                actual_unix_ms: 1,
            })
        );
        assert_eq!(
            ledger
                .apply(
                    JobCommand::submit(
                        304,
                        JobSpec::new(
                            job,
                            99_999,
                            1,
                            1,
                            1,
                            1,
                            1,
                            TransactionTime::new(1, 0),
                            GraphProjectionScope::Event,
                            "dtg.graph.pageRank",
                            "1.0.0",
                            "dtg.analytics-native",
                            "1.0.0",
                            Vec::new(),
                            [1; 32],
                            ProjectionLimits::new(1, 1, 1).unwrap(),
                        )
                        .unwrap(),
                        3,
                    )
                    .unwrap(),
                )
                .unwrap_err(),
            JobError::JobAlreadyExists(job)
        );
    }
}
