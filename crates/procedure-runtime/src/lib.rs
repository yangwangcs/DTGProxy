#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
use std::sync::Arc;

use analytics_api::{
    AlgorithmDescriptor, AlgorithmField, AlgorithmRequest, AlgorithmType, AlgorithmValue,
    AnalyticsOutput, AnalyticsProvider, GraphModel, ProjectedGraph, ProviderError,
    builtin_algorithm_descriptors,
};
use cypher_ast::CypherProfile;
use temporal_ir::{Column, ProcedureIdentity, RowSchema, SlotId, ValueType};

pub use temporal_ir::{ProcedureEffect, ProcedurePlacement};

mod cluster_coordinator;
mod job_procedures;

pub use cluster_coordinator::{
    AnalyticsCancelRequest, AnalyticsCancelResponse, AnalyticsResultPage, AnalyticsResultsRequest,
    AnalyticsStatus, AnalyticsStatusRequest, AnalyticsSubmitRequest, AnalyticsSubmitResponse,
    ClusterAnalyticsCoordinator, ClusterAnalyticsError, JobInvocationContext,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProcedureArgumentStyle {
    MapOrPositional,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ProcedurePermission {
    AnalyticsRead,
    GraphWrite,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProcedureAccess {
    capabilities: BTreeSet<ProcedurePermission>,
}

impl ProcedureAccess {
    #[must_use]
    pub fn allow_all() -> Self {
        Self {
            capabilities: [
                ProcedurePermission::AnalyticsRead,
                ProcedurePermission::GraphWrite,
            ]
            .into_iter()
            .collect(),
        }
    }

    #[must_use]
    pub fn analytics_read() -> Self {
        Self {
            capabilities: [ProcedurePermission::AnalyticsRead].into_iter().collect(),
        }
    }

    #[must_use]
    pub const fn denied() -> Self {
        Self {
            capabilities: BTreeSet::new(),
        }
    }

    #[must_use]
    pub fn allows(&self, permission: ProcedurePermission) -> bool {
        self.capabilities.contains(&permission)
    }
}

impl Default for ProcedureAccess {
    fn default() -> Self {
        Self::allow_all()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProcedureLimits {
    max_invocations: u32,
    max_input_rows: u64,
    max_output_rows: u64,
    max_value_bytes: u64,
    max_result_bytes: u64,
    max_graph_vertices: u64,
    max_graph_edges: u64,
    max_graph_bytes: u64,
}

impl ProcedureLimits {
    pub fn new(
        max_invocations: u32,
        max_input_rows: u64,
        max_output_rows: u64,
        max_value_bytes: u64,
        max_result_bytes: u64,
    ) -> Result<Self, ProcedureError> {
        if max_invocations == 0
            || max_input_rows == 0
            || max_output_rows == 0
            || max_value_bytes == 0
            || max_result_bytes == 0
        {
            return Err(ProcedureError::InvalidLimits);
        }
        Ok(Self {
            max_invocations,
            max_input_rows,
            max_output_rows,
            max_value_bytes,
            max_result_bytes,
            max_graph_vertices: 1_000_000,
            max_graph_edges: 1_000_000,
            max_graph_bytes: 128 << 20,
        })
    }

    pub fn with_graph_limits(
        mut self,
        max_graph_vertices: u64,
        max_graph_edges: u64,
        max_graph_bytes: u64,
    ) -> Result<Self, ProcedureError> {
        if max_graph_vertices == 0 || max_graph_edges == 0 || max_graph_bytes == 0 {
            return Err(ProcedureError::InvalidLimits);
        }
        self.max_graph_vertices = max_graph_vertices;
        self.max_graph_edges = max_graph_edges;
        self.max_graph_bytes = max_graph_bytes;
        Ok(self)
    }

    #[must_use]
    pub const fn analytics_default() -> Self {
        Self {
            max_invocations: 16_384,
            max_input_rows: 16_384,
            max_output_rows: 1_000_000,
            max_value_bytes: 1 << 20,
            max_result_bytes: 64 << 20,
            max_graph_vertices: 1_000_000,
            max_graph_edges: 1_000_000,
            max_graph_bytes: 128 << 20,
        }
    }

    #[must_use]
    pub const fn max_invocations(self) -> u32 {
        self.max_invocations
    }
    #[must_use]
    pub const fn max_input_rows(self) -> u64 {
        self.max_input_rows
    }
    #[must_use]
    pub const fn max_output_rows(self) -> u64 {
        self.max_output_rows
    }
    #[must_use]
    pub const fn max_value_bytes(self) -> u64 {
        self.max_value_bytes
    }
    #[must_use]
    pub const fn max_result_bytes(self) -> u64 {
        self.max_result_bytes
    }
    #[must_use]
    pub const fn max_graph_vertices(self) -> u64 {
        self.max_graph_vertices
    }
    #[must_use]
    pub const fn max_graph_edges(self) -> u64 {
        self.max_graph_edges
    }
    #[must_use]
    pub const fn max_graph_bytes(self) -> u64 {
        self.max_graph_bytes
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProcedureInput {
    name: String,
    value_type: ValueType,
    nullable: bool,
    default: Option<AlgorithmValue>,
    algorithm_type: Option<AlgorithmType>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProcedureField {
    name: String,
    value_type: ValueType,
    nullable: bool,
    default: Option<AlgorithmValue>,
}

impl ProcedureField {
    #[must_use]
    pub fn new(name: impl Into<String>, value_type: ValueType, nullable: bool) -> Self {
        Self {
            name: name.into(),
            value_type,
            nullable,
            default: None,
        }
    }

    #[must_use]
    pub fn with_default(mut self, default: AlgorithmValue) -> Self {
        self.default = Some(default);
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProcedureDefinition {
    name: String,
    inputs: Vec<ProcedureField>,
    outputs: Vec<ProcedureField>,
    effect: ProcedureEffect,
    permission: ProcedurePermission,
    placement: ProcedurePlacement,
    limits: ProcedureLimits,
}

impl ProcedureDefinition {
    #[must_use]
    pub fn new(
        name: impl Into<String>,
        inputs: Vec<ProcedureField>,
        outputs: Vec<ProcedureField>,
        effect: ProcedureEffect,
        permission: ProcedurePermission,
        placement: ProcedurePlacement,
    ) -> Self {
        Self {
            name: name.into(),
            inputs,
            outputs,
            effect,
            permission,
            placement,
            limits: ProcedureLimits::analytics_default(),
        }
    }

    #[must_use]
    pub const fn with_limits(mut self, limits: ProcedureLimits) -> Self {
        self.limits = limits;
        self
    }
}

impl ProcedureInput {
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
    #[must_use]
    pub const fn value_type(&self) -> &ValueType {
        &self.value_type
    }
    #[must_use]
    pub const fn nullable(&self) -> bool {
        self.nullable
    }
    #[must_use]
    pub const fn default(&self) -> Option<&AlgorithmValue> {
        self.default.as_ref()
    }
    #[must_use]
    pub const fn required(&self) -> bool {
        self.default.is_none() && !self.nullable
    }
    #[must_use]
    pub const fn algorithm_type(&self) -> Option<AlgorithmType> {
        self.algorithm_type
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProcedureDescriptor {
    identity: ProcedureIdentity,
    name: String,
    argument_style: ProcedureArgumentStyle,
    inputs: Vec<ProcedureInput>,
    output: RowSchema,
    effect: ProcedureEffect,
    deterministic: bool,
    permission: ProcedurePermission,
    placement: ProcedurePlacement,
    profiles: Vec<CypherProfile>,
    limits: ProcedureLimits,
    supports_overlay: bool,
    algorithm: Option<AlgorithmDescriptor>,
}

impl ProcedureDescriptor {
    #[must_use]
    pub const fn identity(&self) -> &ProcedureIdentity {
        &self.identity
    }
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
    #[must_use]
    pub const fn argument_style(&self) -> ProcedureArgumentStyle {
        self.argument_style
    }
    #[must_use]
    pub fn inputs(&self) -> &[ProcedureInput] {
        &self.inputs
    }
    #[must_use]
    pub const fn output(&self) -> &RowSchema {
        &self.output
    }
    #[must_use]
    pub const fn effect(&self) -> ProcedureEffect {
        self.effect
    }
    #[must_use]
    pub const fn deterministic(&self) -> bool {
        self.deterministic
    }
    #[must_use]
    pub const fn permission(&self) -> ProcedurePermission {
        self.permission
    }
    #[must_use]
    pub const fn placement(&self) -> ProcedurePlacement {
        self.placement
    }
    #[must_use]
    pub fn supports_profile(&self, profile: CypherProfile) -> bool {
        self.profiles.contains(&profile)
    }
    #[must_use]
    pub const fn limits(&self) -> ProcedureLimits {
        self.limits
    }
    #[must_use]
    pub const fn supports_overlay(&self) -> bool {
        self.supports_overlay
    }
    #[must_use]
    pub const fn algorithm(&self) -> Option<&AlgorithmDescriptor> {
        self.algorithm.as_ref()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProcedureCatalog {
    revision: u64,
    descriptors: BTreeMap<String, ProcedureDescriptor>,
}

impl ProcedureCatalog {
    pub fn builtin_analytics() -> Result<Self, ProcedureError> {
        let algorithms = builtin_algorithm_descriptors();
        let revision = catalog_revision(&algorithms);
        let mut descriptors = BTreeMap::new();
        for algorithm in algorithms {
            let procedure_revision = procedure_revision(&algorithm);
            let authority_key = authority_key(&algorithm, revision, procedure_revision);
            let output = RowSchema::new(
                algorithm
                    .outputs()
                    .iter()
                    .enumerate()
                    .map(|(index, field)| {
                        Ok(Column::new(
                            SlotId::new(
                                u32::try_from(index).map_err(|_| ProcedureError::InvalidCatalog)?,
                            ),
                            field.name(),
                            value_type(field.value_type()),
                            field.nullable(),
                        ))
                    })
                    .collect::<Result<Vec<_>, ProcedureError>>()?,
            )
            .map_err(|_| ProcedureError::InvalidCatalog)?;
            let inputs = algorithm.inputs().iter().map(input).collect();
            let name = algorithm.name().to_owned();
            let descriptor = ProcedureDescriptor {
                identity: ProcedureIdentity::new(authority_key, revision, procedure_revision),
                name: name.clone(),
                argument_style: ProcedureArgumentStyle::MapOrPositional,
                inputs,
                output,
                effect: ProcedureEffect::ReadOnly,
                deterministic: algorithm.deterministic(),
                permission: ProcedurePermission::AnalyticsRead,
                placement: ProcedurePlacement::Coordinator,
                profiles: vec![CypherProfile::cypher_25()],
                limits: ProcedureLimits::analytics_default(),
                supports_overlay: algorithm.graph_models() == [GraphModel::Snapshot],
                algorithm: Some(algorithm),
            };
            if descriptors
                .insert(name.to_ascii_lowercase(), descriptor)
                .is_some()
            {
                return Err(ProcedureError::InvalidCatalog);
            }
        }
        if descriptors.is_empty() {
            return Err(ProcedureError::InvalidCatalog);
        }
        job_procedures::with_job_descriptors(Self {
            revision,
            descriptors,
        })
    }

    pub fn from_definitions(definitions: Vec<ProcedureDefinition>) -> Result<Self, ProcedureError> {
        if definitions.is_empty() {
            return Err(ProcedureError::InvalidCatalog);
        }
        let mut names = BTreeSet::new();
        for definition in &definitions {
            validate_definition(definition)?;
            if !names.insert(definition.name.to_ascii_lowercase()) {
                return Err(ProcedureError::InvalidCatalog);
            }
        }
        let revision = definition_catalog_revision(&definitions);
        let mut descriptors = BTreeMap::new();
        for definition in definitions {
            let procedure_revision = definition_revision(&definition);
            let authority_key =
                definition_authority_key(&definition.name, revision, procedure_revision);
            let output = schema_from_fields(&definition.outputs)?;
            let inputs = definition
                .inputs
                .iter()
                .map(|field| ProcedureInput {
                    name: field.name.clone(),
                    value_type: field.value_type.clone(),
                    nullable: field.nullable,
                    default: field.default.clone(),
                    algorithm_type: None,
                })
                .collect();
            let name = definition.name;
            let descriptor = ProcedureDescriptor {
                identity: ProcedureIdentity::new(authority_key, revision, procedure_revision),
                name: name.clone(),
                argument_style: ProcedureArgumentStyle::MapOrPositional,
                inputs,
                output,
                effect: definition.effect,
                deterministic: true,
                permission: definition.permission,
                placement: definition.placement,
                profiles: vec![CypherProfile::cypher_25()],
                limits: definition.limits,
                supports_overlay: false,
                algorithm: None,
            };
            if descriptors
                .insert(name.to_ascii_lowercase(), descriptor)
                .is_some()
            {
                return Err(ProcedureError::InvalidCatalog);
            }
        }
        Ok(Self {
            revision,
            descriptors,
        })
    }

    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    #[must_use]
    pub fn resolve(&self, name: &str) -> Option<&ProcedureDescriptor> {
        self.descriptors.get(&name.to_ascii_lowercase())
    }

    #[must_use]
    pub fn resolve_by_identity(
        &self,
        identity: &ProcedureIdentity,
    ) -> Option<&ProcedureDescriptor> {
        self.descriptors
            .values()
            .find(|descriptor| descriptor.identity() == identity)
    }

    pub fn validate_identity(&self, identity: &ProcedureIdentity) -> Result<(), ProcedureError> {
        if identity.catalog_revision() != self.revision {
            return Err(ProcedureError::CatalogRevisionMismatch);
        }
        if self
            .descriptors
            .values()
            .any(|descriptor| descriptor.identity() == identity)
        {
            Ok(())
        } else {
            Err(ProcedureError::UnknownIdentity)
        }
    }
}

fn validate_definition(definition: &ProcedureDefinition) -> Result<(), ProcedureError> {
    if definition.name.is_empty() || definition.outputs.is_empty() {
        return Err(ProcedureError::InvalidCatalog);
    }
    validate_definition_fields(&definition.inputs)?;
    validate_definition_fields(&definition.outputs)
}

fn validate_definition_fields(fields: &[ProcedureField]) -> Result<(), ProcedureError> {
    let mut names = BTreeSet::new();
    for field in fields {
        if field.name.is_empty() || !names.insert(field.name.as_str()) {
            return Err(ProcedureError::InvalidCatalog);
        }
        if let Some(default) = &field.default {
            let value = procedure_algorithm_value(default.clone())
                .map_err(|_| ProcedureError::InvalidCatalog)?;
            if !procedure_value_matches(&value, &field.value_type, field.nullable) {
                return Err(ProcedureError::InvalidCatalog);
            }
        }
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProcedureError {
    InvalidCatalog,
    InvalidLimits,
    CatalogRevisionMismatch,
    UnknownIdentity,
    ProviderAlreadyRegistered,
    ProviderMissing,
    PermissionDenied,
    InvalidSecurityFingerprint,
    UnknownArgument,
    MissingArgument,
    ArgumentTypeMismatch,
    Provider(String),
    ProviderSchemaMismatch,
    ProviderTypeMismatch,
    OutputRowLimit,
    ValueByteLimit,
    ResultByteLimit,
    SizeOverflow,
    ProviderWorker,
}

impl Display for ProcedureError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "procedure runtime failed: {self:?}")
    }
}

impl Error for ProcedureError {}

impl ProcedureError {
    #[must_use]
    pub fn code(&self) -> &str {
        match self {
            Self::InvalidCatalog => "DTG-PROCEDURE-CATALOG",
            Self::InvalidLimits => "DTG-PROCEDURE-LIMITS",
            Self::CatalogRevisionMismatch => "DTG-PROCEDURE-CATALOG-REVISION",
            Self::UnknownIdentity => "DTG-PROCEDURE-IDENTITY",
            Self::ProviderAlreadyRegistered => "DTG-PROCEDURE-PROVIDER-DUPLICATE",
            Self::ProviderMissing => "DTG-PROCEDURE-PROVIDER-MISSING",
            Self::PermissionDenied => "DTG-PROCEDURE-PERMISSION",
            Self::InvalidSecurityFingerprint => "DTG-PROCEDURE-SECURITY",
            Self::UnknownArgument => "DTG-PROCEDURE-ARGUMENT-UNKNOWN",
            Self::MissingArgument => "DTG-PROCEDURE-ARGUMENT-MISSING",
            Self::ArgumentTypeMismatch => "DTG-PROCEDURE-ARGUMENT-TYPE",
            Self::Provider(detail) => provider_stable_code(detail),
            Self::ProviderSchemaMismatch => "DTG-PROCEDURE-PROVIDER-SCHEMA",
            Self::ProviderTypeMismatch => "DTG-PROCEDURE-PROVIDER-TYPE",
            Self::OutputRowLimit => "DTG-PROCEDURE-OUTPUT-ROWS",
            Self::ValueByteLimit => "DTG-PROCEDURE-VALUE-BYTES",
            Self::ResultByteLimit => "DTG-PROCEDURE-RESULT-BYTES",
            Self::SizeOverflow => "DTG-PROCEDURE-SIZE",
            Self::ProviderWorker => "DTG-PROCEDURE-WORKER",
        }
    }
}

fn provider_stable_code(detail: &str) -> &str {
    let candidate = detail
        .split_once(':')
        .map_or(detail, |(code, _)| code)
        .trim();
    if candidate.starts_with("DTG-")
        && candidate
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'-')
    {
        candidate
    } else {
        "DTG-PROCEDURE-PROVIDER"
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProcedureValue {
    Null,
    Boolean(bool),
    Integer(i64),
    FloatBits(u64),
    String(String),
    Bytes(Vec<u8>),
    TimestampMicros(i64),
    List(Vec<Self>),
    Map(BTreeMap<u32, Self>),
}

impl ProcedureValue {
    pub fn estimated_bytes(&self) -> Result<u64, ProcedureError> {
        match self {
            Self::Null => Ok(1),
            Self::Boolean(_) => Ok(2),
            Self::Integer(_) | Self::FloatBits(_) | Self::TimestampMicros(_) => Ok(9),
            Self::String(value) => 5_u64
                .checked_add(u64::try_from(value.len()).map_err(|_| ProcedureError::SizeOverflow)?)
                .ok_or(ProcedureError::SizeOverflow),
            Self::Bytes(value) => 5_u64
                .checked_add(u64::try_from(value.len()).map_err(|_| ProcedureError::SizeOverflow)?)
                .ok_or(ProcedureError::SizeOverflow),
            Self::List(values) => values.iter().try_fold(5_u64, |total, value| {
                total
                    .checked_add(value.estimated_bytes()?)
                    .ok_or(ProcedureError::SizeOverflow)
            }),
            Self::Map(values) => values.values().try_fold(5_u64, |total, value| {
                total
                    .checked_add(4)
                    .and_then(|total| total.checked_add(value.estimated_bytes().ok()?))
                    .ok_or(ProcedureError::SizeOverflow)
            }),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProcedureResult {
    columns: Vec<String>,
    rows: Vec<Vec<ProcedureValue>>,
}

impl ProcedureResult {
    #[must_use]
    pub const fn new(columns: Vec<String>, rows: Vec<Vec<ProcedureValue>>) -> Self {
        Self { columns, rows }
    }

    #[must_use]
    pub fn columns(&self) -> &[String] {
        &self.columns
    }
    #[must_use]
    pub fn rows(&self) -> &[Vec<ProcedureValue>] {
        &self.rows
    }
}

pub struct ProcedureOutput {
    expected: RowSchema,
    limits: ProcedureLimits,
    columns: Option<Vec<String>>,
    rows: Vec<Vec<ProcedureValue>>,
    result_bytes: u64,
}

impl ProcedureOutput {
    fn new(expected: RowSchema, limits: ProcedureLimits) -> Self {
        Self {
            expected,
            limits,
            columns: None,
            rows: Vec::new(),
            result_bytes: 0,
        }
    }

    pub fn declare_columns(&mut self, columns: Vec<String>) -> Result<(), ProcedureError> {
        if self.columns.is_some()
            || columns.len() != self.expected.columns().len()
            || columns
                .iter()
                .zip(self.expected.columns())
                .any(|(actual, expected)| actual != expected.name())
        {
            return Err(ProcedureError::ProviderSchemaMismatch);
        }
        self.columns = Some(columns);
        Ok(())
    }

    pub fn push_row(&mut self, row: Vec<ProcedureValue>) -> Result<(), ProcedureError> {
        let columns = self
            .columns
            .as_ref()
            .ok_or(ProcedureError::ProviderSchemaMismatch)?;
        if row.len() != columns.len() {
            return Err(ProcedureError::ProviderSchemaMismatch);
        }
        let next_rows = self
            .rows
            .len()
            .checked_add(1)
            .ok_or(ProcedureError::SizeOverflow)?;
        if u64::try_from(next_rows).map_err(|_| ProcedureError::SizeOverflow)?
            > self.limits.max_output_rows()
        {
            return Err(ProcedureError::OutputRowLimit);
        }
        let mut next_result_bytes = self.result_bytes;
        for (value, column) in row.iter().zip(self.expected.columns()) {
            if !procedure_value_matches(value, column.value_type(), column.nullable()) {
                return Err(ProcedureError::ProviderTypeMismatch);
            }
            let value_bytes = value.estimated_bytes()?;
            if value_bytes > self.limits.max_value_bytes() {
                return Err(ProcedureError::ValueByteLimit);
            }
            next_result_bytes = next_result_bytes
                .checked_add(value_bytes)
                .ok_or(ProcedureError::SizeOverflow)?;
            if next_result_bytes > self.limits.max_result_bytes() {
                return Err(ProcedureError::ResultByteLimit);
            }
        }
        self.result_bytes = next_result_bytes;
        self.rows.push(row);
        Ok(())
    }

    fn finish(self) -> Result<ProcedureResult, ProcedureError> {
        let columns = self.columns.ok_or(ProcedureError::ProviderSchemaMismatch)?;
        Ok(ProcedureResult::new(columns, self.rows))
    }
}

pub struct ProcedureInvocation {
    identity: ProcedureIdentity,
    arguments: BTreeMap<String, ProcedureValue>,
    graph: Option<Arc<ProjectedGraph>>,
    security_fingerprint: [u8; 32],
    access: ProcedureAccess,
    job_context: Option<JobInvocationContext>,
}

impl ProcedureInvocation {
    #[must_use]
    pub fn new(
        identity: ProcedureIdentity,
        arguments: BTreeMap<String, ProcedureValue>,
        graph: Option<Arc<ProjectedGraph>>,
        security_fingerprint: [u8; 32],
        access: &ProcedureAccess,
    ) -> Self {
        Self {
            identity,
            arguments,
            graph,
            security_fingerprint,
            access: access.clone(),
            job_context: None,
        }
    }

    #[must_use]
    pub fn with_job_context(mut self, context: JobInvocationContext) -> Self {
        self.job_context = Some(context);
        self
    }

    #[must_use]
    pub const fn identity(&self) -> ProcedureIdentity {
        self.identity
    }
    #[must_use]
    pub const fn arguments(&self) -> &BTreeMap<String, ProcedureValue> {
        &self.arguments
    }
    #[must_use]
    pub fn graph(&self) -> Option<&ProjectedGraph> {
        self.graph.as_deref()
    }
    #[must_use]
    pub fn shared_graph(&self) -> Option<Arc<ProjectedGraph>> {
        self.graph.clone()
    }
    #[must_use]
    pub const fn security_fingerprint(&self) -> [u8; 32] {
        self.security_fingerprint
    }

    #[must_use]
    pub const fn job_context(&self) -> Option<&JobInvocationContext> {
        self.job_context.as_ref()
    }
}

pub trait ProcedureProvider: Send + Sync {
    fn invoke(
        &self,
        invocation: &ProcedureInvocation,
        output: &mut ProcedureOutput,
    ) -> Result<(), ProcedureError>;
}

pub struct ProcedureRegistry {
    catalog: ProcedureCatalog,
    providers: BTreeMap<[u8; 32], Arc<dyn ProcedureProvider>>,
    workers: Arc<tokio::sync::Semaphore>,
}

impl Debug for ProcedureRegistry {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProcedureRegistry")
            .field("catalog", &self.catalog)
            .field("provider_count", &self.providers.len())
            .field("available_workers", &self.workers.available_permits())
            .finish()
    }
}

impl ProcedureRegistry {
    #[must_use]
    pub fn new(catalog: ProcedureCatalog) -> Self {
        Self {
            catalog,
            providers: BTreeMap::new(),
            workers: Arc::new(tokio::sync::Semaphore::new(64)),
        }
    }

    pub fn with_worker_limit(mut self, max_workers: usize) -> Result<Self, ProcedureError> {
        if max_workers == 0 {
            return Err(ProcedureError::InvalidLimits);
        }
        self.workers = Arc::new(tokio::sync::Semaphore::new(max_workers));
        Ok(self)
    }

    pub fn builtin_analytics(
        provider: Arc<dyn AnalyticsProvider>,
        coordinator: Arc<dyn ClusterAnalyticsCoordinator>,
    ) -> Result<Self, ProcedureError> {
        let catalog = ProcedureCatalog::builtin_analytics()?;
        let catalog_algorithms = catalog
            .descriptors
            .values()
            .filter_map(|descriptor| descriptor.algorithm().cloned())
            .map(|descriptor| (descriptor.name().to_owned(), descriptor))
            .collect::<BTreeMap<_, _>>();
        let provider_algorithms = provider
            .algorithms()
            .into_iter()
            .map(|descriptor| (descriptor.name().to_owned(), descriptor))
            .collect::<BTreeMap<_, _>>();
        if catalog_algorithms != provider_algorithms {
            return Err(ProcedureError::InvalidCatalog);
        }
        let bindings = catalog
            .descriptors
            .values()
            .filter_map(|descriptor| {
                descriptor
                    .algorithm()
                    .cloned()
                    .map(|algorithm| (*descriptor.identity(), algorithm))
            })
            .collect::<Vec<_>>();
        let mut registry = Self::new(catalog);
        for (identity, algorithm) in bindings {
            registry.register(
                identity,
                Arc::new(AnalyticsProcedureProvider {
                    provider: Arc::clone(&provider),
                    algorithm,
                }),
            )?;
        }
        job_procedures::register_job_providers(&mut registry, provider, coordinator)?;
        Ok(registry)
    }

    #[must_use]
    pub const fn catalog(&self) -> &ProcedureCatalog {
        &self.catalog
    }

    pub fn register(
        &mut self,
        identity: ProcedureIdentity,
        provider: Arc<dyn ProcedureProvider>,
    ) -> Result<(), ProcedureError> {
        self.catalog.validate_identity(&identity)?;
        if self
            .providers
            .insert(identity.authority_key(), provider)
            .is_some()
        {
            return Err(ProcedureError::ProviderAlreadyRegistered);
        }
        Ok(())
    }

    pub async fn invoke(
        &self,
        mut invocation: ProcedureInvocation,
    ) -> Result<ProcedureResult, ProcedureError> {
        invocation.arguments = self.preflight(
            invocation.identity,
            std::mem::take(&mut invocation.arguments),
            invocation.security_fingerprint,
            &invocation.access,
        )?;
        self.catalog.validate_identity(&invocation.identity)?;
        let descriptor = self
            .catalog
            .descriptors
            .values()
            .find(|descriptor| descriptor.identity() == &invocation.identity)
            .cloned()
            .ok_or(ProcedureError::UnknownIdentity)?;
        let provider = self
            .providers
            .get(&invocation.identity.authority_key())
            .cloned()
            .ok_or(ProcedureError::ProviderMissing)?;
        let permit = Arc::clone(&self.workers)
            .acquire_owned()
            .await
            .map_err(|_| ProcedureError::ProviderWorker)?;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let mut output = ProcedureOutput::new(descriptor.output().clone(), descriptor.limits());
            provider.invoke(&invocation, &mut output)?;
            output.finish()
        })
        .await
        .map_err(|_| ProcedureError::ProviderWorker)?
    }

    pub fn preflight(
        &self,
        identity: ProcedureIdentity,
        arguments: BTreeMap<String, ProcedureValue>,
        security_fingerprint: [u8; 32],
        access: &ProcedureAccess,
    ) -> Result<BTreeMap<String, ProcedureValue>, ProcedureError> {
        self.preflight_partial(
            identity,
            arguments,
            &BTreeSet::new(),
            security_fingerprint,
            access,
        )
    }

    pub fn preflight_partial(
        &self,
        identity: ProcedureIdentity,
        mut arguments: BTreeMap<String, ProcedureValue>,
        deferred: &BTreeSet<String>,
        security_fingerprint: [u8; 32],
        access: &ProcedureAccess,
    ) -> Result<BTreeMap<String, ProcedureValue>, ProcedureError> {
        self.catalog.validate_identity(&identity)?;
        let descriptor = self
            .catalog
            .descriptors
            .values()
            .find(|descriptor| descriptor.identity() == &identity)
            .ok_or(ProcedureError::UnknownIdentity)?;
        if security_fingerprint == [0; 32] {
            return Err(ProcedureError::InvalidSecurityFingerprint);
        }
        if !access.allows(descriptor.permission()) {
            return Err(ProcedureError::PermissionDenied);
        }
        if deferred
            .iter()
            .any(|name| !descriptor.inputs().iter().any(|input| input.name() == name))
        {
            return Err(ProcedureError::UnknownArgument);
        }
        for input in descriptor.inputs() {
            if arguments.contains_key(input.name()) || deferred.contains(input.name()) {
                continue;
            }
            if let Some(default) = input.default() {
                arguments.insert(
                    input.name().to_owned(),
                    procedure_algorithm_value(default.clone())?,
                );
            }
        }
        validate_runtime_arguments(descriptor, &arguments, deferred)?;
        Ok(arguments)
    }
}

struct AnalyticsProcedureProvider {
    provider: Arc<dyn AnalyticsProvider>,
    algorithm: AlgorithmDescriptor,
}

impl ProcedureProvider for AnalyticsProcedureProvider {
    fn invoke(
        &self,
        invocation: &ProcedureInvocation,
        output: &mut ProcedureOutput,
    ) -> Result<(), ProcedureError> {
        let graph = invocation
            .shared_graph()
            .ok_or_else(|| ProcedureError::Provider("DTG-ANALYTICS-GRAPH-MISSING".into()))?;
        let mut parameters = BTreeMap::new();
        for input in self.algorithm.inputs() {
            let Some(value) = invocation.arguments().get(input.name()) else {
                continue;
            };
            parameters.insert(
                input.name().to_owned(),
                algorithm_value(input.value_type(), value.clone())?,
            );
        }
        let request = AlgorithmRequest::from_shared(self.algorithm.name(), graph, parameters)
            .map_err(|error| ProcedureError::Provider(error.code().to_owned()))?;
        let mut bridge = ProcedureAnalyticsOutput {
            output,
            procedure_error: None,
        };
        let result = self.provider.execute_into(request, &mut bridge);
        if let Some(error) = bridge.procedure_error {
            return Err(error);
        }
        result.map_err(|error| ProcedureError::Provider(error.code().to_owned()))
    }
}

struct ProcedureAnalyticsOutput<'a> {
    output: &'a mut ProcedureOutput,
    procedure_error: Option<ProcedureError>,
}

impl ProcedureAnalyticsOutput<'_> {
    fn fail(&mut self, error: ProcedureError) -> ProviderError {
        let code = error.code().to_owned();
        self.procedure_error = Some(error);
        ProviderError::new(code, "procedure output rejected analytics result")
    }
}

impl AnalyticsOutput for ProcedureAnalyticsOutput<'_> {
    fn declare_columns(&mut self, columns: Vec<String>) -> Result<(), ProviderError> {
        self.output
            .declare_columns(columns)
            .map_err(|error| self.fail(error))
    }

    fn push_row(&mut self, row: Vec<AlgorithmValue>) -> Result<(), ProviderError> {
        let row = row
            .into_iter()
            .map(procedure_algorithm_value)
            .collect::<Result<Vec<_>, ProcedureError>>()
            .map_err(|error| self.fail(error))?;
        self.output.push_row(row).map_err(|error| self.fail(error))
    }
}

fn algorithm_value(
    expected: AlgorithmType,
    value: ProcedureValue,
) -> Result<AlgorithmValue, ProcedureError> {
    match (expected, value) {
        (_, ProcedureValue::Null) => Ok(AlgorithmValue::Null),
        (AlgorithmType::Boolean, ProcedureValue::Boolean(value)) => {
            Ok(AlgorithmValue::Boolean(value))
        }
        (AlgorithmType::Integer, ProcedureValue::Integer(value)) => {
            Ok(AlgorithmValue::Integer(value))
        }
        (AlgorithmType::Float, ProcedureValue::FloatBits(value)) => {
            Ok(AlgorithmValue::FloatBits(value))
        }
        (AlgorithmType::Float, ProcedureValue::Integer(value)) => {
            Ok(AlgorithmValue::FloatBits((value as f64).to_bits()))
        }
        (AlgorithmType::String, ProcedureValue::String(value)) => Ok(AlgorithmValue::String(value)),
        (AlgorithmType::Vertex, ProcedureValue::Integer(value)) if value >= 0 => Ok(
            AlgorithmValue::Vertex(analytics_api::VertexId::new(value as u128)),
        ),
        (AlgorithmType::Vertex, ProcedureValue::String(value)) => value
            .parse::<u128>()
            .map(analytics_api::VertexId::new)
            .map(AlgorithmValue::Vertex)
            .map_err(|_| ProcedureError::ArgumentTypeMismatch),
        (AlgorithmType::Time, ProcedureValue::TimestampMicros(value))
        | (AlgorithmType::Time, ProcedureValue::Integer(value)) => Ok(AlgorithmValue::Time(
            temporal_types::ValidTime::from_micros(value),
        )),
        _ => Err(ProcedureError::ArgumentTypeMismatch),
    }
}

fn procedure_algorithm_value(value: AlgorithmValue) -> Result<ProcedureValue, ProcedureError> {
    Ok(match value {
        AlgorithmValue::Null => ProcedureValue::Null,
        AlgorithmValue::Boolean(value) => ProcedureValue::Boolean(value),
        AlgorithmValue::Integer(value) => ProcedureValue::Integer(value),
        AlgorithmValue::FloatBits(value) => ProcedureValue::FloatBits(value),
        AlgorithmValue::String(value) => ProcedureValue::String(value),
        AlgorithmValue::Vertex(value) => ProcedureValue::String(value.value().to_string()),
        AlgorithmValue::Time(value) => ProcedureValue::TimestampMicros(value.as_micros()),
    })
}

fn validate_runtime_arguments(
    descriptor: &ProcedureDescriptor,
    arguments: &BTreeMap<String, ProcedureValue>,
    deferred: &BTreeSet<String>,
) -> Result<(), ProcedureError> {
    let limits = descriptor.limits();
    let mut input_bytes = 0_u64;
    for (name, value) in arguments {
        let input = descriptor
            .inputs()
            .iter()
            .find(|input| input.name() == name)
            .ok_or(ProcedureError::UnknownArgument)?;
        if !procedure_input_matches(value, input) {
            return Err(ProcedureError::ArgumentTypeMismatch);
        }
        let value_bytes = value.estimated_bytes()?;
        if value_bytes > limits.max_value_bytes() {
            return Err(ProcedureError::ValueByteLimit);
        }
        input_bytes = input_bytes
            .checked_add(value_bytes)
            .ok_or(ProcedureError::SizeOverflow)?;
        if input_bytes > limits.max_result_bytes() {
            return Err(ProcedureError::ResultByteLimit);
        }
    }
    if descriptor.inputs().iter().any(|input| {
        input.required()
            && !arguments.contains_key(input.name())
            && !deferred.contains(input.name())
    }) {
        return Err(ProcedureError::MissingArgument);
    }
    Ok(())
}

fn procedure_input_matches(value: &ProcedureValue, input: &ProcedureInput) -> bool {
    match input.algorithm_type() {
        Some(AlgorithmType::Vertex) => {
            matches!(
                value,
                ProcedureValue::Integer(value) if *value >= 0
            ) || matches!(value, ProcedureValue::String(_))
        }
        Some(AlgorithmType::Time) => matches!(
            value,
            ProcedureValue::Integer(_) | ProcedureValue::TimestampMicros(_)
        ),
        Some(AlgorithmType::Float) => matches!(
            value,
            ProcedureValue::Integer(_) | ProcedureValue::FloatBits(_)
        ),
        _ => procedure_value_matches(value, input.value_type(), input.nullable()),
    }
}

fn procedure_value_matches(value: &ProcedureValue, expected: &ValueType, nullable: bool) -> bool {
    match value {
        ProcedureValue::Null => nullable,
        _ if expected == &ValueType::Any => true,
        ProcedureValue::Boolean(_) => expected == &ValueType::Boolean,
        ProcedureValue::Integer(_) => expected == &ValueType::Integer,
        ProcedureValue::FloatBits(_) => expected == &ValueType::Float,
        ProcedureValue::String(_) => expected == &ValueType::String,
        ProcedureValue::Bytes(_) => expected == &ValueType::Bytes,
        ProcedureValue::TimestampMicros(_) => expected == &ValueType::Temporal,
        ProcedureValue::List(values) => match expected {
            ValueType::List(item) => values
                .iter()
                .all(|value| procedure_value_matches(value, item, true)),
            _ => false,
        },
        ProcedureValue::Map(_) => expected == &ValueType::Map,
    }
}

fn input(field: &AlgorithmField) -> ProcedureInput {
    ProcedureInput {
        name: field.name().to_owned(),
        value_type: value_type(field.value_type()),
        nullable: field.nullable(),
        default: field.default().cloned(),
        algorithm_type: Some(field.value_type()),
    }
}

fn schema_from_fields(fields: &[ProcedureField]) -> Result<RowSchema, ProcedureError> {
    RowSchema::new(
        fields
            .iter()
            .enumerate()
            .map(|(index, field)| {
                Ok(Column::new(
                    SlotId::new(u32::try_from(index).map_err(|_| ProcedureError::InvalidCatalog)?),
                    &field.name,
                    field.value_type.clone(),
                    field.nullable,
                ))
            })
            .collect::<Result<Vec<_>, ProcedureError>>()?,
    )
    .map_err(|_| ProcedureError::InvalidCatalog)
}

fn value_type(value: AlgorithmType) -> ValueType {
    match value {
        AlgorithmType::Boolean => ValueType::Boolean,
        AlgorithmType::Integer => ValueType::Integer,
        AlgorithmType::Float => ValueType::Float,
        AlgorithmType::String | AlgorithmType::Vertex => ValueType::String,
        AlgorithmType::Time => ValueType::Temporal,
    }
}

fn catalog_revision(algorithms: &[AlgorithmDescriptor]) -> u64 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"DTGProxy/ProcedureCatalog/Latest");
    for algorithm in algorithms {
        hash_algorithm(&mut hasher, algorithm);
    }
    nonzero_revision(hasher.finalize().as_bytes())
}

fn procedure_revision(algorithm: &AlgorithmDescriptor) -> u64 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"DTGProxy/ProcedureDescriptor/Latest");
    hash_algorithm(&mut hasher, algorithm);
    nonzero_revision(hasher.finalize().as_bytes())
}

fn authority_key(
    algorithm: &AlgorithmDescriptor,
    catalog_revision: u64,
    procedure_revision: u64,
) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"DTGProxy/ProcedureAuthority/Latest");
    hasher.update(&catalog_revision.to_be_bytes());
    hasher.update(&procedure_revision.to_be_bytes());
    hasher.update(algorithm.name().to_ascii_lowercase().as_bytes());
    *hasher.finalize().as_bytes()
}

fn definition_catalog_revision(definitions: &[ProcedureDefinition]) -> u64 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"DTGProxy/ProcedureCatalog/Latest");
    for definition in definitions {
        hash_definition(&mut hasher, definition);
    }
    nonzero_revision(hasher.finalize().as_bytes())
}

fn definition_revision(definition: &ProcedureDefinition) -> u64 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"DTGProxy/ProcedureDescriptor/Latest");
    hash_definition(&mut hasher, definition);
    nonzero_revision(hasher.finalize().as_bytes())
}

fn definition_authority_key(
    name: &str,
    catalog_revision: u64,
    procedure_revision: u64,
) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"DTGProxy/ProcedureAuthority/Latest");
    hasher.update(&catalog_revision.to_be_bytes());
    hasher.update(&procedure_revision.to_be_bytes());
    hasher.update(name.to_ascii_lowercase().as_bytes());
    *hasher.finalize().as_bytes()
}

fn hash_definition(hasher: &mut blake3::Hasher, definition: &ProcedureDefinition) {
    hash_bytes(hasher, definition.name.as_bytes());
    hasher.update(&[
        definition.effect as u8,
        definition.permission as u8,
        definition.placement as u8,
    ]);
    hasher.update(&definition.limits.max_invocations().to_be_bytes());
    hasher.update(&definition.limits.max_input_rows().to_be_bytes());
    hasher.update(&definition.limits.max_output_rows().to_be_bytes());
    hasher.update(&definition.limits.max_value_bytes().to_be_bytes());
    hasher.update(&definition.limits.max_result_bytes().to_be_bytes());
    hasher.update(&definition.limits.max_graph_vertices().to_be_bytes());
    hasher.update(&definition.limits.max_graph_edges().to_be_bytes());
    hasher.update(&definition.limits.max_graph_bytes().to_be_bytes());
    hash_procedure_fields(hasher, &definition.inputs);
    hash_procedure_fields(hasher, &definition.outputs);
}

fn hash_algorithm(hasher: &mut blake3::Hasher, algorithm: &AlgorithmDescriptor) {
    hash_bytes(hasher, algorithm.name().as_bytes());
    hash_bytes(hasher, algorithm.version().as_bytes());
    hash_len(hasher, algorithm.graph_models().len());
    for model in algorithm.graph_models() {
        hasher.update(&[match model {
            GraphModel::Snapshot => 0,
            GraphModel::Event => 1,
            GraphModel::Interval => 2,
            GraphModel::Delta => 3,
        }]);
    }
    hasher.update(&[
        u8::from(algorithm.exact()),
        u8::from(algorithm.deterministic()),
        u8::from(algorithm.distributed()),
        u8::from(algorithm.incremental()),
        ProcedureEffect::ReadOnly as u8,
        ProcedurePermission::AnalyticsRead as u8,
        ProcedurePlacement::Coordinator as u8,
        u8::from(algorithm.graph_models() == [GraphModel::Snapshot]),
    ]);
    let limits = ProcedureLimits::analytics_default();
    hasher.update(&limits.max_invocations().to_be_bytes());
    hasher.update(&limits.max_input_rows().to_be_bytes());
    hasher.update(&limits.max_output_rows().to_be_bytes());
    hasher.update(&limits.max_value_bytes().to_be_bytes());
    hasher.update(&limits.max_result_bytes().to_be_bytes());
    hasher.update(&limits.max_graph_vertices().to_be_bytes());
    hasher.update(&limits.max_graph_edges().to_be_bytes());
    hasher.update(&limits.max_graph_bytes().to_be_bytes());
    hash_algorithm_fields(hasher, algorithm.inputs());
    hash_algorithm_fields(hasher, algorithm.outputs());
}

fn hash_procedure_fields(hasher: &mut blake3::Hasher, fields: &[ProcedureField]) {
    hash_len(hasher, fields.len());
    for field in fields {
        hash_bytes(hasher, field.name.as_bytes());
        hash_value_type(hasher, &field.value_type);
        hasher.update(&[u8::from(field.nullable)]);
        hash_optional_value(hasher, field.default.as_ref());
    }
}

fn hash_algorithm_fields(hasher: &mut blake3::Hasher, fields: &[AlgorithmField]) {
    hash_len(hasher, fields.len());
    for field in fields {
        hash_bytes(hasher, field.name().as_bytes());
        hasher.update(&[field.value_type() as u8, u8::from(field.nullable())]);
        hash_optional_value(hasher, field.default());
    }
}

fn hash_optional_value(hasher: &mut blake3::Hasher, value: Option<&AlgorithmValue>) {
    match value {
        None => {
            hasher.update(&[0]);
        }
        Some(value) => {
            hasher.update(&[1]);
            hash_algorithm_value(hasher, value);
        }
    }
}

fn hash_algorithm_value(hasher: &mut blake3::Hasher, value: &AlgorithmValue) {
    match value {
        AlgorithmValue::Null => {
            hasher.update(&[0]);
        }
        AlgorithmValue::Boolean(value) => {
            hasher.update(&[1, u8::from(*value)]);
        }
        AlgorithmValue::Integer(value) => {
            hasher.update(&[2]);
            hasher.update(&value.to_be_bytes());
        }
        AlgorithmValue::FloatBits(value) => {
            hasher.update(&[3]);
            hasher.update(&value.to_be_bytes());
        }
        AlgorithmValue::String(value) => {
            hasher.update(&[4]);
            hash_bytes(hasher, value.as_bytes());
        }
        AlgorithmValue::Vertex(value) => {
            hasher.update(&[5]);
            hasher.update(&value.value().to_be_bytes());
        }
        AlgorithmValue::Time(value) => {
            hasher.update(&[6]);
            hasher.update(&value.as_micros().to_be_bytes());
        }
    }
}

fn hash_value_type(hasher: &mut blake3::Hasher, value_type: &ValueType) {
    match value_type {
        ValueType::Any => {
            hasher.update(&[0]);
        }
        ValueType::Null => {
            hasher.update(&[1]);
        }
        ValueType::Boolean => {
            hasher.update(&[2]);
        }
        ValueType::Integer => {
            hasher.update(&[3]);
        }
        ValueType::Float => {
            hasher.update(&[4]);
        }
        ValueType::String => {
            hasher.update(&[5]);
        }
        ValueType::Bytes => {
            hasher.update(&[6]);
        }
        ValueType::List(item) => {
            hasher.update(&[7]);
            hash_value_type(hasher, item);
        }
        ValueType::Map => {
            hasher.update(&[8]);
        }
        ValueType::Node => {
            hasher.update(&[9]);
        }
        ValueType::Relationship => {
            hasher.update(&[10]);
        }
        ValueType::Path => {
            hasher.update(&[11]);
        }
        ValueType::Temporal => {
            hasher.update(&[12]);
        }
        ValueType::Spatial => {
            hasher.update(&[13]);
        }
        ValueType::Vector => {
            hasher.update(&[14]);
        }
    }
}

fn hash_bytes(hasher: &mut blake3::Hasher, bytes: &[u8]) {
    hasher.update(&u64::try_from(bytes.len()).unwrap_or(u64::MAX).to_be_bytes());
    hasher.update(bytes);
}

fn hash_len(hasher: &mut blake3::Hasher, len: usize) {
    hasher.update(&u64::try_from(len).unwrap_or(u64::MAX).to_be_bytes());
}

fn nonzero_revision(digest: &[u8; 32]) -> u64 {
    u64::from_be_bytes(digest[..8].try_into().expect("digest contains eight bytes")).max(1)
}
