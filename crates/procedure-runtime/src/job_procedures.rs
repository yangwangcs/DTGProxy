use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use analytics_api::{AlgorithmDescriptor, AlgorithmValue, AnalyticsProvider, ProviderDescriptor};
use analytics_ledger::{
    AnalyticsJobId, GraphProjectionScope, JobState, encode_algorithm_parameters,
};
use cypher_ast::CypherProfile;
use temporal_ir::{ProcedureIdentity, ValueType};

use super::{
    AnalyticsCancelRequest, AnalyticsResultsRequest, AnalyticsStatusRequest,
    AnalyticsSubmitRequest, ClusterAnalyticsCoordinator, ClusterAnalyticsError,
    ProcedureArgumentStyle, ProcedureCatalog, ProcedureDefinition, ProcedureDescriptor,
    ProcedureEffect, ProcedureError, ProcedureField, ProcedureInput, ProcedureInvocation,
    ProcedureOutput, ProcedurePermission, ProcedurePlacement, ProcedureProvider, ProcedureRegistry,
    algorithm_value, definition_authority_key, definition_catalog_revision, definition_revision,
    procedure_algorithm_value, procedure_revision, schema_from_fields, validate_definition,
};

const SUBMIT: &str = "dtg.analytics.submit";
const STATUS: &str = "dtg.analytics.status";
const RESULTS: &str = "dtg.analytics.results";
const CANCEL: &str = "dtg.analytics.cancel";

pub(super) fn with_job_descriptors(
    mut catalog: ProcedureCatalog,
) -> Result<ProcedureCatalog, ProcedureError> {
    let definitions = job_definitions();
    for definition in &definitions {
        validate_definition(definition)?;
        if catalog.resolve(&definition.name).is_some() {
            return Err(ProcedureError::InvalidCatalog);
        }
    }
    let revision = combined_revision(catalog.revision, &definitions);
    for descriptor in catalog.descriptors.values_mut() {
        let algorithm = descriptor
            .algorithm
            .as_ref()
            .ok_or(ProcedureError::InvalidCatalog)?;
        let procedure_revision = procedure_revision(algorithm);
        descriptor.identity = ProcedureIdentity::new(
            super::authority_key(algorithm, revision, procedure_revision),
            revision,
            procedure_revision,
        );
    }
    for definition in definitions {
        let procedure_revision = definition_revision(&definition);
        let identity = ProcedureIdentity::new(
            definition_authority_key(&definition.name, revision, procedure_revision),
            revision,
            procedure_revision,
        );
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
            identity,
            name: name.clone(),
            argument_style: ProcedureArgumentStyle::MapOrPositional,
            inputs,
            output,
            effect: ProcedureEffect::ReadOnly,
            deterministic: false,
            permission: ProcedurePermission::AnalyticsRead,
            placement: ProcedurePlacement::Coordinator,
            profiles: vec![CypherProfile::cypher_25()],
            limits: definition.limits,
            supports_overlay: name == SUBMIT,
            algorithm: None,
        };
        if catalog
            .descriptors
            .insert(name.to_ascii_lowercase(), descriptor)
            .is_some()
        {
            return Err(ProcedureError::InvalidCatalog);
        }
    }
    catalog.revision = revision;
    Ok(catalog)
}

pub(super) fn register_job_providers(
    registry: &mut ProcedureRegistry,
    provider: Arc<dyn AnalyticsProvider>,
    coordinator: Arc<dyn ClusterAnalyticsCoordinator>,
) -> Result<(), ProcedureError> {
    for (name, kind) in [
        (SUBMIT, JobProcedure::Submit),
        (STATUS, JobProcedure::Status),
        (RESULTS, JobProcedure::Results),
        (CANCEL, JobProcedure::Cancel),
    ] {
        let identity = *registry
            .catalog
            .resolve(name)
            .ok_or(ProcedureError::InvalidCatalog)?
            .identity();
        registry.register(
            identity,
            Arc::new(JobProcedureProvider {
                coordinator: Arc::clone(&coordinator),
                provider: Arc::clone(&provider),
                kind,
            }),
        )?;
    }
    Ok(())
}

fn combined_revision(current: u64, definitions: &[ProcedureDefinition]) -> u64 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"DTGProxy/ProcedureCatalog/Latest/AnalyticsJobs");
    hasher.update(&current.to_be_bytes());
    hasher.update(&definition_catalog_revision(definitions).to_be_bytes());
    super::nonzero_revision(hasher.finalize().as_bytes())
}

fn job_definitions() -> Vec<ProcedureDefinition> {
    let string = |name, nullable| ProcedureField::new(name, ValueType::String, nullable);
    let integer = |name, nullable| ProcedureField::new(name, ValueType::Integer, nullable);
    vec![
        ProcedureDefinition::new(
            SUBMIT,
            vec![
                string("algorithm", false),
                ProcedureField::new("parameters", ValueType::Map, false),
            ],
            vec![string("jobId", false)],
            ProcedureEffect::ReadOnly,
            ProcedurePermission::AnalyticsRead,
            ProcedurePlacement::Coordinator,
        ),
        ProcedureDefinition::new(
            STATUS,
            vec![string("jobId", false)],
            vec![
                string("state", false),
                integer("completedUnits", false),
                integer("totalUnits", true),
                string("error", true),
            ],
            ProcedureEffect::ReadOnly,
            ProcedurePermission::AnalyticsRead,
            ProcedurePlacement::Coordinator,
        ),
        ProcedureDefinition::new(
            RESULTS,
            vec![
                string("jobId", false),
                ProcedureField::new("offset", ValueType::Integer, false)
                    .with_default(analytics_api::AlgorithmValue::Integer(0)),
                ProcedureField::new("limit", ValueType::Integer, false)
                    .with_default(analytics_api::AlgorithmValue::Integer(1_000)),
            ],
            vec![
                ProcedureField::new(
                    "columns",
                    ValueType::List(Box::new(ValueType::String)),
                    false,
                ),
                ProcedureField::new("row", ValueType::List(Box::new(ValueType::Any)), false),
                integer("rowIndex", false),
                ProcedureField::new("hasMore", ValueType::Boolean, false),
            ],
            ProcedureEffect::ReadOnly,
            ProcedurePermission::AnalyticsRead,
            ProcedurePlacement::Coordinator,
        ),
        ProcedureDefinition::new(
            CANCEL,
            vec![string("jobId", false)],
            vec![ProcedureField::new("canceled", ValueType::Boolean, false)],
            ProcedureEffect::ReadOnly,
            ProcedurePermission::AnalyticsRead,
            ProcedurePlacement::Coordinator,
        ),
    ]
}

#[derive(Clone, Copy)]
enum JobProcedure {
    Submit,
    Status,
    Results,
    Cancel,
}

struct JobProcedureProvider {
    coordinator: Arc<dyn ClusterAnalyticsCoordinator>,
    provider: Arc<dyn AnalyticsProvider>,
    kind: JobProcedure,
}

impl ProcedureProvider for JobProcedureProvider {
    fn invoke(
        &self,
        invocation: &ProcedureInvocation,
        output: &mut ProcedureOutput,
    ) -> Result<(), ProcedureError> {
        match self.kind {
            JobProcedure::Submit => self.submit(invocation, output),
            JobProcedure::Status => self.status(invocation, output),
            JobProcedure::Results => self.results(invocation, output),
            JobProcedure::Cancel => self.cancel(invocation, output),
        }
    }
}

impl JobProcedureProvider {
    fn submit(
        &self,
        invocation: &ProcedureInvocation,
        output: &mut ProcedureOutput,
    ) -> Result<(), ProcedureError> {
        let algorithm = string_argument(invocation, "algorithm")?;
        let raw_parameters = match invocation.arguments().get("parameters") {
            Some(super::ProcedureValue::Map(parameters)) => parameters.clone(),
            _ => return Err(ProcedureError::ArgumentTypeMismatch),
        };
        let descriptor = self
            .provider
            .algorithms()
            .into_iter()
            .find(|descriptor| descriptor.name() == algorithm)
            .ok_or_else(|| provider_error("DTG-ANALYTICS-UNKNOWN", "unknown algorithm"))?;
        let graph = invocation
            .shared_graph()
            .ok_or_else(|| provider_error("DTG-ANALYTICS-GRAPH-MISSING", "graph is required"))?;
        if !descriptor.graph_models().contains(&graph.model()) {
            return Err(provider_error(
                "DTG-ANALYTICS-GRAPH-MODEL",
                "algorithm does not support this projected graph model",
            ));
        }
        let parameters = typed_parameters(&descriptor, raw_parameters)?;
        let encoded_parameters = encode_algorithm_parameters(&parameters)
            .map_err(|error| provider_error("DTG-ANALYTICS-PARAMETERS", error.to_string()))?;
        let context = invocation.job_context().ok_or_else(|| {
            provider_error(
                "DTG-ANALYTICS-JOB-CONTEXT",
                "analytics submit requires immutable Gateway fences",
            )
        })?;
        if context.projection().graph_model() != graph.model() {
            return Err(provider_error(
                "DTG-ANALYTICS-GRAPH-MODEL",
                "projected graph model differs from the immutable projection scope",
            ));
        }
        let provider = self.provider.descriptor();
        let submission_request_id = submission_request_id(
            invocation,
            context,
            &descriptor,
            &provider,
            &encoded_parameters,
        );
        let request = AnalyticsSubmitRequest::new(
            submission_request_id,
            context.clone(),
            descriptor.name().to_owned(),
            descriptor.version().to_owned(),
            provider.name().to_owned(),
            provider.version().to_owned(),
            encoded_parameters,
            invocation.security_fingerprint(),
        )
        .map_err(map_coordinator_error)?;
        let id = self
            .coordinator
            .submit(request)
            .map_err(map_coordinator_error)?
            .job_id();
        output.declare_columns(vec!["jobId".into()])?;
        output.push_row(vec![super::ProcedureValue::String(id.to_string())])
    }

    fn status(
        &self,
        invocation: &ProcedureInvocation,
        output: &mut ProcedureOutput,
    ) -> Result<(), ProcedureError> {
        let context = job_context(invocation)?;
        let status = self
            .coordinator
            .status(
                AnalyticsStatusRequest::new(
                    context.outer_request_id(),
                    job_id(invocation)?,
                    invocation.security_fingerprint(),
                    context.deadline_unix_ms(),
                )
                .map_err(map_coordinator_error)?,
            )
            .map_err(map_coordinator_error)?;
        output.declare_columns(vec![
            "state".into(),
            "completedUnits".into(),
            "totalUnits".into(),
            "error".into(),
        ])?;
        output.push_row(vec![
            super::ProcedureValue::String(state_name(status.state()).into()),
            integer(status.completed_units())?,
            status
                .total_units()
                .map(integer)
                .transpose()?
                .unwrap_or(super::ProcedureValue::Null),
            status
                .failure()
                .map(|error| super::ProcedureValue::String(error.to_owned()))
                .unwrap_or(super::ProcedureValue::Null),
        ])
    }

    fn results(
        &self,
        invocation: &ProcedureInvocation,
        output: &mut ProcedureOutput,
    ) -> Result<(), ProcedureError> {
        let id = job_id(invocation)?;
        let offset = optional_nonnegative_integer(invocation, "offset", 0)?;
        let limit = optional_nonnegative_integer(invocation, "limit", 1_000)?;
        let context = job_context(invocation)?;
        let result = self
            .coordinator
            .results(
                AnalyticsResultsRequest::new(
                    context.outer_request_id(),
                    id,
                    invocation.security_fingerprint(),
                    context.deadline_unix_ms(),
                    offset,
                    limit,
                )
                .map_err(map_coordinator_error)?,
            )
            .map_err(map_coordinator_error)?;
        let row_count = result.total_rows();
        output.declare_columns(vec![
            "columns".into(),
            "row".into(),
            "rowIndex".into(),
            "hasMore".into(),
        ])?;
        let columns: Vec<super::ProcedureValue> = result
            .columns()
            .iter()
            .cloned()
            .map(super::ProcedureValue::String)
            .collect();
        for (index, row) in result.rows().iter().enumerate() {
            let row_index = offset
                .checked_add(index)
                .ok_or(ProcedureError::SizeOverflow)?;
            let row_index_u64 =
                u64::try_from(row_index).map_err(|_| ProcedureError::SizeOverflow)?;
            output.push_row(vec![
                super::ProcedureValue::List(columns.clone()),
                super::ProcedureValue::List(
                    row.iter()
                        .cloned()
                        .map(procedure_algorithm_value)
                        .collect::<Result<Vec<_>, _>>()?,
                ),
                integer(row_index_u64)?,
                super::ProcedureValue::Boolean(row_index_u64.saturating_add(1) < row_count),
            ])?;
        }
        Ok(())
    }

    fn cancel(
        &self,
        invocation: &ProcedureInvocation,
        output: &mut ProcedureOutput,
    ) -> Result<(), ProcedureError> {
        let context = job_context(invocation)?;
        let canceled = self
            .coordinator
            .cancel(
                AnalyticsCancelRequest::new(
                    context.outer_request_id(),
                    job_id(invocation)?,
                    invocation.security_fingerprint(),
                    context.deadline_unix_ms(),
                )
                .map_err(map_coordinator_error)?,
            )
            .map_err(map_coordinator_error)?
            .canceled();
        output.declare_columns(vec!["canceled".into()])?;
        output.push_row(vec![super::ProcedureValue::Boolean(canceled)])
    }
}

fn typed_parameters(
    descriptor: &AlgorithmDescriptor,
    mut raw: BTreeMap<u32, super::ProcedureValue>,
) -> Result<BTreeMap<String, AlgorithmValue>, ProcedureError> {
    let mut identifiers = BTreeSet::new();
    let mut parameters = BTreeMap::new();
    for field in descriptor.inputs() {
        let identifier = stable_id(field.name());
        if !identifiers.insert(identifier) {
            return Err(provider_error(
                "DTG-ANALYTICS-PARAMETER-COLLISION",
                "algorithm input names have colliding stable identifiers",
            ));
        }
        let value = match raw.remove(&identifier) {
            Some(value) => algorithm_value(field.value_type(), value)?,
            None => field
                .default()
                .cloned()
                .ok_or(ProcedureError::MissingArgument)?,
        };
        parameters.insert(field.name().to_owned(), value);
    }
    if !raw.is_empty() {
        return Err(ProcedureError::UnknownArgument);
    }
    Ok(parameters)
}

fn stable_id(name: &str) -> u32 {
    let digest = blake3::hash(name.as_bytes());
    u32::from_be_bytes(
        digest.as_bytes()[..4]
            .try_into()
            .expect("digest has four bytes"),
    )
}

fn string_argument<'a>(
    invocation: &'a ProcedureInvocation,
    name: &str,
) -> Result<&'a str, ProcedureError> {
    match invocation.arguments().get(name) {
        Some(super::ProcedureValue::String(value)) => Ok(value),
        _ => Err(ProcedureError::ArgumentTypeMismatch),
    }
}

fn job_id(invocation: &ProcedureInvocation) -> Result<AnalyticsJobId, ProcedureError> {
    let value = string_argument(invocation, "jobId")?;
    value.parse().map_err(|_| {
        provider_error(
            "DTG-ANALYTICS-JOB-ID",
            "job id must be 32 lowercase hexadecimal characters encoding a nonzero u128",
        )
    })
}

fn optional_nonnegative_integer(
    invocation: &ProcedureInvocation,
    name: &str,
    default: usize,
) -> Result<usize, ProcedureError> {
    match invocation.arguments().get(name) {
        None => Ok(default),
        Some(super::ProcedureValue::Integer(value)) => usize::try_from(*value).map_err(|_| {
            provider_error(
                "DTG-ANALYTICS-RESULT-PAGE",
                format!("{name} must be non-negative"),
            )
        }),
        _ => Err(ProcedureError::ArgumentTypeMismatch),
    }
}

fn state_name(state: JobState) -> &'static str {
    match state {
        JobState::Queued => "QUEUED",
        JobState::Leased => "LEASED",
        JobState::Running => "RUNNING",
        JobState::Succeeded => "SUCCEEDED",
        JobState::Failed => "FAILED",
        JobState::Canceled => "CANCELED",
    }
}

fn integer(value: u64) -> Result<super::ProcedureValue, ProcedureError> {
    i64::try_from(value)
        .map(super::ProcedureValue::Integer)
        .map_err(|_| ProcedureError::SizeOverflow)
}

fn job_context(
    invocation: &ProcedureInvocation,
) -> Result<&super::JobInvocationContext, ProcedureError> {
    invocation.job_context().ok_or_else(|| {
        provider_error(
            "DTG-ANALYTICS-JOB-CONTEXT",
            "analytics job operation requires immutable Gateway fences",
        )
    })
}

fn map_coordinator_error(error: ClusterAnalyticsError) -> ProcedureError {
    provider_error(error.code(), error.message())
}

fn submission_request_id(
    invocation: &ProcedureInvocation,
    context: &super::JobInvocationContext,
    algorithm: &AlgorithmDescriptor,
    _provider: &ProviderDescriptor,
    parameters: &[u8],
) -> u128 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"DTGProxy/AnalyticsSubmission/Latest");
    hasher.update(&context.outer_request_id().to_be_bytes());
    hasher.update(&context.graph_id().to_be_bytes());
    hash_projection(&mut hasher, context.projection());
    let algorithm_name = algorithm.name();
    hasher.update(&(algorithm_name.len() as u64).to_be_bytes());
    hasher.update(algorithm_name.as_bytes());
    hasher.update(&(parameters.len() as u64).to_be_bytes());
    hasher.update(parameters);
    hasher.update(&invocation.security_fingerprint());
    u128::from_be_bytes(
        hasher.finalize().as_bytes()[..16]
            .try_into()
            .expect("BLAKE3 digest has sixteen leading bytes"),
    )
    .max(1)
}

fn hash_projection(hasher: &mut blake3::Hasher, projection: &GraphProjectionScope) {
    match projection {
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
}

fn provider_error(code: &str, message: impl AsRef<str>) -> ProcedureError {
    ProcedureError::Provider(format!("{code}: {}", message.as_ref()))
}
