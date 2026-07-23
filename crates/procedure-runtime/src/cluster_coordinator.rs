use std::error::Error;
use std::fmt::{self, Display, Formatter};

use analytics_api::AlgorithmValue;
use analytics_ledger::{AnalyticsJobId, GraphProjectionScope, JobState, ProjectionLimits};
use temporal_types::TransactionTime;

const MAX_COORDINATOR_ERROR_BYTES: usize = 4_096;
const MAX_RESULT_COLUMNS: usize = 256;
const MAX_RESULT_ROWS: usize = 4_096;
const MAX_RESULT_VALUE_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JobInvocationContext {
    outer_request_id: u128,
    deadline_unix_ms: u64,
    graph_id: u64,
    catalog_revision: u64,
    topology_epoch: u64,
    schema_version: u64,
    backend_generation: u64,
    transaction_snapshot: TransactionTime,
    projection: GraphProjectionScope,
    limits: ProjectionLimits,
}

impl JobInvocationContext {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        outer_request_id: u128,
        deadline_unix_ms: u64,
        graph_id: u64,
        catalog_revision: u64,
        topology_epoch: u64,
        schema_version: u64,
        backend_generation: u64,
        transaction_snapshot: TransactionTime,
        projection: GraphProjectionScope,
        limits: ProjectionLimits,
    ) -> Result<Self, ClusterAnalyticsError> {
        if outer_request_id == 0
            || deadline_unix_ms == 0
            || graph_id == 0
            || catalog_revision == 0
            || topology_epoch == 0
            || schema_version == 0
            || backend_generation == 0
        {
            return Err(ClusterAnalyticsError::new(
                "DTG-ANALYTICS-JOB-CONTEXT",
                "analytics job invocation context contains a zero fence",
            ));
        }
        Ok(Self {
            outer_request_id,
            deadline_unix_ms,
            graph_id,
            catalog_revision,
            topology_epoch,
            schema_version,
            backend_generation,
            transaction_snapshot,
            projection,
            limits,
        })
    }

    #[must_use]
    pub const fn outer_request_id(&self) -> u128 {
        self.outer_request_id
    }

    #[must_use]
    pub const fn deadline_unix_ms(&self) -> u64 {
        self.deadline_unix_ms
    }

    #[must_use]
    pub const fn graph_id(&self) -> u64 {
        self.graph_id
    }

    #[must_use]
    pub const fn catalog_revision(&self) -> u64 {
        self.catalog_revision
    }

    #[must_use]
    pub const fn topology_epoch(&self) -> u64 {
        self.topology_epoch
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
    pub const fn transaction_snapshot(&self) -> TransactionTime {
        self.transaction_snapshot
    }

    #[must_use]
    pub const fn projection(&self) -> &GraphProjectionScope {
        &self.projection
    }

    #[must_use]
    pub const fn limits(&self) -> ProjectionLimits {
        self.limits
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AnalyticsSubmitRequest {
    submission_request_id: u128,
    context: JobInvocationContext,
    algorithm: String,
    algorithm_version: String,
    provider: String,
    provider_version: String,
    parameters: Vec<u8>,
    security_fingerprint: [u8; 32],
}

impl AnalyticsSubmitRequest {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        submission_request_id: u128,
        context: JobInvocationContext,
        algorithm: String,
        algorithm_version: String,
        provider: String,
        provider_version: String,
        parameters: Vec<u8>,
        security_fingerprint: [u8; 32],
    ) -> Result<Self, ClusterAnalyticsError> {
        if submission_request_id == 0
            || security_fingerprint == [0; 32]
            || algorithm.is_empty()
            || algorithm_version.is_empty()
            || provider.is_empty()
            || provider_version.is_empty()
            || parameters.is_empty()
        {
            return Err(ClusterAnalyticsError::new(
                "DTG-ANALYTICS-JOB-SUBMIT",
                "analytics submit request is not canonically fenced",
            ));
        }
        Ok(Self {
            submission_request_id,
            context,
            algorithm,
            algorithm_version,
            provider,
            provider_version,
            parameters,
            security_fingerprint,
        })
    }

    #[must_use]
    pub const fn submission_request_id(&self) -> u128 {
        self.submission_request_id
    }

    #[must_use]
    pub const fn deadline_unix_ms(&self) -> u64 {
        self.context.deadline_unix_ms()
    }

    #[must_use]
    pub const fn graph_id(&self) -> u64 {
        self.context.graph_id()
    }

    #[must_use]
    pub const fn catalog_revision(&self) -> u64 {
        self.context.catalog_revision()
    }

    #[must_use]
    pub const fn topology_epoch(&self) -> u64 {
        self.context.topology_epoch()
    }

    #[must_use]
    pub const fn schema_version(&self) -> u64 {
        self.context.schema_version()
    }

    #[must_use]
    pub const fn backend_generation(&self) -> u64 {
        self.context.backend_generation()
    }

    #[must_use]
    pub const fn transaction_snapshot(&self) -> TransactionTime {
        self.context.transaction_snapshot()
    }

    #[must_use]
    pub const fn projection(&self) -> &GraphProjectionScope {
        self.context.projection()
    }

    #[must_use]
    pub const fn limits(&self) -> ProjectionLimits {
        self.context.limits()
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
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AnalyticsSubmitResponse {
    job_id: AnalyticsJobId,
}

impl AnalyticsSubmitResponse {
    #[must_use]
    pub const fn new(job_id: AnalyticsJobId) -> Self {
        Self { job_id }
    }

    #[must_use]
    pub const fn job_id(self) -> AnalyticsJobId {
        self.job_id
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AnalyticsStatusRequest {
    invocation_request_id: u128,
    job_id: AnalyticsJobId,
    security_fingerprint: [u8; 32],
    deadline_unix_ms: u64,
}

impl AnalyticsStatusRequest {
    pub(crate) fn new(
        invocation_request_id: u128,
        job_id: AnalyticsJobId,
        security_fingerprint: [u8; 32],
        deadline_unix_ms: u64,
    ) -> Result<Self, ClusterAnalyticsError> {
        validate_action_request(
            invocation_request_id,
            security_fingerprint,
            deadline_unix_ms,
        )?;
        Ok(Self {
            invocation_request_id,
            job_id,
            security_fingerprint,
            deadline_unix_ms,
        })
    }

    #[must_use]
    pub const fn invocation_request_id(self) -> u128 {
        self.invocation_request_id
    }

    #[must_use]
    pub const fn job_id(self) -> AnalyticsJobId {
        self.job_id
    }

    #[must_use]
    pub const fn security_fingerprint(self) -> [u8; 32] {
        self.security_fingerprint
    }

    #[must_use]
    pub const fn deadline_unix_ms(self) -> u64 {
        self.deadline_unix_ms
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AnalyticsStatus {
    state: JobState,
    completed_units: u64,
    total_units: Option<u64>,
    failure: Option<String>,
}

impl AnalyticsStatus {
    pub fn new(
        state: JobState,
        completed_units: u64,
        total_units: Option<u64>,
        failure: Option<String>,
    ) -> Result<Self, ClusterAnalyticsError> {
        if total_units.is_some_and(|total| completed_units > total)
            || failure
                .as_ref()
                .is_some_and(|value| value.len() > MAX_COORDINATOR_ERROR_BYTES)
        {
            return Err(ClusterAnalyticsError::new(
                "DTG-ANALYTICS-JOB-STATUS",
                "analytics status exceeds its current bounds",
            ));
        }
        Ok(Self {
            state,
            completed_units,
            total_units,
            failure,
        })
    }

    #[must_use]
    pub const fn state(&self) -> JobState {
        self.state
    }

    #[must_use]
    pub const fn completed_units(&self) -> u64 {
        self.completed_units
    }

    #[must_use]
    pub const fn total_units(&self) -> Option<u64> {
        self.total_units
    }

    #[must_use]
    pub fn failure(&self) -> Option<&str> {
        self.failure.as_deref()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AnalyticsResultsRequest {
    invocation_request_id: u128,
    job_id: AnalyticsJobId,
    security_fingerprint: [u8; 32],
    deadline_unix_ms: u64,
    offset: usize,
    limit: usize,
}

impl AnalyticsResultsRequest {
    pub(crate) fn new(
        invocation_request_id: u128,
        job_id: AnalyticsJobId,
        security_fingerprint: [u8; 32],
        deadline_unix_ms: u64,
        offset: usize,
        limit: usize,
    ) -> Result<Self, ClusterAnalyticsError> {
        validate_action_request(
            invocation_request_id,
            security_fingerprint,
            deadline_unix_ms,
        )?;
        if limit == 0 || limit > MAX_RESULT_ROWS {
            return Err(ClusterAnalyticsError::new(
                "DTG-ANALYTICS-RESULT-LIMIT",
                "analytics result limit is outside the current bound",
            ));
        }
        Ok(Self {
            invocation_request_id,
            job_id,
            security_fingerprint,
            deadline_unix_ms,
            offset,
            limit,
        })
    }

    #[must_use]
    pub const fn invocation_request_id(self) -> u128 {
        self.invocation_request_id
    }
    #[must_use]
    pub const fn job_id(self) -> AnalyticsJobId {
        self.job_id
    }
    #[must_use]
    pub const fn security_fingerprint(self) -> [u8; 32] {
        self.security_fingerprint
    }
    #[must_use]
    pub const fn deadline_unix_ms(self) -> u64 {
        self.deadline_unix_ms
    }
    #[must_use]
    pub const fn offset(self) -> usize {
        self.offset
    }
    #[must_use]
    pub const fn limit(self) -> usize {
        self.limit
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AnalyticsResultPage {
    columns: Vec<String>,
    rows: Vec<Vec<AlgorithmValue>>,
    total_rows: u64,
}

impl AnalyticsResultPage {
    pub fn new(
        columns: Vec<String>,
        rows: Vec<Vec<AlgorithmValue>>,
        total_rows: u64,
    ) -> Result<Self, ClusterAnalyticsError> {
        if columns.is_empty()
            || columns.len() > MAX_RESULT_COLUMNS
            || rows.len() > MAX_RESULT_ROWS
            || rows.iter().any(|row| row.len() != columns.len())
            || estimated_result_bytes(&columns, &rows) > MAX_RESULT_VALUE_BYTES
        {
            return Err(ClusterAnalyticsError::new(
                "DTG-ANALYTICS-RESULT-BOUNDS",
                "analytics result page exceeds its current bounds",
            ));
        }
        Ok(Self {
            columns,
            rows,
            total_rows,
        })
    }

    #[must_use]
    pub fn columns(&self) -> &[String] {
        &self.columns
    }
    #[must_use]
    pub fn rows(&self) -> &[Vec<AlgorithmValue>] {
        &self.rows
    }
    #[must_use]
    pub const fn total_rows(&self) -> u64 {
        self.total_rows
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AnalyticsCancelRequest {
    invocation_request_id: u128,
    job_id: AnalyticsJobId,
    security_fingerprint: [u8; 32],
    deadline_unix_ms: u64,
}

impl AnalyticsCancelRequest {
    pub(crate) fn new(
        invocation_request_id: u128,
        job_id: AnalyticsJobId,
        security_fingerprint: [u8; 32],
        deadline_unix_ms: u64,
    ) -> Result<Self, ClusterAnalyticsError> {
        validate_action_request(
            invocation_request_id,
            security_fingerprint,
            deadline_unix_ms,
        )?;
        Ok(Self {
            invocation_request_id,
            job_id,
            security_fingerprint,
            deadline_unix_ms,
        })
    }

    #[must_use]
    pub const fn invocation_request_id(self) -> u128 {
        self.invocation_request_id
    }
    #[must_use]
    pub const fn job_id(self) -> AnalyticsJobId {
        self.job_id
    }
    #[must_use]
    pub const fn security_fingerprint(self) -> [u8; 32] {
        self.security_fingerprint
    }
    #[must_use]
    pub const fn deadline_unix_ms(self) -> u64 {
        self.deadline_unix_ms
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AnalyticsCancelResponse {
    canceled: bool,
}

impl AnalyticsCancelResponse {
    #[must_use]
    pub const fn new(canceled: bool) -> Self {
        Self { canceled }
    }

    #[must_use]
    pub const fn canceled(self) -> bool {
        self.canceled
    }
}

pub trait ClusterAnalyticsCoordinator: Send + Sync {
    fn submit(
        &self,
        request: AnalyticsSubmitRequest,
    ) -> Result<AnalyticsSubmitResponse, ClusterAnalyticsError>;
    fn status(
        &self,
        request: AnalyticsStatusRequest,
    ) -> Result<AnalyticsStatus, ClusterAnalyticsError>;
    fn results(
        &self,
        request: AnalyticsResultsRequest,
    ) -> Result<AnalyticsResultPage, ClusterAnalyticsError>;
    fn cancel(
        &self,
        request: AnalyticsCancelRequest,
    ) -> Result<AnalyticsCancelResponse, ClusterAnalyticsError>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterAnalyticsError {
    code: String,
    message: String,
}

impl ClusterAnalyticsError {
    #[must_use]
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        let mut code = code.into();
        if !is_stable_code(&code) {
            code = "DTG-ANALYTICS-COORDINATOR".into();
        }
        let mut message = message.into();
        if message.len() > MAX_COORDINATOR_ERROR_BYTES {
            let boundary = (0..=MAX_COORDINATOR_ERROR_BYTES)
                .rev()
                .find(|index| message.is_char_boundary(*index))
                .unwrap_or(0);
            message.truncate(boundary);
        }
        Self { code, message }
    }

    #[must_use]
    pub fn code(&self) -> &str {
        &self.code
    }

    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl Display for ClusterAnalyticsError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.code, self.message)
    }
}

impl Error for ClusterAnalyticsError {}

fn validate_action_request(
    invocation_request_id: u128,
    security_fingerprint: [u8; 32],
    deadline_unix_ms: u64,
) -> Result<(), ClusterAnalyticsError> {
    if invocation_request_id == 0 || security_fingerprint == [0; 32] || deadline_unix_ms == 0 {
        return Err(ClusterAnalyticsError::new(
            "DTG-ANALYTICS-JOB-CONTEXT",
            "analytics job action is missing a request, deadline, or security fence",
        ));
    }
    Ok(())
}

fn is_stable_code(code: &str) -> bool {
    code.starts_with("DTG-")
        && code
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'-')
}

fn estimated_result_bytes(columns: &[String], rows: &[Vec<AlgorithmValue>]) -> usize {
    columns
        .iter()
        .map(String::len)
        .chain(rows.iter().flatten().map(|value| match value {
            AlgorithmValue::Null => 1,
            AlgorithmValue::Boolean(_) => 1,
            AlgorithmValue::Integer(_) | AlgorithmValue::FloatBits(_) | AlgorithmValue::Time(_) => {
                8
            }
            AlgorithmValue::Vertex(_) => 16,
            AlgorithmValue::String(value) => value.len(),
        }))
        .try_fold(0_usize, usize::checked_add)
        .unwrap_or(usize::MAX)
}
