use std::collections::BTreeMap;
use std::fmt;
use std::time::Duration;

use dtg_execution::{
    GatewayAnalyticsState, GatewayCancellationToken, GatewayExecutionError, GatewayOperation,
    GatewayRows, GatewayValue,
};

use crate::GatewayService;

pub struct BoltSession<'a> {
    service: &'a GatewayService,
}

impl<'a> BoltSession<'a> {
    pub(crate) const fn new(service: &'a GatewayService) -> Self {
        Self { service }
    }

    pub fn query(&self, statement: impl Into<String>) -> BoltQuery<'a> {
        BoltQuery {
            service: self.service,
            statement: statement.into(),
            parameters: BTreeMap::new(),
            transaction_id: None,
            cancellation: GatewayCancellationToken::new(),
            request_timeout: None,
        }
    }

    pub async fn begin(&self) -> Result<BoltTransaction<'a>, BoltError> {
        let cancellation = GatewayCancellationToken::new();
        let response = self
            .service
            .execute_statement("BEGIN".into(), BTreeMap::new(), None, &cancellation)
            .await?;
        match response {
            dtg_execution::GatewayResponse::Transaction { transaction_id } => Ok(BoltTransaction {
                service: self.service,
                transaction_id,
            }),
            _ => Err(BoltError::protocol(
                "BEGIN returned a non-transaction response",
            )),
        }
    }

    pub async fn analytics_status(&self, job_id: u128) -> Result<GatewayAnalyticsState, BoltError> {
        let cancellation = GatewayCancellationToken::new();
        let response = self
            .service
            .execute_operation(
                GatewayOperation::AnalyticsStatus { job_id },
                None,
                &cancellation,
            )
            .await?;
        match response {
            dtg_execution::GatewayResponse::AnalyticsStatus {
                job_id: actual,
                state,
            } if actual == job_id => Ok(state),
            _ => Err(BoltError::protocol(
                "analytics status returned an unexpected response",
            )),
        }
    }

    pub async fn analytics_result(&self, job_id: u128) -> Result<GatewayRows, BoltError> {
        let cancellation = GatewayCancellationToken::new();
        let response = self
            .service
            .execute_operation(
                GatewayOperation::AnalyticsResult { job_id },
                None,
                &cancellation,
            )
            .await?;
        match response {
            dtg_execution::GatewayResponse::AnalyticsResult {
                job_id: actual,
                rows,
            } if actual == job_id => Ok(rows),
            _ => Err(BoltError::protocol(
                "analytics result returned an unexpected response",
            )),
        }
    }

    pub async fn analytics_cancel(&self, job_id: u128) -> Result<(), BoltError> {
        let cancellation = GatewayCancellationToken::new();
        let response = self
            .service
            .execute_operation(
                GatewayOperation::CancelAnalytics { job_id },
                None,
                &cancellation,
            )
            .await?;
        match response {
            dtg_execution::GatewayResponse::AnalyticsCancelled { job_id: actual }
                if actual == job_id =>
            {
                Ok(())
            }
            _ => Err(BoltError::protocol(
                "analytics cancellation returned an unexpected response",
            )),
        }
    }
}

pub struct BoltTransaction<'a> {
    service: &'a GatewayService,
    transaction_id: u128,
}

impl<'a> BoltTransaction<'a> {
    pub const fn id(&self) -> u128 {
        self.transaction_id
    }

    pub fn query(&self, statement: impl Into<String>) -> BoltQuery<'a> {
        BoltQuery {
            service: self.service,
            statement: statement.into(),
            parameters: BTreeMap::new(),
            transaction_id: Some(self.transaction_id),
            cancellation: GatewayCancellationToken::new(),
            request_timeout: None,
        }
    }

    pub async fn commit(self) -> Result<(), BoltError> {
        self.finish("COMMIT").await
    }

    pub async fn rollback(self) -> Result<(), BoltError> {
        self.finish("ROLLBACK").await
    }

    async fn finish(self, statement: &str) -> Result<(), BoltError> {
        let cancellation = GatewayCancellationToken::new();
        let response = self
            .service
            .execute_statement(
                statement.into(),
                BTreeMap::new(),
                Some(self.transaction_id),
                &cancellation,
            )
            .await?;
        match response {
            dtg_execution::GatewayResponse::Acknowledged => Ok(()),
            _ => Err(BoltError::protocol(
                "transaction boundary returned an unexpected response",
            )),
        }
    }
}

pub struct BoltQuery<'a> {
    service: &'a GatewayService,
    statement: String,
    parameters: BTreeMap<String, GatewayValue>,
    transaction_id: Option<u128>,
    cancellation: GatewayCancellationToken,
    request_timeout: Option<Duration>,
}

impl<'a> BoltQuery<'a> {
    pub fn param(mut self, name: impl Into<String>, value: impl Into<GatewayValue>) -> Self {
        self.parameters.insert(name.into(), value.into());
        self
    }

    pub fn transaction(mut self, transaction_id: u128) -> Self {
        self.transaction_id = Some(transaction_id);
        self
    }

    pub fn cancellation(mut self, cancellation: GatewayCancellationToken) -> Self {
        self.cancellation = cancellation;
        self
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = Some(timeout);
        self
    }

    pub async fn run(self) -> Result<GatewayRows, BoltError> {
        self.service
            .execute_query(
                self.statement,
                self.parameters,
                self.transaction_id,
                &self.cancellation,
                self.request_timeout,
            )
            .await
    }

    pub async fn execute(self) -> Result<dtg_execution::GatewayResponse, BoltError> {
        self.service
            .execute_statement_with_timeout(
                self.statement,
                self.parameters,
                self.transaction_id,
                &self.cancellation,
                self.request_timeout,
            )
            .await
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BoltError {
    code: String,
    message: String,
}

impl BoltError {
    pub(crate) fn protocol(message: impl Into<String>) -> Self {
        Self {
            code: "DTG-GATEWAY-BOLT-PROTOCOL".into(),
            message: message.into(),
        }
    }

    pub fn code(&self) -> &str {
        &self.code
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl From<GatewayExecutionError> for BoltError {
    fn from(error: GatewayExecutionError) -> Self {
        Self {
            code: error.code().to_owned(),
            message: error.message().to_owned(),
        }
    }
}

impl fmt::Display for BoltError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for BoltError {}
