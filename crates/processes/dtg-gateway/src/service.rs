use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use dtg_execution::{
    GATEWAY_PIPELINE_MAX_PENDING, GatewayCancellationToken, GatewayExecution, GatewayOperation,
    GatewayRequestContext, GatewayResponse, GatewayRows, GatewayValue, RequestStageMetrics,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::{BoltError, BoltSession, GatewayConfig};

pub struct GatewayService {
    config: GatewayConfig,
    execution: GatewayExecution,
    request_nonce: u64,
    request_sequence: AtomicU64,
    bolt_read_pipeline_permits: Arc<Semaphore>,
}

impl GatewayService {
    pub fn new(config: GatewayConfig, execution: GatewayExecution) -> Self {
        let request_nonce = unix_time_nanos() as u64 | 1;
        Self {
            config,
            execution,
            request_nonce,
            request_sequence: AtomicU64::new(1),
            bolt_read_pipeline_permits: Arc::new(Semaphore::new(GATEWAY_PIPELINE_MAX_PENDING)),
        }
    }

    pub fn config(&self) -> &GatewayConfig {
        &self.config
    }

    pub fn request_metrics(&self) -> Arc<RequestStageMetrics> {
        self.execution.request_metrics()
    }

    pub(crate) async fn acquire_bolt_read_pipeline_permit(&self) -> OwnedSemaphorePermit {
        Arc::clone(&self.bolt_read_pipeline_permits)
            .acquire_owned()
            .await
            .expect("GatewayService retains its Bolt pipeline semaphore")
    }

    pub fn classify_bolt_statement(
        &self,
        statement: &str,
    ) -> Result<crate::BoltStatementClass, BoltError> {
        self.execution
            .classify_statement(statement)
            .map(|operation| {
                if matches!(operation, GatewayOperation::Query) {
                    crate::BoltStatementClass::Read
                } else {
                    crate::BoltStatementClass::Barrier
                }
            })
            .map_err(|error| BoltError::from_code(error.code(), error.to_string()))
    }

    pub const fn bolt(&self) -> BoltSession<'_> {
        BoltSession::new(self)
    }

    pub(crate) async fn execute_query(
        &self,
        statement: String,
        parameters: BTreeMap<String, GatewayValue>,
        transaction_id: Option<u128>,
        cancellation: &GatewayCancellationToken,
        request_timeout: Option<std::time::Duration>,
    ) -> Result<GatewayRows, BoltError> {
        let response = self
            .execute_statement_with_timeout(
                statement,
                parameters,
                transaction_id,
                cancellation,
                request_timeout,
            )
            .await?;
        match response {
            GatewayResponse::Rows(rows) => Ok(rows),
            GatewayResponse::AnalyticsResult { rows, .. } => Ok(rows),
            _ => Err(BoltError::protocol(
                "statement execution returned a non-row response",
            )),
        }
    }

    pub(crate) async fn execute_statement(
        &self,
        statement: String,
        parameters: BTreeMap<String, GatewayValue>,
        transaction_id: Option<u128>,
        cancellation: &GatewayCancellationToken,
    ) -> Result<GatewayResponse, BoltError> {
        self.execute_statement_with_timeout(
            statement,
            parameters,
            transaction_id,
            cancellation,
            None,
        )
        .await
    }

    pub(crate) async fn execute_statement_with_timeout(
        &self,
        statement: String,
        parameters: BTreeMap<String, GatewayValue>,
        transaction_id: Option<u128>,
        cancellation: &GatewayCancellationToken,
        request_timeout: Option<std::time::Duration>,
    ) -> Result<GatewayResponse, BoltError> {
        let context = self.request_context(request_timeout)?;
        self.execution
            .execute_statement(context, statement, parameters, transaction_id, cancellation)
            .await
            .map_err(Into::into)
    }

    pub(crate) async fn execute_operation(
        &self,
        operation: GatewayOperation,
        transaction_id: Option<u128>,
        cancellation: &GatewayCancellationToken,
    ) -> Result<GatewayResponse, BoltError> {
        let context = self.request_context(None)?;
        self.execution
            .execute_operation(context, operation, transaction_id, cancellation)
            .await
            .map_err(Into::into)
    }

    fn request_context(
        &self,
        request_timeout: Option<std::time::Duration>,
    ) -> Result<GatewayRequestContext, BoltError> {
        let sequence = self.request_sequence.fetch_add(1, Ordering::Relaxed);
        let request_id = (u128::from(self.request_nonce) << 64) | u128::from(sequence.max(1));
        let timeout_ms = u64::try_from(
            request_timeout
                .unwrap_or(self.config.request_timeout())
                .as_millis(),
        )
        .map_err(|_| BoltError::protocol("request timeout exceeds u64 milliseconds"))?;
        let now_ms = u64::try_from(unix_time_millis())
            .map_err(|_| BoltError::protocol("system time exceeds u64 milliseconds"))?;
        let deadline_unix_ms = now_ms
            .checked_add(timeout_ms)
            .ok_or_else(|| BoltError::protocol("request deadline overflow"))?;
        GatewayRequestContext::new(
            self.config.cluster_id(),
            request_id,
            deadline_unix_ms,
            Vec::new(),
        )
        .map_err(Into::into)
    }
}

fn unix_time_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

fn unix_time_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}
