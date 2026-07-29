use std::sync::Arc;

use dtg_analytics::{
    AnalyticsJobError, AnalyticsJobId, AnalyticsJobSpec, AnalyticsLedger, JobCas, JobLease,
    JobTimestamp, WorkerId,
};
use dtg_control::{
    ActionCommand, ActionId, ActionRecord, CatalogCommand, CatalogState, ControlActionLedger,
    ControlError,
};
use dtg_storage::{TransactionId, TransactionTime, Version};
use dtg_transaction::{CommitResolution, TimestampAuthority, TxnFuture};

use crate::ExecutionBuildError;

pub struct MetaExecution {
    catalog: CatalogState,
    timestamps: Arc<dyn TimestampAuthority>,
    analytics: AnalyticsLedger,
    actions: ControlActionLedger,
}

impl MetaExecution {
    pub fn builder() -> MetaExecutionBuilder {
        MetaExecutionBuilder::default()
    }

    pub const fn catalog_version(&self) -> Version {
        self.catalog.version()
    }

    pub fn allocate_start_time(
        &self,
        transaction_id: TransactionId,
    ) -> TxnFuture<'_, TransactionTime> {
        self.timestamps.allocate_start_time(transaction_id)
    }

    pub fn reserve_commit_time(
        &self,
        transaction_id: TransactionId,
    ) -> TxnFuture<'_, TransactionTime> {
        self.timestamps.reserve_commit_time(transaction_id)
    }

    pub fn resolve_commit_time(
        &self,
        transaction_id: TransactionId,
        commit_time: TransactionTime,
        resolution: CommitResolution,
    ) -> TxnFuture<'_, ()> {
        self.timestamps
            .resolve_commit_time(transaction_id, commit_time, resolution)
    }

    pub fn apply_catalog_command(
        &mut self,
        command: CatalogCommand,
    ) -> Result<Version, ControlError> {
        let next = self.catalog.apply(command)?;
        let version = next.version();
        self.catalog = next;
        Ok(version)
    }

    pub fn apply_action_command(
        &mut self,
        command: ActionCommand,
    ) -> Result<ActionRecord, ControlError> {
        self.actions.apply(command)
    }

    pub fn action_record(&self, action_id: ActionId) -> Option<&ActionRecord> {
        self.actions.record(action_id)
    }

    pub fn submit_analytics_job(
        &mut self,
        spec: AnalyticsJobSpec,
        submitted_at: JobTimestamp,
    ) -> Result<AnalyticsJobId, AnalyticsJobError> {
        self.analytics.submit(spec, submitted_at)
    }

    pub fn analytics_job_cas(&self, job_id: AnalyticsJobId) -> Option<JobCas> {
        self.analytics.record(job_id).map(|record| record.cas())
    }

    pub fn claim_analytics_job(
        &mut self,
        job_id: AnalyticsJobId,
        worker: WorkerId,
        now: JobTimestamp,
        expected: JobCas,
    ) -> Result<JobLease, AnalyticsJobError> {
        self.analytics.claim(job_id, worker, now, expected)
    }

    pub fn renew_analytics_job_lease(
        &mut self,
        lease: JobLease,
        now: JobTimestamp,
    ) -> Result<JobLease, AnalyticsJobError> {
        self.analytics.renew(lease, now)
    }
}

#[derive(Default)]
pub struct MetaExecutionBuilder {
    catalog: Option<CatalogState>,
    timestamps: Option<Arc<dyn TimestampAuthority>>,
    analytics: Option<AnalyticsLedger>,
    actions: Option<ControlActionLedger>,
}

impl MetaExecutionBuilder {
    pub fn with_catalog(mut self, catalog: CatalogState) -> Self {
        self.catalog = Some(catalog);
        self
    }

    pub fn with_timestamps(mut self, timestamps: Arc<dyn TimestampAuthority>) -> Self {
        self.timestamps = Some(timestamps);
        self
    }

    pub fn with_analytics_ledger(mut self, analytics: AnalyticsLedger) -> Self {
        self.analytics = Some(analytics);
        self
    }

    pub fn with_action_ledger(mut self, actions: ControlActionLedger) -> Self {
        self.actions = Some(actions);
        self
    }

    pub fn build(self) -> Result<MetaExecution, ExecutionBuildError> {
        Ok(MetaExecution {
            catalog: self
                .catalog
                .ok_or(ExecutionBuildError::MissingComponent("catalog"))?,
            timestamps: self
                .timestamps
                .ok_or(ExecutionBuildError::MissingComponent("timestamp authority"))?,
            analytics: self
                .analytics
                .ok_or(ExecutionBuildError::MissingComponent("analytics ledger"))?,
            actions: self.actions.unwrap_or_default(),
        })
    }
}
