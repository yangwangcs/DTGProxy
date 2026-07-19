use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

use analytics_api::{AlgorithmRequest, AlgorithmResult, AnalyticsProvider};

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct AnalyticsJobId(u64);

impl AnalyticsJobId {
    #[must_use]
    pub const fn value(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JobState {
    Queued,
    Running,
    Succeeded,
    Failed,
    Canceled,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JobCheckpoint {
    completed_units: u64,
    total_units: Option<u64>,
}

impl JobCheckpoint {
    #[must_use]
    pub const fn completed_units(&self) -> u64 {
        self.completed_units
    }

    #[must_use]
    pub const fn total_units(&self) -> Option<u64> {
        self.total_units
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JobStatus {
    state: JobState,
    checkpoint: JobCheckpoint,
    error: Option<String>,
}

impl JobStatus {
    #[must_use]
    pub const fn state(&self) -> JobState {
        self.state
    }

    #[must_use]
    pub const fn checkpoint(&self) -> &JobCheckpoint {
        &self.checkpoint
    }

    #[must_use]
    pub fn error(&self) -> Option<&str> {
        self.error.as_deref()
    }
}

struct JobRecord {
    status: JobStatus,
    result: Option<AlgorithmResult>,
    cancel: Arc<AtomicBool>,
}

#[derive(Clone)]
pub struct AnalyticsJobManager {
    provider: Arc<dyn AnalyticsProvider>,
    jobs: Arc<Mutex<BTreeMap<AnalyticsJobId, JobRecord>>>,
    next_id: Arc<AtomicU64>,
    maximum_jobs: usize,
}

impl AnalyticsJobManager {
    pub fn new(
        provider: Arc<dyn AnalyticsProvider>,
        maximum_jobs: usize,
    ) -> Result<Self, JobError> {
        if maximum_jobs == 0 {
            return Err(JobError::new(
                "DTG-ANALYTICS-INVALID-JOB-LIMIT",
                "maximum job count must be non-zero",
            ));
        }
        Ok(Self {
            provider,
            jobs: Arc::new(Mutex::new(BTreeMap::new())),
            next_id: Arc::new(AtomicU64::new(1)),
            maximum_jobs,
        })
    }

    pub fn submit(&self, request: AlgorithmRequest) -> Result<AnalyticsJobId, JobError> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        if id == u64::MAX {
            return Err(JobError::new(
                "DTG-ANALYTICS-JOB-ID-EXHAUSTED",
                "analytics job identity space exhausted",
            ));
        }
        let id = AnalyticsJobId(id);
        let cancel = Arc::new(AtomicBool::new(false));
        {
            let mut jobs = self.lock_jobs()?;
            if jobs.len() >= self.maximum_jobs {
                return Err(JobError::new(
                    "DTG-ANALYTICS-JOB-LIMIT",
                    "maximum resident analytics job count reached",
                ));
            }
            jobs.insert(
                id,
                JobRecord {
                    status: JobStatus {
                        state: JobState::Queued,
                        checkpoint: JobCheckpoint {
                            completed_units: 0,
                            total_units: None,
                        },
                        error: None,
                    },
                    result: None,
                    cancel: Arc::clone(&cancel),
                },
            );
        }
        let jobs = Arc::clone(&self.jobs);
        let provider = Arc::clone(&self.provider);
        thread::Builder::new()
            .name(format!("dtg-analytics-{id:?}"))
            .spawn(move || {
                if let Ok(mut records) = jobs.lock()
                    && let Some(record) = records.get_mut(&id)
                {
                    if record.cancel.load(Ordering::Acquire) {
                        record.status.state = JobState::Canceled;
                        return;
                    }
                    record.status.state = JobState::Running;
                    record.status.checkpoint.completed_units = 1;
                }
                let result = provider.execute(request);
                if let Ok(mut records) = jobs.lock()
                    && let Some(record) = records.get_mut(&id)
                {
                    if record.cancel.load(Ordering::Acquire) {
                        record.status.state = JobState::Canceled;
                    } else {
                        match result {
                            Ok(result) => {
                                record.status.state = JobState::Succeeded;
                                record.status.checkpoint.completed_units = 2;
                                record.result = Some(result);
                            }
                            Err(error) => {
                                record.status.state = JobState::Failed;
                                record.status.error = Some(error.to_string());
                            }
                        }
                    }
                }
            })
            .map_err(|error| JobError::new("DTG-ANALYTICS-JOB-SPAWN", error.to_string()))?;
        Ok(id)
    }

    pub fn status(&self, id: AnalyticsJobId) -> Result<JobStatus, JobError> {
        Ok(self
            .lock_jobs()?
            .get(&id)
            .ok_or_else(|| JobError::unknown(id))?
            .status
            .clone())
    }

    pub fn checkpoint(&self, id: AnalyticsJobId) -> Result<Option<JobCheckpoint>, JobError> {
        Ok(Some(self.status(id)?.checkpoint.clone()))
    }

    pub fn results(&self, id: AnalyticsJobId) -> Result<Option<AlgorithmResult>, JobError> {
        Ok(self
            .lock_jobs()?
            .get(&id)
            .ok_or_else(|| JobError::unknown(id))?
            .result
            .clone())
    }

    pub fn cancel(&self, id: AnalyticsJobId) -> Result<(), JobError> {
        let mut jobs = self.lock_jobs()?;
        let record = jobs.get_mut(&id).ok_or_else(|| JobError::unknown(id))?;
        if matches!(record.status.state, JobState::Succeeded | JobState::Failed) {
            return Err(JobError::new(
                "DTG-ANALYTICS-JOB-FINAL",
                "a completed analytics job cannot be canceled",
            ));
        }
        record.cancel.store(true, Ordering::Release);
        record.status.state = JobState::Canceled;
        Ok(())
    }

    pub fn remove(&self, id: AnalyticsJobId) -> Result<(), JobError> {
        if self.lock_jobs()?.remove(&id).is_none() {
            return Err(JobError::unknown(id));
        }
        Ok(())
    }

    fn lock_jobs(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, BTreeMap<AnalyticsJobId, JobRecord>>, JobError> {
        self.jobs.lock().map_err(|_| {
            JobError::new(
                "DTG-ANALYTICS-JOB-LOCK",
                "analytics job state lock poisoned",
            )
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JobError {
    code: &'static str,
    message: String,
}

impl JobError {
    fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    fn unknown(id: AnalyticsJobId) -> Self {
        Self::new(
            "DTG-ANALYTICS-JOB-NOT-FOUND",
            format!("analytics job {} does not exist", id.0),
        )
    }

    #[must_use]
    pub const fn code(&self) -> &'static str {
        self.code
    }
}

impl Display for JobError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.code, self.message)
    }
}

impl Error for JobError {}
