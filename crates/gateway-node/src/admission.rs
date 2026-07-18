use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

pub struct AdmissionController {
    semaphore: Arc<Semaphore>,
    maximum: usize,
    closed: AtomicBool,
}

impl AdmissionController {
    pub fn new(maximum: usize) -> Result<Self, AdmissionError> {
        if maximum == 0 {
            return Err(AdmissionError::InvalidMaximum);
        }
        Ok(Self {
            semaphore: Arc::new(Semaphore::new(maximum)),
            maximum,
            closed: AtomicBool::new(false),
        })
    }

    pub fn try_enter(&self) -> Result<AdmissionPermit, AdmissionError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(AdmissionError::Closed);
        }
        Arc::clone(&self.semaphore)
            .try_acquire_owned()
            .map(|permit| AdmissionPermit { _permit: permit })
            .map_err(|_| AdmissionError::Exhausted)
    }

    pub fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.semaphore.close();
    }

    #[must_use]
    pub fn inflight(&self) -> usize {
        self.maximum
            .saturating_sub(self.semaphore.available_permits())
    }

    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
}

pub struct AdmissionPermit {
    _permit: OwnedSemaphorePermit,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdmissionError {
    InvalidMaximum,
    Exhausted,
    Closed,
}

impl Display for AdmissionError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidMaximum => formatter.write_str("admission maximum must be non-zero"),
            Self::Exhausted => formatter.write_str("Gateway admission limit reached"),
            Self::Closed => formatter.write_str("Gateway admission is closed"),
        }
    }
}

impl Error for AdmissionError {}
