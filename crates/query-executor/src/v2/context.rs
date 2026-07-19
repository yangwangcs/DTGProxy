use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use super::{RuntimeError, RuntimeValue};

#[derive(Clone, Debug, Default)]
pub struct CancellationToken {
    cancelled: Arc<AtomicBool>,
}

impl CancellationToken {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
}

#[derive(Clone, Debug)]
pub struct ExecutionContext {
    parameters: BTreeMap<String, RuntimeValue>,
    cancellation: CancellationToken,
    deadline: Option<Instant>,
}

impl ExecutionContext {
    #[must_use]
    pub fn new(parameters: BTreeMap<String, RuntimeValue>) -> Self {
        Self {
            parameters,
            cancellation: CancellationToken::new(),
            deadline: None,
        }
    }

    #[must_use]
    pub fn with_cancellation(mut self, cancellation: CancellationToken) -> Self {
        self.cancellation = cancellation;
        self
    }

    #[must_use]
    pub fn with_deadline(mut self, deadline: Instant) -> Self {
        self.deadline = Some(deadline);
        self
    }

    pub(crate) fn check_fences(&self) -> Result<(), RuntimeError> {
        if self.cancellation.is_cancelled() {
            return Err(RuntimeError::Cancelled);
        }
        if self
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            return Err(RuntimeError::DeadlineExceeded);
        }
        Ok(())
    }

    pub(crate) fn parameter(&self, name: &str) -> Result<&RuntimeValue, RuntimeError> {
        self.parameters
            .get(name)
            .ok_or_else(|| RuntimeError::MissingParameter(name.to_owned()))
    }
}

impl Default for ExecutionContext {
    fn default() -> Self {
        Self::new(BTreeMap::new())
    }
}
