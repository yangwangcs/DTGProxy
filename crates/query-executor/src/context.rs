use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;
use tokio::sync::Notify;

use super::{GraphOverlay, RuntimeError, RuntimeValue};
use analytics_api::ProjectedGraph;
use procedure_runtime::{JobInvocationContext, ProcedureAccess, ProcedureRegistry};

#[derive(Debug, Default)]
struct CancellationState {
    cancelled: AtomicBool,
    changed: Notify,
}

#[derive(Clone, Debug, Default)]
pub struct CancellationToken {
    state: Arc<CancellationState>,
}

impl CancellationToken {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn cancel(&self) {
        self.state.cancelled.store(true, Ordering::Release);
        self.state.changed.notify_waiters();
    }

    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.state.cancelled.load(Ordering::Acquire)
    }

    pub async fn cancelled(&self) {
        let mut changed = Box::pin(self.state.changed.notified());
        loop {
            changed.as_mut().enable();
            if self.is_cancelled() {
                return;
            }
            changed.as_mut().await;
            changed.set(self.state.changed.notified());
        }
    }
}

#[derive(Clone, Debug)]
pub struct ExecutionContext {
    parameters: BTreeMap<String, RuntimeValue>,
    cancellation: CancellationToken,
    deadline: Option<Instant>,
    graph_overlay: GraphOverlay,
    procedure_registry: Option<Arc<ProcedureRegistry>>,
    procedure_graph: Option<Arc<ProjectedGraph>>,
    security_fingerprint: [u8; 32],
    procedure_access: ProcedureAccess,
    job_invocation_context: Option<JobInvocationContext>,
}

impl ExecutionContext {
    #[must_use]
    pub fn new(parameters: BTreeMap<String, RuntimeValue>) -> Self {
        Self {
            parameters,
            cancellation: CancellationToken::new(),
            deadline: None,
            graph_overlay: GraphOverlay::default(),
            procedure_registry: None,
            procedure_graph: None,
            security_fingerprint: [0; 32],
            procedure_access: ProcedureAccess::denied(),
            job_invocation_context: None,
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

    #[must_use]
    pub fn with_graph_overlay(mut self, graph_overlay: GraphOverlay) -> Self {
        self.graph_overlay = graph_overlay;
        self
    }

    #[must_use]
    pub fn for_shard(mut self, shard_id: u32) -> Self {
        self.graph_overlay = self.graph_overlay.for_shard(shard_id);
        self
    }

    #[must_use]
    pub fn with_procedure_runtime(
        mut self,
        registry: Arc<ProcedureRegistry>,
        graph: Option<Arc<ProjectedGraph>>,
        security_fingerprint: [u8; 32],
        access: ProcedureAccess,
    ) -> Self {
        self.procedure_registry = Some(registry);
        self.procedure_graph = graph;
        self.security_fingerprint = security_fingerprint;
        self.procedure_access = access;
        self
    }

    #[must_use]
    pub fn with_job_invocation_context(mut self, context: JobInvocationContext) -> Self {
        self.job_invocation_context = Some(context);
        self
    }

    pub(crate) const fn graph_overlay(&self) -> &GraphOverlay {
        &self.graph_overlay
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

    pub async fn cancelled(&self) {
        self.cancellation.cancelled().await;
    }

    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }

    pub const fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    pub(crate) fn parameter(&self, name: &str) -> Result<&RuntimeValue, RuntimeError> {
        self.parameters
            .get(name)
            .ok_or_else(|| RuntimeError::MissingParameter(name.to_owned()))
    }

    pub(crate) fn procedure_registry(&self) -> Result<&ProcedureRegistry, RuntimeError> {
        self.procedure_registry
            .as_deref()
            .ok_or(RuntimeError::ProcedureRuntimeMissing)
    }

    pub(crate) fn procedure_graph(&self) -> Option<Arc<ProjectedGraph>> {
        self.procedure_graph.clone()
    }

    pub const fn security_fingerprint(&self) -> [u8; 32] {
        self.security_fingerprint
    }

    pub(crate) const fn procedure_access(&self) -> &ProcedureAccess {
        &self.procedure_access
    }

    pub(crate) const fn job_invocation_context(&self) -> Option<&JobInvocationContext> {
        self.job_invocation_context.as_ref()
    }
}

impl Default for ExecutionContext {
    fn default() -> Self {
        Self::new(BTreeMap::new())
    }
}
