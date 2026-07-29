use std::collections::BTreeMap;

use dtg_control::{
    ActionCommand, ActionFailure, ActionRecord, ActionState, CatalogReplica, CatalogState,
    ControlError, ObservedNodeState, ReconcileAction, Reconciler, Version,
};

use crate::ExecutionBuildError;

pub trait ControlActionExecutor: Send + Sync {
    fn execute(&self, action: &ReconcileAction) -> Result<(), ActionFailure>;
}

pub struct ControllerExecution {
    catalog: CatalogReplica,
    observations: BTreeMap<String, ObservedNodeState>,
}

impl ControllerExecution {
    pub fn builder() -> ControllerExecutionBuilder {
        ControllerExecutionBuilder::default()
    }

    pub fn record_observation(
        &mut self,
        observation: ObservedNodeState,
    ) -> Result<(), ControlError> {
        observation.validate()?;
        if observation.catalog_version() != self.catalog.snapshot().version() {
            return Err(ControlError::StaleObservation {
                expected: self.catalog.snapshot().version(),
                actual: observation.catalog_version(),
            });
        }
        self.observations
            .insert(observation.node_id().to_owned(), observation);
        Ok(())
    }

    pub fn reconcile(&self) -> Result<Vec<ReconcileAction>, ControlError> {
        let observations = self.observations.values().cloned().collect::<Vec<_>>();
        Reconciler::reconcile(self.catalog.snapshot(), &observations)
    }

    pub const fn catalog_version(&self) -> Version {
        self.catalog.snapshot().version()
    }

    pub fn install_catalog(&mut self, catalog: CatalogState) -> Result<bool, ControlError> {
        let changed = self.catalog.install(catalog)?;
        if changed {
            self.observations.clear();
        }
        Ok(changed)
    }

    pub fn execute_claimed_action(
        &self,
        record: &ActionRecord,
        executor: &dyn ControlActionExecutor,
    ) -> Result<ActionCommand, ControlError> {
        let ActionState::Claimed { lease, .. } = record.state() else {
            return Err(ControlError::InvalidAction(
                "Controller may execute only a Meta-claimed action",
            ));
        };
        Ok(match executor.execute(record.action()) {
            Ok(()) => ActionCommand::complete(record.action_id(), *lease),
            Err(failure) => ActionCommand::fail(record.action_id(), *lease, failure),
        })
    }
}

#[derive(Default)]
pub struct ControllerExecutionBuilder {
    catalog: Option<CatalogState>,
}

impl ControllerExecutionBuilder {
    pub fn with_catalog(mut self, catalog: CatalogState) -> Self {
        self.catalog = Some(catalog);
        self
    }

    pub fn build(self) -> Result<ControllerExecution, ExecutionBuildError> {
        Ok(ControllerExecution {
            catalog: CatalogReplica::new(
                self.catalog
                    .ok_or(ExecutionBuildError::MissingComponent("catalog"))?,
            )
            .map_err(|_| ExecutionBuildError::MissingComponent("valid catalog"))?,
            observations: BTreeMap::new(),
        })
    }
}
