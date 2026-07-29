use std::collections::BTreeMap;

use dtg_control::{CatalogState, ControlError, ObservedNodeState, ReconcileAction, Reconciler};

use crate::ExecutionBuildError;

pub struct ControllerExecution {
    catalog: CatalogState,
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
        if observation.catalog_version() != self.catalog.version() {
            return Err(ControlError::StaleObservation {
                expected: self.catalog.version(),
                actual: observation.catalog_version(),
            });
        }
        self.observations
            .insert(observation.node_id().to_owned(), observation);
        Ok(())
    }

    pub fn reconcile(&self) -> Result<Vec<ReconcileAction>, ControlError> {
        let observations = self.observations.values().cloned().collect::<Vec<_>>();
        Reconciler::reconcile(&self.catalog, &observations)
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
            catalog: self
                .catalog
                .ok_or(ExecutionBuildError::MissingComponent("catalog"))?,
            observations: BTreeMap::new(),
        })
    }
}
