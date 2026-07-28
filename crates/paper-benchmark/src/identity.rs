use crate::{ContractError, RawObservation, valid_digest};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResultIdentity {
    pub digest: String,
    pub row_count: u64,
}

impl ResultIdentity {
    pub fn validate(&self) -> Result<(), ContractError> {
        if !valid_digest(&self.digest) {
            return Err(ContractError::InvalidField("identity.digest"));
        }
        Ok(())
    }
}

pub fn validate_comparable_identities<'a>(
    observations: impl IntoIterator<Item = &'a RawObservation>,
) -> Result<ResultIdentity, ContractError> {
    let observations: Vec<_> = observations.into_iter().collect();
    if observations.len() != 3 {
        return Err(ContractError::ComparisonMismatch);
    }
    let first = observations[0];
    first.validate()?;

    let mut paths = BTreeSet::new();
    for observation in observations {
        observation.validate()?;
        if !paths.insert(observation.path)
            || observation.identity != first.identity
            || observation.run_id != first.run_id
            || observation.backend != first.backend
            || observation.workload != first.workload
            || observation.dataset_digest != first.dataset_digest
            || observation.workload_digest != first.workload_digest
            || observation.topology != first.topology
            || observation.concurrency != first.concurrency
            || observation.ablation != first.ablation
            || observation.repetition != first.repetition
            || observation.configuration_digest != first.configuration_digest
            || observation.snapshot != first.snapshot
            || observation.parameters_digest != first.parameters_digest
        {
            return Err(ContractError::ComparisonMismatch);
        }
    }

    if paths.len() != 3 {
        return Err(ContractError::ComparisonMismatch);
    }

    Ok(first.identity.clone())
}
