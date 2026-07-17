#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Mutex, MutexGuard};

use storage_api::{
    AdapterCapabilities, AdapterError, AdapterFuture, ApplyReceipt, CommittedMutationBatch,
    LogicalKey, MutationOperation, StorageAdapter,
};

#[derive(Default)]
struct State {
    data: BTreeMap<LogicalKey, Vec<u8>>,
    applied_log_index: u64,
    log_fingerprints: BTreeMap<u64, u64>,
    mutation_fingerprints: BTreeMap<(u128, u32), u64>,
}

#[derive(Default)]
pub struct MemoryAdapter {
    state: Mutex<State>,
}

impl MemoryAdapter {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn lock_state(&self) -> Result<MutexGuard<'_, State>, AdapterError> {
        self.state.lock().map_err(|_| AdapterError::LockPoisoned)
    }

    fn apply(&self, batch: CommittedMutationBatch) -> Result<ApplyReceipt, AdapterError> {
        let mut state = self.lock_state()?;
        let batch_fingerprint = batch.fingerprint();

        if batch.log_index <= state.applied_log_index {
            return match state.log_fingerprints.get(&batch.log_index) {
                Some(fingerprint) if *fingerprint == batch_fingerprint => Ok(ApplyReceipt {
                    applied_log_index: state.applied_log_index,
                    duplicate: true,
                }),
                _ => Err(AdapterError::CommittedLogReplayMismatch {
                    log_index: batch.log_index,
                }),
            };
        }

        let expected = state.applied_log_index.saturating_add(1);
        if batch.log_index != expected {
            return Err(AdapterError::NonContiguousLogIndex {
                expected,
                actual: batch.log_index,
            });
        }

        let mut batch_sequences = BTreeSet::new();
        let mut fingerprints = Vec::with_capacity(batch.mutations.len());
        for mutation in &batch.mutations {
            if !batch_sequences.insert(mutation.sequence) {
                return Err(AdapterError::DuplicateMutationSequence {
                    txn_id: batch.txn_id,
                    sequence: mutation.sequence,
                });
            }

            let fingerprint = mutation.fingerprint();
            if let Some(previous) = state
                .mutation_fingerprints
                .get(&(batch.txn_id, mutation.sequence))
                && *previous != fingerprint
            {
                return Err(AdapterError::MutationReplayMismatch {
                    txn_id: batch.txn_id,
                    sequence: mutation.sequence,
                });
            }
            fingerprints.push((mutation.sequence, fingerprint));
        }

        let mut next_data = state.data.clone();
        for mutation in &batch.mutations {
            match &mutation.operation {
                MutationOperation::Put { key, value } => {
                    next_data.insert(key.clone(), value.clone());
                }
                MutationOperation::Delete { key } => {
                    next_data.remove(key);
                }
            }
        }

        state.data = next_data;
        for (sequence, fingerprint) in fingerprints {
            state
                .mutation_fingerprints
                .insert((batch.txn_id, sequence), fingerprint);
        }
        state
            .log_fingerprints
            .insert(batch.log_index, batch_fingerprint);
        state.applied_log_index = batch.log_index;

        Ok(ApplyReceipt {
            applied_log_index: batch.log_index,
            duplicate: false,
        })
    }
}

impl StorageAdapter for MemoryAdapter {
    fn capabilities(&self) -> AdapterCapabilities {
        AdapterCapabilities {
            local_atomic_batch: true,
            idempotent_apply: true,
        }
    }

    fn apply_committed<'a>(
        &'a self,
        batch: CommittedMutationBatch,
    ) -> AdapterFuture<'a, ApplyReceipt> {
        Box::pin(async move { self.apply(batch) })
    }

    fn multi_get<'a>(&'a self, keys: &'a [LogicalKey]) -> AdapterFuture<'a, Vec<Option<Vec<u8>>>> {
        Box::pin(async move {
            let state = self.lock_state()?;
            Ok(keys
                .iter()
                .map(|key| state.data.get(key).cloned())
                .collect())
        })
    }

    fn applied_log_index(&self) -> Result<u64, AdapterError> {
        Ok(self.lock_state()?.applied_log_index)
    }
}
