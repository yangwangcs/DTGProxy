#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Mutex, MutexGuard};

use storage_api::{
    AdapterCapabilities, AdapterError, AdapterFuture, ApplyReceipt, CommittedMutationBatch,
    LogicalKey, Mutation, MutationOperation, StorageAdapter,
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
        let batch_fingerprint = fingerprint_batch(&batch);

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

            let fingerprint = fingerprint_mutation(mutation);
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

    fn multi_get<'a>(
        &'a self,
        keys: &'a [LogicalKey],
    ) -> AdapterFuture<'a, Vec<Option<Vec<u8>>>> {
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

fn fingerprint_batch(batch: &CommittedMutationBatch) -> u64 {
    let mut fingerprint = Fnv1a::new();
    fingerprint.write(&batch.shard_id.to_be_bytes());
    fingerprint.write(&batch.log_index.to_be_bytes());
    fingerprint.write(&batch.txn_id.to_be_bytes());
    fingerprint.write_len(batch.mutations.len());
    for mutation in &batch.mutations {
        fingerprint.write(&fingerprint_mutation(mutation).to_be_bytes());
    }
    fingerprint.finish()
}

fn fingerprint_mutation(mutation: &Mutation) -> u64 {
    let mut fingerprint = Fnv1a::new();
    fingerprint.write(&mutation.sequence.to_be_bytes());
    match &mutation.operation {
        MutationOperation::Put { key, value } => {
            fingerprint.write(&[1]);
            fingerprint.write_length_delimited(key.as_bytes());
            fingerprint.write_length_delimited(value);
        }
        MutationOperation::Delete { key } => {
            fingerprint.write(&[2]);
            fingerprint.write_length_delimited(key.as_bytes());
        }
    }
    fingerprint.finish()
}

struct Fnv1a(u64);

impl Fnv1a {
    const OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;

    const fn new() -> Self {
        Self(Self::OFFSET_BASIS)
    }

    fn write(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 ^= u64::from(*byte);
            self.0 = self.0.wrapping_mul(Self::PRIME);
        }
    }

    fn write_len(&mut self, len: usize) {
        let len = u64::try_from(len).expect("usize always fits in u64 on supported targets");
        self.write(&len.to_be_bytes());
    }

    fn write_length_delimited(&mut self, bytes: &[u8]) {
        self.write_len(bytes.len());
        self.write(bytes);
    }

    const fn finish(&self) -> u64 {
        self.0
    }
}

