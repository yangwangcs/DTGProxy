#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Mutex, MutexGuard};

use storage_api::{
    AdapterCapabilities, AdapterDescriptorV1, AdapterError, AdapterFuture, ApplyReceipt,
    BackendFamily, CandidateScanPage, CandidateScanRequest, CanonicalScanPage,
    CanonicalScanRequest, ChangeScanPage, ChangeScanRequest, CommittedMutationBatch, Durability,
    KeySpan, KeyValue, LogicalKey, MutationOperation, PushdownGuarantee,
    QueryPrimitiveCapabilities, ReadSnapshot, SnapshotCapability, StorageAdapter,
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

struct MemoryReadSnapshot {
    data: BTreeMap<LogicalKey, Vec<u8>>,
    applied_log_index: u64,
}

impl ReadSnapshot for MemoryReadSnapshot {
    fn applied_log_index(&self) -> u64 {
        self.applied_log_index
    }

    fn multi_get<'a>(&'a self, keys: &'a [LogicalKey]) -> AdapterFuture<'a, Vec<Option<Vec<u8>>>> {
        Box::pin(async move { Ok(keys.iter().map(|key| self.data.get(key).cloned()).collect()) })
    }

    fn scan<'a>(&'a self, span: &'a KeySpan) -> AdapterFuture<'a, Vec<KeyValue>> {
        Box::pin(async move {
            let start = LogicalKey::in_keyspace(span.keyspace(), span.start().to_vec());
            let mut values = Vec::new();
            let mut retained = 0_u64;
            for (key, value) in self.data.range(start..) {
                if key.keyspace() != span.keyspace() || !span.contains(key.as_bytes()) {
                    break;
                }
                retained = storage_api::charge_scan_entry(span, retained, key.as_bytes(), value)?;
                values.push(KeyValue::new(key.clone(), value.clone()));
                if values.len() == span.limit().unwrap_or(usize::MAX) {
                    break;
                }
            }
            Ok(values)
        })
    }

    fn scan_canonical<'a>(
        &'a self,
        request: &'a CanonicalScanRequest,
    ) -> AdapterFuture<'a, CanonicalScanPage> {
        Box::pin(async move {
            let (entries, next_start) = bounded_page(&self.data, request.span(), request.bounds())?;
            CanonicalScanPage::new(request, self.applied_log_index, entries, next_start)
                .map_err(|error| AdapterError::Backend(error.to_string()))
        })
    }

    fn scan_candidates<'a>(
        &'a self,
        request: &'a CandidateScanRequest,
    ) -> AdapterFuture<'a, CandidateScanPage> {
        Box::pin(async move {
            let (entries, next_start) = bounded_page(&self.data, request.span(), request.bounds())?;
            CandidateScanPage::new(
                request,
                self.applied_log_index,
                PushdownGuarantee::Candidate,
                entries,
                next_start,
            )
            .map_err(|error| AdapterError::Backend(error.to_string()))
        })
    }

    fn scan_changes<'a>(
        &'a self,
        request: &'a ChangeScanRequest,
    ) -> AdapterFuture<'a, ChangeScanPage> {
        Box::pin(async move {
            let (entries, next_start) = bounded_page(&self.data, request.span(), request.bounds())?;
            ChangeScanPage::new(
                request,
                self.applied_log_index,
                PushdownGuarantee::Candidate,
                entries,
                next_start,
            )
            .map_err(|error| AdapterError::Backend(error.to_string()))
        })
    }
}

impl StorageAdapter for MemoryAdapter {
    fn descriptor(&self) -> AdapterDescriptorV1 {
        AdapterDescriptorV1::new(
            "memory",
            env!("CARGO_PKG_VERSION"),
            BackendFamily::Test,
            self.capabilities(),
        )
    }

    fn capabilities(&self) -> AdapterCapabilities {
        AdapterCapabilities {
            local_atomic_batch: true,
            idempotent_apply: true,
            consistent_multi_get: true,
            ordered_scan: true,
            durable_applied_index: false,
            durability: Durability::Volatile,
            snapshot: SnapshotCapability::None,
            logical_export: false,
            logical_restore: false,
            predicate_pushdown: true,
            adjacency_pushdown: false,
            change_feed: false,
        }
    }

    fn query_primitive_capabilities(&self) -> QueryPrimitiveCapabilities {
        QueryPrimitiveCapabilities::new(
            PushdownGuarantee::Candidate,
            PushdownGuarantee::Unsupported,
            PushdownGuarantee::Unsupported,
            PushdownGuarantee::Candidate,
        )
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

    fn scan<'a>(&'a self, span: &'a KeySpan) -> AdapterFuture<'a, Vec<KeyValue>> {
        Box::pin(async move {
            let state = self.lock_state()?;
            let start = LogicalKey::in_keyspace(span.keyspace(), span.start().to_vec());
            let mut values = Vec::new();
            let mut retained = 0_u64;
            for (key, value) in state.data.range(start..) {
                if key.keyspace() != span.keyspace() || !span.contains(key.as_bytes()) {
                    break;
                }
                retained = storage_api::charge_scan_entry(span, retained, key.as_bytes(), value)?;
                values.push(KeyValue::new(key.clone(), value.clone()));
                if values.len() == span.limit().unwrap_or(usize::MAX) {
                    break;
                }
            }
            Ok(values)
        })
    }

    fn scan_canonical<'a>(
        &'a self,
        request: &'a CanonicalScanRequest,
    ) -> AdapterFuture<'a, CanonicalScanPage> {
        Box::pin(async move {
            let state = self.lock_state()?;
            let (entries, next_start) =
                bounded_page(&state.data, request.span(), request.bounds())?;
            CanonicalScanPage::new(request, state.applied_log_index, entries, next_start)
                .map_err(|error| AdapterError::Backend(error.to_string()))
        })
    }

    fn scan_candidates<'a>(
        &'a self,
        request: &'a CandidateScanRequest,
    ) -> AdapterFuture<'a, CandidateScanPage> {
        Box::pin(async move {
            let state = self.lock_state()?;
            let span = request.span();
            let bounds = request.bounds();
            let start = LogicalKey::in_keyspace(span.keyspace(), span.start().to_vec());
            let mut entries = Vec::new();
            let mut retained = 0_u64;
            let mut next_start = None;

            for (key, value) in state.data.range(start..) {
                if key.keyspace() != span.keyspace() || !span.contains(key.as_bytes()) {
                    break;
                }
                if entries.len() == bounds.max_items() {
                    next_start = Some(key.clone());
                    break;
                }

                let entry_bytes = u64::try_from(key.as_bytes().len())
                    .ok()
                    .and_then(|key_bytes| {
                        u64::try_from(value.len())
                            .ok()
                            .and_then(|value_bytes| key_bytes.checked_add(value_bytes))
                    })
                    .unwrap_or(u64::MAX);
                let required = retained.saturating_add(entry_bytes);
                if required > bounds.max_bytes() {
                    if entries.is_empty() {
                        return Err(AdapterError::ScanByteLimit {
                            limit: bounds.max_bytes(),
                            required,
                        });
                    }
                    next_start = Some(key.clone());
                    break;
                }

                retained = required;
                entries.push(KeyValue::new(key.clone(), value.clone()));
            }

            CandidateScanPage::new(
                request,
                state.applied_log_index,
                PushdownGuarantee::Candidate,
                entries,
                next_start,
            )
            .map_err(|error| AdapterError::Backend(error.to_string()))
        })
    }

    fn scan_changes<'a>(
        &'a self,
        request: &'a ChangeScanRequest,
    ) -> AdapterFuture<'a, ChangeScanPage> {
        Box::pin(async move {
            let state = self.lock_state()?;
            let (entries, next_start) =
                bounded_page(&state.data, request.span(), request.bounds())?;
            ChangeScanPage::new(
                request,
                state.applied_log_index,
                PushdownGuarantee::Candidate,
                entries,
                next_start,
            )
            .map_err(|error| AdapterError::Backend(error.to_string()))
        })
    }

    fn begin_read_snapshot<'a>(&'a self) -> AdapterFuture<'a, Box<dyn ReadSnapshot + 'a>> {
        Box::pin(async move {
            let state = self.lock_state()?;
            Ok(Box::new(MemoryReadSnapshot {
                data: state.data.clone(),
                applied_log_index: state.applied_log_index,
            }) as Box<dyn ReadSnapshot + 'a>)
        })
    }

    fn applied_log_index(&self) -> Result<u64, AdapterError> {
        Ok(self.lock_state()?.applied_log_index)
    }
}

fn bounded_page(
    data: &BTreeMap<LogicalKey, Vec<u8>>,
    span: &KeySpan,
    bounds: storage_api::QueryPageBounds,
) -> Result<(Vec<KeyValue>, Option<LogicalKey>), AdapterError> {
    let start = LogicalKey::in_keyspace(span.keyspace(), span.start().to_vec());
    let mut entries = Vec::new();
    let mut retained = 0_u64;
    let mut next_start = None;
    for (key, value) in data.range(start..) {
        if key.keyspace() != span.keyspace() || !span.contains(key.as_bytes()) {
            break;
        }
        if entries.len() == bounds.max_items() {
            next_start = Some(key.clone());
            break;
        }
        let entry_bytes = u64::try_from(key.as_bytes().len())
            .ok()
            .and_then(|key_bytes| {
                u64::try_from(value.len())
                    .ok()
                    .and_then(|value_bytes| key_bytes.checked_add(value_bytes))
            })
            .unwrap_or(u64::MAX);
        let required = retained.saturating_add(entry_bytes);
        if required > bounds.max_bytes() {
            if entries.is_empty() {
                return Err(AdapterError::ScanByteLimit {
                    limit: bounds.max_bytes(),
                    required,
                });
            }
            next_start = Some(key.clone());
            break;
        }
        retained = required;
        entries.push(KeyValue::new(key.clone(), value.clone()));
    }
    Ok((entries, next_start))
}
