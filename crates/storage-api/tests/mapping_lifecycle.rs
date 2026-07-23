use std::future::Future;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};

use storage_api::{
    AdapterError, ApplyReceipt, BackendFamily, CommittedMutationBatch, Durability, KeySpan,
    KeyValue, Keyspace, LogicalKey, MappingBackedAdapter, MappingCapabilities, MappingDescriptorV1,
    MappingFuture, MappingRequirement, Mutation, PreparedMappingTransaction, SnapshotCapability,
    StorageAdapter, TemporalBackendMapping,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FailurePoint {
    None,
    Prepare,
    Apply,
    Commit,
}

#[derive(Clone)]
struct RecordingMapping {
    events: Arc<Mutex<Vec<&'static str>>>,
    failure: FailurePoint,
}

impl RecordingMapping {
    fn new(failure: FailurePoint) -> Self {
        Self {
            events: Arc::new(Mutex::new(Vec::new())),
            failure,
        }
    }

    fn events(&self) -> Vec<&'static str> {
        self.events.lock().unwrap().clone()
    }

    fn clear(&self) {
        self.events.lock().unwrap().clear();
    }

    fn push(&self, event: &'static str) {
        self.events.lock().unwrap().push(event);
    }
}

struct RecordingTransaction {
    mapping: RecordingMapping,
}

impl PreparedMappingTransaction for RecordingTransaction {
    fn apply<'a>(&'a mut self) -> MappingFuture<'a, ()> {
        Box::pin(async move {
            self.mapping.push("apply");
            if self.mapping.failure == FailurePoint::Apply {
                return Err(AdapterError::Backend("apply failed".into()));
            }
            Ok(())
        })
    }

    fn commit<'a>(&'a mut self) -> MappingFuture<'a, ApplyReceipt> {
        Box::pin(async move {
            self.mapping.push("commit");
            if self.mapping.failure == FailurePoint::Commit {
                return Err(AdapterError::Backend("commit failed".into()));
            }
            Ok(ApplyReceipt {
                applied_log_index: 1,
                duplicate: false,
            })
        })
    }

    fn abort<'a>(self: Box<Self>) -> MappingFuture<'a, ()>
    where
        Self: 'a,
    {
        Box::pin(async move {
            self.mapping.push("abort");
            Ok(())
        })
    }
}

impl TemporalBackendMapping for RecordingMapping {
    fn describe_schema(&self) -> MappingDescriptorV1 {
        MappingDescriptorV1::new(
            "recording-mapping",
            "1.0.0",
            BackendFamily::Test,
            [17; 32],
            MappingCapabilities {
                atomic_batch_lifecycle: true,
                deterministic_mapping: true,
                idempotent_replay: true,
                canonical_multi_get: true,
                canonical_ordered_scan: true,
                durable_applied_index: true,
                durability: Durability::Synchronous,
                snapshot: SnapshotCapability::LogicalExport,
                canonical_export: true,
                canonical_restore: true,
                native_temporal_layout: true,
                predicate_pushdown: false,
                adjacency_pushdown: false,
                change_feed: false,
            },
        )
        .unwrap()
    }

    fn validate_mapping(&self) -> Result<(), AdapterError> {
        self.push("validate");
        Ok(())
    }

    fn prepare<'a>(
        &'a self,
        _batch: CommittedMutationBatch,
    ) -> MappingFuture<'a, Box<dyn PreparedMappingTransaction + 'a>> {
        Box::pin(async move {
            self.push("prepare");
            if self.failure == FailurePoint::Prepare {
                return Err(AdapterError::Backend("prepare failed".into()));
            }
            Ok(Box::new(RecordingTransaction {
                mapping: self.clone(),
            }) as Box<dyn PreparedMappingTransaction + 'a>)
        })
    }

    fn multi_get<'a>(&'a self, keys: &'a [LogicalKey]) -> MappingFuture<'a, Vec<Option<Vec<u8>>>> {
        Box::pin(async move {
            Ok(keys
                .iter()
                .map(|key| Some(key.as_bytes().to_vec()))
                .collect())
        })
    }

    fn scan<'a>(&'a self, span: &'a KeySpan) -> MappingFuture<'a, Vec<KeyValue>> {
        Box::pin(async move {
            Ok(vec![KeyValue::new(
                LogicalKey::in_keyspace(span.keyspace(), span.start().to_vec()),
                b"scan".to_vec(),
            )])
        })
    }

    fn applied_log_index(&self) -> Result<u64, AdapterError> {
        Ok(9)
    }
}

fn batch() -> CommittedMutationBatch {
    CommittedMutationBatch {
        shard_id: 3,
        log_index: 1,
        txn_id: 7,
        mutations: vec![Mutation::put(
            0,
            LogicalKey::new(b"key".to_vec()),
            b"value".to_vec(),
        )],
    }
}

#[test]
fn bridge_runs_validate_prepare_apply_commit_in_order() {
    let mapping = RecordingMapping::new(FailurePoint::None);
    let adapter = MappingBackedAdapter::new(
        Arc::new(mapping.clone()),
        MappingRequirement::ManagedReplica,
    )
    .unwrap();
    mapping.clear();

    let receipt = block_on(adapter.apply_committed(batch())).unwrap();

    assert_eq!(receipt.applied_log_index, 1);
    assert_eq!(mapping.events(), ["prepare", "apply", "commit"]);
}

#[test]
fn bridge_aborts_after_apply_or_commit_failure_but_not_prepare_failure() {
    for (failure, expected) in [
        (FailurePoint::Prepare, vec!["prepare"]),
        (FailurePoint::Apply, vec!["prepare", "apply", "abort"]),
        (
            FailurePoint::Commit,
            vec!["prepare", "apply", "commit", "abort"],
        ),
    ] {
        let mapping = RecordingMapping::new(failure);
        let adapter = MappingBackedAdapter::new(
            Arc::new(mapping.clone()),
            MappingRequirement::ManagedReplica,
        )
        .unwrap();
        mapping.clear();

        assert!(block_on(adapter.apply_committed(batch())).is_err());
        assert_eq!(mapping.events(), expected, "failure point {failure:?}");
    }
}

#[test]
fn bridge_forwards_canonical_get_scan_descriptor_and_applied_index() {
    let mapping = RecordingMapping::new(FailurePoint::None);
    let first = LogicalKey::new(b"a".to_vec());
    let second = LogicalKey::in_keyspace(Keyspace::History, b"b".to_vec());
    assert!(mapping.capabilities().canonical_multi_get);
    assert_eq!(block_on(mapping.get(&first)).unwrap(), Some(b"a".to_vec()));
    let adapter =
        MappingBackedAdapter::new(Arc::new(mapping), MappingRequirement::ManagedReplica).unwrap();

    assert_eq!(
        block_on(adapter.multi_get(&[first, second])).unwrap(),
        vec![Some(b"a".to_vec()), Some(b"b".to_vec())]
    );
    let span = KeySpan::prefix(Keyspace::History, b"history/".to_vec());
    let rows = block_on(adapter.scan(&span)).unwrap();
    assert_eq!(rows[0].key().keyspace(), Keyspace::History);
    assert_eq!(rows[0].key().as_bytes(), b"history/");
    assert_eq!(rows[0].value(), b"scan");
    assert_eq!(adapter.applied_log_index().unwrap(), 9);
    assert_eq!(adapter.mapping_descriptor().name(), "recording-mapping");
    assert_eq!(adapter.descriptor().family(), BackendFamily::Test);
}

struct NoopWake;

impl Wake for NoopWake {
    fn wake(self: Arc<Self>) {}
}

fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::from(Arc::new(NoopWake));
    let mut context = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}
