use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use crate::{
    AdapterError, ApplyReceipt, CommittedMutationBatch, KeySpan, Keyspace, LogicalKey,
    LogicalSnapshotChunkV1, LogicalSnapshotExportRequest, LogicalSnapshotManifestV1, Mutation,
    TemporalBackendMapping,
};

pub fn run_mapping_tck(mapping: &dyn TemporalBackendMapping) {
    let current_a = LogicalKey::in_keyspace(Keyspace::TemporalIndex, b"entity/a".to_vec());
    let current_b = LogicalKey::in_keyspace(Keyspace::TemporalIndex, b"entity/b".to_vec());
    let history_a = LogicalKey::in_keyspace(Keyspace::Meta, b"entity/a".to_vec());
    let first = batch(
        1,
        101,
        vec![
            Mutation::put(0, current_b.clone(), b"current-b".to_vec()),
            Mutation::put(1, current_a.clone(), b"current-a".to_vec()),
            Mutation::put(2, history_a.clone(), b"history-a".to_vec()),
        ],
    );
    let receipt = apply(mapping, first.clone()).expect("initial Mapping commit");
    assert_eq!(receipt.applied_log_index, 1);
    assert!(!receipt.duplicate);
    assert!(
        apply(mapping, first)
            .expect("exact Mapping replay")
            .duplicate
    );

    assert_eq!(
        block_on(mapping.multi_get(&[
            current_a.clone(),
            current_b.clone(),
            history_a.clone(),
            LogicalKey::in_keyspace(Keyspace::TemporalIndex, b"missing".to_vec()),
        ]))
        .expect("canonical Mapping multi-get"),
        vec![
            Some(b"current-a".to_vec()),
            Some(b"current-b".to_vec()),
            Some(b"history-a".to_vec()),
            None,
        ]
    );
    let scan = block_on(mapping.scan(&KeySpan::prefix(
        Keyspace::TemporalIndex,
        b"entity/".to_vec(),
    )))
    .expect("canonical Mapping scan");
    assert_eq!(scan.len(), 2);
    assert_eq!(scan[0].key(), &current_a);
    assert_eq!(scan[1].key(), &current_b);

    assert_eq!(
        apply(mapping, batch(3, 103, Vec::new())),
        Err(AdapterError::NonContiguousLogIndex {
            expected: 2,
            actual: 3,
        })
    );
    assert_eq!(
        apply(
            mapping,
            batch(
                2,
                102,
                vec![
                    Mutation::put(
                        0,
                        LogicalKey::in_keyspace(Keyspace::TemporalIndex, b"partial/a".to_vec()),
                        vec![1],
                    ),
                    Mutation::put(
                        0,
                        LogicalKey::in_keyspace(Keyspace::TemporalIndex, b"partial/b".to_vec()),
                        vec![2],
                    ),
                ],
            ),
        ),
        Err(AdapterError::DuplicateMutationSequence {
            txn_id: 102,
            sequence: 0,
        })
    );
    assert_eq!(
        block_on(mapping.multi_get(&[
            LogicalKey::in_keyspace(Keyspace::TemporalIndex, b"partial/a".to_vec()),
            LogicalKey::in_keyspace(Keyspace::TemporalIndex, b"partial/b".to_vec()),
        ]))
        .expect("failed batch visibility check"),
        vec![None, None]
    );

    apply(
        mapping,
        batch(
            2,
            104,
            vec![
                Mutation::delete(0, current_a.clone()),
                Mutation::put(
                    1,
                    LogicalKey::in_keyspace(Keyspace::Meta, b"entity/z".to_vec()),
                    b"history-z".to_vec(),
                ),
            ],
        ),
    )
    .expect("delete/history Mapping commit");
    assert_eq!(
        block_on(mapping.multi_get(&[current_a, history_a])).expect("post-delete Mapping read"),
        vec![None, Some(b"history-a".to_vec())]
    );
    assert_eq!(
        mapping.applied_log_index().expect("Mapping applied index"),
        2
    );

    let empty = batch(3, 106, Vec::new());
    let receipt = apply(mapping, empty.clone()).expect("empty Mapping commit");
    assert_eq!(receipt.applied_log_index, 3);
    assert!(!receipt.duplicate);
    assert!(
        apply(mapping, empty)
            .expect("exact empty Mapping replay")
            .duplicate
    );
}

pub fn run_mapping_restore_tck(
    source: &dyn TemporalBackendMapping,
    destination: &dyn TemporalBackendMapping,
) {
    let (header, chunks, manifest, source_entries) = export(source);
    let mut restore =
        block_on(destination.restore_canonical(header)).expect("begin canonical Mapping restore");
    for chunk in chunks {
        block_on(restore.write_chunk(chunk)).expect("write canonical Mapping restore chunk");
    }
    block_on(restore.commit(manifest)).expect("commit canonical Mapping restore");

    let (_, _, _, destination_entries) = export(destination);
    assert_eq!(destination_entries, source_entries);
    assert_eq!(
        destination
            .applied_log_index()
            .expect("restored Mapping applied index"),
        source
            .applied_log_index()
            .expect("source Mapping applied index")
    );
    apply(
        destination,
        batch(
            4,
            105,
            vec![Mutation::put(
                0,
                LogicalKey::in_keyspace(Keyspace::TemporalIndex, b"continued".to_vec()),
                b"after-restore".to_vec(),
            )],
        ),
    )
    .expect("continue after canonical Mapping restore");
}

fn apply(
    mapping: &dyn TemporalBackendMapping,
    batch: CommittedMutationBatch,
) -> Result<ApplyReceipt, AdapterError> {
    let mut transaction = block_on(mapping.prepare(batch))?;
    if let Err(error) = block_on(transaction.apply()) {
        let _ = block_on(transaction.abort());
        return Err(error);
    }
    match block_on(transaction.commit()) {
        Ok(receipt) => Ok(receipt),
        Err(error) => {
            let _ = block_on(transaction.abort());
            Err(error)
        }
    }
}

type CanonicalExport = (
    crate::LogicalSnapshotHeaderV1,
    Vec<LogicalSnapshotChunkV1>,
    LogicalSnapshotManifestV1,
    Vec<(u8, Vec<u8>, Vec<u8>)>,
);

fn export(mapping: &dyn TemporalBackendMapping) -> CanonicalExport {
    let mut reader = block_on(mapping.export_canonical(
        LogicalSnapshotExportRequest::new(2, 4_096).expect("TCK snapshot limits"),
    ))
    .expect("begin canonical Mapping export");
    let header = reader.header().clone();
    let mut chunks = Vec::new();
    let mut entries = Vec::new();
    while let Some(chunk) = block_on(reader.next_chunk()).expect("read Mapping export chunk") {
        entries.extend(chunk.entries().iter().map(|entry| {
            (
                entry.key().keyspace().tag(),
                entry.key().as_bytes().to_vec(),
                entry.value().to_vec(),
            )
        }));
        chunks.push(chunk);
    }
    let manifest = block_on(reader.finish()).expect("finish canonical Mapping export");
    (header, chunks, manifest, entries)
}

fn batch(log_index: u64, txn_id: u128, mutations: Vec<Mutation>) -> CommittedMutationBatch {
    CommittedMutationBatch {
        shard_id: 3,
        log_index,
        txn_id,
        mutations,
    }
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
