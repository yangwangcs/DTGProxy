use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use adapter_memory::MemoryAdapter;
use adapter_rocksdb::RocksAdapter;
use raft::eraftpb::{Entry, EntryType, HardState};
use raft_command::{ApplyPreparedV1, CommandBodyV1, CommandEnvelopeV1};
use raft_logstore::RocksRaftStorage;
use replica_snapshot::{create_snapshot_bundle, install_snapshot_bundle};
use shard_runtime::{DurableRaftReplica, DurableReplicaError, ShardStateMachine};
use storage_api::{KeySpan, Keyspace, StorageAdapter};
use temporal_storage::{
    EdgeMutation, EdgeTypeId, ElementId, ElementRef, GraphId, LabelId, PartitionId, PrepareContext,
    TemporalStore, TemporalTransaction, VertexMutation,
};
use temporal_types::{CanonicalElement, GraphValue, Interval, TransactionTime, ValidTime};

#[test]
fn snapshot_suffix_recovery_preserves_complete_bitemporal_graph_semantics() {
    let root = tempfile::tempdir().unwrap();
    let source_path = root.path().join("source");
    let bundle_path = root.path().join("bundle-at-2");
    let installed_path = root.path().join("follower-generation");
    let planner = TemporalStore::new(MemoryAdapter::new());
    let mut source = block_on(ShardStateMachine::open(
        RocksAdapter::open(&source_path).unwrap(),
        7,
        9,
    ))
    .unwrap();

    let initial = TemporalTransaction::new()
        .with_vertex(
            VertexMutation::put(
                vertex(1),
                LabelId::new(1),
                interval(1, Some(10)),
                payload("v1"),
            )
            .unwrap(),
        )
        .with_vertex(
            VertexMutation::put(
                vertex(2),
                LabelId::new(1),
                interval(1, Some(10)),
                payload("v2"),
            )
            .unwrap(),
        )
        .with_edge(
            EdgeMutation::put(
                edge(),
                EdgeTypeId::new(5),
                ElementId::new(1),
                ElementId::new(2),
                interval(2, Some(9)),
                payload("e1"),
            )
            .unwrap(),
        );
    block_on(prepare_apply(
        &planner,
        &mut source,
        1,
        1,
        101,
        0,
        100,
        initial,
    ));

    let correction = TemporalTransaction::new()
        .with_vertex(
            VertexMutation::put(
                vertex(1),
                LabelId::new(1),
                interval(4, Some(7)),
                payload("v1-corrected"),
            )
            .unwrap(),
        )
        .with_edge(
            EdgeMutation::put(
                edge(),
                EdgeTypeId::new(5),
                ElementId::new(1),
                ElementId::new(2),
                interval(4, Some(7)),
                payload("e-corrected"),
            )
            .unwrap(),
        );
    block_on(prepare_apply(
        &planner,
        &mut source,
        1,
        2,
        102,
        100,
        200,
        correction,
    ));
    create_snapshot_bundle(&source, &[1, 2, 3], &bundle_path).unwrap();

    let coordinated_delete = TemporalTransaction::new()
        .with_vertex(
            VertexMutation::delete(vertex(1), LabelId::new(1), interval(5, Some(6))).unwrap(),
        )
        .with_edge(
            EdgeMutation::delete(
                edge(),
                EdgeTypeId::new(5),
                ElementId::new(1),
                ElementId::new(2),
                interval(5, Some(6)),
            )
            .unwrap(),
        );
    let suffix = block_on(prepare_apply(
        &planner,
        &mut source,
        2,
        3,
        103,
        200,
        300,
        coordinated_delete,
    ));

    let installed = block_on(install_snapshot_bundle(&bundle_path, &installed_path)).unwrap();
    let storage = RocksRaftStorage::open(&installed.raft_wal_path, &[1, 2, 3]).unwrap();
    storage
        .persist_ready(
            None,
            &[Entry {
                entry_type: EntryType::EntryNormal.into(),
                term: 2,
                index: 3,
                data: suffix,
                ..Default::default()
            }],
            Some(&HardState {
                term: 2,
                commit: 3,
                ..Default::default()
            }),
        )
        .unwrap();
    drop(storage);

    let mut restored = block_on(DurableRaftReplica::open(
        2,
        &[1, 2, 3],
        7,
        9,
        &installed.raft_wal_path,
        &installed.adapter_path,
    ))
    .unwrap();
    block_on(drain(&mut restored)).unwrap();
    assert_eq!(restored.metadata(), source.metadata());
    drop(restored);
    drop(source);

    let source_store = TemporalStore::new(RocksAdapter::open(&source_path).unwrap());
    let restored_store = TemporalStore::new(RocksAdapter::open(&installed.adapter_path).unwrap());
    assert_temporal_queries_equal(&source_store, &restored_store);
    assert_all_logical_rows_equal(source_store.adapter(), restored_store.adapter());
}

#[allow(clippy::too_many_arguments)]
async fn prepare_apply(
    planner: &TemporalStore<MemoryAdapter>,
    machine: &mut ShardStateMachine<RocksAdapter>,
    term: u64,
    index: u64,
    request_id: u128,
    read: i64,
    commit: i64,
    transaction: TemporalTransaction,
) -> Vec<u8> {
    let prepared = planner
        .prepare_transaction(
            PrepareContext::new(7, request_id + 1_000, tx(read), tx(commit)),
            transaction,
        )
        .await
        .unwrap();
    planner
        .adapter()
        .apply_committed(prepared.clone().commit_at(index))
        .await
        .unwrap();
    let command = CommandEnvelopeV1::new(
        7,
        9,
        request_id,
        CommandBodyV1::ApplyPrepared(ApplyPreparedV1 {
            commit_ts: tx(commit),
            batch: prepared,
        }),
    )
    .encode()
    .unwrap();
    machine.apply_entry(term, index, &command).await.unwrap();
    command
}

fn assert_temporal_queries_equal(
    source: &TemporalStore<RocksAdapter>,
    restored: &TemporalStore<RocksAdapter>,
) {
    for valid_micros in [3, 4, 5, 6, 8] {
        let valid = valid(valid_micros);
        assert_eq!(
            block_on(source.vertex_current(vertex(1), valid)),
            block_on(restored.vertex_current(vertex(1), valid))
        );
        assert_eq!(
            block_on(source.edge_current(edge(), valid)),
            block_on(restored.edge_current(edge(), valid))
        );
        assert_eq!(
            block_on(source.expand_out_current(graph(), partition(), ElementId::new(1), valid)),
            block_on(restored.expand_out_current(graph(), partition(), ElementId::new(1), valid,))
        );
        assert_eq!(
            block_on(source.expand_in_current(graph(), partition(), ElementId::new(2), valid)),
            block_on(restored.expand_in_current(graph(), partition(), ElementId::new(2), valid,))
        );
        for transaction in [150, 250, 350] {
            assert_eq!(
                block_on(source.vertex_as_of(vertex(1), valid, tx(transaction))),
                block_on(restored.vertex_as_of(vertex(1), valid, tx(transaction)))
            );
            assert_eq!(
                block_on(source.edge_as_of(edge(), valid, tx(transaction))),
                block_on(restored.edge_as_of(edge(), valid, tx(transaction)))
            );
            assert_eq!(
                block_on(source.expand_out_as_of(
                    graph(),
                    partition(),
                    ElementId::new(1),
                    valid,
                    tx(transaction),
                )),
                block_on(restored.expand_out_as_of(
                    graph(),
                    partition(),
                    ElementId::new(1),
                    valid,
                    tx(transaction),
                ))
            );
        }
    }
    assert_eq!(
        block_on(source.diff_vertex(vertex(1), tx(150), tx(350))),
        block_on(restored.diff_vertex(vertex(1), tx(150), tx(350)))
    );
    assert_eq!(
        block_on(source.diff_edge(edge(), tx(150), tx(350))),
        block_on(restored.diff_edge(edge(), tx(150), tx(350)))
    );
}

fn assert_all_logical_rows_equal(source: &RocksAdapter, restored: &RocksAdapter) {
    for keyspace in Keyspace::ALL {
        let span = KeySpan::range(keyspace, Vec::new(), None).unwrap();
        assert_eq!(
            block_on(source.scan(&span)).unwrap(),
            block_on(restored.scan(&span)).unwrap(),
            "keyspace {keyspace:?} differs"
        );
    }
}

async fn drain(replica: &mut DurableRaftReplica) -> Result<(), DurableReplicaError> {
    for _ in 0..100 {
        if !replica.has_ready() {
            return Ok(());
        }
        let _messages = replica.process_ready().await?;
    }
    panic!("Ready loop did not quiesce");
}

fn graph() -> GraphId {
    GraphId::new(1)
}

fn partition() -> PartitionId {
    PartitionId::new(0)
}

fn vertex(id: u128) -> ElementRef {
    ElementRef::vertex(graph(), partition(), ElementId::new(id))
}

fn edge() -> ElementRef {
    ElementRef::edge(graph(), partition(), ElementId::new(10))
}

fn tx(value: i64) -> TransactionTime {
    TransactionTime::new(value, 0)
}

fn valid(value: i64) -> ValidTime {
    ValidTime::from_micros(value)
}

fn interval(start: i64, end: Option<i64>) -> Interval<ValidTime> {
    Interval::new(valid(start), end.map(valid)).unwrap()
}

fn payload(value: &str) -> CanonicalElement {
    CanonicalElement::new(
        1,
        BTreeMap::from([(1, GraphValue::String(value.to_owned()))]),
    )
}

fn block_on<F: Future>(future: F) -> F::Output {
    struct ThreadWaker(std::thread::Thread);

    impl Wake for ThreadWaker {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.unpark();
        }
    }

    let waker = Waker::from(Arc::new(ThreadWaker(std::thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = Box::pin(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => std::thread::park(),
        }
    }
}
