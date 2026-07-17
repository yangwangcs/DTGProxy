use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use adapter_memory::MemoryAdapter;
use adapter_rocksdb::RocksAdapter;
use temporal_model::Timeline;
use temporal_storage::{
    CommitContext, ElementId, ElementRef, GraphId, LabelId, PartitionId, TemporalStore,
    VertexMutation,
};
use temporal_types::{CanonicalElement, GraphValue, Interval, TransactionTime, ValidTime};

const OPERATION_COUNT: u64 = 32;

fn tx(value: i64) -> TransactionTime {
    TransactionTime::new(value, 0)
}

fn valid(value: i64) -> ValidTime {
    ValidTime::from_micros(value)
}

fn interval(start: i64, end: i64) -> Interval<ValidTime> {
    Interval::new(valid(start), Some(valid(end))).unwrap()
}

fn vertex() -> ElementRef {
    ElementRef::vertex(GraphId::new(77), PartitionId::new(3), ElementId::new(9))
}

fn payload(operation: u64) -> CanonicalElement {
    CanonicalElement::new(
        1,
        BTreeMap::from([
            (1, GraphValue::Integer(operation as i64)),
            (2, GraphValue::String(format!("value-{operation}"))),
        ]),
    )
}

#[test]
fn fixed_seed_operations_match_model_memory_and_rocksdb_after_every_commit() {
    let directory = tempfile::tempdir().unwrap();
    let memory = TemporalStore::new(MemoryAdapter::new());
    let rocks = TemporalStore::new(RocksAdapter::open(directory.path()).unwrap());
    let label = LabelId::new(5);
    let mut model = Timeline::new();
    let mut seed = 0xd7_6a_5b_4c_3d_2e_1f_u64;
    let mut previous_commit = 0_i64;
    let mut commits = Vec::new();

    for operation in 1..=OPERATION_COUNT {
        let commit = i64::try_from(operation * 100).unwrap();
        let (changed, value) = if operation == 1 {
            (interval(0, 100), payload(operation))
        } else {
            let start = i64::try_from(next(&mut seed) % 90).unwrap();
            let width = i64::try_from((next(&mut seed) % 10) + 1).unwrap();
            (interval(start, start + width), payload(operation))
        };
        let context = CommitContext::new(
            3,
            operation,
            u128::from(operation),
            tx(previous_commit),
            tx(commit),
        );
        let mutation = VertexMutation::put(vertex(), label, changed, value.clone()).unwrap();

        block_on(memory.commit_vertex(context, mutation.clone())).unwrap();
        block_on(rocks.commit_vertex(context, mutation)).unwrap();
        if operation == 1 {
            model.put_initial(changed, value, tx(commit)).unwrap();
        } else {
            model
                .correct(changed, value, tx(previous_commit), tx(commit))
                .unwrap();
        }
        commits.push(commit);

        for point in (0..100).step_by(7) {
            let expected = model.value_at(valid(point), tx(commit)).unwrap().cloned();
            assert_eq!(
                block_on(memory.vertex_current(vertex(), valid(point))).unwrap(),
                expected
            );
            assert_eq!(
                block_on(rocks.vertex_current(vertex(), valid(point))).unwrap(),
                expected
            );
        }

        for snapshot in [commits[0], commits[commits.len() / 2], commit] {
            let point = i64::try_from(next(&mut seed) % 100).unwrap();
            let expected = model.value_at(valid(point), tx(snapshot)).unwrap().cloned();
            assert_eq!(
                block_on(memory.vertex_as_of(vertex(), valid(point), tx(snapshot))).unwrap(),
                expected
            );
            assert_eq!(
                block_on(rocks.vertex_as_of(vertex(), valid(point), tx(snapshot))).unwrap(),
                expected
            );
        }
        previous_commit = commit;
    }
}

fn next(seed: &mut u64) -> u64 {
    *seed = seed
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407);
    *seed
}

struct NoopWake;

impl Wake for NoopWake {
    fn wake(self: Arc<Self>) {}
}

fn block_on<F>(future: F) -> F::Output
where
    F: Future,
{
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
