use std::collections::BTreeMap;
use std::future::Future;
use std::hint::black_box;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use adapter_rocksdb::RocksAdapter;
use temporal_storage::{
    CommitContext, ElementId, ElementRef, GraphId, LabelId, PartitionId, TemporalStore,
    VertexMutation,
};
use temporal_types::{CanonicalElement, GraphValue, Interval, TransactionTime, ValidTime};

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

fn main() {
    let iterations = std::env::var("DTGPROXY_BENCH_ALLOC_ITERS")
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(1_u64);
    let directory = tempfile::tempdir().expect("temporary allocator benchmark directory");
    let store = TemporalStore::new(RocksAdapter::open(directory.path()).expect("open RocksDB"));
    let vertex = ElementRef::vertex(GraphId::new(1), PartitionId::new(0), ElementId::new(1));
    let label = LabelId::new(1);

    for index in 1..=1_008_u64 {
        let read = i64::try_from((index - 1) * 100).unwrap();
        let commit = i64::try_from(index * 100).unwrap();
        block_on(store.commit_vertex(
            context(index, read, commit),
            VertexMutation::put(vertex, label, interval(0, 100), payload(index)).unwrap(),
        ))
        .unwrap();
    }

    for (name, snapshot) in [
        ("as_of_replay_depth_0", 99_200_i64),
        ("as_of_replay_depth_1", 99_300_i64),
        ("as_of_replay_depth_8", 100_000_i64),
        ("as_of_replay_depth_15", 100_700_i64),
    ] {
        black_box(block_on(store.vertex_as_of(vertex, valid(50), tx(snapshot))).unwrap());
        measure_allocator_window(name, iterations, || {
            black_box(block_on(store.vertex_as_of(vertex, valid(50), tx(snapshot))).unwrap());
        });
    }

    println!("allocator_iterations={iterations}");
}

fn measure_allocator_window(name: &str, iterations: u64, mut operation: impl FnMut()) {
    let profiler = dhat::Profiler::builder()
        .testing()
        .trim_backtraces(Some(4))
        .build();
    for _ in 0..iterations {
        operation();
    }
    let stats = dhat::HeapStats::get();
    drop(profiler);

    println!(
        "{name}_allocator_total_bytes_per_op={}",
        stats.total_bytes / iterations
    );
    println!("{name}_allocator_peak_live_bytes={}", stats.max_bytes);
    println!("{name}_allocator_current_live_bytes={}", stats.curr_bytes);
    println!(
        "{name}_allocator_allocations_per_op={}",
        stats.total_blocks / iterations
    );
}

fn context(log_index: u64, read: i64, commit: i64) -> CommitContext {
    CommitContext::new(3, log_index, u128::from(log_index), tx(read), tx(commit))
}

fn tx(value: i64) -> TransactionTime {
    TransactionTime::new(value, 0)
}

fn valid(value: i64) -> ValidTime {
    ValidTime::from_micros(value)
}

fn interval(start: i64, end: i64) -> Interval<ValidTime> {
    Interval::new(valid(start), Some(valid(end))).unwrap()
}

fn payload(value: u64) -> CanonicalElement {
    CanonicalElement::new(
        1,
        BTreeMap::from([
            (1, GraphValue::Integer(i64::try_from(value).unwrap())),
            (2, GraphValue::String(format!("payload-{value}"))),
        ]),
    )
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
