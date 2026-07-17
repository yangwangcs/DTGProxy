use std::collections::BTreeMap;
use std::future::Future;
use std::hint::black_box;
use std::path::Path;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::time::Instant;

use adapter_rocksdb::RocksAdapter;
use temporal_storage::{
    CommitContext, EdgeMutation, EdgeTypeId, ElementId, ElementRef, GraphId, LabelId, PartitionId,
    TemporalStore, VertexMutation,
};
use temporal_types::{CanonicalElement, GraphValue, Interval, TransactionTime, ValidTime};

fn main() {
    let iterations = std::env::var("DTGPROXY_BENCH_ITERS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(1_000_u64);
    let directory = tempfile::tempdir().expect("temporary benchmark directory");
    let store = TemporalStore::new(RocksAdapter::open(directory.path()).expect("open RocksDB"));
    let vertex = ElementRef::vertex(GraphId::new(1), PartitionId::new(0), ElementId::new(1));
    let label = LabelId::new(1);

    for index in 1..=32_u64 {
        let read = i64::try_from((index - 1) * 100).unwrap();
        let commit = i64::try_from(index * 100).unwrap();
        let start = if index == 1 {
            0
        } else {
            i64::try_from((index * 7) % 90).unwrap()
        };
        let end = if index == 1 { 100 } else { start + 10 };
        block_on(store.commit_vertex(
            context(index, read, commit),
            VertexMutation::put(vertex, label, interval(start, end), payload(index)).unwrap(),
        ))
        .unwrap();
    }

    measure("current_point_lookup", iterations, || {
        black_box(block_on(store.vertex_current(vertex, valid(50))).unwrap());
    });
    measure("as_of_latest_depth_1", iterations, || {
        black_box(block_on(store.vertex_as_of(vertex, valid(50), tx(3_200))).unwrap());
    });
    measure("as_of_oldest_depth_32", iterations, || {
        black_box(block_on(store.vertex_as_of(vertex, valid(50), tx(100))).unwrap());
    });

    let correction_start = Instant::now();
    for offset in 1..=8_u64 {
        let log_index = 32 + offset;
        let read = i64::try_from(3_100 + offset * 100).unwrap();
        let commit = read + 100;
        let start = i64::try_from(offset * 9).unwrap();
        block_on(
            store.commit_vertex(
                context(log_index, read, commit),
                VertexMutation::put(
                    vertex,
                    label,
                    interval(start, start + 4),
                    payload(log_index),
                )
                .unwrap(),
            ),
        )
        .unwrap();
    }
    report("retroactive_commit_sync", 8, correction_start.elapsed());

    let source = ElementId::new(10);
    let edge_type = EdgeTypeId::new(9);
    for degree in 0..32_u64 {
        let log_index = 41 + degree;
        let edge = ElementRef::edge(
            GraphId::new(1),
            PartitionId::new(0),
            ElementId::new(1_000 + u128::from(degree)),
        );
        block_on(
            store.commit_edge(
                context(log_index, 0, 10_000 + i64::try_from(degree).unwrap()),
                EdgeMutation::put(
                    edge,
                    edge_type,
                    source,
                    ElementId::new(2_000 + u128::from(degree)),
                    interval(0, 100),
                    payload(degree),
                )
                .unwrap(),
            ),
        )
        .unwrap();
    }
    measure("expand_out_degree_32", iterations, || {
        black_box(
            block_on(store.expand_out_current(
                GraphId::new(1),
                PartitionId::new(0),
                source,
                valid(50),
            ))
            .unwrap(),
        );
    });

    println!("rocksdb_bytes={}", directory_size(directory.path()));
    println!("iterations={iterations}");
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

fn measure(mut name: &str, iterations: u64, mut operation: impl FnMut()) {
    if iterations == 0 {
        name = "invalid_zero_iteration_benchmark";
    }
    let start = Instant::now();
    for _ in 0..iterations {
        operation();
    }
    report(name, iterations, start.elapsed());
}

fn report(name: &str, iterations: u64, elapsed: std::time::Duration) {
    let nanos = elapsed.as_nanos() / u128::from(iterations.max(1));
    println!("{name}_ns_per_op={nanos}");
}

fn directory_size(path: &Path) -> u64 {
    std::fs::read_dir(path)
        .into_iter()
        .flatten()
        .flatten()
        .map(|entry| {
            let path = entry.path();
            if path.is_dir() {
                directory_size(&path)
            } else {
                entry.metadata().map_or(0, |metadata| metadata.len())
            }
        })
        .sum()
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
