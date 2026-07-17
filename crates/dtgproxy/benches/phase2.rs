use std::collections::BTreeMap;
use std::future::Future;
use std::hint::black_box;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::time::{Duration, Instant};

use adapter_memory::MemoryAdapter;
use adapter_rocksdb::RocksAdapter;
use dtgproxy::{DeploymentConfig, ShardPlacement};
use query_executor::ShardQueryExecutor;
use raft::eraftpb::{Entry, EntryType, HardState};
use raft_command::{ApplyPreparedV1, CommandBodyV1, CommandEnvelopeV1};
use raft_logstore::RocksRaftStorage;
use replica_snapshot::{create_snapshot_bundle, install_snapshot_bundle};
use shard_runtime::{DurableRaftReplica, InProcessShardGroup, ShardStateMachine};
use storage_api::{Keyspace, LogicalKey, Mutation, PreparedMutationBatch, StorageAdapter};
use temporal_ir::{GraphScope, PointOperator, TemporalPlan, TemporalSelector};
use temporal_storage::{
    ElementId, ElementRef, GraphId, LabelId, PartitionId, PrepareContext, TemporalStore,
    TemporalTransaction, VertexMutation,
};
use temporal_types::{CanonicalElement, GraphValue, Interval, TransactionTime, ValidTime};

const SHARD_ID: u32 = 7;
const EPOCH: u64 = 9;
const RF1: &[u64] = &[1];
const RF3: &[u64] = &[1, 2, 3];

fn main() {
    let iterations = env_iterations("DTGPROXY_PHASE2_BENCH_ITERS", 1_000);
    let snapshot_iterations = env_iterations("DTGPROXY_SNAPSHOT_BENCH_ITERS", 5);
    println!(
        "{{\"suite\":\"dtgproxy-phase2-v1\",\"release\":{},\"iterations\":{},\"snapshot_iterations\":{}}}",
        !cfg!(debug_assertions),
        iterations,
        snapshot_iterations
    );

    benchmark_proposals("rf1", RF1, iterations);
    benchmark_proposals("rf3", RF3, iterations);
    benchmark_raft_ticks("rf1", RF1, iterations);
    benchmark_raft_ticks("rf3", RF3, iterations);
    benchmark_closed_timestamp(RF3, iterations);
    benchmark_follower_snapshot_read(iterations);
    benchmark_routing(iterations.saturating_mul(100));
    benchmark_snapshot_catchup(snapshot_iterations);
}

fn benchmark_proposals(label: &str, voters: &[u64], iterations: usize) {
    let mut group = block_on(InProcessShardGroup::new(SHARD_ID, EPOCH, voters)).unwrap();
    block_on(group.elect(voters[0])).unwrap();
    let mut end_to_end = Sample::new(format!("{label}_proposal_commit_apply"));
    let mut proposal_to_commit = Sample::new(format!("{label}_proposal_to_commit_observed"));
    let mut commit_to_apply = Sample::new(format!("{label}_commit_to_apply"));
    for ordinal in 0..iterations {
        let sequence = u64::try_from(ordinal).unwrap().saturating_add(1);
        let request_id = u128::from(sequence);
        let command = plain_command(
            request_id,
            i64::try_from(sequence).unwrap(),
            format!("bench/{sequence:020}").into_bytes(),
            vec![b'x'; 64],
        );
        let started = Instant::now();
        let receipt = block_on(group.propose_and_wait(command, 20)).unwrap();
        end_to_end.record(started.elapsed());
        proposal_to_commit.record(receipt.proposal_to_commit);
        commit_to_apply.record(receipt.commit_to_apply);
    }
    end_to_end.print();
    proposal_to_commit.print();
    commit_to_apply.print();
}

fn benchmark_raft_ticks(label: &str, voters: &[u64], iterations: usize) {
    let mut group = block_on(InProcessShardGroup::new(SHARD_ID, EPOCH, voters)).unwrap();
    block_on(group.elect(voters[0])).unwrap();
    let mut sample = Sample::new(format!("{label}_raft_tick_and_ready_drive"));
    for _ in 0..iterations {
        let started = Instant::now();
        block_on(group.drive_ticks(1)).unwrap();
        sample.record(started.elapsed());
    }
    sample.print();
}

fn benchmark_closed_timestamp(voters: &[u64], iterations: usize) {
    let mut group = block_on(InProcessShardGroup::new(SHARD_ID, EPOCH, voters)).unwrap();
    block_on(group.elect(voters[0])).unwrap();
    let mut sample = Sample::new("rf3_closed_timestamp_replicated_tick");
    for ordinal in 0..iterations {
        let closed = i64::try_from(ordinal).unwrap().saturating_add(1);
        let started = Instant::now();
        block_on(group.advance_closed_timestamp(tx(closed), 20)).unwrap();
        sample.record(started.elapsed());
    }
    sample.print();
}

fn benchmark_follower_snapshot_read(iterations: usize) {
    let mut group = block_on(InProcessShardGroup::new(SHARD_ID, EPOCH, RF3)).unwrap();
    block_on(group.elect(1)).unwrap();
    let planner = TemporalStore::new(MemoryAdapter::new());
    let vertex = ElementRef::vertex(
        GraphId::new(1),
        PartitionId::new(SHARD_ID),
        ElementId::new(42),
    );
    let prepared = block_on(
        planner.prepare_transaction(
            PrepareContext::new(SHARD_ID, 10_000, tx(0), tx(100)),
            TemporalTransaction::new().with_vertex(
                VertexMutation::put(
                    vertex,
                    LabelId::new(1),
                    Interval::new(ValidTime::from_micros(0), None).unwrap(),
                    CanonicalElement::new(
                        1,
                        BTreeMap::from([(1, GraphValue::String("benchmark".to_owned()))]),
                    ),
                )
                .unwrap(),
            ),
        ),
    )
    .unwrap();
    block_on(
        planner
            .adapter()
            .apply_committed(prepared.clone().commit_at(1)),
    )
    .unwrap();
    let command = CommandEnvelopeV1::new(
        SHARD_ID,
        EPOCH,
        10_001,
        CommandBodyV1::ApplyPrepared(ApplyPreparedV1 {
            commit_ts: tx(100),
            batch: prepared,
        }),
    )
    .encode()
    .unwrap();
    block_on(group.propose_and_wait(command, 20)).unwrap();
    block_on(group.advance_closed_timestamp(tx(100), 20)).unwrap();
    let plan = TemporalPlan::point(
        GraphScope::new(GraphId::new(1), PartitionId::new(SHARD_ID)),
        PointOperator::VertexById(ElementId::new(42)),
        ValidTime::from_micros(1),
        TemporalSelector::AsOf(tx(100)),
        1,
    );

    let mut sample = Sample::new("rf3_follower_readindex_and_temporal_point_read");
    for _ in 0..iterations {
        let started = Instant::now();
        let proof = block_on(group.issue_follower_read_proof(EPOCH, 5)).unwrap();
        let result = block_on(ShardQueryExecutor::execute_follower(
            &group, 2, EPOCH, &proof, &plan,
        ))
        .unwrap();
        assert_eq!(result.records().len(), 1);
        sample.record(started.elapsed());
    }
    sample.print();
}

fn benchmark_routing(iterations: usize) {
    let primary =
        DeploymentConfig::primary_replica(ShardPlacement::new(1, 1, vec![1, 2, 3]).unwrap());
    let shared = DeploymentConfig::shared_nothing(
        0x4454_4750,
        (1_u32..=128)
            .map(|shard_id| ShardPlacement::new(shard_id, 1, vec![1, 2, 3]).unwrap())
            .collect(),
    )
    .unwrap();
    let mut primary_sample = Sample::new("primary_replica_route_scope");
    let mut shared_sample = Sample::new("shared_nothing_128_shard_rendezvous_route_scope");
    for ordinal in 0..iterations {
        let partition = u32::try_from(ordinal % 65_536).unwrap();
        let scope = GraphScope::new(GraphId::new(9), PartitionId::new(partition));
        let started = Instant::now();
        black_box(primary.route_scope(black_box(scope)));
        primary_sample.record(started.elapsed());
        let started = Instant::now();
        black_box(shared.route_scope(black_box(scope)));
        shared_sample.record(started.elapsed());
    }
    primary_sample.print();
    shared_sample.print();
}

fn benchmark_snapshot_catchup(iterations: usize) {
    let root = tempfile::tempdir().unwrap();
    let source_path = root.path().join("source");
    let bundle_path = root.path().join("bundle-at-32");
    let mut source = block_on(ShardStateMachine::open(
        RocksAdapter::open(&source_path).unwrap(),
        SHARD_ID,
        EPOCH,
    ))
    .unwrap();
    block_on(source.apply_noop_entry(1, 1)).unwrap();
    for index in 2_u64..=32 {
        let command = plain_command(
            u128::from(index),
            i64::try_from(index).unwrap(),
            format!("snapshot/{index:020}").into_bytes(),
            vec![b's'; 256],
        );
        block_on(source.apply_entry(1, index, &command)).unwrap();
    }
    create_snapshot_bundle(&source, RF3, &bundle_path).unwrap();

    let mut suffix = Vec::new();
    for index in 33_u64..=48 {
        let command = plain_command(
            u128::from(index),
            i64::try_from(index).unwrap(),
            format!("suffix/{index:020}").into_bytes(),
            vec![b't'; 256],
        );
        block_on(source.apply_entry(2, index, &command)).unwrap();
        suffix.push(Entry {
            entry_type: EntryType::EntryNormal.into(),
            term: 2,
            index,
            data: command,
            ..Default::default()
        });
    }

    let mut sample = Sample::new("snapshot_install_and_16_entry_suffix_catchup");
    for ordinal in 0..iterations {
        let destination = root.path().join(format!("installed-{ordinal}"));
        let started = Instant::now();
        let installed = block_on(install_snapshot_bundle(&bundle_path, &destination)).unwrap();
        let storage = RocksRaftStorage::open(&installed.raft_wal_path, RF3).unwrap();
        storage
            .persist_ready(
                None,
                &suffix,
                Some(&HardState {
                    term: 2,
                    commit: 48,
                    ..Default::default()
                }),
            )
            .unwrap();
        drop(storage);
        let mut restored = block_on(DurableRaftReplica::open(
            2,
            RF3,
            SHARD_ID,
            EPOCH,
            &installed.raft_wal_path,
            &installed.adapter_path,
        ))
        .unwrap();
        block_on(drain(&mut restored));
        assert_eq!(restored.metadata().applied_index, 48);
        sample.record(started.elapsed());
    }
    sample.print();
}

async fn drain(replica: &mut DurableRaftReplica) {
    for _ in 0..100 {
        if !replica.has_ready() {
            return;
        }
        let _messages = replica.process_ready().await.unwrap();
    }
    panic!("durable Replica Ready loop did not quiesce");
}

fn plain_command(request_id: u128, commit: i64, key: Vec<u8>, value: Vec<u8>) -> Vec<u8> {
    CommandEnvelopeV1::new(
        SHARD_ID,
        EPOCH,
        request_id,
        CommandBodyV1::ApplyPrepared(ApplyPreparedV1 {
            commit_ts: tx(commit),
            batch: PreparedMutationBatch {
                shard_id: SHARD_ID,
                txn_id: request_id.saturating_add(10_000),
                mutations: vec![Mutation::put(
                    0,
                    LogicalKey::in_keyspace(Keyspace::Current, key),
                    value,
                )],
            },
        }),
    )
    .encode()
    .unwrap()
}

const fn tx(value: i64) -> TransactionTime {
    TransactionTime::new(value, 0)
}

fn env_iterations(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

struct Sample {
    name: String,
    nanoseconds: Vec<u128>,
}

impl Sample {
    fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            nanoseconds: Vec::new(),
        }
    }

    fn record(&mut self, elapsed: Duration) {
        self.nanoseconds.push(elapsed.as_nanos());
    }

    fn print(mut self) {
        self.nanoseconds.sort_unstable();
        let count = self.nanoseconds.len();
        let sum = self.nanoseconds.iter().copied().sum::<u128>();
        let mean = sum / u128::try_from(count).unwrap();
        println!(
            "{{\"name\":\"{}\",\"iterations\":{},\"mean_ns\":{},\"p50_ns\":{},\"p95_ns\":{},\"p99_ns\":{},\"max_ns\":{}}}",
            self.name,
            count,
            mean,
            percentile(&self.nanoseconds, 50),
            percentile(&self.nanoseconds, 95),
            percentile(&self.nanoseconds, 99),
            self.nanoseconds[count - 1]
        );
    }
}

fn percentile(sorted: &[u128], percentile: usize) -> u128 {
    let rank = sorted.len().saturating_mul(percentile).div_ceil(100);
    sorted[rank.saturating_sub(1)]
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
