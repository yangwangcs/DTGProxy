use std::collections::BTreeMap;
use std::future::Future;
use std::net::{Ipv4Addr, SocketAddr, TcpListener};
use std::path::Path;
use std::process::{Command, Output, Stdio};
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use adapter_rocksdb::RocksAdapter;
use storage_api::{Keyspace, LogicalKey, StorageAdapter};

const SHARD_ID: u32 = 7;
const EPOCH: u64 = 9;
const VALUE_KEY: &[u8] = b"smoke/value";

#[test]
fn three_process_tcp_group_replicates_durably_then_restarts_under_a_new_leader() {
    let root = tempfile::tempdir().unwrap();
    let first = b"first-process-wave";
    let first_outputs = run_wave(root.path(), 1, 101, 100, first);
    assert_wave_succeeded(&first_outputs, first);

    let second = b"second-process-wave";
    let second_outputs = run_wave(root.path(), 2, 102, 200, second);
    assert_wave_succeeded(&second_outputs, second);

    for node_id in 1..=3 {
        let adapter =
            RocksAdapter::open(root.path().join(format!("node-{node_id}/adapter"))).unwrap();
        let key = LogicalKey::in_keyspace(Keyspace::Current, VALUE_KEY.to_vec());
        assert_eq!(
            block_on(adapter.multi_get(&[key])).unwrap().pop().flatten(),
            Some(second.to_vec())
        );
        assert!(adapter.applied_log_index().unwrap() >= 4);
    }
}

fn run_wave(
    root: &Path,
    campaign_node: u64,
    request_id: u128,
    commit_ts: i64,
    value: &[u8],
) -> BTreeMap<u64, Output> {
    let (reservations, addresses) = reserve_addresses();
    let peer_argument = addresses
        .iter()
        .map(|(node, address)| format!("{node}={address}"))
        .collect::<Vec<_>>()
        .join(",");
    drop(reservations);

    let mut children = BTreeMap::new();
    for (node_id, address) in &addresses {
        let mut command = Command::new(env!("CARGO_BIN_EXE_dtgproxy-raft-node"));
        command
            .args([
                "--node-id",
                &node_id.to_string(),
                "--shard-id",
                &SHARD_ID.to_string(),
                "--epoch",
                &EPOCH.to_string(),
                "--listen",
                &address.to_string(),
                "--peers",
                &peer_argument,
                "--root",
                &root.join(format!("node-{node_id}")).to_string_lossy(),
                "--request-id",
                &request_id.to_string(),
                "--commit-ts",
                &commit_ts.to_string(),
                "--value-hex",
                &encode_hex(value),
                "--campaign-delay-ms",
                "200",
                "--tick-ms",
                "50",
                "--timeout-ms",
                "15000",
                "--linger-ms",
                "1000",
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if *node_id == campaign_node {
            command.arg("--campaign");
        }
        children.insert(*node_id, command.spawn().unwrap());
    }

    children
        .into_iter()
        .map(|(node_id, child)| (node_id, child.wait_with_output().unwrap()))
        .collect()
}

fn assert_wave_succeeded(outputs: &BTreeMap<u64, Output>, value: &[u8]) {
    for (node_id, output) in outputs {
        assert!(
            output.status.success(),
            "node {node_id} failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(stdout.contains(&format!("node={node_id}")));
        assert!(stdout.contains(&format!("value={}", encode_hex(value))));
    }
}

fn reserve_addresses() -> (Vec<TcpListener>, BTreeMap<u64, SocketAddr>) {
    let mut listeners = Vec::new();
    let mut addresses = BTreeMap::new();
    for node_id in 1..=3 {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        addresses.insert(node_id, listener.local_addr().unwrap());
        listeners.push(listener);
    }
    (listeners, addresses)
}

fn encode_hex(value: &[u8]) -> String {
    value.iter().map(|byte| format!("{byte:02x}")).collect()
}

struct ThreadWaker(std::thread::Thread);

impl Wake for ThreadWaker {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

fn block_on<F: Future>(future: F) -> F::Output {
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
