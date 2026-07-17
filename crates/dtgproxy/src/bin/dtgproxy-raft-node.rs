#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::future::Future;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::thread;
use std::time::{Duration, Instant};

use raft_command::{ApplyPreparedV1, CommandBodyV1, CommandEnvelopeV1};
use raft_transport::{RaftTransportError, TcpRaftTransport};
use shard_runtime::DurableRaftReplica;
use storage_api::{Keyspace, LogicalKey, Mutation, PreparedMutationBatch, StorageAdapter};
use temporal_types::TransactionTime;

const VALUE_KEY: &[u8] = b"smoke/value";

fn main() -> ExitCode {
    match Arguments::parse(std::env::args().skip(1).collect()).and_then(|arguments| run(&arguments))
    {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("dtgproxy-raft-node error: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run(arguments: &Arguments) -> Result<(), String> {
    let voters: Vec<_> = arguments.peers.keys().copied().collect();
    if !voters.contains(&arguments.node_id) {
        return Err("peer map does not contain this node".to_owned());
    }
    std::fs::create_dir_all(&arguments.root).map_err(|error| error.to_string())?;
    let mut replica = block_on(DurableRaftReplica::open(
        arguments.node_id,
        &voters,
        arguments.shard_id,
        arguments.placement_epoch,
        arguments.root.join("raft"),
        arguments.root.join("adapter"),
    ))
    .map_err(|error| error.to_string())?;
    let mut transport = TcpRaftTransport::bind(
        arguments.shard_id,
        arguments.node_id,
        arguments.listen,
        arguments.peers.clone(),
    )
    .map_err(|error| error.to_string())?;
    transport.set_timeouts(Duration::from_millis(100), Duration::from_millis(250));

    let started = Instant::now();
    let campaign_at = started + arguments.campaign_delay;
    let deadline = started + arguments.timeout;
    let mut next_tick = started + arguments.tick_interval;
    let mut campaigned = false;
    let mut proposed = false;
    let mut observed_at = None;
    loop {
        if Instant::now() >= deadline {
            return Err(format!(
                "node {} timed out at applied index {}",
                arguments.node_id,
                replica.metadata().applied_index
            ));
        }

        for message in transport
            .receive_available()
            .map_err(|error| error.to_string())?
        {
            replica.step(message).map_err(|error| error.to_string())?;
        }
        drain_ready(&mut replica, &transport)?;

        let now = Instant::now();
        if arguments.campaign && !campaigned && now >= campaign_at {
            replica.campaign().map_err(|error| error.to_string())?;
            campaigned = true;
        }
        if arguments.campaign && replica.is_leader() && !proposed {
            replica
                .propose(arguments.request_id, arguments.command())
                .map_err(|error| error.to_string())?;
            proposed = true;
        }
        if now >= next_tick {
            replica.tick();
            next_tick = now + arguments.tick_interval;
        }
        drain_ready(&mut replica, &transport)?;

        if read_value(&replica)? == Some(arguments.value.clone()) {
            let first_observed = *observed_at.get_or_insert(now);
            if now.duration_since(first_observed) >= arguments.linger {
                println!(
                    "node={} applied={} value={}",
                    arguments.node_id,
                    replica.metadata().applied_index,
                    encode_hex(&arguments.value)
                );
                return Ok(());
            }
        }
        thread::sleep(Duration::from_millis(2));
    }
}

fn drain_ready(
    replica: &mut DurableRaftReplica,
    transport: &TcpRaftTransport,
) -> Result<(), String> {
    for _ in 0..256 {
        if !replica.has_ready() {
            return Ok(());
        }
        let messages = block_on(replica.process_ready()).map_err(|error| error.to_string())?;
        for message in messages {
            if let Err(error) = transport.send(&message)
                && !matches!(error, RaftTransportError::Io(_))
            {
                return Err(error.to_string());
            }
        }
    }
    Err("Ready loop exceeded 256 rounds".to_owned())
}

fn read_value(replica: &DurableRaftReplica) -> Result<Option<Vec<u8>>, String> {
    let key = LogicalKey::in_keyspace(Keyspace::Current, VALUE_KEY.to_vec());
    block_on(replica.adapter().multi_get(&[key]))
        .map_err(|error| error.to_string())
        .map(|mut values| values.pop().flatten())
}

struct Arguments {
    node_id: u64,
    shard_id: u32,
    placement_epoch: u64,
    listen: SocketAddr,
    peers: BTreeMap<u64, SocketAddr>,
    root: PathBuf,
    campaign: bool,
    request_id: u128,
    commit_ts: i64,
    value: Vec<u8>,
    campaign_delay: Duration,
    tick_interval: Duration,
    timeout: Duration,
    linger: Duration,
}

impl Arguments {
    fn parse(arguments: Vec<String>) -> Result<Self, String> {
        let mut values = BTreeMap::new();
        let mut campaign = false;
        let mut index = 0;
        while index < arguments.len() {
            let flag = &arguments[index];
            if flag == "--campaign" {
                campaign = true;
                index += 1;
                continue;
            }
            let value = arguments
                .get(index + 1)
                .ok_or_else(|| format!("missing value for {flag}"))?;
            if values.insert(flag.clone(), value.clone()).is_some() {
                return Err(format!("duplicate argument {flag}"));
            }
            index += 2;
        }
        Ok(Self {
            node_id: parse_required(&values, "--node-id")?,
            shard_id: parse_required(&values, "--shard-id")?,
            placement_epoch: parse_required(&values, "--epoch")?,
            listen: parse_required(&values, "--listen")?,
            peers: parse_peers(required(&values, "--peers")?)?,
            root: PathBuf::from(required(&values, "--root")?),
            campaign,
            request_id: parse_required(&values, "--request-id")?,
            commit_ts: parse_required(&values, "--commit-ts")?,
            value: decode_hex(required(&values, "--value-hex")?)?,
            campaign_delay: Duration::from_millis(parse_or(
                &values,
                "--campaign-delay-ms",
                250_u64,
            )?),
            tick_interval: Duration::from_millis(parse_or(&values, "--tick-ms", 50_u64)?),
            timeout: Duration::from_millis(parse_or(&values, "--timeout-ms", 15_000_u64)?),
            linger: Duration::from_millis(parse_or(&values, "--linger-ms", 750_u64)?),
        })
    }

    fn command(&self) -> Vec<u8> {
        CommandEnvelopeV1::new(
            self.shard_id,
            self.placement_epoch,
            self.request_id,
            CommandBodyV1::ApplyPrepared(ApplyPreparedV1 {
                commit_ts: TransactionTime::new(self.commit_ts, 0),
                batch: PreparedMutationBatch {
                    shard_id: self.shard_id,
                    txn_id: self.request_id + 1_000,
                    mutations: vec![Mutation::put(
                        0,
                        LogicalKey::in_keyspace(Keyspace::Current, VALUE_KEY.to_vec()),
                        self.value.clone(),
                    )],
                },
            }),
        )
        .encode()
        .expect("smoke command is bounded and canonical")
    }
}

fn required<'a>(values: &'a BTreeMap<String, String>, flag: &str) -> Result<&'a str, String> {
    values
        .get(flag)
        .map(String::as_str)
        .ok_or_else(|| format!("missing required argument {flag}"))
}

fn parse_required<T>(values: &BTreeMap<String, String>, flag: &str) -> Result<T, String>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    required(values, flag)?
        .parse()
        .map_err(|error| format!("invalid {flag}: {error}"))
}

fn parse_or<T>(values: &BTreeMap<String, String>, flag: &str, default: T) -> Result<T, String>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    values.get(flag).map_or(Ok(default), |value| {
        value
            .parse()
            .map_err(|error| format!("invalid {flag}: {error}"))
    })
}

fn parse_peers(value: &str) -> Result<BTreeMap<u64, SocketAddr>, String> {
    let mut peers = BTreeMap::new();
    for entry in value.split(',') {
        let (node, address) = entry
            .split_once('=')
            .ok_or_else(|| format!("invalid peer entry {entry}"))?;
        let node = node
            .parse()
            .map_err(|error| format!("invalid peer node {node}: {error}"))?;
        let address = address
            .parse()
            .map_err(|error| format!("invalid peer address {address}: {error}"))?;
        if peers.insert(node, address).is_some() {
            return Err(format!("duplicate peer node {node}"));
        }
    }
    if peers.is_empty() {
        return Err("peer map cannot be empty".to_owned());
    }
    Ok(peers)
}

fn decode_hex(value: &str) -> Result<Vec<u8>, String> {
    if !value.len().is_multiple_of(2) {
        return Err("hex value must contain an even number of digits".to_owned());
    }
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let high = hex_digit(pair[0])?;
            let low = hex_digit(pair[1])?;
            Ok((high << 4) | low)
        })
        .collect()
}

fn hex_digit(digit: u8) -> Result<u8, String> {
    match digit {
        b'0'..=b'9' => Ok(digit - b'0'),
        b'a'..=b'f' => Ok(digit - b'a' + 10),
        b'A'..=b'F' => Ok(digit - b'A' + 10),
        _ => Err(format!("invalid hex digit {}", char::from(digit))),
    }
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
            Poll::Pending => thread::park(),
        }
    }
}
