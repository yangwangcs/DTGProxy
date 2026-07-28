use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cluster_protocol::CLUSTER_PROTOCOL_VERSION;
use cluster_protocol::proto::meta_service_client::MetaServiceClient;
use cluster_protocol::proto::{AllocateTimestampRequest, RequestContext};
use temporal_types::TransactionTime;
use timestamp_oracle::advance_timestamp;
use tokio::sync::{mpsc, oneshot};
use tonic::transport::{Channel, Endpoint};

const MAX_READ_BATCH_SIZE: usize = 64;

#[derive(Clone)]
pub(crate) struct MetaTimestampClient {
    cluster_id: [u8; 16],
    clients: Arc<Vec<MetaServiceClient<Channel>>>,
    preferred_endpoint: Arc<AtomicUsize>,
    read_requests: mpsc::Sender<ReadSnapshotRequest>,
}

impl MetaTimestampClient {
    pub(crate) fn new(
        cluster_id: [u8; 16],
        endpoints: Vec<SocketAddr>,
        maximum_pending_reads: usize,
    ) -> Result<Self, MetaTimestampClientError> {
        if cluster_id == [0; 16] || endpoints.is_empty() || maximum_pending_reads == 0 {
            return Err(MetaTimestampClientError::InvalidConfiguration);
        }
        let clients = endpoints
            .into_iter()
            .map(|endpoint| {
                Endpoint::from_shared(format!("http://{endpoint}"))
                    .map(|endpoint| MetaServiceClient::new(endpoint.connect_lazy()))
                    .map_err(|error| MetaTimestampClientError::Protocol(error.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let clients = Arc::new(clients);
        let preferred_endpoint = Arc::new(AtomicUsize::new(0));
        let (read_requests, receiver) = mpsc::channel(maximum_pending_reads);
        tokio::spawn(run_read_broker(
            cluster_id,
            Arc::clone(&clients),
            Arc::clone(&preferred_endpoint),
            receiver,
        ));
        Ok(Self {
            cluster_id,
            clients,
            preferred_endpoint,
            read_requests,
        })
    }

    pub(crate) async fn allocate_read_snapshot(
        &self,
        request_id: u128,
        deadline_unix_ms: u64,
    ) -> Result<TransactionTime, MetaTimestampClientError> {
        let (response, receiver) = oneshot::channel();
        let request = ReadSnapshotRequest {
            request_id,
            deadline_unix_ms,
            response,
        };
        tokio::time::timeout(
            remaining_duration(deadline_unix_ms)?,
            self.read_requests.send(request),
        )
        .await
        .map_err(|_| MetaTimestampClientError::DeadlineExceeded)?
        .map_err(|_| MetaTimestampClientError::Unavailable("read broker stopped".into()))?;
        tokio::time::timeout(remaining_duration(deadline_unix_ms)?, receiver)
            .await
            .map_err(|_| MetaTimestampClientError::DeadlineExceeded)?
            .map_err(|_| MetaTimestampClientError::Unavailable("read broker stopped".into()))?
    }

    pub(crate) async fn allocate_transaction_timestamps(
        &self,
        request_id: u128,
        deadline_unix_ms: u64,
    ) -> Result<(TransactionTime, TransactionTime), MetaTimestampClientError> {
        let start = allocate_exact(
            self.cluster_id,
            self.clients.as_ref(),
            self.preferred_endpoint.as_ref(),
            request_id,
            deadline_unix_ms,
            3,
        )
        .await?;
        let commit = advance_timestamp(start, 2)
            .map_err(|error| MetaTimestampClientError::Protocol(error.to_string()))?;
        Ok((start, commit))
    }
}

struct ReadSnapshotRequest {
    request_id: u128,
    deadline_unix_ms: u64,
    response: oneshot::Sender<Result<TransactionTime, MetaTimestampClientError>>,
}

async fn run_read_broker(
    cluster_id: [u8; 16],
    clients: Arc<Vec<MetaServiceClient<Channel>>>,
    preferred_endpoint: Arc<AtomicUsize>,
    mut receiver: mpsc::Receiver<ReadSnapshotRequest>,
) {
    while let Some(first) = receiver.recv().await {
        let mut batch = Vec::with_capacity(MAX_READ_BATCH_SIZE);
        if !first.response.is_closed() {
            batch.push(first);
        }
        tokio::task::yield_now().await;
        while batch.len() < MAX_READ_BATCH_SIZE {
            let Ok(request) = receiver.try_recv() else {
                break;
            };
            if !request.response.is_closed() {
                batch.push(request);
            }
        }
        let now = match unix_time_ms() {
            Ok(now) => now,
            Err(error) => {
                send_batch_error(batch, error);
                continue;
            }
        };
        let mut active = Vec::with_capacity(batch.len());
        for request in batch {
            if request.deadline_unix_ms <= now {
                let _ = request
                    .response
                    .send(Err(MetaTimestampClientError::DeadlineExceeded));
            } else {
                active.push(request);
            }
        }
        if active.is_empty() {
            continue;
        }
        let deadline_unix_ms = active
            .iter()
            .map(|request| request.deadline_unix_ms)
            .min()
            .expect("nonempty read batch");
        let request_id = active[0].request_id;
        let count = u32::try_from(active.len()).expect("bounded read batch fits u32");
        match allocate_exact(
            cluster_id,
            clients.as_ref(),
            preferred_endpoint.as_ref(),
            request_id,
            deadline_unix_ms,
            count,
        )
        .await
        {
            Ok(first) => {
                for (offset, request) in active.into_iter().enumerate() {
                    let result = u32::try_from(offset)
                        .map_err(|_| {
                            MetaTimestampClientError::Protocol(
                                "read timestamp batch offset overflowed".into(),
                            )
                        })
                        .and_then(|offset| {
                            advance_timestamp(first, offset).map_err(|error| {
                                MetaTimestampClientError::Protocol(error.to_string())
                            })
                        });
                    let _ = request.response.send(result);
                }
            }
            Err(error) => send_batch_error(active, error),
        }
    }
}

async fn allocate_exact(
    cluster_id: [u8; 16],
    clients: &[MetaServiceClient<Channel>],
    preferred_endpoint: &AtomicUsize,
    request_id: u128,
    deadline_unix_ms: u64,
    count: u32,
) -> Result<TransactionTime, MetaTimestampClientError> {
    let start = preferred_endpoint.load(Ordering::Relaxed) % clients.len();
    let mut last_error = None;
    for offset in 0..clients.len() {
        let index = (start + offset) % clients.len();
        let mut client = clients[index].clone();
        let request = AllocateTimestampRequest {
            context: Some(RequestContext {
                protocol_version: CLUSTER_PROTOCOL_VERSION,
                cluster_id: cluster_id.to_vec(),
                request_id: request_id.to_be_bytes().to_vec(),
                deadline_unix_ms,
            }),
            count,
            observed_physical_ms: unix_time_ms()?,
        };
        let response = tokio::time::timeout(
            remaining_duration(deadline_unix_ms)?,
            client.allocate_timestamp(request),
        )
        .await;
        match response {
            Ok(Ok(response)) => {
                let response = response.into_inner();
                if response.count != count {
                    return Err(MetaTimestampClientError::Protocol(format!(
                        "Meta returned timestamp count {}, expected {count}",
                        response.count
                    )));
                }
                let physical_micros = response
                    .first_physical_ms
                    .checked_mul(1_000)
                    .and_then(|value| i64::try_from(value).ok())
                    .ok_or_else(|| {
                        MetaTimestampClientError::Protocol(
                            "Meta timestamp physical value overflowed".into(),
                        )
                    })?;
                preferred_endpoint.store(index, Ordering::Relaxed);
                return Ok(TransactionTime::new(
                    physical_micros,
                    response.first_logical,
                ));
            }
            Ok(Err(status)) => last_error = Some(status.to_string()),
            Err(_) => last_error = Some("Meta timestamp allocation timed out".into()),
        }
    }
    Err(MetaTimestampClientError::Unavailable(
        last_error.unwrap_or_else(|| "no Meta endpoint was reachable".into()),
    ))
}

fn send_batch_error(batch: Vec<ReadSnapshotRequest>, error: MetaTimestampClientError) {
    for request in batch {
        let _ = request.response.send(Err(error.clone()));
    }
}

fn remaining_duration(deadline_unix_ms: u64) -> Result<Duration, MetaTimestampClientError> {
    let remaining_ms = deadline_unix_ms
        .checked_sub(unix_time_ms()?)
        .filter(|remaining| *remaining > 0)
        .ok_or(MetaTimestampClientError::DeadlineExceeded)?;
    Ok(Duration::from_millis(remaining_ms))
}

fn unix_time_ms() -> Result<u64, MetaTimestampClientError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| MetaTimestampClientError::Protocol(error.to_string()))?;
    u64::try_from(elapsed.as_millis())
        .map_err(|_| MetaTimestampClientError::Protocol("wall clock overflowed".into()))
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum MetaTimestampClientError {
    InvalidConfiguration,
    DeadlineExceeded,
    Unavailable(String),
    Protocol(String),
}

impl Display for MetaTimestampClientError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfiguration => {
                formatter.write_str("invalid Meta timestamp client configuration")
            }
            Self::DeadlineExceeded => {
                formatter.write_str("Meta timestamp allocation deadline exceeded")
            }
            Self::Unavailable(message) => {
                write!(formatter, "Meta timestamp service unavailable: {message}")
            }
            Self::Protocol(message) => {
                write!(formatter, "Meta timestamp protocol error: {message}")
            }
        }
    }
}

impl Error for MetaTimestampClientError {}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    use cluster_protocol::proto::meta_service_server::MetaServiceServer;
    use meta_node::{MetaNodeService, MetaRaftReplica, ReplicatedTso};
    use timestamp_oracle::{ManualClock, advance_timestamp};
    use tokio::sync::{Barrier, Mutex};
    use tokio::task::JoinSet;
    use tokio_stream::StreamExt;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::Server;

    use super::MetaTimestampClient;

    const CLUSTER_ID: [u8; 16] = [0x6d; 16];

    fn now_ms() -> u64 {
        u64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock after Unix epoch")
                .as_millis(),
        )
        .expect("wall clock fits u64 milliseconds")
    }

    fn elected_meta(root: &std::path::Path) -> MetaNodeService {
        let mut replica =
            MetaRaftReplica::open(1, &[1], root.join("raft"), root.join("state")).unwrap();
        replica.campaign().unwrap();
        for _ in 0..32 {
            assert!(replica.drain_ready().unwrap().is_empty());
            if replica.is_leader() {
                break;
            }
            replica.tick();
        }
        assert!(replica.is_leader());
        MetaNodeService::new(
            CLUSTER_ID,
            Arc::new(Mutex::new(replica)),
            Arc::new(
                ReplicatedTso::new(Arc::new(ManualClock::new(1_000_000)), 128, 1_000_000).unwrap(),
            ),
        )
    }

    async fn serve_meta(
        meta: MetaNodeService,
        rpc_count: Arc<AtomicUsize>,
        connection_count: Arc<AtomicUsize>,
    ) -> (
        std::net::SocketAddr,
        tokio::sync::oneshot::Sender<()>,
        tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (shutdown, shutdown_rx) = tokio::sync::oneshot::channel();
        let service = MetaServiceServer::with_interceptor(meta, move |request| {
            rpc_count.fetch_add(1, Ordering::Relaxed);
            Ok(request)
        });
        let incoming = TcpListenerStream::new(listener).map(move |connection| {
            if connection.is_ok() {
                connection_count.fetch_add(1, Ordering::Relaxed);
            }
            connection
        });
        let server = tokio::spawn(
            Server::builder()
                .add_service(service)
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = shutdown_rx.await;
                }),
        );
        (address, shutdown, server)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sixty_four_concurrent_read_snapshots_share_timestamp_rpcs() {
        let temporary = tempfile::tempdir().unwrap();
        let rpc_count = Arc::new(AtomicUsize::new(0));
        let connection_count = Arc::new(AtomicUsize::new(0));
        let (address, shutdown, server) = serve_meta(
            elected_meta(temporary.path()),
            Arc::clone(&rpc_count),
            connection_count,
        )
        .await;
        let client = MetaTimestampClient::new(CLUSTER_ID, vec![address], 64).unwrap();
        let barrier = Arc::new(Barrier::new(65));
        let mut tasks = JoinSet::new();
        for request_id in 1_u128..=64 {
            let client = client.clone();
            let barrier = Arc::clone(&barrier);
            tasks.spawn(async move {
                barrier.wait().await;
                client
                    .allocate_read_snapshot(request_id, now_ms() + 5_000)
                    .await
            });
        }
        barrier.wait().await;

        let mut timestamps = Vec::with_capacity(64);
        while let Some(result) = tasks.join_next().await {
            timestamps.push(result.unwrap().unwrap());
        }
        timestamps.sort_unstable();
        assert_eq!(timestamps.len(), 64);
        assert!(timestamps.windows(2).all(|pair| {
            advance_timestamp(pair[0], 1)
                .map(|next| next == pair[1])
                .unwrap_or(false)
        }));
        assert!(
            rpc_count.load(Ordering::Relaxed) <= 8,
            "64 concurrent reads should coalesce into far fewer than 64 RPCs"
        );

        let _ = shutdown.send(());
        server.await.unwrap().unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn transaction_count_three_and_single_read_reuse_one_connection() {
        let temporary = tempfile::tempdir().unwrap();
        let rpc_count = Arc::new(AtomicUsize::new(0));
        let connection_count = Arc::new(AtomicUsize::new(0));
        let (address, shutdown, server) = serve_meta(
            elected_meta(temporary.path()),
            Arc::clone(&rpc_count),
            Arc::clone(&connection_count),
        )
        .await;
        let client = MetaTimestampClient::new(CLUSTER_ID, vec![address], 8).unwrap();
        let deadline = now_ms() + 5_000;

        let (start, commit) = client
            .allocate_transaction_timestamps(101, deadline)
            .await
            .unwrap();
        let snapshot = client.allocate_read_snapshot(102, deadline).await.unwrap();

        assert_eq!(commit, advance_timestamp(start, 2).unwrap());
        assert_eq!(snapshot, advance_timestamp(start, 3).unwrap());
        assert_eq!(rpc_count.load(Ordering::Relaxed), 2);
        assert_eq!(connection_count.load(Ordering::Relaxed), 1);

        let _ = shutdown.send(());
        server.await.unwrap().unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn read_snapshot_fails_over_from_an_unreachable_endpoint() {
        let unavailable = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap()
            .local_addr()
            .unwrap();
        let temporary = tempfile::tempdir().unwrap();
        let rpc_count = Arc::new(AtomicUsize::new(0));
        let connection_count = Arc::new(AtomicUsize::new(0));
        let (address, shutdown, server) = serve_meta(
            elected_meta(temporary.path()),
            Arc::clone(&rpc_count),
            connection_count,
        )
        .await;
        let client = MetaTimestampClient::new(CLUSTER_ID, vec![unavailable, address], 8).unwrap();

        let snapshot = client
            .allocate_read_snapshot(201, now_ms() + 5_000)
            .await
            .unwrap();

        assert!(snapshot.physical_micros() > 0);
        assert_eq!(rpc_count.load(Ordering::Relaxed), 1);

        let _ = shutdown.send(());
        server.await.unwrap().unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn expired_read_deadline_fails_closed_without_an_rpc() {
        let temporary = tempfile::tempdir().unwrap();
        let rpc_count = Arc::new(AtomicUsize::new(0));
        let connection_count = Arc::new(AtomicUsize::new(0));
        let (address, shutdown, server) = serve_meta(
            elected_meta(temporary.path()),
            Arc::clone(&rpc_count),
            connection_count,
        )
        .await;
        let client = MetaTimestampClient::new(CLUSTER_ID, vec![address], 8).unwrap();

        let error = client
            .allocate_read_snapshot(301, now_ms())
            .await
            .unwrap_err();

        assert_eq!(error, super::MetaTimestampClientError::DeadlineExceeded);
        assert_eq!(rpc_count.load(Ordering::Relaxed), 0);

        let _ = shutdown.send(());
        server.await.unwrap().unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn canceled_waiter_is_not_reserved_for_a_later_read() {
        let temporary = tempfile::tempdir().unwrap();
        let rpc_count = Arc::new(AtomicUsize::new(0));
        let connection_count = Arc::new(AtomicUsize::new(0));
        let (address, shutdown, server) = serve_meta(
            elected_meta(temporary.path()),
            Arc::clone(&rpc_count),
            connection_count,
        )
        .await;
        let client = MetaTimestampClient::new(CLUSTER_ID, vec![address], 8).unwrap();
        let (response, canceled) = tokio::sync::oneshot::channel();
        drop(canceled);
        client
            .read_requests
            .send(super::ReadSnapshotRequest {
                request_id: 401,
                deadline_unix_ms: now_ms() + 5_000,
                response,
            })
            .await
            .unwrap();

        let snapshot = client
            .allocate_read_snapshot(402, now_ms() + 5_000)
            .await
            .unwrap();

        assert!(snapshot.physical_micros() > 0);
        assert_eq!(rpc_count.load(Ordering::Relaxed), 1);

        let _ = shutdown.send(());
        server.await.unwrap().unwrap();
    }
}
