use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use raft_transport::{
    MAX_ROUTED_FRAME_BYTES, RaftTransportError, RoutedRaftMessage, decode_routed_message_frame,
    encode_routed_message_frame,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, mpsc, oneshot, watch};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::timeout;

const ACCEPTED_ACK: u8 = 0xA1;
const REJECTED_ACK: u8 = 0xE1;
const MAX_QUEUE_CAPACITY: usize = 65_536;
const DELIVERY_TIMEOUT: Duration = Duration::from_millis(250);
const MIN_ROUTED_FRAME_BYTES: usize = 50;

struct OutboundMessage {
    routed: RoutedRaftMessage,
    response: oneshot::Sender<Result<(), RaftNetworkError>>,
}

pub struct RaftDelivery {
    response: oneshot::Receiver<Result<(), RaftNetworkError>>,
}

impl RaftDelivery {
    pub async fn wait(self) -> Result<(), RaftNetworkError> {
        self.response.await.map_err(|_| RaftNetworkError::Stopped)?
    }
}

pub struct SharedRaftTransport {
    cluster_id: [u8; 16],
    node_id: u64,
    local_address: SocketAddr,
    queue_capacity: usize,
    senders: BTreeMap<u64, mpsc::Sender<OutboundMessage>>,
    allowed_peers: Arc<RwLock<BTreeSet<u64>>>,
    inbound: Mutex<mpsc::Receiver<RoutedRaftMessage>>,
    shutdown: watch::Sender<bool>,
    accept_join: Option<JoinHandle<Result<(), RaftNetworkError>>>,
    writer_joins: Vec<JoinHandle<Result<(), RaftNetworkError>>>,
    accepted_connections: Arc<AtomicU64>,
}

impl SharedRaftTransport {
    pub async fn bind(
        cluster_id: [u8; 16],
        node_id: u64,
        listen_address: SocketAddr,
        queue_capacity: usize,
    ) -> Result<Self, RaftNetworkError> {
        if cluster_id == [0; 16] || node_id == 0 {
            return Err(RaftNetworkError::InvalidIdentity);
        }
        if !listen_address.ip().is_loopback() {
            return Err(RaftNetworkError::InsecureNonLoopback);
        }
        if queue_capacity == 0 || queue_capacity > MAX_QUEUE_CAPACITY {
            return Err(RaftNetworkError::InvalidQueueCapacity);
        }
        let listener = TcpListener::bind(listen_address).await?;
        let local_address = listener.local_addr()?;
        let allowed_peers = Arc::new(RwLock::new(BTreeSet::new()));
        let (inbound_sender, inbound) = mpsc::channel(queue_capacity);
        let (shutdown, shutdown_receiver) = watch::channel(false);
        let accepted_connections = Arc::new(AtomicU64::new(0));
        let accept_join = tokio::spawn(run_accept_loop(
            listener,
            cluster_id,
            node_id,
            Arc::clone(&allowed_peers),
            inbound_sender,
            shutdown_receiver,
            Arc::clone(&accepted_connections),
        ));
        Ok(Self {
            cluster_id,
            node_id,
            local_address,
            queue_capacity,
            senders: BTreeMap::new(),
            allowed_peers,
            inbound: Mutex::new(inbound),
            shutdown,
            accept_join: Some(accept_join),
            writer_joins: Vec::new(),
            accepted_connections,
        })
    }

    #[must_use]
    pub const fn local_addr(&self) -> SocketAddr {
        self.local_address
    }

    #[must_use]
    pub fn accepted_connections(&self) -> u64 {
        self.accepted_connections.load(Ordering::Relaxed)
    }

    pub fn add_peer(&mut self, node_id: u64, address: SocketAddr) -> Result<(), RaftNetworkError> {
        if node_id == 0 || node_id == self.node_id || address.port() == 0 {
            return Err(RaftNetworkError::InvalidPeer { node_id });
        }
        if !address.ip().is_loopback() {
            return Err(RaftNetworkError::InsecureNonLoopback);
        }
        if self.senders.contains_key(&node_id) {
            return Err(RaftNetworkError::DuplicatePeer { node_id });
        }
        self.allowed_peers
            .write()
            .map_err(|_| RaftNetworkError::LockPoisoned)?
            .insert(node_id);
        let (sender, receiver) = mpsc::channel(self.queue_capacity);
        let join = tokio::spawn(run_writer(address, receiver, self.shutdown.subscribe()));
        self.senders.insert(node_id, sender);
        self.writer_joins.push(join);
        Ok(())
    }

    pub fn try_send(&self, routed: RoutedRaftMessage) -> Result<RaftDelivery, RaftNetworkError> {
        if routed.route().cluster_id() != &self.cluster_id {
            return Err(RaftNetworkError::WrongCluster);
        }
        if routed.message().from != self.node_id {
            return Err(RaftNetworkError::WrongSource {
                expected: self.node_id,
                actual: routed.message().from,
            });
        }
        let target = routed.message().to;
        let sender = self
            .senders
            .get(&target)
            .ok_or(RaftNetworkError::UnknownPeer { node_id: target })?;
        let (response_sender, response) = oneshot::channel();
        sender
            .try_send(OutboundMessage {
                routed,
                response: response_sender,
            })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => {
                    RaftNetworkError::PeerQueueFull { node_id: target }
                }
                mpsc::error::TrySendError::Closed(_) => RaftNetworkError::Stopped,
            })?;
        Ok(RaftDelivery { response })
    }

    pub async fn send(&self, routed: RoutedRaftMessage) -> Result<(), RaftNetworkError> {
        self.try_send(routed)?.wait().await
    }

    pub async fn receive(&self) -> Result<RoutedRaftMessage, RaftNetworkError> {
        self.inbound
            .lock()
            .await
            .recv()
            .await
            .ok_or(RaftNetworkError::Stopped)
    }

    pub async fn shutdown(mut self) -> Result<(), RaftNetworkError> {
        let _ = self.shutdown.send(true);
        self.senders.clear();
        let mut first_error = None;
        for join in self.writer_joins.drain(..) {
            match join.await {
                Ok(Ok(())) => {}
                Ok(Err(error)) if first_error.is_none() => first_error = Some(error),
                Err(error) if first_error.is_none() => {
                    first_error = Some(RaftNetworkError::Join(error.to_string()));
                }
                Ok(Err(_)) | Err(_) => {}
            }
        }
        if let Some(join) = self.accept_join.take() {
            match join.await {
                Ok(Ok(())) => {}
                Ok(Err(error)) if first_error.is_none() => first_error = Some(error),
                Err(error) if first_error.is_none() => {
                    first_error = Some(RaftNetworkError::Join(error.to_string()));
                }
                Ok(Err(_)) | Err(_) => {}
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

async fn run_writer(
    address: SocketAddr,
    mut receiver: mpsc::Receiver<OutboundMessage>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), RaftNetworkError> {
    let mut connection = None;
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return Ok(());
                }
            }
            outbound = receiver.recv() => {
                let Some(outbound) = outbound else {
                    return Ok(());
                };
                let result = deliver(address, &mut connection, &outbound.routed).await;
                let _ = outbound.response.send(result);
            }
        }
    }
}

async fn deliver(
    address: SocketAddr,
    connection: &mut Option<TcpStream>,
    routed: &RoutedRaftMessage,
) -> Result<(), RaftNetworkError> {
    let frame = encode_routed_message_frame(routed)?;
    let frame_length = u32::try_from(frame.len()).map_err(|_| RaftNetworkError::FrameTooLarge)?;
    let mut last_error = RaftNetworkError::Stopped;
    for _ in 0..2 {
        if connection.is_none() {
            match timeout(DELIVERY_TIMEOUT, TcpStream::connect(address)).await {
                Ok(Ok(stream)) => *connection = Some(stream),
                Ok(Err(error)) => {
                    last_error = error.into();
                    continue;
                }
                Err(_) => {
                    last_error = RaftNetworkError::Timeout;
                    continue;
                }
            }
        }
        let stream = connection.as_mut().expect("connection was opened");
        let attempt = timeout(DELIVERY_TIMEOUT, async {
            stream.write_all(&frame_length.to_be_bytes()).await?;
            stream.write_all(&frame).await?;
            stream.flush().await?;
            stream.read_u8().await
        })
        .await;
        match attempt {
            Ok(Ok(ACCEPTED_ACK)) => return Ok(()),
            Ok(Ok(REJECTED_ACK)) => {
                *connection = None;
                return Err(RaftNetworkError::PeerRejected);
            }
            Ok(Ok(_)) => {
                last_error = RaftNetworkError::InvalidAcknowledgement;
                *connection = None;
            }
            Ok(Err(error)) => {
                last_error = error.into();
                *connection = None;
            }
            Err(_) => {
                last_error = RaftNetworkError::Timeout;
                *connection = None;
            }
        }
    }
    Err(last_error)
}

async fn run_accept_loop(
    listener: TcpListener,
    cluster_id: [u8; 16],
    node_id: u64,
    allowed_peers: Arc<RwLock<BTreeSet<u64>>>,
    inbound: mpsc::Sender<RoutedRaftMessage>,
    mut shutdown: watch::Receiver<bool>,
    accepted_connections: Arc<AtomicU64>,
) -> Result<(), RaftNetworkError> {
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                accepted_connections.fetch_add(1, Ordering::Relaxed);
                connections.spawn(handle_connection(
                    stream,
                    cluster_id,
                    node_id,
                    Arc::clone(&allowed_peers),
                    inbound.clone(),
                ));
            }
            joined = connections.join_next(), if !connections.is_empty() => {
                let _ = joined;
            }
        }
    }
    connections.abort_all();
    while connections.join_next().await.is_some() {}
    Ok(())
}

async fn handle_connection(
    mut stream: TcpStream,
    cluster_id: [u8; 16],
    node_id: u64,
    allowed_peers: Arc<RwLock<BTreeSet<u64>>>,
    inbound: mpsc::Sender<RoutedRaftMessage>,
) -> Result<(), RaftNetworkError> {
    let mut bound_peer = None;
    loop {
        let mut length_bytes = [0_u8; 4];
        match stream.read_exact(&mut length_bytes).await {
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(error) => return Err(error.into()),
        }
        let length = u32::from_be_bytes(length_bytes) as usize;
        if !(MIN_ROUTED_FRAME_BYTES..=MAX_ROUTED_FRAME_BYTES).contains(&length) {
            reject(&mut stream).await;
            return Err(RaftNetworkError::InvalidFrameLength);
        }
        let mut frame = vec![0_u8; length];
        stream.read_exact(&mut frame).await?;
        let routed = match decode_routed_message_frame(cluster_id, node_id, &frame) {
            Ok(routed) => routed,
            Err(error) => {
                reject(&mut stream).await;
                return Err(error.into());
            }
        };
        let source = routed.message().from;
        let allowed = allowed_peers
            .read()
            .map_err(|_| RaftNetworkError::LockPoisoned)?
            .contains(&source);
        if !allowed {
            reject(&mut stream).await;
            return Err(RaftNetworkError::UnknownPeer { node_id: source });
        }
        if bound_peer.is_some_and(|peer| peer != source) {
            reject(&mut stream).await;
            return Err(RaftNetworkError::PeerIdentityChanged);
        }
        bound_peer = Some(source);
        if let Err(error) = inbound.try_send(routed) {
            reject(&mut stream).await;
            return Err(match error {
                mpsc::error::TrySendError::Full(_) => RaftNetworkError::InboundQueueFull,
                mpsc::error::TrySendError::Closed(_) => RaftNetworkError::Stopped,
            });
        }
        stream.write_u8(ACCEPTED_ACK).await?;
        stream.flush().await?;
    }
}

async fn reject(stream: &mut TcpStream) {
    let _ = stream.write_u8(REJECTED_ACK).await;
    let _ = stream.flush().await;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RaftNetworkError {
    Io(String),
    Transport(RaftTransportError),
    InvalidIdentity,
    InvalidQueueCapacity,
    InsecureNonLoopback,
    InvalidPeer { node_id: u64 },
    DuplicatePeer { node_id: u64 },
    UnknownPeer { node_id: u64 },
    WrongCluster,
    WrongSource { expected: u64, actual: u64 },
    PeerQueueFull { node_id: u64 },
    InboundQueueFull,
    PeerIdentityChanged,
    PeerRejected,
    InvalidAcknowledgement,
    InvalidFrameLength,
    FrameTooLarge,
    Timeout,
    Stopped,
    LockPoisoned,
    Join(String),
}

impl Display for RaftNetworkError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(message) => write!(formatter, "Raft network I/O error: {message}"),
            Self::Transport(error) => write!(formatter, "Raft frame error: {error}"),
            Self::InvalidIdentity => formatter.write_str("invalid local Raft identity"),
            Self::InvalidQueueCapacity => formatter.write_str("invalid Raft queue capacity"),
            Self::InsecureNonLoopback => {
                formatter.write_str("plaintext Raft transport is restricted to loopback")
            }
            Self::InvalidPeer { node_id } => write!(formatter, "invalid Raft peer {node_id}"),
            Self::DuplicatePeer { node_id } => write!(formatter, "duplicate Raft peer {node_id}"),
            Self::UnknownPeer { node_id } => write!(formatter, "unknown Raft peer {node_id}"),
            Self::WrongCluster => formatter.write_str("outbound Raft message has another cluster"),
            Self::WrongSource { expected, actual } => write!(
                formatter,
                "outbound Raft message source {actual}; expected {expected}"
            ),
            Self::PeerQueueFull { node_id } => {
                write!(formatter, "Raft peer {node_id} queue is full")
            }
            Self::InboundQueueFull => formatter.write_str("Raft inbound queue is full"),
            Self::PeerIdentityChanged => {
                formatter.write_str("Raft connection changed peer identity")
            }
            Self::PeerRejected => formatter.write_str("Raft peer rejected the frame"),
            Self::InvalidAcknowledgement => {
                formatter.write_str("Raft peer returned an invalid acknowledgement")
            }
            Self::InvalidFrameLength => formatter.write_str("invalid routed Raft frame length"),
            Self::FrameTooLarge => formatter.write_str("routed Raft frame is too large"),
            Self::Timeout => formatter.write_str("Raft delivery timed out"),
            Self::Stopped => formatter.write_str("Raft network has stopped"),
            Self::LockPoisoned => formatter.write_str("Raft network lock is poisoned"),
            Self::Join(message) => write!(formatter, "Raft network task join error: {message}"),
        }
    }
}

impl Error for RaftNetworkError {}

impl From<std::io::Error> for RaftNetworkError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error.to_string())
    }
}

impl From<RaftTransportError> for RaftNetworkError {
    fn from(error: RaftTransportError) -> Self {
        Self::Transport(error)
    }
}
