#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::future::Future;
use std::io::{Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError, TrySendError};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

use prost::Message;
use storage_api::{
    ADAPTER_SPI_VERSION, AdapterCapabilities, AdapterDescriptorV1, AdapterError, AdapterFuture,
    ApplyReceipt, BackendFamily, CommittedMutationBatch, Durability, KeySpan, KeyValue, Keyspace,
    LOGICAL_SNAPSHOT_FORMAT_VERSION, LogicalKey, LogicalSnapshotChunkV1,
    LogicalSnapshotExportRequest, LogicalSnapshotHeaderV1, LogicalSnapshotManifestV1,
    LogicalSnapshotReader, MAX_LOGICAL_SNAPSHOT_CHUNK_BYTES, MAX_LOGICAL_SNAPSHOT_CHUNK_ENTRIES,
    Mutation, MutationOperation, SnapshotCapability, StorageAdapter,
};

mod service;
pub use service::{
    MAX_ACTIVE_SNAPSHOT_SESSIONS, SNAPSHOT_SESSION_IDLE_TIMEOUT, SidecarRestoreBackend,
    SidecarService, TcpSidecarAdapterFactory, spawn_stateful_tcp_sidecar_server,
};

const MAGIC: [u8; 4] = *b"DTAS";
const WIRE_VERSION: u16 = 1;
const HEADER_BYTES: usize = 28;
const CHECKSUM_BYTES: usize = 4;
pub const MAX_FRAME_PAYLOAD_BYTES: usize = 20 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FeatureSet(u64);

impl FeatureSet {
    pub const BASE_ADAPTER_V1: Self = Self(1 << 0);
    pub const LOGICAL_EXPORT_SESSION_V1: Self = Self(1 << 1);
    pub const LOGICAL_RESTORE_SESSION_V1: Self = Self(1 << 2);
    pub const RESUMABLE_ORDINAL_REPLAY_V1: Self = Self(1 << 3);
    pub const ALL: Self = Self(
        Self::BASE_ADAPTER_V1.0
            | Self::LOGICAL_EXPORT_SESSION_V1.0
            | Self::LOGICAL_RESTORE_SESSION_V1.0
            | Self::RESUMABLE_ORDINAL_REPLAY_V1.0,
    );
    pub const EMPTY: Self = Self(0);

    pub fn from_bits(bits: u64) -> Result<Self, ProtocolError> {
        if bits & !Self::ALL.0 == 0 {
            Ok(Self(bits))
        } else {
            Err(ProtocolError::UnknownFeatureBits {
                bits: bits & !Self::ALL.0,
            })
        }
    }

    #[must_use]
    pub const fn bits(self) -> u64 {
        self.0
    }

    #[must_use]
    pub const fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }

    #[must_use]
    pub const fn intersection(self, other: Self) -> Self {
        Self(self.0 & other.0)
    }

    #[must_use]
    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HelloRequest {
    pub required_features: FeatureSet,
    pub optional_features: FeatureSet,
    pub max_payload_bytes: u32,
}

impl HelloRequest {
    #[must_use]
    pub fn adapter_client() -> Self {
        Self {
            required_features: FeatureSet::BASE_ADAPTER_V1,
            optional_features: FeatureSet::LOGICAL_EXPORT_SESSION_V1
                .union(FeatureSet::LOGICAL_RESTORE_SESSION_V1)
                .union(FeatureSet::RESUMABLE_ORDINAL_REPLAY_V1),
            max_payload_bytes: u32::try_from(MAX_FRAME_PAYLOAD_BYTES)
                .expect("frame maximum fits in u32"),
        }
    }

    #[must_use]
    pub fn restore_client() -> Self {
        Self {
            required_features: FeatureSet::BASE_ADAPTER_V1
                .union(FeatureSet::LOGICAL_RESTORE_SESSION_V1)
                .union(FeatureSet::RESUMABLE_ORDINAL_REPLAY_V1),
            optional_features: FeatureSet::EMPTY,
            max_payload_bytes: u32::try_from(MAX_FRAME_PAYLOAD_BYTES)
                .expect("frame maximum fits in u32"),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HelloResponse {
    pub wire_version: u16,
    pub negotiated_features: FeatureSet,
    pub max_payload_bytes: u32,
    pub max_chunk_bytes: u32,
    pub max_chunk_entries: u32,
    pub snapshot_format_version: u16,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublicAdapterOpenRequest {
    instance_id: String,
    parameters: BTreeMap<String, String>,
}

impl PublicAdapterOpenRequest {
    #[must_use]
    pub fn new(instance_id: impl Into<String>) -> Self {
        Self {
            instance_id: instance_id.into(),
            parameters: BTreeMap::new(),
        }
    }

    #[must_use]
    pub fn with_parameter(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.parameters.insert(name.into(), value.into());
        self
    }

    #[must_use]
    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    #[must_use]
    pub const fn parameters(&self) -> &BTreeMap<String, String> {
        &self.parameters
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BeginExportRequest {
    pub limits: LogicalSnapshotExportRequest,
    pub expected_applied_log_index: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BeginRestoreRequest {
    pub header: LogicalSnapshotHeaderV1,
    pub target: PublicAdapterOpenRequest,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExportStarted {
    pub session_id: u128,
    pub header: LogicalSnapshotHeaderV1,
    pub limits: LogicalSnapshotExportRequest,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RestoreStarted {
    pub session_id: u128,
    pub prospective_descriptor: AdapterDescriptorV1,
    pub max_chunk_bytes: u32,
    pub max_chunk_entries: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RestoreComplete {
    pub session_id: u128,
    pub final_descriptor: AdapterDescriptorV1,
    pub applied_log_index: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u32)]
pub enum RemoteErrorCode {
    FeatureUnsupported = 100,
    NotActive = 101,
    ResourceExhausted = 102,
    SessionUnknown = 103,
    SessionExpired = 104,
    SessionKindMismatch = 105,
    SessionBusy = 106,
    OrdinalGap = 107,
    OrdinalRegression = 108,
    ChunkDigestMismatch = 109,
    RequestReplayMismatch = 110,
    TerminalReplayMismatch = 111,
    RestoreAlreadyInProgress = 112,
    ServiceFaulted = 113,
    TargetRequestMismatch = 114,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum FrameKind {
    Request = 1,
    Response = 2,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Request {
    Hello(HelloRequest),
    Describe,
    Apply(CommittedMutationBatch),
    MultiGet(Vec<LogicalKey>),
    Scan(KeySpan),
    AppliedLogIndex,
    Health,
    BeginExport(BeginExportRequest),
    ExportNext {
        session_id: u128,
        expected_ordinal: u64,
    },
    BeginRestore(BeginRestoreRequest),
    RestoreChunk {
        session_id: u128,
        chunk: LogicalSnapshotChunkV1,
    },
    FinishRestore {
        session_id: u128,
        manifest: LogicalSnapshotManifestV1,
    },
    AbortSession {
        session_id: u128,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Response {
    Hello(HelloResponse),
    Descriptor(AdapterDescriptorV1),
    Apply(ApplyReceipt),
    MultiGet(Vec<Option<Vec<u8>>>),
    Scan(Vec<KeyValue>),
    AppliedLogIndex(u64),
    Health(HealthStatus),
    ExportStarted(ExportStarted),
    ExportChunk {
        session_id: u128,
        chunk: LogicalSnapshotChunkV1,
    },
    ExportComplete {
        session_id: u128,
        manifest: LogicalSnapshotManifestV1,
    },
    RestoreStarted(RestoreStarted),
    RestoreChunkAccepted {
        session_id: u128,
        ordinal: u64,
        digest: [u8; 32],
    },
    RestoreComplete(RestoreComplete),
    SessionAborted {
        session_id: u128,
    },
    Error(RemoteError),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HealthStatus {
    pub ready: bool,
    pub detail: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RemoteError {
    pub code: u32,
    pub message: String,
    pub retryable: bool,
}

impl Display for RemoteError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "remote Adapter error {} (retryable={}): {}",
            self.code, self.retryable, self.message
        )
    }
}

impl Error for RemoteError {}

pub type SidecarTransportFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Response, SidecarClientError>> + Send + 'a>>;

/// A request/response transport. Implementations must preserve a request as one
/// atomic exchange and may retry it only with the same request identifier.
pub trait SidecarTransport: Send + Sync {
    fn call<'a>(&'a self, request: Request) -> SidecarTransportFuture<'a>;
}

/// In-process transport used by conformance tests and embedded Sidecars.
pub struct LoopbackTransport {
    adapter: Arc<dyn StorageAdapter>,
}

impl LoopbackTransport {
    #[must_use]
    pub const fn new(adapter: Arc<dyn StorageAdapter>) -> Self {
        Self { adapter }
    }
}

impl SidecarTransport for LoopbackTransport {
    fn call<'a>(&'a self, request: Request) -> SidecarTransportFuture<'a> {
        Box::pin(async move { Ok(dispatch_request(self.adapter.as_ref(), request).await) })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TcpSidecarConfig {
    address: SocketAddr,
    pool_size: usize,
    connect_timeout: Duration,
    read_timeout: Duration,
    write_timeout: Duration,
}

impl TcpSidecarConfig {
    #[must_use]
    pub const fn new(address: SocketAddr) -> Self {
        Self {
            address,
            pool_size: 4,
            connect_timeout: Duration::from_secs(3),
            read_timeout: Duration::from_secs(10),
            write_timeout: Duration::from_secs(10),
        }
    }

    pub fn with_pool_size(mut self, pool_size: usize) -> Result<Self, SidecarClientError> {
        if pool_size == 0 {
            return Err(SidecarClientError::InvalidConfiguration(
                "TCP connection pool must contain at least one connection".to_owned(),
            ));
        }
        self.pool_size = pool_size;
        Ok(self)
    }

    pub fn with_timeouts(
        mut self,
        connect_timeout: Duration,
        read_timeout: Duration,
        write_timeout: Duration,
    ) -> Result<Self, SidecarClientError> {
        if connect_timeout.is_zero() || read_timeout.is_zero() || write_timeout.is_zero() {
            return Err(SidecarClientError::InvalidConfiguration(
                "TCP timeouts must be positive".to_owned(),
            ));
        }
        self.connect_timeout = connect_timeout;
        self.read_timeout = read_timeout;
        self.write_timeout = write_timeout;
        Ok(self)
    }
}

static NEXT_CLIENT_ID: AtomicU64 = AtomicU64::new(1);

/// Blocking, bounded TCP connection pool. DTGProxy invokes Storage Adapters on
/// dedicated storage workers, so blocking socket I/O cannot occupy Raft workers.
pub struct TcpSidecarTransport {
    config: TcpSidecarConfig,
    connections: Vec<Mutex<Option<TcpStream>>>,
    next_connection: AtomicUsize,
    client_id: u64,
    next_sequence: AtomicU64,
}

impl TcpSidecarTransport {
    pub fn connect(config: TcpSidecarConfig) -> Result<Self, SidecarClientError> {
        if config.pool_size == 0 {
            return Err(SidecarClientError::InvalidConfiguration(
                "TCP connection pool must contain at least one connection".to_owned(),
            ));
        }
        let mut connections = Vec::with_capacity(config.pool_size);
        for _ in 0..config.pool_size {
            connections.push(Mutex::new(Some(connect_tcp_stream(config)?)));
        }
        Ok(Self {
            config,
            connections,
            next_connection: AtomicUsize::new(0),
            client_id: NEXT_CLIENT_ID.fetch_add(1, Ordering::Relaxed),
            next_sequence: AtomicU64::new(1),
        })
    }

    fn request_id(&self) -> u128 {
        let sequence = self.next_sequence.fetch_add(1, Ordering::Relaxed);
        (u128::from(self.client_id) << 64) | u128::from(sequence)
    }

    fn exchange(
        &self,
        request_id: u128,
        request: &Request,
    ) -> Result<Response, SidecarClientError> {
        let slot = self.next_connection.fetch_add(1, Ordering::Relaxed) % self.connections.len();
        let mut connection = self.connections[slot]
            .lock()
            .map_err(|_| SidecarClientError::ConnectionPoolPoisoned)?;
        if connection.is_none() {
            *connection = Some(connect_tcp_stream(self.config)?);
        }
        let first = exchange_once(
            connection.as_mut().expect("connection initialized"),
            request_id,
            request,
        );
        match first {
            Ok(response) => Ok(response),
            Err(first_error) => {
                *connection = None;
                let mut replacement = connect_tcp_stream(self.config)?;
                match exchange_once(&mut replacement, request_id, request) {
                    Ok(response) => {
                        *connection = Some(replacement);
                        Ok(response)
                    }
                    Err(second_error) => Err(SidecarClientError::Transport(format!(
                        "request failed before and after reconnect: {first_error}; {second_error}"
                    ))),
                }
            }
        }
    }
}

impl SidecarTransport for TcpSidecarTransport {
    fn call<'a>(&'a self, request: Request) -> SidecarTransportFuture<'a> {
        Box::pin(async move { self.exchange(self.request_id(), &request) })
    }
}

fn connect_tcp_stream(config: TcpSidecarConfig) -> Result<TcpStream, SidecarClientError> {
    let stream = TcpStream::connect_timeout(&config.address, config.connect_timeout)
        .map_err(|error| SidecarClientError::Transport(error.to_string()))?;
    stream
        .set_nodelay(true)
        .and_then(|()| stream.set_read_timeout(Some(config.read_timeout)))
        .and_then(|()| stream.set_write_timeout(Some(config.write_timeout)))
        .map_err(|error| SidecarClientError::Transport(error.to_string()))?;
    Ok(stream)
}

fn exchange_once(
    stream: &mut TcpStream,
    request_id: u128,
    request: &Request,
) -> Result<Response, SidecarClientError> {
    write_frame(stream, request_id, request)
        .map_err(|error| SidecarClientError::Transport(error.to_string()))?;
    let response = read_frame::<_, Response>(stream)
        .map_err(|error| SidecarClientError::Transport(error.to_string()))?;
    if response.request_id() != request_id {
        return Err(SidecarClientError::RequestIdMismatch {
            expected: request_id,
            actual: response.request_id(),
        });
    }
    Ok(response.into_message())
}

/// Serves a persistent TCP connection until the peer closes it. Each response
/// carries the request identifier from the matching request frame.
pub fn serve_connection(
    mut stream: TcpStream,
    adapter: &dyn StorageAdapter,
) -> Result<(), ProtocolError> {
    while let Some(frame) = read_frame_or_eof::<_, Request>(&mut stream)? {
        let request_id = frame.request_id();
        let response = block_on_dispatch(dispatch_request(adapter, frame.into_message()));
        write_frame(&mut stream, request_id, &response)?;
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TcpSidecarServerConfig {
    bind_address: SocketAddr,
    worker_threads: usize,
    pending_connections: usize,
    accept_poll_interval: Duration,
}

impl TcpSidecarServerConfig {
    #[must_use]
    pub const fn new(bind_address: SocketAddr) -> Self {
        Self {
            bind_address,
            worker_threads: 8,
            pending_connections: 64,
            accept_poll_interval: Duration::from_millis(10),
        }
    }

    pub fn with_capacity(
        mut self,
        worker_threads: usize,
        pending_connections: usize,
    ) -> Result<Self, SidecarServerError> {
        if worker_threads == 0 || pending_connections == 0 {
            return Err(SidecarServerError::InvalidConfiguration(
                "worker and pending-connection capacities must be positive".to_owned(),
            ));
        }
        self.worker_threads = worker_threads;
        self.pending_connections = pending_connections;
        Ok(self)
    }

    pub fn with_accept_poll_interval(
        mut self,
        accept_poll_interval: Duration,
    ) -> Result<Self, SidecarServerError> {
        if accept_poll_interval.is_zero() {
            return Err(SidecarServerError::InvalidConfiguration(
                "accept poll interval must be positive".to_owned(),
            ));
        }
        self.accept_poll_interval = accept_poll_interval;
        Ok(self)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SidecarServerError {
    InvalidConfiguration(String),
    Io(String),
    StatePoisoned,
    WorkerPanicked,
    ServerPanicked,
}

impl Display for SidecarServerError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfiguration(message) => {
                write!(formatter, "invalid Sidecar server configuration: {message}")
            }
            Self::Io(message) => write!(formatter, "Sidecar server I/O error: {message}"),
            Self::StatePoisoned => formatter.write_str("Sidecar server state lock is poisoned"),
            Self::WorkerPanicked => formatter.write_str("Sidecar server worker panicked"),
            Self::ServerPanicked => formatter.write_str("Sidecar server thread panicked"),
        }
    }
}

impl Error for SidecarServerError {}

type ActiveConnections = Arc<Mutex<BTreeMap<u64, TcpStream>>>;

pub struct TcpSidecarServerHandle {
    local_addr: SocketAddr,
    shutdown: Arc<AtomicBool>,
    active_connections: ActiveConnections,
    server_thread: Option<std::thread::JoinHandle<Result<(), SidecarServerError>>>,
}

impl TcpSidecarServerHandle {
    #[must_use]
    pub const fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub fn shutdown(mut self) -> Result<(), SidecarServerError> {
        self.stop_and_join()
    }

    fn stop_and_join(&mut self) -> Result<(), SidecarServerError> {
        self.shutdown.store(true, Ordering::Release);
        close_active_connections(&self.active_connections)?;
        let Some(server_thread) = self.server_thread.take() else {
            return Ok(());
        };
        server_thread
            .join()
            .map_err(|_| SidecarServerError::ServerPanicked)?
    }
}

impl Drop for TcpSidecarServerHandle {
    fn drop(&mut self) {
        let _ = self.stop_and_join();
    }
}

pub fn spawn_tcp_sidecar_server(
    config: TcpSidecarServerConfig,
    adapter: Arc<dyn StorageAdapter>,
) -> Result<TcpSidecarServerHandle, SidecarServerError> {
    if config.worker_threads == 0 || config.pending_connections == 0 {
        return Err(SidecarServerError::InvalidConfiguration(
            "worker and pending-connection capacities must be positive".to_owned(),
        ));
    }
    let listener = TcpListener::bind(config.bind_address)
        .map_err(|error| SidecarServerError::Io(error.to_string()))?;
    listener
        .set_nonblocking(true)
        .map_err(|error| SidecarServerError::Io(error.to_string()))?;
    let local_addr = listener
        .local_addr()
        .map_err(|error| SidecarServerError::Io(error.to_string()))?;
    let shutdown = Arc::new(AtomicBool::new(false));
    let active_connections = Arc::new(Mutex::new(BTreeMap::new()));
    let server_shutdown = Arc::clone(&shutdown);
    let server_connections = Arc::clone(&active_connections);
    let server_thread = std::thread::Builder::new()
        .name("dtg-sidecar-accept".to_owned())
        .spawn(move || {
            run_tcp_sidecar_server(
                listener,
                config,
                adapter,
                server_shutdown,
                server_connections,
            )
        })
        .map_err(|error| SidecarServerError::Io(error.to_string()))?;
    Ok(TcpSidecarServerHandle {
        local_addr,
        shutdown,
        active_connections,
        server_thread: Some(server_thread),
    })
}

fn run_tcp_sidecar_server(
    listener: TcpListener,
    config: TcpSidecarServerConfig,
    adapter: Arc<dyn StorageAdapter>,
    shutdown: Arc<AtomicBool>,
    active_connections: ActiveConnections,
) -> Result<(), SidecarServerError> {
    let (sender, receiver) = mpsc::sync_channel(config.pending_connections);
    let receiver = Arc::new(Mutex::new(receiver));
    let mut workers = Vec::with_capacity(config.worker_threads);
    for worker_id in 0..config.worker_threads {
        let worker_adapter = Arc::clone(&adapter);
        let worker_receiver = Arc::clone(&receiver);
        let worker_shutdown = Arc::clone(&shutdown);
        let worker_connections = Arc::clone(&active_connections);
        workers.push(
            std::thread::Builder::new()
                .name(format!("dtg-sidecar-worker-{worker_id}"))
                .spawn(move || {
                    sidecar_worker_loop(
                        worker_adapter,
                        worker_receiver,
                        worker_shutdown,
                        worker_connections,
                        config.accept_poll_interval,
                    )
                })
                .map_err(|error| SidecarServerError::Io(error.to_string()))?,
        );
    }

    let accept_result = loop {
        if shutdown.load(Ordering::Acquire) {
            break Ok(());
        }
        match listener.accept() {
            Ok((stream, _peer)) => {
                if let Err(error) = stream
                    .set_nonblocking(false)
                    .and_then(|()| stream.set_nodelay(true))
                {
                    break Err(SidecarServerError::Io(error.to_string()));
                }
                match sender.try_send(stream) {
                    Ok(()) => {}
                    Err(TrySendError::Full(_stream)) => {}
                    Err(TrySendError::Disconnected(_stream)) => break Ok(()),
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(config.accept_poll_interval);
            }
            Err(error) => break Err(SidecarServerError::Io(error.to_string())),
        }
    };
    shutdown.store(true, Ordering::Release);
    drop(sender);
    close_active_connections(&active_connections)?;
    for worker in workers {
        worker
            .join()
            .map_err(|_| SidecarServerError::WorkerPanicked)??;
    }
    accept_result
}

static NEXT_SERVER_CONNECTION_ID: AtomicU64 = AtomicU64::new(1);

fn sidecar_worker_loop(
    adapter: Arc<dyn StorageAdapter>,
    receiver: Arc<Mutex<mpsc::Receiver<TcpStream>>>,
    shutdown: Arc<AtomicBool>,
    active_connections: ActiveConnections,
    poll_interval: Duration,
) -> Result<(), SidecarServerError> {
    loop {
        if shutdown.load(Ordering::Acquire) {
            return Ok(());
        }
        let received = receiver
            .lock()
            .map_err(|_| SidecarServerError::StatePoisoned)?
            .recv_timeout(poll_interval);
        let stream = match received {
            Ok(stream) => stream,
            Err(RecvTimeoutError::Timeout) => continue,
            Err(RecvTimeoutError::Disconnected) => return Ok(()),
        };
        let connection_id = NEXT_SERVER_CONNECTION_ID.fetch_add(1, Ordering::Relaxed);
        let control_stream = stream
            .try_clone()
            .map_err(|error| SidecarServerError::Io(error.to_string()))?;
        active_connections
            .lock()
            .map_err(|_| SidecarServerError::StatePoisoned)?
            .insert(connection_id, control_stream);
        if shutdown.load(Ordering::Acquire) {
            let _ = stream.shutdown(Shutdown::Both);
        } else {
            let _ = serve_connection(stream, adapter.as_ref());
        }
        active_connections
            .lock()
            .map_err(|_| SidecarServerError::StatePoisoned)?
            .remove(&connection_id);
    }
}

fn close_active_connections(
    active_connections: &ActiveConnections,
) -> Result<(), SidecarServerError> {
    let connections = active_connections
        .lock()
        .map_err(|_| SidecarServerError::StatePoisoned)?;
    for stream in connections.values() {
        let _ = stream.shutdown(Shutdown::Both);
    }
    Ok(())
}

struct DispatchWake(std::thread::Thread);

impl Wake for DispatchWake {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

fn block_on_dispatch<F: Future>(future: F) -> F::Output {
    let waker = Waker::from(Arc::new(DispatchWake(std::thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => std::thread::park(),
        }
    }
}

/// Converts the bounded Sidecar protocol into Storage Adapter calls. Adapter
/// failures are values on the wire so the connection remains reusable.
pub async fn dispatch_request(adapter: &dyn StorageAdapter, request: Request) -> Response {
    match request {
        Request::Hello(request) => {
            let supported = FeatureSet::BASE_ADAPTER_V1;
            if !supported.contains(request.required_features) {
                Response::Error(RemoteError {
                    code: RemoteErrorCode::FeatureUnsupported as u32,
                    message: "required Sidecar Feature is unavailable".to_owned(),
                    retryable: false,
                })
            } else {
                Response::Hello(HelloResponse {
                    wire_version: WIRE_VERSION,
                    negotiated_features: request
                        .required_features
                        .union(request.optional_features)
                        .intersection(supported),
                    max_payload_bytes: u32::try_from(MAX_FRAME_PAYLOAD_BYTES)
                        .expect("frame maximum fits in u32"),
                    max_chunk_bytes: u32::try_from(MAX_LOGICAL_SNAPSHOT_CHUNK_BYTES)
                        .expect("Chunk maximum fits in u32"),
                    max_chunk_entries: u32::try_from(MAX_LOGICAL_SNAPSHOT_CHUNK_ENTRIES)
                        .expect("Chunk entry maximum fits in u32"),
                    snapshot_format_version: LOGICAL_SNAPSHOT_FORMAT_VERSION,
                })
            }
        }
        Request::Describe => Response::Descriptor(adapter.descriptor()),
        Request::Apply(batch) => match adapter.apply_committed(batch).await {
            Ok(receipt) => Response::Apply(receipt),
            Err(error) => Response::Error(encode_adapter_error(&error)),
        },
        Request::MultiGet(keys) => match adapter.multi_get(&keys).await {
            Ok(values) => Response::MultiGet(values),
            Err(error) => Response::Error(encode_adapter_error(&error)),
        },
        Request::Scan(span) => match adapter.scan(&span).await {
            Ok(values) => Response::Scan(values),
            Err(error) => Response::Error(encode_adapter_error(&error)),
        },
        Request::AppliedLogIndex => match adapter.applied_log_index() {
            Ok(index) => Response::AppliedLogIndex(index),
            Err(error) => Response::Error(encode_adapter_error(&error)),
        },
        Request::Health => match adapter.applied_log_index() {
            Ok(index) => Response::Health(HealthStatus {
                ready: true,
                detail: format!("ready at applied log index {index}"),
            }),
            Err(error) => Response::Health(HealthStatus {
                ready: false,
                detail: error.to_string(),
            }),
        },
        Request::BeginExport(_)
        | Request::ExportNext { .. }
        | Request::BeginRestore(_)
        | Request::RestoreChunk { .. }
        | Request::FinishRestore { .. }
        | Request::AbortSession { .. } => Response::Error(RemoteError {
            code: RemoteErrorCode::FeatureUnsupported as u32,
            message: "snapshot sessions require a stateful Sidecar service".to_owned(),
            retryable: false,
        }),
    }
}

fn encode_adapter_error(error: &AdapterError) -> RemoteError {
    let (code, retryable) = match error {
        AdapterError::NonContiguousLogIndex { .. } => (1, false),
        AdapterError::CommittedLogReplayMismatch { .. } => (2, false),
        AdapterError::DuplicateMutationSequence { .. } => (3, false),
        AdapterError::MutationReplayMismatch { .. } => (4, false),
        AdapterError::Backend(_) => (5, true),
        AdapterError::LockPoisoned => (6, true),
        AdapterError::UnsupportedOperation { .. } => (7, false),
        AdapterError::LogicalSnapshot(_) => (8, false),
    };
    RemoteError {
        code,
        message: error.to_string(),
        retryable,
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SidecarClientError {
    InvalidConfiguration(String),
    Transport(String),
    ConnectionPoolPoisoned,
    RequestIdMismatch {
        expected: u128,
        actual: u128,
    },
    Remote(RemoteError),
    UnexpectedResponse {
        operation: &'static str,
        response: &'static str,
    },
    NotReady(String),
    InvalidMultiGetCount {
        expected: usize,
        actual: usize,
    },
    AppliedIndexBehind {
        required: u64,
        actual: u64,
    },
    AppliedIndexRegressed {
        cached: u64,
        actual: u64,
    },
    InvalidScan(String),
    InvalidSnapshotResponse(String),
}

impl Display for SidecarClientError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfiguration(message) => {
                write!(formatter, "invalid Sidecar configuration: {message}")
            }
            Self::Transport(message) => write!(formatter, "Sidecar transport error: {message}"),
            Self::ConnectionPoolPoisoned => {
                formatter.write_str("Sidecar connection pool lock is poisoned")
            }
            Self::RequestIdMismatch { expected, actual } => write!(
                formatter,
                "Sidecar response request ID {actual} differs from expected ID {expected}"
            ),
            Self::Remote(error) => Display::fmt(error, formatter),
            Self::UnexpectedResponse {
                operation,
                response,
            } => write!(
                formatter,
                "Sidecar operation {operation} returned unexpected {response} response"
            ),
            Self::NotReady(detail) => write!(formatter, "Sidecar is not ready: {detail}"),
            Self::InvalidMultiGetCount { expected, actual } => write!(
                formatter,
                "Sidecar multi-get returned {actual} values for {expected} keys"
            ),
            Self::AppliedIndexBehind { required, actual } => write!(
                formatter,
                "Sidecar acknowledged applied index {actual}, below required index {required}"
            ),
            Self::AppliedIndexRegressed { cached, actual } => write!(
                formatter,
                "Sidecar applied index regressed from {cached} to {actual}"
            ),
            Self::InvalidScan(message) => {
                write!(formatter, "invalid Sidecar scan response: {message}")
            }
            Self::InvalidSnapshotResponse(message) => {
                write!(formatter, "invalid Sidecar snapshot response: {message}")
            }
        }
    }
}

impl Error for SidecarClientError {}

impl From<SidecarClientError> for AdapterError {
    fn from(error: SidecarClientError) -> Self {
        Self::Backend(error.to_string())
    }
}

pub struct SidecarAdapter<T> {
    transport: T,
    descriptor: AdapterDescriptorV1,
    applied_log_index: AtomicU64,
}

impl<T: SidecarTransport> SidecarAdapter<T> {
    pub async fn connect(transport: T) -> Result<Self, SidecarClientError> {
        let descriptor = match transport.call(Request::Describe).await? {
            Response::Descriptor(descriptor) => descriptor,
            Response::Error(error) => return Err(SidecarClientError::Remote(error)),
            response => {
                return Err(unexpected_response("describe", &response));
            }
        };
        if descriptor.spi_version() != ADAPTER_SPI_VERSION {
            return Err(SidecarClientError::NotReady(format!(
                "Adapter SPI version {} differs from required version {}",
                descriptor.spi_version(),
                ADAPTER_SPI_VERSION
            )));
        }
        match transport.call(Request::Health).await? {
            Response::Health(status) if status.ready => {}
            Response::Health(status) => return Err(SidecarClientError::NotReady(status.detail)),
            Response::Error(error) => return Err(SidecarClientError::Remote(error)),
            response => return Err(unexpected_response("health", &response)),
        }
        let applied_log_index = match transport.call(Request::AppliedLogIndex).await? {
            Response::AppliedLogIndex(index) => index,
            Response::Error(error) => return Err(SidecarClientError::Remote(error)),
            response => return Err(unexpected_response("applied-log-index", &response)),
        };
        Ok(Self {
            transport,
            descriptor,
            applied_log_index: AtomicU64::new(applied_log_index),
        })
    }

    fn validate_index(&self, required: u64, actual: u64) -> Result<(), SidecarClientError> {
        if actual < required {
            return Err(SidecarClientError::AppliedIndexBehind { required, actual });
        }
        let cached = self.applied_log_index.load(Ordering::Acquire);
        if actual < cached {
            return Err(SidecarClientError::AppliedIndexRegressed { cached, actual });
        }
        self.applied_log_index.fetch_max(actual, Ordering::AcqRel);
        Ok(())
    }
}

impl<T: SidecarTransport> StorageAdapter for SidecarAdapter<T> {
    fn descriptor(&self) -> AdapterDescriptorV1 {
        self.descriptor.clone()
    }

    fn capabilities(&self) -> AdapterCapabilities {
        self.descriptor.capabilities()
    }

    fn apply_committed<'a>(
        &'a self,
        batch: CommittedMutationBatch,
    ) -> AdapterFuture<'a, ApplyReceipt> {
        Box::pin(async move {
            let required_index = batch.log_index;
            let response = self
                .transport
                .call(Request::Apply(batch))
                .await
                .map_err(AdapterError::from)?;
            match response {
                Response::Apply(receipt) => {
                    self.validate_index(required_index, receipt.applied_log_index)
                        .map_err(AdapterError::from)?;
                    Ok(receipt)
                }
                Response::Error(error) => {
                    Err(AdapterError::from(SidecarClientError::Remote(error)))
                }
                response => Err(AdapterError::from(unexpected_response("apply", &response))),
            }
        })
    }

    fn multi_get<'a>(&'a self, keys: &'a [LogicalKey]) -> AdapterFuture<'a, Vec<Option<Vec<u8>>>> {
        Box::pin(async move {
            let expected = keys.len();
            let response = self
                .transport
                .call(Request::MultiGet(keys.to_vec()))
                .await
                .map_err(AdapterError::from)?;
            match response {
                Response::MultiGet(values) if values.len() == expected => Ok(values),
                Response::MultiGet(values) => Err(AdapterError::from(
                    SidecarClientError::InvalidMultiGetCount {
                        expected,
                        actual: values.len(),
                    },
                )),
                Response::Error(error) => {
                    Err(AdapterError::from(SidecarClientError::Remote(error)))
                }
                response => Err(AdapterError::from(unexpected_response(
                    "multi-get",
                    &response,
                ))),
            }
        })
    }

    fn scan<'a>(&'a self, span: &'a KeySpan) -> AdapterFuture<'a, Vec<KeyValue>> {
        Box::pin(async move {
            let response = self
                .transport
                .call(Request::Scan(span.clone()))
                .await
                .map_err(AdapterError::from)?;
            match response {
                Response::Scan(values) => {
                    validate_scan_response(span, &values).map_err(AdapterError::from)?;
                    Ok(values)
                }
                Response::Error(error) => {
                    Err(AdapterError::from(SidecarClientError::Remote(error)))
                }
                response => Err(AdapterError::from(unexpected_response("scan", &response))),
            }
        })
    }

    fn begin_logical_export<'a>(
        &'a self,
        request: LogicalSnapshotExportRequest,
    ) -> AdapterFuture<'a, Box<dyn LogicalSnapshotReader + 'a>> {
        Box::pin(async move {
            let response = self
                .transport
                .call(Request::BeginExport(BeginExportRequest {
                    limits: request,
                    expected_applied_log_index: Some(
                        self.applied_log_index.load(Ordering::Acquire),
                    ),
                }))
                .await
                .map_err(AdapterError::from)?;
            match response {
                Response::ExportStarted(started) => {
                    if started.limits != request {
                        return Err(AdapterError::from(
                            SidecarClientError::InvalidSnapshotResponse(
                                "Sidecar changed logical export limits".into(),
                            ),
                        ));
                    }
                    Ok(Box::new(SidecarLogicalSnapshotReader {
                        transport: &self.transport,
                        session_id: started.session_id,
                        header: started.header,
                        next_ordinal: 0,
                        manifest: None,
                    })
                        as Box<dyn LogicalSnapshotReader + 'a>)
                }
                Response::Error(error) => {
                    Err(AdapterError::from(SidecarClientError::Remote(error)))
                }
                response => Err(AdapterError::from(unexpected_response(
                    "begin-export",
                    &response,
                ))),
            }
        })
    }

    fn applied_log_index(&self) -> Result<u64, AdapterError> {
        Ok(self.applied_log_index.load(Ordering::Acquire))
    }
}

struct SidecarLogicalSnapshotReader<'transport, T> {
    transport: &'transport T,
    session_id: u128,
    header: LogicalSnapshotHeaderV1,
    next_ordinal: u64,
    manifest: Option<LogicalSnapshotManifestV1>,
}

impl<T: SidecarTransport> LogicalSnapshotReader for SidecarLogicalSnapshotReader<'_, T> {
    fn header(&self) -> &LogicalSnapshotHeaderV1 {
        &self.header
    }

    fn next_chunk<'a>(&'a mut self) -> AdapterFuture<'a, Option<LogicalSnapshotChunkV1>> {
        Box::pin(async move {
            if self.manifest.is_some() {
                return Ok(None);
            }
            let response = self
                .transport
                .call(Request::ExportNext {
                    session_id: self.session_id,
                    expected_ordinal: self.next_ordinal,
                })
                .await
                .map_err(AdapterError::from)?;
            match response {
                Response::ExportChunk { session_id, chunk }
                    if session_id == self.session_id && chunk.ordinal() == self.next_ordinal =>
                {
                    self.next_ordinal = self
                        .next_ordinal
                        .checked_add(1)
                        .ok_or(storage_api::LogicalSnapshotError::CountOverflow)?;
                    Ok(Some(chunk))
                }
                Response::ExportComplete {
                    session_id,
                    manifest,
                } if session_id == self.session_id => {
                    if manifest.header() != &self.header
                        || manifest.total_chunks() != self.next_ordinal
                    {
                        return Err(AdapterError::from(
                            SidecarClientError::InvalidSnapshotResponse(
                                "Sidecar export manifest does not match the session".into(),
                            ),
                        ));
                    }
                    self.manifest = Some(manifest);
                    Ok(None)
                }
                Response::Error(error) => {
                    Err(AdapterError::from(SidecarClientError::Remote(error)))
                }
                response => Err(AdapterError::from(unexpected_response(
                    "export-next",
                    &response,
                ))),
            }
        })
    }

    fn finish<'a>(self: Box<Self>) -> AdapterFuture<'a, LogicalSnapshotManifestV1>
    where
        Self: 'a,
    {
        Box::pin(async move {
            self.manifest.ok_or_else(|| {
                AdapterError::from(SidecarClientError::InvalidSnapshotResponse(
                    "Sidecar export was not exhausted before finish".into(),
                ))
            })
        })
    }
}

fn validate_scan_response(span: &KeySpan, values: &[KeyValue]) -> Result<(), SidecarClientError> {
    if span.limit().is_some_and(|limit| values.len() > limit) {
        return Err(SidecarClientError::InvalidScan(format!(
            "returned {} rows above limit {}",
            values.len(),
            span.limit().expect("checked limit")
        )));
    }
    let mut previous: Option<&LogicalKey> = None;
    for value in values {
        let key = value.key();
        if key.keyspace() != span.keyspace() || !span.contains(key.as_bytes()) {
            return Err(SidecarClientError::InvalidScan(
                "row lies outside the requested key span".to_owned(),
            ));
        }
        if previous.is_some_and(|previous| previous >= key) {
            return Err(SidecarClientError::InvalidScan(
                "rows are not in strict key order".to_owned(),
            ));
        }
        previous = Some(key);
    }
    Ok(())
}

fn unexpected_response(operation: &'static str, response: &Response) -> SidecarClientError {
    SidecarClientError::UnexpectedResponse {
        operation,
        response: response_name(response),
    }
}

const fn response_name(response: &Response) -> &'static str {
    match response {
        Response::Hello(_) => "hello",
        Response::Descriptor(_) => "descriptor",
        Response::Apply(_) => "apply",
        Response::MultiGet(_) => "multi-get",
        Response::Scan(_) => "scan",
        Response::AppliedLogIndex(_) => "applied-log-index",
        Response::Health(_) => "health",
        Response::ExportStarted(_) => "export-started",
        Response::ExportChunk { .. } => "export-chunk",
        Response::ExportComplete { .. } => "export-complete",
        Response::RestoreStarted(_) => "restore-started",
        Response::RestoreChunkAccepted { .. } => "restore-chunk-accepted",
        Response::RestoreComplete(_) => "restore-complete",
        Response::SessionAborted { .. } => "session-aborted",
        Response::Error(_) => "error",
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Frame<T> {
    kind: FrameKind,
    request_id: u128,
    message: T,
}

impl<T> Frame<T> {
    #[must_use]
    pub const fn kind(&self) -> FrameKind {
        self.kind
    }

    #[must_use]
    pub const fn request_id(&self) -> u128 {
        self.request_id
    }

    #[must_use]
    pub const fn message(&self) -> &T {
        &self.message
    }

    #[must_use]
    pub fn into_message(self) -> T {
        self.message
    }
}

pub trait ProtocolMessage: Sized {
    const KIND: FrameKind;

    fn encode_payload(&self) -> Result<Vec<u8>, ProtocolError>;
    fn decode_payload(payload: &[u8]) -> Result<Self, ProtocolError>;
}

pub fn encode_frame<T: ProtocolMessage>(
    request_id: u128,
    message: &T,
) -> Result<Vec<u8>, ProtocolError> {
    let payload = message.encode_payload()?;
    if payload.len() > MAX_FRAME_PAYLOAD_BYTES {
        return Err(ProtocolError::PayloadTooLarge {
            max: MAX_FRAME_PAYLOAD_BYTES,
            actual: payload.len(),
        });
    }
    let payload_length = u32::try_from(payload.len()).map_err(|_| ProtocolError::LengthOverflow)?;
    let mut frame = Vec::with_capacity(HEADER_BYTES + payload.len() + CHECKSUM_BYTES);
    frame.extend_from_slice(&MAGIC);
    frame.extend_from_slice(&WIRE_VERSION.to_be_bytes());
    frame.push(T::KIND as u8);
    frame.push(0);
    frame.extend_from_slice(&request_id.to_be_bytes());
    frame.extend_from_slice(&payload_length.to_be_bytes());
    frame.extend_from_slice(&payload);
    frame.extend_from_slice(&crc32fast::hash(&frame).to_be_bytes());
    Ok(frame)
}

pub fn decode_frame<T: ProtocolMessage>(bytes: &[u8]) -> Result<Frame<T>, ProtocolError> {
    if bytes.len() < HEADER_BYTES + CHECKSUM_BYTES {
        return Err(ProtocolError::TruncatedFrame);
    }
    if bytes[..4] != MAGIC {
        return Err(ProtocolError::InvalidMagic);
    }
    let version = u16::from_be_bytes(bytes[4..6].try_into().expect("fixed version slice"));
    if version != WIRE_VERSION {
        return Err(ProtocolError::UnsupportedWireVersion { version });
    }
    let kind = decode_frame_kind(bytes[6])?;
    if kind != T::KIND {
        return Err(ProtocolError::UnexpectedFrameKind {
            expected: T::KIND,
            actual: kind,
        });
    }
    if bytes[7] != 0 {
        return Err(ProtocolError::UnknownFlags { flags: bytes[7] });
    }
    let request_id = u128::from_be_bytes(
        bytes[8..24]
            .try_into()
            .expect("fixed request identifier slice"),
    );
    let payload_length = u32::from_be_bytes(
        bytes[24..28]
            .try_into()
            .expect("fixed payload length slice"),
    );
    let payload_length =
        usize::try_from(payload_length).map_err(|_| ProtocolError::LengthOverflow)?;
    if payload_length > MAX_FRAME_PAYLOAD_BYTES {
        return Err(ProtocolError::PayloadTooLarge {
            max: MAX_FRAME_PAYLOAD_BYTES,
            actual: payload_length,
        });
    }
    let expected_length = HEADER_BYTES
        .checked_add(payload_length)
        .and_then(|length| length.checked_add(CHECKSUM_BYTES))
        .ok_or(ProtocolError::LengthOverflow)?;
    if bytes.len() != expected_length {
        return Err(ProtocolError::FrameLengthMismatch {
            expected: expected_length,
            actual: bytes.len(),
        });
    }
    let checksum_offset = expected_length - CHECKSUM_BYTES;
    let expected_checksum = u32::from_be_bytes(
        bytes[checksum_offset..]
            .try_into()
            .expect("fixed checksum slice"),
    );
    if crc32fast::hash(&bytes[..checksum_offset]) != expected_checksum {
        return Err(ProtocolError::ChecksumMismatch);
    }
    let payload = &bytes[HEADER_BYTES..checksum_offset];
    let message = T::decode_payload(payload)?;
    if message.encode_payload()? != payload {
        return Err(ProtocolError::NonCanonicalPayload);
    }
    Ok(Frame {
        kind,
        request_id,
        message,
    })
}

pub fn write_frame<W: Write, T: ProtocolMessage>(
    writer: &mut W,
    request_id: u128,
    message: &T,
) -> Result<(), ProtocolError> {
    writer.write_all(&encode_frame(request_id, message)?)?;
    writer.flush()?;
    Ok(())
}

pub fn read_frame<R: Read, T: ProtocolMessage>(reader: &mut R) -> Result<Frame<T>, ProtocolError> {
    read_frame_or_eof(reader)?.ok_or(ProtocolError::TruncatedFrame)
}

/// Reads one frame while distinguishing a clean close between frames from a
/// truncated header or payload.
pub fn read_frame_or_eof<R: Read, T: ProtocolMessage>(
    reader: &mut R,
) -> Result<Option<Frame<T>>, ProtocolError> {
    let mut header = [0_u8; HEADER_BYTES];
    loop {
        match reader.read(&mut header[..1]) {
            Ok(0) => return Ok(None),
            Ok(1) => break,
            Ok(_) => unreachable!("one-byte read returned more than one byte"),
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error.into()),
        }
    }
    reader.read_exact(&mut header[1..])?;
    let payload_length = u32::from_be_bytes(
        header[24..28]
            .try_into()
            .expect("fixed payload length slice"),
    );
    let payload_length =
        usize::try_from(payload_length).map_err(|_| ProtocolError::LengthOverflow)?;
    if payload_length > MAX_FRAME_PAYLOAD_BYTES {
        return Err(ProtocolError::PayloadTooLarge {
            max: MAX_FRAME_PAYLOAD_BYTES,
            actual: payload_length,
        });
    }
    let mut frame = Vec::with_capacity(HEADER_BYTES + payload_length + CHECKSUM_BYTES);
    frame.extend_from_slice(&header);
    let suffix_length = payload_length
        .checked_add(CHECKSUM_BYTES)
        .ok_or(ProtocolError::LengthOverflow)?;
    frame.resize(HEADER_BYTES + suffix_length, 0);
    reader.read_exact(&mut frame[HEADER_BYTES..])?;
    decode_frame(&frame).map(Some)
}

fn decode_frame_kind(tag: u8) -> Result<FrameKind, ProtocolError> {
    match tag {
        1 => Ok(FrameKind::Request),
        2 => Ok(FrameKind::Response),
        _ => Err(ProtocolError::UnknownFrameKind { tag }),
    }
}

impl ProtocolMessage for Request {
    const KIND: FrameKind = FrameKind::Request;

    fn encode_payload(&self) -> Result<Vec<u8>, ProtocolError> {
        let body = match self {
            Self::Hello(request) => wire::request_envelope::Body::Hello(wire::HelloRequest {
                required_features: request.required_features.bits(),
                optional_features: request.optional_features.bits(),
                max_payload_bytes: request.max_payload_bytes,
            }),
            Self::Describe => wire::request_envelope::Body::Describe(true),
            Self::Apply(batch) => wire::request_envelope::Body::Apply(encode_batch(batch)?),
            Self::MultiGet(keys) => wire::request_envelope::Body::MultiGet(wire::MultiGetRequest {
                keys: keys.iter().map(encode_key).collect(),
            }),
            Self::Scan(span) => wire::request_envelope::Body::Scan(encode_span(span)?),
            Self::AppliedLogIndex => wire::request_envelope::Body::AppliedLogIndex(true),
            Self::Health => wire::request_envelope::Body::Health(true),
            Self::BeginExport(request) => {
                wire::request_envelope::Body::BeginExport(wire::BeginExportRequest {
                    limits: Some(encode_snapshot_limits(request.limits)?),
                    expected_applied_log_index: request.expected_applied_log_index,
                })
            }
            Self::ExportNext {
                session_id,
                expected_ordinal,
            } => wire::request_envelope::Body::ExportNext(wire::ExportNextRequest {
                session_id: encode_u128(*session_id),
                expected_ordinal: *expected_ordinal,
            }),
            Self::BeginRestore(request) => {
                wire::request_envelope::Body::BeginRestore(wire::BeginRestoreRequest {
                    header: Some(encode_snapshot_header(&request.header)),
                    target: Some(encode_public_open_request(&request.target)),
                })
            }
            Self::RestoreChunk { session_id, chunk } => {
                wire::request_envelope::Body::RestoreChunk(wire::RestoreChunkRequest {
                    session_id: encode_u128(*session_id),
                    chunk: Some(encode_snapshot_chunk(chunk)),
                })
            }
            Self::FinishRestore {
                session_id,
                manifest,
            } => wire::request_envelope::Body::FinishRestore(wire::FinishRestoreRequest {
                session_id: encode_u128(*session_id),
                manifest: Some(encode_snapshot_manifest(manifest)),
            }),
            Self::AbortSession { session_id } => {
                wire::request_envelope::Body::AbortSession(wire::AbortSessionRequest {
                    session_id: encode_u128(*session_id),
                })
            }
        };
        encode_protobuf(&wire::RequestEnvelope {
            spi_version: u32::from(ADAPTER_SPI_VERSION),
            body: Some(body),
        })
    }

    fn decode_payload(payload: &[u8]) -> Result<Self, ProtocolError> {
        let envelope = wire::RequestEnvelope::decode(payload)
            .map_err(|error| ProtocolError::PayloadDecode(error.to_string()))?;
        let spi_version = u16::try_from(envelope.spi_version)
            .map_err(|_| ProtocolError::InvalidSpiVersion(envelope.spi_version))?;
        if spi_version != ADAPTER_SPI_VERSION {
            return Err(ProtocolError::UnsupportedSpiVersion {
                expected: ADAPTER_SPI_VERSION,
                actual: spi_version,
            });
        }
        match envelope.body.ok_or(ProtocolError::MissingBody)? {
            wire::request_envelope::Body::Hello(request) => Ok(Self::Hello(HelloRequest {
                required_features: FeatureSet::from_bits(request.required_features)?,
                optional_features: FeatureSet::from_bits(request.optional_features)?,
                max_payload_bytes: request.max_payload_bytes,
            })),
            wire::request_envelope::Body::Describe(true) => Ok(Self::Describe),
            wire::request_envelope::Body::Apply(batch) => Ok(Self::Apply(decode_batch(batch)?)),
            wire::request_envelope::Body::MultiGet(request) => Ok(Self::MultiGet(
                request
                    .keys
                    .into_iter()
                    .map(decode_key)
                    .collect::<Result<_, _>>()?,
            )),
            wire::request_envelope::Body::Scan(span) => Ok(Self::Scan(decode_span(span)?)),
            wire::request_envelope::Body::AppliedLogIndex(true) => Ok(Self::AppliedLogIndex),
            wire::request_envelope::Body::Health(true) => Ok(Self::Health),
            wire::request_envelope::Body::BeginExport(request) => {
                Ok(Self::BeginExport(BeginExportRequest {
                    limits: decode_snapshot_limits(
                        request
                            .limits
                            .ok_or(ProtocolError::MissingSnapshotField("export limits"))?,
                    )?,
                    expected_applied_log_index: request.expected_applied_log_index,
                }))
            }
            wire::request_envelope::Body::ExportNext(request) => Ok(Self::ExportNext {
                session_id: decode_session_id(&request.session_id)?,
                expected_ordinal: request.expected_ordinal,
            }),
            wire::request_envelope::Body::BeginRestore(request) => {
                Ok(Self::BeginRestore(BeginRestoreRequest {
                    header: decode_snapshot_header(
                        request
                            .header
                            .ok_or(ProtocolError::MissingSnapshotField("restore header"))?,
                    )?,
                    target: decode_public_open_request(
                        request
                            .target
                            .ok_or(ProtocolError::MissingSnapshotField("restore target"))?,
                    )?,
                }))
            }
            wire::request_envelope::Body::RestoreChunk(request) => Ok(Self::RestoreChunk {
                session_id: decode_session_id(&request.session_id)?,
                chunk: decode_snapshot_chunk(
                    request
                        .chunk
                        .ok_or(ProtocolError::MissingSnapshotField("restore chunk"))?,
                )?,
            }),
            wire::request_envelope::Body::FinishRestore(request) => Ok(Self::FinishRestore {
                session_id: decode_session_id(&request.session_id)?,
                manifest: decode_snapshot_manifest(
                    request
                        .manifest
                        .ok_or(ProtocolError::MissingSnapshotField("restore manifest"))?,
                )?,
            }),
            wire::request_envelope::Body::AbortSession(request) => Ok(Self::AbortSession {
                session_id: decode_session_id(&request.session_id)?,
            }),
            _ => Err(ProtocolError::NonCanonicalBody),
        }
    }
}

impl ProtocolMessage for Response {
    const KIND: FrameKind = FrameKind::Response;

    fn encode_payload(&self) -> Result<Vec<u8>, ProtocolError> {
        let body = match self {
            Self::Hello(response) => {
                if response.wire_version != WIRE_VERSION {
                    return Err(ProtocolError::UnsupportedWireVersion {
                        version: response.wire_version,
                    });
                }
                if response.snapshot_format_version != LOGICAL_SNAPSHOT_FORMAT_VERSION {
                    return Err(ProtocolError::UnsupportedSnapshotFormat {
                        expected: LOGICAL_SNAPSHOT_FORMAT_VERSION,
                        actual: u32::from(response.snapshot_format_version),
                    });
                }
                validate_chunk_limits(response.max_chunk_entries, response.max_chunk_bytes)?;
                wire::response_envelope::Body::Hello(wire::HelloResponse {
                    wire_version: u32::from(response.wire_version),
                    negotiated_features: response.negotiated_features.bits(),
                    max_payload_bytes: response.max_payload_bytes,
                    max_chunk_bytes: response.max_chunk_bytes,
                    max_chunk_entries: response.max_chunk_entries,
                    snapshot_format_version: u32::from(response.snapshot_format_version),
                })
            }
            Self::Descriptor(descriptor) => {
                wire::response_envelope::Body::Descriptor(encode_descriptor(descriptor))
            }
            Self::Apply(receipt) => wire::response_envelope::Body::Apply(wire::ApplyResponse {
                applied_log_index: receipt.applied_log_index,
                duplicate: receipt.duplicate,
            }),
            Self::MultiGet(values) => {
                wire::response_envelope::Body::MultiGet(wire::MultiGetResponse {
                    values: values
                        .iter()
                        .map(|value| wire::OptionalBytes {
                            present: value.is_some(),
                            value: value.clone().unwrap_or_default(),
                        })
                        .collect(),
                })
            }
            Self::Scan(values) => wire::response_envelope::Body::Scan(wire::ScanResponse {
                values: values.iter().map(encode_key_value).collect(),
            }),
            Self::AppliedLogIndex(index) => wire::response_envelope::Body::AppliedLogIndex(*index),
            Self::Health(status) => wire::response_envelope::Body::Health(wire::HealthResponse {
                ready: status.ready,
                detail: status.detail.clone(),
            }),
            Self::ExportStarted(started) => {
                wire::response_envelope::Body::ExportStarted(wire::ExportStartedResponse {
                    session_id: encode_u128(started.session_id),
                    header: Some(encode_snapshot_header(&started.header)),
                    limits: Some(encode_snapshot_limits(started.limits)?),
                })
            }
            Self::ExportChunk { session_id, chunk } => {
                wire::response_envelope::Body::ExportChunk(wire::ExportChunkResponse {
                    session_id: encode_u128(*session_id),
                    chunk: Some(encode_snapshot_chunk(chunk)),
                })
            }
            Self::ExportComplete {
                session_id,
                manifest,
            } => wire::response_envelope::Body::ExportComplete(wire::ExportCompleteResponse {
                session_id: encode_u128(*session_id),
                manifest: Some(encode_snapshot_manifest(manifest)),
            }),
            Self::RestoreStarted(started) => {
                validate_chunk_limits(started.max_chunk_entries, started.max_chunk_bytes)?;
                wire::response_envelope::Body::RestoreStarted(wire::RestoreStartedResponse {
                    session_id: encode_u128(started.session_id),
                    prospective_descriptor: Some(encode_descriptor(
                        &started.prospective_descriptor,
                    )),
                    max_chunk_bytes: started.max_chunk_bytes,
                    max_chunk_entries: started.max_chunk_entries,
                })
            }
            Self::RestoreChunkAccepted {
                session_id,
                ordinal,
                digest,
            } => wire::response_envelope::Body::RestoreChunkAccepted(
                wire::RestoreChunkAcceptedResponse {
                    session_id: encode_u128(*session_id),
                    ordinal: *ordinal,
                    digest: digest.to_vec(),
                },
            ),
            Self::RestoreComplete(complete) => {
                wire::response_envelope::Body::RestoreComplete(wire::RestoreCompleteResponse {
                    session_id: encode_u128(complete.session_id),
                    final_descriptor: Some(encode_descriptor(&complete.final_descriptor)),
                    applied_log_index: complete.applied_log_index,
                })
            }
            Self::SessionAborted { session_id } => {
                wire::response_envelope::Body::SessionAborted(wire::SessionAbortedResponse {
                    session_id: encode_u128(*session_id),
                })
            }
            Self::Error(error) => wire::response_envelope::Body::Error(wire::ErrorResponse {
                code: error.code,
                message: error.message.clone(),
                retryable: error.retryable,
            }),
        };
        encode_protobuf(&wire::ResponseEnvelope {
            spi_version: u32::from(ADAPTER_SPI_VERSION),
            body: Some(body),
        })
    }

    fn decode_payload(payload: &[u8]) -> Result<Self, ProtocolError> {
        let envelope = wire::ResponseEnvelope::decode(payload)
            .map_err(|error| ProtocolError::PayloadDecode(error.to_string()))?;
        let spi_version = u16::try_from(envelope.spi_version)
            .map_err(|_| ProtocolError::InvalidSpiVersion(envelope.spi_version))?;
        if spi_version != ADAPTER_SPI_VERSION {
            return Err(ProtocolError::UnsupportedSpiVersion {
                expected: ADAPTER_SPI_VERSION,
                actual: spi_version,
            });
        }
        match envelope.body.ok_or(ProtocolError::MissingBody)? {
            wire::response_envelope::Body::Hello(response) => {
                let wire_version = u16::try_from(response.wire_version)
                    .map_err(|_| ProtocolError::InvalidWireVersion(response.wire_version))?;
                if wire_version != WIRE_VERSION {
                    return Err(ProtocolError::UnsupportedWireVersion {
                        version: wire_version,
                    });
                }
                let snapshot_format_version = u16::try_from(response.snapshot_format_version)
                    .map_err(|_| ProtocolError::UnsupportedSnapshotFormat {
                        expected: LOGICAL_SNAPSHOT_FORMAT_VERSION,
                        actual: response.snapshot_format_version,
                    })?;
                if snapshot_format_version != LOGICAL_SNAPSHOT_FORMAT_VERSION {
                    return Err(ProtocolError::UnsupportedSnapshotFormat {
                        expected: LOGICAL_SNAPSHOT_FORMAT_VERSION,
                        actual: response.snapshot_format_version,
                    });
                }
                validate_chunk_limits(response.max_chunk_entries, response.max_chunk_bytes)?;
                Ok(Self::Hello(HelloResponse {
                    wire_version,
                    negotiated_features: FeatureSet::from_bits(response.negotiated_features)?,
                    max_payload_bytes: response.max_payload_bytes,
                    max_chunk_bytes: response.max_chunk_bytes,
                    max_chunk_entries: response.max_chunk_entries,
                    snapshot_format_version,
                }))
            }
            wire::response_envelope::Body::Descriptor(descriptor) => {
                Ok(Self::Descriptor(decode_descriptor(descriptor)?))
            }
            wire::response_envelope::Body::Apply(receipt) => Ok(Self::Apply(ApplyReceipt {
                applied_log_index: receipt.applied_log_index,
                duplicate: receipt.duplicate,
            })),
            wire::response_envelope::Body::MultiGet(response) => Ok(Self::MultiGet(
                response
                    .values
                    .into_iter()
                    .map(|value| value.present.then_some(value.value))
                    .collect(),
            )),
            wire::response_envelope::Body::Scan(response) => Ok(Self::Scan(
                response
                    .values
                    .into_iter()
                    .map(decode_key_value)
                    .collect::<Result<_, _>>()?,
            )),
            wire::response_envelope::Body::AppliedLogIndex(index) => {
                Ok(Self::AppliedLogIndex(index))
            }
            wire::response_envelope::Body::Health(status) => Ok(Self::Health(HealthStatus {
                ready: status.ready,
                detail: status.detail,
            })),
            wire::response_envelope::Body::ExportStarted(started) => {
                Ok(Self::ExportStarted(ExportStarted {
                    session_id: decode_session_id(&started.session_id)?,
                    header: decode_snapshot_header(
                        started
                            .header
                            .ok_or(ProtocolError::MissingSnapshotField("export header"))?,
                    )?,
                    limits: decode_snapshot_limits(
                        started
                            .limits
                            .ok_or(ProtocolError::MissingSnapshotField("export limits"))?,
                    )?,
                }))
            }
            wire::response_envelope::Body::ExportChunk(response) => Ok(Self::ExportChunk {
                session_id: decode_session_id(&response.session_id)?,
                chunk: decode_snapshot_chunk(
                    response
                        .chunk
                        .ok_or(ProtocolError::MissingSnapshotField("export chunk"))?,
                )?,
            }),
            wire::response_envelope::Body::ExportComplete(response) => Ok(Self::ExportComplete {
                session_id: decode_session_id(&response.session_id)?,
                manifest: decode_snapshot_manifest(
                    response
                        .manifest
                        .ok_or(ProtocolError::MissingSnapshotField("export manifest"))?,
                )?,
            }),
            wire::response_envelope::Body::RestoreStarted(started) => {
                validate_chunk_limits(started.max_chunk_entries, started.max_chunk_bytes)?;
                Ok(Self::RestoreStarted(RestoreStarted {
                    session_id: decode_session_id(&started.session_id)?,
                    prospective_descriptor: decode_descriptor(
                        started.prospective_descriptor.ok_or(
                            ProtocolError::MissingSnapshotField("prospective descriptor"),
                        )?,
                    )?,
                    max_chunk_bytes: started.max_chunk_bytes,
                    max_chunk_entries: started.max_chunk_entries,
                }))
            }
            wire::response_envelope::Body::RestoreChunkAccepted(response) => {
                Ok(Self::RestoreChunkAccepted {
                    session_id: decode_session_id(&response.session_id)?,
                    ordinal: response.ordinal,
                    digest: decode_digest(&response.digest)?,
                })
            }
            wire::response_envelope::Body::RestoreComplete(complete) => {
                Ok(Self::RestoreComplete(RestoreComplete {
                    session_id: decode_session_id(&complete.session_id)?,
                    final_descriptor: decode_descriptor(
                        complete
                            .final_descriptor
                            .ok_or(ProtocolError::MissingSnapshotField("final descriptor"))?,
                    )?,
                    applied_log_index: complete.applied_log_index,
                }))
            }
            wire::response_envelope::Body::SessionAborted(response) => Ok(Self::SessionAborted {
                session_id: decode_session_id(&response.session_id)?,
            }),
            wire::response_envelope::Body::Error(error) => Ok(Self::Error(RemoteError {
                code: error.code,
                message: error.message,
                retryable: error.retryable,
            })),
        }
    }
}

fn encode_protobuf<M: Message>(message: &M) -> Result<Vec<u8>, ProtocolError> {
    let length = message.encoded_len();
    if length > MAX_FRAME_PAYLOAD_BYTES {
        return Err(ProtocolError::PayloadTooLarge {
            max: MAX_FRAME_PAYLOAD_BYTES,
            actual: length,
        });
    }
    let mut bytes = Vec::with_capacity(length);
    message
        .encode(&mut bytes)
        .map_err(|error| ProtocolError::PayloadEncode(error.to_string()))?;
    Ok(bytes)
}

fn encode_u128(value: u128) -> Vec<u8> {
    value.to_be_bytes().to_vec()
}

fn decode_session_id(bytes: &[u8]) -> Result<u128, ProtocolError> {
    let array: [u8; 16] = bytes
        .try_into()
        .map_err(|_| ProtocolError::InvalidSessionIdLength {
            actual: bytes.len(),
        })?;
    Ok(u128::from_be_bytes(array))
}

fn decode_snapshot_id(bytes: &[u8]) -> Result<u128, ProtocolError> {
    let array: [u8; 16] = bytes
        .try_into()
        .map_err(|_| ProtocolError::InvalidSnapshotIdLength {
            actual: bytes.len(),
        })?;
    Ok(u128::from_be_bytes(array))
}

fn decode_digest(bytes: &[u8]) -> Result<[u8; 32], ProtocolError> {
    bytes
        .try_into()
        .map_err(|_| ProtocolError::InvalidDigestLength {
            actual: bytes.len(),
        })
}

fn encode_snapshot_limits(
    limits: LogicalSnapshotExportRequest,
) -> Result<wire::SnapshotLimits, ProtocolError> {
    Ok(wire::SnapshotLimits {
        max_entries: u32::try_from(limits.max_entries_per_chunk())
            .map_err(|_| ProtocolError::LengthOverflow)?,
        max_bytes: u32::try_from(limits.max_bytes_per_chunk())
            .map_err(|_| ProtocolError::LengthOverflow)?,
    })
}

fn decode_snapshot_limits(
    limits: wire::SnapshotLimits,
) -> Result<LogicalSnapshotExportRequest, ProtocolError> {
    LogicalSnapshotExportRequest::new(
        usize::try_from(limits.max_entries).map_err(|_| ProtocolError::LengthOverflow)?,
        usize::try_from(limits.max_bytes).map_err(|_| ProtocolError::LengthOverflow)?,
    )
    .map_err(|error| ProtocolError::InvalidChunkLimits(error.to_string()))
}

fn validate_chunk_limits(max_entries: u32, max_bytes: u32) -> Result<(), ProtocolError> {
    decode_snapshot_limits(wire::SnapshotLimits {
        max_entries,
        max_bytes,
    })
    .map(drop)
}

fn encode_snapshot_header(header: &LogicalSnapshotHeaderV1) -> wire::SnapshotHeader {
    wire::SnapshotHeader {
        format_version: u32::from(header.format_version()),
        snapshot_id: encode_u128(header.snapshot_id()),
        applied_log_index: header.applied_log_index(),
    }
}

fn decode_snapshot_header(
    header: wire::SnapshotHeader,
) -> Result<LogicalSnapshotHeaderV1, ProtocolError> {
    if header.format_version != u32::from(LOGICAL_SNAPSHOT_FORMAT_VERSION) {
        return Err(ProtocolError::UnsupportedSnapshotFormat {
            expected: LOGICAL_SNAPSHOT_FORMAT_VERSION,
            actual: header.format_version,
        });
    }
    Ok(LogicalSnapshotHeaderV1::new(
        decode_snapshot_id(&header.snapshot_id)?,
        header.applied_log_index,
    ))
}

fn encode_snapshot_chunk(chunk: &LogicalSnapshotChunkV1) -> wire::SnapshotChunk {
    wire::SnapshotChunk {
        snapshot_id: encode_u128(chunk.snapshot_id()),
        ordinal: chunk.ordinal(),
        entries: chunk.entries().iter().map(encode_key_value).collect(),
        digest: chunk.digest().to_vec(),
    }
}

fn decode_snapshot_chunk(
    chunk: wire::SnapshotChunk,
) -> Result<LogicalSnapshotChunkV1, ProtocolError> {
    let entries = chunk
        .entries
        .into_iter()
        .map(decode_key_value)
        .collect::<Result<_, _>>()?;
    LogicalSnapshotChunkV1::from_parts(
        decode_snapshot_id(&chunk.snapshot_id)?,
        chunk.ordinal,
        entries,
        decode_digest(&chunk.digest)?,
    )
    .map_err(|error| ProtocolError::InvalidSnapshot(error.to_string()))
}

fn encode_snapshot_manifest(manifest: &LogicalSnapshotManifestV1) -> wire::SnapshotManifest {
    wire::SnapshotManifest {
        header: Some(encode_snapshot_header(manifest.header())),
        total_chunks: manifest.total_chunks(),
        total_entries: manifest.total_entries(),
        content_digest: manifest.content_digest().to_vec(),
    }
}

fn decode_snapshot_manifest(
    manifest: wire::SnapshotManifest,
) -> Result<LogicalSnapshotManifestV1, ProtocolError> {
    let header = decode_snapshot_header(
        manifest
            .header
            .ok_or(ProtocolError::MissingSnapshotField("manifest header"))?,
    )?;
    LogicalSnapshotManifestV1::from_parts(
        header,
        manifest.total_chunks,
        manifest.total_entries,
        decode_digest(&manifest.content_digest)?,
    )
    .map_err(|error| ProtocolError::InvalidSnapshot(error.to_string()))
}

fn encode_public_open_request(
    request: &PublicAdapterOpenRequest,
) -> wire::PublicAdapterOpenRequest {
    wire::PublicAdapterOpenRequest {
        instance_id: request.instance_id.clone(),
        parameters: request
            .parameters
            .iter()
            .map(|(name, value)| wire::PublicParameter {
                name: name.clone(),
                value: value.clone(),
            })
            .collect(),
    }
}

fn decode_public_open_request(
    request: wire::PublicAdapterOpenRequest,
) -> Result<PublicAdapterOpenRequest, ProtocolError> {
    let mut parameters = BTreeMap::new();
    let mut previous_name: Option<String> = None;
    for parameter in request.parameters {
        if previous_name
            .as_ref()
            .is_some_and(|previous| previous >= &parameter.name)
        {
            return Err(ProtocolError::NonCanonicalPublicParameters);
        }
        previous_name = Some(parameter.name.clone());
        parameters.insert(parameter.name, parameter.value);
    }
    Ok(PublicAdapterOpenRequest {
        instance_id: request.instance_id,
        parameters,
    })
}

fn encode_batch(batch: &CommittedMutationBatch) -> Result<wire::ApplyRequest, ProtocolError> {
    Ok(wire::ApplyRequest {
        shard_id: batch.shard_id,
        log_index: batch.log_index,
        txn_id: batch.txn_id.to_be_bytes().to_vec(),
        mutations: batch
            .mutations
            .iter()
            .map(encode_mutation)
            .collect::<Result<_, _>>()?,
    })
}

fn decode_batch(batch: wire::ApplyRequest) -> Result<CommittedMutationBatch, ProtocolError> {
    let txn_id: [u8; 16] = batch
        .txn_id
        .try_into()
        .map_err(|_| ProtocolError::InvalidTxnIdLength)?;
    Ok(CommittedMutationBatch {
        shard_id: batch.shard_id,
        log_index: batch.log_index,
        txn_id: u128::from_be_bytes(txn_id),
        mutations: batch
            .mutations
            .into_iter()
            .map(decode_mutation)
            .collect::<Result<_, _>>()?,
    })
}

fn encode_mutation(mutation: &Mutation) -> Result<wire::Mutation, ProtocolError> {
    let (operation, key, value) = match &mutation.operation {
        MutationOperation::Put { key, value } => (1, encode_key(key), value.clone()),
        MutationOperation::Delete { key } => (2, encode_key(key), Vec::new()),
    };
    Ok(wire::Mutation {
        sequence: mutation.sequence,
        operation,
        key: Some(key),
        value,
    })
}

fn decode_mutation(mutation: wire::Mutation) -> Result<Mutation, ProtocolError> {
    let key = decode_key(mutation.key.ok_or(ProtocolError::MissingLogicalKey)?)?;
    match mutation.operation {
        1 => Ok(Mutation::put(mutation.sequence, key, mutation.value)),
        2 if mutation.value.is_empty() => Ok(Mutation::delete(mutation.sequence, key)),
        2 => Err(ProtocolError::NonCanonicalDelete),
        tag => Err(ProtocolError::UnknownMutationOperation { tag }),
    }
}

fn encode_key(key: &LogicalKey) -> wire::LogicalKey {
    wire::LogicalKey {
        keyspace: u32::from(key.keyspace().tag()),
        key: key.as_bytes().to_vec(),
    }
}

fn decode_key(key: wire::LogicalKey) -> Result<LogicalKey, ProtocolError> {
    Ok(LogicalKey::in_keyspace(
        decode_keyspace(key.keyspace)?,
        key.key,
    ))
}

fn encode_key_value(value: &KeyValue) -> wire::KeyValue {
    wire::KeyValue {
        key: Some(encode_key(value.key())),
        value: value.value().to_vec(),
    }
}

fn decode_key_value(value: wire::KeyValue) -> Result<KeyValue, ProtocolError> {
    Ok(KeyValue::new(
        decode_key(value.key.ok_or(ProtocolError::MissingLogicalKey)?)?,
        value.value,
    ))
}

fn encode_span(span: &KeySpan) -> Result<wire::ScanRequest, ProtocolError> {
    Ok(wire::ScanRequest {
        keyspace: u32::from(span.keyspace().tag()),
        start: span.start().to_vec(),
        has_end: span.end().is_some(),
        end: span.end().unwrap_or_default().to_vec(),
        has_required_prefix: span.required_prefix().is_some(),
        required_prefix: span.required_prefix().unwrap_or_default().to_vec(),
        has_limit: span.limit().is_some(),
        limit: span
            .limit()
            .map(u64::try_from)
            .transpose()
            .map_err(|_| ProtocolError::LengthOverflow)?
            .unwrap_or_default(),
    })
}

fn decode_span(span: wire::ScanRequest) -> Result<KeySpan, ProtocolError> {
    if !span.has_end && !span.end.is_empty()
        || !span.has_required_prefix && !span.required_prefix.is_empty()
        || !span.has_limit && span.limit != 0
    {
        return Err(ProtocolError::NonCanonicalSpan);
    }
    let keyspace = decode_keyspace(span.keyspace)?;
    let decoded = if span.has_required_prefix {
        let decoded = KeySpan::prefix_from(keyspace, span.required_prefix, span.start)
            .map_err(|error| ProtocolError::InvalidSpan(error.to_string()))?;
        if decoded.end() != span.has_end.then_some(span.end.as_slice()) {
            return Err(ProtocolError::NonCanonicalSpan);
        }
        decoded
    } else {
        KeySpan::range(keyspace, span.start, span.has_end.then_some(span.end))
            .map_err(|error| ProtocolError::InvalidSpan(error.to_string()))?
    };
    if span.has_limit {
        decoded
            .with_limit(usize::try_from(span.limit).map_err(|_| ProtocolError::LengthOverflow)?)
            .map_err(|error| ProtocolError::InvalidSpan(error.to_string()))
    } else {
        Ok(decoded)
    }
}

fn decode_keyspace(tag: u32) -> Result<Keyspace, ProtocolError> {
    match tag {
        0 => Ok(Keyspace::Meta),
        1 => Ok(Keyspace::Identity),
        2 => Ok(Keyspace::Current),
        3 => Ok(Keyspace::AdjOut),
        4 => Ok(Keyspace::AdjIn),
        5 => Ok(Keyspace::History),
        6 => Ok(Keyspace::TemporalIndex),
        7 => Ok(Keyspace::Txn),
        _ => Err(ProtocolError::UnknownKeyspace { tag }),
    }
}

fn encode_descriptor(descriptor: &AdapterDescriptorV1) -> wire::DescriptorResponse {
    let capabilities = descriptor.capabilities();
    wire::DescriptorResponse {
        spi_version: u32::from(descriptor.spi_version()),
        implementation: descriptor.implementation().to_owned(),
        implementation_version: descriptor.implementation_version().to_owned(),
        family: encode_family(descriptor.family()),
        capabilities: Some(wire::Capabilities {
            local_atomic_batch: capabilities.local_atomic_batch,
            idempotent_apply: capabilities.idempotent_apply,
            consistent_multi_get: capabilities.consistent_multi_get,
            ordered_scan: capabilities.ordered_scan,
            durable_applied_index: capabilities.durable_applied_index,
            durability: encode_durability(capabilities.durability),
            snapshot: encode_snapshot(capabilities.snapshot),
            logical_export: capabilities.logical_export,
            logical_restore: capabilities.logical_restore,
            predicate_pushdown: capabilities.predicate_pushdown,
            adjacency_pushdown: capabilities.adjacency_pushdown,
            change_feed: capabilities.change_feed,
        }),
    }
}

fn decode_descriptor(
    descriptor: wire::DescriptorResponse,
) -> Result<AdapterDescriptorV1, ProtocolError> {
    let capabilities = descriptor
        .capabilities
        .ok_or(ProtocolError::MissingCapabilities)?;
    let spi_version = u16::try_from(descriptor.spi_version)
        .map_err(|_| ProtocolError::InvalidSpiVersion(descriptor.spi_version))?;
    Ok(AdapterDescriptorV1::with_spi_version(
        spi_version,
        descriptor.implementation,
        descriptor.implementation_version,
        decode_family(descriptor.family)?,
        AdapterCapabilities {
            local_atomic_batch: capabilities.local_atomic_batch,
            idempotent_apply: capabilities.idempotent_apply,
            consistent_multi_get: capabilities.consistent_multi_get,
            ordered_scan: capabilities.ordered_scan,
            durable_applied_index: capabilities.durable_applied_index,
            durability: decode_durability(capabilities.durability)?,
            snapshot: decode_snapshot(capabilities.snapshot)?,
            logical_export: capabilities.logical_export,
            logical_restore: capabilities.logical_restore,
            predicate_pushdown: capabilities.predicate_pushdown,
            adjacency_pushdown: capabilities.adjacency_pushdown,
            change_feed: capabilities.change_feed,
        },
    ))
}

const fn encode_family(family: BackendFamily) -> u32 {
    match family {
        BackendFamily::KeyValue => 1,
        BackendFamily::Sql => 2,
        BackendFamily::PropertyGraph => 3,
        BackendFamily::Test => 4,
    }
}

fn decode_family(tag: u32) -> Result<BackendFamily, ProtocolError> {
    match tag {
        1 => Ok(BackendFamily::KeyValue),
        2 => Ok(BackendFamily::Sql),
        3 => Ok(BackendFamily::PropertyGraph),
        4 => Ok(BackendFamily::Test),
        _ => Err(ProtocolError::UnknownBackendFamily { tag }),
    }
}

const fn encode_durability(durability: Durability) -> u32 {
    match durability {
        Durability::Volatile => 1,
        Durability::BackendConfigured => 2,
        Durability::Synchronous => 3,
    }
}

fn decode_durability(tag: u32) -> Result<Durability, ProtocolError> {
    match tag {
        1 => Ok(Durability::Volatile),
        2 => Ok(Durability::BackendConfigured),
        3 => Ok(Durability::Synchronous),
        _ => Err(ProtocolError::UnknownDurability { tag }),
    }
}

const fn encode_snapshot(snapshot: SnapshotCapability) -> u32 {
    match snapshot {
        SnapshotCapability::None => 1,
        SnapshotCapability::PhysicalCheckpoint => 2,
        SnapshotCapability::LogicalExport => 3,
    }
}

fn decode_snapshot(tag: u32) -> Result<SnapshotCapability, ProtocolError> {
    match tag {
        1 => Ok(SnapshotCapability::None),
        2 => Ok(SnapshotCapability::PhysicalCheckpoint),
        3 => Ok(SnapshotCapability::LogicalExport),
        _ => Err(ProtocolError::UnknownSnapshotCapability { tag }),
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProtocolError {
    Io(String),
    InvalidMagic,
    UnsupportedWireVersion {
        version: u16,
    },
    UnknownFrameKind {
        tag: u8,
    },
    UnexpectedFrameKind {
        expected: FrameKind,
        actual: FrameKind,
    },
    UnknownFlags {
        flags: u8,
    },
    TruncatedFrame,
    PayloadTooLarge {
        max: usize,
        actual: usize,
    },
    FrameLengthMismatch {
        expected: usize,
        actual: usize,
    },
    ChecksumMismatch,
    LengthOverflow,
    PayloadEncode(String),
    PayloadDecode(String),
    NonCanonicalPayload,
    InvalidSpiVersion(u32),
    UnsupportedSpiVersion {
        expected: u16,
        actual: u16,
    },
    MissingBody,
    NonCanonicalBody,
    InvalidTxnIdLength,
    InvalidSessionIdLength {
        actual: usize,
    },
    InvalidSnapshotIdLength {
        actual: usize,
    },
    InvalidDigestLength {
        actual: usize,
    },
    UnknownFeatureBits {
        bits: u64,
    },
    InvalidWireVersion(u32),
    UnsupportedSnapshotFormat {
        expected: u16,
        actual: u32,
    },
    MissingSnapshotField(&'static str),
    InvalidSnapshot(String),
    InvalidChunkLimits(String),
    NonCanonicalPublicParameters,
    MissingLogicalKey,
    NonCanonicalDelete,
    UnknownMutationOperation {
        tag: u32,
    },
    UnknownKeyspace {
        tag: u32,
    },
    InvalidSpan(String),
    NonCanonicalSpan,
    MissingCapabilities,
    UnknownBackendFamily {
        tag: u32,
    },
    UnknownDurability {
        tag: u32,
    },
    UnknownSnapshotCapability {
        tag: u32,
    },
}

impl Display for ProtocolError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(message) => write!(formatter, "Sidecar I/O error: {message}"),
            Self::InvalidMagic => formatter.write_str("invalid Sidecar frame magic"),
            Self::UnsupportedWireVersion { version } => {
                write!(formatter, "unsupported Sidecar wire version {version}")
            }
            Self::UnknownFrameKind { tag } => write!(formatter, "unknown frame kind {tag}"),
            Self::UnexpectedFrameKind { expected, actual } => {
                write!(formatter, "expected {expected:?} frame, got {actual:?}")
            }
            Self::UnknownFlags { flags } => write!(formatter, "unknown frame flags {flags:#x}"),
            Self::TruncatedFrame => formatter.write_str("Sidecar frame is truncated"),
            Self::PayloadTooLarge { max, actual } => {
                write!(
                    formatter,
                    "Sidecar payload has {actual} bytes; maximum is {max}"
                )
            }
            Self::FrameLengthMismatch { expected, actual } => write!(
                formatter,
                "Sidecar frame has {actual} bytes; expected {expected}"
            ),
            Self::ChecksumMismatch => formatter.write_str("Sidecar frame checksum mismatch"),
            Self::LengthOverflow => formatter.write_str("Sidecar frame length overflow"),
            Self::PayloadEncode(message) => write!(formatter, "payload encode error: {message}"),
            Self::PayloadDecode(message) => write!(formatter, "payload decode error: {message}"),
            Self::NonCanonicalPayload => {
                formatter.write_str("Sidecar Protobuf payload is not canonically encoded")
            }
            Self::InvalidSpiVersion(version) => write!(formatter, "invalid SPI version {version}"),
            Self::UnsupportedSpiVersion { expected, actual } => write!(
                formatter,
                "Sidecar SPI version {actual} differs from required version {expected}"
            ),
            Self::MissingBody => formatter.write_str("Sidecar envelope has no body"),
            Self::NonCanonicalBody => formatter.write_str("Sidecar body is non-canonical"),
            Self::InvalidTxnIdLength => formatter.write_str("transaction ID must have 16 bytes"),
            Self::InvalidSessionIdLength { actual } => {
                write!(formatter, "session ID has {actual} bytes; expected 16")
            }
            Self::InvalidSnapshotIdLength { actual } => {
                write!(formatter, "snapshot ID has {actual} bytes; expected 16")
            }
            Self::InvalidDigestLength { actual } => {
                write!(formatter, "snapshot digest has {actual} bytes; expected 32")
            }
            Self::UnknownFeatureBits { bits } => {
                write!(formatter, "unknown Sidecar Feature bits {bits:#x}")
            }
            Self::InvalidWireVersion(version) => {
                write!(formatter, "invalid Sidecar Wire version {version}")
            }
            Self::UnsupportedSnapshotFormat { expected, actual } => write!(
                formatter,
                "logical snapshot format {actual} differs from supported version {expected}"
            ),
            Self::MissingSnapshotField(field) => {
                write!(formatter, "Sidecar snapshot field {field} is missing")
            }
            Self::InvalidSnapshot(message) => {
                write!(formatter, "invalid logical snapshot object: {message}")
            }
            Self::InvalidChunkLimits(message) => {
                write!(
                    formatter,
                    "invalid logical snapshot Chunk limits: {message}"
                )
            }
            Self::NonCanonicalPublicParameters => {
                formatter.write_str("public Adapter parameters are not strictly name-ordered")
            }
            Self::MissingLogicalKey => formatter.write_str("logical key is missing"),
            Self::NonCanonicalDelete => formatter.write_str("delete mutation carries a value"),
            Self::UnknownMutationOperation { tag } => {
                write!(formatter, "unknown mutation operation {tag}")
            }
            Self::UnknownKeyspace { tag } => write!(formatter, "unknown keyspace {tag}"),
            Self::InvalidSpan(message) => write!(formatter, "invalid key span: {message}"),
            Self::NonCanonicalSpan => formatter.write_str("key span is non-canonical"),
            Self::MissingCapabilities => formatter.write_str("Adapter capabilities are missing"),
            Self::UnknownBackendFamily { tag } => {
                write!(formatter, "unknown backend family {tag}")
            }
            Self::UnknownDurability { tag } => write!(formatter, "unknown durability {tag}"),
            Self::UnknownSnapshotCapability { tag } => {
                write!(formatter, "unknown snapshot capability {tag}")
            }
        }
    }
}

impl Error for ProtocolError {}

impl From<std::io::Error> for ProtocolError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error.to_string())
    }
}

mod wire {
    use prost::Message;

    #[derive(Clone, PartialEq, Message)]
    pub struct LogicalKey {
        #[prost(uint32, tag = "1")]
        pub keyspace: u32,
        #[prost(bytes = "vec", tag = "2")]
        pub key: Vec<u8>,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct Mutation {
        #[prost(uint32, tag = "1")]
        pub sequence: u32,
        #[prost(uint32, tag = "2")]
        pub operation: u32,
        #[prost(message, optional, tag = "3")]
        pub key: Option<LogicalKey>,
        #[prost(bytes = "vec", tag = "4")]
        pub value: Vec<u8>,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct ApplyRequest {
        #[prost(uint32, tag = "1")]
        pub shard_id: u32,
        #[prost(uint64, tag = "2")]
        pub log_index: u64,
        #[prost(bytes = "vec", tag = "3")]
        pub txn_id: Vec<u8>,
        #[prost(message, repeated, tag = "4")]
        pub mutations: Vec<Mutation>,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct MultiGetRequest {
        #[prost(message, repeated, tag = "1")]
        pub keys: Vec<LogicalKey>,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct ScanRequest {
        #[prost(uint32, tag = "1")]
        pub keyspace: u32,
        #[prost(bytes = "vec", tag = "2")]
        pub start: Vec<u8>,
        #[prost(bool, tag = "3")]
        pub has_end: bool,
        #[prost(bytes = "vec", tag = "4")]
        pub end: Vec<u8>,
        #[prost(bool, tag = "5")]
        pub has_required_prefix: bool,
        #[prost(bytes = "vec", tag = "6")]
        pub required_prefix: Vec<u8>,
        #[prost(bool, tag = "7")]
        pub has_limit: bool,
        #[prost(uint64, tag = "8")]
        pub limit: u64,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct HelloRequest {
        #[prost(uint64, tag = "1")]
        pub required_features: u64,
        #[prost(uint64, tag = "2")]
        pub optional_features: u64,
        #[prost(uint32, tag = "3")]
        pub max_payload_bytes: u32,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct SnapshotLimits {
        #[prost(uint32, tag = "1")]
        pub max_entries: u32,
        #[prost(uint32, tag = "2")]
        pub max_bytes: u32,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct SnapshotHeader {
        #[prost(uint32, tag = "1")]
        pub format_version: u32,
        #[prost(bytes = "vec", tag = "2")]
        pub snapshot_id: Vec<u8>,
        #[prost(uint64, tag = "3")]
        pub applied_log_index: u64,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct SnapshotChunk {
        #[prost(bytes = "vec", tag = "1")]
        pub snapshot_id: Vec<u8>,
        #[prost(uint64, tag = "2")]
        pub ordinal: u64,
        #[prost(message, repeated, tag = "3")]
        pub entries: Vec<KeyValue>,
        #[prost(bytes = "vec", tag = "4")]
        pub digest: Vec<u8>,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct SnapshotManifest {
        #[prost(message, optional, tag = "1")]
        pub header: Option<SnapshotHeader>,
        #[prost(uint64, tag = "2")]
        pub total_chunks: u64,
        #[prost(uint64, tag = "3")]
        pub total_entries: u64,
        #[prost(bytes = "vec", tag = "4")]
        pub content_digest: Vec<u8>,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct PublicParameter {
        #[prost(string, tag = "1")]
        pub name: String,
        #[prost(string, tag = "2")]
        pub value: String,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct PublicAdapterOpenRequest {
        #[prost(string, tag = "1")]
        pub instance_id: String,
        #[prost(message, repeated, tag = "2")]
        pub parameters: Vec<PublicParameter>,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct BeginExportRequest {
        #[prost(message, optional, tag = "1")]
        pub limits: Option<SnapshotLimits>,
        #[prost(uint64, optional, tag = "2")]
        pub expected_applied_log_index: Option<u64>,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct ExportNextRequest {
        #[prost(bytes = "vec", tag = "1")]
        pub session_id: Vec<u8>,
        #[prost(uint64, tag = "2")]
        pub expected_ordinal: u64,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct BeginRestoreRequest {
        #[prost(message, optional, tag = "1")]
        pub header: Option<SnapshotHeader>,
        #[prost(message, optional, tag = "2")]
        pub target: Option<PublicAdapterOpenRequest>,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct RestoreChunkRequest {
        #[prost(bytes = "vec", tag = "1")]
        pub session_id: Vec<u8>,
        #[prost(message, optional, tag = "2")]
        pub chunk: Option<SnapshotChunk>,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct FinishRestoreRequest {
        #[prost(bytes = "vec", tag = "1")]
        pub session_id: Vec<u8>,
        #[prost(message, optional, tag = "2")]
        pub manifest: Option<SnapshotManifest>,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct AbortSessionRequest {
        #[prost(bytes = "vec", tag = "1")]
        pub session_id: Vec<u8>,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct RequestEnvelope {
        #[prost(uint32, tag = "1")]
        pub spi_version: u32,
        #[prost(
            oneof = "request_envelope::Body",
            tags = "2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14"
        )]
        pub body: Option<request_envelope::Body>,
    }

    pub mod request_envelope {
        use super::{
            AbortSessionRequest, ApplyRequest, BeginExportRequest, BeginRestoreRequest,
            ExportNextRequest, FinishRestoreRequest, HelloRequest, MultiGetRequest,
            RestoreChunkRequest, ScanRequest,
        };
        use prost::Oneof;

        #[derive(Clone, PartialEq, Oneof)]
        pub enum Body {
            #[prost(bool, tag = "2")]
            Describe(bool),
            #[prost(message, tag = "3")]
            Apply(ApplyRequest),
            #[prost(message, tag = "4")]
            MultiGet(MultiGetRequest),
            #[prost(message, tag = "5")]
            Scan(ScanRequest),
            #[prost(bool, tag = "6")]
            AppliedLogIndex(bool),
            #[prost(bool, tag = "7")]
            Health(bool),
            #[prost(message, tag = "8")]
            Hello(HelloRequest),
            #[prost(message, tag = "9")]
            BeginExport(BeginExportRequest),
            #[prost(message, tag = "10")]
            ExportNext(ExportNextRequest),
            #[prost(message, tag = "11")]
            BeginRestore(BeginRestoreRequest),
            #[prost(message, tag = "12")]
            RestoreChunk(RestoreChunkRequest),
            #[prost(message, tag = "13")]
            FinishRestore(FinishRestoreRequest),
            #[prost(message, tag = "14")]
            AbortSession(AbortSessionRequest),
        }
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct Capabilities {
        #[prost(bool, tag = "1")]
        pub local_atomic_batch: bool,
        #[prost(bool, tag = "2")]
        pub idempotent_apply: bool,
        #[prost(bool, tag = "3")]
        pub consistent_multi_get: bool,
        #[prost(bool, tag = "4")]
        pub ordered_scan: bool,
        #[prost(bool, tag = "5")]
        pub durable_applied_index: bool,
        #[prost(uint32, tag = "6")]
        pub durability: u32,
        #[prost(uint32, tag = "7")]
        pub snapshot: u32,
        #[prost(bool, tag = "8")]
        pub predicate_pushdown: bool,
        #[prost(bool, tag = "9")]
        pub adjacency_pushdown: bool,
        #[prost(bool, tag = "10")]
        pub change_feed: bool,
        #[prost(bool, tag = "11")]
        pub logical_export: bool,
        #[prost(bool, tag = "12")]
        pub logical_restore: bool,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct DescriptorResponse {
        #[prost(uint32, tag = "1")]
        pub spi_version: u32,
        #[prost(string, tag = "2")]
        pub implementation: String,
        #[prost(string, tag = "3")]
        pub implementation_version: String,
        #[prost(uint32, tag = "4")]
        pub family: u32,
        #[prost(message, optional, tag = "5")]
        pub capabilities: Option<Capabilities>,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct ApplyResponse {
        #[prost(uint64, tag = "1")]
        pub applied_log_index: u64,
        #[prost(bool, tag = "2")]
        pub duplicate: bool,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct OptionalBytes {
        #[prost(bool, tag = "1")]
        pub present: bool,
        #[prost(bytes = "vec", tag = "2")]
        pub value: Vec<u8>,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct MultiGetResponse {
        #[prost(message, repeated, tag = "1")]
        pub values: Vec<OptionalBytes>,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct KeyValue {
        #[prost(message, optional, tag = "1")]
        pub key: Option<LogicalKey>,
        #[prost(bytes = "vec", tag = "2")]
        pub value: Vec<u8>,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct ScanResponse {
        #[prost(message, repeated, tag = "1")]
        pub values: Vec<KeyValue>,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct HealthResponse {
        #[prost(bool, tag = "1")]
        pub ready: bool,
        #[prost(string, tag = "2")]
        pub detail: String,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct ErrorResponse {
        #[prost(uint32, tag = "1")]
        pub code: u32,
        #[prost(string, tag = "2")]
        pub message: String,
        #[prost(bool, tag = "3")]
        pub retryable: bool,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct HelloResponse {
        #[prost(uint32, tag = "1")]
        pub wire_version: u32,
        #[prost(uint64, tag = "2")]
        pub negotiated_features: u64,
        #[prost(uint32, tag = "3")]
        pub max_payload_bytes: u32,
        #[prost(uint32, tag = "4")]
        pub max_chunk_bytes: u32,
        #[prost(uint32, tag = "5")]
        pub max_chunk_entries: u32,
        #[prost(uint32, tag = "6")]
        pub snapshot_format_version: u32,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct ExportStartedResponse {
        #[prost(bytes = "vec", tag = "1")]
        pub session_id: Vec<u8>,
        #[prost(message, optional, tag = "2")]
        pub header: Option<SnapshotHeader>,
        #[prost(message, optional, tag = "3")]
        pub limits: Option<SnapshotLimits>,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct ExportChunkResponse {
        #[prost(bytes = "vec", tag = "1")]
        pub session_id: Vec<u8>,
        #[prost(message, optional, tag = "2")]
        pub chunk: Option<SnapshotChunk>,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct ExportCompleteResponse {
        #[prost(bytes = "vec", tag = "1")]
        pub session_id: Vec<u8>,
        #[prost(message, optional, tag = "2")]
        pub manifest: Option<SnapshotManifest>,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct RestoreStartedResponse {
        #[prost(bytes = "vec", tag = "1")]
        pub session_id: Vec<u8>,
        #[prost(message, optional, tag = "2")]
        pub prospective_descriptor: Option<DescriptorResponse>,
        #[prost(uint32, tag = "3")]
        pub max_chunk_bytes: u32,
        #[prost(uint32, tag = "4")]
        pub max_chunk_entries: u32,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct RestoreChunkAcceptedResponse {
        #[prost(bytes = "vec", tag = "1")]
        pub session_id: Vec<u8>,
        #[prost(uint64, tag = "2")]
        pub ordinal: u64,
        #[prost(bytes = "vec", tag = "3")]
        pub digest: Vec<u8>,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct RestoreCompleteResponse {
        #[prost(bytes = "vec", tag = "1")]
        pub session_id: Vec<u8>,
        #[prost(message, optional, tag = "2")]
        pub final_descriptor: Option<DescriptorResponse>,
        #[prost(uint64, tag = "3")]
        pub applied_log_index: u64,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct SessionAbortedResponse {
        #[prost(bytes = "vec", tag = "1")]
        pub session_id: Vec<u8>,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct ResponseEnvelope {
        #[prost(uint32, tag = "1")]
        pub spi_version: u32,
        #[prost(
            oneof = "response_envelope::Body",
            tags = "2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16"
        )]
        pub body: Option<response_envelope::Body>,
    }

    pub mod response_envelope {
        use super::{
            ApplyResponse, DescriptorResponse, ErrorResponse, ExportChunkResponse,
            ExportCompleteResponse, ExportStartedResponse, HealthResponse, HelloResponse,
            MultiGetResponse, RestoreChunkAcceptedResponse, RestoreCompleteResponse,
            RestoreStartedResponse, ScanResponse, SessionAbortedResponse,
        };
        use prost::Oneof;

        #[derive(Clone, PartialEq, Oneof)]
        pub enum Body {
            #[prost(message, tag = "2")]
            Descriptor(DescriptorResponse),
            #[prost(message, tag = "3")]
            Apply(ApplyResponse),
            #[prost(message, tag = "4")]
            MultiGet(MultiGetResponse),
            #[prost(message, tag = "5")]
            Scan(ScanResponse),
            #[prost(uint64, tag = "6")]
            AppliedLogIndex(u64),
            #[prost(message, tag = "7")]
            Health(HealthResponse),
            #[prost(message, tag = "8")]
            Error(ErrorResponse),
            #[prost(message, tag = "9")]
            Hello(HelloResponse),
            #[prost(message, tag = "10")]
            ExportStarted(ExportStartedResponse),
            #[prost(message, tag = "11")]
            ExportChunk(ExportChunkResponse),
            #[prost(message, tag = "12")]
            ExportComplete(ExportCompleteResponse),
            #[prost(message, tag = "13")]
            RestoreStarted(RestoreStartedResponse),
            #[prost(message, tag = "14")]
            RestoreChunkAccepted(RestoreChunkAcceptedResponse),
            #[prost(message, tag = "15")]
            RestoreComplete(RestoreCompleteResponse),
            #[prost(message, tag = "16")]
            SessionAborted(SessionAbortedResponse),
        }
    }
}
