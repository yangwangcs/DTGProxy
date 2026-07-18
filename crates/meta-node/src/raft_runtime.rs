use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cluster_protocol::proto::meta_raft_service_client::MetaRaftServiceClient;
use cluster_protocol::proto::meta_raft_service_server::MetaRaftService;
use cluster_protocol::proto::{MetaRaftStepRequest, MetaRaftStepResponse, RequestContext};
use cluster_protocol::{CLUSTER_PROTOCOL_VERSION, CommonRequestContext, MAX_COMMAND_BYTES};
use prost::Message as ProstMessage;
use raft::eraftpb::Message;
use tokio::sync::{Mutex, Notify, watch};
use tokio::task::JoinHandle;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};
use tonic::{Request, Response, Status};

use crate::{MetaRaftError, MetaRaftReplica, MetaTlsFiles, MetaTransportSecurity};

const TICK_INTERVAL: Duration = Duration::from_millis(50);
const DELIVERY_TIMEOUT: Duration = Duration::from_millis(500);
const RPC_ENVELOPE_ALLOWANCE: usize = 64 * 1024;

#[derive(Clone)]
enum ClientSecurity {
    Plaintext,
    MutualTls {
        ca: Vec<u8>,
        certificate: Vec<u8>,
        private_key: Vec<u8>,
    },
}

impl ClientSecurity {
    fn load(security: &MetaTransportSecurity) -> Result<Self, MetaRaftRuntimeError> {
        match security {
            MetaTransportSecurity::LoopbackPlaintext => Ok(Self::Plaintext),
            MetaTransportSecurity::MutualTls(files) => Ok(Self::MutualTls {
                ca: read_tls(files, MetaTlsFiles::ca_certificate)?,
                certificate: read_tls(files, MetaTlsFiles::node_certificate)?,
                private_key: read_tls(files, MetaTlsFiles::private_key)?,
            }),
        }
    }
}

fn read_tls(
    files: &MetaTlsFiles,
    path: fn(&MetaTlsFiles) -> &std::path::Path,
) -> Result<Vec<u8>, MetaRaftRuntimeError> {
    std::fs::read(path(files)).map_err(|error| MetaRaftRuntimeError::Io(error.to_string()))
}

pub struct MetaRaftRuntime {
    notify: Arc<Notify>,
    shutdown: watch::Sender<bool>,
    join: JoinHandle<Result<(), MetaRaftRuntimeError>>,
}

impl MetaRaftRuntime {
    pub fn spawn(
        cluster_id: [u8; 16],
        node_id: u64,
        replica: Arc<Mutex<MetaRaftReplica>>,
        peers: BTreeMap<u64, SocketAddr>,
        security: &MetaTransportSecurity,
    ) -> Result<Self, MetaRaftRuntimeError> {
        if cluster_id == [0; 16]
            || node_id == 0
            || peers.keys().any(|peer| *peer == 0 || *peer == node_id)
        {
            return Err(MetaRaftRuntimeError::InvalidConfiguration);
        }
        let security = ClientSecurity::load(security)?;
        let notify = Arc::new(Notify::new());
        let (shutdown, receiver) = watch::channel(false);
        let join = tokio::spawn(run_driver(
            cluster_id,
            node_id,
            replica,
            peers,
            security,
            Arc::clone(&notify),
            receiver,
        ));
        Ok(Self {
            notify,
            shutdown,
            join,
        })
    }

    #[must_use]
    pub fn notify(&self) -> Arc<Notify> {
        Arc::clone(&self.notify)
    }

    pub async fn shutdown(self) -> Result<(), MetaRaftRuntimeError> {
        let _ = self.shutdown.send(true);
        self.join
            .await
            .map_err(|error| MetaRaftRuntimeError::Join(error.to_string()))?
    }
}

#[derive(Clone)]
pub struct MetaRaftGrpcService {
    cluster_id: [u8; 16],
    node_id: u64,
    replica: Arc<Mutex<MetaRaftReplica>>,
    notify: Arc<Notify>,
}

impl MetaRaftGrpcService {
    #[must_use]
    pub const fn new(
        cluster_id: [u8; 16],
        node_id: u64,
        replica: Arc<Mutex<MetaRaftReplica>>,
        notify: Arc<Notify>,
    ) -> Self {
        Self {
            cluster_id,
            node_id,
            replica,
            notify,
        }
    }
}

#[tonic::async_trait]
impl MetaRaftService for MetaRaftGrpcService {
    async fn step(
        &self,
        request: Request<MetaRaftStepRequest>,
    ) -> Result<Response<MetaRaftStepResponse>, Status> {
        let request = request.into_inner();
        let context: CommonRequestContext = request
            .context
            .ok_or_else(|| Status::invalid_argument("missing request context"))?
            .try_into()
            .map_err(|error: cluster_protocol::ProtocolError| {
                Status::invalid_argument(error.to_string())
            })?;
        if context.cluster_id() != &self.cluster_id {
            return Err(Status::permission_denied("cluster identity mismatch"));
        }
        context
            .ensure_active_at(unix_time_ms()?)
            .map_err(|error| Status::deadline_exceeded(error.to_string()))?;
        if request.from_node_id == 0
            || request.to_node_id != self.node_id
            || request.message.is_empty()
            || request.message.len() > MAX_COMMAND_BYTES
        {
            return Err(Status::invalid_argument(
                "invalid Meta Raft route or payload",
            ));
        }
        let message = Message::decode(request.message.as_slice())
            .map_err(|_| Status::invalid_argument("invalid Meta Raft protobuf"))?;
        if message.from != request.from_node_id || message.to != request.to_node_id {
            return Err(Status::invalid_argument(
                "Meta Raft envelope identity mismatch",
            ));
        }
        let mut replica = self.replica.lock().await;
        replica
            .step(message)
            .map_err(|error| Status::internal(error.to_string()))?;
        let current_term = replica.current_term();
        drop(replica);
        self.notify.notify_one();
        Ok(Response::new(MetaRaftStepResponse { current_term }))
    }
}

async fn run_driver(
    cluster_id: [u8; 16],
    node_id: u64,
    replica: Arc<Mutex<MetaRaftReplica>>,
    peers: BTreeMap<u64, SocketAddr>,
    security: ClientSecurity,
    notify: Arc<Notify>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), MetaRaftRuntimeError> {
    let mut ticker = tokio::time::interval(TICK_INTERVAL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        let ticked = tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return Ok(());
                }
                false
            }
            _ = ticker.tick() => true,
            () = notify.notified() => false,
        };
        let messages = {
            let mut replica = replica.lock().await;
            if ticked {
                replica.tick();
            }
            replica.drain_ready()?
        };
        for message in messages {
            let Some(address) = peers.get(&message.to).copied() else {
                return Err(MetaRaftRuntimeError::UnknownPeer {
                    node_id: message.to,
                });
            };
            let security = security.clone();
            tokio::spawn(async move {
                let _ = deliver(cluster_id, node_id, address, security, message).await;
            });
        }
    }
}

async fn deliver(
    cluster_id: [u8; 16],
    node_id: u64,
    address: SocketAddr,
    security: ClientSecurity,
    message: Message,
) -> Result<(), MetaRaftRuntimeError> {
    let mut client = connect(address, security).await?;
    let payload = message.encode_to_vec();
    if payload.len() > MAX_COMMAND_BYTES {
        return Err(MetaRaftRuntimeError::MessageTooLarge);
    }
    let now = unix_time_ms().map_err(|status| MetaRaftRuntimeError::Clock(status.to_string()))?;
    let request_id = raft_request_id(&message, &payload);
    let request = MetaRaftStepRequest {
        context: Some(RequestContext {
            protocol_version: CLUSTER_PROTOCOL_VERSION,
            cluster_id: cluster_id.to_vec(),
            request_id: request_id.to_vec(),
            deadline_unix_ms: now.saturating_add(
                u64::try_from(DELIVERY_TIMEOUT.as_millis()).expect("delivery timeout fits u64"),
            ),
        }),
        from_node_id: node_id,
        to_node_id: message.to,
        message: payload,
    };
    tokio::time::timeout(DELIVERY_TIMEOUT, client.step(request))
        .await
        .map_err(|_| MetaRaftRuntimeError::DeliveryTimeout)?
        .map_err(|error| MetaRaftRuntimeError::Transport(error.to_string()))?;
    Ok(())
}

async fn connect(
    address: SocketAddr,
    security: ClientSecurity,
) -> Result<MetaRaftServiceClient<Channel>, MetaRaftRuntimeError> {
    let scheme = match security {
        ClientSecurity::Plaintext => "http",
        ClientSecurity::MutualTls { .. } => "https",
    };
    let mut endpoint = Endpoint::from_shared(format!("{scheme}://{address}"))
        .map_err(|error| MetaRaftRuntimeError::Transport(error.to_string()))?
        .connect_timeout(DELIVERY_TIMEOUT)
        .timeout(DELIVERY_TIMEOUT);
    if let ClientSecurity::MutualTls {
        ca,
        certificate,
        private_key,
    } = security
    {
        endpoint = endpoint
            .tls_config(
                ClientTlsConfig::new()
                    .ca_certificate(Certificate::from_pem(ca))
                    .identity(Identity::from_pem(certificate, private_key))
                    .domain_name(address.ip().to_string()),
            )
            .map_err(|error| MetaRaftRuntimeError::Transport(error.to_string()))?;
    }
    let channel = endpoint
        .connect()
        .await
        .map_err(|error| MetaRaftRuntimeError::Transport(error.to_string()))?;
    Ok(MetaRaftServiceClient::new(channel)
        .max_decoding_message_size(MAX_COMMAND_BYTES + RPC_ENVELOPE_ALLOWANCE)
        .max_encoding_message_size(MAX_COMMAND_BYTES + RPC_ENVELOPE_ALLOWANCE))
}

fn raft_request_id(message: &Message, payload: &[u8]) -> [u8; 16] {
    let mut request_id = [0_u8; 16];
    request_id[..8].copy_from_slice(&message.term.to_be_bytes());
    let identity = message.index
        ^ message.commit
        ^ message.from.rotate_left(7)
        ^ message.to.rotate_left(13)
        ^ u64::from(crc32fast::hash(payload));
    request_id[8..].copy_from_slice(&identity.to_be_bytes());
    if request_id == [0; 16] {
        request_id[15] = 1;
    }
    request_id
}

fn unix_time_ms() -> Result<u64, Status> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| Status::internal("system clock is before Unix epoch"))?
        .as_millis();
    u64::try_from(millis).map_err(|_| Status::internal("system clock overflow"))
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MetaRaftRuntimeError {
    InvalidConfiguration,
    UnknownPeer { node_id: u64 },
    MessageTooLarge,
    DeliveryTimeout,
    Io(String),
    Clock(String),
    Transport(String),
    Join(String),
    Raft(String),
}

impl Display for MetaRaftRuntimeError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfiguration => formatter.write_str("invalid Meta Raft runtime config"),
            Self::UnknownPeer { node_id } => write!(formatter, "unknown Meta peer {node_id}"),
            Self::MessageTooLarge => formatter.write_str("Meta Raft message exceeds its bound"),
            Self::DeliveryTimeout => formatter.write_str("Meta Raft delivery timed out"),
            Self::Io(message) => write!(formatter, "Meta Raft I/O error: {message}"),
            Self::Clock(message) => write!(formatter, "Meta Raft clock error: {message}"),
            Self::Transport(message) => write!(formatter, "Meta Raft transport error: {message}"),
            Self::Join(message) => write!(formatter, "Meta Raft task join error: {message}"),
            Self::Raft(message) => write!(formatter, "Meta Raft state error: {message}"),
        }
    }
}

impl Error for MetaRaftRuntimeError {}

impl From<MetaRaftError> for MetaRaftRuntimeError {
    fn from(error: MetaRaftError) -> Self {
        Self::Raft(error.to_string())
    }
}
