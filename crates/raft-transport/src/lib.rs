#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::time::Duration;

use prost::Message as ProstMessage;
use raft::eraftpb::Message;

const MAGIC: [u8; 4] = *b"DTRM";
const VERSION: u16 = 1;
const HEADER_BYTES: usize = 14;
const CHECKSUM_BYTES: usize = 4;
pub const MAX_RAFT_MESSAGE_BYTES: usize = 16 * 1024 * 1024;
const MAX_FRAME_BYTES: usize = HEADER_BYTES + MAX_RAFT_MESSAGE_BYTES + CHECKSUM_BYTES;

pub fn encode_message_frame(
    shard_id: u32,
    message: &Message,
) -> Result<Vec<u8>, RaftTransportError> {
    if message.from == 0 || message.to == 0 {
        return Err(RaftTransportError::InvalidRoute);
    }
    let payload = message.encode_to_vec();
    if payload.len() > MAX_RAFT_MESSAGE_BYTES {
        return Err(RaftTransportError::FrameTooLarge);
    }
    let payload_length =
        u32::try_from(payload.len()).map_err(|_| RaftTransportError::FrameTooLarge)?;
    let mut frame = Vec::with_capacity(HEADER_BYTES + payload.len() + CHECKSUM_BYTES);
    frame.extend_from_slice(&MAGIC);
    frame.extend_from_slice(&VERSION.to_be_bytes());
    frame.extend_from_slice(&shard_id.to_be_bytes());
    frame.extend_from_slice(&payload_length.to_be_bytes());
    frame.extend_from_slice(&payload);
    let checksum = crc32fast::hash(&frame);
    frame.extend_from_slice(&checksum.to_be_bytes());
    Ok(frame)
}

pub fn decode_message_frame(
    expected_shard_id: u32,
    expected_node_id: u64,
    frame: &[u8],
) -> Result<Message, RaftTransportError> {
    if frame.len() < HEADER_BYTES + CHECKSUM_BYTES || frame.len() > MAX_FRAME_BYTES {
        return Err(RaftTransportError::InvalidFrameLength);
    }
    if frame[..4] != MAGIC {
        return Err(RaftTransportError::InvalidMagic);
    }
    let version = u16::from_be_bytes(frame[4..6].try_into().expect("fixed version slice"));
    if version != VERSION {
        return Err(RaftTransportError::UnsupportedVersion { version });
    }
    let payload_length = usize::try_from(u32::from_be_bytes(
        frame[10..14]
            .try_into()
            .expect("fixed payload length slice"),
    ))
    .map_err(|_| RaftTransportError::FrameTooLarge)?;
    if payload_length > MAX_RAFT_MESSAGE_BYTES
        || frame.len() != HEADER_BYTES + payload_length + CHECKSUM_BYTES
    {
        return Err(RaftTransportError::InvalidFrameLength);
    }
    let checksum_offset = frame.len() - CHECKSUM_BYTES;
    let stored_checksum = u32::from_be_bytes(
        frame[checksum_offset..]
            .try_into()
            .expect("fixed checksum slice"),
    );
    if crc32fast::hash(&frame[..checksum_offset]) != stored_checksum {
        return Err(RaftTransportError::ChecksumMismatch);
    }
    let shard_id = u32::from_be_bytes(frame[6..10].try_into().expect("fixed shard slice"));
    if shard_id != expected_shard_id {
        return Err(RaftTransportError::ShardMismatch {
            expected: expected_shard_id,
            actual: shard_id,
        });
    }
    let message = Message::decode(&frame[HEADER_BYTES..checksum_offset])
        .map_err(|_| RaftTransportError::InvalidProtobuf)?;
    if message.to != expected_node_id {
        return Err(RaftTransportError::RouteMismatch {
            expected: expected_node_id,
            actual: message.to,
        });
    }
    if message.from == 0 {
        return Err(RaftTransportError::InvalidRoute);
    }
    Ok(message)
}

pub struct TcpRaftTransport {
    shard_id: u32,
    node_id: u64,
    listener: TcpListener,
    peers: BTreeMap<u64, SocketAddr>,
    connect_timeout: Duration,
    io_timeout: Duration,
}

impl TcpRaftTransport {
    pub fn bind(
        shard_id: u32,
        node_id: u64,
        listen_address: SocketAddr,
        peers: BTreeMap<u64, SocketAddr>,
    ) -> Result<Self, RaftTransportError> {
        if node_id == 0 {
            return Err(RaftTransportError::InvalidRoute);
        }
        let listener = TcpListener::bind(listen_address)?;
        listener.set_nonblocking(true)?;
        Ok(Self {
            shard_id,
            node_id,
            listener,
            peers,
            connect_timeout: Duration::from_millis(250),
            io_timeout: Duration::from_millis(500),
        })
    }

    pub fn set_peer(&mut self, node_id: u64, address: SocketAddr) {
        self.peers.insert(node_id, address);
    }

    pub fn set_timeouts(&mut self, connect_timeout: Duration, io_timeout: Duration) {
        self.connect_timeout = connect_timeout;
        self.io_timeout = io_timeout;
    }

    pub fn local_addr(&self) -> Result<SocketAddr, RaftTransportError> {
        Ok(self.listener.local_addr()?)
    }

    pub fn send(&self, message: &Message) -> Result<(), RaftTransportError> {
        if message.from != self.node_id || message.to == 0 {
            return Err(RaftTransportError::InvalidRoute);
        }
        let peer = self
            .peers
            .get(&message.to)
            .copied()
            .ok_or(RaftTransportError::UnknownPeer {
                node_id: message.to,
            })?;
        let frame = encode_message_frame(self.shard_id, message)?;
        let frame_length =
            u32::try_from(frame.len()).map_err(|_| RaftTransportError::FrameTooLarge)?;
        let mut stream = TcpStream::connect_timeout(&peer, self.connect_timeout)?;
        stream.set_write_timeout(Some(self.io_timeout))?;
        stream.write_all(&frame_length.to_be_bytes())?;
        stream.write_all(&frame)?;
        stream.flush()?;
        Ok(())
    }

    pub fn receive_available(&self) -> Result<Vec<Message>, RaftTransportError> {
        let mut messages = Vec::new();
        loop {
            let (mut stream, _) = match self.listener.accept() {
                Ok(connection) => connection,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                Err(error) => return Err(error.into()),
            };
            stream.set_nonblocking(false)?;
            stream.set_read_timeout(Some(self.io_timeout))?;
            let mut length_bytes = [0_u8; 4];
            stream.read_exact(&mut length_bytes)?;
            let length = usize::try_from(u32::from_be_bytes(length_bytes))
                .map_err(|_| RaftTransportError::FrameTooLarge)?;
            if !(HEADER_BYTES + CHECKSUM_BYTES..=MAX_FRAME_BYTES).contains(&length) {
                return Err(RaftTransportError::InvalidFrameLength);
            }
            let mut frame = vec![0_u8; length];
            stream.read_exact(&mut frame)?;
            let message = decode_message_frame(self.shard_id, self.node_id, &frame)?;
            if !self.peers.contains_key(&message.from) {
                return Err(RaftTransportError::UnknownPeer {
                    node_id: message.from,
                });
            }
            messages.push(message);
        }
        Ok(messages)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RaftTransportError {
    Io(String),
    InvalidMagic,
    UnsupportedVersion { version: u16 },
    InvalidFrameLength,
    FrameTooLarge,
    ChecksumMismatch,
    InvalidProtobuf,
    InvalidRoute,
    RouteMismatch { expected: u64, actual: u64 },
    ShardMismatch { expected: u32, actual: u32 },
    UnknownPeer { node_id: u64 },
}

impl Display for RaftTransportError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(message) => write!(formatter, "Raft transport I/O error: {message}"),
            Self::InvalidMagic => formatter.write_str("invalid Raft message frame magic"),
            Self::UnsupportedVersion { version } => {
                write!(
                    formatter,
                    "unsupported Raft message frame version {version}"
                )
            }
            Self::InvalidFrameLength => formatter.write_str("invalid Raft message frame length"),
            Self::FrameTooLarge => formatter.write_str("Raft message frame exceeds its size limit"),
            Self::ChecksumMismatch => formatter.write_str("Raft message frame checksum mismatch"),
            Self::InvalidProtobuf => formatter.write_str("invalid Raft protobuf payload"),
            Self::InvalidRoute => formatter.write_str("invalid Raft message route"),
            Self::RouteMismatch { expected, actual } => {
                write!(
                    formatter,
                    "Raft message targets node {actual}; expected {expected}"
                )
            }
            Self::ShardMismatch { expected, actual } => {
                write!(
                    formatter,
                    "Raft message targets shard {actual}; expected {expected}"
                )
            }
            Self::UnknownPeer { node_id } => write!(formatter, "unknown Raft peer {node_id}"),
        }
    }
}

impl Error for RaftTransportError {}

impl From<io::Error> for RaftTransportError {
    fn from(error: io::Error) -> Self {
        Self::Io(error.to_string())
    }
}
