#![forbid(unsafe_code)]

use std::error::Error;
use std::fmt::{self, Display, Formatter};

use prost::Message as ProstMessage;
use raft::eraftpb::Message;

const MAGIC: [u8; 4] = *b"DTRM";
const ROUTED_VERSION: u16 = 2;
const ROUTED_HEADER_BYTES: usize = 46;
const CHECKSUM_BYTES: usize = 4;
pub const MAX_RAFT_MESSAGE_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_ROUTED_FRAME_BYTES: usize =
    ROUTED_HEADER_BYTES + MAX_RAFT_MESSAGE_BYTES + CHECKSUM_BYTES;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RaftRoute {
    cluster_id: [u8; 16],
    graph_id: u64,
    shard_id: u32,
    placement_epoch: u64,
}

impl RaftRoute {
    pub fn new(
        cluster_id: [u8; 16],
        graph_id: u64,
        shard_id: u32,
        placement_epoch: u64,
    ) -> Result<Self, RaftTransportError> {
        if cluster_id == [0; 16] || graph_id == 0 || shard_id == 0 || placement_epoch == 0 {
            return Err(RaftTransportError::InvalidRoute);
        }
        Ok(Self {
            cluster_id,
            graph_id,
            shard_id,
            placement_epoch,
        })
    }

    #[must_use]
    pub const fn cluster_id(&self) -> &[u8; 16] {
        &self.cluster_id
    }

    #[must_use]
    pub const fn graph_id(&self) -> u64 {
        self.graph_id
    }

    #[must_use]
    pub const fn shard_id(&self) -> u32 {
        self.shard_id
    }

    #[must_use]
    pub const fn placement_epoch(&self) -> u64 {
        self.placement_epoch
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct RoutedRaftMessage {
    route: RaftRoute,
    message: Message,
}

impl RoutedRaftMessage {
    pub fn new(route: RaftRoute, message: Message) -> Result<Self, RaftTransportError> {
        if message.from == 0 || message.to == 0 {
            return Err(RaftTransportError::InvalidRoute);
        }
        Ok(Self { route, message })
    }

    #[must_use]
    pub const fn route(&self) -> &RaftRoute {
        &self.route
    }

    #[must_use]
    pub const fn message(&self) -> &Message {
        &self.message
    }

    #[must_use]
    pub fn into_message(self) -> Message {
        self.message
    }
}

pub fn encode_routed_message_frame(
    routed: &RoutedRaftMessage,
) -> Result<Vec<u8>, RaftTransportError> {
    if routed.message.from == 0 || routed.message.to == 0 {
        return Err(RaftTransportError::InvalidRoute);
    }
    let payload = routed.message.encode_to_vec();
    if payload.len() > MAX_RAFT_MESSAGE_BYTES {
        return Err(RaftTransportError::FrameTooLarge);
    }
    let payload_length =
        u32::try_from(payload.len()).map_err(|_| RaftTransportError::FrameTooLarge)?;
    let mut frame = Vec::with_capacity(ROUTED_HEADER_BYTES + payload.len() + CHECKSUM_BYTES);
    frame.extend_from_slice(&MAGIC);
    frame.extend_from_slice(&ROUTED_VERSION.to_be_bytes());
    frame.extend_from_slice(&routed.route.cluster_id);
    frame.extend_from_slice(&routed.route.graph_id.to_be_bytes());
    frame.extend_from_slice(&routed.route.shard_id.to_be_bytes());
    frame.extend_from_slice(&routed.route.placement_epoch.to_be_bytes());
    frame.extend_from_slice(&payload_length.to_be_bytes());
    frame.extend_from_slice(&payload);
    let checksum = crc32fast::hash(&frame);
    frame.extend_from_slice(&checksum.to_be_bytes());
    Ok(frame)
}

pub fn decode_routed_message_frame(
    expected_cluster_id: [u8; 16],
    expected_node_id: u64,
    frame: &[u8],
) -> Result<RoutedRaftMessage, RaftTransportError> {
    if expected_cluster_id == [0; 16] || expected_node_id == 0 {
        return Err(RaftTransportError::InvalidRoute);
    }
    if frame.len() < ROUTED_HEADER_BYTES + CHECKSUM_BYTES || frame.len() > MAX_ROUTED_FRAME_BYTES {
        return Err(RaftTransportError::InvalidFrameLength);
    }
    if frame[..4] != MAGIC {
        return Err(RaftTransportError::InvalidMagic);
    }
    let version = u16::from_be_bytes(frame[4..6].try_into().expect("fixed version slice"));
    if version != ROUTED_VERSION {
        return Err(RaftTransportError::UnsupportedVersion { version });
    }
    let payload_length = u32::from_be_bytes(
        frame[42..46]
            .try_into()
            .expect("fixed payload length slice"),
    ) as usize;
    if payload_length > MAX_RAFT_MESSAGE_BYTES
        || frame.len() != ROUTED_HEADER_BYTES + payload_length + CHECKSUM_BYTES
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
    let cluster_id = frame[6..22].try_into().expect("fixed cluster ID slice");
    if cluster_id != expected_cluster_id {
        return Err(RaftTransportError::ClusterMismatch);
    }
    let graph_id = u64::from_be_bytes(frame[22..30].try_into().expect("fixed graph slice"));
    let shard_id = u32::from_be_bytes(frame[30..34].try_into().expect("fixed shard slice"));
    let placement_epoch = u64::from_be_bytes(frame[34..42].try_into().expect("fixed epoch slice"));
    let route = RaftRoute::new(cluster_id, graph_id, shard_id, placement_epoch)?;
    let message = Message::decode(&frame[ROUTED_HEADER_BYTES..checksum_offset])
        .map_err(|_| RaftTransportError::InvalidProtobuf)?;
    if message.to != expected_node_id {
        return Err(RaftTransportError::RouteMismatch {
            expected: expected_node_id,
            actual: message.to,
        });
    }
    RoutedRaftMessage::new(route, message)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RaftTransportError {
    InvalidMagic,
    UnsupportedVersion { version: u16 },
    InvalidFrameLength,
    FrameTooLarge,
    ChecksumMismatch,
    InvalidProtobuf,
    InvalidRoute,
    ClusterMismatch,
    RouteMismatch { expected: u64, actual: u64 },
}

impl Display for RaftTransportError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
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
            Self::ClusterMismatch => formatter.write_str("Raft message belongs to another cluster"),
            Self::RouteMismatch { expected, actual } => {
                write!(
                    formatter,
                    "Raft message targets node {actual}; expected {expected}"
                )
            }
        }
    }
}

impl Error for RaftTransportError {}
