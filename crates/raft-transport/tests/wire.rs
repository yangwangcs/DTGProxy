use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use raft::eraftpb::{Message, MessageType};
use raft_transport::{
    RaftTransportError, TcpRaftTransport, decode_message_frame, encode_message_frame,
};

#[test]
fn versioned_frame_round_trips_and_rejects_corruption_or_wrong_route() {
    let message = message(1, 2, 7);
    let encoded = encode_message_frame(9, &message).unwrap();
    assert_eq!(decode_message_frame(9, 2, &encoded).unwrap(), message);
    assert!(matches!(
        decode_message_frame(8, 2, &encoded),
        Err(RaftTransportError::ShardMismatch { .. })
    ));
    assert!(matches!(
        decode_message_frame(9, 3, &encoded),
        Err(RaftTransportError::RouteMismatch { .. })
    ));

    let mut corrupted = encoded;
    let middle = corrupted.len() / 2;
    corrupted[middle] ^= 1;
    assert!(matches!(
        decode_message_frame(9, 2, &corrupted),
        Err(RaftTransportError::ChecksumMismatch)
    ));
}

#[test]
fn tcp_loopback_moves_real_raft_messages_between_replaceable_endpoints() {
    let mut first = TcpRaftTransport::bind(9, 1, localhost(0), BTreeMap::new()).unwrap();
    let mut second = TcpRaftTransport::bind(9, 2, localhost(0), BTreeMap::new()).unwrap();
    first.set_peer(2, second.local_addr().unwrap());
    second.set_peer(1, first.local_addr().unwrap());
    first.set_timeouts(Duration::from_millis(500), Duration::from_millis(500));
    second.set_timeouts(Duration::from_millis(500), Duration::from_millis(500));

    let outbound = message(1, 2, 11);
    first.send(&outbound).unwrap();
    assert_eq!(second.receive_available().unwrap(), vec![outbound]);

    let response = message(2, 1, 12);
    second.send(&response).unwrap();
    assert_eq!(first.receive_available().unwrap(), vec![response]);
}

fn localhost(port: u16) -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)
}

fn message(from: u64, to: u64, index: u64) -> Message {
    Message {
        msg_type: MessageType::MsgAppend.into(),
        from,
        to,
        term: 3,
        index,
        commit: index.saturating_sub(1),
        ..Default::default()
    }
}
