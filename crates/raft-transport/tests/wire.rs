use raft::eraftpb::{Message, MessageType};
use raft_transport::{
    RaftRoute, RaftTransportError, RoutedRaftMessage, decode_routed_message_frame,
    encode_routed_message_frame,
};

#[test]
fn cluster_routed_frame_carries_graph_shard_epoch_and_revalidates_identity() {
    let route = RaftRoute::new([0x61; 16], 7, 9, 11).unwrap();
    let routed = RoutedRaftMessage::new(route.clone(), message(1, 2, 13)).unwrap();
    let encoded = encode_routed_message_frame(&routed).unwrap();
    assert_eq!(
        decode_routed_message_frame([0x61; 16], 2, &encoded).unwrap(),
        routed
    );
    assert_eq!(
        decode_routed_message_frame([0x62; 16], 2, &encoded),
        Err(RaftTransportError::ClusterMismatch)
    );
    assert!(matches!(
        decode_routed_message_frame([0x61; 16], 3, &encoded),
        Err(RaftTransportError::RouteMismatch {
            expected: 3,
            actual: 2
        })
    ));

    let mut corrupted = encoded;
    let middle = corrupted.len() / 2;
    corrupted[middle] ^= 1;
    assert_eq!(
        decode_routed_message_frame([0x61; 16], 2, &corrupted),
        Err(RaftTransportError::ChecksumMismatch)
    );
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
