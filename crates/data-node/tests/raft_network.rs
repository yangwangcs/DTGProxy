use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use data_node::{RaftNetworkError, SharedRaftTransport};
use raft::eraftpb::{Message, MessageType};
use raft_transport::{RaftRoute, RoutedRaftMessage};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn localhost(port: u16) -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)
}

fn routed(
    cluster_id: [u8; 16],
    graph_id: u64,
    shard_id: u32,
    epoch: u64,
    from: u64,
    to: u64,
    index: u64,
) -> RoutedRaftMessage {
    RoutedRaftMessage::new(
        RaftRoute::new(cluster_id, graph_id, shard_id, epoch).unwrap(),
        Message {
            msg_type: MessageType::MsgAppend.into(),
            from,
            to,
            term: 3,
            index,
            commit: index.saturating_sub(1),
            ..Default::default()
        },
    )
    .unwrap()
}

#[tokio::test(flavor = "current_thread")]
async fn one_persistent_connection_multiplexes_shards_and_acknowledges_delivery() {
    let cluster = [0x71; 16];
    let mut first = SharedRaftTransport::bind(cluster, 1, localhost(0), 4)
        .await
        .unwrap();
    let mut second = SharedRaftTransport::bind(cluster, 2, localhost(0), 4)
        .await
        .unwrap();
    first.add_peer(2, second.local_addr()).unwrap();
    second.add_peer(1, first.local_addr()).unwrap();

    let shard_9 = routed(cluster, 7, 9, 11, 1, 2, 101);
    let shard_10 = routed(cluster, 7, 10, 13, 1, 2, 102);
    first.send(shard_9.clone()).await.unwrap();
    first.send(shard_10.clone()).await.unwrap();

    assert_eq!(second.receive().await.unwrap(), shard_9);
    assert_eq!(second.receive().await.unwrap(), shard_10);
    assert_eq!(second.accepted_connections(), 1);

    first.shutdown().await.unwrap();
    second.shutdown().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn per_peer_queue_is_bounded_before_network_work_and_unknown_peers_fail_closed() {
    let cluster = [0x72; 16];
    let mut first = SharedRaftTransport::bind(cluster, 1, localhost(0), 1)
        .await
        .unwrap();
    let mut second = SharedRaftTransport::bind(cluster, 2, localhost(0), 1)
        .await
        .unwrap();
    first.add_peer(2, second.local_addr()).unwrap();
    second.add_peer(1, first.local_addr()).unwrap();

    let delivery = first
        .try_send(routed(cluster, 7, 9, 11, 1, 2, 101))
        .unwrap();
    assert!(matches!(
        first.try_send(routed(cluster, 7, 10, 13, 1, 2, 102)),
        Err(RaftNetworkError::PeerQueueFull { node_id: 2 })
    ));
    assert!(matches!(
        first.try_send(routed(cluster, 7, 9, 11, 1, 99, 103)),
        Err(RaftNetworkError::UnknownPeer { node_id: 99 })
    ));
    tokio::time::timeout(Duration::from_secs(2), delivery.wait())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), second.receive())
            .await
            .unwrap()
            .unwrap()
            .message()
            .index,
        101
    );

    first.shutdown().await.unwrap();
    second.shutdown().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn receiver_rejects_a_valid_frame_from_another_cluster() {
    let mut first = SharedRaftTransport::bind([0x73; 16], 1, localhost(0), 4)
        .await
        .unwrap();
    let mut second = SharedRaftTransport::bind([0x74; 16], 2, localhost(0), 4)
        .await
        .unwrap();
    first.add_peer(2, second.local_addr()).unwrap();
    second.add_peer(1, first.local_addr()).unwrap();

    assert_eq!(
        first.send(routed([0x73; 16], 7, 9, 11, 1, 2, 101)).await,
        Err(RaftNetworkError::PeerRejected)
    );

    first.shutdown().await.unwrap();
    second.shutdown().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn plaintext_transport_is_restricted_to_loopback() {
    assert!(matches!(
        SharedRaftTransport::bind([0x75; 16], 1, "0.0.0.0:7101".parse().unwrap(), 4).await,
        Err(RaftNetworkError::InsecureNonLoopback)
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn a_slow_peer_does_not_block_another_peers_writer() {
    let cluster = [0x76; 16];
    let blackhole = tokio::net::TcpListener::bind(localhost(0)).await.unwrap();
    let blackhole_address = blackhole.local_addr().unwrap();
    let blackhole_task = tokio::spawn(async move {
        let (_stream, _) = blackhole.accept().await.unwrap();
        tokio::time::sleep(Duration::from_secs(2)).await;
    });

    let mut first = SharedRaftTransport::bind(cluster, 1, localhost(0), 4)
        .await
        .unwrap();
    let mut third = SharedRaftTransport::bind(cluster, 3, localhost(0), 4)
        .await
        .unwrap();
    first.add_peer(2, blackhole_address).unwrap();
    first.add_peer(3, third.local_addr()).unwrap();
    third.add_peer(1, first.local_addr()).unwrap();

    let slow = first
        .try_send(routed(cluster, 7, 9, 11, 1, 2, 101))
        .unwrap();
    first
        .send(routed(cluster, 7, 10, 13, 1, 3, 102))
        .await
        .unwrap();
    assert_eq!(third.receive().await.unwrap().message().index, 102);
    assert_eq!(slow.wait().await, Err(RaftNetworkError::Timeout));

    blackhole_task.abort();
    let _ = blackhole_task.await;
    first.shutdown().await.unwrap();
    third.shutdown().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn reconnect_replays_the_exact_same_routed_frame() {
    let cluster = [0x77; 16];
    let listener = tokio::net::TcpListener::bind(localhost(0)).await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let mut frames = Vec::new();
        for attempt in 0..2 {
            let (mut stream, _) = listener.accept().await.unwrap();
            let length = stream.read_u32().await.unwrap() as usize;
            let mut frame = vec![0; length];
            stream.read_exact(&mut frame).await.unwrap();
            frames.push(frame);
            if attempt == 1 {
                stream.write_u8(0xA1).await.unwrap();
                stream.flush().await.unwrap();
            }
        }
        frames
    });

    let mut first = SharedRaftTransport::bind(cluster, 1, localhost(0), 4)
        .await
        .unwrap();
    first.add_peer(2, address).unwrap();
    first
        .send(routed(cluster, 7, 9, 11, 1, 2, 101))
        .await
        .unwrap();

    let frames = peer.await.unwrap();
    assert_eq!(frames.len(), 2);
    assert_eq!(frames[0], frames[1]);
    first.shutdown().await.unwrap();
}
