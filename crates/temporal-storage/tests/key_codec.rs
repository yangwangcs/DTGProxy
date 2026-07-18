use storage_api::Keyspace;
use temporal_storage::{
    EdgeTypeId, ElementId, ElementRef, GraphId, GraphKey, KeyCodecError, LabelId, PartitionId,
    cross_in_adjacency_key, cross_out_adjacency_key, current_vertex_key, decode_graph_key,
    graph_key_scope, history_anchor_key, history_prefix, in_adjacency_key, out_adjacency_key,
    vertex_identity_key,
};
use temporal_types::TransactionTime;

fn vertex() -> ElementRef {
    ElementRef::vertex(GraphId::new(1), PartitionId::new(2), ElementId::new(3))
}

#[test]
fn cross_partition_adjacency_preserves_remote_and_edge_ownership() {
    let graph = GraphId::new(9);
    let source_partition = PartitionId::new(4);
    let destination_partition = PartitionId::new(8);
    let source = ElementId::new(10);
    let destination = ElementId::new(20);
    let edge = ElementId::new(30);
    let edge_type = EdgeTypeId::new(7);
    let out = cross_out_adjacency_key(
        graph,
        source_partition,
        source,
        edge_type,
        5,
        destination_partition,
        destination,
        source_partition,
        edge,
    );
    let incoming = cross_in_adjacency_key(
        graph,
        destination_partition,
        destination,
        edge_type,
        5,
        source_partition,
        source,
        source_partition,
        edge,
    );

    assert_eq!(out.as_bytes()[0], 0x12);
    assert_eq!(incoming.as_bytes()[0], 0x13);
    let decoded_out = decode_graph_key(&out).unwrap();
    let decoded_in = decode_graph_key(&incoming).unwrap();
    assert_eq!(
        decoded_out,
        GraphKey::CrossOutAdjacency {
            graph,
            partition: source_partition,
            source,
            edge_type,
            bucket: 5,
            destination_partition,
            destination,
            edge_partition: source_partition,
            edge,
        }
    );
    assert_eq!(
        decoded_in,
        GraphKey::CrossInAdjacency {
            graph,
            partition: destination_partition,
            destination,
            edge_type,
            bucket: 5,
            source_partition,
            source,
            edge_partition: source_partition,
            edge,
        }
    );
    assert_eq!(graph_key_scope(decoded_out), (graph, source_partition));
    assert_eq!(graph_key_scope(decoded_in), (graph, destination_partition));
}

#[test]
fn schema_identifiers_keep_their_explicit_widths() {
    assert_eq!(LabelId::new(4).value(), 4);
    assert_eq!(EdgeTypeId::new(7).value(), 7);
}

#[test]
fn vertex_identity_and_current_keys_have_stable_golden_bytes() {
    let mut identity_bytes = vec![0x01];
    identity_bytes.extend_from_slice(&1_u64.to_be_bytes());
    identity_bytes.extend_from_slice(&2_u32.to_be_bytes());
    identity_bytes.extend_from_slice(&3_u128.to_be_bytes());

    let identity = vertex_identity_key(vertex());
    let current = current_vertex_key(vertex());

    assert_eq!(identity.keyspace(), Keyspace::Identity);
    assert_eq!(identity.as_bytes(), identity_bytes);
    assert_eq!(current.keyspace(), Keyspace::Current);
    assert_eq!(current.as_bytes()[0], 0x08);
    assert_eq!(&current.as_bytes()[1..], &identity.as_bytes()[1..]);
    assert_eq!(
        decode_graph_key(&identity).unwrap(),
        GraphKey::VertexIdentity(vertex())
    );
    assert_eq!(
        decode_graph_key(&current).unwrap(),
        GraphKey::CurrentVertex(vertex())
    );
}

#[test]
fn adjacency_keys_are_directional_and_round_trip() {
    let graph = GraphId::new(9);
    let partition = PartitionId::new(4);
    let source = ElementId::new(10);
    let destination = ElementId::new(20);
    let edge = ElementId::new(30);
    let edge_type = EdgeTypeId::new(7);

    let out = out_adjacency_key(graph, partition, source, edge_type, 5, destination, edge);
    let incoming = in_adjacency_key(graph, partition, destination, edge_type, 5, source, edge);

    assert_eq!(out.keyspace(), Keyspace::AdjOut);
    assert_eq!(out.as_bytes()[0], 0x10);
    assert_eq!(incoming.keyspace(), Keyspace::AdjIn);
    assert_eq!(incoming.as_bytes()[0], 0x11);
    assert_eq!(
        decode_graph_key(&out).unwrap(),
        GraphKey::OutAdjacency {
            graph,
            partition,
            source,
            edge_type,
            bucket: 5,
            destination,
            edge,
        }
    );
    assert_eq!(
        decode_graph_key(&incoming).unwrap(),
        GraphKey::InAdjacency {
            graph,
            partition,
            destination,
            edge_type,
            bucket: 5,
            source,
            edge,
        }
    );
}

#[test]
fn reverse_transaction_time_orders_newer_history_first() {
    let older = history_anchor_key(vertex(), TransactionTime::new(100, 0), 0);
    let newer = history_anchor_key(vertex(), TransactionTime::new(200, 0), 0);

    assert_eq!(older.keyspace(), Keyspace::History);
    assert!(newer.as_bytes() < older.as_bytes());
    assert!(newer.as_bytes().starts_with(&history_prefix(vertex())));
    assert_eq!(
        decode_graph_key(&newer).unwrap(),
        GraphKey::HistoryAnchor {
            element: vertex(),
            transaction_time: TransactionTime::new(200, 0),
            segment_id: 0,
        }
    );
}

#[test]
fn decoder_rejects_wrong_keyspace_truncation_and_trailing_bytes() {
    let mut wrong_space = vertex_identity_key(vertex());
    wrong_space =
        storage_api::LogicalKey::in_keyspace(Keyspace::Current, wrong_space.as_bytes().to_vec());
    assert_eq!(
        decode_graph_key(&wrong_space),
        Err(KeyCodecError::WrongKeyspace)
    );

    let truncated = storage_api::LogicalKey::in_keyspace(Keyspace::Identity, vec![0x01, 0]);
    assert_eq!(
        decode_graph_key(&truncated),
        Err(KeyCodecError::UnexpectedEnd)
    );

    let mut trailing = vertex_identity_key(vertex()).as_bytes().to_vec();
    trailing.push(0);
    let trailing = storage_api::LogicalKey::in_keyspace(Keyspace::Identity, trailing);
    assert_eq!(
        decode_graph_key(&trailing),
        Err(KeyCodecError::TrailingBytes)
    );
}
