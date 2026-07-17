#![forbid(unsafe_code)]

mod key;

pub use key::{
    EdgeTypeId, ElementId, ElementKind, ElementRef, GraphId, GraphKey, KeyCodecError, LabelId,
    PartitionId, current_edge_key, current_vertex_key, decode_graph_key, edge_identity_key,
    history_anchor_key, history_prefix, in_adjacency_key, out_adjacency_key, vertex_identity_key,
};
