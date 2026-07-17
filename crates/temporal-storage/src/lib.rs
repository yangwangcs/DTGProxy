#![forbid(unsafe_code)]

mod key;
mod record;
mod rewrite;
mod store;

pub use key::{
    EdgeTypeId, ElementId, ElementKind, ElementRef, GraphId, GraphKey, KeyCodecError, LabelId,
    PartitionId, current_edge_key, current_vertex_key, decode_graph_key, edge_identity_key,
    history_anchor_key, history_prefix, in_adjacency_key, in_adjacency_prefix, out_adjacency_key,
    out_adjacency_prefix, vertex_identity_key,
};
pub use record::{
    EdgeIdentity, HistoryAnchor, ProjectionRecord, RecordCodecError, ValidSegment, VertexIdentity,
};
pub use store::{
    CommitContext, EdgeMutation, EdgeView, TemporalStore, TemporalStoreError, TemporalStoreFuture,
    VertexMutation,
};
