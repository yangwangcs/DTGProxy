#![forbid(unsafe_code)]

mod canonical;
mod diff;
mod history;
mod key;
mod mapping_tck;
mod overlay;
mod record;
mod rewrite;
mod store;
mod transaction;

pub use canonical::{
    CanonicalGraphEntry, CanonicalMappingError, decode_canonical_graph_entry,
    encode_canonical_graph_entry,
};
pub use diff::{TemporalChange, TemporalChangeKind};
pub use key::{
    EdgeTypeId, ElementId, ElementKind, ElementRef, GraphId, GraphKey, KeyCodecError, LabelId,
    PartitionId, cross_in_adjacency_key, cross_in_adjacency_prefix, cross_out_adjacency_key,
    cross_out_adjacency_prefix, current_edge_graph_prefix, current_edge_key,
    current_vertex_graph_prefix, current_vertex_key, decode_graph_key, edge_identity_graph_prefix,
    edge_identity_key, edge_identity_prefix, graph_key_scope, history_anchor_key, history_prefix,
    in_adjacency_key, in_adjacency_prefix, out_adjacency_key, out_adjacency_prefix,
    vertex_identity_graph_prefix, vertex_identity_key,
};
pub use mapping_tck::run_temporal_graph_mapping_tck;
pub use overlay::{OverlaySavepoint, TransactionOverlay, TransactionOverlayError};
pub use record::{
    EdgeIdentity, HistoryAnchor, HistoryDelta, HistoryEntry, ProjectionRecord, RecordCodecError,
    ValidSegment, VertexIdentity,
};
pub use store::{
    CommitContext, EdgeMutation, EdgeTemporalSegment, EdgeView, PrepareContext, TemporalStore,
    TemporalStoreError, TemporalStoreFuture, VertexMutation, VertexTemporalSegment, VertexView,
};
pub use transaction::{EndpointGuard, TemporalTransaction};
