#![forbid(unsafe_code)]

mod canonical;
mod diff;
mod history;
mod history_reader;
mod key;
mod mapping_tck;
mod observed_adapter;
mod overlay;
mod record;
mod record_ref;
mod rewrite;
mod store;
mod transaction;

pub use canonical::{
    CanonicalGraphEntry, CanonicalMappingError, decode_canonical_graph_entry,
    encode_canonical_graph_entry,
};
pub use diff::{TemporalChange, TemporalChangeKind};
pub use history_reader::{
    HistoryReadBudget, HistoryReadStats, PointHistoryOutcome, PointHistoryReader,
    PointHistoryRequest, PropertyDemand,
};
pub use key::{
    EdgeTypeId, ElementId, ElementKind, ElementRef, GraphId, GraphKey, KeyCodecError, LabelId,
    PartitionId, cross_in_adjacency_key, cross_in_adjacency_prefix, cross_out_adjacency_key,
    cross_out_adjacency_prefix, current_edge_graph_prefix, current_edge_key,
    current_vertex_graph_prefix, current_vertex_key, decode_graph_key, edge_identity_graph_prefix,
    edge_identity_key, edge_identity_prefix, graph_key_prefix_scope, graph_key_scope,
    history_anchor_key, history_prefix, in_adjacency_key, in_adjacency_prefix, out_adjacency_key,
    out_adjacency_prefix, temporal_event_graph_prefix, temporal_event_key,
    temporal_event_valid_key, vertex_identity_graph_prefix, vertex_identity_key,
};
pub use mapping_tck::run_temporal_graph_mapping_tck;
pub use observed_adapter::{AdapterCallObserver, ObservedStorageAdapter, observe_read_snapshot};
pub use overlay::{OverlaySavepoint, TransactionOverlay, TransactionOverlayError};
pub use record::{
    CanonicalTemporalEvent, EdgeIdentity, HistoryAnchor, HistoryDelta, HistoryEntry,
    ProjectionRecord, RecordCodecError, TemporalEventMetadata, TemporalEventOperation,
    ValidSegment, VertexIdentity,
};
pub use record_ref::{
    HistoryAnchorRef, HistoryDeltaRef, HistoryEntryRef, HistoryOperationRef, ProjectionRecordRef,
};
pub use store::{
    CommitContext, EdgeMutation, EdgeTemporalSegment, EdgeView, PrepareContext, TemporalScanBudget,
    TemporalStore, TemporalStoreError, TemporalStoreFuture, VertexCandidateScanPage,
    VertexMutation, VertexTemporalSegment, VertexView,
};
pub use transaction::{EndpointGuard, TemporalTransaction};
