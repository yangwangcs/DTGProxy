#![forbid(unsafe_code)]

use temporal_storage::{
    EdgeTypeId, EdgeView, ElementId, ElementRef, LabelId, TemporalEventOperation, VertexView,
};
use temporal_types::CanonicalElement;

mod batch;
mod child_invocation;
mod column_batch;
mod context;
mod error;
mod executor;
mod expression;
mod overlay;
mod temporal;
mod temporal_row;

pub use batch::{MAX_BATCH_ROWS, RecordBatch, RuntimeValue};
pub use child_invocation::{
    ApplyBudgetLedger, ChildInvocationFuture, ChildInvocationLimits, ChildOutputDemand,
    ChildPlanInvoker, IntervalChildInvocationFuture,
};
pub use column_batch::{ColumnBatch, ColumnValueRef, ColumnVector, ValidityBitmap};
pub use context::{
    AblationAxis, BenchmarkAblationConfig, BenchmarkAblationCounters,
    BenchmarkAblationCountersSnapshot, CancellationToken, ExecutionContext, QueryExecutionMetrics,
    QueryExecutionMetricsSnapshot,
};
pub use error::RuntimeError;
pub use executor::{BatchExecutor, preflight_procedure_parameters};
pub use overlay::{GraphOverlay, GraphOverlayEntry, GraphOverlayError};
pub use temporal::{
    ChangeEventBatch, ChangeFragmentResult, ChangeScanScope, ChangeWindow, ExecutionMorsel,
    RecordMorselFuture, RecordMorselSource, ResolvedTemporalScope, ResolvedValidTime,
    TemporalBatchExecutor, TemporalExecutionError, TemporalRead, TransactionRead,
    execute_interval_coordinator_operators, execute_interval_coordinator_operators_with_invoker,
    execute_interval_coordinator_operators_with_invoker_and_ledger, resolve_change_scope,
    resolve_temporal_scope,
};
pub use temporal_row::{
    TemporalProvenance, TemporalRecordBatch, TemporalRegion, TemporalRow, coalesce_temporal_rows,
    distinct_temporal_rows, ensure_temporal_rows_memory, temporal_hash_join,
    temporal_hash_join_bounded, temporal_join, temporal_left_hash_join,
    temporal_left_hash_join_bounded,
};
pub use temporal_semantics::{
    ChangeEvent, DerivedInterval, IntervalCell, TemporalOperation, coalesce_interval_cells,
    derive_visible_intervals, intersect_interval_sets,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ChangeMetadata {
    valid_from: temporal_types::ValidTime,
    valid_to: Option<temporal_types::ValidTime>,
    commit: temporal_types::TransactionTime,
    ordinal: u32,
    operation: TemporalEventOperation,
}

impl ChangeMetadata {
    #[must_use]
    pub const fn new(
        valid_from: temporal_types::ValidTime,
        valid_to: Option<temporal_types::ValidTime>,
        commit: temporal_types::TransactionTime,
        ordinal: u32,
        operation: TemporalEventOperation,
    ) -> Self {
        Self {
            valid_from,
            valid_to,
            commit,
            ordinal,
            operation,
        }
    }

    #[must_use]
    pub const fn valid_from(&self) -> temporal_types::ValidTime {
        self.valid_from
    }
    #[must_use]
    pub const fn valid_to(&self) -> Option<temporal_types::ValidTime> {
        self.valid_to
    }
    #[must_use]
    pub const fn commit(&self) -> temporal_types::TransactionTime {
        self.commit
    }
    #[must_use]
    pub const fn ordinal(&self) -> u32 {
        self.ordinal
    }
    #[must_use]
    pub const fn operation(&self) -> TemporalEventOperation {
        self.operation
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VertexRecord {
    element: ElementRef,
    label: Option<LabelId>,
    payload: CanonicalElement,
    change_metadata: Option<ChangeMetadata>,
}

impl VertexRecord {
    #[must_use]
    pub const fn new(
        element: ElementRef,
        label: Option<LabelId>,
        payload: CanonicalElement,
    ) -> Self {
        Self {
            element,
            label,
            payload,
            change_metadata: None,
        }
    }

    #[must_use]
    pub const fn element(&self) -> ElementRef {
        self.element
    }

    #[must_use]
    pub const fn label(&self) -> Option<LabelId> {
        self.label
    }

    #[must_use]
    pub const fn payload(&self) -> &CanonicalElement {
        &self.payload
    }

    #[must_use]
    pub const fn with_change_metadata(mut self, metadata: ChangeMetadata) -> Self {
        self.change_metadata = Some(metadata);
        self
    }

    #[must_use]
    pub const fn change_metadata(&self) -> Option<ChangeMetadata> {
        self.change_metadata
    }
}

impl From<VertexView> for VertexRecord {
    fn from(value: VertexView) -> Self {
        Self::new(
            value.element(),
            Some(value.label()),
            value.payload().clone(),
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EdgeRecord {
    element: ElementRef,
    edge_type: EdgeTypeId,
    source: ElementRef,
    destination: ElementRef,
    payload: CanonicalElement,
    change_metadata: Option<ChangeMetadata>,
}

impl EdgeRecord {
    #[must_use]
    pub const fn new(
        element: ElementRef,
        edge_type: EdgeTypeId,
        source: ElementId,
        destination: ElementId,
        payload: CanonicalElement,
    ) -> Self {
        Self::from_endpoints(
            element,
            edge_type,
            ElementRef::vertex(element.graph(), element.partition(), source),
            ElementRef::vertex(element.graph(), element.partition(), destination),
            payload,
        )
    }

    #[must_use]
    pub const fn from_endpoints(
        element: ElementRef,
        edge_type: EdgeTypeId,
        source: ElementRef,
        destination: ElementRef,
        payload: CanonicalElement,
    ) -> Self {
        Self {
            element,
            edge_type,
            source,
            destination,
            payload,
            change_metadata: None,
        }
    }

    #[must_use]
    pub const fn element(&self) -> ElementRef {
        self.element
    }

    #[must_use]
    pub const fn edge_type(&self) -> EdgeTypeId {
        self.edge_type
    }

    #[must_use]
    pub const fn source(&self) -> ElementId {
        self.source.id()
    }

    #[must_use]
    pub const fn destination(&self) -> ElementId {
        self.destination.id()
    }

    #[must_use]
    pub const fn source_ref(&self) -> ElementRef {
        self.source
    }

    #[must_use]
    pub const fn destination_ref(&self) -> ElementRef {
        self.destination
    }

    #[must_use]
    pub const fn payload(&self) -> &CanonicalElement {
        &self.payload
    }

    #[must_use]
    pub const fn with_change_metadata(mut self, metadata: ChangeMetadata) -> Self {
        self.change_metadata = Some(metadata);
        self
    }

    #[must_use]
    pub const fn change_metadata(&self) -> Option<ChangeMetadata> {
        self.change_metadata
    }
}

impl From<EdgeView> for EdgeRecord {
    fn from(value: EdgeView) -> Self {
        Self::from_endpoints(
            value.element(),
            value.edge_type(),
            value.source_ref(),
            value.destination_ref(),
            value.payload().clone(),
        )
    }
}
