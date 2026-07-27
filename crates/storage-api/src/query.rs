use std::error::Error;
use std::fmt::{self, Display, Formatter};

use crate::{KeySpan, KeyValue, Keyspace, LogicalKey};
use temporal_types::{GraphValue, ValidTime};

pub const MAX_QUERY_PAGE_ITEMS: usize = 65_536;
pub const MAX_QUERY_PAGE_BYTES: u64 = 16 * 1024 * 1024;
pub const MAX_CANDIDATE_CONSTRAINTS: usize = 256;

/// Describes what an adapter guarantees about a pushed-down operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PushdownGuarantee {
    /// The primitive must not be scheduled on this adapter.
    Unsupported,
    /// Results are a no-false-negative superset; DTGProxy must evaluate residuals.
    Candidate,
    /// Results are complete and exact for the request; residuals may be omitted.
    Exact,
}

impl PushdownGuarantee {
    /// Returns `None` when execution is unsupported, otherwise whether the
    /// caller must retain residual evaluation.
    #[must_use]
    pub const fn residual_required(self) -> Option<bool> {
        match self {
            Self::Unsupported => None,
            Self::Candidate => Some(true),
            Self::Exact => Some(false),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueryPrimitiveCapabilities {
    candidate_scan: PushdownGuarantee,
    property_gather: PushdownGuarantee,
    adjacency_expand: PushdownGuarantee,
    change_scan: PushdownGuarantee,
}

impl QueryPrimitiveCapabilities {
    pub const NONE: Self = Self::new(
        PushdownGuarantee::Unsupported,
        PushdownGuarantee::Unsupported,
        PushdownGuarantee::Unsupported,
        PushdownGuarantee::Unsupported,
    );

    #[must_use]
    pub const fn new(
        candidate_scan: PushdownGuarantee,
        property_gather: PushdownGuarantee,
        adjacency_expand: PushdownGuarantee,
        change_scan: PushdownGuarantee,
    ) -> Self {
        Self {
            candidate_scan,
            property_gather,
            adjacency_expand,
            change_scan,
        }
    }

    #[must_use]
    pub const fn candidate_scan(self) -> PushdownGuarantee {
        self.candidate_scan
    }

    #[must_use]
    pub const fn property_gather(self) -> PushdownGuarantee {
        self.property_gather
    }

    #[must_use]
    pub const fn adjacency_expand(self) -> PushdownGuarantee {
        self.adjacency_expand
    }

    #[must_use]
    pub const fn change_scan(self) -> PushdownGuarantee {
        self.change_scan
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueryPageBounds {
    max_items: usize,
    max_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CanonicalScanRequest {
    span: KeySpan,
    bounds: QueryPageBounds,
}

impl CanonicalScanRequest {
    pub fn new(span: KeySpan, bounds: QueryPageBounds) -> Result<Self, QueryPrimitiveError> {
        validate_unbounded_span(&span)?;
        charge_request_bytes(0, span_size(&span), bounds)?;
        Ok(Self { span, bounds })
    }

    #[must_use]
    pub const fn span(&self) -> &KeySpan {
        &self.span
    }

    #[must_use]
    pub const fn bounds(&self) -> QueryPageBounds {
        self.bounds
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CanonicalScanPage {
    applied_log_index: u64,
    entries: Vec<KeyValue>,
    next_start: Option<LogicalKey>,
}

impl CanonicalScanPage {
    pub fn new(
        request: &CanonicalScanRequest,
        applied_log_index: u64,
        entries: Vec<KeyValue>,
        next_start: Option<LogicalKey>,
    ) -> Result<Self, QueryPrimitiveError> {
        validate_canonical_scan_page(
            request.span(),
            request.bounds(),
            &entries,
            next_start.as_ref(),
        )?;
        Ok(Self {
            applied_log_index,
            entries,
            next_start,
        })
    }

    #[must_use]
    pub const fn applied_log_index(&self) -> u64 {
        self.applied_log_index
    }

    #[must_use]
    pub fn entries(&self) -> &[KeyValue] {
        &self.entries
    }

    #[must_use]
    pub const fn next_start(&self) -> Option<&LogicalKey> {
        self.next_start.as_ref()
    }

    #[must_use]
    pub fn into_entries(self) -> Vec<KeyValue> {
        self.entries
    }
}

impl QueryPageBounds {
    pub fn new(max_items: usize, max_bytes: u64) -> Result<Self, QueryPrimitiveError> {
        if max_items == 0 {
            return Err(QueryPrimitiveError::ZeroItemLimit);
        }
        if max_bytes == 0 {
            return Err(QueryPrimitiveError::ZeroByteLimit);
        }
        if max_items > MAX_QUERY_PAGE_ITEMS {
            return Err(QueryPrimitiveError::ItemLimitTooLarge {
                limit: max_items,
                maximum: MAX_QUERY_PAGE_ITEMS,
            });
        }
        if max_bytes > MAX_QUERY_PAGE_BYTES {
            return Err(QueryPrimitiveError::ByteLimitTooLarge {
                limit: max_bytes,
                maximum: MAX_QUERY_PAGE_BYTES,
            });
        }
        Ok(Self {
            max_items,
            max_bytes,
        })
    }

    #[must_use]
    pub const fn max_items(self) -> usize {
        self.max_items
    }

    #[must_use]
    pub const fn max_bytes(self) -> u64 {
        self.max_bytes
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PropertyId(u32);

impl PropertyId {
    #[must_use]
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn value(self) -> u32 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ComparisonOperator {
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PropertyConstraint {
    property: PropertyId,
    operator: ComparisonOperator,
    value: GraphValue,
}

impl PropertyConstraint {
    #[must_use]
    pub const fn new(
        property: PropertyId,
        operator: ComparisonOperator,
        value: GraphValue,
    ) -> Self {
        Self {
            property,
            operator,
            value,
        }
    }

    #[must_use]
    pub const fn property(&self) -> PropertyId {
        self.property
    }

    #[must_use]
    pub const fn operator(&self) -> ComparisonOperator {
        self.operator
    }

    #[must_use]
    pub const fn value(&self) -> &GraphValue {
        &self.value
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct CandidateScanRequest {
    span: KeySpan,
    valid_time: ValidTime,
    constraints: Vec<PropertyConstraint>,
    bounds: QueryPageBounds,
}

impl CandidateScanRequest {
    pub fn new(
        span: KeySpan,
        valid_time: ValidTime,
        constraints: Vec<PropertyConstraint>,
        bounds: QueryPageBounds,
    ) -> Result<Self, QueryPrimitiveError> {
        validate_unbounded_span(&span)?;
        validate_keyspace(
            QueryPrimitiveKind::CandidateScan,
            span.keyspace(),
            &[
                Keyspace::Current,
                Keyspace::History,
                Keyspace::TemporalIndex,
            ],
        )?;
        if constraints.len() > MAX_CANDIDATE_CONSTRAINTS {
            return Err(QueryPrimitiveError::InputLimitExceeded {
                limit: MAX_CANDIDATE_CONSTRAINTS,
                actual: constraints.len(),
            });
        }
        let mut retained = charge_request_bytes(0, span_size(&span), bounds)?;
        for constraint in &constraints {
            retained = charge_request_bytes(retained, std::mem::size_of::<u32>(), bounds)?;
            retained =
                charge_request_bytes(retained, graph_value_size(constraint.value()), bounds)?;
        }
        Ok(Self {
            span,
            valid_time,
            constraints,
            bounds,
        })
    }

    #[must_use]
    pub const fn span(&self) -> &KeySpan {
        &self.span
    }

    #[must_use]
    pub const fn valid_time(&self) -> ValidTime {
        self.valid_time
    }

    #[must_use]
    pub fn constraints(&self) -> &[PropertyConstraint] {
        &self.constraints
    }

    #[must_use]
    pub const fn bounds(&self) -> QueryPageBounds {
        self.bounds
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct CandidateScanPage {
    applied_log_index: u64,
    guarantee: PushdownGuarantee,
    entries: Vec<KeyValue>,
    next_start: Option<LogicalKey>,
}

impl CandidateScanPage {
    pub fn new(
        request: &CandidateScanRequest,
        applied_log_index: u64,
        guarantee: PushdownGuarantee,
        entries: Vec<KeyValue>,
        next_start: Option<LogicalKey>,
    ) -> Result<Self, QueryPrimitiveError> {
        validate_scan_page(
            QueryPrimitiveKind::CandidateScan,
            request.span(),
            request.bounds(),
            guarantee,
            &entries,
            next_start.as_ref(),
        )?;
        Ok(Self {
            applied_log_index,
            guarantee,
            entries,
            next_start,
        })
    }

    scan_page_accessors!();
}

#[derive(Clone, Debug, PartialEq)]
pub struct PropertyGatherRequest {
    keys: Vec<LogicalKey>,
    properties: Vec<PropertyId>,
    bounds: QueryPageBounds,
}

impl PropertyGatherRequest {
    pub fn new(
        keys: Vec<LogicalKey>,
        properties: Vec<PropertyId>,
        bounds: QueryPageBounds,
    ) -> Result<Self, QueryPrimitiveError> {
        validate_nonempty_input(&keys, bounds)?;
        validate_nonempty_input(&properties, bounds)?;
        let cell_count = keys.len().saturating_mul(properties.len());
        if cell_count > bounds.max_items() {
            return Err(QueryPrimitiveError::InputLimitExceeded {
                limit: bounds.max_items(),
                actual: cell_count,
            });
        }
        let mut retained = 0;
        for key in &keys {
            validate_keyspace(
                QueryPrimitiveKind::PropertyGather,
                key.keyspace(),
                &[Keyspace::Current, Keyspace::History],
            )?;
            retained = charge_request_bytes(retained, key.as_bytes().len(), bounds)?;
        }
        for _ in &properties {
            retained = charge_request_bytes(retained, std::mem::size_of::<u32>(), bounds)?;
        }
        Ok(Self {
            keys,
            properties,
            bounds,
        })
    }

    #[must_use]
    pub fn keys(&self) -> &[LogicalKey] {
        &self.keys
    }

    #[must_use]
    pub fn properties(&self) -> &[PropertyId] {
        &self.properties
    }

    #[must_use]
    pub const fn bounds(&self) -> QueryPageBounds {
        self.bounds
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct PropertyRow {
    key: LogicalKey,
    values: Vec<Option<GraphValue>>,
}

impl PropertyRow {
    #[must_use]
    pub const fn new(key: LogicalKey, values: Vec<Option<GraphValue>>) -> Self {
        Self { key, values }
    }

    #[must_use]
    pub const fn key(&self) -> &LogicalKey {
        &self.key
    }

    #[must_use]
    pub fn values(&self) -> &[Option<GraphValue>] {
        &self.values
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct PropertyGatherPage {
    applied_log_index: u64,
    guarantee: PushdownGuarantee,
    rows: Vec<PropertyRow>,
}

impl PropertyGatherPage {
    pub fn new(
        request: &PropertyGatherRequest,
        applied_log_index: u64,
        guarantee: PushdownGuarantee,
        rows: Vec<PropertyRow>,
    ) -> Result<Self, QueryPrimitiveError> {
        validate_supported_guarantee(guarantee)?;
        if rows.len() != request.keys().len() {
            return Err(QueryPrimitiveError::CardinalityMismatch {
                expected: request.keys().len(),
                actual: rows.len(),
            });
        }
        let mut retained = 0;
        for (expected_key, row) in request.keys().iter().zip(&rows) {
            if row.key() != expected_key {
                return Err(QueryPrimitiveError::ResponseKeyMismatch);
            }
            if row.values().len() != request.properties().len() {
                return Err(QueryPrimitiveError::CardinalityMismatch {
                    expected: request.properties().len(),
                    actual: row.values().len(),
                });
            }
            retained = charge_bytes(retained, row.key().as_bytes().len(), request.bounds())?;
            for value in row.values().iter().flatten() {
                retained = charge_bytes(retained, graph_value_size(value), request.bounds())?;
            }
        }
        Ok(Self {
            applied_log_index,
            guarantee,
            rows,
        })
    }

    #[must_use]
    pub const fn applied_log_index(&self) -> u64 {
        self.applied_log_index
    }

    #[must_use]
    pub const fn guarantee(&self) -> PushdownGuarantee {
        self.guarantee
    }

    #[must_use]
    pub fn rows(&self) -> &[PropertyRow] {
        &self.rows
    }

    #[must_use]
    pub fn into_rows(self) -> Vec<PropertyRow> {
        self.rows
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct AdjacencyExpandRequest {
    spans: Vec<KeySpan>,
    bounds: QueryPageBounds,
}

impl AdjacencyExpandRequest {
    pub fn new(spans: Vec<KeySpan>, bounds: QueryPageBounds) -> Result<Self, QueryPrimitiveError> {
        validate_nonempty_input(&spans, bounds)?;
        let direction = spans[0].keyspace();
        let mut retained = 0;
        for span in &spans {
            validate_unbounded_span(span)?;
            validate_keyspace(
                QueryPrimitiveKind::AdjacencyExpand,
                span.keyspace(),
                &[Keyspace::AdjOut, Keyspace::AdjIn],
            )?;
            if span.keyspace() != direction {
                return Err(QueryPrimitiveError::MixedAdjacencyDirections);
            }
            retained = charge_request_bytes(retained, span_size(span), bounds)?;
        }
        Ok(Self { spans, bounds })
    }

    #[must_use]
    pub fn spans(&self) -> &[KeySpan] {
        &self.spans
    }

    #[must_use]
    pub const fn bounds(&self) -> QueryPageBounds {
        self.bounds
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct AdjacencyEntry {
    input_ordinal: usize,
    entry: KeyValue,
}

impl AdjacencyEntry {
    #[must_use]
    pub const fn new(input_ordinal: usize, entry: KeyValue) -> Self {
        Self {
            input_ordinal,
            entry,
        }
    }

    #[must_use]
    pub const fn input_ordinal(&self) -> usize {
        self.input_ordinal
    }

    #[must_use]
    pub const fn entry(&self) -> &KeyValue {
        &self.entry
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdjacencyCursor {
    input_ordinal: usize,
    start: LogicalKey,
}

impl AdjacencyCursor {
    #[must_use]
    pub const fn new(input_ordinal: usize, start: LogicalKey) -> Self {
        Self {
            input_ordinal,
            start,
        }
    }

    #[must_use]
    pub const fn input_ordinal(&self) -> usize {
        self.input_ordinal
    }

    #[must_use]
    pub const fn start(&self) -> &LogicalKey {
        &self.start
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct AdjacencyExpandPage {
    applied_log_index: u64,
    guarantee: PushdownGuarantee,
    entries: Vec<AdjacencyEntry>,
    next: Option<AdjacencyCursor>,
}

impl AdjacencyExpandPage {
    pub fn new(
        request: &AdjacencyExpandRequest,
        applied_log_index: u64,
        guarantee: PushdownGuarantee,
        entries: Vec<AdjacencyEntry>,
        next: Option<AdjacencyCursor>,
    ) -> Result<Self, QueryPrimitiveError> {
        validate_supported_guarantee(guarantee)?;
        validate_page_item_count(entries.len(), request.bounds())?;
        let mut retained = 0;
        let mut previous: Option<(usize, &[u8])> = None;
        for candidate in &entries {
            let span = request.spans().get(candidate.input_ordinal()).ok_or(
                QueryPrimitiveError::UnknownInputOrdinal {
                    ordinal: candidate.input_ordinal(),
                    input_count: request.spans().len(),
                },
            )?;
            validate_entry(QueryPrimitiveKind::AdjacencyExpand, span, candidate.entry())?;
            let position = (
                candidate.input_ordinal(),
                candidate.entry().key().as_bytes(),
            );
            if previous.is_some_and(|previous| previous >= position) {
                return Err(QueryPrimitiveError::EntriesNotStrictlyOrdered);
            }
            previous = Some(position);
            retained = charge_entry(retained, candidate.entry(), request.bounds())?;
        }
        if let Some(cursor) = &next {
            let span = request.spans().get(cursor.input_ordinal()).ok_or(
                QueryPrimitiveError::UnknownInputOrdinal {
                    ordinal: cursor.input_ordinal(),
                    input_count: request.spans().len(),
                },
            )?;
            validate_cursor(QueryPrimitiveKind::AdjacencyExpand, span, cursor.start())?;
        }
        Ok(Self {
            applied_log_index,
            guarantee,
            entries,
            next,
        })
    }

    #[must_use]
    pub const fn applied_log_index(&self) -> u64 {
        self.applied_log_index
    }

    #[must_use]
    pub const fn guarantee(&self) -> PushdownGuarantee {
        self.guarantee
    }

    #[must_use]
    pub fn entries(&self) -> &[AdjacencyEntry] {
        &self.entries
    }

    #[must_use]
    pub const fn next(&self) -> Option<&AdjacencyCursor> {
        self.next.as_ref()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChangeScanRequest {
    span: KeySpan,
    bounds: QueryPageBounds,
}

impl ChangeScanRequest {
    pub fn new(span: KeySpan, bounds: QueryPageBounds) -> Result<Self, QueryPrimitiveError> {
        validate_unbounded_span(&span)?;
        validate_keyspace(
            QueryPrimitiveKind::ChangeScan,
            span.keyspace(),
            &[Keyspace::TemporalIndex],
        )?;
        charge_request_bytes(0, span_size(&span), bounds)?;
        Ok(Self { span, bounds })
    }

    #[must_use]
    pub const fn span(&self) -> &KeySpan {
        &self.span
    }

    #[must_use]
    pub const fn bounds(&self) -> QueryPageBounds {
        self.bounds
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ChangeScanPage {
    applied_log_index: u64,
    guarantee: PushdownGuarantee,
    entries: Vec<KeyValue>,
    next_start: Option<LogicalKey>,
}

impl ChangeScanPage {
    pub fn new(
        request: &ChangeScanRequest,
        applied_log_index: u64,
        guarantee: PushdownGuarantee,
        entries: Vec<KeyValue>,
        next_start: Option<LogicalKey>,
    ) -> Result<Self, QueryPrimitiveError> {
        validate_scan_page(
            QueryPrimitiveKind::ChangeScan,
            request.span(),
            request.bounds(),
            guarantee,
            &entries,
            next_start.as_ref(),
        )?;
        Ok(Self {
            applied_log_index,
            guarantee,
            entries,
            next_start,
        })
    }

    scan_page_accessors!();
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryPrimitiveKind {
    CanonicalScan,
    CandidateScan,
    PropertyGather,
    AdjacencyExpand,
    ChangeScan,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryPrimitiveError {
    ZeroItemLimit,
    ZeroByteLimit,
    ItemLimitTooLarge {
        limit: usize,
        maximum: usize,
    },
    ByteLimitTooLarge {
        limit: u64,
        maximum: u64,
    },
    EmptyInput,
    InputLimitExceeded {
        limit: usize,
        actual: usize,
    },
    InvalidKeyspace {
        primitive: QueryPrimitiveKind,
        actual: Keyspace,
    },
    SpanAlreadyBounded,
    MixedAdjacencyDirections,
    UnsupportedGuarantee,
    PageItemLimitExceeded {
        limit: usize,
        actual: usize,
    },
    PageByteLimitExceeded {
        limit: u64,
        required: u64,
    },
    RequestByteLimitExceeded {
        limit: u64,
        required: u64,
    },
    EntryOutsideSpan,
    EntriesNotStrictlyOrdered,
    InvalidContinuation,
    CardinalityMismatch {
        expected: usize,
        actual: usize,
    },
    ResponseKeyMismatch,
    UnknownInputOrdinal {
        ordinal: usize,
        input_count: usize,
    },
}

impl Display for QueryPrimitiveError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroItemLimit => formatter.write_str("query page item limit must be positive"),
            Self::ZeroByteLimit => formatter.write_str("query page byte limit must be positive"),
            Self::ItemLimitTooLarge { limit, maximum } => {
                write!(formatter, "query page item limit {limit} exceeds {maximum}")
            }
            Self::ByteLimitTooLarge { limit, maximum } => {
                write!(formatter, "query page byte limit {limit} exceeds {maximum}")
            }
            Self::EmptyInput => formatter.write_str("query primitive input must not be empty"),
            Self::InputLimitExceeded { limit, actual } => {
                write!(
                    formatter,
                    "query primitive input has {actual} items above {limit}"
                )
            }
            Self::InvalidKeyspace { primitive, actual } => {
                write!(formatter, "{actual:?} is invalid for {primitive:?}")
            }
            Self::SpanAlreadyBounded => {
                formatter.write_str("primitive request span must not carry separate limits")
            }
            Self::MixedAdjacencyDirections => {
                formatter.write_str("one adjacency request cannot mix incoming and outgoing spans")
            }
            Self::UnsupportedGuarantee => {
                formatter.write_str("an adapter page cannot claim an unsupported guarantee")
            }
            Self::PageItemLimitExceeded { limit, actual } => {
                write!(formatter, "query page has {actual} items above {limit}")
            }
            Self::PageByteLimitExceeded { limit, required } => {
                write!(
                    formatter,
                    "query page requires {required} bytes above {limit}"
                )
            }
            Self::RequestByteLimitExceeded { limit, required } => {
                write!(
                    formatter,
                    "query request requires {required} bytes above {limit}"
                )
            }
            Self::EntryOutsideSpan => formatter.write_str("query page entry is outside its span"),
            Self::EntriesNotStrictlyOrdered => {
                formatter.write_str("query page entries must be strictly ordered")
            }
            Self::InvalidContinuation => {
                formatter.write_str("query page continuation is outside or behind its span")
            }
            Self::CardinalityMismatch { expected, actual } => {
                write!(
                    formatter,
                    "response cardinality {actual} does not match {expected}"
                )
            }
            Self::ResponseKeyMismatch => {
                formatter.write_str("property response key does not match request order")
            }
            Self::UnknownInputOrdinal {
                ordinal,
                input_count,
            } => write!(
                formatter,
                "adjacency input ordinal {ordinal} is outside {input_count} inputs"
            ),
        }
    }
}

impl Error for QueryPrimitiveError {}

macro_rules! scan_page_accessors {
    () => {
        #[must_use]
        pub const fn applied_log_index(&self) -> u64 {
            self.applied_log_index
        }

        #[must_use]
        pub const fn guarantee(&self) -> PushdownGuarantee {
            self.guarantee
        }

        #[must_use]
        pub fn entries(&self) -> &[KeyValue] {
            &self.entries
        }

        #[must_use]
        pub const fn next_start(&self) -> Option<&LogicalKey> {
            self.next_start.as_ref()
        }

        #[must_use]
        pub fn into_entries(self) -> Vec<KeyValue> {
            self.entries
        }
    };
}

use scan_page_accessors;

fn validate_unbounded_span(span: &KeySpan) -> Result<(), QueryPrimitiveError> {
    if span.limit().is_some() || span.max_bytes().is_some() {
        return Err(QueryPrimitiveError::SpanAlreadyBounded);
    }
    Ok(())
}

fn validate_nonempty_input<T>(
    input: &[T],
    bounds: QueryPageBounds,
) -> Result<(), QueryPrimitiveError> {
    if input.is_empty() {
        return Err(QueryPrimitiveError::EmptyInput);
    }
    if input.len() > bounds.max_items() {
        return Err(QueryPrimitiveError::InputLimitExceeded {
            limit: bounds.max_items(),
            actual: input.len(),
        });
    }
    Ok(())
}

fn validate_keyspace(
    primitive: QueryPrimitiveKind,
    actual: Keyspace,
    allowed: &[Keyspace],
) -> Result<(), QueryPrimitiveError> {
    if !allowed.contains(&actual) {
        return Err(QueryPrimitiveError::InvalidKeyspace { primitive, actual });
    }
    Ok(())
}

fn validate_supported_guarantee(guarantee: PushdownGuarantee) -> Result<(), QueryPrimitiveError> {
    if guarantee == PushdownGuarantee::Unsupported {
        return Err(QueryPrimitiveError::UnsupportedGuarantee);
    }
    Ok(())
}

fn validate_scan_page(
    primitive: QueryPrimitiveKind,
    span: &KeySpan,
    bounds: QueryPageBounds,
    guarantee: PushdownGuarantee,
    entries: &[KeyValue],
    next_start: Option<&LogicalKey>,
) -> Result<(), QueryPrimitiveError> {
    validate_supported_guarantee(guarantee)?;
    validate_page_item_count(entries.len(), bounds)?;
    let mut retained = 0;
    let mut previous: Option<&[u8]> = None;
    for entry in entries {
        validate_entry(primitive, span, entry)?;
        if previous.is_some_and(|previous| previous >= entry.key().as_bytes()) {
            return Err(QueryPrimitiveError::EntriesNotStrictlyOrdered);
        }
        previous = Some(entry.key().as_bytes());
        retained = charge_entry(retained, entry, bounds)?;
    }
    if let Some(next_start) = next_start {
        validate_cursor(primitive, span, next_start)?;
        if previous.is_some_and(|previous| previous >= next_start.as_bytes()) {
            return Err(QueryPrimitiveError::InvalidContinuation);
        }
    }
    Ok(())
}

fn validate_canonical_scan_page(
    span: &KeySpan,
    bounds: QueryPageBounds,
    entries: &[KeyValue],
    next_start: Option<&LogicalKey>,
) -> Result<(), QueryPrimitiveError> {
    validate_page_item_count(entries.len(), bounds)?;
    let mut retained = 0;
    let mut previous: Option<&[u8]> = None;
    for entry in entries {
        validate_entry(QueryPrimitiveKind::CanonicalScan, span, entry)?;
        if previous.is_some_and(|previous| previous >= entry.key().as_bytes()) {
            return Err(QueryPrimitiveError::EntriesNotStrictlyOrdered);
        }
        previous = Some(entry.key().as_bytes());
        retained = charge_entry(retained, entry, bounds)?;
    }
    if let Some(next_start) = next_start {
        validate_cursor(QueryPrimitiveKind::CanonicalScan, span, next_start)?;
        if previous.is_some_and(|previous| previous >= next_start.as_bytes()) {
            return Err(QueryPrimitiveError::InvalidContinuation);
        }
    }
    Ok(())
}

fn validate_page_item_count(
    actual: usize,
    bounds: QueryPageBounds,
) -> Result<(), QueryPrimitiveError> {
    if actual > bounds.max_items() {
        return Err(QueryPrimitiveError::PageItemLimitExceeded {
            limit: bounds.max_items(),
            actual,
        });
    }
    Ok(())
}

fn validate_entry(
    primitive: QueryPrimitiveKind,
    span: &KeySpan,
    entry: &KeyValue,
) -> Result<(), QueryPrimitiveError> {
    validate_keyspace(primitive, entry.key().keyspace(), &[span.keyspace()])?;
    if !span.contains(entry.key().as_bytes()) {
        return Err(QueryPrimitiveError::EntryOutsideSpan);
    }
    Ok(())
}

fn validate_cursor(
    primitive: QueryPrimitiveKind,
    span: &KeySpan,
    cursor: &LogicalKey,
) -> Result<(), QueryPrimitiveError> {
    validate_keyspace(primitive, cursor.keyspace(), &[span.keyspace()])?;
    if !span.contains(cursor.as_bytes()) {
        return Err(QueryPrimitiveError::InvalidContinuation);
    }
    Ok(())
}

fn charge_entry(
    retained: u64,
    entry: &KeyValue,
    bounds: QueryPageBounds,
) -> Result<u64, QueryPrimitiveError> {
    let retained = charge_bytes(retained, entry.key().as_bytes().len(), bounds)?;
    charge_bytes(retained, entry.value().len(), bounds)
}

fn charge_bytes(
    retained: u64,
    bytes: usize,
    bounds: QueryPageBounds,
) -> Result<u64, QueryPrimitiveError> {
    let required = u64::try_from(bytes)
        .ok()
        .and_then(|bytes| retained.checked_add(bytes))
        .unwrap_or(u64::MAX);
    if required > bounds.max_bytes() {
        return Err(QueryPrimitiveError::PageByteLimitExceeded {
            limit: bounds.max_bytes(),
            required,
        });
    }
    Ok(required)
}

fn charge_request_bytes(
    retained: u64,
    bytes: usize,
    bounds: QueryPageBounds,
) -> Result<u64, QueryPrimitiveError> {
    let required = u64::try_from(bytes)
        .ok()
        .and_then(|bytes| retained.checked_add(bytes))
        .unwrap_or(u64::MAX);
    if required > bounds.max_bytes() {
        return Err(QueryPrimitiveError::RequestByteLimitExceeded {
            limit: bounds.max_bytes(),
            required,
        });
    }
    Ok(required)
}

fn span_size(span: &KeySpan) -> usize {
    span.start()
        .len()
        .saturating_add(span.end().map_or(0, <[u8]>::len))
        .saturating_add(span.required_prefix().map_or(0, <[u8]>::len))
}

fn graph_value_size(value: &GraphValue) -> usize {
    match value {
        GraphValue::Null => 0,
        GraphValue::Boolean(_) => 1,
        GraphValue::Integer(_) | GraphValue::FloatBits(_) | GraphValue::TimestampMicros(_) => 8,
        GraphValue::String(value) => value.len(),
        GraphValue::Bytes(value) => value.len(),
        GraphValue::List(values) => values.iter().fold(0_usize, |size, value| {
            size.saturating_add(graph_value_size(value))
        }),
    }
}
