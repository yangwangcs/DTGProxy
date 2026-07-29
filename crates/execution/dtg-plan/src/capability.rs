use dtg_storage::CapabilityManifest;

pub const CAP_VERTEX_POINT: &str = "read.vertex.point";
pub const CAP_VERTEX_SCAN: &str = "read.vertex.scan";
pub const CAP_TEMPORAL_EXACT: &str = "semantics.temporal.exact";
pub const CAP_NULL_EXACT: &str = "semantics.null.exact";
pub const CAP_DUPLICATE_EXACT: &str = "semantics.duplicate.exact";
pub const CAP_ORDER_EXACT: &str = "semantics.order.exact";
pub const CAP_SNAPSHOT_EXACT: &str = "semantics.snapshot.exact";

pub const EXACT_VERTEX_POINT_CAPABILITIES: [&str; 6] = [
    CAP_VERTEX_POINT,
    CAP_TEMPORAL_EXACT,
    CAP_NULL_EXACT,
    CAP_DUPLICATE_EXACT,
    CAP_ORDER_EXACT,
    CAP_SNAPSHOT_EXACT,
];

pub const EXACT_VERTEX_SCAN_CAPABILITIES: [&str; 6] = [
    CAP_VERTEX_SCAN,
    CAP_TEMPORAL_EXACT,
    CAP_NULL_EXACT,
    CAP_DUPLICATE_EXACT,
    CAP_ORDER_EXACT,
    CAP_SNAPSHOT_EXACT,
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PushdownKind {
    VertexPoint,
    VertexScan,
}

impl PushdownKind {
    pub const fn capability(self) -> &'static str {
        match self {
            Self::VertexPoint => CAP_VERTEX_POINT,
            Self::VertexScan => CAP_VERTEX_SCAN,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SemanticRequirements {
    temporal: bool,
    nulls: bool,
    duplicates: bool,
    order: bool,
    snapshot: bool,
}

impl SemanticRequirements {
    pub const fn exact() -> Self {
        Self {
            temporal: true,
            nulls: true,
            duplicates: true,
            order: true,
            snapshot: true,
        }
    }

    pub const fn temporal(self) -> bool {
        self.temporal
    }

    pub const fn nulls(self) -> bool {
        self.nulls
    }

    pub const fn duplicates(self) -> bool {
        self.duplicates
    }

    pub const fn order(self) -> bool {
        self.order
    }

    pub const fn snapshot(self) -> bool {
        self.snapshot
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PushdownGuarantee {
    temporal: bool,
    nulls: bool,
    duplicates: bool,
    order: bool,
    snapshot: bool,
}

impl PushdownGuarantee {
    pub fn from_manifest(manifest: &CapabilityManifest) -> Self {
        Self {
            temporal: manifest.supports(CAP_TEMPORAL_EXACT),
            nulls: manifest.supports(CAP_NULL_EXACT),
            duplicates: manifest.supports(CAP_DUPLICATE_EXACT),
            order: manifest.supports(CAP_ORDER_EXACT),
            snapshot: manifest.supports(CAP_SNAPSHOT_EXACT),
        }
    }

    pub const fn is_exact(self) -> bool {
        self.temporal && self.nulls && self.duplicates && self.order && self.snapshot
    }

    pub const fn temporal(self) -> bool {
        self.temporal
    }

    pub const fn nulls(self) -> bool {
        self.nulls
    }

    pub const fn duplicates(self) -> bool {
        self.duplicates
    }

    pub const fn order(self) -> bool {
        self.order
    }

    pub const fn snapshot(self) -> bool {
        self.snapshot
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PushdownDecision {
    Exact(PushdownGuarantee),
    ResidualRequired(PushdownGuarantee),
    Unsupported,
}

pub fn decide_pushdown(manifest: &CapabilityManifest, kind: PushdownKind) -> PushdownDecision {
    if !manifest.supports(kind.capability()) {
        return PushdownDecision::Unsupported;
    }
    let guarantee = PushdownGuarantee::from_manifest(manifest);
    if guarantee.is_exact() {
        PushdownDecision::Exact(guarantee)
    } else {
        PushdownDecision::ResidualRequired(guarantee)
    }
}
