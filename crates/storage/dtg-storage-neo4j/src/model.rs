const LABELS: &[&str] = &[
    "DtgOwner",
    "DtgVertex",
    "DtgVersion",
    "DtgTransaction",
    "DtgMetadata",
    "DtgReplay",
    "DtgChange",
    "DtgSnapshotStage",
    "DtgSnapshotInstall",
    "DtgSnapshotActivation",
];

const RELATIONSHIPS: &[&str] = &["DTG_EDGE", "HAS_VERSION"];

const CONSTRAINTS: &[&str] = &[
    "CREATE CONSTRAINT dtg_owner_identity IF NOT EXISTS FOR (owner:DtgOwner) REQUIRE owner.namespace_id IS UNIQUE",
    "CREATE CONSTRAINT dtg_vertex_identity IF NOT EXISTS FOR (vertex:DtgVertex) REQUIRE (vertex.namespace_id, vertex.backend_generation, vertex.vertex_id) IS UNIQUE",
    "CREATE CONSTRAINT dtg_version_identity IF NOT EXISTS FOR (version:DtgVersion) REQUIRE (version.namespace_id, version.backend_generation, version.entity_kind, version.entity_id, version.version, version.transaction_time, version.raft_index, version.ordinal) IS UNIQUE",
    "CREATE CONSTRAINT dtg_transaction_identity IF NOT EXISTS FOR (transaction:DtgTransaction) REQUIRE (transaction.namespace_id, transaction.backend_generation, transaction.transaction_id) IS UNIQUE",
    "CREATE CONSTRAINT dtg_metadata_identity IF NOT EXISTS FOR (metadata:DtgMetadata) REQUIRE (metadata.namespace_id, metadata.backend_generation, metadata.key) IS UNIQUE",
    "CREATE CONSTRAINT dtg_replay_identity IF NOT EXISTS FOR (replay:DtgReplay) REQUIRE (replay.namespace_id, replay.backend_generation, replay.raft_index) IS UNIQUE",
    "CREATE CONSTRAINT dtg_change_identity IF NOT EXISTS FOR (change:DtgChange) REQUIRE (change.namespace_id, change.backend_generation, change.raft_index, change.ordinal) IS UNIQUE",
    "CREATE CONSTRAINT dtg_snapshot_stage_identity IF NOT EXISTS FOR (stage:DtgSnapshotStage) REQUIRE (stage.namespace_id, stage.backend_generation, stage.restore_id, stage.ordinal) IS UNIQUE",
    "CREATE CONSTRAINT dtg_snapshot_install_identity IF NOT EXISTS FOR (stage:DtgSnapshotInstall) REQUIRE (stage.namespace_id, stage.backend_generation) IS UNIQUE",
    "CREATE CONSTRAINT dtg_snapshot_activation_identity IF NOT EXISTS FOR (activation:DtgSnapshotActivation) REQUIRE (activation.namespace_id, activation.backend_generation) IS UNIQUE",
];

const INDEXES: &[&str] = &[
    "CREATE INDEX dtg_vertex_lookup IF NOT EXISTS FOR (vertex:DtgVertex) ON (vertex.namespace_id, vertex.backend_generation, vertex.vertex_id)",
    "CREATE INDEX dtg_version_history IF NOT EXISTS FOR (version:DtgVersion) ON (version.namespace_id, version.backend_generation, version.entity_kind, version.entity_id, version.valid_from)",
    "CREATE INDEX dtg_transaction_lookup IF NOT EXISTS FOR (transaction:DtgTransaction) ON (transaction.namespace_id, transaction.backend_generation, transaction.transaction_id)",
    "CREATE INDEX dtg_metadata_lookup IF NOT EXISTS FOR (metadata:DtgMetadata) ON (metadata.namespace_id, metadata.backend_generation, metadata.key)",
    "CREATE INDEX dtg_replay_lookup IF NOT EXISTS FOR (replay:DtgReplay) ON (replay.namespace_id, replay.backend_generation, replay.raft_index)",
    "CREATE INDEX dtg_change_scan IF NOT EXISTS FOR (change:DtgChange) ON (change.namespace_id, change.backend_generation, change.raft_index, change.ordinal)",
    "CREATE INDEX dtg_edge_lookup IF NOT EXISTS FOR ()-[edge:DTG_EDGE]-() ON (edge.namespace_id, edge.backend_generation, edge.edge_id)",
];

const QUERY_CONTRACTS: &[&str] = &[
    "MATCH (owner:DtgOwner {namespace_id: $namespace_id, backend_generation: $backend_generation}) RETURN owner",
    "MATCH (vertex:DtgVertex {namespace_id: $namespace_id, backend_generation: $backend_generation}) WHERE vertex.vertex_id >= $start_vertex_id RETURN vertex ORDER BY vertex.vertex_id LIMIT $limit",
    "MATCH (version:DtgVersion {namespace_id: $namespace_id, backend_generation: $backend_generation}) WHERE version.entity_kind = $entity_kind AND version.entity_id = $entity_id RETURN version ORDER BY version.valid_from DESC, version.transaction_id DESC LIMIT $limit",
    "MATCH (source:DtgVertex {namespace_id: $namespace_id, backend_generation: $backend_generation})-[edge:DTG_EDGE {namespace_id: $namespace_id, backend_generation: $backend_generation}]->(target:DtgVertex) WHERE source.vertex_id = $vertex_id RETURN edge, source, target ORDER BY edge.edge_id LIMIT $limit",
    "MATCH (transaction:DtgTransaction {namespace_id: $namespace_id, backend_generation: $backend_generation}) WHERE transaction.transaction_id >= $start_transaction_id RETURN transaction ORDER BY transaction.transaction_id LIMIT $limit",
    "MATCH (metadata:DtgMetadata {namespace_id: $namespace_id, backend_generation: $backend_generation}) WHERE metadata.key >= $start_key RETURN metadata ORDER BY metadata.key LIMIT $limit",
    "MATCH (replay:DtgReplay {namespace_id: $namespace_id, backend_generation: $backend_generation}) WHERE replay.raft_index >= $start_raft_index RETURN replay ORDER BY replay.raft_index LIMIT $limit",
    "MATCH (change:DtgChange {namespace_id: $namespace_id, backend_generation: $backend_generation}) WHERE change.raft_index >= $start_raft_index RETURN change ORDER BY change.raft_index, change.ordinal LIMIT $limit",
];

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct NativeModel;

impl NativeModel {
    #[must_use]
    pub const fn v1() -> Self {
        Self
    }

    #[must_use]
    pub const fn labels(self) -> &'static [&'static str] {
        LABELS
    }

    #[must_use]
    pub const fn relationships(self) -> &'static [&'static str] {
        RELATIONSHIPS
    }

    #[must_use]
    pub const fn constraints(self) -> &'static [&'static str] {
        CONSTRAINTS
    }

    #[must_use]
    pub const fn indexes(self) -> &'static [&'static str] {
        INDEXES
    }

    #[must_use]
    pub const fn query_contracts(self) -> &'static [&'static str] {
        QUERY_CONTRACTS
    }
}
