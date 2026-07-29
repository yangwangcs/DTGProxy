CREATE TABLE IF NOT EXISTS replica_owner (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    cluster_id BYTEA NOT NULL CHECK (octet_length(cluster_id) = 8),
    graph_id BYTEA NOT NULL CHECK (octet_length(graph_id) = 8),
    shard_id BYTEA NOT NULL CHECK (octet_length(shard_id) = 8),
    placement_epoch BYTEA NOT NULL CHECK (octet_length(placement_epoch) = 8),
    replica_id BYTEA NOT NULL CHECK (octet_length(replica_id) = 8),
    backend_generation BYTEA NOT NULL CHECK (octet_length(backend_generation) = 8),
    backend_class_digest BYTEA NOT NULL CHECK (octet_length(backend_class_digest) = 32),
    provider_kind TEXT NOT NULL,
    contract_version INTEGER NOT NULL CHECK (contract_version > 0),
    layout_version INTEGER NOT NULL CHECK (layout_version > 0),
    capability_digest BYTEA NOT NULL CHECK (octet_length(capability_digest) = 32),
    namespace_id TEXT NOT NULL,
    endpoint_profile_ref TEXT NOT NULL,
    credential_ref TEXT NOT NULL,
    binding_role SMALLINT NOT NULL CHECK (binding_role BETWEEN 1 AND 3),
    binding_digest BYTEA NOT NULL CHECK (octet_length(binding_digest) = 32)
);

CREATE TABLE IF NOT EXISTS current_vertex (
    vertex_id BYTEA PRIMARY KEY CHECK (octet_length(vertex_id) = 16),
    version BYTEA NOT NULL CHECK (octet_length(version) = 8),
    valid_from BIGINT NOT NULL,
    valid_to BIGINT NOT NULL,
    transaction_time BIGINT NOT NULL CHECK (transaction_time >= 0),
    properties BYTEA NOT NULL,
    CHECK (valid_from < valid_to)
);

CREATE TABLE IF NOT EXISTS current_edge (
    edge_id BYTEA PRIMARY KEY CHECK (octet_length(edge_id) = 16),
    source_vertex_id BYTEA NOT NULL CHECK (octet_length(source_vertex_id) = 16),
    target_vertex_id BYTEA NOT NULL CHECK (octet_length(target_vertex_id) = 16),
    edge_type TEXT NOT NULL CHECK (edge_type <> ''),
    version BYTEA NOT NULL CHECK (octet_length(version) = 8),
    valid_from BIGINT NOT NULL,
    valid_to BIGINT NOT NULL,
    transaction_time BIGINT NOT NULL CHECK (transaction_time >= 0),
    properties BYTEA NOT NULL,
    CHECK (valid_from < valid_to)
);

CREATE TABLE IF NOT EXISTS vertex_history (
    vertex_id BYTEA NOT NULL CHECK (octet_length(vertex_id) = 16),
    version BYTEA NOT NULL CHECK (octet_length(version) = 8),
    valid_from BIGINT,
    valid_to BIGINT,
    transaction_time BIGINT NOT NULL CHECK (transaction_time >= 0),
    properties BYTEA,
    tombstone BOOLEAN NOT NULL,
    raft_index BYTEA NOT NULL CHECK (octet_length(raft_index) = 8),
    mutation_ordinal BYTEA NOT NULL CHECK (octet_length(mutation_ordinal) = 8),
    PRIMARY KEY (raft_index, mutation_ordinal)
);

CREATE INDEX IF NOT EXISTS vertex_history_lookup
    ON vertex_history (vertex_id, transaction_time DESC, version DESC);
CREATE INDEX IF NOT EXISTS vertex_history_validity
    ON vertex_history (vertex_id, valid_from, valid_to, transaction_time DESC);

CREATE TABLE IF NOT EXISTS edge_history (
    edge_id BYTEA NOT NULL CHECK (octet_length(edge_id) = 16),
    source_vertex_id BYTEA CHECK (octet_length(source_vertex_id) = 16),
    target_vertex_id BYTEA CHECK (octet_length(target_vertex_id) = 16),
    edge_type TEXT,
    version BYTEA NOT NULL CHECK (octet_length(version) = 8),
    valid_from BIGINT,
    valid_to BIGINT,
    transaction_time BIGINT NOT NULL CHECK (transaction_time >= 0),
    properties BYTEA,
    tombstone BOOLEAN NOT NULL,
    raft_index BYTEA NOT NULL CHECK (octet_length(raft_index) = 8),
    mutation_ordinal BYTEA NOT NULL CHECK (octet_length(mutation_ordinal) = 8),
    PRIMARY KEY (raft_index, mutation_ordinal)
);

CREATE INDEX IF NOT EXISTS edge_history_lookup
    ON edge_history (edge_id, transaction_time DESC, version DESC);
CREATE INDEX IF NOT EXISTS edge_history_outgoing
    ON edge_history (source_vertex_id, transaction_time DESC, edge_id);
CREATE INDEX IF NOT EXISTS edge_history_incoming
    ON edge_history (target_vertex_id, transaction_time DESC, edge_id);
CREATE INDEX IF NOT EXISTS edge_history_validity
    ON edge_history (edge_id, valid_from, valid_to, transaction_time DESC);

CREATE TABLE IF NOT EXISTS adjacency (
    vertex_id BYTEA NOT NULL CHECK (octet_length(vertex_id) = 16),
    edge_id BYTEA NOT NULL CHECK (octet_length(edge_id) = 16),
    peer_vertex_id BYTEA NOT NULL CHECK (octet_length(peer_vertex_id) = 16),
    direction SMALLINT NOT NULL CHECK (direction IN (1, 2)),
    edge_type TEXT NOT NULL,
    valid_from BIGINT NOT NULL,
    valid_to BIGINT NOT NULL,
    transaction_time BIGINT NOT NULL CHECK (transaction_time >= 0),
    version BYTEA NOT NULL CHECK (octet_length(version) = 8),
    PRIMARY KEY (vertex_id, direction, edge_id)
);

CREATE INDEX IF NOT EXISTS adjacency_bounded_expand
    ON adjacency (vertex_id, direction, edge_id);

CREATE TABLE IF NOT EXISTS transaction_state (
    transaction_id BYTEA PRIMARY KEY CHECK (octet_length(transaction_id) = 16),
    state SMALLINT NOT NULL CHECK (state BETWEEN 1 AND 3),
    transaction_time BIGINT NOT NULL CHECK (transaction_time >= 0),
    record_digest BYTEA NOT NULL CHECK (octet_length(record_digest) = 32)
);

CREATE TABLE IF NOT EXISTS replica_meta (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    applied_index BYTEA NOT NULL CHECK (octet_length(applied_index) = 8)
);

CREATE TABLE IF NOT EXISTS replica_metadata (
    name TEXT PRIMARY KEY CHECK (name <> ''),
    value BYTEA NOT NULL
);

CREATE TABLE IF NOT EXISTS replay_identity (
    raft_index BYTEA PRIMARY KEY CHECK (octet_length(raft_index) = 8),
    raft_term BYTEA NOT NULL CHECK (octet_length(raft_term) = 8),
    command_id BYTEA NOT NULL CHECK (octet_length(command_id) = 16),
    mutation_digest BYTEA NOT NULL CHECK (octet_length(mutation_digest) = 32)
);

CREATE TABLE IF NOT EXISTS change_record (
    raft_index BYTEA NOT NULL CHECK (octet_length(raft_index) = 8),
    mutation_ordinal BYTEA NOT NULL CHECK (octet_length(mutation_ordinal) = 8),
    mutation_kind SMALLINT NOT NULL CHECK (mutation_kind BETWEEN 1 AND 6),
    mutation_payload BYTEA NOT NULL,
    PRIMARY KEY (raft_index, mutation_ordinal)
);

CREATE TABLE IF NOT EXISTS snapshot_stage (
    snapshot_id BYTEA NOT NULL CHECK (octet_length(snapshot_id) = 16),
    chunk_ordinal BYTEA NOT NULL CHECK (octet_length(chunk_ordinal) = 8),
    chunk_digest BYTEA NOT NULL CHECK (octet_length(chunk_digest) = 32),
    record_count BIGINT NOT NULL CHECK (record_count > 0),
    PRIMARY KEY (snapshot_id, chunk_ordinal)
);

CREATE TABLE IF NOT EXISTS snapshot_stage_record (
    snapshot_id BYTEA NOT NULL CHECK (octet_length(snapshot_id) = 16),
    chunk_ordinal BYTEA NOT NULL CHECK (octet_length(chunk_ordinal) = 8),
    record_ordinal BYTEA NOT NULL CHECK (octet_length(record_ordinal) = 8),
    record_kind SMALLINT NOT NULL CHECK (record_kind BETWEEN 1 AND 8),
    mutation_payload BYTEA,
    logical_key BYTEA,
    raft_index BYTEA,
    raft_term_or_ordinal BYTEA,
    command_id BYTEA,
    digest BYTEA,
    PRIMARY KEY (snapshot_id, chunk_ordinal, record_ordinal)
);

CREATE TABLE IF NOT EXISTS snapshot_install (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    candidate_binding_digest BYTEA NOT NULL CHECK (octet_length(candidate_binding_digest) = 32),
    snapshot_id BYTEA NOT NULL CHECK (octet_length(snapshot_id) = 16),
    applied_index BYTEA NOT NULL CHECK (octet_length(applied_index) = 8),
    format_version INTEGER NOT NULL CHECK (format_version > 0),
    chunk_count BYTEA NOT NULL CHECK (octet_length(chunk_count) = 8),
    record_count BYTEA NOT NULL CHECK (octet_length(record_count) = 8),
    content_digest BYTEA NOT NULL CHECK (octet_length(content_digest) = 32)
);

CREATE TABLE IF NOT EXISTS snapshot_activation (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    candidate_binding_digest BYTEA NOT NULL CHECK (octet_length(candidate_binding_digest) = 32),
    active_binding_digest BYTEA NOT NULL CHECK (octet_length(active_binding_digest) = 32),
    snapshot_id BYTEA NOT NULL CHECK (octet_length(snapshot_id) = 16),
    applied_index BYTEA NOT NULL CHECK (octet_length(applied_index) = 8),
    format_version INTEGER NOT NULL CHECK (format_version > 0),
    chunk_count BYTEA NOT NULL CHECK (octet_length(chunk_count) = 8),
    record_count BYTEA NOT NULL CHECK (octet_length(record_count) = 8),
    content_digest BYTEA NOT NULL CHECK (octet_length(content_digest) = 32)
);
