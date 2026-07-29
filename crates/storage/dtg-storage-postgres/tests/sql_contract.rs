#![forbid(unsafe_code)]

const SCHEMA: &str = include_str!("../migrations/0001_replica_schema.sql");

#[test]
fn schema_uses_native_typed_tables_without_a_canonical_mirror() {
    let normalized = SCHEMA.to_ascii_lowercase();
    for table in [
        "replica_owner",
        "current_vertex",
        "current_edge",
        "vertex_history",
        "edge_history",
        "adjacency",
        "transaction_state",
        "replica_meta",
        "replay_identity",
        "change_record",
        "snapshot_stage",
        "snapshot_stage_record",
    ] {
        assert!(
            normalized.contains(&format!("create table {table}"))
                || normalized.contains(&format!("create table if not exists {table}")),
            "native schema is missing typed table {table}"
        );
    }

    for forbidden in ["canonical_kv", "canonical kv", "mirror_kv", "sidecar"] {
        assert!(
            !normalized.contains(forbidden),
            "native schema contains forbidden legacy surface {forbidden}"
        );
    }
}

#[test]
fn schema_models_full_ownership_fence_and_monotonic_replay_state() {
    let normalized = SCHEMA.to_ascii_lowercase();
    for column in [
        "cluster_id",
        "graph_id",
        "shard_id",
        "placement_epoch",
        "replica_id",
        "backend_generation",
        "backend_class_digest",
        "provider_kind",
        "contract_version",
        "layout_version",
        "capability_digest",
        "namespace_id",
        "endpoint_profile_ref",
        "credential_ref",
        "binding_role",
        "binding_digest",
        "applied_index",
        "raft_term",
        "command_id",
        "mutation_digest",
    ] {
        assert!(normalized.contains(column), "schema is missing {column}");
    }
}

#[test]
fn schema_has_temporal_and_adjacency_indexes_for_bounded_reads() {
    let normalized = SCHEMA.to_ascii_lowercase();
    for shape in [
        "valid_from",
        "valid_to",
        "transaction_time",
        "source_vertex_id",
        "target_vertex_id",
        "direction",
        "create index",
    ] {
        assert!(normalized.contains(shape), "schema is missing {shape}");
    }
}

#[test]
fn provider_sql_uses_explicit_isolation_owner_locks_and_applied_index_cas() {
    let apply = include_str!("../src/apply.rs").to_ascii_lowercase();
    let store = include_str!("../src/lib.rs").to_ascii_lowercase();
    let schema = include_str!("../src/schema.rs").to_ascii_lowercase();

    assert!(apply.contains("begin isolation level serializable"));
    assert!(apply.contains("set local synchronous_commit = on"));
    assert!(apply.contains("verify_owner(client, store.binding_ref(), true)"));
    assert!(apply.contains("where singleton = true and applied_index = $2"));
    assert!(schema.contains("for update"));
    assert!(schema.contains("commit"));
    assert!(store.contains("begin isolation level repeatable read read only"));
}

#[test]
fn provider_queries_are_parameterized_and_do_not_materialize_the_namespace() {
    let read_view = include_str!("../src/read_view.rs").to_ascii_lowercase();
    assert!(read_view.contains("vertex_id = $1"));
    assert!(read_view.contains("transaction_time between $2 and $3"));
    assert!(read_view.contains("limit $4") || read_view.contains("limit $5"));
    assert!(!read_view.contains("load_all_entries"));
}

#[test]
fn schema_persists_complete_candidate_install_and_activation_fences() {
    let normalized = SCHEMA.to_ascii_lowercase();
    for table in ["snapshot_install", "snapshot_activation"] {
        assert!(
            normalized.contains(&format!("create table if not exists {table}")),
            "native schema is missing {table}"
        );
    }
    for column in [
        "candidate_binding_digest",
        "active_binding_digest",
        "snapshot_id",
        "applied_index",
        "format_version",
        "chunk_count",
        "record_count",
        "content_digest",
    ] {
        assert!(
            normalized.contains(column),
            "activation schema is missing {column}"
        );
    }
}

#[test]
fn candidate_commit_persists_marker_in_the_restore_transaction() {
    let snapshot = include_str!("../src/snapshot.rs").to_ascii_lowercase();
    assert!(snapshot.contains("bindingrole::candidate"));
    assert!(snapshot.contains("insert into snapshot_install"));
    assert!(snapshot.contains("manifest.chunk_count()"));
    assert!(snapshot.contains("manifest.record_count()"));
    assert!(snapshot.contains("manifest.content_digest()"));
    assert!(snapshot.contains("verify_staged_chunks"));
}

#[test]
fn snapshot_restore_validates_in_database_and_loads_one_raft_batch_at_a_time() {
    let snapshot = include_str!("../src/snapshot.rs").to_ascii_lowercase();
    assert!(snapshot.contains("validate_staged_state_sets"));
    assert!(
        snapshot.contains("select * from supplied_graph except select * from authenticated_graph")
    );
    assert!(snapshot.contains("distinct on (logical_key)"));
    assert!(snapshot.contains("where snapshot_id = $1 and record_kind = 8 and raft_index = $2"));
    assert!(snapshot.contains("verify_staged_change_count"));
    assert!(!snapshot.contains("staged_snapshot_records"));
    assert!(!snapshot.contains("vec<snapshotrecord>"));
}

#[test]
fn activation_is_serializable_durable_locked_and_consumes_the_install_marker() {
    let store = include_str!("../src/lib.rs").to_ascii_lowercase();
    let snapshot = include_str!("../src/snapshot.rs").to_ascii_lowercase();
    assert!(store.contains("impl logicalreplicaactivation for postgresreplicastore"));
    assert!(snapshot.contains("begin isolation level serializable"));
    assert!(snapshot.contains("set local synchronous_commit = on"));
    assert!(snapshot.contains("load_owner(&client, true)"));
    assert!(snapshot.contains("update replica_owner"));
    assert!(snapshot.contains("delete from snapshot_install"));
    assert!(snapshot.contains("insert into snapshot_activation"));
}
