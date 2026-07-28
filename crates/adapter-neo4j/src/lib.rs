#![forbid(unsafe_code)]

use std::collections::{BTreeSet, VecDeque};
use std::fmt;
use std::io::Read;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use adapter_registry::{
    AdapterFactory, AdapterFactoryError, AdapterFactoryFuture, AdapterOpenRequest,
    AdapterRestoreFuture, AdapterRestoreSession, AdapterRestoreSessionFuture,
};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use blake3::Hasher;
use serde::de::{self, DeserializeSeed, IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde_json::{Value, json};
use storage_api::{
    ADAPTER_META_APPLIED_LOG_INDEX_KEY, AdapterCapabilities, AdapterDescriptorV1, AdapterError,
    AdapterFuture, AdjacencyEntry, AdjacencyExpandPage, AdjacencyExpandRequest, ApplyReceipt,
    BackendFamily, CandidateScanPage, CandidateScanRequest, CanonicalBatchScanPage,
    CanonicalBatchScanRequest, CanonicalRestoreSession, CanonicalScanPage, CanonicalScanRequest,
    ChangeScanPage, ChangeScanRequest, CommittedMutationBatch, Durability, KeySpan, KeyValue,
    Keyspace, LogicalKey, LogicalSnapshotAccumulator, LogicalSnapshotChunkV1, LogicalSnapshotError,
    LogicalSnapshotExportRequest, LogicalSnapshotHeaderV1, LogicalSnapshotManifestV1,
    LogicalSnapshotReader, MappingCapabilities, MappingDescriptorV1, MappingFuture,
    MutationOperation, PreparedMappingTransaction, PropertyGatherPage, PropertyGatherRequest,
    PropertyRow, PushdownGuarantee, QueryPageBounds, QueryPrimitiveCapabilities, ReadSnapshot,
    SnapshotCapability, StorageAdapter, TemporalBackendMapping, new_logical_snapshot_id,
};
use temporal_storage::{
    CanonicalGraphEntry, GraphKey, HistoryEntry, ProjectionRecord, decode_canonical_graph_entry,
    decode_graph_key,
};

pub const NEO4J_SCHEMA_VERSION: u16 = 2;
pub const NEO4J_INSTANCE_CONSTRAINT: &str = "CREATE CONSTRAINT dtgproxy_instance_v1 IF NOT EXISTS FOR (n:DTGProxyInstance) REQUIRE n.instance_id IS UNIQUE";
pub const NEO4J_RECORD_CONSTRAINT: &str = "CREATE CONSTRAINT dtgproxy_record_v1 IF NOT EXISTS FOR (n:DTGCanonicalRecord) REQUIRE (n.instance_id, n.keyspace, n.logical_key_hex) IS UNIQUE";
pub const NEO4J_ENDPOINT_CONSTRAINT: &str = "CREATE CONSTRAINT dtgproxy_endpoint_v1 IF NOT EXISTS FOR (n:DTGVertexEndpoint) REQUIRE (n.instance_id, n.graph_hex, n.partition_hex, n.element_hex) IS UNIQUE";
pub const NEO4J_LOG_CONSTRAINT: &str = "CREATE CONSTRAINT dtgproxy_log_v1 IF NOT EXISTS FOR (n:DTGProxyAppliedLog) REQUIRE (n.instance_id, n.log_index_hex) IS UNIQUE";
pub const NEO4J_MUTATION_CONSTRAINT: &str = "CREATE CONSTRAINT dtgproxy_mutation_v1 IF NOT EXISTS FOR (n:DTGProxyMutation) REQUIRE (n.instance_id, n.txn_id_hex, n.sequence) IS UNIQUE";

fn neo4j_mapping_descriptor() -> MappingDescriptorV1 {
    let mut hasher = Hasher::new();
    hasher.update(b"DTGProxy/Neo4jNativeTemporalMapping/2");
    for statement in [
        NEO4J_INSTANCE_CONSTRAINT,
        NEO4J_RECORD_CONSTRAINT,
        NEO4J_ENDPOINT_CONSTRAINT,
        NEO4J_LOG_CONSTRAINT,
        NEO4J_MUTATION_CONSTRAINT,
    ] {
        hasher.update(statement.as_bytes());
    }
    MappingDescriptorV1::new(
        "neo4j-native-temporal",
        "1.1.0",
        BackendFamily::PropertyGraph,
        *hasher.finalize().as_bytes(),
        MappingCapabilities {
            atomic_batch_lifecycle: true,
            deterministic_mapping: true,
            idempotent_replay: true,
            canonical_multi_get: true,
            canonical_ordered_scan: true,
            durable_applied_index: true,
            durability: Durability::Synchronous,
            snapshot: SnapshotCapability::LogicalExport,
            canonical_export: true,
            canonical_restore: true,
            native_temporal_layout: true,
            predicate_pushdown: true,
            adjacency_pushdown: true,
            change_feed: false,
        },
    )
    .expect("static Neo4j Mapping descriptor is valid")
}

pub const APPLY_CYPHER: &str = r#"
MATCH (instance:DTGProxyInstance {instance_id: $instance_id, published: true})
SET instance.__dtgproxy_snapshot_fence = instance.applied_index_hex
WITH instance
OPTIONAL MATCH (existing_log:DTGProxyAppliedLog {instance_id: $instance_id, log_index_hex: $log_index_hex})
WITH instance, existing_log, instance.applied_index_hex AS previous_index_hex,
     (instance.applied_index_hex = $expected_index_hex AND existing_log IS NULL) AS log_can_apply
UNWIND $mutations AS mutation
OPTIONAL MATCH (stored_fingerprint:DTGProxyMutation {
  instance_id: $instance_id,
  txn_id_hex: $txn_id_hex,
  sequence: mutation.sequence
})
WITH instance, existing_log, previous_index_hex, log_can_apply,
     collect({
       mutation: mutation,
       stored_fingerprint_hex: stored_fingerprint.fingerprint_hex
     }) AS candidate_mutations
WITH instance, existing_log, previous_index_hex, log_can_apply, candidate_mutations,
     head([
       candidate IN candidate_mutations
       WHERE candidate.stored_fingerprint_hex IS NOT NULL
         AND candidate.stored_fingerprint_hex <> candidate.mutation.fingerprint_hex
       | candidate.mutation.sequence
     ]) AS conflicting_sequence
UNWIND candidate_mutations AS candidate
WITH instance, existing_log, previous_index_hex, candidate.mutation AS mutation,
     conflicting_sequence,
     (log_can_apply AND conflicting_sequence IS NULL) AS should_apply
OPTIONAL MATCH ()-[existing_edge:DTG_EDGE {
  instance_id: $instance_id,
  edge_key_hex: mutation.logical_key_hex
}]->()
WITH instance, existing_log, previous_index_hex, should_apply, conflicting_sequence, mutation,
     collect(existing_edge) AS existing_edges
FOREACH (_ IN CASE WHEN should_apply THEN [1] ELSE [] END |
  MERGE (record:DTGCanonicalRecord {
    instance_id: $instance_id,
    keyspace: mutation.keyspace,
    logical_key_hex: mutation.logical_key_hex
  })
  SET record:$(mutation.label),
      record.kind = mutation.kind,
      record.present = mutation.present,
      record.value_base64 = mutation.value_base64,
      record.valid_min_micros = mutation.valid_min_micros,
      record.valid_max_micros = mutation.valid_max_micros,
      record.property_equal_tokens = mutation.property_equal_tokens,
      record.graph_hex = mutation.graph_hex,
      record.partition_hex = mutation.partition_hex,
      record.element_hex = mutation.element_hex,
      record.transaction_physical = mutation.transaction_physical,
      record.transaction_logical_hex = mutation.transaction_logical_hex,
      record.segment_hex = mutation.segment_hex
  MERGE (fingerprint:DTGProxyMutation {
    instance_id: $instance_id,
    txn_id_hex: $txn_id_hex,
    sequence: mutation.sequence
  })
  SET fingerprint.fingerprint_hex = mutation.fingerprint_hex
)
FOREACH (_ IN CASE WHEN should_apply AND mutation.kind = 'vertex_current' THEN [1] ELSE [] END |
  MERGE (vertex:DTGVertexEndpoint {
    instance_id: $instance_id,
    graph_hex: mutation.graph_hex,
    partition_hex: mutation.partition_hex,
    element_hex: mutation.element_hex
  })
  SET vertex.current_present = mutation.present,
      vertex.current_projection_base64 = mutation.value_base64
)
FOREACH (edge IN CASE
  WHEN should_apply AND mutation.kind = 'edge_identity' AND NOT mutation.present
  THEN existing_edges ELSE [] END |
  SET edge.present = false
)
FOREACH (_ IN CASE WHEN should_apply AND mutation.kind = 'edge_identity' AND mutation.has_edge_endpoints THEN [1] ELSE [] END |
  MERGE (source:DTGVertexEndpoint {
    instance_id: $instance_id,
    graph_hex: mutation.graph_hex,
    partition_hex: mutation.source_partition_hex,
    element_hex: mutation.source_hex
  })
  MERGE (destination:DTGVertexEndpoint {
    instance_id: $instance_id,
    graph_hex: mutation.graph_hex,
    partition_hex: mutation.destination_partition_hex,
    element_hex: mutation.destination_hex
  })
  MERGE (source)-[edge:DTG_EDGE {
    instance_id: $instance_id,
    edge_key_hex: mutation.logical_key_hex
  }]->(destination)
  SET edge.present = mutation.present,
      edge.edge_type_hex = mutation.edge_type_hex,
      edge.edge_partition_hex = mutation.partition_hex,
      edge.edge_element_hex = mutation.element_hex
)
WITH DISTINCT instance, existing_log, previous_index_hex, should_apply, conflicting_sequence
FOREACH (_ IN CASE WHEN should_apply THEN [1] ELSE [] END |
  MERGE (log:DTGProxyAppliedLog {instance_id: $instance_id, log_index_hex: $log_index_hex})
  SET log.fingerprint_hex = $batch_fingerprint_hex,
      instance.applied_index_hex = $log_index_hex
)
RETURN previous_index_hex, existing_log.fingerprint_hex, should_apply, conflicting_sequence
"#;

const APPLY_EMPTY_CYPHER: &str = r#"
MATCH (instance:DTGProxyInstance {instance_id: $instance_id, published: true})
SET instance.__dtgproxy_snapshot_fence = instance.applied_index_hex
WITH instance
OPTIONAL MATCH (existing_log:DTGProxyAppliedLog {
  instance_id: $instance_id,
  log_index_hex: $log_index_hex
})
WITH instance, existing_log, instance.applied_index_hex AS previous_index_hex,
     (instance.applied_index_hex = $expected_index_hex AND existing_log IS NULL) AS should_apply
FOREACH (_ IN CASE WHEN should_apply THEN [1] ELSE [] END |
  MERGE (log:DTGProxyAppliedLog {instance_id: $instance_id, log_index_hex: $log_index_hex})
  SET log.fingerprint_hex = $batch_fingerprint_hex,
      instance.applied_index_hex = $log_index_hex
)
RETURN previous_index_hex, existing_log.fingerprint_hex, should_apply,
       NULL AS conflicting_sequence
"#;

const RESTORE_CYPHER: &str = r#"
UNWIND $entries AS mutation
MERGE (record:DTGCanonicalRecord {
  instance_id: $instance_id,
  keyspace: mutation.keyspace,
  logical_key_hex: mutation.logical_key_hex
})
SET record:$(mutation.label),
    record.kind = mutation.kind,
    record.present = true,
    record.value_base64 = mutation.value_base64,
    record.valid_min_micros = mutation.valid_min_micros,
    record.valid_max_micros = mutation.valid_max_micros,
    record.property_equal_tokens = mutation.property_equal_tokens,
    record.graph_hex = mutation.graph_hex,
    record.partition_hex = mutation.partition_hex,
    record.element_hex = mutation.element_hex,
    record.transaction_physical = mutation.transaction_physical,
    record.transaction_logical_hex = mutation.transaction_logical_hex,
    record.segment_hex = mutation.segment_hex
FOREACH (_ IN CASE WHEN mutation.kind = 'vertex_current' THEN [1] ELSE [] END |
  MERGE (vertex:DTGVertexEndpoint {
    instance_id: $instance_id,
    graph_hex: mutation.graph_hex,
    partition_hex: mutation.partition_hex,
    element_hex: mutation.element_hex
  })
  SET vertex.current_present = true,
      vertex.current_projection_base64 = mutation.value_base64
)
FOREACH (_ IN CASE WHEN mutation.kind = 'edge_identity' AND mutation.has_edge_endpoints THEN [1] ELSE [] END |
  MERGE (source:DTGVertexEndpoint {
    instance_id: $instance_id,
    graph_hex: mutation.graph_hex,
    partition_hex: mutation.source_partition_hex,
    element_hex: mutation.source_hex
  })
  MERGE (destination:DTGVertexEndpoint {
    instance_id: $instance_id,
    graph_hex: mutation.graph_hex,
    partition_hex: mutation.destination_partition_hex,
    element_hex: mutation.destination_hex
  })
  MERGE (source)-[edge:DTG_EDGE {
    instance_id: $instance_id,
    edge_key_hex: mutation.logical_key_hex
  }]->(destination)
  SET edge.present = true,
      edge.edge_type_hex = mutation.edge_type_hex,
      edge.edge_partition_hex = mutation.partition_hex,
      edge.edge_element_hex = mutation.element_hex
)
RETURN count(record)
"#;

const MULTI_GET_CYPHER: &str = "UNWIND $keys AS requested OPTIONAL MATCH (record:DTGCanonicalRecord {instance_id: $instance_id, keyspace: requested.keyspace, logical_key_hex: requested.logical_key_hex, present: true}) RETURN requested.ordinal, record.value_base64 ORDER BY requested.ordinal";
const SCAN_CYPHER: &str = "MATCH (record:DTGCanonicalRecord {instance_id: $instance_id, keyspace: $keyspace, present: true}) WHERE record.logical_key_hex >= $start_hex AND ($end_hex IS NULL OR record.logical_key_hex < $end_hex) RETURN record.logical_key_hex, record.value_base64 ORDER BY record.logical_key_hex LIMIT $limit";
pub const CANONICAL_BATCH_SCAN_CYPHER: &str = r#"
UNWIND $ranges AS range
CALL (range) {
  MATCH (record:DTGCanonicalRecord {
    instance_id: $instance_id,
    keyspace: range.keyspace,
    present: true
  })
  WHERE record.logical_key_hex >= range.start_hex
    AND (range.end_hex IS NULL OR record.logical_key_hex < range.end_hex)
    AND (range.required_prefix_hex IS NULL OR
         record.logical_key_hex STARTS WITH range.required_prefix_hex)
  WITH range, record
  ORDER BY record.logical_key_hex
  LIMIT range.max_items + 1
  RETURN range.ordinal AS ordinal, record.logical_key_hex AS logical_key_hex,
         record.value_base64 AS value_base64, range.max_bytes AS max_bytes
}
RETURN ordinal, logical_key_hex, value_base64, max_bytes
ORDER BY ordinal, logical_key_hex
"#;
const CANDIDATE_SCAN_CYPHER: &str = "MATCH (record:DTGCanonicalRecord {instance_id: $instance_id, keyspace: $keyspace, present: true}) WHERE record.logical_key_hex >= $start_hex AND ($end_hex IS NULL OR record.logical_key_hex < $end_hex) AND (record.valid_min_micros IS NULL OR record.valid_min_micros <= $valid_time_micros) AND (record.valid_max_micros IS NULL OR $valid_time_micros < record.valid_max_micros) AND all(constraint IN $constraints WHERE constraint.operator <> 'equal' OR record.property_equal_tokens IS NULL OR constraint.token IN record.property_equal_tokens) RETURN record.logical_key_hex, record.value_base64 ORDER BY record.logical_key_hex LIMIT $limit";
const PROPERTY_GATHER_CYPHER: &str = "UNWIND $keys AS requested OPTIONAL MATCH (record:DTGCanonicalRecord {instance_id: $instance_id, keyspace: requested.keyspace, logical_key_hex: requested.logical_key_hex, present: true}) RETURN requested.ordinal, record.value_base64 ORDER BY requested.ordinal";
const ADJACENCY_EXPAND_CYPHER: &str = "UNWIND $spans AS requested MATCH (record:DTGCanonicalRecord {instance_id: $instance_id, keyspace: $keyspace, present: true}) WHERE record.logical_key_hex >= requested.start_hex AND (requested.end_hex IS NULL OR record.logical_key_hex < requested.end_hex) RETURN requested.ordinal, record.logical_key_hex, record.value_base64 ORDER BY requested.ordinal, record.logical_key_hex LIMIT $limit";
const CHANGE_SCAN_CYPHER: &str = "MATCH (record:DTGCanonicalRecord {instance_id: $instance_id, keyspace: $keyspace, present: true}) WHERE record.logical_key_hex >= $start_hex AND ($end_hex IS NULL OR record.logical_key_hex < $end_hex) RETURN record.logical_key_hex, record.value_base64 ORDER BY record.logical_key_hex LIMIT $limit";
const BEGIN_READ_SNAPSHOT_CYPHER: &str = "MATCH (instance:DTGProxyInstance {instance_id: $instance_id, published: true}) SET instance.__dtgproxy_snapshot_fence = instance.applied_index_hex RETURN instance.applied_index_hex";
const EXPORT_CYPHER: &str = "MATCH (instance:DTGProxyInstance {instance_id: $instance_id, published: true}) OPTIONAL MATCH (record:DTGCanonicalRecord {instance_id: $instance_id, present: true}) RETURN instance.applied_index_hex, record.keyspace, record.logical_key_hex, record.value_base64 ORDER BY record.keyspace, record.logical_key_hex";
const DELETE_UNPUBLISHED_NAMESPACE_CYPHER: &str = "MATCH (instance:DTGProxyInstance {instance_id: $instance_id, published: false}) OPTIONAL MATCH (n {instance_id: $instance_id}) WHERE n:DTGProxyInstance OR n:DTGCanonicalRecord OR n:DTGVertexEndpoint OR n:DTGProxyAppliedLog OR n:DTGProxyMutation DETACH DELETE n RETURN count(n)";
const MAX_NEO4J_SCAN_BODY_BYTES: u64 = 64 * 1024 * 1024;
const MAX_NEO4J_SCAN_ROWS: u64 = 1_000_000;
const NEO4J_SCAN_ENVELOPE_BYTES: u64 = 4096;
const NEO4J_SCAN_ROW_OVERHEAD_BYTES: u64 = 64;

pub struct Neo4jAdapterFactory;

impl Neo4jAdapterFactory {
    pub fn open_mapping(
        endpoint: impl Into<String>,
        database: impl Into<String>,
        username: impl Into<String>,
        password: impl Into<String>,
        instance_id: &str,
    ) -> Result<Arc<dyn TemporalBackendMapping>, AdapterError> {
        Self::open_mapping_with_mode(
            endpoint,
            database,
            username,
            password,
            instance_id,
            OpenMode::Serving,
        )
    }

    pub fn open_restore_mapping(
        endpoint: impl Into<String>,
        database: impl Into<String>,
        username: impl Into<String>,
        password: impl Into<String>,
        instance_id: &str,
    ) -> Result<Arc<dyn TemporalBackendMapping>, AdapterError> {
        Self::open_mapping_with_mode(
            endpoint,
            database,
            username,
            password,
            instance_id,
            OpenMode::Restore,
        )
    }

    fn open_mapping_with_mode(
        endpoint: impl Into<String>,
        database: impl Into<String>,
        username: impl Into<String>,
        password: impl Into<String>,
        instance_id: &str,
        mode: OpenMode,
    ) -> Result<Arc<dyn TemporalBackendMapping>, AdapterError> {
        let configuration = Neo4jConfiguration {
            endpoint: endpoint.into().trim_end_matches('/').to_owned(),
            database: database.into(),
            username: username.into(),
            password: password.into(),
            timeout: Duration::from_secs(30),
        };
        if !(configuration.endpoint.starts_with("http://")
            || configuration.endpoint.starts_with("https://"))
            || !valid_identifier(&configuration.database)
        {
            return Err(AdapterError::Backend(
                "invalid Neo4j Mapping connection configuration".into(),
            ));
        }
        Ok(Arc::new(Neo4jAdapter::connect(
            configuration,
            instance_id,
            mode,
        )?))
    }
}

impl AdapterFactory for Neo4jAdapterFactory {
    fn provider_name(&self) -> &str {
        "neo4j"
    }

    fn mapping_descriptor(&self) -> Option<MappingDescriptorV1> {
        Some(neo4j_mapping_descriptor())
    }

    fn open<'a>(&'a self, request: &'a AdapterOpenRequest) -> AdapterFactoryFuture<'a> {
        Box::pin(async move {
            let configuration = Neo4jConfiguration::from_request(request)?;
            let adapter =
                Neo4jAdapter::connect(configuration, request.instance_id(), OpenMode::Serving)
                    .map_err(factory_error)?;
            Ok(Arc::new(adapter) as Arc<dyn StorageAdapter>)
        })
    }

    fn begin_restore<'a>(
        &'a self,
        request: &'a AdapterOpenRequest,
        header: LogicalSnapshotHeaderV1,
    ) -> AdapterRestoreSessionFuture<'a> {
        Box::pin(async move {
            let configuration = Neo4jConfiguration::from_request(request)?;
            let adapter =
                Neo4jAdapter::connect(configuration, request.instance_id(), OpenMode::Restore)
                    .map_err(factory_error)?;
            Ok(Box::new(Neo4jRestoreSession {
                adapter: Some(adapter),
                accumulator: LogicalSnapshotAccumulator::new(header.clone()),
                header,
                last_chunk: None,
                saw_applied_index: false,
                published: false,
            }) as Box<dyn AdapterRestoreSession + 'a>)
        })
    }
}

#[derive(Clone)]
struct Neo4jConfiguration {
    endpoint: String,
    database: String,
    username: String,
    password: String,
    timeout: Duration,
}

impl Neo4jConfiguration {
    fn from_request(request: &AdapterOpenRequest) -> Result<Self, AdapterFactoryError> {
        let endpoint = request
            .parameter("endpoint")
            .ok_or_else(|| AdapterFactoryError::new("Neo4j Adapter requires endpoint"))?
            .trim_end_matches('/')
            .to_owned();
        if !(endpoint.starts_with("http://") || endpoint.starts_with("https://")) {
            return Err(AdapterFactoryError::new(
                "Neo4j endpoint must use http:// or https://",
            ));
        }
        let database = request.parameter("database").unwrap_or("neo4j").to_owned();
        if !valid_identifier(&database) {
            return Err(AdapterFactoryError::new("invalid Neo4j database name"));
        }
        let username = request.parameter("username").unwrap_or("neo4j").to_owned();
        let password = request
            .secret("password")
            .ok_or_else(|| AdapterFactoryError::new("Neo4j Adapter requires secret password"))?
            .expose()
            .to_owned();
        let timeout_seconds = request
            .parameter("timeout_seconds")
            .unwrap_or("30")
            .parse::<u64>()
            .map_err(|_| AdapterFactoryError::new("invalid Neo4j timeout_seconds"))?;
        if timeout_seconds == 0 || timeout_seconds > 300 {
            return Err(AdapterFactoryError::new(
                "Neo4j timeout_seconds must be in 1..=300",
            ));
        }
        Ok(Self {
            endpoint,
            database,
            username,
            password,
            timeout: Duration::from_secs(timeout_seconds),
        })
    }
}

const fn valid_identifier(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.is_empty() || bytes.len() > 63 {
        return false;
    }
    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        if !(byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-' || byte == b'.') {
            return false;
        }
        index += 1;
    }
    true
}

#[derive(Clone, Copy)]
enum OpenMode {
    Serving,
    Restore,
}

pub struct Neo4jAdapter {
    client: QueryApiClient,
    instance_id: String,
    apply_guard: Mutex<()>,
}

impl Neo4jAdapter {
    fn connect(
        configuration: Neo4jConfiguration,
        instance_id: &str,
        mode: OpenMode,
    ) -> Result<Self, AdapterError> {
        if instance_id.is_empty() || instance_id.len() > 255 {
            return Err(AdapterError::Backend("invalid Neo4j instance ID".into()));
        }
        let client = QueryApiClient::new(configuration)?;
        let mapping = neo4j_mapping_descriptor();
        for statement in [
            NEO4J_INSTANCE_CONSTRAINT,
            NEO4J_RECORD_CONSTRAINT,
            NEO4J_ENDPOINT_CONSTRAINT,
            NEO4J_LOG_CONSTRAINT,
            NEO4J_MUTATION_CONSTRAINT,
        ] {
            client.execute(statement, json!({}))?;
        }
        let rows = client.execute(
            "OPTIONAL MATCH (instance:DTGProxyInstance {instance_id: $instance_id}) RETURN instance.published, instance.applied_index_hex, instance.schema_version, instance.mapping_name, instance.mapping_version, instance.mapping_fingerprint_hex",
            json!({"instance_id": instance_id}),
        )?;
        let existing = rows.first();
        if let Some(row) = existing.filter(|row| !existing_instance_missing(Some(row))) {
            validate_instance_metadata(row, &mapping)?;
        }
        match (mode, existing) {
            (OpenMode::Serving, Some(row))
                if row.first().and_then(Value::as_bool) == Some(true) => {}
            (OpenMode::Serving, _) if existing_instance_missing(existing) => {
                client.execute(
                    "CREATE (:DTGProxyInstance {instance_id: $instance_id, schema_version: $schema_version, mapping_name: $mapping_name, mapping_version: $mapping_version, mapping_fingerprint_hex: $mapping_fingerprint_hex, applied_index_hex: $zero, published: true})",
                    json!({"instance_id": instance_id, "schema_version": NEO4J_SCHEMA_VERSION, "mapping_name": mapping.name(), "mapping_version": mapping.version(), "mapping_fingerprint_hex": hex(&mapping.schema_fingerprint()), "zero": u64_hex(0)}),
                )?;
            }
            (OpenMode::Serving, _) => {
                return Err(AdapterError::Backend(
                    "Neo4j Adapter instance is an unpublished restore target".into(),
                ));
            }
            (OpenMode::Restore, _) if existing_instance_missing(existing) => {
                client.execute(
                    "CREATE (:DTGProxyInstance {instance_id: $instance_id, schema_version: $schema_version, mapping_name: $mapping_name, mapping_version: $mapping_version, mapping_fingerprint_hex: $mapping_fingerprint_hex, applied_index_hex: $zero, published: false})",
                    json!({"instance_id": instance_id, "schema_version": NEO4J_SCHEMA_VERSION, "mapping_name": mapping.name(), "mapping_version": mapping.version(), "mapping_fingerprint_hex": hex(&mapping.schema_fingerprint()), "zero": u64_hex(0)}),
                )?;
            }
            (OpenMode::Restore, Some(row))
                if row.first().and_then(Value::as_bool) == Some(false) =>
            {
                client.execute(
                    "MATCH (n {instance_id: $instance_id}) WHERE n:DTGCanonicalRecord OR n:DTGVertexEndpoint OR n:DTGProxyAppliedLog OR n:DTGProxyMutation DETACH DELETE n RETURN count(n)",
                    json!({"instance_id": instance_id}),
                )?;
            }
            (OpenMode::Restore, _) => {
                return Err(AdapterError::Backend(
                    "Neo4j restore target already exists and is published".into(),
                ));
            }
        }
        Ok(Self {
            client,
            instance_id: instance_id.to_owned(),
            apply_guard: Mutex::new(()),
        })
    }

    #[must_use]
    pub const fn capabilities(&self) -> AdapterCapabilities {
        AdapterCapabilities {
            local_atomic_batch: true,
            idempotent_apply: true,
            consistent_multi_get: true,
            ordered_scan: true,
            durable_applied_index: true,
            durability: Durability::Synchronous,
            snapshot: SnapshotCapability::LogicalExport,
            logical_export: true,
            logical_restore: true,
            predicate_pushdown: true,
            adjacency_pushdown: true,
            change_feed: false,
        }
    }

    fn apply(&self, batch: CommittedMutationBatch) -> Result<ApplyReceipt, AdapterError> {
        let _guard = self
            .apply_guard
            .lock()
            .map_err(|_| AdapterError::LockPoisoned)?;
        validate_batch(&batch)?;
        let mutations = batch
            .mutations
            .iter()
            .map(|mutation| {
                native_mutation_json(
                    mutation.sequence,
                    mutation.fingerprint(),
                    &mutation.operation,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let statement = if mutations.is_empty() {
            APPLY_EMPTY_CYPHER
        } else {
            APPLY_CYPHER
        };
        let rows = self.client.execute(
            statement,
            json!({
                "instance_id": self.instance_id,
                "expected_index_hex": u64_hex(batch.log_index.saturating_sub(1)),
                "log_index_hex": u64_hex(batch.log_index),
                "txn_id_hex": u128_hex(batch.txn_id),
                "batch_fingerprint_hex": u64_hex(batch.fingerprint()),
                "mutations": mutations,
            }),
        )?;
        let row = rows.first().ok_or_else(|| {
            AdapterError::Backend("Neo4j apply did not find a published Adapter instance".into())
        })?;
        let previous = parse_u64_hex(value_string(row.first(), "previous index")?)?;
        let existing_fingerprint = row.get(1).and_then(Value::as_str);
        let applied = row.get(2).and_then(Value::as_bool).unwrap_or(false);
        if let Some(sequence) = row
            .get(3)
            .and_then(Value::as_u64)
            .and_then(|value| u32::try_from(value).ok())
        {
            return Err(AdapterError::MutationReplayMismatch {
                txn_id: batch.txn_id,
                sequence,
            });
        }
        if applied {
            return Ok(ApplyReceipt {
                applied_log_index: batch.log_index,
                duplicate: false,
            });
        }
        if batch.log_index <= previous {
            return match existing_fingerprint {
                Some(fingerprint) if fingerprint == u64_hex(batch.fingerprint()) => {
                    Ok(ApplyReceipt {
                        applied_log_index: previous,
                        duplicate: true,
                    })
                }
                _ => Err(AdapterError::CommittedLogReplayMismatch {
                    log_index: batch.log_index,
                }),
            };
        }
        Err(AdapterError::NonContiguousLogIndex {
            expected: previous.saturating_add(1),
            actual: batch.log_index,
        })
    }

    fn validate_durable_batch(&self, batch: &CommittedMutationBatch) -> Result<(), AdapterError> {
        let rows = self.client.execute(
            "MATCH (instance:DTGProxyInstance {instance_id: $instance_id, published: true}) OPTIONAL MATCH (log:DTGProxyAppliedLog {instance_id: $instance_id, log_index_hex: $log_index_hex}) RETURN instance.applied_index_hex, log.fingerprint_hex",
            json!({
                "instance_id": self.instance_id,
                "log_index_hex": u64_hex(batch.log_index),
            }),
        )?;
        let row = rows.first().ok_or_else(|| {
            AdapterError::Backend("Neo4j Mapping instance is not published".into())
        })?;
        let applied = parse_u64_hex(value_string(row.first(), "applied index")?)?;
        if batch.log_index <= applied {
            let stored = row.get(1).and_then(Value::as_str);
            let fingerprint = u64_hex(batch.fingerprint());
            if stored != Some(fingerprint.as_str()) {
                return Err(AdapterError::CommittedLogReplayMismatch {
                    log_index: batch.log_index,
                });
            }
        } else if batch.log_index != applied.saturating_add(1) {
            return Err(AdapterError::NonContiguousLogIndex {
                expected: applied.saturating_add(1),
                actual: batch.log_index,
            });
        }

        if !batch.mutations.is_empty() {
            let mutations = batch
                .mutations
                .iter()
                .map(|mutation| {
                    json!({
                        "sequence": mutation.sequence,
                        "fingerprint_hex": u64_hex(mutation.fingerprint()),
                    })
                })
                .collect::<Vec<_>>();
            let rows = self.client.execute(
                "UNWIND $mutations AS mutation OPTIONAL MATCH (stored:DTGProxyMutation {instance_id: $instance_id, txn_id_hex: $txn_id_hex, sequence: mutation.sequence}) RETURN mutation.sequence, mutation.fingerprint_hex, stored.fingerprint_hex ORDER BY mutation.sequence",
                json!({
                    "instance_id": self.instance_id,
                    "txn_id_hex": u128_hex(batch.txn_id),
                    "mutations": mutations,
                }),
            )?;
            for row in rows {
                let sequence = row
                    .first()
                    .and_then(Value::as_u64)
                    .and_then(|value| u32::try_from(value).ok())
                    .ok_or_else(|| {
                        AdapterError::Backend(
                            "Neo4j replay validation returned an invalid sequence".into(),
                        )
                    })?;
                let expected = row.get(1).and_then(Value::as_str).ok_or_else(|| {
                    AdapterError::Backend(
                        "Neo4j replay validation omitted the requested fingerprint".into(),
                    )
                })?;
                if let Some(stored) = row.get(2).and_then(Value::as_str)
                    && stored != expected
                {
                    return Err(AdapterError::MutationReplayMismatch {
                        txn_id: batch.txn_id,
                        sequence,
                    });
                }
            }
        }
        Ok(())
    }

    fn multi_get_values(&self, keys: &[LogicalKey]) -> Result<Vec<Option<Vec<u8>>>, AdapterError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let requested = keys
            .iter()
            .enumerate()
            .map(|(ordinal, key)| {
                json!({"ordinal": ordinal, "keyspace": key.keyspace().tag(), "logical_key_hex": hex(key.as_bytes())})
            })
            .collect::<Vec<_>>();
        let rows = self.client.execute(
            MULTI_GET_CYPHER,
            json!({"instance_id": self.instance_id, "keys": requested}),
        )?;
        if rows.len() != keys.len() {
            return Err(AdapterError::Backend(
                "Neo4j multi-get returned an unexpected row count".into(),
            ));
        }
        rows.iter()
            .map(|row| decode_optional_base64(row.get(1)))
            .collect()
    }

    fn scan_values(&self, span: &KeySpan) -> Result<Vec<KeyValue>, AdapterError> {
        let limit = span.limit().unwrap_or(usize::MAX.min(i64::MAX as usize));
        self.client.execute_scan(
            SCAN_CYPHER,
            json!({
                "instance_id": self.instance_id,
                "keyspace": span.keyspace().tag(),
                "start_hex": hex(span.start()),
                "end_hex": span.end().map(hex),
                "limit": limit,
            }),
            span,
        )
    }

    fn export_entries(&self) -> Result<(u64, Vec<KeyValue>), AdapterError> {
        let rows = self
            .client
            .execute(EXPORT_CYPHER, json!({"instance_id": self.instance_id}))?;
        let first = rows.first().ok_or_else(|| {
            AdapterError::Backend("Neo4j export did not find Adapter instance".into())
        })?;
        let applied_index = parse_u64_hex(value_string(first.first(), "applied index")?)?;
        let mut entries = Vec::new();
        for row in rows {
            let Some(keyspace) = row.get(1).and_then(Value::as_u64) else {
                continue;
            };
            let keyspace = decode_keyspace(
                u8::try_from(keyspace)
                    .map_err(|_| AdapterError::Backend("Neo4j keyspace tag exceeds u8".into()))?,
            )?;
            entries.push(KeyValue::new(
                LogicalKey::in_keyspace(
                    keyspace,
                    decode_hex(value_string(row.get(2), "logical key")?)?,
                ),
                decode_base64(row.get(3).ok_or_else(|| {
                    AdapterError::Backend("Neo4j export row is missing value".into())
                })?)?,
            ));
        }
        entries.push(KeyValue::new(
            LogicalKey::in_keyspace(Keyspace::Meta, ADAPTER_META_APPLIED_LOG_INDEX_KEY.to_vec()),
            applied_index.to_be_bytes().to_vec(),
        ));
        entries.sort_by(|left, right| left.key().cmp(right.key()));
        Ok((applied_index, entries))
    }

    fn restore_entries(
        &self,
        entries: &[KeyValue],
        expected_index: u64,
    ) -> Result<bool, AdapterError> {
        let mut saw_applied_index = false;
        let mut values = Vec::new();
        for entry in entries {
            if entry.key().keyspace() == Keyspace::Meta
                && entry.key().as_bytes() == ADAPTER_META_APPLIED_LOG_INDEX_KEY
            {
                if entry.value() != expected_index.to_be_bytes() {
                    return Err(AdapterError::Backend(
                        "Neo4j restore applied-index record mismatch".into(),
                    ));
                }
                saw_applied_index = true;
                continue;
            }
            values.push(native_record_json(entry.key(), Some(entry.value()), true)?);
        }
        if !values.is_empty() {
            self.client.execute(
                RESTORE_CYPHER,
                json!({"instance_id": self.instance_id, "entries": values}),
            )?;
        }
        Ok(saw_applied_index)
    }

    fn publish_restore(&self, applied_index: u64) -> Result<(), AdapterError> {
        let rows = self.client.execute(
            "MATCH (instance:DTGProxyInstance {instance_id: $instance_id, published: false}) SET instance.applied_index_hex = $applied_index_hex, instance.published = true RETURN instance.applied_index_hex",
            json!({"instance_id": self.instance_id, "applied_index_hex": u64_hex(applied_index)}),
        )?;
        if rows.len() != 1 {
            return Err(AdapterError::Backend(
                "Neo4j restore target could not be published".into(),
            ));
        }
        Ok(())
    }

    fn delete_namespace(&self) -> Result<(), AdapterError> {
        self.client.execute(
            DELETE_UNPUBLISHED_NAMESPACE_CYPHER,
            json!({"instance_id": self.instance_id}),
        )?;
        Ok(())
    }

    fn begin_query_snapshot(&self) -> Result<Neo4jReadSnapshot<'_>, AdapterError> {
        let (transaction, rows) = self.client.begin_transaction(
            BEGIN_READ_SNAPSHOT_CYPHER,
            json!({"instance_id": self.instance_id}),
        )?;
        let applied_log_index = parse_u64_hex(value_string(
            rows.first().and_then(|row| row.first()),
            "applied index",
        )?)?;
        Ok(Neo4jReadSnapshot {
            transaction: Mutex::new(transaction),
            instance_id: self.instance_id.clone(),
            applied_log_index,
        })
    }
}

impl StorageAdapter for Neo4jAdapter {
    fn descriptor(&self) -> AdapterDescriptorV1 {
        AdapterDescriptorV1::new(
            "neo4j-query-api",
            env!("CARGO_PKG_VERSION"),
            BackendFamily::PropertyGraph,
            StorageAdapter::capabilities(self),
        )
    }

    fn capabilities(&self) -> AdapterCapabilities {
        Neo4jAdapter::capabilities(self)
    }

    fn query_primitive_capabilities(&self) -> QueryPrimitiveCapabilities {
        QueryPrimitiveCapabilities::new(
            PushdownGuarantee::Candidate,
            PushdownGuarantee::Candidate,
            PushdownGuarantee::Candidate,
            PushdownGuarantee::Candidate,
        )
    }

    fn mapping_descriptor(&self) -> Option<MappingDescriptorV1> {
        Some(neo4j_mapping_descriptor())
    }

    fn apply_committed<'a>(
        &'a self,
        batch: CommittedMutationBatch,
    ) -> AdapterFuture<'a, ApplyReceipt> {
        Box::pin(async move {
            self.validate_durable_batch(&batch)?;
            self.apply(batch)
        })
    }

    fn multi_get<'a>(&'a self, keys: &'a [LogicalKey]) -> AdapterFuture<'a, Vec<Option<Vec<u8>>>> {
        Box::pin(async move { self.multi_get_values(keys) })
    }

    fn scan<'a>(&'a self, span: &'a KeySpan) -> AdapterFuture<'a, Vec<KeyValue>> {
        Box::pin(async move { self.scan_values(span) })
    }

    fn scan_candidates<'a>(
        &'a self,
        request: &'a CandidateScanRequest,
    ) -> AdapterFuture<'a, CandidateScanPage> {
        Box::pin(async move { self.begin_query_snapshot()?.scan_candidates_page(request) })
    }

    fn gather_properties<'a>(
        &'a self,
        request: &'a PropertyGatherRequest,
    ) -> AdapterFuture<'a, PropertyGatherPage> {
        Box::pin(async move { self.begin_query_snapshot()?.gather_properties_page(request) })
    }

    fn expand_adjacency<'a>(
        &'a self,
        request: &'a AdjacencyExpandRequest,
    ) -> AdapterFuture<'a, AdjacencyExpandPage> {
        Box::pin(async move { self.begin_query_snapshot()?.expand_adjacency_page(request) })
    }

    fn scan_changes<'a>(
        &'a self,
        request: &'a ChangeScanRequest,
    ) -> AdapterFuture<'a, ChangeScanPage> {
        Box::pin(async move { self.begin_query_snapshot()?.scan_changes_page(request) })
    }

    fn begin_read_snapshot<'a>(&'a self) -> AdapterFuture<'a, Box<dyn ReadSnapshot + 'a>> {
        Box::pin(
            async move { Ok(Box::new(self.begin_query_snapshot()?) as Box<dyn ReadSnapshot + 'a>) },
        )
    }

    fn begin_logical_export<'a>(
        &'a self,
        request: LogicalSnapshotExportRequest,
    ) -> AdapterFuture<'a, Box<dyn LogicalSnapshotReader + 'a>> {
        Box::pin(async move {
            let (applied_index, entries) = self.export_entries()?;
            Ok(Box::new(Neo4jLogicalSnapshotReader::new(
                applied_index,
                entries,
                request,
            )?) as Box<dyn LogicalSnapshotReader + 'a>)
        })
    }

    fn applied_log_index(&self) -> Result<u64, AdapterError> {
        let rows = self.client.execute(
            "MATCH (instance:DTGProxyInstance {instance_id: $instance_id, published: true}) RETURN instance.applied_index_hex",
            json!({"instance_id": self.instance_id}),
        )?;
        parse_u64_hex(value_string(
            rows.first().and_then(|row| row.first()),
            "applied index",
        )?)
    }
}

struct Neo4jPreparedMapping<'a> {
    adapter: &'a Neo4jAdapter,
    batch: Option<CommittedMutationBatch>,
    applied: bool,
}

impl PreparedMappingTransaction for Neo4jPreparedMapping<'_> {
    fn apply<'a>(&'a mut self) -> MappingFuture<'a, ()> {
        Box::pin(async move {
            if self.applied {
                return Err(AdapterError::Backend(
                    "Neo4j Mapping transaction was applied twice".into(),
                ));
            }
            if self.batch.is_none() {
                return Err(AdapterError::Backend(
                    "Neo4j Mapping transaction is already finished".into(),
                ));
            }
            self.applied = true;
            Ok(())
        })
    }

    fn commit<'a>(&'a mut self) -> MappingFuture<'a, ApplyReceipt> {
        Box::pin(async move {
            if !self.applied {
                return Err(AdapterError::Backend(
                    "Neo4j Mapping transaction must be applied before commit".into(),
                ));
            }
            let batch = self.batch.take().ok_or_else(|| {
                AdapterError::Backend("Neo4j Mapping transaction is already finished".into())
            })?;
            self.adapter.apply(batch)
        })
    }

    fn abort<'a>(mut self: Box<Self>) -> MappingFuture<'a, ()>
    where
        Self: 'a,
    {
        Box::pin(async move {
            self.batch.take();
            Ok(())
        })
    }
}

struct Neo4jCanonicalRestore<'a> {
    adapter: &'a Neo4jAdapter,
    header: LogicalSnapshotHeaderV1,
    accumulator: LogicalSnapshotAccumulator,
    last_chunk: Option<(u64, [u8; 32])>,
    saw_applied_index: bool,
    finished: bool,
}

impl CanonicalRestoreSession for Neo4jCanonicalRestore<'_> {
    fn write_chunk<'a>(&'a mut self, chunk: LogicalSnapshotChunkV1) -> MappingFuture<'a, ()> {
        Box::pin(async move {
            if self.finished {
                return Err(AdapterError::Backend(
                    "Neo4j canonical restore is finished".into(),
                ));
            }
            if self.last_chunk == Some((chunk.ordinal(), chunk.digest())) {
                return Ok(());
            }
            let mut accumulator = self.accumulator.clone();
            accumulator.observe(&chunk)?;
            let saw = self
                .adapter
                .restore_entries(chunk.entries(), self.header.applied_log_index())?;
            self.saw_applied_index |= saw;
            self.last_chunk = Some((chunk.ordinal(), chunk.digest()));
            self.accumulator = accumulator;
            Ok(())
        })
    }

    fn commit<'a>(&'a mut self, manifest: LogicalSnapshotManifestV1) -> MappingFuture<'a, ()> {
        Box::pin(async move {
            if self.finished {
                return Err(AdapterError::Backend(
                    "Neo4j canonical restore is finished".into(),
                ));
            }
            self.accumulator.clone().verify(&manifest)?;
            if self.header.applied_log_index() != 0 && !self.saw_applied_index {
                return Err(AdapterError::Backend(
                    "Neo4j snapshot is missing its applied-index record".into(),
                ));
            }
            self.adapter
                .publish_restore(self.header.applied_log_index())?;
            self.finished = true;
            Ok(())
        })
    }

    fn abort<'a>(mut self: Box<Self>) -> MappingFuture<'a, ()>
    where
        Self: 'a,
    {
        Box::pin(async move {
            self.adapter.delete_namespace()?;
            self.finished = true;
            Ok(())
        })
    }
}

impl Drop for Neo4jCanonicalRestore<'_> {
    fn drop(&mut self) {
        if !self.finished {
            let _ = self.adapter.delete_namespace();
        }
    }
}

impl TemporalBackendMapping for Neo4jAdapter {
    fn describe_schema(&self) -> MappingDescriptorV1 {
        neo4j_mapping_descriptor()
    }

    fn validate_mapping(&self) -> Result<(), AdapterError> {
        StorageAdapter::applied_log_index(self).map(|_| ())
    }

    fn prepare<'a>(
        &'a self,
        batch: CommittedMutationBatch,
    ) -> MappingFuture<'a, Box<dyn PreparedMappingTransaction + 'a>> {
        Box::pin(async move {
            self.validate_durable_batch(&batch)?;
            validate_batch(&batch)?;
            Ok(Box::new(Neo4jPreparedMapping {
                adapter: self,
                batch: Some(batch),
                applied: false,
            }) as Box<dyn PreparedMappingTransaction + 'a>)
        })
    }

    fn multi_get<'a>(&'a self, keys: &'a [LogicalKey]) -> MappingFuture<'a, Vec<Option<Vec<u8>>>> {
        Box::pin(async move { self.multi_get_values(keys) })
    }

    fn scan<'a>(&'a self, span: &'a KeySpan) -> MappingFuture<'a, Vec<KeyValue>> {
        Box::pin(async move { self.scan_values(span) })
    }

    fn begin_read_snapshot<'a>(&'a self) -> MappingFuture<'a, Box<dyn ReadSnapshot + 'a>> {
        <Self as StorageAdapter>::begin_read_snapshot(self)
    }

    fn export_canonical<'a>(
        &'a self,
        request: LogicalSnapshotExportRequest,
    ) -> MappingFuture<'a, Box<dyn LogicalSnapshotReader + 'a>> {
        Box::pin(async move {
            let (applied_index, entries) = self.export_entries()?;
            Ok(Box::new(Neo4jLogicalSnapshotReader::new(
                applied_index,
                entries,
                request,
            )?) as Box<dyn LogicalSnapshotReader + 'a>)
        })
    }

    fn restore_canonical<'a>(
        &'a self,
        header: LogicalSnapshotHeaderV1,
    ) -> MappingFuture<'a, Box<dyn CanonicalRestoreSession + 'a>> {
        Box::pin(async move {
            let rows = self.client.execute(
                "MATCH (instance:DTGProxyInstance {instance_id: $instance_id}) RETURN instance.published",
                json!({"instance_id": self.instance_id}),
            )?;
            if rows
                .first()
                .and_then(|row| row.first())
                .and_then(Value::as_bool)
                != Some(false)
            {
                return Err(AdapterError::Backend(
                    "Neo4j canonical restore requires an unpublished target".into(),
                ));
            }
            Ok(Box::new(Neo4jCanonicalRestore {
                adapter: self,
                accumulator: LogicalSnapshotAccumulator::new(header.clone()),
                header,
                last_chunk: None,
                saw_applied_index: false,
                finished: false,
            }) as Box<dyn CanonicalRestoreSession + 'a>)
        })
    }

    fn applied_log_index(&self) -> Result<u64, AdapterError> {
        StorageAdapter::applied_log_index(self)
    }
}

struct QueryApiClient {
    agent: ureq::Agent,
    url: String,
    authorization: String,
}

impl QueryApiClient {
    fn new(configuration: Neo4jConfiguration) -> Result<Self, AdapterError> {
        let credentials = BASE64.encode(format!(
            "{}:{}",
            configuration.username, configuration.password
        ));
        Ok(Self {
            agent: ureq::AgentBuilder::new()
                .timeout(configuration.timeout)
                .build(),
            url: format!(
                "{}/db/{}/query/v2",
                configuration.endpoint, configuration.database
            ),
            authorization: format!("Basic {credentials}"),
        })
    }

    fn execute(&self, statement: &str, parameters: Value) -> Result<Vec<Vec<Value>>, AdapterError> {
        let response = self.post(&self.url, None, statement, parameters);
        let response = match response {
            Ok(response) => response,
            Err(error) => match *error {
                ureq::Error::Status(_, response) => {
                    let status = response.status();
                    let body = response.into_string().unwrap_or_default();
                    return Err(AdapterError::Backend(format!(
                        "Neo4j Query API returned HTTP {status}: {body}"
                    )));
                }
                ureq::Error::Transport(error) => {
                    return Err(AdapterError::Backend(format!(
                        "Neo4j Query API transport failed: {error}"
                    )));
                }
            },
        };
        let body: Value = response.into_json().map_err(|error| {
            AdapterError::Backend(format!("invalid Neo4j Query API JSON: {error}"))
        })?;
        if let Some(errors) = body.get("errors").and_then(Value::as_array)
            && !errors.is_empty()
        {
            return Err(AdapterError::Backend(format!(
                "Neo4j Query API error: {}",
                errors[0]
            )));
        }
        body.pointer("/data/values")
            .and_then(Value::as_array)
            .map(|rows| {
                rows.iter()
                    .map(|row| {
                        row.as_array().cloned().ok_or_else(|| {
                            AdapterError::Backend("Neo4j result row is not an array".into())
                        })
                    })
                    .collect()
            })
            .transpose()
            .map(Option::unwrap_or_default)
    }

    fn begin_transaction(
        &self,
        statement: &str,
        parameters: Value,
    ) -> Result<(QueryApiTransaction<'_>, Vec<Vec<Value>>), AdapterError> {
        let url = format!("{}/tx", self.url);
        let response = self.post(&url, None, statement, parameters);
        let response = match response {
            Ok(response) => response,
            Err(error) => match *error {
                ureq::Error::Status(_, response) => {
                    let status = response.status();
                    let body = response.into_string().unwrap_or_default();
                    return Err(AdapterError::Backend(format!(
                        "Neo4j Query API returned HTTP {status}: {body}"
                    )));
                }
                ureq::Error::Transport(error) => {
                    return Err(AdapterError::Backend(format!(
                        "Neo4j Query API transport failed: {error}"
                    )));
                }
            },
        };
        let affinity = response.header("neo4j-cluster-affinity").map(str::to_owned);
        let body: Value = response.into_json().map_err(|error| {
            AdapterError::Backend(format!("invalid Neo4j Query API JSON: {error}"))
        })?;
        let transaction_id = body
            .pointer("/transaction/id")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
            .ok_or_else(|| {
                AdapterError::Backend(
                    "Neo4j Query API transaction response omitted transaction ID".into(),
                )
            })?;
        let transaction = QueryApiTransaction {
            client: self,
            url: format!("{url}/{transaction_id}"),
            affinity,
            open: true,
        };
        reject_query_errors(&body)?;
        let rows = query_rows(&body)?;
        Ok((transaction, rows))
    }

    fn post(
        &self,
        url: &str,
        affinity: Option<&str>,
        statement: &str,
        parameters: Value,
    ) -> Result<ureq::Response, Box<ureq::Error>> {
        let mut request = self
            .agent
            .post(url)
            .set("Accept", "application/json")
            .set("Authorization", &self.authorization);
        if let Some(affinity) = affinity {
            request = request.set("neo4j-cluster-affinity", affinity);
        }
        request
            .send_json(json!({"statement": statement, "parameters": parameters}))
            .map_err(Box::new)
    }

    fn execute_scan(
        &self,
        statement: &str,
        parameters: Value,
        span: &KeySpan,
    ) -> Result<Vec<KeyValue>, AdapterError> {
        let body_limit = neo4j_scan_body_limit(span)?;
        let response = self.post(&self.url, None, statement, parameters);
        let response = match response {
            Ok(response) => response,
            Err(error) => match *error {
                ureq::Error::Status(_, response) => {
                    let status = response.status();
                    let body = bounded_error_body(response)?;
                    return Err(AdapterError::Backend(format!(
                        "Neo4j Query API returned HTTP {status}: {body}"
                    )));
                }
                ureq::Error::Transport(error) => {
                    return Err(AdapterError::Backend(format!(
                        "Neo4j Query API transport failed: {error}"
                    )));
                }
            },
        };
        decode_scan_body(response.into_reader(), span, body_limit)
    }
}

struct QueryApiTransaction<'a> {
    client: &'a QueryApiClient,
    url: String,
    affinity: Option<String>,
    open: bool,
}

impl QueryApiTransaction<'_> {
    fn execute(&self, statement: &str, parameters: Value) -> Result<Vec<Vec<Value>>, AdapterError> {
        let response = self
            .client
            .post(&self.url, self.affinity.as_deref(), statement, parameters);
        let response = match response {
            Ok(response) => response,
            Err(error) => match *error {
                ureq::Error::Status(_, response) => {
                    let status = response.status();
                    let body = response.into_string().unwrap_or_default();
                    return Err(AdapterError::Backend(format!(
                        "Neo4j Query API returned HTTP {status}: {body}"
                    )));
                }
                ureq::Error::Transport(error) => {
                    return Err(AdapterError::Backend(format!(
                        "Neo4j Query API transport failed: {error}"
                    )));
                }
            },
        };
        let body: Value = response.into_json().map_err(|error| {
            AdapterError::Backend(format!("invalid Neo4j Query API JSON: {error}"))
        })?;
        reject_query_errors(&body)?;
        query_rows(&body)
    }

    fn execute_bounded(
        &self,
        statement: &str,
        parameters: Value,
        body_limit: u64,
    ) -> Result<Vec<Vec<Value>>, AdapterError> {
        let response = self
            .client
            .post(&self.url, self.affinity.as_deref(), statement, parameters);
        let response = match response {
            Ok(response) => response,
            Err(error) => match *error {
                ureq::Error::Status(_, response) => {
                    let status = response.status();
                    let body = bounded_error_body(response)?;
                    return Err(AdapterError::Backend(format!(
                        "Neo4j Query API returned HTTP {status}: {body}"
                    )));
                }
                ureq::Error::Transport(error) => {
                    return Err(AdapterError::Backend(format!(
                        "Neo4j Query API transport failed: {error}"
                    )));
                }
            },
        };
        let body = read_bounded_body(response.into_reader(), body_limit)?;
        let body: Value = serde_json::from_slice(&body).map_err(|error| {
            AdapterError::Backend(format!("invalid Neo4j typed query JSON: {error}"))
        })?;
        reject_query_errors(&body)?;
        query_rows(&body)
    }

    fn execute_scan(
        &self,
        statement: &str,
        parameters: Value,
        span: &KeySpan,
    ) -> Result<Vec<KeyValue>, AdapterError> {
        let body_limit = neo4j_scan_body_limit(span)?;
        let response = self
            .client
            .post(&self.url, self.affinity.as_deref(), statement, parameters);
        let response = match response {
            Ok(response) => response,
            Err(error) => match *error {
                ureq::Error::Status(_, response) => {
                    let status = response.status();
                    let body = bounded_error_body(response)?;
                    return Err(AdapterError::Backend(format!(
                        "Neo4j Query API returned HTTP {status}: {body}"
                    )));
                }
                ureq::Error::Transport(error) => {
                    return Err(AdapterError::Backend(format!(
                        "Neo4j Query API transport failed: {error}"
                    )));
                }
            },
        };
        decode_scan_body(response.into_reader(), span, body_limit)
    }

    fn execute_canonical_scan(
        &self,
        parameters: Value,
        request: &CanonicalScanRequest,
    ) -> Result<(Vec<KeyValue>, Option<LogicalKey>), AdapterError> {
        let body_limit = canonical_scan_body_limit(request);
        let response =
            self.client
                .post(&self.url, self.affinity.as_deref(), SCAN_CYPHER, parameters);
        let response = match response {
            Ok(response) => response,
            Err(error) => match *error {
                ureq::Error::Status(_, response) => {
                    let status = response.status();
                    let body = bounded_error_body(response)?;
                    return Err(AdapterError::Backend(format!(
                        "Neo4j Query API returned HTTP {status}: {body}"
                    )));
                }
                ureq::Error::Transport(error) => {
                    return Err(AdapterError::Backend(format!(
                        "Neo4j Query API transport failed: {error}"
                    )));
                }
            },
        };
        let body = read_bounded_body(response.into_reader(), body_limit)?;
        let body: Value = serde_json::from_slice(&body)
            .map_err(|error| AdapterError::Backend(format!("invalid Neo4j scan JSON: {error}")))?;
        reject_query_errors(&body)?;
        bounded_canonical_page(query_rows(&body)?, request)
    }

    fn rollback(&mut self) -> Result<(), AdapterError> {
        if !self.open {
            return Ok(());
        }
        let mut request = self
            .client
            .agent
            .delete(&self.url)
            .set("Accept", "application/json")
            .set("Authorization", &self.client.authorization);
        if let Some(affinity) = self.affinity.as_deref() {
            request = request.set("neo4j-cluster-affinity", affinity);
        }
        match request.call() {
            Ok(_) => {
                self.open = false;
                Ok(())
            }
            Err(ureq::Error::Status(_, response)) => {
                let status = response.status();
                let body = response.into_string().unwrap_or_default();
                Err(AdapterError::Backend(format!(
                    "Neo4j Query API rollback returned HTTP {status}: {body}"
                )))
            }
            Err(ureq::Error::Transport(error)) => Err(AdapterError::Backend(format!(
                "Neo4j Query API rollback transport failed: {error}"
            ))),
        }
    }
}

impl Drop for QueryApiTransaction<'_> {
    fn drop(&mut self) {
        let _ = self.rollback();
    }
}

struct Neo4jReadSnapshot<'a> {
    transaction: Mutex<QueryApiTransaction<'a>>,
    instance_id: String,
    applied_log_index: u64,
}

impl Neo4jReadSnapshot<'_> {
    fn scan_canonical_batch_page(
        &self,
        request: &CanonicalBatchScanRequest,
    ) -> Result<CanonicalBatchScanPage, AdapterError> {
        let ranges = request
            .scans()
            .iter()
            .enumerate()
            .map(|(ordinal, scan)| {
                Ok(json!({
                    "ordinal": ordinal,
                    "keyspace": scan.span().keyspace().tag(),
                    "start_hex": hex(scan.span().start()),
                    "end_hex": scan.span().end().map(hex),
                    "required_prefix_hex": scan.span().required_prefix().map(hex),
                    "max_items": primitive_query_limit(scan.bounds())?,
                    "max_bytes": scan.bounds().max_bytes(),
                }))
            })
            .collect::<Result<Vec<_>, AdapterError>>()?;
        let rows = self
            .transaction
            .lock()
            .map_err(|_| AdapterError::LockPoisoned)?
            .execute_bounded(
                CANONICAL_BATCH_SCAN_CYPHER,
                json!({"instance_id": self.instance_id, "ranges": ranges}),
                canonical_batch_body_limit(request),
            )?;
        let mut grouped = vec![Vec::new(); request.scans().len()];
        for row in rows {
            let ordinal = row
                .first()
                .and_then(Value::as_u64)
                .and_then(|ordinal| usize::try_from(ordinal).ok())
                .ok_or_else(|| {
                    AdapterError::Backend("Neo4j batch scan omitted an ordinal".into())
                })?;
            let grouped = grouped.get_mut(ordinal).ok_or_else(|| {
                AdapterError::Backend("Neo4j batch scan returned an unknown ordinal".into())
            })?;
            grouped.push(vec![row[1].clone(), row[2].clone()]);
        }
        let mut pages = Vec::with_capacity(request.scans().len());
        for (scan, rows) in request.scans().iter().zip(grouped) {
            let (entries, next_start) = bounded_canonical_page(rows, scan)?;
            pages.push(
                CanonicalScanPage::new(scan, self.applied_log_index, entries, next_start)
                    .map_err(query_page_error)?,
            );
        }
        CanonicalBatchScanPage::new(request, self.applied_log_index, pages)
            .map_err(query_page_error)
    }

    fn scan_candidates_page(
        &self,
        request: &CandidateScanRequest,
    ) -> Result<CandidateScanPage, AdapterError> {
        let span = request.span();
        let rows = self
            .transaction
            .lock()
            .map_err(|_| AdapterError::LockPoisoned)?
            .execute_bounded(
                CANDIDATE_SCAN_CYPHER,
                json!({
                    "instance_id": self.instance_id,
                    "keyspace": span.keyspace().tag(),
                    "start_hex": hex(span.start()),
                    "end_hex": span.end().map(hex),
                    "valid_time_micros": request.valid_time().as_micros(),
                    "constraints": candidate_constraint_parameters(request.constraints()),
                    "limit": primitive_query_limit(request.bounds())?,
                }),
                primitive_query_body_limit(request.bounds()),
            )?;
        let (entries, next_start) = bounded_key_value_rows(rows, span, request.bounds(), 0, 1)?;
        CandidateScanPage::new(
            request,
            self.applied_log_index,
            PushdownGuarantee::Candidate,
            entries,
            next_start,
        )
        .map_err(query_page_error)
    }

    fn scan_changes_page(
        &self,
        request: &ChangeScanRequest,
    ) -> Result<ChangeScanPage, AdapterError> {
        let span = request.span();
        let rows = self
            .transaction
            .lock()
            .map_err(|_| AdapterError::LockPoisoned)?
            .execute_bounded(
                CHANGE_SCAN_CYPHER,
                json!({
                    "instance_id": self.instance_id,
                    "keyspace": span.keyspace().tag(),
                    "start_hex": hex(span.start()),
                    "end_hex": span.end().map(hex),
                    "limit": primitive_query_limit(request.bounds())?,
                }),
                primitive_query_body_limit(request.bounds()),
            )?;
        let (entries, next_start) = bounded_key_value_rows(rows, span, request.bounds(), 0, 1)?;
        ChangeScanPage::new(
            request,
            self.applied_log_index,
            PushdownGuarantee::Candidate,
            entries,
            next_start,
        )
        .map_err(query_page_error)
    }

    fn gather_properties_page(
        &self,
        request: &PropertyGatherRequest,
    ) -> Result<PropertyGatherPage, AdapterError> {
        let keys = request
            .keys()
            .iter()
            .enumerate()
            .map(|(ordinal, key)| {
                json!({
                    "ordinal": ordinal,
                    "keyspace": key.keyspace().tag(),
                    "logical_key_hex": hex(key.as_bytes()),
                })
            })
            .collect::<Vec<_>>();
        let rows = self
            .transaction
            .lock()
            .map_err(|_| AdapterError::LockPoisoned)?
            .execute_bounded(
                PROPERTY_GATHER_CYPHER,
                json!({"instance_id": self.instance_id, "keys": keys}),
                primitive_query_body_limit(request.bounds()),
            )?;
        if rows.len() != request.keys().len() {
            return Err(AdapterError::Backend(
                "Neo4j property gather returned an unexpected row count".into(),
            ));
        }
        let mut property_rows = Vec::with_capacity(rows.len());
        for (expected_ordinal, (key, row)) in request.keys().iter().zip(rows).enumerate() {
            let ordinal = row.first().and_then(Value::as_u64).ok_or_else(|| {
                AdapterError::Backend("Neo4j property gather omitted an ordinal".into())
            })?;
            if ordinal != u64::try_from(expected_ordinal).unwrap_or(u64::MAX) {
                return Err(AdapterError::Backend(
                    "Neo4j property gather returned rows out of request order".into(),
                ));
            }
            let raw = decode_optional_base64(row.get(1))?;
            let values = gathered_property_values(key.keyspace(), raw.as_deref(), request)?;
            property_rows.push(PropertyRow::new(key.clone(), values));
        }
        PropertyGatherPage::new(
            request,
            self.applied_log_index,
            PushdownGuarantee::Candidate,
            property_rows,
        )
        .map_err(query_page_error)
    }

    fn expand_adjacency_page(
        &self,
        request: &AdjacencyExpandRequest,
    ) -> Result<AdjacencyExpandPage, AdapterError> {
        let spans = request
            .spans()
            .iter()
            .enumerate()
            .map(|(ordinal, span)| {
                json!({
                    "ordinal": ordinal,
                    "start_hex": hex(span.start()),
                    "end_hex": span.end().map(hex),
                })
            })
            .collect::<Vec<_>>();
        let rows = self
            .transaction
            .lock()
            .map_err(|_| AdapterError::LockPoisoned)?
            .execute_bounded(
                ADJACENCY_EXPAND_CYPHER,
                json!({
                    "instance_id": self.instance_id,
                    "keyspace": request.spans()[0].keyspace().tag(),
                    "spans": spans,
                    "limit": primitive_query_limit(request.bounds())?,
                }),
                primitive_query_body_limit(request.bounds()),
            )?;
        let (entries, next) = bounded_adjacency_rows(rows, request)?;
        AdjacencyExpandPage::new(
            request,
            self.applied_log_index,
            PushdownGuarantee::Candidate,
            entries,
            next,
        )
        .map_err(query_page_error)
    }
}

impl ReadSnapshot for Neo4jReadSnapshot<'_> {
    fn applied_log_index(&self) -> u64 {
        self.applied_log_index
    }

    fn multi_get<'a>(&'a self, keys: &'a [LogicalKey]) -> AdapterFuture<'a, Vec<Option<Vec<u8>>>> {
        Box::pin(async move {
            if keys.is_empty() {
                return Ok(Vec::new());
            }
            let requested = keys
                .iter()
                .enumerate()
                .map(|(ordinal, key)| {
                    json!({"ordinal": ordinal, "keyspace": key.keyspace().tag(), "logical_key_hex": hex(key.as_bytes())})
                })
                .collect::<Vec<_>>();
            let rows = self
                .transaction
                .lock()
                .map_err(|_| AdapterError::LockPoisoned)?
                .execute(
                    MULTI_GET_CYPHER,
                    json!({"instance_id": self.instance_id, "keys": requested}),
                )?;
            if rows.len() != keys.len() {
                return Err(AdapterError::Backend(
                    "Neo4j multi-get returned an unexpected row count".into(),
                ));
            }
            rows.iter()
                .map(|row| decode_optional_base64(row.get(1)))
                .collect()
        })
    }

    fn scan<'a>(&'a self, span: &'a KeySpan) -> AdapterFuture<'a, Vec<KeyValue>> {
        Box::pin(async move {
            let limit = span.limit().unwrap_or(usize::MAX.min(i64::MAX as usize));
            self.transaction
                .lock()
                .map_err(|_| AdapterError::LockPoisoned)?
                .execute_scan(
                    SCAN_CYPHER,
                    json!({
                        "instance_id": self.instance_id,
                        "keyspace": span.keyspace().tag(),
                        "start_hex": hex(span.start()),
                        "end_hex": span.end().map(hex),
                        "limit": limit,
                    }),
                    span,
                )
        })
    }

    fn scan_canonical<'a>(
        &'a self,
        request: &'a CanonicalScanRequest,
    ) -> AdapterFuture<'a, CanonicalScanPage> {
        Box::pin(async move {
            let span = request.span();
            let limit = request.bounds().max_items().checked_add(1).ok_or_else(|| {
                AdapterError::Backend("Neo4j canonical scan limit overflow".into())
            })?;
            let (entries, next_start) = self
                .transaction
                .lock()
                .map_err(|_| AdapterError::LockPoisoned)?
                .execute_canonical_scan(
                    json!({
                        "instance_id": self.instance_id,
                        "keyspace": span.keyspace().tag(),
                        "start_hex": hex(span.start()),
                        "end_hex": span.end().map(hex),
                        "limit": limit,
                    }),
                    request,
                )?;
            CanonicalScanPage::new(request, self.applied_log_index, entries, next_start)
                .map_err(|error| AdapterError::Backend(error.to_string()))
        })
    }

    fn scan_canonical_batch<'a>(
        &'a self,
        request: &'a CanonicalBatchScanRequest,
    ) -> AdapterFuture<'a, CanonicalBatchScanPage> {
        Box::pin(async move { self.scan_canonical_batch_page(request) })
    }

    fn scan_candidates<'a>(
        &'a self,
        request: &'a CandidateScanRequest,
    ) -> AdapterFuture<'a, CandidateScanPage> {
        Box::pin(async move { self.scan_candidates_page(request) })
    }

    fn scan_changes<'a>(
        &'a self,
        request: &'a ChangeScanRequest,
    ) -> AdapterFuture<'a, ChangeScanPage> {
        Box::pin(async move { self.scan_changes_page(request) })
    }
}

fn reject_query_errors(body: &Value) -> Result<(), AdapterError> {
    if let Some(errors) = body.get("errors").and_then(Value::as_array)
        && !errors.is_empty()
    {
        return Err(AdapterError::Backend(format!(
            "Neo4j Query API error: {}",
            errors[0]
        )));
    }
    Ok(())
}

fn query_rows(body: &Value) -> Result<Vec<Vec<Value>>, AdapterError> {
    body.pointer("/data/values")
        .and_then(Value::as_array)
        .map(|rows| {
            rows.iter()
                .map(|row| {
                    row.as_array().cloned().ok_or_else(|| {
                        AdapterError::Backend("Neo4j result row is not an array".into())
                    })
                })
                .collect()
        })
        .transpose()
        .map(Option::unwrap_or_default)
}

fn neo4j_scan_body_limit(span: &KeySpan) -> Result<u64, AdapterError> {
    let Some(max_bytes) = span.max_bytes() else {
        return Ok(MAX_NEO4J_SCAN_BODY_BYTES);
    };
    let row_bound = span
        .limit()
        .map(|limit| u64::try_from(limit).unwrap_or(u64::MAX))
        .unwrap_or_else(|| max_bytes.saturating_add(1))
        .min(MAX_NEO4J_SCAN_ROWS);
    let encoded_payload = max_bytes.saturating_mul(2);
    let row_overhead = row_bound.saturating_mul(NEO4J_SCAN_ROW_OVERHEAD_BYTES);
    let required = NEO4J_SCAN_ENVELOPE_BYTES
        .saturating_add(encoded_payload)
        .saturating_add(row_overhead);
    Ok(required.min(MAX_NEO4J_SCAN_BODY_BYTES))
}

fn bounded_error_body(response: ureq::Response) -> Result<String, AdapterError> {
    let mut body = String::new();
    response
        .into_reader()
        .take(16 * 1024)
        .read_to_string(&mut body)
        .map_err(|error| AdapterError::Backend(format!("Neo4j error body read failed: {error}")))?;
    Ok(body)
}

fn canonical_scan_body_limit(request: &CanonicalScanRequest) -> u64 {
    let bounds = request.bounds();
    let rows = u64::try_from(bounds.max_items())
        .unwrap_or(u64::MAX)
        .saturating_add(1);
    NEO4J_SCAN_ENVELOPE_BYTES
        .saturating_add(bounds.max_bytes().saturating_mul(2))
        .saturating_add(rows.saturating_mul(NEO4J_SCAN_ROW_OVERHEAD_BYTES))
        .min(MAX_NEO4J_SCAN_BODY_BYTES)
}

fn canonical_batch_body_limit(request: &CanonicalBatchScanRequest) -> u64 {
    request
        .scans()
        .iter()
        .fold(NEO4J_SCAN_ENVELOPE_BYTES, |limit, scan| {
            limit.saturating_add(canonical_scan_body_limit(scan))
        })
        .min(MAX_NEO4J_SCAN_BODY_BYTES)
}

fn primitive_query_limit(bounds: QueryPageBounds) -> Result<usize, AdapterError> {
    bounds
        .max_items()
        .checked_add(1)
        .ok_or_else(|| AdapterError::Backend("Neo4j typed query limit overflow".into()))
}

fn primitive_query_body_limit(bounds: QueryPageBounds) -> u64 {
    let rows = u64::try_from(bounds.max_items())
        .unwrap_or(u64::MAX)
        .saturating_add(1);
    NEO4J_SCAN_ENVELOPE_BYTES
        .saturating_add(bounds.max_bytes().saturating_mul(3))
        .saturating_add(rows.saturating_mul(NEO4J_SCAN_ROW_OVERHEAD_BYTES))
        .min(MAX_NEO4J_SCAN_BODY_BYTES)
}

fn candidate_constraint_parameters(constraints: &[storage_api::PropertyConstraint]) -> Vec<Value> {
    constraints
        .iter()
        .map(|constraint| {
            let operator = match constraint.operator() {
                storage_api::ComparisonOperator::Equal => "equal",
                storage_api::ComparisonOperator::NotEqual => "not_equal",
                storage_api::ComparisonOperator::LessThan => "less_than",
                storage_api::ComparisonOperator::LessThanOrEqual => "less_than_or_equal",
                storage_api::ComparisonOperator::GreaterThan => "greater_than",
                storage_api::ComparisonOperator::GreaterThanOrEqual => "greater_than_or_equal",
            };
            json!({
                "property_id": constraint.property().value(),
                "operator": operator,
                "value": graph_value_parameter(constraint.value()),
                "token": property_equal_token(constraint.property().value(), constraint.value()),
            })
        })
        .collect()
}

fn graph_value_parameter(value: &temporal_types::GraphValue) -> Value {
    match value {
        temporal_types::GraphValue::Null => json!({"type": "null", "value": null}),
        temporal_types::GraphValue::Boolean(value) => {
            json!({"type": "boolean", "value": value})
        }
        temporal_types::GraphValue::Integer(value) => json!({"type": "integer", "value": value}),
        temporal_types::GraphValue::FloatBits(value) => {
            json!({"type": "float_bits", "value": u64_hex(*value)})
        }
        temporal_types::GraphValue::String(value) => json!({"type": "string", "value": value}),
        temporal_types::GraphValue::Bytes(value) => {
            json!({"type": "bytes", "value": BASE64.encode(value)})
        }
        temporal_types::GraphValue::TimestampMicros(value) => {
            json!({"type": "timestamp_micros", "value": value})
        }
        temporal_types::GraphValue::List(values) => json!({
            "type": "list",
            "value": values.iter().map(graph_value_parameter).collect::<Vec<_>>(),
        }),
    }
}

fn query_page_error(error: impl ToString) -> AdapterError {
    AdapterError::Backend(error.to_string())
}

fn stable_projection_property(
    projection: Option<&ProjectionRecord>,
    property_id: u32,
) -> Option<temporal_types::GraphValue> {
    let mut segments = projection?.segments().iter();
    let first = segments.next()?.payload().property(property_id)?.clone();
    segments
        .all(|segment| segment.payload().property(property_id) == Some(&first))
        .then_some(first)
}

fn gathered_property_values(
    keyspace: Keyspace,
    raw: Option<&[u8]>,
    request: &PropertyGatherRequest,
) -> Result<Vec<Option<temporal_types::GraphValue>>, AdapterError> {
    let Some(raw) = raw else {
        return Ok(vec![None; request.properties().len()]);
    };
    match keyspace {
        Keyspace::Current => {
            let projection = ProjectionRecord::decode(raw)
                .map_err(|error| AdapterError::Backend(error.to_string()))?;
            Ok(request
                .properties()
                .iter()
                .map(|property| stable_projection_property(Some(&projection), property.value()))
                .collect())
        }
        Keyspace::History => {
            let history = HistoryEntry::decode(raw)
                .map_err(|error| AdapterError::Backend(error.to_string()))?;
            Ok(request
                .properties()
                .iter()
                .map(|property| {
                    history
                        .replacement()
                        .and_then(|element| element.property(property.value()))
                        .cloned()
                })
                .collect())
        }
        _ => Err(AdapterError::Backend(
            "Neo4j property gather received an unsupported keyspace".into(),
        )),
    }
}

fn bounded_key_value_rows(
    rows: Vec<Vec<Value>>,
    span: &KeySpan,
    bounds: QueryPageBounds,
    key_column: usize,
    value_column: usize,
) -> Result<(Vec<KeyValue>, Option<LogicalKey>), AdapterError> {
    let mut entries = Vec::new();
    let mut retained = 0_u64;
    let mut previous: Option<Vec<u8>> = None;
    for row in rows {
        let key = decode_hex(value_string(row.get(key_column), "logical key")?)?;
        if !span.contains(&key) {
            return Err(AdapterError::Backend(
                "Neo4j typed query returned a key outside the requested span".into(),
            ));
        }
        if previous
            .as_deref()
            .is_some_and(|previous| previous >= key.as_slice())
        {
            return Err(AdapterError::Backend(
                "Neo4j typed query returned keys out of canonical order".into(),
            ));
        }
        previous = Some(key.clone());
        let logical_key = LogicalKey::in_keyspace(span.keyspace(), key);
        if entries.len() == bounds.max_items() {
            return Ok((entries, Some(logical_key)));
        }
        let value = decode_base64(row.get(value_column).ok_or_else(|| {
            AdapterError::Backend("Neo4j typed query row is missing value".into())
        })?)?;
        let required = retained.saturating_add(entry_retained_bytes(&logical_key, &value));
        if required > bounds.max_bytes() {
            if entries.is_empty() {
                return Err(AdapterError::ScanByteLimit {
                    limit: bounds.max_bytes(),
                    required,
                });
            }
            return Ok((entries, Some(logical_key)));
        }
        retained = required;
        entries.push(KeyValue::new(logical_key, value));
    }
    Ok((entries, None))
}

fn bounded_adjacency_rows(
    rows: Vec<Vec<Value>>,
    request: &AdjacencyExpandRequest,
) -> Result<(Vec<AdjacencyEntry>, Option<storage_api::AdjacencyCursor>), AdapterError> {
    let mut entries = Vec::new();
    let mut retained = 0_u64;
    let mut previous: Option<(usize, Vec<u8>)> = None;
    for row in rows {
        let ordinal = row
            .first()
            .and_then(Value::as_u64)
            .and_then(|value| usize::try_from(value).ok())
            .ok_or_else(|| {
                AdapterError::Backend("Neo4j adjacency query omitted an ordinal".into())
            })?;
        let span = request.spans().get(ordinal).ok_or_else(|| {
            AdapterError::Backend("Neo4j adjacency query returned an unknown ordinal".into())
        })?;
        let key = decode_hex(value_string(row.get(1), "logical key")?)?;
        if !span.contains(&key) {
            return Err(AdapterError::Backend(
                "Neo4j adjacency query returned a key outside its input span".into(),
            ));
        }
        if previous.as_ref().is_some_and(|previous| {
            (previous.0, previous.1.as_slice()) >= (ordinal, key.as_slice())
        }) {
            return Err(AdapterError::Backend(
                "Neo4j adjacency query returned keys out of request order".into(),
            ));
        }
        previous = Some((ordinal, key.clone()));
        let logical_key = LogicalKey::in_keyspace(span.keyspace(), key);
        if entries.len() == request.bounds().max_items() {
            return Ok((
                entries,
                Some(storage_api::AdjacencyCursor::new(ordinal, logical_key)),
            ));
        }
        let value = decode_base64(row.get(2).ok_or_else(|| {
            AdapterError::Backend("Neo4j adjacency query row is missing value".into())
        })?)?;
        let required = retained.saturating_add(entry_retained_bytes(&logical_key, &value));
        if required > request.bounds().max_bytes() {
            if entries.is_empty() {
                return Err(AdapterError::ScanByteLimit {
                    limit: request.bounds().max_bytes(),
                    required,
                });
            }
            return Ok((
                entries,
                Some(storage_api::AdjacencyCursor::new(ordinal, logical_key)),
            ));
        }
        retained = required;
        entries.push(AdjacencyEntry::new(
            ordinal,
            KeyValue::new(logical_key, value),
        ));
    }
    Ok((entries, None))
}

fn entry_retained_bytes(key: &LogicalKey, value: &[u8]) -> u64 {
    u64::try_from(key.as_bytes().len())
        .ok()
        .and_then(|key_bytes| {
            u64::try_from(value.len())
                .ok()
                .and_then(|value_bytes| key_bytes.checked_add(value_bytes))
        })
        .unwrap_or(u64::MAX)
}

fn read_bounded_body(reader: impl Read, limit: u64) -> Result<Vec<u8>, AdapterError> {
    let read_limit = limit
        .checked_add(1)
        .ok_or_else(|| AdapterError::Backend("Neo4j scan response bound overflow".into()))?;
    let mut body = Vec::new();
    reader
        .take(read_limit)
        .read_to_end(&mut body)
        .map_err(|error| AdapterError::Backend(format!("Neo4j scan body read failed: {error}")))?;
    if u64::try_from(body.len()).unwrap_or(u64::MAX) > limit {
        return Err(AdapterError::ScanResponseByteLimit {
            limit,
            required: read_limit,
        });
    }
    Ok(body)
}

fn bounded_canonical_page(
    rows: Vec<Vec<Value>>,
    request: &CanonicalScanRequest,
) -> Result<(Vec<KeyValue>, Option<LogicalKey>), AdapterError> {
    let span = request.span();
    let bounds = request.bounds();
    let mut entries = Vec::new();
    let mut retained = 0_u64;
    let mut previous: Option<Vec<u8>> = None;
    for row in rows {
        let key = decode_hex(value_string(row.first(), "logical key")?)?;
        if !span.contains(&key) {
            return Err(AdapterError::Backend(
                "Neo4j canonical scan returned a key outside the requested span".into(),
            ));
        }
        if previous
            .as_deref()
            .is_some_and(|previous| previous >= key.as_slice())
        {
            return Err(AdapterError::Backend(
                "Neo4j canonical scan returned keys out of canonical order".into(),
            ));
        }
        previous = Some(key.clone());
        let logical_key = LogicalKey::in_keyspace(span.keyspace(), key);
        if entries.len() == bounds.max_items() {
            return Ok((entries, Some(logical_key)));
        }
        let value = decode_base64(row.get(1).ok_or_else(|| {
            AdapterError::Backend("Neo4j canonical scan row is missing value".into())
        })?)?;
        let entry_bytes = u64::try_from(logical_key.as_bytes().len())
            .ok()
            .and_then(|key_bytes| {
                u64::try_from(value.len())
                    .ok()
                    .and_then(|value_bytes| key_bytes.checked_add(value_bytes))
            })
            .unwrap_or(u64::MAX);
        let required = retained.saturating_add(entry_bytes);
        if required > bounds.max_bytes() {
            if entries.is_empty() {
                return Err(AdapterError::ScanByteLimit {
                    limit: bounds.max_bytes(),
                    required,
                });
            }
            return Ok((entries, Some(logical_key)));
        }
        retained = required;
        entries.push(KeyValue::new(logical_key, value));
    }
    Ok((entries, None))
}

fn decode_scan_body(
    reader: impl Read,
    span: &KeySpan,
    max_body_bytes: u64,
) -> Result<Vec<KeyValue>, AdapterError> {
    let read_limit = max_body_bytes
        .checked_add(1)
        .ok_or_else(|| AdapterError::Backend("Neo4j scan response bound overflow".into()))?;
    let mut reader = reader.take(read_limit);
    let mut state = ScanDecodeState {
        span,
        retained: 0,
        values: Vec::new(),
        failure: None,
    };
    let mut deserializer = serde_json::Deserializer::from_reader(&mut reader);
    let decoded = ScanEnvelopeSeed { state: &mut state }.deserialize(&mut deserializer);
    let ended = decoded.and_then(|()| deserializer.end());
    drop(deserializer);
    if let Some(error) = state.failure {
        return Err(error);
    }
    if reader.limit() == 0 {
        return Err(AdapterError::ScanResponseByteLimit {
            limit: max_body_bytes,
            required: read_limit,
        });
    }
    ended.map_err(|error| AdapterError::Backend(format!("invalid Neo4j scan JSON: {error}")))?;
    Ok(state.values)
}

struct ScanDecodeState<'a> {
    span: &'a KeySpan,
    retained: u64,
    values: Vec<KeyValue>,
    failure: Option<AdapterError>,
}

struct ScanEnvelopeSeed<'a, 'state> {
    state: &'state mut ScanDecodeState<'a>,
}

impl<'de> DeserializeSeed<'de> for ScanEnvelopeSeed<'_, '_> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(ScanEnvelopeVisitor { state: self.state })
    }
}

struct ScanEnvelopeVisitor<'a, 'state> {
    state: &'state mut ScanDecodeState<'a>,
}

impl<'de> Visitor<'de> for ScanEnvelopeVisitor<'_, '_> {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a Neo4j query response object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        while let Some(field) = map.next_key::<String>()? {
            match field.as_str() {
                "data" => map.next_value_seed(ScanDataSeed { state: self.state })?,
                "errors" => {
                    let errors = map.next_value::<Vec<Value>>()?;
                    if let Some(error) = errors.first() {
                        self.state.failure = Some(AdapterError::Backend(format!(
                            "Neo4j Query API error: {error}"
                        )));
                        return Err(de::Error::custom("Neo4j Query API returned an error"));
                    }
                }
                _ => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }
        Ok(())
    }
}

struct ScanDataSeed<'a, 'state> {
    state: &'state mut ScanDecodeState<'a>,
}

impl<'de> DeserializeSeed<'de> for ScanDataSeed<'_, '_> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(ScanDataVisitor { state: self.state })
    }
}

struct ScanDataVisitor<'a, 'state> {
    state: &'state mut ScanDecodeState<'a>,
}

impl<'de> Visitor<'de> for ScanDataVisitor<'_, '_> {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a Neo4j query data object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        while let Some(field) = map.next_key::<String>()? {
            if field == "values" {
                map.next_value_seed(ScanRowsSeed { state: self.state })?;
            } else {
                map.next_value::<IgnoredAny>()?;
            }
        }
        Ok(())
    }
}

struct ScanRowsSeed<'a, 'state> {
    state: &'state mut ScanDecodeState<'a>,
}

impl<'de> DeserializeSeed<'de> for ScanRowsSeed<'_, '_> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_seq(ScanRowsVisitor { state: self.state })
    }
}

struct ScanRowsVisitor<'a, 'state> {
    state: &'state mut ScanDecodeState<'a>,
}

impl<'de> Visitor<'de> for ScanRowsVisitor<'_, '_> {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("an array of Neo4j scan rows")
    }

    fn visit_seq<A>(self, mut rows: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        while let Some(row) = rows.next_element::<Vec<Value>>()? {
            let decoded = (|| {
                if self
                    .state
                    .span
                    .limit()
                    .is_some_and(|limit| self.state.values.len() >= limit)
                {
                    return Err(AdapterError::Backend(
                        "Neo4j scan returned more rows than requested".into(),
                    ));
                }
                let key = decode_hex(value_string(row.first(), "logical key")?)?;
                if !self.state.span.contains(&key) {
                    return Err(AdapterError::Backend(
                        "Neo4j scan returned a key outside the requested span".into(),
                    ));
                }
                let value = decode_base64(row.get(1).ok_or_else(|| {
                    AdapterError::Backend("Neo4j scan row is missing value".into())
                })?)?;
                let retained = storage_api::charge_scan_entry(
                    self.state.span,
                    self.state.retained,
                    &key,
                    &value,
                )?;
                Ok((key, value, retained))
            })();
            match decoded {
                Ok((key, value, retained)) => {
                    self.state.retained = retained;
                    self.state.values.push(KeyValue::new(
                        LogicalKey::in_keyspace(self.state.span.keyspace(), key),
                        value,
                    ));
                }
                Err(error) => {
                    self.state.failure = Some(error);
                    return Err(de::Error::custom("Neo4j scan row was rejected"));
                }
            }
        }
        Ok(())
    }
}

struct Neo4jLogicalSnapshotReader {
    header: LogicalSnapshotHeaderV1,
    chunks: VecDeque<LogicalSnapshotChunkV1>,
    accumulator: LogicalSnapshotAccumulator,
    exhausted: bool,
}

impl Neo4jLogicalSnapshotReader {
    fn new(
        applied_index: u64,
        entries: Vec<KeyValue>,
        request: LogicalSnapshotExportRequest,
    ) -> Result<Self, AdapterError> {
        let header = LogicalSnapshotHeaderV1::new(new_logical_snapshot_id(), applied_index);
        let mut chunks = VecDeque::new();
        let mut current = Vec::new();
        let mut current_bytes = 0_usize;
        let mut ordinal = 0_u64;
        for entry in entries {
            let entry_bytes = snapshot_entry_bytes(&entry);
            if entry_bytes > request.max_bytes_per_chunk() {
                return Err(LogicalSnapshotError::EntryTooLarge {
                    max: request.max_bytes_per_chunk(),
                    actual: entry_bytes,
                }
                .into());
            }
            if !current.is_empty()
                && (current.len() == request.max_entries_per_chunk()
                    || current_bytes.saturating_add(entry_bytes) > request.max_bytes_per_chunk())
            {
                chunks.push_back(LogicalSnapshotChunkV1::new(
                    header.snapshot_id(),
                    ordinal,
                    std::mem::take(&mut current),
                )?);
                ordinal = ordinal
                    .checked_add(1)
                    .ok_or(LogicalSnapshotError::CountOverflow)?;
                current_bytes = 0;
            }
            current_bytes = current_bytes.saturating_add(entry_bytes);
            current.push(entry);
        }
        if !current.is_empty() {
            chunks.push_back(LogicalSnapshotChunkV1::new(
                header.snapshot_id(),
                ordinal,
                current,
            )?);
        }
        Ok(Self {
            accumulator: LogicalSnapshotAccumulator::new(header.clone()),
            header,
            chunks,
            exhausted: false,
        })
    }
}

impl LogicalSnapshotReader for Neo4jLogicalSnapshotReader {
    fn header(&self) -> &LogicalSnapshotHeaderV1 {
        &self.header
    }

    fn next_chunk<'a>(&'a mut self) -> AdapterFuture<'a, Option<LogicalSnapshotChunkV1>> {
        Box::pin(async move {
            let chunk = self.chunks.pop_front();
            if let Some(chunk) = &chunk {
                self.accumulator.observe(chunk)?;
            } else {
                self.exhausted = true;
            }
            Ok(chunk)
        })
    }

    fn finish<'a>(self: Box<Self>) -> AdapterFuture<'a, LogicalSnapshotManifestV1>
    where
        Self: 'a,
    {
        Box::pin(async move {
            if !self.exhausted {
                return Err(LogicalSnapshotError::ExportNotExhausted.into());
            }
            Ok(self.accumulator.complete())
        })
    }
}

struct Neo4jRestoreSession {
    adapter: Option<Neo4jAdapter>,
    header: LogicalSnapshotHeaderV1,
    accumulator: LogicalSnapshotAccumulator,
    last_chunk: Option<(u64, [u8; 32])>,
    saw_applied_index: bool,
    published: bool,
}

impl AdapterRestoreSession for Neo4jRestoreSession {
    fn descriptor(&self) -> AdapterDescriptorV1 {
        self.adapter
            .as_ref()
            .expect("restore Adapter exists before finish")
            .descriptor()
    }

    fn write_chunk<'a>(
        &'a mut self,
        chunk: LogicalSnapshotChunkV1,
    ) -> AdapterRestoreFuture<'a, ()> {
        Box::pin(async move {
            if self.last_chunk == Some((chunk.ordinal(), chunk.digest())) {
                return Ok(());
            }
            let mut accumulator = self.accumulator.clone();
            accumulator
                .observe(&chunk)
                .map_err(|error| AdapterFactoryError::new(error.to_string()))?;
            let saw = self
                .adapter
                .as_ref()
                .ok_or_else(|| AdapterFactoryError::new("restore session is finished"))?
                .restore_entries(chunk.entries(), self.header.applied_log_index())
                .map_err(factory_error)?;
            self.saw_applied_index |= saw;
            self.last_chunk = Some((chunk.ordinal(), chunk.digest()));
            self.accumulator = accumulator;
            Ok(())
        })
    }

    fn finish<'a>(
        mut self: Box<Self>,
        manifest: LogicalSnapshotManifestV1,
    ) -> AdapterRestoreFuture<'a, Arc<dyn StorageAdapter>>
    where
        Self: 'a,
    {
        Box::pin(async move {
            self.accumulator
                .clone()
                .verify(&manifest)
                .map_err(|error| AdapterFactoryError::new(error.to_string()))?;
            if self.header.applied_log_index() != 0 && !self.saw_applied_index {
                return Err(AdapterFactoryError::new(
                    "logical snapshot is missing applied-index record",
                ));
            }
            let adapter = self
                .adapter
                .take()
                .ok_or_else(|| AdapterFactoryError::new("restore session is finished"))?;
            adapter
                .publish_restore(self.header.applied_log_index())
                .map_err(factory_error)?;
            self.published = true;
            Ok(Arc::new(adapter) as Arc<dyn StorageAdapter>)
        })
    }

    fn abort<'a>(mut self: Box<Self>) -> AdapterRestoreFuture<'a, ()>
    where
        Self: 'a,
    {
        Box::pin(async move {
            self.adapter
                .take()
                .ok_or_else(|| AdapterFactoryError::new("restore session is finished"))?
                .delete_namespace()
                .map_err(factory_error)
        })
    }
}

impl Drop for Neo4jRestoreSession {
    fn drop(&mut self) {
        if !self.published
            && let Some(adapter) = self.adapter.take()
        {
            let _ = adapter.delete_namespace();
        }
    }
}

#[derive(Default)]
struct Neo4jNativeFields {
    kind: &'static str,
    label: &'static str,
    graph_hex: String,
    partition_hex: String,
    element_hex: String,
    edge_type_hex: String,
    source_partition_hex: String,
    source_hex: String,
    destination_partition_hex: String,
    destination_hex: String,
    transaction_physical: i64,
    transaction_logical_hex: String,
    segment_hex: String,
    has_edge_endpoints: bool,
    valid_min_micros: Option<i64>,
    valid_max_micros: Option<i64>,
    property_equal_tokens: Vec<String>,
}

fn native_mutation_json(
    sequence: u32,
    fingerprint: u64,
    operation: &MutationOperation,
) -> Result<Value, AdapterError> {
    let mut value = match operation {
        MutationOperation::Put { key, value } => {
            native_record_json(key, Some(value.as_slice()), true)?
        }
        MutationOperation::Delete { key } => native_record_json(key, None, false)?,
    };
    let object = value.as_object_mut().ok_or_else(|| {
        AdapterError::Backend("Neo4j native mutation payload is not an object".into())
    })?;
    object.insert("sequence".into(), json!(sequence));
    object.insert("fingerprint_hex".into(), json!(u64_hex(fingerprint)));
    Ok(value)
}

fn native_record_json(
    key: &LogicalKey,
    value: Option<&[u8]>,
    present: bool,
) -> Result<Value, AdapterError> {
    let fields = match value {
        Some(value) => native_fields_from_entry(
            &decode_canonical_graph_entry(key, value)
                .map_err(|error| AdapterError::Backend(error.to_string()))?,
        ),
        None => native_fields_from_key(key)?,
    };
    Ok(json!({
        "keyspace": key.keyspace().tag(),
        "logical_key_hex": hex(key.as_bytes()),
        "present": present,
        "value_base64": value.map_or_else(String::new, |bytes| BASE64.encode(bytes)),
        "valid_min_micros": fields.valid_min_micros,
        "valid_max_micros": fields.valid_max_micros,
        "property_equal_tokens": fields.property_equal_tokens,
        "kind": fields.kind,
        "label": fields.label,
        "graph_hex": fields.graph_hex,
        "partition_hex": fields.partition_hex,
        "element_hex": fields.element_hex,
        "edge_type_hex": fields.edge_type_hex,
        "source_partition_hex": fields.source_partition_hex,
        "source_hex": fields.source_hex,
        "destination_partition_hex": fields.destination_partition_hex,
        "destination_hex": fields.destination_hex,
        "transaction_physical": fields.transaction_physical,
        "transaction_logical_hex": fields.transaction_logical_hex,
        "segment_hex": fields.segment_hex,
        "has_edge_endpoints": fields.has_edge_endpoints,
    }))
}

fn native_fields_from_entry(entry: &CanonicalGraphEntry) -> Neo4jNativeFields {
    match entry {
        CanonicalGraphEntry::VertexIdentity { value, .. } => {
            element_fields("vertex_identity", "DTGVertexIdentity", value.element())
        }
        CanonicalGraphEntry::EdgeIdentity { value, .. } => {
            let mut fields = element_fields("edge_identity", "DTGEdgeIdentity", value.element());
            fields.edge_type_hex = hex(&value.edge_type().value().to_be_bytes());
            fields.source_partition_hex =
                hex(&value.source_ref().partition().value().to_be_bytes());
            fields.source_hex = hex(&value.source().value().to_be_bytes());
            fields.destination_partition_hex =
                hex(&value.destination_ref().partition().value().to_be_bytes());
            fields.destination_hex = hex(&value.destination().value().to_be_bytes());
            fields.has_edge_endpoints = true;
            fields
        }
        CanonicalGraphEntry::Current { key, value } => {
            let mut fields = match *key {
                GraphKey::CurrentVertex(element) => {
                    element_fields("vertex_current", "DTGVertexCurrent", element)
                }
                GraphKey::CurrentEdge(element) => {
                    element_fields("edge_current", "DTGEdgeCurrent", element)
                }
                _ => unreachable!("canonical Current entry has a Current key"),
            };
            add_projection_query_fields(&mut fields, value);
            fields
        }
        CanonicalGraphEntry::Adjacency { key, value } => {
            let mut fields = adjacency_fields(*key);
            add_projection_query_fields(&mut fields, value);
            fields
        }
        CanonicalGraphEntry::History { key, .. } => history_fields(*key),
        CanonicalGraphEntry::Opaque { .. } => Neo4jNativeFields {
            kind: "opaque",
            label: "DTGOpaqueRecord",
            ..Neo4jNativeFields::default()
        },
    }
}

fn native_fields_from_key(key: &LogicalKey) -> Result<Neo4jNativeFields, AdapterError> {
    if matches!(
        key.keyspace(),
        Keyspace::Meta | Keyspace::TemporalIndex | Keyspace::Txn
    ) {
        return Ok(Neo4jNativeFields {
            kind: "opaque",
            label: "DTGOpaqueRecord",
            ..Neo4jNativeFields::default()
        });
    }
    let key = decode_graph_key(key).map_err(|error| AdapterError::Backend(error.to_string()))?;
    Ok(match key {
        GraphKey::VertexIdentity(element) => {
            element_fields("vertex_identity", "DTGVertexIdentity", element)
        }
        GraphKey::EdgeIdentity(element) => {
            element_fields("edge_identity", "DTGEdgeIdentity", element)
        }
        GraphKey::CurrentVertex(element) => {
            element_fields("vertex_current", "DTGVertexCurrent", element)
        }
        GraphKey::CurrentEdge(element) => element_fields("edge_current", "DTGEdgeCurrent", element),
        key @ (GraphKey::OutAdjacency { .. }
        | GraphKey::InAdjacency { .. }
        | GraphKey::CrossOutAdjacency { .. }
        | GraphKey::CrossInAdjacency { .. }) => adjacency_fields(key),
        key @ GraphKey::HistoryAnchor { .. } => history_fields(key),
        GraphKey::TemporalEvent { .. } | GraphKey::TemporalEventValid { .. } => Neo4jNativeFields {
            kind: "opaque",
            label: "DTGOpaqueRecord",
            ..Neo4jNativeFields::default()
        },
    })
}

fn element_fields(
    kind: &'static str,
    label: &'static str,
    element: temporal_storage::ElementRef,
) -> Neo4jNativeFields {
    Neo4jNativeFields {
        kind,
        label,
        graph_hex: hex(&element.graph().value().to_be_bytes()),
        partition_hex: hex(&element.partition().value().to_be_bytes()),
        element_hex: hex(&element.id().value().to_be_bytes()),
        ..Neo4jNativeFields::default()
    }
}

fn add_projection_query_fields(fields: &mut Neo4jNativeFields, projection: &ProjectionRecord) {
    fields.valid_min_micros = projection
        .segments()
        .first()
        .map(|segment| segment.valid().start().as_micros());
    fields.valid_max_micros = projection
        .segments()
        .last()
        .and_then(|segment| segment.valid().end())
        .map(temporal_types::ValidTime::as_micros);
    let mut tokens = BTreeSet::new();
    for segment in projection.segments() {
        for (property_id, value) in segment.payload().properties() {
            tokens.insert(property_equal_token(*property_id, value));
        }
    }
    fields.property_equal_tokens = tokens.into_iter().collect();
}

fn property_equal_token(property_id: u32, value: &temporal_types::GraphValue) -> String {
    let mut hasher = Hasher::new();
    hasher.update(&property_id.to_be_bytes());
    hasher.update(format!("{value:?}").as_bytes());
    hex(hasher.finalize().as_bytes())
}

fn history_fields(key: GraphKey) -> Neo4jNativeFields {
    let GraphKey::HistoryAnchor {
        element,
        transaction_time,
        segment_id,
    } = key
    else {
        unreachable!("history_fields requires a History key")
    };
    let (kind, label) = match element.kind() {
        temporal_storage::ElementKind::Vertex => ("vertex_history", "DTGVertexHistory"),
        temporal_storage::ElementKind::Edge => ("edge_history", "DTGEdgeHistory"),
    };
    let mut fields = element_fields(kind, label, element);
    fields.transaction_physical = transaction_time.physical_micros();
    fields.transaction_logical_hex = hex(&transaction_time.logical().to_be_bytes());
    fields.segment_hex = hex(&segment_id.to_be_bytes());
    fields
}

fn adjacency_fields(key: GraphKey) -> Neo4jNativeFields {
    let (kind, label, graph, partition, endpoint, edge_type, edge) = match key {
        GraphKey::OutAdjacency {
            graph,
            partition,
            source,
            edge_type,
            edge,
            ..
        }
        | GraphKey::CrossOutAdjacency {
            graph,
            partition,
            source,
            edge_type,
            edge,
            ..
        } => (
            "adjacency_out",
            "DTGOutAdjacency",
            graph,
            partition,
            source,
            edge_type,
            edge,
        ),
        GraphKey::InAdjacency {
            graph,
            partition,
            destination,
            edge_type,
            edge,
            ..
        }
        | GraphKey::CrossInAdjacency {
            graph,
            partition,
            destination,
            edge_type,
            edge,
            ..
        } => (
            "adjacency_in",
            "DTGInAdjacency",
            graph,
            partition,
            destination,
            edge_type,
            edge,
        ),
        _ => unreachable!("adjacency_fields requires an adjacency key"),
    };
    Neo4jNativeFields {
        kind,
        label,
        graph_hex: hex(&graph.value().to_be_bytes()),
        partition_hex: hex(&partition.value().to_be_bytes()),
        element_hex: hex(&endpoint.value().to_be_bytes()),
        edge_type_hex: hex(&edge_type.value().to_be_bytes()),
        destination_hex: hex(&edge.value().to_be_bytes()),
        ..Neo4jNativeFields::default()
    }
}

fn validate_batch(batch: &CommittedMutationBatch) -> Result<(), AdapterError> {
    let mut sequences = BTreeSet::new();
    for mutation in &batch.mutations {
        if !sequences.insert(mutation.sequence) {
            return Err(AdapterError::DuplicateMutationSequence {
                txn_id: batch.txn_id,
                sequence: mutation.sequence,
            });
        }
        match &mutation.operation {
            MutationOperation::Put { key, value } => {
                decode_canonical_graph_entry(key, value)
                    .map_err(|error| AdapterError::Backend(error.to_string()))?;
            }
            MutationOperation::Delete { key } => {
                native_fields_from_key(key)?;
            }
        }
    }
    Ok(())
}

fn existing_instance_missing(row: Option<&Vec<Value>>) -> bool {
    row.is_none() || row.is_some_and(|row| row.first().is_none_or(Value::is_null))
}

fn validate_instance_metadata(
    row: &[Value],
    mapping: &MappingDescriptorV1,
) -> Result<(), AdapterError> {
    let schema_version = row.get(2).and_then(Value::as_u64);
    let mapping_name = row.get(3).and_then(Value::as_str);
    let mapping_version = row.get(4).and_then(Value::as_str);
    let mapping_fingerprint = row.get(5).and_then(Value::as_str);
    if schema_version != Some(u64::from(NEO4J_SCHEMA_VERSION))
        || mapping_name != Some(mapping.name())
        || mapping_version != Some(mapping.version())
        || mapping_fingerprint != Some(hex(&mapping.schema_fingerprint()).as_str())
    {
        return Err(AdapterError::Backend(
            "Neo4j Mapping schema metadata differs from the current descriptor".into(),
        ));
    }
    Ok(())
}

fn value_string<'a>(value: Option<&'a Value>, field: &str) -> Result<&'a str, AdapterError> {
    value.and_then(Value::as_str).ok_or_else(|| {
        AdapterError::Backend(format!("Neo4j result is missing string field {field}"))
    })
}

fn factory_error(error: impl ToString) -> AdapterFactoryError {
    AdapterFactoryError::new(error.to_string())
}

fn decode_base64(value: &Value) -> Result<Vec<u8>, AdapterError> {
    BASE64
        .decode(value_string(Some(value), "base64 value")?)
        .map_err(|error| AdapterError::Backend(format!("invalid Neo4j base64 value: {error}")))
}

fn decode_optional_base64(value: Option<&Value>) -> Result<Option<Vec<u8>>, AdapterError> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(value) => decode_base64(value).map(Some),
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn decode_hex(value: &str) -> Result<Vec<u8>, AdapterError> {
    if !value.len().is_multiple_of(2) {
        return Err(AdapterError::Backend(
            "invalid Neo4j hexadecimal key".into(),
        ));
    }
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let pair = std::str::from_utf8(pair)
                .map_err(|_| AdapterError::Backend("invalid Neo4j hexadecimal key".into()))?;
            u8::from_str_radix(pair, 16)
                .map_err(|_| AdapterError::Backend("invalid Neo4j hexadecimal key".into()))
        })
        .collect()
}

fn u64_hex(value: u64) -> String {
    format!("{value:016x}")
}

fn u128_hex(value: u128) -> String {
    format!("{value:032x}")
}

fn parse_u64_hex(value: &str) -> Result<u64, AdapterError> {
    u64::from_str_radix(value, 16)
        .map_err(|_| AdapterError::Backend("invalid Neo4j applied-index value".into()))
}

fn decode_keyspace(tag: u8) -> Result<Keyspace, AdapterError> {
    Keyspace::ALL
        .into_iter()
        .find(|keyspace| keyspace.tag() == tag)
        .ok_or_else(|| AdapterError::Backend(format!("invalid Neo4j keyspace tag {tag}")))
}

fn snapshot_entry_bytes(entry: &KeyValue) -> usize {
    1_usize
        .saturating_add(8)
        .saturating_add(entry.key().as_bytes().len())
        .saturating_add(8)
        .saturating_add(entry.value().len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;
    use std::io::{Cursor, Write as _};
    use std::net::TcpListener;
    use std::sync::mpsc;
    use std::task::{Context, Poll, Wake, Waker};
    use std::thread;
    use std::time::Instant;

    #[test]
    fn graph_contract_uses_reserved_labels_constraints_and_parameterized_cypher() {
        assert!(NEO4J_INSTANCE_CONSTRAINT.contains("IS UNIQUE"));
        assert!(NEO4J_RECORD_CONSTRAINT.contains("instance_id"));
        assert!(NEO4J_ENDPOINT_CONSTRAINT.contains("element_hex"));
        assert!(!APPLY_CYPHER.contains("DTGProxyKV"));
        assert!(APPLY_CYPHER.contains("DTGVertexEndpoint"));
        assert!(APPLY_CYPHER.contains("DTG_EDGE"));
        assert!(APPLY_CYPHER.contains("$mutations"));
        assert!(APPLY_CYPHER.contains("$expected_index_hex"));
        assert!(!APPLY_CYPHER.contains("value_base64: '"));
    }

    #[test]
    fn committed_apply_acquires_the_instance_fence_before_other_matches() {
        for statement in [APPLY_CYPHER, APPLY_EMPTY_CYPHER] {
            let fence = statement
                .find("SET instance.__dtgproxy_snapshot_fence")
                .expect("apply must acquire the snapshot write fence");
            let next_match = statement[fence..]
                .find("OPTIONAL MATCH")
                .map(|offset| fence + offset)
                .expect("apply includes its existing-log lookup");
            let with_instance = statement[fence..next_match]
                .find("WITH instance")
                .map(|offset| fence + offset)
                .expect("apply must carry the fenced instance into the next query part");
            assert!(fence < with_instance && with_instance < next_match);
        }
    }

    #[test]
    fn canonical_identity_records_map_to_native_labels_and_edge_endpoints() {
        let graph = temporal_storage::GraphId::new(7);
        let partition = temporal_storage::PartitionId::new(3);
        let vertex = temporal_storage::ElementRef::vertex(
            graph,
            partition,
            temporal_storage::ElementId::new(11),
        );
        let vertex_identity =
            temporal_storage::VertexIdentity::new(vertex, temporal_storage::LabelId::new(9))
                .unwrap();
        let vertex_json = native_record_json(
            &temporal_storage::vertex_identity_key(vertex),
            Some(&vertex_identity.encode()),
            true,
        )
        .unwrap();
        assert_eq!(vertex_json["label"], "DTGVertexIdentity");
        assert_eq!(vertex_json["kind"], "vertex_identity");

        let edge = temporal_storage::ElementRef::edge(
            graph,
            partition,
            temporal_storage::ElementId::new(21),
        );
        let destination = temporal_storage::ElementRef::vertex(
            graph,
            temporal_storage::PartitionId::new(4),
            temporal_storage::ElementId::new(12),
        );
        let edge_identity = temporal_storage::EdgeIdentity::new_between(
            edge,
            temporal_storage::EdgeTypeId::new(5),
            vertex,
            destination,
        )
        .unwrap();
        let edge_json = native_record_json(
            &temporal_storage::edge_identity_key(edge),
            Some(&edge_identity.encode()),
            true,
        )
        .unwrap();
        assert_eq!(edge_json["label"], "DTGEdgeIdentity");
        assert_eq!(edge_json["has_edge_endpoints"], true);
        assert_eq!(edge_json["destination_partition_hex"], "00000004");
    }

    #[test]
    fn restore_cleanup_is_fenced_to_unpublished_instances() {
        assert!(DELETE_UNPUBLISHED_NAMESPACE_CYPHER.contains("published: false"));
        assert!(!DELETE_UNPUBLISHED_NAMESPACE_CYPHER.contains("published: true"));
    }

    #[test]
    fn hexadecimal_key_encoding_preserves_binary_order() {
        let mut keys = vec![vec![0xff], vec![0, 1], vec![0], vec![1]];
        let mut encoded = keys.iter().map(|key| hex(key)).collect::<Vec<_>>();
        keys.sort();
        encoded.sort();
        assert_eq!(encoded, keys.iter().map(|key| hex(key)).collect::<Vec<_>>());
        for key in keys {
            assert_eq!(decode_hex(&hex(&key)).unwrap(), key);
        }
    }

    #[test]
    fn configuration_rejects_non_http_endpoint_and_unsafe_database_name() {
        let request = AdapterOpenRequest::new("test")
            .with_parameter("endpoint", "bolt://localhost:7687")
            .with_secret("password", adapter_registry::SecretString::new("secret"));
        assert!(Neo4jConfiguration::from_request(&request).is_err());
        let request = AdapterOpenRequest::new("test")
            .with_parameter("endpoint", "http://localhost:7474")
            .with_parameter("database", "neo4j/other")
            .with_secret("password", adapter_registry::SecretString::new("secret"));
        assert!(Neo4jConfiguration::from_request(&request).is_err());
    }

    #[test]
    fn optional_match_null_cells_decode_as_missing_values() {
        assert_eq!(decode_optional_base64(None).unwrap(), None);
        assert_eq!(decode_optional_base64(Some(&Value::Null)).unwrap(), None);
        assert_eq!(
            decode_optional_base64(Some(&Value::String("cGF5bG9hZA==".into()))).unwrap(),
            Some(b"payload".to_vec())
        );
    }

    #[test]
    fn scan_json_is_charged_one_row_at_a_time_before_retention() {
        let span = KeySpan::prefix(Keyspace::Current, Vec::new())
            .with_max_bytes(1)
            .unwrap();
        let body = br#"{"data":{"values":[["61","Yg=="],["63","ZA=="]]},"errors":[]}"#;

        assert_eq!(
            decode_scan_body(Cursor::new(body), &span, 4096),
            Err(AdapterError::ScanByteLimit {
                limit: 1,
                required: 2,
            })
        );
    }

    #[test]
    fn default_projection_budget_caps_the_body_without_rejecting_a_small_response() {
        let span = KeySpan::prefix(Keyspace::Current, Vec::new())
            .with_limit(1_000_000)
            .unwrap()
            .with_max_bytes(128 << 20)
            .unwrap();
        let body_limit = neo4j_scan_body_limit(&span).expect("bounded Neo4j response");
        let body = br#"{"data":{"values":[]},"errors":[]}"#;

        assert_eq!(body_limit, MAX_NEO4J_SCAN_BODY_BYTES);
        assert_eq!(
            decode_scan_body(Cursor::new(body), &span, body_limit),
            Ok(Vec::new())
        );
    }

    #[test]
    fn scan_body_limit_reports_the_first_actual_wire_byte_above_the_cap() {
        let span = KeySpan::prefix(Keyspace::Current, Vec::new())
            .with_limit(1_000_000)
            .unwrap()
            .with_max_bytes(128 << 20)
            .unwrap();

        assert_eq!(
            decode_scan_body(std::io::repeat(b' '), &span, MAX_NEO4J_SCAN_BODY_BYTES),
            Err(AdapterError::ScanResponseByteLimit {
                limit: MAX_NEO4J_SCAN_BODY_BYTES,
                required: MAX_NEO4J_SCAN_BODY_BYTES + 1,
            })
        );
    }

    #[test]
    fn query_read_snapshot_reuses_one_transaction_and_rolls_back_on_drop() {
        let (endpoint, requests, server) = mock_query_api(vec![
            MockResponse::json_with_affinity(
                r#"{"data":{"values":[["000000000000002a"]]},"errors":[],"transaction":{"id":"tx-42","expires":"2099-01-01T00:00:00Z"}}"#,
                "member-7",
            ),
            MockResponse::json(
                r#"{"data":{"values":[[0,"dmFsdWU="]]},"errors":[],"transaction":{"id":"tx-42"}}"#,
            ),
            MockResponse::json(
                r#"{"data":{"values":[["6b","dmFsdWU="]]},"errors":[],"transaction":{"id":"tx-42"}}"#,
            ),
            MockResponse::json(
                r#"{"data":{"values":[[0,"dmFsdWU="]]},"errors":[],"transaction":{"id":"tx-42"}}"#,
            ),
            MockResponse::json(r#"{"errors":[]}"#),
        ]);
        let adapter = test_adapter(&endpoint);

        let snapshot = block_on(TemporalBackendMapping::begin_read_snapshot(&adapter))
            .expect("Neo4j should open an explicit read transaction");
        assert_eq!(snapshot.applied_log_index(), 42);
        assert_eq!(
            block_on(snapshot.multi_get(&[test_key(b"k")])).unwrap(),
            vec![Some(b"value".to_vec())]
        );
        assert_eq!(
            block_on(snapshot.scan(&KeySpan::prefix(Keyspace::TemporalIndex, b"k".to_vec(),)))
                .unwrap(),
            vec![KeyValue::new(test_key(b"k"), b"value".to_vec())]
        );
        assert_eq!(
            block_on(snapshot.multi_get(&[test_key(b"k")])).unwrap(),
            vec![Some(b"value".to_vec())]
        );
        drop(snapshot);

        let requests = requests.into_iter().collect::<Vec<_>>();
        server.join().unwrap();
        assert_eq!(requests.len(), 5);
        assert_eq!(requests[0].method, "POST");
        assert_eq!(requests[0].path, "/db/neo4j/query/v2/tx");
        assert!(
            requests[0]
                .body
                .contains("SET instance.__dtgproxy_snapshot_fence")
        );
        for request in &requests[1..4] {
            assert_eq!(request.method, "POST");
            assert_eq!(request.path, "/db/neo4j/query/v2/tx/tx-42");
            assert_eq!(request.header("neo4j-cluster-affinity"), Some("member-7"));
        }
        assert_eq!(requests[4].method, "DELETE");
        assert_eq!(requests[4].path, "/db/neo4j/query/v2/tx/tx-42");
        assert_eq!(
            requests[4].header("neo4j-cluster-affinity"),
            Some("member-7")
        );
    }

    #[test]
    fn canonical_scan_uses_transaction_limit_and_strict_continuation() {
        let (endpoint, requests, server) = mock_query_api(vec![
            MockResponse::json_with_affinity(
                r#"{"data":{"values":[["000000000000002a"]]},"errors":[],"transaction":{"id":"tx-page"}}"#,
                "member-9",
            ),
            MockResponse::json(
                r#"{"data":{"values":[["61","MQ=="],["62","Mg=="],["63","Mw=="]]},"errors":[],"transaction":{"id":"tx-page"}}"#,
            ),
            MockResponse::json(r#"{"errors":[]}"#),
        ]);
        let adapter = test_adapter(&endpoint);
        let snapshot = block_on(StorageAdapter::begin_read_snapshot(&adapter)).unwrap();
        let request = CanonicalScanRequest::new(
            KeySpan::prefix(Keyspace::TemporalIndex, Vec::new()),
            storage_api::QueryPageBounds::new(2, 64).unwrap(),
        )
        .unwrap();

        let page = block_on(snapshot.scan_canonical(&request)).unwrap();

        assert_eq!(page.applied_log_index(), 42);
        assert_eq!(
            page.entries(),
            &[
                KeyValue::new(test_key(b"a"), b"1".to_vec()),
                KeyValue::new(test_key(b"b"), b"2".to_vec()),
            ]
        );
        assert_eq!(page.next_start(), Some(&test_key(b"c")));
        drop(snapshot);
        server.join().unwrap();
        let requests = requests.into_iter().collect::<Vec<_>>();
        assert_eq!(requests[1].path, "/db/neo4j/query/v2/tx/tx-page");
        assert_eq!(
            requests[1].header("neo4j-cluster-affinity"),
            Some("member-9")
        );
        let body: Value = serde_json::from_str(&requests[1].body).unwrap();
        assert_eq!(body["parameters"]["limit"], 3);
        assert!(
            body["statement"]
                .as_str()
                .unwrap()
                .contains("ORDER BY record.logical_key_hex LIMIT $limit")
        );
    }

    #[test]
    fn canonical_scan_stops_before_exceeding_byte_budget() {
        let (endpoint, _requests, server) = mock_query_api(vec![
            MockResponse::json(
                r#"{"data":{"values":[["0000000000000007"]]},"errors":[],"transaction":{"id":"tx-bytes"}}"#,
            ),
            MockResponse::json(
                r#"{"data":{"values":[["61","MTI="],["62","MzQ1Ng=="]]},"errors":[],"transaction":{"id":"tx-bytes"}}"#,
            ),
            MockResponse::json(r#"{"errors":[]}"#),
        ]);
        let adapter = test_adapter(&endpoint);
        let snapshot = block_on(StorageAdapter::begin_read_snapshot(&adapter)).unwrap();
        let request = CanonicalScanRequest::new(
            KeySpan::prefix(Keyspace::TemporalIndex, Vec::new()),
            storage_api::QueryPageBounds::new(8, 4).unwrap(),
        )
        .unwrap();

        let page = block_on(snapshot.scan_canonical(&request)).unwrap();

        assert_eq!(
            page.entries(),
            &[KeyValue::new(test_key(b"a"), b"12".to_vec())]
        );
        assert_eq!(page.next_start(), Some(&test_key(b"b")));
        drop(snapshot);
        server.join().unwrap();
    }

    #[test]
    fn canonical_scan_rejects_first_entry_over_byte_budget() {
        let request = CanonicalScanRequest::new(
            KeySpan::prefix(Keyspace::TemporalIndex, Vec::new()),
            storage_api::QueryPageBounds::new(8, 2).unwrap(),
        )
        .unwrap();

        assert_eq!(
            bounded_canonical_page(
                vec![vec![
                    Value::String("61".into()),
                    Value::String("MTI=".into())
                ]],
                &request,
            ),
            Err(AdapterError::ScanByteLimit {
                limit: 2,
                required: 3,
            })
        );
    }

    #[test]
    fn canonical_scan_rejects_noncanonical_backend_order() {
        let request = CanonicalScanRequest::new(
            KeySpan::prefix(Keyspace::TemporalIndex, Vec::new()),
            storage_api::QueryPageBounds::new(8, 64).unwrap(),
        )
        .unwrap();
        let rows = vec![
            vec![Value::String("62".into()), Value::String("Mg==".into())],
            vec![Value::String("61".into()), Value::String("MQ==".into())],
        ];

        assert!(matches!(
            bounded_canonical_page(rows, &request),
            Err(AdapterError::Backend(message)) if message.contains("canonical order")
        ));
    }

    #[test]
    fn typed_query_primitives_are_parameterized_bounded_candidates() {
        for statement in [
            CANDIDATE_SCAN_CYPHER,
            PROPERTY_GATHER_CYPHER,
            ADJACENCY_EXPAND_CYPHER,
            CHANGE_SCAN_CYPHER,
        ] {
            assert!(statement.contains("$instance_id"));
            assert!(!statement.contains("snapshot-test"));
            assert!(!statement.contains(" LIMIT 100"));
        }
        assert!(CANDIDATE_SCAN_CYPHER.contains("$start_hex"));
        assert!(CANDIDATE_SCAN_CYPHER.contains("$end_hex"));
        assert!(CANDIDATE_SCAN_CYPHER.contains("$valid_time_micros"));
        assert!(CANDIDATE_SCAN_CYPHER.contains("all(constraint IN $constraints"));
        assert!(CANDIDATE_SCAN_CYPHER.contains("record.property_equal_tokens IS NULL"));
        assert!(CANDIDATE_SCAN_CYPHER.contains("ORDER BY record.logical_key_hex"));
        assert!(CANDIDATE_SCAN_CYPHER.contains("LIMIT $limit"));
        assert!(PROPERTY_GATHER_CYPHER.contains("UNWIND $keys"));
        assert!(PROPERTY_GATHER_CYPHER.contains("ORDER BY requested.ordinal"));
        assert!(ADJACENCY_EXPAND_CYPHER.contains("UNWIND $spans"));
        assert!(ADJACENCY_EXPAND_CYPHER.contains("ORDER BY requested.ordinal"));
        assert!(ADJACENCY_EXPAND_CYPHER.contains("LIMIT $limit"));
        assert!(CHANGE_SCAN_CYPHER.contains("$keyspace"));
        assert!(APPLY_CYPHER.contains("record.valid_min_micros"));
        assert!(APPLY_CYPHER.contains("record.property_equal_tokens"));
        assert!(RESTORE_CYPHER.contains("record.valid_min_micros"));
        assert!(RESTORE_CYPHER.contains("record.property_equal_tokens"));
        assert!(BEGIN_READ_SNAPSHOT_CYPHER.contains("$instance_id"));
        assert!(BEGIN_READ_SNAPSHOT_CYPHER.contains("instance.applied_index_hex"));
    }

    #[test]
    fn candidate_scan_passes_typed_constraints_as_parameters() {
        let (endpoint, requests, server) = mock_query_api(vec![
            MockResponse::json(
                r#"{"data":{"values":[["0000000000000003"]]},"errors":[],"transaction":{"id":"tx-constraints"}}"#,
            ),
            MockResponse::json(
                r#"{"data":{"values":[]},"errors":[],"transaction":{"id":"tx-constraints"}}"#,
            ),
            MockResponse::json(r#"{"errors":[]}"#),
        ]);
        let adapter = test_adapter(&endpoint);
        let snapshot = block_on(StorageAdapter::begin_read_snapshot(&adapter)).unwrap();
        let request = CandidateScanRequest::new(
            KeySpan::prefix(Keyspace::Current, Vec::new()),
            temporal_types::ValidTime::from_micros(17),
            vec![storage_api::PropertyConstraint::new(
                storage_api::PropertyId::new(9),
                storage_api::ComparisonOperator::Equal,
                temporal_types::GraphValue::String("active".into()),
            )],
            QueryPageBounds::new(4, 256).unwrap(),
        )
        .unwrap();

        block_on(snapshot.scan_candidates(&request)).unwrap();
        drop(snapshot);

        server.join().unwrap();
        let requests = requests.into_iter().collect::<Vec<_>>();
        let body: Value = serde_json::from_str(&requests[1].body).unwrap();
        assert_eq!(body["parameters"]["valid_time_micros"], 17);
        assert_eq!(body["parameters"]["constraints"][0]["property_id"], 9);
        assert_eq!(body["parameters"]["constraints"][0]["operator"], "equal");
        assert_eq!(
            body["parameters"]["constraints"][0]["value"],
            serde_json::json!({"type": "string", "value": "active"})
        );
        assert!(!body["statement"].as_str().unwrap().contains("active"));
    }

    #[test]
    fn typed_query_primitives_advertise_only_candidate_guarantees() {
        let adapter = test_adapter("http://127.0.0.1:1");
        let capabilities = adapter.query_primitive_capabilities();
        assert_eq!(capabilities.candidate_scan(), PushdownGuarantee::Candidate);
        assert_eq!(capabilities.property_gather(), PushdownGuarantee::Candidate);
        assert_eq!(
            capabilities.adjacency_expand(),
            PushdownGuarantee::Candidate
        );
        assert_eq!(capabilities.change_scan(), PushdownGuarantee::Candidate);
        assert!(adapter.capabilities().predicate_pushdown);
        assert!(adapter.capabilities().adjacency_pushdown);
        assert!(!adapter.capabilities().change_feed);
        assert!(neo4j_mapping_descriptor().capabilities().predicate_pushdown);
        assert!(neo4j_mapping_descriptor().capabilities().adjacency_pushdown);
        assert!(!neo4j_mapping_descriptor().capabilities().change_feed);
    }

    #[test]
    fn typed_candidate_and_change_pages_preserve_bounds_continuations_and_snapshot_index() {
        let (endpoint, requests, server) = mock_query_api(vec![
            MockResponse::json_with_affinity(
                r#"{"data":{"values":[["000000000000002a"]]},"errors":[],"transaction":{"id":"tx-typed"}}"#,
                "member-typed",
            ),
            MockResponse::json(
                r#"{"data":{"values":[["61","MQ=="],["62","Mg=="],["63","Mw=="]]},"errors":[],"transaction":{"id":"tx-typed"}}"#,
            ),
            MockResponse::json(
                r#"{"data":{"values":[["78","NA=="],["79","NQ=="]]},"errors":[],"transaction":{"id":"tx-typed"}}"#,
            ),
            MockResponse::json(r#"{"errors":[]}"#),
        ]);
        let adapter = test_adapter(&endpoint);
        let snapshot = block_on(StorageAdapter::begin_read_snapshot(&adapter)).unwrap();
        let candidate = CandidateScanRequest::new(
            KeySpan::prefix(Keyspace::Current, Vec::new()),
            temporal_types::ValidTime::from_micros(7),
            Vec::new(),
            QueryPageBounds::new(2, 64).unwrap(),
        )
        .unwrap();
        let changes = ChangeScanRequest::new(
            KeySpan::prefix(Keyspace::TemporalIndex, Vec::new()),
            QueryPageBounds::new(8, 64).unwrap(),
        )
        .unwrap();

        let candidate_page = block_on(snapshot.scan_candidates(&candidate)).unwrap();
        let change_page = block_on(snapshot.scan_changes(&changes)).unwrap();

        assert_eq!(candidate_page.applied_log_index(), 42);
        assert_eq!(candidate_page.guarantee(), PushdownGuarantee::Candidate);
        assert_eq!(candidate_page.entries().len(), 2);
        assert_eq!(
            candidate_page.next_start(),
            Some(&LogicalKey::in_keyspace(Keyspace::Current, b"c".to_vec()))
        );
        assert_eq!(change_page.applied_log_index(), 42);
        assert_eq!(change_page.guarantee(), PushdownGuarantee::Candidate);
        assert_eq!(change_page.entries().len(), 2);
        assert_eq!(change_page.next_start(), None);
        drop(snapshot);

        server.join().unwrap();
        let requests = requests.into_iter().collect::<Vec<_>>();
        let candidate_body: Value = serde_json::from_str(&requests[1].body).unwrap();
        assert_eq!(candidate_body["parameters"]["limit"], 3);
        assert_eq!(
            candidate_body["parameters"]["keyspace"],
            Keyspace::Current.tag()
        );
        let change_body: Value = serde_json::from_str(&requests[2].body).unwrap();
        assert_eq!(change_body["parameters"]["limit"], 9);
        assert_eq!(
            change_body["parameters"]["keyspace"],
            Keyspace::TemporalIndex.tag()
        );
        assert_eq!(requests[3].method, "DELETE");
    }

    #[test]
    fn typed_property_and_adjacency_queries_preserve_request_order_and_candidate_status() {
        let (endpoint, requests, server) = mock_query_api(vec![
            MockResponse::json_with_affinity(
                r#"{"data":{"values":[["000000000000000b"]]},"errors":[],"transaction":{"id":"tx-properties"}}"#,
                "member-properties",
            ),
            MockResponse::json(
                r#"{"data":{"values":[[0,null],[1,null]]},"errors":[],"transaction":{"id":"tx-properties"}}"#,
            ),
            MockResponse::json(r#"{"errors":[]}"#),
            MockResponse::json_with_affinity(
                r#"{"data":{"values":[["000000000000000b"]]},"errors":[],"transaction":{"id":"tx-adjacency"}}"#,
                "member-adjacency",
            ),
            MockResponse::json(
                r#"{"data":{"values":[[0,"61","MQ=="],[1,"6261","Mg=="],[1,"6262","Mw=="]]},"errors":[],"transaction":{"id":"tx-adjacency"}}"#,
            ),
            MockResponse::json(r#"{"errors":[]}"#),
        ]);
        let adapter = test_adapter(&endpoint);
        let property_request = PropertyGatherRequest::new(
            vec![
                LogicalKey::in_keyspace(Keyspace::Current, b"first".to_vec()),
                LogicalKey::in_keyspace(Keyspace::Current, b"second".to_vec()),
            ],
            vec![storage_api::PropertyId::new(1)],
            QueryPageBounds::new(4, 256).unwrap(),
        )
        .unwrap();
        let adjacency_request = AdjacencyExpandRequest::new(
            vec![
                KeySpan::prefix(Keyspace::AdjOut, b"a".to_vec()),
                KeySpan::prefix(Keyspace::AdjOut, b"b".to_vec()),
            ],
            QueryPageBounds::new(2, 64).unwrap(),
        )
        .unwrap();

        let properties = block_on(adapter.gather_properties(&property_request)).unwrap();
        let adjacency = block_on(adapter.expand_adjacency(&adjacency_request)).unwrap();

        assert_eq!(properties.applied_log_index(), 11);
        assert_eq!(properties.guarantee(), PushdownGuarantee::Candidate);
        assert_eq!(properties.rows().len(), 2);
        assert!(properties.rows().iter().all(|row| row.values() == [None]));
        assert_eq!(adjacency.applied_log_index(), 11);
        assert_eq!(adjacency.guarantee(), PushdownGuarantee::Candidate);
        assert_eq!(adjacency.entries().len(), 2);
        assert_eq!(adjacency.entries()[0].input_ordinal(), 0);
        assert_eq!(adjacency.entries()[1].input_ordinal(), 1);
        assert_eq!(
            adjacency.next(),
            Some(&storage_api::AdjacencyCursor::new(
                1,
                LogicalKey::in_keyspace(Keyspace::AdjOut, b"bb".to_vec())
            ))
        );

        server.join().unwrap();
        let requests = requests.into_iter().collect::<Vec<_>>();
        let property_body: Value = serde_json::from_str(&requests[1].body).unwrap();
        assert_eq!(property_body["parameters"]["keys"][0]["ordinal"], 0);
        assert_eq!(property_body["parameters"]["keys"][1]["ordinal"], 1);
        let adjacency_body: Value = serde_json::from_str(&requests[4].body).unwrap();
        assert_eq!(adjacency_body["parameters"]["limit"], 3);
        assert_eq!(adjacency_body["parameters"]["spans"][0]["ordinal"], 0);
        assert_eq!(adjacency_body["parameters"]["spans"][1]["ordinal"], 1);
    }

    #[test]
    fn failed_snapshot_begin_rolls_back_the_open_transaction() {
        let (endpoint, requests, server) = mock_query_api(vec![
            MockResponse::json_with_affinity(
                r#"{"data":{"values":[]},"errors":[{"code":"Neo.ClientError.Statement.SyntaxError"}],"transaction":{"id":"tx-failed"}}"#,
                "member-8",
            ),
            MockResponse::json(r#"{"errors":[]}"#),
        ]);
        let adapter = test_adapter(&endpoint);

        let result = block_on(StorageAdapter::begin_read_snapshot(&adapter));
        assert!(matches!(result, Err(AdapterError::Backend(_))));

        server.join().unwrap();
        let requests = requests.into_iter().collect::<Vec<_>>();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[1].method, "DELETE");
        assert_eq!(requests[1].path, "/db/neo4j/query/v2/tx/tx-failed");
        assert_eq!(
            requests[1].header("neo4j-cluster-affinity"),
            Some("member-8")
        );
    }

    #[test]
    fn fenced_scan_reports_the_index_pinned_by_its_transaction() {
        let (endpoint, requests, server) = mock_query_api(vec![
            MockResponse::json(
                r#"{"data":{"values":[["0000000000000007"]]},"errors":[],"transaction":{"id":"tx-scan"}}"#,
            ),
            MockResponse::json(
                r#"{"data":{"values":[["6b","dmFsdWU="]]},"errors":[],"transaction":{"id":"tx-scan"}}"#,
            ),
            MockResponse::json(r#"{"errors":[]}"#),
        ]);
        let adapter = test_adapter(&endpoint);

        let scan = block_on(StorageAdapter::scan_fenced(
            &adapter,
            &KeySpan::prefix(Keyspace::TemporalIndex, b"k".to_vec()),
        ))
        .unwrap();

        assert_eq!(scan.applied_log_index(), 7);
        assert_eq!(
            scan.entries(),
            &[KeyValue::new(test_key(b"k"), b"value".to_vec())]
        );
        server.join().unwrap();
        let requests = requests.into_iter().collect::<Vec<_>>();
        assert_eq!(requests.len(), 3);
        assert_eq!(requests[0].path, "/db/neo4j/query/v2/tx");
        assert_eq!(requests[1].path, "/db/neo4j/query/v2/tx/tx-scan");
        assert_eq!(requests[2].method, "DELETE");
    }

    fn test_adapter(endpoint: &str) -> Neo4jAdapter {
        Neo4jAdapter {
            client: QueryApiClient::new(Neo4jConfiguration {
                endpoint: endpoint.to_owned(),
                database: "neo4j".into(),
                username: "neo4j".into(),
                password: "secret".into(),
                timeout: Duration::from_secs(2),
            })
            .unwrap(),
            instance_id: "snapshot-test".into(),
            apply_guard: Mutex::new(()),
        }
    }

    fn test_key(bytes: &[u8]) -> LogicalKey {
        LogicalKey::in_keyspace(Keyspace::TemporalIndex, bytes.to_vec())
    }

    #[derive(Debug)]
    struct MockRequest {
        method: String,
        path: String,
        headers: Vec<(String, String)>,
        body: String,
    }

    impl MockRequest {
        fn header(&self, name: &str) -> Option<&str> {
            self.headers
                .iter()
                .find(|(candidate, _)| candidate.eq_ignore_ascii_case(name))
                .map(|(_, value)| value.as_str())
        }
    }

    struct MockResponse {
        body: &'static str,
        affinity: Option<&'static str>,
    }

    impl MockResponse {
        const fn json(body: &'static str) -> Self {
            Self {
                body,
                affinity: None,
            }
        }

        const fn json_with_affinity(body: &'static str, affinity: &'static str) -> Self {
            Self {
                body,
                affinity: Some(affinity),
            }
        }
    }

    fn mock_query_api(
        responses: Vec<MockResponse>,
    ) -> (String, mpsc::Receiver<MockRequest>, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let (sender, receiver) = mpsc::channel();
        let server = thread::spawn(move || {
            for response in responses {
                let deadline = Instant::now() + Duration::from_secs(2);
                let mut stream = loop {
                    match listener.accept() {
                        Ok((stream, _)) => {
                            stream.set_nonblocking(false).unwrap();
                            break stream;
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            if Instant::now() >= deadline {
                                return;
                            }
                            thread::sleep(Duration::from_millis(5));
                        }
                        Err(error) => panic!("mock Query API accept failed: {error}"),
                    }
                };
                let request = read_mock_request(&mut stream);
                sender.send(request).unwrap();
                let affinity = response.affinity.map_or_else(String::new, |value| {
                    format!("neo4j-cluster-affinity: {value}\r\n")
                });
                write!(
                    stream,
                    "HTTP/1.1 202 Accepted\r\nContent-Type: application/json\r\nContent-Length: {}\r\n{}Connection: close\r\n\r\n{}",
                    response.body.len(),
                    affinity,
                    response.body,
                )
                .unwrap();
            }
        });
        (endpoint, receiver, server)
    }

    fn read_mock_request(stream: &mut impl Read) -> MockRequest {
        let mut bytes = Vec::new();
        let mut chunk = [0_u8; 1024];
        let header_end = loop {
            let read = stream.read(&mut chunk).unwrap();
            assert_ne!(read, 0, "request ended before headers");
            bytes.extend_from_slice(&chunk[..read]);
            if let Some(position) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
                break position + 4;
            }
        };
        let headers = String::from_utf8(bytes[..header_end].to_vec()).unwrap();
        let mut lines = headers.split("\r\n");
        let mut request_line = lines.next().unwrap().split_whitespace();
        let method = request_line.next().unwrap().to_owned();
        let path = request_line.next().unwrap().to_owned();
        let headers = lines
            .filter_map(|line| line.split_once(':'))
            .map(|(name, value)| (name.to_owned(), value.trim().to_owned()))
            .collect::<Vec<_>>();
        let content_length = headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
            .map_or(0, |(_, value)| value.parse().unwrap());
        while bytes.len() < header_end + content_length {
            let read = stream.read(&mut chunk).unwrap();
            assert_ne!(read, 0, "request ended before body");
            bytes.extend_from_slice(&chunk[..read]);
        }
        MockRequest {
            method,
            path,
            headers,
            body: String::from_utf8(bytes[header_end..header_end + content_length].to_vec())
                .unwrap(),
        }
    }

    fn block_on<F: Future>(future: F) -> F::Output {
        struct NoopWake;
        impl Wake for NoopWake {
            fn wake(self: Arc<Self>) {}
        }
        let waker = Waker::from(Arc::new(NoopWake));
        let mut context = Context::from_waker(&waker);
        let mut future = Box::pin(future);
        loop {
            if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
                return output;
            }
            thread::yield_now();
        }
    }
}
