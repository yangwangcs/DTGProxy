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
    AdapterFuture, ApplyReceipt, BackendFamily, CanonicalRestoreSession, CommittedMutationBatch,
    Durability, KeySpan, KeyValue, Keyspace, LogicalKey, LogicalSnapshotAccumulator,
    LogicalSnapshotChunkV1, LogicalSnapshotError, LogicalSnapshotExportRequest,
    LogicalSnapshotHeaderV1, LogicalSnapshotManifestV1, LogicalSnapshotReader,
    MappingBackedAdapter, MappingCapabilities, MappingDescriptorV1, MappingFuture,
    MappingRequirement, MutationOperation, PreparedMappingTransaction, SnapshotCapability,
    StorageAdapter, TemporalBackendMapping, new_logical_snapshot_id,
};
use temporal_storage::{
    CanonicalGraphEntry, GraphKey, decode_canonical_graph_entry, decode_graph_key,
};

pub const NEO4J_SCHEMA_VERSION: u16 = 1;
pub const NEO4J_INSTANCE_CONSTRAINT: &str = "CREATE CONSTRAINT dtgproxy_instance_v1 IF NOT EXISTS FOR (n:DTGProxyInstance) REQUIRE n.instance_id IS UNIQUE";
pub const NEO4J_RECORD_CONSTRAINT: &str = "CREATE CONSTRAINT dtgproxy_record_v1 IF NOT EXISTS FOR (n:DTGCanonicalRecord) REQUIRE (n.instance_id, n.keyspace, n.logical_key_hex) IS UNIQUE";
pub const NEO4J_ENDPOINT_CONSTRAINT: &str = "CREATE CONSTRAINT dtgproxy_endpoint_v1 IF NOT EXISTS FOR (n:DTGVertexEndpoint) REQUIRE (n.instance_id, n.graph_hex, n.partition_hex, n.element_hex) IS UNIQUE";
pub const NEO4J_LOG_CONSTRAINT: &str = "CREATE CONSTRAINT dtgproxy_log_v1 IF NOT EXISTS FOR (n:DTGProxyAppliedLog) REQUIRE (n.instance_id, n.log_index_hex) IS UNIQUE";
pub const NEO4J_MUTATION_CONSTRAINT: &str = "CREATE CONSTRAINT dtgproxy_mutation_v1 IF NOT EXISTS FOR (n:DTGProxyMutation) REQUIRE (n.instance_id, n.txn_id_hex, n.sequence) IS UNIQUE";

fn neo4j_mapping_descriptor() -> MappingDescriptorV1 {
    let mut hasher = Hasher::new();
    hasher.update(b"DTGProxy/Neo4jNativeTemporalMapping/1");
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
        "1.0.0",
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
            predicate_pushdown: false,
            adjacency_pushdown: false,
            change_feed: false,
        },
    )
    .expect("static Neo4j Mapping descriptor is valid")
}

pub const APPLY_CYPHER: &str = r#"
MATCH (instance:DTGProxyInstance {instance_id: $instance_id, published: true})
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
            let implementation_version = env!("CARGO_PKG_VERSION");
            let adapter = MappingBackedAdapter::with_runtime_identity(
                "neo4j-query-api",
                implementation_version,
                Arc::new(adapter),
                MappingRequirement::HotPluggableReplica,
            )
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
            predicate_pushdown: false,
            adjacency_pushdown: false,
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

    fn apply_committed<'a>(
        &'a self,
        batch: CommittedMutationBatch,
    ) -> AdapterFuture<'a, ApplyReceipt> {
        Box::pin(async move { self.apply(batch) })
    }

    fn multi_get<'a>(&'a self, keys: &'a [LogicalKey]) -> AdapterFuture<'a, Vec<Option<Vec<u8>>>> {
        Box::pin(async move { self.multi_get_values(keys) })
    }

    fn scan<'a>(&'a self, span: &'a KeySpan) -> AdapterFuture<'a, Vec<KeyValue>> {
        Box::pin(async move { self.scan_values(span) })
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
        let response = self
            .agent
            .post(&self.url)
            .set("Accept", "application/json")
            .set("Authorization", &self.authorization)
            .send_json(json!({"statement": statement, "parameters": parameters}));
        let response = match response {
            Ok(response) => response,
            Err(ureq::Error::Status(_, response)) => {
                let status = response.status();
                let body = response.into_string().unwrap_or_default();
                return Err(AdapterError::Backend(format!(
                    "Neo4j Query API returned HTTP {status}: {body}"
                )));
            }
            Err(ureq::Error::Transport(error)) => {
                return Err(AdapterError::Backend(format!(
                    "Neo4j Query API transport failed: {error}"
                )));
            }
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

    fn execute_scan(
        &self,
        statement: &str,
        parameters: Value,
        span: &KeySpan,
    ) -> Result<Vec<KeyValue>, AdapterError> {
        let body_limit = neo4j_scan_body_limit(span)?;
        let response = self
            .agent
            .post(&self.url)
            .set("Accept", "application/json")
            .set("Authorization", &self.authorization)
            .send_json(json!({"statement": statement, "parameters": parameters}));
        let response = match response {
            Ok(response) => response,
            Err(ureq::Error::Status(_, response)) => {
                let status = response.status();
                let body = bounded_error_body(response)?;
                return Err(AdapterError::Backend(format!(
                    "Neo4j Query API returned HTTP {status}: {body}"
                )));
            }
            Err(ureq::Error::Transport(error)) => {
                return Err(AdapterError::Backend(format!(
                    "Neo4j Query API transport failed: {error}"
                )));
            }
        };
        decode_scan_body(response.into_reader(), span, body_limit)
    }
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
            let implementation_version = env!("CARGO_PKG_VERSION");
            let adapter = MappingBackedAdapter::with_runtime_identity(
                "neo4j-query-api",
                implementation_version,
                Arc::new(adapter),
                MappingRequirement::HotPluggableReplica,
            )
            .map_err(factory_error)?;
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
        CanonicalGraphEntry::Current { key, .. } => match *key {
            GraphKey::CurrentVertex(element) => {
                element_fields("vertex_current", "DTGVertexCurrent", element)
            }
            GraphKey::CurrentEdge(element) => {
                element_fields("edge_current", "DTGEdgeCurrent", element)
            }
            _ => unreachable!("canonical Current entry has a Current key"),
        },
        CanonicalGraphEntry::Adjacency { key, .. } => adjacency_fields(*key),
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
    use std::io::Cursor;

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
}
