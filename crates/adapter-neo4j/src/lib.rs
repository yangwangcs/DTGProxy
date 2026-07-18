#![forbid(unsafe_code)]

use std::collections::{BTreeSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use adapter_registry::{
    AdapterFactory, AdapterFactoryError, AdapterFactoryFuture, AdapterOpenRequest,
    AdapterRestoreFuture, AdapterRestoreSession, AdapterRestoreSessionFuture,
};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde_json::{Value, json};
use storage_api::{
    ADAPTER_META_APPLIED_LOG_INDEX_KEY, AdapterCapabilities, AdapterDescriptorV1, AdapterError,
    AdapterFuture, ApplyReceipt, BackendFamily, CommittedMutationBatch, Durability, KeySpan,
    KeyValue, Keyspace, LogicalKey, LogicalSnapshotAccumulator, LogicalSnapshotChunkV1,
    LogicalSnapshotError, LogicalSnapshotExportRequest, LogicalSnapshotHeaderV1,
    LogicalSnapshotManifestV1, LogicalSnapshotReader, MutationOperation, SnapshotCapability,
    StorageAdapter, new_logical_snapshot_id,
};

pub const NEO4J_SCHEMA_VERSION: u16 = 1;
pub const NEO4J_INSTANCE_CONSTRAINT: &str = "CREATE CONSTRAINT dtgproxy_instance_v1 IF NOT EXISTS FOR (n:DTGProxyInstance) REQUIRE n.instance_id IS UNIQUE";
pub const NEO4J_KV_CONSTRAINT: &str = "CREATE CONSTRAINT dtgproxy_kv_v1 IF NOT EXISTS FOR (n:DTGProxyKV) REQUIRE (n.instance_id, n.keyspace, n.logical_key_hex) IS UNIQUE";
pub const NEO4J_LOG_CONSTRAINT: &str = "CREATE CONSTRAINT dtgproxy_log_v1 IF NOT EXISTS FOR (n:DTGProxyAppliedLog) REQUIRE (n.instance_id, n.log_index_hex) IS UNIQUE";
pub const NEO4J_MUTATION_CONSTRAINT: &str = "CREATE CONSTRAINT dtgproxy_mutation_v1 IF NOT EXISTS FOR (n:DTGProxyMutation) REQUIRE (n.instance_id, n.txn_id_hex, n.sequence) IS UNIQUE";

pub const APPLY_CYPHER: &str = "MATCH (instance:DTGProxyInstance {instance_id: $instance_id, published: true}) OPTIONAL MATCH (existing_log:DTGProxyAppliedLog {instance_id: $instance_id, log_index_hex: $log_index_hex}) WITH instance, existing_log, instance.applied_index_hex AS previous_index_hex, (instance.applied_index_hex = $expected_index_hex AND existing_log IS NULL) AS should_apply UNWIND $mutations AS mutation FOREACH (_ IN CASE WHEN should_apply THEN [1] ELSE [] END | MERGE (kv:DTGProxyKV {instance_id: $instance_id, keyspace: mutation.keyspace, logical_key_hex: mutation.logical_key_hex}) SET kv.present = mutation.present, kv.value_base64 = mutation.value_base64 MERGE (fingerprint:DTGProxyMutation {instance_id: $instance_id, txn_id_hex: $txn_id_hex, sequence: mutation.sequence}) SET fingerprint.fingerprint_hex = mutation.fingerprint_hex) WITH DISTINCT instance, existing_log, previous_index_hex, should_apply FOREACH (_ IN CASE WHEN should_apply THEN [1] ELSE [] END | MERGE (log:DTGProxyAppliedLog {instance_id: $instance_id, log_index_hex: $log_index_hex}) SET log.fingerprint_hex = $batch_fingerprint_hex SET instance.applied_index_hex = $log_index_hex) RETURN previous_index_hex, existing_log.fingerprint_hex, should_apply";

const MULTI_GET_CYPHER: &str = "UNWIND $keys AS requested OPTIONAL MATCH (kv:DTGProxyKV {instance_id: $instance_id, keyspace: requested.keyspace, logical_key_hex: requested.logical_key_hex, present: true}) RETURN requested.ordinal, kv.value_base64 ORDER BY requested.ordinal";
const SCAN_CYPHER: &str = "MATCH (kv:DTGProxyKV {instance_id: $instance_id, keyspace: $keyspace, present: true}) WHERE kv.logical_key_hex >= $start_hex AND ($end_hex IS NULL OR kv.logical_key_hex < $end_hex) RETURN kv.logical_key_hex, kv.value_base64 ORDER BY kv.logical_key_hex LIMIT $limit";
const EXPORT_CYPHER: &str = "MATCH (instance:DTGProxyInstance {instance_id: $instance_id, published: true}) OPTIONAL MATCH (kv:DTGProxyKV {instance_id: $instance_id, present: true}) RETURN instance.applied_index_hex, kv.keyspace, kv.logical_key_hex, kv.value_base64 ORDER BY kv.keyspace, kv.logical_key_hex";
const RESTORE_CYPHER: &str = "UNWIND $entries AS entry MERGE (kv:DTGProxyKV {instance_id: $instance_id, keyspace: entry.keyspace, logical_key_hex: entry.logical_key_hex}) SET kv.present = true, kv.value_base64 = entry.value_base64 RETURN count(kv)";

pub struct Neo4jAdapterFactory;

impl AdapterFactory for Neo4jAdapterFactory {
    fn provider_name(&self) -> &str {
        "neo4j"
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
        for statement in [
            NEO4J_INSTANCE_CONSTRAINT,
            NEO4J_KV_CONSTRAINT,
            NEO4J_LOG_CONSTRAINT,
            NEO4J_MUTATION_CONSTRAINT,
        ] {
            client.execute(statement, json!({}))?;
        }
        let rows = client.execute(
            "OPTIONAL MATCH (instance:DTGProxyInstance {instance_id: $instance_id}) RETURN instance.published, instance.applied_index_hex",
            json!({"instance_id": instance_id}),
        )?;
        let existing = rows.first();
        match (mode, existing) {
            (OpenMode::Serving, Some(row))
                if row.first().and_then(Value::as_bool) == Some(true) => {}
            (OpenMode::Serving, _) if existing_instance_missing(existing) => {
                client.execute(
                    "CREATE (:DTGProxyInstance {instance_id: $instance_id, schema_version: $schema_version, applied_index_hex: $zero, published: true})",
                    json!({"instance_id": instance_id, "schema_version": NEO4J_SCHEMA_VERSION, "zero": u64_hex(0)}),
                )?;
            }
            (OpenMode::Serving, _) => {
                return Err(AdapterError::Backend(
                    "Neo4j Adapter instance is an unpublished restore target".into(),
                ));
            }
            (OpenMode::Restore, _) if existing_instance_missing(existing) => {
                client.execute(
                    "CREATE (:DTGProxyInstance {instance_id: $instance_id, schema_version: $schema_version, applied_index_hex: $zero, published: false})",
                    json!({"instance_id": instance_id, "schema_version": NEO4J_SCHEMA_VERSION, "zero": u64_hex(0)}),
                )?;
            }
            (OpenMode::Restore, Some(row))
                if row.first().and_then(Value::as_bool) == Some(false) =>
            {
                client.execute(
                    "MATCH (n {instance_id: $instance_id}) WHERE n:DTGProxyKV OR n:DTGProxyAppliedLog OR n:DTGProxyMutation DETACH DELETE n RETURN count(n)",
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
            .map(|mutation| match &mutation.operation {
                MutationOperation::Put { key, value } => json!({
                    "sequence": mutation.sequence,
                    "fingerprint_hex": u64_hex(mutation.fingerprint()),
                    "keyspace": key.keyspace().tag(),
                    "logical_key_hex": hex(key.as_bytes()),
                    "present": true,
                    "value_base64": BASE64.encode(value),
                }),
                MutationOperation::Delete { key } => json!({
                    "sequence": mutation.sequence,
                    "fingerprint_hex": u64_hex(mutation.fingerprint()),
                    "keyspace": key.keyspace().tag(),
                    "logical_key_hex": hex(key.as_bytes()),
                    "present": false,
                    "value_base64": "",
                }),
            })
            .collect::<Vec<_>>();
        let rows = self.client.execute(
            APPLY_CYPHER,
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
            .map(|row| row.get(1).map(decode_base64).transpose())
            .collect()
    }

    fn scan_values(&self, span: &KeySpan) -> Result<Vec<KeyValue>, AdapterError> {
        let limit = span.limit().unwrap_or(usize::MAX.min(i64::MAX as usize));
        let rows = self.client.execute(
            SCAN_CYPHER,
            json!({
                "instance_id": self.instance_id,
                "keyspace": span.keyspace().tag(),
                "start_hex": hex(span.start()),
                "end_hex": span.end().map(hex),
                "limit": limit,
            }),
        )?;
        let mut values = Vec::with_capacity(rows.len());
        for row in rows {
            let key = decode_hex(value_string(row.first(), "logical key")?)?;
            if !span.contains(&key) {
                break;
            }
            values.push(KeyValue::new(
                LogicalKey::in_keyspace(span.keyspace(), key),
                decode_base64(row.get(1).ok_or_else(|| {
                    AdapterError::Backend("Neo4j scan row is missing value".into())
                })?)?,
            ));
        }
        Ok(values)
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
            values.push(json!({
                "keyspace": entry.key().keyspace().tag(),
                "logical_key_hex": hex(entry.key().as_bytes()),
                "value_base64": BASE64.encode(entry.value()),
            }));
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
            "MATCH (n {instance_id: $instance_id}) WHERE n:DTGProxyInstance OR n:DTGProxyKV OR n:DTGProxyAppliedLog OR n:DTGProxyMutation DETACH DELETE n RETURN count(n)",
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
            self.capabilities(),
        )
    }

    fn capabilities(&self) -> AdapterCapabilities {
        self.capabilities()
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

fn validate_batch(batch: &CommittedMutationBatch) -> Result<(), AdapterError> {
    let mut sequences = BTreeSet::new();
    for mutation in &batch.mutations {
        if !sequences.insert(mutation.sequence) {
            return Err(AdapterError::DuplicateMutationSequence {
                txn_id: batch.txn_id,
                sequence: mutation.sequence,
            });
        }
    }
    if batch.mutations.is_empty() {
        return Err(AdapterError::Backend(
            "Neo4j Adapter cannot apply an empty committed batch".into(),
        ));
    }
    Ok(())
}

fn existing_instance_missing(row: Option<&Vec<Value>>) -> bool {
    row.is_none() || row.is_some_and(|row| row.first().is_none_or(Value::is_null))
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

    #[test]
    fn graph_contract_uses_reserved_labels_constraints_and_parameterized_cypher() {
        assert!(NEO4J_INSTANCE_CONSTRAINT.contains("IS UNIQUE"));
        assert!(NEO4J_KV_CONSTRAINT.contains("instance_id"));
        assert!(APPLY_CYPHER.contains("$mutations"));
        assert!(APPLY_CYPHER.contains("$expected_index_hex"));
        assert!(!APPLY_CYPHER.contains("value_base64: '"));
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
}
