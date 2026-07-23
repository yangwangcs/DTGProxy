#![forbid(unsafe_code)]

use std::collections::BTreeSet;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use adapter_registry::{
    AdapterFactory, AdapterFactoryError, AdapterFactoryFuture, AdapterOpenRequest,
    AdapterRestoreFuture, AdapterRestoreSession, AdapterRestoreSessionFuture,
};
use postgres::{Client, GenericClient, IsolationLevel, NoTls, Transaction};
use storage_api::{
    ADAPTER_META_APPLIED_LOG_INDEX_KEY, AdapterCapabilities, AdapterDescriptorV1, AdapterError,
    AdapterFuture, ApplyReceipt, BackendFamily, CanonicalRestoreSession, CommittedMutationBatch,
    Durability, KeySpan, KeyValue, Keyspace, LogicalKey, LogicalSnapshotAccumulator,
    LogicalSnapshotChunkV1, LogicalSnapshotError, LogicalSnapshotExportRequest,
    LogicalSnapshotHeaderV1, LogicalSnapshotManifestV1, LogicalSnapshotReader,
    MappingBackedAdapter, MappingCapabilities, MappingDescriptorV1, MappingFuture,
    MappingRequirement, MutationOperation, PreparedMappingTransaction, SnapshotCapability,
    StorageAdapter, TemporalBackendMapping, adapter_log_fingerprint_key,
    adapter_mutation_fingerprint_key, new_logical_snapshot_id,
};
use temporal_storage::{
    CanonicalGraphEntry, EdgeIdentity, ElementKind, GraphKey, HistoryEntry, ProjectionRecord,
    VertexIdentity, cross_in_adjacency_key, cross_out_adjacency_key, current_edge_key,
    current_vertex_key, decode_canonical_graph_entry, decode_graph_key, edge_identity_key,
    history_anchor_key, in_adjacency_key, out_adjacency_key, vertex_identity_key,
};
use temporal_types::TransactionTime;

pub const POSTGRES_SCHEMA_VERSION: i32 = 1;
pub const DEFAULT_POOL_SIZE: usize = 8;
pub const MAX_POOL_SIZE: usize = 128;

pub const POSTGRES_SCHEMA: &str = r#"
CREATE SCHEMA IF NOT EXISTS dtgproxy;
CREATE TABLE IF NOT EXISTS dtgproxy.schema_meta (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    schema_version INTEGER NOT NULL,
    mapping_name TEXT NOT NULL,
    mapping_version TEXT NOT NULL,
    schema_fingerprint BYTEA NOT NULL CHECK (octet_length(schema_fingerprint) = 32)
);
INSERT INTO dtgproxy.schema_meta(singleton, schema_version, mapping_name, mapping_version, schema_fingerprint)
VALUES (TRUE, 1, 'postgresql-native-temporal', '1.0.0', decode(repeat('00', 32), 'hex'))
ON CONFLICT (singleton) DO NOTHING;
CREATE TABLE IF NOT EXISTS dtgproxy.adapter_instance (
    instance_id TEXT PRIMARY KEY,
    schema_version INTEGER NOT NULL,
    mapping_fingerprint BYTEA NOT NULL CHECK (octet_length(mapping_fingerprint) = 32),
    applied_log_index BYTEA NOT NULL CHECK (octet_length(applied_log_index) = 8),
    has_applied_index_record BOOLEAN NOT NULL,
    published BOOLEAN NOT NULL
);
CREATE TABLE IF NOT EXISTS dtgproxy.vertex_identity (
    instance_id TEXT NOT NULL REFERENCES dtgproxy.adapter_instance(instance_id) ON DELETE CASCADE,
    graph_id BYTEA NOT NULL CHECK (octet_length(graph_id) = 8),
    partition_id BYTEA NOT NULL CHECK (octet_length(partition_id) = 4),
    vertex_id BYTEA NOT NULL CHECK (octet_length(vertex_id) = 16),
    label_id BYTEA NOT NULL CHECK (octet_length(label_id) = 4),
    PRIMARY KEY (instance_id, graph_id, partition_id, vertex_id)
);
CREATE TABLE IF NOT EXISTS dtgproxy.edge_identity (
    instance_id TEXT NOT NULL REFERENCES dtgproxy.adapter_instance(instance_id) ON DELETE CASCADE,
    graph_id BYTEA NOT NULL CHECK (octet_length(graph_id) = 8),
    partition_id BYTEA NOT NULL CHECK (octet_length(partition_id) = 4),
    edge_id BYTEA NOT NULL CHECK (octet_length(edge_id) = 16),
    edge_type BYTEA NOT NULL CHECK (octet_length(edge_type) = 4),
    source_partition BYTEA NOT NULL CHECK (octet_length(source_partition) = 4),
    source_id BYTEA NOT NULL CHECK (octet_length(source_id) = 16),
    destination_partition BYTEA NOT NULL CHECK (octet_length(destination_partition) = 4),
    destination_id BYTEA NOT NULL CHECK (octet_length(destination_id) = 16),
    PRIMARY KEY (instance_id, graph_id, partition_id, edge_id)
);
CREATE TABLE IF NOT EXISTS dtgproxy.vertex_current (
    instance_id TEXT NOT NULL REFERENCES dtgproxy.adapter_instance(instance_id) ON DELETE CASCADE,
    graph_id BYTEA NOT NULL CHECK (octet_length(graph_id) = 8),
    partition_id BYTEA NOT NULL CHECK (octet_length(partition_id) = 4),
    vertex_id BYTEA NOT NULL CHECK (octet_length(vertex_id) = 16),
    projection BYTEA NOT NULL,
    PRIMARY KEY (instance_id, graph_id, partition_id, vertex_id)
);
CREATE TABLE IF NOT EXISTS dtgproxy.edge_current (
    instance_id TEXT NOT NULL REFERENCES dtgproxy.adapter_instance(instance_id) ON DELETE CASCADE,
    graph_id BYTEA NOT NULL CHECK (octet_length(graph_id) = 8),
    partition_id BYTEA NOT NULL CHECK (octet_length(partition_id) = 4),
    edge_id BYTEA NOT NULL CHECK (octet_length(edge_id) = 16),
    projection BYTEA NOT NULL,
    PRIMARY KEY (instance_id, graph_id, partition_id, edge_id)
);
CREATE TABLE IF NOT EXISTS dtgproxy.history (
    instance_id TEXT NOT NULL REFERENCES dtgproxy.adapter_instance(instance_id) ON DELETE CASCADE,
    graph_id BYTEA NOT NULL CHECK (octet_length(graph_id) = 8),
    partition_id BYTEA NOT NULL CHECK (octet_length(partition_id) = 4),
    element_kind SMALLINT NOT NULL CHECK (element_kind IN (1, 2)),
    element_id BYTEA NOT NULL CHECK (octet_length(element_id) = 16),
    transaction_physical BIGINT NOT NULL,
    transaction_logical BYTEA NOT NULL CHECK (octet_length(transaction_logical) = 4),
    segment_id BYTEA NOT NULL CHECK (octet_length(segment_id) = 4),
    history_value BYTEA NOT NULL,
    PRIMARY KEY (instance_id, graph_id, partition_id, element_kind, element_id, transaction_physical, transaction_logical, segment_id)
);
CREATE TABLE IF NOT EXISTS dtgproxy.out_adjacency (
    instance_id TEXT NOT NULL REFERENCES dtgproxy.adapter_instance(instance_id) ON DELETE CASCADE,
    key_tag SMALLINT NOT NULL CHECK (key_tag IN (16, 18)),
    graph_id BYTEA NOT NULL CHECK (octet_length(graph_id) = 8),
    local_partition BYTEA NOT NULL CHECK (octet_length(local_partition) = 4),
    local_endpoint BYTEA NOT NULL CHECK (octet_length(local_endpoint) = 16),
    edge_type BYTEA NOT NULL CHECK (octet_length(edge_type) = 4),
    bucket BYTEA NOT NULL CHECK (octet_length(bucket) = 2),
    remote_partition BYTEA NOT NULL CHECK (octet_length(remote_partition) = 4),
    remote_endpoint BYTEA NOT NULL CHECK (octet_length(remote_endpoint) = 16),
    edge_partition BYTEA NOT NULL CHECK (octet_length(edge_partition) = 4),
    edge_id BYTEA NOT NULL CHECK (octet_length(edge_id) = 16),
    projection BYTEA NOT NULL,
    PRIMARY KEY (instance_id, key_tag, graph_id, local_partition, local_endpoint, edge_type, bucket, remote_partition, remote_endpoint, edge_partition, edge_id)
);
CREATE TABLE IF NOT EXISTS dtgproxy.in_adjacency (
    instance_id TEXT NOT NULL REFERENCES dtgproxy.adapter_instance(instance_id) ON DELETE CASCADE,
    key_tag SMALLINT NOT NULL CHECK (key_tag IN (17, 19)),
    graph_id BYTEA NOT NULL CHECK (octet_length(graph_id) = 8),
    local_partition BYTEA NOT NULL CHECK (octet_length(local_partition) = 4),
    local_endpoint BYTEA NOT NULL CHECK (octet_length(local_endpoint) = 16),
    edge_type BYTEA NOT NULL CHECK (octet_length(edge_type) = 4),
    bucket BYTEA NOT NULL CHECK (octet_length(bucket) = 2),
    remote_partition BYTEA NOT NULL CHECK (octet_length(remote_partition) = 4),
    remote_endpoint BYTEA NOT NULL CHECK (octet_length(remote_endpoint) = 16),
    edge_partition BYTEA NOT NULL CHECK (octet_length(edge_partition) = 4),
    edge_id BYTEA NOT NULL CHECK (octet_length(edge_id) = 16),
    projection BYTEA NOT NULL,
    PRIMARY KEY (instance_id, key_tag, graph_id, local_partition, local_endpoint, edge_type, bucket, remote_partition, remote_endpoint, edge_partition, edge_id)
);
CREATE TABLE IF NOT EXISTS dtgproxy.opaque_records (
    instance_id TEXT NOT NULL REFERENCES dtgproxy.adapter_instance(instance_id) ON DELETE CASCADE,
    keyspace SMALLINT NOT NULL CHECK (keyspace IN (0, 6, 7)),
    logical_key BYTEA NOT NULL,
    value BYTEA NOT NULL,
    PRIMARY KEY (instance_id, keyspace, logical_key)
);
CREATE TABLE IF NOT EXISTS dtgproxy.replay_log (
    instance_id TEXT NOT NULL REFERENCES dtgproxy.adapter_instance(instance_id) ON DELETE CASCADE,
    log_index BYTEA NOT NULL CHECK (octet_length(log_index) = 8),
    fingerprint BYTEA NOT NULL CHECK (octet_length(fingerprint) = 8),
    PRIMARY KEY (instance_id, log_index)
);
CREATE TABLE IF NOT EXISTS dtgproxy.replay_mutation (
    instance_id TEXT NOT NULL REFERENCES dtgproxy.adapter_instance(instance_id) ON DELETE CASCADE,
    txn_id BYTEA NOT NULL CHECK (octet_length(txn_id) = 16),
    sequence BYTEA NOT NULL CHECK (octet_length(sequence) = 4),
    fingerprint BYTEA NOT NULL CHECK (octet_length(fingerprint) = 8),
    PRIMARY KEY (instance_id, txn_id, sequence)
);
"#;

const CONFIGURE_CONNECTION: &str = r#"
SET statement_timeout = '30s';
SET lock_timeout = '10s';
SET idle_in_transaction_session_timeout = '60s';
"#;
const SCHEMA_LOCK_HIGH: i32 = 0x4454_4750;
const SCHEMA_LOCK_LOW: i32 = 1;

const REQUIRED_SCHEMA_COLUMNS: &[(&str, &[&str])] = &[
    (
        "schema_meta",
        &[
            "singleton",
            "schema_version",
            "mapping_name",
            "mapping_version",
            "schema_fingerprint",
        ],
    ),
    (
        "adapter_instance",
        &[
            "instance_id",
            "schema_version",
            "mapping_fingerprint",
            "applied_log_index",
            "has_applied_index_record",
            "published",
        ],
    ),
    (
        "vertex_identity",
        &[
            "instance_id",
            "graph_id",
            "partition_id",
            "vertex_id",
            "label_id",
        ],
    ),
    (
        "edge_identity",
        &[
            "instance_id",
            "graph_id",
            "partition_id",
            "edge_id",
            "edge_type",
            "source_partition",
            "source_id",
            "destination_partition",
            "destination_id",
        ],
    ),
    (
        "vertex_current",
        &[
            "instance_id",
            "graph_id",
            "partition_id",
            "vertex_id",
            "projection",
        ],
    ),
    (
        "edge_current",
        &[
            "instance_id",
            "graph_id",
            "partition_id",
            "edge_id",
            "projection",
        ],
    ),
    (
        "history",
        &[
            "instance_id",
            "graph_id",
            "partition_id",
            "element_kind",
            "element_id",
            "transaction_physical",
            "transaction_logical",
            "segment_id",
            "history_value",
        ],
    ),
    (
        "out_adjacency",
        &[
            "instance_id",
            "key_tag",
            "graph_id",
            "local_partition",
            "local_endpoint",
            "edge_type",
            "bucket",
            "remote_partition",
            "remote_endpoint",
            "edge_partition",
            "edge_id",
            "projection",
        ],
    ),
    (
        "in_adjacency",
        &[
            "instance_id",
            "key_tag",
            "graph_id",
            "local_partition",
            "local_endpoint",
            "edge_type",
            "bucket",
            "remote_partition",
            "remote_endpoint",
            "edge_partition",
            "edge_id",
            "projection",
        ],
    ),
    (
        "opaque_records",
        &["instance_id", "keyspace", "logical_key", "value"],
    ),
    ("replay_log", &["instance_id", "log_index", "fingerprint"]),
    (
        "replay_mutation",
        &["instance_id", "txn_id", "sequence", "fingerprint"],
    ),
];

const SELECT_INSTANCE_FOR_UPDATE: &str = "SELECT schema_version, mapping_fingerprint, applied_log_index, has_applied_index_record, published FROM dtgproxy.adapter_instance WHERE instance_id = $1 FOR UPDATE";
const SELECT_INSTANCE: &str = "SELECT schema_version, mapping_fingerprint, applied_log_index, has_applied_index_record, published FROM dtgproxy.adapter_instance WHERE instance_id = $1";
const INSERT_INSTANCE: &str = "INSERT INTO dtgproxy.adapter_instance(instance_id, schema_version, mapping_fingerprint, applied_log_index, has_applied_index_record, published) VALUES ($1, $2, $3, $4, $5, $6)";
const UPDATE_APPLIED_INDEX: &str = "UPDATE dtgproxy.adapter_instance SET applied_log_index = $2, has_applied_index_record = TRUE WHERE instance_id = $1 AND published = TRUE";
const PUBLISH_RESTORE: &str = "UPDATE dtgproxy.adapter_instance SET applied_log_index = $2, has_applied_index_record = $3, published = TRUE WHERE instance_id = $1 AND published = FALSE";

pub struct PostgresAdapterFactory;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OpenMode {
    Serving,
    Restore,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ExistingInstanceAction {
    Use,
    Reclaim,
    RejectUnpublished,
    RejectPublished,
}

const fn existing_instance_action(mode: OpenMode, published: bool) -> ExistingInstanceAction {
    match (mode, published) {
        (OpenMode::Serving, true) => ExistingInstanceAction::Use,
        (OpenMode::Serving, false) => ExistingInstanceAction::RejectUnpublished,
        (OpenMode::Restore, false) => ExistingInstanceAction::Reclaim,
        (OpenMode::Restore, true) => ExistingInstanceAction::RejectPublished,
    }
}

struct ExportSlots {
    active: Arc<AtomicUsize>,
    max: usize,
}

impl ExportSlots {
    fn new(max: usize) -> Self {
        Self {
            active: Arc::new(AtomicUsize::new(0)),
            max,
        }
    }

    fn try_acquire(&self) -> Result<ExportSlot, AdapterError> {
        self.active
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |active| {
                (active < self.max).then_some(active + 1)
            })
            .map_err(|_| {
                AdapterError::Backend(format!(
                    "PostgreSQL logical export limit {} is exhausted",
                    self.max
                ))
            })?;
        Ok(ExportSlot {
            active: Arc::clone(&self.active),
        })
    }
}

struct ExportSlot {
    active: Arc<AtomicUsize>,
}

impl Drop for ExportSlot {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::AcqRel);
    }
}

impl AdapterFactory for PostgresAdapterFactory {
    fn provider_name(&self) -> &str {
        "postgresql"
    }

    fn mapping_descriptor(&self) -> Option<MappingDescriptorV1> {
        Some(postgres_mapping_descriptor())
    }

    fn open<'a>(&'a self, request: &'a AdapterOpenRequest) -> AdapterFactoryFuture<'a> {
        Box::pin(async move {
            let (connection_string, pool_size) = factory_configuration(request)?;
            let mapping = Arc::new(
                PostgresAdapter::connect(
                    connection_string,
                    request.instance_id(),
                    pool_size,
                    OpenMode::Serving,
                )
                .map_err(|error| AdapterFactoryError::new(error.to_string()))?,
            );
            let adapter = MappingBackedAdapter::with_runtime_identity(
                "postgresql",
                mapping.server_version.clone(),
                mapping,
                MappingRequirement::HotPluggableReplica,
            )
            .map_err(|error| AdapterFactoryError::new(error.to_string()))?;
            Ok(Arc::new(adapter) as Arc<dyn StorageAdapter>)
        })
    }

    fn begin_restore<'a>(
        &'a self,
        request: &'a AdapterOpenRequest,
        header: LogicalSnapshotHeaderV1,
    ) -> AdapterRestoreSessionFuture<'a> {
        Box::pin(async move {
            let (connection_string, pool_size) = factory_configuration(request)?;
            let adapter = PostgresAdapter::connect(
                connection_string,
                request.instance_id(),
                pool_size,
                OpenMode::Restore,
            )
            .map_err(|error| AdapterFactoryError::new(error.to_string()))?;
            Ok(Box::new(PostgresRestoreSession {
                adapter: Some(adapter),
                accumulator: LogicalSnapshotAccumulator::new(header.clone()),
                header,
                saw_applied_index_record: false,
                last_chunk: None,
                published: false,
            }) as Box<dyn AdapterRestoreSession + 'a>)
        })
    }
}

fn postgres_mapping_descriptor() -> MappingDescriptorV1 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"DTGProxy/PostgreSQLNativeTemporalMapping/1");
    hasher.update(POSTGRES_SCHEMA.as_bytes());
    for keyspace in Keyspace::ALL {
        hasher.update(&[keyspace.tag()]);
        hasher.update(keyspace.column_family().as_bytes());
    }
    MappingDescriptorV1::new(
        "postgresql-native-temporal",
        "1.0.0",
        BackendFamily::Sql,
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
    .expect("static PostgreSQL Mapping descriptor is valid")
}

fn factory_configuration(
    request: &AdapterOpenRequest,
) -> Result<(&str, usize), AdapterFactoryError> {
    let connection_string = request
        .secret("connection_string")
        .ok_or_else(|| {
            AdapterFactoryError::new("PostgreSQL Adapter requires the secret connection_string")
        })?
        .expose();
    let pool_size = request
        .parameter("pool_size")
        .map(str::parse::<usize>)
        .transpose()
        .map_err(|_| AdapterFactoryError::new("PostgreSQL pool_size must be an integer"))?
        .unwrap_or(DEFAULT_POOL_SIZE);
    validate_pool_size(pool_size).map_err(|error| AdapterFactoryError::new(error.to_string()))?;
    Ok((connection_string, pool_size))
}

pub struct PostgresAdapter {
    connection_string: String,
    instance_id: String,
    server_version: String,
    durability: Durability,
    _lease_client: Mutex<Client>,
    connections: Vec<Mutex<Client>>,
    next_connection: AtomicUsize,
    apply_guard: Mutex<()>,
    export_slots: ExportSlots,
}

impl PostgresAdapter {
    pub fn open(
        connection_string: impl Into<String>,
        instance_id: impl Into<String>,
        pool_size: usize,
    ) -> Result<Self, PostgresAdapterError> {
        Self::connect(
            &connection_string.into(),
            &instance_id.into(),
            pool_size,
            OpenMode::Serving,
        )
    }

    pub fn open_restore_target(
        connection_string: impl Into<String>,
        instance_id: impl Into<String>,
        pool_size: usize,
    ) -> Result<Self, PostgresAdapterError> {
        Self::connect(
            &connection_string.into(),
            &instance_id.into(),
            pool_size,
            OpenMode::Restore,
        )
    }

    fn connect(
        connection_string: &str,
        instance_id: &str,
        pool_size: usize,
        mode: OpenMode,
    ) -> Result<Self, PostgresAdapterError> {
        validate_instance_id(instance_id)?;
        validate_pool_size(pool_size)?;
        let mut lease_client = Client::connect(connection_string, NoTls).map_err(pg_error)?;
        configure_client(&mut lease_client)?;
        lease_client
            .query_one(
                "SELECT pg_advisory_lock($1, $2)",
                &[&SCHEMA_LOCK_HIGH, &SCHEMA_LOCK_LOW],
            )
            .map_err(pg_error)?;
        lease_client
            .batch_execute(POSTGRES_SCHEMA)
            .map_err(pg_error)?;
        validate_schema_catalog(&mut lease_client)?;
        let schema_row = lease_client
            .query_one(
                "SELECT schema_version, mapping_name, mapping_version, schema_fingerprint FROM dtgproxy.schema_meta WHERE singleton = TRUE",
                &[],
            )
            .map_err(pg_error)?;
        let global_schema_version: i32 = schema_row.get(0);
        if global_schema_version != POSTGRES_SCHEMA_VERSION {
            return Err(PostgresAdapterError::SchemaVersionMismatch {
                expected: POSTGRES_SCHEMA_VERSION,
                actual: global_schema_version,
            });
        }
        let descriptor = postgres_mapping_descriptor();
        let mapping_name: String = schema_row.get(1);
        let mapping_version: String = schema_row.get(2);
        let mut schema_fingerprint: Vec<u8> = schema_row.get(3);
        if schema_fingerprint == vec![0; 32] {
            lease_client
                .execute(
                    "UPDATE dtgproxy.schema_meta SET schema_fingerprint = $1 WHERE singleton = TRUE AND schema_fingerprint = decode(repeat('00', 32), 'hex')",
                    &[&descriptor.schema_fingerprint().as_slice()],
                )
                .map_err(pg_error)?;
            schema_fingerprint = descriptor.schema_fingerprint().to_vec();
        }
        if mapping_name != descriptor.name()
            || mapping_version != descriptor.version()
            || schema_fingerprint != descriptor.schema_fingerprint()
        {
            return Err(PostgresAdapterError::SchemaFingerprintMismatch);
        }
        lease_client
            .query_one(
                "SELECT pg_advisory_unlock($1, $2)",
                &[&SCHEMA_LOCK_HIGH, &SCHEMA_LOCK_LOW],
            )
            .map_err(pg_error)?;
        let (lock_high, lock_low) = advisory_lock_keys(instance_id);
        let locked: bool = lease_client
            .query_one(
                "SELECT pg_try_advisory_lock($1, $2)",
                &[&lock_high, &lock_low],
            )
            .map_err(pg_error)?
            .get(0);
        if !locked {
            return Err(PostgresAdapterError::InstanceAlreadyLeased(
                instance_id.to_owned(),
            ));
        }
        let existing = lease_client
            .query_opt(SELECT_INSTANCE, &[&instance_id])
            .map_err(pg_error)?;
        let existing_action = existing
            .as_ref()
            .map(|row| {
                let (_, _, _, published) = validate_instance_row(row)?;
                Ok(existing_instance_action(mode, published))
            })
            .transpose()?;
        match existing_action {
            Some(ExistingInstanceAction::RejectUnpublished) => {
                return Err(PostgresAdapterError::UnpublishedInstance(
                    instance_id.to_owned(),
                ));
            }
            Some(ExistingInstanceAction::RejectPublished) => {
                return Err(PostgresAdapterError::RestoreTargetExists(
                    instance_id.to_owned(),
                ));
            }
            _ => {}
        }
        let fsync: String = lease_client
            .query_one("SHOW fsync", &[])
            .map_err(pg_error)?
            .get(0);
        let server_version: String = lease_client
            .query_one("SHOW server_version", &[])
            .map_err(pg_error)?
            .get(0);
        let durability = if fsync == "on" {
            Durability::Synchronous
        } else {
            Durability::BackendConfigured
        };
        let mut connections = Vec::with_capacity(pool_size);
        for _ in 0..pool_size {
            let mut client = Client::connect(connection_string, NoTls).map_err(pg_error)?;
            configure_client(&mut client)?;
            connections.push(Mutex::new(client));
        }
        if existing_action == Some(ExistingInstanceAction::Reclaim) {
            lease_client
                .execute(
                    "DELETE FROM dtgproxy.adapter_instance WHERE instance_id = $1",
                    &[&instance_id],
                )
                .map_err(pg_error)?;
        }
        if existing_action.is_none() || existing_action == Some(ExistingInstanceAction::Reclaim) {
            let published = mode == OpenMode::Serving;
            lease_client
                .execute(
                    INSERT_INSTANCE,
                    &[
                        &instance_id,
                        &POSTGRES_SCHEMA_VERSION,
                        &descriptor.schema_fingerprint().as_slice(),
                        &0_u64.to_be_bytes().as_slice(),
                        &false,
                        &published,
                    ],
                )
                .map_err(pg_error)?;
        }
        Ok(Self {
            connection_string: connection_string.to_owned(),
            instance_id: instance_id.to_owned(),
            server_version,
            durability,
            _lease_client: Mutex::new(lease_client),
            connections,
            next_connection: AtomicUsize::new(0),
            apply_guard: Mutex::new(()),
            export_slots: ExportSlots::new(pool_size),
        })
    }

    #[must_use]
    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    fn connection(&self) -> Result<MutexGuard<'_, Client>, AdapterError> {
        let index = self.next_connection.fetch_add(1, Ordering::Relaxed) % self.connections.len();
        self.connections[index]
            .lock()
            .map_err(|_| AdapterError::LockPoisoned)
    }

    fn apply(&self, batch: CommittedMutationBatch) -> Result<ApplyReceipt, AdapterError> {
        let _apply_guard = self
            .apply_guard
            .lock()
            .map_err(|_| AdapterError::LockPoisoned)?;
        let mut client = self.connection()?;
        let mut transaction = client
            .build_transaction()
            .isolation_level(IsolationLevel::Serializable)
            .start()
            .map_err(adapter_pg_error)?;
        transaction
            .batch_execute("SET LOCAL synchronous_commit = on")
            .map_err(adapter_pg_error)?;
        let row = transaction
            .query_one(SELECT_INSTANCE_FOR_UPDATE, &[&self.instance_id])
            .map_err(adapter_pg_error)?;
        let (_, applied_log_index) = validate_instance_row_for_adapter(&row)?;
        let batch_fingerprint = batch.fingerprint();
        if batch.log_index <= applied_log_index {
            let stored = get_u64(
                &mut transaction,
                &self.instance_id,
                Keyspace::Txn,
                &adapter_log_fingerprint_key(batch.log_index),
            )?;
            return match stored {
                Some(fingerprint) if fingerprint == batch_fingerprint => Ok(ApplyReceipt {
                    applied_log_index,
                    duplicate: true,
                }),
                _ => Err(AdapterError::CommittedLogReplayMismatch {
                    log_index: batch.log_index,
                }),
            };
        }
        let expected = applied_log_index.saturating_add(1);
        if batch.log_index != expected {
            return Err(AdapterError::NonContiguousLogIndex {
                expected,
                actual: batch.log_index,
            });
        }
        let mut sequences = BTreeSet::new();
        let mut mutation_fingerprints = Vec::with_capacity(batch.mutations.len());
        for mutation in &batch.mutations {
            if !sequences.insert(mutation.sequence) {
                return Err(AdapterError::DuplicateMutationSequence {
                    txn_id: batch.txn_id,
                    sequence: mutation.sequence,
                });
            }
            let key = adapter_mutation_fingerprint_key(batch.txn_id, mutation.sequence);
            let fingerprint = mutation.fingerprint();
            if let Some(previous) =
                get_u64(&mut transaction, &self.instance_id, Keyspace::Txn, &key)?
                && previous != fingerprint
            {
                return Err(AdapterError::MutationReplayMismatch {
                    txn_id: batch.txn_id,
                    sequence: mutation.sequence,
                });
            }
            mutation_fingerprints.push((key, fingerprint));
        }
        for mutation in &batch.mutations {
            match &mutation.operation {
                MutationOperation::Put { key, value } => {
                    put_value(&mut transaction, &self.instance_id, key, value)?;
                }
                MutationOperation::Delete { key } => {
                    delete_value(&mut transaction, &self.instance_id, key)?;
                }
            }
        }
        for (key, fingerprint) in mutation_fingerprints {
            put_raw(
                &mut transaction,
                &self.instance_id,
                Keyspace::Txn,
                &key,
                &fingerprint.to_be_bytes(),
            )?;
        }
        put_raw(
            &mut transaction,
            &self.instance_id,
            Keyspace::Txn,
            &adapter_log_fingerprint_key(batch.log_index),
            &batch_fingerprint.to_be_bytes(),
        )?;
        put_raw(
            &mut transaction,
            &self.instance_id,
            Keyspace::Meta,
            ADAPTER_META_APPLIED_LOG_INDEX_KEY,
            &batch.log_index.to_be_bytes(),
        )?;
        let updated = transaction
            .execute(
                UPDATE_APPLIED_INDEX,
                &[&self.instance_id, &batch.log_index.to_be_bytes().as_slice()],
            )
            .map_err(adapter_pg_error)?;
        if updated != 1 {
            return Err(AdapterError::Backend(
                "PostgreSQL Adapter instance became unpublished during apply".to_owned(),
            ));
        }
        transaction.commit().map_err(adapter_pg_error)?;
        Ok(ApplyReceipt {
            applied_log_index: batch.log_index,
            duplicate: false,
        })
    }

    fn multi_get_values(&self, keys: &[LogicalKey]) -> Result<Vec<Option<Vec<u8>>>, AdapterError> {
        let mut client = self.connection()?;
        let mut transaction = client
            .build_transaction()
            .isolation_level(IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()
            .map_err(adapter_pg_error)?;
        let entries = load_all_entries(&mut transaction, &self.instance_id)?;
        let values = keys
            .iter()
            .map(|key| {
                entries
                    .iter()
                    .find(|entry| {
                        entry.key().keyspace() == key.keyspace()
                            && entry.key().as_bytes() == key.as_bytes()
                    })
                    .map(|entry| entry.value().to_vec())
            })
            .collect();
        transaction.commit().map_err(adapter_pg_error)?;
        Ok(values)
    }

    fn scan_values(&self, span: &KeySpan) -> Result<Vec<KeyValue>, AdapterError> {
        let mut client = self.connection()?;
        let mut transaction = client
            .build_transaction()
            .isolation_level(IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()
            .map_err(adapter_pg_error)?;
        let entries = load_all_entries(&mut transaction, &self.instance_id)?;
        let mut values = Vec::new();
        let mut retained = 0_u64;
        for entry in entries {
            if entry.key().keyspace() != span.keyspace() || !span.contains(entry.key().as_bytes()) {
                continue;
            }
            retained = storage_api::charge_scan_entry(
                span,
                retained,
                entry.key().as_bytes(),
                entry.value(),
            )?;
            values.push(entry);
            if span.limit().is_some_and(|limit| values.len() >= limit) {
                break;
            }
        }
        transaction.commit().map_err(adapter_pg_error)?;
        Ok(values)
    }

    fn current_applied_log_index(&self) -> Result<u64, AdapterError> {
        let mut client = self.connection()?;
        let row = client
            .query_one(SELECT_INSTANCE, &[&self.instance_id])
            .map_err(adapter_pg_error)?;
        validate_instance_row_for_adapter(&row).map(|(_, index)| index)
    }

    fn validate_durable_batch(&self, batch: &CommittedMutationBatch) -> Result<(), AdapterError> {
        let mut client = self.connection()?;
        let mut transaction = client
            .build_transaction()
            .isolation_level(IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()
            .map_err(adapter_pg_error)?;
        let row = transaction
            .query_one(SELECT_INSTANCE, &[&self.instance_id])
            .map_err(adapter_pg_error)?;
        let (_, applied_log_index) = validate_instance_row_for_adapter(&row)?;
        let batch_fingerprint = batch.fingerprint();
        if batch.log_index <= applied_log_index {
            let stored = get_u64(
                &mut transaction,
                &self.instance_id,
                Keyspace::Txn,
                &adapter_log_fingerprint_key(batch.log_index),
            )?;
            if stored != Some(batch_fingerprint) {
                return Err(AdapterError::CommittedLogReplayMismatch {
                    log_index: batch.log_index,
                });
            }
        } else if batch.log_index != applied_log_index.saturating_add(1) {
            return Err(AdapterError::NonContiguousLogIndex {
                expected: applied_log_index.saturating_add(1),
                actual: batch.log_index,
            });
        }
        for mutation in &batch.mutations {
            let key = adapter_mutation_fingerprint_key(batch.txn_id, mutation.sequence);
            if let Some(previous) =
                get_u64(&mut transaction, &self.instance_id, Keyspace::Txn, &key)?
                && previous != mutation.fingerprint()
            {
                return Err(AdapterError::MutationReplayMismatch {
                    txn_id: batch.txn_id,
                    sequence: mutation.sequence,
                });
            }
        }
        transaction.commit().map_err(adapter_pg_error)
    }

    fn begin_export(
        &self,
        request: LogicalSnapshotExportRequest,
    ) -> Result<PostgresLogicalSnapshotReader, AdapterError> {
        let export_slot = self.export_slots.try_acquire()?;
        let mut client =
            Client::connect(&self.connection_string, NoTls).map_err(adapter_pg_error)?;
        client
            .batch_execute(CONFIGURE_CONNECTION)
            .map_err(adapter_pg_error)?;
        client
            .batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .map_err(adapter_pg_error)?;
        let row = client
            .query_one(SELECT_INSTANCE, &[&self.instance_id])
            .map_err(adapter_pg_error)?;
        let (_, applied_log_index) = validate_instance_row_for_adapter(&row)?;
        let entries = load_all_entries(&mut client, &self.instance_id)?;
        let header = LogicalSnapshotHeaderV1::new(new_logical_snapshot_id(), applied_log_index);
        Ok(PostgresLogicalSnapshotReader {
            client,
            entries,
            entry_position: 0,
            request,
            accumulator: LogicalSnapshotAccumulator::new(header.clone()),
            header,
            next_ordinal: 0,
            exhausted: false,
            finished: false,
            _export_slot: export_slot,
        })
    }

    fn restore_entries(
        &self,
        entries: &[KeyValue],
        expected_index: u64,
    ) -> Result<bool, AdapterError> {
        let mut client = self.connection()?;
        let mut transaction = client
            .build_transaction()
            .isolation_level(IsolationLevel::Serializable)
            .start()
            .map_err(adapter_pg_error)?;
        transaction
            .batch_execute("SET LOCAL synchronous_commit = on")
            .map_err(adapter_pg_error)?;
        let row = transaction
            .query_one(SELECT_INSTANCE_FOR_UPDATE, &[&self.instance_id])
            .map_err(adapter_pg_error)?;
        let (_, current_index) = validate_instance_row_for_restore(&row)?;
        if current_index != 0 {
            return Err(AdapterError::Backend(
                "PostgreSQL logical restore target already has an applied index".to_owned(),
            ));
        }
        let mut saw_applied_index = false;
        for entry in entries {
            if entry.key().keyspace() == Keyspace::Meta
                && entry.key().as_bytes() == ADAPTER_META_APPLIED_LOG_INDEX_KEY
            {
                if entry.value() != expected_index.to_be_bytes() {
                    return Err(AdapterError::Backend(
                        "logical snapshot applied-index record differs from its header".to_owned(),
                    ));
                }
                saw_applied_index = true;
                continue;
            }
            put_native(
                &mut transaction,
                &self.instance_id,
                entry.key(),
                entry.value(),
            )?;
        }
        transaction.commit().map_err(adapter_pg_error)?;
        Ok(saw_applied_index)
    }

    fn publish_restore(
        &self,
        applied_index: u64,
        source_had_record: bool,
    ) -> Result<(), AdapterError> {
        let mut client = self.connection()?;
        let mut transaction = client
            .build_transaction()
            .isolation_level(IsolationLevel::Serializable)
            .start()
            .map_err(adapter_pg_error)?;
        transaction
            .batch_execute("SET LOCAL synchronous_commit = on")
            .map_err(adapter_pg_error)?;
        let row = transaction
            .query_one(SELECT_INSTANCE_FOR_UPDATE, &[&self.instance_id])
            .map_err(adapter_pg_error)?;
        let (_, current_index) = validate_instance_row_for_restore(&row)?;
        if current_index != 0 {
            return Err(AdapterError::Backend(
                "PostgreSQL restore target advanced before publication".to_owned(),
            ));
        }
        if applied_index != 0 || source_had_record {
            put_raw(
                &mut transaction,
                &self.instance_id,
                Keyspace::Meta,
                ADAPTER_META_APPLIED_LOG_INDEX_KEY,
                &applied_index.to_be_bytes(),
            )?;
        }
        let published = transaction
            .execute(
                PUBLISH_RESTORE,
                &[
                    &self.instance_id,
                    &applied_index.to_be_bytes().as_slice(),
                    &source_had_record,
                ],
            )
            .map_err(adapter_pg_error)?;
        if published != 1 {
            return Err(AdapterError::Backend(
                "PostgreSQL restore target was not unpublished at publication".to_owned(),
            ));
        }
        transaction.commit().map_err(adapter_pg_error)
    }

    fn delete_namespace(&self) -> Result<(), AdapterError> {
        let mut client = self.connection()?;
        let mut transaction = client.transaction().map_err(adapter_pg_error)?;
        transaction
            .execute(
                "DELETE FROM dtgproxy.adapter_instance WHERE instance_id = $1 AND published = FALSE",
                &[&self.instance_id],
            )
            .map_err(adapter_pg_error)?;
        transaction.commit().map_err(adapter_pg_error)
    }

    fn ensure_restore_target_unpublished(&self) -> Result<(), AdapterError> {
        let mut client = self.connection()?;
        let row = client
            .query_opt(SELECT_INSTANCE, &[&self.instance_id])
            .map_err(adapter_pg_error)?
            .ok_or_else(|| {
                AdapterError::Backend(
                    "PostgreSQL restore target instance does not exist".to_owned(),
                )
            })?;
        validate_instance_row_for_restore(&row).map(|_| ())
    }
}

fn validate_schema_catalog(client: &mut Client) -> Result<(), PostgresAdapterError> {
    let shadow_exists: bool = client
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'dtgproxy' AND c.relname = 'canonical_kv' AND c.relkind IN ('r', 'p', 'f'))",
            &[],
        )
        .map_err(pg_error)?
        .get(0);
    if shadow_exists {
        return Err(PostgresAdapterError::SchemaFingerprintMismatch);
    }

    let rows = client
        .query(
            "SELECT c.relname, a.attname FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid WHERE n.nspname = 'dtgproxy' AND c.relkind IN ('r', 'p') AND a.attnum > 0 AND NOT a.attisdropped",
            &[],
        )
        .map_err(pg_error)?;
    let columns: BTreeSet<(String, String)> = rows
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    if REQUIRED_SCHEMA_COLUMNS.iter().any(|(table, required)| {
        required
            .iter()
            .any(|column| !columns.contains(&(table.to_string(), (*column).to_owned())))
    }) {
        return Err(PostgresAdapterError::SchemaFingerprintMismatch);
    }
    Ok(())
}

impl StorageAdapter for PostgresAdapter {
    fn descriptor(&self) -> AdapterDescriptorV1 {
        AdapterDescriptorV1::new(
            "postgresql",
            self.server_version.clone(),
            BackendFamily::Sql,
            StorageAdapter::capabilities(self),
        )
    }

    fn capabilities(&self) -> AdapterCapabilities {
        AdapterCapabilities {
            local_atomic_batch: true,
            idempotent_apply: true,
            consistent_multi_get: true,
            ordered_scan: true,
            durable_applied_index: true,
            durability: self.durability,
            snapshot: SnapshotCapability::LogicalExport,
            logical_export: true,
            logical_restore: true,
            predicate_pushdown: false,
            adjacency_pushdown: false,
            change_feed: false,
        }
    }

    fn mapping_descriptor(&self) -> Option<MappingDescriptorV1> {
        Some(postgres_mapping_descriptor())
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
            Ok(Box::new(self.begin_export(request)?) as Box<dyn LogicalSnapshotReader + 'a>)
        })
    }

    fn applied_log_index(&self) -> Result<u64, AdapterError> {
        self.current_applied_log_index()
    }
}

struct PostgresPreparedTransaction<'a> {
    adapter: &'a PostgresAdapter,
    batch: Option<CommittedMutationBatch>,
    applied: bool,
}

impl PreparedMappingTransaction for PostgresPreparedTransaction<'_> {
    fn apply<'a>(&'a mut self) -> MappingFuture<'a, ()> {
        Box::pin(async move {
            if self.applied {
                return Err(AdapterError::Backend(
                    "PostgreSQL Mapping transaction was applied twice".to_owned(),
                ));
            }
            if self.batch.is_none() {
                return Err(AdapterError::Backend(
                    "PostgreSQL Mapping transaction is already finished".to_owned(),
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
                    "PostgreSQL Mapping transaction must be applied before commit".to_owned(),
                ));
            }
            let batch = self.batch.take().ok_or_else(|| {
                AdapterError::Backend(
                    "PostgreSQL Mapping transaction is already finished".to_owned(),
                )
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

struct PostgresCanonicalRestoreSession<'a> {
    adapter: &'a PostgresAdapter,
    header: LogicalSnapshotHeaderV1,
    accumulator: LogicalSnapshotAccumulator,
    saw_applied_index_record: bool,
    last_chunk: Option<(u64, [u8; 32])>,
    finished: bool,
}

impl CanonicalRestoreSession for PostgresCanonicalRestoreSession<'_> {
    fn write_chunk<'a>(&'a mut self, chunk: LogicalSnapshotChunkV1) -> MappingFuture<'a, ()> {
        Box::pin(async move {
            if self.finished {
                return Err(AdapterError::Backend(
                    "PostgreSQL canonical restore session is already finished".to_owned(),
                ));
            }
            if self.last_chunk == Some((chunk.ordinal(), chunk.digest())) {
                return Ok(());
            }
            let mut next = self.accumulator.clone();
            next.observe(&chunk)?;
            self.saw_applied_index_record |= self
                .adapter
                .restore_entries(chunk.entries(), self.header.applied_log_index())?;
            self.last_chunk = Some((chunk.ordinal(), chunk.digest()));
            self.accumulator = next;
            Ok(())
        })
    }

    fn commit<'a>(&'a mut self, manifest: LogicalSnapshotManifestV1) -> MappingFuture<'a, ()> {
        Box::pin(async move {
            if self.finished {
                return Err(AdapterError::Backend(
                    "PostgreSQL canonical restore session is already finished".to_owned(),
                ));
            }
            self.accumulator.clone().verify(&manifest)?;
            if self.header.applied_log_index() != 0 && !self.saw_applied_index_record {
                return Err(AdapterError::Backend(
                    "logical snapshot is missing its applied-index record".to_owned(),
                ));
            }
            self.adapter.publish_restore(
                self.header.applied_log_index(),
                self.saw_applied_index_record,
            )?;
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

impl Drop for PostgresCanonicalRestoreSession<'_> {
    fn drop(&mut self) {
        if !self.finished {
            let _ = self.adapter.delete_namespace();
        }
    }
}

impl TemporalBackendMapping for PostgresAdapter {
    fn describe_schema(&self) -> MappingDescriptorV1 {
        postgres_mapping_descriptor()
    }

    fn validate_mapping(&self) -> Result<(), AdapterError> {
        if self.durability != Durability::Synchronous {
            return Err(AdapterError::Backend(
                "PostgreSQL native Mapping requires fsync=on".to_owned(),
            ));
        }
        self.current_applied_log_index().map(|_| ())
    }

    fn prepare<'a>(
        &'a self,
        batch: CommittedMutationBatch,
    ) -> MappingFuture<'a, Box<dyn PreparedMappingTransaction + 'a>> {
        Box::pin(async move {
            validate_prepared_batch(&batch)?;
            self.validate_durable_batch(&batch)?;
            Ok(Box::new(PostgresPreparedTransaction {
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
            Ok(Box::new(self.begin_export(request)?) as Box<dyn LogicalSnapshotReader + 'a>)
        })
    }

    fn restore_canonical<'a>(
        &'a self,
        header: LogicalSnapshotHeaderV1,
    ) -> MappingFuture<'a, Box<dyn CanonicalRestoreSession + 'a>> {
        Box::pin(async move {
            self.ensure_restore_target_unpublished()?;
            Ok(Box::new(PostgresCanonicalRestoreSession {
                adapter: self,
                accumulator: LogicalSnapshotAccumulator::new(header.clone()),
                header,
                saw_applied_index_record: false,
                last_chunk: None,
                finished: false,
            }) as Box<dyn CanonicalRestoreSession + 'a>)
        })
    }

    fn applied_log_index(&self) -> Result<u64, AdapterError> {
        self.current_applied_log_index()
    }
}

fn validate_prepared_batch(batch: &CommittedMutationBatch) -> Result<(), AdapterError> {
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
                if matches!(
                    key.keyspace(),
                    Keyspace::Identity
                        | Keyspace::Current
                        | Keyspace::AdjOut
                        | Keyspace::AdjIn
                        | Keyspace::History
                ) {
                    decode_graph_key(key)
                        .map_err(|error| AdapterError::Backend(error.to_string()))?;
                }
            }
        }
    }
    Ok(())
}

struct PostgresLogicalSnapshotReader {
    client: Client,
    entries: Vec<KeyValue>,
    entry_position: usize,
    request: LogicalSnapshotExportRequest,
    header: LogicalSnapshotHeaderV1,
    accumulator: LogicalSnapshotAccumulator,
    next_ordinal: u64,
    exhausted: bool,
    finished: bool,
    _export_slot: ExportSlot,
}

impl PostgresLogicalSnapshotReader {
    fn read_next_chunk(&mut self) -> Result<Option<LogicalSnapshotChunkV1>, AdapterError> {
        if self.exhausted {
            return Ok(None);
        }
        let mut entries = Vec::new();
        let mut budget = SnapshotChunkBudget::new(self.request);
        while self.entry_position < self.entries.len() {
            let entry = &self.entries[self.entry_position];
            if !budget.try_reserve(entry.key().as_bytes(), entry.value())? {
                break;
            }
            entries.push(entry.clone());
            self.entry_position += 1;
            if budget.entries() == self.request.max_entries_per_chunk()
                || budget.bytes() == self.request.max_bytes_per_chunk()
            {
                break;
            }
        }
        if entries.is_empty() {
            self.exhausted = true;
            return Ok(None);
        }
        let chunk =
            LogicalSnapshotChunkV1::new(self.header.snapshot_id(), self.next_ordinal, entries)?;
        self.accumulator.observe(&chunk)?;
        self.next_ordinal = self
            .next_ordinal
            .checked_add(1)
            .ok_or(LogicalSnapshotError::CountOverflow)?;
        Ok(Some(chunk))
    }
}

impl LogicalSnapshotReader for PostgresLogicalSnapshotReader {
    fn header(&self) -> &LogicalSnapshotHeaderV1 {
        &self.header
    }

    fn next_chunk<'a>(&'a mut self) -> AdapterFuture<'a, Option<LogicalSnapshotChunkV1>> {
        Box::pin(async move { self.read_next_chunk() })
    }

    fn finish<'a>(mut self: Box<Self>) -> AdapterFuture<'a, LogicalSnapshotManifestV1>
    where
        Self: 'a,
    {
        Box::pin(async move {
            if !self.exhausted {
                return Err(LogicalSnapshotError::ExportNotExhausted.into());
            }
            self.client
                .batch_execute("COMMIT")
                .map_err(adapter_pg_error)?;
            self.finished = true;
            Ok(self.accumulator.clone().complete())
        })
    }
}

impl Drop for PostgresLogicalSnapshotReader {
    fn drop(&mut self) {
        if !self.finished {
            let _ = self.client.batch_execute("ROLLBACK");
        }
    }
}

struct PostgresRestoreSession {
    adapter: Option<PostgresAdapter>,
    header: LogicalSnapshotHeaderV1,
    accumulator: LogicalSnapshotAccumulator,
    saw_applied_index_record: bool,
    last_chunk: Option<(u64, [u8; 32])>,
    published: bool,
}

impl AdapterRestoreSession for PostgresRestoreSession {
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
            let mut next_accumulator = self.accumulator.clone();
            next_accumulator
                .observe(&chunk)
                .map_err(|error| AdapterFactoryError::new(error.to_string()))?;
            let adapter = self.adapter.as_ref().ok_or_else(|| {
                AdapterFactoryError::new("PostgreSQL restore session is already finished")
            })?;
            let saw_applied = adapter
                .restore_entries(chunk.entries(), self.header.applied_log_index())
                .map_err(|error| AdapterFactoryError::new(error.to_string()))?;
            self.saw_applied_index_record |= saw_applied;
            self.last_chunk = Some((chunk.ordinal(), chunk.digest()));
            self.accumulator = next_accumulator;
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
            if self.header.applied_log_index() != 0 && !self.saw_applied_index_record {
                return Err(AdapterFactoryError::new(
                    "logical snapshot is missing its applied-index record",
                ));
            }
            let adapter = self.adapter.as_ref().ok_or_else(|| {
                AdapterFactoryError::new("PostgreSQL restore session is already finished")
            })?;
            adapter
                .publish_restore(
                    self.header.applied_log_index(),
                    self.saw_applied_index_record,
                )
                .map_err(|error| AdapterFactoryError::new(error.to_string()))?;
            let adapter = self
                .adapter
                .take()
                .expect("restore Adapter checked immediately before take");
            let implementation_version = adapter.server_version.clone();
            let mapping = Arc::new(adapter);
            let adapter = MappingBackedAdapter::with_runtime_identity(
                "postgresql",
                implementation_version,
                mapping,
                MappingRequirement::HotPluggableReplica,
            )
            .map_err(|error| AdapterFactoryError::new(error.to_string()))?;
            self.published = true;
            Ok(Arc::new(adapter) as Arc<dyn StorageAdapter>)
        })
    }

    fn abort<'a>(mut self: Box<Self>) -> AdapterRestoreFuture<'a, ()>
    where
        Self: 'a,
    {
        Box::pin(async move {
            let adapter = self.adapter.take().ok_or_else(|| {
                AdapterFactoryError::new("PostgreSQL restore session is already finished")
            })?;
            adapter
                .delete_namespace()
                .map_err(|error| AdapterFactoryError::new(error.to_string()))?;
            Ok(())
        })
    }
}

impl Drop for PostgresRestoreSession {
    fn drop(&mut self) {
        if !self.published
            && let Some(adapter) = self.adapter.take()
        {
            let _ = adapter.delete_namespace();
        }
    }
}

fn validate_instance_id(instance_id: &str) -> Result<(), PostgresAdapterError> {
    if !(1..=128).contains(&instance_id.len())
        || !instance_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._:-".contains(&byte))
    {
        return Err(PostgresAdapterError::InvalidInstanceId);
    }
    Ok(())
}

fn validate_pool_size(pool_size: usize) -> Result<(), PostgresAdapterError> {
    if !(1..=MAX_POOL_SIZE).contains(&pool_size) {
        return Err(PostgresAdapterError::InvalidPoolSize {
            max: MAX_POOL_SIZE,
            actual: pool_size,
        });
    }
    Ok(())
}

fn validate_instance_row(
    row: &postgres::Row,
) -> Result<(i32, u64, bool, bool), PostgresAdapterError> {
    let version: i32 = row.get(0);
    if version != POSTGRES_SCHEMA_VERSION {
        return Err(PostgresAdapterError::SchemaVersionMismatch {
            expected: POSTGRES_SCHEMA_VERSION,
            actual: version,
        });
    }
    let fingerprint: Vec<u8> = row.get(1);
    if fingerprint != postgres_mapping_descriptor().schema_fingerprint() {
        return Err(PostgresAdapterError::SchemaFingerprintMismatch);
    }
    let index: Vec<u8> = row.get(2);
    let index: [u8; 8] = index
        .try_into()
        .map_err(|_| PostgresAdapterError::CorruptAppliedIndex)?;
    let has_applied_index_record: bool = row.get(3);
    let published: bool = row.get(4);
    Ok((
        version,
        u64::from_be_bytes(index),
        has_applied_index_record,
        published,
    ))
}

fn validate_instance_row_for_adapter(row: &postgres::Row) -> Result<(i32, u64), AdapterError> {
    let (version, index, _, published) =
        validate_instance_row(row).map_err(|error| AdapterError::Backend(error.to_string()))?;
    if !published {
        return Err(AdapterError::Backend(
            "PostgreSQL Adapter instance is not published".to_owned(),
        ));
    }
    Ok((version, index))
}

fn validate_instance_row_for_restore(row: &postgres::Row) -> Result<(i32, u64), AdapterError> {
    let (version, index, _, published) =
        validate_instance_row(row).map_err(|error| AdapterError::Backend(error.to_string()))?;
    if published {
        return Err(AdapterError::Backend(
            "PostgreSQL restore target is already published".to_owned(),
        ));
    }
    Ok((version, index))
}

fn load_all_entries<C: GenericClient>(
    transaction: &mut C,
    instance_id: &str,
) -> Result<Vec<KeyValue>, AdapterError> {
    let mut entries = Vec::new();

    for row in transaction
        .query(
            "SELECT graph_id, partition_id, vertex_id, label_id FROM dtgproxy.vertex_identity WHERE instance_id = $1",
            &[&instance_id],
        )
        .map_err(adapter_pg_error)?
    {
        let element = temporal_storage::ElementRef::vertex(
            temporal_storage::GraphId::new(read_u64(row.get(0), "vertex graph_id")?),
            temporal_storage::PartitionId::new(read_u32(row.get(1), "vertex partition_id")?),
            temporal_storage::ElementId::new(read_u128(row.get(2), "vertex_id")?),
        );
        let value = VertexIdentity::new(
            element,
            temporal_storage::LabelId::new(read_u32(row.get(3), "label_id")?),
        )
        .map_err(|error| AdapterError::Backend(error.to_string()))?
        .encode();
        entries.push(KeyValue::new(vertex_identity_key(element), value));
    }

    for row in transaction
        .query(
            "SELECT graph_id, partition_id, edge_id, edge_type, source_partition, source_id, destination_partition, destination_id FROM dtgproxy.edge_identity WHERE instance_id = $1",
            &[&instance_id],
        )
        .map_err(adapter_pg_error)?
    {
        let graph = temporal_storage::GraphId::new(read_u64(row.get(0), "edge graph_id")?);
        let partition = temporal_storage::PartitionId::new(read_u32(row.get(1), "edge partition_id")?);
        let element = temporal_storage::ElementRef::edge(
            graph,
            partition,
            temporal_storage::ElementId::new(read_u128(row.get(2), "edge_id")?),
        );
        let source = temporal_storage::ElementRef::vertex(
            graph,
            temporal_storage::PartitionId::new(read_u32(row.get(4), "source_partition")?),
            temporal_storage::ElementId::new(read_u128(row.get(5), "source_id")?),
        );
        let destination = temporal_storage::ElementRef::vertex(
            graph,
            temporal_storage::PartitionId::new(read_u32(row.get(6), "destination_partition")?),
            temporal_storage::ElementId::new(read_u128(row.get(7), "destination_id")?),
        );
        let value = EdgeIdentity::new_between(
            element,
            temporal_storage::EdgeTypeId::new(read_u32(row.get(3), "edge_type")?),
            source,
            destination,
        )
        .map_err(|error| AdapterError::Backend(error.to_string()))?
        .encode();
        entries.push(KeyValue::new(edge_identity_key(element), value));
    }

    append_current_entries(
        transaction,
        instance_id,
        "vertex_current",
        ElementKind::Vertex,
        &mut entries,
    )?;
    append_current_entries(
        transaction,
        instance_id,
        "edge_current",
        ElementKind::Edge,
        &mut entries,
    )?;

    for row in transaction
        .query(
            "SELECT graph_id, partition_id, element_kind, element_id, transaction_physical, transaction_logical, segment_id, history_value FROM dtgproxy.history WHERE instance_id = $1",
            &[&instance_id],
        )
        .map_err(adapter_pg_error)?
    {
        let graph = temporal_storage::GraphId::new(read_u64(row.get(0), "history graph_id")?);
        let partition = temporal_storage::PartitionId::new(read_u32(row.get(1), "history partition_id")?);
        let kind: i16 = row.get(2);
        let id = temporal_storage::ElementId::new(read_u128(row.get(3), "history element_id")?);
        let element = match kind {
            1 => temporal_storage::ElementRef::vertex(graph, partition, id),
            2 => temporal_storage::ElementRef::edge(graph, partition, id),
            _ => return Err(AdapterError::Backend(format!("invalid history element kind {kind}"))),
        };
        let physical: i64 = row.get(4);
        let logical = read_u32(row.get(5), "history transaction_logical")?;
        let segment_id = read_u32(row.get(6), "history segment_id")?;
        let value: Vec<u8> = row.get(7);
        HistoryEntry::decode(&value).map_err(|error| AdapterError::Backend(error.to_string()))?;
        entries.push(KeyValue::new(
            history_anchor_key(element, TransactionTime::new(physical, logical), segment_id),
            value,
        ));
    }

    append_adjacency_entries(transaction, instance_id, "out", &mut entries)?;
    append_adjacency_entries(transaction, instance_id, "in", &mut entries)?;

    for row in transaction
        .query(
            "SELECT keyspace, logical_key, value FROM dtgproxy.opaque_records WHERE instance_id = $1",
            &[&instance_id],
        )
        .map_err(adapter_pg_error)?
    {
        let tag: i16 = row.get(0);
        let keyspace = keyspace_from_i16(tag)?;
        entries.push(KeyValue::new(
            LogicalKey::in_keyspace(keyspace, row.get(1)),
            row.get(2),
        ));
    }

    for row in transaction
        .query(
            "SELECT log_index, fingerprint FROM dtgproxy.replay_log WHERE instance_id = $1",
            &[&instance_id],
        )
        .map_err(adapter_pg_error)?
    {
        let log_index = read_u64(row.get(0), "replay log_index")?;
        entries.push(KeyValue::new(
            LogicalKey::in_keyspace(
                Keyspace::Txn,
                adapter_log_fingerprint_key(log_index).to_vec(),
            ),
            row.get(1),
        ));
    }
    for row in transaction
        .query(
            "SELECT txn_id, sequence, fingerprint FROM dtgproxy.replay_mutation WHERE instance_id = $1",
            &[&instance_id],
        )
        .map_err(adapter_pg_error)?
    {
        let txn_id = read_u128(row.get(0), "replay txn_id")?;
        let sequence = read_u32(row.get(1), "replay sequence")?;
        entries.push(KeyValue::new(
            LogicalKey::in_keyspace(
                Keyspace::Txn,
                adapter_mutation_fingerprint_key(txn_id, sequence).to_vec(),
            ),
            row.get(2),
        ));
    }

    let row = transaction
        .query_one(SELECT_INSTANCE, &[&instance_id])
        .map_err(adapter_pg_error)?;
    let (_, applied_index, has_applied_index_record, _) =
        validate_instance_row(&row).map_err(|error| AdapterError::Backend(error.to_string()))?;
    if has_applied_index_record {
        entries.push(KeyValue::new(
            LogicalKey::in_keyspace(Keyspace::Meta, ADAPTER_META_APPLIED_LOG_INDEX_KEY.to_vec()),
            applied_index.to_be_bytes().to_vec(),
        ));
    }

    entries.sort_by(|left, right| {
        left.key()
            .keyspace()
            .cmp(&right.key().keyspace())
            .then_with(|| left.key().as_bytes().cmp(right.key().as_bytes()))
    });
    for pair in entries.windows(2) {
        if pair[0].key() == pair[1].key() {
            return Err(AdapterError::Backend(format!(
                "PostgreSQL native Mapping reconstructed duplicate canonical key {:?}",
                pair[0].key()
            )));
        }
    }
    Ok(entries)
}

fn append_current_entries<C: GenericClient>(
    transaction: &mut C,
    instance_id: &str,
    table: &'static str,
    kind: ElementKind,
    entries: &mut Vec<KeyValue>,
) -> Result<(), AdapterError> {
    let statement = match kind {
        ElementKind::Vertex => {
            "SELECT graph_id, partition_id, vertex_id, projection FROM dtgproxy.vertex_current WHERE instance_id = $1"
        }
        ElementKind::Edge => {
            "SELECT graph_id, partition_id, edge_id, projection FROM dtgproxy.edge_current WHERE instance_id = $1"
        }
    };
    debug_assert!(matches!(
        (table, kind),
        ("vertex_current", ElementKind::Vertex) | ("edge_current", ElementKind::Edge)
    ));
    for row in transaction
        .query(statement, &[&instance_id])
        .map_err(adapter_pg_error)?
    {
        let graph = temporal_storage::GraphId::new(read_u64(row.get(0), "current graph_id")?);
        let partition =
            temporal_storage::PartitionId::new(read_u32(row.get(1), "current partition_id")?);
        let id = temporal_storage::ElementId::new(read_u128(row.get(2), "current element_id")?);
        let value: Vec<u8> = row.get(3);
        ProjectionRecord::decode(&value)
            .map_err(|error| AdapterError::Backend(error.to_string()))?;
        let key = match kind {
            ElementKind::Vertex => {
                current_vertex_key(temporal_storage::ElementRef::vertex(graph, partition, id))
            }
            ElementKind::Edge => {
                current_edge_key(temporal_storage::ElementRef::edge(graph, partition, id))
            }
        };
        entries.push(KeyValue::new(key, value));
    }
    Ok(())
}

fn append_adjacency_entries<C: GenericClient>(
    transaction: &mut C,
    instance_id: &str,
    direction: &'static str,
    entries: &mut Vec<KeyValue>,
) -> Result<(), AdapterError> {
    let statement = if direction == "out" {
        "SELECT key_tag, graph_id, local_partition, local_endpoint, edge_type, bucket, remote_partition, remote_endpoint, edge_partition, edge_id, projection FROM dtgproxy.out_adjacency WHERE instance_id = $1"
    } else {
        "SELECT key_tag, graph_id, local_partition, local_endpoint, edge_type, bucket, remote_partition, remote_endpoint, edge_partition, edge_id, projection FROM dtgproxy.in_adjacency WHERE instance_id = $1"
    };
    for row in transaction
        .query(statement, &[&instance_id])
        .map_err(adapter_pg_error)?
    {
        let tag: i16 = row.get(0);
        let graph = temporal_storage::GraphId::new(read_u64(row.get(1), "adjacency graph_id")?);
        let local_partition =
            temporal_storage::PartitionId::new(read_u32(row.get(2), "adjacency local_partition")?);
        let local_endpoint =
            temporal_storage::ElementId::new(read_u128(row.get(3), "adjacency local_endpoint")?);
        let edge_type =
            temporal_storage::EdgeTypeId::new(read_u32(row.get(4), "adjacency edge_type")?);
        let bucket = read_u16(row.get(5), "adjacency bucket")?;
        let remote_partition =
            temporal_storage::PartitionId::new(read_u32(row.get(6), "adjacency remote_partition")?);
        let remote_endpoint =
            temporal_storage::ElementId::new(read_u128(row.get(7), "adjacency remote_endpoint")?);
        let edge_partition =
            temporal_storage::PartitionId::new(read_u32(row.get(8), "adjacency edge_partition")?);
        let edge = temporal_storage::ElementId::new(read_u128(row.get(9), "adjacency edge_id")?);
        let value: Vec<u8> = row.get(10);
        ProjectionRecord::decode(&value)
            .map_err(|error| AdapterError::Backend(error.to_string()))?;
        let key = match tag {
            0x10 => out_adjacency_key(
                graph,
                local_partition,
                local_endpoint,
                edge_type,
                bucket,
                remote_endpoint,
                edge,
            ),
            0x11 => in_adjacency_key(
                graph,
                local_partition,
                local_endpoint,
                edge_type,
                bucket,
                remote_endpoint,
                edge,
            ),
            0x12 => cross_out_adjacency_key(
                graph,
                local_partition,
                local_endpoint,
                edge_type,
                bucket,
                remote_partition,
                remote_endpoint,
                edge_partition,
                edge,
            ),
            0x13 => cross_in_adjacency_key(
                graph,
                local_partition,
                local_endpoint,
                edge_type,
                bucket,
                remote_partition,
                remote_endpoint,
                edge_partition,
                edge,
            ),
            _ => {
                return Err(AdapterError::Backend(format!(
                    "invalid adjacency tag {tag}"
                )));
            }
        };
        if (direction == "out") != matches!(tag, 0x10 | 0x12) {
            return Err(AdapterError::Backend(
                "adjacency direction table and key tag disagree".to_owned(),
            ));
        }
        entries.push(KeyValue::new(key, value));
    }
    Ok(())
}

fn keyspace_from_i16(value: i16) -> Result<Keyspace, AdapterError> {
    match value {
        0 => Ok(Keyspace::Meta),
        1 => Ok(Keyspace::Identity),
        2 => Ok(Keyspace::Current),
        3 => Ok(Keyspace::AdjOut),
        4 => Ok(Keyspace::AdjIn),
        5 => Ok(Keyspace::History),
        6 => Ok(Keyspace::TemporalIndex),
        7 => Ok(Keyspace::Txn),
        other => Err(AdapterError::Backend(format!(
            "invalid keyspace tag {other}"
        ))),
    }
}

fn read_fixed<const N: usize>(bytes: Vec<u8>, label: &str) -> Result<[u8; N], AdapterError> {
    bytes.try_into().map_err(|bytes: Vec<u8>| {
        AdapterError::Backend(format!(
            "PostgreSQL {label} has {} bytes, expected {N}",
            bytes.len()
        ))
    })
}

fn read_u16(bytes: Vec<u8>, label: &str) -> Result<u16, AdapterError> {
    Ok(u16::from_be_bytes(read_fixed(bytes, label)?))
}

fn read_u32(bytes: Vec<u8>, label: &str) -> Result<u32, AdapterError> {
    Ok(u32::from_be_bytes(read_fixed(bytes, label)?))
}

fn read_u64(bytes: Vec<u8>, label: &str) -> Result<u64, AdapterError> {
    Ok(u64::from_be_bytes(read_fixed(bytes, label)?))
}

fn read_u128(bytes: Vec<u8>, label: &str) -> Result<u128, AdapterError> {
    Ok(u128::from_be_bytes(read_fixed(bytes, label)?))
}

fn get_u64(
    transaction: &mut Transaction<'_>,
    instance_id: &str,
    keyspace: Keyspace,
    key: &[u8],
) -> Result<Option<u64>, AdapterError> {
    let row = if keyspace == Keyspace::Txn && key.len() == 9 && key.first() == Some(&1) {
        transaction
            .query_opt(
                "SELECT fingerprint FROM dtgproxy.replay_log WHERE instance_id = $1 AND log_index = $2",
                &[&instance_id, &&key[1..]],
            )
            .map_err(adapter_pg_error)?
    } else if keyspace == Keyspace::Txn && key.len() == 21 && key.first() == Some(&2) {
        transaction
            .query_opt(
                "SELECT fingerprint FROM dtgproxy.replay_mutation WHERE instance_id = $1 AND txn_id = $2 AND sequence = $3",
                &[&instance_id, &&key[1..17], &&key[17..21]],
            )
            .map_err(adapter_pg_error)?
    } else {
        let keyspace = i16::from(keyspace.tag());
        transaction
            .query_opt(
                "SELECT value FROM dtgproxy.opaque_records WHERE instance_id = $1 AND keyspace = $2 AND logical_key = $3",
                &[&instance_id, &keyspace, &key],
            )
            .map_err(adapter_pg_error)?
    };
    row.map(|row| decode_u64_value(row.get(0), "replay fingerprint"))
        .transpose()
}

fn put_value(
    transaction: &mut Transaction<'_>,
    instance_id: &str,
    key: &LogicalKey,
    value: &[u8],
) -> Result<(), AdapterError> {
    put_native(transaction, instance_id, key, value)
}

fn put_raw(
    transaction: &mut Transaction<'_>,
    instance_id: &str,
    keyspace: Keyspace,
    key: &[u8],
    value: &[u8],
) -> Result<(), AdapterError> {
    put_native(
        transaction,
        instance_id,
        &LogicalKey::in_keyspace(keyspace, key.to_vec()),
        value,
    )
}

fn delete_value(
    transaction: &mut Transaction<'_>,
    instance_id: &str,
    key: &LogicalKey,
) -> Result<(), AdapterError> {
    delete_native(transaction, instance_id, key)
}

fn put_native(
    transaction: &mut Transaction<'_>,
    instance_id: &str,
    key: &LogicalKey,
    value: &[u8],
) -> Result<(), AdapterError> {
    let entry = decode_canonical_graph_entry(key, value)
        .map_err(|error| AdapterError::Backend(error.to_string()))?;
    let changed = match entry {
        CanonicalGraphEntry::VertexIdentity { value, .. } => transaction.execute(
            r#"INSERT INTO dtgproxy.vertex_identity(instance_id, graph_id, partition_id, vertex_id, label_id)
               VALUES ($1, $2, $3, $4, $5)
               ON CONFLICT (instance_id, graph_id, partition_id, vertex_id)
               DO UPDATE SET label_id = EXCLUDED.label_id
               WHERE dtgproxy.vertex_identity.label_id = EXCLUDED.label_id"#,
            &[
                &instance_id,
                &value.element().graph().value().to_be_bytes().as_slice(),
                &value.element().partition().value().to_be_bytes().as_slice(),
                &value.element().id().value().to_be_bytes().as_slice(),
                &value.label().value().to_be_bytes().as_slice(),
            ],
        ),
        CanonicalGraphEntry::EdgeIdentity { value, .. } => transaction.execute(
            r#"INSERT INTO dtgproxy.edge_identity(instance_id, graph_id, partition_id, edge_id, edge_type, source_partition, source_id, destination_partition, destination_id)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
               ON CONFLICT (instance_id, graph_id, partition_id, edge_id)
               DO UPDATE SET edge_type = EXCLUDED.edge_type
               WHERE dtgproxy.edge_identity.edge_type = EXCLUDED.edge_type
                 AND dtgproxy.edge_identity.source_partition = EXCLUDED.source_partition
                 AND dtgproxy.edge_identity.source_id = EXCLUDED.source_id
                 AND dtgproxy.edge_identity.destination_partition = EXCLUDED.destination_partition
                 AND dtgproxy.edge_identity.destination_id = EXCLUDED.destination_id"#,
            &[
                &instance_id,
                &value.element().graph().value().to_be_bytes().as_slice(),
                &value.element().partition().value().to_be_bytes().as_slice(),
                &value.element().id().value().to_be_bytes().as_slice(),
                &value.edge_type().value().to_be_bytes().as_slice(),
                &value.source_ref().partition().value().to_be_bytes().as_slice(),
                &value.source_ref().id().value().to_be_bytes().as_slice(),
                &value.destination_ref().partition().value().to_be_bytes().as_slice(),
                &value.destination_ref().id().value().to_be_bytes().as_slice(),
            ],
        ),
        CanonicalGraphEntry::Current { key, .. } => match key {
            GraphKey::CurrentVertex(element) => transaction.execute(
                r#"INSERT INTO dtgproxy.vertex_current(instance_id, graph_id, partition_id, vertex_id, projection)
                   VALUES ($1, $2, $3, $4, $5)
                   ON CONFLICT (instance_id, graph_id, partition_id, vertex_id)
                   DO UPDATE SET projection = EXCLUDED.projection"#,
                &[
                    &instance_id,
                    &element.graph().value().to_be_bytes().as_slice(),
                    &element.partition().value().to_be_bytes().as_slice(),
                    &element.id().value().to_be_bytes().as_slice(),
                    &value,
                ],
            ),
            GraphKey::CurrentEdge(element) => transaction.execute(
                r#"INSERT INTO dtgproxy.edge_current(instance_id, graph_id, partition_id, edge_id, projection)
                   VALUES ($1, $2, $3, $4, $5)
                   ON CONFLICT (instance_id, graph_id, partition_id, edge_id)
                   DO UPDATE SET projection = EXCLUDED.projection"#,
                &[
                    &instance_id,
                    &element.graph().value().to_be_bytes().as_slice(),
                    &element.partition().value().to_be_bytes().as_slice(),
                    &element.id().value().to_be_bytes().as_slice(),
                    &value,
                ],
            ),
            _ => return Err(AdapterError::Backend("invalid Current mapping key".to_owned())),
        },
        CanonicalGraphEntry::Adjacency { key, .. } => {
            let (table, tag, graph, local_partition, local_endpoint, edge_type, bucket, remote_partition, remote_endpoint, edge_partition, edge) =
                adjacency_columns(key)?;
            let statement = if table == "out" {
                r#"INSERT INTO dtgproxy.out_adjacency(instance_id, key_tag, graph_id, local_partition, local_endpoint, edge_type, bucket, remote_partition, remote_endpoint, edge_partition, edge_id, projection)
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                   ON CONFLICT (instance_id, key_tag, graph_id, local_partition, local_endpoint, edge_type, bucket, remote_partition, remote_endpoint, edge_partition, edge_id)
                   DO UPDATE SET projection = EXCLUDED.projection"#
            } else {
                r#"INSERT INTO dtgproxy.in_adjacency(instance_id, key_tag, graph_id, local_partition, local_endpoint, edge_type, bucket, remote_partition, remote_endpoint, edge_partition, edge_id, projection)
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                   ON CONFLICT (instance_id, key_tag, graph_id, local_partition, local_endpoint, edge_type, bucket, remote_partition, remote_endpoint, edge_partition, edge_id)
                   DO UPDATE SET projection = EXCLUDED.projection"#
            };
            transaction.execute(
                statement,
                &[
                    &instance_id,
                    &tag,
                    &graph.as_slice(),
                    &local_partition.as_slice(),
                    &local_endpoint.as_slice(),
                    &edge_type.as_slice(),
                    &bucket.as_slice(),
                    &remote_partition.as_slice(),
                    &remote_endpoint.as_slice(),
                    &edge_partition.as_slice(),
                    &edge.as_slice(),
                    &value,
                ],
            )
        }
        CanonicalGraphEntry::History { key, .. } => {
            let GraphKey::HistoryAnchor {
                element,
                transaction_time,
                segment_id,
            } = key
            else {
                return Err(AdapterError::Backend("invalid History mapping key".to_owned()));
            };
            transaction.execute(
                r#"INSERT INTO dtgproxy.history(instance_id, graph_id, partition_id, element_kind, element_id, transaction_physical, transaction_logical, segment_id, history_value)
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                   ON CONFLICT (instance_id, graph_id, partition_id, element_kind, element_id, transaction_physical, transaction_logical, segment_id)
                   DO UPDATE SET history_value = EXCLUDED.history_value
                   WHERE dtgproxy.history.history_value = EXCLUDED.history_value"#,
                &[
                    &instance_id,
                    &element.graph().value().to_be_bytes().as_slice(),
                    &element.partition().value().to_be_bytes().as_slice(),
                    &(element.kind() as i16),
                    &element.id().value().to_be_bytes().as_slice(),
                    &transaction_time.physical_micros(),
                    &transaction_time.logical().to_be_bytes().as_slice(),
                    &segment_id.to_be_bytes().as_slice(),
                    &value,
                ],
            )
        }
        CanonicalGraphEntry::Opaque { key, value } => {
            if key.keyspace() == Keyspace::Meta
                && key.as_bytes() == ADAPTER_META_APPLIED_LOG_INDEX_KEY
            {
                if value.len() != 8 {
                    return Err(AdapterError::Backend(
                        "PostgreSQL applied-index record is not 8 bytes".to_owned(),
                    ));
                }
                return Ok(());
            }
            if key.keyspace() == Keyspace::Txn
                && key.as_bytes().len() == 9
                && key.as_bytes().first() == Some(&1)
            {
                transaction.execute(
                    r#"INSERT INTO dtgproxy.replay_log(instance_id, log_index, fingerprint)
                       VALUES ($1, $2, $3)
                       ON CONFLICT (instance_id, log_index)
                       DO UPDATE SET fingerprint = EXCLUDED.fingerprint
                       WHERE dtgproxy.replay_log.fingerprint = EXCLUDED.fingerprint"#,
                    &[&instance_id, &&key.as_bytes()[1..], &value],
                )
            } else if key.keyspace() == Keyspace::Txn
                && key.as_bytes().len() == 21
                && key.as_bytes().first() == Some(&2)
            {
                transaction.execute(
                    r#"INSERT INTO dtgproxy.replay_mutation(instance_id, txn_id, sequence, fingerprint)
                       VALUES ($1, $2, $3, $4)
                       ON CONFLICT (instance_id, txn_id, sequence)
                       DO UPDATE SET fingerprint = EXCLUDED.fingerprint
                       WHERE dtgproxy.replay_mutation.fingerprint = EXCLUDED.fingerprint"#,
                    &[
                        &instance_id,
                        &&key.as_bytes()[1..17],
                        &&key.as_bytes()[17..21],
                        &value,
                    ],
                )
            } else {
                let keyspace = i16::from(key.keyspace().tag());
                transaction.execute(
                    r#"INSERT INTO dtgproxy.opaque_records(instance_id, keyspace, logical_key, value)
                       VALUES ($1, $2, $3, $4)
                       ON CONFLICT (instance_id, keyspace, logical_key)
                       DO UPDATE SET value = EXCLUDED.value"#,
                    &[&instance_id, &keyspace, &key.as_bytes(), &value],
                )
            }
        }
    }
    .map_err(adapter_pg_error)?;
    if changed != 1 {
        return Err(AdapterError::Backend(format!(
            "PostgreSQL native Mapping rejected divergent content for {:?}",
            key.keyspace()
        )));
    }
    Ok(())
}

type AdjacencyColumns = (
    &'static str,
    i16,
    [u8; 8],
    [u8; 4],
    [u8; 16],
    [u8; 4],
    [u8; 2],
    [u8; 4],
    [u8; 16],
    [u8; 4],
    [u8; 16],
);

fn adjacency_columns(key: GraphKey) -> Result<AdjacencyColumns, AdapterError> {
    let columns = match key {
        GraphKey::OutAdjacency {
            graph,
            partition,
            source,
            edge_type,
            bucket,
            destination,
            edge,
        } => (
            "out",
            0x10,
            graph.value().to_be_bytes(),
            partition.value().to_be_bytes(),
            source.value().to_be_bytes(),
            edge_type.value().to_be_bytes(),
            bucket.to_be_bytes(),
            partition.value().to_be_bytes(),
            destination.value().to_be_bytes(),
            partition.value().to_be_bytes(),
            edge.value().to_be_bytes(),
        ),
        GraphKey::InAdjacency {
            graph,
            partition,
            destination,
            edge_type,
            bucket,
            source,
            edge,
        } => (
            "in",
            0x11,
            graph.value().to_be_bytes(),
            partition.value().to_be_bytes(),
            destination.value().to_be_bytes(),
            edge_type.value().to_be_bytes(),
            bucket.to_be_bytes(),
            partition.value().to_be_bytes(),
            source.value().to_be_bytes(),
            partition.value().to_be_bytes(),
            edge.value().to_be_bytes(),
        ),
        GraphKey::CrossOutAdjacency {
            graph,
            partition,
            source,
            edge_type,
            bucket,
            destination_partition,
            destination,
            edge_partition,
            edge,
        } => (
            "out",
            0x12,
            graph.value().to_be_bytes(),
            partition.value().to_be_bytes(),
            source.value().to_be_bytes(),
            edge_type.value().to_be_bytes(),
            bucket.to_be_bytes(),
            destination_partition.value().to_be_bytes(),
            destination.value().to_be_bytes(),
            edge_partition.value().to_be_bytes(),
            edge.value().to_be_bytes(),
        ),
        GraphKey::CrossInAdjacency {
            graph,
            partition,
            destination,
            edge_type,
            bucket,
            source_partition,
            source,
            edge_partition,
            edge,
        } => (
            "in",
            0x13,
            graph.value().to_be_bytes(),
            partition.value().to_be_bytes(),
            destination.value().to_be_bytes(),
            edge_type.value().to_be_bytes(),
            bucket.to_be_bytes(),
            source_partition.value().to_be_bytes(),
            source.value().to_be_bytes(),
            edge_partition.value().to_be_bytes(),
            edge.value().to_be_bytes(),
        ),
        _ => {
            return Err(AdapterError::Backend(
                "invalid adjacency mapping key".to_owned(),
            ));
        }
    };
    Ok(columns)
}

fn delete_native(
    transaction: &mut Transaction<'_>,
    instance_id: &str,
    key: &LogicalKey,
) -> Result<(), AdapterError> {
    if key.keyspace() == Keyspace::Meta && key.as_bytes() == ADAPTER_META_APPLIED_LOG_INDEX_KEY {
        return Err(AdapterError::Backend(
            "PostgreSQL Mapping cannot delete the applied-index control record".to_owned(),
        ));
    }
    if matches!(
        key.keyspace(),
        Keyspace::Meta | Keyspace::TemporalIndex | Keyspace::Txn
    ) {
        if key.keyspace() == Keyspace::Txn
            && key.as_bytes().len() == 9
            && key.as_bytes().first() == Some(&1)
        {
            transaction
                .execute(
                    "DELETE FROM dtgproxy.replay_log WHERE instance_id = $1 AND log_index = $2",
                    &[&instance_id, &&key.as_bytes()[1..]],
                )
                .map_err(adapter_pg_error)?;
        } else if key.keyspace() == Keyspace::Txn
            && key.as_bytes().len() == 21
            && key.as_bytes().first() == Some(&2)
        {
            transaction
                .execute(
                    "DELETE FROM dtgproxy.replay_mutation WHERE instance_id = $1 AND txn_id = $2 AND sequence = $3",
                    &[&instance_id, &&key.as_bytes()[1..17], &&key.as_bytes()[17..21]],
                )
                .map_err(adapter_pg_error)?;
        } else {
            let keyspace = i16::from(key.keyspace().tag());
            transaction
                .execute(
                    "DELETE FROM dtgproxy.opaque_records WHERE instance_id = $1 AND keyspace = $2 AND logical_key = $3",
                    &[&instance_id, &keyspace, &key.as_bytes()],
                )
                .map_err(adapter_pg_error)?;
        }
        return Ok(());
    }

    match decode_graph_key(key).map_err(|error| AdapterError::Backend(error.to_string()))? {
        GraphKey::VertexIdentity(element) => delete_entity_row(
            transaction,
            "vertex_identity",
            "vertex_id",
            instance_id,
            element,
        )?,
        GraphKey::EdgeIdentity(element) => delete_entity_row(
            transaction,
            "edge_identity",
            "edge_id",
            instance_id,
            element,
        )?,
        GraphKey::CurrentVertex(element) => delete_entity_row(
            transaction,
            "vertex_current",
            "vertex_id",
            instance_id,
            element,
        )?,
        GraphKey::CurrentEdge(element) => {
            delete_entity_row(transaction, "edge_current", "edge_id", instance_id, element)?
        }
        GraphKey::HistoryAnchor {
            element,
            transaction_time,
            segment_id,
        } => {
            transaction
                .execute(
                    "DELETE FROM dtgproxy.history WHERE instance_id = $1 AND graph_id = $2 AND partition_id = $3 AND element_kind = $4 AND element_id = $5 AND transaction_physical = $6 AND transaction_logical = $7 AND segment_id = $8",
                    &[
                        &instance_id,
                        &element.graph().value().to_be_bytes().as_slice(),
                        &element.partition().value().to_be_bytes().as_slice(),
                        &(element.kind() as i16),
                        &element.id().value().to_be_bytes().as_slice(),
                        &transaction_time.physical_micros(),
                        &transaction_time.logical().to_be_bytes().as_slice(),
                        &segment_id.to_be_bytes().as_slice(),
                    ],
                )
                .map_err(adapter_pg_error)?;
        }
        adjacency @ (GraphKey::OutAdjacency { .. }
        | GraphKey::InAdjacency { .. }
        | GraphKey::CrossOutAdjacency { .. }
        | GraphKey::CrossInAdjacency { .. }) => {
            let (
                table,
                tag,
                graph,
                local_partition,
                local_endpoint,
                edge_type,
                bucket,
                remote_partition,
                remote_endpoint,
                edge_partition,
                edge,
            ) = adjacency_columns(adjacency)?;
            let statement = if table == "out" {
                "DELETE FROM dtgproxy.out_adjacency WHERE instance_id = $1 AND key_tag = $2 AND graph_id = $3 AND local_partition = $4 AND local_endpoint = $5 AND edge_type = $6 AND bucket = $7 AND remote_partition = $8 AND remote_endpoint = $9 AND edge_partition = $10 AND edge_id = $11"
            } else {
                "DELETE FROM dtgproxy.in_adjacency WHERE instance_id = $1 AND key_tag = $2 AND graph_id = $3 AND local_partition = $4 AND local_endpoint = $5 AND edge_type = $6 AND bucket = $7 AND remote_partition = $8 AND remote_endpoint = $9 AND edge_partition = $10 AND edge_id = $11"
            };
            transaction
                .execute(
                    statement,
                    &[
                        &instance_id,
                        &tag,
                        &graph.as_slice(),
                        &local_partition.as_slice(),
                        &local_endpoint.as_slice(),
                        &edge_type.as_slice(),
                        &bucket.as_slice(),
                        &remote_partition.as_slice(),
                        &remote_endpoint.as_slice(),
                        &edge_partition.as_slice(),
                        &edge.as_slice(),
                    ],
                )
                .map_err(adapter_pg_error)?;
        }
    }
    Ok(())
}

fn delete_entity_row(
    transaction: &mut Transaction<'_>,
    table: &'static str,
    id_column: &'static str,
    instance_id: &str,
    element: temporal_storage::ElementRef,
) -> Result<(), AdapterError> {
    let statement = match (table, id_column) {
        ("vertex_identity", "vertex_id") => {
            "DELETE FROM dtgproxy.vertex_identity WHERE instance_id = $1 AND graph_id = $2 AND partition_id = $3 AND vertex_id = $4"
        }
        ("edge_identity", "edge_id") => {
            "DELETE FROM dtgproxy.edge_identity WHERE instance_id = $1 AND graph_id = $2 AND partition_id = $3 AND edge_id = $4"
        }
        ("vertex_current", "vertex_id") => {
            "DELETE FROM dtgproxy.vertex_current WHERE instance_id = $1 AND graph_id = $2 AND partition_id = $3 AND vertex_id = $4"
        }
        ("edge_current", "edge_id") => {
            "DELETE FROM dtgproxy.edge_current WHERE instance_id = $1 AND graph_id = $2 AND partition_id = $3 AND edge_id = $4"
        }
        _ => {
            return Err(AdapterError::Backend(
                "invalid native entity table".to_owned(),
            ));
        }
    };
    transaction
        .execute(
            statement,
            &[
                &instance_id,
                &element.graph().value().to_be_bytes().as_slice(),
                &element.partition().value().to_be_bytes().as_slice(),
                &element.id().value().to_be_bytes().as_slice(),
            ],
        )
        .map_err(adapter_pg_error)?;
    Ok(())
}

fn decode_u64_value(bytes: Vec<u8>, label: &str) -> Result<u64, AdapterError> {
    let bytes: [u8; 8] = bytes.try_into().map_err(|bytes: Vec<u8>| {
        AdapterError::Backend(format!(
            "PostgreSQL {label} has {} bytes, expected 8",
            bytes.len()
        ))
    })?;
    Ok(u64::from_be_bytes(bytes))
}

fn snapshot_entry_bytes(key: &[u8], value: &[u8]) -> usize {
    1_usize
        .saturating_add(8)
        .saturating_add(key.len())
        .saturating_add(8)
        .saturating_add(value.len())
}

struct SnapshotChunkBudget {
    request: LogicalSnapshotExportRequest,
    entries: usize,
    bytes: usize,
}

impl SnapshotChunkBudget {
    const fn new(request: LogicalSnapshotExportRequest) -> Self {
        Self {
            request,
            entries: 0,
            bytes: 0,
        }
    }

    fn try_reserve(&mut self, key: &[u8], value: &[u8]) -> Result<bool, AdapterError> {
        let entry_bytes = snapshot_entry_bytes(key, value);
        if entry_bytes > self.request.max_bytes_per_chunk() {
            return Err(LogicalSnapshotError::EntryTooLarge {
                max: self.request.max_bytes_per_chunk(),
                actual: entry_bytes,
            }
            .into());
        }
        let Some(next_bytes) = self.bytes.checked_add(entry_bytes) else {
            return Err(LogicalSnapshotError::CountOverflow.into());
        };
        if self.entries == self.request.max_entries_per_chunk()
            || next_bytes > self.request.max_bytes_per_chunk()
        {
            return Ok(false);
        }
        self.entries = self
            .entries
            .checked_add(1)
            .ok_or(LogicalSnapshotError::CountOverflow)?;
        self.bytes = next_bytes;
        Ok(true)
    }

    const fn entries(&self) -> usize {
        self.entries
    }

    const fn bytes(&self) -> usize {
        self.bytes
    }
}

fn advisory_lock_keys(instance_id: &str) -> (i32, i32) {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in instance_id.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    ((hash >> 32) as i32, hash as i32)
}

fn configure_client(client: &mut Client) -> Result<(), PostgresAdapterError> {
    client.batch_execute(CONFIGURE_CONNECTION).map_err(pg_error)
}

fn pg_error(error: postgres::Error) -> PostgresAdapterError {
    PostgresAdapterError::Backend(error.to_string())
}

fn adapter_pg_error(error: postgres::Error) -> AdapterError {
    let classification = error.code().map_or("postgres", |code| match *code {
        postgres::error::SqlState::T_R_SERIALIZATION_FAILURE => "retryable serialization",
        postgres::error::SqlState::T_R_DEADLOCK_DETECTED => "retryable deadlock",
        _ => "postgres",
    });
    AdapterError::Backend(format!("{classification}: {error}"))
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PostgresAdapterError {
    InvalidInstanceId,
    InvalidPoolSize { max: usize, actual: usize },
    InstanceAlreadyLeased(String),
    RestoreTargetExists(String),
    UnpublishedInstance(String),
    SchemaVersionMismatch { expected: i32, actual: i32 },
    SchemaFingerprintMismatch,
    CorruptAppliedIndex,
    Backend(String),
}

impl Display for PostgresAdapterError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidInstanceId => formatter.write_str(
                "PostgreSQL instance ID must contain 1..=128 ASCII alphanumeric/._:- bytes",
            ),
            Self::InvalidPoolSize { max, actual } => write!(
                formatter,
                "PostgreSQL pool size {actual} is outside 1..={max}"
            ),
            Self::InstanceAlreadyLeased(instance) => {
                write!(
                    formatter,
                    "PostgreSQL instance {instance} already has a writer lease"
                )
            }
            Self::RestoreTargetExists(instance) => write!(
                formatter,
                "PostgreSQL restore target instance {instance} already exists"
            ),
            Self::UnpublishedInstance(instance) => write!(
                formatter,
                "PostgreSQL instance {instance} contains an unpublished restore namespace"
            ),
            Self::SchemaVersionMismatch { expected, actual } => write!(
                formatter,
                "PostgreSQL Adapter schema version {actual} differs from expected {expected}"
            ),
            Self::SchemaFingerprintMismatch => {
                formatter.write_str("PostgreSQL native Mapping schema fingerprint is incompatible")
            }
            Self::CorruptAppliedIndex => {
                formatter.write_str("PostgreSQL applied index is not an 8-byte value")
            }
            Self::Backend(message) => write!(formatter, "PostgreSQL backend error: {message}"),
        }
    }
}

impl Error for PostgresAdapterError {}

#[cfg(test)]
mod tests {
    use super::{
        ExistingInstanceAction, ExportSlots, OpenMode, SnapshotChunkBudget,
        existing_instance_action,
    };
    use storage_api::LogicalSnapshotExportRequest;

    #[test]
    fn serving_open_rejects_an_unpublished_restore_namespace() {
        assert_eq!(
            existing_instance_action(OpenMode::Serving, false),
            ExistingInstanceAction::RejectUnpublished
        );
        assert_eq!(
            existing_instance_action(OpenMode::Serving, true),
            ExistingInstanceAction::Use
        );
    }

    #[test]
    fn restore_open_reclaims_only_an_unpublished_namespace() {
        assert_eq!(
            existing_instance_action(OpenMode::Restore, false),
            ExistingInstanceAction::Reclaim
        );
        assert_eq!(
            existing_instance_action(OpenMode::Restore, true),
            ExistingInstanceAction::RejectPublished
        );
    }

    #[test]
    fn logical_export_slots_are_bounded_and_released_on_drop() {
        let slots = ExportSlots::new(1);
        let first = slots.try_acquire().unwrap();
        assert!(slots.try_acquire().is_err());
        drop(first);
        assert!(slots.try_acquire().is_ok());
    }

    #[test]
    fn snapshot_budget_defers_a_row_before_exceeding_the_chunk_byte_limit() {
        let request = LogicalSnapshotExportRequest::new(2, 20).unwrap();
        let mut budget = SnapshotChunkBudget::new(request);
        assert!(budget.try_reserve(b"", b"").unwrap());
        assert!(!budget.try_reserve(b"", b"").unwrap());
        assert_eq!(budget.entries(), 1);
        assert_eq!(budget.bytes(), 17);
    }
}
