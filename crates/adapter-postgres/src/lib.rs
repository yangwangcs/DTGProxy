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
use fallible_iterator::FallibleIterator;
use postgres::types::ToSql;
use postgres::{Client, IsolationLevel, NoTls, Transaction};
use storage_api::{
    ADAPTER_META_APPLIED_LOG_INDEX_KEY, AdapterCapabilities, AdapterDescriptorV1, AdapterError,
    AdapterFuture, ApplyReceipt, BackendFamily, CommittedMutationBatch, Durability, KeySpan,
    KeyValue, Keyspace, LogicalKey, LogicalSnapshotAccumulator, LogicalSnapshotChunkV1,
    LogicalSnapshotError, LogicalSnapshotExportRequest, LogicalSnapshotHeaderV1,
    LogicalSnapshotManifestV1, LogicalSnapshotReader, MutationOperation, SnapshotCapability,
    StorageAdapter, adapter_log_fingerprint_key, adapter_mutation_fingerprint_key,
    new_logical_snapshot_id,
};

pub const POSTGRES_SCHEMA_VERSION: i32 = 1;
pub const DEFAULT_POOL_SIZE: usize = 8;
pub const MAX_POOL_SIZE: usize = 128;

pub const POSTGRES_SCHEMA_V1: &str = r#"
CREATE SCHEMA IF NOT EXISTS dtgproxy;
CREATE TABLE IF NOT EXISTS dtgproxy.schema_meta (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    schema_version INTEGER NOT NULL
);
INSERT INTO dtgproxy.schema_meta(singleton, schema_version)
VALUES (TRUE, 1)
ON CONFLICT (singleton) DO NOTHING;
CREATE TABLE IF NOT EXISTS dtgproxy.adapter_instance (
    instance_id TEXT PRIMARY KEY,
    schema_version INTEGER NOT NULL,
    applied_log_index BYTEA NOT NULL CHECK (octet_length(applied_log_index) = 8),
    published BOOLEAN NOT NULL
);
CREATE TABLE IF NOT EXISTS dtgproxy.canonical_kv (
    instance_id TEXT NOT NULL REFERENCES dtgproxy.adapter_instance(instance_id) ON DELETE CASCADE,
    keyspace SMALLINT NOT NULL CHECK (keyspace BETWEEN 0 AND 7),
    logical_key BYTEA NOT NULL,
    value BYTEA NOT NULL,
    PRIMARY KEY (instance_id, keyspace, logical_key)
);
"#;

const CONFIGURE_CONNECTION: &str = r#"
SET statement_timeout = '30s';
SET lock_timeout = '10s';
SET idle_in_transaction_session_timeout = '60s';
"#;
const SCHEMA_LOCK_HIGH: i32 = 0x4454_4750;
const SCHEMA_LOCK_LOW: i32 = 1;

const SELECT_INSTANCE_FOR_UPDATE: &str = "SELECT schema_version, applied_log_index, published FROM dtgproxy.adapter_instance WHERE instance_id = $1 FOR UPDATE";
const SELECT_INSTANCE: &str = "SELECT schema_version, applied_log_index, published FROM dtgproxy.adapter_instance WHERE instance_id = $1";
const INSERT_INSTANCE: &str = "INSERT INTO dtgproxy.adapter_instance(instance_id, schema_version, applied_log_index, published) VALUES ($1, $2, $3, $4)";
const SELECT_VALUE: &str = "SELECT value FROM dtgproxy.canonical_kv WHERE instance_id = $1 AND keyspace = $2 AND logical_key = $3";
const UPSERT_VALUE: &str = r#"
INSERT INTO dtgproxy.canonical_kv(instance_id, keyspace, logical_key, value)
VALUES ($1, $2, $3, $4)
ON CONFLICT (instance_id, keyspace, logical_key)
DO UPDATE SET value = EXCLUDED.value
"#;
const RESTORE_VALUE: &str = r#"
INSERT INTO dtgproxy.canonical_kv(instance_id, keyspace, logical_key, value)
VALUES ($1, $2, $3, $4)
ON CONFLICT (instance_id, keyspace, logical_key)
DO UPDATE SET value = EXCLUDED.value
WHERE dtgproxy.canonical_kv.value = EXCLUDED.value
"#;
const DELETE_VALUE: &str = "DELETE FROM dtgproxy.canonical_kv WHERE instance_id = $1 AND keyspace = $2 AND logical_key = $3";
const UPDATE_APPLIED_INDEX: &str = "UPDATE dtgproxy.adapter_instance SET applied_log_index = $2 WHERE instance_id = $1 AND published = TRUE";
const PUBLISH_RESTORE: &str = "UPDATE dtgproxy.adapter_instance SET applied_log_index = $2, published = TRUE WHERE instance_id = $1 AND published = FALSE";
const SCAN_WITH_END_AND_LIMIT: &str = r#"
SELECT logical_key, value FROM dtgproxy.canonical_kv
WHERE instance_id = $1 AND keyspace = $2 AND logical_key >= $3 AND logical_key < $4
ORDER BY logical_key LIMIT $5
"#;
const SCAN_WITH_END: &str = r#"
SELECT logical_key, value FROM dtgproxy.canonical_kv
WHERE instance_id = $1 AND keyspace = $2 AND logical_key >= $3 AND logical_key < $4
ORDER BY logical_key
"#;
const SCAN_WITH_LIMIT: &str = r#"
SELECT logical_key, value FROM dtgproxy.canonical_kv
WHERE instance_id = $1 AND keyspace = $2 AND logical_key >= $3
ORDER BY logical_key LIMIT $4
"#;
const SCAN_UNBOUNDED: &str = r#"
SELECT logical_key, value FROM dtgproxy.canonical_kv
WHERE instance_id = $1 AND keyspace = $2 AND logical_key >= $3
ORDER BY logical_key
"#;

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
    fn provider_name(&self) -> &'static str {
        "postgresql"
    }

    fn open<'a>(&'a self, request: &'a AdapterOpenRequest) -> AdapterFactoryFuture<'a> {
        Box::pin(async move {
            let (connection_string, pool_size) = factory_configuration(request)?;
            let adapter = PostgresAdapter::connect(
                connection_string,
                request.instance_id(),
                pool_size,
                OpenMode::Serving,
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
            .batch_execute(POSTGRES_SCHEMA_V1)
            .map_err(pg_error)?;
        let global_schema_version: i32 = lease_client
            .query_one(
                "SELECT schema_version FROM dtgproxy.schema_meta WHERE singleton = TRUE",
                &[],
            )
            .map_err(pg_error)?
            .get(0);
        if global_schema_version != POSTGRES_SCHEMA_VERSION {
            return Err(PostgresAdapterError::SchemaVersionMismatch {
                expected: POSTGRES_SCHEMA_VERSION,
                actual: global_schema_version,
            });
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
                let (_, _, published) = validate_instance_row(row)?;
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
                        &0_u64.to_be_bytes().as_slice(),
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
        let mut values = Vec::with_capacity(keys.len());
        for key in keys {
            let keyspace = i16::from(key.keyspace().tag());
            values.push(
                transaction
                    .query_opt(
                        SELECT_VALUE,
                        &[&self.instance_id, &keyspace, &key.as_bytes()],
                    )
                    .map_err(adapter_pg_error)?
                    .map(|row| row.get(0)),
            );
        }
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
        let keyspace = i16::from(span.keyspace().tag());
        let limit =
            span.limit().map(i64::try_from).transpose().map_err(|_| {
                AdapterError::Backend("PostgreSQL scan limit exceeds i64".to_owned())
            })?;
        let rows = match (span.end(), limit) {
            (Some(end), Some(limit)) => transaction
                .query(
                    SCAN_WITH_END_AND_LIMIT,
                    &[&self.instance_id, &keyspace, &span.start(), &end, &limit],
                )
                .map_err(adapter_pg_error)?,
            (Some(end), None) => transaction
                .query(
                    SCAN_WITH_END,
                    &[&self.instance_id, &keyspace, &span.start(), &end],
                )
                .map_err(adapter_pg_error)?,
            (None, Some(limit)) => transaction
                .query(
                    SCAN_WITH_LIMIT,
                    &[&self.instance_id, &keyspace, &span.start(), &limit],
                )
                .map_err(adapter_pg_error)?,
            (None, None) => transaction
                .query(
                    SCAN_UNBOUNDED,
                    &[&self.instance_id, &keyspace, &span.start()],
                )
                .map_err(adapter_pg_error)?,
        };
        let mut values = Vec::with_capacity(rows.len());
        for row in rows {
            let key: Vec<u8> = row.get(0);
            if !span.contains(&key) {
                break;
            }
            values.push(KeyValue::new(
                LogicalKey::in_keyspace(span.keyspace(), key),
                row.get(1),
            ));
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
        let header = LogicalSnapshotHeaderV1::new(new_logical_snapshot_id(), applied_log_index);
        Ok(PostgresLogicalSnapshotReader {
            client,
            instance_id: self.instance_id.clone(),
            request,
            accumulator: LogicalSnapshotAccumulator::new(header.clone()),
            header,
            keyspace_index: 0,
            next_key: None,
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
            let keyspace = i16::from(entry.key().keyspace().tag());
            let changed = transaction
                .execute(
                    RESTORE_VALUE,
                    &[
                        &self.instance_id,
                        &keyspace,
                        &entry.key().as_bytes(),
                        &entry.value(),
                    ],
                )
                .map_err(adapter_pg_error)?;
            if changed != 1 {
                return Err(AdapterError::Backend(
                    "PostgreSQL restore encountered divergent existing content".to_owned(),
                ));
            }
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
                &[&self.instance_id, &applied_index.to_be_bytes().as_slice()],
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
                "DELETE FROM dtgproxy.canonical_kv WHERE instance_id = $1",
                &[&self.instance_id],
            )
            .map_err(adapter_pg_error)?;
        transaction
            .execute(
                "DELETE FROM dtgproxy.adapter_instance WHERE instance_id = $1",
                &[&self.instance_id],
            )
            .map_err(adapter_pg_error)?;
        transaction.commit().map_err(adapter_pg_error)
    }
}

impl StorageAdapter for PostgresAdapter {
    fn descriptor(&self) -> AdapterDescriptorV1 {
        AdapterDescriptorV1::new(
            "postgresql",
            self.server_version.clone(),
            BackendFamily::Sql,
            self.capabilities(),
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

struct PostgresLogicalSnapshotReader {
    client: Client,
    instance_id: String,
    request: LogicalSnapshotExportRequest,
    header: LogicalSnapshotHeaderV1,
    accumulator: LogicalSnapshotAccumulator,
    keyspace_index: usize,
    next_key: Option<Vec<u8>>,
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
        'keyspaces: while self.keyspace_index < Keyspace::ALL.len() {
            let keyspace = Keyspace::ALL[self.keyspace_index];
            let start = self.next_key.clone().unwrap_or_default();
            let keyspace_tag = i16::from(keyspace.tag());
            let query_limit = i64::try_from(self.request.max_entries_per_chunk())
                .map_err(|_| LogicalSnapshotError::CountOverflow)?;
            let params: [&(dyn ToSql + Sync); 4] =
                [&self.instance_id, &keyspace_tag, &start, &query_limit];
            let mut rows = self
                .client
                .query_raw(SCAN_WITH_LIMIT, params)
                .map_err(adapter_pg_error)?;
            while let Some(row) = rows.next().map_err(adapter_pg_error)? {
                let key: Vec<u8> = row.get(0);
                let value: Vec<u8> = row.get(1);
                if !budget.try_reserve(&key, &value)? {
                    self.next_key = Some(key);
                    break 'keyspaces;
                }
                entries.push(KeyValue::new(LogicalKey::in_keyspace(keyspace, key), value));
                if budget.entries() == self.request.max_entries_per_chunk()
                    || budget.bytes() == self.request.max_bytes_per_chunk()
                {
                    self.next_key = entries
                        .last()
                        .map(|entry| strict_successor(entry.key().as_bytes()));
                    break 'keyspaces;
                }
            }
            self.keyspace_index += 1;
            self.next_key = None;
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
            self.published = true;
            Ok(Arc::new(adapter) as Arc<dyn StorageAdapter>)
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

fn validate_instance_row(row: &postgres::Row) -> Result<(i32, u64, bool), PostgresAdapterError> {
    let version: i32 = row.get(0);
    if version != POSTGRES_SCHEMA_VERSION {
        return Err(PostgresAdapterError::SchemaVersionMismatch {
            expected: POSTGRES_SCHEMA_VERSION,
            actual: version,
        });
    }
    let index: Vec<u8> = row.get(1);
    let index: [u8; 8] = index
        .try_into()
        .map_err(|_| PostgresAdapterError::CorruptAppliedIndex)?;
    let published: bool = row.get(2);
    Ok((version, u64::from_be_bytes(index), published))
}

fn validate_instance_row_for_adapter(row: &postgres::Row) -> Result<(i32, u64), AdapterError> {
    let (version, index, published) =
        validate_instance_row(row).map_err(|error| AdapterError::Backend(error.to_string()))?;
    if !published {
        return Err(AdapterError::Backend(
            "PostgreSQL Adapter instance is not published".to_owned(),
        ));
    }
    Ok((version, index))
}

fn validate_instance_row_for_restore(row: &postgres::Row) -> Result<(i32, u64), AdapterError> {
    let (version, index, published) =
        validate_instance_row(row).map_err(|error| AdapterError::Backend(error.to_string()))?;
    if published {
        return Err(AdapterError::Backend(
            "PostgreSQL restore target is already published".to_owned(),
        ));
    }
    Ok((version, index))
}

fn get_u64(
    transaction: &mut Transaction<'_>,
    instance_id: &str,
    keyspace: Keyspace,
    key: &[u8],
) -> Result<Option<u64>, AdapterError> {
    let keyspace = i16::from(keyspace.tag());
    transaction
        .query_opt(SELECT_VALUE, &[&instance_id, &keyspace, &key])
        .map_err(adapter_pg_error)?
        .map(|row| {
            let bytes: Vec<u8> = row.get(0);
            let bytes: [u8; 8] = bytes.try_into().map_err(|bytes: Vec<u8>| {
                AdapterError::Backend(format!(
                    "PostgreSQL replay fingerprint has {} bytes, expected 8",
                    bytes.len()
                ))
            })?;
            Ok(u64::from_be_bytes(bytes))
        })
        .transpose()
}

fn put_value(
    transaction: &mut Transaction<'_>,
    instance_id: &str,
    key: &LogicalKey,
    value: &[u8],
) -> Result<(), AdapterError> {
    put_raw(
        transaction,
        instance_id,
        key.keyspace(),
        key.as_bytes(),
        value,
    )
}

fn put_raw(
    transaction: &mut Transaction<'_>,
    instance_id: &str,
    keyspace: Keyspace,
    key: &[u8],
    value: &[u8],
) -> Result<(), AdapterError> {
    let keyspace = i16::from(keyspace.tag());
    transaction
        .execute(UPSERT_VALUE, &[&instance_id, &keyspace, &key, &value])
        .map_err(adapter_pg_error)?;
    Ok(())
}

fn delete_value(
    transaction: &mut Transaction<'_>,
    instance_id: &str,
    key: &LogicalKey,
) -> Result<(), AdapterError> {
    let keyspace = i16::from(key.keyspace().tag());
    transaction
        .execute(DELETE_VALUE, &[&instance_id, &keyspace, &key.as_bytes()])
        .map_err(adapter_pg_error)?;
    Ok(())
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

fn strict_successor(key: &[u8]) -> Vec<u8> {
    let mut successor = Vec::with_capacity(key.len().saturating_add(1));
    successor.extend_from_slice(key);
    successor.push(0);
    successor
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
