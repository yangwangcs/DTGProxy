#![forbid(unsafe_code)]

use std::collections::BTreeSet;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use adapter_registry::{
    AdapterFactory, AdapterFactoryError, AdapterFactoryFuture, AdapterOpenRequest,
    AdapterRestoreFuture, AdapterRestoreSession, AdapterRestoreSessionFuture,
};
use rocksdb::{
    BoundColumnFamily, ColumnFamilyDescriptor, DBWithThreadMode, Direction, IteratorMode,
    MultiThreaded, Options, SnapshotWithThreadMode, WriteBatch, WriteOptions,
};
use storage_api::{
    AdapterCapabilities, AdapterDescriptorV1, AdapterError, AdapterFuture, ApplyReceipt,
    BackendFamily, CommittedMutationBatch, Durability, KeySpan, KeyValue, Keyspace, LogicalKey,
    LogicalSnapshotAccumulator, LogicalSnapshotChunkV1, LogicalSnapshotError,
    LogicalSnapshotExportRequest, LogicalSnapshotHeaderV1, LogicalSnapshotManifestV1,
    LogicalSnapshotReader, MutationOperation, SnapshotCapability, StorageAdapter,
};

const APPLIED_LOG_INDEX_KEY: &[u8] = b"\x00applied_log_index";
const LOG_FINGERPRINT_PREFIX: u8 = 0x01;
const MUTATION_FINGERPRINT_PREFIX: u8 = 0x02;
static NEXT_SNAPSHOT_ID: AtomicU64 = AtomicU64::new(1);

type RocksDb = DBWithThreadMode<MultiThreaded>;

pub struct RocksAdapterFactory;

impl AdapterFactory for RocksAdapterFactory {
    fn provider_name(&self) -> &'static str {
        "rocksdb"
    }

    fn open<'a>(&'a self, request: &'a AdapterOpenRequest) -> AdapterFactoryFuture<'a> {
        Box::pin(async move {
            let path = request.parameter("path").ok_or_else(|| {
                AdapterFactoryError::new("RocksDB Adapter requires the public parameter path")
            })?;
            let adapter = RocksAdapter::open(path)
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
            let target_path = request.parameter("path").ok_or_else(|| {
                AdapterFactoryError::new("RocksDB Adapter requires the public parameter path")
            })?;
            let target_path = PathBuf::from(target_path);
            if target_path.exists() {
                return Err(AdapterFactoryError::new(format!(
                    "RocksDB restore target {} already exists",
                    target_path.display()
                )));
            }
            let parent = nonempty_parent(&target_path);
            if !parent.is_dir() {
                return Err(AdapterFactoryError::new(format!(
                    "RocksDB restore parent {} does not exist",
                    parent.display()
                )));
            }
            let target_name = target_path
                .file_name()
                .ok_or_else(|| AdapterFactoryError::new("RocksDB restore target has no name"))?
                .to_string_lossy();
            let staging_path = parent.join(format!(
                ".{target_name}.dtg-restore-{:032x}",
                header.snapshot_id()
            ));
            if staging_path.exists() {
                return Err(AdapterFactoryError::new(format!(
                    "RocksDB restore staging path {} already exists",
                    staging_path.display()
                )));
            }
            let adapter = RocksAdapter::open(&staging_path)
                .map_err(|error| AdapterFactoryError::new(error.to_string()))?;
            Ok(Box::new(RocksRestoreSession {
                target_path,
                staging_path,
                adapter: Some(adapter),
                accumulator: LogicalSnapshotAccumulator::new(header.clone()),
                header,
                saw_applied_index_record: false,
            }) as Box<dyn AdapterRestoreSession + 'a>)
        })
    }
}

pub struct RocksAdapter {
    path: PathBuf,
    db: RocksDb,
    apply_guard: Mutex<()>,
}

impl RocksAdapter {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, AdapterError> {
        let path = path.as_ref().to_path_buf();
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);

        let mut descriptors = Vec::with_capacity(Keyspace::ALL.len() + 1);
        descriptors.push(ColumnFamilyDescriptor::new("default", Options::default()));
        descriptors.extend(Keyspace::ALL.into_iter().map(|keyspace| {
            ColumnFamilyDescriptor::new(keyspace.column_family(), Options::default())
        }));

        let db =
            RocksDb::open_cf_descriptors(&options, &path, descriptors).map_err(backend_error)?;

        Ok(Self {
            path,
            db,
            apply_guard: Mutex::new(()),
        })
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
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
            snapshot: SnapshotCapability::PhysicalCheckpoint,
            logical_export: true,
            logical_restore: true,
            predicate_pushdown: false,
            adjacency_pushdown: false,
            change_feed: false,
        }
    }

    pub fn column_family_names(&self) -> Result<Vec<String>, AdapterError> {
        let mut names = RocksDb::list_cf(&Options::default(), &self.path).map_err(backend_error)?;
        names.sort();
        Ok(names)
    }

    pub fn checkpoint(&self, destination: impl AsRef<Path>) -> Result<(), AdapterError> {
        let checkpoint = rocksdb::checkpoint::Checkpoint::new(&self.db).map_err(backend_error)?;
        checkpoint
            .create_checkpoint(destination)
            .map_err(backend_error)
    }

    fn cf(&self, keyspace: Keyspace) -> Result<Arc<BoundColumnFamily<'_>>, AdapterError> {
        self.db.cf_handle(keyspace.column_family()).ok_or_else(|| {
            AdapterError::Backend(format!(
                "missing column family {}",
                keyspace.column_family()
            ))
        })
    }

    fn read_u64(&self, keyspace: Keyspace, key: &[u8]) -> Result<Option<u64>, AdapterError> {
        let cf = self.cf(keyspace)?;
        let value = self.db.get_cf(&cf, key).map_err(backend_error)?;
        value.map_or(Ok(None), |bytes| {
            let bytes: [u8; 8] = bytes.try_into().map_err(|bytes: Vec<u8>| {
                AdapterError::Backend(format!(
                    "corrupt u64 metadata in {}: expected 8 bytes, got {}",
                    keyspace.column_family(),
                    bytes.len()
                ))
            })?;
            Ok(Some(u64::from_be_bytes(bytes)))
        })
    }

    fn current_applied_log_index(&self) -> Result<u64, AdapterError> {
        Ok(self
            .read_u64(Keyspace::Meta, APPLIED_LOG_INDEX_KEY)?
            .unwrap_or(0))
    }

    fn apply(&self, batch: CommittedMutationBatch) -> Result<ApplyReceipt, AdapterError> {
        let _apply_guard = self
            .apply_guard
            .lock()
            .map_err(|_| AdapterError::LockPoisoned)?;
        let applied_log_index = self.current_applied_log_index()?;
        let batch_fingerprint = batch.fingerprint();

        if batch.log_index <= applied_log_index {
            let stored = self.read_u64(Keyspace::Txn, &log_fingerprint_key(batch.log_index))?;
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

        let mut batch_sequences = BTreeSet::new();
        let mut mutation_fingerprints = Vec::with_capacity(batch.mutations.len());
        for mutation in &batch.mutations {
            if !batch_sequences.insert(mutation.sequence) {
                return Err(AdapterError::DuplicateMutationSequence {
                    txn_id: batch.txn_id,
                    sequence: mutation.sequence,
                });
            }

            let fingerprint = mutation.fingerprint();
            let metadata_key = mutation_fingerprint_key(batch.txn_id, mutation.sequence);
            if let Some(previous) = self.read_u64(Keyspace::Txn, &metadata_key)?
                && previous != fingerprint
            {
                return Err(AdapterError::MutationReplayMismatch {
                    txn_id: batch.txn_id,
                    sequence: mutation.sequence,
                });
            }
            mutation_fingerprints.push((metadata_key, fingerprint));
        }

        let mut write_batch = WriteBatch::default();
        for mutation in &batch.mutations {
            match &mutation.operation {
                MutationOperation::Put { key, value } => {
                    let cf = self.cf(key.keyspace())?;
                    write_batch.put_cf(&cf, key.as_bytes(), value);
                }
                MutationOperation::Delete { key } => {
                    let cf = self.cf(key.keyspace())?;
                    write_batch.delete_cf(&cf, key.as_bytes());
                }
            }
        }

        let txn_cf = self.cf(Keyspace::Txn)?;
        for (metadata_key, fingerprint) in mutation_fingerprints {
            write_batch.put_cf(&txn_cf, metadata_key, fingerprint.to_be_bytes());
        }
        write_batch.put_cf(
            &txn_cf,
            log_fingerprint_key(batch.log_index),
            batch_fingerprint.to_be_bytes(),
        );

        let meta_cf = self.cf(Keyspace::Meta)?;
        write_batch.put_cf(
            &meta_cf,
            APPLIED_LOG_INDEX_KEY,
            batch.log_index.to_be_bytes(),
        );

        let mut write_options = WriteOptions::default();
        write_options.set_sync(true);
        write_options.disable_wal(false);
        self.db
            .write_opt(write_batch, &write_options)
            .map_err(backend_error)?;

        Ok(ApplyReceipt {
            applied_log_index: batch.log_index,
            duplicate: false,
        })
    }

    fn restore_logical_entries(
        &self,
        entries: &[KeyValue],
        expected_applied_index: u64,
    ) -> Result<bool, AdapterError> {
        let _apply_guard = self
            .apply_guard
            .lock()
            .map_err(|_| AdapterError::LockPoisoned)?;
        if self.current_applied_log_index()? != 0 {
            return Err(AdapterError::Backend(
                "logical restore target already has an applied log index".to_owned(),
            ));
        }
        let mut write_batch = WriteBatch::default();
        let mut writes = 0_usize;
        let mut saw_applied_index = false;
        for entry in entries {
            if entry.key().keyspace() == Keyspace::Meta
                && entry.key().as_bytes() == APPLIED_LOG_INDEX_KEY
            {
                if entry.value() != expected_applied_index.to_be_bytes() {
                    return Err(AdapterError::Backend(
                        "logical snapshot applied-index record differs from its header".to_owned(),
                    ));
                }
                saw_applied_index = true;
                continue;
            }
            let cf = self.cf(entry.key().keyspace())?;
            write_batch.put_cf(&cf, entry.key().as_bytes(), entry.value());
            writes = writes.saturating_add(1);
        }
        if writes != 0 {
            self.write_sync(write_batch)?;
        }
        Ok(saw_applied_index)
    }

    fn publish_restored_applied_index(
        &self,
        applied_log_index: u64,
        source_had_record: bool,
    ) -> Result<(), AdapterError> {
        if applied_log_index == 0 && !source_had_record {
            return Ok(());
        }
        let mut write_batch = WriteBatch::default();
        let meta_cf = self.cf(Keyspace::Meta)?;
        write_batch.put_cf(
            &meta_cf,
            APPLIED_LOG_INDEX_KEY,
            applied_log_index.to_be_bytes(),
        );
        self.write_sync(write_batch)
    }

    fn write_sync(&self, write_batch: WriteBatch) -> Result<(), AdapterError> {
        let mut write_options = WriteOptions::default();
        write_options.set_sync(true);
        write_options.disable_wal(false);
        self.db
            .write_opt(write_batch, &write_options)
            .map_err(backend_error)
    }
}

struct RocksRestoreSession {
    target_path: PathBuf,
    staging_path: PathBuf,
    adapter: Option<RocksAdapter>,
    header: LogicalSnapshotHeaderV1,
    accumulator: LogicalSnapshotAccumulator,
    saw_applied_index_record: bool,
}

impl AdapterRestoreSession for RocksRestoreSession {
    fn write_chunk<'a>(
        &'a mut self,
        chunk: LogicalSnapshotChunkV1,
    ) -> AdapterRestoreFuture<'a, ()> {
        Box::pin(async move {
            let mut next_accumulator = self.accumulator.clone();
            next_accumulator
                .observe(&chunk)
                .map_err(|error| AdapterFactoryError::new(error.to_string()))?;
            let adapter = self.adapter.as_ref().ok_or_else(|| {
                AdapterFactoryError::new("RocksDB restore session is already finished")
            })?;
            let saw_applied_index = adapter
                .restore_logical_entries(chunk.entries(), self.header.applied_log_index())
                .map_err(|error| AdapterFactoryError::new(error.to_string()))?;
            self.saw_applied_index_record |= saw_applied_index;
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
            let adapter = self.adapter.take().ok_or_else(|| {
                AdapterFactoryError::new("RocksDB restore session is already finished")
            })?;
            adapter
                .publish_restored_applied_index(
                    self.header.applied_log_index(),
                    self.saw_applied_index_record,
                )
                .map_err(|error| AdapterFactoryError::new(error.to_string()))?;
            drop(adapter);
            sync_tree(&self.staging_path)?;
            fs::rename(&self.staging_path, &self.target_path)
                .map_err(|error| AdapterFactoryError::new(error.to_string()))?;
            File::open(nonempty_parent(&self.target_path))
                .and_then(|directory| directory.sync_all())
                .map_err(|error| AdapterFactoryError::new(error.to_string()))?;
            let restored = RocksAdapter::open(&self.target_path)
                .map_err(|error| AdapterFactoryError::new(error.to_string()))?;
            let actual_index = restored
                .applied_log_index()
                .map_err(|error| AdapterFactoryError::new(error.to_string()))?;
            if actual_index != self.header.applied_log_index() {
                return Err(AdapterFactoryError::new(format!(
                    "restored applied index {actual_index} differs from snapshot index {}",
                    self.header.applied_log_index()
                )));
            }
            Ok(Arc::new(restored) as Arc<dyn StorageAdapter>)
        })
    }
}

impl Drop for RocksRestoreSession {
    fn drop(&mut self) {
        drop(self.adapter.take());
        if self.staging_path.exists() {
            let _ = fs::remove_dir_all(&self.staging_path);
        }
    }
}

fn nonempty_parent(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

fn sync_tree(root: &Path) -> Result<(), AdapterFactoryError> {
    let mut entries = fs::read_dir(root)
        .map_err(|error| AdapterFactoryError::new(error.to_string()))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| AdapterFactoryError::new(error.to_string()))?;
    entries.sort_by_key(fs::DirEntry::file_name);
    for entry in entries {
        let path = entry.path();
        if entry
            .file_type()
            .map_err(|error| AdapterFactoryError::new(error.to_string()))?
            .is_dir()
        {
            sync_tree(&path)?;
        } else {
            File::open(&path)
                .and_then(|file| file.sync_all())
                .map_err(|error| AdapterFactoryError::new(error.to_string()))?;
        }
    }
    File::open(root)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| AdapterFactoryError::new(error.to_string()))
}

impl StorageAdapter for RocksAdapter {
    fn descriptor(&self) -> AdapterDescriptorV1 {
        AdapterDescriptorV1::new(
            "rocksdb",
            env!("CARGO_PKG_VERSION"),
            BackendFamily::KeyValue,
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
        Box::pin(async move {
            let handles: Vec<_> = keys
                .iter()
                .map(|key| self.cf(key.keyspace()))
                .collect::<Result<_, _>>()?;
            let snapshot = self.db.snapshot();
            snapshot
                .multi_get_cf(
                    handles
                        .iter()
                        .zip(keys)
                        .map(|(cf, key)| (cf, key.as_bytes())),
                )
                .into_iter()
                .map(|result| result.map_err(backend_error))
                .collect()
        })
    }

    fn scan<'a>(&'a self, span: &'a KeySpan) -> AdapterFuture<'a, Vec<KeyValue>> {
        Box::pin(async move {
            let cf = self.cf(span.keyspace())?;
            let snapshot = self.db.snapshot();
            let iterator =
                snapshot.iterator_cf(&cf, IteratorMode::From(span.start(), Direction::Forward));
            let mut values = Vec::new();
            for item in iterator {
                let (key, value) = item.map_err(backend_error)?;
                if !span.contains(&key) {
                    break;
                }
                values.push(KeyValue::new(
                    LogicalKey::in_keyspace(span.keyspace(), key.into_vec()),
                    value.into_vec(),
                ));
                if values.len() == span.limit().unwrap_or(usize::MAX) {
                    break;
                }
            }
            Ok(values)
        })
    }

    fn begin_logical_export<'a>(
        &'a self,
        request: LogicalSnapshotExportRequest,
    ) -> AdapterFuture<'a, Box<dyn LogicalSnapshotReader + 'a>> {
        Box::pin(async move {
            let apply_guard = self
                .apply_guard
                .lock()
                .map_err(|_| AdapterError::LockPoisoned)?;
            let applied_log_index = self.current_applied_log_index()?;
            let snapshot = self.db.snapshot();
            drop(apply_guard);
            let header = LogicalSnapshotHeaderV1::new(snapshot_id(), applied_log_index);
            Ok(Box::new(RocksLogicalSnapshotReader {
                adapter: self,
                snapshot,
                request,
                accumulator: LogicalSnapshotAccumulator::new(header.clone()),
                header,
                keyspace_index: 0,
                next_key: None,
                next_ordinal: 0,
                exhausted: false,
            }) as Box<dyn LogicalSnapshotReader + 'a>)
        })
    }

    fn applied_log_index(&self) -> Result<u64, AdapterError> {
        self.current_applied_log_index()
    }
}

struct RocksLogicalSnapshotReader<'a> {
    adapter: &'a RocksAdapter,
    snapshot: SnapshotWithThreadMode<'a, RocksDb>,
    request: LogicalSnapshotExportRequest,
    header: LogicalSnapshotHeaderV1,
    accumulator: LogicalSnapshotAccumulator,
    keyspace_index: usize,
    next_key: Option<Vec<u8>>,
    next_ordinal: u64,
    exhausted: bool,
}

impl RocksLogicalSnapshotReader<'_> {
    fn read_next_chunk(&mut self) -> Result<Option<LogicalSnapshotChunkV1>, AdapterError> {
        if self.exhausted {
            return Ok(None);
        }
        let mut entries = Vec::new();
        let mut payload_bytes = 0_usize;
        'keyspaces: while self.keyspace_index < Keyspace::ALL.len() {
            let keyspace = Keyspace::ALL[self.keyspace_index];
            let cf = self.adapter.cf(keyspace)?;
            let start = self.next_key.as_deref().unwrap_or_default();
            let iterator = self
                .snapshot
                .iterator_cf(&cf, IteratorMode::From(start, Direction::Forward));
            for item in iterator {
                let (key, value) = item.map_err(backend_error)?;
                let entry_bytes = snapshot_entry_bytes(&key, &value);
                if entry_bytes > self.request.max_bytes_per_chunk() {
                    return Err(LogicalSnapshotError::EntryTooLarge {
                        max: self.request.max_bytes_per_chunk(),
                        actual: entry_bytes,
                    }
                    .into());
                }
                let would_exceed_entries = entries.len() == self.request.max_entries_per_chunk();
                let would_exceed_bytes = payload_bytes
                    .checked_add(entry_bytes)
                    .is_none_or(|bytes| bytes > self.request.max_bytes_per_chunk());
                if would_exceed_entries || would_exceed_bytes {
                    self.next_key = Some(key.into_vec());
                    break 'keyspaces;
                }
                payload_bytes = payload_bytes
                    .checked_add(entry_bytes)
                    .ok_or(LogicalSnapshotError::CountOverflow)?;
                entries.push(KeyValue::new(
                    LogicalKey::in_keyspace(keyspace, key.into_vec()),
                    value.into_vec(),
                ));
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

impl LogicalSnapshotReader for RocksLogicalSnapshotReader<'_> {
    fn header(&self) -> &LogicalSnapshotHeaderV1 {
        &self.header
    }

    fn next_chunk<'a>(&'a mut self) -> AdapterFuture<'a, Option<LogicalSnapshotChunkV1>> {
        Box::pin(async move { self.read_next_chunk() })
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

fn snapshot_entry_bytes(key: &[u8], value: &[u8]) -> usize {
    1_usize
        .saturating_add(8)
        .saturating_add(key.len())
        .saturating_add(8)
        .saturating_add(value.len())
}

fn snapshot_id() -> u128 {
    let time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    let process_and_time = (time as u64) ^ (u64::from(std::process::id()) << 32);
    let sequence = NEXT_SNAPSHOT_ID.fetch_add(1, Ordering::Relaxed);
    (u128::from(sequence) << 64) | u128::from(process_and_time)
}

fn log_fingerprint_key(log_index: u64) -> [u8; 9] {
    let mut key = [0; 9];
    key[0] = LOG_FINGERPRINT_PREFIX;
    key[1..].copy_from_slice(&log_index.to_be_bytes());
    key
}

fn mutation_fingerprint_key(txn_id: u128, sequence: u32) -> [u8; 21] {
    let mut key = [0; 21];
    key[0] = MUTATION_FINGERPRINT_PREFIX;
    key[1..17].copy_from_slice(&txn_id.to_be_bytes());
    key[17..].copy_from_slice(&sequence.to_be_bytes());
    key
}

fn backend_error(error: rocksdb::Error) -> AdapterError {
    AdapterError::Backend(error.to_string())
}
