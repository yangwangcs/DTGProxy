#![forbid(unsafe_code)]

use std::collections::BTreeSet;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use adapter_registry::{
    AdapterFactory, AdapterFactoryError, AdapterFactoryFuture, AdapterOpenRequest,
    AdapterRestoreFuture, AdapterRestoreSession, AdapterRestoreSessionFuture,
};
use rocksdb::{
    BoundColumnFamily, ColumnFamilyDescriptor, DBWithThreadMode, Direction, IteratorMode,
    MultiThreaded, Options, SnapshotWithThreadMode, WriteBatch, WriteOptions,
};
use storage_api::{
    ADAPTER_META_APPLIED_LOG_INDEX_KEY, AdapterCapabilities, AdapterDescriptorV1, AdapterError,
    AdapterFuture, AdjacencyCursor, AdjacencyEntry, AdjacencyExpandPage, AdjacencyExpandRequest,
    ApplyReceipt, BackendFamily, CandidateScanPage, CandidateScanRequest, CanonicalRestoreSession,
    CanonicalScanPage, CanonicalScanRequest, ChangeScanPage, ChangeScanRequest,
    CommittedMutationBatch, Durability, KeySpan, KeyValue, Keyspace, LogicalKey,
    LogicalSnapshotAccumulator, LogicalSnapshotChunkV1, LogicalSnapshotError,
    LogicalSnapshotExportRequest, LogicalSnapshotHeaderV1, LogicalSnapshotManifestV1,
    LogicalSnapshotReader, MappingCapabilities, MappingDescriptorV1, MappingFuture,
    MutationOperation, PreparedMappingTransaction, PropertyGatherPage, PropertyGatherRequest,
    PropertyRow, PushdownGuarantee, QueryPageBounds, QueryPrimitiveCapabilities, ReadSnapshot,
    SnapshotCapability, StorageAdapter, TemporalBackendMapping, adapter_log_fingerprint_key,
    adapter_mutation_fingerprint_key, new_logical_snapshot_id,
};
use temporal_types::CanonicalElement;

type RocksDb = DBWithThreadMode<MultiThreaded>;

pub struct RocksAdapterFactory;

impl AdapterFactory for RocksAdapterFactory {
    fn provider_name(&self) -> &str {
        "rocksdb"
    }

    fn mapping_descriptor(&self) -> Option<MappingDescriptorV1> {
        Some(rocks_mapping_descriptor())
    }

    fn open<'a>(&'a self, request: &'a AdapterOpenRequest) -> AdapterFactoryFuture<'a> {
        Box::pin(async move {
            let path = request.parameter("path").ok_or_else(|| {
                AdapterFactoryError::new("RocksDB Adapter requires the public parameter path")
            })?;
            let adapter = RocksAdapter::open(path)
                .map_err(|error| AdapterFactoryError::new(error.to_string()))?;
            adapter
                .validate_mapping()
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
                last_chunk: None,
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
            predicate_pushdown: true,
            adjacency_pushdown: true,
            change_feed: true,
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
            .read_u64(Keyspace::Meta, ADAPTER_META_APPLIED_LOG_INDEX_KEY)?
            .unwrap_or(0))
    }

    fn validate_batch_for_prepare(
        &self,
        batch: &CommittedMutationBatch,
    ) -> Result<Option<ApplyReceipt>, AdapterError> {
        let applied_log_index = self.current_applied_log_index()?;
        let batch_fingerprint = batch.fingerprint();
        if batch.log_index <= applied_log_index {
            let stored =
                self.read_u64(Keyspace::Txn, &adapter_log_fingerprint_key(batch.log_index))?;
            return match stored {
                Some(fingerprint) if fingerprint == batch_fingerprint => Ok(Some(ApplyReceipt {
                    applied_log_index,
                    duplicate: true,
                })),
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
        for mutation in &batch.mutations {
            if !sequences.insert(mutation.sequence) {
                return Err(AdapterError::DuplicateMutationSequence {
                    txn_id: batch.txn_id,
                    sequence: mutation.sequence,
                });
            }
            let fingerprint = mutation.fingerprint();
            let metadata_key = adapter_mutation_fingerprint_key(batch.txn_id, mutation.sequence);
            if let Some(previous) = self.read_u64(Keyspace::Txn, &metadata_key)?
                && previous != fingerprint
            {
                return Err(AdapterError::MutationReplayMismatch {
                    txn_id: batch.txn_id,
                    sequence: mutation.sequence,
                });
            }
        }
        Ok(None)
    }

    fn apply(&self, batch: CommittedMutationBatch) -> Result<ApplyReceipt, AdapterError> {
        let _apply_guard = self
            .apply_guard
            .lock()
            .map_err(|_| AdapterError::LockPoisoned)?;
        if let Some(receipt) = self.validate_batch_for_prepare(&batch)? {
            return Ok(receipt);
        }
        let batch_fingerprint = batch.fingerprint();

        let mut mutation_fingerprints = Vec::with_capacity(batch.mutations.len());
        for mutation in &batch.mutations {
            let fingerprint = mutation.fingerprint();
            let metadata_key = adapter_mutation_fingerprint_key(batch.txn_id, mutation.sequence);
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
            adapter_log_fingerprint_key(batch.log_index),
            batch_fingerprint.to_be_bytes(),
        );

        let meta_cf = self.cf(Keyspace::Meta)?;
        write_batch.put_cf(
            &meta_cf,
            ADAPTER_META_APPLIED_LOG_INDEX_KEY,
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
                && entry.key().as_bytes() == ADAPTER_META_APPLIED_LOG_INDEX_KEY
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
            ADAPTER_META_APPLIED_LOG_INDEX_KEY,
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

    fn is_mapping_empty(&self) -> Result<bool, AdapterError> {
        for keyspace in Keyspace::ALL {
            let cf = self.cf(keyspace)?;
            let mut iterator = self.db.iterator_cf(&cf, IteratorMode::Start);
            if iterator
                .next()
                .transpose()
                .map_err(backend_error)?
                .is_some()
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn clear_mapping(&self) -> Result<(), AdapterError> {
        let _guard = self
            .apply_guard
            .lock()
            .map_err(|_| AdapterError::LockPoisoned)?;
        let mut write_batch = WriteBatch::default();
        let mut count = 0_usize;
        for keyspace in Keyspace::ALL {
            let cf = self.cf(keyspace)?;
            for entry in self.db.iterator_cf(&cf, IteratorMode::Start) {
                let (key, _) = entry.map_err(backend_error)?;
                write_batch.delete_cf(&cf, key);
                count = count.saturating_add(1);
            }
        }
        if count != 0 {
            self.write_sync(write_batch)?;
        }
        Ok(())
    }
}

struct RocksPreparedMapping<'a> {
    adapter: &'a RocksAdapter,
    batch: CommittedMutationBatch,
    applied: bool,
}

impl PreparedMappingTransaction for RocksPreparedMapping<'_> {
    fn apply<'a>(&'a mut self) -> MappingFuture<'a, ()> {
        Box::pin(async move {
            if self.applied {
                return Err(AdapterError::Backend(
                    "RocksDB Mapping transaction was applied twice".into(),
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
                    "RocksDB Mapping transaction must be applied before commit".into(),
                ));
            }
            self.adapter.apply(self.batch.clone())
        })
    }

    fn abort<'a>(self: Box<Self>) -> MappingFuture<'a, ()>
    where
        Self: 'a,
    {
        Box::pin(async { Ok(()) })
    }
}

impl TemporalBackendMapping for RocksAdapter {
    fn describe_schema(&self) -> MappingDescriptorV1 {
        rocks_mapping_descriptor()
    }

    fn validate_mapping(&self) -> Result<(), AdapterError> {
        let mut expected = vec!["default".to_owned()];
        expected.extend(
            Keyspace::ALL
                .into_iter()
                .map(|keyspace| keyspace.column_family().to_owned()),
        );
        expected.sort();
        let actual = self.column_family_names()?;
        if actual != expected {
            return Err(AdapterError::Backend(format!(
                "RocksDB Mapping column families {actual:?} differ from expected {expected:?}"
            )));
        }
        Ok(())
    }

    fn prepare<'a>(
        &'a self,
        batch: CommittedMutationBatch,
    ) -> MappingFuture<'a, Box<dyn PreparedMappingTransaction + 'a>> {
        Box::pin(async move {
            let _guard = self
                .apply_guard
                .lock()
                .map_err(|_| AdapterError::LockPoisoned)?;
            self.validate_batch_for_prepare(&batch)?;
            Ok(Box::new(RocksPreparedMapping {
                adapter: self,
                batch,
                applied: false,
            }) as Box<dyn PreparedMappingTransaction + 'a>)
        })
    }

    fn multi_get<'a>(&'a self, keys: &'a [LogicalKey]) -> MappingFuture<'a, Vec<Option<Vec<u8>>>> {
        <Self as StorageAdapter>::multi_get(self, keys)
    }

    fn scan<'a>(&'a self, span: &'a KeySpan) -> MappingFuture<'a, Vec<KeyValue>> {
        <Self as StorageAdapter>::scan(self, span)
    }

    fn begin_read_snapshot<'a>(&'a self) -> MappingFuture<'a, Box<dyn ReadSnapshot + 'a>> {
        <Self as StorageAdapter>::begin_read_snapshot(self)
    }

    fn export_canonical<'a>(
        &'a self,
        request: LogicalSnapshotExportRequest,
    ) -> MappingFuture<'a, Box<dyn LogicalSnapshotReader + 'a>> {
        <Self as StorageAdapter>::begin_logical_export(self, request)
    }

    fn restore_canonical<'a>(
        &'a self,
        header: LogicalSnapshotHeaderV1,
    ) -> MappingFuture<'a, Box<dyn CanonicalRestoreSession + 'a>> {
        Box::pin(async move {
            if !self.is_mapping_empty()? {
                return Err(AdapterError::Backend(
                    "RocksDB canonical restore target is not empty".into(),
                ));
            }
            Ok(Box::new(RocksCanonicalRestoreSession {
                adapter: self,
                accumulator: LogicalSnapshotAccumulator::new(header.clone()),
                header,
                saw_applied_index_record: false,
                last_chunk: None,
                committed: false,
            }) as Box<dyn CanonicalRestoreSession + 'a>)
        })
    }

    fn create_physical_checkpoint(&self, destination: &Path) -> Result<(), AdapterError> {
        self.checkpoint(destination)
    }

    fn applied_log_index(&self) -> Result<u64, AdapterError> {
        self.current_applied_log_index()
    }
}

struct RocksCanonicalRestoreSession<'a> {
    adapter: &'a RocksAdapter,
    header: LogicalSnapshotHeaderV1,
    accumulator: LogicalSnapshotAccumulator,
    saw_applied_index_record: bool,
    last_chunk: Option<(u64, [u8; 32])>,
    committed: bool,
}

impl CanonicalRestoreSession for RocksCanonicalRestoreSession<'_> {
    fn write_chunk<'a>(&'a mut self, chunk: LogicalSnapshotChunkV1) -> MappingFuture<'a, ()> {
        Box::pin(async move {
            if self.committed {
                return Err(AdapterError::Backend(
                    "RocksDB canonical restore is already committed".into(),
                ));
            }
            if self.last_chunk == Some((chunk.ordinal(), chunk.digest())) {
                return Ok(());
            }
            let mut next = self.accumulator.clone();
            next.observe(&chunk)?;
            self.saw_applied_index_record |= self
                .adapter
                .restore_logical_entries(chunk.entries(), self.header.applied_log_index())?;
            self.last_chunk = Some((chunk.ordinal(), chunk.digest()));
            self.accumulator = next;
            Ok(())
        })
    }

    fn commit<'a>(&'a mut self, manifest: LogicalSnapshotManifestV1) -> MappingFuture<'a, ()> {
        Box::pin(async move {
            if self.committed {
                return Err(AdapterError::Backend(
                    "RocksDB canonical restore is already committed".into(),
                ));
            }
            self.accumulator.clone().verify(&manifest)?;
            if self.header.applied_log_index() != 0 && !self.saw_applied_index_record {
                return Err(AdapterError::Backend(
                    "canonical snapshot is missing its applied-index record".into(),
                ));
            }
            self.adapter.publish_restored_applied_index(
                self.header.applied_log_index(),
                self.saw_applied_index_record,
            )?;
            self.committed = true;
            Ok(())
        })
    }

    fn abort<'a>(mut self: Box<Self>) -> MappingFuture<'a, ()>
    where
        Self: 'a,
    {
        Box::pin(async move {
            self.adapter.clear_mapping()?;
            self.committed = true;
            Ok(())
        })
    }
}

impl Drop for RocksCanonicalRestoreSession<'_> {
    fn drop(&mut self) {
        if !self.committed {
            let _ = self.adapter.clear_mapping();
        }
    }
}

fn rocks_mapping_descriptor() -> MappingDescriptorV1 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"DTGProxy/RocksDBCanonicalMapping/1");
    for keyspace in Keyspace::ALL {
        hasher.update(&[keyspace.tag()]);
        hasher.update(keyspace.column_family().as_bytes());
    }
    hasher.update(ADAPTER_META_APPLIED_LOG_INDEX_KEY);
    MappingDescriptorV1::new(
        "rocksdb-canonical",
        "1.0.0",
        BackendFamily::KeyValue,
        *hasher.finalize().as_bytes(),
        MappingCapabilities {
            atomic_batch_lifecycle: true,
            deterministic_mapping: true,
            idempotent_replay: true,
            canonical_multi_get: true,
            canonical_ordered_scan: true,
            durable_applied_index: true,
            durability: Durability::Synchronous,
            snapshot: SnapshotCapability::PhysicalCheckpoint,
            canonical_export: true,
            canonical_restore: true,
            native_temporal_layout: true,
            predicate_pushdown: true,
            adjacency_pushdown: true,
            change_feed: true,
        },
    )
    .expect("static RocksDB Mapping descriptor is valid")
}

struct RocksRestoreSession {
    target_path: PathBuf,
    staging_path: PathBuf,
    adapter: Option<RocksAdapter>,
    header: LogicalSnapshotHeaderV1,
    accumulator: LogicalSnapshotAccumulator,
    saw_applied_index_record: bool,
    last_chunk: Option<(u64, [u8; 32])>,
}

impl AdapterRestoreSession for RocksRestoreSession {
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
                AdapterFactoryError::new("RocksDB restore session is already finished")
            })?;
            let saw_applied_index = adapter
                .restore_logical_entries(chunk.entries(), self.header.applied_log_index())
                .map_err(|error| AdapterFactoryError::new(error.to_string()))?;
            self.saw_applied_index_record |= saw_applied_index;
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
            let actual_index = StorageAdapter::applied_log_index(&restored)
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

    fn abort<'a>(mut self: Box<Self>) -> AdapterRestoreFuture<'a, ()>
    where
        Self: 'a,
    {
        Box::pin(async move {
            drop(self.adapter.take());
            if self.staging_path.exists() {
                fs::remove_dir_all(&self.staging_path)
                    .map_err(|error| AdapterFactoryError::new(error.to_string()))?;
            }
            Ok(())
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
            StorageAdapter::capabilities(self),
        )
    }

    fn capabilities(&self) -> AdapterCapabilities {
        RocksAdapter::capabilities(self)
    }

    fn query_primitive_capabilities(&self) -> QueryPrimitiveCapabilities {
        rocks_query_primitive_capabilities()
    }

    fn mapping_descriptor(&self) -> Option<MappingDescriptorV1> {
        Some(rocks_mapping_descriptor())
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
            let mut retained = 0_u64;
            for item in iterator {
                let (key, value) = item.map_err(backend_error)?;
                if !span.contains(&key) {
                    break;
                }
                retained = storage_api::charge_scan_entry(span, retained, &key, &value)?;
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

    fn scan_candidates<'a>(
        &'a self,
        request: &'a CandidateScanRequest,
    ) -> AdapterFuture<'a, CandidateScanPage> {
        Box::pin(async move {
            let read = self.open_read_snapshot()?;
            read.scan_candidates(request).await
        })
    }

    fn gather_properties<'a>(
        &'a self,
        request: &'a PropertyGatherRequest,
    ) -> AdapterFuture<'a, PropertyGatherPage> {
        Box::pin(async move {
            let read = self.open_read_snapshot()?;
            read.gather_properties(request)
        })
    }

    fn expand_adjacency<'a>(
        &'a self,
        request: &'a AdjacencyExpandRequest,
    ) -> AdapterFuture<'a, AdjacencyExpandPage> {
        Box::pin(async move {
            let read = self.open_read_snapshot()?;
            read.expand_adjacency(request)
        })
    }

    fn scan_changes<'a>(
        &'a self,
        request: &'a ChangeScanRequest,
    ) -> AdapterFuture<'a, ChangeScanPage> {
        Box::pin(async move {
            let read = self.open_read_snapshot()?;
            read.scan_changes(request).await
        })
    }

    fn begin_read_snapshot<'a>(&'a self) -> AdapterFuture<'a, Box<dyn ReadSnapshot + 'a>> {
        Box::pin(
            async move { Ok(Box::new(self.open_read_snapshot()?) as Box<dyn ReadSnapshot + 'a>) },
        )
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
            let header = LogicalSnapshotHeaderV1::new(new_logical_snapshot_id(), applied_log_index);
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

    fn create_physical_checkpoint(&self, destination: &Path) -> Result<(), AdapterError> {
        self.checkpoint(destination)
    }

    fn applied_log_index(&self) -> Result<u64, AdapterError> {
        self.current_applied_log_index()
    }
}

struct RocksReadSnapshot<'a> {
    adapter: &'a RocksAdapter,
    snapshot: SnapshotWithThreadMode<'a, RocksDb>,
    applied_log_index: u64,
}

impl ReadSnapshot for RocksReadSnapshot<'_> {
    fn applied_log_index(&self) -> u64 {
        self.applied_log_index
    }

    fn multi_get<'a>(&'a self, keys: &'a [LogicalKey]) -> AdapterFuture<'a, Vec<Option<Vec<u8>>>> {
        Box::pin(async move {
            let handles: Vec<_> = keys
                .iter()
                .map(|key| self.adapter.cf(key.keyspace()))
                .collect::<Result<_, _>>()?;
            self.snapshot
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
            let cf = self.adapter.cf(span.keyspace())?;
            let iterator = self
                .snapshot
                .iterator_cf(&cf, IteratorMode::From(span.start(), Direction::Forward));
            let mut values = Vec::new();
            let mut retained = 0_u64;
            for item in iterator {
                let (key, value) = item.map_err(backend_error)?;
                if !span.contains(&key) {
                    break;
                }
                retained = storage_api::charge_scan_entry(span, retained, &key, &value)?;
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

    fn scan_canonical<'a>(
        &'a self,
        request: &'a CanonicalScanRequest,
    ) -> AdapterFuture<'a, CanonicalScanPage> {
        Box::pin(async move {
            let cf = self.adapter.cf(request.span().keyspace())?;
            let iterator = self.snapshot.iterator_cf(
                &cf,
                IteratorMode::From(request.span().start(), Direction::Forward),
            );
            let (entries, next_start) =
                bounded_rocks_page(iterator, request.span(), request.bounds())?;
            CanonicalScanPage::new(request, self.applied_log_index, entries, next_start)
                .map_err(|error| AdapterError::Backend(error.to_string()))
        })
    }

    fn scan_candidates<'a>(
        &'a self,
        request: &'a CandidateScanRequest,
    ) -> AdapterFuture<'a, CandidateScanPage> {
        Box::pin(async move {
            let (entries, next_start) = self.scan_page(request.span(), request.bounds())?;
            CandidateScanPage::new(
                request,
                self.applied_log_index,
                PushdownGuarantee::Candidate,
                entries,
                next_start,
            )
            .map_err(query_error)
        })
    }

    fn scan_changes<'a>(
        &'a self,
        request: &'a ChangeScanRequest,
    ) -> AdapterFuture<'a, ChangeScanPage> {
        Box::pin(async move {
            let (entries, next_start) = self.scan_page(request.span(), request.bounds())?;
            ChangeScanPage::new(
                request,
                self.applied_log_index,
                PushdownGuarantee::Exact,
                entries,
                next_start,
            )
            .map_err(query_error)
        })
    }
}

impl RocksAdapter {
    fn open_read_snapshot(&self) -> Result<RocksReadSnapshot<'_>, AdapterError> {
        let apply_guard = self
            .apply_guard
            .lock()
            .map_err(|_| AdapterError::LockPoisoned)?;
        let applied_log_index = self.current_applied_log_index()?;
        let snapshot = self.db.snapshot();
        drop(apply_guard);
        Ok(RocksReadSnapshot {
            adapter: self,
            snapshot,
            applied_log_index,
        })
    }
}

impl RocksReadSnapshot<'_> {
    fn scan_page(
        &self,
        span: &KeySpan,
        bounds: QueryPageBounds,
    ) -> Result<(Vec<KeyValue>, Option<LogicalKey>), AdapterError> {
        let cf = self.adapter.cf(span.keyspace())?;
        let iterator = self
            .snapshot
            .iterator_cf(&cf, IteratorMode::From(span.start(), Direction::Forward));
        bounded_rocks_page(iterator, span, bounds)
    }

    fn gather_properties(
        &self,
        request: &PropertyGatherRequest,
    ) -> Result<PropertyGatherPage, AdapterError> {
        let handles = request
            .keys()
            .iter()
            .map(|key| self.adapter.cf(key.keyspace()))
            .collect::<Result<Vec<_>, _>>()?;
        let values = self.snapshot.multi_get_cf(
            handles
                .iter()
                .zip(request.keys())
                .map(|(cf, key)| (cf, key.as_bytes())),
        );
        let rows = request
            .keys()
            .iter()
            .zip(values)
            .map(|(key, value)| {
                let value = value.map_err(backend_error)?;
                let properties = value
                    .as_deref()
                    .and_then(stable_projection_payload)
                    .map_or_else(
                        || vec![None; request.properties().len()],
                        |payload| {
                            request
                                .properties()
                                .iter()
                                .map(|property| payload.property(property.value()).cloned())
                                .collect()
                        },
                    );
                Ok(PropertyRow::new(key.clone(), properties))
            })
            .collect::<Result<Vec<_>, AdapterError>>()?;
        PropertyGatherPage::new(
            request,
            self.applied_log_index,
            PushdownGuarantee::Candidate,
            rows,
        )
        .map_err(query_error)
    }

    fn expand_adjacency(
        &self,
        request: &AdjacencyExpandRequest,
    ) -> Result<AdjacencyExpandPage, AdapterError> {
        let mut entries = Vec::new();
        let mut retained = 0_u64;
        let mut next = None;
        'spans: for (input_ordinal, span) in request.spans().iter().enumerate() {
            let cf = self.adapter.cf(span.keyspace())?;
            let iterator = self
                .snapshot
                .iterator_cf(&cf, IteratorMode::From(span.start(), Direction::Forward));
            for item in iterator {
                let (key, value) = item.map_err(backend_error)?;
                if !span.contains(&key) {
                    break;
                }
                let entry_bytes = query_entry_bytes(&key, &value);
                if entries.len() == request.bounds().max_items()
                    || retained.saturating_add(entry_bytes) > request.bounds().max_bytes()
                {
                    if entries.is_empty() {
                        return Err(AdapterError::ScanByteLimit {
                            limit: request.bounds().max_bytes(),
                            required: entry_bytes,
                        });
                    }
                    next = Some(AdjacencyCursor::new(
                        input_ordinal,
                        LogicalKey::in_keyspace(span.keyspace(), key.into_vec()),
                    ));
                    break 'spans;
                }
                retained = retained.saturating_add(entry_bytes);
                entries.push(AdjacencyEntry::new(
                    input_ordinal,
                    KeyValue::new(
                        LogicalKey::in_keyspace(span.keyspace(), key.into_vec()),
                        value.into_vec(),
                    ),
                ));
            }
        }
        AdjacencyExpandPage::new(
            request,
            self.applied_log_index,
            PushdownGuarantee::Exact,
            entries,
            next,
        )
        .map_err(query_error)
    }
}

fn bounded_rocks_page(
    iterator: rocksdb::DBIteratorWithThreadMode<'_, RocksDb>,
    span: &KeySpan,
    bounds: QueryPageBounds,
) -> Result<(Vec<KeyValue>, Option<LogicalKey>), AdapterError> {
    let mut entries = Vec::new();
    let mut retained = 0_u64;
    let mut next_start = None;
    for item in iterator {
        let (key, value) = item.map_err(backend_error)?;
        if !span.contains(&key) {
            break;
        }
        if entries.len() == bounds.max_items() {
            next_start = Some(LogicalKey::in_keyspace(span.keyspace(), key.into_vec()));
            break;
        }
        let entry_bytes = query_entry_bytes(&key, &value);
        let required = retained.saturating_add(entry_bytes);
        if required > bounds.max_bytes() {
            if entries.is_empty() {
                return Err(AdapterError::ScanByteLimit {
                    limit: bounds.max_bytes(),
                    required,
                });
            }
            next_start = Some(LogicalKey::in_keyspace(span.keyspace(), key.into_vec()));
            break;
        }
        retained = required;
        entries.push(KeyValue::new(
            LogicalKey::in_keyspace(span.keyspace(), key.into_vec()),
            value.into_vec(),
        ));
    }
    Ok((entries, next_start))
}

const fn rocks_query_primitive_capabilities() -> QueryPrimitiveCapabilities {
    QueryPrimitiveCapabilities::new(
        PushdownGuarantee::Candidate,
        PushdownGuarantee::Candidate,
        PushdownGuarantee::Exact,
        PushdownGuarantee::Exact,
    )
}

fn query_entry_bytes(key: &[u8], value: &[u8]) -> u64 {
    u64::try_from(key.len())
        .ok()
        .and_then(|key_bytes| {
            u64::try_from(value.len())
                .ok()
                .and_then(|value_bytes| key_bytes.checked_add(value_bytes))
        })
        .unwrap_or(u64::MAX)
}

fn stable_projection_payload(bytes: &[u8]) -> Option<CanonicalElement> {
    let mut decoder = ProjectionDecoder::new(bytes);
    decoder.expect(b"DTGP")?;
    if decoder.read_u16()? != 1 {
        return None;
    }
    decoder.skip(12)?;
    let segment_count = decoder.read_u32()? as usize;
    let mut stable = None;
    for _ in 0..segment_count {
        decoder.skip(8)?;
        match decoder.read_u8()? {
            0 => {}
            1 => decoder.skip(8)?,
            _ => return None,
        }
        let payload_length = decoder.read_u32()? as usize;
        let payload = CanonicalElement::decode(decoder.take(payload_length)?).ok()?;
        if stable.as_ref().is_some_and(|stable| stable != &payload) {
            return None;
        }
        stable = Some(payload);
    }
    let checksum_position = decoder.position;
    let stored_checksum = decoder.read_u64()?;
    if decoder.position != bytes.len() || stored_checksum != fnv1a(&bytes[..checksum_position]) {
        return None;
    }
    stable
}

struct ProjectionDecoder<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> ProjectionDecoder<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn take(&mut self, length: usize) -> Option<&'a [u8]> {
        let end = self.position.checked_add(length)?;
        let bytes = self.bytes.get(self.position..end)?;
        self.position = end;
        Some(bytes)
    }

    fn expect(&mut self, expected: &[u8]) -> Option<()> {
        (self.take(expected.len())? == expected).then_some(())
    }

    fn skip(&mut self, length: usize) -> Option<()> {
        self.take(length).map(|_| ())
    }

    fn read_u8(&mut self) -> Option<u8> {
        Some(self.take(1)?[0])
    }

    fn read_u16(&mut self) -> Option<u16> {
        Some(u16::from_be_bytes(self.take(2)?.try_into().ok()?))
    }

    fn read_u32(&mut self) -> Option<u32> {
        Some(u32::from_be_bytes(self.take(4)?.try_into().ok()?))
    }

    fn read_u64(&mut self) -> Option<u64> {
        Some(u64::from_be_bytes(self.take(8)?.try_into().ok()?))
    }
}

fn fnv1a(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325_u64, |value, byte| {
        (value ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
    })
}

fn query_error(error: storage_api::QueryPrimitiveError) -> AdapterError {
    AdapterError::Backend(error.to_string())
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

fn backend_error(error: rocksdb::Error) -> AdapterError {
    AdapterError::Backend(error.to_string())
}
