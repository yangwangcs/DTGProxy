#![forbid(unsafe_code)]

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use rocksdb::{
    BoundColumnFamily, ColumnFamilyDescriptor, DBWithThreadMode, Direction, IteratorMode,
    MultiThreaded, Options, WriteBatch, WriteOptions,
};
use storage_api::{
    AdapterCapabilities, AdapterError, AdapterFuture, ApplyReceipt, CommittedMutationBatch,
    KeySpan, KeyValue, Keyspace, LogicalKey, MutationOperation, StorageAdapter,
};

const APPLIED_LOG_INDEX_KEY: &[u8] = b"\x00applied_log_index";
const LOG_FINGERPRINT_PREFIX: u8 = 0x01;
const MUTATION_FINGERPRINT_PREFIX: u8 = 0x02;

type RocksDb = DBWithThreadMode<MultiThreaded>;

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
}

impl StorageAdapter for RocksAdapter {
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

    fn applied_log_index(&self) -> Result<u64, AdapterError> {
        self.current_applied_log_index()
    }
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
