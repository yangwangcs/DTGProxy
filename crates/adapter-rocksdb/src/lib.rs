#![forbid(unsafe_code)]

use std::path::{Path, PathBuf};

use rocksdb::{ColumnFamilyDescriptor, DBWithThreadMode, MultiThreaded, Options};
use storage_api::{AdapterCapabilities, AdapterError, Keyspace};

type RocksDb = DBWithThreadMode<MultiThreaded>;

pub struct RocksAdapter {
    path: PathBuf,
    _db: RocksDb,
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

        Ok(Self { path, _db: db })
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
}

fn backend_error(error: rocksdb::Error) -> AdapterError {
    AdapterError::Backend(error.to_string())
}
