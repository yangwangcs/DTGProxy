use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use dtg_language_ir::RowSchema;
use dtg_storage::{
    EdgeId, EdgeVersion, TransactionTime, ValidInterval, Value, Version, VertexId, VertexVersion,
};

use crate::{QueryError, QueryValue, SpillConfig, SpillHandle, SpillStore};

const MAGIC: &[u8; 8] = b"DTGSPIL1";
const FORMAT_VERSION: u32 = 1;
const HEADER_BYTES: u64 = 8 + 4 + 16 + 8 + 8;
const RECORD_HEADER_BYTES: u64 = 8 + 8;
const CHECKSUM_BYTES: u64 = 32;
const DEFAULT_MAX_ROWS: usize = 1_000_000;
const DEFAULT_MAX_ROW_BYTES: u64 = 16 * 1024 * 1024;
const DEFAULT_MAX_RUN_BYTES: u64 = 1024 * 1024 * 1024;

static NAMESPACE_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FileSpillLimits {
    max_rows: usize,
    max_row_bytes: u64,
    max_run_bytes: u64,
}

impl FileSpillLimits {
    pub fn new(
        max_rows: usize,
        max_row_bytes: u64,
        max_run_bytes: u64,
    ) -> Result<Self, QueryError> {
        if max_rows == 0 || max_row_bytes == 0 || max_run_bytes == 0 {
            return Err(QueryError::InvalidPlan(
                "file spill limits must be nonzero".into(),
            ));
        }
        Ok(Self {
            max_rows,
            max_row_bytes,
            max_run_bytes,
        })
    }
}

impl Default for FileSpillLimits {
    fn default() -> Self {
        Self {
            max_rows: DEFAULT_MAX_ROWS,
            max_row_bytes: DEFAULT_MAX_ROW_BYTES,
            max_run_bytes: DEFAULT_MAX_RUN_BYTES,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BuiltInSpillPolicy {
    root: PathBuf,
    threshold_bytes: u64,
    limits: FileSpillLimits,
    orphan_age: Duration,
}

impl BuiltInSpillPolicy {
    pub fn new(
        root: impl AsRef<Path>,
        threshold_bytes: u64,
        limits: FileSpillLimits,
        orphan_age: Duration,
    ) -> Result<Self, QueryError> {
        if threshold_bytes == 0 {
            return Err(QueryError::InvalidPlan(
                "built-in spill threshold must be nonzero".into(),
            ));
        }
        Ok(Self {
            root: root.as_ref().to_owned(),
            threshold_bytes,
            limits,
            orphan_age,
        })
    }

    pub(crate) fn create_config(&self) -> Result<SpillConfig, QueryError> {
        FileSpillStore::reclaim_orphans(&self.root, self.orphan_age)?;
        SpillConfig::new(
            Arc::new(FileSpillStore::create(&self.root, self.limits)?),
            self.threshold_bytes,
        )
    }
}

impl Default for BuiltInSpillPolicy {
    fn default() -> Self {
        Self {
            root: std::env::temp_dir().join("dtg-query-spill-v1"),
            threshold_bytes: 8 * 1024 * 1024,
            limits: FileSpillLimits::default(),
            orphan_age: Duration::from_secs(24 * 60 * 60),
        }
    }
}

#[derive(Clone, Debug)]
struct RowMeta {
    payload_offset: u64,
    payload_bytes: u64,
    estimated_bytes: u64,
}

#[derive(Clone, Debug)]
struct RunMeta {
    path: PathBuf,
    rows: Vec<RowMeta>,
}

pub struct FileSpillStore {
    namespace_id: u128,
    handle_prefix: u32,
    namespace_path: PathBuf,
    active_path: PathBuf,
    limits: FileSpillLimits,
    next_run: AtomicU64,
    runs: Mutex<BTreeMap<SpillHandle, RunMeta>>,
}

impl FileSpillStore {
    pub fn create(root: impl AsRef<Path>, limits: FileSpillLimits) -> Result<Self, QueryError> {
        let root = root.as_ref();
        fs::create_dir_all(root).map_err(io_error)?;
        Self::reclaim_orphans(root, Duration::from_secs(24 * 60 * 60))?;
        let (namespace_id, namespace_path) = create_namespace(root)?;
        let active_path = namespace_path.join("active");
        write_active_marker(&active_path)?;
        let digest = blake3::hash(&namespace_id.to_be_bytes());
        let handle_prefix = u32::from_be_bytes(digest.as_bytes()[..4].try_into().expect("slice"));
        Ok(Self {
            namespace_id,
            handle_prefix,
            namespace_path,
            active_path,
            limits,
            next_run: AtomicU64::new(1),
            runs: Mutex::new(BTreeMap::new()),
        })
    }

    pub fn reclaim_orphans(
        root: impl AsRef<Path>,
        minimum_age: Duration,
    ) -> Result<usize, QueryError> {
        let root = root.as_ref();
        let entries = match fs::read_dir(root) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
            Err(error) => return Err(io_error(error)),
        };
        let now = SystemTime::now();
        let mut reclaimed = 0;
        for entry in entries {
            let entry = entry.map_err(io_error)?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if !valid_namespace_name(&name) {
                continue;
            }
            let metadata = fs::symlink_metadata(entry.path()).map_err(io_error)?;
            if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
                continue;
            }
            let active = entry.path().join("active");
            let modified = fs::metadata(&active)
                .and_then(|metadata| metadata.modified())
                .or_else(|_| metadata.modified())
                .map_err(io_error)?;
            let age = now.duration_since(modified).unwrap_or(Duration::ZERO);
            if age >= minimum_age {
                fs::remove_dir_all(entry.path()).map_err(io_error)?;
                reclaimed += 1;
            }
        }
        Ok(reclaimed)
    }

    pub fn namespace_path(&self) -> &Path {
        &self.namespace_path
    }

    fn next_handle(&self) -> Result<SpillHandle, QueryError> {
        let run = self.next_run.fetch_add(1, Ordering::Relaxed);
        let run = u32::try_from(run).map_err(|_| {
            QueryError::Storage("file spill run identity space is exhausted".into())
        })?;
        Ok(SpillHandle::new(
            (u64::from(self.handle_prefix) << 32) | u64::from(run),
        ))
    }

    fn validate_handle(&self, handle: SpillHandle) -> Result<(), QueryError> {
        if (handle.get() >> 32) as u32 != self.handle_prefix {
            return Err(QueryError::Storage(
                "spill handle belongs to a different query namespace".into(),
            ));
        }
        Ok(())
    }

    fn touch(&self) -> Result<(), QueryError> {
        write_active_marker(&self.active_path)
    }
}

impl SpillStore for FileSpillStore {
    fn reserved_write_bytes(&self, batch: &crate::ColumnBatch) -> Result<u64, QueryError> {
        if batch.row_count() > self.limits.max_rows {
            return Err(QueryError::Storage(
                "spill run exceeds the configured row bound".into(),
            ));
        }
        let mut total = HEADER_BYTES;
        for row_index in 0..batch.row_count() {
            let mut payload = 8_u64;
            for column_index in 0..batch.schema().fields.len() {
                let value = batch
                    .column(column_index)
                    .and_then(|column| column.get(row_index))
                    .ok_or_else(|| QueryError::InvalidBatch("spill batch shape changed".into()))?;
                payload = payload.saturating_add(encoded_value_size(value, 0)?);
            }
            if payload > self.limits.max_row_bytes {
                return Err(QueryError::Storage(
                    "spill row exceeds the configured byte bound".into(),
                ));
            }
            total = total
                .saturating_add(RECORD_HEADER_BYTES)
                .saturating_add(payload)
                .saturating_add(CHECKSUM_BYTES);
            if total > self.limits.max_run_bytes {
                return Err(QueryError::Storage(
                    "spill run exceeds the configured byte bound".into(),
                ));
            }
        }
        Ok(total)
    }

    fn write_run(
        &self,
        schema: &RowSchema,
        rows: Vec<Vec<QueryValue>>,
    ) -> Result<SpillHandle, QueryError> {
        self.touch()?;
        if rows.is_empty() || rows.len() > self.limits.max_rows {
            return Err(QueryError::Storage(
                "spill run must contain a bounded nonempty row set".into(),
            ));
        }
        let handle = self.next_handle()?;
        let final_path = self
            .namespace_path
            .join(format!("{:016x}.run", handle.get()));
        let temporary_path = self
            .namespace_path
            .join(format!("{:016x}.tmp", handle.get()));
        let result = self.write_run_file(&temporary_path, handle, schema, &rows);
        let row_meta = match result {
            Ok(row_meta) => row_meta,
            Err(error) => {
                let _ = fs::remove_file(&temporary_path);
                return Err(error);
            }
        };
        fs::rename(&temporary_path, &final_path).map_err(io_error)?;
        sync_directory(&self.namespace_path)?;
        self.runs.lock().map_err(poisoned)?.insert(
            handle,
            RunMeta {
                path: final_path,
                rows: row_meta,
            },
        );
        Ok(handle)
    }

    fn row_count(&self, handle: SpillHandle) -> Result<usize, QueryError> {
        self.validate_handle(handle)?;
        self.touch()?;
        self.runs
            .lock()
            .map_err(poisoned)?
            .get(&handle)
            .map(|run| run.rows.len())
            .ok_or_else(|| QueryError::Storage("spill run is absent".into()))
    }

    fn row_estimated_bytes(&self, handle: SpillHandle, index: usize) -> Result<u64, QueryError> {
        self.validate_handle(handle)?;
        self.touch()?;
        self.runs
            .lock()
            .map_err(poisoned)?
            .get(&handle)
            .and_then(|run| run.rows.get(index))
            .map(|row| row.estimated_bytes.max(row.payload_bytes))
            .ok_or_else(|| QueryError::Storage("spill row is absent".into()))
    }

    fn read_row(&self, handle: SpillHandle, index: usize) -> Result<Vec<QueryValue>, QueryError> {
        self.validate_handle(handle)?;
        self.touch()?;
        let (path, row) = {
            let runs = self.runs.lock().map_err(poisoned)?;
            let run = runs
                .get(&handle)
                .ok_or_else(|| QueryError::Storage("spill run is absent".into()))?;
            let row = run
                .rows
                .get(index)
                .cloned()
                .ok_or_else(|| QueryError::Storage("spill row is absent".into()))?;
            (run.path.clone(), row)
        };
        if row.payload_bytes > self.limits.max_row_bytes {
            return Err(QueryError::Storage(
                "spill row exceeds the configured byte bound".into(),
            ));
        }
        let payload_size = usize::try_from(row.payload_bytes)
            .map_err(|_| QueryError::Storage("spill row size is unsupported".into()))?;
        let mut file = File::open(path).map_err(io_error)?;
        validate_header(&mut file, self.namespace_id, handle)?;
        file.seek(SeekFrom::Start(row.payload_offset))
            .map_err(io_error)?;
        let mut payload = vec![0; payload_size];
        file.read_exact(&mut payload).map_err(io_error)?;
        let mut checksum = [0; 32];
        file.read_exact(&mut checksum).map_err(io_error)?;
        let expected = row_checksum(
            self.namespace_id,
            handle,
            index,
            row.estimated_bytes,
            &payload,
        );
        if checksum != *expected.as_bytes() {
            return Err(QueryError::Storage(
                "spill row checksum verification failed".into(),
            ));
        }
        decode_row(&payload, self.limits.max_row_bytes)
    }

    fn remove_run(&self, handle: SpillHandle) -> Result<(), QueryError> {
        self.validate_handle(handle)?;
        let run = self.runs.lock().map_err(poisoned)?.remove(&handle);
        if let Some(run) = run {
            match fs::remove_file(run.path) {
                Ok(()) => sync_directory(&self.namespace_path)?,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(io_error(error)),
            }
        }
        self.touch()
    }
}

impl FileSpillStore {
    fn write_run_file(
        &self,
        path: &Path,
        handle: SpillHandle,
        schema: &RowSchema,
        rows: &[Vec<QueryValue>],
    ) -> Result<Vec<RowMeta>, QueryError> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(io_error)?;
        file.write_all(MAGIC).map_err(io_error)?;
        file.write_all(&FORMAT_VERSION.to_be_bytes())
            .map_err(io_error)?;
        file.write_all(&self.namespace_id.to_be_bytes())
            .map_err(io_error)?;
        file.write_all(&handle.get().to_be_bytes())
            .map_err(io_error)?;
        file.write_all(&(rows.len() as u64).to_be_bytes())
            .map_err(io_error)?;
        let mut total = HEADER_BYTES;
        let mut metadata = Vec::with_capacity(rows.len());
        for (index, row) in rows.iter().enumerate() {
            validate_row(schema, row)?;
            let estimated_bytes: u64 = row.iter().map(QueryValue::estimated_bytes).sum();
            let payload = encode_row(row, self.limits.max_row_bytes)?;
            let payload_bytes = payload.len() as u64;
            let record_bytes = RECORD_HEADER_BYTES
                .saturating_add(payload_bytes)
                .saturating_add(CHECKSUM_BYTES);
            total = total.saturating_add(record_bytes);
            if total > self.limits.max_run_bytes {
                return Err(QueryError::Storage(
                    "spill run exceeds the configured byte bound".into(),
                ));
            }
            file.write_all(&payload_bytes.to_be_bytes())
                .map_err(io_error)?;
            file.write_all(&estimated_bytes.to_be_bytes())
                .map_err(io_error)?;
            let payload_offset = file.stream_position().map_err(io_error)?;
            file.write_all(&payload).map_err(io_error)?;
            file.write_all(
                row_checksum(self.namespace_id, handle, index, estimated_bytes, &payload)
                    .as_bytes(),
            )
            .map_err(io_error)?;
            metadata.push(RowMeta {
                payload_offset,
                payload_bytes,
                estimated_bytes,
            });
        }
        file.sync_all().map_err(io_error)?;
        Ok(metadata)
    }
}

impl Drop for FileSpillStore {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.namespace_path);
    }
}

fn create_namespace(root: &Path) -> Result<(u128, PathBuf), QueryError> {
    for _ in 0..32 {
        let counter = NAMESPACE_COUNTER.fetch_add(1, Ordering::Relaxed);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_nanos();
        let namespace_id = nanos ^ (u128::from(std::process::id()) << 64) ^ u128::from(counter);
        let path = root.join(format!("query-{namespace_id:032x}"));
        match fs::create_dir(&path) {
            Ok(()) => return Ok((namespace_id, path)),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(io_error(error)),
        }
    }
    Err(QueryError::Storage(
        "could not allocate a private spill namespace".into(),
    ))
}

fn valid_namespace_name(name: &str) -> bool {
    name.len() == 38
        && name.starts_with("query-")
        && name[6..].bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn write_active_marker(path: &Path) -> Result<(), QueryError> {
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .map_err(io_error)?;
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs();
    file.write_all(&timestamp.to_be_bytes()).map_err(io_error)?;
    file.sync_data().map_err(io_error)
}

fn sync_directory(path: &Path) -> Result<(), QueryError> {
    File::open(path)
        .map_err(io_error)?
        .sync_all()
        .map_err(io_error)
}

fn validate_header(
    file: &mut File,
    namespace_id: u128,
    handle: SpillHandle,
) -> Result<(), QueryError> {
    file.seek(SeekFrom::Start(0)).map_err(io_error)?;
    let mut magic = [0; 8];
    file.read_exact(&mut magic).map_err(io_error)?;
    let version = read_u32(file)?;
    let stored_namespace = read_u128(file)?;
    let stored_handle = read_u64(file)?;
    let _row_count = read_u64(file)?;
    if &magic != MAGIC
        || version != FORMAT_VERSION
        || stored_namespace != namespace_id
        || stored_handle != handle.get()
    {
        return Err(QueryError::Storage(
            "spill record header is invalid or belongs to another query".into(),
        ));
    }
    Ok(())
}

fn row_checksum(
    namespace_id: u128,
    handle: SpillHandle,
    index: usize,
    estimated_bytes: u64,
    payload: &[u8],
) -> blake3::Hash {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"dtg-query-spill-row-v1");
    hasher.update(&namespace_id.to_be_bytes());
    hasher.update(&handle.get().to_be_bytes());
    hasher.update(&(index as u64).to_be_bytes());
    hasher.update(&estimated_bytes.to_be_bytes());
    hasher.update(payload);
    hasher.finalize()
}

fn validate_row(schema: &RowSchema, row: &[QueryValue]) -> Result<(), QueryError> {
    if row.len() != schema.fields.len() {
        return Err(QueryError::InvalidBatch(
            "spill row width does not match its schema".into(),
        ));
    }
    if schema
        .fields
        .iter()
        .zip(row)
        .any(|(field, value)| !value.matches_type(&field.data_type, field.nullable))
    {
        return Err(QueryError::InvalidBatch(
            "spill row contains a value outside its schema".into(),
        ));
    }
    Ok(())
}

fn encode_row(row: &[QueryValue], maximum: u64) -> Result<Vec<u8>, QueryError> {
    let mut output = Vec::new();
    put_u64(&mut output, row.len() as u64, maximum)?;
    for value in row {
        encode_value(&mut output, value, maximum, 0)?;
    }
    Ok(output)
}

fn encoded_value_size(value: &QueryValue, depth: u8) -> Result<u64, QueryError> {
    if depth >= 32 {
        return Err(QueryError::Storage(
            "spill value nesting exceeds the configured bound".into(),
        ));
    }
    let size = match value {
        QueryValue::Null => 1,
        QueryValue::Boolean(_) => 2,
        QueryValue::Integer(_) | QueryValue::FloatBits(_) => 9,
        QueryValue::Bytes(value) => 9_u64.saturating_add(value.len() as u64),
        QueryValue::String(value) => 9_u64.saturating_add(value.len() as u64),
        QueryValue::List(values) => values.iter().try_fold(9_u64, |size, value| {
            Ok::<_, QueryError>(size.saturating_add(encoded_value_size(value, depth + 1)?))
        })?,
        QueryValue::Map(values) => values.iter().try_fold(9_u64, |size, (name, value)| {
            Ok::<_, QueryError>(
                size.saturating_add(8)
                    .saturating_add(name.len() as u64)
                    .saturating_add(encoded_value_size(value, depth + 1)?),
            )
        })?,
        QueryValue::Vertex(vertex) => vertex.properties().iter().try_fold(
            1_u64 + 16 + 8 + 8 + 8 + 8 + 8,
            |size, (name, value)| {
                Ok::<_, QueryError>(
                    size.saturating_add(8)
                        .saturating_add(name.len() as u64)
                        .saturating_add(encoded_value_size(
                            &QueryValue::from_kernel(value.clone()),
                            depth + 1,
                        )?),
                )
            },
        )?,
        QueryValue::Relationship(edge) => edge.properties().iter().try_fold(
            1_u64 + 16 + 16 + 16 + 8 + edge.edge_type().len() as u64 + 8 + 8 + 8 + 8 + 8,
            |size, (name, value)| {
                Ok::<_, QueryError>(
                    size.saturating_add(8)
                        .saturating_add(name.len() as u64)
                        .saturating_add(encoded_value_size(
                            &QueryValue::from_kernel(value.clone()),
                            depth + 1,
                        )?),
                )
            },
        )?,
    };
    Ok(size)
}

fn encode_value(
    output: &mut Vec<u8>,
    value: &QueryValue,
    maximum: u64,
    depth: u8,
) -> Result<(), QueryError> {
    if depth >= 32 {
        return Err(QueryError::Storage(
            "spill value nesting exceeds the configured bound".into(),
        ));
    }
    match value {
        QueryValue::Null => put_u8(output, 0, maximum),
        QueryValue::Boolean(value) => {
            put_u8(output, 1, maximum)?;
            put_u8(output, u8::from(*value), maximum)
        }
        QueryValue::Integer(value) => {
            put_u8(output, 2, maximum)?;
            put_bytes(output, &value.to_be_bytes(), maximum)
        }
        QueryValue::FloatBits(value) => {
            put_u8(output, 3, maximum)?;
            put_bytes(output, &value.to_be_bytes(), maximum)
        }
        QueryValue::Bytes(value) => {
            put_u8(output, 4, maximum)?;
            put_length_prefixed(output, value, maximum)
        }
        QueryValue::String(value) => {
            put_u8(output, 5, maximum)?;
            put_length_prefixed(output, value.as_bytes(), maximum)
        }
        QueryValue::List(values) => {
            put_u8(output, 6, maximum)?;
            put_u64(output, values.len() as u64, maximum)?;
            for value in values {
                encode_value(output, value, maximum, depth + 1)?;
            }
            Ok(())
        }
        QueryValue::Map(values) => {
            put_u8(output, 7, maximum)?;
            encode_map(
                output,
                values.iter().map(|(name, value)| (name, value.clone())),
                maximum,
                depth + 1,
            )
        }
        QueryValue::Vertex(vertex) => {
            put_u8(output, 8, maximum)?;
            put_bytes(output, &vertex.id().get().to_be_bytes(), maximum)?;
            encode_versioned_entity_header(
                output,
                vertex.version(),
                vertex.valid_time(),
                vertex.transaction_time(),
                maximum,
            )?;
            encode_map(
                output,
                vertex
                    .properties()
                    .iter()
                    .map(|(name, value)| (name, QueryValue::from_kernel(value.clone()))),
                maximum,
                depth + 1,
            )
        }
        QueryValue::Relationship(edge) => {
            put_u8(output, 9, maximum)?;
            put_bytes(output, &edge.id().get().to_be_bytes(), maximum)?;
            put_bytes(output, &edge.source().get().to_be_bytes(), maximum)?;
            put_bytes(output, &edge.target().get().to_be_bytes(), maximum)?;
            put_length_prefixed(output, edge.edge_type().as_bytes(), maximum)?;
            encode_versioned_entity_header(
                output,
                edge.version(),
                edge.valid_time(),
                edge.transaction_time(),
                maximum,
            )?;
            encode_map(
                output,
                edge.properties()
                    .iter()
                    .map(|(name, value)| (name, QueryValue::from_kernel(value.clone()))),
                maximum,
                depth + 1,
            )
        }
    }
}

fn encode_versioned_entity_header(
    output: &mut Vec<u8>,
    version: Version,
    valid_time: ValidInterval,
    transaction_time: TransactionTime,
    maximum: u64,
) -> Result<(), QueryError> {
    put_bytes(output, &version.get().to_be_bytes(), maximum)?;
    put_bytes(output, &valid_time.start().to_be_bytes(), maximum)?;
    put_bytes(output, &valid_time.end().to_be_bytes(), maximum)?;
    put_bytes(output, &transaction_time.get().to_be_bytes(), maximum)
}

fn encode_map<'a>(
    output: &mut Vec<u8>,
    values: impl ExactSizeIterator<Item = (&'a String, QueryValue)>,
    maximum: u64,
    depth: u8,
) -> Result<(), QueryError> {
    put_u64(output, values.len() as u64, maximum)?;
    for (name, value) in values {
        put_length_prefixed(output, name.as_bytes(), maximum)?;
        encode_value(output, &value, maximum, depth)?;
    }
    Ok(())
}

fn decode_row(payload: &[u8], maximum: u64) -> Result<Vec<QueryValue>, QueryError> {
    let mut decoder = Decoder::new(payload, maximum);
    let count = decoder.length()?;
    let mut row = Vec::with_capacity(count);
    for _ in 0..count {
        row.push(decoder.value(0)?);
    }
    decoder.finish()?;
    Ok(row)
}

struct Decoder<'a> {
    remaining: &'a [u8],
    maximum: u64,
}

impl<'a> Decoder<'a> {
    const fn new(bytes: &'a [u8], maximum: u64) -> Self {
        Self {
            remaining: bytes,
            maximum,
        }
    }

    fn value(&mut self, depth: u8) -> Result<QueryValue, QueryError> {
        if depth >= 32 {
            return Err(corrupt("spill value nesting exceeds its bound"));
        }
        match self.u8()? {
            0 => Ok(QueryValue::Null),
            1 => match self.u8()? {
                0 => Ok(QueryValue::Boolean(false)),
                1 => Ok(QueryValue::Boolean(true)),
                _ => Err(corrupt("spill boolean tag is invalid")),
            },
            2 => Ok(QueryValue::Integer(self.i64()?)),
            3 => Ok(QueryValue::FloatBits(self.u64()?)),
            4 => Ok(QueryValue::Bytes(self.bytes()?.to_vec())),
            5 => String::from_utf8(self.bytes()?.to_vec())
                .map(QueryValue::String)
                .map_err(|_| corrupt("spill string is not UTF-8")),
            6 => {
                let count = self.length()?;
                let mut values = Vec::with_capacity(count);
                for _ in 0..count {
                    values.push(self.value(depth + 1)?);
                }
                Ok(QueryValue::List(values))
            }
            7 => self.map(depth + 1).map(QueryValue::Map),
            8 => self.vertex(depth + 1).map(QueryValue::Vertex),
            9 => self.relationship(depth + 1).map(QueryValue::Relationship),
            _ => Err(corrupt("spill value tag is invalid")),
        }
    }

    fn vertex(&mut self, depth: u8) -> Result<VertexVersion, QueryError> {
        let id = VertexId::new(self.u128()?).map_err(storage_error)?;
        let (version, valid_time, transaction_time) = self.versioned_entity_header()?;
        let properties = query_map_to_properties(self.map(depth)?)?;
        VertexVersion::new(id, version, valid_time, transaction_time, properties)
            .map_err(storage_error)
    }

    fn relationship(&mut self, depth: u8) -> Result<EdgeVersion, QueryError> {
        let id = EdgeId::new(self.u128()?).map_err(storage_error)?;
        let source = VertexId::new(self.u128()?).map_err(storage_error)?;
        let target = VertexId::new(self.u128()?).map_err(storage_error)?;
        let edge_type = String::from_utf8(self.bytes()?.to_vec())
            .map_err(|_| corrupt("spill edge type is not UTF-8"))?;
        let (version, valid_time, transaction_time) = self.versioned_entity_header()?;
        let properties = query_map_to_properties(self.map(depth)?)?;
        EdgeVersion::new(
            id,
            source,
            target,
            edge_type,
            version,
            valid_time,
            transaction_time,
            properties,
        )
        .map_err(storage_error)
    }

    fn versioned_entity_header(
        &mut self,
    ) -> Result<(Version, ValidInterval, TransactionTime), QueryError> {
        let version = Version::new(self.u64()?);
        let start = self.i64()?;
        let end = self.i64()?;
        let valid_time =
            ValidInterval::new(start, end).map_err(|error| corrupt(&error.to_string()))?;
        let transaction_time =
            TransactionTime::new(self.i64()?).map_err(|error| corrupt(&error.to_string()))?;
        Ok((version, valid_time, transaction_time))
    }

    fn map(&mut self, depth: u8) -> Result<BTreeMap<String, QueryValue>, QueryError> {
        let count = self.length()?;
        let mut values = BTreeMap::new();
        for _ in 0..count {
            let name = String::from_utf8(self.bytes()?.to_vec())
                .map_err(|_| corrupt("spill map key is not UTF-8"))?;
            let value = self.value(depth)?;
            if values.insert(name, value).is_some() {
                return Err(corrupt("spill map contains a duplicate key"));
            }
        }
        Ok(values)
    }

    fn length(&mut self) -> Result<usize, QueryError> {
        let value = self.u64()?;
        if value > self.maximum {
            return Err(corrupt("spill length exceeds its bound"));
        }
        usize::try_from(value).map_err(|_| corrupt("spill length is unsupported"))
    }

    fn bytes(&mut self) -> Result<&'a [u8], QueryError> {
        let length = self.length()?;
        self.take(length)
    }

    fn u8(&mut self) -> Result<u8, QueryError> {
        Ok(self.take(1)?[0])
    }

    fn u64(&mut self) -> Result<u64, QueryError> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into().expect("slice")))
    }

    fn u128(&mut self) -> Result<u128, QueryError> {
        Ok(u128::from_be_bytes(
            self.take(16)?.try_into().expect("slice"),
        ))
    }

    fn i64(&mut self) -> Result<i64, QueryError> {
        Ok(i64::from_be_bytes(self.take(8)?.try_into().expect("slice")))
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], QueryError> {
        if self.remaining.len() < length {
            return Err(corrupt("spill payload is truncated"));
        }
        let (value, remaining) = self.remaining.split_at(length);
        self.remaining = remaining;
        Ok(value)
    }

    fn finish(self) -> Result<(), QueryError> {
        if self.remaining.is_empty() {
            Ok(())
        } else {
            Err(corrupt("spill payload contains trailing bytes"))
        }
    }
}

fn query_map_to_properties(
    values: BTreeMap<String, QueryValue>,
) -> Result<BTreeMap<String, Value>, QueryError> {
    values
        .into_iter()
        .map(|(name, value)| query_value_to_kernel(value).map(|value| (name, value)))
        .collect()
}

fn query_value_to_kernel(value: QueryValue) -> Result<Value, QueryError> {
    match value {
        QueryValue::Null => Ok(Value::Null),
        QueryValue::Boolean(value) => Ok(Value::Boolean(value)),
        QueryValue::Integer(value) => Ok(Value::Integer(value)),
        QueryValue::FloatBits(value) => Ok(Value::FloatBits(value)),
        QueryValue::Bytes(value) => Ok(Value::Bytes(value)),
        QueryValue::String(value) => Ok(Value::String(value)),
        QueryValue::List(values) => values
            .into_iter()
            .map(query_value_to_kernel)
            .collect::<Result<Vec<_>, _>>()
            .map(Value::List),
        QueryValue::Map(values) => values
            .into_iter()
            .map(|(name, value)| query_value_to_kernel(value).map(|value| (name, value)))
            .collect::<Result<_, _>>()
            .map(Value::Map),
        QueryValue::Vertex(_) | QueryValue::Relationship(_) => Err(corrupt(
            "spill entity properties cannot contain nested graph entities",
        )),
    }
}

fn put_length_prefixed(output: &mut Vec<u8>, value: &[u8], maximum: u64) -> Result<(), QueryError> {
    put_u64(output, value.len() as u64, maximum)?;
    put_bytes(output, value, maximum)
}

fn put_u8(output: &mut Vec<u8>, value: u8, maximum: u64) -> Result<(), QueryError> {
    put_bytes(output, &[value], maximum)
}

fn put_u64(output: &mut Vec<u8>, value: u64, maximum: u64) -> Result<(), QueryError> {
    put_bytes(output, &value.to_be_bytes(), maximum)
}

fn put_bytes(output: &mut Vec<u8>, value: &[u8], maximum: u64) -> Result<(), QueryError> {
    let new_len = output.len().saturating_add(value.len()) as u64;
    if new_len > maximum {
        return Err(QueryError::Storage(
            "spill row exceeds the configured byte bound".into(),
        ));
    }
    output.extend_from_slice(value);
    Ok(())
}

fn read_u32(reader: &mut impl Read) -> Result<u32, QueryError> {
    let mut bytes = [0; 4];
    reader.read_exact(&mut bytes).map_err(io_error)?;
    Ok(u32::from_be_bytes(bytes))
}

fn read_u64(reader: &mut impl Read) -> Result<u64, QueryError> {
    let mut bytes = [0; 8];
    reader.read_exact(&mut bytes).map_err(io_error)?;
    Ok(u64::from_be_bytes(bytes))
}

fn read_u128(reader: &mut impl Read) -> Result<u128, QueryError> {
    let mut bytes = [0; 16];
    reader.read_exact(&mut bytes).map_err(io_error)?;
    Ok(u128::from_be_bytes(bytes))
}

fn io_error(error: std::io::Error) -> QueryError {
    QueryError::Storage(format!("file spill IO failed: {error}"))
}

fn storage_error(error: dtg_storage::StorageError) -> QueryError {
    QueryError::Storage(format!("spill record is invalid: {error}"))
}

fn poisoned<T>(_: std::sync::PoisonError<T>) -> QueryError {
    QueryError::Storage("file spill state lock is poisoned".into())
}

fn corrupt(message: &str) -> QueryError {
    QueryError::Storage(format!("spill record is corrupt: {message}"))
}
