use std::collections::BTreeMap;

use query_executor::{
    ChangeMetadata, ColumnBatch, ColumnVector, EdgeRecord, MAX_BATCH_ROWS, RecordBatch,
    RuntimeValue, VertexRecord,
};
use temporal_ir::{RowSchema, ValueType};
use temporal_storage::{
    EdgeTypeId, ElementId, ElementKind, ElementRef, GraphId, LabelId, PartitionId,
    TemporalEventOperation,
};
use temporal_types::{CanonicalElement, GraphValue, TransactionTime, ValidTime};

use crate::{DISTRIBUTED_QUERY_PROTOCOL_VERSION, DistributedQueryError, SnapshotToken};

const MAGIC: &[u8; 4] = b"DTXE";
const HEADER_BYTES: usize = 168;
const CHECKSUM_OFFSET: usize = 164;
const FLAG_HAS_MORE: u16 = 1;
const MAX_COLUMNS: usize = 1_024;
const MAX_NESTING: u8 = 64;
const MAX_COLLECTION_ENTRIES: usize = 65_536;
pub const MAX_EXCHANGE_PAYLOAD_BYTES: usize = 16 * 1024 * 1024;

const VALUE_NULL: u8 = 0;
const VALUE_FALSE: u8 = 1;
const VALUE_TRUE: u8 = 2;
const VALUE_INTEGER: u8 = 3;
const VALUE_FLOAT: u8 = 4;
const VALUE_STRING: u8 = 5;
const VALUE_BYTES: u8 = 6;
const VALUE_TEMPORAL: u8 = 7;
const VALUE_LIST: u8 = 8;
const VALUE_MAP: u8 = 9;
const VALUE_NODE: u8 = 10;
const VALUE_RELATIONSHIP: u8 = 11;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExchangeCodecLimits {
    max_rows: usize,
    max_payload_bytes: usize,
    max_decoded_bytes: usize,
}

impl ExchangeCodecLimits {
    pub fn new(max_rows: usize, max_payload_bytes: usize) -> Result<Self, DistributedQueryError> {
        if max_rows == 0 || max_rows > MAX_BATCH_ROWS || max_payload_bytes == 0 {
            return Err(DistributedQueryError::InvalidRequest);
        }
        Ok(Self {
            max_rows,
            max_payload_bytes: max_payload_bytes.min(MAX_EXCHANGE_PAYLOAD_BYTES),
            max_decoded_bytes: max_payload_bytes,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExchangeDecodeExpectation {
    shard_id: u32,
    sequence: u64,
    snapshot: SnapshotToken,
    schema: RowSchema,
}

impl ExchangeDecodeExpectation {
    #[must_use]
    pub fn new(shard_id: u32, sequence: u64, snapshot: SnapshotToken, schema: RowSchema) -> Self {
        Self {
            shard_id,
            sequence,
            snapshot,
            schema,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExchangeFrame {
    bytes: Vec<u8>,
    shard_id: u32,
    sequence: u64,
    has_more: bool,
    snapshot: SnapshotToken,
    schema_fingerprint: [u8; 32],
    row_count: usize,
    column_count: usize,
}

impl ExchangeFrame {
    pub fn encode(
        shard_id: u32,
        sequence: u64,
        has_more: bool,
        snapshot: &SnapshotToken,
        batch: &ColumnBatch,
        limits: ExchangeCodecLimits,
    ) -> Result<Self, DistributedQueryError> {
        if batch.row_count() > limits.max_rows || batch.schema().columns().len() > MAX_COLUMNS {
            return Err(DistributedQueryError::PayloadLimit);
        }
        if batch.estimated_bytes()
            > u64::try_from(limits.max_payload_bytes)
                .map_err(|_| DistributedQueryError::PayloadLimit)?
        {
            return Err(DistributedQueryError::PayloadLimit);
        }
        let payload_len = encoded_columns_len(batch)?;
        if payload_len > limits.max_payload_bytes {
            return Err(DistributedQueryError::PayloadLimit);
        }
        let payload = encode_columns(batch, payload_len)?;
        preflight_decoded_bytes(
            &payload,
            batch.schema(),
            batch.row_count(),
            limits.max_decoded_bytes,
        )?;
        let schema_fingerprint = schema_fingerprint(batch.schema());
        let payload_len =
            u32::try_from(payload.len()).map_err(|_| DistributedQueryError::PayloadLimit)?;
        let row_count =
            u32::try_from(batch.row_count()).map_err(|_| DistributedQueryError::PayloadLimit)?;
        let column_count = u32::try_from(batch.schema().columns().len())
            .map_err(|_| DistributedQueryError::PayloadLimit)?;
        let mut bytes = Vec::with_capacity(HEADER_BYTES + payload.len());
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(&DISTRIBUTED_QUERY_PROTOCOL_VERSION.to_be_bytes());
        bytes.extend_from_slice(&(if has_more { FLAG_HAS_MORE } else { 0 }).to_be_bytes());
        bytes.extend_from_slice(&shard_id.to_be_bytes());
        bytes.extend_from_slice(&sequence.to_be_bytes());
        bytes.extend_from_slice(&snapshot.graph_id().to_be_bytes());
        bytes.extend_from_slice(&snapshot.schema_version().to_be_bytes());
        bytes.extend_from_slice(&snapshot.topology_epoch().to_be_bytes());
        bytes.extend_from_slice(&snapshot.transaction_time().physical_micros().to_be_bytes());
        bytes.extend_from_slice(&snapshot.transaction_time().logical().to_be_bytes());
        bytes.extend_from_slice(&snapshot.security_fingerprint());
        bytes.extend_from_slice(&snapshot.fingerprint());
        bytes.extend_from_slice(&schema_fingerprint);
        bytes.extend_from_slice(&row_count.to_be_bytes());
        bytes.extend_from_slice(&column_count.to_be_bytes());
        bytes.extend_from_slice(&payload_len.to_be_bytes());
        bytes.extend_from_slice(&0_u32.to_be_bytes());
        bytes.extend_from_slice(&payload);
        let mut checksum_hasher = crc32fast::Hasher::new();
        checksum_hasher.update(&bytes[..CHECKSUM_OFFSET]);
        checksum_hasher.update(&bytes[HEADER_BYTES..]);
        let checksum = checksum_hasher.finalize();
        bytes[CHECKSUM_OFFSET..HEADER_BYTES].copy_from_slice(&checksum.to_be_bytes());
        #[cfg(any(test, feature = "test-support"))]
        if let Some(metrics) = crate::current_exchange_test_metrics() {
            metrics.record_encoded_frame();
        }
        Ok(Self {
            bytes,
            shard_id,
            sequence,
            has_more,
            snapshot: snapshot.clone(),
            schema_fingerprint,
            row_count: batch.row_count(),
            column_count: batch.schema().columns().len(),
        })
    }

    pub fn from_bytes(
        bytes: Vec<u8>,
        limits: ExchangeCodecLimits,
    ) -> Result<Self, DistributedQueryError> {
        if bytes.len() < HEADER_BYTES {
            return Err(DistributedQueryError::MalformedExchange);
        }
        let mut reader = Reader::new(&bytes);
        if reader.take(4)? != MAGIC {
            return Err(DistributedQueryError::MalformedExchange);
        }
        let version = reader.read_u16()?;
        if version != DISTRIBUTED_QUERY_PROTOCOL_VERSION {
            return Err(DistributedQueryError::ExchangeVersionMismatch(version));
        }
        let flags = reader.read_u16()?;
        if flags & !FLAG_HAS_MORE != 0 {
            return Err(DistributedQueryError::MalformedExchange);
        }
        let shard_id = reader.read_u32()?;
        let sequence = reader.read_u64()?;
        let graph_id = reader.read_u64()?;
        let schema_version = reader.read_u64()?;
        let topology_epoch = reader.read_u64()?;
        let transaction_time = TransactionTime::new(reader.read_i64()?, reader.read_u32()?);
        let security_fingerprint = reader.read_array_32()?;
        let encoded_snapshot_fingerprint = reader.read_array_32()?;
        let snapshot = SnapshotToken::new(
            graph_id,
            schema_version,
            topology_epoch,
            transaction_time,
            security_fingerprint,
        )?;
        if snapshot.fingerprint() != encoded_snapshot_fingerprint {
            return Err(DistributedQueryError::MalformedExchange);
        }
        let schema_fingerprint = reader.read_array_32()?;
        let row_count =
            usize::try_from(reader.read_u32()?).map_err(|_| DistributedQueryError::PayloadLimit)?;
        let column_count =
            usize::try_from(reader.read_u32()?).map_err(|_| DistributedQueryError::PayloadLimit)?;
        let payload_len =
            usize::try_from(reader.read_u32()?).map_err(|_| DistributedQueryError::PayloadLimit)?;
        let checksum = reader.read_u32()?;
        if row_count > limits.max_rows
            || column_count > MAX_COLUMNS
            || payload_len > limits.max_payload_bytes
            || reader.remaining() != payload_len
        {
            return Err(DistributedQueryError::PayloadLimit);
        }
        let mut checksum_hasher = crc32fast::Hasher::new();
        checksum_hasher.update(&bytes[..CHECKSUM_OFFSET]);
        checksum_hasher.update(reader.remaining_bytes());
        if checksum_hasher.finalize() != checksum {
            return Err(DistributedQueryError::ExchangeChecksumMismatch);
        }
        Ok(Self {
            bytes,
            shard_id,
            sequence,
            has_more: flags & FLAG_HAS_MORE != 0,
            snapshot,
            schema_fingerprint,
            row_count,
            column_count,
        })
    }

    pub fn decode(
        &self,
        expected: ExchangeDecodeExpectation,
        limits: ExchangeCodecLimits,
    ) -> Result<DecodedExchangeBatch, DistributedQueryError> {
        self.clone().into_decoded(expected, limits)
    }

    pub fn into_decoded(
        self,
        expected: ExchangeDecodeExpectation,
        limits: ExchangeCodecLimits,
    ) -> Result<DecodedExchangeBatch, DistributedQueryError> {
        #[cfg(any(test, feature = "test-support"))]
        let reservation = crate::current_exchange_test_metrics().map(|metrics| {
            let bytes = u64::try_from(self.bytes.len()).unwrap_or(u64::MAX);
            metrics.reserve_frame(bytes);
            (metrics, bytes)
        });
        let result = (|| {
            let verified = Self::from_bytes(self.bytes, limits)?;
            verified.ensure_expectation(&expected)?;
            let payload = verified
                .bytes
                .get(HEADER_BYTES..)
                .ok_or(DistributedQueryError::MalformedExchange)?;
            preflight_decoded_bytes(
                payload,
                &expected.schema,
                verified.row_count,
                limits.max_decoded_bytes,
            )?;
            let batch = decode_columns(payload, expected.schema, verified.row_count)?;
            #[cfg(any(test, feature = "test-support"))]
            if let Some(metrics) = crate::current_exchange_test_metrics() {
                metrics.record_decoded_frame();
            }
            Ok(DecodedExchangeBatch {
                shard_id: verified.shard_id,
                sequence: verified.sequence,
                has_more: verified.has_more,
                batch,
            })
        })();
        #[cfg(any(test, feature = "test-support"))]
        if let Some((metrics, bytes)) = reservation {
            metrics.release_frame(bytes);
        }
        result
    }

    pub fn decoded_bytes_upper_bound(
        &self,
        expected: &ExchangeDecodeExpectation,
        limits: ExchangeCodecLimits,
    ) -> Result<u64, DistributedQueryError> {
        self.ensure_expectation(expected)?;
        let payload = self
            .bytes
            .get(HEADER_BYTES..)
            .ok_or(DistributedQueryError::MalformedExchange)?;
        preflight_decoded_bytes(
            payload,
            &expected.schema,
            self.row_count,
            limits.max_decoded_bytes,
        )
    }

    fn ensure_expectation(
        &self,
        expected: &ExchangeDecodeExpectation,
    ) -> Result<(), DistributedQueryError> {
        if self.shard_id != expected.shard_id
            || self.sequence != expected.sequence
            || self.snapshot != expected.snapshot
        {
            return Err(DistributedQueryError::ExchangeMetadataMismatch);
        }
        if self.schema_fingerprint != schema_fingerprint(&expected.schema)
            || self.column_count != expected.schema.columns().len()
        {
            return Err(DistributedQueryError::ExchangeSchemaMismatch);
        }
        Ok(())
    }

    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    #[must_use]
    pub const fn shard_id(&self) -> u32 {
        self.shard_id
    }

    #[must_use]
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }

    #[must_use]
    pub const fn has_more(&self) -> bool {
        self.has_more
    }

    #[must_use]
    pub const fn snapshot_fingerprint(&self) -> [u8; 32] {
        self.snapshot.fingerprint()
    }

    #[must_use]
    pub const fn schema_fingerprint(&self) -> [u8; 32] {
        self.schema_fingerprint
    }

    #[must_use]
    pub const fn row_count(&self) -> usize {
        self.row_count
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DecodedExchangeBatch {
    shard_id: u32,
    sequence: u64,
    has_more: bool,
    batch: ColumnBatch,
}

impl DecodedExchangeBatch {
    #[must_use]
    pub const fn shard_id(&self) -> u32 {
        self.shard_id
    }

    #[must_use]
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }

    #[must_use]
    pub const fn has_more(&self) -> bool {
        self.has_more
    }

    #[must_use]
    pub const fn batch(&self) -> &ColumnBatch {
        &self.batch
    }

    pub fn into_batch(self) -> ColumnBatch {
        self.batch
    }
}

#[must_use]
pub fn schema_fingerprint(schema: &RowSchema) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"DTGProxy/ExchangeSchema/1");
    hasher.update(&(schema.columns().len() as u64).to_be_bytes());
    for column in schema.columns() {
        hasher.update(&column.slot().value().to_be_bytes());
        hash_value_type(column.value_type(), &mut hasher);
        hasher.update(&[u8::from(column.nullable())]);
    }
    *hasher.finalize().as_bytes()
}

fn hash_value_type(value_type: &ValueType, hasher: &mut blake3::Hasher) {
    let mut current = value_type;
    loop {
        match current {
            ValueType::Any => {
                hasher.update(&[0]);
            }
            ValueType::Null => {
                hasher.update(&[1]);
            }
            ValueType::Boolean => {
                hasher.update(&[2]);
            }
            ValueType::Integer => {
                hasher.update(&[3]);
            }
            ValueType::Float => {
                hasher.update(&[4]);
            }
            ValueType::String => {
                hasher.update(&[5]);
            }
            ValueType::Bytes => {
                hasher.update(&[6]);
            }
            ValueType::List(item) => {
                hasher.update(&[7]);
                current = item;
                continue;
            }
            ValueType::Map => {
                hasher.update(&[8]);
            }
            ValueType::Node => {
                hasher.update(&[9]);
            }
            ValueType::Relationship => {
                hasher.update(&[10]);
            }
            ValueType::Path => {
                hasher.update(&[11]);
            }
            ValueType::Temporal => {
                hasher.update(&[12]);
            }
            ValueType::Spatial => {
                hasher.update(&[13]);
            }
            ValueType::Vector => {
                hasher.update(&[14]);
            }
        }
        break;
    }
}

fn encoded_columns_len(batch: &ColumnBatch) -> Result<usize, DistributedQueryError> {
    let mut total = 0_usize;
    for (column_index, column) in batch.schema().columns().iter().enumerate() {
        let vector = batch
            .column(column_index)
            .ok_or(DistributedQueryError::MalformedExchange)?;
        total = checked_add_len(total, 4)?;
        total = checked_add_len(total, encoded_value_type_len(column.value_type(), 0)?)?;
        total = checked_add_len(total, 1 + 4 + batch.row_count().div_ceil(8) + 4)?;
        let mut data_len = match column.value_type() {
            ValueType::Boolean => batch.row_count(),
            ValueType::Integer | ValueType::Float | ValueType::Temporal => batch
                .row_count()
                .checked_mul(8)
                .ok_or(DistributedQueryError::PayloadLimit)?,
            ValueType::String | ValueType::Bytes => variable_column_len(vector, batch.row_count())?,
            ValueType::Any
            | ValueType::Null
            | ValueType::List(_)
            | ValueType::Map
            | ValueType::Node
            | ValueType::Relationship
            | ValueType::Path
            | ValueType::Spatial
            | ValueType::Vector => 0,
        };
        for row_index in 0..batch.row_count() {
            let value = batch
                .value_ref(column_index, row_index)
                .ok_or(DistributedQueryError::MalformedExchange)?;
            match column.value_type() {
                ValueType::String | ValueType::Bytes => {}
                ValueType::Any
                | ValueType::Null
                | ValueType::List(_)
                | ValueType::Map
                | ValueType::Node
                | ValueType::Relationship
                | ValueType::Path
                | ValueType::Spatial
                | ValueType::Vector => {
                    data_len = checked_add_len(data_len, 4)?;
                    let boundary = value
                        .boundary()
                        .or_else(|| value.is_null().then_some(&RuntimeValue::Null))
                        .ok_or(DistributedQueryError::UnsupportedExchangeValue)?;
                    data_len = checked_add_len(data_len, encoded_runtime_value_len(boundary, 0)?)?;
                }
                ValueType::Boolean
                | ValueType::Integer
                | ValueType::Float
                | ValueType::Temporal => {}
            }
        }
        total = checked_add_len(total, data_len)?;
    }
    Ok(total)
}

fn encoded_value_type_len(
    value_type: &ValueType,
    depth: u8,
) -> Result<usize, DistributedQueryError> {
    if depth > MAX_NESTING {
        return Err(DistributedQueryError::PayloadLimit);
    }
    match value_type {
        ValueType::List(item) => checked_add_len(1, encoded_value_type_len(item, depth + 1)?),
        _ => Ok(1),
    }
}

fn encoded_runtime_value_len(
    value: &RuntimeValue,
    depth: u8,
) -> Result<usize, DistributedQueryError> {
    if depth > MAX_NESTING {
        return Err(DistributedQueryError::PayloadLimit);
    }
    match value {
        RuntimeValue::Null | RuntimeValue::Boolean(_) => Ok(1),
        RuntimeValue::Integer(_)
        | RuntimeValue::FloatBits(_)
        | RuntimeValue::TimestampMicros(_) => Ok(9),
        RuntimeValue::String(value) => checked_add_len(5, value.len()),
        RuntimeValue::Bytes(value) => checked_add_len(5, value.len()),
        RuntimeValue::List(values) => {
            if values.len() > MAX_COLLECTION_ENTRIES {
                return Err(DistributedQueryError::PayloadLimit);
            }
            let mut len = 5_usize;
            for value in values {
                len = checked_add_len(len, encoded_runtime_value_len(value, depth + 1)?)?;
            }
            Ok(len)
        }
        RuntimeValue::Map(values) => {
            if values.len() > MAX_COLLECTION_ENTRIES {
                return Err(DistributedQueryError::PayloadLimit);
            }
            let mut len = 5_usize;
            for value in values.values() {
                len = checked_add_len(len, 4)?;
                len = checked_add_len(len, encoded_runtime_value_len(value, depth + 1)?)?;
            }
            Ok(len)
        }
        RuntimeValue::Node(value) => {
            let mut len = 1 + 29 + 1;
            if value.label().is_some() {
                len = checked_add_len(len, 4)?;
            }
            len = checked_add_len(len, 4 + canonical_element_len(value.payload())?)?;
            checked_add_len(len, encoded_change_metadata_len(value.change_metadata())?)
        }
        RuntimeValue::Relationship(value) => {
            let len = 1 + 29 + 4 + 29 + 29 + 4 + canonical_element_len(value.payload())?;
            checked_add_len(len, encoded_change_metadata_len(value.change_metadata())?)
        }
    }
}

fn canonical_element_len(payload: &CanonicalElement) -> Result<usize, DistributedQueryError> {
    let mut len = 4 + 8 + 4;
    for value in payload.properties().values() {
        len = checked_add_len(len, 4)?;
        len = checked_add_len(len, graph_value_len(value, 0)?)?;
    }
    Ok(len)
}

fn graph_value_len(value: &GraphValue, depth: u8) -> Result<usize, DistributedQueryError> {
    if depth > MAX_NESTING {
        return Err(DistributedQueryError::PayloadLimit);
    }
    match value {
        GraphValue::Null | GraphValue::Boolean(_) => Ok(1),
        GraphValue::Integer(_) | GraphValue::FloatBits(_) | GraphValue::TimestampMicros(_) => Ok(9),
        GraphValue::String(value) => checked_add_len(5, value.len()),
        GraphValue::Bytes(value) => checked_add_len(5, value.len()),
        GraphValue::List(values) => {
            if values.len() > MAX_COLLECTION_ENTRIES {
                return Err(DistributedQueryError::PayloadLimit);
            }
            let mut len = 5_usize;
            for value in values {
                len = checked_add_len(len, graph_value_len(value, depth + 1)?)?;
            }
            Ok(len)
        }
    }
}

fn encoded_change_metadata_len(
    metadata: Option<ChangeMetadata>,
) -> Result<usize, DistributedQueryError> {
    let Some(metadata) = metadata else {
        return Ok(1);
    };
    if metadata
        .valid_to()
        .is_some_and(|valid_to| valid_to <= metadata.valid_from())
    {
        return Err(DistributedQueryError::UnsupportedExchangeValue);
    }
    Ok(if metadata.valid_to().is_some() {
        35
    } else {
        27
    })
}

fn checked_add_len(left: usize, right: usize) -> Result<usize, DistributedQueryError> {
    left.checked_add(right)
        .ok_or(DistributedQueryError::PayloadLimit)
}

fn encode_columns(
    batch: &ColumnBatch,
    encoded_len: usize,
) -> Result<Vec<u8>, DistributedQueryError> {
    let mut output = Vec::with_capacity(encoded_len);
    for (column_index, column) in batch.schema().columns().iter().enumerate() {
        let vector = batch
            .column(column_index)
            .ok_or(DistributedQueryError::MalformedExchange)?;
        output.extend_from_slice(&column.slot().value().to_be_bytes());
        encode_value_type(column.value_type(), 0, &mut output)?;
        output.push(u8::from(column.nullable()));
        let validity = encode_validity(vector.validity(), batch.row_count())?;
        write_len(&mut output, validity.len())?;
        output.extend_from_slice(&validity);
        let data_len_offset = output.len();
        write_len(&mut output, 0)?;
        let data_start = output.len();
        encode_column_data(
            batch,
            column_index,
            vector,
            column.value_type(),
            &mut output,
        )?;
        let data_len = output.len() - data_start;
        overwrite_len(&mut output[data_len_offset..data_start], data_len)?;
    }
    if output.len() != encoded_len {
        return Err(DistributedQueryError::MalformedExchange);
    }
    Ok(output)
}

fn decode_columns(
    payload: &[u8],
    schema: RowSchema,
    row_count: usize,
) -> Result<ColumnBatch, DistributedQueryError> {
    let mut reader = Reader::new(payload);
    let mut columns = Vec::with_capacity(schema.columns().len());
    for column in schema.columns() {
        if reader.read_u32()? != column.slot().value()
            || decode_value_type(&mut reader, 0)? != *column.value_type()
            || reader.read_u8()? != u8::from(column.nullable())
        {
            return Err(DistributedQueryError::ExchangeSchemaMismatch);
        }
        let validity_len = reader.read_len()?;
        let expected_validity_len = row_count.div_ceil(8);
        if validity_len != expected_validity_len {
            return Err(DistributedQueryError::MalformedExchange);
        }
        let validity = reader.take(validity_len)?.to_vec();
        validate_validity(&validity, row_count)?;
        let data_len = reader.read_len()?;
        let mut data = Reader::new(reader.take(data_len)?);
        let values = decode_column_data(column.value_type(), row_count, &validity, &mut data)?;
        if !data.is_finished() {
            return Err(DistributedQueryError::MalformedExchange);
        }
        columns.push(values);
    }
    if !reader.is_finished() {
        return Err(DistributedQueryError::MalformedExchange);
    }
    let mut rows = vec![Vec::with_capacity(columns.len()); row_count];
    for column in columns {
        for (row, value) in rows.iter_mut().zip(column) {
            row.push(value);
        }
    }
    let records =
        RecordBatch::try_new(schema, rows).map_err(|_| DistributedQueryError::MalformedExchange)?;
    ColumnBatch::from_record_batch(&records).map_err(|_| DistributedQueryError::MalformedExchange)
}

fn preflight_decoded_bytes(
    payload: &[u8],
    schema: &RowSchema,
    row_count: usize,
    max_decoded_bytes: usize,
) -> Result<u64, DistributedQueryError> {
    let mut reader = Reader::new(payload);
    let cell_count = row_count
        .checked_mul(schema.columns().len())
        .ok_or(DistributedQueryError::PayloadLimit)?;
    let runtime_value_bytes = cell_count
        .checked_mul(std::mem::size_of::<RuntimeValue>())
        .and_then(|bytes| bytes.checked_mul(2))
        .ok_or(DistributedQueryError::PayloadLimit)?;
    let row_vector_bytes = row_count
        .checked_mul(std::mem::size_of::<Vec<RuntimeValue>>())
        .ok_or(DistributedQueryError::PayloadLimit)?;
    let mut decoded_bytes = u64::try_from(runtime_value_bytes)
        .map_err(|_| DistributedQueryError::PayloadLimit)?
        .checked_add(
            u64::try_from(row_vector_bytes).map_err(|_| DistributedQueryError::PayloadLimit)?,
        )
        .ok_or(DistributedQueryError::PayloadLimit)?;
    for column in schema.columns() {
        if reader.read_u32()? != column.slot().value()
            || decode_value_type(&mut reader, 0)? != *column.value_type()
            || reader.read_u8()? != u8::from(column.nullable())
        {
            return Err(DistributedQueryError::ExchangeSchemaMismatch);
        }
        let validity_len = reader.read_len()?;
        if validity_len != row_count.div_ceil(8) {
            return Err(DistributedQueryError::MalformedExchange);
        }
        reader.take(validity_len)?;
        decoded_bytes = add_decoded_bytes(decoded_bytes, validity_len)?;
        let data_len = reader.read_len()?;
        let mut data = Reader::new(reader.take(data_len)?);
        match column.value_type() {
            ValueType::Boolean => {
                if data.remaining() != row_count {
                    return Err(DistributedQueryError::MalformedExchange);
                }
                data.take(row_count)?;
                decoded_bytes = add_decoded_bytes(decoded_bytes, row_count)?;
            }
            ValueType::Integer | ValueType::Float | ValueType::Temporal => {
                let bytes = row_count
                    .checked_mul(8)
                    .ok_or(DistributedQueryError::PayloadLimit)?;
                if data.remaining() != bytes {
                    return Err(DistributedQueryError::MalformedExchange);
                }
                data.take(bytes)?;
                decoded_bytes = add_decoded_bytes(decoded_bytes, bytes)?;
            }
            ValueType::String | ValueType::Bytes => {
                let offset_bytes = row_count
                    .checked_add(1)
                    .and_then(|count| count.checked_mul(4))
                    .ok_or(DistributedQueryError::PayloadLimit)?;
                if data.remaining() < offset_bytes {
                    return Err(DistributedQueryError::MalformedExchange);
                }
                let mut previous = 0_usize;
                for index in 0..=row_count {
                    let offset = data.read_len()?;
                    if (index == 0 && offset != 0) || offset < previous {
                        return Err(DistributedQueryError::MalformedExchange);
                    }
                    previous = offset;
                }
                if data.remaining() != previous {
                    return Err(DistributedQueryError::MalformedExchange);
                }
                data.take(previous)?;
                decoded_bytes = add_decoded_bytes(
                    decoded_bytes,
                    offset_bytes
                        .checked_mul(2)
                        .ok_or(DistributedQueryError::PayloadLimit)?,
                )?;
                decoded_bytes = add_decoded_bytes(
                    decoded_bytes,
                    previous
                        .checked_mul(3)
                        .ok_or(DistributedQueryError::PayloadLimit)?,
                )?;
            }
            ValueType::Any
            | ValueType::Null
            | ValueType::List(_)
            | ValueType::Map
            | ValueType::Node
            | ValueType::Relationship
            | ValueType::Path
            | ValueType::Spatial
            | ValueType::Vector => {
                for _ in 0..row_count {
                    let length = data.read_len()?;
                    let mut value = Reader::new(data.take(length)?);
                    decoded_bytes = decoded_bytes
                        .checked_add(
                            measure_runtime_value(&mut value, 0)?
                                .checked_mul(2)
                                .ok_or(DistributedQueryError::PayloadLimit)?,
                        )
                        .ok_or(DistributedQueryError::PayloadLimit)?;
                    if !value.is_finished() {
                        return Err(DistributedQueryError::MalformedExchange);
                    }
                }
            }
        }
        if !data.is_finished()
            || decoded_bytes
                > u64::try_from(max_decoded_bytes)
                    .map_err(|_| DistributedQueryError::PayloadLimit)?
        {
            return Err(DistributedQueryError::PayloadLimit);
        }
    }
    if !reader.is_finished() {
        return Err(DistributedQueryError::MalformedExchange);
    }
    Ok(decoded_bytes)
}

fn measure_runtime_value(reader: &mut Reader<'_>, depth: u8) -> Result<u64, DistributedQueryError> {
    if depth > MAX_NESTING {
        return Err(DistributedQueryError::MalformedExchange);
    }
    let base = u64::try_from(std::mem::size_of::<RuntimeValue>())
        .map_err(|_| DistributedQueryError::PayloadLimit)?;
    let extra = match reader.read_u8()? {
        VALUE_NULL | VALUE_FALSE | VALUE_TRUE => 0,
        VALUE_INTEGER | VALUE_FLOAT | VALUE_TEMPORAL => {
            reader.take(8)?;
            0
        }
        VALUE_STRING | VALUE_BYTES => u64::try_from(reader.read_bytes()?.len())
            .map_err(|_| DistributedQueryError::PayloadLimit)?,
        VALUE_LIST => {
            let count = reader.read_len()?;
            if count > MAX_COLLECTION_ENTRIES || count > reader.remaining() {
                return Err(DistributedQueryError::MalformedExchange);
            }
            let mut bytes = 0_u64;
            for _ in 0..count {
                bytes = bytes
                    .checked_add(measure_runtime_value(reader, depth + 1)?)
                    .ok_or(DistributedQueryError::PayloadLimit)?;
            }
            bytes
        }
        VALUE_MAP => {
            let count = reader.read_len()?;
            if count > MAX_COLLECTION_ENTRIES || count > reader.remaining() / 5 {
                return Err(DistributedQueryError::MalformedExchange);
            }
            let mut bytes = 0_u64;
            for _ in 0..count {
                reader.take(4)?;
                let value = measure_runtime_value(reader, depth + 1)?;
                bytes = bytes
                    .checked_add(64)
                    .and_then(|bytes| bytes.checked_add(value))
                    .ok_or(DistributedQueryError::PayloadLimit)?;
            }
            bytes
        }
        VALUE_NODE => measure_node(reader)?,
        VALUE_RELATIONSHIP => measure_relationship(reader)?,
        _ => return Err(DistributedQueryError::MalformedExchange),
    };
    base.checked_add(extra)
        .ok_or(DistributedQueryError::PayloadLimit)
}

fn measure_node(reader: &mut Reader<'_>) -> Result<u64, DistributedQueryError> {
    reader.take(29)?;
    match reader.read_u8()? {
        0 => {}
        1 => {
            reader.take(4)?;
        }
        _ => return Err(DistributedQueryError::MalformedExchange),
    }
    let payload_bytes = reader.read_bytes()?.len();
    measure_change_metadata(reader)?;
    u64::try_from(payload_bytes)
        .map_err(|_| DistributedQueryError::PayloadLimit)?
        .checked_mul(32)
        .and_then(|bytes| bytes.checked_add(128))
        .ok_or(DistributedQueryError::PayloadLimit)
}

fn measure_relationship(reader: &mut Reader<'_>) -> Result<u64, DistributedQueryError> {
    reader.take(29 + 4 + 29 + 29)?;
    let payload_bytes = reader.read_bytes()?.len();
    measure_change_metadata(reader)?;
    u64::try_from(payload_bytes)
        .map_err(|_| DistributedQueryError::PayloadLimit)?
        .checked_mul(32)
        .and_then(|bytes| bytes.checked_add(256))
        .ok_or(DistributedQueryError::PayloadLimit)
}

fn measure_change_metadata(reader: &mut Reader<'_>) -> Result<(), DistributedQueryError> {
    match reader.read_u8()? {
        0 => Ok(()),
        1 => {
            reader.take(8)?;
            match reader.read_u8()? {
                0 => {}
                1 => {
                    reader.take(8)?;
                }
                _ => return Err(DistributedQueryError::MalformedExchange),
            }
            reader.take(8 + 4 + 4 + 1)?;
            Ok(())
        }
        _ => Err(DistributedQueryError::MalformedExchange),
    }
}

fn add_decoded_bytes(current: u64, additional: usize) -> Result<u64, DistributedQueryError> {
    current
        .checked_add(u64::try_from(additional).map_err(|_| DistributedQueryError::PayloadLimit)?)
        .ok_or(DistributedQueryError::PayloadLimit)
}

fn encode_validity(
    validity_bitmap: &query_executor::ValidityBitmap,
    row_count: usize,
) -> Result<Vec<u8>, DistributedQueryError> {
    let mut validity = vec![0_u8; row_count.div_ceil(8)];
    for index in 0..row_count {
        if validity_bitmap
            .is_valid(index)
            .ok_or(DistributedQueryError::MalformedExchange)?
        {
            validity[index / 8] |= 1 << (index % 8);
        }
    }
    Ok(validity)
}

fn is_valid(validity: &[u8], index: usize) -> Result<bool, DistributedQueryError> {
    validity
        .get(index / 8)
        .map(|byte| byte & (1 << (index % 8)) != 0)
        .ok_or(DistributedQueryError::MalformedExchange)
}

fn validate_validity(validity: &[u8], row_count: usize) -> Result<(), DistributedQueryError> {
    let used_bits = row_count % 8;
    if used_bits == 0 {
        return Ok(());
    }
    let unused_mask = !((1_u8 << used_bits) - 1);
    if validity.last().is_some_and(|byte| byte & unused_mask != 0) {
        return Err(DistributedQueryError::MalformedExchange);
    }
    Ok(())
}

fn encode_column_data(
    batch: &ColumnBatch,
    column_index: usize,
    column: &ColumnVector,
    value_type: &ValueType,
    output: &mut Vec<u8>,
) -> Result<(), DistributedQueryError> {
    match value_type {
        ValueType::Boolean => match column {
            ColumnVector::Boolean { values, .. } => {
                for value in values {
                    output.push(u8::from(*value));
                }
            }
            _ => return Err(DistributedQueryError::MalformedExchange),
        },
        ValueType::Integer => match column {
            ColumnVector::Integer { values, .. } => {
                for value in values {
                    output.extend_from_slice(&value.to_be_bytes());
                }
            }
            _ => return Err(DistributedQueryError::MalformedExchange),
        },
        ValueType::Float => match column {
            ColumnVector::FloatBits { values, .. } => {
                for value in values {
                    output.extend_from_slice(&value.to_be_bytes());
                }
            }
            _ => return Err(DistributedQueryError::MalformedExchange),
        },
        ValueType::Temporal => match column {
            ColumnVector::TimestampMicros { values, .. } => {
                for value in values {
                    output.extend_from_slice(&value.to_be_bytes());
                }
            }
            _ => return Err(DistributedQueryError::MalformedExchange),
        },
        ValueType::String => encode_variable(column, output, false)?,
        ValueType::Bytes => encode_variable(column, output, true)?,
        ValueType::Any
        | ValueType::Null
        | ValueType::List(_)
        | ValueType::Map
        | ValueType::Node
        | ValueType::Relationship => {
            for row in 0..batch.row_count() {
                let value = batch
                    .value_ref(column_index, row)
                    .ok_or(DistributedQueryError::MalformedExchange)?;
                let runtime_value = value
                    .boundary()
                    .or_else(|| value.is_null().then_some(&RuntimeValue::Null))
                    .ok_or(DistributedQueryError::UnsupportedExchangeValue)?;
                let len_offset = output.len();
                write_len(output, 0)?;
                let encoded_start = output.len();
                encode_runtime_value(runtime_value, 0, output)?;
                let encoded_len = output.len() - encoded_start;
                overwrite_len(&mut output[len_offset..encoded_start], encoded_len)?;
            }
        }
        ValueType::Path | ValueType::Spatial | ValueType::Vector => {
            for row in 0..batch.row_count() {
                if !batch
                    .value_ref(column_index, row)
                    .ok_or(DistributedQueryError::MalformedExchange)?
                    .is_null()
                {
                    return Err(DistributedQueryError::UnsupportedExchangeValue);
                }
                write_len(output, 1)?;
                output.push(VALUE_NULL);
            }
        }
    }
    Ok(())
}

fn encode_variable(
    column: &ColumnVector,
    output: &mut Vec<u8>,
    bytes: bool,
) -> Result<(), DistributedQueryError> {
    match column {
        ColumnVector::Utf8 { offsets, data, .. } if !bytes => {
            validate_variable_column(offsets, data, None)?;
            for offset in offsets {
                output.extend_from_slice(&offset.to_be_bytes());
            }
            output.extend_from_slice(data);
            Ok(())
        }
        ColumnVector::Bytes { offsets, data, .. } if bytes => {
            validate_variable_column(offsets, data, None)?;
            for offset in offsets {
                output.extend_from_slice(&offset.to_be_bytes());
            }
            output.extend_from_slice(data);
            Ok(())
        }
        _ => Err(DistributedQueryError::MalformedExchange),
    }
}

fn variable_column_len(
    column: &ColumnVector,
    row_count: usize,
) -> Result<usize, DistributedQueryError> {
    match column {
        ColumnVector::Utf8 { offsets, data, .. } | ColumnVector::Bytes { offsets, data, .. } => {
            validate_variable_column(offsets, data, Some(row_count))?;
            offsets
                .len()
                .checked_mul(4)
                .and_then(|len| len.checked_add(data.len()))
                .ok_or(DistributedQueryError::PayloadLimit)
        }
        _ => Err(DistributedQueryError::MalformedExchange),
    }
}

fn validate_variable_column(
    offsets: &[u32],
    data: &[u8],
    row_count: Option<usize>,
) -> Result<(), DistributedQueryError> {
    if row_count.is_some_and(|count| offsets.len() != count + 1) {
        return Err(DistributedQueryError::MalformedExchange);
    }
    if offsets.first().copied() != Some(0) {
        return Err(DistributedQueryError::MalformedExchange);
    }
    let mut previous = 0_u32;
    for offset in offsets {
        if *offset < previous {
            return Err(DistributedQueryError::MalformedExchange);
        }
        previous = *offset;
    }
    if usize::try_from(previous).ok() != Some(data.len()) {
        return Err(DistributedQueryError::MalformedExchange);
    }
    Ok(())
}

fn overwrite_len(bytes: &mut [u8], len: usize) -> Result<(), DistributedQueryError> {
    if bytes.len() != 4 {
        return Err(DistributedQueryError::MalformedExchange);
    }
    bytes.copy_from_slice(
        &u32::try_from(len)
            .map_err(|_| DistributedQueryError::PayloadLimit)?
            .to_be_bytes(),
    );
    Ok(())
}

fn decode_column_data(
    value_type: &ValueType,
    row_count: usize,
    validity: &[u8],
    reader: &mut Reader<'_>,
) -> Result<Vec<RuntimeValue>, DistributedQueryError> {
    let mut values = Vec::with_capacity(row_count);
    match value_type {
        ValueType::Boolean => {
            for index in 0..row_count {
                let value = reader.read_u8()?;
                if value > 1 {
                    return Err(DistributedQueryError::MalformedExchange);
                }
                let valid = is_valid(validity, index)?;
                if !valid && value != 0 {
                    return Err(DistributedQueryError::MalformedExchange);
                }
                values.push(if valid {
                    RuntimeValue::Boolean(value != 0)
                } else {
                    RuntimeValue::Null
                });
            }
        }
        ValueType::Integer | ValueType::Float | ValueType::Temporal => {
            for index in 0..row_count {
                let raw = reader.read_u64()?;
                let valid = is_valid(validity, index)?;
                if !valid && raw != 0 {
                    return Err(DistributedQueryError::MalformedExchange);
                }
                values.push(if !valid {
                    RuntimeValue::Null
                } else {
                    match value_type {
                        ValueType::Integer => {
                            RuntimeValue::Integer(i64::from_be_bytes(raw.to_be_bytes()))
                        }
                        ValueType::Float => RuntimeValue::FloatBits(raw),
                        ValueType::Temporal => {
                            RuntimeValue::TimestampMicros(i64::from_be_bytes(raw.to_be_bytes()))
                        }
                        _ => unreachable!(),
                    }
                });
            }
        }
        ValueType::String | ValueType::Bytes => {
            let mut offsets = Vec::with_capacity(row_count + 1);
            for _ in 0..=row_count {
                offsets.push(reader.read_len()?);
            }
            if offsets.first() != Some(&0) || offsets.windows(2).any(|pair| pair[0] > pair[1]) {
                return Err(DistributedQueryError::MalformedExchange);
            }
            let data_len = *offsets
                .last()
                .ok_or(DistributedQueryError::MalformedExchange)?;
            let data = reader.take(data_len)?;
            for index in 0..row_count {
                if !is_valid(validity, index)? {
                    if offsets[index] != offsets[index + 1] {
                        return Err(DistributedQueryError::MalformedExchange);
                    }
                    values.push(RuntimeValue::Null);
                    continue;
                }
                let value = data
                    .get(offsets[index]..offsets[index + 1])
                    .ok_or(DistributedQueryError::MalformedExchange)?;
                values.push(if matches!(value_type, ValueType::String) {
                    RuntimeValue::String(
                        String::from_utf8(value.to_vec())
                            .map_err(|_| DistributedQueryError::MalformedExchange)?,
                    )
                } else {
                    RuntimeValue::Bytes(value.to_vec())
                });
            }
        }
        ValueType::Any
        | ValueType::Null
        | ValueType::List(_)
        | ValueType::Map
        | ValueType::Node
        | ValueType::Relationship
        | ValueType::Path
        | ValueType::Spatial
        | ValueType::Vector => {
            for index in 0..row_count {
                let length = reader.read_len()?;
                let mut value_reader = Reader::new(reader.take(length)?);
                let value = decode_runtime_value(&mut value_reader, 0)?;
                if !value_reader.is_finished()
                    || is_valid(validity, index)? == matches!(value, RuntimeValue::Null)
                {
                    return Err(DistributedQueryError::MalformedExchange);
                }
                values.push(value);
            }
        }
    }
    Ok(values)
}

fn encode_value_type(
    value_type: &ValueType,
    depth: u8,
    output: &mut Vec<u8>,
) -> Result<(), DistributedQueryError> {
    if depth > MAX_NESTING {
        return Err(DistributedQueryError::PayloadLimit);
    }
    match value_type {
        ValueType::Any => output.push(0),
        ValueType::Null => output.push(1),
        ValueType::Boolean => output.push(2),
        ValueType::Integer => output.push(3),
        ValueType::Float => output.push(4),
        ValueType::String => output.push(5),
        ValueType::Bytes => output.push(6),
        ValueType::List(item) => {
            output.push(7);
            encode_value_type(item, depth + 1, output)?;
        }
        ValueType::Map => output.push(8),
        ValueType::Node => output.push(9),
        ValueType::Relationship => output.push(10),
        ValueType::Path => output.push(11),
        ValueType::Temporal => output.push(12),
        ValueType::Spatial => output.push(13),
        ValueType::Vector => output.push(14),
    }
    Ok(())
}

fn decode_value_type(
    reader: &mut Reader<'_>,
    depth: u8,
) -> Result<ValueType, DistributedQueryError> {
    if depth > MAX_NESTING {
        return Err(DistributedQueryError::MalformedExchange);
    }
    Ok(match reader.read_u8()? {
        0 => ValueType::Any,
        1 => ValueType::Null,
        2 => ValueType::Boolean,
        3 => ValueType::Integer,
        4 => ValueType::Float,
        5 => ValueType::String,
        6 => ValueType::Bytes,
        7 => ValueType::List(Box::new(decode_value_type(reader, depth + 1)?)),
        8 => ValueType::Map,
        9 => ValueType::Node,
        10 => ValueType::Relationship,
        11 => ValueType::Path,
        12 => ValueType::Temporal,
        13 => ValueType::Spatial,
        14 => ValueType::Vector,
        _ => return Err(DistributedQueryError::MalformedExchange),
    })
}

fn encode_runtime_value(
    value: &RuntimeValue,
    depth: u8,
    output: &mut Vec<u8>,
) -> Result<(), DistributedQueryError> {
    if depth > MAX_NESTING {
        return Err(DistributedQueryError::PayloadLimit);
    }
    match value {
        RuntimeValue::Null => output.push(VALUE_NULL),
        RuntimeValue::Boolean(false) => output.push(VALUE_FALSE),
        RuntimeValue::Boolean(true) => output.push(VALUE_TRUE),
        RuntimeValue::Integer(value) => {
            output.push(VALUE_INTEGER);
            output.extend_from_slice(&value.to_be_bytes());
        }
        RuntimeValue::FloatBits(value) => {
            output.push(VALUE_FLOAT);
            output.extend_from_slice(&value.to_be_bytes());
        }
        RuntimeValue::String(value) => {
            output.push(VALUE_STRING);
            write_len(output, value.len())?;
            output.extend_from_slice(value.as_bytes());
        }
        RuntimeValue::Bytes(value) => {
            output.push(VALUE_BYTES);
            write_len(output, value.len())?;
            output.extend_from_slice(value);
        }
        RuntimeValue::TimestampMicros(value) => {
            output.push(VALUE_TEMPORAL);
            output.extend_from_slice(&value.to_be_bytes());
        }
        RuntimeValue::List(values) => {
            if values.len() > MAX_COLLECTION_ENTRIES {
                return Err(DistributedQueryError::PayloadLimit);
            }
            output.push(VALUE_LIST);
            write_len(output, values.len())?;
            for value in values {
                encode_runtime_value(value, depth + 1, output)?;
            }
        }
        RuntimeValue::Map(values) => {
            if values.len() > MAX_COLLECTION_ENTRIES {
                return Err(DistributedQueryError::PayloadLimit);
            }
            output.push(VALUE_MAP);
            write_len(output, values.len())?;
            for (key, value) in values {
                output.extend_from_slice(&key.to_be_bytes());
                encode_runtime_value(value, depth + 1, output)?;
            }
        }
        RuntimeValue::Node(value) => {
            output.push(VALUE_NODE);
            encode_element_ref(value.element(), output);
            match value.label() {
                Some(label) => {
                    output.push(1);
                    output.extend_from_slice(&label.value().to_be_bytes());
                }
                None => output.push(0),
            }
            encode_payload(value.payload(), output)?;
            encode_change_metadata(value.change_metadata(), output)?;
        }
        RuntimeValue::Relationship(value) => {
            output.push(VALUE_RELATIONSHIP);
            encode_element_ref(value.element(), output);
            output.extend_from_slice(&value.edge_type().value().to_be_bytes());
            encode_element_ref(value.source_ref(), output);
            encode_element_ref(value.destination_ref(), output);
            encode_payload(value.payload(), output)?;
            encode_change_metadata(value.change_metadata(), output)?;
        }
    }
    Ok(())
}

fn decode_runtime_value(
    reader: &mut Reader<'_>,
    depth: u8,
) -> Result<RuntimeValue, DistributedQueryError> {
    if depth > MAX_NESTING {
        return Err(DistributedQueryError::MalformedExchange);
    }
    Ok(match reader.read_u8()? {
        VALUE_NULL => RuntimeValue::Null,
        VALUE_FALSE => RuntimeValue::Boolean(false),
        VALUE_TRUE => RuntimeValue::Boolean(true),
        VALUE_INTEGER => RuntimeValue::Integer(reader.read_i64()?),
        VALUE_FLOAT => RuntimeValue::FloatBits(reader.read_u64()?),
        VALUE_STRING => RuntimeValue::String(
            String::from_utf8(reader.read_bytes()?.to_vec())
                .map_err(|_| DistributedQueryError::MalformedExchange)?,
        ),
        VALUE_BYTES => RuntimeValue::Bytes(reader.read_bytes()?.to_vec()),
        VALUE_TEMPORAL => RuntimeValue::TimestampMicros(reader.read_i64()?),
        VALUE_LIST => {
            let count = reader.read_len()?;
            if count > MAX_COLLECTION_ENTRIES || count > reader.remaining() {
                return Err(DistributedQueryError::MalformedExchange);
            }
            let mut values = Vec::with_capacity(count);
            for _ in 0..count {
                values.push(decode_runtime_value(reader, depth + 1)?);
            }
            RuntimeValue::List(values)
        }
        VALUE_MAP => {
            let count = reader.read_len()?;
            if count > MAX_COLLECTION_ENTRIES || count > reader.remaining() / 5 {
                return Err(DistributedQueryError::MalformedExchange);
            }
            let mut values = BTreeMap::new();
            let mut previous_key = None;
            for _ in 0..count {
                let key = reader.read_u32()?;
                if previous_key.is_some_and(|previous| previous >= key) {
                    return Err(DistributedQueryError::MalformedExchange);
                }
                previous_key = Some(key);
                if values
                    .insert(key, decode_runtime_value(reader, depth + 1)?)
                    .is_some()
                {
                    return Err(DistributedQueryError::MalformedExchange);
                }
            }
            RuntimeValue::Map(values)
        }
        VALUE_NODE => {
            let element = decode_element_ref(reader)?;
            if element.kind() != ElementKind::Vertex {
                return Err(DistributedQueryError::MalformedExchange);
            }
            let label = match reader.read_u8()? {
                0 => None,
                1 => Some(LabelId::new(reader.read_u32()?)),
                _ => return Err(DistributedQueryError::MalformedExchange),
            };
            let payload = decode_payload(reader)?;
            let metadata = decode_change_metadata(reader)?;
            let mut value = VertexRecord::new(element, label, payload);
            if let Some(metadata) = metadata {
                value = value.with_change_metadata(metadata);
            }
            RuntimeValue::Node(value)
        }
        VALUE_RELATIONSHIP => {
            let element = decode_element_ref(reader)?;
            if element.kind() != ElementKind::Edge {
                return Err(DistributedQueryError::MalformedExchange);
            }
            let edge_type = EdgeTypeId::new(reader.read_u32()?);
            let source = decode_element_ref(reader)?;
            let destination = decode_element_ref(reader)?;
            if source.kind() != ElementKind::Vertex || destination.kind() != ElementKind::Vertex {
                return Err(DistributedQueryError::MalformedExchange);
            }
            let payload = decode_payload(reader)?;
            let metadata = decode_change_metadata(reader)?;
            let mut value =
                EdgeRecord::from_endpoints(element, edge_type, source, destination, payload);
            if let Some(metadata) = metadata {
                value = value.with_change_metadata(metadata);
            }
            RuntimeValue::Relationship(value)
        }
        _ => return Err(DistributedQueryError::MalformedExchange),
    })
}

fn encode_element_ref(value: ElementRef, output: &mut Vec<u8>) {
    output.extend_from_slice(&value.graph().value().to_be_bytes());
    output.extend_from_slice(&value.partition().value().to_be_bytes());
    output.push(value.kind() as u8);
    output.extend_from_slice(&value.id().value().to_be_bytes());
}

fn decode_element_ref(reader: &mut Reader<'_>) -> Result<ElementRef, DistributedQueryError> {
    let graph = GraphId::new(reader.read_u64()?);
    let partition = PartitionId::new(reader.read_u32()?);
    let kind = reader.read_u8()?;
    let id = ElementId::new(reader.read_u128()?);
    match kind {
        1 => Ok(ElementRef::vertex(graph, partition, id)),
        2 => Ok(ElementRef::edge(graph, partition, id)),
        _ => Err(DistributedQueryError::MalformedExchange),
    }
}

fn encode_payload(
    payload: &CanonicalElement,
    output: &mut Vec<u8>,
) -> Result<(), DistributedQueryError> {
    let payload = payload
        .encode()
        .map_err(|_| DistributedQueryError::UnsupportedExchangeValue)?;
    write_len(output, payload.len())?;
    output.extend_from_slice(&payload);
    Ok(())
}

fn decode_payload(reader: &mut Reader<'_>) -> Result<CanonicalElement, DistributedQueryError> {
    let encoded = reader.read_bytes()?;
    let payload =
        CanonicalElement::decode(encoded).map_err(|_| DistributedQueryError::MalformedExchange)?;
    if payload
        .encode()
        .map_err(|_| DistributedQueryError::MalformedExchange)?
        != encoded
    {
        return Err(DistributedQueryError::MalformedExchange);
    }
    Ok(payload)
}

fn encode_change_metadata(
    metadata: Option<ChangeMetadata>,
    output: &mut Vec<u8>,
) -> Result<(), DistributedQueryError> {
    let Some(metadata) = metadata else {
        output.push(0);
        return Ok(());
    };
    if metadata
        .valid_to()
        .is_some_and(|valid_to| valid_to <= metadata.valid_from())
    {
        return Err(DistributedQueryError::UnsupportedExchangeValue);
    }
    output.push(1);
    output.extend_from_slice(&metadata.valid_from().as_micros().to_be_bytes());
    match metadata.valid_to() {
        Some(valid_to) => {
            output.push(1);
            output.extend_from_slice(&valid_to.as_micros().to_be_bytes());
        }
        None => output.push(0),
    }
    output.extend_from_slice(&metadata.commit().physical_micros().to_be_bytes());
    output.extend_from_slice(&metadata.commit().logical().to_be_bytes());
    output.extend_from_slice(&metadata.ordinal().to_be_bytes());
    output.push(match metadata.operation() {
        TemporalEventOperation::Put => 1,
        TemporalEventOperation::Delete => 2,
    });
    Ok(())
}

fn decode_change_metadata(
    reader: &mut Reader<'_>,
) -> Result<Option<ChangeMetadata>, DistributedQueryError> {
    match reader.read_u8()? {
        0 => Ok(None),
        1 => {
            let valid_from = ValidTime::from_micros(reader.read_i64()?);
            let valid_to = match reader.read_u8()? {
                0 => None,
                1 => Some(ValidTime::from_micros(reader.read_i64()?)),
                _ => return Err(DistributedQueryError::MalformedExchange),
            };
            if valid_to.is_some_and(|valid_to| valid_to <= valid_from) {
                return Err(DistributedQueryError::MalformedExchange);
            }
            let commit = TransactionTime::new(reader.read_i64()?, reader.read_u32()?);
            let ordinal = reader.read_u32()?;
            let operation = match reader.read_u8()? {
                1 => TemporalEventOperation::Put,
                2 => TemporalEventOperation::Delete,
                _ => return Err(DistributedQueryError::MalformedExchange),
            };
            Ok(Some(ChangeMetadata::new(
                valid_from, valid_to, commit, ordinal, operation,
            )))
        }
        _ => Err(DistributedQueryError::MalformedExchange),
    }
}

fn write_len(output: &mut Vec<u8>, len: usize) -> Result<(), DistributedQueryError> {
    output.extend_from_slice(
        &u32::try_from(len)
            .map_err(|_| DistributedQueryError::PayloadLimit)?
            .to_be_bytes(),
    );
    Ok(())
}

struct Reader<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Reader<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], DistributedQueryError> {
        let end = self
            .position
            .checked_add(length)
            .ok_or(DistributedQueryError::MalformedExchange)?;
        let value = self
            .bytes
            .get(self.position..end)
            .ok_or(DistributedQueryError::MalformedExchange)?;
        self.position = end;
        Ok(value)
    }

    fn read_u8(&mut self) -> Result<u8, DistributedQueryError> {
        self.take(1).map(|value| value[0])
    }

    fn read_u16(&mut self) -> Result<u16, DistributedQueryError> {
        self.take(2)
            .map(|value| u16::from_be_bytes([value[0], value[1]]))
    }

    fn read_u32(&mut self) -> Result<u32, DistributedQueryError> {
        self.take(4)
            .map(|value| u32::from_be_bytes(value.try_into().expect("fixed length")))
    }

    fn read_u64(&mut self) -> Result<u64, DistributedQueryError> {
        self.take(8)
            .map(|value| u64::from_be_bytes(value.try_into().expect("fixed length")))
    }

    fn read_i64(&mut self) -> Result<i64, DistributedQueryError> {
        self.take(8)
            .map(|value| i64::from_be_bytes(value.try_into().expect("fixed length")))
    }

    fn read_u128(&mut self) -> Result<u128, DistributedQueryError> {
        self.take(16)
            .map(|value| u128::from_be_bytes(value.try_into().expect("fixed length")))
    }

    fn read_array_32(&mut self) -> Result<[u8; 32], DistributedQueryError> {
        self.take(32)
            .map(|value| value.try_into().expect("fixed length"))
    }

    fn read_len(&mut self) -> Result<usize, DistributedQueryError> {
        usize::try_from(self.read_u32()?).map_err(|_| DistributedQueryError::MalformedExchange)
    }

    fn read_bytes(&mut self) -> Result<&'a [u8], DistributedQueryError> {
        let length = self.read_len()?;
        self.take(length)
    }

    const fn remaining(&self) -> usize {
        self.bytes.len() - self.position
    }

    fn remaining_bytes(&self) -> &'a [u8] {
        &self.bytes[self.position..]
    }

    const fn is_finished(&self) -> bool {
        self.position == self.bytes.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_canonical_element_rejects_noncanonical_property_order() {
        let mut payload = Vec::new();
        payload.extend_from_slice(b"DTP1");
        payload.extend_from_slice(&3_u64.to_be_bytes());
        payload.extend_from_slice(&2_u32.to_be_bytes());
        payload.extend_from_slice(&2_u32.to_be_bytes());
        payload.push(0);
        payload.extend_from_slice(&1_u32.to_be_bytes());
        payload.push(0);
        let mut framed = Vec::new();
        framed.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        framed.extend_from_slice(&payload);
        let mut reader = Reader::new(&framed);

        assert_eq!(
            decode_payload(&mut reader),
            Err(DistributedQueryError::MalformedExchange)
        );
    }
}
