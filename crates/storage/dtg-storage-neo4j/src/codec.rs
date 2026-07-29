use std::collections::BTreeMap;

use dtg_storage::{
    Digest32, EdgeId, EdgeTombstone, EdgeVersion, LogicalMutation, Properties, ReplicaMetadata,
    StorageError, TransactionId, TransactionRecord, TransactionState, TransactionTime,
    ValidInterval, Value, Version, VertexId, VertexTombstone, VertexVersion,
};

const MAX_NESTING: usize = 64;

pub(crate) fn encode_value(value: &Value) -> Result<Vec<u8>, StorageError> {
    let mut encoder = Encoder::default();
    encoder.value(value, 0)?;
    Ok(encoder.finish())
}

pub(crate) fn decode_value(bytes: &[u8]) -> Result<Value, StorageError> {
    let mut decoder = Decoder::new(bytes);
    let value = decoder.value(0)?;
    decoder.finish()?;
    Ok(value)
}

pub(crate) fn encode_mutation(mutation: &LogicalMutation) -> Result<Vec<u8>, StorageError> {
    let mut encoder = Encoder::default();
    encoder.mutation(mutation)?;
    Ok(encoder.finish())
}

pub(crate) fn decode_mutation(bytes: &[u8]) -> Result<LogicalMutation, StorageError> {
    let mut decoder = Decoder::new(bytes);
    let mutation = decoder.mutation()?;
    decoder.finish()?;
    Ok(mutation)
}

pub(crate) const fn mutation_kind(mutation: &LogicalMutation) -> i16 {
    match mutation {
        LogicalMutation::PutVertex(_) => 1,
        LogicalMutation::DeleteVertex(_) => 2,
        LogicalMutation::PutEdge(_) => 3,
        LogicalMutation::DeleteEdge(_) => 4,
        LogicalMutation::PutTransaction(_) => 5,
        LogicalMutation::PutReplicaMetadata(_) => 6,
    }
}

#[derive(Default)]
struct Encoder {
    bytes: Vec<u8>,
}

impl Encoder {
    fn finish(self) -> Vec<u8> {
        self.bytes
    }

    fn u8(&mut self, value: u8) {
        self.bytes.push(value);
    }

    fn u32(&mut self, value: u32) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    fn u64(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    fn u128(&mut self, value: u128) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    fn i64(&mut self, value: i64) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    fn bytes(&mut self, value: &[u8]) -> Result<(), StorageError> {
        let length = u32::try_from(value.len())
            .map_err(|_| StorageError::Internal("Neo4j value is too large".into()))?;
        self.u32(length);
        self.bytes.extend_from_slice(value);
        Ok(())
    }

    fn string(&mut self, value: &str) -> Result<(), StorageError> {
        self.bytes(value.as_bytes())
    }

    fn digest(&mut self, value: Digest32) {
        self.bytes.extend_from_slice(&value.get());
    }

    fn properties(&mut self, properties: &Properties) -> Result<(), StorageError> {
        let length = u32::try_from(properties.len())
            .map_err(|_| StorageError::Internal("too many Neo4j properties".into()))?;
        self.u32(length);
        for (name, value) in properties {
            self.string(name)?;
            self.value(value, 0)?;
        }
        Ok(())
    }

    fn value(&mut self, value: &Value, depth: usize) -> Result<(), StorageError> {
        if depth > MAX_NESTING {
            return Err(StorageError::Internal(
                "Neo4j value nesting exceeds the provider bound".into(),
            ));
        }
        match value {
            Value::Null => self.u8(0),
            Value::Boolean(value) => {
                self.u8(1);
                self.u8(u8::from(*value));
            }
            Value::Integer(value) => {
                self.u8(2);
                self.i64(*value);
            }
            Value::FloatBits(value) => {
                self.u8(3);
                self.u64(*value);
            }
            Value::Bytes(value) => {
                self.u8(4);
                self.bytes(value)?;
            }
            Value::String(value) => {
                self.u8(5);
                self.string(value)?;
            }
            Value::List(values) => {
                self.u8(6);
                let length = u32::try_from(values.len())
                    .map_err(|_| StorageError::Internal("Neo4j list is too large".into()))?;
                self.u32(length);
                for value in values {
                    self.value(value, depth + 1)?;
                }
            }
            Value::Map(values) => {
                self.u8(7);
                let length = u32::try_from(values.len())
                    .map_err(|_| StorageError::Internal("Neo4j map is too large".into()))?;
                self.u32(length);
                for (name, value) in values {
                    self.string(name)?;
                    self.value(value, depth + 1)?;
                }
            }
        }
        Ok(())
    }

    fn vertex(&mut self, vertex: &VertexVersion) -> Result<(), StorageError> {
        self.u128(vertex.id().get());
        self.u64(vertex.version().get());
        self.i64(vertex.valid_time().start());
        self.i64(vertex.valid_time().end());
        self.i64(vertex.transaction_time().get());
        self.properties(vertex.properties())
    }

    fn edge(&mut self, edge: &EdgeVersion) -> Result<(), StorageError> {
        self.u128(edge.id().get());
        self.u128(edge.source().get());
        self.u128(edge.target().get());
        self.string(edge.edge_type())?;
        self.u64(edge.version().get());
        self.i64(edge.valid_time().start());
        self.i64(edge.valid_time().end());
        self.i64(edge.transaction_time().get());
        self.properties(edge.properties())
    }

    fn transaction(&mut self, transaction: &TransactionRecord) {
        self.u128(transaction.id().get());
        self.u8(match transaction.state() {
            TransactionState::Prepared => 1,
            TransactionState::Committed => 2,
            TransactionState::Aborted => 3,
        });
        self.i64(transaction.transaction_time().get());
        self.digest(transaction.record_digest());
    }

    fn mutation(&mut self, mutation: &LogicalMutation) -> Result<(), StorageError> {
        self.u8(mutation_kind(mutation) as u8);
        match mutation {
            LogicalMutation::PutVertex(vertex) => self.vertex(vertex)?,
            LogicalMutation::DeleteVertex(tombstone) => {
                self.u128(tombstone.id().get());
                self.u64(tombstone.version().get());
                self.i64(tombstone.transaction_time().get());
            }
            LogicalMutation::PutEdge(edge) => self.edge(edge)?,
            LogicalMutation::DeleteEdge(tombstone) => {
                self.u128(tombstone.id().get());
                self.u64(tombstone.version().get());
                self.i64(tombstone.transaction_time().get());
            }
            LogicalMutation::PutTransaction(transaction) => self.transaction(transaction),
            LogicalMutation::PutReplicaMetadata(metadata) => {
                self.string(metadata.name())?;
                self.value(metadata.value(), 0)?;
            }
        }
        Ok(())
    }
}

struct Decoder<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Decoder<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn finish(&self) -> Result<(), StorageError> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(corrupt("trailing bytes in Neo4j typed value"))
        }
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], StorageError> {
        let end = self
            .offset
            .checked_add(length)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(|| corrupt("truncated Neo4j typed value"))?;
        let value = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(value)
    }

    fn u8(&mut self) -> Result<u8, StorageError> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, StorageError> {
        Ok(u32::from_be_bytes(
            self.take(4)?
                .try_into()
                .map_err(|_| corrupt("invalid Neo4j u32"))?,
        ))
    }

    fn u64(&mut self) -> Result<u64, StorageError> {
        Ok(u64::from_be_bytes(
            self.take(8)?
                .try_into()
                .map_err(|_| corrupt("invalid Neo4j u64"))?,
        ))
    }

    fn u128(&mut self) -> Result<u128, StorageError> {
        Ok(u128::from_be_bytes(
            self.take(16)?
                .try_into()
                .map_err(|_| corrupt("invalid Neo4j u128"))?,
        ))
    }

    fn i64(&mut self) -> Result<i64, StorageError> {
        Ok(i64::from_be_bytes(
            self.take(8)?
                .try_into()
                .map_err(|_| corrupt("invalid Neo4j i64"))?,
        ))
    }

    fn bytes(&mut self) -> Result<Vec<u8>, StorageError> {
        let length =
            usize::try_from(self.u32()?).map_err(|_| corrupt("invalid Neo4j byte length"))?;
        Ok(self.take(length)?.to_vec())
    }

    fn string(&mut self) -> Result<String, StorageError> {
        String::from_utf8(self.bytes()?).map_err(|_| corrupt("invalid Neo4j UTF-8"))
    }

    fn digest(&mut self) -> Result<Digest32, StorageError> {
        Ok(Digest32::new(
            self.take(32)?
                .try_into()
                .map_err(|_| corrupt("invalid Neo4j digest"))?,
        ))
    }

    fn properties(&mut self, depth: usize) -> Result<Properties, StorageError> {
        if depth > MAX_NESTING {
            return Err(corrupt("Neo4j property nesting exceeds provider bound"));
        }
        let count = self.u32()?;
        let mut values = BTreeMap::new();
        for _ in 0..count {
            let name = self.string()?;
            let value = self.value(depth + 1)?;
            if values.insert(name, value).is_some() {
                return Err(corrupt("duplicate Neo4j property name"));
            }
        }
        Ok(values)
    }

    fn value(&mut self, depth: usize) -> Result<Value, StorageError> {
        if depth > MAX_NESTING {
            return Err(corrupt("Neo4j value nesting exceeds provider bound"));
        }
        match self.u8()? {
            0 => Ok(Value::Null),
            1 => match self.u8()? {
                0 => Ok(Value::Boolean(false)),
                1 => Ok(Value::Boolean(true)),
                _ => Err(corrupt("invalid Neo4j boolean")),
            },
            2 => Ok(Value::Integer(self.i64()?)),
            3 => Ok(Value::FloatBits(self.u64()?)),
            4 => Ok(Value::Bytes(self.bytes()?)),
            5 => Ok(Value::String(self.string()?)),
            6 => {
                let count = self.u32()?;
                let mut values = Vec::new();
                for _ in 0..count {
                    values.push(self.value(depth + 1)?);
                }
                Ok(Value::List(values))
            }
            7 => Ok(Value::Map(self.properties(depth + 1)?)),
            _ => Err(corrupt("unknown Neo4j value tag")),
        }
    }

    fn vertex(&mut self) -> Result<VertexVersion, StorageError> {
        VertexVersion::new(
            VertexId::new(self.u128()?)?,
            Version::new(self.u64()?),
            ValidInterval::new(self.i64()?, self.i64()?).map_err(kernel_error)?,
            TransactionTime::new(self.i64()?).map_err(kernel_error)?,
            self.properties(0)?,
        )
    }

    fn edge(&mut self) -> Result<EdgeVersion, StorageError> {
        EdgeVersion::new(
            EdgeId::new(self.u128()?)?,
            VertexId::new(self.u128()?)?,
            VertexId::new(self.u128()?)?,
            self.string()?,
            Version::new(self.u64()?),
            ValidInterval::new(self.i64()?, self.i64()?).map_err(kernel_error)?,
            TransactionTime::new(self.i64()?).map_err(kernel_error)?,
            self.properties(0)?,
        )
    }

    fn transaction(&mut self) -> Result<TransactionRecord, StorageError> {
        let id = TransactionId::new(self.u128()?).map_err(kernel_error)?;
        let state = match self.u8()? {
            1 => TransactionState::Prepared,
            2 => TransactionState::Committed,
            3 => TransactionState::Aborted,
            _ => return Err(corrupt("unknown Neo4j transaction state")),
        };
        TransactionRecord::new(
            id,
            state,
            TransactionTime::new(self.i64()?).map_err(kernel_error)?,
            self.digest()?,
        )
    }

    fn mutation(&mut self) -> Result<LogicalMutation, StorageError> {
        match self.u8()? {
            1 => Ok(LogicalMutation::PutVertex(self.vertex()?)),
            2 => Ok(LogicalMutation::DeleteVertex(VertexTombstone::new(
                VertexId::new(self.u128()?)?,
                Version::new(self.u64()?),
                TransactionTime::new(self.i64()?).map_err(kernel_error)?,
            ))),
            3 => Ok(LogicalMutation::PutEdge(self.edge()?)),
            4 => Ok(LogicalMutation::DeleteEdge(EdgeTombstone::new(
                EdgeId::new(self.u128()?)?,
                Version::new(self.u64()?),
                TransactionTime::new(self.i64()?).map_err(kernel_error)?,
            ))),
            5 => Ok(LogicalMutation::PutTransaction(self.transaction()?)),
            6 => Ok(LogicalMutation::PutReplicaMetadata(ReplicaMetadata::new(
                self.string()?,
                self.value(0)?,
            )?)),
            _ => Err(corrupt("unknown Neo4j mutation tag")),
        }
    }
}

fn corrupt(message: &str) -> StorageError {
    StorageError::Internal(message.into())
}

fn kernel_error(error: dtg_kernel::KernelError) -> StorageError {
    StorageError::Internal(format!("invalid typed Neo4j value: {error}"))
}
