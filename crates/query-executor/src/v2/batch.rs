use std::collections::BTreeMap;

use temporal_ir::v2::{RowSchema, ValueType};
use temporal_types::GraphValue;

use crate::{EdgeRecord, VertexRecord};

use super::RuntimeError;

pub const MAX_BATCH_ROWS: usize = 16_384;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RuntimeValue {
    Null,
    Boolean(bool),
    Integer(i64),
    FloatBits(u64),
    String(String),
    Bytes(Vec<u8>),
    TimestampMicros(i64),
    List(Vec<Self>),
    Map(BTreeMap<u32, Self>),
    Node(VertexRecord),
    Relationship(EdgeRecord),
}

impl RuntimeValue {
    #[must_use]
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::Null => "NULL",
            Self::Boolean(_) => "BOOLEAN",
            Self::Integer(_) => "INTEGER",
            Self::FloatBits(_) => "FLOAT",
            Self::String(_) => "STRING",
            Self::Bytes(_) => "BYTES",
            Self::TimestampMicros(_) => "TEMPORAL",
            Self::List(_) => "LIST",
            Self::Map(_) => "MAP",
            Self::Node(_) => "NODE",
            Self::Relationship(_) => "RELATIONSHIP",
        }
    }

    pub(crate) fn estimated_bytes(&self) -> Result<u64, RuntimeError> {
        let payload = match self {
            Self::Null => 1,
            Self::Boolean(_) => 2,
            Self::Integer(_) | Self::FloatBits(_) | Self::TimestampMicros(_) => 9,
            Self::String(value) => 5_u64
                .checked_add(u64::try_from(value.len()).map_err(|_| RuntimeError::SizeOverflow)?)
                .ok_or(RuntimeError::SizeOverflow)?,
            Self::Bytes(value) => 5_u64
                .checked_add(u64::try_from(value.len()).map_err(|_| RuntimeError::SizeOverflow)?)
                .ok_or(RuntimeError::SizeOverflow)?,
            Self::List(values) => collection_size(values.iter())?,
            Self::Map(values) => collection_size(values.values())?
                .checked_add(
                    u64::try_from(values.len())
                        .map_err(|_| RuntimeError::SizeOverflow)?
                        .checked_mul(4)
                        .ok_or(RuntimeError::SizeOverflow)?,
                )
                .ok_or(RuntimeError::SizeOverflow)?,
            Self::Node(value) => 64_u64
                .checked_add(
                    u64::try_from(
                        value
                            .payload()
                            .encode()
                            .map_err(|_| RuntimeError::SizeOverflow)?
                            .len(),
                    )
                    .map_err(|_| RuntimeError::SizeOverflow)?,
                )
                .ok_or(RuntimeError::SizeOverflow)?,
            Self::Relationship(value) => 96_u64
                .checked_add(
                    u64::try_from(
                        value
                            .payload()
                            .encode()
                            .map_err(|_| RuntimeError::SizeOverflow)?
                            .len(),
                    )
                    .map_err(|_| RuntimeError::SizeOverflow)?,
                )
                .ok_or(RuntimeError::SizeOverflow)?,
        };
        Ok(payload)
    }
}

fn collection_size<'a>(
    mut values: impl Iterator<Item = &'a RuntimeValue>,
) -> Result<u64, RuntimeError> {
    values.try_fold(5_u64, |size, value| {
        size.checked_add(value.estimated_bytes()?)
            .ok_or(RuntimeError::SizeOverflow)
    })
}

impl From<GraphValue> for RuntimeValue {
    fn from(value: GraphValue) -> Self {
        match value {
            GraphValue::Null => Self::Null,
            GraphValue::Boolean(value) => Self::Boolean(value),
            GraphValue::Integer(value) => Self::Integer(value),
            GraphValue::FloatBits(value) => Self::FloatBits(value),
            GraphValue::String(value) => Self::String(value),
            GraphValue::Bytes(value) => Self::Bytes(value),
            GraphValue::TimestampMicros(value) => Self::TimestampMicros(value),
            GraphValue::List(values) => Self::List(values.into_iter().map(Into::into).collect()),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecordBatch {
    schema: RowSchema,
    rows: Vec<Vec<RuntimeValue>>,
    estimated_bytes: u64,
}

impl RecordBatch {
    pub fn try_new(schema: RowSchema, rows: Vec<Vec<RuntimeValue>>) -> Result<Self, RuntimeError> {
        if rows.len() > MAX_BATCH_ROWS {
            return Err(RuntimeError::BatchTooLarge {
                max: MAX_BATCH_ROWS,
                actual: rows.len(),
            });
        }
        let mut estimated_bytes = 0_u64;
        for row in &rows {
            if row.len() != schema.columns().len() {
                return Err(RuntimeError::RowWidth {
                    expected: schema.columns().len(),
                    actual: row.len(),
                });
            }
            for (column, value) in schema.columns().iter().zip(row) {
                if matches!(value, RuntimeValue::Null) {
                    if !column.nullable() {
                        return Err(RuntimeError::NullInNonNullableColumn {
                            slot: column.slot(),
                        });
                    }
                } else if !matches_type(value, column.value_type()) {
                    return Err(RuntimeError::TypeMismatch {
                        expected: column.value_type().clone(),
                        actual: value.kind(),
                    });
                }
                estimated_bytes = estimated_bytes
                    .checked_add(value.estimated_bytes()?)
                    .ok_or(RuntimeError::SizeOverflow)?;
            }
        }
        Ok(Self {
            schema,
            rows,
            estimated_bytes,
        })
    }

    #[must_use]
    pub const fn schema(&self) -> &RowSchema {
        &self.schema
    }

    #[must_use]
    pub fn rows(&self) -> &[Vec<RuntimeValue>] {
        &self.rows
    }

    #[must_use]
    pub const fn estimated_bytes(&self) -> u64 {
        self.estimated_bytes
    }

    pub fn rechunk(self, max_rows: usize) -> Result<Vec<Self>, RuntimeError> {
        if max_rows == 0 || max_rows > MAX_BATCH_ROWS {
            return Err(RuntimeError::InvalidBatchRows(max_rows));
        }
        if self.rows.is_empty() {
            return Ok(vec![self]);
        }
        self.rows
            .chunks(max_rows)
            .map(|rows| Self::try_new(self.schema.clone(), rows.to_vec()))
            .collect()
    }

    pub(crate) fn into_rows(self) -> Vec<Vec<RuntimeValue>> {
        self.rows
    }
}

fn matches_type(value: &RuntimeValue, expected: &ValueType) -> bool {
    match (value, expected) {
        (_, ValueType::Any) => true,
        (RuntimeValue::Boolean(_), ValueType::Boolean)
        | (RuntimeValue::Integer(_), ValueType::Integer)
        | (RuntimeValue::FloatBits(_), ValueType::Float)
        | (RuntimeValue::String(_), ValueType::String)
        | (RuntimeValue::Bytes(_), ValueType::Bytes)
        | (RuntimeValue::Map(_), ValueType::Map)
        | (RuntimeValue::Node(_), ValueType::Node)
        | (RuntimeValue::Relationship(_), ValueType::Relationship)
        | (RuntimeValue::TimestampMicros(_), ValueType::Temporal) => true,
        (RuntimeValue::List(values), ValueType::List(item)) => values
            .iter()
            .all(|value| matches!(value, RuntimeValue::Null) || matches_type(value, item)),
        _ => false,
    }
}
