use temporal_ir::{RowSchema, ValueType};

use crate::{RecordBatch, RuntimeError, RuntimeValue};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ColumnValueRef<'a> {
    Null,
    Boolean(bool),
    Integer(i64),
    FloatBits(u64),
    TimestampMicros(i64),
    Utf8(&'a str),
    Bytes(&'a [u8]),
    Boundary(&'a RuntimeValue),
}

impl<'a> ColumnValueRef<'a> {
    #[must_use]
    pub const fn is_null(self) -> bool {
        matches!(self, Self::Null)
    }

    #[must_use]
    pub const fn boolean(self) -> Option<bool> {
        match self {
            Self::Boolean(value) => Some(value),
            _ => None,
        }
    }

    #[must_use]
    pub const fn integer(self) -> Option<i64> {
        match self {
            Self::Integer(value) => Some(value),
            _ => None,
        }
    }

    #[must_use]
    pub const fn float_bits(self) -> Option<u64> {
        match self {
            Self::FloatBits(value) => Some(value),
            _ => None,
        }
    }

    #[must_use]
    pub const fn timestamp_micros(self) -> Option<i64> {
        match self {
            Self::TimestampMicros(value) => Some(value),
            _ => None,
        }
    }

    #[must_use]
    pub const fn utf8(self) -> Option<&'a str> {
        match self {
            Self::Utf8(value) => Some(value),
            _ => None,
        }
    }

    #[must_use]
    pub const fn bytes(self) -> Option<&'a [u8]> {
        match self {
            Self::Bytes(value) => Some(value),
            _ => None,
        }
    }

    #[must_use]
    pub const fn boundary(self) -> Option<&'a RuntimeValue> {
        match self {
            Self::Boundary(value) => Some(value),
            _ => None,
        }
    }

    #[must_use]
    pub fn into_owned(self) -> RuntimeValue {
        match self {
            Self::Null => RuntimeValue::Null,
            Self::Boolean(value) => RuntimeValue::Boolean(value),
            Self::Integer(value) => RuntimeValue::Integer(value),
            Self::FloatBits(value) => RuntimeValue::FloatBits(value),
            Self::TimestampMicros(value) => RuntimeValue::TimestampMicros(value),
            Self::Utf8(value) => RuntimeValue::String(value.to_owned()),
            Self::Bytes(value) => RuntimeValue::Bytes(value.to_vec()),
            Self::Boundary(value) => value.clone(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidityBitmap {
    bits: Vec<u64>,
    len: usize,
}

impl ValidityBitmap {
    fn from_values(values: &[RuntimeValue]) -> Self {
        let mut bits = vec![0_u64; values.len().div_ceil(64)];
        for (index, value) in values.iter().enumerate() {
            if !matches!(value, RuntimeValue::Null) {
                bits[index / 64] |= 1_u64 << (index % 64);
            }
        }
        Self {
            bits,
            len: values.len(),
        }
    }

    #[must_use]
    pub fn is_valid(&self, index: usize) -> Option<bool> {
        (index < self.len).then(|| self.bits[index / 64] & (1_u64 << (index % 64)) != 0)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ColumnVector {
    Boolean {
        values: Vec<bool>,
        validity: ValidityBitmap,
    },
    Integer {
        values: Vec<i64>,
        validity: ValidityBitmap,
    },
    FloatBits {
        values: Vec<u64>,
        validity: ValidityBitmap,
    },
    TimestampMicros {
        values: Vec<i64>,
        validity: ValidityBitmap,
    },
    Utf8 {
        offsets: Vec<u32>,
        data: Vec<u8>,
        validity: ValidityBitmap,
    },
    Bytes {
        offsets: Vec<u32>,
        data: Vec<u8>,
        validity: ValidityBitmap,
    },
    BoundaryValues {
        values: Vec<RuntimeValue>,
        validity: ValidityBitmap,
    },
}

impl ColumnVector {
    fn from_values(expected: &ValueType, values: Vec<RuntimeValue>) -> Result<Self, RuntimeError> {
        let validity = ValidityBitmap::from_values(&values);
        match expected {
            ValueType::Boolean => Ok(Self::Boolean {
                values: values
                    .iter()
                    .map(|value| match value {
                        RuntimeValue::Boolean(value) => Ok(*value),
                        RuntimeValue::Null => Ok(false),
                        value => Err(RuntimeError::TypeMismatch {
                            expected: ValueType::Boolean,
                            actual: value.kind(),
                        }),
                    })
                    .collect::<Result<_, _>>()?,
                validity,
            }),
            ValueType::Integer => Ok(Self::Integer {
                values: values
                    .iter()
                    .map(|value| match value {
                        RuntimeValue::Integer(value) => Ok(*value),
                        RuntimeValue::Null => Ok(0),
                        value => Err(RuntimeError::TypeMismatch {
                            expected: ValueType::Integer,
                            actual: value.kind(),
                        }),
                    })
                    .collect::<Result<_, _>>()?,
                validity,
            }),
            ValueType::Float => Ok(Self::FloatBits {
                values: values
                    .iter()
                    .map(|value| match value {
                        RuntimeValue::FloatBits(value) => Ok(*value),
                        RuntimeValue::Null => Ok(0),
                        value => Err(RuntimeError::TypeMismatch {
                            expected: ValueType::Float,
                            actual: value.kind(),
                        }),
                    })
                    .collect::<Result<_, _>>()?,
                validity,
            }),
            ValueType::Temporal => Ok(Self::TimestampMicros {
                values: values
                    .iter()
                    .map(|value| match value {
                        RuntimeValue::TimestampMicros(value) => Ok(*value),
                        RuntimeValue::Null => Ok(0),
                        value => Err(RuntimeError::TypeMismatch {
                            expected: ValueType::Temporal,
                            actual: value.kind(),
                        }),
                    })
                    .collect::<Result<_, _>>()?,
                validity,
            }),
            ValueType::String => Self::variable(values, validity, false),
            ValueType::Bytes => Self::variable(values, validity, true),
            ValueType::Any
            | ValueType::Null
            | ValueType::List(_)
            | ValueType::Map
            | ValueType::Node
            | ValueType::Relationship
            | ValueType::Path
            | ValueType::Spatial
            | ValueType::Vector => Ok(Self::BoundaryValues { values, validity }),
        }
    }

    fn variable(
        values: Vec<RuntimeValue>,
        validity: ValidityBitmap,
        bytes: bool,
    ) -> Result<Self, RuntimeError> {
        let mut offsets = Vec::with_capacity(values.len() + 1);
        let mut data = Vec::new();
        offsets.push(0);
        for value in values {
            match value {
                RuntimeValue::String(value) if !bytes => data.extend_from_slice(value.as_bytes()),
                RuntimeValue::Bytes(value) if bytes => data.extend_from_slice(&value),
                RuntimeValue::Null => {}
                value => {
                    return Err(RuntimeError::TypeMismatch {
                        expected: if bytes {
                            ValueType::Bytes
                        } else {
                            ValueType::String
                        },
                        actual: value.kind(),
                    });
                }
            }
            offsets.push(u32::try_from(data.len()).map_err(|_| RuntimeError::SizeOverflow)?);
        }
        if bytes {
            Ok(Self::Bytes {
                offsets,
                data,
                validity,
            })
        } else {
            Ok(Self::Utf8 {
                offsets,
                data,
                validity,
            })
        }
    }

    fn value_ref_at(&self, index: usize) -> Option<ColumnValueRef<'_>> {
        if !self.validity().is_valid(index)? {
            return Some(ColumnValueRef::Null);
        }
        match self {
            Self::Boolean { values, .. } => values.get(index).copied().map(ColumnValueRef::Boolean),
            Self::Integer { values, .. } => values.get(index).copied().map(ColumnValueRef::Integer),
            Self::FloatBits { values, .. } => {
                values.get(index).copied().map(ColumnValueRef::FloatBits)
            }
            Self::TimestampMicros { values, .. } => values
                .get(index)
                .copied()
                .map(ColumnValueRef::TimestampMicros),
            Self::Utf8 { offsets, data, .. } => variable_value_ref(offsets, data, index, false),
            Self::Bytes { offsets, data, .. } => variable_value_ref(offsets, data, index, true),
            Self::BoundaryValues { values, .. } => values.get(index).map(ColumnValueRef::Boundary),
        }
    }

    fn value_at(&self, index: usize) -> Option<RuntimeValue> {
        self.value_ref_at(index).map(ColumnValueRef::into_owned)
    }

    #[must_use]
    pub const fn validity(&self) -> &ValidityBitmap {
        match self {
            Self::Boolean { validity, .. }
            | Self::Integer { validity, .. }
            | Self::FloatBits { validity, .. }
            | Self::TimestampMicros { validity, .. }
            | Self::Utf8 { validity, .. }
            | Self::Bytes { validity, .. }
            | Self::BoundaryValues { validity, .. } => validity,
        }
    }

    fn variable_width_bytes(&self) -> u64 {
        match self {
            Self::Utf8 { data, .. } | Self::Bytes { data, .. } => {
                u64::try_from(data.len()).unwrap_or(u64::MAX)
            }
            Self::BoundaryValues { values, .. } => values.iter().fold(0_u64, |total, value| {
                total.saturating_add(value.variable_width_bytes())
            }),
            Self::Boolean { .. }
            | Self::Integer { .. }
            | Self::FloatBits { .. }
            | Self::TimestampMicros { .. } => 0,
        }
    }
}

fn variable_value_ref<'a>(
    offsets: &[u32],
    data: &'a [u8],
    index: usize,
    bytes: bool,
) -> Option<ColumnValueRef<'a>> {
    let start = usize::try_from(*offsets.get(index)?).ok()?;
    let end = usize::try_from(*offsets.get(index + 1)?).ok()?;
    let value = data.get(start..end)?;
    if bytes {
        Some(ColumnValueRef::Bytes(value))
    } else {
        std::str::from_utf8(value).ok().map(ColumnValueRef::Utf8)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ColumnBatch {
    schema: RowSchema,
    columns: Vec<ColumnVector>,
    row_count: usize,
    estimated_bytes: u64,
}

impl ColumnBatch {
    pub fn from_record_batch(batch: &RecordBatch) -> Result<Self, RuntimeError> {
        let mut columns = Vec::with_capacity(batch.schema().columns().len());
        for (column_index, column) in batch.schema().columns().iter().enumerate() {
            let values = batch
                .rows()
                .iter()
                .map(|row| row[column_index].clone())
                .collect();
            columns.push(ColumnVector::from_values(column.value_type(), values)?);
        }
        Ok(Self {
            schema: batch.schema().clone(),
            columns,
            row_count: batch.rows().len(),
            estimated_bytes: batch.estimated_bytes(),
        })
    }

    #[must_use]
    pub const fn schema(&self) -> &RowSchema {
        &self.schema
    }

    #[must_use]
    pub const fn row_count(&self) -> usize {
        self.row_count
    }

    #[must_use]
    pub const fn estimated_bytes(&self) -> u64 {
        self.estimated_bytes
    }

    #[must_use]
    pub fn variable_width_bytes(&self) -> u64 {
        self.columns.iter().fold(0_u64, |total, column| {
            total.saturating_add(column.variable_width_bytes())
        })
    }

    #[must_use]
    pub fn column(&self, index: usize) -> Option<&ColumnVector> {
        self.columns.get(index)
    }

    pub fn value(&self, column: usize, row: usize) -> Option<RuntimeValue> {
        self.value_ref(column, row).map(ColumnValueRef::into_owned)
    }

    pub fn value_ref(&self, column: usize, row: usize) -> Option<ColumnValueRef<'_>> {
        (row < self.row_count).then(|| self.columns.get(column)?.value_ref_at(row))?
    }

    pub fn row(&self, index: usize) -> Option<Vec<RuntimeValue>> {
        (index < self.row_count).then(|| {
            self.columns
                .iter()
                .map(|column| column.value_at(index))
                .collect::<Option<Vec<_>>>()
        })?
    }

    pub fn to_record_batch(&self) -> Result<RecordBatch, RuntimeError> {
        let rows = (0..self.row_count)
            .map(|index| self.row(index).ok_or(RuntimeError::InvalidBatchRows(index)))
            .collect::<Result<Vec<_>, _>>()?;
        RecordBatch::try_new(self.schema.clone(), rows)
    }
}
