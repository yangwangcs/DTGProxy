use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};

const MAGIC: &[u8; 4] = b"DTP1";
const MAX_NESTING: u8 = 64;

const TAG_NULL: u8 = 0;
const TAG_FALSE: u8 = 1;
const TAG_TRUE: u8 = 2;
const TAG_INTEGER: u8 = 3;
const TAG_FLOAT_BITS: u8 = 4;
const TAG_STRING: u8 = 5;
const TAG_BYTES: u8 = 6;
const TAG_TIMESTAMP_MICROS: u8 = 7;
const TAG_LIST: u8 = 8;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GraphValue {
    Null,
    Boolean(bool),
    Integer(i64),
    FloatBits(u64),
    String(String),
    Bytes(Vec<u8>),
    TimestampMicros(i64),
    List(Vec<Self>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CanonicalElement {
    schema_version: u64,
    properties: BTreeMap<u32, GraphValue>,
}

impl CanonicalElement {
    #[must_use]
    pub const fn new(schema_version: u64, properties: BTreeMap<u32, GraphValue>) -> Self {
        Self {
            schema_version,
            properties,
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>, CodecError> {
        let mut output = Vec::new();
        output.extend_from_slice(MAGIC);
        output.extend_from_slice(&self.schema_version.to_be_bytes());
        write_len(&mut output, self.properties.len())?;

        for (property_id, value) in &self.properties {
            output.extend_from_slice(&property_id.to_be_bytes());
            encode_value(value, 0, &mut output)?;
        }

        Ok(output)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        let mut decoder = Decoder::new(bytes);
        if decoder.take(4)? != MAGIC {
            return Err(CodecError::InvalidMagic);
        }

        let schema_version = decoder.read_u64()?;
        let property_count = decoder.read_u32()? as usize;
        decoder.ensure_minimum_remaining(property_count, 5)?;

        let mut properties = BTreeMap::new();
        for _ in 0..property_count {
            let property_id = decoder.read_u32()?;
            let value = decoder.decode_value(0)?;
            if properties.insert(property_id, value).is_some() {
                return Err(CodecError::DuplicateProperty(property_id));
            }
        }

        if !decoder.is_finished() {
            return Err(CodecError::TrailingBytes);
        }

        Ok(Self {
            schema_version,
            properties,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CodecError {
    InvalidMagic,
    UnexpectedEnd,
    InvalidUtf8,
    UnknownTag(u8),
    TrailingBytes,
    NestingLimitExceeded,
    LengthOverflow,
    InvalidCollectionLength,
    DuplicateProperty(u32),
}

impl Display for CodecError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidMagic => formatter.write_str("invalid canonical payload magic"),
            Self::UnexpectedEnd => formatter.write_str("canonical payload ended unexpectedly"),
            Self::InvalidUtf8 => formatter.write_str("canonical string is not valid UTF-8"),
            Self::UnknownTag(tag) => write!(formatter, "unknown canonical value tag {tag}"),
            Self::TrailingBytes => formatter.write_str("canonical payload has trailing bytes"),
            Self::NestingLimitExceeded => {
                formatter.write_str("canonical value nesting limit exceeded")
            }
            Self::LengthOverflow => formatter.write_str("canonical collection length exceeds u32"),
            Self::InvalidCollectionLength => {
                formatter.write_str("canonical collection length exceeds remaining payload")
            }
            Self::DuplicateProperty(property_id) => {
                write!(formatter, "duplicate canonical property id {property_id}")
            }
        }
    }
}

impl Error for CodecError {}

fn write_len(output: &mut Vec<u8>, len: usize) -> Result<(), CodecError> {
    let len = u32::try_from(len).map_err(|_| CodecError::LengthOverflow)?;
    output.extend_from_slice(&len.to_be_bytes());
    Ok(())
}

fn encode_value(
    value: &GraphValue,
    depth: u8,
    output: &mut Vec<u8>,
) -> Result<(), CodecError> {
    if depth > MAX_NESTING {
        return Err(CodecError::NestingLimitExceeded);
    }

    match value {
        GraphValue::Null => output.push(TAG_NULL),
        GraphValue::Boolean(false) => output.push(TAG_FALSE),
        GraphValue::Boolean(true) => output.push(TAG_TRUE),
        GraphValue::Integer(value) => {
            output.push(TAG_INTEGER);
            output.extend_from_slice(&value.to_be_bytes());
        }
        GraphValue::FloatBits(value) => {
            output.push(TAG_FLOAT_BITS);
            output.extend_from_slice(&value.to_be_bytes());
        }
        GraphValue::String(value) => {
            output.push(TAG_STRING);
            write_len(output, value.len())?;
            output.extend_from_slice(value.as_bytes());
        }
        GraphValue::Bytes(value) => {
            output.push(TAG_BYTES);
            write_len(output, value.len())?;
            output.extend_from_slice(value);
        }
        GraphValue::TimestampMicros(value) => {
            output.push(TAG_TIMESTAMP_MICROS);
            output.extend_from_slice(&value.to_be_bytes());
        }
        GraphValue::List(values) => {
            output.push(TAG_LIST);
            write_len(output, values.len())?;
            for value in values {
                encode_value(value, depth.saturating_add(1), output)?;
            }
        }
    }
    Ok(())
}

struct Decoder<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Decoder<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn is_finished(&self) -> bool {
        self.position == self.bytes.len()
    }

    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.position)
    }

    fn ensure_minimum_remaining(
        &self,
        count: usize,
        bytes_per_item: usize,
    ) -> Result<(), CodecError> {
        let minimum = count
            .checked_mul(bytes_per_item)
            .ok_or(CodecError::InvalidCollectionLength)?;
        if minimum > self.remaining() {
            return Err(CodecError::InvalidCollectionLength);
        }
        Ok(())
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], CodecError> {
        let end = self
            .position
            .checked_add(len)
            .ok_or(CodecError::UnexpectedEnd)?;
        let value = self
            .bytes
            .get(self.position..end)
            .ok_or(CodecError::UnexpectedEnd)?;
        self.position = end;
        Ok(value)
    }

    fn read_u8(&mut self) -> Result<u8, CodecError> {
        Ok(self.take(1)?[0])
    }

    fn read_u32(&mut self) -> Result<u32, CodecError> {
        let bytes: [u8; 4] = self
            .take(4)?
            .try_into()
            .map_err(|_| CodecError::UnexpectedEnd)?;
        Ok(u32::from_be_bytes(bytes))
    }

    fn read_u64(&mut self) -> Result<u64, CodecError> {
        let bytes: [u8; 8] = self
            .take(8)?
            .try_into()
            .map_err(|_| CodecError::UnexpectedEnd)?;
        Ok(u64::from_be_bytes(bytes))
    }

    fn read_i64(&mut self) -> Result<i64, CodecError> {
        let bytes: [u8; 8] = self
            .take(8)?
            .try_into()
            .map_err(|_| CodecError::UnexpectedEnd)?;
        Ok(i64::from_be_bytes(bytes))
    }

    fn decode_value(&mut self, depth: u8) -> Result<GraphValue, CodecError> {
        if depth > MAX_NESTING {
            return Err(CodecError::NestingLimitExceeded);
        }

        match self.read_u8()? {
            TAG_NULL => Ok(GraphValue::Null),
            TAG_FALSE => Ok(GraphValue::Boolean(false)),
            TAG_TRUE => Ok(GraphValue::Boolean(true)),
            TAG_INTEGER => Ok(GraphValue::Integer(self.read_i64()?)),
            TAG_FLOAT_BITS => Ok(GraphValue::FloatBits(self.read_u64()?)),
            TAG_STRING => {
                let len = self.read_u32()? as usize;
                let value = std::str::from_utf8(self.take(len)?)
                    .map_err(|_| CodecError::InvalidUtf8)?
                    .to_owned();
                Ok(GraphValue::String(value))
            }
            TAG_BYTES => {
                let len = self.read_u32()? as usize;
                Ok(GraphValue::Bytes(self.take(len)?.to_vec()))
            }
            TAG_TIMESTAMP_MICROS => Ok(GraphValue::TimestampMicros(self.read_i64()?)),
            TAG_LIST => {
                let len = self.read_u32()? as usize;
                self.ensure_minimum_remaining(len, 1)?;
                let mut values = Vec::with_capacity(len);
                for _ in 0..len {
                    values.push(self.decode_value(depth.saturating_add(1))?);
                }
                Ok(GraphValue::List(values))
            }
            tag => Err(CodecError::UnknownTag(tag)),
        }
    }
}

