use std::collections::BTreeMap;

use crate::ProtocolError;

const DEFAULT_MAX_BYTES: usize = 4 * 1024 * 1024;
const DEFAULT_MAX_ITEMS: usize = 1_000_000;
const DEFAULT_MAX_DEPTH: usize = 64;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PackStreamLimits {
    max_bytes: usize,
    max_collection_items: usize,
    max_depth: usize,
}

impl PackStreamLimits {
    pub fn new(
        max_bytes: usize,
        max_collection_items: usize,
        max_depth: usize,
    ) -> Result<Self, ProtocolError> {
        if max_bytes == 0 || max_collection_items == 0 || max_depth == 0 {
            return Err(ProtocolError::new(
                "DTG-BOLT-INVALID-LIMIT",
                0,
                "PackStream limits must be positive",
            ));
        }
        Ok(Self {
            max_bytes,
            max_collection_items,
            max_depth,
        })
    }
}

impl Default for PackStreamLimits {
    fn default() -> Self {
        Self {
            max_bytes: DEFAULT_MAX_BYTES,
            max_collection_items: DEFAULT_MAX_ITEMS,
            max_depth: DEFAULT_MAX_DEPTH,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Value {
    Null,
    Boolean(bool),
    Integer(i64),
    FloatBits(u64),
    Bytes(Vec<u8>),
    String(String),
    List(Vec<Value>),
    Map(BTreeMap<String, Value>),
    Structure { signature: u8, fields: Vec<Value> },
}

pub fn encode(value: &Value) -> Result<Vec<u8>, ProtocolError> {
    let limits = PackStreamLimits::default();
    let mut output = Vec::new();
    encode_value(value, 0, limits, &mut output)?;
    if output.len() > limits.max_bytes {
        return Err(ProtocolError::new(
            "DTG-BOLT-BYTE-LIMIT",
            output.len(),
            "encoded PackStream value exceeds the byte limit",
        ));
    }
    Ok(output)
}

pub fn decode(bytes: &[u8]) -> Result<Value, ProtocolError> {
    decode_with_limits(bytes, PackStreamLimits::default())
}

pub fn decode_with_limits(bytes: &[u8], limits: PackStreamLimits) -> Result<Value, ProtocolError> {
    if bytes.len() > limits.max_bytes {
        return Err(ProtocolError::new(
            "DTG-BOLT-BYTE-LIMIT",
            bytes.len(),
            "PackStream payload exceeds the byte limit",
        ));
    }
    let mut decoder = Decoder {
        bytes,
        position: 0,
        limits,
        items: 0,
    };
    let value = decoder.value(0)?;
    if decoder.position != bytes.len() {
        return Err(ProtocolError::new(
            "DTG-BOLT-TRAILING-BYTES",
            decoder.position,
            "PackStream value has trailing bytes",
        ));
    }
    Ok(value)
}

fn encode_value(
    value: &Value,
    depth: usize,
    limits: PackStreamLimits,
    output: &mut Vec<u8>,
) -> Result<(), ProtocolError> {
    if depth > limits.max_depth {
        return Err(ProtocolError::new(
            "DTG-BOLT-NESTING-LIMIT",
            output.len(),
            "PackStream nesting exceeds the configured limit",
        ));
    }
    match value {
        Value::Null => output.push(0xC0),
        Value::Boolean(false) => output.push(0xC2),
        Value::Boolean(true) => output.push(0xC3),
        Value::Integer(value) => encode_integer(*value, output),
        Value::FloatBits(bits) => {
            output.push(0xC1);
            output.extend_from_slice(&bits.to_be_bytes());
        }
        Value::Bytes(bytes) => {
            write_length(bytes.len(), 0xCC, 0xCD, 0xCE, output)?;
            output.extend_from_slice(bytes);
        }
        Value::String(value) => {
            write_tiny_or_length(value.len(), 0x80, 0xD0, 0xD1, 0xD2, output)?;
            output.extend_from_slice(value.as_bytes());
        }
        Value::List(values) => {
            check_items(values.len(), limits, output.len())?;
            write_tiny_or_length(values.len(), 0x90, 0xD4, 0xD5, 0xD6, output)?;
            for value in values {
                encode_value(value, depth + 1, limits, output)?;
            }
        }
        Value::Map(values) => {
            check_items(values.len(), limits, output.len())?;
            write_tiny_or_length(values.len(), 0xA0, 0xD8, 0xD9, 0xDA, output)?;
            for (key, value) in values {
                encode_value(&Value::String(key.clone()), depth + 1, limits, output)?;
                encode_value(value, depth + 1, limits, output)?;
            }
        }
        Value::Structure { signature, fields } => {
            if fields.len() > 15 {
                return Err(ProtocolError::new(
                    "DTG-BOLT-STRUCTURE-TOO-LARGE",
                    output.len(),
                    "PackStream structures support at most 15 fields",
                ));
            }
            output.push(0xB0 | u8::try_from(fields.len()).expect("structure length fits u8"));
            output.push(*signature);
            for field in fields {
                encode_value(field, depth + 1, limits, output)?;
            }
        }
    }
    if output.len() > limits.max_bytes {
        return Err(ProtocolError::new(
            "DTG-BOLT-BYTE-LIMIT",
            output.len(),
            "encoded PackStream value exceeds the byte limit",
        ));
    }
    Ok(())
}

fn encode_integer(value: i64, output: &mut Vec<u8>) {
    if (-16..=127).contains(&value) {
        output.push(value as i8 as u8);
    } else if let Ok(value) = i8::try_from(value) {
        output.extend_from_slice(&[0xC8, value as u8]);
    } else if let Ok(value) = i16::try_from(value) {
        output.push(0xC9);
        output.extend_from_slice(&value.to_be_bytes());
    } else if let Ok(value) = i32::try_from(value) {
        output.push(0xCA);
        output.extend_from_slice(&value.to_be_bytes());
    } else {
        output.push(0xCB);
        output.extend_from_slice(&value.to_be_bytes());
    }
}

fn write_tiny_or_length(
    length: usize,
    tiny: u8,
    marker8: u8,
    marker16: u8,
    marker32: u8,
    output: &mut Vec<u8>,
) -> Result<(), ProtocolError> {
    if length <= 15 {
        output.push(tiny | u8::try_from(length).expect("tiny length fits u8"));
        Ok(())
    } else {
        write_length(length, marker8, marker16, marker32, output)
    }
}

fn write_length(
    length: usize,
    marker8: u8,
    marker16: u8,
    marker32: u8,
    output: &mut Vec<u8>,
) -> Result<(), ProtocolError> {
    if let Ok(length) = u8::try_from(length) {
        output.extend_from_slice(&[marker8, length]);
    } else if let Ok(length) = u16::try_from(length) {
        output.push(marker16);
        output.extend_from_slice(&length.to_be_bytes());
    } else {
        let length = u32::try_from(length).map_err(|_| {
            ProtocolError::new(
                "DTG-BOLT-LENGTH-OVERFLOW",
                output.len(),
                "PackStream collection length exceeds u32",
            )
        })?;
        output.push(marker32);
        output.extend_from_slice(&length.to_be_bytes());
    }
    Ok(())
}

fn check_items(count: usize, limits: PackStreamLimits, offset: usize) -> Result<(), ProtocolError> {
    if count > limits.max_collection_items {
        return Err(ProtocolError::new(
            "DTG-BOLT-COLLECTION-LIMIT",
            offset,
            "PackStream collection exceeds the item limit",
        ));
    }
    Ok(())
}

struct Decoder<'bytes> {
    bytes: &'bytes [u8],
    position: usize,
    limits: PackStreamLimits,
    items: usize,
}

impl Decoder<'_> {
    fn value(&mut self, depth: usize) -> Result<Value, ProtocolError> {
        if depth > self.limits.max_depth {
            return Err(self.error(
                "DTG-BOLT-NESTING-LIMIT",
                "PackStream nesting exceeds the configured limit",
            ));
        }
        self.items = self.items.checked_add(1).ok_or_else(|| {
            self.error(
                "DTG-BOLT-COLLECTION-LIMIT",
                "PackStream item count overflow",
            )
        })?;
        if self.items > self.limits.max_collection_items {
            return Err(self.error(
                "DTG-BOLT-COLLECTION-LIMIT",
                "PackStream payload exceeds the total item limit",
            ));
        }
        let marker = self.u8()?;
        match marker {
            0x00..=0x7F => Ok(Value::Integer(i64::from(marker))),
            0xF0..=0xFF => Ok(Value::Integer(i64::from(marker as i8))),
            0x80..=0x8F => self.string(usize::from(marker & 0x0F)),
            0x90..=0x9F => self.list(usize::from(marker & 0x0F), depth),
            0xA0..=0xAF => self.map(usize::from(marker & 0x0F), depth),
            0xB0..=0xBF => self.structure(usize::from(marker & 0x0F), depth),
            0xC0 => Ok(Value::Null),
            0xC1 => Ok(Value::FloatBits(self.u64()?)),
            0xC2 => Ok(Value::Boolean(false)),
            0xC3 => Ok(Value::Boolean(true)),
            0xC8 => Ok(Value::Integer(i64::from(self.u8()? as i8))),
            0xC9 => Ok(Value::Integer(i64::from(self.i16()?))),
            0xCA => Ok(Value::Integer(i64::from(self.i32()?))),
            0xCB => Ok(Value::Integer(self.i64()?)),
            0xCC => {
                let length = usize::from(self.u8()?);
                self.bytes(length)
            }
            0xCD => {
                let length = usize::from(self.u16()?);
                self.bytes(length)
            }
            0xCE => {
                let length = usize::try_from(self.u32()?).map_err(|_| {
                    self.error("DTG-BOLT-LENGTH-OVERFLOW", "byte length does not fit usize")
                })?;
                self.bytes(length)
            }
            0xD0 => {
                let length = usize::from(self.u8()?);
                self.string(length)
            }
            0xD1 => {
                let length = usize::from(self.u16()?);
                self.string(length)
            }
            0xD2 => {
                let length = usize::try_from(self.u32()?).map_err(|_| {
                    self.error(
                        "DTG-BOLT-LENGTH-OVERFLOW",
                        "string length does not fit usize",
                    )
                })?;
                self.string(length)
            }
            0xD4 => {
                let length = usize::from(self.u8()?);
                self.list(length, depth)
            }
            0xD5 => {
                let length = usize::from(self.u16()?);
                self.list(length, depth)
            }
            0xD6 => {
                let length = usize::try_from(self.u32()?).map_err(|_| {
                    self.error("DTG-BOLT-LENGTH-OVERFLOW", "list length does not fit usize")
                })?;
                self.list(length, depth)
            }
            0xD8 => {
                let length = usize::from(self.u8()?);
                self.map(length, depth)
            }
            0xD9 => {
                let length = usize::from(self.u16()?);
                self.map(length, depth)
            }
            0xDA => {
                let length = usize::try_from(self.u32()?).map_err(|_| {
                    self.error("DTG-BOLT-LENGTH-OVERFLOW", "map length does not fit usize")
                })?;
                self.map(length, depth)
            }
            _ => Err(self.error(
                "DTG-BOLT-UNKNOWN-MARKER",
                format!("unknown PackStream marker 0x{marker:02x}"),
            )),
        }
    }

    fn string(&mut self, length: usize) -> Result<Value, ProtocolError> {
        let offset = self.position;
        let bytes = self.take(length)?;
        let value = std::str::from_utf8(bytes)
            .map_err(|_| {
                ProtocolError::new("DTG-BOLT-INVALID-UTF8", offset, "string is not valid UTF-8")
            })?
            .to_owned();
        Ok(Value::String(value))
    }

    fn bytes(&mut self, length: usize) -> Result<Value, ProtocolError> {
        Ok(Value::Bytes(self.take(length)?.to_vec()))
    }

    fn list(&mut self, length: usize, depth: usize) -> Result<Value, ProtocolError> {
        self.check_count(length)?;
        let mut values =
            Vec::with_capacity(length.min(self.bytes.len().saturating_sub(self.position)));
        for _ in 0..length {
            values.push(self.value(depth + 1)?);
        }
        Ok(Value::List(values))
    }

    fn map(&mut self, length: usize, depth: usize) -> Result<Value, ProtocolError> {
        self.check_count(length)?;
        let mut values = BTreeMap::new();
        for _ in 0..length {
            let Value::String(key) = self.value(depth + 1)? else {
                return Err(self.error(
                    "DTG-BOLT-NONSTRING-MAP-KEY",
                    "PackStream map keys must be strings",
                ));
            };
            let value = self.value(depth + 1)?;
            if values.insert(key, value).is_some() {
                return Err(self.error(
                    "DTG-BOLT-DUPLICATE-MAP-KEY",
                    "PackStream map key is repeated",
                ));
            }
        }
        Ok(Value::Map(values))
    }

    fn structure(&mut self, length: usize, depth: usize) -> Result<Value, ProtocolError> {
        let signature = self.u8()?;
        let mut fields = Vec::with_capacity(length);
        for _ in 0..length {
            fields.push(self.value(depth + 1)?);
        }
        Ok(Value::Structure { signature, fields })
    }

    fn check_count(&self, count: usize) -> Result<(), ProtocolError> {
        if count > self.limits.max_collection_items {
            return Err(self.error(
                "DTG-BOLT-COLLECTION-LIMIT",
                "declared collection length exceeds the item limit",
            ));
        }
        Ok(())
    }

    fn take(&mut self, length: usize) -> Result<&[u8], ProtocolError> {
        let end = self
            .position
            .checked_add(length)
            .ok_or_else(|| self.error("DTG-BOLT-LENGTH-OVERFLOW", "PackStream length overflows"))?;
        let bytes = self.bytes.get(self.position..end).ok_or_else(|| {
            self.error(
                "DTG-BOLT-UNEXPECTED-END",
                "PackStream payload ended unexpectedly",
            )
        })?;
        self.position = end;
        Ok(bytes)
    }

    fn u8(&mut self) -> Result<u8, ProtocolError> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, ProtocolError> {
        Ok(u16::from_be_bytes(
            self.take(2)?.try_into().expect("two bytes"),
        ))
    }

    fn u32(&mut self) -> Result<u32, ProtocolError> {
        Ok(u32::from_be_bytes(
            self.take(4)?.try_into().expect("four bytes"),
        ))
    }

    fn u64(&mut self) -> Result<u64, ProtocolError> {
        Ok(u64::from_be_bytes(
            self.take(8)?.try_into().expect("eight bytes"),
        ))
    }

    fn i16(&mut self) -> Result<i16, ProtocolError> {
        Ok(i16::from_be_bytes(
            self.take(2)?.try_into().expect("two bytes"),
        ))
    }

    fn i32(&mut self) -> Result<i32, ProtocolError> {
        Ok(i32::from_be_bytes(
            self.take(4)?.try_into().expect("four bytes"),
        ))
    }

    fn i64(&mut self) -> Result<i64, ProtocolError> {
        Ok(i64::from_be_bytes(
            self.take(8)?.try_into().expect("eight bytes"),
        ))
    }

    fn error(&self, code: &'static str, message: impl Into<String>) -> ProtocolError {
        ProtocolError::new(code, self.position, message)
    }
}
