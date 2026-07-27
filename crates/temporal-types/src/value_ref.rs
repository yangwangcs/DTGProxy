use std::collections::{BTreeMap, BTreeSet};

use crate::value::{MAGIC, decode_value, encoded_value_len};
use crate::{CanonicalElement, CodecError, GraphValue};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CanonicalElementRef<'a> {
    encoded: &'a [u8],
    schema_version: u64,
    properties_offset: usize,
    property_count: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CanonicalPropertyRef<'a> {
    encoded_value: &'a [u8],
}

impl<'a> CanonicalElementRef<'a> {
    pub fn parse(encoded: &'a [u8]) -> Result<Self, CodecError> {
        let mut position = 0;
        if take(encoded, &mut position, MAGIC.len())? != MAGIC {
            return Err(CodecError::InvalidMagic);
        }

        let schema_version = read_u64(encoded, &mut position)?;
        let property_count = read_u32(encoded, &mut position)?;
        ensure_minimum_properties(encoded, position, property_count)?;
        let properties_offset = position;

        for _ in 0..property_count {
            let property_offset = position;
            let property_id = read_u32(encoded, &mut position)?;
            let value_len =
                encoded_value_len(encoded.get(position..).ok_or(CodecError::UnexpectedEnd)?)?;
            let value_end = position
                .checked_add(value_len)
                .ok_or(CodecError::UnexpectedEnd)?;
            encoded
                .get(position..value_end)
                .ok_or(CodecError::UnexpectedEnd)?;

            if contains_property_id(&encoded[properties_offset..property_offset], property_id)? {
                return Err(CodecError::DuplicateProperty(property_id));
            }
            position = value_end;
        }

        if position != encoded.len() {
            return Err(CodecError::TrailingBytes);
        }

        Ok(Self {
            encoded,
            schema_version,
            properties_offset,
            property_count,
        })
    }

    #[must_use]
    pub const fn schema_version(self) -> u64 {
        self.schema_version
    }

    #[must_use]
    pub const fn encoded(self) -> &'a [u8] {
        self.encoded
    }

    pub fn property(
        self,
        property_id: u32,
    ) -> Result<Option<CanonicalPropertyRef<'a>>, CodecError> {
        let mut position = self.properties_offset;
        for _ in 0..self.property_count {
            let current_property_id = read_u32(self.encoded, &mut position)?;
            let value_start = position;
            let value_len = encoded_value_len(
                self.encoded
                    .get(value_start..)
                    .ok_or(CodecError::UnexpectedEnd)?,
            )?;
            let value_end = value_start
                .checked_add(value_len)
                .ok_or(CodecError::UnexpectedEnd)?;
            let encoded_value = self
                .encoded
                .get(value_start..value_end)
                .ok_or(CodecError::UnexpectedEnd)?;
            position = value_end;

            if current_property_id == property_id {
                return Ok(Some(CanonicalPropertyRef { encoded_value }));
            }
        }
        Ok(None)
    }

    pub fn project(self, demanded: &[u32]) -> Result<CanonicalElement, CodecError> {
        let demanded: BTreeSet<_> = demanded.iter().copied().collect();
        let mut properties = BTreeMap::new();

        let mut position = self.properties_offset;
        for _ in 0..self.property_count {
            let property_id = read_u32(self.encoded, &mut position)?;
            let value_start = position;
            let value_len = encoded_value_len(
                self.encoded
                    .get(value_start..)
                    .ok_or(CodecError::UnexpectedEnd)?,
            )?;
            let value_end = value_start
                .checked_add(value_len)
                .ok_or(CodecError::UnexpectedEnd)?;
            let encoded_value = self
                .encoded
                .get(value_start..value_end)
                .ok_or(CodecError::UnexpectedEnd)?;
            position = value_end;

            if demanded.contains(&property_id) {
                properties.insert(property_id, decode_value(encoded_value)?);
            }
        }

        Ok(CanonicalElement::new(self.schema_version, properties))
    }
}

impl CanonicalPropertyRef<'_> {
    pub fn decode(self) -> Result<GraphValue, CodecError> {
        decode_value(self.encoded_value)
    }
}

fn ensure_minimum_properties(
    encoded: &[u8],
    position: usize,
    property_count: u32,
) -> Result<(), CodecError> {
    let remaining = encoded
        .len()
        .checked_sub(position)
        .ok_or(CodecError::UnexpectedEnd)?;
    let minimum = (property_count as usize)
        .checked_mul(5)
        .ok_or(CodecError::InvalidCollectionLength)?;
    if minimum > remaining {
        return Err(CodecError::InvalidCollectionLength);
    }
    Ok(())
}

fn contains_property_id(properties: &[u8], sought_property_id: u32) -> Result<bool, CodecError> {
    let mut position = 0;
    while position < properties.len() {
        let property_id = read_u32(properties, &mut position)?;
        let value_len = encoded_value_len(
            properties
                .get(position..)
                .ok_or(CodecError::UnexpectedEnd)?,
        )?;
        let value_end = position
            .checked_add(value_len)
            .ok_or(CodecError::UnexpectedEnd)?;
        properties
            .get(position..value_end)
            .ok_or(CodecError::UnexpectedEnd)?;
        position = value_end;

        if property_id == sought_property_id {
            return Ok(true);
        }
    }
    Ok(false)
}

fn take<'a>(encoded: &'a [u8], position: &mut usize, len: usize) -> Result<&'a [u8], CodecError> {
    let end = position.checked_add(len).ok_or(CodecError::UnexpectedEnd)?;
    let value = encoded
        .get(*position..end)
        .ok_or(CodecError::UnexpectedEnd)?;
    *position = end;
    Ok(value)
}

fn read_u32(encoded: &[u8], position: &mut usize) -> Result<u32, CodecError> {
    let bytes: [u8; 4] = take(encoded, position, 4)?
        .try_into()
        .map_err(|_| CodecError::UnexpectedEnd)?;
    Ok(u32::from_be_bytes(bytes))
}

fn read_u64(encoded: &[u8], position: &mut usize) -> Result<u64, CodecError> {
    let bytes: [u8; 8] = take(encoded, position, 8)?
        .try_into()
        .map_err(|_| CodecError::UnexpectedEnd)?;
    Ok(u64::from_be_bytes(bytes))
}
