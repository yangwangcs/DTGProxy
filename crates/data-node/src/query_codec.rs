use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};

use storage_api::{
    CandidateScanPage, CandidateScanRequest, ComparisonOperator, KeySpan, KeyValue, Keyspace,
    LogicalKey, MAX_QUERY_PAGE_ITEMS, PropertyConstraint, PropertyId, PushdownGuarantee,
    QueryPageBounds,
};
use temporal_types::{CanonicalElement, ValidTime};

const PLAN_MAGIC: &[u8; 4] = b"DTCQ";
const BATCH_MAGIC: &[u8; 4] = b"DTCB";
const VERSION: u16 = 1;
const MAX_PLAN_BYTES: usize = 2 * 1024 * 1024;
const MIN_BATCH_BYTES: usize = 1024;
const MAX_BATCH_BYTES: usize = 4 * 1024 * 1024;

pub fn is_candidate_scan_plan(encoded: &[u8]) -> bool {
    encoded.starts_with(PLAN_MAGIC)
}

pub fn encode_candidate_scan_plan(
    request: &CandidateScanRequest,
) -> Result<Vec<u8>, CandidateCodecError> {
    let mut encoded = Vec::new();
    encoded.extend_from_slice(PLAN_MAGIC);
    encoded.extend_from_slice(&VERSION.to_be_bytes());
    encode_span(request.span(), &mut encoded)?;
    encoded.extend_from_slice(&request.valid_time().as_micros().to_be_bytes());
    encoded.extend_from_slice(
        &u32::try_from(request.bounds().max_items())
            .map_err(|_| CandidateCodecError::Limit)?
            .to_be_bytes(),
    );
    encoded.extend_from_slice(&request.bounds().max_bytes().to_be_bytes());
    encoded.extend_from_slice(
        &u16::try_from(request.constraints().len())
            .map_err(|_| CandidateCodecError::Limit)?
            .to_be_bytes(),
    );
    for constraint in request.constraints() {
        encoded.extend_from_slice(&constraint.property().value().to_be_bytes());
        encoded.push(comparison_tag(constraint.operator()));
        let value = CanonicalElement::new(
            1,
            BTreeMap::from([(constraint.property().value(), constraint.value().clone())]),
        )
        .encode()
        .map_err(|_| CandidateCodecError::Value)?;
        put_bytes(&mut encoded, &value)?;
    }
    if encoded.len().saturating_add(4) > MAX_PLAN_BYTES {
        return Err(CandidateCodecError::Limit);
    }
    append_checksum(&mut encoded);
    Ok(encoded)
}

pub fn decode_candidate_scan_plan(
    encoded: &[u8],
) -> Result<CandidateScanRequest, CandidateCodecError> {
    let payload = checked_payload(encoded, PLAN_MAGIC, MAX_PLAN_BYTES)?;
    let mut reader = Reader::new(payload);
    reader.expect(PLAN_MAGIC)?;
    reader.expect_version()?;
    let span = decode_span(&mut reader)?;
    let valid_time = ValidTime::from_micros(reader.i64()?);
    let bounds = QueryPageBounds::new(
        usize::try_from(reader.u32()?).map_err(|_| CandidateCodecError::Limit)?,
        reader.u64()?,
    )
    .map_err(|_| CandidateCodecError::Limit)?;
    let count = usize::from(reader.u16()?);
    let mut constraints = Vec::with_capacity(count);
    for _ in 0..count {
        let property = PropertyId::new(reader.u32()?);
        let operator = decode_comparison(reader.u8()?)?;
        let value =
            CanonicalElement::decode(reader.bytes()?).map_err(|_| CandidateCodecError::Value)?;
        let value = value
            .property(property.value())
            .cloned()
            .ok_or(CandidateCodecError::Value)?;
        constraints.push(PropertyConstraint::new(property, operator, value));
    }
    reader.finish()?;
    CandidateScanRequest::new(span, valid_time, constraints, bounds)
        .map_err(|_| CandidateCodecError::Invalid)
}

pub fn encode_candidate_scan_batches(
    page: CandidateScanPage,
    maximum_batch_bytes: usize,
) -> Result<Vec<Vec<u8>>, CandidateCodecError> {
    if !(MIN_BATCH_BYTES..=MAX_BATCH_BYTES).contains(&maximum_batch_bytes) {
        return Err(CandidateCodecError::Limit);
    }
    let guarantee = page.guarantee();
    let next_start = page.next_start().cloned();
    let mut batches = Vec::new();
    let mut current = Vec::new();
    let mut current_bytes = 32_usize;
    for entry in page.into_entries() {
        let bytes = 9_usize
            .checked_add(entry.key().as_bytes().len())
            .and_then(|value| value.checked_add(entry.value().len()))
            .ok_or(CandidateCodecError::Limit)?;
        if bytes.saturating_add(32) > maximum_batch_bytes {
            return Err(CandidateCodecError::Limit);
        }
        if !current.is_empty() && current_bytes.saturating_add(bytes) > maximum_batch_bytes {
            batches.push(std::mem::take(&mut current));
            current_bytes = 32;
        }
        current_bytes = current_bytes.saturating_add(bytes);
        current.push(entry);
    }
    batches.push(current);
    let terminal = batches.len().saturating_sub(1);
    batches
        .into_iter()
        .enumerate()
        .map(|(index, rows)| {
            encode_candidate_scan_batch(
                guarantee,
                &rows,
                (index == terminal).then_some(next_start.as_ref()).flatten(),
                maximum_batch_bytes,
            )
        })
        .collect()
}

pub fn decode_candidate_scan_batch(
    encoded: &[u8],
) -> Result<(PushdownGuarantee, Vec<KeyValue>, Option<LogicalKey>), CandidateCodecError> {
    let payload = checked_payload(encoded, BATCH_MAGIC, MAX_BATCH_BYTES)?;
    let mut reader = Reader::new(payload);
    reader.expect(BATCH_MAGIC)?;
    reader.expect_version()?;
    let guarantee = decode_guarantee(reader.u8()?)?;
    let next_start = match reader.u8()? {
        0 => None,
        1 => Some(decode_key(&mut reader)?),
        _ => return Err(CandidateCodecError::Invalid),
    };
    let count = usize::try_from(reader.u32()?).map_err(|_| CandidateCodecError::Limit)?;
    if count > MAX_QUERY_PAGE_ITEMS || count > reader.remaining() / 9 {
        return Err(CandidateCodecError::Limit);
    }
    let mut rows = Vec::with_capacity(count);
    for _ in 0..count {
        let key = decode_key(&mut reader)?;
        let value = reader.bytes()?.to_vec();
        rows.push(KeyValue::new(key, value));
    }
    reader.finish()?;
    Ok((guarantee, rows, next_start))
}

fn encode_candidate_scan_batch(
    guarantee: PushdownGuarantee,
    rows: &[KeyValue],
    next_start: Option<&LogicalKey>,
    maximum_batch_bytes: usize,
) -> Result<Vec<u8>, CandidateCodecError> {
    let mut encoded = Vec::new();
    encoded.extend_from_slice(BATCH_MAGIC);
    encoded.extend_from_slice(&VERSION.to_be_bytes());
    encoded.push(guarantee_tag(guarantee)?);
    encoded.push(u8::from(next_start.is_some()));
    if let Some(next_start) = next_start {
        encode_key(next_start, &mut encoded)?;
    }
    encoded.extend_from_slice(
        &u32::try_from(rows.len())
            .map_err(|_| CandidateCodecError::Limit)?
            .to_be_bytes(),
    );
    for row in rows {
        encode_key(row.key(), &mut encoded)?;
        put_bytes(&mut encoded, row.value())?;
    }
    if encoded.len().saturating_add(4) > maximum_batch_bytes {
        return Err(CandidateCodecError::Limit);
    }
    append_checksum(&mut encoded);
    Ok(encoded)
}

fn encode_span(span: &KeySpan, encoded: &mut Vec<u8>) -> Result<(), CandidateCodecError> {
    if span.limit().is_some() || span.max_bytes().is_some() {
        return Err(CandidateCodecError::Invalid);
    }
    encoded.push(span.keyspace().tag());
    put_bytes(encoded, span.start())?;
    put_optional_bytes(encoded, span.end())?;
    put_optional_bytes(encoded, span.required_prefix())
}

fn decode_span(reader: &mut Reader<'_>) -> Result<KeySpan, CandidateCodecError> {
    let keyspace = decode_keyspace(reader.u8()?)?;
    let start = reader.bytes()?.to_vec();
    let end = reader.optional_bytes()?.map(ToOwned::to_owned);
    let prefix = reader.optional_bytes()?.map(ToOwned::to_owned);
    match prefix {
        Some(prefix) => {
            let span = KeySpan::prefix_from(keyspace, prefix, start)
                .map_err(|_| CandidateCodecError::Invalid)?;
            if span.end() != end.as_deref() {
                return Err(CandidateCodecError::Invalid);
            }
            Ok(span)
        }
        None => KeySpan::range(keyspace, start, end).map_err(|_| CandidateCodecError::Invalid),
    }
}

fn encode_key(key: &LogicalKey, encoded: &mut Vec<u8>) -> Result<(), CandidateCodecError> {
    encoded.push(key.keyspace().tag());
    put_bytes(encoded, key.as_bytes())
}

fn decode_key(reader: &mut Reader<'_>) -> Result<LogicalKey, CandidateCodecError> {
    let keyspace = decode_keyspace(reader.u8()?)?;
    Ok(LogicalKey::in_keyspace(keyspace, reader.bytes()?.to_vec()))
}

fn put_bytes(encoded: &mut Vec<u8>, bytes: &[u8]) -> Result<(), CandidateCodecError> {
    encoded.extend_from_slice(
        &u32::try_from(bytes.len())
            .map_err(|_| CandidateCodecError::Limit)?
            .to_be_bytes(),
    );
    encoded.extend_from_slice(bytes);
    Ok(())
}

fn put_optional_bytes(
    encoded: &mut Vec<u8>,
    bytes: Option<&[u8]>,
) -> Result<(), CandidateCodecError> {
    encoded.push(u8::from(bytes.is_some()));
    if let Some(bytes) = bytes {
        put_bytes(encoded, bytes)?;
    }
    Ok(())
}

fn comparison_tag(operator: ComparisonOperator) -> u8 {
    match operator {
        ComparisonOperator::Equal => 0,
        ComparisonOperator::NotEqual => 1,
        ComparisonOperator::LessThan => 2,
        ComparisonOperator::LessThanOrEqual => 3,
        ComparisonOperator::GreaterThan => 4,
        ComparisonOperator::GreaterThanOrEqual => 5,
    }
}

fn decode_comparison(tag: u8) -> Result<ComparisonOperator, CandidateCodecError> {
    match tag {
        0 => Ok(ComparisonOperator::Equal),
        1 => Ok(ComparisonOperator::NotEqual),
        2 => Ok(ComparisonOperator::LessThan),
        3 => Ok(ComparisonOperator::LessThanOrEqual),
        4 => Ok(ComparisonOperator::GreaterThan),
        5 => Ok(ComparisonOperator::GreaterThanOrEqual),
        _ => Err(CandidateCodecError::Invalid),
    }
}

fn guarantee_tag(guarantee: PushdownGuarantee) -> Result<u8, CandidateCodecError> {
    match guarantee {
        PushdownGuarantee::Candidate => Ok(1),
        PushdownGuarantee::Exact => Ok(2),
        PushdownGuarantee::Unsupported => Err(CandidateCodecError::Invalid),
    }
}

fn decode_guarantee(tag: u8) -> Result<PushdownGuarantee, CandidateCodecError> {
    match tag {
        1 => Ok(PushdownGuarantee::Candidate),
        2 => Ok(PushdownGuarantee::Exact),
        _ => Err(CandidateCodecError::Invalid),
    }
}

fn decode_keyspace(tag: u8) -> Result<Keyspace, CandidateCodecError> {
    Keyspace::ALL
        .into_iter()
        .find(|keyspace| keyspace.tag() == tag)
        .ok_or(CandidateCodecError::Invalid)
}

fn checked_payload<'a>(
    encoded: &'a [u8],
    magic: &[u8; 4],
    maximum: usize,
) -> Result<&'a [u8], CandidateCodecError> {
    if encoded.len() < 12 || encoded.len() > maximum || !encoded.starts_with(magic) {
        return Err(CandidateCodecError::Invalid);
    }
    let split = encoded.len() - 4;
    let checksum = u32::from_be_bytes(
        encoded[split..]
            .try_into()
            .map_err(|_| CandidateCodecError::Invalid)?,
    );
    if crc32fast::hash(&encoded[..split]) != checksum {
        return Err(CandidateCodecError::Checksum);
    }
    Ok(&encoded[..split])
}

fn append_checksum(encoded: &mut Vec<u8>) {
    encoded.extend_from_slice(&crc32fast::hash(encoded).to_be_bytes());
}

struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], CandidateCodecError> {
        let end = self
            .offset
            .checked_add(length)
            .filter(|end| *end <= self.bytes.len())
            .ok_or(CandidateCodecError::Invalid)?;
        let value = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(value)
    }

    fn expect(&mut self, expected: &[u8]) -> Result<(), CandidateCodecError> {
        (self.take(expected.len())? == expected)
            .then_some(())
            .ok_or(CandidateCodecError::Invalid)
    }

    fn expect_version(&mut self) -> Result<(), CandidateCodecError> {
        (self.u16()? == VERSION)
            .then_some(())
            .ok_or(CandidateCodecError::Version)
    }

    fn u8(&mut self) -> Result<u8, CandidateCodecError> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, CandidateCodecError> {
        Ok(u16::from_be_bytes(
            self.take(2)?
                .try_into()
                .map_err(|_| CandidateCodecError::Invalid)?,
        ))
    }

    fn u32(&mut self) -> Result<u32, CandidateCodecError> {
        Ok(u32::from_be_bytes(
            self.take(4)?
                .try_into()
                .map_err(|_| CandidateCodecError::Invalid)?,
        ))
    }

    fn u64(&mut self) -> Result<u64, CandidateCodecError> {
        Ok(u64::from_be_bytes(
            self.take(8)?
                .try_into()
                .map_err(|_| CandidateCodecError::Invalid)?,
        ))
    }

    fn i64(&mut self) -> Result<i64, CandidateCodecError> {
        Ok(i64::from_be_bytes(
            self.take(8)?
                .try_into()
                .map_err(|_| CandidateCodecError::Invalid)?,
        ))
    }

    fn bytes(&mut self) -> Result<&'a [u8], CandidateCodecError> {
        let length = usize::try_from(self.u32()?).map_err(|_| CandidateCodecError::Limit)?;
        self.take(length)
    }

    fn optional_bytes(&mut self) -> Result<Option<&'a [u8]>, CandidateCodecError> {
        match self.u8()? {
            0 => Ok(None),
            1 => self.bytes().map(Some),
            _ => Err(CandidateCodecError::Invalid),
        }
    }

    fn finish(self) -> Result<(), CandidateCodecError> {
        (self.offset == self.bytes.len())
            .then_some(())
            .ok_or(CandidateCodecError::Invalid)
    }

    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.offset)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CandidateCodecError {
    Invalid,
    Version,
    Checksum,
    Limit,
    Value,
}

impl Display for CandidateCodecError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid typed candidate scan payload: {self:?}")
    }
}

impl Error for CandidateCodecError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn candidate_batch_rejects_impossible_entry_count_before_allocation() {
        let mut encoded = Vec::new();
        encoded.extend_from_slice(BATCH_MAGIC);
        encoded.extend_from_slice(&VERSION.to_be_bytes());
        encoded.push(guarantee_tag(PushdownGuarantee::Candidate).unwrap());
        encoded.push(0);
        encoded.extend_from_slice(&u32::MAX.to_be_bytes());
        append_checksum(&mut encoded);

        assert_eq!(
            decode_candidate_scan_batch(&encoded),
            Err(CandidateCodecError::Limit)
        );
    }
}
