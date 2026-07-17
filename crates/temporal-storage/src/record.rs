use std::error::Error;
use std::fmt::{self, Display, Formatter};

use temporal_types::{CanonicalElement, CodecError, Interval, TransactionTime, ValidTime};

use crate::{EdgeTypeId, ElementId, ElementKind, ElementRef, GraphId, LabelId, PartitionId};

const IDENTITY_MAGIC: &[u8; 4] = b"DTGI";
const PROJECTION_MAGIC: &[u8; 4] = b"DTGP";
const ANCHOR_MAGIC: &[u8; 4] = b"DTGA";
const DELTA_MAGIC: &[u8; 4] = b"DTGD";
const FORMAT_VERSION: u16 = 1;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VertexIdentity {
    element: ElementRef,
    label: LabelId,
}

impl VertexIdentity {
    pub fn new(element: ElementRef, label: LabelId) -> Result<Self, RecordCodecError> {
        if element.kind() != ElementKind::Vertex {
            return Err(RecordCodecError::WrongElementKind);
        }
        Ok(Self { element, label })
    }

    #[must_use]
    pub const fn element(&self) -> ElementRef {
        self.element
    }

    #[must_use]
    pub const fn label(&self) -> LabelId {
        self.label
    }

    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut output = identity_header(ElementKind::Vertex, self.element);
        output.extend_from_slice(&self.label.value().to_be_bytes());
        append_checksum(&mut output);
        output
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, RecordCodecError> {
        let mut decoder = IdentityDecoder::new(bytes, ElementKind::Vertex)?;
        let label = LabelId::new(decoder.decoder.read_u32()?);
        decoder.finish()?;
        Self::new(decoder.element, label)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EdgeIdentity {
    element: ElementRef,
    edge_type: EdgeTypeId,
    source: ElementId,
    destination: ElementId,
}

impl EdgeIdentity {
    pub fn new(
        element: ElementRef,
        edge_type: EdgeTypeId,
        source: ElementId,
        destination: ElementId,
    ) -> Result<Self, RecordCodecError> {
        if element.kind() != ElementKind::Edge {
            return Err(RecordCodecError::WrongElementKind);
        }
        Ok(Self {
            element,
            edge_type,
            source,
            destination,
        })
    }

    #[must_use]
    pub const fn element(&self) -> ElementRef {
        self.element
    }

    #[must_use]
    pub const fn edge_type(&self) -> EdgeTypeId {
        self.edge_type
    }

    #[must_use]
    pub const fn source(&self) -> ElementId {
        self.source
    }

    #[must_use]
    pub const fn destination(&self) -> ElementId {
        self.destination
    }

    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut output = identity_header(ElementKind::Edge, self.element);
        output.extend_from_slice(&self.edge_type.value().to_be_bytes());
        output.extend_from_slice(&self.source.value().to_be_bytes());
        output.extend_from_slice(&self.destination.value().to_be_bytes());
        append_checksum(&mut output);
        output
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, RecordCodecError> {
        let mut decoder = IdentityDecoder::new(bytes, ElementKind::Edge)?;
        let edge_type = EdgeTypeId::new(decoder.decoder.read_u32()?);
        let source = ElementId::new(decoder.decoder.read_u128()?);
        let destination = ElementId::new(decoder.decoder.read_u128()?);
        decoder.finish()?;
        Self::new(decoder.element, edge_type, source, destination)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidSegment {
    valid: Interval<ValidTime>,
    payload: CanonicalElement,
}

impl ValidSegment {
    #[must_use]
    pub const fn new(valid: Interval<ValidTime>, payload: CanonicalElement) -> Self {
        Self { valid, payload }
    }

    #[must_use]
    pub const fn valid(&self) -> Interval<ValidTime> {
        self.valid
    }

    #[must_use]
    pub const fn payload(&self) -> &CanonicalElement {
        &self.payload
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectionRecord {
    commit_ts: TransactionTime,
    segments: Vec<ValidSegment>,
}

impl ProjectionRecord {
    pub fn new(
        commit_ts: TransactionTime,
        segments: Vec<ValidSegment>,
    ) -> Result<Self, RecordCodecError> {
        validate_segments(&segments)?;
        Ok(Self {
            commit_ts,
            segments,
        })
    }

    #[must_use]
    pub const fn commit_ts(&self) -> TransactionTime {
        self.commit_ts
    }

    #[must_use]
    pub fn segments(&self) -> &[ValidSegment] {
        &self.segments
    }

    #[must_use]
    pub fn visible_at(&self, valid_time: ValidTime) -> Option<&CanonicalElement> {
        self.segments
            .iter()
            .find(|segment| segment.valid.contains(valid_time))
            .map(ValidSegment::payload)
    }

    pub fn encode(&self) -> Result<Vec<u8>, RecordCodecError> {
        let mut output = Vec::new();
        output.extend_from_slice(PROJECTION_MAGIC);
        output.extend_from_slice(&FORMAT_VERSION.to_be_bytes());
        encode_transaction_time(&mut output, self.commit_ts);
        write_len(&mut output, self.segments.len())?;
        for segment in &self.segments {
            encode_interval(&mut output, segment.valid);
            let payload = segment
                .payload
                .encode()
                .map_err(RecordCodecError::Canonical)?;
            write_len(&mut output, payload.len())?;
            output.extend_from_slice(&payload);
        }
        append_checksum(&mut output);
        Ok(output)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, RecordCodecError> {
        let mut decoder = Decoder::new(bytes);
        decoder.expect_magic(PROJECTION_MAGIC)?;
        decoder.expect_version()?;
        let commit_ts = decoder.read_transaction_time()?;
        let segment_count = decoder.read_u32()? as usize;
        let mut segments = Vec::with_capacity(segment_count.min(1024));
        for _ in 0..segment_count {
            let valid = decoder.read_interval()?;
            let payload_length = decoder.read_u32()? as usize;
            let payload = CanonicalElement::decode(decoder.take(payload_length)?)
                .map_err(RecordCodecError::Canonical)?;
            segments.push(ValidSegment::new(valid, payload));
        }
        decoder.verify_checksum_and_finish()?;
        Self::new(commit_ts, segments)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HistoryAnchor {
    commit_ts: TransactionTime,
    changed_valid: Interval<ValidTime>,
    projection: ProjectionRecord,
}

impl HistoryAnchor {
    pub fn new(
        commit_ts: TransactionTime,
        changed_valid: Interval<ValidTime>,
        projection: ProjectionRecord,
    ) -> Result<Self, RecordCodecError> {
        if commit_ts != projection.commit_ts {
            return Err(RecordCodecError::CommitTimestampMismatch);
        }
        Ok(Self {
            commit_ts,
            changed_valid,
            projection,
        })
    }

    #[must_use]
    pub const fn commit_ts(&self) -> TransactionTime {
        self.commit_ts
    }

    #[must_use]
    pub const fn changed_valid(&self) -> Interval<ValidTime> {
        self.changed_valid
    }

    #[must_use]
    pub const fn projection(&self) -> &ProjectionRecord {
        &self.projection
    }

    pub fn encode(&self) -> Result<Vec<u8>, RecordCodecError> {
        let projection = self.projection.encode()?;
        let mut output = Vec::new();
        output.extend_from_slice(ANCHOR_MAGIC);
        output.extend_from_slice(&FORMAT_VERSION.to_be_bytes());
        encode_transaction_time(&mut output, self.commit_ts);
        encode_interval(&mut output, self.changed_valid);
        write_len(&mut output, projection.len())?;
        output.extend_from_slice(&projection);
        append_checksum(&mut output);
        Ok(output)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, RecordCodecError> {
        let mut decoder = Decoder::new(bytes);
        decoder.expect_magic(ANCHOR_MAGIC)?;
        decoder.expect_version()?;
        let commit_ts = decoder.read_transaction_time()?;
        let changed_valid = decoder.read_interval()?;
        let projection_length = decoder.read_u32()? as usize;
        let projection = ProjectionRecord::decode(decoder.take(projection_length)?)?;
        decoder.verify_checksum_and_finish()?;
        Self::new(commit_ts, changed_valid, projection)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HistoryDelta {
    commit_ts: TransactionTime,
    changed_valid: Interval<ValidTime>,
    replacement: Option<CanonicalElement>,
}

impl HistoryDelta {
    #[must_use]
    pub const fn put(
        commit_ts: TransactionTime,
        changed_valid: Interval<ValidTime>,
        replacement: CanonicalElement,
    ) -> Self {
        Self {
            commit_ts,
            changed_valid,
            replacement: Some(replacement),
        }
    }

    #[must_use]
    pub const fn delete(commit_ts: TransactionTime, changed_valid: Interval<ValidTime>) -> Self {
        Self {
            commit_ts,
            changed_valid,
            replacement: None,
        }
    }

    #[must_use]
    pub const fn commit_ts(&self) -> TransactionTime {
        self.commit_ts
    }

    #[must_use]
    pub const fn changed_valid(&self) -> Interval<ValidTime> {
        self.changed_valid
    }

    #[must_use]
    pub const fn replacement(&self) -> Option<&CanonicalElement> {
        self.replacement.as_ref()
    }

    pub fn encode(&self) -> Result<Vec<u8>, RecordCodecError> {
        let mut output = Vec::new();
        output.extend_from_slice(DELTA_MAGIC);
        output.extend_from_slice(&FORMAT_VERSION.to_be_bytes());
        encode_transaction_time(&mut output, self.commit_ts);
        encode_interval(&mut output, self.changed_valid);
        match &self.replacement {
            Some(replacement) => {
                output.push(1);
                let payload = replacement.encode().map_err(RecordCodecError::Canonical)?;
                write_len(&mut output, payload.len())?;
                output.extend_from_slice(&payload);
            }
            None => output.push(0),
        }
        append_checksum(&mut output);
        Ok(output)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, RecordCodecError> {
        let mut decoder = Decoder::new(bytes);
        decoder.expect_magic(DELTA_MAGIC)?;
        decoder.expect_version()?;
        let commit_ts = decoder.read_transaction_time()?;
        let changed_valid = decoder.read_interval()?;
        let replacement = match decoder.read_u8()? {
            0 => None,
            1 => {
                let length = decoder.read_u32()? as usize;
                Some(
                    CanonicalElement::decode(decoder.take(length)?)
                        .map_err(RecordCodecError::Canonical)?,
                )
            }
            _ => return Err(RecordCodecError::InvalidHistoryOperation),
        };
        decoder.verify_checksum_and_finish()?;
        Ok(Self {
            commit_ts,
            changed_valid,
            replacement,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HistoryEntry {
    Anchor(HistoryAnchor),
    Delta(HistoryDelta),
}

impl HistoryEntry {
    pub fn decode(bytes: &[u8]) -> Result<Self, RecordCodecError> {
        let magic = bytes.get(..4).ok_or(RecordCodecError::UnexpectedEnd)?;
        if magic == ANCHOR_MAGIC {
            Ok(Self::Anchor(HistoryAnchor::decode(bytes)?))
        } else if magic == DELTA_MAGIC {
            Ok(Self::Delta(HistoryDelta::decode(bytes)?))
        } else {
            Err(RecordCodecError::InvalidMagic)
        }
    }

    #[must_use]
    pub const fn commit_ts(&self) -> TransactionTime {
        match self {
            Self::Anchor(anchor) => anchor.commit_ts(),
            Self::Delta(delta) => delta.commit_ts(),
        }
    }

    #[must_use]
    pub const fn changed_valid(&self) -> Interval<ValidTime> {
        match self {
            Self::Anchor(anchor) => anchor.changed_valid(),
            Self::Delta(delta) => delta.changed_valid(),
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>, RecordCodecError> {
        match self {
            Self::Anchor(anchor) => anchor.encode(),
            Self::Delta(delta) => delta.encode(),
        }
    }

    #[must_use]
    pub const fn is_anchor(&self) -> bool {
        matches!(self, Self::Anchor(_))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RecordCodecError {
    InvalidMagic,
    UnsupportedVersion(u16),
    WrongElementKind,
    UnexpectedEnd,
    TrailingBytes,
    LengthOverflow,
    InvalidInterval,
    InvalidHistoryOperation,
    OverlappingOrUnsortedSegments,
    CommitTimestampMismatch,
    ChecksumMismatch,
    Canonical(CodecError),
}

impl Display for RecordCodecError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidMagic => formatter.write_str("invalid temporal record magic"),
            Self::UnsupportedVersion(version) => {
                write!(formatter, "unsupported temporal record version {version}")
            }
            Self::WrongElementKind => formatter.write_str("identity has the wrong element kind"),
            Self::UnexpectedEnd => formatter.write_str("temporal record ended unexpectedly"),
            Self::TrailingBytes => formatter.write_str("temporal record has trailing bytes"),
            Self::LengthOverflow => formatter.write_str("temporal record length exceeds u32"),
            Self::InvalidInterval => {
                formatter.write_str("temporal record contains an invalid interval")
            }
            Self::InvalidHistoryOperation => {
                formatter.write_str("history delta contains an invalid operation")
            }
            Self::OverlappingOrUnsortedSegments => {
                formatter.write_str("projection segments overlap or are not ordered")
            }
            Self::CommitTimestampMismatch => {
                formatter.write_str("anchor and projection commit timestamps differ")
            }
            Self::ChecksumMismatch => formatter.write_str("temporal record checksum mismatch"),
            Self::Canonical(error) => write!(formatter, "invalid canonical payload: {error}"),
        }
    }
}

impl Error for RecordCodecError {}

fn identity_header(kind: ElementKind, element: ElementRef) -> Vec<u8> {
    let mut output = Vec::new();
    output.extend_from_slice(IDENTITY_MAGIC);
    output.extend_from_slice(&FORMAT_VERSION.to_be_bytes());
    output.push(kind as u8);
    output.extend_from_slice(&element.graph().value().to_be_bytes());
    output.extend_from_slice(&element.partition().value().to_be_bytes());
    output.extend_from_slice(&element.id().value().to_be_bytes());
    output
}

fn validate_segments(segments: &[ValidSegment]) -> Result<(), RecordCodecError> {
    for pair in segments.windows(2) {
        let previous = pair[0].valid;
        let next = pair[1].valid;
        if previous.end().is_none_or(|end| next.start() < end) {
            return Err(RecordCodecError::OverlappingOrUnsortedSegments);
        }
    }
    Ok(())
}

fn encode_transaction_time(output: &mut Vec<u8>, value: TransactionTime) {
    output.extend_from_slice(&value.physical_micros().to_be_bytes());
    output.extend_from_slice(&value.logical().to_be_bytes());
}

fn encode_interval(output: &mut Vec<u8>, interval: Interval<ValidTime>) {
    output.extend_from_slice(&interval.start().as_micros().to_be_bytes());
    match interval.end() {
        Some(end) => {
            output.push(1);
            output.extend_from_slice(&end.as_micros().to_be_bytes());
        }
        None => output.push(0),
    }
}

fn write_len(output: &mut Vec<u8>, length: usize) -> Result<(), RecordCodecError> {
    let length = u32::try_from(length).map_err(|_| RecordCodecError::LengthOverflow)?;
    output.extend_from_slice(&length.to_be_bytes());
    Ok(())
}

fn append_checksum(output: &mut Vec<u8>) {
    output.extend_from_slice(&checksum(output).to_be_bytes());
}

fn checksum(bytes: &[u8]) -> u64 {
    let mut value = 0xcbf2_9ce4_8422_2325_u64;
    for byte in bytes {
        value ^= u64::from(*byte);
        value = value.wrapping_mul(0x0000_0100_0000_01b3);
    }
    value
}

struct IdentityDecoder<'a> {
    decoder: Decoder<'a>,
    element: ElementRef,
}

impl<'a> IdentityDecoder<'a> {
    fn new(bytes: &'a [u8], expected_kind: ElementKind) -> Result<Self, RecordCodecError> {
        let mut decoder = Decoder::new(bytes);
        decoder.expect_magic(IDENTITY_MAGIC)?;
        decoder.expect_version()?;
        let kind = match decoder.read_u8()? {
            1 => ElementKind::Vertex,
            2 => ElementKind::Edge,
            _ => return Err(RecordCodecError::WrongElementKind),
        };
        if kind != expected_kind {
            return Err(RecordCodecError::WrongElementKind);
        }
        let graph = GraphId::new(decoder.read_u64()?);
        let partition = PartitionId::new(decoder.read_u32()?);
        let id = ElementId::new(decoder.read_u128()?);
        let element = match kind {
            ElementKind::Vertex => ElementRef::vertex(graph, partition, id),
            ElementKind::Edge => ElementRef::edge(graph, partition, id),
        };
        Ok(Self { decoder, element })
    }

    fn finish(&mut self) -> Result<(), RecordCodecError> {
        self.decoder.verify_checksum_and_finish()
    }
}

struct Decoder<'a> {
    input: &'a [u8],
    position: usize,
}

impl<'a> Decoder<'a> {
    const fn new(input: &'a [u8]) -> Self {
        Self { input, position: 0 }
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], RecordCodecError> {
        let end = self
            .position
            .checked_add(length)
            .ok_or(RecordCodecError::UnexpectedEnd)?;
        let value = self
            .input
            .get(self.position..end)
            .ok_or(RecordCodecError::UnexpectedEnd)?;
        self.position = end;
        Ok(value)
    }

    fn take_array<const N: usize>(&mut self) -> Result<[u8; N], RecordCodecError> {
        self.take(N)?
            .try_into()
            .map_err(|_| RecordCodecError::UnexpectedEnd)
    }

    fn expect_magic(&mut self, expected: &[u8; 4]) -> Result<(), RecordCodecError> {
        if self.take(4)? == expected {
            Ok(())
        } else {
            Err(RecordCodecError::InvalidMagic)
        }
    }

    fn expect_version(&mut self) -> Result<(), RecordCodecError> {
        let version = self.read_u16()?;
        if version == FORMAT_VERSION {
            Ok(())
        } else {
            Err(RecordCodecError::UnsupportedVersion(version))
        }
    }

    fn read_u8(&mut self) -> Result<u8, RecordCodecError> {
        Ok(self.take(1)?[0])
    }

    fn read_u16(&mut self) -> Result<u16, RecordCodecError> {
        Ok(u16::from_be_bytes(self.take_array()?))
    }

    fn read_u32(&mut self) -> Result<u32, RecordCodecError> {
        Ok(u32::from_be_bytes(self.take_array()?))
    }

    fn read_u64(&mut self) -> Result<u64, RecordCodecError> {
        Ok(u64::from_be_bytes(self.take_array()?))
    }

    fn read_i64(&mut self) -> Result<i64, RecordCodecError> {
        Ok(i64::from_be_bytes(self.take_array()?))
    }

    fn read_u128(&mut self) -> Result<u128, RecordCodecError> {
        Ok(u128::from_be_bytes(self.take_array()?))
    }

    fn read_transaction_time(&mut self) -> Result<TransactionTime, RecordCodecError> {
        Ok(TransactionTime::new(self.read_i64()?, self.read_u32()?))
    }

    fn read_interval(&mut self) -> Result<Interval<ValidTime>, RecordCodecError> {
        let start = ValidTime::from_micros(self.read_i64()?);
        let end = match self.read_u8()? {
            0 => None,
            1 => Some(ValidTime::from_micros(self.read_i64()?)),
            _ => return Err(RecordCodecError::InvalidInterval),
        };
        Interval::new(start, end).map_err(|_| RecordCodecError::InvalidInterval)
    }

    fn verify_checksum_and_finish(&mut self) -> Result<(), RecordCodecError> {
        let checksum_position = self.position;
        let stored = self.read_u64()?;
        if self.position != self.input.len() {
            return Err(RecordCodecError::TrailingBytes);
        }
        if stored != checksum(&self.input[..checksum_position]) {
            return Err(RecordCodecError::ChecksumMismatch);
        }
        Ok(())
    }
}
