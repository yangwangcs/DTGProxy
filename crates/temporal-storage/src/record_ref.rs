use temporal_types::{CanonicalElementRef, Interval, TransactionTime, ValidTime};

use crate::record::{
    ANCHOR_MAGIC, DELTA_MAGIC, Decoder, HistoryRecordKind, PROJECTION_MAGIC, RecordCodecError,
    history_record_kind, validate_segment_order,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HistoryEntryRef<'a> {
    Anchor(HistoryAnchorRef<'a>),
    Delta(HistoryDeltaRef<'a>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HistoryAnchorRef<'a> {
    commit_ts: TransactionTime,
    changed_valid: Interval<ValidTime>,
    projection: ProjectionRecordRef<'a>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HistoryDeltaRef<'a> {
    commit_ts: TransactionTime,
    changed_valid: Interval<ValidTime>,
    operation: HistoryOperationRef<'a>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HistoryOperationRef<'a> {
    Put(CanonicalElementRef<'a>),
    Delete,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProjectionRecordRef<'a> {
    commit_ts: TransactionTime,
    encoded_segments: &'a [u8],
    segment_count: usize,
}

impl<'a> ProjectionRecordRef<'a> {
    pub fn parse(bytes: &'a [u8]) -> Result<Self, RecordCodecError> {
        let mut decoder = Decoder::new(bytes);
        decoder.expect_magic(PROJECTION_MAGIC)?;
        decoder.expect_version()?;
        let commit_ts = decoder.read_transaction_time()?;
        let segment_count = decoder.read_length()?;
        let segments_start = decoder.position();

        for _ in 0..segment_count {
            decoder.read_interval()?;
            let payload_length = decoder.read_length()?;
            CanonicalElementRef::parse(decoder.take(payload_length)?)
                .map_err(RecordCodecError::Canonical)?;
        }

        let segments_end = decoder.position();
        decoder.verify_checksum_and_finish()?;
        let encoded_segments = bytes
            .get(segments_start..segments_end)
            .ok_or(RecordCodecError::UnexpectedEnd)?;
        validate_segments(encoded_segments, segment_count)?;

        Ok(Self {
            commit_ts,
            encoded_segments,
            segment_count,
        })
    }

    #[must_use]
    pub const fn commit_ts(self) -> TransactionTime {
        self.commit_ts
    }

    #[must_use]
    pub const fn segment_count(self) -> usize {
        self.segment_count
    }

    pub fn visible_at(
        self,
        valid_time: ValidTime,
    ) -> Result<Option<CanonicalElementRef<'a>>, RecordCodecError> {
        let mut decoder = Decoder::new(self.encoded_segments);
        for _ in 0..self.segment_count {
            let valid = decoder.read_interval()?;
            let payload_length = decoder.read_length()?;
            let payload = decoder.take(payload_length)?;
            if valid.contains(valid_time) {
                return CanonicalElementRef::parse(payload)
                    .map(Some)
                    .map_err(RecordCodecError::Canonical);
            }
        }
        Ok(None)
    }
}

impl<'a> HistoryEntryRef<'a> {
    pub fn parse(bytes: &'a [u8]) -> Result<Self, RecordCodecError> {
        match history_record_kind(bytes)? {
            HistoryRecordKind::Anchor => Ok(Self::Anchor(HistoryAnchorRef::parse(bytes)?)),
            HistoryRecordKind::Delta => Ok(Self::Delta(HistoryDeltaRef::parse(bytes)?)),
        }
    }

    #[must_use]
    pub const fn commit_ts(self) -> TransactionTime {
        match self {
            Self::Anchor(anchor) => anchor.commit_ts(),
            Self::Delta(delta) => delta.commit_ts(),
        }
    }

    #[must_use]
    pub const fn changed_valid(self) -> Interval<ValidTime> {
        match self {
            Self::Anchor(anchor) => anchor.changed_valid(),
            Self::Delta(delta) => delta.changed_valid(),
        }
    }

    pub fn replacement(self) -> Result<Option<CanonicalElementRef<'a>>, RecordCodecError> {
        match self {
            Self::Anchor(anchor) => anchor
                .projection()
                .visible_at(anchor.changed_valid().start()),
            Self::Delta(delta) => match delta.operation() {
                HistoryOperationRef::Put(payload) => Ok(Some(payload)),
                HistoryOperationRef::Delete => Ok(None),
            },
        }
    }

    #[must_use]
    pub const fn is_anchor(self) -> bool {
        matches!(self, Self::Anchor(_))
    }
}

impl<'a> HistoryAnchorRef<'a> {
    pub fn parse(bytes: &'a [u8]) -> Result<Self, RecordCodecError> {
        let mut decoder = Decoder::new(bytes);
        decoder.expect_magic(ANCHOR_MAGIC)?;
        decoder.expect_version()?;
        let commit_ts = decoder.read_transaction_time()?;
        let changed_valid = decoder.read_interval()?;
        let projection_length = decoder.read_length()?;
        let projection = ProjectionRecordRef::parse(decoder.take(projection_length)?)?;
        decoder.verify_checksum_and_finish()?;
        if commit_ts != projection.commit_ts() {
            return Err(RecordCodecError::CommitTimestampMismatch);
        }
        Ok(Self {
            commit_ts,
            changed_valid,
            projection,
        })
    }

    #[must_use]
    pub const fn commit_ts(self) -> TransactionTime {
        self.commit_ts
    }

    #[must_use]
    pub const fn changed_valid(self) -> Interval<ValidTime> {
        self.changed_valid
    }

    #[must_use]
    pub const fn projection(self) -> ProjectionRecordRef<'a> {
        self.projection
    }
}

impl<'a> HistoryDeltaRef<'a> {
    pub fn parse(bytes: &'a [u8]) -> Result<Self, RecordCodecError> {
        let mut decoder = Decoder::new(bytes);
        decoder.expect_magic(DELTA_MAGIC)?;
        decoder.expect_version()?;
        let commit_ts = decoder.read_transaction_time()?;
        let changed_valid = decoder.read_interval()?;
        let operation = match decoder.read_u8()? {
            0 => HistoryOperationRef::Delete,
            1 => {
                let payload_length = decoder.read_length()?;
                let payload = CanonicalElementRef::parse(decoder.take(payload_length)?)
                    .map_err(RecordCodecError::Canonical)?;
                HistoryOperationRef::Put(payload)
            }
            _ => return Err(RecordCodecError::InvalidHistoryOperation),
        };
        decoder.verify_checksum_and_finish()?;
        Ok(Self {
            commit_ts,
            changed_valid,
            operation,
        })
    }

    #[must_use]
    pub const fn commit_ts(self) -> TransactionTime {
        self.commit_ts
    }

    #[must_use]
    pub const fn changed_valid(self) -> Interval<ValidTime> {
        self.changed_valid
    }

    #[must_use]
    pub const fn operation(self) -> HistoryOperationRef<'a> {
        self.operation
    }
}

fn validate_segments(
    encoded_segments: &[u8],
    segment_count: usize,
) -> Result<(), RecordCodecError> {
    let mut decoder = Decoder::new(encoded_segments);
    let mut previous = None;
    for _ in 0..segment_count {
        let valid = decoder.read_interval()?;
        if let Some(previous) = previous {
            validate_segment_order(previous, valid)?;
        }
        previous = Some(valid);
        let payload_length = decoder.read_length()?;
        decoder.take(payload_length)?;
    }
    Ok(())
}
