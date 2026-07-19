use std::error::Error;
use std::fmt::{self, Display, Formatter};

use super::{LogicalNodeId, SlotId};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ValidationError {
    UnsupportedVersion {
        expected: u16,
        actual: u16,
    },
    InvalidGraphId,
    InvalidSchemaVersion,
    InvalidTopologyEpoch,
    InvalidSemanticBaseline,
    InvalidQueryFingerprint,
    TooManyNodes {
        max: usize,
        actual: usize,
    },
    InvalidRoot(LogicalNodeId),
    InvalidInput {
        node: LogicalNodeId,
        input: LogicalNodeId,
    },
    InvalidInputCount {
        node: LogicalNodeId,
        expected: usize,
        actual: usize,
    },
    DuplicateSlot(SlotId),
    UnknownSlot {
        node: LogicalNodeId,
        slot: SlotId,
    },
    OutputSchemaMismatch,
}

impl Display for ValidationError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedVersion { expected, actual } => write!(
                formatter,
                "unsupported Temporal IR v2 version {actual}; expected {expected}"
            ),
            Self::InvalidGraphId => formatter.write_str("graph ID must be non-zero"),
            Self::InvalidSchemaVersion => formatter.write_str("schema version must be non-zero"),
            Self::InvalidTopologyEpoch => formatter.write_str("topology epoch must be non-zero"),
            Self::InvalidSemanticBaseline => {
                formatter.write_str("semantic baseline must be non-empty and bounded")
            }
            Self::InvalidQueryFingerprint => {
                formatter.write_str("query fingerprint must be non-zero")
            }
            Self::TooManyNodes { max, actual } => {
                write!(
                    formatter,
                    "logical plan has {actual} nodes; maximum is {max}"
                )
            }
            Self::InvalidRoot(root) => write!(formatter, "invalid logical root {root:?}"),
            Self::InvalidInput { node, input } => {
                write!(formatter, "node {node:?} has invalid input {input:?}")
            }
            Self::InvalidInputCount {
                node,
                expected,
                actual,
            } => write!(
                formatter,
                "node {node:?} requires {expected} inputs; got {actual}"
            ),
            Self::DuplicateSlot(slot) => write!(formatter, "duplicate row slot {slot:?}"),
            Self::UnknownSlot { node, slot } => {
                write!(formatter, "node {node:?} references unknown slot {slot:?}")
            }
            Self::OutputSchemaMismatch => {
                formatter.write_str("plan output schema differs from root schema")
            }
        }
    }
}

impl Error for ValidationError {}
