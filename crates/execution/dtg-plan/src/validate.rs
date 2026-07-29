use std::fmt;

use dtg_language_ir::{GraphScope, IrError, LogicalNodeId, LogicalProgram};
use dtg_storage::{Digest32, ShardId, StorageError};

use crate::PlanningContext;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PlanError {
    InvalidProgram(IrError),
    InvalidCatalog(String),
    GraphMismatch,
    UnsupportedStatement,
    UnboundPointIdentity {
        node: LogicalNodeId,
    },
    NoBoundedAccess {
        node: LogicalNodeId,
    },
    CapabilityDrift {
        shard_id: ShardId,
        expected: Digest32,
        actual: Digest32,
    },
    InvalidStorageRequest(String),
}

impl fmt::Display for PlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidProgram(error) => write!(formatter, "invalid logical program: {error}"),
            Self::InvalidCatalog(message) => write!(formatter, "invalid catalog: {message}"),
            Self::GraphMismatch => {
                formatter.write_str("logical graph scope does not match catalog graph")
            }
            Self::UnsupportedStatement => {
                formatter.write_str("logical statement is not supported by query planning")
            }
            Self::UnboundPointIdentity { node } => write!(
                formatter,
                "point identity for logical node {} is not a positive literal",
                node.get()
            ),
            Self::NoBoundedAccess { node } => write!(
                formatter,
                "logical node {} has no bounded storage access",
                node.get()
            ),
            Self::CapabilityDrift { shard_id, .. } => write!(
                formatter,
                "capability digest drift for Shard {}",
                shard_id.get()
            ),
            Self::InvalidStorageRequest(message) => {
                write!(formatter, "invalid storage request: {message}")
            }
        }
    }
}

impl std::error::Error for PlanError {}

impl From<IrError> for PlanError {
    fn from(value: IrError) -> Self {
        Self::InvalidProgram(value)
    }
}

impl From<StorageError> for PlanError {
    fn from(value: StorageError) -> Self {
        Self::InvalidStorageRequest(value.to_string())
    }
}

pub(crate) fn validate_inputs(
    program: &LogicalProgram,
    context: &PlanningContext,
) -> Result<(), PlanError> {
    dtg_language_ir::validate_program(program)?;
    if let GraphScope::Explicit(graph_id) = program.graph_scope
        && graph_id != context.catalog().graph_id()
    {
        return Err(PlanError::GraphMismatch);
    }
    Ok(())
}
