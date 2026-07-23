use std::error::Error;
use std::fmt::{self, Display, Formatter};

use storage_api::{Keyspace, LogicalKey};
use temporal_types::TransactionTime;

use crate::{
    EdgeIdentity, GraphKey, HistoryEntry, KeyCodecError, ProjectionRecord, RecordCodecError,
    VertexIdentity, cross_in_adjacency_key, cross_out_adjacency_key, current_edge_key,
    current_vertex_key, decode_graph_key, edge_identity_key, history_anchor_key, in_adjacency_key,
    out_adjacency_key, vertex_identity_key,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CanonicalGraphEntry {
    VertexIdentity {
        key: GraphKey,
        value: VertexIdentity,
    },
    EdgeIdentity {
        key: GraphKey,
        value: EdgeIdentity,
    },
    Current {
        key: GraphKey,
        value: ProjectionRecord,
    },
    Adjacency {
        key: GraphKey,
        value: ProjectionRecord,
    },
    History {
        key: GraphKey,
        value: HistoryEntry,
    },
    Opaque {
        key: LogicalKey,
        value: Vec<u8>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CanonicalMappingError {
    Key(KeyCodecError),
    Record(RecordCodecError),
    UnsupportedKeyspace(Keyspace),
    ValueKindMismatch {
        keyspace: Keyspace,
    },
    HistoryTimestampMismatch {
        key: TransactionTime,
        value: TransactionTime,
    },
    NonCanonicalBytes,
}

impl Display for CanonicalMappingError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Key(error) => write!(formatter, "invalid canonical graph key: {error}"),
            Self::Record(error) => write!(formatter, "invalid canonical graph value: {error}"),
            Self::UnsupportedKeyspace(keyspace) => {
                write!(formatter, "unsupported typed graph keyspace {keyspace:?}")
            }
            Self::ValueKindMismatch { keyspace } => {
                write!(
                    formatter,
                    "canonical graph value kind mismatches keyspace {keyspace:?}"
                )
            }
            Self::HistoryTimestampMismatch { key, value } => write!(
                formatter,
                "history key transaction timestamp {key:?} differs from value timestamp {value:?}"
            ),
            Self::NonCanonicalBytes => {
                formatter.write_str("canonical graph value does not re-encode byte-identically")
            }
        }
    }
}

impl Error for CanonicalMappingError {}

impl From<KeyCodecError> for CanonicalMappingError {
    fn from(error: KeyCodecError) -> Self {
        Self::Key(error)
    }
}

impl From<RecordCodecError> for CanonicalMappingError {
    fn from(error: RecordCodecError) -> Self {
        Self::Record(error)
    }
}

pub fn decode_canonical_graph_entry(
    key: &LogicalKey,
    value: &[u8],
) -> Result<CanonicalGraphEntry, CanonicalMappingError> {
    let entry = match key.keyspace() {
        Keyspace::Identity => match decode_graph_key(key)? {
            graph_key @ GraphKey::VertexIdentity(element) => {
                let decoded = VertexIdentity::decode(value)?;
                if decoded.element() != element {
                    return Err(CanonicalMappingError::ValueKindMismatch {
                        keyspace: key.keyspace(),
                    });
                }
                CanonicalGraphEntry::VertexIdentity {
                    key: graph_key,
                    value: decoded,
                }
            }
            graph_key @ GraphKey::EdgeIdentity(element) => {
                let decoded = EdgeIdentity::decode(value)?;
                if decoded.element() != element {
                    return Err(CanonicalMappingError::ValueKindMismatch {
                        keyspace: key.keyspace(),
                    });
                }
                CanonicalGraphEntry::EdgeIdentity {
                    key: graph_key,
                    value: decoded,
                }
            }
            _ => return Err(CanonicalMappingError::Key(KeyCodecError::UnknownTag(0))),
        },
        Keyspace::Current => match decode_graph_key(key)? {
            graph_key @ (GraphKey::CurrentVertex(_) | GraphKey::CurrentEdge(_)) => {
                CanonicalGraphEntry::Current {
                    key: graph_key,
                    value: ProjectionRecord::decode(value)?,
                }
            }
            _ => return Err(CanonicalMappingError::Key(KeyCodecError::UnknownTag(0))),
        },
        Keyspace::AdjOut | Keyspace::AdjIn => match decode_graph_key(key)? {
            graph_key @ (GraphKey::OutAdjacency { .. }
            | GraphKey::InAdjacency { .. }
            | GraphKey::CrossOutAdjacency { .. }
            | GraphKey::CrossInAdjacency { .. }) => CanonicalGraphEntry::Adjacency {
                key: graph_key,
                value: ProjectionRecord::decode(value)?,
            },
            _ => return Err(CanonicalMappingError::Key(KeyCodecError::UnknownTag(0))),
        },
        Keyspace::History => match decode_graph_key(key)? {
            graph_key @ GraphKey::HistoryAnchor {
                transaction_time, ..
            } => {
                let decoded = HistoryEntry::decode(value)?;
                if decoded.commit_ts() != transaction_time {
                    return Err(CanonicalMappingError::HistoryTimestampMismatch {
                        key: transaction_time,
                        value: decoded.commit_ts(),
                    });
                }
                CanonicalGraphEntry::History {
                    key: graph_key,
                    value: decoded,
                }
            }
            _ => return Err(CanonicalMappingError::Key(KeyCodecError::UnknownTag(0))),
        },
        Keyspace::Meta | Keyspace::TemporalIndex | Keyspace::Txn => CanonicalGraphEntry::Opaque {
            key: key.clone(),
            value: value.to_vec(),
        },
    };

    let (encoded_key, encoded_value) = encode_canonical_graph_entry(&entry)?;
    if &encoded_key != key || encoded_value != value {
        return Err(CanonicalMappingError::NonCanonicalBytes);
    }
    Ok(entry)
}

pub fn encode_canonical_graph_entry(
    entry: &CanonicalGraphEntry,
) -> Result<(LogicalKey, Vec<u8>), CanonicalMappingError> {
    match entry {
        CanonicalGraphEntry::VertexIdentity { key, value } => {
            let GraphKey::VertexIdentity(element) = *key else {
                return Err(CanonicalMappingError::ValueKindMismatch {
                    keyspace: Keyspace::Identity,
                });
            };
            if value.element() != element {
                return Err(CanonicalMappingError::ValueKindMismatch {
                    keyspace: Keyspace::Identity,
                });
            }
            Ok((vertex_identity_key(element), value.encode()))
        }
        CanonicalGraphEntry::EdgeIdentity { key, value } => {
            let GraphKey::EdgeIdentity(element) = *key else {
                return Err(CanonicalMappingError::ValueKindMismatch {
                    keyspace: Keyspace::Identity,
                });
            };
            if value.element() != element {
                return Err(CanonicalMappingError::ValueKindMismatch {
                    keyspace: Keyspace::Identity,
                });
            }
            Ok((edge_identity_key(element), value.encode()))
        }
        CanonicalGraphEntry::Current { key, value } => {
            let encoded_key = match *key {
                GraphKey::CurrentVertex(element) => current_vertex_key(element),
                GraphKey::CurrentEdge(element) => current_edge_key(element),
                _ => {
                    return Err(CanonicalMappingError::ValueKindMismatch {
                        keyspace: Keyspace::Current,
                    });
                }
            };
            Ok((encoded_key, value.encode()?))
        }
        CanonicalGraphEntry::Adjacency { key, value } => {
            let encoded_key = match *key {
                GraphKey::OutAdjacency {
                    graph,
                    partition,
                    source,
                    edge_type,
                    bucket,
                    destination,
                    edge,
                } => out_adjacency_key(
                    graph,
                    partition,
                    source,
                    edge_type,
                    bucket,
                    destination,
                    edge,
                ),
                GraphKey::InAdjacency {
                    graph,
                    partition,
                    destination,
                    edge_type,
                    bucket,
                    source,
                    edge,
                } => in_adjacency_key(
                    graph,
                    partition,
                    destination,
                    edge_type,
                    bucket,
                    source,
                    edge,
                ),
                GraphKey::CrossOutAdjacency {
                    graph,
                    partition,
                    source,
                    edge_type,
                    bucket,
                    destination_partition,
                    destination,
                    edge_partition,
                    edge,
                } => cross_out_adjacency_key(
                    graph,
                    partition,
                    source,
                    edge_type,
                    bucket,
                    destination_partition,
                    destination,
                    edge_partition,
                    edge,
                ),
                GraphKey::CrossInAdjacency {
                    graph,
                    partition,
                    destination,
                    edge_type,
                    bucket,
                    source_partition,
                    source,
                    edge_partition,
                    edge,
                } => cross_in_adjacency_key(
                    graph,
                    partition,
                    destination,
                    edge_type,
                    bucket,
                    source_partition,
                    source,
                    edge_partition,
                    edge,
                ),
                _ => {
                    return Err(CanonicalMappingError::ValueKindMismatch {
                        keyspace: Keyspace::AdjOut,
                    });
                }
            };
            Ok((encoded_key, value.encode()?))
        }
        CanonicalGraphEntry::History { key, value } => {
            let GraphKey::HistoryAnchor {
                element,
                transaction_time,
                segment_id,
            } = *key
            else {
                return Err(CanonicalMappingError::ValueKindMismatch {
                    keyspace: Keyspace::History,
                });
            };
            if value.commit_ts() != transaction_time {
                return Err(CanonicalMappingError::HistoryTimestampMismatch {
                    key: transaction_time,
                    value: value.commit_ts(),
                });
            }
            Ok((
                history_anchor_key(element, transaction_time, segment_id),
                value.encode()?,
            ))
        }
        CanonicalGraphEntry::Opaque { key, value } => Ok((key.clone(), value.clone())),
    }
}
