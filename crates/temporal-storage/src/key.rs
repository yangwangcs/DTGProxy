use std::error::Error;
use std::fmt::{self, Display, Formatter};

use storage_api::{Keyspace, LogicalKey};
use temporal_types::TransactionTime;

const TAG_VERTEX_IDENTITY: u8 = 0x01;
const TAG_EDGE_IDENTITY: u8 = 0x02;
const TAG_CURRENT_VERTEX: u8 = 0x08;
const TAG_CURRENT_EDGE: u8 = 0x09;
const TAG_ADJ_OUT: u8 = 0x10;
const TAG_ADJ_IN: u8 = 0x11;
const TAG_HISTORY_ANCHOR: u8 = 0x20;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct GraphId(u64);

impl GraphId {
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn value(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PartitionId(u32);

impl PartitionId {
    #[must_use]
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn value(self) -> u32 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ElementId(u128);

impl ElementId {
    #[must_use]
    pub const fn new(value: u128) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn value(self) -> u128 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct EdgeTypeId(u32);

impl EdgeTypeId {
    #[must_use]
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn value(self) -> u32 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct LabelId(u32);

impl LabelId {
    #[must_use]
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn value(self) -> u32 {
        self.0
    }
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ElementKind {
    Vertex = 1,
    Edge = 2,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ElementRef {
    graph: GraphId,
    partition: PartitionId,
    kind: ElementKind,
    id: ElementId,
}

impl ElementRef {
    #[must_use]
    pub const fn vertex(graph: GraphId, partition: PartitionId, id: ElementId) -> Self {
        Self {
            graph,
            partition,
            kind: ElementKind::Vertex,
            id,
        }
    }

    #[must_use]
    pub const fn edge(graph: GraphId, partition: PartitionId, id: ElementId) -> Self {
        Self {
            graph,
            partition,
            kind: ElementKind::Edge,
            id,
        }
    }

    #[must_use]
    pub const fn graph(self) -> GraphId {
        self.graph
    }

    #[must_use]
    pub const fn partition(self) -> PartitionId {
        self.partition
    }

    #[must_use]
    pub const fn kind(self) -> ElementKind {
        self.kind
    }

    #[must_use]
    pub const fn id(self) -> ElementId {
        self.id
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GraphKey {
    VertexIdentity(ElementRef),
    EdgeIdentity(ElementRef),
    CurrentVertex(ElementRef),
    CurrentEdge(ElementRef),
    OutAdjacency {
        graph: GraphId,
        partition: PartitionId,
        source: ElementId,
        edge_type: EdgeTypeId,
        bucket: u16,
        destination: ElementId,
        edge: ElementId,
    },
    InAdjacency {
        graph: GraphId,
        partition: PartitionId,
        destination: ElementId,
        edge_type: EdgeTypeId,
        bucket: u16,
        source: ElementId,
        edge: ElementId,
    },
    HistoryAnchor {
        element: ElementRef,
        transaction_time: TransactionTime,
        segment_id: u32,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KeyCodecError {
    UnknownTag(u8),
    WrongKeyspace,
    InvalidElementKind(u8),
    UnexpectedEnd,
    TrailingBytes,
}

impl Display for KeyCodecError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownTag(tag) => write!(formatter, "unknown temporal graph key tag {tag}"),
            Self::WrongKeyspace => {
                formatter.write_str("temporal graph key tag is in the wrong keyspace")
            }
            Self::InvalidElementKind(kind) => {
                write!(formatter, "invalid temporal element kind {kind}")
            }
            Self::UnexpectedEnd => formatter.write_str("temporal graph key ended unexpectedly"),
            Self::TrailingBytes => formatter.write_str("temporal graph key has trailing bytes"),
        }
    }
}

impl Error for KeyCodecError {}

#[must_use]
pub fn vertex_identity_key(element: ElementRef) -> LogicalKey {
    entity_key(Keyspace::Identity, TAG_VERTEX_IDENTITY, element)
}

#[must_use]
pub fn edge_identity_key(element: ElementRef) -> LogicalKey {
    entity_key(Keyspace::Identity, TAG_EDGE_IDENTITY, element)
}

#[must_use]
pub fn current_vertex_key(element: ElementRef) -> LogicalKey {
    entity_key(Keyspace::Current, TAG_CURRENT_VERTEX, element)
}

#[must_use]
pub fn current_edge_key(element: ElementRef) -> LogicalKey {
    entity_key(Keyspace::Current, TAG_CURRENT_EDGE, element)
}

#[must_use]
pub fn history_prefix(element: ElementRef) -> Vec<u8> {
    let mut key = Vec::with_capacity(30);
    key.push(TAG_HISTORY_ANCHOR);
    encode_element_ref(&mut key, element, true);
    key
}

#[must_use]
pub fn history_anchor_key(
    element: ElementRef,
    transaction_time: TransactionTime,
    segment_id: u32,
) -> LogicalKey {
    let mut key = history_prefix(element);
    let mut reversed = encode_transaction_time(transaction_time);
    for byte in &mut reversed {
        *byte = !*byte;
    }
    key.extend_from_slice(&reversed);
    key.extend_from_slice(&segment_id.to_be_bytes());
    LogicalKey::in_keyspace(Keyspace::History, key)
}

#[must_use]
#[allow(clippy::too_many_arguments)]
pub fn out_adjacency_key(
    graph: GraphId,
    partition: PartitionId,
    source: ElementId,
    edge_type: EdgeTypeId,
    bucket: u16,
    destination: ElementId,
    edge: ElementId,
) -> LogicalKey {
    adjacency_key(
        Keyspace::AdjOut,
        TAG_ADJ_OUT,
        graph,
        partition,
        source,
        edge_type,
        bucket,
        destination,
        edge,
    )
}

#[must_use]
#[allow(clippy::too_many_arguments)]
pub fn in_adjacency_key(
    graph: GraphId,
    partition: PartitionId,
    destination: ElementId,
    edge_type: EdgeTypeId,
    bucket: u16,
    source: ElementId,
    edge: ElementId,
) -> LogicalKey {
    adjacency_key(
        Keyspace::AdjIn,
        TAG_ADJ_IN,
        graph,
        partition,
        destination,
        edge_type,
        bucket,
        source,
        edge,
    )
}

pub fn decode_graph_key(key: &LogicalKey) -> Result<GraphKey, KeyCodecError> {
    let mut decoder = Decoder::new(key.as_bytes());
    let tag = decoder.read_u8()?;
    let decoded = match tag {
        TAG_VERTEX_IDENTITY => {
            require_keyspace(key, Keyspace::Identity)?;
            GraphKey::VertexIdentity(decoder.read_element_ref(ElementKind::Vertex, false)?)
        }
        TAG_EDGE_IDENTITY => {
            require_keyspace(key, Keyspace::Identity)?;
            GraphKey::EdgeIdentity(decoder.read_element_ref(ElementKind::Edge, false)?)
        }
        TAG_CURRENT_VERTEX => {
            require_keyspace(key, Keyspace::Current)?;
            GraphKey::CurrentVertex(decoder.read_element_ref(ElementKind::Vertex, false)?)
        }
        TAG_CURRENT_EDGE => {
            require_keyspace(key, Keyspace::Current)?;
            GraphKey::CurrentEdge(decoder.read_element_ref(ElementKind::Edge, false)?)
        }
        TAG_ADJ_OUT => {
            require_keyspace(key, Keyspace::AdjOut)?;
            let (graph, partition, first, edge_type, bucket, second, edge) =
                decoder.read_adjacency()?;
            GraphKey::OutAdjacency {
                graph,
                partition,
                source: first,
                edge_type,
                bucket,
                destination: second,
                edge,
            }
        }
        TAG_ADJ_IN => {
            require_keyspace(key, Keyspace::AdjIn)?;
            let (graph, partition, first, edge_type, bucket, second, edge) =
                decoder.read_adjacency()?;
            GraphKey::InAdjacency {
                graph,
                partition,
                destination: first,
                edge_type,
                bucket,
                source: second,
                edge,
            }
        }
        TAG_HISTORY_ANCHOR => {
            require_keyspace(key, Keyspace::History)?;
            let element = decoder.read_element_ref(ElementKind::Vertex, true)?;
            let mut transaction = decoder.take_array::<12>()?;
            for byte in &mut transaction {
                *byte = !*byte;
            }
            let transaction_time = decode_transaction_time(transaction);
            let segment_id = decoder.read_u32()?;
            GraphKey::HistoryAnchor {
                element,
                transaction_time,
                segment_id,
            }
        }
        other => return Err(KeyCodecError::UnknownTag(other)),
    };
    if decoder.finished() {
        Ok(decoded)
    } else {
        Err(KeyCodecError::TrailingBytes)
    }
}

fn entity_key(keyspace: Keyspace, tag: u8, element: ElementRef) -> LogicalKey {
    let mut key = Vec::with_capacity(29);
    key.push(tag);
    encode_element_ref(&mut key, element, false);
    LogicalKey::in_keyspace(keyspace, key)
}

fn encode_element_ref(output: &mut Vec<u8>, element: ElementRef, include_kind: bool) {
    output.extend_from_slice(&element.graph.value().to_be_bytes());
    output.extend_from_slice(&element.partition.value().to_be_bytes());
    if include_kind {
        output.push(element.kind as u8);
    }
    output.extend_from_slice(&element.id.value().to_be_bytes());
}

#[allow(clippy::too_many_arguments)]
fn adjacency_key(
    keyspace: Keyspace,
    tag: u8,
    graph: GraphId,
    partition: PartitionId,
    first: ElementId,
    edge_type: EdgeTypeId,
    bucket: u16,
    second: ElementId,
    edge: ElementId,
) -> LogicalKey {
    let mut key = Vec::with_capacity(67);
    key.push(tag);
    key.extend_from_slice(&graph.value().to_be_bytes());
    key.extend_from_slice(&partition.value().to_be_bytes());
    key.extend_from_slice(&first.value().to_be_bytes());
    key.extend_from_slice(&edge_type.value().to_be_bytes());
    key.extend_from_slice(&bucket.to_be_bytes());
    key.extend_from_slice(&second.value().to_be_bytes());
    key.extend_from_slice(&edge.value().to_be_bytes());
    LogicalKey::in_keyspace(keyspace, key)
}

fn encode_transaction_time(value: TransactionTime) -> [u8; 12] {
    let ordered_physical = (value.physical_micros() as u64) ^ (1_u64 << 63);
    let mut encoded = [0; 12];
    encoded[..8].copy_from_slice(&ordered_physical.to_be_bytes());
    encoded[8..].copy_from_slice(&value.logical().to_be_bytes());
    encoded
}

fn decode_transaction_time(value: [u8; 12]) -> TransactionTime {
    let physical = u64::from_be_bytes(value[..8].try_into().expect("fixed-width slice"));
    let physical = (physical ^ (1_u64 << 63)) as i64;
    let logical = u32::from_be_bytes(value[8..].try_into().expect("fixed-width slice"));
    TransactionTime::new(physical, logical)
}

fn require_keyspace(key: &LogicalKey, expected: Keyspace) -> Result<(), KeyCodecError> {
    if key.keyspace() == expected {
        Ok(())
    } else {
        Err(KeyCodecError::WrongKeyspace)
    }
}

type AdjacencyParts = (
    GraphId,
    PartitionId,
    ElementId,
    EdgeTypeId,
    u16,
    ElementId,
    ElementId,
);

struct Decoder<'a> {
    input: &'a [u8],
    position: usize,
}

impl<'a> Decoder<'a> {
    const fn new(input: &'a [u8]) -> Self {
        Self { input, position: 0 }
    }

    fn finished(&self) -> bool {
        self.position == self.input.len()
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], KeyCodecError> {
        let end = self
            .position
            .checked_add(length)
            .ok_or(KeyCodecError::UnexpectedEnd)?;
        let value = self
            .input
            .get(self.position..end)
            .ok_or(KeyCodecError::UnexpectedEnd)?;
        self.position = end;
        Ok(value)
    }

    fn take_array<const N: usize>(&mut self) -> Result<[u8; N], KeyCodecError> {
        self.take(N)?
            .try_into()
            .map_err(|_| KeyCodecError::UnexpectedEnd)
    }

    fn read_u8(&mut self) -> Result<u8, KeyCodecError> {
        Ok(self.take(1)?[0])
    }

    fn read_u16(&mut self) -> Result<u16, KeyCodecError> {
        Ok(u16::from_be_bytes(self.take_array()?))
    }

    fn read_u32(&mut self) -> Result<u32, KeyCodecError> {
        Ok(u32::from_be_bytes(self.take_array()?))
    }

    fn read_u64(&mut self) -> Result<u64, KeyCodecError> {
        Ok(u64::from_be_bytes(self.take_array()?))
    }

    fn read_u128(&mut self) -> Result<u128, KeyCodecError> {
        Ok(u128::from_be_bytes(self.take_array()?))
    }

    fn read_element_ref(
        &mut self,
        implicit_kind: ElementKind,
        include_kind: bool,
    ) -> Result<ElementRef, KeyCodecError> {
        let graph = GraphId::new(self.read_u64()?);
        let partition = PartitionId::new(self.read_u32()?);
        let kind = if include_kind {
            match self.read_u8()? {
                1 => ElementKind::Vertex,
                2 => ElementKind::Edge,
                kind => return Err(KeyCodecError::InvalidElementKind(kind)),
            }
        } else {
            implicit_kind
        };
        let id = ElementId::new(self.read_u128()?);
        Ok(ElementRef {
            graph,
            partition,
            kind,
            id,
        })
    }

    fn read_adjacency(&mut self) -> Result<AdjacencyParts, KeyCodecError> {
        Ok((
            GraphId::new(self.read_u64()?),
            PartitionId::new(self.read_u32()?),
            ElementId::new(self.read_u128()?),
            EdgeTypeId::new(self.read_u32()?),
            self.read_u16()?,
            ElementId::new(self.read_u128()?),
            ElementId::new(self.read_u128()?),
        ))
    }
}
