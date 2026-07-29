use dtg_kernel::{BackendGeneration, ClusterId, GraphId, PlacementEpoch, ShardId, Version};

use crate::{MAX_TRACE_CONTEXT_BYTES, PROTOCOL_MAJOR, ProtocolError, proto};

pub const SUPPORTED_MINOR_MIN: u32 = 0;
pub const SUPPORTED_MINOR_MAX: u32 = 1;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestContext {
    protocol_minor: u32,
    cluster_id: ClusterId,
    request_id: u128,
    deadline_unix_ms: u64,
    trace_context: Vec<u8>,
}

impl RequestContext {
    pub const fn protocol_minor(&self) -> u32 {
        self.protocol_minor
    }

    pub const fn cluster_id(&self) -> ClusterId {
        self.cluster_id
    }

    pub const fn request_id(&self) -> u128 {
        self.request_id
    }

    pub const fn deadline_unix_ms(&self) -> u64 {
        self.deadline_unix_ms
    }

    pub fn trace_context(&self) -> &[u8] {
        &self.trace_context
    }
}

impl TryFrom<proto::RequestContext> for RequestContext {
    type Error = ProtocolError;

    fn try_from(wire: proto::RequestContext) -> Result<Self, Self::Error> {
        if wire.protocol_major != PROTOCOL_MAJOR {
            return Err(ProtocolError::MajorVersion);
        }
        if !(SUPPORTED_MINOR_MIN..=SUPPORTED_MINOR_MAX).contains(&wire.protocol_minor) {
            return Err(ProtocolError::MinorVersion);
        }
        let cluster_id = exact_u64(&wire.cluster_id)?;
        let cluster_id = ClusterId::new(cluster_id).map_err(|_| ProtocolError::ZeroCluster)?;
        let request_id = exact_u128(&wire.request_id)?;
        if request_id == 0 {
            return Err(ProtocolError::ZeroRequest);
        }
        if wire.deadline_unix_ms == 0 {
            return Err(ProtocolError::ZeroDeadline);
        }
        if wire.trace_context.len() > MAX_TRACE_CONTEXT_BYTES {
            return Err(ProtocolError::TraceLimit);
        }
        Ok(Self {
            protocol_minor: wire.protocol_minor,
            cluster_id,
            request_id,
            deadline_unix_ms: wire.deadline_unix_ms,
            trace_context: wire.trace_context,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShardRequestContext {
    request: RequestContext,
    graph_id: GraphId,
    shard_id: ShardId,
    placement_epoch: PlacementEpoch,
    backend_generation: BackendGeneration,
    catalog_version: Version,
}

impl ShardRequestContext {
    pub const fn request(&self) -> &RequestContext {
        &self.request
    }

    pub const fn graph_id(&self) -> GraphId {
        self.graph_id
    }

    pub const fn shard_id(&self) -> ShardId {
        self.shard_id
    }

    pub const fn placement_epoch(&self) -> PlacementEpoch {
        self.placement_epoch
    }

    pub const fn backend_generation(&self) -> BackendGeneration {
        self.backend_generation
    }

    pub const fn catalog_version(&self) -> Version {
        self.catalog_version
    }
}

impl TryFrom<proto::ShardContext> for ShardRequestContext {
    type Error = ProtocolError;

    fn try_from(wire: proto::ShardContext) -> Result<Self, Self::Error> {
        let request = wire
            .request
            .ok_or(ProtocolError::MissingContext)?
            .try_into()?;
        let graph_id = GraphId::new(wire.graph_id).map_err(|_| ProtocolError::ZeroGraph)?;
        let shard_id =
            ShardId::new(u64::from(wire.shard_id)).map_err(|_| ProtocolError::ZeroShard)?;
        let placement_epoch =
            PlacementEpoch::new(wire.placement_epoch).map_err(|_| ProtocolError::ZeroEpoch)?;
        let backend_generation = BackendGeneration::new(wire.backend_generation)
            .map_err(|_| ProtocolError::ZeroGeneration)?;
        if wire.catalog_version == 0 {
            return Err(ProtocolError::ZeroCatalog);
        }
        Ok(Self {
            request,
            graph_id,
            shard_id,
            placement_epoch,
            backend_generation,
            catalog_version: Version::new(wire.catalog_version),
        })
    }
}

pub(crate) fn exact_u128(bytes: &[u8]) -> Result<u128, ProtocolError> {
    let bytes: [u8; 16] = bytes
        .try_into()
        .map_err(|_| ProtocolError::IdentifierLength)?;
    Ok(u128::from_be_bytes(bytes))
}

fn exact_u64(bytes: &[u8]) -> Result<u64, ProtocolError> {
    let bytes: [u8; 8] = bytes
        .try_into()
        .map_err(|_| ProtocolError::IdentifierLength)?;
    Ok(u64::from_be_bytes(bytes))
}
