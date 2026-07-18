use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cluster_protocol::CLUSTER_PROTOCOL_VERSION;
use cluster_protocol::proto::meta_service_client::MetaServiceClient;
use cluster_protocol::proto::{GetCatalogRequest, RequestContext, WatchCatalogRequest};
use control_plane::{CatalogCommand, CatalogState, GraphDefinition};
use shard_client::{RemoteReplica, RemoteShardClient, RemoteTopology};
use tokio::sync::watch;
use tokio_stream::StreamExt;
use tonic::Request;

use crate::RemoteGatewayService;

pub struct GatewayCatalogSnapshot {
    state: CatalogState,
    graph: GraphDefinition,
    topology: RemoteTopology,
}

impl GatewayCatalogSnapshot {
    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.state.revision()
    }

    #[must_use]
    pub const fn state(&self) -> &CatalogState {
        &self.state
    }

    #[must_use]
    pub const fn graph(&self) -> &GraphDefinition {
        &self.graph
    }

    #[must_use]
    pub const fn topology(&self) -> &RemoteTopology {
        &self.topology
    }

    #[must_use]
    pub fn into_parts(self) -> (CatalogState, GraphDefinition, RemoteTopology) {
        (self.state, self.graph, self.topology)
    }
}

pub struct GatewayCatalogRouter {
    cluster_id: [u8; 16],
    node_id: u64,
    graph_id: u64,
    meta_seeds: Vec<SocketAddr>,
    data_nodes: BTreeMap<u64, SocketAddr>,
    watch_timeout: Duration,
    request_sequence: AtomicU64,
}

impl GatewayCatalogRouter {
    pub fn new(
        cluster_id: [u8; 16],
        node_id: u64,
        graph_id: u64,
        meta_seeds: Vec<SocketAddr>,
        data_nodes: BTreeMap<u64, SocketAddr>,
        watch_timeout: Duration,
    ) -> Result<Self, GatewayCatalogError> {
        if cluster_id == [0; 16]
            || node_id == 0
            || graph_id == 0
            || meta_seeds.is_empty()
            || data_nodes.is_empty()
            || watch_timeout.is_zero()
        {
            return Err(GatewayCatalogError::InvalidConfiguration);
        }
        Ok(Self {
            cluster_id,
            node_id,
            graph_id,
            meta_seeds,
            data_nodes,
            watch_timeout,
            request_sequence: AtomicU64::new(1),
        })
    }

    pub async fn load(
        &self,
        minimum_revision: u64,
    ) -> Result<GatewayCatalogSnapshot, GatewayCatalogError> {
        let mut last_error = None;
        for endpoint in &self.meta_seeds {
            let mut client = match MetaServiceClient::connect(format!("http://{endpoint}")).await {
                Ok(client) => client,
                Err(error) => {
                    last_error = Some(error.to_string());
                    continue;
                }
            };
            let request = GetCatalogRequest {
                context: Some(self.request_context()?),
                minimum_revision,
            };
            match client.get_catalog(Request::new(request)).await {
                Ok(response) => {
                    let snapshot = response
                        .into_inner()
                        .snapshot
                        .ok_or(GatewayCatalogError::MissingSnapshot)?;
                    if snapshot.revision == 0
                        || crc32fast::hash(&snapshot.payload) != snapshot.checksum
                    {
                        return Err(GatewayCatalogError::CorruptSnapshot);
                    }
                    let state = CatalogState::decode_snapshot(&snapshot.payload)
                        .map_err(|error| GatewayCatalogError::Catalog(error.to_string()))?;
                    if state.revision() != snapshot.revision || state.revision() < minimum_revision
                    {
                        return Err(GatewayCatalogError::CorruptSnapshot);
                    }
                    return self.snapshot_from_state(state);
                }
                Err(error) => last_error = Some(error.to_string()),
            }
        }
        Err(GatewayCatalogError::MetaUnavailable(
            last_error.unwrap_or_else(|| "no Meta seed was reachable".into()),
        ))
    }

    pub async fn run_watch(
        &self,
        mut state: CatalogState,
        service: Arc<RemoteGatewayService>,
        shard_client: Arc<RemoteShardClient>,
        mut shutdown: watch::Receiver<bool>,
    ) {
        while !*shutdown.borrow() {
            let outcome = self
                .watch_once(
                    &mut state,
                    Arc::clone(&service),
                    Arc::clone(&shard_client),
                    &mut shutdown,
                )
                .await;
            if *shutdown.borrow() {
                return;
            }
            if outcome.is_err() {
                if let Ok(snapshot) = self.load(state.revision()).await {
                    let (new_state, graph, topology) = snapshot.into_parts();
                    if shard_client.install_topology(topology).is_ok()
                        && service.install_catalog(new_state.revision(), graph).is_ok()
                    {
                        state = new_state;
                    }
                }
                tokio::select! {
                    () = tokio::time::sleep(Duration::from_millis(100)) => {}
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() {
                            return;
                        }
                    }
                }
            }
        }
    }

    async fn watch_once(
        &self,
        state: &mut CatalogState,
        service: Arc<RemoteGatewayService>,
        shard_client: Arc<RemoteShardClient>,
        shutdown: &mut watch::Receiver<bool>,
    ) -> Result<(), GatewayCatalogError> {
        let mut last_error = None;
        for endpoint in &self.meta_seeds {
            let mut client = match MetaServiceClient::connect(format!("http://{endpoint}")).await {
                Ok(client) => client,
                Err(error) => {
                    last_error = Some(error.to_string());
                    continue;
                }
            };
            let request = WatchCatalogRequest {
                context: Some(self.request_context()?),
                after_revision: state.revision(),
            };
            let mut stream = match client.watch_catalog(Request::new(request)).await {
                Ok(response) => response.into_inner(),
                Err(error) => {
                    last_error = Some(error.to_string());
                    continue;
                }
            };
            loop {
                let event = tokio::select! {
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() {
                            return Ok(());
                        }
                        continue;
                    }
                    event = stream.next() => event,
                };
                let Some(event) = event else {
                    return Err(GatewayCatalogError::WatchEnded);
                };
                let event = event.map_err(|error| GatewayCatalogError::Watch(error.to_string()))?;
                if event.revision != state.revision().saturating_add(1)
                    || crc32fast::hash(&event.command) != event.checksum
                {
                    return Err(GatewayCatalogError::CorruptEvent);
                }
                let command = CatalogCommand::decode(&event.command)
                    .map_err(|error| GatewayCatalogError::Catalog(error.to_string()))?;
                let receipt = state
                    .apply(command)
                    .map_err(|error| GatewayCatalogError::Catalog(error.to_string()))?;
                if receipt.revision() != event.revision {
                    return Err(GatewayCatalogError::CorruptEvent);
                }
                let snapshot = self.snapshot_from_state(state.clone())?;
                let (_, graph, topology) = snapshot.into_parts();
                shard_client
                    .install_topology(topology)
                    .map_err(|error| GatewayCatalogError::Topology(error.to_string()))?;
                service
                    .install_catalog(state.revision(), graph)
                    .map_err(|error| GatewayCatalogError::Topology(error.to_string()))?;
            }
        }
        Err(GatewayCatalogError::MetaUnavailable(
            last_error.unwrap_or_else(|| "no Meta seed was reachable".into()),
        ))
    }

    fn snapshot_from_state(
        &self,
        state: CatalogState,
    ) -> Result<GatewayCatalogSnapshot, GatewayCatalogError> {
        let graph =
            state
                .graph(self.graph_id)
                .cloned()
                .ok_or(GatewayCatalogError::GraphMissing {
                    graph_id: self.graph_id,
                })?;
        let topology = build_remote_topology(state.revision(), &graph, &self.data_nodes)?;
        Ok(GatewayCatalogSnapshot {
            state,
            graph,
            topology,
        })
    }

    fn request_context(&self) -> Result<RequestContext, GatewayCatalogError> {
        let sequence = self.request_sequence.fetch_add(1, Ordering::Relaxed);
        let request_id = (u128::from(self.node_id) << 64) | u128::from(sequence.max(1));
        Ok(RequestContext {
            protocol_version: CLUSTER_PROTOCOL_VERSION,
            cluster_id: self.cluster_id.to_vec(),
            request_id: request_id.to_be_bytes().to_vec(),
            deadline_unix_ms: unix_time_ms()?
                .checked_add(
                    u64::try_from(self.watch_timeout.as_millis())
                        .map_err(|_| GatewayCatalogError::ClockOverflow)?,
                )
                .ok_or(GatewayCatalogError::ClockOverflow)?,
        })
    }
}

pub fn build_remote_topology(
    revision: u64,
    graph: &GraphDefinition,
    data_nodes: &BTreeMap<u64, SocketAddr>,
) -> Result<RemoteTopology, GatewayCatalogError> {
    let routes = graph
        .topology()
        .placements()
        .iter()
        .map(|placement| {
            let replicas = placement
                .voters()
                .iter()
                .map(|node_id| {
                    let address = data_nodes
                        .get(node_id)
                        .copied()
                        .ok_or(GatewayCatalogError::DataNodeMissing { node_id: *node_id })?;
                    RemoteReplica::new(*node_id, address)
                        .map_err(|error| GatewayCatalogError::Topology(error.to_string()))
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok((
                placement.shard_id(),
                placement.epoch(),
                placement.voters()[0],
                replicas,
            ))
        })
        .collect::<Result<Vec<_>, GatewayCatalogError>>()?;
    RemoteTopology::new(revision, graph.graph_id(), routes)
        .map_err(|error| GatewayCatalogError::Topology(error.to_string()))
}

fn unix_time_ms() -> Result<u64, GatewayCatalogError> {
    let milliseconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| GatewayCatalogError::ClockOverflow)?
        .as_millis();
    u64::try_from(milliseconds).map_err(|_| GatewayCatalogError::ClockOverflow)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GatewayCatalogError {
    InvalidConfiguration,
    MetaUnavailable(String),
    MissingSnapshot,
    CorruptSnapshot,
    CorruptEvent,
    WatchEnded,
    Watch(String),
    GraphMissing { graph_id: u64 },
    DataNodeMissing { node_id: u64 },
    Catalog(String),
    Topology(String),
    ClockOverflow,
}

impl Display for GatewayCatalogError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfiguration => {
                formatter.write_str("invalid Catalog router configuration")
            }
            Self::MetaUnavailable(message) => write!(formatter, "Meta unavailable: {message}"),
            Self::MissingSnapshot => formatter.write_str("Meta response omitted Catalog snapshot"),
            Self::CorruptSnapshot => formatter.write_str("Catalog snapshot integrity check failed"),
            Self::CorruptEvent => formatter.write_str("Catalog event integrity check failed"),
            Self::WatchEnded => formatter.write_str("Catalog watch ended"),
            Self::Watch(message) => write!(formatter, "Catalog watch failed: {message}"),
            Self::GraphMissing { graph_id } => {
                write!(formatter, "graph {graph_id} is absent from Catalog")
            }
            Self::DataNodeMissing { node_id } => {
                write!(formatter, "Data node {node_id} has no configured address")
            }
            Self::Catalog(message) => write!(formatter, "Catalog error: {message}"),
            Self::Topology(message) => write!(formatter, "topology error: {message}"),
            Self::ClockOverflow => formatter.write_str("system clock overflow"),
        }
    }
}

impl Error for GatewayCatalogError {}
