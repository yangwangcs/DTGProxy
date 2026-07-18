use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use raft_transport::{RaftRoute, RoutedRaftMessage};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;

use crate::{DataNodeHost, RaftNetworkError, SharedRaftTransport};

const MAX_OUTBOUND_PER_REPLICA: usize = 1_024;

pub struct DataRaftRuntime {
    shutdown: watch::Sender<bool>,
    join: JoinHandle<Result<(), DataRaftRuntimeError>>,
    transport: Arc<SharedRaftTransport>,
}

impl DataRaftRuntime {
    pub async fn start(
        host: Arc<DataNodeHost>,
        listen_address: SocketAddr,
        peers: &BTreeMap<u64, SocketAddr>,
        queue_capacity: usize,
        tick_interval: Duration,
    ) -> Result<Self, DataRaftRuntimeError> {
        if tick_interval.is_zero() {
            return Err(DataRaftRuntimeError::InvalidTickInterval);
        }
        let mut transport = SharedRaftTransport::bind(
            *host.identity().cluster_id(),
            host.identity().node_id(),
            listen_address,
            queue_capacity,
        )
        .await?;
        for (&node_id, &address) in peers {
            transport.add_peer(node_id, address)?;
        }
        let transport = Arc::new(transport);
        let (shutdown, receiver) = watch::channel(false);
        let join = tokio::spawn(run(host, Arc::clone(&transport), receiver, tick_interval));
        Ok(Self {
            shutdown,
            join,
            transport,
        })
    }

    #[must_use]
    pub fn local_addr(&self) -> SocketAddr {
        self.transport.local_addr()
    }

    pub async fn shutdown(self) -> Result<(), DataRaftRuntimeError> {
        let _ = self.shutdown.send(true);
        match self.join.await {
            Ok(result) => result?,
            Err(error) => return Err(DataRaftRuntimeError::Join(error.to_string())),
        }
        let transport =
            Arc::try_unwrap(self.transport).map_err(|_| DataRaftRuntimeError::TransportRetained)?;
        transport.shutdown().await?;
        Ok(())
    }
}

async fn run(
    host: Arc<DataNodeHost>,
    transport: Arc<SharedRaftTransport>,
    mut shutdown: watch::Receiver<bool>,
    tick_interval: Duration,
) -> Result<(), DataRaftRuntimeError> {
    let mut ticker = tokio::time::interval(tick_interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return Ok(());
                }
            }
            incoming = transport.receive() => {
                match incoming {
                    Ok(message) => {
                        let _ = host.step_routed(message).await;
                    }
                    Err(RaftNetworkError::Stopped) if *shutdown.borrow() => return Ok(()),
                    Err(error) => return Err(error.into()),
                }
            }
            _ = ticker.tick() => {
                pump_outbound(&host, &transport).await;
            }
        }
    }
}

async fn pump_outbound(host: &DataNodeHost, transport: &SharedRaftTransport) {
    let Ok(keys) = host.replica_keys() else {
        return;
    };
    for key in keys {
        let _ = host.try_tick(key);
        let Ok(status) = host.status(key).await else {
            continue;
        };
        let Ok(messages) = host.take_outbound(key, MAX_OUTBOUND_PER_REPLICA).await else {
            continue;
        };
        let Ok(route) = RaftRoute::new(
            *host.identity().cluster_id(),
            key.graph_id(),
            key.shard_id(),
            status.placement_epoch(),
        ) else {
            continue;
        };
        for message in messages {
            let Ok(routed) = RoutedRaftMessage::new(route.clone(), message) else {
                continue;
            };
            let _ = transport.try_send(routed);
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DataRaftRuntimeError {
    Network(RaftNetworkError),
    InvalidTickInterval,
    Join(String),
    TransportRetained,
}

impl Display for DataRaftRuntimeError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Network(error) => write!(formatter, "Data Raft network error: {error}"),
            Self::InvalidTickInterval => formatter.write_str("Raft tick interval must be non-zero"),
            Self::Join(message) => write!(formatter, "Data Raft runtime join error: {message}"),
            Self::TransportRetained => formatter.write_str("Data Raft transport is still retained"),
        }
    }
}

impl Error for DataRaftRuntimeError {}

impl From<RaftNetworkError> for DataRaftRuntimeError {
    fn from(error: RaftNetworkError) -> Self {
        Self::Network(error)
    }
}
