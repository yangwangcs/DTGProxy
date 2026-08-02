#![forbid(unsafe_code)]

mod config;
mod provider;
mod raft_transport;
mod service;

pub use config::{CredentialProfile, DataConfigError, DataProcessConfig, EndpointProfile};
pub use provider::{FjallResolver, KuzuResolver, PostgresResolver, RemoteResolver};
pub use raft_transport::TonicRaftTransport;
pub use service::{
    AssignmentUpdate, DataMetrics, DataNode, DataNodeBuilder, DataNodeError, DataRpcService,
    LifecycleState, RaftTransport, ReplicaFailure,
};

pub const CLEAN_BREAK_ARCHITECTURE_VERSION: u32 = 1;
