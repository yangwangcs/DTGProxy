#![forbid(unsafe_code)]

mod config;
mod provider;
mod service;

pub use config::{CredentialProfile, DataConfigError, DataProcessConfig, EndpointProfile};
pub use provider::{FjallResolver, Neo4jResolver, PostgresResolver, RemoteResolver};
pub use service::{
    AssignmentUpdate, DataMetrics, DataNode, DataNodeBuilder, DataNodeError, DataRpcService,
    LifecycleState, RaftTransport, ReplicaFailure,
};

pub const CLEAN_BREAK_ARCHITECTURE_VERSION: u32 = 1;
