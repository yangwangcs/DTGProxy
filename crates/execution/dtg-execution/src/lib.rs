#![forbid(unsafe_code)]

mod controller;
mod data;
mod gateway;
mod meta;
mod meta_raft;
mod write;

pub use dtg_analytics as analytics;
pub use dtg_cluster_v2 as cluster_protocol;
pub use dtg_control as control;
pub use dtg_plan as planning;
pub use dtg_shard as shard;
pub use dtg_storage as storage;
pub use dtg_transaction as transaction;

pub use controller::{ControlActionExecutor, ControllerExecution, ControllerExecutionBuilder};
pub use data::{
    DataExecution, DataExecutionBuilder, ExecutionBuildError, ProviderResolver,
    ProviderResolverSet, ResolvedReplicaStore,
};
pub use dtg_storage::{ProviderKind, ReplicaBinding, ReplicaStateStore, StorageError, StoreFuture};
pub use gateway::{
    GatewayAnalyticsState, GatewayCancellationToken, GatewayClusterRequest, GatewayExecution,
    GatewayExecutionBuilder, GatewayExecutionError, GatewayExecutionTransport,
    GatewayExecutionTransportFactory, GatewayFuture, GatewayOperation, GatewayProtocolV2Client,
    GatewayProtocolV2Transport, GatewayRequestContext, GatewayResponse, GatewayRetry, GatewayRows,
    GatewayTemporalMode, GatewayTime, GatewayValue, TonicGatewayProtocolV2TransportFactory,
    encode_physical_fragment_body,
};
pub use meta::{MetaExecution, MetaExecutionBuilder};
pub use meta_raft::{
    MetaCommittedEntry, MetaRaftError, MetaRaftHost, MetaRaftProgress, MetaRaftRole,
};
pub use write::{
    PHYSICAL_WRITE_VERSION, PhysicalWriteError, PhysicalWriteFragment, PhysicalWritePlan,
    PhysicalWritePlanner, RoutedWriteMutation, WritePlanningContext, WriteShardTarget,
};

pub const CLEAN_BREAK_ARCHITECTURE_VERSION: u32 = 1;
