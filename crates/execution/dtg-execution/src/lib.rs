#![forbid(unsafe_code)]

mod controller;
mod data;
mod gateway;
mod meta;

pub use dtg_analytics as analytics;
pub use dtg_cluster_v2 as cluster_protocol;
pub use dtg_control as control;
pub use dtg_storage as storage;
pub use dtg_transaction as transaction;

pub use controller::{ControllerExecution, ControllerExecutionBuilder};
pub use data::{
    DataExecution, DataExecutionBuilder, ExecutionBuildError, ProviderResolver, ProviderResolverSet,
};
pub use dtg_storage::{ProviderKind, ReplicaBinding, ReplicaStateStore, StorageError, StoreFuture};
pub use gateway::{GatewayExecution, GatewayExecutionBuilder};
pub use meta::{MetaExecution, MetaExecutionBuilder};

pub const CLEAN_BREAK_ARCHITECTURE_VERSION: u32 = 1;
