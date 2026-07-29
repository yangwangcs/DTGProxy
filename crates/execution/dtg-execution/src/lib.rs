#![forbid(unsafe_code)]

mod controller;
mod data;
mod gateway;
mod meta;

pub use controller::{ControllerExecution, ControllerExecutionBuilder};
pub use data::{
    DataExecution, DataExecutionBuilder, ExecutionBuildError, ProviderResolver, ProviderResolverSet,
};
pub use dtg_storage::{ProviderKind, ReplicaBinding, ReplicaStateStore, StorageError, StoreFuture};
pub use gateway::{GatewayExecution, GatewayExecutionBuilder};
pub use meta::{MetaExecution, MetaExecutionBuilder};

pub const CLEAN_BREAK_ARCHITECTURE_VERSION: u32 = 1;
