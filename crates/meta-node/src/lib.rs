#![forbid(unsafe_code)]

mod raft_host;
mod state_machine;

pub use raft_host::{MetaRaftError, MetaRaftReplica};
pub use state_machine::{
    CatalogEvent, MetaApplyReceipt, MetaStateError, MetaStateMachine, WatchBatch,
};
