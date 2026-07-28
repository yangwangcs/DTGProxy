#![forbid(unsafe_code)]

mod error;
mod id;
mod time;
mod value;
mod version;

pub use error::{ErrorClass, KernelError, RetryClass};
pub use id::{
    BackendGeneration, ClusterId, GraphId, PlacementEpoch, ReplicaId, ShardId, TransactionId,
};
pub use time::{TransactionTime, ValidInterval};
pub use value::Value;
pub use version::{Digest32, Version};

pub const CLEAN_BREAK_ARCHITECTURE_VERSION: u32 = 1;
