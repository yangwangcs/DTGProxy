#![forbid(unsafe_code)]

mod auth;
mod client;
mod codec;
mod read_view;
mod reference_server;
mod snapshot;
mod wire;

pub use auth::RemoteAuthToken;
pub use client::{RemoteError, StorageRemoteClient};
pub use reference_server::{
    ReferenceServerConfig, ReferenceStorageServer, ReferenceStorageTckFactory,
};

pub const CLEAN_BREAK_ARCHITECTURE_VERSION: u32 = 1;
