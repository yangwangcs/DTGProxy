#![forbid(unsafe_code)]

mod error;
mod framing;
mod handshake;
mod message;
mod packstream;

pub use error::ProtocolError;
pub use framing::{ChunkDecoder, encode_chunks};
pub use handshake::{BOLT_MAGIC, BoltVersion, HANDSHAKE_BYTES, Handshake, negotiate};
pub use message::{ClientMessage, decode_client_message, encode_client_message};
pub use packstream::{PackStreamLimits, Value, decode, decode_with_limits, encode};
