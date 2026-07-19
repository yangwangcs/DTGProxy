use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::io::ErrorKind;
use std::sync::Arc;

use bolt_protocol::{
    BoltVersion, ChunkDecoder, HANDSHAKE_BYTES, Handshake, Value, decode_client_message, encode,
    encode_chunks, negotiate,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::{BoltMachine, BoltService, ConnectionState, ServerMessage};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BoltConnectionConfig {
    supported_versions: Vec<BoltVersion>,
    max_message_bytes: usize,
    max_chunk_bytes: usize,
    read_buffer_bytes: usize,
}

impl BoltConnectionConfig {
    pub fn new(
        supported_versions: Vec<BoltVersion>,
        max_message_bytes: usize,
        max_chunk_bytes: usize,
        read_buffer_bytes: usize,
    ) -> Result<Self, ConnectionError> {
        if supported_versions.is_empty()
            || supported_versions.iter().any(|version| version.is_zero())
            || max_message_bytes == 0
            || max_chunk_bytes == 0
            || max_chunk_bytes > usize::from(u16::MAX)
            || read_buffer_bytes == 0
            || read_buffer_bytes > max_message_bytes
        {
            return Err(ConnectionError::InvalidConfiguration);
        }
        Ok(Self {
            supported_versions,
            max_message_bytes,
            max_chunk_bytes,
            read_buffer_bytes,
        })
    }
}

impl Default for BoltConnectionConfig {
    fn default() -> Self {
        Self {
            supported_versions: vec![BoltVersion::new(5, 8, 0)],
            max_message_bytes: 16 << 20,
            max_chunk_bytes: usize::from(u16::MAX),
            read_buffer_bytes: 16 << 10,
        }
    }
}

pub async fn serve_connection<S, T>(
    stream: &mut T,
    service: Arc<S>,
    config: BoltConnectionConfig,
) -> Result<(), ConnectionError>
where
    S: BoltService + 'static,
    T: AsyncRead + AsyncWrite + Unpin,
{
    let mut handshake_bytes = [0; HANDSHAKE_BYTES];
    stream.read_exact(&mut handshake_bytes).await?;
    let handshake = Handshake::decode(&handshake_bytes)
        .map_err(|error| ConnectionError::Protocol(error.to_string()))?;
    let selected = negotiate(handshake.proposals(), &config.supported_versions);
    stream
        .write_all(
            &selected
                .unwrap_or_else(|| BoltVersion::new(0, 0, 0))
                .encode(),
        )
        .await?;
    stream.flush().await?;
    if selected.is_none() {
        return Err(ConnectionError::UnsupportedVersion);
    }

    let mut decoder = ChunkDecoder::new(config.max_message_bytes, config.max_chunk_bytes)
        .map_err(|error| ConnectionError::Protocol(error.to_string()))?;
    let mut machine = BoltMachine::new(service);
    let mut buffer = vec![0; config.read_buffer_bytes];
    loop {
        let read = stream.read(&mut buffer).await?;
        if read == 0 {
            return Ok(());
        }
        let messages = decoder
            .push(&buffer[..read])
            .map_err(|error| ConnectionError::Protocol(error.to_string()))?;
        for payload in messages {
            if payload.is_empty() {
                return Err(ConnectionError::Protocol(
                    "Bolt message payload cannot be empty".into(),
                ));
            }
            let message = decode_client_message(&payload)
                .map_err(|error| ConnectionError::Protocol(error.to_string()))?;
            let responses = machine.handle(message).await;
            for response in responses {
                let payload = encode_server_message(response)?;
                let framed = encode_chunks(&payload, config.max_chunk_bytes)
                    .map_err(|error| ConnectionError::Protocol(error.to_string()))?;
                stream.write_all(&framed).await?;
            }
            stream.flush().await?;
            if machine.state() == ConnectionState::Defunct {
                return Ok(());
            }
        }
    }
}

fn encode_server_message(message: ServerMessage) -> Result<Vec<u8>, ConnectionError> {
    let value = match message {
        ServerMessage::Success(metadata) => Value::Structure {
            signature: 0x70,
            fields: vec![Value::Map(metadata)],
        },
        ServerMessage::Record(values) => Value::Structure {
            signature: 0x71,
            fields: vec![Value::List(values)],
        },
        ServerMessage::Ignored => Value::Structure {
            signature: 0x7E,
            fields: Vec::new(),
        },
        ServerMessage::Failure { code, message } => Value::Structure {
            signature: 0x7F,
            fields: vec![Value::Map(BTreeMap::from([
                ("code".into(), Value::String(code)),
                ("message".into(), Value::String(message)),
            ]))],
        },
    };
    encode(&value).map_err(|error| ConnectionError::Protocol(error.to_string()))
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConnectionError {
    InvalidConfiguration,
    UnsupportedVersion,
    Io(ErrorKind),
    Protocol(String),
}

impl Display for ConnectionError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "Bolt connection failed: {self:?}")
    }
}

impl Error for ConnectionError {}

impl From<std::io::Error> for ConnectionError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value.kind())
    }
}
