use std::collections::{BTreeMap, VecDeque};
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use bolt_protocol::{
    BOLT_MAGIC, BoltVersion, ChunkDecoder, ClientMessage, Value, decode, encode, encode_chunks,
    encode_client_message,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const MAX_WARMUPS: usize = 10_000;
const MAX_SAMPLES: usize = 10_000;
const MAX_MESSAGE_BYTES: usize = 16 << 20;
const READ_BUFFER_BYTES: usize = 16 << 10;
const MAX_CHUNK_BYTES: usize = u16::MAX as usize;
const SUCCESS: u8 = 0x70;
const RECORD: u8 = 0x71;
const IGNORED: u8 = 0x7E;
const FAILURE: u8 = 0x7F;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExternalTtfrProbeConfig {
    address: SocketAddr,
    query: String,
    parameters: BTreeMap<String, Value>,
    warmup_count: usize,
    sample_count: usize,
    operation_timeout: Duration,
}

impl ExternalTtfrProbeConfig {
    pub fn new(
        address: SocketAddr,
        query: impl Into<String>,
        parameters: BTreeMap<String, Value>,
        warmup_count: usize,
        sample_count: usize,
        operation_timeout: Duration,
    ) -> Result<Self, ExternalTtfrProbeError> {
        let query = query.into();
        if query.trim().is_empty()
            || warmup_count > MAX_WARMUPS
            || sample_count == 0
            || sample_count > MAX_SAMPLES
            || warmup_count.checked_add(sample_count).is_none()
            || operation_timeout.is_zero()
        {
            return Err(ExternalTtfrProbeError::InvalidConfiguration);
        }
        Ok(Self {
            address,
            query,
            parameters,
            warmup_count,
            sample_count,
            operation_timeout,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExternalTtfrSample {
    ttfr: Duration,
    total_latency: Duration,
    result_digest: [u8; 32],
    row_count: usize,
}

impl ExternalTtfrSample {
    #[must_use]
    pub const fn ttfr(&self) -> Duration {
        self.ttfr
    }

    #[must_use]
    pub const fn total_latency(&self) -> Duration {
        self.total_latency
    }

    #[must_use]
    pub const fn result_digest(&self) -> [u8; 32] {
        self.result_digest
    }

    #[must_use]
    pub const fn row_count(&self) -> usize {
        self.row_count
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExternalTtfrProbeReport {
    samples: Vec<ExternalTtfrSample>,
    result_digest: [u8; 32],
    row_count: usize,
}

impl ExternalTtfrProbeReport {
    #[must_use]
    pub fn samples(&self) -> &[ExternalTtfrSample] {
        &self.samples
    }

    #[must_use]
    pub const fn result_digest(&self) -> [u8; 32] {
        self.result_digest
    }

    #[must_use]
    pub const fn row_count(&self) -> usize {
        self.row_count
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExternalTtfrProbeError {
    InvalidConfiguration,
    UnsupportedVersion,
    Io(ErrorKind),
    Timeout,
    Protocol(String),
    ServerFailure { code: String, message: String },
    EmptyResult,
    ResultMismatch,
}

pub struct BoltProbeSession {
    stream: TcpStream,
    receiver: Receiver,
    operation_timeout: Duration,
    run_extra: BTreeMap<String, Value>,
    usable: bool,
}

impl BoltProbeSession {
    pub async fn connect(
        address: SocketAddr,
        operation_timeout: Duration,
    ) -> Result<Self, ExternalTtfrProbeError> {
        Self::connect_with_benchmark_session(address, operation_timeout, None).await
    }

    pub async fn connect_with_benchmark_session(
        address: SocketAddr,
        operation_timeout: Duration,
        benchmark_session: Option<String>,
    ) -> Result<Self, ExternalTtfrProbeError> {
        if operation_timeout.is_zero() {
            return Err(ExternalTtfrProbeError::InvalidConfiguration);
        }
        tokio::time::timeout(
            operation_timeout,
            Self::connect_unbounded(address, operation_timeout, benchmark_session),
        )
        .await
        .map_err(|_| ExternalTtfrProbeError::Timeout)?
    }

    pub async fn execute(
        &mut self,
        query: &str,
        parameters: BTreeMap<String, Value>,
    ) -> Result<ExternalTtfrSample, ExternalTtfrProbeError> {
        if !self.usable || query.trim().is_empty() {
            return Err(ExternalTtfrProbeError::InvalidConfiguration);
        }
        let result = tokio::time::timeout(
            self.operation_timeout,
            self.execute_unbounded(query, parameters),
        )
        .await
        .map_err(|_| ExternalTtfrProbeError::Timeout)
        .and_then(|result| result);
        if result.is_err() {
            self.usable = false;
        }
        result
    }

    pub async fn goodbye(mut self) -> Result<(), ExternalTtfrProbeError> {
        if !self.usable {
            return Err(ExternalTtfrProbeError::Protocol(
                "cannot send GOODBYE on an unusable Bolt session".into(),
            ));
        }
        tokio::time::timeout(
            self.operation_timeout,
            send(&mut self.stream, &ClientMessage::Goodbye),
        )
        .await
        .map_err(|_| ExternalTtfrProbeError::Timeout)??;
        self.usable = false;
        Ok(())
    }

    async fn connect_unbounded(
        address: SocketAddr,
        operation_timeout: Duration,
        benchmark_session: Option<String>,
    ) -> Result<Self, ExternalTtfrProbeError> {
        let mut stream = TcpStream::connect(address).await?;
        stream.set_nodelay(true)?;
        negotiate_version(&mut stream).await?;
        let mut receiver = Receiver::new()?;
        send(
            &mut stream,
            &ClientMessage::Hello(BTreeMap::from([(
                "user_agent".into(),
                Value::String("DTGProxy-TTFR-Probe/1.1".into()),
            )])),
        )
        .await?;
        expect_success(receiver.receive(&mut stream).await?, "HELLO")?;
        Ok(Self {
            stream,
            receiver,
            operation_timeout,
            run_extra: benchmark_session.map_or_else(BTreeMap::new, |session| {
                BTreeMap::from([("dtgproxy.paper.session".to_owned(), Value::String(session))])
            }),
            usable: true,
        })
    }

    async fn execute_unbounded(
        &mut self,
        query: &str,
        parameters: BTreeMap<String, Value>,
    ) -> Result<ExternalTtfrSample, ExternalTtfrProbeError> {
        let run = framed(&ClientMessage::Run {
            query: query.to_owned(),
            parameters,
            extra: self.run_extra.clone(),
        })?;
        let pull = framed(&ClientMessage::Pull {
            n: -1,
            query_id: None,
        })?;
        let started = Instant::now();
        self.stream.write_all(&run).await?;
        self.stream.flush().await?;
        expect_success(self.receiver.receive(&mut self.stream).await?, "RUN")?;
        self.stream.write_all(&pull).await?;
        self.stream.flush().await?;
        receive_records(&mut self.stream, &mut self.receiver, started).await
    }
}

impl Display for ExternalTtfrProbeError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "external Bolt TTFR probe failed: {self:?}")
    }
}

impl Error for ExternalTtfrProbeError {}

impl From<std::io::Error> for ExternalTtfrProbeError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error.kind())
    }
}

pub async fn probe_external_ttfr(
    config: ExternalTtfrProbeConfig,
) -> Result<ExternalTtfrProbeReport, ExternalTtfrProbeError> {
    for _ in 0..config.warmup_count {
        probe_once(&config).await?;
    }

    let mut samples = Vec::with_capacity(config.sample_count);
    for _ in 0..config.sample_count {
        samples.push(probe_once(&config).await?);
    }
    let first = samples.first().expect("configuration requires a sample");
    if samples.iter().any(|sample| {
        sample.row_count != first.row_count || sample.result_digest != first.result_digest
    }) {
        return Err(ExternalTtfrProbeError::ResultMismatch);
    }
    Ok(ExternalTtfrProbeReport {
        result_digest: first.result_digest,
        row_count: first.row_count,
        samples,
    })
}

async fn probe_once(
    config: &ExternalTtfrProbeConfig,
) -> Result<ExternalTtfrSample, ExternalTtfrProbeError> {
    tokio::time::timeout(config.operation_timeout, probe_once_unbounded(config))
        .await
        .map_err(|_| ExternalTtfrProbeError::Timeout)?
}

async fn probe_once_unbounded(
    config: &ExternalTtfrProbeConfig,
) -> Result<ExternalTtfrSample, ExternalTtfrProbeError> {
    let mut session =
        BoltProbeSession::connect_unbounded(config.address, config.operation_timeout, None).await?;
    let result = session
        .execute_unbounded(&config.query, config.parameters.clone())
        .await;
    let _ = send(&mut session.stream, &ClientMessage::Goodbye).await;
    result
}

async fn negotiate_version(stream: &mut TcpStream) -> Result<(), ExternalTtfrProbeError> {
    let version = BoltVersion::new(5, 8, 0);
    let mut handshake = Vec::from(BOLT_MAGIC.to_be_bytes());
    handshake.extend_from_slice(&version.encode());
    handshake.extend_from_slice(&[0; 12]);
    stream.write_all(&handshake).await?;
    stream.flush().await?;
    let mut selected = [0; 4];
    stream.read_exact(&mut selected).await?;
    if selected != version.encode() {
        return Err(ExternalTtfrProbeError::UnsupportedVersion);
    }
    Ok(())
}

async fn receive_records(
    stream: &mut TcpStream,
    receiver: &mut Receiver,
    started: Instant,
) -> Result<ExternalTtfrSample, ExternalTtfrProbeError> {
    let mut ttfr = None;
    let mut row_count = 0_usize;
    let mut digest = blake3::Hasher::new();
    loop {
        let payload = receiver.receive(stream).await?;
        let value = decode(&payload).map_err(protocol)?;
        let Value::Structure { signature, fields } = &value else {
            return Err(ExternalTtfrProbeError::Protocol(
                "server response is not a Bolt structure".into(),
            ));
        };
        match *signature {
            RECORD => {
                if !matches!(fields.as_slice(), [Value::List(_)]) {
                    return Err(ExternalTtfrProbeError::Protocol(
                        "RECORD must contain one list field".into(),
                    ));
                }
                ttfr.get_or_insert_with(|| started.elapsed());
                row_count = row_count.checked_add(1).ok_or_else(|| {
                    ExternalTtfrProbeError::Protocol("record count overflow".into())
                })?;
                let canonical = encode(&value).map_err(protocol)?;
                digest.update(&(canonical.len() as u64).to_be_bytes());
                digest.update(&canonical);
            }
            SUCCESS => {
                let metadata = one_metadata(fields, "PULL SUCCESS")?;
                if metadata.get("has_more") == Some(&Value::Boolean(true)) {
                    return Err(ExternalTtfrProbeError::Protocol(
                        "PULL -1 returned an incomplete result".into(),
                    ));
                }
                let Some(ttfr) = ttfr else {
                    return Err(ExternalTtfrProbeError::EmptyResult);
                };
                return Ok(ExternalTtfrSample {
                    ttfr,
                    total_latency: started.elapsed(),
                    result_digest: *digest.finalize().as_bytes(),
                    row_count,
                });
            }
            FAILURE => return Err(server_failure(fields)?),
            IGNORED => {
                return Err(ExternalTtfrProbeError::Protocol(
                    "server ignored PULL".into(),
                ));
            }
            other => {
                return Err(ExternalTtfrProbeError::Protocol(format!(
                    "unexpected Bolt response signature 0x{other:02x}"
                )));
            }
        }
    }
}

fn expect_success(payload: Vec<u8>, phase: &str) -> Result<(), ExternalTtfrProbeError> {
    let value = decode(&payload).map_err(protocol)?;
    let Value::Structure { signature, fields } = &value else {
        return Err(ExternalTtfrProbeError::Protocol(format!(
            "{phase} response is not a Bolt structure"
        )));
    };
    match *signature {
        SUCCESS => {
            one_metadata(fields, phase)?;
            Ok(())
        }
        FAILURE => Err(server_failure(fields)?),
        IGNORED => Err(ExternalTtfrProbeError::Protocol(format!(
            "server ignored {phase}"
        ))),
        other => Err(ExternalTtfrProbeError::Protocol(format!(
            "unexpected {phase} response signature 0x{other:02x}"
        ))),
    }
}

fn one_metadata<'a>(
    fields: &'a [Value],
    phase: &str,
) -> Result<&'a BTreeMap<String, Value>, ExternalTtfrProbeError> {
    let [Value::Map(metadata)] = fields else {
        return Err(ExternalTtfrProbeError::Protocol(format!(
            "{phase} must contain one metadata map"
        )));
    };
    Ok(metadata)
}

fn server_failure(fields: &[Value]) -> Result<ExternalTtfrProbeError, ExternalTtfrProbeError> {
    let metadata = one_metadata(fields, "FAILURE")?;
    let code = match metadata.get("code") {
        Some(Value::String(code)) => code.clone(),
        _ => {
            return Err(ExternalTtfrProbeError::Protocol(
                "FAILURE metadata is missing string code".into(),
            ));
        }
    };
    let message = match metadata.get("message") {
        Some(Value::String(message)) => message.clone(),
        _ => {
            return Err(ExternalTtfrProbeError::Protocol(
                "FAILURE metadata is missing string message".into(),
            ));
        }
    };
    Ok(ExternalTtfrProbeError::ServerFailure { code, message })
}

async fn send(
    stream: &mut TcpStream,
    message: &ClientMessage,
) -> Result<(), ExternalTtfrProbeError> {
    let bytes = framed(message)?;
    stream.write_all(&bytes).await?;
    stream.flush().await?;
    Ok(())
}

fn framed(message: &ClientMessage) -> Result<Vec<u8>, ExternalTtfrProbeError> {
    let payload = encode_client_message(message).map_err(protocol)?;
    encode_chunks(&payload, MAX_CHUNK_BYTES).map_err(protocol)
}

fn protocol(error: impl Display) -> ExternalTtfrProbeError {
    ExternalTtfrProbeError::Protocol(error.to_string())
}

struct Receiver {
    decoder: ChunkDecoder,
    pending: VecDeque<Vec<u8>>,
    buffer: Vec<u8>,
}

impl Receiver {
    fn new() -> Result<Self, ExternalTtfrProbeError> {
        Ok(Self {
            decoder: ChunkDecoder::new(MAX_MESSAGE_BYTES, MAX_CHUNK_BYTES).map_err(protocol)?,
            pending: VecDeque::new(),
            buffer: vec![0; READ_BUFFER_BYTES],
        })
    }

    async fn receive(&mut self, stream: &mut TcpStream) -> Result<Vec<u8>, ExternalTtfrProbeError> {
        loop {
            if let Some(payload) = self.pending.pop_front() {
                if payload.is_empty() {
                    return Err(ExternalTtfrProbeError::Protocol(
                        "Bolt message payload cannot be empty".into(),
                    ));
                }
                return Ok(payload);
            }
            let read = stream.read(&mut self.buffer).await?;
            if read == 0 {
                return Err(ExternalTtfrProbeError::Protocol(
                    "connection closed before the Bolt summary".into(),
                ));
            }
            self.pending
                .extend(self.decoder.push(&self.buffer[..read]).map_err(protocol)?);
        }
    }
}
