use std::collections::BTreeMap;
use std::fmt;
use std::io;
use std::sync::Arc;
use std::time::Duration;

use dtg_execution::{
    GatewayAnalyticsState, GatewayCancellationToken, GatewayExecutionError, GatewayOperation,
    GatewayRows, GatewayValue, RequestStage, RequestStageMetrics,
};

use crate::GatewayService;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const BOLT_MAGIC: [u8; 4] = [0x60, 0x60, 0xb0, 0x17];
const BOLT_V5_4: [u8; 4] = [0x00, 0x00, 0x04, 0x05];
const MAX_BOLT_MESSAGE_BYTES: usize = 8 * 1024 * 1024;

pub async fn serve_bolt(
    listener: TcpListener,
    service: Arc<GatewayService>,
) -> Result<(), io::Error> {
    loop {
        let (socket, _) = listener.accept().await?;
        let service = Arc::clone(&service);
        tokio::spawn(async move {
            let _ = serve_connection(socket, service).await;
        });
    }
}

async fn serve_connection(
    mut socket: TcpStream,
    service: Arc<GatewayService>,
) -> Result<(), io::Error> {
    negotiate(&mut socket).await?;
    let request_metrics = service.request_metrics();
    let mut pending_rows = None;
    loop {
        let Some(message) = read_chunked_message(&mut socket).await? else {
            return Ok(());
        };
        let timer = request_metrics.start(RequestStage::BoltDecode);
        let decoded = timer.finish_result(decode_message(&message))?;
        match decoded {
            BoltMessage::Hello => {
                write_success(&mut socket, &request_metrics, &[]).await?;
            }
            BoltMessage::Run {
                statement,
                parameters,
            } => {
                let cancellation = GatewayCancellationToken::new();
                match service
                    .execute_statement(statement, parameters, None, &cancellation)
                    .await
                {
                    Ok(dtg_execution::GatewayResponse::Rows(rows))
                    | Ok(dtg_execution::GatewayResponse::AnalyticsResult { rows, .. }) => {
                        let fields = rows.fields().to_vec();
                        pending_rows = Some(rows);
                        write_success(
                            &mut socket,
                            &request_metrics,
                            &[("fields", PackValue::Strings(fields))],
                        )
                        .await?;
                    }
                    Ok(_) => {
                        pending_rows = None;
                        write_success(&mut socket, &request_metrics, &[]).await?;
                    }
                    Err(error) => {
                        pending_rows = None;
                        write_failure(&mut socket, &request_metrics, &error).await?;
                    }
                }
            }
            BoltMessage::Pull => {
                if let Some(rows) = pending_rows.take() {
                    for row in rows.rows() {
                        write_record(&mut socket, &request_metrics, row).await?;
                    }
                }
                write_success(
                    &mut socket,
                    &request_metrics,
                    &[("has_more", PackValue::Boolean(false))],
                )
                .await?;
            }
            BoltMessage::Goodbye => return Ok(()),
            BoltMessage::Reset => {
                pending_rows = None;
                write_success(&mut socket, &request_metrics, &[]).await?;
            }
            BoltMessage::Unsupported => {
                write_failure(
                    &mut socket,
                    &request_metrics,
                    &BoltError::protocol("unsupported Bolt message signature"),
                )
                .await?;
            }
        }
    }
}

enum BoltMessage {
    Hello,
    Run {
        statement: String,
        parameters: BTreeMap<String, GatewayValue>,
    },
    Pull,
    Goodbye,
    Reset,
    Unsupported,
}

fn decode_message(message: &[u8]) -> Result<BoltMessage, io::Error> {
    let mut decoder = PackDecoder::new(message);
    let (fields, signature) = decoder.structure()?;
    let decoded = match signature {
        0x01 if fields == 1 => {
            decoder.skip_value()?;
            BoltMessage::Hello
        }
        0x10 if fields == 3 => {
            let statement = decoder.string()?.to_owned();
            let parameters = decoder.gateway_map()?;
            decoder.skip_value()?;
            BoltMessage::Run {
                statement,
                parameters,
            }
        }
        0x3f if fields == 1 => {
            decoder.skip_value()?;
            BoltMessage::Pull
        }
        0x0f if fields == 0 => BoltMessage::Goodbye,
        0x02 if fields == 0 => BoltMessage::Reset,
        _ => BoltMessage::Unsupported,
    };
    if matches!(decoded, BoltMessage::Unsupported) {
        return Ok(decoded);
    }
    decoder.finish()?;
    Ok(decoded)
}

async fn negotiate(socket: &mut TcpStream) -> Result<(), io::Error> {
    let mut handshake = [0_u8; 20];
    socket.read_exact(&mut handshake).await?;
    if handshake[..4] != BOLT_MAGIC {
        socket.write_all(&[0; 4]).await?;
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid Bolt handshake magic",
        ));
    }
    let supported = handshake[4..]
        .chunks_exact(4)
        .any(|proposal| proposal == BOLT_V5_4 || proposal == [0, 0, 0, 5]);
    socket
        .write_all(if supported { &BOLT_V5_4 } else { &[0; 4] })
        .await?;
    if supported {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "no supported Bolt version was proposed",
        ))
    }
}

async fn read_chunked_message(socket: &mut TcpStream) -> Result<Option<Vec<u8>>, io::Error> {
    let mut message = Vec::new();
    loop {
        let mut length = [0_u8; 2];
        match socket.read_exact(&mut length).await {
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof && message.is_empty() => {
                return Ok(None);
            }
            Err(error) => return Err(error),
        }
        let length = usize::from(u16::from_be_bytes(length));
        if length == 0 {
            return Ok(Some(message));
        }
        let new_len = message
            .len()
            .checked_add(length)
            .filter(|length| *length <= MAX_BOLT_MESSAGE_BYTES)
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "Bolt message is oversized")
            })?;
        let start = message.len();
        message.resize(new_len, 0);
        socket.read_exact(&mut message[start..]).await?;
    }
}

async fn write_chunked_message(socket: &mut TcpStream, message: &[u8]) -> Result<(), io::Error> {
    for chunk in message.chunks(usize::from(u16::MAX)) {
        socket
            .write_all(&(chunk.len() as u16).to_be_bytes())
            .await?;
        socket.write_all(chunk).await?;
    }
    socket.write_all(&[0, 0]).await
}

enum PackValue {
    Boolean(bool),
    Strings(Vec<String>),
}

async fn write_success(
    socket: &mut TcpStream,
    request_metrics: &Arc<RequestStageMetrics>,
    metadata: &[(&str, PackValue)],
) -> Result<(), io::Error> {
    let timer = request_metrics.start(RequestStage::BoltEncode);
    let encoded: Result<Vec<u8>, io::Error> = (|| {
        let mut message = vec![0xb1, 0x70];
        encode_tiny_map_len(metadata.len(), &mut message)?;
        for (key, value) in metadata {
            encode_string(key, &mut message)?;
            match value {
                PackValue::Boolean(value) => message.push(if *value { 0xc3 } else { 0xc2 }),
                PackValue::Strings(values) => {
                    encode_list_len(values.len(), &mut message)?;
                    for value in values {
                        encode_string(value, &mut message)?;
                    }
                }
            }
        }
        Ok(message)
    })();
    let message = timer.finish_result(encoded)?;
    write_chunked_message(socket, &message).await
}

async fn write_failure(
    socket: &mut TcpStream,
    request_metrics: &Arc<RequestStageMetrics>,
    error: &BoltError,
) -> Result<(), io::Error> {
    let timer = request_metrics.start(RequestStage::BoltEncode);
    let encoded: Result<Vec<u8>, io::Error> = (|| {
        let mut message = vec![0xb1, 0x7f, 0xa2];
        encode_string("code", &mut message)?;
        encode_string(error.code(), &mut message)?;
        encode_string("message", &mut message)?;
        encode_string(error.message(), &mut message)?;
        Ok(message)
    })();
    let message = timer.finish_result(encoded)?;
    write_chunked_message(socket, &message).await
}

async fn write_record(
    socket: &mut TcpStream,
    request_metrics: &Arc<RequestStageMetrics>,
    row: &[GatewayValue],
) -> Result<(), io::Error> {
    let timer = request_metrics.start(RequestStage::BoltEncode);
    let encoded: Result<Vec<u8>, io::Error> = (|| {
        let mut message = vec![0xb1, 0x71];
        encode_list_len(row.len(), &mut message)?;
        for value in row {
            encode_gateway_value(value, &mut message)?;
        }
        Ok(message)
    })();
    let message = timer.finish_result(encoded)?;
    write_chunked_message(socket, &message).await
}

fn encode_gateway_value(value: &GatewayValue, output: &mut Vec<u8>) -> Result<(), io::Error> {
    match value {
        GatewayValue::Null => output.push(0xc0),
        GatewayValue::Boolean(value) => output.push(if *value { 0xc3 } else { 0xc2 }),
        GatewayValue::Integer(value) if (-16..=127).contains(value) => output.push(*value as u8),
        GatewayValue::Integer(value) => {
            output.push(0xcb);
            output.extend_from_slice(&value.to_be_bytes());
        }
        GatewayValue::FloatBits(value) => {
            output.push(0xc1);
            output.extend_from_slice(&value.to_be_bytes());
        }
        GatewayValue::Bytes(value) => {
            if let Ok(length) = u8::try_from(value.len()) {
                output.extend_from_slice(&[0xcc, length]);
            } else if let Ok(length) = u16::try_from(value.len()) {
                output.push(0xcd);
                output.extend_from_slice(&length.to_be_bytes());
            } else {
                let length = u32::try_from(value.len()).map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "Bolt bytes are oversized")
                })?;
                output.push(0xce);
                output.extend_from_slice(&length.to_be_bytes());
            }
            output.extend_from_slice(value);
        }
        GatewayValue::String(value) => encode_string(value, output)?,
        GatewayValue::List(values) => {
            encode_list_len(values.len(), output)?;
            for value in values {
                encode_gateway_value(value, output)?;
            }
        }
        GatewayValue::Map(values) => {
            encode_map_len(values.len(), output)?;
            for (key, value) in values {
                encode_string(key, output)?;
                encode_gateway_value(value, output)?;
            }
        }
    }
    Ok(())
}

fn encode_string(value: &str, output: &mut Vec<u8>) -> Result<(), io::Error> {
    let length = value.len();
    if length <= 15 {
        output.push(0x80 | length as u8);
    } else if let Ok(length) = u8::try_from(length) {
        output.extend_from_slice(&[0xd0, length]);
    } else if let Ok(length) = u16::try_from(length) {
        output.push(0xd1);
        output.extend_from_slice(&length.to_be_bytes());
    } else {
        let length = u32::try_from(length)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Bolt string is oversized"))?;
        output.push(0xd2);
        output.extend_from_slice(&length.to_be_bytes());
    }
    output.extend_from_slice(value.as_bytes());
    Ok(())
}

fn encode_list_len(length: usize, output: &mut Vec<u8>) -> Result<(), io::Error> {
    if length <= 15 {
        output.push(0x90 | length as u8);
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Bolt record list exceeds supported width",
        ))
    }
}

fn encode_tiny_map_len(length: usize, output: &mut Vec<u8>) -> Result<(), io::Error> {
    if length <= 15 {
        output.push(0xa0 | length as u8);
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Bolt metadata map exceeds supported width",
        ))
    }
}

fn encode_map_len(length: usize, output: &mut Vec<u8>) -> Result<(), io::Error> {
    encode_tiny_map_len(length, output)
}

struct PackDecoder<'a> {
    bytes: &'a [u8],
    cursor: usize,
}

impl<'a> PackDecoder<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, cursor: 0 }
    }

    fn structure(&mut self) -> Result<(usize, u8), io::Error> {
        let marker = self.byte()?;
        if marker & 0xf0 != 0xb0 {
            return Err(invalid_pack("Bolt message is not a tiny structure"));
        }
        Ok((usize::from(marker & 0x0f), self.byte()?))
    }

    fn string(&mut self) -> Result<&'a str, io::Error> {
        let marker = self.byte()?;
        let length = match marker {
            0x80..=0x8f => usize::from(marker & 0x0f),
            0xd0 => usize::from(self.byte()?),
            0xd1 => usize::from(self.u16()?),
            0xd2 => usize::try_from(self.u32()?)
                .map_err(|_| invalid_pack("Bolt string length exceeds usize"))?,
            _ => return Err(invalid_pack("expected a Bolt string")),
        };
        let bytes = self.take(length)?;
        std::str::from_utf8(bytes).map_err(|_| invalid_pack("Bolt string is not UTF-8"))
    }

    fn gateway_map(&mut self) -> Result<BTreeMap<String, GatewayValue>, io::Error> {
        let length = self.collection_len(0xa0, 0xd8, 0xd9)?;
        let mut values = BTreeMap::new();
        for _ in 0..length {
            let key = self.string()?.to_owned();
            let value = self.gateway_value()?;
            if values.insert(key, value).is_some() {
                return Err(invalid_pack("Bolt parameter map repeats a key"));
            }
        }
        Ok(values)
    }

    fn gateway_value(&mut self) -> Result<GatewayValue, io::Error> {
        let marker = self.peek()?;
        match marker {
            0x00..=0x7f => Ok(GatewayValue::Integer(i64::from(self.byte()?))),
            0xf0..=0xff => Ok(GatewayValue::Integer(i64::from(self.byte()? as i8))),
            0xc0 => {
                self.byte()?;
                Ok(GatewayValue::Null)
            }
            0xc2 | 0xc3 => Ok(GatewayValue::Boolean(self.byte()? == 0xc3)),
            0xc8 => {
                self.byte()?;
                Ok(GatewayValue::Integer(i64::from(self.byte()? as i8)))
            }
            0xc9 => {
                self.byte()?;
                Ok(GatewayValue::Integer(i64::from(self.i16()?)))
            }
            0xca => {
                self.byte()?;
                Ok(GatewayValue::Integer(i64::from(self.i32()?)))
            }
            0xcb => {
                self.byte()?;
                Ok(GatewayValue::Integer(self.i64()?))
            }
            0xc1 => {
                self.byte()?;
                Ok(GatewayValue::FloatBits(self.u64()?))
            }
            0xcc => {
                self.byte()?;
                let length = usize::from(self.byte()?);
                Ok(GatewayValue::Bytes(self.take(length)?.to_vec()))
            }
            0xcd => {
                self.byte()?;
                let length = usize::from(self.u16()?);
                Ok(GatewayValue::Bytes(self.take(length)?.to_vec()))
            }
            0xce => {
                self.byte()?;
                let length = usize::try_from(self.u32()?)
                    .map_err(|_| invalid_pack("Bolt bytes length exceeds usize"))?;
                Ok(GatewayValue::Bytes(self.take(length)?.to_vec()))
            }
            0x80..=0x8f | 0xd0..=0xd2 => Ok(GatewayValue::String(self.string()?.to_owned())),
            0x90..=0x9f | 0xd4 | 0xd5 | 0xd6 => {
                let length = self.collection_len(0x90, 0xd4, 0xd5)?;
                let mut values = Vec::with_capacity(length);
                for _ in 0..length {
                    values.push(self.gateway_value()?);
                }
                Ok(GatewayValue::List(values))
            }
            0xa0..=0xaf | 0xd8 | 0xd9 | 0xda => Ok(GatewayValue::Map(self.gateway_map()?)),
            _ => Err(invalid_pack("unsupported Bolt parameter value")),
        }
    }

    fn skip_value(&mut self) -> Result<(), io::Error> {
        let marker = self.peek()?;
        match marker {
            0x00..=0x7f | 0xf0..=0xff | 0xc0 | 0xc2 | 0xc3 => {
                self.byte()?;
            }
            0xc8 => {
                self.take(2)?;
            }
            0xc9 => {
                self.take(3)?;
            }
            0xca => {
                self.take(5)?;
            }
            0xcb | 0xc1 => {
                self.take(9)?;
            }
            0xcc => {
                self.byte()?;
                let length = usize::from(self.byte()?);
                self.take(length)?;
            }
            0xcd => {
                self.byte()?;
                let length = usize::from(self.u16()?);
                self.take(length)?;
            }
            0xce => {
                self.byte()?;
                let length = usize::try_from(self.u32()?)
                    .map_err(|_| invalid_pack("Bolt bytes length exceeds usize"))?;
                self.take(length)?;
            }
            0x80..=0x8f | 0xd0..=0xd2 => {
                self.string()?;
            }
            0x90..=0x9f | 0xd4..=0xd6 => {
                let length = self.collection_len(0x90, 0xd4, 0xd5)?;
                for _ in 0..length {
                    self.skip_value()?;
                }
            }
            0xa0..=0xaf | 0xd8..=0xda => {
                let length = self.collection_len(0xa0, 0xd8, 0xd9)?;
                for _ in 0..length {
                    self.string()?;
                    self.skip_value()?;
                }
            }
            _ => return Err(invalid_pack("unsupported Bolt value")),
        }
        Ok(())
    }

    fn collection_len(
        &mut self,
        tiny_prefix: u8,
        marker8: u8,
        marker16: u8,
    ) -> Result<usize, io::Error> {
        let marker = self.byte()?;
        if marker & 0xf0 == tiny_prefix {
            return Ok(usize::from(marker & 0x0f));
        }
        if marker == marker8 {
            return Ok(usize::from(self.byte()?));
        }
        if marker == marker16 {
            return Ok(usize::from(self.u16()?));
        }
        if marker == marker16 + 1 {
            return usize::try_from(self.u32()?)
                .map_err(|_| invalid_pack("Bolt collection length exceeds usize"));
        }
        Err(invalid_pack("unexpected Bolt collection marker"))
    }

    fn finish(&self) -> Result<(), io::Error> {
        if self.cursor == self.bytes.len() {
            Ok(())
        } else {
            Err(invalid_pack("Bolt message has trailing bytes"))
        }
    }

    fn peek(&self) -> Result<u8, io::Error> {
        self.bytes
            .get(self.cursor)
            .copied()
            .ok_or_else(|| invalid_pack("truncated Bolt message"))
    }

    fn byte(&mut self) -> Result<u8, io::Error> {
        let value = self.peek()?;
        self.cursor += 1;
        Ok(value)
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], io::Error> {
        let end = self
            .cursor
            .checked_add(length)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(|| invalid_pack("truncated Bolt value"))?;
        let bytes = &self.bytes[self.cursor..end];
        self.cursor = end;
        Ok(bytes)
    }

    fn u16(&mut self) -> Result<u16, io::Error> {
        Ok(u16::from_be_bytes(
            self.take(2)?.try_into().expect("length checked"),
        ))
    }

    fn u32(&mut self) -> Result<u32, io::Error> {
        Ok(u32::from_be_bytes(
            self.take(4)?.try_into().expect("length checked"),
        ))
    }

    fn u64(&mut self) -> Result<u64, io::Error> {
        Ok(u64::from_be_bytes(
            self.take(8)?.try_into().expect("length checked"),
        ))
    }

    fn i16(&mut self) -> Result<i16, io::Error> {
        Ok(i16::from_be_bytes(
            self.take(2)?.try_into().expect("length checked"),
        ))
    }

    fn i32(&mut self) -> Result<i32, io::Error> {
        Ok(i32::from_be_bytes(
            self.take(4)?.try_into().expect("length checked"),
        ))
    }

    fn i64(&mut self) -> Result<i64, io::Error> {
        Ok(i64::from_be_bytes(
            self.take(8)?.try_into().expect("length checked"),
        ))
    }
}

fn invalid_pack(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

pub struct BoltSession<'a> {
    service: &'a GatewayService,
}

impl<'a> BoltSession<'a> {
    pub(crate) const fn new(service: &'a GatewayService) -> Self {
        Self { service }
    }

    pub fn query(&self, statement: impl Into<String>) -> BoltQuery<'a> {
        BoltQuery {
            service: self.service,
            statement: statement.into(),
            parameters: BTreeMap::new(),
            transaction_id: None,
            cancellation: GatewayCancellationToken::new(),
            request_timeout: None,
        }
    }

    pub async fn begin(&self) -> Result<BoltTransaction<'a>, BoltError> {
        let cancellation = GatewayCancellationToken::new();
        let response = self
            .service
            .execute_statement("BEGIN".into(), BTreeMap::new(), None, &cancellation)
            .await?;
        match response {
            dtg_execution::GatewayResponse::Transaction { transaction_id } => Ok(BoltTransaction {
                service: self.service,
                transaction_id,
            }),
            _ => Err(BoltError::protocol(
                "BEGIN returned a non-transaction response",
            )),
        }
    }

    pub async fn analytics_status(&self, job_id: u128) -> Result<GatewayAnalyticsState, BoltError> {
        let cancellation = GatewayCancellationToken::new();
        let response = self
            .service
            .execute_operation(
                GatewayOperation::AnalyticsStatus { job_id },
                None,
                &cancellation,
            )
            .await?;
        match response {
            dtg_execution::GatewayResponse::AnalyticsStatus {
                job_id: actual,
                state,
            } if actual == job_id => Ok(state),
            _ => Err(BoltError::protocol(
                "analytics status returned an unexpected response",
            )),
        }
    }

    pub async fn analytics_result(&self, job_id: u128) -> Result<GatewayRows, BoltError> {
        let cancellation = GatewayCancellationToken::new();
        let response = self
            .service
            .execute_operation(
                GatewayOperation::AnalyticsResult { job_id },
                None,
                &cancellation,
            )
            .await?;
        match response {
            dtg_execution::GatewayResponse::AnalyticsResult {
                job_id: actual,
                rows,
            } if actual == job_id => Ok(rows),
            _ => Err(BoltError::protocol(
                "analytics result returned an unexpected response",
            )),
        }
    }

    pub async fn analytics_cancel(&self, job_id: u128) -> Result<(), BoltError> {
        let cancellation = GatewayCancellationToken::new();
        let response = self
            .service
            .execute_operation(
                GatewayOperation::CancelAnalytics { job_id },
                None,
                &cancellation,
            )
            .await?;
        match response {
            dtg_execution::GatewayResponse::AnalyticsCancelled { job_id: actual }
                if actual == job_id =>
            {
                Ok(())
            }
            _ => Err(BoltError::protocol(
                "analytics cancellation returned an unexpected response",
            )),
        }
    }
}

pub struct BoltTransaction<'a> {
    service: &'a GatewayService,
    transaction_id: u128,
}

impl<'a> BoltTransaction<'a> {
    pub const fn id(&self) -> u128 {
        self.transaction_id
    }

    pub fn query(&self, statement: impl Into<String>) -> BoltQuery<'a> {
        BoltQuery {
            service: self.service,
            statement: statement.into(),
            parameters: BTreeMap::new(),
            transaction_id: Some(self.transaction_id),
            cancellation: GatewayCancellationToken::new(),
            request_timeout: None,
        }
    }

    pub async fn commit(self) -> Result<(), BoltError> {
        self.finish("COMMIT").await
    }

    pub async fn rollback(self) -> Result<(), BoltError> {
        self.finish("ROLLBACK").await
    }

    async fn finish(self, statement: &str) -> Result<(), BoltError> {
        let cancellation = GatewayCancellationToken::new();
        let response = self
            .service
            .execute_statement(
                statement.into(),
                BTreeMap::new(),
                Some(self.transaction_id),
                &cancellation,
            )
            .await?;
        match response {
            dtg_execution::GatewayResponse::Acknowledged => Ok(()),
            _ => Err(BoltError::protocol(
                "transaction boundary returned an unexpected response",
            )),
        }
    }
}

pub struct BoltQuery<'a> {
    service: &'a GatewayService,
    statement: String,
    parameters: BTreeMap<String, GatewayValue>,
    transaction_id: Option<u128>,
    cancellation: GatewayCancellationToken,
    request_timeout: Option<Duration>,
}

impl<'a> BoltQuery<'a> {
    pub fn param(mut self, name: impl Into<String>, value: impl Into<GatewayValue>) -> Self {
        self.parameters.insert(name.into(), value.into());
        self
    }

    pub fn transaction(mut self, transaction_id: u128) -> Self {
        self.transaction_id = Some(transaction_id);
        self
    }

    pub fn cancellation(mut self, cancellation: GatewayCancellationToken) -> Self {
        self.cancellation = cancellation;
        self
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = Some(timeout);
        self
    }

    pub async fn run(self) -> Result<GatewayRows, BoltError> {
        self.service
            .execute_query(
                self.statement,
                self.parameters,
                self.transaction_id,
                &self.cancellation,
                self.request_timeout,
            )
            .await
    }

    pub async fn execute(self) -> Result<dtg_execution::GatewayResponse, BoltError> {
        self.service
            .execute_statement_with_timeout(
                self.statement,
                self.parameters,
                self.transaction_id,
                &self.cancellation,
                self.request_timeout,
            )
            .await
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BoltError {
    code: String,
    message: String,
}

impl BoltError {
    pub(crate) fn protocol(message: impl Into<String>) -> Self {
        Self {
            code: "DTG-GATEWAY-BOLT-PROTOCOL".into(),
            message: message.into(),
        }
    }

    pub fn code(&self) -> &str {
        &self.code
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl From<GatewayExecutionError> for BoltError {
    fn from(error: GatewayExecutionError) -> Self {
        Self {
            code: error.code().to_owned(),
            message: error.message().to_owned(),
        }
    }
}

impl fmt::Display for BoltError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for BoltError {}
