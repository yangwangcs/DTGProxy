use std::collections::BTreeMap;
use std::io;
use std::net::SocketAddr;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::task::JoinSet;

use super::{CellSpec, RawObservation, Workload};

const BOLT_MAGIC: [u8; 4] = [0x60, 0x60, 0xb0, 0x17];
const BOLT_V5_4: [u8; 4] = [0, 0, 4, 5];
const MAX_MESSAGE_BYTES: usize = 8 * 1024 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BoltValue {
    Null,
    Boolean(bool),
    Integer(i64),
    FloatBits(u64),
    Bytes(Vec<u8>),
    String(String),
    List(Vec<Self>),
    Map(BTreeMap<String, Self>),
    Structure { signature: u8, fields: Vec<Self> },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BoltResult {
    pub fields: Vec<String>,
    pub rows: Vec<Vec<BoltValue>>,
    pub summary: BTreeMap<String, BoltValue>,
    pub result_digest: String,
}

pub struct BoltSession {
    socket: TcpStream,
}

impl BoltSession {
    pub async fn connect(address: SocketAddr) -> io::Result<Self> {
        let mut socket = TcpStream::connect(address).await?;
        socket.set_nodelay(true)?;
        socket
            .write_all(&[
                BOLT_MAGIC[0],
                BOLT_MAGIC[1],
                BOLT_MAGIC[2],
                BOLT_MAGIC[3],
                BOLT_V5_4[0],
                BOLT_V5_4[1],
                BOLT_V5_4[2],
                BOLT_V5_4[3],
                0,
                0,
                0,
                5,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
            ])
            .await?;
        let mut selected = [0_u8; 4];
        socket.read_exact(&mut selected).await?;
        if selected != BOLT_V5_4 {
            return Err(invalid_data("Bolt server did not negotiate version 5.4"));
        }
        write_message(&mut socket, &[0xb1, 0x01, 0xa0]).await?;
        let hello = read_message(&mut socket).await?;
        let _ = decode_success(&hello)?;
        Ok(Self { socket })
    }

    pub fn nodelay(&self) -> io::Result<bool> {
        self.socket.nodelay()
    }

    pub async fn run(
        &mut self,
        statement: &str,
        parameters: BTreeMap<String, BoltValue>,
    ) -> io::Result<BoltResult> {
        let message = run_message(statement, &parameters)?;
        write_message(&mut self.socket, &message).await?;
        let metadata = decode_success(&read_message(&mut self.socket).await?)?;
        let fields = match metadata.get("fields") {
            None => Vec::new(),
            Some(BoltValue::List(values)) => values
                .iter()
                .map(|value| match value {
                    BoltValue::String(value) => Ok(value.clone()),
                    _ => Err(invalid_data("Bolt RUN fields must be strings")),
                })
                .collect::<io::Result<Vec<_>>>()?,
            Some(_) => return Err(invalid_data("Bolt RUN fields must be a list")),
        };

        write_message(&mut self.socket, &[0xb1, 0x3f, 0xa0]).await?;
        let mut rows = Vec::new();
        let summary = loop {
            let message = read_message(&mut self.socket).await?;
            let value = decode_message(&message)?;
            match value {
                BoltValue::Structure {
                    signature: 0x71,
                    mut fields,
                } if fields.len() == 1 => match fields.remove(0) {
                    BoltValue::List(row) => rows.push(row),
                    _ => return Err(invalid_data("Bolt RECORD must contain a list")),
                },
                BoltValue::Structure {
                    signature: 0x70,
                    mut fields,
                } if fields.len() == 1 => match fields.remove(0) {
                    BoltValue::Map(summary) => break summary,
                    _ => return Err(invalid_data("Bolt SUCCESS must contain a map")),
                },
                BoltValue::Structure {
                    signature: 0x7f,
                    mut fields,
                } if fields.len() == 1 => {
                    let detail = match fields.remove(0) {
                        BoltValue::Map(metadata) => format_failure(&metadata),
                        _ => "Bolt FAILURE did not contain a map".into(),
                    };
                    return Err(invalid_data(detail));
                }
                _ => return Err(invalid_data("unexpected Bolt response while pulling rows")),
            }
        };

        Ok(BoltResult {
            result_digest: result_digest(&fields, &rows),
            fields,
            rows,
            summary,
        })
    }

    pub async fn run_pipeline(
        &mut self,
        requests: &[(String, BTreeMap<String, BoltValue>)],
    ) -> io::Result<Vec<BoltResult>> {
        for (statement, parameters) in requests {
            write_message(&mut self.socket, &run_message(statement, parameters)?).await?;
            write_message(&mut self.socket, &[0xb1, 0x3f, 0xa0]).await?;
        }

        let mut results = Vec::with_capacity(requests.len());
        for _ in requests {
            let metadata = decode_success(&read_message(&mut self.socket).await?)?;
            let fields = fields_from_run_metadata(metadata)?;
            let mut rows = Vec::new();
            let summary = loop {
                let message = read_message(&mut self.socket).await?;
                let value = decode_message(&message)?;
                match value {
                    BoltValue::Structure {
                        signature: 0x71,
                        mut fields,
                    } if fields.len() == 1 => match fields.remove(0) {
                        BoltValue::List(row) => rows.push(row),
                        _ => return Err(invalid_data("Bolt RECORD must contain a list")),
                    },
                    BoltValue::Structure {
                        signature: 0x70,
                        mut fields,
                    } if fields.len() == 1 => match fields.remove(0) {
                        BoltValue::Map(summary) => break summary,
                        _ => return Err(invalid_data("Bolt SUCCESS must contain a map")),
                    },
                    BoltValue::Structure {
                        signature: 0x7f,
                        mut fields,
                    } if fields.len() == 1 => {
                        let detail = match fields.remove(0) {
                            BoltValue::Map(metadata) => format_failure(&metadata),
                            _ => "Bolt FAILURE did not contain a map".into(),
                        };
                        return Err(invalid_data(detail));
                    }
                    _ => return Err(invalid_data("unexpected Bolt response while pulling rows")),
                }
            };
            results.push(BoltResult {
                result_digest: result_digest(&fields, &rows),
                fields,
                rows,
                summary,
            });
        }
        Ok(results)
    }
}

fn run_message(statement: &str, parameters: &BTreeMap<String, BoltValue>) -> io::Result<Vec<u8>> {
    let mut message = vec![0xb3, 0x10];
    encode_string(statement, &mut message)?;
    encode_map(parameters, &mut message)?;
    message.push(0xa0);
    Ok(message)
}

fn fields_from_run_metadata(metadata: BTreeMap<String, BoltValue>) -> io::Result<Vec<String>> {
    match metadata.get("fields") {
        None => Ok(Vec::new()),
        Some(BoltValue::List(values)) => values
            .iter()
            .map(|value| match value {
                BoltValue::String(value) => Ok(value.clone()),
                _ => Err(invalid_data("Bolt RUN fields must be strings")),
            })
            .collect::<io::Result<Vec<_>>>(),
        Some(_) => Err(invalid_data("Bolt RUN fields must be a list")),
    }
}

pub async fn measure_cell(
    address: SocketAddr,
    cell: CellSpec,
    warmup: Duration,
    measurement: Duration,
) -> io::Result<RawObservation> {
    measure_pipeline_cell(address, cell, 1, warmup, measurement).await
}

pub async fn measure_pipeline_cell(
    address: SocketAddr,
    cell: CellSpec,
    depth: usize,
    warmup: Duration,
    measurement: Duration,
) -> io::Result<RawObservation> {
    if depth == 0 {
        return Err(invalid_data("Bolt pipeline depth must be positive"));
    }
    if cell.workload.is_write() && depth > 1 {
        return Err(invalid_data(
            "Bolt pipeline measurement only supports read workloads",
        ));
    }
    let phase_started = Instant::now();
    let started_at_unix_ns = unix_time_nanos();
    let warmup_deadline = phase_started + warmup;
    let measurement_deadline = warmup_deadline + measurement;
    let (statement, parameters) = workload_request(cell.workload);
    let query_digest = query_digest(statement, &parameters);
    let mut workers = JoinSet::new();
    for _ in 0..cell.concurrency {
        let statement = statement.to_owned();
        let parameters = parameters.clone();
        workers.spawn(async move {
            measure_worker(
                address,
                cell.workload,
                statement,
                parameters,
                depth,
                warmup_deadline,
                measurement_deadline,
            )
            .await
        });
    }

    tokio::time::sleep_until(warmup_deadline.into()).await;
    let measurement_started_at_unix_ns = unix_time_nanos();
    tokio::time::sleep_until(measurement_deadline.into()).await;
    let measurement_finished_at_unix_ns = unix_time_nanos();

    let mut latency_samples_ns = Vec::new();
    let mut operations = 0_u64;
    let mut warmup_operations = 0_u64;
    let mut measured_duration_ns = 0_u64;
    let mut identity = None;
    while let Some(worker) = workers.join_next().await {
        let worker = worker
            .map_err(|error| invalid_data(format!("measurement worker failed: {error}")))??;
        latency_samples_ns.extend(worker.latency_samples_ns);
        operations += worker.operations;
        warmup_operations += worker.warmup_operations;
        measured_duration_ns = measured_duration_ns.max(worker.measured_duration_ns);
        if let Some(result) = worker.identity {
            let candidate = (
                result.fields,
                result.rows.len() as u64,
                result.result_digest,
            );
            if let Some(existing) = &identity {
                if existing != &candidate {
                    return Err(invalid_data(
                        "read operations in one cell returned different identities",
                    ));
                }
            } else {
                identity = Some(candidate);
            }
        }
    }
    let (_, row_count, result_digest) = identity.unwrap_or((Vec::new(), 0, String::new()));
    Ok(RawObservation {
        backend: cell.backend,
        workload: cell.workload,
        concurrency: cell.concurrency,
        repetition: cell.repetition,
        started_at_unix_ns,
        finished_at_unix_ns: measurement_finished_at_unix_ns,
        warmup_finished_at_unix_ns: measurement_started_at_unix_ns,
        measurement_started_at_unix_ns,
        measurement_finished_at_unix_ns,
        measured_duration_ns,
        operations,
        warmup_operations,
        persisted_operations: 0,
        errors: 0,
        latency_samples_ns,
        row_count,
        result_digest,
        query_digest,
        transport_mode: if depth == 1 {
            "unary".into()
        } else {
            "pipeline".into()
        },
        gateway_stage_metrics: None,
        data_stage_metrics: None,
    })
}

struct WorkerMeasurement {
    latency_samples_ns: Vec<u64>,
    operations: u64,
    warmup_operations: u64,
    measured_duration_ns: u64,
    identity: Option<BoltResult>,
}

async fn measure_worker(
    address: SocketAddr,
    workload: Workload,
    statement: String,
    parameters: BTreeMap<String, BoltValue>,
    depth: usize,
    warmup_deadline: Instant,
    measurement_deadline: Instant,
) -> io::Result<WorkerMeasurement> {
    let mut session = BoltSession::connect(address).await?;
    let mut identity = None;
    let requests = vec![(statement, parameters); depth];
    let mut warmup_operations = 0_u64;
    while Instant::now() < warmup_deadline {
        let results = if depth == 1 {
            vec![session.run(&requests[0].0, requests[0].1.clone()).await?]
        } else {
            session.run_pipeline(&requests).await?
        };
        for result in results {
            validate_result(workload, &result)?;
            check_identity(workload, &mut identity, result)?;
            warmup_operations += 1;
        }
    }

    let mut latency_samples_ns = Vec::new();
    let mut operations = 0_u64;
    while Instant::now() < measurement_deadline {
        let operation_started = Instant::now();
        let results = if depth == 1 {
            vec![session.run(&requests[0].0, requests[0].1.clone()).await?]
        } else {
            session.run_pipeline(&requests).await?
        };
        let per_request_latency = nanos_u64(operation_started.elapsed())
            / u64::try_from(results.len()).unwrap_or(1).max(1);
        for result in results {
            validate_result(workload, &result)?;
            check_identity(workload, &mut identity, result)?;
            latency_samples_ns.push(per_request_latency);
            operations += 1;
        }
    }
    Ok(WorkerMeasurement {
        latency_samples_ns,
        operations,
        warmup_operations,
        measured_duration_ns: nanos_u64(
            measurement_deadline.saturating_duration_since(warmup_deadline),
        ),
        identity,
    })
}

fn workload_request(workload: Workload) -> (&'static str, BTreeMap<String, BoltValue>) {
    match workload {
        Workload::CreateVertex => ("CREATE (n:Bench {value: 1}) VALID FROM 1", BTreeMap::new()),
        Workload::PointLookup => (
            "MATCH (n) WHERE n.id = $id RETURN n.id",
            BTreeMap::from([("id".into(), BoltValue::Integer(2048))]),
        ),
        Workload::OneHopExpand => (
            "MATCH (a)-[r]->(b) WHERE a.id = $id RETURN r",
            BTreeMap::from([("id".into(), BoltValue::Integer(2048))]),
        ),
        Workload::TwoHopExpand => (
            "MATCH (a)-[first]->(middle)-[second]->(destination) WHERE a.id = $id RETURN second",
            BTreeMap::from([("id".into(), BoltValue::Integer(2048))]),
        ),
        Workload::CountVertices => ("MATCH (n) RETURN COUNT(*)", BTreeMap::new()),
    }
}

fn validate_result(workload: Workload, result: &BoltResult) -> io::Result<()> {
    if workload.is_write() && !result.rows.is_empty() {
        return Err(invalid_data("write workload returned rows"));
    }
    Ok(())
}

fn check_identity(
    workload: Workload,
    identity: &mut Option<BoltResult>,
    result: BoltResult,
) -> io::Result<()> {
    if !workload.is_write()
        && let Some(existing) = identity
        && (existing.fields != result.fields || existing.result_digest != result.result_digest)
    {
        return Err(invalid_data("read operation returned a different identity"));
    }
    *identity = Some(result);
    Ok(())
}

async fn write_message(socket: &mut TcpStream, message: &[u8]) -> io::Result<()> {
    let chunk_count = message.len().div_ceil(usize::from(u16::MAX));
    let mut encoded = Vec::with_capacity(message.len() + chunk_count * 2 + 2);
    for chunk in message.chunks(usize::from(u16::MAX)) {
        encoded.extend_from_slice(&(chunk.len() as u16).to_be_bytes());
        encoded.extend_from_slice(chunk);
    }
    encoded.extend_from_slice(&[0, 0]);
    socket.write_all(&encoded).await
}

async fn read_message(socket: &mut TcpStream) -> io::Result<Vec<u8>> {
    let mut message = Vec::new();
    loop {
        let mut length = [0_u8; 2];
        socket.read_exact(&mut length).await?;
        let length = usize::from(u16::from_be_bytes(length));
        if length == 0 {
            return Ok(message);
        }
        let new_len = message
            .len()
            .checked_add(length)
            .filter(|length| *length <= MAX_MESSAGE_BYTES)
            .ok_or_else(|| invalid_data("Bolt message exceeds the diagnostic bound"))?;
        let start = message.len();
        message.resize(new_len, 0);
        socket.read_exact(&mut message[start..]).await?;
    }
}

fn decode_success(message: &[u8]) -> io::Result<BTreeMap<String, BoltValue>> {
    match decode_message(message)? {
        BoltValue::Structure {
            signature: 0x70,
            mut fields,
        } if fields.len() == 1 => match fields.remove(0) {
            BoltValue::Map(metadata) => Ok(metadata),
            _ => Err(invalid_data("Bolt SUCCESS must contain a map")),
        },
        BoltValue::Structure {
            signature: 0x7f,
            mut fields,
        } if fields.len() == 1 => match fields.remove(0) {
            BoltValue::Map(metadata) => Err(invalid_data(format_failure(&metadata))),
            _ => Err(invalid_data("Bolt FAILURE must contain a map")),
        },
        _ => Err(invalid_data("expected Bolt SUCCESS")),
    }
}

fn decode_message(message: &[u8]) -> io::Result<BoltValue> {
    let mut decoder = PackDecoder {
        bytes: message,
        offset: 0,
    };
    let value = decoder.value()?;
    if decoder.offset != message.len() {
        return Err(invalid_data(
            "Bolt message contains trailing PackStream data",
        ));
    }
    Ok(value)
}

struct PackDecoder<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> PackDecoder<'a> {
    fn value(&mut self) -> io::Result<BoltValue> {
        let marker = self.byte()?;
        match marker {
            0x00..=0x7f => Ok(BoltValue::Integer(i64::from(marker))),
            0xf0..=0xff => Ok(BoltValue::Integer(i64::from(marker as i8))),
            0xc0 => Ok(BoltValue::Null),
            0xc2 => Ok(BoltValue::Boolean(false)),
            0xc3 => Ok(BoltValue::Boolean(true)),
            0xc1 => Ok(BoltValue::FloatBits(u64::from_be_bytes(self.take_array()?))),
            0xc8 => Ok(BoltValue::Integer(i64::from(self.byte()? as i8))),
            0xc9 => Ok(BoltValue::Integer(i64::from(i16::from_be_bytes(
                self.take_array()?,
            )))),
            0xca => Ok(BoltValue::Integer(i64::from(i32::from_be_bytes(
                self.take_array()?,
            )))),
            0xcb => Ok(BoltValue::Integer(i64::from_be_bytes(self.take_array()?))),
            0xcc => {
                let len = usize::from(self.byte()?);
                Ok(BoltValue::Bytes(self.take(len)?.to_vec()))
            }
            0xcd => {
                let len = usize::from(u16::from_be_bytes(self.take_array()?));
                Ok(BoltValue::Bytes(self.take(len)?.to_vec()))
            }
            0xce => {
                let len = usize::try_from(u32::from_be_bytes(self.take_array()?))
                    .map_err(|_| invalid_data("Bolt bytes length overflows usize"))?;
                Ok(BoltValue::Bytes(self.take(len)?.to_vec()))
            }
            0x80..=0x8f => self.string(usize::from(marker & 0x0f)),
            0xd0 => {
                let len = usize::from(self.byte()?);
                self.string(len)
            }
            0xd1 => {
                let len = usize::from(u16::from_be_bytes(self.take_array()?));
                self.string(len)
            }
            0xd2 => {
                let len = usize::try_from(u32::from_be_bytes(self.take_array()?))
                    .map_err(|_| invalid_data("Bolt string length overflows usize"))?;
                self.string(len)
            }
            0x90..=0x9f => self.list(usize::from(marker & 0x0f)),
            0xd4 => {
                let len = usize::from(self.byte()?);
                self.list(len)
            }
            0xd5 => {
                let len = usize::from(u16::from_be_bytes(self.take_array()?));
                self.list(len)
            }
            0xd6 => {
                let len = usize::try_from(u32::from_be_bytes(self.take_array()?))
                    .map_err(|_| invalid_data("Bolt list length overflows usize"))?;
                self.list(len)
            }
            0xa0..=0xaf => self.map(usize::from(marker & 0x0f)),
            0xd8 => {
                let len = usize::from(self.byte()?);
                self.map(len)
            }
            0xd9 => {
                let len = usize::from(u16::from_be_bytes(self.take_array()?));
                self.map(len)
            }
            0xda => {
                let len = usize::try_from(u32::from_be_bytes(self.take_array()?))
                    .map_err(|_| invalid_data("Bolt map length overflows usize"))?;
                self.map(len)
            }
            0xb0..=0xbf => self.structure(usize::from(marker & 0x0f)),
            _ => Err(invalid_data(format!(
                "unsupported PackStream marker {marker:#x}"
            ))),
        }
    }

    fn byte(&mut self) -> io::Result<u8> {
        self.take(1).map(|bytes| bytes[0])
    }

    fn take(&mut self, len: usize) -> io::Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(len)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(|| invalid_data("truncated PackStream value"))?;
        let bytes = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(bytes)
    }

    fn take_array<const N: usize>(&mut self) -> io::Result<[u8; N]> {
        self.take(N)?
            .try_into()
            .map_err(|_| invalid_data("invalid PackStream array"))
    }

    fn string(&mut self, len: usize) -> io::Result<BoltValue> {
        let bytes = self.take(len)?;
        let value = std::str::from_utf8(bytes)
            .map_err(|_| invalid_data("Bolt string is not UTF-8"))?
            .to_owned();
        Ok(BoltValue::String(value))
    }

    fn list(&mut self, len: usize) -> io::Result<BoltValue> {
        let mut values = Vec::with_capacity(len);
        for _ in 0..len {
            values.push(self.value()?);
        }
        Ok(BoltValue::List(values))
    }

    fn map(&mut self, len: usize) -> io::Result<BoltValue> {
        let mut values = BTreeMap::new();
        for _ in 0..len {
            let key = match self.value()? {
                BoltValue::String(key) => key,
                _ => return Err(invalid_data("Bolt map key is not a string")),
            };
            values.insert(key, self.value()?);
        }
        Ok(BoltValue::Map(values))
    }

    fn structure(&mut self, len: usize) -> io::Result<BoltValue> {
        let signature = self.byte()?;
        let mut fields = Vec::with_capacity(len);
        for _ in 0..len {
            fields.push(self.value()?);
        }
        Ok(BoltValue::Structure { signature, fields })
    }
}

fn encode_map(values: &BTreeMap<String, BoltValue>, output: &mut Vec<u8>) -> io::Result<()> {
    encode_collection_len(values.len(), 0xa0, 0xd8, 0xd9, 0xda, output)?;
    for (key, value) in values {
        encode_string(key, output)?;
        encode_value(value, output)?;
    }
    Ok(())
}

fn encode_value(value: &BoltValue, output: &mut Vec<u8>) -> io::Result<()> {
    match value {
        BoltValue::Null => output.push(0xc0),
        BoltValue::Boolean(value) => output.push(if *value { 0xc3 } else { 0xc2 }),
        BoltValue::Integer(value) if (-16..=127).contains(value) => output.push(*value as u8),
        BoltValue::Integer(value) => {
            output.push(0xcb);
            output.extend_from_slice(&value.to_be_bytes());
        }
        BoltValue::FloatBits(value) => {
            output.push(0xc1);
            output.extend_from_slice(&value.to_be_bytes());
        }
        BoltValue::Bytes(value) => {
            encode_blob_len(value.len(), 0xcc, 0xcd, 0xce, output)?;
            output.extend_from_slice(value);
        }
        BoltValue::String(value) => encode_string(value, output)?,
        BoltValue::List(values) => {
            encode_collection_len(values.len(), 0x90, 0xd4, 0xd5, 0xd6, output)?;
            for value in values {
                encode_value(value, output)?;
            }
        }
        BoltValue::Map(values) => encode_map(values, output)?,
        BoltValue::Structure { .. } => {
            return Err(invalid_data("Bolt parameters cannot be structures"));
        }
    }
    Ok(())
}

fn encode_string(value: &str, output: &mut Vec<u8>) -> io::Result<()> {
    encode_blob_len(value.len(), 0x80, 0xd0, 0xd1, output)?;
    output.extend_from_slice(value.as_bytes());
    Ok(())
}

fn encode_blob_len(len: usize, tiny: u8, one: u8, two: u8, output: &mut Vec<u8>) -> io::Result<()> {
    if len <= 15 && tiny == 0x80 {
        output.push(tiny | u8::try_from(len).unwrap());
    } else if let Ok(len) = u8::try_from(len) {
        output.extend_from_slice(&[one, len]);
    } else if let Ok(len) = u16::try_from(len) {
        output.push(two);
        output.extend_from_slice(&len.to_be_bytes());
    } else {
        let len = u32::try_from(len).map_err(|_| invalid_data("PackStream value is too large"))?;
        output.push(match tiny {
            0x80 => 0xd2,
            0xcc => 0xce,
            _ => return Err(invalid_data("unsupported PackStream binary marker")),
        });
        output.extend_from_slice(&len.to_be_bytes());
    }
    Ok(())
}

fn encode_collection_len(
    len: usize,
    tiny: u8,
    one: u8,
    two: u8,
    four: u8,
    output: &mut Vec<u8>,
) -> io::Result<()> {
    if len <= 15 {
        output.push(tiny | u8::try_from(len).unwrap());
    } else if let Ok(len) = u8::try_from(len) {
        output.extend_from_slice(&[one, len]);
    } else if let Ok(len) = u16::try_from(len) {
        output.push(two);
        output.extend_from_slice(&len.to_be_bytes());
    } else {
        output.push(four);
        output.extend_from_slice(
            &u32::try_from(len)
                .map_err(|_| invalid_data("PackStream collection is too large"))?
                .to_be_bytes(),
        );
    }
    Ok(())
}

fn result_digest(fields: &[String], rows: &[Vec<BoltValue>]) -> String {
    let mut digest = Sha256::new();
    canonical_u64(&mut digest, fields.len() as u64);
    for field in fields {
        canonical_string(&mut digest, field);
    }
    canonical_u64(&mut digest, rows.len() as u64);
    for row in rows {
        canonical_u64(&mut digest, row.len() as u64);
        for value in row {
            canonical_value(&mut digest, value);
        }
    }
    hex_digest(digest.finalize())
}

fn query_digest(statement: &str, parameters: &BTreeMap<String, BoltValue>) -> String {
    let mut digest = Sha256::new();
    canonical_string(&mut digest, statement);
    canonical_u64(&mut digest, parameters.len() as u64);
    for (key, value) in parameters {
        canonical_string(&mut digest, key);
        canonical_value(&mut digest, value);
    }
    hex_digest(digest.finalize())
}

fn canonical_value(digest: &mut Sha256, value: &BoltValue) {
    match value {
        BoltValue::Null => digest.update([0]),
        BoltValue::Boolean(value) => digest.update([1, u8::from(*value)]),
        BoltValue::Integer(value) => {
            digest.update([2]);
            digest.update(value.to_be_bytes());
        }
        BoltValue::FloatBits(value) => {
            digest.update([3]);
            digest.update(value.to_be_bytes());
        }
        BoltValue::Bytes(value) => {
            digest.update([4]);
            canonical_bytes(digest, value);
        }
        BoltValue::String(value) => {
            digest.update([5]);
            canonical_string(digest, value);
        }
        BoltValue::List(values) => {
            digest.update([6]);
            canonical_u64(digest, values.len() as u64);
            for value in values {
                canonical_value(digest, value);
            }
        }
        BoltValue::Map(values) => {
            digest.update([7]);
            canonical_u64(digest, values.len() as u64);
            for (key, value) in values {
                canonical_string(digest, key);
                canonical_value(digest, value);
            }
        }
        BoltValue::Structure { signature, fields } => {
            digest.update([8, *signature]);
            canonical_u64(digest, fields.len() as u64);
            for field in fields {
                canonical_value(digest, field);
            }
        }
    }
}

fn canonical_string(digest: &mut Sha256, value: &str) {
    canonical_bytes(digest, value.as_bytes());
}

fn canonical_bytes(digest: &mut Sha256, value: &[u8]) {
    canonical_u64(digest, value.len() as u64);
    digest.update(value);
}

fn canonical_u64(digest: &mut Sha256, value: u64) {
    digest.update(value.to_be_bytes());
}

fn hex_digest(digest: impl AsRef<[u8]>) -> String {
    digest
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn format_failure(metadata: &BTreeMap<String, BoltValue>) -> String {
    let code = metadata
        .get("code")
        .and_then(|value| match value {
            BoltValue::String(value) => Some(value.as_str()),
            _ => None,
        })
        .unwrap_or("BOLT-FAILURE");
    let message = metadata
        .get("message")
        .and_then(|value| match value {
            BoltValue::String(value) => Some(value.as_str()),
            _ => None,
        })
        .unwrap_or("server did not provide a failure message");
    format!("{code}: {message}")
}

fn invalid_data(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

fn nanos_u64(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn unix_time_nanos() -> u64 {
    nanos_u64(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default(),
    )
}
