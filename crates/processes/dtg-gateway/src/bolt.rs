use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::io;
use std::sync::Arc;
use std::time::Duration;

use dtg_execution::{
    GatewayAnalyticsState, GatewayCancellationToken, GatewayExecutionError, GatewayOperation,
    GatewayResponse, GatewayRows, GatewayValue, RequestDetail, RequestStage, RequestStageMetrics,
    StageOutcome,
};

use crate::GatewayService;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, tcp::OwnedReadHalf, tcp::OwnedWriteHalf};
use tokio::sync::mpsc;

const BOLT_MAGIC: [u8; 4] = [0x60, 0x60, 0xb0, 0x17];
const BOLT_V5_4: [u8; 4] = [0x00, 0x00, 0x04, 0x05];
const MAX_BOLT_MESSAGE_BYTES: usize = 8 * 1024 * 1024;

pub async fn serve_bolt(
    listener: TcpListener,
    service: Arc<GatewayService>,
) -> Result<(), io::Error> {
    loop {
        let (socket, _) = listener.accept().await?;
        if configure_bolt_socket(&socket).is_err() {
            continue;
        }
        let service = Arc::clone(&service);
        tokio::spawn(async move {
            let _ = serve_connection(socket, service).await;
        });
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BoltStatementClass {
    Read,
    Barrier,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct BoltReadPipelineLimits {
    max_pending: usize,
    max_request_bytes: usize,
}

impl BoltReadPipelineLimits {
    pub(crate) const fn new(max_pending: usize, max_request_bytes: usize) -> Self {
        Self {
            max_pending,
            max_request_bytes,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BoltPipelineAdmission {
    Full,
    DuplicateRequestId,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BoltPipelineStateError {
    MissingPendingRead,
    DuplicateTerminal,
}

pub(crate) struct BoltReadPipeline {
    limits: BoltReadPipelineLimits,
    pending_request_bytes: usize,
    jobs: VecDeque<BoltReadJob>,
}

struct BoltReadJob {
    id: u64,
    request_bytes: usize,
    cancellation: GatewayCancellationToken,
    pull_received: bool,
    run_emitted: bool,
    completed_at: Option<std::time::Instant>,
    terminal: Option<Result<GatewayResponse, BoltError>>,
}

pub(crate) struct ReadyBoltRun {
    pub(crate) id: u64,
    pub(crate) ordered_write_wait: Option<Duration>,
    pub(crate) terminal: Result<GatewayResponse, BoltError>,
}

pub(crate) struct ReadyBoltPull {
    pub(crate) id: u64,
    pub(crate) terminal: Result<GatewayResponse, BoltError>,
}

impl BoltReadPipeline {
    pub(crate) const fn new(limits: BoltReadPipelineLimits) -> Self {
        Self {
            limits,
            pending_request_bytes: 0,
            jobs: VecDeque::new(),
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.jobs.len()
    }

    pub(crate) fn submit(
        &mut self,
        id: u64,
        request_bytes: usize,
        cancellation: GatewayCancellationToken,
    ) -> Result<(), BoltPipelineAdmission> {
        if self.jobs.iter().any(|job| job.id == id) {
            return Err(BoltPipelineAdmission::DuplicateRequestId);
        }
        let Some(next_bytes) = self.pending_request_bytes.checked_add(request_bytes) else {
            return Err(BoltPipelineAdmission::Full);
        };
        if self.jobs.len() >= self.limits.max_pending || next_bytes > self.limits.max_request_bytes
        {
            return Err(BoltPipelineAdmission::Full);
        }
        self.pending_request_bytes = next_bytes;
        self.jobs.push_back(BoltReadJob {
            id,
            request_bytes,
            cancellation,
            pull_received: false,
            run_emitted: false,
            completed_at: None,
            terminal: None,
        });
        Ok(())
    }

    pub(crate) fn mark_next_pull(&mut self) -> Result<(), BoltPipelineStateError> {
        let Some(job) = self.jobs.iter_mut().find(|job| !job.pull_received) else {
            return Err(BoltPipelineStateError::MissingPendingRead);
        };
        job.pull_received = true;
        Ok(())
    }

    pub(crate) fn complete(
        &mut self,
        id: u64,
        terminal: Result<GatewayResponse, BoltError>,
    ) -> Result<(), BoltPipelineStateError> {
        let Some(job) = self.jobs.iter_mut().find(|job| job.id == id) else {
            return Ok(());
        };
        if job.terminal.is_some() {
            return Err(BoltPipelineStateError::DuplicateTerminal);
        }
        job.completed_at = Some(std::time::Instant::now());
        job.terminal = Some(terminal);
        Ok(())
    }

    pub(crate) fn take_run_ready(&mut self) -> Option<ReadyBoltRun> {
        let job = self.jobs.front_mut()?;
        if job.run_emitted || job.terminal.is_none() {
            return None;
        }
        job.run_emitted = true;
        Some(ReadyBoltRun {
            id: job.id,
            ordered_write_wait: job.completed_at.take().map(|completed| completed.elapsed()),
            terminal: job
                .terminal
                .clone()
                .expect("checked front job has a terminal result"),
        })
    }

    pub(crate) fn take_pull_ready(&mut self) -> Option<ReadyBoltPull> {
        let ready = self
            .jobs
            .front()
            .is_some_and(|job| job.pull_received && job.run_emitted && job.terminal.is_some());
        if !ready {
            return None;
        }
        let job = self.jobs.pop_front().expect("checked front job is present");
        self.pending_request_bytes = self.pending_request_bytes.saturating_sub(job.request_bytes);
        Some(ReadyBoltPull {
            id: job.id,
            terminal: job
                .terminal
                .expect("checked front job has a terminal result"),
        })
    }

    pub(crate) fn reset(&mut self) {
        for job in self.jobs.drain(..) {
            job.cancellation.cancel();
        }
        self.pending_request_bytes = 0;
    }
}

fn configure_bolt_socket(socket: &TcpStream) -> Result<(), io::Error> {
    socket.set_nodelay(true)
}

async fn serve_connection(
    mut socket: TcpStream,
    service: Arc<GatewayService>,
) -> Result<(), io::Error> {
    negotiate(&mut socket).await?;
    if service.config().bolt_read_pipeline_enabled() {
        return serve_pipelined_connection(socket, service).await;
    }
    serve_serial_connection(socket, service).await
}

async fn serve_serial_connection(
    mut socket: TcpStream,
    service: Arc<GatewayService>,
) -> Result<(), io::Error> {
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

const BOLT_READ_PIPELINE_MAX_PENDING: usize = 64;
const BOLT_READ_PIPELINE_MAX_REQUEST_BYTES: usize = 64 * 1024;
const BOLT_READ_PIPELINE_MAX_DEFERRED_MESSAGES: usize = 64;

async fn serve_pipelined_connection(
    socket: TcpStream,
    service: Arc<GatewayService>,
) -> Result<(), io::Error> {
    let request_metrics = service.request_metrics();
    let (reader, mut writer) = socket.into_split();
    let (message_sender, mut message_receiver) = mpsc::channel(64);
    let reader_task = tokio::spawn(read_bolt_messages(reader, message_sender));
    let result = serve_pipelined_events(
        &mut writer,
        service,
        &request_metrics,
        &mut message_receiver,
    )
    .await;
    reader_task.abort();
    let _ = reader_task.await;
    result
}

async fn read_bolt_messages(mut reader: OwnedReadHalf, sender: mpsc::Sender<Vec<u8>>) {
    while let Ok(Some(message)) = read_chunked_message(&mut reader).await {
        if sender.send(message).await.is_err() {
            return;
        }
    }
}

async fn serve_pipelined_events(
    writer: &mut OwnedWriteHalf,
    service: Arc<GatewayService>,
    request_metrics: &Arc<RequestStageMetrics>,
    message_receiver: &mut mpsc::Receiver<Vec<u8>>,
) -> Result<(), io::Error> {
    let mut pipeline = BoltReadPipeline::new(BoltReadPipelineLimits::new(
        BOLT_READ_PIPELINE_MAX_PENDING,
        BOLT_READ_PIPELINE_MAX_REQUEST_BYTES,
    ));
    let (completion_sender, mut completion_receiver) = mpsc::channel(64);
    let mut next_id = 1_u64;
    let mut serial_pending_rows = None;
    let mut deferred_messages = VecDeque::new();

    loop {
        let message = if let Some(message) = deferred_messages.pop_front() {
            message
        } else {
            loop {
                tokio::select! {
                    completion = completion_receiver.recv(), if pipeline.len() != 0 => {
                        if let Some((id, terminal)) = completion {
                            let _ = pipeline.complete(id, terminal);
                            flush_pipeline_ready(writer, request_metrics, &mut pipeline).await?;
                        }
                    }
                    message = message_receiver.recv() => {
                        let Some(message) = message else {
                            pipeline.reset();
                            return Ok(());
                        };
                        break message;
                    }
                }
            }
        };
        let timer = request_metrics.start(RequestStage::BoltDecode);
        let decoded = timer.finish_result(decode_message(&message))?;
        match decoded {
            BoltMessage::Hello => {
                write_success(writer, request_metrics, &[]).await?;
            }
            BoltMessage::Run {
                statement,
                parameters,
            } => {
                let statement_class = match service.classify_bolt_statement(&statement) {
                    Ok(class) => class,
                    Err(error) => {
                        write_failure(writer, request_metrics, &error).await?;
                        continue;
                    }
                };
                match statement_class {
                    BoltStatementClass::Read => {
                        let id = next_id;
                        next_id = next_id.wrapping_add(1).max(1);
                        let cancellation = GatewayCancellationToken::new();
                        if pipeline
                            .submit(id, message.len(), cancellation.clone())
                            .is_err()
                        {
                            write_failure(
                                writer,
                                request_metrics,
                                &BoltError::protocol("Bolt read pipeline backpressure"),
                            )
                            .await?;
                            continue;
                        }
                        let service = Arc::clone(&service);
                        let completion_sender = completion_sender.clone();
                        let enqueue_timer = request_metrics
                            .start_detail(RequestDetail::BoltReadPipelineEnqueueWait);
                        let request_metrics = Arc::clone(request_metrics);
                        tokio::spawn(async move {
                            let _permit = service.acquire_bolt_read_pipeline_permit().await;
                            enqueue_timer.finish(StageOutcome::Success);
                            let execution_timer = request_metrics
                                .start_detail(RequestDetail::BoltReadPipelineExecutionWait);
                            let terminal = service
                                .execute_statement(statement, parameters, None, &cancellation)
                                .await;
                            execution_timer.finish(if terminal.is_ok() {
                                StageOutcome::Success
                            } else if cancellation.is_cancelled() {
                                StageOutcome::Cancelled
                            } else {
                                StageOutcome::Error
                            });
                            let _ = completion_sender.send((id, terminal)).await;
                        });
                    }
                    BoltStatementClass::Barrier => {
                        match drain_pipeline_before_barrier(
                            writer,
                            request_metrics,
                            &mut pipeline,
                            &mut completion_receiver,
                            message_receiver,
                            &mut deferred_messages,
                        )
                        .await?
                        {
                            PipelineDrain::Drained => {}
                            PipelineDrain::Reset => {
                                serial_pending_rows = None;
                                continue;
                            }
                            PipelineDrain::Closed => return Ok(()),
                        }
                        match service
                            .execute_statement(
                                statement,
                                parameters,
                                None,
                                &GatewayCancellationToken::new(),
                            )
                            .await
                        {
                            Ok(GatewayResponse::Rows(rows))
                            | Ok(GatewayResponse::AnalyticsResult { rows, .. }) => {
                                let fields = rows.fields().to_vec();
                                serial_pending_rows = Some(rows);
                                write_success(
                                    writer,
                                    request_metrics,
                                    &[("fields", PackValue::Strings(fields))],
                                )
                                .await?;
                            }
                            Ok(_) => {
                                serial_pending_rows = None;
                                write_success(writer, request_metrics, &[]).await?;
                            }
                            Err(error) => {
                                serial_pending_rows = None;
                                write_failure(writer, request_metrics, &error).await?;
                            }
                        }
                    }
                }
            }
            BoltMessage::Pull => {
                if pipeline.mark_next_pull().is_ok() {
                    flush_pipeline_ready(writer, request_metrics, &mut pipeline).await?;
                } else {
                    if let Some(rows) = serial_pending_rows.take() {
                        for row in rows.rows() {
                            write_record(writer, request_metrics, row).await?;
                        }
                    }
                    write_success(
                        writer,
                        request_metrics,
                        &[("has_more", PackValue::Boolean(false))],
                    )
                    .await?;
                }
            }
            BoltMessage::Goodbye => {
                pipeline.reset();
                return Ok(());
            }
            BoltMessage::Reset => {
                pipeline.reset();
                serial_pending_rows = None;
                write_success(writer, request_metrics, &[]).await?;
            }
            BoltMessage::Unsupported => {
                write_failure(
                    writer,
                    request_metrics,
                    &BoltError::protocol("unsupported Bolt message signature"),
                )
                .await?;
            }
        }
    }
}

enum PipelineDrain {
    Drained,
    Reset,
    Closed,
}

async fn flush_pipeline_ready<W>(
    socket: &mut W,
    request_metrics: &Arc<RequestStageMetrics>,
    pipeline: &mut BoltReadPipeline,
) -> Result<(), io::Error>
where
    W: AsyncWrite + Unpin,
{
    loop {
        if let Some(ready) = pipeline.take_run_ready() {
            let _read_id = ready.id;
            if let Some(wait) = ready.ordered_write_wait {
                request_metrics.record_detail(
                    RequestDetail::BoltReadPipelineOrderedWriteWait,
                    if ready.terminal.is_ok() {
                        StageOutcome::Success
                    } else {
                        StageOutcome::Error
                    },
                    u64::try_from(wait.as_nanos()).unwrap_or(u64::MAX),
                );
            }
            write_run_terminal(socket, request_metrics, &ready.terminal).await?;
            continue;
        }
        if let Some(ready) = pipeline.take_pull_ready() {
            let _read_id = ready.id;
            write_pull_terminal(socket, request_metrics, &ready.terminal).await?;
            continue;
        }
        return Ok(());
    }
}

async fn drain_pipeline_before_barrier(
    writer: &mut OwnedWriteHalf,
    request_metrics: &Arc<RequestStageMetrics>,
    pipeline: &mut BoltReadPipeline,
    completion_receiver: &mut mpsc::Receiver<(u64, Result<GatewayResponse, BoltError>)>,
    message_receiver: &mut mpsc::Receiver<Vec<u8>>,
    deferred_messages: &mut VecDeque<Vec<u8>>,
) -> Result<PipelineDrain, io::Error> {
    while pipeline.len() != 0 {
        tokio::select! {
            completion = completion_receiver.recv() => {
                if let Some((id, terminal)) = completion {
                    let _ = pipeline.complete(id, terminal);
                    flush_pipeline_ready(writer, request_metrics, pipeline).await?;
                }
            }
            message = message_receiver.recv(), if deferred_messages.len() < BOLT_READ_PIPELINE_MAX_DEFERRED_MESSAGES => {
                let Some(message) = message else {
                    pipeline.reset();
                    return Ok(PipelineDrain::Closed);
                };
                let timer = request_metrics.start(RequestStage::BoltDecode);
                let decoded = timer.finish_result(decode_message(&message))?;
                match decoded {
                    BoltMessage::Reset => {
                        pipeline.reset();
                        write_success(writer, request_metrics, &[]).await?;
                        return Ok(PipelineDrain::Reset);
                    }
                    BoltMessage::Goodbye => {
                        pipeline.reset();
                        return Ok(PipelineDrain::Closed);
                    }
                    _ => {
                        deferred_messages.push_back(message);
                    }
                }
            }
        }
    }
    Ok(PipelineDrain::Drained)
}

async fn write_run_terminal<W>(
    socket: &mut W,
    request_metrics: &Arc<RequestStageMetrics>,
    terminal: &Result<GatewayResponse, BoltError>,
) -> Result<(), io::Error>
where
    W: AsyncWrite + Unpin,
{
    match terminal {
        Ok(GatewayResponse::Rows(rows)) | Ok(GatewayResponse::AnalyticsResult { rows, .. }) => {
            write_success(
                socket,
                request_metrics,
                &[("fields", PackValue::Strings(rows.fields().to_vec()))],
            )
            .await
        }
        Ok(_) => write_success(socket, request_metrics, &[]).await,
        Err(error) => write_failure(socket, request_metrics, error).await,
    }
}

async fn write_pull_terminal<W>(
    socket: &mut W,
    request_metrics: &Arc<RequestStageMetrics>,
    terminal: &Result<GatewayResponse, BoltError>,
) -> Result<(), io::Error>
where
    W: AsyncWrite + Unpin,
{
    if let Ok(GatewayResponse::Rows(rows) | GatewayResponse::AnalyticsResult { rows, .. }) =
        terminal
    {
        for row in rows.rows() {
            write_record(socket, request_metrics, row).await?;
        }
    }
    write_success(
        socket,
        request_metrics,
        &[("has_more", PackValue::Boolean(false))],
    )
    .await
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

async fn read_chunked_message<R>(socket: &mut R) -> Result<Option<Vec<u8>>, io::Error>
where
    R: AsyncRead + Unpin,
{
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

async fn write_chunked_message<W>(socket: &mut W, message: &[u8]) -> Result<(), io::Error>
where
    W: AsyncWrite + Unpin,
{
    socket.write_all(&encode_chunked_message(message)).await
}

fn encode_chunked_message(message: &[u8]) -> Vec<u8> {
    let chunk_count = message.len().div_ceil(usize::from(u16::MAX));
    let mut encoded = Vec::with_capacity(message.len() + chunk_count * 2 + 2);
    for chunk in message.chunks(usize::from(u16::MAX)) {
        encoded.extend_from_slice(&(chunk.len() as u16).to_be_bytes());
        encoded.extend_from_slice(chunk);
    }
    encoded.extend_from_slice(&[0, 0]);
    encoded
}

enum PackValue {
    Boolean(bool),
    Strings(Vec<String>),
}

async fn write_success<W>(
    socket: &mut W,
    request_metrics: &Arc<RequestStageMetrics>,
    metadata: &[(&str, PackValue)],
) -> Result<(), io::Error>
where
    W: AsyncWrite + Unpin,
{
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

async fn write_failure<W>(
    socket: &mut W,
    request_metrics: &Arc<RequestStageMetrics>,
    error: &BoltError,
) -> Result<(), io::Error>
where
    W: AsyncWrite + Unpin,
{
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

async fn write_record<W>(
    socket: &mut W,
    request_metrics: &Arc<RequestStageMetrics>,
    row: &[GatewayValue],
) -> Result<(), io::Error>
where
    W: AsyncWrite + Unpin,
{
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
    pub(crate) fn from_code(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
        }
    }

    pub(crate) fn protocol(message: impl Into<String>) -> Self {
        Self::from_code("DTG-GATEWAY-BOLT-PROTOCOL", message)
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

#[cfg(test)]
mod tests {
    use super::{
        BoltPipelineAdmission, BoltReadPipeline, BoltReadPipelineLimits, configure_bolt_socket,
        encode_chunked_message,
    };
    use dtg_execution::{GatewayCancellationToken, GatewayResponse};
    use tokio::net::{TcpListener, TcpStream};

    #[tokio::test]
    async fn bolt_socket_configuration_disables_nagle() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = tokio::spawn(TcpStream::connect(listener.local_addr().unwrap()));
        let (server, _) = listener.accept().await.unwrap();
        let _client = client.await.unwrap().unwrap();

        configure_bolt_socket(&server).unwrap();

        assert!(server.nodelay().unwrap());
    }

    #[test]
    fn pipelined_reads_complete_out_of_order_but_are_released_in_input_order() {
        let mut pipeline = BoltReadPipeline::new(BoltReadPipelineLimits::new(2, 32));
        let first = GatewayCancellationToken::new();
        let second = GatewayCancellationToken::new();
        pipeline.submit(1, 8, first).unwrap();
        pipeline.mark_next_pull().unwrap();
        pipeline.submit(2, 8, second).unwrap();
        pipeline.mark_next_pull().unwrap();

        pipeline
            .complete(2, Ok(GatewayResponse::Acknowledged))
            .unwrap();
        assert!(pipeline.take_run_ready().is_none());
        pipeline
            .complete(1, Ok(GatewayResponse::Acknowledged))
            .unwrap();

        let first_run = pipeline.take_run_ready().unwrap();
        assert!(pipeline.take_run_ready().is_none());
        let first_ready = pipeline.take_pull_ready().unwrap();
        let second_run = pipeline.take_run_ready().unwrap();
        let second_ready = pipeline.take_pull_ready().unwrap();
        assert_eq!(first_run.id, 1);
        assert_eq!(second_run.id, 2);
        assert_eq!(first_ready.id, 1);
        assert_eq!(second_ready.id, 2);
        assert!(matches!(
            first_ready.terminal,
            Ok(GatewayResponse::Acknowledged)
        ));
        assert!(matches!(
            second_ready.terminal,
            Ok(GatewayResponse::Acknowledged)
        ));
    }

    #[test]
    fn pipeline_rejects_admission_without_evicting_existing_reads() {
        let mut pipeline = BoltReadPipeline::new(BoltReadPipelineLimits::new(1, 8));
        pipeline
            .submit(1, 8, GatewayCancellationToken::new())
            .unwrap();
        assert_eq!(
            pipeline
                .submit(2, 1, GatewayCancellationToken::new())
                .unwrap_err(),
            BoltPipelineAdmission::Full
        );
        assert_eq!(pipeline.len(), 1);
    }

    #[test]
    fn reset_cancels_active_reads_and_late_completion_is_ignored() {
        let mut pipeline = BoltReadPipeline::new(BoltReadPipelineLimits::new(2, 32));
        let cancellation = GatewayCancellationToken::new();
        pipeline.submit(1, 8, cancellation.clone()).unwrap();

        pipeline.reset();
        assert!(cancellation.is_cancelled());
        assert!(
            pipeline
                .complete(1, Ok(GatewayResponse::Acknowledged))
                .is_ok()
        );
        assert!(pipeline.take_run_ready().is_none());
        assert!(pipeline.take_pull_ready().is_none());
    }

    #[test]
    fn bolt_frame_is_encoded_as_one_contiguous_buffer() {
        assert_eq!(
            encode_chunked_message(&[0xb1, 0x70, 0xa0]),
            vec![0, 3, 0xb1, 0x70, 0xa0, 0, 0]
        );
    }
}
