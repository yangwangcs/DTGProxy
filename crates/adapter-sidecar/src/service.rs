use std::collections::{BTreeMap, VecDeque};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use adapter_registry::{
    AdapterFactory, AdapterFactoryError, AdapterFactoryFuture, AdapterOpenRequest, AdapterRegistry,
    AdapterRestoreFuture, AdapterRestoreSession, AdapterRestoreSessionFuture, SecretString,
};
use storage_api::{
    AdapterDescriptorV1, AdapterError, AdapterRequirement, LogicalSnapshotAccumulator,
    LogicalSnapshotChunkV1, LogicalSnapshotHeaderV1, LogicalSnapshotManifestV1,
    LogicalSnapshotReader, StorageAdapter,
};

use super::{
    BeginRestoreRequest, ExportStarted, FeatureSet, HelloResponse, MAX_FRAME_PAYLOAD_BYTES,
    ProtocolError, PublicAdapterOpenRequest, RemoteError, RemoteErrorCode, Request, Response,
    RestoreComplete, RestoreStarted, SidecarTransport, TcpSidecarConfig, TcpSidecarServerConfig,
    TcpSidecarServerHandle, TcpSidecarTransport, WIRE_VERSION, block_on_dispatch,
    read_frame_or_eof, write_frame,
};

pub struct SidecarRestoreBackend {
    registry: Arc<AdapterRegistry>,
    provider: String,
    requirement: AdapterRequirement,
    prospective_descriptor: AdapterDescriptorV1,
    secrets: BTreeMap<String, String>,
}

impl SidecarRestoreBackend {
    #[must_use]
    pub fn new(
        registry: Arc<AdapterRegistry>,
        provider: impl Into<String>,
        requirement: AdapterRequirement,
        prospective_descriptor: AdapterDescriptorV1,
    ) -> Self {
        Self {
            registry,
            provider: provider.into(),
            requirement,
            prospective_descriptor,
            secrets: BTreeMap::new(),
        }
    }

    #[must_use]
    pub fn with_secret(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.secrets.insert(name.into(), value.into());
        self
    }

    fn open_request(&self, request: &PublicAdapterOpenRequest) -> AdapterOpenRequest {
        let mut opened = AdapterOpenRequest::new(request.instance_id());
        for (name, value) in request.parameters() {
            opened = opened.with_parameter(name, value);
        }
        for (name, value) in &self.secrets {
            opened = opened.with_secret(name, SecretString::new(value));
        }
        opened
    }
}

enum SnapshotSession {
    Export {
        started: ExportStarted,
        chunks: VecDeque<LogicalSnapshotChunkV1>,
        manifest: LogicalSnapshotManifestV1,
    },
    Restore {
        request: BeginRestoreRequest,
        chunks: Vec<LogicalSnapshotChunkV1>,
        accumulator: Box<LogicalSnapshotAccumulator>,
        last_chunk: Option<(u64, [u8; 32])>,
    },
}

pub struct SidecarService {
    active: RwLock<Arc<dyn StorageAdapter>>,
    restore_backend: Option<SidecarRestoreBackend>,
    sessions: Mutex<BTreeMap<u128, SnapshotSession>>,
    next_session: AtomicU64,
}

impl SidecarService {
    #[must_use]
    pub fn new(
        active: Arc<dyn StorageAdapter>,
        restore_backend: Option<SidecarRestoreBackend>,
    ) -> Self {
        Self {
            active: RwLock::new(active),
            restore_backend,
            sessions: Mutex::new(BTreeMap::new()),
            next_session: AtomicU64::new(1),
        }
    }

    fn active_adapter(&self) -> Result<Arc<dyn StorageAdapter>, RemoteError> {
        self.active
            .read()
            .map(|active| Arc::clone(&active))
            .map_err(|_| {
                remote_error(
                    RemoteErrorCode::ServiceFaulted,
                    "active Adapter lock is poisoned",
                )
            })
    }

    fn supported_features(&self) -> Result<FeatureSet, RemoteError> {
        let active = self.active_adapter()?;
        let mut features = FeatureSet::BASE_ADAPTER_V1;
        if active.capabilities().logical_export {
            features = features.union(FeatureSet::LOGICAL_EXPORT_SESSION_V1);
        }
        if self.restore_backend.is_some() {
            features = features
                .union(FeatureSet::LOGICAL_RESTORE_SESSION_V1)
                .union(FeatureSet::RESUMABLE_ORDINAL_REPLAY_V1);
        }
        Ok(features)
    }

    fn session_id(&self) -> u128 {
        u128::from(self.next_session.fetch_add(1, Ordering::Relaxed))
    }

    pub async fn dispatch(&self, request: Request) -> Response {
        match self.dispatch_inner(request).await {
            Ok(response) => response,
            Err(error) => Response::Error(error),
        }
    }

    async fn dispatch_inner(&self, request: Request) -> Result<Response, RemoteError> {
        match request {
            Request::Hello(request) => {
                let supported = self.supported_features()?;
                if !supported.contains(request.required_features) {
                    return Err(remote_error(
                        RemoteErrorCode::FeatureUnsupported,
                        "required Sidecar Feature is unavailable",
                    ));
                }
                Ok(Response::Hello(HelloResponse {
                    wire_version: WIRE_VERSION,
                    negotiated_features: request
                        .required_features
                        .union(request.optional_features)
                        .intersection(supported),
                    max_payload_bytes: u32::try_from(MAX_FRAME_PAYLOAD_BYTES)
                        .expect("frame maximum fits u32"),
                    max_chunk_bytes: u32::try_from(storage_api::MAX_LOGICAL_SNAPSHOT_CHUNK_BYTES)
                        .expect("chunk maximum fits u32"),
                    max_chunk_entries: u32::try_from(
                        storage_api::MAX_LOGICAL_SNAPSHOT_CHUNK_ENTRIES,
                    )
                    .expect("entry maximum fits u32"),
                    snapshot_format_version: storage_api::LOGICAL_SNAPSHOT_FORMAT_VERSION,
                }))
            }
            Request::BeginExport(request) => self.begin_export(request).await,
            Request::ExportNext {
                session_id,
                expected_ordinal,
            } => self.export_next(session_id, expected_ordinal),
            Request::BeginRestore(request) => self.begin_restore(request),
            Request::RestoreChunk { session_id, chunk } => self.restore_chunk(session_id, chunk),
            Request::FinishRestore {
                session_id,
                manifest,
            } => self.finish_restore(session_id, manifest).await,
            Request::AbortSession { session_id } => self.abort_session(session_id),
            request => Ok(super::dispatch_request(self.active_adapter()?.as_ref(), request).await),
        }
    }

    async fn begin_export(
        &self,
        request: super::BeginExportRequest,
    ) -> Result<Response, RemoteError> {
        let active = self.active_adapter()?;
        let mut reader = active
            .begin_logical_export(request.limits)
            .await
            .map_err(adapter_remote_error)?;
        if request
            .expected_applied_log_index
            .is_some_and(|expected| expected != reader.header().applied_log_index())
        {
            return Err(remote_error(
                RemoteErrorCode::RequestReplayMismatch,
                "export applied-index fence does not match the Adapter",
            ));
        }
        let header = reader.header().clone();
        let mut chunks = VecDeque::new();
        while let Some(chunk) = reader.next_chunk().await.map_err(adapter_remote_error)? {
            chunks.push_back(chunk);
        }
        let manifest = reader.finish().await.map_err(adapter_remote_error)?;
        let session_id = self.session_id();
        let started = ExportStarted {
            session_id,
            header,
            limits: request.limits,
        };
        self.sessions
            .lock()
            .map_err(|_| remote_error(RemoteErrorCode::ServiceFaulted, "session lock is poisoned"))?
            .insert(
                session_id,
                SnapshotSession::Export {
                    started: started.clone(),
                    chunks,
                    manifest,
                },
            );
        Ok(Response::ExportStarted(started))
    }

    fn export_next(
        &self,
        session_id: u128,
        expected_ordinal: u64,
    ) -> Result<Response, RemoteError> {
        let mut sessions = self.sessions.lock().map_err(|_| {
            remote_error(RemoteErrorCode::ServiceFaulted, "session lock is poisoned")
        })?;
        let session = sessions.get_mut(&session_id).ok_or_else(|| {
            remote_error(RemoteErrorCode::SessionUnknown, "unknown export session")
        })?;
        let SnapshotSession::Export {
            started,
            chunks,
            manifest,
        } = session
        else {
            return Err(remote_error(
                RemoteErrorCode::SessionKindMismatch,
                "session is not an export",
            ));
        };
        if let Some(chunk) = chunks.front() {
            if expected_ordinal != chunk.ordinal() {
                let code = if expected_ordinal < chunk.ordinal() {
                    RemoteErrorCode::OrdinalRegression
                } else {
                    RemoteErrorCode::OrdinalGap
                };
                return Err(remote_error(code, "unexpected export chunk ordinal"));
            }
            return Ok(Response::ExportChunk {
                session_id,
                chunk: chunks.pop_front().expect("front chunk exists"),
            });
        }
        if expected_ordinal != manifest.total_chunks() {
            return Err(remote_error(
                RemoteErrorCode::OrdinalGap,
                "export completion ordinal does not match manifest",
            ));
        }
        let response = Response::ExportComplete {
            session_id: started.session_id,
            manifest: manifest.clone(),
        };
        sessions.remove(&session_id);
        Ok(response)
    }

    fn begin_restore(&self, request: BeginRestoreRequest) -> Result<Response, RemoteError> {
        let backend = self.restore_backend.as_ref().ok_or_else(|| {
            remote_error(
                RemoteErrorCode::FeatureUnsupported,
                "restore is not configured on this Sidecar",
            )
        })?;
        let session_id = self.session_id();
        self.sessions
            .lock()
            .map_err(|_| remote_error(RemoteErrorCode::ServiceFaulted, "session lock is poisoned"))?
            .insert(
                session_id,
                SnapshotSession::Restore {
                    accumulator: Box::new(LogicalSnapshotAccumulator::new(request.header.clone())),
                    request,
                    chunks: Vec::new(),
                    last_chunk: None,
                },
            );
        Ok(Response::RestoreStarted(RestoreStarted {
            session_id,
            prospective_descriptor: backend.prospective_descriptor.clone(),
            max_chunk_bytes: u32::try_from(storage_api::MAX_LOGICAL_SNAPSHOT_CHUNK_BYTES)
                .expect("chunk maximum fits u32"),
            max_chunk_entries: u32::try_from(storage_api::MAX_LOGICAL_SNAPSHOT_CHUNK_ENTRIES)
                .expect("entry maximum fits u32"),
        }))
    }

    fn restore_chunk(
        &self,
        session_id: u128,
        chunk: LogicalSnapshotChunkV1,
    ) -> Result<Response, RemoteError> {
        let mut sessions = self.sessions.lock().map_err(|_| {
            remote_error(RemoteErrorCode::ServiceFaulted, "session lock is poisoned")
        })?;
        let SnapshotSession::Restore {
            chunks,
            accumulator,
            last_chunk,
            ..
        } = sessions.get_mut(&session_id).ok_or_else(|| {
            remote_error(RemoteErrorCode::SessionUnknown, "unknown restore session")
        })?
        else {
            return Err(remote_error(
                RemoteErrorCode::SessionKindMismatch,
                "session is not a restore",
            ));
        };
        if *last_chunk == Some((chunk.ordinal(), chunk.digest())) {
            return Ok(Response::RestoreChunkAccepted {
                session_id,
                ordinal: chunk.ordinal(),
                digest: chunk.digest(),
            });
        }
        accumulator.observe(&chunk).map_err(|error| {
            remote_error(RemoteErrorCode::ChunkDigestMismatch, &error.to_string())
        })?;
        *last_chunk = Some((chunk.ordinal(), chunk.digest()));
        let response = Response::RestoreChunkAccepted {
            session_id,
            ordinal: chunk.ordinal(),
            digest: chunk.digest(),
        };
        chunks.push(chunk);
        Ok(response)
    }

    async fn finish_restore(
        &self,
        session_id: u128,
        manifest: LogicalSnapshotManifestV1,
    ) -> Result<Response, RemoteError> {
        let session = self
            .sessions
            .lock()
            .map_err(|_| remote_error(RemoteErrorCode::ServiceFaulted, "session lock is poisoned"))?
            .remove(&session_id)
            .ok_or_else(|| {
                remote_error(RemoteErrorCode::SessionUnknown, "unknown restore session")
            })?;
        let SnapshotSession::Restore {
            request,
            chunks,
            accumulator,
            ..
        } = session
        else {
            return Err(remote_error(
                RemoteErrorCode::SessionKindMismatch,
                "session is not a restore",
            ));
        };
        (*accumulator).verify(&manifest).map_err(|error| {
            remote_error(RemoteErrorCode::TerminalReplayMismatch, &error.to_string())
        })?;
        let backend = self.restore_backend.as_ref().ok_or_else(|| {
            remote_error(
                RemoteErrorCode::FeatureUnsupported,
                "restore backend disappeared",
            )
        })?;
        let open_request = backend.open_request(&request.target);
        let reader = Box::new(BufferedSnapshotReader::new(
            request.header,
            chunks,
            manifest,
        ));
        let opened = backend
            .registry
            .restore(
                &backend.provider,
                &open_request,
                backend.requirement,
                reader,
            )
            .await
            .map_err(|error| remote_error(RemoteErrorCode::ServiceFaulted, &error.to_string()))?;
        let descriptor = opened.descriptor().clone();
        let adapter = opened.into_adapter();
        let applied_log_index = adapter.applied_log_index().map_err(adapter_remote_error)?;
        *self.active.write().map_err(|_| {
            remote_error(
                RemoteErrorCode::ServiceFaulted,
                "active Adapter lock is poisoned",
            )
        })? = adapter;
        Ok(Response::RestoreComplete(RestoreComplete {
            session_id,
            final_descriptor: descriptor,
            applied_log_index,
        }))
    }

    fn abort_session(&self, session_id: u128) -> Result<Response, RemoteError> {
        self.sessions
            .lock()
            .map_err(|_| remote_error(RemoteErrorCode::ServiceFaulted, "session lock is poisoned"))?
            .remove(&session_id)
            .ok_or_else(|| remote_error(RemoteErrorCode::SessionUnknown, "unknown session"))?;
        Ok(Response::SessionAborted { session_id })
    }
}

struct BufferedSnapshotReader {
    header: LogicalSnapshotHeaderV1,
    chunks: VecDeque<LogicalSnapshotChunkV1>,
    manifest: LogicalSnapshotManifestV1,
    accumulator: LogicalSnapshotAccumulator,
    exhausted: bool,
}

impl BufferedSnapshotReader {
    fn new(
        header: LogicalSnapshotHeaderV1,
        chunks: Vec<LogicalSnapshotChunkV1>,
        manifest: LogicalSnapshotManifestV1,
    ) -> Self {
        Self {
            accumulator: LogicalSnapshotAccumulator::new(header.clone()),
            header,
            chunks: chunks.into(),
            manifest,
            exhausted: false,
        }
    }
}

impl LogicalSnapshotReader for BufferedSnapshotReader {
    fn header(&self) -> &LogicalSnapshotHeaderV1 {
        &self.header
    }

    fn next_chunk<'a>(
        &'a mut self,
    ) -> storage_api::AdapterFuture<'a, Option<LogicalSnapshotChunkV1>> {
        Box::pin(async move {
            let chunk = self.chunks.pop_front();
            if let Some(chunk) = &chunk {
                self.accumulator.observe(chunk)?;
            } else {
                self.exhausted = true;
            }
            Ok(chunk)
        })
    }

    fn finish<'a>(self: Box<Self>) -> storage_api::AdapterFuture<'a, LogicalSnapshotManifestV1>
    where
        Self: 'a,
    {
        Box::pin(async move {
            if !self.exhausted {
                return Err(storage_api::LogicalSnapshotError::ExportNotExhausted.into());
            }
            self.accumulator.verify(&self.manifest)?;
            Ok(self.manifest)
        })
    }
}

pub fn spawn_stateful_tcp_sidecar_server(
    config: TcpSidecarServerConfig,
    service: Arc<SidecarService>,
) -> Result<TcpSidecarServerHandle, super::SidecarServerError> {
    let listener = TcpListener::bind(config.bind_address)
        .map_err(|error| super::SidecarServerError::Io(error.to_string()))?;
    listener
        .set_nonblocking(true)
        .map_err(|error| super::SidecarServerError::Io(error.to_string()))?;
    let local_addr = listener
        .local_addr()
        .map_err(|error| super::SidecarServerError::Io(error.to_string()))?;
    let shutdown = Arc::new(AtomicBool::new(false));
    let active_connections = Arc::new(Mutex::new(BTreeMap::new()));
    let thread_shutdown = Arc::clone(&shutdown);
    let thread_connections = Arc::clone(&active_connections);
    let server_thread = std::thread::Builder::new()
        .name("dtg-stateful-sidecar-accept".into())
        .spawn(move || {
            let mut workers = Vec::new();
            while !thread_shutdown.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        stream
                            .set_nonblocking(false)
                            .map_err(|error| super::SidecarServerError::Io(error.to_string()))?;
                        if workers.len() >= config.worker_threads {
                            let _ = stream.shutdown(Shutdown::Both);
                            continue;
                        }
                        let connection_id =
                            super::NEXT_SERVER_CONNECTION_ID.fetch_add(1, Ordering::Relaxed);
                        let control = stream
                            .try_clone()
                            .map_err(|error| super::SidecarServerError::Io(error.to_string()))?;
                        thread_connections
                            .lock()
                            .map_err(|_| super::SidecarServerError::StatePoisoned)?
                            .insert(connection_id, control);
                        let worker_service = Arc::clone(&service);
                        let worker_connections = Arc::clone(&thread_connections);
                        workers.push(std::thread::spawn(move || {
                            let result = serve_stateful_connection(stream, &worker_service);
                            if let Ok(mut connections) = worker_connections.lock() {
                                connections.remove(&connection_id);
                            }
                            result
                        }));
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(config.accept_poll_interval);
                    }
                    Err(error) => {
                        return Err(super::SidecarServerError::Io(error.to_string()));
                    }
                }
            }
            for stream in thread_connections
                .lock()
                .map_err(|_| super::SidecarServerError::StatePoisoned)?
                .values()
            {
                let _ = stream.shutdown(Shutdown::Both);
            }
            for worker in workers {
                worker
                    .join()
                    .map_err(|_| super::SidecarServerError::WorkerPanicked)?
                    .map_err(|error| super::SidecarServerError::Io(error.to_string()))?;
            }
            Ok(())
        })
        .map_err(|error| super::SidecarServerError::Io(error.to_string()))?;
    Ok(TcpSidecarServerHandle {
        local_addr,
        shutdown,
        active_connections,
        server_thread: Some(server_thread),
    })
}

fn serve_stateful_connection(
    mut stream: TcpStream,
    service: &SidecarService,
) -> Result<(), ProtocolError> {
    while let Some(frame) = read_frame_or_eof::<_, Request>(&mut stream)? {
        let request_id = frame.request_id();
        let response = block_on_dispatch(service.dispatch(frame.into_message()));
        write_frame(&mut stream, request_id, &response)?;
    }
    Ok(())
}

pub struct TcpSidecarAdapterFactory;

impl AdapterFactory for TcpSidecarAdapterFactory {
    fn provider_name(&self) -> &str {
        "sidecar"
    }

    fn open<'a>(&'a self, request: &'a AdapterOpenRequest) -> AdapterFactoryFuture<'a> {
        Box::pin(async move {
            let transport = transport_from_request(request)?;
            let adapter = super::SidecarAdapter::connect(transport)
                .await
                .map_err(factory_error)?;
            Ok(Arc::new(adapter) as Arc<dyn StorageAdapter>)
        })
    }

    fn begin_restore<'a>(
        &'a self,
        request: &'a AdapterOpenRequest,
        header: LogicalSnapshotHeaderV1,
    ) -> AdapterRestoreSessionFuture<'a> {
        Box::pin(async move {
            let transport = transport_from_request(request)?;
            match transport
                .call(Request::Hello(super::HelloRequest::restore_client()))
                .await
                .map_err(factory_error)?
            {
                Response::Hello(response)
                    if response
                        .negotiated_features
                        .contains(FeatureSet::LOGICAL_RESTORE_SESSION_V1) => {}
                Response::Error(error) => return Err(factory_error(error)),
                _ => return Err(AdapterFactoryError::new("Sidecar restore Hello failed")),
            }
            let mut target = PublicAdapterOpenRequest::new(request.instance_id());
            for (name, value) in request.public_parameters() {
                if !is_transport_parameter(name) {
                    target = target.with_parameter(name, value);
                }
            }
            let response = transport
                .call(Request::BeginRestore(BeginRestoreRequest {
                    header,
                    target,
                }))
                .await
                .map_err(factory_error)?;
            match response {
                Response::RestoreStarted(started) => Ok(Box::new(RemoteRestoreSession {
                    transport: Some(transport),
                    session_id: started.session_id,
                    descriptor: started.prospective_descriptor,
                    finished: false,
                })
                    as Box<dyn AdapterRestoreSession + 'a>),
                Response::Error(error) => Err(factory_error(error)),
                _ => Err(AdapterFactoryError::new(
                    "Sidecar begin-restore returned an unexpected response",
                )),
            }
        })
    }
}

struct RemoteRestoreSession {
    transport: Option<TcpSidecarTransport>,
    session_id: u128,
    descriptor: AdapterDescriptorV1,
    finished: bool,
}

impl AdapterRestoreSession for RemoteRestoreSession {
    fn descriptor(&self) -> AdapterDescriptorV1 {
        self.descriptor.clone()
    }

    fn write_chunk<'a>(
        &'a mut self,
        chunk: LogicalSnapshotChunkV1,
    ) -> AdapterRestoreFuture<'a, ()> {
        Box::pin(async move {
            let ordinal = chunk.ordinal();
            let digest = chunk.digest();
            let response = self
                .transport
                .as_ref()
                .ok_or_else(|| AdapterFactoryError::new("restore session is finished"))?
                .call(Request::RestoreChunk {
                    session_id: self.session_id,
                    chunk,
                })
                .await
                .map_err(factory_error)?;
            match response {
                Response::RestoreChunkAccepted {
                    session_id,
                    ordinal: accepted_ordinal,
                    digest: accepted_digest,
                } if session_id == self.session_id
                    && accepted_ordinal == ordinal
                    && accepted_digest == digest =>
                {
                    Ok(())
                }
                Response::Error(error) => Err(factory_error(error)),
                _ => Err(AdapterFactoryError::new(
                    "Sidecar restore-chunk acknowledgement mismatch",
                )),
            }
        })
    }

    fn finish<'a>(
        mut self: Box<Self>,
        manifest: LogicalSnapshotManifestV1,
    ) -> AdapterRestoreFuture<'a, Arc<dyn StorageAdapter>>
    where
        Self: 'a,
    {
        Box::pin(async move {
            let transport = self
                .transport
                .take()
                .ok_or_else(|| AdapterFactoryError::new("restore session is finished"))?;
            let response = transport
                .call(Request::FinishRestore {
                    session_id: self.session_id,
                    manifest,
                })
                .await
                .map_err(factory_error)?;
            match response {
                Response::RestoreComplete(complete)
                    if complete.session_id == self.session_id
                        && complete.final_descriptor == self.descriptor => {}
                Response::Error(error) => return Err(factory_error(error)),
                _ => {
                    return Err(AdapterFactoryError::new(
                        "Sidecar finish-restore response mismatch",
                    ));
                }
            }
            self.finished = true;
            let adapter = super::SidecarAdapter::connect(transport)
                .await
                .map_err(factory_error)?;
            Ok(Arc::new(adapter) as Arc<dyn StorageAdapter>)
        })
    }

    fn abort<'a>(mut self: Box<Self>) -> AdapterRestoreFuture<'a, ()>
    where
        Self: 'a,
    {
        Box::pin(async move {
            let transport = self
                .transport
                .take()
                .ok_or_else(|| AdapterFactoryError::new("restore session is finished"))?;
            match transport
                .call(Request::AbortSession {
                    session_id: self.session_id,
                })
                .await
                .map_err(factory_error)?
            {
                Response::SessionAborted { session_id } if session_id == self.session_id => {
                    self.finished = true;
                    Ok(())
                }
                Response::Error(error) => Err(factory_error(error)),
                _ => Err(AdapterFactoryError::new("Sidecar abort response mismatch")),
            }
        })
    }
}

fn transport_from_request(
    request: &AdapterOpenRequest,
) -> Result<TcpSidecarTransport, AdapterFactoryError> {
    let endpoint = request
        .parameter("endpoint")
        .ok_or_else(|| AdapterFactoryError::new("Sidecar Adapter requires endpoint"))?
        .parse::<SocketAddr>()
        .map_err(|_| AdapterFactoryError::new("invalid Sidecar endpoint"))?;
    let pool_size = request
        .parameter("pool_size")
        .unwrap_or("2")
        .parse::<usize>()
        .map_err(|_| AdapterFactoryError::new("invalid Sidecar pool_size"))?;
    let config = TcpSidecarConfig::new(endpoint)
        .with_pool_size(pool_size)
        .map_err(factory_error)?;
    TcpSidecarTransport::connect(config).map_err(factory_error)
}

fn is_transport_parameter(name: &str) -> bool {
    matches!(
        name,
        "endpoint" | "pool_size" | "connect_timeout_ms" | "read_timeout_ms" | "write_timeout_ms"
    )
}

fn adapter_remote_error(error: AdapterError) -> RemoteError {
    RemoteError {
        code: RemoteErrorCode::ServiceFaulted as u32,
        message: error.to_string(),
        retryable: false,
    }
}

fn remote_error(code: RemoteErrorCode, message: &str) -> RemoteError {
    RemoteError {
        code: code as u32,
        message: message.to_owned(),
        retryable: false,
    }
}

fn factory_error(error: impl ToString) -> AdapterFactoryError {
    AdapterFactoryError::new(error.to_string())
}
