use std::collections::{BTreeMap, VecDeque};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError, TrySendError};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use adapter_registry::{
    AdapterFactory, AdapterFactoryError, AdapterFactoryFuture, AdapterOpenRequest, AdapterRegistry,
    AdapterRestoreFuture, AdapterRestoreSession, AdapterRestoreSessionFuture, RegistryError,
    SecretString,
};
use storage_api::{
    AdapterDescriptorV1, AdapterError, AdapterRequirement, CandidateScanPage, CandidateScanRequest,
    CanonicalScanPage, CanonicalScanRequest, KeySpan, KeyValue, LogicalKey,
    LogicalSnapshotAccumulator, LogicalSnapshotChunkV1, LogicalSnapshotHeaderV1,
    LogicalSnapshotManifestV1, LogicalSnapshotReader, PushdownGuarantee, StorageAdapter,
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

pub const MAX_ACTIVE_SNAPSHOT_SESSIONS: usize = 64;
pub const MAX_PENDING_READ_VIEW_COMMANDS: usize = 4;
pub const SNAPSHOT_SESSION_IDLE_TIMEOUT: Duration = Duration::from_secs(5 * 60);

enum SnapshotSession {
    Reserved {
        last_activity: Instant,
    },
    Export {
        last_activity: Instant,
        started: ExportStarted,
        chunks: VecDeque<LogicalSnapshotChunkV1>,
        manifest: LogicalSnapshotManifestV1,
    },
    Restore {
        last_activity: Instant,
        request: BeginRestoreRequest,
        chunks: Vec<LogicalSnapshotChunkV1>,
        accumulator: Box<LogicalSnapshotAccumulator>,
        last_chunk: Option<(u64, [u8; 32])>,
    },
    ReadView {
        last_activity: Instant,
        worker: ReadViewWorker,
    },
}

impl SnapshotSession {
    fn last_activity(&self) -> Instant {
        match self {
            Self::Reserved { last_activity }
            | Self::Export { last_activity, .. }
            | Self::Restore { last_activity, .. }
            | Self::ReadView { last_activity, .. } => *last_activity,
        }
    }

    fn touch(&mut self, now: Instant) {
        match self {
            Self::Reserved { last_activity }
            | Self::Export { last_activity, .. }
            | Self::Restore { last_activity, .. }
            | Self::ReadView { last_activity, .. } => *last_activity = now,
        }
    }
}

enum ReadViewCommand {
    MultiGet {
        keys: Vec<LogicalKey>,
        reply: mpsc::SyncSender<Result<Vec<Option<Vec<u8>>>, RemoteError>>,
    },
    Scan {
        span: KeySpan,
        reply: mpsc::SyncSender<Result<Vec<KeyValue>, RemoteError>>,
    },
    CanonicalScan {
        request: CanonicalScanRequest,
        reply: mpsc::SyncSender<Result<CanonicalScanPage, RemoteError>>,
    },
    CandidateScan {
        request: CandidateScanRequest,
        reply: mpsc::SyncSender<Result<CandidateScanPage, RemoteError>>,
    },
}

struct ReadViewWorker {
    commands: Option<mpsc::SyncSender<ReadViewCommand>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

#[derive(Clone)]
struct ReadViewHandle {
    commands: mpsc::SyncSender<ReadViewCommand>,
}

impl ReadViewWorker {
    fn spawn(adapter: Arc<dyn StorageAdapter>) -> Result<(Self, u64), RemoteError> {
        let (commands, receiver) = mpsc::sync_channel(MAX_PENDING_READ_VIEW_COMMANDS);
        let (started, startup) = mpsc::sync_channel(1);
        let thread = std::thread::Builder::new()
            .name("dtg-sidecar-read-view".into())
            .spawn(move || {
                let binding = match adapter.read_snapshot_binding() {
                    Ok(binding) => binding,
                    Err(error) => {
                        let _ = started.send(Err(read_view_remote_error(error)));
                        return;
                    }
                };
                let snapshot = match binding.as_ref() {
                    Some(binding) => block_on_dispatch(binding.owner().begin_read_snapshot()),
                    None => block_on_dispatch(adapter.begin_read_snapshot()),
                };
                let snapshot = match snapshot {
                    Ok(snapshot) => snapshot,
                    Err(error) => {
                        let _ = started.send(Err(read_view_remote_error(error)));
                        return;
                    }
                };
                if started.send(Ok(snapshot.applied_log_index())).is_err() {
                    return;
                }
                while let Ok(command) = receiver.recv() {
                    match command {
                        ReadViewCommand::MultiGet { keys, reply } => {
                            let result = block_on_dispatch(snapshot.multi_get(&keys))
                                .map_err(|error| super::encode_adapter_error(&error));
                            let _ = reply.send(result);
                        }
                        ReadViewCommand::Scan { span, reply } => {
                            let result = block_on_dispatch(snapshot.scan(&span))
                                .map_err(|error| super::encode_adapter_error(&error));
                            let _ = reply.send(result);
                        }
                        ReadViewCommand::CanonicalScan { request, reply } => {
                            let result = block_on_dispatch(snapshot.scan_canonical(&request))
                                .map_err(|error| super::encode_adapter_error(&error));
                            let _ = reply.send(result);
                        }
                        ReadViewCommand::CandidateScan { request, reply } => {
                            let result = block_on_dispatch(snapshot.scan_candidates(&request))
                                .map_err(|error| super::encode_adapter_error(&error));
                            let _ = reply.send(result);
                        }
                    }
                }
            })
            .map_err(|error| {
                remote_error(
                    RemoteErrorCode::ServiceFaulted,
                    &format!("failed to start read-view worker: {error}"),
                )
            })?;
        let applied_log_index = startup.recv().map_err(|_| {
            remote_error(
                RemoteErrorCode::ServiceFaulted,
                "read-view worker stopped during startup",
            )
        })??;
        Ok((
            Self {
                commands: Some(commands),
                thread: Some(thread),
            },
            applied_log_index,
        ))
    }

    fn handle(&self) -> Result<ReadViewHandle, RemoteError> {
        Ok(ReadViewHandle {
            commands: self
                .commands
                .as_ref()
                .ok_or_else(worker_stopped_error)?
                .clone(),
        })
    }
}

impl ReadViewHandle {
    fn multi_get(&self, keys: Vec<LogicalKey>) -> Result<Vec<Option<Vec<u8>>>, RemoteError> {
        let (reply, response) = mpsc::sync_channel(1);
        self.send(ReadViewCommand::MultiGet { keys, reply })?;
        response.recv().map_err(|_| worker_stopped_error())?
    }

    fn scan(&self, span: KeySpan) -> Result<Vec<KeyValue>, RemoteError> {
        let (reply, response) = mpsc::sync_channel(1);
        self.send(ReadViewCommand::Scan { span, reply })?;
        response.recv().map_err(|_| worker_stopped_error())?
    }

    fn scan_canonical(
        &self,
        request: CanonicalScanRequest,
    ) -> Result<CanonicalScanPage, RemoteError> {
        let (reply, response) = mpsc::sync_channel(1);
        self.send(ReadViewCommand::CanonicalScan { request, reply })?;
        response.recv().map_err(|_| worker_stopped_error())?
    }

    fn scan_candidates(
        &self,
        request: CandidateScanRequest,
    ) -> Result<CandidateScanPage, RemoteError> {
        let (reply, response) = mpsc::sync_channel(1);
        self.send(ReadViewCommand::CandidateScan { request, reply })?;
        response.recv().map_err(|_| worker_stopped_error())?
    }

    fn send(&self, command: ReadViewCommand) -> Result<(), RemoteError> {
        match self.commands.try_send(command) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => Err(remote_error(
                RemoteErrorCode::SessionBusy,
                "read-view session command queue is full",
            )),
            Err(TrySendError::Disconnected(_)) => Err(worker_stopped_error()),
        }
    }
}

impl Drop for ReadViewWorker {
    fn drop(&mut self) {
        self.commands.take();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
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
        let mut features = FeatureSet::BASE_ADAPTER_V1
            .union(FeatureSet::READ_VIEW_SESSION_V1)
            .union(FeatureSet::CANONICAL_SCAN_READ_VIEW_V1);
        if active.capabilities().predicate_pushdown
            && active.query_primitive_capabilities().candidate_scan()
                != PushdownGuarantee::Unsupported
        {
            features = features.union(FeatureSet::CANDIDATE_SCAN_READ_VIEW_V1);
        }
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

    fn reserve_session(&self) -> Result<u128, RemoteError> {
        let now = Instant::now();
        let mut sessions = self.sessions.lock().map_err(|_| {
            remote_error(RemoteErrorCode::ServiceFaulted, "session lock is poisoned")
        })?;
        let expired_ids = sessions
            .iter()
            .filter_map(|(session_id, session)| {
                (now.saturating_duration_since(session.last_activity())
                    >= SNAPSHOT_SESSION_IDLE_TIMEOUT)
                    .then_some(*session_id)
            })
            .collect::<Vec<_>>();
        let expired = expired_ids
            .into_iter()
            .filter_map(|session_id| sessions.remove(&session_id))
            .collect::<Vec<_>>();
        if sessions.len() >= MAX_ACTIVE_SNAPSHOT_SESSIONS {
            drop(sessions);
            drop(expired);
            return Err(remote_error(
                RemoteErrorCode::ResourceExhausted,
                "active snapshot-session capacity is exhausted",
            ));
        }
        let session_id = self.session_id();
        sessions.insert(session_id, SnapshotSession::Reserved { last_activity: now });
        drop(sessions);
        drop(expired);
        Ok(session_id)
    }

    fn discard_session(&self, session_id: u128) {
        let removed = self
            .sessions
            .lock()
            .ok()
            .and_then(|mut sessions| sessions.remove(&session_id));
        drop(removed);
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
            Request::BeginReadView => self.begin_read_view().await,
            Request::ReadViewMultiGet { session_id, keys } => {
                self.read_view_multi_get(session_id, keys)
            }
            Request::ReadViewScan { session_id, span } => self.read_view_scan(session_id, span),
            Request::ReadViewCanonicalScan {
                session_id,
                request,
            } => self.read_view_canonical_scan(session_id, request),
            Request::ReadViewCandidateScan {
                session_id,
                request,
            } => self.read_view_candidate_scan(session_id, request),
            Request::EndReadView { session_id } => self.end_read_view(session_id),
            request => Ok(super::dispatch_request(self.active_adapter()?.as_ref(), request).await),
        }
    }

    async fn begin_read_view(&self) -> Result<Response, RemoteError> {
        let session_id = self.reserve_session()?;
        let started = ReadViewWorker::spawn(self.active_adapter()?);
        let (worker, applied_log_index) = match started {
            Ok(started) => started,
            Err(error) => {
                self.discard_session(session_id);
                return Err(error);
            }
        };
        let mut sessions = self.sessions.lock().map_err(|_| {
            remote_error(RemoteErrorCode::ServiceFaulted, "session lock is poisoned")
        })?;
        if !matches!(
            sessions.get(&session_id),
            Some(SnapshotSession::Reserved { .. })
        ) {
            drop(sessions);
            drop(worker);
            return Err(remote_error(
                RemoteErrorCode::SessionExpired,
                "read-view reservation expired before backend startup completed",
            ));
        }
        sessions.insert(
            session_id,
            SnapshotSession::ReadView {
                last_activity: Instant::now(),
                worker,
            },
        );
        drop(sessions);
        Ok(Response::ReadViewStarted {
            session_id,
            applied_log_index,
        })
    }

    fn read_view_multi_get(
        &self,
        session_id: u128,
        keys: Vec<LogicalKey>,
    ) -> Result<Response, RemoteError> {
        let worker = self.read_view_handle(session_id)?;
        let values = worker.multi_get(keys)?;
        Ok(Response::ReadViewMultiGet { session_id, values })
    }

    fn read_view_scan(&self, session_id: u128, span: KeySpan) -> Result<Response, RemoteError> {
        let worker = self.read_view_handle(session_id)?;
        let values = worker.scan(span)?;
        Ok(Response::ReadViewScan { session_id, values })
    }

    fn read_view_canonical_scan(
        &self,
        session_id: u128,
        request: CanonicalScanRequest,
    ) -> Result<Response, RemoteError> {
        let worker = self.read_view_handle(session_id)?;
        let page = worker.scan_canonical(request)?;
        Ok(Response::ReadViewCanonicalScan {
            session_id,
            applied_log_index: page.applied_log_index(),
            entries: page.entries().to_vec(),
            next_start: page.next_start().cloned(),
        })
    }

    fn read_view_candidate_scan(
        &self,
        session_id: u128,
        request: CandidateScanRequest,
    ) -> Result<Response, RemoteError> {
        let worker = self.read_view_handle(session_id)?;
        let page = worker.scan_candidates(request)?;
        Ok(Response::ReadViewCandidateScan {
            session_id,
            applied_log_index: page.applied_log_index(),
            guarantee: page.guarantee(),
            entries: page.entries().to_vec(),
            next_start: page.next_start().cloned(),
        })
    }

    fn read_view_handle(&self, session_id: u128) -> Result<ReadViewHandle, RemoteError> {
        let mut sessions = self.sessions.lock().map_err(|_| {
            remote_error(RemoteErrorCode::ServiceFaulted, "session lock is poisoned")
        })?;
        let now = Instant::now();
        if sessions.get(&session_id).is_some_and(|session| {
            now.saturating_duration_since(session.last_activity()) >= SNAPSHOT_SESSION_IDLE_TIMEOUT
        }) {
            let expired = sessions.remove(&session_id);
            drop(sessions);
            drop(expired);
            return Err(remote_error(
                RemoteErrorCode::SessionExpired,
                "read-view session expired",
            ));
        }
        let session = sessions.get_mut(&session_id).ok_or_else(|| {
            remote_error(RemoteErrorCode::SessionUnknown, "unknown read-view session")
        })?;
        session.touch(now);
        let SnapshotSession::ReadView { worker, .. } = session else {
            return Err(remote_error(
                RemoteErrorCode::SessionKindMismatch,
                "session is not a read view",
            ));
        };
        worker.handle()
    }

    fn end_read_view(&self, session_id: u128) -> Result<Response, RemoteError> {
        let session = {
            let mut sessions = self.sessions.lock().map_err(|_| {
                remote_error(RemoteErrorCode::ServiceFaulted, "session lock is poisoned")
            })?;
            let session = sessions.get(&session_id).ok_or_else(|| {
                remote_error(RemoteErrorCode::SessionUnknown, "unknown read-view session")
            })?;
            if !matches!(session, SnapshotSession::ReadView { .. }) {
                return Err(remote_error(
                    RemoteErrorCode::SessionKindMismatch,
                    "session is not a read view",
                ));
            }
            sessions
                .remove(&session_id)
                .expect("checked session exists")
        };
        drop(session);
        Ok(Response::ReadViewEnded { session_id })
    }

    async fn begin_export(
        &self,
        request: super::BeginExportRequest,
    ) -> Result<Response, RemoteError> {
        let session_id = self.reserve_session()?;
        let prepared = async {
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
            Ok((header, chunks, manifest))
        }
        .await;
        let (header, chunks, manifest) = match prepared {
            Ok(prepared) => prepared,
            Err(error) => {
                self.discard_session(session_id);
                return Err(error);
            }
        };
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
                    last_activity: Instant::now(),
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
        let now = Instant::now();
        if sessions.get(&session_id).is_some_and(|session| {
            now.saturating_duration_since(session.last_activity()) >= SNAPSHOT_SESSION_IDLE_TIMEOUT
        }) {
            sessions.remove(&session_id);
            return Err(remote_error(
                RemoteErrorCode::SessionExpired,
                "export session expired",
            ));
        }
        let session = sessions.get_mut(&session_id).ok_or_else(|| {
            remote_error(RemoteErrorCode::SessionUnknown, "unknown export session")
        })?;
        session.touch(now);
        let SnapshotSession::Export {
            started,
            chunks,
            manifest,
            ..
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
        let session_id = self.reserve_session()?;
        self.sessions
            .lock()
            .map_err(|_| remote_error(RemoteErrorCode::ServiceFaulted, "session lock is poisoned"))?
            .insert(
                session_id,
                SnapshotSession::Restore {
                    last_activity: Instant::now(),
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
        let now = Instant::now();
        if sessions.get(&session_id).is_some_and(|session| {
            now.saturating_duration_since(session.last_activity()) >= SNAPSHOT_SESSION_IDLE_TIMEOUT
        }) {
            sessions.remove(&session_id);
            return Err(remote_error(
                RemoteErrorCode::SessionExpired,
                "restore session expired",
            ));
        }
        let session = sessions.get_mut(&session_id).ok_or_else(|| {
            remote_error(RemoteErrorCode::SessionUnknown, "unknown restore session")
        })?;
        session.touch(now);
        let SnapshotSession::Restore {
            chunks,
            accumulator,
            last_chunk,
            ..
        } = session
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
        let (session, expired) = {
            let mut sessions = self.sessions.lock().map_err(|_| {
                remote_error(RemoteErrorCode::ServiceFaulted, "session lock is poisoned")
            })?;
            let expired = sessions.get(&session_id).is_some_and(|session| {
                Instant::now().saturating_duration_since(session.last_activity())
                    >= SNAPSHOT_SESSION_IDLE_TIMEOUT
            });
            let session = sessions.remove(&session_id).ok_or_else(|| {
                remote_error(RemoteErrorCode::SessionUnknown, "unknown restore session")
            })?;
            (session, expired)
        };
        if expired {
            return Err(remote_error(
                RemoteErrorCode::SessionExpired,
                "restore session expired",
            ));
        }
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
            .map_err(registry_remote_error)?;
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
        let session = self
            .sessions
            .lock()
            .map_err(|_| remote_error(RemoteErrorCode::ServiceFaulted, "session lock is poisoned"))?
            .remove(&session_id)
            .ok_or_else(|| remote_error(RemoteErrorCode::SessionUnknown, "unknown session"))?;
        if Instant::now().saturating_duration_since(session.last_activity())
            >= SNAPSHOT_SESSION_IDLE_TIMEOUT
        {
            return Err(remote_error(
                RemoteErrorCode::SessionExpired,
                "snapshot session expired",
            ));
        }
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
            run_stateful_tcp_sidecar_server(
                listener,
                config,
                service,
                thread_shutdown,
                thread_connections,
            )
        })
        .map_err(|error| super::SidecarServerError::Io(error.to_string()))?;
    Ok(TcpSidecarServerHandle {
        local_addr,
        shutdown,
        active_connections,
        server_thread: Some(server_thread),
    })
}

fn run_stateful_tcp_sidecar_server(
    listener: TcpListener,
    config: TcpSidecarServerConfig,
    service: Arc<SidecarService>,
    shutdown: Arc<AtomicBool>,
    active_connections: Arc<Mutex<BTreeMap<u64, TcpStream>>>,
) -> Result<(), super::SidecarServerError> {
    let (sender, receiver) = mpsc::sync_channel(config.pending_connections);
    let receiver = Arc::new(Mutex::new(receiver));
    let mut workers = Vec::with_capacity(config.worker_threads);
    for worker_id in 0..config.worker_threads {
        let worker_service = Arc::clone(&service);
        let worker_receiver = Arc::clone(&receiver);
        let worker_shutdown = Arc::clone(&shutdown);
        let worker_connections = Arc::clone(&active_connections);
        workers.push(
            std::thread::Builder::new()
                .name(format!("dtg-stateful-sidecar-worker-{worker_id}"))
                .spawn(move || {
                    stateful_sidecar_worker_loop(
                        worker_service,
                        worker_receiver,
                        worker_shutdown,
                        worker_connections,
                        config.accept_poll_interval,
                    )
                })
                .map_err(|error| super::SidecarServerError::Io(error.to_string()))?,
        );
    }

    let accept_result = loop {
        if shutdown.load(Ordering::Acquire) {
            break Ok(());
        }
        match listener.accept() {
            Ok((stream, _)) => {
                if let Err(error) = stream
                    .set_nonblocking(false)
                    .and_then(|()| stream.set_nodelay(true))
                {
                    break Err(super::SidecarServerError::Io(error.to_string()));
                }
                match sender.try_send(stream) {
                    Ok(()) => {}
                    Err(TrySendError::Full(_)) => {}
                    Err(TrySendError::Disconnected(_)) => break Ok(()),
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(config.accept_poll_interval);
            }
            Err(error) => break Err(super::SidecarServerError::Io(error.to_string())),
        }
    };
    shutdown.store(true, Ordering::Release);
    drop(sender);
    super::close_active_connections(&active_connections)?;
    for worker in workers {
        worker
            .join()
            .map_err(|_| super::SidecarServerError::WorkerPanicked)??;
    }
    accept_result
}

fn stateful_sidecar_worker_loop(
    service: Arc<SidecarService>,
    receiver: Arc<Mutex<mpsc::Receiver<TcpStream>>>,
    shutdown: Arc<AtomicBool>,
    active_connections: Arc<Mutex<BTreeMap<u64, TcpStream>>>,
    poll_interval: std::time::Duration,
) -> Result<(), super::SidecarServerError> {
    loop {
        if shutdown.load(Ordering::Acquire) {
            return Ok(());
        }
        let received = receiver
            .lock()
            .map_err(|_| super::SidecarServerError::StatePoisoned)?
            .recv_timeout(poll_interval);
        let stream = match received {
            Ok(stream) => stream,
            Err(RecvTimeoutError::Timeout) => continue,
            Err(RecvTimeoutError::Disconnected) => return Ok(()),
        };
        let connection_id = super::NEXT_SERVER_CONNECTION_ID.fetch_add(1, Ordering::Relaxed);
        let control = stream
            .try_clone()
            .map_err(|error| super::SidecarServerError::Io(error.to_string()))?;
        active_connections
            .lock()
            .map_err(|_| super::SidecarServerError::StatePoisoned)?
            .insert(connection_id, control);
        if shutdown.load(Ordering::Acquire) {
            let _ = stream.shutdown(Shutdown::Both);
        } else {
            let _ = serve_stateful_connection(stream, &service);
        }
        active_connections
            .lock()
            .map_err(|_| super::SidecarServerError::StatePoisoned)?
            .remove(&connection_id);
    }
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
                if let Some(target_name) = name.strip_prefix("target.") {
                    target = target.with_parameter(target_name, value);
                } else if !is_transport_parameter(name) && name != "target_provider" {
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
    let connect_timeout = timeout_parameter(request, "connect_timeout_ms", 3_000)?;
    let read_timeout = timeout_parameter(request, "read_timeout_ms", 10_000)?;
    let write_timeout = timeout_parameter(request, "write_timeout_ms", 10_000)?;
    let config = TcpSidecarConfig::new(endpoint)
        .with_pool_size(pool_size)
        .and_then(|config| config.with_timeouts(connect_timeout, read_timeout, write_timeout))
        .map_err(factory_error)?;
    TcpSidecarTransport::connect(config).map_err(factory_error)
}

fn timeout_parameter(
    request: &AdapterOpenRequest,
    name: &str,
    default_millis: u64,
) -> Result<Duration, AdapterFactoryError> {
    let millis = request
        .parameter(name)
        .map_or(Ok(default_millis), str::parse::<u64>)
        .map_err(|_| AdapterFactoryError::new(format!("invalid Sidecar {name}")))?;
    if millis == 0 {
        return Err(AdapterFactoryError::new(format!(
            "Sidecar {name} must be positive"
        )));
    }
    Ok(Duration::from_millis(millis))
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
        scan_limit: None,
        scan_required: None,
        scan_response_limit: None,
        scan_response_required: None,
    }
}

fn read_view_remote_error(error: AdapterError) -> RemoteError {
    if matches!(error, AdapterError::UnsupportedOperation { .. }) {
        remote_error(
            RemoteErrorCode::FeatureUnsupported,
            "active Adapter does not support query read snapshots",
        )
    } else {
        super::encode_adapter_error(&error)
    }
}

fn worker_stopped_error() -> RemoteError {
    remote_error(
        RemoteErrorCode::ServiceFaulted,
        "read-view worker stopped unexpectedly",
    )
}

fn registry_remote_error(error: RegistryError) -> RemoteError {
    let mapping_incompatible = matches!(
        &error,
        RegistryError::MappingIncompatible(_)
            | RegistryError::Target(AdapterError::Mapping(_))
            | RegistryError::Source(AdapterError::Mapping(_))
    );
    remote_error(
        if mapping_incompatible {
            RemoteErrorCode::MappingIncompatible
        } else {
            RemoteErrorCode::ServiceFaulted
        },
        &error.to_string(),
    )
}

fn remote_error(code: RemoteErrorCode, message: &str) -> RemoteError {
    RemoteError {
        code: code as u32,
        message: message.to_owned(),
        retryable: false,
        scan_limit: None,
        scan_required: None,
        scan_response_limit: None,
        scan_response_required: None,
    }
}

fn factory_error(error: impl ToString) -> AdapterFactoryError {
    AdapterFactoryError::new(error.to_string())
}
