use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::pin::Pin;
use std::sync::Arc;

use dtg_execution::analytics::{AnalyticsJobError, AnalyticsLedger};
use dtg_execution::cluster_protocol::proto::{
    self, BoundedPayload, RetryDisposition, StatusCode, TypedStatus,
    meta_service_server::{MetaService, MetaServiceServer},
};
use dtg_execution::cluster_protocol::{
    PROTOCOL_MAJOR, checksum_bytes, validate_control_observation, validate_transaction_request,
};
use dtg_execution::control::{
    ActionCommand, ActionId, ActionRecord, BackendClass, BackendGeneration, BindingRole,
    CatalogCommand, CatalogState, ControlActionLedger, GraphId, PlacementEpoch, ProviderKind,
    ReplicaBinding, ReplicaBindingRecord, RetentionPin, ShardId, ShardPlacement, Version,
};
use dtg_execution::storage::{
    CommandId, ConsensusCommandEnvelope, ConsensusEntry, ConsensusStore, DurabilityPolicy,
    RaftMembership, ReplicaId, StorageError,
};
use dtg_execution::transaction::{
    CommitResolution, DurableTimestampAuthority, TimestampAuthority, TimestampCommandLog,
    TimestampLogFuture, TimestampOperation, TransactionId, TransactionTime, TxnError,
};
use dtg_execution::{
    ExecutionBuildError, MetaExecution, MetaRaftError, MetaRaftHost, MetaRaftRole,
};
use dtg_storage_fjall::FjallConsensusStore;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, mpsc, oneshot, watch};
use tokio::time::{Duration, Instant, timeout_at};
use tokio_stream::{Stream, wrappers::ReceiverStream};
use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};
use tonic::{Request, Response, Status};

use crate::{MetaConfig, TransportSecurity};

const CONSENSUS_FORMAT_VERSION: u32 = 1;
const TIMESTAMP_LOG_TERM: u64 = 1;
const CATALOG_GRAPH_ID: u64 = u64::MAX - 1;
const TIMESTAMP_GRAPH_ID: u64 = u64::MAX - 2;
const TIMESTAMP_BATCH_MAX_OPERATIONS: usize = 64;
const TIMESTAMP_BATCH_WINDOW: Duration = Duration::from_micros(250);

pub struct MetaProcess {
    config: MetaConfig,
    core: Arc<MetaCore>,
    timestamps: Arc<DurableTimestampAuthority>,
    timestamp_batcher: Arc<TimestampRpcBatcher>,
}

struct MetaCore {
    execution: Mutex<MetaExecution>,
    raft: Mutex<MetaRaftHost>,
    catalog_commands: Mutex<Vec<CatalogCommand>>,
    catalog_revision: watch::Sender<Version>,
}

impl MetaProcess {
    pub async fn open(config: MetaConfig) -> Result<Self, MetaProcessError> {
        std::fs::create_dir_all(config.data_directory())?;
        let catalog_store = Arc::new(FjallConsensusStore::open(
            config.catalog_consensus_path(),
            consensus_binding(&config, CATALOG_GRAPH_ID, "meta-catalog")?,
        )?);
        let timestamp_store = Arc::new(FjallConsensusStore::open(
            config.timestamp_consensus_path(),
            consensus_binding(&config, TIMESTAMP_GRAPH_ID, "meta-timestamps")?,
        )?);
        configure_meta_membership(&config, catalog_store.as_ref()).await?;
        let committed_index = catalog_store.hard_state().await?.committed_index;
        let mut raft = MetaRaftHost::open(
            catalog_store,
            ReplicaId::new(config.node_id())
                .map_err(|error| MetaProcessError::Consensus(error.to_string()))?,
            committed_index,
        )
        .await?;
        let timestamp_log = Arc::new(FjallTimestampLog::open(timestamp_store).await?);
        let timestamps = Arc::new(DurableTimestampAuthority::open(timestamp_log).await?);
        let timestamp_batcher = Arc::new(TimestampRpcBatcher::new(
            timestamps.clone(),
            TIMESTAMP_BATCH_MAX_OPERATIONS,
            TIMESTAMP_BATCH_WINDOW,
        ));

        let mut commands = Vec::new();
        let mut actions = ControlActionLedger::new();
        let mut catalog = CatalogState::new();
        for entry in raft.recovery_entries() {
            match decode_authority_command(entry.payload())? {
                AuthorityCommand::Catalog(command) => {
                    catalog = catalog.apply(command.clone())?;
                    commands.push(command);
                }
                AuthorityCommand::Action(command) => {
                    actions.apply(command)?;
                }
            }
        }
        if config.peers().len() == 1 && raft.role() != MetaRaftRole::Leader {
            raft.campaign()?;
            let progress = raft.drive_ready().await?;
            if !progress.committed.is_empty() {
                return Err(MetaProcessError::Consensus(
                    "single-node Meta election committed an unexpected command".into(),
                ));
            }
        }
        let catalog_version = catalog.version();
        let execution = MetaExecution::builder()
            .with_catalog(catalog)
            .with_timestamps(timestamps.clone())
            .with_analytics_ledger(AnalyticsLedger::new(config.analytics_lease_duration())?)
            .with_action_ledger(actions)
            .build()?;
        let (catalog_revision, _) = watch::channel(catalog_version);
        Ok(Self {
            config,
            core: Arc::new(MetaCore {
                execution: Mutex::new(execution),
                raft: Mutex::new(raft),
                catalog_commands: Mutex::new(commands),
                catalog_revision,
            }),
            timestamps,
            timestamp_batcher,
        })
    }

    pub async fn propose(&self, command: CatalogCommand) -> Result<Version, MetaProcessError> {
        let payload = encode_authority_command(&AuthorityCommand::Catalog(command.clone()))?;
        let proposal_id = catalog_proposal_id(&payload);
        let mut raft = self.core.raft.lock().await;
        raft.propose(proposal_id, payload)?;
        let progress = raft.drive_ready().await?;
        drop(raft);
        let mut result = None;
        for committed in progress.committed {
            match decode_authority_command(committed.payload())? {
                AuthorityCommand::Catalog(committed_command) => {
                    let mut execution = self.core.execution.lock().await;
                    let version = execution.apply_catalog_command(committed_command.clone())?;
                    drop(execution);
                    self.core
                        .catalog_commands
                        .lock()
                        .await
                        .push(committed_command);
                    self.core.catalog_revision.send_replace(version);
                    if committed.proposal_id() == proposal_id {
                        result = Some(version);
                    }
                }
                AuthorityCommand::Action(command) => {
                    self.core
                        .execution
                        .lock()
                        .await
                        .apply_action_command(command)?;
                }
            }
        }
        result.ok_or(MetaProcessError::ProposalPending)
    }

    pub async fn apply_action_command(
        &self,
        command: ActionCommand,
    ) -> Result<ActionRecord, MetaProcessError> {
        let payload = encode_authority_command(&AuthorityCommand::Action(command))?;
        let proposal_id = catalog_proposal_id(&payload);
        let mut raft = self.core.raft.lock().await;
        raft.propose(proposal_id, payload)?;
        let progress = raft.drive_ready().await?;
        drop(raft);
        let mut result = None;
        for committed in progress.committed {
            match decode_authority_command(committed.payload())? {
                AuthorityCommand::Catalog(command) => {
                    let version = self
                        .core
                        .execution
                        .lock()
                        .await
                        .apply_catalog_command(command.clone())?;
                    self.core.catalog_commands.lock().await.push(command);
                    self.core.catalog_revision.send_replace(version);
                }
                AuthorityCommand::Action(command) => {
                    let record = self
                        .core
                        .execution
                        .lock()
                        .await
                        .apply_action_command(command)?;
                    if committed.proposal_id() == proposal_id {
                        result = Some(record);
                    }
                }
            }
        }
        result.ok_or(MetaProcessError::ProposalPending)
    }

    pub async fn action_record(&self, action_id: ActionId) -> Option<ActionRecord> {
        self.core
            .execution
            .lock()
            .await
            .action_record(action_id)
            .cloned()
    }

    pub async fn catalog_version(&self) -> Version {
        self.core.execution.lock().await.catalog_version()
    }

    pub async fn leader_id(&self) -> Option<ReplicaId> {
        self.core.raft.lock().await.leader_id()
    }

    pub fn timestamps(&self) -> Arc<dyn TimestampAuthority> {
        self.timestamps.clone()
    }

    pub fn rpc_service(&self) -> MetaRpcService {
        MetaRpcService {
            timestamps: self.timestamps.clone(),
            timestamp_batcher: self.timestamp_batcher.clone(),
            cluster_id: self.config.cluster_id(),
            core: self.core.clone(),
        }
    }

    pub async fn serve(self) -> Result<(), MetaProcessError> {
        let mut server = Server::builder();
        if let TransportSecurity::MutualTls(files) = self.config.security() {
            let certificate = tokio::fs::read(files.certificate()).await?;
            let private_key = tokio::fs::read(files.private_key()).await?;
            let client_ca = tokio::fs::read(files.client_ca()).await?;
            server = server.tls_config(
                ServerTlsConfig::new()
                    .identity(Identity::from_pem(certificate, private_key))
                    .client_ca_root(Certificate::from_pem(client_ca)),
            )?;
        }
        let address = self.config.listen_addr();
        tracing::info!(%address, node_id = self.config.node_id(), "Meta process serving cluster protocol v2");
        server
            .add_service(MetaServiceServer::new(self.rpc_service()))
            .serve_with_shutdown(address, shutdown_signal())
            .await?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct MetaRpcService {
    timestamps: Arc<DurableTimestampAuthority>,
    timestamp_batcher: Arc<TimestampRpcBatcher>,
    cluster_id: u64,
    core: Arc<MetaCore>,
}

impl MetaRpcService {
    pub const fn protocol_major(&self) -> u32 {
        PROTOCOL_MAJOR
    }
}

struct TimestampRpcRequest {
    operations: Vec<TimestampOperation>,
    response: oneshot::Sender<Vec<Result<TransactionTime, TxnError>>>,
}

struct TimestampRpcBatcher {
    sender: mpsc::Sender<TimestampRpcRequest>,
}

impl TimestampRpcBatcher {
    fn new(
        timestamps: Arc<DurableTimestampAuthority>,
        max_operations: usize,
        window: Duration,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(max_operations);
        tokio::spawn(run_timestamp_batcher(
            timestamps,
            receiver,
            max_operations,
            window,
        ));
        Self { sender }
    }

    async fn submit(&self, operation: TimestampOperation) -> Result<TransactionTime, TxnError> {
        self.submit_operations(vec![operation])
            .await
            .into_iter()
            .next()
            .expect("one timestamp operation produces one result")
    }

    async fn submit_pair(
        &self,
        first: TimestampOperation,
        second: TimestampOperation,
    ) -> Result<(TransactionTime, TransactionTime), TxnError> {
        let mut results = self
            .submit_operations(vec![first, second])
            .await
            .into_iter();
        let first = results
            .next()
            .expect("timestamp pair produces a first result")?;
        let second = results
            .next()
            .expect("timestamp pair produces a second result")?;
        Ok((first, second))
    }

    async fn submit_operations(
        &self,
        operations: Vec<TimestampOperation>,
    ) -> Vec<Result<TransactionTime, TxnError>> {
        let (response, receiver) = oneshot::channel();
        if self
            .sender
            .send(TimestampRpcRequest {
                operations,
                response,
            })
            .await
            .is_err()
        {
            return vec![Err(TxnError::Storage("timestamp batcher stopped".into()))];
        }
        receiver
            .await
            .unwrap_or_else(|_| vec![Err(TxnError::Storage("timestamp batcher stopped".into()))])
    }
}

async fn run_timestamp_batcher(
    timestamps: Arc<DurableTimestampAuthority>,
    mut receiver: mpsc::Receiver<TimestampRpcRequest>,
    max_operations: usize,
    window: Duration,
) {
    while let Some(first) = receiver.recv().await {
        let mut requests = vec![first];
        let deadline = Instant::now() + window;
        while requests.len() < max_operations {
            match timeout_at(deadline, receiver.recv()).await {
                Ok(Some(request)) => requests.push(request),
                Ok(None) | Err(_) => break,
            }
        }
        let operation_counts = requests
            .iter()
            .map(|request| request.operations.len())
            .collect::<Vec<_>>();
        let mut results = timestamps
            .apply_batch(
                requests
                    .iter()
                    .flat_map(|request| request.operations.iter().copied())
                    .collect(),
            )
            .await
            .into_iter();
        for (request, count) in requests.into_iter().zip(operation_counts) {
            let response = (0..count)
                .map(|_| {
                    results.next().unwrap_or_else(|| {
                        Err(TxnError::Storage(
                            "timestamp batcher lost an operation result".into(),
                        ))
                    })
                })
                .collect();
            let _ = request.response.send(response);
        }
    }
}

#[tonic::async_trait]
impl MetaService for MetaRpcService {
    type WatchCatalogStream =
        Pin<Box<dyn Stream<Item = Result<proto::CatalogSnapshot, Status>> + Send + 'static>>;

    async fn submit_transaction(
        &self,
        request: Request<proto::TransactionRequest>,
    ) -> Result<Response<TypedStatus>, Status> {
        let wire = request.into_inner();
        let context = response_context_from_shard(wire.context.as_ref(), self.cluster_id);
        if let Err(error) = self.core.raft.lock().await.leader_read_index() {
            return Ok(Response::new(status(
                context,
                StatusCode::Unavailable,
                RetryDisposition::Safe,
                &error.to_string(),
                Vec::new(),
            )));
        }
        if let Err(error) = validate_transaction_request(wire.clone()) {
            return Ok(Response::new(status(
                context,
                StatusCode::InvalidRequest,
                RetryDisposition::Never,
                error.code(),
                Vec::new(),
            )));
        }
        let transaction_id = match exact_transaction_id(&wire.transaction_id) {
            Ok(transaction_id) => transaction_id,
            Err(error) => {
                return Ok(Response::new(status(
                    context,
                    StatusCode::InvalidRequest,
                    RetryDisposition::Never,
                    error.code(),
                    Vec::new(),
                )));
            }
        };
        let result = match wire.operation {
            1 => self
                .timestamp_batcher
                .submit(TimestampOperation::AllocateStart { transaction_id })
                .await
                .map(timestamp_response),
            2 => self
                .timestamp_batcher
                .submit(TimestampOperation::ReserveCommit { transaction_id })
                .await
                .map(timestamp_response),
            3 => {
                let reservation = self
                    .timestamps
                    .commit_time_reservation(transaction_id)
                    .await;
                match reservation {
                    Ok(Some(reservation)) => self
                        .timestamp_batcher
                        .submit(TimestampOperation::ResolveCommit {
                            transaction_id,
                            commit_time: reservation.commit_time(),
                            resolution: CommitResolution::Aborted,
                        })
                        .await
                        .map(timestamp_response),
                    Ok(None) => Err(TxnError::CorruptRecovery),
                    Err(error) => Err(error),
                }
            }
            4 => self
                .timestamps
                .commit_time_reservation(transaction_id)
                .await
                .and_then(|reservation| {
                    reservation
                        .map(|reservation| reservation.commit_time())
                        .ok_or(TxnError::CorruptRecovery)
                })
                .map(timestamp_response),
            5 => {
                let reservation = self
                    .timestamps
                    .commit_time_reservation(transaction_id)
                    .await;
                match reservation {
                    Ok(Some(reservation)) => self
                        .timestamp_batcher
                        .submit(TimestampOperation::ResolveCommit {
                            transaction_id,
                            commit_time: reservation.commit_time(),
                            resolution: CommitResolution::Committed,
                        })
                        .await
                        .map(timestamp_response),
                    Ok(None) => Err(TxnError::CorruptRecovery),
                    Err(error) => Err(error),
                }
            }
            6 => self
                .timestamp_batcher
                .submit_pair(
                    TimestampOperation::AllocateStart { transaction_id },
                    TimestampOperation::ReserveCommit { transaction_id },
                )
                .await
                .map(|(start, commit)| {
                    let mut body = timestamp_response(start);
                    body.extend_from_slice(&commit.get().to_be_bytes());
                    body
                }),
            _ => unreachable!("validated transaction operation"),
        };
        Ok(Response::new(match result {
            Ok(body) => status(context, StatusCode::Ok, RetryDisposition::Never, "ok", body),
            Err(error) => status(
                context,
                StatusCode::Conflict,
                RetryDisposition::Safe,
                error.code(),
                Vec::new(),
            ),
        }))
    }

    async fn report_observation(
        &self,
        request: Request<proto::ControlObservation>,
    ) -> Result<Response<TypedStatus>, Status> {
        let wire = request.into_inner();
        let context = response_context(wire.request.as_ref(), self.cluster_id);
        Ok(Response::new(match validate_control_observation(wire) {
            Ok(_) => status(
                context,
                StatusCode::Unavailable,
                RetryDisposition::Safe,
                "observations are served by ControllerService",
                Vec::new(),
            ),
            Err(error) => status(
                context,
                StatusCode::InvalidRequest,
                RetryDisposition::Never,
                error.code(),
                Vec::new(),
            ),
        }))
    }

    async fn watch_catalog(
        &self,
        request: Request<proto::CatalogWatchRequest>,
    ) -> Result<Response<Self::WatchCatalogStream>, Status> {
        let request = request.into_inner();
        let context = request
            .request
            .clone()
            .ok_or_else(|| Status::invalid_argument("DTG-PROTOCOL-MISSING-CONTEXT"))?;
        dtg_execution::cluster_protocol::RequestContext::try_from(context.clone())
            .map_err(|error| Status::invalid_argument(error.code()))?;
        self.core
            .raft
            .lock()
            .await
            .leader_read_index()
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
        let mut revisions = self.core.catalog_revision.subscribe();
        let core = self.core.clone();
        let (sender, receiver) = tokio::sync::mpsc::channel(8);
        tokio::spawn(async move {
            loop {
                let revision = *revisions.borrow_and_update();
                if revision.get() >= request.after_revision {
                    let snapshot = core.catalog_snapshot(context.clone(), revision).await;
                    if sender.send(snapshot.map_err(Status::from)).await.is_err() {
                        break;
                    }
                }
                if revisions.changed().await.is_err() {
                    break;
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(receiver))))
    }
}

fn timestamp_response(timestamp: TransactionTime) -> Vec<u8> {
    timestamp.get().to_be_bytes().to_vec()
}

impl MetaCore {
    async fn catalog_snapshot(
        &self,
        request: proto::RequestContext,
        revision: Version,
    ) -> Result<proto::CatalogSnapshot, MetaProcessError> {
        let commands = self.catalog_commands.lock().await;
        if commands.len() as u64 != revision.get() {
            return Err(MetaProcessError::ConsensusIndex);
        }
        let wire = commands
            .iter()
            .map(CatalogCommandWire::from_command)
            .collect::<Vec<_>>();
        let body = serde_json::to_vec(&wire)
            .map_err(|error| MetaProcessError::Codec(error.to_string()))?;
        Ok(proto::CatalogSnapshot {
            request: Some(request),
            revision: revision.get(),
            payload: Some(BoundedPayload {
                format_version: 1,
                declared_len: body.len() as u64,
                item_count: wire.len() as u32,
                checksum: checksum_bytes(&body).to_vec(),
                body,
            }),
        })
    }
}

struct FjallTimestampLog {
    store: Arc<FjallConsensusStore>,
    next_index: Mutex<u64>,
}

impl FjallTimestampLog {
    async fn open(store: Arc<FjallConsensusStore>) -> Result<Self, MetaProcessError> {
        let entries = store.entries(1, u64::MAX, u64::MAX).await?;
        let next_index = entries
            .last()
            .map_or(1, |entry| entry.index().saturating_add(1));
        Ok(Self {
            store,
            next_index: Mutex::new(next_index),
        })
    }
}

impl TimestampCommandLog for FjallTimestampLog {
    fn replay(&self) -> TimestampLogFuture<'_, Vec<Vec<u8>>> {
        Box::pin(async move {
            self.store
                .entries(1, u64::MAX, u64::MAX)
                .await
                .map(|entries| {
                    entries
                        .into_iter()
                        .map(|entry| entry.command().payload().to_vec())
                        .collect()
                })
                .map_err(TxnError::from)
        })
    }

    fn append_batch(&self, commands: Vec<Vec<u8>>) -> TimestampLogFuture<'_, ()> {
        Box::pin(async move {
            if commands.is_empty() {
                return Ok(());
            }
            let mut next_index = self.next_index.lock().await;
            let first_index = *next_index;
            let entries = commands
                .into_iter()
                .enumerate()
                .map(|(offset, command)| {
                    let index = first_index
                        .checked_add(offset as u64)
                        .ok_or(TxnError::ResourceLimit)?;
                    timestamp_consensus_entry(index, command)
                        .map_err(|error| TxnError::Storage(error.to_string()))
                })
                .collect::<Result<Vec<_>, _>>()?;
            let next = first_index
                .checked_add(entries.len() as u64)
                .ok_or(TxnError::ResourceLimit)?;
            self.store.append(entries).await.map_err(TxnError::from)?;
            *next_index = next;
            Ok(())
        })
    }
}

#[cfg(test)]
mod timestamp_batcher_tests {
    use std::sync::{Arc, Mutex as StdMutex};
    use std::time::Duration;

    use dtg_execution::transaction::{
        DurableTimestampAuthority, TimestampCommandLog, TimestampLogFuture, TimestampOperation,
        TransactionId,
    };

    use super::TimestampRpcBatcher;

    #[derive(Default)]
    struct MemoryTimestampLog {
        batches: StdMutex<usize>,
    }

    impl TimestampCommandLog for MemoryTimestampLog {
        fn replay(&self) -> TimestampLogFuture<'_, Vec<Vec<u8>>> {
            Box::pin(async { Ok(Vec::new()) })
        }

        fn append_batch(&self, commands: Vec<Vec<u8>>) -> TimestampLogFuture<'_, ()> {
            Box::pin(async move {
                assert!(!commands.is_empty());
                *self.batches.lock().unwrap() += 1;
                Ok(())
            })
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn timestamp_rpc_batcher_persists_concurrent_allocations_once() {
        let log = Arc::new(MemoryTimestampLog::default());
        let authority = Arc::new(DurableTimestampAuthority::open(log.clone()).await.unwrap());
        let batcher = Arc::new(TimestampRpcBatcher::new(
            authority,
            64,
            Duration::from_millis(20),
        ));
        let barrier = Arc::new(tokio::sync::Barrier::new(9));
        let mut tasks = tokio::task::JoinSet::new();
        for id in 1..=8 {
            let batcher = batcher.clone();
            let barrier = barrier.clone();
            tasks.spawn(async move {
                barrier.wait().await;
                batcher
                    .submit(TimestampOperation::AllocateStart {
                        transaction_id: TransactionId::new(id).unwrap(),
                    })
                    .await
            });
        }
        barrier.wait().await;
        while let Some(result) = tasks.join_next().await {
            assert!(result.unwrap().is_ok());
        }
        assert_eq!(*log.batches.lock().unwrap(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn timestamp_rpc_batcher_prepares_start_and_commit_in_one_atomic_batch() {
        let log = Arc::new(MemoryTimestampLog::default());
        let authority = Arc::new(DurableTimestampAuthority::open(log.clone()).await.unwrap());
        let batcher = TimestampRpcBatcher::new(authority, 64, Duration::from_millis(20));

        let (start, commit) = batcher
            .submit_pair(
                TimestampOperation::AllocateStart {
                    transaction_id: TransactionId::new(101).unwrap(),
                },
                TimestampOperation::ReserveCommit {
                    transaction_id: TransactionId::new(101).unwrap(),
                },
            )
            .await
            .unwrap();

        assert!(commit > start);
        assert_eq!(*log.batches.lock().unwrap(), 1);
    }
}

fn timestamp_consensus_entry(index: u64, payload: Vec<u8>) -> Result<ConsensusEntry, StorageError> {
    ConsensusEntry::new(
        CONSENSUS_FORMAT_VERSION,
        TIMESTAMP_LOG_TERM,
        index,
        CommandId::new(u128::from(index))?,
        ConsensusCommandEnvelope::new(CONSENSUS_FORMAT_VERSION, payload)?,
    )
}

async fn configure_meta_membership(
    config: &MetaConfig,
    store: &dyn ConsensusStore,
) -> Result<(), MetaProcessError> {
    let voters = config
        .peers()
        .iter()
        .map(|peer| {
            ReplicaId::new(peer.node_id())
                .map_err(|error| MetaProcessError::Consensus(error.to_string()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    match store.membership().await {
        Ok(existing)
            if existing.voters == voters
                && existing.learners.is_empty()
                && existing.configuration_index == 1 => {}
        Ok(_) => {
            return Err(MetaProcessError::Consensus(
                "configured Meta peers do not match durable membership".into(),
            ));
        }
        Err(StorageError::NotFound) => {
            store
                .set_membership(RaftMembership {
                    voters,
                    learners: Vec::new(),
                    configuration_index: 1,
                })
                .await?;
        }
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

fn catalog_proposal_id(payload: &[u8]) -> u128 {
    let digest = checksum_bytes(payload);
    u128::from_be_bytes(digest[..16].try_into().expect("digest prefix length")).max(1)
}

fn consensus_binding(
    config: &MetaConfig,
    graph_id: u64,
    namespace: &str,
) -> Result<ReplicaBinding, StorageError> {
    let class = BackendClass::new(
        ProviderKind::Fjall,
        1,
        1,
        [
            "adjacency",
            "immutable-read-view",
            "logical-snapshot",
            "point",
        ],
    )?;
    ReplicaBinding::builder()
        .cluster_id(config.cluster_id())
        .graph_id(graph_id)
        .shard_id(1)
        .placement_epoch(1)
        .replica_id(config.node_id())
        .backend_generation(1)
        .backend_class_digest(class.digest())
        .provider_kind(ProviderKind::Fjall)
        .contract_version(1)
        .layout_version(1)
        .capability_digest(class.required_capabilities().digest())
        .namespace_id(if graph_id == CATALOG_GRAPH_ID {
            config.consensus_namespace()
        } else {
            namespace
        })
        .endpoint_profile_ref("process://local-fjall")
        .credential_ref("process://local-fjall")
        .role(BindingRole::Active)
        .build()
}

fn status(
    request: proto::RequestContext,
    code: StatusCode,
    retry: RetryDisposition,
    message: &str,
    details: Vec<u8>,
) -> TypedStatus {
    let payload = (!details.is_empty()).then(|| BoundedPayload {
        format_version: 1,
        declared_len: details.len() as u64,
        item_count: 1,
        checksum: checksum_bytes(&details).to_vec(),
        body: details,
    });
    TypedStatus {
        request: Some(request),
        code: code as i32,
        retry: retry as i32,
        message: message.to_owned(),
        idempotency_key: Vec::new(),
        details: payload,
    }
}

fn response_context_from_shard(
    context: Option<&proto::ShardContext>,
    cluster_id: u64,
) -> proto::RequestContext {
    response_context(
        context.and_then(|context| context.request.as_ref()),
        cluster_id,
    )
}

fn response_context(
    context: Option<&proto::RequestContext>,
    cluster_id: u64,
) -> proto::RequestContext {
    context.cloned().unwrap_or_else(|| proto::RequestContext {
        protocol_major: PROTOCOL_MAJOR,
        protocol_minor: 0,
        cluster_id: cluster_id.to_be_bytes().to_vec(),
        request_id: 1_u128.to_be_bytes().to_vec(),
        deadline_unix_ms: 1,
        trace_context: Vec::new(),
    })
}

fn exact_transaction_id(
    bytes: &[u8],
) -> Result<TransactionId, dtg_execution::cluster_protocol::ProtocolError> {
    let bytes: [u8; 16] = bytes
        .try_into()
        .map_err(|_| dtg_execution::cluster_protocol::ProtocolError::IdentifierLength)?;
    TransactionId::new(u128::from_be_bytes(bytes))
        .map_err(|_| dtg_execution::cluster_protocol::ProtocolError::IdentifierLength)
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum CatalogCommandWire {
    PutPlacement {
        expected_version: u64,
        placement: PlacementWire,
    },
    PinRetention {
        expected_version: u64,
        graph_id: u64,
        shard_id: u64,
        generation: u64,
        pin: String,
    },
    UnpinRetention {
        expected_version: u64,
        graph_id: u64,
        shard_id: u64,
        generation: u64,
        pin: String,
    },
}

#[derive(Serialize, Deserialize)]
struct PlacementWire {
    graph_id: u64,
    shard_id: u64,
    placement_epoch: u64,
    active_generation: u64,
    backend_class: BackendClassWire,
    replicas: Vec<ReplicaRecordWire>,
}

#[derive(Serialize, Deserialize)]
struct ReplicaRecordWire {
    binding: ReplicaBindingWire,
    backend_class: BackendClassWire,
}

#[derive(Serialize, Deserialize)]
struct BackendClassWire {
    provider: ProviderWire,
    contract_version: u32,
    layout_version: u32,
    durability: DurabilityWire,
    capabilities: Vec<String>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ProviderWire {
    Fjall,
    PostgreSql,
    Neo4j,
    Remote(String),
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum DurabilityWire {
    DurableCommit,
    DurableCommitWithReplicaSync,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum BindingRoleWire {
    Candidate,
    Active,
    Retiring,
}

#[derive(Serialize, Deserialize)]
struct ReplicaBindingWire {
    cluster_id: u64,
    graph_id: u64,
    shard_id: u64,
    placement_epoch: u64,
    replica_id: u64,
    backend_generation: u64,
    provider: ProviderWire,
    contract_version: u32,
    layout_version: u32,
    namespace_id: String,
    endpoint_profile_ref: String,
    credential_ref: String,
    role: BindingRoleWire,
}

fn encode_catalog_command(command: &CatalogCommand) -> Result<Vec<u8>, MetaProcessError> {
    let wire = CatalogCommandWire::from_command(command);
    serde_json::to_vec(&wire).map_err(|error| MetaProcessError::Codec(error.to_string()))
}

enum AuthorityCommand {
    Catalog(CatalogCommand),
    Action(ActionCommand),
}

fn encode_authority_command(command: &AuthorityCommand) -> Result<Vec<u8>, MetaProcessError> {
    let mut bytes = Vec::new();
    match command {
        AuthorityCommand::Catalog(command) => {
            bytes.push(1);
            bytes.extend_from_slice(&encode_catalog_command(command)?);
        }
        AuthorityCommand::Action(command) => {
            bytes.push(2);
            bytes.extend_from_slice(&command.encode_current()?);
        }
    }
    Ok(bytes)
}

fn decode_authority_command(bytes: &[u8]) -> Result<AuthorityCommand, MetaProcessError> {
    let (tag, payload) = bytes
        .split_first()
        .ok_or_else(|| MetaProcessError::Codec("empty Meta authority command".into()))?;
    match tag {
        1 => Ok(AuthorityCommand::Catalog(decode_catalog_command(payload)?)),
        2 => Ok(AuthorityCommand::Action(ActionCommand::decode(payload)?)),
        _ => Err(MetaProcessError::Codec(
            "unknown Meta authority command tag".into(),
        )),
    }
}

impl CatalogCommandWire {
    fn from_command(command: &CatalogCommand) -> Self {
        match command {
            CatalogCommand::PutPlacement {
                expected_version,
                placement,
            } => Self::PutPlacement {
                expected_version: expected_version.get(),
                placement: PlacementWire::from(placement),
            },
            CatalogCommand::PinRetention {
                expected_version,
                graph_id,
                shard_id,
                generation,
                pin,
            } => Self::PinRetention {
                expected_version: expected_version.get(),
                graph_id: graph_id.get(),
                shard_id: shard_id.get(),
                generation: generation.get(),
                pin: pin.as_str().to_owned(),
            },
            CatalogCommand::UnpinRetention {
                expected_version,
                graph_id,
                shard_id,
                generation,
                pin,
            } => Self::UnpinRetention {
                expected_version: expected_version.get(),
                graph_id: graph_id.get(),
                shard_id: shard_id.get(),
                generation: generation.get(),
                pin: pin.as_str().to_owned(),
            },
        }
    }
}

impl From<MetaProcessError> for Status {
    fn from(error: MetaProcessError) -> Self {
        Self::internal(error.to_string())
    }
}

fn decode_catalog_command(bytes: &[u8]) -> Result<CatalogCommand, MetaProcessError> {
    let wire: CatalogCommandWire = serde_json::from_slice(bytes)
        .map_err(|error| MetaProcessError::Codec(error.to_string()))?;
    match wire {
        CatalogCommandWire::PutPlacement {
            expected_version,
            placement,
        } => Ok(CatalogCommand::put_placement(
            Version::new(expected_version),
            placement.try_into()?,
        )),
        CatalogCommandWire::PinRetention {
            expected_version,
            graph_id,
            shard_id,
            generation,
            pin,
        } => Ok(CatalogCommand::pin_retention(
            Version::new(expected_version),
            GraphId::new(graph_id)
                .map_err(|_| MetaProcessError::Codec("invalid graph ID".into()))?,
            ShardId::new(shard_id)
                .map_err(|_| MetaProcessError::Codec("invalid shard ID".into()))?,
            BackendGeneration::new(generation)
                .map_err(|_| MetaProcessError::Codec("invalid generation".into()))?,
            RetentionPin::new(pin)?,
        )),
        CatalogCommandWire::UnpinRetention {
            expected_version,
            graph_id,
            shard_id,
            generation,
            pin,
        } => Ok(CatalogCommand::unpin_retention(
            Version::new(expected_version),
            GraphId::new(graph_id)
                .map_err(|_| MetaProcessError::Codec("invalid graph ID".into()))?,
            ShardId::new(shard_id)
                .map_err(|_| MetaProcessError::Codec("invalid shard ID".into()))?,
            BackendGeneration::new(generation)
                .map_err(|_| MetaProcessError::Codec("invalid generation".into()))?,
            RetentionPin::new(pin)?,
        )),
    }
}

impl From<&ShardPlacement> for PlacementWire {
    fn from(value: &ShardPlacement) -> Self {
        Self {
            graph_id: value.graph_id.get(),
            shard_id: value.shard_id.get(),
            placement_epoch: value.placement_epoch.get(),
            active_generation: value.active_generation.get(),
            backend_class: BackendClassWire::from(&value.backend_class),
            replicas: value.replicas.iter().map(ReplicaRecordWire::from).collect(),
        }
    }
}

impl TryFrom<PlacementWire> for ShardPlacement {
    type Error = MetaProcessError;

    fn try_from(value: PlacementWire) -> Result<Self, Self::Error> {
        Ok(Self {
            graph_id: GraphId::new(value.graph_id)
                .map_err(|_| MetaProcessError::Codec("invalid graph ID".into()))?,
            shard_id: ShardId::new(value.shard_id)
                .map_err(|_| MetaProcessError::Codec("invalid shard ID".into()))?,
            placement_epoch: PlacementEpoch::new(value.placement_epoch)
                .map_err(|_| MetaProcessError::Codec("invalid placement epoch".into()))?,
            active_generation: BackendGeneration::new(value.active_generation)
                .map_err(|_| MetaProcessError::Codec("invalid generation".into()))?,
            backend_class: value.backend_class.try_into()?,
            replicas: value
                .replicas
                .into_iter()
                .map(ReplicaBindingRecord::try_from)
                .collect::<Result<Vec<_>, _>>()?,
        })
    }
}

impl From<&ReplicaBindingRecord> for ReplicaRecordWire {
    fn from(value: &ReplicaBindingRecord) -> Self {
        Self {
            binding: ReplicaBindingWire::from(value.binding()),
            backend_class: BackendClassWire::from(value.backend_class()),
        }
    }
}

impl TryFrom<ReplicaRecordWire> for ReplicaBindingRecord {
    type Error = MetaProcessError;

    fn try_from(value: ReplicaRecordWire) -> Result<Self, Self::Error> {
        let backend_class: BackendClass = value.backend_class.try_into()?;
        let binding = value.binding.into_binding(&backend_class)?;
        Ok(ReplicaBindingRecord::new(binding, backend_class)?)
    }
}

impl From<&BackendClass> for BackendClassWire {
    fn from(value: &BackendClass) -> Self {
        Self {
            provider: ProviderWire::from(value.provider_kind()),
            contract_version: value.contract_version(),
            layout_version: value.layout_version(),
            durability: match value.durability_policy() {
                DurabilityPolicy::DurableCommit => DurabilityWire::DurableCommit,
                DurabilityPolicy::DurableCommitWithReplicaSync => {
                    DurabilityWire::DurableCommitWithReplicaSync
                }
            },
            capabilities: value
                .required_capabilities()
                .names()
                .map(str::to_owned)
                .collect(),
        }
    }
}

impl TryFrom<BackendClassWire> for BackendClass {
    type Error = MetaProcessError;

    fn try_from(value: BackendClassWire) -> Result<Self, Self::Error> {
        Ok(BackendClass::with_durability(
            value.provider.into(),
            value.contract_version,
            value.layout_version,
            match value.durability {
                DurabilityWire::DurableCommit => DurabilityPolicy::DurableCommit,
                DurabilityWire::DurableCommitWithReplicaSync => {
                    DurabilityPolicy::DurableCommitWithReplicaSync
                }
            },
            value.capabilities,
        )?)
    }
}

impl From<&ProviderKind> for ProviderWire {
    fn from(value: &ProviderKind) -> Self {
        match value {
            ProviderKind::Fjall => Self::Fjall,
            ProviderKind::PostgreSql => Self::PostgreSql,
            ProviderKind::Neo4j => Self::Neo4j,
            ProviderKind::Remote(name) => Self::Remote(name.clone()),
        }
    }
}

impl From<ProviderWire> for ProviderKind {
    fn from(value: ProviderWire) -> Self {
        match value {
            ProviderWire::Fjall => Self::Fjall,
            ProviderWire::PostgreSql => Self::PostgreSql,
            ProviderWire::Neo4j => Self::Neo4j,
            ProviderWire::Remote(name) => Self::Remote(name),
        }
    }
}

impl From<&ReplicaBinding> for ReplicaBindingWire {
    fn from(value: &ReplicaBinding) -> Self {
        Self {
            cluster_id: value.cluster_id().get(),
            graph_id: value.graph_id().get(),
            shard_id: value.shard_id().get(),
            placement_epoch: value.placement_epoch().get(),
            replica_id: value.replica_id().get(),
            backend_generation: value.backend_generation().get(),
            provider: ProviderWire::from(value.provider_kind()),
            contract_version: value.contract_version(),
            layout_version: value.layout_version(),
            namespace_id: value.namespace_id().as_str().to_owned(),
            endpoint_profile_ref: value.endpoint_profile_ref().to_owned(),
            credential_ref: value.credential_ref().to_owned(),
            role: match value.role() {
                BindingRole::Candidate => BindingRoleWire::Candidate,
                BindingRole::Active => BindingRoleWire::Active,
                BindingRole::Retiring => BindingRoleWire::Retiring,
            },
        }
    }
}

impl ReplicaBindingWire {
    fn into_binding(
        self,
        backend_class: &BackendClass,
    ) -> Result<ReplicaBinding, MetaProcessError> {
        Ok(ReplicaBinding::builder()
            .cluster_id(self.cluster_id)
            .graph_id(self.graph_id)
            .shard_id(self.shard_id)
            .placement_epoch(self.placement_epoch)
            .replica_id(self.replica_id)
            .backend_generation(self.backend_generation)
            .backend_class_digest(backend_class.digest())
            .provider_kind(self.provider.into())
            .contract_version(self.contract_version)
            .layout_version(self.layout_version)
            .capability_digest(backend_class.required_capabilities().digest())
            .namespace_id(self.namespace_id)
            .endpoint_profile_ref(self.endpoint_profile_ref)
            .credential_ref(self.credential_ref)
            .role(match self.role {
                BindingRoleWire::Candidate => BindingRole::Candidate,
                BindingRoleWire::Active => BindingRole::Active,
                BindingRoleWire::Retiring => BindingRole::Retiring,
            })
            .build()?)
    }
}

#[derive(Debug)]
pub enum MetaProcessError {
    Io(String),
    Storage(String),
    Control(String),
    Analytics(String),
    Execution(String),
    Codec(String),
    Transport(String),
    Transaction(String),
    Consensus(String),
    ConsensusIndex,
    ProposalPending,
}

impl Display for MetaProcessError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(message)
            | Self::Storage(message)
            | Self::Control(message)
            | Self::Analytics(message)
            | Self::Execution(message)
            | Self::Codec(message)
            | Self::Transport(message)
            | Self::Transaction(message)
            | Self::Consensus(message) => formatter.write_str(message),
            Self::ConsensusIndex => formatter.write_str("Meta consensus index overflow"),
            Self::ProposalPending => formatter.write_str("Meta proposal is awaiting quorum commit"),
        }
    }
}

impl Error for MetaProcessError {}

impl From<std::io::Error> for MetaProcessError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error.to_string())
    }
}

impl From<StorageError> for MetaProcessError {
    fn from(error: StorageError) -> Self {
        Self::Storage(error.to_string())
    }
}

impl From<dtg_execution::control::ControlError> for MetaProcessError {
    fn from(error: dtg_execution::control::ControlError) -> Self {
        Self::Control(error.to_string())
    }
}

impl From<AnalyticsJobError> for MetaProcessError {
    fn from(error: AnalyticsJobError) -> Self {
        Self::Analytics(error.to_string())
    }
}

impl From<ExecutionBuildError> for MetaProcessError {
    fn from(error: ExecutionBuildError) -> Self {
        Self::Execution(error.to_string())
    }
}

impl From<TxnError> for MetaProcessError {
    fn from(error: TxnError) -> Self {
        Self::Transaction(error.to_string())
    }
}

impl From<tonic::transport::Error> for MetaProcessError {
    fn from(error: tonic::transport::Error) -> Self {
        Self::Transport(error.to_string())
    }
}

impl From<MetaRaftError> for MetaProcessError {
    fn from(error: MetaRaftError) -> Self {
        Self::Consensus(error.to_string())
    }
}
