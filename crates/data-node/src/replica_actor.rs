use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex as StdMutex};

use adapter_registry::MigrationStatus;
use raft::eraftpb::Message;
use shard_runtime::DurableRaftReplica;
use storage_api::{AdapterRequirement, KeySpan, KeyValue, LogicalKey, StorageAdapter};
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::{
    BackendManager, BackendProfile, BackendSlotState, HostError, ProposalOutcome, ReplicaSpec,
    ReplicaStatus,
};

const MAX_READY_ROUNDS: usize = 256;
const MAX_PENDING_READ_BARRIERS: usize = 1_024;

struct PendingReadBarrier {
    request_id: u128,
    placement_epoch: u64,
    leader_id: u64,
    term: u64,
    deadline: Instant,
    submitted: bool,
    read_index: Option<u64>,
    response: oneshot::Sender<Result<u64, HostError>>,
}

pub(crate) struct ReplicaActorHandle {
    pub(crate) spec: ReplicaSpec,
    sender: mpsc::Sender<ActorCommand>,
    outbound: Arc<Mutex<mpsc::Receiver<Message>>>,
    join: StdMutex<Option<JoinHandle<Result<(), HostError>>>>,
}

impl ReplicaActorHandle {
    pub(crate) async fn open(
        node_id: u64,
        data_directory: &Path,
        backend_manager: Arc<BackendManager>,
        spec: ReplicaSpec,
        queue_capacity: usize,
    ) -> Result<Self, HostError> {
        let replica_directory = data_directory.join(spec.relative_directory());
        std::fs::create_dir_all(&replica_directory).map_err(HostError::from_io)?;
        let backend_slot = backend_manager
            .open_slot(&replica_directory, spec.backend_slot())
            .await
            .map_err(|error| HostError::Adapter(error.to_string()))?;
        let replica = DurableRaftReplica::open_with_adapter_slot(
            node_id,
            spec.voters(),
            spec.shard_id(),
            spec.placement_epoch(),
            replica_directory.join("raft"),
            backend_slot,
        )
        .await
        .map_err(HostError::from_durable)?;
        let spec = spec.reconcile_backend(
            replica.metadata(),
            replica.backend_slot().migration_status(),
        )?;
        let (sender, receiver) = mpsc::channel(queue_capacity);
        let (outbound_sender, outbound) = mpsc::channel(queue_capacity);
        let actor_spec = spec.clone();
        let join = tokio::spawn(async move {
            run_actor(
                replica,
                actor_spec,
                replica_directory,
                backend_manager,
                receiver,
                outbound_sender,
            )
            .await
        });
        Ok(Self {
            spec,
            sender,
            outbound: Arc::new(Mutex::new(outbound)),
            join: StdMutex::new(Some(join)),
        })
    }

    pub(crate) fn sender(&self) -> mpsc::Sender<ActorCommand> {
        self.sender.clone()
    }

    pub(crate) fn outbound(&self) -> Arc<Mutex<mpsc::Receiver<Message>>> {
        Arc::clone(&self.outbound)
    }

    pub(crate) async fn take_outbound_from(
        outbound: &Mutex<mpsc::Receiver<Message>>,
        maximum: usize,
    ) -> Vec<Message> {
        let mut receiver = outbound.lock().await;
        let mut messages = Vec::with_capacity(maximum.min(receiver.len()));
        while messages.len() < maximum {
            match receiver.try_recv() {
                Ok(message) => messages.push(message),
                Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected) => {
                    break;
                }
            }
        }
        messages
    }

    pub(crate) async fn shutdown(self) -> Result<(), HostError> {
        let (response_sender, response_receiver) = oneshot::channel();
        let send_result = self
            .sender
            .send(ActorCommand::Shutdown(response_sender))
            .await;
        let actor_result = if send_result.is_ok() {
            response_receiver
                .await
                .map_err(|_| HostError::ActorStopped)?
        } else {
            Err(HostError::ActorStopped)
        };
        let join = self
            .join
            .lock()
            .map_err(|_| HostError::LockPoisoned)?
            .take();
        let join_result = match join {
            Some(join) => join
                .await
                .map_err(|error| HostError::Join(error.to_string()))?,
            None => Ok(()),
        };
        actor_result.and(join_result)
    }
}

pub(crate) enum ActorCommand {
    Campaign(oneshot::Sender<Result<ReplicaStatus, HostError>>),
    Propose {
        request_id: u128,
        command: Vec<u8>,
        response: oneshot::Sender<Result<ProposalOutcome, HostError>>,
    },
    ProposalStatus {
        request_id: u128,
        command: Vec<u8>,
        response: oneshot::Sender<Result<ProposalOutcome, HostError>>,
    },
    LeaderReadPermit {
        placement_epoch: u64,
        request_id: u128,
        deadline: Instant,
        response: oneshot::Sender<Result<u64, HostError>>,
    },
    Step {
        message: Box<Message>,
        response: oneshot::Sender<Result<ReplicaStatus, HostError>>,
    },
    Tick,
    Status(oneshot::Sender<Result<ReplicaStatus, HostError>>),
    BackendState(oneshot::Sender<Result<(ReplicaSpec, ReplicaStatus, MigrationStatus), HostError>>),
    MultiGet {
        keys: Vec<LogicalKey>,
        response: oneshot::Sender<Result<Vec<Option<Vec<u8>>>, HostError>>,
    },
    Scan {
        span: KeySpan,
        response: oneshot::Sender<Result<Vec<KeyValue>, HostError>>,
    },
    CreateSnapshot {
        destination: PathBuf,
        response: oneshot::Sender<Result<replica_snapshot::SnapshotManifestV1, HostError>>,
    },
    PrepareBackendTarget {
        placement_epoch: u64,
        target_generation: u64,
        target_profile: BackendProfile,
        response: oneshot::Sender<Result<(ReplicaSpec, ReplicaStatus), HostError>>,
    },
    ChangeMembership {
        operation_id: u128,
        old_voters: Vec<u64>,
        new_voters: Vec<u64>,
        learners: Vec<u64>,
        response: oneshot::Sender<Result<(ReplicaStatus, bool), HostError>>,
    },
    PrepareActivation {
        target_epoch: u64,
        voters: Vec<u64>,
        response: oneshot::Sender<Result<ReplicaSpec, HostError>>,
    },
    CommitActivation {
        spec: Box<ReplicaSpec>,
        response: oneshot::Sender<Result<ReplicaStatus, HostError>>,
    },
    Shutdown(oneshot::Sender<Result<(), HostError>>),
}

async fn run_actor(
    mut replica: DurableRaftReplica,
    mut spec: ReplicaSpec,
    replica_directory: PathBuf,
    backend_manager: Arc<BackendManager>,
    mut receiver: mpsc::Receiver<ActorCommand>,
    outbound: mpsc::Sender<Message>,
) -> Result<(), HostError> {
    let mut pending_read_barriers = BTreeMap::new();
    let mut next_read_sequence = 1_u64;
    loop {
        settle_read_barriers(&mut replica, &mut pending_read_barriers);
        let command = if let Some(deadline) = pending_read_barriers
            .values()
            .map(|pending| pending.deadline)
            .min()
        {
            tokio::select! {
                biased;
                () = tokio::time::sleep_until(deadline) => {
                    settle_read_barriers(&mut replica, &mut pending_read_barriers);
                    continue;
                }
                command = receiver.recv() => command,
            }
        } else {
            receiver.recv().await
        };
        let Some(command) = command else {
            fail_all_read_barriers(
                &mut replica,
                &mut pending_read_barriers,
                HostError::ActorStopped,
            );
            return Ok(());
        };
        match command {
            ActorCommand::PrepareBackendTarget {
                placement_epoch,
                target_generation,
                target_profile,
                response,
            } => {
                let result = prepare_backend_target(
                    &mut replica,
                    &mut spec,
                    &replica_directory,
                    backend_manager.as_ref(),
                    placement_epoch,
                    target_generation,
                    target_profile,
                )
                .await
                .map(|()| (spec.clone(), status(&replica, &spec)));
                let _ = response.send(result);
            }
            ActorCommand::Campaign(response) => {
                let result = match replica.campaign() {
                    Ok(()) => drive_ready(
                        &mut replica,
                        &outbound,
                        &mut spec,
                        &mut pending_read_barriers,
                    )
                    .await
                    .map(|()| status(&replica, &spec)),
                    Err(error) => Err(HostError::from_durable(error)),
                };
                let _ = response.send(result);
            }
            ActorCommand::Propose {
                request_id,
                command,
                response,
            } => {
                let result = match replica
                    .state_machine()
                    .request_replay(request_id, &command)
                    .await
                {
                    Ok(true) => Ok(ProposalOutcome::new(status(&replica, &spec), true)),
                    Ok(false) => match replica.propose(request_id, command.clone()) {
                        Ok(()) => match drive_ready(
                            &mut replica,
                            &outbound,
                            &mut spec,
                            &mut pending_read_barriers,
                        )
                        .await
                        {
                            Ok(()) => match replica
                                .state_machine()
                                .request_replay(request_id, &command)
                                .await
                            {
                                Ok(true) => {
                                    Ok(ProposalOutcome::new(status(&replica, &spec), false))
                                }
                                Ok(false) => Err(HostError::ProposalPending { request_id }),
                                Err(error) => Err(HostError::from_runtime(error)),
                            },
                            Err(error) => Err(error),
                        },
                        Err(error) => Err(HostError::from_durable(error)),
                    },
                    Err(error) => Err(HostError::from_runtime(error)),
                };
                let _ = response.send(result);
            }
            ActorCommand::ProposalStatus {
                request_id,
                command,
                response,
            } => {
                let result = match replica
                    .state_machine()
                    .request_replay(request_id, &command)
                    .await
                {
                    Ok(true) => Ok(ProposalOutcome::new(status(&replica, &spec), false)),
                    Ok(false) => Err(HostError::ProposalPending { request_id }),
                    Err(error) => Err(HostError::from_runtime(error)),
                };
                let _ = response.send(result);
            }
            ActorCommand::LeaderReadPermit {
                placement_epoch,
                request_id,
                deadline,
                response,
            } => {
                if request_id == 0 {
                    let _ = response.send(Err(HostError::InvalidReadContext));
                    continue;
                }
                if deadline <= Instant::now() {
                    let _ = response.send(Err(HostError::ReadBarrierDeadline { request_id }));
                    continue;
                }
                let authoritative_epoch = replica.metadata().placement_epoch;
                if placement_epoch != authoritative_epoch {
                    let _ = response.send(Err(HostError::StaleEpoch {
                        expected: authoritative_epoch,
                        actual: placement_epoch,
                    }));
                    continue;
                }
                if pending_read_barriers
                    .values()
                    .any(|pending| pending.request_id == request_id)
                {
                    let _ = response.send(Err(HostError::DuplicateReadContext { request_id }));
                    continue;
                }
                if pending_read_barriers.len() >= MAX_PENDING_READ_BARRIERS {
                    let _ = response.send(Err(HostError::ReadBarrierLimit));
                    continue;
                }
                let Some(leader_id) = replica.leader_id().filter(|_| replica.is_leader()) else {
                    let _ = response.send(Err(HostError::NotLeader {
                        leader_id: replica.leader_id(),
                    }));
                    continue;
                };
                let sequence = next_read_sequence;
                let Some(incremented) = next_read_sequence.checked_add(1) else {
                    let _ = response.send(Err(HostError::ReadBarrierContextExhausted));
                    continue;
                };
                next_read_sequence = incremented;
                let context =
                    read_index_context(spec.shard_id(), authoritative_epoch, request_id, sequence);
                let term = replica.current_term();
                pending_read_barriers.insert(
                    context.clone(),
                    PendingReadBarrier {
                        request_id,
                        placement_epoch,
                        leader_id,
                        term,
                        deadline,
                        submitted: false,
                        read_index: None,
                        response,
                    },
                );
                if let Err(error) = drive_ready(
                    &mut replica,
                    &outbound,
                    &mut spec,
                    &mut pending_read_barriers,
                )
                .await
                    && let Some(pending) = pending_read_barriers.remove(&context)
                {
                    replica.cancel_read_index(&context);
                    let _ = pending.response.send(Err(error));
                }
            }
            ActorCommand::Step { message, response } => {
                let result = match replica.step(*message) {
                    Ok(()) => drive_ready(
                        &mut replica,
                        &outbound,
                        &mut spec,
                        &mut pending_read_barriers,
                    )
                    .await
                    .map(|()| status(&replica, &spec)),
                    Err(error) => Err(HostError::from_durable(error)),
                };
                let _ = response.send(result);
            }
            ActorCommand::Tick => {
                replica.tick();
                drive_ready(
                    &mut replica,
                    &outbound,
                    &mut spec,
                    &mut pending_read_barriers,
                )
                .await?;
            }
            ActorCommand::Status(response) => {
                let _ = response.send(Ok(status(&replica, &spec)));
            }
            ActorCommand::BackendState(response) => {
                let _ = response.send(Ok((
                    spec.clone(),
                    status(&replica, &spec),
                    replica.backend_slot().migration_status(),
                )));
            }
            ActorCommand::MultiGet { keys, response } => {
                let result = replica
                    .adapter()
                    .multi_get(&keys)
                    .await
                    .map_err(HostError::from_adapter);
                let _ = response.send(result);
            }
            ActorCommand::Scan { span, response } => {
                let result = replica
                    .adapter()
                    .scan(&span)
                    .await
                    .map_err(HostError::from_adapter);
                let _ = response.send(result);
            }
            ActorCommand::CreateSnapshot {
                destination,
                response,
            } => {
                let result = if !replica.is_leader() {
                    Err(HostError::NotLeader {
                        leader_id: replica.leader_id(),
                    })
                } else {
                    replica_snapshot::create_snapshot_bundle(
                        replica.state_machine(),
                        spec.voters(),
                        destination,
                    )
                    .map_err(|error| HostError::Snapshot(error.to_string()))
                };
                let _ = response.send(result);
            }
            ActorCommand::ChangeMembership {
                operation_id,
                old_voters,
                new_voters,
                learners,
                response,
            } => {
                let result = match replica.membership() {
                    Ok(current)
                        if current.voters_outgoing.is_empty()
                            && current.voters == new_voters
                            && current.learners == learners =>
                    {
                        Ok((status(&replica, &spec), true))
                    }
                    Ok(current)
                        if !current.voters_outgoing.is_empty()
                            && current.voters == new_voters
                            && current.voters_outgoing == old_voters
                            && current.learners == learners =>
                    {
                        if !replica.is_leader() {
                            Err(HostError::NotLeader {
                                leader_id: replica.leader_id(),
                            })
                        } else {
                            match replica.leave_joint_membership(operation_id) {
                                Ok(()) => {
                                    match drive_ready(
                                        &mut replica,
                                        &outbound,
                                        &mut spec,
                                        &mut pending_read_barriers,
                                    )
                                    .await
                                    {
                                        Ok(()) => match replica.membership() {
                                            Ok(final_state)
                                                if final_state.voters_outgoing.is_empty()
                                                    && final_state.voters == new_voters
                                                    && final_state.learners == learners =>
                                            {
                                                Ok((status(&replica, &spec), false))
                                            }
                                            Ok(_) => Err(HostError::MembershipPending),
                                            Err(error) => Err(HostError::from_durable(error)),
                                        },
                                        Err(error) => Err(error),
                                    }
                                }
                                Err(error) => Err(HostError::from_durable(error)),
                            }
                        }
                    }
                    Ok(current) if current.voters != old_voters => {
                        Err(HostError::MembershipConflict)
                    }
                    Ok(_) => {
                        match replica.propose_membership(operation_id, &new_voters, &learners) {
                            Ok(_) => match drive_ready(
                                &mut replica,
                                &outbound,
                                &mut spec,
                                &mut pending_read_barriers,
                            )
                            .await
                            {
                                Ok(()) => match replica.membership() {
                                    Ok(current)
                                        if current.voters == new_voters
                                            && current.learners == learners =>
                                    {
                                        Ok((status(&replica, &spec), false))
                                    }
                                    Ok(_) => Err(HostError::MembershipPending),
                                    Err(error) => Err(HostError::from_durable(error)),
                                },
                                Err(error) => Err(error),
                            },
                            Err(error) => Err(HostError::from_durable(error)),
                        }
                    }
                    Err(error) => Err(HostError::from_durable(error)),
                };
                let _ = response.send(result);
            }
            ActorCommand::PrepareActivation {
                target_epoch,
                voters,
                response,
            } => {
                let result = if spec.placement_epoch() == target_epoch
                    && spec.voters() == voters
                    && spec.role() == crate::ReplicaRole::Voter
                {
                    Ok(spec.clone())
                } else {
                    match replica.membership() {
                        Ok(membership)
                            if replica.metadata().placement_epoch == target_epoch
                                && membership.voters == voters
                                && membership.learners.is_empty() =>
                        {
                            spec.entry()
                                .clone()
                                .activated(target_epoch, voters)
                                .map(ReplicaSpec::from_entry)
                                .map_err(HostError::from)
                        }
                        Ok(_) => Err(HostError::ActivationFenceMismatch),
                        Err(error) => Err(HostError::from_durable(error)),
                    }
                };
                let _ = response.send(result);
            }
            ActorCommand::CommitActivation {
                spec: activated,
                response,
            } => {
                spec = *activated;
                let _ = response.send(Ok(status(&replica, &spec)));
            }
            ActorCommand::Shutdown(response) => {
                fail_all_read_barriers(
                    &mut replica,
                    &mut pending_read_barriers,
                    HostError::ActorStopped,
                );
                let result = drive_ready(
                    &mut replica,
                    &outbound,
                    &mut spec,
                    &mut pending_read_barriers,
                )
                .await;
                let _ = response.send(result);
                return Ok(());
            }
        }
    }
}

async fn drive_ready(
    replica: &mut DurableRaftReplica,
    outbound: &mpsc::Sender<Message>,
    spec: &mut ReplicaSpec,
    pending_read_barriers: &mut BTreeMap<Vec<u8>, PendingReadBarrier>,
) -> Result<(), HostError> {
    for _ in 0..MAX_READY_ROUNDS {
        if !replica.has_ready() {
            *spec = spec.reconcile_backend(
                replica.metadata(),
                replica.backend_slot().migration_status(),
            )?;
            settle_read_barriers(replica, pending_read_barriers);
            if replica.has_ready() {
                continue;
            }
            return Ok(());
        }
        let messages = replica
            .process_ready()
            .await
            .map_err(HostError::from_durable)?;
        for message in messages {
            outbound.try_send(message).map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => HostError::OutboundOverloaded,
                mpsc::error::TrySendError::Closed(_) => HostError::ActorStopped,
            })?;
        }
        settle_read_barriers(replica, pending_read_barriers);
    }
    Err(HostError::ReadyLoopLimit)
}

fn settle_read_barriers(
    replica: &mut DurableRaftReplica,
    pending: &mut BTreeMap<Vec<u8>, PendingReadBarrier>,
) {
    while let Some((context, read_index, leader_id, term)) = replica.take_completed_read_state() {
        let Some(barrier) = pending.get_mut(&context) else {
            continue;
        };
        if barrier.leader_id == leader_id && barrier.term == term {
            barrier.read_index = Some(read_index);
        } else if let Some(barrier) = pending.remove(&context) {
            let _ = barrier.response.send(Err(HostError::NotLeader {
                leader_id: replica.leader_id(),
            }));
        }
    }

    let now = Instant::now();
    let contexts = pending.keys().cloned().collect::<Vec<_>>();
    for context in contexts {
        let outcome = {
            let barrier = pending
                .get(&context)
                .expect("read barrier context came from pending map");
            if barrier.response.is_closed() {
                Some(None)
            } else if barrier.deadline <= now {
                Some(Some(Err(HostError::ReadBarrierDeadline {
                    request_id: barrier.request_id,
                })))
            } else if barrier.placement_epoch != replica.metadata().placement_epoch {
                Some(Some(Err(HostError::StaleEpoch {
                    expected: replica.metadata().placement_epoch,
                    actual: barrier.placement_epoch,
                })))
            } else if !replica.is_leader()
                || replica.leader_id() != Some(barrier.leader_id)
                || replica.current_term() != barrier.term
            {
                Some(Some(Err(HostError::NotLeader {
                    leader_id: replica.leader_id(),
                })))
            } else if let Some(read_index) = barrier.read_index
                && (read_index == 0 || replica.metadata().applied_index >= read_index)
            {
                if read_index == 0 {
                    Some(Some(Err(HostError::ReadBarrierUnavailable {
                        request_id: barrier.request_id,
                    })))
                } else {
                    Some(Some(Ok(read_index)))
                }
            } else {
                None
            }
        };
        let Some(outcome) = outcome else {
            continue;
        };
        replica.cancel_read_index(&context);
        let barrier = pending
            .remove(&context)
            .expect("settled read barrier remains pending");
        if let Some(result) = outcome {
            let _ = barrier.response.send(result);
        }
    }

    let contexts = pending
        .iter()
        .filter_map(|(context, barrier)| (!barrier.submitted).then_some(context.clone()))
        .collect::<Vec<_>>();
    for context in contexts {
        match replica.request_read_index(context.clone()) {
            Ok(()) => {
                if let Some(barrier) = pending.get_mut(&context) {
                    barrier.submitted = true;
                }
            }
            Err(shard_runtime::DurableReplicaError::ReadIndexLeaderNotReady) => {}
            Err(error) => {
                let barrier = pending
                    .remove(&context)
                    .expect("failed ReadIndex request remains pending");
                let _ = barrier.response.send(Err(HostError::from_durable(error)));
            }
        }
    }
}

fn fail_all_read_barriers(
    replica: &mut DurableRaftReplica,
    pending: &mut BTreeMap<Vec<u8>, PendingReadBarrier>,
    error: HostError,
) {
    for (context, barrier) in std::mem::take(pending) {
        replica.cancel_read_index(&context);
        let _ = barrier.response.send(Err(error.clone()));
    }
}

fn read_index_context(
    shard_id: u32,
    placement_epoch: u64,
    request_id: u128,
    sequence: u64,
) -> Vec<u8> {
    let mut context = Vec::with_capacity(40);
    context.extend_from_slice(b"DTRD");
    context.extend_from_slice(&shard_id.to_be_bytes());
    context.extend_from_slice(&placement_epoch.to_be_bytes());
    context.extend_from_slice(&request_id.to_be_bytes());
    context.extend_from_slice(&sequence.to_be_bytes());
    context
}

#[allow(clippy::too_many_arguments)]
async fn prepare_backend_target(
    replica: &mut DurableRaftReplica,
    spec: &mut ReplicaSpec,
    replica_directory: &Path,
    backend_manager: &BackendManager,
    placement_epoch: u64,
    target_generation: u64,
    target_profile: BackendProfile,
) -> Result<(), HostError> {
    if placement_epoch != spec.placement_epoch() {
        return Err(HostError::StaleEpoch {
            expected: spec.placement_epoch(),
            actual: placement_epoch,
        });
    }
    let source_generation = replica.metadata().backend_generation;
    if target_generation != source_generation.checked_add(1).unwrap_or(0) {
        return Err(HostError::Adapter(
            "target backend generation is not consecutive".into(),
        ));
    }
    if let BackendSlotState::DualApplying {
        source_generation: local_source,
        target_generation: local_target,
        target,
        ..
    } = spec.backend_slot()
        && *local_source == source_generation
        && *local_target == target_generation
        && target.digest() == target_profile.digest()
    {
        return Ok(());
    }
    if !matches!(
        replica.metadata().backend_lifecycle,
        shard_runtime::BackendLifecycle::Active
    ) || !matches!(
        replica.backend_slot().migration_status(),
        MigrationStatus::Idle { generation } if generation == source_generation
    ) {
        return Err(HostError::Adapter(
            "backend slot is not ready to prepare a target".into(),
        ));
    }
    let source_profile = spec.backend_slot().active_profile().clone();
    let (target, fence_index) = backend_manager
        .restore_target(
            replica_directory,
            replica.backend_slot().active_adapter(),
            &target_profile,
        )
        .await
        .map_err(|error| HostError::Adapter(error.to_string()))?;
    replica
        .backend_slot()
        .start_migration(target, AdapterRequirement::HotPluggableReplica)
        .map_err(|error| HostError::Adapter(error.to_string()))?;
    let slot = BackendSlotState::dual_applying(
        source_generation,
        source_profile,
        target_generation,
        target_profile,
        fence_index,
        fence_index,
    )?;
    *spec = ReplicaSpec::from_entry(spec.entry().clone().with_backend_slot(slot)?);
    Ok(())
}

fn status(replica: &DurableRaftReplica, spec: &ReplicaSpec) -> ReplicaStatus {
    ReplicaStatus::new(
        spec.graph_id(),
        spec.shard_id(),
        replica.metadata().placement_epoch,
        replica.node_id(),
        replica.is_leader(),
        replica.leader_id(),
        replica.current_term(),
        replica.commit_index(),
        replica.metadata().applied_index,
        spec.role(),
        spec.schema_version(),
        spec.backend_generation(),
        spec.snapshot_index(),
        true,
    )
}
