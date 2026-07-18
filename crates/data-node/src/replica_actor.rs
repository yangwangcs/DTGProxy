use std::path::Path;
use std::sync::{Arc, Mutex as StdMutex};

use raft::eraftpb::Message;
use shard_runtime::DurableRaftReplica;
use storage_api::{LogicalKey, StorageAdapter};
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::{HostError, ProposalOutcome, ReplicaSpec, ReplicaStatus};

const MAX_READY_ROUNDS: usize = 256;

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
        spec: ReplicaSpec,
        queue_capacity: usize,
    ) -> Result<Self, HostError> {
        let replica_directory = data_directory.join(spec.relative_directory());
        std::fs::create_dir_all(&replica_directory).map_err(HostError::from_io)?;
        let replica = DurableRaftReplica::open(
            node_id,
            spec.voters(),
            spec.shard_id(),
            spec.placement_epoch(),
            replica_directory.join("raft"),
            replica_directory.join("adapter"),
        )
        .await
        .map_err(HostError::from_durable)?;
        let (sender, receiver) = mpsc::channel(queue_capacity);
        let (outbound_sender, outbound) = mpsc::channel(queue_capacity);
        let actor_spec = spec.clone();
        let join =
            tokio::spawn(
                async move { run_actor(replica, actor_spec, receiver, outbound_sender).await },
            );
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
    Step {
        message: Box<Message>,
        response: oneshot::Sender<Result<ReplicaStatus, HostError>>,
    },
    Tick,
    Status(oneshot::Sender<Result<ReplicaStatus, HostError>>),
    MultiGet {
        keys: Vec<LogicalKey>,
        response: oneshot::Sender<Result<Vec<Option<Vec<u8>>>, HostError>>,
    },
    Shutdown(oneshot::Sender<Result<(), HostError>>),
}

async fn run_actor(
    mut replica: DurableRaftReplica,
    spec: ReplicaSpec,
    mut receiver: mpsc::Receiver<ActorCommand>,
    outbound: mpsc::Sender<Message>,
) -> Result<(), HostError> {
    while let Some(command) = receiver.recv().await {
        match command {
            ActorCommand::Campaign(response) => {
                let result = match replica.campaign() {
                    Ok(()) => drive_ready(&mut replica, &outbound)
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
                    Ok(duplicate) => match replica.propose(request_id, command) {
                        Ok(()) => drive_ready(&mut replica, &outbound)
                            .await
                            .map(|()| ProposalOutcome::new(status(&replica, &spec), duplicate)),
                        Err(error) => Err(HostError::from_durable(error)),
                    },
                    Err(error) => Err(HostError::from_runtime(error)),
                };
                let _ = response.send(result);
            }
            ActorCommand::Step { message, response } => {
                let result = match replica.step(*message) {
                    Ok(()) => drive_ready(&mut replica, &outbound)
                        .await
                        .map(|()| status(&replica, &spec)),
                    Err(error) => Err(HostError::from_durable(error)),
                };
                let _ = response.send(result);
            }
            ActorCommand::Tick => {
                replica.tick();
                drive_ready(&mut replica, &outbound).await?;
            }
            ActorCommand::Status(response) => {
                let _ = response.send(Ok(status(&replica, &spec)));
            }
            ActorCommand::MultiGet { keys, response } => {
                let result = replica
                    .adapter()
                    .multi_get(&keys)
                    .await
                    .map_err(|error| HostError::Adapter(error.to_string()));
                let _ = response.send(result);
            }
            ActorCommand::Shutdown(response) => {
                let result = drive_ready(&mut replica, &outbound).await;
                let _ = response.send(result);
                return Ok(());
            }
        }
    }
    Ok(())
}

async fn drive_ready(
    replica: &mut DurableRaftReplica,
    outbound: &mpsc::Sender<Message>,
) -> Result<(), HostError> {
    for _ in 0..MAX_READY_ROUNDS {
        if !replica.has_ready() {
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
    }
    Err(HostError::ReadyLoopLimit)
}

fn status(replica: &DurableRaftReplica, spec: &ReplicaSpec) -> ReplicaStatus {
    ReplicaStatus::new(
        spec.graph_id(),
        spec.shard_id(),
        spec.placement_epoch(),
        replica.node_id(),
        replica.is_leader(),
        replica.leader_id(),
        replica.current_term(),
        replica.commit_index(),
        replica.metadata().applied_index,
        spec.role(),
        spec.schema_version(),
        spec.backend_generation(),
    )
}
