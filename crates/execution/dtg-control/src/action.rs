use std::collections::BTreeMap;

use crate::{
    BackendGeneration, BindingRole, ControlError, Digest32, GraphId, NamespaceId, PlacementEpoch,
    ProviderKind, ReconcileAction, ReplicaBinding, ReplicaId, ShardId, Version,
};

const ACTION_COMMAND_FORMAT_VERSION: u32 = 1;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub struct ActionId([u8; 32]);

impl ActionId {
    pub const fn get(self) -> [u8; 32] {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ActionLease {
    epoch: u64,
    expires_at: u64,
}

impl ActionLease {
    pub const fn epoch(self) -> u64 {
        self.epoch
    }

    pub const fn expires_at(self) -> u64 {
        self.expires_at
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ActionFailure {
    message: String,
    retryable: bool,
}

impl ActionFailure {
    pub fn retryable(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            retryable: true,
        }
    }

    pub fn terminal(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            retryable: false,
        }
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub const fn is_retryable(&self) -> bool {
        self.retryable
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ActionState {
    Pending {
        attempt: u32,
    },
    Claimed {
        attempt: u32,
        worker: String,
        lease: ActionLease,
    },
    Completed {
        attempt: u32,
        lease: ActionLease,
    },
    Failed {
        attempt: u32,
        failure: ActionFailure,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ActionRecord {
    action_id: ActionId,
    catalog_version: Version,
    action: ReconcileAction,
    state: ActionState,
}

impl ActionRecord {
    pub const fn action_id(&self) -> ActionId {
        self.action_id
    }

    pub const fn catalog_version(&self) -> Version {
        self.catalog_version
    }

    pub const fn action(&self) -> &ReconcileAction {
        &self.action
    }

    pub const fn state(&self) -> &ActionState {
        &self.state
    }

    pub const fn lease(&self) -> Option<ActionLease> {
        match self.state {
            ActionState::Claimed { lease, .. } => Some(lease),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ActionCommand {
    Enqueue {
        catalog_version: Version,
        action: ReconcileAction,
    },
    Claim {
        action_id: ActionId,
        worker: String,
        now: u64,
        lease_duration: u64,
    },
    Complete {
        action_id: ActionId,
        lease: ActionLease,
    },
    Fail {
        action_id: ActionId,
        lease: ActionLease,
        failure: ActionFailure,
    },
}

impl ActionCommand {
    pub const fn enqueue(catalog_version: Version, action: ReconcileAction) -> Self {
        Self::Enqueue {
            catalog_version,
            action,
        }
    }

    pub fn claim(
        action_id: ActionId,
        worker: impl Into<String>,
        now: u64,
        lease_duration: u64,
    ) -> Self {
        Self::Claim {
            action_id,
            worker: worker.into(),
            now,
            lease_duration,
        }
    }

    pub const fn complete(action_id: ActionId, lease: ActionLease) -> Self {
        Self::Complete { action_id, lease }
    }

    pub const fn fail(action_id: ActionId, lease: ActionLease, failure: ActionFailure) -> Self {
        Self::Fail {
            action_id,
            lease,
            failure,
        }
    }

    pub fn encode_current(&self) -> Result<Vec<u8>, ControlError> {
        let mut out = Vec::new();
        put_u32(&mut out, ACTION_COMMAND_FORMAT_VERSION);
        match self {
            Self::Enqueue {
                catalog_version,
                action,
            } => {
                out.push(1);
                put_u64(&mut out, catalog_version.get());
                encode_reconcile_action(&mut out, action)?;
            }
            Self::Claim {
                action_id,
                worker,
                now,
                lease_duration,
            } => {
                out.push(2);
                out.extend_from_slice(&action_id.0);
                put_string(&mut out, worker)?;
                put_u64(&mut out, *now);
                put_u64(&mut out, *lease_duration);
            }
            Self::Complete { action_id, lease } => {
                out.push(3);
                out.extend_from_slice(&action_id.0);
                encode_lease(&mut out, *lease);
            }
            Self::Fail {
                action_id,
                lease,
                failure,
            } => {
                out.push(4);
                out.extend_from_slice(&action_id.0);
                encode_lease(&mut out, *lease);
                out.push(u8::from(failure.retryable));
                put_string(&mut out, &failure.message)?;
            }
        }
        Ok(out)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, ControlError> {
        let mut input = Input::new(bytes);
        if input.u32()? != ACTION_COMMAND_FORMAT_VERSION {
            return Err(ControlError::InvalidAction(
                "unsupported action command format version",
            ));
        }
        let command = match input.byte()? {
            1 => Self::enqueue(
                Version::new(input.u64()?),
                decode_reconcile_action(&mut input)?,
            ),
            2 => Self::claim(
                input.action_id()?,
                input.string()?,
                input.u64()?,
                input.u64()?,
            ),
            3 => Self::complete(input.action_id()?, input.lease()?),
            4 => {
                let action_id = input.action_id()?;
                let lease = input.lease()?;
                let retryable = match input.byte()? {
                    0 => false,
                    1 => true,
                    _ => return Err(ControlError::InvalidAction("invalid failure retry flag")),
                };
                let message = input.string()?;
                Self::fail(
                    action_id,
                    lease,
                    if retryable {
                        ActionFailure::retryable(message)
                    } else {
                        ActionFailure::terminal(message)
                    },
                )
            }
            _ => return Err(ControlError::InvalidAction("unknown action command tag")),
        };
        if !input.is_empty() {
            return Err(ControlError::InvalidAction(
                "action command contains trailing bytes",
            ));
        }
        Ok(command)
    }
}

fn encode_lease(out: &mut Vec<u8>, lease: ActionLease) {
    put_u64(out, lease.epoch);
    put_u64(out, lease.expires_at);
}

fn encode_reconcile_action(
    out: &mut Vec<u8>,
    action: &ReconcileAction,
) -> Result<(), ControlError> {
    match action {
        ReconcileAction::Allocate { binding } => {
            out.push(1);
            encode_binding(out, binding)?;
        }
        ReconcileAction::StartLearner {
            graph_id,
            shard_id,
            placement_epoch,
            backend_generation,
            replica_id,
        }
        | ReconcileAction::Promote {
            graph_id,
            shard_id,
            placement_epoch,
            backend_generation,
            replica_id,
        }
        | ReconcileAction::Seal {
            graph_id,
            shard_id,
            placement_epoch,
            backend_generation,
            replica_id,
        } => {
            out.push(match action {
                ReconcileAction::StartLearner { .. } => 2,
                ReconcileAction::Promote { .. } => 3,
                _ => 5,
            });
            encode_target(
                out,
                graph_id.get(),
                shard_id.get(),
                placement_epoch.get(),
                backend_generation.get(),
                replica_id.get(),
            );
        }
        ReconcileAction::TransferLeader {
            graph_id,
            shard_id,
            placement_epoch,
            from,
            to,
        } => {
            out.push(4);
            put_u64(out, graph_id.get());
            put_u64(out, shard_id.get());
            put_u64(out, placement_epoch.get());
            put_u64(out, from.get());
            put_u64(out, to.get());
        }
        ReconcileAction::Migrate {
            graph_id,
            shard_id,
            placement_epoch,
            from_generation,
            to_generation,
        } => {
            out.push(6);
            put_u64(out, graph_id.get());
            put_u64(out, shard_id.get());
            put_u64(out, placement_epoch.get());
            put_u64(out, from_generation.get());
            put_u64(out, to_generation.get());
        }
        ReconcileAction::DeleteNamespace {
            graph_id,
            shard_id,
            placement_epoch,
            backend_generation,
            replica_id,
            namespace_id,
        } => {
            out.push(7);
            encode_target(
                out,
                graph_id.get(),
                shard_id.get(),
                placement_epoch.get(),
                backend_generation.get(),
                replica_id.get(),
            );
            put_string(out, namespace_id.as_str())?;
        }
    }
    Ok(())
}

fn decode_reconcile_action(input: &mut Input<'_>) -> Result<ReconcileAction, ControlError> {
    Ok(match input.byte()? {
        1 => ReconcileAction::Allocate {
            binding: decode_binding(input)?,
        },
        tag @ (2 | 3 | 5) => {
            let (graph_id, shard_id, placement_epoch, backend_generation, replica_id) =
                decode_target(input)?;
            match tag {
                2 => ReconcileAction::StartLearner {
                    graph_id,
                    shard_id,
                    placement_epoch,
                    backend_generation,
                    replica_id,
                },
                3 => ReconcileAction::Promote {
                    graph_id,
                    shard_id,
                    placement_epoch,
                    backend_generation,
                    replica_id,
                },
                _ => ReconcileAction::Seal {
                    graph_id,
                    shard_id,
                    placement_epoch,
                    backend_generation,
                    replica_id,
                },
            }
        }
        4 => ReconcileAction::TransferLeader {
            graph_id: graph_id(input.u64()?)?,
            shard_id: shard_id(input.u64()?)?,
            placement_epoch: placement_epoch(input.u64()?)?,
            from: replica_id(input.u64()?)?,
            to: replica_id(input.u64()?)?,
        },
        6 => ReconcileAction::Migrate {
            graph_id: graph_id(input.u64()?)?,
            shard_id: shard_id(input.u64()?)?,
            placement_epoch: placement_epoch(input.u64()?)?,
            from_generation: generation(input.u64()?)?,
            to_generation: generation(input.u64()?)?,
        },
        7 => {
            let (graph_id, shard_id, placement_epoch, backend_generation, replica_id) =
                decode_target(input)?;
            ReconcileAction::DeleteNamespace {
                graph_id,
                shard_id,
                placement_epoch,
                backend_generation,
                replica_id,
                namespace_id: NamespaceId::new(input.string()?)
                    .map_err(|_| ControlError::InvalidAction("invalid namespace"))?,
            }
        }
        _ => return Err(ControlError::InvalidAction("unknown reconcile action tag")),
    })
}

fn encode_target(
    out: &mut Vec<u8>,
    graph: u64,
    shard: u64,
    epoch: u64,
    generation: u64,
    replica: u64,
) {
    for value in [graph, shard, epoch, generation, replica] {
        put_u64(out, value);
    }
}

fn decode_target(
    input: &mut Input<'_>,
) -> Result<
    (
        GraphId,
        ShardId,
        PlacementEpoch,
        BackendGeneration,
        ReplicaId,
    ),
    ControlError,
> {
    Ok((
        graph_id(input.u64()?)?,
        shard_id(input.u64()?)?,
        placement_epoch(input.u64()?)?,
        generation(input.u64()?)?,
        replica_id(input.u64()?)?,
    ))
}

fn encode_binding(out: &mut Vec<u8>, binding: &ReplicaBinding) -> Result<(), ControlError> {
    for value in [
        binding.cluster_id().get(),
        binding.graph_id().get(),
        binding.shard_id().get(),
        binding.placement_epoch().get(),
        binding.replica_id().get(),
        binding.backend_generation().get(),
    ] {
        put_u64(out, value);
    }
    out.extend_from_slice(&binding.backend_class_digest().get());
    match binding.provider_kind() {
        ProviderKind::Fjall => out.push(1),
        ProviderKind::PostgreSql => out.push(2),
        ProviderKind::Kuzu => out.push(3),
        ProviderKind::Remote(name) => {
            out.push(4);
            put_string(out, name)?;
        }
    }
    put_u32(out, binding.contract_version());
    put_u32(out, binding.layout_version());
    out.extend_from_slice(&binding.capability_digest().get());
    put_string(out, binding.namespace_id().as_str())?;
    put_string(out, binding.endpoint_profile_ref())?;
    put_string(out, binding.credential_ref())?;
    out.push(match binding.role() {
        BindingRole::Candidate => 1,
        BindingRole::Active => 2,
        BindingRole::Retiring => 3,
    });
    Ok(())
}

fn decode_binding(input: &mut Input<'_>) -> Result<ReplicaBinding, ControlError> {
    let cluster = input.u64()?;
    let graph = input.u64()?;
    let shard = input.u64()?;
    let epoch = input.u64()?;
    let replica = input.u64()?;
    let generation = input.u64()?;
    let backend_class_digest = Digest32::new(input.array32()?);
    let provider = match input.byte()? {
        1 => ProviderKind::Fjall,
        2 => ProviderKind::PostgreSql,
        3 => ProviderKind::Kuzu,
        4 => ProviderKind::Remote(input.string()?),
        _ => return Err(ControlError::InvalidAction("invalid provider")),
    };
    let contract = input.u32()?;
    let layout = input.u32()?;
    let capability_digest = Digest32::new(input.array32()?);
    let namespace = input.string()?;
    let endpoint = input.string()?;
    let credential = input.string()?;
    let role = match input.byte()? {
        1 => BindingRole::Candidate,
        2 => BindingRole::Active,
        3 => BindingRole::Retiring,
        _ => return Err(ControlError::InvalidAction("invalid binding role")),
    };
    ReplicaBinding::builder()
        .cluster_id(cluster)
        .graph_id(graph)
        .shard_id(shard)
        .placement_epoch(epoch)
        .replica_id(replica)
        .backend_generation(generation)
        .backend_class_digest(backend_class_digest)
        .provider_kind(provider)
        .contract_version(contract)
        .layout_version(layout)
        .capability_digest(capability_digest)
        .namespace_id(namespace)
        .endpoint_profile_ref(endpoint)
        .credential_ref(credential)
        .role(role)
        .build()
        .map_err(|_| ControlError::InvalidAction("invalid replica binding"))
}

fn graph_id(value: u64) -> Result<GraphId, ControlError> {
    GraphId::new(value).map_err(|_| ControlError::InvalidAction("invalid graph ID"))
}
fn shard_id(value: u64) -> Result<ShardId, ControlError> {
    ShardId::new(value).map_err(|_| ControlError::InvalidAction("invalid shard ID"))
}
fn placement_epoch(value: u64) -> Result<PlacementEpoch, ControlError> {
    PlacementEpoch::new(value).map_err(|_| ControlError::InvalidAction("invalid placement epoch"))
}
fn generation(value: u64) -> Result<BackendGeneration, ControlError> {
    BackendGeneration::new(value).map_err(|_| ControlError::InvalidAction("invalid generation"))
}
fn replica_id(value: u64) -> Result<ReplicaId, ControlError> {
    ReplicaId::new(value).map_err(|_| ControlError::InvalidAction("invalid replica ID"))
}

fn put_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_be_bytes());
}
fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_be_bytes());
}
fn put_string(out: &mut Vec<u8>, value: &str) -> Result<(), ControlError> {
    let len: u32 = value
        .len()
        .try_into()
        .map_err(|_| ControlError::InvalidAction("string too large"))?;
    put_u32(out, len);
    out.extend_from_slice(value.as_bytes());
    Ok(())
}

struct Input<'a> {
    bytes: &'a [u8],
    offset: usize,
}
impl<'a> Input<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }
    fn take(&mut self, len: usize) -> Result<&'a [u8], ControlError> {
        let end = self
            .offset
            .checked_add(len)
            .filter(|end| *end <= self.bytes.len())
            .ok_or(ControlError::InvalidAction("truncated action command"))?;
        let value = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(value)
    }
    fn byte(&mut self) -> Result<u8, ControlError> {
        Ok(self.take(1)?[0])
    }
    fn u32(&mut self) -> Result<u32, ControlError> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64, ControlError> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn array32(&mut self) -> Result<[u8; 32], ControlError> {
        Ok(self.take(32)?.try_into().unwrap())
    }
    fn string(&mut self) -> Result<String, ControlError> {
        let len = self.u32()? as usize;
        String::from_utf8(self.take(len)?.to_vec())
            .map_err(|_| ControlError::InvalidAction("invalid UTF-8"))
    }
    fn action_id(&mut self) -> Result<ActionId, ControlError> {
        Ok(ActionId(self.array32()?))
    }
    fn lease(&mut self) -> Result<ActionLease, ControlError> {
        Ok(ActionLease {
            epoch: self.u64()?,
            expires_at: self.u64()?,
        })
    }
    fn is_empty(&self) -> bool {
        self.offset == self.bytes.len()
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ControlActionLedger {
    records: BTreeMap<ActionId, ActionRecord>,
}

impl ControlActionLedger {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn records(&self) -> impl Iterator<Item = &ActionRecord> {
        self.records.values()
    }

    pub fn record(&self, action_id: ActionId) -> Option<&ActionRecord> {
        self.records.get(&action_id)
    }

    pub fn apply(&mut self, command: ActionCommand) -> Result<ActionRecord, ControlError> {
        let action_id = match command {
            ActionCommand::Enqueue {
                catalog_version,
                action,
            } => {
                let action_id = action_id(catalog_version, &action);
                self.records.entry(action_id).or_insert(ActionRecord {
                    action_id,
                    catalog_version,
                    action,
                    state: ActionState::Pending { attempt: 0 },
                });
                action_id
            }
            ActionCommand::Claim {
                action_id,
                worker,
                now,
                lease_duration,
            } => {
                if worker.trim().is_empty() || lease_duration == 0 {
                    return Err(ControlError::InvalidAction(
                        "claim worker and lease duration must be nonempty",
                    ));
                }
                let record = self
                    .records
                    .get_mut(&action_id)
                    .ok_or(ControlError::UnknownAction)?;
                if matches!(record.state, ActionState::Completed { .. }) {
                    return Ok(record.clone());
                }
                let prior_attempt = match &record.state {
                    ActionState::Pending { attempt } => *attempt,
                    ActionState::Claimed { attempt, lease, .. } if now > lease.expires_at => {
                        *attempt
                    }
                    ActionState::Claimed { .. } => {
                        return Err(ControlError::InvalidAction("action lease is still active"));
                    }
                    ActionState::Completed { .. } => unreachable!("handled above"),
                    ActionState::Failed { .. } => {
                        return Err(ControlError::InvalidAction(
                            "terminal action cannot be claimed",
                        ));
                    }
                };
                let attempt = prior_attempt
                    .checked_add(1)
                    .ok_or(ControlError::InvalidAction("action attempt overflow"))?;
                let epoch = u64::from(attempt);
                let expires_at = now
                    .checked_add(lease_duration)
                    .ok_or(ControlError::InvalidAction("action lease overflow"))?;
                record.state = ActionState::Claimed {
                    attempt,
                    worker,
                    lease: ActionLease { epoch, expires_at },
                };
                action_id
            }
            ActionCommand::Complete { action_id, lease } => {
                let record = self
                    .records
                    .get_mut(&action_id)
                    .ok_or(ControlError::UnknownAction)?;
                match record.state {
                    ActionState::Claimed {
                        attempt,
                        lease: active,
                        ..
                    } if active == lease => {
                        record.state = ActionState::Completed { attempt, lease }
                    }
                    ActionState::Completed { lease: active, .. } if active == lease => {
                        return Ok(record.clone());
                    }
                    _ => return Err(ControlError::StaleActionLease),
                }
                action_id
            }
            ActionCommand::Fail {
                action_id,
                lease,
                failure,
            } => {
                let record = self
                    .records
                    .get_mut(&action_id)
                    .ok_or(ControlError::UnknownAction)?;
                let attempt = match record.state {
                    ActionState::Claimed {
                        attempt,
                        lease: active,
                        ..
                    } if active == lease => attempt,
                    _ => return Err(ControlError::StaleActionLease),
                };
                record.state = if failure.retryable {
                    ActionState::Pending { attempt }
                } else {
                    ActionState::Failed { attempt, failure }
                };
                action_id
            }
        };
        self.records
            .get(&action_id)
            .cloned()
            .ok_or(ControlError::UnknownAction)
    }
}

fn action_id(catalog_version: Version, action: &ReconcileAction) -> ActionId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"dtg-control-action-v1");
    hasher.update(&catalog_version.get().to_be_bytes());
    encode_action(&mut hasher, action);
    ActionId(*hasher.finalize().as_bytes())
}

fn encode_action(hasher: &mut blake3::Hasher, action: &ReconcileAction) {
    match action {
        ReconcileAction::Allocate { binding } => {
            hasher.update(&[1]);
            hasher.update(&binding.identity_digest().get());
        }
        ReconcileAction::StartLearner {
            graph_id,
            shard_id,
            placement_epoch,
            backend_generation,
            replica_id,
        }
        | ReconcileAction::Promote {
            graph_id,
            shard_id,
            placement_epoch,
            backend_generation,
            replica_id,
        }
        | ReconcileAction::Seal {
            graph_id,
            shard_id,
            placement_epoch,
            backend_generation,
            replica_id,
        } => {
            let tag = match action {
                ReconcileAction::StartLearner { .. } => 2,
                ReconcileAction::Promote { .. } => 3,
                _ => 5,
            };
            hasher.update(&[tag]);
            encode_replica_target(
                hasher,
                graph_id.get(),
                shard_id.get(),
                placement_epoch.get(),
                backend_generation.get(),
                replica_id.get(),
            );
        }
        ReconcileAction::TransferLeader {
            graph_id,
            shard_id,
            placement_epoch,
            from,
            to,
        } => {
            hasher.update(&[4]);
            hasher.update(&graph_id.get().to_be_bytes());
            hasher.update(&shard_id.get().to_be_bytes());
            hasher.update(&placement_epoch.get().to_be_bytes());
            hasher.update(&from.get().to_be_bytes());
            hasher.update(&to.get().to_be_bytes());
        }
        ReconcileAction::Migrate {
            graph_id,
            shard_id,
            placement_epoch,
            from_generation,
            to_generation,
        } => {
            hasher.update(&[6]);
            hasher.update(&graph_id.get().to_be_bytes());
            hasher.update(&shard_id.get().to_be_bytes());
            hasher.update(&placement_epoch.get().to_be_bytes());
            hasher.update(&from_generation.get().to_be_bytes());
            hasher.update(&to_generation.get().to_be_bytes());
        }
        ReconcileAction::DeleteNamespace {
            graph_id,
            shard_id,
            placement_epoch,
            backend_generation,
            replica_id,
            namespace_id,
        } => {
            hasher.update(&[7]);
            encode_replica_target(
                hasher,
                graph_id.get(),
                shard_id.get(),
                placement_epoch.get(),
                backend_generation.get(),
                replica_id.get(),
            );
            hasher.update(&(namespace_id.as_str().len() as u64).to_be_bytes());
            hasher.update(namespace_id.as_str().as_bytes());
        }
    }
}

fn encode_replica_target(
    hasher: &mut blake3::Hasher,
    graph_id: u64,
    shard_id: u64,
    placement_epoch: u64,
    backend_generation: u64,
    replica_id: u64,
) {
    hasher.update(&graph_id.to_be_bytes());
    hasher.update(&shard_id.to_be_bytes());
    hasher.update(&placement_epoch.to_be_bytes());
    hasher.update(&backend_generation.to_be_bytes());
    hasher.update(&replica_id.to_be_bytes());
}
